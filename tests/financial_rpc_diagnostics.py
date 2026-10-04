"""Bounded, test-only failure observations; no execution or acceptance authority.

Calls are forwarded unchanged. Timing instrumentation adds overhead and is not a
performance benchmark. The failure snapshot can race still-active fixture
writers; the pre-removal snapshot follows the fixture's existing owner close.
"""
from contextlib import contextmanager
import base64
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import time
import traceback
from unittest.mock import patch


class FinancialRPCDiagnostics:
    MAX_EVENTS = 512
    MAX_EVENT_BYTES = 4 * 1024 * 1024
    MAX_FILES = 256
    MAX_FILE_BYTES = 8 * 1024 * 1024
    MAX_TOTAL_BYTES = 64 * 1024 * 1024
    TABLES = ('financial_cohorts_v2', 'financial_actions_v2', 'financial_requests_v2',
              'financial_rpc_config_v2', 'financial_rpc_requests_v2', 'tasks',
              'reservations', 'budget_changes')

    def __init__(self, case):
        self.case = case
        self.started = time.monotonic_ns()
        self.lock = threading.Lock()
        self.events = []
        self.event_bytes = 0
        self.dropped_events = 0
        self.output = None
        self.sequence = 0
        self.failure = None
        try:
            parent = Path(os.environ.get('GOSSIP_RPC_DIAGNOSTIC_ROOT',
                str(Path(__file__).resolve().parents[1] / 'runs'))).resolve()
            parent.mkdir(parents=True, exist_ok=True)
            self.output = Path(tempfile.mkdtemp(prefix='issued-rpc-diagnostic-', dir=parent))
            print('RPC_DIAGNOSTICS=' + str(self.output), file=sys.stderr, flush=True)
        except BaseException as error:
            self.retention_error('create', error)
        # Preserve the existing cleanup order. This callback runs before the
        # fixture cleanup; the wrapper below captures again at the last point
        # before its original TemporaryDirectory removes the original files.
        case.addCleanup(lambda: self.snapshot('before-fixture-cleanup'))
        original_cleanup = case.temp.cleanup
        def cleanup():
            self.snapshot('before-temporary-removal-after-existing-owner-close')
            try:
                return original_cleanup()
            except BaseException as error:
                self.event('temporary-cleanup-error', error_type=type(error).__name__, error=str(error))
                self.flush()
                raise
        case.temp.cleanup = cleanup

    def retention_error(self, operation, error):
        try:
            print('RPC_DIAGNOSTIC_RETENTION_ERROR ' + operation + ': ' + repr(error), file=sys.stderr, flush=True)
        except BaseException:
            pass  # Diagnostics must never replace the test's original failure.

    def event(self, kind, **values):
        try:
            value = {'kind': kind, 'monotonic_ns': time.monotonic_ns(),
                     'thread_id': threading.get_ident(), **values}
            raw = json.dumps(value, sort_keys=True, ensure_ascii=True, allow_nan=False).encode()
            with self.lock:
                if len(self.events) >= self.MAX_EVENTS or self.event_bytes + len(raw) > self.MAX_EVENT_BYTES:
                    self.dropped_events += 1
                else:
                    self.events.append(json.loads(raw))
                    self.event_bytes += len(raw)
        except BaseException as error:
            self.retention_error('event', error)

    def flush(self):
        if self.output is None:
            return
        try:
            with self.lock:
                value = {'protocol': 'issued-rpc-test-diagnostics-v1', 'test': self.case.id(),
                         'started_monotonic_ns': self.started, 'events': list(self.events),
                         'dropped_events': self.dropped_events, 'failure': self.failure,
                         'interpretation': 'Call-through fixture diagnostics; not raw acceptance or timing qualification.'}
            self.sequence += 1
            (self.output / ('timeline-%03d.json' % self.sequence)).write_text(json.dumps(value, indent=2) + '\n')
        except BaseException as error:
            self.retention_error('flush', error)

    @staticmethod
    def sql_value(value):
        if isinstance(value, bytes):
            return {'base64': base64.b64encode(value).decode(), 'sha256': hashlib.sha256(value).hexdigest(),
                    'bytes': len(value)}
        return value

    def snapshot(self, label):
        if self.output is None:
            return
        self.event('snapshot', label=label, transport_entered_count=len(self.case.transport.calls),
                   failed_closed=getattr(self.case.authority, 'failed_closed', None))
        try:
            destination = self.output / ('snapshot-%03d' % (self.sequence + 1))
            destination.mkdir()
            inventory = {'label': label, 'original_root': str(self.case.root), 'files': [], 'errors': [],
                         'raw_files_may_change_during_failure_snapshot': True}
            total = 0
            for source in sorted(self.case.root.rglob('*')):
                if not source.is_file() or source.is_symlink():
                    continue
                name = str(source.relative_to(self.case.root))
                if len(inventory['files']) >= self.MAX_FILES:
                    inventory['errors'].append('File census exceeds bound; suffix omitted')
                    break
                try:
                    before = source.stat()
                    if before.st_size > self.MAX_FILE_BYTES or total + before.st_size > self.MAX_TOTAL_BYTES:
                        inventory['errors'].append('Bound excludes: ' + name)
                        continue
                    with source.open('rb') as stream:
                        raw = stream.read(self.MAX_FILE_BYTES + 1)
                    if len(raw) > self.MAX_FILE_BYTES or total + len(raw) > self.MAX_TOTAL_BYTES:
                        raise ValueError('File grew beyond retained byte bound')
                    after = source.stat()
                    target = destination / 'originals' / name
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(raw)
                    total += len(raw)
                    inventory['files'].append({'path': name, 'sha256': hashlib.sha256(raw).hexdigest(),
                        'bytes': len(raw), 'original_device': before.st_dev, 'original_inode': before.st_ino,
                        'stable_stat': (before.st_size, before.st_mtime_ns, before.st_ino)
                            == (after.st_size, after.st_mtime_ns, after.st_ino)})
                except BaseException as error:
                    inventory['errors'].append(name + ': ' + repr(error))
            # The logical SQL view is one read-only transaction, separate from
            # the raw-file observations above; never label the two atomic.
            sql = {'tables': {}, 'errors': [], 'transaction': 'read-only snapshot'}
            try:
                with sqlite3.connect(self.case.path.as_uri() + '?mode=ro', uri=True, timeout=.2) as db:
                    db.execute('PRAGMA query_only=ON')
                    db.execute('BEGIN')
                    existing = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                    sql_bytes = 0
                    for table in self.TABLES:
                        if table not in existing:
                            continue
                        cursor = db.execute('SELECT * FROM ' + table + ' ORDER BY rowid LIMIT 513')
                        rows = []
                        truncated = False
                        for row in cursor:
                            values = [self.sql_value(v) for v in row]
                            size = len(json.dumps(values).encode())
                            if len(rows) >= 512 or sql_bytes + size > self.MAX_FILE_BYTES // 2:
                                truncated = True
                                break
                            rows.append(values)
                            sql_bytes += size
                        sql['tables'][table] = {'columns': [item[0] for item in cursor.description],
                            'rows': rows, 'truncated': truncated}
                    db.rollback()
            except BaseException as error:
                sql['errors'].append(repr(error))
            raw_sql = json.dumps(sql, sort_keys=True).encode()
            if len(raw_sql) <= self.MAX_FILE_BYTES:
                (destination / 'sql-readonly.json').write_bytes(raw_sql)
            else:
                inventory['errors'].append('Logical SQL exceeds bound; omitted')
            (destination / 'inventory.json').write_text(json.dumps(inventory, indent=2) + '\n')
        except BaseException as error:
            self.retention_error('snapshot', error)
        self.flush()

    @contextmanager
    def instrument(self, financial_class, rpc_class, opener_class):
        guard = financial_class._guard_evidence
        replay = financial_class._replay
        handle = rpc_class.handle
        opened = opener_class.open
        def guarded(owner, *args, **kwargs):
            self.event('guard-enter')
            try:
                result = guard(owner, *args, **kwargs)
            except BaseException as error:
                self.event('guard-error', error_type=type(error).__name__, error=str(error))
                raise
            self.event('guard-return')
            return result
        def replayed(owner, actor, request_id, *args, **kwargs):
            try:
                return replay(owner, actor, request_id, *args, **kwargs)
            except BaseException as error:
                try:
                    # The frozen journal raises from None. Preserve its hidden
                    # __context__ without changing exception propagation.
                    original = error
                    chain = []
                    seen = set()
                    while original is not None and id(original) not in seen and len(chain) < 8:
                        seen.add(id(original))
                        chain.append({'type': type(original).__name__, 'message': str(original),
                            'suppressed_context': original.__suppress_context__,
                            'traceback': ''.join(traceback.format_exception(type(original), original,
                                original.__traceback__, limit=32, chain=False))})
                        original = original.__cause__ or original.__context__
                    self.event('replay-error', actor=actor, request_id=request_id,
                        exception_chain=chain, chain_truncated=original is not None, frame_limit=32)
                except BaseException as retention_error:
                    self.retention_error('replay-exception', retention_error)
                raise
        def handled(owner, envelope):
            body = envelope['body']
            suffix = body['request_id'].removeprefix('rpc-claim-')
            claim_index = int(suffix) if body['operation'] == 'claim' and suffix.isdigit() else None
            self.event('rpc-enter', operation=body['operation'], request_id=body['request_id'], claim_index=claim_index,
                       transport_entered_count=len(self.case.transport.calls))
            try:
                response = handle(owner, envelope)
            except BaseException as error:
                self.event('rpc-error', request_id=body['request_id'], error_type=type(error).__name__, error=str(error))
                raise
            self.event('rpc-return', request_id=body['request_id'], claim_index=claim_index, response=response,
                       transport_entered_count=len(self.case.transport.calls))
            return response
        def opening(owner, request, timeout):
            self.event('transport-enter', transport_entered_count=len(self.case.transport.calls),
                       request_body_sha256=hashlib.sha256(request.data).hexdigest(), timeout=timeout)
            try:
                result = opened(owner, request, timeout)
            except BaseException as error:
                self.event('transport-error', error_type=type(error).__name__, error=str(error),
                           transport_entered_count=len(self.case.transport.calls))
                raise
            self.event('transport-return', transport_entered_count=len(self.case.transport.calls))
            return result
        try:
            with patch.object(financial_class, '_guard_evidence', guarded), \
                    patch.object(financial_class, '_replay', replayed), \
                    patch.object(rpc_class, 'handle', handled), patch.object(opener_class, 'open', opening):
                yield
        except BaseException as error:
            self.failure = {'type': type(error).__name__, 'message': str(error), 'traceback': traceback.format_exc()}
            self.event('test-failure', **self.failure)
            self.snapshot('failure-before-test-cleanup')
            raise
        finally:
            self.flush()
