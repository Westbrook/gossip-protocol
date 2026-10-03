"""Trusted authored M3 worker overlay; never model or hidden acceptance input.

Only explicit enrollment plus an explicit worker process grants automatic job
execution. Recovery uses the admitted immutable manifest and inherited receipt.
"""
from __future__ import annotations

from textwrap import dedent


_WORKER = dedent(r'''
    """Opt-in, fenced execution over the inherited durable job state machine."""
    import sqlite3

    from library.catalog.validation import validate_entries
    from library.common import LibraryError

    VALIDATION_ERRORS = frozenset((
        'invalid_source', 'unsupported_type', 'invalid_utf8', 'too_large',
        'invalid_batch', 'source_changed', 'capacity'))

    class WorkerMixin:
        def enqueue_job(self, job_id):
            with self._write():
                job = self._job_row(job_id)
                if job['state'] not in ('queued', 'running'):
                    raise LibraryError('job_state')
                enrolled = self.db.execute(
                    'SELECT enrolled FROM job_control WHERE job_id=?', (job_id,)).fetchone()
                if enrolled is not None and enrolled['enrolled']:
                    status = 'unchanged'
                else:
                    self.db.execute('INSERT INTO job_control VALUES (?,?,1) '
                        'ON CONFLICT(job_id) DO UPDATE SET enrolled=1', (job_id, job['epoch']))
                    status = 'enqueued'
                return {'status': status, 'job': self.get_job(job_id)}

        def commit_job(self, job_id, epoch, *, fail_before_commit=False):
            claim = getattr(self, '_active_worker_claim', None)
            if claim is None:
                return super().commit_job(job_id, epoch, fail_before_commit=fail_before_commit)
            if claim['job_id'] != job_id or claim['epoch'] != epoch:
                raise LibraryError('stale_worker')
            # The guard owns the outer transaction. Returning from this public
            # method means SQLite commit, including receipt, has completed.
            with self.worker_guard(claim):
                return super().commit_job(job_id, epoch, fail_before_commit=fail_before_commit)

        def _worker_error(self, owner, code):
            # Diagnostics are worker writes too: never let a stale process
            # overwrite the current installation's error after being fenced.
            with self.worker_guard(owner):
                self.record_error('worker', code)

        def worker_process_once(self, owner):
            # Validate even an idle owner, before trusting its job selection.
            try:
                with self.worker_guard(owner):
                    row = self.db.execute(
                        "SELECT j.job_id,j.epoch FROM jobs j JOIN job_control c USING(job_id) "
                        "WHERE c.enrolled=1 AND j.state IN ('queued','running') ORDER BY j.job_id LIMIT 1"
                    ).fetchone()
                    if row is None:
                        # A completed idle check is a successful worker operation.
                        # Clear its prior error under the same owner fence, while
                        # preserving an unrelated maintenance operation's error.
                        self.record_error('worker', None)
                        return {'processed': None, 'job': None}
            except (OSError, sqlite3.Error) as error:
                try:
                    self._worker_error(owner, 'io_error')
                except (OSError, sqlite3.Error):
                    # Keep the declared failure when the database cannot log
                    # it. LibraryError authority fences still propagate.
                    pass
                raise LibraryError('io_error') from error
            claim = dict(owner, job_id=row['job_id'], epoch=row['epoch'])
            try:
                failure = None
                with self.worker_guard(claim) as current:
                    if current['state'] not in ('queued', 'running'):
                        raise LibraryError('job_state')
                    if current['state'] == 'queued':
                        try:
                            validate_entries(self.job_manifest(claim['job_id']), self.all_documents())
                        except LibraryError as error:
                            if error.code not in VALIDATION_ERRORS:
                                raise
                            # Catch within the guard so this terminal failure
                            # survives the transaction; raise only afterwards.
                            self.fail_job(claim['job_id'], claim['epoch'], error.code)
                            failure = error.code
                        else:
                            self.start_job(claim['job_id'], claim['epoch'])
                if failure is not None:
                    raise LibraryError(failure)
                self._active_worker_claim = claim
                try:
                    receipt = self.commit_job(claim['job_id'], claim['epoch'])
                finally:
                    self._active_worker_claim = None
                if 'error' in receipt:
                    raise LibraryError(receipt['error'])
                self._worker_error(claim, None)
                return {'processed': claim['job_id'], 'job': receipt['job']}
            except LibraryError as error:
                # A cancellation, retry or restore can invalidate a claim.
                # Such stale observations never gain authority to log writes.
                if error.code not in ('stale_worker', 'stale_epoch', 'job_state'):
                    self._worker_error(claim, error.code)
                raise
            except (OSError, sqlite3.Error) as error:
                try:
                    self._worker_error(claim, 'io_error')
                except (OSError, sqlite3.Error):
                    pass
                raise LibraryError('io_error') from error
''').lstrip('\n').rstrip() + '\n'

_PROCESS = dedent(r'''
    """Explicit worker command; importing or opening an app never starts it."""
    import argparse
    import json
    import sqlite3
    import sys
    import time

    from library.catalog.worker import VALIDATION_ERRORS
    from library.common import LibraryError

    def _cleanup(store, owner):
        try:
            with store.worker_guard(owner):
                store.cleanup_artifacts()
        except (LibraryError, OSError, sqlite3.Error) as error:
            code = error.code if isinstance(error, LibraryError) else 'io_error'
            if code in ('stale_worker', 'stale_epoch'):
                raise
            try:
                with store.worker_guard(owner):
                    store.record_error('worker', code)
            except (OSError, sqlite3.Error):
                # An unavailable database cannot durably record its own I/O
                # failure. Preserve the failure rather than reporting success.
                pass
            raise LibraryError(code) from error

    def run_worker(store, *, once=False):
        if type(once) is not bool:
            raise LibraryError('invalid_request')
        with store.worker_owner() as owner:
            # Core owner admission performs the first bounded cleanup pass.
            while True:
                try:
                    result = store.worker_process_once(owner)
                except LibraryError as error:
                    _cleanup(store, owner)
                    if once or error.code not in VALIDATION_ERRORS:
                        raise
                    # The job is terminal failed, so it is never automatically
                    # retried. Other explicitly enrolled jobs remain eligible.
                    continue
                _cleanup(store, owner)
                if once:
                    return result
                if result['processed'] is None:
                    time.sleep(1)

    class WorkerParser(argparse.ArgumentParser):
        def error(self, message):
            raise LibraryError('invalid_request')

    def main(argv=None):
        from library.catalog.store import Store
        try:
            parser = WorkerParser(description='Explicit local library worker', allow_abbrev=False)
            parser.add_argument('--db', default='library.sqlite')
            parser.add_argument('--root', default='.')
            parser.add_argument('--backup-dir')
            parser.add_argument('--once', action='store_true')
            args = parser.parse_args(argv)
            store = Store(args.db, backup_dir=args.backup_dir)
            try:
                result = run_worker(store, once=args.once)
            finally:
                store.close()
            if args.once:
                print(json.dumps(result, ensure_ascii=True, sort_keys=True))
            return 0
        except KeyboardInterrupt:
            return 0
        except LibraryError as error:
            print(json.dumps({'error': error.code}), file=sys.stderr)
            return 2
        except (OSError, sqlite3.Error):
            print(json.dumps({'error': 'io_error'}), file=sys.stderr)
            return 2

    if __name__ == '__main__':
        raise SystemExit(main())
''').lstrip('\n').rstrip() + '\n'


def worker_files() -> dict[str, str]:
    """Return fresh source overlays for explicit worker execution."""
    return {'library/catalog/worker.py': _WORKER, 'library/ingestion/worker.py': _PROCESS}
