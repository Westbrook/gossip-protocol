"""Retain compact study receipts, exact accepted projects, and source snapshots.

All accounting and source-binding checks precede output creation. A completed
export is published atomically to a fresh destination; failed attempts leave the
original evidence available and never strand a partial final destination.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
import ctypes
import errno
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import stat
import sys
import tempfile
import unicodedata
from typing import Any, Sequence

from devtools.git_snapshot import read_text_tree
from gossip_harness.gitstore import GitStore


def save(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n', encoding='utf-8')


def _component(value: Any) -> str:
    name = str(value)
    device = name.split('.')[0].upper()
    windows_devices = {'CON', 'PRN', 'AUX', 'NUL', 'CLOCK$'} | {
        prefix + suffix for prefix in ('COM', 'LPT') for suffix in '123456789¹²³'}
    if (not name or name in ('.', '..') or name.endswith((' ', '.'))
            or any(ord(char) < 32 or char in '/\\:<>"|?*' for char in name)
            or device in windows_devices):
        raise ValueError('Invalid evidence path component')
    return name


def _regular(path: Path, root: Path) -> None:
    if not path.is_relative_to(root) or path == root:
        raise ValueError('Evidence path escapes its input root')
    for parent in (path, *path.parents):
        if parent == root:
            break
        if parent.is_symlink():
            raise ValueError(f'Evidence path must not contain symlinks: {path}')
    if not stat.S_ISREG(path.stat().st_mode):
        raise ValueError(f'Evidence must be a regular file: {path}')


class InputSnapshot:
    """Keep parsed evidence and exported bytes identical, then check originals."""

    def __init__(self) -> None:
        self.files: dict[Path, tuple[Path, bytes]] = {}

    def read(self, path: Path, root: Path) -> bytes:
        if path not in self.files:
            _regular(path, root)
            self.files[path] = (root, path.read_bytes())
        return self.files[path][1]

    def verify(self) -> None:
        for path, (root, contents) in self.files.items():
            _regular(path, root)
            if path.read_bytes() != contents:
                raise ValueError(f'Input evidence changed while preparing export: {path}')


def _read_ledger(path: Path) -> tuple[dict[str, int], dict[str, int]]:
    if path.is_symlink() or not path.is_file():
        raise ValueError('Accounting ledger must be a regular existing file')
    with closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True)) as db:
        # Explicitly bind every query to the same point-in-time snapshot. The
        # shared ledger may legitimately advance after this read-only audit.
        db.execute('BEGIN')
        ledger = dict(reservations=db.execute('select count(*) from reservations').fetchone()[0],
                      unsettled_reservations=db.execute("select count(*) from reservations where state != 'settled'").fetchone()[0],
                      completed_tasks=db.execute("select count(*) from tasks where status='complete'").fetchone()[0],
                      pending_intents=db.execute("select count(*) from intents where state='pending'").fetchone()[0])
        budget = db.execute("select value from settings where key='budget'").fetchone()
        if budget is None or type(budget[0]) is not int or budget[0] < 0:
            raise ValueError('Accounting ledger has no valid budget')
        limit = budget[0]
        used = db.execute('select coalesce(sum(coalesce(spent,amount)),0) from reservations').fetchone()[0]
        if type(used) is not int or used < 0:
            raise ValueError('Accounting ledger has invalid usage')
        return ledger, dict(limit=limit, spent_or_reserved=used, remaining=limit-used)


def _publish_directory(staging: Path, destination: Path) -> None:
    """Atomic directory rename that cannot replace even an empty destination.

    POSIX rename() alone may replace an existing empty directory. Use the
    platform's exclusive rename primitive, and fail closed if unavailable.
    """
    if sys.platform == 'win32':
        os.rename(staging, destination)
        return
    libc = ctypes.CDLL(None, use_errno=True)
    if sys.platform == 'darwin':
        rename = libc.renamex_np
        rename.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint]
        rename.restype = ctypes.c_int
        result = rename(os.fsencode(staging), os.fsencode(destination), 4)  # RENAME_EXCL
    elif sys.platform.startswith('linux') and hasattr(libc, 'renameat2'):
        rename = libc.renameat2
        rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
        rename.restype = ctypes.c_int
        result = rename(-100, os.fsencode(staging), -100, os.fsencode(destination), 1)  # RENAME_NOREPLACE
    else:
        raise OSError(errno.ENOTSUP, 'Atomic exclusive directory rename is unavailable')
    if result:
        code = ctypes.get_errno()
        raise OSError(code, os.strerror(code), str(destination))


def _validate_export_paths(files: dict[str, bytes]) -> None:
    # Retention is portable across case-insensitive and Unicode-normalizing
    # filesystems. Refuse ambiguous trees instead of silently losing a file.
    seen: set[str] = set()
    for name in files:
        parts = name.split('/')
        for part in parts:
            _component(part)
        normalized = unicodedata.normalize('NFC', name).casefold()
        if normalized in seen:
            raise ValueError('Accepted export has colliding paths')
        seen.add(normalized)
    for name in seen:
        if any(parent.as_posix() in seen for parent in Path(name).parents if parent.as_posix() != '.'):
            raise ValueError('Accepted export has conflicting file and directory paths')


def _write_export(root: Path, files: dict[str, bytes]) -> None:
    for name, content in files.items():
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)


def retain(discovery_root: Path, recovery_root: Path, output: Path, accounting_ledger: Path,
           *, source_root: Path | None = None) -> dict[str, Any]:
    """Read and validate all inputs, then atomically publish a complete export."""
    discovery_root, recovery_root = Path(discovery_root), Path(recovery_root)
    for root in (discovery_root, recovery_root):
        if root.is_symlink() or not root.is_dir():
            raise ValueError('Experiment inputs must be real directories')
    discovery_root, recovery_root = discovery_root.resolve(), recovery_root.resolve()
    output = Path(output).absolute()
    if output.exists() or output.is_symlink():
        raise FileExistsError('Retained output must be fresh')
    output = output.parent.resolve() / output.name
    if any(output.is_relative_to(root) for root in (discovery_root, recovery_root)):
        raise ValueError('Retained output must be outside its experiment inputs')
    snapshot = InputSnapshot()
    discovery = json.loads(snapshot.read(discovery_root / 'results.json', discovery_root))
    recovery = json.loads(snapshot.read(recovery_root / 'results.json', recovery_root))
    trace = [json.loads(line) for line in snapshot.read(discovery_root / 'trace.jsonl', discovery_root).splitlines()]
    rtrace = [json.loads(line) for line in snapshot.read(recovery_root / 'trace.jsonl', recovery_root).splitlines()]
    recovery_manifest = json.loads(snapshot.read(recovery_root / 'manifest.json', recovery_root))
    recorded_ledger = discovery['contract'].get('budget_ledger')
    if recorded_ledger is not None and Path(recorded_ledger).resolve() != Path(accounting_ledger).resolve():
        raise ValueError('Accounting ledger differs from the recorded discovery ledger')
    ledger, cumulative_budget = _read_ledger(Path(accounting_ledger))
    for before, after in ((discovery['budget_before'], discovery['budget']),
                          (recovery_manifest['budget_before'], recovery['budget'])):
        if 'limit' in before and 'limit' in after and before['limit'] != after['limit']:
            raise ValueError('Recorded budget limit changed during a run')
    actual_discovery = sum(r.get('usage_units', 0) or 0 for r in trace
                           if r['kind'] in ('discovery_finished', 'worker_finished', 'worker_failed', 'discovery_failed'))
    delta_discovery = discovery['budget']['spent_or_reserved'] - discovery['budget_before']['spent_or_reserved']
    if actual_discovery != delta_discovery:
        raise ValueError('Discovery request accounting does not reconcile with budget delta')
    actual_recovery = sum(r.get('usage_units', 0) or 0 for r in rtrace if r['kind'] in ('worker_result', 'worker_failure'))
    if actual_recovery != recovery['budget']['spent_or_reserved'] - recovery_manifest['budget_before']['spent_or_reserved']:
        raise ValueError('Recovery request accounting does not reconcile with budget delta')
    if any(run['budget']['spent_or_reserved'] > cumulative_budget['spent_or_reserved'] for run in (discovery, recovery)):
        raise ValueError('Current accounting ledger is older than the recorded runs')

    files: dict[str, bytes] = {}
    for name, path in (('discovery', discovery_root), ('recovery', recovery_root)):
        for filename in ('results.json', 'manifest.json', 'trace.jsonl', 'frozen-repair.json'):
            if (path / filename).exists() or (path / filename).is_symlink():
                files[f'{name}/{filename}'] = snapshot.read(path / filename, path)
    summaries = []
    aggregate: dict[str, dict[str, int]] = defaultdict(lambda: dict(trials=0, accepted=0, logical_calls=0,
                                       repair_attempts=0, validation_calls=0,
                                       coding_usage_units_attributed=0, discovery_usage_units_attributed=0))
    accepted_events: dict[tuple[Any, Any], list[Any]] = defaultdict(list)
    for event in trace:
        if event['kind'] == 'project_accepted':
            accepted_events[(event['trial'], event['variant'])].append(event['exact_tested_sha'])
    git_jobs: list[tuple[Path, Path, str, str | None]] = []
    for case in discovery['cases']:
        trial, variant = _component(case['trial']), _component(case['variant'])
        rows = [r for r in case['controlled_worker_audit'] if r['kind'] in ('result', 'failure')]
        coding_cost = sum(r['original_usage_units'] or 0 for r in rows)
        physical_cost = sum(r['usage_units'] or 0 for r in rows)
        scopes = len(case['required_tasks'])
        item = dict(trial=case['trial'], variant=case['variant'], accepted=case['project_accepted'],
                    coding_usage_units_attributed=coding_cost, incremental_coding_usage_units=physical_cost,
                    discovery_usage_units_attributed=case['discovery_usage_units_attributed'],
                    total_usage_units_attributed=coding_cost+case['discovery_usage_units_attributed'],
                    logical_calls=case['counters']['worker_calls'],
                    repair_attempts=max(0, case['counters']['worker_calls']-scopes),
                    validation_calls=case['counters']['validation_calls'],
                    evidence_transport=case['evidence_transport'],
                    release_head=case['release_head'], model_execution=case['model_execution'])
        summaries.append(item)
        row = aggregate[variant]
        row['trials'] += 1
        row['accepted'] += int(item['accepted'])
        for key in ('logical_calls', 'repair_attempts', 'validation_calls',
                    'coding_usage_units_attributed', 'discovery_usage_units_attributed'):
            row[key] += item[key]
        source = discovery_root / f'trial-{trial}'
        prefix = f'discovery/trial-{trial}'
        files[f'{prefix}/notes.json'] = snapshot.read(source / 'notes.json', discovery_root)
        files[f'{prefix}/{variant}/responses.json'] = snapshot.read(source / variant / 'responses.json', discovery_root)
        if item['accepted']:
            if accepted_events[(case['trial'], case['variant'])] != [case['release_head']]:
                raise ValueError('Discovery release differs from exact tested commit')
            git_jobs.append((discovery_root, source / variant / 'release.git', case['release_head'], f'{prefix}/{variant}/accepted'))
    for row in aggregate.values():
        row['total_usage_units_attributed'] = row['coding_usage_units_attributed'] + row['discovery_usage_units_attributed']
    for case in recovery['cases']:
        if not case['project_accepted']:
            continue
        if case['release_head'] != case['exact_tested_sha']:
            raise ValueError('Recovery release differs from exact tested commit')
        git_jobs.append((recovery_root, recovery_root / _component(case['case']) / 'release.git', case['release_head'], None))
    if recovery.get('repair', {}).get('status') == 'accepted':
        repair = recovery['repair']
        if repair['release_head'] != repair['exact_tested_sha']:
            raise ValueError('Generated repair differs from tested release')
        git_jobs.append((recovery_root, recovery_root / 'generation' / 'release.git', repair['release_head'], 'recovery/accepted'))

    # Snapshot immutable Git objects only after the cheap accounting/binding
    # gates. Batch immutable object reads at the recorded commit, never HEAD.
    stores: list[tuple[GitStore, str]] = []
    for root, store_path, expected, export_prefix in git_jobs:
        if (any(parent.is_symlink() for parent in (store_path, *store_path.parents) if parent.is_relative_to(root))
                or not store_path.resolve().is_relative_to(root)):
            raise ValueError('Git evidence path escapes its input root')
        store = GitStore(store_path)
        if store.head() != expected:
            raise ValueError('Accepted Git head differs from recorded release')
        stores.append((store, expected))
        if export_prefix is not None:
            for name, content in read_text_tree(store.path, expected).items():
                parts = name.split('/')
                if any(_component(part).casefold() == '.git' for part in parts) or unicodedata.normalize('NFC', name).casefold() == 'source_commit':
                    raise ValueError('Invalid accepted source path')
                files[f'{export_prefix}/{name}'] = content.encode('utf-8')
            files[f'{export_prefix}/SOURCE_COMMIT'] = (expected + '\n').encode('utf-8')
    source_root = Path(source_root) if source_root is not None else Path(__file__).resolve().parent
    source_hashes = {}
    for source in sorted((source_root / 'gossip_harness').glob('*.py')):
        _regular(source, source_root)
        # One read supplies both the archived bytes and their digest. Concurrent
        # work cannot produce a hash of different bytes than those exported.
        contents = source.read_bytes()
        name = source.relative_to(source_root).as_posix()
        files[f'source-snapshot/{name}'] = contents
        source_hashes[name] = hashlib.sha256(contents).hexdigest()
    summary = dict(discovery_cases=summaries, discovery_by_arm=dict(aggregate),
                   discovery_actual_micro_usd=actual_discovery, recovery_actual_micro_usd=actual_recovery,
                   continuation_actual_micro_usd=actual_discovery+actual_recovery,
                   modes={'discovery':discovery['contract']['mode'], 'recovery':recovery['mode']},
                   cumulative_budget=cumulative_budget, ledger_verification=ledger,
                   discovery_model_request_attempts=(sum(1 for r in trace if r['kind']=='discovery_started'
                       or (r['kind']=='worker_started' and r['reserved_units'] > 0)) if discovery['contract']['mode']=='live' else 0),
                   recovery_model_request_attempts=recovery['repair']['calls'] if recovery['mode']=='live' else 0,
                   recovery_cases=recovery['cases'], source_sha256=source_hashes,
                   cost_note='Conservative token-based estimates, not invoices. Attributed arm costs include reused scouts and replayed coding usage; actual ledger charges them only once.')
    files['summary.json'] = (json.dumps(summary, indent=2, sort_keys=True) + '\n').encode('utf-8')
    provenance = dict(version=1, publication='atomic-exclusive',
                      source_binding_note='Source files are retention-time snapshots; accepted projects are bound to recorded tested commits.',
                      input_sha256={f'{label}/{path.relative_to(root).as_posix()}': hashlib.sha256(contents).hexdigest()
                                    for path, (root, contents) in snapshot.files.items()
                                    for label in ('discovery' if root == discovery_root else 'recovery',)})
    files['retention-manifest.json'] = (json.dumps(provenance, indent=2, sort_keys=True) + '\n').encode('utf-8')
    _validate_export_paths(files)
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f'.{output.name}.staging-', dir=output.parent))
    try:
        _write_export(staging, files)
        snapshot.verify()
        if any(store.head() != expected for store, expected in stores):
            raise ValueError('Accepted Git head changed while preparing export')
        _publish_directory(staging, output)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    return summary


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--discovery', type=Path, required=True)
    parser.add_argument('--recovery', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--accounting-ledger', type=Path, default=Path('runs/first-live-budget.sqlite'))
    args = parser.parse_args(argv)
    summary = retain(args.discovery, args.recovery, args.output, args.accounting_ledger)
    print(json.dumps({k: summary[k] for k in ('discovery_actual_micro_usd','recovery_actual_micro_usd',
                     'continuation_actual_micro_usd','cumulative_budget','ledger_verification',
                     'discovery_model_request_attempts','recovery_model_request_attempts')}, indent=2))


if __name__ == '__main__':
    main()
