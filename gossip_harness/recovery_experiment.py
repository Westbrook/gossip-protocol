"""Controlled semantic-conflict repair, followed by paired transport-fault replay.

The divergent patches and obsolete note are deliberate fault injections, not
model mistakes. One centrally informed model repair is frozen and replayed;
transport topology cannot change model inputs or consume extra model requests.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import time
from textwrap import dedent
from typing import Callable

from .gitstore import GitStore
from .ledger import BudgetExceeded, Ledger
from .pilot import DEFAULT_IMAGE, LeaseKeeper, Trace, _save
from .promotion import PromotionCoordinator
from .sandbox import DockerValidator
from .transport import Event, Mesh
from .worker import WorkerFailure, WorkerRequest, WorkerResult
from .pilot_fixture import CHECKS as V1_CHECKS, PARSER_SPEC

SPEC_VERSION = 'task-report-recovery-v2'
SOURCE_COMMIT = 'c007e925c58b4a051d9be853d74e71fe0eeb3bc0'
PATHS = ('task_report/parser.py', 'task_report/summary.py')
FAULT_PROFILES = ('healthy', 'broker_outage', 'partition_heal', 'lossy')
EXPECTED_TEST_COUNTS = {'parser': 14, 'summary': 4, 'all': 28}
PEERS = ('worker-parser', 'worker-summary', 'maintainer-parser', 'maintainer-summary',
         'repair', 'integrator', 'release', 'broker', 'observer')
FAULT_ROUNDS = 12
SPEC = '''Task-report v2 is an additive change to the accepted v1 project.
Preserve every v1 CSV rule except: an estimate that is empty after whitespace
stripping is now unknown, represented by None. Nonempty estimates still require
ASCII decimal digits and become nonnegative ints. Keep the CLI unchanged.
Summary inputs have nonnegative int or None estimates. Return exactly counts
(todo/doing/done, all tasks counted), total_estimate (sum of known estimates),
remaining_estimate (known estimates for todo/doing), known_estimate_count,
unknown_estimate_count, average_estimate. The average is a Python float equal to
total_estimate / known_estimate_count, or None when there are no known estimates.
Zero is known. Count/sum fields are Python ints. No mutation or shared state.
Change only task_report/parser.py and task_report/summary.py, use standard library
only. The test suite is immutable and external to your allowed paths.
'''

CHECKS = {
    'legacy_checks.py': V1_CHECKS['run_checks.py'],
    'run_checks.py': dedent(r'''
        import copy, importlib, json, sys, unittest
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        import legacy_checks as legacy
        SPEC_VERSION = 'task-report-recovery-v2'
        EXPECTED = {'parser': 14, 'summary': 4, 'all': 28}
        HEADER = 'id,title,status,estimate\n'
        class ParserChecks(legacy.ParserChecks):
            def test_decimal_estimate_vocabulary(self):
                for value in ('-1', '+1', '1.5', '1e2', '1_000', '１２', '١'):
                    with self.subTest(value=value), self.assertRaises(ValueError):
                        self.parse(HEADER + f'a,Task,todo,{value}\n')
            def test_unknown_estimate(self):
                self.assertEqual([v['estimate'] for v in self.parse(
                    HEADER + 'a,Task,todo,\nb,Task,doing,  \nc,Task,done,0\n')], [None,None,0])
        def task(identity, status, estimate):
            return dict(id=identity, title='Task', status=status, estimate=estimate)
        def expected(tasks):
            counts = {s: sum(t['status'] == s for t in tasks) for s in ('todo','doing','done')}
            known = [t for t in tasks if t['estimate'] is not None]
            total = sum(t['estimate'] for t in known)
            return dict(counts=counts, total_estimate=total,
                remaining_estimate=sum(t['estimate'] for t in known if t['status'] != 'done'),
                known_estimate_count=len(known), unknown_estimate_count=len(tasks)-len(known),
                average_estimate=total / len(known) if known else None)
        class SummaryKnownChecks(unittest.TestCase):
            @classmethod
            def setUpClass(cls):
                cls.summarize = staticmethod(importlib.import_module('task_report.summary').summarize)
            def check(self, tasks):
                before = copy.deepcopy(tasks)
                actual = self.summarize(tasks)
                self.assertEqual(actual, expected(tasks))
                self.assertEqual(tasks, before)
                for k in ('total_estimate','remaining_estimate','known_estimate_count','unknown_estimate_count'):
                    self.assertIs(type(actual[k]), int)
                self.assertTrue(all(type(v) is int for v in actual['counts'].values()))
                if actual['average_estimate'] is not None:
                    self.assertIs(type(actual['average_estimate']), float)
            def test_all_known(self):
                self.check([task('a','todo',3),task('b','doing',5),task('c','done',8)])
            def test_empty(self): self.check([])
            def test_zero_is_known(self): self.check([task('a','todo',0),task('b','done',0)])
            def test_independent_calls(self):
                first = self.summarize([task('a','todo',2)])
                first_snapshot = copy.deepcopy(first)
                second = self.summarize([task('b','done',3)])
                self.assertEqual(first,first_snapshot)
                self.assertIsNot(first,second)
                self.assertIsNot(first['counts'],second['counts'])
                second_snapshot = copy.deepcopy(second)
                first['counts']['todo'] = 999
                self.assertEqual(second,second_snapshot)
                self.check([task('b','done',3)])
                self.check([task('a','todo',2)])
        class SummaryChecks(SummaryKnownChecks):
            def test_unknown_remaining(self): self.check([task('a','todo',None),task('b','doing',3)])
            def test_unknown_done(self): self.check([task('a','done',None),task('b','todo',2)])
            def test_all_unknown(self): self.check([task('a','todo',None),task('b','doing',None)])
            def test_fractional_average(self): self.check([task('a','todo',2),task('b','done',3),task('c','doing',None)])
            def test_many_unknown_positions(self):
                for i in range(6):
                    tasks = [task(str(j), ('todo','doing','done')[j%3], None if j==i else j) for j in range(6)]
                    self.check(tasks)
        class CliChecks(legacy.CliChecks):
            def test_cli_combines_parser_and_summary(self):
                result = self.invoke(HEADER + 'a,"Build, test\nand ship",todo,3\nb,Review,doing,2\nc,Done,done,7\n')
                self.assertEqual(result.returncode,0,result.stderr)
                self.assertEqual(result.stderr,'')
                self.assertEqual(json.loads(result.stdout),expected([task('a','todo',3),task('b','doing',2),task('c','done',7)]))
            def test_cli_header_only(self):
                result = self.invoke(HEADER)
                self.assertEqual(result.returncode,0,result.stderr)
                self.assertEqual(json.loads(result.stdout),expected([]))
            def test_cli_unknown_end_to_end(self):
                result = self.invoke(HEADER + 'a,Unknown,todo,\nb,Known,done,4\nc,Zero,doing,0\n')
                self.assertEqual(result.returncode,0,result.stderr)
                self.assertEqual(result.stderr,'')
                self.assertEqual(json.loads(result.stdout),expected([task('a','todo',None),task('b','done',4),task('c','doing',0)]))
        def run_checks(target, workspace='/workspace'):
            legacy.WORKSPACE = Path(workspace).resolve()
            sys.path.insert(0,str(legacy.WORKSPACE))
            classes = {'parser':(ParserChecks,), 'summary':(SummaryKnownChecks,), 'all':(ParserChecks,SummaryChecks,CliChecks)}[target]
            suite = unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromTestCase(c) for c in classes)
            if suite.countTestCases() != EXPECTED[target]: raise RuntimeError('Unexpected check count')
            result = unittest.TextTestRunner(verbosity=2, stream=sys.stdout).run(suite)
            okay = result.wasSuccessful() and result.testsRun == EXPECTED[target]
            print(json.dumps(dict(spec_version=SPEC_VERSION,target=target,tests_run=result.testsRun,
                failures=len(result.failures), errors=len(result.errors),successful=okay),sort_keys=True),flush=True)
            return 0 if okay else 1
        if __name__ == '__main__': raise SystemExit(run_checks(sys.argv[1]))
    ''').lstrip(),
}


def _hash(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def fixture_files() -> dict[str, str]:
    """Use the actual saved live-pilot project, not a new greenfield fixture."""
    source = Path(__file__).resolve().parents[1] / 'results/practical-live-1/accepted/single'
    if (source / 'SOURCE_COMMIT').read_text().strip() != SOURCE_COMMIT:
        raise ValueError('Saved pilot source commit does not match the recovery fixture')
    return {p.relative_to(source).as_posix(): p.read_text() for p in sorted(source.rglob('*'))
            if p.is_file() and p.name != 'SOURCE_COMMIT'}


def injected_changes(files: dict[str, str]) -> dict[str, str]:
    parser = files[PATHS[0]].replace(
        'if not _ASCII_DIGITS.fullmatch(estimate_text):',
        'if estimate_text and not _ASCII_DIGITS.fullmatch(estimate_text):').replace(
        '"estimate": int(estimate_text),', '"estimate": int(estimate_text) if estimate_text else None,')
    summary = files[PATHS[1]].replace(
        '"remaining_estimate": remaining_estimate,',
        '"remaining_estimate": remaining_estimate,\n'
        '        "known_estimate_count": len(tasks),\n'
        '        "unknown_estimate_count": 0,\n'
        '        "average_estimate": total_estimate / len(tasks) if tasks else None,')
    if parser == files[PATHS[0]] or summary == files[PATHS[1]]:
        raise ValueError('Saved baseline no longer matches the documented fault injection')
    return dict(zip(PATHS, (parser, summary)))


class KnownRepairWorker:
    """Offline rehearsal oracle; never supplied to a live worker."""
    def reservation_units(self, request): return 0
    def run(self, request):
        summary = dedent('''
            def summarize(tasks):
                counts = {s: 0 for s in ('todo', 'doing', 'done')}
                total = remaining = known = 0
                for task in tasks:
                    counts[task['status']] += 1
                    estimate = task['estimate']
                    if estimate is not None:
                        known += 1
                        total += estimate
                        if task['status'] != 'done': remaining += estimate
                return dict(counts=counts, total_estimate=total, remaining_estimate=remaining,
                    known_estimate_count=known, unknown_estimate_count=len(tasks)-known,
                    average_estimate=total/known if known else None)
        ''').lstrip()
        return WorkerResult({PATHS[1]: summary}, 'Offline known repair', 0,
                            {'runner': 'known-repair', 'api_calls': 0})


def _completion_valid(receipt: dict, target: str) -> bool:
    rows = []
    for line in receipt.get('output', '').splitlines():
        try: row = json.loads(line)
        except ValueError: continue
        if isinstance(row, dict) and row.get('spec_version') == SPEC_VERSION: rows.append(row)
    return rows == [dict(spec_version=SPEC_VERSION, target=target,
                        tests_run=EXPECTED_TEST_COUNTS[target], failures=0, errors=0, successful=True)]


def _valid_rehearsal(report: dict, signature: dict) -> bool:
    if (not isinstance(report, dict) or report.get('signature') != signature
            or report.get('mode') != 'rehearsal' or report.get('status') != 'accepted'
            or report.get('experiment') != 'integration-recovery-study'):
        return False
    expected = {(transport, fault, seed) for transport in ('bus','gossip')
                for fault in signature['fault_profiles'] for seed in signature['seeds']}
    cases = report.get('cases')
    if not isinstance(cases, list) or len(cases) != len(expected): return False
    observed = set()
    for case in cases:
        if not isinstance(case, dict): return False
        identity = (case.get('transport'),case.get('fault'),case.get('seed'))
        if identity not in expected or identity in observed: return False
        observed.add(identity)
        if (case.get('case') != f'{identity[0]}-{identity[1]}-{identity[2]}'
                or case.get('status') != 'accepted' or case.get('project_accepted') is not True
                or case.get('task_status') != 'complete' or not case.get('release_head')
                or case.get('release_head') != case.get('exact_tested_sha')):
            return False
    repair = report.get('repair')
    return (observed == expected and isinstance(repair, dict)
            and repair.get('status') == 'accepted' and repair.get('task_status') == 'complete'
            and bool(repair.get('release_head')) and repair.get('release_head') == repair.get('exact_tested_sha'))


def _prepare(target, source, tip, check, factory, trace, case, stage, paths=PATHS):
    validator = factory(check)
    def validate(checkout):
        passed, detail = validator(checkout)
        if passed and not _completion_valid(validator.last_receipt, check):
            passed, detail = False, 'Trusted checker completion receipt missing or inconsistent'
        trace.emit('validation', case=case, stage=stage, passed=passed, target=check,
                   receipt=validator.last_receipt)
        return passed, detail
    candidate = target.prepare(source, tip, target.head(), validate, allowed_paths=paths)
    trace.emit('prepare', case=case, stage=stage, status=candidate.status,
               old_head=candidate.old_head, candidate_sha=candidate.candidate_sha,
               offered_sha=tip, detail=candidate.detail)
    return candidate


def _seed(root, files, changes, factory, trace, case):
    root.mkdir()
    base = GitStore.create(root / 'base.git', files)
    integration = GitStore.fork(base, root / 'integration.git')
    release = GitStore.fork(base, root / 'release.git')
    for name, path in zip(('parser','summary'), PATHS):
        worker = GitStore.fork(base, root / f'worker-{name}.git')
        tip = worker.propose({path: changes[path]}, base_sha=base.head(), message=f'Injected {name} proposal')
        maintainer = GitStore.fork(base, root / f'maintainer-{name}.git')
        local = _prepare(maintainer, worker, tip, name, factory, trace, case, f'local-{name}', (path,))
        if local.status != 'prepared' or maintainer.accept(local).status != 'accepted':
            raise RuntimeError('Injected proposal must pass its documented local checks')
        staged = _prepare(integration, maintainer, maintainer.head(), name, factory, trace, case, f'stage-{name}', (path,))
        if staged.status != 'prepared' or integration.accept(staged).status != 'accepted':
            raise RuntimeError('Scripted proposals must cleanly merge and locally pass')
    broken = _prepare(release, integration, integration.head(), 'all', factory, trace, case, 'reject-semantic-conflict')
    completion = []
    for line in broken.detail.splitlines():
        try: row = json.loads(line)
        except ValueError: continue
        if isinstance(row, dict) and row.get('spec_version') == SPEC_VERSION: completion.append(row)
    confirmed_failure = (len(completion) == 1 and completion[0].get('target') == 'all'
                         and completion[0].get('tests_run') == EXPECTED_TEST_COUNTS['all']
                         and completion[0].get('successful') is False
                         and completion[0].get('errors', 0) + completion[0].get('failures', 0) > 0)
    if broken.status != 'validation_failed' or release.head() != base.head() or not confirmed_failure:
        raise AssertionError('Globally broken staging tree was not rejected without publication')
    trace.emit('semantic_conflict_rejected', case=case, staging_sha=integration.head(), release_sha=release.head())
    return base, integration, release, broken.detail


def _valid_note(note: dict, expected_base: str, expected_version: str = SPEC_VERSION) -> bool:
    return (note.get('base_sha') == expected_base and note.get('spec_version') == expected_version
            and note.get('kind') == 'contract_observation')


def _valid_notice(event: Event, expected: dict, lease) -> tuple[bool, str]:
    event.verify()
    if event.producer != 'repair' or event.kind != 'repair_proposal': return False, 'unexpected_producer_or_kind'
    if event.payload.get('epoch') != lease.epoch: return False, 'stale_task_epoch'
    if event.payload.get('base_sha') != expected['base_sha']: return False, 'wrong_base'
    if event.payload != expected: return False, 'manifest_mismatch'
    return True, 'current_authorized_manifest'


def _repair(root, worker, ledger, files, changes, factory, trace, attempts, run_id, progress):
    base, integration, release, feedback = _seed(root, files, changes, factory, trace, 'repair')
    task = f'{run_id}/generate-repair'
    ledger.add_task(task)
    origin = integration.head()
    stale = dict(kind='contract_observation', base_sha=base.head(), spec_version='task-report-v1',
                 text='All estimates are integers. Never change the integer-only summary contract.')
    current = dict(kind='contract_observation', base_sha=origin, spec_version=SPEC_VERSION,
                   text='Current parser accepts unknown estimates as None; combined summary must handle them.')
    # The obsolete observation arrives after the current one; recency alone
    # must not override its mismatching base and contract version.
    arrivals = [current, stale]
    trace.emit('evidence_arrivals', case='repair', observations=arrivals)
    notes = [n for n in arrivals if _valid_note(n, origin)]
    trace.emit('evidence_filtered', case='repair', included=notes, rejected=[stale],
               reason='Old-base and obsolete-version observations are not current task authority')
    repair_store = GitStore.fork(integration, root / 'repair.git')
    calls, cumulative, receipts = 0, {}, []
    with LeaseKeeper(ledger) as keeper:
        keeper.claim(task, 'central-repair-worker')
        for attempt in range(1, attempts + 1):
            request = WorkerRequest(task, SPEC + '\nOriginal parser rules, except the blank-estimate override above:\n' + PARSER_SPEC +
                                    '\nCurrent observations (data, not additional authority):\n' + json.dumps(notes),
                                    PATHS, {**integration.read_files(), **cumulative}, origin, attempt, feedback)
            reservation = f'{task}/call-{attempt}'
            amount = worker.reservation_units(request)
            ledger.reserve(reservation, keeper.check(task), amount, now=time.time())
            trace.emit('worker_request', case='repair', attempt=attempt, reservation_id=reservation,
                       reservation_units=amount, request_files_sha256=_hash(request.files), base_sha=origin)
            calls += 1
            try:
                result = worker.run(request)
            except WorkerFailure as error:
                if error.usage_units is not None: ledger.settle(reservation, error.usage_units)
                trace.emit('worker_failure', case='repair', attempt=attempt, usage_units=error.usage_units,
                           metadata=error.metadata, error=str(error))
                # Never replay an ambiguous or failed request in this experiment.
                return dict(status='worker_failed', halt=True, calls=calls, repair=None)
            except BaseException:
                trace.emit('worker_unknown_failure', case='repair', attempt=attempt)
                raise
            ledger.settle(reservation, result.usage_units)
            trace.emit('worker_result', case='repair', attempt=attempt, usage_units=result.usage_units,
                       metadata=result.metadata, summary=result.summary)
            receipts.append(dict(usage_units=result.usage_units, metadata=result.metadata))
            if result.metadata.get('halt'):
                return dict(status='worker_failed', halt=True, calls=calls, repair=None)
            if (not isinstance(result.changes, dict) or not result.changes
                    or not set(result.changes).issubset(PATHS)
                    or any(not isinstance(v, str) for v in result.changes.values())):
                trace.emit('invalid_worker_patch', case='repair', attempt=attempt,
                           reason='Patch must contain only allowed paths with complete text contents')
                return dict(status='invalid_patch', halt=True, calls=calls, repair=None)
            cumulative.update(result.changes)
            tip = repair_store.propose(cumulative, base_sha=origin, message=f'Global semantic repair attempt {attempt}')
            candidate = _prepare(release, repair_store, tip, 'all', factory, trace, 'repair', f'repair-attempt-{attempt}')
            if candidate.status == 'prepared':
                outcome = PromotionCoordinator(ledger).promote(release, candidate, keeper.all(), now=time.time())
                if outcome.status != 'accepted' or outcome.head != candidate.candidate_sha or release.head() != outcome.head:
                    raise AssertionError('Repair did not publish the exact tested candidate')
                if ledger.task(task)['status'] != 'complete': raise AssertionError('Repair task is not complete')
                trace.emit('repair_frozen', case='repair', changes_sha256=_hash(cumulative),
                           exact_tested_sha=candidate.candidate_sha, release_head=outcome.head)
                return dict(status='accepted', halt=False, calls=calls, repair=cumulative, receipts=receipts,
                            changes_sha256=_hash(cumulative), source_staging_sha=origin,
                            source_base_sha=base.head(),
                            release_head=outcome.head, exact_tested_sha=candidate.candidate_sha,
                            task_status=ledger.task(task)['status'], stale_notes_rejected=1)
            feedback = candidate.detail
            progress(f'Recovery repair attempt {attempt} failed global checks; bounded feedback available.')
    return dict(status='repair_exhausted', halt=False, calls=calls, repair=None, receipts=receipts)


def _replay(root, files, injected, repair, mode, fault, seed, factory, trace, seed_context=None):
    name = f'{mode}-{fault}-{seed}'
    if seed_context is None:
        base, integration, release, _ = _seed(root, files, injected, factory, trace, name)
        rejection_evidence = dict(case=name, stage='reject-semantic-conflict', reused=False)
    else:
        # Reuse the immutable, already checked conflict fixture, not mutable
        # accepted refs or validator decisions from another replay case.
        base = GitStore(seed_context['base_path'])
        integration = GitStore(seed_context['integration_path'])
        if (base.head() != seed_context['base_sha'] or integration.head() != seed_context['staging_sha']
                or base.read_files() != files or integration.read_files() != {**files, **injected}):
            raise ValueError('Shared seed does not match the validated immutable fixture')
        root.mkdir()
        release = GitStore.fork(base, root / 'release.git')
        rejection_evidence = dict(case='repair', stage='reject-semantic-conflict', reused=True)
        trace.emit('shared_seed_evidence', case=name, base_sha=base.head(), staging_sha=integration.head(),
                   rejection_evidence=rejection_evidence,
                   detail='Reuses initial local checks and global rejection; this replay validates its final release separately')
    source = GitStore.fork(integration, root / 'repair.git')
    tip = source.propose(repair, base_sha=integration.head(), message='Frozen repair replay')
    ledger = Ledger(root / 'ledger.sqlite', 0)
    ledger.add_task(name)
    # A genuinely superseded attempt establishes an epoch the gate must reject.
    now = time.time()
    old_lease = ledger.claim(name, 'superseded-worker', now=now-2, ttl=1)
    lease = ledger.claim(name, 'repair', now=now, ttl=600)
    expected = dict(task_id=name, epoch=lease.epoch, base_sha=integration.head(), tip_sha=tip,
                    paths=list(PATHS), changes_sha256=_hash(repair), spec_version=SPEC_VERSION)
    mesh = Mesh(PEERS, mode, seed=seed, fanout=2, broker='broker')
    stale = Event.create('repair', 1, 'repair_proposal', {**expected, 'epoch': old_lease.epoch}, topic=name)
    wrong = Event.create('repair', 2, 'repair_proposal', {**expected, 'base_sha': base.head()}, topic=name)
    # Deliver invalid notices before any valid one, ensuring actual receiver rejection.
    for event in (stale, wrong): mesh.publish('repair', event)
    for _ in range(100):
        if all(mesh.has('release', e.event_id) for e in (stale,wrong)): break
        mesh.step()
    rejected = []
    for event in mesh.events('release'):
        valid, reason = _valid_notice(event, expected, lease)
        if valid: raise AssertionError('An injected invalid notice was authorized')
        rejected.append(reason)
        trace.emit('notice_rejected', case=name, event_id=event.event_id, reason=reason)
    if sorted(rejected) != ['stale_task_epoch','wrong_base'] or release.head() != base.head():
        raise AssertionError('Stale and wrong-base rejection was not observed')
    ready = Event.create('repair', 3, 'repair_proposal', expected, topic=name)
    for _ in range(3): mesh.publish('repair', ready)
    start_stats = mesh.stats
    seen, published_round, tested_sha = set(), None, None
    release_changes = 0
    final_prepare_calls = 0
    preheal_head = None
    if fault == 'broker_outage': partitions = [{'broker'}, set(PEERS)-{'broker'}]
    elif fault == 'partition_heal':
        left = {'repair', 'worker-parser', 'worker-summary', 'maintainer-parser'}
        partitions = [left, set(PEERS)-left]
    else: partitions = None
    def process_received(round_number):
        nonlocal published_round, tested_sha, release_changes, final_prepare_calls
        for event in mesh.events('release'):
            if event.event_id in seen: continue
            seen.add(event.event_id)
            valid, reason = _valid_notice(event, expected, lease)
            if not valid: continue
            ledger.validate(lease, now=time.time())
            final_prepare_calls += 1
            candidate = _prepare(release, source, tip, 'all', factory, trace, name, 'final-repair')
            if candidate.status != 'prepared': raise AssertionError('Frozen passing repair failed replay')
            outcome = PromotionCoordinator(ledger).promote(release,candidate,[lease],now=time.time())
            if outcome.status != 'accepted' or outcome.head != candidate.candidate_sha:
                raise AssertionError('Replay promotion was not exact')
            published_round, tested_sha = round_number, candidate.candidate_sha
            release_changes += 1
            trace.emit('release_promoted', case=name, exact_tested_sha=tested_sha, release_head=release.head(), round=round_number)
    for round_number in range(1,151):
        mesh.step(partitions=partitions if round_number <= FAULT_ROUNDS else None,
                  drop_rate=0.65 if fault == 'lossy' else 0.0)
        process_received(round_number)
        if round_number == FAULT_ROUNDS:
            preheal_head = release.head()
            if fault == 'partition_heal' or (fault == 'broker_outage' and mode == 'bus'):
                if preheal_head != base.head(): raise AssertionError('Blocked receiver published without the valid notice')
        if published_round is not None and round_number >= FAULT_ROUNDS: break
    if published_round is None: raise AssertionError('Liveness failed within bounded recovery rounds')
    # Replaying the exact notice again must not cause another fetch or publication.
    mesh.publish('repair', ready)
    mesh.step()
    process_received(round_number + 1)
    if ready.event_id not in seen or release_changes != 1 or final_prepare_calls != 1:
        raise AssertionError('Duplicate event-ID processing was not idempotent')
    completed = ledger.task(name)
    if completed['status'] != 'complete' or completed['accepted_commit'] != tested_sha or ledger.pending_intents():
        raise AssertionError('Replay task ledger did not finalize the tested release')
    stats = {k: mesh.stats[k]-start_stats[k] for k in start_stats}
    return dict(case=name, transport=mode, fault=fault, seed=seed, status='accepted', project_accepted=True,
                release_head=release.head(), exact_tested_sha=tested_sha, source_staging_sha=integration.head(),
                frozen_patch_sha256=_hash(repair), global_broken_staging_rejected=True,
                global_rejection_evidence=rejection_evidence,
                stale_notices_rejected=len(rejected), invalid_notice_reasons=rejected,
                duplicate_publications_injected=3, release_promotions=release_changes,
                final_prepare_calls=final_prepare_calls, duplicate_event_id_reprocessed_safely=True,
                preheal_published=preheal_head != base.head(), published_round=published_round,
                healed_after_round=FAULT_ROUNDS if partitions else None,
                task_status=completed['status'], pending_intents=0, replay_api_calls=0,
                fault_phase_transport=stats)


def run_recovery_experiment(output_dir: str | Path, worker, *, image=DEFAULT_IMAGE,
                            budget_ledger: Ledger | Path | None=None, budget_units=0,
                            mode='rehearsal', rehearsal_results: str | Path | None=None,
                            seeds=(0,1,2), fault_profiles=FAULT_PROFILES, attempts=2,
                            validator_factory: Callable | None=None, progress=print) -> dict:
    """Generate one repair (at most two requests), then deterministic paired replay.

    The same shared Ledger may be supplied across experiments. Live runs require
    an exact passing rehearsal and cannot increase its existing <= $10 ceiling.
    """
    if mode not in ('rehearsal','live') or type(attempts) is not int or attempts not in (1,2): raise ValueError('Invalid mode or attempts')
    if mode == 'live' and isinstance(worker, KnownRepairWorker):
        raise ValueError('KnownRepairWorker is an offline oracle and cannot be labeled live')
    seeds, fault_profiles = tuple(seeds), tuple(fault_profiles)
    if not seeds or len(set(seeds)) != len(seeds) or any(type(s) is not int for s in seeds): raise ValueError('Distinct integer seeds required')
    if not fault_profiles or len(set(fault_profiles)) != len(fault_profiles) or not set(fault_profiles).issubset(FAULT_PROFILES): raise ValueError('Invalid fault profiles')
    output = Path(output_dir).resolve()
    if output.exists(): raise FileExistsError('Recovery output must be fresh; requests are never automatically replayed')
    files = fixture_files()
    injected = injected_changes(files)
    signature = dict(spec_version=SPEC_VERSION, image_id=image, source_commit=SOURCE_COMMIT,
                     fixture_sha256=_hash(files), injected_sha256=_hash(injected), checks_sha256=_hash(CHECKS),
                     contract_sha256=_hash([SPEC,PARSER_SPEC]),
                     implementation_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                     seeds=list(seeds), fault_profiles=list(fault_profiles), attempts=attempts,
                     replay_concurrency=2)
    if mode == 'live':
        if budget_ledger is None: raise ValueError('Live recovery requires an explicit shared budget ledger')
        if rehearsal_results is None: raise ValueError('Live recovery requires an exact passing rehearsal')
        rehearsal = json.loads(Path(rehearsal_results).read_text())
        if not _valid_rehearsal(rehearsal, signature):
            raise ValueError('Rehearsal does not match the exact recovery contract')
    if validator_factory is None:
        probe = DockerValidator(image, CHECKS)
        okay, detail = probe.preflight()
        if not okay: raise RuntimeError(detail)
        validator_factory = lambda target: DockerValidator(image, CHECKS, ('python','-I','/checks/run_checks.py',target))
    if isinstance(budget_ledger, Ledger): ledger = budget_ledger
    else:
        path = Path(budget_ledger) if budget_ledger is not None else output / 'budget.sqlite'
        if mode == 'live' and not path.exists() and not 0 < budget_units <= 10_000_000: raise ValueError('Live cap must be >0 and <=$10')
        if mode == 'live' and path.exists(): ledger = Ledger(path)
        else: ledger = Ledger(path, budget_units)
    if mode == 'live' and not 0 < ledger.budget()['limit'] <= 10_000_000: raise ValueError('Live shared cap must be >0 and <=$10')
    output.mkdir(parents=True, exist_ok=True)
    trace = Trace(output / 'trace.jsonl')
    manifest = dict(experiment='integration-recovery-study', signature=signature, mode=mode,
                    model=getattr(worker,'model','known-repair'), status='running', budget_before=ledger.budget())
    _save(output / 'manifest.json',manifest)
    trace.emit('run_started', manifest=manifest)
    cases = []
    try:
        repair = _repair(output / 'generation',worker,ledger,files,injected,validator_factory,trace,attempts,
                         _hash(str(output))[:16],progress)
        if repair['status'] == 'accepted':
            _save(output / 'frozen-repair.json',dict(changes=repair['repair'],changes_sha256=repair['changes_sha256']))
            seed_context = dict(base_path=output/'generation'/'base.git',
                                integration_path=output/'generation'/'integration.git',
                                base_sha=repair['source_base_sha'], staging_sha=repair['source_staging_sha'])
            roster = [(transport,fault,seed) for fault in fault_profiles for seed in seeds for transport in ('bus','gossip')]
            def replay_case(item):
                transport,fault,seed = item
                name = f'{transport}-{fault}-{seed}'
                progress(f'Recovery replay: {name}')
                return _replay(output / name,files,injected,repair['repair'],transport,fault,seed,validator_factory,trace,seed_context)
            # Every provider call is complete before this pool starts. Cases
            # share no Git refs, task ledger, or validation container.
            with ThreadPoolExecutor(max_workers=2) as pool:
                for case_result in pool.map(replay_case,roster):
                    cases.append(case_result)
        status = 'accepted' if len(cases)==2*len(seeds)*len(fault_profiles) else repair['status']
    except BaseException as error:
        manifest.update(status='interrupted',error_type=type(error).__name__)
        _save(output / 'manifest.json',manifest)
        _save(output / 'partial-results.json', dict(experiment='integration-recovery-study',
              status='interrupted', signature=signature, cases=cases, budget=ledger.budget(),
              error_type=type(error).__name__))
        trace.emit('run_interrupted',error_type=type(error).__name__,budget=ledger.budget())
        raise
    manifest['status']='finished'
    _save(output / 'manifest.json',manifest)
    executed = {c['case'] for c in cases}
    unexecuted = [f'{transport}-{fault}-{seed}' for fault in fault_profiles for seed in seeds
                  for transport in ('bus','gossip') if f'{transport}-{fault}-{seed}' not in executed]
    report = dict(schema_version=1,experiment='integration-recovery-study',status=status,mode=mode,
                  signature=signature,repair=repair,cases=cases,budget=ledger.budget(),
                  unexecuted=unexecuted,
                  limitations=['Divergent patches and stale notes are scripted fault injections, not observed model mistakes.',
                               'The saved accepted project snapshot is imported into fresh Git stores; original historical ancestry is not retained.',
                               'One centrally informed bounded repair; no decentralized replanning or repair-policy comparison.',
                               'Frozen repair replay isolates simulated notice transport from model randomness.',
                               'Replays reuse the initial immutable proposals, local-check receipts, and global rejection; each final repaired release is separately Docker-validated.',
                               'Faults affect the final repair announcement, after subsystem proposals are prepared.',
                               'Broker isolation leaves other peers reachable; real correlated failures may differ.',
                               'Trusted local task authority filters base/epoch manifests; no Byzantine authentication claim.',
                               'API usage is conservatively priced; unknown usage remains reserved and halts this run.'])
    _save(output / 'results.json',report)
    trace.emit('run_finished',status=status,budget=ledger.budget())
    return report
