"""Offline orchestration controls; no model requests or untrusted host execution."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import types
import unittest
from unittest.mock import Mock, patch

from gossip_harness.gitstore import GitStore
from gossip_harness.ledger import Ledger
from gossip_harness.pilot import Trace
from gossip_harness.swarm_experiment import (
    ARMS, HaltRun, _case, contract, model_contract, run_swarm_experiment,
    validate_rehearsal,
)
from gossip_harness.worker import MODEL, STRONG_MODEL, OpenAIWorker, WorkerFailure, WorkerResult


HIDDEN_MARKER = 'SEALED-HOLDOUT-LABEL-DO-NOT-SEND'
REFERENCE_MARKER = 'REFERENCE-SOURCE-DO-NOT-SEND'
GOOD = 'def solve(payload):\n    return payload  # fake-validator-good\n'
BAD = 'def solve(payload):\n    return None  # fake-validator-bad\n'


def miniature_task(index=0):
    def case(value):
        return dict(input={'n': value}, expected={'answer': value + 1}, requirement='r1')
    return types.SimpleNamespace(
        id=f'task-{index}', family=f'family-{index % 3}', spec='r1: Return the input n plus one as answer.',
        requirements=('r1',), initial_files={'solution.py': BAD, 'README.md': 'Public starter.'},
        known_solution=REFERENCE_MARKER, public_cases=(case(0),),
        hidden_cases=({**case(10000), 'requirement': HIDDEN_MARKER},),
        fuzz_cases=tuple(case(i) for i in range(1, 17)),
        oracle=lambda value: {'answer': value['n'] + 1},
    )


class ScriptedWorker:
    """Emits bounded text fixtures, never imports or runs candidate code."""
    def __init__(self, task, *, usage=10, invalid=False, overlap_fuzz=False):
        self.task, self.usage, self.invalid, self.overlap_fuzz = task, usage, invalid, overlap_fuzz
        self.requests = []
        self.lock = threading.Lock()

    def reservation_units(self, request):
        return 100

    def run(self, request):
        with self.lock:
            self.requests.append(request)
        role = request.task_id.split('/')[1]
        identifier = request.task_id.split('/')[2]
        if self.invalid:
            return WorkerResult({'solution.py': None, 'tests.json': '{invalid'}, 'Invalid fixture', self.usage, {})
        changes = {}
        if 'solution.py' in request.allowed_paths:
            bad = (role == 'pool' and identifier == '0') or (role in ('strong', 'sequential') and request.attempt == 1)
            changes['solution.py'] = BAD if bad else GOOD
        if 'tests.json' in request.allowed_paths:
            if role == 'reviewer':
                cases = [dict(input={'n': n}, expected={'answer': n + 1}, requirement='r1') for n in range(101, 107)]
            elif self.overlap_fuzz:
                cases = self.task.fuzz_cases[6:12]
            else:
                cases = self.task.fuzz_cases[:6]
            changes['tests.json'] = json.dumps(list(cases))
        return WorkerResult(changes, 'Offline scripted result', self.usage, {'api_calls': 0})


class RecordingValidator:
    def __init__(self, root, testcase, worker):
        self.root, self.testcase, self.worker = root, testcase, worker
        self.calls = []
        self.first_hidden_requests = None

    def evaluate(self, files, cases):
        self.calls.append(deepcopy(cases))
        if any(c.get('requirement') == HIDDEN_MARKER for c in cases):
            # The durable selection artifact, rather than an in-memory flag,
            # must already bind all arm winners and the complete candidate pool.
            frozen = json.loads((self.root / 'selection-frozen.json').read_text())
            self.testcase.assertEqual(set(frozen['selections']), set(ARMS))
            self.testcase.assertEqual(len(frozen['candidates']), 6)
            self.testcase.assertTrue(all(v['tip_sha'] for v in frozen['candidates'].values()))
            if self.first_hidden_requests is None:
                self.first_hidden_requests = len(self.worker.requests)
            self.testcase.assertEqual(len(self.worker.requests), self.first_hidden_requests)
        passed = 'fake-validator-good' in files['solution.py']
        outcomes = [dict(index=i, id=case.get('id'), passed=passed,
                         status='passed' if passed else 'wrong_answer',
                         actual=case['expected'] if passed else None)
                    for i, case in enumerate(cases)]
        return dict(passed=passed, status='passed' if passed else 'failed', outcomes=outcomes,
                    cleanup_verified=True, timed_out=False, runtime_seconds=0)


class SwarmControlTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='swarm-control-')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.tasks = tuple(miniature_task(i) for i in range(8))
        fixture = types.ModuleType('gossip_harness.swarm_fixture')
        fixture.TASKS = self.tasks
        fixture.fixture_signature = lambda: {'offline_test_roster': 'v1'}
        replacement = patch.dict('sys.modules', {'gossip_harness.swarm_fixture': fixture})
        replacement.start()
        self.addCleanup(replacement.stop)
        signature = patch('gossip_harness.swarm_experiment.source_signature', return_value={'offline-controller': 'v1'})
        signature.start()
        self.addCleanup(signature.stop)
        def forbidden_transport(*args):
            raise AssertionError('Control tests must never reach any HTTP transport')
        # Use the real pinned worker types and profile validation at the live
        # boundary. Only their provider-call methods are stubbed, and the dummy
        # credential plus forbidden transport are a second no-network guard.
        self.cheap = OpenAIWorker('offline-test-placeholder', model=MODEL,
                                  max_output_tokens=4096, transport=forbidden_transport)
        self.strong = OpenAIWorker('offline-test-placeholder', model=STRONG_MODEL,
                                   max_output_tokens=4096, transport=forbidden_transport)
        for worker in (self.cheap, self.strong):
            worker.reservation_units = Mock(side_effect=AssertionError('Unexpected reservation'))
            worker.run = Mock(side_effect=AssertionError('Unexpected provider request'))
        self.validator = Mock()
        self.validator.preflight.return_value = (True, 'offline test')
        self.factory = Mock(return_value=self.validator)

    def proof(self):
        from gossip_harness.pilot import DEFAULT_IMAGE
        expected = contract(self.tasks, DEFAULT_IMAGE)
        proof = dict(experiment='candidate-selection-v1', mode='rehearsal', status='finished',
                     repetitions=1, contract=expected, unexecuted=[], cases=[])
        for task in self.tasks:
            proof['cases'].append(dict(task_id=task.id, family=task.family, repetition=0,
                rehearsal_verified=True, matched_extra_test_counts=True,
                arms={arm: dict(accepted=True, selected='candidate-1', release_head='a' * 40,
                                exact_tested_sha='a' * 40, tested_files_sha256='b' * 64)
                      for arm in ARMS}))
        return proof

    def live(self, proof=None, **overrides):
        path = self.root / 'proof.json'
        if proof is not None:
            path.write_text(json.dumps(proof))
        options = dict(mode='live', budget_ledger=self.root / 'budget.sqlite', budget_units=10_000_000,
                       rehearsal_results=path if proof is not None else None)
        options.update(overrides)
        # Production still constructs its own validator and rejects the public
        # injection seam in live mode. This patch confines offline preflight
        # control to the unit-test process without weakening that boundary.
        with patch('gossip_harness.blackbox_validator.BlackboxValidator', self.factory):
            return run_swarm_experiment(self.root / 'run', self.cheap, self.strong, **options)

    def assert_no_requests(self):
        for worker in (self.cheap, self.strong):
            worker.reservation_units.assert_not_called()
            worker.run.assert_not_called()

    def test_live_requires_exact_rehearsal_before_preflight_or_reservation(self):
        with self.assertRaisesRegex(ValueError, 'rehearsal'):
            self.live()
        for defect in ('duplicate', 'missing', 'unfinished', 'wrong-model', 'failed-case'):
            proof = self.proof()
            if defect == 'duplicate':
                proof['cases'][-1] = deepcopy(proof['cases'][0])
            elif defect == 'missing':
                proof['cases'].pop()
            elif defect == 'unfinished':
                proof['status'] = 'interrupted'
            elif defect == 'wrong-model':
                proof['contract']['models']['strong']['model'] = 'not-the-frozen-model'
            else:
                proof['cases'][0]['rehearsal_verified'] = False
            with self.subTest(defect=defect), self.assertRaises(ValueError):
                self.live(proof)
        self.factory.assert_not_called()
        self.assert_no_requests()
        self.assertFalse((self.root / 'budget.sqlite').exists())

    def test_rehearsal_rejects_missing_arm_or_false_release_evidence(self):
        for defect in ('missing-arm', 'failed-arm', 'mismatched-sha', 'unmatched-tests'):
            proof = self.proof()
            row = proof['cases'][0]
            if defect == 'missing-arm':
                row['arms'].pop('pool-reviewer')
            elif defect == 'failed-arm':
                row['arms']['pool-reviewer']['accepted'] = False
            elif defect == 'mismatched-sha':
                row['arms']['pool-reviewer']['exact_tested_sha'] = 'c' * 40
            else:
                row['matched_extra_test_counts'] = False
            with self.subTest(defect=defect), self.assertRaises(ValueError):
                validate_rehearsal(proof, self.proof()['contract'])

    def test_live_rejects_fake_workers_and_custom_validator_injection(self):
        with self.assertRaisesRegex(ValueError, 'pinned model profiles'):
            self.live(self.proof(), validator_factory=self.factory)
        original = self.cheap
        self.cheap = Mock()
        self.cheap.profile_manifest.return_value = model_contract()['cheap']
        try:
            with self.assertRaisesRegex(ValueError, 'pinned model profiles'):
                self.live(self.proof())
            self.assert_no_requests()
        finally:
            self.cheap = original
        self.factory.assert_not_called()
        self.assert_no_requests()
        self.assertFalse((self.root / 'run').exists())
        self.assertFalse((self.root / 'budget.sqlite').exists())

    def test_failed_preflight_and_invalid_bounds_create_no_run(self):
        for repetitions in (0, 3, True):
            with self.subTest(repetitions=repetitions), self.assertRaises(ValueError):
                self.live(self.proof(), repetitions=repetitions)
        for budget in (True, 10.5, 0, 50_000_001):
            with self.subTest(budget=budget), self.assertRaises(ValueError):
                self.live(self.proof(), budget_units=budget)
        self.validator.preflight.return_value = (False, 'image unavailable')
        with self.assertRaisesRegex(RuntimeError, 'preflight'):
            self.live(self.proof())
        self.assert_no_requests()
        self.assertFalse((self.root / 'run').exists())
        self.assertFalse((self.root / 'budget.sqlite').exists())

    def test_two_repetitions_execute_exact_eight_task_roster(self):
        def stub_case(root, task, repetition, *args, **kwargs):
            return dict(task_id=task.id, repetition=repetition)
        with patch('gossip_harness.swarm_experiment._case', side_effect=stub_case) as case:
            report = self.live(self.proof(), repetitions=2)
        self.assertEqual(report['status'], 'finished')
        self.assertEqual(report['unexecuted'], [])
        self.assertEqual(case.call_count, 16)
        self.assertEqual({(r['task_id'], r['repetition']) for r in report['cases']},
                         {(t.id, repeat) for t in self.tasks for repeat in (0, 1)})
        self.assert_no_requests()

    def test_unknown_usage_preserves_reservation_and_stops_remaining_tasks(self):
        ledger = Ledger(self.root / 'budget.sqlite', budget_units=10_000_000)
        ledger.add_task('earlier-study')
        lease = ledger.claim('earlier-study', 'old-worker', now=0, ttl=10)
        ledger.reserve('earlier-call', lease, 2000, now=1)
        ledger.settle('earlier-call', 1000)
        self.strong.reservation_units.return_value = 100
        self.strong.reservation_units.side_effect = None
        self.strong.run.side_effect = WorkerFailure('Ambiguous offline provider receipt', None, {'halt': True})
        with self.assertRaises(HaltRun):
            self.live(self.proof())
        self.strong.run.assert_called_once()
        self.cheap.run.assert_not_called()
        report = json.loads((self.root / 'run' / 'results.json').read_text())
        self.assertEqual(report['status'], 'interrupted')
        self.assertEqual(report['budget_before']['spent_or_reserved'], 1000)
        self.assertEqual(report['budget']['spent_or_reserved'], 1100)
        self.assertEqual(len(report['unexecuted']), 16)
        self.assertFalse((self.root / 'run' / 'repeat-1').exists())
        with sqlite3.connect(self.root / 'budget.sqlite') as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM reservations WHERE state='reserved'").fetchone()[0], 1)

    def test_frozen_pool_cost_attribution_holdout_boundary_and_exact_publication(self):
        task = self.tasks[0]
        root = self.root / 'case'
        worker = ScriptedWorker(task, overlap_fuzz=True)
        validator = RecordingValidator(root, self, worker)
        ledger = Ledger(self.root / 'case-budget.sqlite', budget_units=10_000)
        result = _case(root, task, 0, worker, worker, ledger, validator,
                       Trace(self.root / 'case-trace.jsonl'), 'offline-case', mode='rehearsal', progress=lambda _: None)
        self.assertTrue(result['rehearsal_verified'])
        self.assertTrue(result['matched_extra_test_counts'])
        serialized = json.dumps([asdict(request) for request in worker.requests])
        self.assertNotIn(HIDDEN_MARKER, serialized)
        self.assertNotIn(REFERENCE_MARKER, serialized)
        self.assertEqual(len(worker.requests), 10)  # 2 + 2 baselines, four pool, blind, reviewer.
        self.assertEqual(result['actual_micro_usd'], 100)
        self.assertEqual(ledger.budget()['spent_or_reserved'], 100)
        self.assertEqual(result['arms']['pool-fixed']['attributed_micro_usd'], 50)
        self.assertEqual(result['arms']['pool-fuzz']['attributed_micro_usd'], 50)
        self.assertEqual(result['arms']['pool-reviewer']['attributed_micro_usd'], 60)
        evidence = json.loads((root / 'selection-evidence.json').read_text())
        self.assertEqual(len(evidence['candidate_commits']), 4)
        self.assertEqual(evidence['fixed']['candidate_order'], evidence['reviewer']['candidate_order'])
        self.assertEqual(evidence['fixed']['candidate_order'], evidence['fuzz']['candidate_order'])
        for arm in result['arms'].values():
            self.assertTrue(arm['accepted'])
            self.assertEqual(arm['release_head'], arm['exact_tested_sha'])
            self.assertEqual(GitStore(arm['release_path']).head(), arm['exact_tested_sha'])
        trace = [json.loads(line) for line in (self.root / 'case-trace.jsonl').read_text().splitlines()]
        frozen = next(i for i, row in enumerate(trace) if row['kind'] == 'selection_frozen')
        hidden = [i for i, row in enumerate(trace) if row['kind'] == 'validation' and row['stage'] == 'heldout']
        self.assertEqual(len(hidden), 6)
        self.assertTrue(all(i > frozen for i in hidden))

    def test_infrastructure_failure_is_not_recorded_as_model_incorrectness(self):
        task = self.tasks[0]
        worker = ScriptedWorker(task)
        validator = Mock()
        validator.evaluate.return_value = dict(passed=False, status='infraerror', outcomes=[], cleanup_verified=True)
        with self.assertRaises(HaltRun):
            _case(self.root / 'infra', task, 0, worker, worker,
                  Ledger(self.root / 'infra-budget.sqlite', budget_units=10_000), validator,
                  Trace(self.root / 'infra-trace.jsonl'), 'infra', mode='rehearsal', progress=lambda _: None)
        self.assertEqual(len(worker.requests), 1)

    def test_invalid_model_output_is_retained_as_failure_without_retry_loop(self):
        task = self.tasks[0]
        root = self.root / 'invalid'
        worker = ScriptedWorker(task, invalid=True)
        validator = RecordingValidator(root, self, worker)
        ledger = Ledger(self.root / 'invalid-budget.sqlite', budget_units=10_000)
        store = Mock()
        store.path = self.root / 'stub.git'
        store.head.return_value = 'a' * 40
        store.propose.return_value = 'b' * 40
        # Git mechanics are covered by the real-Git case above; this probe
        # isolates invalid-output control flow and its bounded invocation count.
        with patch('gossip_harness.swarm_experiment.GitStore') as git:
            git.create.return_value = store
            git.fork.return_value = store
            with patch('gossip_harness.swarm_experiment.PromotionCoordinator') as promotion:
                promotion.return_value.promote.return_value.status = 'accepted'
                result = _case(root, task, 0, worker, worker, ledger, validator,
                               Trace(self.root / 'invalid-trace.jsonl'), 'invalid',
                               mode='rehearsal', progress=lambda _: None)
        self.assertFalse(result['rehearsal_verified'])
        self.assertTrue(all(not arm['accepted'] for arm in result['arms'].values()))
        self.assertEqual(len(worker.requests), 8)
        self.assertEqual(result['actual_micro_usd'], 80)
        evidence = json.loads((root / 'selection-evidence.json').read_text())
        self.assertTrue(evidence['fixed']['abstained'])
        self.assertTrue(all(item['rejected'] for item in evidence['candidate_tests'].values()))
        self.assertEqual(len(list((root / 'requests').glob('*.result.json'))), 8)


if __name__ == '__main__':
    unittest.main()
