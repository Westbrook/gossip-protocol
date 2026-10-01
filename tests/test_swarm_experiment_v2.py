"""Fast v2 controller checks; fixtures never execute candidate source."""
from __future__ import annotations

from copy import deepcopy
from contextlib import contextmanager
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import Mock, patch

from devtools.validation_session import ResourceBudget, digest as validation_digest
from gossip_harness import swarm_experiment_v2 as swarm
from gossip_harness.ledger import Ledger
from gossip_harness.pilot import DEFAULT_IMAGE, Trace
from gossip_harness.worker import MODEL, STRONG_MODEL, OpenAIWorker, WorkerResult


GOOD = 'def solve(payload):\n    return payload  # fixture-good\n'
BAD = 'def solve(payload):\n    return None  # fixture-bad\n'
HIDDEN = 'SEALED-HIDDEN-DO-NOT-SEND'


def miniature_task(index=0):
    def case(value):
        return dict(input={'n': value}, expected={'answer': value + 1}, requirement='r1')
    return types.SimpleNamespace(
        id=f'task-{index}', family=f'family-{index % 3}', spec='r1: Return n plus one as answer.',
        requirements=('r1',), initial_files={'solution.py': BAD, 'README.md': 'Starter'},
        known_solution=GOOD, public_cases=(case(0),),
        hidden_cases=({**case(10000), 'requirement': HIDDEN},),
        fuzz_cases=tuple(case(i) for i in range(1, 17)),
        oracle=lambda value: {'answer': value['n'] + 1})


class FixtureWorker:
    def __init__(self, task):
        self.task = task
        self.requests = []

    def reservation_units(self, request):
        return 0

    def run(self, request):
        self.requests.append(request)
        role, identifier = request.task_id.split('/')[1:3]
        changes = {}
        if 'solution.py' in request.allowed_paths:
            bad = (role == 'pool' and identifier == '0') or (role in ('strong', 'sequential') and request.attempt == 1)
            changes['solution.py'] = BAD if bad else GOOD
        if 'tests.json' in request.allowed_paths:
            cases = self.task.fuzz_cases[6:12] if role == 'reviewer' else self.task.fuzz_cases[:6]
            changes['tests.json'] = json.dumps(list(cases))
        return WorkerResult(changes, 'Offline fixture', 0, {'api_calls': 0, 'runner': 'known-solution'})


class FixtureSession:
    def __init__(self, root, testcase, *, malformed=False, reused_final=False, misbound=False):
        self.root, self.testcase = root, testcase
        self.malformed, self.reused_final = malformed, reused_final
        self.misbound = misbound
        self.calls = []
        self.observations = []

    def raise_if_cancelled(self):
        pass

    def evaluate_many(self, requests, cases, *, purpose, reason):
        self.calls.append(dict(requests=deepcopy(requests), cases=deepcopy(cases),
                               purpose=purpose, reason=reason))
        self.testcase.assertTrue(all(set(case) <= {'input', 'expected', 'id', 'requirement'} for case in cases))
        if purpose == 'final':
            frozen = json.loads((self.root / 'selection-frozen.json').read_text())
            self.testcase.assertEqual(set(frozen['selections']), set(swarm.ARMS))
            self.testcase.assertEqual(len(frozen['candidates']), 6)
            self.testcase.assertTrue(reason)
        envelopes = []
        for request in requests:
            passed = 'fixture-good' in request['files']['solution.py']
            receipt = dict(passed=passed, status='passed' if passed else 'failed', cleanup_verified=True,
                           outcomes=[dict(index=i, passed=passed, status='passed' if passed else 'wrong_answer',
                                          actual=case['expected'] if passed else None)
                                     for i, case in enumerate(cases)])
            if self.malformed:
                receipt['cleanup_verified'] = False
            reused = purpose == 'final' and self.reused_final
            validation = dict(
                physical=not reused, execution_id='offline-' + request['label'],
                source_sha256='0' * 64 if self.misbound else validation_digest(request['files']),
                suite_sha256=validation_digest(cases),
                purpose=purpose, session_id='offline', logical_index=len(self.observations),
                reuse={'kind': 'persisted'} if reused else None)
            self.observations.append(dict(session_id='offline', logical_index=len(self.observations),
                                          artifact_path=str(self.root),
                                          job=dict(files=deepcopy(request['files']), cases=deepcopy(cases),
                                                   purpose=purpose, reason=reason),
                                          result={**validation, 'receipt': receipt}))
            envelopes.append(dict(receipt=receipt, validation=validation))
        return envelopes


class FixtureGit:
    """Materialize trusted text for checkout callbacks; do not invoke Git or code."""
    stores = {}

    def __new__(cls, path):
        return cls.stores[str(path)]

    @classmethod
    def create(cls, path, files):
        item = object.__new__(cls)
        item.path = Path(path)
        item.path.mkdir()
        item.current = hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest()
        item.trees = {item.current: dict(files)}
        cls.stores[str(path)] = item
        return item

    @classmethod
    def fork(cls, base, path):
        return cls.create(path, base.trees[base.current])

    def head(self):
        return self.current

    def propose(self, changes, base_sha=None, message=None):
        files = {**self.trees[base_sha or self.current], **changes}
        sha = hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest()
        self.trees[sha] = files
        return sha

    def prepare(self, source, tip, head, verify, allowed_paths):
        files = source.trees[tip]
        changed = {key for key in files if files[key] != self.trees[head].get(key)}
        if not changed <= set(allowed_paths):
            raise AssertionError('Wrong publication path scope')
        checkout = self.path / 'checkout'
        checkout.mkdir()
        for name, content in files.items():
            destination = checkout / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_text(content)
        if not verify(checkout)[0]:
            raise AssertionError('Exact content verification failed')
        return types.SimpleNamespace(candidate_sha=tip, files=files)


class FixturePromotion:
    def __init__(self, ledger):
        pass

    def promote(self, store, prepared, leases, now):
        store.current = prepared.candidate_sha
        store.trees[store.current] = prepared.files
        return types.SimpleNamespace(status='accepted')


class SwarmV2Tests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='swarm-v2-control-')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.tasks = tuple(miniature_task(i) for i in range(8))
        FixtureGit.stores = {}

    def proof(self, expected=None):
        expected = expected or swarm.contract(self.tasks, DEFAULT_IMAGE)
        return dict(experiment=swarm.PROTOCOL, mode='rehearsal', status='finished',
                    repetitions=1, contract=expected, unexecuted=[], cases=[
                        dict(task_id=task.id, repetition=0, rehearsal_verified=True,
                             matched_extra_test_counts=True,
                             validation_counts={'heldout_physical': 6, 'heldout_reused': 0},
                             arms={arm: dict(accepted=True, release_head='a' * 40, exact_tested_sha='a' * 40)
                                   for arm in swarm.ARMS}) for task in self.tasks])

    def case(self, *, malformed=False, reused_final=False, misbound=False):
        task = self.tasks[0]
        worker = FixtureWorker(task)
        root = self.root / 'case'
        session = FixtureSession(root, self, malformed=malformed, reused_final=reused_final,
                                 misbound=misbound)
        ledger = Ledger(self.root / 'budget.sqlite', budget_units=0)
        with patch.object(swarm, 'GitStore', FixtureGit), patch.object(swarm, 'PromotionCoordinator', FixturePromotion):
            result = swarm._case(root, task, 0, worker, worker, ledger, session,
                                 Trace(self.root / 'trace.jsonl'), 'offline',
                                 mode='rehearsal', progress=lambda value: None)
        return result, worker, session

    def test_contract_binds_sources_resources_and_explicit_reuse_default(self):
        expected = swarm.contract(self.tasks, DEFAULT_IMAGE)
        self.assertEqual(expected['protocol'], 'candidate-selection-v2')
        self.assertIn('gossip_harness/swarm_experiment_v2.py', expected['sources'])
        self.assertIn('gossip_harness/gitstore.py', expected['sources'])
        self.assertIn('devtools/study_validation.py', expected['sources'])
        self.assertEqual(expected, swarm.contract(self.tasks, DEFAULT_IMAGE))
        self.assertNotEqual(expected, swarm.contract(self.tasks, DEFAULT_IMAGE,
                                                     budget=ResourceBudget(workers=1)))
        self.assertNotEqual(expected, swarm.contract(self.tasks, DEFAULT_IMAGE, deterministic_visible=True))

    def test_rehearsal_rejects_v1_and_changed_contract_before_execution(self):
        expected = swarm.contract(self.tasks, DEFAULT_IMAGE)
        swarm.validate_rehearsal(self.proof(expected), expected)
        for defect in ('v1', 'resource', 'missing', 'duplicate', 'unfinished'):
            proof = self.proof(expected)
            if defect == 'v1':
                proof['experiment'] = 'candidate-selection-v1'
            elif defect == 'resource':
                proof['contract'] = swarm.contract(self.tasks, DEFAULT_IMAGE, budget=ResourceBudget(workers=1))
            elif defect == 'missing':
                proof['cases'].pop()
            elif defect == 'duplicate':
                proof['cases'][-1] = deepcopy(proof['cases'][0])
            else:
                proof['status'] = 'interrupted'
            with self.subTest(defect=defect), self.assertRaises(ValueError):
                swarm.validate_rehearsal(proof, expected)

    def test_rehearsal_requires_every_arm_exact_publication_and_physical_holdout(self):
        expected = swarm.contract(self.tasks, DEFAULT_IMAGE)
        for defect in ('missing-arm', 'failed-arm', 'wrong-tree', 'unmatched-cases', 'reused-final', 'missing-final'):
            proof = self.proof(expected)
            case = proof['cases'][0]
            if defect == 'missing-arm':
                case['arms'].pop('pool-fixed')
            elif defect == 'failed-arm':
                case['arms']['pool-fixed']['accepted'] = False
            elif defect == 'wrong-tree':
                case['arms']['pool-fixed']['exact_tested_sha'] = 'b' * 40
            elif defect == 'unmatched-cases':
                case['matched_extra_test_counts'] = False
            elif defect == 'reused-final':
                case['validation_counts']['heldout_reused'] = 1
            else:
                case['validation_counts']['heldout_physical'] = 5
            with self.subTest(defect=defect), self.assertRaises(ValueError):
                swarm.validate_rehearsal(proof, expected)

    def test_matrices_batch_preserve_repairs_holdout_barrier_and_exact_trees(self):
        result, worker, session = self.case()
        self.assertTrue(all(arm['accepted'] for arm in result['arms'].values()))
        self.assertEqual([len(call['requests']) for call in session.calls], [1, 1, 1, 1, 4, 4, 6])
        self.assertEqual([call['purpose'] for call in session.calls], ['visible'] * 6 + ['final'])
        self.assertTrue(all('heldout' in row['label'] for row in session.calls[-1]['requests']))
        self.assertEqual(result['validation_counts'], dict(logical=18, physical=18, reused=0,
                                                          heldout_physical=6, heldout_reused=0))
        for role in ('strong', 'sequential'):
            requests = [request for request in worker.requests if request.task_id.split('/')[1] == role]
            self.assertEqual([request.attempt for request in requests], [1, 2])
            self.assertEqual(requests[0].feedback, '')
            self.assertIn('wrong_answer', requests[1].feedback)
        self.assertNotIn(HIDDEN, json.dumps([asdict(request) for request in worker.requests]))
        for arm in result['arms'].values():
            self.assertEqual(arm['release_head'], arm['exact_tested_sha'])

    def test_infrastructure_failure_stops_before_pool_or_hidden(self):
        with self.assertRaisesRegex(swarm.HaltRun, 'infrastructure'):
            self.case(malformed=True)
        self.assertFalse((self.root / 'case' / 'selection-frozen.json').exists())

    def test_reused_final_is_rejected_despite_passing_receipt(self):
        with self.assertRaisesRegex(swarm.HaltRun, 'provenance'):
            self.case(reused_final=True)
        self.assertFalse((self.root / 'case' / 'result.json').exists())

    def test_receipt_for_a_different_candidate_is_not_a_matrix_result(self):
        with self.assertRaisesRegex(swarm.HaltRun, 'ordered candidate'):
            self.case(misbound=True)
        self.assertFalse((self.root / 'case' / 'selection-frozen.json').exists())

    def test_live_missing_proof_is_rejected_before_session_ledger_or_provider(self):
        cheap = OpenAIWorker('offline', model=MODEL, max_output_tokens=4096)
        strong = OpenAIWorker('offline', model=STRONG_MODEL, max_output_tokens=4096)
        with patch.object(swarm, 'StudyValidationSession') as session, self.assertRaisesRegex(ValueError, 'rehearsal'):
            swarm.run_swarm_experiment(self.root / 'run', cheap, strong, mode='live',
                                       budget_ledger=self.root / 'budget.sqlite', budget_units=1)
        session.assert_not_called()
        self.assertFalse((self.root / 'run').exists())
        self.assertFalse((self.root / 'budget.sqlite').exists())

    def test_invalid_budget_or_determinism_fails_before_session(self):
        for options in (dict(validation_budget=ResourceBudget(workers=0)), dict(deterministic_visible='yes')):
            with patch.object(swarm, 'StudyValidationSession') as session, self.assertRaises(ValueError):
                swarm.run_swarm_experiment(self.root / 'run', Mock(), Mock(), **options)
            session.assert_not_called()

    def test_existing_output_is_never_overwritten(self):
        output = self.root / 'run'
        output.mkdir()
        (output / 'evidence').write_text('retain')
        with patch.object(swarm, 'StudyValidationSession') as session, self.assertRaises(FileExistsError):
            swarm.run_swarm_experiment(output, Mock(), Mock())
        session.assert_not_called()
        self.assertEqual((output / 'evidence').read_text(), 'retain')

    def test_dangling_output_symlink_is_not_followed(self):
        output = self.root / 'run'
        target = self.root / 'missing'
        output.symlink_to(target)
        with patch.object(swarm, 'StudyValidationSession') as session, self.assertRaises(FileExistsError):
            swarm.run_swarm_experiment(output, Mock(), Mock())
        session.assert_not_called()
        self.assertFalse(target.exists())

    def test_cancelled_controller_starts_no_worker(self):
        with patch.object(FixtureSession, 'raise_if_cancelled', side_effect=RuntimeError('cancelled')):
            with self.assertRaisesRegex(RuntimeError, 'cancelled'):
                self.case()
        self.assertEqual(list((self.root / 'case' / 'requests').iterdir()), [])

    def test_cancellation_during_final_save_retains_interrupted_status(self):
        def session_factory(output, **options):
            output.mkdir(parents=True)
            options = {key: value for key, value in options.items() if key not in ('cache', 'validator_factory')}
            return types.SimpleNamespace(contract=swarm.study_validation_contract(**options),
                                         preflight=lambda: (True, 'offline'), raise_if_cancelled=lambda: None)
        @contextmanager
        def late_cancel(session):
            yield
            raise RuntimeError('cancelled')
        def case(root, task, repetition, *args, **kwargs):
            return dict(task_id=task.id, repetition=repetition)
        with patch.object(swarm, 'StudyValidationSession', side_effect=session_factory), \
                patch.object(swarm, 'study_signals', late_cancel), patch.object(swarm, '_case', side_effect=case):
            with self.assertRaisesRegex(RuntimeError, 'cancelled'):
                swarm.run_swarm_experiment(self.root / 'run', Mock(), Mock(), repetitions=1)
        self.assertEqual(json.loads((self.root / 'run' / 'results.json').read_text())['status'], 'interrupted')

    def test_cli_requests_one_full_known_worker_rehearsal(self):
        from gossip_harness.swarm_fixture import TASKS
        expected = swarm.contract(TASKS, DEFAULT_IMAGE)
        proof = self.proof(expected)
        for row, task in zip(proof['cases'], TASKS):
            row['task_id'] = task.id
        with patch.object(swarm, 'run_swarm_experiment', return_value=proof) as run, \
                patch.object(swarm, 'audit_rehearsal', return_value={'qualified': True}) as audit:
            self.assertEqual(swarm.main(['--output', str(self.root / 'run')]), 0)
        audit.assert_called_once_with(proof, expected, evidence_root=self.root / 'run')
        args, options = run.call_args
        self.assertIsInstance(args[1], swarm.KnownSwarmWorker)
        self.assertIsInstance(args[2], swarm.KnownSwarmWorker)
        self.assertEqual(options['repetitions'], 1)
        self.assertEqual(options['mode'], 'rehearsal')
        self.assertFalse(options['deterministic_visible'])
        self.assertEqual(options['validation_budget'], ResourceBudget())

    def test_retained_audit_binds_real_controller_matrices_and_rejects_tampering(self):
        root = self.root / 'proof'
        root.mkdir()
        ledger = Ledger(root / 'ledger.sqlite', budget_units=0)
        trace = Trace(root / 'trace.jsonl')
        session = FixtureSession(root, self)
        cases = []
        with patch.object(swarm, 'GitStore', FixtureGit), patch.object(swarm, 'PromotionCoordinator', FixturePromotion):
            for task in self.tasks:
                session.root = root / 'repeat-0' / task.id
                worker = FixtureWorker(task)
                cases.append(swarm._case(session.root, task, 0, worker, worker, ledger, session, trace,
                                         task.id, mode='rehearsal', progress=lambda value: None))
        expected = swarm.contract(self.tasks, DEFAULT_IMAGE)
        proof = {**self.proof(expected), 'cases': cases, 'budget_before': ledger.budget(), 'budget': ledger.budget()}
        (root / 'results.json').write_text(json.dumps(proof))
        for relative in expected['sources']:
            destination = root / 'source-snapshot' / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(Path(relative).read_bytes())
        observations = {'observations': session.observations}
        def tree(path, sha):
            return FixtureGit.stores[str(path)].trees[sha]
        with patch('gossip_harness.swarm_fixture.TASKS', self.tasks), \
                patch.object(swarm, 'audit_study_sessions', return_value=observations), \
                patch.object(swarm, 'read_text_tree', side_effect=tree):
            result = swarm.audit_rehearsal(proof, expected, evidence_root=root)
            self.assertTrue(result['qualified'])
            self.assertEqual(result['observations'], 144)
            trace_path = root / 'trace.jsonl'
            original = trace_path.read_text()
            rows = [json.loads(line) for line in original.splitlines()]
            barrier = next(row for row in rows if row['kind'] == 'selection_frozen')
            barrier['sha256'] = '0' * 64
            trace_path.write_text(''.join(json.dumps(row) + '\n' for row in rows))
            with self.assertRaisesRegex(ValueError, 'barrier'):
                swarm.audit_rehearsal(proof, expected, evidence_root=root)
            trace_path.write_text(original)
            observation = next(row for row in session.observations if row['job']['purpose'] == 'final')
            observation['job']['files'] = {'solution.py': 'different candidate'}
            with self.assertRaisesRegex(ValueError, 'ordered matrix'):
                swarm.audit_rehearsal(proof, expected, evidence_root=root)


if __name__ == '__main__':
    unittest.main()
