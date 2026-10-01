"""Study controller guards with fake fixtures and no Git, Docker, or API calls."""

import json
from pathlib import Path
import tempfile
from types import ModuleType
import unittest
from unittest.mock import Mock, patch

import gossip_harness
from gossip_harness.discovery_experiment import (
    ARMS, DEFAULT_IMAGE, EXPERIMENT, PROTOCOL_VERSION, _hash,
    deterministic_feedback, run_discovery_experiment,
)
from gossip_harness.evidence import SemanticNote
from gossip_harness.ledger import Ledger
from gossip_harness.worker import WorkerFailure


class DiscoveryControlTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.fixture = ModuleType('gossip_harness.research_fixture')
        self.fixture.SPEC_VERSION = 'control-test-v1'
        self.fixture.INITIAL_FILES = {'README.md': 'The full common project specification.',
                                      'parser.py': 'parser baseline', 'summary.py': 'summary baseline'}
        self.fixture.CHECKS = {'checks.py': 'Never executed by these tests.'}
        self.fixture.COMBINED_SPEC = 'Both parser and summary must satisfy the complete contract.'
        self.fixture.TASKS = [
            {'id': 'parser', 'allowed_paths': ('parser.py',), 'instructions': 'Parser requirements'},
            {'id': 'summary', 'allowed_paths': ('summary.py',), 'instructions': 'Summary requirements'},
        ]
        fixture_patch = patch.object(gossip_harness, 'research_fixture', self.fixture, create=True)
        fixture_patch.start()
        self.addCleanup(fixture_patch.stop)
        self.worker = Mock()
        self.worker.model = 'offline-controller-stub'
        self.validator = Mock()
        self.validator.preflight.return_value = (True, 'Offline preflight stub')
        self.factory = Mock(return_value=self.validator)

    def proof(self, **overrides):
        contract = dict(mode='rehearsal', protocol_version=PROTOCOL_VERSION,
                        spec_version=self.fixture.SPEC_VERSION, image=DEFAULT_IMAGE,
                        initial_files_sha256=_hash(self.fixture.INITIAL_FILES),
                        checks_sha256=_hash(self.fixture.CHECKS),
                        task_spec_sha256=_hash(self.fixture.TASKS), arms=list(ARMS), attempts=2,
                        status='finished', repetitions=1)
        contract.update(overrides)
        path = self.root / 'rehearsal.json'
        path.write_text(json.dumps(dict(
            experiment=EXPERIMENT, contract=contract, unexecuted=[],
            cases=[dict(trial=0, variant=arm, project_accepted=True) for arm in ARMS])))
        return path

    def run_study(self, **kwargs):
        options = dict(mode='live', budget_ledger=self.root / 'shared.sqlite',
                       budget_units=10_000_000, validator_factory=self.factory)
        options.update(kwargs)
        return run_discovery_experiment(self.root / 'run', self.worker, **options)

    def no_paid_work(self):
        self.worker.reservation_units.assert_not_called()
        self.worker.run.assert_not_called()

    def test_feedback_removes_elapsed_time_without_erasing_failure_semantics(self):
        first = ('Docker validation failed\n'
                 'FAIL: test_dependency_readiness (checks.SummaryChecks)\n'
                 'AssertionError: ["a"] != ["b"]\n'
                 'Ran 19 tests in 0.003s\nFAILED (failures=1)\n')
        second = first.replace('0.003s', '12.907s')
        normalized = deterministic_feedback(first)
        self.assertEqual(normalized, deterministic_feedback(second))
        self.assertIn('Ran 19 tests in <elapsed>s', normalized)
        self.assertIn('test_dependency_readiness', normalized)
        self.assertIn('AssertionError: ["a"] != ["b"]', normalized)
        self.assertNotEqual(normalized, deterministic_feedback(
            second.replace('test_dependency_readiness', 'test_graph_cycle')))
        self.assertNotEqual(normalized, deterministic_feedback(
            second.replace('["b"]', '["c"]')))
        self.assertNotEqual(normalized, deterministic_feedback(
            second.replace('19 tests', '18 tests')))

    def test_invalid_trial_bound_or_budget_rejected_before_preflight(self):
        for repetitions in (0, 4, True):
            with self.subTest(repetitions=repetitions), self.assertRaises(ValueError):
                self.run_study(repetitions=repetitions)
        with self.assertRaises(ValueError):
            self.run_study(budget_units=10_000_001)
        self.factory.assert_not_called()
        self.no_paid_work()

    def test_live_requires_rehearsal_before_any_reservation(self):
        with self.assertRaisesRegex(ValueError, 'rehearsal'):
            self.run_study()
        self.factory.assert_not_called()
        self.no_paid_work()
        self.assertFalse((self.root / 'shared.sqlite').exists())

    def test_changed_task_spec_invalidates_rehearsal(self):
        with self.assertRaisesRegex(ValueError, 'rehearsal'):
            self.run_study(rehearsal_results=self.proof(task_spec_sha256='obsolete'))
        self.factory.assert_not_called()
        self.no_paid_work()

    def test_duplicate_or_missing_rehearsal_roster_prevents_work(self):
        for defect in ('duplicate', 'missing-trial', 'unfinished'):
            with self.subTest(defect=defect):
                proof = self.proof()
                data = json.loads(proof.read_text())
                if defect == 'duplicate':
                    data['cases'].append(dict(data['cases'][0]))
                elif defect == 'missing-trial':
                    data['contract']['repetitions'] = 2
                else:
                    data['contract']['status'] = 'running'
                proof.write_text(json.dumps(data))
                with self.assertRaisesRegex(ValueError, 'rehearsal'):
                    self.run_study(rehearsal_results=proof)
        self.factory.assert_not_called()
        self.no_paid_work()

    def test_failed_preflight_does_not_create_run_or_budget(self):
        self.validator.preflight.return_value = (False, 'Pinned image unavailable')
        with self.assertRaisesRegex(RuntimeError, 'preflight'):
            self.run_study(rehearsal_results=self.proof())
        self.no_paid_work()
        self.assertFalse((self.root / 'run').exists())
        self.assertFalse((self.root / 'shared.sqlite').exists())

    def test_unknown_discovery_cost_preserves_prior_budget_and_stops_all_trials(self):
        ledger = Ledger(self.root / 'shared.sqlite', budget_units=10_000_000)
        ledger.add_task('prior-run')
        lease = ledger.claim('prior-run', 'worker', now=0, ttl=10)
        ledger.reserve('prior-call', lease, 200_000, now=1)
        ledger.settle('prior-call', 100_000)
        self.worker.reservation_units.return_value = 500_000
        self.worker.run.side_effect = WorkerFailure('Offline ambiguous response', None, {'halt': True})
        store = Mock()
        store.head.return_value = 'a' * 40
        with patch('gossip_harness.discovery_experiment.GitStore') as git:
            git.create.return_value = store
            git.fork.return_value = store
            with self.assertRaises(WorkerFailure):
                self.run_study(rehearsal_results=self.proof())
        self.worker.run.assert_called_once()
        request = self.worker.run.call_args.args[0]
        self.assertEqual(request.files, self.fixture.INITIAL_FILES)
        self.assertIn(self.fixture.COMBINED_SPEC, request.instructions)
        report = json.loads((self.root / 'run' / 'results.json').read_text())
        self.assertEqual(report['contract']['status'], 'interrupted')
        self.assertEqual(report['budget_before']['spent_or_reserved'], 100_000)
        self.assertEqual(report['budget']['spent_or_reserved'], 600_000)
        self.assertEqual(report['budget']['limit'], 10_000_000)
        self.assertEqual(report['cases'], [])
        self.assertEqual(len(report['unexecuted']), 3 * len(ARMS))
        self.assertFalse((self.root / 'run' / 'trial-1').exists())

    def test_fatal_coding_arm_stops_later_arms_and_trials(self):
        notes = tuple(SemanticNote.parse(json.dumps({
            'summary': 'An observation', 'recommendation': 'Check the complete contract',
            'references': [{'path': 'README.md', 'quote': 'The full common project specification.'}],
        }), producer=f"scout-{task['id']}", task_id=task['id'],
            files=self.fixture.INITIAL_FILES) for task in self.fixture.TASKS)

        def failed_case(root, variant, *args, **kwargs):
            root.mkdir()
            return dict(variant=variant, project_accepted=False,
                        status='request_indeterminate', halt=True)

        with patch('gossip_harness.discovery_experiment._discover', return_value=(notes, 0)) as discover:
            with patch('gossip_harness.discovery_experiment._case', side_effect=failed_case) as case:
                report = self.run_study(rehearsal_results=self.proof())
        discover.assert_called_once()
        case.assert_called_once()
        self.assertEqual(report['cases'][0]['status'], 'request_indeterminate')
        self.assertEqual(len(report['unexecuted']), 3 * len(ARMS) - 1)
        self.assertFalse((self.root / 'run' / 'trial-1').exists())
        self.no_paid_work()


if __name__ == '__main__':
    unittest.main()
