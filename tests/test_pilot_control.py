"""Fast pilot control tests: no Docker, provider requests, or Git subprocesses."""

import hashlib
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

from gossip_harness.ledger import ClaimRejected, Ledger
from gossip_harness.pilot import DEFAULT_IMAGE, LeaseKeeper, VARIANTS, run_pilot
from gossip_harness.pilot_fixture import CHECKS, INITIAL_FILES
from gossip_harness.worker import WorkerFailure


class PilotControlTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.worker = Mock()
        self.worker.model = 'offline-control-test'
        self.validator = Mock()
        self.validator.preflight.return_value = (True, 'offline preflight stub')
        self.factory = Mock(return_value=self.validator)

    def rehearsal(self, **contract_changes):
        contract = {
            'mode': 'rehearsal', 'image': DEFAULT_IMAGE,
            'initial_files_sha256': hashlib.sha256(
                json.dumps(INITIAL_FILES, sort_keys=True).encode()).hexdigest(),
            'checks_sha256': hashlib.sha256(
                json.dumps(CHECKS, sort_keys=True).encode()).hexdigest(),
        }
        contract.update(contract_changes)
        path = self.root / 'offline-rehearsal.json'
        path.write_text(json.dumps({
            'experiment': 'practical-coding-pilot', 'contract': contract,
            'cases': [{'variant': variant, 'project_accepted': True}
                      for variant in VARIANTS],
        }))
        return path

    def run_live_stub(self, **kwargs):
        options = dict(image=DEFAULT_IMAGE, budget_ledger=self.root / 'shared.sqlite',
                       budget_units=10_000_000, mode='live',
                       validator_factory=self.factory)
        options.update(kwargs)
        return run_pilot(self.root / 'run', self.worker, **options)

    def assert_no_worker_call(self):
        self.worker.reservation_units.assert_not_called()
        self.worker.run.assert_not_called()

    def test_heartbeat_keeps_staged_attempt_alive_beyond_original_expiry(self):
        ledger = Ledger(self.root / 'leases.sqlite', budget_units=0)
        ledger.add_task('staged')
        with LeaseKeeper(ledger, ttl=0.75) as keeper:
            original = keeper.claim('staged', 'worker')
            time.sleep(1.1)
            self.assertGreater(time.time(), original.expires_at)
            current = keeper.check('staged')
            self.assertEqual(current.epoch, original.epoch)
            self.assertGreater(current.expires_at, original.expires_at)
            self.assertGreater(current.expires_at, time.time())
        self.assertFalse(keeper.thread.is_alive())

    def test_heartbeat_error_prevents_publication(self):
        ledger = Ledger(self.root / 'leases.sqlite', budget_units=0)
        ledger.add_task('staged')
        with LeaseKeeper(ledger) as keeper:
            keeper.claim('staged', 'worker')
            with keeper.lock:
                keeper.errors.append('OperationalError')
            with self.assertRaises(ClaimRejected):
                keeper.all()

    def test_missing_rehearsal_prevents_even_preflight_or_reservation(self):
        with self.assertRaisesRegex(ValueError, 'rehearsal'):
            self.run_live_stub()
        self.factory.assert_not_called()
        self.assert_no_worker_call()
        self.assertFalse((self.root / 'run').exists())
        self.assertFalse((self.root / 'shared.sqlite').exists())

    def test_changed_checks_invalidate_rehearsal_before_worker_use(self):
        proof = self.rehearsal(checks_sha256='different')
        with self.assertRaisesRegex(ValueError, 'rehearsal'):
            self.run_live_stub(rehearsal_results=proof)
        self.factory.assert_not_called()
        self.assert_no_worker_call()

    def test_incomplete_rehearsal_prevents_worker_use(self):
        proof = self.rehearsal()
        data = json.loads(proof.read_text())
        data['cases'][-1]['project_accepted'] = False
        proof.write_text(json.dumps(data))
        with self.assertRaisesRegex(ValueError, 'rehearsal'):
            self.run_live_stub(rehearsal_results=proof)
        self.assert_no_worker_call()

    def test_budget_over_ten_dollars_rejected_before_worker_use(self):
        with self.assertRaisesRegex(ValueError, 'capped at'):
            self.run_live_stub(budget_units=10_000_001)
        self.factory.assert_not_called()
        self.assert_no_worker_call()

    def test_failed_preflight_prevents_reservation_and_output_creation(self):
        self.validator.preflight.return_value = (False, 'image unavailable')
        with self.assertRaisesRegex(RuntimeError, 'preflight'):
            self.run_live_stub(rehearsal_results=self.rehearsal())
        self.assert_no_worker_call()
        self.assertFalse((self.root / 'run').exists())
        self.assertFalse((self.root / 'shared.sqlite').exists())

    def test_unknown_request_cost_is_retained_and_later_variants_not_started(self):
        self.worker.reservation_units.return_value = 500_000
        self.worker.run.side_effect = WorkerFailure(
            'Offline simulated ambiguous response', None, {'halt': True})
        store = Mock()
        store.head.return_value = 'a' * 40
        with patch('gossip_harness.pilot.GitStore') as git:
            git.create.return_value = store
            git.fork.return_value = store
            report = self.run_live_stub(rehearsal_results=self.rehearsal())
        self.worker.run.assert_called_once()
        self.assertEqual(report['cases'][0]['status'], 'request_indeterminate')
        self.assertFalse(report['cases'][0]['project_accepted'])
        self.assertTrue(report['cases'][0]['halt'])
        self.assertEqual(set(report['cases'][0]['task_states'].values()), {'claimed'})
        self.assertEqual(report['budget']['spent_or_reserved'], 500_000)
        self.assertEqual(report['unexecuted_variants'], list(VARIANTS[1:]))
        self.assertEqual(report['contract']['status'], 'finished')
        self.assertTrue((self.root / 'run' / 'results.json').exists())

    def test_known_failure_usage_accumulates_in_one_budget_across_variants(self):
        self.worker.reservation_units.return_value = 500_000
        self.worker.run.side_effect = WorkerFailure(
            'Offline simulated complete response failure', 100_000, {'halt': False})
        store = Mock()
        store.head.return_value = 'a' * 40
        with patch('gossip_harness.pilot.GitStore') as git:
            git.create.return_value = store
            git.fork.return_value = store
            report = self.run_live_stub(rehearsal_results=self.rehearsal(),
                                        budget_units=2_000_000)
        self.assertEqual(self.worker.run.call_count, 5)
        self.assertEqual([c['status'] for c in report['cases']], ['worker_failed'] * 3)
        self.assertFalse(any(c['project_accepted'] for c in report['cases']))
        self.assertEqual(report['budget']['limit'], 2_000_000)
        self.assertEqual(report['budget']['spent_or_reserved'], 500_000)
        self.assertEqual(report['unexecuted_variants'], [])

    def test_existing_output_never_replays_worker_request(self):
        (self.root / 'run').mkdir()
        with self.assertRaisesRegex(FileExistsError, 'never replayed'):
            self.run_live_stub(rehearsal_results=self.rehearsal())
        self.factory.assert_not_called()
        self.assert_no_worker_call()


if __name__ == '__main__':
    unittest.main()
