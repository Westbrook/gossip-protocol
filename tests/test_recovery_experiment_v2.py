"""V2 accounting rejection is read-only and precedes original preparation."""
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from gossip_harness import recovery_experiment_v2 as guarded
from gossip_harness.ledger import Ledger


class RecoveryEntryV2Tests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.path = self.root / 'ledger.sqlite'
        self.ledger = Ledger(self.path, 1000)
        self.output = self.root / 'run'

    def live(self, **kwargs):
        options = dict(mode='live', budget_ledger=self.path)
        options.update(kwargs)
        return guarded.run_recovery_experiment(self.output, object(), **options)

    def test_read_only_guard_counts_unsettled_usage_without_erasing_it(self):
        self.ledger.add_task('work')
        lease = self.ledger.claim('work', 'worker', now=1, ttl=10)
        self.ledger.reserve('reservation', lease, 300, now=2)
        with sqlite3.connect(self.path) as db:
            before = list(db.iterdump())
        receipt = guarded.inspect_live_ledger(self.path)
        self.assertEqual(receipt['spent_or_reserved'], 300)
        self.assertEqual(receipt['remaining'], 700)
        self.assertEqual(receipt['unsettled'], 1)
        self.assertTrue(receipt['read_only'])
        with sqlite3.connect(self.path) as db:
            self.assertEqual(list(db.iterdump()), before)

    def test_invalid_cap_stops_before_original_runner_or_output(self):
        for cap in (0, -1, 10_000_001, 'bad'):
            with sqlite3.connect(self.path) as db:
                db.execute("UPDATE settings SET value=? WHERE key='budget'", (cap,))
            with patch.object(guarded.original, 'run_recovery_experiment') as run:
                with self.assertRaisesRegex(ValueError, 'shared cap'):
                    self.live()
                run.assert_not_called()
            self.assertFalse(self.output.exists())

    def test_missing_symlink_foreign_owned_or_bad_sidecar_rejects_before_original(self):
        with patch.object(guarded.original, 'run_recovery_experiment') as run:
            with self.assertRaisesRegex(ValueError, 'missing'):
                self.live(budget_ledger=self.root / 'absent.sqlite')
            link = self.root / 'link.sqlite'
            link.symlink_to(self.path)
            with self.assertRaisesRegex(ValueError, 'regular files'):
                self.live(budget_ledger=link)
            with patch.object(guarded.os, 'getuid', return_value=-1):
                with self.assertRaisesRegex(ValueError, 'owned by'):
                    self.live()
            sidecar = Path(str(self.path) + '.schema.lock')
            sidecar.unlink()
            sidecar.symlink_to(self.path)
            with self.assertRaisesRegex(ValueError, 'regular files'):
                self.live()
            run.assert_not_called()

    def test_invalid_reservation_or_exceeded_cap_rejects_before_preparation(self):
        self.ledger.add_task('work')
        lease = self.ledger.claim('work', 'worker', now=1, ttl=10)
        self.ledger.reserve('r', lease, 100, now=2)
        for sql in ("UPDATE reservations SET spent=200,state='settled'",
                    "UPDATE reservations SET spent=NULL,state='reserved',amount=2000",
                    "UPDATE reservations SET amount=100,epoch=20",
                    "UPDATE reservations SET epoch=1,task_id='missing'"):
            with sqlite3.connect(self.path) as db:
                db.execute(sql)
            with patch.object(guarded.original, 'run_recovery_experiment') as run:
                with self.assertRaises(ValueError):
                    self.live()
                run.assert_not_called()

    def test_existing_budget_cannot_be_changed_and_old_rehearsal_is_insufficient(self):
        with patch.object(guarded.original, 'run_recovery_experiment') as run:
            with self.assertRaisesRegex(ValueError, 'cannot change'):
                self.live(budget_units=2000)
            with self.assertRaisesRegex(ValueError, 'v2 rehearsal'):
                self.live()
            old = self.root / 'old.json'
            old.write_text('{}')
            with self.assertRaisesRegex(ValueError, 'v2 rehearsal'):
                self.live(rehearsal_results=old)
            run.assert_not_called()

    def test_matching_guard_rehearsal_delegates_without_bypassing_original_gate(self):
        proof_dir = self.root / 'rehearsal'
        proof_dir.mkdir()
        proof = proof_dir / 'results.json'
        proof.write_text('{"status":"accepted"}')
        contract = guarded.entry_contract(image=guarded.original.DEFAULT_IMAGE, seeds=(0,1,2),
            fault_profiles=tuple(guarded.original.FAULT_PROFILES), attempts=2)
        data = dict(schema_version=2, protocol=guarded.PROTOCOL, qualification_mode='physical-rehearsal',
                    contract=contract, mode='rehearsal', passed=True,
                    result_sha256=hashlib.sha256(proof.read_bytes()).hexdigest())
        (proof_dir / 'entry-guard.json').write_text(json.dumps(data))
        with patch.object(guarded.original, 'run_recovery_experiment', side_effect=ValueError('original rehearsal gate')) as run:
            with self.assertRaisesRegex(ValueError, 'original rehearsal gate'):
                self.live(rehearsal_results=proof)
            self.assertEqual(run.call_args.kwargs['rehearsal_results'], proof)
            self.assertEqual(run.call_args.kwargs['budget_ledger'], self.path)
        data['contract']['replay_concurrency'] = 3
        (proof_dir / 'entry-guard.json').write_text(json.dumps(data))
        with patch.object(guarded.original, 'run_recovery_experiment') as run:
            with self.assertRaisesRegex(ValueError, 'exact matching'):
                self.live(rehearsal_results=proof)
            run.assert_not_called()

    def test_rehearsal_retains_versioned_guard_bound_to_original_result(self):
        def rehearsal(output, worker, **kwargs):
            output.mkdir()
            result = dict(status='accepted', mode=kwargs['mode'])
            (output / 'results.json').write_text(json.dumps(result))
            return result
        with patch.object(guarded.original, 'run_recovery_experiment', side_effect=rehearsal):
            result = guarded.run_recovery_experiment(self.output, object())
        receipt = json.loads((self.output / 'entry-guard.json').read_text())
        self.assertEqual(result['status'], 'accepted')
        self.assertEqual(receipt['protocol'], guarded.PROTOCOL)
        self.assertTrue(receipt['passed'])
        self.assertEqual(receipt['result_sha256'], hashlib.sha256((self.output / 'results.json').read_bytes()).hexdigest())
        self.assertIsNone(receipt['ledger_preflight'])

    def test_failed_original_output_keeps_guard_diagnostics(self):
        def failing(output, worker, **kwargs):
            output.mkdir()
            raise RuntimeError('failed before completion')
        with patch.object(guarded.original, 'run_recovery_experiment', side_effect=failing):
            with self.assertRaises(RuntimeError):
                guarded.run_recovery_experiment(self.output, object())
        receipt = json.loads((self.output / 'entry-guard.json').read_text())
        self.assertFalse(receipt['passed'])
        self.assertIn('failed before completion', receipt['failure'])

    def test_custom_validator_never_mints_reusable_guard_or_starts_live_work(self):
        def rehearsal(output, worker, **kwargs):
            output.mkdir()
            result = dict(status='accepted', mode='rehearsal')
            (output / 'results.json').write_text(json.dumps(result))
            return result
        custom = lambda _: None
        with patch.object(guarded.original, 'run_recovery_experiment', side_effect=rehearsal):
            result = guarded.run_recovery_experiment(self.output, object(), validator_factory=custom)
        self.assertEqual(result['status'], 'accepted')
        receipt = json.loads((self.output / 'entry-guard.json').read_text())
        self.assertFalse(receipt['passed'])
        self.assertEqual(receipt['qualification_mode'], 'test-only-custom-validator')
        self.output = self.root / 'live'
        with patch.object(guarded.original, 'run_recovery_experiment') as run:
            with self.assertRaisesRegex(ValueError, 'test-only'):
                self.live(validator_factory=custom)
            run.assert_not_called()
