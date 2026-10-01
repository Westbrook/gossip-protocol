"""Retained accounting and proposal diagnostics fail closed on mismatches."""
import sqlite3
import tempfile
import unittest
from pathlib import Path

from gossip_harness.ledger import Ledger
from retain_swarm import inspect_ledger, proposal_counts


class RetainSwarmTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='retain-swarm-')
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / 'budget.sqlite'
        ledger = Ledger(self.path, budget_units=1000)
        ledger.add_task('study/0/task/study-record')
        lease = ledger.claim('study/0/task/study-record', 'worker', now=1, ttl=100)
        ledger.reserve('study/0/task/strong-0-1', lease, 100, now=2)
        ledger.settle('study/0/task/strong-0-1', 30)
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE tasks SET status='complete'")
        self.calls = {'study/0/task/strong-0-1': 30}
        self.before = dict(limit=1000, spent_or_reserved=0, remaining=1000)
        self.after = dict(limit=1000, spent_or_reserved=30, remaining=970)

    def test_read_only_accounting_reconciles_and_preserves_database(self):
        before = self.path.read_bytes()
        result = inspect_ledger(self.path, 'study', self.calls, self.before, self.after)
        self.assertEqual(result['ledger_verification']['study_actual_micro_usd'], 30)
        self.assertTrue(result['ledger_verification']['read_only'])
        self.assertEqual(self.path.read_bytes(), before)

    def test_invocation_cost_mismatch_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'differs from invocation'):
            inspect_ledger(self.path, 'study', {'study/0/task/strong-0-1': 29}, self.before, self.after)

    def test_budget_delta_mismatch_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'global budget delta'):
            inspect_ledger(self.path, 'study', self.calls, self.before, {**self.after, 'spent_or_reserved': 31})

    def test_unknown_or_unsettled_reservation_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'reservations differ'):
            inspect_ledger(self.path, 'other-study', self.calls, self.before, self.after)
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE reservations SET state='reserved',spent=NULL")
        with self.assertRaisesRegex(ValueError, 'unsettled'):
            inspect_ledger(self.path, 'study', self.calls, self.before, self.after)

    def test_duplicate_and_rejected_batches_are_separate(self):
        receipts = [dict(oracle_assisted=True, proposed_count=3,
                         eligible=[dict(id='a'), dict(id='b')],
                         rejected=[dict(index=2, reason='duplicate_input')]),
                    dict(oracle_assisted=True, proposed_count=2,
                         eligible=[dict(id='b')], rejected=[dict(index=1, reason='wrong_expectation')]),
                    dict(oracle_assisted=True, proposed_count=0,
                         eligible=[], rejected=[dict(index=None, reason='batch_invalid_json')])]
        result = proposal_counts(receipts, {'a'})
        self.assertEqual(result['proposed'], 5)
        self.assertEqual(result['eligible'], 3)
        self.assertEqual(result['unique_eligible_inputs'], 2)
        self.assertEqual(result['duplicates_across_batches'], 1)
        self.assertEqual(result['within_batch_duplicate_rejections'], 1)
        self.assertEqual(result['rejected_cases'], 2)
        self.assertEqual(result['rejected_batches'], 1)
        self.assertEqual(result['eligible_already_in_baseline'], 1)


if __name__ == '__main__':
    unittest.main()
