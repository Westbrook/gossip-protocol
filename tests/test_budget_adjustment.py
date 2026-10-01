import concurrent.futures
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest

from gossip_harness.ledger import Ledger


class BudgetAdjustmentTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "ledger.sqlite"
        self.ledger = Ledger(self.path, budget_units=10)
        self.ledger.add_task("first")
        self.lease = self.ledger.claim("first", "worker", now=0, ttl=10)

    def increase(self, **overrides):
        args = dict(change_id="approved-change", new_budget_units=20,
                    expected_old=10, reason="Approved follow-up study", now=20)
        args.update(overrides)
        return self.ledger.increase_budget(**args)

    def rows(self, table):
        with sqlite3.connect(self.path) as db:
            return db.execute(f"SELECT * FROM {table} ORDER BY rowid").fetchall()

    def test_quiescent_raise_preserves_charges_tasks_dependencies_and_intents(self):
        self.ledger.add_task("second", ["first"])
        self.ledger.reserve("actual-request", self.lease, 9, now=1)
        self.ledger.settle("actual-request", 7)
        intent = self.ledger.begin_intent([self.lease], self.path.parent, "old", "new", now=2)
        self.ledger.finish_intent(intent["id"], True, "Accepted before new study")
        tables = ("tasks", "dependencies", "reservations", "intents")
        before = {table: self.rows(table) for table in tables}

        receipt = self.increase()

        self.assertEqual(receipt, {"id": "approved-change", "old_budget": 10,
                                  "new_budget": 20, "reason": "Approved follow-up study",
                                  "changed_at": 20.0, "spent_or_reserved": 7})
        self.assertEqual({table: self.rows(table) for table in tables}, before)
        self.assertEqual(self.ledger.budget(), {"limit": 20, "spent_or_reserved": 7, "remaining": 13})
        reopened = Ledger(self.path, budget_units=20)
        self.assertEqual(reopened.budget(), self.ledger.budget())
        with self.assertRaisesRegex(ValueError, "silently changed"):
            Ledger(self.path, budget_units=30)
        with self.assertRaisesRegex(ValueError, "silently changed"):
            Ledger(self.path, budget_units=10)

    def test_unsettled_expired_reservation_cannot_be_erased_by_raise(self):
        self.ledger.reserve("ambiguous-response", self.lease, 10, now=1)
        self.ledger.claim("first", "replacement", now=11, ttl=10)
        before = self.rows("reservations")
        with self.assertRaisesRegex(ValueError, "unsettled"):
            self.increase()
        self.assertEqual(self.ledger.budget(), {"limit": 10, "spent_or_reserved": 10, "remaining": 0})
        self.assertEqual(self.rows("reservations"), before)
        self.assertEqual(self.rows("budget_changes"), [])
        self.ledger.settle("ambiguous-response", 8)
        self.assertEqual(self.increase()["spent_or_reserved"], 8)

    def test_pending_promotion_blocks_raise_until_reconciled(self):
        intent = self.ledger.begin_intent([self.lease], self.path.parent, "old", "new", now=2)
        before = self.rows("intents")
        with self.assertRaisesRegex(ValueError, "pending"):
            self.increase()
        self.assertEqual(self.rows("intents"), before)
        self.assertEqual(self.rows("budget_changes"), [])
        self.assertEqual(self.ledger.budget()["limit"], 10)
        self.ledger.finish_intent(intent["id"], False, "CAS rejected")
        self.assertEqual(self.increase()["new_budget"], 20)

    def test_invalid_arguments_leave_cap_and_audit_unchanged(self):
        invalid = [dict(change_id=""), dict(change_id="  "), dict(change_id=None),
                   dict(new_budget_units=10), dict(new_budget_units=9),
                   dict(new_budget_units=-1), dict(new_budget_units=20.0),
                   dict(new_budget_units=True), dict(new_budget_units=2**63),
                   dict(expected_old=-1), dict(expected_old=10.0), dict(expected_old=True),
                   dict(expected_old=11), dict(reason=""), dict(reason=" \n"),
                   dict(reason=None), dict(now=float("nan")), dict(now=float("inf")),
                   dict(now=-1), dict(now=True), dict(now="20")]
        for args in invalid:
            with self.subTest(args=args):
                with self.assertRaises(ValueError):
                    self.increase(**args)
                self.assertEqual(self.ledger.budget()["limit"], 10)
                self.assertEqual(self.rows("budget_changes"), [])

    def test_idempotent_receipt_survives_later_cap_and_inflight_activity(self):
        receipt = self.increase()
        self.increase(change_id="second-change", expected_old=20, new_budget_units=30, now=21)
        later = self.ledger.claim("first", "later-worker", now=22, ttl=10)
        self.ledger.reserve("later-request", later, 5, now=23)
        self.assertEqual(self.increase(), receipt)
        self.assertEqual(self.ledger.budget()["limit"], 30)
        self.assertEqual(len(self.rows("budget_changes")), 2)
        for different in (dict(new_budget_units=21), dict(expected_old=9),
                          dict(reason="Different approval"), dict(now=22)):
            with self.subTest(different=different):
                with self.assertRaisesRegex(ValueError, "different parameters"):
                    self.increase(**different)
        self.assertEqual(len(self.rows("budget_changes")), 2)

    def test_concurrent_compare_and_swap_has_exactly_one_winner(self):
        barrier = threading.Barrier(8)

        def attempt(index):
            ledger = Ledger(self.path)
            barrier.wait(timeout=10)
            try:
                return ledger.increase_budget(str(index), 20 + index, expected_old=10,
                                              reason="Approved parallel request", now=20)
            except ValueError as error:
                return str(error)

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            outcomes = list(pool.map(attempt, range(8)))
        winners = [outcome for outcome in outcomes if isinstance(outcome, dict)]
        self.assertEqual(len(winners), 1)
        self.assertEqual(len(self.rows("budget_changes")), 1)
        self.assertEqual(self.ledger.budget()["limit"], winners[0]["new_budget"])
        self.assertEqual(sum("expected old cap" in result for result in outcomes if isinstance(result, str)), 7)

    def test_concurrent_identical_retries_share_one_receipt(self):
        barrier = threading.Barrier(8)

        def attempt(_):
            ledger = Ledger(self.path)
            barrier.wait(timeout=10)
            return ledger.increase_budget("one-approval", 20, expected_old=10,
                                          reason="Approved parallel request", now=20)

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            receipts = list(pool.map(attempt, range(8)))
        self.assertEqual(receipts, [receipts[0]] * 8)
        self.assertEqual(len(self.rows("budget_changes")), 1)
        self.assertEqual(self.ledger.budget()["limit"], 20)


if __name__ == "__main__":
    unittest.main()
