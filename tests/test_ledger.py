import concurrent.futures
from pathlib import Path
import tempfile
import unittest

from gossip_harness.ledger import BudgetExceeded, ClaimRejected, Ledger


class LedgerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "ledger.sqlite"
        self.ledger = Ledger(self.path, budget_units=10)
        self.ledger.add_task("first")

    def test_expiry_reassigns_epoch_and_fences_old_owner(self):
        a = self.ledger.claim("first", "a", now=0, ttl=5)
        with self.assertRaises(ClaimRejected):
            self.ledger.claim("first", "b", now=4, ttl=5)
        b = self.ledger.claim("first", "b", now=5, ttl=5)
        self.assertEqual(b.epoch, a.epoch + 1)
        with self.assertRaises(ClaimRejected):
            self.ledger.validate(a, now=6)
        self.ledger.validate(b, now=6)

    def test_expired_worker_cannot_renew_or_start_promotion(self):
        a = self.ledger.claim("first", "a", now=0, ttl=5)
        with self.assertRaises(ClaimRejected):
            self.ledger.renew(a, now=5, ttl=5)
        with self.assertRaises(ClaimRejected):
            self.ledger.begin_intent([a], self.path.parent, "old", "new", now=5)

    def test_claim_race_has_one_winner(self):
        def attempt(worker):
            try:
                return Ledger(self.path).claim("first", worker, now=0, ttl=10)
            except ClaimRejected:
                return None
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            winners = [lease for lease in pool.map(attempt, map(str, range(8))) if lease]
        self.assertEqual(len(winners), 1)

    def test_fresh_database_can_be_initialized_concurrently(self):
        path = self.path.parent / "simultaneous.sqlite"
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            ledgers = list(pool.map(lambda _: Ledger(path, budget_units=10), range(8)))
        self.assertEqual([v.budget()["limit"] for v in ledgers], [10] * 8)

    def test_reservations_bound_concurrent_spend_and_settle_unused_capacity(self):
        a = self.ledger.claim("first", "a", now=0, ttl=5)
        self.ledger.reserve("r1", a, 8, now=1)
        self.ledger.reserve("r1", a, 8, now=1)  # idempotent, no extra credit
        with self.assertRaises(BudgetExceeded):
            self.ledger.reserve("r2", a, 3, now=1)
        self.ledger.settle("r1", 6)
        self.ledger.reserve("r2", a, 4, now=1)
        self.assertEqual(self.ledger.budget()["remaining"], 0)
        with self.assertRaises(ValueError):
            self.ledger.settle("r1", 5)
        with self.assertRaises(ValueError):
            self.ledger.settle("r2", 5)

    def test_expiry_does_not_refund_inflight_calls(self):
        a = self.ledger.claim("first", "a", now=0, ttl=5)
        self.ledger.reserve("r1", a, 10, now=1)
        b = self.ledger.claim("first", "b", now=5, ttl=5)
        with self.assertRaises(BudgetExceeded):
            self.ledger.reserve("r2", b, 1, now=6)
        self.ledger.settle("r1", 9)
        self.ledger.reserve("r2", b, 1, now=6)

    def test_dependencies_unblock_only_after_accepted_intent(self):
        self.ledger.add_task("second", ["first"])
        with self.assertRaises(ClaimRejected):
            self.ledger.claim("second", "b", now=0, ttl=5)
        a = self.ledger.claim("first", "a", now=0, ttl=5)
        intent = self.ledger.begin_intent([a], self.path.parent, "old", "new", now=1)
        with self.assertRaises(ClaimRejected):
            self.ledger.claim("first", "b", now=50, ttl=5)
        self.ledger.finish_intent(intent["id"], True, "Tested commit accepted")
        self.ledger.claim("second", "b", now=50, ttl=5)

    def test_rejected_intent_releases_attempt_without_completing_it(self):
        a = self.ledger.claim("first", "a", now=0, ttl=5)
        intent = self.ledger.begin_intent([a], self.path.parent, "old", "new", now=1)
        self.ledger.finish_intent(intent["id"], False, "CAS stale")
        self.assertEqual(self.ledger.task("first")["status"], "claimed")
        self.ledger.validate(a, now=2)
        with self.assertRaises(ValueError):
            self.ledger.finish_intent(intent["id"], True, "Cannot change outcome")

    def test_multi_task_intent_validates_all_before_locking_any(self):
        self.ledger.add_task("second")
        a = self.ledger.claim("first", "a", now=0, ttl=10)
        b = self.ledger.claim("second", "b", now=0, ttl=1)
        with self.assertRaises(ClaimRejected):
            self.ledger.begin_intent([a, b], self.path.parent, "old", "new", now=2)
        self.assertEqual(self.ledger.task("first")["status"], "claimed")
        self.assertEqual(self.ledger.pending_intents(), [])

    def test_state_survives_reopen_and_budget_cannot_silently_change(self):
        a = self.ledger.claim("first", "a", now=0, ttl=5)
        self.ledger.reserve("r", a, 7, now=1)
        again = Ledger(self.path)
        again.validate(a, now=2)
        self.assertEqual(again.budget()["remaining"], 3)
        with self.assertRaises(ValueError):
            Ledger(self.path, budget_units=100)

    def test_nonfinite_time_is_rejected(self):
        with self.assertRaises(ValueError):
            self.ledger.claim("first", "a", now=float("nan"), ttl=5)
        with self.assertRaises(ValueError):
            self.ledger.claim("first", "a", now=0, ttl=float("inf"))


if __name__ == "__main__":
    unittest.main()
