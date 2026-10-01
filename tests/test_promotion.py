from pathlib import Path
import tempfile
import threading
import unittest

from gossip_harness.gitstore import GitStore
from gossip_harness.ledger import ClaimRejected, Ledger
from gossip_harness.promotion import PromotionCoordinator, SimulatedCrash


class PromotionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.store = GitStore.create(root / "release.git", {"value.txt": "0\n"})
        self.old = self.store.head()
        self.ledger = Ledger(root / "ledger.sqlite", budget_units=10)
        self.ledger.add_task("change")
        self.lease = self.ledger.claim("change", "worker", now=0, ttl=10)
        tip = self.store.propose({"value.txt": "1\n"})
        self.candidate = self.store.prepare(self.store, tip, self.old, lambda path: (True, "Fixture checked"))
        self.coordinator = PromotionCoordinator(self.ledger)

    def test_promotes_exact_tested_commit_and_finishes_task(self):
        result = self.coordinator.promote(self.store, self.candidate, [self.lease], now=1)
        self.assertEqual(result.status, "accepted")
        self.assertEqual(self.store.head(), self.candidate.candidate_sha)
        self.assertEqual(self.ledger.task("change")["accepted_commit"], self.store.head())
        replay = self.coordinator.promote(self.store, self.candidate, [self.lease], now=20)
        self.assertEqual(replay.status, "accepted")

    def test_stale_lease_never_reaches_git(self):
        replacement = self.ledger.claim("change", "replacement", now=10, ttl=10)
        with self.assertRaises(ClaimRejected):
            self.coordinator.promote(self.store, self.candidate, [self.lease], now=11)
        self.assertEqual(self.store.head(), self.old)
        self.ledger.validate(replacement, now=11)

    def test_crash_after_intent_recovers_unpublished_work(self):
        with self.assertRaises(SimulatedCrash):
            self.coordinator.promote(self.store, self.candidate, [self.lease], now=1, crash_at="after_intent")
        self.assertEqual(self.store.head(), self.old)
        with self.assertRaises(ClaimRejected):
            self.ledger.claim("change", "other", now=50, ttl=5)
        reopened = PromotionCoordinator(Ledger(self.ledger.path))
        outcomes = reopened.recover()
        self.assertEqual(outcomes[0].status, "rejected")
        self.ledger.claim("change", "other", now=50, ttl=5)
        self.assertEqual(reopened.recover(), [])

    def test_crash_after_git_recovers_accepted_work(self):
        with self.assertRaises(SimulatedCrash):
            self.coordinator.promote(self.store, self.candidate, [self.lease], now=1, crash_at="after_git")
        self.assertEqual(self.store.head(), self.candidate.candidate_sha)
        self.assertEqual(self.ledger.task("change")["status"], "submitting")
        outcomes = PromotionCoordinator(Ledger(self.ledger.path)).recover()
        self.assertEqual(outcomes[0].status, "accepted")
        self.assertEqual(self.ledger.task("change")["status"], "complete")

    def test_recovered_unpublished_candidate_can_be_retried_on_same_lease(self):
        with self.assertRaises(SimulatedCrash):
            self.coordinator.promote(self.store, self.candidate, [self.lease], now=1, crash_at="after_intent")
        rejected = self.coordinator.recover()[0]
        retried = self.coordinator.promote(self.store, self.candidate, [self.lease], now=2)
        self.assertEqual(retried.status, "accepted")
        self.assertNotEqual(rejected.intent_id, retried.intent_id)
        replay = self.coordinator.promote(self.store, self.candidate, [self.lease], now=20)
        self.assertEqual(replay.intent_id, retried.intent_id)

    def test_stale_head_preserves_newer_release(self):
        other = self.store.propose({"other.txt": "independent\n"})
        newer = self.store.prepare(self.store, other, self.old, lambda path: (True, "Checked"))
        self.store.accept(newer)
        result = self.coordinator.promote(self.store, self.candidate, [self.lease], now=1)
        self.assertEqual(result.status, "stale")
        self.assertEqual(self.store.head(), newer.candidate_sha)
        self.assertEqual(self.ledger.task("change")["status"], "claimed")

    def test_recovery_cannot_cancel_a_live_promotion(self):
        entered, release, recovered = threading.Event(), threading.Event(), threading.Event()
        original = self.store.accept
        errors = []
        def slow_accept(candidate):
            entered.set()
            if not release.wait(10):
                raise TimeoutError("Test did not release promotion")
            return original(candidate)
        self.store.accept = slow_accept
        def promote():
            try:
                self.coordinator.promote(self.store, self.candidate, [self.lease], now=1)
            except BaseException as error:
                errors.append(error)
        def recover():
            try:
                PromotionCoordinator(Ledger(self.ledger.path)).recover()
                recovered.set()
            except BaseException as error:
                errors.append(error)
        worker = threading.Thread(target=promote)
        worker.start()
        self.assertTrue(entered.wait(10))
        recovery = threading.Thread(target=recover)
        recovery.start()
        try:
            self.assertFalse(recovered.wait(0.1))
        finally:
            release.set()
            worker.join(10)
            recovery.join(10)
        self.assertFalse(worker.is_alive() or recovery.is_alive())
        self.assertEqual(errors, [])
        self.assertTrue(recovered.is_set())
        self.assertEqual(self.ledger.task("change")["status"], "complete")


if __name__ == "__main__":
    unittest.main()
