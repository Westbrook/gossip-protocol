"""Local snapshot and crash-reopen contracts; no process or provider execution."""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path
import sqlite3
import tempfile
import unittest

from gossip_harness.peer_store_v1 import Store, canonical_bytes
from gossip_harness.peer_work_v1 import PROTOCOL, WorkError, WorkJournal, digest, plan_action, text_sha


def config(role="builder"):
    return {"run_id": "run-1", "task_id": "build" if role == "builder" else "review",
            "role": role, "seed_producer": "seed", "builder_peer": "alice", "authority_sha256": "a" * 64}


def receipt(intent, text=None):
    result = {"text": intent["request"]["text"].upper() if text is None else text}
    result["text_sha256"] = text_sha(result["text"])
    return {"status": "completed", "run_id": intent["run_id"], "config_sha256": intent["authority_sha256"],
            "task_id": intent["task_id"], "epoch": 1, "action_id": intent["action_id"],
            "reservation_id": "reservation-1", "request_sha256": intent["request_sha256"],
            "authority_time": 100.0, "result": result, "usage_units": 0}


def envelope(intent, response):
    return {"protocol": PROTOCOL, "run_id": intent["run_id"], "generation": 0,
            "role": intent["role"], "intent": intent, "result": response["result"], "authority": response}


def claim(intent):
    return {"status": "ok", "run_id": intent["run_id"], "config_sha256": intent["authority_sha256"],
            "lease": {"task_id": intent["task_id"], "worker_id": intent["node_id"], "epoch": 1, "expires_at": 110.0},
            "authority_time": 100.0}


class Fixture:
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.seed = Store(self.root / "seed.sqlite", "seed")
        self.a = Store(self.root / "alice.sqlite", "alice")
        self.b = Store(self.root / "bob.sqlite", "bob")
        self.seed_event = self.seed.publish("initial", "work_seed",
            {"protocol": PROTOCOL, "run_id": "run-1", "generation": 0, "text": "a small source"})

    def arrive(self, target, source):
        target.merge(source.export(target.inventory()))

    def builder_intent(self):
        self.arrive(self.a, self.seed)
        return plan_action(self.a, config())


class WorkPlannerTests(Fixture, unittest.TestCase):
    def test_only_arrived_events_create_work_and_snapshots_do_not_change(self):
        self.assertIsNone(plan_action(self.a, config()))
        intent = self.builder_intent()
        frozen = deepcopy(intent)
        self.a.publish("later", "note", {"text": "later evidence"})
        self.assertEqual(intent, frozen)
        self.assertEqual(intent["visible_event_ids"], [self.seed_event["event_id"]])
        self.assertEqual(intent["request"]["text"], "a small source")
        self.assertEqual(intent["request_sha256"], digest(intent["request"]))
        self.assertNotEqual(plan_action(self.a, config())["request_sha256"], intent["request_sha256"])

    def test_review_waits_for_arrived_result_and_reads_its_actual_bytes(self):
        intent = self.builder_intent()
        self.arrive(self.b, self.seed)
        self.assertIsNone(plan_action(self.b, config("reviewer")))
        # The reviewer must consume even an imperfect builder output; deciding
        # whether it passes belongs to the actual review fixture.
        answer = receipt(intent, "imperfect lower case")
        event = self.a.publish("build-result", "work_result", envelope(intent, answer))
        self.assertIsNone(plan_action(self.b, config("reviewer")))
        self.arrive(self.b, self.a)
        review = plan_action(self.b, config("reviewer"))
        self.assertEqual(review["request"]["text"], answer["result"]["text"])
        self.assertEqual(review["source_sha256"], text_sha(answer["result"]["text"]))
        self.assertEqual(review["prerequisite_event_ids"], sorted([self.seed_event["event_id"], event["event_id"]]))

    def test_untrusted_seed_producer_cannot_trigger_work(self):
        self.a.publish("pretend", "work_seed", {"protocol": PROTOCOL, "run_id": "run-1", "generation": 0, "text": "forged"})
        self.assertIsNone(plan_action(self.a, config()))

    def test_conflicting_seed_and_unsupported_supersession_fail_closed(self):
        self.builder_intent()
        self.seed.publish("second", "work_seed", {"protocol": PROTOCOL, "run_id": "run-1", "generation": 0, "text": "other"})
        self.arrive(self.a, self.seed)
        with self.assertRaises(WorkError):
            plan_action(self.a, config())
        fresh = Store(self.root / "fresh.sqlite", "alice")
        source = Store(self.root / "new-seed.sqlite", "seed")
        source.publish("next", "work_seed", {"protocol": PROTOCOL, "run_id": "run-1", "generation": 1, "text": "new"})
        self.arrive(fresh, source)
        with self.assertRaises(WorkError):
            plan_action(fresh, config())

    def test_completion_receipt_must_bind_profile_request_result_and_zero_usage(self):
        intent = self.builder_intent()
        for label, mutate in (
            ("profile", lambda x: x.update(config_sha256="b" * 64)),
            ("request", lambda x: x.update(request_sha256="b" * 64)),
            ("run", lambda x: x.update(run_id="other")),
            ("usage", lambda x: x.update(usage_units=True)),
            ("pending", lambda x: x.update(status="pending")),
        ):
            with self.subTest(label=label):
                source = Store(self.root / f"producer-{label}.sqlite", "alice")
                target = Store(self.root / f"reviewer-{label}.sqlite", "bob")
                self.arrive(target, self.seed)
                answer = receipt(intent)
                mutate(answer)
                source.publish("result", "work_result", envelope(intent, answer))
                self.arrive(target, source)
                with self.assertRaises(WorkError):
                    plan_action(target, config("reviewer"))

    def test_seed_bytes_and_visible_context_are_bounded_before_work(self):
        source = Store(self.root / "large-seed.sqlite", "seed")
        source.publish("large", "work_seed", {"protocol": PROTOCOL, "run_id": "run-1", "generation": 0, "text": "é" * 1001})
        self.arrive(self.a, source)
        with self.assertRaises(WorkError):
            plan_action(self.a, config())
        for index in range(32):
            self.b.publish(f"note-{index}", "note", {"value": index})
        self.arrive(self.b, self.seed)
        with self.assertRaises(WorkError):
            plan_action(self.b, config("reviewer"))

    def test_outbox_capacity_is_admitted_before_any_authority_call(self):
        source = Store(self.root / "capacity-seed.sqlite", "seed")
        source.publish("capacity", "work_seed", {"protocol": PROTOCOL, "run_id": "run-1", "generation": 0, "text": "x" * 2000})
        self.arrive(self.a, source)
        for index in range(31):
            self.a.publish(f"context-{index}", "note", {"value": index})
        self.assertEqual(self.a.state()["count"], 32)
        with self.assertRaisesRegex(WorkError, "dissemination capacity"):
            plan_action(self.a, config())


class WorkJournalTests(Fixture, unittest.TestCase):
    def journal(self):
        return WorkJournal(self.root / "work.sqlite", "alice", config())

    def pending(self):
        intent = self.builder_intent()
        journal = self.journal()
        journal.prepare(intent)
        journal.record(intent["action_id"], "claimed", claim=claim(intent))
        journal.record(intent["action_id"], "dispatch_pending")
        return journal, intent

    def completed(self):
        journal, intent = self.pending()
        answer = receipt(intent)
        journal.record(intent["action_id"], "result", response=answer, result=answer["result"])
        return journal, intent, answer

    def test_exact_prepare_replays_but_new_context_cannot_replace_slot(self):
        intent = self.builder_intent()
        journal = self.journal()
        first = journal.prepare(intent)
        self.assertEqual(first, self.journal().prepare(deepcopy(intent)))
        self.a.publish("later", "note", {"text": "changed context"})
        with self.assertRaises(WorkError):
            journal.prepare(plan_action(self.a, config()))
        self.assertEqual(journal.read(), first)

    def test_reopen_preserves_pending_and_terminal_unknown_without_new_action(self):
        journal, intent = self.pending()
        self.assertEqual(self.journal().read()["phase"], "dispatch_pending")
        journal.record(intent["action_id"], "unknown", response={"status": "unknown"})
        reopened = self.journal()
        self.assertEqual(reopened.read()["phase"], "unknown")
        with self.assertRaises(WorkError):
            reopened.record(intent["action_id"], "dispatch_pending")

    def test_invalid_result_transition_rolls_back_entire_update(self):
        journal, intent = self.pending()
        before = journal.read()
        bad = receipt(intent)
        bad["action_id"] = "b" * 64
        with self.assertRaises(WorkError):
            journal.record(intent["action_id"], "result", response=bad, result=bad["result"])
        self.assertEqual(journal.read(), before)

    def test_outbox_replay_after_store_commit_has_one_event(self):
        journal, intent, answer = self.completed()
        outbox = envelope(intent, answer)
        row = journal.set_outbox(intent["action_id"], outbox)
        published = self.a.publish(row["outbox_command_id"], "work_result", row["outbox"])
        # Simulate process loss after the Store commit, before the action row
        # records its event ID. Reopen and replay the exact durable outbox.
        reopened = self.journal()
        recovered = reopened.read()
        replayed = self.a.publish(recovered["outbox_command_id"], "work_result", recovered["outbox"])
        self.assertEqual(published, replayed)
        final = reopened.mark_published(intent["action_id"], replayed["event_id"])
        self.assertEqual(final["phase"], "published")
        self.assertEqual(self.a.state()["count"], 2)
        self.assertEqual(self.journal().mark_published(intent["action_id"], replayed["event_id"]), final)
        with self.assertRaises(WorkError):
            reopened.mark_published(intent["action_id"], "f" * 64)

    def test_outbox_and_completed_receipt_cannot_be_replaced(self):
        journal, intent, answer = self.completed()
        journal.set_outbox(intent["action_id"], envelope(intent, answer))
        before = journal.read()
        with self.assertRaises(WorkError):
            journal.set_outbox(intent["action_id"], {"unrelated": True})
        changed = deepcopy(answer)
        changed["authority_time"] += 1
        with self.assertRaises(WorkError):
            journal.record(intent["action_id"], "result", response=changed)
        self.assertEqual(journal.read(), before)

    def test_tampered_checksum_and_rebound_identity_are_rejected_on_reopen(self):
        journal, intent = self.pending()
        changed = journal.read()
        changed["intent"]["request"]["text"] = "tampered"
        with sqlite3.connect(self.root / "work.sqlite") as db:
            db.execute("UPDATE action SET payload=?", (canonical_bytes(changed),))
        with self.assertRaises(WorkError):
            self.journal()
        other = config()
        other["run_id"] = "different-run"
        with self.assertRaises(WorkError):
            WorkJournal(self.root / "work.sqlite", "alice", other)

    def test_rechecksummed_invalid_phase_is_rejected(self):
        journal, _ = self.pending()
        changed = journal.read()
        changed["phase"] = "result"
        changed["checksum"] = digest({key: value for key, value in changed.items() if key != "checksum"})
        with sqlite3.connect(self.root / "work.sqlite") as db:
            db.execute("UPDATE action SET payload=?", (canonical_bytes(changed),))
        with self.assertRaises(WorkError):
            self.journal()

    def test_immutable_receipts_do_not_coerce_numbers_to_booleans(self):
        journal, intent = self.pending()
        before = journal.read()
        changed = claim(intent)
        changed["lease"]["epoch"] = True
        with self.assertRaises(WorkError):
            journal.record(intent["action_id"], "dispatch_pending", claim=changed)
        self.assertEqual(journal.read(), before)

    def test_claim_and_completion_bind_exact_owner_and_epoch(self):
        intent = self.builder_intent()
        journal = self.journal()
        journal.prepare(intent)
        other = claim(intent)
        other["lease"]["worker_id"] = "bob"
        with self.assertRaises(WorkError):
            journal.record(intent["action_id"], "claimed", claim=other)
        self.assertEqual(journal.read()["phase"], "prepared")
        journal.record(intent["action_id"], "claimed", claim=claim(intent))
        journal.record(intent["action_id"], "dispatch_pending")
        answer = receipt(intent)
        answer["epoch"] = 2
        with self.assertRaises(WorkError):
            journal.record(intent["action_id"], "result", response=answer, result=answer["result"])
        self.assertEqual(journal.read()["phase"], "dispatch_pending")

    def test_schema_damage_does_not_create_a_fresh_action(self):
        journal, _ = self.pending()
        with sqlite3.connect(self.root / "work.sqlite") as db:
            db.execute("DROP TABLE action")
        with self.assertRaises(WorkError):
            self.journal()


if __name__ == "__main__":
    unittest.main()
