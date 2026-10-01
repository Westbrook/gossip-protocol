from __future__ import annotations

import copy
import hashlib
import unittest
from unittest.mock import patch

from gossip_harness.transport import Event
from simulation.benchmark_transport import (
    ARMS, PEERS, SCENARIOS, _field, canonical, digest, event_from_dict,
    frozen_artifact_corpus, local_decision, run_experiment, run_trial, synthetic_corpus, validate_corpus,
)


class BenchmarkTransportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.corpus = synthetic_corpus()
        self.records = {item["event"]["event_id"]: event_from_dict(item["event"]) for item in self.corpus["schedule"]}

    def decide(self, records=None, offer="good"):
        return local_decision(self.records if records is None else records, offer, 2, "builder-a")

    def test_complete_positive_and_refutation_decisions(self) -> None:
        self.assertEqual(self.decide(), "accept")
        self.assertEqual(self.decide(offer="refuted"), "reject")
        validate_corpus(self.corpus, 64)

    def test_missing_challenge_never_counts_as_clearance(self) -> None:
        records = {key: event for key, event in self.records.items() if not (event.kind == "receipt" and event.producer == "checker-b")}
        self.assertIsNone(self.decide(records))
        self.assertIsNone(self.decide(records, "refuted"))

    def test_refutation_requires_all_positive_receipts_too(self) -> None:
        records = {key: event for key, event in self.records.items() if not (event.kind == "receipt" and event.producer == "checker-a")}
        self.assertIsNone(self.decide(records, "refuted"))

    def test_missing_exact_source_bytes_prevents_action(self) -> None:
        records = {key: event for key, event in self.records.items() if not (event.kind == "artifact" and event.producer == "builder-a")}
        self.assertIsNone(self.decide(records))

    def test_missing_exact_evidence_bytes_prevents_action(self) -> None:
        records = {key: event for key, event in self.records.items() if not (event.kind == "artifact" and event.producer == "checker-b")}
        self.assertIsNone(self.decide(records))

    def test_epoch_fence_rejects_stale_contract(self) -> None:
        records = dict(self.records)
        for key, event in list(records.items()):
            if event.kind == "contract":
                payload = event.payload
                payload["epoch"] = 1
                replacement = Event.create(event.producer, event.sequence, event.kind, payload)
                del records[key]
                records[replacement.event_id] = replacement
        self.assertIsNone(self.decide(records))

    def test_wrong_source_cannot_supply_required_receipt(self) -> None:
        records = dict(self.records)
        for key, event in list(records.items()):
            if event.kind == "receipt" and event.payload["offer_id"] == "good" and event.producer == "checker-b" and event.payload["epoch"] == 2:
                payload = event.payload
                payload["source_sha256"] = "0" * 64
                replacement = Event.create(event.producer, event.sequence, event.kind, payload)
                del records[key]
                records[replacement.event_id] = replacement
        self.assertIsNone(self.decide(records))

    def test_builder_cannot_impersonate_checker_even_with_changed_contract(self) -> None:
        records = dict(self.records)
        required = next(event for event in records.values() if event.kind == "receipt" and event.payload["offer_id"] == "good" and event.producer == "checker-b" and event.payload["epoch"] == 2)
        fake = Event.create("builder-a", 999, "receipt", required.payload)
        del records[required.event_id]
        records[fake.event_id] = fake
        for key, event in list(records.items()):
            if event.kind == "contract" and event.payload["offer_id"] == "good":
                payload = event.payload
                for item in payload["required_receipts"]:
                    if item["event_id"] == required.event_id:
                        item["event_id"] = fake.event_id
                replacement = Event.create(event.producer, event.sequence, event.kind, payload)
                del records[key]
                records[replacement.event_id] = replacement
        self.assertIsNone(self.decide(records))

    def test_conflicting_authority_contracts_fail_closed(self) -> None:
        event = next(event for event in self.records.values() if event.kind == "contract" and event.payload["offer_id"] == "good")
        payload = event.payload
        payload["source_sha256"] = "a" * 64
        other = Event.create(event.producer, 999, "contract", payload)
        self.records[other.event_id] = other
        self.assertIsNone(self.decide())

    def test_authority_cannot_authorize_builder_self_challenge(self) -> None:
        required = next(event for event in self.records.values() if event.kind == "receipt" and event.payload["offer_id"] == "good" and event.producer == "checker-b" and event.payload["epoch"] == 2)
        evidence = next(event for event in self.records.values() if event.kind == "artifact" and event.payload["sha256"] == required.payload["evidence_sha256"])
        copied = Event.create("builder-a", 998, "artifact", evidence.payload)
        fake = Event.create("builder-a", 999, "receipt", required.payload)
        self.records[copied.event_id] = copied
        self.records[fake.event_id] = fake
        del self.records[required.event_id]
        for key, event in list(self.records.items()):
            if event.kind == "contract" and event.payload["offer_id"] == "good":
                payload = event.payload
                for item in payload["required_receipts"]:
                    if item["event_id"] == required.event_id:
                        item["event_id"] = fake.event_id
                        item["producer"] = "builder-a"
                replacement = Event.create(event.producer, event.sequence, event.kind, payload)
                del self.records[key]
                self.records[replacement.event_id] = replacement
        self.assertIsNone(self.decide())

    def test_frozen_helper_preserves_exact_bytes_and_supplied_audited_verdict(self) -> None:
        source = '{"file.py":"original bytes\\n"}\r\n'
        corpus = frozen_artifact_corpus([{"offer_id": "real-candidate", "source_utf8": source, "positive_evidence_utf8": "retained public receipt\r\n", "challenge_evidence_utf8": "retained independent failure receipt\n", "positive_passed": True, "challenge_clear": False}], label="Frozen study artifacts", provenance={"audit_sha256": "a" * 64})
        self.assertEqual(corpus["purpose"], "frozen-artifact-replay")
        self.assertEqual(corpus["expected_decisions"], {"real-candidate": "reject"})
        artifacts = [item["event"]["payload"] for item in corpus["schedule"] if item["event"]["kind"] == "artifact"]
        self.assertIn({"content_utf8": source, "sha256": hashlib.sha256(source.encode()).hexdigest()}, artifacts)
        self.assertTrue(run_trial(corpus, "durable-broker", "healthy", 2)["correct_complete"])

    def test_content_hash_and_artifact_byte_tampering_rejected(self) -> None:
        event = copy.deepcopy(self.corpus["schedule"][0]["event"])
        event["payload"]["tampered"] = True
        with self.assertRaises(ValueError):
            event_from_dict(event)
        corpus = copy.deepcopy(self.corpus)
        item = next(item for item in corpus["schedule"] if item["event"]["kind"] == "artifact")
        event = item["event"]
        event["payload"]["content_utf8"] += "tampered"
        item["event"] = Event.create(event["producer"], event["sequence"], event["kind"], event["payload"]).to_dict()
        with self.assertRaisesRegex(ValueError, "Artifact byte"):
            validate_corpus(corpus, 64)

    def test_fixed_contacts_and_round_snapshot_causality(self) -> None:
        trial = run_trial(self.corpus, "durable-broker", "healthy", 1, batch_size=100, retain_trace=True)
        self.assertTrue(trial["correct_complete"])
        self.assertEqual(trial["metrics"]["attempted_contacts"], 64 * len(PEERS))
        self.assertTrue(all(len(row) == len(PEERS) for row in trial["contact_trace"]))
        self.assertGreaterEqual(trial["decisions"]["reviewer-a"]["refuted"]["round"], 15)
        self.assertEqual(trial["contact_schedule_sha256"], digest(trial["contact_trace"]))

    def test_restarts_preserve_evidence_and_idempotency(self) -> None:
        trial = run_trial(self.corpus, "replicated-broker-client-failover", "persisted-restart", 22)
        self.assertTrue(trial["correct_complete"])
        self.assertEqual(trial["lost_facts"], 0)
        self.assertEqual(trial["duplicate_receipt_consumptions"], 0)
        self.assertEqual(trial["metrics"]["duplicate_origin_publications"], 1)
        self.assertEqual({item["peer"] for item in trial["restarts"]}, {"primary", "checker-a", "reviewer-a"})
        self.assertTrue(all(item["persisted_event_ids"] for item in trial["restarts"]))

    def test_permanent_missing_challenge_is_censored_not_accepted(self) -> None:
        with patch("simulation.benchmark_transport._reachable", side_effect=lambda source, target, scenario, round_index: "checker-b" not in (source, target)):
            trial = run_trial(self.corpus, "gossip", "healthy", 44)
        self.assertFalse(trial["correct_complete"])
        self.assertIsNone(trial["rounds_to_all_actionable"])
        self.assertEqual(trial["metrics"]["false_accepts"], 0)
        self.assertEqual(trial["actionable_decisions"], 0)
        self.assertGreater(trial["undelivered_reviewer_facts"], 0)

    def test_seed_reproducibility_and_environmental_field(self) -> None:
        first = run_trial(self.corpus, "gossip", "loss-reorder", 17)
        second = run_trial(self.corpus, "gossip", "loss-reorder", 17)
        self.assertEqual(first, second)
        self.assertNotEqual(first["contact_schedule_sha256"], run_trial(self.corpus, "gossip", "loss-reorder", 18)["contact_schedule_sha256"])
        self.assertEqual(_field(17, 8, "primary", "standby", 0, "loss"), _field(17, 8, "primary", "standby", 0, "loss"))
        self.assertNotEqual(_field(17, 8, "primary", "standby", 0, "loss"), _field(17, 8, "primary", "standby", 1, "loss"))

    def test_all_arms_fault_matrix_safe_with_equal_attempt_budget(self) -> None:
        for arm in ARMS:
            for scenario in SCENARIOS:
                with self.subTest(arm=arm, scenario=scenario):
                    trial = run_trial(self.corpus, arm, scenario, 20261002)
                    self.assertTrue(trial["correct_complete"])
                    self.assertEqual(trial["metrics"]["false_accepts"], 0)
                    self.assertEqual(trial["metrics"]["false_rejects"], 0)
                    self.assertEqual(trial["lost_facts"], 0)
                    self.assertEqual(trial["metrics"]["attempted_contacts"], 512)

    def test_single_broker_waits_for_primary_without_observer_bypass(self) -> None:
        broker = run_trial(self.corpus, "durable-broker", "primary-isolation", 20261002)
        failover = run_trial(self.corpus, "replicated-broker-client-failover", "primary-isolation", 20261002)
        self.assertGreater(broker["rounds_to_all_actionable"], 20)
        self.assertLess(failover["rounds_to_all_actionable"], broker["rounds_to_all_actionable"])
        self.assertGreater(failover["metrics"]["client_route_switches"], 0)

    def test_external_expected_outcomes_do_not_drive_local_decision(self) -> None:
        corpus = copy.deepcopy(self.corpus)
        corpus["expected_decisions"]["refuted"] = "accept"
        self.assertEqual(self.decide(offer="refuted"), "reject")
        with self.assertRaisesRegex(ValueError, "external expected outcome"):
            validate_corpus(corpus, 64)

    def test_corpus_rejects_late_publication_and_low_seed_count(self) -> None:
        corpus = copy.deepcopy(self.corpus)
        corpus["schedule"][0]["round"] = 100
        with self.assertRaises(ValueError):
            validate_corpus(corpus, 64)
        with self.assertRaises(ValueError):
            run_experiment(self.corpus, seeds=19)

    def test_summary_safety_precedes_speed_and_artifact_binding(self) -> None:
        def fake_trial(corpus, arm, scenario, seed, **kwargs):
            return {"arm": arm, "scenario": scenario, "seed": seed, "correct_complete": arm != "gossip", "rounds_to_all_actionable": 1 if arm == "gossip" else 20, "contacts_to_all_actionable": 8 if arm == "gossip" else 160, "lost_facts": 0, "metrics": {"false_accepts": int(arm == "gossip"), "false_rejects": 0, "pending_reviewer_offer_rounds": 0, "serialized_record_bytes": 0, "duplicate_record_deliveries": 0}}
        bound = hashlib.sha256(canonical(self.corpus).encode()).hexdigest()
        with patch("simulation.benchmark_transport.run_trial", side_effect=fake_trial):
            result = run_experiment(self.corpus, corpus_bytes_sha256=bound)
        self.assertEqual(result["corpus_input_bytes_sha256"], bound)
        self.assertEqual(len(result["trials"]), 20 * 3 * 7)
        self.assertTrue(all(row["loss"] == 20 for row in result["paired_win_draw_loss"]))


if __name__ == "__main__":
    unittest.main()
