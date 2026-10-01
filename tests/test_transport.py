"""Transport correctness checks; these do not measure software-project success."""

from dataclasses import FrozenInstanceError, replace
import hashlib
import json
import unittest

from gossip_harness.transport import Event, Mesh


class EventTests(unittest.TestCase):
    def test_hash_is_canonical_and_covers_all_public_content(self):
        event = Event.create("a", 0, "proposal", {"z": [1, {"b": True}], "a": "é"})
        equivalent = Event.create("a", 0, "proposal", {"a": "é", "z": [1, {"b": True}]})
        self.assertEqual(event, equivalent)
        body = event.to_dict()
        body.pop("event_id")
        digest = hashlib.sha256(json.dumps(body, sort_keys=True,
                              separators=(",", ":"), ensure_ascii=True).encode()).hexdigest()
        self.assertEqual(event.event_id, digest)
        self.assertNotEqual(event.event_id,
                            Event.create("a", 0, "proposal", event.payload, "other").event_id)

    def test_payload_is_defensively_immutable(self):
        payload = {"evidence": [{"commit": "abc"}]}
        event = Event.create("a", 0, "observation", payload)
        payload["evidence"][0]["commit"] = "changed"
        event.payload["evidence"].clear()
        event.to_dict()["payload"]["evidence"].clear()
        self.assertEqual(event.payload, {"evidence": [{"commit": "abc"}]})
        event.verify()
        with self.assertRaises(FrozenInstanceError):
            event.kind = "approved"

    def test_publish_rejects_tampering_and_wrong_origin(self):
        event = Event.create("a", 0, "observation", {"ok": True})
        mesh = Mesh(["a", "b"], "bus")
        tampered = (replace(event, kind="approved"), replace(event, event_id="bad"),
                    replace(event, _payload_json='{"ok":false}'),
                    replace(event, _payload_json="not-json"),
                    replace(event, sequence=-1))
        for record in tampered:
            with self.subTest(record=record), self.assertRaises(ValueError):
                mesh.publish("a", record)
        with self.assertRaises(ValueError):
            mesh.publish("b", event)
        self.assertEqual(mesh.events("a"), ())

    def test_rejects_non_json_or_ambiguous_payloads(self):
        for payload in ({"bad": float("nan")}, {1: "coerced key"},
                        {"bad": (1, 2)}, {"bad": object()}, []):
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                Event.create("a", 0, "observation", payload)


class TransportTests(unittest.TestCase):
    def setUp(self):
        self.peers = [f"worker-{index}" for index in range(8)]

    def populated(self, mode, seed=17, batch_size=2):
        mesh = Mesh(self.peers, mode, seed=seed, fanout=2, batch_size=batch_size)
        records = []
        for peer in self.peers:
            for sequence in range(3):
                event = Event.create(peer, sequence, "observation", {"value": sequence})
                mesh.publish(peer, event)
                records.append(event)
        return mesh, records

    def test_bus_and_gossip_converge_to_same_records(self):
        expected = None
        for mode in ("bus", "gossip"):
            mesh, records = self.populated(mode)
            self.assertTrue(mesh.converge())
            ids = tuple(sorted(event.event_id for event in records))
            self.assertEqual(mesh.stats["event_deliveries"], len(records) * (len(self.peers) - 1))
            for peer in self.peers:
                self.assertEqual(tuple(event.event_id for event in mesh.events(peer)), ids)
            if expected is None:
                expected = mesh.events(self.peers[0])
            self.assertEqual(mesh.events(self.peers[0]), expected)

    def test_partition_prevents_crossing_and_healing_repairs(self):
        for mode in ("bus", "gossip"):
            with self.subTest(mode=mode):
                mesh, records = self.populated(mode)
                left, right = set(self.peers[:4]), set(self.peers[4:])
                self.assertFalse(mesh.converge(20, partitions=[left, right]))
                for peer in left:
                    self.assertTrue(all(event.producer in left for event in mesh.events(peer)))
                for peer in right:
                    self.assertTrue(all(event.producer in right for event in mesh.events(peer)))
                self.assertTrue(mesh.converge())
                self.assertTrue(all(mesh.has(peer, event.event_id)
                                    for peer in self.peers for event in records))

    def test_dropped_contacts_retry_and_are_counted(self):
        for mode in ("bus", "gossip"):
            mesh = Mesh(["a", "b"], mode)
            event = Event.create("a", 0, "observation", {})
            mesh.publish("a", event)
            self.assertEqual(mesh.step(drop_rate=1), 0)
            self.assertEqual(mesh.stats["contacts"], 1 if mode == "bus" else 2)
            self.assertFalse(mesh.has("b", event.event_id))
            self.assertTrue(mesh.converge(10))
            self.assertEqual(mesh.stats["event_deliveries"], 1)

    def test_bus_attempts_only_one_exchange_per_leaf_per_round(self):
        mesh, _ = self.populated("bus")
        for drop_rate in (0, 1, 0.4):
            before = mesh.stats["contacts"]
            mesh.step(drop_rate=drop_rate)
            self.assertEqual(mesh.stats["contacts"] - before, len(self.peers) - 1)

    def test_bus_leaf_origin_requires_ingress_and_egress_rounds(self):
        mesh = Mesh(["a", "b", "c"], "bus", broker="b")
        event = Event.create("a", 0, "observation", {})
        mesh.publish("a", event)
        self.assertEqual(mesh.step(), 1)
        self.assertTrue(mesh.has("b", event.event_id))
        self.assertFalse(mesh.has("c", event.event_id))
        self.assertEqual(mesh.step(), 1)
        self.assertTrue(mesh.has("c", event.event_id))
        self.assertEqual(mesh.stats["contacts"], 4)

    def test_bus_leaves_cannot_exchange_without_broker_connectivity(self):
        mesh = Mesh(["broker", "leaf-a", "leaf-b"], "bus")
        event = Event.create("leaf-a", 0, "observation", {})
        mesh.publish("leaf-a", event)
        partitions = [{"broker"}, {"leaf-a", "leaf-b"}]
        self.assertFalse(mesh.converge(3, partitions=partitions))
        self.assertTrue(mesh.has("leaf-a", event.event_id))
        self.assertFalse(mesh.has("leaf-b", event.event_id))
        self.assertFalse(mesh.has("broker", event.event_id))
        self.assertTrue(mesh.converge(2))

    def test_partial_loss_is_reproducible_and_eventually_recovers(self):
        first, _ = self.populated("gossip", seed=31)
        second, _ = self.populated("gossip", seed=31)
        for _ in range(12):
            self.assertEqual(first.step(drop_rate=0.4), second.step(drop_rate=0.4))
            self.assertEqual(first.stats, second.stats)
            for peer in self.peers:
                self.assertEqual(first.events(peer), second.events(peer))
        self.assertTrue(first.converge(100, drop_rate=0.4))

    def test_single_record_batches_do_not_starve_finite_backlog(self):
        for mode in ("bus", "gossip"):
            mesh, records = self.populated(mode, batch_size=1)
            self.assertTrue(mesh.converge(200))
            self.assertEqual(len(mesh.events(self.peers[0])), len(records))

    def test_snapshot_round_does_not_relay_newly_received_records(self):
        # Force a -> b, b -> c, c -> b contacts. Newly learned a records must
        # not reach c until the next round, even though b runs after a.
        class FixedSelections:
            def __init__(self):
                self.targets = iter((["b"], ["c"], ["b"]))

            def sample(self, candidates, count):
                return next(self.targets)

        mesh = Mesh(["a", "b", "c"], "gossip", fanout=1)
        mesh._rng = FixedSelections()
        event = Event.create("a", 0, "observation", {})
        mesh.publish("a", event)
        self.assertEqual(mesh.step(), 1)
        self.assertTrue(mesh.has("b", event.event_id))
        self.assertFalse(mesh.has("c", event.event_id))

    def test_duplicate_publications_are_idempotent_and_stats_are_copies(self):
        mesh = Mesh(["a", "b"], "gossip")
        event = Event.create("a", 0, "proposal", {})
        mesh.publish("a", event)
        mesh.publish("a", event)
        self.assertEqual(mesh.step(), 1)
        self.assertEqual(mesh.stats["event_deliveries"], 1)
        self.assertEqual(mesh.stats["duplicate_deliveries"], 1)
        mesh.stats["contacts"] = 99
        self.assertEqual(mesh.stats["contacts"], 2)
        self.assertEqual(mesh.step(), 0)

    def test_topics_and_refutations_do_not_change_replication_or_truth(self):
        mesh = Mesh(["a", "b"], "bus")
        proposal = Event.create("a", 0, "proposal", {"claim": "tests pass"}, "private-looking")
        refutation = Event.create("b", 0, "refutation", {"target": proposal.event_id})
        mesh.publish("a", proposal)
        mesh.publish("b", refutation)
        self.assertTrue(mesh.converge())
        self.assertEqual(len(mesh.events("a")), 2)
        self.assertTrue(mesh.has("b", proposal.event_id))

    def test_invalid_fault_configuration_does_not_advance_state(self):
        mesh = Mesh(["a", "b"], "gossip")
        for kwargs in ({"drop_rate": float("nan")}, {"drop_rate": 1.1},
                       {"partitions": [{"a"}]},
                       {"partitions": [{"a", "b"}, {"b"}]},
                       {"partitions": [{"a"}, {"unknown"}]}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                mesh.step(**kwargs)
        self.assertEqual(mesh.stats["rounds"], 0)


if __name__ == "__main__":
    unittest.main()
