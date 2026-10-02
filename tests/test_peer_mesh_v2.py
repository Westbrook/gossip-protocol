"""V2 mesh contracts: isolated stores and actual bounded loopback TCP exchange."""
from __future__ import annotations

from contextlib import ExitStack
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import selectors
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from typing import Any, Callable

from gossip_harness.peer_mesh_v2 import (
    MeshConfig, MeshError, MeshLimits, MeshNode, observer_request,
)
from gossip_harness.peer_project_contract_v2 import EvidenceRef


ROSTER = ("origin", "relay", "recipient")
CONTRACT = hashlib.sha256(b"mesh-v2-offline-test-contract").hexdigest()
TRANSPORT_KEY = "t" * 32
OBSERVER_KEY = "o" * 32


def config(root: Path, name: str = "origin", **changes: Any) -> MeshConfig:
    values: dict[str, Any] = dict(root=root, node_id=name, cohort_id="mesh-test-cohort",
                                  execution_contract_sha256=CONTRACT, roster=ROSTER,
                                  transport_key=TRANSPORT_KEY, observer_key=OBSERVER_KEY,
                                  interval=0.05, fanout=2)
    values.update(changes)
    return MeshConfig(**values)


class MeshStoreV2Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.root = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))

    def node(self, name: str = "origin", **changes: Any) -> MeshNode:
        node = MeshNode(config(self.root / name, name, **changes))
        self.stack.callback(node.close)
        return node

    def test_publish_round_trip_is_local_exact_and_durable(self) -> None:
        node = self.node()
        data = b"evidence\x00\xff" + bytes(range(256)) * 300
        ref = node.publish("source", data, "publish-1")
        self.assertIsInstance(ref, EvidenceRef)
        self.assertEqual((ref.producer, ref.kind, ref.payload_sha256),
                         ("origin", "source", hashlib.sha256(data).hexdigest()))
        self.assertEqual(node.arrived(), (ref,))
        self.assertEqual(node.resolve(ref), data)
        self.assertTrue(node.want(ref))
        node.close()
        reopened = self.node()
        self.assertEqual(reopened.arrived(), (ref,))
        self.assertEqual(reopened.resolve(ref), data)
        self.assertEqual(reopened.publish("source", data, "publish-1"), ref)
        self.assertEqual(reopened.summary()["events"], 1)
        self.assertEqual(reopened.summary()["commands"], 1)

    def test_conflicting_command_replay_cannot_replace_kind_or_bytes(self) -> None:
        node = self.node()
        ref = node.publish("source", b"original", "same-command")
        node.close()
        reopened = self.node()
        for kind, payload in (("review", b"original"), ("source", b"changed")):
            with self.subTest(kind=kind), self.assertRaises(MeshError):
                reopened.publish(kind, payload, "same-command")
        self.assertEqual(reopened.resolve(ref), b"original")
        self.assertEqual(reopened.arrived(), (ref,))
        self.assertEqual(reopened.summary()["commands"], 1)

    def test_separate_commands_share_payload_capacity_but_not_event_identity(self) -> None:
        node = self.node(limits=replace(MeshLimits(), max_payloads=1))
        first = node.publish("source", b"same bytes", "first")
        second = node.publish("review", b"same bytes", "second")
        self.assertNotEqual(first.event_id, second.event_id)
        self.assertEqual(first.payload_sha256, second.payload_sha256)
        self.assertEqual(node.summary()["events"], 2)
        self.assertEqual(node.summary()["payloads"], 1)
        self.assertEqual(node.summary()["reserved_bytes"], len(b"same bytes"))
        self.assertEqual(node.resolve(second), b"same bytes")

    def test_event_quota_preserves_exact_replay_and_rejects_new_command_atomically(self) -> None:
        node = self.node(limits=replace(MeshLimits(), max_events=1))
        first = node.publish("source", b"one", "first")
        self.assertEqual(node.publish("source", b"one", "first"), first)
        with self.assertRaises(MeshError):
            node.publish("source", b"two", "second")
        self.assertEqual(node.arrived(), (first,))
        self.assertEqual(node.summary()["commands"], 1)
        self.assertEqual(node.summary()["payloads"], 1)
        self.assertEqual(node.resolve(first), b"one")

    def test_payload_and_byte_quota_reject_publish_without_orphan_events(self) -> None:
        for limits in (replace(MeshLimits(), max_payloads=1),
                       replace(MeshLimits(), max_reserved_bytes=3)):
            with self.subTest(limits=limits):
                with ExitStack() as stack:
                    root = Path(stack.enter_context(tempfile.TemporaryDirectory()))
                    node = MeshNode(config(root, limits=limits))
                    stack.callback(node.close)
                    ref = node.publish("source", b"one", "first")
                    with self.assertRaises(MeshError):
                        node.publish("source", b"two", "second")
                    self.assertEqual(node.arrived(), (ref,))
                    self.assertEqual(node.summary()["reserved_bytes"], 3)
                    self.assertEqual(node.summary()["commands"], 1)

    def test_payload_maximum_rejects_oversize_without_mutation(self) -> None:
        node = self.node(limits=replace(MeshLimits(), max_payload_bytes=3))
        with self.assertRaises(MeshError):
            node.publish("source", b"four", "too-large")
        self.assertEqual(node.arrived(), ())
        self.assertEqual(node.summary()["payloads"], 0)
        self.assertEqual(node.summary()["commands"], 0)
        self.assertEqual(node.resolve(node.publish("source", b"one", "fits")), b"one")

    def test_arrival_is_metadata_only_and_want_reserves_once(self) -> None:
        sender, receiver = self.node(), self.node("recipient")
        ref = sender.publish("source", b"not unsolicited", "first")
        cursor, notices = sender.store.export(0)
        self.assertGreater(cursor, 0)
        receiver.store.merge(notices)
        receiver.store.merge(notices)
        self.assertEqual(receiver.arrived(), (ref,))
        self.assertIsNone(receiver.resolve(ref))
        self.assertEqual(receiver.summary()["payloads"], 0)
        self.assertTrue(receiver.want(ref))
        self.assertTrue(receiver.want(ref))
        self.assertEqual(receiver.summary()["payloads"], 1)
        self.assertEqual(receiver.summary()["reserved_bytes"], len(b"not unsolicited"))
        self.assertIsNone(receiver.resolve(ref))

    def test_quota_denied_demand_preserves_arrived_notice_and_existing_reservation(self) -> None:
        sender = self.node()
        receiver = self.node("recipient", limits=replace(MeshLimits(), max_reserved_bytes=3))
        first = sender.publish("source", b"one", "first")
        second = sender.publish("source", b"second", "second")
        receiver.store.merge(sender.store.export(0)[1])
        self.assertFalse(receiver.want(second))
        self.assertEqual(receiver.summary()["reserved_bytes"], 0)
        self.assertEqual(receiver.summary()["payloads"], 0)
        self.assertTrue(receiver.want(first))
        self.assertFalse(receiver.want(second))
        self.assertIn(second, receiver.arrived())
        self.assertEqual(receiver.summary()["reserved_bytes"], 3)
        self.assertEqual(receiver.summary()["payloads"], 1)
        receiver.close()
        reopened = self.node("recipient", limits=replace(MeshLimits(), max_reserved_bytes=3))
        self.assertIn(second, reopened.arrived())
        self.assertFalse(reopened.want(second))
        self.assertTrue(reopened.want(first))
        self.assertEqual(reopened.summary()["reserved_bytes"], 3)

    def test_resolve_and_want_require_every_field_of_an_arrived_reference(self) -> None:
        node = self.node()
        ref = node.publish("source", b"exact bytes", "first")
        variants = (replace(ref, event_id="0" * 64), replace(ref, producer="relay"),
                    replace(ref, kind="review"), replace(ref, payload_sha256="0" * 64))
        for changed in variants:
            with self.subTest(changed=changed):
                self.assertFalse(node.want(changed))
                self.assertIsNone(node.resolve(changed))
        self.assertEqual(node.resolve(ref), b"exact bytes")
        self.assertEqual(node.summary()["payloads"], 1)

    def test_export_cursor_propagates_only_new_notices_and_returns_detached_records(self) -> None:
        sender, receiver = self.node(), self.node("recipient")
        first = sender.publish("source", b"first", "first")
        cursor, notices = sender.store.export(0)
        receiver.store.merge(notices)
        self.assertEqual(sender.store.export(cursor)[1], [])
        notices[0].clear()
        self.assertEqual(receiver.arrived(), (first,))
        self.assertTrue(sender.store.notice(first))
        second = sender.publish("review", b"second", "second")
        next_cursor, newer = sender.store.export(cursor)
        self.assertGreater(next_cursor, cursor)
        receiver.store.merge(newer)
        self.assertEqual(set(receiver.arrived()), {first, second})
        self.assertEqual(receiver.summary()["payloads"], 0)

    def test_malformed_notice_is_rejected_without_arrival_or_reservation(self) -> None:
        sender, receiver = self.node(), self.node("recipient")
        ref = sender.publish("source", b"first", "first")
        notice = sender.store.notice(ref)
        assert notice is not None
        for malformed in ({}, {**deepcopy(notice), "unexpected": True}):
            with self.subTest(malformed=malformed), self.assertRaises(MeshError):
                receiver.store.merge([malformed])
        with self.assertRaises(MeshError):
            receiver.store.merge([notice, {}])
        self.assertEqual(receiver.arrived(), ())
        self.assertEqual(receiver.summary()["payloads"], 0)

    def test_origin_equivocation_and_unknown_own_origin_history_fail_closed(self) -> None:
        sender, receiver = self.node(), self.node("recipient")
        first = sender.publish("source", b"first", "first")
        receiver.store.merge(sender.store.export(0)[1])
        alternate = MeshNode(config(self.root / "alternate-origin"))
        self.stack.callback(alternate.close)
        alternate.publish("source", b"conflicting first sequence", "another-command")
        with self.assertRaises(MeshError):
            receiver.store.merge(alternate.store.export(0)[1])
        self.assertEqual(receiver.arrived(), (first,))
        with self.assertRaises(MeshError):
            alternate.store.merge(sender.store.export(0)[1])
        empty_origin = MeshNode(config(self.root / "empty-origin"))
        self.stack.callback(empty_origin.close)
        with self.assertRaises(MeshError):
            empty_origin.store.merge(sender.store.export(0)[1])
        self.assertEqual(empty_origin.arrived(), ())

    def test_partial_chunks_survive_restart_and_reject_corruption(self) -> None:
        sender, receiver = self.node(), self.node("recipient")
        chunk_bytes = MeshLimits().chunk_bytes
        payload = b"x" * chunk_bytes + b"tail"
        ref = sender.publish("source", payload, "partial")
        receiver.store.merge(sender.store.export(0)[1])
        with self.assertRaises(MeshError):
            receiver.store.accept_chunk(ref.payload_sha256, 1, b"tail")
        self.assertTrue(receiver.want(ref))
        with self.assertRaises(MeshError):
            receiver.store.accept_chunk(ref.payload_sha256, 1, b"fail")
        receiver.store.accept_chunk(ref.payload_sha256, 1, b"tail")
        self.assertIsNone(receiver.resolve(ref))
        self.assertEqual(receiver.summary()["stored_bytes"], 4)
        receiver.close()
        reopened = self.node("recipient")
        self.assertIsNone(reopened.resolve(ref))
        self.assertEqual(reopened.store.read_chunk(ref.payload_sha256, 1), b"tail")
        reopened.store.accept_chunk(ref.payload_sha256, 0, payload[:chunk_bytes])
        self.assertEqual(reopened.resolve(ref), payload)
        self.assertEqual(reopened.summary()["complete_payloads"], 1)

    def test_each_neighbor_sees_every_pending_demand_when_contacts_interleave(self) -> None:
        sender = self.node()
        receiver = self.node("recipient", limits=replace(MeshLimits(), chunk_batch=1))
        refs = (sender.publish("source", b"first object", "first"),
                sender.publish("source", b"second object", "second"))
        receiver.store.merge(sender.store.export(0)[1])
        for ref in refs:
            self.assertTrue(receiver.want(ref))
        by_neighbor: dict[str, set[str]] = {"origin": set(), "relay": set()}
        for _ in range(2):
            for peer in ("origin", "relay"):
                needs = receiver._local_needs(peer, "outgoing")
                self.assertEqual(len(needs), 1)
                by_neighbor[peer].add(needs[0]["sha"])
        expected = {ref.payload_sha256 for ref in refs}
        self.assertEqual(by_neighbor["origin"], expected)
        self.assertEqual(by_neighbor["relay"], expected)

    def test_reopen_audits_corrupt_bytes_commands_sequence_and_completeness(self) -> None:
        mutations = (
            ("UPDATE chunks SET data=?", (b"tampered",)),
            ("UPDATE commands SET request_sha=?", ("0" * 64,)),
            ("UPDATE metadata SET value=? WHERE key='next_sequence'", (b"1",)),
            ("UPDATE metadata SET value=? WHERE key='membership'", (b"[]",)),
            ("UPDATE payloads SET complete=0", ()),
        )
        for sql, values in mutations:
            with self.subTest(sql=sql), ExitStack() as stack:
                root = Path(stack.enter_context(tempfile.TemporaryDirectory()))
                node = MeshNode(config(root))
                stack.callback(node.close)
                node.publish("source", b"original", "first")
                node.close()
                with sqlite3.connect(root / "mesh.sqlite") as database:
                    database.execute(sql, values)
                with self.assertRaises(MeshError):
                    reopened = MeshNode(config(root))
                    stack.callback(reopened.close)

    def test_reopen_rejects_removed_constraints_even_when_retained_rows_are_valid(self) -> None:
        node = self.node()
        node.publish("source", b"retained", "first")
        node.close()
        with sqlite3.connect(self.root / "origin" / "mesh.sqlite") as database:
            database.executescript("""
                ALTER TABLE chunks RENAME TO old_chunks;
                CREATE TABLE chunks (sha TEXT NOT NULL, idx INTEGER NOT NULL, data BLOB NOT NULL);
                INSERT INTO chunks SELECT * FROM old_chunks;
                DROP TABLE old_chunks;
            """)
            self.assertEqual(database.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            self.assertEqual(database.execute("PRAGMA foreign_key_check").fetchall(), [])
            self.assertEqual(database.execute("SELECT data FROM chunks").fetchall(), [(b"retained",)])
        with self.assertRaises(MeshError):
            reopened = MeshNode(config(self.root / "origin"))
            self.stack.callback(reopened.close)

    def test_publication_does_not_overwrite_corrupted_live_payload_or_add_an_event(self) -> None:
        node = self.node()
        ref = node.publish("source", b"original", "first")
        with sqlite3.connect(self.root / "origin" / "mesh.sqlite") as database:
            database.execute("UPDATE chunks SET data=?", (b"tampered",))
        with self.assertRaises(MeshError):
            node.resolve(ref)
        with self.assertRaises(MeshError):
            node.publish("review", b"original", "second")
        self.assertEqual(node.arrived(), (ref,))
        self.assertEqual(node.summary()["events"], 1)
        self.assertEqual(node.summary()["commands"], 1)
        with sqlite3.connect(self.root / "origin" / "mesh.sqlite") as database:
            self.assertEqual(database.execute("SELECT data FROM chunks").fetchall(), [(b"tampered",)])

    def test_database_and_sidecar_symlinks_are_refused_before_touching_their_targets(self) -> None:
        for suffix in ("", "-wal", "-shm", "-journal"):
            with self.subTest(suffix=suffix), ExitStack() as stack:
                root = Path(stack.enter_context(tempfile.TemporaryDirectory()))
                target = root / "sentinel"
                target.write_bytes(b"untouched")
                (root / ("mesh.sqlite" + suffix)).symlink_to(target)
                with self.assertRaises(MeshError):
                    node = MeshNode(config(root))
                    stack.callback(node.close)
                self.assertEqual(target.read_bytes(), b"untouched")

    def test_config_json_rejects_coercible_wrong_shapes(self) -> None:
        valid: dict[str, Any] = {
            "root": str(self.root / "json"), "node_id": "origin", "cohort_id": "mesh-test-cohort",
            "execution_contract_sha256": CONTRACT, "roster": list(ROSTER),
            "transport_key": TRANSPORT_KEY, "observer_key": OBSERVER_KEY,
        }
        self.assertEqual(MeshConfig.from_dict(valid), config(self.root / "json"))
        invalid: list[Any] = [None, [], {**valid, "extra": True}, {**valid, "root": self.root},
                             {**valid, "roster": "origin"}, {**valid, "roster": ROSTER},
                             {**valid, "roster": dict.fromkeys(ROSTER)},
                             {**valid, "brokers": "origin"}, {**valid, "brokers": ()},
                             {**valid, "limits": []}, {**valid, "limits": MeshLimits()},
                             {key: value for key, value in valid.items() if key != "transport_key"}]
        for value in invalid:
            with self.subTest(value=repr(value)[:100]), self.assertRaises(MeshError):
                MeshConfig.from_dict(value)

    def test_repeated_arrival_reads_stay_complete_after_rejected_batch_and_later_merge(self) -> None:
        sender, receiver = self.node(), self.node("recipient")
        first = sender.publish("source", b"first", "first")
        cursor, notices = sender.store.export(0)
        receiver.store.merge(notices)
        self.assertEqual(receiver.arrived(), (first,))
        second = sender.publish("source", b"second", "second")
        _, later = sender.store.export(cursor)
        with self.assertRaises(MeshError):
            receiver.store.merge([*later, {}])
        self.assertEqual(receiver.arrived(), (first,))
        self.assertEqual(receiver.arrived(), (first,))
        receiver.store.merge(later)
        self.assertEqual(receiver.arrived(), (first, second))
        self.assertEqual(receiver.arrived(), (first, second))
        receiver.close()
        self.assertEqual(self.node("recipient").arrived(), (first, second))

    def test_root_has_one_live_owner_and_reopens_after_close(self) -> None:
        node = self.node()
        with self.assertRaises(MeshError):
            MeshNode(config(self.root / "origin"))
        node.close()
        reopened = self.node()
        self.assertEqual(reopened.arrived(), ())

    def test_durable_root_is_bound_to_node_cohort_contract_roster_and_transport_key(self) -> None:
        node = self.node()
        ref = node.publish("source", b"bound", "first")
        node.close()
        variants = (dict(node_id="relay"), dict(cohort_id="another-cohort"),
                    dict(execution_contract_sha256="0" * 64),
                    dict(roster=("origin", "relay")), dict(transport_key="x" * 32))
        for changes in variants:
            with self.subTest(changes=tuple(changes)), self.assertRaises(MeshError):
                MeshNode(config(self.root / "origin", **changes))
        self.assertEqual(self.node().resolve(ref), b"bound")

    def test_sixty_four_roles_are_supported_but_invalid_rosters_fail_closed(self) -> None:
        names = tuple(f"role-{index}" for index in range(64))
        node = self.node("role-0", roster=names)
        self.assertEqual(node.publish("source", b"roster", "first").producer, "role-0")
        invalid = (names + ("role-64",), ("origin", "origin"), ("relay",), ())
        for roster in invalid:
            with self.subTest(roster=roster), self.assertRaises(MeshError):
                MeshNode(config(self.root / "invalid", roster=roster))

    def test_peer_configuration_is_complete_and_immutable(self) -> None:
        node = self.node()
        peers = dict(zip(ROSTER, (41001, 41002, 41003)))
        with self.assertRaises(MeshError):
            node.configure({"origin": 41001})
        node.configure(peers)
        node.configure(dict(peers))
        with self.assertRaises(MeshError):
            node.configure({**peers, "relay": 42002})


class MeshTCPV2Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.root = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        self.nodes: dict[str, MeshNode] = {}
        self.ports: dict[str, int] = {}

    def start(self, name: str, *, expected_port: int | None = None) -> MeshNode:
        node = MeshNode(config(self.root / name, name))
        self.stack.callback(node.close)
        ready = node.start()
        self.assertEqual(ready["node_id"], name)
        self.assertGreater(ready["pid"], 0)
        self.assertGreater(ready["port"], 0)
        if expected_port is not None:
            self.assertEqual(ready["port"], expected_port)
        self.nodes[name] = node
        self.ports[name] = ready["port"]
        return node

    def cluster(self, *, configure: bool = True) -> tuple[MeshNode, MeshNode, MeshNode]:
        nodes = tuple(self.start(name) for name in ROSTER)
        if configure:
            for node in nodes:
                node.configure(dict(self.ports))
        return nodes[0], nodes[1], nodes[2]

    def wait(self, predicate: Callable[[], bool], reason: str) -> None:
        deadline = time.monotonic() + 8
        wake = threading.Event()
        while time.monotonic() < deadline:
            if predicate():
                return
            wake.wait(min(0.02, max(0, deadline - time.monotonic())))
        self.fail(f"Mesh condition exceeded bounded deadline: {reason}")

    def test_notices_travel_without_payloads_then_demand_uses_a_surviving_relay(self) -> None:
        origin, relay, recipient = self.cluster()
        payload = bytes(range(256)) * 300 + b"last chunk"
        ref = origin.publish("candidate-bundle", payload, "source-command")
        self.wait(lambda: ref in relay.arrived() and ref in recipient.arrived(), "notice arrival")
        threading.Event().wait(0.15)
        for node in (relay, recipient):
            self.assertIsNone(node.resolve(ref))
            self.assertEqual(node.summary()["payloads"], 0)
            self.assertEqual(node.summary()["stored_bytes"], 0)
        self.assertTrue(relay.want(ref))
        self.wait(lambda: relay.resolve(ref) == payload, "relay demanded complete chunks")
        self.assertIsNone(recipient.resolve(ref))
        origin.close()
        self.assertTrue(recipient.want(ref))
        self.wait(lambda: recipient.resolve(ref) == payload, "recipient recovered through surviving relay")

    def test_partition_hides_evidence_until_healed_then_demand_resolves(self) -> None:
        origin, relay, recipient = self.cluster()
        origin.block(("recipient",))
        relay.block(("recipient",))
        recipient.block(("origin", "relay"))
        payload = b"partitioned evidence"
        ref = origin.publish("source", payload, "partition-command")
        self.wait(lambda: ref in relay.arrived(), "unpartitioned peer arrival")
        threading.Event().wait(0.15)
        self.assertNotIn(ref, recipient.arrived())
        self.assertFalse(recipient.want(ref))
        self.assertIsNone(recipient.resolve(ref))
        for node in (origin, relay, recipient):
            node.block(())
        self.wait(lambda: ref in recipient.arrived(), "healed partition notice arrival")
        self.assertIsNone(recipient.resolve(ref))
        self.assertTrue(recipient.want(ref))
        self.wait(lambda: recipient.resolve(ref) == payload, "healed partition payload demand")

    def test_observer_can_configure_summarize_and_block_but_cannot_access_evidence(self) -> None:
        origin, _, _ = self.cluster(configure=False)
        for name in ROSTER:
            observer_request(self.ports[name], name, OBSERVER_KEY, "configure", {"peers": self.ports})
        ref = origin.publish("source", b"private evidence", "observer-boundary")
        summary = observer_request(self.ports["origin"], "origin", OBSERVER_KEY, "summary")
        self.assertEqual(summary["events"], 1)
        observer_request(self.ports["origin"], "origin", OBSERVER_KEY, "block", {"peers": ["recipient"]})
        for operation in ("publish", "sync", "read"):
            with self.subTest(operation=operation), self.assertRaises(MeshError):
                observer_request(self.ports["origin"], "origin", OBSERVER_KEY, operation, {})
        for key in (TRANSPORT_KEY, "invalid-observer-key" * 2):
            with self.subTest(key=key[:5]), self.assertRaises(MeshError):
                observer_request(self.ports["origin"], "origin", key, "summary")
        with self.assertRaises(MeshError):
            observer_request(self.ports["origin"], "relay", OBSERVER_KEY, "summary")
        self.assertEqual(origin.arrived(), (ref,))
        self.assertEqual(origin.resolve(ref), b"private evidence")

    def test_same_port_restart_preserves_arrival_demand_and_partition_faults(self) -> None:
        origin, relay, recipient = self.cluster()
        payload = b"restart evidence" * 5000
        ref = origin.publish("source", payload, "restart-command")
        self.wait(lambda: ref in relay.arrived() and ref in recipient.arrived(), "pre-restart arrival")
        recipient.block(("origin", "relay"))
        self.assertTrue(recipient.want(ref))
        port = self.ports["recipient"]
        recipient.close()
        reopened = self.start("recipient", expected_port=port)
        reopened.configure(dict(self.ports))
        self.assertIn(ref, reopened.arrived())
        self.assertEqual(reopened.summary()["reserved_bytes"], len(payload))
        threading.Event().wait(0.2)
        self.assertIsNone(reopened.resolve(ref))
        reopened.block(())
        self.wait(lambda: reopened.resolve(ref) == payload, "durable demand resumes after unblock")
        self.assertEqual(reopened.summary()["complete_payloads"], 1)

    def test_cli_announces_its_own_process_serves_observer_and_stops_cleanly(self) -> None:
        path = self.root / "cli-config.json"
        path.write_text(json.dumps({
            "root": str(self.root / "cli"), "node_id": "origin", "cohort_id": "mesh-test-cohort",
            "execution_contract_sha256": CONTRACT, "roster": list(ROSTER),
            "transport_key": TRANSPORT_KEY, "observer_key": OBSERVER_KEY,
        }), encoding="utf-8")
        process = subprocess.Popen(
            [sys.executable, "-m", "gossip_harness.peer_mesh_v2", "--config", str(path)],
            cwd=Path(__file__).resolve().parents[1], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        assert process.stdout is not None and process.stderr is not None
        self.stack.callback(process.stdout.close)
        self.stack.callback(process.stderr.close)

        def stop_owned_process() -> None:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=4)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=4)

        self.stack.callback(stop_owned_process)
        raw = b""
        deadline = time.monotonic() + 8
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            while b"\n" not in raw and time.monotonic() < deadline:
                if selector.select(timeout=min(0.05, max(0, deadline - time.monotonic()))):
                    chunk = os.read(process.stdout.fileno(), 4096)
                    if not chunk:
                        break
                    raw += chunk
                    self.assertLessEqual(len(raw), 4096)
        self.assertIn(b"\n", raw, "CLI did not announce readiness before bounded deadline")
        ready = json.loads(raw.split(b"\n", 1)[0])
        self.assertNotEqual(process.pid, os.getpid())
        self.assertEqual(ready["pid"], process.pid)
        self.assertEqual(ready["node_id"], "origin")
        summary = observer_request(ready["port"], "origin", OBSERVER_KEY, "summary")
        self.assertEqual(summary["pid"], process.pid)
        self.assertEqual(summary["events"], 0)
        self.assertNotIn(TRANSPORT_KEY.encode(), raw)
        self.assertNotIn(OBSERVER_KEY.encode(), raw)
        process.terminate()
        self.assertEqual(process.wait(timeout=8), 0)


if __name__ == "__main__":
    unittest.main()
