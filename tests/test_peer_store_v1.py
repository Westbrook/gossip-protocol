"""Offline contract checks; process-exit durability belongs to runtime tests."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import hashlib
from pathlib import Path
import random
import sqlite3
import tempfile
import unittest
from typing import Any

from gossip_harness.peer_store_v1 import (
    MAX_ARTIFACT_BYTES, MAX_ARTIFACT_DEPTH, MAX_BATCH, MAX_EVENTS, PROTOCOL, Store, StoreError,
    canonical_bytes, strict_loads,
)


def fact(sequence: int, *, producer: str = "origin", content: dict | None = None,
         kind: str = "observation") -> tuple[dict, dict]:
    envelope = {"protocol": PROTOCOL, "content": content if content is not None else {"value": sequence}}
    sha = hashlib.sha256(canonical_bytes(envelope)).hexdigest()
    event = {"protocol": PROTOCOL, "producer": producer, "sequence": sequence,
             "kind": kind, "artifact_sha256": sha}
    event["event_id"] = hashlib.sha256(canonical_bytes(event)).hexdigest()
    return event, {sha: envelope}


def batch(*facts: tuple[dict, dict]) -> dict:
    return deepcopy({"events": [item[0] for item in facts],
                     "artifacts": {sha: value for item in facts for sha, value in item[1].items()}})


class PeerStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.addCleanup(self.temp.cleanup)

    def store(self, name: str = "node", **kwargs) -> Store:
        return Store(self.root / f"{name}.sqlite", name, **kwargs)

    def test_strict_json_rejects_duplicates_nonfinite_and_unsupported_values(self) -> None:
        for raw in ('{"x":1,"x":2}', '{"nested":{"x":1,"x":2}}',
                    '{"x":NaN}', '{"x":Infinity}', '{"x":1e999}', b'"\xff"'):
            with self.subTest(raw=raw), self.assertRaises(StoreError):
                strict_loads(raw)
        for value in ({1: "integer key"}, {"x": (1, 2)}, {"x": float("nan")}, {"x": b"bytes"}):
            with self.subTest(value=value), self.assertRaises(StoreError):
                canonical_bytes(value)
        self.assertEqual(canonical_bytes({"b": 2, "a": [1, True]}), b'{"a":[1,true],"b":2}')
        with self.assertRaises(StoreError):
            strict_loads('{"padding":"123"}', max_bytes=8)
        nested: list = []
        cursor = nested
        for _ in range(66):
            child: list = []
            cursor.append(child)
            cursor = child
        with self.assertRaises(StoreError):
            canonical_bytes(nested)

    def test_publish_replay_survives_reopen_and_checks_request_binding(self) -> None:
        store = self.store()
        first = store.publish("request-1", "observation", {"b": 2, "a": 1})
        reopened = self.store()
        self.assertEqual(first, reopened.publish("request-1", "observation", {"a": 1, "b": 2}))
        self.assertEqual(reopened.state()["next_sequence"], 2)
        self.assertEqual(reopened.artifact(first["artifact_sha256"]), {"a": 1, "b": 2})
        for kind, content in (("changed", {"a": 1, "b": 2}), ("observation", {"a": 9})):
            with self.assertRaisesRegex(StoreError, "conflicting command"):
                reopened.publish("request-1", kind, content)
        self.assertEqual(reopened.state()["count"], 1)
        self.assertEqual(reopened.publish("request-2", "observation", {"a": 1})["sequence"], 2)

    def test_artifact_depth_boundary_and_atomic_overflow_rejection(self) -> None:
        def nested(depth: int) -> dict:
            value: Any = "leaf"
            for _ in range(depth):
                value = {"child": value}
            return value
        store = self.store()
        accepted = nested(MAX_ARTIFACT_DEPTH)
        rejected = nested(MAX_ARTIFACT_DEPTH + 1)
        event = store.publish("at-boundary", "observation", accepted)
        self.assertEqual(store.artifact(event["artifact_sha256"]), accepted)
        receiver = self.store("receiver")
        self.assertEqual(receiver.merge(store.export([])), {"added": 1, "duplicates": 0})
        self.assertEqual(receiver.artifact(event["artifact_sha256"]), accepted)
        before = store.state()
        with self.assertRaisesRegex(StoreError, "nesting exceeds 48"):
            store.publish("too-deep", "observation", rejected)
        self.assertEqual(store.state(), before)
        with self.assertRaisesRegex(StoreError, "nesting exceeds 48"):
            store.merge(batch(fact(1), fact(2, content=rejected)))
        self.assertEqual(store.state(), before)
        self.assertEqual(store.publish("at-boundary", "observation", accepted), event)

    def test_retained_artifact_depth_limit_is_revalidated(self) -> None:
        content: Any = "leaf"
        for _ in range(MAX_ARTIFACT_DEPTH + 1):
            content = {"child": content}
        invalid_event, invalid_artifacts = fact(1, content=content)
        sha = invalid_event["artifact_sha256"]
        store = self.store()
        with sqlite3.connect(store.path) as db:
            db.execute("INSERT INTO artifacts VALUES (?, ?)",
                       (sha, canonical_bytes(invalid_artifacts[sha])))
            db.execute("INSERT INTO events VALUES (?, ?, ?, ?, ?)",
                       (invalid_event["event_id"], "origin", 1, sha, canonical_bytes(invalid_event)))
        for read in (store.state, lambda: store.artifact(sha), lambda: self.store()):
            with self.assertRaisesRegex(StoreError, "nesting exceeds 48"):
                read()

    def test_store_identity_and_wal_settings(self) -> None:
        store = self.store()
        with sqlite3.connect(store.path) as db:
            self.assertEqual(db.execute("PRAGMA journal_mode").fetchone()[0], "wal")
        with store._connection() as db:
            self.assertEqual(db.execute("PRAGMA synchronous").fetchone()[0], 2)
            self.assertEqual(db.execute("PRAGMA foreign_keys").fetchone()[0], 1)
        with self.assertRaisesRegex(StoreError, "identity"):
            Store(store.path, "other")
        self.assertEqual(self.store().state()["count"], 0)

    def test_merge_out_of_order_converges_with_deterministic_bounded_export(self) -> None:
        left, right = self.store("left"), self.store("right")
        facts = [fact(i, producer="alpha" if i % 2 else "beta") for i in range(1, 22)]
        order = list(facts)
        random.Random(31).shuffle(order)
        for offset in range(0, len(order), MAX_BATCH):
            left.merge(batch(*order[offset:offset + MAX_BATCH]))
        while right.inventory() != left.inventory():
            delta = left.export(right.inventory(), limit=3)
            self.assertLessEqual(len(delta["events"]), 3)
            right.merge(delta)
        self.assertEqual(left.state()["events"], right.state()["events"])
        self.assertEqual(left.state()["artifact_ids"], right.state()["artifact_ids"])
        self.assertEqual(left.inventory(), sorted(left.inventory()))
        self.assertEqual(left.export(left.inventory()), {"events": [], "artifacts": {}})
        for item in facts:
            self.assertEqual(right.merge(batch(item)), {"added": 0, "duplicates": 1})

    def test_merge_validates_entire_batch_before_inserting(self) -> None:
        store = self.store()
        good, missing = fact(1), fact(2)
        invalid = batch(good, missing)
        del invalid["artifacts"][missing[0]["artifact_sha256"]]
        with self.assertRaisesRegex(StoreError, "missing artifact"):
            store.merge(invalid)
        self.assertEqual(store.inventory(), [])
        corrupt = batch(good, missing)
        corrupt["artifacts"][missing[0]["artifact_sha256"]]["content"]["value"] = 90
        with self.assertRaisesRegex(StoreError, "artifact hash"):
            store.merge(corrupt)
        self.assertEqual(store.state()["artifact_ids"], [])
        self.assertEqual(store.merge(batch(good, missing))["added"], 2)

    def test_origin_equivocation_rejects_existing_and_in_batch_conflicts_atomically(self) -> None:
        store = self.store()
        original, conflicting, unrelated = fact(4), fact(4, content={"replacement": True}), fact(8)
        with self.assertRaisesRegex(StoreError, "equivocation"):
            store.merge(batch(original, conflicting))
        self.assertEqual(store.inventory(), [])
        store.merge(batch(original))
        before = store.state()
        with self.assertRaisesRegex(StoreError, "equivocation"):
            store.merge(batch(unrelated, conflicting))
        self.assertEqual(store.state(), before)

    def test_duplicate_batches_and_commands_never_fire_crash_hook(self) -> None:
        calls: list[tuple[str, str]] = []
        store = self.store(crash_hook=lambda point, operation: calls.append((point, operation)))
        local = store.publish("request", "observation", {"x": 1})
        self.assertEqual(calls, [("before_commit", "publish"), ("after_commit", "publish")])
        calls.clear()
        self.assertEqual(store.publish("request", "observation", {"x": 1}), local)
        self.assertEqual(store.merge(store.export([])), {"added": 0, "duplicates": 1})
        self.assertEqual(store.merge(batch()), {"added": 0, "duplicates": 0})
        self.assertEqual(calls, [])
        remote = fact(1)
        self.assertEqual(store.merge(batch(remote, remote)), {"added": 1, "duplicates": 1})
        self.assertEqual(calls, [("before_commit", "merge"), ("after_commit", "merge")])

    def test_hook_exception_before_commit_rolls_back_publish_and_merge(self) -> None:
        def hook(point: str, operation: str) -> None:
            if point == "before_commit":
                raise RuntimeError("injected before commit")
        store = self.store(crash_hook=hook)
        with self.assertRaises(RuntimeError):
            store.publish("request", "observation", {"x": 1})
        self.assertEqual(self.store().state()["next_sequence"], 1)
        with self.assertRaises(RuntimeError):
            store.merge(batch(fact(1)))
        self.assertEqual(self.store().state()["artifact_ids"], [])
        self.assertEqual(self.store().publish("request", "observation", {"x": 1})["sequence"], 1)

    def test_hook_exception_after_commit_preserves_request_and_merge(self) -> None:
        def hook(point: str, operation: str) -> None:
            if point == "after_commit":
                raise RuntimeError("injected lost acknowledgement")
        store = self.store(crash_hook=hook)
        with self.assertRaises(RuntimeError):
            store.publish("request", "observation", {"x": 1})
        self.assertEqual(store.publish("request", "observation", {"x": 1})["sequence"], 1)
        with self.assertRaises(RuntimeError):
            store.merge(batch(fact(1)))
        self.assertEqual(store.merge(batch(fact(1))), {"added": 0, "duplicates": 1})
        self.assertEqual(self.store().state()["count"], 2)

    def test_shared_artifact_and_local_origin_sequence_resume(self) -> None:
        store = self.store()
        one, two = fact(9, producer="node", content={"same": True}), fact(2, content={"same": True})
        store.merge(batch(one, two))
        self.assertEqual(len(store.state()["artifact_ids"]), 1)
        self.assertEqual(store.publish("next", "observation", {"same": True})["sequence"], 10)
        self.assertEqual(len(store.state()["artifact_ids"]), 1)

    def test_corrupt_stored_artifact_is_detected_on_read_merge_and_reopen(self) -> None:
        store = self.store()
        store.merge(batch(fact(1)))
        with sqlite3.connect(store.path) as db:
            db.execute("UPDATE artifacts SET payload=?", (b'{"protocol":"gossip-peer-v1","content":{}}',))
        for action in (store.state, lambda: store.merge(batch(fact(2))), lambda: self.store()):
            with self.assertRaisesRegex(StoreError, "artifact hash"):
                action()

    def test_missing_schema_never_recreates_lost_replay_bindings(self) -> None:
        store = self.store()
        store.publish("acknowledged", "observation", {"x": 1})
        with sqlite3.connect(store.path) as db:
            db.execute("DROP TABLE commands")
        with self.assertRaisesRegex(StoreError, "schema"):
            self.store()
        with sqlite3.connect(store.path) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM events").fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT count(*) FROM sqlite_master WHERE name='commands'").fetchone()[0], 0)

    def test_corrupt_event_metadata_or_request_binding_is_rejected(self) -> None:
        for corruption in ("event", "sequence", "command"):
            with self.subTest(corruption=corruption):
                store = self.store(corruption)
                store.publish("request", "observation", {"x": 1})
                with sqlite3.connect(store.path) as db:
                    if corruption == "event":
                        db.execute("UPDATE events SET producer='wrong'")
                    elif corruption == "sequence":
                        db.execute("UPDATE metadata SET value='1' WHERE key='next_sequence'")
                    else:
                        db.execute("UPDATE commands SET request_sha=?", ("0" * 64,))
                with self.assertRaises(StoreError):
                    store.export([])

    def test_limits_and_shape_rejection_leave_store_unchanged(self) -> None:
        store = self.store()
        oversized = {"padding": "x" * MAX_ARTIFACT_BYTES}
        with self.assertRaises(StoreError):
            store.publish("request", "observation", oversized)
        for invalid in (batch(*[fact(i) for i in range(1, MAX_BATCH + 2)]),
                        {"events": [], "artifacts": fact(1)[1]},
                        {"events": [], "artifacts": {}, "extra": True},
                        {"events": {}, "artifacts": {}}):
            with self.assertRaises(StoreError):
                store.merge(invalid)
        for known, limit in (([], 0), ([], MAX_BATCH + 1), ([], True), (["bad"], 1),
                             (["0" * 64] * (MAX_EVENTS + 1), 1)):
            with self.assertRaises(StoreError):
                store.export(known, limit)
        for field, value in (("producer", "bad node"), ("kind", "../file"),
                             ("sequence", True), ("sequence", 0), ("protocol", "future"),
                             ("event_id", "0" * 64)):
            invalid = batch(fact(1))
            invalid["events"][0][field] = value
            with self.subTest(field=field), self.assertRaises(StoreError):
                store.merge(invalid)
        self.assertEqual(store.state()["count"], 0)

    def test_event_capacity_is_atomic_and_still_allows_replay(self) -> None:
        store = self.store()
        acknowledged = store.publish("acknowledged", "observation", {"x": 1})
        for start in range(1, MAX_EVENTS, MAX_BATCH):
            store.merge(batch(*[fact(i) for i in range(start, min(start + MAX_BATCH, MAX_EVENTS))]))
        before = store.state()
        with self.assertRaisesRegex(StoreError, "capacity"):
            store.merge(batch(fact(500)))
        with self.assertRaisesRegex(StoreError, "capacity"):
            store.publish("new", "observation", {"x": 1})
        self.assertEqual(store.merge(batch(fact(1))), {"added": 0, "duplicates": 1})
        self.assertEqual(store.publish("acknowledged", "observation", {"x": 1}), acknowledged)
        self.assertEqual(store.state(), before)

    def test_concurrent_merge_publish_and_reads_share_consistent_snapshots(self) -> None:
        store = self.store()
        def publish() -> None:
            for index in range(12):
                store.publish(f"request-{index}", "observation", {"index": index})
        def merge_and_read() -> None:
            for index in range(1, 13):
                store.merge(batch(fact(index)))
                state = store.state()
                self.assertEqual(state["count"], len(state["events"]))
                self.assertEqual(set(state["artifact_ids"]),
                                 {event["artifact_sha256"] for event in state["events"]})
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(publish), pool.submit(merge_and_read)]
            for future in futures:
                future.result()
        self.assertEqual(store.state()["count"], 24)
        self.assertEqual(store.state()["next_sequence"], 13)

    def test_concurrent_publish_serializes_sequences_and_same_request(self) -> None:
        store = self.store()
        def publish(index: int) -> dict:
            return store.publish(f"request-{index}", "observation", {"index": index})
        with ThreadPoolExecutor(max_workers=2) as pool:
            events = list(pool.map(publish, [0, 0, *range(1, 12)]))
        self.assertEqual(events[0], events[1])
        self.assertEqual(sorted(event["sequence"] for event in store.state()["events"]), list(range(1, 13)))
        self.assertEqual(self.store().state()["next_sequence"], 13)


if __name__ == "__main__":
    unittest.main()
