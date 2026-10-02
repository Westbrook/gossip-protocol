"""Offline payload storage contracts; live transfer is covered by runtime tests."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import hashlib
from pathlib import Path
import sqlite3
import tempfile
import unittest
from typing import Any

from gossip_harness.peer_payload_store_v1 import (
    CHUNK_SIZE, MAX_CHUNKS, MAX_DESCRIPTOR_BYTES, MAX_OBJECT_BYTES, MAX_OBJECTS,
    MAX_RESERVED_BYTES, PROTOCOL, PayloadError, PayloadStore,
)
from gossip_harness.peer_store_v1 import canonical_bytes


def descriptor(data: bytes, media_type: str = "application/octet-stream") -> dict[str, Any]:
    return {"protocol": PROTOCOL, "sha256": hashlib.sha256(data).hexdigest(), "size": len(data),
            "chunk_size": CHUNK_SIZE, "chunk_sha256": [
                hashlib.sha256(data[offset:offset + CHUNK_SIZE]).hexdigest()
                for offset in range(0, len(data), CHUNK_SIZE)], "media_type": media_type}


def reservation(index: int, size: int) -> dict[str, Any]:
    """Unfetched descriptors exercise declared capacity without allocating bodies."""
    return {"protocol": PROTOCOL, "sha256": hashlib.sha256(f"object-{index}".encode()).hexdigest(),
            "size": size, "chunk_size": CHUNK_SIZE,
            "chunk_sha256": ["0" * 64] * ((size + CHUNK_SIZE - 1) // CHUNK_SIZE),
            "media_type": "application/octet-stream"}


class PeerPayloadStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.addCleanup(self.temp.cleanup)

    def store(self, name: str = "node") -> PayloadStore:
        return PayloadStore(self.root / name, name)

    def counts(self, store: PayloadStore) -> tuple[int, int, int]:
        with sqlite3.connect(store.path) as db:
            count, reserved = db.execute("SELECT count(*), coalesce(sum(size), 0) FROM objects").fetchone()
            chunks = db.execute("SELECT count(*) FROM chunks").fetchone()[0]
        return count, reserved, chunks

    def test_put_round_trip_and_reopen_preserve_exact_bytes_and_media(self) -> None:
        store = self.store()
        payloads = [(b'{"unicode":"\xe2\x98\x83"}', "application/json"),
                    (b"git bundle\x00\xff", "application/x-git-bundle"),
                    (bytes(range(256)) * 600, "application/octet-stream")]
        for data, media in payloads:
            with self.subTest(media=media):
                value = store.put(data, media)
                self.assertEqual(value, descriptor(data, media))
                reopened = self.store()
                self.assertEqual(reopened.descriptor(value["sha256"]), value)
                self.assertTrue(reopened.has_complete(value["sha256"]))
                self.assertEqual(reopened.missing(value["sha256"]), [])
                self.assertEqual(reopened.read(value["sha256"]), data)
                self.assertEqual(b"".join(reopened.read_chunk(value["sha256"], index)
                                          for index in range(len(value["chunk_sha256"]))), data)

    def test_partial_out_of_order_chunks_are_durable_and_relayable(self) -> None:
        data = b"a" * CHUNK_SIZE + b"b" * CHUNK_SIZE + b"tail"
        value = descriptor(data)
        sha = value["sha256"]
        receiver, relay = self.store("receiver"), self.store("relay")
        receiver.register(value)
        relay.register(value)
        self.assertEqual(receiver.missing(sha), [0, 1, 2])
        receiver.accept_chunk(sha, 2, b"tail")
        reopened = self.store("receiver")
        self.assertEqual(reopened.missing(sha), [0, 1])
        self.assertFalse(reopened.has_complete(sha))
        with self.assertRaisesRegex(PayloadError, "incomplete"):
            reopened.read(sha)
        with self.assertRaisesRegex(PayloadError, "unavailable"):
            reopened.read_chunk(sha, 0)
        relay.accept_chunk(sha, 2, reopened.read_chunk(sha, 2))
        self.assertEqual(relay.read_chunk(sha, 2), b"tail")
        for index in (1, 0):
            reopened.accept_chunk(sha, index, data[index * CHUNK_SIZE:(index + 1) * CHUNK_SIZE])
        self.assertTrue(reopened.has_complete(sha))
        self.assertEqual(reopened.read(sha), data)
        self.assertFalse(relay.has_complete(sha))

    def test_empty_object_requires_empty_digest_and_is_immediately_complete(self) -> None:
        store = self.store()
        value = descriptor(b"")
        store.register(value)
        self.assertEqual(store.put(b"", "application/octet-stream"), value)
        self.assertTrue(store.has_complete(value["sha256"]))
        self.assertEqual(store.missing(value["sha256"]), [])
        self.assertEqual(store.read(value["sha256"]), b"")
        self.assertEqual(self.counts(store), (1, 0, 0))
        with self.assertRaises(PayloadError):
            store.accept_chunk(value["sha256"], 0, b"")
        invalid = {**value, "sha256": "0" * 64}
        with self.assertRaisesRegex(PayloadError, "empty payload hash"):
            store.register(invalid)

    def test_duplicate_registration_put_and_chunks_never_charge_twice(self) -> None:
        store = self.store()
        data = b"x" * CHUNK_SIZE + b"tail"
        value = descriptor(data)
        sha = value["sha256"]
        store.register(value)
        self.assertEqual(self.counts(store), (1, len(data), 0))
        store.accept_chunk(sha, 1, b"tail")
        for _ in range(3):
            store.register(deepcopy(value))
            store.accept_chunk(sha, 1, b"tail")
        self.assertEqual(self.counts(store), (1, len(data), 1))
        self.assertEqual(store.put(data, value["media_type"]), value)
        self.assertEqual(store.put(data, value["media_type"]), value)
        self.assertEqual(self.counts(store), (1, len(data), 2))
        self.assertEqual(self.store().read(sha), data)

    def test_descriptor_identity_includes_media_type_and_chunk_manifest(self) -> None:
        store = self.store()
        value = store.put(b"immutable", "application/octet-stream")
        before = self.counts(store)
        changes = [("media_type", "application/json"), ("chunk_sha256", ["0" * 64]), ("size", 8)]
        for key, replacement in changes:
            changed = {**value, key: replacement}
            with self.subTest(key=key), self.assertRaisesRegex(PayloadError, "conflicting immutable"):
                store.register(changed)
        with self.assertRaisesRegex(PayloadError, "conflicting immutable"):
            store.put(b"immutable", "application/json")
        self.assertEqual(self.counts(store), before)
        self.assertEqual(store.read(value["sha256"]), b"immutable")

    def test_descriptor_inputs_and_outputs_are_detached(self) -> None:
        store = self.store()
        value = descriptor(b"value")
        expected = deepcopy(value)
        store.register(value)
        value["chunk_sha256"][0] = "0" * 64
        value["size"] = 500
        exposed = store.descriptor(expected["sha256"])
        exposed["chunk_sha256"].clear()
        self.assertEqual(store.descriptor(expected["sha256"]), expected)
        store.accept_chunk(expected["sha256"], 0, b"value")
        returned = store.put(b"value", "application/octet-stream")
        returned["chunk_sha256"].clear()
        self.assertEqual(store.descriptor(expected["sha256"]), expected)

    def test_descriptor_shape_types_digests_and_limits_fail_without_reservation(self) -> None:
        store = self.store()
        good = descriptor(b"value")
        invalid: list[Any] = [None, [], {**good, "extra": True}, {k: v for k, v in good.items() if k != "size"}]
        mutations = {
            "protocol": [False, "other", [], PROTOCOL + "\ud800"],
            "sha256": [True, "A" * 64, "0" * 63, "0" * 65, "../payload"],
            "size": [True, 5.0, -1, MAX_OBJECT_BYTES + 1, "5"],
            "chunk_size": [True, float(CHUNK_SIZE), CHUNK_SIZE - 1],
            "chunk_sha256": [(), [], ["G" * 64], [True], ["0" * 64] * (MAX_CHUNKS + 1)],
            "media_type": [None, [], "text/plain", "x" * MAX_DESCRIPTOR_BYTES],
        }
        for key, values in mutations.items():
            invalid.extend({**good, key: value} for value in values)
        for value in invalid:
            with self.subTest(value=repr(value)[:150]), self.assertRaises(PayloadError):
                store.register(value)
        self.assertEqual(self.counts(store), (0, 0, 0))

    def test_chunk_exact_indices_sizes_hashes_and_bytes_are_enforced_atomically(self) -> None:
        store = self.store()
        data = b"x" * CHUNK_SIZE + b"end"
        value = descriptor(data)
        sha = value["sha256"]
        store.register(value)
        invalid = [(True, data[:CHUNK_SIZE]), (0.0, data[:CHUNK_SIZE]), (-1, b"end"),
                   (MAX_CHUNKS, b"end"), (2, b"end"), (0, b"x" * (CHUNK_SIZE - 1)),
                   (0, b"x" * (CHUNK_SIZE + 1)), (1, b"end!"), (1, b"bad"),
                   (1, bytearray(b"end")), (1, memoryview(b"end"))]
        for index, chunk in invalid:
            with self.subTest(index=index, size=len(chunk)), self.assertRaises(PayloadError):
                store.accept_chunk(sha, index, chunk)
        self.assertEqual(store.missing(sha), [0, 1])
        self.assertEqual(self.counts(store), (1, len(data), 0))
        store.accept_chunk(sha, 1, b"end")
        with self.assertRaises(PayloadError):
            store.accept_chunk(sha, 1, b"bad")
        self.assertEqual(store.read_chunk(sha, 1), b"end")

    def test_final_hash_failure_rolls_back_only_incoming_chunk(self) -> None:
        store = self.store()
        data = b"x" * CHUNK_SIZE + b"end"
        value = {**descriptor(data), "sha256": "0" * 64}
        sha = value["sha256"]
        store.register(value)
        store.accept_chunk(sha, 0, data[:CHUNK_SIZE])
        with self.assertRaisesRegex(PayloadError, "complete payload hash"):
            store.accept_chunk(sha, 1, b"end")
        reopened = self.store()
        self.assertEqual(reopened.read_chunk(sha, 0), data[:CHUNK_SIZE])
        self.assertEqual(reopened.missing(sha), [1])
        self.assertFalse(reopened.has_complete(sha))
        self.assertEqual(self.counts(reopened), (1, len(data), 1))

    def test_unknown_objects_and_invalid_digests_fail_closed(self) -> None:
        store = self.store()
        sha = "0" * 64
        self.assertFalse(store.has_complete(sha))
        for action in (lambda: store.descriptor(sha), lambda: store.missing(sha),
                       lambda: store.read(sha), lambda: store.read_chunk(sha, 0),
                       lambda: store.accept_chunk(sha, 0, b"x")):
            with self.assertRaisesRegex(PayloadError, "unknown payload"):
                action()
        for invalid in ("../file", True, "A" * 64):
            for method in (store.descriptor, store.missing, store.read, store.has_complete):
                with self.assertRaises(PayloadError):
                    method(invalid)
        self.assertEqual(self.counts(store), (0, 0, 0))

    def test_put_enforces_maximum_size_and_exact_input_bytes(self) -> None:
        store = self.store()
        for invalid in (bytearray(b"value"), memoryview(b"value"), "value", None,
                        b"x" * (MAX_OBJECT_BYTES + 1)):
            with self.assertRaises(PayloadError):
                store.put(invalid, "application/octet-stream")
        with self.assertRaises(PayloadError):
            store.put(b"x", "text/plain")
        self.assertEqual(self.counts(store), (0, 0, 0))
        data = bytes(range(256)) * (MAX_OBJECT_BYTES // 256)
        value = store.put(data, "application/octet-stream")
        self.assertEqual(len(value["chunk_sha256"]), MAX_CHUNKS)
        self.assertEqual(self.counts(store), (1, MAX_OBJECT_BYTES, MAX_CHUNKS))
        self.assertEqual(self.store().read(value["sha256"]), data)

    def test_object_capacity_counts_unique_descriptors_and_allows_replay(self) -> None:
        store = self.store()
        values = [reservation(index, 1) for index in range(MAX_OBJECTS)]
        for value in values:
            store.register(value)
        self.assertEqual(self.counts(store), (MAX_OBJECTS, MAX_OBJECTS, 0))
        with self.assertRaisesRegex(PayloadError, "object capacity"):
            store.register(reservation(MAX_OBJECTS, 1))
        with self.assertRaisesRegex(PayloadError, "object capacity"):
            store.put(b"new", "application/octet-stream")
        reopened = self.store()
        reopened.register(values[0])
        self.assertEqual(self.counts(reopened), (MAX_OBJECTS, MAX_OBJECTS, 0))

    def test_reserved_byte_capacity_applies_before_chunks_and_survives_restart(self) -> None:
        store = self.store()
        values = [reservation(index, MAX_OBJECT_BYTES)
                  for index in range(MAX_RESERVED_BYTES // MAX_OBJECT_BYTES)]
        for value in values:
            store.register(value)
        before = (len(values), MAX_RESERVED_BYTES, 0)
        self.assertEqual(self.counts(store), before)
        with self.assertRaisesRegex(PayloadError, "reserved byte capacity"):
            store.register(reservation(900, 1))
        reopened = self.store()
        for value in values:
            reopened.register(value)
        with self.assertRaisesRegex(PayloadError, "reserved byte capacity"):
            reopened.put(b"new", "application/octet-stream")
        self.assertEqual(self.counts(reopened), before)
        reopened.register(descriptor(b""))
        self.assertEqual(self.counts(reopened), (len(values) + 1, MAX_RESERVED_BYTES, 0))

    def test_root_node_binding_and_connection_durability_settings(self) -> None:
        store = self.store()
        value = store.put(b"binding", "application/octet-stream")
        with store._connection() as db:
            self.assertEqual(db.execute("PRAGMA journal_mode").fetchone()[0], "wal")
            self.assertEqual(db.execute("PRAGMA synchronous").fetchone()[0], 2)
            self.assertEqual(db.execute("PRAGMA foreign_keys").fetchone()[0], 1)
        with self.assertRaisesRegex(PayloadError, "identity"):
            PayloadStore(store.root, "other")
        copied_root = self.root / "copied"
        copied_root.mkdir()
        with sqlite3.connect(store.path) as source, sqlite3.connect(copied_root / "payloads.sqlite") as target:
            source.backup(target)
        with self.assertRaisesRegex(PayloadError, "identity"):
            PayloadStore(copied_root, "node")
        self.assertEqual(self.store().read(value["sha256"]), b"binding")
        for invalid in ("../node", "node/name", "", True):
            with self.assertRaises(PayloadError):
                PayloadStore(self.root / "invalid", invalid)
        self.assertFalse((self.root / "invalid").exists())

    def test_database_and_sidecar_symlinks_are_rejected_without_writing_target(self) -> None:
        for suffix in ("", "-wal", "-shm", "-journal"):
            with self.subTest(suffix=suffix):
                root = self.root / (suffix or "main")
                root.mkdir()
                target = self.root / ((suffix or "main") + ".target")
                target.write_bytes(b"untouched")
                (root / ("payloads.sqlite" + suffix)).symlink_to(target)
                with self.assertRaisesRegex(PayloadError, "database path"):
                    PayloadStore(root, "node")
                self.assertEqual(target.read_bytes(), b"untouched")

    def test_resolved_root_alias_is_stable_and_changed_root_symlink_is_rejected(self) -> None:
        actual = self.root / "actual"
        actual.mkdir()
        alias = self.root / "alias"
        alias.symlink_to(actual, target_is_directory=True)
        store = PayloadStore(alias, "node")
        value = store.put(b"bound", "application/octet-stream")
        self.assertEqual(PayloadStore(actual, "node").read(value["sha256"]), b"bound")
        moved = self.root / "moved"
        actual.rename(moved)
        actual.symlink_to(moved, target_is_directory=True)
        with self.assertRaisesRegex(PayloadError, "root identity"):
            store.read(value["sha256"])
        with self.assertRaisesRegex(PayloadError, "identity"):
            PayloadStore(alias, "node")

    def test_missing_schema_is_not_recreated_on_reopen(self) -> None:
        store = self.store()
        store.put(b"durable", "application/octet-stream")
        with sqlite3.connect(store.path) as db:
            db.execute("DROP TABLE chunks")
        with self.assertRaisesRegex(PayloadError, "schema"):
            self.store()
        with sqlite3.connect(store.path) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM objects").fetchone()[0], 1)
            self.assertIsNone(db.execute("SELECT name FROM sqlite_master WHERE name='chunks'").fetchone())

    def test_empty_existing_database_is_not_silently_reinitialized(self) -> None:
        store = self.store()
        store.put(b"durable", "application/octet-stream")
        with sqlite3.connect(store.path) as db:
            for table in ("chunks", "objects", "metadata"):
                db.execute(f"DROP TABLE {table}")
        with self.assertRaisesRegex(PayloadError, "missing payload schema"):
            self.store()
        with sqlite3.connect(store.path) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM sqlite_master WHERE type='table'").fetchone()[0], 0)

    def test_duplicate_rows_in_foreign_schema_cannot_hide_in_catalog(self) -> None:
        for table in ("metadata", "objects", "chunks"):
            with self.subTest(table=table):
                store = self.store(table)
                value = store.put(b"value", "application/octet-stream")
                with sqlite3.connect(store.path) as db:
                    db.execute(f"CREATE TABLE replacement AS SELECT * FROM {table}")
                    db.execute(f"INSERT INTO replacement SELECT * FROM {table}")
                    db.execute(f"DROP TABLE {table}")
                    db.execute(f"ALTER TABLE replacement RENAME TO {table}")
                with self.assertRaises(PayloadError):
                    store.read(value["sha256"])
                with self.assertRaises(PayloadError):
                    PayloadStore(store.root, table)

    def test_corrupt_chunk_bytes_are_rejected_on_reads_writes_and_reopen(self) -> None:
        store = self.store()
        value = store.put(b"original", "application/octet-stream")
        sha = value["sha256"]
        with sqlite3.connect(store.path) as db:
            db.execute("UPDATE chunks SET data=?", (b"tampered",))
        for action in (lambda: store.descriptor(sha), lambda: store.read(sha), lambda: store.read_chunk(sha, 0),
                       lambda: store.missing(sha), lambda: store.has_complete(sha),
                       lambda: store.accept_chunk(sha, 0, b"original"),
                       lambda: store.put(b"original", "application/octet-stream"),
                       lambda: store.register(value), self.store):
            with self.assertRaisesRegex(PayloadError, "chunk hash"):
                action()

    def test_durable_final_hash_is_rechecked_independently_of_chunk_hashes(self) -> None:
        store = self.store()
        data = b"value"
        value = {**descriptor(data), "sha256": "0" * 64}
        store.register(value)
        with sqlite3.connect(store.path) as db:
            db.execute("INSERT INTO chunks VALUES (?, ?, ?)", (value["sha256"], 0, data))
        for action in (lambda: store.read(value["sha256"]), lambda: store.has_complete(value["sha256"]),
                       lambda: store.missing(value["sha256"]), self.store):
            with self.assertRaisesRegex(PayloadError, "complete payload hash"):
                action()

    def test_stored_descriptors_require_canonical_strict_json_and_matching_columns(self) -> None:
        for corruption in ("duplicate", "noncanonical", "oversized", "size", "sha", "invalid"):
            with self.subTest(corruption=corruption):
                store = self.store(corruption)
                value = store.put(b"value", "application/octet-stream")
                raw = canonical_bytes(value)
                with sqlite3.connect(store.path) as db:
                    if corruption == "duplicate":
                        db.execute("UPDATE objects SET descriptor=?", (b'{"size":5,' + raw[1:],))
                    elif corruption == "noncanonical":
                        db.execute("UPDATE objects SET descriptor=?", (raw + b" ",))
                    elif corruption == "oversized":
                        db.execute("UPDATE objects SET descriptor=?", (b" " * (MAX_DESCRIPTOR_BYTES + 1),))
                    elif corruption == "size":
                        db.execute("UPDATE objects SET size=6")
                    elif corruption == "sha":
                        db.execute("UPDATE objects SET descriptor=?", (canonical_bytes({**value, "sha256": "0" * 64}),))
                    else:
                        db.execute("UPDATE objects SET descriptor=?", (b"not json",))
                for action in (lambda: store.descriptor(value["sha256"]),
                               lambda: PayloadStore(store.root, corruption)):
                    with self.assertRaises(PayloadError):
                        action()

    def test_orphan_out_of_range_and_wrong_sized_stored_chunks_are_rejected(self) -> None:
        for corruption in ("orphan", "negative", "outside", "wrong_size", "wrong_type"):
            with self.subTest(corruption=corruption):
                store = self.store(corruption)
                value = descriptor(b"value")
                store.register(value)
                sha, index, data = value["sha256"], 0, b"value"
                if corruption == "orphan":
                    sha = "0" * 64
                elif corruption == "negative":
                    index = -1
                elif corruption == "outside":
                    index = 1
                elif corruption == "wrong_size":
                    data = b"longer"
                with sqlite3.connect(store.path) as db:
                    db.execute("INSERT INTO chunks VALUES (?, ?, ?)",
                               (sha, index, "value" if corruption == "wrong_type" else data))
                with self.assertRaises(PayloadError):
                    store.missing(value["sha256"])
                with self.assertRaises(PayloadError):
                    PayloadStore(store.root, corruption)

    def test_corrupt_durable_limits_are_rejected_before_materializing_payloads(self) -> None:
        for corruption in ("count", "reserved", "negative", "oversized_chunk"):
            with self.subTest(corruption=corruption):
                store = self.store(corruption)
                with sqlite3.connect(store.path) as db:
                    if corruption == "count":
                        values = [reservation(index, 1) for index in range(MAX_OBJECTS + 1)]
                    elif corruption == "reserved":
                        values = [reservation(index, MAX_OBJECT_BYTES)
                                  for index in range(MAX_RESERVED_BYTES // MAX_OBJECT_BYTES + 1)]
                    else:
                        values = [descriptor(b"value")]
                    for value in values:
                        db.execute("INSERT INTO objects VALUES (?, ?, ?)",
                                   (value["sha256"], value["size"], canonical_bytes(value)))
                    if corruption == "negative":
                        db.execute("UPDATE objects SET size=-1")
                    elif corruption == "oversized_chunk":
                        db.execute("INSERT INTO chunks VALUES (?, ?, ?)",
                                   (values[0]["sha256"], 0, b"x" * (CHUNK_SIZE + 1)))
                with self.assertRaises(PayloadError):
                    store.has_complete("0" * 64)
                with self.assertRaises(PayloadError):
                    PayloadStore(store.root, corruption)

    def test_concurrent_duplicate_chunks_and_puts_share_one_immutable_object(self) -> None:
        store = self.store()
        data = b"x" * CHUNK_SIZE + b"tail"
        value = descriptor(data)
        store.register(value)

        def write(index: int) -> None:
            if index % 2:
                store.put(data, "application/octet-stream")
            else:
                store.accept_chunk(value["sha256"], 1, b"tail")
            self.assertEqual(store.descriptor(value["sha256"]), value)

        with ThreadPoolExecutor(max_workers=2) as pool:
            for future in [pool.submit(write, index) for index in range(8)]:
                future.result()
        self.assertEqual(self.counts(store), (1, len(data), 2))
        self.assertEqual(self.store().read(value["sha256"]), data)

    def test_concurrent_admission_cannot_overbook_declared_capacity(self) -> None:
        store = self.store()
        for index in range(MAX_RESERVED_BYTES // MAX_OBJECT_BYTES - 1):
            store.register(reservation(index, MAX_OBJECT_BYTES))

        def register(index: int) -> bool:
            try:
                store.register(reservation(index, MAX_OBJECT_BYTES))
                return True
            except PayloadError as error:
                self.assertIn("capacity", str(error))
                return False

        with ThreadPoolExecutor(max_workers=2) as pool:
            accepted = list(pool.map(register, [500, 501]))
        self.assertEqual(sorted(accepted), [False, True])
        self.assertEqual(self.counts(store), (MAX_RESERVED_BYTES // MAX_OBJECT_BYTES, MAX_RESERVED_BYTES, 0))


if __name__ == "__main__":
    unittest.main()
