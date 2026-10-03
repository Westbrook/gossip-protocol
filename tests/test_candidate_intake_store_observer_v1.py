"""B02 census bounds and preserved mapper controls; synthetic trusted SQL only."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from gossip_harness import candidate_intake_store_cases_v1 as cases
from gossip_harness import candidate_intake_store_observer_v1 as observer
from gossip_harness import candidate_storage_observer_v1 as frozen


def compound_capture() -> tuple[bytes, dict]:
    """Exact public aggregate byte limit using worst-case one-byte JSON escapes."""
    entries = [{"source": f"part-{index:02d}.txt", "text": "\x01" * 8192} for index in range(64)]
    expected = cases.stored_job("compound", entries, "completed")
    with tempfile.TemporaryDirectory() as temporary:
        path = Path(temporary) / "catalog.sqlite"
        connection = sqlite3.connect(path)
        try:
            connection.executescript("""
                CREATE TABLE blobs (blob_id TEXT PRIMARY KEY,content BLOB NOT NULL);
                CREATE TABLE documents (document_id TEXT PRIMARY KEY,source_id TEXT,source TEXT,blob_id TEXT,title TEXT);
                CREATE TABLE jobs (job_id TEXT PRIMARY KEY,epoch INTEGER,state TEXT,total INTEGER,
                    completed INTEGER,error TEXT,manifest TEXT,content_hashes TEXT,receipt TEXT);
            """)
            for doc in expected["receipt"]["documents"]:
                connection.execute("INSERT OR IGNORE INTO blobs VALUES (?,?)", (doc["blob_id"], doc["text"].encode()))
                connection.execute("INSERT INTO documents VALUES (?,?,?,?,?)",
                    tuple(doc[key] for key in ("document_id", "source_id", "source", "blob_id", "title")))
            values = tuple(expected["public"][key] for key in ("job_id", "epoch", "state", "total", "completed", "error"))
            connection.execute("INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?,?)", values + tuple(
                json.dumps(expected[key], ensure_ascii=True, sort_keys=True, separators=(",", ":"))
                for key in ("manifest", "content_hashes", "receipt")))
            connection.commit()
        finally:
            connection.close()
        return path.read_bytes(), expected


class CandidateIntakeStoreObserverTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.raw, cls.expected = compound_capture()

    def capture(self, raw=None):
        raw = self.raw if raw is None else raw
        files = {"catalog.sqlite": raw}
        registration = observer.Registration("a" * 64, observer.SQLITE_LAYOUT, ("catalog.sqlite",),
            "b" * 64, observer.sqlite_schema_sha256(raw))
        return observer.observe_capture(files, registration, "a" * 64)

    def test_compound_limit_control_characters_map_whole_sqlite_job_row(self):
        self.assertEqual(sum(len(row["text"].encode()) for row in self.expected["manifest"]), 524288)
        self.assertGreater(len(observer.encoded(self.expected["manifest"])) + len(observer.encoded(self.expected["receipt"])), 6 * 1024 * 1024)
        result = self.capture()
        self.assertEqual(result.data["jobs"], [self.expected])
        self.assertEqual(len(result.data["documents"]), 64)
        self.assertEqual(len(result.data["blobs"]), 1)
        self.assertGreater(len(observer.encoded(result.data)), 8 * 1024 * 1024)

    def test_new_limits_do_not_modify_frozen_observer_contract(self):
        self.assertEqual(frozen.MAX_FIELD_BYTES, 2 * 1024 * 1024)
        self.assertEqual(frozen.MAX_CAPTURE_BYTES, 8 * 1024 * 1024)
        self.assertEqual(observer.MAX_FIELD_BYTES, 8 * 1024 * 1024)
        self.assertEqual(observer.MAX_CAPTURE_BYTES, 32 * 1024 * 1024)
        self.assertEqual(observer.FORKED_FROM_SHA256, hashlib.sha256(Path(frozen.__file__).read_bytes()).hexdigest())
        registration = frozen.Registration("a" * 64, frozen.SQLITE_LAYOUT, ("catalog.sqlite",),
            "b" * 64, observer.sqlite_schema_sha256(self.raw))
        with self.assertRaises(frozen.ObservationUnavailable):
            frozen.observe_capture({"catalog.sqlite": self.raw}, registration, "a" * 64)

    def test_over_limit_row_is_unavailable(self):
        with patch.object(observer, "MAX_FIELD_BYTES", 3 * 1024 * 1024), self.assertRaises(observer.ObservationUnavailable):
            self.capture()

    def test_capture_byte_limit_is_still_enforced(self):
        with patch.object(observer, "MAX_CAPTURE_BYTES", len(self.raw) - 1), self.assertRaises(observer.ObservationUnavailable):
            self.capture()

    def test_serialized_census_limit_is_independent_of_raw_size(self):
        with patch.object(observer, "MAX_CAPTURE_BYTES", len(self.raw) + 4096), self.assertRaises(observer.ObservationUnavailable):
            self.capture()

    def test_unknown_schema_source_and_extra_path_remain_unavailable(self):
        registration = observer.Registration("a" * 64, observer.SQLITE_LAYOUT, ("catalog.sqlite",), "b" * 64, "c" * 64)
        for files, source in (({"catalog.sqlite": self.raw}, "a" * 64),
                              ({"catalog.sqlite": self.raw}, "c" * 64),
                              ({"catalog.sqlite": self.raw, "wal": b""}, "a" * 64)):
            with self.subTest(paths=list(files), source=source), self.assertRaises(observer.ObservationUnavailable):
                observer.observe_capture(files, registration, source)

    def test_duplicate_and_nonfinite_persisted_json_are_refused(self):
        for raw in ('{"a":1,"a":2}', '[NaN]', '[Infinity]'):
            with self.subTest(raw=raw), self.assertRaises(observer.ObservationUnavailable):
                observer._json(raw)
