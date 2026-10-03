"""Physical persisted-byte mapper controls, not candidate product acceptance."""
from __future__ import annotations

import base64
import copy
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from gossip_harness.candidate_storage_cases_v1 import definitions, encoded
from gossip_harness.candidate_storage_observer_v1 import (
    JSON_LAYOUT, SQLITE_LAYOUT, V2_SQLITE_LAYOUT, V2_STORAGE_PATHS, MAX_ROWS, ObservationUnavailable, Registration,
    evaluate_case, observe_capture, split_json_schema_sha256, sqlite_schema_sha256,
)

SOURCE = "a" * 64
REVIEW = "b" * 64
SCHEMA = """
CREATE TABLE blobs(blob_id TEXT PRIMARY KEY,content BLOB NOT NULL);
CREATE TABLE documents(document_id TEXT PRIMARY KEY,source_id TEXT,source TEXT,blob_id TEXT,title TEXT);
CREATE TABLE jobs(job_id TEXT PRIMARY KEY,epoch INTEGER,state TEXT,total INTEGER,completed INTEGER,error TEXT,
                  manifest TEXT,content_hashes TEXT,receipt TEXT);
"""


def records(data: dict) -> tuple[list[dict], list[dict], list[dict]]:
    contents = {doc["blob_id"]: doc["text"].encode() for doc in data["documents"]}
    blobs = [{"blob_id": bid, "content": raw} for bid, raw in contents.items()]
    documents = [{key: value for key, value in doc.items() if key != "text"} for doc in data["documents"]]
    jobs = [row["public"] | {"manifest": json.dumps(row["manifest"], ensure_ascii=True),
                            "content_hashes": json.dumps(row["content_hashes"]),
                            "receipt": None if row["receipt"] is None else json.dumps(row["receipt"])}
            for row in data["jobs"]]
    return blobs, documents, jobs


def physical_files(layout: str, data: dict, *, edit=None, extra_sql: str = "") -> dict[str, bytes]:
    blobs, documents, jobs = records(data)
    if edit is not None:
        edit(blobs, documents, jobs)
    # Both controls write and reread actual durable bytes; the JSON layout has
    # distinct files and content encoding rather than masquerading as SQLite.
    with tempfile.TemporaryDirectory(prefix="storage-mapper-control-") as name:
        root = Path(name)
        if layout == SQLITE_LAYOUT:
            path = root / "catalog.sqlite"
            connection = sqlite3.connect(path)
            connection.executescript(SCHEMA + extra_sql)
            for row in blobs:
                connection.execute("INSERT INTO blobs VALUES (?,?)", tuple(row.values()))
            for row in documents:
                connection.execute("INSERT INTO documents VALUES (?,?,?,?,?)", tuple(row.values()))
            for row in jobs:
                connection.execute("INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?,?)", tuple(row.values()))
            connection.commit()
            connection.close()
            return {"catalog.sqlite": path.read_bytes()}
        raw_files = {"content.json": encoded([{"key": row["blob_id"], "payload_base64": base64.b64encode(row["content"]).decode()}
                                               for row in blobs]),
                     "catalog.json": encoded(documents), "jobs.json": encoded(jobs)}
        for path, raw in raw_files.items():
            (root / path).write_bytes(raw)
        return {path: (root / path).read_bytes() for path in raw_files}


def observe(files: dict[str, bytes], layout: str):
    # Authored schema controls only. Production registration must be authenticated
    # externally; inspecting a schema cannot authorize arbitrary candidate source.
    schema = sqlite_schema_sha256(files["catalog.sqlite"]) if layout == SQLITE_LAYOUT else split_json_schema_sha256()
    registration = Registration(SOURCE, layout, tuple(sorted(files)), REVIEW, schema)
    return observe_capture(files, registration, SOURCE)


class CandidateStorageObserverV1Tests(unittest.TestCase):
    def test_both_physical_layouts_normalize_all_eight_frozen_histories(self):
        for case in definitions():
            observations = {}
            for layout in (SQLITE_LAYOUT, JSON_LAYOUT):
                phases = [observe(physical_files(layout, case[phase]), layout) for phase in ("before", "after", "reopened")]
                self.assertTrue(evaluate_case(case["case_id"], *phases, case["result"])["all_local_assertions_passed"],
                                (layout, case["case_id"]))
                observations[layout] = [value.data for value in phases]
                self.assertTrue(all(value.files for value in phases))
            self.assertEqual(observations[SQLITE_LAYOUT], observations[JSON_LAYOUT])

    def test_orphan_blob_is_observed_despite_unchanged_public_documents(self):
        case = definitions()[0]
        for layout in (SQLITE_LAYOUT, JSON_LAYOUT):
            before = observe(physical_files(layout, case["before"]), layout)
            after = observe(physical_files(layout, case["after"], edit=lambda b, d, j: b.append(
                {"blob_id": "blob-" + hashlib.sha256(b"leak").hexdigest(), "content": b"leak"})), layout)
            result = evaluate_case("rollback", before, after, before, case["result"])
            self.assertEqual(before.data["documents"], after.data["documents"])
            self.assertFalse(result["checks"]["after.persisted-state"])
            self.assertFalse(result["all_local_assertions_passed"])

    def test_rolled_back_terminal_receipt_and_partial_document_are_detected(self):
        case = definitions()[0]
        for layout in (SQLITE_LAYOUT, JSON_LAYOUT):
            before = observe(physical_files(layout, case["before"]), layout)
            def leak(blobs, documents, jobs):
                jobs[0]["receipt"] = '{"documents":[],"job":{}}'
                documents.append(dict(documents[0], document_id="other", source_id="other", source="other.txt"))
            after = observe(physical_files(layout, case["after"], edit=leak), layout)
            result = evaluate_case("rollback", before, after, after, case["result"])
            self.assertFalse(result["checks"]["case.receipt-conservation"])
            self.assertFalse(result["checks"]["after.persisted-state"])

    def test_semantically_equal_manifest_rewrite_breaks_exact_immutability(self):
        case = next(row for row in definitions() if row["case_id"] == "canonical-replay")
        for layout in (SQLITE_LAYOUT, JSON_LAYOUT):
            before = observe(physical_files(layout, case["before"]), layout)
            def rewrite(blobs, documents, jobs):
                jobs[0]["manifest"] = json.dumps(json.loads(jobs[0]["manifest"]), separators=(",", ":"))
            after = observe(physical_files(layout, case["after"], edit=rewrite), layout)
            self.assertEqual(after.data, before.data)
            result = evaluate_case("canonical-replay", before, after, after, case["result"])
            self.assertFalse(result["checks"]["case.immutable-manifest"])

    def test_result_error_phase_is_not_interchangeable(self):
        for case in definitions()[:4]:
            phases = [observe(physical_files(JSON_LAYOUT, case[phase]), JSON_LAYOUT) for phase in ("before", "after", "reopened")]
            result = evaluate_case(case["case_id"], *phases, {"error": "invalid_request" if case["result"] != {"error": "invalid_request"} else "capacity"})
            self.assertFalse(result["checks"]["result"])
            self.assertEqual(result["failure_phase"], case["failure_phase"])
            self.assertIsNone(result["full_requirement_verdict"])

    def test_failed_job_cannot_replace_required_running_rollback_state(self):
        case = definitions()[0]
        before = observe(physical_files(JSON_LAYOUT, case["before"]), JSON_LAYOUT)
        changed = copy.deepcopy(case["after"])
        changed["jobs"][0]["public"].update(state="failed", error="injected_failure")
        after = observe(physical_files(JSON_LAYOUT, changed), JSON_LAYOUT)
        self.assertFalse(evaluate_case("rollback", before, after, after, case["result"])["all_local_assertions_passed"])

    def test_reopen_must_retain_exact_state_and_raw_receipts(self):
        case = next(row for row in definitions() if row["case_id"] == "completed-replay")
        before = observe(physical_files(JSON_LAYOUT, case["before"]), JSON_LAYOUT)
        def rewrite(blobs, documents, jobs):
            jobs[0]["receipt"] = json.dumps(json.loads(jobs[0]["receipt"]), indent=1)
        reopened = observe(physical_files(JSON_LAYOUT, case["reopened"], edit=rewrite), JSON_LAYOUT)
        result = evaluate_case(case["case_id"], before, before, reopened, case["result"])
        self.assertFalse(result["checks"]["reopened.persisted-strings"])
        self.assertEqual(before.data, reopened.data)

    def test_source_schema_review_and_layout_are_required_not_self_qualifying(self):
        files = physical_files(SQLITE_LAYOUT, definitions()[0]["before"])
        schema = sqlite_schema_sha256(files["catalog.sqlite"])
        for reg, source in [
            (Registration(SOURCE, "unknown", ("catalog.sqlite",), REVIEW, schema), SOURCE),
            (Registration(SOURCE, SQLITE_LAYOUT, ("catalog.sqlite",), REVIEW, schema), "c" * 64),
            (Registration(SOURCE, SQLITE_LAYOUT, ("catalog.sqlite",), "", schema), SOURCE),
            (Registration(SOURCE, SQLITE_LAYOUT, ("catalog.sqlite",), REVIEW, "c" * 64), SOURCE),
        ]:
            with self.assertRaises(ObservationUnavailable):
                observe_capture(files, reg, source)

    def test_unqualified_sidecars_and_missing_files_cannot_look_clean(self):
        files = physical_files(SQLITE_LAYOUT, definitions()[0]["before"])
        schema = sqlite_schema_sha256(files["catalog.sqlite"])
        reg = Registration(SOURCE, SQLITE_LAYOUT, ("catalog.sqlite",), REVIEW, schema)
        for extra in ("catalog.sqlite-wal", "catalog.sqlite-journal", "hidden-blobs.bin"):
            with self.assertRaises(ObservationUnavailable):
                observe_capture(files | {extra: b""}, reg, SOURCE)
        with self.assertRaises(ObservationUnavailable):
            observe_capture({}, reg, SOURCE)

    def test_views_and_virtual_tables_are_not_evaluated(self):
        for extra in (
            "CREATE VIEW suspicious AS SELECT load_extension('bad');",
            "CREATE VIRTUAL TABLE suspicious USING fts5(content);",
        ):
            files = physical_files(SQLITE_LAYOUT, definitions()[0]["before"], extra_sql=extra)
            with self.assertRaises(ObservationUnavailable):
                sqlite_schema_sha256(files["catalog.sqlite"])

    def test_registered_write_trigger_is_retained_but_never_executed(self):
        case = definitions()[0]
        plain = physical_files(SQLITE_LAYOUT, case["before"])
        # This trigger would abort a write if invoked; the immutable observer
        # only performs fixed SELECTs, and its schema is a distinct authority.
        files = physical_files(SQLITE_LAYOUT, case["before"], extra_sql=
            "CREATE TRIGGER suspicious AFTER UPDATE ON jobs BEGIN SELECT RAISE(ABORT,'must not run'); END;")
        self.assertNotEqual(sqlite_schema_sha256(plain["catalog.sqlite"]), sqlite_schema_sha256(files["catalog.sqlite"]))
        self.assertEqual(observe(files, SQLITE_LAYOUT).data, case["before"])
        reg = Registration(SOURCE, SQLITE_LAYOUT, ("catalog.sqlite",), REVIEW,
                           sqlite_schema_sha256(plain["catalog.sqlite"]))
        with self.assertRaises(ObservationUnavailable):
            observe_capture(files, reg, SOURCE)

    def test_auxiliary_v2_catalog_corruption_cannot_hide_behind_public_projection(self):
        case = definitions()[0]
        def capture(value):
            return observe(physical_files(SQLITE_LAYOUT, case["before"], extra_sql=
                "CREATE TABLE later_control (key TEXT,value TEXT); INSERT INTO later_control VALUES ('generation','" + value + "');"), SQLITE_LAYOUT)
        before, after = capture("4"), capture("5")
        self.assertEqual(before.data, after.data)
        self.assertNotEqual(before.auxiliary_tables, after.auxiliary_tables)
        result = evaluate_case("rollback", before, after, after, case["result"])
        self.assertFalse(result["checks"]["after.auxiliary-conservation"])

    def test_row_bound_rejects_incomplete_census(self):
        def many(blobs, documents, jobs):
            blobs.extend({"blob_id": "extra-" + str(i), "content": b"x"} for i in range(MAX_ROWS))
        files = physical_files(SQLITE_LAYOUT, definitions()[0]["before"], edit=many)
        with self.assertRaises(ObservationUnavailable):
            sqlite_schema_sha256(files["catalog.sqlite"])

    def test_duplicate_json_keys_and_nonfinite_values_are_not_normalized_away(self):
        for raw in (b'[{"key":"a","key":"b","payload_base64":""}]',
                    b'[{"key":NaN,"payload_base64":""}]'):
            files = {"content.json": raw, "catalog.json": b"[]", "jobs.json": b"[]"}
            with self.assertRaises(ObservationUnavailable):
                observe(files, JSON_LAYOUT)

    def test_boolean_epoch_is_not_equal_to_integer_epoch(self):
        case = definitions()[0]
        before = observe(physical_files(JSON_LAYOUT, case["before"]), JSON_LAYOUT)
        changed = copy.deepcopy(case["after"])
        changed["jobs"][0]["public"]["epoch"] = True
        after = observe(physical_files(JSON_LAYOUT, changed), JSON_LAYOUT)
        self.assertFalse(evaluate_case("rollback", before, after, after, case["result"])["all_local_assertions_passed"])

    def test_mutated_public_cache_fails_even_when_storage_is_intact(self):
        case = next(row for row in definitions() if row["case_id"] == "value-mutation")
        before = observe(physical_files(JSON_LAYOUT, case["before"]), JSON_LAYOUT)
        result = copy.deepcopy(case["result"])
        result["job"]["state"] = "corrupted"
        checks = evaluate_case("value-mutation", before, before, before, result)
        self.assertFalse(checks["checks"]["result"])
        self.assertFalse(checks["all_local_assertions_passed"])

    def test_v2_profile_records_two_reviewed_locks_and_rejects_any_other_file(self):
        case = definitions()[0]
        files = physical_files(SQLITE_LAYOUT, case["before"]) | {path: b"" for path in V2_STORAGE_PATHS[1:]}
        reg = Registration(SOURCE, V2_SQLITE_LAYOUT, V2_STORAGE_PATHS, REVIEW,
                           sqlite_schema_sha256(files["catalog.sqlite"]))
        observed = observe_capture(files, reg, SOURCE)
        self.assertEqual({row["path"] for row in observed.files}, set(V2_STORAGE_PATHS))
        self.assertTrue(evaluate_case("rollback", observed, observed, observed, case["result"])["all_local_assertions_passed"])
        for changed in (files | {"catalog.sqlite-journal": b""},
                        files | {V2_STORAGE_PATHS[1]: b"unreviewed"},
                        {key: value for key, value in files.items() if key != V2_STORAGE_PATHS[1]}):
            with self.assertRaises(ObservationUnavailable):
                observe_capture(changed, reg, SOURCE)

    def test_unknown_case_and_mixed_registration_refuse_assertion(self):
        case = definitions()[0]
        first = observe(physical_files(SQLITE_LAYOUT, case["before"]), SQLITE_LAYOUT)
        second = observe(physical_files(JSON_LAYOUT, case["before"]), JSON_LAYOUT)
        with self.assertRaises(ObservationUnavailable):
            evaluate_case("unknown", first, first, first, {})
        with self.assertRaises(ObservationUnavailable):
            evaluate_case("rollback", first, second, first, case["result"])
