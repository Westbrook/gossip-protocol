"""Definition/fixture checks only: no M4 application or candidate is executed."""
from __future__ import annotations

import ast
import base64
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sqlite3
import unittest
import zlib

from gossip_harness.library_m4_acceptance_cases_v1 import (
    CHILD_ADAPTER, CONTRACT_SHA256, REQUIREMENT_IDS, acceptance_cases, registry_manifest,
)
from gossip_harness.library_m4_fixture_v1 import fixture_files, fixture_manifest, snapshot_inventory


def _canonical(value, *, ascii=True):
    return json.dumps(value, sort_keys=True, ensure_ascii=ascii, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def _observer_helpers():
    parsed = ast.parse(CHILD_ADAPTER)
    nodes = [node for node in parsed.body if isinstance(node, ast.FunctionDef) and
             node.name in {"fixture_bytes", "strict_json", "confined"}]
    namespace = {"Path": Path, "base64": base64, "hashlib": hashlib, "json": json, "zlib": zlib}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "m4-fixture-observation", "exec"), namespace)
    return namespace


class LibraryM4AcceptanceCasesTests(unittest.TestCase):
    def test_frozen_contract_and_definition_hashes(self):
        root = Path(__file__).resolve().parents[1]
        raw = (root / "library-cumulative-product-v1.json").read_bytes()
        self.assertEqual(hashlib.sha256(raw).hexdigest(), CONTRACT_SHA256)
        requirements = [row["id"] for row in json.loads(raw)["requirements"] if row["milestone"] == "M4"]
        self.assertEqual(requirements, [*REQUIREMENT_IDS, "M4-RELEASE-HANDOFF"])
        manifest = registry_manifest()
        self.assertEqual(manifest["ordered_inputs_sha256"], "80078592345e01e615cdf9d1fca600d79aab4cdb827fce11820274f29654c61e")
        self.assertEqual(manifest["ordered_expected_sha256"], "780e2bbb80849fa751ded1dcc5a64bddf0e2ff95dde564a114a761c5df8bee98")
        self.assertEqual(manifest["ordered_cases_sha256"], "bc967ec65cce876366ebb66c15caea1d42bd2400ea06aabed6823f4f0823d11f")
        self.assertEqual(manifest["adapter_sha256"], "268e996467005f2b4dbbfcb75cecb94a59d3cd0014d7948fa5fd78b9902f4003")
        self.assertEqual(manifest["public_fixture_files_sha256"], fixture_manifest()["files_sha256"])
        self.assertEqual(manifest["status"], "unqualified_definition")
        self.assertEqual(manifest["uncovered_requirements"], ["M4-RELEASE-HANDOFF"])

    def test_bounded_cases_and_separate_host_expectations(self):
        cases = acceptance_cases()
        self.assertEqual(len(cases), 18)
        self.assertEqual(len(set(case["id"] for case in cases)), 18)
        self.assertEqual(set().union(*(set(case["requirement_ids"]) for case in cases)), set(REQUIREMENT_IDS))
        for case in cases:
            with self.subTest(case=case["id"]):
                self.assertTrue(case["targeted_defects"])
                self.assertLessEqual(len(_canonical(case["input"])), 65536)
                self.assertLessEqual(len(_canonical(case["expected"])), 65536)
                self.assertEqual(set(case["input"]), {"fixture", "files", "actions"})
                self.assertNotIn("expected", case["input"])
                self.assertEqual(sum(action.get("observe", True) for action in case["input"]["actions"]),
                                 len(case["expected"]["observations"]))
        manifest = registry_manifest()
        for requirement in REQUIREMENT_IDS:
            self.assertEqual(manifest["requirement_cases"][requirement],
                             [case["id"] for case in cases if requirement in case["requirement_ids"]])

    def test_fresh_definitions_and_no_host_candidate_imports(self):
        before = registry_manifest()
        cases = acceptance_cases()
        cases[0]["input"]["actions"].clear()
        cases[0]["expected"]["observations"].clear()
        self.assertEqual(registry_manifest(), before)
        root = Path(__file__).resolve().parents[1]
        for path in (root / "gossip_harness/library_m4_acceptance_cases_v1.py", Path(__file__)):
            for node in ast.walk(ast.parse(path.read_text())):
                if isinstance(node, ast.ImportFrom):
                    self.assertNotIn("reference", node.module or "")
                    self.assertFalse((node.module or "").startswith("library."))
                if isinstance(node, ast.Import):
                    self.assertFalse(any(name.name.startswith("library.") for name in node.names))
        compile(CHILD_ADAPTER, "independent-m4-observer", "exec")
        self.assertNotIn('"expected"', CHILD_ADAPTER)
        self.assertNotIn('"passed"', CHILD_ADAPTER)
        self.assertNotIn("shell=True", CHILD_ADAPTER)
        self.assertIn('"-I"', CHILD_ADAPTER)
        self.assertIn("timeout=8", CHILD_ADAPTER)
        self.assertIn("mode=ro", CHILD_ADAPTER)

    def test_frozen_input_bytes_and_public_inventories_remain_unchanged(self):
        files = fixture_files()
        decode = _observer_helpers()["fixture_bytes"]
        for case in acceptance_cases():
            fixture = case["input"]["fixture"]
            if fixture is None:
                continue
            raw = decode(fixture)
            self.assertEqual(hashlib.sha256(raw).hexdigest(), fixture["sha256"])
            self.assertEqual(len(raw), fixture["bytes"])
        by_id = {case["id"]: case for case in acceptance_cases()}
        for name in ("v0", "m2"):
            case = by_id["m4-frozen-" + name + "-migration-preserves-inventory"]
            self.assertEqual(decode(case["input"]["fixture"]), files["compatibility/" + name + ".sqlite3"])
            inventory = snapshot_inventory(name)
            observations = case["expected"]["observations"]
            self.assertEqual(observations[1]["value"], inventory["migration_response"])
            self.assertEqual(observations[5]["records"], inventory["records2"])
            self.assertEqual(observations[6]["records"], inventory["records4"])
        self.assertEqual(fixture_files(), files)

    def test_corruption_refusal_observes_original_file_bytes_after_each_attempt(self):
        decode = _observer_helpers()["fixture_bytes"]
        cases = [case for case in acceptance_cases() if case["id"].startswith("m4-reject-")]
        self.assertEqual(len(cases), 9)
        for case in cases:
            with self.subTest(case=case["id"]):
                raw = decode(case["input"]["fixture"])
                expected = case["expected"]["observations"]
                digest = {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
                self.assertEqual([expected[index] for index in (0, 2, 4)], [digest] * 3)
                self.assertEqual(expected[1]["value"], expected[3])
                self.assertEqual(expected[1]["exit"], 2)
                self.assertEqual([action["op"] for action in case["input"]["actions"]],
                                 ["file_digest", "cli", "file_digest", "open", "file_digest"])
        unknown = next(case for case in cases if "unknown-schema" in case["id"])
        with sqlite3.connect(":memory:") as connection:
            connection.deserialize(decode(unknown["input"]["fixture"]))
            self.assertEqual(connection.execute("SELECT value FROM metadata WHERE key='schema'").fetchone(), ("99",))

    def test_authored_schema0_jobs_keep_original_serialization(self):
        case = next(case for case in acceptance_cases() if case["id"] == "m4-authored-m1-schema0-receipt-and-queued-job")
        raw = _observer_helpers()["fixture_bytes"](case["input"]["fixture"])
        connection = sqlite3.connect(":memory:")
        try:
            connection.deserialize(raw)
            self.assertEqual(connection.execute("SELECT value FROM metadata WHERE key='schema'").fetchone(), ("0",))
            self.assertEqual(connection.execute("SELECT count(*) FROM documents").fetchone()[0], 1)
            rows = connection.execute("SELECT job_id,state,manifest,content_hashes,receipt FROM jobs ORDER BY job_id").fetchall()
            self.assertEqual([(row[0], row[1]) for row in rows], [("complete", "completed"), ("pending", "queued")])
            serialization = case["expected"]["observations"][5]
            self.assertEqual(rows[0][2:], tuple(serialization[key] for key in ("manifest_json", "content_hashes_json", "receipt_json")))
            receipt = json.loads(rows[0][4])
            self.assertEqual(set(receipt["documents"][0]), {"document_id", "source_id", "source", "title", "blob_id", "text"})
            self.assertIsNone(rows[1][4])
        finally:
            connection.close()

    def test_revision_identity_is_document_number_and_blob_bound(self):
        for name in ("v0", "m2"):
            inventory = snapshot_inventory(name)
            for docid, revisions in inventory["histories4"].items():
                for revision in revisions:
                    identity = b"revision\0" + docid.encode("ascii") + b"\0" + str(revision["revision"]).encode("ascii") + b"\0" + revision["blob_id"].encode("ascii")
                    self.assertEqual(revision["revision_id"], "rev-" + hashlib.sha256(identity).hexdigest())
        case = next(case for case in acceptance_cases() if case["id"] == "m4-restore-reused-number-new-content-identity")
        observations = case["expected"]["observations"]
        before, after = observations[2]["current_revision"], observations[8]["current_revision"]
        self.assertEqual(before["revision"], after["revision"])
        self.assertNotEqual(before["revision_id"], after["revision_id"])
        self.assertEqual(observations[6], {"error": "stale_version"})
        self.assertEqual(observations[10], {"error": "not_found"})
        self.assertEqual(observations[8]["edit_version"], 4)

    def test_receipt_backup_projection_has_exact_original_strings_and_digest(self):
        case = next(case for case in acceptance_cases() if case["id"] == "m4-frozen-m2-receipt-bytes-through-backup-restore")
        envelope = next(value for value in case["expected"]["observations"] if
                        isinstance(value, dict) and value.get("format") == "local-research-library-backup-v3")
        payload = envelope["payload"]
        self.assertEqual(payload["schema"], 3)
        self.assertEqual(hashlib.sha256(_canonical(payload, ascii=False)).hexdigest(), envelope["payload_sha256"])
        original = snapshot_inventory("m2")
        self.assertEqual(payload["jobs"], [dict(row, enrolled=False) for row in original["jobs"]])
        tombstone = next(record for record in payload["documents"] if record["deleted"])
        receipt = json.loads(next(row["receipt_json"] for row in payload["jobs"] if row["job"]["job_id"] == "batch-done"))
        historical = next(doc for doc in receipt["documents"] if doc["document_id"] == tombstone["document"]["document_id"])
        self.assertNotEqual(historical["text"], tombstone["document"]["text"])

    def test_unicode_v4_export_exact_byte_boundary_and_legacy_shape(self):
        case = next(case for case in acceptance_cases() if case["id"] == "m4-opt-in-export-and-strict-v1-routes")
        observations = case["expected"]["observations"]
        value = observations[1]
        raw = _canonical(value, ascii=False)
        self.assertEqual(observations[2], {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()})
        self.assertNotEqual(len(raw), len(raw.decode()))
        self.assertEqual(observations[3], {"error": "too_large"})
        self.assertEqual(observations[0]["documents"], [])
        self.assertEqual(observations[4]["format"], "local-research-library-export-v2")
        self.assertEqual(observations[5]["format"], "local-research-library-v0")
        call = next(action for action in case["input"]["actions"] if action.get("canonical"))
        self.assertEqual(call["kwargs"]["max_bytes"], len(raw))

    def test_observer_fixture_codec_and_json_guards(self):
        helpers = _observer_helpers()
        original = next(case["input"]["fixture"] for case in acceptance_cases() if case["input"]["fixture"])
        for changes in ({"bytes": True}, {"bytes": 1048577}, {"data": "!"}, {"sha256": "0" * 64},
                        {"data": original["data"] + "AA=="}, {"encoding": "raw"}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                helpers["fixture_bytes"](dict(original, **changes))
        too_big = dict(original, bytes=1048576, data=base64.b64encode(zlib.compress(b"x" * 1048577)).decode())
        with self.assertRaises(ValueError):
            helpers["fixture_bytes"](too_big)
        for raw in (b'{"a":1,"a":2}', b'{"a":NaN}', b'{"a":1e999}', b'"\xff"'):
            with self.subTest(raw=raw), self.assertRaises((ValueError, UnicodeDecodeError)):
                helpers["strict_json"](raw)
        for value in ("", "../escape", "/absolute", "a//b", "a/./b"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                helpers["confined"](Path("/fixture"), value)
        changed = deepcopy(original)
        changed["extra"] = True
        with self.assertRaises(ValueError):
            helpers["fixture_bytes"](changed)


if __name__ == "__main__":
    unittest.main()
