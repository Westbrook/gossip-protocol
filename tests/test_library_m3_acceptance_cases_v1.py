"""Definition and fixture checks only; no M3 application/candidate is executed."""
from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from gossip_harness.library_m3_acceptance_cases_v1 import (
    CHILD_ADAPTER, CONTRACT_SHA256, REQUIREMENT_IDS, acceptance_cases, registry_manifest,
)


def _canonical(value, *, ascii=True):
    return json.dumps(value, sort_keys=True, ensure_ascii=ascii, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def _fixture_functions():
    module = ast.parse(CHILD_ADAPTER)
    selected = [node for node in module.body if isinstance(node, ast.FunctionDef)
                and node.name in {"encode", "identities", "seed_database", "strict_json", "confined"}]
    namespace = {"hashlib": hashlib, "sqlite3": sqlite3, "json": json, "Path": Path}
    exec(compile(ast.Module(body=selected, type_ignores=[]), "authored-m3-fixtures", "exec"), namespace)
    return namespace


class LibraryM3AcceptanceCasesTests(unittest.TestCase):
    def test_frozen_definitions_and_public_contract(self):
        contract = Path(__file__).resolve().parents[1] / "library-cumulative-product-v1.json"
        self.assertEqual(hashlib.sha256(contract.read_bytes()).hexdigest(), CONTRACT_SHA256)
        requirements = json.loads(contract.read_text())["requirements"]
        self.assertEqual(tuple(row["id"] for row in requirements if row["milestone"] == "M3"), REQUIREMENT_IDS)
        manifest = registry_manifest()
        self.assertEqual(manifest["ordered_inputs_sha256"], "8239ec21b90d0cdd9a8afd5de22d4801d2399f7f30d71bc78e81f2584535de40")
        self.assertEqual(manifest["ordered_expected_sha256"], "2d8751ede71f517b5ac36057b9c5823b4aedb9d67b4303883967efff31330259")
        self.assertEqual(manifest["ordered_cases_sha256"], "6b9bbf052e8f23f6c99248eb1579d363682657432f53f8ca51b7595fcf10e78e")
        self.assertEqual(manifest["adapter_sha256"], "e35f98cc47aba37219bed0ccbd543a5b648b6f65dd98d2a2425535d968b511df")
        self.assertEqual(manifest["status"], "unqualified_definition")
        self.assertEqual(manifest["purpose"], "independent_acceptance")

    def test_bounded_cases_have_separate_expectations_and_complete_mappings(self):
        cases = acceptance_cases()
        self.assertEqual(len(cases), 10)
        self.assertEqual(len({case["id"] for case in cases}), len(cases))
        self.assertEqual(set().union(*(set(c["requirement_ids"]) for c in cases)), set(REQUIREMENT_IDS))
        manifest = registry_manifest()
        for case in cases:
            with self.subTest(case=case["id"]):
                self.assertTrue(case["targeted_defects"])
                self.assertEqual(set(case["input"]), {"fixture", "files", "backups", "actions"})
                self.assertLessEqual(len(_canonical(case["input"])), 65536)
                self.assertLessEqual(len(_canonical(case["expected"])), 65536)
                self.assertNotIn("expected", case["input"])
                self.assertEqual(sum(a.get("observe", True) for a in case["input"]["actions"]),
                                 len(case["expected"]["observations"]))
                self.assertEqual(len(case["requirement_ids"]), len(set(case["requirement_ids"])))
        for requirement in REQUIREMENT_IDS:
            self.assertEqual(manifest["requirement_cases"][requirement],
                             [c["id"] for c in cases if requirement in c["requirement_ids"]])

    def test_fresh_definitions_no_host_candidate_imports_or_child_scoring(self):
        before = registry_manifest()
        cases = acceptance_cases(); cases[0]["input"]["actions"].clear()
        cases[0]["expected"]["observations"].clear()
        self.assertEqual(registry_manifest(), before)
        source = Path(__file__).resolve().parents[1] / "gossip_harness/library_m3_acceptance_cases_v1.py"
        for node in ast.walk(ast.parse(source.read_text())):
            if isinstance(node, ast.ImportFrom):
                self.assertNotIn("reference", node.module or "")
                self.assertFalse((node.module or "").startswith("library."))
        compile(CHILD_ADAPTER, "m3-observer", "exec")
        self.assertNotIn('"expected"', CHILD_ADAPTER)
        self.assertNotIn('"passed"', CHILD_ADAPTER)
        self.assertNotIn("shell=True", CHILD_ADAPTER)
        self.assertIn('"-I"', CHILD_ADAPTER)
        self.assertIn('timeout=8', CHILD_ADAPTER)

    def test_seed_fixtures_preserve_original_jobs_and_content(self):
        seed = _fixture_functions()["seed_database"]
        for case in acceptance_cases():
            with self.subTest(case=case["id"]), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "fixture.sqlite"
                fixture = case["input"]["fixture"]
                seed(path, fixture)
                with sqlite3.connect(path) as db:
                    self.assertEqual(db.execute("SELECT value FROM metadata WHERE key='schema'").fetchone(), ("0",))
                    self.assertEqual(db.execute("SELECT count(*) FROM documents").fetchone()[0], len(fixture["entries"]))
                    self.assertEqual(db.execute("SELECT count(*) FROM jobs").fetchone()[0], len(fixture["jobs"]))
                    for row in fixture["jobs"]:
                        value = db.execute("SELECT manifest,content_hashes,receipt FROM jobs WHERE job_id=?",
                                           (row["job"]["job_id"],)).fetchone()
                        self.assertEqual(value, tuple(row[key] for key in ("manifest_json", "content_hashes_json", "receipt_json")))
                    for blob, content in db.execute("SELECT blob_id,content FROM blobs"):
                        self.assertEqual(blob, "blob-" + hashlib.sha256(content).hexdigest())

    def test_semantic_corruptions_rehash_payload_and_valid_fixture_graph(self):
        case = next(c for c in acceptance_cases() if c["id"] == "m3-manual-backup-corrupt-graph-no-mutation")
        backups = {row["name"]: row["value"] for row in case["input"]["backups"]}
        for name, envelope in backups.items():
            with self.subTest(name=name):
                digest = hashlib.sha256(_canonical(envelope["payload"], ascii=False)).hexdigest()
                if name == "digest.json":
                    self.assertNotEqual(digest, envelope["payload_sha256"])
                else:
                    self.assertEqual(digest, envelope["payload_sha256"])
        payload = backups["valid.json"]["payload"]
        self.assertEqual(payload["schema"], 3)
        self.assertEqual(set(payload), {"schema", "generation", "documents", "revisions", "blobs", "collections", "jobs"})
        record = payload["documents"][0]; revision = payload["revisions"][0]; blob = payload["blobs"][0]
        self.assertEqual(record["document"]["document_id"], revision["document_id"])
        self.assertEqual(record["revision"], revision["revision"])
        self.assertEqual(record["document"]["blob_id"], revision["blob_id"])
        self.assertEqual(revision["blob_id"], blob["blob_id"])
        self.assertEqual(blob["blob_id"], "blob-" + hashlib.sha256(blob["text"].encode()).hexdigest())
        self.assertEqual(backups["duplicate.json"]["payload"]["revisions"][0], backups["duplicate.json"]["payload"]["revisions"][1])
        self.assertEqual(backups["gap.json"]["payload"]["revisions"][0]["revision"], 2)

    def test_backup_keeps_historical_receipt_and_deferred_admission(self):
        cases = {c["id"]: c for c in acceptance_cases()}
        envelope = cases["m3-backup-preserves-tombstone-history-receipt"]["expected"]["observations"][1]
        payload = envelope["payload"]
        self.assertEqual(hashlib.sha256(_canonical(payload, ascii=False)).hexdigest(), envelope["payload_sha256"])
        self.assertTrue(payload["documents"][0]["deleted"])
        self.assertEqual(payload["documents"][0]["document"]["text"], "new")
        receipt = json.loads(payload["jobs"][0]["receipt_json"])
        self.assertEqual(receipt["documents"][0]["text"], "old")
        self.assertIn(receipt["documents"][0]["blob_id"], {blob["blob_id"] for blob in payload["blobs"]})
        deferred = cases["m3-restore-deferred-invalid-manifest"]["input"]["backups"][0]["value"]["payload"]["jobs"][0]
        self.assertEqual(deferred["job"]["state"], "queued")
        self.assertEqual(json.loads(deferred["manifest_json"])[0]["source"], "../escape.txt")
        self.assertIsNone(deferred["receipt_json"])

    def test_unicode_export_boundary_uses_canonical_bytes(self):
        case = next(c for c in acceptance_cases() if c["id"] == "m3-export-selection-canonical-byte-bound")
        expected = case["expected"]["observations"]
        payload = expected[3]; raw = _canonical(payload, ascii=False)
        self.assertEqual(expected[4], {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()})
        self.assertNotEqual(len(raw), len(raw.decode()))
        actions = case["input"]["actions"]
        self.assertEqual(actions[5]["kwargs"]["max_bytes"], len(raw))
        self.assertEqual(actions[7]["kwargs"]["max_bytes"], len(raw) - 1)
        self.assertEqual(expected[0]["documents"], [])
        self.assertEqual(len(expected[1]["documents"]), 1)
        self.assertEqual([row["record"]["document"]["source"] for row in payload["documents"]], ["z.txt", "é.md"])

    def test_aba_expectations_keep_nonrewinding_tokens(self):
        case = next(c for c in acceptance_cases() if c["id"] == "m3-restore-removal-edit-and-job-aba")
        observations = case["expected"]["observations"]
        self.assertEqual(observations[3], {"restored": True, "generation": 5, "documents": 1, "jobs": 2})
        self.assertEqual(observations[5]["records"][0]["edit_version"], 4)
        self.assertEqual(observations[6], {"error": "stale_version"})
        self.assertEqual(observations[8]["epoch"], 2)
        self.assertEqual(observations[12]["epoch"], 4)
        self.assertEqual(observations[14], {"error": "stale_epoch"})
        self.assertEqual(observations[15]["edit_version"], 3)
        self.assertEqual(observations[16], {"error": "stale_version"})
        self.assertEqual(observations[-1]["generation"], 6)
        self.assertEqual(observations[10]["job"]["epoch"], 1)

    def test_fixture_json_and_path_guards_reject_unsafe_observation_inputs(self):
        helpers = _fixture_functions()
        for raw in (b'{"a":1,"a":2}', b'{"a":NaN}', b'{"a":1e999}', b'"\xff"'):
            with self.subTest(raw=raw), self.assertRaises((ValueError, UnicodeDecodeError)):
                helpers["strict_json"](raw)
        self.assertEqual(helpers["strict_json"]('"é"'.encode()), "é")
        for path in ("", "../escape", "/absolute", "a//b", "a/./b"):
            with self.subTest(path=path), self.assertRaises(ValueError):
                helpers["confined"](Path("/fixture"), path)


if __name__ == "__main__":
    unittest.main()
