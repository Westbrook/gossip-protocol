"""Definition/fixture checks; these do not execute or qualify an M2 candidate."""
from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from gossip_harness.library_m2_acceptance_cases_v1 import (
    CHILD_ADAPTER, CONTRACT_SHA256, REQUIREMENT_IDS, acceptance_cases, registry_manifest,
)


def _canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(",", ":"),
                      allow_nan=False).encode()


def _fixture_functions():
    # Run only the authored fixture constructor from the observer. No imports
    # from library.*, candidate source, reference implementation or oracle.
    module = ast.parse(CHILD_ADAPTER)
    selected = [node for node in module.body if isinstance(node, ast.FunctionDef)
                and node.name in {"identities", "seed_database", "expand"}]
    namespace = {"hashlib": hashlib, "sqlite3": sqlite3}
    exec(compile(ast.Module(body=selected, type_ignores=[]), "authored-fixtures", "exec"), namespace)
    return namespace


class LibraryM2AcceptanceCasesTests(unittest.TestCase):
    def test_frozen_definitions_and_normative_contract(self):
        contract = Path(__file__).resolve().parents[1] / "library-cumulative-product-v1.json"
        self.assertEqual(hashlib.sha256(contract.read_bytes()).hexdigest(), CONTRACT_SHA256)
        requirements = json.loads(contract.read_text())["requirements"]
        self.assertEqual(tuple(row["id"] for row in requirements if row["milestone"] == "M2"), REQUIREMENT_IDS)
        manifest = registry_manifest()
        self.assertEqual(manifest["ordered_inputs_sha256"],
                         "e7816435100538d9f4d23166f61d084c8c67617400ba80055582f15a7f2e7744")
        self.assertEqual(manifest["ordered_expected_sha256"],
                         "f7377ea25ed5c0c88fb1750ab8330ce6bf46cbe31bf37a9516c3132a3963fe80")
        self.assertEqual(manifest["ordered_cases_sha256"],
                         "91ea061bb949a4ce42fdd3e8a8d49e765b7348e602f251df0cb091c5ef20df9c")
        self.assertEqual(manifest["adapter_sha256"],
                         "0b1d19f959dcb36226c4bd4ac606378b0e62e3af920fb5fa4e21453ddd13ee86")
        self.assertEqual(manifest["status"], "unqualified_definition")
        self.assertTrue(manifest["limitations"])

    def test_exact_case_mappings_and_bounded_separate_payloads(self):
        cases = acceptance_cases()
        self.assertEqual(len(cases), 12)
        self.assertEqual(len({case["id"] for case in cases}), len(cases))
        self.assertEqual(set().union(*(set(c["requirement_ids"]) for c in cases)), set(REQUIREMENT_IDS))
        for case in cases:
            with self.subTest(case=case["id"]):
                self.assertTrue(case["requirement_ids"])
                self.assertTrue(case["targeted_defects"])
                self.assertEqual(len(case["requirement_ids"]), len(set(case["requirement_ids"])))
                self.assertEqual(set(case["input"]), {"fixture", "files", "actions"})
                self.assertLessEqual(len(_canonical(case["input"])), 65536)
                self.assertLessEqual(len(_canonical(case["expected"])), 65536)
                self.assertEqual(sum(a.get("observe", True) for a in case["input"]["actions"]),
                                 len(case["expected"]["observations"]))
                self.assertEqual(json.loads(_canonical(case["input"])), case["input"])
        self.assertLess(len(_canonical([c["input"] for c in cases])), 1048576)
        manifest = registry_manifest()
        for requirement in REQUIREMENT_IDS:
            self.assertEqual(manifest["requirement_cases"][requirement],
                             [c["id"] for c in cases if requirement in c["requirement_ids"]])

    def test_definitions_are_fresh_and_host_imports_no_application(self):
        prior = registry_manifest()
        cases = acceptance_cases()
        cases[0]["input"]["actions"].clear()
        cases[0]["expected"]["observations"].clear()
        self.assertEqual(registry_manifest(), prior)
        source = Path(__file__).resolve().parents[1] / "gossip_harness/library_m2_acceptance_cases_v1.py"
        parsed = ast.parse(source.read_text())
        for node in ast.walk(parsed):
            if isinstance(node, ast.ImportFrom):
                self.assertNotIn("reference", node.module or "")
                self.assertFalse((node.module or "").startswith("library."))
        compile(CHILD_ADAPTER, "m2-observer", "exec")
        self.assertNotIn('"expected"', CHILD_ADAPTER)
        self.assertNotIn('"passed"', CHILD_ADAPTER)

    def test_declared_legacy_snapshot_and_original_job_bytes(self):
        case = acceptance_cases()[0]
        fixture = case["input"]["fixture"]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.sqlite"
            _fixture_functions()["seed_database"](path, fixture)
            with sqlite3.connect(path) as db:
                self.assertEqual(db.execute("SELECT value FROM metadata WHERE key='schema'").fetchone()[0], "0")
                row = db.execute("SELECT manifest,content_hashes,receipt FROM jobs").fetchone()
                expected = fixture["jobs"][0]
                self.assertEqual(row, tuple(expected[k] for k in ("manifest", "content_hashes", "receipt")))
                receipt = json.loads(row[2]); doc = receipt["documents"][0]
                actual = db.execute("SELECT document_id,source_id,source,blob_id,title FROM documents").fetchone()
                self.assertEqual(actual, tuple(doc[k] for k in ("document_id", "source_id", "source", "blob_id", "title")))
                self.assertEqual(bytes(db.execute("SELECT content FROM blobs").fetchone()[0]).decode(), doc["text"])

    def test_blob_fixture_hits_exact_retained_distinct_byte_cap(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.sqlite"
            _fixture_functions()["seed_database"](path, {"kind": "blob_capacity"})
            with sqlite3.connect(path) as db:
                self.assertEqual(db.execute("SELECT count(*),sum(length(content)) FROM blobs").fetchone(),
                                 (512, 16777216))
                self.assertEqual(db.execute("SELECT count(*) FROM documents").fetchone()[0], 33)
                self.assertEqual(db.execute("SELECT count(*) FROM revisions").fetchone()[0], 512)
                self.assertEqual(db.execute("PRAGMA foreign_key_check").fetchall(), [])
                heads = db.execute("SELECT d.source,l.current_revision,l.edit_version,b.content FROM documents d "
                                   "JOIN lifecycle l USING(document_id) JOIN blobs b USING(blob_id) "
                                   "ORDER BY d.source").fetchall()
                self.assertEqual([row[1] for row in heads], [16] * 31 + [15, 1])
                self.assertEqual([row[2] for row in heads], [16] * 31 + [15, 1])
                refresh_count = sum(row[1] - 1 for row in heads)
                self.assertEqual(refresh_count, 31 * 15 + 14)
                self.assertEqual(refresh_count, 479)
                self.assertEqual(db.execute("SELECT value FROM metadata WHERE key='catalog_generation'").fetchone()[0],
                                 str(refresh_count))
                self.assertEqual(heads[-1][3][:7], b"032:01:")
                for blob, content in db.execute("SELECT blob_id,content FROM blobs"):
                    self.assertEqual(blob, "blob-" + hashlib.sha256(content).hexdigest())
        case = next(c for c in acceptance_cases() if c["id"] == "m2-retained-blob-capacity")
        self.assertTrue(all(a["digest"] for a in case["input"]["actions"]))
        self.assertEqual(case["expected"]["observations"][0],
                         {"sha256": hashlib.sha256(_canonical({"error": "capacity"})).hexdigest()})

    def test_document_fixture_keeps_distinct_identity_and_shared_blob(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.sqlite"
            _fixture_functions()["seed_database"](path, {"kind": "document_capacity"})
            with sqlite3.connect(path) as db:
                self.assertEqual(db.execute("SELECT count(*),count(DISTINCT document_id),count(DISTINCT source_id) "
                                            "FROM documents").fetchone(), (256, 256, 256))
                self.assertEqual(db.execute("SELECT count(*) FROM blobs").fetchone()[0], 1)

    def test_bounded_text_recipes_preserve_utf8_boundary(self):
        expand = _fixture_functions()["expand"]
        text = expand({"$repeat": ["é", 16385]})
        self.assertEqual(len(text.encode()), 32770)
        text = expand({"$repeat": ["é", 8193]})
        self.assertEqual(len(text.encode()), 16386)
        self.assertEqual(expand({"args": [True, {"$repeat": ["x", 2]}]}), {"args": [True, "xx"]})
        for count in (-1, True, 32770):
            with self.subTest(count=count), self.assertRaises(ValueError):
                expand({"$repeat": ["x", count]})

    def test_high_signal_histories_retain_exact_boundary_observations(self):
        cases = {c["id"]: c for c in acceptance_cases()}
        aba = cases["m2-migrate-aba-receipt"]["expected"]["observations"]
        self.assertEqual(aba[1]["document"]["blob_id"], aba[3]["record"]["document"]["blob_id"])
        self.assertEqual(aba[3]["record"]["edit_version"], 3)
        self.assertEqual(aba[4], {"error": "stale_version"})
        self.assertEqual(aba[8]["documents"][0]["text"], "first\n")
        race = cases["m2-competing-connections"]["expected"]["observations"][0]
        self.assertEqual(sum(value.get("error") == "stale_version" for value in race), 1)
        self.assertEqual(sum(value.get("status") == "updated" for value in race), 1)
        revisions = cases["m2-revision-boundary"]["expected"]["observations"]
        self.assertEqual(revisions[1], {"error": "revision_capacity"})
        self.assertEqual(len(revisions[3]["revisions"]), 16)
        self.assertEqual(revisions[2]["status"], "unchanged")


if __name__ == "__main__":
    unittest.main()
