"""Definition-only checks: no M4 candidate or authored reference is executed."""
from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path
import unittest

from gossip_harness.library_cumulative_public_fixture_v1 import (
    CONTRACT_SHA256, PUBLIC_PATHS, PUBLIC_TEST_SOURCE, WORKFLOW_FORMAT,
    public_files, public_manifest, workflow_contract,
)


class LibraryCumulativePublicFixtureTests(unittest.TestCase):
    def test_generated_adapter_is_real_persistent_application_driver(self):
        compile(PUBLIC_TEST_SOURCE, "test_cumulative_public.py", "exec")
        source = Path(__file__).resolve().parents[1] / "gossip_harness/library_cumulative_public_fixture_v1.py"
        imports = [node.module or "" for node in ast.walk(ast.parse(source.read_text()))
                   if isinstance(node, ast.ImportFrom)]
        self.assertFalse(any("reference" in module or "acceptance_cases" in module for module in imports))
        for fragment in ("from library.catalog.store import Store", "from library.ingestion.jobs import JobManager",
                         "from library.ingestion.local import import_file", "service.refresh_document(",
                         "service.replace_annotations(", 'shutil.copyfile(original, database)',
                         '"-I"', "timeout=15", "original.read_bytes(), before"):
            self.assertIn(fragment, PUBLIC_TEST_SOURCE)
        self.assertNotIn("shell=True", PUBLIC_TEST_SOURCE)
        self.assertNotIn("solution.solve", PUBLIC_TEST_SOURCE)

    def test_workflow_contract_is_versioned_closed_and_uses_explicit_tokens(self):
        grammar = workflow_contract()
        self.assertEqual(grammar["format"], WORKFLOW_FORMAT)
        self.assertEqual(grammar["max_operations"], 64)
        self.assertEqual(grammar["max_json_bytes"], 61440)
        self.assertEqual(set(grammar["operation_fields"]), {"import", "submit", "prepare", "commit", "annotate", "refresh"})
        self.assertIn("expected_version", grammar["operation_fields"]["annotate"])
        self.assertIn("expected_version", grammar["operation_fields"]["refresh"])
        grammar["asset_paths"].clear()
        self.assertEqual(len(workflow_contract()["asset_paths"]), 3)

    def test_workflow_validation_rejects_escape_implicit_tokens_and_unknown_grammar(self):
        tree = ast.parse(PUBLIC_TEST_SOURCE)
        names = {"canonical", "strict_json", "validate_workflow"}
        definitions = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
        grammar = workflow_contract()
        namespace = {"json": json, "WORKFLOW_FORMAT": WORKFLOW_FORMAT, "ASSETS": frozenset(grammar["asset_paths"])}
        exec(compile(ast.Module(body=definitions, type_ignores=[]), "workflow-definition", "exec"), namespace)
        validate = namespace["validate_workflow"]
        good = {"format": WORKFLOW_FORMAT, "operations": [
            {"op": "import", "source": "welcome.txt", "path": "release/dataset/welcome.txt"},
            {"op": "annotate", "source": "welcome.txt", "expected_version": 1,
             "notes": "café", "tags": ["demo"], "collections": []}]}
        self.assertEqual(validate(good), good)
        bad_ops = [
            {"op": "import", "source": "../escape.txt", "path": "release/dataset/welcome.txt"},
            {"op": "import", "source": "ok.txt", "path": "../outside.txt"},
            {"op": "annotate", "source": "welcome.txt", "notes": "", "tags": [], "collections": []},
            {"op": "refresh", "source": "welcome.txt", "expected_version": True, "text": "x"},
            {"op": "solve", "expected": {}},
        ]
        for op in bad_ops:
            with self.subTest(op=op), self.assertRaises(ValueError):
                validate({"format": WORKFLOW_FORMAT, "operations": [op]})
        with self.assertRaises(ValueError):
            validate({"format": WORKFLOW_FORMAT, "operations": [good["operations"][0]] * 65})
        with self.assertRaises(ValueError):
            namespace["strict_json"]('{"format":"x","format":"y"}')

    def test_public_artifacts_bind_frozen_inputs_and_disclose_remaining_gaps(self):
        from gossip_harness.library_m4_fixture_v1 import fixture_files
        manifest = public_manifest()
        self.assertEqual(manifest["status"], "unqualified_definition")
        self.assertIsNone(manifest["execution"])
        self.assertEqual(manifest["product_contract_sha256"], CONTRACT_SHA256)
        self.assertEqual(hashlib.sha256(manifest["normative_contract_utf8"].encode()).hexdigest(), CONTRACT_SHA256)
        self.assertEqual(manifest["adapter_sha256"], hashlib.sha256(PUBLIC_TEST_SOURCE.encode()).hexdigest())
        self.assertGreaterEqual(len(manifest["coverage_gaps"]), 6)
        files = fixture_files()
        self.assertEqual(manifest["compatibility_files"], [
            {"path": path, "bytes": len(value), "sha256": hashlib.sha256(value).hexdigest()}
            for path, value in sorted(files.items())])
        public = public_files()
        self.assertEqual(tuple(sorted(public)), PUBLIC_PATHS)
        self.assertEqual(json.loads(public["cumulative-public-contract.json"]), manifest)
        self.assertEqual(public["test_cumulative_public.py"], PUBLIC_TEST_SOURCE)
        ownership = manifest["ownership"]
        self.assertIn("release/dataset/workflow.json", ownership["additional_owned_paths"]["clients"])
        self.assertIn("test_cumulative_public.py", ownership["future_fixture_owned_paths"])
        self.assertIn("solution.py", ownership["immutable_preserved_paths"])
