"""Public v2 fixture definitions: no candidate or reference acceptance claim."""
from __future__ import annotations

import ast
import hashlib
import json
import unittest

from gossip_harness.library_cumulative_public_fixture_v1 import public_manifest as v1_manifest
from gossip_harness.library_cumulative_public_fixture_v2 import (
    CONTRACT_SHA256, COUNTER_MAX, PUBLIC_PATHS, PUBLIC_TEST_SOURCE, WORKFLOW_FORMAT,
    public_files, public_manifest, workflow_contract,
)


class LibraryCumulativePublicFixtureV2Tests(unittest.TestCase):
    def test_exact_new_binding_keeps_positive_cases_and_explicit_gaps(self):
        previous, manifest = v1_manifest(), public_manifest()
        self.assertEqual(manifest["format"], "local-research-library-cumulative-public-v2")
        self.assertEqual(manifest["product_contract_sha256"], CONTRACT_SHA256)
        self.assertEqual(hashlib.sha256(manifest["normative_contract_utf8"].encode()).hexdigest(), CONTRACT_SHA256)
        self.assertEqual(manifest["status"], "unqualified_definition")
        self.assertIsNone(manifest["execution"])
        self.assertEqual(manifest["public_checks"], previous["public_checks"])
        self.assertEqual(manifest["compatibility_files"], previous["compatibility_files"])
        self.assertEqual(manifest["coverage"], previous["coverage"])
        self.assertIn("six v2 persistence amendments", manifest["coverage_gaps"][-1])
        self.assertEqual(manifest["derivation"]["adapter_sha256"], previous["adapter_sha256"])
        self.assertEqual(manifest["adapter_sha256"], hashlib.sha256(PUBLIC_TEST_SOURCE.encode()).hexdigest())
        self.assertEqual(tuple(sorted(public_files())), PUBLIC_PATHS)
        compile(PUBLIC_TEST_SOURCE, "test_cumulative_public.py", "exec")

    def test_signed64_workflow_tokens_validate_exact_integer_domain(self):
        tree = ast.parse(PUBLIC_TEST_SOURCE)
        definitions = [node for node in tree.body if isinstance(node, ast.FunctionDef)
                       and node.name in {"canonical", "strict_json", "validate_workflow"}]
        namespace = {"json": json, "WORKFLOW_FORMAT": WORKFLOW_FORMAT,
                     "ASSETS": frozenset(workflow_contract()["asset_paths"])}
        exec(compile(ast.Module(body=definitions, type_ignores=[]), "public-v2-grammar", "exec"), namespace)
        for key in ("epoch", "expected_version"):
            for token in (1, 2**53 + 1, COUNTER_MAX):
                op = ({"op": "commit", "job_id": "job", "epoch": token} if key == "epoch" else
                      {"op": "refresh", "source": "x.txt", "expected_version": token, "text": "x"})
                value = {"format": WORKFLOW_FORMAT, "operations": [op]}
                self.assertEqual(namespace["validate_workflow"](value), value)
                for invalid in (False, True, 0, -1, COUNTER_MAX + 1, float(token), str(token)):
                    bad = {"format": WORKFLOW_FORMAT, "operations": [{**op, key: invalid}]}
                    with self.subTest(key=key, invalid=invalid), self.assertRaises(ValueError):
                        namespace["validate_workflow"](bad)
        with self.assertRaises(ValueError):
            namespace["strict_json"]('{"x":1,"x":2}')
        self.assertEqual(workflow_contract()["positive_token_max"], COUNTER_MAX)

    def test_mutating_returned_definitions_cannot_rewrite_frozen_inputs(self):
        value = public_manifest()
        value["coverage"].clear()
        value["compatibility_files"].clear()
        value["workflow"]["asset_paths"].clear()
        self.assertGreater(len(public_manifest()["coverage"]), 0)
        self.assertEqual(len(public_manifest()["compatibility_files"]), 5)
        self.assertEqual(len(workflow_contract()["asset_paths"]), 3)
        self.assertEqual(json.loads(public_files()["cumulative-public-contract.json"]), public_manifest())
