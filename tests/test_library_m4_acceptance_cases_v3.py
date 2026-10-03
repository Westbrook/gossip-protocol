"""V3 adapter-only correction checks; no evaluated application is executed."""
from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path
import unittest

from gossip_harness import library_m4_acceptance_cases_v2 as prior
from gossip_harness.library_m4_acceptance_cases_v3 import (
    CHILD_ADAPTER, PRIOR_SOURCE_SHA256, acceptance_cases, registry_manifest, score_case,
)


class _DomainError(Exception):
    code = "domain"


class LibraryM4AcceptanceCasesV3Tests(unittest.TestCase):
    def test_v2_frozen_inputs_expected_and_scorer_are_unchanged(self):
        path = Path(__file__).resolve().parents[1] / "gossip_harness/library_m4_acceptance_cases_v2.py"
        self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), PRIOR_SOURCE_SHA256)
        self.assertEqual(acceptance_cases(), prior.acceptance_cases())
        self.assertIs(score_case, prior.score_case)
        old, new = prior.registry_manifest(), registry_manifest()
        for key in ("ordered_inputs_sha256", "ordered_expected_sha256", "ordered_cases_sha256",
                    "scorer_protocol", "scorer_source", "scorer_sha256", "public_fixture_files_sha256"):
            self.assertEqual(new[key], old[key])
        self.assertEqual(new["adapter_sha256"], "5b4f9e7b04f930e50ac5d0f7703d5d5ae5f2f777c4d2a70f4fa2a1b5a31d9394")
        self.assertEqual(new["protocol"], "library-m4-independent-cases-v3")
        self.assertEqual(new["prior_protocol"], "library-m4-independent-cases-v2")
        self.assertEqual(len(new["corrections"]), 3)
        for case in acceptance_cases():
            self.assertLessEqual(len(json.dumps(case["input"]).encode()), 65536)
            self.assertLessEqual(len(json.dumps(case["expected"]).encode()), 65536)

    def test_exact_adapter_delta_and_no_child_oracle(self):
        original = '        return result\n    except LibraryError as error:'
        replacement = '''        if (action["target"] == "service" and action["method"] == "request"
                and len(action["args"]) == 3 and action["args"][:2] == ["POST", "/api/v1/export"]
                and type(result) is tuple and len(result) == 2
                and type(result[0]) is int and type(result[1]) is bytes):
            result = (result[0], strict_json(result[1]))
        return result
    except LibraryError as error:'''
        self.assertEqual(prior.CHILD_ADAPTER.count(original), 1)
        self.assertEqual(CHILD_ADAPTER, prior.CHILD_ADAPTER.replace(original, replacement))
        compile(CHILD_ADAPTER, "v3-observer", "exec")
        for forbidden in ("score_case", "$integer_at_least", '"expected"', '"passed"', '"accepted"'):
            self.assertNotIn(forbidden, CHILD_ADAPTER)

    def test_only_explicit_versioned_post_export_bytes_pair_decodes(self):
        nodes = [node for node in ast.parse(CHILD_ADAPTER).body if isinstance(node, ast.FunctionDef)
                 and node.name in {"strict_json", "invoke"}]
        class ServiceStub:
            value = (200, b'{"format":"local-research-library-export-v4","generation":0,"documents":[]}')

            def request(self, *args, **kwargs):
                return self.value

            other = request

        stub = ServiceStub()
        namespace = {"json": json, "LibraryError": _DomainError, "Service": lambda *a, **k: stub}
        exec(compile(ast.Module(body=nodes, type_ignores=[]), "isolated-observer-unit", "exec"), namespace)
        invoke = namespace["invoke"]
        action = {"target": "service", "method": "request", "args": ["POST", "/api/v1/export", {}], "kwargs": {}}
        parsed = {"format": "local-research-library-export-v4", "generation": 0, "documents": []}
        self.assertEqual(invoke(action, object(), None, None), (200, parsed))
        for args in (["GET", "/api/v1/export", {}], ["POST", "/api/export", {}],
                     ["POST", "/api/v1/export?x=1", {}], ["POST", "/api/export-bundle", {}],
                     ["POST", "/api/v1/export"], ["POST", "/api/v1/export", {}, None]):
            self.assertIs(invoke(dict(action, args=args), object(), None, None), stub.value)
        self.assertIs(invoke(dict(action, method="other"), object(), None, None), stub.value)
        byte_body = stub.value[1]
        for value in ((200, parsed), (True, byte_body), ("200", byte_body), [200, byte_body],
                      (200, byte_body, 0), (200,), byte_body, (200, byte_body.decode())):
            stub.value = value
            self.assertIs(invoke(action, object(), None, None), value)
        for raw in (b'{"a":1,"a":2}', b'{"a":NaN}', b'{"a":1e999}', b'"\xff"'):
            stub.value = (200, raw)
            with self.subTest(raw=raw), self.assertRaises((ValueError, UnicodeDecodeError)):
                invoke(action, object(), None, None)


if __name__ == "__main__":
    unittest.main()
