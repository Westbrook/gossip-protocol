"""V2 definition, correction-delta and host scorer checks; no product execution."""
from __future__ import annotations

import ast
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import unittest

from gossip_harness import library_m4_acceptance_cases_v1 as prior
from gossip_harness.library_m4_acceptance_cases_v2 import (
    CHILD_ADAPTER, DOMAIN_CASE_ID, DOMAIN_FIELD, DOMAIN_OBSERVATION, MINIMUM_MARKER,
    PRIOR_SOURCE_SHA256, acceptance_cases, registry_manifest, score_case,
)


def _domain_example():
    case = next(case for case in acceptance_cases() if case["id"] == DOMAIN_CASE_ID)
    actual = deepcopy(case["expected"])
    actual["observations"][DOMAIN_OBSERVATION][DOMAIN_FIELD] = 0
    return case, actual


class LibraryM4AcceptanceCasesV2Tests(unittest.TestCase):
    def test_frozen_v1_preserved_and_v2_hashes(self):
        path = Path(__file__).resolve().parents[1] / "gossip_harness/library_m4_acceptance_cases_v1.py"
        self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), PRIOR_SOURCE_SHA256)
        manifest = registry_manifest()
        self.assertEqual(manifest["ordered_inputs_sha256"], prior.registry_manifest()["ordered_inputs_sha256"])
        self.assertEqual(manifest["ordered_expected_sha256"], "7000373db9de73dad1fa8ff8bc82fbade0c4416f03a2395fdf5812399be1bbb3")
        self.assertEqual(manifest["ordered_cases_sha256"], "87104867f4bfdf573a7ff05d6af09e03c0afe2f48270ca47ed8da285f77139d5")
        self.assertEqual(manifest["adapter_sha256"], "a730db692ccce165b5f2d832b982ca4eca24ab90edca3d70521d8c0832904712")
        self.assertEqual(manifest["scorer_sha256"], "418c1e5307ebc30ac8363834690739f5ba22931ebce941db7246f563527b329c")
        self.assertEqual(hashlib.sha256(manifest["scorer_source"].encode()).hexdigest(), manifest["scorer_sha256"])
        self.assertEqual(manifest["scorer_protocol"], "library-m4-host-observation-scorer-v2")
        self.assertEqual(len(manifest["corrections"]), 2)

    def test_only_one_expected_leaf_changes_and_inputs_stay_identical(self):
        old = prior.acceptance_cases()
        new = acceptance_cases()
        self.assertEqual(len(new), 18)
        changed = next(case for case in new if case["id"] == DOMAIN_CASE_ID)
        self.assertEqual(changed["expected"]["observations"][DOMAIN_OBSERVATION][DOMAIN_FIELD], {MINIMUM_MARKER: 0})
        changed["expected"]["observations"][DOMAIN_OBSERVATION][DOMAIN_FIELD] = 0
        self.assertEqual(new, old)
        for case in acceptance_cases():
            self.assertLessEqual(len(json.dumps(case["input"]).encode()), 65536)
            self.assertLessEqual(len(json.dumps(case["expected"]).encode()), 65536)
            self.assertNotIn("expected", case["input"])

    def test_typed_nonnegative_worker_generation_domain(self):
        case, example = _domain_example()
        for generation in (0, 1, 17, 10 ** 30):
            actual = deepcopy(example)
            actual["observations"][DOMAIN_OBSERVATION][DOMAIN_FIELD] = generation
            self.assertTrue(score_case(case, actual))
        for generation in (True, False, -1, -10 ** 30, 0.0, 1.0, "0", None, [], {}, {MINIMUM_MARKER: 0}):
            actual = deepcopy(example)
            actual["observations"][DOMAIN_OBSERVATION][DOMAIN_FIELD] = generation
            with self.subTest(generation=generation):
                self.assertFalse(score_case(case, actual))

    def test_wrong_shapes_fields_and_other_observations_reject(self):
        case, example = _domain_example()
        variants = [None, [], {"observations": None}, {"observations": []}, dict(example, extra=0)]
        for change in ("missing", "extra", "wrong_generation", "wrong_schema", "wrong_jobs", "earlier"):
            value = deepcopy(example)
            current = value["observations"][DOMAIN_OBSERVATION]
            if change == "missing":
                del current[DOMAIN_FIELD]
            elif change == "extra":
                current["extra"] = 0
            elif change == "wrong_generation":
                current["generation"] += 1
            elif change == "wrong_schema":
                current["schema"] = 3
            elif change == "wrong_jobs":
                current["jobs"]["completed"] += 1
            else:
                value["observations"][0] = {"opened": False}
            variants.append(value)
        for value in variants:
            with self.subTest(value=value):
                self.assertFalse(score_case(case, value))
        changed_case = deepcopy(case)
        changed_case["expected"]["observations"][DOMAIN_OBSERVATION][DOMAIN_FIELD] = {MINIMUM_MARKER: 1}
        self.assertFalse(score_case(changed_case, example))

    def test_all_other_cases_remain_exact_and_type_sensitive(self):
        for case in acceptance_cases():
            if case["id"] == DOMAIN_CASE_ID:
                continue
            self.assertTrue(score_case(case, deepcopy(case["expected"])))
            wrong = deepcopy(case["expected"])
            wrong["extra"] = None
            self.assertFalse(score_case(case, wrong))
        artificial = {"id": "ordinary", "expected": {"observations": [{"generation": 0, "flag": True}]}}
        self.assertFalse(score_case(artificial, {"observations": [{"generation": False, "flag": True}]}))
        self.assertFalse(score_case(artificial, {"observations": [{"generation": 0.0, "flag": True}]}))
        self.assertFalse(score_case(artificial, {"observations": [{"generation": 0, "flag": 1}]}))
        artificial["expected"] = {"observations": [{MINIMUM_MARKER: 0}]}
        self.assertFalse(score_case(artificial, {"observations": [1]}))

    def test_adapter_change_is_only_named_legacy_export_decoding(self):
        original = '        return function(*action["args"], **action["kwargs"])'
        replacement = '''        result = function(*action["args"], **action["kwargs"])
        if action["target"] == "service" and action["method"] == "export" and type(result) is bytes:
            result = strict_json(result)
        return result'''
        self.assertEqual(CHILD_ADAPTER, prior.CHILD_ADAPTER.replace(original, replacement))
        compile(CHILD_ADAPTER, "m4-v2-observer", "exec")
        self.assertNotIn("score_case", CHILD_ADAPTER)
        self.assertNotIn("$integer_at_least", CHILD_ADAPTER)
        self.assertNotIn('"expected"', CHILD_ADAPTER)
        self.assertNotIn('"passed"', CHILD_ADAPTER)
        self.assertNotIn('"accepted"', CHILD_ADAPTER)
        module = ast.parse(CHILD_ADAPTER)
        helpers = [node for node in module.body if isinstance(node, ast.FunctionDef)
                   and node.name in {"strict_json", "invoke"}]
        namespace = {"json": json, "Service": lambda *args, **kwargs: _FakeService(), "LibraryError": ValueError}
        exec(compile(ast.Module(body=helpers, type_ignores=[]), "observer-unit", "exec"), namespace)
        action = {"target": "service", "method": "export", "args": [], "kwargs": {}}
        self.assertEqual(namespace["invoke"](action, object(), None, None), {"format": "legacy", "documents": []})
        action["method"] = "export_v1"
        self.assertEqual(namespace["invoke"](action, object(), None, None), b'{"format":"legacy","documents":[]}')

    def test_registry_and_definitions_are_fresh(self):
        before = registry_manifest()
        cases = acceptance_cases()
        cases[0]["expected"]["observations"].clear()
        cases[0]["input"]["actions"].clear()
        case, actual = _domain_example()
        saved_case, saved_actual = deepcopy(case), deepcopy(actual)
        self.assertTrue(score_case(case, actual))
        self.assertEqual(case, saved_case)
        self.assertEqual(actual, saved_actual)
        self.assertEqual(registry_manifest(), before)


class _FakeService:
    def export(self):
        return b'{"format":"legacy","documents":[]}'

    export_v1 = export


if __name__ == "__main__":
    unittest.main()
