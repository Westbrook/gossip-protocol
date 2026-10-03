"""B02 v2 preservation, alias identity and independent JSON fixture controls."""
from __future__ import annotations

import base64
from copy import deepcopy
import hashlib
import json
import unittest

from gossip_harness import candidate_intake_store_cases_v1 as v1
from gossip_harness import candidate_intake_store_cases_v2 as v2
from gossip_harness.candidate_intake_store_observer_v1 import Observation, ObservationUnavailable


def _snapshots(case: dict) -> list[Observation]:
    rows = []
    for phase in v2.PHASES:
        data = deepcopy(case["expected"][phase])
        strings = {row["public"]["job_id"]: {
            "manifest": v2.encoded(row["manifest"]).decode(),
            "content_hashes": v2.encoded(row["content_hashes"]).decode(),
            "receipt": None if row["receipt"] is None else v2.encoded(row["receipt"]).decode(),
        } for row in data["jobs"]}
        rows.append(Observation(data, strings, (), "a" * 64, "b" * 64, {"control": []}, "unit-profile"))
    return rows


def _fixture(case: dict) -> bytes:
    row = next(row for row in case["recipe"]["fixtures"] if row["path"] == "batch")
    return base64.b64decode(row["bytes_base64"], validate=True)


def _evaluate(case: dict, snapshots: list[Observation] | None = None, results: dict | None = None) -> dict:
    return v2.evaluate_case(case["case_id"], *(_snapshots(case) if snapshots is None else snapshots),
                            deepcopy(case["expected"]["results"]) if results is None else results)


class CandidateIntakeStoreV2DefinitionsTests(unittest.TestCase):
    def test_original_238_rows_are_exactly_unchanged_and_ordered(self) -> None:
        old = v1.definitions()
        new = v2.definitions()
        self.assertEqual(len(old), 238)
        self.assertEqual(len(new), 241)
        self.assertEqual(v2.encoded(new[:238]), v2.encoded(old))
        self.assertEqual(v2.CASE_IDS[:238], v1.CASE_IDS)
        self.assertEqual(v2.CASE_IDS[238:], v2.EXTRA_CASE_IDS)
        self.assertEqual(v1.definition_sha256(), v2.INHERITED_DEFINITION_SHA256)
        self.assertNotEqual(v2.definition_sha256(), v1.definition_sha256())

    def test_each_original_selected_branch_and_recipe_is_preserved(self) -> None:
        for case in v1.definitions():
            case_id = case["case_id"]
            self.assertEqual(v2.execution_recipe(case_id), v1.execution_recipe(case_id))
            for expected in case.get("expected_alternatives", {"only": case["expected"]}).values():
                self.assertEqual(v2.select_expected_case(case_id, expected["results"]),
                                 v1.select_expected_case(case_id, expected["results"]))

    def test_large_legal_whitespace_changes_no_decoded_content(self) -> None:
        case = v2.case_definition("intake-json-large-whitespace-valid")
        raw = _fixture(case)
        self.assertGreater(len(raw), 6 * 1024 * 1024)
        self.assertLess(len(raw), 8 * 1024 * 1024)
        self.assertEqual(json.loads(raw), json.loads(_fixture(v1.case_definition("intake-json-literal"))))
        for prefix in (raw[:3 * 1024 * 1024], raw[-3 * 1024 * 1024:]):
            self.assertEqual(set(prefix), set(b" \t\r\n"))
        self.assertEqual(case["expected"], v1.case_definition("intake-json-literal")["expected"])

    def test_malformed_suffix_is_decisive_json_syntax_error(self) -> None:
        valid = _fixture(v2.case_definition("intake-json-large-whitespace-valid"))
        case = v2.case_definition("intake-json-large-whitespace-syntax")
        raw = _fixture(case)
        self.assertEqual(raw, valid + b"!")
        raw.decode("utf-8", errors="strict")
        with self.assertRaises(json.JSONDecodeError):
            json.loads(raw)
        self.assertEqual(case["expected"]["results"]["after"], [{"error": "invalid_json"}, {"error": "not_found"}])
        self.assertEqual(case["expected"]["before"], case["expected"]["after"])

    def test_invalid_utf8_suffix_is_decisive_decoder_error(self) -> None:
        valid = _fixture(v2.case_definition("intake-json-large-whitespace-valid"))
        case = v2.case_definition("intake-json-large-whitespace-utf8")
        raw = _fixture(case)
        self.assertEqual(raw, valid + b"\xff")
        with self.assertRaises(UnicodeDecodeError):
            raw.decode("utf-8", errors="strict")
        self.assertEqual(case["expected"]["results"]["after"], [{"error": "invalid_utf8"}, {"error": "not_found"}])
        self.assertEqual(case["expected"]["before"], case["expected"]["reopened"])

    def test_extra_recipes_and_sources_have_explicit_fixed_bindings(self) -> None:
        self.assertEqual(set(v2.definition_sources()), set(v2.INHERITED_SOURCES) | {
            "gossip_harness/candidate_intake_store_cases_v2.py"})
        for case_id, base_id in v2.EXTRA_ALIASES:
            case = v2.case_definition(case_id)
            base = v1.case_definition(base_id)
            binding = case["scorer_alias"]
            self.assertEqual(binding["case_id"], case_id)
            self.assertEqual(binding["base_case_id"], base_id)
            self.assertEqual(binding["base_case_sha256"], hashlib.sha256(v2.encoded(base)).hexdigest())
            self.assertEqual(binding["recipe_sha256"], hashlib.sha256(v2.encoded(case["recipe"])).hexdigest())
            self.assertNotEqual(binding["recipe_sha256"], binding["base_recipe_sha256"])
            self.assertEqual(binding["expected_history_sha256"], hashlib.sha256(v2.encoded(base["expected"])).hexdigest())
            self.assertEqual(binding["fixture_sha256"], hashlib.sha256(_fixture(case)).hexdigest())
            self.assertLess(len(v2.encoded(case["recipe"])), 12 * 1024 * 1024)
            self.assertTrue(all(assertion["case_id"] == case_id for assertion in case["assertions"]))

    def test_accessors_do_not_allow_caller_recipe_or_expected_mutation(self) -> None:
        digest = v2.definition_sha256()
        case_id = v2.EXTRA_CASE_IDS[0]
        case = v2.case_definition(case_id)
        case["expected"]["after"]["jobs"].clear()
        case["recipe"]["phases"]["after"].clear()
        case["scorer_alias"]["base_case_id"] = "missing-get_job"
        self.assertTrue(v2.case_definition(case_id)["expected"]["after"]["jobs"])
        self.assertTrue(v2.execution_recipe(case_id)["phases"]["after"])
        self.assertEqual(v2.definition_sha256(), digest)
        with self.assertRaises(ValueError):
            v2.case_definition("caller-invented-alias")

    def test_alias_guard_rejects_identity_digest_and_history_mutations(self) -> None:
        for mutation in ("identity", "recipe", "expected", "fixture"):
            case = v2.case_definition(v2.EXTRA_CASE_IDS[0])
            base = v1.case_definition(case["scorer_alias"]["base_case_id"])
            if mutation == "identity":
                case["scorer_alias"]["base_case_id"] = "missing-get_job"
            elif mutation == "recipe":
                case["recipe"]["phases"]["after"].clear()
            elif mutation == "expected":
                case["expected"]["after"]["jobs"].clear()
            else:
                case["recipe"]["fixtures"][0]["bytes_base64"] = "YQ=="
            with self.assertRaises(RuntimeError):
                v2._validate_alias(case, base)


class CandidateIntakeStoreV2ScorerTests(unittest.TestCase):
    def test_extra_successes_report_actual_identity_and_explicit_alias(self) -> None:
        for case_id, base_id in v2.EXTRA_ALIASES:
            case = v2.case_definition(case_id)
            result = _evaluate(case)
            self.assertTrue(result["all_local_assertions_passed"])
            self.assertEqual(result["protocol"], v2.PROTOCOL)
            self.assertEqual(result["case_id"], case_id)
            self.assertEqual(result["scorer_alias"]["base_case_id"], base_id)
            self.assertEqual(result["recipe_sha256"], case["scorer_alias"]["recipe_sha256"])
            self.assertEqual(result["definition_sha256"], v2.definition_sha256())
            self.assertIsNone(result["full_requirement_verdict"])

    def test_original_score_checks_and_unavailability_are_unchanged(self) -> None:
        for case_id in ("replay-completed", "interfere-start_job-cancel", "intake-json-count-over"):
            case = v1.case_definition(case_id)
            snapshots = _snapshots(case)
            results = deepcopy(case["expected"]["results"])
            if case_id.startswith("interfere-"):
                snapshots[0].data["documents"][0]["text"] = "already incorrect"
                results["after"][0] = {"observation_unavailable": "public_boundary_interposition"}
            old = v1.evaluate_case(case_id, *snapshots, results)
            new = v2.evaluate_case(case_id, *snapshots, results)
            for key in ("checks", "observation_unavailable", "unavailable_phases", "expected_variant", "all_local_assertions_passed"):
                self.assertEqual(old[key], new[key])
            self.assertIsNone(new["scorer_alias"])
        case_id = "intake-json-count-over"
        case = v2.case_definition(case_id)
        case["expected"] = deepcopy(case["expected_alternatives"]["immediate"])
        result = _evaluate(case)
        self.assertEqual(result["expected_variant"], "immediate")
        self.assertEqual(result["expected_history_sha256"], hashlib.sha256(v2.encoded(case["expected"])).hexdigest())

    def test_success_rejects_raw_size_cap_as_wrong_outcome(self) -> None:
        case = v2.case_definition(v2.EXTRA_CASE_IDS[0])
        results = deepcopy(case["expected"]["results"])
        results["after"][0] = {"error": "too_large"}
        result = _evaluate(case, results=results)
        self.assertFalse(result["checks"]["after.result.0"])
        self.assertFalse(result["all_local_assertions_passed"])

    def test_syntax_and_utf8_codes_cannot_be_interchanged(self) -> None:
        for case_id, wrong in ((v2.EXTRA_CASE_IDS[1], "invalid_utf8"), (v2.EXTRA_CASE_IDS[2], "invalid_json")):
            case = v2.case_definition(case_id)
            results = deepcopy(case["expected"]["results"])
            results["after"][0] = {"error": wrong}
            self.assertFalse(_evaluate(case, results=results)["checks"]["after.result.0"])

    def test_negative_case_rejects_admission_or_orphan_despite_correct_error(self) -> None:
        for case_id in v2.EXTRA_CASE_IDS[1:]:
            case = v2.case_definition(case_id)
            snapshots = _snapshots(case)
            snapshots[1].data["jobs"].append(v1.stored_job("unexpected", []))
            snapshots[1].data["blobs"].append({"blob_id": "orphan", "sha256": "c" * 64, "bytes": 1})
            result = _evaluate(case, snapshots=snapshots)
            self.assertTrue(result["checks"]["after.result.0"])
            self.assertFalse(result["checks"]["after.persisted-state"])

    def test_unavailable_newcase_keeps_earlier_failure_and_masks_dependents(self) -> None:
        case = v2.case_definition(v2.EXTRA_CASE_IDS[0])
        snapshots = _snapshots(case)
        snapshots[0].data["documents"][0]["text"] = "wrong before"
        results = deepcopy(case["expected"]["results"])
        results["after"][0] = {"observation_unavailable": "reader_unavailable"}
        result = _evaluate(case, snapshots, results)
        self.assertFalse(result["checks"]["before.persisted-state"])
        self.assertIsNone(result["checks"]["after.result.0"])
        self.assertIsNone(result["checks"]["reopened.persisted-state"])
        self.assertEqual(result["case_id"], case["case_id"])
        self.assertFalse(result["all_local_assertions_passed"])

    def test_precondition_mismatch_remains_unavailable(self) -> None:
        case = v2.case_definition(v2.EXTRA_CASE_IDS[0])
        with self.assertRaises(ObservationUnavailable):
            v2.evaluate_case(case["case_id"], *_snapshots(case), {})


if __name__ == "__main__":
    unittest.main()
