"""Offline controls for the versioned wrong-kind partial evaluation policy."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
import hashlib
import unittest
from unittest.mock import patch

from gossip_harness import candidate_intake_store_cases_v1 as v1
from gossip_harness import candidate_intake_store_cases_v2 as v2
from gossip_harness import candidate_intake_store_cases_v3 as v3
from gossip_harness.candidate_intake_store_observer_v1 import Observation, ObservationUnavailable


def _sha(value: object) -> str:
    return hashlib.sha256(v3.encoded(value)).hexdigest()


def _snapshots(case: dict) -> list[Observation]:
    snapshots = []
    for phase in v3.PHASES:
        state = deepcopy(case["expected"][phase])
        strings = {row["public"]["job_id"]: {
            "manifest": v3.encoded(row["manifest"]).decode(),
            "content_hashes": v3.encoded(row["content_hashes"]).decode(),
            "receipt": None if row["receipt"] is None else v3.encoded(row["receipt"]).decode(),
        } for row in state["jobs"]}
        snapshots.append(Observation(state, strings, (), "a" * 64, "b" * 64, {"control": []}, "unit-profile"))
    return snapshots


def _evaluate(case: dict, snapshots: list[Observation] | None = None, results: dict | None = None) -> dict:
    return v3.evaluate_case(case["case_id"], *(_snapshots(case) if snapshots is None else snapshots),
        deepcopy(case["expected"]["results"]) if results is None else results)


def _with_frame(case: dict, frame: object) -> dict:
    results = deepcopy(case["expected"]["results"])
    results["after"][0] = deepcopy(frame)
    return results


def _job(total: int = 0) -> dict:
    return {"job_id": "case", "epoch": 1, "state": "queued", "total": total, "completed": 0, "error": None}


class CandidateIntakeStoreV3DefinitionTests(unittest.TestCase):
    def test_all_241_recipes_order_and_baseline_expected_histories_are_preserved(self) -> None:
        old = v2.definitions()
        new = v3.definitions()
        self.assertEqual(len(new), 241)
        self.assertEqual(v3.CASE_IDS, v2.CASE_IDS)
        unchanged = 0
        for before, after in zip(old, new):
            self.assertEqual(after["case_id"], before["case_id"])
            self.assertEqual(after["recipe"], before["recipe"])
            self.assertEqual(after["expected"], before["expected"])
            self.assertEqual(after.get("expected_alternatives"), before.get("expected_alternatives"))
            if after["case_id"] not in v3.POLICY_LIMITED_CASE_IDS:
                self.assertEqual(after, before)
                unchanged += 1
        self.assertEqual(unchanged, 238)
        self.assertEqual(v2.definition_sha256(), v3.INHERITED_DEFINITION_SHA256)
        self.assertNotEqual(v3.definition_sha256(), v2.definition_sha256())

    def test_three_policy_rows_bind_fixed_baselines_and_no_exact_code_credit(self) -> None:
        self.assertEqual(v3.POLICY_LIMITED_CASE_IDS, (
            "intake-directory-wrong-kind", "intake-zip-wrong-kind", "intake-json-wrong-kind"))
        for case_id in v3.POLICY_LIMITED_CASE_IDS:
            base = v2.case_definition(case_id)
            case = v3.case_definition(case_id)
            disposition = case["evaluation_disposition"]
            self.assertEqual(disposition["baseline_case_sha256"], _sha(base))
            self.assertEqual(disposition["baseline_recipe_sha256"], _sha(base["recipe"]))
            self.assertEqual(disposition["baseline_expected_history_sha256"], _sha(base["expected"]))
            self.assertEqual(disposition["evaluation_policy_sha256"], v3.evaluation_policy_sha256())
            self.assertFalse(disposition["exact_code_credit"])
            for assertion in case["assertions"]:
                if assertion["selector"] == "/results/after/0":
                    expected = (v3.UNSPECIFIED_ASSERTION_IDS[0] if assertion["requirement_id"] == "M1-I28"
                                else v3.NATIVE_REJECTION_ASSERTION_ID)
                    self.assertEqual(assertion["check_id"], expected)
                    self.assertFalse(assertion["full_requirement"])

    def test_policy_and_source_identities_bind_normative_sources_not_mutable_receipts(self) -> None:
        policy = v3.evaluation_policy()
        self.assertEqual(policy["exact_code_status"], "unspecified")
        self.assertIsNone(policy["exact_code"])
        self.assertFalse(policy["product_requirement_closure"])
        self.assertEqual(v3.evaluation_policy_sha256(), _sha(policy))
        self.assertEqual(set(v3.definition_sources()), set(v3.INHERITED_SOURCES) | set(v3.NORMATIVE_SOURCES)
                         | {"gossip_harness/candidate_intake_store_cases_v3.py"})
        self.assertFalse(any(name.startswith("runs/") for name in v3.definition_sources()))

    def test_defensive_accessors_cannot_change_recipes_policy_or_histories(self) -> None:
        digest = v3.definition_sha256()
        case_id = v3.POLICY_LIMITED_CASE_IDS[0]
        case = v3.case_definition(case_id)
        case["expected"]["after"]["jobs"].clear()
        case["evaluation_disposition"]["exact_code_credit"] = True
        v3.execution_recipe(case_id)["phases"]["after"].clear()
        v3.evaluation_policy()["case_ids"].clear()
        self.assertTrue(v3.case_definition(case_id)["expected"]["after"]["jobs"])
        self.assertFalse(v3.case_definition(case_id)["evaluation_disposition"]["exact_code_credit"])
        self.assertTrue(v3.execution_recipe(case_id)["phases"]["after"])
        self.assertEqual(len(v3.evaluation_policy()["case_ids"]), 3)
        self.assertEqual(v3.definition_sha256(), digest)
        with self.assertRaises(ValueError):
            v3.case_definition("invented-policy-alias")

    def test_loaded_frozen_and_normative_source_guards_refuse_drift(self) -> None:
        sources = v3.definition_sources()
        sources["gossip_harness/candidate_intake_store_cases_v3.py"] = "0" * 64
        with patch.object(v3, "definition_sources", return_value=sources), self.assertRaisesRegex(RuntimeError, "loaded B02 v3"):
            v3.definition_sha256()
        sources = v3.definition_sources()
        sources["library-cumulative-product-v2.json"] = "0" * 64
        with patch.object(v3, "definition_sources", return_value=sources), self.assertRaisesRegex(RuntimeError, "normative source"):
            v3.evaluation_policy()
        with patch.object(v2, "definition_sha256", return_value="0" * 64), self.assertRaisesRegex(RuntimeError, "v2 definition"):
            v3.case_definition(v3.CASE_IDS[0])

    def test_contract_hash_binds_selected_immediate_and_deferred_histories(self) -> None:
        for case_id in v1.AMBIGUOUS_JSON_CASES:
            hashes = set()
            for variant in ("immediate", "deferred"):
                case = v3.case_definition(case_id)
                case["expected"] = deepcopy(case["expected_alternatives"][variant])
                scored = _evaluate(case)
                hashes.add(scored["evaluation_contract_sha256"])
                self.assertEqual(scored["expected_variant"], variant)
                self.assertEqual(scored["expected_history_sha256"], _sha(case["expected"]))
                self.assertEqual(scored["baseline_expected_history_sha256"], _sha(case["expected"]))
                self.assertEqual(scored["evaluation_contract_sha256"],
                                 v3.evaluation_contract_sha256(case_id, case["expected"]["results"]))
                self.assertTrue(scored["all_local_assertions_passed"])
            self.assertEqual(len(hashes), 2)


class CandidateIntakeStoreV3PolicyTests(unittest.TestCase):
    def test_nonempty_native_codes_have_equal_supported_judgment_and_preserve_code(self) -> None:
        for case_id in v3.POLICY_LIMITED_CASE_IDS:
            case = v3.case_definition(case_id)
            for code in ("io_error", "invalid_source", "unanticipated_code", " ", "é"):
                with self.subTest(case_id=case_id, code=code):
                    scored = _evaluate(case, results=_with_frame(case, {"error": code}))
                    self.assertTrue(scored["supported_assertions_passed"])
                    self.assertTrue(scored["supported_domain_rejection"])
                    self.assertTrue(scored["native_error_frame_qualified"])
                    self.assertTrue(scored["partial_qualification"])
                    self.assertFalse(scored["all_local_assertions_passed"])
                    self.assertFalse(scored["exact_code_credit"])
                    self.assertEqual(scored["observed_error_code"], code)
                    self.assertEqual(scored["observed_error_frame"], {"error": code})
                    self.assertEqual(scored["normalizer_frame_status"], "native-error")
                    self.assertEqual(scored["legacy_exact_error_diagnostic"]["matched"], code == "io_error")
                    self.assertEqual(scored["unspecified_assertions"], list(v3.UNSPECIFIED_ASSERTION_IDS))
                    self.assertEqual(scored["observation_unavailable"], [])
                    self.assertEqual({key for key, value in scored["checks"].items() if value is None},
                                     set(v3.UNSPECIFIED_ASSERTION_IDS))
                    self.assertIsNone(scored["full_requirement_verdict"])

    def test_valid_job_success_including_zero_entries_is_known_rejection_failure(self) -> None:
        for case_id in v3.POLICY_LIMITED_CASE_IDS:
            case = v3.case_definition(case_id)
            for total in (0, 1, 65):
                scored = _evaluate(case, results=_with_frame(case, {"value": _job(total)}))
                self.assertFalse(scored["supported_domain_rejection"])
                self.assertFalse(scored["supported_assertions_passed"])
                self.assertEqual(scored["normalizer_frame_status"], "job-success")
                self.assertEqual(scored["local_outcome"], "supported-failed")
                self.assertEqual(scored["observation_unavailable"], [])

    def test_unrecognized_presentations_are_unknown_not_product_failures_or_passes(self) -> None:
        frames = [None, [], {}, {"error": ""}, {"error": True}, {"error": "x", "value": None},
                  {"value": None}, {"value": {"error": "x"}}, {"unexpected_exception": "OSError"},
                  {"value": {**_job(), "epoch": True}}, {"value": {**_job(), "extra": True}},
                  {"value": {**_job(), "total": -1}}, {"value": {**_job(), "job_id": "bad/id"}}]
        for case_id in v3.POLICY_LIMITED_CASE_IDS:
            case = v3.case_definition(case_id)
            for frame in frames:
                with self.subTest(case_id=case_id, frame=frame):
                    scored = _evaluate(case, results=_with_frame(case, frame))
                    self.assertIsNone(scored["supported_domain_rejection"])
                    self.assertFalse(scored["native_error_frame_qualified"])
                    self.assertFalse(scored["supported_assertions_passed"])
                    self.assertFalse(scored["all_local_assertions_passed"])
                    self.assertEqual(scored["local_outcome"], "observation-unavailable")
                    self.assertEqual(scored["normalizer_frame_status"], "unqualified")
                    self.assertEqual(scored["unavailable_phases"], [])
                    self.assertFalse(any(value is False for value in scored["checks"].values()))
                    self.assertTrue(scored["checks"]["after.persisted-state"])
                    self.assertEqual(scored["observed_error_frame"], frame)
                    self.assertEqual([row["check_id"] for row in scored["observation_unavailable"]],
                                     [v3.NATIVE_REJECTION_ASSERTION_ID])

    def test_native_job_recognition_has_strict_types_domains_and_state_shape(self) -> None:
        for state in ("queued", "running", "cancelled", "completed", "failed"):
            job = {**_job(2), "state": state, "completed": 2 if state == "completed" else 0,
                   "error": "x" if state == "failed" else None}
            self.assertTrue(v3._is_job(job), state)
        for field, value in (("epoch", 0), ("epoch", 2**63), ("epoch", True), ("total", True),
                             ("completed", True), ("completed", 1), ("state", "unknown"),
                             ("error", "x"), ("job_id", ""), ("job_id", "a" * 65)):
            self.assertFalse(v3._is_job({**_job(), field: value}), (field, value))
        self.assertFalse(v3._is_job({**_job(), "state": "failed"}))

    def test_no_admission_lookup_result_and_state_checks_remain_strict(self) -> None:
        for case_id in v3.POLICY_LIMITED_CASE_IDS:
            case = v3.case_definition(case_id)
            for frame in ({"error": "other"}, {"value": None}):
                snapshots = _snapshots(case)
                snapshots[1].data["jobs"].append(v1.stored_job("case", []))
                results = _with_frame(case, frame)
                results["after"][1] = {"value": _job()}
                scored = _evaluate(case, snapshots, results)
                self.assertFalse(scored["checks"]["after.result.1"])
                self.assertFalse(scored["checks"]["after.persisted-state"])
                self.assertFalse(scored["supported_assertions_passed"])
                self.assertEqual(scored["local_outcome"], "supported-failed")

    def test_document_blob_job_and_manifest_failures_are_never_masked_by_policy(self) -> None:
        for case_id in v3.POLICY_LIMITED_CASE_IDS:
            case = v3.case_definition(case_id)
            for facet in ("document", "blob", "job", "manifest", "hashes", "receipt"):
                for frame in ({"error": "other"}, {"value": {"error": "maybe_native"}}):
                    snapshots = _snapshots(case)
                    if facet == "document":
                        snapshots[1].data["documents"][0]["text"] = "changed"
                    elif facet == "blob":
                        snapshots[1].data["blobs"].append({"blob_id": "orphan", "sha256": "f" * 64, "bytes": 0})
                    elif facet == "job":
                        snapshots[1].data["jobs"][0]["public"]["epoch"] = 2
                    else:
                        field = {"manifest": "manifest", "hashes": "content_hashes", "receipt": "receipt"}[facet]
                        snapshots[1].persisted_strings["untouched"][field] = "changed"
                    scored = _evaluate(case, snapshots, _with_frame(case, frame))
                    self.assertTrue(any(value is False for value in scored["checks"].values()), facet)
                    self.assertFalse(scored["supported_assertions_passed"])
                    self.assertEqual(scored["local_outcome"], "supported-failed")

    def test_auxiliary_conservation_failure_survives_rejection_or_unknown_frame(self) -> None:
        for case_id in v3.POLICY_LIMITED_CASE_IDS:
            case = v3.case_definition(case_id)
            for frame in ({"error": "other"}, {"value": None}):
                snapshots = _snapshots(case)
                snapshots[1].auxiliary_tables["control"].append({"value": "unexpected"})
                scored = _evaluate(case, snapshots, _with_frame(case, frame))
                self.assertFalse(scored["checks"]["after.auxiliary-conservation"])
                self.assertFalse(scored["checks"]["reopened.auxiliary-conservation"])
                self.assertFalse(scored["supported_assertions_passed"])

    def test_before_failure_survives_unknown_and_explicit_unavailable_markers(self) -> None:
        case = v3.case_definition(v3.POLICY_LIMITED_CASE_IDS[0])
        for frame in ({"value": None}, {"observation_unavailable": "reader_unavailable"},
                      {"not_run": "dependency_unavailable"}):
            snapshots = _snapshots(case)
            snapshots[0].data["documents"][0]["text"] = "prior wrong"
            scored = _evaluate(case, snapshots, _with_frame(case, frame))
            self.assertFalse(scored["checks"]["before.persisted-state"])
            self.assertFalse(scored["supported_assertions_passed"])
            self.assertIsNone(scored["checks"][v3.NATIVE_REJECTION_ASSERTION_ID])
            self.assertEqual(scored["unspecified_assertions"], list(v3.UNSPECIFIED_ASSERTION_IDS))
            self.assertNotIn(v3.UNSPECIFIED_ASSERTION_IDS[0],
                             [row["check_id"] for row in scored["observation_unavailable"]])
            if "value" not in frame:
                self.assertIsNone(scored["checks"]["after.persisted-state"])
                self.assertIsNone(scored["checks"]["reopened.persisted-state"])
                self.assertEqual(scored["normalizer_frame_status"], "dependency-unavailable")

    def test_after_failure_and_observed_code_survive_later_unavailability(self) -> None:
        case = v3.case_definition(v3.POLICY_LIMITED_CASE_IDS[0])
        snapshots = _snapshots(case)
        snapshots[1].persisted_strings["untouched"]["receipt"] = "unexpected"
        results = _with_frame(case, {"error": "different"})
        results["reopened"] = [{"observation_unavailable": "later_reader"}]
        scored = _evaluate(case, snapshots, results)
        self.assertTrue(scored["supported_domain_rejection"])
        self.assertEqual(scored["observed_error_code"], "different")
        self.assertFalse(scored["checks"]["after.untouched.receipt-conservation"])
        self.assertIsNone(scored["checks"]["reopened.persisted-state"])
        self.assertFalse(scored["supported_assertions_passed"])

    def test_missing_extra_outputs_and_registration_mismatch_cannot_qualify(self) -> None:
        case = v3.case_definition(v3.POLICY_LIMITED_CASE_IDS[0])
        for after in ([], [{"error": "x"}], [{"error": "x"}, {"error": "not_found"}, {"value": None}]):
            results = deepcopy(case["expected"]["results"])
            results["after"] = after
            scored = _evaluate(case, results=results)
            self.assertFalse(scored["checks"]["after.result-count"])
            self.assertFalse(scored["supported_assertions_passed"])
        snapshots = _snapshots(case)
        snapshots[1] = replace(snapshots[1], registration_sha256="f" * 64)
        with self.assertRaises(ObservationUnavailable):
            _evaluate(case, snapshots)
        with self.assertRaises(ObservationUnavailable):
            _evaluate(case, results={})

    def test_ordinary_empty_intake_still_passes_and_nonpolicy_checks_match_v2(self) -> None:
        for case_id in ("intake-directory-empty", "intake-zip-empty", "intake-json-empty", "replay-completed",
                        "intake-json-large-whitespace-valid", "interfere-start_job-cancel"):
            case = v3.case_definition(case_id)
            snapshots = _snapshots(case)
            results = deepcopy(case["expected"]["results"])
            if case_id.startswith("interfere-"):
                snapshots[0].data["documents"][0]["text"] = "wrong before"
                results["after"][0] = {"observation_unavailable": "public_boundary_interposition"}
            old = v2.evaluate_case(case_id, *snapshots, results)
            scored = _evaluate(case, snapshots, results)
            for key in ("checks", "observation_unavailable", "unavailable_phases", "all_local_assertions_passed"):
                self.assertEqual(scored[key], old[key])
            self.assertFalse(scored["partial_qualification"])
            self.assertEqual(scored["unspecified_assertions"], [])
            if "empty" in case_id:
                self.assertTrue(scored["all_local_assertions_passed"])


if __name__ == "__main__":
    unittest.main()
