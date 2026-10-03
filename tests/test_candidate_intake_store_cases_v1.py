"""Offline controls for prospective B02 definitions and host-only scoring."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
import io
import json
from pathlib import Path
import unittest
import zipfile

from gossip_harness import candidate_intake_store_cases_v1 as cases
from gossip_harness.candidate_intake_store_observer_v1 import Observation, ObservationUnavailable


def observations(case_id: str) -> tuple[dict, list[Observation]]:
    case = cases.case_definition(case_id)
    result = []
    for phase in cases.PHASES:
        state = deepcopy(case["expected"][phase])
        strings = {row["public"]["job_id"]: {
            "manifest": cases.encoded(row["manifest"]).decode(),
            "content_hashes": cases.encoded(row["content_hashes"]).decode(),
            "receipt": None if row["receipt"] is None else cases.encoded(row["receipt"]).decode(),
        } for row in state["jobs"]}
        result.append(Observation(state, strings, (), "a" * 64, "b" * 64, {"control": []}, "test-profile"))
    return case, result


def score(case: dict, snapshots: list[Observation], results: dict | None = None) -> dict:
    return cases.evaluate_case(case["case_id"], *snapshots,
                               deepcopy(case["expected"]["results"]) if results is None else results)


class CandidateIntakeStoreDefinitionsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.rows = cases.definitions()
        cls.index = {row["case_id"]: row for row in cls.rows}

    def test_exact_42_id_target_denominator_and_unique_cases(self) -> None:
        ledger = json.loads(Path("analysis/cumulative-coverage-map-v1.json").read_text())
        batch = next(row for row in ledger["next_gate_batches"] if row["id"].startswith("B02-"))
        self.assertEqual(set(cases.TARGET_IDS), set(batch["target_requirement_ids"]))
        self.assertEqual(len(cases.TARGET_IDS), 42)
        self.assertEqual(len(self.rows), len(self.index))
        self.assertEqual(set(cases.CASE_IDS), set(self.index))
        self.assertEqual({rid for row in self.rows for rid in row["requirement_ids"]}, set(cases.TARGET_IDS))

    def test_direct_three_by_five_by_three_matrix_uses_legal_epochs(self) -> None:
        matrix = [row for row in self.rows if row["family"] == "state-epoch"]
        self.assertEqual(len(matrix), 45)
        for method in ("start_job", "fail_job", "commit_job"):
            for state in cases.STATES:
                for relation, token in (("lower", 1), ("current", 2), ("higher", 3)):
                    row = self.index[f"store-{method}-{state}-{relation}"]
                    job = next(job for job in row["expected"]["before"]["jobs"] if job["public"]["job_id"] == "case")
                    self.assertEqual(job["public"]["state"], state)
                    self.assertEqual(job["public"]["epoch"], 2)
                    self.assertEqual(row["recipe"]["phases"]["after"][0]["args"][1], token)
                    if relation != "current":
                        self.assertEqual(row["expected"]["results"]["after"], [{"error": "stale_epoch"}])

    def test_missing_methods_and_mutable_producers_are_explicit(self) -> None:
        methods = ("get_job", "job_manifest", "start_job", "fail_job", "cancel_job", "retry_job", "commit_job")
        self.assertTrue(all("missing-" + method in self.index for method in methods))
        required = ("create_job", "get_job", "list_jobs", "job_manifest", "start_job", "fail_job", "cancel_job", "retry_job", "commit_job")
        self.assertTrue(all("fresh-value-" + method in self.index for method in required))
        self.assertIn("input-manifest-snapshot", self.index)

    def test_interference_is_declared_at_public_methods(self) -> None:
        rows = [row for row in self.rows if row["family"] == "controlled-interference"]
        self.assertEqual(len(rows), 5)
        for row in rows:
            operation = row["recipe"]["phases"]["after"][0]
            self.assertEqual(operation["op"], "interfere")
            self.assertIn(operation["boundary"], ("start_job", "fail_job"))
            self.assertEqual(row["expected"]["results"]["after"][0]["value"]["boundary_calls"], 1)

    def test_assertions_have_unique_applicable_selectors_and_no_full_claim(self) -> None:
        for row in self.rows:
            self.assertEqual(len(row["requirement_ids"]), len(set(row["requirement_ids"])))
            keys = set()
            for assertion in row["assertions"]:
                self.assertIn(assertion["class"], ("positive", "negative", "boundary", "history"))
                self.assertFalse(assertion["full_requirement"])
                self.assertEqual(assertion["case_id"], row["case_id"])
                value = row["expected"]
                for key in assertion["selector"].strip("/").split("/"):
                    value = value[int(key)] if isinstance(value, list) else value[key]
                identity = (assertion["requirement_id"], assertion["selector"])
                self.assertNotIn(identity, keys)
                keys.add(identity)
            self.assertTrue(row["omissions"])

    def test_definition_cache_is_defensive_and_digest_is_stable(self) -> None:
        original = cases.definition_sha256()
        row = cases.case_definition("replay-completed")
        row["expected"]["after"]["jobs"].clear()
        recipe = cases.execution_recipe("replay-completed")
        recipe["phases"]["after"].clear()
        self.assertTrue(cases.case_definition("replay-completed")["expected"]["after"]["jobs"])
        self.assertTrue(cases.execution_recipe("replay-completed")["phases"]["after"])
        self.assertEqual(cases.definition_sha256(), original)
        self.assertTrue(all(len(digest) == 64 for digest in cases.definition_sources().values()))

    def test_fixture_paths_and_wire_bound_are_finite(self) -> None:
        for row in self.rows:
            paths = [fixture["path"] for fixture in row["recipe"]["fixtures"]]
            self.assertEqual(len(paths), len(set(paths)), row["case_id"])
            self.assertTrue(all(not path.startswith("/") and ".." not in path.split("/") for path in paths))
            # Explicit retained phase output including legal JSON escaping. This
            # cap is the new B02 driver contract, not the frozen B01 observer cap.
            wire_sizes = [len(cases.encoded({"results": row["expected"]["results"][phase]})) for phase in cases.PHASES]
            self.assertLessEqual(sum(wire_sizes), 16 * 1024 * 1024)

    def test_document_and_unencodable_hash_contract(self) -> None:
        doc = cases.document("a.txt", "é\n")
        self.assertEqual(doc["blob_id"], "blob-edd3a863872a04239eb29ad4bc12fc892b3d4ae57cc7e786a3697816f8e141c2")
        row = cases.stored_job("case", [{"source": "a.txt", "text": "\ud800"}])
        self.assertEqual(row["content_hashes"], [None])

    def test_zip_fixture_padding_is_legal_and_preserves_payload(self) -> None:
        raw = cases.zip_bytes([{"name": "a.txt", "raw": b"text"}], pad_to=1048576)
        self.assertEqual(len(raw), 1048576)
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            self.assertEqual(archive.read("a.txt"), b"text")


class CandidateIntakeStoreScorerTests(unittest.TestCase):
    def test_complete_success_is_only_local(self) -> None:
        case, snapshots = observations("replay-completed")
        result = score(case, snapshots)
        self.assertTrue(result["all_local_assertions_passed"])
        self.assertIsNone(result["full_requirement_verdict"])
        self.assertEqual(result["authority"], "unregistered-local-observation-only")

    def test_boolean_integer_substitution_is_not_equal(self) -> None:
        case, snapshots = observations("state-prepare-queued")
        outputs = deepcopy(case["expected"]["results"])
        outputs["after"][0]["value"]["epoch"] = True
        result = score(case, snapshots, outputs)
        self.assertFalse(result["checks"]["after.result.0"])

    def test_candidate_truth_flag_cannot_replace_output(self) -> None:
        case, snapshots = observations("replay-completed")
        outputs = deepcopy(case["expected"]["results"])
        outputs["after"][0] = {"passed": True}
        self.assertFalse(score(case, snapshots, outputs)["checks"]["after.result.0"])

    def test_missing_and_extra_results_fail(self) -> None:
        case, snapshots = observations("replay-completed")
        outputs = deepcopy(case["expected"]["results"])
        outputs["after"].pop()
        self.assertFalse(score(case, snapshots, outputs)["checks"]["after.result-count"])
        outputs = deepcopy(case["expected"]["results"])
        outputs["after"].append({"value": None})
        self.assertFalse(score(case, snapshots, outputs)["checks"]["after.result-count"])

    def test_extra_persisted_job_is_failure_not_scoring_exception(self) -> None:
        case, snapshots = observations("replay-completed")
        extra = cases.stored_job("unexpected", [])
        snapshots[0].data["jobs"].append(extra)
        snapshots[0].persisted_strings["unexpected"] = {"manifest": "[]", "content_hashes": "[]", "receipt": None}
        result = score(case, snapshots)
        self.assertFalse(result["checks"]["before.persisted-state"])
        self.assertFalse(result["checks"]["before.persisted-string-job-set"])

    def test_missing_persisted_strings_is_explicit_failure(self) -> None:
        case, snapshots = observations("replay-completed")
        snapshots[0].persisted_strings.pop("case")
        result = score(case, snapshots)
        self.assertFalse(result["checks"]["before.persisted-string-job-set"])
        self.assertFalse(result["checks"]["after.case.immutable-manifest"])

    def test_missing_job_keeps_other_failures(self) -> None:
        case, snapshots = observations("replay-completed")
        snapshots[0].data["jobs"] = []
        outputs = deepcopy(case["expected"]["results"])
        outputs["after"][0] = {"error": "wrong"}
        result = score(case, snapshots, outputs)
        self.assertFalse(result["checks"]["before.persisted-state"])
        self.assertFalse(result["checks"]["after.result.0"])

    def test_ambiguity_accepts_only_each_complete_declared_branch(self) -> None:
        for case_id in cases.AMBIGUOUS_JSON_CASES:
            for variant in ("immediate", "deferred"):
                case = cases.case_definition(case_id)
                case["expected"] = deepcopy(case["expected_alternatives"][variant])
                snapshots = []
                for phase in cases.PHASES:
                    state = deepcopy(case["expected"][phase])
                    strings = {row["public"]["job_id"]: {"manifest": cases.encoded(row["manifest"]).decode(),
                        "content_hashes": cases.encoded(row["content_hashes"]).decode(),
                        "receipt": None if row["receipt"] is None else cases.encoded(row["receipt"]).decode()}
                        for row in state["jobs"]}
                    snapshots.append(Observation(state, strings, (), "a" * 64, "b" * 64, {"control": []}, "test-profile"))
                result = score(case, snapshots)
                self.assertTrue(result["all_local_assertions_passed"], (case_id, variant, result["checks"]))
                self.assertEqual(result["expected_variant"], variant)

    def test_ambiguity_rejects_error_with_admitted_job_and_success_without_job(self) -> None:
        for case_id in cases.AMBIGUOUS_JSON_CASES:
            case, deferred_snapshots = observations(case_id)
            immediate_outputs = deepcopy(case["expected_alternatives"]["immediate"]["results"])
            result = score(case, deferred_snapshots, immediate_outputs)
            self.assertFalse(result["checks"]["before.persisted-state"], case_id)
            no_job = [replace(snapshot, data=deepcopy(case["expected_alternatives"]["immediate"][phase]))
                      for phase, snapshot in zip(cases.PHASES, deferred_snapshots)]
            result = score(case, no_job)
            self.assertFalse(result["checks"]["before.persisted-state"], case_id)

    def test_ambiguity_rejects_wrong_final_code_and_mutation(self) -> None:
        for case_id in cases.AMBIGUOUS_JSON_CASES:
            case, snapshots = observations(case_id)
            outputs = deepcopy(case["expected"]["results"])
            outputs["after"][0] = {"error": "different_code"}
            snapshots[1].data["documents"][0]["text"] = "changed"
            result = score(case, snapshots, outputs)
            self.assertFalse(result["checks"]["after.result.0"], case_id)
            self.assertFalse(result["checks"]["after.persisted-state"], case_id)

    def test_interposition_unavailable_masks_dependencies_and_keeps_prior_failure(self) -> None:
        case, snapshots = observations("interfere-start_job-cancel")
        snapshots[0].data["documents"][0]["text"] = "already wrong"
        outputs = deepcopy(case["expected"]["results"])
        outputs["after"][0] = {"observation_unavailable": "public_boundary_interposition"}
        result = score(case, snapshots, outputs)
        self.assertFalse(result["checks"]["before.persisted-state"])
        self.assertIsNone(result["checks"]["after.result.0"])
        self.assertIsNone(result["checks"]["after.persisted-state"])
        self.assertIsNone(result["checks"]["reopened.persisted-state"])
        self.assertEqual(result["unavailable_phases"], ["after", "reopened"])
        self.assertFalse(result["all_local_assertions_passed"])

    def test_interposition_unavailable_does_not_invent_failed_expected_transition(self) -> None:
        case, snapshots = observations("interfere-start_job-cancel")
        outputs = deepcopy(case["expected"]["results"])
        outputs["after"][0] = {"value": {"prepare": {"observation_unavailable": "public_boundary_interposition"}}}
        snapshots[1] = deepcopy(snapshots[0])
        snapshots[2] = deepcopy(snapshots[0])
        result = score(case, snapshots, outputs)
        self.assertTrue(result["observation_unavailable"])
        self.assertNotIn(False, result["checks"].values())
        self.assertFalse(result["all_local_assertions_passed"])

    def test_later_unavailability_preserves_prior_raw_receipt_failure(self) -> None:
        # Completed replay fixture has an existing receipt before the action.
        case, snapshots = observations("replay-completed")
        snapshots[1].persisted_strings["case"]["receipt"] += " "
        outputs = deepcopy(case["expected"]["results"])
        outputs["reopened"] = [{"observation_unavailable": "late_reader"}]
        result = score(case, snapshots, outputs)
        self.assertFalse(result["checks"]["after.case.receipt-conservation"])
        self.assertIsNone(result["checks"]["reopened.case.receipt-conservation"])

    def test_nonobject_envelope_is_unavailable_before_branch_selection(self) -> None:
        case, snapshots = observations("intake-json-count-over")
        with self.assertRaises(ObservationUnavailable):
            cases.evaluate_case(case["case_id"], *snapshots, [])

    def test_new_orphan_blob_fails_after_snapshot(self) -> None:
        case, snapshots = observations("missing-get_job")
        snapshots[1].data["blobs"].append({"blob_id": "orphan", "bytes": 1, "sha256": "x" * 64})
        self.assertFalse(score(case, snapshots)["checks"]["after.persisted-state"])

    def test_replay_must_preserve_exact_stored_strings(self) -> None:
        case, snapshots = observations("replay-completed")
        snapshots[1].persisted_strings["case"]["receipt"] += " "
        result = score(case, snapshots)
        self.assertFalse(result["checks"]["after.case.receipt-conservation"])
        self.assertFalse(result["checks"]["reopened.persisted-strings"])

    def test_new_completed_receipt_is_allowed(self) -> None:
        case, snapshots = observations("store-commit_job-running-current")
        self.assertTrue(score(case, snapshots)["all_local_assertions_passed"])

    def test_no_change_auxiliary_mutation_is_detected(self) -> None:
        case, snapshots = observations("missing-get_job")
        snapshots[1].auxiliary_tables["control"] = [{"changed": True}]
        self.assertFalse(score(case, snapshots)["checks"]["after.auxiliary-conservation"])

    def test_registry_and_envelope_mismatch_are_unavailable(self) -> None:
        case, snapshots = observations("missing-get_job")
        snapshots[1] = replace(snapshots[1], registration_sha256="c" * 64)
        with self.assertRaises(ObservationUnavailable):
            score(case, snapshots)
        _, snapshots = observations("missing-get_job")
        with self.assertRaises(ObservationUnavailable):
            score(case, snapshots, {"before": []})

    def test_unprescribed_error_requires_real_error_not_success(self) -> None:
        self.assertTrue(cases._matches_result({"error": "io_error"}, {"error_present": True}))
        self.assertFalse(cases._matches_result({"value": None}, {"error_present": True}))
        self.assertFalse(cases._matches_result({"unexpected_exception": "TypeError"}, {"error_present": True}))


if __name__ == "__main__":
    unittest.main()
