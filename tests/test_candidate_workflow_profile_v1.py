"""Closed evaluator definition controls; no candidate, Engine or provider execution."""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
import unittest
from unittest.mock import patch

from gossip_harness import candidate_workflow_profile_v1 as profile
from gossip_harness import library_project_fixture_v1 as original


class CandidateWorkflowDefinitionTests(unittest.TestCase):
    """Frozen admission and independently authored prospective expectation census."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.cases = profile.definitions()
        cls.by_id = {row["case_id"]: row for row in cls.cases}

    def test_complete_roster_and_original_eight_are_exact(self) -> None:
        self.assertEqual(tuple(row["case_id"] for row in self.cases), profile.CASE_IDS)
        self.assertEqual(len(self.cases), 20)
        self.assertEqual(sum(len(row["calls"]) for row in self.cases), 23)
        self.assertEqual(sum(len(call["input"]["operations"]) for row in self.cases for call in row["calls"]), 212)
        self.assertEqual(tuple(tuple(len(call["input"]["operations"]) for call in row["calls"])
                               for row in self.cases), profile.OPERATION_COUNTS)
        for expected, case in zip(original.public_cases("m1"), self.cases[:8], strict=True):
            with self.subTest(case=case["case_id"]):
                self.assertEqual(case["case_id"], expected["id"])
                self.assertTrue(profile.exact(case["calls"][0]["input"], expected["input"]))
                self.assertTrue(profile.exact(case["calls"][0]["expected"], expected["expected"]))
                self.assertEqual(case["original_definition_purpose"], "public_development")

    def test_all_calls_admitted_and_only_authored_token_coordinates_change(self) -> None:
        allowed = {
            "WF15-existing-job-token-domain": {3, 8},
            "WF16-absent-token-and-hook-distinction": {2, 3, 4, 5, 6, 7, 8, 10, 12, 14},
            "WF17-normalized-answer-growth": {1},
        }
        for case in self.cases:
            for call in case["calls"]:
                with self.subTest(case=case["case_id"], call=call["call_id"]):
                    admission = call["historical_admission"]
                    self.assertIs(admission["admitted"], True)
                    self.assertEqual(admission["guard_source_sha256"], profile.ORIGINAL_SOURCE_SHA256)
                    before, after = deepcopy(admission["historical_expected"]), call["expected"]
                    self.assertLessEqual(len(profile.normalized(call["input"])), 61440)
                    self.assertLessEqual(len(profile.normalized(before)), 61440)
                    self.assertLessEqual(len(profile.normalized(after)), 61824)
                    observed = {i for i, (old, new) in enumerate(zip(before["results"], after["results"], strict=True))
                                if not profile.exact(old, new)}
                    self.assertEqual(observed, allowed.get(case["case_id"], set()))
                    for index in observed:
                        self.assertIn(before["results"][index], [{"error": "stale_epoch"}, {"error": "not_found"}])
                        self.assertEqual(after["results"][index], {"error": "invalid_request"})
                        before["results"][index] = deepcopy(after["results"][index])
                    self.assertTrue(profile.exact(before, after))

    def test_all_eighteen_admission_control_families(self) -> None:
        expected_ids = (
            "exact64ops", "over65ops", "input61440", "input61441", "historical-answer61440",
            "historical-answer61441", "unknown-operation", "missing-operation-field", "extra-operation-field",
            "present-epoch0", "present-epoch-minus1", "present-epoch-bool", "present-epoch-float",
            "present-epoch-string", "present-epoch-null", "present-malformed-fault-flag",
            "invalid-milestone", "malformed-entries-shape",
        )
        controls = profile.admission_controls()
        self.assertEqual(tuple(row["control_id"] for row in controls), expected_ids)
        for row in controls:
            with self.subTest(control=row["control_id"]):
                self.assertIs(profile.historical_admission(row["input"])["admitted"], row["expected_admitted"])
                self.assertEqual(row["purpose"], "harness_qualification")
                self.assertIs(row["product_history"], False)
        self.assertEqual(len(profile.normalized(controls[2]["input"])), 61440)
        self.assertEqual(len(profile.normalized(controls[3]["input"])), 61441)
        self.assertEqual(profile.historical_admission(controls[4]["input"])["historical_answer_bytes"], 61440)
        # Independently construct the rejected model-size witness without executing a modified oracle.
        call = self.by_id["WF17-normalized-answer-growth"]["calls"][0]
        modeled = deepcopy(call["historical_admission"]["historical_expected"])
        for doc in (modeled["results"][0]["document"], modeled["documents"][0]):
            doc["text"] = "x" * 30361
            doc["blob_id"] = "blob-" + hashlib.sha256(doc["text"].encode()).hexdigest()
        modeled["results"][1] = {"error": "invalid_source"}
        self.assertEqual(len(profile.normalized(modeled)), 61441)
        self.assertEqual(profile.historical_admission(controls[5]["input"])["reason"], "Workflow output too large")

    def test_defaults_exports_and_v0_unsupported_continuation(self) -> None:
        wf10 = self.by_id["WF10-defaults-and-export-selection"]["calls"][0]
        results = wf10["expected"]["results"]
        self.assertNotIn("milestone", wf10["input"])
        self.assertNotIn("jobs", wf10["expected"])
        self.assertEqual(results[2], results[3])
        self.assertEqual(len(results[4]["documents"]), 2)
        self.assertEqual(results[5]["documents"], [])
        self.assertEqual(results[7]["total"], 2)
        self.assertEqual(results[7]["documents"], results[8]["documents"])
        wf11 = self.by_id["WF11-v0-unsupported-m1-continuation"]["calls"][0]
        self.assertEqual([op["op"] for op in wf11["input"]["operations"]],
                         ["submit", "prepare", "commit", "cancel", "retry", "job", "reopen", "import", "list"])
        self.assertEqual(wf11["expected"]["results"][:6], [{"error": "unsupported_operation"}] * 6)
        self.assertEqual(wf11["expected"]["results"][6], {"reopened": True})
        self.assertEqual(wf11["expected"]["results"][8]["total"], 1)

    def test_mixed_states_have_sorted_fresh_snapshots_and_epoch_fence(self) -> None:
        expected = self.by_id["WF12-mixed-job-state-order-and-reopen"]["calls"][0]["expected"]
        self.assertEqual([job["job_id"] for job in expected["jobs"]],
                         ["c-completed", "f-failed", "r-running", "x-cancelled", "z-queued"])
        self.assertEqual([job["state"] for job in expected["jobs"]],
                         ["completed", "failed", "running", "cancelled", "queued"])
        self.assertEqual(expected["jobs"][3]["epoch"], 4)
        self.assertEqual(expected["results"][8]["state"], "queued")
        self.assertEqual(expected["results"][10]["epoch"], 2)
        self.assertEqual(expected["results"][17]["epoch"], 3)
        self.assertEqual(expected["results"][19], {"error": "stale_epoch"})
        self.assertEqual(expected["results"][4], {"error": "invalid_source"})
        self.assertEqual(expected["results"][14]["error"], "invalid_source")
        self.assertEqual([doc["source"] for doc in expected["results"][7]["documents"]], ["y.txt", "z.txt"])
        self.assertEqual(expected["documents"], expected["results"][22]["documents"])

    def test_existing_absent_epoch_and_hook_precedence_remain_distinct(self) -> None:
        absent = self.by_id["WF16-absent-token-and-hook-distinction"]["calls"][0]
        expected = absent["expected"]["results"]
        self.assertEqual(expected[:2], [{"error": "not_found"}] * 2)
        self.assertEqual(expected[2:9], [{"error": "invalid_request"}] * 7)
        self.assertEqual(expected[9:15], [{"error": "not_found"}, {"error": "invalid_request"}] * 3)
        self.assertTrue(all(row == {"error": "not_found"}
                            for row in absent["historical_admission"]["historical_expected"]["results"][:15]))
        present = self.by_id["WF15-existing-job-token-domain"]["calls"][0]["expected"]
        self.assertEqual(present["results"][1], {"error": "job_state"})
        self.assertEqual(present["results"][2], {"error": "stale_epoch"})
        self.assertEqual(present["results"][5], present["results"][6])
        self.assertEqual(present["results"][8], {"error": "invalid_request"})
        self.assertEqual(present["jobs"], [present["results"][9]])

    def test_growth_witness_and_mechanism_qualification_are_separate(self) -> None:
        call = self.by_id["WF17-normalized-answer-growth"]["calls"][0]
        self.assertEqual(call["historical_admission"]["historical_answer_bytes"], 61440)
        self.assertEqual(call["normalized_expected_bytes"], 61446)
        self.assertLess(call["normalized_expected_bytes"], profile.LIMITS["normalized_v2_answer_bytes"])
        controls = profile.mechanism_controls()
        self.assertEqual(len(controls), 12)
        self.assertEqual(tuple(row["control_id"] for row in controls), profile.MECHANISM_CONTROL_IDS)
        self.assertTrue(all(row["purpose"] == "harness_qualification" and row["product_history"] is False for row in controls))
        self.assertEqual(profile.LIMITS, {"operations_per_call": 64, "original_input_bytes": 61440,
            "original_answer_bytes": 61440, "normalized_v2_answer_bytes": 61824,
            "raw_frame_payload_bytes": 131072, "history_stdout_bytes": 524288,
            "history_stderr_bytes": 65536, "call_timeout_seconds": 30,
            "history_timeout_seconds": 300, "calls_per_history": 3})

    def test_same_process_inputs_and_exact_fault_repeat(self) -> None:
        calls = self.by_id["WF18-same-process-call-isolation"]["calls"]
        self.assertEqual([call["call_id"] for call in calls], ["call-000", "call-001", "call-002"])
        self.assertEqual(calls[1]["expected"]["results"][:2],
                         [{"error": "not_found"}, {"documents": [], "total": 0}])
        self.assertEqual(calls[2]["expected"], {"results": [], "documents": [], "jobs": []})
        self.assertNotEqual(calls[0]["expected"]["documents"], calls[1]["expected"]["documents"])
        fault = self.by_id["WF19-provisional-fault-boundary"]["calls"][0]
        old = self.by_id["m1-injected-transaction-failure"]["calls"][0]
        self.assertTrue(profile.exact(fault, old))
        self.assertEqual(fault["expected"]["results"][3], {"error": "injected_failure"})
        self.assertEqual(fault["expected"]["results"][6]["state"], "running")

    def test_child_recipe_is_inputs_only_and_definitions_are_fresh(self) -> None:
        for case in self.cases:
            recipe = profile.recipe_for(case["case_id"])
            self.assertEqual(set(recipe), {"protocol", "case_id", "calls"})
            self.assertEqual(recipe["calls"], [call["input"] for call in case["calls"]])
            self.assertEqual(set(profile.input_files(case["case_id"])), {"workflow-input.json"})
            self.assertEqual(json.loads(profile.input_files(case["case_id"])["workflow-input.json"]), recipe)
        changed = profile.case_definition("WF09-empty-modes")
        changed["calls"][0]["expected"]["documents"].append({"forged": True})
        self.assertEqual(profile.case_definition("WF09-empty-modes")["calls"][0]["expected"]["documents"], [])

    def test_profile_type_purpose_source_and_selector_precision(self) -> None:
        value = profile.profile_for("WF15-existing-job-token-domain", "independent_acceptance")
        self.assertEqual(profile.reconstruct(value), value)
        self.assertEqual(value.family, "workflow-final-m4-v1")
        self.assertEqual(set(value.requirement_ids), {"V0-ADAPTER-01", "V0-ADAPTER-02", "M1-ADAPTER-03"})
        self.assertEqual(value.original_definition_purpose, "public_contract_regression")
        self.assertEqual(value.phases, ("call-000",))
        self.assertEqual(len(value.ordered_case_ids), 13)
        self.assertEqual(len(set(value.ordered_case_ids)), 13)
        for selector in value.selectors():
            self.assertFalse(selector["semantically_reviewed"])
            self.assertEqual(selector["comparison"], "exact_typed_json")
            self.assertEqual(selector["assertion_kind"], "history")
            self.assertEqual(selector["logical_gate_ids"], ["M1-GATE-ADAPTER"])
            self.assertTrue(selector["definition_pointer"].startswith("/calls/0/expected"))
            self.assertNotIn("CLARIFY-INPUT-DOMAIN", selector["source_unit_ids"])
            self.assertNotIn("M4-COMPATIBILITY:clause:2", selector["source_unit_ids"])
        for bad in (True, "bad", "harness_qualification", None):
            with self.subTest(purpose=bad), self.assertRaises(ValueError):
                profile.profile_for(value.case_id, bad)
        with self.assertRaises(ValueError):
            profile.profile_for("unknown")
        class Lookalike(profile.WorkflowProfile):
            pass
        with self.assertRaises(ValueError):
            profile.reconstruct(Lookalike(value.case_id, value.purpose))
        with patch.object(profile, "ORIGINAL_SOURCE_SHA256", "0" * 64), self.assertRaises(ValueError):
            profile.case_definition(value.case_id)

    def test_records_preserve_public_views_for_every_history_and_purpose(self) -> None:
        for purpose in profile.registry.PURPOSES:
            for case_id in profile.CASE_IDS:
                with self.subTest(case=case_id, purpose=purpose):
                    value = profile.profile_for(case_id, purpose)
                    record = value.record()
                    self.assertTrue(profile.exact(record["definition"], profile.case_definition(case_id)))
                    self.assertTrue(profile.exact(record["selectors"], value.selectors()))
                    self.assertEqual(record["ordered_calls"], list(value.phases))
                    self.assertEqual(record["ordered_case_ids"], list(value.ordered_case_ids))
                    self.assertEqual(record["requirement_ids"], list(value.requirement_ids))
                    self.assertEqual(record["input_recipe_sha256"], profile.digest(profile.recipe_for(case_id)))
                    self.assertEqual(record["purpose"], purpose)

    def test_record_mutation_cannot_leak_and_source_revocation_is_fresh(self) -> None:
        value = profile.profile_for("WF18-same-process-call-isolation")
        first = value.record()
        expected = deepcopy(first)
        first["definition"]["calls"][0]["input"]["operations"].clear()
        first["definition"]["calls"][0]["expected"].clear()
        first["selectors"][0]["source_unit_ids"].append("forged")
        first["ordered_case_ids"].clear()
        first["requirement_ids"].clear()
        first["limits"]["call_timeout_seconds"] = 999
        self.assertTrue(profile.exact(value.record(), expected))
        with patch.object(profile, "ORIGINAL_SOURCE_SHA256", "0" * 64), self.assertRaises(ValueError):
            value.record()
        self.assertTrue(profile.exact(value.record(), expected))

    def test_exact_types_complete_bad_call_survives_missing_tail(self) -> None:
        value = profile.profile_for("WF18-same-process-call-isolation")
        first = profile.expected_for(value.case_id, 0)
        first["results"][0]["epoch"] = True
        projection = profile.project(value, {0: first}, {0: {"candidate_marker": "all-pass"}})
        self.assertEqual(projection["known_failed"], [value.case_id + ":call-000:result-000"])
        self.assertTrue(any(":call-001:" in key for key in projection["unavailable"]))
        self.assertTrue(any(":call-002:" in key for key in projection["unavailable"]))
        self.assertIs(projection["diagnostics"]["capture_facts_grant_semantic_or_lifecycle_credit"], False)
        all_good = profile.project(value, {i: profile.expected_for(value.case_id, i) for i in range(3)})
        self.assertEqual(all_good["known_failed"], [])
        self.assertEqual(all_good["unavailable"], [])
        self.assertFalse(profile.exact({"epoch": 1}, {"epoch": 1.0}))

    def test_complete_malformed_shape_does_not_fabricate_operation_values(self) -> None:
        value = profile.profile_for("WF14-exact64-operations")
        bad = profile.project(value, {0: {"results": [], "documents": [], "jobs": []}})
        self.assertEqual(bad["known_failed"], [value.case_id + ":call-000:shape"])
        self.assertEqual(len(bad["unavailable"]), 64)
        extra = profile.expected_for(value.case_id, 0)
        extra["forged"] = "pass"
        self.assertIn(value.case_id + ":call-000:shape", profile.project(value, {0: extra})["known_failed"])
        with self.assertRaises(ValueError):
            profile.project(value, {True: {}})
        with self.assertRaises(ValueError):
            profile.project(value, {1: {}})


    @staticmethod
    def _wf19_snapshots() -> dict[int, dict]:
        # Trusted mapper-shaped pure fixtures, not claimed physical captures.
        snapshots = {}
        for role, expected in profile.capture_expectations(profile.CAPTURE_CASE_ID).items():
            strings = deepcopy(expected["persisted_fields"])
            for job in expected["data"]["jobs"]:
                strings[job["public"]["job_id"]]["manifest"] = json.dumps(job["manifest"], indent=2)
                strings[job["public"]["job_id"]]["receipt"] = (
                    None if job["receipt"] is None else json.dumps(job["receipt"], indent=2))
            snapshots[role] = {"data": deepcopy(expected["data"]), "persisted_strings": strings,
                               "auxiliary_tables": {"unscored": [{"diagnostic": True}]}}
        return {0: snapshots}

    def test_wf19_capture_expectations_are_independently_derived_and_closed(self) -> None:
        expected = profile.capture_expectations(profile.CAPTURE_CASE_ID)
        self.assertEqual(tuple(expected), ("initial", "post_fault", "reopened", "final"))
        self.assertEqual(expected["initial"], {"data": {"blobs": [], "documents": [], "jobs": []},
                                               "persisted_fields": {}})
        keep_digest = hashlib.sha256(b"keep").hexdigest()
        new_digest = hashlib.sha256(b"new").hexdigest()
        post = expected["post_fault"]
        self.assertEqual(post["data"]["blobs"], [{"blob_id": "blob-" + keep_digest,
                                                "bytes": 4, "sha256": keep_digest}])
        self.assertEqual([row["source"] for row in post["data"]["documents"]], ["keep.txt"])
        self.assertEqual(post["data"]["jobs"], [{
            "public": {"job_id": "fault", "epoch": 1, "state": "running", "total": 1,
                       "completed": 0, "error": None},
            "manifest": [{"source": "new.txt", "text": "new"}],
            "content_hashes": [new_digest], "receipt": None}])
        self.assertEqual(post["persisted_fields"], {"fault": {
            "content_hashes": '["' + new_digest + '"]'}})
        self.assertEqual(post, expected["reopened"])
        self.assertIsNot(post, expected["reopened"])
        final = expected["final"]["data"]
        self.assertEqual([doc["source"] for doc in final["documents"]], ["keep.txt", "new.txt"])
        self.assertEqual({blob["sha256"] for blob in final["blobs"]}, {keep_digest, new_digest})
        self.assertEqual(final["jobs"][0]["public"]["state"], "completed")
        self.assertEqual(final["jobs"][0]["public"]["completed"], 1)
        self.assertEqual(final["jobs"][0]["receipt"]["job"], final["jobs"][0]["public"])
        self.assertEqual([doc["source"] for doc in final["jobs"][0]["receipt"]["documents"]], ["new.txt"])
        self.assertEqual(profile.capture_expectations("m1-injected-transaction-failure"), {})
        self.assertEqual(profile.capture_selectors("WF18-same-process-call-isolation"), ())
        selectors = profile.capture_selectors(profile.CAPTURE_CASE_ID)
        self.assertEqual(len(selectors), 16)
        self.assertEqual(len(profile.profile_for(profile.CAPTURE_CASE_ID).ordered_case_ids), 27)
        self.assertEqual(len({row["case_id"] for row in selectors}), 16)
        self.assertTrue(all(row["evidence_kind"] == "captured_sqlite" for row in selectors))
        self.assertTrue(all(row["logical_gate_ids"] == ["M1-GATE-ADAPTER"] for row in selectors))
        self.assertEqual(self.by_id[profile.CAPTURE_CASE_ID]["capture_expectations"], expected)

    def test_wf19_orphan_is_failed_before_later_success_and_survives_missing_tail(self) -> None:
        value = profile.profile_for(profile.CAPTURE_CASE_ID)
        captures = self._wf19_snapshots()
        responses = {0: profile.expected_for(value.case_id, 0)}
        positive = profile.project(value, responses, captures)
        self.assertEqual(positive["known_failed"], [])
        self.assertEqual(positive["unavailable"], [])
        # A premature new blob is absorbed by the later successful commit:
        # final-only comparison would miss this rollback defect.
        new_blob = next(blob for blob in captures[0]["final"]["data"]["blobs"]
                        if blob["sha256"] == hashlib.sha256(b"new").hexdigest())
        for role in ("post_fault", "reopened"):
            captures[0][role]["data"]["blobs"].append(deepcopy(new_blob))
            captures[0][role]["data"]["blobs"].sort(key=lambda row: row["blob_id"])
        expected_failures = [value.case_id + ":call-000:capture:" + role + ":blobs"
                             for role in ("post_fault", "reopened")]
        self.assertEqual(profile.project(value, responses, captures)["known_failed"], expected_failures)
        del captures[0]["final"]
        missing = profile.project(value, {}, captures)
        self.assertEqual(missing["known_failed"], expected_failures)
        self.assertEqual(sum(":capture:final:" in key for key in missing["unavailable"]), 4)
        self.assertIn(value.case_id + ":call-000:shape", missing["unavailable"])

    def test_wf19_job_document_receipt_and_stored_hash_mutations_are_independent(self) -> None:
        value = profile.profile_for(profile.CAPTURE_CASE_ID)
        mutations = (
            ("documents", lambda state: state["data"]["documents"].clear()),
            ("jobs", lambda state: state["data"]["jobs"][0]["public"].update(epoch=True)),
            ("jobs", lambda state: state["data"]["jobs"][0].update(receipt={"forged": True})),
            ("persisted_fields", lambda state: state["persisted_strings"]["fault"].update(content_hashes='[]')),
            ("jobs", lambda state: state["data"]["jobs"][0].update(manifest=[])),
        )
        for component, mutate in mutations:
            with self.subTest(component=component):
                captures = self._wf19_snapshots()
                mutate(captures[0]["post_fault"])
                failed = profile.project(value, {}, captures)["known_failed"]
                self.assertEqual(failed, [value.case_id + ":call-000:capture:post_fault:" + component])
        # Manifest/receipt spelling is deliberately not fixed; parsed original receipt
        # semantics are already compared through the normalized job graph.
        captures = self._wf19_snapshots()
        captures[0]["post_fault"]["persisted_strings"]["fault"]["manifest"] = ' [ { "text" : "new", "source" : "new.txt" } ] '
        captures[0]["final"]["persisted_strings"]["fault"]["receipt"] = json.dumps(
            captures[0]["final"]["data"]["jobs"][0]["receipt"], separators=(",", ":"))
        self.assertEqual(profile.project(value, {}, captures)["known_failed"], [])

    def test_wf19_missing_roles_and_candidate_markers_never_grant_capture_pass(self) -> None:
        value = profile.profile_for(profile.CAPTURE_CASE_ID)
        responses = {0: profile.expected_for(value.case_id, 0)}
        for evidence in (None, {}, {0: []}, {0: {role: {"candidate_marker": "passed"}
                                                   for role in profile.CAPTURE_ROLES}}):
            with self.subTest(evidence=evidence):
                projected = profile.project(value, responses, evidence)
                self.assertEqual(projected["known_failed"], [])
                self.assertEqual(len(projected["unavailable"]), 16)
        captures = self._wf19_snapshots()
        del captures[0]["post_fault"]
        missing = profile.project(value, responses, captures)
        self.assertEqual(len(missing["unavailable"]), 4)
        self.assertTrue(all(":capture:post_fault:" in key for key in missing["unavailable"]))
        legacy = profile.profile_for("m1-injected-transaction-failure")
        unscored = profile.project(legacy, {0: profile.expected_for(legacy.case_id, 0)}, captures)
        self.assertEqual(unscored["known_failed"], [])
        self.assertEqual(unscored["unavailable"], [])
        self.assertEqual(unscored["diagnostics"]["scored_capture_roles"], [])


    def test_m4_compatibility_maps_only_seven_actual_unchanged_results(self) -> None:
        expected = {
            "inherited-text-persistence": {0, 2, 4},
            "m1-cancel-fences-old-token": {0, 1, 7, 8},
        }
        total = 0
        for case_id in profile.CASE_IDS:
            value = profile.profile_for(case_id)
            mapped = [row for row in value.selectors()
                      if profile.M4_COMPATIBILITY_UNIT in row["source_unit_ids"]]
            self.assertEqual({row["operation_index"] for row in mapped}, expected.get(case_id, set()))
            self.assertEqual("M4-COMPATIBILITY" in value.requirement_ids, case_id in expected)
            self.assertNotIn("M4-COMPATIBILITY:clause:2", value.requirement_ids)
            total += len(mapped)
            for row in mapped:
                self.assertEqual(row["kind"], "result")
                self.assertEqual(row["call_index"], 0)
                self.assertEqual(row["evidence_kind"], "complete_workflow_call")
                facet = next(facet for facet in row["source_unit_facets"]
                             if facet["source_unit_id"] == profile.M4_COMPATIBILITY_UNIT)
                self.assertEqual(facet["logical_gate_ids"], ["M4-COMPATIBILITY.public-contract"])
                self.assertEqual(facet["source_reference"]["json_pointer"], "/requirements/15/clauses/2")
                self.assertEqual(facet["source_reference"]["value_sha256"], profile.M4_COMPATIBILITY_CLAUSE_SHA256)
                self.assertIs(row["semantically_reviewed"], False)
        self.assertEqual(total, 7)


    def test_selector_lanes_match_actual_catalog_gate_lanes(self) -> None:
        catalog_path = profile.ROOT / "analysis/cumulative-coverage-map-v1.json"
        catalog_raw = catalog_path.read_bytes()
        self.assertEqual(hashlib.sha256(catalog_raw).hexdigest(),
                         "421635d4282a218a267059d161f3245daa8e0d24018b5552d7c594f18f43823c")
        table = json.loads(catalog_raw)["logical_gate_table"]
        actual = {row[0]: row[2] for row in table["rows"]}
        self.assertEqual({key: actual[key] for key in
                          ("M1-GATE-PUBLIC", "M1-GATE-ADAPTER", "M4-COMPATIBILITY.public-contract")},
                         {"M1-GATE-PUBLIC": "public-contract", "M1-GATE-ADAPTER": "workflow",
                          "M4-COMPATIBILITY.public-contract": "public-contract"})
        selector_count = 0
        capture_count = 0
        for case_id in profile.CASE_IDS:
            value = profile.profile_for(case_id)
            for row in value.selectors():
                with self.subTest(case=case_id, selector=row["case_id"]):
                    expected_lanes = list(dict.fromkeys(actual[gate] for gate in row["logical_gate_ids"]))
                    self.assertEqual(row["lanes"], expected_lanes)
                    facet_gates = set(gate for facet in row["source_unit_facets"]
                                      for gate in facet["logical_gate_ids"])
                    self.assertEqual(set(row["logical_gate_ids"]), facet_gates)
                    self.assertEqual(row["lanes"], ["public-contract", "workflow"]
                                     if case_id in profile.ORIGINAL_CASE_IDS else ["workflow"])
                    if row["kind"] == "capture":
                        capture_count += 1
                        self.assertEqual(row["logical_gate_ids"], ["M1-GATE-ADAPTER"])
                    selector_count += 1
            self.assertEqual(value.original_definition_purpose, "public_development"
                             if case_id in profile.ORIGINAL_CASE_IDS else "public_contract_regression")
        self.assertEqual(selector_count, 294)
        self.assertEqual(capture_count, 16)


if __name__ == "__main__":
    unittest.main()
