"""Controller-policy regressions with deterministic source/evidence callbacks.

These tests exercise the real stage engine, not a detached routing model. They
never execute candidate Python, call a provider, or relax receipt validation.
"""
from copy import deepcopy
import json
import unittest
from unittest.mock import patch

from gossip_harness import benchmark_stage as historical
from gossip_harness.continuation_controller import (
    PROTOCOL, repair_comparison, run_continuation_stage,
)
from tests.test_benchmark_stage import Callbacks, builder, code, digest, probe, project, review


class ContinuationCallbacks(Callbacks):
    def run(self, policy="strong-anchor", prior=None, initial=None, seed=17, **options):
        return run_continuation_stage(self.project, self.stage, policy,
            initial or self.project["initial_files"], prior or {},
            invoke=self.invoke, evaluate=self.evaluate, retain=self.retain,
            validate_probes=self.validate_probes, emit=self.emit, after_initial=self.after_initial,
            candidate_seed=seed, **options)


def envelope(result, initial=None, seed=17):
    return dict(pool=deepcopy(result["initial_pool"]), pool_sha256=result["initial_pool_sha256"],
        initial_files_sha256=digest(initial or project()["initial_files"]), candidate_seed=seed,
        purpose="shared_initial_setup")


def normalize_protocol(value):
    if isinstance(value, str):
        return value.replace(PROTOCOL, historical.PROTOCOL)
    if isinstance(value, list):
        return [normalize_protocol(item) for item in value]
    if isinstance(value, dict):
        return {key: normalize_protocol(item) for key, item in value.items()}
    return value


class ContinuationControllerTests(unittest.TestCase):
    def test_inspection_of_executed_failure_repairs_immediately_then_requires_acceptance(self):
        def answer(r):
            if r["role"] == "builder":
                return builder(code() if r["metadata"]["phase"] == "repair" else {})
            return review(r, action="inspect") if r["metadata"]["round"] == 1 else review(r, slot="repair-1")
        cb = ContinuationCallbacks(answer)
        result = cb.run()
        self.assertTrue(result["completed"])
        self.assertEqual(result["metrics"]["repairs"], 1)
        self.assertEqual(result["metrics"]["reviewer_calls"], 2)
        self.assertEqual(len(result["initial_pool"]["candidates"]), 1)
        self.assertEqual(len(result["candidates"]), 2)
        self.assertTrue(result["repair_comparisons"][0]["failure_progress"])
        self.assertEqual(result["repair_comparisons"][0]["resolved_failures"], ["public-0"])
        self.assertEqual([r["role"] for r in cb.requests], ["builder", "reviewer", "builder", "reviewer"])

    def test_routing_passing_alternative_never_autoaccepts(self):
        def answer(r):
            if r["role"] == "builder":
                return builder({} if r["metadata"]["candidate_id"] == "slot-0" else code())
            if r["metadata"]["round"] == 1:
                return review(r, action="inspect", slot="slot-0")
            ctx = json.loads(r["files"][historical.CONTEXT_PATH])
            self.assertEqual(ctx["continuation"]["required_next_action"]["action"], "review_alternative")
            return review(r, slot="slot-1")
        result = ContinuationCallbacks(answer).run("independent-four")
        self.assertTrue(result["completed"])
        self.assertEqual(result["metrics"]["repairs"], 0)
        self.assertEqual(result["metrics"]["reviewer_calls"], 2)
        self.assertEqual(result["metrics"]["alternative_routes"], 1)
        self.assertFalse([r for r in result["trajectory"] if r["kind"] == "reviewer"][0]["accepted"])

    def test_unchanged_alternative_route_is_not_repeated_or_reset_by_duplicate_aliases(self):
        def answer(r):
            if r["role"] == "builder":
                return builder({} if r["metadata"]["candidate_id"] == "slot-0" else code())
            return review(r, action="inspect", slot="slot-0")
        result = ContinuationCallbacks(answer).run("independent-four")
        routes = [r for r in result["trajectory"] if r["kind"] == "routing"]
        self.assertEqual([r["action"] for r in routes], ["review_alternative", "review_repair", "review_repair", "stop"])
        self.assertFalse(result["completed"])
        self.assertEqual(result["metrics"]["repairs"], 2)
        self.assertEqual(result["metrics"]["alternative_routes"], 1)

    def test_probe_executes_before_alternative_routing_and_invalidates_old_pass(self):
        def answer(r):
            if r["role"] == "builder":
                return builder(code(probe=2) if r["metadata"]["phase"] == "repair" else
                               {"public0": 0, "probe": 2} if r["metadata"]["candidate_id"] == "slot-0" else code())
            if r["metadata"]["round"] == 1:
                return review(r, action="accept", probes=[probe()])
            return review(r, slot="repair-1")
        result = ContinuationCallbacks(answer).run("independent-four")
        self.assertTrue(result["completed"])
        self.assertEqual(result["metrics"]["alternative_routes"], 0)
        self.assertEqual(result["metrics"]["blocked_acceptances"], 1)
        self.assertEqual(result["metrics"]["repairs"], 1)

    def test_alternative_with_fewer_but_new_failures_is_not_an_improvement(self):
        def answer(r):
            if r["role"] == "builder":
                if r["metadata"]["phase"] == "repair":
                    return builder(code(probe=2))
                return builder({"public0": 0, "public1": 0, "probe": 2}) if r["metadata"]["candidate_id"] == "slot-0" else builder(code())
            return review(r, action="inspect", probes=[probe()]) if r["metadata"]["round"] == 1 else review(r, slot="repair-1")
        result = ContinuationCallbacks(answer, stage=1).run("independent-four")
        self.assertTrue(result["completed"])
        self.assertEqual(result["metrics"]["alternative_routes"], 0)
        self.assertEqual(result["metrics"]["repairs"], 1)
        route = next(row for row in result["trajectory"] if row["kind"] == "routing")
        self.assertEqual(route["executed_failure_ids"], ["public-0", "public-1"])
        self.assertEqual(route["action"], "review_repair")

    def test_out_of_scope_repair_preserves_predecessor_eligibility_and_cannot_accept_tampering(self):
        def answer(r):
            if r["role"] == "builder":
                return builder(code(), extra={"README.md": "tampered"}) if r["metadata"]["phase"] == "repair" else builder({"public0": 0})
            return review(r, action="inspect") if r["metadata"]["round"] == 1 else review(r, slot="repair-1")
        result = ContinuationCallbacks(answer).run(limits={"max_repairs": 1})
        original = result["initial_pool"]["candidates"][result["alias_map"]["slot-0"]]
        repaired = result["candidates"]["candidate-E"]
        self.assertEqual(repaired["eligibility_origin"], original["eligibility_origin"])
        self.assertEqual(repaired["binding"]["files_sha256"], original["binding"]["files_sha256"])
        self.assertEqual(repaired["latest_attempt_status"], "rejected_source_proposal")
        self.assertTrue(repaired["source_valid"])
        self.assertEqual(result["files"]["README.md"], "trusted")
        self.assertEqual(result["files"]["engine.py"], original["files"]["engine.py"])
        self.assertEqual(result["metrics"]["blocked_acceptances"], 1)
        self.assertFalse(result["repair_comparisons"][0]["failure_progress"])
        self.assertFalse(result["completed"])

    def test_regression_is_recorded_even_when_total_failures_decrease(self):
        cases = [dict(id=f"case-{i}", input=i, expected=i, requirement="R0") for i in range(3)]
        before = {"case-0": {"passed": False}, "case-1": {"passed": False}, "case-2": {"passed": True}}
        after = {"case-0": {"passed": True}, "case-1": {"passed": True}, "case-2": {"passed": False}}
        row = repair_comparison(before, after, cases)
        self.assertEqual(row["resolved_failures"], ["case-0", "case-1"])
        self.assertEqual(row["introduced_failures"], ["case-2"])
        self.assertTrue(row["regression"])
        self.assertFalse(row["failure_progress"])

    def test_checkpoint_and_eligibility_survive_regressive_repair_and_later_probe(self):
        def answer(r):
            if r["role"] == "builder":
                return builder({"public0": 0, "probe": 2, "later": 2}) if r["metadata"]["phase"] == "repair" else builder(code(later=2))
            return review(r, action="inspect", probes=[probe()] if r["metadata"]["round"] == 1 else
                          [probe("later")] if r["metadata"]["round"] == 2 else [],
                          slot="slot-0")
        cb = ContinuationCallbacks(answer)
        result = cb.run(limits={"max_repairs": 1})
        before = result["initial_pool"]["candidates"][result["alias_map"]["slot-0"]]
        retained = result["candidates"][before["alias"]]
        self.assertEqual(before["binding"], retained["binding"])
        self.assertEqual(before["eligibility_origin"], retained["eligibility_origin"])
        comparison = result["repair_comparisons"][0]
        self.assertTrue(comparison["regression"])
        self.assertFalse(comparison["failure_progress"])
        self.assertEqual(comparison["introduced_failures"], ["public-0"])
        self.assertEqual(len(result["matrix"]["candidate-E"]), 3)
        self.assertTrue(any("candidate-E-new-probes" in label for _, _, label in cb.evaluations))
        self.assertEqual(result["selected"], before["alias"])
        self.assertFalse(result["completed"])

    def test_rejected_and_unchanged_repairs_cannot_reset_failure_progress(self):
        for repair_result in (None, builder({"public0": 0})):
            def answer(r):
                if r["role"] == "builder":
                    return builder({"public0": 0}) if r["metadata"]["phase"] == "initial" else repair_result
                return review(r, action="inspect", slot="repair-1" if r["metadata"]["round"] > 1 else "slot-0")
            result = ContinuationCallbacks(answer).run("independent-four")
            self.assertFalse(result["completed"])
            self.assertEqual(result["metrics"]["repairs"], 2)
            self.assertEqual(result["metrics"]["escalated_repairs"], 1)
            self.assertTrue(all(not row["failure_progress"] for row in result["repair_comparisons"]))
            self.assertTrue(result["candidates"]["candidate-E"]["source_valid"])
            self.assertEqual(result["metrics"]["repair_failure_progress"], 0)

    def test_all_pass_uncertainty_requires_admitted_focused_evidence(self):
        for probes, remaining, expected_reviews in (([probe()], ["R0"], 2),
                ([probe(expected=999)], ["R0"], 2), ([], ["R0"], 2), ([probe()], [], 1)):
            def answer(r):
                if r["role"] == "builder":
                    return builder(code(probe=2))
                return review(r, action="inspect", probes=probes, remaining=remaining) if r["metadata"]["round"] == 1 else review(r)
            result = ContinuationCallbacks(answer).run()
            self.assertEqual(result["metrics"]["reviewer_calls"], expected_reviews)
            self.assertEqual(result["metrics"]["repairs"], 0)
            self.assertEqual(result["completed"], expected_reviews == 2)

    def test_unrelated_or_duplicate_probe_does_not_justify_uncertainty_continuation(self):
        for proposed in ([probe(requirement="R0")], [dict(input="public1", expected=1, requirement="R1")]):
            cb = ContinuationCallbacks(lambda r: builder(code(probe=2)) if r["role"] == "builder" else
                review(r, action="inspect", probes=proposed, remaining=["R1"]), stage=1)
            result = cb.run()
            self.assertEqual(result["routing_stop"]["reason_code"], "uncertainty_without_focused_evidence")
            self.assertEqual(result["metrics"]["reviewer_calls"], 2)
            self.assertEqual(result["metrics"]["repairs"], 0)

    def test_empty_uncertainty_gets_one_focused_opportunity_within_existing_review_budget(self):
        def answer(r):
            if r["role"] == "builder":
                return builder(code(probe=2))
            number = r["metadata"]["round"]
            if number == 1:
                return review(r, action="inspect", remaining=["R0"])
            if number == 2:
                context = json.loads(r["files"][historical.CONTEXT_PATH])
                focus = context["continuation"]["required_next_action"]
                self.assertEqual(focus["action"], "request_focused_probe")
                self.assertEqual(focus["focus_remaining"], ["R0"])
                self.assertIn("existing reviewer opportunity is reserved", r["instructions"])
                self.assertEqual(len(context["verified_cases"]), 1)
                return review(r, action="inspect", probes=[probe()], remaining=["R0"])
            return review(r)
        result = ContinuationCallbacks(answer).run()
        self.assertTrue(result["completed"])
        self.assertEqual(result["metrics"]["reviewer_calls"], 3)
        self.assertEqual(result["metrics"]["focused_review_requests"], 1)
        self.assertEqual(result["metrics"]["focused_evidence_steps"], 1)
        self.assertEqual(result["metrics"]["repairs"], 0)

    def test_repeated_empty_focus_stops_after_second_review_without_extra_calls(self):
        cb = ContinuationCallbacks(lambda r: builder(code()) if r["role"] == "builder" else
            review(r, action="inspect", remaining=["R0"]))
        result = cb.run()
        self.assertFalse(result["completed"])
        self.assertEqual(result["metrics"]["reviewer_calls"], 2)
        self.assertEqual(result["metrics"]["focused_review_requests"], 1)
        self.assertEqual(result["routing_stop"]["reason_code"], "uncertainty_without_focused_evidence")
        self.assertEqual(len(cb.requests), 3)

    def test_focus_retry_last_round_cannot_add_fifth_or_unreviewable_repair(self):
        for limit in (1, 2):
            cb = ContinuationCallbacks(lambda r: builder(code()) if r["role"] == "builder" else
                review(r, action="inspect", remaining=["R0"]))
            result = cb.run(limits={"max_reviews": limit})
            self.assertEqual(result["metrics"]["reviewer_calls"], limit)
            self.assertEqual(result["metrics"]["focused_review_requests"], limit - 1)
            self.assertEqual(result["metrics"]["repairs"], 0)
            self.assertFalse(result["completed"])

    def test_stop_reports_returned_source_state_separately_from_reviewed_failure(self):
        def answer(r):
            if r["role"] == "builder":
                return builder({"public0": 0}) if r["metadata"]["candidate_id"] == "slot-0" else builder(code())
            return review(r, action="inspect")
        result = ContinuationCallbacks(answer).run("independent-four", limits={"max_reviews": 1})
        self.assertFalse(result["completed"])
        self.assertTrue(all(row["passed"] for row in result["matrix"][result["selected"]].values()))
        self.assertEqual(result["reason_code"], "review_acceptance_missing")
        self.assertEqual(result["routing_stop"]["reason_code"], "active_checks_failed")
        self.assertNotEqual(result["selected"], result["routing_stop"]["candidate"])

    def test_speculative_repair_for_passing_evidence_stops_without_builder_call(self):
        result = ContinuationCallbacks(lambda r: builder(code()) if r["role"] == "builder" else
            review(r, action="repair", remaining=["R0"])).run()
        self.assertFalse(result["completed"])
        self.assertEqual(result["metrics"]["repairs"], 0)
        self.assertEqual(result["routing_stop"]["reason_code"], "uncertainty_without_focused_evidence")

    def test_last_review_failure_never_dispatches_unreviewable_repair(self):
        result = ContinuationCallbacks(lambda r: builder(code()) if r["role"] == "builder" else
            review(r, probes=[probe()])).run(limits={"max_reviews": 1})
        self.assertFalse(result["completed"])
        self.assertEqual(result["metrics"]["repairs"], 0)
        self.assertEqual(result["metrics"]["blocked_acceptances"], 1)
        self.assertEqual(result["reason_code"], "active_checks_failed")

    def test_accept_requires_remaining_empty_and_source_eligibility(self):
        result = ContinuationCallbacks(lambda r: builder(code()) if r["role"] == "builder" else
            review(r, remaining=["R0"])).run()
        self.assertFalse(result["completed"])
        self.assertEqual(result["metrics"]["blocked_acceptances"], 2)
        initial = {**project()["initial_files"], "engine.py": json.dumps(code())}
        result = ContinuationCallbacks(lambda r: None if r["role"] == "builder" else review(r)).run(initial=initial)
        self.assertFalse(result["completed"])
        self.assertEqual(result["reason_code"], "source_ineligible")
        self.assertEqual(result["metrics"]["repairs"], 0)

    def test_malformed_review_cannot_grant_acceptance_but_executed_failure_can_route_repair(self):
        result = ContinuationCallbacks(lambda r: builder({"public0": 0}) if r["role"] == "builder" else None).run()
        self.assertFalse(result["completed"])
        self.assertEqual(result["metrics"]["repairs"], 2)
        self.assertEqual(result["metrics"]["invalid_reviews"], 3)
        passing = ContinuationCallbacks(lambda r: builder(code()) if r["role"] == "builder" else None).run()
        self.assertEqual(passing["routing_stop"]["reason_code"], "invalid_review_unresolved")
        self.assertEqual(passing["metrics"]["repairs"], 0)

    def test_receipt_infrastructure_or_binding_failure_propagates(self):
        changes: tuple[dict, ...] = ({"cleanup_verified": False}, {"source_sha256": "wrong"},
            {"suite_sha256": "wrong"}, {"status": "infrastructure_failed"}, {"outcomes": []})
        for change in changes:
            cb = ContinuationCallbacks(lambda r: builder(code()))
            evaluate = cb.evaluate
            with patch.object(cb, "evaluate", side_effect=lambda *args: {**evaluate(*args), **change}):
                with self.assertRaises(RuntimeError):
                    cb.run()
            self.assertFalse(any(r["role"] == "reviewer" for r in cb.requests))

    def test_no_private_data_or_host_provenance_reaches_models_after_repair(self):
        def answer(r):
            if r["role"] == "builder":
                return builder(code() if r["metadata"]["phase"] == "repair" else {"public0": 0})
            return review(r, action="inspect") if r["metadata"]["round"] == 1 else review(r, slot="repair-1")
        cb = ContinuationCallbacks(answer)
        cb.run()
        for request in cb.requests:
            text = json.dumps(request["files"]) + request["feedback"] + request["instructions"]
            for forbidden in ("PRIVATE_HIDDEN_SENTINEL", "PRIVATE_ANSWER_SENTINEL", "PRIVATE_ORACLE_SENTINEL", "/host-private-store/"):
                self.assertNotIn(forbidden, text)

    def test_extra_alias_notes_do_not_cross_stage_checkpoint(self):
        def answer(r):
            if r["role"] == "builder":
                return builder(code(), notes="candidate-E has fixed it") if r["metadata"]["phase"] == "repair" else builder({"public0": 0})
            return review(r, action="inspect") if r["metadata"]["round"] == 1 else review(r, slot="repair-1")
        result = ContinuationCallbacks(answer).run()
        self.assertTrue(result["completed"])
        self.assertEqual(result["notes"], "")

    def test_initial_only_durably_freezes_before_any_scout_or_reviewer(self):
        cb = ContinuationCallbacks(lambda r: builder(code()))
        result = cb.run("independent-four", initial_only=True)
        self.assertTrue(result["initial_only"])
        self.assertEqual(result["initial_pool_sha256"], digest(result["initial_pool"]))
        self.assertEqual(len(cb.requests), 4)
        self.assertEqual(len(cb.frozen), 1)
        self.assertFalse(cb.gates)
        self.assertNotIn("completed", result)
        cb = ContinuationCallbacks(lambda r: builder(code()))
        cb.scouts = [dict(scout_id="one", probes=[probe()])]
        with self.assertRaisesRegex(ValueError, "Initial-only"):
            cb.run(initial_only=True)

    def test_shared_pool_import_preserves_original_lineage_with_fresh_arm_checks(self):
        prepared = ContinuationCallbacks(lambda r: builder(code())).run("independent-four", initial_only=True)
        for mode in ("current", "improved"):
            cb = ContinuationCallbacks(lambda r: review(r))
            result = cb.run("independent-four", frozen_initial_pool=envelope(prepared), controller_mode=mode)
            self.assertTrue(result["completed"])
            self.assertEqual(result["metrics"]["initial_builder_calls"], 0)
            self.assertEqual(result["metrics"]["shared_initial_builder_opportunities"], 4)
            self.assertEqual(len(cb.evaluations), 4)
            self.assertTrue(all("fresh-evidence" in row[2] for row in cb.evaluations))
            self.assertEqual(len(cb.requests), 1)
            for alias, item in result["initial_pool"]["candidates"].items():
                original = prepared["initial_pool"]["candidates"][alias]
                self.assertEqual(item["files"], original["files"])
                self.assertEqual(item["eligibility_origin"], original["eligibility_origin"])
                self.assertEqual(item["shared_original_binding"], original["binding"])
                self.assertEqual(item["shared_original_validation_receipts"], original["validation_receipts"])

    def test_shared_pool_original_pass_is_not_reused_as_arm_correctness(self):
        prepared = ContinuationCallbacks(lambda r: builder(code())).run(initial_only=True)
        cb = ContinuationCallbacks(lambda r: review(r))
        original = cb.evaluate
        def changed_evaluator(*args):
            receipt = original(*args)
            receipt.update(passed=False, status="failed")
            receipt["outcomes"][0].update(passed=False, actual=0)
            return receipt
        with patch.object(cb, "evaluate", side_effect=changed_evaluator):
            result = cb.run(frozen_initial_pool=envelope(prepared), limits={"max_reviews": 1})
        self.assertFalse(result["completed"])
        self.assertEqual(result["metrics"]["blocked_acceptances"], 1)

    def test_shared_pool_wrong_hash_order_scope_eligibility_or_receipt_fails_closed(self):
        prepared = ContinuationCallbacks(lambda r: builder(code())).run("independent-four", initial_only=True)
        shared = envelope(prepared)
        def wrong_order(pool):
            pool["candidate_order"].reverse()
        def wrong_scope(pool):
            next(iter(pool["candidates"].values()))["files"]["README.md"] = "tampered"
        def wrong_eligibility(pool):
            next(iter(pool["candidates"].values()))["eligibility_origin"]["files_sha256"] = "wrong"
        def wrong_receipt(pool):
            next(iter(pool["candidates"].values()))["validation_receipts"][0]["receipt"]["suite_sha256"] = "wrong"
        for mutate in (wrong_order, wrong_scope, wrong_eligibility, wrong_receipt):
            copy = deepcopy(shared)
            mutate(copy["pool"])
            copy["pool_sha256"] = digest(copy["pool"])
            cb = ContinuationCallbacks(lambda r: review(r))
            with self.assertRaises((ValueError, RuntimeError)):
                cb.run("independent-four", frozen_initial_pool=copy)
            self.assertFalse(cb.requests)
            self.assertFalse(cb.evaluations)
        for key, value in (("pool_sha256", "wrong"), ("candidate_seed", 18), ("initial_files_sha256", "wrong"), ("purpose", "independent_acceptance")):
            bad = deepcopy(shared)
            bad[key] = value
            with self.assertRaises(ValueError):
                ContinuationCallbacks(lambda r: review(r)).run("independent-four", frozen_initial_pool=bad)

    def test_current_mode_matches_historical_requests_and_decisions_across_repair_stalls(self):
        scenarios = ("accept", "repair", "inspect", "probe")
        for scenario in scenarios:
            def answer(r):
                if r["role"] == "builder":
                    return builder(code(probe=2) if scenario == "accept" else {"public0": 0}, notes="checkpoint mentions candidate-E and candidate-F")
                if scenario == "probe":
                    return review(r, action="inspect", probes=[probe()], remaining=["R0"])
                return review(r, action=scenario)
            for policy in ("strong-anchor", "sequential-four", "independent-four"):
                with self.subTest(scenario=scenario, policy=policy):
                    old_cb, new_cb = Callbacks(answer), ContinuationCallbacks(answer)
                    old_result = old_cb.run(policy)
                    new_result = new_cb.run(policy, controller_mode="current")
                    # Protocol-dependent initial pool hashes differ by design.
                    # Every model request and behavioral decision remains equal.
                    old_requests = sorted(old_cb.requests, key=lambda r: (r["metadata"]["phase"] if "phase" in r["metadata"] else "review", r["metadata"]["round"], r["metadata"]["candidate_id"]))
                    new_requests = sorted(new_cb.requests, key=lambda r: (r["metadata"].get("phase", "review"), r["metadata"]["round"], r["metadata"]["candidate_id"]))
                    self.assertEqual(normalize_protocol(new_requests), old_requests)
                    self.assertEqual(new_cb.evaluations, old_cb.evaluations)
                    self.assertEqual(new_cb.retained, old_cb.retained)
                    self.assertEqual(new_result["trajectory"], old_result["trajectory"])
                    self.assertEqual({key: new_result["metrics"][key] for key in old_result["metrics"]}, old_result["metrics"])
                    for key in ("completed", "files", "reason_code", "selected", "remaining", "matrix", "candidates"):
                        self.assertEqual(new_result[key], old_result[key])

    def test_controller_caps_cannot_silently_expand(self):
        for key, value in (("max_repairs", 3), ("max_reviews", 5), ("max_new_probes", 9), ("max_escalations", 2)):
            with self.assertRaisesRegex(ValueError, "offered caps"):
                ContinuationCallbacks(lambda r: builder(code())).run(limits={key: value})

    def test_repair_comparison_rejects_missing_or_nonboolean_outcomes(self):
        case = [dict(id="one", input=1, expected=1, requirement="R0")]
        for before in ({}, {"one": {"passed": 1}}):
            with self.assertRaises(RuntimeError):
                repair_comparison(before, {"one": {"passed": True}}, case)
