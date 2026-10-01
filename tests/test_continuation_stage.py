from copy import deepcopy
import json
import threading
import unittest

from gossip_harness.verification_probes import parse_proposals, revalidate_cases
from gossip_harness.continuation_stage import run_continuation_stage, digest, PROTOCOL
from gossip_harness.worker import WorkerResult


def project():
    return dict(id="evolving", title="Evolving repository",
        allowed_paths=("engine.py", "helper.py"),
        initial_files={"engine.py": "{}", "helper.py": "stable", "README.md": "trusted"},
        stages=tuple(dict(specification=f"Implement milestone {index}", requirements=[f"R{index}"],
            visible_cases=[dict(id=f"public-{index}", input=f"public{index}", expected=1, requirement=f"R{index}")],
            hidden_cases=[dict(input="PRIVATE_HIDDEN_SENTINEL", expected="PRIVATE_ANSWER_SENTINEL")],
            known_files={"engine.py": "PRIVATE_ORACLE_SENTINEL"}) for index in range(4)))


def code(**extra):
    return {**{f"public{i}": 1 for i in range(4)}, **extra}


def builder(values, notes=None, extra=None):
    changes = {"engine.py": json.dumps(values)}
    if notes is not None:
        changes["notes.json"] = json.dumps(dict(notes=notes, remaining=[]))
    changes.update(extra or {})
    return WorkerResult(changes, "source proposal", 0, {})


def review(request, action="accept", slot="slot-0", probes=None, notes="Inspect the implementation", remaining=None):
    value = dict(candidate=request["metadata"]["alias_by_slot"][slot], action=action,
                 notes=notes, remaining=remaining or [], probes=probes or [])
    return WorkerResult({"review.json": json.dumps(value)}, "review decision", 0, {})


def probe(value="probe", expected=2, requirement="R0"):
    return dict(input=value, expected=expected, requirement=requirement)


def reference(stage, value):
    if value == "stageful":
        return stage + 2
    return 1 if value.startswith("public") else 2


def validate_input(stage, value):
    if not isinstance(value, str) or len(value) > 40 or value.startswith("outside"):
        raise ValueError("Outside the fixture domain")


class Callbacks:
    def __init__(self, answer, stage=0):
        self.project = project()
        self.answer = answer
        self.stage = stage
        self.requests = []
        self.events = []
        self.retained = {}
        self.evaluations = []
        self.gates = []
        self.lock = threading.Lock()

    def invoke(self, **request):
        with self.lock:
            self.requests.append(deepcopy(request))
        return self.answer(request)

    def retain(self, files, label):
        self.retained[label] = deepcopy(files)
        sha = digest(files)
        return dict(files_sha256=sha, tip_sha=sha[:40], store_path="/host-private-store/" + label + ".git")

    def evaluate(self, files, cases, label):
        self.evaluations.append((deepcopy(files), deepcopy(cases), label))
        assert any(retained == files for retained in self.retained.values())
        actual = json.loads(files["engine.py"])
        outcomes = [dict(index=index, passed=digest(actual.get(case["input"])) == digest(case["expected"]),
                         actual=actual.get(case["input"]), status="executed") for index, case in enumerate(cases)]
        passed = all(row["passed"] for row in outcomes)
        # BlackboxValidator drops provenance from the execution-suite binding;
        # the stage keeps its separate hash of the full retained evidence.
        execution_cases = [{key: case[key] for key in ("input", "expected", "id", "requirement") if key in case}
                           for case in cases]
        return dict(passed=passed, status="passed" if passed else "failed", outcomes=outcomes,
                    cleanup_verified=True, source_sha256=digest(files), suite_sha256=digest(execution_cases))

    def validate_probes(self, envelope, stage, origin):
        self.gates.append(deepcopy(envelope))
        if envelope["mode"] == "revalidate":
            return revalidate_cases(envelope["probes"], stage_index=stage, reference=reference,
                                    validate_input=validate_input, origin=origin)
        requirements = [requirement for item in self.project["stages"][:stage + 1] for requirement in item["requirements"]]
        return parse_proposals(json.dumps({"probes": envelope["probes"]}), stage_index=stage,
             requirements=requirements, reference=reference, validate_input=validate_input, origin=origin,
             existing_cases=envelope["existing_cases"], max_new=envelope["max_new"])

    def emit(self, event, **data):
        self.events.append(dict(event=event, **deepcopy(data)))

    def run(self, policy="strong-reviewed", prior=None, initial=None, seed=17, **options):
        return run_continuation_stage(self.project, self.stage, policy,
            initial or self.project["initial_files"], prior or {},
            invoke=self.invoke, evaluate=self.evaluate, retain=self.retain,
            validate_probes=self.validate_probes, emit=self.emit, candidate_seed=seed, **options)


class ContinuationStageTests(unittest.TestCase):
    def test_every_policy_requires_review_acceptance_and_passing_evidence(self):
        for policy, count, model in (("strong-reviewed", 1, "strong"), ("cheap-reviewed", 1, "cheap"),
                                    ("cheap-selective", 1, "cheap"), ("portfolio-reviewed", 4, "cheap")):
            with self.subTest(policy=policy):
                cb = Callbacks(lambda r: review(r) if r["role"] == "reviewer" else builder(code()))
                result = cb.run(policy)
                self.assertTrue(result["completed"])
                self.assertEqual(result["protocol"], PROTOCOL)
                self.assertEqual(result["metrics"]["builder_calls"], count)
                self.assertEqual({r["model"] for r in cb.requests if r["role"] == "builder"}, {model})

    def test_rejected_repair_preserves_exact_previously_eligible_source(self):
        for rejected in (None, builder(code(), extra={"README.md": "bad"}),
                         WorkerResult({"engine.py": None}, "delete", 0, {})):
            with self.subTest(rejected=rejected):
                def answer(r):
                    if r["role"] == "reviewer":
                        return review(r, "repair" if r["metadata"]["round"] == 1 else "accept")
                    return builder(code()) if r["metadata"]["round"] == 1 else rejected

                result = Callbacks(answer).run()
                rows = [row for row in result["trajectory"] if row["kind"] == "builder"]
                self.assertTrue(result["completed"])
                self.assertEqual(rows[0]["files_sha256"], rows[1]["files_sha256"])
                self.assertEqual(rows[1]["attempt_status"], "rejected_source_proposal")
                self.assertTrue(rows[1]["source_valid"])
                self.assertFalse(rows[1]["attempt_admissible"])
                self.assertEqual(result["files"]["README.md"], "trusted")
                self.assertEqual(result["metrics"]["retained_eligibility_preserved"], 1)

    def test_direct_no_op_repair_preserves_scope_approval_without_new_approval(self):
        def answer(r):
            return review(r, "repair" if r["metadata"]["round"] == 1 else "accept") if r["role"] == "reviewer" else builder(code())
        result = Callbacks(answer).run()
        rows = [r for r in result["trajectory"] if r["kind"] == "builder"]
        self.assertTrue(result["completed"])
        self.assertEqual(rows[1]["attempt_status"], "no_effective_source_change")
        self.assertTrue(rows[1]["source_valid"])
        self.assertEqual(result["candidates"][result["selected"]]["eligibility_origin"]["label"],
                         "stage-0-build-slot-0-1")

    def test_unapproved_passing_baseline_cannot_gain_eligibility_from_no_op(self):
        initial = {**project()["initial_files"], "engine.py": json.dumps(code())}
        for proposed in (None, builder(code()), WorkerResult({"notes.json": "{}"}, "notes", 0, {})):
            cb = Callbacks(lambda r: review(r) if r["role"] == "reviewer" else proposed)
            result = cb.run(initial=initial, limits={"max_reviews": 1})
            self.assertFalse(result["completed"])
            self.assertFalse(result["source_valid"])
            self.assertEqual(result["reason_code"], "source_ineligible")
            self.assertTrue(all(v["passed"] for v in result["matrix"][result["selected"]].values()))

    def test_exact_host_approved_baseline_remains_eligible_on_bad_attempt(self):
        initial = {**project()["initial_files"], "engine.py": json.dumps(code())}
        approval = dict(files_sha256=digest(initial), source_valid=True, approval_id="retained-git-source-check-1")
        cb = Callbacks(lambda r: review(r) if r["role"] == "reviewer" else None)
        result = cb.run(initial=initial, baseline_approval=approval)
        self.assertTrue(result["completed"])
        self.assertEqual(result["candidates"][result["selected"]]["eligibility_origin"]["kind"], "approved_baseline")
        self.assertEqual(len(cb.evaluations), 1)
        for changed in ({**approval, "files_sha256": "wrong"}, {**approval, "source_valid": False},
                        {**approval, "approval_id": ""}, {**approval, "extra": 1}):
            cb = Callbacks(lambda r: None)
            with self.assertRaisesRegex(ValueError, "Baseline"):
                cb.run(initial=initial, baseline_approval=changed)
            self.assertFalse(cb.requests)

    def test_valid_effective_proposal_can_establish_eligibility_after_invalid_initial(self):
        def answer(r):
            if r["role"] == "reviewer":
                return review(r, "repair" if r["metadata"]["round"] == 1 else "accept")
            return None if r["metadata"]["round"] == 1 else builder(code())
        result = Callbacks(answer).run()
        self.assertTrue(result["completed"])
        self.assertTrue(result["source_valid"])
        self.assertEqual(result["metrics"]["invalid_source_proposals"], 1)

    def test_stagnant_inspection_triggers_bounded_strong_repair_then_separate_review(self):
        def answer(r):
            if r["role"] == "reviewer":
                return review(r, "inspect" if r["metadata"]["round"] < 3 else "accept")
            return builder(code())
        cb = Callbacks(answer)
        result = cb.run()
        self.assertTrue(result["completed"])
        self.assertEqual(result["metrics"]["escalations"], 1)
        self.assertEqual(result["metrics"]["repairs"], 1)
        self.assertEqual(result["metrics"]["reviewer_calls"], 3)
        self.assertFalse(result["escalations"][0]["effective_source_change"])
        self.assertEqual(result["escalations"][0]["action"], "strong_repair")
        review_requests = [r for r in cb.requests if r["role"] == "reviewer"]
        self.assertIsNotNone(review_requests[-1]["metadata"]["escalation_id"])
        self.assertIn("No forced acceptance", review_requests[-1]["instructions"])

    def test_remaining_prose_oscillation_does_not_manufacture_progress(self):
        def answer(r):
            if r["role"] == "builder":
                return builder(code())
            return review(r, "inspect", notes=f"New assertion {r['metadata']['round']}",
                          remaining=["R0"] if r["metadata"]["round"] % 2 else [])
        result = Callbacks(answer).run(limits={"max_escalations": 1})
        self.assertFalse(result["completed"])
        self.assertEqual(result["reason_code"], "stagnation_capacity_exhausted")
        self.assertEqual(result["metrics"]["reviewer_calls"], 4)
        self.assertEqual(result["metrics"]["repairs"], 1)

    def test_candidate_reselection_does_not_reset_stall(self):
        def answer(r):
            if r["role"] == "builder":
                return builder(code())
            return review(r, "inspect", slot=f"slot-{r['metadata']['round'] % 4}")
        result = Callbacks(answer).run("portfolio-reviewed", limits={"max_escalations": 0})
        self.assertFalse(result["completed"])
        self.assertEqual(result["metrics"]["reviewer_calls"], 2)

    def test_source_cycle_is_detected_as_revisited_state(self):
        def answer(r):
            if r["role"] == "reviewer":
                return review(r, "repair", remaining=["R0"])
            return builder(code(extra=r["metadata"]["round"] % 2))
        result = Callbacks(answer).run(limits={"max_escalations": 0})
        progress = [r for r in result["trajectory"] if r["kind"] == "progress"]
        self.assertTrue(progress[0]["novel_progress"])
        self.assertTrue(progress[1]["revisited_state"])
        self.assertFalse(progress[1]["novel_progress"])
        self.assertEqual(result["reason_code"], "stagnation_capacity_exhausted")

    def test_cheap_selective_escalates_builder_but_fixed_cheap_policy_does_not(self):
        for policy, expected in (("cheap-selective", ["cheap", "strong"]), ("cheap-reviewed", ["cheap"])):
            cb = Callbacks(lambda r: review(r, "inspect" if r["metadata"]["round"] < 3 else "accept")
                           if r["role"] == "reviewer" else builder(code()))
            result = cb.run(policy)
            self.assertTrue(result["completed"])
            self.assertEqual([r["model"] for r in cb.requests if r["role"] == "builder"], expected)

    def test_exhausted_probe_budget_and_rejected_duplicates_do_not_count_as_progress(self):
        def answer(r):
            return review(r, "inspect", probes=[probe()]) if r["role"] == "reviewer" else builder(code(probe=2))
        result = Callbacks(answer).run(limits={"max_new_probes": 0, "max_escalations": 0})
        self.assertFalse(result["completed"])
        self.assertEqual(result["metrics"]["admitted_new_probes"], 0)
        self.assertEqual(result["metrics"]["rejected_probes"], 2)
        self.assertEqual(result["metrics"]["reviewer_calls"], 2)

    def test_repeated_failed_cheap_repairs_reserve_capacity_for_strong_escalation(self):
        def answer(r):
            if r["role"] == "reviewer":
                return review(r, "repair" if r["metadata"]["round"] < 3 else "accept")
            return builder(code()) if r["model"] == "strong" else builder(code(public0=0))
        cb = Callbacks(answer)
        result = cb.run("cheap-selective", limits={"max_repairs": 2})
        self.assertTrue(result["completed"])
        self.assertEqual([r["model"] for r in cb.requests if r["role"] == "builder"],
                         ["cheap", "cheap", "strong"])
        self.assertEqual(result["metrics"]["repairs"], 2)
        self.assertEqual(result["metrics"]["escalated_repairs"], 1)
        self.assertEqual(result["metrics"]["reviewer_calls"], 3)

    def test_new_oracle_admitted_evidence_resets_stall(self):
        def answer(r):
            if r["role"] == "builder":
                return builder(code(probe=2))
            round_number = r["metadata"]["round"]
            return review(r, "accept" if round_number == 3 else "inspect",
                          probes=[probe()] if round_number == 2 else [])
        result = Callbacks(answer).run(limits={"max_escalations": 0})
        self.assertTrue(result["completed"])
        progress = [r for r in result["trajectory"] if r["kind"] == "progress"]
        self.assertEqual(progress[-1]["consecutive_stagnant_reviews"], 0)
        self.assertTrue(progress[-1]["novel_progress"])

    def test_new_probe_blocks_provisional_acceptance_and_runs_on_all_candidates(self):
        def answer(r):
            if r["role"] == "builder":
                return builder(code(probe=0 if r["metadata"]["candidate_id"] == "slot-0" else 2))
            first = r["metadata"]["round"] == 1
            return review(r, slot="slot-0" if first else "slot-1", probes=[probe()] if first else [])
        cb = Callbacks(answer)
        result = cb.run("portfolio-reviewed")
        self.assertTrue(result["completed"])
        self.assertEqual(result["metrics"]["blocked_acceptances"], 1)
        self.assertEqual(len([v for v in cb.evaluations if v[2].endswith("new-probes")]), 4)

    def test_all_active_evidence_reexecutes_after_effective_source_change(self):
        def answer(r):
            if r["role"] == "reviewer":
                return review(r, "repair" if r["metadata"]["round"] == 1 else "accept", probes=[probe()])
            return builder(code(probe=2 if r["metadata"]["round"] == 1 else 0))
        cb = Callbacks(answer)
        result = cb.run(limits={"max_reviews": 2})
        self.assertFalse(result["completed"])
        self.assertEqual(result["reason_code"], "active_checks_failed")
        repairs = [v for v in cb.evaluations if "-2-all-evidence" in v[2]]
        self.assertEqual(len(repairs[0][1]), 2)
        self.assertEqual(repairs[0][1][-1]["input"], "probe")

    def test_missing_or_wrong_source_and_suite_bindings_fail_closed(self):
        for field, value in (("source_sha256", None), ("source_sha256", "bad"),
                             ("suite_sha256", None), ("suite_sha256", "bad")):
            cb = Callbacks(lambda r: builder(code()))
            original = cb.evaluate
            def evaluate(*args):
                receipt = original(*args)
                if value is None:
                    receipt.pop(field)
                else:
                    receipt[field] = value
                return receipt
            cb.evaluate = evaluate
            with self.assertRaisesRegex(RuntimeError, "binding mismatch"):
                cb.run()

    def test_no_repair_runs_without_subsequent_review_capacity(self):
        cb = Callbacks(lambda r: review(r, "repair", remaining=["R0"])
                       if r["role"] == "reviewer" else builder(code()))
        result = cb.run(limits={"max_reviews": 1})
        self.assertFalse(result["completed"])
        self.assertEqual(result["metrics"]["repairs"], 0)
        self.assertEqual(result["reason_code"], "reviewer_requirements_unresolved")

    def test_missing_or_unverified_cleanup_receipt_never_authorizes_acceptance(self):
        for value in ("missing", False, None):
            cb = Callbacks(lambda r: review(r) if r["role"] == "reviewer" else builder(code()))
            original = cb.evaluate
            def evaluate(*args):
                receipt = original(*args)
                if value == "missing":
                    receipt.pop("cleanup_verified")
                else:
                    receipt["cleanup_verified"] = value
                return receipt
            cb.evaluate = evaluate
            with self.assertRaisesRegex(RuntimeError, "complete receipt"):
                cb.run()

    def test_repair_cap_exhaustion_uses_review_escalation_and_never_autopromotes(self):
        cb = Callbacks(lambda r: review(r, "inspect") if r["role"] == "reviewer" else builder(code()))
        result = cb.run(limits={"max_repairs": 0, "max_escalations": 1})
        self.assertFalse(result["completed"])
        self.assertEqual(result["metrics"]["repairs"], 0)
        self.assertEqual(result["escalations"][0]["action"], "escalated_review")
        self.assertEqual(result["metrics"]["reviewer_calls"], 4)
        self.assertEqual(result["metrics"]["builder_calls"], 1)

    def test_invalid_review_is_bounded_and_keeps_meaningful_remaining(self):
        cb = Callbacks(lambda r: WorkerResult({"review.json": "{}"}, "invalid", 0, {})
                       if r["role"] == "reviewer" else builder({}))
        result = cb.run(limits={"max_escalations": 0})
        self.assertFalse(result["completed"])
        self.assertEqual(result["remaining"], ["R0"])
        self.assertEqual(result["metrics"]["invalid_reviews"], 2)

    def test_provider_and_validation_exceptions_propagate_without_retry(self):
        cb = Callbacks(lambda r: (_ for _ in ()).throw(RuntimeError("unknown usage")))
        with self.assertRaisesRegex(RuntimeError, "unknown usage"):
            cb.run()
        self.assertEqual(len(cb.requests), 1)
        cb = Callbacks(lambda r: builder(code()))
        cb.evaluate = lambda *args: dict(status="timeout", passed=False)
        with self.assertRaises(RuntimeError):
            cb.run()

    def test_hidden_cases_model_identity_and_store_paths_never_enter_review_content(self):
        cb = Callbacks(lambda r: review(r) if r["role"] == "reviewer" else builder(code()))
        cb.run("portfolio-reviewed")
        request = next(r for r in cb.requests if r["role"] == "reviewer")
        content = json.dumps({k: v for k, v in request.items() if k not in {"metadata", "model"}})
        for secret in ("PRIVATE_HIDDEN_SENTINEL", "PRIVATE_ANSWER_SENTINEL", "PRIVATE_ORACLE_SENTINEL",
                       "/host-private-store/", "portfolio-reviewed"):
            self.assertNotIn(secret, content)

    def test_deterministic_replay_including_stagnation_and_concurrent_initial_calls(self):
        answer = lambda r: review(r, "inspect" if r["metadata"]["round"] < 3 else "accept") if r["role"] == "reviewer" else builder(code())
        first, second = Callbacks(answer), Callbacks(answer)
        self.assertEqual(first.run("portfolio-reviewed"), second.run("portfolio-reviewed"))
        self.assertEqual(first.events, second.events)
        normalized = lambda requests: sorted((digest(r["metadata"]), digest(r)) for r in requests)
        self.assertEqual(normalized(first.requests), normalized(second.requests))

    def test_two_stage_project_and_retained_probes_are_supported(self):
        first = Callbacks(lambda r: review(r, probes=[probe()]) if r["role"] == "reviewer" else builder(code(probe=2))).run()
        cb = Callbacks(lambda r: review(r) if r["role"] == "reviewer" else builder(code(probe=2, changed=True)), stage=1)
        cb.project["stages"] = cb.project["stages"][:2]
        result = cb.run(prior=first, initial=first["files"])
        self.assertTrue(result["completed"])
        self.assertEqual(len(result["probe_pool"]), 1)
        self.assertEqual(result["probe_pool"][0]["validated_stage_index"], 1)
        self.assertEqual(len(result["check_history"]), 2)

    def test_malformed_limits_fail_before_invocation(self):
        for limits in ({"max_reviews": 0}, {"max_repairs": True}, {"max_escalations": -1},
                       {"unknown": 2}, {"max_new_probes": 1000}):
            cb = Callbacks(lambda r: None)
            with self.assertRaisesRegex(ValueError, "limit"):
                cb.run(limits=limits)
            self.assertFalse(cb.requests)

    def test_later_public_case_cannot_collide_with_retained_probe_identity(self):
        first = Callbacks(lambda r: review(r, probes=[probe()]) if r["role"] == "reviewer"
                          else builder(code(probe=2))).run()
        cb = Callbacks(lambda r: review(r) if r["role"] == "reviewer"
                       else builder(code(probe=2, public1=0)), stage=1)
        cb.project["stages"][1]["visible_cases"][0]["id"] = first["probe_pool"][0]["id"]
        with self.assertRaisesRegex(RuntimeError, "distinct active case IDs"):
            cb.run(prior=first, initial=first["files"])
        self.assertFalse(cb.requests)


if __name__ == "__main__":
    unittest.main()
