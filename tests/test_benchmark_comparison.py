"""Synthetic retained-evidence checks; never imports or executes candidate code."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from analysis.benchmark_comparison import (
    ANCHOR, Inputs, MATCHED_POLICIES, POLICIES, POLICY_CONTRACT, PROJECTS, PROTOCOL,
    digest, distribution, interval_summary, paired_comparison, summarize,
)


class BenchmarkComparisonTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve() / "cohort"
        self.root.mkdir()
        self.make_cohort()

    def write(self, name, value):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value, allow_nan=False))

    def read(self, name):
        return json.loads((self.root / name).read_text())

    def sha(self, name):
        return hashlib.sha256((self.root / name).read_bytes()).hexdigest()

    def change(self, name, edit):
        value = self.read(name)
        edit(value)
        self.write(name, value)

    def refresh(self):
        self.change("independent-audit.json", lambda value: value.update(
            results_sha256=self.sha("results.json"), timings_sha256=self.sha("timings.json")))

    def make_cohort(self):
        source_path = self.root / "source-snapshot/gossip_harness/frozen.py"
        source_path.parent.mkdir(parents=True)
        source_path.write_text("raise RuntimeError('Candidate source must never execute')\n")
        projects = [dict(id=project, stages=[dict(hidden_cases=[dict(id=f"{project}-s{stage}-c{i}", requirement=f"s{stage}-r{i // 2}")
                    for i in range(2 if project == PROJECTS[0] else 4)]) for stage in range(2)]) for project in PROJECTS]
        roster = [[project, policy, rep] for project in PROJECTS for policy in POLICIES
                  for rep in range(POLICY_CONTRACT[policy]["repetitions"])]
        self.write("study-plan.json", dict(protocol=PROTOCOL, projects=list(PROJECTS), policies=POLICY_CONTRACT, roster=roster))
        faults = dict(protocol="evidence-frontier-fault-bank-v1", projects=[])
        self.write("fault-banks.json", faults)
        contract = dict(protocol=PROTOCOL, policies=POLICY_CONTRACT, milestones=2, roster=roster,
            sources={"frozen.py": self.sha("source-snapshot/gossip_harness/frozen.py")},
            plan_sha256=self.sha("study-plan.json"), fixtures={p["id"]: dict(fixture_sha256=digest(p)) for p in projects},
            fault_banks_sha256=digest(faults))
        self.write("preregistered.json", dict(contract=contract, roster=roster))
        self.write("fixtures.json", dict(projects=projects))
        spans, terminal, frozen, cases, audits = [], [], [], [], []
        def span(kind, start, end, **labels):
            row = dict(span_id=len(spans), kind=kind, clock_id="synthetic-clock", status="finished",
                       started_monotonic_ns=int(start * 1e9), finished_monotonic_ns=int(end * 1e9), elapsed_seconds=end - start, **labels)
            spans.append(row)
            return row
        for ordinal, (project_id, policy, rep) in enumerate(roster):
            run_id = f"{project_id}-{policy}-{rep}"
            start, end = ordinal * 30, ordinal * 30 + 20
            model = span("model_trajectory", start, end, run_id=run_id)
            stages, calls = [], []
            for stage in range(2):
                current = []
                for i, (role, a, b) in enumerate([("scout", 1, 2), ("scout", 1.5, 2.5), ("builder", 3, 4), ("reviewer", 5, 6)]):
                    call_id = f"{role}-{stage}-{i}"
                    physical = span("physical_provider_request", start + stage * 10 + a, start + stage * 10 + b,
                                    run_id=run_id, role=role, model="cheap" if role == "scout" else "strong", call_id=call_id)
                    call = dict(call_id=call_id, role=role, model=physical["model"], physical_dispatch=True,
                                physical_span_id=physical["span_id"], usage_units=10)
                    calls.append(call)
                    current.append(call)
                # Deliberate overlap verifies Docker union rather than summation as elapsed wall time.
                span("docker_validation", start + stage * 10 + 7, start + stage * 10 + 9, run_id=run_id, purpose="stage_evidence")
                span("docker_validation", start + stage * 10 + 8, start + stage * 10 + 9, run_id=run_id, purpose="stage_evidence")
                stages.append(dict(completed=True, invocations=current, scouts=dict(calls=2, admitted=2, rejected=1, rejected_batches=0),
                                   metrics=dict(initial_builder_calls=4, repairs=0)))
            state = dict(stages=stages)
            self.write(f"{run_id}/trajectory.json", state)
            trajectory_sha = self.sha(f"{run_id}/trajectory.json")
            terminal.append(dict(run_id=run_id, trajectory_sha256=trajectory_sha, span_id=model["span_id"], elapsed_seconds=20, terminal="visible_complete"))
            frozen.append(dict(run_id=run_id, project_id=project_id, policy=policy, repetition=rep, trajectory_sha256=trajectory_sha))
            project = next(p for p in projects if p["id"] == project_id)
            expected = [case for stage in project["stages"] for case in stage["hidden_cases"]]
            accepted = policy == ANCHOR or (policy == MATCHED_POLICIES[0] and rep == 0) or (policy == MATCHED_POLICIES[1] and project_id == PROJECTS[1])
            outcomes = [dict(id=case["id"], requirement=case["requirement"], passed=accepted or i < len(expected) - 1) for i, case in enumerate(expected)]
            private = dict(passed=all(row["passed"] for row in outcomes), outcomes=outcomes)
            visible = dict(passed=True)
            self.write(f"{run_id}/final-private-receipt.json", private)
            self.write(f"{run_id}/final-visible-receipt.json", visible)
            coverage = {req: all(row["passed"] for row in outcomes if row["requirement"] == req) for req in sorted({row["requirement"] for row in outcomes})}
            final_start = 310 + ordinal * 6
            span("final_evaluation", final_start, final_start + 5, run_id=run_id)
            span("docker_validation", final_start, final_start + 2, run_id=run_id, purpose="final_visible")
            span("docker_validation", final_start + 2, final_start + 4, run_id=run_id, purpose="final_private")
            cases.append(dict(run_id=run_id, project_id=project_id, policy=policy, repetition=rep, accepted=accepted,
                status="accepted" if accepted else "final_quality_failed", milestones_completed=2, trajectory_sha256=trajectory_sha,
                final_hidden=private, final_visible=visible, requirement_coverage=coverage, usage_micro_usd=80,
                invocations=calls, physical_provider_calls=8, metrics=dict(initial_builder_calls=8, repairs=0),
                scouts=dict(calls=4, admitted=4, rejected=2, rejected_batches=0)))
            audits.append(dict(run_id=run_id, accepted=accepted, milestones_completed=2, stages=[dict(completed=True), dict(completed=True)]))
        freeze = dict(protocol=PROTOCOL, contract_sha=digest(contract), trajectories=frozen,
                      private_evaluation_started=False, frozen_monotonic_ns=300_000_000_000)
        self.write("frozen-trajectories.json", freeze)
        freeze_sha = self.sha("frozen-trajectories.json")
        for case in cases:
            case["freeze_manifest_sha256"] = freeze_sha
            self.write(f"{case['run_id']}/result.json", case)
        self.write("timings.json", dict(clock_id="synthetic-clock", spans=spans))
        self.write("results.json", dict(experiment=PROTOCOL, status="finished", phase="finished", mode="live", contract=contract,
            contract_sha=digest(contract), freeze_manifest_sha256=freeze_sha, active_case=None, unexecuted=[], censored=[],
            cases=cases, trajectories=terminal, incremental_micro_usd=800))
        self.write("independent-audit.json", dict(protocol="independent-evidence-frontier-audit-v1", passed=True,
            results_sha256=self.sha("results.json"), timings_sha256=self.sha("timings.json"), freeze_manifest_sha256=freeze_sha,
            contract_sha256=digest(contract), scope=dict(primary_cohort=True, diagnostic_matrix=False), cases=audits,
            accounting=dict(study_usage_micro_usd=800)))

    def test_complete_cohort_four_blocks_anchors_context_only(self):
        summary = summarize(self.root)
        self.assertEqual(summary["status"], "certified_complete")
        self.assertEqual(len(summary["trajectories"]), 10)
        for comparison in summary["comparisons"].values():
            self.assertEqual((comparison["blocks"], comparison["application_identities"]), (4, 2))
            self.assertFalse(any(ANCHOR in row["left_run"] + row["right_run"] for row in comparison["matches"]))
        self.assertEqual(summary["policies"][ANCHOR]["trajectories"], 2)
        self.assertTrue(summary["policies"][ANCHOR]["context_only"])
        self.assertIsNone(summary["elo"])
        self.assertIsNone(summary["ranking"])
        self.assertIsNone(summary["diagnostics"])
        self.assertTrue(summary["bindings"]["inputs_unchanged"])

    def test_macro_coverage_weights_projects_not_case_denominators(self):
        summary = summarize(self.root)
        policy = summary["policies"][MATCHED_POLICIES[0]]
        # Graph 3/4 requirements; calendar 7/8. Mean .8125 differs from pooled10/12.
        self.assertEqual(policy["mean_requirement_coverage_equal_project_weight"], .8125)
        self.assertNotEqual(policy["mean_requirement_coverage"], policy["requirements_passed"] / policy["requirements_total"])

    def test_requirement_groups_require_all_cases(self):
        summary = summarize(self.root)
        row = next(r for r in summary["trajectories"] if r["project_id"] == PROJECTS[0] and not r["accepted"])
        self.assertEqual((row["hidden_passed"], row["hidden_total"]), (3, 4))
        self.assertEqual((row["requirements_passed"], row["requirements_total"]), (1, 2))

    def test_acceptance_priority_is_exposed_separately_from_coverage(self):
        rows = summarize(self.root)["trajectories"]
        for row in rows:
            row.update(accepted=row["policy"] == MATCHED_POLICIES[0], requirements_passed=0 if row["policy"] == MATCHED_POLICIES[0] else row["requirements_total"])
        self.assertEqual(paired_comparison(rows, "quality_ordered")["wins"], 4)
        self.assertEqual(paired_comparison(rows, "whole_project_acceptance")["wins"], 4)
        self.assertEqual(paired_comparison(rows, "requirement_coverage")["losses"], 4)

    def test_pairing_is_by_project_rep_not_file_order_or_all_cross(self):
        rows = summarize(self.root)["trajectories"]
        original = paired_comparison(rows, "quality_ordered")
        self.assertEqual(original, paired_comparison(list(reversed(rows)), "quality_ordered"))
        self.assertEqual(len(original["matches"]), 4)
        with self.assertRaisesRegex(ValueError, "matched roster"):
            paired_comparison(rows[:-1] + [rows[0]], "quality_ordered")

    def test_barrier_waits_separate_from_active_and_overlap(self):
        row = summarize(self.root)["trajectories"][0]["timing"]
        self.assertEqual((row["total_active_seconds"], row["wait_to_global_freeze_seconds"], row["post_freeze_evaluation_queue_seconds"]), (25, 280, 10))
        self.assertEqual(row["total_active_seconds"] + row["total_wait_seconds"], row["start_to_final_outcome_seconds"])
        self.assertEqual((row["physical_provider"]["cumulative_seconds"], row["physical_provider"]["union_wall_seconds"]), (8, 7))
        self.assertEqual((row["docker_validation"]["cumulative_seconds"], row["docker_validation"]["union_wall_seconds"]), (10, 8))
        self.assertIsNone(distribution([])["median_seconds"])

    def test_incomplete_inventory_never_reads_private_or_publishes_scores(self):
        self.change("results.json", lambda row: row.update(status="interrupted", phase="model_trajectories",
            trajectories=row["trajectories"][:1], cases=[], unexecuted=row["contract"]["roster"][2:],
            censored=[dict(run_id=row["trajectories"][1]["run_id"], reason="BudgetExceeded")]))
        (self.root / "independent-audit.json").unlink()
        read = Inputs.read
        with patch.object(Inputs, "read", autospec=True, side_effect=lambda owner, path: read(owner, path) if "private" not in str(path) else self.fail("private read")):
            summary = summarize(self.root)
        self.assertEqual(summary["status"], "incomplete_unscored")
        for key in ("policies", "projects", "comparisons", "diagnostics", "ranking", "elo"):
            self.assertIsNone(summary[key])
        self.assertEqual(CounterLike(summary["trajectories"]), {"terminal_not_certified": 1, "censored": 1, "unexecuted": 8})
        with self.assertRaisesRegex(ValueError, "Incomplete cohort"):
            summarize(self.root, diagnostics=self.root.parent / "diagnostics")

    def test_every_incomplete_signal_suppresses_scores(self):
        original = self.read("results.json")
        for changes in (dict(phase="final_evaluation"), dict(status="interrupted"), dict(active_case=dict(run_id=original["cases"][0]["run_id"])),
                        dict(censored=[dict(run_id=original["cases"][0]["run_id"], reason="Stopped")]), dict(unexecuted=[original["contract"]["roster"][0]])):
            self.write("results.json", {**original, **changes})
            with self.subTest(changes=changes):
                self.assertIsNone(summarize(self.root)["comparisons"])

    def test_missing_or_duplicate_complete_case_fails_closed(self):
        self.change("results.json", lambda row: row["cases"].__setitem__(1, row["cases"][0]))
        self.refresh()
        with self.assertRaisesRegex(ValueError, "roster mismatch"):
            summarize(self.root)

    def test_boolean_repetition_identity_rejected(self):
        self.change("results.json", lambda row: row["contract"]["roster"][0].__setitem__(2, False))
        # Keep typed contract digest/registration consistent to reach roster validation.
        report = self.read("results.json")
        self.change("study-plan.json", lambda row: row.update(roster=report["contract"]["roster"]))
        report["contract"]["plan_sha256"] = self.sha("study-plan.json")
        report["contract_sha"] = digest(report["contract"])
        self.write("results.json", report)
        self.change("preregistered.json", lambda row: row.update(contract=report["contract"], roster=report["contract"]["roster"]))
        with self.assertRaisesRegex(ValueError, "roster mismatch"):
            summarize(self.root)

    def test_exact_primary_certificate_and_timing_bindings_required(self):
        original = self.read("independent-audit.json")
        for changes in (dict(passed=False), dict(results_sha256="bad"), dict(timings_sha256="bad"), dict(contract_sha256="bad"), dict(scope=dict(primary_cohort=False))):
            self.write("independent-audit.json", {**original, **changes})
            with self.subTest(changes=changes), self.assertRaisesRegex(ValueError, "successful primary audit"):
                summarize(self.root)

    def test_frozen_source_fault_bank_and_fixture_changes_rejected(self):
        original = (self.root / "source-snapshot/gossip_harness/frozen.py").read_bytes()
        (self.root / "source-snapshot/gossip_harness/frozen.py").write_bytes(b"changed")
        with self.assertRaisesRegex(ValueError, "source identity"):
            summarize(self.root)
        (self.root / "source-snapshot/gossip_harness/frozen.py").write_bytes(original)
        self.change("fault-banks.json", lambda value: value.update(changed=True))
        with self.assertRaisesRegex(ValueError, "fault-bank identity"):
            summarize(self.root)

    def test_audited_milestone_mismatch_fails(self):
        self.change("independent-audit.json", lambda value: value["cases"][0]["stages"][0].update(completed=False))
        with self.assertRaisesRegex(ValueError, "primary outcome"):
            summarize(self.root)

    def test_physical_request_labels_and_costs_reconciled(self):
        self.change("timings.json", lambda value: value["spans"][1].update(role="wrong"))
        self.refresh()
        with self.assertRaisesRegex(ValueError, "Physical provider labels"):
            summarize(self.root)

    def test_unknown_docker_scope_cannot_pollute_primary_timing(self):
        self.change("timings.json", lambda value: next(span for span in value["spans"] if span["kind"] == "docker_validation").update(purpose="diagnostic"))
        self.refresh()
        with self.assertRaisesRegex(ValueError, "Unknown primary Docker purpose"):
            summarize(self.root)

    def test_unclosed_or_mixed_clock_timing_rejected(self):
        span = self.read("timings.json")["spans"][0]
        for edit in (dict(status="running"), dict(elapsed_seconds=999), dict(finished_monotonic_ns=-1)):
            with self.subTest(edit=edit), self.assertRaises(ValueError):
                interval_summary([{**span, **edit}])
        with self.assertRaisesRegex(ValueError, "different monotonic"):
            interval_summary([span, {**span, "clock_id": "other"}])

    def test_stable_input_reads_and_path_guards(self):
        inputs = Inputs(self.root)
        inputs.read("results.json")
        self.change("results.json", lambda value: value.update(changed=True))
        with self.assertRaisesRegex(ValueError, "between reads"):
            inputs.read("results.json")
        with self.assertRaisesRegex(ValueError, "Unsafe input path"):
            inputs.raw("../outside")
        (self.root / "alias").symlink_to(self.root / "study-plan.json")
        with self.assertRaisesRegex(ValueError, "symlink"):
            inputs.raw("alias")

    def test_duplicate_json_members_and_changed_read_set_rejected(self):
        (self.root / "duplicate.json").write_text('{"a": 1, "a": 2}')
        with self.assertRaisesRegex(ValueError, "Duplicate JSON"):
            Inputs(self.root).read("duplicate.json")
        with patch.object(Inputs, "unchanged", return_value=False), self.assertRaisesRegex(ValueError, "changed during"):
            summarize(self.root)

    def make_diagnostic(self):
        root = self.root.parent / "diagnostic"
        root.mkdir()
        self.diagnostic_root = root
        def write(name, value):
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(value))
        def sha(name):
            return hashlib.sha256((root / name).read_bytes()).hexdigest()
        evaluator = b"raise RuntimeError('Evaluator must not execute')\n"
        (root / "evaluator.py").write_bytes(evaluator)
        source = self.root / "source-snapshot/gossip_harness/benchmark_diagnostics.py"
        source.write_bytes(evaluator)
        report = self.read("results.json")
        contract = report["contract"]
        contract["sources"]["benchmark_diagnostics.py"] = self.sha(source.relative_to(self.root))
        contract_sha = digest(contract)
        report["contract_sha"] = contract_sha
        self.change("preregistered.json", lambda row: row.update(contract=contract))
        self.change("frozen-trajectories.json", lambda row: row.update(contract_sha=contract_sha))
        freeze_sha = self.sha("frozen-trajectories.json")
        report["freeze_manifest_sha256"] = freeze_sha
        for case in report["cases"]:
            case["freeze_manifest_sha256"] = freeze_sha
            self.write(f"{case['run_id']}/result.json", case)
        self.write("results.json", report)
        self.change("independent-audit.json", lambda row: row.update(contract_sha256=contract_sha, freeze_manifest_sha256=freeze_sha))
        self.refresh()
        groups, plan_groups = [], []
        for project in PROJECTS:
            for stage in range(2):
                quality = dict(source_id="source-a", requirements_passed=1, requirements_total=2, private_perfect=False)
                pools = []
                for policy in POLICIES:
                    for rep in range(POLICY_CONTRACT[policy]["repetitions"]):
                        for phase in ("initial", "final"):
                            final = phase == "final"
                            selector = dict(quality=quality if final else None, requirement_selection_regret=1 if final else None,
                                            correct_available_but_not_selected=final)
                            pools.append(dict(run_id=f"{project}-{policy}-{rep}", policy=policy, repetition=rep, phase=phase,
                                requirements_total=2, correct_eligible_public_passing_source_available=final,
                                actual_final_selection_in_this_pool=final, actual_same_pool_requirement_regret=1 if final else None,
                                actual_same_pool_correct_selection_miss=True if final else None,
                                counterfactuals={scope: {name: deepcopy(selector) for name in ("public_only", "scout", "reviewer", "combined")}
                                                for scope in ("own_history", "pooled_same_project_stage")}))
                group = dict(project_id=project, stage_index=stage, qualified=True, pools=pools,
                    faults=[dict(id="fault-a", family="atomicity", public_survived=True, witness_killed=True),
                            dict(id="fault-b", family="atomicity", public_survived=True, witness_killed=True),
                            dict(id="fault-c", family="other", public_survived=False, witness_killed=True)],
                    generated_probes=[dict(id="probe-a", origins=[dict(source="scout"), dict(source="reviewer"), dict(source="scout")],
                        killed_public_surviving_families=["atomicity"], killed_public_surviving_fault_ids=["fault-a", "fault-b"], discriminates_retained_candidates=True),
                        dict(id="probe-b", origins=[dict(source="scout")], killed_public_surviving_families=["atomicity"],
                             killed_public_surviving_fault_ids=["fault-a", "fault-b"], discriminates_retained_candidates=True)])
                groups.append(group)
                plan_groups.append(dict(project_id=project, stage_index=stage, sources={"source-a": {}, "source-b": {}}))
        plan = dict(protocol="evidence-frontier-diagnostic-v1", primary_run=str(self.root), groups=plan_groups,
            input_primary_results_sha256=self.sha("results.json"), primary_contract_sha256=contract_sha,
            global_freeze_sha256=freeze_sha, input_timings_sha256=self.sha("timings.json"),
            input_hashes={name: self.sha(name) for name in ("results.json", "timings.json", "frozen-trajectories.json")},
            diagnostic_source_sha256=sha("evaluator.py"), frozen_diagnostic_source_sha256=sha("evaluator.py"),
            freeze_monotonic_ns=300_000_000_000, invocation_count=0)
        write("plan.json", plan)
        rows = []
        for group in plan_groups:
            for source_id in group["sources"]:
                job = f"{group['project_id']}-stage-{group['stage_index']}-{source_id}"
                receipt_path = f"receipts/{job}.json"
                write(receipt_path, dict(passed=True))
                ordinal = len(rows)
                row = dict(job_id=job, project_id=group["project_id"], stage_index=group["stage_index"], source_id=source_id,
                    receipt_path=receipt_path, receipt_sha256=sha(receipt_path), status="verified", physical_execution=True,
                    evaluation_attempted=True, started_monotonic_ns=(400 + ordinal) * 1_000_000_000,
                    finished_monotonic_ns=(402 + ordinal) * 1_000_000_000, elapsed_seconds=2)
                rows.append(row)
                write(f"rows/{job}.json", row)
        count = len(rows)
        write("receipt-index.json", dict(protocol=plan["protocol"], plan_sha256=sha("plan.json"), invocation_count=0,
            planned_jobs=count, evaluation_attempts=count, confirmed_candidate_executions=count, rows=rows))
        write("results.json", dict(protocol=plan["protocol"], plan_sha256=sha("plan.json"), invocation_count=0,
            status="finished", primary_scores_changed=False, inputs_unchanged=True, physical_candidate_executions=count,
            groups=groups, limitations=["Synthetic test data, no candidate execution."]))
        write("audit.json", dict(protocol="evidence-frontier-diagnostic-audit-v1", passed=True, inputs_unchanged=True,
            plan_sha256=sha("plan.json"), results_sha256=sha("results.json"), receipt_index_sha256=sha("receipt-index.json"),
            input_primary_results_sha256=plan["input_primary_results_sha256"], primary_contract_sha256=contract_sha,
            global_freeze_sha256=freeze_sha, input_timings_sha256=plan["input_timings_sha256"], invocation_count=0,
            verified_matrix_rows=count, audited_groups=4))
        return root

    def diagnostic_change(self, name, edit, *, refresh_certificate=False):
        path = self.diagnostic_root / name
        data = json.loads(path.read_text())
        edit(data)
        path.write_text(json.dumps(data))
        if refresh_certificate:
            audit = self.diagnostic_root / "audit.json"
            certificate = json.loads(audit.read_text())
            certificate.update(results_sha256=hashlib.sha256((self.diagnostic_root / "results.json").read_bytes()).hexdigest(),
                receipt_index_sha256=hashlib.sha256((self.diagnostic_root / "receipt-index.json").read_bytes()).hexdigest())
            audit.write_text(json.dumps(certificate))

    def test_separate_diagnostic_summaries_deduplicate_families_and_origins(self):
        root = self.make_diagnostic()
        result = summarize(self.root, diagnostics=root)
        diagnostic = result["diagnostics"]
        self.assertEqual(diagnostic["status"], "separately_certified")
        self.assertEqual(diagnostic["physical_candidate_executions"], 8)
        roles = diagnostic["groups"][0]["evidence_by_origin_role"]
        self.assertEqual((roles["scout"]["unique_probe_inputs"], roles["scout"]["families_detected_count"], roles["scout"]["public_surviving_family_count"]), (2, 1, 1))
        self.assertEqual(roles["scout"]["duplicate_nonempty_fault_kill_vectors"], 1)
        self.assertEqual(roles["reviewer"]["unique_probe_inputs"], 1)
        self.assertEqual(roles["combined"]["families_detected_count"], 1)
        self.assertEqual(roles["scout"]["families_not_detected_by_reviewer"], [])
        self.assertEqual(diagnostic["diagnostic_validation_timing"]["cumulative_seconds"], 16)
        self.assertEqual(diagnostic["diagnostic_validation_timing"]["union_wall_seconds"], 9)
        self.assertEqual(result["trajectories"][0]["timing"]["total_active_seconds"], 25)
        additional = result["bindings"]["additional_inputs"][0]["inputs_sha256"]
        self.assertIn("audit.json", additional)
        self.assertTrue(any(name.startswith("receipts/") for name in additional))

    def test_diagnostic_initial_final_null_regret_and_abstention_stay_distinct(self):
        root = self.make_diagnostic()
        phases = summarize(self.root, diagnostics=root)["diagnostics"]["groups"][0]["policies"][MATCHED_POLICIES[0]]["phases"]
        initial, final = phases["initial"], phases["final"]
        self.assertIsNone(initial["actual_mean_same_pool_requirement_regret_fraction"])
        self.assertEqual(initial["actual_regret_undefined_observations"], 2)
        self.assertEqual(final["actual_mean_same_pool_requirement_regret_fraction"], .5)
        for scope in ("own_history", "pooled_same_project_stage"):
            self.assertEqual(initial["counterfactuals"][scope]["scout"]["abstentions"], 2)
            self.assertIsNone(initial["counterfactuals"][scope]["scout"]["mean_requirement_regret_fraction"])
            self.assertEqual(final["counterfactuals"][scope]["scout"]["available_perfect_but_not_selected"], 2)

    def test_diagnostic_needs_separate_successful_certificate(self):
        root = self.make_diagnostic()
        self.diagnostic_change("audit.json", lambda value: value.update(passed=False))
        with self.assertRaisesRegex(ValueError, "separately certified"):
            summarize(self.root, diagnostics=root)

    def test_diagnostic_qualification_failure_is_not_zero_utility(self):
        root = self.make_diagnostic()
        self.diagnostic_change("results.json", lambda value: value.update(status="qualification_failed"), refresh_certificate=True)
        with self.assertRaisesRegex(ValueError, "separately certified"):
            summarize(self.root, diagnostics=root)

    def test_diagnostic_exact_results_plan_and_index_bindings(self):
        root = self.make_diagnostic()
        original = (root / "audit.json").read_bytes()
        for key in ("results_sha256", "plan_sha256", "receipt_index_sha256"):
            (root / "audit.json").write_bytes(original)
            self.diagnostic_change("audit.json", lambda value: value.update({key: "wrong"}))
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "bind exact artifacts"):
                summarize(self.root, diagnostics=root)

    def test_diagnostic_cannot_belong_to_another_primary_freeze(self):
        root = self.make_diagnostic()
        self.diagnostic_change("audit.json", lambda value: value.update(global_freeze_sha256="wrong"))
        with self.assertRaisesRegex(ValueError, "different primary evidence"):
            summarize(self.root, diagnostics=root)

    def test_diagnostic_receipt_tamper_and_duplicate_row_rejected(self):
        root = self.make_diagnostic()
        first = next((root / "receipts").glob("*.json"))
        original = first.read_bytes()
        first.write_bytes(original + b"\n")
        with self.assertRaisesRegex(ValueError, "receipt bytes changed"):
            summarize(self.root, diagnostics=root)
        first.write_bytes(original)
        self.diagnostic_change("receipt-index.json", lambda value: value["rows"].__setitem__(1, value["rows"][0]), refresh_certificate=True)
        with self.assertRaisesRegex(ValueError, "execution roster/count"):
            summarize(self.root, diagnostics=root)

    def test_case_and_pair_boolean_repetition_cannot_alias_integer(self):
        rows = summarize(self.root)["trajectories"]
        rows[0]["repetition"] = False
        with self.assertRaisesRegex(ValueError, "repetition type"):
            paired_comparison(rows, "quality_ordered")
        self.change("results.json", lambda row: row["cases"][0].update(repetition=False))
        self.refresh()
        with self.assertRaisesRegex(ValueError, "roster mismatch"):
            summarize(self.root)


def CounterLike(rows):
    return {key: sum(row["execution_status"] == key for row in rows) for key in {row["execution_status"] for row in rows}}


if __name__ == "__main__":
    unittest.main()
