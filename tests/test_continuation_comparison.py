import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from analysis.continuation_comparison import (
    Inputs, POLICIES, PROTOCOL, cross_comparison, digest, distribution, interval_summary, summarize,
)


class ContinuationComparisonTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.make_cohort()

    def write(self, path, value):
        path = self.root / path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value))

    def read(self, path):
        return json.loads((self.root / path).read_text())

    def sha(self, path):
        return hashlib.sha256((self.root / path).read_bytes()).hexdigest()

    def change(self, path, edit):
        value = self.read(path)
        edit(value)
        self.write(path, value)

    def refresh_audit_inputs(self):
        self.change("independent-audit.json", lambda data: data.update(
            results_sha256=self.sha("results.json"), timings_sha256=self.sha("timings.json")))

    def make_cohort(self):
        source = "raise RuntimeError('Source must never execute')\n"
        source_path = self.root / "source-snapshot/gossip_harness/frozen.py"
        source_path.parent.mkdir(parents=True)
        source_path.write_text(source)
        (self.root / "study-plan.json").write_text("{}\n")
        project = dict(id="transport", stages=[dict(hidden_cases=[dict(id=f"private-{stage}", requirement=f"r{stage}")])
                                               for stage in range(2)])
        roster = [[project["id"], policy, rep] for policy in POLICIES for rep in range(2)]
        contract = dict(protocol=PROTOCOL, project_id=project["id"], policies=list(POLICIES), repetitions=2,
            milestones=2, roster=roster, sources={"frozen.py": self.sha("source-snapshot/gossip_harness/frozen.py")},
            plan_sha256=self.sha("study-plan.json"), fixture_sha256=digest(project))
        self.write("fixtures.json", dict(projects=[project]))
        self.write("preregistered.json", dict(contract=contract, roster=roster))
        spans, terminal, frozen, cases, audit_cases = [], [], [], [], []

        def span(kind, start, end, **labels):
            row = dict(span_id=len(spans), kind=kind, clock_id="one-clock", status="finished",
                started_monotonic_ns=int(start * 1e9), finished_monotonic_ns=int(end * 1e9),
                elapsed_seconds=end - start, **labels)
            spans.append(row)
            return row

        for ordinal, (project_id, policy, rep) in enumerate(roster):
            run_id = f"{project_id}-{policy}-{rep}"
            start, end = ordinal * 30, ordinal * 30 + 20
            model = span("model_trajectory", start, end, run_id=run_id)
            stages, calls = [], []
            for stage in range(2):
                current = []
                requests = [("builder", 3, 4), ("reviewer", 5, 6)]
                if policy == "scout-assisted":
                    requests = [("scout", 1, 2), ("scout", 1.5, 2.5)] + requests
                for index, (role, a, b) in enumerate(requests):
                    call_id = f"{role}-{stage}-{index}"
                    physical = span("physical_provider_request", start + stage * 10 + a, start + stage * 10 + b,
                                    run_id=run_id, role=role, model="cheap" if role == "scout" else "strong", call_id=call_id)
                    call = dict(call_id=call_id, role=role, model=physical["model"], physical_dispatch=True,
                                physical_span_id=physical["span_id"], usage_units=10)
                    calls.append(call)
                    current.append(call)
                span("docker_validation", start + stage * 10 + 7, start + stage * 10 + 8,
                     run_id=run_id, purpose="stage_evidence")
                scouts = dict(calls=2, admitted=2, rejected=1) if policy == "scout-assisted" else dict(calls=0, admitted=0, rejected=0)
                stages.append(dict(completed=True, scouts=scouts, invocations=current))
            state = dict(stages=stages)
            self.write(f"{run_id}/trajectory.json", state)
            trajectory_sha = self.sha(f"{run_id}/trajectory.json")
            terminal.append(dict(run_id=run_id, trajectory_sha256=trajectory_sha,
                                 span_id=model["span_id"], elapsed_seconds=model["elapsed_seconds"]))
            frozen.append(dict(run_id=run_id, project_id=project_id, policy=policy, repetition=rep,
                               trajectory_sha256=trajectory_sha))
            accepted = policy == "scout-assisted" or rep == 0
            outcomes = [dict(id="private-0", requirement="r0", passed=True),
                        dict(id="private-1", requirement="r1", passed=accepted)]
            receipt = dict(outcomes=outcomes)
            self.write(f"{run_id}/final-private-receipt.json", receipt)
            case = dict(run_id=run_id, project_id=project_id, policy=policy, repetition=rep,
                accepted=accepted, status="accepted" if accepted else "final_quality_failed", milestones_completed=2,
                trajectory_sha256=trajectory_sha, final_hidden=receipt, requirement_coverage=dict(r0=True, r1=accepted),
                physical_provider_calls=len(calls), invocations=calls, usage_micro_usd=len(calls) * 10,
                scouts={key: sum(stage["scouts"][key] for stage in stages) for key in ("calls", "admitted", "rejected")},
                metrics=dict(repairs=1))
            cases.append(case)
            self.write(f"{run_id}/result.json", case)
            final_start = 130 + ordinal * 10
            span("final_evaluation", final_start, final_start + 5, run_id=run_id)
            for purpose, offset in (("final_visible", 0), ("final_private", 2)):
                span("docker_validation", final_start + offset, final_start + offset + 2,
                     run_id=run_id, purpose=purpose)
            audit_cases.append(dict(run_id=run_id, accepted=accepted,
                                    stages=[dict(completed=True), dict(completed=True)]))
        freeze = dict(protocol=PROTOCOL, contract_sha=digest(contract), trajectories=frozen,
                      frozen_monotonic_ns=120_000_000_000, private_evaluation_started=False)
        self.write("frozen-trajectories.json", freeze)
        self.write("timings.json", dict(clock_id="one-clock", spans=spans))
        cost = sum(row["usage_micro_usd"] for row in cases)
        report = dict(experiment=PROTOCOL, mode="live", status="finished", phase="finished", active_case=None,
            censored=[], unexecuted=[], contract=contract, contract_sha=digest(contract), cases=cases,
            trajectories=terminal, freeze_manifest_sha256=self.sha("frozen-trajectories.json"), incremental_micro_usd=cost)
        self.write("results.json", report)
        self.write("independent-audit.json", dict(protocol="independent-continuation-audit-v1", passed=True,
            results_sha256=self.sha("results.json"), timings_sha256=self.sha("timings.json"), contract_sha256=digest(contract),
            freeze_manifest_sha256=self.sha("frozen-trajectories.json"), cases=audit_cases,
            accounting=dict(study_usage_micro_usd=cost)))

    def test_complete_cohort_keeps_acceptance_and_coverage_separate(self):
        result = summarize(self.root)
        self.assertEqual(result["policies"]["strong-maintainer"]["accepted"], 1)
        self.assertEqual(result["policies"]["scout-assisted"]["accepted"], 2)
        self.assertEqual(result["policies"]["scout-assisted"]["cost_by_role_micro_usd"],
                         dict(scout=80, builder=40, reviewer=40))
        comparison = result["comparisons"]["whole_project_acceptance"]
        self.assertEqual((comparison["wins"], comparison["draws"], comparison["losses"]), (0, 2, 2))
        self.assertEqual(comparison["derived_comparisons"], 4)
        self.assertIsNone(result["elo"])
        self.assertIsNone(result["cross_cohort_ranking"])
        self.assertTrue(result["bindings"]["inputs_unchanged"])

    def test_barrier_waits_are_separate_from_active_time(self):
        row = summarize(self.root)["trajectories"][0]["timing"]
        self.assertEqual(row["model_trajectory_active_seconds"], 20)
        self.assertEqual(row["final_evaluation_active_seconds"], 5)
        self.assertEqual(row["total_active_seconds"], 25)
        self.assertEqual(row["wait_to_global_freeze_seconds"], 100)
        self.assertEqual(row["post_freeze_evaluation_queue_seconds"], 10)
        self.assertEqual(row["total_active_seconds"] + row["total_wait_seconds"], row["start_to_final_outcome_seconds"])

    def test_provider_overlap_is_not_added_to_parent_time(self):
        row = summarize(self.root)["trajectories"][2]["timing"]
        provider = row["physical_provider"]
        self.assertEqual(provider["cumulative_seconds"], 8)
        self.assertEqual(provider["union_wall_seconds"], 7)
        self.assertEqual(provider["overlap_seconds"], 1)
        self.assertEqual(row["total_active_seconds"], 25)
        self.assertEqual(row["docker_validation"]["cumulative_seconds"], 6)
        self.assertTrue(row["docker_validation"]["nonoverlapping"])

    def test_empty_duration_distribution_is_not_zero_success_time(self):
        self.assertIsNone(distribution([])["median_seconds"])
        self.assertIsNone(distribution([])["cumulative_seconds"])

    def test_unclosed_negative_and_inconsistent_spans_rejected(self):
        span = self.read("timings.json")["spans"][0]
        for change in (dict(status="running"), dict(finished_monotonic_ns=-1), dict(elapsed_seconds=999)):
            with self.subTest(change=change), self.assertRaises(ValueError):
                interval_summary([{**span, **change}])

    def test_mixed_monotonic_clocks_rejected(self):
        span = self.read("timings.json")["spans"][0]
        with self.assertRaisesRegex(ValueError, "different monotonic"):
            interval_summary([span, {**span, "clock_id": "another"}])

    def test_nonoverlap_guard_and_interval_union(self):
        a = dict(clock_id="one", status="finished", started_monotonic_ns=0,
                 finished_monotonic_ns=3_000_000_000, elapsed_seconds=3)
        b = {**a, "started_monotonic_ns": 2_000_000_000, "finished_monotonic_ns": 5_000_000_000}
        result = interval_summary([a, b])
        self.assertEqual((result["cumulative_seconds"], result["union_wall_seconds"]), (6, 5))
        with self.assertRaisesRegex(ValueError, "nonoverlapping"):
            interval_summary([a, b], require_nonoverlap=True)

    def test_incomplete_or_censored_cohort_rejected(self):
        self.change("results.json", lambda row: row.update(censored=["partial"]))
        self.refresh_audit_inputs()
        with self.assertRaisesRegex(ValueError, "complete uncensored"):
            summarize(self.root)

    def test_audit_must_bind_exact_timing_bytes(self):
        path = self.root / "timings.json"
        path.write_text(path.read_text() + "\n")
        with self.assertRaisesRegex(ValueError, "exact results and timings"):
            summarize(self.root)

    def test_audit_must_be_successful(self):
        self.change("independent-audit.json", lambda row: row.update(passed=False))
        with self.assertRaisesRegex(ValueError, "certified audit"):
            summarize(self.root)

    def test_missing_case_cannot_be_certified(self):
        self.change("results.json", lambda row: row["cases"].pop())
        self.refresh_audit_inputs()
        with self.assertRaisesRegex(ValueError, "four-run roster"):
            summarize(self.root)

    def test_source_mutation_rejected(self):
        (self.root / "source-snapshot/gossip_harness/frozen.py").write_text("changed")
        with self.assertRaisesRegex(ValueError, "source identity"):
            summarize(self.root)

    def test_audited_milestone_disagreement_rejected(self):
        self.change("independent-audit.json", lambda row: row["cases"][0]["stages"][0].update(completed=False))
        with self.assertRaisesRegex(ValueError, "primary outcome"):
            summarize(self.root)

    def test_global_barrier_cannot_precede_model_completion(self):
        self.change("frozen-trajectories.json", lambda row: row.update(frozen_monotonic_ns=10_000_000_000))
        freeze_sha = self.sha("frozen-trajectories.json")
        self.change("results.json", lambda row: row.update(freeze_manifest_sha256=freeze_sha))
        self.change("independent-audit.json", lambda row: row.update(freeze_manifest_sha256=freeze_sha))
        self.refresh_audit_inputs()
        with self.assertRaisesRegex(ValueError, "global freeze barrier"):
            summarize(self.root)

    def test_scout_counts_reconciled_to_stage_records(self):
        run_id = self.read("results.json")["cases"][2]["run_id"]
        self.change("results.json", lambda row: row["cases"][2]["scouts"].update(admitted=100))
        self.write(f"{run_id}/result.json", self.read("results.json")["cases"][2])
        self.refresh_audit_inputs()
        with self.assertRaisesRegex(ValueError, "Scout aggregate"):
            summarize(self.root)

    def test_physical_request_labels_reconciled_to_calls(self):
        self.change("timings.json", lambda row: row["spans"][1].update(role="invented"))
        self.refresh_audit_inputs()
        with self.assertRaisesRegex(ValueError, "Physical provider labels"):
            summarize(self.root)

    def test_private_group_coverage_is_not_case_fraction(self):
        rows = [dict(policy=policy, project_id="p", requirements_total=2,
                     requirements_passed=count, accepted=False, run_id=f"{policy}{rep}")
                for policy, counts in zip(POLICIES, ([2, 1], [1, 1])) for rep, count in enumerate(counts)]
        self.assertEqual(cross_comparison(rows, "accepted")["draws"], 4)
        self.assertEqual(cross_comparison(rows, "requirements_passed")["wins"], 2)
        self.assertEqual(cross_comparison(rows, "requirements_passed")["draws"], 2)

    def test_inputs_cannot_change_mid_read(self):
        with patch.object(Inputs, "unchanged", return_value=False), self.assertRaisesRegex(ValueError, "changed during"):
            summarize(self.root)


if __name__ == "__main__":
    unittest.main()
