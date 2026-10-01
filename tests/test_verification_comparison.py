import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from analysis.verification_comparison import BoundInputs, digest, distribution, matchup, rankings, summarize


class VerificationComparisonTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.build_run()

    def write(self, name, value):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value))

    def mutate(self, name, edit):
        value = json.loads((self.root / name).read_text())
        edit(value)
        self.write(name, value)

    def build_run(self):
        source = b"raise RuntimeError('Frozen source must never be imported')\n"
        source_path = self.root / "source-snapshot/gossip_harness/frozen.py"
        source_path.parent.mkdir(parents=True)
        source_path.write_bytes(source)
        plan = b"{}\n"
        (self.root / "study-plan.json").write_bytes(plan)
        projects = [dict(id=project, stages=[dict(hidden_cases=[
            dict(id=f"{project}-one", requirement="first"),
            dict(id=f"{project}-two", requirement="second")])]) for project in ("a", "b")]
        policies = ["strong", "cheap", "portfolio"]
        contract = dict(policies=policies, sources={"frozen.py": hashlib.sha256(source).hexdigest()},
                        plan_sha256=hashlib.sha256(plan).hexdigest(), fixture_sha256=digest(projects))
        roster = [(project, policy, rep) for project in ("a", "b") for policy in policies for rep in range(2)]
        cases, audit_cases, summaries = [], [], {}
        for project, policy, rep in roster:
            run_id = f"{project}-{policy}-{rep}"
            accepted = policy == "strong"
            outcomes = [dict(id=f"{project}-one", requirement="first", passed=True),
                        dict(id=f"{project}-two", requirement="second", passed=accepted)]
            reservation = f"{run_id}/call"
            case = dict(run_id=run_id, project_id=project, policy=policy, repetition=rep,
                accepted=accepted, usage_micro_usd=100, milestones_completed=1,
                status="accepted" if accepted else "final_quality_failed", elapsed_seconds=30 + rep,
                final_hidden=dict(outcomes=outcomes), requirement_coverage=dict(first=True, second=accepted),
                invocations=[dict(reservation=reservation)])
            cases.append(case)
            self.write(f"{run_id}/result.json", case)
            self.write(f"{run_id}/final-private-receipt.json", case["final_hidden"])
            self.write(f"{run_id}/stage-0/journal/call.result.json", dict(payload=dict(metadata=dict(model="test"))))
            (self.root / run_id / "trace-1.jsonl").write_text(json.dumps(dict(kind="provider_dispatch", reservation=reservation)) + "\n")
            audit_cases.append(dict(run_id=run_id, accepted=accepted, usage_micro_usd=100, requests=1,
                                   stages=[dict(completed=True)],
                                   private_failed_case_ids=[] if accepted else [f"{project}-two"]))
        for policy in policies:
            selected = [row for row in cases if row["policy"] == policy]
            summaries[policy] = dict(evaluated_trajectories=4, accepted=sum(row["accepted"] for row in selected),
                                     milestones_completed=4, cost_micro_usd=400, invocations=4)
        report = dict(status="finished", active_case=None, censored=[], unexecuted=[], contract=contract,
                      contract_sha=digest(contract), cases=cases, repetitions=2, incremental_micro_usd=1200)
        self.write("results.json", report)
        self.write("preregistered.json", dict(contract=contract, roster=roster))
        self.write("fixtures.json", dict(projects=projects))
        self.write("descriptive-summary.json", dict(status="finished", contract_sha=digest(contract), policies=summaries))
        self.write("independent-audit.json", dict(certified=True, passed=True, status="finished",
            finalized_cases_passed=True, unaudited_work=dict(active_case=None, censored=[], unexecuted=[]),
            contract_sha256=digest(contract), cases=audit_cases, verified_requests=12))

    def test_complete_analysis_has_separate_rankings_missing_provider_time_and_bindings(self):
        result = summarize(self.root)
        self.assertEqual(result["rankings"]["whole_project_acceptance"], [
            dict(policy="strong", rank=1, value=1.0), dict(policy="cheap", rank=2, value=0.0),
            dict(policy="portfolio", rank=2, value=0.0)])
        self.assertEqual(result["derived_comparisons_per_policy_pair"], 8)
        self.assertEqual(result["pairwise"]["whole_project_acceptance"][0]["wins"], 8)
        self.assertEqual(result["index_pairing_sensitivity"]["whole_project_acceptance"][0]["wins"], 4)
        self.assertIsNone(result["timing"]["provider_latency_seconds"])
        self.assertIsNone(result["elo"]["value"])
        self.assertEqual(result["timing"]["journal_results_inspected"], 12)
        self.assertTrue(result["bindings"]["inputs_unchanged"])
        self.assertIn("results.json", result["bindings"]["inputs_sha256"])

    def test_no_success_timing_is_undefined_not_zero(self):
        result = summarize(self.root)
        timing = result["policies"]["cheap"]["timing"]["accepted_only"]
        self.assertEqual(timing["observations"], 0)
        self.assertIsNone(timing["median_seconds"])
        self.assertIsNone(timing["sum_seconds"])

    def test_missing_times_remain_missing(self):
        self.assertEqual(distribution([None, 4, 8])["median_seconds"], 6)
        self.assertEqual(distribution([None, 4, 8])["missing"], 1)
        self.assertIsNone(distribution([None])["sum_seconds"])

    def test_invalid_durations_rejected(self):
        for value in (-1, True, float("nan"), float("inf"), "4"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                distribution([value])

    def test_rank_ties_preserved(self):
        result = rankings({"c": 0, "b": 2, "a": 2}, lambda value: value)
        self.assertEqual([row["rank"] for row in result], [1, 1, 3])

    def test_cross_comparisons_do_not_depend_on_repetition_labels(self):
        rows = []
        for policy, values in (("a", [2, 0]), ("b", [1, 0])):
            for rep, value in enumerate(values):
                rows.append(dict(project_id="p", policy=policy, repetition=rep, run_id=f"{policy}{rep}",
                                 requirements_total=2, requirements_passed=value))
        before = matchup(rows, "a", "b", "requirements_passed")
        self.assertEqual((before["wins"], before["draws"], before["losses"]), (2, 1, 1))
        for row in rows:
            if row["policy"] == "b":
                row["repetition"] = 1 - row["repetition"]
        after = matchup(rows, "a", "b", "requirements_passed")
        self.assertEqual(before["win_score"], after["win_score"])
        self.assertEqual(before["source_trajectories_per_policy"], 2)
        self.assertEqual(before["independent_application_identities"], 1)

    def test_acceptance_draws_do_not_use_requirement_tie_breaker(self):
        rows = [dict(project_id="p", repetition=0, policy=policy, run_id=policy,
                     accepted=False, requirements_total=2, requirements_passed=value)
                for policy, value in (("a", 2), ("b", 1))]
        self.assertEqual(matchup(rows, "a", "b", "accepted")["draws"], 1)
        self.assertEqual(matchup(rows, "a", "b", "requirements_passed")["wins"], 1)

    def test_incomplete_or_duplicate_match_rejected(self):
        row = dict(project_id="p", repetition=0, policy="a", run_id="a", requirements_total=2, accepted=True)
        for rows in ([row], [row, row]):
            with self.assertRaises(ValueError):
                matchup(rows, "a", "b", "accepted")

    def test_mismatched_requirement_denominators_rejected(self):
        rows = [dict(project_id="p", repetition=0, policy=policy, run_id=policy,
                     requirements_total=total, accepted=True) for policy, total in (("a", 2), ("b", 3))]
        with self.assertRaisesRegex(ValueError, "denominators"):
            matchup(rows, "a", "b", "accepted")

    def test_unfinished_or_censored_run_rejected(self):
        for field, value in (("status", "running"), ("active_case", {}), ("censored", ["x"]), ("unexecuted", ["x"])):
            original = (self.root / "results.json").read_bytes()
            self.mutate("results.json", lambda data: data.update({field: value}))
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "complete"):
                summarize(self.root)
            (self.root / "results.json").write_bytes(original)

    def test_uncertified_audit_rejected(self):
        self.mutate("independent-audit.json", lambda data: data.update(certified=False))
        with self.assertRaisesRegex(ValueError, "certified"):
            summarize(self.root)

    def test_frozen_source_drift_rejected(self):
        (self.root / "source-snapshot/gossip_harness/frozen.py").write_text("changed")
        with self.assertRaisesRegex(ValueError, "source hash"):
            summarize(self.root)

    def test_duplicate_or_missing_roster_rejected(self):
        self.mutate("preregistered.json", lambda data: data["roster"].append(data["roster"][0]))
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            summarize(self.root)

    def test_per_run_result_drift_rejected(self):
        self.mutate("a-strong-0/result.json", lambda data: data.update(elapsed_seconds=999))
        with self.assertRaisesRegex(ValueError, "Per-run result"):
            summarize(self.root)

    def test_audit_disagreement_rejected(self):
        self.mutate("independent-audit.json", lambda data: data["cases"][0].update(accepted=False))
        with self.assertRaisesRegex(ValueError, "Audit result"):
            summarize(self.root)

    def test_audit_stage_completion_disagreement_rejected(self):
        self.mutate("independent-audit.json", lambda data: data["cases"][0]["stages"][0].update(completed=False))
        with self.assertRaisesRegex(ValueError, "Audit result"):
            summarize(self.root)

    def test_descriptive_summary_disagreement_rejected(self):
        self.mutate("descriptive-summary.json", lambda data: data["policies"]["strong"].update(cost_micro_usd=0))
        with self.assertRaisesRegex(ValueError, "summary disagrees"):
            summarize(self.root)

    def test_duplicate_dispatch_rejected(self):
        path = self.root / "a-strong-0/trace-1.jsonl"
        path.write_text(path.read_text() * 2)
        with self.assertRaisesRegex(ValueError, "Duplicate provider dispatch"):
            summarize(self.root)

    def test_input_mutation_during_analysis_rejected(self):
        with patch.object(BoundInputs, "unchanged", return_value=False), self.assertRaisesRegex(ValueError, "changed during"):
            summarize(self.root)

    def test_path_escape_rejected(self):
        with self.assertRaisesRegex(ValueError, "escapes"):
            BoundInputs(self.root).raw("../outside.json")


if __name__ == "__main__":
    unittest.main()
