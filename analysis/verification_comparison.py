"""Read-only, post-freeze comparisons; no candidate imports, execution, or API calls.

Ranks describe this small retained study. The two independent application
identities do not support a calibrated population Elo or superiority claim.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from fractions import Fraction
import hashlib
import itertools
import json
import math
from pathlib import Path
import statistics


VERSION = "verification-comparison-v1"


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=True,
                                    allow_nan=False, separators=(",", ":")).encode()).hexdigest()


def number(value, label):
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
        raise ValueError(f"Invalid nonnegative finite {label}")
    return value


def distribution(values):
    """Missing times remain missing, and no-success groups remain empty."""
    present = [number(value, "duration") for value in values if value is not None]
    return dict(observations=len(values), measured=len(present), missing=len(values) - len(present),
                median_seconds=statistics.median(present) if present else None,
                min_seconds=min(present) if present else None,
                max_seconds=max(present) if present else None,
                sum_seconds=sum(present) if present else None)


def matchup(rows, left, right, field, *, index_paired=False):
    by_pair = defaultdict(dict)
    for row in rows:
        key = row["project_id"], row["repetition"]
        if row["policy"] in by_pair[key]:
            raise ValueError("Duplicate matched policy trajectory")
        by_pair[key][row["policy"]] = row
    for policies in by_pair.values():
        if left not in policies or right not in policies:
            raise ValueError("Incomplete policy matchup")
    matches = []
    pairs = [(values[left], values[right]) for _, values in sorted(by_pair.items())]
    if not index_paired:
        pairs = [(a, b) for a in rows if a["policy"] == left
                 for b in rows if b["policy"] == right and a["project_id"] == b["project_id"]]
    for a, b in pairs:
        if a["requirements_total"] != b["requirements_total"]:
            raise ValueError("Matched requirement denominators differ")
        av, bv = a[field], b[field]
        outcome = "win" if av > bv else "loss" if av < bv else "draw"
        matches.append(dict(project_id=a["project_id"], left_repetition=a["repetition"],
                            right_repetition=b["repetition"], left_run=a["run_id"],
                            right_run=b["run_id"], left_value=av, right_value=bv, outcome=outcome))

    def score(group):
        wins = sum(row["outcome"] == "win" for row in group)
        draws = sum(row["outcome"] == "draw" for row in group)
        losses = sum(row["outcome"] == "loss" for row in group)
        return dict(pairs=len(group), wins=wins, draws=draws, losses=losses,
                    win_score=(wins + 0.5 * draws) / len(group) if group else None)

    projects = sorted({row["project_id"] for row in matches})
    by_project = {project: score([row for row in matches if row["project_id"] == project])
                  for project in projects}
    omitted = {project: score([row for row in matches if row["project_id"] != project])["win_score"]
               for project in projects}
    sensitivity = [value for value in omitted.values() if value is not None]
    project_scores = [value["win_score"] for value in by_project.values()]
    return dict(left_policy=left, right_policy=right, metric=field, **score(matches),
                comparison_design="same_repetition_index_sensitivity" if index_paired else "all_within_application_cross_comparisons",
                source_trajectories_per_policy=len(by_pair), independent_application_identities=len(projects),
                equal_project_win_score=sum(project_scores) / len(project_scores) if project_scores else None,
                project_score_range=[min(project_scores), max(project_scores)] if project_scores else None,
                by_project=by_project, matches=matches,
                leave_one_project_out=dict(scores=omitted,
                    min_score=min(sensitivity) if sensitivity else None,
                    max_score=max(sensitivity) if sensitivity else None,
                    interpretation="Descriptive project sensitivity, not a confidence interval."))


def rankings(policies, key):
    """Competition ranks preserve ties, using exact rational coverage."""
    values = {policy: key(row) for policy, row in policies.items()}
    return [{"policy": policy, "rank": 1 + sum(other > value for other in values.values()),
             "value": float(value)} for policy, value in sorted(values.items(), key=lambda item: (-item[1], item[0]))]


class BoundInputs:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.hashes = {}

    def raw(self, relative):
        path = self.root / relative
        resolved = path.resolve()
        if not resolved.is_relative_to(self.root) or path.is_symlink():
            raise ValueError("Input path escapes the retained run")
        raw = path.read_bytes()
        self.hashes[str(relative)] = hashlib.sha256(raw).hexdigest()
        return raw

    def read(self, relative):
        return json.loads(self.raw(relative))

    def unchanged(self):
        return all(hashlib.sha256((self.root / path).read_bytes()).hexdigest() == sha
                   for path, sha in self.hashes.items())


def summarize(root):
    inputs = BoundInputs(root)
    report = inputs.read("results.json")
    audit = inputs.read("independent-audit.json")
    prior = inputs.read("descriptive-summary.json")
    registered = inputs.read("preregistered.json")
    fixtures = inputs.read("fixtures.json")
    if (report["status"] != "finished" or report.get("active_case") is not None
            or report.get("censored") or report.get("unexecuted")):
        raise ValueError("A complete, uncensored, frozen run is required")
    if (audit.get("certified") is not True or audit.get("passed") is not True
            or audit.get("status") != "finished" or not audit.get("finalized_cases_passed")
            or audit.get("unaudited_work") != dict(active_case=None, censored=[], unexecuted=[])):
        raise ValueError("A complete certified independent audit is required")
    contract = report["contract"]
    contract_sha = digest(contract)
    if (report["contract_sha"] != contract_sha or audit["contract_sha256"] != contract_sha
            or prior["contract_sha"] != contract_sha or registered["contract"] != contract):
        raise ValueError("Contract identity mismatch")
    if prior["status"] != "finished" or digest(fixtures["projects"]) != contract["fixture_sha256"]:
        raise ValueError("Fixture or descriptive summary mismatch")
    for name, expected in contract["sources"].items():
        relative = f"source-snapshot/gossip_harness/{name}"
        if hashlib.sha256(inputs.raw(relative)).hexdigest() != expected:
            raise ValueError("Frozen source hash mismatch")
    if hashlib.sha256(inputs.raw("study-plan.json")).hexdigest() != contract["plan_sha256"]:
        raise ValueError("Frozen study plan hash mismatch")
    roster = [tuple(value) for value in registered["roster"]]
    if len(roster) != len(set(roster)):
        raise ValueError("Duplicate preregistered trajectory")
    identities = [(case["project_id"], case["policy"], case["repetition"]) for case in report["cases"]]
    if sorted(identities) != sorted(roster):
        raise ValueError("Final trajectories do not match the registered roster")
    policies = list(contract["policies"])
    projects = {project["id"]: project for project in fixtures["projects"]}
    expected_roster = {(project, policy, rep) for project in projects for policy in policies
                       for rep in range(report["repetitions"])}
    if set(roster) != expected_roster:
        raise ValueError("Study roster is not balanced and complete")
    audit_cases = {case["run_id"]: case for case in audit["cases"]}
    if len(audit_cases) != len(report["cases"]):
        raise ValueError("Audited trajectory roster mismatch")
    rows = []
    journal_fields = defaultdict(set)
    dispatches = set()
    total_journals = 0
    for case in report["cases"]:
        run_id = case["run_id"]
        if Path(run_id).name != run_id or run_id in (".", ".."):
            raise ValueError("Unsafe run ID")
        if inputs.read(f"{run_id}/result.json") != case:
            raise ValueError("Per-run result differs from aggregate result")
        if inputs.read(f"{run_id}/final-private-receipt.json") != case["final_hidden"]:
            raise ValueError("Private receipt differs from aggregate result")
        outcomes = case["final_hidden"]["outcomes"]
        expected_cases = [item for stage in projects[case["project_id"]]["stages"]
                          for item in stage["hidden_cases"]]
        expected_keys = [(item["id"], item["requirement"]) for item in expected_cases]
        if [(item["id"], item["requirement"]) for item in outcomes] != expected_keys:
            raise ValueError("Private suite identity/order mismatch")
        coverage = defaultdict(list)
        for outcome in outcomes:
            if type(outcome["passed"]) is not bool:
                raise ValueError("Nonboolean private outcome")
            coverage[outcome["requirement"]].append(outcome["passed"])
        grouped = {name: all(values) for name, values in coverage.items()}
        if grouped != case["requirement_coverage"]:
            raise ValueError("Requirement coverage does not match private outcomes")
        if type(case["accepted"]) is not bool:
            raise ValueError("Nonboolean acceptance")
        milestones_total = len(projects[case["project_id"]]["stages"])
        completed = case["milestones_completed"]
        if type(completed) is not int or not 0 <= completed <= milestones_total:
            raise ValueError("Invalid milestone count")
        audited = audit_cases.get(run_id)
        if (audited is None or audited["accepted"] != case["accepted"]
                or audited["usage_micro_usd"] != case["usage_micro_usd"]
                or sum(stage["completed"] is True for stage in audited["stages"]) != completed
                or sorted(audited["private_failed_case_ids"]) != sorted(
                    item["id"] for item in outcomes if not item["passed"])):
            raise ValueError("Audit result disagrees with trajectory")
        calls = case["invocations"]
        reservations = {call["reservation"] for call in calls}
        if len(reservations) != len(calls) or len(calls) != audited["requests"]:
            raise ValueError("Duplicate or mismatched invocation count")
        case_dispatches = set()
        for path in sorted((inputs.root / run_id).glob("trace-*.jsonl")):
            raw = inputs.raw(path.relative_to(inputs.root))
            for line in raw.splitlines():
                event = json.loads(line)
                if event["kind"] == "provider_dispatch":
                    reservation = event["reservation"]
                    if reservation in dispatches:
                        raise ValueError("Duplicate provider dispatch")
                    dispatches.add(reservation)
                    case_dispatches.add(reservation)
        if case_dispatches != reservations:
            raise ValueError("Dispatch and invocation identities differ")
        journal_count = 0
        for path in sorted((inputs.root / run_id).glob("stage-*/journal/*.result.json")):
            receipt = inputs.read(path.relative_to(inputs.root))
            journal_fields["result"].update(receipt)
            journal_fields["result_payload_metadata"].update(receipt["payload"]["metadata"])
            journal_count += 1
        if journal_count != len(calls):
            raise ValueError("Journal and invocation counts differ")
        total_journals += journal_count
        elapsed = case.get("elapsed_seconds")
        if elapsed is not None:
            number(elapsed, "trajectory elapsed time")
        rows.append(dict(run_id=run_id, project_id=case["project_id"], policy=case["policy"],
                         repetition=case["repetition"], status=case["status"], accepted=case["accepted"],
                         milestones_completed=completed, milestones_total=milestones_total,
                         all_milestones_completed=completed == milestones_total,
                         hidden_passed=sum(item["passed"] for item in outcomes), hidden_total=len(outcomes),
                         requirements_passed=sum(grouped.values()), requirements_total=len(grouped),
                         requirement_coverage=grouped, elapsed_seconds=elapsed,
                         provider_invocations=len(calls), provider_latency_seconds=None,
                         cost_micro_usd=case["usage_micro_usd"]))
    if total_journals != audit["verified_requests"]:
        raise ValueError("Audit provider request count mismatch")
    summaries = {}
    for policy in policies:
        selected = [row for row in rows if row["policy"] == policy]
        project_coverage = {project: sum(Fraction(row["requirements_passed"], row["requirements_total"])
                            for row in selected if row["project_id"] == project) / report["repetitions"]
                            for project in projects}
        mean_coverage = sum(project_coverage.values()) / len(projects)
        summary = dict(trajectories=len(selected), accepted=sum(row["accepted"] for row in selected),
            milestones_completed=sum(row["milestones_completed"] for row in selected),
            milestones_total=sum(row["milestones_total"] for row in selected),
            requirements_passed=sum(row["requirements_passed"] for row in selected),
            requirements_total=sum(row["requirements_total"] for row in selected),
            mean_project_requirement_coverage=float(mean_coverage),
            mean_project_requirement_coverage_fraction=[mean_coverage.numerator, mean_coverage.denominator],
            project_requirement_coverage={key: float(value) for key, value in project_coverage.items()},
            cost_micro_usd=sum(row["cost_micro_usd"] for row in selected),
            provider_invocations=sum(row["provider_invocations"] for row in selected),
            timing=dict(all_attempts=distribution([row["elapsed_seconds"] for row in selected]),
                all_milestones_completed=distribution([row["elapsed_seconds"] for row in selected if row["all_milestones_completed"]]),
                accepted_only=distribution([row["elapsed_seconds"] for row in selected if row["accepted"]]),
                stopped_before_final_milestone=distribution([row["elapsed_seconds"] for row in selected if not row["all_milestones_completed"]])))
        existing = prior["policies"][policy]
        for new, old in (("trajectories", "evaluated_trajectories"), ("accepted", "accepted"),
                         ("milestones_completed", "milestones_completed"), ("cost_micro_usd", "cost_micro_usd"),
                         ("provider_invocations", "invocations")):
            if summary[new] != existing[old]:
                raise ValueError("Existing descriptive summary disagrees")
        summaries[policy] = summary
    if sum(row["cost_micro_usd"] for row in rows) != report["incremental_micro_usd"]:
        raise ValueError("Finalized costs differ from study delta")
    result = dict(schema_version=1, analysis_version=VERSION, purpose="Post-freeze descriptive policy comparison",
        execution="Reuses retained audited observations; zero new candidate executions and zero API calls.",
        run=str(inputs.root), contract_sha256=contract_sha, application_identities=len(projects),
        source_trajectories_per_policy=len(projects) * report["repetitions"],
        derived_comparisons_per_policy_pair=len(projects) * report["repetitions"] ** 2,
        policies=summaries, trajectories=sorted(rows, key=lambda row: (row["project_id"], row["repetition"], row["policy"])),
        rankings=dict(whole_project_acceptance=rankings(summaries, lambda row: Fraction(row["accepted"], row["trajectories"])),
            requirement_coverage=rankings(summaries, lambda row: Fraction(*row["mean_project_requirement_coverage_fraction"]))),
        pairwise={label: [matchup(rows, left, right, field) for left, right in itertools.combinations(policies, 2)]
                  for label, field in (("whole_project_acceptance", "accepted"), ("requirement_coverage", "requirements_passed"))},
        index_pairing_sensitivity={label: [matchup(rows, left, right, field, index_paired=True)
                    for left, right in itertools.combinations(policies, 2)]
                  for label, field in (("whole_project_acceptance", "accepted"), ("requirement_coverage", "requirements_passed"))},
        metric_definitions=dict(requirement_coverage="A requirement group passes only if all its frozen private cases pass; groups are unweighted and differ between applications. The policy coverage mean weights each application equally.",
            strict_match="An accepted trajectory beats an unaccepted trajectory; equal acceptance is a draw, even when both failed.",
            coverage_match="Compare number of fully passed requirement groups within the same application; no acceptance, speed, or cost tie-breaker.",
            win_score="(wins + 0.5 * draws) / comparisons, averaged equally across applications. All three runs face all three runs of the other policy within each application. The 18 derived comparisons are not 18 independent games; they reuse six source trajectories per policy across two applications.",
            index_pairing_sensitivity="Six same-repetition-index comparisons are shown separately as a descriptive sensitivity check. Repetition labels do not imply shared provider/model randomness or a preregistered matched statistical test.",
            rank="Descriptive competition rank; tied values share rank. Acceptance and coverage rankings are separate."),
        timing=dict(unit="seconds", clock="controller time.monotonic",
            scope="From before supervise through final evaluation; includes model work, Git, Docker, forced crash recovery, and final visible/private validation. Excludes study preflight and later diagnostics/audit.",
            provider_latency_seconds=None, aggregate_provider_call_seconds=None,
            missing_provider_reason="Journal records have no request start/end or duration fields, and traces have dispatch starts only. Validation/review events and filesystem mtimes are not provider completion timestamps.",
            journal_results_inspected=total_journals, observed_journal_fields={key: sorted(value) for key, value in journal_fields.items()},
            speed_ranking=None, caveat="Timing is descriptive under variable shared-host load. Concurrent calls can overlap; their summed time would not equal end-to-end wall time. Early stopping is separately shown, never rewarded as speed. Accepted-only timing is conditional on success."),
        elo=dict(value=None, status="not_calibrated", reason="Six source trajectories per policy across two applications do not support a stable general Elo. Transparent within-application comparison scores avoid arbitrary starting ratings, K factors, and match ordering."),
        limitations=["Two synthetic applications; repeated trajectories are not independent repositories.",
                     "Post-hoc descriptive metrics, not preregistered inferential tests or population rankings.",
                     "The final hidden suite includes future requirements for early stops, measuring delivered project completeness rather than stage-only quality.",
                     "Arms use unequal compute, evidence, and review context; model profiles, reasoning settings, and host contention are study-specific.",
                     "No observed score establishes a causal gossip benefit or statistically general model superiority.",
                     "A retained-source eligibility defect affected this frozen study; no primary outcomes are rescored here."])
    if not inputs.unchanged():
        raise ValueError("Retained inputs changed during analysis")
    result["bindings"] = dict(inputs_sha256=dict(sorted(inputs.hashes.items())),
        analysis_source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), inputs_unchanged=True,
        qualification="Checks frozen source/fixture/plan identities and reconciles retained audited outcomes; this is not a rerun of the independent audit.")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.resolve().is_relative_to(args.run.resolve()):
        raise ValueError("Write new analysis outside the frozen run")
    result = summarize(args.run)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x") as stream:
        stream.write(json.dumps(result, indent=2, allow_nan=False) + "\n")
    print(json.dumps({policy: {key: value[key] for key in ("accepted", "trajectories", "mean_project_requirement_coverage", "timing")}
                      for policy, value in result["policies"].items()}))


if __name__ == "__main__":
    main()
