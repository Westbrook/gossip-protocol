"""Read-only, cohort-specific quality and measured-time summary for continuation v1.

No candidate, runner, fixture, or auditor is imported or executed. Complete
certified retained observations are a prerequisite; this is not another audit.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import hashlib
import json
import math
from pathlib import Path
import statistics


PROTOCOL = "continuation-transport-v1"
POLICIES = ("strong-maintainer", "scout-assisted")
ANALYSIS_VERSION = "continuation-comparison-v1"


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=True,
                                    allow_nan=False, separators=(",", ":")).encode()).hexdigest()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def distribution(values):
    require(all(type(value) in (int, float) and math.isfinite(value) and value >= 0 for value in values),
            "Durations must be finite nonnegative numbers")
    return dict(observations=len(values), min_seconds=min(values) if values else None,
                median_seconds=statistics.median(values) if values else None,
                max_seconds=max(values) if values else None,
                cumulative_seconds=sum(values) if values else None)


def interval_summary(spans, *, require_nonoverlap=False):
    """Report cumulative work separately from the union of overlapping intervals."""
    intervals = []
    clocks = {row["clock_id"] for row in spans}
    require(len(clocks) <= 1, "Cannot compare intervals from different monotonic clocks")
    for row in spans:
        start, end = row["started_monotonic_ns"], row["finished_monotonic_ns"]
        require(type(start) is int and type(end) is int and 0 <= start <= end,
                "Invalid monotonic interval")
        require(row.get("status") in ("finished", "failed"), "Unfinished timing span")
        require(type(row.get("elapsed_seconds")) in (int, float)
                and row["elapsed_seconds"] == (end - start) / 1e9, "Timing duration disagrees with interval")
        intervals.append((start, end))
    merged = []
    for start, end in sorted(intervals):
        if merged and start < merged[-1][1]:
            require(not require_nonoverlap, "Expected nonoverlapping execution spans")
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    cumulative = sum(end - start for start, end in intervals)
    union = sum(end - start for start, end in merged)
    return dict(span_count=len(spans), cumulative_seconds=cumulative / 1e9,
                union_wall_seconds=union / 1e9, overlap_seconds=(cumulative - union) / 1e9,
                nonoverlapping=cumulative == union, per_span=distribution([row["elapsed_seconds"] for row in spans]))


def cross_comparison(rows, field):
    left = [row for row in rows if row["policy"] == POLICIES[0]]
    right = [row for row in rows if row["policy"] == POLICIES[1]]
    require(len(left) == len(right) == 2, "Comparison requires exactly two runs per policy")
    matches = []
    for a in left:
        for b in right:
            require(a["project_id"] == b["project_id"]
                    and a["requirements_total"] == b["requirements_total"], "Cross-comparison cohort mismatch")
            av, bv = a[field], b[field]
            outcome = "win" if av > bv else "loss" if av < bv else "draw"
            matches.append(dict(left_run=a["run_id"], right_run=b["run_id"],
                                left_value=av, right_value=bv, outcome=outcome))
    counts = Counter(row["outcome"] for row in matches)
    return dict(left_policy=POLICIES[0], right_policy=POLICIES[1], metric=field,
        derived_comparisons=4, source_runs_per_policy=2, application_identities=1,
        wins=counts["win"], draws=counts["draw"], losses=counts["loss"],
        win_score=(counts["win"] + 0.5 * counts["draw"]) / 4, matches=matches)


class Inputs:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.hashes = {}

    def raw(self, relative):
        path = self.root / relative
        require(path.resolve().is_relative_to(self.root) and not path.is_symlink(), "Input path escapes cohort")
        raw = path.read_bytes()
        self.hashes[str(relative)] = hashlib.sha256(raw).hexdigest()
        return raw

    def read(self, relative):
        return json.loads(self.raw(relative))

    def unchanged(self):
        return all(hashlib.sha256((self.root / name).read_bytes()).hexdigest() == value
                   for name, value in self.hashes.items())


def summarize(root):
    inputs = Inputs(root)
    report = inputs.read("results.json")
    audit = inputs.read("independent-audit.json")
    freeze = inputs.read("frozen-trajectories.json")
    timing = inputs.read("timings.json")
    registered = inputs.read("preregistered.json")
    fixture = inputs.read("fixtures.json")
    require(report.get("experiment") == PROTOCOL and report.get("status") == report.get("phase") == "finished"
            and report.get("mode") in ("live", "rehearsal") and not report.get("unexecuted")
            and not report.get("censored") and report.get("active_case") is None,
            "Only complete uncensored continuation cohorts can be compared")
    require(audit.get("protocol") == "independent-continuation-audit-v1" and audit.get("passed") is True
            and audit.get("results_sha256") == inputs.hashes["results.json"]
            and audit.get("timings_sha256") == inputs.hashes["timings.json"],
            "A certified audit bound to these exact results and timings is required")
    contract = report["contract"]
    require(report["contract_sha"] == audit["contract_sha256"] == digest(contract)
            and registered["contract"] == contract and contract["protocol"] == PROTOCOL
            and contract["policies"] == list(POLICIES) and contract["repetitions"] == contract["milestones"] == 2,
            "Continuation contract mismatch")
    require(len(fixture["projects"]) == 1, "One-module cohort required")
    project = fixture["projects"][0]
    require(project["id"] == contract["project_id"] and digest(project) == contract["fixture_sha256"]
            and len(project["stages"]) == 2, "Fixture identity mismatch")
    for name, expected in contract["sources"].items():
        require(hashlib.sha256(inputs.raw(f"source-snapshot/gossip_harness/{name}")).hexdigest() == expected,
                "Frozen source identity mismatch")
    require(hashlib.sha256(inputs.raw("study-plan.json")).hexdigest() == contract["plan_sha256"],
            "Frozen plan identity mismatch")
    roster = [[project["id"], policy, rep] for policy in POLICIES for rep in range(2)]
    identities = lambda rows: [[row["project_id"], row["policy"], row["repetition"]] for row in rows]
    require(sorted(contract["roster"]) == sorted(roster) and registered["roster"] == contract["roster"]
            and identities(report["cases"]) == contract["roster"]
            and identities(freeze["trajectories"]) == contract["roster"], "Exact four-run roster mismatch")
    freeze_sha = inputs.hashes["frozen-trajectories.json"]
    require(report["freeze_manifest_sha256"] == audit["freeze_manifest_sha256"] == freeze_sha
            and freeze["contract_sha"] == digest(contract) and freeze["protocol"] == PROTOCOL
            and freeze["private_evaluation_started"] is False, "Freeze manifest mismatch")
    barrier = freeze["frozen_monotonic_ns"]
    require(type(barrier) is int and barrier >= 0, "Invalid global barrier time")
    spans = timing["spans"]
    require(len({row["span_id"] for row in spans}) == len(spans)
            and all(row["clock_id"] == timing["clock_id"] for row in spans), "Timing clock or span identity mismatch")
    interval_summary(spans)
    by_span = {row["span_id"]: row for row in spans}
    audit_rows = {row["run_id"]: row for row in audit["cases"]}
    terminal_rows = {row["run_id"]: row for row in report["trajectories"]}
    run_ids = {row["run_id"] for row in report["cases"]}
    require(len(audit_rows) == len(terminal_rows) == len(run_ids) == 4
            and set(audit_rows) == set(terminal_rows) == run_ids, "Audited or timed trajectory roster mismatch")
    require(all(row.get("run_id") in run_ids for row in spans if "run_id" in row), "Unknown timing trajectory")
    expected_hidden = [case for stage in project["stages"] for case in stage["hidden_cases"]]
    provider_ids, rows = set(), []
    for case, frozen in zip(report["cases"], freeze["trajectories"]):
        run_id = case["run_id"]
        require(Path(run_id).name == run_id and run_id not in (".", ".."), "Unsafe run ID")
        state = inputs.read(f"{run_id}/trajectory.json")
        require(inputs.read(f"{run_id}/result.json") == case
                and inputs.hashes[f"{run_id}/trajectory.json"] == case["trajectory_sha256"] == frozen["trajectory_sha256"]
                and inputs.read(f"{run_id}/final-private-receipt.json") == case["final_hidden"], "Retained case evidence mismatch")
        require(case["accepted"] is audit_rows[run_id]["accepted"] and type(case["accepted"]) is bool
                and case["milestones_completed"] == sum(stage["completed"] is True for stage in audit_rows[run_id]["stages"])
                == sum(stage["completed"] is True for stage in state["stages"]), "Audited primary outcome mismatch")
        outcomes = case["final_hidden"]["outcomes"]
        require([(row["id"], row["requirement"]) for row in outcomes]
                == [(row["id"], row["requirement"]) for row in expected_hidden]
                and all(type(row["passed"]) is bool for row in outcomes), "Private suite or outcome identity mismatch")
        coverage = {requirement: all(row["passed"] for row in outcomes if row["requirement"] == requirement)
                    for requirement in {row["requirement"] for row in outcomes}}
        require(coverage == case["requirement_coverage"], "Private requirement coverage mismatch")
        own = [row for row in spans if row.get("run_id") == run_id]
        model = [row for row in own if row["kind"] == "model_trajectory"]
        final = [row for row in own if row["kind"] == "final_evaluation"]
        require(len(model) == len(final) == 1, "Each run needs one model and final-evaluation span")
        model, final = model[0], final[0]
        require(model["status"] == final["status"] == "finished"
                and model["finished_monotonic_ns"] <= barrier <= final["started_monotonic_ns"],
                "Model/final spans violate the global freeze barrier")
        terminal = terminal_rows[run_id]
        require(terminal["span_id"] == model["span_id"] and terminal["elapsed_seconds"] == model["elapsed_seconds"]
                and terminal["trajectory_sha256"] == case["trajectory_sha256"], "Trajectory timing summary mismatch")
        active = interval_summary([model, final], require_nonoverlap=True)["cumulative_seconds"]
        wait_freeze = (barrier - model["finished_monotonic_ns"]) / 1e9
        wait_final = (final["started_monotonic_ns"] - barrier) / 1e9
        endpoint = (final["finished_monotonic_ns"] - model["started_monotonic_ns"]) / 1e9
        physical = [row for row in own if row["kind"] == "physical_provider_request"]
        call_rows = case["invocations"]
        expected_physical = {row["physical_span_id"] for row in call_rows if row["physical_dispatch"]}
        require({row["span_id"] for row in physical} == expected_physical
                and len(physical) == case["physical_provider_calls"]
                and not provider_ids.intersection(expected_physical), "Physical provider call identity mismatch")
        provider_ids.update(expected_physical)
        for call in call_rows:
            if not call["physical_dispatch"]:
                continue
            span = by_span[call["physical_span_id"]]
            require(all(span[key] == call[key] for key in ("call_id", "role", "model"))
                    and model["started_monotonic_ns"] <= span["started_monotonic_ns"]
                    <= span["finished_monotonic_ns"] <= model["finished_monotonic_ns"],
                    "Physical provider labels or containment mismatch")
        docker = [row for row in own if row["kind"] == "docker_validation"]
        for span in docker:
            parent = final if span["purpose"].startswith("final_") else model
            require(parent["started_monotonic_ns"] <= span["started_monotonic_ns"]
                    <= span["finished_monotonic_ns"] <= parent["finished_monotonic_ns"],
                    "Docker validation is outside its active parent span")
        scout_totals = {key: sum(stage["scouts"][key] for stage in state["stages"])
                        for key in ("calls", "admitted", "rejected")}
        require(scout_totals == case["scouts"] and scout_totals["calls"] == sum(call["role"] == "scout" for call in call_rows),
                "Scout aggregate mismatch")
        require(case["usage_micro_usd"] == sum(call["usage_units"] for call in call_rows), "Per-run API cost mismatch")
        rows.append(dict(run_id=run_id, project_id=case["project_id"], policy=case["policy"], repetition=case["repetition"],
            status=case["status"], accepted=case["accepted"], milestones_completed=case["milestones_completed"], milestones_total=2,
            requirements_passed=sum(coverage.values()), requirements_total=len(coverage), requirement_coverage=coverage,
            hidden_passed=sum(row["passed"] for row in outcomes), hidden_total=len(outcomes),
            cost_micro_usd=case["usage_micro_usd"], invocations=len(call_rows), physical_provider_calls=len(physical),
            calls_by_role=dict(Counter(call["role"] for call in call_rows)),
            cost_by_role_micro_usd={role: sum(call["usage_units"] for call in call_rows if call["role"] == role)
                                    for role in sorted({call["role"] for call in call_rows})},
            scouts=scout_totals, metrics=case["metrics"],
            timing=dict(model_trajectory_active_seconds=model["elapsed_seconds"], final_evaluation_active_seconds=final["elapsed_seconds"],
                total_active_seconds=active, wait_to_global_freeze_seconds=wait_freeze,
                post_freeze_evaluation_queue_seconds=wait_final, total_wait_seconds=wait_freeze + wait_final,
                start_to_final_outcome_seconds=endpoint, physical_provider=interval_summary(physical),
                physical_provider_by_role={role: interval_summary([span for span in physical if span["role"] == role])
                                           for role in sorted({span["role"] for span in physical})},
                docker_validation=interval_summary(docker, require_nonoverlap=True))))
    require(sum(row["cost_micro_usd"] for row in rows) == report["incremental_micro_usd"]
            == audit["accounting"]["study_usage_micro_usd"], "Cohort usage differs from audited accounting")
    policies = {}
    fields = ("model_trajectory_active_seconds", "final_evaluation_active_seconds", "total_active_seconds",
              "wait_to_global_freeze_seconds", "post_freeze_evaluation_queue_seconds", "total_wait_seconds", "start_to_final_outcome_seconds")
    for policy in POLICIES:
        selected = [row for row in rows if row["policy"] == policy]
        policy_spans = [span for span in spans if span.get("run_id") in {row["run_id"] for row in selected}]
        policies[policy] = dict(trajectories=2, accepted=sum(row["accepted"] for row in selected),
            milestones_completed=sum(row["milestones_completed"] for row in selected), milestones_total=4,
            requirements_passed=sum(row["requirements_passed"] for row in selected),
            requirements_total=sum(row["requirements_total"] for row in selected),
            mean_requirement_coverage=statistics.mean(row["requirements_passed"] / row["requirements_total"] for row in selected),
            terminal_outcomes=dict(Counter(row["status"] for row in selected)),
            cost_micro_usd=sum(row["cost_micro_usd"] for row in selected), invocations=sum(row["invocations"] for row in selected),
            physical_provider_calls=sum(row["physical_provider_calls"] for row in selected),
            calls_by_role=dict(sum((Counter(row["calls_by_role"]) for row in selected), Counter())),
            cost_by_role_micro_usd=dict(sum((Counter(row["cost_by_role_micro_usd"]) for row in selected), Counter())),
            scouts={key: sum(row["scouts"][key] for row in selected) for key in ("calls", "admitted", "rejected")},
            timing={"all_attempts": {key: distribution([row["timing"][key] for row in selected]) for key in fields},
                    "accepted_active": distribution([row["timing"]["total_active_seconds"] for row in selected if row["accepted"]]),
                    "stopped_before_final_milestone_active": distribution([row["timing"]["total_active_seconds"] for row in selected if row["milestones_completed"] < 2]),
                    "physical_provider": interval_summary([span for span in policy_spans if span["kind"] == "physical_provider_request"]),
                    "docker_validation": interval_summary([span for span in policy_spans if span["kind"] == "docker_validation"], require_nonoverlap=True)})
    require(inputs.unchanged(), "Retained inputs changed during comparison")
    return dict(schema_version=1, analysis_version=ANALYSIS_VERSION, cohort=PROTOCOL, mode=report["mode"],
        execution="Reuses retained audited observations; zero API calls or candidate executions.", run=str(inputs.root),
        contract_sha256=digest(contract), application_identities=1, trajectories=rows, policies=policies,
        comparisons=dict(whole_project_acceptance=cross_comparison(rows, "accepted"),
                         requirement_coverage=cross_comparison(rows, "requirements_passed")),
        elo=None, cross_cohort_ranking=None,
        definitions=dict(requirements="A requirement group passes only when all its private cases pass. Coverage and whole-project acceptance are separate metrics.",
            comparisons="Every one of the two maintainer runs faces each of the two assisted runs: four derived comparisons reusing four source runs within one module, not four independent games. A draw includes two failed projects. Score=(wins+0.5*draws)/4.",
            active="Model trajectory elapsed (including models, Git, stage validation and controller work) plus final-evaluation elapsed, after checking nonoverlap. Nested provider, scout, stage and Docker spans are not added to this total.",
            waiting="Wait-to-global-freeze and subsequent queue-to-final-evaluation are separate from active work. Early trajectories wait while later trajectories finish. These are study scheduling delays, not model processing speed.",
            provider="Physical request wall time includes network/provider wait and parsing, not pure model forward-pass latency. Cumulative request seconds can exceed union wall exposure because scout calls overlap; neither is added to parent active time.",
            docker="Physical validation wall spans; cumulative seconds are reported only after checking nonoverlap. This includes container lifecycle and host verification within the measured call, not only candidate CPU.",
            scouts="Admitted counts are accepted case proposals, not unique discovered defects. Rejected counts are admission-log entries: a malformed whole response can contribute one rejection, while well-formed responses can have individual proposal rejections. These units must not be conflated. This study uses trusted oracle label validation."),
        limitations=["One existing repository module, two milestones and two runs per policy; no general causal or statistical superiority claim.",
            "The assisted policy adds compute and candidate-aware evidence; this does not isolate gossip transport or controller changes.",
            "The final private suite covers both milestones, including unreached scope after early stops; its score measures delivered completeness rather than stage-only quality.",
            "Host contention and provider conditions can vary. Early termination is reported separately and never rewarded as speed.",
            "Accepted-only timing is conditional on success; groups without accepted projects have null timing, not zero.",
            "No comparison or rating is pooled across earlier cohorts with different contracts, fixtures or timing definitions.",
            "This is descriptive post-run aggregation of trusted-host evidence, not another independent audit or clock attestation."],
        bindings=dict(inputs_sha256=dict(sorted(inputs.hashes.items())), inputs_unchanged=True,
                      analysis_source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    require(not args.output.resolve().is_relative_to(args.run.resolve()), "Write new analysis outside frozen cohort")
    result = summarize(args.run)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x") as stream:
        stream.write(json.dumps(result, indent=2, allow_nan=False) + "\n")
    print(json.dumps({policy: {key: value[key] for key in ("accepted", "trajectories", "mean_requirement_coverage", "cost_micro_usd")}
                      for policy, value in result["policies"].items()}))


if __name__ == "__main__":
    main()
