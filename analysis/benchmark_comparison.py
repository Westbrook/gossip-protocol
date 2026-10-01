"""Read-only descriptive comparison of a certified evidence-frontier cohort.

No runner, fixture, candidate, or auditor is imported or executed. A partial
cohort produces an unscored status inventory; diagnostics need a separate audit.
"""
from __future__ import annotations

import argparse
from collections import Counter
from copy import deepcopy
from fractions import Fraction
import hashlib
import json
import math
from pathlib import Path
import statistics

PROTOCOL = "evidence-frontier-v1"
ANALYSIS_VERSION = "evidence-frontier-comparison-v1"
MATCHED_POLICIES = ("sequential-four", "independent-four")
ANCHOR = "strong-anchor"
POLICIES = (*MATCHED_POLICIES, ANCHOR)
PROJECTS = ("graph-patch", "calendar-exchange")
POLICY_CONTRACT = {
    "sequential-four": dict(repetitions=2, initial_builders=4, model="cheap", formation="sequential"),
    "independent-four": dict(repetitions=2, initial_builders=4, model="cheap", formation="independent"),
    "strong-anchor": dict(repetitions=1, initial_builders=1, model="strong", formation="independent"),
}
TIME_FIELDS = ("model_trajectory_active_seconds", "final_evaluation_active_seconds", "total_active_seconds",
               "wait_to_global_freeze_seconds", "post_freeze_evaluation_queue_seconds", "total_wait_seconds",
               "start_to_final_outcome_seconds")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=True,
                                    allow_nan=False, separators=(",", ":")).encode()).hexdigest()


def decode(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "Duplicate JSON member")
            result[key] = value
        return result
    def constant(_):
        raise ValueError("Nonfinite JSON value")
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=constant)


class Inputs:
    """Hash every consumed byte and reject changes even between repeated reads."""
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.hashes = {}

    def path(self, relative):
        relative = Path(relative)
        require(not relative.is_absolute() and ".." not in relative.parts, "Unsafe input path")
        path = self.root / relative
        require(path.resolve().is_relative_to(self.root)
                and not any(parent.is_symlink() for parent in (path, *path.parents) if parent != self.root),
                "Input path escapes cohort or uses a symlink")
        return path

    def raw(self, relative):
        key = str(Path(relative))
        raw = self.path(relative).read_bytes()
        value = hashlib.sha256(raw).hexdigest()
        require(key not in self.hashes or self.hashes[key] == value, "Retained input changed between reads")
        self.hashes[key] = value
        return raw

    def read(self, relative):
        return decode(self.raw(relative))

    def unchanged(self):
        return all(hashlib.sha256(self.path(name).read_bytes()).hexdigest() == expected
                   for name, expected in self.hashes.items())


def distribution(values):
    require(all(type(value) in (int, float) and math.isfinite(value) and value >= 0 for value in values),
            "Durations must be finite nonnegative numbers")
    return dict(observations=len(values), min_seconds=min(values) if values else None,
                median_seconds=statistics.median(values) if values else None,
                max_seconds=max(values) if values else None, cumulative_seconds=sum(values) if values else None)


def interval_summary(spans):
    require(len({row["clock_id"] for row in spans}) <= 1, "Cannot combine different monotonic clocks")
    intervals = []
    for row in spans:
        start, end = row["started_monotonic_ns"], row["finished_monotonic_ns"]
        require(type(start) is int and type(end) is int and 0 <= start <= end,
                "Invalid monotonic interval")
        require(row.get("status") in ("finished", "failed") and type(row.get("elapsed_seconds")) in (int, float)
                and row["elapsed_seconds"] == (end - start) / 1e9, "Unclosed or inconsistent timing span")
        intervals.append((start, end))
    merged = []
    for start, end in sorted(intervals):
        if merged and start < merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    cumulative, union = sum(b - a for a, b in intervals), sum(b - a for a, b in merged)
    return dict(span_count=len(spans), cumulative_seconds=cumulative / 1e9,
                union_wall_seconds=union / 1e9, overlap_seconds=(cumulative - union) / 1e9,
                per_span=distribution([row["elapsed_seconds"] for row in spans]))


def paired_comparison(rows, metric):
    """Exactly four declared project/repetition blocks, never all-cross games."""
    require(metric in ("quality_ordered", "whole_project_acceptance", "requirement_coverage"), "Unknown comparison metric")
    matched = [row for row in rows if row["policy"] in MATCHED_POLICIES]
    require(all(type(row["repetition"]) is int for row in matched), "Invalid matched repetition type")
    index = {(row["project_id"], row["repetition"], row["policy"]): row for row in matched}
    keys = {(project, rep, policy) for project in PROJECTS for rep in range(2) for policy in MATCHED_POLICIES}
    require(len(matched) == len(index) == 8 and set(index) == keys, "Incomplete or duplicate matched roster")
    matches = []
    for project in PROJECTS:
        for rep in range(2):
            a, b = (index[(project, rep, policy)] for policy in MATCHED_POLICIES)
            require(a["requirements_total"] == b["requirements_total"] > 0, "Within-project requirement denominator differs")
            def value(row):
                coverage = Fraction(row["requirements_passed"], row["requirements_total"])
                if metric == "whole_project_acceptance":
                    return int(row["accepted"])
                return (int(row["accepted"]), coverage) if metric == "quality_ordered" else coverage
            av, bv = value(a), value(b)
            matches.append(dict(project_id=project, repetition=rep, left_run=a["run_id"], right_run=b["run_id"],
                left_accepted=a["accepted"], right_accepted=b["accepted"],
                left_requirements_passed=a["requirements_passed"], right_requirements_passed=b["requirements_passed"],
                requirements_total=a["requirements_total"], outcome="win" if av > bv else "loss" if av < bv else "draw"))
    def counts(selected):
        count = Counter(row["outcome"] for row in selected)
        return dict(blocks=len(selected), wins=count["win"], draws=count["draw"], losses=count["loss"],
                    win_score=(count["win"] + 0.5 * count["draw"]) / len(selected))
    return dict(left_policy=MATCHED_POLICIES[0], right_policy=MATCHED_POLICIES[1], metric=metric,
        **counts(matches), application_identities=2, source_trajectories_per_policy=4,
        per_project={project: counts([row for row in matches if row["project_id"] == project]) for project in PROJECTS},
        matches=matches)


def validate_contract(inputs, report):
    contract = report["contract"]
    registered, plan = inputs.read("preregistered.json"), inputs.read("study-plan.json")
    require(contract.get("protocol") == PROTOCOL and report.get("contract_sha") == digest(contract)
            and registered["contract"] == contract and type(contract.get("milestones")) is int
            and contract["milestones"] == 2 and contract["policies"] == POLICY_CONTRACT,
            "Frozen benchmark contract mismatch")
    require(inputs.hashes["study-plan.json"] == contract["plan_sha256"] and plan["protocol"] == PROTOCOL
            and plan["policies"] == POLICY_CONTRACT and set(plan["projects"]) == set(PROJECTS),
            "Frozen plan identity mismatch")
    roster = contract["roster"]
    expected = {(project, policy, rep) for project in PROJECTS for policy in POLICIES
                for rep in range(POLICY_CONTRACT[policy]["repetitions"])}
    require(type(roster) is list and len(roster) == 10
            and all(type(row) is list and len(row) == 3 and type(row[2]) is int for row in roster)
            and len({tuple(row) for row in roster}) == 10 and {tuple(row) for row in roster} == expected
            and registered["roster"] == roster and plan["roster"] == roster, "Exact ten-trajectory roster mismatch")
    for name, expected_sha in contract["sources"].items():
        inputs.raw(f"source-snapshot/gossip_harness/{name}")
        require(inputs.hashes[f"source-snapshot/gossip_harness/{name}"] == expected_sha, "Frozen source identity mismatch")
    for name, expected_sha in contract.get("extra_sources", {}).items():
        inputs.raw(f"source-snapshot/{name}")
        require(inputs.hashes[f"source-snapshot/{name}"] == expected_sha, "Frozen extra source identity mismatch")
    return contract


def bindings(inputs, others=()):
    require(inputs.unchanged() and all(item.unchanged() for item in others), "Retained inputs changed during comparison")
    return dict(primary_root=str(inputs.root), inputs_sha256=dict(sorted(inputs.hashes.items())), inputs_unchanged=True,
        additional_inputs=[dict(root=str(item.root), inputs_sha256=dict(sorted(item.hashes.items()))) for item in others],
        analysis_source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())


def incomplete_summary(inputs, report, contract):
    """Retain censoring and unexecuted work without consuming private evidence."""
    terminals = report.get("trajectories", [])
    terminal_index = {row["run_id"]: row for row in terminals}
    require(len(terminal_index) == len(terminals), "Duplicate terminal trajectory")
    planned = {f"{p}-{policy}-{rep}" for p, policy, rep in contract["roster"]}
    require(set(terminal_index) <= planned, "Unknown partial trajectory")
    censored = {row["run_id"]: row for row in report.get("censored", [])}
    unexecuted = {tuple(row) for row in report.get("unexecuted", [])}
    require(set(censored) <= planned and unexecuted <= {tuple(row) for row in contract["roster"]}, "Unknown censored or unexecuted work")
    rows = []
    active = report.get("active_case") or {}
    for project, policy, rep in contract["roster"]:
        run_id = f"{project}-{policy}-{rep}"
        status = ("censored" if run_id in censored else "active" if active.get("run_id") == run_id
                  else "unexecuted" if (project, policy, rep) in unexecuted
                  else "terminal_not_certified" if run_id in terminal_index else "not_recorded")
        rows.append(dict(run_id=run_id, project_id=project, policy=policy, repetition=rep, execution_status=status,
            terminal=terminal_index.get(run_id, {}).get("terminal"), censor_reason=censored.get(run_id, {}).get("reason")))
    return dict(schema_version=1, analysis_version=ANALYSIS_VERSION, cohort=PROTOCOL, status="incomplete_unscored",
        mode=report["mode"], certification="Not certified as a complete cohort; inventory fields are retained report claims only.",
        execution="Read-only status inventory; zero API calls or candidate executions. Does not inspect or report private scores or read separate private-evaluation artifacts.",
        run=str(inputs.root), contract_sha256=digest(contract), source_status=report.get("status"),
        source_phase=report.get("phase"), planned_trajectories=10, terminal_trajectories=len(terminals),
        trajectories=rows, policies=None, projects=None, comparisons=None, diagnostics=None, elo=None,
        ranking=None, cross_cohort_ranking=None, reported_incremental_micro_usd=report.get("incremental_micro_usd"),
        reported_unsettled=deepcopy(report.get("unsettled")),
        limitations=["Partial and unexecuted work is not scored as incorrect or ranked as faster.",
                     "No completed-cohort primary or private diagnostic scores are published before complete independent certification."],
        bindings=bindings(inputs))


def read_audit(inputs, audit_path, others):
    if audit_path is None:
        return inputs.read("independent-audit.json")
    path = Path(audit_path).resolve()
    if path.is_relative_to(inputs.root):
        return inputs.read(path.relative_to(inputs.root))
    audit_inputs = Inputs(path.parent)
    others.append(audit_inputs)
    return audit_inputs.read(path.name)


def count_map_sum(rows, key):
    keys = {name for row in rows for name in row[key]}
    return {name: sum(row[key].get(name, 0) for row in rows) for name in sorted(keys)}


def policy_summary(rows, spans):
    run_ids = {row["run_id"] for row in rows}
    own = [span for span in spans if span.get("run_id") in run_ids]
    return dict(trajectories=len(rows), accepted=sum(row["accepted"] for row in rows),
        milestones_completed=sum(row["milestones_completed"] for row in rows), milestones_total=2 * len(rows),
        requirements_passed=sum(row["requirements_passed"] for row in rows), requirements_total=sum(row["requirements_total"] for row in rows),
        mean_requirement_coverage=statistics.mean(row["requirements_passed"] / row["requirements_total"] for row in rows),
        terminal_outcomes=dict(Counter(row["status"] for row in rows)),
        cost_micro_usd=sum(row["cost_micro_usd"] for row in rows), invocations=sum(row["invocations"] for row in rows),
        physical_provider_calls=sum(row["physical_provider_calls"] for row in rows),
        calls_by_role=count_map_sum(rows, "calls_by_role"), cost_by_role_micro_usd=count_map_sum(rows, "cost_by_role_micro_usd"),
        metrics=count_map_sum(rows, "metrics"), scouts=count_map_sum(rows, "scouts"),
        timing=dict(all_attempts={key: distribution([row["timing"][key] for row in rows]) for key in TIME_FIELDS},
            accepted_active=distribution([row["timing"]["total_active_seconds"] for row in rows if row["accepted"]]),
            visible_complete_but_rejected_active=distribution([row["timing"]["total_active_seconds"] for row in rows
                                                              if row["milestones_completed"] == 2 and not row["accepted"]]),
            stopped_before_final_milestone_active=distribution([row["timing"]["total_active_seconds"] for row in rows if row["milestones_completed"] < 2]),
            physical_provider=interval_summary([span for span in own if span["kind"] == "physical_provider_request"]),
            docker_validation=interval_summary([span for span in own if span["kind"] == "docker_validation"])))


def summarize(root, *, diagnostics=None, diagnostic_audit=None, audit_path=None):
    inputs, others = Inputs(root), []
    report = inputs.read("results.json")
    require(report.get("experiment") == PROTOCOL and report.get("mode") in ("live", "rehearsal"), "Unknown benchmark cohort")
    contract = validate_contract(inputs, report)
    complete = (report.get("status") == report.get("phase") == "finished" and not report.get("unexecuted")
                and not report.get("censored") and report.get("active_case") is None)
    if not complete:
        require(diagnostics is None and diagnostic_audit is None, "Incomplete cohort cannot publish private diagnostics")
        return incomplete_summary(inputs, report, contract)
    audit = read_audit(inputs, audit_path, others)
    freeze, timing, fixtures = inputs.read("frozen-trajectories.json"), inputs.read("timings.json"), inputs.read("fixtures.json")
    freeze_sha = inputs.hashes["frozen-trajectories.json"]
    require(audit.get("protocol") == "independent-evidence-frontier-audit-v1" and audit.get("passed") is True
            and audit.get("results_sha256") == inputs.hashes["results.json"]
            and audit.get("timings_sha256") == inputs.hashes["timings.json"]
            and audit.get("contract_sha256") == digest(contract)
            and audit.get("scope", {}).get("primary_cohort") is True,
            "A successful primary audit bound to these exact results and timings is required")
    require(report["freeze_manifest_sha256"] == audit["freeze_manifest_sha256"] == freeze_sha
            and freeze["protocol"] == PROTOCOL and freeze["contract_sha"] == digest(contract)
            and freeze["private_evaluation_started"] is False, "Freeze manifest mismatch")
    projects = {project["id"]: project for project in fixtures["projects"]}
    require(len(fixtures["projects"]) == len(projects) == 2 and set(projects) == set(PROJECTS), "Fixture project identities differ")
    for project_id, project in projects.items():
        require(digest(project) == contract["fixtures"][project_id]["fixture_sha256"] and len(project["stages"]) == 2,
                "Fixture identity mismatch")
    fault_banks = inputs.read("fault-banks.json")
    require(digest(fault_banks) == contract["fault_banks_sha256"], "Frozen fault-bank identity mismatch")
    identities = lambda rows: [[row["project_id"], row["policy"], row["repetition"]] for row in rows]
    require(all(type(row["repetition"]) is int for row in [*report["cases"], *freeze["trajectories"]])
            and identities(report["cases"]) == identities(freeze["trajectories"]) == contract["roster"], "Final or frozen roster mismatch")
    run_ids = [f"{p}-{policy}-{rep}" for p, policy, rep in contract["roster"]]
    audit_rows = {row["run_id"]: row for row in audit["cases"]}
    terminal_rows = {row["run_id"]: row for row in report["trajectories"]}
    require(len(audit["cases"]) == len(audit_rows) == len(report["trajectories"]) == len(terminal_rows) == 10
            and set(audit_rows) == set(terminal_rows) == set(run_ids), "Audited or timed trajectory roster mismatch")
    barrier = freeze["frozen_monotonic_ns"]
    require(type(barrier) is int and barrier >= 0, "Invalid global freeze barrier")
    spans = timing["spans"]
    require(len({row["span_id"] for row in spans}) == len(spans)
            and all(row["clock_id"] == timing["clock_id"] for row in spans), "Timing clock or span identity mismatch")
    interval_summary(spans)
    require(all(span.get("run_id") in run_ids for span in spans if "run_id" in span), "Unknown timing trajectory")
    by_span = {row["span_id"]: row for row in spans}
    rows, provider_ids = [], set()
    for case, frozen, expected_id in zip(report["cases"], freeze["trajectories"], run_ids):
        run_id = case["run_id"]
        require(run_id == frozen["run_id"] == expected_id, "Case run identity mismatch")
        state = inputs.read(f"{run_id}/trajectory.json")
        require(inputs.read(f"{run_id}/result.json") == case
                and inputs.hashes[f"{run_id}/trajectory.json"] == case["trajectory_sha256"] == frozen["trajectory_sha256"]
                and inputs.read(f"{run_id}/final-private-receipt.json") == case["final_hidden"]
                and inputs.read(f"{run_id}/final-visible-receipt.json") == case["final_visible"], "Retained case evidence mismatch")
        stages, audited = state["stages"], audit_rows[run_id]
        require(all(type(stage["completed"]) is bool for stage in stages)
                and type(case["accepted"]) is bool and case["accepted"] is audited["accepted"]
                and type(case["milestones_completed"]) is int
                and case["milestones_completed"] == audited["milestones_completed"]
                == sum(stage["completed"] is True for stage in audited["stages"])
                == sum(stage["completed"] for stage in stages), "Audited primary outcome mismatch")
        require(case["accepted"] is bool(case["milestones_completed"] == 2 and case["final_hidden"]["passed"]
                                        and case["final_visible"]["passed"]), "Acceptance disagrees with completion and final receipts")
        expected_hidden = [row for stage in projects[case["project_id"]]["stages"] for row in stage["hidden_cases"]]
        outcomes = case["final_hidden"]["outcomes"]
        require([(row["id"], row["requirement"]) for row in outcomes] == [(row["id"], row["requirement"]) for row in expected_hidden]
                and bool(outcomes) and all(type(row["passed"]) is bool for row in outcomes), "Private suite or outcome identity mismatch")
        coverage = {req: all(row["passed"] for row in outcomes if row["requirement"] == req) for req in sorted({row["requirement"] for row in outcomes})}
        require(coverage == case["requirement_coverage"], "Private requirement coverage mismatch")
        own = [span for span in spans if span.get("run_id") == run_id]
        model = [span for span in own if span["kind"] == "model_trajectory"]
        final = [span for span in own if span["kind"] == "final_evaluation"]
        require(len(model) == len(final) == 1, "Each run needs one model and final-evaluation span")
        model, final = model[0], final[0]
        require(model["status"] == final["status"] == "finished"
                and model["finished_monotonic_ns"] <= barrier <= final["started_monotonic_ns"], "Model/final spans violate global freeze barrier")
        terminal = terminal_rows[run_id]
        require(terminal["span_id"] == model["span_id"] and terminal["elapsed_seconds"] == model["elapsed_seconds"]
                and terminal["trajectory_sha256"] == case["trajectory_sha256"], "Trajectory timing summary mismatch")
        call_rows = case["invocations"]
        require(call_rows == [call for stage in stages for call in stage["invocations"]]
                and all(type(call["usage_units"]) is int and call["usage_units"] >= 0
                        and type(call["physical_dispatch"]) is bool for call in call_rows), "Invocation or cost identity mismatch")
        physical = [span for span in own if span["kind"] == "physical_provider_request"]
        expected_physical = [call["physical_span_id"] for call in call_rows if call["physical_dispatch"]]
        require(len(expected_physical) == len(set(expected_physical)) == len(physical) == case["physical_provider_calls"]
                and {span["span_id"] for span in physical} == set(expected_physical)
                and not provider_ids.intersection(expected_physical), "Physical provider call identity mismatch")
        provider_ids.update(expected_physical)
        for call in call_rows:
            if call["physical_dispatch"]:
                span = by_span[call["physical_span_id"]]
                require(all(span[key] == call[key] for key in ("call_id", "role", "model"))
                        and model["started_monotonic_ns"] <= span["started_monotonic_ns"] <= span["finished_monotonic_ns"] <= model["finished_monotonic_ns"],
                        "Physical provider labels or containment mismatch")
        docker = [span for span in own if span["kind"] == "docker_validation"]
        for span in docker:
            require(span.get("purpose") in ("stage_evidence", "final_visible", "final_private"), "Unknown primary Docker purpose")
            parent = model if span["purpose"] == "stage_evidence" else final
            require(parent["started_monotonic_ns"] <= span["started_monotonic_ns"] <= span["finished_monotonic_ns"] <= parent["finished_monotonic_ns"],
                    "Docker validation is outside its active parent span")
        scout_totals = {key: sum(stage["scouts"][key] for stage in stages) for key in ("calls", "admitted", "rejected", "rejected_batches")}
        metric_totals = {key: sum(stage["metrics"].get(key, 0) for stage in stages) for key in {key for stage in stages for key in stage["metrics"]}}
        require(scout_totals == case["scouts"] and scout_totals["calls"] == sum(call["role"] == "scout" for call in call_rows), "Scout aggregate mismatch")
        require(metric_totals == case["metrics"], "Controller metric aggregate mismatch")
        require(case["usage_micro_usd"] == sum(call["usage_units"] for call in call_rows), "Per-run API cost mismatch")
        active = model["elapsed_seconds"] + final["elapsed_seconds"]
        wait_freeze, wait_final = (barrier - model["finished_monotonic_ns"]) / 1e9, (final["started_monotonic_ns"] - barrier) / 1e9
        rows.append(dict(run_id=run_id, project_id=case["project_id"], policy=case["policy"], repetition=case["repetition"],
            context_only=case["policy"] == ANCHOR, status=case["status"], accepted=case["accepted"],
            milestones_completed=case["milestones_completed"], milestones_total=2,
            requirements_passed=sum(coverage.values()), requirements_total=len(coverage), requirement_coverage=coverage,
            requirements_by_introduced_stage=[dict(stage_index=index, requirements={req: coverage[req] for req in sorted({c["requirement"] for c in stage["hidden_cases"]})})
                                              for index, stage in enumerate(projects[case["project_id"]]["stages"])],
            hidden_passed=sum(row["passed"] for row in outcomes), hidden_total=len(outcomes),
            cost_micro_usd=case["usage_micro_usd"], invocations=len(call_rows), physical_provider_calls=len(physical),
            calls_by_role=dict(Counter(call["role"] for call in call_rows)),
            cost_by_role_micro_usd={role: sum(call["usage_units"] for call in call_rows if call["role"] == role) for role in sorted({call["role"] for call in call_rows})},
            metrics=metric_totals, scouts=scout_totals,
            timing=dict(model_trajectory_active_seconds=model["elapsed_seconds"], final_evaluation_active_seconds=final["elapsed_seconds"],
                total_active_seconds=active, wait_to_global_freeze_seconds=wait_freeze, post_freeze_evaluation_queue_seconds=wait_final,
                total_wait_seconds=wait_freeze + wait_final, start_to_final_outcome_seconds=(final["finished_monotonic_ns"] - model["started_monotonic_ns"]) / 1e9,
                physical_provider=interval_summary(physical),
                physical_provider_by_role={role: interval_summary([span for span in physical if span["role"] == role]) for role in sorted({span["role"] for span in physical})},
                docker_validation=interval_summary(docker))))
    require(sum(row["cost_micro_usd"] for row in rows) == report["incremental_micro_usd"] == audit["accounting"]["study_usage_micro_usd"], "Cohort usage differs from audited accounting")
    per_project = {project: {policy: policy_summary([row for row in rows if row["project_id"] == project and row["policy"] == policy], spans)
                            for policy in POLICIES} for project in PROJECTS}
    policies = {policy: {**policy_summary([row for row in rows if row["policy"] == policy], spans),
                         "context_only": policy == ANCHOR,
                         "mean_requirement_coverage_equal_project_weight": statistics.mean(per_project[project][policy]["mean_requirement_coverage"] for project in PROJECTS)} for policy in POLICIES}
    diagnostic_summary = None
    if diagnostics is not None:
        diagnostic_summary, diagnostic_inputs = summarize_diagnostics(inputs, contract, diagnostics, diagnostic_audit)
        others.extend(diagnostic_inputs)
    else:
        require(diagnostic_audit is None, "Diagnostic audit provided without diagnostic evidence")
    return dict(schema_version=1, analysis_version=ANALYSIS_VERSION, cohort=PROTOCOL, status="certified_complete", mode=report["mode"],
        execution="Reuses retained audited observations; zero API calls or candidate executions.", run=str(inputs.root), contract_sha256=digest(contract),
        application_identities=2, planned_trajectories=10, trajectories=rows, projects=per_project, policies=policies,
        comparisons={metric: paired_comparison(rows, metric) for metric in ("quality_ordered", "whole_project_acceptance", "requirement_coverage")},
        diagnostics=diagnostic_summary, elo=None, ranking=None, cross_cohort_ranking=None,
        definitions=DEFINITIONS, limitations=LIMITATIONS, bindings=bindings(inputs, others))


DEFINITIONS = dict(
    requirements="A requirement passes only when every private case in its group passes. Per-project rates precede equal-project macro averages; raw counts across differing project suites are also shown but are not cross-project rankings.",
    comparisons="Four same-project/repetition blocks compare sequential-four (left) with independent-four (right). Quality order compares whole-project acceptance first, then requirement-group coverage. Separate acceptance and coverage comparisons expose the tie-break. Repetition numbers do not imply shared provider randomness. Win score=(wins+0.5*draws)/4; a draw can mean two failed projects.",
    anchors="One strong-anchor trajectory per project is descriptive context, with different initial compute and replication; anchors are excluded from every paired comparison.",
    active="Model-trajectory elapsed plus final-evaluation elapsed after validating the whole-cohort barrier. Nested provider and Docker spans are not added to active time.",
    waiting="Wait to global provider freeze and queue after freeze are scheduling delays, separately excluded from active time.",
    provider="Physical request interval union accounts for overlap. Cumulative request duration includes network/provider wait and parsing, not pure model forward-pass time.",
    docker="Physical primary validation interval union and cumulative durations include lifecycle/host work. Post-freeze diagnostic execution is separate overhead, not model-trajectory time.",
    scouts="Admissions are validated proposals, not unique discovered bugs. Rejected proposal entries and malformed whole-batch counts remain distinct units.",
    earlier_requirements="Final-source private coverage is grouped by introducing milestone. This alone does not claim an intermediate source passed an unexecuted private case.")
LIMITATIONS = [
    "Two synthetic application domains, two repetitions per matched policy and oracle-assisted labels; no calibrated Elo, confidence interval, or general superiority claim.",
    "The matched contrast changes candidate formation under a common central controller; it does not isolate gossip transport or establish a decentralized coding advantage.",
    "Initial call opportunities are matched, not realized token use, repair demand, or final cost. Strong anchors are context only.",
    "Final cumulative private evaluation includes unreached requirements after bounded early stops; coverage measures delivered project completeness.",
    "All-attempt timing is descriptive. Accepted-only timing is conditional on success; no accepted runs yields null, and an early stop is not a speed win.",
    "Provider/Docker spans overlap parent active intervals and are nonadditive. Host contention and provider conditions can vary.",
    "Secondary selectors reuse frozen sources and observed tests, not newly executed agent policies. Pooled evidence was unavailable to the original selector.",
    "This aggregation checks retained source/certificate bindings; it is not another independent audit or external clock attestation.",
]


def regret_summary(pools, scope, selector):
    observations = [pool["counterfactuals"][scope][selector] for pool in pools]
    defined = [(pool, row) for pool, row in zip(pools, observations) if row["requirement_selection_regret"] is not None]
    selected = [row["quality"] for row in observations if row["quality"] is not None]
    return dict(pool_observations=len(pools), selected=len(selected), abstentions=len(observations) - len(selected),
        available_perfect_pools=sum(pool["correct_eligible_public_passing_source_available"] for pool in pools),
        available_perfect_but_not_selected=sum(row["correct_available_but_not_selected"] for row in observations),
        defined_regret_observations=len(defined), undefined_regret_observations=len(pools) - len(defined),
        mean_requirement_regret_fraction=(statistics.mean(row["requirement_selection_regret"] / pool["requirements_total"]
                                                        for pool, row in defined) if defined else None),
        mean_selected_requirement_coverage=(statistics.mean(row["requirements_passed"] / row["requirements_total"]
                                                           for row in selected) if selected else None))


def diagnostic_group_summary(group):
    """Keep fault-family and source-pool denominators local to project/stage."""
    surviving = [fault for fault in group["faults"] if fault["public_survived"] and fault["witness_killed"]]
    families = {fault["family"] for fault in surviving}
    probes = group["generated_probes"]
    require(len({probe["id"] for probe in probes}) == len(probes), "Duplicate diagnostic probe identity")
    by_role = {}
    for role in ("scout", "reviewer", "combined"):
        selected = [probe for probe in probes if role == "combined" or any(origin.get("source") == role for origin in probe["origins"])]
        killed = {family for probe in selected for family in probe["killed_public_surviving_families"]}
        require(killed <= families, "Probe names an unqualified fault family")
        nonempty = [tuple(probe["killed_public_surviving_fault_ids"]) for probe in selected if probe["killed_public_surviving_fault_ids"]]
        by_role[role] = dict(unique_probe_inputs=len(selected), public_surviving_families=sorted(families),
            public_surviving_family_count=len(families), families_detected=sorted(killed), families_detected_count=len(killed),
            family_detection_fraction=len(killed) / len(families) if families else None,
            probes_with_no_observed_fault_kill=sum(not probe["killed_public_surviving_fault_ids"] for probe in selected),
            duplicate_nonempty_fault_kill_vectors=len(nonempty) - len(set(nonempty)),
            candidate_discriminating_probes=sum(probe["discriminates_retained_candidates"] for probe in selected))
    scout, reviewer = (set(by_role[role]["families_detected"]) for role in ("scout", "reviewer"))
    by_role["scout"]["families_not_detected_by_reviewer"] = sorted(scout - reviewer)
    by_role["reviewer"]["families_not_detected_by_scout"] = sorted(reviewer - scout)
    pools_by_policy = {}
    for policy in POLICIES:
        phases = {}
        for phase in ("initial", "final"):
            pools = [pool for pool in group["pools"] if pool["policy"] == policy and pool["phase"] == phase]
            defined = [pool for pool in pools if pool["actual_same_pool_requirement_regret"] is not None]
            phases[phase] = dict(pool_observations=len(pools),
                actual_final_source_present=sum(pool["actual_final_selection_in_this_pool"] for pool in pools),
                actual_regret_defined_observations=len(defined), actual_regret_undefined_observations=len(pools) - len(defined),
                actual_mean_same_pool_requirement_regret_fraction=(statistics.mean(pool["actual_same_pool_requirement_regret"] / pool["requirements_total"]
                                                                                  for pool in defined) if defined else None),
                actual_available_perfect_selection_misses=sum(pool["actual_same_pool_correct_selection_miss"] is True for pool in pools),
                counterfactuals={scope: {selector: regret_summary(pools, scope, selector)
                    for selector in ("public_only", "scout", "reviewer", "combined")}
                    for scope in ("own_history", "pooled_same_project_stage")})
        pools_by_policy[policy] = dict(context_only=policy == ANCHOR, phases=phases)
    return dict(project_id=group["project_id"], stage_index=group["stage_index"],
        fault_units="Distinct family names within this project and cumulative milestone; do not add repeated families across milestones as new defects.",
        public_surviving_fault_instances=len(surviving), public_surviving_fault_families=sorted(families),
        evidence_by_origin_role=by_role, policies=pools_by_policy)


def summarize_diagnostics(primary_inputs, contract, root, audit_path):
    diagnostic_inputs, additional = Inputs(root), []
    require(not diagnostic_inputs.root.is_relative_to(primary_inputs.root)
            and not primary_inputs.root.is_relative_to(diagnostic_inputs.root), "Diagnostic evidence must be outside the frozen primary cohort")
    plan = diagnostic_inputs.read("plan.json")
    result = diagnostic_inputs.read("results.json")
    index = diagnostic_inputs.read("receipt-index.json")
    if audit_path is None:
        audit = diagnostic_inputs.read("audit.json")
    else:
        audit = read_audit(diagnostic_inputs, audit_path, additional)
    protocol = "evidence-frontier-diagnostic-v1"
    require(plan["protocol"] == result["protocol"] == index["protocol"] == protocol
            and audit.get("protocol") == "evidence-frontier-diagnostic-audit-v1" and audit.get("passed") is True
            and audit.get("inputs_unchanged") is True and result.get("status") == "finished"
            and result.get("primary_scores_changed") is False and result.get("inputs_unchanged") is True,
            "A separately certified qualified diagnostic is required")
    require(audit["plan_sha256"] == result["plan_sha256"] == index["plan_sha256"] == diagnostic_inputs.hashes["plan.json"]
            and audit["results_sha256"] == diagnostic_inputs.hashes["results.json"]
            and audit["receipt_index_sha256"] == diagnostic_inputs.hashes["receipt-index.json"],
            "Diagnostic certificate does not bind exact artifacts")
    require(Path(plan["primary_run"]).resolve() == primary_inputs.root
            and audit["input_primary_results_sha256"] == plan["input_primary_results_sha256"] == primary_inputs.hashes["results.json"]
            and audit["primary_contract_sha256"] == plan["primary_contract_sha256"] == digest(contract)
            and audit["global_freeze_sha256"] == plan["global_freeze_sha256"] == primary_inputs.hashes["frozen-trajectories.json"]
            and audit["input_timings_sha256"] == plan["input_timings_sha256"] == primary_inputs.hashes["timings.json"],
            "Diagnostic certificate belongs to different primary evidence")
    require(all(type(row["invocation_count"]) is int and row["invocation_count"] == 0 for row in (plan, index, result, audit)),
            "Secondary diagnostic cannot contain provider calls")
    for name, expected in plan["input_hashes"].items():
        primary_inputs.raw(name)
        require(primary_inputs.hashes[str(Path(name))] == expected, "Diagnostic primary input binding changed")
    diagnostic_inputs.raw("evaluator.py")
    require(diagnostic_inputs.hashes["evaluator.py"] == plan["diagnostic_source_sha256"]
            == plan["frozen_diagnostic_source_sha256"] == contract["sources"]["benchmark_diagnostics.py"],
            "Diagnostic evaluator differs from frozen contract")
    expected_groups = {(project, stage) for project in PROJECTS for stage in range(2)}
    identities = [(group["project_id"], group["stage_index"]) for group in result["groups"]]
    require(len(identities) == len(set(identities)) == audit["audited_groups"] == 4 and set(identities) == expected_groups
            and all(group["qualified"] is True for group in result["groups"]), "Diagnostic group qualification or roster differs")
    jobs = {(group["project_id"], group["stage_index"], source_id) for group in plan["groups"] for source_id in group["sources"]}
    rows = index["rows"]
    row_ids = [(row["project_id"], row["stage_index"], row["source_id"]) for row in rows]
    require(len(rows) == len(set(row_ids)) == len(jobs) == audit["verified_matrix_rows"]
            == result["physical_candidate_executions"] == index["planned_jobs"]
            == index["evaluation_attempts"] == index["confirmed_candidate_executions"] and set(row_ids) == jobs,
            "Diagnostic physical execution roster/count differs")
    spans = []
    for row in rows:
        require(diagnostic_inputs.read(f"rows/{row['job_id']}.json") == row, "Diagnostic row sidecar changed")
        receipt_path = row["receipt_path"]
        diagnostic_inputs.raw(receipt_path)
        require(diagnostic_inputs.hashes[str(Path(receipt_path))] == row["receipt_sha256"], "Diagnostic receipt bytes changed")
        require(row["status"] == "verified" and row["physical_execution"] is True and row["evaluation_attempted"] is True
                and row["started_monotonic_ns"] >= plan["freeze_monotonic_ns"], "Diagnostic execution is not verified after freeze")
        spans.append(dict(clock_id="one-diagnostic-process-monotonic-clock", status="finished",
                          **{key: row[key] for key in ("started_monotonic_ns", "finished_monotonic_ns", "elapsed_seconds")}))
    groups = [diagnostic_group_summary(group) for group in result["groups"]]
    return dict(status="separately_certified", root=str(diagnostic_inputs.root), invocation_count=0,
        physical_candidate_executions=len(rows), diagnostic_validation_timing=interval_summary(spans),
        groups=groups, retained_group_details=deepcopy(result["groups"]),
        definitions=dict(selection_regret="Private requirement coverage gap relative to the best originally eligible public-passing source in the same frozen pool. Undefined/abstained/out-of-pool results remain null, not zero.",
            source_pools="Initial and final eligible pools are distinct. The best-ever retained-source ceiling includes sources outside the final pool and may include originally ineligible occurrences; its labeled scope is preserved in retained details.",
            evidence_scope="Own-history and pooled same-project/stage evidence are separate retrospective selectors. Reviewer evidence on an initial pool may postdate repairs. Pooled evidence was unavailable during live selection.",
            fault_utility="Distinct public-surviving fault families are local project/stage denominators. Multiple origins or mutants in one family do not multiply family credit. A probe attributed to both scout and reviewer appears in both origin sets, so those sets are nonadditive.",
            nulls="An empty fault-family denominator, absent pool, undefined same-pool regret, or no selected source produces null rates; absence is not perfect performance."),
        limitations=deepcopy(result.get("limitations", []))), [diagnostic_inputs, *additional]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--audit", type=Path)
    parser.add_argument("--diagnostics", type=Path)
    parser.add_argument("--diagnostic-audit", type=Path)
    args = parser.parse_args()
    output = args.output.resolve()
    require(not output.is_relative_to(args.run.resolve()), "Write new analysis outside frozen cohort")
    if args.diagnostics is not None:
        require(not output.is_relative_to(args.diagnostics.resolve()), "Write new analysis outside diagnostic evidence")
    result = summarize(args.run, diagnostics=args.diagnostics, diagnostic_audit=args.diagnostic_audit, audit_path=args.audit)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x") as stream:
        stream.write(json.dumps(result, indent=2, allow_nan=False) + "\n")
    print(json.dumps(dict(status=result["status"], output=str(args.output), comparisons=result["comparisons"])))


if __name__ == "__main__":
    main()
