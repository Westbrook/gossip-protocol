"""Preregistered, read-only comparison of certified continuation-followup v1.

Consumes retained JSON and source bytes only. Never imports or executes runner,
controller, fixture, candidate or auditor code. Shared work is charged once;
partial cohorts remain unscored and are never promoted into policy rankings.
"""
from __future__ import annotations

import argparse
from collections import Counter
from fractions import Fraction
import hashlib
import json
import math
from pathlib import Path
import statistics

PROTOCOL = "continuation-followup-v1"
ANALYSIS_VERSION = "continuation-followup-descriptive-comparison-v1"
ANALYSIS_PATH = "analysis/continuation_followup_comparison.py"
AUDIT_PROTOCOL = "independent-continuation-followup-cohort-audit-v2"
PROJECTS = ("warehouse", "job-queue")
POLICIES = ("current-independent", "improved-independent", "improved-sequential")
CONTRASTS = {
    "controller_package": ("improved-independent", "current-independent"),
    "candidate_formation": ("improved-independent", "improved-sequential"),
}
POLICY_CONTRACT = {policy: dict(controller="current" if policy == POLICIES[0] else "improved",
    formation="sequential" if policy == POLICIES[2] else "independent", initial_builders=4, model="cheap") for policy in POLICIES}
TIME_FIELDS = ("arm_model_work_seconds", "final_adjudication_seconds", "arm_active_seconds",
               "wait_after_shared_setup_seconds", "wait_to_global_freeze_seconds",
               "post_freeze_adjudication_queue_seconds", "arm_start_to_final_seconds")
REQUIREMENT_PREFIXES = {"warehouse": ("W0-", "W1-", "W2-"), "job-queue": ("Q0-", "Q1-", "Q2-")}
REPAIR_KEYS = ("repairs", "effective_source_changes", "no_effective_source_proposals", "retained_repair_checkpoints",
    "repair_regressions", "repair_failure_progress", "resolved_known_failures", "introduced_failures",
    "alternative_routes", "focused_review_requests", "focused_evidence_steps", "stagnation_events", "escalations")

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
    merged: list[tuple[int, int]] = []
    for start, end in sorted(intervals):
        if merged and start < merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    cumulative, union = sum(b - a for a, b in intervals), sum(b - a for a, b in merged)
    return dict(span_count=len(spans), cumulative_seconds=cumulative / 1e9,
                union_wall_seconds=union / 1e9, overlap_seconds=(cumulative - union) / 1e9,
                per_span=distribution([row["elapsed_seconds"] for row in spans]))


def bound(inputs, binding):
    path = Path(binding["path"])
    if path.is_absolute():
        require(path.resolve().is_relative_to(inputs.root), "Bound artifact escapes cohort")
        path = path.relative_to(inputs.root)
    value = inputs.read(path)
    require(inputs.hashes[str(path)] == binding["sha256"], "Bound artifact bytes changed")
    return value


def fraction_value(value):
    value = Fraction(value)
    return dict(numerator=value.numerator, denominator=value.denominator, approximate=float(value))


def allocation(parts):
    """Joint-study bookkeeping, deliberately not a standalone-policy estimate."""
    result = {}
    for policy in POLICIES:
        divisor = 1 if policy == "improved-sequential" else 2
        formation = POLICY_CONTRACT[policy]["formation"]
        result[policy] = {key: Fraction(parts[formation][key], divisor) + Fraction(parts["scouts"][key], 3)
                          for key in ("usage_micro_usd", "physical_provider_calls")}
    for key in ("usage_micro_usd", "physical_provider_calls"):
        require(sum(row[key] for row in result.values()) == sum(row[key] for row in parts.values()),
                "Shared allocation does not reconcile exactly")
    return result


def requirement_summary(coverage, project, taxonomy):
    categories = taxonomy[project]
    require(set(categories) == {"baseline_inherited", "milestone_1_added", "milestone_2_added"}, "Requirement taxonomy categories differ")
    prefixes = tuple(categories.values())
    require(all(type(value) is bool for value in coverage.values()) and bool(coverage)
            and all(sum(name.startswith(prefix) for prefix in prefixes) == 1 for name in coverage),
            "Unclassified or nonboolean named requirement")
    result = {}
    for category, prefix in categories.items():
        groups = {key: value for key, value in coverage.items() if key.startswith(prefix)}
        require(groups, "Missing preregistered requirement category")
        result[category] = dict(passed=sum(groups.values()), total=len(groups), fraction=sum(groups.values()) / len(groups), groups=groups)
    added = {key: value for key, value in coverage.items() if not key.startswith(categories["baseline_inherited"])}
    result["all_added"] = dict(passed=sum(added.values()), total=len(added), fraction=sum(added.values()) / len(added), groups=added)
    return result


def paired(rows, left, right, metric):
    require(metric in ("quality_ordered", "whole_project_acceptance", "requirement_coverage"), "Unknown quality comparison")
    selected = [row for row in rows if row["policy"] in (left, right)]
    require(all(type(row["repetition"]) is int for row in selected), "Noninteger repetition identity")
    index = {(row["project_id"], row["repetition"], row["policy"]): row for row in selected}
    require(len(selected) == len(index) == 8 and set(index) == {(p, r, a) for p in PROJECTS for r in range(2) for a in (left, right)},
            "Exact four-block paired roster required")
    matches = []
    for project in PROJECTS:
        for rep in range(2):
            a, b = index[(project, rep, left)], index[(project, rep, right)]
            require(a["requirements_total"] == b["requirements_total"] > 0, "Within-block requirement denominator differs")
            def value(row):
                coverage = Fraction(row["requirements_passed"], row["requirements_total"])
                if metric == "whole_project_acceptance":
                    return int(row["accepted"])
                return (int(row["accepted"]), coverage) if metric == "quality_ordered" else coverage
            av, bv = value(a), value(b)
            matches.append(dict(project_id=project, repetition=rep, left_run=a["run_id"], right_run=b["run_id"],
                left_accepted=a["accepted"], right_accepted=b["accepted"], left_requirements_passed=a["requirements_passed"],
                right_requirements_passed=b["requirements_passed"], requirements_total=a["requirements_total"],
                outcome="win" if av > bv else "loss" if av < bv else "draw"))
    def count(items):
        counts = Counter(row["outcome"] for row in items)
        return dict(blocks=len(items), wins=counts["win"], draws=counts["draw"], losses=counts["loss"],
                    descriptive_win_score=(counts["win"] + .5 * counts["draw"]) / len(items))
    return dict(left_policy=left, right_policy=right, metric=metric, **count(matches), application_domains=2,
        source_trajectories_per_policy=4, matches=matches,
        per_project={p: count([row for row in matches if row["project_id"] == p]) for p in PROJECTS})


def check_contract(inputs, report):
    contract = report["contract"]
    preregistered, plan = inputs.read("preregistered.json"), inputs.read("study-plan.json")
    require(contract["protocol"] == PROTOCOL and report["contract_sha"] == digest(contract)
            and preregistered["contract"] == contract and contract["policies"] == POLICY_CONTRACT
            and type(contract["milestones"]) is int and contract["milestones"] == 2
            and type(contract["repetitions"]) is int and contract["repetitions"] == 2,
            "Frozen follow-up contract mismatch")
    require(inputs.hashes["study-plan.json"] == contract["plan_sha256"] and plan["protocol"] == PROTOCOL
            and plan["policies"] == POLICY_CONTRACT and tuple(plan["projects"]) == PROJECTS,
            "Frozen study plan mismatch")
    analysis = plan["pilot_analysis"]
    require(analysis["version"] == ANALYSIS_VERSION and analysis["primary_contrasts"] == [list(pair) for pair in CONTRASTS.values()],
            "Preregistered primary contrasts changed")
    require(contract["pilot_analysis"] == analysis and all(analysis["requirement_taxonomy"][p] == dict(zip(("baseline_inherited", "milestone_1_added", "milestone_2_added"), REQUIREMENT_PREFIXES[p])) for p in PROJECTS), "Frozen requirement taxonomy differs")
    roster = contract["roster"]
    require(type(roster) is list and len(roster) == 12
            and all(type(row) is list and len(row) == 3 and type(row[2]) is int for row in roster)
            and len({tuple(row) for row in roster}) == 12
            and {tuple(row) for row in roster} == {(p, a, r) for p in PROJECTS for a in POLICIES for r in range(2)}
            and plan["roster"] == roster, "Exact twelve-arm roster mismatch")
    require(len(contract["block_order"]) == 4 and all(type(row[1]) is int for row in contract["block_order"])
            and {tuple(row) for row in contract["block_order"]} == {(p, r) for p in PROJECTS for r in range(2)}
            and contract["block_order"] == plan["block_order"], "Exact four shared blocks required")
    for name, expected in contract["sources"].items():
        path = "source-snapshot/gossip_harness/" + name
        inputs.raw(path)
        require(inputs.hashes[path] == expected, "Frozen scientific source changed")
    for name, expected in contract["extra_sources"].items():
        path = "source-snapshot/" + name
        inputs.raw(path)
        require(inputs.hashes[path] == expected, "Frozen extra source changed")
    require(contract["extra_sources"].get(ANALYSIS_PATH) == hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "Comparison implementation differs from preregistered source")
    return contract, plan


def finish(inputs, result, external=()):
    require(inputs.unchanged() and all(item.unchanged() for item in external), "Evidence changed during comparison")
    result["bindings"] = dict(primary_root=str(inputs.root), inputs_sha256=dict(sorted(inputs.hashes.items())),
        additional_inputs=[dict(root=str(item.root), inputs_sha256=dict(sorted(item.hashes.items()))) for item in external],
        inputs_unchanged=True, analysis_source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
    return result


def unscored(inputs, report, contract):
    trajectories = report.get("trajectories", [])
    recorded = {row["run_id"]: row for row in trajectories}
    planned = {f"{p}-{a}-{r}" for p, a, r in contract["roster"]}
    require(len(recorded) == len(trajectories) and set(recorded) <= planned, "Invalid partial trajectory inventory")
    censored = {row.get("run_id"): row for row in report.get("censored", [])}
    missing = {tuple(row) for row in report.get("unexecuted", [])}
    active = report.get("active_case") or {}
    rows = []
    for project, policy, rep in contract["roster"]:
        run_id = f"{project}-{policy}-{rep}"
        status = ("censored" if run_id in censored else "active" if run_id == active.get("run_id") else
                  "unexecuted" if (project, policy, rep) in missing else "terminal_not_certified" if run_id in recorded else "not_recorded")
        rows.append(dict(run_id=run_id, project_id=project, policy=policy, repetition=rep, execution_status=status,
            recorded_terminal=recorded.get(run_id, {}).get("terminal"), failure_reason=censored.get(run_id, {}).get("reason")))
    return finish(inputs, dict(schema_version=1, analysis_version=ANALYSIS_VERSION, cohort=PROTOCOL,
        status="incomplete_unscored", mode=report["mode"], source_status=report["status"], source_phase=report["phase"],
        execution="Read-only inventory; no API calls or candidate execution. Embedded private scores are not inspected or reported; separate private receipts are not read.",
        certification="No complete-cohort certificate; execution fields are retained report claims only.",
        contract_sha256=digest(contract), planned_trajectories=12, terminal_trajectories=len(recorded),
        trajectories=rows, projects=None, policies=None, contrasts=None, accounting=None, shared_setups=None,
        elo=None, population_p_values=None, ranking=None, failure_type=report.get("failure_type"),
        censored=report.get("censored", []), reported_incremental_micro_usd=report.get("incremental_micro_usd"),
        limitations=["Missing and failed work is retained without quality or speed rankings.", "Partial finalization does not permit publishing completed-cohort scores."]))


def read_certificate(inputs, audit_path):
    if audit_path is None:
        return inputs.read("independent-audit.json"), []
    path = Path(audit_path).resolve()
    if path.is_relative_to(inputs.root):
        return inputs.read(path.relative_to(inputs.root)), []
    other = Inputs(path.parent)
    return other.read(path.name), [other]


def call_summary(calls, spans, used_physical, allowed_failed, *, parent):
    require(all(type(call["physical_dispatch"]) is bool and type(call["usage_units"]) is int and call["usage_units"] >= 0 for call in calls),
            "Invalid physical-call or usage label")
    expected = [call["physical_span_id"] for call in calls if call["physical_dispatch"]]
    require(len(expected) == len(set(expected)) and not used_physical.intersection(expected), "Physical provider request double counted")
    by_id = {span["span_id"]: span for span in spans}
    physical = []
    for call in calls:
        for key in ("physical_span_id", "request_span_id"):
            identity = call.get(key)
            if identity is None:
                continue
            require(identity in by_id, "Recorded invocation span missing")
            observed = by_id[identity]
            if observed["status"] == "failed":
                require(observed["kind"] in ("physical_provider_request", "scripted_worker", "worker_request")
                        and call.get("outcome") == "failure" and not call.get("metadata", {}).get("halt")
                        and all(observed[name] == call[name] for name in ("call_id", "role", "model"))
                        and parent["started_monotonic_ns"] <= observed["started_monotonic_ns"]
                        <= observed["finished_monotonic_ns"] <= parent["finished_monotonic_ns"],
                        "Failed invocation is not a settled known outcome")
                allowed_failed.add(identity)
    for call in calls:
        if not call["physical_dispatch"]:
            continue
        require(call["physical_span_id"] in by_id, "Physical provider span missing")
        span = by_id[call["physical_span_id"]]
        require(span["kind"] == "physical_provider_request" and span["status"] in ("finished", "failed")
                and all(span[key] == call[key] for key in ("call_id", "model", "role"))
                and parent["started_monotonic_ns"] <= span["started_monotonic_ns"] <= span["finished_monotonic_ns"] <= parent["finished_monotonic_ns"],
                "Physical provider identity or parent binding mismatch")
        physical.append(span)
    require({span["span_id"] for span in spans if span["kind"] == "physical_provider_request"} == set(expected),
            "Unlinked physical provider request")
    used_physical.update(expected)
    return dict(usage_micro_usd=sum(call["usage_units"] for call in calls), physical_provider_calls=len(physical), invocations=len(calls),
        invocations_by_role=dict(Counter(call["role"] for call in calls)),
        usage_by_role_micro_usd={role: sum(call["usage_units"] for call in calls if call["role"] == role) for role in sorted({call["role"] for call in calls})},
        physical_provider_timing=interval_summary(physical))


def repairs_summary(stages, mode):
    metrics = {key: sum(stage["metrics"].get(key, 0) for stage in stages) for key in REPAIR_KEYS}
    comparisons = [dict(stage_index=index, **comparison) for index, stage in enumerate(stages)
                   for comparison in stage.get("repair_comparisons", [])]
    if mode == "improved":
        require(len(comparisons) == metrics["retained_repair_checkpoints"] == metrics["repairs"], "Retained repair count differs")
        require(sum(row["regression"] is True for row in comparisons) == metrics["repair_regressions"]
                and sum(row["failure_progress"] is True for row in comparisons) == metrics["repair_failure_progress"]
                and sum(len(row["resolved_failures"]) for row in comparisons) == metrics["resolved_known_failures"]
                and sum(len(row["introduced_failures"]) for row in comparisons) == metrics["introduced_failures"], "Repair regression/progress aggregates differ")
        for row in comparisons:
            require(type(row["regression"]) is bool and type(row["failure_progress"]) is bool
                    and row["regression"] is bool(row["introduced_failures"])
                    and row["failure_progress"] is bool(row["resolved_failures"] and not row["introduced_failures"]),
                    "New source or net pass gain cannot stand in for failure progress")
    else:
        require(not comparisons, "Current controller unexpectedly claims improved repair instrumentation")
    return dict(metrics=metrics, comparison_observations=len(comparisons), comparisons=comparisons,
        same_case_regressions_observed=metrics["repair_regressions"] if mode == "improved" else None,
        same_case_failure_progress_observed=metrics["repair_failure_progress"] if mode == "improved" else None,
        instrumentation="Source-bound identical-case comparisons" if mode == "improved" else "No equivalent retained same-case repair comparison; zero metric fields are not evidence of zero regressions.",
        terminal_reasons=[dict(stage_index=index, completed=stage["completed"], reason_code=stage.get("reason_code"),
                               reason=stage.get("reason"), remaining=stage.get("remaining", [])) for index, stage in enumerate(stages)])


def summarize_policy(rows):
    def sum_map(key):
        return {name: sum(row[key].get(name, 0) for row in rows) for name in sorted({name for row in rows for name in row[key]})}
    return dict(trajectories=len(rows), accepted=sum(row["accepted"] for row in rows),
        bounded_stops=sum(row["milestones_completed"] < 2 for row in rows),
        milestones_completed=sum(row["milestones_completed"] for row in rows), milestones_total=2 * len(rows),
        requirements_passed=sum(row["requirements_passed"] for row in rows), requirements_total=sum(row["requirements_total"] for row in rows),
        mean_requirement_coverage=statistics.mean(row["requirements_passed"] / row["requirements_total"] for row in rows),
        requirement_categories={category: dict(passed=sum(row["requirement_categories"][category]["passed"] for row in rows),
            total=sum(row["requirement_categories"][category]["total"] for row in rows),
            mean_fraction=statistics.mean(row["requirement_categories"][category]["fraction"] for row in rows))
            for category in ("baseline_inherited", "milestone_1_added", "milestone_2_added", "all_added")},
        terminal_outcomes=dict(Counter(row["status"] for row in rows)), arm_usage_micro_usd=sum(row["usage_micro_usd"] for row in rows),
        arm_physical_provider_calls=sum(row["physical_provider_calls"] for row in rows), arm_invocations=sum(row["invocations"] for row in rows),
        arm_invocations_by_role=sum_map("invocations_by_role"), arm_usage_by_role_micro_usd=sum_map("usage_by_role_micro_usd"),
        repair_metrics={key: sum(row["repairs"]["metrics"][key] for row in rows) for key in REPAIR_KEYS},
        measured_same_case_regressions=(sum(row["repairs"]["same_case_regressions_observed"] for row in rows)
                                       if all(row["repairs"]["same_case_regressions_observed"] is not None for row in rows) else None),
        measured_same_case_failure_progress=(sum(row["repairs"]["same_case_failure_progress_observed"] for row in rows)
                                            if all(row["repairs"]["same_case_failure_progress_observed"] is not None for row in rows) else None),
        timing=dict(all_attempts={key: distribution([row["timing"][key] for row in rows]) for key in TIME_FIELDS},
            accepted_arm_active=distribution([row["timing"]["arm_active_seconds"] for row in rows if row["accepted"]]),
            bounded_stop_arm_active=distribution([row["timing"]["arm_active_seconds"] for row in rows if row["milestones_completed"] < 2]),
            complete_but_quality_rejected_arm_active=distribution([row["timing"]["arm_active_seconds"] for row in rows
                                                                 if row["milestones_completed"] == 2 and not row["accepted"]])))


def summarize(root, *, audit_path=None):
    inputs = Inputs(root)
    report = inputs.read("results.json")
    require(report.get("experiment") == PROTOCOL and report.get("mode") in ("live", "rehearsal"), "Unknown follow-up cohort")
    contract, plan = check_contract(inputs, report)
    if not (report.get("status") == report.get("phase") == "finished" and not report.get("unexecuted")
            and not report.get("censored") and report.get("active_case") is None):
        return unscored(inputs, report, contract)
    audit, external = read_certificate(inputs, audit_path)
    freeze, timing, fixture = inputs.read("frozen-trajectories.json"), inputs.read("timings.json"), inputs.read("fixtures.json")
    freeze_sha = inputs.hashes["frozen-trajectories.json"]
    require(audit.get("protocol") == AUDIT_PROTOCOL and audit.get("passed") is True
            and audit.get("mode") == report["mode"]
            and audit.get("scope", {}).get("primary_cohort") is True
            and audit.get("scope", {}).get("live_model_quality") is (report["mode"] == "live")
            and audit.get("results_sha256") == inputs.hashes["results.json"]
            and audit.get("timings_sha256") == inputs.hashes["timings.json"]
            and audit.get("contract_sha256") == digest(contract)
            and type(contract["extra_sources"].get("analysis/audit_continuation_followup.py")) is str
            and audit.get("auditor_sha256") == contract["extra_sources"]["analysis/audit_continuation_followup.py"],
            "Successful exact whole-cohort audit required")
    require(report["freeze_manifest_sha256"] == audit["freeze_manifest_sha256"] == freeze_sha
            and freeze["protocol"] == PROTOCOL and freeze["contract_sha"] == digest(contract)
            and freeze["private_evaluation_started"] is False, "Whole-cohort freeze binding differs")
    barrier = freeze["frozen_monotonic_ns"]
    require(type(barrier) is int and barrier >= 0, "Invalid global freeze time")
    projects = {project["id"]: project for project in fixture["projects"]}
    require(len(fixture["projects"]) == len(projects) == 2 and set(projects) == set(PROJECTS), "Fixture roster differs")
    for project_id, project in projects.items():
        require(digest(project) == contract["fixtures"][project_id]["fixture_sha256"] and len(project["stages"]) == 2, "Frozen fixture bytes differ")
    identities = lambda rows: [[row["project_id"], row["policy"], row["repetition"]] for row in rows]
    require(all(type(row["repetition"]) is int for row in [*report["cases"], *freeze["trajectories"]])
            and identities(report["cases"]) == identities(freeze["trajectories"]) == contract["roster"], "Final or frozen twelve-arm roster differs")
    run_ids = [f"{p}-{a}-{r}" for p, a, r in contract["roster"]]
    audited = {row["run_id"]: row for row in audit["cases"]}
    terminal = {row["run_id"]: row for row in report["trajectories"]}
    require(len(audit["cases"]) == len(audited) == len(report["trajectories"]) == len(terminal) == 12
            and set(audited) == set(terminal) == set(run_ids), "Audited or timed arm roster differs")
    spans = timing["spans"]
    require(len({span["span_id"] for span in spans}) == len(spans)
            and all(span["clock_id"] == timing["clock_id"] and span["status"] in ("finished", "failed") for span in spans), "Unfinished or duplicate cohort timing")
    interval_summary(spans)
    by_id = {span["span_id"]: span for span in spans}
    setup_rows = report["shared_setups"]
    require(len(setup_rows) == 4 and all(type(row["repetition"]) is int for row in setup_rows)
            and [[row["project_id"], row["repetition"]] for row in setup_rows] == contract["block_order"], "Four shared setups must match block order")
    require([[row["project_id"], row["repetition"]] for row in freeze["shared_setups"]] == contract["block_order"]
            and all(a["manifest"] == b["manifest"] for a, b in zip(setup_rows, freeze["shared_setups"])), "Shared setup differs from global freeze")
    setup_audits = {(row["project_id"], row["repetition"]): row for row in audit["shared_setups"]}
    require(len(audit["shared_setups"]) == len(setup_audits) == 4 and set(setup_audits) == {tuple(row) for row in contract["block_order"]}, "Shared setup audit roster differs")
    used_physical: set[int] = set()
    allowed_failed: set[int] = set()
    shared, setup_index, measured_parents = [], {}, []
    allowed_span_owners = set(run_ids)
    for setup in setup_rows:
        project, rep = setup["project_id"], setup["repetition"]
        identity = (project, rep)
        manifest = bound(inputs, setup["manifest"])
        require(manifest["protocol"] == "continuation-followup-shared-setup-v1" and manifest["project_id"] == project
                and type(manifest["repetition"]) is int and manifest["repetition"] == rep
                and manifest["contract_sha256"] == digest(contract)
                and setup_audits[identity]["manifest_sha256"] == setup["manifest"]["sha256"], "Shared setup identity or certificate differs")
        parent = by_id[setup["span_id"]]
        require(parent["kind"] == "shared_setup_block" and parent["status"] == "finished" and parent["run_id"] == f"{project}-{rep}-shared"
                and parent["elapsed_seconds"] == setup["elapsed_seconds"] and parent["finished_monotonic_ns"] <= barrier,
                "Shared setup parent timing differs")
        allowed_span_owners.add(parent["run_id"])
        measured_parents.append(parent)
        parts, all_calls, all_owned = {}, [], []
        for formation in ("independent", "sequential", "scouts"):
            binding = manifest["scouts"]["session"] if formation == "scouts" else manifest["pools"][formation]["session"]
            session = bound(inputs, binding)
            session_id = f"{project}-{rep}-shared-{formation}"
            require(session["session_id"] == session_id, "Shared session identity differs")
            allowed_span_owners.add(session_id)
            own = [span for span in spans if span.get("run_id") == session_id]
            calls = session["invocations"]
            require(len(calls) == (2 if formation == "scouts" else 4)
                    and all(call["role"] == ("scout" if formation == "scouts" else "builder") for call in calls), "Shared generation opportunity count differs")
            counts = call_summary(calls, own, used_physical, allowed_failed, parent=parent)
            require(all(counts[key] == session[key] for key in ("usage_micro_usd", "physical_provider_calls")), "Shared session accounting differs")
            parts[formation] = counts
            all_calls.extend(calls)
            all_owned.extend(own)
        require(sorted(map(digest, all_calls)) == sorted(map(digest, setup["invocations"])), "Shared calls differ from retained sessions")
        for key in ("usage_micro_usd", "physical_provider_calls"):
            require(sum(part[key] for part in parts.values()) == setup[key] == manifest[key] == setup_audits[identity][key], "Physical shared setup counted inconsistently")
        docker = [span for span in all_owned if span["kind"] == "docker_validation"]
        require(all(span["purpose"] == "stage_evidence" and parent["started_monotonic_ns"] <= span["started_monotonic_ns"]
                    <= span["finished_monotonic_ns"] <= parent["finished_monotonic_ns"] for span in docker), "Shared validation escaped setup parent")
        allocated = allocation(parts)
        row = dict(project_id=project, repetition=rep, manifest_sha256=setup["manifest"]["sha256"],
            physical_usage_micro_usd=setup["usage_micro_usd"], physical_provider_calls=setup["physical_provider_calls"], invocations=len(all_calls),
            parts=parts, timing=dict(shared_setup_wall_seconds=parent["elapsed_seconds"],
                physical_provider=interval_summary([span for span in all_owned if span["kind"] == "physical_provider_request"]),
                docker_validation=interval_summary(docker)),
            optional_joint_experiment_allocation={policy: {key: fraction_value(value) for key, value in values.items()} for policy, values in allocated.items()})
        shared.append(row)
        setup_index[identity] = dict(manifest=manifest, binding=setup["manifest"], parent=parent, allocation=allocated)
    rows = []
    for case, frozen, run_id in zip(report["cases"], freeze["trajectories"], run_ids):
        require(case["run_id"] == frozen["run_id"] == run_id, "Arm identity differs")
        state = inputs.read(f"{run_id}/trajectory.json")
        require(inputs.hashes[f"{run_id}/trajectory.json"] == case["trajectory_sha256"] == frozen["trajectory"]["sha256"]
                and inputs.read(f"{run_id}/result.json") == case
                and inputs.read(f"{run_id}/final-private-receipt.json") == case["final_hidden"]
                and inputs.read(f"{run_id}/final-visible-receipt.json") == case["final_visible"], "Retained final arm evidence differs")
        stages = state["stages"]
        require(1 <= len(stages) <= 2 and all(type(stage["completed"]) is bool for stage in stages)
                and all(stage["completed"] for stage in stages[:-1]), "Arm skipped or invented milestone completion")
        complete = len(stages) == 2 and all(stage["completed"] for stage in stages)
        require(type(case["accepted"]) is bool and case["accepted"] is audited[run_id]["accepted"]
                and case["accepted"] is bool(complete and case["final_visible"]["passed"] and case["final_hidden"]["passed"])
                and case["milestones_completed"] == audited[run_id]["milestones_completed"] == sum(stage["completed"] for stage in stages)
                == sum(stage["completed"] is True for stage in audited[run_id]["stages"]), "Audited completion/acceptance differs")
        project = projects[case["project_id"]]
        expected = [row for stage in project["stages"] for row in stage["hidden_cases"]]
        outcomes = case["final_hidden"]["outcomes"]
        require([(row["id"], row["requirement"]) for row in outcomes] == [(row["id"], row["requirement"]) for row in expected]
                and all(type(row["passed"]) is bool for row in outcomes), "Final private case identity differs")
        coverage = {name: all(row["passed"] for row in outcomes if row["requirement"] == name) for name in sorted({row["requirement"] for row in outcomes})}
        require(coverage == case["requirement_coverage"], "Named requirement coverage differs")
        own = [span for span in spans if span.get("run_id") == run_id]
        model, final = ([span for span in own if span["kind"] == kind] for kind in ("model_trajectory", "final_evaluation"))
        require(len(model) == len(final) == 1, "Arm requires one measured work and final-adjudication interval")
        model, final = model[0], final[0]
        setup = setup_index[(case["project_id"], case["repetition"])]
        require(model["status"] == final["status"] == "finished" and setup["parent"]["finished_monotonic_ns"] <= model["started_monotonic_ns"]
                and model["finished_monotonic_ns"] <= barrier <= final["started_monotonic_ns"], "Arm work violates setup/final barrier")
        require(terminal[run_id]["span_id"] == model["span_id"] and terminal[run_id]["elapsed_seconds"] == model["elapsed_seconds"]
                and terminal[run_id]["trajectory_sha256"] == case["trajectory_sha256"], "Arm timing summary differs")
        measured_parents.extend((model, final))
        calls = case["invocations"]
        require(calls == [call for stage in stages for call in stage["invocations"]], "Arm invocation roster differs")
        counts = call_summary(calls, own, used_physical, allowed_failed, parent=model)
        require(all(case[key] == counts[key] for key in ("usage_micro_usd", "physical_provider_calls")), "Arm usage differs from physical calls")
        attribution = case["shared_import"]
        require(attribution == state["shared_import"] == stages[0]["shared_import"]
                and attribution["initial_pool"] == setup["manifest"]["pools"][POLICY_CONTRACT[case["policy"]]["formation"]]["initial_pool"]
                and attribution["manifest"] == setup["binding"] and attribution["physical_provider_calls"] == attribution["charged_micro_usd"] == 0
                and attribution["attributed_builder_opportunities"] == 4 and attribution["attributed_scout_opportunities"] == 2,
                "Imported setup incorrectly charged or attributed")
        if "formation" in attribution:
            require(attribution["formation"] == POLICY_CONTRACT[case["policy"]]["formation"], "Wrong imported pool formation")
        docker = [span for span in own if span["kind"] == "docker_validation"]
        for span in docker:
            require(span.get("purpose") in ("stage_evidence", "final_visible", "final_private"), "Unknown arm validation purpose")
            parent = model if span["purpose"] == "stage_evidence" else final
            require(parent["started_monotonic_ns"] <= span["started_monotonic_ns"] <= span["finished_monotonic_ns"] <= parent["finished_monotonic_ns"], "Arm Docker span escaped measured parent")
        metric_totals = {key: sum(stage["metrics"].get(key, 0) for stage in stages) for key in {key for stage in stages for key in stage["metrics"]}}
        require(metric_totals == case["metrics"], "Controller metric totals differ")
        repaired = repairs_summary(stages, POLICY_CONTRACT[case["policy"]]["controller"])
        allocation_row = setup["allocation"][case["policy"]]
        rows.append(dict(run_id=run_id, project_id=case["project_id"], policy=case["policy"], repetition=case["repetition"],
            status=case["status"], accepted=case["accepted"], milestones_completed=case["milestones_completed"],
            milestones_total=2, requirements_passed=sum(coverage.values()), requirements_total=len(coverage), requirement_coverage=coverage,
            requirement_categories=requirement_summary(coverage, case["project_id"], plan["pilot_analysis"]["requirement_taxonomy"]),
            private_cases_passed=sum(row["passed"] for row in outcomes), private_cases_total=len(outcomes), **counts,
            imported_builder_opportunities=4, imported_scout_opportunities=2, shared_setup_manifest_sha256=setup["binding"]["sha256"],
            repairs=repaired, metrics=metric_totals,
            optional_joint_experiment_allocation={key: fraction_value(value) for key, value in allocation_row.items()},
            timing=dict(arm_model_work_seconds=model["elapsed_seconds"], final_adjudication_seconds=final["elapsed_seconds"],
                arm_active_seconds=model["elapsed_seconds"] + final["elapsed_seconds"],
                wait_after_shared_setup_seconds=(model["started_monotonic_ns"] - setup["parent"]["finished_monotonic_ns"]) / 1e9,
                wait_to_global_freeze_seconds=(barrier - model["finished_monotonic_ns"]) / 1e9,
                post_freeze_adjudication_queue_seconds=(final["started_monotonic_ns"] - barrier) / 1e9,
                arm_start_to_final_seconds=(final["finished_monotonic_ns"] - model["started_monotonic_ns"]) / 1e9,
                physical_provider=counts["physical_provider_timing"], docker_validation=interval_summary(docker))))
    require(all(span["kind"] == "study" or span.get("run_id") in allowed_span_owners for span in spans), "Unknown timing owner")
    require({span["span_id"] for span in spans if span["status"] == "failed"} == allowed_failed, "Unbound failed execution span")
    require(used_physical == {span["span_id"] for span in spans if span["kind"] == "physical_provider_request"}, "Physical requests missing from accounting")
    arm_cost, setup_cost = sum(row["usage_micro_usd"] for row in rows), sum(row["physical_usage_micro_usd"] for row in shared)
    require(arm_cost + setup_cost == report["incremental_micro_usd"] == audit["accounting"]["study_usage_micro_usd"], "Shared plus arm accounting does not reconcile")
    if report["mode"] == "rehearsal":
        require(arm_cost + setup_cost == 0 and not used_physical, "Scripted rehearsal cannot claim paid provider work")
    per_project = {p: {a: summarize_policy([row for row in rows if row["project_id"] == p and row["policy"] == a]) for a in POLICIES} for p in PROJECTS}
    policies = {a: {**summarize_policy([row for row in rows if row["policy"] == a]),
                    "equal_project_mean_requirement_coverage": statistics.mean(per_project[p][a]["mean_requirement_coverage"] for p in PROJECTS)} for a in POLICIES}
    return finish(inputs, dict(schema_version=1, analysis_version=ANALYSIS_VERSION, cohort=PROTOCOL, status="certified_complete",
        mode=report["mode"], evidence_class="scripted_infrastructure_control" if report["mode"] == "rehearsal" else "exploratory_live_model_observation",
        model_quality_evidence=report["mode"] == "live", run=str(inputs.root), contract_sha256=digest(contract), execution="Retained independently audited evidence only; no API calls or candidate execution.",
        application_domains=2, development_blocks=4, trajectories=rows, projects=per_project, policies=policies, shared_setups=shared,
        contrasts={name: {metric: paired(rows, left, right, metric) for metric in ("quality_ordered", "whole_project_acceptance", "requirement_coverage")}
                   for name, (left, right) in CONTRASTS.items()},
        accounting=dict(arm_usage_micro_usd=arm_cost, shared_setup_usage_micro_usd=setup_cost,
            total_physical_usage_micro_usd=arm_cost + setup_cost,
            arm_physical_provider_calls=sum(row["physical_provider_calls"] for row in rows),
            shared_physical_provider_calls=sum(row["physical_provider_calls"] for row in shared),
            total_physical_provider_calls=len(used_physical), allocation_rule=plan["pilot_analysis"]["experimental_allocation_rule"],
            allocation_is_policy_cost_ranking=False),
        measured_time=dict(shared_setup=distribution([row["timing"]["shared_setup_wall_seconds"] for row in shared]),
            primary_parent_intervals=interval_summary(measured_parents),
            physical_provider=interval_summary([span for span in spans if span["kind"] == "physical_provider_request"]),
            docker_validation=interval_summary([span for span in spans if span["kind"] == "docker_validation"])),
        definitions=DEFINITIONS, limitations=LIMITATIONS, elo=None, population_p_values=None, ranking=None), external)


DEFINITIONS = dict(
    quality="Both milestones require explicit completion and every final public/admitted/private case must pass for acceptance. A private named requirement group passes only when every case in that group passes.",
    pairing="Each contrast uses four same-project/repetition blocks spanning two domains. Current and improved independent arms share the exact first pool and scouts; later work follows each actual lineage. Contrasts reuse the improved-independent arm and are not independent experiments.",
    categories="The frozen plan identifies original baseline requirements separately from first and second milestone additions. W2-inherited belongs to added integration behavior. Final-source coverage does not establish that unexecuted intermediate private cases passed.",
    timing="Arm active=model work+independent final adjudication. Shared setup and scheduling waits remain separate. Provider and Docker spans nest within measured parents; report their unions and cumulative durations without adding them to active time. Setup snapshot timing files are never summed.",
    money="Frozen-price API token usage estimates, not invoices or total engineering/compute cost. Physical setup and arm usage reconcile once. Optional rational allocations describe this joint experiment only; reused independent generation is not a standalone deployment advantage or a cost ranking.",
    calls="Arm calls are physical dispatches and invocations actually performed for that arm. Shared calls occur once. Imported four-builder/two-scout opportunities are provenance, never additional provider requests.",
    repairs="Resolving a known failing case without introducing any failure is observed same-case failure progress. Any previously passing case becoming failing is a regression, even if net passes improve. Effective source changes, new evidence and regressions remain distinct. Current control lacks equivalent repair instrumentation; its zero counters do not prove zero regressions.")
LIMITATIONS = [
    "Exploratory controller-development pilot on two repository-shaped synthetic applications, not confirmatory evidence across independent task families.",
    "Controller contrast changes a package of continuation rules; formation contrast changes initial source formation under improved control. Neither tests live gossip against central orchestration.",
    "Four blocks repeat two domains. Shared sources/scouts and reused contrast arms create dependence; no calibrated Elo, population p-value or general superiority inference.",
    "Bounded stops remain in all planned denominators. Unexecuted or censored cohorts receive no completed-cohort scores; early termination never earns a speed win.",
    "Accepted-only timing is conditional on success and null if none succeed. Provider conditions, host contention and order effects are not controlled away.",
    "Shared cost allocation is joint-study bookkeeping only and is deliberately excluded from quality rankings.",
    "Trusted oracle-assisted evidence and independent private cases measure this frozen contract, not all real-world requirements.",
    "Passing a scripted zero-API rehearsal qualifies infrastructure and control flow, not model quality.",
    "This is read-only aggregation of a retained independent certificate, not another audit or external execution attestation.",
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--audit", type=Path)
    args = parser.parse_args()
    require(not args.output.resolve().is_relative_to(args.run.resolve()), "Write comparison outside frozen cohort")
    result = summarize(args.run, audit_path=args.audit)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x") as stream:
        stream.write(json.dumps(result, indent=2, allow_nan=False) + "\n")
    print(json.dumps(dict(status=result["status"], output=str(args.output))))


if __name__ == "__main__":
    main()
