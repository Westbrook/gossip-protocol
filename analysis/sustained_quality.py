"""Recompute descriptive quality/persistence analysis of finalized study cases.

This is a read-only analysis, not the retained-evidence audit. Candidate code is
never imported or executed. A running study contributes only finalized cases;
their uneven policy roster must not be treated as a final comparison.
"""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path


def load(path):
    return json.loads(Path(path).read_text())


def cases(project, index, kind):
    return [case for stage in project["stages"][:index + 1] for case in stage[kind + "_cases"]]


def analyze(run):
    run = Path(run).resolve()
    report_bytes = (run / "results.json").read_bytes()
    report = json.loads(report_bytes)
    fixtures = {project["id"]: project for project in load(run / "fixtures.json")}
    policies = {}
    details = []
    all_review_only_repairs = []
    for record in report["cases"]:
        project = fixtures[record["project_id"]]
        root = Path(record["root"])
        trajectory_bytes = (root / "trajectory.json").read_bytes()
        if hashlib.sha256(trajectory_bytes).hexdigest() != record["trajectory_sha256"]:
            raise ValueError("Frozen trajectory changed: " + record["run_id"])
        state = json.loads(trajectory_bytes)
        group = policies.setdefault(record["policy"], Counter())
        group.update(project_runs=1, accepted=int(record["accepted"]),
                     milestones_completed=record["milestones_completed"], milestones_possible=3,
                     final_requirements_passed=sum(record["requirement_coverage"].values()),
                     final_requirements_checked=len(record["requirement_coverage"]),
                     usage_micro_usd=record["usage_micro_usd"])
        handoff = record["handoff"]
        group["verified_distinct_process_handoffs"] += int(bool(handoff.get("verified")
                  and handoff["previous_pid"] != handoff["resumed_pid"]))
        stages = []
        previous_private = None
        transitions = []
        for index, stage in enumerate(state["stages"]):
            builder_stats = Counter()
            followups = []
            candidate_by_id = {}
            reviewers = []
            initial = []
            stage_review_only_repairs = []
            for event in stage["trajectory"]:
                if event["kind"] == "builder":
                    builder_stats["builder_calls"] += 1
                    builder_stats["passing_visible_proposals" if event["visible_passed"] else "failing_visible_proposals"] += 1
                    builder_stats["raw_proposal_regressions"] += len(event["regressions"])
                    control = event.get("control")
                    if not control:
                        builder_stats["invalid_control_artifacts"] += 1
                    elif control["action"] == "complete" and not event["visible_passed"]:
                        builder_stats["complete_claims_with_visible_defects"] += 1
                    elif control["action"] == "continue" and event["visible_passed"]:
                        builder_stats["continue_claims_despite_passing_visible"] += 1
                    previous = candidate_by_id.get(event["candidate"])
                    if previous is None:
                        initial.append(event)
                    else:
                        old_control = previous.get("control")
                        if not previous["visible_passed"]:
                            category = "repair_after_visible_defect"
                        elif reviewers:
                            review_action = (reviewers[-1].get("review") or {}).get("action")
                            category = ("reviewer_requested_repair_after_visible_pass" if review_action == "repair"
                                        else "reviewer_control_or_gate_repair_after_visible_pass")
                        elif not old_control:
                            category = "repair_of_invalid_control_after_visible_pass"
                        elif old_control["action"] == "continue":
                            category = "continuation_after_visible_pass"
                        else:
                            category = "completion_declaration_repair_after_visible_pass"
                        builder_stats["followup_calls"] += 1
                        builder_stats[category] += 1
                        unchanged = event["files_sha256"] == previous["files_sha256"]
                        builder_stats["followups_with_unchanged_source"] += int(unchanged)
                        recovered = not previous["visible_passed"] and event["visible_passed"]
                        builder_stats["followups_recovering_visible_suite"] += int(recovered)
                        followups.append(dict(candidate=event["candidate"], round=event["round"],
                             category=category, unchanged_source=unchanged, recovered_visible_suite=recovered,
                             previous_failed_case_ids=previous["failures"], current_failed_case_ids=event["failures"]))
                    candidate_by_id[event["candidate"]] = event
                else:
                    reviewers.append(event)
                    builder_stats["reviewer_calls"] += 1
                    chosen = candidate_by_id[event["selected"]]
                    best = min(candidate_by_id, key=lambda key: (
                        not candidate_by_id[key]["source_valid"], len(candidate_by_id[key]["failures"]), key))
                    builder_stats["review_choices_different_from_visible_best"] += int(event["selected"] != best)
                    if event["accepted"]:
                        builder_stats["reviewer_acceptances"] += 1
                        control = chosen.get("control")
                        builder_stats["review_accepts_without_builder_complete_claim"] += int(
                            not control or control["action"] != "complete")
                    review = event.get("review")
                    if review and review["action"] == "repair" and chosen["visible_passed"]:
                        note = dict(run_id=record["run_id"], stage_index=index, review_round=event["round"],
                            candidate=event["selected"], notes=review["notes"], remaining=review["remaining"],
                            visible_passed_before_repair=True, before_binding=chosen["binding"],
                            before_proposal_json=str(Path(chosen["binding"]["store_path"]).with_suffix(".json")),
                            counterfactual_hidden_quality="Not measured in the preregistered run")
                        stage_review_only_repairs.append(note)
                        all_review_only_repairs.append(note)
            if reviewers:
                builder_stats["portfolio_milestones"] += 1
                builder_stats["portfolio_initial_pool_with_a_visible_pass"] += int(any(row["visible_passed"] for row in initial))
                builder_stats["portfolio_initial_pool_with_no_visible_pass"] += int(not any(row["visible_passed"] for row in initial))
                builder_stats["portfolio_initial_passing_candidates"] += sum(row["visible_passed"] for row in initial)
                builder_stats["portfolio_initial_candidates"] += len(initial)
            hidden_cases = cases(project, index, "hidden")
            outcomes = record["historical"][index]["hidden"]["outcomes"]
            current_private = {case["id"]: outcome["passed"] for case, outcome in zip(hidden_cases, outcomes)}
            hidden_failed = [name for name, passed in current_private.items() if not passed]
            builder_stats["selected_milestones_with_hidden_failure"] += int(bool(hidden_failed))
            if previous_private is not None:
                common = set(previous_private) & set(current_private)
                regressions = sorted(name for name in common if previous_private[name] and not current_private[name])
                recoveries = sorted(name for name in common if not previous_private[name] and current_private[name])
                transitions.append(dict(from_stage=index - 1, to_stage=index, recurring_cases=len(common),
                                        regressed_case_ids=regressions, recovered_case_ids=recoveries))
                group["selected_hidden_regressions"] += len(regressions)
                group["selected_hidden_recoveries"] += len(recoveries)
                group["selected_hidden_transition_opportunities"] += len(common)
            previous_private = current_private
            group.update(builder_stats)
            stages.append(dict(stage_index=index, selected=stage["selected"], completed=stage["completed"],
                               selected_hidden_failed_case_ids=hidden_failed, proposal_diagnostics=dict(builder_stats),
                               followups=followups, review_only_repairs=stage_review_only_repairs))
        final_hidden = [case["id"] for case, outcome in zip(cases(project, 2, "hidden"), record["final_hidden"]["outcomes"])
                        if not outcome["passed"]]
        details.append(dict(run_id=record["run_id"], accepted=record["accepted"], stages=stages,
                            final_hidden_failed_case_ids=final_hidden, handoff=handoff,
                            selected_private_transitions=transitions))
    for group in policies.values():
        group["raw_proposal_regressions_per_builder_call"] = group["raw_proposal_regressions"] / group["builder_calls"]
    return dict(study_status=report["status"], finalized_project_runs=len(report["cases"]),
                report_snapshot_sha256=hashlib.sha256(report_bytes).hexdigest(),
                unexecuted=report["unexecuted"], policies={key: dict(value) for key, value in policies.items()},
                cases=details, review_only_repairs=all_review_only_repairs,
                caveats=["Descriptive read-only analysis; run sustained_audit for evidence binding verification.",
                         "Primary outcomes are final requirement coverage and whole-project completion.",
                         "Raw losing-proposal regressions are exploratory branch diagnostics, not a policy-quality numerator.",
                         "A builder continuation or malformed completion artifact is not necessarily a software defect.",
                         "Historical private checks are retrospective and never supplied as agent repair feedback.",
                         "Passing-visible reviewer repairs need counterfactual private evaluation before claiming measured hidden lift.",
                         "While the study is running, finalized policies have unequal sample counts."])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = json.dumps(analyze(args.run), indent=2, sort_keys=True) + "\n"
    if args.output:
        with args.output.open("x") as stream:
            stream.write(result)
    else:
        print(result, end="")


if __name__ == "__main__":
    main()
