"""Independent retained-evidence checks for the evidence-frontier benchmark.

No candidate, fixture, runner or controller code is imported or executed.
Historical read-only JSON/receipt/journal/accounting helpers are reused, never
their old roster or policy assumptions. Diagnostic scores require their own
receipt audit and are explicitly outside a primary-only certificate.
"""
from __future__ import annotations

import argparse
import ast
from copy import deepcopy
import json
import math
from pathlib import Path
import random

from analysis.audit_continuation import (
    EvidenceReads, SOURCES as EARLIER_SOURCES, _READ_SET, audit_calls, audit_evaluations, audit_timings,
    execution_suites, load, read_bytes, sha,
)
from gossip_harness.gitstore import GitStore
from gossip_harness.verification_audit import (
    _adapter_hash, _builder_note, _probe_binding, _prompts, _review,
    audit_accounting, audit_receipt, bare, cases_for, decode,
    digest, inside, public_cases, public_view, require, safe_notes, same,
)

PROTOCOL = "evidence-frontier-v1"
STAGE_PROTOCOL = "evidence-frontier-stage-v1"
POLICIES = {"sequential-four": (2, 4, "cheap", "sequential"),
            "independent-four": (2, 4, "cheap", "independent"),
            "strong-anchor": (1, 1, "strong", "independent")}
PROJECTS = {"graph-patch", "calendar-exchange"}
LIMITS = dict(max_reviews=4, max_repairs=2, max_new_probes=8,
              max_escalations=1, stagnation_reviews=2)
CONTEXT = "verification-context.json"
SOURCES = EARLIER_SOURCES | {"benchmark_experiment.py", "benchmark_stage.py", "benchmark_diagnostics.py",
                           "benchmark_graph_patch.py", "benchmark_calendar_exchange.py"}


def check_source_inventory(sources):
    require(type(sources) is dict and set(sources) == SOURCES, "Frozen source inventory changed")


def roster_for(plan):
    require(plan.get("protocol") == PROTOCOL and type(plan.get("milestones")) is int
            and plan["milestones"] == 2 and set(plan["projects"]) == PROJECTS
            and len(plan["projects"]) == 2 and set(plan["policies"]) == set(POLICIES), "Benchmark plan/roster differs")
    rows = []
    for policy, (repetitions, count, model, formation) in POLICIES.items():
        expected = dict(repetitions=repetitions, initial_builders=count, model=model, formation=formation)
        require(same(plan["policies"][policy], expected), "Benchmark policy opportunities differ")
        rows.extend([project, policy, repetition] for project in plan["projects"] for repetition in range(repetitions))
    require(type(plan.get("roster_seed")) is int, "Invalid roster seed")
    random.Random(plan["roster_seed"]).shuffle(rows)
    return rows


def check_roster(actual, plan):
    expected = {tuple(row) for row in roster_for(plan)}
    require(type(actual) is list and len(actual) == 10
            and all(type(row) is list and len(row) == 3 and type(row[2]) is int for row in actual)
            and len({tuple(row) for row in actual}) == 10 and {tuple(row) for row in actual} == expected,
            "Exact ten-trajectory roster required")


def check_fault_banks(value, expected_digest):
    require(digest(value) == expected_digest and value.get("protocol") == "evidence-frontier-fault-bank-v1"
            and type(value.get("projects")) is list and len(value["projects"]) == 2
            and {project["project_id"] for project in value["projects"]} == PROJECTS,
            "Preregistered fault-bank data changed")
    for project in value["projects"]:
        require([stage["stage_index"] for stage in project["stages"]] == [0, 1]
                and all(type(stage["stage_index"]) is int and type(stage["fault_bank"]) is list
                        and type(stage["correct_controls"]) is list for stage in project["stages"]),
                "Fault-bank milestone inventory changed")


def freeze_barrier(freeze, run_ids, spans):
    """No candidate-private or diagnostic work precedes the whole cohort freeze."""
    require(freeze.get("protocol") == PROTOCOL and freeze.get("private_evaluation_started") is False
            and [row["run_id"] for row in freeze["trajectories"]] == list(run_ids)
            and len(set(run_ids)) == len(run_ids) == 10, "Incomplete cohort freeze")
    barrier = freeze.get("frozen_monotonic_ns")
    require(type(barrier) is int and barrier >= 0, "Missing cohort monotonic barrier")
    generation = {"model_trajectory", "stage", "worker_request", "physical_provider_request", "scripted_worker", "scout_batch"}
    for span in spans.values():
        if span["kind"] in generation:
            require(span["finished_monotonic_ns"] <= barrier, "Provider-driven work crossed cohort freeze")
        if span["kind"] == "docker_validation" and span.get("purpose") != "stage_evidence":
            require(span["started_monotonic_ns"] >= barrier, "Private/fault/diagnostic execution preceded cohort freeze")


def stage_execution_index(stage, calls, project, index, root, contract, spans, run_id):
    """Read physical outcomes once, preserving their exact timing/source binding."""
    suites = execution_suites(stage, calls, project, index)
    records = []
    for span in spans.values():
        if span["kind"] != "docker_validation" or span.get("run_id") != run_id or span.get("stage_index") != index:
            continue
        require(span.get("purpose") == "stage_evidence", "Private evidence entered generation stage")
        path = inside(span["receipt_path"], root)
        require(sha(path) == span["receipt_sha256"], "Physical stage receipt changed")
        receipt = load(path)
        require(receipt["suite_sha256"] in suites, "Unknown physically executed stage suite")
        records.append((span["finished_monotonic_ns"], span["label"], receipt, suites[receipt["suite_sha256"]]))
    records.sort(key=lambda row: row[0])
    versions = {digest(row["binding"]): (f"stage-{index}-build-{row['slot']}-{row['round']}-all-evidence", row["candidate"])
                for row in stage["trajectory"] if row["kind"] == "builder"}
    def matrix(files, cases, before, binding):
        label, alias = versions[digest(binding)]
        bases = [row for row in records if row[1] == label]
        require(len(bases) == 1 and bases[0][0] <= before, "Candidate version lacks prior base execution")
        base_end = bases[0][0]
        combined = {}
        for ended, observed_label, receipt, suite in records:
            belongs = observed_label == label or (f"-{alias}-" in observed_label
                and observed_label.endswith(("-new-probes", "-scout-probes")))
            if base_end <= ended <= before and belongs and receipt["source_sha256"] == digest(files):
                combined.update(audit_receipt(receipt, files, suite, contract))
        require(all(case["id"] in combined for case in cases), "Prompt/freeze evidence lacks prior execution")
        return {case["id"]: combined[case["id"]] for case in cases}
    return matrix


def scope_proposal(previous, valid, changes, allowed):
    admissible = (type(changes) is dict and all(path in [*allowed, "notes.json"] for path in changes)
                  and all(type(value) is str for path, value in changes.items() if path in allowed))
    files = {**previous, **{p: v for p, v in changes.items() if p in allowed}} if admissible else dict(previous)
    changed = not same(files, previous)
    return files, bool(admissible and changed) or valid, admissible, changed


def review_from_call(call, requirements, aliases):
    """Mirror the controller's bounded envelope before interpreting its fields."""
    try:
        content = call.get("changes", {}).get("review.json")
        require(type(content) is str and len(content.encode()) <= 65536, "Invalid review envelope")
        value, stack, count = decode(content), [], 0
        stack.append((value, 0))
        while stack:
            item, depth = stack.pop()
            count += 1
            require(count <= 16384 and depth <= 32, "Review tree exceeds controller limits")
            require(not isinstance(item, float) or math.isfinite(item), "Nonfinite review number")
            if type(item) is dict:
                stack.extend((child, depth + 1) for child in item.values())
            elif type(item) is list:
                stack.extend((child, depth + 1) for child in item)
        return _review(call, requirements, aliases)
    except (ValueError, TypeError, AttributeError, UnicodeError, RecursionError):
        return None


def scout_proposals(call):
    try:
        require(set(call.get("changes", {})) == {"probes.json"}, "Malformed scout response")
        content = call["changes"]["probes.json"]
        require(type(content) is str and len(content.encode()) <= 65536, "Oversized scout envelope")
        parsed = decode(content)
        require(type(parsed) is dict and set(parsed) == {"probes"} and type(parsed["probes"]) is list
                and len(parsed["probes"]) <= 4, "Malformed scout response")
        return deepcopy(parsed["probes"])
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
        return []


def initial_opportunities(policy, stage_start, initial_rows, calls, git_files, initial_cases):
    """Every retained checkpoint counts; identical content never erases a call."""
    _, count, model, formation = POLICIES[policy]
    require(len(initial_rows) == count and len({row["slot"] for row in initial_rows}) == count,
            "Missing or duplicate initial builder opportunity")
    previous = stage_start
    for i, row in enumerate(initial_rows):
        require(row["slot"] == f"slot-{i}" and type(row["round"]) is int and row["round"] == 1,
                "Initial opportunities reordered")
        request, call = calls[f"builder-slot-{i}-1"]
        expected_source = previous if formation == "sequential" else stage_start
        files = dict(request["files"])
        context = decode(files.pop(CONTEXT))
        require(call["model"] == model and call["role"] == "builder" and same(files, expected_source)
                and row["previous_files_sha256"] == digest(expected_source)
                and same(context["verified_cases"], public_view(initial_cases)), "Initial formation/source/evidence differs")
        previous = git_files(row["binding"])
    return count


def audit_stage(stage, project, index, policy, initial, prior, calls, contract, root, git_files, spans, run_id, prompts):
    _, count, builder_model, formation = POLICIES[policy]
    require(stage["protocol"] == STAGE_PROTOCOL and same(stage["limits"], LIMITS)
            and stage["formation"] == formation and type(stage["completed"]) is bool
            and type(stage["stage_index"]) is int and stage["stage_index"] == index, "Stage controller/limits changed")
    require(all(type(value) is int and value >= 0 for value in stage["metrics"].values()), "Stage counters are not exact integers")
    public = public_cases(project, index)
    reqs = list(dict.fromkeys(r for s in project["stages"][:index + 1] for r in s["requirements"]))
    aliases, candidates = stage["candidate_order"], {}
    require(len(aliases) == len(set(aliases)) == count
            and list(stage["alias_map"]) == [f"slot-{i}" for i in range(count)]
            and [stage["alias_map"][f"slot-{i}"] for i in range(count)] == aliases, "Candidate opportunities/order changed")
    active, pool, gates = list(public), [], iter(stage["probe_receipts"])
    counters = {"scout": 0, "reviewer": 0}
    def gate(proposals, mode, source, origin):
        record = next(gates, None)
        cap = len(proposals) if mode == "revalidate" else 8 - counters[source]
        require(record is not None and record["mode"] == mode and same(record["origin"], origin)
                and same(record["envelope"], dict(mode=mode, probes=proposals, existing_cases=active, max_new=cap)),
                "Probe gate order/input/cap changed")
        receipt = record["receipt"]
        require(receipt["oracle_assisted"] is True and receipt["mode"] == mode
                and type(receipt["stage_index"]) is int and receipt["stage_index"] == index, "Probe gate identity changed")
        admitted = receipt["admitted_cases"]
        require(type(admitted) is list and len(admitted) <= cap and (mode == "revalidate" or len(proposals) <= 4), "Probe opportunity cap exceeded")
        seen = {digest(case["input"]) for case in active} if mode == "new" else set()
        for case in admitted:
            _probe_binding(case)
            require(case["requirement"] in reqs and digest(case["input"]) not in seen
                    and any(same(bare(case), bare(p)) and (mode != "revalidate" or p["id"] == case["id"])
                            for p in proposals if type(p) is dict and {"input", "expected", "requirement"} <= set(p)),
                    "Probe label corrected, duplicated or injected")
            seen.add(digest(case["input"]))
        if mode == "new":
            counters[source] += len(admitted)
            pool.extend(admitted)
            active.extend(admitted)
        else:
            retired = receipt["retired"]
            require({c["id"] for c in admitted}.isdisjoint({c["id"] for c in retired})
                    and {c["id"] for c in admitted} | {c["id"] for c in retired} == {c["id"] for c in proposals}, "Lost revalidated probe")
        return admitted
    if prior.get("probe_pool"):
        admitted = gate(prior["probe_pool"], "revalidate", "reviewer", dict(project_id=project["id"], stage_index=index, source="retained_probes"))
        visible_inputs = {digest(c["input"]): c for c in public}
        for case in admitted:
            duplicate = visible_inputs.get(digest(case["input"]))
            require(duplicate is None or same(duplicate["expected"], case["expected"]), "Retained/public expectation conflicts")
            if duplicate is None:
                active.append(case)
                pool.append(case)
    initial_cases = deepcopy(active)
    frozen = load(root / "initial-pool.json")
    require(frozen["protocol"] == PROTOCOL and frozen["run_id"] == run_id and frozen["stage_index"] == index
            and same(frozen["pool"], stage["initial_pool"])
            and digest(frozen["pool"]) == frozen["pool_sha256"] == stage["initial_pool_sha256"]
            and stage["initial_freeze"] == dict(freeze_id=str(root / "initial-pool.json"), pool_sha256=frozen["pool_sha256"]), "Initial pool freeze changed")
    initial_pool = frozen["pool"]
    barrier = frozen["frozen_monotonic_ns"]
    require(type(barrier) is int and same(initial_pool["evidence_cases"], initial_cases)
            and initial_pool["evidence_sha256"] == digest(initial_cases)
            and initial_pool["candidate_order"] == aliases and initial_pool["policy"] == policy,
            "Initial pool includes later evidence or wrong policy")
    initial_rows = [row for row in stage["trajectory"] if row["kind"] == "builder" and row["phase"] == "initial"]
    initial_opportunities(policy, initial, initial_rows, calls, git_files, initial_cases)
    matrix = stage_execution_index(stage, calls, project, index, root, contract, spans, run_id)
    scouts = stage["scouts"]
    require(same(load(root / "scouts.json"), scouts) and scouts["calls"] == 2
            and scouts["source_sha256"] == digest(initial)
            and scouts["initial_pool_artifact_sha256"] == sha(root / "initial-pool.json")
            and scouts["initial_pool_sha256"] == frozen["pool_sha256"], "Scout/pool/source binding differs")
    scout_context = dict(project_id=project["id"], stage_index=index,
        milestones=[{k: s[k] for k in ("requirements", "specification", "spec") if k in s} for s in project["stages"][:index + 1]],
        public_cases=cases_for(project, index, "visible"))
    used_calls = set()
    for i in range(2):
        call_id = f"scout-scout-{i}-1"
        request, call = calls[call_id]
        used_calls.add(call_id)
        files = dict(request["files"])
        context = decode(files.pop("scout-context.json"))
        require(call["role"] == "scout" and call["model"] == "cheap" and same(files, initial)
                and same(context, scout_context) and digest(request["files"]) == scouts["shared_input_sha256"]
                and request["allowed_paths"] == ["probes.json"] and request["feedback"] == ""
                and request["instructions"] == prompts["scout_prompt"]
                and spans[call["request_span_id"]]["started_monotonic_ns"] >= barrier,
                "Scout ran before freeze or received candidate/private/peer information")
        proposals = scout_proposals(call)
        require(same(scouts["proposals"][i], dict(scout_id=f"scout-{i}", probes=proposals)), "Scout proposal parse changed")
    baseline = stage["baseline_approval"]
    require(baseline["source_valid"] is True and baseline["files_sha256"] == digest(initial), "Baseline eligibility binding differs")
    context_base = dict(project_id=project["id"], project_title=project["title"], stage_index=index,
        milestones=[dict(index=i, specification=s.get("specification", s.get("spec")), requirements=s["requirements"])
                    for i, s in enumerate(project["stages"][:index + 1])], prior_notes=safe_notes(prior.get("notes", "")),
        prior_remaining=sorted(set(prior.get("remaining", []))), prior_machine_history=prior.get("check_history", []))
    reviewer_extras = {"candidate_source_hashes", "candidate_eligibility", "matrix", "previous_decision", "new_probe_capacity_remaining", "continuation"}
    builders, reviews, accepted, scouts_applied = 0, 0, None, False
    escalation_repairs = {row["id"]: row for row in stage["escalations"] if row["action"] == "strong_repair"}
    require(len(escalation_repairs) == sum(row["action"] == "strong_repair" for row in stage["escalations"])
            and len(stage["escalations"]) <= 1, "Escalation repair IDs/cap changed")
    used_escalations = set()
    for event in stage["trajectory"]:
        kind = event["kind"]
        if kind not in {"builder", "reviewer"}:
            require(kind in {"progress", "escalation"}, "Unknown controller event")
            continue
        require(accepted is None, "Provider work followed explicit stage acceptance")
        call_id = f"builder-{event['slot']}-{event['round']}" if kind == "builder" else f"reviewer-review-{event['round']}"
        request, call = calls[call_id]
        used_calls.add(call_id)
        before = spans[call["request_span_id"]]["started_monotonic_ns"]
        context = decode(request["files"][CONTEXT])
        base = {k: v for k, v in context.items() if k not in {"verified_cases"} | reviewer_extras}
        require(same(base, context_base) and set(context) == set(context_base) | {"verified_cases"} | (reviewer_extras if kind == "reviewer" else set()), "Provider context includes nonpublic fields")
        if kind == "reviewer" and not scouts_applied:
            require(len(candidates) == count, "Reviewer ran before initial pool completed")
            for i in range(2):
                gate(scouts["proposals"][i]["probes"], "new", "scout", dict(project_id=project["id"], stage_index=index, source="scout", scout_id=f"scout-{i}"))
            scouts_applied = True
        require(same(context["verified_cases"], public_view(active)), "Provider evidence frontier differs")
        if kind == "builder":
            builders += 1
            alias = event["candidate"]
            require(stage["alias_map"][event["slot"]] == alias, "Builder slot alias changed")
            previous_item = candidates.get(alias)
            is_initial = event["phase"] == "initial"
            if is_initial and formation == "sequential" and event["slot"] != "slot-0":
                previous_item = candidates[aliases[int(event["slot"].split("-")[1]) - 1]]
            previous = previous_item["files"] if previous_item else initial
            previous_valid = previous_item["source_valid"] if previous_item else True
            source_files = {k: v for k, v in request["files"].items() if k != CONTEXT}
            meta = call["controller_metadata"]
            if meta.get("escalation_id"):
                escalation_id = meta["escalation_id"]
                require(not is_initial and escalation_id in escalation_repairs and escalation_id not in used_escalations
                        and escalation_repairs[escalation_id]["candidate"] == alias, "Unbound or repeated strong escalation repair")
                used_escalations.add(escalation_id)
            expected_model = "strong" if meta.get("escalation_id") else builder_model
            require(call["role"] == "builder" and call["model"] == event["model"] == expected_model and same(source_files, previous)
                    and request["allowed_paths"] == [*project["allowed_paths"], "notes.json"]
                    and request["instructions"] == prompts["builder_prompt"] and meta["phase"] == event["phase"], "Builder model/source/scope changed")
            if is_initial:
                require(not scouts_applied and spans[call["request_span_id"]]["finished_monotonic_ns"] <= barrier, "Initial generation crossed scout barrier")
            feedback = decode(request["feedback"])
            if is_initial:
                expected_feedback = dict(checkpoint_notes=previous_item["notes"] if previous_item else context_base["prior_notes"])
                if formation == "sequential" and previous_item:
                    expected_feedback.update(preceding_source_sha256=digest(previous), evidence=[dict(case=case, outcome=matrix(previous, active, before, previous_item["binding"])[case["id"]]) for case in public_view(active)])
                require(same(feedback, expected_feedback), "Initial builder peer/evidence dependency differs")
            else:
                require(set(feedback) <= {"review_notes", "remaining", "evidence", "continuation_escalation"}
                        and same([item["case"] for item in feedback["evidence"]], public_view(active)), "Repair feedback includes unknown evidence")
                checked = matrix(previous, active, before, previous_item["binding"])
                require(all(same(item["outcome"], checked[item["case"]["id"]]) for item in feedback["evidence"]), "Repair outcome differs from physical evidence")
            files, valid, admissible, changed = scope_proposal(previous, previous_valid, call.get("changes"), project["allowed_paths"])
            require(same(git_files(event["binding"]), files) and event["files_sha256"] == digest(files)
                    and event["previous_files_sha256"] == digest(previous) and event["source_valid"] is valid
                    and event["prior_source_valid"] is previous_valid and event["attempt_admissible"] is admissible
                    and event["effective_source_change"] is changed, "Source/eligibility chain differs")
            note = _builder_note(call.get("changes"), reqs)
            candidates[alias] = dict(files=files, binding=event["binding"], source_valid=valid,
                notes=safe_notes(note["notes"]) if note and admissible else previous_item["notes"] if previous_item else context_base["prior_notes"],
                remaining=note["remaining"] if note and admissible else previous_item["remaining"] if previous_item else [],
                reviewer_remaining=candidates.get(alias, {}).get("reviewer_remaining", []),
                latest_attempt_status="effective_source_proposal" if changed else "no_effective_source_change" if admissible else "rejected_source_proposal")
            if is_initial:
                frozen_item = initial_pool["candidates"][alias]
                require(same(frozen_item["files"], files) and same(frozen_item["binding"], event["binding"])
                        and frozen_item["source_valid"] is valid and same(frozen_item["matrix"], matrix(files, initial_cases, barrier, event["binding"])), "Frozen initial candidate/source/evidence differs")
        else:
            reviews += 1
            require(call["model"] == "strong" and call["role"] == "reviewer" and request["allowed_paths"] == ["review.json"]
                    and request["feedback"] == "" and request["instructions"] in [prompts["reviewer_prompt"], prompts["reviewer_prompt"] + prompts["escalation_suffix"]], "Reviewer opportunity changed")
            expected_files = {f"candidates/{alias}/{p}": text for alias in aliases for p, text in candidates[alias]["files"].items()}
            require(same({k: v for k, v in request["files"].items() if k != CONTEXT}, expected_files)
                    and same(context["candidate_source_hashes"], {a: digest(candidates[a]["files"]) for a in aliases})
                    and same(context["candidate_eligibility"], {a: {k: candidates[a][k] for k in ("source_valid", "latest_attempt_status")} for a in aliases})
                    and same(context["matrix"], {a: matrix(candidates[a]["files"], active, before, candidates[a]["binding"]) for a in aliases})
                    and same(context["continuation"]["limits"], LIMITS), "Reviewer source or physical matrix differs")
            review = review_from_call(call, reqs, aliases)
            require(same(review, event["review"]) and type(event["accepted"]) is bool, "Saved review differs from raw decision")
            if review:
                gate(review["probes"], "new", "reviewer", dict(project_id=project["id"], stage_index=index, source="reviewer", review_round=event["round"]))
                candidates[review["candidate"]]["reviewer_remaining"] = review["remaining"]
            if event["accepted"]:
                require(accepted is None and review and review["action"] == "accept" and not review["remaining"]
                        and candidates[review["candidate"]]["source_valid"]
                        and same(event["binding"], candidates[review["candidate"]]["binding"])
                        and same(event["binding"], stage["selected_binding"]), "Unsupported or repeated acceptance")
                accepted = review["candidate"]
    require(next(gates, None) is None and used_calls == set(calls) and builders - count == stage["metrics"]["repairs"] <= 2
            and used_escalations == set(escalation_repairs)
            and stage["metrics"]["initial_builder_calls"] == count and builders == stage["metrics"]["builder_calls"]
            and reviews == stage["metrics"]["reviewer_calls"] <= 4 and len(stage["escalations"]) == stage["metrics"]["escalations"] <= 1,
            "Unlinked calls/gates or expanded opportunities")
    require(same(stage["evidence_cases"], active) and stage["evidence_sha256"] == digest(active)
            and same(stage["probe_pool"], pool) and scouts["admitted"] == counters["scout"]
            and stage["metrics"]["admitted_scout_probes"] == counters["scout"]
            and stage["metrics"]["admitted_new_probes"] == counters["reviewer"], "Final evidence frontier changed")
    final_matrix = {}
    for alias, candidate in candidates.items():
        saved = stage["candidates"][alias]
        require(all(same(saved[k], candidate[k]) for k in candidate if k != "files"), "Final retained candidate changed")
        final_matrix[alias] = matrix(candidate["files"], active, float("inf"), candidate["binding"])
    require(same(stage["matrix"], final_matrix), "Final matrix differs from physical evidence")
    selected = stage["selected"]
    if accepted is None:
        expected = min(aliases, key=lambda a: (not candidates[a]["source_valid"], sum(not r["passed"] for r in final_matrix[a].values()), aliases.index(a)))
        require(selected == expected, "Incomplete fallback selection differs")
    else:
        require(selected == accepted and all(r["passed"] for r in final_matrix[selected].values()), "Accepted source fails evidence")
    chosen = candidates[selected]
    remaining = {case["requirement"] for case in active if not final_matrix[selected][case["id"]]["passed"]} | set(chosen["remaining"]) | set(chosen["reviewer_remaining"])
    require(stage["completed"] is (accepted is not None) and same(stage["files"], chosen["files"])
            and same(stage["selected_binding"], chosen["binding"]) and stage["source_valid"] is chosen["source_valid"]
            and stage["remaining"] == ([] if accepted is not None else sorted(remaining)), "Stage completion/source/remaining differs")
    audit_evaluations(stage, calls, project, index, contract, root, git_files, spans, run_id)
    return dict(initial_builders=count, builders=builders, reviewers=reviews, scouts=2,
                completed=accepted is not None, initial_pool_sha256=frozen["pool_sha256"])


def bound_file(binding, path):
    require(Path(binding["path"]).resolve() == path.resolve() and binding["sha256"] == sha(path), "Frozen artifact path/bytes changed")


def audit_exports(frozen, stage, root, project, git_files):
    """Full global freeze includes every initial and repaired proposal, not just winner."""
    index = stage["stage_index"]
    require(frozen["stage_index"] == index, "Frozen stage artifact order changed")
    require(frozen["initial_pool"]["pool_sha256"] == stage["initial_pool_sha256"], "Global/initial pool digest differs")
    for key, filename in (("result", "result.json"), ("initial_pool", "initial-pool.json"), ("candidate_exports", "candidate-exports.json"), ("scouts", "scouts.json")):
        bound_file(frozen[key], root / filename)
    catalog = load(root / "candidate-exports.json")
    rows = [r for r in stage["trajectory"] if r["kind"] == "builder"]
    require(catalog["protocol"] == PROTOCOL and catalog["stage_index"] == index
            and catalog["project_id"] == project["id"] and catalog["run_id"] == root.parent.name
            and same(catalog["exports"], frozen["exports"]) and len(catalog["exports"]) == len(rows), "Frozen candidate export manifest changed")
    available = {}
    for export, row in zip(catalog["exports"], rows):
        label = f"stage-{index}-build-{row['slot']}-{row['round']}"
        path = root / (label + ".json")
        bound_file(export, path)
        saved = load(path)
        require(export["label"] == saved["label"] == label and same(export["binding"], row["binding"])
                and same(saved["binding"], export["binding"]) and same(saved["files"], git_files(saved["binding"]))
                and digest(saved["files"]) == export["files_sha256"] == row["files_sha256"]
                and set(saved["files"]) == set(project["initial_files"])
                and all(saved["files"][p] == text for p, text in project["initial_files"].items() if p not in project["allowed_paths"]),
                "Frozen proposal source/scope changed")
        available[digest(saved["binding"])] = saved["files"]
    require(len({r["label"] for r in catalog["exports"]}) == len(rows), "Duplicate frozen proposal label")
    for group in (stage["initial_pool"]["candidates"], stage["candidates"]):
        for candidate in group.values():
            require(digest(candidate["binding"]) in available and ("files" not in candidate or same(candidate["files"], available[digest(candidate["binding"])])), "Candidate disappeared from frozen export history")
    require(same(available[digest(stage["selected_binding"])], stage["files"]), "Selected source absent from frozen history")
    require(len(frozen["requests"]) == len(stage["invocations"]), "Frozen request roster differs")
    for entry, call in zip(frozen["requests"], stage["invocations"]):
        require(entry["call_id"] == call["call_id"], "Frozen calls reordered")
        for kind in ("request", "result"):
            bound_file(entry[kind], root / "requests" / f"{call['call_id']}.{kind}.json")
        for kind, path in call["journal_paths"].items():
            bound_file(entry["journal"][kind], inside(path, root))


def _audit_run(run, *, accounting_ledger=None):
    run = Path(run).resolve()
    report, prereg, plan = load(run / "results.json"), load(run / "preregistered.json"), load(run / "study-plan.json")
    require(report.get("experiment") == PROTOCOL and report.get("status") == "finished" and report.get("phase") == "finished"
            and report.get("mode") in {"live", "rehearsal"} and report.get("active_case") is None
            and not report.get("censored") and not report.get("unexecuted"), "Only a fully finalized ten-trajectory cohort can be certified")
    contract = report["contract"]
    check_source_inventory(contract["sources"])
    require(report["contract_sha"] == digest(contract) and same(prereg["contract"], contract)
            and same(prereg["budget_before"], report["budget_before"])
            and contract["protocol"] == PROTOCOL and same(contract["limits"], LIMITS)
            and contract["milestones"] == 2 and contract["output_tokens"] == 12288
            and contract["hidden_feedback"] is False and contract["controller_crash_injection"] is False,
            "Frozen execution contract changed")
    for name, expected in contract["sources"].items():
        require(sha(run / "source-snapshot" / "gossip_harness" / name) == expected, "Frozen source changed: " + name)
    require(set(contract["extra_sources"]) == {"analysis/qualify_benchmark.py"}, "Qualification source inventory changed")
    for name, expected in contract["extra_sources"].items():
        require(sha(run / "source-snapshot" / name) == expected, "Frozen qualification source changed")
    qualification = report["qualification"]
    require(same(qualification, prereg["qualification"]) and qualification["contract_sha"] == digest(contract), "Qualification binding differs")
    qualification_path = Path(qualification["path"]).resolve()
    require(sha(qualification_path) == qualification["sha256"], "Qualification report changed")
    qualification_report = load(qualification_path)
    qualification_manifest = load(qualification_path.parent / "manifest.json")
    require(qualification_report["protocol"] == qualification_manifest["protocol"] == "evidence-frontier-fixture-qualification-v1"
            and qualification_report["status"] == "qualified" and qualification_report["api_calls"] == 0
            and qualification_report["contract_sha256"] == qualification_manifest["contract_sha256"] == digest(contract)
            and qualification_report["manifest_sha256"] == sha(qualification_path.parent / "manifest.json")
            and same(qualification_manifest["contract"], contract), "Qualified fixture/contract certificate differs")
    require(sha(run / "study-plan.json") == contract["plan_sha256"] and same(plan["policies"], contract["policies"])
            and same(plan["budget"], contract["budget"]) and all(same(plan["controller"][k], v) for k, v in LIMITS.items())
            and plan["controller"]["scouts"] == contract["scouts"]["count"] == 2
            and plan["controller"]["proposals_per_scout"] == 4, "Frozen plan/opportunities changed")
    roster = contract["roster"]
    check_roster(roster, plan)
    require(same(prereg["roster"], roster) and same([[c["project_id"], c["policy"], c["repetition"]] for c in report["cases"]], roster), "Final roster/order changed")
    fixtures = load(run / "fixtures.json")["projects"]
    projects = {p["id"]: p for p in fixtures}
    require(len(fixtures) == len(projects) == 2 and set(projects) == PROJECTS and set(contract["fixtures"]) == PROJECTS, "Frozen project roster changed")
    for identifier, project in projects.items():
        binding = contract["fixtures"][identifier]
        require(binding["fixture_sha256"] == digest(project) and binding["baseline_sha256"] == digest(project["initial_files"])
                and len(project["stages"]) == 2, "Frozen project/baseline differs")
    fault_banks = load(run / "fault-banks.json")
    check_fault_banks(fault_banks, contract["fault_banks_sha256"])
    namespace = PROTOCOL + "/" + digest([str(run), digest(contract)])[:24]
    require(report["namespace"] == prereg["namespace"] == namespace
            and report["incremental_commitment_cap"] == prereg["incremental_commitment_cap"] == contract["budget"]["incremental_cap_micro_usd"], "Study namespace/cap differs")
    require(same(prereg["rehearsal"], report["rehearsal"]), "Rehearsal binding differs")
    if report["mode"] == "live":
        proof = report["rehearsal"]
        proof_path = Path(proof["results_path"]).resolve()
        proof_report = load(proof_path)
        require(sha(proof_path) == proof["results_sha256"] and proof["contract_sha"] == digest(contract)
                and same(proof_report["contract"], contract) and proof_report["mode"] == "rehearsal"
                and proof_report["status"] == "finished" and len(proof_report["cases"]) == 10
                and all(c["accepted"] is True and c["usage_micro_usd"] == 0 and c["physical_provider_calls"] == 0 for c in proof_report["cases"])
                and proof_report["incremental_micro_usd"] == 0 and not proof_report["censored"] and not proof_report["unexecuted"], "Exact zero-API proof binding differs")
    else:
        require(report["rehearsal"] is None, "Offline proof acquired upstream proof")
    freeze = load(run / "frozen-trajectories.json")
    freeze_sha = sha(run / "frozen-trajectories.json")
    run_ids = [f"{project}-{policy}-{rep}" for project, policy, rep in roster]
    require(report["freeze_manifest_sha256"] == freeze_sha and freeze["contract_sha"] == digest(contract), "Global freeze binding changed")
    bound_file(freeze["fixtures"], run / "fixtures.json")
    bound_file(freeze["fault_banks"], run / "fault-banks.json")
    spans = audit_timings(load(run / "timings.json"), freeze, run_ids)
    freeze_barrier(freeze, run_ids, spans)
    require({p.parent.name for p in run.glob("*/config.json")} == set(run_ids), "Unlinked trajectory directory")
    stage_text = read_bytes(run / "source-snapshot" / "gossip_harness" / "benchmark_stage.py").decode()
    prompts = _prompts(stage_text)
    prompts["escalation_suffix"], = [n.value for n in ast.walk(ast.parse(stage_text)) if isinstance(n, ast.Constant)
                                    and isinstance(n.value, str) and n.value.startswith(" This is a bounded continuation review")]
    runner_ast = ast.parse(read_bytes(run / "source-snapshot" / "gossip_harness" / "benchmark_experiment.py").decode())
    scout_function, = [node for node in runner_ast.body if isinstance(node, ast.FunctionDef) and node.name == "scout_probes"]
    prompts["scout_prompt"], = [ast.literal_eval(node.value) for node in scout_function.body if isinstance(node, ast.Assign)
                               and any(isinstance(target, ast.Name) and target.id == "instructions" for target in node.targets)]
    validation_contract = {**contract, "adapter_sha256": _adapter_hash(read_bytes(run / "source-snapshot" / "gossip_harness" / "blackbox_validator.py").decode())}
    cache, all_bindings, billing_heads, commitments, actual_stages, audits = {}, [], [], [], set(), []
    def git_files(binding):
        path = inside(binding["store_path"], run)
        key = (str(path), binding["tip_sha"])
        if key not in cache:
            cache[key] = GitStore(path).read_files(binding["tip_sha"])
        require(digest(cache[key]) == binding["files_sha256"], "Retained Git digest differs")
        return cache[key]
    require([row["run_id"] for row in report["trajectories"]] == run_ids, "Trajectory timing roster differs")
    for case, frozen, timing in zip(report["cases"], freeze["trajectories"], report["trajectories"]):
        root = run / case["run_id"]
        config, state = load(root / "config.json"), load(root / "trajectory.json")
        project = projects[case["project_id"]]
        require(same(load(root / "result.json"), case) and config["root"] == case["root"] == str(root)
                and all(config[k] == state[k] == case[k] == frozen[k] for k in ("run_id", "project_id", "policy", "repetition"))
                and config["namespace"] == namespace + "/" + case["run_id"]
                and config["contract_sha"] == state["contract_sha"] == digest(contract)
                and config["image"] == contract["image"] and config["live"] is (report["mode"] == "live"), "Case/config identity differs")
        require(sha(root / "trajectory.json") == case["trajectory_sha256"] == frozen["trajectory_sha256"] == timing["trajectory_sha256"]
                and digest(state["files"]) == case["files_sha256"] == frozen["files_sha256"]
                and same(state["final_binding"], frozen["final_binding"]) and same(git_files(state["final_binding"]), state["files"])
                and GitStore(inside(state["final_binding"]["store_path"], run)).head() == state["final_binding"]["accepted_head"], "Terminal source/Git/freeze differs")
        timed = spans[timing["span_id"]]
        require(timed["kind"] == "model_trajectory" and timed["run_id"] == case["run_id"] and timed["status"] == "finished"
                and same(timing["elapsed_seconds"], timed["elapsed_seconds"]) and timing["terminal"] == state["terminal"], "Trajectory elapsed/terminal summary differs")
        stages = state["stages"]
        require(1 <= len(stages) <= 2 and all(s["completed"] is True for s in stages[:-1])
                and len(frozen["stage_artifacts"]) == len(stages)
                and {p.name for p in root.glob("stage-*") if p.is_dir()} == {f"stage-{i}" for i in range(len(stages))}, "Skipped or orphan stage")
        initial, prior, flat_calls, stage_audits = project["initial_files"], {}, [], []
        require(same(GitStore(root / "initial.git").read_files(), initial), "Initial Git source differs")
        for index, stage in enumerate(stages):
            stage_root = root / f"stage-{index}"
            billing = config["namespace"] + f"/stage-{index}"
            actual_stages.add((case["run_id"], index))
            require(same(load(stage_root / "result.json"), stage), "Stage result differs")
            audit_exports(frozen["stage_artifacts"][index], stage, stage_root, project, git_files)
            calls, bindings = audit_calls(stage_root, stage["invocations"], billing, contract, report["mode"], spans, case["run_id"], index)
            all_bindings.extend(bindings)
            stage_audits.append(audit_stage(stage, project, index, case["policy"], initial, prior, calls, validation_contract,
                                           stage_root, git_files, spans, case["run_id"], prompts))
            record = GitStore(stage_root / "record.git")
            require(same(decode(record.read_files()["record.json"]), {k: stage[k] for k in ("completed", "status", "selected_binding")}), "Accounting record differs")
            billing_heads.append((billing, record.head()))
            if stage["completed"]:
                require(same(GitStore(stage_root / "checkpoint.git").read_files(), stage["files"]), "Stage checkpoint differs")
            for call, binding in zip(stage["invocations"], bindings):
                wrapper = spans[call["request_span_id"]]
                commitments.append((wrapper["started_monotonic_ns"], wrapper["finished_monotonic_ns"], binding["amount"], binding["usage"]))
            flat_calls.extend(stage["invocations"])
            initial, prior = stage["files"], stage
        full = len(stages) == 2 and all(s["completed"] for s in stages)
        require(state["terminal"] == frozen["terminal"] == ("visible_complete" if full else "bounded_incomplete")
                and same(state["files"], stages[-1]["files"]) and same(flat_calls, case["invocations"])
                and case["milestones_completed"] == frozen["milestones_completed"] == sum(s["completed"] for s in stages)
                and frozen["evidence_sha256"] == digest(stages[-1]["evidence_cases"]), "Terminal outcome/evidence differs")
        suites = dict(visible=cases_for(project, 1, "visible") + (stages[-1]["probe_pool"] if len(stages) == 2 else []), private=cases_for(project, 1, "hidden"))
        for kind, cases in suites.items():
            path = root / f"final-{kind}-receipt.json"
            receipt = load(path)
            require(same(receipt, case["final_hidden" if kind == "private" else "final_visible"]), "Final receipt summary differs")
            audit_receipt(receipt, state["files"], cases, validation_contract, "Final " + kind)
            matches = [s for s in spans.values() if s["kind"] == "docker_validation" and s.get("run_id") == case["run_id"] and s.get("purpose") == "final_" + kind]
            require(len(matches) == 1 and matches[0]["freeze_manifest_sha256"] == freeze_sha
                    and Path(matches[0]["receipt_path"]).resolve() == path and matches[0]["receipt_sha256"] == sha(path)
                    and matches[0]["case_count"] == len(cases), "Final execution/freeze receipt differs")
        accepted = bool(full and case["final_visible"]["passed"] and case["final_hidden"]["passed"])
        coverage = {r: all(outcome["passed"] for c, outcome in zip(suites["private"], case["final_hidden"]["outcomes"]) if c["requirement"] == r)
                    for r in sorted({c["requirement"] for c in suites["private"]})}
        require(case["accepted"] is accepted and same(case["requirement_coverage"], coverage)
                and case["usage_micro_usd"] == sum(c["usage_units"] for c in flat_calls)
                and case["physical_provider_calls"] == sum(c["physical_dispatch"] for c in flat_calls)
                and case["freeze_manifest_sha256"] == freeze_sha, "Primary acceptance/coverage/usage differs")
        if accepted:
            release = GitStore(root / "release.git")
            require(release.head() == case["release_head"] and same(release.read_files(), state["files"]), "Released source differs from tested source")
        else:
            require(case["release_head"] is None and not (root / "release.git").exists(), "Rejected source was released")
        audits.append(dict(run_id=case["run_id"], accepted=accepted, milestones_completed=case["milestones_completed"], stages=stage_audits))
    known = {"study", "model_trajectory", "stage", "scout_batch", "worker_request", "physical_provider_request", "scripted_worker", "docker_validation", "final_evaluation"}
    for timed in spans.values():
        require(timed["kind"] in known and (timed["kind"] == "study" or timed.get("run_id") in run_ids), "Unknown/orphan execution span")
        if "stage_index" in timed:
            require(type(timed["stage_index"]) is int and (timed["run_id"], timed["stage_index"]) in actual_stages, "Execution targets nonexistent stage")
        elif timed["kind"] in {"stage", "scout_batch", "worker_request", "physical_provider_request", "scripted_worker"}:
            raise ValueError("Stage execution lacks stage identity")
        if timed["kind"] == "docker_validation":
            require(timed.get("purpose") in {"stage_evidence", "final_visible", "final_private"}
                    and ((timed["purpose"] == "stage_evidence") == ("stage_index" in timed)), "Unknown primary Docker purpose; diagnostics need separate audit")
    accounting = audit_accounting(accounting_ledger or run / "accounting-snapshot.sqlite", all_bindings, billing_heads, [], report, study_namespace=namespace)
    cap = contract["budget"]["incremental_cap_micro_usd"] if report["mode"] == "live" else 0
    peak = max((sum(usage if end <= at else amount if start <= at else 0 for start, end, amount, usage in commitments)
                for at in [start for start, _, _, _ in commitments]), default=0)
    require(accounting["study_usage_micro_usd"] <= cap and peak <= cap, "Reservation-inclusive study cap exceeded")
    if report["mode"] == "live":
        require(report["budget_before"]["spent_or_reserved"] == contract["budget"]["expected_starting_usage_micro_usd"]
                and report["budget_before"]["limit"] == accounting["current_limit"] == contract["budget"]["shared_cumulative_cap_micro_usd"]
                and report["budget_before"]["spent_or_reserved"] + peak <= accounting["current_limit"], "Shared budget preservation differs")
    return dict(protocol="independent-evidence-frontier-audit-v1", passed=True, root=str(run),
        results_sha256=sha(run / "results.json"), timings_sha256=sha(run / "timings.json"),
        freeze_manifest_sha256=freeze_sha, contract_sha256=digest(contract), auditor_sha256=sha(__file__),
        cases=audits, accounting=accounting, conservative_peak_commitment_micro_usd=peak,
        scope=dict(primary_cohort=True, candidate_exports=True, initial_generation=True, diagnostic_matrix=False),
        qualification_binding=qualification,
        limitations=["Retained trusted-host consistency, not external execution/clock attestation.",
            "Fault-bank semantics and golden/control qualification rely on separate fixture qualification; no oracle or candidate is executed by this audit.",
            "Checks opportunity bounds, source eligibility and explicit acceptance; does not prove qualitative reviewer judgments.",
            "Post-freeze candidate/fault/cross-policy diagnostic scores are outside this primary certificate and need separately bound receipt review.",
            "Censored, unknown-outcome and resumed cohorts are not certified as complete ten-trajectory studies."])


def audit_run(run, *, accounting_ledger=None):
    reads = EvidenceReads()
    token = _READ_SET.set(reads)
    try:
        result = _audit_run(run, accounting_ledger=accounting_ledger)
        reads.verify_unchanged()
        result["consumed_evidence_files"] = len(reads.hashes)
        result["consumed_evidence_sha256"] = digest({str(path): value for path, value in sorted(reads.hashes.items())})
        return result
    finally:
        _READ_SET.reset(token)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--accounting-ledger", type=Path)
    args = parser.parse_args()
    require(not args.output.exists(), "Refusing to replace audit evidence")
    result = audit_run(args.run, accounting_ledger=args.accounting_ledger)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, sort_keys=True, indent=2) + "\n")
    print(json.dumps(dict(passed=True, cases=len(result["cases"]), output=str(args.output))))


if __name__ == "__main__":
    main()
