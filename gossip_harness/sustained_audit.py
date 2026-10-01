"""Read-only audit of retained sustained-study evidence, without running code.

Source snapshots and candidate files are data. Git object reads and SQLite
read-only connections verify bindings; no candidate or snapshot module is
imported, checked out, or executed. Hashes prove internal consistency, not an
independent attestation of the machine that originally ran the validators.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
import hashlib
import json
from pathlib import Path
import sqlite3

from .gitstore import GitStore


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=True,
                                    allow_nan=False, separators=(",", ":")).encode()).hexdigest()


def _require(condition, message):
    if not condition:
        raise ValueError(message)


def _object(pairs):
    result = {}
    for key, value in pairs:
        _require(key not in result, "Duplicate JSON member in retained evidence")
        result[key] = value
    return result


def _constant(value):
    raise ValueError("Nonfinite JSON in retained evidence")


def _data(text):
    return json.loads(text, object_pairs_hook=_object, parse_constant=_constant)


def _control(changes, requirements, reviewer=False):
    artifact = "review.json" if reviewer else "control.json"
    try:
        if not isinstance(changes, dict) or (reviewer and set(changes) != {artifact}):
            return None
        text = changes.get(artifact)
        if not isinstance(text, str) or len(text.encode()) > 32768:
            return None
        value = _data(text)
        fields = {"action", "notes", "remaining"} | ({"candidate"} if reviewer else set())
        if (not isinstance(value, dict) or set(value) != fields
                or value["action"] not in ({"accept", "repair"} if reviewer else {"complete", "continue"})
                or not isinstance(value["notes"], str) or len(value["notes"]) > 8192
                or not isinstance(value["remaining"], list)
                or any(not isinstance(item, str) or item not in requirements for item in value["remaining"])
                or len(set(value["remaining"])) != len(value["remaining"])
                or (reviewer and value["candidate"] not in {"c0", "c1", "c2", "c3"})):
            return None
        return value
    except (TypeError, ValueError, RecursionError, UnicodeError):
        return None


def _visible_feedback(receipt, cases):
    rows = []
    for index, (case, outcome) in enumerate(zip(cases, receipt["outcomes"])):
        row = {"id": case.get("id") or f"visible-{index}-{digest(case['input'])[:16]}",
               "requirement": case.get("requirement"), "input": case["input"],
               "expected": case["expected"], "passed": outcome["passed"], "status": outcome.get("status")}
        row.update({key: outcome[key] for key in ("actual", "error", "stderr") if key in outcome})
        rows.append(row)
    return dict(passed=receipt["passed"], outcomes=rows)


def _load(path):
    try:
        return json.loads(Path(path).read_text(), object_pairs_hook=_object,
                          parse_constant=_constant)
    except (OSError, UnicodeError, json.JSONDecodeError, RecursionError) as error:
        raise ValueError(f"Cannot read retained JSON: {path}") from error


def _inside(path, root):
    path = Path(path).resolve()
    _require(path.is_relative_to(root), "Evidence path escapes the retained run")
    return path


def _cases(project, stage, kind):
    return [case for item in project["stages"][:stage + 1] for case in item[kind + "_cases"]]


def _receipt(receipt, files, cases, label):
    _require(receipt.get("source_sha256") == digest(files), label + ": source hash mismatch")
    _require(receipt.get("suite_sha256") == digest(cases), label + ": suite hash mismatch")
    rows = receipt.get("outcomes", [])
    _require(receipt.get("cleanup_verified") is True, label + ": sandbox cleanup unverified")
    _require(len(rows) == len(cases) and [row.get("index") for row in rows] == list(range(len(cases)))
             and all(type(row.get("passed")) is bool for row in rows), label + ": incomplete outcome matrix")
    passed = all(row["passed"] for row in rows)
    _require(type(receipt.get("passed")) is bool and receipt["passed"] == passed
             and receipt.get("status") == ("passed" if passed else "failed"), label + ": inconsistent aggregate")
    for case, row in zip(cases, rows):
        if row["passed"]:
            _require("actual" in row and digest(row["actual"]) == digest(case["expected"]),
                     label + ": passing outcome has wrong actual value")
    return rows


def summarize_completed(report):
    """Summarize only finalized case records, with an explicit interruption note."""
    groups = {}
    for case in report.get("cases", []):
        group = groups.setdefault(case["policy"], dict(project_runs=0, accepted=0,
                    milestones_completed=0, milestones_possible=0,
                    requirements_passed=0, requirements_checked=0, usage_micro_usd=0,
                    builder_calls=0, regressions=0, premature_completion=0, stagnation_events=0))
        group["project_runs"] += 1
        group["accepted"] += int(case["accepted"])
        group["milestones_completed"] += case["milestones_completed"]
        group["milestones_possible"] += report["contract"]["milestones"]
        group["requirements_passed"] += sum(case["requirement_coverage"].values())
        group["requirements_checked"] += len(case["requirement_coverage"])
        group["usage_micro_usd"] += case["usage_micro_usd"]
        for name in ("builder_calls", "regressions", "premature_completion", "stagnation_events"):
            group[name] += case["metrics"][name]
    for group in groups.values():
        group["proposal_regressions_per_builder_call"] = group["regressions"] / group["builder_calls"] if group["builder_calls"] else None
    warning = None
    if report.get("status") != "finished" or report.get("unexecuted"):
        warning = ("Partial study: aggregates include finalized case records only; "
                   "unexecuted or interrupted projects are excluded, and charged partial work may remain.")
    return {"policies": groups, "completed_case_records": len(report.get("cases", [])),
            "unexecuted": report.get("unexecuted", []), "warning": warning}


def _audit_run(run, *, accounting_ledger=None):
    """Return a JSON audit or raise ValueError on any broken evidence binding."""
    auditor_sha = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    run = Path(run).resolve()
    if run.is_file():
        _require(run.name == "results.json", "Expected a run directory or its results.json")
        run = run.parent
    report = _load(run / "results.json")
    contract = report["contract"]
    _require(report.get("experiment") == "sustained-quality-v1", "Wrong study protocol")
    _require(report.get("contract_sha") == digest(contract), "Contract digest mismatch")
    preregistered = _load(run / "preregistered.json")
    _require(preregistered["contract"] == contract, "Preregistered contract mismatch")
    for name, expected in contract["sources"].items():
        source = _inside(run / "source-snapshot" / "gossip_harness" / name, run)
        _require(hashlib.sha256(source.read_bytes()).hexdigest() == expected,
                 "Source snapshot mismatch: " + name)
    fixture = _load(run / "fixtures.json")
    _require(digest(fixture) == contract["fixture_sha256"], "Retained fixture digest mismatch")
    project_map = {project["id"]: project for project in fixture}
    _require(set(project_map) == set(contract["project_ids"]) and len(project_map) == len(fixture),
             "Retained fixture project roster mismatch")
    roster = [tuple(item) for item in preregistered["roster"]]
    wanted = {(project, policy, repetition) for repetition in range(report["repetitions"])
              for project in contract["project_ids"] for policy in contract["policies"]}
    _require(len(roster) == len(set(roster)) and set(roster) == wanted, "Invalid preregistered roster")
    actual = [(case["project_id"], case["policy"], case["repetition"]) for case in report["cases"]]
    unexecuted = [tuple(item) for item in report["unexecuted"]]
    _require(len(actual) == len(set(actual)) and len(unexecuted) == len(set(unexecuted))
             and not set(actual) & set(unexecuted) and set(actual) | set(unexecuted) == wanted,
             "Finalized/unexecuted case roster mismatch")
    _require(report["status"] != "finished" or not unexecuted, "Finished study has unexecuted projects")
    git_cache = {}
    invocation_bindings = []
    billing_bindings = []
    audits = []
    ledger_paths = set()
    namespaces = []

    def git_files(binding):
        path = _inside(binding["store_path"], run)
        key = (str(path), binding["tip_sha"])
        if key not in git_cache:
            git_cache[key] = GitStore(path).read_files(binding["tip_sha"])
        files = git_cache[key]
        if "files_sha256" in binding:
            _require(digest(files) == binding["files_sha256"], "Git source binding hash mismatch")
        return files

    def git_head_files(path):
        path = _inside(path, run)
        store = GitStore(path)
        head = store.head()
        return head, git_files(dict(store_path=str(path), tip_sha=head))

    for case in report["cases"]:
        root = _inside(case["root"], run)
        _require(root.name == case["run_id"] and _load(root / "result.json") == case,
                 "Case result does not match report")
        config = _load(root / "config.json")
        _require(config["contract_sha"] == report["contract_sha"]
                 and all(config[name] == case[name] for name in ("project_id", "policy", "repetition", "run_id")),
                 "Case config mismatch")
        ledger_paths.add(str(Path(config["budget_ledger"]).resolve()))
        namespaces.append(config["namespace"])
        project = project_map[case["project_id"]]
        trajectory_bytes = (root / "trajectory.json").read_bytes()
        _require(hashlib.sha256(trajectory_bytes).hexdigest() == case["trajectory_sha256"],
                 "Frozen trajectory digest mismatch")
        state = _load(root / "trajectory.json")
        _require(1 <= len(state["stages"]) <= contract["milestones"], "Invalid milestone trajectory length")
        _require(state["contract_sha"] == report["contract_sha"] and state["files"] == state["stages"][-1]["files"]
                 and digest(state["files"]) == case["files_sha256"], "Final source/trajectory mismatch")
        _, initial = git_head_files(root / "initial.git")
        _require(initial == project["initial_files"], "Initial repository fixture mismatch")
        previous_files = initial
        calls = []
        stage_metrics = defaultdict(int)
        review_comparisons = []
        trace = []
        for path in sorted(root.glob("trace-*.jsonl")):
            trace.extend(json.loads(line, object_pairs_hook=_object, parse_constant=_constant)
                         for line in path.read_text().splitlines() if line.strip())
        starts = {(row["stage_index"], row["call_id"]): row for row in trace if row["kind"] == "request_started"}
        _require(len(starts) == sum(row["kind"] == "request_started" for row in trace), "Duplicate request-start trace")
        for stage_index, stage in enumerate(state["stages"]):
            stage_root = root / f"stage-{stage_index}"
            _require(_load(stage_root / "result.json") == stage and stage["stage_index"] == stage_index,
                     "Stage result does not match trajectory")
            suite = _cases(project, stage_index, "visible")
            slot_files = {}
            candidates = {}
            slot_history = {}
            current_history = []
            prior = state["stages"][stage_index - 1] if stage_index else {}
            requirements = [item for item_stage in project["stages"][:stage_index + 1]
                            for item in item_stage["requirements"]]
            previous_review = None
            current_notes = prior.get("notes", "")
            candidate_notes = {}
            reviewer_notes = {}
            if prior:
                previous_feedback = _visible_feedback(prior["visible_receipt"], _cases(project, stage_index - 1, "visible"))
                seed = dict(source_sha=digest(previous_files),
                            failures=sorted(row["id"] for row in previous_feedback["outcomes"] if not row["passed"]),
                            passing={row["id"] for row in previous_feedback["outcomes"] if row["passed"]},
                            source_run=0, failure_run=0)
                slot_history = {slot: dict(seed) for slot in ("single", "c0", "c1", "c2", "c3")}

            def check_context(request, source_files, expected_history, expected_notes):
                _require(set(request["files"]) == set(source_files) | {"__stage_context__.json"}
                         and all(request["files"][name] == content for name, content in source_files.items()),
                         "Model request contains unexpected files or incorrect source trees")
                context = _data(request["files"]["__stage_context__.json"])
                expected = dict(project_id=project["id"], project_title=project["title"], stage_index=stage_index,
                                milestones=[dict(index=index, spec=item["spec"], requirements=item["requirements"])
                                            for index, item in enumerate(project["stages"][:stage_index + 1])],
                                visible_cases=suite, prior_agent_notes=prior.get("notes", ""),
                                prior_machine_history=prior.get("check_history", []), machine_history=expected_history,
                                current_agent_notes=expected_notes)
                _require(set(context) == set(expected)
                         and all(context[key] == value for key, value in expected.items()),
                         "Model context differs from cumulative visible specification/history")

            builder_rows = []
            reviewer_rows = []
            invocation_map = {row["call_id"]: row for row in stage["invocations"]}
            _require(len(invocation_map) == len(stage["invocations"]), "Duplicate invocation ID")
            for row in stage["trajectory"]:
                if row["kind"] == "builder":
                    builder_rows.append(row)
                    candidate = row["candidate"]
                    call_id = f"builder-{candidate}-{row['round']}"
                    binding = row["binding"]
                    source_path = _inside(binding["store_path"], run).with_suffix(".json")
                    saved = _load(source_path)
                    files = git_files(binding)
                    _require(saved == dict(binding=binding, files=files)
                             and digest(files) == row["files_sha256"], "Proposal JSON/Git/hash mismatch")
                    response = _load(stage_root / "requests" / (call_id + ".result.json"))
                    request = _load(stage_root / "requests" / (call_id + ".request.json"))
                    before = slot_files.get(candidate, previous_files)
                    initial_pool = case["policy"] == "reviewed-portfolio" and row["round"] == 1
                    expected_notes = prior.get("notes", "") if initial_pool else current_notes
                    if case["policy"] == "reviewed-portfolio" and not initial_pool:
                        expected_notes = candidate_notes[candidate]
                        if reviewer_notes.get(candidate):
                            expected_notes += "\nReviewer checkpoint for this candidate:\n" + reviewer_notes[candidate]
                    check_context(request, before, [] if initial_pool else current_history, expected_notes)
                    _require(request["allowed_paths"] == [*project["allowed_paths"], "control.json"],
                             "Builder request permits unauthorized paths")
                    if row["round"] == 1:
                        _require(_data(request["feedback"]) == {"resume_notes": prior.get("notes", "")},
                                 "Initial builder feedback is not the checkpoint notes")
                    else:
                        previous = candidates[candidate]
                        expected_review = None
                        if case["policy"] == "reviewed-portfolio":
                            _require(candidate == previous_review["selected"], "Repair changed the wrong candidate slot")
                            expected_review = dict(review=previous_review["review"], errors=previous_review["errors"],
                                                   fallback_used=previous_review["fallback_used"])
                        _require(_data(request["feedback"]) == dict(
                            visible_validation=_visible_feedback(previous["receipt"], suite),
                            control_errors=previous["row"]["errors"], stagnation_warning=previous["row"]["stagnant"],
                            review=expected_review), "Builder repair feedback differs from visible evidence")
                    changes = response.get("changes", {})
                    valid = ("changes" in response and isinstance(changes, dict)
                             and all(path in (*project["allowed_paths"], "control.json") for path in changes)
                             and all(isinstance(value, str) for path, value in changes.items()
                                     if path in project["allowed_paths"]))
                    expected_files = dict(before)
                    if valid:
                        expected_files.update({path: value for path, value in changes.items()
                                               if path in project["allowed_paths"]})
                    _require(files == expected_files and row["source_valid"] == valid,
                             "Retained candidate does not match the model's permitted source changes")
                    control = _control(changes, requirements)
                    _require(row["control"] == control, "Builder control decision differs from raw model artifact")
                    if valid and control:
                        candidate_notes[candidate] = control["notes"]
                    else:
                        candidate_notes.setdefault(candidate, prior.get("notes", ""))
                    current_notes = candidate_notes[candidate]
                    receipt = _load(source_path.with_name(source_path.stem + ".validation.json"))
                    outcomes = _receipt(receipt, files, suite, call_id)
                    _require(row["visible_passed"] == receipt["passed"], "Trajectory visible outcome mismatch")
                    complete = bool(control and control["action"] == "complete" and not control["remaining"]
                                    and valid and receipt["passed"])
                    _require(row["complete"] == complete, "Builder completion gate mismatch")
                    feedback = _visible_feedback(receipt, suite)
                    passed_ids = {item["id"] for item in feedback["outcomes"] if item["passed"]}
                    failed_ids = sorted(item["id"] for item in feedback["outcomes"] if not item["passed"])
                    old = slot_history.get(candidate)
                    regression = sorted(old["passing"] - passed_ids) if old else []
                    source_run = old["source_run"] + 1 if old and old["source_sha"] == digest(files) else 0
                    failure_run = old["failure_run"] + 1 if old and old["failures"] == failed_ids and failed_ids else 0
                    stagnant = source_run >= 2 or failure_run >= 2
                    _require(row["regressions"] == regression and row["stagnant"] == stagnant,
                             "Regression or stagnation label differs from validated candidate history")
                    slot_history[candidate] = dict(passing=passed_ids, failures=failed_ids, source_sha=digest(files),
                                                   source_run=source_run, failure_run=failure_run)
                    current_history.append(dict(stage_index=stage_index, candidate=candidate, round=row["round"],
                        passed=receipt["passed"], outcomes=feedback["outcomes"], errors=row["errors"],
                        regressions=regression, stagnant=stagnant))
                    slot_files[candidate] = files
                    candidates[candidate] = dict(row=row, files=files, receipt=receipt,
                                                failures=sum(not item["passed"] for item in outcomes))
                elif row["kind"] == "reviewer":
                    reviewer_rows.append(row)
                    _require(row["selected"] in candidates, "Reviewer selected an unknown candidate")
                    chosen = candidates[row["selected"]]
                    best = min(candidates, key=lambda key: (not candidates[key]["row"]["source_valid"],
                                                            candidates[key]["failures"], key))
                    call_id = f"reviewer-portfolio-{row['round']}"
                    request = _load(stage_root / "requests" / (call_id + ".request.json"))
                    response = _load(stage_root / "requests" / (call_id + ".result.json"))
                    review = _control(response.get("changes", {}), requirements, reviewer=True)
                    _require(row["review"] == review and row["selected"] == (review["candidate"] if review else best)
                             and row["fallback_used"] == (review is None), "Review decision differs from raw model artifact")
                    review_files = {f"candidates/{slot}/{path}": content for slot, item in candidates.items()
                                    for path, content in item["files"].items()}
                    check_context(request, review_files, current_history, current_notes)
                    _require(request["allowed_paths"] == ["review.json"], "Reviewer request permits source changes")
                    evidence = dict(candidates={slot: dict(binding=item["row"]["binding"], control=item["row"]["control"],
                                     visible_validation=_visible_feedback(item["receipt"], suite), errors=item["row"]["errors"])
                                               for slot, item in candidates.items()},
                                    previous_review_errors=previous_review["errors"] if previous_review else [])
                    _require(_data(request["feedback"]) == evidence, "Reviewer saw an incorrect candidate evidence matrix")
                    _require(row["binding"] == chosen["row"]["binding"], "Review is bound to a stale candidate")
                    _require(row["accepted"] == bool(review and review["action"] == "accept" and not review["remaining"]
                             and chosen["receipt"]["passed"] and chosen["row"]["source_valid"]),
                             "Invalid passing reviewer gate")
                    previous_review = row
                    if review:
                        current_notes = review["notes"]
                        reviewer_notes[row["selected"]] = review["notes"]
                    review_comparisons.append(dict(stage_index=stage_index, round=row["round"],
                        selected=row["selected"], visible_best=best, changed_choice=row["selected"] != best,
                        accepted=row["accepted"], selected_failed_cases=chosen["failures"],
                        best_failed_cases=candidates[best]["failures"]))
                else:
                    raise ValueError("Unknown stage trajectory event")
            _require(stage["selected"] in candidates, "Stage selected unknown candidate")
            final_decision = stage["trajectory"][-1]
            decision_complete = final_decision.get("accepted") is True if reviewer_rows else final_decision.get("complete") is True
            expected_selected = (final_decision["selected"] if decision_complete else min(candidates, key=lambda key:
                                 (not candidates[key]["row"]["source_valid"], candidates[key]["failures"], key))) if reviewer_rows else "single"
            _require(stage["completed"] == decision_complete and stage["selected"] == expected_selected,
                     "Stage selection/completion differs from its final reviewer or deterministic fallback")
            selected = candidates[stage["selected"]]
            checkpoint_notes = candidate_notes[stage["selected"]]
            if reviewer_notes.get(stage["selected"]):
                checkpoint_notes += "\nReviewer checkpoint for this candidate:\n" + reviewer_notes[stage["selected"]]
            _require(stage["notes"] == checkpoint_notes, "Checkpoint notes belong to a different candidate")
            _require(stage["check_history"] == prior.get("check_history", []) + current_history,
                     "Checkpoint machine history does not match the validated trajectory")
            _require(stage["selected_binding"] == selected["row"]["binding"]
                     and stage["files"] == selected["files"]
                     and stage["visible_receipt"] == selected["receipt"], "Selected-stage source/evidence mismatch")
            if stage["completed"]:
                final_row = stage["trajectory"][-1]
                _require(final_row.get("accepted") is True if reviewer_rows else final_row.get("complete") is True,
                         "Stage claims completion without a final passing decision")
                head, files = git_head_files(stage_root / "checkpoint.git")
                _require(files == stage["files"] and state["completed_stages"][stage_index] ==
                         dict(completed=True, stage_index=stage_index, head=head), "Milestone checkpoint mismatch")
            counts = dict(builder_calls=len(builder_rows), reviewer_calls=len(reviewer_rows),
                          repairs=len(builder_rows) - (4 if reviewer_rows else 1),
                          premature_completion=sum(bool(row.get("control") and row["control"]["action"] == "complete"
                              and not row["complete"]) for row in builder_rows)
                              + sum(bool(row.get("review") and row["review"]["action"] == "accept"
                                         and not row["accepted"]) for row in reviewer_rows),
                          regressions=sum(len(row["regressions"]) for row in builder_rows),
                          stagnation_events=sum(row["stagnant"] for row in builder_rows))
            if case["policy"] == "reviewed-portfolio":
                _require(4 <= len(builder_rows) <= 4 + contract["portfolio_repairs"]
                         and 1 <= len(reviewer_rows) <= contract["reviewer_calls_per_stage"]
                         and [(row["candidate"], row["round"]) for row in builder_rows[:4]] == [(f"c{i}", 1) for i in range(4)]
                         and [row["round"] for row in reviewer_rows] == list(range(1, len(reviewer_rows) + 1))
                         and [row["round"] for row in builder_rows[4:]] == list(range(2, len(builder_rows) - 2))
                         and len(builder_rows) == 3 + len(reviewer_rows), "Portfolio role or call limits mismatch")
                kinds = ["builder"] * 4
                for index in range(len(reviewer_rows)):
                    kinds.append("reviewer")
                    if index < len(reviewer_rows) - 1:
                        kinds.append("builder")
                _require([row["kind"] for row in stage["trajectory"]] == kinds
                         and not any(row["accepted"] for row in reviewer_rows[:-1]),
                         "Portfolio continued after acceptance or violated review/repair order")
            else:
                _require(not reviewer_rows and 1 <= len(builder_rows) <= contract["baseline_calls_per_stage"]
                         and all(row["candidate"] == "single" for row in builder_rows)
                         and [row["round"] for row in builder_rows] == list(range(1, len(builder_rows) + 1)),
                         "Sequential role or call limits mismatch")
                _require(not any(row["complete"] for row in builder_rows[:-1]), "Sequential policy continued after completion")
            _require(stage["completed"] or len(builder_rows) == contract["baseline_calls_per_stage"],
                     "Incomplete stage stopped before its bounded call limit")
            for name, value in counts.items():
                _require(stage["metrics"][name] == value, "Stage metric mismatch: " + name)
                stage_metrics[name] += value
            _require(set(invocation_map) == {f"builder-{row['candidate']}-{row['round']}" for row in builder_rows}
                     | {f"reviewer-portfolio-{row['round']}" for row in reviewer_rows}, "Unlinked model invocation")
            billing = config["namespace"] + f"/stage-{stage_index}"
            record_head, record_files = git_head_files(stage_root / "record.git")
            _require(json.loads(record_files["record.json"]) == {
                "completed": stage["completed"], "status": stage["status"],
                "selected_binding": stage["selected_binding"]}, "Accounting record does not bind stage result")
            billing_bindings.append((billing, record_head))
            for call_id, invocation in invocation_map.items():
                request = _load(stage_root / "requests" / (call_id + ".request.json"))
                response = _load(stage_root / "requests" / (call_id + ".result.json"))
                start = starts.get((stage_index, call_id))
                _require(start is not None and start["request_sha256"] == digest(request)
                         and request["task_id"] == billing + "/" + call_id
                         and invocation["reservation"] == billing + "/" + call_id
                         and all(response.get(key) == value for key, value in invocation.items()),
                         "Request/result/invocation trace binding mismatch")
                _require(type(invocation["usage_units"]) is int and invocation["usage_units"] >= 0,
                         "Completed case has unknown or invalid usage")
                expected_role = "reviewer" if call_id.startswith("reviewer-") else "builder"
                expected_model = "strong" if expected_role == "reviewer" or case["policy"] == "strong-single" else "cheap"
                _require(invocation["role"] == expected_role and invocation["model"] == start["model"] == expected_model,
                         "Invocation model/role differs from the policy")
                if report["mode"] == "live" and "changes" in response:
                    _require(response["metadata"].get("model") == contract["models"][expected_model]["model"],
                             "Returned model differs from the pinned policy model")
                invocation_bindings.append((invocation, billing, start["reserved_units"]))
            calls.extend(stage["invocations"])
            previous_files = stage["files"]
        _require(calls == state["invocations"] == case["invocations"], "Project invocation roster mismatch")
        _require(dict(stage_metrics) == case["metrics"] and sum(call["usage_units"] for call in calls)
                 == case["usage_micro_usd"], "Project metrics or usage mismatch")
        completed = sum(stage["completed"] for stage in state["stages"])
        _require(completed == len(state["completed_stages"]) == case["milestones_completed"]
                 and all(stage["completed"] for stage in state["stages"][:-1]), "Milestone progression mismatch")
        if len(state["stages"]) == contract["milestones"]:
            handoff = _load(root / "handoff.json")
            checkpoint_path = root / "checkpoint.json"
            checkpoint = _load(checkpoint_path)
            resumed = _load(root / "resume.json")
            head, files = git_head_files(checkpoint["store_path"])
            _require(hashlib.sha256(checkpoint_path.read_bytes()).hexdigest() == handoff["checkpoint_sha256"]
                     == resumed["checkpoint_sha256"] and checkpoint["contract_sha"] == report["contract_sha"]
                     and checkpoint["next_stage"] == 2 and checkpoint["stages"] == state["stages"][:2]
                     and checkpoint["files"] == files == state["stages"][1]["files"]
                     and checkpoint["head"] == head == handoff["head"] == resumed["head"]
                     and checkpoint["pid"] == handoff["previous_pid"] == resumed["previous_pid"]
                     and type(resumed["resumed_pid"]) is int and resumed["resumed_pid"] > 0
                     and resumed["previous_pid"] != resumed["resumed_pid"] and resumed["verified"] is True
                     and resumed["files_sha256"] == digest(files) and resumed == case["handoff"],
                     "Cross-process checkpoint/resume mismatch")
        else:
            _require(not case["handoff"], "Unexpected handoff for a project stopped before milestone three")
        final_visible = _receipt(case["final_visible"], state["files"], _cases(project, 2, "visible"), "Final visible")
        hidden_cases = _cases(project, 2, "hidden")
        final_hidden = _receipt(case["final_hidden"], state["files"], hidden_cases, "Final hidden")
        _require(len(case["historical"]) == len(state["stages"]), "Historical evaluation roster mismatch")
        selected_hidden_transitions = []
        previous_hidden = None
        for index, historical in enumerate(case["historical"]):
            _require(historical["stage_index"] == index and historical["binding"] == state["stages"][index]["selected_binding"],
                     "Historical evidence has the wrong stage binding")
            historical_cases = _cases(project, index, "hidden")
            historical_rows = _receipt(historical["hidden"], state["stages"][index]["files"], historical_cases, "Historical hidden")
            current_hidden = {item.get("id", digest(item["input"])): row["passed"]
                              for item, row in zip(historical_cases, historical_rows)}
            if previous_hidden is not None:
                common = set(previous_hidden) & set(current_hidden)
                selected_hidden_transitions.append(dict(from_stage=index - 1, to_stage=index,
                    recurring_cases=len(common),
                    regressed_case_ids=sorted(key for key in common if previous_hidden[key] and not current_hidden[key]),
                    recovered_case_ids=sorted(key for key in common if not previous_hidden[key] and current_hidden[key])))
            previous_hidden = current_hidden
        coverage = {requirement: all(row["passed"] for case_data, row in zip(hidden_cases, final_hidden)
                                    if case_data["requirement"] == requirement)
                    for requirement in sorted({item["requirement"] for item in hidden_cases})}
        accepted = completed == contract["milestones"] and all(row["passed"] for row in final_visible + final_hidden)
        _require(case["requirement_coverage"] == coverage and case["accepted"] == accepted, "Final quality calculation mismatch")
        if accepted:
            head, files = git_head_files(root / "release.git")
            exported = {str(path.relative_to(root / "accepted")): path.read_text()
                        for path in (root / "accepted").rglob("*") if path.is_file()}
            _require(head == case["release_head"] == case["exact_tested_sha"]
                     and files == exported == state["files"], "Accepted release/export mismatch")
        else:
            _require(case["release_head"] is None and case["exact_tested_sha"] is None
                     and not (root / "accepted").exists(), "Failed project has accepted release evidence")
        audits.append(dict(run_id=case["run_id"], accepted=accepted,
            final_hidden_failed_case_ids=[item.get("id", index) for index, (item, row)
                                          in enumerate(zip(hidden_cases, final_hidden)) if not row["passed"]],
            review_choices=review_comparisons, verified_builder_proposals=stage_metrics["builder_calls"],
            verified_requests=len(calls), selected_hidden_transitions=selected_hidden_transitions))

    _require(len(ledger_paths) <= 1, "Cases use different shared accounting ledgers")
    ledger_path = Path(accounting_ledger).resolve() if accounting_ledger else (
        Path(next(iter(ledger_paths))) if ledger_paths else run / "budget.sqlite")
    _require(ledger_path.is_file(), "Accounting ledger is missing")
    db = sqlite3.connect(ledger_path.as_uri() + "?mode=ro", uri=True, timeout=10)
    db.row_factory = sqlite3.Row
    try:
        db.execute("PRAGMA query_only=ON")
        db.execute("BEGIN")
        for invocation, billing, reserved in invocation_bindings:
            row = db.execute("SELECT * FROM reservations WHERE id=?", (invocation["reservation"],)).fetchone()
            _require(row is not None and row["task_id"] == billing and row["state"] == "settled"
                     and row["amount"] == reserved and row["spent"] == invocation["usage_units"],
                     "Request usage is not exactly bound to a settled reservation")
        for billing, head in billing_bindings:
            row = db.execute("SELECT * FROM tasks WHERE id=?", (billing,)).fetchone()
            _require(row is not None and row["status"] == "complete" and row["accepted_commit"] == head
                     and row["intent_id"] is None, "Stage billing task is not complete at its record commit")
            observed = {item[0] for item in db.execute("SELECT id FROM reservations WHERE task_id=?", (billing,))}
            expected = {invocation["reservation"] for invocation, task, _ in invocation_bindings if task == billing}
            _require(observed == expected, "Unlinked reservation in completed stage")
        for namespace in namespaces:
            observed = {item[0] for item in db.execute("SELECT id FROM reservations WHERE task_id LIKE ?",
                                                     (namespace + "/%",))}
            expected = {invocation["reservation"] for invocation, task, _ in invocation_bindings
                        if task.startswith(namespace + "/")}
            _require(observed == expected, "Unlinked reservation in finalized project")
        unsettled = db.execute("SELECT COUNT(*) FROM reservations WHERE spent IS NULL OR state!='settled'").fetchone()[0]
        pending = db.execute("SELECT COUNT(*) FROM intents WHERE state='pending'").fetchone()[0]
        current_spend = db.execute("SELECT COALESCE(SUM(COALESCE(spent,amount)),0) FROM reservations").fetchone()[0]
        cap = db.execute("SELECT value FROM settings WHERE key='budget'").fetchone()[0]
    finally:
        db.close()
    usage = sum(invocation["usage_units"] for invocation, _, _ in invocation_bindings)
    if report["status"] == "finished":
        _require(usage == report["incremental_micro_usd"]
                 == report["budget"]["spent_or_reserved"] - report["budget_before"]["spent_or_reserved"],
                 "Finished-study budget delta differs from request usage")
    summary = summarize_completed(report)
    for case, audited in zip(report["cases"], audits):
        group = summary["policies"][case["policy"]]
        group.setdefault("selected_hidden_regressions", 0)
        group.setdefault("selected_hidden_recoveries", 0)
        group.setdefault("selected_hidden_transition_opportunities", 0)
        for transition in audited["selected_hidden_transitions"]:
            group["selected_hidden_regressions"] += len(transition["regressed_case_ids"])
            group["selected_hidden_recoveries"] += len(transition["recovered_case_ids"])
            group["selected_hidden_transition_opportunities"] += transition["recurring_cases"]
    _require(hashlib.sha256(Path(__file__).read_bytes()).hexdigest() == auditor_sha,
             "Auditor source changed during verification")
    return dict(audit_version=1, auditor_source_sha256=auditor_sha,
        passed=True, run=str(run), status=report["status"],
        source_snapshot_verified=True, fixtures_verified=True, summary=summary,
        cases=audits, verified_requests=len(invocation_bindings), verified_billing_tasks=len(billing_bindings),
        verified_usage_micro_usd=usage, accounting=dict(ledger=str(ledger_path), current_spent_or_reserved=current_spend,
        limit=cap, unsettled_reservations=unsettled, pending_promotions=pending),
        caveats=["Hashes verify consistency of retained evidence, not an external runtime attestation.",
                 "Review comparisons use visible evidence only; unselected candidates have no hidden-quality counterfactual.",
                 "Raw proposal regressions include four initial portfolio branches; compare opportunity denominators, not raw counts.",
                 "Selected hidden regression/recovery transitions are retrospective diagnostics unavailable to the agents."])


def audit_run(run, *, accounting_ledger=None):
    """Verify evidence without mutations; malformed or broken bindings fail closed."""
    try:
        return _audit_run(run, accounting_ledger=accounting_ledger)
    except ValueError:
        raise
    except (OSError, KeyError, IndexError, TypeError, AttributeError, RuntimeError, sqlite3.Error) as error:
        raise ValueError("Retained evidence is incomplete, malformed, or unreadable: " + str(error)) from error


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--accounting-ledger", type=Path)
    parser.add_argument("--output", type=Path, help="Create a new audit JSON; existing evidence is never overwritten")
    args = parser.parse_args()
    result = audit_run(args.run, accounting_ledger=args.accounting_ledger)
    data = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        with args.output.open("x") as stream:
            stream.write(data)
    print(data, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
