"""Version-two milestone loops with batched, source-bound visible observations.

The v1 engine remains byte-for-byte unchanged for frozen studies. This engine
separates immutable proposal preparation, serial Git retention, independent
validation, and deterministic controller reduction. It performs no receipt
reuse itself: the caller owns the explicit versioned reuse policy.

This module performs no I/O itself. The caller owns model accounting, sandbox
execution, Git retention, and hidden evaluation. Only explicitly selected public
project fields are ever passed to a model; hidden cases and reference code must
stay in the caller. Completion is a claim checked against cumulative visible
evidence, not a certificate of hidden correctness.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import hashlib
import json
from typing import Any


POLICIES = ("strong-single", "cheap-sequential", "reviewed-portfolio")
BASELINE_CALLS = 8
PORTFOLIO_CANDIDATES = 4
PORTFOLIO_REPAIRS = 4
CONTEXT_PATH = "__stage_context__.json"
PROTOCOL = "sustained-stage-v2"


def _json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=True, allow_nan=False,
                      separators=(",", ":"))


def _sha(value):
    return hashlib.sha256(_json(value).encode()).hexdigest()


def _pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _constant(value):
    raise ValueError("nonfinite JSON")


def _decode(text):
    if not isinstance(text, str) or len(text.encode()) > 32_768:
        raise ValueError("control artifact must be bounded text")
    return json.loads(text, object_pairs_hook=_pairs, parse_constant=_constant)


def _requirement_ids(stages):
    result = []
    for stage in stages:
        for item in stage["requirements"]:
            key = item["id"] if isinstance(item, dict) else item
            if not isinstance(key, str) or not key:
                raise ValueError("requirements must have nonempty string IDs")
            if key not in result:
                result.append(key)
    return result


def _control(text, requirements, reviewer=False):
    try:
        value = _decode(text)
        fields = {"action", "notes", "remaining"}
        if reviewer:
            fields.add("candidate")
        if not isinstance(value, dict) or set(value) != fields:
            raise ValueError("wrong control fields")
        actions = {"accept", "repair"} if reviewer else {"continue", "complete"}
        if value["action"] not in actions:
            raise ValueError("invalid action")
        if (not isinstance(value["notes"], str) or len(value["notes"]) > 8192
                or not isinstance(value["remaining"], list)
                or any(not isinstance(item, str) or item not in requirements
                       for item in value["remaining"])
                or len(set(value["remaining"])) != len(value["remaining"])):
            raise ValueError("invalid notes or requirement IDs")
        if reviewer and value["candidate"] not in {
                f"c{index}" for index in range(PORTFOLIO_CANDIDATES)}:
            raise ValueError("invalid candidate ID")
        return value, None
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
        return None, "Invalid review.json" if reviewer else "Invalid control.json"


def _source_changes(initial, allowed, result, artifact):
    """Control is separate from source. Invalid source edits reject atomically."""
    if result is None:
        return dict(initial), "Worker returned no usable proposal", None
    changes = getattr(result, "changes", None)
    if not isinstance(changes, dict):
        return dict(initial), "Worker changes must be a mapping", None
    control = changes.get(artifact)
    illegal = sorted(str(path) for path in changes if path not in (*allowed, artifact))
    if illegal:
        return dict(initial), "Proposal changes unauthorized paths: " + ", ".join(illegal), control
    if any(not isinstance(value, str) for path, value in changes.items()
           if path in allowed):
        return dict(initial), "Source deletion or non-text content is forbidden", control
    files = dict(initial)
    files.update({path: value for path, value in changes.items() if path in allowed})
    if any(path not in files or not isinstance(files[path], str) for path in allowed):
        return dict(initial), "All allowed source files must remain present", control
    return files, None, control


def _case_key(case, index):
    return case.get("id") or f"visible-{index}-{_sha(case['input'])[:16]}"


def _feedback_receipt(receipt, cases):
    """Attach only the visible suite to ordered machine outcomes."""
    if (not isinstance(receipt, dict) or receipt.get("status") not in {"passed", "failed"}
            or receipt.get("cleanup_verified", True) is not True):
        raise RuntimeError("Visible evaluation did not return a valid sandbox result")
    outcomes = receipt.get("outcomes")
    if (not isinstance(outcomes, list) or len(outcomes) != len(cases)
            or type(receipt.get("passed")) is not bool):
        raise RuntimeError("Visible evaluation has an incomplete outcome matrix")
    rows = []
    for index, (case, outcome) in enumerate(zip(cases, outcomes)):
        if (not isinstance(outcome, dict) or type(outcome.get("passed")) is not bool
                or ("index" in outcome and outcome["index"] != index)):
            raise RuntimeError("Visible evaluation outcome is invalid or out of order")
        row = {"id": _case_key(case, index), "requirement": case.get("requirement"),
               "input": case["input"], "expected": case["expected"],
               "passed": outcome["passed"], "status": outcome.get("status")}
        # Validator diagnostics are bounded by the caller. No complete receipt
        # is copied: host-only paths and future private data cannot enter prompts.
        for key in ("actual", "error", "stderr"):
            if key in outcome:
                row[key] = outcome[key]
        rows.append(row)
    passed = all(row["passed"] for row in rows)
    if receipt["passed"] != passed or receipt["status"] != ("passed" if passed else "failed"):
        raise RuntimeError("Visible aggregate result is inconsistent with its outcomes")
    return {"passed": passed, "outcomes": rows}


def _verify_observation(receipt, validation, proposal, cases):
    """Defend the engine boundary without replacing the adapter's full checks."""
    source_sha, suite_sha = proposal["source_sha256"], _sha(cases)
    if (not isinstance(receipt, dict) or not isinstance(validation, dict)
            or receipt.get("source_sha256") != source_sha
            or receipt.get("suite_sha256") != suite_sha
            or receipt.get("cleanup_verified") is not True
            or validation.get("source_sha256") != source_sha
            or validation.get("suite_sha256") != suite_sha
            or validation.get("label") != proposal["label"]
            or validation.get("purpose") != "visible"
            or type(validation.get("physical")) is not bool
            or not isinstance(validation.get("execution_id"), str)
            or not validation["execution_id"]):
        raise RuntimeError("Visible evaluation source, suite, or provenance mismatch")
    reuse = validation.get("reuse")
    if ((validation["physical"] and reuse is not None)
            or (not validation["physical"] and (
                not isinstance(reuse, dict)
                or reuse.get("execution_id") != validation["execution_id"]
                or not isinstance(reuse.get("origin_session_id"), str)
                or not reuse["origin_session_id"]
                or not isinstance(reuse.get("artifact_path"), str)
                or not reuse["artifact_path"]))):
        raise RuntimeError("Visible evaluation lacks valid physical/reused provenance")


def run_stage(project, stage_index, policy, initial_files, prior_state, *,
              invoke, evaluate_many, retain, emit):
    """Run one milestone with bounded persistence and completion gates.

    ``invoke`` may be concurrent only for the initial portfolio. Every proposal
    is parsed and source-hashed before serial ``retain`` calls. ``evaluate_many``
    receives ordered dictionaries with ``files``, ``label``, ``source_sha256``;
    it returns the same-length ordered ``{receipt, validation}`` envelopes.
    The caller may execute this immutable batch concurrently within one resource
    budget. All envelopes pass source, suite, and provenance checks before any
    controller state is reduced in input order. Repairs use singleton batches.
    Exceptions propagate; infrastructure failure never becomes model feedback.
    """
    if policy not in POLICIES:
        raise ValueError("unknown stage policy")
    if type(stage_index) is not int or not 0 <= stage_index < len(project["stages"]):
        raise ValueError("invalid stage index")
    stages = project["stages"][:stage_index + 1]
    allowed = tuple(project["allowed_paths"])
    if (not allowed or len(set(allowed)) != len(allowed)
            or any(not isinstance(path, str) or not path or path.startswith("/")
                   or "\\" in path or any(part in {"", ".", "..", ".git"}
                                              for part in path.split("/"))
                   or path in {"control.json", "review.json", CONTEXT_PATH}
                   for path in allowed)):
        raise ValueError("invalid allowed source paths")
    if (not isinstance(initial_files, dict)
            or any(not isinstance(path, str) or not isinstance(value, str)
                   for path, value in initial_files.items())
            or any(path not in initial_files for path in allowed)
            or any(path in initial_files for path in (CONTEXT_PATH, "control.json", "review.json"))):
        raise ValueError("initial repository is incomplete or uses reserved paths")
    requirements = _requirement_ids(stages)
    cases = [deepcopy(case) for stage in stages for case in stage["visible_cases"]]
    if not cases:
        raise ValueError("a milestone needs visible evidence")
    case_keys = [_case_key(case, index) for index, case in enumerate(cases)]
    if len(case_keys) != len(set(case_keys)):
        raise ValueError("visible case IDs must be unique")
    prior_state = prior_state or {}
    prior_notes = prior_state.get("notes", "")
    prior_history = deepcopy(prior_state.get("check_history", []))
    # Project dictionaries deliberately are not serialized. They also contain
    # hidden cases and known files, which must never be in model context.
    context = {
        "project_id": project["id"], "project_title": project["title"],
        "stage_index": stage_index,
        "milestones": [{"index": index, "spec": stage["spec"],
                        "requirements": stage["requirements"]}
                       for index, stage in enumerate(stages)],
        "visible_cases": cases, "prior_agent_notes": prior_notes,
        "prior_machine_history": prior_history,
    }
    metrics = {"builder_calls": 0, "reviewer_calls": 0, "invalid_controls": 0,
               "invalid_proposals": 0, "premature_completion": 0,
               "regressions": 0, "stagnation_events": 0,
               "provider_empty_results": 0, "repairs": 0}
    trajectory = []
    history: list[dict[str, Any]] = []
    previous_by_slot = {}
    review_remaining: dict[str, list[str]] = {}
    review_notes: dict[str, str] = {}
    slot_notes = {slot: prior_notes for slot in
                  ("single", *(f"c{index}" for index in range(PORTFOLIO_CANDIDATES)))}
    notes = prior_notes
    if stage_index and prior_state.get("visible_receipt") is not None:
        old_cases = [case for stage in stages[:-1] for case in stage["visible_cases"]]
        old_feedback = _feedback_receipt(prior_state["visible_receipt"], old_cases)
        seed = {"source_sha": _sha(initial_files),
                "failures": sorted(row["id"] for row in old_feedback["outcomes"] if not row["passed"]),
                "passing": {row["id"] for row in old_feedback["outcomes"] if row["passed"]},
                "unchanged_source_run": 0, "unchanged_failure_run": 0}
        previous_by_slot = {slot: deepcopy(seed) for slot in
                            ("single", *(f"c{index}" for index in range(PORTFOLIO_CANDIDATES)))}

    def request(role, model, files, round_number, candidate_id, feedback, instructions):
        metadata = {"project_id": project["id"], "stage_index": stage_index,
                    "policy": policy, "role": role, "round": round_number,
                    "candidate_id": candidate_id}
        request_files = dict(files)
        request_files[CONTEXT_PATH] = _json({**context, "current_agent_notes": notes,
                                           "machine_history": history})
        return invoke(role=role, model=model, files=request_files,
                      instructions=instructions,
                      allowed_paths=("review.json",) if role == "reviewer"
                      else (*allowed, "control.json"),
                      feedback=_json(feedback), metadata=metadata)

    builder_instructions = (
        "Implement the current milestone while preserving every earlier requirement. "
        f"Read {CONTEXT_PATH} for the full cumulative specification, visible examples, "
        "durable notes and machine-check history. The repository files are supplied. "
        "Change only the allowed source files; return their full replacement text. "
        "Do not delete files, modify trusted files, fabricate executed checks, or "
        "produce implementation shortcuts based on recognizing a test input. Also "
        "return control.json as strict JSON with exactly action, notes and remaining. "
        "action is continue or complete; notes is a useful checkpoint string; "
        "remaining is a list of outstanding requirement IDs from the cumulative "
        "specification. Claim complete only when all cumulative requirements are "
        "implemented, with remaining empty. Visible failures will be returned for "
        "bounded repair; passing visible checks is not proof all requirements hold."
    )

    def prepare_proposal(result, current, slot, round_number):
        # No shared controller state changes here. Hash before retaining or
        # evaluating so a versioned adapter can recognize identical observations.
        files, source_error, text = _source_changes(current, allowed, result, "control.json")
        control, control_error = _control(text, requirements)
        return {"files": files, "source_sha256": _sha(files),
                "source_error": source_error, "control": control,
                "control_error": control_error, "empty_result": result is None,
                "slot": slot, "round": round_number,
                "label": f"stage-{stage_index}-{policy}-{slot}-build-{round_number}"}

    def evaluate_proposals(prepared):
        # Retention may mutate Git or its ledger and therefore stays on the
        # controller thread. Callback inputs are detached from prepared state.
        bindings = [deepcopy(retain(dict(item["files"]), item["label"]))
                    for item in prepared]
        batch = [{key: deepcopy(item[key]) for key in ("files", "label", "source_sha256")}
                 for item in prepared]
        expected_batch = deepcopy(batch)
        envelopes = evaluate_many(batch)
        if batch != expected_batch:
            raise RuntimeError("Visible evaluator mutated its immutable input batch")
        if not isinstance(envelopes, list) or len(envelopes) != len(prepared):
            raise RuntimeError("Visible evaluation returned an incomplete batch")
        observations = []
        for item, envelope in zip(prepared, envelopes):
            if not isinstance(envelope, dict) or set(envelope) != {"receipt", "validation"}:
                raise RuntimeError("Visible evaluation returned an invalid envelope")
            receipt, validation = deepcopy(envelope["receipt"]), deepcopy(envelope["validation"])
            _verify_observation(receipt, validation, item, cases)
            feedback = _feedback_receipt(receipt, cases)
            observations.append((receipt, validation, feedback))
        # Validate the whole independent batch before publishing any of its
        # state transitions, then reduce only on this controller thread.
        return [reduce_proposal(item, binding, *observation)
                for item, binding, observation in zip(prepared, bindings, observations)]

    def proposal(result, current, slot, round_number):
        return evaluate_proposals([prepare_proposal(result, current, slot, round_number)])[0]

    def reduce_proposal(item, binding, receipt, validation, feedback):
        nonlocal notes
        files, source_sha = item["files"], item["source_sha256"]
        source_error, control_error = item["source_error"], item["control_error"]
        control, slot, round_number = item["control"], item["slot"], item["round"]
        metrics["builder_calls"] += 1
        metrics["provider_empty_results"] += int(item["empty_result"])
        metrics["invalid_proposals"] += int(source_error is not None)
        metrics["invalid_controls"] += int(control_error is not None)
        failures = sorted(row["id"] for row in feedback["outcomes"] if not row["passed"])
        passing = {row["id"] for row in feedback["outcomes"] if row["passed"]}
        previous = previous_by_slot.get(slot)
        regression = sorted(previous["passing"] - passing) if previous else []
        same_source = bool(previous and previous["source_sha"] == source_sha)
        same_failures = bool(previous and previous["failures"] == failures and failures)
        unchanged_source_run = previous["unchanged_source_run"] + 1 if previous and same_source else 0
        unchanged_failure_run = previous["unchanged_failure_run"] + 1 if previous and same_failures else 0
        stagnant = unchanged_source_run >= 2 or unchanged_failure_run >= 2
        metrics["regressions"] += len(regression)
        metrics["stagnation_events"] += int(stagnant)
        previous_by_slot[slot] = {"source_sha": source_sha, "failures": failures,
                                  "passing": passing,
                                  "unchanged_source_run": unchanged_source_run,
                                  "unchanged_failure_run": unchanged_failure_run}
        claimed_complete = bool(control and control["action"] == "complete")
        complete = bool(claimed_complete and not control["remaining"]
                        and not source_error and feedback["passed"])
        if claimed_complete and not complete:
            metrics["premature_completion"] += 1
        if control and source_error is None:
            slot_notes[slot] = control["notes"]
        notes = slot_notes[slot]
        errors = [error for error in (source_error, control_error) if error]
        if claimed_complete and control["remaining"]:
            errors.append("Completion requires an empty remaining list")
        if claimed_complete and not feedback["passed"]:
            errors.append("Completion denied because cumulative visible checks failed")
        row = {"kind": "builder", "candidate": slot, "round": round_number,
               "binding": binding, "files_sha256": source_sha,
               "source_valid": source_error is None, "control": control,
               "validation": validation,
               "control_error": control_error, "proposal_error": source_error,
               "complete": complete, "visible_passed": feedback["passed"],
               "regressions": regression, "stagnant": stagnant,
               "errors": errors, "failures": failures}
        trajectory.append(row)
        history.append({"stage_index": stage_index, "candidate": slot,
                        "round": round_number, "passed": feedback["passed"],
                        "outcomes": feedback["outcomes"], "errors": errors,
                        "regressions": regression, "stagnant": stagnant})
        emit("stage_proposal_evaluated", **{key: value for key, value in row.items() if key != "kind"})
        return {"id": slot, "files": files, "binding": binding,
                "receipt": receipt, "validation": validation, "feedback": feedback, "control": control,
                "source_valid": source_error is None, "complete": complete,
                "errors": errors, "failures": failures, "stagnant": stagnant,
                "notes": slot_notes[slot]}

    def repair_feedback(candidate, extra=None):
        return {"visible_validation": candidate["feedback"],
                "control_errors": candidate["errors"],
                "stagnation_warning": candidate["stagnant"],
                "review": extra}

    def checkpoint_notes(candidate):
        checkpoint = candidate["notes"]
        review_note = review_notes.get(candidate["id"])
        if review_note:
            checkpoint += "\nReviewer checkpoint for this candidate:\n" + review_note
        return checkpoint

    def finish(candidate, completed, reason):
        remaining = (candidate["control"]["remaining"] if candidate.get("control") else requirements)
        failed_requirements = {row["requirement"] for row in candidate["feedback"]["outcomes"]
                               if not row["passed"] and row["requirement"] in requirements}
        abandoned = sorted(set(remaining) | failed_requirements
                           | set(review_remaining.get(candidate["id"], []))) if not completed else []
        # A false completion may claim no remaining work despite untested gaps.
        # Keep the explicit terminal state instead of inventing those unknown IDs.
        result = {"files": candidate["files"], "completed": completed,
                  "status": "complete" if completed else "max_steps_incomplete",
                  "reason": reason, "selected": candidate["id"],
                  "selected_binding": candidate["binding"],
                  "visible_receipt": candidate["receipt"], "visible_validation": candidate["validation"],
                  "stage_protocol": PROTOCOL, "notes": checkpoint_notes(candidate),
                  "attempts": metrics["builder_calls"], "trajectory": trajectory,
                  "metrics": {**metrics, "abandoned_remaining": abandoned,
                              "abandoned_remaining_count": len(abandoned)},
                  "check_history": [*prior_history, *history],
                  "remaining": abandoned}
        emit("stage_finished", stage_index=stage_index, policy=policy,
             selected=result["selected"], completed=completed, reason=reason,
             selected_binding=candidate["binding"], metrics=result["metrics"])
        return result

    if policy in {"strong-single", "cheap-sequential"}:
        model = "strong" if policy == "strong-single" else "cheap"
        current = dict(initial_files)
        feedback = {"resume_notes": prior_notes}
        for round_number in range(1, BASELINE_CALLS + 1):
            result = request("builder", model, current, round_number, "single",
                             feedback, builder_instructions)
            candidate = proposal(result, current, "single", round_number)
            if candidate["complete"]:
                return finish(candidate, True, "Model completion claim passed cumulative visible checks")
            current = candidate["files"]
            feedback = repair_feedback(candidate)
            if round_number < BASELINE_CALLS:
                metrics["repairs"] += 1
        return finish(candidate, False, "Coding-call limit reached without a valid passing completion")

    # Only invocation is concurrent. Immutable initial repository and context are
    # copied separately for every worker; one result cannot influence its peers.
    with ThreadPoolExecutor(max_workers=PORTFOLIO_CANDIDATES) as pool:
        futures = [pool.submit(request, "builder", "cheap", dict(initial_files), 1,
                               f"c{index}", {"resume_notes": prior_notes}, builder_instructions)
                   for index in range(PORTFOLIO_CANDIDATES)]
        initial_results = [future.result() for future in futures]
    prepared = [prepare_proposal(result, initial_files, f"c{index}", 1)
                for index, result in enumerate(initial_results)]
    candidates = {candidate["id"]: candidate for candidate in evaluate_proposals(prepared)}
    reviewer_instructions = (
        "Review all four anonymous implementations against the complete cumulative "
        "specification and the visible execution matrix. Visible tests are limited; "
        "inspect for omitted behavior, regressions and assumptions. Return ONLY "
        "review.json, strict JSON with exactly candidate, action, notes, remaining. "
        "candidate is c0, c1, c2 or c3. action is accept or repair. notes explains "
        "the decision and concrete repair instructions. remaining is a list of "
        "outstanding cumulative requirement IDs. Accept only if this candidate "
        "meets all specified requirements, all cumulative visible checks pass, "
        "and remaining is empty. You cannot modify code or the trusted tests."
    )
    review_errors: list[str] = []

    def fallback():
        # Deterministic visible-only fallback is a repair/carry-forward choice,
        # never independent authorization to finish a milestone.
        return min(candidates.values(), key=lambda item: (
            not item["source_valid"], len(item["failures"]), item["id"]))

    for review_round in range(1, PORTFOLIO_REPAIRS + 2):
        review_files = {f"candidates/{slot}/{path}": content
                        for slot, candidate in candidates.items()
                        for path, content in candidate["files"].items()}
        evidence = {"candidates": {
            slot: {"binding": {"files_sha256": _sha(candidate["files"])},
                   "control": candidate["control"],
                   "visible_validation": candidate["feedback"], "errors": candidate["errors"]}
            for slot, candidate in candidates.items()}, "previous_review_errors": review_errors}
        response = request("reviewer", "strong", review_files, review_round, "portfolio",
                           evidence, reviewer_instructions)
        metrics["reviewer_calls"] += 1
        if response is None:
            metrics["provider_empty_results"] += 1
        changes = getattr(response, "changes", {}) if response is not None else {}
        review, error = _control(changes.get("review.json") if isinstance(changes, dict) else None,
                                 requirements, reviewer=True)
        if not isinstance(changes, dict) or set(changes) != {"review.json"}:
            review, error = None, "Reviewer may return only review.json"
        candidate = candidates[review["candidate"]] if review else fallback()
        accepted = bool(review and review["action"] == "accept"
                        and not review["remaining"] and candidate["feedback"]["passed"]
                        and candidate["source_valid"])
        review_errors = []
        if error:
            metrics["invalid_controls"] += 1
            review_errors.append(error)
        if review:
            notes = review["notes"]
            review_remaining[candidate["id"]] = review["remaining"]
            review_notes[candidate["id"]] = review["notes"]
        if review and review["action"] == "accept" and not accepted:
            metrics["premature_completion"] += 1
            review_errors.append("Acceptance requires valid source, passing checks and no remaining requirements")
        row = {"kind": "reviewer", "round": review_round, "review": review,
               "errors": review_errors, "selected": candidate["id"], "accepted": accepted,
               "fallback_used": review is None, "binding": candidate["binding"]}
        trajectory.append(row)
        emit("stage_review", **{key: value for key, value in row.items() if key != "kind"})
        if accepted:
            return finish(candidate, True, "Strong reviewer accepted a candidate passing cumulative visible checks")
        if review_round > PORTFOLIO_REPAIRS:
            break
        metrics["repairs"] += 1
        notes = checkpoint_notes(candidate)
        result = request("builder", "cheap", candidate["files"], review_round + 1,
                         candidate["id"], repair_feedback(candidate, {
                             "review": review, "errors": review_errors,
                             "fallback_used": review is None}), builder_instructions)
        candidates[candidate["id"]] = proposal(result, candidate["files"],
                                               candidate["id"], review_round + 1)
    return finish(fallback(), False,
                  "Review/repair limit reached; deterministic visible fallback retained without completion")
