"""Independent read-only audit of retained verification-study evidence.

No runner, journal, fixture, snapshot, or candidate code is imported/executed.
Git reads, strict JSON, and a read-only SQLite transaction check consistency.
Retained hashes and process receipts are not external runtime attestations.
"""
from __future__ import annotations

import argparse
import ast
from collections import defaultdict
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import random
import re
import sqlite3

from .gitstore import GitStore


SOURCE_NAMES = frozenset((
    "sustained_experiment.py", "sustained_stage.py", "sustained_checkpoint.py",
    "sustained_queue.py", "sustained_inventory.py", "blackbox_validator.py",
    "sandbox.py", "worker.py", "ledger.py", "gitstore.py", "promotion.py", "pilot.py",
    "verification_experiment.py", "verification_stage.py", "verification_probes.py",
    "verification_journal.py", "verification_integration.py",
    "verification_buildgraph.py", "verification_calendar.py"))
POLICIES = {"strong-reviewed": (1, "strong"), "cheap-reviewed": (1, "cheap"),
            "portfolio-reviewed": (4, "cheap")}
CONTEXT = "verification-context.json"


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=True, allow_nan=False,
                      separators=(",", ":"))


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def journal_digest(value):
    return hashlib.sha256((canonical(value) + "\n").encode()).hexdigest()


def same(left, right):
    """Retained JSON equality must distinguish bool/int/float everywhere."""
    return canonical(left) == canonical(right)


def require(value, message):
    if not value:
        raise ValueError(message)


def _pairs(items):
    result = {}
    for key, value in items:
        require(key not in result, "Duplicate JSON member")
        result[key] = value
    return result


def _constant(value):
    raise ValueError("Nonfinite retained JSON")


def decode(text):
    return json.loads(text, object_pairs_hook=_pairs, parse_constant=_constant)


def load(path):
    return decode(Path(path).read_bytes())


def inside(path, root):
    path = Path(path).resolve()
    require(path.is_relative_to(root), "Evidence path escapes study directory")
    return path


def cases_for(project, stage, kind):
    return [deepcopy(case) for item in project["stages"][:stage + 1]
            for case in item[kind + "_cases"]]


def public_cases(project, stage):
    cases = cases_for(project, stage, "visible")
    for index, case in enumerate(cases):
        case.setdefault("id", f"public-{index}-{digest(case['input'])[:16]}")
    return cases


def bare(case):
    return {key: deepcopy(case[key]) for key in ("input", "expected", "requirement")}


def public_view(cases):
    return [{"id": case["id"], **bare(case)} for case in cases]


def safe_notes(text):
    if not isinstance(text, str) or re.search(
            r"candidate-[abcd]|\b(?:c[0-3]|slot-[0-3])\b", text, re.IGNORECASE):
        return ""
    return text[:8192]


def audit_receipt(receipt, files, cases, contract, label="Validation"):
    require(type(receipt) is dict, label + ": invalid receipt")
    execution_cases = [{key: case[key] for key in ("input", "expected", "id", "requirement") if key in case}
                       for case in cases]
    require(receipt.get("source_sha256") == digest(files)
            and receipt.get("suite_sha256") == digest(execution_cases), label + ": source/suite binding mismatch")
    require(receipt.get("image_id") == contract["image"]
            and receipt.get("timeout_seconds") == contract["suite_timeout_seconds"]
            and receipt.get("case_timeout_seconds") == contract["case_timeout_seconds"]
            and receipt.get("cleanup_verified") is True, label + ": sandbox configuration/cleanup mismatch")
    for flag in ("timed_out", "output_truncated", "input_delivery_failed"):
        require(flag not in receipt or receipt[flag] is False, label + ": contradictory sandbox failure flag")
    require("exit_code" not in receipt or type(receipt["exit_code"]) is int and receipt["exit_code"] == 0,
            label + ": invalid supervisor exit status")
    if "adapter_sha256" in contract:
        require(receipt.get("adapter_sha256") == contract["adapter_sha256"]
                and receipt.get("protocol") == "gossip-blackbox-v1" and receipt.get("schema_version") == 1,
                label + ": receipt uses a different trusted adapter")
    rows = receipt.get("outcomes")
    require(type(rows) is list and len(rows) == len(cases), label + ": incomplete outcome matrix")
    for index, (case, row) in enumerate(zip(cases, rows)):
        require(type(row) is dict and type(row.get("index")) is int and row["index"] == index
                and type(row.get("passed")) is bool, label + ": malformed outcome")
        if row["passed"]:
            require(row.get("status") == "passed" and "actual" in row and digest(row["actual"]) == digest(case["expected"]),
                    label + ": passing result has wrong typed value")
        else:
            require(row.get("status") in {"wrong_answer", "error", "timeout", "output_error", "output_limit", "invalid_output"},
                    label + ": failed outcome has contradictory status")
            if row["status"] == "wrong_answer":
                require("actual" in row and digest(row["actual"]) != digest(case["expected"]),
                        label + ": wrong-answer outcome equals expectation")
    passed = all(row["passed"] for row in rows)
    require(type(receipt.get("passed")) is bool and receipt["passed"] == passed
            and receipt.get("status") == ("passed" if passed else "failed"), label + ": wrong aggregate")
    return {case["id"]: {key: deepcopy(row[key]) for key in
            ("passed", "status", "actual", "error", "stderr") if key in row}
            for case, row in zip(cases, rows)}


def audit_journal(stage_root, invocations, billing, contract, mode, dispatches):
    """Bind every scoped call to one durable response, dispatch and charge."""
    stage_root = Path(stage_root)
    calls = {item["call_id"]: item for item in invocations}
    require(len(calls) == len(invocations), "Duplicate stage invocation")
    requests = stage_root / "requests"
    require({p.name[:-13] for p in requests.glob("*.request.json")} == set(calls)
            and {p.name[:-12] for p in requests.glob("*.result.json")} == set(calls),
            "Saved request/result roster differs from invocations")
    expected_journal_files, bindings, loaded = set(), [], {}
    for call_id, call in calls.items():
        request = load(requests / f"{call_id}.request.json")
        result = load(requests / f"{call_id}.result.json")
        require(same(result, call), "Saved result differs from invocation")
        reservation = billing + "/" + call_id
        require(call["reservation"] == reservation and request["task_id"] == "task-" + digest(reservation)[:24],
                "Call reservation/task identity mismatch")
        require(type(call["usage_units"]) is int and call["usage_units"] >= 0,
                "Unsettled or unknown provider usage")
        stem = hashlib.sha256(call_id.encode()).hexdigest()
        owner = hashlib.sha256(reservation.encode()).hexdigest() + ".reservation.json"
        names = [stem + f".{kind}.json" for kind in ("request", "result", "settled")] + [owner]
        expected_journal_files.update(names)
        stored_request, stored_result, settled, reservation_owner = [load(stage_root / "journal" / name) for name in names]
        identity = dict(schema_version=1, call_id=call_id, reservation_id=reservation,
                        request_sha256=journal_digest(request))
        require(same(reservation_owner, identity) and same(stored_request, {**identity, "request": request}),
                "Journal request/reservation owner binding mismatch")
        kind = "failure" if "failure" in call else "result"
        payload = (dict(message=call["failure"], usage_units=call["usage_units"], metadata=call["metadata"])
                   if kind == "failure" else {key: call[key] for key in ("changes", "summary", "usage_units", "metadata")})
        require(same(stored_result, {**identity, "kind": kind, "payload": payload,
                                  "payload_sha256": journal_digest(payload)}),
                "Journal response/payload mismatch")
        result_bytes = (stage_root / "journal" / names[1]).read_bytes()
        require(same(settled, {**identity, "result_sha256": hashlib.sha256(result_bytes).hexdigest(),
                            "usage_units": call["usage_units"]}), "Journal settlement mismatch")
        require(call_id in dispatches, "Missing provider dispatch")
        dispatch = dispatches[call_id]
        require(dispatch["reservation"] == reservation and dispatch["request_sha256"] == digest(request)
                and dispatch["role"] == call["role"] and dispatch["model"] == call["model"],
                "Dispatch identity/model mismatch")
        profile = contract["models"][call["model"]]
        amount = profile["reservation_units"] if mode == "live" else 0
        if mode == "rehearsal":
            require(call["usage_units"] == 0 and call["metadata"].get("api_calls") == 0,
                    "Rehearsal contains a paid call")
        else:
            require(call["metadata"].get("model") == profile["model"], "Returned model differs from pinned profile")
            tokens = call["metadata"].get("usage")
            require(type(tokens) is dict and all(type(tokens.get(key)) is int and tokens[key] >= 0
                    for key in ("input_tokens", "output_tokens")), "Live call lacks complete token accounting")
            input_tokens, output_tokens = tokens["input_tokens"], tokens["output_tokens"]
            require(output_tokens <= contract["output_tokens"] and input_tokens + output_tokens <= profile["context_tokens"],
                    "Provider token usage exceeds reserved bounds")
            long_context = (profile.get("long_context_above_input_tokens") is not None
                            and input_tokens > profile["long_context_above_input_tokens"])
            input_rate = profile["long_input_micro_usd_per_million"] if long_context else profile["input_micro_usd_per_million"]
            output_rate = profile["long_output_micro_usd_per_million"] if long_context else profile["output_micro_usd_per_million"]
            expected_cost = (input_tokens * input_rate + output_tokens * output_rate + 999999) // 1000000
            require(call["usage_units"] == expected_cost, "Paid usage differs from frozen pricing")
        bindings.append(dict(reservation=reservation, billing=billing, usage=call["usage_units"],
                             amount=amount, response_id=call["metadata"].get("response_id")))
        loaded[call_id] = (request, call)
    require({p.name for p in (stage_root / "journal").glob("*.json")} == expected_journal_files,
            "Unknown/orphan journal request, result, settlement or reservation")
    require(set(dispatches) == set(calls), "Unlinked provider dispatch")
    return loaded, bindings


def _prompts(source):
    """Read only literal prompt expressions from the frozen Python AST."""
    result = {}
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Assign) or len(node.targets) != 1 or not isinstance(node.targets[0], ast.Name):
            continue
        name = node.targets[0].id
        if name not in {"builder_prompt", "reviewer_prompt"}:
            continue
        if isinstance(node.value, ast.JoinedStr):
            parts = []
            for part in node.value.values:
                if isinstance(part, ast.Constant) and isinstance(part.value, str):
                    parts.append(part.value)
                else:
                    require(isinstance(part, ast.FormattedValue) and isinstance(part.value, ast.Name)
                            and part.value.id == "CONTEXT_PATH" and part.format_spec is None,
                            "Unrecognized frozen prompt expression")
                    parts.append(CONTEXT)
            result[name] = "".join(parts)
        else:
            result[name] = ast.literal_eval(node.value)
    require(set(result) == {"builder_prompt", "reviewer_prompt"}, "Frozen prompts missing")
    return result


def _adapter_hash(source):
    constants = {}
    for node in ast.parse(source).body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            if node.targets[0].id in {"CHILD_ADAPTER", "SUPERVISOR_ADAPTER"}:
                constants[node.targets[0].id] = ast.literal_eval(node.value)
    require(set(constants) == {"CHILD_ADAPTER", "SUPERVISOR_ADAPTER"}
            and all(type(value) is str for value in constants.values()), "Frozen adapter constants are missing")
    return digest({"child.py": constants["CHILD_ADAPTER"], "supervisor.py": constants["SUPERVISOR_ADAPTER"]})


def _review(call, requirements, aliases):
    try:
        changes = call.get("changes", {})
        if set(changes) != {"review.json"} or len(changes["review.json"].encode()) > 65536:
            return None
        value = decode(changes["review.json"])
        if (type(value) is not dict or set(value) != {"candidate", "action", "notes", "remaining", "probes"}
                or value["candidate"] not in aliases or value["action"] not in {"accept", "repair", "inspect"}
                or type(value["notes"]) is not str or len(value["notes"]) > 8192
                or type(value["remaining"]) is not list
                or any(type(item) is not str or item not in requirements for item in value["remaining"])
                or len(set(value["remaining"])) != len(value["remaining"])
                or type(value["probes"]) is not list or len(value["probes"]) > 4):
            return None
        return value
    except (ValueError, TypeError, KeyError, AttributeError, UnicodeError, RecursionError):
        return None


def _builder_note(changes, requirements):
    try:
        text = (changes or {}).get("notes.json")
        if not isinstance(text, str) or len(text.encode()) > 65536:
            return None
        value = decode(text)
        if (type(value) is not dict or set(value) != {"notes", "remaining"}
                or type(value["notes"]) is not str or len(value["notes"]) > 8192
                or type(value["remaining"]) is not list
                or any(type(item) is not str or item not in requirements for item in value["remaining"])
                or len(set(value["remaining"])) != len(value["remaining"])):
            return None
        return value
    except (ValueError, TypeError, AttributeError, UnicodeError, RecursionError):
        return None


def _probe_binding(case):
    require(case.get("validation_policy") == "verification-fixture-oracle-v1"
            and case.get("input_sha256") == digest(case["input"])
            and case.get("proposal_sha256") == digest(bare(case))
            and case.get("id") == case["namespace"] + ":" + digest(case["input"])
            and case.get("validation_sha256") == digest({key: value for key, value in case.items()
                                                         if key != "validation_sha256"}),
            "Generated probe provenance binding mismatch")


def audit_stage(stage, project, stage_index, policy, initial, prior, calls, contract,
                stage_root, git_files, prompts, repetition):
    """Reconstruct candidate state and decision gates from frozen raw artifacts."""
    allowed = project["allowed_paths"]
    requirements = list(dict.fromkeys(item for part in project["stages"][:stage_index + 1]
                                     for item in part["requirements"]))
    public = public_cases(project, stage_index)
    active, pool, candidates = list(public), [], {}
    initial_count, builder_model = POLICIES[policy]
    seed = int(digest([project["id"], repetition, stage_index])[:16], 16)
    aliases = ["candidate-A", "candidate-B", "candidate-C", "candidate-D"]
    random.Random(digest([project["id"], stage_index, seed, "candidate-aliases"])).shuffle(aliases)
    alias_map = {f"slot-{index}": aliases[index] for index in range(initial_count)}
    order = list(alias_map.values())
    require(stage["alias_map"] == alias_map and stage["candidate_order"] == order, "Candidate anonymization/order changed")
    gates = iter(stage["probe_receipts"])
    previous_decision, new_count, reviews, builders, repairs = None, 0, 0, 0, 0
    used_calls = set()
    metrics = dict(builder_calls=0, reviewer_calls=0, repairs=0, invalid_reviews=0,
        invalid_builder_notes=0, invalid_source_proposals=0, blocked_acceptances=0,
        admitted_new_probes=0, generated_probe_attempts=0, rejected_probes=0, retired_probes=0,
        probe_failures_found=0, probe_discriminating_cases=0, acceptances_without_new_probes=0)

    def gate(proposals, mode, origin):
        nonlocal new_count
        record = next(gates, None)
        require(record is not None and record["mode"] == mode and record["origin"] == origin,
                "Missing or reordered probe gate")
        allowance = 8 - new_count if mode == "new" else len(proposals)
        require(same(record["envelope"], dict(mode=mode, probes=proposals, existing_cases=active, max_new=allowance)),
                "Probe gate input/evidence/cap mismatch")
        receipt = record["receipt"]
        require(receipt.get("oracle_assisted") is True and receipt.get("stage_index") == stage_index
                and receipt.get("mode") == mode, "Invalid probe receipt")
        admitted = receipt["admitted_cases"]
        require(type(admitted) is list and len(admitted) <= allowance, "Probe allowance exceeded")
        by_input = defaultdict(list)
        for item in proposals:
            if type(item) is dict and {"input", "expected", "requirement"} <= set(item):
                by_input[digest(item["input"])].append(item)
        seen = {digest(item["input"]) for item in active} if mode == "new" else set()
        for item in admitted:
            _probe_binding(item)
            identity = digest(item["input"])
            require(identity not in seen and item["requirement"] in requirements
                    and any(digest(bare(item)) == digest(bare(original)) for original in by_input[identity])
                    and item["validated_stage_index"] == stage_index, "Probe gate changed a label or injected/duplicated a case")
            if mode == "new":
                require(item["origin"] == origin and item["admitted_stage_index"] == stage_index,
                        "New probe origin/stage mismatch")
            else:
                require(any(item["id"] == old["id"] and item["origin"] == old["origin"] for old in by_input[identity]),
                        "Revalidated probe identity/origin changed")
            seen.add(identity)
        if mode == "new":
            require(receipt["proposed_count"] == len(proposals) and receipt["max_new"] == allowance
                    and receipt["content_sha256"] == hashlib.sha256(json.dumps({"probes": proposals}).encode()).hexdigest(),
                    "Probe receipt is not bound to the submitted raw batch")
            new_count += len(admitted)
            metrics["generated_probe_attempts"] += len(proposals)
        else:
            retired = receipt["retired"]
            require({item["id"] for item in admitted} | {item["id"] for item in retired} == {item["id"] for item in proposals}
                    and len(admitted) + len(retired) == len(proposals)
                    and all(item.get("reason") == "contract_changed" and "expected" not in item and "actual" not in item
                            for item in retired), "Revalidation did not account for old probes without corrected labels")
        metrics["rejected_probes"] += len(receipt["rejected"])
        return admitted

    old_probes = prior.get("probe_pool", [])
    if old_probes:
        retained = gate(old_probes, "revalidate", dict(project_id=project["id"], stage_index=stage_index, source="retained_probes"))
        public_inputs = {digest(item["input"]): item for item in public}
        for item in retained:
            equivalent = public_inputs.get(digest(item["input"]))
            if equivalent:
                require(digest(item["expected"]) == digest(equivalent["expected"]), "Probe/public label disagreement")
            else:
                pool.append(item)
        active.extend(pool)
        metrics["retired_probes"] = len(old_probes) - len(pool)
    context_base = dict(project_id=project["id"], project_title=project["title"], stage_index=stage_index,
        milestones=[dict(index=index, specification=part.get("specification", part.get("spec")), requirements=part["requirements"])
                    for index, part in enumerate(project["stages"][:stage_index + 1])],
        prior_notes=safe_notes(prior.get("notes", "")), prior_remaining=sorted(set(prior.get("remaining", []))),
        prior_machine_history=prior.get("check_history", []))

    def request(call_id, files, context_extra, role, attempt, feedback):
        require(call_id not in used_calls and call_id in calls, "Repeated or missing stage request")
        used_calls.add(call_id)
        req, result = calls[call_id]
        expected_files = {**files, CONTEXT: canonical({**context_base, "verified_cases": public_view(active), **context_extra})}
        require(req["files"] == expected_files and req["attempt"] == attempt
                and req["instructions"] == prompts[role + "_prompt"]
                and req["feedback"] == feedback
                and req["allowed_paths"] == (["review.json"] if role == "reviewer" else [*allowed, "notes.json"]),
                "Model request source/context/prompt/feedback differs from public evidence")
        require(result["role"] == role and result["model"] == ("strong" if role == "reviewer" else builder_model),
                "Call role/model violates policy")
        return result

    def evaluate(item, cases, label):
        path = stage_root / "evaluations" / (digest(dict(source=item["files"], cases=cases)) + ".json")
        receipt = load(path)
        matrix = audit_receipt(receipt, item["files"], cases, contract)
        item["matrix"].update(matrix)
        item["validation_receipts"].append(dict(label=label, case_ids=[case["id"] for case in cases], receipt=receipt))

    for index, row in enumerate(stage["trajectory"]):
        require(type(row.get("round")) is int and row["round"] > 0, "Invalid trajectory round")
        if row["kind"] == "builder":
            slot, alias = row["slot"], row["candidate"]
            require(alias_map.get(slot) == alias, "Builder slot/alias mismatch")
            if builders < initial_count:
                require(slot == f"slot-{builders}" and row["round"] == 1 and reviews == 0, "Initial builders are not independent first attempts")
                previous, feedback = initial, {"checkpoint_notes": context_base["prior_notes"]}
            else:
                require(previous_decision and previous_decision["review"]
                        and index > 0 and stage["trajectory"][index - 1]["kind"] == "reviewer"
                        and previous_decision["review"]["action"] == "repair"
                        and previous_decision["review"]["candidate"] == alias
                        and not previous_decision["accepted"] and reviews < 6 and repairs < 4,
                        "Builder repair lacks reviewer authorization")
                repairs += 1
                require(row["round"] == repairs + 1, "Repair round mismatch")
                previous = candidates[alias]["files"]
                feedback = dict(review_notes=previous_decision["review"]["notes"], remaining=previous_decision["review"]["remaining"],
                    evidence=[dict(case=case, outcome=candidates[alias]["matrix"][case["id"]]) for case in public_view(active)])
            result = request(f"builder-{slot}-{row['round']}", previous, {}, "builder", row["round"], canonical(feedback))
            changes = result.get("changes")
            valid = (type(changes) is dict and all(path in [*allowed, "notes.json"] for path in changes)
                     and all(isinstance(value, str) for path, value in changes.items() if path in allowed))
            files = dict(previous)
            if valid:
                files.update({path: value for path, value in changes.items() if path in allowed})
            note = _builder_note(changes, requirements)
            prior_note = candidates.get(alias, {}).get("notes", context_base["prior_notes"])
            metrics["invalid_builder_notes"] += int(note is None)
            metrics["invalid_source_proposals"] += int(not valid)
            require(row["source_valid"] is valid and row["files_sha256"] == digest(files)
                    and row["notes_valid"] is (note is not None)
                    and git_files(row["binding"]) == files, "Builder proposal/source binding mismatch")
            label = f"stage-{stage_index}-build-{slot}-{row['round']}"
            retained = load(stage_root / (label + ".json"))
            require(retained == dict(binding=row["binding"], files=files), "Retained proposal record mismatch")
            item = dict(files=files, binding=row["binding"], slot=slot, alias=alias, source_valid=valid,
                        notes=safe_notes(note["notes"]) if note and valid else prior_note,
                        remaining=note["remaining"] if note else [], matrix={}, validation_receipts=[])
            candidates[alias] = item
            evaluate(item, active, label + "-all-evidence")
            failures = [case["id"] for case in active if not item["matrix"][case["id"]]["passed"]]
            require(row["visible_passed"] == (not failures) and row["failures"] == failures,
                    "Builder failure summary differs from receipts")
            builders += 1
        elif row["kind"] == "reviewer":
            if previous_decision and index > 0 and stage["trajectory"][index - 1]["kind"] == "reviewer":
                require(not previous_decision["review"] or previous_decision["review"]["action"] != "repair"
                        or repairs >= 4, "Authorized repair was skipped")
            reviews += 1
            require(builders >= initial_count and row["round"] == reviews and reviews <= 6,
                    "Reviewer order/limit mismatch")
            matrix = {alias: {case["id"]: item["matrix"][case["id"]] for case in active} for alias, item in candidates.items()}
            files = {f"candidates/{alias}/{path}": text for alias, item in candidates.items() for path, text in item["files"].items()}
            extra = dict(candidate_source_hashes={alias: digest(item["files"]) for alias, item in candidates.items()},
                         candidate_eligibility={alias: dict(source_valid=item["source_valid"]) for alias, item in candidates.items()},
                         matrix=matrix, previous_decision=previous_decision, new_probe_capacity_remaining=8 - new_count)
            result = request(f"reviewer-review-{reviews}", files, extra, "reviewer", reviews, "")
            review = _review(result, requirements, candidates)
            require(row["review"] == review and row["evidence_before_sha256"] == digest(active), "Review artifact/evidence mismatch")
            admitted = []
            errors = []
            if review:
                admitted = gate(review["probes"], "new", dict(project_id=project["id"], stage_index=stage_index,
                                                             source="reviewer", review_round=reviews))
                if admitted:
                    pool.extend(admitted)
                    active.extend(admitted)
                    for alias in order:
                        evaluate(candidates[alias], admitted, f"stage-{stage_index}-review-{reviews}-{alias}-new-probes")
                    for new_case in admitted:
                        values = [candidates[alias]["matrix"][new_case["id"]]["passed"] for alias in order]
                        metrics["probe_failures_found"] += sum(not value for value in values)
                        metrics["probe_discriminating_cases"] += int(len(set(values)) > 1)
                item = candidates[review["candidate"]]
                require(row["selected"] == review["candidate"] and row["binding"] == item["binding"], "Reviewer selected stale source")
                accepted = (review["action"] == "accept" and not review["remaining"] and item["source_valid"]
                            and all(item["matrix"][case["id"]]["passed"] for case in active))
                if review["action"] == "accept":
                    if not accepted:
                        metrics["blocked_acceptances"] += 1
                        errors.append("Acceptance denied: source, remaining requirements, or verified checks are not passing")
                    elif not new_count:
                        metrics["acceptances_without_new_probes"] += 1
                elif review["action"] == "repair" and (repairs >= 4 or reviews == 6):
                    errors.append("Repair denied: no repair opportunity or subsequent review remains")
            else:
                accepted = False
                metrics["invalid_reviews"] += 1
                errors.append("Invalid review.json; no source change or acceptance authorized")
            require(row["admitted_probe_ids"] == [case["id"] for case in admitted]
                    and row["accepted"] is accepted and row["errors"] == errors
                    and row["evidence_after_sha256"] == digest(active),
                    "Reviewer acceptance does not follow new-probe executions")
            require(not accepted or index == len(stage["trajectory"]) - 1, "Stage continued after acceptance")
            previous_decision = {key: deepcopy(row[key]) for key in ("round", "review", "accepted", "errors", "admitted_probe_ids")}
        else:
            raise ValueError("Unknown trajectory event")
    require(next(gates, None) is None and set(calls) == used_calls, "Unlinked probe gate or provider call")
    require(initial_count <= builders <= initial_count + 4 and 1 <= reviews <= 6
            and stage["trajectory"][-1]["kind"] == "reviewer", "Stage call bounds/order mismatch")
    completed = previous_decision["accepted"]
    selected = (previous_decision["review"]["candidate"] if completed else min(order, key=lambda alias:
                (not candidates[alias]["source_valid"], sum(not candidates[alias]["matrix"][case["id"]]["passed"] for case in active), order.index(alias))))
    require(completed or reviews == 6, "Incomplete stage stopped before its review bound")
    item = candidates[selected]
    require(stage["completed"] is completed and stage["selected"] == selected
            and stage["files"] == item["files"] and stage["selected_binding"] == item["binding"]
            and stage["status"] == ("complete" if completed else "bounded_incomplete")
            and stage["notes"] == safe_notes(item["notes"])
            and stage["remaining"] == ([] if completed else sorted({case["requirement"] for case in active
                if not item["matrix"][case["id"]]["passed"]} | set(item["remaining"]))),
            "Stage selected source differs from final review/fallback")
    require(stage["probe_pool"] == pool and stage["evidence_cases"] == active
            and stage["evidence_sha256"] == digest(active), "Final active evidence differs from accumulated gates")
    require(set(stage["candidates"]) == set(candidates), "Retained final candidate roster mismatch")
    for alias, candidate in candidates.items():
        saved = stage["candidates"][alias]
        for key in ("binding", "slot", "alias", "source_valid", "notes", "remaining", "matrix", "validation_receipts"):
            require(same(saved[key], candidate[key]), "Latest candidate matrix/receipt binding mismatch")
    require(same(stage["matrix"], {alias: item["matrix"] for alias, item in candidates.items()}), "Stage matrix mismatch")
    selected_item = candidates[selected]
    expected_history = prior.get("check_history", []) + [dict(stage_index=stage_index, completed=completed,
        source_sha256=digest(selected_item["files"]), evidence_sha256=digest(active),
        outcomes=[dict(case_id=case["id"], requirement=case["requirement"],
            passed=selected_item["matrix"][case["id"]]["passed"]) for case in active])]
    require(same(stage["check_history"], expected_history), "Durable machine history mismatch")
    metrics.update(builder_calls=builders, reviewer_calls=reviews, repairs=repairs, admitted_new_probes=new_count)
    require(same(stage["metrics"], metrics) and stage["attempts"] == builders, "Stage metric mismatch")
    return dict(builders=builders, reviewers=reviews, probes=new_count, completed=completed)


def audit_integration(receipt, initial_files, current_files, updates, root, git_files, *, initial_binding, current_binding):
    require(receipt["component"] == "trusted-upstream-policy-integration"
            and receipt["status"] == "accepted" and receipt["prepare_status"] == "prepared"
            and receipt["old_evidence_prepare_status"] == "prepared" and receipt["api_calls"] == 0
            and receipt["validation_kind"] == "exact-text-tree-comparison-no-code-execution"
            and receipt["stale_probe_status"] == "stale" and receipt["source_unchanged"] is True,
            "Trusted integration/stale rejection failed")
    expected = {**current_files, **updates}
    require(inside(receipt["initial_store"], root) == inside(initial_binding["store_path"], root)
            and receipt["initial_head"] == initial_binding["tip_sha"]
            and inside(receipt["current_store"], root) == inside(current_binding["store_path"], root)
            and receipt["current_head"] == current_binding["tip_sha"],
            "Integration does not descend from the actual prior checkpoint")
    for field, files in (("initial_files_sha256", initial_files), ("current_files_sha256", current_files),
                         ("updates_sha256", updates), ("expected_files_sha256", expected), ("merged_files_sha256", expected)):
        require(receipt[field] == digest(files), "Policy integration tree/hash mismatch")
    require(receipt["incoming_changed_paths"] == ["policy.json"]
            and receipt["exact_tested_sha"] == receipt["merged_head"] == receipt["stale_probe_head"],
            "Policy integration changed unexpected paths or published stale evidence")
    for field, head_field, files in (("initial_store", "initial_head", initial_files),
                                    ("current_store", "current_head", current_files),
                                    ("destination", "merged_head", expected)):
        path = inside(receipt[field], root)
        store = GitStore(path)
        require(store.head() == receipt[head_field]
                and git_files(dict(store_path=str(path), tip_sha=receipt[head_field])) == files,
                "Policy integration Git source/accepted ref mismatch")
    store = GitStore(inside(receipt["destination"], root))
    parents = store._git("rev-list", "--parents", "-n", "1", receipt["merged_head"]).split()[1:]
    require(parents == receipt["merge_parents"] == [receipt["current_head"], receipt["incoming_tip"]]
            and store.is_ancestor(receipt["current_head"], receipt["merged_head"])
            and store.is_ancestor(receipt["incoming_tip"], receipt["merged_head"]),
            "Policy merge does not preserve both histories")
    incoming_parents = store._git("rev-list", "--parents", "-n", "1", receipt["incoming_tip"]).split()[1:]
    changed = store._git("diff", "--name-only", "-z", receipt["initial_head"], receipt["incoming_tip"]).rstrip("\0").split("\0")
    require(incoming_parents == [receipt["initial_head"]] and changed == ["policy.json"]
            and git_files(dict(store_path=str(store.path), tip_sha=receipt["incoming_tip"])) == {**initial_files, **updates},
            "Incoming policy branch has the wrong base, source or scope")
    require(git_files(dict(store_path=str(store.path), tip_sha=receipt["old_evidence_candidate_sha"])) == current_files,
            "Stale evidence candidate is not bound to the previous source")


def audit_fault(root, case, state, contract_sha, initial_store, stage_zero_head, stage_one_calls, traces, *, expected_amount=None):
    marker = load(root / "fault-ready.json")
    supervisor = load(root / "fault-supervisor.json")
    resume = load(root / "resume.json")
    require(same(supervisor, case["fault"]) and same(resume, case["resume"])
            and supervisor["marker_sha256"] == digest(marker) and supervisor["returncode"] == -9
            and type(marker["pid"]) is int and marker["pid"] > 0
            and supervisor["previous_pid"] == marker["pid"] == resume["previous_pid"]
            and type(resume["resumed_pid"]) is int and resume["resumed_pid"] > 0
            and resume["resumed_pid"] != marker["pid"] and resume["verified"] is True
            and marker["other_unsettled"] == 0 and marker["stage_index"] == resume["stage_index"] == 1,
            "Forced-kill/resume evidence mismatch")
    call_id = "reviewer-review-1"
    request, response = stage_one_calls[call_id]
    require(marker["call_id"] == call_id and marker["reservation"] == response["reservation"]
            and marker["request_sha256"] == digest(request), "Killed response belongs to another request")
    stem = hashlib.sha256(call_id.encode()).hexdigest()
    for kind in ("request", "result", "settled"):
        require(inside(marker["journal_paths"][kind], root) == root / "stage-1" / "journal" / f"{stem}.{kind}.json",
                "Fault marker points to a different journal")
    result_path = root / "stage-1" / "journal" / f"{stem}.result.json"
    reservation = supervisor["reservation_at_kill"]
    require(supervisor["saved_result_sha256"] == hashlib.sha256(result_path.read_bytes()).hexdigest()
            and reservation["id"] == marker["reservation"] and reservation["state"] == "reserved"
            and reservation["spent"] is None and type(reservation["amount"]) is int
            and reservation["amount"] >= response["usage_units"]
            and (expected_amount is None or reservation["amount"] == expected_amount)
            and reservation["task_id"] == marker["reservation"].rsplit("/", 1)[0],
            "Fault did not occur at a saved-response, unsettled-reservation boundary")
    checkpoint = dict(contract_sha=contract_sha, next_stage=1, stages=state["stages"][:1],
                      files=state["stages"][0]["files"], store_path=str(root / "stage-0" / "checkpoint.git"), head=stage_zero_head)
    require(resume["initial_head"] == stage_zero_head and resume["checkpoint_sha256"] == digest(checkpoint),
            "Resume did not reconstruct the pre-kill durable checkpoint")
    before = traces.get(marker["pid"], [])
    after = traces.get(resume["resumed_pid"], [])
    require(any(row["kind"] == "provider_dispatch" and row["stage_index"] == 1 and row["call_id"] == call_id for row in before)
            and not any(row["kind"] == "provider_dispatch" and row["stage_index"] == 1 and row["call_id"] == call_id for row in after)
            and any(row["kind"] == "verification_review" and row["stage_index"] == 1 and row["round"] == 1 for row in after),
            "Durable reviewer response was not replayed without another dispatch")


def audit_accounting(path, bindings, billing_heads, namespaces, report, *, check_delta=True, study_namespace=None):
    path = Path(path).resolve()
    require(path.is_file(), "Accounting ledger is missing")
    require(len({item["reservation"] for item in bindings}) == len(bindings), "Reservation reused across journals")
    response_ids = [item["response_id"] for item in bindings if item["response_id"]]
    require(len(response_ids) == len(set(response_ids)), "Provider response ID reused for distinct requests")
    with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as db:
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA query_only=ON")
        db.execute("BEGIN")
        for item in bindings:
            row = db.execute("SELECT * FROM reservations WHERE id=?", (item["reservation"],)).fetchone()
            task = db.execute("SELECT * FROM tasks WHERE id=?", (item["billing"],)).fetchone()
            require(row is not None and task is not None and row["task_id"] == item["billing"]
                    and row["state"] == "settled" and type(row["spent"]) is int
                    and type(row["amount"]) is int and row["spent"] == item["usage"]
                    and row["amount"] == item["amount"] and 0 <= row["spent"] <= row["amount"]
                    and type(row["epoch"]) is int and 1 <= row["epoch"] <= task["epoch"],
                    "Usage is not exactly bound to a settled reservation")
        for billing, head in billing_heads:
            row = db.execute("SELECT * FROM tasks WHERE id=?", (billing,)).fetchone()
            require(row is not None and row["status"] == "complete" and row["accepted_commit"] == head
                    and row["intent_id"] is None, "Stage accounting promotion is incomplete or mismatched")
        for namespace in namespaces:
            prefix = namespace + "/"
            observed = {row[0] for row in db.execute("SELECT id FROM reservations WHERE substr(task_id,1,?)=?", (len(prefix), prefix))}
            expected = {item["reservation"] for item in bindings if item["billing"].startswith(prefix)}
            require(observed == expected, "Unexplained/orphan study reservation")
            observed_tasks = {row[0] for row in db.execute("SELECT id FROM tasks WHERE substr(id,1,?)=?", (len(prefix), prefix))}
            require(observed_tasks == {billing for billing, _ in billing_heads if billing.startswith(prefix)},
                    "Unexplained or unfinished study billing task")
        if study_namespace is not None:
            prefix = study_namespace + "/"
            observed = {row[0] for row in db.execute("SELECT id FROM reservations WHERE substr(task_id,1,?)=?", (len(prefix), prefix))}
            observed_tasks = {row[0] for row in db.execute("SELECT id FROM tasks WHERE substr(id,1,?)=?", (len(prefix), prefix))}
            require(observed == {item["reservation"] for item in bindings}
                    and observed_tasks == {billing for billing, _ in billing_heads},
                    "Unknown case or orphan charge in the complete study namespace")
        unsettled = db.execute("SELECT count(*) FROM reservations WHERE spent IS NULL OR state!='settled'").fetchone()[0]
        pending = db.execute("SELECT count(*) FROM intents WHERE state='pending'").fetchone()[0]
        committed = db.execute("SELECT coalesce(sum(coalesce(spent,amount)),0) FROM reservations").fetchone()[0]
        cap = db.execute("SELECT value FROM settings WHERE key='budget'").fetchone()[0]
    usage = sum(item["usage"] for item in bindings)
    if check_delta:
        require(usage == report["incremental_micro_usd"]
                == report["budget"]["spent_or_reserved"] - report["budget_before"]["spent_or_reserved"],
                "Study budget delta differs from unique settled requests")
    require(committed >= report["budget"]["spent_or_reserved"], "Ledger lost previously recorded spending")
    return dict(ledger=str(path), study_usage_micro_usd=usage, current_spent_or_reserved=committed,
                current_limit=cap, unrelated_or_current_unsettled_reservations=unsettled,
                unrelated_or_current_pending_promotions=pending)


def _audit_run(run, accounting_ledger=None, finalized_only=False):
    run = Path(run).resolve()
    if run.is_file():
        require(run.name == "results.json", "Expected study directory or results.json")
        run = run.parent
    result_sha = hashlib.sha256((run / "results.json").read_bytes()).hexdigest()
    auditor_sha = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    report = load(run / "results.json")
    finished = (report.get("status") == "finished" and not report.get("unexecuted")
                and not report.get("active_case") and not report.get("censored"))
    require(report["experiment"] == "verification-quality-v1" and (finished or finalized_only)
            and report.get("mode") in {"live", "rehearsal"},
            "Only a fully finalized study can receive a passing independent audit")
    contract = report["contract"]
    prereg = load(run / "preregistered.json")
    require(report["contract_sha"] == digest(contract) and same(prereg["contract"], contract)
            and same(prereg["budget_before"], report["budget_before"]), "Preregistered contract/budget mismatch")
    require(set(contract["sources"]) == SOURCE_NAMES and "verification_audit.py" not in contract["sources"],
            "Frozen source roster is incomplete or includes the independent auditor")
    for name, expected in contract["sources"].items():
        require(hashlib.sha256((run / "source-snapshot" / "gossip_harness" / name).read_bytes()).hexdigest() == expected,
                "Frozen source snapshot mismatch: " + name)
    require(hashlib.sha256((run / "study-plan.json").read_bytes()).hexdigest() == contract["plan_sha256"], "Frozen plan hash mismatch")
    fixtures = load(run / "fixtures.json")["projects"]
    require(digest(fixtures) == contract["fixture_sha256"], "Frozen fixture binding mismatch")
    projects = {project["id"]: project for project in fixtures}
    require(len(projects) == len(fixtures) and set(projects) == set(contract["project_ids"])
            and set(contract["policies"]) == set(POLICIES) and contract["milestones"] == 4
            and contract["repair_calls"] == 4 and contract["reviewer_calls"] == 6
            and contract["probes_per_review"] == 4 and contract["new_probes_per_stage"] == 8
            and contract["initial_candidates"] == {key: value[0] for key, value in POLICIES.items()}
            and contract["hidden_feedback"] is False, "Study policy/fixture manifest mismatch")
    expected_roster = {(project, policy, rep) for project in projects for policy in POLICIES for rep in range(report["repetitions"])}
    roster = [tuple(item) for item in prereg["roster"]]
    actual = [(case["project_id"], case["policy"], case["repetition"]) for case in report["cases"]]
    require(len(roster) == len(set(roster)) and set(roster) == expected_roster
            and actual == (roster if finished else roster[:len(actual)]),
            "Final study roster/order differs from preregistration")
    if finished:
        require({path.parent for path in run.glob("*/config.json")} == {run / case["run_id"] for case in report["cases"]},
                "Unlinked project execution directory")
    if not finished:
        remaining = [tuple(item) for item in report["unexecuted"]]
        for item in ([report["active_case"]] if report.get("active_case") else []) + report.get("censored", []):
            remaining.append((item["project_id"], item["policy"], item["repetition"]))
        require(len(remaining) == len(set(remaining)) and not set(actual) & set(remaining)
                and set(actual) | set(remaining) == expected_roster,
                "Partial finalized/active/censored/unexecuted roster mismatch")
    prompts = _prompts((run / "source-snapshot" / "gossip_harness" / "verification_stage.py").read_text())
    validation_contract = {**contract, "adapter_sha256": _adapter_hash(
        (run / "source-snapshot" / "gossip_harness" / "blackbox_validator.py").read_text())}
    cache, all_bindings, billing_heads, ledger_paths, namespaces, audits = {}, [], [], set(), [], []

    def git_files(binding):
        path = inside(binding["store_path"], run)
        key = (str(path), binding["tip_sha"])
        if key not in cache:
            cache[key] = GitStore(path).read_files(binding["tip_sha"])
        files = cache[key]
        if "files_sha256" in binding:
            require(binding["files_sha256"] == digest(files), "Git source hash mismatch")
        return files

    for case in report["cases"]:
        root = inside(case["root"], run)
        config = load(root / "config.json")
        require(root == run / case["run_id"]
                and case["run_id"] == f"{case['project_id']}-{case['policy']}-{case['repetition']}"
                and same(load(root / "result.json"), case)
                and config["root"] == str(root) and config["contract_sha"] == report["contract_sha"]
                and config["image"] == contract["image"] and config["live"] is (report["mode"] == "live")
                and all(config[key] == case[key] for key in ("project_id", "policy", "repetition", "run_id")),
                "Case/config/root result mismatch")
        expected_namespace = "verification-quality-v1/" + digest(str(run))[:16] + "/" + case["run_id"]
        require(config["namespace"] == expected_namespace, "Accounting namespace is not bound to study path")
        namespaces.append(config["namespace"])
        ledger_paths.add(str(Path(config["budget_ledger"]).resolve()))
        project = projects[case["project_id"]]
        state = load(root / "trajectory.json")
        require(hashlib.sha256((root / "trajectory.json").read_bytes()).hexdigest() == case["trajectory_sha256"]
                and state["contract_sha"] == report["contract_sha"]
                and 1 <= len(state["stages"]) <= 4 and state["files"] == state["stages"][-1]["files"]
                and digest(state["files"]) == case["files_sha256"], "Frozen trajectory/source mismatch")
        require({path.name for path in root.glob("stage-*") if path.is_dir()}
                == {f"stage-{index}" for index in range(len(state["stages"]))},
                "Unlinked reached-stage artifact directory")
        if len(state["stages"]) < 4:
            require(not case["integration"], "Unreached policy integration has fabricated evidence")
        initial_store = GitStore(root / "initial.git")
        require(initial_store.read_files() == project["initial_files"], "Initial Git tree differs from fixture")
        previous_files, previous_store = project["initial_files"], initial_store
        durable_store = initial_store
        traces, dispatches = {}, defaultdict(dict)
        for path in root.glob("trace-*.jsonl"):
            pid = int(path.stem.removeprefix("trace-"))
            rows = [decode(line) for line in path.read_text().splitlines() if line.strip()]
            require([row["index"] for row in rows] == list(range(1, len(rows) + 1)), "Trace event sequence mismatch")
            traces[pid] = rows
            for row in rows:
                if row["kind"] == "provider_dispatch":
                    require(row["call_id"] not in dispatches[row["stage_index"]], "Duplicate local provider dispatch")
                    dispatches[row["stage_index"]][row["call_id"]] = row
        case_calls, stage_audits, stage_one_calls, stage_zero_head = [], [], None, None
        for index, stage in enumerate(state["stages"]):
            stage_root = root / f"stage-{index}"
            require(same(load(stage_root / "result.json"), stage) and stage["stage_index"] == index,
                    "Stage result/trajectory mismatch")
            require(index == 0 or state["stages"][index - 1]["completed"] is True, "Continued after incomplete milestone")
            updates = project["stages"][index].get("trusted_updates", {})
            if updates:
                integration = load(stage_root / "integration.json")
                require(integration == load(stage_root / "upstream.git.integration" / "receipt.json"), "Integration receipt copies differ")
                audit_integration(integration, project["initial_files"], previous_files, updates, root, git_files,
                    initial_binding=dict(store_path=str(initial_store.path), tip_sha=initial_store.head()),
                    current_binding=dict(store_path=str(previous_store.path), tip_sha=previous_store.head()))
                previous_store = GitStore(stage_root / "upstream.git")
                previous_files = {**previous_files, **updates}
                require(case["integration"] == integration, "Final integration summary mismatch")
            billing = config["namespace"] + f"/stage-{index}"
            loaded, bindings = audit_journal(stage_root, stage["invocations"], billing, contract, report["mode"], dispatches[index])
            require(all(request["base_sha"] == previous_store.head() for request, _ in loaded.values()), "Request base differs from stage Git checkpoint")
            all_bindings.extend(bindings)
            case_calls.extend(stage["invocations"])
            prior = state["stages"][index - 1] if index else {}
            stage_audits.append(audit_stage(stage, project, index, case["policy"], previous_files, prior,
                loaded, validation_contract, stage_root, git_files, prompts, case["repetition"]))
            if index == 1:
                stage_one_calls = loaded
            record_store = GitStore(stage_root / "record.git")
            record = decode(record_store.read_files()["record.json"])
            require(record == {key: stage[key] for key in ("completed", "status", "selected_binding")},
                    "Stage accounting record differs from selection")
            billing_heads.append((billing, record_store.head()))
            if stage["completed"]:
                checkpoint = GitStore(stage_root / "checkpoint.git")
                require(checkpoint.read_files() == stage["files"]
                        and checkpoint.is_ancestor(previous_store.head(), checkpoint.head())
                        and checkpoint.is_ancestor(stage["selected_binding"]["tip_sha"], checkpoint.head()),
                        "Checkpoint lost selected tree or previous history")
                previous_store = checkpoint
                durable_store = checkpoint
                if index == 0:
                    stage_zero_head = checkpoint.head()
            previous_files = stage["files"]
        require(set(dispatches) == set(range(len(state["stages"]))) and same(case_calls, case["invocations"]),
                "Project has unlinked stage dispatches or calls")
        require(sum(call["usage_units"] for call in case_calls) == case["usage_micro_usd"], "Case usage total mismatch")
        for key, value in case["metrics"].items():
            require(value == sum(stage["metrics"].get(key, 0) for stage in state["stages"]), "Case metric total mismatch")
        completed = sum(stage["completed"] for stage in state["stages"])
        require(completed == case["milestones_completed"] == state["next_stage"], "Completed milestone count mismatch")
        saved_state = load(root / "state.json")
        require(hashlib.sha256((root / "state.json").read_bytes()).hexdigest() == load(root / "state-binding.json")["sha256"]
                and saved_state["stages"] == state["stages"][:completed]
                and saved_state["next_stage"] == completed and saved_state["contract_sha"] == report["contract_sha"]
                and saved_state["head"] == durable_store.head()
                and inside(saved_state["store_path"], root) == durable_store.path
                and saved_state["files"] == durable_store.read_files()
                and state["head"] == durable_store.head()
                and inside(state["store_path"], root) == durable_store.path,
                "Durable checkpoint binding mismatch")
        if len(state["stages"]) >= 2:
            audit_fault(root, case, state, report["contract_sha"], initial_store, stage_zero_head, stage_one_calls, traces,
                        expected_amount=contract["models"]["strong"]["reservation_units"] if report["mode"] == "live" else 0)
        else:
            require(not case["fault"] and not case["resume"], "Unreached fault has fabricated recovery evidence")
        final_cases = cases_for(project, 3, "visible") + (state["stages"][-1]["probe_pool"] if len(state["stages"]) == 4 else [])
        hidden = cases_for(project, 3, "hidden")
        require(same(load(root / "final-visible-receipt.json"), case["final_visible"])
                and same(load(root / "final-private-receipt.json"), case["final_hidden"]), "Final raw receipt differs from report")
        audit_receipt(case["final_visible"], state["files"], final_cases, validation_contract, "Final public/generated")
        audit_receipt(case["final_hidden"], state["files"], hidden, validation_contract, "Final private")
        accepted = completed == 4 and case["final_visible"]["passed"] and case["final_hidden"]["passed"]
        coverage = {requirement: all(row["passed"] for item, row in zip(hidden, case["final_hidden"]["outcomes"])
                                    if item["requirement"] == requirement) for requirement in sorted({item["requirement"] for item in hidden})}
        expected_status = "accepted" if accepted else "final_quality_failed" if completed == 4 else "max_steps_incomplete"
        require(case["accepted"] is accepted and same(case["requirement_coverage"], coverage)
                and case["status"] == expected_status
                and state["terminal"] == ("visible_complete" if completed == 4 else "max_steps_incomplete"),
                "Primary quality calculation/status mismatch")
        if accepted:
            release = GitStore(root / "release.git")
            exported = {}
            for path in (root / "accepted").rglob("*"):
                require(not path.is_symlink(), "Accepted export contains a symlink")
                if path.is_file():
                    exported[path.relative_to(root / "accepted").as_posix()] = path.read_text()
            require(release.head() == case["release_head"] and release.read_files() == state["files"] == exported
                    and release.is_ancestor(previous_store.head(), release.head()), "Release is not the exact tested final tree/history")
        else:
            require(case["release_head"] is None and not (root / "release.git").exists()
                    and not (root / "accepted").exists(), "Failed project has a published release")
        audits.append(dict(run_id=case["run_id"], accepted=accepted, stages=stage_audits,
                           requests=len(case_calls), usage_micro_usd=case["usage_micro_usd"],
                           fault_verified=len(state["stages"]) >= 2,
                           private_failed_case_ids=[item["id"] for item, row in zip(hidden, case["final_hidden"]["outcomes"]) if not row["passed"]]))
    require(len(ledger_paths) == 1, "Study uses inconsistent accounting ledgers")
    ledger = Path(accounting_ledger) if accounting_ledger else Path(next(iter(ledger_paths)))
    accounting = audit_accounting(ledger, all_bindings, billing_heads, namespaces, report, check_delta=finished,
        study_namespace="verification-quality-v1/" + digest(str(run))[:16] if finished else None)
    require(hashlib.sha256((run / "results.json").read_bytes()).hexdigest() == result_sha
            and hashlib.sha256(Path(__file__).read_bytes()).hexdigest() == auditor_sha,
            "Study results or auditor changed during verification")
    return dict(audit_version=1, passed=finished, certified=finished, finalized_cases_passed=True,
        run=str(run), status=report["status"],
        auditor_source_sha256=auditor_sha,
        contract_sha256=report["contract_sha"], source_snapshot_verified=True, fixtures_verified=True,
        verified_requests=len(all_bindings), verified_billing_tasks=len(billing_heads), cases=audits,
        accounting=accounting,
        unaudited_work=dict(active_case=report.get("active_case"), censored=report.get("censored", []), unexecuted=report["unexecuted"]),
        limitations=[
            "Finalized-only mode does not certify active/censored work or reconcile their charges to the final project records; passed remains false until the complete study is audited.",
            "Hashes and receipts establish retained-evidence consistency, not external attestation of execution, fsync or SIGKILL.",
            "The auditor never executes candidate, fixture, snapshot or oracle code; generated-probe semantic eligibility rests on the recorded trusted oracle gate.",
            "One local provider dispatch and immutable settled charge are verified; provider-side exactly-once execution is not established.",
            "Recovery is from a durably saved response before settlement, not an unknown in-flight provider outcome.",
            "Policy integration checks a trusted merge and stale-evidence gate; it does not demonstrate autonomous textual-conflict resolution.",
            "Frozen pricing checks token-accounting estimates, not provider invoices; shared-ledger activity after this run is reported separately.",
            "Operational breadth also changes cheap-call opportunities, reviewer context and candidate-aware test mining; this is not an equal-compute isolated-diversity estimate."])


def audit_run(run, *, accounting_ledger=None, finalized_only=False):
    try:
        return _audit_run(run, accounting_ledger, finalized_only)
    except ValueError:
        raise
    except (OSError, KeyError, IndexError, TypeError, AttributeError, RuntimeError, sqlite3.Error, StopIteration) as error:
        raise ValueError("Retained evidence is incomplete, malformed or unreadable: " + str(error)) from error


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--accounting-ledger", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--finalized-only", action="store_true", help="Audit finalized cases of a partial study without certifying the whole run")
    args = parser.parse_args()
    result = audit_run(args.run, accounting_ledger=args.accounting_ledger, finalized_only=args.finalized_only)
    text = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        with args.output.open("x") as stream:
            stream.write(text)
    print(text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
