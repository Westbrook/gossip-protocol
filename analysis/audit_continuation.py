"""Read-only consistency audit for the four-trajectory transport pilot.

No candidate, fixture, runner or frozen source is imported or executed. Reuses
the earlier independent auditor's strict JSON, Docker-receipt and ledger checks.
This is retained-evidence verification, not external runtime attestation.
"""
from __future__ import annotations

import argparse
import ast
from contextvars import ContextVar
from copy import deepcopy
from datetime import datetime
import hashlib
import json
from pathlib import Path
import random

from gossip_harness.gitstore import GitStore
from gossip_harness.verification_audit import (
    SOURCE_NAMES, _adapter_hash, _probe_binding, _prompts, _review,
    audit_accounting, audit_receipt, bare, canonical, cases_for, decode,
    digest, inside, journal_digest, public_cases, public_view, require, same, safe_notes,
)

PROTOCOL = "continuation-transport-v1"
POLICIES = ["strong-maintainer", "scout-assisted"]
LIMITS = dict(max_reviews=6, max_repairs=4, max_new_probes=8,
              max_escalations=2, stagnation_reviews=2)
SOURCES = SOURCE_NAMES | {"continuation_experiment.py", "continuation_stage.py",
                          "continuation_transport.py", "transport.py"}
_READ_SET = ContextVar("continuation_audit_reads", default=None)


class EvidenceReads:
    """Hash the very bytes decoded and reject changing retained evidence."""
    def __init__(self):
        self.hashes = {}

    def capture(self, path, raw):
        path = Path(path).resolve()
        value = hashlib.sha256(raw).hexdigest()
        require(path not in self.hashes or self.hashes[path] == value, "Consumed evidence changed during audit: " + str(path))
        self.hashes[path] = value
        return value

    def verify_unchanged(self):
        for path, expected in self.hashes.items():
            require(path.is_file() and hashlib.sha256(path.read_bytes()).hexdigest() == expected,
                    "Consumed evidence changed during audit: " + str(path))


def read_bytes(path):
    raw = Path(path).read_bytes()
    reads = _READ_SET.get()
    if reads is not None:
        reads.capture(path, raw)
    return raw


def load(path):
    return decode(read_bytes(path))


def sha(path):
    return hashlib.sha256(read_bytes(path)).hexdigest()


def execution(cases):
    return [{key: case[key] for key in ("input", "expected", "id", "requirement") if key in case}
            for case in cases]


def audit_timings(document, freeze, run_ids):
    rows = document.get("spans")
    require(type(rows) is list and len({row["span_id"] for row in rows}) == len(rows),
            "Duplicate or missing timing spans")
    spans = {}
    for row in rows:
        require(type(row.get("span_id")) is int and row["span_id"] >= 0
                and row.get("clock_id") == document.get("clock_id")
                and row.get("status") in {"finished", "failed"}, "Unclosed or foreign-clock span")
        start, end = row.get("started_monotonic_ns"), row.get("finished_monotonic_ns")
        elapsed = row.get("elapsed_seconds")
        require(type(start) is int and type(end) is int and 0 <= start <= end
                and type(elapsed) in {int, float} and elapsed == (end - start) / 1e9,
                "Timing duration is not its monotonic interval")
        require(datetime.fromisoformat(row["started_utc"]).utcoffset() is not None
                and datetime.fromisoformat(row["finished_utc"]).utcoffset() is not None,
                "Timing UTC values lack timezone")
        if "run_id" in row:
            require(row["run_id"] in run_ids, "Timing span names an unknown trajectory")
        spans[row["span_id"]] = row
    barrier = freeze.get("frozen_monotonic_ns")
    require(type(barrier) is int and barrier >= 0, "Freeze lacks monotonic barrier")
    for row in rows:
        if row["kind"] in {"worker_request", "physical_provider_request", "scripted_worker", "stage", "model_trajectory", "scout_batch"}:
            require(row["finished_monotonic_ns"] <= barrier, "Provider-driven work continued after freeze")
        if row["kind"] == "docker_validation" and row.get("purpose") in {"final_private", "private"}:
            require(row["started_monotonic_ns"] >= barrier, "Private evaluation preceded global freeze")
    return spans


def audit_calls(root, calls, billing, contract, mode, spans, run_id, stage_index):
    """Each completed invocation has exactly one response, settlement and dispatch."""
    require(len({row["call_id"] for row in calls}) == len(calls), "Duplicate invocation")
    ids = {row["call_id"] for row in calls}
    requests = root / "requests"
    require({p.name[:-13] for p in requests.glob("*.request.json")} == ids
            and {p.name[:-12] for p in requests.glob("*.result.json")} == ids, "Orphan raw request/result")
    loaded, bindings, journal_files, span_ids = {}, [], set(), set()
    for call in calls:
        call_id = call["call_id"]
        request = load(requests / f"{call_id}.request.json")
        require(same(load(requests / f"{call_id}.result.json"), call), "Invocation differs from raw result")
        reservation = billing + "/" + call_id
        require(call["reservation"] == reservation and request["task_id"] == "task-" + digest([digest(contract), reservation])[:24]
                and call["request_sha256"] == digest(request), "Request/reservation identity differs")
        stem = hashlib.sha256(call_id.encode()).hexdigest()
        names = [stem + f".{kind}.json" for kind in ("request", "result", "settled")]
        names.append(hashlib.sha256(reservation.encode()).hexdigest() + ".reservation.json")
        journal_files.update(names)
        stored_request, response, settled, owner = [load(root / "journal" / name) for name in names]
        identity = dict(schema_version=1, call_id=call_id, reservation_id=reservation, request_sha256=journal_digest(request))
        require(same(owner, identity) and same(stored_request, {**identity, "request": request}), "Journal request owner differs")
        failed = "failure" in call
        payload = (dict(message=call["failure"], usage_units=call["usage_units"], metadata=call["metadata"])
                   if failed else {key: call[key] for key in ("changes", "summary", "usage_units", "metadata")})
        require(same(response, {**identity, "kind": "failure" if failed else "result", "payload": payload,
                               "payload_sha256": journal_digest(payload)}), "Journal response differs")
        result_sha = sha(root / "journal" / names[1])
        require(call["journal_result_sha256"] == result_sha
                and same(settled, {**identity, "result_sha256": result_sha, "usage_units": call["usage_units"]}),
                "Journal settlement differs")
        for kind, name in zip(("request", "result", "settled"), names):
            require(Path(call["journal_paths"][kind]).resolve() == root / "journal" / name, "Journal path changed")
        usage = call["usage_units"]
        require(type(usage) is int and usage >= 0 and call["outcome"] == ("failure" if failed else "result"),
                "Unknown or malformed call outcome")
        require(call["model"] in {"cheap", "strong"} and call["role"] in {"scout", "builder", "reviewer"},
                "Unknown provider role/model")
        profile = contract["models"][call["model"]]
        if mode == "rehearsal":
            require(usage == 0 and call["metadata"].get("api_calls") == 0
                    and call["physical_dispatch"] is False and call["journal_replay"] is False, "Paid rehearsal call")
        else:
            meta, tokens = call["metadata"], call["metadata"].get("usage")
            require(meta.get("model") == profile["model"] and type(tokens) is dict
                    and all(type(tokens.get(key)) is int and tokens[key] >= 0 for key in ("input_tokens", "output_tokens")),
                    "Live response lacks complete model/token accounting")
            n, out = tokens["input_tokens"], tokens["output_tokens"]
            require(out <= contract["output_tokens"] and n + out <= profile["context_tokens"], "Usage exceeds reserved context")
            long = profile.get("long_context_above_input_tokens") is not None and n > profile["long_context_above_input_tokens"]
            rate_in = profile["long_input_micro_usd_per_million"] if long else profile["input_micro_usd_per_million"]
            rate_out = profile["long_output_micro_usd_per_million"] if long else profile["output_micro_usd_per_million"]
            require(usage == (n * rate_in + out * rate_out + 999999) // 1000000, "Usage differs from frozen pricing")
            require(type(call["physical_dispatch"]) is bool and type(call["journal_replay"]) is bool
                    and call["physical_dispatch"] != call["journal_replay"], "Physical/replay labels conflict")
        wrapper = spans[call["request_span_id"]]
        expected_labels = dict(run_id=run_id, stage_index=stage_index, call_id=call_id, role=call["role"], model=call["model"])
        require(wrapper["kind"] == "worker_request" and all(wrapper.get(k) == v for k, v in expected_labels.items()),
                "Request span labels differ")
        physical = [row for row in spans.values() if row["kind"] in {"physical_provider_request", "scripted_worker"}
                    and all(row.get(k) == v for k, v in expected_labels.items())]
        require(len(physical) == 1, "Missing or duplicate physical/scripted dispatch")
        dispatch = physical[0]
        require(dispatch["kind"] == ("physical_provider_request" if mode == "live" else "scripted_worker")
                and dispatch["span_id"] == call["physical_span_id"]
                and wrapper["started_monotonic_ns"] <= dispatch["started_monotonic_ns"]
                <= dispatch["finished_monotonic_ns"] <= wrapper["finished_monotonic_ns"], "Dispatch interval differs from wrapper")
        span_ids.update((wrapper["span_id"], dispatch["span_id"]))
        bindings.append(dict(reservation=reservation, billing=billing, usage=usage,
            amount=profile["reservation_units"] if mode == "live" else 0, response_id=call["metadata"].get("response_id")))
        loaded[call_id] = (request, call)
    require({p.name for p in (root / "journal").glob("*.json")} == journal_files, "Orphan journal evidence")
    observed = {row["span_id"] for row in spans.values() if row.get("run_id") == run_id
                and row.get("stage_index") == stage_index
                and row["kind"] in {"worker_request", "physical_provider_request", "scripted_worker"}}
    require(observed == span_ids, "Unlinked request or dispatch span")
    return loaded, bindings


def audit_stage(stage, project, index, policy, initial, prior, calls, contract, root, git_files, spans, run_id, prompts=None):
    """Reconstruct the one-source eligibility chain and explicit acceptance gate."""
    require(stage["protocol"] == "continuation-stage-v1" and same(stage["limits"], LIMITS), "Changed engine or limits")
    require(type(stage["completed"]) is bool and stage["stage_index"] == index
            and len(stage["candidate_order"]) == 1
            and set(stage["alias_map"]) == {"slot-0"}, "Changed stage or candidate count")
    alias = stage["alias_map"]["slot-0"]
    require(stage["candidate_order"] == [alias] and stage["selected"] == alias, "Selected an unknown source")
    reqs = list(dict.fromkeys(r for part in project["stages"][:index + 1] for r in part["requirements"]))
    public = public_cases(project, index)
    evidence = stage["evidence_cases"]
    require(same(evidence, public + stage["probe_pool"]) and stage["evidence_sha256"] == digest(evidence)
            and len({c["id"] for c in evidence}) == len(evidence), "Stage evidence changed")
    for case in stage["probe_pool"]:
        _probe_binding(case)
    admitted_all, reviewer_admitted = [], 0
    for gate in stage["probe_receipts"]:
        receipt, envelope = gate["receipt"], gate["envelope"]
        require(receipt.get("oracle_assisted") is True and receipt["stage_index"] == index
                and receipt["mode"] == gate["mode"] and envelope["mode"] == gate["mode"], "Invalid probe gate")
        proposals = envelope["probes"]
        admitted = receipt["admitted_cases"]
        require(len(admitted) <= envelope["max_new"], "Probe admission exceeds remaining cap")
        for case in admitted:
            _probe_binding(case)
            require(case["requirement"] in reqs and any(same(bare(case), bare(p)) for p in proposals
                    if type(p) is dict and {"input", "expected", "requirement"} <= set(p)), "Corrected or injected probe label")
        if gate["mode"] == "new":
            reviewer_admitted += len(admitted)
            require(len(proposals) <= 4 and envelope["max_new"] == 8 - (reviewer_admitted - len(admitted)), "Reviewer probe cap changed")
        admitted_all.extend(admitted)
    require(reviewer_admitted <= 8 and all(any(same(case, admitted) for admitted in admitted_all)
            for case in stage["probe_pool"]), "Unadmitted active probe")

    scouts = stage["scouts"]
    expected_scouts = 2 if policy == "scout-assisted" else 0
    require(scouts["calls"] == expected_scouts, "Scout policy changed")
    scout_calls = [(request, call) for request, call in calls.values() if call["role"] == "scout"]
    require(len(scout_calls) == expected_scouts, "Missing or extra scout charge")
    if expected_scouts:
        require(same(load(root / "scouts.json"), scouts) and scouts["source_sha256"] == digest(initial), "Scout source binding changed")
        contexts = []
        for request, call in scout_calls:
            require(call["model"] == "cheap" and request["allowed_paths"] == ["probes.json"]
                    and request["feedback"] == "", "Scout can edit source or uses wrong model")
            files = dict(request["files"])
            contexts.append(decode(files.pop("scout-context.json")))
            require(same(files, initial) and digest(request["files"]) == scouts["shared_input_sha256"], "Scout inputs differ or include peer data")
        expected_context = dict(project_id=project["id"], stage_index=index,
            milestones=[{key: item[key] for key in ("requirements", "specification", "spec") if key in item}
                        for item in project["stages"][:index + 1]], public_cases=cases_for(project, index, "visible"))
        require(all(same(context, expected_context) for context in contexts), "Scout context includes nonpublic material")
        require(scouts["admitted"] == sum(len(r["admitted_cases"]) for r in scouts["receipts"]) <= 8
                and scouts["rejected"] == sum(len(r["rejected"]) for r in scouts["receipts"]), "Scout counts differ")
        for scout_index, receipt in enumerate(scouts["receipts"]):
            require(same(load(root / f"scout-{scout_index}-admission.json"), receipt), "Scout admission differs")
            request, call = calls[f"scout-scout-{scout_index}-1"]
            try:
                proposals = decode(call.get("changes", {}).get("probes.json", ""))["probes"]
            except (ValueError, TypeError, KeyError):
                proposals = []
            for case in receipt["admitted_cases"]:
                _probe_binding(case)
                require(any(same(bare(case), p) for p in proposals), "Scout gate corrected a label")
    else:
        require(scouts == dict(calls=0, admitted=0, rejected=0, receipts=[]), "Control arm acquired scouts")

    valid = stage["baseline_approval"]["source_valid"]
    require(valid is True and stage["baseline_approval"]["files_sha256"] == digest(initial), "Invalid baseline approval")
    current, builder_count, reviewer_count, accepted = dict(initial), 0, 0, False
    seen_calls, reviewer_remaining = {call["call_id"] for _, call in scout_calls}, []
    latest_attempt_status = None
    by_suite = execution_suites(stage, calls, project, index)
    def physical_matrix(files, cases, call_id):
        before = spans[calls[call_id][1]["request_span_id"]]["started_monotonic_ns"]
        matrix = {}
        for timed in sorted(spans.values(), key=lambda s: s["finished_monotonic_ns"]):
            if (timed["kind"] != "docker_validation" or timed.get("run_id") != run_id
                    or timed.get("stage_index") != index or timed["finished_monotonic_ns"] > before):
                continue
            path = inside(timed["receipt_path"], root)
            require(sha(path) == timed["receipt_sha256"], "Prior physical receipt changed")
            receipt = load(path)
            if receipt["source_sha256"] != digest(files):
                continue
            require(receipt["suite_sha256"] in by_suite, "Prior execution uses unknown evidence")
            matrix.update(audit_receipt(receipt, files, by_suite[receipt["suite_sha256"]], contract))
        require(all(case["id"] in matrix for case in cases), "Prompt evidence lacks prior physical execution")
        return {case["id"]: matrix[case["id"]] for case in cases}
    context_keys = {"project_id", "project_title", "stage_index", "milestones", "prior_notes", "prior_remaining", "prior_machine_history", "verified_cases"}
    reviewer_keys = {"candidate_source_hashes", "candidate_eligibility", "matrix", "previous_decision", "new_probe_capacity_remaining", "continuation"}
    for event in stage["trajectory"]:
        kind = event["kind"]
        if kind not in {"builder", "reviewer"}:
            require(kind in {"progress", "escalation"}, "Unknown controller event")
            continue
        call_id = f"builder-slot-0-{event['round']}" if kind == "builder" else f"reviewer-review-{event['round']}"
        request, call = calls[call_id]
        seen_calls.add(call_id)
        require(call["role"] == kind and call["model"] == "strong", "Both arms must use the same strong maintainer/reviewer")
        files = dict(request["files"])
        context = decode(files.pop("verification-context.json"))
        require(set(context) == context_keys | (reviewer_keys if kind == "reviewer" else set())
                and context["project_id"] == project["id"] and context["project_title"] == project["title"]
                and context["stage_index"] == index
                and same(context["milestones"], [dict(index=i, specification=part.get("specification", part.get("spec")), requirements=part["requirements"])
                                                 for i, part in enumerate(project["stages"][:index + 1])]), "Provider context includes nonpublic or altered contract")
        require(context["prior_notes"] == safe_notes(prior.get("notes", ""))
                and same(context["prior_remaining"], sorted(set(prior.get("remaining", []))))
                and same(context["prior_machine_history"], prior.get("check_history", [])), "Provider checkpoint context changed")
        if prompts is not None:
            allowed_prompts = [prompts[kind + "_prompt"]]
            if kind == "reviewer":
                allowed_prompts.append(prompts["reviewer_prompt"] + prompts["escalation_suffix"])
            require(request["instructions"] in allowed_prompts, "Provider instructions differ from frozen public prompt")
        require(all(any(same(case, known) for known in public_view(evidence)) for case in context["verified_cases"]),
                "Provider context contains unadmitted/private evidence")
        if kind == "builder":
            builder_count += 1
            require(same(files, current) and request["allowed_paths"] == project["allowed_paths"] + ["notes.json"], "Builder input/scope changed")
            feedback = decode(request["feedback"])
            require(type(feedback) is dict and set(feedback) <= {"checkpoint_notes", "review_notes", "remaining", "evidence", "continuation_escalation"}, "Builder feedback contains unknown material")
            for item in feedback.get("evidence", []):
                require(any(same(item["case"], known) for known in public_view(evidence)), "Builder feedback contains private evidence")
            if feedback.get("evidence"):
                expected = physical_matrix(current, [item["case"] for item in feedback["evidence"]], call_id)
                require(all(same(item["outcome"], expected[item["case"]["id"]]) for item in feedback["evidence"]),
                        "Builder feedback outcome differs from prior execution")
            changes = call.get("changes")
            admissible = type(changes) is dict and all(path in project["allowed_paths"] + ["notes.json"] for path in changes) and all(
                type(value) is str for path, value in changes.items() if path in project["allowed_paths"])
            next_files = {**current, **{p: v for p, v in changes.items() if p in project["allowed_paths"]}} if admissible else current
            changed = not same(next_files, current)
            next_valid = bool(admissible and changed) or valid
            require(event["prior_source_valid"] is valid and event["source_valid"] is next_valid
                    and event["attempt_admissible"] is admissible and event["effective_source_change"] is changed
                    and event["previous_files_sha256"] == digest(current)
                    and event["files_sha256"] == digest(next_files)
                    and same(git_files(event["binding"]), next_files), "Builder source or no-op eligibility chain differs")
            current, valid = next_files, next_valid
            latest_attempt_status = "effective_source_proposal" if changed else "no_effective_source_change" if admissible else "rejected_source_proposal"
        else:
            reviewer_count += 1
            require(same(files, {f"candidates/{alias}/{p}": text for p, text in current.items()})
                    and request["allowed_paths"] == ["review.json"] and request["feedback"] == ""
                    and same(context["candidate_source_hashes"], {alias: digest(current)})
                    and same(context["continuation"]["limits"], LIMITS), "Reviewer source/controller changed")
            expected_matrix = physical_matrix(current, context["verified_cases"], call_id)
            require(same(context["candidate_eligibility"], {alias: dict(source_valid=valid, latest_attempt_status=latest_attempt_status)})
                    and same(context["matrix"], {alias: expected_matrix}),
                    "Reviewer matrix or eligibility differs from prior physical evidence")
            review = _review(call, reqs, [alias])
            require(same(review, event["review"]), "Saved decision differs from raw reviewer response")
            if review:
                reviewer_remaining = review["remaining"]
            require(type(event["accepted"]) is bool, "Acceptance must be a boolean")
            if event["accepted"]:
                require(not accepted and review and review["action"] == "accept" and not review["remaining"] and valid
                        and same(event["binding"], stage["selected_binding"]), "Automatic or unsupported stage acceptance")
                accepted = True
    require(seen_calls == set(calls) and builder_count <= 5 and reviewer_count <= 6
            and builder_count == stage["metrics"]["builder_calls"] and reviewer_count == stage["metrics"]["reviewer_calls"]
            and stage["metrics"]["repairs"] == builder_count - 1
            and len(stage["escalations"]) == stage["metrics"]["escalations"] <= 2, "Calls/caps/metrics differ")
    require(same(current, stage["files"]) and same(git_files(stage["selected_binding"]), current)
            and stage["source_valid"] is valid and stage["completed"] is accepted, "Selected source or completion differs")
    candidate = stage["candidates"][alias]
    matrix = {}
    for item in candidate["validation_receipts"]:
        cases = [next(case for case in evidence if case["id"] == case_id) for case_id in item["case_ids"]]
        matrix.update(audit_receipt(item["receipt"], current, cases, contract))
    require(same(matrix, stage["matrix"][alias]) and set(matrix) == {case["id"] for case in evidence}, "Final stage matrix differs")
    require(not accepted or valid and all(row["passed"] for row in matrix.values()) and stage["remaining"] == [], "Accepted stage fails active evidence")
    if not accepted:
        remaining = {case["requirement"] for case in evidence if not matrix[case["id"]]["passed"]} | set(candidate["remaining"]) | set(reviewer_remaining)
        require(stage["remaining"] == sorted(remaining), "Incomplete remaining requirements differ")
    return dict(builders=builder_count, reviewers=reviewer_count, scouts=expected_scouts, completed=accepted)


def execution_suites(stage, calls, project, index):
    suites = [public_cases(project, index), stage["evidence_cases"]]
    for request, call in calls.values():
        if call["role"] != "scout":
            suites.append(decode(request["files"]["verification-context.json"])["verified_cases"])
    suites.extend(gate["receipt"]["admitted_cases"] for gate in stage["probe_receipts"] if gate["mode"] == "new")
    return {digest(execution(cases)): cases for cases in suites}


def audit_evaluations(stage, calls, project, index, contract, root, git_files, spans, run_id):
    """Bind every physical stage validation to a known source and admitted suite."""
    sources = {digest(git_files(row["binding"])): git_files(row["binding"])
               for row in stage["trajectory"] if row["kind"] == "builder"}
    by_digest = execution_suites(stage, calls, project, index)
    used, raw_receipts = set(), set()
    for span in spans.values():
        if span["kind"] != "docker_validation" or span.get("run_id") != run_id or span.get("stage_index") != index:
            continue
        require(span.get("purpose") == "stage_evidence" and span["status"] == "finished", "Unknown or failed physical validation")
        path = inside(span["receipt_path"], root)
        require(path.parent == root / "evaluations" and sha(path) == span["receipt_sha256"], "Stage raw receipt/path changed")
        receipt = load(path)
        require(receipt["source_sha256"] in sources and receipt["suite_sha256"] in by_digest, "Validation source or evidence is unknown")
        cases = by_digest[receipt["suite_sha256"]]
        require(span["case_count"] == len(cases), "Validation timing case count changed")
        audit_receipt(receipt, sources[receipt["source_sha256"]], cases, contract, "Stage raw validation")
        used.add(path)
        raw_receipts.add(digest(receipt))
    require(set((root / "evaluations").glob("*.json")) == used, "Unlinked physical validation receipt")
    for candidate in stage["candidates"].values():
        require(all(digest(item["receipt"]) in raw_receipts for item in candidate["validation_receipts"]), "Candidate receipt lacks physical execution")


def _audit_run(run, *, accounting_ledger=None):
    run = Path(run).resolve()
    report = load(run / "results.json")
    require(report.get("experiment") == PROTOCOL and report.get("status") == "finished"
            and report.get("phase") == "finished" and report.get("mode") in {"live", "rehearsal"}
            and not report.get("unexecuted") and not report.get("censored") and report.get("active_case") is None,
            "Only a fully finalized four-trajectory study can be certified")
    contract, prereg = report["contract"], load(run / "preregistered.json")
    require(report["contract_sha"] == digest(contract) and same(prereg["contract"], contract)
            and same(prereg["budget_before"], report["budget_before"]), "Preregistered contract/budget changed")
    require(set(contract["sources"]) == SOURCES and contract["protocol"] == PROTOCOL
            and contract["policies"] == POLICIES and contract["repetitions"] == 2 and contract["milestones"] == 2
            and contract["hidden_feedback"] is False and contract["controller_crash_injection"] is False
            and same(contract["limits"], LIMITS), "Study/controller manifest changed")
    for name, expected in contract["sources"].items():
        require(sha(run / "source-snapshot" / "gossip_harness" / name) == expected, "Frozen source changed: " + name)
    plan = load(run / "study-plan.json")
    require(sha(run / "study-plan.json") == contract["plan_sha256"] and plan["protocol"] == PROTOCOL
            and plan["common_controller"]["engine_policy"] == "strong-reviewed"
            and plan["policies"] == POLICIES and same(plan["budget"], contract["budget"])
            and same(plan["scouts"], contract["scouts"]), "Frozen plan/budget/policy changed")
    require(same(prereg.get("rehearsal"), report.get("rehearsal")), "Rehearsal binding differs from preregistration")
    if report["mode"] == "live":
        proof = report["rehearsal"]
        proof_path = Path(proof["results_path"]).resolve()
        proof_report = load(proof_path)
        require(sha(proof_path) == proof["results_sha256"] and proof["contract_sha"] == digest(contract)
                and same(proof_report["contract"], contract) and proof_report["contract_sha"] == digest(contract)
                and proof_report["mode"] == "rehearsal" and proof_report["status"] == "finished"
                and len(proof_report["cases"]) == 4 and all(c["accepted"] is True for c in proof_report["cases"])
                and proof_report["incremental_micro_usd"] == 0 and not proof_report.get("censored")
                and not proof_report.get("unexecuted") and proof_report.get("active_case") is None,
                "Exact completed zero-API rehearsal binding changed")
    else:
        require(report.get("rehearsal") is None, "Rehearsal unexpectedly references another proof")
    project, = load(run / "fixtures.json")["projects"]
    require(digest(project) == contract["fixture_sha256"] and project["id"] == contract["project_id"]
            and len(project["stages"]) == 2
            and hashlib.sha256(project["initial_files"]["gossip_harness/transport.py"].encode()).hexdigest() == contract["baseline_sha256"],
            "Frozen fixture/baseline changed")
    roster = [[project["id"], policy, rep] for rep in range(2) for policy in POLICIES]
    random.Random(plan["roster_seed"]).shuffle(roster)
    require(same(contract["roster"], roster) and same(prereg["roster"], roster)
            and [[c["project_id"], c["policy"], c["repetition"]] for c in report["cases"]] == roster, "Roster/order changed")
    namespace = PROTOCOL + "/" + digest([str(run), digest(contract)])[:24]
    require(report["namespace"] == prereg["namespace"] == namespace
            and report["incremental_commitment_cap"] == prereg["incremental_commitment_cap"] == contract["budget"]["incremental_cap_micro_usd"],
            "Study accounting namespace/cap changed")
    freeze = load(run / "frozen-trajectories.json")
    freeze_sha = sha(run / "frozen-trajectories.json")
    run_ids = [f"{p}-{policy}-{rep}" for p, policy, rep in roster]
    require(report["freeze_manifest_sha256"] == freeze_sha and freeze["protocol"] == PROTOCOL
            and freeze["contract_sha"] == digest(contract) and freeze["private_evaluation_started"] is False
            and [r["run_id"] for r in freeze["trajectories"]] == run_ids, "Four-source freeze manifest changed")
    spans = audit_timings(load(run / "timings.json"), freeze, run_ids)
    require([row["run_id"] for row in report["trajectories"]] == run_ids, "Trajectory timing roster differs")
    for row, frozen in zip(report["trajectories"], freeze["trajectories"]):
        timed = spans[row["span_id"]]
        require(timed["kind"] == "model_trajectory" and timed["run_id"] == row["run_id"]
                and timed["status"] == "finished" and same(row["elapsed_seconds"], timed["elapsed_seconds"])
                and row["terminal"] == frozen["terminal"] and row["trajectory_sha256"] == frozen["trajectory_sha256"],
                "Trajectory elapsed/source summary differs from physical span")
    validation_contract = {**contract, "adapter_sha256": _adapter_hash(read_bytes(run / "source-snapshot" / "gossip_harness" / "blackbox_validator.py").decode())}
    stage_source = read_bytes(run / "source-snapshot" / "gossip_harness" / "continuation_stage.py").decode()
    prompts = _prompts(stage_source)
    prompts["escalation_suffix"], = [node.value for node in ast.walk(ast.parse(stage_source))
                                    if isinstance(node, ast.Constant) and isinstance(node.value, str)
                                    and node.value.startswith(" This is a bounded continuation review")]
    require({p.parent.name for p in run.glob("*/config.json")} == set(run_ids), "Orphan trajectory directory")
    cache, all_bindings, billing_heads, audits, commitments, actual_stages = {}, [], [], [], [], set()
    def git_files(binding):
        path = inside(binding["store_path"], run)
        key = (str(path), binding["tip_sha"])
        if key not in cache:
            cache[key] = GitStore(path).read_files(binding["tip_sha"])
        require(binding.get("files_sha256", digest(cache[key])) == digest(cache[key]), "Git files digest changed")
        return cache[key]
    for case, frozen in zip(report["cases"], freeze["trajectories"]):
        root = run / case["run_id"]
        config, state = load(root / "config.json"), load(root / "trajectory.json")
        require(same(load(root / "result.json"), case) and case["root"] == str(root)
                and config["root"] == str(root) and config["contract_sha"] == state["contract_sha"] == digest(contract)
                and config["image"] == contract["image"] and config["live"] is (report["mode"] == "live")
                and all(config[k] == state[k] == case[k] == frozen[k] for k in ("run_id", "project_id", "policy", "repetition")), "Case/config/trajectory identity changed")
        require(config["namespace"] == namespace + "/" + case["run_id"], "Case billing namespace changed")
        require(sha(root / "trajectory.json") == case["trajectory_sha256"] == frozen["trajectory_sha256"]
                and digest(state["files"]) == case["files_sha256"] == frozen["files_sha256"]
                and same(state["final_binding"], frozen["final_binding"])
                and same(git_files(state["final_binding"]), state["files"])
                and GitStore(inside(state["final_binding"]["store_path"], run)).head() == state["final_binding"]["accepted_head"], "Frozen final source/Git binding changed")
        stages = state["stages"]
        require(1 <= len(stages) <= 2 and all(row["completed"] is True for row in stages[:-1])
                and {p.name for p in root.glob("stage-*") if p.is_dir()} == {f"stage-{i}" for i in range(len(stages))}, "Skipped or orphan stage")
        initial, prior, stage_audits, flat_calls = project["initial_files"], {}, [], []
        require(same(GitStore(root / "initial.git").read_files(), initial), "Initial Git source changed")
        for index, stage in enumerate(stages):
            actual_stages.add((case["run_id"], index))
            stage_root, billing = root / f"stage-{index}", config["namespace"] + f"/stage-{index}"
            require(same(load(stage_root / "result.json"), stage), "Stage result changed")
            loaded, bindings = audit_calls(stage_root, stage["invocations"], billing, contract, report["mode"], spans, case["run_id"], index)
            all_bindings.extend(bindings)
            flat_calls.extend(stage["invocations"])
            stage_audits.append(audit_stage(stage, project, index, case["policy"], initial, prior, loaded, validation_contract,
                                           stage_root, git_files, spans, case["run_id"], prompts))
            audit_evaluations(stage, loaded, project, index, validation_contract, stage_root, git_files, spans, case["run_id"])
            record = GitStore(stage_root / "record.git")
            require(same(decode(record.read_files()["record.json"]), {key: stage[key] for key in ("completed", "status", "selected_binding")}), "Stage accounting record differs")
            billing_heads.append((billing, record.head()))
            if stage["completed"]:
                require(same(GitStore(stage_root / "checkpoint.git").read_files(), stage["files"]), "Checkpoint source differs")
            initial, prior = stage["files"], stage
            for call, binding in zip(stage["invocations"], bindings):
                wrapper = spans[call["request_span_id"]]
                commitments.append((wrapper["started_monotonic_ns"], wrapper["finished_monotonic_ns"], binding["amount"], binding["usage"]))
        full = len(stages) == 2 and all(stage["completed"] for stage in stages)
        require(state["terminal"] == ("visible_complete" if full else "bounded_incomplete")
                and same(state["files"], stages[-1]["files"])
                and frozen["evidence_sha256"] == digest(stages[-1]["evidence_cases"])
                and case["milestones_completed"] == frozen["milestones_completed"] == sum(stage["completed"] for stage in stages)
                and same(flat_calls, case["invocations"]), "Terminal trajectory summary differs")
        suites = dict(visible=cases_for(project, 1, "visible") + (stages[-1]["probe_pool"] if len(stages) == 2 else []), private=cases_for(project, 1, "hidden"))
        for kind, cases in suites.items():
            receipt = load(root / f"final-{kind}-receipt.json")
            require(same(receipt, case["final_hidden" if kind == "private" else "final_visible"]), "Final receipt summary differs")
            audit_receipt(receipt, state["files"], cases, validation_contract, "Final " + kind)
            matches = [s for s in spans.values() if s["kind"] == "docker_validation" and s.get("run_id") == case["run_id"] and s.get("purpose") == "final_" + kind]
            require(len(matches) == 1 and matches[0]["freeze_manifest_sha256"] == freeze_sha
                    and matches[0]["started_monotonic_ns"] >= freeze["frozen_monotonic_ns"]
                    and Path(matches[0]["receipt_path"]).resolve() == root / f"final-{kind}-receipt.json"
                    and matches[0]["receipt_sha256"] == sha(root / f"final-{kind}-receipt.json"), "Final execution precedes freeze or receipt changed")
        accepted = bool(full and case["final_visible"]["passed"] and case["final_hidden"]["passed"])
        coverage = {r: all(outcome["passed"] for c, outcome in zip(suites["private"], case["final_hidden"]["outcomes"]) if c["requirement"] == r)
                    for r in sorted({c["requirement"] for c in suites["private"]})}
        require(case["accepted"] is accepted and same(case["requirement_coverage"], coverage)
                and case["usage_micro_usd"] == sum(call["usage_units"] for call in flat_calls)
                and case["physical_provider_calls"] == sum(call["physical_dispatch"] for call in flat_calls)
                and case["freeze_manifest_sha256"] == freeze_sha, "Primary outcome, coverage or usage differs")
        if accepted:
            release = GitStore(root / "release.git")
            require(release.head() == case["release_head"] and same(release.read_files(), state["files"]), "Accepted release differs from tested source")
        else:
            require(case["release_head"] is None and not (root / "release.git").exists(), "Rejected source was released")
        audits.append(dict(run_id=case["run_id"], accepted=accepted,
                           milestones_completed=sum(stage["completed"] for stage in stages), stages=stage_audits))
    known_kinds = {"study", "model_trajectory", "stage", "scout_batch", "worker_request", "physical_provider_request",
                   "scripted_worker", "docker_validation", "final_evaluation"}
    for timed in spans.values():
        require(timed["kind"] in known_kinds, "Unknown timing/execution span kind")
        require(timed["kind"] == "study" or timed.get("run_id") in run_ids, "Execution span lacks a known trajectory")
        if "stage_index" in timed:
            require(type(timed["stage_index"]) is int and (timed.get("run_id"), timed["stage_index"]) in actual_stages,
                    "Timing/execution span targets an unexecuted stage")
        elif timed["kind"] in {"stage", "scout_batch", "worker_request", "physical_provider_request", "scripted_worker"}:
            raise ValueError("Stage execution lacks stage identity")
        if timed["kind"] == "docker_validation":
            require(timed.get("purpose") in {"stage_evidence", "final_visible", "final_private"}
                    and ((timed["purpose"] == "stage_evidence") == ("stage_index" in timed)), "Unknown Docker execution purpose")
    accounting = audit_accounting(accounting_ledger or run / "accounting-snapshot.sqlite", all_bindings, billing_heads,
        [], report, study_namespace=namespace)
    cap = contract["budget"]["incremental_cap_micro_usd"] if report["mode"] == "live" else 0
    peak = max((sum(usage if end <= at else amount if start <= at else 0 for start, end, amount, usage in commitments)
                for at in [start for start, _, _, _ in commitments]), default=0)
    require(accounting["study_usage_micro_usd"] <= cap and peak <= cap, "Reservation-inclusive study cap exceeded")
    require(report["mode"] == "rehearsal" or report["budget_before"]["limit"] == accounting["current_limit"] == contract["budget"]["shared_cumulative_cap_micro_usd"]
            and report["budget_before"]["spent_or_reserved"] == contract["budget"]["expected_starting_usage_micro_usd"]
            and report["budget_before"]["spent_or_reserved"] + peak <= accounting["current_limit"], "Shared budget/cap differs")
    return dict(protocol="independent-continuation-audit-v1", passed=True, root=str(run), results_sha256=sha(run / "results.json"),
        auditor_sha256=sha(__file__), contract_sha256=digest(contract), freeze_manifest_sha256=freeze_sha,
        timings_sha256=sha(run / "timings.json"),
        cases=audits, accounting=accounting, conservative_peak_commitment_micro_usd=peak,
        limitations=["Consistency of retained trusted-host evidence; not external attestation of execution or clocks.",
            "Oracle expectations and runtime/image identity rely on the frozen qualified fixture/validator; this audit executes no candidate or oracle.",
            "Checks bounded calls, eligibility, evidence and explicit acceptance; does not independently prove every stagnation heuristic or qualitative reviewer judgment.",
            "Only complete four-trajectory, unresumed study runs are certified; censored/unknown outcomes require separate accounting review."])


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
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(dict(passed=True, cases=len(result["cases"]), output=str(args.output))))


if __name__ == "__main__":
    main()
