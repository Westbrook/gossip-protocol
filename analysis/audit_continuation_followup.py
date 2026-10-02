"""Independent, read-only checks for the continuation follow-up cohort.

This auditor never imports the follow-up runner, controller, fixture or candidate
code. Initial source formation, journals, public receipts and shared provenance
are checked independently. A certificate requires every preregistered terminal
trajectory, physical source-bound evaluations and the global private barrier.
The auditor never executes candidate, fixture, runner or controller code.
"""
from __future__ import annotations

import argparse
import ast
from contextvars import ContextVar
from copy import deepcopy
import json
import math
from pathlib import Path
import random
import sqlite3
from typing import Any, cast

from analysis.audit_continuation import (
    EvidenceReads, _READ_SET as _HISTORICAL_READ_SET, audit_calls, audit_timings, load, read_bytes, sha,
)
from analysis.audit_benchmark import SOURCES as PREVIOUS_SOURCES, scout_proposals, scope_proposal
from gossip_harness.gitstore import GitStore
from gossip_harness.verification_audit import (
    _adapter_hash, _builder_note, _prompts, audit_accounting, audit_receipt, cases_for, decode, digest,
    inside, public_cases, public_view, require, safe_notes, same,
)


PROTOCOL = "continuation-followup-v1"
AUDIT_PROTOCOL = "independent-continuation-followup-setup-audit-v1"
STAGE_PROTOCOL = "failure-directed-checkpoint-stage-v1"
SHARED_PROTOCOL = "continuation-followup-shared-setup-v1"
ARMS = ("current-independent", "improved-independent", "improved-sequential")
FORMATIONS = ("independent", "sequential")
CONTEXT = "verification-context.json"
SOURCES = PREVIOUS_SOURCES | {"continuation_controller.py", "continuation_followup.py",
                              "benchmark_warehouse.py", "benchmark_job_queue.py"}
EXTRA_SOURCES = {"analysis/qualify_continuation_followup.py", "analysis/audit_continuation_followup.py",
                 "analysis/audit_followup_stage.py", "analysis/audit_continuation.py",
                 "analysis/audit_benchmark.py", "gossip_harness/verification_audit.py",
                 "analysis/continuation_followup_comparison.py"}
COHORT_AUDIT_PROTOCOL = "independent-continuation-followup-cohort-audit-v2"
LIMITS = dict(max_reviews=4, max_repairs=2, max_new_probes=8,
              max_escalations=1, stagnation_reviews=2)
_READ_SET = cast(ContextVar[EvidenceReads | None], _HISTORICAL_READ_SET)


def check_roster(rows, projects):
    """The registered pilot has four blocks and three arms, not 12 replicates."""
    require(type(projects) is list and len(projects) == len(set(projects)) == 2
            and all(type(project) is str and project for project in projects),
            "Exactly two distinct project domains are required")
    expected = {(project, arm, repetition) for project in projects
                for repetition in range(2) for arm in ARMS}
    require(type(rows) is list and len(rows) == 12
            and all(type(row) is list and len(row) == 3 and type(row[2]) is int for row in rows)
            and len({tuple(row) for row in rows}) == 12
            and {tuple(row) for row in rows} == expected,
            "Exact twelve-trajectory, four-block roster required")


def bound_artifact(binding, root, expected=None):
    require(type(binding) is dict and {"path", "sha256"} <= set(binding),
            "Artifact lacks path/digest provenance")
    path = inside(binding["path"], Path(root).resolve())
    require((expected is None or path == expected.resolve())
            and path.is_file() and binding["sha256"] == sha(path),
            "Retained artifact path or bytes changed")
    return path


def git_files(binding, root):
    """Inspect immutable Git objects and any explicitly promoted head."""
    require(type(binding) is dict
            and {"store_path", "tip_sha", "files_sha256"} <= set(binding),
            "Candidate lacks retained Git provenance")
    store = GitStore(inside(binding["store_path"], root))
    files = store.read_files(binding["tip_sha"])
    require(digest(files) == binding["files_sha256"]
            and ("accepted_head" not in binding or store.head() == binding["accepted_head"]),
            "Retained Git source or accepted head changed")
    return files


def audit_initial_pool(pool, *, project, candidate_seed, baseline_approval,
                       formation, calls, contract, root, spans, run_id,
                       builder_prompt=None, initial_result=None):
    """Reconstruct four source opportunities without executing controller code.

    ``calls`` contains already journal-audited (request, result) pairs. Every
    matrix cell must come from a separately bound physical public execution;
    zero-call imported pools cannot masquerade as initial generation here.
    """
    root = Path(root).resolve()
    require(formation in FORMATIONS and type(candidate_seed) is int,
            "Unknown setup formation or alias seed")
    aliases = ["candidate-A", "candidate-B", "candidate-C", "candidate-D"]
    random.Random(digest([project["id"], 0, candidate_seed, "candidate-aliases"])).shuffle(aliases)
    alias_map = {f"slot-{i}": alias for i, alias in enumerate(aliases)}
    public = public_cases(project, 0)
    require(pool.get("protocol") == STAGE_PROTOCOL and pool.get("project_id") == project["id"]
            and type(pool.get("stage_index")) is int and pool["stage_index"] == 0
            and pool.get("policy") == formation + "-four" and pool.get("formation") == formation
            and same(pool.get("alias_map"), alias_map) and pool.get("candidate_order") == aliases
            and set(pool.get("candidates", {})) == set(aliases)
            and same(pool.get("evidence_cases"), public) and pool.get("evidence_sha256") == digest(public),
            "Initial pool contains wrong source opportunities, aliases or public evidence")
    initial = project["initial_files"]
    baseline = GitStore(root / "baseline.git")
    baseline_head = baseline.head()
    require(same(baseline.read_files(), initial), "Setup baseline Git source changed")
    require(type(baseline_approval) is dict
            and set(baseline_approval) == {"files_sha256", "source_valid", "approval_id"}
            and baseline_approval["files_sha256"] == digest(initial)
            and baseline_approval["source_valid"] is True
            and type(baseline_approval["approval_id"]) is str and baseline_approval["approval_id"],
            "Setup baseline lacks an exact source-scope approval")
    expected_ids = {f"builder-slot-{i}-1" for i in range(4)}
    require(set(calls) == expected_ids, "Setup needs four unique physical builder opportunities")
    reqs = list(dict.fromkeys(project["stages"][0]["requirements"]))
    context_base = dict(project_id=project["id"], project_title=project["title"], stage_index=0,
        milestones=[dict(index=0, specification=project["stages"][0].get("specification", project["stages"][0].get("spec")),
                         requirements=project["stages"][0]["requirements"])],
        prior_notes="", prior_remaining=[], prior_machine_history=[], verified_cases=public_view(public))
    previous = None
    used_validations = set()
    expected_trajectory = []
    for index, alias in enumerate(aliases):
        slot, label = f"slot-{index}", f"stage-0-build-slot-{index}-1"
        request, call = calls[f"builder-{slot}-1"]
        predecessor = previous if formation == "sequential" else None
        base = predecessor["files"] if predecessor else initial
        contents = dict(request["files"])
        context = decode(contents.pop(CONTEXT))
        feedback = dict(checkpoint_notes=predecessor["notes"] if predecessor else "")
        if predecessor:
            feedback.update(preceding_source_sha256=digest(base), evidence=[dict(case=case,
                outcome=deepcopy(predecessor["matrix"][case["id"]])) for case in public_view(public)])
        require(call["role"] == "builder" and call["model"] == "cheap"
                and call["journal_replay"] is False
                and same(contents, base) and same(context, context_base)
                and request["base_sha"] == baseline_head
                and type(request["attempt"]) is int and request["attempt"] == 1
                and request["allowed_paths"] == [*project["allowed_paths"], "notes.json"]
                and same(decode(request["feedback"]), feedback)
                and same(call["controller_metadata"], dict(project_id=project["id"], stage_index=0,
                    policy=formation + "-four", role="builder", round=1, candidate_id=slot,
                    escalation_id=None, protocol=STAGE_PROTOCOL, phase="initial"))
                and (builder_prompt is None or request["instructions"] == builder_prompt),
                "Initial builder source, role, instructions or public context differs")
        changes = None if "failure" in call else call.get("changes")
        files, valid, admissible, changed = scope_proposal(base,
            predecessor["source_valid"] if predecessor else True, changes, project["allowed_paths"])
        item = pool["candidates"][alias]
        note = _builder_note(changes, reqs)
        note_text = safe_notes(note["notes"]) if note else ""
        origin = (dict(kind="admissible_proposal", label=label, files_sha256=digest(files)) if changed else
                  deepcopy(predecessor["eligibility_origin"]) if predecessor else
                  dict(kind="approved_baseline", approval=deepcopy(baseline_approval), files_sha256=digest(files)))
        attempt_status = ("effective_source_proposal" if changed else
                          "no_effective_source_change" if admissible else "rejected_source_proposal")
        require(same(item["files"], files) and same(git_files(item["binding"], root), files)
                and GitStore(inside(item["binding"]["store_path"], root)).head() == baseline_head
                and set(files) == set(initial)
                and all(files[path] == value for path, value in initial.items() if path not in project["allowed_paths"])
                and item["slot"] == slot and item["alias"] == alias
                and item["source_valid"] is valid and same(item["eligibility_origin"], origin)
                and item["latest_attempt_admissible"] is admissible
                and item["latest_attempt_status"] == attempt_status
                and item["notes"] == (note_text if note and admissible else predecessor["notes"] if predecessor else "")
                and item["remaining"] == (note["remaining"] if note and admissible else predecessor["remaining"] if predecessor else [])
                and item["reviewer_remaining"] == [],
                "Retained initial proposal, source eligibility or advisory state differs")
        receipts = item["validation_receipts"]
        require(type(receipts) is list and len(receipts) == 1
                and receipts[0]["label"] == label + "-all-evidence"
                and receipts[0]["case_ids"] == [case["id"] for case in public],
                "Initial candidate needs one complete original public execution")
        receipt = receipts[0]["receipt"]
        require(same(item["matrix"], audit_receipt(receipt, files, public, contract)),
                "Initial matrix differs from its exact typed receipt")
        matches = [row for row in spans.values() if row.get("run_id") == run_id
                   and row["kind"] == "docker_validation" and row.get("label") == receipts[0]["label"]]
        require(len(matches) == 1, "Missing or duplicate physical initial validation")
        span = matches[0]
        receipt_path = inside(span["receipt_path"], root)
        require(span.get("stage_index") == 0 and span.get("purpose") == "stage_evidence"
                and span.get("validator_origin") == "physical_docker"
                and span["status"] == "finished" and span["receipt_sha256"] == sha(receipt_path)
                and same(load(receipt_path), receipt) and span["case_count"] == len(public)
                and spans[call["request_span_id"]]["finished_monotonic_ns"] <= span["started_monotonic_ns"],
                "Initial receipt, timing or public purpose changed")
        if predecessor:
            require(predecessor["validation_finished_monotonic_ns"] <= spans[call["request_span_id"]]["started_monotonic_ns"],
                    "Serial source was requested before predecessor evidence existed")
        expected_trajectory.append(dict(kind="builder", slot=slot, candidate=alias, round=1,
            phase="initial", inherited_from=predecessor["alias"] if predecessor else None,
            source_valid=valid, notes_valid=note is not None, binding=item["binding"],
            attempt_status=attempt_status, attempt_admissible=admissible,
            effective_source_change=changed, prior_source_valid=predecessor["source_valid"] if predecessor else True,
            previous_files_sha256=digest(base), model="cheap", files_sha256=digest(files),
            visible_passed=all(value["passed"] for value in item["matrix"].values()),
            failures=[case["id"] for case in public if not item["matrix"][case["id"]]["passed"]]))
        used_validations.add(span["span_id"])
        previous = {**item, "validation_finished_monotonic_ns": span["finished_monotonic_ns"]}
    observed = {row["span_id"] for row in spans.values() if row.get("run_id") == run_id
                and row["kind"] == "docker_validation"}
    require(observed == used_validations, "Orphan or nonpublic setup validation")
    if initial_result is not None:
        require(initial_result.get("protocol") == STAGE_PROTOCOL
                and initial_result.get("controller_mode") == "current"
                and initial_result.get("initial_only") is True and initial_result.get("shared_setup") is None
                and same(initial_result.get("initial_pool"), pool)
                and initial_result.get("initial_pool_sha256") == digest(pool)
                and same(initial_result.get("trajectory"), expected_trajectory),
                "Initial-only trajectory differs from reconstructed physical opportunities")
        metrics = initial_result["metrics"]
        names = ("builder_calls reviewer_calls repairs invalid_reviews invalid_builder_notes invalid_source_proposals "
            "blocked_acceptances admitted_new_probes generated_probe_attempts rejected_probes retired_probes "
            "probe_failures_found probe_discriminating_cases acceptances_without_new_probes no_effective_source_proposals "
            "retained_eligibility_preserved effective_source_changes escalations escalated_repairs escalated_reviews "
            "stagnation_events strongest_builder_calls initial_builder_calls scout_proposal_attempts admitted_scout_probes "
            "rejected_scout_probes scout_probe_failures_found scout_probe_discriminating_cases focused_review_requests "
            "retained_repair_checkpoints repair_regressions repair_failure_progress resolved_known_failures "
            "introduced_failures alternative_routes focused_evidence_steps shared_initial_builder_opportunities").split()
        expected_metrics = dict.fromkeys(names, 0)
        expected_metrics.update(builder_calls=4, initial_builder_calls=4,
            invalid_builder_notes=sum(not row["notes_valid"] for row in expected_trajectory),
            invalid_source_proposals=sum(not row["attempt_admissible"] for row in expected_trajectory),
            no_effective_source_proposals=sum(row["attempt_admissible"] and not row["effective_source_change"] for row in expected_trajectory),
            effective_source_changes=sum(row["effective_source_change"] for row in expected_trajectory),
            retained_eligibility_preserved=sum(not row["effective_source_change"] and row["prior_source_valid"] for row in expected_trajectory))
        require(same(metrics, expected_metrics),
                "Setup performed continuation or attributed imports as fresh builders")
    return dict(formation=formation, initial_opportunities=4, public_executions=4,
                public_cases=len(public), pool_sha256=digest(pool))


def check_frozen_snapshot(path):
    """A retained SQLite backup cannot silently consult unbound sidecar state."""
    path = Path(path)
    for suffix in ("-wal", "-journal"):
        sidecar = Path(str(path) + suffix)
        require(not sidecar.exists() or read_bytes(sidecar) == b"",
                "Frozen accounting snapshot has an unbound WAL or rollback journal")


def check_cohort_contract(contract, plan, root):
    """Validate scientific opportunities and the code actually doing this audit."""
    require(contract.get("protocol") == PROTOCOL and contract.get("status") == "cohort_implementation"
            and contract.get("full_cohort_qualification") is True and contract.get("hidden_feedback") is False
            and type(contract.get("live_enabled")) is bool,
            "Only the implemented zero-API cohort contract is supported")
    check_roster(contract["roster"], contract["project_ids"])
    policies = {arm: dict(controller="current" if arm == "current-independent" else "improved",
        formation="sequential" if arm == "improved-sequential" else "independent", initial_builders=4, model="cheap")
        for arm in ARMS}
    require(same(contract["policies"], policies) and same(contract["limits"], LIMITS)
            and same(contract["project_ids"], ["warehouse", "job-queue"])
            and type(contract["milestones"]) is int and contract["milestones"] == 2
            and type(contract["repetitions"]) is int and contract["repetitions"] == 2
            and type(contract["output_tokens"]) is int and contract["output_tokens"] == 12288
            and type(contract["case_timeout_seconds"]) is int and contract["case_timeout_seconds"] == 12
            and type(contract["suite_timeout_seconds"]) is int and contract["suite_timeout_seconds"] == 300
            and contract["improved_controller_rules"].get("uncertainty_focus_retries") == 1
            and type(contract["improved_controller_rules"].get("uncertainty_focus_retries")) is int
            and contract["pairing"].get("stage0") == "shared-independent-pool-and-common-scouts"
            and contract["pairing"].get("later") == "arm-specific",
            "Cohort opportunities, public evidence policy or limits changed")
    image = contract.get("image")
    require(type(image) is str and image.startswith("sha256:") and len(image) == 71
            and all(char in "0123456789abcdef" for char in image[7:]), "Unpinned execution image")
    require(set(contract["sources"]) == SOURCES and set(contract["extra_sources"]) == EXTRA_SOURCES,
            "Complete scientific and independent auditor source inventory required")
    for name, expected in contract["sources"].items():
        require(sha(root / "source-snapshot" / "gossip_harness" / name) == expected,
                "Frozen scientific source changed: " + name)
    repository = Path(__file__).resolve().parent.parent
    require(sha(repository / "gossip_harness" / "gitstore.py") == contract["sources"]["gitstore.py"],
            "The executing Git reader differs from its qualified source")
    for name, expected in contract["extra_sources"].items():
        require(sha(root / "source-snapshot" / name) == expected, "Frozen auxiliary source changed: " + name)
        if name != "analysis/qualify_continuation_followup.py":
            require(sha(repository / name) == expected,
                    "The executing independent auditor differs from the qualified source: " + name)
    require(sha(root / "study-plan.json") == contract["plan_sha256"] and plan.get("protocol") == PROTOCOL
            and all(same(plan[key], contract[key]) for key in
                    ("roster", "block_order", "policies", "budget", "pairing", "improved_controller_rules",
                     "milestones", "repetitions"))
            and same(plan["projects"], contract["project_ids"])
            and all(type(plan["controller"].get(key)) is int and plan["controller"][key] == value
                    for key, value in {**LIMITS, "output_tokens": 12288, "scouts": 2, "proposals_per_scout": 4}.items()),
            "Frozen plan and runtime opportunities differ")
    require(contract.get("rehearsal_version") == "controller-rehearsal-v2"
            and same(contract.get("rehearsal_controls"), plan.get("rehearsal"))
            and contract["live_enabled"] is (plan.get("readiness", {}).get("live_authorized") is True),
            "Rehearsal mechanisms are not bound to the frozen plan")
    blocks = contract["block_order"]
    require(type(blocks) is list and len(blocks) == 4
            and all(type(row) is list and len(row) == 2 and type(row[1]) is int for row in blocks)
            and {tuple(row) for row in blocks} == {(pid, rep) for pid in contract["project_ids"] for rep in range(2)}
            and plan["generation_order"] in [list(FORMATIONS), list(reversed(FORMATIONS))],
            "Complete paired setup order required")
    budget = contract["budget"]
    keys = ("incremental_cap_micro_usd", "shared_cumulative_cap_micro_usd", "expected_starting_usage_micro_usd")
    require(all(type(budget.get(key)) is int and budget[key] >= 0 for key in keys)
            and 0 < budget[keys[0]] <= budget[keys[1]] - budget[keys[2]], "Frozen authorization bounds changed")
    check_model_profiles(contract, read_bytes(root / "source-snapshot" / "gossip_harness" / "worker.py").decode())


def check_model_profiles(contract, source):
    """Read the closed profile registry as literal AST data, never import it."""
    tree = ast.parse(source)
    constants = {}
    protected = {"MODEL", "STRONG_MODEL", "CONTEXT_TOKENS", "MODEL_MAX_OUTPUT_TOKENS", "ENDPOINT",
                 "PRICE_VERIFIED_ON", "MAX_REQUEST_BYTES"}
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            name = node.targets[0].id
            try:
                parsed = ast.literal_eval(node.value)
            except (ValueError, TypeError):
                require(name not in protected, "Worker pricing constant is not literal")
            else:
                require(name not in protected or name not in constants, "Worker pricing constant was rebound")
                constants[name] = parsed
    require(all(sum(isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store) and node.id == name
                    for node in ast.walk(tree)) == 1 for name in protected | {"MODEL_PROFILES"}),
            "Worker pricing registry or constants have additional assignments")
    def value(node):
        return constants[node.id] if isinstance(node, ast.Name) else ast.literal_eval(node)
    registry, = [node.value for node in tree.body if isinstance(node, ast.AnnAssign)
                 and isinstance(node.target, ast.Name) and node.target.id == "MODEL_PROFILES"]
    if not (isinstance(registry, ast.Call) and isinstance(registry.func, ast.Name)
            and registry.func.id == "MappingProxyType" and len(registry.args) == 1
            and not registry.keywords and isinstance(registry.args[0], ast.Dict)):
        raise ValueError("Frozen worker profile registry is not literal")
    fields = ("model", "context_tokens", "max_output_tokens", "input_micro_usd_per_million",
              "output_micro_usd_per_million", "source_url")
    profiles = {}
    for key, node in zip(registry.args[0].keys, registry.args[0].values):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "ModelProfile"
                and len(node.args) == len(fields)
                and all(item.arg in {"long_context_above_input_tokens", "long_input_micro_usd_per_million",
                        "long_output_micro_usd_per_million", "price_verified_on"} for item in node.keywords)
                and len({item.arg for item in node.keywords}) == len(node.keywords)):
            raise ValueError("Frozen model profile shape differs")
        row = {name: value(argument) for name, argument in zip(fields, node.args)}
        row.update(long_context_above_input_tokens=None, long_input_micro_usd_per_million=None,
                   long_output_micro_usd_per_million=None, price_verified_on=constants["PRICE_VERIFIED_ON"])
        row.update({cast(str, item.arg): value(item.value) for item in node.keywords})
        long = row["long_context_above_input_tokens"] is not None and row["context_tokens"] > row["long_context_above_input_tokens"]
        rate_in = row["long_input_micro_usd_per_million"] if long else row["input_micro_usd_per_million"]
        rate_out = row["long_output_micro_usd_per_million"] if long else row["output_micro_usd_per_million"]
        row.update(endpoint=constants["ENDPOINT"], service_tier="default", reasoning_effort="low",
            configured_max_output_tokens=contract["output_tokens"], max_request_bytes=constants["MAX_REQUEST_BYTES"],
            cached_input_accounting="uncached_rate", reservation_policy="full_context_input_plus_configured_max_output",
            reservation_units=(row["context_tokens"] * rate_in + contract["output_tokens"] * rate_out + 999999) // 1000000)
        profiles[value(key)] = row
    expected = {role: profiles[constants[name]] for role, name in (("cheap", "MODEL"), ("strong", "STRONG_MODEL"))}
    require(same(contract["models"], expected), "Frozen pricing, model identity or reservation profile differs from worker source")
    envelope = contract["call_envelope"]
    require(envelope["maximum_physical_calls"] == 256 and type(envelope["maximum_physical_calls"]) is int
            and same(envelope["maximum_calls_by_model"], dict(cheap=160, strong=120))
            and same(envelope["reservation_micro_usd_per_call"], {role: row["reservation_units"] for role, row in expected.items()})
            and type(envelope["conservative_full_cohort_micro_usd"]) is int
            and envelope["conservative_full_cohort_micro_usd"] == 136 * expected["cheap"]["reservation_units"] + 120 * expected["strong"]["reservation_units"],
            "Physical call and full-context commitment envelope differs")


def known_failure_spans(calls, spans, mode="rehearsal"):
    """Only independently journal-checked settled failures may have failed spans."""
    allowed = set()
    for call in calls:
        failed = "failure" in call
        for key, kind in (("request_span_id", "worker_request"),
                          ("physical_span_id", "scripted_worker" if mode == "rehearsal" else "physical_provider_request")):
            span = spans[call[key]]
            require(span["kind"] == kind and span["call_id"] == call["call_id"]
                    and span["status"] == ("failed" if failed else "finished"),
                    "Physical request status differs from its retained call outcome")
            if failed:
                require(call["outcome"] == "failure" and type(call.get("failure")) is str
                        and type(call["usage_units"]) is int and call["usage_units"] >= 0
                        and call["physical_dispatch"] is (mode == "live") and call["journal_replay"] is False
                        and (mode == "live" or call["usage_units"] == call["metadata"].get("api_calls") == 0)
                        and not call["metadata"].get("halt")
                        and span.get("error_type") == "WorkerFailure",
                        "Unknown or indeterminate failure cannot qualify a complete rehearsal")
                allowed.add(call[key])
    return allowed


def check_cohort_barrier(freeze, run_ids, session_ids, spans, allowed_failed=()):
    """Incomplete and stopped arms stay in the denominator before private work."""
    require(freeze.get("protocol") == PROTOCOL and freeze.get("private_evaluation_started") is False
            and [row["run_id"] for row in freeze["trajectories"]] == list(run_ids)
            and len(set(run_ids)) == len(run_ids) == 12,
            "Incomplete twelve-trajectory global freeze")
    barrier = freeze.get("frozen_monotonic_ns")
    require(type(barrier) is int and barrier >= 0, "Missing global monotonic barrier")
    generation = {"model_trajectory", "stage", "worker_request", "physical_provider_request", "scripted_worker", "scout_batch", "shared_setup_block"}
    allowed = generation | {"study", "docker_validation", "final_evaluation"}
    for span_id, row in spans.items():
        require(row.get("kind") in allowed and (row["kind"] == "study"
                or row.get("run_id") in set(run_ids) | set(session_ids)), "Orphan execution span")
        require(row.get("status") == "finished" or (span_id in allowed_failed
                and row.get("status") == "failed" and row["kind"] in {"worker_request", "scripted_worker", "physical_provider_request"}),
                "Failed or unfinished execution is not a complete cohort")
        if row["kind"] in generation:
            require(row["finished_monotonic_ns"] <= barrier, "Generation crossed global freeze")
        if row["kind"] == "docker_validation":
            purpose = row.get("purpose")
            require(purpose in {"stage_evidence", "final_visible", "final_private"}
                    and row.get("validator_origin") == "physical_docker", "Unknown or synthetic validation purpose")
            if purpose == "stage_evidence":
                require(row.get("run_id") in set(session_ids) | set(run_ids) and type(row.get("stage_index")) is int
                        and row["stage_index"] in range(2) and row["finished_monotonic_ns"] <= barrier,
                        "Stage evidence crossed global freeze or lacks session identity")
            else:
                require(row.get("run_id") in run_ids and "stage_index" not in row
                        and row["started_monotonic_ns"] >= barrier,
                        "Independent final evaluation preceded the complete cohort barrier")


def check_fresh_receipts(spans, root, contract):
    """Every physical observation has one distinct file and container identity."""
    root = Path(root).resolve()
    paths, containers = set(), set()
    for row in spans.values():
        if row["kind"] != "docker_validation":
            continue
        path = inside(row["receipt_path"], root)
        receipt = load(path)
        name = receipt.get("container_name")
        require(path not in paths and sha(path) == row["receipt_sha256"]
                and type(name) is str and name.startswith("gossip-blackbox-") and name not in containers
                and receipt.get("image_id") == contract["image"],
                "Independent executions reused a receipt/container or changed runtime")
        paths.add(path)
        containers.add(name)
    return paths, containers


def check_primary_outcome(case, state, project, validation_contract):
    """Acceptance follows exact final source and both completed milestones."""
    stages = state["stages"]
    require(type(stages) is list and 1 <= len(stages) <= 2
            and all(stage.get("completed") is True for stage in stages[:-1])
            and all(type(stage.get("completed")) is bool for stage in stages),
            "Missing, skipped or nonboolean milestone completion")
    full = len(stages) == 2 and all(stage["completed"] for stage in stages)
    terminal = "visible_complete" if full else "bounded_incomplete"
    require(state["terminal"] == terminal and case["milestones_completed"] == sum(stage["completed"] for stage in stages)
            and same(state["files"], stages[-1]["files"]), "Terminal state differs from actual milestone decisions")
    suites = dict(visible=cases_for(project, 1, "visible") + (stages[-1]["probe_pool"] if len(stages) == 2 else []),
                  private=cases_for(project, 1, "hidden"))
    for kind, cases in suites.items():
        audit_receipt(case["final_hidden" if kind == "private" else "final_visible"], state["files"], cases,
                      validation_contract, "Independent final " + kind)
    accepted = bool(full and case["final_visible"]["passed"] and case["final_hidden"]["passed"])
    coverage = {requirement: all(outcome["passed"] for item, outcome in
        zip(suites["private"], case["final_hidden"]["outcomes"]) if item["requirement"] == requirement)
        for requirement in sorted({item["requirement"] for item in suites["private"]})}
    require(type(case.get("accepted")) is bool and case["accepted"] is accepted
            and same(case["requirement_coverage"], coverage), "Acceptance or requirement coverage contradicts raw final outcomes")
    return accepted, suites


def audit_qualification(binding, contract, projects, validation_contract):
    """Derive qualification from retained source/suite executions, not flags.

    The fixture's scientific meaning remains a separately reviewed assumption.
    Here its exact bound matrix, baseline eligibility, runnable mutant witnesses
    and diverse correct controls must all agree with independently read output.
    """
    path = Path(binding["path"]).resolve()
    require(path.name == "results.json" and sha(path) == binding["sha256"],
            "Qualification result binding changed")
    report, manifest = load(path), load(path.parent / "manifest.json")
    protocol = "continuation-followup-fixture-qualification-v2"
    require(report.get("protocol") == manifest.get("protocol") == protocol
            and report.get("status") == "qualified" and report.get("api_calls") == 0
            and type(report.get("api_calls")) is int and manifest.get("api_calls") == 0
            and manifest.get("candidate_execution_on_host") is False
            and report["contract_sha256"] == manifest["contract_sha256"] == digest(contract)
            and same(manifest["contract"], contract)
            and report["manifest_sha256"] == sha(path.parent / "manifest.json")
            and report["execution_environment_sha256"] == manifest["execution_environment_sha256"]
                == digest(manifest["execution_environment"])
            and report["runtime_identity_sha256"] == digest(report["runtime_identity"])
            and manifest["adapter_sha256"] == validation_contract["adapter_sha256"],
            "Qualification execution or dependency identity changed")
    require(type(manifest.get("minimum_surviving_families")) is int
            and manifest["minimum_surviving_families"] == 4, "Qualification fault-family threshold changed")
    runtime = report["runtime_identity"]
    require(type(runtime) is dict and set(runtime) == {"server_sha256", "daemon_id_sha256", "configuration_sha256",
            "context_sha256", "server", "image"}
            and all(type(runtime[key]) is str and len(runtime[key]) == 64
                    and all(char in "0123456789abcdef" for char in runtime[key])
                    for key in ("server_sha256", "daemon_id_sha256", "configuration_sha256", "context_sha256"))
            and set(runtime["server"]) == {"Version", "ApiVersion", "Os", "Arch"}
            and all(type(value) is str and value for value in runtime["server"].values())
            and set(runtime["image"]) == {"Id", "Os", "Architecture", "Variant"}
            and runtime["image"]["Id"] == contract["image"]
            and all(type(runtime["image"][key]) is str and runtime["image"][key] for key in ("Os", "Architecture"))
            and (runtime["image"]["Variant"] is None or type(runtime["image"]["Variant"]) is str),
            "Qualification runtime lacks complete pinned image, daemon or context identity")
    expected_inputs = {"continuation-followup-study-plan.json": contract["plan_sha256"],
        **{"gossip_harness/" + key: value for key, value in contract["sources"].items()},
        **contract["extra_sources"]}
    require(same(manifest["inputs"], [dict(path=key, sha256=value) for key, value in sorted(expected_inputs.items())]),
            "Qualification omitted or reordered a scientific dependency")
    for name, expected in expected_inputs.items():
        require(sha(inside(path.parent / "inputs" / name, path.parent)) == expected,
                "Qualification dependency bytes changed: " + name)
    matrix = manifest["matrix"]
    stages = [[pid, index] for pid in contract["project_ids"] for index in range(2)]
    require(contract.get("qualification_protocol") == protocol
            and manifest["matrix_sha256"] == contract.get("qualification_matrix_sha256") == digest(matrix)
            and same(matrix["stages"], stages)
            and matrix.get("baseline_projects") == contract["project_ids"]
            and len(matrix["rows"]) == len(report["executions"]) == len(report["classifications"]) == 42,
            "The complete forty-two-source qualification matrix is required")
    containers, fixture_ids, classifications = set(), set(), []
    baselines = {}
    for index, (row, executed) in enumerate(zip(matrix["rows"], report["executions"])):
        require(type(row["index"]) is int and row["index"] == index
                and row["source_sha256"] == digest(row["files"])
                and row["suite_sha256"] == digest(row["cases"])
                and row["fixture_id"] not in fixture_ids
                and row["receipt"] == f"receipts/{index:04d}.json"
                and all(same(executed[key], row[key]) for key in
                        ("index", "purpose", "fixture_id", "receipt", "source_sha256", "suite_sha256"))
                and executed["physically_executed"] is True and executed["reused"] is False
                and executed["verified"] is True and executed["runtime_identity_sha256"] == report["runtime_identity_sha256"],
                "Qualification row identity, freshness or source/suite changed")
        fixture_ids.add(row["fixture_id"])
        receipt_path = inside(path.parent / row["receipt"], path.parent)
        receipt = load(receipt_path)
        require(sha(receipt_path) == executed["receipt_sha256"], "Qualification raw receipt changed")
        audit_receipt(receipt, row["files"], row["cases"], validation_contract, "Fixture qualification")
        name = receipt.get("container_name")
        require(type(name) is str and name.startswith("gossip-blackbox-") and name not in containers,
                "Qualification source/suite executions must use fresh containers")
        containers.add(name)
        outcomes, groups = receipt["outcomes"], row["groups"]
        require(set(groups) == {"public", "private", "witness"}
                and groups["public"] and groups["private"]
                and all(type(value) is int and value in range(len(outcomes))
                        for indexes in groups.values() for value in indexes), "Invalid qualification case groups")
        runnable = all(item["status"] in {"passed", "wrong_answer"} for item in outcomes)
        public_passed = all(outcomes[item]["passed"] for item in groups["public"])
        witnesses = [item for item in groups["witness"] if outcomes[item]["status"] == "wrong_answer"]
        qualified = (runnable and public_passed and bool(witnesses) if row["kind"] == "mutant"
                     else receipt["passed"] and runnable)
        classification = {key: row[key] for key in ("index", "project_id", "stage_index", "source_id", "kind", "family", "fixture_id")}
        classification.update(runnable=runnable, public_surviving=public_passed and runnable,
            witness_wrong_answers=witnesses, all_cases_passed=receipt["passed"], qualified=qualified)
        require(same(classification, report["classifications"][index]) and qualified,
                "Fixture qualification does not follow from actual runnable outputs")
        classifications.append(classification)
        project = projects[row["project_id"]]
        if row["kind"] == "baseline":
            require(index in (40, 41) and row["stage_index"] == -1 and type(row["stage_index"]) is int
                    and row["source_id"] == "initial" and same(row["files"], project["initial_files"])
                    and not groups["witness"] and row["project_id"] not in baselines,
                    "Baseline eligibility lacks its exact independent qualification")
            baselines[row["project_id"]] = dict(row=row, execution=executed, receipt_path=str(receipt_path))
        else:
            require([row["project_id"], row["stage_index"]] in stages
                    and type(row["stage_index"]) is int and row["kind"] in {"golden", "control", "mutant"},
                    "Unknown qualified fixture source")
            if row["kind"] == "golden":
                require(same(row["files"], project["stages"][row["stage_index"]]["known_files"]),
                        "Qualified golden source differs from the frozen fixture")
            public = cases_for(project, row["stage_index"], "visible")
            private = cases_for(project, row["stage_index"], "hidden")
            for group, expected in (("public", public), ("private", private)):
                actual = [row["cases"][item] for item in groups[group]]
                # Suites deduplicate identical inputs but preserve every required observation.
                require(all(any(same({k: case[k] for k in ("id", "requirement", "input", "expected") if k in case}, observed)
                                    for observed in actual) for case in expected),
                        "Qualification omitted an acceptance scenario")
            require(all(item in groups["private"] for item in groups["witness"]),
                    "Mutant witness is not an independently held-out acceptance case")
    require(set(baselines) == set(projects), "Missing qualified application baseline")
    stage_conclusions = []
    for pid, stage in stages:
        rows = [row for row in matrix["rows"] if row["project_id"] == pid and row["stage_index"] == stage]
        require(sum(row["kind"] == "golden" for row in rows) == 1
                and sum(row["kind"] == "control" for row in rows) == 2
                and len({row["family"] for row in rows if row["kind"] == "mutant"}) >= 4,
                "Qualification lacks required correct controls or independent fault families")
        golden, = [row for row in rows if row["kind"] == "golden"]
        controls = [row for row in rows if row["kind"] == "control"]
        require(any(row["source_sha256"] != golden["source_sha256"] for row in controls),
                "Qualification correct controls lack source diversity")
        distinct = [row["source_sha256"] for row in rows
                    if not (row["kind"] == "control" and row["source_id"] == "correct"
                            and row["source_sha256"] == golden["source_sha256"])]
        require(len(distinct) == len(set(distinct)), "Qualification duplicates a mutant or nonbaseline control source")
        families = sorted({row["family"] for row in rows if row["kind"] == "mutant"})
        stage_conclusions.append(dict(project_id=pid, stage_index=stage, qualified=True,
            public_surviving_families=families, public_surviving_family_count=len(families), source_count=len(rows)))
    expected_conclusion = dict(qualified=True, complete=True, stages=stage_conclusions,
        baselines=[dict(project_id=pid, qualified=True, source_count=1) for pid in contract["project_ids"]])
    require(same(report["conclusion"], expected_conclusion), "Qualification conclusion differs from independently reconstructed rows")
    return dict(report=report, manifest=manifest, baselines=baselines, containers=containers)


def qualified_baseline_proof(qualification, project_id):
    report = qualification["report"]
    item = qualification["baselines"][project_id]
    row, execution = item["row"], item["execution"]
    return dict(protocol=report["protocol"], contract_sha256=report["contract_sha256"],
        manifest_sha256=report["manifest_sha256"], execution_environment_sha256=report["execution_environment_sha256"],
        runtime_identity_sha256=report["runtime_identity_sha256"], project_id=project_id,
        stage_index=-1, fixture_id=row["fixture_id"], source_id="initial", index=row["index"],
        purpose=row["purpose"], source_sha256=row["source_sha256"], suite_sha256=row["suite_sha256"],
        receipt=row["receipt"], receipt_sha256=execution["receipt_sha256"], physically_executed=True, reused=False)


def audit_local_promotion(destination, initial_head, files, allowed_paths):
    """Read the exact Git CAS outcome and its independent local intent ledger."""
    destination = Path(destination).resolve()
    store = GitStore(destination)
    head = store.head()
    require(same(store.read_files(), files) and store.is_ancestor(initial_head, head),
            "Promoted Git source or accepted ancestry differs")
    changed = store._git("diff", "--name-only", "-z", initial_head, head).rstrip("\0").split("\0")
    require(set(changed) - {""} <= set(allowed_paths), "Promotion changes trusted source outside the allowed scope")
    ledger = destination.with_suffix(".sqlite")
    check_frozen_snapshot(ledger)
    sha(ledger)
    with sqlite3.connect(ledger.as_uri() + "?mode=ro", uri=True) as db:
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA query_only=ON")
        db.execute("BEGIN")
        tasks = [dict(row) for row in db.execute("SELECT * FROM tasks")]
        intents = [dict(row) for row in db.execute("SELECT * FROM intents")]
        require(len(tasks) == len(intents) == 1
                and db.execute("SELECT count(*) FROM reservations").fetchone()[0] == 0,
                "Local promotion has orphan tasks, intents or usage")
        task, intent = tasks[0], intents[0]
        leases = decode(intent["leases"])
        require(type(leases) is list and len(leases) == 1 and type(leases[0]) is dict
                and set(leases[0]) == {"task_id", "worker_id", "epoch", "expires_at"}
                and type(leases[0]["task_id"]) is str and type(leases[0]["worker_id"]) is str
                and type(leases[0]["epoch"]) is int and leases[0]["epoch"] >= 1
                and type(leases[0]["expires_at"]) in {int, float}
                and math.isfinite(leases[0]["expires_at"]) and leases[0]["expires_at"] > 0,
                "Promotion intent does not contain one exact typed serialized Lease")
        require(task["id"] == "exact-tree" and task["status"] == "complete" and task["accepted_commit"] == head
                and task["intent_id"] is None and intent["state"] == "accepted"
                and Path(intent["repository"]).resolve() == destination
                and intent["old_head"] == initial_head and intent["new_head"] == head
                and len(leases) == 1 and leases[0]["task_id"] == task["id"]
                and leases[0]["epoch"] == task["epoch"] and leases[0]["worker_id"] == task["worker"]
                and leases[0]["expires_at"] == task["expires"],
                "Promotion is not bound to a settled exact-tree Git CAS intent")
    check_frozen_snapshot(ledger)
    return head


def audit_stage_session(session, stage, root, project, initial, contract, validation_contract, spans, run_id, index, billing, mode):
    """Bind every raw opportunity and retained proposal to one session."""
    require(session.get("protocol") == PROTOCOL and session.get("mode") == mode
            and session.get("execution_mode") == mode and session.get("session_kind") == "stage"
            and session.get("session_id") == run_id and session.get("project_id") == project["id"]
            and type(session.get("stage_index")) is int and session["stage_index"] == index
            and session["billing"] == billing and session["contract_sha256"] == digest(contract)
            and session["image"] == contract["image"] and session["validator_origin"] == "physical_docker"
            and session["initial_files_sha256"] == digest(initial)
            and same(session["result"], stage) and same(session["invocations"], stage["invocations"]),
            "Stage session source, execution origin, opportunity inventory or result differs")
    baseline = GitStore(root / "baseline.git")
    require(same(baseline.read_files(), initial), "Later stage did not start from its own accepted source")
    calls, bindings = audit_calls(root, session["invocations"], billing, contract, mode, spans, run_id, index)
    require(session["physical_provider_calls"] == sum(call["physical_dispatch"] for _, call in calls.values())
            and type(session["physical_provider_calls"]) is int
            and session["usage_micro_usd"] == sum(call["usage_units"] for _, call in calls.values())
            and type(session["usage_micro_usd"]) is int
            and all(call["execution_kind"] == ("scripted_worker" if mode == "rehearsal" else "physical_provider_request")
                    and call["journal_replay"] is False and request["base_sha"] == baseline.head()
                    and (mode == "rehearsal" or not ({"rehearsal_version", "rehearsal_noop"} & set(call["metadata"])))
                    for request, call in calls.values()), "Session changed execution mode, cash, replay or its source base")
    exports, validation_ids = set(), set()
    for entry in session["exports"]:
        path = bound_artifact(entry, root, root / (entry["label"].replace("/", "-") + ".json"))
        source = load(path)
        files = git_files(source["binding"], root)
        store = GitStore(inside(source["binding"]["store_path"], root))
        require(entry["label"] not in exports and source["label"] == entry["label"]
                and same(entry["binding"], source["binding"]) and same(source["files"], files)
                and digest(files) == entry["files_sha256"] and set(files) == set(initial)
                and all(files[name] == initial[name] for name in initial if name not in project["allowed_paths"])
                and store.head() == baseline.head()
                and store._git("rev-list", "--parents", "-n", "1", source["binding"]["tip_sha"]).split()[1:] == [baseline.head()],
                "Immutable candidate export, proposal parent or source scope differs")
        exports.add(entry["label"])
    expected_exports = {f"stage-{index}-build-{row['slot']}-{row['round']}"
                        for row in stage["trajectory"] if row["kind"] == "builder"}
    if stage.get("shared_setup") is not None:
        expected_exports.update(f"stage-{index}-shared-{row['slot']}" for row in stage["initial_pool"]["candidates"].values())
    require(exports == expected_exports, "Session retained proposal inventory differs from controller opportunities")
    for entry in session["validations"]:
        path = bound_artifact(entry, root)
        timed = spans[entry["span_id"]]
        require(entry["span_id"] not in validation_ids and timed["kind"] == "docker_validation"
                and timed["run_id"] == run_id and timed["stage_index"] == index
                and timed["purpose"] == "stage_evidence" and timed["label"] == entry["label"]
                and Path(timed["receipt_path"]).resolve() == path and timed["receipt_sha256"] == entry["sha256"]
                and timed["case_count"] == len(entry["cases"]), "Session validation identity differs")
        audit_receipt(load(path), entry["files"], entry["cases"], validation_contract)
        validation_ids.add(entry["span_id"])
    require(validation_ids == {row["span_id"] for row in spans.values() if row["kind"] == "docker_validation"
            and row.get("run_id") == run_id and row.get("stage_index") == index}
            and {path.resolve() for path in (root / "evaluations").glob("*.json")}
                == {Path(entry["path"]).resolve() for entry in session["validations"]},
            "Orphan or omitted physical stage execution")
    record = session["accounting_record"]
    store = GitStore(bound_path := inside(record["store_path"], root))
    expected_record = {key: stage[key] for key in ("completed", "status", "selected_binding")}
    require(bound_path == root / "record.git" and store.head() == record["accepted_head"]
            and same(record["record"], expected_record)
            and same(decode(store.read_files()["record.json"]), expected_record),
            "Stage accounting record is not its exact accepted decision")
    return calls, bindings, (billing, store.head())


def _audit_shared_setup(run, *, accounting_ledger=None, qualification=None, cohort_details=False):
    root = Path(run).resolve()
    manifest = load(root / "shared-setup.json")
    mode = manifest.get("execution_mode")
    require(mode in {"rehearsal", "live"} and manifest.get("protocol") == SHARED_PROTOCOL
            and manifest.get("mode") == ("offline" if mode == "rehearsal" else "live")
            and manifest.get("validator_origin") == "physical_docker"
            and type(manifest.get("stage_index")) is int and manifest["stage_index"] == 0
            and type(manifest.get("repetition")) is int and manifest["repetition"] in range(2)
            and manifest.get("project_id") in {"warehouse", "job-queue"}
            and type(manifest.get("physical_provider_calls")) is int
            and manifest["physical_provider_calls"] == (0 if mode == "rehearsal" else 10)
            and type(manifest.get("usage_micro_usd")) is int and manifest["usage_micro_usd"] >= 0,
            "Only a complete physical-Docker shared setup can receive this certificate")
    contract = load(bound_artifact(manifest["contract"], root, root / "contract.json"))
    project = load(bound_artifact(manifest["fixture"], root, root / "fixture.json"))
    document = load(bound_artifact(manifest["timings"], root, root / "timings.json"))
    snapshot = bound_artifact(manifest["accounting"], root, root / "accounting-snapshot.sqlite")
    def frozen_snapshot():
        # Read-only SQLite can still consume a WAL or rollback journal. An
        # immutable backup certificate must bind the entire consulted state.
        for suffix in ("-wal", "-journal"):
            sidecar = Path(str(snapshot) + suffix)
            require(not sidecar.exists() or read_bytes(sidecar) == b"",
                    "Frozen accounting snapshot has an unbound WAL or rollback journal")
    frozen_snapshot()
    require(contract.get("protocol") == PROTOCOL and contract.get("status") == "cohort_implementation"
            and digest(contract) == manifest["contract_sha256"] and type(contract.get("live_enabled")) is bool
            and contract.get("full_cohort_qualification") is True and contract.get("hidden_feedback") is False
            and type(contract.get("milestones")) is int and contract["milestones"] == 2
            and type(contract.get("repetitions")) is int and contract["repetitions"] == 2
            and type(contract.get("output_tokens")) is int and contract["output_tokens"] == 12288
            and type(contract.get("case_timeout_seconds")) is int and contract["case_timeout_seconds"] == 12
            and type(contract.get("suite_timeout_seconds")) is int and contract["suite_timeout_seconds"] == 300
            and contract.get("image", "").startswith("sha256:") and len(contract["image"]) == 71
            and all(char in "0123456789abcdef" for char in contract["image"][7:])
            and set(contract.get("project_ids", [])) == {"warehouse", "job-queue"},
            "Unsupported or unbound follow-up foundation contract")
    check_roster(contract["roster"], contract["project_ids"])
    blocks = contract["block_order"]
    require(type(blocks) is list and len(blocks) == 4
            and all(type(row) is list and len(row) == 2 and type(row[1]) is int for row in blocks)
            and {tuple(row) for row in blocks} == {(pid, rep) for pid in contract["project_ids"] for rep in range(2)},
            "Exact four-block setup roster required")
    policies = {arm: dict(controller="current" if arm == "current-independent" else "improved",
        formation="sequential" if arm == "improved-sequential" else "independent", initial_builders=4, model="cheap")
        for arm in ARMS}
    limits = dict(max_reviews=4, max_repairs=2, max_new_probes=8, max_escalations=1, stagnation_reviews=2)
    require(same(contract["policies"], policies) and same(contract["limits"], limits)
            and contract["pairing"].get("stage0") == "shared-independent-pool-and-common-scouts"
            and contract["pairing"].get("later") == "arm-specific"
            and type(contract.get("improved_controller_rules", {}).get("uncertainty_focus_retries")) is int
            and contract["improved_controller_rules"]["uncertainty_focus_retries"] == 1
            and set(contract["sources"]) == SOURCES
            and set(contract["extra_sources"]) == EXTRA_SOURCES,
            "Frozen policy, limits, pairing or source inventory changed")
    for name, expected in contract["sources"].items():
        require(sha(root / "source-snapshot" / "gossip_harness" / name) == expected,
                "Frozen scientific source changed: " + name)
    for name, expected in contract["extra_sources"].items():
        require(sha(root / "source-snapshot" / name) == expected, "Frozen qualification source changed")
        if name != "analysis/qualify_continuation_followup.py":
            require(sha(Path(__file__).resolve().parent.parent / name) == expected,
                    "Executing setup auditor dependency differs from qualified source")
    require(sha(Path(__file__).resolve().parent.parent / "gossip_harness" / "gitstore.py")
            == contract["sources"]["gitstore.py"], "Executing setup Git reader differs from qualified source")
    plan = load(root / "study-plan.json")
    require(sha(root / "study-plan.json") == contract["plan_sha256"] and plan["protocol"] == PROTOCOL
            and same(plan["roster"], contract["roster"]) and same(plan["policies"], policies)
            and same(plan["budget"], contract["budget"]) and same(plan["pairing"], contract["pairing"])
            and same(plan["improved_controller_rules"], contract["improved_controller_rules"])
            and same(plan["block_order"], contract["block_order"])
            and same(plan["projects"], contract["project_ids"])
            and same(plan["milestones"], contract["milestones"])
            and same(plan["repetitions"], contract["repetitions"])
            and all(type(plan["controller"][key]) is int and plan["controller"][key] == value
                    for key, value in {**limits, "output_tokens": 12288, "scouts": 2, "proposals_per_scout": 4}.items()),
            "Plan and execution opportunities differ")
    budget = contract["budget"]
    money_keys = ("incremental_cap_micro_usd", "shared_cumulative_cap_micro_usd", "expected_starting_usage_micro_usd")
    require(all(type(budget.get(key)) is int and budget[key] >= 0 for key in money_keys)
            and 0 < budget[money_keys[0]] <= budget[money_keys[1]] - budget[money_keys[2]],
            "Frozen budget bounds differ from the plan's integer commitment constraint")
    pid, repetition = manifest["project_id"], manifest["repetition"]
    block_id = f"{pid}-{repetition}"
    seed = int(digest([PROTOCOL, pid, repetition, 0])[:16], 16)
    require(project["id"] == pid and manifest["block_id"] == block_id
            and type(manifest["candidate_seed"]) is int and manifest["candidate_seed"] == seed
            and manifest["initial_files_sha256"] == digest(project["initial_files"])
            and contract["fixtures"][pid]["fixture_sha256"] == digest(project)
            and contract["fixtures"][pid]["baseline_sha256"] == digest(project["initial_files"])
            and manifest["generation_order"] in [list(FORMATIONS), list(reversed(FORMATIONS))]
            and same(manifest["generation_order"], plan["generation_order"])
            and set(manifest["pools"]) == set(FORMATIONS)
            and same(manifest["attribution"], {arm: policies[arm]["formation"] for arm in ARMS}),
            "Shared source, seed, formation order or arm attribution changed")
    proof = manifest["baseline_qualification"]
    expected_approval = dict(files_sha256=digest(project["initial_files"]), source_valid=True,
                            approval_id="qualified-baseline:" + digest(proof))
    require(same(manifest["baseline_approval"], expected_approval), "Shared baseline approval changed")
    all_session_ids = [f"{project_id}-{rep}-shared-{kind}" for project_id in contract["project_ids"]
                       for rep in range(2) for kind in (*FORMATIONS, "scouts")]
    all_block_ids = [f"{project_id}-{rep}-shared" for project_id in contract["project_ids"] for rep in range(2)]
    all_spans = audit_timings(document, manifest, all_session_ids + all_block_ids)
    session_ids = {f"{block_id}-shared-{kind}" for kind in (*FORMATIONS, "scouts")}
    spans = {key: row for key, row in all_spans.items() if row.get("run_id") in session_ids}
    require(spans and all(row["kind"] in {"worker_request", "scripted_worker", "physical_provider_request", "docker_validation", "scout_batch"}
            and type(row.get("stage_index")) is int and row["stage_index"] == 0
            and (row["status"] == "finished" or row["status"] == "failed"
                 and row["kind"] in {"worker_request", "scripted_worker", "physical_provider_request"})
            and row["finished_monotonic_ns"] <= manifest["frozen_monotonic_ns"] for row in spans.values()),
            "Shared setup contains orphan, private, failed or unfinished execution")
    validation_contract = {**contract, "adapter_sha256": _adapter_hash(read_bytes(
        root / "source-snapshot" / "gossip_harness" / "blackbox_validator.py").decode())}
    if qualification is None:
        fixtures = load(bound_artifact(manifest["fixtures"], root, root / "fixtures.json"))["projects"]
        projects = {item["id"]: item for item in fixtures}
        require(len(fixtures) == len(projects) == 2 and set(projects) == set(contract["project_ids"])
                and all(digest(item) == contract["fixtures"][pid]["fixture_sha256"] for pid, item in projects.items()),
                "Shared setup qualification fixture roster changed")
        qualification = audit_qualification(manifest["qualification"], contract,
            projects, validation_contract)
    require(same(proof, qualified_baseline_proof(qualification, pid)),
            "Shared baseline approval lacks the exact physically qualified baseline")
    prompts = _prompts(read_bytes(root / "source-snapshot" / "gossip_harness" / "continuation_controller.py").decode())
    runner_ast = ast.parse(read_bytes(root / "source-snapshot" / "gossip_harness" / "benchmark_experiment.py").decode())
    scout_function, = [node for node in runner_ast.body if isinstance(node, ast.FunctionDef) and node.name == "scout_probes"]
    scout_prompt, = [ast.literal_eval(node.value) for node in scout_function.body if isinstance(node, ast.Assign)
                    and any(isinstance(target, ast.Name) and target.id == "instructions" for target in node.targets)]
    bindings, billing_heads, sessions = [], [], {}
    for kind in (*FORMATIONS, "scouts"):
        directory = root / kind
        group = manifest["pools"][kind] if kind in FORMATIONS else manifest["scouts"]
        session = load(bound_artifact(group["session"], root, directory / "session.json"))
        session_id = f"{block_id}-shared-{kind}"
        billing = manifest["namespace"] + "/" + session_id
        require(session["protocol"] == SHARED_PROTOCOL and session["mode"] == manifest["mode"]
                and session["execution_mode"] == mode and session["session_kind"] == "shared_setup"
                and session["session_id"] == session_id and session["project_id"] == pid
                and type(session["stage_index"]) is int and session["stage_index"] == 0
                and session["billing"] == billing and session["contract_sha256"] == digest(contract)
                and session["image"] == contract["image"] and session["validator_origin"] == "physical_docker"
                and type(session["physical_provider_calls"]) is int
                and session["physical_provider_calls"] == (0 if mode == "rehearsal" else 2 if kind == "scouts" else 4)
                and type(session["usage_micro_usd"]) is int and session["usage_micro_usd"] >= 0
                and session["initial_files_sha256"] == digest(project["initial_files"])
                and same(GitStore(directory / "baseline.git").read_files(), project["initial_files"]),
                "Session identity, source, execution origin or zero-API status changed")
        calls, call_bindings = audit_calls(directory, session["invocations"], billing, contract,
            mode, spans, session_id, 0)
        bindings.extend(call_bindings)
        require(all(row["execution_kind"] == ("scripted_worker" if mode == "rehearsal" else "physical_provider_request")
                    and row["journal_replay"] is False for _, row in calls.values())
                and session["usage_micro_usd"] == sum(row["usage_units"] for _, row in calls.values()),
                "Shared setup execution mode or usage differs from physical calls")
        record = session["accounting_record"]
        store_path = inside(record["store_path"], directory)
        store = GitStore(store_path)
        require(store_path == directory / "record.git" and store.head() == record["accepted_head"]
                and same(record["record"], dict(completed=False, status="shared_initial_setup", selected_binding=None))
                and same(store.read_files(), {"record.json": json.dumps(record["record"], sort_keys=True)}),
                "Setup accounting promotion differs or claims project acceptance")
        billing_heads.append((billing, store.head()))
        sessions[kind] = (session, calls)
    pool_audits, freezes = [], {}
    containers, receipt_paths = set(), set()
    for formation in FORMATIONS:
        directory, group = root / formation, manifest["pools"][formation]
        frozen = load(bound_artifact(group["initial_pool"], root, directory / "initial-pool.json"))
        session, calls = sessions[formation]
        require(frozen["protocol"] == SHARED_PROTOCOL and frozen["session_id"] == session["session_id"]
                and type(frozen["stage_index"]) is int and frozen["stage_index"] == 0
                and type(frozen["frozen_monotonic_ns"]) is int
                and frozen["pool_sha256"] == group["pool_sha256"] == digest(frozen["pool"])
                and same(session["result"]["initial_freeze"], dict(freeze_id=str(directory / "initial-pool.json"),
                    pool_sha256=frozen["pool_sha256"]))
                and all(row["finished_monotonic_ns"] <= frozen["frozen_monotonic_ns"]
                        for row in spans.values() if row.get("run_id") == session["session_id"]),
                "Initial pool freeze precedes execution or has changed bindings")
        summary = audit_initial_pool(frozen["pool"], project=project, candidate_seed=seed,
            baseline_approval=expected_approval, formation=formation, calls=calls,
            contract=validation_contract, root=directory, spans=spans, run_id=session["session_id"],
            builder_prompt=prompts["builder_prompt"], initial_result=session["result"])
        exports = session["exports"]
        require(type(exports) is list and len(exports) == 4
                and len({row["label"] for row in exports}) == 4, "Initial source exports are incomplete")
        for row, alias in zip(exports, frozen["pool"]["candidate_order"]):
            item = frozen["pool"]["candidates"][alias]
            label = f"stage-0-build-{item['slot']}-1"
            saved = load(bound_artifact(row, directory, directory / (label + ".json")))
            require(row["label"] == saved["label"] == label and same(row["binding"], item["binding"])
                    and same(saved["binding"], item["binding"]) and same(saved["files"], item["files"])
                    and row["files_sha256"] == digest(item["files"]), "Export source differs from retained pool")
        validations = session["validations"]
        require(len(validations) == 4 and len({row["span_id"] for row in validations}) == 4,
                "Initial validation export inventory differs")
        for row in validations:
            path = bound_artifact(row, directory)
            span = spans[row["span_id"]]
            require(Path(span["receipt_path"]).resolve() == path and span["label"] == row["label"]
                    and same(row["cases"], public_cases(project, 0)), "Exported validation source or suite changed")
            receipt = load(path)
            container = receipt.get("container_name")
            require(type(container) is str and container and container not in containers
                    and path not in receipt_paths, "Repeated physical receipt/container cannot count as fresh execution")
            containers.add(container)
            receipt_paths.add(path)
            audit_receipt(receipt, row["files"], row["cases"], validation_contract)
        require({path.name for path in (directory / "evaluations").glob("*.json")}
                == {Path(row["path"]).name for row in validations}, "Orphan setup evaluation receipt")
        freezes[formation] = frozen
        pool_audits.append(summary)
    first, second = manifest["generation_order"]
    require(freezes[first]["frozen_monotonic_ns"] <= min(row["started_monotonic_ns"] for row in spans.values()
        if row.get("run_id") == sessions[second][0]["session_id"]), "Initial generation order changed")
    group, directory = manifest["scouts"], root / "scouts"
    barrier = load(bound_artifact(group["barrier"], root, directory / "initial-pool.json"))
    evidence = load(bound_artifact(group["proposals"], root, directory / "scouts.json"))
    session, calls = sessions["scouts"]
    expected_barrier = dict(protocol=SHARED_PROTOCOL, project_id=pid, repetition=repetition,
                            candidate_pools=manifest["pools"])
    require(barrier["protocol"] == SHARED_PROTOCOL and barrier["session_id"] == session["session_id"]
            and type(barrier["stage_index"]) is int and barrier["stage_index"] == 0
            and type(barrier["frozen_monotonic_ns"]) is int and same(barrier["pool"], expected_barrier)
            and barrier["pool_sha256"] == digest(expected_barrier)
            and max(row["frozen_monotonic_ns"] for row in freezes.values()) <= barrier["frozen_monotonic_ns"]
            and set(calls) == {"scout-scout-0-1", "scout-scout-1-1"}
            and session["exports"] == session["validations"] == []
            and not list((directory / "evaluations").glob("*.json"))
            and not any(row["kind"] == "docker_validation" and row["run_id"] == session["session_id"]
                        for row in spans.values()), "Common scout barrier or call inventory differs")
    context = dict(project_id=pid, stage_index=0,
        milestones=[{key: project["stages"][0][key] for key in ("requirements", "specification", "spec")
                     if key in project["stages"][0]}], public_cases=cases_for(project, 0, "visible"))
    inputs = {**project["initial_files"], "scout-context.json": json.dumps(context, sort_keys=True)}
    proposals, parses = [], []
    for index in range(2):
        request, call = calls[f"scout-scout-{index}-1"]
        require(call["role"] == "scout" and call["model"] == "cheap"
                and same(request["files"], inputs) and request["feedback"] == ""
                and request["base_sha"] == GitStore(directory / "baseline.git").head()
                and type(request["attempt"]) is int and request["attempt"] == 1
                and same(call["controller_metadata"], dict(role="scout", round=1,
                    candidate_id=f"scout-{index}", scout_index=index, stage_index=0,
                    project_id=pid, phase="post_initial_freeze"))
                and request["allowed_paths"] == ["probes.json"] and request["instructions"] == scout_prompt
                and spans[call["request_span_id"]]["started_monotonic_ns"] >= barrier["frozen_monotonic_ns"],
                "Scout ran before both pools froze or received candidate/private context")
        probes = scout_proposals(call)
        proposals.append(dict(scout_id=f"scout-{index}", probes=probes))
        try:
            changes = call.get("changes", {})
            content = changes["probes.json"]
            require(set(changes) == {"probes.json"} and type(content) is str
                    and len(content.encode()) <= 65536, "Malformed scout envelope")
            parsed = decode(content)
            require(type(parsed) is dict and set(parsed) == {"probes"} and type(parsed["probes"]) is list
                    and len(parsed["probes"]) <= 4, "Malformed scout envelope")
            parse = dict(status="parsed", proposed_count=len(parsed["probes"]))
        except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
            parse = dict(status="rejected_batch", proposed_count=None, reason="missing_or_malformed_scout_envelope")
        parses.append(dict(scout_id=f"scout-{index}", **parse))
    expected_evidence = dict(calls=2, parses=parses,
        rejected_batches=sum(row["status"] == "rejected_batch" for row in parses),
        source_sha256=digest(project["initial_files"]), shared_input_sha256=digest(inputs),
        initial_pool_sha256=digest(expected_barrier), initial_pool_artifact_sha256=sha(directory / "initial-pool.json"),
        proposals=proposals)
    require(same(evidence, expected_evidence)
            and same(session["result"], dict(scouts=proposals, evidence=expected_evidence)),
            "Common scout proposals or parse outcomes changed")
    batches = [row for row in spans.values() if row["kind"] == "scout_batch"]
    require(len(batches) == 1 and batches[0]["run_id"] == session["session_id"]
            and batches[0]["initial_pool_sha256"] == digest(expected_barrier)
            and all(batches[0]["started_monotonic_ns"] <= spans[call["request_span_id"]]["started_monotonic_ns"]
                    <= spans[call["request_span_id"]]["finished_monotonic_ns"] <= batches[0]["finished_monotonic_ns"]
                    for _, call in calls.values()), "Common scout batch timing differs")
    accounting = audit_accounting(snapshot, bindings, billing_heads, [], manifest,
                                  study_namespace=manifest["namespace"])
    shared_invocations = [call for session, _ in sessions.values() for call in session["invocations"]]
    allowed_failed = known_failure_spans(shared_invocations, spans, mode)
    require({key for key, row in spans.items() if row["status"] == "failed"} == allowed_failed,
            "Shared setup has a failure outside its audited settled call outcomes")
    frozen_snapshot()
    current_accounting = None
    if accounting_ledger is not None and Path(accounting_ledger).resolve() != snapshot:
        current_accounting = audit_accounting(accounting_ledger, bindings, billing_heads, [], manifest,
                                              study_namespace=manifest["namespace"])
    require(accounting["study_usage_micro_usd"] == manifest["usage_micro_usd"]
            and (mode == "live" or manifest["usage_micro_usd"] == 0) and len(bindings) == 10,
            "Shared setup duplicated or omitted physical scripted calls or charges")
    result = dict(protocol=AUDIT_PROTOCOL, passed=True, root=str(root), auditor_sha256=sha(__file__),
        manifest_sha256=sha(root / "shared-setup.json"), contract_sha256=digest(contract),
        project_id=pid, repetition=repetition, initial_pools=pool_audits,
        mode=mode, scripted_calls=10 if mode == "rehearsal" else 0,
        physical_provider_calls=10 if mode == "live" else 0, public_docker_executions=8,
        physical_scout_calls=2 if mode == "live" else 0, scripted_scout_calls=2 if mode == "rehearsal" else 0,
        usage_micro_usd=manifest["usage_micro_usd"], accounting=accounting,
        current_accounting=current_accounting,
        scope=dict(shared_initial_setup=True, primary_cohort=False, controller_decision_replay=False,
                   prospective_arm_execution=False, oracle_probe_admission=False, fixture_qualification=False),
        limitations=["Trusted-host retained consistency, not external runtime attestation.",
            "This is one setup block, not a complete twelve-trajectory quality result.",
            "Shared setup source provenance never licenses receipt reuse or charges for imported arm opportunities.",
            "Controller routing, later milestone execution, final private barriers and project acceptance remain unaudited."])
    if cohort_details:
        result["_details"] = dict(bindings=bindings, billing_heads=billing_heads,
            calls=shared_invocations,
            manifest=manifest, spans=spans)
    return result


def audit_shared_setup(run, *, accounting_ledger=None):
    reads = EvidenceReads()
    token = _READ_SET.set(reads)
    try:
        result = _audit_shared_setup(run, accounting_ledger=accounting_ledger)
        reads.verify_unchanged()
        result["consumed_evidence_files"] = len(reads.hashes)
        result["consumed_evidence_sha256"] = digest({str(path): value for path, value in sorted(reads.hashes.items())})
        return result
    finally:
        _READ_SET.reset(token)


def check_artifact_catalog(row, root):
    """A global freeze includes every retained JSON record and Git proposal."""
    paths = [bound_artifact(binding, root) for binding in row["artifact_files"]]
    expected = {path.resolve() for path in root.rglob("*") if path.is_file()
                and path.suffix in {".json", ".jsonl", ".py"}
                and not any(part.endswith(".git") for part in path.relative_to(root).parts)}
    require(len(paths) == len(set(paths)) and set(paths) == expected,
            "Global freeze omitted or duplicated a retained source/request/probe artifact")
    bindings = row["git_bindings"]
    require(len({(entry["store_path"], entry["tip_sha"]) for entry in bindings}) == len(bindings),
            "Duplicate frozen Git provenance")
    for entry in bindings:
        require("accepted_head" in entry, "Frozen Git source lacks accepted-ref binding")
        git_files(entry, root)


def expected_shared_import(manifest_binding, manifest, policy):
    formation = "sequential" if policy == "improved-sequential" else "independent"
    group = manifest["pools"][formation]
    frozen = load(group["initial_pool"]["path"])
    scouts = load(manifest["scouts"]["proposals"]["path"])
    envelope = dict(pool=frozen["pool"], pool_sha256=frozen["pool_sha256"],
        initial_files_sha256=manifest["initial_files_sha256"], candidate_seed=manifest["candidate_seed"],
        purpose="shared_initial_setup")
    attribution = dict(purpose="shared_initial_setup_import", manifest=manifest_binding, policy=policy,
        formation=formation, initial_pool=group["initial_pool"], scouts=manifest["scouts"]["proposals"],
        physical_provider_calls=0, charged_micro_usd=0, attributed_builder_opportunities=4,
        attributed_scout_opportunities=2,
        receipt_policy="Imported source provenance only; every arm executes fresh source-bound validation.")
    return dict(pool=frozen["pool"], binding=envelope, scouts=scouts), attribution


def check_rehearsal_mechanisms(case, stages):
    """The stage replay has already derived these counts from raw decisions."""
    require(case["accepted"] is True and case["milestones_completed"] == 2,
            "A matching infrastructure rehearsal must finish and independently accept all milestones")
    for index, stage in enumerate(stages):
        metrics = stage["metrics"]
        improved = case["policy"] != "current-independent"
        focused = improved and case["repetition"] == 1 and index == 1
        require(metrics["initial_builder_calls"] == (0 if index == 0 else 4)
                and metrics["shared_initial_builder_opportunities"] == (4 if index == 0 else 0)
                and metrics["repairs"] == (0 if focused else 2)
                and metrics["reviewer_calls"] == (2 if focused else 3 if improved else 4),
                "Rehearsal did not execute its declared continuation opportunities")
        if improved:
            if focused:
                require(metrics["focused_review_requests"] == 1,
                        "Rehearsal never executed the bounded focused-review path")
            else:
                require(metrics["retained_repair_checkpoints"] == 2 and metrics["blocked_acceptances"] == 2
                        and metrics["escalations"] == 1,
                        "Rehearsal did not retain repairs or exercise blocked acceptance and bounded escalation")
                if case["repetition"] == 0:
                    require(metrics["repair_regressions"] >= 1 and metrics["introduced_failures"] >= 1
                            and metrics["resolved_known_failures"] >= 1,
                            "Rehearsal lacks a source-bound exchanged-failure regression")
                else:
                    failures = [call for call in stage["invocations"] if "failure" in call
                                and call["role"] == "builder" and call["usage_units"] == 0
                                and call["metadata"].get("rehearsal_noop") is True]
                    require(len(failures) == 1 and metrics["retained_eligibility_preserved"] >= 1,
                            "Rehearsal did not exercise a known no-op failure with retained source eligibility")
        require(stage["scouts"]["attributed_calls"] == 2 and stage["scouts"]["rejected"] >= 4,
                "Rehearsal scout rejection controls are missing")


def _audit_run(run, *, accounting_ledger=None):
    from analysis.audit_followup_stage import audit_stage, controller_prompts

    run = Path(run).resolve()
    if run.is_file():
        require(run.name == "results.json", "Pass a study directory or results.json")
        run = run.parent
    report, prereg, plan = load(run / "results.json"), load(run / "preregistered.json"), load(run / "study-plan.json")
    require(report.get("experiment") == PROTOCOL and report.get("status") == report.get("phase") == "finished"
            and report.get("mode") in {"rehearsal", "live"} and report.get("active_case") is None
            and report.get("censored") == report.get("unexecuted") == []
            and same(report.get("unsettled"), dict(reservations=0, promotions=0)),
            "Only a fully finalized twelve-trajectory cohort can be certified under this contract")
    mode = report["mode"]
    contract = report["contract"]
    check_cohort_contract(contract, plan, run)
    contract_sha = digest(contract)
    require(report["contract_sha"] == prereg["contract_sha"] == contract_sha
            and same(load(run / "contract.json"), contract) and same(prereg["contract"], contract)
            and same(prereg["budget_before"], report["budget_before"])
            and prereg["mode"] == mode and same(prereg["rehearsal"], report["rehearsal"]),
            "Preregistered contract, execution mode or budget changed")
    if mode == "live":
        funding = plan["budget"].get("funding", {})
        require(contract["live_enabled"] is True and plan.get("readiness", {}).get("live_authorized") is True
                and funding.get("status") == "approved_capped_cohort_admission"
                and funding.get("policy") == "bounded_budget_termination_no_incomplete_comparison"
                and type(funding.get("approved_incremental_micro_usd")) is int
                and funding["approved_incremental_micro_usd"] == contract["budget"]["incremental_cap_micro_usd"]
                and funding["approved_incremental_micro_usd"] >= max(4 * contract["models"]["cheap"]["reservation_units"],
                                                                   contract["models"]["strong"]["reservation_units"]),
                "Live execution is outside the frozen funded admission contract")
        proof_binding = report["rehearsal"]
        proof_path = Path(proof_binding["results"]["path"]).resolve()
        proof = load(proof_path)
        require(proof_path.name == "results.json" and proof_path.parent != run
                and sha(proof_path) == proof_binding["results"]["sha256"]
                and proof.get("mode") == "rehearsal" and same(proof.get("contract"), contract),
                "Live cohort lacks a complete matching rehearsal")
        proof_audit = _audit_run(proof_path.parent)
        audit_path = Path(proof_binding["audit"]["path"]).resolve()
        saved_audit = load(audit_path)
        require(sha(audit_path) == proof_binding["audit"]["sha256"] and saved_audit.get("passed") is True
                and saved_audit.get("mode") == "rehearsal" and saved_audit.get("scope", {}).get("primary_cohort") is True
                and all(saved_audit.get(key) == proof_audit[key] for key in
                        ("protocol", "results_sha256", "timings_sha256", "freeze_manifest_sha256", "contract_sha256", "auditor_sha256")),
                "Retained rehearsal certificate does not bind independently reaudited evidence")
    else:
        require(report["rehearsal"] is None, "A rehearsal cannot borrow upstream approval")
    namespace = PROTOCOL + "/" + digest([str(run), contract_sha])[:24]
    require(report["namespace"] == prereg["namespace"] == namespace
            and Path(report["root"]).resolve() == run, "Cohort identity is not bound to its root and contract")
    fixtures = load(run / "fixtures.json")["projects"]
    projects = {project["id"]: project for project in fixtures}
    require(len(fixtures) == len(projects) == 2 and [project["id"] for project in fixtures] == contract["project_ids"]
            and set(contract["fixtures"]) == set(projects), "Frozen project roster changed")
    for pid, project in projects.items():
        require(len(project["stages"]) == 2 and digest(project) == contract["fixtures"][pid]["fixture_sha256"]
                and digest(project["initial_files"]) == contract["fixtures"][pid]["baseline_sha256"],
                "Frozen application source or acceptance fixture changed")
    validation_contract = {**contract, "adapter_sha256": _adapter_hash(read_bytes(
        run / "source-snapshot" / "gossip_harness" / "blackbox_validator.py").decode())}
    qualification = audit_qualification(report["qualification"], contract, projects, validation_contract)
    require(same(prereg["qualification"], report["qualification"])
            and same(prereg["baseline_qualifications"], report["baseline_qualifications"])
            and same(report["baseline_qualifications"], {pid: qualified_baseline_proof(qualification, pid) for pid in projects}),
            "Preregistered physical baseline eligibility changed")
    runtime = qualification["report"]["runtime_identity"]
    require(same(report["runtime_identity"], runtime) and same(prereg["runtime_identity"], runtime)
            and same(report["runtime_after"], runtime)
            and report["runtime_identity_sha256"] == prereg["runtime_identity_sha256"]
                == report["runtime_after_sha256"] == digest(runtime),
            "Qualified Docker runtime differs before or after cohort execution")
    roster, blocks = contract["roster"], contract["block_order"]
    run_ids = [f"{pid}-{policy}-{rep}" for pid, policy, rep in roster]
    shared_ids = [f"{pid}-{rep}-shared-{kind}" for pid, rep in blocks for kind in (*FORMATIONS, "scouts")]
    block_ids = [f"{pid}-{rep}-shared" for pid, rep in blocks]
    require(same([[row["project_id"], row["policy"], row["repetition"]] for row in report["cases"]], roster)
            and [row["run_id"] for row in report["cases"]] == run_ids
            and [row["run_id"] for row in report["trajectories"]] == run_ids
            and same([[row["project_id"], row["repetition"]] for row in report["shared_setups"]], blocks),
            "Final ordered cohort or shared-setup roster changed")
    freeze = load(run / "frozen-trajectories.json")
    freeze_sha = sha(run / "frozen-trajectories.json")
    require(report["freeze_manifest_sha256"] == freeze_sha and freeze["contract_sha"] == contract_sha
            and same([[row["project_id"], row["repetition"]] for row in freeze["shared_setups"]], blocks),
            "Global freeze omitted a shared block or changed contract")
    for key, name in (("fixtures", "fixtures.json"), ("plan", "study-plan.json"), ("contract", "contract.json")):
        bound_artifact(freeze[key], run, run / name)
    source_paths = [bound_artifact(row, run) for row in freeze["sources"]]
    require(len(source_paths) == len(set(source_paths))
            and set(source_paths) == {run / "source-snapshot" / "gossip_harness" / name for name in contract["sources"]}
                | {run / "source-snapshot" / name for name in contract["extra_sources"]},
            "Global freeze does not bind every source and independent audit helper")
    document = load(run / "timings.json")
    spans = audit_timings(document, freeze, run_ids + shared_ids + block_ids)
    reported_calls = [call for group in report["shared_setups"] + report["cases"] for call in group["invocations"]]
    allowed_failed = known_failure_spans(reported_calls, spans, mode)
    check_cohort_barrier(freeze, run_ids, shared_ids + block_ids, spans, allowed_failed)
    receipt_paths, containers = check_fresh_receipts(spans, run, contract)
    require(containers.isdisjoint(qualification["containers"]),
            "Fixture qualification was reused as an independent arm or final judgment")
    require({path.parent.name for path in run.glob("*/config.json")} == set(run_ids), "Unlinked trajectory directory")
    source = read_bytes(run / "source-snapshot" / "gossip_harness" / "continuation_controller.py").decode()
    prompts = controller_prompts(source)
    runner_tree = ast.parse(read_bytes(run / "source-snapshot" / "gossip_harness" / "benchmark_experiment.py").decode())
    scout_function, = [node for node in runner_tree.body if isinstance(node, ast.FunctionDef) and node.name == "scout_probes"]
    prompts["scout_prompt"], = [ast.literal_eval(node.value) for node in scout_function.body if isinstance(node, ast.Assign)
                               and any(isinstance(target, ast.Name) and target.id == "instructions" for target in node.targets)]
    all_bindings, billing_heads, all_calls, shared_audits, shared_manifests = [], [], [], [], {}
    last_setup_finished = 0
    for summary, frozen in zip(report["shared_setups"], freeze["shared_setups"]):
        pid, rep = summary["project_id"], summary["repetition"]
        directory = run / "shared" / f"{pid}-{rep}"
        require(same(summary["manifest"], frozen["manifest"]), "Shared summary and cohort freeze differ")
        bound_artifact(frozen["manifest"], run, directory / "shared-setup.json")
        check_artifact_catalog(frozen, directory)
        checked = _audit_shared_setup(directory, qualification=qualification, cohort_details=True)
        details = checked.pop("_details")
        manifest = details["manifest"]
        require(manifest["namespace"] == namespace + f"/{pid}-{rep}"
                and manifest["contract_sha256"] == checked["contract_sha256"] == contract_sha
                and manifest["cohort_controls"] is True and manifest["execution_mode"] == mode
                and same(load(directory / "fixture.json"), projects[pid])
                and same(manifest["qualification"], report["qualification"])
                and same(summary["invocations"], details["calls"])
                and summary["usage_micro_usd"] == checked["usage_micro_usd"]
                and summary["physical_provider_calls"] == checked["physical_provider_calls"],
                "Shared setup was duplicated, recharged or detached from its block")
        for span_id, row in details["spans"].items():
            require(same(row, spans[span_id]), "Shared timing snapshot differs from the final study clock")
        timed = spans[summary["span_id"]]
        require(timed["kind"] == "shared_setup_block" and timed["run_id"] == f"{pid}-{rep}-shared"
                and timed["started_monotonic_ns"] >= last_setup_finished
                and timed["finished_monotonic_ns"] >= manifest["frozen_monotonic_ns"]
                and same(summary["elapsed_seconds"], timed["elapsed_seconds"]), "Shared setup timing or block order differs")
        last_setup_finished = timed["finished_monotonic_ns"]
        all_bindings.extend(details["bindings"])
        billing_heads.extend(details["billing_heads"])
        all_calls.extend(details["calls"])
        shared_audits.append(checked)
        shared_manifests[(pid, rep)] = (frozen["manifest"], manifest)
    last_trajectory_finished, audited, actual_stages = last_setup_finished, [], set()
    for case, frozen, timing in zip(report["cases"], freeze["trajectories"], report["trajectories"]):
        run_id = case["run_id"]
        root = run / run_id
        config, state = load(root / "config.json"), load(root / "trajectory.json")
        bound_artifact(frozen["config"], run, root / "config.json")
        project = projects[case["project_id"]]
        require(all(same(config[key], state[key]) and same(state[key], case[key]) and same(case[key], frozen[key])
                    for key in ("run_id", "project_id", "policy", "repetition"))
                and config["root"] == case["root"] == str(root) and config["mode"] == report["mode"]
                and config["image"] == contract["image"] and config["namespace"] == namespace + "/" + run_id
                and config["contract_sha"] == state["contract_sha"] == contract_sha
                and same(load(root / "state.json"), state) and same(load(root / "result.json"), case),
                "Trajectory/config/result identity or terminal state changed")
        path = bound_artifact(frozen["trajectory"], run, root / "trajectory.json")
        require(sha(path) == case["trajectory_sha256"] == timing["trajectory_sha256"]
                and digest(state["files"]) == case["files_sha256"] == frozen["files_sha256"]
                and same(state["final_binding"], frozen["final_binding"])
                and same(git_files(state["final_binding"], root), state["files"]),
                "Terminal source or Git accepted ref differs from the global freeze")
        timed = spans[timing["span_id"]]
        require(timed["kind"] == "model_trajectory" and timed["run_id"] == run_id
                and timing["terminal"] == state["terminal"] and same(timing["elapsed_seconds"], timed["elapsed_seconds"])
                and timed["started_monotonic_ns"] >= last_trajectory_finished,
                "Trajectory timing, source-freeze ordering or terminal classification differs")
        last_trajectory_finished = timed["finished_monotonic_ns"]
        stages = state["stages"]
        require(1 <= len(stages) <= 2 and len(frozen["stage_artifacts"]) == len(state["stage_sessions"]) == len(stages)
                and {path.name for path in root.glob("stage-*") if path.is_dir()} == {f"stage-{index}" for index in range(len(stages))},
                "Skipped or orphan milestone directory")
        initial = project["initial_files"]
        prior: dict[str, Any] = {}
        flat_calls, stage_audits = [], []
        base = GitStore(root / "initial.git")
        require(same(base.read_files(), initial), "Initial project Git source changed")
        manifest_binding, manifest = shared_manifests[(case["project_id"], case["repetition"])]
        shared, attribution = expected_shared_import(manifest_binding, manifest, case["policy"])
        require(same(state["shared_import"], attribution) and same(case["shared_import"], attribution),
                "Arm changed shared source/probe attribution or charged an import")
        previous_stage_finished = timed["started_monotonic_ns"]
        for index, (stage, artifact, session_binding) in enumerate(zip(stages, frozen["stage_artifacts"], state["stage_sessions"])):
            stage_root = root / f"stage-{index}"
            require(type(stage.get("stage_index")) is int and stage["stage_index"] == artifact["stage_index"] == index
                    and stage["policy"] == case["policy"] and stage["initial_files_sha256"] == digest(initial)
                    and same(stage.get("shared_import"), attribution if index == 0 else None)
                    and same(artifact["session"], session_binding), "Milestone policy, baseline or shared import changed")
            for key, name in (("result", "result.json"), ("session", "session.json"), ("initial_pool", "initial-pool.json"), ("scouts", "scouts.json")):
                bound_artifact(artifact[key], run, stage_root / name)
            check_artifact_catalog(artifact, stage_root)
            session = load(stage_root / "session.json")
            require(same(load(stage_root / "result.json"), stage), "Frozen stage result differs from trajectory")
            approval = manifest["baseline_approval"] if index == 0 else dict(files_sha256=digest(initial), source_valid=True, approval_id=base.head())
            require(same(stage["baseline_approval"], approval), "Later stage source eligibility is not its own accepted lineage")
            seed = int(digest([PROTOCOL, project["id"], case["repetition"], index])[:16], 16)
            aliases = ["candidate-A", "candidate-B", "candidate-C", "candidate-D"]
            random.Random(digest([project["id"], index, seed, "candidate-aliases"])).shuffle(aliases)
            require(same(stage["initial_pool"]["alias_map"], {f"slot-{slot}": alias for slot, alias in enumerate(aliases)})
                    and stage["initial_pool"]["candidate_order"] == aliases,
                    "Candidate assignment differs from the preregistered block/stage seed")
            calls, bindings, billing_head = audit_stage_session(session, stage, stage_root, project, initial,
                contract, validation_contract, spans, run_id, index, config["namespace"] + f"/stage-{index}", mode)
            stage_audits.append(audit_stage(stage, project, index, case["policy"], initial, prior, calls,
                validation_contract, stage_root, lambda binding: git_files(binding, root), spans, run_id, prompts,
                shared=shared if index == 0 else None))
            stage_spans = [row for row in spans.values() if row["kind"] == "stage" and row.get("run_id") == run_id
                           and row.get("stage_index") == index]
            require(len(stage_spans) == 1 and previous_stage_finished <= stage_spans[0]["started_monotonic_ns"]
                    <= stage_spans[0]["finished_monotonic_ns"] <= timed["finished_monotonic_ns"],
                    "Milestone execution is not inside its own trajectory interval")
            outer = stage_spans[0]
            for child in spans.values():
                if child.get("run_id") == run_id and child.get("stage_index") == index:
                    require(outer["started_monotonic_ns"] <= child["started_monotonic_ns"]
                            <= child["finished_monotonic_ns"] <= outer["finished_monotonic_ns"],
                            "Stage timing excludes part of its actual execution work")
            initial_freeze = load(stage_root / "initial-pool.json")["frozen_monotonic_ns"]
            require(type(initial_freeze) is int and outer["started_monotonic_ns"] <= initial_freeze <= outer["finished_monotonic_ns"],
                    "Initial source freeze is outside the actual stage interval")
            previous_stage_finished = outer["finished_monotonic_ns"]
            if stage["completed"]:
                audit_local_promotion(stage_root / "checkpoint.git", base.head(), stage["files"], project["allowed_paths"])
                base = GitStore(stage_root / "checkpoint.git")
            else:
                require(index == len(stages) - 1 and not (stage_root / "checkpoint.git").exists(),
                        "Incomplete milestone advanced its accepted source or continued")
            actual_stages.add((run_id, index))
            all_bindings.extend(bindings)
            billing_heads.append(billing_head)
            flat_calls.extend(stage["invocations"])
            initial, prior = stage["files"], stage
        require(same(flat_calls, case["invocations"]) and frozen["terminal"] == state["terminal"]
                and frozen["milestones_completed"] == case["milestones_completed"]
                and frozen["evidence_sha256"] == digest(stages[-1]["evidence_cases"])
                and case["usage_micro_usd"] == sum(call["usage_units"] for call in flat_calls)
                and case["physical_provider_calls"] == sum(call["physical_dispatch"] for call in flat_calls)
                and case["freeze_manifest_sha256"] == freeze_sha, "Arm usage, source freeze or milestone total differs")
        metrics = {key: sum(stage["metrics"].get(key, 0) for stage in stages) for key in {key for stage in stages for key in stage["metrics"]}}
        scout_totals = {key: sum(stage["scouts"][key] for stage in stages)
                        for key in ("calls", "attributed_calls", "admitted", "rejected", "rejected_batches")}
        require(same(case["metrics"], metrics) and same(case["scouts"], scout_totals), "Case totals differ from independently replayed stages")
        accepted, suites = check_primary_outcome(case, state, project, validation_contract)
        require(case["status"] == ("accepted" if accepted else "final_quality_failed" if state["terminal"] == "visible_complete" else "bounded_incomplete"),
                "Final status hides an incomplete or rejected trajectory")
        for kind, cases in suites.items():
            path = root / f"final-{kind}-receipt.json"
            require(same(load(path), case["final_hidden" if kind == "private" else "final_visible"]), "Final raw receipt differs from summary")
            matches = [row for row in spans.values() if row["kind"] == "docker_validation" and row.get("run_id") == run_id
                       and row.get("purpose") == "final_" + kind]
            require(len(matches) == 1 and matches[0]["freeze_manifest_sha256"] == freeze_sha
                    and Path(matches[0]["receipt_path"]).resolve() == path and matches[0]["case_count"] == len(cases),
                    "Final independent judgment lacks the exact global freeze and suite binding")
        if accepted:
            release_head = audit_local_promotion(root / "release.git", GitStore(root / "initial.git").head(), state["files"], project["allowed_paths"])
            require(case["release_head"] == release_head, "Release differs from the independently accepted source")
        else:
            require(case["release_head"] is None and not (root / "release.git").exists(), "Rejected project was released")
        if mode == "rehearsal":
            check_rehearsal_mechanisms(case, stages)
        all_calls.extend(flat_calls)
        audited.append(dict(run_id=run_id, accepted=accepted, milestones_completed=case["milestones_completed"], stages=stage_audits))
    for timed in spans.values():
        if timed.get("run_id") in run_ids and "stage_index" in timed:
            require(type(timed["stage_index"]) is int and (timed["run_id"], timed["stage_index"]) in actual_stages,
                    "Orphan execution targets a missing milestone")
    require(len(all_calls) <= 256 and sum(call["model"] == "cheap" for call in all_calls) <= 160
            and sum(call["model"] == "strong" for call in all_calls) <= 120,
            "Physical call inventory exceeded the preregistered envelope")
    require(same(all_calls, reported_calls) and known_failure_spans(all_calls, spans, mode) == allowed_failed,
            "Failed dispatch span is not bound to a settled audited call")
    snapshot = run / "accounting-snapshot.sqlite"
    check_frozen_snapshot(snapshot)
    sha(snapshot)
    accounting = audit_accounting(snapshot, all_bindings, billing_heads, [], report, study_namespace=namespace)
    require(accounting["study_usage_micro_usd"] == report["incremental_micro_usd"]
            == sum(row["usage_micro_usd"] for row in report["shared_setups"]) + sum(row["usage_micro_usd"] for row in report["cases"])
            and accounting["unrelated_or_current_unsettled_reservations"] == accounting["unrelated_or_current_pending_promotions"] == 0
            and accounting["current_spent_or_reserved"] == report["budget"]["spent_or_reserved"]
            and accounting["current_spent_or_reserved"] <= accounting["current_limit"],
            "Shared plus arm usage was not settled exactly once")
    by_reservation = {binding["reservation"]: binding for binding in all_bindings}
    commitments = [(spans[call["request_span_id"]]["started_monotonic_ns"], spans[call["request_span_id"]]["finished_monotonic_ns"],
                    by_reservation[call["reservation"]]["amount"], call["usage_units"]) for call in all_calls]
    peak = max((sum(usage if ended <= at else amount if started <= at else 0
                    for started, ended, amount, usage in commitments)
                for at in [started for started, _, _, _ in commitments]), default=0)
    if mode == "rehearsal":
        require(accounting["study_usage_micro_usd"] == peak == 0
                and accounting["current_limit"] == report["budget"]["limit"] == report["budget_before"]["limit"] == 0,
                "Rehearsal contains provider spending or reservations")
    else:
        budget = contract["budget"]
        require(report["budget_before"]["spent_or_reserved"] == budget["expected_starting_usage_micro_usd"]
                and accounting["current_limit"] == report["budget_before"]["limit"] == report["budget"]["limit"]
                    == budget["shared_cumulative_cap_micro_usd"]
                and accounting["study_usage_micro_usd"] <= budget["incremental_cap_micro_usd"]
                and budget["expected_starting_usage_micro_usd"] + accounting["study_usage_micro_usd"] <= accounting["current_limit"],
                "Settled live spending exceeded the frozen incremental or cumulative cap")
    current_accounting = None
    if accounting_ledger is not None and Path(accounting_ledger).resolve() != snapshot:
        current_accounting = audit_accounting(accounting_ledger, all_bindings, billing_heads, [], report, study_namespace=namespace)
    check_frozen_snapshot(snapshot)
    return dict(protocol=COHORT_AUDIT_PROTOCOL, passed=True, mode=report["mode"], root=str(run),
        results_sha256=sha(run / "results.json"), timings_sha256=sha(run / "timings.json"),
        freeze_manifest_sha256=freeze_sha, contract_sha256=contract_sha, auditor_sha256=sha(__file__),
        cases=audited, shared_setups=shared_audits, accounting=accounting, current_accounting=current_accounting,
        physical_docker_executions=len(receipt_paths),
        physical_provider_calls=sum(call["physical_dispatch"] for call in all_calls),
        scripted_calls=len(all_calls) if mode == "rehearsal" else 0,
        conservative_peak_commitment_micro_usd=peak,
        budget_evidence=dict(unique_settled_charges=True, frozen_cumulative_total=True,
            historical_reservation_peak_independently_reconstructed=False,
            wrapper_interval_upper_bound_micro_usd=peak,
            admission_guard="Exact frozen StudyBudget and Ledger transactional admission guards; request intervals are conservative bounds, not reservation events."),
        qualification_binding=report["qualification"], runtime_identity_sha256=digest(runtime),
        scope=dict(primary_cohort=True, shared_initial_setup=True, controller_decision_replay=True,
                   exact_source_promotions=True, global_private_barrier=True, live_model_quality=mode == "live"),
        limitations=[("Scripted infrastructure rehearsal, not model-quality or statistical evidence." if mode == "rehearsal"
                     else "Exploratory development pilot; two application domains do not establish population-level superiority."),
            "Trusted-host retained consistency, not independent external execution or clock attestation.",
            "Historical reservation-time compliance relies on the qualified transactional admission guards; request-interval upper bounds do not prove the measured historical peak.",
            "Fixture meaning and task representativeness remain separately reviewed scientific assumptions.",
            "A passing audit does not authorize live dispatch, spending or a change to the scientific contract."])


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
    parser.add_argument("--scope", choices=("shared-setup", "cohort"), default="shared-setup")
    args = parser.parse_args()
    require(not args.output.exists(), "Refusing to overwrite retained audit evidence")
    function = audit_shared_setup if args.scope == "shared-setup" else audit_run
    result = function(args.run, accounting_ledger=args.accounting_ledger)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x") as handle:
        handle.write(json.dumps(result, sort_keys=True, indent=2) + "\n")
    print(json.dumps(dict(passed=True, scope=args.scope, output=str(args.output))))


if __name__ == "__main__":
    main()
