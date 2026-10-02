"""Independent, read-only checks for the continuation follow-up setup.

This auditor never imports the follow-up runner, controller, fixture or candidate
code. Initial source formation, journals, public receipts and shared provenance
can be checked independently. A complete cohort certificate is deliberately
unsupported until independent controller-decision replay is implemented.
"""
from __future__ import annotations

import argparse
import ast
from contextvars import ContextVar
from copy import deepcopy
import json
from pathlib import Path
import random
from typing import cast

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
    path = inside(binding["path"], root)
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
        if any(alias.lower() in note_text.lower() for alias in ("candidate-E", "candidate-F")):
            note_text = ""
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


def audit_run(run, **kwargs):
    """Do not promote setup consistency into unsupported study qualification."""
    raise ValueError("Full continuation-followup cohort certification is unsupported: "
                     "independent controller-decision replay and final-evaluation audit are not implemented")


def _audit_shared_setup(run, *, accounting_ledger=None):
    root = Path(run).resolve()
    manifest = load(root / "shared-setup.json")
    require(manifest.get("protocol") == SHARED_PROTOCOL and manifest.get("mode") == "offline"
            and manifest.get("validator_origin") == "physical_docker"
            and type(manifest.get("stage_index")) is int and manifest["stage_index"] == 0
            and type(manifest.get("repetition")) is int and manifest["repetition"] in range(2)
            and manifest.get("project_id") in {"warehouse", "job-queue"}
            and type(manifest.get("physical_provider_calls")) is int and manifest["physical_provider_calls"] == 0
            and type(manifest.get("usage_micro_usd")) is int and manifest["usage_micro_usd"] == 0,
            "Only a physical-Docker, zero-API shared setup can receive this certificate")
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
    require(contract.get("protocol") == PROTOCOL and contract.get("status") == "offline_foundations_only"
            and digest(contract) == manifest["contract_sha256"] and contract.get("live_enabled") is False
            and contract.get("full_cohort_qualification") is False and contract.get("hidden_feedback") is False
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
            and set(contract["extra_sources"]) == {"analysis/qualify_continuation_followup.py"},
            "Frozen policy, limits, pairing or source inventory changed")
    for name, expected in contract["sources"].items():
        require(sha(root / "source-snapshot" / "gossip_harness" / name) == expected,
                "Frozen scientific source changed: " + name)
    for name, expected in contract["extra_sources"].items():
        require(sha(root / "source-snapshot" / name) == expected, "Frozen qualification source changed")
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
    expected_approval = dict(files_sha256=digest(project["initial_files"]), source_valid=True,
                            approval_id="trusted-baseline:" + digest(project["initial_files"]))
    require(same(manifest["baseline_approval"], expected_approval), "Shared baseline approval changed")
    all_session_ids = [f"{project_id}-{rep}-shared-{kind}" for project_id in contract["project_ids"]
                       for rep in range(2) for kind in (*FORMATIONS, "scouts")]
    all_spans = audit_timings(document, manifest, all_session_ids)
    session_ids = {f"{block_id}-shared-{kind}" for kind in (*FORMATIONS, "scouts")}
    spans = {key: row for key, row in all_spans.items() if row.get("run_id") in session_ids}
    require(spans and all(row["kind"] in {"worker_request", "scripted_worker", "docker_validation", "scout_batch"}
            and type(row.get("stage_index")) is int and row["stage_index"] == 0 and row["status"] == "finished"
            and row["finished_monotonic_ns"] <= manifest["frozen_monotonic_ns"] for row in spans.values()),
            "Shared setup contains orphan, private, failed or unfinished execution")
    validation_contract = {**contract, "adapter_sha256": _adapter_hash(read_bytes(
        root / "source-snapshot" / "gossip_harness" / "blackbox_validator.py").decode())}
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
        require(session["protocol"] == SHARED_PROTOCOL and session["mode"] == "offline"
                and session["session_id"] == session_id and session["project_id"] == pid
                and type(session["stage_index"]) is int and session["stage_index"] == 0
                and session["billing"] == billing and session["contract_sha256"] == digest(contract)
                and session["image"] == contract["image"] and session["validator_origin"] == "physical_docker"
                and type(session["physical_provider_calls"]) is int and session["physical_provider_calls"] == 0
                and type(session["usage_micro_usd"]) is int and session["usage_micro_usd"] == 0
                and session["initial_files_sha256"] == digest(project["initial_files"])
                and same(GitStore(directory / "baseline.git").read_files(), project["initial_files"]),
                "Session identity, source, execution origin or zero-API status changed")
        calls, call_bindings = audit_calls(directory, session["invocations"], billing, contract,
            "rehearsal", spans, session_id, 0)
        bindings.extend(call_bindings)
        require(all(row["execution_kind"] == "scripted_worker" for _, row in calls.values()),
                "Shared setup contains a provider dispatch")
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
    frozen_snapshot()
    current_accounting = None
    if accounting_ledger is not None and Path(accounting_ledger).resolve() != snapshot:
        current_accounting = audit_accounting(accounting_ledger, bindings, billing_heads, [], manifest,
                                              study_namespace=manifest["namespace"])
    require(accounting["study_usage_micro_usd"] == 0 and len(bindings) == 10,
            "Shared setup duplicated or omitted physical scripted calls or charges")
    return dict(protocol=AUDIT_PROTOCOL, passed=True, root=str(root), auditor_sha256=sha(__file__),
        manifest_sha256=sha(root / "shared-setup.json"), contract_sha256=digest(contract),
        project_id=pid, repetition=repetition, initial_pools=pool_audits,
        scripted_calls=10, physical_provider_calls=0, public_docker_executions=8,
        physical_scout_calls=0, scripted_scout_calls=2, usage_micro_usd=0, accounting=accounting,
        current_accounting=current_accounting,
        scope=dict(shared_initial_setup=True, primary_cohort=False, controller_decision_replay=False,
                   prospective_arm_execution=False, oracle_probe_admission=False, fixture_qualification=False),
        limitations=["Trusted-host retained consistency, not external runtime attestation.",
            "This is one zero-API setup block, not a complete twelve-trajectory rehearsal or a live result.",
            "Shared setup source provenance never licenses receipt reuse or charges for imported arm opportunities.",
            "Controller routing, later milestone execution, final private barriers and project acceptance remain unaudited."])


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
