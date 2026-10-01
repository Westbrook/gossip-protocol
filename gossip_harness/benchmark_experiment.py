"""Versioned evidence-frontier cohort with frozen initial pools and final barrier.

Matched sequential and independent candidate formation shares one reviewer and
post-generation scouts. Every source, proposal and selection is frozen before
private acceptance or cross-policy diagnostics. Candidate code executes only in
BlackboxValidator; provider outcomes use the unchanged durable request journal.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import importlib
import json
from pathlib import Path
import random
import shutil
import subprocess
import sys
import threading
import time

from .blackbox_validator import BlackboxValidator
from .gitstore import GitStore
from .ledger import Ledger
from .pilot import DEFAULT_IMAGE, LeaseKeeper, Trace
from .sustained_experiment import credential, digest, local_promote, record_stage
from .verification_experiment import (CASE_TIMEOUT, CORE as PREVIOUS_CORE,
                                      SUITE_TIMEOUT, cases_for, oracle_gate,
                                      requirements, verify_receipt)
from .verification_journal import RequestJournal
from .worker import MODEL, STRONG_MODEL, OpenAIWorker, WorkerFailure, WorkerRequest, WorkerResult
from .continuation_experiment import (Spans, StudyBudget, live_ownership, read, save,
                                      sha_file, snapshot_accounting, unsettled)


PROTOCOL = "evidence-frontier-v1"
POLICIES = ("sequential-four", "independent-four", "strong-anchor")
MILESTONES = 2
OUTPUT_TOKENS = 12288
PROJECT_MODULES = {"graph-patch": "benchmark_graph_patch", "calendar-exchange": "benchmark_calendar_exchange"}
CORE = tuple(sorted(set(PREVIOUS_CORE + ("continuation_experiment.py", "continuation_stage.py",
    "continuation_transport.py", "transport.py", "benchmark_experiment.py", "benchmark_stage.py", "benchmark_diagnostics.py",
    *[name + ".py" for name in PROJECT_MODULES.values()]))))
PLAN_PATH = Path(__file__).parent.parent / "benchmark-study-plan.json"


def fixture(project_id):
    if project_id not in PROJECT_MODULES:
        raise ValueError("Unknown benchmark project")
    return importlib.import_module("." + PROJECT_MODULES[project_id], __package__)


def study_plan():
    plan = read(PLAN_PATH)
    if (plan.get("protocol") != PROTOCOL or plan.get("milestones") != MILESTONES
            or plan.get("projects") != list(PROJECT_MODULES)
            or set(plan.get("policies", {})) != set(POLICIES)
            or plan.get("controller", {}).get("output_tokens") != OUTPUT_TOKENS):
        raise ValueError("Unsupported benchmark study plan")
    expected_policies = {"sequential-four": (2, 4, "cheap", "sequential"),
        "independent-four": (2, 4, "cheap", "independent"), "strong-anchor": (1, 1, "strong", "independent")}
    for policy, values in expected_policies.items():
        row = plan["policies"][policy]
        if tuple(row.get(key) for key in ("repetitions", "initial_builders", "model", "formation")) != values:
            raise ValueError("Unsupported benchmark candidate policy")
    budget = plan.get("budget", {})
    for key in ("target_usage_micro_usd", "incremental_cap_micro_usd", "shared_cumulative_cap_micro_usd",
                "expected_starting_usage_micro_usd"):
        if type(budget.get(key)) is not int or budget[key] < 0:
            raise ValueError("Study budget must contain nonnegative integer microUSD")
    if not (0 < budget["target_usage_micro_usd"] <= budget["incremental_cap_micro_usd"]
            <= budget["shared_cumulative_cap_micro_usd"] - budget["expected_starting_usage_micro_usd"]):
        raise ValueError("Invalid study budget bound")
    controller = plan["controller"]
    if controller.get("scouts") != 2 or controller.get("proposals_per_scout") != 4:
        raise ValueError("The registered scout contract requires two independent four-proposal scouts")
    return plan


def roster():
    plan = study_plan()
    expected = {(project, policy, repetition) for project in PROJECT_MODULES for policy in POLICIES
        for repetition in range(plan["policies"][policy]["repetitions"])}
    if "roster" in plan:
        rows = plan["roster"]
    else:
        projects = list(PROJECT_MODULES)
        random.Random(plan["roster_seed"]).shuffle(projects)
        a, b = projects
        rows = [[a, "sequential-four", 0], [a, "independent-four", 0], [b, "strong-anchor", 0],
                [b, "independent-four", 0], [b, "sequential-four", 0], [a, "strong-anchor", 0],
                [b, "sequential-four", 1], [b, "independent-four", 1],
                [a, "independent-four", 1], [a, "sequential-four", 1]]
    if (not isinstance(rows, list) or len(rows) != 10 or any(not isinstance(row, list) or len(row) != 3
            or type(row[2]) is not int for row in rows) or {tuple(row) for row in rows} != expected):
        raise ValueError("The exact ten-trajectory roster is required")
    return deepcopy(rows)


def contract(image=DEFAULT_IMAGE):
    plan = study_plan()
    modules = {project: fixture(project) for project in PROJECT_MODULES}
    limits = {key: plan["controller"][key] for key in
        ("max_reviews", "max_repairs", "max_new_probes", "max_escalations", "stagnation_reviews")}
    return dict(protocol=PROTOCOL,
        sources={name: sha_file(Path(__file__).parent / name) for name in CORE},
        plan_sha256=sha_file(PLAN_PATH), fault_banks_sha256=digest(fault_banks()),
        extra_sources={"analysis/qualify_benchmark.py": sha_file(Path(__file__).parent.parent / "analysis/qualify_benchmark.py")},
        fixtures={key: dict(fixture_sha256=digest(module.PROJECT),
            baseline_sha256=digest(module.PROJECT["initial_files"]), module=PROJECT_MODULES[key])
            for key, module in modules.items()}, image=image,
        controller_runtime=dict(python=sys.version, implementation=sys.implementation.name,
            executable=sys.executable, platform=sys.platform,
            git=subprocess.check_output(["git", "--version"], text=True).strip()),
        project_ids=list(PROJECT_MODULES), policies=deepcopy(plan["policies"]),
        roster=roster(), milestones=MILESTONES, limits=limits,
        scouts=dict(count=2, proposals_per_scout=4, visibility="stage-start source and public specification only"),
        budget=deepcopy(plan["budget"]), case_timeout_seconds=CASE_TIMEOUT,
        suite_timeout_seconds=SUITE_TIMEOUT, output_tokens=OUTPUT_TOKENS,
        models={role: OpenAIWorker("profile-only", model=model, max_output_tokens=OUTPUT_TOKENS).profile_manifest()
            for role, model in (("cheap", MODEL), ("strong", STRONG_MODEL))},
        initial_barrier="Every initial candidate is retained, evaluated and durably frozen before scouts",
        private_barrier="All ten terminal trajectories, every retained candidate and all generated evidence frozen before private evaluation",
        freeze_integrity="Full actual Git/source verification at cohort freeze and final independent audit; per-final-suite gates reread all frozen artifact bytes and exact final heads. Immutable object reads are not repeated; all candidate executions remain physical.",
        rehearsal_controls="Golden initial sources, charged-zero no-op repair, stalled inspections, bounded escalation, explicit acceptance, valid/wrong/duplicate scouts",
        controller_crash_injection=False, hidden_feedback=False,
        timing="UTC and monotonic spans; physical calls distinct from replay; overlap is not additive wall time")


def known_result(project, module, stage, metadata):
    """Deterministic offline controls; never called by a live dispatch."""
    role = metadata["role"]
    if role == "scout":
        def proposal_for(index):
            payload = deepcopy(module.rehearsal_probe(stage, index))
            return dict(input=payload, expected=module.reference(stage, payload),
                        requirement=requirements(project, stage)[0])
        proposal = proposal_for(metadata["scout_index"])
        wrong = proposal_for(metadata["scout_index"] + 2)
        return WorkerResult({"probes.json": json.dumps({"probes": [proposal, dict(wrong, expected={"wrong": True}), proposal]})},
                            "Offline independent scout", 0, {"api_calls": 0})
    if role == "reviewer":
        alias = metadata["alias_by_slot"]["slot-0"]
        round_number = metadata["round"]
        return WorkerResult({"review.json": json.dumps(dict(candidate=alias,
            action="repair" if round_number == 1 else "inspect" if round_number < 4 else "accept",
            notes="Rehearse unchanged source and bounded continuation" if round_number < 4 else "Checked rehearsal source",
            remaining=[requirements(project, stage)[0]] if round_number < 4 else [], probes=[]))},
            "Offline reviewer", 0, {"api_calls": 0})
    if metadata.get("phase") == "repair" and metadata["round"] == 2:
        raise WorkerFailure("Offline no-effective-change control", usage_units=0,
                            metadata={"api_calls": 0, "rehearsal_noop": True})
    changes = {name: project["stages"][stage]["known_files"][name] for name in project["allowed_paths"]}
    changes["notes.json"] = json.dumps(dict(notes="Preserve cumulative requirements", remaining=[]))
    return WorkerResult(changes, "Offline known implementation", 0, {"api_calls": 0})


def decode_scout(result):
    """Parse an untrusted batch without executing code or supplying oracle labels."""
    content = (result.changes.get("probes.json") if result is not None
               and isinstance(result.changes, dict) and set(result.changes) == {"probes.json"} else None)
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise ValueError("Duplicate JSON member")
            value[key] = item
        return value
    def constant(_):
        raise ValueError("Nonfinite JSON")
    try:
        if not isinstance(content, str) or len(content.encode("utf-8")) > 65536:
            raise ValueError("Missing or oversized scout envelope")
        value = json.loads(content, object_pairs_hook=pairs, parse_constant=constant)
        if (not isinstance(value, dict) or set(value) != {"probes"}
                or not isinstance(value["probes"], list) or len(value["probes"]) > 4):
            raise ValueError("Malformed scout envelope")
        # The existing oracle gate handles each proposal's bounded JSON/schema.
        return deepcopy(value["probes"]), dict(status="parsed", proposed_count=len(value["probes"]))
    except (ValueError, TypeError, UnicodeError, RecursionError):
        return [], dict(status="rejected_batch", proposed_count=None,
                        reason="missing_or_malformed_scout_envelope")


def scout_probes(project, stage, initial_files, invoke, root, spans, run_id, frozen_pool):
    """Run two independent scouts only after the initial pool is durable."""
    freeze = read(root / "initial-pool.json")
    if freeze.get("pool_sha256") != digest(frozen_pool) or freeze.get("pool") != frozen_pool:
        raise ValueError("Initial pool must be durably frozen before scout dispatch")
    public = cases_for(project, stage, "visible")
    context = dict(project_id=project["id"], stage_index=stage,
        milestones=[{key: item[key] for key in ("requirements", "specification", "spec") if key in item}
                    for item in project["stages"][:stage + 1]], public_cases=deepcopy(public))
    instructions = ("Find useful boundary and interaction tests for this maintenance milestone. "
        "Read scout-context.json and the stage-start source. Return only probes.json containing "
        'exactly {"probes": [{"requirement": requirement_id, "input": JSON_input, '
        '"expected": exact_JSON_output}]}, at most four proposals. Do not change source or write '
        "executable tests. A trusted oracle checks your labels; wrong labels are rejected without correction. "
        "You have no peer proposals or candidate implementations. Prefer new-operation interactions over "
        "empty or malformed-input-only checks and public duplicates. Include at least two successful "
        "multi-command paths through the current milestone's new operations, and at least one rejected "
        "transaction followed by state, audit, or retry/persistence observations. Admission checks only "
        "domain and expected values, not test strength. Do not claim that checks were executed.")
    inputs = {**initial_files, "scout-context.json": json.dumps(context, sort_keys=True)}
    def scout(index):
        return invoke(role="scout", model="cheap", files=deepcopy(inputs), instructions=instructions,
            allowed_paths=("probes.json",), feedback="", metadata=dict(role="scout", round=1,
                candidate_id=f"scout-{index}", scout_index=index, stage_index=stage, project_id=project["id"],
                phase="post_initial_freeze"))
    with spans.measure("scout_batch", run_id=run_id, stage_index=stage,
                       initial_pool_sha256=freeze["pool_sha256"]):
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(scout, index) for index in range(2)]
            results = [future.result() for future in futures]
    scouts, parses = [], []
    for index, result in enumerate(results):
        proposals, parse = decode_scout(result)
        scouts.append(dict(scout_id=f"scout-{index}", probes=proposals))
        parses.append(dict(scout_id=f"scout-{index}", **parse))
    evidence = dict(calls=2, parses=parses, rejected_batches=sum(row["status"] == "rejected_batch" for row in parses),
        source_sha256=digest(initial_files), shared_input_sha256=digest(inputs),
        initial_pool_sha256=freeze["pool_sha256"], initial_pool_artifact_sha256=sha_file(root / "initial-pool.json"),
        proposals=deepcopy(scouts))
    save(root / "scouts.json", evidence)
    return scouts, evidence


def trajectory(config, expected, ledger, budget, models, spans):
    from .benchmark_stage import run_benchmark_stage
    root = Path(config["root"])
    module, contract_sha = fixture(config["project_id"]), digest(expected)
    project = module.PROJECT
    base = GitStore.create(root / "initial.git", project["initial_files"])
    state = dict(contract_sha=contract_sha, run_id=config["run_id"], project_id=project["id"],
        policy=config["policy"], repetition=config["repetition"], stages=[], next_stage=0,
        files=deepcopy(project["initial_files"]), store_path=str(base.path), head=base.head())
    save(root / "state.json", state)
    trace = Trace(root / "trace.jsonl")
    fatal = threading.Event()
    for stage in range(MILESTONES):
        if digest(contract(config["image"])) != contract_sha:
            raise ValueError("Execution contract changed after preregistration")
        stage_root = root / f"stage-{stage}"
        stage_root.mkdir()
        (stage_root / "requests").mkdir()
        (stage_root / "evaluations").mkdir()
        billing = config["namespace"] + f"/stage-{stage}"
        ledger.add_task(billing)
        journal = RequestJournal(stage_root / "journal")
        calls, calls_lock = {}, threading.RLock()
        exports = []
        initial_files = deepcopy(state["files"])

        def emit(kind, /, **data):
            data.pop("kind", None)
            data.pop("stage_index", None)
            trace.emit(kind, stage_index=stage, monotonic_ns=time.monotonic_ns(), **data)

        with spans.measure("stage", run_id=config["run_id"], stage_index=stage), LeaseKeeper(ledger) as keeper:
            keeper.claim(billing, "benchmark-controller")

            def invoke(*, role, model, files, instructions, allowed_paths, feedback, metadata):
                if fatal.is_set():
                    raise RuntimeError("An earlier provider outcome requires investigation")
                if model not in {"cheap", "strong"} or (config["live"] and model not in models):
                    raise ValueError("Unknown model profile")
                call_id = f"{role}-{metadata['candidate_id']}-{metadata['round']}"
                reservation = billing + "/" + call_id
                request = WorkerRequest("task-" + digest([contract_sha, reservation])[:24], instructions,
                    tuple(allowed_paths), deepcopy(files), base.head(), metadata["round"], feedback)
                save(stage_root / "requests" / f"{call_id}.request.json", asdict(request))
                worker = models[model] if config["live"] else None
                units = worker.reservation_units(request) if worker else 0
                row = dict(call_id=call_id, reservation=reservation, role=role, model=model,
                    request_sha256=digest(asdict(request)), physical_dispatch=False,
                    controller_metadata=deepcopy(metadata),
                    journal_paths={key: str(path) for key, path in journal.paths(call_id).items()})

                def actual_call():
                    if fatal.is_set():
                        raise RuntimeError("Dispatch stopped after another unknown outcome")
                    if digest(contract(config["image"])) != contract_sha:
                        raise ValueError("Execution contract changed before provider dispatch")
                    row["physical_dispatch"] = bool(config["live"])
                    kind = "physical_provider_request" if config["live"] else "scripted_worker"
                    with spans.measure(kind, run_id=config["run_id"], stage_index=stage,
                            call_id=call_id, role=role, model=model,
                            meaning="Request wall time including network/provider wait; not model forward-pass latency") as span:
                        row["physical_span_id"] = span["span_id"]
                        emit("provider_dispatch" if config["live"] else "scripted_dispatch",
                             call_id=call_id, reservation=reservation, model=model, role=role)
                        return worker.run(request) if worker else known_result(project, module, stage, metadata)

                outcome = None
                try:
                    with spans.measure("worker_request", run_id=config["run_id"], stage_index=stage,
                                       call_id=call_id, role=role, model=model) as span:
                        row["request_span_id"] = span["span_id"]
                        outcome = journal.execute(call_id, request, reservation, actual_call,
                            lambda: budget.reserve(reservation, keeper.check(billing), units),
                            lambda amount: ledger.settle(reservation, amount))
                    row.update(asdict(outcome))
                    row["outcome"] = "result"
                    return outcome
                except WorkerFailure as error:
                    row.update(outcome="failure", usage_units=error.usage_units,
                               metadata=error.metadata, failure=str(error))
                    if error.usage_units is None or error.metadata.get("halt"):
                        fatal.set()
                        raise RuntimeError("Provider outcome requires investigation; no automatic retry") from None
                    return None
                except BaseException as error:
                    fatal.set()
                    row.update(outcome="stopped", usage_units=None, error_type=type(error).__name__)
                    raise
                finally:
                    path = journal.paths(call_id)["result"]
                    row["journal_result_sha256"] = sha_file(path) if path.exists() else None
                    row["journal_replay"] = not row.get("physical_dispatch") and config["live"] and path.exists()
                    with calls_lock:
                        calls[call_id] = row
                        save(stage_root / "requests" / f"{call_id}.result.json", row)

            def retain(files, label):
                store = GitStore.fork(base, stage_root / (label.replace("/", "-") + ".git"))
                tip = store.propose({name: files[name] for name in project["allowed_paths"]}, message=label)
                if store.read_files(tip) != files:
                    raise ValueError("Retained proposal differs from its source")
                binding = dict(store_path=str(store.path), tip_sha=tip, files_sha256=digest(files))
                export_path = stage_root / (label.replace("/", "-") + ".json")
                save(export_path, dict(label=label, binding=binding, files=files))
                exports.append(dict(label=label, path=str(export_path), sha256=sha_file(export_path),
                    files_sha256=digest(files), binding=deepcopy(binding)))
                save(stage_root / "candidate-exports.json", dict(protocol=PROTOCOL,
                    run_id=config["run_id"], project_id=project["id"], stage_index=stage, exports=exports))
                return binding

            def evaluate(files, cases, label):
                validator = BlackboxValidator(config["image"], timeout_seconds=SUITE_TIMEOUT,
                                               case_timeout_seconds=CASE_TIMEOUT)
                with spans.measure("docker_validation", run_id=config["run_id"], stage_index=stage,
                                   label=label, purpose="stage_evidence", case_count=len(cases)) as span:
                    result = validator.evaluate(files, cases)
                    path = stage_root / "evaluations" / f"{span['span_id']}.json"
                    save(path, result)
                    spans.annotate(span, receipt_path=str(path), receipt_sha256=sha_file(path))
                    return verify_receipt(result, files, cases, config["image"])

            prior = deepcopy(state["stages"][-1]) if state["stages"] else {}
            scouts = {}
            def after_initial(pool):
                if (pool.get("project_id") != project["id"] or pool.get("stage_index") != stage
                        or pool.get("policy") != config["policy"]):
                    raise ValueError("Initial candidate pool identity changed")
                path = stage_root / "initial-pool.json"
                if path.exists():
                    raise ValueError("Initial pool may only be frozen once")
                save(path, dict(protocol=PROTOCOL, run_id=config["run_id"], stage_index=stage,
                    pool=deepcopy(pool), pool_sha256=digest(pool),
                    frozen_utc=datetime.now(timezone.utc).isoformat(), frozen_monotonic_ns=time.monotonic_ns()))
                proposals, evidence = scout_probes(project, stage, initial_files, invoke, stage_root,
                    spans, config["run_id"], pool)
                scouts.update(evidence)
                return dict(freeze_id=str(path), pool_sha256=digest(pool), scouts=proposals)
            approval = dict(files_sha256=digest(initial_files), source_valid=True,
                approval_id=base.head() if stage else "trusted-baseline:" + digest(project["initial_files"]))
            result = run_benchmark_stage(project, stage, config["policy"], initial_files, prior,
                invoke=invoke, evaluate=evaluate, retain=retain, validate_probes=oracle_gate(module, project),
                emit=emit, after_initial=after_initial,
                candidate_seed=int(digest([project["id"], config["repetition"], stage])[:16], 16),
                limits=expected["limits"], baseline_approval=approval)
            scouts.update(admitted=result["metrics"]["admitted_scout_probes"],
                          rejected=result["metrics"]["rejected_scout_probes"])
            save(stage_root / "scouts.json", scouts)
            result.update(stage_index=stage, invocations=[calls[key] for key in sorted(calls)], scouts=scouts)
            save(stage_root / "result.json", result)
            record_stage(stage_root, result, ledger, keeper.check(billing))
        state["stages"].append(result)
        state["files"] = result["files"]
        if not result["completed"]:
            state["terminal"] = "bounded_incomplete"
            chosen = result["selected_binding"]
            store = GitStore(chosen["store_path"])
            state["final_binding"] = {**chosen, "accepted_head": store.head()}
            break
        base = local_promote(base, result["selected_binding"], stage_root / "checkpoint.git",
                             result["files"], project["allowed_paths"])
        state.update(store_path=str(base.path), head=base.head(), next_stage=stage + 1)
        state["final_binding"] = dict(store_path=str(base.path), tip_sha=base.head(),
                                     accepted_head=base.head(), files_sha256=digest(state["files"]))
        save(root / "state.json", state)
    else:
        state["terminal"] = "visible_complete"
    save(root / "trajectory.json", state)
    return state


def validate_trajectory(state, expected, identity, *, verify_git_sources=True):
    if (state.get("contract_sha") != digest(expected)
            or [state.get("project_id"), state.get("policy"), state.get("repetition")] != list(identity)
            or type(state.get("repetition")) is not int
            or state.get("terminal") not in {"visible_complete", "bounded_incomplete"}):
        raise ValueError("Trajectory identity or terminal contract is invalid")
    stages = state.get("stages")
    if not isinstance(stages, list) or not 1 <= len(stages) <= MILESTONES:
        raise ValueError("Trajectory must retain every attempted milestone")
    for index, result in enumerate(stages):
        if (type(result.get("stage_index")) is not int or result["stage_index"] != index
                or type(result.get("completed")) is not bool
                or (index < len(stages) - 1 and result["completed"] is not True)):
            raise ValueError("Trajectory skipped or reordered a milestone")
    full = len(stages) == MILESTONES and all(row["completed"] for row in stages)
    if (state["terminal"] == "visible_complete") != full:
        raise ValueError("Terminal classification disagrees with completed milestones")
    if state.get("files") != stages[-1].get("files"):
        raise ValueError("Final source differs from selected stage source")
    binding = state.get("final_binding", {})
    if binding.get("files_sha256") != digest(state["files"]):
        raise ValueError("Final source digest changed")
    store = GitStore(binding["store_path"])
    if (store.head() != binding.get("accepted_head")
            or (verify_git_sources and store.read_files(binding["tip_sha"]) != state["files"])):
        raise ValueError("Final Git/source binding changed")
    return state


def bound_file(path):
    path = Path(path)
    return dict(path=str(path.resolve()), sha256=sha_file(path))


def fault_banks():
    return dict(protocol="evidence-frontier-fault-bank-v1", projects=[dict(project_id=project_id,
        stages=[dict(stage_index=stage, fault_bank=fixture(project_id).fault_bank(stage),
                     correct_controls=fixture(project_id).correct_controls(stage))
                for stage in range(MILESTONES)]) for project_id in PROJECT_MODULES])


def collect_stage_artifacts(root, state, *, verify_git_sources=True):
    """Validate all retained versions and return exact immutable artifact bindings.

    Diagnostics can read these JSON/Git bindings without importing this runner or
    executing candidate code. Each export label is stage-N-build-slot-K-round.
    """
    root = Path(root)
    project = fixture(state["project_id"]).PROJECT
    rows = []
    for stage, result in enumerate(state["stages"]):
        stage_root = root / f"stage-{stage}"
        saved_result = read(stage_root / "result.json")
        if saved_result != result or result.get("stage_index") != stage:
            raise ValueError("Retained stage result changed")
        initial = read(stage_root / "initial-pool.json")
        pool = initial.get("pool", {})
        if (initial.get("protocol") != PROTOCOL or initial.get("run_id") != state["run_id"]
                or initial.get("stage_index") != stage or initial.get("pool_sha256") != digest(pool)
                or pool != result.get("initial_pool") or digest(pool) != result.get("initial_pool_sha256")
                or pool.get("project_id") != state["project_id"] or pool.get("stage_index") != stage
                or pool.get("policy") != state["policy"]
                or result.get("initial_freeze") != dict(freeze_id=str(stage_root / "initial-pool.json"),
                                                        pool_sha256=digest(pool))):
            raise ValueError("Initial candidate freeze changed")
        expected_count = study_plan()["policies"][state["policy"]]["initial_builders"]
        candidates = pool.get("candidates", {})
        if (len(candidates) != expected_count or len(pool.get("candidate_order", [])) != expected_count
                or set(pool["candidate_order"]) != set(candidates)
                or result.get("metrics", {}).get("initial_builder_calls") != expected_count):
            raise ValueError("Initial candidate formation is incomplete")
        catalog = read(stage_root / "candidate-exports.json")
        if (catalog.get("protocol") != PROTOCOL or catalog.get("run_id") != state["run_id"]
                or catalog.get("project_id") != state["project_id"] or catalog.get("stage_index") != stage):
            raise ValueError("Candidate export manifest identity changed")
        exports = catalog.get("exports", [])
        builder_rows = [row for row in result["trajectory"] if row.get("kind") == "builder"]
        labels = [f"stage-{stage}-build-{row['slot']}-{row['round']}" for row in builder_rows]
        if ([row.get("label") for row in exports] != labels or len(set(labels)) != len(labels)
                or len(exports) != result["metrics"]["builder_calls"]):
            raise ValueError("Every initial and repaired source must remain exported")
        export_bindings = []
        available = {}
        for entry, builder in zip(exports, builder_rows):
            path = stage_root / (entry["label"].replace("/", "-") + ".json")
            if (Path(entry.get("path", "")).resolve() != path.resolve()
                    or sha_file(path) != entry.get("sha256")):
                raise ValueError("Candidate export bytes changed")
            exported = read(path)
            files, binding = exported.get("files", {}), exported.get("binding", {})
            if (exported.get("label") != entry["label"] or binding != entry.get("binding")
                    or binding != builder.get("binding") or digest(files) != entry.get("files_sha256")
                    or digest(files) != binding.get("files_sha256") or digest(files) != builder.get("files_sha256")
                    or set(files) != set(project["initial_files"])
                    or any(files[name] != content for name, content in project["initial_files"].items()
                           if name not in project["allowed_paths"])):
                raise ValueError("Candidate export source or scope changed")
            if verify_git_sources:
                store = GitStore(binding["store_path"])
                if store.read_files(binding["tip_sha"]) != files:
                    raise ValueError("Retained candidate Git source changed")
            available[digest(binding)] = files
            export_bindings.append(deepcopy(entry))
        for group in (candidates, result.get("candidates", {})):
            if set(group) != set(candidates):
                raise ValueError("Final candidate pool changed identity")
            for item in group.values():
                files = available.get(digest(item.get("binding", {})))
                if files is None or ("files" in item and item["files"] != files):
                    raise ValueError("Candidate pool references an unexported source")
        if available.get(digest(result["selected_binding"])) != result["files"]:
            raise ValueError("Selected candidate is absent from retained exports")
        scouts = read(stage_root / "scouts.json")
        if scouts != result.get("scouts") or scouts.get("calls") != 2:
            raise ValueError("Retained scouts changed")
        requests = []
        for call in result["invocations"]:
            call_id = call["call_id"]
            request_path = stage_root / "requests" / f"{call_id}.request.json"
            result_path = stage_root / "requests" / f"{call_id}.result.json"
            if digest(read(request_path)) != call.get("request_sha256") or read(result_path) != call:
                raise ValueError("Provider proposal or request changed")
            journal = {key: bound_file(path) for key, path in call["journal_paths"].items()}
            if journal["result"]["sha256"] != call.get("journal_result_sha256"):
                raise ValueError("Journal response changed")
            requests.append(dict(call_id=call_id, request=bound_file(request_path),
                                 result=bound_file(result_path), journal=journal))
        rows.append(dict(stage_index=stage, result=bound_file(stage_root / "result.json"),
            initial_pool={**bound_file(stage_root / "initial-pool.json"), "pool_sha256": digest(pool)},
            candidate_exports=bound_file(stage_root / "candidate-exports.json"), exports=export_bindings,
            scouts=bound_file(stage_root / "scouts.json"), requests=requests))
    return rows


def freeze_trajectories(output, expected):
    rows = []
    output = Path(output)
    fixtures_path, faults_path = output / "fixtures.json", output / "fault-banks.json"
    # Fixture definitions use immutable tuples; persisted JSON represents those
    # sequences as arrays/lists. Compare the same canonical JSON contract used
    # by preregistration while retaining exact file-byte bindings below.
    if (digest(read(fixtures_path)) != digest({"projects": [fixture(project).PROJECT for project in PROJECT_MODULES]})
            or digest(read(faults_path)) != expected["fault_banks_sha256"]):
        raise ValueError("Preregistered fixtures or fault banks changed")
    for identity in expected["roster"]:
        project_id, policy, repetition = identity
        run_id = f"{project_id}-{policy}-{repetition}"
        path = output / run_id / "trajectory.json"
        state = validate_trajectory(read(path), expected, identity)
        rows.append(dict(run_id=run_id, project_id=project_id, policy=policy, repetition=repetition,
            trajectory_sha256=sha_file(path), files_sha256=digest(state["files"]),
            final_binding=state["final_binding"], terminal=state["terminal"],
            evidence_sha256=digest(state["stages"][-1]["evidence_cases"]),
            milestones_completed=sum(row["completed"] for row in state["stages"]),
            stage_artifacts=collect_stage_artifacts(output / run_id, state)))
    manifest = dict(protocol=PROTOCOL, contract_sha=digest(expected), trajectories=rows,
        fixtures=bound_file(fixtures_path), fault_banks=bound_file(faults_path),
        frozen_utc=datetime.now(timezone.utc).isoformat(), frozen_monotonic_ns=time.monotonic_ns(),
        private_evaluation_started=False)
    save(output / "frozen-trajectories.json", manifest)
    return manifest


def validate_freeze(output, expected, expected_sha, *, verify_git_sources=True):
    """Reconcile frozen bytes and heads; optional repeated immutable Git reads.

    Freeze creation and rehearsal acceptance verify every actual Git source.
    Final-suite gates use the frozen source exports, rehash every bound artifact,
    and reread final accepted refs. Promotion still verifies the actual offered
    Git source and exact tested merged tree through the unchanged CAS gate.
    """
    output = Path(output)
    path = output / "frozen-trajectories.json"
    if sha_file(path) != expected_sha:
        raise ValueError("Study-wide freeze manifest changed")
    manifest = read(path)
    rows = manifest.get("trajectories", [])
    actual = [[row.get("project_id"), row.get("policy"), row.get("repetition")] for row in rows]
    if (manifest.get("protocol") != PROTOCOL or manifest.get("contract_sha") != digest(expected)
            or actual != expected["roster"] or len(rows) != 10
            or manifest.get("private_evaluation_started") is not False):
        raise ValueError("Exact ten-trajectory freeze is required before private evaluation")
    for key, filename in (("fixtures", "fixtures.json"), ("fault_banks", "fault-banks.json")):
        if manifest.get(key) != bound_file(output / filename):
            raise ValueError("Frozen fixture or fault-bank snapshot changed")
    if digest(read(output / "fault-banks.json")) != expected["fault_banks_sha256"]:
        raise ValueError("Fault bank differs from the execution contract")
    states = {}
    for identity, row in zip(expected["roster"], rows):
        run_id = f"{identity[0]}-{identity[1]}-{identity[2]}"
        if row.get("run_id") != run_id or type(row.get("repetition")) is not int:
            raise ValueError("Frozen roster identity changed")
        path = output / run_id / "trajectory.json"
        if sha_file(path) != row.get("trajectory_sha256"):
            raise ValueError("Frozen trajectory changed")
        state = validate_trajectory(read(path), expected, identity, verify_git_sources=verify_git_sources)
        if (row.get("final_binding") != state["final_binding"]
                or row.get("files_sha256") != digest(state["files"])
                or row.get("evidence_sha256") != digest(state["stages"][-1]["evidence_cases"])
                or row.get("stage_artifacts") != collect_stage_artifacts(output / run_id, state, verify_git_sources=verify_git_sources)):
            raise ValueError("Frozen source, proposal, selection or evidence changed")
        states[run_id] = state
    return states


def finalize(output, config, expected, freeze_sha, spans):
    states = validate_freeze(output, expected, freeze_sha, verify_git_sources=False)
    root = Path(config["root"])
    state = states[config["run_id"]]
    project = fixture(config["project_id"]).PROJECT
    full = len(state["stages"]) == MILESTONES and all(row["completed"] for row in state["stages"])
    active = state["stages"][-1]["probe_pool"] if len(state["stages"]) == MILESTONES else []
    suites = {"visible": cases_for(project, MILESTONES - 1, "visible") + active,
              "private": cases_for(project, MILESTONES - 1, "hidden")}
    receipts = {}
    for kind, cases in suites.items():
        validate_freeze(output, expected, freeze_sha, verify_git_sources=False)
        validator = BlackboxValidator(config["image"], timeout_seconds=SUITE_TIMEOUT,
                                       case_timeout_seconds=CASE_TIMEOUT)
        with spans.measure("docker_validation", run_id=config["run_id"], purpose="final_" + kind,
                case_count=len(cases), freeze_manifest_sha256=freeze_sha,
                freeze_verification="all_frozen_artifact_bytes_and_exact_final_heads") as span:
            receipt = validator.evaluate(state["files"], cases)
            path = root / f"final-{kind}-receipt.json"
            save(path, receipt)
            spans.annotate(span, receipt_path=str(path), receipt_sha256=sha_file(path))
            receipts[kind] = verify_receipt(receipt, state["files"], cases, config["image"])
    accepted = bool(full and receipts["visible"]["passed"] and receipts["private"]["passed"])
    release = None
    if accepted:
        release = local_promote(GitStore(root / "initial.git"), state["final_binding"], root / "release.git",
                                state["files"], project["allowed_paths"])
    pairs = list(zip(suites["private"], receipts["private"]["outcomes"]))
    coverage = {key: all(outcome["passed"] for case, outcome in pairs if case["requirement"] == key)
                for key in sorted({case["requirement"] for case in suites["private"]})}
    calls = [call for stage in state["stages"] for call in stage["invocations"]]
    metric_keys = {key for stage in state["stages"] for key in stage["metrics"]}
    result = dict(run_id=config["run_id"], root=str(root), project_id=project["id"], policy=config["policy"],
        repetition=config["repetition"], accepted=accepted,
        status="accepted" if accepted else "final_quality_failed" if full else state["terminal"],
        milestones_completed=sum(row["completed"] for row in state["stages"]),
        files_sha256=digest(state["files"]), trajectory_sha256=sha_file(root / "trajectory.json"),
        freeze_manifest_sha256=freeze_sha, requirement_coverage=coverage,
        final_visible=receipts["visible"], final_hidden=receipts["private"],
        release_head=release.head() if release else None, invocations=calls,
        usage_micro_usd=sum(row["usage_units"] for row in calls),
        physical_provider_calls=sum(row["physical_dispatch"] for row in calls),
        metrics={key: sum(row["metrics"].get(key, 0) for row in state["stages"]) for key in sorted(metric_keys)},
        scouts={key: sum(row["scouts"][key] for row in state["stages"]) for key in ("calls", "admitted", "rejected", "rejected_batches")})
    save(root / "result.json", result)
    return result


def validate_timing_evidence(root, expected, freeze_sha):
    """Check real ordering and independent final execution in a retained proof."""
    root = Path(root).resolve()
    clock = read(root / "timings.json")
    spans = clock.get("spans", [])
    if (not isinstance(spans, list) or not spans
            or [row.get("span_id") for row in spans] != list(range(len(spans)))):
        raise ValueError("Timing spans must have complete unique ordered identities")
    freeze = read(root / "frozen-trajectories.json")
    boundary = freeze.get("frozen_monotonic_ns")
    if type(boundary) is not int:
        raise ValueError("Cohort freeze requires its monotonic boundary")
    for span in spans:
        if (span.get("clock_id") != clock.get("clock_id")
                or type(span.get("started_monotonic_ns")) is not int
                or type(span.get("finished_monotonic_ns")) is not int
                or span["started_monotonic_ns"] > span["finished_monotonic_ns"]
                or span.get("status") not in {"finished", "failed"}):
            raise ValueError("Timing evidence contains an unfinished or foreign-clock span")
        if span.get("kind") in {"physical_provider_request", "scripted_worker", "model_trajectory"}:
            if span["finished_monotonic_ns"] > boundary:
                raise ValueError("Provider cohort crossed the private-evaluation barrier")
    roster_ids = {f"{project}-{policy}-{repetition}" for project, policy, repetition in expected["roster"]}
    trajectories = [span for span in spans if span.get("kind") == "model_trajectory"]
    if len(trajectories) != len(roster_ids) or {span.get("run_id") for span in trajectories} != roster_ids:
        raise ValueError("Every trajectory requires its completed physical timing span")
    containers, final_keys = set(), set()
    for span in spans:
        if span.get("kind") != "docker_validation":
            continue
        path = Path(span.get("receipt_path", "")).resolve()
        if not path.is_relative_to(root) or sha_file(path) != span.get("receipt_sha256"):
            raise ValueError("Physical validation span lost its exact receipt binding")
        receipt = read(path)
        container = receipt.get("container_name")
        if not isinstance(container, str) or not container or container in containers:
            raise ValueError("Every validation purpose requires a fresh physical container")
        containers.add(container)
        if span.get("purpose") not in {"final_visible", "final_private"}:
            continue
        kind = span["purpose"].removeprefix("final_")
        key = (span.get("run_id"), kind)
        if (key in final_keys or key[0] not in roster_ids or span.get("status") != "finished"
                or span["started_monotonic_ns"] < boundary
                or span.get("freeze_manifest_sha256") != freeze_sha
                or path != root / key[0] / f"final-{kind}-receipt.json"):
            raise ValueError("Final acceptance did not independently execute after the full cohort freeze")
        final_keys.add(key)
    if final_keys != {(run_id, kind) for run_id in roster_ids for kind in ("visible", "private")}:
        raise ValueError("Every frozen trajectory requires both fresh final acceptance suites")
    return {row["span_id"]: row for row in spans}


def validate_rehearsal(proof, expected, proof_root=None):
    rows = proof.get("cases", [])
    actual = [[row.get("project_id"), row.get("policy"), row.get("repetition")] for row in rows]
    if (proof.get("experiment") != PROTOCOL or proof.get("mode") != "rehearsal"
            or proof.get("contract") != expected or proof.get("contract_sha") != digest(expected)
            or proof.get("status") != "finished" or proof.get("unexecuted") or proof.get("censored")
            or proof.get("active_case") is not None or actual != expected["roster"] or len(rows) != 10
            or type(proof.get("incremental_micro_usd")) is not int or proof["incremental_micro_usd"] != 0):
        raise ValueError("Exact complete ten-trajectory rehearsal is required")
    for row in rows:
        metrics, scouts = row.get("metrics", {}), row.get("scouts", {})
        count = expected["policies"][row["policy"]]["initial_builders"]
        if (row.get("accepted") is not True or type(row.get("repetition")) is not int
                or any(type(row.get(key)) is not int for key in
                       ("milestones_completed", "physical_provider_calls", "usage_micro_usd"))
                or any(type(metrics.get(key)) is not int for key in
                       ("retained_eligibility_preserved", "escalations", "stagnation_events", "repairs", "initial_builder_calls"))
                or any(type(scouts.get(key)) is not int for key in ("calls", "admitted", "rejected", "rejected_batches"))
                or row.get("milestones_completed") != MILESTONES
                or row.get("physical_provider_calls") != 0 or row.get("usage_micro_usd") != 0
                or metrics.get("retained_eligibility_preserved", 0) < MILESTONES
                or metrics.get("escalations", 0) < MILESTONES
                or metrics.get("stagnation_events", 0) < MILESTONES
                or metrics.get("repairs") != 2 * MILESTONES
                or metrics.get("initial_builder_calls") != count * MILESTONES):
            raise ValueError("Rehearsal must exercise matched formation, preserved eligibility and bounded continuation")
        if (scouts.get("calls") != 4 or scouts.get("admitted", 0) < 4
                or scouts.get("rejected", 0) < 8 or scouts.get("rejected_batches") != 0):
            raise ValueError("Rehearsal must exercise independent scouts and wrong/duplicate probe controls")
        calls = row.get("invocations", [])
        if (not calls or any(type(call.get("usage_units")) is not int or call["usage_units"] != 0
                            or call.get("physical_dispatch") is not False
                            or type(call.get("metadata", {}).get("api_calls")) is not int
                            or call["metadata"]["api_calls"] != 0 for call in calls)
                or sum(call.get("metadata", {}).get("rehearsal_noop") is True for call in calls) != MILESTONES):
            raise ValueError("Rehearsal must retain its zero-API no-op failures")
    if proof_root is not None:
        proof_root = Path(proof_root).resolve()
        if Path(proof.get("root", "")).resolve() != proof_root:
            raise ValueError("Rehearsal root changed")
        states = validate_freeze(proof_root, expected, proof["freeze_manifest_sha256"])
        span_map = validate_timing_evidence(proof_root, expected, proof["freeze_manifest_sha256"])
        for row in rows:
            root = proof_root / row["run_id"]
            if read(root / "result.json") != row:
                raise ValueError("Rehearsal result summary changed")
            state = states[row["run_id"]]
            project = fixture(row["project_id"]).PROJECT
            for stage, result in enumerate(state["stages"]):
                freeze = read(root / f"stage-{stage}" / "initial-pool.json")
                initial_calls = [call for call in result["invocations"] if call["role"] == "builder"
                    and call["controller_metadata"].get("phase") == "initial"]
                scout_calls = [call for call in result["invocations"] if call["role"] == "scout"]
                count = expected["policies"][row["policy"]]["initial_builders"]
                model = expected["policies"][row["policy"]]["model"]
                if len(initial_calls) != count or len(scout_calls) != 2 or any(call["model"] != model for call in initial_calls):
                    raise ValueError("Rehearsal has an incomplete initial formation or scout batch")
                if any(span_map[call["physical_span_id"]]["finished_monotonic_ns"] > freeze["frozen_monotonic_ns"]
                       for call in initial_calls) or any(
                       span_map[call["physical_span_id"]]["started_monotonic_ns"] < freeze["frozen_monotonic_ns"]
                       for call in scout_calls):
                    raise ValueError("Scouts crossed the durable initial-pool barrier")
            suites = {"visible": cases_for(project, MILESTONES - 1, "visible") + state["stages"][-1]["probe_pool"],
                      "private": cases_for(project, MILESTONES - 1, "hidden")}
            for kind, cases in suites.items():
                receipt = read(root / f"final-{kind}-receipt.json")
                if not verify_receipt(receipt, state["files"], cases, expected["image"])["passed"]:
                    raise ValueError("Rehearsal release evidence did not pass")
                if receipt != row["final_hidden" if kind == "private" else "final_visible"]:
                    raise ValueError("Rehearsal receipt summary changed")
            release = GitStore(root / "release.git")
            if (release.head() != row["release_head"] or release.read_files() != state["files"]
                    or row.get("files_sha256") != digest(state["files"])):
                raise ValueError("Rehearsal accepted release changed")


def _run_owned(output, *, live=False, rehearsal=None, qualification=None, budget_ledger=Path("runs/first-live-budget.sqlite"),
        env_file=Path(".env.local"), image=DEFAULT_IMAGE, incremental_cap_micro_usd=None):
    expected = contract(image)
    cap = expected["budget"]["incremental_cap_micro_usd"]
    if incremental_cap_micro_usd is not None and incremental_cap_micro_usd != cap:
        raise ValueError("CLI study cap must exactly match the registered plan")
    output = Path(output).resolve()
    if output.exists():
        raise ValueError("Use a fresh output directory; previous evidence is never overwritten")
    if qualification is None:
        raise ValueError("Benchmark work requires its complete exact fixture qualification")
    from analysis.qualify_benchmark import validate_qualification
    qualification_path = Path(qualification).resolve()
    qualification_sha = sha_file(qualification_path)
    validate_qualification(qualification_path, digest(expected))
    if sha_file(qualification_path) != qualification_sha:
        raise ValueError("Fixture qualification changed while verifying it")
    qualification_binding = dict(path=str(qualification_path), sha256=qualification_sha,
                                 contract_sha=digest(expected))
    rehearsal_binding = None
    if live:
        if rehearsal is None:
            raise ValueError("Live work requires its complete exact offline rehearsal")
        rehearsal_path = Path(rehearsal).resolve()
        proof_bytes = rehearsal_path.read_bytes()
        proof = json.loads(proof_bytes)
        rehearsal_binding = dict(results_path=str(rehearsal_path),
            results_sha256=hashlib.sha256(proof_bytes).hexdigest(), contract_sha=proof.get("contract_sha"))
        validate_rehearsal(proof, expected, rehearsal_path.parent)
        if sha_file(rehearsal_path) != rehearsal_binding["results_sha256"]:
            raise ValueError("Rehearsal changed while verifying its evidence")
        ledger_path = Path(budget_ledger).resolve()
        if not ledger_path.is_file():
            raise ValueError("Live work requires the existing shared ledger")
        ledger = Ledger(ledger_path)
        before = ledger.budget()
        if (before["limit"] != expected["budget"]["shared_cumulative_cap_micro_usd"]
                or before["spent_or_reserved"] != expected["budget"]["expected_starting_usage_micro_usd"]
                or any(unsettled(ledger_path).values())):
            raise ValueError("Shared ledger no longer matches the quiescent registered starting budget")
        key = credential(env_file)
        models = {role: OpenAIWorker(key, model=model, max_output_tokens=OUTPUT_TOKENS, timeout=180)
                  for role, model in (("cheap", MODEL), ("strong", STRONG_MODEL))}
    else:
        models = {}
        ledger_path = output / "budget.sqlite"
    validator = BlackboxValidator(image, timeout_seconds=SUITE_TIMEOUT, case_timeout_seconds=CASE_TIMEOUT)
    okay, detail = validator.preflight()
    if not okay:
        raise RuntimeError("Docker preflight failed: " + detail)
    output.mkdir(parents=True)
    if not live:
        ledger = Ledger(ledger_path, budget_units=0)
        before = ledger.budget()
    namespace = PROTOCOL + "/" + digest([str(output), digest(expected)])[:24]
    budget = StudyBudget(ledger, namespace, cap if live else 0, output / "budget-admission.lock")
    snapshot = output / "source-snapshot" / "gossip_harness"
    snapshot.mkdir(parents=True)
    for name in CORE:
        shutil.copyfile(Path(__file__).parent / name, snapshot / name)
    for name in expected["extra_sources"]:
        extra = output / "source-snapshot" / name
        extra.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(Path(__file__).parent.parent / name, extra)
    shutil.copyfile(PLAN_PATH, output / "study-plan.json")
    save(output / "fixtures.json", {"projects": [fixture(project).PROJECT for project in PROJECT_MODULES]})
    save(output / "fault-banks.json", fault_banks())
    save(output / "preregistered.json", dict(contract=expected, roster=expected["roster"],
        fixtures=bound_file(output / "fixtures.json"), fault_banks=bound_file(output / "fault-banks.json"),
        qualification=qualification_binding,
        budget_before=before, namespace=namespace, incremental_commitment_cap=cap, rehearsal=rehearsal_binding))
    report = dict(experiment=PROTOCOL, root=str(output), mode="live" if live else "rehearsal",
        contract=expected, contract_sha=digest(expected), policies=expected["policies"], cases=[],
        trajectories=[], unexecuted=deepcopy(expected["roster"]), censored=[], active_case=None,
        phase="model_trajectories", status="running", budget_before=before, budget=before,
        namespace=namespace, incremental_commitment_cap=cap, started_at=time.time())
    report["rehearsal"] = rehearsal_binding
    report["qualification"] = qualification_binding
    save(output / "results.json", report)
    spans = Spans(output / "timings.json")
    configs = []
    try:
        with spans.measure("study", mode=report["mode"]):
            for project_id, policy, repetition in expected["roster"]:
                run_id = f"{project_id}-{policy}-{repetition}"
                root = output / run_id
                root.mkdir()
                config = dict(root=str(root), run_id=run_id, project_id=project_id, policy=policy,
                    repetition=repetition, live=live, image=image, namespace=namespace + "/" + run_id,
                    budget_ledger=str(ledger_path), contract_sha=digest(expected))
                configs.append(config)
                save(root / "config.json", config)
                report["active_case"] = {**config, "phase": "model_trajectory"}
                report["unexecuted"].remove([project_id, policy, repetition])
                save(output / "results.json", report)
                with spans.measure("model_trajectory", run_id=run_id) as span:
                    state = trajectory(config, expected, ledger, budget, models, spans)
                report["trajectories"].append(dict(run_id=run_id, terminal=state["terminal"],
                    trajectory_sha256=sha_file(root / "trajectory.json"), span_id=span["span_id"],
                    elapsed_seconds=span["elapsed_seconds"]))
                report["active_case"] = None
                report["budget"] = ledger.budget()
                save(output / "results.json", report)
            if any(unsettled(ledger_path).values()):
                raise RuntimeError("All model work must be settled before the private-evaluation barrier")
            freeze_trajectories(output, expected)
            freeze_sha = sha_file(output / "frozen-trajectories.json")
            report.update(phase="final_evaluation", freeze_manifest_sha256=freeze_sha)
            save(output / "results.json", report)
            for config in configs:
                report["active_case"] = {**config, "phase": "final_evaluation"}
                save(output / "results.json", report)
                with spans.measure("final_evaluation", run_id=config["run_id"]):
                    result = finalize(output, config, expected, freeze_sha, spans)
                report["cases"].append(result)
                report["active_case"] = None
                save(output / "results.json", report)
            report.update(status="finished", phase="finished")
    except BaseException as error:
        report.update(status="interrupted", failure_type=type(error).__name__)
        if report["active_case"] is not None:
            report["censored"].append({**report["active_case"], "reason": type(error).__name__})
            report["active_case"] = None
        raise
    finally:
        report["budget"] = ledger.budget()
        report["finished_at"] = time.time()
        report["incremental_micro_usd"] = report["budget"]["spent_or_reserved"] - before["spent_or_reserved"]
        report["unsettled"] = unsettled(ledger_path)
        save(output / "results.json", report)
        snapshot_accounting(ledger, output / "accounting-snapshot.sqlite")
    if not live:
        try:
            validate_rehearsal(report, expected, output)
        except Exception:
            report.update(status="qualification_failed", phase="qualification_failed")
            save(output / "results.json", report)
            raise
    return report


def run(output, *, live=False, rehearsal=None, qualification=None, budget_ledger=Path("runs/first-live-budget.sqlite"),
        env_file=Path(".env.local"), image=DEFAULT_IMAGE, incremental_cap_micro_usd=None):
    options = dict(live=live, rehearsal=rehearsal, qualification=qualification, budget_ledger=budget_ledger,
                   env_file=env_file, image=image, incremental_cap_micro_usd=incremental_cap_micro_usd)
    if live:
        with live_ownership(budget_ledger):
            return _run_owned(output, **options)
    return _run_owned(output, **options)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["run"])
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--rehearsal", type=Path)
    parser.add_argument("--qualification", type=Path, required=True)
    parser.add_argument("--budget-ledger", type=Path, default=Path("runs/first-live-budget.sqlite"))
    parser.add_argument("--env-file", type=Path, default=Path(".env.local"))
    parser.add_argument("--image", default=DEFAULT_IMAGE)
    parser.add_argument("--incremental-cap-micro-usd", type=int)
    options = vars(parser.parse_args())
    options.pop("command")
    try:
        report = run(**options)
    except Exception as error:
        print(f"Benchmark study stopped ({type(error).__name__}); inspect retained evidence before any further dispatch.",
              file=sys.stderr, flush=True)
        return 1
    print(json.dumps(dict(status=report["status"], accepted=sum(row["accepted"] for row in report["cases"]),
                         projects=len(report["cases"]), budget=report["budget"])), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
