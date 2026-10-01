"""Two-stage transport maintenance pilot with a study-wide private-test barrier.

The two policies share one strong maintainer/reviewer controller. Independent
cheap scouts contribute oracle-checked data probes before maintenance begins.
All candidate execution stays inside BlackboxValidator; provider outcomes and
accounting use the existing durable journal without automatic retries.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import random
import shutil
import sqlite3
import subprocess
import sys
import threading
import time

from .blackbox_validator import BlackboxValidator
from .gitstore import GitStore
from .ledger import BudgetExceeded, Ledger
from .pilot import DEFAULT_IMAGE, LeaseKeeper, Trace
from .sustained_checkpoint import save_checkpoint
from .sustained_experiment import credential, digest, local_promote, record_stage
from .verification_experiment import (CASE_TIMEOUT, CORE as PREVIOUS_CORE,
                                      SUITE_TIMEOUT, cases_for, oracle_gate,
                                      requirements, verify_receipt)
from .verification_journal import RequestJournal
from .worker import MODEL, STRONG_MODEL, OpenAIWorker, WorkerFailure, WorkerRequest, WorkerResult


PROTOCOL = "continuation-transport-v1"
POLICIES = ("strong-maintainer", "scout-assisted")
REPETITIONS = 2
MILESTONES = 2
OUTPUT_TOKENS = 12288
CORE = tuple(sorted(set(PREVIOUS_CORE + ("continuation_experiment.py", "continuation_stage.py",
                                        "continuation_transport.py", "transport.py"))))
PLAN_PATH = Path(__file__).parent.parent / "continuation-study-plan.json"


def save(path, value):
    return save_checkpoint(Path(path), value)


def read(path):
    return json.loads(Path(path).read_text())


def sha_file(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def fixture():
    from . import continuation_transport
    return continuation_transport


def study_plan():
    plan = read(PLAN_PATH)
    if (plan.get("protocol") != PROTOCOL or plan.get("milestones") != MILESTONES
            or plan.get("repetitions") != REPETITIONS or plan.get("policies") != list(POLICIES)
            or plan.get("execution", {}).get("output_tokens") != OUTPUT_TOKENS):
        raise ValueError("Unsupported continuation study plan")
    budget = plan.get("budget", {})
    for key in ("incremental_cap_micro_usd", "shared_cumulative_cap_micro_usd",
                "expected_starting_usage_micro_usd"):
        if type(budget.get(key)) is not int or budget[key] < 0:
            raise ValueError("Study budget must contain nonnegative integer microUSD")
    if not 0 < budget["incremental_cap_micro_usd"] <= budget["shared_cumulative_cap_micro_usd"]:
        raise ValueError("Invalid study budget bound")
    return plan


def roster(project_id):
    rows = [(project_id, policy, repetition) for repetition in range(REPETITIONS) for policy in POLICIES]
    random.Random(study_plan()["roster_seed"]).shuffle(rows)
    return rows


def contract(image=DEFAULT_IMAGE):
    plan, module = study_plan(), fixture()
    controller = plan["common_controller"]
    limits = {"max_reviews": controller["max_reviews"], "max_repairs": controller["max_repairs"],
              "max_new_probes": controller["max_new_reviewer_probes_per_stage"],
              "max_escalations": controller["max_escalations"],
              "stagnation_reviews": controller["stagnant_reviews_before_escalation"]}
    return dict(protocol=PROTOCOL,
        sources={name: sha_file(Path(__file__).parent / name) for name in CORE},
        plan_sha256=sha_file(PLAN_PATH), fixture_sha256=digest(module.PROJECT),
        baseline_sha256=module.BASELINE_SHA256, image=image,
        controller_runtime=dict(python=sys.version, implementation=sys.implementation.name,
            executable=sys.executable, platform=sys.platform,
            git=subprocess.check_output(["git", "--version"], text=True).strip()),
        project_id=module.PROJECT["id"], policies=list(POLICIES), repetitions=REPETITIONS,
        roster=[list(row) for row in roster(module.PROJECT["id"])], milestones=MILESTONES,
        limits=limits, scouts=deepcopy(plan["scouts"]), budget=deepcopy(plan["budget"]),
        case_timeout_seconds=CASE_TIMEOUT, suite_timeout_seconds=SUITE_TIMEOUT,
        output_tokens=OUTPUT_TOKENS,
        models={role: OpenAIWorker("profile-only", model=model, max_output_tokens=OUTPUT_TOKENS).profile_manifest()
                for role, model in (("cheap", MODEL), ("strong", STRONG_MODEL))},
        private_barrier="All four terminal source bindings durably frozen before any private evaluation",
        rehearsal_controls="Known valid source, charged-zero no-op failure, stalled inspections, bounded escalation, explicit acceptance",
        controller_crash_injection=False, hidden_feedback=False,
        timing="UTC and monotonic spans; physical calls distinct from replay; overlap is not additive wall time")


class Spans:
    """Persist starts before side effects and ends even when a call fails."""
    def __init__(self, path):
        self.path = Path(path)
        self.lock = threading.RLock()
        self.rows = []
        self.clock_id = f"{os.getpid()}-{time.monotonic_ns()}"

    @contextmanager
    def measure(self, kind, **labels):
        with self.lock:
            row = dict(span_id=len(self.rows), kind=kind, clock_id=self.clock_id,
                started_utc=datetime.now(timezone.utc).isoformat(), started_monotonic_ns=time.monotonic_ns(),
                status="running", **labels)
            self.rows.append(row)
            self._save()
        error_type = None
        try:
            yield row
        except BaseException as error:
            error_type = type(error).__name__
            raise
        finally:
            with self.lock:
                row["status"] = "failed" if error_type else "finished"
                if error_type:
                    row["error_type"] = error_type
                row.update(finished_utc=datetime.now(timezone.utc).isoformat(),
                           finished_monotonic_ns=time.monotonic_ns())
                row["elapsed_seconds"] = (row["finished_monotonic_ns"] - row["started_monotonic_ns"]) / 1e9
                self._save()

    def annotate(self, row, **values):
        with self.lock:
            row.update(values)
            self._save()

    def _save(self):
        save(self.path, dict(clock="time.monotonic_ns", clock_id=self.clock_id,
            interpretation="Nested and concurrent spans overlap; do not sum as wall time", spans=self.rows))


def unsettled(ledger_path):
    with sqlite3.connect(Path(ledger_path).resolve().as_uri() + "?mode=ro", uri=True) as db:
        pending = db.execute("SELECT count(*) FROM reservations WHERE spent IS NULL OR state!='settled'").fetchone()[0]
        promotions = db.execute("SELECT count(*) FROM intents WHERE state='pending'").fetchone()[0]
    return dict(reservations=pending, promotions=promotions)


class StudyBudget:
    """Serialize reservation admission across threads/processes in this study.

    Exact task namespace commitments enforce the study cap. Ledger.reserve
    independently enforces the immutable global cap in its own transaction.
    Settlement can only reduce commitments and may safely occur outside this lock.
    """
    def __init__(self, ledger, namespace, cap, lock_path):
        if type(cap) is not int or cap < 0 or not namespace:
            raise ValueError("Invalid study commitment cap or namespace")
        self.ledger, self.namespace, self.cap = ledger, namespace, cap
        self.lock_path = Path(lock_path)
        self.thread_lock = threading.RLock()

    def reserve(self, reservation, lease, units):
        prefix = self.namespace + "/"
        if not lease.task_id.startswith(prefix) or not reservation.startswith(lease.task_id + "/"):
            raise ValueError("Reservation is outside the study namespace")
        if type(units) is not int or units < 0:
            raise ValueError("Reservation must use nonnegative integer units")
        with self.thread_lock, self.lock_path.open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            with sqlite3.connect(self.ledger.path.resolve().as_uri() + "?mode=ro", uri=True) as db:
                rows = db.execute("SELECT id,task_id,amount,spent FROM reservations").fetchall()
            committed = sum(amount if spent is None else spent for _, task, amount, spent in rows
                            if task.startswith(prefix))
            prior = next((row for row in rows if row[0] == reservation), None)
            if prior is None and committed + units > self.cap:
                raise BudgetExceeded("Study commitment ceiling exhausted; no provider dispatch")
            self.ledger.reserve(reservation, lease, units, now=time.time())


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
    if metadata["round"] == 2:
        raise WorkerFailure("Offline no-effective-change control", usage_units=0,
                            metadata={"api_calls": 0, "rehearsal_noop": True})
    changes = {name: project["stages"][stage]["known_files"][name] for name in project["allowed_paths"]}
    changes["notes.json"] = json.dumps(dict(notes="Preserve cumulative requirements", remaining=[]))
    return WorkerResult(changes, "Offline known implementation", 0, {"api_calls": 0})


def scout_probes(project, module, stage, initial_files, prior, invoke, root, spans, run_id):
    """Independent inputs and parallel dispatch; admission remains deterministic."""
    from .verification_probes import parse_proposals
    public = cases_for(project, stage, "visible")
    context = dict(project_id=project["id"], stage_index=stage,
        milestones=[{key: item[key] for key in ("requirements", "specification", "spec") if key in item}
                    for item in project["stages"][:stage + 1]],
        public_cases=deepcopy(public))
    instructions = ("Find useful boundary and interaction tests for this maintenance milestone. "
        "Read scout-context.json and the stage-start source. Return only probes.json containing "
        "exactly {\"probes\": [{\"requirement\": requirement_id, \"input\": JSON_input, "
        "\"expected\": exact_JSON_output}]}, at most four proposals. Do not change source or write "
        "executable tests. A trusted oracle checks your labels; wrong labels are rejected without correction. "
        "You have no peer proposals. Do not claim that checks were executed.")
    inputs = {**initial_files, "scout-context.json": json.dumps(context, sort_keys=True)}
    def scout(index):
        return invoke(role="scout", model="cheap", files=deepcopy(inputs), instructions=instructions,
            allowed_paths=("probes.json",), feedback="", metadata=dict(role="scout", round=1,
                candidate_id=f"scout-{index}", scout_index=index, stage_index=stage, project_id=project["id"]))
    with spans.measure("scout_batch", run_id=run_id, stage_index=stage):
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(scout, index) for index in range(2)]
            results = [future.result() for future in futures]
    updated = deepcopy(prior)
    pool = list(updated.get("probe_pool", []))
    admitted, rejected = 0, 0
    receipts = []
    for index, result in enumerate(results):
        content = (result.changes.get("probes.json") if result is not None
                   and set(result.changes) == {"probes.json"} else None)
        receipt = parse_proposals(content, stage_index=stage, requirements=requirements(project, stage),
            reference=module.reference, validate_input=module.validate_input,
            origin=dict(project_id=project["id"], stage_index=stage, source="independent_scout", scout_index=index),
            existing_cases=public + pool, max_new=8 - admitted, namespace=project["id"])
        pool.extend(receipt["admitted_cases"])
        admitted += len(receipt["admitted_cases"])
        rejected += len(receipt["rejected"])
        receipts.append(receipt)
        save(root / f"scout-{index}-admission.json", receipt)
    updated["probe_pool"] = pool
    evidence = dict(calls=2, admitted=admitted, rejected=rejected, receipts=receipts,
                    source_sha256=digest(initial_files), shared_input_sha256=digest(inputs))
    save(root / "scouts.json", evidence)
    return updated, evidence


def trajectory(config, expected, ledger, budget, models, spans):
    from .continuation_stage import run_continuation_stage
    root = Path(config["root"])
    module, contract_sha = fixture(), digest(expected)
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
        initial_files = deepcopy(state["files"])

        def emit(kind, /, **data):
            data.pop("kind", None)
            data.pop("stage_index", None)
            trace.emit(kind, stage_index=stage, monotonic_ns=time.monotonic_ns(), **data)

        with spans.measure("stage", run_id=config["run_id"], stage_index=stage), LeaseKeeper(ledger) as keeper:
            keeper.claim(billing, "continuation-controller")

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
                save(stage_root / (label.replace("/", "-") + ".json"), dict(binding=binding, files=files))
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
            scouts = dict(calls=0, admitted=0, rejected=0, receipts=[])
            if config["policy"] == "scout-assisted":
                prior, scouts = scout_probes(project, module, stage, initial_files, prior,
                                             invoke, stage_root, spans, config["run_id"])
            approval = dict(files_sha256=digest(initial_files), source_valid=True,
                approval_id=base.head() if stage else "trusted-baseline:" + module.BASELINE_SHA256)
            result = run_continuation_stage(project, stage, "strong-reviewed", initial_files, prior,
                invoke=invoke, evaluate=evaluate, retain=retain, validate_probes=oracle_gate(module, project),
                emit=emit, candidate_seed=int(digest([project["id"], config["repetition"], stage])[:16], 16),
                limits=expected["limits"], baseline_approval=approval)
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


def validate_trajectory(state, expected, identity):
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
            or store.read_files(binding["tip_sha"]) != state["files"]):
        raise ValueError("Final Git/source binding changed")
    return state


def freeze_trajectories(output, expected):
    rows = []
    for identity in expected["roster"]:
        project_id, policy, repetition = identity
        run_id = f"{project_id}-{policy}-{repetition}"
        path = Path(output) / run_id / "trajectory.json"
        state = validate_trajectory(read(path), expected, identity)
        rows.append(dict(run_id=run_id, project_id=project_id, policy=policy, repetition=repetition,
            trajectory_sha256=sha_file(path), files_sha256=digest(state["files"]),
            final_binding=state["final_binding"], terminal=state["terminal"],
            evidence_sha256=digest(state["stages"][-1]["evidence_cases"]),
            milestones_completed=sum(row["completed"] for row in state["stages"])))
    manifest = dict(protocol=PROTOCOL, contract_sha=digest(expected), trajectories=rows,
        frozen_utc=datetime.now(timezone.utc).isoformat(), frozen_monotonic_ns=time.monotonic_ns(),
        private_evaluation_started=False)
    save(Path(output) / "frozen-trajectories.json", manifest)
    return manifest


def validate_freeze(output, expected, expected_sha):
    path = Path(output) / "frozen-trajectories.json"
    if sha_file(path) != expected_sha:
        raise ValueError("Study-wide freeze manifest changed")
    manifest = read(path)
    rows = manifest.get("trajectories", [])
    actual = [[row.get("project_id"), row.get("policy"), row.get("repetition")] for row in rows]
    if (manifest.get("protocol") != PROTOCOL or manifest.get("contract_sha") != digest(expected)
            or actual != expected["roster"] or len(rows) != 4
            or manifest.get("private_evaluation_started") is not False):
        raise ValueError("Exact four-trajectory freeze is required before private evaluation")
    states = {}
    for identity, row in zip(expected["roster"], rows):
        run_id = f"{identity[0]}-{identity[1]}-{identity[2]}"
        if row.get("run_id") != run_id or type(row.get("repetition")) is not int:
            raise ValueError("Frozen roster identity changed")
        path = Path(output) / run_id / "trajectory.json"
        if sha_file(path) != row.get("trajectory_sha256"):
            raise ValueError("Frozen trajectory changed")
        state = validate_trajectory(read(path), expected, identity)
        if (row.get("final_binding") != state["final_binding"]
                or row.get("files_sha256") != digest(state["files"])
                or row.get("evidence_sha256") != digest(state["stages"][-1]["evidence_cases"])):
            raise ValueError("Frozen source or evidence changed")
        states[run_id] = state
    return states


def finalize(output, config, expected, freeze_sha, spans):
    states = validate_freeze(output, expected, freeze_sha)
    root = Path(config["root"])
    state = states[config["run_id"]]
    project = fixture().PROJECT
    full = len(state["stages"]) == MILESTONES and all(row["completed"] for row in state["stages"])
    active = state["stages"][-1]["probe_pool"] if len(state["stages"]) == MILESTONES else []
    suites = {"visible": cases_for(project, MILESTONES - 1, "visible") + active,
              "private": cases_for(project, MILESTONES - 1, "hidden")}
    receipts = {}
    for kind, cases in suites.items():
        validate_freeze(output, expected, freeze_sha)
        validator = BlackboxValidator(config["image"], timeout_seconds=SUITE_TIMEOUT,
                                       case_timeout_seconds=CASE_TIMEOUT)
        with spans.measure("docker_validation", run_id=config["run_id"], purpose="final_" + kind,
                case_count=len(cases), freeze_manifest_sha256=freeze_sha) as span:
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
        scouts={key: sum(row["scouts"][key] for row in state["stages"]) for key in ("calls", "admitted", "rejected")})
    save(root / "result.json", result)
    return result


def validate_rehearsal(proof, expected, proof_root=None):
    rows = proof.get("cases", [])
    actual = [[row.get("project_id"), row.get("policy"), row.get("repetition")] for row in rows]
    if (proof.get("experiment") != PROTOCOL or proof.get("mode") != "rehearsal"
            or proof.get("contract") != expected or proof.get("contract_sha") != digest(expected)
            or proof.get("status") != "finished" or proof.get("unexecuted") or proof.get("censored")
            or proof.get("active_case") is not None or actual != expected["roster"] or len(rows) != 4
            or type(proof.get("incremental_micro_usd")) is not int or proof["incremental_micro_usd"] != 0):
        raise ValueError("Exact complete four-trajectory rehearsal is required")
    for row in rows:
        metrics, scouts = row.get("metrics", {}), row.get("scouts", {})
        if (row.get("accepted") is not True or type(row.get("repetition")) is not int
                or any(type(row.get(key)) is not int for key in
                       ("milestones_completed", "physical_provider_calls", "usage_micro_usd"))
                or any(type(metrics.get(key)) is not int for key in
                       ("retained_eligibility_preserved", "escalations", "stagnation_events", "repairs"))
                or any(type(scouts.get(key)) is not int for key in ("calls", "admitted", "rejected"))
                or row.get("milestones_completed") != MILESTONES
                or row.get("physical_provider_calls") != 0 or row.get("usage_micro_usd") != 0
                or metrics.get("retained_eligibility_preserved", 0) < MILESTONES
                or metrics.get("escalations", 0) < MILESTONES
                or metrics.get("stagnation_events", 0) < MILESTONES
                or metrics.get("repairs", 0) < 2 * MILESTONES):
            raise ValueError("Rehearsal must exercise preserved no-op eligibility and bounded continuation")
        if row["policy"] == "scout-assisted":
            if scouts.get("calls") != 4 or scouts.get("admitted", 0) < 4 or scouts.get("rejected", 0) < 8:
                raise ValueError("Rehearsal must exercise independent scouts and negative probe controls")
        elif any(scouts.get(key) != 0 for key in ("calls", "admitted", "rejected")):
            raise ValueError("Maintainer-only rehearsal cannot contain scouts")
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
        project = fixture().PROJECT
        for row in rows:
            root = proof_root / row["run_id"]
            if read(root / "result.json") != row:
                raise ValueError("Rehearsal result summary changed")
            state = states[row["run_id"]]
            suites = {"visible": cases_for(project, 1, "visible") + state["stages"][-1]["probe_pool"],
                      "private": cases_for(project, 1, "hidden")}
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


def snapshot_accounting(ledger, destination):
    with sqlite3.connect(ledger.path.resolve().as_uri() + "?mode=ro", uri=True) as source:
        with sqlite3.connect(destination) as target:
            source.backup(target)


def _run_owned(output, *, live=False, rehearsal=None, budget_ledger=Path("runs/first-live-budget.sqlite"),
        env_file=Path(".env.local"), image=DEFAULT_IMAGE, incremental_cap_micro_usd=None):
    expected = contract(image)
    cap = expected["budget"]["incremental_cap_micro_usd"]
    if incremental_cap_micro_usd is not None and incremental_cap_micro_usd != cap:
        raise ValueError("CLI study cap must exactly match the registered plan")
    output = Path(output).resolve()
    if output.exists():
        raise ValueError("Use a fresh output directory; previous evidence is never overwritten")
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
    shutil.copyfile(PLAN_PATH, output / "study-plan.json")
    save(output / "fixtures.json", {"projects": [fixture().PROJECT]})
    save(output / "preregistered.json", dict(contract=expected, roster=expected["roster"],
        budget_before=before, namespace=namespace, incremental_commitment_cap=cap, rehearsal=rehearsal_binding))
    report = dict(experiment=PROTOCOL, root=str(output), mode="live" if live else "rehearsal",
        contract=expected, contract_sha=digest(expected), repetitions=REPETITIONS, cases=[],
        trajectories=[], unexecuted=deepcopy(expected["roster"]), censored=[], active_case=None,
        phase="model_trajectories", status="running", budget_before=before, budget=before,
        namespace=namespace, incremental_commitment_cap=cap, started_at=time.time())
    report["rehearsal"] = rehearsal_binding
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


@contextmanager
def live_ownership(ledger_path):
    """Prevent two fresh outputs spending the same registered pilot allowance."""
    ledger_path = Path(ledger_path).resolve()
    if not ledger_path.is_file():
        raise ValueError("Live work requires the existing shared ledger")
    with Path(str(ledger_path) + ".continuation-live.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("Another continuation run owns the shared ledger") from None
        yield


def run(output, *, live=False, rehearsal=None, budget_ledger=Path("runs/first-live-budget.sqlite"),
        env_file=Path(".env.local"), image=DEFAULT_IMAGE, incremental_cap_micro_usd=None):
    options = dict(live=live, rehearsal=rehearsal, budget_ledger=budget_ledger,
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
    parser.add_argument("--budget-ledger", type=Path, default=Path("runs/first-live-budget.sqlite"))
    parser.add_argument("--env-file", type=Path, default=Path(".env.local"))
    parser.add_argument("--image", default=DEFAULT_IMAGE)
    parser.add_argument("--incremental-cap-micro-usd", type=int)
    options = vars(parser.parse_args())
    options.pop("command")
    try:
        report = run(**options)
    except Exception as error:
        print(f"Continuation study stopped ({type(error).__name__}); inspect retained evidence before any further dispatch.",
              file=sys.stderr, flush=True)
        return 1
    print(json.dumps(dict(status=report["status"], accepted=sum(row["accepted"] for row in report["cases"]),
                         projects=len(report["cases"]), budget=report["budget"])), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
