"""Twelve-trajectory controller pilot with shared setup and a final barrier.

Provider work, immutable source imports, candidate execution and independent
final acceptance have distinct receipts. A complete exact rehearsal and audit,
fixture qualification and an explicit funded plan gate every live dispatch.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timezone
import importlib
import json
import os
import tempfile
from pathlib import Path
import shutil
import subprocess
import sys
import threading
import time
from typing import Any

from .benchmark_experiment import CORE as PREVIOUS_CORE, known_result, scout_probes
from .blackbox_validator import BlackboxValidator
from .continuation_experiment import Spans, StudyBudget, live_ownership, read, sha_file, snapshot_accounting, unsettled
from .gitstore import GitStore
from .ledger import Ledger
from .pilot import DEFAULT_IMAGE, LeaseKeeper, Trace
from .sustained_experiment import credential, digest, local_promote, record_stage
from .verification_experiment import CASE_TIMEOUT, SUITE_TIMEOUT, cases_for, oracle_gate, requirements, verify_receipt
from .verification_journal import RequestJournal
from .worker import MODEL, STRONG_MODEL, OpenAIWorker, WorkerFailure, WorkerRequest, WorkerResult


PROTOCOL = "continuation-followup-v1"
SHARED_PROTOCOL = "continuation-followup-shared-setup-v1"
OUTPUT_TOKENS = 12288
MILESTONES = 2
REPETITIONS = 2
PROJECT_MODULES = {"warehouse": "benchmark_warehouse", "job-queue": "benchmark_job_queue"}
POLICIES: dict[str, dict[str, Any]] = {
    "current-independent": dict(controller="current", formation="independent", initial_builders=4, model="cheap"),
    "improved-independent": dict(controller="improved", formation="independent", initial_builders=4, model="cheap"),
    "improved-sequential": dict(controller="improved", formation="sequential", initial_builders=4, model="cheap"),
}
LIMITS = dict(max_reviews=4, max_repairs=2, max_new_probes=8,
              max_escalations=1, stagnation_reviews=2)
PLAN_PATH = Path(__file__).resolve().parent.parent / "continuation-followup-study-plan.json"
CORE = tuple(sorted(set(PREVIOUS_CORE + ("continuation_followup.py", "continuation_controller.py",
    *[name + ".py" for name in PROJECT_MODULES.values()]))))
EXTRA_SOURCES = ("analysis/qualify_continuation_followup.py", "analysis/audit_continuation_followup.py",
    "analysis/audit_followup_stage.py", "analysis/audit_continuation.py", "analysis/audit_benchmark.py",
    "gossip_harness/verification_audit.py", "analysis/continuation_followup_comparison.py")
REHEARSAL_VERSION = "controller-rehearsal-v2"


def save(path, value):
    """Durable JSON that also preserves deliberately invalid Unicode inputs."""
    data = (json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True,
                       allow_nan=False) + "\n").encode("utf-8")
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="wb", prefix=f".{target.name}.", suffix=".tmp",
                                         dir=target.parent, delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
        temporary = None
        descriptor = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return sha_file(target)


def fixture(project_id):
    if project_id not in PROJECT_MODULES:
        raise ValueError("Unknown follow-up project")
    return importlib.import_module("." + PROJECT_MODULES[project_id], __package__)


def validate_plan(plan):
    """Reject changed scientific opportunities or an incomplete paired roster."""
    if (not isinstance(plan, dict) or plan.get("protocol") != PROTOCOL
            or plan.get("projects") != list(PROJECT_MODULES)
            or type(plan.get("milestones")) is not int or plan["milestones"] != MILESTONES
            or type(plan.get("repetitions")) is not int or plan["repetitions"] != REPETITIONS
            or plan.get("policies") != POLICIES):
        raise ValueError("Unsupported follow-up project, controller or formation contract")
    controller = plan.get("controller", {})
    fixed = {**LIMITS, "output_tokens": OUTPUT_TOKENS, "scouts": 2, "proposals_per_scout": 4}
    if any(type(controller.get(key)) is not int or controller[key] != value for key, value in fixed.items()):
        raise ValueError("Follow-up opportunities and token ceilings must remain matched")
    retries = plan.get("improved_controller_rules", {}).get("uncertainty_focus_retries")
    if type(retries) is not int or retries != 1:
        raise ValueError("The improved controller has exactly one bounded uncertainty-focus retry")
    if plan.get("rehearsal", {}).get("version") != REHEARSAL_VERSION:
        raise ValueError("Exact scripted rehearsal version required")
    expected = {(project, policy, repetition) for project in PROJECT_MODULES
                for policy in POLICIES for repetition in range(REPETITIONS)}
    rows = plan.get("roster")
    if (not isinstance(rows, list) or len(rows) != 12
            or any(not isinstance(row, list) or len(row) != 3 or type(row[2]) is not int for row in rows)
            or len({tuple(row) for row in rows}) != 12 or {tuple(row) for row in rows} != expected):
        raise ValueError("Exact twelve-trajectory paired roster required")
    blocks = plan.get("block_order")
    expected_blocks = {(project, repetition) for project in PROJECT_MODULES for repetition in range(REPETITIONS)}
    if (not isinstance(blocks, list) or len(blocks) != 4
            or any(not isinstance(row, list) or len(row) != 2 or type(row[1]) is not int for row in blocks)
            or {tuple(row) for row in blocks} != expected_blocks):
        raise ValueError("Exact four-block setup order required")
    if plan.get("generation_order") not in (["independent", "sequential"], ["sequential", "independent"]):
        raise ValueError("Both independent and serial formation must precede common scouts")
    if any(plan.get("pairing", {}).get(key) != value for key, value in
           {"stage0": "shared-independent-pool-and-common-scouts", "later": "arm-specific"}.items()):
        raise ValueError("Shared setup and later arm-specific calls must be explicit")
    budget = plan.get("budget", {})
    names = ("incremental_cap_micro_usd", "shared_cumulative_cap_micro_usd", "expected_starting_usage_micro_usd")
    if any(type(budget.get(key)) is not int or budget[key] < 0 for key in names):
        raise ValueError("Budget bounds must be nonnegative integer microUSD")
    if not 0 < budget[names[0]] <= budget[names[1]] - budget[names[2]]:
        raise ValueError("Study ceiling exceeds remaining authorized cumulative budget")
    return deepcopy(plan)


def study_plan():
    return validate_plan(read(PLAN_PATH))


def contract(image=DEFAULT_IMAGE):
    """Bind the development contract without implying frozen execution readiness."""
    from analysis.qualify_continuation_followup import matrix_for
    plan = study_plan()
    modules = {project: fixture(project) for project in PROJECT_MODULES}
    return dict(protocol=PROTOCOL, status="cohort_implementation",
        sources={name: sha_file(Path(__file__).parent / name) for name in CORE},
        extra_sources={name: sha_file(Path(__file__).parent.parent / name) for name in EXTRA_SOURCES},
        plan_sha256=sha_file(PLAN_PATH), fixtures={key: dict(fixture_sha256=digest(module.PROJECT),
            baseline_sha256=digest(module.PROJECT["initial_files"]), module=PROJECT_MODULES[key])
            for key, module in modules.items()}, image=image,
        controller_runtime=dict(python=sys.version, implementation=sys.implementation.name,
            executable=sys.executable, platform=sys.platform,
            git=subprocess.check_output(["git", "--version"], text=True).strip()),
        project_ids=list(PROJECT_MODULES), policies=deepcopy(POLICIES),
        roster=deepcopy(plan["roster"]), block_order=deepcopy(plan["block_order"]),
        milestones=MILESTONES, repetitions=REPETITIONS, limits=deepcopy(LIMITS),
        budget=deepcopy(plan["budget"]), output_tokens=OUTPUT_TOKENS,
        case_timeout_seconds=CASE_TIMEOUT, suite_timeout_seconds=SUITE_TIMEOUT,
        models={role: OpenAIWorker("profile-only", model=model,
            max_output_tokens=OUTPUT_TOKENS).profile_manifest()
            for role, model in (("cheap", MODEL), ("strong", STRONG_MODEL))},
        pairing=deepcopy(plan["pairing"]), improved_controller_rules=deepcopy(plan["improved_controller_rules"]),
        call_envelope=call_envelope(),
        hidden_feedback=False, physical_execution="BlackboxValidator Docker only",
        rehearsal_version=REHEARSAL_VERSION,
        rehearsal_controls=deepcopy(plan.get("rehearsal", {})),
        pilot_analysis=deepcopy(plan.get("pilot_analysis", {})),
        qualification_protocol="continuation-followup-fixture-qualification-v2",
        qualification_matrix_sha256=digest(matrix_for(list(modules.values()))),
        live_enabled=plan.get("readiness", {}).get("live_authorized") is True,
        full_cohort_qualification=True,
        private_barrier="All four shared setups and twelve terminal trajectories frozen before any private final execution")


def call_envelope():
    """Worst-case offered calls; full-context cash bounds are not forecasts.

    Four blocks each share four A/B builder calls and all three arms' two
    scouts. Later stages are independent. A strong escalation replaces a cheap
    repair inside the ordinary two-repair cap, never adds a model opportunity.
    """
    profiles = {role: OpenAIWorker("profile-only", model=model,
        max_output_tokens=OUTPUT_TOKENS).profile_manifest()
        for role, model in (("cheap", MODEL), ("strong", STRONG_MODEL))}
    counts = dict(initial_builders=80, scouts=32, cheap_repairs=24, strong_repairs=24, reviewers=96)
    cheap, strong = 136, 120
    bounds = {role: profile["reservation_units"] for role, profile in profiles.items()}
    return dict(protocol=PROTOCOL, planned_trajectories=12, planned_blocks=4,
        stages_per_trajectory=2, maximum_physical_calls=sum(counts.values()),
        maximum_cost_allocation_by_role=counts,
        maximum_cost_allocation_by_model=dict(cheap=cheap, strong=strong),
        maximum_calls_by_model=dict(cheap=160, strong=120),
        maximum_calls_by_role=dict(initial_builders=80, scouts=32, cheap_repairs=48, strong_repairs=24, reviewers=96),
        allocation_constraint="Cheap and strong repair calls sum to at most48, of which at most24 are strong. Per-model maxima cannot occur together.",
        shared_stage0=dict(physical_builders=32, physical_scouts=8,
            attributed_initial_builder_opportunities=48, attributed_scout_opportunities=24),
        reservation_micro_usd_per_call=bounds,
        conservative_full_cohort_micro_usd=cheap * bounds["cheap"] + strong * bounds["strong"],
        maximum_phase_reservation_micro_usd=max(4 * bounds["cheap"], bounds["strong"]),
        accounting="Shared setup cash counted once. Per-arm opportunities are attribution, not extra physical calls.",
        bound_interpretation="Sum of full-context maximum-call reservations; not expected realized spend.")


def live_readiness(plan):
    """A plan cannot replace qualification, exact rehearsal or funding evidence."""
    validate_plan(plan)
    envelope = call_envelope()
    blockers = []
    if plan.get("readiness", {}).get("live_authorized") is not True:
        blockers.append("plan does not authorize live execution")
    funding = plan.get("budget", {}).get("funding", {})
    cap = plan["budget"]["incremental_cap_micro_usd"]
    if (funding.get("status") != "approved_capped_cohort_admission"
            or funding.get("policy") != "bounded_budget_termination_no_incomplete_comparison"
            or type(funding.get("approved_incremental_micro_usd")) is not int
            or funding.get("approved_incremental_micro_usd") != cap):
        blockers.append("prospective capped-cohort funding must bind the exact authorized incremental ceiling")
    if cap < envelope["maximum_phase_reservation_micro_usd"]:
        blockers.append("authorized ceiling cannot reserve the largest offered execution phase")
    return dict(ready=False, plan_ready=not blockers, protocol=PROTOCOL,
        blockers=blockers + ["exact fixture qualification, complete matching rehearsal and independent audit must be supplied"],
        authorized_headroom_micro_usd=plan["budget"]["incremental_cap_micro_usd"],
        conservative_full_cohort_micro_usd=envelope["conservative_full_cohort_micro_usd"],
        paid_calls_enabled=False)


def rehearsal_result(project, module, stage, metadata, *, repetition, controller_mode, cohort_controls):
    """Explicit scripted faults exercise the production execution boundaries.

    Fault wrappers are confined to zero-API rehearsal sources. They never enter
    live prompts or provide a fallback for a failed live request.
    """
    if not cohort_controls or metadata["role"] == "scout":
        return known_result(project, module, stage, metadata)
    role = metadata["role"]
    focused = stage == 1 and repetition == 1 and controller_mode == "improved"
    if role == "reviewer":
        aliases = metadata["alias_by_slot"]
        number = metadata["round"]
        if controller_mode == "current":
            alias, action = aliases["slot-0"], "repair" if number == 1 else "accept" if number == 4 else "inspect"
        elif focused:
            alias, action = aliases["slot-0"], "inspect" if number == 1 else "accept"
        else:
            alias = aliases.get("repair-2", aliases.get("repair-1", aliases["slot-0"]))
            action = "accept"
        remaining = [] if action == "accept" else [requirements(project, stage)[0]]
        return WorkerResult({"review.json": json.dumps(dict(candidate=alias, action=action,
            notes="Scripted controller boundary exercise", remaining=remaining, probes=[]))},
            "Zero-API follow-up reviewer", 0, {"api_calls": 0, "rehearsal_version": REHEARSAL_VERSION})
    repair = metadata.get("phase") == "repair"
    first_repair = repair and metadata["round"] == 2
    if first_repair and repetition == 1:
        raise WorkerFailure("Zero-API no-effective-change rehearsal control", usage_units=0,
            metadata={"api_calls": 0, "rehearsal_noop": True, "rehearsal_version": REHEARSAL_VERSION})
    changes = {name: project["stages"][stage]["known_files"][name] for name in project["allowed_paths"]}
    if (not repair and not focused) or (first_repair and repetition == 0):
        operations = {"warehouse": ("allocate", "hold"), "job-queue": ("ack", "renew")}
        operation = operations[project["id"]][int(first_repair)]
        service = project["allowed_paths"][-1]
        changes[service] += ("\n_REHEARSAL_ORIGINAL_EXECUTE = execute\n"
            f"REHEARSAL_FAULT_OP = {operation!r}\n"
            "def execute(*args, **kwargs):\n"
            "    command = args[-1]\n"
            "    if isinstance(command, dict) and command.get('op') == REHEARSAL_FAULT_OP:\n"
            "        return {'rehearsal_fault': REHEARSAL_FAULT_OP}\n"
            "    return _REHEARSAL_ORIGINAL_EXECUTE(*args, **kwargs)\n")
    changes["notes.json"] = json.dumps(dict(notes="Scripted maintenance checkpoint", remaining=[]))
    return WorkerResult(changes, "Zero-API follow-up source control", 0,
                        {"api_calls": 0, "rehearsal_version": REHEARSAL_VERSION})


def bound_file(path):
    path = Path(path).resolve()
    return dict(path=str(path), sha256=sha_file(path))


def candidate_seed(project_id, repetition, stage=0):
    if project_id not in PROJECT_MODULES or type(repetition) is not int or repetition not in range(REPETITIONS):
        raise ValueError("Unknown paired block")
    if type(stage) is not int or stage not in range(MILESTONES):
        raise ValueError("Unknown milestone")
    return int(digest([PROTOCOL, project_id, repetition, stage])[:16], 16)


class ExecutionSession:
    """A fresh scoped Git, request journal and accounting boundary for one stage."""

    def __init__(self, root, *, project, module, stage, session_id, namespace,
                 contract_sha256, image, ledger, budget, spans, fatal,
                 validator_factory=BlackboxValidator, mode="rehearsal", models=None,
                 initial_files=None, base_store=None, session_kind="shared_setup",
                 billing_suffix=None, repetition=0, controller_mode="current",
                 cohort_controls=False, contract_check=None):
        if mode not in {"rehearsal", "live"} or session_kind not in {"shared_setup", "stage"}:
            raise ValueError("Explicit supported execution mode and session purpose required")
        self.root = Path(root).resolve()
        if self.root.exists():
            raise ValueError("Use a fresh session directory; retained evidence is never overwritten")
        self.root.mkdir(parents=True)
        for name in ("requests", "evaluations"):
            (self.root / name).mkdir()
        self.project, self.module, self.stage = project, module, stage
        self.session_id, self.namespace = session_id, namespace
        self.contract_sha256, self.image = contract_sha256, image
        self.ledger, self.budget, self.spans, self.fatal = ledger, budget, spans, fatal
        self.mode, self.models, self.session_kind = mode, models or {}, session_kind
        self.repetition, self.controller_mode = repetition, controller_mode
        self.cohort_controls, self.contract_check = cohort_controls, contract_check
        self.initial_files = deepcopy(project["initial_files"] if initial_files is None else initial_files)
        self.validator_factory = validator_factory
        self.validator_origin = "physical_docker" if validator_factory is BlackboxValidator else "unit_test_control"
        self.base = (GitStore.fork(base_store, self.root / "baseline.git") if base_store else
                     GitStore.create(self.root / "baseline.git", self.initial_files))
        if self.base.read_files() != self.initial_files:
            raise ValueError("Stage baseline is not its actual accepted lineage")
        self.journal = RequestJournal(self.root / "journal")
        self.trace = Trace(self.root / "trace.jsonl")
        self.calls, self.exports, self.validations = {}, [], []
        self.started_call_ids: set[str] = set()
        self.lock = threading.RLock()
        self.keeper = LeaseKeeper(ledger)
        self.billing = namespace + "/" + (billing_suffix or session_id)

    def __enter__(self):
        self.ledger.add_task(self.billing)
        self.keeper.__enter__()
        self.keeper.claim(self.billing, "followup-controller")
        return self

    def __exit__(self, *exc):
        return self.keeper.__exit__(*exc)

    def emit(self, kind, /, **data):
        data.pop("kind", None)
        data.pop("stage_index", None)
        self.trace.emit(kind, stage_index=self.stage, session_id=self.session_id,
                        monotonic_ns=time.monotonic_ns(), **data)

    def _prepare_invoke(self, *, role, model, files, instructions, allowed_paths, feedback, metadata):
        if self.fatal.is_set():
            raise RuntimeError("An earlier execution requires investigation")
        if model not in {"cheap", "strong"}:
            raise ValueError("Unknown pinned model profile")
        if self.mode == "live" and not isinstance(self.models.get(model), OpenAIWorker):
            raise ValueError("Missing or invalid live model; scripted fallback is forbidden")
        if self.contract_check:
            self.contract_check()
        call_id = f"{role}-{metadata['candidate_id']}-{metadata['round']}"
        with self.lock:
            if call_id in self.started_call_ids:
                raise ValueError("A session cannot replace or implicitly replay a retained call")
            self.started_call_ids.add(call_id)
        reservation = self.billing + "/" + call_id
        request = WorkerRequest("task-" + digest([self.contract_sha256, reservation])[:24], instructions,
            tuple(allowed_paths), deepcopy(files), self.base.head(), metadata["round"], feedback)
        save(self.root / "requests" / f"{call_id}.request.json", asdict(request))
        row = dict(call_id=call_id, reservation=reservation, role=role, model=model,
            request_sha256=digest(asdict(request)), physical_dispatch=False,
            execution_kind="physical_provider_request" if self.mode == "live" else "scripted_worker",
            controller_metadata=deepcopy(metadata),
            journal_paths={key: str(path) for key, path in self.journal.paths(call_id).items()})

        worker = self.models[model] if self.mode == "live" else None
        units = worker.reservation_units(request) if worker is not None else 0

        return call_id, reservation, request, row, worker, units

    def invoke(self, *, role, model, files, instructions, allowed_paths, feedback, metadata):
        try:
            call_id, reservation, request, row, worker, units = self._prepare_invoke(
                role=role, model=model, files=files, instructions=instructions,
                allowed_paths=allowed_paths, feedback=feedback, metadata=metadata)
        except BaseException:
            self.fatal.set()
            raise

        def actual_call():
            if self.fatal.is_set():
                raise RuntimeError("Dispatch stopped after another failure")
            if self.contract_check:
                self.contract_check()
            row["physical_dispatch"] = self.mode == "live"
            with self.spans.measure(row["execution_kind"], run_id=self.session_id, stage_index=self.stage,
                    call_id=call_id, role=role, model=model) as span:
                row["physical_span_id"] = span["span_id"]
                if self.mode == "live":
                    if not isinstance(worker, OpenAIWorker):
                        raise ValueError("Invalid live worker; scripted fallback is forbidden")
                    return worker.run(request)
                return rehearsal_result(self.project, self.module, self.stage, metadata,
                    repetition=self.repetition, controller_mode=self.controller_mode,
                    cohort_controls=self.cohort_controls)

        try:
            with self.spans.measure("worker_request", run_id=self.session_id, stage_index=self.stage,
                    call_id=call_id, role=role, model=model) as span:
                row["request_span_id"] = span["span_id"]
                outcome = self.journal.execute(call_id, request, reservation, actual_call,
                    lambda: self.budget.reserve(reservation, self.keeper.check(self.billing), units),
                    lambda amount: self.ledger.settle(reservation, amount))
            row.update(asdict(outcome), outcome="result")
            return outcome
        except WorkerFailure as error:
            row.update(outcome="failure", usage_units=error.usage_units,
                       metadata=error.metadata, failure=str(error))
            if error.usage_units is None or error.metadata.get("halt"):
                self.fatal.set()
                raise RuntimeError("Unknown outcome requires investigation; no automatic retry") from None
            return None
        except BaseException as error:
            self.fatal.set()
            row.update(outcome="stopped", usage_units=None, error_type=type(error).__name__)
            raise
        finally:
            path = self.journal.paths(call_id)["result"]
            row["journal_result_sha256"] = sha_file(path) if path.exists() else None
            row["journal_replay"] = "physical_span_id" not in row and path.exists()
            with self.lock:
                if call_id in self.calls:
                    raise ValueError("A session cannot silently replace a retained call")
                self.calls[call_id] = row
                save(self.root / "requests" / f"{call_id}.result.json", row)

    def retain(self, files, label):
        if (set(files) != set(self.project["initial_files"])
                or any(files[path] != content for path, content in self.project["initial_files"].items()
                       if path not in self.project["allowed_paths"])):
            raise ValueError("Retained proposal violated trusted source scope")
        filename = label.replace("/", "-")
        store = GitStore.fork(self.base, self.root / (filename + ".git"))
        tip = store.propose({name: files[name] for name in self.project["allowed_paths"]}, message=label)
        if store.read_files(tip) != files:
            raise ValueError("Retained Git proposal differs from source")
        binding = dict(store_path=str(store.path), tip_sha=tip, files_sha256=digest(files))
        path = self.root / (filename + ".json")
        save(path, dict(label=label, binding=binding, files=files))
        self.exports.append(dict(label=label, **bound_file(path), files_sha256=digest(files), binding=binding))
        return binding

    def evaluate(self, files, cases, label):
        validator = self.validator_factory(self.image, timeout_seconds=SUITE_TIMEOUT,
                                           case_timeout_seconds=CASE_TIMEOUT)
        with self.spans.measure("docker_validation", run_id=self.session_id, stage_index=self.stage,
                label=label, purpose="stage_evidence", case_count=len(cases),
                validator_origin=self.validator_origin) as span:
            result = validator.evaluate(files, cases)
            path = self.root / "evaluations" / f"{span['span_id']}.json"
            save(path, result)
            self.spans.annotate(span, receipt_path=str(path), receipt_sha256=sha_file(path))
            self.validations.append(dict(label=label, span_id=span["span_id"], **bound_file(path),
                                         files=deepcopy(files), cases=deepcopy(cases)))
            return verify_receipt(result, files, cases, self.image)

    def freeze_initial(self, pool):
        path = self.root / "initial-pool.json"
        if path.exists():
            raise ValueError("A shared pool may only be frozen once")
        save(path, dict(protocol=SHARED_PROTOCOL, session_id=self.session_id,
            stage_index=self.stage, pool=pool, pool_sha256=digest(pool),
            frozen_utc=datetime.now(timezone.utc).isoformat(), frozen_monotonic_ns=time.monotonic_ns()))
        return dict(freeze_id=str(path), pool_sha256=digest(pool), scouts=[])

    def finish(self, result):
        accounting_record = ({key: result[key] for key in ("completed", "status", "selected_binding")}
            if self.session_kind == "stage" else dict(completed=False, status="shared_initial_setup", selected_binding=None))
        record_stage(self.root, accounting_record, self.ledger, self.keeper.check(self.billing))
        accounting_store = GitStore(self.root / "record.git")
        value = dict(protocol=SHARED_PROTOCOL if self.session_kind == "shared_setup" else PROTOCOL,
            session_id=self.session_id, mode="offline" if self.mode == "rehearsal" and self.session_kind == "shared_setup" else self.mode,
            execution_mode=self.mode, session_kind=self.session_kind,
            project_id=self.project["id"], stage_index=self.stage, billing=self.billing,
            contract_sha256=self.contract_sha256, image=self.image, result=result,
            invocations=[self.calls[key] for key in sorted(self.calls)], exports=self.exports,
            validations=self.validations, initial_files_sha256=digest(self.initial_files),
            validator_origin=self.validator_origin,
            accounting_record=dict(store_path=str(accounting_store.path), accepted_head=accounting_store.head(),
                                   record=accounting_record),
            physical_provider_calls=sum(row["physical_dispatch"] for row in self.calls.values()),
            usage_micro_usd=sum(row["usage_units"] for row in self.calls.values()))
        save(self.root / "session.json", value)
        return bound_file(self.root / "session.json")


# Retained setup API name; production callers specify mode explicitly above.
OfflineSession = ExecutionSession


def prepare_shared_block(output, project_id, repetition, *, contract_sha256, image,
                         ledger, budget, spans, generation_order=("independent", "sequential"),
                         validator_factory=BlackboxValidator, expected_contract=None,
                         mode="rehearsal", models=None, fatal=None, cohort_controls=False,
                         baseline_proof=None, qualification_binding=None, contract_check=None):
    """Physically prepare both source pools, then one common blind scout batch.

    This zero-API setup alone is not a cohort rehearsal or correctness result.
    Its sessions preserve real immutable Git sources and physical Docker receipts
    unless a caller explicitly supplies a unit-test validator.
    """
    from .continuation_controller import run_continuation_stage
    if tuple(generation_order) not in (("independent", "sequential"), ("sequential", "independent")):
        raise ValueError("Shared setup requires exactly both formation policies")
    seed = candidate_seed(project_id, repetition)
    if not isinstance(contract_sha256, str) or len(contract_sha256) != 64:
        raise ValueError("Shared setup requires a frozen contract digest")
    expected_contract = contract(image) if expected_contract is None else deepcopy(expected_contract)
    if digest(expected_contract) != contract_sha256 or expected_contract.get("image") != image:
        raise ValueError("Shared setup contract snapshot does not match its digest or runtime image")
    output = Path(output).resolve()
    if output.exists():
        raise ValueError("Use a fresh shared-setup directory")
    output.mkdir(parents=True)
    module = fixture(project_id)
    project = module.PROJECT
    if project["id"] != project_id:
        raise ValueError("Fixture identity differs from requested block")
    block_id = f"{project_id}-{repetition}"
    namespace = budget.namespace + "/" + block_id
    budget_before = ledger.budget()
    save(output / "contract.json", expected_contract)
    save(output / "fixture.json", project)
    save(output / "fixtures.json", {"projects": [fixture(pid).PROJECT for pid in PROJECT_MODULES]})
    for name, expected_sha in expected_contract["sources"].items():
        source = Path(__file__).parent / name
        if sha_file(source) != expected_sha:
            raise ValueError("Scientific source changed before shared setup")
        target = output / "source-snapshot" / "gossip_harness" / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
    for name, expected_sha in expected_contract.get("extra_sources", {}).items():
        source = Path(__file__).parent.parent / name
        if sha_file(source) != expected_sha:
            raise ValueError("Qualification source changed before shared setup")
        target = output / "source-snapshot" / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
    if sha_file(PLAN_PATH) != expected_contract["plan_sha256"]:
        raise ValueError("Study plan changed before shared setup")
    shutil.copyfile(PLAN_PATH, output / "study-plan.json")
    fatal = threading.Event() if fatal is None else fatal
    pools = {}
    approval = dict(files_sha256=digest(project["initial_files"]), source_valid=True,
        approval_id="qualified-baseline:" + digest(baseline_proof) if baseline_proof else
                    "trusted-baseline:" + digest(project["initial_files"]))
    for formation in generation_order:
        session_id = f"{block_id}-shared-{formation}"
        with OfflineSession(output / formation, project=project, module=module, stage=0,
                session_id=session_id, namespace=namespace, contract_sha256=contract_sha256,
                image=image, ledger=ledger, budget=budget, spans=spans, fatal=fatal,
                validator_factory=validator_factory, mode=mode, models=models, repetition=repetition,
                cohort_controls=cohort_controls, contract_check=contract_check) as session:
            result = run_continuation_stage(project, 0, formation + "-four", project["initial_files"], {},
                invoke=session.invoke, evaluate=session.evaluate, retain=session.retain,
                validate_probes=oracle_gate(module, project), emit=session.emit,
                after_initial=session.freeze_initial, candidate_seed=seed, limits=LIMITS,
                baseline_approval=approval,
                controller_mode="current", initial_only=True)
            pools[formation] = dict(session=session.finish(result),
                initial_pool=bound_file(session.root / "initial-pool.json"),
                pool_sha256=result["initial_pool_sha256"])
    # The scout prompt sees only the trusted application baseline and public
    # specification. The barrier contains hashes, never candidate source text.
    barrier = dict(protocol=SHARED_PROTOCOL, project_id=project_id, repetition=repetition,
                   candidate_pools=deepcopy(pools))
    with OfflineSession(output / "scouts", project=project, module=module, stage=0,
            session_id=f"{block_id}-shared-scouts", namespace=namespace,
            contract_sha256=contract_sha256, image=image, ledger=ledger, budget=budget,
            spans=spans, fatal=fatal, validator_factory=validator_factory, mode=mode, models=models,
            repetition=repetition, cohort_controls=cohort_controls, contract_check=contract_check) as session:
        session.freeze_initial(barrier)
        scouts, evidence = scout_probes(project, 0, project["initial_files"], session.invoke,
            session.root, spans, session.session_id, barrier)
        scout_binding = dict(session=session.finish(dict(scouts=scouts, evidence=evidence)),
            proposals=bound_file(session.root / "scouts.json"),
            barrier=bound_file(session.root / "initial-pool.json"))
    if any(unsettled(ledger.path).values()):
        raise RuntimeError("Shared setup must settle all work before import")
    for name, expected_sha in expected_contract["sources"].items():
        if sha_file(Path(__file__).parent / name) != expected_sha:
            raise ValueError("Scientific source changed while shared setup was running")
    for name, expected_sha in expected_contract.get("extra_sources", {}).items():
        if sha_file(Path(__file__).parent.parent / name) != expected_sha:
            raise ValueError("Qualification source changed while shared setup was running")
    if sha_file(PLAN_PATH) != expected_contract["plan_sha256"]:
        raise ValueError("Study plan changed while shared setup was running")
    clock = read(spans.path)
    save(output / "timings.json", {**clock, "spans": [row for row in clock["spans"]
        if row["status"] in {"finished", "failed"}
        and row.get("run_id", "").endswith(("-shared-independent", "-shared-sequential", "-shared-scouts"))],
        "snapshot_scope": "closed shared-setup session execution spans"})
    snapshot_accounting(ledger, output / "accounting-snapshot.sqlite")
    sessions = [read(value["session"]["path"]) for value in [*pools.values(), scout_binding]]
    manifest = dict(protocol=SHARED_PROTOCOL, mode="offline" if mode == "rehearsal" else mode,
        execution_mode=mode, cohort_controls=cohort_controls, baseline_qualification=deepcopy(baseline_proof),
        qualification=deepcopy(qualification_binding), project_id=project_id,
        repetition=repetition, block_id=block_id, stage_index=0, contract_sha256=contract_sha256,
        candidate_seed=seed, initial_files_sha256=digest(project["initial_files"]),
        generation_order=list(generation_order), pools=pools, scouts=scout_binding,
        frozen_utc=datetime.now(timezone.utc).isoformat(), frozen_monotonic_ns=time.monotonic_ns(),
        physical_provider_calls=sum(value["physical_provider_calls"] for value in sessions),
        usage_micro_usd=sum(value["usage_micro_usd"] for value in sessions),
        namespace=namespace, budget_before=budget_before, budget=ledger.budget(),
        incremental_micro_usd=ledger.budget()["spent_or_reserved"] - budget_before["spent_or_reserved"],
        validator_origin="physical_docker" if validator_factory is BlackboxValidator else "unit_test_control",
        contract=bound_file(output / "contract.json"), fixture=bound_file(output / "fixture.json"),
        fixtures=bound_file(output / "fixtures.json"),
        timings=bound_file(output / "timings.json"), accounting=bound_file(output / "accounting-snapshot.sqlite"),
        baseline_approval=approval,
        attribution={"current-independent": "independent", "improved-independent": "independent",
                     "improved-sequential": "sequential"},
        accounting_interpretation="Eight initial builders and two scouts executed once for the block; per-arm attribution is not physical replay.")
    save(output / "shared-setup.json", manifest)
    return manifest


def import_shared_initial(manifest_path, policy, *, project_id, repetition, initial_files,
                          contract_sha256, expected_manifest_sha256):
    """Read one exact source pool and common probes without provider dispatch.

    Independent audit remains required. These bindings reject accidental source
    or manifest replacement and prevent setup reuse across blocks or purposes.
    The controller independently rechecks eligibility and executes fresh arm
    receipts. No correctness receipt is reused by this reader.
    """
    if policy not in POLICIES:
        raise ValueError("Unknown continuation arm")
    path = Path(manifest_path).resolve()
    if sha_file(path) != expected_manifest_sha256:
        raise ValueError("Shared manifest changed after its caller froze the binding")
    manifest = read(path)
    if (manifest.get("protocol") != SHARED_PROTOCOL or type(manifest.get("stage_index")) is not int
            or manifest["stage_index"] != 0
            or type(manifest.get("repetition")) is not int or manifest["repetition"] != repetition
            or manifest.get("project_id") != project_id or manifest.get("contract_sha256") != contract_sha256
            or manifest.get("candidate_seed") != candidate_seed(project_id, repetition)
            or manifest.get("initial_files_sha256") != digest(initial_files)
            or set(manifest.get("pools", {})) != {"independent", "sequential"}):
        raise ValueError("Shared setup does not match this block, source or execution contract")
    formation = POLICIES[policy]["formation"]
    if manifest.get("attribution", {}).get(policy) != formation:
        raise ValueError("Shared setup changed arm attribution")

    def verified_path(binding):
        target = Path(binding["path"]).resolve()
        if not target.is_relative_to(path.parent) or sha_file(target) != binding["sha256"]:
            raise ValueError("Shared setup artifact binding changed")
        return target

    def checked(binding):
        return read(verified_path(binding))

    expected = checked(manifest["contract"])
    project = checked(manifest["fixture"])
    checked(manifest["fixtures"])
    if (digest(expected) != contract_sha256 or project.get("initial_files") != initial_files
            or ("fixtures" in expected and expected["fixtures"][project_id]["fixture_sha256"] != digest(project))):
        raise ValueError("Shared setup contract or fixture provenance changed")
    checked(manifest["timings"])
    verified_path(manifest["accounting"])
    verified_path(dict(path=str(path.parent / "study-plan.json"), sha256=expected["plan_sha256"]))
    for name, value in expected["sources"].items():
        verified_path(dict(path=str(path.parent / "source-snapshot" / "gossip_harness" / name), sha256=value))
    for name, value in expected.get("extra_sources", {}).items():
        verified_path(dict(path=str(path.parent / "source-snapshot" / name), sha256=value))

    def checked_session(binding):
        session_path = verified_path(binding)
        session = read(session_path)
        directory = session_path.parent
        if session.get("contract_sha256") != contract_sha256:
            raise ValueError("Shared session belongs to another contract")
        for entry in session["exports"]:
            exported = checked(entry)
            store_path = Path(exported["binding"]["store_path"]).resolve()
            if (not store_path.is_relative_to(path.parent)
                    or exported["binding"] != entry["binding"]
                    or digest(exported["files"]) != entry["files_sha256"]
                    or GitStore(store_path).read_files(exported["binding"]["tip_sha"]) != exported["files"]):
                raise ValueError("Shared retained Git/source export changed")
        for entry in session["validations"]:
            checked(entry)
        for call in session["invocations"]:
            request_path = (directory / "requests" / (call["call_id"] + ".request.json")).resolve()
            result_path = (directory / "requests" / (call["call_id"] + ".result.json")).resolve()
            if (not request_path.is_relative_to(directory) or not result_path.is_relative_to(directory)
                    or digest(read(request_path)) != call["request_sha256"] or read(result_path) != call):
                raise ValueError("Shared request or response provenance changed")
            response = checked(dict(path=call["journal_paths"]["result"], sha256=call["journal_result_sha256"]))
            journal_values = {}
            for kind in ("request", "settled"):
                journal_path = Path(call["journal_paths"][kind]).resolve()
                if not journal_path.is_relative_to(directory):
                    raise ValueError("Shared journal provenance escaped its session")
                journal_values[kind] = read(journal_path)
            identity = {key: response[key] for key in ("schema_version", "call_id", "reservation_id", "request_sha256")}
            if (identity["call_id"] != call["call_id"] or identity["reservation_id"] != call["reservation"]
                    or journal_values["request"] != {**identity, "request": read(request_path)}
                    or journal_values["settled"] != {**identity, "result_sha256": call["journal_result_sha256"],
                                                      "usage_units": call["usage_units"]}):
                raise ValueError("Shared journal request or settlement changed")
        record = session["accounting_record"]
        record_path = Path(record["store_path"]).resolve()
        if (not record_path.is_relative_to(path.parent) or GitStore(record_path).head() != record["accepted_head"]
                or GitStore(record_path).read_files() != {"record.json": json.dumps(record["record"], sort_keys=True)}):
            raise ValueError("Shared accounting Git record changed")
        return session

    # Check both formation sessions, even when this arm imports only one. This
    # preserves the common scout barrier instead of accepting a partial block.
    for item in manifest["pools"].values():
        checked_session(item["session"])
        frozen = checked(item["initial_pool"])
        if frozen["pool_sha256"] != item["pool_sha256"] or digest(frozen["pool"]) != item["pool_sha256"]:
            raise ValueError("Shared candidate pool changed")
    item = manifest["pools"][formation]
    frozen = checked(item["initial_pool"])
    scouts = checked(manifest["scouts"]["proposals"])
    checked_session(manifest["scouts"]["session"])
    barrier = checked(manifest["scouts"]["barrier"])
    if (barrier["pool"].get("candidate_pools") != manifest["pools"]
            or scouts.get("initial_pool_sha256") != digest(barrier["pool"])
            or scouts.get("source_sha256") != digest(initial_files)):
        raise ValueError("Common scouts are not bound to both frozen source pools")
    envelope = dict(pool=deepcopy(frozen["pool"]), pool_sha256=frozen["pool_sha256"],
        initial_files_sha256=digest(initial_files), candidate_seed=manifest["candidate_seed"],
        purpose="shared_initial_setup")
    attribution = dict(purpose="shared_initial_setup_import", manifest=bound_file(path), policy=policy,
        formation=formation, initial_pool=item["initial_pool"], scouts=manifest["scouts"]["proposals"],
        physical_provider_calls=0, charged_micro_usd=0,
        attributed_builder_opportunities=4, attributed_scout_opportunities=2,
        receipt_policy="Imported source provenance only; every arm executes fresh source-bound validation.")
    return dict(frozen_initial_pool=envelope, scouts=deepcopy(scouts["proposals"]), attribution=attribution)


def check_current_contract(expected):
    for name, value in expected["sources"].items():
        if sha_file(Path(__file__).parent / name) != value:
            raise ValueError("Scientific execution source changed after preregistration")
    for name, value in expected["extra_sources"].items():
        if sha_file(Path(__file__).parent.parent / name) != value:
            raise ValueError("Scientific support source changed after preregistration")
    if sha_file(PLAN_PATH) != expected["plan_sha256"]:
        raise ValueError("Scientific plan changed after preregistration")
    for pid, item in expected.get("fixtures", {}).items():
        if digest(fixture(pid).PROJECT) != item["fixture_sha256"]:
            raise ValueError("In-memory fixture differs from its frozen contract")


def snapshot_sources(output, expected):
    """Freeze code, evaluation fixtures and the exact plan before any requests."""
    output = Path(output)
    for name, value in expected["sources"].items():
        source = Path(__file__).parent / name
        if sha_file(source) != value:
            raise ValueError("Scientific source changed during snapshot")
        target = output / "source-snapshot" / "gossip_harness" / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
    for name, value in expected["extra_sources"].items():
        source = Path(__file__).parent.parent / name
        if sha_file(source) != value:
            raise ValueError("Scientific support source changed during snapshot")
        target = output / "source-snapshot" / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
    shutil.copyfile(PLAN_PATH, output / "study-plan.json")
    save(output / "fixtures.json", {"projects": [fixture(pid).PROJECT for pid in PROJECT_MODULES]})
    save(output / "contract.json", expected)
    check_current_contract(expected)


def shared_calls(manifest):
    groups = [*manifest["pools"].values(), manifest["scouts"]]
    return [call for group in groups for call in read(group["session"]["path"])["invocations"]]


def trajectory(config, expected, shared_binding, ledger, budget, models, spans, fatal,
               *, validator_factory=BlackboxValidator):
    """Execute both milestones from this arm's actual accepted lineage."""
    from .continuation_controller import run_continuation_stage
    root = Path(config["root"])
    module, project = fixture(config["project_id"]), fixture(config["project_id"]).PROJECT
    base = GitStore.create(root / "initial.git", project["initial_files"])
    policy = POLICIES[config["policy"]]
    shared_manifest = read(shared_binding["path"])
    shared = import_shared_initial(shared_binding["path"], config["policy"], project_id=project["id"],
        repetition=config["repetition"], initial_files=project["initial_files"], contract_sha256=digest(expected),
        expected_manifest_sha256=shared_binding["sha256"])
    state = dict(contract_sha=digest(expected), run_id=config["run_id"], project_id=project["id"],
        policy=config["policy"], repetition=config["repetition"], stages=[], stage_sessions=[], next_stage=0,
        files=deepcopy(project["initial_files"]), store_path=str(base.path), head=base.head(),
        shared_import=deepcopy(shared["attribution"]))
    save(root / "state.json", state)
    for stage in range(MILESTONES):
        check_current_contract(expected)
        initial = deepcopy(state["files"])
        stage_root = root / f"stage-{stage}"
        prior = deepcopy(state["stages"][-1]) if state["stages"] else {}
        approval = (deepcopy(shared_manifest["baseline_approval"]) if stage == 0 else
                    dict(files_sha256=digest(initial), source_valid=True, approval_id=base.head()))
        with spans.measure("stage", run_id=config["run_id"], stage_index=stage), ExecutionSession(
                stage_root, project=project, module=module, stage=stage, session_id=config["run_id"],
                namespace=config["namespace"], billing_suffix=f"stage-{stage}", contract_sha256=digest(expected),
                image=expected["image"], ledger=ledger, budget=budget, spans=spans, fatal=fatal,
                validator_factory=validator_factory, mode=config["mode"], models=models,
                initial_files=initial, base_store=base, session_kind="stage", repetition=config["repetition"],
                controller_mode=policy["controller"], cohort_controls=True,
                contract_check=lambda: check_current_contract(expected)) as session:
            scouts: dict[str, Any] = {}

            def after_initial(pool):
                receipt = session.freeze_initial(pool)
                if stage == 0:
                    original = read(shared_manifest["scouts"]["proposals"]["path"])
                    scouts.update(source="shared_initial_setup", shared_manifest=deepcopy(shared_binding),
                        proposals=deepcopy(shared["scouts"]), calls=0, attributed_calls=2,
                        rejected_batches=original["rejected_batches"])
                    return {**receipt, "scouts": deepcopy(shared["scouts"])}
                proposals, evidence = scout_probes(project, stage, initial, session.invoke,
                    stage_root, spans, config["run_id"], pool)
                scouts.update(evidence, source="arm_specific", attributed_calls=2)
                return {**receipt, "scouts": proposals}

            result = run_continuation_stage(project, stage, policy["formation"] + "-four", initial, prior,
                invoke=session.invoke, evaluate=session.evaluate, retain=session.retain,
                validate_probes=oracle_gate(module, project), emit=session.emit, after_initial=after_initial,
                candidate_seed=candidate_seed(project["id"], config["repetition"], stage), limits=expected["limits"],
                baseline_approval=approval, controller_mode=policy["controller"],
                frozen_initial_pool=shared["frozen_initial_pool"] if stage == 0 else None)
            scouts.update(admitted=result["metrics"]["admitted_scout_probes"],
                          rejected=result["metrics"]["rejected_scout_probes"])
            save(stage_root / "scouts.json", scouts)
            result.update(stage_index=stage, policy=config["policy"],
                initial_files_sha256=digest(initial), invocations=[session.calls[key] for key in sorted(session.calls)],
                scouts=scouts, shared_import=deepcopy(shared["attribution"]) if stage == 0 else None)
            save(stage_root / "result.json", result)
            session_binding = session.finish(result)
        state["stages"].append(result)
        state["stage_sessions"].append(session_binding)
        state["files"] = result["files"]
        if not result["completed"]:
            chosen = result["selected_binding"]
            state.update(terminal="bounded_incomplete", final_binding={**chosen,
                         "accepted_head": GitStore(chosen["store_path"]).head()})
            break
        base = local_promote(base, result["selected_binding"], stage_root / "checkpoint.git",
                             result["files"], project["allowed_paths"])
        state.update(store_path=str(base.path), head=base.head(), next_stage=stage + 1,
            final_binding=dict(store_path=str(base.path), tip_sha=base.head(), accepted_head=base.head(),
                               files_sha256=digest(state["files"])))
        save(root / "state.json", state)
    else:
        state["terminal"] = "visible_complete"
    save(root / "state.json", state)
    save(root / "trajectory.json", state)
    return state


def _artifact_files(root):
    """All non-Git JSON evidence, including source/probe exports and traces."""
    root = Path(root)
    return [bound_file(path) for path in sorted(root.rglob("*")) if path.is_file()
            and path.suffix in {".json", ".jsonl", ".py"}
            and not any(part.endswith(".git") for part in path.relative_to(root).parts)]


def _session_git_bindings(session, root, *, verify_sources):
    rows = []
    for exported in session["exports"]:
        binding = deepcopy(exported["binding"])
        store = GitStore(binding["store_path"])
        if verify_sources and digest(store.read_files(binding["tip_sha"])) != binding["files_sha256"]:
            raise ValueError("Candidate source changed before cohort freeze")
        binding["accepted_head"] = store.head()
        rows.append(binding)
    for name in ("baseline.git", "record.git"):
        store = GitStore(Path(root) / name)
        rows.append(dict(store_path=str(store.path), tip_sha=store.head(), accepted_head=store.head(),
                         files_sha256=digest(store.read_files())))
    return rows


def collect_stage_artifacts(root, state, *, verify_sources=True):
    root = Path(root)
    rows = []
    for index, stage in enumerate(state["stages"]):
        directory = root / f"stage-{index}"
        binding = state["stage_sessions"][index]
        if binding != bound_file(directory / "session.json"):
            raise ValueError("Stage session changed")
        session = read(directory / "session.json")
        if session["result"] != stage or read(directory / "result.json") != stage:
            raise ValueError("Stage result differs from its durable session")
        rows.append(dict(stage_index=index, session=binding, result=bound_file(directory / "result.json"),
            initial_pool=bound_file(directory / "initial-pool.json"), scouts=bound_file(directory / "scouts.json"),
            artifact_files=_artifact_files(directory),
            git_bindings=_session_git_bindings(session, directory, verify_sources=verify_sources)))
    return rows


def validate_trajectory(state, expected, identity, *, verify_sources=True):
    if (state.get("contract_sha") != digest(expected)
            or [state.get("project_id"), state.get("policy"), state.get("repetition")] != identity
            or type(state.get("repetition")) is not int):
        raise ValueError("Trajectory identity changed")
    stages = state.get("stages", [])
    if (not 1 <= len(stages) <= MILESTONES or len(state.get("stage_sessions", [])) != len(stages)
            or any(type(row.get("stage_index")) is not int or row["stage_index"] != index
                   or type(row.get("completed")) is not bool
                   or (index < len(stages) - 1 and not row["completed"]) for index, row in enumerate(stages))):
        raise ValueError("Milestone lineage skipped an incomplete stage")
    full = len(stages) == MILESTONES and all(row["completed"] for row in stages)
    if (state.get("terminal") != ("visible_complete" if full else "bounded_incomplete")
            or state.get("files") != stages[-1]["files"]):
        raise ValueError("Terminal source or completion classification changed")
    binding = state["final_binding"]
    store = GitStore(binding["store_path"])
    if (binding["files_sha256"] != digest(state["files"]) or store.head() != binding["accepted_head"]
            or (verify_sources and store.read_files(binding["tip_sha"]) != state["files"])):
        raise ValueError("Final Git source or accepted head changed")
    return state


def freeze_trajectories(output, expected, shared_setups):
    output = Path(output)
    rows, shared_rows = [], []
    for entry in shared_setups:
        binding = entry["manifest"]
        if binding != bound_file(binding["path"]):
            raise ValueError("Shared setup changed before cohort freeze")
        manifest = read(binding["path"])
        directory = Path(binding["path"]).parent
        import_shared_initial(binding["path"], "current-independent", project_id=manifest["project_id"],
            repetition=manifest["repetition"], initial_files=fixture(manifest["project_id"]).PROJECT["initial_files"],
            contract_sha256=digest(expected), expected_manifest_sha256=binding["sha256"])
        git_bindings = []
        for group in [*manifest["pools"].values(), manifest["scouts"]]:
            session = read(group["session"]["path"])
            git_bindings.extend(_session_git_bindings(session, Path(group["session"]["path"]).parent,
                                                      verify_sources=True))
        shared_rows.append(dict(project_id=manifest["project_id"], repetition=manifest["repetition"],
            manifest=binding, artifact_files=_artifact_files(directory), git_bindings=git_bindings))
    if [[row["project_id"], row["repetition"]] for row in shared_rows] != expected["block_order"]:
        raise ValueError("Every registered shared block must finish before cohort freeze")
    for identity in expected["roster"]:
        run_id = f"{identity[0]}-{identity[1]}-{identity[2]}"
        root = output / run_id
        state = validate_trajectory(read(root / "trajectory.json"), expected, identity)
        rows.append(dict(run_id=run_id, project_id=identity[0], policy=identity[1], repetition=identity[2],
            trajectory=bound_file(root / "trajectory.json"), config=bound_file(root / "config.json"),
            files_sha256=digest(state["files"]),
            final_binding=state["final_binding"], terminal=state["terminal"],
            evidence_sha256=digest(state["stages"][-1]["evidence_cases"]),
            milestones_completed=sum(stage["completed"] for stage in state["stages"]),
            stage_artifacts=collect_stage_artifacts(root, state)))
    sources = []
    for relative, value in [("gossip_harness/" + name, value) for name, value in expected["sources"].items()] + list(expected["extra_sources"].items()):
        binding = bound_file(output / "source-snapshot" / relative)
        if binding["sha256"] != value:
            raise ValueError("Scientific snapshot changed before cohort freeze")
        sources.append(binding)
    if (sha_file(output / "study-plan.json") != expected["plan_sha256"]
            or digest(read(output / "contract.json")) != digest(expected)
            or digest(read(output / "fixtures.json")) != digest({"projects": [fixture(pid).PROJECT for pid in expected["project_ids"]]})):
        raise ValueError("Scientific contract snapshot changed before cohort freeze")
    manifest = dict(protocol=PROTOCOL, contract_sha=digest(expected), trajectories=rows,
        shared_setups=shared_rows, fixtures=bound_file(output / "fixtures.json"), sources=sources,
        plan=bound_file(output / "study-plan.json"), contract=bound_file(output / "contract.json"),
        frozen_utc=datetime.now(timezone.utc).isoformat(), frozen_monotonic_ns=time.monotonic_ns(),
        private_evaluation_started=False)
    save(output / "frozen-trajectories.json", manifest)
    return manifest


def validate_freeze(output, expected, freeze_sha, *, verify_sources=False):
    output = Path(output)
    path = output / "frozen-trajectories.json"
    if sha_file(path) != freeze_sha:
        raise ValueError("Cohort freeze manifest changed")
    frozen = read(path)
    if (frozen.get("protocol") != PROTOCOL or frozen.get("contract_sha") != digest(expected)
            or frozen.get("private_evaluation_started") is not False
            or [[row["project_id"], row["policy"], row["repetition"]] for row in frozen["trajectories"]] != expected["roster"]
            or [[row["project_id"], row["repetition"]] for row in frozen["shared_setups"]] != expected["block_order"]):
        raise ValueError("Exact complete shared-setup and terminal cohort barrier required")
    bindings = [frozen[key] for key in ("fixtures", "plan", "contract")] + frozen["sources"]
    git_bindings = []
    for row in frozen["shared_setups"]:
        bindings.extend([row["manifest"], *row["artifact_files"]])
        git_bindings.extend(row["git_bindings"])
    states = {}
    for identity, row in zip(expected["roster"], frozen["trajectories"]):
        bindings.extend([row["trajectory"], row["config"]])
        for stage in row["stage_artifacts"]:
            bindings.extend(stage["artifact_files"])
            git_bindings.extend(stage["git_bindings"])
        state = validate_trajectory(read(row["trajectory"]["path"]), expected, identity, verify_sources=verify_sources)
        if (state["final_binding"] != row["final_binding"] or digest(state["files"]) != row["files_sha256"]
                or digest(state["stages"][-1]["evidence_cases"]) != row["evidence_sha256"]):
            raise ValueError("Final source or evidence differs from cohort freeze")
        states[row["run_id"]] = state
    for binding in bindings:
        path = Path(binding["path"]).resolve()
        if not path.is_relative_to(output.resolve()) or bound_file(path) != binding:
            raise ValueError("Frozen evidence artifact changed")
    for binding in git_bindings:
        store = GitStore(binding["store_path"])
        if (store.head() != binding["accepted_head"]
                or (verify_sources and digest(store.read_files(binding["tip_sha"])) != binding["files_sha256"])):
            raise ValueError("Frozen candidate or accounting Git binding changed")
    return states


def finalize(output, config, expected, freeze_sha, spans, *, validator_factory=BlackboxValidator):
    state = validate_freeze(output, expected, freeze_sha)[config["run_id"]]
    root, project = Path(config["root"]), fixture(config["project_id"]).PROJECT
    full = len(state["stages"]) == MILESTONES and all(stage["completed"] for stage in state["stages"])
    active = state["stages"][-1]["probe_pool"] if len(state["stages"]) == MILESTONES else []
    suites = dict(visible=cases_for(project, MILESTONES - 1, "visible") + active,
                  private=cases_for(project, MILESTONES - 1, "hidden"))
    receipts = {}
    for kind, cases in suites.items():
        validate_freeze(output, expected, freeze_sha)
        check_current_contract(expected)
        validator = validator_factory(expected["image"], timeout_seconds=SUITE_TIMEOUT, case_timeout_seconds=CASE_TIMEOUT)
        with spans.measure("docker_validation", run_id=config["run_id"], purpose="final_" + kind,
                case_count=len(cases), freeze_manifest_sha256=freeze_sha,
                validator_origin="physical_docker" if validator_factory is BlackboxValidator else "unit_test_control") as span:
            receipt = validator.evaluate(state["files"], cases)
            path = root / f"final-{kind}-receipt.json"
            save(path, receipt)
            spans.annotate(span, receipt_path=str(path), receipt_sha256=sha_file(path))
            receipts[kind] = verify_receipt(receipt, state["files"], cases, expected["image"])
    accepted = bool(full and receipts["visible"]["passed"] and receipts["private"]["passed"])
    release = (local_promote(GitStore(root / "initial.git"), state["final_binding"], root / "release.git",
                            state["files"], project["allowed_paths"]) if accepted else None)
    pairs = list(zip(suites["private"], receipts["private"]["outcomes"]))
    coverage = {key: all(outcome["passed"] for case, outcome in pairs if case["requirement"] == key)
                for key in sorted({case["requirement"] for case in suites["private"]})}
    calls = [call for stage in state["stages"] for call in stage["invocations"]]
    metrics = {key for stage in state["stages"] for key in stage["metrics"]}
    result = dict(run_id=config["run_id"], root=str(root), project_id=project["id"], policy=config["policy"],
        repetition=config["repetition"], accepted=accepted,
        status="accepted" if accepted else "final_quality_failed" if full else "bounded_incomplete",
        milestones_completed=sum(stage["completed"] for stage in state["stages"]),
        files_sha256=digest(state["files"]), trajectory_sha256=sha_file(root / "trajectory.json"),
        freeze_manifest_sha256=freeze_sha, requirement_coverage=coverage,
        final_visible=receipts["visible"], final_hidden=receipts["private"], release_head=release.head() if release else None,
        invocations=calls, usage_micro_usd=sum(call["usage_units"] for call in calls),
        physical_provider_calls=sum(call["physical_dispatch"] for call in calls),
        shared_import=deepcopy(state["shared_import"]),
        metrics={key: sum(stage["metrics"].get(key, 0) for stage in state["stages"]) for key in sorted(metrics)},
        scouts={key: sum(stage["scouts"][key] for stage in state["stages"])
                for key in ("calls", "attributed_calls", "admitted", "rejected", "rejected_batches")})
    save(root / "result.json", result)
    return result


def validate_rehearsal(proof, expected, proof_root):
    """Require a complete matching zero-API cohort with exercised controls."""
    root = Path(proof_root).resolve()
    rows = proof.get("cases", [])
    if (proof.get("experiment") != PROTOCOL or proof.get("mode") != "rehearsal"
            or proof.get("status") != "finished" or proof.get("phase") != "finished"
            or proof.get("contract") != expected or proof.get("contract_sha") != digest(expected)
            or proof.get("unexecuted") or proof.get("censored") or proof.get("active_case") is not None
            or proof.get("incremental_micro_usd") != 0 or proof.get("unsettled") != dict(reservations=0, promotions=0)
            or [[row["project_id"], row["policy"], row["repetition"]] for row in rows] != expected["roster"]
            or len(rows) != 12 or Path(proof.get("root", "")).resolve() != root):
        raise ValueError("Exact complete twelve-trajectory rehearsal required")
    states = validate_freeze(root, expected, proof["freeze_manifest_sha256"], verify_sources=True)
    for row in rows:
        if (row.get("accepted") is not True or row.get("milestones_completed") != 2
                or row.get("physical_provider_calls") != 0 or row.get("usage_micro_usd") != 0
                or read(root / row["run_id"] / "result.json") != row):
            raise ValueError("Every rehearsal trajectory must independently accept both milestones at zero API usage")
        stages = states[row["run_id"]]["stages"]
        for stage, result in enumerate(stages):
            metrics = result["metrics"]
            focused = row["policy"] != "current-independent" and row["repetition"] == 1 and stage == 1
            if (metrics["initial_builder_calls"] != (0 if stage == 0 else 4)
                    or metrics["shared_initial_builder_opportunities"] != (4 if stage == 0 else 0)
                    or metrics["repairs"] != (0 if focused else 2)
                    or metrics["reviewer_calls"] != (4 if row["policy"] == "current-independent" else 2 if focused else 3)):
                raise ValueError("Rehearsal did not exercise its declared formation and continuation controls")
            if row["policy"] != "current-independent":
                if focused:
                    if metrics["focused_review_requests"] != 1:
                        raise ValueError("Focused uncertainty retry was not exercised")
                elif (metrics["retained_repair_checkpoints"] != 2 or metrics["blocked_acceptances"] != 2
                      or metrics["escalations"] != 1
                      or (row["repetition"] == 0 and metrics["repair_regressions"] < 1)):
                    raise ValueError("Repair retention, blocked acceptance or regression control was not exercised")
            if result["scouts"]["attributed_calls"] != 2 or result["scouts"]["rejected"] < 4:
                raise ValueError("Wrong and duplicate scout controls were not retained")
    # The independent audit checks every decision, execution and actual timing;
    # this runner-side gate additionally refuses missing final receipts.
    for row in rows:
        state = states[row["run_id"]]
        project = fixture(row["project_id"]).PROJECT
        for kind, cases in (("visible", cases_for(project, 1, "visible") + state["stages"][-1]["probe_pool"]),
                            ("private", cases_for(project, 1, "hidden"))):
            receipt = read(root / row["run_id"] / f"final-{kind}-receipt.json")
            if not verify_receipt(receipt, state["files"], cases, expected["image"])["passed"]:
                raise ValueError("Rehearsal final execution failed")


def validate_rehearsal_certificate(saved, current, *, results_sha256, contract_sha256):
    """Require the same certificate bindings as independent final live audit."""
    keys = ("protocol", "results_sha256", "timings_sha256", "freeze_manifest_sha256",
            "contract_sha256", "auditor_sha256")
    if (any(value.get("passed") is not True or value.get("mode") != "rehearsal"
            or value.get("scope", {}).get("primary_cohort") is not True for value in (saved, current))
            or current.get("results_sha256") != results_sha256
            or current.get("contract_sha256") != contract_sha256
            or any(type(current.get(key)) is not str or not current[key]
                   or saved.get(key) != current[key] for key in keys)):
        raise ValueError("Retained rehearsal certificate does not bind independently reaudited evidence")


def _run_owned(output, *, mode="rehearsal", qualification=None, rehearsal=None, rehearsal_audit=None,
               budget_ledger=Path("runs/first-live-budget.sqlite"), env_file=Path(".env.local"),
               image=DEFAULT_IMAGE, validator_factory=BlackboxValidator):
    from analysis.qualify_continuation_followup import baseline_proof, runtime_identity, validate_qualification
    if mode not in {"rehearsal", "live"}:
        raise ValueError("Explicit rehearsal or live mode required")
    expected = contract(image)
    if mode == "live" and not live_readiness(study_plan())["plan_ready"]:
        raise ValueError("Live dispatch is closed pending the concrete funded preregistration")
    output = Path(output).resolve()
    if output.exists():
        raise ValueError("Use a fresh study directory; existing evidence is never overwritten")
    if qualification is None:
        raise ValueError("Complete exact fixture qualification is required before any setup")
    qualification = Path(qualification).resolve()
    qualification_binding = bound_file(qualification)
    qualified = validate_qualification(qualification, digest(expected))
    # The qualifier returns the validated report; preserve an explicit check if
    # an older API accidentally returns a boolean or no evidence.
    if not isinstance(qualified, dict):
        raise ValueError("Qualifier did not return its validated receipt")
    if bound_file(qualification) != qualification_binding:
        raise ValueError("Fixture qualification changed during checking")
    runtime = runtime_identity(image)
    if qualified["runtime_identity"] != runtime:
        raise ValueError("Qualified Docker runtime differs from selected execution runtime")
    baselines = {pid: baseline_proof(qualified, pid, fixture(pid).PROJECT["initial_files"]) for pid in PROJECT_MODULES}
    rehearsal_binding = None
    if mode == "live":
        if not live_readiness(study_plan())["plan_ready"]:
            raise ValueError("Live dispatch is closed pending the concrete funded preregistration")
        if rehearsal is None or rehearsal_audit is None:
            raise ValueError("Live execution requires complete matching rehearsal and independent audit")
        proof_path, audit_path = Path(rehearsal).resolve(), Path(rehearsal_audit).resolve()
        if proof_path.name != "results.json" or proof_path.parent == output:
            raise ValueError("Live execution requires another cohort's results.json proof")
        proof_binding, audit_binding = bound_file(proof_path), bound_file(audit_path)
        proof, audit = read(proof_path), read(audit_path)
        validate_rehearsal(proof, expected, proof_path.parent)
        if (audit.get("passed") is not True or audit.get("scope", {}).get("primary_cohort") is not True
                or audit.get("results_sha256") != sha_file(proof_path)
                or audit.get("contract_sha256") != digest(expected)):
            raise ValueError("Independent rehearsal audit does not bind the exact complete proof")
        # A retained success flag alone is insufficient; rerun the read-only
        # independent audit against the same frozen evidence before dispatch.
        from analysis.audit_continuation_followup import audit_run
        current_audit = audit_run(proof_path.parent)
        validate_rehearsal_certificate(audit, current_audit,
            results_sha256=proof_binding["sha256"], contract_sha256=digest(expected))
        if bound_file(proof_path) != proof_binding or bound_file(audit_path) != audit_binding:
            raise ValueError("Rehearsal proof or certificate changed during independent re-audit")
        rehearsal_binding = dict(results=proof_binding, audit=audit_binding)
        ledger_path = Path(budget_ledger).resolve()
        if not ledger_path.is_file():
            raise ValueError("Live execution requires the existing shared ledger")
        ledger = Ledger(ledger_path)
        before = ledger.budget()
        if (before["limit"] != expected["budget"]["shared_cumulative_cap_micro_usd"]
                or before["spent_or_reserved"] != expected["budget"]["expected_starting_usage_micro_usd"]
                or any(unsettled(ledger_path).values())):
            raise ValueError("Live ledger differs from the quiescent registered starting budget")
        key = credential(env_file)
        models = {role: OpenAIWorker(key, model=model, max_output_tokens=OUTPUT_TOKENS, timeout=180)
                  for role, model in (("cheap", MODEL), ("strong", STRONG_MODEL))}
    else:
        models, ledger_path = {}, output / "budget.sqlite"
    validator = validator_factory(image, timeout_seconds=SUITE_TIMEOUT, case_timeout_seconds=CASE_TIMEOUT)
    okay, detail = validator.preflight()
    if not okay:
        raise RuntimeError("Docker preflight failed: " + detail)
    output.mkdir(parents=True)
    if mode == "rehearsal":
        ledger = Ledger(ledger_path, budget_units=0)
        before = ledger.budget()
    namespace = PROTOCOL + "/" + digest([str(output), digest(expected)])[:24]
    cap = expected["budget"]["incremental_cap_micro_usd"] if mode == "live" else 0
    budget = StudyBudget(ledger, namespace, cap, output / "budget.lock")
    snapshot_sources(output, expected)
    save(output / "preregistered.json", dict(contract=expected, contract_sha=digest(expected),
        qualification=qualification_binding, baseline_qualifications=baselines, runtime_identity=runtime,
        runtime_identity_sha256=digest(runtime), budget_before=before, namespace=namespace,
        rehearsal=rehearsal_binding, mode=mode))
    report = dict(experiment=PROTOCOL, root=str(output), mode=mode, contract=expected, contract_sha=digest(expected),
        namespace=namespace, status="running", phase="shared_setup", shared_setups=[], trajectories=[], cases=[],
        unexecuted=deepcopy(expected["roster"]), censored=[], active_case=None, budget_before=before, budget=before,
        qualification=qualification_binding, baseline_qualifications=baselines, rehearsal=rehearsal_binding,
        runtime_identity=runtime, runtime_identity_sha256=digest(runtime), started_at=time.time())
    save(output / "results.json", report)
    spans, fatal, configs = Spans(output / "timings.json"), threading.Event(), []
    try:
        with spans.measure("study", mode=mode):
            for project_id, repetition in expected["block_order"]:
                block_id = f"{project_id}-{repetition}"
                report["active_case"] = dict(phase="shared_setup", project_id=project_id, repetition=repetition)
                save(output / "results.json", report)
                with spans.measure("shared_setup_block", run_id=block_id + "-shared") as span:
                    manifest = prepare_shared_block(output / "shared" / block_id, project_id, repetition,
                        contract_sha256=digest(expected), image=image, ledger=ledger, budget=budget, spans=spans,
                        generation_order=study_plan()["generation_order"], expected_contract=expected,
                        validator_factory=validator_factory, mode=mode, models=models, fatal=fatal,
                        cohort_controls=True, baseline_proof=baselines[project_id],
                        qualification_binding=qualification_binding, contract_check=lambda: check_current_contract(expected))
                report["shared_setups"].append(dict(project_id=project_id, repetition=repetition,
                    manifest=bound_file(output / "shared" / block_id / "shared-setup.json"),
                    invocations=shared_calls(manifest), usage_micro_usd=manifest["usage_micro_usd"],
                    physical_provider_calls=manifest["physical_provider_calls"], span_id=span["span_id"],
                    elapsed_seconds=span["elapsed_seconds"]))
                report["active_case"], report["budget"] = None, ledger.budget()
                save(output / "results.json", report)
            report["phase"] = "model_trajectories"
            for project_id, policy, repetition in expected["roster"]:
                run_id = f"{project_id}-{policy}-{repetition}"
                root = output / run_id
                root.mkdir()
                config = dict(root=str(root), run_id=run_id, project_id=project_id, policy=policy,
                    repetition=repetition, mode=mode, image=image, namespace=namespace + "/" + run_id,
                    contract_sha=digest(expected))
                configs.append(config)
                save(root / "config.json", config)
                shared = next(row["manifest"] for row in report["shared_setups"]
                              if row["project_id"] == project_id and row["repetition"] == repetition)
                report["active_case"] = {**config, "phase": "model_trajectory"}
                report["unexecuted"].remove([project_id, policy, repetition])
                save(output / "results.json", report)
                with spans.measure("model_trajectory", run_id=run_id) as span:
                    state = trajectory(config, expected, shared, ledger, budget, models, spans, fatal,
                                       validator_factory=validator_factory)
                report["trajectories"].append(dict(run_id=run_id, terminal=state["terminal"],
                    trajectory_sha256=sha_file(root / "trajectory.json"), span_id=span["span_id"],
                    elapsed_seconds=span["elapsed_seconds"]))
                report["active_case"], report["budget"] = None, ledger.budget()
                save(output / "results.json", report)
            if any(unsettled(ledger_path).values()):
                raise RuntimeError("All work must settle before the cohort private-evaluation barrier")
            freeze_trajectories(output, expected, report["shared_setups"])
            freeze_sha = sha_file(output / "frozen-trajectories.json")
            report.update(phase="final_evaluation", freeze_manifest_sha256=freeze_sha)
            save(output / "results.json", report)
            for config in configs:
                report["active_case"] = {**config, "phase": "final_evaluation"}
                save(output / "results.json", report)
                with spans.measure("final_evaluation", run_id=config["run_id"]):
                    result = finalize(output, config, expected, freeze_sha, spans,
                                      validator_factory=validator_factory)
                report["cases"].append(result)
                report["active_case"] = None
                save(output / "results.json", report)
            runtime_after = runtime_identity(image)
            if runtime_after != runtime:
                raise ValueError("Docker runtime identity changed across the complete cohort")
            report.update(status="finished", phase="finished", runtime_after=runtime_after,
                          runtime_after_sha256=digest(runtime_after))
    except BaseException as error:
        fatal.set()
        report.update(status="interrupted", failure_type=type(error).__name__)
        if report["active_case"] is not None:
            report["censored"].append({**report["active_case"], "reason": type(error).__name__})
            report["active_case"] = None
        raise
    finally:
        report.update(budget=ledger.budget(), finished_at=time.time(), unsettled=unsettled(ledger_path))
        report["incremental_micro_usd"] = report["budget"]["spent_or_reserved"] - before["spent_or_reserved"]
        save(output / "results.json", report)
        snapshot_accounting(ledger, output / "accounting-snapshot.sqlite")
    if mode == "rehearsal":
        try:
            validate_rehearsal(report, expected, output)
        except Exception:
            report.update(status="qualification_failed", phase="qualification_failed")
            save(output / "results.json", report)
            raise
    return report


def run(output, *, mode="rehearsal", **options):
    if mode == "live":
        with live_ownership(options.get("budget_ledger", Path("runs/first-live-budget.sqlite"))):
            return _run_owned(output, mode=mode, **options)
    return _run_owned(output, mode=mode, **options)


def prepare_offline_setup(output, project_id, repetition, *, qualification, image=DEFAULT_IMAGE):
    """Run one qualified zero-API setup block, never the full follow-up cohort."""
    output = Path(output).resolve()
    if output.exists():
        raise ValueError("Use a fresh output directory")
    if qualification is None:
        raise ValueError("Physical shared setup requires exact fixture qualification")
    from analysis.qualify_continuation_followup import baseline_proof, validate_qualification
    expected = contract(image)
    qualification = Path(qualification).resolve()
    qualification_binding = bound_file(qualification)
    qualified = validate_qualification(qualification, digest(expected))
    if bound_file(qualification) != qualification_binding:
        raise ValueError("Fixture qualification changed during eligibility checking")
    candidate_seed(project_id, repetition)
    validator = BlackboxValidator(image, timeout_seconds=SUITE_TIMEOUT, case_timeout_seconds=CASE_TIMEOUT)
    okay, detail = validator.preflight()
    if not okay:
        raise RuntimeError("Docker preflight failed: " + detail)
    output.mkdir(parents=True)
    ledger = Ledger(output / "budget.sqlite", budget_units=0)
    namespace = PROTOCOL + "/offline-setup-" + digest([str(output), digest(expected)])[:24]
    budget = StudyBudget(ledger, namespace, 0, output / "budget.lock")
    spans = Spans(output / "timings.json")
    manifest = prepare_shared_block(output / "block", project_id, repetition,
        contract_sha256=digest(expected), image=image, ledger=ledger, budget=budget, spans=spans,
        generation_order=study_plan()["generation_order"], expected_contract=expected,
        baseline_proof=baseline_proof(qualified, project_id, fixture(project_id).PROJECT["initial_files"]),
        qualification_binding=qualification_binding)
    save(output / "setup-result.json", dict(protocol=PROTOCOL, status="shared_setup_prepared",
        primary_cohort=False, api_calls=0, qualification=qualification_binding,
        shared_setup=bound_file(output / "block" / "shared-setup.json")))
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["describe", "prepare-shared-setup", "run"])
    parser.add_argument("--output", type=Path)
    parser.add_argument("--project", choices=list(PROJECT_MODULES))
    parser.add_argument("--repetition", type=int, choices=list(range(REPETITIONS)))
    parser.add_argument("--qualification", type=Path)
    parser.add_argument("--image", default=DEFAULT_IMAGE)
    parser.add_argument("--mode", choices=["rehearsal", "live"], default="rehearsal")
    parser.add_argument("--rehearsal", type=Path)
    parser.add_argument("--rehearsal-audit", type=Path)
    parser.add_argument("--budget-ledger", type=Path, default=Path("runs/first-live-budget.sqlite"))
    parser.add_argument("--env-file", type=Path, default=Path(".env.local"))
    args = parser.parse_args()
    if args.command == "describe":
        print(json.dumps(dict(readiness=live_readiness(study_plan()), call_envelope=call_envelope()), indent=2))
        return 0
    if args.command == "run":
        if args.output is None or args.qualification is None:
            parser.error("run requires --output and --qualification")
        result = run(args.output, mode=args.mode, qualification=args.qualification, rehearsal=args.rehearsal,
            rehearsal_audit=args.rehearsal_audit, budget_ledger=args.budget_ledger, env_file=args.env_file, image=args.image)
        print(json.dumps(dict(status=result["status"], mode=result["mode"],
            accepted=sum(row["accepted"] for row in result["cases"]), budget=result["budget"])))
        return 0
    if args.output is None or args.project is None or args.repetition is None or args.qualification is None:
        parser.error("prepare-shared-setup requires --output, --project, --repetition and --qualification")
    result = prepare_offline_setup(args.output, args.project, args.repetition,
                                  qualification=args.qualification, image=args.image)
    print(json.dumps(dict(status="shared_setup_prepared", api_calls=0, primary_cohort=False,
                          block_id=result["block_id"], output=str(args.output.resolve()))))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
