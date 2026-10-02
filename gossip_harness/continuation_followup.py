"""Offline foundations for the preregistered continuation-controller pilot.

This module owns shared first-milestone setup, physical accounting and immutable
import bindings. It deliberately does not expose a live-study command until the
whole-cohort runner, independent audit and exact rehearsal exist. Shared source
imports are never represented as provider replay or independent replications.
Historical benchmark modules are reused as immutable infrastructure only.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timezone
import importlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import threading
import time
from typing import Any

from .benchmark_experiment import CORE as PREVIOUS_CORE, known_result, scout_probes
from .blackbox_validator import BlackboxValidator
from .continuation_experiment import Spans, StudyBudget, read, save, sha_file, snapshot_accounting, unsettled
from .gitstore import GitStore
from .ledger import Ledger
from .pilot import DEFAULT_IMAGE, LeaseKeeper, Trace
from .sustained_experiment import digest, record_stage
from .verification_experiment import CASE_TIMEOUT, SUITE_TIMEOUT, oracle_gate, verify_receipt
from .verification_journal import RequestJournal
from .worker import MODEL, STRONG_MODEL, OpenAIWorker, WorkerFailure, WorkerRequest


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
EXTRA_SOURCES = ("analysis/qualify_continuation_followup.py",)


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
    plan = study_plan()
    modules = {project: fixture(project) for project in PROJECT_MODULES}
    return dict(protocol=PROTOCOL, status="offline_foundations_only",
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
        live_enabled=False, full_cohort_qualification=False)


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
    """A foundation artifact is never enough to authorize provider dispatch."""
    validate_plan(plan)
    envelope = call_envelope()
    return dict(ready=False, protocol=PROTOCOL,
        blockers=["whole-cohort execution and independent decision audit are not yet implemented",
                  "exact complete fixture qualification and twelve-trajectory rehearsal required",
                  "prospective funding decision must cover complete-cohort admission"],
        authorized_headroom_micro_usd=plan["budget"]["incremental_cap_micro_usd"],
        conservative_full_cohort_micro_usd=envelope["conservative_full_cohort_micro_usd"],
        paid_calls_enabled=False)


def bound_file(path):
    path = Path(path).resolve()
    return dict(path=str(path), sha256=sha_file(path))


def candidate_seed(project_id, repetition, stage=0):
    if project_id not in PROJECT_MODULES or type(repetition) is not int or repetition not in range(REPETITIONS):
        raise ValueError("Unknown paired block")
    if type(stage) is not int or stage not in range(MILESTONES):
        raise ValueError("Unknown milestone")
    return int(digest([PROTOCOL, project_id, repetition, stage])[:16], 16)


class OfflineSession:
    """Real Git/receipt boundary and zero-API worker journal for qualification.

    No injectable provider function or credential path exists here. A future
    live session must be introduced under a new source-bound contract after the
    cohort runner and audit are complete. The evaluator remains injectable only
    for explicit unit-test controls; production setup uses BlackboxValidator.
    """

    def __init__(self, root, *, project, module, stage, session_id, namespace,
                 contract_sha256, image, ledger, budget, spans, fatal,
                 validator_factory=BlackboxValidator):
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
        self.validator_factory = validator_factory
        self.validator_origin = "physical_docker" if validator_factory is BlackboxValidator else "unit_test_control"
        self.base = GitStore.create(self.root / "baseline.git", project["initial_files"])
        self.journal = RequestJournal(self.root / "journal")
        self.trace = Trace(self.root / "trace.jsonl")
        self.calls, self.exports, self.validations = {}, [], []
        self.started_call_ids: set[str] = set()
        self.lock = threading.RLock()
        self.keeper = LeaseKeeper(ledger)
        self.billing = namespace + "/" + session_id

    def __enter__(self):
        self.ledger.add_task(self.billing)
        self.keeper.__enter__()
        self.keeper.claim(self.billing, "followup-offline-controller")
        return self

    def __exit__(self, *exc):
        return self.keeper.__exit__(*exc)

    def emit(self, kind, /, **data):
        data.pop("kind", None)
        data.pop("stage_index", None)
        self.trace.emit(kind, stage_index=self.stage, session_id=self.session_id,
                        monotonic_ns=time.monotonic_ns(), **data)

    def invoke(self, *, role, model, files, instructions, allowed_paths, feedback, metadata):
        if self.fatal.is_set():
            raise RuntimeError("An earlier execution requires investigation")
        if model not in {"cheap", "strong"}:
            raise ValueError("Unknown pinned model profile")
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
            execution_kind="scripted_worker", controller_metadata=deepcopy(metadata),
            journal_paths={key: str(path) for key, path in self.journal.paths(call_id).items()})

        def scripted_call():
            if self.fatal.is_set():
                raise RuntimeError("Dispatch stopped after another failure")
            with self.spans.measure("scripted_worker", run_id=self.session_id, stage_index=self.stage,
                    call_id=call_id, role=role, model=model) as span:
                row["physical_span_id"] = span["span_id"]
                return known_result(self.project, self.module, self.stage, metadata)

        try:
            with self.spans.measure("worker_request", run_id=self.session_id, stage_index=self.stage,
                    call_id=call_id, role=role, model=model) as span:
                row["request_span_id"] = span["span_id"]
                outcome = self.journal.execute(call_id, request, reservation, scripted_call,
                    lambda: self.budget.reserve(reservation, self.keeper.check(self.billing), 0),
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
        accounting_record = dict(completed=False, status="shared_initial_setup", selected_binding=None)
        record_stage(self.root, accounting_record, self.ledger, self.keeper.check(self.billing))
        accounting_store = GitStore(self.root / "record.git")
        value = dict(protocol=SHARED_PROTOCOL, session_id=self.session_id, mode="offline",
            project_id=self.project["id"], stage_index=self.stage, billing=self.billing,
            contract_sha256=self.contract_sha256, image=self.image, result=result,
            invocations=[self.calls[key] for key in sorted(self.calls)], exports=self.exports,
            validations=self.validations, initial_files_sha256=digest(self.project["initial_files"]),
            validator_origin=self.validator_origin,
            accounting_record=dict(store_path=str(accounting_store.path), accepted_head=accounting_store.head(),
                                   record=accounting_record),
            physical_provider_calls=0, usage_micro_usd=sum(row["usage_units"] for row in self.calls.values()))
        save(self.root / "session.json", value)
        return bound_file(self.root / "session.json")


def prepare_shared_block(output, project_id, repetition, *, contract_sha256, image,
                         ledger, budget, spans, generation_order=("independent", "sequential"),
                         validator_factory=BlackboxValidator, expected_contract=None):
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
    fatal = threading.Event()
    pools = {}
    for formation in generation_order:
        session_id = f"{block_id}-shared-{formation}"
        with OfflineSession(output / formation, project=project, module=module, stage=0,
                session_id=session_id, namespace=namespace, contract_sha256=contract_sha256,
                image=image, ledger=ledger, budget=budget, spans=spans, fatal=fatal,
                validator_factory=validator_factory) as session:
            result = run_continuation_stage(project, 0, formation + "-four", project["initial_files"], {},
                invoke=session.invoke, evaluate=session.evaluate, retain=session.retain,
                validate_probes=oracle_gate(module, project), emit=session.emit,
                after_initial=session.freeze_initial, candidate_seed=seed, limits=LIMITS,
                baseline_approval=dict(files_sha256=digest(project["initial_files"]), source_valid=True,
                    approval_id="trusted-baseline:" + digest(project["initial_files"])),
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
            spans=spans, fatal=fatal, validator_factory=validator_factory) as session:
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
    save(output / "timings.json", read(spans.path))
    snapshot_accounting(ledger, output / "accounting-snapshot.sqlite")
    manifest = dict(protocol=SHARED_PROTOCOL, mode="offline", project_id=project_id,
        repetition=repetition, block_id=block_id, stage_index=0, contract_sha256=contract_sha256,
        candidate_seed=seed, initial_files_sha256=digest(project["initial_files"]),
        generation_order=list(generation_order), pools=pools, scouts=scout_binding,
        frozen_utc=datetime.now(timezone.utc).isoformat(), frozen_monotonic_ns=time.monotonic_ns(),
        physical_provider_calls=0, usage_micro_usd=0,
        namespace=namespace, budget_before=budget_before, budget=ledger.budget(), incremental_micro_usd=0,
        validator_origin="physical_docker" if validator_factory is BlackboxValidator else "unit_test_control",
        contract=bound_file(output / "contract.json"), fixture=bound_file(output / "fixture.json"),
        timings=bound_file(output / "timings.json"), accounting=bound_file(output / "accounting-snapshot.sqlite"),
        baseline_approval=dict(files_sha256=digest(project["initial_files"]), source_valid=True,
                               approval_id="trusted-baseline:" + digest(project["initial_files"])),
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


def prepare_offline_setup(output, project_id, repetition, *, qualification, image=DEFAULT_IMAGE):
    """Run one qualified zero-API setup block, never the full follow-up cohort."""
    output = Path(output).resolve()
    if output.exists():
        raise ValueError("Use a fresh output directory")
    if qualification is None:
        raise ValueError("Physical shared setup requires exact fixture qualification")
    from analysis.qualify_continuation_followup import validate_qualification
    expected = contract(image)
    qualification = Path(qualification).resolve()
    qualification_binding = bound_file(qualification)
    validate_qualification(qualification, digest(expected))
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
        generation_order=study_plan()["generation_order"], expected_contract=expected)
    save(output / "setup-result.json", dict(protocol=PROTOCOL, status="shared_setup_prepared",
        primary_cohort=False, api_calls=0, qualification=qualification_binding,
        shared_setup=bound_file(output / "block" / "shared-setup.json")))
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["describe", "prepare-shared-setup"])
    parser.add_argument("--output", type=Path)
    parser.add_argument("--project", choices=list(PROJECT_MODULES))
    parser.add_argument("--repetition", type=int, choices=list(range(REPETITIONS)))
    parser.add_argument("--qualification", type=Path)
    parser.add_argument("--image", default=DEFAULT_IMAGE)
    args = parser.parse_args()
    if args.command == "describe":
        print(json.dumps(dict(readiness=live_readiness(study_plan()), call_envelope=call_envelope()), indent=2))
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
