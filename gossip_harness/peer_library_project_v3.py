"""Twenty-process, explicitly scripted M1 project integration rehearsal.

This is an offline protocol qualification, not an autonomous software result.
The only completion transport is a trusted authored fixture. Candidate execution
uses the real pinned Docker evaluator. The controller never loads credentials or
the production ledger. Run only after the combined cheap verification gates.
"""
from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import asdict, dataclass, replace
import hashlib
import json
import os
from pathlib import Path
import secrets
import sqlite3
import subprocess
import sys
import threading
import time
import traceback
from typing import Any
import urllib.request

from .gitstore import GitStore
from .ledger import Ledger
from .library_project_fixture_v1 import PACKAGE_SCOPES, RUNTIME_IMAGE, public_cases, seed_files
from .library_m1_reference_v1 import m1_files, package_changes, catalog_repair_changes
from .peer_candidate_v2 import CandidatePublisher, named_sources, strictdecode_candidate_notice
from .peer_financial_authority_v2 import CumulativeAuthorityV2, canonical_payload as _canonical_payload, ledger_identity
from .peer_financial_rpc_v2 import FinancialClient, FinancialRPC, FinancialServer
from .peer_git_bundle_v1 import export_bundle, import_bundle
from .peer_mesh_finance_v2 import MeshFinancePayloads
from .peer_mesh_v2 import MeshConfig, MeshNode, observer_request
from .peer_project_contract_v2 import (
    CandidateOffer, Context, DispatchBinding, EvidenceRef, LocalViewManifest,
    ReleaseTarget, SelectionManifest, WorkKey, encode, from_dict, identity,
    resolve_local, strict_loads, to_dict, worker_request_digest,
)
from .peer_project_evidence_v3 import ContributionSlot, EvidenceRegistry
from .peer_project_execution_v3 import ExecutionPolicy, ProjectExecution, required_suite
from .peer_project_promotion_v3 import ProjectPromotion, PromotionResult
from .peer_project_views_v3 import ProjectRoleLoopV3, materialization_directive, verify_materialized_view
from .peer_review_release_v2 import ReviewPolicy, ReviewerProfile, ScopeRule, SLOTS, policy_digest, suite_digest
from .peer_role_loop_v2 import WorkDirective, directive_id, financial_task_id
from .peer_twenty_role_fixture_v2 import BUILDERS, REVIEWERS, ROLES, PACKAGES, SOURCES as V2_SOURCES
from .peer_twenty_role_fixture_v2 import _read, _write as _fixture_write, _wait, runtime_identity
from .worker import HTTPResponse, MODEL, STRONG_MODEL, OpenAIWorker, WorkerRequest, WorkerResult

PROTOCOL = "peer-library-project-v3"
ROSTER = ROLES + ("seed", "finance")
COHORT = "library-m1-offline-rehearsal"
SOURCES = tuple(dict.fromkeys((*V2_SOURCES, "peer_library_project_v3.py",
    "peer_project_views_v3.py", "peer_project_evidence_v3.py", "peer_project_execution_v3.py",
    "peer_project_promotion_v3.py", "peer_review_release_v2.py", "library_project_fixture_v1.py",
    "library_m1_reference_v1.py", "library_m1_ingestion_reference_v1.py",
    "library_m1_clients_reference_v1.py", "blackbox_validator.py", "sandbox.py")))
CLAIMS_EXCLUDED = ("autonomous-model-quality", "live-provider", "statistical-superiority",
    "four-milestone-study", "six-trajectory-study", "process-restart", "partition-recovery")


class RehearsalError(RuntimeError):
    """A retained failed qualification, never a correctness pass."""


def require(condition: bool, detail: str) -> None:
    if not condition:
        raise RehearsalError(detail)


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _plain(value: Any) -> Any:
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    if isinstance(value, dict):
        return {key: _plain(item) for key, item in value.items()}
    return value


def canonical_payload(value: Any) -> bytes:
    return _canonical_payload(_plain(value))


def _write(path: Path, value: Any) -> None:
    _fixture_write(path, _plain(value))


@dataclass(frozen=True)
class ProjectConfig:
    deadline_seconds: float = 1800
    interval: float = 0.08
    executor_slots: int = 4

    def __post_init__(self) -> None:
        require(type(self.deadline_seconds) in (int, float) and 240 <= self.deadline_seconds <= 1800,
                "Project deadline must be between 240 and 1800 seconds")
        require(type(self.interval) in (int, float) and 0.04 <= self.interval <= 0.25,
                "Mesh interval outside registered bounds")
        require(type(self.executor_slots) is int and self.executor_slots == 4,
                "The rehearsal uses exactly four executor slots")


def fingerprints() -> dict[str, str]:
    return {name: sha(Path(__file__).with_name(name).read_bytes()) for name in SOURCES}


def attest(contract: dict[str, Any]) -> dict[str, Any]:
    actual = {"sources": fingerprints(), "runtime": runtime_identity()}
    require(actual["sources"] == contract["sources"] and actual["runtime"] == contract["runtime"],
            "Source or host runtime changed during rehearsal")
    return actual


def package_for(actor: str) -> str:
    require(actor in ROLES, "Unknown role")
    return PACKAGES[BUILDERS.index(actor) // 4 if actor in BUILDERS else REVIEWERS.index(actor)]


def exact_scopes() -> dict[str, tuple[str, ...]]:
    files = m1_files()
    return {package: tuple(sorted(path for path in files
            if any(path == root.rstrip("/") or path.startswith(root.rstrip("/") + "/") for root in PACKAGE_SCOPES[package])))
            for package in PACKAGES}


def call_limit(actor: str) -> int:
    require(actor in ROLES, "Unknown role")
    return 4 if actor == "R1" else 2 if actor == "B01" or actor in REVIEWERS else 1


def work_key(actor: str, kind: str, generation: int) -> WorkKey:
    require(kind in ("build", "repair", "select_source", "review") and generation in (0, 1),
            "Unknown scripted action")
    return WorkKey(package_for(actor), "library-m1", actor + "-" + kind, generation)


def instructions(actor: str, kind: str, generation: int) -> str:
    return (f"OFFLINE SCRIPTED PROTOCOL QUALIFICATION ONLY. Role {actor}; {kind}; generation {generation}. "
            "Read the complete arrived source and evidence. Return only the registered output paths. "
            "The fixture completion is authored, and is not an autonomous quality observation.")


def action_plan() -> tuple[tuple[str, str, int], ...]:
    return (tuple((actor, "build", 0) for actor in BUILDERS)
            + (("R1", "select_source", 0),)
            + tuple((actor, "review", 0) for actor in REVIEWERS)
            + (("B01", "repair", 1), ("R1", "select_source", 1))
            + tuple((actor, "review", 1) for actor in REVIEWERS))


def execution_contract(config: ProjectConfig) -> dict[str, Any]:
    return {"protocol": PROTOCOL, "purpose": "offline-M1-project-integration-qualification",
        "roles": list(ROLES), "infrastructure": ["seed", "finance"], "settings": asdict(config),
        "sources": fingerprints(), "runtime": runtime_identity(), "image_id": RUNTIME_IMAGE,
        "seed_sha256": sha(canonical_payload(seed_files())),
        "reference_sha256": sha(canonical_payload(m1_files())),
        "public_cases_sha256": sha(canonical_payload(public_cases("m1"))),
        "actions": [{"actor": a, "kind": k, "generation": g, "work": to_dict(work_key(a, k, g)),
                     "instructions_sha256": sha(instructions(a, k, g).encode())} for a, k, g in action_plan()],
        "aggregate_role_call_limits": {actor: call_limit(actor) for actor in ROLES},
        "transport": "injected-authored-no-provider", "candidate_execution": "physical-pinned-Docker",
        "intended_fault": "clean-merge-loses-per-source-provenance-through-blob-deduplication",
        "repair": "catalog-generation-1-on-failed-integrated-base",
        "claims_excluded": list(CLAIMS_EXCLUDED)}


def selector_artifact(frontier: dict[str, Any], frontier_sha: str) -> dict[str, Any]:
    selected = []
    for package in PACKAGES:
        entries = [entry for entry in frontier["offers"] if entry["slot"]["package_id"] == package]
        require(bool(entries), "Fixture selector received incomplete frontier")
        choice = min(entries, key=lambda item: item["actor"])
        selected.append({"package_id": package, "offer_sha256": choice["offer_sha256"]})
    return {"protocol": "peer-project-selection-v3", "eligibility_policy_sha256": frontier["eligibility_policy_sha256"],
        "frontier_sha256": frontier_sha, "eligible_offer_sha256s": [item["offer_sha256"] for item in frontier["offers"]],
        "selected": selected}


def review_artifact(actor: str, target: ReleaseTarget, directive: WorkDirective,
                    requirements: tuple[str, ...], *, approve: bool) -> dict[str, Any]:
    refs = (directive.source_ref, *tuple(ref for ref in directive.evidence_refs
            if ref.kind in ("selection-manifest", "execution-receipt")))
    return {"protocol": "peer-scoped-review-v2", "target_sha256": identity(target), "verdicts": [
        {"scope_id": scope, "covered_requirement_ids": list(requirements),
         "evidence_refs": [to_dict(ref) for ref in refs], "verdict": "approve" if approve else "request_changes",
         "rationale": ("Scripted approval after the cumulative physical public suite passed." if approve else
                       "Scripted rejection: physical provenance check failed on the merged source.")}
        for reviewer, scope in SLOTS if reviewer == actor]}


class ScriptedTransport:
    """The sole completion transport. No network fallback or credential loading."""
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.responses: dict[str, tuple[WorkDirective, dict[str, str], str]] = {}
        self.calls: list[dict[str, Any]] = []
        self.active = 0
        self.peak = 0

    def register(self, directive: WorkDirective, changes: dict[str, str], actor: str) -> None:
        task = financial_task_id(directive.context, directive.work)
        with self.lock:
            require(task not in self.responses, "Fixture task response already registered")
            self.responses[task] = (directive, dict(changes), actor)

    def __call__(self, request: urllib.request.Request, timeout: float, maximum: int) -> HTTPResponse:
        require(isinstance(request.data, bytes), "Fixture received a non-byte request")
        raw = request.data
        assert isinstance(raw, bytes)
        payload = json.loads(raw)
        task = json.loads(payload["input"])
        with self.lock:
            directive, changes, actor = self.responses[task["task_id"]]
            recipe = strict_loads(task["validation_feedback"])
            require(recipe["directive"] == directive.to_dict()
                    and task["allowed_paths"] == list(directive.allowed_paths)
                    and task["instructions"] == directive.instructions, "Fixture request changed registered directive")
            require(all(call["task_id"] != task["task_id"] for call in self.calls), "Duplicate fixture invocation")
            call = {"actor": actor, "kind": directive.kind, "generation": directive.work.generation,
                    "task_id": task["task_id"], "started_at": time.monotonic(), "finished_at": None,
                    "request_sha256": sha(raw), "request_bytes": len(raw)}
            self.calls.append(call)
            self.active += 1
            self.peak = max(self.peak, self.active)
        try:
            time.sleep(0.12)
            proposal = {"changes": [{"path": path, "content": content} for path, content in changes.items()],
                        "summary": "Authored offline integration fixture completion."}
            body = {"id": "resp_scripted_" + sha(raw)[:24], "model": payload["model"], "status": "completed",
                    "service_tier": "default", "usage": {"input_tokens": 101, "output_tokens": 20, "total_tokens": 121},
                    "output": [{"type": "message", "role": "assistant", "status": "completed",
                        "content": [{"type": "output_text", "text": json.dumps(proposal)}]}]}
            return HTTPResponse(200, {"X-Request-Id": "req_scripted_" + sha(raw)[:24]}, json.dumps(body).encode())
        finally:
            with self.lock:
                call["finished_at"] = time.monotonic()
                self.active -= 1

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return {"calls": [dict(item) for item in self.calls], "active": self.active, "peak": self.peak}


def _child(config_path: Path) -> int:
    raw = _read(config_path)
    root = config_path.parent
    node = MeshNode(MeshConfig.from_dict(raw["mesh"]))
    loop: ProjectRoleLoopV3 | None = None
    completed: set[str] = set()
    commands: dict[str, dict[str, Any]] = {}
    try:
        _write(root / "ready.json", {**node.start(), "attestation": attest(raw["contract"])})
        client = FinancialClient(raw["finance_port"], raw["actor"], raw["capability"], raw["contract_sha256"])
        policies = {(item["kind"], item["generation"]): item["sha256"] for item in raw["view_policies"]}
        loop = ProjectRoleLoopV3(root / "journal", raw["actor"], node, client,
            call_limit=call_limit(raw["actor"]), policy_sha256=raw["runtime_policy_sha256"],
            result_producer="finance", lease_ttl=180, view_policies=policies)
        while not (root / "stop").exists():
            require(time.monotonic() < raw["deadline_monotonic"], "Role deadline expired")
            for path in sorted((root / "commands").glob("*.json")):
                if path.stem not in commands:
                    command = _read(path)
                    directive = WorkDirective.from_dict(command["directive"])
                    key = loop.enqueue(directive)
                    require(path.stem == key, "Command filename differs from directive identity")
                    commands[key] = command
            loop.tick()
            for snapshot in loop.snapshots():
                if snapshot.directive_id in completed or snapshot.state not in ("published", "stopped"):
                    continue
                require(snapshot.state == "published" and snapshot.reply is not None
                        and snapshot.reply.state == "completed" and snapshot.reply.binding is not None,
                        "Role action stopped without successful completion")
                key = snapshot.directive_id
                request = loop.worker_request(key)
                assert snapshot.result_ref is not None and snapshot.publication_ref is not None
                assert snapshot.reply is not None and snapshot.reply.binding is not None and snapshot.action is not None
                assert snapshot.view is not None and snapshot.reply.result_payload_sha256 is not None
                result_raw = node.resolve(snapshot.result_ref)
                assert result_raw is not None
                body = strict_loads(result_raw)
                result = WorkerResult(**body["payload"])
                candidate = None
                git_path = None
                if raw["actor"] in BUILDERS:
                    command = commands[key]
                    git_path = root / ("git-" + key)
                    if "base_bundle" in command:
                        base_info = command["base_bundle"]
                        ref = from_dict(EvidenceRef, base_info["ref"])
                        while node.resolve(ref) is None:
                            require(time.monotonic() < raw["deadline_monotonic"], "Repair base arrival deadline")
                            node.want(ref)
                            time.sleep(0.03)
                        bundle = resolve_local(ref, node.arrived(), node.resolve(ref))
                        imported = import_bundle(bundle, base_info["manifest"], root / ("base-import-" + key))
                        store = GitStore.create(git_path, seed_files())
                        old = store.head()
                        require(imported.is_ancestor(old, request.base_sha)
                                and imported.read_files(request.base_sha) == request.files,
                                "Imported repair base differs from exact arrived history and source")
                        staged = store.prepare(imported, request.base_sha, old,
                            lambda _path: (True, "Authenticated source-only private repair base"),
                            allowed_paths=tuple(path for paths in raw["package_scopes"].values() for path in paths))
                        require(staged.status == "prepared" and staged.candidate_sha == request.base_sha
                                and store.accept(staged).status == "accepted",
                                "Private repair base did not preserve exact imported Git ancestry")
                        _write(root / ("base-adoption-" + key + ".json"), {
                            "purpose": "private-builder-base-only", "bundle_ref": to_dict(ref),
                            "manifest": base_info["manifest"], "prior_seed": old,
                            "adopted_commit": request.base_sha, "protected_release": False})
                    else:
                        store = GitStore.create(git_path, request.files)
                    require(store.head() == request.base_sha and store.read_files() == request.files,
                            "Private builder base differs from arrived source")
                    publisher = CandidatePublisher(root / ("candidate-" + key), node, store, actor=raw["actor"],
                        baseline_sha=request.base_sha, package_scopes={p: tuple(paths) for p, paths in raw["package_scopes"].items()},
                        execution_contract_sha256=raw["contract_sha256"], completed_proof=loop.completed_proof)
                    publication = publisher.publish(snapshot.reply.binding, request, result, snapshot.reply.result_payload_sha256)
                    candidate = {"offer": to_dict(publication.offer), "offer_ref": to_dict(publication.offer_ref)}
                replay = client.submit(snapshot.action, snapshot.reply.binding.lease)
                require(replay == snapshot.reply, "Exact terminal replay changed")
                _write(root / "results" / (key + ".json"), {"actor": raw["actor"], "pid": os.getpid(),
                    "directive_id": key, "reply": to_dict(snapshot.reply), "view": to_dict(snapshot.view),
                    "publication_ref": to_dict(snapshot.publication_ref), "result_ref": to_dict(snapshot.result_ref),
                    "request_sha256": worker_request_digest(request), "request_bytes": len(canonical_payload(asdict(request))),
                    "candidate": candidate, "git_path": str(git_path) if git_path else None,
                    "exact_terminal_replay": True, "finished_at": time.monotonic()})
                completed.add(key)
            time.sleep(0.04)
        _write(root / "final.json", {"actor": raw["actor"], "pid": os.getpid(),
            "journal_root": str(loop.root), "completed_actions": sorted(completed),
            "call_limit": call_limit(raw["actor"]), "history": list(loop.history()),
            "attestation": attest(raw["contract"]), "mesh_summary": node.summary()})
        return 0
    except BaseException:
        _write(root / "failure.json", {"pid": os.getpid(), "traceback": traceback.format_exc()})
        return 1
    finally:
        try:
            if loop is not None:
                loop.close()
        finally:
            node.close()


def textual_conflict_probe(root: Path) -> dict[str, Any]:
    """Actual Git conflict rejection, independent of semantic evaluation."""
    base = GitStore.create(root / "protected", {"conflict.txt": "original\n"})
    left = GitStore.fork(base, root / "left")
    right = GitStore.fork(base, root / "right")
    left_oid = left.propose({"conflict.txt": "left\n"})
    right_oid = right.propose({"conflict.txt": "right\n"})
    first = base.prepare(left, left_oid, base.head(), lambda _p: (True, "Source-only control"), ("conflict.txt",))
    require(base.accept(first).status == "accepted", "Conflict control first merge failed")
    accepted = base.head()
    conflict = base.prepare(right, right_oid, accepted, lambda _p: (True, "Must not validate conflict"), ("conflict.txt",))
    require(conflict.status == "text_conflict" and base.head() == accepted, "Text conflict changed protected head")
    return {"status": "rejected", "candidate": asdict(conflict), "head_unchanged": True,
            "candidate_execution": False, "purpose": "source-only-textual-conflict-negative-control"}


def validate_accounting(observations: dict[str, Any], completions: dict[str, dict[str, Any]],
                        reservations: list[dict[str, Any]]) -> dict[str, Any]:
    """Require a one-to-one final reconciliation, not an authored expected count."""
    calls = observations["calls"]
    expected = Counter(action_plan())
    actual = Counter((item["actor"], item["kind"], item["generation"]) for item in calls)
    bindings = tuple(from_dict(DispatchBinding, result["reply"]["binding"]) for result in completions.values())
    require(actual == expected and observations["active"] == 0
            and all(item["finished_at"] is not None for item in calls), "Scripted action cohort differs")
    require(Counter((b.action.actor, b.action.kind, b.action.work.generation) for b in bindings) == expected
            and len({b.call_id for b in bindings}) == len(bindings)
            and len({b.reservation_id for b in bindings}) == len(bindings), "Actual charged completion cohort differs")
    require(len(reservations) == len(bindings)
            and {row["id"] for row in reservations} == {b.reservation_id for b in bindings}
            and all(row["state"] == "settled" and type(row["spent"]) is int for row in reservations),
            "Disposable accounting is incomplete or has unsettled outcomes")
    require(Counter(item["actor"] for item in calls) == Counter({a: call_limit(a) for a in ROLES}),
            "An aggregate role allowance was bypassed or omitted")
    return {"completed_actions": len(bindings), "settled_reservations": len(reservations),
            "unsettled_reservations": 0, "simulated_spend_units": sum(row["spent"] for row in reservations),
            "per_actor_calls": dict(sorted(Counter(item["actor"] for item in calls).items())),
            "api_spend": False}


def run_project(output: Path, config: ProjectConfig = ProjectConfig()) -> dict[str, Any]:
    output = output.absolute()
    require(output == output.resolve() and not output.exists(), "Output must be a fresh canonical path")
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    started = time.monotonic()
    deadline = started + config.deadline_seconds
    contract = execution_contract(config)
    contract_sha = sha(canonical_payload(contract))
    _write(output / "execution-contract.json", contract)
    receipt: dict[str, Any] = {"protocol": PROTOCOL, "passed": False, "attempted": True, "physically_executed": False,
        "execution_contract_sha256": contract_sha, "claims_excluded": list(CLAIMS_EXCLUDED)}
    processes: dict[str, subprocess.Popen[Any]] = {}
    streams: list[Any] = []
    nodes: list[MeshNode] = []
    registries: list[EvidenceRegistry] = []
    finance = None
    payloads = None
    execution = None
    promotion = None
    server = None
    server_thread = None
    transport = ScriptedTransport()
    cleanup_errors: list[str] = []
    stages: list[dict[str, Any]] = []
    completions: dict[str, dict[str, Any]] = {}
    directives: dict[str, WorkDirective] = {}
    physical_publications: list[dict[str, Any]] = []
    try:
        transport_key, observer_key = secrets.token_hex(32), secrets.token_hex(32)
        capabilities = {actor: secrets.token_hex(32) for actor in ROLES}
        def mesh_config(actor: str, root: Path) -> MeshConfig:
            return MeshConfig(root, actor, COHORT, contract_sha, ROSTER, transport_key,
                              observer_key=observer_key, interval=config.interval, fanout=2)
        seed = MeshNode(mesh_config("seed", output / "seed-mesh"))
        nodes.append(seed)
        financial_mesh = MeshNode(mesh_config("finance", output / "finance-mesh"))
        nodes.append(financial_mesh)
        seed_ready, finance_ready = seed.start(), financial_mesh.start()
        files = seed_files()
        scopes = exact_scopes()
        protected = GitStore.create(output / "protected-git", files)
        baseline = protected.head()
        cases = public_cases("m1")
        context = Context(contract_sha, COHORT, "semantic-rejection-and-repair", 1, sha(canonical_payload(cases)))
        suite = required_suite(context, cases)
        source_ref = seed.publish("project-source", canonical_payload({"files": files, "base_sha": baseline}), "seed-source")
        execution = ProjectExecution(output / "execution", seed, protected, producer="seed",
            execution_contract_sha256=contract_sha, cohort_id=COHORT,
            policy=ExecutionPolicy(RUNTIME_IMAGE, timeout_seconds=90, case_timeout_seconds=5, seed=0))
        workers = {"mini": OpenAIWorker("offline-scripted-no-key", model=MODEL, max_output_tokens=16384, timeout=30, transport=transport),
                   "strong": OpenAIWorker("offline-scripted-no-key", model=STRONG_MODEL, max_output_tokens=16384, timeout=30, transport=transport)}
        ledger_path = output / "disposable-ledger.sqlite"
        Ledger(ledger_path, 100_000_000)
        def allowed(actor: str, kind: str) -> tuple[str, ...]:
            return ("selection.json",) if kind == "select_source" else ("review.json",) if kind == "review" else scopes[package_for(actor)]
        specs = [{"context": to_dict(context), "work": to_dict(work_key(a, k, g)), "actors": [a], "kinds": [k],
            "profiles": ["mini" if a in BUILDERS else "strong"], "allowed_paths": list(allowed(a, k)),
            "max_reserved_units": 8_000_000} for a, k, g in action_plan()]
        cohort = {"cohort_id": COHORT, "execution_contract_sha256": contract_sha,
            "ledger_identity": ledger_identity(ledger_path), "journal_root": str(output / "financial-journal"),
            "transport_identity": "authored-project-rehearsal-no-provider", "task_specs": specs}
        payloads = MeshFinancePayloads(output / "financial-payloads", financial_mesh, ROLES)
        finance = CumulativeAuthorityV2.open(ledger_path, output / "financial-service", cohort,
            100_000_000, 100_000_000, 0, payloads=payloads, workers=workers, max_workers=4)
        profile_hashes = {key: sha(canonical_payload(value)) for key, value in finance.profiles.items()}
        eligibility = sha(b"library-m1-scripted-complete-frontier-v3")
        requirements = tuple(dict.fromkeys(case.get("requirement", case["id"]) for case in cases))
        inherited = tuple(item for item in named_sources(files) if item.path not in {p for ps in scopes.values() for p in ps})
        policy = ReviewPolicy(context, tuple(ScopeRule(a, p, requirements) for a, p in SLOTS),
            tuple(ReviewerProfile(a, "strong", profile_hashes["strong"]) for a in REVIEWERS),
            requirements, requirements, requirements, eligibility, suite_digest(suite), execution.provenance_sha256,
            finance.config_sha256, tuple((p, 0) for p in PACKAGES), inherited)
        repair_policy = replace(policy, package_generations=tuple((p, int(p == "catalog")) for p in PACKAGES))
        view_policies = {("build", 0): eligibility, ("repair", 1): eligibility,
            ("select_source", 0): eligibility, ("select_source", 1): eligibility,
            ("review", 0): policy_digest(policy), ("review", 1): policy_digest(repair_policy)}
        rpc = FinancialRPC(finance, capabilities, request_guard=payloads.request_guard,
                           request_guard_sha256=payloads.request_guard_sha256)
        server = FinancialServer(0, rpc)
        server_thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
        server_thread.start()
        env = {name: value for name, value in os.environ.items() if name not in {"OPENAI_API_KEY", "OPENAI_ORG_ID", "OPENAI_PROJECT_ID"}}
        for actor in ROLES:
            root = output / "roles" / actor
            (root / "commands").mkdir(parents=True)
            (root / "results").mkdir()
            mesh = mesh_config(actor, root / "mesh")
            mesh_raw = asdict(mesh)
            mesh_raw.update(root=str(mesh.root), roster=list(mesh.roster), brokers=list(mesh.brokers))
            path = root / "config.json"
            _write(path, {"actor": actor, "mesh": mesh_raw, "capability": capabilities[actor],
                "finance_port": server.server_address[1], "contract_sha256": contract_sha, "contract": contract,
                "runtime_policy_sha256": sha(canonical_payload({"actions": contract["actions"], "scopes": scopes})),
                "view_policies": [{"kind": k, "generation": g, "sha256": value} for (k, g), value in view_policies.items()],
                "package_scopes": scopes, "deadline_monotonic": deadline})
            stream = (root / "process.log").open("xb")
            streams.append(stream)
            processes[actor] = subprocess.Popen([sys.executable, "-m", "gossip_harness.peer_library_project_v3",
                "--role-config", str(path)], stdout=stream, stderr=stream, env=env, cwd=Path(__file__).resolve().parents[1])
        _wait(lambda: all((output / "roles" / a / "ready.json").exists() for a in ROLES), deadline, processes)
        ready = {a: _read(output / "roles" / a / "ready.json") for a in ROLES}
        require(len({v["pid"] for v in ready.values()}) == 20 and all(ready[a]["pid"] == processes[a].pid for a in ROLES),
                "Twenty distinct OS roles were not observed")
        peers = {**{a: value["port"] for a, value in ready.items()}, "seed": seed_ready["port"], "finance": finance_ready["port"]}
        seed.configure(peers)
        financial_mesh.configure(peers)
        for actor in ROLES:
            observer_request(peers[actor], actor, observer_key, "configure", {"peers": peers})

        registry_for_action: dict[str, EvidenceRegistry] = {}
        def arrived(ref: EvidenceRef) -> bool:
            seed.want(ref)
            return ref in seed.arrived() and seed.resolve(ref) is not None
        def wait_refs(refs: tuple[EvidenceRef, ...]) -> None:
            _wait(lambda: all([arrived(ref) for ref in refs]), deadline, processes)
        def make(actor: str, kind: str, generation: int, source: EvidenceRef,
                 evidence: tuple[EvidenceRef, ...] = ()) -> WorkDirective:
            return WorkDirective(context, work_key(actor, kind, generation), "mini" if actor in BUILDERS else "strong",
                kind, source, evidence, allowed(actor, kind), instructions(actor, kind, generation))
        def dispatch(actor: str, directive: WorkDirective, changes: dict[str, str], base_bundle: dict[str, Any] | None = None) -> str:
            key = directive_id(directive)
            directives["dispatch-" + key] = directive
            transport.register(directive, changes, actor)
            command: dict[str, Any] = {"directive": directive.to_dict()}
            if base_bundle is not None:
                command["base_bundle"] = base_bundle
            _write(output / "roles" / actor / "commands" / (key + ".json"), command)
            return key
        def collect(actor: str, key: str) -> dict[str, Any]:
            path = output / "roles" / actor / "results" / (key + ".json")
            _wait(path.exists, deadline, processes)
            result = _read(path)
            binding = from_dict(DispatchBinding, result["reply"]["binding"])
            ref = from_dict(EvidenceRef, result["publication_ref"])
            view = from_dict(LocalViewManifest, result["view"])
            wait_refs((ref, binding.action.worker_payload_ref, from_dict(EvidenceRef, result["result_ref"]),
                       view.source_ref, *view.evidence_refs))
            completions[binding.action.request_id] = result
            return result
        def pure_view(binding: DispatchBinding, request: WorkerRequest, claimed: LocalViewManifest) -> LocalViewManifest:
            expected = directives[binding.action.request_id]
            require(materialization_directive(request) == expected, "Actual request differs from registered exact directive")
            return verify_materialized_view(binding, request, claimed, seed,
                policy_sha256=view_policies[binding.action.kind, binding.action.work.generation],
                expected_instructions_sha256=sha(expected.instructions.encode()), expected_attempt_limit=expected.attempt_limit)
        def verified_view(binding: DispatchBinding) -> LocalViewManifest:
            assert finance is not None
            proof = finance.verified_terminal(binding)
            result = completions[binding.action.request_id]
            ref = from_dict(EvidenceRef, result["publication_ref"])
            body = strict_loads(resolve_local(ref, seed.arrived(), seed.resolve(ref)))
            require(ref.producer == binding.action.actor and ref.kind == "role-result"
                    and body["protocol"] == "peer-role-loop-v2" and body["actor"] == binding.action.actor
                    and body["action"] == to_dict(binding.action) and body["reply"] == to_dict(proof["reply"]),
                    "Review completion is not the exact arrived accountable role result")
            result_ref = from_dict(EvidenceRef, body["result_ref"])
            require(result_ref.producer == "finance" and result_ref.kind == "financial-result"
                    and resolve_local(result_ref, seed.arrived(), seed.resolve(result_ref)) == canonical_payload(proof["result_payload"]),
                    "Review financial result bytes differ")
            view = pure_view(binding, proof["worker_request"], from_dict(LocalViewManifest, body["view_manifest"]))
            registry_for_action[binding.action.request_id].verified_candidate_materials(view)
            return view
        def slot(actor: str, kind: str, generation: int, base: str) -> ContributionSlot:
            profile = "mini" if actor in BUILDERS else "strong"
            return ContributionSlot(actor, work_key(actor, kind, generation), profile, profile_hashes[profile],
                allowed(actor, kind), base, sha(instructions(actor, kind, generation).encode()))
        def registry(generation: int, prior_base: str) -> EvidenceRegistry:
            slots = tuple(slot(a, "repair" if generation and a == "B01" else "build",
                1 if generation and a == "B01" else 0, prior_base if generation and a == "B01" else baseline)
                for a in BUILDERS if not generation or a not in ("B02", "B03", "B04"))
            value = EvidenceRegistry(output / ("registry-" + str(generation)), seed, context=context,
                authority_config_sha256=finance.config_sha256,
                package_scopes={p: tuple(root.rstrip("/") for root in v) for p, v in PACKAGE_SCOPES.items()}, slots=slots,
                selector=slot("R1", "select_source", generation, baseline),
                bases={baseline: files, prior_base: protected.read_files(prior_base)}, eligibility_policy_sha256=eligibility,
                view_policies=view_policies, verified_terminal=finance.verified_terminal, verified_view=pure_view)
            registries.append(value)
            return value
        def register(value: EvidenceRegistry, result: dict[str, Any]) -> CandidateOffer:
            ref = from_dict(EvidenceRef, result["candidate"]["offer_ref"])
            wait_refs((ref,))
            notice = seed.resolve(ref)
            assert notice is not None
            offer, _manifest = strictdecode_candidate_notice(notice, expected_contract_sha256=contract_sha)
            wait_refs((offer.bundle_ref,))
            return value.register_offer(ref, from_dict(EvidenceRef, result["publication_ref"]))
        build_keys = {a: dispatch(a, make(a, "build", 0, source_ref),
            package_changes(package_for(a), defective_blob_dedup=(a == "B01"))) for a in BUILDERS}
        build_results = {a: collect(a, build_keys[a]) for a in BUILDERS}
        initial_registry = registry(0, baseline)
        initial_offers = tuple(register(initial_registry, build_results[a]) for a in BUILDERS)
        registry_by_selection: dict[str, EvidenceRegistry] = {}
        registry_by_offer: dict[str, EvidenceRegistry] = {}
        for offer in initial_offers:
            registry_by_offer[identity(offer)] = initial_registry
        def verified_selection(selection: SelectionManifest) -> SelectionManifest:
            return registry_by_selection[identity(selection)].verified_selection(selection)
        def verified_contribution(offer: CandidateOffer) -> Any:
            return registry_by_offer[identity(offer)].verified_contribution(offer)
        promotion = ProjectPromotion(output / "promotion", protected, repository_id="local-research-library",
            baseline_sha=baseline, policy=policy, initial_suite=suite, package_scopes=scopes,
            verified_selection=verified_selection, verified_contribution=verified_contribution,
            verified_terminal=finance.verified_terminal, verified_view=verified_view,
            verified_execution=execution.verified_execution)
        def round_trip(generation: int, value: EvidenceRegistry, offers: tuple[CandidateOffer, ...]) -> tuple[Any, Any, Any]:
            frontier_ref = value.frontier()
            frontier_raw = seed.resolve(frontier_ref)
            assert frontier_raw is not None
            frontier = strict_loads(frontier_raw)
            materials = tuple(ref for offer in offers for ref in value.normalized_refs(offer))
            select = make("R1", "select_source", generation, source_ref, (frontier_ref, *materials))
            key = dispatch("R1", select, {"selection.json": json.dumps(selector_artifact(frontier, frontier_ref.payload_sha256))})
            result = collect("R1", key)
            binding = from_dict(DispatchBinding, result["reply"]["binding"])
            registry_for_action[binding.action.request_id] = value
            selection = value.materialize_selection(binding, from_dict(EvidenceRef, result["publication_ref"]))
            registry_by_selection[identity(selection)] = value
            stores = {identity(offer): GitStore(value.root / ("quarantine-" + identity(offer)) / "quarantine.git") for offer in offers}
            staged = promotion.stage(selection, offers, stores, generation=generation,
                package_generations=(policy if generation == 0 else repair_policy).package_generations,
                integration_order=PACKAGES if generation == 0 else ("ingestion", "query", "clients", "catalog"))
            publication = execution.execute("m1-public-generation-" + str(generation),
                execution.subject(context, staged.commit_oid, suite), suite, cases)
            physical_publications.append(asdict(publication))
            receipt["physically_executed"] = any(item["physically_executed"] for item in physical_publications)
            require(publication.physically_executed and not publication.replayed, "Expected a new physical merged-source evaluation")
            expected_failures = ("m1-per-source-provenance",) if generation == 0 else ()
            failed = tuple(check for check, status in publication.check_results if status != "passed")
            require(failed == expected_failures, "Physical check outcomes differ from the registered semantic control")
            target = promotion.bind_target(staged, evaluator_sha256=execution.evaluator_sha256,
                                           execution_receipt_refs=(publication.receipt_ref,))
            target_ref = seed.publish("release-target", encode(target), "target-" + identity(target))
            target_source = seed.publish("project-source", canonical_payload({"files": protected.read_files(staged.commit_oid),
                "base_sha": staged.commit_oid}), "source-" + identity(target))
            selected_ids = {item.offer_sha256 for item in selection.selected}
            evidence = (target_source, value.selection_ref(selection), publication.receipt_ref,
                *tuple(ref for offer in offers if identity(offer) in selected_ids for ref in value.normalized_refs(offer)))
            review_directives = {a: make(a, "review", generation, target_ref, evidence) for a in REVIEWERS}
            promotion.begin_review_round(target, round_number=generation,
                requests=tuple((a, "dispatch-" + directive_id(review_directives[a])) for a in REVIEWERS))
            keys = {a: dispatch(a, review_directives[a], {"review.json": json.dumps(review_artifact(a, target,
                review_directives[a], requirements, approve=generation == 1))}) for a in REVIEWERS}
            review_results = [collect(a, keys[a]) for a in REVIEWERS]
            bindings = tuple(from_dict(DispatchBinding, result["reply"]["binding"]) for result in review_results)
            for review_binding in bindings:
                registry_for_action[review_binding.action.request_id] = value
            verdicts = promotion.record_reviews(target, bindings)
            outcome = promotion.promote(target)
            require(len(verdicts) == 8, "Missing actual scoped reviewer decisions")
            if generation == 0:
                require(not isinstance(outcome, PromotionResult) and not outcome.eligible
                        and len(outcome.rejected_slots) == 8 and protected.head() == baseline,
                        "Semantic failure or review rejection advanced protected Git")
            else:
                require(isinstance(outcome, PromotionResult) and outcome.status == "accepted"
                        and protected.head() == target.commit_oid, "Fresh complete review did not promote exact repaired source")
            record = {"generation": generation, "selection": to_dict(selection), "target": to_dict(target),
                "execution": asdict(publication), "verdicts": [to_dict(item) for item in verdicts], "outcome": asdict(outcome)}
            _write(output / ("round-" + str(generation) + ".json"), record)
            stages.append({"generation": generation, "commit_oid": target.commit_oid,
                "failed_checks": list(failed), "scoped_decisions": len(verdicts), "promoted": generation == 1})
            return staged, target, selection
        bad_stage, _bad_target, _bad_selection = round_trip(0, initial_registry, initial_offers)
        # The repair inherits actual integrated history through gossip bundle bytes.
        protected._git("update-ref", "refs/harness/proposals/" + bad_stage.commit_oid, bad_stage.commit_oid)
        bundle, manifest = export_bundle(protected, bad_stage.commit_oid, baseline)
        bundle_ref = seed.publish("project-base-bundle", bundle, "repair-base-bundle")
        repair_source = seed.publish("project-source", canonical_payload({"files": protected.read_files(bad_stage.commit_oid),
            "base_sha": bad_stage.commit_oid}), "repair-base-source")
        repair_key = dispatch("B01", make("B01", "repair", 1, repair_source), catalog_repair_changes(),
                              {"ref": to_dict(bundle_ref), "manifest": manifest})
        repair_result = collect("B01", repair_key)
        repaired_registry = registry(1, bad_stage.commit_oid)
        repaired_offers = tuple(register(repaired_registry, repair_result if a == "B01" else build_results[a])
            for a in BUILDERS if a not in ("B02", "B03", "B04"))
        for offer in repaired_offers:
            registry_by_offer[identity(offer)] = repaired_registry
        round_trip(1, repaired_registry, repaired_offers)
        conflict = textual_conflict_probe(output / "textual-conflict-control")
        _write(output / "textual-conflict-control.json", conflict)
        require(protected.read_files() == m1_files(), "Final accepted source differs from complete authored M1 application")
        attest(contract)
        observations = transport.snapshot()
        with sqlite3.connect(ledger_path) as accounting_db:
            accounting_db.row_factory = sqlite3.Row
            reservations = [dict(row) for row in accounting_db.execute("SELECT * FROM reservations ORDER BY id")]
        accounting = validate_accounting(observations, completions, reservations)
        require(time.monotonic() < deadline, "Deadline expired before final qualification")
        receipt.update(passed=True, stages=stages, transport=observations, final_commit=protected.head(),
            accounting=accounting, disposable_ledger_budget=finance.ledger.budget(), elapsed_seconds=time.monotonic() - started,
            counts={"role_processes": 20, "builder_processes": 16, "reviewer_processes": 4,
                "infrastructure_members": 2, "injected_calls": 27, "api_calls": 0, "api_spend_micro_usd": 0,
                "private_candidate_bundles": 17, "physical_public_evaluations": 2,
                "rejected_scope_decisions": 8, "fresh_approved_scope_decisions": 8, "protected_releases": 1,
                "textual_conflicts_rejected": 1}, role_pids={a: ready[a]["pid"] for a in ROLES})
    except BaseException:
        receipt.update(failure=traceback.format_exc(), stages=stages, transport=transport.snapshot())
    finally:
        for actor in processes:
            try:
                (output / "roles" / actor / "stop").touch(exist_ok=False)
            except BaseException as error:
                cleanup_errors.append("Stop marker " + actor + ": " + repr(error))
        stop_deadline = time.monotonic() + 15
        for actor, process in processes.items():
            try:
                process.wait(timeout=max(0.01, stop_deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                try:
                    process.terminate()
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    try:
                        process.kill()
                        process.wait(timeout=3)
                    except BaseException as error:
                        cleanup_errors.append("Process kill " + actor + ": " + repr(error))
                except BaseException as error:
                    cleanup_errors.append("Process terminate " + actor + ": " + repr(error))
                finally:
                    cleanup_errors.append("Forced process cleanup: " + actor)
            except BaseException as error:
                cleanup_errors.append("Process wait " + actor + ": " + repr(error))
            if process.returncode != 0:
                cleanup_errors.append(f"Role {actor} exited {process.returncode}")
        for stream in streams:
            try:
                stream.close()
            except BaseException as error:
                cleanup_errors.append("Log close: " + repr(error))
        final_roles = []
        if receipt["passed"]:
            try:
                for actor, process in processes.items():
                    final = _read(output / "roles" / actor / "final.json")
                    expected_keys = sorted(key.removeprefix("dispatch-") for key, directive in directives.items()
                        if directive.work.slot_id.startswith(actor + "-"))
                    require(final["actor"] == actor and final["pid"] == process.pid
                            and final["journal_root"] == str(output / "roles" / actor / "journal")
                            and final["completed_actions"] == expected_keys and final["call_limit"] == call_limit(actor)
                            and final["attestation"] == {"sources": contract["sources"], "runtime": contract["runtime"]}
                            and final["mesh_summary"]["fatal_error"] is None,
                            "Final role journal, source/runtime attestation, or process identity differs")
                    final_roles.append({"actor": actor, "pid": process.pid, "completed_actions": expected_keys,
                        "journal_root": final["journal_root"], "receipt": str(output / "roles" / actor / "final.json"),
                        "sha256": sha((output / "roles" / actor / "final.json").read_bytes())})
            except BaseException:
                cleanup_errors.append("Final role qualification: " + traceback.format_exc())
        if server is not None:
            try:
                if server_thread is not None and server_thread.is_alive():
                    server.shutdown()
            except BaseException as error:
                cleanup_errors.append("Financial RPC shutdown: " + repr(error))
            try:
                server.server_close()
            except BaseException as error:
                cleanup_errors.append("Financial RPC close: " + repr(error))
        if server_thread is not None:
            try:
                if server_thread.ident is not None:
                    server_thread.join(3)
                if server_thread.is_alive():
                    cleanup_errors.append("Financial RPC thread did not stop")
            except BaseException as error:
                cleanup_errors.append("Financial RPC join: " + repr(error))
        for resource in (promotion, execution, *reversed(registries), finance, payloads, *reversed(nodes)):
            if resource is not None:
                try:
                    resource.close()
                except BaseException as error:
                    cleanup_errors.append(type(error).__name__ + ": " + str(error))
        receipt["cleanup_errors"] = cleanup_errors
        receipt["owned_process_exit_codes"] = {a: p.returncode for a, p in processes.items()}
        receipt["physical_execution_publications"] = physical_publications
        receipt["final_roles"] = final_roles
        receipt["passed"] = receipt["passed"] and not cleanup_errors
        receipt["total_elapsed_seconds"] = time.monotonic() - started
        _write(output / "receipt.json", receipt)
    return receipt


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--role-config", type=Path)
    parser.add_argument("--deadline-seconds", type=float, default=1800)
    args = parser.parse_args(argv)
    if args.role_config is not None:
        require(args.output is None, "Role mode cannot create a controller")
        return _child(args.role_config)
    require(args.output is not None, "A fresh --output directory is required")
    result = run_project(args.output, ProjectConfig(deadline_seconds=args.deadline_seconds))
    print(json.dumps({"passed": result["passed"], "receipt": str(args.output.absolute() / "receipt.json"),
                      "counts": result.get("counts"), "cleanup_errors": result["cleanup_errors"]}))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
