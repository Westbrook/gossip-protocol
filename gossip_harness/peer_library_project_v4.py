"""Bounded twenty-role developmental M1 pilot with qualified fixture/live finance.

The controller schedules phases; gossip carries authenticated evidence between
sixteen builders and four reviewers. Live source proposals are model-written.
The offline fixture is an injected transport and is never a live fallback.
No credential loader or automatic live enrollment is provided by this module.
"""
from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import asdict, dataclass, replace
import hashlib
import itertools
import json
import math
import os
from pathlib import Path
import secrets
import shutil
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
from .peer_candidate_v2 import CandidatePublisher, named_sources, strictdecode_candidate_notice
from .peer_financial_authority_v2 import canonical_payload as _canonical_payload, ledger_identity
from .peer_financial_authority_v3 import (
    CumulativeAuthorityV3, FIXTURE_TRANSPORT, LIVE_TRANSPORT, PERMIT_PROTOCOL,
    REQUIRED_SOURCES, digest, profile_manifest,
)
from .peer_financial_rpc_v2 import FinancialClient, FinancialServer
from .peer_financial_rpc_v3 import FinancialRPCV3
from .peer_git_bundle_v1 import export_bundle, import_bundle
from .peer_library_contract_v4 import (
    BUILDERS, REVIEWERS, ROLES, MAX_OUTPUT_TOKENS, CapacityError,
    action_limits, builder_prompt, call_limit, contribution_capacity, exact_scopes, package_for,
    prospective_contract, request_capacity, reviewer_prompt, selector_prompt, work_key,
)
from .peer_mesh_finance_v2 import MeshFinancePayloads
from .peer_mesh_v2 import MeshConfig, MeshNode, observer_request
from .peer_project_contract_v2 import (
    CandidateOffer, Context, DispatchBinding, EvidenceRef, LocalViewManifest, ReleaseTarget,
    SelectionManifest, encode, from_dict, identity, resolve_local, strict_loads, to_dict,
    worker_request_digest,
)
from .peer_project_evidence_v3 import ContributionSlot, EvidenceRegistry
from .peer_project_execution_v3 import ExecutionPolicy, ProjectExecution, required_suite
from .peer_project_promotion_v3 import ProjectPromotion, PromotionResult
from .peer_project_repair_v4 import (
    TopicRepairPlan, UnchangedTopic, assert_current_plan, plan_topic_repairs,
    publish_repair_directive, repair_frontier, verify_repair_directive,
)
from .peer_project_views_v3 import materialization_directive, materialize_project, verify_materialized_view
from .peer_review_release_v2 import (
    ReviewPolicy, ReviewerProfile, ScopeRule, SLOTS, materialize_verdicts, policy_digest, suite_digest,
)
from .peer_role_loop_v2 import RoleLoop, WorkDirective, directive_id, financial_task_id
from .peer_store_v1 import strict_loads as bounded_loads
from .peer_twenty_role_fixture_v2 import SOURCES as V2_SOURCES
from .peer_twenty_role_fixture_v2 import _read, _write as _fixture_write, runtime_identity
from .worker import HTTPResponse, MODEL, STRONG_MODEL, OpenAIWorker, WorkerFailure, WorkerRequest, WorkerResult

PROTOCOL = "peer-library-project-v4"
COHORT = "library-m1-development-v4"
ROSTER = ROLES + ("seed", "finance")
PACKAGES = ("catalog", "ingestion", "query", "clients")
REPOSITORY = Path(__file__).resolve().parent.parent
SOURCES = tuple(sorted(set(REQUIRED_SOURCES) | {"gossip_harness/" + name for name in (
    *V2_SOURCES, "peer_library_project_v4.py", "peer_library_contract_v4.py", "peer_project_repair_v4.py",
    "peer_project_views_v3.py", "peer_project_evidence_v3.py", "peer_project_execution_v3.py",
    "peer_project_promotion_v3.py", "peer_review_release_v2.py", "peer_review_recovery_v2.py",
    "library_project_fixture_v1.py", "library_m1_acceptance_v4.py", "blackbox_validator.py", "sandbox.py",
    "library_m1_reference_v1.py", "library_m1_ingestion_reference_v1.py", "library_m1_clients_reference_v1.py",
    "peer_library_live_v4.py", "library_m1_browser_acceptance_v4.py", "peer_financial_rpc_v3.py",
)} | {"devtools/browser/library_m1_acceptance.cjs", "devtools/browser/lifecycle.cjs",
       "devtools/browser/package.json", "devtools/browser/package-lock.json",
       "pyproject.toml", "verification-manifest.json"}))
CLAIMS_EXCLUDED = ("statistical-superiority", "held-out-family", "four-milestone-study", "six-trajectory-study",
                   "decentralized-task-planning", "process-restart", "partition-recovery")


def require(condition: bool, reason: str) -> None:
    if not condition:
        raise PilotError(reason)


class PilotError(RuntimeError):
    """An integrity or infrastructure failure, distinct from project rejection."""


class PilotStop(RuntimeError):
    """A prospectively bounded terminal project observation."""
    def __init__(self, outcome: str, detail: str):
        self.outcome, self.detail = outcome, detail
        super().__init__(detail)


def _plain(value: Any) -> Any:
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    if isinstance(value, dict):
        return {key: _plain(item) for key, item in value.items()}
    return value


def canonical_payload(value: Any) -> bytes:
    return _canonical_payload(_plain(value))


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _write(path: Path, value: Any) -> None:
    _fixture_write(path, _plain(value))


@dataclass(frozen=True)
class ProjectConfig:
    deadline_seconds: int = 5400
    interval: float = 0.08
    executor_slots: int = 4
    worker_timeout: float = 120

    def __post_init__(self) -> None:
        require(type(self.deadline_seconds) is int and math.isfinite(self.deadline_seconds)
                and 240 <= self.deadline_seconds <= 7200 and int(self.deadline_seconds) == self.deadline_seconds, "Deadline outside bounded pilot policy")
        require(type(self.interval) in (int, float) and math.isfinite(self.interval)
                and 0.04 <= self.interval <= 0.25, "Mesh interval outside registered bounds")
        require(type(self.executor_slots) is int and self.executor_slots == 4, "Exactly four financial executors")
        require(type(self.worker_timeout) in (int, float) and self.worker_timeout == 120,
                "Worker timeout must match the prospective 120-second profile")


def fingerprints() -> dict[str, str]:
    return {name: sha((REPOSITORY / name).read_bytes()) for name in SOURCES}


def profile_specs(config: ProjectConfig) -> dict[str, dict[str, Any]]:
    return {name: profile_manifest(OpenAIWorker("offline-profile-only", model=model,
        max_output_tokens=MAX_OUTPUT_TOKENS, timeout=config.worker_timeout))
        for name, model in (("mini", MODEL), ("strong", STRONG_MODEL))}


def action_roster() -> tuple[tuple[str, str, int, bool], ...]:
    """Potential identities; mutually exclusive builder repair choices cap actual calls."""
    return (tuple((actor, "build", 0, False) for actor in BUILDERS)
        + tuple((actor, "repair", generation, False) for generation in (1, 2) for actor in BUILDERS)
        + tuple(("R1", "select_source", generation, correction)
                for generation in range(3) for correction in (False, True))
        + tuple((actor, "review", generation, correction)
                for generation in range(3) for actor in REVIEWERS for correction in (False, True)))


class ActionBudget:
    """Trusted prospective dispatch guard; never reset between source rounds."""
    def __init__(self) -> None:
        self.actions: list[tuple[str, str, int, bool]] = []

    def reserve(self, actor: str, kind: str, generation: int, correction: bool = False) -> None:
        require(type(actor) is str and type(kind) is str and type(generation) is int
                and type(correction) is bool, "Action identity types differ")
        item = actor, kind, generation, correction
        require(item in action_roster() and item not in self.actions, "Unknown or repeated prospective action")
        if correction:
            require((actor, kind, generation, False) in self.actions, "Correction needs its original complete response")
        if kind == "repair":
            require(not any(k == "repair" and g == generation and package_for(a) == package_for(actor)
                            for a, k, g, _ in self.actions), "One selected repair per package and generation")
        trial = [*self.actions, item]
        counts = Counter("builder" if k in ("build", "repair") else k for _, k, _, _ in trial)
        require(len(trial) <= 54 and counts["builder"] <= 24 and counts["select_source"] <= 6
                and counts["review"] <= 24, "Global action allowance exhausted")
        require(sum(a == actor for a, _, _, _ in trial) <= call_limit(actor), "Aggregate role allowance exhausted")
        self.actions.append(item)



def browser_runtime_identity() -> dict[str, Any]:
    from .library_m1_browser_acceptance_v4 import BrowserAcceptancePolicy, runtime_identity as browser_identity
    override = os.environ.get("GOSSIP_BROWSER_NODE")
    bundled = Path.home() / ".cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin/node"
    executable = Path(override) if override else bundled if bundled.is_file() else Path(shutil.which("node") or "")
    require(executable.is_absolute() and executable.is_file(), "Existing pinned browser Node runtime required")
    policy = BrowserAcceptancePolicy(RUNTIME_IMAGE, str(executable.resolve()))
    return {"node_executable": policy.node_executable, "timeout_seconds": policy.timeout_seconds,
            "node_modules_path": policy.node_modules_path, "runtime": browser_identity(policy)}


REQUIRED_TEST_CLASSES = (
    "tests/test_peer_financial_authority_v2.py::PeerFinancialAuthorityV2Tests",
    "tests/test_peer_financial_authority_v3.py::PeerFinancialAuthorityV3Tests",
    "tests/test_peer_financial_rpc_v2.py::PeerFinancialRPCV2Tests",
    "tests/test_peer_financial_rpc_v2.py::PeerFinancialRPCTCPV2Tests",
    "tests/test_peer_financial_rpc_v3.py::PeerFinancialRPCV3Tests",
    "tests/test_peer_financial_rpc_v3.py::PeerFinancialRPCTCPV3Tests",
    "tests/test_peer_role_loop_v2.py::PeerRoleLoopV2Tests",
    "tests/test_peer_library_contract_v4.py::PeerLibraryContractV4Tests",
    "tests/test_peer_project_repair_v4.py::PeerProjectRepairV4Tests",
    "tests/test_peer_library_project_v4.py::PeerLibraryProjectV4Tests",
    "tests/test_peer_library_project_v4.py::PeerLibraryProjectV4TCPTests",
    "tests/test_library_m1_acceptance_v4.py::LibraryM1AcceptanceV4Tests",
    "tests/test_library_m1_acceptance_v4.py::LibraryM1AcceptanceV4AuthoredSmokeTests",
    "tests/test_library_m1_browser_acceptance.py::LibraryM1BrowserAcceptanceTests",
    "tests/test_peer_library_live_v4.py::PeerLibraryLiveV4Tests",
    "tests/test_peer_project_views_v3.py::PeerProjectViewsV3Tests",
    "tests/test_peer_project_evidence_v3.py::ProjectEvidenceV3Tests",
    "tests/test_peer_project_execution_v3.py::PeerProjectExecutionV3Tests",
    "tests/test_peer_project_promotion_v3.py::PeerProjectPromotionV3Tests",
    "tests/test_peer_library_project_v3.py::PeerLibraryProjectV3Tests",
    "tests/test_peer_library_project_v3.py::PeerLibraryProjectV3GitTests",
)

def execution_design(config: ProjectConfig = ProjectConfig()) -> dict[str, Any]:
    """Stable across fixture/live transport, enrollment paths, secrets and ledgers."""
    return {"protocol": PROTOCOL, "purpose": "healthy-developmental-M1-twenty-role-pilot",
        "settings": asdict(config), "browser_runtime": browser_runtime_identity(),
        "required_test_classes": list(REQUIRED_TEST_CLASSES), "max_workers": config.executor_slots, "profiles": profile_specs(config),
        "sources": fingerprints(), "runtime": runtime_identity(), "image_id": RUNTIME_IMAGE,
        "seed_sha256": sha(canonical_payload(seed_files())),
        "public_cases_sha256": sha(canonical_payload(public_cases("m1"))),
        "prospective": prospective_contract(deadline_seconds=int(config.deadline_seconds)), "potential_actions": [list(item) for item in action_roster()],
        "action_limits": action_limits(),
        "rehearsal_requirements": {"protocol": PROTOCOL, "exact_counts": {"role_processes": 20,
            "builder_processes": 16, "reviewer_processes": 4, "api_calls": 0, "api_spend_micro_usd": 0,
            "injected_calls": 32, "private_candidate_bundles": 17,
            "source_repairs_attempted": 4, "source_repairs_effective": 1, "source_repairs_unchanged": 3},
            "minimum_counts": {"physical_public_evaluations": 2, "protected_releases": 1, "source_repairs": 4,
                "selector_format_corrections": 1, "review_format_corrections": 1, "independent_interface_cases_passed": 5,
                "independent_browser_observations_passed": 1}},
        "phase_policy": "blind16-physical-merged-source-fresh4review-outcome-topic-repair-at-most3generations",
        "format_policy": "one-known-complete-format-correction-per-selector-or-reviewer-per-generation",
        "controller": "trusted-fixed-phase-dispatcher", "repair_feedback": "public-only",
        "unchanged_repair": "authenticated-known-empty-failure-retains-exact-topic-no-approval-fresh-evaluation-and-reviews",
        "acceptance": "independent-private-after-whole-cohort-freeze-no-further-model-actions",
        "claims_excluded": list(CLAIMS_EXCLUDED)}


def execution_contract(config: ProjectConfig = ProjectConfig()) -> dict[str, Any]:
    design = execution_design(config)
    return {"protocol": PROTOCOL, "execution_design": design, "execution_design_sha256": digest(design),
            "sources": design["sources"], "runtime": design["runtime"]}


def attest(contract: dict[str, Any]) -> dict[str, Any]:
    actual = {"sources": fingerprints(), "runtime": runtime_identity()}
    require(actual == {"sources": contract["sources"], "runtime": contract["runtime"]},
            "Source or host runtime changed during pilot")
    return actual


def allowed(actor: str, kind: str) -> tuple[str, ...]:
    return (("selection.json",) if kind == "select_source" else ("review.json",)
            if kind == "review" else exact_scopes()[package_for(actor)])


def enrollment(output: Path, contract: dict[str, Any], ledger_path: Path, *, mode: str) -> dict[str, Any]:
    require(mode in ("fixture", "live"), "Explicit fixture or live enrollment required")
    contract_sha = digest(contract)
    context = Context(contract_sha, COHORT, "healthy-developmental-m1", 1,
                      sha(canonical_payload(public_cases("m1"))))
    specs = [{"context": to_dict(context), "work": to_dict(work_key(a, k, g, correction=c)),
        "actors": [a], "kinds": [k], "profiles": ["mini" if a in BUILDERS else "strong"],
        "allowed_paths": list(allowed(a, k)), "max_reserved_units": 8_000_000}
        for a, k, g, c in action_roster()]
    return {"cohort_id": COHORT, "execution_contract_sha256": contract_sha,
        "ledger_identity": ledger_identity(ledger_path), "journal_root": str(output / "financial-journal"),
        "transport_identity": FIXTURE_TRANSPORT if mode == "fixture" else LIVE_TRANSPORT, "task_specs": specs}


def fixture_permit(cohort: dict[str, Any], contract: dict[str, Any]) -> dict[str, Any]:
    design = contract["execution_design"]
    return {"protocol": PERMIT_PROTOCOL, "mode": "fixture", "approval_ref": "offline-no-provider-disposable-ledger",
        "execution_design": design, "execution_design_sha256": digest(design),
        "cohort_contract_sha256": digest(cohort), "sources": contract["sources"], "profiles": design["profiles"],
        "incremental_cap_micro_usd": 100_000_000, "expected_global_cap": 100_000_000,
        "expected_opening_usage": 0, "max_workers": 4, "qualification": None}


def sanitized_child_environment(environment: dict[str, str]) -> dict[str, str]:
    """Children have role capabilities only; no inherited provider credentials."""
    exact = {"OPENAI_API_KEY", "OPENAI_ORG_ID", "OPENAI_PROJECT_ID", "ANTHROPIC_API_KEY", "GOOGLE_API_KEY",
             "GEMINI_API_KEY", "AZURE_OPENAI_API_KEY", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"}
    return {key: value for key, value in environment.items()
            if key not in exact and not key.upper().endswith(("_API_KEY", "_ACCESS_TOKEN", "_SECRET_KEY"))}


class ProjectRoleLoopV4(RoleLoop):
    """Outcome-dependent policies registered by the trusted host before enqueue."""
    def __init__(self, *args: Any, allowed_policies: tuple[str, ...], **kwargs: Any):
        require(type(allowed_policies) is tuple and bool(allowed_policies)
                and len(set(allowed_policies)) == len(allowed_policies)
                and all(type(value) is str and len(value) == 64 and all(c in "0123456789abcdef" for c in value)
                        for value in allowed_policies), "Closed policy allowlist required")
        self.allowed_policies = allowed_policies
        self.action_policies: dict[str, str] = {}
        kwargs["policy_sha256"] = digest({"caller_policy": kwargs["policy_sha256"],
            "allowed_policies": list(allowed_policies), "materializer": sha(Path(__file__).read_bytes()),
            "views": sha(Path(__file__).with_name("peer_project_views_v3.py").read_bytes())})
        super().__init__(*args, **kwargs)
        try:
            self.policy_root = self.root / "policy-registrations"
            self.policy_root.mkdir(exist_ok=True)
            for row in self._records():
                path = self.policy_root / (row["id"] + ".json")
                require(path.is_file(), "Retained action lost policy registration")
                registration = _read(path)
                require(registration["directive_id"] == row["id"] and registration["policy_sha256"] in self.allowed_policies,
                        "Retained policy registration differs")
                self.action_policies[row["id"]] = registration["policy_sha256"]
        except BaseException:
            self.close()
            raise

    def register(self, directive: WorkDirective, policy: str) -> str:
        require(policy in self.allowed_policies, "Unregistered view policy")
        key = directive_id(directive)
        require(key not in self.action_policies or self.action_policies[key] == policy, "Action policy changed")
        path = self.policy_root / (key + ".json")
        registration = {"directive_id": key, "policy_sha256": policy}
        if path.exists():
            require(_read(path) == registration, "Durable action policy changed")
        else:
            _write(path, registration)
        self.action_policies[key] = policy
        return self.enqueue(directive)

    def _materialize(self, directive: WorkDirective) -> tuple[WorkerRequest, LocalViewManifest] | None:
        key = directive_id(directive)
        require(key in self.action_policies, "Action policy not registered before materialization")
        result = materialize_project(directive, self.mesh, self.action_policies[key])
        if result is not None:
            request, view = result
            request_capacity(request, profile_id=directive.profile_id, view_manifest_sha256=identity(view))
        return result


class EvidenceRegistryV4(EvidenceRegistry):
    """One prescribed corrected selector slot, preserving the complete frontier.

    The controller's verified_view additionally checks the correction against the
    exact original request and output. The inherited adapter authenticates every
    financial, Git and local-materialization binding for both slots.
    """
    def _terminal(self, binding: DispatchBinding, role_ref: EvidenceRef, slot: ContributionSlot) -> Any:
        if slot == self.selector and binding.action.work == work_key("R1", "select_source",
                self.selector.work.generation, correction=True):
            slot = replace(slot, work=binding.action.work)
        return super()._terminal(binding, role_ref, slot)


def _child(config_path: Path) -> int:
    raw, root = _read(config_path), config_path.parent
    node = MeshNode(MeshConfig.from_dict(raw["mesh"]))
    loop: ProjectRoleLoopV4 | None = None
    completed: set[str] = set()
    commands: dict[str, dict[str, Any]] = {}
    try:
        _write(root / "ready.json", {**node.start(), "attestation": attest(raw["contract"])})
        client = FinancialClient(raw["finance_port"], raw["actor"], raw["capability"], raw["contract_sha256"])
        loop = ProjectRoleLoopV4(root / "journal", raw["actor"], node, client,
            call_limit=call_limit(raw["actor"]), policy_sha256=raw["runtime_policy_sha256"],
            allowed_policies=tuple(raw["allowed_policies"]), result_producer="finance", lease_ttl=180)
        while not (root / "stop").exists():
            for path in sorted((root / "commands").glob("*.json")):
                if path.stem not in commands:
                    command = _read(path)
                    directive = WorkDirective.from_dict(command["directive"])
                    key = loop.register(directive, command["policy_sha256"])
                    require(path.stem == key, "Command filename differs from immutable directive")
                    commands[key] = command
            try:
                loop.tick()
            except CapacityError as error:
                _write(root / "capacity-stop.json", {"outcome": "context_capacity_exhausted", "detail": str(error)})
                break
            for snapshot in loop.snapshots():
                key = snapshot.directive_id
                if key in completed:
                    continue
                if snapshot.state not in ("published", "stopped"):
                    continue
                result: dict[str, Any] = {"actor": raw["actor"], "pid": os.getpid(), "directive_id": key,
                    "state": snapshot.state, "reason": snapshot.reason,
                    "reply": to_dict(snapshot.reply) if snapshot.reply else None,
                    "view": to_dict(snapshot.view) if snapshot.view else None,
                    "publication_ref": to_dict(snapshot.publication_ref) if snapshot.publication_ref else None,
                    "result_ref": to_dict(snapshot.result_ref) if snapshot.result_ref else None,
                    "candidate": None, "git_path": None, "finished_at": time.monotonic()}
                if snapshot.state == "published" and snapshot.reply is not None:
                    require(snapshot.action is not None and snapshot.reply.binding is not None, "Published action has no binding")
                    assert snapshot.action is not None and snapshot.reply.binding is not None
                    replay = client.submit(snapshot.action, snapshot.reply.binding.lease)
                    require(replay == snapshot.reply, "Exact terminal replay changed")
                    result["exact_terminal_replay"] = True
                    request = loop.worker_request(key)
                    result["request_sha256"] = worker_request_digest(request)
                    if snapshot.reply.state == "completed" and raw["actor"] in BUILDERS:
                        assert snapshot.result_ref is not None
                        body = strict_loads(resolve_local(snapshot.result_ref, node.arrived(), node.resolve(snapshot.result_ref)))
                        proposal = WorkerResult(**body["payload"])
                        try:
                            candidate_files = dict(request.files)
                            for changed_path, text in proposal.changes.items():
                                if text is None:
                                    candidate_files.pop(changed_path, None)
                                else:
                                    candidate_files[changed_path] = text
                            contribution_capacity(package_for(raw["actor"]), {p: t for p, t in candidate_files.items()
                                if p.startswith(PACKAGE_SCOPES[package_for(raw["actor"])][0])})
                            store_path = root / ("git-" + key)
                            command = commands[key]
                            if "base_bundle" in command:
                                info = command["base_bundle"]
                                ref = from_dict(EvidenceRef, info["ref"])
                                while node.resolve(ref) is None:
                                    require(time.monotonic() < raw["deadline_monotonic"], "Topic base arrival deadline")
                                    node.want(ref)
                                    time.sleep(0.03)
                                imported = import_bundle(resolve_local(ref, node.arrived(), node.resolve(ref)),
                                                         info["manifest"], root / ("base-import-" + key))
                                store = GitStore.create(store_path, seed_files())
                                old = store.head()
                                require(imported.is_ancestor(old, request.base_sha)
                                        and imported.read_files(request.base_sha) == request.files, "Topic base differs")
                                staged = store.prepare(imported, request.base_sha, old,
                                    lambda _p: (True, "Authenticated selected package topic ancestry"),
                                    allowed_paths=raw["package_scopes"][package_for(raw["actor"])])
                                require(staged.status in ("prepared", "noop") and staged.candidate_sha == request.base_sha
                                        and store.accept(staged).status in ("accepted", "noop"), "Topic adoption failed")
                            else:
                                store = GitStore.create(store_path, request.files)
                            require(store.head() == request.base_sha and store.read_files() == request.files,
                                    "Builder private tree differs from authenticated topic base")
                            publisher = CandidatePublisher(root / ("candidate-" + key), node, store, actor=raw["actor"],
                                baseline_sha=request.base_sha, package_scopes={p: tuple(v) for p, v in raw["package_scopes"].items()},
                                execution_contract_sha256=raw["contract_sha256"], completed_proof=loop.completed_proof)
                            assert snapshot.reply.result_payload_sha256 is not None
                            publication = publisher.publish(snapshot.reply.binding, request, proposal, snapshot.reply.result_payload_sha256)
                            result["candidate"] = {"offer": to_dict(publication.offer), "offer_ref": to_dict(publication.offer_ref)}
                            result["git_path"] = str(store_path)
                        except CapacityError as error:
                            result["candidate_error"] = {"outcome": "context_capacity_exhausted", "detail": str(error)}
                        except (ValueError, RuntimeError) as error:
                            result["candidate_error"] = {"outcome": "invalid_candidate", "detail": str(error)}
                _write(root / "results" / (key + ".json"), result)
                completed.add(key)
            time.sleep(0.04)
        _write(root / "final.json", {"actor": raw["actor"], "pid": os.getpid(), "journal_root": str(loop.root),
            "completed_actions": sorted(completed), "call_limit": call_limit(raw["actor"]),
            "snapshots": [asdict(item) for item in loop.snapshots()], "history": list(loop.history()),
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


class FrozenFrontier:
    """One immutable eligibility membership per generation, including late arrivals."""
    def __init__(self) -> None:
        self.generations: dict[int, tuple[str, ...]] = {}

    def seal(self, generation: int, offers: tuple[CandidateOffer, ...]) -> tuple[str, ...]:
        require(type(generation) is int and 0 <= generation <= 2 and type(offers) is tuple and bool(offers),
                "Invalid frontier seal")
        values = tuple(identity(offer) for offer in offers)
        require(len(set(values)) == len(values), "Duplicate frozen offer")
        if generation in self.generations:
            require(self.generations[generation] == values, "Sealed eligibility frontier cannot change")
        else:
            self.generations[generation] = values
        return values


class ScriptedTransport:
    """Explicit qualification fixture only, with no provider fallback."""
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.responses: dict[str, tuple[WorkDirective, dict[str, str], str]] = {}
        self.calls: list[dict[str, Any]] = []
        self.active = 0
        self.peak = 0

    def register(self, directive: WorkDirective, changes: dict[str, str], actor: str) -> None:
        task = financial_task_id(directive.context, directive.work)
        with self.lock:
            require(task not in self.responses, "Fixture response already registered")
            self.responses[task] = directive, dict(changes), actor

    def __call__(self, request: urllib.request.Request, timeout: float, maximum: int) -> HTTPResponse:
        require(isinstance(request.data, bytes), "Fixture request requires exact bytes")
        raw = request.data
        assert isinstance(raw, bytes)
        payload = json.loads(raw)
        task = json.loads(payload["input"])
        with self.lock:
            directive, changes, actor = self.responses[task["task_id"]]
            recipe = strict_loads(task["validation_feedback"])
            require(recipe["directive"] == directive.to_dict() and task["instructions"] == directive.instructions,
                    "Fixture request changed exact directive")
            require(all(item["task_id"] != task["task_id"] for item in self.calls), "Repeated fixture invocation")
            item = {"actor": actor, "kind": directive.kind, "generation": directive.work.generation,
                    "task_id": task["task_id"], "started_at": time.monotonic(), "finished_at": None,
                    "request_sha256": sha(raw), "request_bytes": len(raw)}
            self.calls.append(item)
            self.active += 1
            self.peak = max(self.peak, self.active)
        try:
            proposal = {"changes": [{"path": path, "content": text} for path, text in changes.items()],
                        "summary": "Authored offline qualification fixture; not a model-quality observation."}
            body = {"id": "resp_fixture_" + sha(raw)[:24], "model": payload["model"], "status": "completed",
                "service_tier": "default", "usage": {"input_tokens": 101, "output_tokens": 20, "total_tokens": 121},
                "output": [{"type": "message", "role": "assistant", "status": "completed", "content": [
                    {"type": "output_text", "text": json.dumps(proposal)}]}]}
            return HTTPResponse(200, {"X-Request-Id": "req_fixture_" + sha(raw)[:24]}, json.dumps(body).encode())
        finally:
            with self.lock:
                item["finished_at"] = time.monotonic()
                self.active -= 1

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return {"calls": [dict(item) for item in self.calls], "active": self.active, "peak": self.peak}


def _fixture_build(actor: str, *, repair: bool) -> dict[str, str]:
    # This import is reachable only from explicit fixture dispatch. Live callers
    # never receive reference source, expected edits or mandatory defect choices.
    from .library_m1_reference_v1 import package_changes
    return package_changes(package_for(actor), defective_blob_dedup=not repair and actor == "B01")


def _fixture_selection(frontier: dict[str, Any], frontier_sha: str) -> dict[str, Any]:
    selected = []
    for package in PACKAGES:
        entries = [item for item in frontier["offers"] if item["slot"]["package_id"] == package]
        require(bool(entries), "Fixture frontier omitted package")
        choice = min(entries, key=lambda item: item["actor"])
        selected.append({"package_id": package, "offer_sha256": choice["offer_sha256"]})
    return {"protocol": "peer-project-selection-v3", "eligibility_policy_sha256": frontier["eligibility_policy_sha256"],
        "frontier_sha256": frontier_sha, "eligible_offer_sha256s": [item["offer_sha256"] for item in frontier["offers"]],
        "selected": selected}


def _fixture_review(actor: str, target: ReleaseTarget, refs: tuple[EvidenceRef, ...],
                    requirements: tuple[str, ...], *, approve: bool) -> dict[str, Any]:
    return {"protocol": "peer-scoped-review-v2", "target_sha256": identity(target), "verdicts": [
        {"scope_id": scope, "covered_requirement_ids": list(requirements), "evidence_refs": [to_dict(ref) for ref in refs],
         "verdict": "approve" if approve else "request_changes", "rationale":
             "Authored fixture response after public merged evaluation; not independent model judgment."}
        for reviewer, scope in SLOTS if reviewer == actor]}


def repair_packages_from_gate(gate: Any, failed_checks: tuple[str, ...]) -> tuple[str, ...]:
    """Conservative public failures implicate the integration; scoped rejections add owners."""
    packages = set(PACKAGES if failed_checks else ())
    for _reviewer, package in (*gate.rejected_slots, *gate.missing_slots):
        packages.add(package)
    return tuple(package for package in PACKAGES if package in packages)


def accounting_snapshot(ledger_path: Path, cohort_id: str,
                        completions: dict[str, dict[str, Any]], mode: str) -> dict[str, Any]:
    """Observe this cohort only; do not reset or count earlier cumulative spend."""
    with sqlite3.connect("file:" + str(ledger_path) + "?mode=ro", uri=True) as db:
        db.row_factory = sqlite3.Row
        rows = [dict(row) for row in db.execute("""SELECT a.actor,a.request_id,a.reservation_id AS call_id,a.reservation_id,
            a.state AS action_state,r.state,r.amount,r.spent FROM financial_actions_v2 a
            LEFT JOIN reservations r ON r.id=a.reservation_id WHERE a.cohort=? ORDER BY a.reservation_id""", (cohort_id,))]
    require(all(row["state"] is not None for row in rows), "Financial action lost its reservation")
    supplied = [result["reply"]["binding"] for result in completions.values()
                if result.get("reply") and result["reply"].get("binding")]
    require(len({item["call_id"] for item in supplied}) == len(supplied), "Duplicate completion call binding")
    known = {result["reply"]["binding"]["call_id"]: result["reply"]["binding"]
        for result in completions.values() if result.get("reply") and result["reply"].get("binding")}
    bindings_exact = all(row["call_id"] in known and known[row["call_id"]]["reservation_id"] == row["reservation_id"]
                         for row in rows)
    require(len({row["call_id"] for row in rows}) == len(rows) and len(rows) <= 54, "Financial call membership differs")
    counts = Counter(row["actor"] for row in rows)
    require(all(count <= call_limit(actor) for actor, count in counts.items()), "Financial role cap exceeded")
    unsettled = [row["call_id"] for row in rows if row["state"] != "settled" or type(row["spent"]) is not int]
    return {"admitted_calls": len(rows), "settled_reservations": len(rows) - len(unsettled),
        "unsettled_reservations": len(unsettled), "unsettled_call_ids": unsettled,
        "complete_bindings_reconciled": bindings_exact and len(known) == len(rows),
        "per_actor_calls": dict(sorted(counts.items())), "calls": rows,
        "known_spend_micro_usd": sum(row["spent"] for row in rows if type(row["spent"]) is int),
        "api_spend": mode == "live"}


def _format_correction(original: WorkDirective, request: WorkerRequest, actor: str,
                       response: str, errors: tuple[str, ...]) -> tuple[WorkDirective, Any, Any]:
    from .peer_library_contract_v4 import format_correction_directive
    from .peer_review_recovery_v2 import ProviderOutcome, ValidationOutcome
    outcome = ProviderOutcome(status="complete", response=response)
    validation = ValidationOutcome(valid=False, errors=errors)
    return format_correction_directive(original, request, actor=actor, provider_outcome=outcome, validation=validation), outcome, validation


class ProjectRun:
    """One owned cohort; all dynamic state is persisted before dependent dispatch."""
    def __init__(self, output: Path, config: ProjectConfig, mode: str, workers: dict[str, OpenAIWorker] | None,
                 ledger_path: Path | None, permit: dict[str, Any] | None, expected_permit_sha256: str | None):
        self.output, self.config, self.mode = output, config, mode
        self.workers, self.ledger_path, self.permit, self.permit_pin = workers, ledger_path, permit, expected_permit_sha256
        self.pending_repair_plan: Any = None
        self.started = time.monotonic()
        self.deadline = self.started + config.deadline_seconds
        self.contract = execution_contract(config)
        self.contract_sha = digest(self.contract)
        self.design_sha = self.contract["execution_design_sha256"]
        self.context = Context(self.contract_sha, COHORT, "healthy-developmental-m1", 1,
                               sha(canonical_payload(public_cases("m1"))))
        self.processes: dict[str, subprocess.Popen[Any]] = {}
        self.streams: list[Any] = []
        self.nodes: list[MeshNode] = []
        self.registries: list[EvidenceRegistry] = []
        self.finance: Any = None
        self.payloads: Any = None
        self.execution: Any = None
        self.promotion: Any = None
        self.server: Any = None
        self.server_thread: Any = None
        self.transport = ScriptedTransport() if mode == "fixture" else None
        self.cleanup_errors: list[str] = []
        self.completions: dict[str, dict[str, Any]] = {}
        self.directives: dict[str, WorkDirective] = {}
        self.directive_policies: dict[str, str] = {}
        self.corrections: dict[str, tuple[WorkerRequest, str, Any, Any]] = {}
        self.repair_plans: dict[str, Any] = {}
        self.unchanged_repairs: dict[str, Any] = {}
        self.pending_unchanged_repairs: tuple[Any, ...] = ()
        self.action_budget, self.frontier_seal = ActionBudget(), FrozenFrontier()
        self.registry_for_action: dict[str, EvidenceRegistry] = {}
        self.registry_by_selection: dict[str, EvidenceRegistry] = {}
        self.registry_by_offer: dict[str, EvidenceRegistry] = {}
        self.offer_results: dict[str, dict[str, Any]] = {}
        self.stages: list[dict[str, Any]] = []
        self.physical_publications: list[dict[str, Any]] = []
        self.last_target: ReleaseTarget | None = None
        self.last_gate: Any = None
        self.last_review_results: list[dict[str, Any]] = []
        self.receipt: dict[str, Any] = {"protocol": PROTOCOL, "mode": mode, "passed": False,
            "attempted": True, "physically_executed": False, "execution_contract_sha256": self.contract_sha,
            "execution_design_sha256": self.design_sha, "claims_excluded": list(CLAIMS_EXCLUDED),
            "outcome": "not_started", "protected_release": False}

    def wait(self, predicate: Any) -> None:
        while not predicate():
            for actor, process in self.processes.items():
                capacity = self.output / "roles" / actor / "capacity-stop.json"
                if capacity.exists():
                    raise PilotStop("context_capacity_exhausted", _read(capacity)["detail"])
                if process.poll() is not None:
                    raise PilotError("Role process exited during cohort: " + actor)
            if time.monotonic() >= self.deadline:
                raise PilotStop("deadline_exhausted", "Declared whole-cohort deadline expired")
            time.sleep(0.08)

    def wait_refs(self, refs: tuple[EvidenceRef, ...]) -> None:
        def arrived() -> bool:
            result = True
            for ref in refs:
                self.seed.want(ref)
                result = ref in self.seed.arrived() and self.seed.resolve(ref) is not None and result
            return result
        self.wait(arrived)

    def start_financial_service(self, capabilities: dict[str, str]) -> None:
        """Join the actual qualified V3 owner to the unchanged bounded wire API.

        This is the production setup seam exercised by the localhost regression;
        V2's historical exact-offline-owner constructor remains unavailable here.
        """
        rpc = FinancialRPCV3(self.finance, capabilities,
            expected_financial_config_sha256=self.finance.config_sha256,
            request_guard=self.payloads.request_guard,
            request_guard_sha256=self.payloads.request_guard_sha256)
        self.server = FinancialServer(0, rpc)
        self.server_thread = threading.Thread(target=self.server.serve_forever,
            kwargs={"poll_interval": 0.05}, daemon=True)
        self.server_thread.start()

    def setup(self) -> None:
        for folder in ("dispatches", "corrections", "unchanged-repairs"):
            (self.output / folder).mkdir()
        _write(self.output / "execution-contract.json", self.contract)
        config, output = self.config, self.output
        transport_key, observer_key = secrets.token_hex(32), secrets.token_hex(32)
        capabilities = {actor: secrets.token_hex(32) for actor in ROLES}
        def mesh_config(actor: str, root: Path) -> MeshConfig:
            return MeshConfig(root, actor, COHORT, self.contract_sha, ROSTER, transport_key,
                              observer_key=observer_key, interval=config.interval, fanout=2)
        self.seed = MeshNode(mesh_config("seed", output / "seed-mesh"))
        self.nodes.append(self.seed)
        financial_mesh = MeshNode(mesh_config("finance", output / "finance-mesh"))
        self.nodes.append(financial_mesh)
        seed_ready, finance_ready = self.seed.start(), financial_mesh.start()
        self.files, self.scopes, self.cases = seed_files(), exact_scopes(), public_cases("m1")
        self.protected = GitStore.create(output / "protected-git", self.files)
        self.baseline = self.protected.head()
        self.suite = required_suite(self.context, self.cases)
        self.source_ref = self.seed.publish("project-source", canonical_payload({"files": self.files,
            "base_sha": self.baseline}), "seed-source")
        self.execution = ProjectExecution(output / "execution", self.seed, self.protected, producer="seed",
            execution_contract_sha256=self.contract_sha, cohort_id=COHORT,
            policy=ExecutionPolicy(RUNTIME_IMAGE, timeout_seconds=90, case_timeout_seconds=5, seed=0))
        if self.mode == "fixture":
            require(self.workers is None and self.ledger_path is None and self.permit is None and self.permit_pin is None,
                    "Fixture cannot enroll external workers, ledger or permit")
            self.workers = {name: OpenAIWorker("offline-no-provider", model=model, max_output_tokens=MAX_OUTPUT_TOKENS,
                timeout=config.worker_timeout, transport=self.transport)
                for name, model in (("mini", MODEL), ("strong", STRONG_MODEL))}
            self.ledger_path = output / "disposable-ledger.sqlite"
            Ledger(self.ledger_path, 100_000_000)
        assert self.ledger_path is not None and self.workers is not None
        cohort = enrollment(output, self.contract, self.ledger_path, mode=self.mode)
        if self.mode == "fixture":
            self.permit = fixture_permit(cohort, self.contract)
            self.permit_pin = digest(self.permit)
        assert self.permit is not None
        require(self.permit["execution_design"] == self.contract["execution_design"], "Permit design differs")
        self.payloads = MeshFinancePayloads(output / "financial-payloads", financial_mesh, ROLES)
        self.finance = CumulativeAuthorityV3.open(self.ledger_path, output / "financial-service", cohort,
            self.permit["incremental_cap_micro_usd"], self.permit["expected_global_cap"], self.permit["expected_opening_usage"],
            payloads=self.payloads, workers=self.workers, permit=self.permit, expected_permit_sha256=self.permit_pin,
            max_workers=config.executor_slots, mode=self.mode)
        self.profile_hashes = {key: digest(value) for key, value in self.finance.profiles.items()}
        self.eligibility = digest({"protocol": PROTOCOL, "eligibility": "complete-valid-initial-frontier-then-selected-topic-successors",
                                   "capacity": 48000, "execution_design_sha256": self.design_sha})
        self.requirements = tuple(dict.fromkeys(case.get("requirement", case["id"]) for case in self.cases))
        inherited = tuple(item for item in named_sources(self.files) if item.path not in {p for ps in self.scopes.values() for p in ps})
        self.policy = ReviewPolicy(self.context, tuple(ScopeRule(a, p, self.requirements) for a, p in SLOTS),
            tuple(ReviewerProfile(a, "strong", self.profile_hashes["strong"]) for a in REVIEWERS),
            self.requirements, self.requirements, self.requirements, self.eligibility, suite_digest(self.suite),
            self.execution.provenance_sha256, self.finance.config_sha256, tuple((p, 0) for p in PACKAGES), inherited)
        policy_allowlist = tuple(sorted({self.eligibility, *(policy_digest(replace(self.policy,
            package_generations=tuple(zip(PACKAGES, generations, strict=True))))
            for generations in itertools.product(range(3), repeat=4))}))
        self.start_financial_service(capabilities)
        for actor in ROLES:
            root = output / "roles" / actor
            (root / "commands").mkdir(parents=True)
            (root / "results").mkdir()
            mesh = mesh_config(actor, root / "mesh")
            mesh_raw = asdict(mesh)
            mesh_raw.update(root=str(mesh.root), roster=list(mesh.roster), brokers=list(mesh.brokers))
            path = root / "config.json"
            _write(path, {"actor": actor, "mesh": mesh_raw, "capability": capabilities[actor],
                "finance_port": self.server.server_address[1], "contract_sha256": self.contract_sha, "contract": self.contract,
                "runtime_policy_sha256": digest({"design": self.design_sha, "policies": list(policy_allowlist)}),
                "allowed_policies": list(policy_allowlist), "package_scopes": self.scopes, "deadline_monotonic": self.deadline})
            stream = (root / "process.log").open("xb")
            self.streams.append(stream)
            self.processes[actor] = subprocess.Popen([sys.executable, "-m", "gossip_harness.peer_library_project_v4",
                "--role-config", str(path)], stdout=stream, stderr=stream,
                env=sanitized_child_environment(dict(os.environ)), cwd=REPOSITORY)
        self.wait(lambda: all((output / "roles" / a / "ready.json").exists() for a in ROLES))
        self.ready = {a: _read(output / "roles" / a / "ready.json") for a in ROLES}
        require(len({item["pid"] for item in self.ready.values()}) == 20
                and all(self.ready[a]["pid"] == self.processes[a].pid for a in ROLES), "Twenty OS role identities differ")
        peers = {**{a: value["port"] for a, value in self.ready.items()}, "seed": seed_ready["port"], "finance": finance_ready["port"]}
        self.seed.configure(peers)
        financial_mesh.configure(peers)
        for actor in ROLES:
            observer_request(peers[actor], actor, observer_key, "configure", {"peers": peers})
        self.promotion = ProjectPromotion(output / "promotion", self.protected, repository_id="local-research-library",
            baseline_sha=self.baseline, policy=self.policy, initial_suite=self.suite, package_scopes=self.scopes,
            verified_selection=self.verified_selection, verified_contribution=self.verified_contribution,
            verified_terminal=self.finance.verified_terminal, verified_view=self.verified_view,
            verified_execution=self.execution.verified_execution)
        self.receipt["financial_config"] = self.finance.config
        self.receipt["financial_config_sha256"] = self.finance.config_sha256

    def make(self, actor: str, kind: str, generation: int, source: EvidenceRef,
             evidence: tuple[EvidenceRef, ...] = (), instructions: str | None = None) -> WorkDirective:
        template = instructions if instructions is not None else (builder_prompt(actor, repair=kind == "repair")
            if actor in BUILDERS else selector_prompt())
        return WorkDirective(self.context, work_key(actor, kind, generation), "mini" if actor in BUILDERS else "strong",
            kind, source, evidence, allowed(actor, kind), template)

    def dispatch(self, actor: str, directive: WorkDirective, policy: str, *, generation: int,
                 correction: bool = False, fixture_changes: dict[str, str] | None = None,
                 base_bundle: dict[str, Any] | None = None) -> str:
        if time.monotonic() >= self.deadline:
            raise PilotStop("deadline_exhausted", "No new action after declared deadline")
        materialized = materialize_project(directive, self.seed, policy)
        require(materialized is not None, "Controller lacks complete evidence before registering action")
        assert materialized is not None
        request, view = materialized
        try:
            capacity = request_capacity(request, profile_id=directive.profile_id, view_manifest_sha256=identity(view))
        except CapacityError as error:
            raise PilotStop("context_capacity_exhausted", str(error)) from error
        require(not self.frozen, "No model action after whole-cohort freeze")
        self.action_budget.reserve(actor, directive.kind, directive.work.generation, correction)
        key = directive_id(directive)
        require("dispatch-" + key not in self.directives, "Repeated directive identity")
        self.directives["dispatch-" + key] = directive
        self.directive_policies["dispatch-" + key] = policy
        command: dict[str, Any] = {"directive": directive.to_dict(), "policy_sha256": policy}
        if base_bundle is not None:
            command["base_bundle"] = base_bundle
        _write(self.output / "dispatches" / (key + ".json"), {"actor": actor, "source_round": generation,
            "correction": correction, "command": command, "capacity": asdict(capacity)})
        if self.transport is not None:
            require(fixture_changes is not None, "Fixture completion must be explicit")
            assert fixture_changes is not None
            self.transport.register(directive, fixture_changes, actor)
        else:
            require(fixture_changes is None, "Live dispatch cannot receive authored fixture source")
        _write(self.output / "roles" / actor / "commands" / (key + ".json"), command)
        return key

    def collect(self, actor: str, key: str) -> dict[str, Any]:
        path = self.output / "roles" / actor / "results" / (key + ".json")
        def arrived_or_bounded_stop() -> bool:
            if path.exists():
                return True
            self.check_admission_wait(actor, key)
            return False
        self.wait(arrived_or_bounded_stop)
        result = _read(path)
        require(result["actor"] == actor and result["pid"] == self.processes[actor].pid
                and result["directive_id"] == key, "Role completion identity differs")
        self.completions["dispatch-" + key] = result
        if result.get("publication_ref"):
            binding = from_dict(DispatchBinding, result["reply"]["binding"])
            view = from_dict(LocalViewManifest, result["view"])
            self.wait_refs((from_dict(EvidenceRef, result["publication_ref"]), binding.action.worker_payload_ref,
                from_dict(EvidenceRef, result["result_ref"]), view.source_ref, *view.evidence_refs))
        if result.get("candidate_error", {}).get("outcome") == "context_capacity_exhausted":
            raise PilotStop("context_capacity_exhausted", result["candidate_error"]["detail"])
        reply = result.get("reply") or {}
        if reply.get("state") == "unknown":
            raise PilotStop("unknown_provider_outcome", "Retained unknown outcome; no reinvocation")
        return result

    def check_admission_wait(self, actor: str, key: str) -> None:
        journal = self.output / "roles" / actor / "journal" / "role.sqlite"
        if not journal.exists() or self.ledger_path is None:
            return
        with sqlite3.connect(journal.as_uri() + "?mode=ro", uri=True) as db:
            row = db.execute("SELECT body FROM actions WHERE id=?", (key,)).fetchone()
        if row is None:
            return
        state = bounded_loads(bytes(row[0]), max_bytes=2_100_000)
        reply = state.get("reply") or {}
        reason = reply.get("reason")
        if reply.get("state") != "waiting":
            return
        if reason == "cohort_call_limit":
            raise PilotStop("call_limit_exhausted", "Prospective aggregate admission count exhausted")
        if reason == "cohort_halted":
            raise PilotStop("unknown_provider_outcome", "Financial cohort halted; no new calls")
        if reason not in ("slot_budget", "cohort_budget", "global_budget"):
            return
        with sqlite3.connect(self.ledger_path.as_uri() + "?mode=ro", uri=True) as db:
            db.execute("BEGIN")
            pending = db.execute("SELECT COUNT(*) FROM financial_actions_v2 WHERE cohort=? AND state IN "
                "('pending','publication_pending')", (COHORT,)).fetchone()[0]
            if pending:
                return  # Known settlements can release conservative reservation headroom.
            used = db.execute("SELECT COALESCE(SUM(COALESCE(r.spent,r.amount)),0) FROM reservations r "
                "JOIN financial_actions_v2 a ON a.reservation_id=r.id WHERE a.cohort=?", (COHORT,)).fetchone()[0]
            global_used = db.execute("SELECT COALESCE(SUM(COALESCE(spent,amount)),0) FROM reservations").fetchone()[0]
        directive = self.directives["dispatch-" + key]
        units = self.finance.workers[directive.profile_id].profile_manifest()["reservation_units"]
        assert self.permit is not None
        if reason == "slot_budget" or used + units > self.permit["incremental_cap_micro_usd"] \
                or global_used + units > self.permit["expected_global_cap"]:
            raise PilotStop("budget_exhausted", "Known settled usage leaves insufficient registered reservation headroom")

    def successful(self, result: dict[str, Any]) -> bool:
        return bool(result.get("reply") and result["reply"]["state"] == "completed")

    def pure_view(self, binding: DispatchBinding, request: WorkerRequest, claimed: LocalViewManifest) -> LocalViewManifest:
        expected = self.directives[binding.action.request_id]
        require(materialization_directive(request) == expected, "Actual request differs from trusted registered directive")
        view = verify_materialized_view(binding, request, claimed, self.seed,
            policy_sha256=self.directive_policies[binding.action.request_id],
            expected_instructions_sha256=sha(expected.instructions.encode()), expected_attempt_limit=expected.attempt_limit)
        if binding.action.request_id in self.repair_plans:
            verify_repair_directive(self.repair_plans[binding.action.request_id], expected, self.seed)
        if binding.action.request_id in self.corrections:
            from .peer_library_contract_v4 import verify_format_correction
            original, actor, outcome, validation = self.corrections[binding.action.request_id]
            verify_format_correction(original, request, actor=actor, provider_outcome=outcome, validation=validation)
        return view

    def verified_role_view(self, binding: DispatchBinding, proof: dict[str, Any]) -> LocalViewManifest:
        """Authenticate arrived role/result bytes without granting release authority."""
        require(proof["binding"] == binding, "Financial proof belongs to another dispatch")
        request_ref = binding.action.worker_payload_ref
        require(resolve_local(request_ref, self.seed.arrived(), self.seed.resolve(request_ref)) == canonical_payload({
            "worker_request": asdict(proof["worker_request"]),
            "view_manifest_sha256": binding.action.view_manifest_sha256}),
            "Arrived worker request differs from exact financial proof")
        result = self.completions[binding.action.request_id]
        require(result["reply"] == to_dict(proof["reply"]), "Role completion differs from exact financial proof")
        ref = from_dict(EvidenceRef, result["publication_ref"])
        body = strict_loads(resolve_local(ref, self.seed.arrived(), self.seed.resolve(ref)))
        require(ref.producer == binding.action.actor and ref.kind == "role-result"
                and body["protocol"] == "peer-role-loop-v2" and body["actor"] == binding.action.actor
                and body["action"] == to_dict(binding.action) and body["reply"] == to_dict(proof["reply"]),
                "Role publication differs from exact financial proof")
        result_ref = from_dict(EvidenceRef, body["result_ref"])
        require(result_ref.producer == "finance" and result_ref.kind == "financial-result"
                and resolve_local(result_ref, self.seed.arrived(), self.seed.resolve(result_ref)) == canonical_payload(proof["result_payload"]),
                "Role result bytes differ from financial proof")
        require(result["result_ref"] == to_dict(result_ref) and result["view"] == body["view_manifest"],
                "Role completion view or result reference differs from its publication")
        return self.pure_view(binding, proof["worker_request"], from_dict(LocalViewManifest, body["view_manifest"]))

    def verified_view(self, binding: DispatchBinding) -> LocalViewManifest:
        proof = self.finance.verified_terminal(binding)
        view = self.verified_role_view(binding, proof)
        self.registry_for_action[binding.action.request_id].verified_candidate_materials(view)
        return view

    def _unchanged_repair_proof(self, plan: TopicRepairPlan, actor: str,
                                result: dict[str, Any]) -> UnchangedTopic:
        """Recognize one accounted no-change outcome; never create a candidate."""
        assert_current_plan(plan, self.current_failure)
        require(not self.frozen, "No repair disposition after whole-cohort freeze")
        reply = result.get("reply") or {}
        if (reply.get("state") != "failed" or result.get("candidate") is not None
                or result.get("candidate_error") is not None):
            raise PilotStop("missing_repair_candidate", "Repair has neither a changed candidate nor a known unchanged proposal")
        binding = from_dict(DispatchBinding, reply["binding"])
        action = binding.action
        topic = plan.topic(package_for(actor))
        key = action.request_id
        directive = self.directives.get(key)
        require(directive is not None, "Unchanged repair lacks a registered directive")
        assert directive is not None
        require(topic.repair and topic.offer.dispatch.action.actor == actor and action.actor == actor
                and action.kind == "repair" and action.work == work_key(actor, "repair", topic.new_generation)
                and key == "dispatch-" + directive_id(directive)
                and self.repair_plans.get(key) == plan and self.completions.get(key) == result
                and result["actor"] == actor and result["pid"] == self.processes[actor].pid
                and result["directive_id"] == directive_id(directive) and result["state"] == "published"
                and result["exact_terminal_replay"] is True and result["git_path"] is None
                and result["request_sha256"] == binding.normalized_worker_request_sha256,
                "Unchanged repair differs from its exact registered current action")
        proof = self.finance.verified_known_failure(binding)
        failure = proof["result"]
        if (type(failure) is not WorkerFailure or str(failure) != "Proposal contains no effective changes"
                or failure.metadata.get("failure_kind") != "empty" or failure.metadata.get("halt", False) is not False
                or self.finance.failed_closed or type(failure.usage_units) is not int
                or proof["reply"].state != "failed" or proof["reply"].usage_units != failure.usage_units):
            raise PilotStop("missing_repair_candidate", "Only an exact known settled nonhalt empty proposal can retain its topic")
        self.verified_role_view(binding, proof)
        request = proof["worker_request"]
        require(request.files == dict(topic.base_files) and request.base_sha == topic.base_sha
                and self.verified_contribution(topic.offer) == named_sources({path: body for path, body in topic.base_files
                                                                            if path in topic.scope}),
                "Unchanged repair source differs from the authenticated selected topic")
        return UnchangedTopic(topic.package_id, plan.sha256, identity(topic.offer), binding,
                              sha(canonical_payload(proof["result_payload"])))

    def retain_unchanged_repair(self, plan: TopicRepairPlan, actor: str,
                                result: dict[str, Any]) -> UnchangedTopic:
        require(plan.failed_target.generation + 1 not in self.frontier_seal.generations,
                "No late repair disposition after successor frontier seal")
        disposition = self._unchanged_repair_proof(plan, actor, result)
        key = disposition.dispatch.action.request_id
        path = self.output / "unchanged-repairs" / (key + ".json")
        require(key not in self.unchanged_repairs and not path.exists(), "Unchanged disposition already sealed")
        _write(path, disposition.body())
        self.unchanged_repairs[key] = disposition
        return disposition

    def verified_unchanged_repair(self, disposition: UnchangedTopic) -> UnchangedTopic:
        """Admission-time proof; historical audits inspect retained immutable bytes."""
        key = disposition.dispatch.action.request_id
        require(self.unchanged_repairs.get(key) == disposition
                and _read(self.output / "unchanged-repairs" / (key + ".json")) == disposition.body(),
                "Unchanged disposition differs from its durable registration")
        actual = self._unchanged_repair_proof(self.repair_plans[key], disposition.dispatch.action.actor,
                                             self.completions[key])
        require(actual == disposition, "Unchanged disposition proof changed")
        return actual

    def successor_generations(self, generation: int,
                              offers: tuple[CandidateOffer, ...]) -> tuple[tuple[str, int], ...]:
        """Reconcile the next registry with changed and retained selected topics."""
        plan = self.pending_repair_plan
        require(plan is not None and generation == plan.failed_target.generation + 1,
                "Successor round differs from its current repair plan")
        # A prior repair retained in this round is still an old offer. Its kind
        # alone cannot identify a new replacement.
        replacements = tuple(offer for offer in offers if offer.dispatch.action.kind == "repair"
            and offer.dispatch.action.work.generation == generation
            and plan.topic(offer.dispatch.action.work.package_id).repair)
        frontier = repair_frontier(plan, replacements, current_failure=self.current_failure,
            unchanged=self.pending_unchanged_repairs, verified_unchanged=self.verified_unchanged_repair)
        require(len(offers) == len(frontier) and {identity(item) for item in offers} == {identity(item) for item in frontier},
                "Successor registry differs from exact repaired-topic frontier")
        return tuple((offer.dispatch.action.work.package_id, offer.dispatch.action.work.generation) for offer in frontier)

    def verified_selection(self, selection: SelectionManifest) -> SelectionManifest:
        return self.registry_by_selection[identity(selection)].verified_selection(selection)

    def verified_contribution(self, offer: CandidateOffer) -> Any:
        return self.registry_by_offer[identity(offer)].verified_contribution(offer)

    def register(self, registry: EvidenceRegistry, result: dict[str, Any]) -> CandidateOffer:
        ref = from_dict(EvidenceRef, result["candidate"]["offer_ref"])
        self.wait_refs((ref,))
        offer, _ = strictdecode_candidate_notice(resolve_local(ref, self.seed.arrived(), self.seed.resolve(ref)),
                                                  expected_contract_sha256=self.contract_sha)
        self.wait_refs((offer.bundle_ref,))
        offer = registry.register_offer(ref, from_dict(EvidenceRef, result["publication_ref"]))
        contribution_capacity(offer.dispatch.action.work.package_id, registry.candidate_files(offer))
        self.registry_by_offer[identity(offer)] = registry
        self.offer_results[identity(offer)] = result
        return offer

    def registry(self, generation: int, results: list[dict[str, Any]]) -> EvidenceRegistryV4:
        slots, bases, view_policies = [], {self.baseline: self.files}, {}
        for result in results:
            binding = from_dict(DispatchBinding, result["reply"]["binding"])
            proof = self.finance.verified_terminal(binding)
            request = proof["worker_request"]
            directive = self.directives[binding.action.request_id]
            bases[request.base_sha] = request.files
            slots.append(ContributionSlot(binding.action.actor, binding.action.work, binding.action.profile_id,
                self.profile_hashes[binding.action.profile_id], directive.allowed_paths, request.base_sha,
                sha(directive.instructions.encode()), directive.attempt_limit))
            view_policies[binding.action.kind, binding.action.work.generation] = self.eligibility
        view_policies["select_source", generation] = self.eligibility
        selector = ContributionSlot("R1", work_key("R1", "select_source", generation), "strong",
            self.profile_hashes["strong"], ("selection.json",), self.baseline, sha(selector_prompt().encode()))
        registry = EvidenceRegistryV4(self.output / ("registry-" + str(generation)), self.seed, context=self.context,
            authority_config_sha256=self.finance.config_sha256,
            package_scopes={p: tuple(root.rstrip("/") for root in roots) for p, roots in PACKAGE_SCOPES.items()},
            slots=tuple(slots), selector=selector, bases=bases, eligibility_policy_sha256=self.eligibility,
            view_policies=view_policies, verified_terminal=self.finance.verified_terminal, verified_view=self.pure_view)
        self.registries.append(registry)
        return registry

    def correction(self, actor: str, directive: WorkDirective, result: dict[str, Any], error: Exception,
                   policy: str, generation: int, fixture_changes: dict[str, str] | None) -> tuple[str, WorkDirective]:
        binding = from_dict(DispatchBinding, result["reply"]["binding"])
        proof = self.finance.verified_terminal(binding)
        response = proof["result"].changes.get("selection.json" if directive.kind == "select_source" else "review.json")
        if type(response) is not str:
            raise PilotStop("format_exhausted", "No complete artifact response available for bounded correction")
        errors = (str(error)[:2048] or type(error).__name__,)
        corrected, outcome, validation = _format_correction(directive, proof["worker_request"], actor, response, errors)
        corrected_id = "dispatch-" + directive_id(corrected)
        require(corrected_id not in self.corrections, "Correction allowance cannot reset")
        self.corrections[corrected_id] = proof["worker_request"], actor, outcome, validation
        _write(self.output / "corrections" / (directive_id(corrected) + ".json"), {
            "original_request_sha256": worker_request_digest(proof["worker_request"]),
            "original_result_payload_sha256": proof["reply"].result_payload_sha256,
            "original_directive": directive.to_dict(), "corrected_directive": corrected.to_dict(),
            "provider_outcome": asdict(outcome), "validation": asdict(validation)})
        key = self.dispatch(actor, corrected, policy, generation=generation, correction=True, fixture_changes=fixture_changes)
        return key, corrected

    def select(self, generation: int, registry: EvidenceRegistryV4,
               offers: tuple[CandidateOffer, ...]) -> SelectionManifest:
        values = self.frontier_seal.seal(generation, offers)
        _write(self.output / ("frontier-seal-" + str(generation) + ".json"), {
            "generation": generation, "eligible_offer_sha256s": list(values), "late_offers": "never-admitted"})
        frontier_ref = registry.frontier()
        frontier = strict_loads(resolve_local(frontier_ref, self.seed.arrived(), self.seed.resolve(frontier_ref)))
        materials = tuple(ref for offer in offers for ref in registry.normalized_refs(offer))
        directive = self.make("R1", "select_source", generation, self.source_ref, (frontier_ref, *materials))
        fixture = {"selection.json": json.dumps(_fixture_selection(frontier, frontier_ref.payload_sha256))} if self.transport else None
        initial_fixture = {"selection.json": "{invalid selector JSON"} if self.transport and generation == 0 else fixture
        key = self.dispatch("R1", directive, self.eligibility, generation=generation, fixture_changes=initial_fixture)
        result = self.collect("R1", key)
        if not self.successful(result):
            raise PilotStop("selector_provider_failure", "Selector did not return a known complete artifact")
        binding = from_dict(DispatchBinding, result["reply"]["binding"])
        self.registry_for_action[binding.action.request_id] = registry
        try:
            selection = registry.materialize_selection(binding, from_dict(EvidenceRef, result["publication_ref"]))
        except (ValueError, KeyError) as error:
            key, _ = self.correction("R1", directive, result, error, self.eligibility, generation, fixture)
            result = self.collect("R1", key)
            if not self.successful(result):
                raise PilotStop("selector_provider_failure", "Selector correction did not return a complete artifact")
            binding = from_dict(DispatchBinding, result["reply"]["binding"])
            self.registry_for_action[binding.action.request_id] = registry
            try:
                selection = registry.materialize_selection(binding, from_dict(EvidenceRef, result["publication_ref"]))
            except (ValueError, KeyError) as final_error:
                raise PilotStop("format_exhausted", "Selector correction malformed: " + str(final_error)) from final_error
        self.registry_by_selection[identity(selection)] = registry
        return selection

    def review(self, generation: int, registry: EvidenceRegistryV4, offers: tuple[CandidateOffer, ...],
               selection: SelectionManifest, target: ReleaseTarget, publication: Any,
               package_generations: tuple[tuple[str, int], ...]) -> tuple[Any, list[Any]]:
        target_ref = self.seed.publish("release-target", encode(target), "target-" + identity(target))
        target_source = self.seed.publish("project-source", canonical_payload({"files": self.protected.read_files(target.commit_oid),
            "base_sha": target.commit_oid}), "source-" + identity(target))
        selected_ids = {item.offer_sha256 for item in selection.selected}
        essential_refs = target_ref, registry.selection_ref(selection), publication.receipt_ref
        evidence = (target_source, *essential_refs[1:], *tuple(ref for offer in offers if identity(offer) in selected_ids
                                                             for ref in registry.normalized_refs(offer)))
        policy = policy_digest(replace(self.policy, package_generations=package_generations))
        directives = {actor: self.make(actor, "review", generation, target_ref, evidence,
            reviewer_prompt(actor, target_sha256=identity(target), requirement_ids=self.requirements,
                            evidence_refs=essential_refs)) for actor in REVIEWERS}
        self.promotion.begin_review_round(target, round_number=0,
            requests=tuple((actor, "dispatch-" + directive_id(directives[actor])) for actor in REVIEWERS))
        all_pass = all(status == "passed" for _, status in publication.check_results)
        fixtures = {actor: {"review.json": json.dumps(_fixture_review(actor, target, essential_refs,
            self.requirements, approve=all_pass))} for actor in REVIEWERS} if self.transport else {}
        keys = {actor: self.dispatch(actor, directives[actor], policy, generation=generation,
            fixture_changes=({"review.json": "{invalid reviewer JSON"} if actor == "R2" and generation == 0
                             else fixtures[actor]) if self.transport else None) for actor in REVIEWERS}
        originals = {actor: self.collect(actor, keys[actor]) for actor in REVIEWERS}
        valid_bindings, corrections = [], {}
        review_results = []
        for actor in REVIEWERS:
            result = originals[actor]
            review_results.append(result)
            if not self.successful(result):
                raise PilotStop("review_provider_failure", "Reviewer did not return a known complete artifact")
            binding = from_dict(DispatchBinding, result["reply"]["binding"])
            self.registry_for_action[binding.action.request_id] = registry
            try:
                verdicts = materialize_verdicts(binding, target, self.finance.verified_terminal)
                require({item.scope_id for item in verdicts} == {scope for reviewer, scope in SLOTS if reviewer == actor},
                        "Reviewer omitted an assigned scope")
                valid_bindings.append(binding)
            except (ValueError, KeyError, PilotError) as error:
                proof = self.finance.verified_terminal(binding)
                response = proof["result"].changes.get("review.json")
                if type(response) is not str:
                    raise PilotStop("format_exhausted", "Reviewer has no complete review artifact") from error
                corrected, outcome, validation = _format_correction(directives[actor], proof["worker_request"], actor,
                    response, (str(error)[:2048] or type(error).__name__,))
                corrections[actor] = corrected, proof["worker_request"], outcome, validation
        if valid_bindings:
            self.promotion.record_reviews(target, tuple(valid_bindings))
        if corrections:
            # Only malformed reviewers get a new registered request. Good peers'
            # genuine approvals/rejections retain their original charged calls.
            self.promotion.begin_review_round(target, round_number=1,
                requests=tuple((actor, "dispatch-" + directive_id(item[0])) for actor, item in corrections.items()))
            corrected_keys = {}
            for actor, (directive, original_request, outcome, validation) in corrections.items():
                request_id = "dispatch-" + directive_id(directive)
                require(request_id not in self.corrections, "Reviewer correction allowance cannot reset")
                self.corrections[request_id] = original_request, actor, outcome, validation
                _write(self.output / "corrections" / (directive_id(directive) + ".json"), {
                    "original_request_sha256": worker_request_digest(original_request),
                    "corrected_directive": directive.to_dict(), "provider_outcome": asdict(outcome),
                    "validation": asdict(validation)})
                corrected_keys[actor] = self.dispatch(actor, directive, policy, generation=generation,
                    correction=True, fixture_changes=fixtures.get(actor))
            for actor, key in corrected_keys.items():
                result = self.collect(actor, key)
                review_results.append(result)
                if not self.successful(result):
                    raise PilotStop("review_provider_failure", "Reviewer correction returned no complete artifact")
                binding = from_dict(DispatchBinding, result["reply"]["binding"])
                self.registry_for_action[binding.action.request_id] = registry
                try:
                    verdicts = materialize_verdicts(binding, target, self.finance.verified_terminal)
                    require({item.scope_id for item in verdicts} == {scope for reviewer, scope in SLOTS if reviewer == actor},
                            "Corrected reviewer omitted scope")
                    self.promotion.record_reviews(target, (binding,))
                except (ValueError, KeyError, PilotError) as error:
                    raise PilotStop("format_exhausted", "Reviewer correction malformed: " + str(error)) from error
        self.last_review_results = review_results
        current = self.promotion.reviews.get(identity(target), {})
        current_verdicts = [verdict for actor in sorted(current) for verdict in current[actor]]
        if time.monotonic() >= self.deadline:
            raise PilotStop("deadline_exhausted", "Deadline expired before the protected release gate")
        return self.promotion.promote(target), current_verdicts

    def current_failure(self) -> tuple[ReleaseTarget, Any]:
        require(self.last_target is not None and self.last_gate is not None, "No current failed public target")
        assert self.last_target is not None
        return self.last_target, self.last_gate

    def verified_feedback(self, target: ReleaseTarget, gate: Any) -> tuple[EvidenceRef, ...]:
        require((target, gate) == self.current_failure(), "Public repair feedback is stale")
        require(gate.target_sha256 == identity(target) and not gate.eligible, "Public repair gate differs")
        self.execution.verified_execution(target, self.suite)
        refs = list(target.execution_receipt_refs)
        for result in self.last_review_results:
            binding = from_dict(DispatchBinding, result["reply"]["binding"])
            self.verified_view(binding)
            refs.extend((from_dict(EvidenceRef, result["publication_ref"]), from_dict(EvidenceRef, result["result_ref"])))
        # Even malformed responses remain complete public evidence; the gate's
        # current verdicts separately identify the valid latest scoped decisions.
        return tuple(refs)

    def work(self) -> None:
        initial_keys = {actor: self.dispatch(actor, self.make(actor, "build", 0, self.source_ref), self.eligibility,
            generation=0, fixture_changes=_fixture_build(actor, repair=False) if self.transport else None) for actor in BUILDERS}
        results = [self.collect(actor, initial_keys[actor]) for actor in BUILDERS]
        eligible = [item for item in results if self.successful(item) and item.get("candidate") is not None]
        excluded = [{"actor": item["actor"], "reply": item.get("reply"), "candidate_error": item.get("candidate_error")}
                    for item in results if item not in eligible]
        _write(self.output / "initial-eligibility.json", {"eligible_actors": [item["actor"] for item in eligible], "excluded": excluded})
        if {package_for(item["actor"]) for item in eligible} != set(PACKAGES):
            raise PilotStop("missing_candidate_package", "A package has no complete eligible source proposal")
        generations = tuple((package, 0) for package in PACKAGES)
        for generation in range(3):
            registry = self.registry(generation, eligible)
            offers = tuple(self.register(registry, result) for result in eligible)
            if generation and self.pending_repair_plan is not None:
                generations = self.successor_generations(generation, offers)
            selection = self.select(generation, registry, offers)
            stores = {identity(offer): GitStore(registry.root / ("quarantine-" + identity(offer)) / "quarantine.git") for offer in offers}
            try:
                staged = self.promotion.stage(selection, offers, stores, generation=generation,
                                               package_generations=generations, integration_order=PACKAGES)
            except ValueError as error:
                raise PilotStop("integration_rejected", "Selected Git topics could not stage: " + str(error)) from error
            if time.monotonic() >= self.deadline:
                raise PilotStop("deadline_exhausted", "Deadline expired before fresh merged-source evaluation")
            publication = self.execution.execute("m1-public-generation-" + str(generation),
                self.execution.subject(self.context, staged.commit_oid, self.suite), self.suite, self.cases)
            self.physical_publications.append(asdict(publication))
            self.receipt["physically_executed"] = any(item["physically_executed"] for item in self.physical_publications)
            require(publication.physically_executed and not publication.replayed, "Public evaluation must physically execute once")
            failed = tuple(check for check, status in publication.check_results if status != "passed")
            if any(status == "infrastructure_failure" for _, status in publication.check_results):
                raise PilotStop("evaluation_infrastructure_failure", "Public evaluator infrastructure failed")
            target = self.promotion.bind_target(staged, evaluator_sha256=self.execution.evaluator_sha256,
                                               execution_receipt_refs=(publication.receipt_ref,))
            self.last_target = target
            self.last_gate = None
            outcome, verdicts = self.review(generation, registry, offers, selection, target, publication, generations)
            accepted = isinstance(outcome, PromotionResult) and outcome.status == "accepted"
            record = {"generation": generation, "selection": to_dict(selection), "target": to_dict(target),
                "execution": asdict(publication), "verdicts": [to_dict(item) for item in verdicts], "outcome": asdict(outcome)}
            _write(self.output / ("round-" + str(generation) + ".json"), record)
            self.stages.append({"generation": generation, "commit_oid": target.commit_oid, "failed_checks": list(failed),
                "scoped_decisions": len(verdicts), "promoted": accepted, "package_generations": generations})
            if accepted:
                require(self.protected.head() == target.commit_oid, "Promoted head differs from exact target")
                self.receipt.update(outcome="protected_release_accepted", protected_release=True)
                return
            self.last_gate = outcome
            if generation == 2:
                raise PilotStop("source_generations_exhausted", "Three source generations ended without an eligible release")
            repair_packages = repair_packages_from_gate(outcome, failed)
            if not repair_packages:
                raise PilotStop("release_gate_rejected", "No source repair scope follows from the current public evidence")
            feedback_refs = self.verified_feedback(target, outcome)
            plan = plan_topic_repairs(target_store=self.protected, baseline_sha=self.baseline, selection=selection,
                offers=offers, stores_by_offer=stores, package_scopes=self.scopes, repair_packages=repair_packages,
                feedback_refs=feedback_refs, current_failure=self.current_failure, verified_selection=self.verified_selection,
                verified_contribution=self.verified_contribution, verified_feedback=self.verified_feedback)
            _write(self.output / ("topic-repair-plan-" + str(generation + 1) + ".json"), plan.body())
            selected = {part.package_id: next(offer for offer in offers if identity(offer) == part.offer_sha256)
                        for part in selection.selected}
            keys = {}
            for package in repair_packages:
                topic = plan.topic(package)
                actor = topic.offer.dispatch.action.actor
                directive = publish_repair_directive(plan, package, self.seed,
                    work=work_key(actor, "repair", topic.new_generation), profile_id="mini",
                    instructions=builder_prompt(actor, repair=True), current_failure=self.current_failure)
                self.repair_plans["dispatch-" + directive_id(directive)] = plan
                store = stores[identity(topic.offer)]
                bundle, manifest = export_bundle(store, topic.base_sha, self.baseline)
                ref = self.seed.publish("project-base-bundle", bundle, "topic-base-" + identity(topic.offer))
                keys[package] = self.dispatch(actor, directive, self.eligibility, generation=generation + 1,
                    fixture_changes=_fixture_build(actor, repair=True) if self.transport else None,
                    base_bundle={"ref": to_dict(ref), "manifest": manifest})
            replacements, unchanged = {}, []
            for package, key in keys.items():
                actor = plan.topic(package).offer.dispatch.action.actor
                result = self.collect(actor, key)
                if self.successful(result) and result.get("candidate") is not None:
                    replacements[package] = result
                else:
                    unchanged.append(self.retain_unchanged_repair(plan, actor, result))
            eligible = [replacements[package] if package in replacements else self.offer_results[identity(selected[package])]
                        for package in PACKAGES]
            self.pending_repair_plan = plan
            self.pending_unchanged_repairs = tuple(unchanged)

    def cleanup(self) -> None:
        for actor in self.processes:
            try:
                (self.output / "roles" / actor / "stop").touch(exist_ok=False)
            except BaseException as error:
                self.cleanup_errors.append("Stop marker " + actor + ": " + repr(error))
        stop_deadline = time.monotonic() + 20
        for actor, process in self.processes.items():
            try:
                process.wait(timeout=max(0.01, stop_deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                try:
                    process.terminate()
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=3)
                except BaseException as error:
                    self.cleanup_errors.append("Terminate " + actor + ": " + repr(error))
                self.cleanup_errors.append("Forced owned process cleanup: " + actor)
            except BaseException as error:
                self.cleanup_errors.append("Process wait " + actor + ": " + repr(error))
            if process.returncode != 0:
                self.cleanup_errors.append(f"Role {actor} exited {process.returncode}")
        for stream in self.streams:
            try:
                stream.close()
            except BaseException as error:
                self.cleanup_errors.append("Log close: " + repr(error))
        if self.server is not None:
            try:
                if self.server_thread is not None and self.server_thread.is_alive():
                    self.server.shutdown()
                self.server.server_close()
                if self.server_thread is not None and self.server_thread.ident is not None:
                    self.server_thread.join(3)
                    require(not self.server_thread.is_alive(), "Financial RPC thread did not stop")
            except BaseException as error:
                self.cleanup_errors.append("Financial RPC cleanup: " + repr(error))
        for resource in (self.promotion, self.execution, *reversed(self.registries), self.finance, self.payloads,
                         *reversed(self.nodes)):
            if resource is not None:
                try:
                    resource.close()
                except BaseException as error:
                    self.cleanup_errors.append(type(resource).__name__ + ": " + repr(error))
        final_roles = []
        for actor, process in self.processes.items():
            try:
                path = self.output / "roles" / actor / "final.json"
                final = _read(path)
                expected = sorted(key.removeprefix("dispatch-") for key, directive in self.directives.items()
                                  if directive.work.slot_id.startswith(actor + "-"))
                require(final["actor"] == actor and final["pid"] == process.pid
                        and final["journal_root"] == str(self.output / "roles" / actor / "journal")
                        and final["call_limit"] == call_limit(actor)
                        and final["attestation"] == {"sources": self.contract["sources"], "runtime": self.contract["runtime"]}
                        and final["mesh_summary"]["fatal_error"] is None, "Final role identity or attestation differs")
                final_roles.append({"actor": actor, "pid": process.pid, "journal_root": final["journal_root"],
                    "completed_actions": final["completed_actions"], "all_dispatched_terminal": final["completed_actions"] == expected,
                    "receipt": str(path), "sha256": sha(path.read_bytes())})
            except BaseException as error:
                self.cleanup_errors.append("Final role " + actor + ": " + repr(error))
        self.receipt.update(final_roles=final_roles, owned_process_exit_codes={a: p.returncode for a, p in self.processes.items()},
            cleanup_errors=self.cleanup_errors, stages=self.stages, physical_execution_publications=self.physical_publications,
            unchanged_repairs=[item.body() for item in self.unchanged_repairs.values()],
            dispatched_actions=[{"actor": a, "kind": k, "generation": g, "correction": c}
                                for a, k, g, c in self.action_budget.actions],
            transport=self.transport.snapshot() if self.transport is not None else {"kind": "live-provider-journals"})

    def freeze_and_accept(self) -> None:
        """Irreversible local phase barrier: no model work is resumed afterwards."""
        from .library_m1_acceptance_v4 import AcceptancePolicy, FrozenSubject, run_once, source_sha256
        self.frozen = True
        assert self.ledger_path is not None
        ledger_path = self.ledger_path
        accounting = accounting_snapshot(self.ledger_path, COHORT, self.completions, self.mode)
        self.receipt["accounting"] = accounting
        terminal = (not self.cleanup_errors and len(self.receipt["final_roles"]) == 20
            and all(item["all_dispatched_terminal"] for item in self.receipt["final_roles"])
            and accounting["unsettled_reservations"] == 0 and accounting["complete_bindings_reconciled"])
        if not terminal:
            self.receipt["independent_acceptance"] = {"status": "not_run", "reason": "whole-cohort terminal barrier unproven"}
            return
        commit = self.last_target.commit_oid if self.last_target is not None else self.baseline
        files = self.protected.read_files(commit)
        tree = self.protected._git("rev-parse", commit + "^{tree}")
        subject = FrozenSubject(COHORT, self.context.trajectory_id, self.contract_sha, commit, tree, source_sha256(files))
        freeze = {"protocol": "peer-library-project-freeze-v4", "execution_contract_sha256": self.contract_sha,
            "execution_design_sha256": self.design_sha, "subject": asdict(subject),
            "all_dispatched_terminal": True, "accounting": accounting,
            "final_roles": self.receipt["final_roles"], "protected_release": self.receipt["protected_release"],
            "public_outcome": self.receipt["outcome"], "no_further_model_actions": True,
            "dispatched_actions": self.receipt["dispatched_actions"],
            "financial_config_sha256": self.finance.config_sha256,
            "unchanged_repair_artifacts": [{"path": str(self.output / "unchanged-repairs" / (key + ".json")),
                "sha256": sha((self.output / "unchanged-repairs" / (key + ".json")).read_bytes())}
                for key in self.unchanged_repairs],
            "completion_artifacts": [{"path": str(self.output / "roles" / result["actor"] / "results" / (result["directive_id"] + ".json")),
                "sha256": sha((self.output / "roles" / result["actor"] / "results" / (result["directive_id"] + ".json")).read_bytes())}
                for result in self.completions.values()]}
        path = self.output / "cohort-freeze.json"
        _write(path, freeze)
        frozen_bytes = path.read_bytes()
        frozen_sha = sha(frozen_bytes)
        def verify_freeze(actual: Any, raw: bytes) -> bool:
            return bool(self.frozen and actual == subject and raw == frozen_bytes and path.read_bytes() == frozen_bytes
                and sha(raw) == frozen_sha and all(process.poll() == 0 for process in self.processes.values())
                and all(sha(Path(item["receipt"]).read_bytes()) == item["sha256"] for item in freeze["final_roles"])
                and all(sha(Path(item["path"]).read_bytes()) == item["sha256"] for item in freeze["completion_artifacts"])
                and all(sha(Path(item["path"]).read_bytes()) == item["sha256"] for item in freeze["unchanged_repair_artifacts"])
                and self.protected.read_files(commit) == files and self.protected._git("rev-parse", commit + "^{tree}") == tree
                and accounting_snapshot(ledger_path, COHORT, self.completions, self.mode) == accounting)
        self.receipt["freeze"] = {"path": str(path), "sha256": frozen_sha, "subject": asdict(subject)}
        self.receipt["independent_acceptance"] = run_once(self.output / "independent-interface-acceptance", files,
            subject, frozen_bytes, verify_freeze, AcceptancePolicy(RUNTIME_IMAGE, timeout_seconds=120, case_timeout_seconds=30))
        from .library_m1_browser_acceptance_v4 import BrowserAcceptancePolicy, run_once as run_browser_once
        runtime = self.contract["execution_design"]["browser_runtime"]
        require(browser_runtime_identity() == runtime, "Pinned browser runtime changed after qualification")
        self.receipt["independent_browser_acceptance"] = run_browser_once(
            self.output / "independent-browser-acceptance", files, subject, frozen_bytes, verify_freeze,
            BrowserAcceptancePolicy(RUNTIME_IMAGE, runtime["node_executable"],
                timeout_seconds=runtime["timeout_seconds"], node_modules_path=runtime["node_modules_path"]))
        self.receipt["final_commit"] = commit
        self.receipt["final_source_sha256"] = subject.source_sha256

    def run(self) -> dict[str, Any]:
        self.frozen = False
        self.pending_repair_plan = None
        try:
            self.setup()
            self.work()
            attest(self.contract)
            self.receipt["passed"] = True
        except PilotStop as stop:
            self.receipt.update(outcome=stop.outcome, terminal_detail=stop.detail, passed=True)
        except BaseException:
            self.receipt.update(outcome="harness_failure", failure=traceback.format_exc(), passed=False)
        finally:
            self.cleanup()
        if self.finance is not None and self.ledger_path is not None:
            try:
                self.freeze_and_accept()
            except BaseException:
                self.receipt.update(acceptance_failure=traceback.format_exc(), passed=False)
                self.receipt.setdefault("independent_acceptance", {"status": "not_run"})
                self.receipt.setdefault("independent_browser_acceptance", {"status": "not_run"})
        accounting = self.receipt.get("accounting", {})
        observed = Counter((item["kind"], item["correction"]) for item in self.receipt["dispatched_actions"])
        self.receipt["counts"] = {"role_processes": len(self.processes),
            "builder_processes": sum(actor in BUILDERS for actor in self.processes),
            "reviewer_processes": sum(actor in REVIEWERS for actor in self.processes), "infrastructure_members": len(self.nodes),
            "api_calls": accounting.get("admitted_calls", 0) if self.mode == "live" else 0,
            "api_spend_micro_usd": accounting.get("known_spend_micro_usd", 0) if self.mode == "live" else 0,
            "injected_calls": len(self.transport.snapshot()["calls"]) if self.transport else 0,
            "source_repairs": observed["repair", False], "selector_format_corrections": observed["select_source", True],
            "source_repairs_attempted": observed["repair", False],
            "source_repairs_effective": sum(item.get("candidate") is not None and self.successful(item)
                and item["reply"]["binding"]["action"]["kind"] == "repair" for item in self.completions.values()),
            "source_repairs_unchanged": len(self.unchanged_repairs),
            "review_format_corrections": observed["review", True],
            "private_candidate_bundles": sum(item.get("candidate") is not None for item in self.completions.values()),
            "physical_public_evaluations": sum(bool(item["physically_executed"]) for item in self.physical_publications),
            "protected_releases": int(self.receipt["protected_release"]),
            "independent_interface_cases_passed": (self.receipt.get("independent_acceptance", {}).get("case_count", 0)
                if self.receipt.get("independent_acceptance", {}).get("interface_checks_passed") else 0),
            "independent_browser_observations_passed": int(bool(self.receipt.get("independent_browser_acceptance", {}).get("browser_checks_passed")))}
        if self.mode == "fixture":
            requirements = self.contract["execution_design"]["rehearsal_requirements"]
            complete_fixture = all(self.receipt["counts"].get(key) == expected for key, expected in requirements["exact_counts"].items())
            complete_fixture = complete_fixture and all(self.receipt["counts"].get(key, 0) >= expected
                for key, expected in requirements["minimum_counts"].items())
            self.receipt["fixture_branches_qualified"] = complete_fixture
            self.receipt["passed"] = self.receipt["passed"] and complete_fixture
        self.receipt["passed"] = self.receipt["passed"] and not self.cleanup_errors
        self.receipt["total_elapsed_seconds"] = time.monotonic() - self.started
        self.receipt["deadline_exceeded"] = self.receipt["total_elapsed_seconds"] > self.config.deadline_seconds
        if self.receipt["deadline_exceeded"]:
            self.receipt["outcome_before_deadline_audit"] = self.receipt["outcome"]
            self.receipt.update(outcome="deadline_exhausted", passed=False)
        _write(self.output / "receipt.json", self.receipt)
        return self.receipt


def run_project(output: Path, config: ProjectConfig = ProjectConfig(), *, mode: str = "fixture",
                workers: dict[str, OpenAIWorker] | None = None, ledger_path: Path | None = None,
                permit: dict[str, Any] | None = None, expected_permit_sha256: str | None = None) -> dict[str, Any]:
    """Explicit live entry uses parent-owned workers, pinned permit and existing ledger.

    The launcher performs preflight before credential loading; this function
    independently verifies its exact enrollment before any provider invocation.
    No arbitrary injected worker is accepted as a live provider by finance V3.
    """
    require(mode in ("fixture", "live"), "Mode must be explicit fixture or live")
    output = output.absolute()
    require(output == output.resolve() and not output.exists(), "Output must be a fresh canonical path")
    if mode == "live":
        require(type(workers) is dict and set(workers) == {"mini", "strong"}
                and ledger_path is not None and ledger_path.is_file() and type(permit) is dict
                and type(expected_permit_sha256) is str, "Live pilot requires existing ledger, workers and pinned operator permit")
        assert permit is not None
        require(permit.get("mode") == "live" and isinstance(permit.get("qualification"), dict),
                "Live pilot requires an independently pinned qualification capsule")
        require(digest(permit) == expected_permit_sha256, "Operator permit pin differs")
    else:
        require(workers is None and ledger_path is None and permit is None and expected_permit_sha256 is None,
                "Fixture run cannot accept live capabilities or an external ledger")
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    return ProjectRun(output, config, mode, workers, ledger_path, permit, expected_permit_sha256).run()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--role-config", type=Path)
    parser.add_argument("--deadline-seconds", type=int, default=5400)
    parser.add_argument("--plan", action="store_true")
    args = parser.parse_args(argv)
    if args.role_config is not None:
        require(args.output is None and not args.plan, "Role mode cannot create a controller")
        return _child(args.role_config)
    config = ProjectConfig(deadline_seconds=args.deadline_seconds)
    if args.plan:
        print(json.dumps(execution_contract(config), sort_keys=True, indent=2))
        return 0
    require(args.output is not None, "Fresh --output required for offline fixture; live uses separate qualified launcher")
    result = run_project(args.output, config)
    print(json.dumps({"passed": result["passed"], "outcome": result["outcome"],
        "receipt": str(args.output.absolute() / "receipt.json"), "counts": result["counts"]}))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
