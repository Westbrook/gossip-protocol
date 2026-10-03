"""Explicitly permitted live adapter over the cumulative V2 accounting engine.

A pinned operator permit is authorization evidence on a trusted host, not a
signature or a defense against a malicious operator. The adapter never discovers
credentials, creates a wallet, changes its cap, or retries a provider request.
Fixture mode uses injected responses and cannot satisfy a live transport check.
"""
from __future__ import annotations

import ast
import hashlib
import json
import math
from pathlib import Path
import re
import sqlite3
import time
from typing import Any, Callable

from . import worker as worker_module
from .peer_financial_authority_v2 import (
    CumulativeAuthorityV2, FinancialError, Payloads, canonical_payload,
)
from .peer_store_v1 import strict_loads
from .peer_project_contract_v2 import ActionRequest, DispatchBinding, decode
from .worker import OpenAIWorker, WorkerRequest

PROTOCOL = "peer-financial-authority-v3"
PERMIT_PROTOCOL = "peer-financial-operator-permit-v3"
QUALIFICATION_PROTOCOL = "peer-financial-qualification-v3"
LIVE_TRANSPORT = "openai-responses-https-no-retry-v3"
FIXTURE_TRANSPORT = "injected-responses-fixture-v3"
_REPOSITORY = Path(__file__).resolve().parent.parent
_HTTPS_TRANSPORT = worker_module._https_transport
_WORKER_RUN = OpenAIWorker.run
_WORKER_PROFILE = OpenAIWorker.profile_manifest
_WORKER_RESERVATION = OpenAIWorker.reservation_units
_WORKER_PAYLOAD = OpenAIWorker._payload
_WORKER_USAGE = OpenAIWorker._usage
_MODEL_PROFILES = worker_module.MODEL_PROFILES
_ENDPOINT = "https://api.openai.com/v1/responses"
MAX_EVIDENCE_BYTES = 16_000_000
_SHA = re.compile(r"[0-9a-f]{64}\Z")
REQUIRED_SOURCES = (
    "gossip_harness/peer_financial_authority_v3.py",
    "gossip_harness/peer_financial_authority_v2.py",
    "gossip_harness/peer_authority_v1.py",
    "gossip_harness/peer_project_contract_v2.py",
    "gossip_harness/peer_store_v1.py",
    "gossip_harness/ledger.py",
    "gossip_harness/verification_journal.py",
    "gossip_harness/worker.py",
    "devtools/verify.py",
    "devtools/telemetry.py",
)
REQUIRED_TEST_CLASSES = (
    "tests/test_peer_financial_authority_v2.py::PeerFinancialAuthorityV2Tests",
    "tests/test_peer_financial_authority_v3.py::PeerFinancialAuthorityV3Tests",
)
_PERMIT_FIELDS = {
    "protocol", "mode", "approval_ref", "execution_design", "execution_design_sha256",
    "cohort_contract_sha256", "sources", "profiles", "incremental_cap_micro_usd",
    "expected_global_cap", "expected_opening_usage", "max_workers", "qualification",
}


def _require(condition: bool, reason: str) -> None:
    if not condition:
        raise FinancialError(reason)


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def digest(value: Any) -> str:
    """The canonical digest used for permit and execution-design identities."""
    return _sha(canonical_payload(value))


def _hash(value: Any) -> bool:
    return type(value) is str and _SHA.fullmatch(value) is not None


def _action_limits(value: Any) -> dict:
    _require(type(value) is dict and set(value) == {"total", "by_kind", "by_kind_generation", "by_actor"},
             "An explicit closed action limit policy is required")
    _require(type(value["total"]) is int and 1 <= value["total"] <= 1024,
             "Invalid aggregate action limit")
    for field in ("by_kind", "by_actor"):
        counts = value[field]
        _require(type(counts) is dict and bool(counts)
                 and all(type(key) is str and bool(key) and type(count) is int and 0 <= count <= 1024
                         for key, count in counts.items()), "Invalid action count limits")
    generations = value["by_kind_generation"]
    _require(type(generations) is dict and set(generations) == set(value["by_kind"]),
             "Generation limits must cover each allowed action kind")
    for kind, counts in generations.items():
        _require(type(counts) is dict and bool(counts)
                 and all(type(key) is str and re.fullmatch(r"0|[1-9][0-9]{0,3}", key) is not None
                         and type(count) is int and 0 <= count <= value["by_kind"][kind]
                         for key, count in counts.items()), "Invalid per-generation action limits")
    return value


def source_fingerprints() -> dict[str, str]:
    """Current minimum financial dependency closure, for operator preparation."""
    return {name: _sha((_REPOSITORY / name).read_bytes()) for name in REQUIRED_SOURCES}


def profile_manifest(worker: OpenAIWorker) -> dict:
    """Transport-independent worker identity, shared by rehearsal and live design."""
    _require(type(worker) is OpenAIWorker, "An exact OpenAIWorker is required")
    return {"manifest": worker.profile_manifest(), "timeout": worker.timeout}


def _source_map(value: Any, *, require_financial: bool = True) -> dict:
    _require(type(value) is dict and bool(value), "A nonempty source map is required")
    if require_financial:
        _require(set(REQUIRED_SOURCES) <= set(value), "Financial dependency closure is incomplete")
    for name, expected in value.items():
        _require(type(name) is str and worker_module._path_valid(name) and _hash(expected),
                 "Invalid source identity")
        path = _REPOSITORY / name
        _require(path.resolve() == path and path.is_file(), "Source path is unavailable or indirect")
        _require(_sha(path.read_bytes()) == expected, "Qualified source bytes changed")
    return value


def _bound_file(reference: Any) -> tuple[dict, bytes]:
    _require(type(reference) is dict and set(reference) == {"path", "sha256"}
             and type(reference["path"]) is str and _hash(reference["sha256"]),
             "Invalid immutable evidence reference")
    path = Path(reference["path"])
    _require(path.is_absolute() and path == path.resolve() and path.is_file(),
             "Evidence needs an existing canonical absolute path")
    _require(path.stat().st_size <= MAX_EVIDENCE_BYTES, "Evidence file exceeds its limit")
    with path.open("rb") as stream:
        raw = stream.read(MAX_EVIDENCE_BYTES + 1)
    _require(len(raw) <= MAX_EVIDENCE_BYTES and _sha(raw) == reference["sha256"],
             "Immutable evidence bytes differ")
    value = strict_loads(raw, max_bytes=MAX_EVIDENCE_BYTES)
    _require(type(value) is dict, "Evidence must be a JSON object")
    return value, raw


def _gate(gate: Any, sources: dict, design: dict) -> None:
    from devtools.verify import runtime_fingerprint
    _require(type(gate) is dict and set(gate) == {"summary", "inputs"}, "Invalid gate references")
    summary, _ = _bound_file(gate["summary"])
    inputs, _ = _bound_file(gate["inputs"])
    # devtools.verify deliberately uses this encoding, not the protocol encoding.
    fingerprint = _sha(json.dumps(inputs, sort_keys=True).encode())
    count = summary.get("selected_total")
    _require(summary.get("status") == "passed" and summary.get("fingerprint") == fingerprint
             and summary.get("stale_inputs") is False and summary.get("stale_runtime") is False
             and not summary.get("error") and type(count) is int and count > 0
             and summary.get("outcomes") == {"passed": count}
             and type(summary.get("static")) is dict and summary["static"].get("status") == "passed"
             and summary["static"].get("returncode") == 0,
             "Central gate is not a complete passing observation")
    _require(type(inputs.get("inputs")) is dict
             and all(inputs["inputs"].get(name) == sha for name, sha in sources.items()),
             "Central gate does not bind the qualified sources")
    _source_map(inputs["inputs"], require_financial=False)
    _require(inputs.get("runner_sha256") == _sha((_REPOSITORY / "devtools/verify.py").read_bytes())
             and inputs.get("static_command") is None, "Gate runner or static command differs")
    _require(inputs.get("runtime") == runtime_fingerprint(), "Current runtime differs from the qualified gate")
    jobs = summary.get("jobs")
    _require(type(jobs) is list and bool(jobs), "Central gate has no class evidence")
    assert isinstance(jobs, list)
    expected_classes = design.get("required_test_classes", list(REQUIRED_TEST_CLASSES))
    _require(type(expected_classes) is list and bool(expected_classes)
             and all(type(item) is str for item in expected_classes)
             and set(REQUIRED_TEST_CLASSES) <= set(expected_classes), "Incomplete required test classes")
    seen = set()
    seen_tests = set()
    total = 0
    for job in jobs:
        _require(type(job) is dict and job.get("status") == "passed" and job.get("physical") is True
                 and "reused_from" not in job and job.get("returncode") == 0
                 and not job.get("fixture_errors"), "Gate class was not physically qualified")
        tests = job.get("tests")
        _require(type(tests) is list and bool(tests)
                 and all(type(test) is dict and test.get("status") == "passed" for test in tests),
                 "Gate contains missing or nonpassing tests")
        name = job.get("class")
        _require(type(name) is str and name not in seen, "Duplicate gate class identity")
        identifiers = [test.get("id") for test in tests]
        _require(all(type(item) is str and item not in seen_tests for item in identifiers)
                 and len(set(identifiers)) == len(identifiers), "Duplicate or invalid gate test identity")
        seen_tests.update(identifiers)
        if name in expected_classes:
            relative, class_name = name.split("::")
            _require(worker_module._path_valid(relative), "Invalid required test source")
            path = _REPOSITORY / relative
            _require(path.resolve() == path and path.is_file()
                     and inputs["inputs"].get(relative) == _sha(path.read_bytes()),
                     "Required test source is not bound by the gate")
            module = ast.parse(path.read_bytes())
            classes = [node for node in module.body if isinstance(node, ast.ClassDef) and node.name == class_name]
            _require(len(classes) == 1, "Required test class is unavailable")
            expected_ids = [name + "." + method for method in sorted(
                node.name for node in classes[0].body
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("test"))]
            _require(bool(expected_ids) and identifiers == expected_ids,
                     "Required class was not physically executed in full ordered scope")
        seen.add(name)
        total += len(tests)
        retained, _ = _bound_file({"path": job.get("evidence_path"), "sha256": job.get("evidence_sha256")})
        _require(retained.get("status") == "passed" and retained.get("tests") == tests,
                 "Gate class receipt differs from its summary")
    _require(total == count and set(expected_classes) <= seen, "Central gate omitted required coverage")


def _rehearsal_complete(receipt: dict, design: dict, sources: dict) -> None:
    requirements = design.get("rehearsal_requirements")
    _require(type(requirements) is dict and set(requirements) == {"protocol", "exact_counts", "minimum_counts"}
             and type(requirements["protocol"]) is str and bool(requirements["protocol"])
             and receipt.get("protocol") == requirements["protocol"], "Rehearsal runner protocol differs")
    assert isinstance(requirements, dict)
    counts = receipt["counts"]
    for field in ("exact_counts", "minimum_counts"):
        expected = requirements[field]
        _require(type(expected) is dict and bool(expected), "Rehearsal completeness counts are missing")
        for name, count in expected.items():
            actual = counts.get(name)
            _require(type(count) is int and count >= 0 and type(actual) is int
                     and (actual == count if field == "exact_counts" else actual >= count),
                     "Rehearsal did not complete all registered workflow requirements")
    financial = receipt.get("financial_config")
    _require(type(financial) is dict and receipt.get("financial_config_sha256") == digest(financial)
             and financial.get("protocol") == PROTOCOL and financial.get("mode") == "fixture"
             and financial.get("observation_kind") == "simulated" and financial.get("sources") == sources
             and financial.get("execution_design_sha256") == digest(design),
             "Rehearsal financial identity differs")
    assert isinstance(financial, dict)
    permit = financial.get("operator_permit")
    _require(type(permit) is dict and permit.get("mode") == "fixture" and permit.get("qualification") is None
             and permit.get("execution_design") == design and permit.get("sources") == sources
             and financial.get("operator_permit_sha256") == digest(permit),
             "Rehearsal did not use the same fixture financial policy")
    contract = financial.get("contract")
    _require(type(contract) is dict and type(contract.get("task_specs")) is list,
             "Rehearsal financial task membership is unavailable")
    assert isinstance(contract, dict)
    actors = {actor for spec in contract["task_specs"] for actor in spec["actors"]}
    final_roles = receipt.get("final_roles")
    _require(type(final_roles) is list and len(final_roles) == len(actors)
             and len(final_roles) == counts.get("role_processes"), "Rehearsal role completion is incomplete")
    assert isinstance(final_roles, list)
    seen = set()
    pids = set()
    for role in final_roles:
        _require(type(role) is dict and role.get("actor") in actors and role["actor"] not in seen
                 and type(role.get("pid")) is int and role["pid"] > 0 and role["pid"] not in pids,
                 "Rehearsal role identities are duplicated or missing")
        retained, _ = _bound_file({"path": role.get("receipt"), "sha256": role.get("sha256")})
        _require(all(retained.get(key) == role.get(key) for key in ("actor", "pid", "journal_root", "completed_actions"))
                 and type(role.get("completed_actions")) is list and bool(role["completed_actions"]),
                 "Rehearsal role evidence differs")
        seen.add(role["actor"])
        pids.add(role["pid"])
    exits = receipt.get("owned_process_exit_codes")
    _require(type(exits) is dict and set(exits) == actors
             and all(type(code) is int and code == 0 for code in exits.values()),
             "Rehearsal did not shut down every owned role cleanly")
    accounting = receipt.get("accounting")
    _require(type(accounting) is dict and type(accounting.get("unsettled_reservations")) is int
             and accounting["unsettled_reservations"] == 0, "Rehearsal accounting is not quiescent")


def validate_qualification(reference: dict, execution_design: dict, sources: dict) -> dict:
    """Read exact central-gate and physical-rehearsal evidence before live entry."""
    capsule, _ = _bound_file(reference)
    _require(set(capsule) == {"protocol", "execution_design_sha256", "sources", "gate", "rehearsal"}
             and capsule["protocol"] == QUALIFICATION_PROTOCOL
             and capsule["execution_design_sha256"] == digest(execution_design)
             and capsule["sources"] == sources, "Qualification capsule identity differs")
    _source_map(sources)
    _gate(capsule["gate"], sources, execution_design)
    rehearsal = capsule["rehearsal"]
    _require(type(rehearsal) is dict and set(rehearsal) == {"receipt", "execution_contract"},
             "Invalid rehearsal references")
    receipt, _ = _bound_file(rehearsal["receipt"])
    contract, _ = _bound_file(rehearsal["execution_contract"])
    _require(contract.get("execution_design") == execution_design
             and contract.get("execution_design_sha256") == digest(execution_design)
             and contract.get("sources") == sources,
             "Rehearsal did not execute the approved design and sources")
    _require(receipt.get("passed") is True and receipt.get("physically_executed") is True
             and receipt.get("execution_contract_sha256") == digest(contract)
             and receipt.get("execution_design_sha256") == digest(execution_design)
             and receipt.get("mode") == "fixture" and not receipt.get("cleanup_errors")
             and type(receipt.get("counts")) is dict and receipt["counts"].get("api_calls") == 0
             and type(receipt["counts"].get("api_calls")) is int
             and receipt["counts"].get("api_spend_micro_usd") == 0
             and type(receipt["counts"].get("api_spend_micro_usd")) is int,
             "Matching physical fixture rehearsal is incomplete")
    _rehearsal_complete(receipt, execution_design, sources)
    return capsule


class CumulativeAuthorityV3(CumulativeAuthorityV2):
    @classmethod
    def open(cls, existing_ledger_path: Path, service_root: Path, cohort_contract: dict,
             incremental_cap_micro_usd: int, expected_global_cap: int, expected_opening_usage: int,
             *, payloads: Payloads, workers: dict[str, OpenAIWorker], permit: dict | None = None,
             expected_permit_sha256: str | None = None, max_workers: int = 4,
             recovery: bool = False, mode: str = "live", clock: Callable[[], float] = time.time,
             crash_hook: Callable[[str, str], None] | None = None) -> CumulativeAuthorityV3:
        return cls(existing_ledger_path, service_root, cohort_contract, incremental_cap_micro_usd,
                   expected_global_cap, expected_opening_usage, payloads=payloads, workers=workers,
                   permit=permit, expected_permit_sha256=expected_permit_sha256, max_workers=max_workers,
                   recovery=recovery, mode=mode, clock=clock, crash_hook=crash_hook)

    def __init__(self, existing_ledger_path: Path, service_root: Path, cohort_contract: dict,
                 incremental_cap_micro_usd: int, expected_global_cap: int, expected_opening_usage: int,
                 *, payloads: Payloads, workers: dict[str, OpenAIWorker], permit: dict | None = None,
                 expected_permit_sha256: str | None = None, max_workers: int = 4,
                 recovery: bool = False, mode: str = "live", clock: Callable[[], float] = time.time,
                 crash_hook: Callable[[str, str], None] | None = None):
        self._prepare_permit(cohort_contract, incremental_cap_micro_usd, expected_global_cap,
                             expected_opening_usage, workers=workers, permit=permit,
                             expected_permit_sha256=expected_permit_sha256, max_workers=max_workers, mode=mode)
        super().__init__(existing_ledger_path, service_root, cohort_contract, incremental_cap_micro_usd,
                         expected_global_cap, expected_opening_usage, payloads=payloads, workers=workers,
                         max_workers=max_workers, recovery=recovery, mode=mode, clock=clock, crash_hook=crash_hook)

    @classmethod
    def preflight_permit(cls, cohort_contract: dict, incremental_cap_micro_usd: int,
                         expected_global_cap: int, expected_opening_usage: int, *,
                         workers: dict[str, OpenAIWorker], permit: dict,
                         expected_permit_sha256: str, max_workers: int = 4, mode: str = "live") -> dict:
        """Keyless evidence/profile check using dummy-key workers; never opens a ledger.

        The operator separately checks a read-only ledger snapshot. Actual open
        still obtains exclusive ownership and checks its exact current balance.
        This returns data only, never a dispatch-capable authority or permission.
        """
        checking = object.__new__(cls)
        checking._prepare_permit(cohort_contract, incremental_cap_micro_usd, expected_global_cap,
                                 expected_opening_usage, workers=workers, permit=permit,
                                 expected_permit_sha256=expected_permit_sha256, max_workers=max_workers, mode=mode)
        checking.contract = strict_loads(canonical_payload(cohort_contract), max_bytes=2_100_000)
        _require(set(checking.contract) == {"cohort_id", "execution_contract_sha256", "ledger_identity",
                 "journal_root", "transport_identity", "task_specs"}, "Invalid closed cohort contract")
        profiles = {name: checking._profile(name, worker) for name, worker in workers.items()}
        return {"protocol": PROTOCOL, "mode": mode, "operator_permit_sha256": expected_permit_sha256,
                "execution_design_sha256": checking.permit["execution_design_sha256"],
                "cohort_contract_sha256": digest(checking.contract), "profiles": profiles,
                "ledger_opened": False, "provider_invoked": False}

    def _prepare_permit(self, cohort_contract: dict, incremental_cap_micro_usd: int,
                        expected_global_cap: int, expected_opening_usage: int, *,
                        workers: dict[str, OpenAIWorker], permit: dict | None,
                        expected_permit_sha256: str | None, max_workers: int, mode: str) -> None:
        self._validate_mode(mode)
        _require(type(permit) is dict and set(permit) == _PERMIT_FIELDS
                 and _hash(expected_permit_sha256), "An explicitly pinned closed operator permit is required")
        self._permit_bytes = canonical_payload(permit)
        _require(_sha(self._permit_bytes) == expected_permit_sha256, "Operator permit pin differs")
        self.permit = strict_loads(self._permit_bytes, max_bytes=2_100_000)
        self._permit_sha256 = expected_permit_sha256
        self._mode = mode
        actual_profiles = {name: profile_manifest(worker) for name, worker in workers.items()}
        _require(self.permit["protocol"] == PERMIT_PROTOCOL and self.permit["mode"] == mode
                 and type(self.permit["approval_ref"]) is str and 1 <= len(self.permit["approval_ref"]) <= 1024
                 and self.permit["cohort_contract_sha256"] == digest(cohort_contract)
                 and self.permit["profiles"] == actual_profiles,
                 "Permit does not cover this cohort, mode or profiles")
        for field, value in (("incremental_cap_micro_usd", incremental_cap_micro_usd),
                             ("expected_global_cap", expected_global_cap),
                             ("expected_opening_usage", expected_opening_usage), ("max_workers", max_workers)):
            _require(type(value) is int and type(self.permit[field]) is int and self.permit[field] == value,
                     "Permit financial or executor limits differ")
        _require(0 <= expected_opening_usage <= expected_global_cap
                 and 0 < incremental_cap_micro_usd <= expected_global_cap - expected_opening_usage,
                 "Allocated cohort exceeds the approved remaining cumulative cap")
        design = self.permit["execution_design"]
        _require(type(design) is dict and design.get("profiles") == actual_profiles
                 and type(design.get("max_workers")) is int and design["max_workers"] == max_workers
                 and self.permit["execution_design_sha256"] == digest(design),
                 "Execution design differs from the approved worker resources")
        _action_limits(design.get("action_limits"))
        self._guard_evidence()

    def _validate_mode(self, mode: str) -> None:
        _require(mode in {"live", "fixture"}, "V3 requires explicit live or fixture mode")

    def _guard_evidence(self) -> None:
        _require(canonical_payload(self.permit) == self._permit_bytes, "Operator permit changed in memory")
        _source_map(self.permit["sources"])
        if self._mode == "live":
            _require(type(self.permit["qualification"]) is dict, "Live entry requires immutable qualification")
            validate_qualification(self.permit["qualification"], self.permit["execution_design"],
                                   self.permit["sources"])
        else:
            _require(self.permit["qualification"] is None, "Fixture mode cannot claim live qualification")

    def _profile(self, name: str, worker: OpenAIWorker) -> dict:
        expected_transport = LIVE_TRANSPORT if self._mode == "live" else FIXTURE_TRANSPORT
        _require(type(worker) is OpenAIWorker and worker_module.ENDPOINT == _ENDPOINT
                 and worker_module._https_transport is _HTTPS_TRANSPORT
                 and OpenAIWorker.run is _WORKER_RUN and OpenAIWorker.profile_manifest is _WORKER_PROFILE
                 and OpenAIWorker.reservation_units is _WORKER_RESERVATION
                 and OpenAIWorker._payload is _WORKER_PAYLOAD and OpenAIWorker._usage is _WORKER_USAGE
                 and not {"run", "profile_manifest", "reservation_units", "_payload", "_usage"} & set(vars(worker)),
                 "Worker implementation or endpoint changed")
        _require(worker_module.MODEL_PROFILES is _MODEL_PROFILES
                 and worker._profile is _MODEL_PROFILES.get(worker.model)
                 and type(worker.max_output_tokens) is int and 16 <= worker.max_output_tokens <= worker._profile.max_output_tokens
                 and type(worker.timeout) in (int, float) and math.isfinite(worker.timeout) and worker.timeout > 0,
                 "Worker no longer uses an intact priced model profile")
        _require((worker._transport is _HTTPS_TRANSPORT) == (self._mode == "live"),
                 "Worker transport does not match the explicit execution mode")
        _require(self.contract["transport_identity"] == expected_transport
                 and name in self.permit["profiles"]
                 and profile_manifest(worker) == self.permit["profiles"][name],
                 "Registered worker profile or transport identity differs")
        return {**profile_manifest(worker), "transport_identity": expected_transport,
                "observation_kind": "provider" if self._mode == "live" else "simulated"}

    def _finalize_config(self, config: dict) -> dict:
        limits = _action_limits(self.permit["execution_design"]["action_limits"])
        _require(set(limits["by_actor"]) == self.actors, "Action limits do not match registered actors")
        for spec in self.task_specs.values():
            generation = str(spec["work"]["generation"])
            _require(all(kind in limits["by_kind"] and generation in limits["by_kind_generation"][kind]
                         for kind in spec["kinds"]), "Registered task exceeds prospective action scopes")
        return {**config, "protocol": PROTOCOL, "sources": self.permit["sources"],
                "operator_permit": self.permit, "operator_permit_sha256": self._permit_sha256,
                "expected_opening_usage": self.permit["expected_opening_usage"],
                "execution_design_sha256": self.permit["execution_design_sha256"],
                "observation_kind": "provider" if self._mode == "live" else "simulated"}

    def _admission_reason(self, db: sqlite3.Connection, actor: str, action: ActionRequest) -> str | None:
        _require(canonical_payload(self.permit) == self._permit_bytes, "Operator permit changed before admission")
        limits = self.permit["execution_design"]["action_limits"]
        kind, generation = action.kind, str(action.work.generation)
        rows = list(db.execute("SELECT binding FROM financial_actions_v2 WHERE cohort=?", (self.cohort_id,)))
        admitted = [decode(row["binding"], DispatchBinding,
                           expected_contract_sha256=self.contract["execution_contract_sha256"]).action for row in rows]
        if (len(admitted) >= limits["total"]
                or sum(item.actor == actor for item in admitted) >= limits["by_actor"].get(actor, 0)
                or sum(item.kind == kind for item in admitted) >= limits["by_kind"].get(kind, 0)
                or sum(item.kind == kind and item.work.generation == action.work.generation for item in admitted)
                >= limits["by_kind_generation"].get(kind, {}).get(generation, 0)):
            return "cohort_call_limit"
        return None

    def _before_invoke(self, worker: OpenAIWorker, request: WorkerRequest) -> None:
        self._guard_evidence()
        names = [name for name, registered in self.workers.items() if registered is worker]
        _require(bool(names) and all(self._profile(name, worker) == self.profiles[name] for name in names),
                 "Bound worker changed before provider entry")


def preflight_permit(permit: dict, expected_permit_sha256: str, cohort_contract: dict, profiles: dict) -> dict:
    """Validate an operator's live envelope before loading any actual credential.

    Dummy-key workers only reconstruct the pinned pricing/request profiles; no
    worker is invoked and no ledger or service is opened. The live constructor
    repeats these checks with the real parent-owned workers and ledger lock.
    """
    _require(type(permit) is dict and set(permit) == _PERMIT_FIELDS and permit.get("mode") == "live",
             "Live preflight requires a closed live operator permit")
    _require(type(profiles) is dict and 1 <= len(profiles) <= 8, "Invalid preflight profile catalog")
    workers = {}
    for name, profile in profiles.items():
        _require(type(name) is str and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,95}", name) is not None
                 and type(profile) is dict and set(profile) == {"manifest", "timeout"}
                 and type(profile["manifest"]) is dict, "Invalid preflight worker profile")
        try:
            worker = OpenAIWorker("keyless-profile-preflight-placeholder",
                                  model=profile["manifest"]["model"],
                                  max_output_tokens=profile["manifest"]["configured_max_output_tokens"],
                                  timeout=profile["timeout"])
        except (KeyError, TypeError, ValueError):
            raise FinancialError("Preflight profile is not a supported priced worker") from None
        _require(canonical_payload(profile_manifest(worker)) == canonical_payload(profile),
                 "Preflight profile differs from the priced worker implementation")
        workers[name] = worker
    return CumulativeAuthorityV3.preflight_permit(
        cohort_contract, permit["incremental_cap_micro_usd"], permit["expected_global_cap"],
        permit["expected_opening_usage"], workers=workers, permit=permit,
        expected_permit_sha256=expected_permit_sha256, max_workers=permit["max_workers"], mode="live")
