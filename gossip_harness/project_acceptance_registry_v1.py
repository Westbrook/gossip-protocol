"""Pure acceptance aggregation for a prospectively frozen requirement inventory.

This module checks coverage and identity; it does not run or authenticate tests.
Trusted receipt adapters must verify source, observations, execution provenance,
promotion and the irreversible cohort barrier BEFORE constructing these inputs.
A digest is an identity, not a signature or a proof that a test executed. The
inventory itself requires independent review against the complete product brief.
No current v4 receipt is upgraded to whole-project acceptance by this module.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
import re
from typing import Any

PROTOCOL = "project-acceptance-registry-v1"
PURPOSES = ("public_release", "independent_acceptance", "repeatability")
CASE_STATUSES = ("passed", "failed", "skipped", "infrastructure_error")
_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
_SHA = re.compile(r"[0-9a-f]{64}\Z")
MAX_ITEMS = 512


class AcceptanceError(ValueError):
    """Malformed, contradictory or incorrectly bound normalized evidence."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AcceptanceError(message)


def identifier(value: Any) -> None:
    require(type(value) is str and _ID.fullmatch(value) is not None, "Invalid identifier")


def sha256(value: Any) -> None:
    require(type(value) is str and _SHA.fullmatch(value) is not None, "Invalid SHA256")


def members(value: Any, cls: type, *, nonempty: bool = True) -> None:
    require(type(value) is tuple and (1 if nonempty else 0) <= len(value) <= MAX_ITEMS,
            "Expected bounded immutable collection")
    require(all(type(item) is cls for item in value), "Wrong collection member type")


def identifiers(value: tuple[str, ...]) -> None:
    members(value, str)
    for item in value:
        identifier(item)
    require(len(set(value)) == len(value), "Duplicate identifier")


def fingerprint(record: Any) -> str:
    """Domain-separated identity of one validated immutable registry record."""
    payload = json.dumps({"protocol": PROTOCOL, "kind": type(record).__name__,
                          "body": asdict(record)}, sort_keys=True,
                         separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class Subject:
    cohort_id: str
    trajectory_id: str
    milestone: str
    execution_contract_sha256: str
    requirements_sha256: str
    source_sha256: str

    def __post_init__(self) -> None:
        for value in (self.cohort_id, self.trajectory_id, self.milestone):
            identifier(value)
        for value in (self.execution_contract_sha256, self.requirements_sha256, self.source_sha256):
            sha256(value)


@dataclass(frozen=True, slots=True)
class Binding:
    subject: Subject
    ordered_suite_sha256: str
    evaluator_sha256: str
    runtime_image_sha256: str
    environment_sha256: str
    limits_sha256: str
    seed_sha256: str
    execution_protocol: str
    purpose: str

    def __post_init__(self) -> None:
        require(type(self.subject) is Subject, "Expected exact subject")
        for value in (self.ordered_suite_sha256, self.evaluator_sha256, self.runtime_image_sha256,
                      self.environment_sha256, self.limits_sha256, self.seed_sha256):
            sha256(value)
        identifier(self.execution_protocol)
        require(type(self.purpose) is str and self.purpose in PURPOSES, "Unknown observation purpose")


@dataclass(frozen=True, slots=True)
class Gate:
    gate_id: str
    requirement_ids: tuple[str, ...]
    ordered_case_ids: tuple[str, ...]
    binding: Binding

    def __post_init__(self) -> None:
        identifier(self.gate_id)
        identifiers(self.requirement_ids)
        identifiers(self.ordered_case_ids)
        require(type(self.binding) is Binding, "Expected complete execution binding")


@dataclass(frozen=True, slots=True)
class Registry:
    subject: Subject
    inventory_sha256: str
    requirement_ids: tuple[str, ...]
    cohort_trajectory_ids: tuple[str, ...]
    gates: tuple[Gate, ...]

    def __post_init__(self) -> None:
        require(type(self.subject) is Subject, "Expected registry subject")
        sha256(self.inventory_sha256)
        identifiers(self.requirement_ids)
        identifiers(self.cohort_trajectory_ids)
        require(self.subject.trajectory_id in self.cohort_trajectory_ids, "Subject outside cohort")
        members(self.gates, Gate)
        require(len({gate.gate_id for gate in self.gates}) == len(self.gates), "Duplicate gate")
        require(all(gate.binding.subject == self.subject for gate in self.gates), "Gate subject differs")
        covered = {item for gate in self.gates for item in gate.requirement_ids}
        require(covered == set(self.requirement_ids), "Gate mapping must cover exactly the complete inventory")
        require(any(gate.binding.purpose == "independent_acceptance" for gate in self.gates),
                "Whole-project registry needs independent acceptance")


@dataclass(frozen=True, slots=True)
class CaseResult:
    case_id: str
    status: str

    def __post_init__(self) -> None:
        identifier(self.case_id)
        require(type(self.status) is str and self.status in CASE_STATUSES, "Unknown case status")


@dataclass(frozen=True, slots=True)
class PhysicalExecution:
    binding: Binding
    execution_id: str
    receipt_sha256: str
    verifier_receipt_sha256: str
    terminal_status: str
    outcomes: tuple[CaseResult, ...]
    cohort_freeze_sha256: str | None = None

    def __post_init__(self) -> None:
        require(type(self.binding) is Binding, "Expected physical execution binding")
        identifier(self.execution_id)
        sha256(self.receipt_sha256)
        sha256(self.verifier_receipt_sha256)
        require(type(self.terminal_status) is str and self.terminal_status in
                ("completed", "infrastructure_error"), "Unknown execution status")
        members(self.outcomes, CaseResult, nonempty=False)
        require(len({case.case_id for case in self.outcomes}) == len(self.outcomes), "Repeated case")
        if self.cohort_freeze_sha256 is not None:
            sha256(self.cohort_freeze_sha256)
        if self.binding.purpose != "public_release":
            require(self.cohort_freeze_sha256 is not None, "Independent execution needs frozen-cohort proof")


@dataclass(frozen=True, slots=True)
class Observation:
    gate_id: str
    binding: Binding
    execution: PhysicalExecution
    mode: str = "physical"
    reuse_receipt_sha256: str | None = None

    def __post_init__(self) -> None:
        identifier(self.gate_id)
        require(type(self.binding) is Binding and type(self.execution) is PhysicalExecution,
                "Observation needs normalized physical origin")
        require(self.binding == self.execution.binding, "Observation differs from original execution contract")
        require(type(self.mode) is str and self.mode in ("physical", "reused"), "Unknown observation mode")
        if self.mode == "physical":
            require(self.reuse_receipt_sha256 is None, "Physical execution cannot claim a reuse receipt")
        else:
            sha256(self.reuse_receipt_sha256)
            require(self.binding.purpose == "public_release", "Independent acceptance and repeatability require fresh execution")
            require(self.execution.terminal_status == "completed" and bool(self.execution.outcomes)
                    and all(case.status in ("passed", "failed") for case in self.execution.outcomes),
                    "Infrastructure, skipped or empty results cannot be reused as correctness judgments")


@dataclass(frozen=True, slots=True)
class CohortFreeze:
    subjects: tuple[Subject, ...]
    receipt_sha256: str
    verifier_receipt_sha256: str
    no_further_model_actions: bool

    def __post_init__(self) -> None:
        members(self.subjects, Subject)
        require(len({subject.trajectory_id for subject in self.subjects}) == len(self.subjects),
                "Repeated frozen trajectory")
        sha256(self.receipt_sha256)
        sha256(self.verifier_receipt_sha256)
        require(type(self.no_further_model_actions) is bool, "Expected explicit terminal barrier")


@dataclass(frozen=True, slots=True)
class Promotion:
    subject: Subject
    status: str
    receipt_sha256: str | None = None
    verifier_receipt_sha256: str | None = None

    def __post_init__(self) -> None:
        require(type(self.subject) is Subject, "Expected promoted subject")
        require(type(self.status) is str and self.status in ("promoted", "rejected", "missing", "unknown"),
                "Unknown promotion state")
        if self.status in ("promoted", "rejected"):
            sha256(self.receipt_sha256)
            sha256(self.verifier_receipt_sha256)
        else:
            require(self.receipt_sha256 is None and self.verifier_receipt_sha256 is None,
                    "Unavailable promotion cannot carry a verified receipt")


@dataclass(frozen=True, slots=True)
class RequirementResult:
    requirement_id: str
    status: str
    gate_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class Assessment:
    registry_sha256: str
    status: str
    requirements: tuple[RequirementResult, ...]
    failed_gates: tuple[str, ...]
    missing_gates: tuple[str, ...]
    skipped_gates: tuple[str, ...]
    infrastructure_gates: tuple[str, ...]
    physical_gates: tuple[str, ...]
    reused_gates: tuple[str, ...]
    promotion_status: str
    cohort_frozen: bool

    @property
    def accepted_against_registry(self) -> bool:
        return self.status == "accepted_against_registry"


def design_fingerprint(registry: Registry) -> str:
    """Prospective identity: bind every field except future source/contract digests.

    Source is unknown before model work. The containing execution contract pins
    this design, so including its own digest would introduce a circular hash.
    Cohort, trajectory, milestone, requirements and every gate remain fixed.
    """
    require(type(registry) is Registry, "Expected exact registry")
    body = asdict(registry)
    subject = dict(body["subject"])
    del subject["source_sha256"]
    del subject["execution_contract_sha256"]
    body["subject"] = subject
    for gate in body["gates"]:
        gate["binding"]["subject"] = subject
    raw = json.dumps({"protocol": PROTOCOL, "kind": "registry-design", "body": body},
                     sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def assess(registry: Registry, observations: tuple[Observation, ...], promotion: Promotion,
           freeze: CohortFreeze | None, *, expected_design_sha256: str,
           expected_execution_contract_sha256: str) -> Assessment:
    """Aggregate trusted normalized evidence without conferring execution authority.

    Expected digests must come from the trusted frozen study contract, never be
    calculated from untrusted caller data to make that data pass its own check.
    Invalid identities raise AcceptanceError, never become product failures.
    Known assertion failures remain failures even alongside missing/infra evidence.
    An incomplete cohort cannot admit independent observations. Promotion alone,
    or tests alone, can never produce acceptance.
    """
    require(type(registry) is Registry and type(promotion) is Promotion, "Expected registry and promotion")
    sha256(expected_design_sha256)
    sha256(expected_execution_contract_sha256)
    require(design_fingerprint(registry) == expected_design_sha256, "Registry design differs from frozen contract")
    require(registry.subject.execution_contract_sha256 == expected_execution_contract_sha256,
            "Registry execution contract differs")
    require(promotion.subject == registry.subject, "Promotion belongs to another exact subject")
    members(observations, Observation, nonempty=False)
    require(len({item.gate_id for item in observations}) == len(observations), "Duplicate observed gate")
    require(len({item.execution.execution_id for item in observations}) == len(observations)
            and len({item.execution.receipt_sha256 for item in observations}) == len(observations),
            "One physical origin cannot stand in for separately declared executions")
    gates = {gate.gate_id: gate for gate in registry.gates}
    require(all(item.gate_id in gates for item in observations), "Undeclared observed gate")
    frozen = False
    if freeze is not None:
        require(type(freeze) is CohortFreeze, "Expected normalized freeze evidence")
        expected = set(registry.cohort_trajectory_ids)
        actual = {subject.trajectory_id for subject in freeze.subjects}
        require(actual <= expected, "Unexpected frozen trajectory")
        require(all(subject.cohort_id == registry.subject.cohort_id
                    and subject.execution_contract_sha256 == registry.subject.execution_contract_sha256
                    for subject in freeze.subjects), "Frozen cohort or execution contract differs")
        own = [subject for subject in freeze.subjects if subject.trajectory_id == registry.subject.trajectory_id]
        require(not own or own == [registry.subject], "Frozen subject differs from evaluated source")
        frozen = actual == expected and freeze.no_further_model_actions
    observed = {item.gate_id: item for item in observations}
    flags: dict[str, set[str]] = {gate.gate_id: set() for gate in registry.gates}
    for gate in registry.gates:
        item = observed.get(gate.gate_id)
        if item is None:
            flags[gate.gate_id].add("missing")
            continue
        require(item.binding == gate.binding, "Observation differs from registered exact execution binding")
        execution = item.execution
        if gate.binding.purpose != "public_release":
            require(frozen and freeze is not None
                    and execution.cohort_freeze_sha256 == freeze.receipt_sha256,
                    "Independent observation precedes or differs from complete cohort freeze")
        expected_cases = gate.ordered_case_ids
        actual_cases = tuple(case.case_id for case in execution.outcomes)
        require(set(actual_cases) <= set(expected_cases), "Unexpected observed case")
        require(actual_cases == tuple(case for case in expected_cases if case in actual_cases),
                "Observed case order differs from registered suite")
        if actual_cases != expected_cases:
            flags[gate.gate_id].add("missing")
        if execution.terminal_status == "infrastructure_error":
            flags[gate.gate_id].add("infrastructure_error")
        for case in execution.outcomes:
            if case.status != "passed":
                flags[gate.gate_id].add(case.status)
        if item.mode == "reused":
            require(actual_cases == expected_cases, "Incomplete original suite cannot be reused")
    def marked(name: str) -> tuple[str, ...]:
        return tuple(gate.gate_id for gate in registry.gates if name in flags[gate.gate_id])
    requirements = []
    for requirement_id in registry.requirement_ids:
        ids = tuple(gate.gate_id for gate in registry.gates if requirement_id in gate.requirement_ids)
        all_flags = set().union(*(flags[gate_id] for gate_id in ids))
        status = "failed" if "failed" in all_flags else "incomplete" if all_flags else "passed"
        requirements.append(RequirementResult(requirement_id, status, ids))
    failed, missing = marked("failed"), marked("missing")
    skipped, infrastructure = marked("skipped"), marked("infrastructure_error")
    if failed or promotion.status == "rejected":
        status = "rejected"
    elif missing or skipped or infrastructure or promotion.status != "promoted" or not frozen:
        status = "incomplete"
    else:
        status = "accepted_against_registry"
    return Assessment(fingerprint(registry), status, tuple(requirements), failed, missing, skipped,
                      infrastructure, tuple(gate.gate_id for gate in registry.gates if gate.gate_id in observed and observed[gate.gate_id].mode == "physical"),
                      tuple(gate.gate_id for gate in registry.gates if gate.gate_id in observed and observed[gate.gate_id].mode == "reused"),
                      promotion.status, frozen)
