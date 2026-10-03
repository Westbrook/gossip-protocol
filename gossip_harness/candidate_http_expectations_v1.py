"""Immutable prospective HTTP definitions and diagnostic prerequisite handling.

A definition digest identifies supplied declarations. It does not prove that they
existed before execution, derive their expected values, authenticate observations,
or grant acceptance authority. A separately qualified controller must register
these bytes before dispatch and a future bridge must verify that chronology.
The existing historical observer cannot be promoted through this module.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import base64
import hashlib
import json
import re
from typing import Any

from . import candidate_http_execution_v2 as execution
from . import candidate_http_semantics_v1 as semantics
from . import candidate_http_transport_v1 as wire

PROTOCOL = "candidate-http-expectations-v1"
MAX_CASES = 4096  # An evaluator allocation limit, not product behavior.
MAX_ROWS = 4096
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_OID = re.compile(r"[0-9a-f]{40}\Z")
_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
FACETS = ("status", "json_syntax", "body_shape_value", "error_code",
          "reported_code_status_relation", "response_media_type",
          "listener_before", "listener_after")
PURPOSES = ("harness_qualification", "public_release", "independent_acceptance", "repeatability")


class DefinitionError(ValueError):
    """Malformed author declarations; never a candidate product failure."""


def _require(value: bool, message: str) -> None:
    if not value:
        raise DefinitionError(message)


def _identifier(value: str) -> None:
    _require(type(value) is str and _ID.fullmatch(value) is not None, "Invalid identifier")


def _sha(value: str) -> None:
    _require(type(value) is str and _SHA.fullmatch(value) is not None, "Invalid SHA256")


def _items(value: tuple[Any, ...], cls: type, maximum: int, *, empty: bool = False) -> None:
    _require(type(value) is tuple and (0 if empty else 1) <= len(value) <= maximum
             and all(type(item) is cls for item in value), "Expected bounded immutable typed tuple")


def _ids(value: tuple[str, ...], maximum: int, *, empty: bool = False) -> None:
    _items(value, str, maximum, empty=empty)
    for item in value:
        _identifier(item)
    _require(len(set(value)) == len(value), "Duplicate identifier")


def encoded(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(",", ":"),
                      allow_nan=False).encode("ascii")


def digest(value: Any) -> str:
    return hashlib.sha256(encoded({"protocol": PROTOCOL, "value": value})).hexdigest()


def _bytes(value: bytes) -> dict[str, Any]:
    return {"base64": base64.b64encode(value).decode("ascii"), "bytes": len(value),
            "sha256": hashlib.sha256(value).hexdigest()}


@dataclass(frozen=True, slots=True)
class SourcePin:
    path: str
    sha256: str

    def __post_init__(self) -> None:
        _require(type(self.path) is str and 1 <= len(self.path) <= 512
                 and not self.path.startswith("/") and "\\" not in self.path
                 and all(part not in ("", ".", "..") for part in self.path.split("/"))
                 and all(32 <= ord(c) < 127 for c in self.path), "Relative source path required")
        _sha(self.sha256)

    def record(self) -> dict[str, str]:
        return {"path": self.path, "sha256": self.sha256}


@dataclass(frozen=True, slots=True)
class CatalogRow:
    row_id: str
    family_id: str
    requirement_ids: tuple[str, ...]
    interaction_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _identifier(self.row_id)
        _identifier(self.family_id)
        _ids(self.requirement_ids, 512)
        _ids(self.interaction_ids, 512, empty=True)
        _require(not set(self.requirement_ids) & set(self.interaction_ids),
                 "Requirement and interaction labels must be disjoint within a row")

    def record(self) -> dict[str, Any]:
        return {"row_id": self.row_id, "family_id": self.family_id,
                "requirement_ids": list(self.requirement_ids), "interaction_ids": list(self.interaction_ids)}


@dataclass(frozen=True, slots=True)
class Prerequisite:
    step_id: str
    facet: str

    def __post_init__(self) -> None:
        _identifier(self.step_id)
        _require(type(self.facet) is str and self.facet in FACETS, "Unknown prerequisite facet")

    def record(self) -> dict[str, str]:
        return {"step_id": self.step_id, "facet": self.facet}


@dataclass(frozen=True, slots=True)
class ConditionalFacet:
    """Only this facet depends on all named earlier, actually passing facets."""
    facet: str
    requires: tuple[Prerequisite, ...]

    def __post_init__(self) -> None:
        _require(type(self.facet) is str and self.facet in FACETS, "Unknown conditional facet")
        _items(self.requires, Prerequisite, 512)
        _require(len(set(self.requires)) == len(self.requires), "Repeated prerequisite")

    def record(self) -> dict[str, Any]:
        return {"facet": self.facet, "requires": [item.record() for item in self.requires]}


def _expectation_record(value: semantics.Expectation) -> dict[str, Any]:
    return {"shape": value.shape, "status": value.status, "code": value.code,
            "expected_body": _bytes(value.expected_body) if value.expected_body is not None else None}


def _facet_names(value: semantics.Expectation) -> frozenset[str]:
    names = {"status", "json_syntax", "body_shape_value", "response_media_type",
             "listener_before", "listener_after"}
    if value.shape == "error":
        names.update(("error_code", "reported_code_status_relation"))
    return frozenset(names)


@dataclass(frozen=True, slots=True)
class StepExpectation:
    step_id: str
    expectation: semantics.Expectation
    citations: tuple[str, ...]
    conditions: tuple[ConditionalFacet, ...] = ()

    def __post_init__(self) -> None:
        _identifier(self.step_id)
        _require(type(self.expectation) is semantics.Expectation, "Typed pure expectation required")
        _ids(self.citations, 512)
        _items(self.conditions, ConditionalFacet, len(FACETS), empty=True)
        _require(len({item.facet for item in self.conditions}) == len(self.conditions),
                 "Repeated conditional facet")
        _require(all(item.facet in _facet_names(self.expectation) for item in self.conditions),
                 "Condition names a facet absent from this expectation")

    def record(self) -> dict[str, Any]:
        return {"step_id": self.step_id, "expectation": _expectation_record(self.expectation),
                "citations": list(self.citations), "conditions": [item.record() for item in self.conditions]}


@dataclass(frozen=True, slots=True)
class CasePlan:
    """A complete v2 lifecycle and one declared comparison for every probe.

    v2's root/link/CLI limitations are retained. Cases requiring new mechanics
    remain unmaterialized; this definition does not silently approximate them.
    """
    case_id: str
    row_ids: tuple[str, ...]
    recipe: execution.HttpRecipe
    expectations: tuple[StepExpectation, ...]

    def __post_init__(self) -> None:
        _identifier(self.case_id)
        _ids(self.row_ids, MAX_ROWS)
        _require(type(self.recipe) is execution.HttpRecipe, "Exact v2 HTTP recipe required")
        _items(self.expectations, StepExpectation, execution.MAX_STEPS)
        probes = tuple(step.step_id for step in self.recipe.steps if step.kind == "probe")
        _require(tuple(item.step_id for item in self.expectations) == probes,
                 "Expectations must cover every probe in exact recipe order")
        earlier: dict[str, StepExpectation] = {}
        for item in self.expectations:
            for condition in item.conditions:
                for dependency in condition.requires:
                    _require(dependency.step_id in earlier, "Prerequisite must name an earlier probe")
                    _require(dependency.facet in _facet_names(earlier[dependency.step_id].expectation),
                             "Prerequisite facet absent from earlier expectation")
            earlier[item.step_id] = item

    def record(self, limits: wire.WireLimits) -> dict[str, Any]:
        _require(type(limits) is wire.WireLimits, "Typed wire limits required")
        # Literal fixture bytes accompany the recipe's byte manifest. Requests
        # include both canonical definitions and the exact emitted wire bytes.
        probes = [{"step_id": step.step_id,
                   "definition": _bytes(step.request_json),
                   "wire": _bytes(wire.request_bytes(json.loads(step.request_json), self.recipe.port, limits))}
                  for step in self.recipe.steps if step.kind == "probe"]
        return {"case_id": self.case_id, "row_ids": list(self.row_ids),
                "recipe": self.recipe.record(),
                "fixtures": [{"path": name, "content": _bytes(raw)} for name, raw in self.recipe.fixtures],
                "probes": probes, "expectations": [item.record() for item in self.expectations]}


@dataclass(frozen=True, slots=True)
class ExecutionContext:
    commit_oid: str
    tree_oid: str
    source_sha256: str
    requirements_sha256: str
    evaluator_sha256: str
    runtime_sha256: str
    image_id: str
    environment_sha256: str
    policy: execution.HttpPolicy
    purpose: str
    repetition_id: str

    def __post_init__(self) -> None:
        for value in (self.commit_oid, self.tree_oid):
            _require(type(value) is str and _OID.fullmatch(value) is not None, "Exact Git commit/tree required")
        for value in (self.source_sha256, self.requirements_sha256, self.evaluator_sha256,
                      self.runtime_sha256, self.environment_sha256):
            _sha(value)
        _require(type(self.image_id) is str and self.image_id.startswith("sha256:"), "Pinned image required")
        _sha(self.image_id[7:])
        _require(type(self.policy) is execution.HttpPolicy and self.policy.image_id == self.image_id,
                 "Policy/image identity differs")
        _require(type(self.purpose) is str and self.purpose in PURPOSES, "Explicit observation purpose required")
        _identifier(self.repetition_id)

    def record(self) -> dict[str, Any]:
        from dataclasses import asdict
        return {"commit_oid": self.commit_oid, "tree_oid": self.tree_oid,
                "source_sha256": self.source_sha256, "requirements_sha256": self.requirements_sha256,
                "evaluator_sha256": self.evaluator_sha256, "runtime_sha256": self.runtime_sha256,
                "image_id": self.image_id, "environment_sha256": self.environment_sha256,
                "policy": asdict(self.policy), "purpose": self.purpose, "repetition_id": self.repetition_id,
                "execution_protocol": execution.PROTOCOL, "milestone": "M1"}


@dataclass(frozen=True, slots=True)
class SuitePlan:
    """Coverage against supplied roster only; roster correctness needs review."""
    suite_id: str
    rows: tuple[CatalogRow, ...]
    cases: tuple[CasePlan, ...]
    sources: tuple[SourcePin, ...]
    context: ExecutionContext
    protocol: str = field(default=PROTOCOL, init=False)
    authority: str = field(default="author_declarations_only", init=False)
    acceptance_authority: bool = field(default=False, init=False)
    fresh_execution: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        _identifier(self.suite_id)
        _items(self.rows, CatalogRow, MAX_ROWS)
        _items(self.cases, CasePlan, MAX_CASES)
        _items(self.sources, SourcePin, 512)
        _require(type(self.context) is ExecutionContext, "Complete execution context required")
        _require(len({item.row_id for item in self.rows}) == len(self.rows), "Duplicate catalog row")
        _require(len({item.case_id for item in self.cases}) == len(self.cases), "Duplicate case")
        _require(len({item.path for item in self.sources}) == len(self.sources), "Duplicate source pin")
        _require({row.row_id for row in self.rows} == {name for case in self.cases for name in case.row_ids},
                 "Cases must cover exactly the supplied complete roster")
        # Admit every literal request under the same frozen policy now, before a
        # caller can treat serialized plan bytes as ready for registration.
        for case in self.cases:
            case.record(self.context.policy.wire_limits)

    def record(self) -> dict[str, Any]:
        return {"protocol": self.protocol, "suite_id": self.suite_id,
                "authority": self.authority, "acceptance_authority": False, "fresh_execution": False,
                "rows": [item.record() for item in self.rows],
                "cases": [item.record(self.context.policy.wire_limits) for item in self.cases],
                "sources": [item.record() for item in self.sources], "context": self.context.record()}

    def serialize(self) -> bytes:
        return encoded(self.record())

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.serialize()).hexdigest()


@dataclass(frozen=True, slots=True)
class SuppliedFacts:
    step_id: str
    facts: semantics.ResponseFacts

    def __post_init__(self) -> None:
        _identifier(self.step_id)
        _require(type(self.facts) is semantics.ResponseFacts, "Pure supplied facts required")


@dataclass(frozen=True, slots=True)
class StepDiagnostic:
    step_id: str
    comparison: semantics.Comparison

    def __post_init__(self) -> None:
        _identifier(self.step_id)
        _require(type(self.comparison) is semantics.Comparison, "Typed pure comparison required")


@dataclass(frozen=True, slots=True)
class CaseDiagnostic:
    steps: tuple[StepDiagnostic, ...]
    authority: str = field(default="supplied_values_only", init=False)
    acceptance_authority: bool = field(default=False, init=False)
    fresh_execution: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        _items(self.steps, StepDiagnostic, execution.MAX_STEPS)
        _require(len({item.step_id for item in self.steps}) == len(self.steps), "Duplicate diagnostic step")


def diagnose(case: CasePlan, supplied: tuple[SuppliedFacts, ...]) -> CaseDiagnostic:
    """Evaluate supplied values in recipe order, never authenticate them.

    Missing/failed/unspecified prerequisites make only their dependent facets
    unavailable. Independent known failures survive; missing later facts never
    erase earlier failures. No aggregate product verdict is returned.
    """
    _require(type(case) is CasePlan, "Typed case plan required")
    _items(supplied, SuppliedFacts, execution.MAX_STEPS, empty=True)
    _require(len({item.step_id for item in supplied}) == len(supplied), "Duplicate supplied step")
    wanted = {item.step_id for item in case.expectations}
    _require(all(item.step_id in wanted for item in supplied), "Unknown supplied step")
    facts_by_step = {item.step_id: item.facts for item in supplied}
    previous: dict[tuple[str, str], semantics.Facet] = {}
    rows: list[StepDiagnostic] = []
    for item in case.expectations:
        facts = facts_by_step.get(item.step_id, semantics.ResponseFacts(
            semantics.Missing("step facts not supplied"), semantics.Missing("step facts not supplied"),
            semantics.Missing("step facts not supplied")))
        pure = semantics.compare(facts, item.expectation)
        conditions = {condition.facet: condition.requires for condition in item.conditions}
        facets: list[semantics.Facet] = []
        for facet in pure.facets:
            missing = [dependency for dependency in conditions.get(facet.name, ())
                       if previous[(dependency.step_id, dependency.facet)].disposition != "pass"]
            if missing:
                facet = semantics.Facet(facet.name, "unavailable", "prerequisite did not pass: " + ", ".join(
                    dependency.step_id + "." + dependency.facet for dependency in missing), item.citations)
            facets.append(facet)
            previous[(item.step_id, facet.name)] = facet
        rows.append(StepDiagnostic(item.step_id, semantics.Comparison(tuple(facets))))
    return CaseDiagnostic(tuple(rows))
