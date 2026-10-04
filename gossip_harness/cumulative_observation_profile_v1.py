"""Prospective M4 applicability of the complete existing CLI/HTTP declarations.

These are review inputs, not semantic review, dispatch authority or observations.
Only the explicit health schema successor changes a legacy expected value. A
new executor/comparator must consume the complete profile before fresh dispatch;
old M1 objects/receipts cannot be relabelled. No production ScopePlan is supplied.
"""
from __future__ import annotations

import base64
from copy import deepcopy
from dataclasses import asdict, dataclass, field
import hashlib
import json
from pathlib import Path
from typing import Any

from . import candidate_cli_cases_v1 as cli
from . import candidate_http_cases_v1 as http
from . import candidate_observation_admission_v1 as admission
from . import project_acceptance_registry_v1 as registry

PROTOCOL = "cumulative-observation-profile-v1"
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
ROOT = Path(__file__).resolve().parents[1]
ORIGINAL_CONTRACT = "library-m1-acceptance-inventory-v1.json"
TARGET_CONTRACT = "library-cumulative-product-v2.json"
ORIGINAL_CONTRACT_SHA256 = "f5d808866d6f18f3fd095de5d447bb5b0e2a43e20771761fe91a38cf4f59ad4a"
TARGET_CONTRACT_SHA256 = "2d88ce0775888f148b0ec3caf90b3d5c82d8fed71f53bec5f7e75f492ae998dc"
ORIGINAL_DEFINITION_PURPOSE = "harness_qualification"
TARGET_MILESTONE = "M4"
HEALTH_BEFORE = b'{"status":"ok","schema":0}'
HEALTH_AFTER = b'{"status":"ok","schema":4}'
_HEALTH_ROWS = {"HTTP-EMPTY-HEALTH/health": 1,
                "HTTP-PERSIST-LISTENER/listener-all-epochs": 2}
_SOURCE_MODULES = (
    "candidate_http_cases_core_v1.py", "candidate_http_cases_v1.py",
    "candidate_http_cases_documents_v1.py", "candidate_http_cases_shapes_v1.py",
    "candidate_http_cases_intake_v1.py", "candidate_http_cases_actions_v1.py",
    "candidate_http_cases_paths_v1.py", "candidate_http_cases_persistence_v1.py",
    "candidate_http_relations_v1.py", "candidate_observation_admission_v1.py",
    "project_acceptance_registry_v1.py", "cumulative_observation_profile_v1.py",
)
REMAINING_COVERAGE = (
    "Independent semantic scope/applicability/purpose review and qualified production authority",
    "Full product inventory, including architecture and actual application delegation",
    "V2 signed64 tokens, atomic exhaustion and lossless transport beyond these small-epoch histories",
    "Migration, incarnation fences, physical schema4 and diagnostic precedence",
    "Stored manifest/content-hash/receipt byte preservation and migration/backup validation",
    "Worker enrollment, liveness, actual concurrency and process-death recovery",
    "Backup-root adoption/refusal, lifecycle and revision-aware v1 routes/commands",
    "Browser journeys, complete release/install/docs/dataset obligations",
    "HTTP raw-only observations and mixed-CLI public-state adapter remain unqualified",
    "Four-milestone/six-trajectory controller, barrier, complete rehearsal and fresh acceptance",
)


class ProfileError(ValueError):
    """Invalid prospective declaration; not a product verdict."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ProfileError(message)


def encoded(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=False,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def digest(value: Any) -> str:
    return hashlib.sha256(encoded(value)).hexdigest()


def source_sha256(files: dict[str, bytes]) -> str:
    """The common C06 blob identity; capture/mode/size/authentication stay external."""
    return admission.source_sha256(files)


def definition_sources() -> dict[str, str]:
    """Pinned norms plus the complete declaration dependency sources, no candidate."""
    sources = cli.definition_sources()
    require(sources[ORIGINAL_CONTRACT] == ORIGINAL_CONTRACT_SHA256
            and sources[TARGET_CONTRACT] == TARGET_CONTRACT_SHA256, "Normative contract differs")
    for name, expected in http.NORMATIVE_AND_VALUE_SOURCE_PINS:
        actual = hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
        require(actual == expected, "HTTP normative/value source differs: " + name)
        sources[name] = actual
    for name in _SOURCE_MODULES:
        path = "gossip_harness/" + name
        sources[path] = hashlib.sha256((ROOT / path).read_bytes()).hexdigest()
    require(sources["gossip_harness/cumulative_observation_profile_v1.py"] == LOADED_SOURCE_SHA256,
            "Loaded profile source changed; use a fresh worker")
    return dict(sorted(sources.items()))


def _clauses() -> tuple[dict[str, Any], ...]:
    raw = (ROOT / TARGET_CONTRACT).read_bytes()
    require(hashlib.sha256(raw).hexdigest() == TARGET_CONTRACT_SHA256, "Target contract changed")
    contract = json.loads(raw)
    rows = [{"pointer": "/compatibility", "value": contract["compatibility"]},
            {"pointer": "/amendments", "value": contract["amendments"]}]
    for index, row in enumerate(contract["requirements"]):
        if row["id"] in ("M4-API-SCHEMA", "M4-COMPATIBILITY", "M4-MIGRATION"):
            rows.append({"pointer": "/requirements/" + str(index), "value": row})
    require(len(rows) == 5, "Missing exact successor/compatibility clauses")
    return tuple({"source": TARGET_CONTRACT, "source_sha256": TARGET_CONTRACT_SHA256,
                  "pointer": row["pointer"], "value": row["value"]} for row in rows)


@dataclass(frozen=True, slots=True)
class DiagnosticCell:
    case_id: str
    step_id: str
    assertion_id: str
    applicability: str
    reason: str

    def __post_init__(self) -> None:
        require(all(type(value) is str and bool(value) for value in
                    (self.case_id, self.step_id, self.assertion_id, self.reason)), "Invalid diagnostic cell")
        require(self.applicability in ("normative", "unspecified", "unqualified"),
                "Unknown prospective applicability")


@dataclass(frozen=True, slots=True)
class HealthSuccessor:
    """Exact prospective expectation change; never a rewritten observation."""
    step_id: str
    step_index: int
    original_body: bytes = HEALTH_BEFORE
    target_body: bytes = HEALTH_AFTER

    def __post_init__(self) -> None:
        require(type(self.step_id) is str and bool(self.step_id)
                and type(self.step_index) is int and self.step_index >= 0, "Invalid health step")
        require(type(self.original_body) is bytes and type(self.target_body) is bytes
                and self.original_body == HEALTH_BEFORE and self.target_body == HEALTH_AFTER,
                "Only the declared schema0-to-schema4 successor is supported")

    def record(self) -> dict[str, Any]:
        return {"step_id": self.step_id, "step_index": self.step_index,
                "selector": "/steps/" + str(self.step_index) + "/expectation/semantic/expected_body",
                "original_body_b64": base64.b64encode(self.original_body).decode("ascii"),
                "target_body_b64": base64.b64encode(self.target_body).decode("ascii"),
                "authority_obligation_ids": ["shared:compatibility:2", "M4-API-SCHEMA:clause:2"],
                "source_pointer": "/compatibility/2"}


def _cli_declaration(case_id: str) -> tuple[dict[str, Any], tuple[DiagnosticCell, ...]]:
    try:
        original = cli.case_definition(case_id)
    except KeyError as error:
        raise ProfileError("Unknown CLI history") from error
    cells = []
    for step in original["recipe"]["steps"]:
        expected = original["expectations"][step["step_id"]]
        assertions = expected["assertion_ids"]
        unspecified = expected["unspecified_assertion_ids"]
        require(len(assertions) == len(set(assertions)) and len(unspecified) == len(set(unspecified))
                and set(unspecified) <= set(assertions), "Malformed prospective assertion roster")
        for assertion in assertions:
            is_unspecified = assertion in unspecified
            cells.append(DiagnosticCell(case_id + ":" + step["step_id"] + ":" + assertion,
                step["step_id"], assertion, "unspecified" if is_unspecified else "normative",
                "Source-declared unspecified presentation/code; no acceptance credit" if is_unspecified
                else "Unchanged source-declared legacy assertion under M4 compatibility"))
    return original, tuple(cells)


def _http_declaration(case_id: str) -> tuple[dict[str, Any], tuple[DiagnosticCell, ...], tuple[HealthSuccessor, ...]]:
    found = tuple(case for case in http.definitions() if case.row_id == case_id)
    require(len(found) == 1, "Unknown HTTP history")
    case = found[0]
    cells: list[DiagnosticCell] = []
    successors: list[HealthSuccessor] = []
    prefix = "http-" + hashlib.sha256(case_id.encode()).hexdigest()[:16]
    for index, step in enumerate(case.steps):
        expectation = step.expectation
        raw_only = expectation is not None and expectation.raw_facts_only
        cells.append(DiagnosticCell(prefix + "-" + str(index).zfill(3), step.step_id, "step",
            "unqualified" if raw_only else "normative",
            "Raw facts only; no declared product expectation or acceptance credit" if raw_only else
            "Complete inherited step; every mandatory applicable facet still required"))
        if expectation is not None and expectation.semantic is not None and expectation.semantic.shape == "health":
            require(case_id in _HEALTH_ROWS and step.request is not None
                    and step.request.method == "GET" and step.request.target == "/health"
                    and expectation.semantic.expected_body == HEALTH_BEFORE
                    and expectation.semantic.status == 200, "Unregistered health successor")
            successors.append(HealthSuccessor(step.step_id, index))
    require(len(successors) == _HEALTH_ROWS.get(case_id, 0), "Health successor census differs")
    return case.record(), tuple(cells), tuple(successors)


@dataclass(frozen=True, slots=True)
class CumulativeProfile:
    """Closed factories derive every mapping; caller cannot supply compatibility hashes.

    The entire resulting record must receive independently authenticated semantic
    review and prospective registration. Constructing this object grants neither.
    HTTP decisive IDs aggregate steps, not missing per-facet qualification.
    Every original step must still execute: diagnostic-only raw requests do not
    waive listener, provenance, capture, continuity or cleanup obligations.
    """
    family: str
    case_id: str
    purpose: str
    _record_json: bytes = field(init=False, repr=False)
    diagnostic_cells: tuple[DiagnosticCell, ...] = field(init=False)
    health_successors: tuple[HealthSuccessor, ...] = field(init=False)

    def __post_init__(self) -> None:
        require(type(self.family) is str and self.family in ("cli", "http")
                and type(self.case_id) is str and bool(self.case_id), "Unknown profile family/history")
        require(type(self.purpose) is str and self.purpose in registry.PURPOSES,
                "Only prospective product purposes; qualification remains separate")
        sources = definition_sources()
        if self.family == "cli":
            original, cells = _cli_declaration(self.case_id)
            successors: tuple[HealthSuccessor, ...] = ()
        else:
            original, cells, successors = _http_declaration(self.case_id)
        require(len({cell.case_id for cell in cells}) == len(cells) and bool(cells),
                "Complete unique diagnostic roster required")
        body = {"protocol": PROTOCOL, "family": self.family, "case_id": self.case_id,
            "original_contract": ORIGINAL_CONTRACT, "original_contract_sha256": ORIGINAL_CONTRACT_SHA256,
            "target_contract": TARGET_CONTRACT, "target_contract_sha256": TARGET_CONTRACT_SHA256,
            "original_milestone": "M1", "target_milestone": TARGET_MILESTONE,
            "original_definition_purpose": ORIGINAL_DEFINITION_PURPOSE, "execution_purpose": self.purpose,
            "original_definition_sha256": digest(original), "original_definition": original,
            "definition_sources": sources, "compatibility_clauses": _clauses(),
            "expectation_successors": [item.record() for item in successors],
            "diagnostic_cells": [asdict(cell) for cell in cells],
            "decisive_case_ids": [cell.case_id for cell in cells if cell.applicability == "normative"],
            "remaining_coverage": REMAINING_COVERAGE,
            "scope": "Existing complete legacy history only; no full-product or M4-added-feature coverage",
            "authoring": "Public authored qualification definitions; no held-out or source-blind claim",
            "review_required": True, "dispatch_authority": False, "acceptance_authority": False,
            "observation_reuse": "No old M1 or qualification observations; independent/repeatability must be fresh"}
        object.__setattr__(self, "diagnostic_cells", cells)
        object.__setattr__(self, "health_successors", successors)
        object.__setattr__(self, "_record_json", encoded(body))

    def record(self) -> dict[str, Any]:
        return json.loads(self._record_json)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self._record_json).hexdigest()

    @property
    def diagnostic_case_ids(self) -> tuple[str, ...]:
        return tuple(cell.case_id for cell in self.diagnostic_cells)

    @property
    def decisive_case_ids(self) -> tuple[str, ...]:
        return tuple(cell.case_id for cell in self.diagnostic_cells if cell.applicability == "normative")

    def target_definition(self) -> dict[str, Any]:
        """Prospective data view, NOT an old LiteralCase/Expectation or observed facts."""
        result = deepcopy(self.record()["original_definition"])
        for successor in self.health_successors:
            raw = successor.target_body
            result["steps"][successor.step_index]["expectation"]["semantic"]["expected_body"] = {
                "base64": base64.b64encode(raw).decode("ascii"), "bytes": len(raw),
                "sha256": hashlib.sha256(raw).hexdigest()}
        return {"profile_protocol": PROTOCOL, "profile_sha256": self.sha256,
                "target_milestone": TARGET_MILESTONE, "definition": result,
                "requires_versioned_comparator": bool(self.health_successors)}


def cli_profile(case_id: str, *, purpose: str) -> CumulativeProfile:
    return CumulativeProfile("cli", case_id, purpose)


def http_profile(row_id: str, *, purpose: str) -> CumulativeProfile:
    return CumulativeProfile("http", row_id, purpose)


@dataclass(frozen=True, slots=True)
class ProfileReviewRequest:
    profile: CumulativeProfile

    def __post_init__(self) -> None:
        require(type(self.profile) is CumulativeProfile, "Exact source-derived prospective profile required")

    def record(self) -> dict[str, Any]:
        return {"protocol": PROTOCOL, "kind": "independent-semantic-review-request",
                "profile_sha256": self.profile.sha256, "profile": self.profile.record(),
                "duties": ["Verify exact complete source and target clause mapping and every diagnostic exclusion",
                    "Authenticate prospective chronology, original purpose and fresh execution binding",
                    "Retain all uncovered obligations in the full independent ScopePlan",
                    "Authenticate actual review originals externally; request hashes are not review evidence"]}

    @property
    def sha256(self) -> str:
        return digest(self.record())


def review_request(profile: CumulativeProfile) -> ProfileReviewRequest:
    return ProfileReviewRequest(profile)


def assert_profile_current(profile: CumulativeProfile) -> None:
    require(type(profile) is CumulativeProfile, "Exact profile required")
    actual = CumulativeProfile(profile.family, profile.case_id, profile.purpose)
    require(actual == profile, "Prospective source/mapping profile changed")


@dataclass(frozen=True, slots=True)
class CliOutcomeProjection:
    """Pure normalized values, with all diagnostics retained; no original authority."""
    profile_sha256: str
    diagnostics: tuple[registry.CaseResult, ...]
    decisive: tuple[registry.CaseResult, ...]


def project_cli_outcomes(profile: CumulativeProfile,
                         diagnostics: tuple[registry.CaseResult, ...]) -> CliOutcomeProjection:
    require(type(profile) is CumulativeProfile and profile.family == "cli", "CLI profile required")
    require(type(diagnostics) is tuple and all(type(item) is registry.CaseResult for item in diagnostics)
            and tuple(item.case_id for item in diagnostics) == profile.diagnostic_case_ids,
            "Complete original ordered diagnostic roster required")
    require(all(item.status == "skipped" for cell, item in zip(profile.diagnostic_cells, diagnostics, strict=True)
                if cell.applicability == "unspecified"), "Unspecified diagnostics cannot become passes or failures")
    decisive = tuple(item for cell, item in zip(profile.diagnostic_cells, diagnostics, strict=True)
                     if cell.applicability == "normative")
    return CliOutcomeProjection(profile.sha256, diagnostics, decisive)


def decisive_cli_outcomes(profile: CumulativeProfile,
                          diagnostics: tuple[registry.CaseResult, ...]) -> tuple[registry.CaseResult, ...]:
    return project_cli_outcomes(profile, diagnostics).decisive
