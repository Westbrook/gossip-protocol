"""Shared prospective admission for new product observation executors.

This boundary is not a production authority, full ScopePlan or study controller.
The host injects capabilities that authenticate a separately retained prospective
registration and durable, irreversible full-cohort freeze against original
records/external checkpoints. Returning a matching dataclass or receipt hash is
not that authentication; synthetic callbacks qualify only these checks.

An executor must reconstruct ``actual`` from captured Git blobs and its exact
versioned profile, complete original execution binding, definitions and limits.
It calls before_intent before retaining an intent, check_current immediately
before dispatch, after execution and before/after verifying retained evidence.
Its own durable journal prevents repeated dispatch and authenticates chronology.
This stateless gate cannot prevent a trusted controller opening a second root.
Qualification receipts are never promoted: new purposes require new prospective
registrations and fresh executions. Authored corpus provenance stays explicit;
this module makes no held-out or complete product coverage claim. No assertion
verdict is interpreted here: unavailable admission is not a failed product test.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, fields, is_dataclass
from functools import lru_cache
import hashlib
import inspect
import json
from pathlib import Path
import re
import sys
import types
from typing import Any, Callable

from . import project_acceptance_registry_v1 as registry

PROTOCOL = "candidate-observation-admission-v1"
_OID = re.compile(r"[0-9a-f]{40}\Z")


class AdmissionError(ValueError):
    """Prospective identity, authenticated authority or barrier is invalid."""


class AdmissionUnavailable(AdmissionError):
    """A required original registration or cohort authority is unavailable."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AdmissionError(message)


def encoded(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def digest(value: Any) -> str:
    return hashlib.sha256(encoded(value)).hexdigest()


def source_manifest(files: dict[str, bytes]) -> list[dict[str, Any]]:
    """Canonical complete blob inventory shared by the new CLI/HTTP profiles.

    Capture, size limits and Git mode safety remain executor responsibilities.
    Old execution protocols retain their own historical source identities.
    """
    require(type(files) is dict and bool(files), "Nonempty complete source inventory required")
    for path, raw in files.items():
        require(type(path) is str and bool(path) and not path.startswith("/")
                and "\\" not in path and all(part and part not in (".", "..")
                for part in path.split("/")), "Invalid source inventory path")
        require(type(raw) is bytes, "Source inventory requires original bytes")
    return [{"path": path, "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
            for path, raw in sorted(files.items())]


def source_sha256(files: dict[str, bytes]) -> str:
    return digest({"protocol": PROTOCOL, "kind": "complete-git-blob-inventory",
                   "files": source_manifest(files)})


def binding_sha256(binding: Any, *, gate: registry.Gate) -> str:
    """Hash the complete original binding only after checking its core identity.

    Execution-specific code must additionally derive/check the full evaluator,
    suite, runtime, environment, limits, seed and profile mapping. This helper
    cannot recognize every executor's binding class or authenticate its source.
    It does reject an old qualification binding used with a product-purpose gate.
    """
    require(type(gate) is registry.Gate, "Exact product gate required")
    require(is_dataclass(binding) and not isinstance(binding, type),
            "Complete original execution binding dataclass required")
    body = asdict(binding)
    subject = gate.binding.subject
    expected = {"source_sha256": subject.source_sha256,
                "requirements_sha256": subject.requirements_sha256,
                "milestone": subject.milestone, "purpose": gate.binding.purpose,
                "protocol": gate.binding.execution_protocol}
    require(all(type(body.get(key)) is str and body[key] == value for key, value in expected.items()),
            "Original execution binding source, requirements, milestone, protocol or purpose differs")
    return digest(body)


@dataclass(frozen=True, slots=True)
class ObservationRegistration:
    """Prospective product profile; fields are identities, not authority proofs."""
    gate: registry.Gate
    commit_oid: str
    tree_oid: str
    repetition_id: str
    cohort_trajectory_ids: tuple[str, ...]
    definition_sha256: str
    profile_sha256: str
    original_definition_purpose: str
    original_binding_sha256: str

    def __post_init__(self) -> None:
        require(type(self.gate) is registry.Gate, "Exact registered product gate required")
        require(all(type(value) is str and _OID.fullmatch(value) is not None
                    for value in (self.commit_oid, self.tree_oid)), "Full Git commit and tree required")
        try:
            registry.identifier(self.repetition_id)
            registry.identifier(self.original_definition_purpose)
            registry.identifiers(self.cohort_trajectory_ids)
            for value in (self.definition_sha256, self.profile_sha256, self.original_binding_sha256):
                registry.sha256(value)
        except registry.AcceptanceError as error:
            raise AdmissionError(str(error)) from error
        require(len(self.cohort_trajectory_ids) == 6
                and self.gate.binding.subject.trajectory_id in self.cohort_trajectory_ids,
                "Exactly six distinct registered trajectories including own subject required")
        require(self.gate.binding.purpose in registry.PURPOSES,
                "Qualification is separate from product observation admission")


def freeze_from_record(value: Any) -> registry.CohortFreeze | None:
    """Decode an exact retained record; this is not receipt authentication."""
    if value is None:
        return None
    require(type(value) is dict and set(value) == {item.name for item in fields(registry.CohortFreeze)},
            "Exact frozen-cohort record required")
    rows = value["subjects"]
    require(type(rows) in (list, tuple), "Frozen subjects must be an ordered record collection")
    keys = {item.name for item in fields(registry.Subject)}
    require(all(type(row) is dict and set(row) == keys for row in rows), "Exact frozen subject records required")
    try:
        freeze = registry.CohortFreeze(tuple(registry.Subject(**row) for row in rows),
            value["receipt_sha256"], value["verifier_receipt_sha256"], value["no_further_model_actions"])
    except (registry.AcceptanceError, TypeError) as error:
        raise AdmissionError(str(error)) from error
    require(encoded(asdict(freeze)) == encoded(value), "Frozen record changed during decoding")
    return freeze


class ObservationAdmission:
    """Trusted-host capability boundary; no durable authority is manufactured.

    verify_registration must authenticate the complete separately retained
    prospective record, including profile semantics, original purpose and slot
    admission, against original controller writes and its external checkpoint.
    verify_cohort must authenticate full irreversible stop/accounting/source
    provenance. Both must reject revocation, rollback and substituted records;
    returning None signals unavailable evidence. They may raise AdmissionError.
    Plain success callbacks in tests simulate these responsibilities only.
    """
    def __init__(self, registration: ObservationRegistration, *,
                 verify_registration: Callable[[], ObservationRegistration | None],
                 verify_cohort: Callable[[], registry.CohortFreeze | None] | None = None):
        require(type(registration) is ObservationRegistration, "Typed prospective registration required")
        require(callable(verify_registration), "Trusted prospective registration capability required")
        require(verify_cohort is None or callable(verify_cohort), "Invalid cohort authority capability")
        self._registration = registration
        self._verify_registration = verify_registration
        self._verify_cohort = verify_cohort

    @property
    def registration(self) -> ObservationRegistration:
        return self._registration

    def _authenticate(self, actual: ObservationRegistration) -> None:
        require(type(actual) is ObservationRegistration and actual == self.registration,
                "Actual execution differs from prospective registration")
        original = self._verify_registration()
        if original is None:
            raise AdmissionUnavailable("Independently retained prospective registration unavailable")
        require(type(original) is ObservationRegistration and original == self.registration,
                "Authenticated prospective registration differs")

    def _freeze(self) -> registry.CohortFreeze | None:
        gate = self.registration.gate
        if gate.binding.purpose == "public_release":
            return None
        if self._verify_cohort is None:
            raise AdmissionUnavailable("Independent execution needs trusted full-cohort authority")
        freeze = self._verify_cohort()
        if freeze is None:
            raise AdmissionUnavailable("Authenticated full-cohort freeze unavailable")
        require(type(freeze) is registry.CohortFreeze and freeze.no_further_model_actions,
                "Irreversible full-cohort stop unproven")
        require(len(freeze.subjects) == 6 and {item.trajectory_id for item in freeze.subjects}
                == set(self.registration.cohort_trajectory_ids), "Complete registered six-trajectory cohort required")
        subject = gate.binding.subject
        require(all(item.cohort_id == subject.cohort_id
                    and item.execution_contract_sha256 == subject.execution_contract_sha256
                    and item.requirements_sha256 == subject.requirements_sha256
                    and item.milestone == subject.milestone for item in freeze.subjects)
                and tuple(item for item in freeze.subjects if item.trajectory_id == subject.trajectory_id) == (subject,),
                "Frozen cohort lineage or own final source differs")
        return freeze

    def before_intent(self, actual: ObservationRegistration) -> registry.CohortFreeze | None:
        """Authenticate before any intent or dispatch; retain the returned freeze."""
        self._authenticate(actual)
        freeze = self._freeze()
        self._authenticate(actual)
        return freeze

    def check_current(self, actual: ObservationRegistration,
                      retained_freeze: registry.CohortFreeze | None) -> None:
        """Recheck before dispatch/after execution and around original verification."""
        require(retained_freeze is None or type(retained_freeze) is registry.CohortFreeze,
                "Typed retained freeze required")
        self._authenticate(actual)
        current = self._freeze()
        require(current == retained_freeze, "Cohort barrier changed from original intent")
        self._authenticate(actual)


@lru_cache(maxsize=128)
def _compiled_source(source: bytes, filename: str) -> types.CodeType:
    # Only immutable expected code is cached; live module objects are compared
    # again on every call. Never execute the compiled evaluator source.
    try:
        return compile(source, filename, "exec", dont_inherit=True)
    except (SyntaxError, ValueError, OverflowError) as error:
        raise AdmissionError("Registered evaluator source cannot compile: " + filename) from error


def _unwrapped_function(value: Any) -> Any:
    if isinstance(value, (classmethod, staticmethod)):
        value = value.__func__
    seen: set[int] = set()
    while hasattr(value, "__wrapped__"):
        require(id(value) not in seen, "Cyclic evaluator wrapper")
        seen.add(id(value))
        value = value.__wrapped__
    return value


def _loaded_definitions_match(module: types.ModuleType, source: bytes) -> None:
    """Match source-declared code without executing it or importing dependencies.

    Generated dataclass methods are not source declarations. Nested functions
    are already part of their enclosing function's code constants. Property
    accessors may share a name, so compare each actual getter/setter/deleter.
    Decorated functions use their source-declared __wrapped__ original.
    This is a stale-code check, not authentication of arbitrary mutable globals
    or a hostile host interpreter. A constant-only stale import in an older module
    without an import-time pin may evade code comparison; frozen complete sources
    and fresh trusted workers remain required before physical execution.
    """
    require(type(module) is types.ModuleType and type(module.__file__) is str,
            "Loaded evaluator must have an original source filename")
    assert module.__file__ is not None
    expected = _compiled_source(source, module.__file__)

    def check(code: types.CodeType, namespace: dict[str, Any]) -> None:
        for item in code.co_consts:
            if not isinstance(item, types.CodeType) or item.co_name.startswith("<"):
                continue
            target = namespace.get(item.co_name)
            if not item.co_flags & inspect.CO_NEWLOCALS:
                require(isinstance(target, type), "Source-declared evaluator class replaced: " + item.co_qualname)
                check(item, dict(vars(target)))
                continue
            candidates = (target.fget, target.fset, target.fdel) if isinstance(target, property) else (target,)
            functions = tuple(_unwrapped_function(value) for value in candidates if value is not None)
            require(any(isinstance(value, types.FunctionType) and value.__code__ == item for value in functions),
                    "Previously imported evaluator code differs: " + module.__name__ + "." + item.co_qualname)
    check(expected, vars(module))


def _registered_source_path(name: str) -> tuple[Path, str | None]:
    require(type(name) is str and bool(name) and not name.startswith("/")
            and "\\" not in name and "\x00" not in name
            and all(part and part not in (".", "..") for part in name.split("/")),
            "Invalid registered evaluator source name")
    relative = name
    if relative.startswith(("http/", "finite/")):
        relative = relative.split("/", 1)[1]
    package = Path(__file__).resolve().parent
    if "/" not in relative and relative.endswith(".py"):
        path = package / relative
    else:
        path = package.parent / relative
    require(path.resolve() == path and not path.is_symlink(), "Evaluator source is not canonical")
    module_name = __package__ + "." + path.stem if path.parent == package and path.suffix == ".py" else None
    return path, module_name


def verify_loaded_sources(sources: dict[str, str]) -> None:
    """Check pinned files and already imported registered evaluator definitions.

    Existing http/finite prefixes are provenance namespaces, not directories.
    Non-Python inputs are hashed but never compiled. Unimported modules are not
    imported here. Keep executor _LOADED_SOURCES/definition checks as well: this
    complements their registration snapshots rather than replacing them.
    Known per-module LOADED_SOURCE_SHA256 and own _LOADED_SOURCES pins are checked
    where provided. Hash equality alone cannot prove matching in-memory code.
    """
    require(type(sources) is dict and bool(sources) and len(sources) <= registry.MAX_ITEMS,
            "Bounded registered evaluator source map required")
    inspected: dict[Path, str] = {}
    for name, fingerprint in sources.items():
        try:
            registry.sha256(fingerprint)
        except registry.AcceptanceError as error:
            raise AdmissionError(str(error)) from error
        path, module_name = _registered_source_path(name)
        previous = inspected.get(path)
        require(previous is None or previous == fingerprint, "Conflicting evaluator source aliases")
        if previous is not None:
            continue
        raw = path.read_bytes()
        require(hashlib.sha256(raw).hexdigest() == fingerprint, "Registered evaluator source changed: " + name)
        inspected[path] = fingerprint
        module = sys.modules.get(module_name) if module_name is not None else None
        if module is None:
            continue
        require(type(module) is types.ModuleType and type(getattr(module, "__file__", None)) is str,
                "Imported evaluator source path unavailable: " + name)
        assert module.__file__ is not None
        require(Path(module.__file__).resolve() == path, "Imported evaluator source path differs: " + name)
        if hasattr(module, "LOADED_SOURCE_SHA256"):
            require(module.LOADED_SOURCE_SHA256 == fingerprint, "Previously imported evaluator source hash differs: " + name)
        own_pins = getattr(module, "_LOADED_SOURCES", None)
        if type(own_pins) is dict:
            for key, value in own_pins.items():
                if type(key) is str and key.rsplit("/", 1)[-1] == path.name:
                    require(value == fingerprint, "Previously imported evaluator own source pin differs: " + name)
        _loaded_definitions_match(module, raw)
