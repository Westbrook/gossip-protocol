"""Immutable interfaces for the prospective multi-role project runtime.

These records describe evidence and requests; none grants spending, Git or
release authority.  Transport must prove local arrival, finance must authenticate
the caller and retained journal outcome, and release must enforce scoped review.
No file, network, model, clock or database operation occurs in this module.

Component digests use this module's domain-separated UTF-8 canonical JSON, not
the legacy evaluator's source digest encoding. WorkerRequest and Lease retain
their existing implementations; tuples are explicitly normalized to JSON arrays.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, fields
import hashlib
import json
import math
import re
from types import UnionType
from typing import Any, ClassVar, TypeVar, get_args, get_origin, get_type_hints

from .ledger import Lease
from .worker import WorkerRequest, _path_valid

PROTOCOL = "peer-project-contract-v2"
SCHEMA_VERSION = 2
MAX_RECORD_BYTES = 512_000
MAX_DEPTH = 32
MAX_COLLECTION = 256
MAX_ROLES = 64
PACKAGES = ("catalog", "ingestion", "query", "clients")
ACTION_KINDS = ("plan", "build", "select_tests", "select_source", "review", "repair")
VERDICTS = ("approve", "request_changes", "insufficient_evidence")
_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,95}\Z")
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_OID = re.compile(r"[0-9a-f]{40}\Z")


class ContractError(ValueError):
    """Malformed or internally inconsistent project evidence."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ContractError(message)


def identifier(value: Any) -> str:
    require(type(value) is str and _ID.fullmatch(value) is not None, "Invalid identifier")
    return value


def sha256(value: Any) -> str:
    require(type(value) is str and _SHA.fullmatch(value) is not None, "Invalid SHA256")
    return value


def _integer(value: Any, low: int = 0, high: int = (1 << 63) - 1) -> None:
    require(type(value) is int and low <= value <= high, "Invalid bounded integer")


def _text(value: Any, limit: int = 4096) -> None:
    require(type(value) is str and len(value) <= limit, "Invalid bounded text")
    try:
        value.encode("utf-8")
    except UnicodeError as error:
        raise ContractError("Invalid Unicode text") from error


def _items(value: tuple, *, limit: int = MAX_COLLECTION, unique: bool = True) -> None:
    require(type(value) is tuple and len(value) <= limit, "Invalid bounded tuple")
    if unique:
        require(len(set(value)) == len(value), "Duplicate collection member")


def _ids(values: tuple[str, ...], *, limit: int = MAX_COLLECTION) -> None:
    _items(values, limit=limit)
    for value in values:
        identifier(value)


def _hashes(values: tuple[str, ...]) -> None:
    _items(values)
    for value in values:
        sha256(value)


def _lease(value: Lease) -> None:
    require(type(value) is Lease, "Expected existing Lease")
    identifier(value.task_id)
    identifier(value.worker_id)
    _integer(value.epoch, 1)
    require(type(value.expires_at) in (int, float), "Invalid lease expiration type")
    try:
        require(math.isfinite(value.expires_at) and value.expires_at > 0, "Invalid lease expiration")
    except OverflowError as error:
        raise ContractError("Lease expiration exceeds finite clock range") from error


_TYPE_HINTS: dict[type, dict[str, Any]] = {}


def _hints(cls: type) -> dict[str, Any]:
    if cls not in _TYPE_HINTS:
        _TYPE_HINTS[cls] = get_type_hints(cls)
    return _TYPE_HINTS[cls]


def _typed(value: Any, annotation: Any) -> None:
    origin, args = get_origin(annotation), get_args(annotation)
    if origin is UnionType:
        for option in args:
            try:
                _typed(value, option)
                return
            except ContractError:
                pass
        raise ContractError("Wrong optional field type")
    if origin is tuple:
        require(type(value) is tuple and len(value) <= MAX_COLLECTION, "Expected bounded tuple")
        require(len(args) == 2 and args[1] is Ellipsis, "Unsupported tuple schema")
        for part in value:
            _typed(part, args[0])
        return
    require(type(value) is annotation, "Wrong field type")
    if annotation is Lease:
        _lease(value)


@dataclass(frozen=True, slots=True)
class Record:
    KIND: ClassVar[str]

    def __post_init__(self) -> None:
        hints = _hints(type(self))
        for field in fields(self):
            _typed(getattr(self, field.name), hints[field.name])


@dataclass(frozen=True, slots=True)
class Context(Record):
    KIND: ClassVar[str] = "context"
    execution_contract_sha256: str
    cohort_id: str
    trajectory_id: str
    milestone: int
    requirements_sha256: str

    def __post_init__(self) -> None:
        Record.__post_init__(self)
        sha256(self.execution_contract_sha256)
        identifier(self.cohort_id)
        identifier(self.trajectory_id)
        _integer(self.milestone, 0, 64)
        sha256(self.requirements_sha256)


@dataclass(frozen=True, slots=True)
class WorkKey(Record):
    KIND: ClassVar[str] = "work-key"
    package_id: str
    requirement_id: str
    slot_id: str
    generation: int

    def __post_init__(self) -> None:
        Record.__post_init__(self)
        require(self.package_id in PACKAGES, "Unknown work package")
        identifier(self.requirement_id)
        identifier(self.slot_id)
        _integer(self.generation, 0, 1_000_000)


@dataclass(frozen=True, slots=True)
class EvidenceRef(Record):
    KIND: ClassVar[str] = "evidence-ref"
    event_id: str
    producer: str
    kind: str
    payload_sha256: str

    def __post_init__(self) -> None:
        Record.__post_init__(self)
        sha256(self.event_id)
        identifier(self.producer)
        identifier(self.kind)
        sha256(self.payload_sha256)


@dataclass(frozen=True, slots=True)
class LocalViewManifest(Record):
    KIND: ClassVar[str] = "local-view"
    context: Context
    policy_sha256: str
    source_ref: EvidenceRef
    evidence_refs: tuple[EvidenceRef, ...]
    required_evidence_ids: tuple[str, ...]
    omitted_required_ids: tuple[str, ...]
    materialized_content_sha256: str

    def __post_init__(self) -> None:
        Record.__post_init__(self)
        sha256(self.policy_sha256)
        sha256(self.materialized_content_sha256)
        _items(self.evidence_refs)
        _hashes(self.required_evidence_ids)
        _hashes(self.omitted_required_ids)
        ids = [self.source_ref.event_id, *(ref.event_id for ref in self.evidence_refs)]
        require(len(ids) == len(set(ids)), "Ambiguous local evidence identity")
        missing = set(self.required_evidence_ids) - set(ids)
        require(set(self.omitted_required_ids) == missing, "Incorrect omitted required evidence")

    def require_complete(self) -> None:
        require(not self.omitted_required_ids, "Required local evidence is missing")


@dataclass(frozen=True, slots=True)
class ActionRequest(Record):
    KIND: ClassVar[str] = "action-request"
    context: Context
    action_id: str
    request_id: str
    actor: str
    kind: str
    work: WorkKey
    profile_id: str
    worker_payload_ref: EvidenceRef
    view_manifest_sha256: str

    def __post_init__(self) -> None:
        Record.__post_init__(self)
        for value in (self.action_id, self.request_id, self.actor, self.profile_id):
            identifier(value)
        require(self.kind in ACTION_KINDS, "Unknown action kind")
        require(self.worker_payload_ref.producer == self.actor
                and self.worker_payload_ref.kind == "worker-request", "Worker request is not actor-owned")
        sha256(self.view_manifest_sha256)


@dataclass(frozen=True, slots=True)
class DispatchBinding(Record):
    KIND: ClassVar[str] = "dispatch-binding"
    action: ActionRequest
    lease: Lease
    normalized_worker_request_sha256: str
    profile_sha256: str
    authority_config_sha256: str
    call_id: str
    reservation_id: str
    reserved_units: int

    def __post_init__(self) -> None:
        Record.__post_init__(self)
        require(self.lease.worker_id == self.action.actor, "Lease actor differs from action")
        for value in (self.normalized_worker_request_sha256, self.profile_sha256,
                      self.authority_config_sha256):
            sha256(value)
        identifier(self.call_id)
        identifier(self.reservation_id)
        _integer(self.reserved_units)


@dataclass(frozen=True, slots=True)
class DispatchReply(Record):
    KIND: ClassVar[str] = "dispatch-reply"
    request_id: str
    action_sha256: str
    state: str
    reason: str
    binding: DispatchBinding | None = None
    result_payload_sha256: str | None = None
    usage_units: int | None = None

    def __post_init__(self) -> None:
        Record.__post_init__(self)
        identifier(self.request_id)
        sha256(self.action_sha256)
        _text(self.reason)
        require(self.state in ("waiting", "pending", "publication_pending", "completed", "failed", "unknown"),
                "Unknown dispatch state")
        if self.state == "waiting":
            require(self.binding is None and self.result_payload_sha256 is None and self.usage_units is None,
                    "Waiting work cannot carry admission or usage")
            return
        require(self.binding is not None, "Admitted state requires full binding")
        assert self.binding is not None
        require(self.request_id == self.binding.action.request_id
                and self.action_sha256 == identity(self.binding.action), "Reply action binding differs")
        if self.result_payload_sha256 is not None:
            sha256(self.result_payload_sha256)
        if self.usage_units is not None:
            _integer(self.usage_units, 0, self.binding.reserved_units)
        if self.state == "pending":
            require(self.result_payload_sha256 is None and self.usage_units is None, "Pending state has an outcome")
        elif self.state in ("completed", "failed"):
            require(self.result_payload_sha256 is not None and self.usage_units is not None,
                    "Known terminal state requires result and usage")
        elif self.state == "publication_pending":
            require(self.result_payload_sha256 is not None, "Publication pending lacks durable result")
        else:
            require(self.usage_units is None, "Unknown state cannot invent known usage")


@dataclass(frozen=True, slots=True)
class CandidateOffer(Record):
    KIND: ClassVar[str] = "candidate-offer"
    dispatch: DispatchBinding
    commit_oid: str
    source_sha256: str
    result_payload_sha256: str
    bundle_manifest_sha256: str
    bundle_ref: EvidenceRef

    def __post_init__(self) -> None:
        Record.__post_init__(self)
        require(self.dispatch.action.kind in ("build", "repair"), "Candidate needs a coding action")
        require(_OID.fullmatch(self.commit_oid) is not None, "Expected SHA1 Git commit")
        for value in (self.source_sha256, self.result_payload_sha256, self.bundle_manifest_sha256):
            sha256(value)
        require(self.bundle_ref.producer == self.dispatch.action.actor
                and self.bundle_ref.kind == "candidate-bundle", "Candidate bundle ownership differs")


@dataclass(frozen=True, slots=True)
class SelectedOffer(Record):
    KIND: ClassVar[str] = "selected-offer"
    package_id: str
    offer_sha256: str

    def __post_init__(self) -> None:
        Record.__post_init__(self)
        require(self.package_id in PACKAGES, "Unknown selected package")
        sha256(self.offer_sha256)


@dataclass(frozen=True, slots=True)
class SelectionManifest(Record):
    KIND: ClassVar[str] = "selection-manifest"
    context: Context
    eligibility_policy_sha256: str
    selecting_dispatch: DispatchBinding
    local_view_sha256: str
    eligible_offer_sha256s: tuple[str, ...]
    missing_slots: tuple[str, ...]
    selected: tuple[SelectedOffer, ...]

    def __post_init__(self) -> None:
        Record.__post_init__(self)
        for value in (self.eligibility_policy_sha256, self.local_view_sha256):
            sha256(value)
        _hashes(self.eligible_offer_sha256s)
        _ids(self.missing_slots)
        _items(self.selected, limit=len(PACKAGES))
        require(self.selecting_dispatch.action.context == self.context
                and self.selecting_dispatch.action.kind in ("select_source", "select_tests"),
                "Selection action context or kind differs")
        require(self.selecting_dispatch.action.view_manifest_sha256 == self.local_view_sha256,
                "Selection uses another information set")
        require(len({part.package_id for part in self.selected}) == len(self.selected), "Repeated selected package")
        require(all(part.offer_sha256 in self.eligible_offer_sha256s for part in self.selected),
                "Selected offer is outside eligible information set")


@dataclass(frozen=True, slots=True)
class NamedSource(Record):
    KIND: ClassVar[str] = "named-source"
    path: str
    sha256: str

    def __post_init__(self) -> None:
        Record.__post_init__(self)
        _text(self.path, 512)
        require(bool(self.path) and not self.path.startswith("/") and "\\" not in self.path
                and not any(ord(c) < 32 for c in self.path)
                and all(part not in ("", ".", "..") and part.casefold() != ".git"
                        for part in self.path.split("/")), "Unsafe source path")
        sha256(self.sha256)


@dataclass(frozen=True, slots=True)
class ReleaseTarget(Record):
    KIND: ClassVar[str] = "release-target"
    context: Context
    generation: int
    repository_id: str
    protected_ref: str
    expected_head: str
    git_object_format: str
    commit_oid: str
    tree_oid: str
    sources: tuple[NamedSource, ...]
    selection_sha256: str
    required_suite_sha256: str
    evaluator_sha256: str
    execution_receipt_refs: tuple[EvidenceRef, ...]
    purpose: str = "public_release"
    independent_final_project_acceptance: bool = False

    def __post_init__(self) -> None:
        Record.__post_init__(self)
        _integer(self.generation, 0, 1_000_000)
        identifier(self.repository_id)
        require(self.git_object_format == "sha1", "Only qualified Git SHA1 objects are supported")
        require(all(_OID.fullmatch(value) is not None for value in
                    (self.expected_head, self.commit_oid, self.tree_oid)), "Invalid exact Git object")
        require(re.fullmatch(r"refs/heads/[A-Za-z0-9][A-Za-z0-9_/-]{0,120}", self.protected_ref) is not None
                and all(self.protected_ref.split("/")), "Invalid protected ref")
        _items(self.sources)
        require(bool(self.sources) and len({x.path for x in self.sources}) == len(self.sources),
                "Missing or duplicated named source")
        for value in (self.selection_sha256, self.required_suite_sha256, self.evaluator_sha256):
            sha256(value)
        _items(self.execution_receipt_refs)
        require(bool(self.execution_receipt_refs), "Release needs execution receipt references")
        require(self.purpose == "public_release" and self.independent_final_project_acceptance is False,
                "Public release is not independent final acceptance")


@dataclass(frozen=True, slots=True)
class ScopeVerdict(Record):
    KIND: ClassVar[str] = "scope-verdict"
    review_dispatch: DispatchBinding
    result_payload_sha256: str
    target_sha256: str
    scope_id: str
    covered_requirement_ids: tuple[str, ...]
    evidence_refs: tuple[EvidenceRef, ...]
    verdict: str
    rationale: str

    def __post_init__(self) -> None:
        Record.__post_init__(self)
        require(self.review_dispatch.action.kind == "review", "Verdict needs a review action")
        sha256(self.result_payload_sha256)
        sha256(self.target_sha256)
        identifier(self.scope_id)
        _ids(self.covered_requirement_ids)
        require(bool(self.covered_requirement_ids), "Scope verdict covers no requirement")
        _items(self.evidence_refs)
        require(self.verdict in VERDICTS, "Unknown semantic verdict")
        _text(self.rationale)


@dataclass(frozen=True, slots=True)
class RoleSpec(Record):
    KIND: ClassVar[str] = "role-spec"
    actor: str
    role: str
    profile_id: str
    package_affinity: str

    def __post_init__(self) -> None:
        Record.__post_init__(self)
        identifier(self.actor)
        identifier(self.profile_id)
        require(self.role in ("builder", "reviewer"), "Infrastructure is not a cognitive role")
        require(self.package_affinity in (*PACKAGES, "all"), "Invalid package affinity")


@dataclass(frozen=True, slots=True)
class RoleRoster(Record):
    KIND: ClassVar[str] = "role-roster"
    execution_contract_sha256: str
    cohort_id: str
    roles: tuple[RoleSpec, ...]
    max_builder_calls: int
    max_reviewer_calls: int
    max_concurrent_calls: int

    def __post_init__(self) -> None:
        Record.__post_init__(self)
        sha256(self.execution_contract_sha256)
        identifier(self.cohort_id)
        _items(self.roles, limit=MAX_ROLES)
        require(bool(self.roles) and len({r.actor for r in self.roles}) == len(self.roles), "Invalid role roster")
        for value in (self.max_builder_calls, self.max_reviewer_calls):
            _integer(value, 1, 1_000_000)
        _integer(self.max_concurrent_calls, 1, len(self.roles))


RECORDS = (Context, WorkKey, EvidenceRef, LocalViewManifest, ActionRequest, DispatchBinding,
           DispatchReply, CandidateOffer, SelectedOffer, SelectionManifest, NamedSource,
           ReleaseTarget, ScopeVerdict, RoleSpec, RoleRoster)
T = TypeVar("T", bound=Record)


def _json_value(value: Any, depth: int = 0) -> None:
    require(depth <= MAX_DEPTH, "JSON nesting limit exceeded")
    if type(value) in (str, int, bool, type(None)):
        return
    if type(value) is float:
        require(math.isfinite(value), "Nonfinite JSON value")
        return
    if type(value) is list:
        require(len(value) <= MAX_COLLECTION, "JSON array limit exceeded")
        for item in value:
            _json_value(item, depth + 1)
        return
    if type(value) is dict:
        require(len(value) <= MAX_COLLECTION and all(type(k) is str for k in value), "Invalid JSON object")
        for item in value.values():
            _json_value(item, depth + 1)
        return
    raise ContractError("Unsupported JSON value")


def canonical_bytes(value: Any) -> bytes:
    try:
        _json_value(value)
        raw = json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False,
                         separators=(",", ":")).encode("utf-8")
        require(len(raw) <= MAX_RECORD_BYTES, "Record byte limit exceeded")
        return raw
    except (UnicodeError, RecursionError, OverflowError, ValueError) as error:
        if isinstance(error, ContractError):
            raise
        raise ContractError("Invalid JSON encoding") from error


def _plain(value: Any) -> Any:
    if isinstance(value, Record):
        require(type(value) in RECORDS, "Unknown record class")
        return {field.name: _plain(getattr(value, field.name)) for field in fields(value)}
    if type(value) is Lease:
        _lease(value)
        return asdict(value)
    if type(value) is tuple:
        return [_plain(item) for item in value]
    return value


def to_dict(record: Record) -> dict[str, Any]:
    require(type(record) in RECORDS, "Unknown record class")
    value = _plain(record)
    canonical_bytes(value)
    return value


def _parse(value: Any, annotation: Any) -> Any:
    origin, args = get_origin(annotation), get_args(annotation)
    if origin is UnionType:
        for option in args:
            try:
                return _parse(value, option)
            except ContractError:
                pass
        raise ContractError("Wrong optional field type")
    if origin is tuple:
        require(type(value) is list and len(value) <= MAX_COLLECTION, "Expected bounded JSON array")
        return tuple(_parse(item, args[0]) for item in value)
    if annotation in RECORDS:
        return from_dict(annotation, value)
    if annotation is Lease:
        require(type(value) is dict and set(value) == {"task_id", "worker_id", "epoch", "expires_at"},
                "Invalid lease fields")
        result = Lease(**value)
        _lease(result)
        return result
    _typed(value, annotation)
    return value


def from_dict(cls: type[T], value: Any) -> T:
    require(cls in RECORDS and type(value) is dict, "Invalid record type or body")
    require(set(value) == {field.name for field in fields(cls)}, "Unknown or missing record fields")
    canonical_bytes(value)
    hints = _hints(cls)
    return cls(**{name: _parse(part, hints[name]) for name, part in value.items()})


def _contract(record: Record) -> str | None:
    if isinstance(record, (Context, RoleRoster)):
        return record.execution_contract_sha256
    context = getattr(record, "context", None)
    if type(context) is Context:
        return context.execution_contract_sha256
    if isinstance(record, DispatchBinding):
        return _contract(record.action)
    if isinstance(record, DispatchReply) and record.binding is not None:
        return _contract(record.binding)
    if isinstance(record, CandidateOffer):
        return _contract(record.dispatch)
    if isinstance(record, ScopeVerdict):
        return _contract(record.review_dispatch)
    return None


def identity(record: Record) -> str:
    """Pure record identity; component identity alone conveys no permission."""
    require(type(record) in RECORDS, "Unknown record class")
    body = {"protocol": PROTOCOL, "schema_version": SCHEMA_VERSION,
            "kind": record.KIND, "body": to_dict(record)}
    return hashlib.sha256(b"gossip-project-record\0" + canonical_bytes(body)).hexdigest()


def encode(record: Record, *, execution_contract_sha256: str | None = None) -> bytes:
    require(type(record) in RECORDS, "Unknown record class")
    actual = _contract(record)
    contract = execution_contract_sha256 if execution_contract_sha256 is not None else actual
    sha256(contract)
    require(actual is None or actual == contract, "Envelope execution contract differs")
    return canonical_bytes({"protocol": PROTOCOL, "schema_version": SCHEMA_VERSION,
                            "execution_contract_sha256": contract, "kind": record.KIND,
                            "body": to_dict(record)})


def strict_loads(data: bytes | str) -> Any:
    require(type(data) in (bytes, str), "JSON input must be bytes or text")
    try:
        raw = data.encode("utf-8") if isinstance(data, str) else data
        require(len(raw) <= MAX_RECORD_BYTES, "Record byte limit exceeded")

        def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
            result: dict[str, Any] = {}
            for key, value in items:
                require(key not in result, "Duplicate JSON member")
                result[key] = value
            return result

        def constant(value: str) -> Any:
            raise ContractError("Nonfinite JSON constant")

        value = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs, parse_constant=constant)
        canonical_bytes(value)
        return value
    except (UnicodeError, ValueError, RecursionError) as error:
        if isinstance(error, ContractError):
            raise
        raise ContractError("Invalid JSON") from error


def decode(data: bytes | str, cls: type[T], *, expected_contract_sha256: str) -> T:
    sha256(expected_contract_sha256)
    value = strict_loads(data)
    require(type(value) is dict and set(value) == {"protocol", "schema_version", "kind",
            "execution_contract_sha256", "body"}, "Invalid record envelope")
    require(value["protocol"] == PROTOCOL and type(value["schema_version"]) is int
            and value["schema_version"] == SCHEMA_VERSION and cls in RECORDS
            and value["kind"] == cls.KIND, "Wrong record protocol or kind")
    require(value["execution_contract_sha256"] == expected_contract_sha256, "Wrong execution contract")
    result = from_dict(cls, value["body"])
    require(_contract(result) in (None, expected_contract_sha256), "Nested execution contract differs")
    return result


def lease_digest(lease: Lease) -> str:
    _lease(lease)
    return hashlib.sha256(b"gossip-project-lease\0" + canonical_bytes(asdict(lease))).hexdigest()


def worker_request_digest(request: WorkerRequest) -> str:
    """Validate the legacy worker's pure request shape before canonical hashing.

    Read-only context files need not be in allowed_paths. Provider-specific
    request envelope size and profile limits still belong to OpenAIWorker.
    """
    require(type(request) is WorkerRequest, "Expected existing WorkerRequest")
    for part in (request.task_id, request.instructions, request.base_sha):
        require(type(part) is str and bool(part), "Worker identity and instructions must be nonempty text")
    require(type(request.feedback) is str, "Worker feedback must be text")
    _integer(request.attempt, 1)
    require(type(request.allowed_paths) is tuple and 0 < len(request.allowed_paths) <= MAX_COLLECTION,
            "Worker paths must be a nonempty bounded tuple")
    require(all(type(path) is str and _path_valid(path) for path in request.allowed_paths),
            "Invalid worker writable path")
    require(len(set(request.allowed_paths)) == len(request.allowed_paths), "Duplicate worker writable path")
    require(type(request.files) is dict and len(request.files) <= MAX_COLLECTION, "Invalid worker files")
    require(all(type(path) is str and _path_valid(path) and type(content) is str
                for path, content in request.files.items()), "Invalid worker context file")
    value = asdict(request)
    value["allowed_paths"] = list(request.allowed_paths)
    return hashlib.sha256(b"gossip-project-worker-request\0" + canonical_bytes(value)).hexdigest()


def resolve_local(ref: EvidenceRef, arrived: tuple[EvidenceRef, ...], payload: bytes | None) -> bytes:
    """Validate supplied local material; caller must obtain it from its own store."""
    require(type(ref) is EvidenceRef and type(arrived) is tuple, "Invalid local evidence inputs")
    require(all(type(item) is EvidenceRef for item in arrived), "Invalid arrived evidence member")
    matching = [item for item in arrived if item.event_id == ref.event_id]
    require(len(matching) == 1 and matching[0] == ref, "Evidence did not arrive with matching provenance")
    require(type(payload) is bytes and hashlib.sha256(payload).hexdigest() == ref.payload_sha256,
            "Local payload is missing or has changed")
    assert isinstance(payload, bytes)
    return payload
