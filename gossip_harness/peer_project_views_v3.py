"""Locally reconstructed project views over the unchanged durable role lifecycle.

The worker sees every declared evidence byte in a closed, versioned recipe.
Receivers reconstruct that recipe using their own arrived mesh material and the
financial authority's actual WorkerRequest. This proves materialization, not Git
authenticity or acceptance: the trusted project evidence registry supplies those
separate checks. Missing bytes wait; malformed or inconsistent bytes fail closed.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
import time
from types import MappingProxyType
from typing import Any, Callable, Mapping, TypeVar

from . import peer_role_loop_v2
from .peer_project_contract_v2 import (
    ACTION_KINDS, MAX_RECORD_BYTES, CandidateOffer, Context, DispatchBinding, EvidenceRef, LocalViewManifest,
    NamedSource, Record, ReleaseTarget, SelectionManifest, WorkKey, canonical_bytes,
    decode, from_dict, identity, resolve_local, sha256, strict_loads, to_dict,
    worker_request_digest,
)
from .peer_role_loop_v2 import (
    FinancialRPC, LocalMesh, RoleError, RoleLoop, WorkDirective, directive_id,
    financial_task_id,
)
from .worker import WorkerRequest, _path_valid

PROTOCOL = "peer-project-views-v3"
FRONTIER_PROTOCOL = "peer-project-frontier-v3"
CONTRIBUTION_PROTOCOL = "peer-project-contribution-v3"
T = TypeVar("T", bound=Record)


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _closed(value: Any, fields: set[str], description: str) -> dict[str, Any]:
    if type(value) is not dict or set(value) != fields:
        raise RoleError("Invalid closed " + description)
    return value


def _files(value: Any) -> dict[str, str]:
    if (type(value) is not dict or not value or len(value) > 256
            or any(type(path) is not str or not _path_valid(path) or type(body) is not str
                   for path, body in value.items())):
        raise RoleError("Project source needs complete ordinary text files")
    return dict(value)


def _oid(value: Any) -> str:
    if (type(value) is not str or len(value) != 40
            or any(char not in "0123456789abcdef" for char in value)):
        raise RoleError("Project source needs an exact SHA1 commit")
    return value


@dataclass(frozen=True, slots=True)
class ProjectSource:
    files: dict[str, str]
    base_sha: str

    @classmethod
    def parse(cls, raw: bytes) -> ProjectSource:
        value = _closed(strict_loads(raw), {"files", "base_sha"}, "project source")
        return cls(_files(value["files"]), _oid(value["base_sha"]))


@dataclass(frozen=True, slots=True)
class CandidateContribution:
    offer_sha256: str
    commit_oid: str
    files: dict[str, str]

    @classmethod
    def parse(cls, raw: bytes) -> CandidateContribution:
        value = _closed(strict_loads(raw), {"protocol", "offer_sha256", "commit_oid", "files"},
                        "candidate contribution")
        if value["protocol"] != CONTRIBUTION_PROTOCOL:
            raise RoleError("Wrong contribution protocol")
        sha256(value["offer_sha256"])
        return cls(value["offer_sha256"], _oid(value["commit_oid"]), _files(value["files"]))


@dataclass(frozen=True, slots=True)
class FrontierOffer:
    slot: WorkKey
    actor: str
    offer_sha256: str
    offer_ref: EvidenceRef
    contribution_ref: EvidenceRef


@dataclass(frozen=True, slots=True)
class SelectionFrontier:
    context: Context
    eligibility_policy_sha256: str
    offers: tuple[FrontierOffer, ...]

    @classmethod
    def parse(cls, raw: bytes) -> SelectionFrontier:
        value = _closed(strict_loads(raw), {"protocol", "context", "eligibility_policy_sha256",
                                          "offers", "missing_slots"}, "selection frontier")
        if (value["protocol"] != FRONTIER_PROTOCOL or type(value["missing_slots"]) is not list
                or value["missing_slots"] or type(value["offers"]) is not list
                or not 1 <= len(value["offers"]) <= 64):
            raise RoleError("Only a complete bounded selection frontier is qualified")
        context = from_dict(Context, value["context"])
        sha256(value["eligibility_policy_sha256"])
        offers = []
        for item in value["offers"]:
            item = _closed(item, {"slot", "actor", "offer_sha256", "offer_ref", "contribution_ref"},
                           "frontier offer")
            sha256(item["offer_sha256"])
            offers.append(FrontierOffer(from_dict(WorkKey, item["slot"]), item["actor"],
                item["offer_sha256"], from_dict(EvidenceRef, item["offer_ref"]),
                from_dict(EvidenceRef, item["contribution_ref"])))
        keys = [(entry.slot.package_id, entry.slot.slot_id, entry.actor) for entry in offers]
        if (any(type(entry.actor) is not str for entry in offers) or keys != sorted(keys)
                or len(set(keys)) != len(keys)
                or len({entry.offer_sha256 for entry in offers}) != len(offers)):
            raise RoleError("Ambiguous or unordered frontier membership")
        return cls(context, value["eligibility_policy_sha256"], tuple(offers))


def _local_bodies(directive: WorkDirective, mesh: LocalMesh) -> tuple[bytes, ...] | None:
    arrived, bodies, missing = mesh.arrived(), [], False
    total_bytes = 0
    for ref in directive.required_refs:
        if ref not in arrived:
            mesh.want(ref)
            missing = True
            continue
        raw = mesh.resolve(ref)
        if raw is None:
            mesh.want(ref)
            missing = True
            continue
        total_bytes += len(raw)
        if total_bytes > MAX_RECORD_BYTES:
            # Every raw byte will be embedded in the recipe. Reject an
            # impossible view before resolving and copying later large objects;
            # canonical_bytes still enforces escaping/envelope overhead below.
            raise RoleError("Complete cognitive evidence exceeds the record byte limit")
        bodies.append(resolve_local(ref, arrived, raw))
    return None if missing else tuple(bodies)


def _record(ref: EvidenceRef, raw: bytes, cls: type[T], contract: str) -> T:
    # A normalized candidate/selection record is distinct from an original
    # candidate notice carrying a bundle manifest. Its producer is the service.
    if ref.kind != cls.KIND:
        raise RoleError("Wrong normalized project record kind")
    record = decode(raw, cls, expected_contract_sha256=contract)
    from .peer_project_contract_v2 import encode
    if encode(record) != raw:
        raise RoleError("Normalized record must use the exact canonical encoding")
    return record


def _candidate_materials(directive: WorkDirective, bodies: tuple[bytes, ...]) -> tuple[
        dict[str, tuple[EvidenceRef, CandidateOffer]], dict[str, tuple[EvidenceRef, CandidateContribution]]]:
    offers, contributions = {}, {}
    for ref, raw in zip(directive.required_refs, bodies, strict=True):
        if ref.kind == "candidate-offer":
            offer = _record(ref, raw, CandidateOffer, directive.context.execution_contract_sha256)
            key = identity(offer)
            if key in offers or offer.dispatch.action.context != directive.context:
                raise RoleError("Duplicate or foreign candidate in local view")
            offers[key] = ref, offer
        elif ref.kind == "candidate-contribution":
            contribution = CandidateContribution.parse(raw)
            if contribution.offer_sha256 in contributions:
                raise RoleError("Duplicate candidate contribution")
            contributions[contribution.offer_sha256] = ref, contribution
    if set(offers) != set(contributions):
        raise RoleError("Every candidate record needs its actual owned source bytes")
    for key, (_, offer) in offers.items():
        if contributions[key][1].commit_oid != offer.commit_oid:
            raise RoleError("Candidate contribution names another commit")
    return offers, contributions


def _source(directive: WorkDirective, bodies: tuple[bytes, ...], policy_sha256: str) -> ProjectSource:
    contract = directive.context.execution_contract_sha256
    refs_and_bodies = tuple(zip(directive.required_refs, bodies, strict=True))
    offers, contributions = _candidate_materials(directive, bodies)
    if directive.kind == "review":
        if directive.allowed_paths != ("review.json",):
            raise RoleError("Review may only write review.json")
        target = _record(directive.source_ref, bodies[0], ReleaseTarget, contract)
        if (target.context != directive.context or target.generation != directive.work.generation):
            raise RoleError("Review target has another context or generation")
        sources = [ProjectSource.parse(raw) for ref, raw in refs_and_bodies if ref.kind == "project-source"]
        selections = [_record(ref, raw, SelectionManifest, contract)
                      for ref, raw in refs_and_bodies if ref.kind == "selection-manifest"]
        if len(sources) != 1 or len(selections) != 1:
            raise RoleError("Review needs one whole source packet and one selection")
        source, selection = sources[0], selections[0]
        named = tuple(NamedSource(path, _sha(text.encode("utf-8"))) for path, text in sorted(source.files.items()))
        if (source.base_sha != target.commit_oid or named != target.sources
                or selection.context != target.context or identity(selection) != target.selection_sha256
                or any(ref not in directive.evidence_refs for ref in target.execution_receipt_refs)):
            raise RoleError("Review source, selection or execution evidence differs from target")
        selected = {part.offer_sha256 for part in selection.selected}
        if not selected <= set(offers):
            raise RoleError("Review omits selected candidate source evidence")
        selected_paths: set[str] = set()
        for key in selected:
            files = contributions[key][1].files
            if (selected_paths.intersection(files)
                    or any(source.files.get(path) != text for path, text in files.items())):
                raise RoleError("Selected package source differs from the target or overlaps")
            selected_paths.update(files)
        return source
    if directive.source_ref.kind != "project-source":
        raise RoleError("Nonreview project work needs a project-source packet")
    source = ProjectSource.parse(bodies[0])
    if directive.kind == "select_source":
        if directive.allowed_paths != ("selection.json",):
            raise RoleError("Source selection may only write selection.json")
        frontiers = [SelectionFrontier.parse(raw) for ref, raw in refs_and_bodies
                     if ref.kind == "selection-frontier"]
        if len(frontiers) != 1 or frontiers[0].context != directive.context:
            raise RoleError("Source selector needs its exact complete frontier")
        frontier = frontiers[0]
        if frontier.eligibility_policy_sha256 != policy_sha256:
            raise RoleError("Source frontier uses another registered eligibility policy")
        if set(offers) != {entry.offer_sha256 for entry in frontier.offers}:
            raise RoleError("Materialized candidates differ from the complete frontier")
        for entry in frontier.offers:
            ref, offer = offers[entry.offer_sha256]
            contribution_ref, _ = contributions[entry.offer_sha256]
            if (ref != entry.offer_ref or contribution_ref != entry.contribution_ref
                    or offer.dispatch.action.work != entry.slot or offer.dispatch.action.actor != entry.actor):
                raise RoleError("Frontier candidate identity or source reference differs")
    return source


def materialize_project(directive: WorkDirective, mesh: LocalMesh, policy_sha256: str) -> tuple[
        WorkerRequest, LocalViewManifest] | None:
    """Create exact input from locally arrived bytes; never truncate evidence."""
    sha256(policy_sha256)
    bodies = _local_bodies(directive, mesh)
    if bodies is None:
        return None
    source = _source(directive, bodies, policy_sha256)
    try:
        materials = [{"ref": to_dict(ref), "utf8": raw.decode("utf-8")}
                     for ref, raw in zip(directive.required_refs, bodies, strict=True)]
    except UnicodeError as error:
        raise RoleError("Project cognitive evidence must be complete UTF8 text") from error
    # The directive includes the caller's feedback verbatim. The surrounding
    # schema makes evidence data explicit and gives the receiver a replay recipe.
    feedback = canonical_bytes({"protocol": PROTOCOL, "directive": directive.to_dict(),
                                "materials": materials}).decode("utf-8")
    request = WorkerRequest(financial_task_id(directive.context, directive.work), directive.instructions,
        directive.allowed_paths, source.files, source.base_sha, directive.attempt, feedback)
    digest = worker_request_digest(request)
    view = LocalViewManifest(directive.context, policy_sha256, directive.source_ref,
        directive.evidence_refs, tuple(ref.event_id for ref in directive.required_refs), (), digest)
    view.require_complete()
    return request, view


def materialization_directive(request: WorkerRequest) -> WorkDirective:
    """Read a recipe only from the authority's authenticated actual request."""
    worker_request_digest(request)
    recipe = _closed(strict_loads(request.feedback), {"protocol", "directive", "materials"}, "view recipe")
    if recipe["protocol"] != PROTOCOL:
        raise RoleError("Worker input uses another materialization protocol")
    return WorkDirective.from_dict(recipe["directive"])


def verify_materialized_view(binding: DispatchBinding, request: WorkerRequest, claimed_view: LocalViewManifest,
                             mesh: LocalMesh, *, policy_sha256: str,
                             expected_instructions_sha256: str | None = None,
                             expected_attempt_limit: int | None = None) -> LocalViewManifest:
    """Verify the authority's actual request, never a peer's claimed prompt.

    The caller authenticates ``binding``/``request`` with verified_terminal and
    the claimed manifest with an arrived role-result. This function independently
    checks every referenced byte and recreates the exact normalized worker input.
    The optional pair of expected template/allowance values binds trusted host
    registration. If omitted, the host must perform those policy checks itself.
    Candidate ownership and normalized publication authenticity must also be
    checked against the receiver's contribution registry, not inferred here.
    """
    if (type(binding) is not DispatchBinding or type(claimed_view) is not LocalViewManifest
            or worker_request_digest(request) != binding.normalized_worker_request_sha256):
        raise RoleError("View verification needs the exact dispatched worker request")
    directive = materialization_directive(request)
    if (expected_instructions_sha256 is None) != (expected_attempt_limit is None):
        raise RoleError("Template and attempt allowance must be registered together")
    if expected_instructions_sha256 is not None:
        sha256(expected_instructions_sha256)
        if (type(expected_attempt_limit) is not int
                or _sha(directive.instructions.encode("utf-8")) != expected_instructions_sha256
                or directive.attempt_limit != expected_attempt_limit):
            raise RoleError("Materialization differs from the registered template or attempt allowance")
    action = binding.action
    key = directive_id(directive)
    if ((action.context, action.work, action.profile_id, action.kind, action.action_id, action.request_id) !=
            (directive.context, directive.work, directive.profile_id, directive.kind,
             "action-" + key, "dispatch-" + key)):
        raise RoleError("Materialization recipe belongs to another action")
    materialized = materialize_project(directive, mesh, policy_sha256)
    if materialized is None:
        raise RoleError("Required material has not arrived at the verifying receiver")
    actual_request, actual_view = materialized
    if (actual_request != request or actual_view != claimed_view
            or identity(actual_view) != action.view_manifest_sha256):
        raise RoleError("Claimed local view differs from the exact materialized worker input")
    return actual_view


def _policy_registration(view_policies: Mapping[tuple[str, int], str] | None) -> list[dict[str, Any]] | None:
    if view_policies is None:
        return None
    if not 1 <= len(view_policies) <= 256:
        raise RoleError("Invalid bounded view policy registration")
    for key, digest in view_policies.items():
        if (type(key) is not tuple or len(key) != 2 or key[0] not in ACTION_KINDS
                or type(key[1]) is not int or not 0 <= key[1] <= 1_000_000):
            raise RoleError("Invalid registered action kind or generation")
        sha256(digest)
    return [{"kind": kind, "generation": generation, "policy_sha256": view_policies[kind, generation]}
            for kind, generation in sorted(view_policies)]


def effective_runtime_policy_sha256(caller_policy_sha256: str, *,
        view_policies: Mapping[tuple[str, int], str] | None = None) -> str:
    """Bind this implementation and the inherited lifecycle to the journal.

    The RoleLoop journal separately binds actor, limits and timing configuration.
    The view retains the caller's scientific review-policy digest, as required by
    the release gate; these are deliberately distinct policy identities.
    """
    sha256(caller_policy_sha256)
    return _sha(canonical_bytes({"protocol": PROTOCOL, "caller_policy_sha256": caller_policy_sha256,
        "view_policies": _policy_registration(view_policies),
        "implementation_sha256": _sha(Path(__file__).read_bytes()),
        "role_loop_sha256": _sha(Path(peer_role_loop_v2.__file__).read_bytes())}))


class ProjectRoleLoopV3(RoleLoop):
    """Versioned materializer using RoleLoop's tested durable action lifecycle."""

    def __init__(self, root: Path, actor: str, mesh: LocalMesh, financial_client: FinancialRPC, *,
                 call_limit: int, policy_sha256: str, result_producer: str,
                 lease_ttl: float = 60, renew_margin: float = 5, max_renewals: int = 3,
                 max_actions: int = 256, clock: Callable[[], float] = time.time,
                 crash_hook: Callable[[str, str], None] | None = None,
                 view_policies: Mapping[tuple[str, int], str] | None = None):
        self.view_policy_sha256 = policy_sha256
        self.view_policies = MappingProxyType(dict(view_policies)) if view_policies is not None else None
        super().__init__(root, actor, mesh, financial_client, call_limit=call_limit,
            policy_sha256=effective_runtime_policy_sha256(policy_sha256, view_policies=self.view_policies),
            result_producer=result_producer,
            lease_ttl=lease_ttl, renew_margin=renew_margin, max_renewals=max_renewals,
            max_actions=max_actions, clock=clock, crash_hook=crash_hook)

    def _materialize(self, directive: WorkDirective) -> tuple[WorkerRequest, LocalViewManifest] | None:
        policy = self.view_policy_sha256
        if self.view_policies is not None:
            key = directive.kind, directive.work.generation
            if key not in self.view_policies:
                raise RoleError("Action has no registered kind/generation view policy")
            policy = self.view_policies[key]
        return materialize_project(directive, self.mesh, policy)
