"""Pure exact-target public-review gate, with explicit trusted adapter seams.

This module neither executes work nor promotes Git. A successful receipt only
means the supplied *current trusted snapshot* is eligible for a separately
fenced, exact-head Git CAS. The host must persist suite/refutation history and
prevent rollback, authenticate contribution membership and materialized local
views, and verify retained validator artifacts. Callbacks below are internal
host capabilities, never peer-controlled booleans or RPC parameters.

In particular, ``verified_terminal`` must be the financial authority's retained
journal/accounting verifier. Every submitted ScopeVerdict is reconstructed from
that actual successful WorkerResult, not accepted as an assertion. One response
can contain two scope decisions but remains one call and one reviewer identity.
Public review is not independent final project acceptance.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
from typing import Any, Callable

from .peer_project_contract_v2 import (
    PACKAGES, CandidateOffer, Context, DispatchBinding, DispatchReply, EvidenceRef,
    LocalViewManifest, NamedSource, ReleaseTarget, ScopeVerdict, SelectionManifest,
    canonical_bytes, encode, from_dict, identifier, identity, require, sha256,
    strict_loads, to_dict, worker_request_digest,
)
from .worker import WorkerRequest, WorkerResult

PROTOCOL = "peer-scoped-review-v2"
SLOTS = (("R1", "catalog"), ("R1", "clients"), ("R2", "catalog"),
         ("R2", "ingestion"), ("R3", "ingestion"), ("R3", "query"),
         ("R4", "query"), ("R4", "clients"))


def _json(value: Any) -> Any:
    if isinstance(value, tuple):
        return [_json(item) for item in value]
    if isinstance(value, dict):
        return {key: _json(item) for key, item in value.items()}
    return value


def _digest(kind: str, value: Any) -> str:
    return hashlib.sha256(b"gossip-review-gate-v2\0" + canonical_bytes(
        {"kind": kind, "body": _json(value)})).hexdigest()


def _tuple(values: tuple, cls: type, *, nonempty: bool = True) -> None:
    require(type(values) is tuple and len(values) <= 256, "Expected bounded tuple")
    require(not nonempty or bool(values), "Empty required collection")
    require(all(type(item) is cls for item in values), "Wrong collection member type")
    require(len(set(values)) == len(values), "Duplicate collection member")


def _ids(values: tuple[str, ...]) -> None:
    _tuple(values, str)
    for value in values:
        identifier(value)


@dataclass(frozen=True, slots=True)
class RequiredCheck:
    """A trusted admitted assertion; its immutable contents are content-bound."""

    check_id: str
    assertion_sha256: str

    def __post_init__(self) -> None:
        identifier(self.check_id)
        sha256(self.assertion_sha256)


@dataclass(frozen=True, slots=True)
class RequiredSuite:
    context: Context
    checks: tuple[RequiredCheck, ...]

    def __post_init__(self) -> None:
        require(type(self.context) is Context, "Invalid suite context")
        _tuple(self.checks, RequiredCheck)
        require(len({item.check_id for item in self.checks}) == len(self.checks), "Repeated check ID")


def suite_digest(suite: RequiredSuite) -> str:
    require(type(suite) is RequiredSuite, "Invalid required suite")
    return _digest("required-suite", asdict(suite))


def extend_required_suite(previous: RequiredSuite, additions: tuple[RequiredCheck, ...]) -> RequiredSuite:
    """Append admitted checks without changing, dropping or reordering old ones."""
    _tuple(additions, RequiredCheck)
    return RequiredSuite(previous.context, previous.checks + additions)


@dataclass(frozen=True, slots=True)
class ScopeRule:
    reviewer: str
    scope_id: str
    required_requirement_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        require((self.reviewer, self.scope_id) in SLOTS, "Unregistered reviewer/scope slot")
        _ids(self.required_requirement_ids)


@dataclass(frozen=True, slots=True)
class ReviewerProfile:
    reviewer: str
    profile_id: str
    profile_sha256: str

    def __post_init__(self) -> None:
        require(self.reviewer in ("R1", "R2", "R3", "R4"), "Unknown reviewer")
        identifier(self.profile_id)
        sha256(self.profile_sha256)


@dataclass(frozen=True, slots=True)
class ReviewPolicy:
    """Frozen host registration, including explicit diagonal review obligations."""

    context: Context
    slots: tuple[ScopeRule, ...]
    profiles: tuple[ReviewerProfile, ...]
    required_requirement_ids: tuple[str, ...]
    storage_query_requirement_ids: tuple[str, ...]
    ingestion_client_requirement_ids: tuple[str, ...]
    eligibility_policy_sha256: str
    initial_suite_sha256: str
    execution_provenance_sha256: str
    authority_config_sha256: str
    package_generations: tuple[tuple[str, int], ...]
    inherited_sources: tuple[NamedSource, ...] = ()

    def __post_init__(self) -> None:
        require(type(self.context) is Context, "Invalid policy context")
        _tuple(self.slots, ScopeRule)
        _tuple(self.profiles, ReviewerProfile)
        require(tuple((slot.reviewer, slot.scope_id) for slot in self.slots) == SLOTS,
                "All eight ordered reviewer/scope slots must be registered")
        require(tuple(profile.reviewer for profile in self.profiles) == ("R1", "R2", "R3", "R4"),
                "Four distinct reviewer profiles must be registered")
        require(len({(p.profile_id, p.profile_sha256) for p in self.profiles}) == 1,
                "All reviewers must use the same registered stronger profile")
        for values in (self.required_requirement_ids, self.storage_query_requirement_ids,
                       self.ingestion_client_requirement_ids):
            _ids(values)
        required = set(self.required_requirement_ids)
        require(all(set(slot.required_requirement_ids) <= required for slot in self.slots),
                "Scope contains an unregistered requirement")
        for requirement in required:
            reviewers = {slot.reviewer for slot in self.slots if requirement in slot.required_requirement_ids}
            require(len(reviewers) >= 2, "Each requirement needs two named reviewers")
        for requirements, endpoints in (
                (self.storage_query_requirement_ids, {("R1", "catalog"), ("R3", "query")}),
                (self.ingestion_client_requirement_ids, {("R2", "ingestion"), ("R4", "clients")})):
            require(set(requirements) <= required, "Unregistered diagonal requirement")
            for requirement in requirements:
                covered = {(slot.reviewer, slot.scope_id) for slot in self.slots
                           if requirement in slot.required_requirement_ids}
                require(endpoints <= covered, "Diagonal requirement lacks its named endpoint scopes")
        for digest in (self.eligibility_policy_sha256, self.initial_suite_sha256, self.execution_provenance_sha256,
                       self.authority_config_sha256):
            sha256(digest)
        _tuple(self.package_generations, tuple)
        require(all(len(item) == 2 and type(item[1]) is int and 0 <= item[1] <= 1_000_000
                    for item in self.package_generations), "Invalid selected package generation")
        require(tuple(item[0] for item in self.package_generations) == PACKAGES,
                "All four selected package generations must be registered")
        _tuple(self.inherited_sources, NamedSource, nonempty=False)
        require(len({source.path for source in self.inherited_sources}) == len(self.inherited_sources),
                "Repeated inherited source path")


def policy_digest(policy: ReviewPolicy) -> str:
    require(type(policy) is ReviewPolicy, "Invalid review policy")
    return _digest("policy", asdict(policy))


@dataclass(frozen=True, slots=True)
class AdmittedRefutation:
    """A host-admitted unresolved refutation blocks its exact affected target."""

    target_sha256: str
    scope_ids: tuple[str, ...]
    evidence_ref: EvidenceRef

    def __post_init__(self) -> None:
        sha256(self.target_sha256)
        _ids(self.scope_ids)
        require(set(self.scope_ids) <= set(PACKAGES), "Unknown refuted scope")
        require(type(self.evidence_ref) is EvidenceRef, "Invalid refutation evidence")


@dataclass(frozen=True, slots=True)
class PublicExecutionEvidence:
    """Normalized *verified* existing-validator evidence from an internal adapter.

    The adapter authenticates original artifacts, actual source bytes, execution
    or valid reuse, and runtime/image/environment/limits/seed/protocol bound by
    provenance_sha256. Constructing this record alone is not authentication.
    """

    context: Context
    commit_oid: str
    tree_oid: str
    sources: tuple[NamedSource, ...]
    suite_sha256: str
    evaluator_sha256: str
    provenance_sha256: str
    receipt_refs: tuple[EvidenceRef, ...]
    check_results: tuple[tuple[str, str], ...]
    purpose: str = "public_release"

    def __post_init__(self) -> None:
        require(type(self.context) is Context, "Invalid execution context")
        _tuple(self.sources, NamedSource)
        _tuple(self.receipt_refs, EvidenceRef)
        for value in (self.suite_sha256, self.evaluator_sha256, self.provenance_sha256):
            sha256(value)
        require(type(self.commit_oid) is str and len(self.commit_oid) == 40
                and type(self.tree_oid) is str and len(self.tree_oid) == 40, "Invalid execution object")
        _tuple(self.check_results, tuple)
        for item in self.check_results:
            require(len(item) == 2, "Invalid check outcome")
            identifier(item[0])
            require(item[1] in ("passed", "failed", "unknown", "infrastructure_failure"),
                    "Unknown check outcome")
        require(self.purpose == "public_release", "Independent acceptance cannot authorize public release")


VerifiedTerminal = Callable[[DispatchBinding], dict[str, Any]]
VerifiedView = Callable[[DispatchBinding], LocalViewManifest]
VerifiedContribution = Callable[[CandidateOffer], tuple[NamedSource, ...]]
VerifiedExecution = Callable[[ReleaseTarget, RequiredSuite], PublicExecutionEvidence]
VerifiedSelection = Callable[[SelectionManifest], SelectionManifest]


def _terminal(binding: DispatchBinding, verified_terminal: VerifiedTerminal) -> tuple[WorkerResult, str]:
    proof = verified_terminal(binding)
    require(type(proof) is dict and set(proof) == {"binding", "action", "reply", "result", "result_payload", "worker_request"},
            "Invalid trusted terminal proof")
    reply, result, request = proof["reply"], proof["result"], proof["worker_request"]
    require(proof["binding"] == binding and proof["action"] == binding.action,
            "Terminal proof belongs to another dispatch")
    require(type(reply) is DispatchReply and reply.binding == binding and reply.state == "completed",
            "Review requires this exact successful accounted outcome")
    require(type(request) is WorkerRequest and request.allowed_paths == ("review.json",)
            and worker_request_digest(request) == binding.normalized_worker_request_sha256,
            "Review request is not the exact bound data-artifact request")
    require(type(result) is WorkerResult and type(result.changes) is dict
            and set(result.changes) == {"review.json"} and type(result.changes["review.json"]) is str,
            "Review result must contain exactly the review data artifact")
    require(type(result.usage_units) is int and result.usage_units == reply.usage_units,
            "Review result accounting differs")
    require(type(result.metadata) is dict and not result.metadata.get("halt"), "Halted result cannot approve")
    payload = {"kind": "result", "payload": asdict(result)}
    require(proof["result_payload"] == payload, "WorkerResult differs from retained payload")
    digest = hashlib.sha256(canonical_bytes(payload)).hexdigest()
    require(digest == reply.result_payload_sha256, "Retained response digest differs")
    return result, digest


def materialize_verdicts(binding: DispatchBinding, target: ReleaseTarget,
                         verified_terminal: VerifiedTerminal) -> tuple[ScopeVerdict, ...]:
    """Parse the entire actual response; malformed output never repairs itself.

    WorkerResult.changes["review.json"] is strict JSON with exactly ``protocol``,
    ``target_sha256`` and ``verdicts``. Each verdict contains exactly the five
    semantic ScopeVerdict fields: scope_id, covered_requirement_ids,
    evidence_refs, verdict and rationale. Dispatch/result identity is supplied
    by the verified journal, avoiding self-referential model-generated hashes.
    A valid single scope can be retained; its absent sibling remains absent.
    """
    require(binding.action.kind == "review" and binding.action.context == target.context,
            "Review action belongs to another context")
    require(binding.action.work.generation == target.generation, "Stale review generation")
    result, result_digest = _terminal(binding, verified_terminal)
    response = result.changes["review.json"]
    assert isinstance(response, str)
    value = strict_loads(response)
    require(type(value) is dict and set(value) == {"protocol", "target_sha256", "verdicts"},
            "Invalid reviewer response fields")
    require(value["protocol"] == PROTOCOL and value["target_sha256"] == identity(target),
            "Reviewer response names another target or protocol")
    items = value["verdicts"]
    require(type(items) is list and 1 <= len(items) <= 2, "Expected one or two scoped decisions")
    verdicts = []
    for item in items:
        require(type(item) is dict and set(item) == {"scope_id", "covered_requirement_ids",
                "evidence_refs", "verdict", "rationale"}, "Invalid scoped response fields")
        verdict = from_dict(ScopeVerdict, {**item, "review_dispatch": to_dict(binding),
            "result_payload_sha256": result_digest, "target_sha256": identity(target)})
        require((binding.action.actor, verdict.scope_id) in SLOTS, "Review is outside assigned scopes")
        verdicts.append(verdict)
    require(len({item.scope_id for item in verdicts}) == len(verdicts), "Duplicate scoped decision")
    return tuple(verdicts)


def _bound_view(binding: DispatchBinding, verified_view: VerifiedView) -> LocalViewManifest:
    """Adapter verifies local arrival AND materialization into the bound request.

    Hash-consistent manifests alone do not establish that a model received the
    evidence. The host checks actual worker request bytes against this view's
    materialized_content_sha256 before returning it.
    """
    view = verified_view(binding)
    require(type(view) is LocalViewManifest and view.context == binding.action.context
            and identity(view) == binding.action.view_manifest_sha256, "Wrong materialized review view")
    view.require_complete()
    return view


def _contains(view: LocalViewManifest, refs: tuple[EvidenceRef, ...]) -> None:
    arrived = (view.source_ref, *view.evidence_refs)
    require(all(ref in arrived and ref.event_id in view.required_evidence_ids for ref in refs),
            "Required exact evidence is absent from materialized view")


def _record_ref(view: LocalViewManifest, record: Any, kind: str) -> EvidenceRef:
    digest = hashlib.sha256(encode(record)).hexdigest()
    matches = [ref for ref in (view.source_ref, *view.evidence_refs)
               if ref.kind == kind and ref.payload_sha256 == digest]
    require(len(matches) == 1, "Required immutable record content is absent or ambiguous")
    return matches[0]


def validate_selection(target: ReleaseTarget, selection: SelectionManifest,
                       offers: tuple[CandidateOffer, ...], policy: ReviewPolicy,
                       verified_contribution: VerifiedContribution,
                       verified_selection: VerifiedSelection) -> tuple[str, ...]:
    """Check exact selected membership using authenticated package-source bytes.

    The contribution adapter authenticates candidate journal/bundle provenance
    and returns all package-owned source hashes for the offer. It also checks
    candidate source digest encoding; this gate does not invent another Git or
    bundle implementation. The four disjoint memberships cover the whole target.
    Actual merged commit/tree equality is checked by the execution adapter.

    The mandatory selection adapter authenticates the exact successful selector
    journal response and its materialized eligible-set view, including the
    registered frontier and absent-slot policy. Merely echoing the argument
    would not implement this adapter. The returned immutable manifest must match
    exactly. Candidate generations are explicit policy per package, allowing an
    unchanged package only at its declared generation after a target refresh.
    """
    verified = verified_selection(selection)
    require(type(verified) is SelectionManifest and verified == selection,
            "Source selection lacks exact authenticated selector provenance")
    require(target.context == selection.context == policy.context, "Selection context differs")
    require(identity(selection) == target.selection_sha256, "Target names another selection")
    require(selection.eligibility_policy_sha256 == policy.eligibility_policy_sha256,
            "Unregistered source eligibility policy")
    require(selection.selecting_dispatch.action.kind == "select_source"
            and selection.selecting_dispatch.action.work.generation == target.generation,
            "Wrong selection kind or generation")
    require(selection.selecting_dispatch.authority_config_sha256 == policy.authority_config_sha256,
            "Selector authority provenance differs")
    require(tuple(item.package_id for item in selection.selected) == PACKAGES,
            "Selection must name all four packages in registered order")
    _tuple(offers, CandidateOffer)
    by_digest = {identity(offer): offer for offer in offers}
    require(set(by_digest) == set(selection.eligible_offer_sha256s), "Eligible source membership differs")
    generations = dict(policy.package_generations)
    sources, authors = list(policy.inherited_sources), []
    for selected in selection.selected:
        offer = by_digest[selected.offer_sha256]
        action = offer.dispatch.action
        require(action.context == target.context and action.work.package_id == selected.package_id,
                "Selected offer belongs to another package or context")
        require(offer.dispatch.authority_config_sha256 == policy.authority_config_sha256,
                "Selected candidate authority provenance differs")
        require(action.work.generation == generations[selected.package_id],
                "Selected offer has an incompatible package generation")
        membership = verified_contribution(offer)
        _tuple(membership, NamedSource)
        sources.extend(membership)
        authors.append(action.actor)
    require(len({source.path for source in sources}) == len(sources), "Selected source memberships overlap")
    expected = tuple(sorted(sources, key=lambda source: source.path))
    require(target.sources == expected, "Target source is not the exact whole-four-package selection")
    return tuple(sorted(set(authors)))


@dataclass(frozen=True, slots=True)
class ReviewGateReceipt:
    """Eligibility only: no promotion, model execution or independent acceptance."""

    target_sha256: str
    policy_sha256: str
    suite_sha256: str
    verdict_sha256s: tuple[str, ...]
    call_ids: tuple[str, ...]
    missing_slots: tuple[tuple[str, str], ...]
    rejected_slots: tuple[tuple[str, str], ...]
    blockers: tuple[str, ...]
    eligible: bool
    purpose: str = "public_release"
    independent_final_project_acceptance: bool = False


def receipt_digest(receipt: ReviewGateReceipt) -> str:
    return _digest("eligibility-receipt", asdict(receipt))


def evaluate_release(*, target: ReleaseTarget, selection: SelectionManifest,
                     offers: tuple[CandidateOffer, ...], policy: ReviewPolicy,
                     suite_history: tuple[RequiredSuite, ...], verdicts: tuple[ScopeVerdict, ...],
                     refutations: tuple[AdmittedRefutation, ...],
                     verified_terminal: VerifiedTerminal, verified_view: VerifiedView,
                     verified_contribution: VerifiedContribution,
                     verified_execution: VerifiedExecution,
                     verified_selection: VerifiedSelection) -> ReviewGateReceipt:
    """Evaluate a current trusted snapshot; never take mutable branch heads.

    The host supplies its entire retained admitted suite/refutation state and
    current review round. It must not omit old required checks or refutations,
    roll back its checkpoint, or cherry-pick earlier approvals from later
    request-changes responses. Duplicate slots in one snapshot fail closed.
    Changed target/base/requirements/suite/generation is a new exact identity;
    movements of unrelated working branches are intentionally irrelevant.
    """
    authors = validate_selection(target, selection, offers, policy, verified_contribution, verified_selection)
    _tuple(suite_history, RequiredSuite)
    require(suite_digest(suite_history[0]) == policy.initial_suite_sha256,
            "Required suite history has another initial checkpoint")
    for before, after in zip(suite_history, suite_history[1:]):
        require(before.context == after.context and len(after.checks) > len(before.checks)
                and after.checks[:len(before.checks)] == before.checks,
                "Required tests must grow monotonically without weakening admitted checks")
    suite = suite_history[-1]
    require(suite.context == target.context and suite_digest(suite) == target.required_suite_sha256,
            "Target does not bind latest required suite")
    execution = verified_execution(target, suite)
    require(type(execution) is PublicExecutionEvidence, "Missing trusted execution evidence")
    require((execution.context, execution.commit_oid, execution.tree_oid, execution.sources,
             execution.suite_sha256, execution.evaluator_sha256, execution.provenance_sha256,
             execution.receipt_refs, execution.purpose) ==
            (target.context, target.commit_oid, target.tree_oid, target.sources,
             target.required_suite_sha256, target.evaluator_sha256, policy.execution_provenance_sha256,
             target.execution_receipt_refs, target.purpose), "Execution evidence provenance differs")
    require(tuple(item[0] for item in execution.check_results) == tuple(item.check_id for item in suite.checks),
            "Execution omits or reorders required checks")
    blockers = []
    if any(status != "passed" for _, status in execution.check_results):
        blockers.append("required_execution_not_passed")

    _tuple(verdicts, ScopeVerdict, nonempty=False)
    _tuple(refutations, AdmittedRefutation, nonempty=False)
    slots: dict[tuple[str, str], ScopeVerdict] = {}
    outcomes: dict[str, tuple[ScopeVerdict, ...]] = {}
    views: dict[str, LocalViewManifest] = {}
    profiles = {profile.reviewer: profile for profile in policy.profiles}
    rules = {(slot.reviewer, slot.scope_id): slot for slot in policy.slots}
    calls: dict[str, DispatchBinding] = {}
    rejected: set[tuple[str, str]] = set()
    for verdict in verdicts:
        binding = verdict.review_dispatch
        actor, slot = binding.action.actor, (binding.action.actor, verdict.scope_id)
        require(slot in rules and slot not in slots, "Unregistered or duplicate review slot")
        require(actor not in authors, "Production author cannot approve their own release")
        profile = profiles[actor]
        require((binding.action.profile_id, binding.profile_sha256, binding.authority_config_sha256) ==
                (profile.profile_id, profile.profile_sha256, policy.authority_config_sha256),
                "Review profile or authority provenance differs")
        require(binding.call_id not in calls or calls[binding.call_id] == binding,
                "One charged call cannot have multiple identities")
        calls[binding.call_id] = binding
        key = identity(binding)
        if key not in outcomes:
            outcomes[key] = materialize_verdicts(binding, target, verified_terminal)
            views[key] = _bound_view(binding, verified_view)
        require(verdict in outcomes[key], "ScopeVerdict does not match actual worker response")
        view = views[key]
        require(view.policy_sha256 == policy_digest(policy), "Review uses another registered policy")
        target_ref = _record_ref(view, target, "release-target")
        require(view.source_ref == target_ref, "Review source is not the immutable release target")
        selection_ref = _record_ref(view, selection, "selection-manifest")
        required_refs = (target_ref, selection_ref, *target.execution_receipt_refs)
        _contains(view, required_refs)
        require(verdict.covered_requirement_ids == rules[slot].required_requirement_ids,
                "Scoped requirement or diagonal coverage differs")
        require(all(ref in verdict.evidence_refs for ref in required_refs),
                "Verdict omits exact target/selection/execution evidence")
        _contains(view, verdict.evidence_refs)
        slots[slot] = verdict
        if verdict.verdict != "approve":
            rejected.add(slot)
    for refutation in refutations:
        if refutation.target_sha256 == identity(target):
            rejected.update(slot for slot in SLOTS if slot[1] in refutation.scope_ids)
            blockers.append("admitted_refutation:" + refutation.evidence_ref.event_id)
    missing = tuple(slot for slot in SLOTS if slot not in slots)
    rejected_slots = tuple(slot for slot in SLOTS if slot in rejected)
    return ReviewGateReceipt(identity(target), policy_digest(policy), suite_digest(suite),
        tuple(identity(slots[slot]) for slot in SLOTS if slot in slots), tuple(sorted(calls)),
        missing, rejected_slots, tuple(blockers), not missing and not rejected_slots and not blockers)
