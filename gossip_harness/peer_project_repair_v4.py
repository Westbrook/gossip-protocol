"""Topic-local repairs with complete failed-release context.

This host-owned adapter grants neither model calls nor release authority. A
repair starts from its selected topic's Git commit; the failed combined tree is
read-only evidence. All four topics are restaged through ProjectPromotion after
the successor frontier is frozen, then need fresh execution and review.

Freshness is checked when admitting work or freezing a successor frontier.
Historical immutable view proofs remain verifiable after that successor exists.
The controller must retain each registered plan/directive and the same promotion
journal, and cannot add late results to an already frozen registry.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
from typing import Callable

from .gitstore import GitStore
from .peer_candidate_v2 import named_sources
from .peer_coding_dispatch_v1 import source_digest
from .peer_project_contract_v2 import (
    PACKAGES, CandidateOffer, DispatchBinding, EvidenceRef, ReleaseTarget, SelectionManifest,
    WorkKey, canonical_bytes, identity, require, resolve_local, sha256, to_dict,
)
from .peer_project_views_v3 import ProjectSource
from .peer_review_release_v2 import (
    ReviewGateReceipt, VerifiedContribution, VerifiedSelection, receipt_digest,
)
from .peer_role_loop_v2 import LocalMesh, WorkDirective

PROTOCOL = "peer-project-topic-repair-v4"
CONTEXT_KIND = "project-repair-context"
PUBLIC_FEEDBACK_KINDS = ("execution-receipt", "public-execution", "role-result", "financial-result")
CurrentFailure = Callable[[], tuple[ReleaseTarget, ReviewGateReceipt]]
VerifiedFeedback = Callable[[ReleaseTarget, ReviewGateReceipt], tuple[EvidenceRef, ...]]


@dataclass(frozen=True, slots=True)
class RepairTopic:
    package_id: str
    offer: CandidateOffer
    base_files: tuple[tuple[str, str], ...]
    scope: tuple[str, ...]
    repair: bool
    repair_generation: int

    @property
    def new_generation(self) -> int:
        # A failed no-patch action may retain an older selected topic. Advancing
        # by selected generation would then reuse its previous repair WorkKey.
        return self.repair_generation if self.repair else self.offer.dispatch.action.work.generation

    @property
    def base_sha(self) -> str:
        return self.offer.commit_oid


@dataclass(frozen=True, slots=True)
class TopicRepairPlan:
    """Trusted immutable host registration, not a peer-submitted assertion."""
    baseline_sha: str
    failed_target: ReleaseTarget
    selection: SelectionManifest
    failure_receipt: ReviewGateReceipt
    topics: tuple[RepairTopic, ...]
    failed_files: tuple[tuple[str, str], ...]
    feedback_refs: tuple[EvidenceRef, ...]

    def topic(self, package: str) -> RepairTopic:
        require(package in PACKAGES, "Unknown repair package")
        return next(topic for topic in self.topics if topic.package_id == package)

    @property
    def package_generations(self) -> tuple[tuple[str, int], ...]:
        """Prospective generations; derive final policy from the frozen offers.

        An explicit unchanged disposition retains the prior offer generation.
        """
        return tuple((topic.package_id, topic.new_generation) for topic in self.topics)

    @property
    def failure_receipt_sha256(self) -> str:
        return receipt_digest(self.failure_receipt)

    def body(self) -> dict:
        # Full topic files are retained immutably on the host; hashes suffice in
        # the context since each worker separately receives its whole topic base.
        receipt = self.failure_receipt
        gate = {"target_sha256": receipt.target_sha256, "policy_sha256": receipt.policy_sha256,
            "suite_sha256": receipt.suite_sha256, "verdict_sha256s": list(receipt.verdict_sha256s),
            "call_ids": list(receipt.call_ids), "missing_slots": [list(slot) for slot in receipt.missing_slots],
            "rejected_slots": [list(slot) for slot in receipt.rejected_slots], "blockers": list(receipt.blockers),
            "eligible": receipt.eligible, "purpose": receipt.purpose,
            "independent_final_project_acceptance": receipt.independent_final_project_acceptance}
        return {"protocol": PROTOCOL, "baseline_sha": self.baseline_sha,
            "failed_target": to_dict(self.failed_target), "selection": to_dict(self.selection),
            "failure_receipt_sha256": self.failure_receipt_sha256, "failure_receipt": gate,
            "topics": [{"package_id": topic.package_id, "offer": to_dict(topic.offer),
                "base_sources": [to_dict(item) for item in named_sources(dict(topic.base_files))],
                "scope": list(topic.scope), "repair": topic.repair,
                "new_generation": topic.new_generation} for topic in self.topics],
            "feedback_refs": [to_dict(ref) for ref in self.feedback_refs]}

    @property
    def sha256(self) -> str:
        return hashlib.sha256(canonical_bytes(self.body())).hexdigest()

    def context_payload(self) -> bytes:
        return canonical_bytes({"protocol": PROTOCOL, "plan_sha256": self.sha256,
            "plan": self.body(), "failed_source": {"files": dict(self.failed_files),
                "base_sha": self.failed_target.commit_oid}})


def _failure(current_failure: CurrentFailure) -> tuple[ReleaseTarget, ReviewGateReceipt]:
    value = current_failure()
    require(type(value) is tuple and len(value) == 2, "Trusted current failure required")
    target, receipt = value
    require(type(target) is ReleaseTarget and type(receipt) is ReviewGateReceipt,
            "Trusted current target and gate receipt required")
    require(receipt.target_sha256 == identity(target)
            and receipt.suite_sha256 == target.required_suite_sha256
            and receipt.eligible is False and bool(receipt.blockers or receipt.rejected_slots)
            and receipt.purpose == "public_release"
            and receipt.independent_final_project_acceptance is False,
            "Repair requires a failed current public release gate")
    return target, receipt


def assert_current_plan(plan: TopicRepairPlan, current_failure: CurrentFailure) -> None:
    """Admission fence; do not call this while validating historical proofs."""
    target, receipt = _failure(current_failure)
    require(target == plan.failed_target and receipt_digest(receipt) == plan.failure_receipt_sha256,
            "Repair plan is stale or its public failure evidence changed")


def plan_topic_repairs(*, target_store: GitStore, baseline_sha: str,
        selection: SelectionManifest, offers: tuple[CandidateOffer, ...],
        stores_by_offer: dict[str, GitStore], package_scopes: dict[str, tuple[str, ...]],
        repair_packages: tuple[str, ...], feedback_refs: tuple[EvidenceRef, ...],
        current_failure: CurrentFailure, verified_selection: VerifiedSelection,
        verified_contribution: VerifiedContribution, verified_feedback: VerifiedFeedback) -> TopicRepairPlan:
    """Freeze exact selected topics after trusted public gate failure.

    Callbacks are host capabilities backed by retained promotion and registry
    evidence. ``verified_feedback`` must reconstruct the complete current public
    execution/review artifacts, binding actual reviewer calls and verdicts to the
    supplied gate. Arrival hashes alone are not provenance authentication.
    """
    target, failure = _failure(current_failure)
    require(type(repair_packages) is tuple and bool(repair_packages)
            and len(set(repair_packages)) == len(repair_packages)
            and set(repair_packages) <= set(PACKAGES), "Invalid repair package set")
    require(type(package_scopes) is dict and set(package_scopes) == set(PACKAGES),
            "Register all exact package scopes")
    all_paths: list[str] = []
    for paths in package_scopes.values():
        require(type(paths) is tuple and bool(paths) and len(set(paths)) == len(paths), "Invalid exact scope")
        # Reuse the project source path validator, including forbidden .git and
        # traversal segments. Scopes are exact files, not directory ownership.
        named_sources({path: "" for path in paths})
        require(all(not path.endswith("/") for path in paths), "Repair scope must contain exact files")
        all_paths.extend(paths)
    require(len(set(all_paths)) == len(all_paths), "Repair package scopes overlap")
    require(type(feedback_refs) is tuple and bool(feedback_refs)
            and all(type(ref) is EvidenceRef and ref.kind in PUBLIC_FEEDBACK_KINDS for ref in feedback_refs)
            and len({ref.event_id for ref in feedback_refs}) == len(feedback_refs)
            and set(target.execution_receipt_refs) <= set(feedback_refs),
            "Repair needs complete registered public execution and feedback refs")
    require(verified_feedback(target, failure) == feedback_refs,
            "Repair feedback differs from authenticated complete current public feedback")
    rejected_actors = {actor for actor, _scope in failure.rejected_slots}
    require(not rejected_actors or (rejected_actors <= {ref.producer for ref in feedback_refs
                if ref.kind == "role-result"} and any(ref.kind == "financial-result" for ref in feedback_refs)),
            "Rejected reviews require their role results and actual financial result material")
    require(type(selection) is SelectionManifest and verified_selection(selection) == selection
            and selection.context == target.context and identity(selection) == target.selection_sha256,
            "Failed target has another authenticated selection")
    require(baseline_sha == target.expected_head and target_store.head() == baseline_sha,
            "Repair baseline differs from protected accepted head")
    files = target_store.read_files(target.commit_oid)
    require(named_sources(files) == target.sources
            and target_store._git("rev-parse", target.commit_oid + "^{tree}") == target.tree_oid
            and target_store.is_ancestor(baseline_sha, target.commit_oid),
            "Failed combined source differs from exact release target")
    require(type(offers) is tuple and all(type(offer) is CandidateOffer for offer in offers),
            "Registered candidate offers required")
    by_id = {identity(offer): offer for offer in offers}
    require(len(by_id) == len(offers) and set(by_id) == set(selection.eligible_offer_sha256s),
            "Repair lost the failed selection's exact eligible frontier")
    require(tuple(item.package_id for item in selection.selected) == PACKAGES,
            "Failed selection must cover all four ordered packages")
    topics = []
    for item in selection.selected:
        offer = by_id[item.offer_sha256]
        action = offer.dispatch.action
        require(action.context == target.context and action.work.package_id == item.package_id,
                "Selected topic belongs to another context or package")
        require(action.work.generation <= target.generation < 1_000_000,
                "Selected topic generation exceeds the failed target or repair range")
        scope = package_scopes[item.package_id]
        store = stores_by_offer[identity(offer)]
        topic_files = store.read_files(offer.commit_oid)
        require(source_digest(topic_files) == offer.source_sha256
                and store.is_ancestor(baseline_sha, offer.commit_oid), "Selected topic source or ancestry differs")
        require(set(store._introduced_paths(baseline_sha, offer.commit_oid)) <= set(scope),
                "Selected topic introduces unrelated package history")
        owned = {path: body for path, body in topic_files.items() if path in scope}
        require(bool(owned) and verified_contribution(offer) == named_sources(owned)
                and owned == {path: body for path, body in files.items() if path in scope},
                "Selected topic contribution differs from failed combined source")
        topics.append(RepairTopic(item.package_id, offer, tuple(sorted(topic_files.items())),
            scope, item.package_id in repair_packages, target.generation + 1))
    inherited = {path: body for path, body in files.items() if path not in all_paths}
    require(inherited == {path: body for path, body in target_store.read_files(baseline_sha).items()
                          if path not in all_paths}, "Failed target changed inherited source")
    plan = TopicRepairPlan(baseline_sha, target, selection, failure, tuple(topics),
                           tuple(sorted(files.items())), feedback_refs)
    plan.context_payload()  # Bound the complete evidence before any dispatch.
    return plan


def _source_payload(topic: RepairTopic) -> bytes:
    return canonical_bytes({"files": dict(topic.base_files), "base_sha": topic.base_sha})


def publish_repair_directive(plan: TopicRepairPlan, package: str, mesh: LocalMesh, *,
        work: WorkKey, profile_id: str, instructions: str, current_failure: CurrentFailure,
        attempt: int = 1, attempt_limit: int = 1) -> WorkDirective:
    """Publish a prospective host registration using existing v3 wire records."""
    assert_current_plan(plan, current_failure)
    topic = plan.topic(package)
    source = mesh.publish("project-source", _source_payload(topic),
                          "repair-topic-" + plan.sha256 + "-" + package)
    context = mesh.publish(CONTEXT_KIND, plan.context_payload(), "repair-context-" + plan.sha256)
    directive = WorkDirective(plan.failed_target.context, work, profile_id, "repair", source,
        (context, *plan.feedback_refs), topic.scope, instructions, attempt, attempt_limit,
        "Patch only the selected topic base in files. The complete failed combined tree is read-only "
        "public context in project-repair-context; it is not the patch base. Fresh combined execution "
        "and reviews remain required.")
    verify_repair_directive(plan, directive, mesh)
    return directive


def verify_repair_directive(plan: TopicRepairPlan, directive: WorkDirective, mesh: LocalMesh) -> ProjectSource:
    """Reconstruct an immutable registered repair proof, including after restaging.

    The caller separately matches the full directive against its trusted action
    registration and authenticates the dispatched WorkerRequest via v3 views.
    This function cannot authorize a new dispatch or reopen a frozen frontier.
    """
    topic = plan.topic(directive.work.package_id)
    old = topic.offer.dispatch.action
    require(topic.repair and directive.kind == "repair" and directive.context == plan.failed_target.context
            and directive.work.generation == topic.new_generation
            and directive.work.requirement_id == old.work.requirement_id
            and directive.profile_id == old.profile_id and directive.allowed_paths == topic.scope,
            "Repair directive differs from its registered selected topic")
    require(directive.source_ref.kind == "project-source" and bool(directive.evidence_refs)
            and directive.evidence_refs[0].kind == CONTEXT_KIND
            and directive.evidence_refs[1:] == plan.feedback_refs,
            "Repair directive omits or changes exact failure feedback")
    refs = directive.required_refs
    arrived = mesh.arrived()
    bodies = tuple(resolve_local(ref, arrived, mesh.resolve(ref)) for ref in refs)
    require(bodies[0] == _source_payload(topic) and bodies[1] == plan.context_payload(),
            "Repair topic base or failed combined context differs")
    return ProjectSource.parse(bodies[0])


@dataclass(frozen=True, slots=True)
class UnchangedTopic:
    """A known accounted no-patch result; never a new candidate or approval.

    Construction alone is not authentication. The controller must reconcile the
    registered repair directive, exact local view, settled financial failure and
    arrived result bytes before returning this record from verified_unchanged.
    Unknown outcomes and infrastructure failures do not authorize this fallback.
    """
    package_id: str
    plan_sha256: str
    original_offer_sha256: str
    dispatch: DispatchBinding
    result_sha256: str

    def __post_init__(self) -> None:
        require(self.package_id in PACKAGES and type(self.dispatch) is DispatchBinding,
                "Invalid unchanged topic package or dispatch")
        for digest in (self.plan_sha256, self.original_offer_sha256, self.result_sha256):
            sha256(digest)

    def body(self) -> dict:
        return {"package_id": self.package_id, "plan_sha256": self.plan_sha256,
            "original_offer_sha256": self.original_offer_sha256, "dispatch": to_dict(self.dispatch),
            "result_sha256": self.result_sha256}


VerifiedUnchanged = Callable[[UnchangedTopic], UnchangedTopic]


def repair_frontier(plan: TopicRepairPlan, replacements: tuple[CandidateOffer, ...], *,
                    current_failure: CurrentFailure, unchanged: tuple[UnchangedTopic, ...] = (),
                    verified_unchanged: VerifiedUnchanged | None = None) -> tuple[CandidateOffer, ...]:
    """Freeze exactly four selected topics, not a new sixteen-candidate contest.

    Each replacement must already have an authenticated registry registration.
    The returned immutable tuple is the complete successor eligibility frontier;
    the controller registers it once before selection/staging and does not append
    late candidates. Git source/parent authenticity is checked by that registry
    and the existing scope-checked ProjectPromotion stage. A verified unchanged
    disposition retains the EXACT old offer, source and generation. It is no
    approval, no new candidate and no reuse of a correctness judgment. All four
    topics still require fresh combined evaluation and review. Final generation
    policy must be derived from returned offers, not plan.package_generations.
    """
    assert_current_plan(plan, current_failure)
    require(type(replacements) is tuple and all(type(offer) is CandidateOffer for offer in replacements),
            "Invalid repaired candidate collection")
    by_package = {offer.dispatch.action.work.package_id: offer for offer in replacements}
    require(type(unchanged) is tuple and all(type(item) is UnchangedTopic for item in unchanged),
            "Invalid unchanged topic collection")
    carried = {item.package_id: item for item in unchanged}
    require(len(by_package) == len(replacements) and len(carried) == len(unchanged)
            and not (set(by_package) & set(carried))
            and set(by_package) | set(carried) == {topic.package_id for topic in plan.topics if topic.repair},
            "Successor frontier needs exactly the registered repaired packages")
    require(not unchanged or callable(verified_unchanged), "Unchanged topics need a trusted failure verifier")
    result = []
    for topic in plan.topics:
        if topic.package_id in carried:
            disposition = carried[topic.package_id]
            binding, prior_binding = disposition.dispatch, topic.offer.dispatch
            action, old = binding.action, prior_binding.action
            require(disposition.plan_sha256 == plan.sha256
                    and disposition.original_offer_sha256 == identity(topic.offer)
                    and action.context == plan.failed_target.context and action.kind == "repair"
                    and action.actor == old.actor and action.profile_id == old.profile_id
                    and action.work.package_id == topic.package_id
                    and action.work.requirement_id == old.work.requirement_id
                    and action.work.generation == topic.new_generation
                    and binding.profile_sha256 == prior_binding.profile_sha256
                    and binding.authority_config_sha256 == prior_binding.authority_config_sha256
                    and binding.call_id != prior_binding.call_id and action.request_id != old.request_id,
                    "Unchanged disposition differs from the registered repair or original offer")
            assert verified_unchanged is not None
            verified = verified_unchanged(disposition)
            require(type(verified) is UnchangedTopic and verified == disposition,
                    "Unchanged disposition lacks exact authenticated known-failure proof")
            result.append(topic.offer)
            continue
        offer = by_package[topic.package_id] if topic.repair else topic.offer
        action, prior = offer.dispatch.action, topic.offer.dispatch.action
        require(action.context == plan.failed_target.context and action.actor == prior.actor
                and action.profile_id == prior.profile_id and action.work.generation == topic.new_generation
                and action.work.requirement_id == prior.work.requirement_id
                and offer.dispatch.profile_sha256 == topic.offer.dispatch.profile_sha256
                and offer.dispatch.authority_config_sha256 == topic.offer.dispatch.authority_config_sha256
                and (not topic.repair or action.kind == "repair"),
                "Repaired offer differs from registered actor, profile, context or generation")
        result.append(offer)
    return tuple(result)
