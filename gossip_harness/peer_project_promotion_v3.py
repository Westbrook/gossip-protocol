"""Host-owned project release history and exact-source Git promotion.

Staging is structural preparation, never candidate execution or acceptance.
Only the actual v2 review gate, using retained complete review responses and
trusted evidence adapters, can authorize the protected Git CAS. The journal
retains failed stages, old reviews, refutations and CAS intents. Source/config
bindings and a process owner lock protect ordinary restarts; a caller can also
supply its independently retained checkpoint to detect storage rollback. This
is not a Byzantine storage service or independent final project acceptance.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, replace
import fcntl
import hashlib
from pathlib import Path
import sqlite3
import threading
from typing import Any, Callable

from .gitstore import ACCEPTED, Candidate, GitStore
from .peer_candidate_v2 import named_sources
from .peer_coding_dispatch_v1 import canonical_payload, source_digest
from .peer_project_contract_v2 import (
    PACKAGES, CandidateOffer, DispatchBinding, EvidenceRef, NamedSource,
    ReleaseTarget, ScopeVerdict, SelectionManifest, from_dict, identifier,
    identity, require, sha256, strict_loads, to_dict,
)
from .peer_review_release_v2 import (
    AdmittedRefutation, RequiredCheck, RequiredSuite, ReviewGateReceipt,
    ReviewPolicy, VerifiedContribution, VerifiedExecution, VerifiedSelection,
    VerifiedTerminal, VerifiedView, evaluate_release, extend_required_suite,
    materialize_verdicts, policy_digest, receipt_digest, suite_digest,
)

PROTOCOL = "peer-project-promotion-v3"
MAX_EVENTS = 8192
MAX_EVENT_BYTES = 512_000


class PromotionError(ValueError):
    """A release cannot be reconciled with the retained project history."""


@dataclass(frozen=True)
class StagedRelease:
    stage_id: str
    generation: int
    expected_head: str
    commit_oid: str
    tree_oid: str
    sources: tuple[NamedSource, ...]
    selection_sha256: str
    required_suite_sha256: str
    integration_order: tuple[str, ...]


@dataclass(frozen=True)
class PromotionResult:
    status: str
    target_sha256: str
    old_head: str
    new_head: str
    gate_receipt_sha256: str
    recovered: bool


def _json(value: Any) -> Any:
    if type(value) is tuple:
        return [_json(item) for item in value]
    if type(value) is list:
        return [_json(item) for item in value]
    if type(value) is dict:
        return {key: _json(item) for key, item in value.items()}
    return value


def _encode(value: Any) -> bytes:
    return canonical_payload(_json(value))


def _hash(value: Any) -> str:
    return hashlib.sha256(_encode(value)).hexdigest()


def _suite(value: dict[str, Any], policy: ReviewPolicy) -> RequiredSuite:
    require(value["context"] == to_dict(policy.context), "Suite context changed")
    return RequiredSuite(policy.context, tuple(RequiredCheck(**item) for item in value["checks"]))


def _staged(value: dict[str, Any]) -> StagedRelease:
    return StagedRelease(**(value | {
        "sources": tuple(from_dict(NamedSource, item) for item in value["sources"]),
        "integration_order": tuple(value["integration_order"]),
    }))


def _paths(scopes: dict[str, tuple[str, ...]]) -> tuple[str, ...]:
    require(type(scopes) is dict and set(scopes) == set(PACKAGES), "Register all four package scopes")
    result: list[str] = []
    for package in PACKAGES:
        names = scopes[package]
        require(type(names) is tuple and bool(names), "Package scope must contain exact paths")
        for name in names:
            # NamedSource validates canonical repository paths; directories and
            # prefix ownership are intentionally not accepted by this adapter.
            NamedSource(name, "0" * 64)
            require(not name.endswith("/"), "Package scope must contain files")
            result.append(name)
    require(len(set(result)) == len(result), "Package ownership overlaps")
    return tuple(sorted(result))


class ProjectPromotion:
    """Single-threaded trusted host capability; peers cannot call journal writes.

    Evidence callbacks authenticate actual retained artifacts. They are never
    peer-supplied flags. Selected GitStore objects must be verified quarantine
    stores. Calling ``stage`` does not choose candidates: SelectionManifest is
    authenticated by the supplied selection adapter.
    """

    def __init__(self, root: Path, protected_store: GitStore, *, repository_id: str,
                 baseline_sha: str, policy: ReviewPolicy, initial_suite: RequiredSuite,
                 package_scopes: dict[str, tuple[str, ...]],
                 verified_selection: VerifiedSelection,
                 verified_contribution: VerifiedContribution,
                 verified_terminal: VerifiedTerminal, verified_view: VerifiedView,
                 verified_execution: VerifiedExecution,
                 expected_checkpoint: tuple[int, str] | None = None,
                 crash_hook: Callable[[str], None] | None = None):
        identifier(repository_id)
        require(type(policy) is ReviewPolicy and type(initial_suite) is RequiredSuite,
                "Expected registered policy and initial suite")
        require(initial_suite.context == policy.context and
                suite_digest(initial_suite) == policy.initial_suite_sha256,
                "Initial suite differs from policy")
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.store, self.policy, self.initial_suite = protected_store, policy, initial_suite
        self.repository_id, self.baseline_sha = repository_id, baseline_sha
        self.scopes = dict(package_scopes)
        self.owned_paths = _paths(self.scopes)
        baseline_files = self.store.read_files(baseline_sha)
        inherited = {item.path: item for item in policy.inherited_sources}
        require(not (set(inherited) & set(self.owned_paths)), "Inherited paths overlap package scopes")
        require(set(baseline_files) <= set(inherited) | set(self.owned_paths), "Baseline has unowned files")
        require(tuple(item for item in named_sources(baseline_files) if item.path in inherited) ==
                tuple(sorted(inherited.values(), key=lambda item: item.path)), "Inherited baseline bytes differ")
        require(self.store.is_ancestor(baseline_sha, self.store.head()), "Protected history lost baseline")
        self.verified_selection, self.verified_contribution = verified_selection, verified_contribution
        self.verified_terminal, self.verified_view = verified_terminal, verified_view
        self.verified_execution = verified_execution
        self.crash_hook = crash_hook or (lambda _point: None)
        self._owner_thread = threading.get_ident()
        self._closed = False
        self._lock = (self.root / "owner.lock").open("a+b")
        try:
            fcntl.flock(self._lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self._lock.close()
            raise PromotionError("Another process owns the promotion root") from None
        try:
            self.db = sqlite3.connect(self.root / "history.sqlite3")
            self.db.execute("PRAGMA journal_mode=WAL")
            self.db.execute("PRAGMA synchronous=FULL")
            self.db.execute("CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value BLOB NOT NULL)")
            self.db.execute("CREATE TABLE IF NOT EXISTS events (seq INTEGER PRIMARY KEY, kind TEXT NOT NULL, payload BLOB NOT NULL, previous TEXT NOT NULL, digest TEXT NOT NULL)")
            source_names = ("peer_project_promotion_v3.py", "peer_review_release_v2.py",
                            "peer_project_contract_v2.py", "peer_candidate_v2.py", "gitstore.py")
            config = {"protocol": PROTOCOL, "root": str(self.root), "store": str(self.store.path),
                      "store_inode": self.store.path.stat().st_ino, "repository_id": repository_id,
                      "baseline_sha": baseline_sha, "policy": asdict(policy),
                      "initial_suite": asdict(initial_suite), "scopes": self.scopes,
                      "sources": {name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
                                  for name in source_names}}
            encoded = _encode(config)
            old = self.db.execute("SELECT value FROM metadata WHERE key='config'").fetchone()
            if old is None:
                require(self.db.execute("SELECT count(*) FROM events").fetchone()[0] == 0,
                        "History lacks its root configuration")
                self.db.execute("INSERT INTO metadata VALUES ('config', ?)", (encoded,))
                self.db.commit()
            else:
                require(old[0] == encoded, "Promotion root configuration or source changed")
            self.config_sha256 = hashlib.sha256(encoded).hexdigest()
            self._load()
            if expected_checkpoint is not None:
                seq, digest = expected_checkpoint
                require(type(seq) is int and 0 <= seq <= len(self.events), "Retained checkpoint was rolled back")
                actual = self.config_sha256 if seq == 0 else self.events[seq - 1][2]
                require(digest == actual, "Retained checkpoint identity differs")
        except BaseException:
            if hasattr(self, "db"):
                self.db.close()
            self._lock.close()
            raise

    def _active(self) -> None:
        require(not self._closed and threading.get_ident() == self._owner_thread,
                "Promotion requires its live owner thread")

    def _load(self) -> None:
        rows = self.db.execute("SELECT seq,kind,payload,previous,digest FROM events ORDER BY seq").fetchall()
        require(len(rows) <= MAX_EVENTS, "Promotion history bound exceeded")
        self.events: list[tuple[str, Any, str]] = []
        previous = self.config_sha256
        for expected, (seq, kind, raw, parent, digest) in enumerate(rows, 1):
            require(seq == expected and parent == previous and len(raw) <= MAX_EVENT_BYTES,
                    "Promotion history order or parent changed")
            payload = strict_loads(raw)
            require(canonical_payload(payload) == raw, "Noncanonical promotion history")
            require(digest == _hash({"seq": seq, "kind": kind, "payload": payload, "previous": parent}),
                    "Promotion history content changed")
            self.events.append((kind, payload, digest))
            previous = digest
        self.suites = [self.initial_suite]
        self.intents: dict[str, dict[str, Any]] = {}
        self.stages: dict[str, StagedRelease] = {}
        self.targets: dict[str, ReleaseTarget] = {}
        self.reviews: dict[str, dict[str, tuple[ScopeVerdict, ...]]] = {}
        self.review_rounds: dict[str, int] = {}
        self.review_requests: dict[str, dict[str, str]] = {}
        self.registered_requests: dict[str, tuple[str, str]] = {}
        self.seen_calls: dict[str, tuple[str, str]] = {}
        self.refutations: list[AdmittedRefutation] = []
        self.cas: dict[str, dict[str, Any]] = {}
        self.promotions: dict[str, PromotionResult] = {}
        self.max_generation = -1
        for kind, payload, _ in self.events:
            if kind == "suite":
                suite = _suite(payload, self.policy)
                prior = self.suites[-1]
                require(len(suite.checks) > len(prior.checks) and suite.checks[:len(prior.checks)] == prior.checks,
                        "Required suite history weakened")
                self.suites.append(suite)
            elif kind == "stage_intent":
                stage_id = payload["stage_id"]
                require(stage_id not in self.intents and payload["generation"] > self.max_generation,
                        "Stage generation rolled back")
                self.intents[stage_id] = payload
                self.max_generation = payload["generation"]
            elif kind == "staged":
                staged = _staged(payload)
                require(staged.stage_id in self.intents and staged.stage_id not in self.stages,
                        "Stage lacks one immutable intent")
                self.stages[staged.stage_id] = staged
            elif kind == "target":
                target = from_dict(ReleaseTarget, payload["target"])
                require(payload["stage_id"] in self.stages and payload["stage_id"] not in self.targets,
                        "Target lacks an immutable stage")
                self.targets[payload["stage_id"]] = target
            elif kind == "review_round":
                target_id, number = payload["target_sha256"], payload["round_number"]
                require(number > self.review_rounds.get(target_id, -1), "Review round rolled back")
                self.review_rounds[target_id] = number
                for actor, request_id in payload["requests"]:
                    require(request_id not in self.registered_requests, "Review request identity reused")
                    self.registered_requests[request_id] = (target_id, actor)
                    self.review_requests.setdefault(target_id, {})[actor] = request_id
                    self.reviews.setdefault(target_id, {}).pop(actor, None)
            elif kind == "review":
                target_id, actor = payload["target_sha256"], payload["actor"]
                verdicts = tuple(from_dict(ScopeVerdict, item) for item in payload["verdicts"])
                require(bool(verdicts) and all(item.review_dispatch.action.actor == actor and
                        item.target_sha256 == target_id for item in verdicts), "Review history identity differs")
                require(self.review_requests.get(target_id, {}).get(actor) ==
                        verdicts[0].review_dispatch.action.request_id, "Response superseded its registered review round")
                call = verdicts[0].review_dispatch.call_id
                require(call not in self.seen_calls, "Review call was admitted twice")
                self.seen_calls[call] = (target_id, identity(verdicts[0].review_dispatch))
                self.reviews.setdefault(target_id, {})[actor] = verdicts
            elif kind == "late_review":
                binding = from_dict(DispatchBinding, payload["binding"])
                require(binding.call_id not in self.seen_calls, "Late review call was admitted twice")
                self.seen_calls[binding.call_id] = (payload["target_sha256"], identity(binding))
            elif kind == "refutation":
                ref = AdmittedRefutation(payload["target_sha256"], tuple(payload["scope_ids"]),
                                         from_dict(EvidenceRef, payload["evidence_ref"]))
                require(ref not in self.refutations, "Repeated refutation history")
                self.refutations.append(ref)
            elif kind == "cas_intent":
                require(payload["target_sha256"] not in self.cas, "Repeated promotion intent")
                self.cas[payload["target_sha256"]] = payload
            elif kind == "promoted":
                result = PromotionResult(**payload)
                require(result.target_sha256 in self.cas and result.target_sha256 not in self.promotions,
                        "Promotion lacks one CAS intent")
                self.promotions[result.target_sha256] = result
            else:
                require(kind in ("stage_failed", "gate"), "Unknown promotion history event")

    def _append(self, kind: str, payload: Any) -> None:
        self._active()
        require(len(self.events) < MAX_EVENTS, "Promotion history bound exceeded")
        raw = _encode(payload)
        require(len(raw) <= MAX_EVENT_BYTES, "Promotion event too large")
        previous = self.checkpoint()[1]
        digest = _hash({"seq": len(self.events) + 1, "kind": kind,
                        "payload": strict_loads(raw), "previous": previous})
        with self.db:
            self.db.execute("INSERT INTO events VALUES (?,?,?,?,?)",
                            (len(self.events) + 1, kind, raw, previous, digest))
        self._load()

    def checkpoint(self) -> tuple[int, str]:
        self._active()
        return len(self.events), self.events[-1][2] if self.events else self.config_sha256

    def record_suite(self, additions: tuple[RequiredCheck, ...]) -> RequiredSuite:
        self._active()
        self._no_pending_cas()
        suite = extend_required_suite(self.suites[-1], additions)
        self._append("suite", asdict(suite))
        return suite

    def _no_pending_cas(self) -> None:
        require(all(key in self.promotions for key in self.cas),
                "Recover the existing promotion intent before changing release history")

    def admit_refutation(self, refutation: AdmittedRefutation) -> None:
        self._active()
        self._no_pending_cas()
        require(type(refutation) is AdmittedRefutation and
                refutation.target_sha256 in {identity(target) for target in self.targets.values()},
                "Refutation must name a retained exact target")
        if refutation not in self.refutations:
            self._append("refutation", asdict(refutation))

    def stage(self, selection: SelectionManifest, offers: tuple[CandidateOffer, ...],
              stores_by_offer: dict[str, GitStore], *, generation: int,
              package_generations: tuple[tuple[str, int], ...],
              integration_order: tuple[str, ...] = PACKAGES) -> StagedRelease:
        self._active()
        self._no_pending_cas()
        require(type(generation) is int and 0 <= generation <= 1_000_000, "Invalid stage generation")
        require(type(integration_order) is tuple and len(integration_order) == 4 and
                set(integration_order) == set(PACKAGES), "Integration order must contain four packages")
        policy = replace(self.policy, package_generations=package_generations)
        require(self.verified_selection(selection) == selection and type(selection) is SelectionManifest,
                "Selection has no exact authenticated provenance")
        require(selection.context == policy.context and
                selection.eligibility_policy_sha256 == policy.eligibility_policy_sha256,
                "Selection context or eligibility policy differs")
        require(selection.selecting_dispatch.action.kind == "select_source" and
                selection.selecting_dispatch.action.work.generation == generation and
                selection.selecting_dispatch.authority_config_sha256 == policy.authority_config_sha256,
                "Selector generation or authority differs")
        require(tuple(item.package_id for item in selection.selected) == PACKAGES,
                "Selection must cover four ordered packages")
        by_id = {identity(offer): offer for offer in offers}
        require(len(by_id) == len(offers) and set(by_id) == set(selection.eligible_offer_sha256s),
                "Eligible offer frontier differs")
        selected = {item.package_id: by_id[item.offer_sha256] for item in selection.selected}
        expected_sources = list(policy.inherited_sources)
        generations = dict(package_generations)
        for package, offer in selected.items():
            action = offer.dispatch.action
            require(action.context == policy.context and action.work.package_id == package and
                    action.work.generation == generations[package] and
                    offer.dispatch.authority_config_sha256 == policy.authority_config_sha256,
                    "Selected offer context, package, generation or authority differs")
            contribution = self.verified_contribution(offer)
            require(type(contribution) is tuple and bool(contribution) and
                    all(type(item) is NamedSource for item in contribution), "Missing authenticated contribution")
            require(set(item.path for item in contribution) <= set(self.scopes[package]) and
                    len({item.path for item in contribution}) == len(contribution), "Contribution exceeds exact scope")
            store = stores_by_offer[identity(offer)]
            files = store.read_files(offer.commit_oid)
            require(source_digest(files) == offer.source_sha256, "Imported candidate source digest differs")
            actual = tuple(item for item in named_sources(files) if item.path in self.scopes[package])
            require(actual == tuple(sorted(contribution, key=lambda item: item.path)), "Contribution omits or changes package source")
            require(store.is_ancestor(self.baseline_sha, offer.commit_oid), "Candidate lost baseline ancestry")
            expected_sources.extend(contribution)
        expected = tuple(sorted(expected_sources, key=lambda item: item.path))
        require(len({item.path for item in expected}) == len(expected), "Selected source memberships overlap")
        suite_sha = suite_digest(self.suites[-1])
        stage_id = _hash({"selection": identity(selection), "generation": generation,
                          "suite": suite_sha, "integration_order": integration_order,
                          "policy": policy_digest(policy)})
        if stage_id in self.stages:
            self._verify_staged(self.stages[stage_id])
            return self.stages[stage_id]
        if stage_id not in self.intents:
            require(generation > self.max_generation, "Stage generation must advance")
            self._append("stage_intent", {"stage_id": stage_id, "generation": generation,
                "selection": to_dict(selection), "offers": [to_dict(item) for item in offers],
                "expected_head": self.store.head(), "sources": [to_dict(item) for item in expected],
                "suite_sha256": suite_sha, "package_generations": package_generations,
                "integration_order": integration_order})
            self.crash_hook("after_stage_intent")
        intent = self.intents[stage_id]
        expected_head = intent["expected_head"]
        require(self.store.head() == expected_head, "Protected head changed before staging")
        staging_path = self.root / ("stage-" + stage_id)
        staging = GitStore(staging_path) if staging_path.exists() else GitStore.fork(self.store, staging_path)
        # A resumed stage can have already accepted a prefix. Re-preparing those
        # offers is a Git noop. No proposal, review or failed artifact is erased.
        for package in integration_order:
            offer = selected[package]
            candidate = staging.prepare(stores_by_offer[identity(offer)], offer.commit_oid,
                                        staging.head(), lambda _path: (True, "Structural staging only; no execution"),
                                        allowed_paths=self.scopes[package])
            if candidate.status not in ("prepared", "noop"):
                self._append("stage_failed", {"stage_id": stage_id, "package": package,
                                              "candidate": asdict(candidate)})
                raise PromotionError("Provisional stage failed: " + candidate.status)
            accepted = staging.accept(candidate)
            require(accepted.status in ("accepted", "noop"), "Staging CAS unexpectedly changed")
            self.crash_hook("after_stage_package:" + package)
        files = staging.read_files()
        require(named_sources(files) == expected, "Merged source is not exact selected packages plus inherited source")
        commit_oid = staging.head()
        # A fresh exact-head source-only pin. Tests and review receipts must bind
        # this actual candidate, not any of the individually tested offers.
        prepared = self.store.prepare(staging, commit_oid, expected_head,
            lambda _path: (True, "Exact selected source prepared; execution gate remains pending"),
            allowed_paths=self.owned_paths)
        require(prepared.status in ("prepared", "noop") and prepared.candidate_sha == commit_oid,
                "Protected preparation differs from staged commit")
        require(named_sources(self.store.read_files(commit_oid)) == expected, "Protected candidate source differs")
        staged = StagedRelease(stage_id, generation, expected_head, commit_oid,
            self.store._git("rev-parse", commit_oid + "^{tree}"), expected, identity(selection), suite_sha,
            integration_order)
        self._append("staged", asdict(staged))
        self.crash_hook("after_staged")
        return staged

    def _verify_staged(self, staged: StagedRelease) -> None:
        require(self.stages.get(staged.stage_id) == staged, "Unknown immutable stage")
        require(named_sources(self.store.read_files(staged.commit_oid)) == staged.sources and
                self.store._git("rev-parse", staged.commit_oid + "^{tree}") == staged.tree_oid,
                "Retained staged source or tree differs")
        require(self.store.is_ancestor(staged.expected_head, staged.commit_oid), "Stage lost accepted ancestry")
        require(self.store._git("rev-parse", self.store._candidate_ref(staged.expected_head, staged.commit_oid)) ==
                staged.commit_oid, "Protected candidate pin differs")

    def bind_target(self, staged: StagedRelease, *, evaluator_sha256: str,
                    execution_receipt_refs: tuple[EvidenceRef, ...]) -> ReleaseTarget:
        self._active()
        self._no_pending_cas()
        self._verify_staged(staged)
        target = ReleaseTarget(self.policy.context, staged.generation, self.repository_id, ACCEPTED,
            staged.expected_head, "sha1", staged.commit_oid, staged.tree_oid, staged.sources,
            staged.selection_sha256, staged.required_suite_sha256, evaluator_sha256, execution_receipt_refs)
        if staged.stage_id in self.targets:
            require(self.targets[staged.stage_id] == target, "An immutable stage cannot be rebound to another target")
        else:
            require(staged.generation == self.max_generation and
                    staged.required_suite_sha256 == suite_digest(self.suites[-1]), "Cannot bind a stale stage")
            self._append("target", {"stage_id": staged.stage_id, "target": to_dict(target)})
        return target

    def _current(self, target: ReleaseTarget) -> StagedRelease:
        matches = [self.stages[key] for key, value in self.targets.items() if value == target]
        require(len(matches) == 1, "Target is not a retained immutable release")
        staged = matches[0]
        require(staged.generation == self.max_generation and
                target.required_suite_sha256 == suite_digest(self.suites[-1]), "Release target is stale")
        self._verify_staged(staged)
        return staged

    def begin_review_round(self, target: ReleaseTarget, *, round_number: int,
                           requests: tuple[tuple[str, str], ...]) -> None:
        """Reserve exact reviewer request identities before their dispatch.

        A new round invalidates the selected reviewers' old responses at once.
        This is an internal host operation, not a model-controlled round label.
        Delayed responses from earlier rounds can never restore old approvals.
        """
        self._active()
        self._no_pending_cas()
        self._current(target)
        target_id = identity(target)
        require(type(round_number) is int and 0 <= round_number <= 1_000_000 and
                round_number > self.review_rounds.get(target_id, -1), "Review round must advance")
        require(type(requests) is tuple and 1 <= len(requests) <= 4 and
                all(type(item) is tuple and len(item) == 2 for item in requests), "Invalid review request registration")
        require(len({actor for actor, _ in requests}) == len(requests) and
                len({request for _, request in requests}) == len(requests), "Duplicate reviewer request")
        for actor, request in requests:
            require(actor in ("R1", "R2", "R3", "R4"), "Unregistered reviewer")
            identifier(request)
            require(request not in self.registered_requests, "Review request identity already registered")
        self._append("review_round", {"target_sha256": target_id, "round_number": round_number,
                                      "requests": requests})

    def record_reviews(self, target: ReleaseTarget, dispatches: tuple[DispatchBinding, ...]) -> tuple[ScopeVerdict, ...]:
        self._active()
        self._no_pending_cas()
        self._current(target)
        target_id = identity(target)
        require(type(dispatches) is tuple and len(dispatches) <= 4 and
                len({item.action.actor for item in dispatches}) == len(dispatches), "One complete response per reviewer per batch")
        for binding in dispatches:
            verdicts = materialize_verdicts(binding, target, self.verified_terminal)
            registered = self.registered_requests.get(binding.action.request_id)
            require(registered == (target_id, binding.action.actor), "Review request was not registered before dispatch")
            if binding.call_id in self.seen_calls:
                require(self.seen_calls[binding.call_id] == (target_id, identity(binding)), "Charged call identity changed")
                # Exact replay never reinstates superseded votes or grows history.
                continue
            if self.review_requests[target_id][binding.action.actor] != binding.action.request_id:
                # Financial/model artifacts remain retained. The audit record
                # explains why this genuine but delayed response was excluded.
                self._append("late_review", {"target_sha256": target_id,
                    "binding": to_dict(binding), "verdicts": [to_dict(item) for item in verdicts]})
                continue
            self._append("review", {"target_sha256": target_id, "actor": binding.action.actor,
                                    "verdicts": [to_dict(item) for item in verdicts]})
        current = self.reviews.get(target_id, {})
        return tuple(item for actor in sorted(current) for item in current[actor])

    def promote(self, target: ReleaseTarget) -> PromotionResult | ReviewGateReceipt:
        self._active()
        target_id = identity(target)
        if target_id in self.promotions:
            result = self.promotions[target_id]
            require(self.store.is_ancestor(result.new_head, self.store.head()), "Protected promoted history was rolled back")
            return result
        staged = self._current(target)
        if target_id in self.cas:
            return self._finish_cas(target, staged, recovered=True)
        self._no_pending_cas()
        require(self.store.head() == target.expected_head, "Protected head changed before release gate")
        intent = self.intents[staged.stage_id]
        selection = from_dict(SelectionManifest, intent["selection"])
        offers = tuple(from_dict(CandidateOffer, item) for item in intent["offers"])
        policy = replace(self.policy, package_generations=tuple(tuple(item) for item in intent["package_generations"]))
        current = self.reviews.get(target_id, {})
        verdicts = tuple(item for actor in sorted(current) for item in current[actor])
        receipt = evaluate_release(target=target, selection=selection, offers=offers, policy=policy,
            suite_history=tuple(self.suites), verdicts=verdicts, refutations=tuple(self.refutations),
            verified_selection=self.verified_selection, verified_contribution=self.verified_contribution,
            verified_terminal=self.verified_terminal, verified_view=self.verified_view,
            verified_execution=self.verified_execution)
        self._append("gate", {"target_sha256": target_id, "receipt": asdict(receipt),
                              "receipt_sha256": receipt_digest(receipt)})
        if not receipt.eligible:
            return receipt
        self._append("cas_intent", {"target_sha256": target_id, "stage_id": staged.stage_id,
                                    "gate_receipt_sha256": receipt_digest(receipt),
                                    "checkpoint": self.checkpoint(), "expected_head": target.expected_head,
                                    "candidate_sha": target.commit_oid})
        self.crash_hook("after_cas_intent")
        return self._finish_cas(target, staged, recovered=False)

    def _finish_cas(self, target: ReleaseTarget, staged: StagedRelease, *, recovered: bool) -> PromotionResult:
        self._verify_staged(staged)
        target_id = identity(target)
        intent = self.cas[target_id]
        require(intent["stage_id"] == staged.stage_id and intent["candidate_sha"] == target.commit_oid and
                intent["expected_head"] == target.expected_head, "CAS intent identity differs")
        head = self.store.head()
        if head == target.expected_head:
            result = self.store.accept(Candidate("prepared", target.expected_head, target.commit_oid,
                target.commit_oid, "Exact combined-source execution and scoped review gate passed", ()))
            require(result.status == "accepted", "Protected Git CAS was stale")
            self.crash_hook("after_git_cas")
        elif head != target.commit_oid:
            raise PromotionError("Protected Git head differs from both sides of retained CAS")
        result_record = PromotionResult("accepted", target_id, target.expected_head, target.commit_oid,
                                        intent["gate_receipt_sha256"], recovered)
        self._append("promoted", asdict(result_record))
        self.crash_hook("after_promoted")
        return result_record

    def close(self) -> None:
        if not self._closed:
            self._active()
            self.db.close()
            self._lock.close()
            self._closed = True
