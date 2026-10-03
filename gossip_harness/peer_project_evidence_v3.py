"""Receiver-owned Git contributions and complete-frontier source selection.

Only this local host adapter holds the financial journal verifier. Peer notices
are data, not proofs. Registration imports actual arrived bundles into retained
quarantine, checks their complete source against the accounted patch, and
publishes normalized records plus package-owned bytes for model inspection.
No candidate source is executed and no protected Git reference is advanced.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, dataclass
import fcntl
import hashlib
from pathlib import Path
import sqlite3
import threading
from typing import Any, Callable, Iterator, Protocol

from .gitstore import GitStore
from .peer_candidate_v2 import (bundle_manifest_digest, named_sources, result_payload_bytes,
                                strictdecode_candidate_notice)
from .peer_coding_dispatch_v1 import canonical_payload, source_digest
from .peer_git_bundle_v1 import import_bundle
from .peer_project_contract_v2 import (
    PACKAGES, CandidateOffer, Context, DispatchBinding, DispatchReply, EvidenceRef,
    LocalViewManifest, NamedSource, SelectedOffer, SelectionManifest, WorkKey,
    decode, encode, from_dict, identifier, identity, resolve_local, sha256, to_dict,
    worker_request_digest,
)
from .peer_store_v1 import canonical_bytes, strict_loads
from .worker import WorkerRequest, WorkerResult, _path_valid

PROTOCOL = "peer-project-evidence-v3"
FRONTIER_PROTOCOL = "peer-project-frontier-v3"
CONTRIBUTION_PROTOCOL = "peer-project-contribution-v3"
SELECTION_PROTOCOL = "peer-project-selection-v3"
MAX_RECORDS = 256
MAX_JSON_BYTES = 2_000_000


class EvidenceError(ValueError):
    """Received evidence does not establish the registered source claim."""


class EvidenceUnavailable(FileNotFoundError):
    """A required exact notice or payload has not arrived locally."""


class EvidenceMesh(Protocol):
    node_id: str
    def arrived(self) -> tuple[EvidenceRef, ...]: ...
    def want(self, ref: EvidenceRef) -> bool: ...
    def resolve(self, ref: EvidenceRef) -> bytes | None: ...
    def publish(self, kind: str, payload: bytes, command_id: str) -> EvidenceRef: ...


TerminalVerifier = Callable[[DispatchBinding], dict[str, Any]]
ViewVerifier = Callable[[DispatchBinding, WorkerRequest, LocalViewManifest], LocalViewManifest]


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise EvidenceError(message)


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _json(value: Any) -> bytes:
    return canonical_bytes(value, max_bytes=MAX_JSON_BYTES)


def _inside(path: str, roots: tuple[str, ...]) -> bool:
    return any(path == root or path.startswith(root + "/") for root in roots)


@dataclass(frozen=True, slots=True)
class ContributionSlot:
    """One prospectively registered action slot and its exact allowed source base."""
    actor: str
    work: WorkKey
    profile_id: str
    profile_sha256: str
    allowed_paths: tuple[str, ...]
    base_sha: str
    instructions_sha256: str
    attempt_limit: int = 1

    def __post_init__(self) -> None:
        identifier(self.actor)
        identifier(self.profile_id)
        sha256(self.profile_sha256)
        sha256(self.instructions_sha256)
        _require(type(self.attempt_limit) is int and 1 <= self.attempt_limit <= 1024, "Invalid attempt limit")
        _require(type(self.work) is WorkKey and type(self.allowed_paths) is tuple
                 and 0 < len(self.allowed_paths) <= 128
                 and len(set(self.allowed_paths)) == len(self.allowed_paths)
                 and all(_path_valid(path) for path in self.allowed_paths), "Invalid registered slot paths")
        _require(type(self.base_sha) is str and len(self.base_sha) == 40
                 and all(char in "0123456789abcdef" for char in self.base_sha), "Invalid registered Git base")

    def body(self) -> dict[str, Any]:
        return {"actor": self.actor, "work": to_dict(self.work), "profile_id": self.profile_id,
                "profile_sha256": self.profile_sha256, "allowed_paths": list(self.allowed_paths),
                "base_sha": self.base_sha, "instructions_sha256": self.instructions_sha256,
                "attempt_limit": self.attempt_limit}


class EvidenceRegistry:
    """Bounded immutable registrations for one declared complete frontier.

    ``verified_terminal`` and ``verified_view`` are trusted host capabilities,
    never peer/RPC inputs. The latter must reconstruct the exact v3 materialized
    worker request against locally arrived bytes and the host's directive policy.
    All bases and slot membership are fixed before registration. A new repair
    round uses a new registry and explicit slots/bases, including inherited work.
    """

    def __init__(self, root: Path, mesh: EvidenceMesh, *, context: Context,
                 authority_config_sha256: str, package_scopes: dict[str, tuple[str, ...]],
                 slots: tuple[ContributionSlot, ...], selector: ContributionSlot,
                 bases: dict[str, dict[str, str]], eligibility_policy_sha256: str,
                 view_policies: dict[tuple[str, int], str], verified_terminal: TerminalVerifier,
                 verified_view: ViewVerifier, result_producer: str = "finance"):
        self.root = Path(root).absolute()
        _require(self.root.resolve() == self.root and not self.root.is_symlink(), "Unsafe registry path")
        _require(type(context) is Context and type(selector) is ContributionSlot
                 and type(slots) is tuple and 4 <= len(slots) <= 64
                 and all(type(slot) is ContributionSlot for slot in slots), "Invalid registered frontier")
        for value in (authority_config_sha256, eligibility_policy_sha256):
            sha256(value)
        _require(type(view_policies) is dict and 0 < len(view_policies) <= 256, "Registered view policies required")
        for key, value in view_policies.items():
            _require(type(key) is tuple and len(key) == 2 and key[0] in ("build", "repair", "select_source", "review")
                     and type(key[1]) is int and key[1] >= 0, "Invalid view policy key")
            sha256(value)
        identifier(mesh.node_id)
        identifier(result_producer)
        _require(callable(verified_terminal) and callable(verified_view), "Trusted verifiers are required")
        _require(type(package_scopes) is dict and set(package_scopes) == set(PACKAGES), "Exactly four scopes required")
        for roots in package_scopes.values():
            _require(type(roots) is tuple and bool(roots) and len(set(roots)) == len(roots)
                     and all(_path_valid(path) for path in roots), "Invalid package roots")
        flattened = [(package, root) for package, roots in package_scopes.items() for root in roots]
        _require(all(first == second or not (_inside(a, (b,)) or _inside(b, (a,)))
                     for first, a in flattened for second, b in flattened), "Overlapping package scopes")
        _require(type(bases) is dict and 0 < len(bases) <= 16, "Invalid registered source bases")
        for base, files in bases.items():
            _require(type(base) is str and len(base) == 40 and all(c in "0123456789abcdef" for c in base), "Invalid base OID")
            named_sources(files)
        self.slots = tuple(sorted(slots, key=lambda s: (s.work.package_id, s.work.slot_id, s.actor)))
        keys = [(slot.actor, slot.work) for slot in self.slots]
        _require(len(set(keys)) == len(keys) and {slot.work.package_id for slot in slots} == set(PACKAGES),
                 "Duplicate or incomplete registered frontier")
        for slot in (*slots, selector):
            _require(slot.base_sha in bases, "Unregistered action base")
        for slot in slots:
            _require(all(_inside(path, package_scopes[slot.work.package_id]) for path in slot.allowed_paths),
                     "Registered writable paths exceed package scope")
        _require(selector.allowed_paths == ("selection.json",), "Selector must write only selection.json")
        self.mesh, self.context, self.selector = mesh, context, selector
        self.authority_config_sha256 = authority_config_sha256
        self.package_scopes = dict(package_scopes)
        self.bases = {base: dict(files) for base, files in bases.items()}
        self.eligibility_policy_sha256, self.view_policies = eligibility_policy_sha256, dict(view_policies)
        self.verified_terminal, self.view_verifier, self.result_producer = verified_terminal, verified_view, result_producer
        config = {"protocol": PROTOCOL, "root": str(self.root), "receiver": mesh.node_id,
                  "context": to_dict(context), "authority_config_sha256": authority_config_sha256,
                  "slots": [slot.body() for slot in self.slots], "selector": selector.body(),
                  "bases": {base: source_digest(files) for base, files in sorted(self.bases.items())},
                  "package_scopes": {key: list(value) for key, value in sorted(package_scopes.items())},
                  "eligibility_policy_sha256": eligibility_policy_sha256,
                  "view_policies": [{"kind": kind, "generation": generation, "policy_sha256": digest}
                                    for (kind, generation), digest in sorted(view_policies.items())],
                  "result_producer": result_producer,
                  "missing_slot_policy": "none-permitted", "max_records": MAX_RECORDS}
        self.config = _json(config)
        self.config_sha256 = _sha(self.config)
        self.root.mkdir(parents=True, exist_ok=True)
        for name in ("registry.sqlite", "registry.lock"):
            _require(not (self.root / name).is_symlink(), "Unsafe registry storage")
        self._lock, self._lock_depth = threading.RLock(), 0
        self.db = sqlite3.connect(self.root / "registry.sqlite")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("CREATE TABLE IF NOT EXISTS config (id INTEGER PRIMARY KEY, body BLOB, count INTEGER, chain TEXT)")
        self.db.execute("CREATE TABLE IF NOT EXISTS records (ordinal INTEGER UNIQUE, key TEXT PRIMARY KEY, body BLOB, digest TEXT)")
        try:
            with self._locked():
                row = self.db.execute("SELECT body FROM config WHERE id=1").fetchone()
                if row is None:
                    _require(not self.db.execute("SELECT 1 FROM records").fetchone(), "Missing registry configuration")
                    with self.db:
                        self.db.execute("INSERT INTO config VALUES (1,?,0,?)", (self.config, self.config_sha256))
                self._records()
        except BaseException:
            self.db.close()
            raise

    def close(self) -> None:
        self.db.close()

    @contextmanager
    def _locked(self) -> Iterator[None]:
        # Callbacks may compose normalized_refs into view authentication.
        # Separate flock descriptors would self-block in the same process, so
        # only the outermost owned call acquires the interprocess file lock.
        with self._lock:
            if self._lock_depth:
                self._lock_depth += 1
                try:
                    yield
                finally:
                    self._lock_depth -= 1
                return
            path = self.root / "registry.lock"
            _require(not path.is_symlink(), "Unsafe registry lock")
            with path.open("a+b") as lock:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
                self._lock_depth = 1
                try:
                    yield
                finally:
                    self._lock_depth = 0

    def _records(self) -> dict[str, dict[str, Any]]:
        row = self.db.execute("SELECT body,count,chain FROM config WHERE id=1").fetchone()
        _require(row is not None and row[0] == self.config, "Registry configuration changed")
        assert row is not None
        records: dict[str, dict[str, Any]] = {}
        chain = self.config_sha256
        rows = self.db.execute("SELECT ordinal,key,body,digest FROM records ORDER BY ordinal").fetchall()
        _require(len(rows) <= MAX_RECORDS, "Registry capacity exceeded")
        for ordinal, (number, key, raw, digest) in enumerate(rows, 1):
            _require(number == ordinal and _sha(raw) == digest and key not in records, "Registry record corruption")
            value = strict_loads(raw, max_bytes=MAX_JSON_BYTES)
            _require(type(value) is dict and _json(value) == raw, "Registry record is not canonical")
            records[key] = value
            chain = _sha(chain.encode() + b"\0" + key.encode() + b"\0" + digest.encode())
        _require(row[1:] == (len(rows), chain), "Registry membership changed")
        return records

    def _put(self, key: str, value: dict[str, Any]) -> None:
        records = self._records()
        if key in records:
            _require(records[key] == value, "Immutable registration changed")
            return
        _require(len(records) < MAX_RECORDS, "Registry capacity reached")
        raw, prior = _json(value), self.db.execute("SELECT chain FROM config WHERE id=1").fetchone()[0]
        digest = _sha(raw)
        chain = _sha(prior.encode() + b"\0" + key.encode() + b"\0" + digest.encode())
        with self.db:
            self.db.execute("INSERT INTO records VALUES (?,?,?,?)", (len(records) + 1, key, raw, digest))
            _require(self.db.execute("UPDATE config SET count=?,chain=? WHERE id=1 AND count=? AND chain=?",
                                     (len(records) + 1, chain, len(records), prior)).rowcount == 1, "Registry CAS conflict")

    def _arrived(self, ref: EvidenceRef) -> bytes:
        _require(type(ref) is EvidenceRef, "Exact evidence reference required")
        arrivals = self.mesh.arrived()
        if ref not in arrivals or self.mesh.resolve(ref) is None:
            self.mesh.want(ref)
            raise EvidenceUnavailable("Exact local evidence is not yet available")
        return resolve_local(ref, arrivals, self.mesh.resolve(ref))

    def _publish(self, kind: str, raw: bytes, command: str) -> EvidenceRef:
        ref = self.mesh.publish(kind, raw, command)
        _require(ref.producer == self.mesh.node_id and ref.kind == kind and ref.payload_sha256 == _sha(raw)
                 and self._arrived(ref) == raw, "Receiver publication differs")
        return ref

    def _terminal(self, binding: DispatchBinding, role_ref: EvidenceRef,
                  slot: ContributionSlot) -> tuple[WorkerRequest, WorkerResult, LocalViewManifest]:
        action = binding.action
        _require(action.context == self.context and binding.authority_config_sha256 == self.authority_config_sha256
                 and action.actor == slot.actor and action.work == slot.work and action.profile_id == slot.profile_id
                 and binding.profile_sha256 == slot.profile_sha256, "Unregistered action or authority")
        proof = self.verified_terminal(binding)
        _require(type(proof) is dict and set(proof) == {"binding", "action", "reply", "result", "result_payload", "worker_request"},
                 "Invalid internal terminal proof")
        request, result, reply = proof["worker_request"], proof["result"], proof["reply"]
        _require(type(request) is WorkerRequest and type(result) is WorkerResult and type(reply) is DispatchReply,
                 "Invalid terminal types")
        _require(proof["binding"] == binding and proof["action"] == action and reply.binding == binding
                 and reply.state == "completed" and reply.usage_units == result.usage_units
                 and reply.result_payload_sha256 == _sha(result_payload_bytes(result))
                 and proof["result_payload"] == {"kind": "result", "payload": asdict(result)}
                 and not result.metadata.get("halt"), "Outcome is not the exact successful accounted result")
        _require(worker_request_digest(request) == binding.normalized_worker_request_sha256
                 and request.allowed_paths == slot.allowed_paths and request.base_sha == slot.base_sha
                 and request.files == self.bases[slot.base_sha], "Request, allowed paths, or base differs")
        recipe = strict_loads(request.feedback, max_bytes=MAX_JSON_BYTES)
        _require(type(recipe) is dict and recipe.get("protocol") == "peer-project-views-v3"
                 and type(recipe.get("directive")) is dict
                 and _sha(request.instructions.encode("utf-8")) == slot.instructions_sha256
                 and 1 <= request.attempt <= slot.attempt_limit
                 and recipe["directive"].get("attempt_limit") == slot.attempt_limit,
                 "Request template or finite attempt policy differs")
        request_body = asdict(request)
        request_body["allowed_paths"] = list(request.allowed_paths)
        _require(self._arrived(action.worker_payload_ref) == canonical_payload({"worker_request": request_body,
                    "view_manifest_sha256": action.view_manifest_sha256}), "Arrived worker request differs")
        _require(role_ref.producer == action.actor and role_ref.kind == "role-result", "Wrong role-result producer")
        raw = self._arrived(role_ref)
        body = strict_loads(raw, max_bytes=MAX_JSON_BYTES)
        _require(type(body) is dict and set(body) == {"protocol", "actor", "action", "reply", "view_manifest", "result_ref"}
                 and body["protocol"] == "peer-role-loop-v2" and body["actor"] == action.actor
                 and body["action"] == to_dict(action) and body["reply"] == to_dict(reply)
                 and _json(body) == raw, "Role result is not the exact closed completion")
        result_ref = from_dict(EvidenceRef, body["result_ref"])
        _require(result_ref.producer == self.result_producer and result_ref.kind == "financial-result"
                 and result_ref.payload_sha256 == reply.result_payload_sha256
                 and self._arrived(result_ref) == result_payload_bytes(result), "Arrived finance result differs")
        claimed = from_dict(LocalViewManifest, body["view_manifest"])
        view = self.view_verifier(binding, request, claimed)
        policy_key = action.kind, action.work.generation
        _require(policy_key in self.view_policies, "Action has no registered view policy")
        _require(type(view) is LocalViewManifest and view == claimed and view.context == self.context
                 and view.policy_sha256 == self.view_policies[policy_key] and identity(view) == action.view_manifest_sha256
                 and view.materialized_content_sha256 == worker_request_digest(request), "Actual materialized view differs")
        view.require_complete()
        for ref in (view.source_ref, *view.evidence_refs):
            self._arrived(ref)
        return request, result, view

    def _slot(self, offer: CandidateOffer) -> ContributionSlot:
        matches = [slot for slot in self.slots if slot.actor == offer.dispatch.action.actor
                   and slot.work == offer.dispatch.action.work]
        _require(len(matches) == 1, "Offer has no registered frontier slot")
        return matches[0]

    def _contribution(self, offer: CandidateOffer, offer_ref: EvidenceRef, role_ref: EvidenceRef,
                      *, import_new: bool) -> tuple[dict[str, str], tuple[NamedSource, ...]]:
        _require(offer_ref.producer == offer.dispatch.action.actor and offer_ref.kind == "candidate-offer", "Wrong offer producer")
        decoded, manifest = strictdecode_candidate_notice(self._arrived(offer_ref),
            expected_contract_sha256=self.context.execution_contract_sha256, expected_actor=offer_ref.producer)
        _require(decoded == offer and offer.dispatch.action.kind in ("build", "repair"), "Different or noncoding offer")
        slot = self._slot(offer)
        request, result, _ = self._terminal(offer.dispatch, role_ref, slot)
        _require(offer.result_payload_sha256 == _sha(result_payload_bytes(result))
                 and manifest["base_sha"] == request.base_sha, "Offer result or base differs")
        _require(all(path in request.allowed_paths for path in result.changes), "Patch exceeds exact path scope")
        expected = dict(request.files)
        for path, content in result.changes.items():
            if content is None:
                expected.pop(path, None)
            else:
                expected[path] = content
        _require(expected != request.files and source_digest(expected) == offer.source_sha256, "Offer source differs from accounted patch")
        bundle = self._arrived(offer.bundle_ref)
        quarantine = self.root / ("quarantine-" + identity(offer))
        _require(not quarantine.is_symlink(), "Unsafe quarantine path")
        if import_new and not quarantine.exists():
            attempt_key = "quarantine-intent-" + identity(offer)
            _require(attempt_key not in self._records(),
                     "Prior import attempt has no complete quarantine; refusing retry")
            self._put(attempt_key, {"kind": "quarantine-intent", "offer_sha256": identity(offer),
                                   "bundle_ref": to_dict(offer.bundle_ref),
                                   "manifest_sha256": offer.bundle_manifest_sha256})
            store = import_bundle(bundle, manifest, quarantine)
        else:
            _require((quarantine / "receipt.json").is_file() and not (quarantine / "failure.json").exists(),
                     "Prior quarantine is incomplete or failed; refusing retry")
            retained = strict_loads((quarantine / "receipt.json").read_bytes(), max_bytes=8_000_000)
            _require(retained.get("status") == "quarantined" and retained.get("manifest") == manifest
                     and (quarantine / "received.bundle").read_bytes() == bundle, "Retained quarantine receipt differs")
            store = GitStore(quarantine / "quarantine.git")
        _require(store.read_files(slot.base_sha) == self.bases[slot.base_sha]
                 and store.read_files(offer.commit_oid) == expected, "Imported Git tree is not the exact base plus accounted patch")
        _require(bundle_manifest_digest(manifest) == offer.bundle_manifest_sha256, "Bundle manifest changed")
        owned = {path: text for path, text in expected.items() if _inside(path, self.package_scopes[slot.work.package_id])}
        _require(bool(owned), "Empty package contribution")
        return owned, named_sources(owned)

    def register_offer(self, offer_ref: EvidenceRef, role_result_ref: EvidenceRef) -> CandidateOffer:
        with self._locked():
            offer, _ = strictdecode_candidate_notice(self._arrived(offer_ref),
                expected_contract_sha256=self.context.execution_contract_sha256, expected_actor=offer_ref.producer)
            slot = self._slot(offer)
            key = "offer-" + _sha(_json(slot.body()))
            registration = {"kind": "offer", "offer": to_dict(offer), "offer_ref": to_dict(offer_ref),
                            "role_result_ref": to_dict(role_result_ref)}
            # Intent fixes this slot before any import. Failed quarantines remain
            # retained and cannot be overwritten by another candidate or retried.
            self._put(key, registration)
            owned, _ = self._contribution(offer, offer_ref, role_result_ref, import_new=True)
            normalized = self._publish("candidate-offer", encode(offer), "verified-offer-" + identity(offer))
            contribution = self._publish("candidate-contribution", _json({"protocol": CONTRIBUTION_PROTOCOL,
                "offer_sha256": identity(offer), "commit_oid": offer.commit_oid, "files": owned}),
                "verified-contribution-" + identity(offer))
            self._put("ready-" + key, {"kind": "ready", "offer_sha256": identity(offer),
                "normalized_ref": to_dict(normalized), "contribution_ref": to_dict(contribution)})
            return offer

    def _registered(self, offer: CandidateOffer) -> tuple[dict[str, Any], dict[str, Any]]:
        slot = self._slot(offer)
        key = "offer-" + _sha(_json(slot.body()))
        records = self._records()
        _require(key in records and "ready-" + key in records and records[key]["offer"] == to_dict(offer),
                 "Offer has no complete exact registration")
        return records[key], records["ready-" + key]

    def verified_contribution(self, offer: CandidateOffer) -> tuple[NamedSource, ...]:
        with self._locked():
            registration, _ = self._registered(offer)
            _, sources = self._contribution(offer, from_dict(EvidenceRef, registration["offer_ref"]),
                from_dict(EvidenceRef, registration["role_result_ref"]), import_new=False)
            return sources

    def candidate_files(self, offer: CandidateOffer) -> dict[str, str]:
        """Return owned source bytes only after revalidating actual Git provenance."""
        with self._locked():
            registration, _ = self._registered(offer)
            files, _ = self._contribution(offer, from_dict(EvidenceRef, registration["offer_ref"]),
                from_dict(EvidenceRef, registration["role_result_ref"]), import_new=False)
            return files

    def normalized_refs(self, offer: CandidateOffer) -> tuple[EvidenceRef, EvidenceRef]:
        """Return exact receiver publications after rechecking owned Git bytes."""
        with self._locked():
            registration, ready = self._registered(offer)
            owned, _ = self._contribution(offer, from_dict(EvidenceRef, registration["offer_ref"]),
                from_dict(EvidenceRef, registration["role_result_ref"]), import_new=False)
            record, contribution = (from_dict(EvidenceRef, ready[key])
                                    for key in ("normalized_ref", "contribution_ref"))
            _require(self._arrived(record) == encode(offer) and self._arrived(contribution) == _json({
                "protocol": CONTRIBUTION_PROTOCOL, "offer_sha256": identity(offer), "commit_oid": offer.commit_oid,
                "files": owned}), "Normalized contribution differs")
            return record, contribution

    def verified_candidate_materials(self, view: LocalViewManifest) -> None:
        """Bind review/selection labels to complete authenticated package bytes.

        The pure view materializer verifies input reconstruction. This additional
        trusted callback forbids forged or truncated contribution labels, even
        when their bytes are a subset of an otherwise correct whole target.
        """
        _require(type(view) is LocalViewManifest and view.context == self.context, "Wrong candidate view context")
        view.require_complete()
        refs = (view.source_ref, *view.evidence_refs)
        records = [ref for ref in refs if ref.kind == "candidate-offer"]
        contributions = {ref for ref in refs if ref.kind == "candidate-contribution"}
        expected: set[EvidenceRef] = set()
        for ref in records:
            offer = decode(self._arrived(ref), CandidateOffer,
                           expected_contract_sha256=self.context.execution_contract_sha256)
            normalized, contribution = self.normalized_refs(offer)
            _require(ref == normalized and contribution in contributions
                     and ref.event_id in view.required_evidence_ids
                     and contribution.event_id in view.required_evidence_ids,
                     "View uses unauthenticated candidate material references")
            expected.add(contribution)
        _require(expected == contributions, "View has orphaned candidate source labels")

    def frontier(self) -> EvidenceRef:
        with self._locked():
            return self._frontier()

    def _frontier(self) -> EvidenceRef:
        records, members = self._records(), []
        for slot in self.slots:
            key = "offer-" + _sha(_json(slot.body()))
            _require(key in records and "ready-" + key in records, "Complete frontier has missing registered slots")
            registration, ready = records[key], records["ready-" + key]
            offer = from_dict(CandidateOffer, registration["offer"])
            owned, _ = self._contribution(offer, from_dict(EvidenceRef, registration["offer_ref"]),
                from_dict(EvidenceRef, registration["role_result_ref"]), import_new=False)
            normalized, contribution = (from_dict(EvidenceRef, ready[name]) for name in ("normalized_ref", "contribution_ref"))
            _require(self._arrived(normalized) == encode(offer) and self._arrived(contribution) == _json({
                "protocol": CONTRIBUTION_PROTOCOL, "offer_sha256": identity(offer), "commit_oid": offer.commit_oid,
                "files": owned}), "Normalized contribution differs")
            members.append({"slot": to_dict(slot.work), "actor": slot.actor, "offer_sha256": identity(offer),
                            "offer_ref": to_dict(normalized), "contribution_ref": to_dict(contribution)})
        return self._publish("selection-frontier", _json({"protocol": FRONTIER_PROTOCOL,
            "context": to_dict(self.context), "eligibility_policy_sha256": self.eligibility_policy_sha256,
            "offers": members, "missing_slots": []}), "frontier-" + self.config_sha256)

    def _selection(self, binding: DispatchBinding, role_ref: EvidenceRef) -> SelectionManifest:
        _require(binding.action.kind == "select_source", "Source selection action required")
        _, result, view = self._terminal(binding, role_ref, self.selector)
        _require(set(result.changes) == {"selection.json"} and type(result.changes["selection.json"]) is str,
                 "Selector must return exactly selection.json")
        response = result.changes["selection.json"]
        assert isinstance(response, str)
        value = strict_loads(response, max_bytes=MAX_JSON_BYTES)
        _require(type(value) is dict and set(value) == {"protocol", "eligibility_policy_sha256", "frontier_sha256",
                 "eligible_offer_sha256s", "selected"} and value["protocol"] == SELECTION_PROTOCOL
                 and value["eligibility_policy_sha256"] == self.eligibility_policy_sha256, "Invalid selector artifact")
        frontier_ref = self._frontier()
        frontier = strict_loads(self._arrived(frontier_ref), max_bytes=MAX_JSON_BYTES)
        expected_refs = [frontier_ref]
        for entry in frontier["offers"]:
            expected_refs.extend(from_dict(EvidenceRef, entry[name]) for name in ("offer_ref", "contribution_ref"))
        _require(all(ref in view.evidence_refs and ref.event_id in view.required_evidence_ids for ref in expected_refs),
                 "Selector did not materialize the complete exact frontier")
        eligible = tuple(entry["offer_sha256"] for entry in frontier["offers"])
        _require(value["frontier_sha256"] == frontier_ref.payload_sha256
                 and value["eligible_offer_sha256s"] == list(eligible), "Selector eligibility frontier differs")
        _require(type(value["selected"]) is list, "Selected package list required")
        selected = tuple(from_dict(SelectedOffer, entry) for entry in value["selected"])
        _require(tuple(entry.package_id for entry in selected) == PACKAGES, "Exactly one ordered choice per package required")
        membership = {entry["offer_sha256"]: entry["slot"]["package_id"] for entry in frontier["offers"]}
        _require(all(membership.get(entry.offer_sha256) == entry.package_id for entry in selected), "Selection crosses package scope")
        return SelectionManifest(self.context, self.eligibility_policy_sha256, binding,
                                 identity(view), eligible, (), selected)

    def materialize_selection(self, binding: DispatchBinding, role_result_ref: EvidenceRef) -> SelectionManifest:
        with self._locked():
            selection = self._selection(binding, role_result_ref)
            ref = self._publish("selection-manifest", encode(selection), "selection-" + identity(selection))
            self._put("selection-" + identity(selection), {"kind": "selection", "selection": to_dict(selection),
                "role_result_ref": to_dict(role_result_ref), "selection_ref": to_dict(ref)})
            return selection

    def verified_selection(self, selection: SelectionManifest) -> SelectionManifest:
        with self._locked():
            key, records = "selection-" + identity(selection), self._records()
            _require(key in records and records[key]["selection"] == to_dict(selection), "Selection is not registered")
            row = records[key]
            _require(self._arrived(from_dict(EvidenceRef, row["selection_ref"])) == encode(selection), "Selection bytes differ")
            actual = self._selection(selection.selecting_dispatch, from_dict(EvidenceRef, row["role_result_ref"]))
            _require(actual == selection, "Actual selector result differs")
            return actual

    def selection_ref(self, selection: SelectionManifest) -> EvidenceRef:
        self.verified_selection(selection)
        return from_dict(EvidenceRef, self._records()["selection-" + identity(selection)]["selection_ref"])
