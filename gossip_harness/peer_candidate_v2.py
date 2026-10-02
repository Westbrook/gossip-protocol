"""Durable role-local proposals from authenticated v2 coding completions.

The trusted callback reads the role's retained authenticated completion journal;
model-authored assertions are never completion proof. GitStore and the existing
bundle boundary are reused unchanged. This module never executes candidate code,
contacts finance, promotes a commit, or claims independent acceptance.

CandidateOffer.source_sha256 uses the legacy full-files source_digest encoding.
Bundle manifest SHA256 covers canonical_bytes(manifest) from peer_store_v1 with
UTF-8, sorted keys, compact separators, ensure_ascii=False, and no added newline.
NamedSource digests separately cover each file's raw UTF-8 bytes.
"""
from __future__ import annotations

import base64
from dataclasses import asdict, dataclass
import fcntl
import hashlib
import os
from pathlib import Path
import re
from typing import Any, Callable, Protocol
import uuid

from .gitstore import GitStore, _run
from .peer_coding_dispatch_v1 import canonical_payload, source_digest
from .peer_git_bundle_v1 import MAX_BUNDLE_BYTES, _header, _manifest, _path, export_bundle
from .peer_project_contract_v2 import (
    PACKAGES, CandidateOffer, DispatchBinding, DispatchReply, EvidenceRef, NamedSource,
    from_dict, identifier, sha256, to_dict, worker_request_digest,
)
from .peer_store_v1 import canonical_bytes, strict_loads
from .verification_journal import JournalCorrupt, _result
from .worker import WorkerRequest, WorkerResult

PROTOCOL = "peer-candidate-v2"
MAX_JOURNAL_BYTES = 2 * MAX_BUNDLE_BYTES + 4_000_000
MAX_NOTICE_BYTES = 512_000
CRASH_POINTS = {"after_intent", "after_proposal", "after_bundle", "after_bundle_publish", "after_publish"}
_OID = re.compile(r"[0-9a-f]{40}\Z")


class CandidateError(ValueError):
    """A local candidate cannot be bound to its durable coding completion."""


class CandidateTransport(Protocol):
    node_id: str

    def publish(self, kind: str, payload: bytes, command_id: str) -> EvidenceRef: ...

    def resolve(self, ref: EvidenceRef) -> bytes | None: ...


CompletedProof = Callable[[DispatchBinding, WorkerRequest, WorkerResult, str], DispatchReply]


@dataclass(frozen=True)
class CandidatePublication:
    offer: CandidateOffer
    offer_ref: EvidenceRef


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _json(value: Any) -> bytes:
    return canonical_bytes(value, max_bytes=MAX_JOURNAL_BYTES)


def bundle_manifest_digest(manifest: dict[str, Any]) -> str:
    """SHA256 of the existing bundle manifest's explicit canonical JSON bytes."""
    return _sha(canonical_bytes(_manifest(manifest), max_bytes=MAX_NOTICE_BYTES))


def result_payload_bytes(result: WorkerResult) -> bytes:
    if type(result) is not WorkerResult:
        raise CandidateError("Expected the retained WorkerResult")
    try:
        _result(asdict(result))
    except JournalCorrupt as error:
        raise CandidateError("Retained WorkerResult has invalid fields") from error
    return canonical_payload({"kind": "result", "payload": asdict(result)})


def named_sources(files: dict[str, str]) -> tuple[NamedSource, ...]:
    """Name every ordinary text source by raw file bytes, independently of source_digest."""
    _files(files)
    return tuple(NamedSource(name, _sha(content.encode("utf-8"))) for name, content in sorted(files.items()))


def validate_receipt_sources(offer: CandidateOffer, files: dict[str, str],
                             sources: tuple[NamedSource, ...], *, require_complete: bool = False) -> None:
    """Check full candidate identity and exact named-source membership only.

    This is a receiver helper, not validation of an execution receipt's evaluator,
    purpose, limits or success. The receiver obtains files from verified quarantine.
    """
    if type(offer) is not CandidateOffer or type(sources) is not tuple or not sources:
        raise CandidateError("Expected candidate and nonempty named-source receipt")
    available = {item.path: item.sha256 for item in named_sources(files)}
    if source_digest(files) != offer.source_sha256:
        raise CandidateError("Receipt source tree differs from the candidate")
    if (any(type(item) is not NamedSource for item in sources)
            or len({item.path for item in sources}) != len(sources)
            or any(available.get(item.path) != item.sha256 for item in sources)
            or require_complete and {item.path for item in sources} != set(available)):
        raise CandidateError("Receipt named source is absent, duplicated or has different bytes")


def strictdecode_candidate_notice(payload: bytes, *, expected_contract_sha256: str,
                                  expected_actor: str | None = None) -> tuple[CandidateOffer, dict[str, Any]]:
    """Decode the closed notice and bind its manifest; this does not import its bundle."""
    sha256(expected_contract_sha256)
    value = strict_loads(payload, max_bytes=MAX_NOTICE_BYTES)
    if (type(payload) is not bytes or type(value) is not dict
            or set(value) != {"protocol", "offer", "bundle_manifest"}
            or value["protocol"] != PROTOCOL
            or canonical_bytes(value, max_bytes=MAX_NOTICE_BYTES) != payload):
        raise CandidateError("Invalid canonical candidate notice")
    offer = from_dict(CandidateOffer, value["offer"])
    manifest = _manifest(value["bundle_manifest"])
    if (offer.dispatch.action.context.execution_contract_sha256 != expected_contract_sha256
            or expected_actor is not None and offer.dispatch.action.actor != expected_actor
            or manifest["object_format"] != "sha1"
            or manifest["offered_sha"] != offer.commit_oid
            or manifest["bundle_sha256"] != offer.bundle_ref.payload_sha256
            or bundle_manifest_digest(manifest) != offer.bundle_manifest_sha256):
        raise CandidateError("Candidate notice identity or bundle binding differs")
    return offer, manifest


def _files(files: Any) -> None:
    if type(files) is not dict or any(type(name) is not str or type(text) is not str for name, text in files.items()):
        raise CandidateError("Source must contain text files")
    for name, text in files.items():
        try:
            _path(name.encode("ascii"))
            raw = text.encode("utf-8")
        except (UnicodeError, ValueError) as error:
            raise CandidateError("Source exceeds the supported repository profile") from error
        if b"\x00" in raw:
            raise CandidateError("Source contains binary content")


def _read(path: Path) -> tuple[Any, bytes]:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > MAX_JOURNAL_BYTES:
        raise CandidateError("Missing, unsafe or oversized candidate journal")
    raw = path.read_bytes()
    value = strict_loads(raw, max_bytes=MAX_JOURNAL_BYTES)
    if _json(value) != raw:
        raise CandidateError("Noncanonical candidate journal")
    return value, raw


def _save(path: Path, value: Any) -> None:
    """Install immutable bytes atomically; never overwrite failed or partial evidence."""
    raw = _json(value)
    if path.exists() or path.is_symlink():
        if _read(path)[1] != raw:
            raise CandidateError("Immutable candidate artifact changed")
        return
    temporary = path.parent / (path.name + ".pending-" + uuid.uuid4().hex)
    with temporary.open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    try:
        os.link(temporary, path)
    except FileExistsError:
        if _read(path)[1] != raw:
            raise CandidateError("Concurrent candidate artifact differs")
    temporary.unlink()
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class CandidatePublisher:
    """One private GitStore publisher; each action owns immutable restart stages.

    package_scopes lists registered repository roots (or exact files). The worker
    may change only exact allowed_paths that also fall within its package scope.
    The host must supply a trusted local journal callback, never model metadata.
    """

    def __init__(self, root: Path, transport: CandidateTransport, store: GitStore, *, actor: str,
                 baseline_sha: str, package_scopes: dict[str, tuple[str, ...]],
                 execution_contract_sha256: str, completed_proof: CompletedProof,
                 crash_hook: Callable[[str], None] | None = None):
        identifier(actor)
        sha256(execution_contract_sha256)
        if (transport.node_id != actor or type(baseline_sha) is not str or _OID.fullmatch(baseline_sha) is None
                or type(package_scopes) is not dict or not package_scopes
                or any(package not in PACKAGES for package in package_scopes)
                or not callable(completed_proof)):
            raise CandidateError("Invalid role candidate configuration")
        for roots in package_scopes.values():
            if type(roots) is not tuple or not roots or len(set(roots)) != len(roots):
                raise CandidateError("Package scope must be a nonempty unique tuple")
            for name in roots:
                if type(name) is not str:
                    raise CandidateError("Invalid package path")
                _path(name.encode("ascii"))
        self.root = Path(root).absolute()
        if self.root.is_symlink() or self.root.resolve() != self.root:
            raise CandidateError("Candidate root must have a canonical nonsymlink path")
        self.transport, self.store, self.actor = transport, store, actor
        self.baseline_sha, self.completed_proof, self.crash_hook = baseline_sha, completed_proof, crash_hook
        self.package_scopes = dict(package_scopes)
        self.execution_contract_sha256 = execution_contract_sha256
        self.config = {"protocol": PROTOCOL, "actor": actor, "baseline_sha": baseline_sha,
                       "repository": str(store.path), "execution_contract_sha256": execution_contract_sha256,
                       "package_scopes": {key: list(paths) for key, paths in sorted(package_scopes.items())}}
        if store.head() != baseline_sha:
            raise CandidateError("Role GitStore accepted head differs from its declared seed")
        self.root.mkdir(parents=True, exist_ok=True)
        if not (self.root / "config.json").exists() and any(self.root.iterdir()):
            raise CandidateError("Partial candidate root has no durable configuration")
        _save(self.root / "config.json", self.config)

    def _crash(self, name: str) -> None:
        if self.crash_hook is not None:
            self.crash_hook(name)

    def _capture(self, dispatch: DispatchBinding, request: WorkerRequest, result: WorkerResult,
                 result_sha: str) -> tuple[dict[str, Any], dict[str, str]]:
        if type(dispatch) is not DispatchBinding or type(request) is not WorkerRequest:
            raise CandidateError("Expected exact dispatch and retained WorkerRequest")
        # Round-trip typed records and mutable legacy dataclasses before trusting them.
        dispatch = from_dict(DispatchBinding, to_dict(dispatch))
        request_sha = worker_request_digest(request)
        result_raw = result_payload_bytes(result)
        sha256(result_sha)
        if result.metadata.get("halt"):
            raise CandidateError("Halted coding outcome cannot publish a candidate")
        action = dispatch.action
        if (action.actor != self.actor or action.kind not in ("build", "repair")
                or action.context.execution_contract_sha256 != self.execution_contract_sha256
                or request_sha != dispatch.normalized_worker_request_sha256
                or request.task_id != dispatch.lease.task_id or request.base_sha != self.baseline_sha
                or _sha(result_raw) != result_sha):
            raise CandidateError("Coding actor, kind, request, base or result binding differs")
        scopes = self.package_scopes.get(action.work.package_id, ())
        if any(not any(name == scope or name.startswith(scope + "/") for scope in scopes)
               for name in request.allowed_paths):
            raise CandidateError("Worker writable paths exceed the registered package scope")
        _files(request.files)
        if any(name not in request.allowed_paths for name in result.changes):
            raise CandidateError("Patch exceeds exact worker allowed_paths")
        candidate_files = dict(request.files)
        for name, content in result.changes.items():
            if content is None:
                candidate_files.pop(name, None)
            else:
                candidate_files[name] = content
        _files(candidate_files)
        if candidate_files == request.files:
            raise CandidateError("Coding result has no effective source change")
        proof = self.completed_proof(dispatch, request, result, result_sha)
        if type(proof) is not DispatchReply:
            raise CandidateError("Trusted role journal did not return a DispatchReply")
        proof = from_dict(DispatchReply, to_dict(proof))
        if (proof.state != "completed" or proof.binding != dispatch
                or proof.result_payload_sha256 != result_sha or proof.usage_units != result.usage_units):
            raise CandidateError("Trusted local completion differs from candidate inputs")
        self._base(request.files)
        request_data = asdict(request)
        request_data["allowed_paths"] = list(request.allowed_paths)
        inputs = {"protocol": PROTOCOL, "config_sha256": _sha(_json(self.config)),
                  "dispatch": to_dict(dispatch), "request": request_data,
                  "result": strict_loads(result_raw, max_bytes=MAX_JOURNAL_BYTES),
                  "result_payload_sha256": result_sha, "completed_proof": to_dict(proof),
                  "base_source_sha256": source_digest(request.files),
                  "candidate_source_sha256": source_digest(candidate_files)}
        inputs["proposal_id"] = _sha(_json(inputs))
        return inputs, candidate_files

    def _tree(self, commit: str, files: dict[str, str]) -> None:
        if self.store.read_files(commit) != files:
            raise CandidateError("Git snapshot differs from the exact retained source")
        listing = _run(self.store.path, "ls-tree", "-r", "-t", "-z", "--full-tree", commit).stdout
        for entry in listing.split(b"\x00"):
            if not entry:
                continue
            metadata, separator, name = entry.partition(b"\t")
            if not separator or metadata.split(b" ")[:2] not in ([b"100644", b"blob"], [b"040000", b"tree"]):
                raise CandidateError("Git source has an unsupported file mode")
            _path(name)

    def _base(self, files: dict[str, str]) -> None:
        if self.store.head() != self.baseline_sha:
            raise CandidateError("Role accepted head moved from its immutable seed")
        self._tree(self.baseline_sha, files)

    def _proposal(self, commit: str, files: dict[str, str], proposal_id: str) -> None:
        if type(commit) is not str or _OID.fullmatch(commit) is None:
            raise CandidateError("Invalid retained proposal identity")
        self._tree(commit, files)
        if self.store._git("rev-parse", "--verify", "refs/harness/proposals/" + commit) != commit:
            raise CandidateError("Proposal ref differs from its commit")
        raw = _run(self.store.path, "cat-file", "commit", commit).stdout
        tree = raw.split(b"\n", 1)[0]
        actor = b"Gossip Harness <harness@example.invalid> 946684800 +0000"
        expected = (tree + b"\nparent " + self.baseline_sha.encode("ascii") + b"\nauthor " + actor
                    + b"\ncommitter " + actor + b"\n\nPeer candidate v2 " + proposal_id.encode("ascii") + b"\n")
        if re.fullmatch(rb"tree [0-9a-f]{40}", tree) is None or raw != expected:
            raise CandidateError("Git proposal is not the exact action and direct seed child")

    def _recover_proposal(self, files: dict[str, str], proposal_id: str) -> str:
        suffix = b"\n\nPeer candidate v2 " + proposal_id.encode("ascii") + b"\n"
        found = []
        for commit in self.store._git("for-each-ref", "--format=%(objectname)", "refs/harness/proposals/").splitlines():
            if _run(self.store.path, "cat-file", "commit", commit).stdout.endswith(suffix):
                self._proposal(commit, files, proposal_id)
                found.append(commit)
        if len(found) != 1:
            raise CandidateError("Interrupted proposal has no unique retained result; refusing another mutation")
        return found[0]

    def _publish(self, kind: str, payload: bytes, command_id: str) -> EvidenceRef:
        ref = self.transport.publish(kind, payload, command_id)
        if (type(ref) is not EvidenceRef or ref.producer != self.actor or ref.kind != kind
                or ref.payload_sha256 != _sha(payload) or self.transport.resolve(ref) != payload):
            raise CandidateError("Local transport publication differs from its intended bytes")
        return ref

    def publish(self, dispatch: DispatchBinding, request: WorkerRequest, result: WorkerResult,
                result_payload_sha256: str) -> CandidatePublication:
        # The owned lock serializes multiple adapters/processes using this root.
        lock_path = self.root / "publisher.lock"
        if lock_path.is_symlink():
            raise CandidateError("Candidate lock cannot be a symlink")
        with lock_path.open("a+b") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            return self._publish_locked(dispatch, request, result, result_payload_sha256)

    def _publish_locked(self, dispatch: DispatchBinding, request: WorkerRequest, result: WorkerResult,
                        result_payload_sha256: str) -> CandidatePublication:
        # Worker dataclasses contain mutable dictionaries. Materialize an owned
        # snapshot before the callback or Git can observe caller-side mutation.
        worker_request_digest(request)
        request_data = asdict(request)
        request_data["allowed_paths"] = list(request.allowed_paths)
        request_data = strict_loads(_json(request_data), max_bytes=MAX_JOURNAL_BYTES)
        request_data["allowed_paths"] = tuple(request_data["allowed_paths"])
        request = WorkerRequest(**request_data)
        result = _result(strict_loads(result_payload_bytes(result), max_bytes=MAX_JOURNAL_BYTES)["payload"])
        inputs, files = self._capture(dispatch, request, result, result_payload_sha256)
        directory = self.root / _sha(dispatch.action.action_id.encode("utf-8"))
        if directory.is_symlink():
            raise CandidateError("Candidate action directory cannot be a symlink")
        directory.mkdir(exist_ok=True)
        _save(directory / "intent.json", inputs)
        self._crash("after_intent")
        proposal_id = inputs["proposal_id"]
        proposal_path, started = directory / "proposal.json", directory / "proposal-started.json"
        if not proposal_path.exists():
            if started.exists():
                _save(started, {"proposal_id": proposal_id})
                commit = self._recover_proposal(files, proposal_id)
            else:
                _save(started, {"proposal_id": proposal_id})
                commit = self.store.propose(inputs["result"]["payload"]["changes"], base_sha=self.baseline_sha,
                                            message="Peer candidate v2 " + proposal_id)
                self._proposal(commit, files, proposal_id)
                self._crash("after_proposal")
            _save(proposal_path, {"proposal_id": proposal_id, "commit_oid": commit})
        proposal, _ = _read(proposal_path)
        if type(proposal) is not dict or set(proposal) != {"proposal_id", "commit_oid"} or proposal["proposal_id"] != proposal_id:
            raise CandidateError("Retained proposal receipt differs from its intent")
        commit = proposal["commit_oid"]
        self._proposal(commit, files, proposal_id)
        bundle_path, bundle_started = directory / "bundle.json", directory / "bundle-started.json"
        if not bundle_path.exists():
            if bundle_started.exists():
                raise CandidateError("Interrupted bundle export retained; refusing automatic retry")
            _save(bundle_started, {"proposal_id": proposal_id, "commit_oid": commit})
            payload, manifest = export_bundle(self.store, commit, self.baseline_sha)
            _save(bundle_path, {"manifest": manifest, "payload_base64": base64.b64encode(payload).decode("ascii")})
        bundle, _ = _read(bundle_path)
        if type(bundle) is not dict or set(bundle) != {"manifest", "payload_base64"} or type(bundle["payload_base64"]) is not str:
            raise CandidateError("Invalid retained bundle receipt")
        try:
            payload = base64.b64decode(bundle["payload_base64"], validate=True)
        except ValueError as error:
            raise CandidateError("Invalid retained bundle bytes") from error
        manifest = _manifest(bundle["manifest"])
        if (manifest["base_sha"] != self.baseline_sha or manifest["offered_sha"] != commit
                or manifest["bundle_sha256"] != _sha(payload) or manifest["bundle_bytes"] != len(payload)
                or not 1 <= len(payload) <= MAX_BUNDLE_BYTES):
            raise CandidateError("Retained bundle differs from the intended proposal")
        _header(payload, manifest)
        self._crash("after_bundle")
        bundle_ref = self._publish("candidate-bundle", payload, "candidate-bundle:" + proposal_id)
        _save(directory / "bundle-publication.json", to_dict(bundle_ref))
        self._crash("after_bundle_publish")
        offer = CandidateOffer(dispatch, commit, source_digest(files), result_payload_sha256,
                               bundle_manifest_digest(manifest), bundle_ref)
        notice = canonical_bytes({"protocol": PROTOCOL, "offer": to_dict(offer), "bundle_manifest": manifest},
                                 max_bytes=MAX_NOTICE_BYTES)
        strictdecode_candidate_notice(notice, expected_contract_sha256=self.execution_contract_sha256,
                                      expected_actor=self.actor)
        _save(directory / "notice.json", strict_loads(notice, max_bytes=MAX_NOTICE_BYTES))
        offer_ref = self._publish("candidate-offer", notice, "candidate-offer:" + proposal_id)
        self._crash("after_publish")
        _save(directory / "offer-publication.json", to_dict(offer_ref))
        if self.store.head() != self.baseline_sha:
            raise CandidateError("Candidate publication unexpectedly moved the accepted head")
        return CandidatePublication(offer, offer_ref)
