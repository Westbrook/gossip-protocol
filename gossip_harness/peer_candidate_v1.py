"""Publish an exact peer-local coding proposal and immutable Git bundle.

Only locally arrived coding inputs/results may cause work. The publisher never
executes candidate code, imports another peer's repository, or accepts a commit.
Generation zero is explicit; source selection and reopening are separate work.
"""
from __future__ import annotations

import base64
import hashlib
import os
from pathlib import Path
import platform
import re
import shutil
import sys
import threading
from typing import Any, Callable
import uuid
import weakref

from .gitstore import GitStore, _run
from .peer_coding_dispatch_v1 import MAX_PAYLOAD_BYTES, MAX_RESULT_BYTES, canonical_payload, source_digest
from .peer_coding_runtime_v1 import CodingPeer, PROTOCOL as CODING_PROTOCOL
from .peer_git_bundle_v1 import MAX_BUNDLE_BYTES, _header, _manifest, _path, export_bundle
from .peer_payload_runtime_v1 import PAYLOAD_KIND
from .peer_store_v1 import canonical_bytes, strict_loads
from .verification_journal import _result

PROTOCOL = "peer-candidate-v1"
MAX_RECORD_BYTES = 2 * MAX_BUNDLE_BYTES + MAX_PAYLOAD_BYTES + MAX_RESULT_BYTES
MAX_STATE_BYTES = 65_536
MAX_VISIBLE_EVENTS = 256
CRASH_POINTS = {"after_intent", "after_proposal", "after_bundle", "after_publish"}
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_GIT_SHA = re.compile(r"[0-9a-f]{40}\Z")
_PHASES = {"prepared", "proposal", "bundle", "published"}


class CandidateError(ValueError):
    pass


def _digest(value: Any) -> str:
    return hashlib.sha256(canonical_payload(value)).hexdigest()


def _same(left: Any, right: Any) -> bool:
    return canonical_payload(left) == canonical_payload(right)


def _binary_identity(selected: str) -> dict:
    path = Path(selected).resolve(strict=True)
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return {"path": str(path), "sha256": digest.hexdigest()}


def _execution_identity() -> dict:
    git = shutil.which("git")
    if git is None:
        raise CandidateError("The selected Git executable is unavailable")
    names = ("peer_candidate_v1.py", "gitstore.py", "peer_git_bundle_v1.py", "peer_coding_runtime_v1.py",
             "peer_coding_dispatch_v1.py", "peer_payload_runtime_v1.py", "peer_payload_store_v1.py",
             "peer_runtime_v1.py", "peer_store_v1.py", "verification_journal.py")
    root = Path(__file__).parent
    return {"sources": {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in names},
            "python": _binary_identity(sys.executable), "git": _binary_identity(git),
            "python_version": platform.python_version(), "platform": platform.platform(),
            "git_selectors_sha256": _digest({key: os.environ.get(key) for key in
                ("PATH", "HOME", "DEVELOPER_DIR", "SDKROOT", "LD_LIBRARY_PATH", "LD_PRELOAD",
                 "DYLD_LIBRARY_PATH", "DYLD_INSERT_LIBRARIES")})}


def _read(path: Path, maximum: int = MAX_RECORD_BYTES) -> tuple[Any, bytes]:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > maximum:
        raise CandidateError("Missing, oversized or unsafe candidate journal file")
    raw = path.read_bytes()
    value = strict_loads(raw, max_bytes=maximum)
    if canonical_bytes(value, max_bytes=maximum) != raw:
        raise CandidateError("Candidate journal is not canonical JSON")
    return value, raw


def _write(path: Path, value: Any, *, replace: bool = False, maximum: int = MAX_RECORD_BYTES) -> None:
    raw = canonical_bytes(value, max_bytes=maximum)
    if path.exists() or path.is_symlink():
        if not replace:
            if _read(path, maximum)[1] != raw:
                raise CandidateError("Immutable candidate artifact already has different bytes")
            return
        if path.is_symlink():
            raise CandidateError("Candidate journal cannot be a symlink")
    temporary = path.parent / (path.name + ".pending-" + uuid.uuid4().hex)
    with temporary.open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class CandidatePublisher:
    """One durable coding-action-to-proposal adapter owned by a CodingPeer."""

    def __init__(self, root: Path, peer: CodingPeer, store: GitStore, *, baseline_sha: str,
                 allowed_paths: tuple[str, ...], generation: int = 0,
                 crash_hook: Callable[[str], None] | None = None):
        with peer.coding_lock:
            owner = getattr(peer, "_candidate_publisher_owner", None)
            if owner is not None and owner() is not None:
                raise CandidateError("CodingPeer already owns a live candidate publisher")
            self._initialize(root, peer, store, baseline_sha=baseline_sha, allowed_paths=allowed_paths,
                             generation=generation, crash_hook=crash_hook)
            setattr(peer, "_candidate_publisher_owner", weakref.ref(self))

    def _initialize(self, root: Path, peer: CodingPeer, store: GitStore, *, baseline_sha: str,
                    allowed_paths: tuple[str, ...], generation: int,
                    crash_hook: Callable[[str], None] | None) -> None:
        if (type(baseline_sha) is not str or _GIT_SHA.fullmatch(baseline_sha) is None
                or type(generation) is not int or generation != 0
                or type(allowed_paths) is not tuple or not allowed_paths
                or any(type(path) is not str for path in allowed_paths)
                or len(set(allowed_paths)) != len(allowed_paths)):
            raise CandidateError("Invalid generation-zero candidate configuration")
        for name in allowed_paths:
            try:
                _path(name.encode("ascii"))
            except (UnicodeError, ValueError) as error:
                raise CandidateError("Candidate scope exceeds supported repository profile") from error
        if store.head() != baseline_sha:
            raise CandidateError("Sender accepted head must equal its immutable baseline")
        root = Path(root).absolute()
        if root.is_symlink() or root.resolve() == peer.root.resolve() or not root.resolve().is_relative_to(peer.root.resolve()):
            raise CandidateError("Candidate journal must be beneath its exclusively owned peer root")
        self.root, self.peer, self.store = root, peer, store
        self.baseline_sha, self.allowed_paths = baseline_sha, allowed_paths
        self.crash_hook = crash_hook
        self.lock = threading.RLock()
        self.config = {"protocol": PROTOCOL, "node_id": peer.node_id,
                       "coding_policy_sha256": peer.policy_sha256, "coding": peer.coding_config,
                       "baseline_sha": baseline_sha, "allowed_paths": list(allowed_paths),
                       "generation": generation, "repository": str(store.path),
                       "execution": _execution_identity()}
        self.config = strict_loads(canonical_payload(self.config), max_bytes=MAX_STATE_BYTES)
        self.config_sha256 = _digest(self.config)
        self.state_path = self.root / "candidate-state.json"
        self.inputs_path = self.root / "inputs.json"
        self.bundle_path = self.root / "bundle-record.json"
        self.record: dict | None = None
        self.root.mkdir(parents=True, exist_ok=True)
        if self.state_path.exists():
            value, _ = _read(self.state_path, MAX_STATE_BYTES)
            if (type(value) is not dict or set(value) != {"data", "sha256"}
                    or value["sha256"] != _digest(value["data"])):
                raise CandidateError("Corrupt candidate journal checksum")
            data = value["data"]
            if (type(data) is not dict or set(data) != {"protocol", "config", "record"}
                    or data["protocol"] != PROTOCOL or not _same(data["config"], self.config)):
                raise CandidateError("Candidate journal identity changed")
            self.record = data["record"]
            self._validate_record(self.record)
            if self.record is not None:
                inputs = self._inputs()
                if self.record["phase"] != "prepared":
                    self._proposal(self.record["offered_sha"], inputs)
                if self.record["phase"] in {"bundle", "published"}:
                    _, manifest, record_sha = self._bundle()
                    if not _same(manifest, self.record["bundle_manifest"]) or record_sha != self.record["bundle_record_sha256"]:
                        raise CandidateError("Retained bundle selection changed")
                if self.record["phase"] == "published":
                    self._published()
        else:
            if any(self.root.iterdir()):
                raise CandidateError("Partial candidate journal has no durable identity")
            self._persist(None)

    def _crash(self, point: str) -> None:
        if self.crash_hook is not None:
            self.crash_hook(point)

    def _persist(self, record: dict | None) -> None:
        self._validate_record(record)
        data = {"protocol": PROTOCOL, "config": self.config, "record": record}
        _write(self.state_path, {"data": data, "sha256": _digest(data)},
               replace=True, maximum=MAX_STATE_BYTES)
        self.record = record

    def _validate_record(self, record: Any) -> None:
        if record is None:
            return
        fields = {"intent", "phase", "offered_sha", "bundle_manifest", "bundle_record_sha256",
                  "bundle_notice_event_id", "offer_event_id"}
        if (type(record) is not dict or set(record) != fields or type(record["phase"]) is not str or record["phase"] not in _PHASES
                or type(record["intent"]) is not dict):
            raise CandidateError("Invalid candidate journal fields")
        self._validate_intent(record["intent"])
        if record["phase"] == "prepared":
            if any(record[key] is not None for key in fields - {"intent", "phase"}):
                raise CandidateError("Prepared candidate contains later-phase evidence")
            return
        if type(record["offered_sha"]) is not str or _GIT_SHA.fullmatch(record["offered_sha"]) is None:
            raise CandidateError("Proposal commit is missing")
        if record["phase"] == "proposal":
            if any(record[key] is not None for key in ("bundle_manifest", "bundle_record_sha256", "bundle_notice_event_id", "offer_event_id")):
                raise CandidateError("Proposal contains uncommitted bundle state")
            return
        manifest = _manifest(record["bundle_manifest"])
        if (manifest["base_sha"] != self.baseline_sha or manifest["offered_sha"] != record["offered_sha"]
                or type(record["bundle_record_sha256"]) is not str
                or _SHA.fullmatch(record["bundle_record_sha256"]) is None):
            raise CandidateError("Bundle does not bind the exact proposal")
        for key in ("bundle_notice_event_id", "offer_event_id"):
            if record["phase"] == "published":
                if type(record[key]) is not str or _SHA.fullmatch(record[key]) is None:
                    raise CandidateError("Published candidate lacks an event identity")
            elif record[key] is not None:
                raise CandidateError("Unpublished candidate contains an event identity")

    def _validate_intent(self, intent: Any) -> None:
        hash_fields = {"config_sha256", "action_id", "request_sha256", "result_sha256", "dispatch_config_sha256",
                       "coding_result_event_id", "authority_receipt_sha256", "seed_sha256", "request_notice_event_id",
                       "result_notice_event_id", "peer_source_sha256", "peer_candidate_source_sha256", "proposal_id"}
        fields = hash_fields | {"protocol", "principal", "run_id", "task_id", "generation", "request_id",
                                "profile_id", "epoch", "visible_event_ids"}
        if (type(intent) is not dict or set(intent) != fields or intent["protocol"] != PROTOCOL
                or intent["config_sha256"] != self.config_sha256 or intent["principal"] != self.peer.node_id
                or type(intent["generation"]) is not int or intent["generation"] != 0
                or type(intent["epoch"]) is not int or not 1 <= intent["epoch"] < 2**63
                or any(intent[key] != self.peer.coding_config[key] for key in
                       ("run_id", "task_id", "profile_id", "seed_sha256", "dispatch_config_sha256"))
                or any(type(intent[key]) is not str or _SHA.fullmatch(intent[key]) is None for key in hash_fields)
                or intent["request_id"] != intent["action_id"] + ":coding_dispatch"
                or intent["proposal_id"] != _digest({k: v for k, v in intent.items() if k != "proposal_id"})):
            raise CandidateError("Candidate intent binding changed")
        ids = intent["visible_event_ids"]
        if (type(ids) is not list or not 1 <= len(ids) <= MAX_VISIBLE_EVENTS
                or any(type(value) is not str or _SHA.fullmatch(value) is None for value in ids)
                or ids != sorted(set(ids))
                or any(intent[key] not in ids for key in
                       ("coding_result_event_id", "request_notice_event_id", "result_notice_event_id"))):
            raise CandidateError("Candidate intent visibility changed")

    def state(self) -> dict | None:
        with self.lock:
            return strict_loads(canonical_bytes(self.record, max_bytes=MAX_STATE_BYTES), max_bytes=MAX_STATE_BYTES)

    def _notice(self, events: list[dict], producer: str, sha: str, event_id: str | None = None) -> dict | None:
        for event in events:
            if event_id is not None and event["event_id"] != event_id:
                continue
            if event["producer"] != producer or event["kind"] != PAYLOAD_KIND:
                continue
            content = self.peer.store.artifact(event["artifact_sha256"])
            if content.get("sha256") == sha and content.get("media_type") == "application/json":
                return event
        return None

    def _capture(self) -> dict | None:
        with self.peer.coding_lock:
            coding = strict_loads(canonical_payload(self.peer.record), max_bytes=MAX_STATE_BYTES)
            if coding is None or coding["phase"] != "published":
                return None
            self.peer._validate_record(coding)
        intent, receipt = coding["intent"], coding["response"]
        events = self.peer.store.state()["events"]
        result_event = next((e for e in events if e["event_id"] == coding["result_event_id"]), None)
        if result_event is None:
            return None
        if result_event["producer"] != self.peer.node_id or result_event["kind"] != "coding_result":
            raise CandidateError("Coding result event has a different producer or purpose")
        content = self.peer.store.artifact(result_event["artifact_sha256"])
        expected_fields = {"protocol", "source_kind", "run_id", "task_id", "action_id", "request_sha256",
                           "result_sha256", "result_notice_event_id", "authority"}
        if (set(content) != expected_fields or content["protocol"] != CODING_PROTOCOL
                or content["source_kind"] != "provided_worker_request"
                or any(content[key] != intent[key] for key in ("run_id", "task_id", "action_id", "request_sha256"))
                or content["result_sha256"] != receipt["result_sha256"]
                or not _same(content["authority"], receipt)):
            raise CandidateError("Coding result event differs from the retained request receipt")
        request_notice = self._notice(events, self.peer.node_id, intent["request_sha256"])
        result_notice = self._notice(events, self.peer.coding_config["service_producer"], receipt["result_sha256"],
                                     content["result_notice_event_id"])
        seed_notice = self._notice(events, self.peer.coding_config["seed_producer"], intent["seed_sha256"], intent["seed_event_id"])
        if request_notice is None or result_notice is None or seed_notice is None:
            return None
        if result_notice["event_id"] != content["result_notice_event_id"] or seed_notice["event_id"] != intent["seed_event_id"]:
            raise CandidateError("Coding prerequisite notice binding changed")
        identities = (intent["request_sha256"], receipt["result_sha256"], intent["seed_sha256"])
        if not all(self.peer.payloads.has_complete(sha) for sha in identities):
            return None
        request_raw, result_raw, seed_raw = (self.peer.payloads.read(sha) for sha in identities)
        request = strict_loads(request_raw, max_bytes=MAX_PAYLOAD_BYTES)
        result = strict_loads(result_raw, max_bytes=MAX_RESULT_BYTES)
        seed = strict_loads(seed_raw, max_bytes=MAX_PAYLOAD_BYTES)
        if (any(hashlib.sha256(raw).hexdigest() != sha for raw, sha in zip((request_raw, result_raw, seed_raw), identities))
                or canonical_payload(request) != request_raw or canonical_payload(result) != result_raw
                or canonical_payload(seed) != seed_raw):
            raise CandidateError("Arrived coding payload bytes changed")
        if (type(request) is not dict or set(request) != {"worker_request", "context"}
                or not _same(request["worker_request"], seed) or _digest(seed) != intent["worker_request_sha256"]
                or request["context"] != {"source_sha256": intent["source_sha256"], "event_ids": intent["visible_event_ids"]}
                or type(result) is not dict or set(result) != {"kind", "payload"} or result["kind"] != "result"):
            raise CandidateError("Coding request/source/result provenance differs")
        outcome = _result(result["payload"])
        if type(outcome.usage_units) is not int or outcome.usage_units != receipt["usage_units"]:
            raise CandidateError("Coding usage differs from the authority receipt")
        if (seed.get("base_sha") != self.baseline_sha or seed.get("task_id") != self.peer.coding_config["task_id"]
                or seed.get("allowed_paths") != list(self.allowed_paths)
                or source_digest(seed["files"]) != intent["source_sha256"]):
            raise CandidateError("Coding task, base or scope differs from the candidate configuration")
        candidate_files = self._apply(seed["files"], outcome.changes)
        self._base(seed["files"])
        own_intent = {"protocol": PROTOCOL, "config_sha256": self.config_sha256,
                      "principal": self.peer.node_id, "run_id": intent["run_id"], "task_id": intent["task_id"],
                      "generation": 0, "action_id": intent["action_id"], "request_id": receipt["request_id"],
                      "profile_id": intent["profile_id"], "epoch": receipt["epoch"],
                      "request_sha256": intent["request_sha256"], "result_sha256": receipt["result_sha256"],
                      "dispatch_config_sha256": receipt["dispatch_config_sha256"],
                      "coding_result_event_id": result_event["event_id"],
                      "authority_receipt_sha256": _digest(receipt), "seed_sha256": intent["seed_sha256"],
                      "request_notice_event_id": request_notice["event_id"], "result_notice_event_id": result_notice["event_id"],
                      "visible_event_ids": sorted(e["event_id"] for e in events),
                      "peer_source_sha256": source_digest(seed["files"]),
                      "peer_candidate_source_sha256": source_digest(candidate_files)}
        own_intent["proposal_id"] = _digest(own_intent)
        self._validate_intent(own_intent)
        return {"intent": own_intent, "coding": coding, "request": request, "result": result, "seed": seed,
                "candidate_files": candidate_files}

    def _apply(self, files: dict, changes: dict) -> dict:
        if type(files) is not dict or any(type(key) is not str or type(value) is not str for key, value in files.items()):
            raise CandidateError("Candidate baseline must contain text files")
        result = dict(files)
        if any(path not in self.allowed_paths for path in changes):
            raise CandidateError("Coding changes exceed task-owned paths")
        for path, value in changes.items():
            if value is None:
                result.pop(path, None)
            else:
                result[path] = value
        for path, content in result.items():
            try:
                _path(path.encode("ascii"))
                content.encode("utf-8", errors="strict")
            except (UnicodeError, ValueError) as error:
                raise CandidateError("Proposed files exceed the supported repository profile") from error
        return result

    def _base(self, files: dict) -> None:
        if self.store.head() != self.baseline_sha:
            raise CandidateError("Sender accepted head moved from its immutable baseline")
        self._tree(self.baseline_sha, files)

    def _tree(self, sha: str, files: dict) -> None:
        if not _same(self.store.read_files(sha), files):
            raise CandidateError("WorkerRequest files are not the sender's exact Git base tree")
        listing = _run(self.store.path, "ls-tree", "-r", "-t", "-z", "--full-tree", sha).stdout
        for entry in listing.split(b"\x00"):
            if not entry:
                continue
            fields, separator, path = entry.partition(b"\t")
            if not separator or fields.split(b" ")[:2] not in ([b"100644", b"blob"], [b"040000", b"tree"]):
                raise CandidateError("Source tree has an unsupported file mode")
            _path(path)

    def _proposal(self, offered: str, inputs: dict) -> None:
        """Rebind a recovered Git object to the deterministic, exact local action."""
        self._tree(offered, inputs["candidate_files"])
        reference = "refs/harness/proposals/" + offered
        if self.store._git("rev-parse", "--verify", reference) != offered:
            raise CandidateError("Proposal ref does not pin its exact commit")
        raw = _run(self.store.path, "cat-file", "commit", offered).stdout
        tree = raw.split(b"\n", 1)[0]
        if re.fullmatch(rb"tree [0-9a-f]{40}", tree) is None:
            raise CandidateError("Proposal tree identity changed")
        # GitStore's frozen deterministic identity/date is part of this adapter's
        # source contract. Exact headers exclude another parent or signed/extra
        # headers even when the visible files happen to be identical.
        actor = b"Gossip Harness <harness@example.invalid> 946684800 +0000"
        expected = (tree + b"\nparent " + self.baseline_sha.encode("ascii") + b"\nauthor " + actor
                    + b"\ncommitter " + actor + b"\n\nPeer candidate "
                    + inputs["intent"]["proposal_id"].encode("ascii") + b"\n")
        if raw != expected:
            raise CandidateError("Proposal commit differs from its exact action and direct baseline")

    def _inputs(self) -> dict:
        inputs, _ = _read(self.inputs_path)
        if (type(inputs) is not dict or set(inputs) != {"intent", "coding", "request", "result", "seed", "candidate_files"}
                or self.record is not None and not _same(inputs["intent"], self.record["intent"])):
            raise CandidateError("Immutable proposal input binding differs")
        intent = inputs["intent"]
        self._validate_intent(intent)
        coding, request, result, seed = (inputs[key] for key in ("coding", "request", "result", "seed"))
        self.peer._validate_record(coding)
        if (coding["phase"] != "published" or not _same(coding, self.peer.record)
                or type(request) is not dict or set(request) != {"worker_request", "context"}
                or type(result) is not dict or set(result) != {"kind", "payload"} or result["kind"] != "result"
                or type(seed) is not dict
                or _digest(request) != intent["request_sha256"]
                or _digest(inputs["result"]) != intent["result_sha256"]
                or _digest(inputs["seed"]) != intent["seed_sha256"]
                or not _same(request["worker_request"], seed)
                or _digest(coding["response"]) != intent["authority_receipt_sha256"]
                or any(intent[key] != coding["intent"][key] for key in
                       ("action_id", "run_id", "task_id", "profile_id", "request_sha256", "seed_sha256"))
                or any(intent[key] != coding["response"][key] for key in
                       ("request_id", "result_sha256", "dispatch_config_sha256", "epoch"))
                or intent["coding_result_event_id"] != coding["result_event_id"]
                or seed.get("base_sha") != self.baseline_sha or seed.get("allowed_paths") != list(self.allowed_paths)
                or seed.get("task_id") != intent["task_id"]
                or _digest(seed) != coding["intent"]["worker_request_sha256"]
                or request["context"] != {"source_sha256": coding["intent"]["source_sha256"],
                                           "event_ids": coding["intent"]["visible_event_ids"]}
                or source_digest(seed["files"]) != intent["peer_source_sha256"]
                or intent["peer_source_sha256"] != coding["intent"]["source_sha256"]):
            raise CandidateError("Immutable coding input identities changed")
        events = {event["event_id"]: event for event in self.peer.store.state()["events"]}
        if (any(event_id not in events for event_id in intent["visible_event_ids"])
                or coding["intent"]["seed_event_id"] not in intent["visible_event_ids"]):
            raise CandidateError("Retained candidate prerequisites are no longer local")
        for event_id, producer, sha in (
                (intent["request_notice_event_id"], self.peer.node_id, intent["request_sha256"]),
                (intent["result_notice_event_id"], self.peer.coding_config["service_producer"], intent["result_sha256"]),
                (coding["intent"]["seed_event_id"], self.peer.coding_config["seed_producer"], intent["seed_sha256"])):
            event = events[event_id]
            notice = self.peer.store.artifact(event["artifact_sha256"])
            if (event["kind"] != PAYLOAD_KIND or event["producer"] != producer
                    or notice.get("sha256") != sha or notice.get("media_type") != "application/json"
                    or not self.peer.payloads.has_complete(sha)
                    or hashlib.sha256(self.peer.payloads.read(sha)).hexdigest() != sha):
                raise CandidateError("Retained candidate notice or payload ownership changed")
        event = events[intent["coding_result_event_id"]]
        expected = {"protocol": CODING_PROTOCOL, "source_kind": "provided_worker_request", "run_id": intent["run_id"],
                    "task_id": intent["task_id"], "action_id": intent["action_id"], "request_sha256": intent["request_sha256"],
                    "result_sha256": intent["result_sha256"], "result_notice_event_id": intent["result_notice_event_id"],
                    "authority": coding["response"]}
        if (event["producer"] != self.peer.node_id or event["kind"] != "coding_result"
                or not _same(self.peer.store.artifact(event["artifact_sha256"]), expected)):
            raise CandidateError("Retained coding result event binding changed")
        outcome = _result(result["payload"])
        candidate_files = self._apply(seed["files"], outcome.changes)
        if (not _same(candidate_files, inputs["candidate_files"])
                or source_digest(candidate_files) != intent["peer_candidate_source_sha256"]
                or type(outcome.usage_units) is not int or outcome.usage_units != coding["response"]["usage_units"]):
            raise CandidateError("Proposed source differs from the exact coding result")
        return inputs

    def _bundle(self) -> tuple[bytes, dict, str]:
        value, raw = _read(self.bundle_path)
        if type(value) is not dict or set(value) != {"manifest", "payload_base64"} or type(value["payload_base64"]) is not str:
            raise CandidateError("Invalid immutable bundle record")
        try:
            payload = base64.b64decode(value["payload_base64"], validate=True)
        except ValueError as error:
            raise CandidateError("Invalid encoded bundle") from error
        manifest = _manifest(value["manifest"])
        if (not 1 <= len(payload) <= MAX_BUNDLE_BYTES or manifest["bundle_sha256"] != hashlib.sha256(payload).hexdigest()
                or manifest["bundle_bytes"] != len(payload) or manifest["base_sha"] != self.baseline_sha
                or self.record is None or manifest["offered_sha"] != self.record["offered_sha"]):
            raise CandidateError("Retained bundle bytes differ from the proposal")
        _header(payload, manifest)
        return payload, manifest, hashlib.sha256(raw).hexdigest()

    def _offer(self) -> dict:
        assert self.record is not None
        intent, manifest = self.record["intent"], self.record["bundle_manifest"]
        fields = ("run_id", "task_id", "principal", "epoch", "action_id", "request_id", "profile_id",
                  "request_sha256", "result_sha256", "dispatch_config_sha256", "generation")
        return {"protocol": PROTOCOL, **{key: intent[key] for key in fields},
                "bundle_sha256": manifest["bundle_sha256"], "bundle_manifest": manifest}

    def _published(self) -> None:
        assert self.record is not None
        events = {event["event_id"]: event for event in self.peer.store.state()["events"]}
        for key, kind in (("bundle_notice_event_id", PAYLOAD_KIND), ("offer_event_id", "candidate_offer")):
            event = events.get(self.record[key])
            if event is None or event["producer"] != self.peer.node_id or event["kind"] != kind:
                raise CandidateError("Published candidate event is missing or changed")
            value = self.peer.store.artifact(event["artifact_sha256"])
            if kind == "candidate_offer":
                if not _same(value, self._offer()):
                    raise CandidateError("Published candidate offer changed")
            elif (value.get("sha256") != self.record["bundle_manifest"]["bundle_sha256"]
                  or value.get("media_type") != "application/x-git-bundle"
                  or not self.peer.payloads.has_complete(value["sha256"])):
                raise CandidateError("Published candidate payload changed")

    def tick(self) -> dict | None:
        with self.lock:
            if self.record is not None and self.record["phase"] == "published":
                return self.state()
            if self.record is None:
                if self.inputs_path.exists():
                    inputs = self._inputs()
                else:
                    captured = self._capture()
                    if captured is None:
                        return None
                    inputs = captured
                    _write(self.inputs_path, inputs)
                self._persist({"intent": inputs["intent"], "phase": "prepared", "offered_sha": None,
                               "bundle_manifest": None, "bundle_record_sha256": None,
                               "bundle_notice_event_id": None, "offer_event_id": None})
                self._crash("after_intent")
            inputs = self._inputs()
            assert self.record is not None
            self._base(inputs["seed"]["files"])
            if self.record["phase"] == "prepared":
                offered = self.store.propose(inputs["result"]["payload"]["changes"], base_sha=self.baseline_sha,
                                             message="Peer candidate " + self.record["intent"]["proposal_id"])
                self._proposal(offered, inputs)
                if self.store.head() != self.baseline_sha:
                    raise CandidateError("Materialized Git proposal differs from the coding result")
                self._persist({**self.record, "phase": "proposal", "offered_sha": offered})
                self._crash("after_proposal")
            else:
                self._proposal(self.record["offered_sha"], inputs)
            if self.record["phase"] == "proposal":
                if not self.bundle_path.exists():
                    payload, manifest = export_bundle(self.store, self.record["offered_sha"], self.baseline_sha)
                    _write(self.bundle_path, {"manifest": manifest, "payload_base64": base64.b64encode(payload).decode("ascii")})
                _, manifest, record_sha = self._bundle()
                self._persist({**self.record, "phase": "bundle", "bundle_manifest": manifest, "bundle_record_sha256": record_sha})
                self._crash("after_bundle")
            if self.record["phase"] == "bundle":
                payload, manifest, record_sha = self._bundle()
                if not _same(manifest, self.record["bundle_manifest"]) or record_sha != self.record["bundle_record_sha256"]:
                    raise CandidateError("Immutable bundle selection changed")
                proposal_id = self.record["intent"]["proposal_id"]
                notice = self.peer.publish_payload(payload, "application/x-git-bundle", _digest([PROTOCOL, proposal_id, "bundle"]))
                offer = self.peer.store.publish(_digest([PROTOCOL, proposal_id, "offer"]), "candidate_offer", self._offer())
                self._crash("after_publish")
                self._persist({**self.record, "phase": "published", "bundle_notice_event_id": notice["event_id"],
                               "offer_event_id": offer["event_id"]})
            return self.state()
