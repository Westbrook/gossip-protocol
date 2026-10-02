"""Receiver-owned public validation and fenced Git promotion of arrived bundles.

This bridge never invokes a provider or imports candidate Python on the host.
Public promotion is not independent final-project or cohort acceptance.
"""
from __future__ import annotations

from dataclasses import asdict
import fcntl
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import sys
import threading
from typing import Any
import uuid

from . import blackbox_validator as validator_module
from . import gitstore as git_module
from . import ledger as ledger_module
from . import peer_git_bundle_v1 as bundle_module
from . import promotion as promotion_module
from . import sandbox as sandbox_module
from .blackbox_validator import BlackboxValidator
from .gitstore import Candidate, GitStore, _run
from .ledger import ClaimRejected, Lease
from .peer_coding_dispatch_v1 import CodingDispatch, MAX_PAYLOAD_BYTES, canonical_payload
from .peer_payload_runtime_v1 import PAYLOAD_KIND, PayloadPeer
from .peer_store_v1 import canonical_bytes, strict_loads
from .promotion import PromotionCoordinator, SimulatedCrash

PROTOCOL = "peer-promotion-v1"
OFFER_PROTOCOL = "peer-candidate-v1"
OFFER_KIND = "candidate_offer"
PURPOSE = "public_candidate_promotion"
MAX_RECORD_BYTES = 8 * 1024 * 1024
MAX_ATTEMPTS = 128
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_FIELDS = {"protocol", "generation", "run_id", "task_id", "principal", "epoch", "action_id",
           "request_id", "profile_id", "request_sha256", "result_sha256",
           "dispatch_config_sha256", "bundle_sha256", "bundle_manifest"}
_TERMINAL = {"accepted", "rejected", "interrupted", "infrastructure_failed", "stale"}
CRASH_POINTS = {None, "after_admission", "after_validation", "after_intent", "after_git", "after_promotion"}


class PromotionError(ValueError):
    """An arrived offer or retained trusted record cannot safely be accepted."""


def _raw(value: Any) -> bytes:
    return canonical_bytes(value, max_bytes=MAX_RECORD_BYTES)


def _digest(value: Any) -> str:
    return hashlib.sha256(_raw(value)).hexdigest()


def _evaluator_digest(value: Any) -> str:
    return hashlib.sha256(validator_module._canonical(value).encode("utf-8")).hexdigest()


def _execution_identity() -> dict:
    executables: dict[str, dict | None] = {}
    for name, selected in (("python", sys.executable), ("git", shutil.which("git")),
                           ("docker", shutil.which("docker"))):
        if selected is None:
            executables[name] = None
            continue
        path = Path(selected).resolve(strict=True)
        if not path.is_file():
            raise PromotionError("Selected runtime executable is not a regular file")
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1_048_576), b""):
                digest.update(chunk)
        executables[name] = {"path": str(path), "sha256": digest.hexdigest()}
    # Retain a digest only, never selected environment values. Toolchain shims
    # and dynamic libraries remain the host trust boundary, not a sandbox proof.
    native_selectors = ("PATH", "HOME", "DEVELOPER_DIR", "SDKROOT", "LD_LIBRARY_PATH",
                        "LD_PRELOAD", "DYLD_LIBRARY_PATH", "DYLD_INSERT_LIBRARIES")
    selectors = {"docker": sandbox_module.DockerValidator._environment(),
                 "native": {name: os.environ[name] for name in native_selectors if name in os.environ}}
    return {"executables": executables, "environment_sha256": _digest(selectors)}


def _save(path: Path, value: dict) -> None:
    envelope = {"sha256": _digest(value), "record": value}
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(_raw(envelope))
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _read(path: Path) -> dict:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > MAX_RECORD_BYTES:
        raise PromotionError("Invalid retained promotion record")
    envelope = strict_loads(path.read_bytes(), max_bytes=MAX_RECORD_BYTES)
    if (type(envelope) is not dict or set(envelope) != {"sha256", "record"}
            or type(envelope["record"]) is not dict or _digest(envelope["record"]) != envelope["sha256"]):
        raise PromotionError("Promotion record digest mismatch")
    return envelope["record"]


def validate_offer(value: Any, producer: str) -> dict:
    value = strict_loads(canonical_bytes(value, max_bytes=16_000), max_bytes=16_000)
    if (type(value) is not dict or set(value) != _FIELDS or value["protocol"] != OFFER_PROTOCOL
            or type(value["generation"]) is not int or value["generation"] != 0
            or value["principal"] != producer or type(value["epoch"]) is not int or value["epoch"] < 1):
        raise PromotionError("Invalid producer-bound candidate offer")
    for key in ("run_id", "task_id", "principal", "action_id", "request_id", "profile_id"):
        if type(value[key]) is not str or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,95}", value[key]) is None:
            raise PromotionError("Invalid offer identity")
    for key in ("request_sha256", "result_sha256", "dispatch_config_sha256", "bundle_sha256"):
        if type(value[key]) is not str or _SHA.fullmatch(value[key]) is None:
            raise PromotionError("Invalid offer digest")
    manifest = bundle_module._manifest(value["bundle_manifest"])
    if manifest["bundle_sha256"] != value["bundle_sha256"]:
        raise PromotionError("Offer and bundle identities differ")
    return value


class ReceivedPromotion:
    """One receiver-owned, durable bridge sharing its coding authority ledger."""

    def __init__(self, root: Path, *, peer: PayloadPeer, coding: CodingDispatch,
                 target: GitStore, cases: list[dict], image: str,
                 timeout_seconds: float = 60, case_timeout_seconds: float = 2,
                 crash_at: str | None = None):
        if type(peer) is not PayloadPeer and not isinstance(peer, PayloadPeer):
            raise PromotionError("A real local payload peer is required")
        if not isinstance(coding, CodingDispatch) or not isinstance(target, GitStore):
            raise PromotionError("A coding authority and private Git target are required")
        if crash_at not in CRASH_POINTS:
            raise PromotionError("Unknown crash boundary")
        self.crash_at = crash_at
        self.peer, self.coding, self.target = peer, coding, target
        self.ledger = coding.ledger
        self.coordinator = PromotionCoordinator(self.ledger)
        self.validator = BlackboxValidator(image, timeout_seconds=timeout_seconds,
                                           case_timeout_seconds=case_timeout_seconds)
        _, self.cases, _ = BlackboxValidator._inputs({"solution.py": ""}, cases)
        self.root = Path(root).absolute()
        if self.root.is_symlink() or self.root.resolve() != self.root:
            raise PromotionError("Promotion root cannot traverse symlinks")
        self.root.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.state_lock = threading.RLock()
        self.closed = False
        self.failed_closed = False
        owner_path = self.root / "owner.lock"
        if owner_path.is_symlink():
            raise PromotionError("Invalid promotion owner lock")
        self.owner = owner_path.open("a")
        try:
            fcntl.flock(self.owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.config = {"protocol": PROTOCOL, "purpose": PURPOSE, "generation": 0,
                           "run_id": coding.authority.config["run_id"], "receiver": peer.node_id, "root": str(self.root),
                           "dispatch_config_sha256": coding.config_sha256,
                           "target": str(target.path), "image": image,
                           "suite_sha256": _evaluator_digest(self.cases), "timeout_seconds": timeout_seconds,
                           "case_timeout_seconds": case_timeout_seconds,
                           "adapter_sha256": self.validator._sandbox.checks_sha256,
                           "python": platform.python_version(), "platform": platform.platform(),
                           "execution_identity": _execution_identity(),
                           "seed": None, "protocol_sources": self._sources(),
                           "sandbox_policy": {"network": "none", "memory_bytes": 268435456,
                                              "cpus": 1, "pids": 64, "read_only": True},
                           "independent_final_project_acceptance": False}
            self.config_sha256 = _digest(self.config)
            config_path = self.root / "config.json"
            if config_path.exists():
                if _read(config_path) != self.config:
                    raise PromotionError("Promotion execution contract changed across restart")
            else:
                if any(self.root.glob("attempt-*.json")):
                    raise PromotionError("Unbound promotion attempts")
                _save(config_path, self.config)
            with self.ledger.atomic() as db:
                db.execute("CREATE TABLE IF NOT EXISTS peer_promotion_meta_v1 (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
                bound = db.execute("SELECT value FROM peer_promotion_meta_v1 WHERE key='config_sha256'").fetchone()
                if bound is None:
                    db.execute("INSERT INTO peer_promotion_meta_v1 VALUES ('config_sha256',?)", (self.config_sha256,))
                elif bound[0] != self.config_sha256:
                    raise PromotionError("Authority already bound a different promotion root or contract")
            self.rejections: dict[str, dict] = {}
            rejection_path = self.root / "rejections.json"
            if rejection_path.exists():
                rejected = _read(rejection_path)
                if (set(rejected) != {"config_sha256", "rejections"}
                        or rejected["config_sha256"] != self.config_sha256
                        or type(rejected["rejections"]) is not dict or len(rejected["rejections"]) > MAX_ATTEMPTS):
                    raise PromotionError("Invalid retained offer rejections")
                self.rejections = rejected["rejections"]
                for event_id, failure in self.rejections.items():
                    if (_SHA.fullmatch(event_id) is None or type(failure) is not dict
                            or failure.get("status") != "rejected" or failure.get("offer_event_id") != event_id):
                        raise PromotionError("Invalid retained offer rejection binding")
            self.records: dict[str, dict] = {}
            paths = sorted(self.root.glob("attempt-*.json"))
            if len(paths) > MAX_ATTEMPTS:
                raise PromotionError("Promotion attempt capacity exceeded")
            for path in paths:
                record = _read(path)
                key = record.get("attempt_id")
                if (type(key) is not str or _SHA.fullmatch(key) is None
                        or path.name != "attempt-" + key + ".json"
                        or record.get("config_sha256") != self.config_sha256
                        or record.get("status") not in {*_TERMINAL, "admitted", "preparing", "validated", "promoting"}):
                    raise PromotionError("Invalid promotion attempt binding")
                offer = validate_offer(record["offer"], record["offer"]["principal"])
                if key != self._key(offer):
                    raise PromotionError("Attempt identity mismatch")
                if record["status"] in {"validated", "promoting", "accepted"}:
                    self._validate_saved(record)
                if record["status"] == "accepted":
                    self._validate_accepted(record)
                self.records[key] = record
        except BaseException:
            self.owner.close()
            raise

    @staticmethod
    def _sources() -> dict[str, str]:
        paths = [Path(__file__)]
        for module in (validator_module, sandbox_module, git_module, ledger_module, bundle_module, promotion_module):
            if module.__file__ is None:
                raise PromotionError("Execution source is unavailable")
            paths.append(Path(module.__file__))
        return {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}

    @staticmethod
    def _key(offer: dict) -> str:
        return _digest([PROTOCOL, offer["run_id"], offer["principal"], offer["action_id"]])

    def close(self) -> None:
        with self.lock:
            if not self.closed:
                self.closed = True
                self.owner.close()

    def state(self) -> dict:
        with self.state_lock:
            return strict_loads(_raw({"protocol": PROTOCOL, "config_sha256": self.config_sha256,
                                    "attempts": list(self.records.values()), "rejected_offers": list(self.rejections.values())}), max_bytes=MAX_RECORD_BYTES)

    def _put(self, record: dict) -> dict:
        try:
            _save(self.root / ("attempt-" + record["attempt_id"] + ".json"), record)
        except BaseException:
            self.failed_closed = True
            raise
        with self.state_lock:
            self.records[record["attempt_id"]] = strict_loads(_raw(record), max_bytes=MAX_RECORD_BYTES)
        return record

    def _reject_event(self, event_id: str, detail: str) -> dict:
        if len(self.rejections) >= MAX_ATTEMPTS:
            raise PromotionError("Rejected offer capacity exceeded")
        record = {"status": "rejected", "offer_event_id": event_id, "detail": detail,
                  "independent_final_project_acceptance": False}
        updated = {**self.rejections, event_id: record}
        try:
            _save(self.root / "rejections.json", {"config_sha256": self.config_sha256, "rejections": updated})
        except BaseException:
            self.failed_closed = True
            raise
        with self.state_lock:
            self.rejections = updated
        return record

    def _owned_bytes(self, producer: str, sha: str, media_type: str) -> bytes | None:
        self.peer.arrived_payloads()
        for event in self.peer.store.state()["events"]:
            if event["producer"] == producer and event["kind"] == PAYLOAD_KIND:
                descriptor = self.peer.store.artifact(event["artifact_sha256"])
                if descriptor["sha256"] == sha and descriptor["media_type"] == media_type:
                    if self.peer.payloads.has_complete(sha):
                        return self.peer.payloads.read(sha)
        return None

    def _authoritative(self, offer: dict) -> tuple[dict, dict, dict, dict]:
        if (offer["run_id"] != self.config["run_id"]
                or offer["dispatch_config_sha256"] != self.coding.config_sha256):
            raise PromotionError("Offer belongs to another execution contract")
        with self.coding.lock:
            action, receipt = self.coding._action(offer["principal"], offer["request_id"])
            if action["state"] != "completed" or receipt["status"] != "completed":
                raise PromotionError("Coding dispatch has no authoritative completed result")
            self.coding._validate_terminal(action, receipt)
            for key in ("task_id", "principal", "epoch", "action_id", "request_id", "profile_id"):
                if action[key] != offer[key]:
                    raise PromotionError("Offer differs from authoritative action")
            if action["payload_sha"] != offer["request_sha256"] or receipt["result_sha256"] != offer["result_sha256"]:
                raise PromotionError("Offer request or result binding differs")
            request = strict_loads(action["worker_request"], max_bytes=MAX_PAYLOAD_BYTES)
            result_path = self.coding.journal.paths(action["call_id"])["result"]
            journal = strict_loads(result_path.read_bytes(), max_bytes=MAX_RECORD_BYTES)
            result = journal["payload"]
            expected_result = canonical_payload({"kind": "result", "payload": result})
            arrived_result = self._owned_bytes(self.peer.node_id, offer["result_sha256"], "application/json")
            arrived_request = self._owned_bytes(offer["principal"], offer["request_sha256"], "application/json")
            if arrived_result != expected_result or arrived_request is None:
                raise PromotionError("Authoritative request and service-owned result must be locally available")
            decoded_request = strict_loads(arrived_request, max_bytes=MAX_PAYLOAD_BYTES)
            if canonical_payload(decoded_request["worker_request"]) != action["worker_request"]:
                raise PromotionError("Arrived request differs from authoritative source")
            return action, receipt, request, result

    def _validate_saved(self, record: dict) -> None:
        evidence = record["validation"]
        candidate = Candidate(**{**record["candidate"], "changed_paths": tuple(record["candidate"]["changed_paths"])})
        raw = evidence["receipt"]
        offer = record["offer"]
        lease = record.get("lease")
        if (candidate.candidate_sha is None or candidate.old_head != record["old_head"]
                or candidate.offered_sha != offer["bundle_manifest"]["offered_sha"]
                or type(lease) is not dict or set(lease) != {"task_id", "worker_id", "epoch", "expires_at"}
                or type(lease["epoch"]) is not int or lease["epoch"] < 1
                or (lease["task_id"], lease["worker_id"], lease["epoch"]) !=
                   (offer["task_id"], offer["principal"], offer["epoch"])
                or self.target._git("rev-parse", "--verify", self.target._candidate_ref(
                    candidate.old_head, candidate.candidate_sha)) != candidate.candidate_sha
                or _evaluator_digest(self.target.read_files(candidate.candidate_sha)) != evidence["source_sha256"]):
            raise PromotionError("Retained candidate, lease, source or pin binding differs")
        if (evidence["purpose"] != PURPOSE or evidence["config_sha256"] != self.config_sha256
                or evidence["receipt_sha256"] != _digest(raw)
                or evidence["candidate_sha"] != candidate.candidate_sha or candidate.status != "prepared"
                or raw["source_sha256"] != evidence["source_sha256"]
                or raw["suite_sha256"] != self.config["suite_sha256"]
                or raw["image_id"] != self.config["image"]
                or raw["adapter_sha256"] != self.config["adapter_sha256"]
                or raw["timeout_seconds"] != self.config["timeout_seconds"]
                or raw["case_timeout_seconds"] != self.config["case_timeout_seconds"]
                or raw.get("passed") is not True or raw.get("status") != "passed"
                or raw.get("cleanup_verified") is not True):
            raise PromotionError("Retained validation is not an exact successful public receipt")

    def _intent(self, record: dict) -> dict | None:
        intent_id = record.get("intent_id")
        if type(intent_id) is not str or _SHA.fullmatch(intent_id) is None:
            raise PromotionError("Missing exact promotion intent identity")
        with self.ledger.atomic() as db:
            row = db.execute("SELECT * FROM intents WHERE id=?", (intent_id,)).fetchone()
        if row is None:
            return None
        intent = dict(row)
        candidate = record["candidate"]
        if (intent["repository"] != str(self.target.path) or intent["old_head"] != candidate["old_head"]
                or intent["new_head"] != candidate["candidate_sha"]
                or json.loads(intent["leases"]) != [record["lease"]]
                or intent["state"] not in {"pending", "accepted", "rejected"}):
            raise PromotionError("Promotion intent differs from saved exact candidate and lease")
        return intent

    def _validate_accepted(self, record: dict) -> None:
        intent = self._intent(record)
        candidate_sha = record["candidate"]["candidate_sha"]
        head = self.target.head()
        task = self.ledger.task(record["lease"]["task_id"])
        if (intent is None or intent["state"] != "accepted"
                or not (head == candidate_sha or self.target.is_ancestor(candidate_sha, head))
                or task["status"] != "complete" or task["accepted_commit"] != candidate_sha
                or task["epoch"] != record["lease"]["epoch"] or task["worker"] != record["lease"]["worker_id"]):
            raise PromotionError("Accepted receipt has no matching published Git and task authority")

    def _finish(self, record: dict, status: str, detail: str) -> dict:
        return self._put({**record, "status": status, "detail": detail,
                          "independent_final_project_acceptance": False})

    def tick(self) -> list[dict]:
        events = self.peer.store.state()["events"]
        return [self.process(event["event_id"], crash_at=self.crash_at) for event in events if event["kind"] == OFFER_KIND]

    def process(self, offer_event_id: str, *, crash_at: str | None = None) -> dict:
        if crash_at not in CRASH_POINTS:
            raise PromotionError("Unknown crash boundary")
        with self.coding._active(), self.lock:
            if self.closed or self.failed_closed:
                raise PromotionError("Promotion bridge is closed or failed closed")
            event = next((event for event in self.peer.store.state()["events"]
                          if event["event_id"] == offer_event_id and event["kind"] == OFFER_KIND), None)
            if event is None:
                return {"status": "waiting", "reason": "offer_not_arrived"}
            if offer_event_id in self.rejections:
                return self.rejections[offer_event_id].copy()
            try:
                offer = validate_offer(self.peer.store.artifact(event["artifact_sha256"]), event["producer"])
            except (ValueError, KeyError, TypeError) as error:
                return self._reject_event(offer_event_id, f"Invalid arrived offer: {error}")
            key = self._key(offer)
            prior = self.records.get(key)
            if prior is not None:
                if prior["offer"] != offer:
                    return self._reject_event(offer_event_id, "One coding action cannot silently change its candidate offer")
                if prior["status"] in _TERMINAL:
                    if prior["status"] == "accepted":
                        self._validate_accepted(prior)
                    return strict_loads(_raw(prior), max_bytes=MAX_RECORD_BYTES)
                if prior["status"] in {"admitted", "preparing"}:
                    return self._finish(prior, "interrupted", "Interrupted before durable exact validation; no automatic retry")
                return self._promote(prior, crash_at=crash_at)
            bundle = self._owned_bytes(offer["principal"], offer["bundle_sha256"], "application/x-git-bundle")
            if bundle is None:
                return {"status": "waiting", "reason": "bundle_not_arrived"}
            if len(self.records) >= MAX_ATTEMPTS:
                raise PromotionError("Promotion attempt capacity exceeded")
            record: dict = {"protocol": PROTOCOL, "attempt_id": key, "offer_event_id": offer_event_id,
                      "offer": offer, "config_sha256": self.config_sha256, "status": "admitted",
                      "independent_final_project_acceptance": False}
            self._put(record)
            if crash_at == "after_admission":
                raise SimulatedCrash("Promotion admitted before quarantine or validation")
            try:
                action, receipt, request, result = self._authoritative(offer)
                quarantine = bundle_module.import_bundle(bundle, offer["bundle_manifest"], self.root / ("quarantine-" + key))
                base = offer["bundle_manifest"]["base_sha"]
                offered = offer["bundle_manifest"]["offered_sha"]
                if base != request["base_sha"] or quarantine.read_files(base) != request["files"]:
                    raise PromotionError("Received bundle base differs from authoritative coding source")
                expected = request["files"].copy()
                allowed = self.coding.task_specs[action["task_id"]]["allowed_paths"]
                for path, content in result["changes"].items():
                    if path not in allowed:
                        raise PromotionError("Authoritative patch exceeds allowed paths")
                    if content is None:
                        expected.pop(path, None)
                    else:
                        expected[path] = content
                if quarantine.read_files(offered) != expected:
                    raise PromotionError("Received offered tree differs from authoritative worker patch")
                record = {**record, "status": "preparing", "authority_receipt_sha256": _digest(receipt),
                          "old_head": self.target.head(), "validation": None}
                self._put(record)
                validation: dict = {}

                def validate(checkout: Path) -> tuple[bool, str]:
                    candidate_sha = _run(checkout, "rev-parse", "HEAD").stdout.decode().strip()
                    entries = _run(checkout, "ls-tree", "-r", "-z", candidate_sha).stdout.split(b"\0")
                    files = {}
                    for entry in entries:
                        if not entry:
                            continue
                        metadata, name = entry.split(b"\t", 1)
                        mode, kind, object_id = metadata.split()
                        if mode not in {b"100644", b"100755"} or kind != b"blob":
                            raise PromotionError("Merged candidate has a nonregular source entry")
                        files[name.decode("utf-8")] = _run(checkout, "cat-file", "blob", object_id.decode()).stdout.decode("utf-8")
                    raw = self.validator.evaluate(files, self.cases)
                    validation.update({"purpose": PURPOSE, "config_sha256": self.config_sha256,
                                       "candidate_sha": candidate_sha, "source_sha256": _evaluator_digest(files),
                                       "receipt": raw, "receipt_sha256": _digest(raw),
                                       "physically_executed": raw.get("offline_test_stub") is not True})
                    self._put({**record, "validation": validation})
                    return raw.get("passed") is True and raw.get("cleanup_verified") is True, raw.get("status", "invalid_receipt")

                candidate = self.target.prepare(quarantine, offered, record["old_head"], validate, tuple(allowed))
                record = {**record, "candidate": {**asdict(candidate), "changed_paths": list(candidate.changed_paths)},
                          "validation": validation or None}
                if candidate.status != "prepared":
                    status = "infrastructure_failed" if validation and validation["receipt"]["status"] not in {"passed", "failed"} else "rejected"
                    return self._finish(record, status, candidate.detail)
                task = self.ledger.task(action["task_id"])
                lease = Lease(action["task_id"], action["principal"], action["epoch"], task["expires"])
                record = {**record, "status": "validated", "lease": asdict(lease)}
                self._validate_saved(record)
                self._put(record)
            except (PromotionError, bundle_module.BundleError) as error:
                return self._finish(self.records[key], "rejected", f"{type(error).__name__}: {error}")
            except Exception as error:
                return self._finish(self.records[key], "infrastructure_failed", f"{type(error).__name__}: {error}")
            if crash_at == "after_validation":
                raise SimulatedCrash("Exact validation persisted before promotion")
            return self._promote(record, crash_at=crash_at)

    def _promote(self, record: dict, *, crash_at: str | None) -> dict:
        self._validate_saved(record)
        candidate = Candidate(**{**record["candidate"], "changed_paths": tuple(record["candidate"]["changed_paths"])})
        if candidate.candidate_sha is None:
            raise PromotionError("Missing tested candidate commit")
        lease = Lease(**record["lease"])
        if record["status"] == "promoting":
            # Reconcile only this bridge's exact intent; never recover another
            # coordinator's unrelated live attempt or begin a fresh generation.
            with self.coordinator._exclusive():
                intent = self._intent(record)
                if intent is None:
                    return self._finish(record, "interrupted", "Stopped before durable promotion intent; no automatic retry")
                head = self.target.head()
                accepted = head == candidate.candidate_sha or self.target.is_ancestor(candidate.candidate_sha, head)
                if intent["state"] == "pending":
                    self.ledger.finish_intent(intent["id"], accepted, "Recovered exact receiver promotion intent")
                elif (intent["state"] == "accepted") != accepted:
                    raise PromotionError("Retained intent disagrees with accepted ancestry")
                return self._finish({**record, "intent_id": intent["id"], "head": head},
                                    "accepted" if accepted else "rejected", "Recovered same exact promotion intent")
        try:
            # The provider result's old lease is evidence, never a new lease.
            # Ledger fencing checks current principal, epoch and expiry at CAS admission.
            self._authoritative(record["offer"])
            with self.coordinator._exclusive():
                with self.ledger.atomic() as db:
                    now = self.coding.authority._now(db)
                    current = ledger_module.Ledger._validate(db, lease, now)
                    lease = Lease(lease.task_id, lease.worker_id, lease.epoch, current["expires"])
                    # begin_intent joins this exact write transaction, so its
                    # lease check uses time sampled after SQLite admission.
                    intent = self.ledger.begin_intent([lease], self.target.path, candidate.old_head,
                                                      candidate.candidate_sha, now=now)
                    record = self._put({**record, "status": "promoting", "lease": asdict(lease),
                                        "intent_id": intent["id"]})
                # The intent committed before Git. Replay its same identity;
                # never keep a database transaction open across the Git CAS.
                outcome = self.coordinator._promote(
                    self.target, candidate, [lease], now=now,
                    crash_at=crash_at if crash_at in {"after_intent", "after_git"} else None)
        except SimulatedCrash:
            raise
        except ClaimRejected as error:
            return self._finish(record, "rejected", str(error))
        except Exception as error:
            # If Git or journal failed after an intent, preserve the ambiguous
            # promoting phase for exact recovery instead of inventing failure.
            if self.records[record["attempt_id"]]["status"] == "promoting":
                raise
            return self._finish(record, "rejected", f"{type(error).__name__}: {error}")
        if crash_at == "after_promotion":
            raise SimulatedCrash("Git and ledger finalized before bridge receipt")
        return self._finish({**record, "intent_id": outcome.intent_id, "head": outcome.head},
                            outcome.status if outcome.status in _TERMINAL else "rejected", outcome.detail)
