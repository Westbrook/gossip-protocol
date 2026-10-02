"""Peer-local, provided-task coding over an arrived payload mesh.

The only CLI provider is a frozen offline Responses fixture. No credential
discovery, generated task planning, candidate execution or Git publication is
performed. Peer producer labels assume the existing trusted group, not Byzantine
authentication. A coding receipt and complete arrived result bytes are both
required before the local peer publishes result evidence.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import threading
from typing import Any, Callable

from .peer_authority_v1 import Authority, AuthorityError, _Denied, _Server as AuthorityServer, authority_request
from .peer_coding_dispatch_v1 import CodingDispatch, MAX_PAYLOAD_BYTES, MAX_RESULT_BYTES, PayloadUnavailable, PROTOCOL as DISPATCH_PROTOCOL, canonical_payload, source_digest
from .peer_payload_runtime_v1 import PAYLOAD_KIND, PayloadPeer, PayloadServer
from .peer_runtime_v1 import _save
from .peer_store_v1 import canonical_bytes, strict_loads
from .worker import HTTPResponse, OpenAIWorker, WorkerRequest

PROTOCOL = "peer-coding-runtime-v1"
MAX_CONFIG_BYTES = 4_000_000
MAX_STATE_BYTES = 65_536
MAX_CONTEXT_EVENTS = 32
_SAFE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}\Z")
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_TERMINAL = {"published", "denied", "failed", "unknown"}
_PHASES = {"prepared", "claimed", "dispatch_pending", "receipt", *_TERMINAL}
CRASH_POINTS = {"after_claim", "after_receipt", "after_publish"}


def _digest(value: Any) -> str:
    return hashlib.sha256(canonical_payload(value)).hexdigest()


def _sha(value: Any) -> str:
    if type(value) is not str or _SHA.fullmatch(value) is None:
        raise ValueError("Invalid coding digest")
    return value


def _identifier(value: Any) -> str:
    if type(value) is not str or _SAFE.fullmatch(value) is None:
        raise ValueError("Invalid coding identity")
    return value


def _owned_notice(peer: PayloadPeer, producer: str, sha: str) -> dict | None:
    """Evidence must have arrived in this node's event database."""
    peer.arrived_payloads()
    for event in peer.store.state()["events"]:
        if event["producer"] == producer and event["kind"] == PAYLOAD_KIND:
            descriptor = peer.store.artifact(event["artifact_sha256"])
            if descriptor["sha256"] == sha and descriptor["media_type"] == "application/json":
                return event
    return None


class MeshPayloads:
    """Authority payload access has no path or cross-node store capability."""

    def __init__(self, peer: PayloadPeer):
        self.peer = peer

    def read_owned(self, principal: str, sha: str) -> bytes:
        if _owned_notice(self.peer, principal, sha) is None or not self.peer.payloads.has_complete(sha):
            raise PayloadUnavailable("Request payload has not arrived")
        return self.peer.payloads.read(sha)

    def put_owned(self, principal: str, data: bytes) -> str:
        # CodingDispatch owns the durable journal and invokes this only after
        # recording provider output. Publication is idempotent after a crash.
        sha = hashlib.sha256(data).hexdigest()
        self.peer.publish_payload(data, "application/json", _digest([PROTOCOL, principal, sha, "result"]))
        return sha


class CodingAuthority(Authority):
    """Use the existing signed RPC, ledger, request IDs and lookup namespace."""

    def __init__(self, root: Path, config: dict, *, peer: PayloadPeer,
                 workers: dict[str, OpenAIWorker], task_specs: dict,
                 transport_identity: str, crash_hook: Callable[[str, str], None] | None = None):
        super().__init__(root, config, crash_hook=crash_hook)
        try:
            self.coding = CodingDispatch(self, MeshPayloads(peer), workers, task_specs,
                                         transport_mode="offline", transport_identity=transport_identity,
                                         crash_hook=crash_hook)
        except BaseException:
            self.close()
            raise

    def _process(self, body: dict) -> dict:
        if body["operation"] == "coding_dispatch":
            return self.coding.execute(body)
        return super()._process(body)

    def _operate(self, db: sqlite3.Connection, principal: str, request_id: str,
                 operation: str, payload: dict, now: float) -> tuple[dict, bool]:
        task = payload.get("task_id")
        if type(task) is str and task in self.coding.task_specs:
            if operation == "dispatch":
                raise _Denied("forbidden")
            if operation == "claim":
                self.coding.guard_claim(db, task)
        return super()._operate(db, principal, request_id, operation, payload, now)


class CodingPeer(PayloadPeer):
    """One durable provided task; only the peer's autonomous timer executes it."""

    def __init__(self, root: Path, node_id: str, mode: str, token: str, *, coding: dict,
                 authority: dict, interval: float = 0.1, fanout: int = 2,
                 crash_point: str | None = None,
                 authority_call: Callable[..., dict] = authority_request):
        fields = {"run_id", "task_id", "profile_id", "seed_producer", "seed_sha256", "service_producer",
                  "authority_sha256", "dispatch_config_sha256"}
        if type(coding) is not dict or set(coding) != fields:
            raise ValueError("Invalid coding policy")
        for key in ("run_id", "task_id", "profile_id", "seed_producer", "service_producer"):
            _identifier(coding[key])
        for key in ("seed_sha256", "authority_sha256", "dispatch_config_sha256"):
            _sha(coding[key])
        if (type(authority) is not dict or set(authority) != {"port", "principal", "key"}
                or type(authority["port"]) is not int or not 1 <= authority["port"] <= 65535
                or authority["principal"] != node_id or type(authority["key"]) is not str
                or not 32 <= len(authority["key"]) <= 128
                or crash_point is not None and crash_point not in CRASH_POINTS):
            raise ValueError("Invalid coding endpoint or crash boundary")
        super().__init__(root, node_id, mode, token, interval=interval, fanout=fanout)
        try:
            self.coding_config = strict_loads(canonical_payload(coding))
            self.authority_config, self.authority_call = authority.copy(), authority_call
            self.coding_lock = threading.Lock()
            self.coding_crash_point = crash_point
            self.coding_error: str | None = None
            self.coding_path = root / "coding-state.json"
            self.policy_sha256 = _digest({"protocol": PROTOCOL, "node_id": node_id, "coding": coding,
                                         "capability_sha256": hashlib.sha256(authority["key"].encode()).hexdigest()})
            self.record: dict | None = None
            if self.coding_path.exists():
                envelope = strict_loads(self.coding_path.read_bytes(), max_bytes=MAX_STATE_BYTES)
                if (type(envelope) is not dict or set(envelope) != {"data", "sha256"}
                        or envelope["sha256"] != _digest(envelope["data"])):
                    raise ValueError("Corrupt coding journal checksum")
                data = envelope["data"]
                if (type(data) is not dict or set(data) != {"protocol", "node_id", "policy_sha256", "record"}
                        or data["protocol"] != PROTOCOL or data["node_id"] != node_id
                        or data["policy_sha256"] != self.policy_sha256):
                    raise ValueError("Coding journal identity changed")
                self.record = data["record"]
                if self.record is not None:
                    self._validate_record(self.record)
            else:
                self._persist(self.record)
        except BaseException:
            self.owner.close()
            raise

    def _persist(self, record: dict | None) -> None:
        data = {"protocol": PROTOCOL, "node_id": self.node_id,
                "policy_sha256": self.policy_sha256, "record": record}
        value = {"data": data, "sha256": _digest(data)}
        canonical_bytes(value, max_bytes=MAX_STATE_BYTES)
        _save(self.coding_path, value)

    def _claim_valid(self, intent: dict, reply: Any) -> None:
        if (type(reply) is not dict or reply.get("status") != "ok"
                or reply.get("run_id") != intent["run_id"]
                or reply.get("config_sha256") != self.coding_config["authority_sha256"]
                or type(reply.get("lease")) is not dict):
            raise ValueError("Invalid coding claim")
        lease = reply["lease"]
        if (lease.get("task_id") != intent["task_id"] or lease.get("worker_id") != self.node_id
                or type(lease.get("epoch")) is not int or not 1 <= lease["epoch"] < 2**63
                or type(lease.get("expires_at")) not in (int, float)):
            raise ValueError("Coding claim does not bind local action")

    def _response_valid(self, intent: dict, claim: dict, reply: Any) -> None:
        if (type(reply) is not dict or reply.get("status") not in {"completed", "failed", "unknown"}
                or reply.get("run_id") != intent["run_id"]
                or reply.get("config_sha256") != self.coding_config["authority_sha256"]
                or reply.get("dispatch_config_sha256") != self.coding_config["dispatch_config_sha256"]
                or reply.get("dispatch_protocol") != DISPATCH_PROTOCOL
                or reply.get("principal") != self.node_id
                or reply.get("request_id") != intent["action_id"] + ":coding_dispatch"
                or reply.get("task_id") != intent["task_id"]
                or reply.get("profile_id") != intent["profile_id"]
                or reply.get("action_id") != intent["action_id"]
                or type(reply.get("epoch")) is not int or reply["epoch"] != claim["lease"]["epoch"]
                or reply.get("payload_sha256") != intent["request_sha256"]
                or reply.get("worker_request_sha256") != intent["worker_request_sha256"]):
            raise ValueError("Coding receipt does not bind local request")
        for key in ("profile_sha256", "worker_request_sha256"):
            _sha(reply.get(key))
        for key in ("reservation_id", "call_id"):
            if type(reply.get(key)) is not str or not reply[key]:
                raise ValueError("Missing coding reservation or call identity")
        if type(reply.get("reserved_units")) is not int or reply["reserved_units"] < 0:
            raise ValueError("Invalid coding reservation")
        if reply["status"] in {"completed", "failed"}:
            _sha(reply.get("result_sha256"))
            if type(reply.get("usage_units")) is not int or not 0 <= reply["usage_units"] <= reply["reserved_units"]:
                raise ValueError("Invalid coding usage")

    def _validate_record(self, record: Any) -> None:
        if (type(record) is not dict or set(record) != {"intent", "phase", "claim", "response", "result_event_id"}
                or type(record["phase"]) is not str or record["phase"] not in _PHASES):
            raise ValueError("Invalid coding journal record")
        intent = record["intent"]
        fields = {"protocol", "node_id", "run_id", "task_id", "profile_id", "seed_sha256", "seed_event_id",
                  "visible_event_ids", "source_sha256", "request_sha256", "worker_request_sha256",
                  "policy_sha256", "source_kind", "action_id"}
        if (type(intent) is not dict or set(intent) != fields or intent["protocol"] != PROTOCOL
                or intent["node_id"] != self.node_id or intent["policy_sha256"] != self.policy_sha256
                or intent["source_kind"] != "provided_worker_request"
                or any(intent[key] != self.coding_config[key] for key in ("run_id", "task_id", "profile_id", "seed_sha256"))):
            raise ValueError("Coding intent identity changed")
        for key in ("seed_sha256", "seed_event_id", "source_sha256", "request_sha256", "worker_request_sha256", "action_id"):
            _sha(intent[key])
        ids = intent["visible_event_ids"]
        if (type(ids) is not list or not 1 <= len(ids) <= MAX_CONTEXT_EVENTS
                or any(type(i) is not str or _SHA.fullmatch(i) is None for i in ids)
                or ids != sorted(set(ids)) or intent["seed_event_id"] not in ids
                or intent["action_id"] != _digest({k: v for k, v in intent.items() if k != "action_id"})):
            raise ValueError("Coding intent context changed")
        phase = record["phase"]
        if phase not in {"prepared", "denied"}:
            self._claim_valid(intent, record["claim"])
        elif phase == "prepared" and record["claim"] is not None:
            raise ValueError("Prepared journal has a claim")
        if phase in {"receipt", "published", "failed", "unknown"}:
            self._response_valid(intent, record["claim"], record["response"])
            expected = {"receipt": "completed", "published": "completed", "failed": "failed", "unknown": "unknown"}
            if record["response"]["status"] != expected[phase]:
                raise ValueError("Coding journal phase differs from receipt")
        elif phase != "denied" and record["response"] is not None:
            raise ValueError("Pending journal has a terminal receipt")
        if phase == "denied":
            reply = record["response"]
            if (type(reply) is not dict or reply.get("status") != "rejected"
                    or reply.get("run_id") != intent["run_id"]
                    or reply.get("config_sha256") != self.coding_config["authority_sha256"]):
                raise ValueError("Invalid denied coding receipt")
            if record["claim"] is not None:
                self._claim_valid(intent, record["claim"])
        if phase == "published":
            _sha(record["result_event_id"])
        elif record["result_event_id"] is not None:
            raise ValueError("Unpublished coding journal has result event")

    def _change(self, phase: str, **fields: Any) -> None:
        if self.record is None:
            raise ValueError("Missing coding intent")
        record = {**self.record, "phase": phase, **fields}
        self._validate_record(record)
        self._persist(record)
        self.record = record

    def _seed(self) -> tuple[dict, dict] | None:
        config = self.coding_config
        notice = _owned_notice(self, config["seed_producer"], config["seed_sha256"])
        if notice is None or not self.payloads.has_complete(config["seed_sha256"]):
            return None
        raw = self.payloads.read(config["seed_sha256"])
        seed = strict_loads(raw, max_bytes=MAX_PAYLOAD_BYTES)
        fields = {"task_id", "instructions", "allowed_paths", "files", "base_sha", "attempt", "feedback"}
        if (type(seed) is not dict or set(seed) != fields or canonical_payload(seed) != raw
                or seed["task_id"] != config["task_id"] or type(seed["allowed_paths"]) is not list
                or type(seed["attempt"]) is not int or seed["attempt"] < 1):
            raise ValueError("Invalid provided WorkerRequest seed")
        # Reuse the actual worker request validator without calling its transport.
        request = WorkerRequest(**{**seed, "allowed_paths": tuple(seed["allowed_paths"])})
        OpenAIWorker("offline-validation", transport=_unreachable_transport).reservation_units(request)
        return seed, notice

    def _request_payload(self, intent: dict, seed: dict) -> bytes:
        payload = {"worker_request": seed, "context": {"source_sha256": source_digest(seed["files"]),
                                                        "event_ids": intent["visible_event_ids"]}}
        raw = canonical_payload(payload)
        if (len(raw) > MAX_PAYLOAD_BYTES or hashlib.sha256(raw).hexdigest() != intent["request_sha256"]
                or source_digest(seed["files"]) != intent["source_sha256"]
                or _digest(seed) != intent["worker_request_sha256"]):
            raise ValueError("Coding source or frozen request changed")
        return raw

    def _prepare(self) -> None:
        found = self._seed()
        if found is None:
            return
        seed, notice = found
        visible = sorted(e["event_id"] for e in self.store.state()["events"])
        if len(visible) > MAX_CONTEXT_EVENTS:
            raise ValueError("Local coding context exceeds bounded event count")
        config = self.coding_config
        payload = {"worker_request": seed, "context": {"source_sha256": source_digest(seed["files"]), "event_ids": visible}}
        intent = {"protocol": PROTOCOL, "node_id": self.node_id, "policy_sha256": self.policy_sha256,
                  **{key: config[key] for key in ("run_id", "task_id", "profile_id", "seed_sha256")},
                  "seed_event_id": notice["event_id"], "visible_event_ids": visible,
                  "source_sha256": source_digest(seed["files"]), "request_sha256": _digest(payload),
                  "worker_request_sha256": _digest(seed), "source_kind": "provided_worker_request"}
        intent["action_id"] = _digest(intent)
        self._request_payload(intent, seed)
        record = {"intent": intent, "phase": "prepared", "claim": None, "response": None, "result_event_id": None}
        self._validate_record(record)
        self._persist(record)  # Exact context and IDs precede any authority mutation.
        self.record = record

    def _authority(self, operation: str, payload: dict) -> dict:
        if self.record is None:
            raise ValueError("No coding intent")
        endpoint = self.authority_config
        reply = self.authority_call(endpoint["port"], self.node_id, endpoint["key"],
                                    self.record["intent"]["action_id"] + ":" + operation,
                                    operation, payload, run_id=self.coding_config["run_id"])
        if (reply.get("run_id") != self.coding_config["run_id"]
                or reply.get("config_sha256") != self.coding_config["authority_sha256"]):
            raise ValueError("Coding authority identity changed")
        if operation == "coding_dispatch" and reply.get("dispatch_config_sha256") != self.coding_config["dispatch_config_sha256"]:
            raise ValueError("Coding dispatch configuration changed")
        return reply

    def _crash_coding(self, point: str) -> None:
        if point == self.coding_crash_point:
            os._exit(83)

    def coding_tick(self) -> None:
        if not self.coding_lock.acquire(blocking=False):
            return
        try:
            if self.record is None:
                self._prepare()
            if self.record is None or self.record["phase"] in _TERMINAL:
                return
            intent = self.record["intent"]
            if self.record["phase"] == "prepared":
                found = self._seed()
                if found is None:
                    raise ValueError("Frozen local seed disappeared")
                self.publish_payload(self._request_payload(intent, found[0]), "application/json",
                                     _digest([PROTOCOL, intent["action_id"], "request"]))
                reply = self._authority("claim", {"task_id": intent["task_id"]})
                if reply.get("status") == "rejected":
                    self._change("denied", response=reply)
                    return
                self._change("claimed", claim=reply)
                self._crash_coding("after_claim")
            if self.record["phase"] == "claimed":
                self._change("dispatch_pending")
            if self.record["phase"] == "dispatch_pending":
                reply = self._authority("coding_dispatch", {
                    "task_id": intent["task_id"], "epoch": self.record["claim"]["lease"]["epoch"],
                    "action_id": intent["action_id"], "profile_id": intent["profile_id"],
                    "request_sha256": intent["request_sha256"]})
                status = reply.get("status")
                if status in {"waiting", "dispatching", "publication_pending"}:
                    return
                if status == "rejected":
                    self._change("denied", response=reply)
                    return
                self._response_valid(intent, self.record["claim"], reply)
                if type(status) is not str:
                    raise ValueError("Invalid coding completion status")
                self._change("receipt" if status == "completed" else status, response=reply)
                self._crash_coding("after_receipt")
            if self.record["phase"] == "receipt":
                reply = self.record["response"]
                sha = reply["result_sha256"]
                notice = _owned_notice(self, self.coding_config["service_producer"], sha)
                if notice is None or not self.payloads.has_complete(sha):
                    return
                raw = self.payloads.read(sha)
                result = strict_loads(raw, max_bytes=MAX_RESULT_BYTES)
                if (type(result) is not dict or set(result) != {"kind", "payload"}
                        or result["kind"] != "result" or type(result["payload"]) is not dict
                        or result["payload"].get("usage_units") != reply["usage_units"]
                        or type(result["payload"].get("usage_units")) is not int
                        or canonical_payload(result) != raw):
                    raise ValueError("Coding result payload disagrees with receipt")
                content = {"protocol": PROTOCOL, "source_kind": "provided_worker_request",
                           "run_id": intent["run_id"], "task_id": intent["task_id"], "action_id": intent["action_id"],
                           "request_sha256": intent["request_sha256"], "result_sha256": sha,
                           "result_notice_event_id": notice["event_id"], "authority": reply}
                event = self.store.publish(_digest([PROTOCOL, intent["action_id"], "result-evidence"]),
                                           "coding_result", content)
                self._crash_coding("after_publish")
                self._change("published", result_event_id=event["event_id"])
        except (OSError, TimeoutError, AuthorityError):
            # Exact RPC replay reconciles a lost reply; it is never a new attempt.
            pass
        finally:
            self.coding_lock.release()

    def dispatch(self, body: dict) -> Any:
        result = super().dispatch(body)
        if body.get("sender") == "driver" and body.get("operation") == "state":
            with self.coding_lock:
                result["coding"] = strict_loads(canonical_payload(self.record))
                result["coding_error"] = self.coding_error
        return result

    def run_ticks(self) -> None:
        while not self.stop.wait(self.interval):
            with self.lock:
                enabled = self.settings["enabled"]
            if enabled:
                try:
                    self.turn()
                    self.payload_turn()
                    self.coding_tick()
                except Exception as error:
                    self.coding_error = type(error).__name__
                    self.fatal_error = type(error).__name__
                    self.stop.set()


def _unreachable_transport(request: Any, timeout: float, limit: int) -> HTTPResponse:
    raise RuntimeError("Validation never invokes a provider")


def fixture_workers(profiles: dict, trace_path: Path | None = None) -> tuple[dict[str, OpenAIWorker], str]:
    """Closed CLI transport: immutable inline Responses JSON, no network loader."""
    if type(profiles) is not dict or not 1 <= len(profiles) <= 8:
        raise ValueError("Invalid offline profiles")
    workers: dict[str, OpenAIWorker] = {}
    for name, spec in profiles.items():
        _identifier(name)
        if (type(spec) is not dict or set(spec) != {"mode", "model", "max_output_tokens", "response"}
                or spec["mode"] != "offline-response-fixture" or type(spec["response"]) is not dict):
            raise ValueError("Only offline response fixtures are supported")
        raw = canonical_bytes(spec["response"], max_bytes=MAX_RESULT_BYTES)

        def response(request: Any, timeout: float, limit: int, body: bytes = raw, profile: str = name) -> HTTPResponse:
            if trace_path is not None:
                entry = {"protocol": "offline-coding-transport-entry-v1", "profile_id": profile, "pid": os.getpid(),
                         "request_sha256": hashlib.sha256(request.data).hexdigest(),
                         "response_sha256": hashlib.sha256(body).hexdigest()}
                with trace_path.open("ab") as stream:
                    stream.write(canonical_bytes(entry) + b"\n")
                    stream.flush()
                    os.fsync(stream.fileno())
            return HTTPResponse(200, {}, body)

        workers[name] = OpenAIWorker("offline-fixture-no-api-key", model=spec["model"],
                                     max_output_tokens=spec["max_output_tokens"], transport=response)
    identity = _digest({"profiles": profiles, "module_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()})
    return workers, identity


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--crash-point")
    parser.add_argument("--crash-request-id")
    args = parser.parse_args()
    config = strict_loads(args.config.read_bytes(), max_bytes=MAX_CONFIG_BYTES)
    common = {"root", "node_id", "mode", "token", "port", "interval", "fanout", "role"}
    extra = {"peer": {"seed"}, "worker": {"coding", "authority"},
             "service": {"authority", "authority_port", "task_specs", "profiles"}}
    if (type(config) is not dict or config.get("role") not in extra
            or set(config) != common | extra[config["role"]]
            or type(config["port"]) is not int or not 0 <= config["port"] <= 65535):
        raise ValueError("Invalid coding runtime configuration")
    role, root = config["role"], Path(config["root"])
    kwargs = {key: config[key] for key in ("interval", "fanout")}
    if role == "worker":
        if args.crash_request_id is not None:
            raise ValueError("Worker crash uses its local phase")
        peer: PayloadPeer = CodingPeer(root, config["node_id"], config["mode"], config["token"],
                                        coding=config["coding"], authority=config["authority"],
                                        crash_point=args.crash_point, **kwargs)
    else:
        peer = PayloadPeer(root, config["node_id"], config["mode"], config["token"], **kwargs)
    authority: CodingAuthority | None = None
    authority_server: AuthorityServer | None = None
    authority_thread: threading.Thread | None = None
    tick_thread: threading.Thread | None = None
    try:
        if role == "peer":
            if args.crash_point or args.crash_request_id:
                raise ValueError("Payload-only peer has no coding fault hook")
            if config["seed"] is not None:
                data = canonical_bytes(config["seed"], max_bytes=MAX_PAYLOAD_BYTES)
                peer.publish_payload(data, "application/json", _digest([PROTOCOL, "provided-seed", hashlib.sha256(data).hexdigest()]))
        if role == "service":
            if (type(config["authority_port"]) is not int or not 0 <= config["authority_port"] <= 65535
                    or bool(args.crash_point) != bool(args.crash_request_id)):
                raise ValueError("Invalid authority port or crash arguments")

            def crash(point: str, request_id: str) -> None:
                if point == args.crash_point and request_id == args.crash_request_id:
                    os._exit(84)

            workers, identity = fixture_workers(config["profiles"], root / "offline-transport.jsonl")
            authority = CodingAuthority(root / "authority", config["authority"], peer=peer,
                                        workers=workers, task_specs=config["task_specs"],
                                        transport_identity=identity, crash_hook=crash)
            authority_server = AuthorityServer(config["authority_port"], authority)
            authority_thread = threading.Thread(target=authority_server.serve_forever,
                                                kwargs={"poll_interval": 0.1}, name="coding-authority", daemon=True)
            authority_thread.start()
        with PayloadServer(config["port"], peer) as server:
            tick_thread = threading.Thread(target=peer.run_ticks, name="local-coding-and-payload", daemon=True)
            tick_thread.start()
            ready = {"protocol": PROTOCOL, "node_id": peer.node_id, "role": role,
                     "pid": os.getpid(), "port": server.server_address[1], "provider_mode": "offline-response-fixture"}
            if authority is not None and authority_server is not None:
                ready.update(authority_port=authority_server.server_address[1], config_sha256=authority.config_sha256,
                             dispatch_config_sha256=authority.coding.config_sha256)
            print(json.dumps(ready), flush=True)
            try:
                server.serve_forever(poll_interval=0.1)
            finally:
                peer.stop.set()
                tick_thread.join()
    finally:
        peer.stop.set()
        if tick_thread is not None and tick_thread.is_alive():
            tick_thread.join()
        if authority_server is not None:
            authority_server.shutdown()
            if authority_thread is not None:
                authority_thread.join()
            authority_server.server_close()
        if authority is not None:
            authority.close()
        peer.owner.close()


if __name__ == "__main__":
    main()
