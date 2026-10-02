"""Independent, bounded loopback TCP mesh with explicit payload demand.

Transport ticks run separately from role/action loops. Notices replicate to the
configured membership; bytes move only for local explicit subscriptions (or the
immutable broker relay policy). No directory, file, or observer evidence lookup
exists. HMAC authenticates trusted-runtime membership and request binding, not
Byzantine producer identity. This plaintext loopback protocol is a local harness.
"""
from __future__ import annotations

import argparse
import base64
from dataclasses import asdict, dataclass, field, fields
import hashlib
import hmac
import os
from pathlib import Path
import re
import signal
import socket
import socketserver
import struct
import threading
import time
from typing import Any
import uuid

from .peer_mesh_store_v2 import MeshError, MeshLimits, MeshStore, PROTOCOL, digest, require
from .peer_project_contract_v2 import EvidenceRef, identifier, sha256
from .peer_store_v1 import canonical_bytes, strict_loads

MAX_FRAME = 2_097_152


@dataclass(frozen=True)
class MeshConfig:
    root: Path
    node_id: str
    cohort_id: str
    execution_contract_sha256: str
    roster: tuple[str, ...]
    transport_key: str
    observer_key: str | None = None
    mode: str = "gossip"
    brokers: tuple[str, ...] = ()
    port: int = 0
    interval: float = 0.05
    fanout: int = 2
    limits: MeshLimits = field(default_factory=MeshLimits)

    def __post_init__(self) -> None:
        try:
            identifier(self.node_id)
            identifier(self.cohort_id)
            sha256(self.execution_contract_sha256)
            require(isinstance(self.root, Path), "Root must be a Path")
            require(type(self.roster) is tuple and 1 <= len(self.roster) <= 64
                    and len(set(self.roster)) == len(self.roster), "Invalid roster")
            for member in self.roster:
                identifier(member)
            require(self.node_id in self.roster and "observer" not in self.roster, "Missing self or reserved identity")
            require(type(self.transport_key) is str and 32 <= len(self.transport_key) <= 128,
                    "Invalid transport key")
            require(self.observer_key is None or (type(self.observer_key) is str
                    and 32 <= len(self.observer_key) <= 128 and self.observer_key != self.transport_key),
                    "Observer requires a distinct key")
            require(self.mode in {"gossip", "broker"} and type(self.brokers) is tuple
                    and len(set(self.brokers)) == len(self.brokers)
                    and all(broker in self.roster for broker in self.brokers), "Invalid mesh mode")
            require((self.mode == "gossip" and not self.brokers)
                    or (self.mode == "broker" and len(self.brokers) == 2), "Broker mode needs two named brokers")
            require(type(self.port) is int and 0 <= self.port <= 65535, "Invalid loopback port")
            require(type(self.interval) in (int, float) and 0.01 <= self.interval <= 60, "Invalid tick interval")
            require(type(self.fanout) is int and 1 <= self.fanout <= 4, "Invalid fanout")
            require(type(self.limits) is MeshLimits, "Invalid limit object")
        except (ValueError, TypeError) as error:
            raise MeshError(str(error)) from error

    def identity(self) -> dict[str, Any]:
        return {"protocol": PROTOCOL, "root": str(self.root.resolve()), "node_id": self.node_id,
                "cohort_id": self.cohort_id, "execution_contract_sha256": self.execution_contract_sha256,
                "roster": list(self.roster), "transport_key_sha256": hashlib.sha256(self.transport_key.encode()).hexdigest(),
                "observer_key_sha256": hashlib.sha256(self.observer_key.encode()).hexdigest() if self.observer_key else None,
                "mode": self.mode, "brokers": list(self.brokers), "port": self.port,
                "interval": self.interval, "fanout": self.fanout, "limits": asdict(self.limits),
                "broker_relay_policy": "explicit-want-every-arrived-notice" if self.mode == "broker" else "none"}

    def domain(self) -> str:
        value = self.identity()
        return digest({key: item for key, item in value.items()
                       if key not in {"root", "node_id", "observer_key_sha256", "port"}})

    @classmethod
    def from_dict(cls, value: Any) -> MeshConfig:
        require(type(value) is dict and set(value) <= {item.name for item in fields(cls)}, "Invalid config fields")
        require(type(value.get("root")) is str and type(value.get("roster")) is list
                and ("brokers" not in value or type(value["brokers"]) is list)
                and ("limits" not in value or type(value["limits"]) is dict), "Invalid config JSON types")
        options = dict(value)
        try:
            options["root"] = Path(options["root"])
            options["roster"] = tuple(options["roster"])
            if "brokers" in options:
                options["brokers"] = tuple(options["brokers"])
            if "limits" in options:
                options["limits"] = MeshLimits(**options["limits"])
            return cls(**options)
        except (KeyError, TypeError, ValueError) as error:
            raise MeshError("Invalid config values") from error


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("Mesh frame deadline elapsed")
    return remaining


def _signed(body: dict[str, Any], key: str) -> dict[str, Any]:
    raw = canonical_bytes(body, max_bytes=MAX_FRAME)
    return {"body": body, "mac": hmac.new(key.encode(), raw, hashlib.sha256).hexdigest()}


def _verified(value: Any, key: str) -> dict[str, Any]:
    require(type(value) is dict and set(value) == {"body", "mac"}
            and type(value["body"]) is dict and type(value["mac"]) is str
            and re.fullmatch(r"[0-9a-f]{64}", value["mac"]) is not None, "Invalid authenticated frame")
    require(hmac.compare_digest(_signed(value["body"], key)["mac"], value["mac"]), "Authentication failed")
    return value["body"]


def _read(stream: socket.socket, count: int, deadline: float, node: MeshNode | None) -> bytes:
    chunks: list[bytes] = []
    while count:
        stream.settimeout(_remaining(deadline))
        part = stream.recv(min(count, 65_536))
        if not part:
            raise MeshError("Incomplete mesh frame")
        if node is not None:
            node._count(received_bytes=len(part))
        chunks.append(part)
        count -= len(part)
    return b"".join(chunks)


def _receive(stream: socket.socket, deadline: float, maximum: int, node: MeshNode | None = None) -> Any:
    size = struct.unpack("!I", _read(stream, 4, deadline, node))[0]
    require(0 < size <= maximum, "Frame byte limit exceeded")
    return strict_loads(_read(stream, size, deadline, node), max_bytes=maximum)


def _send(stream: socket.socket, value: Any, deadline: float, maximum: int, node: MeshNode | None = None) -> None:
    raw = canonical_bytes(value, max_bytes=maximum)
    frame = memoryview(struct.pack("!I", len(raw)) + raw)
    while frame:
        stream.settimeout(_remaining(deadline))
        sent = stream.send(frame)
        require(sent > 0, "Connection closed while sending")
        if node is not None:
            node._count(sent_bytes=sent)
        frame = frame[sent:]


def _request(port: int, receiver: str, key: str, sender: str, domain: str, operation: str,
             payload: dict[str, Any], limits: MeshLimits, node: MeshNode | None = None) -> Any:
    require(type(port) is int and 1 <= port <= 65535, "Invalid peer port")
    body = {"protocol": PROTOCOL, "sender": sender, "receiver": receiver, "domain": domain,
            "nonce": uuid.uuid4().hex, "operation": operation, "payload": payload}
    deadline = time.monotonic() + limits.deadline_seconds
    with socket.create_connection(("127.0.0.1", port), timeout=_remaining(deadline)) as stream:
        _send(stream, _signed(body, key), deadline, limits.max_frame_bytes, node)
        reply = _verified(_receive(stream, deadline, limits.max_frame_bytes, node), key)
    require(set(reply) == {"protocol", "sender", "receiver", "nonce", "request_sha256", "ok", "result"}
            and reply["protocol"] == PROTOCOL and reply["sender"] == receiver and reply["receiver"] == sender
            and reply["nonce"] == body["nonce"] and reply["request_sha256"] == digest(body)
            and type(reply["ok"]) is bool, "Response does not bind request")
    require(reply["ok"], str(reply["result"]))
    return reply["result"]


def observer_request(port: int, receiver: str, key: str, operation: str,
                     payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """Observer can read counts, bootstrap membership, and impose partitions."""
    value = _request(port, receiver, key, "observer", "observer", operation, payload or {}, MeshLimits())
    require(type(value) is dict, "Invalid observer response")
    return value


class MeshNode:
    def __init__(self, config: MeshConfig) -> None:
        require(type(config) is MeshConfig, "Expected MeshConfig")
        self.config, self.node_id = config, config.node_id
        self.lock = threading.RLock()
        self.store = MeshStore(config.root, config.identity(), config.limits)
        try:
            self.peers: dict[str, int] = self.store.setting("membership")
            self.blocked = self.store.setting("blocked")
            require(type(self.peers) is dict, "Invalid retained membership")
            if self.peers:
                self._validate_membership(self.peers)
            self._validate_blocked(self.blocked)
        except BaseException:
            self.store.close()
            raise
        self.stopping = threading.Event()
        self.server: _Server | None = None
        self.server_thread: threading.Thread | None = None
        self.tick_thread: threading.Thread | None = None
        self.port = 0
        self.round = 0
        self.relay_cursor = 0
        self.cursors: dict[str, tuple[int, int]] = {}
        self.remote_needs: dict[str, list[dict[str, Any]]] = {}
        self.demand_rounds: dict[str, int] = {}
        self.stats = {"sent_bytes": 0, "received_bytes": 0, "contacts": 0, "failed_contacts": 0,
                      "incoming_requests": 0, "invalid_frames": 0, "rejected_handlers": 0,
                      "active_handlers": 0, "peak_handlers": 0, "ticks": 0,
                      "subscription_rejections": 0, "accepted_chunks": 0}
        self.fatal_error: str | None = None

    def _count(self, **values: int) -> None:
        with self.lock:
            for key, value in values.items():
                self.stats[key] += value
            self.stats["peak_handlers"] = max(self.stats["peak_handlers"], self.stats["active_handlers"])

    def _validate_membership(self, peers: Any) -> None:
        require(type(peers) is dict and set(peers) == set(self.config.roster), "Membership must match frozen roster")
        require(all(type(port) is int and 1 <= port <= 65535 for port in peers.values())
                and len(set(peers.values())) == len(peers), "Invalid or duplicate endpoint")
        if self.config.port:
            require(peers[self.node_id] == self.config.port, "Membership disagrees with configured port")

    def _validate_blocked(self, blocked: Any) -> None:
        require(type(blocked) is list and len(blocked) == len(set(blocked))
                and all(peer in self.config.roster and peer != self.node_id for peer in blocked),
                "Invalid partition members")

    def configure(self, peers: dict[str, int]) -> None:
        self._validate_membership(peers)
        with self.lock:
            require(not self.peers or self.peers == peers, "Membership is immutable after bootstrap")
            require(not self.port or peers[self.node_id] == self.port, "Membership does not match listener")
            self.store.set_setting("membership", peers)
            self.peers = peers.copy()

    def block(self, peers: tuple[str, ...]) -> None:
        require(type(peers) is tuple, "Partition must be a tuple")
        blocked = sorted(peers)
        self._validate_blocked(blocked)
        with self.lock:
            self.store.set_setting("blocked", blocked)
            self.blocked = blocked

    def start(self) -> dict[str, Any]:
        with self.lock:
            require(self.server is None and not self.stopping.is_set(), "Node already started or closed")
            port = self.peers.get(self.node_id, self.config.port)
            server = _Server(("127.0.0.1", port), self)
            self.server = server
            self.port = server.server_address[1]
            self.server_thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
            self.tick_thread = threading.Thread(target=self._run_ticks, daemon=True)
            self.server_thread.start()
            self.tick_thread.start()
            return {"protocol": PROTOCOL, "node_id": self.node_id, "port": self.port, "pid": os.getpid()}

    def close(self) -> None:
        self.stopping.set()
        if self.tick_thread is not None:
            self.tick_thread.join(self.config.limits.deadline_seconds * self.config.fanout + 1)
            require(not self.tick_thread.is_alive(), "Mesh tick shutdown exceeded bound")
        if self.server is not None:
            self.server.shutdown()
            self.server.server_close()
        if self.server_thread is not None:
            self.server_thread.join(1)
        self.store.close()

    def publish(self, kind: str, payload: bytes, command_id: str) -> EvidenceRef:
        return self.store.publish(kind, payload, command_id)

    def arrived(self) -> tuple[EvidenceRef, ...]:
        return self.store.arrived()

    def want(self, ref: EvidenceRef) -> bool:
        accepted = self.store.want(ref)
        if not accepted:
            self._count(subscription_rejections=1)
        return accepted

    def resolve(self, ref: EvidenceRef) -> bytes | None:
        return self.store.resolve(ref)

    def summary(self) -> dict[str, Any]:
        with self.lock:
            return {**self.store.summary(), "protocol": PROTOCOL, "node_id": self.node_id,
                    "pid": os.getpid(), "port": self.port, "mode": self.config.mode,
                    "cohort_id": self.config.cohort_id, "execution_contract_sha256": self.config.execution_contract_sha256,
                    "limits": asdict(self.config.limits), "roster": list(self.config.roster),
                    "membership": self.peers.copy(), "blocked": self.blocked[:],
                    "stats_since_boot": self.stats.copy(), "fatal_error": self.fatal_error}

    def _needs(self, values: Any) -> list[dict[str, Any]]:
        require(type(values) is list and len(values) <= self.config.limits.chunk_batch, "Invalid demand batch")
        seen: set[tuple[str, int]] = set()
        for value in values:
            require(type(value) is dict and set(value) == {"sha", "index"}, "Invalid chunk demand")
            sha256(value["sha"])
            require(type(value["index"]) is int and 0 <= value["index"] < 256, "Invalid chunk index")
            entry = (value["sha"], value["index"])
            require(entry not in seen, "Duplicate chunk demand")
            seen.add(entry)
        return values

    def _local_needs(self, peer: str, direction: str) -> list[dict[str, Any]]:
        # Each neighbor sees every subscription. A shared global tick can alias
        # cyclic peer selection and request an object only from the wrong peer.
        with self.lock:
            key = direction + ":" + peer
            turn = self.demand_rounds.get(key, 0)
            self.demand_rounds[key] = turn + 1
            return self.store.needs(turn)

    def _offer(self, needs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        result = []
        for need in self._needs(needs):
            data = self.store.read_chunk(need["sha"], need["index"])
            if data is not None:
                result.append({**need, "data": base64.b64encode(data).decode("ascii")})
        return result

    def _accept(self, chunks: Any, requested: list[dict[str, Any]] | None = None) -> None:
        require(type(chunks) is list and len(chunks) <= self.config.limits.chunk_batch, "Invalid chunk batch")
        permitted = {(item["sha"], item["index"]) for item in requested} if requested is not None else None
        for chunk in chunks:
            require(type(chunk) is dict and set(chunk) == {"sha", "index", "data"}
                    and type(chunk["data"]) is str and len(chunk["data"]) <= (self.config.limits.chunk_bytes + 2) // 3 * 4,
                    "Invalid encoded chunk")
            self._needs([{key: chunk[key] for key in ("sha", "index")}])
            require(permitted is None or (chunk["sha"], chunk["index"]) in permitted, "Unrequested reply chunk")
            try:
                data = base64.b64decode(chunk["data"], validate=True)
            except ValueError as error:
                raise MeshError("Invalid base64 chunk") from error
            self.store.accept_chunk(chunk["sha"], chunk["index"], data)
            self._count(accepted_chunks=1)

    def _dispatch(self, body: dict[str, Any]) -> Any:
        require(set(body) == {"protocol", "sender", "receiver", "domain", "nonce", "operation", "payload"}
                and body["protocol"] == PROTOCOL and body["receiver"] == self.node_id
                and type(body["nonce"]) is str and re.fullmatch(r"[0-9a-f]{32}", body["nonce"]) is not None
                and type(body["payload"]) is dict, "Invalid request fields")
        sender, operation, payload = body["sender"], body["operation"], body["payload"]
        require(type(sender) is str and type(operation) is str, "Invalid sender or operation")
        if sender == "observer":
            require(body["domain"] == "observer", "Invalid observer domain")
            if operation == "summary" and not payload:
                return self.summary()
            if operation == "configure" and set(payload) == {"peers"}:
                self.configure(payload["peers"])
                return {"configured": True}
            if operation == "block" and set(payload) == {"peers"} and type(payload["peers"]) is list:
                self.block(tuple(payload["peers"]))
                return {"blocked": self.blocked[:]}
            raise MeshError("Observer operation refused")
        with self.lock:
            require(sender in self.peers and sender != self.node_id and sender not in self.blocked,
                    "Peer absent or partitioned")
        require(body["domain"] == self.config.domain() and operation == "exchange", "Peer domain or operation mismatch")
        require(set(payload) == {"after", "cursor", "notices", "needs", "chunks"}, "Invalid exchange fields")
        require(type(payload["cursor"]) is int and 0 <= payload["cursor"] <= self.config.limits.max_events,
                "Invalid offered cursor")
        needs = self._needs(payload["needs"])
        cursor, notices = self.store.export(payload["after"])
        self.store.merge(payload["notices"])
        self._relay()
        self._accept(payload["chunks"])
        return {"push_ack": payload["cursor"], "cursor": cursor, "notices": notices,
                "chunks": self._offer(needs), "needs": self._local_needs(sender, "incoming")}

    def _relay(self) -> None:
        if self.config.mode != "broker" or self.node_id not in self.config.brokers:
            return
        # Named infrastructure explicitly subscribes for store-and-forward. A
        # rejected reservation advances this scan and cannot poison later work.
        with self.lock:
            cursor, notices = self.store.export(self.relay_cursor)
            for notice in notices:
                self.want(self.store.ref(notice))
            self.relay_cursor = cursor

    def _turn(self) -> None:
        with self.lock:
            peers = self.peers.copy()
            blocked = set(self.blocked)
            turn = self.round
            self.round += 1
        if not peers:
            return
        self._relay()
        names = sorted(name for name in peers if name != self.node_id)
        if self.config.mode == "broker":
            names = [name for name in names if name in self.config.brokers]
        if not names:
            return
        offset = (turn * self.config.fanout + self.config.roster.index(self.node_id)) % len(names)
        chosen = (names[offset:] + names[:offset])[:self.config.fanout]
        for name in chosen:
            if name in blocked or self.stopping.is_set():
                continue
            pushed, pulled = self.cursors.get(name, (0, 0))
            cursor, notices = self.store.export(pushed)
            needs = self._local_needs(name, "outgoing")
            payload = {"after": pulled, "cursor": cursor, "notices": notices, "needs": needs,
                       "chunks": self._offer(self.remote_needs.get(name, []))}
            self._count(contacts=1)
            try:
                result = _request(peers[name], name, self.config.transport_key, self.node_id,
                                  self.config.domain(), "exchange", payload, self.config.limits, self)
                require(type(result) is dict and set(result) == {"push_ack", "cursor", "notices", "chunks", "needs"}
                        and type(result["push_ack"]) is int and result["push_ack"] == cursor
                        and type(result["cursor"]) is int and pulled <= result["cursor"] <= self.config.limits.max_events,
                        "Invalid exchange response")
                self.store.merge(result["notices"])
                self._accept(result["chunks"], needs)
                self.remote_needs[name] = self._needs(result["needs"])
                self.cursors[name] = (cursor, result["cursor"])
            except (OSError, ValueError):
                self._count(failed_contacts=1)

    def _run_ticks(self) -> None:
        while not self.stopping.is_set():
            try:
                self._turn()
                self._count(ticks=1)
            except Exception as error:
                with self.lock:
                    self.fatal_error = f"{type(error).__name__}: {error}"
                self.stopping.set()
                return
            self.stopping.wait(self.config.interval)


class _Handler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        server = self.server
        assert isinstance(server, _Server)
        node = server.node
        deadline = time.monotonic() + node.config.limits.deadline_seconds
        try:
            envelope = _receive(self.request, deadline, node.config.limits.max_frame_bytes, node)
            require(type(envelope) is dict and type(envelope.get("body")) is dict, "Invalid frame body")
            key = node.config.observer_key if envelope["body"].get("sender") == "observer" else node.config.transport_key
            require(key is not None, "Observer access disabled")
            assert key is not None
            body = _verified(envelope, key)
            node._count(incoming_requests=1)
            try:
                result, ok = node._dispatch(body), True
            except (ValueError, KeyError, TypeError) as error:
                result, ok = f"{type(error).__name__}: {error}", False
            response = {"protocol": PROTOCOL, "sender": node.node_id, "receiver": body.get("sender"),
                        "nonce": body.get("nonce"), "request_sha256": digest(body), "ok": ok, "result": result}
            _send(self.request, _signed(response, key), deadline, node.config.limits.max_frame_bytes, node)
        except (OSError, ValueError, TypeError, KeyError):
            node._count(invalid_frames=1)


class _Server(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = False
    block_on_close = True
    request_queue_size = 64

    def __init__(self, address: tuple[str, int], node: MeshNode) -> None:
        self.node = node
        self.slots = threading.BoundedSemaphore(node.config.limits.max_handlers)
        super().__init__(address, _Handler)

    def process_request(self, request: Any, client_address: Any) -> None:
        if not self.slots.acquire(blocking=False):
            self.node._count(rejected_handlers=1)
            self.shutdown_request(request)
            return
        self.node._count(active_handlers=1)
        try:
            super().process_request(request, client_address)
        except BaseException:
            self.slots.release()
            self.node._count(active_handlers=-1)
            raise

    def process_request_thread(self, request: Any, client_address: Any) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self.node._count(active_handlers=-1)
            self.slots.release()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    options = parser.parse_args()
    with options.config.open("rb") as stream:
        raw = stream.read(32_001)
    config = MeshConfig.from_dict(strict_loads(raw, max_bytes=32_000))
    node = MeshNode(config)
    signal.signal(signal.SIGTERM, lambda *_: node.stopping.set())
    signal.signal(signal.SIGINT, lambda *_: node.stopping.set())
    try:
        ready = canonical_bytes(node.start()).decode("utf-8")
        print(ready, flush=True)
        while not node.stopping.wait(0.1):
            pass
        if node.fatal_error:
            raise MeshError(node.fatal_error)
    finally:
        node.close()


if __name__ == "__main__":
    main()
