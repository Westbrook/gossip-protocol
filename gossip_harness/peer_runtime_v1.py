"""Bounded local TCP evidence replication in independently running processes.

This foundation has no agent, candidate executor, spending or Git authority.
Only trusted fixture/runtime processes possess the ephemeral cluster key. HMAC
authenticates cluster membership and request/response binding, not Byzantine
producer identity; loopback plaintext is not a production secure transport.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import socket
import socketserver
import struct
import threading
import time
import uuid
from typing import Any, Callable, cast

from .peer_store_v1 import MAX_BATCH, MAX_EVENTS, PROTOCOL, Store, canonical_bytes, strict_loads

MAX_FRAME = 512_000
MAX_CONNECTIONS = 4
DEADLINE_SECONDS = 2.0
MAX_PEERS = 16
IDENTITY = re.compile(r"[a-z][a-z0-9_-]{0,39}\Z")


class TransportError(ValueError):
    pass


def _identity(value: Any) -> str:
    if not isinstance(value, str) or IDENTITY.fullmatch(value) is None:
        raise TransportError("Invalid node identity")
    return value


def _digest(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value, max_bytes=MAX_FRAME)).hexdigest()


def _signed(body: dict, token: str) -> dict:
    return {"body": body, "mac": hmac.new(token.encode(), canonical_bytes(body, max_bytes=MAX_FRAME), hashlib.sha256).hexdigest()}


def _verified(value: Any, token: str) -> dict:
    if type(value) is not dict or set(value) != {"body", "mac"} or type(value["body"]) is not dict:
        raise TransportError("Invalid authenticated envelope")
    wanted = _signed(value["body"], token)["mac"]
    if type(value["mac"]) is not str or not hmac.compare_digest(value["mac"], wanted):
        raise TransportError("Authentication failed")
    return value["body"]


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("Frame deadline elapsed")
    return remaining


def _read_exact(stream: socket.socket, count: int, deadline: float, account: Callable | None = None) -> bytes:
    pieces = []
    while count:
        stream.settimeout(_remaining(deadline))
        chunk = stream.recv(min(count, 65_536))
        if not chunk:
            raise TransportError("Incomplete frame")
        if account:
            account(received_bytes=len(chunk))
        pieces.append(chunk)
        count -= len(chunk)
    return b"".join(pieces)


def receive(stream: socket.socket, deadline: float, account: Callable | None = None) -> tuple[dict, int]:
    size = struct.unpack("!I", _read_exact(stream, 4, deadline, account))[0]
    if not 1 <= size <= MAX_FRAME:
        raise TransportError("Frame exceeds limit")
    data = _read_exact(stream, size, deadline, account)
    return strict_loads(data, max_bytes=MAX_FRAME), size + 4


def send(stream: socket.socket, value: dict, deadline: float, account: Callable | None = None) -> int:
    data = canonical_bytes(value, max_bytes=MAX_FRAME)
    if not 1 <= len(data) <= MAX_FRAME:
        raise TransportError("Frame exceeds limit")
    frame = memoryview(struct.pack("!I", len(data)) + data)
    while frame:
        stream.settimeout(_remaining(deadline))
        sent = stream.send(frame)
        if not sent:
            raise TransportError("Connection closed while sending")
        if account:
            account(sent_bytes=sent)
        frame = frame[sent:]
    return len(data) + 4


def request(port: int, receiver: str, token: str, operation: str, payload: dict | None = None,
            *, sender: str = "driver", timeout: float = DEADLINE_SECONDS,
            account: Callable | None = None) -> tuple[Any, int, int]:
    """One bounded physical TCP request. Never retries an ambiguous mutation."""
    body = dict(protocol=PROTOCOL, sender=sender, receiver=receiver,
                nonce=uuid.uuid4().hex, operation=operation, payload=payload or {})
    deadline = time.monotonic() + timeout
    with socket.create_connection(("127.0.0.1", port), timeout=_remaining(deadline)) as stream:
        sent = send(stream, _signed(body, token), deadline, account)
        envelope, received = receive(stream, deadline, account)
    response = _verified(envelope, token)
    expected = {"protocol", "sender", "recipient", "nonce", "request_sha256", "ok", "result"}
    if (set(response) != expected or response["protocol"] != PROTOCOL
            or response["sender"] != receiver or response["recipient"] != sender
            or response["nonce"] != body["nonce"] or response["request_sha256"] != _digest(body)
            or type(response["ok"]) is not bool):
        raise TransportError("Response does not bind the request")
    if not response["ok"]:
        raise TransportError(str(response["result"]))
    return response["result"], sent, received


def _save(path: Path, value: dict) -> None:
    temporary = path.with_suffix(".tmp")
    with temporary.open("wb") as stream:
        stream.write(canonical_bytes(value))
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class Peer:
    def __init__(self, root: Path, node_id: str, mode: str, token: str,
                 *, interval: float = 0.1, fanout: int = 2):
        self.node_id = _identity(node_id)
        if node_id == "driver" or mode not in {"gossip", "broker"}:
            raise TransportError("Invalid node configuration")
        if not isinstance(token, str) or not 16 <= len(token) <= 128:
            raise TransportError("An ephemeral cluster capability is required")
        if not 0.05 <= interval <= 60 or type(fanout) is not int or not 1 <= fanout <= 4:
            raise TransportError("Invalid cadence or fanout")
        if mode == "broker" and fanout < 2:
            raise TransportError("Two-broker fallback requires fanout of at least two")
        root.mkdir(parents=True, exist_ok=True)
        self.owner = (root / "owner.lock").open("a")
        try:
            fcntl.flock(self.owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.owner.close()
            raise TransportError("Local node store already has an owner") from None
        self.root, self.mode, self.token = root, mode, token
        self.interval, self.fanout = interval, fanout
        self.lock, self.turn_lock = threading.RLock(), threading.Lock()
        self.stop = threading.Event()
        self.fault: tuple[str, str] | None = None
        self.store = Store(root / "events.sqlite", node_id, crash_hook=self._crash)
        self.settings_path = root / "routing.json"
        self.settings: dict[str, Any] = {"node_id": node_id, "mode": mode, "cluster_sha256": hashlib.sha256(token.encode()).hexdigest(), "peers": {}, "brokers": [],
                         "blocked": [], "enabled": False, "round": 0}
        if self.settings_path.exists():
            saved = strict_loads(self.settings_path.read_bytes(), max_bytes=32_000)
            if (type(saved) is not dict or set(saved) != set(self.settings)
                    or saved["node_id"] != node_id or saved["mode"] != mode
                    or saved["cluster_sha256"] != self.settings["cluster_sha256"]
                    or type(saved["round"]) is not int or saved["round"] < 0
                    or type(saved["blocked"]) is not list):
                raise TransportError("Durable routing identity changed")
            self.settings = saved
            self.configure({k: saved[k] for k in ("peers", "brokers", "enabled")})
            if any(v not in saved["peers"] or v == node_id for v in saved["blocked"]):
                raise TransportError("Invalid durable partition")
        self.stats = {"contacts": 0, "failed_contacts": 0, "sent_bytes": 0,
                      "received_bytes": 0, "incoming_requests": 0, "invalid_frames": 0}
        self.fatal_error: str | None = None

    def _crash(self, point: str, operation: str) -> None:
        with self.lock:
            if self.fault != (point, operation):
                return
            self.fault = None
        os._exit(70 if point == "before_commit" else 71)

    def _count(self, **values: int) -> None:
        with self.lock:
            for key, value in values.items():
                self.stats[key] += value

    def configure(self, payload: dict) -> dict:
        if set(payload) != {"peers", "brokers", "enabled"}:
            raise TransportError("Invalid routing schema")
        peers, brokers = payload["peers"], payload["brokers"]
        if type(peers) is not dict or not 1 <= len(peers) <= MAX_PEERS:
            raise TransportError("Invalid membership")
        for name, port in peers.items():
            _identity(name)
            if name == "driver" or type(port) is not int or not 1 <= port <= 65535:
                raise TransportError("Invalid peer endpoint")
        if self.node_id not in peers or len(set(peers.values())) != len(peers):
            raise TransportError("Missing self or duplicate endpoint")
        if (type(brokers) is not list or len(brokers) != len(set(brokers))
                or len(brokers) > 2 or any(b not in peers for b in brokers)
                or (self.mode == "broker" and len(brokers) != 2) or type(payload["enabled"]) is not bool):
            raise TransportError("Invalid broker or enabled configuration")
        with self.lock:
            prior = self.settings["peers"]
            if prior and (prior != peers or self.settings["brokers"] != brokers):
                raise TransportError("Membership is immutable after bootstrap")
            self.settings.update(peers=peers.copy(), brokers=brokers[:], enabled=payload["enabled"])
            _save(self.settings_path, self.settings)
        return {"configured": True}

    def dispatch(self, body: dict) -> Any:
        if (set(body) != {"protocol", "sender", "receiver", "nonce", "operation", "payload"}
                or body["protocol"] != PROTOCOL or body["receiver"] != self.node_id
                or type(body["nonce"]) is not str or re.fullmatch(r"[0-9a-f]{32}", body["nonce"]) is None
                or type(body["payload"]) is not dict):
            raise TransportError("Invalid request schema")
        sender, operation, payload = body["sender"], body["operation"], body["payload"]
        with self.lock:
            peers, blocked = self.settings["peers"].copy(), self.settings["blocked"][:]
        if sender == "driver":
            if operation == "state" and not payload:
                return {**self.store.state(), "pid": os.getpid(), "mode": self.mode,
                        "routing": self.settings.copy(), "stats_since_boot": self.stats.copy(),
                        "fatal_error": self.fatal_error}
            if operation == "artifact" and set(payload) == {"sha256"}:
                return self.store.artifact(payload["sha256"])
            if operation == "publish" and set(payload) == {"command_id", "kind", "content"}:
                return self.store.publish(payload["command_id"], payload["kind"], payload["content"])
            if operation == "configure":
                return self.configure(payload)
            if operation == "block" and set(payload) == {"peers"}:
                values = payload["peers"]
                if type(values) is not list or any(v not in peers or v == self.node_id for v in values):
                    raise TransportError("Invalid partition")
                with self.lock:
                    self.settings["blocked"] = sorted(set(values))
                    _save(self.settings_path, self.settings)
                return {"blocked": sorted(set(values))}
            if operation == "sync" and not payload:
                return self.turn()
            if operation == "arm_crash" and set(payload) == {"point", "operation"}:
                if payload["point"] not in {"before_commit", "after_commit"} or payload["operation"] not in {"publish", "merge"}:
                    raise TransportError("Invalid crash boundary")
                with self.lock:
                    self.fault = (payload["point"], payload["operation"])
                return {"armed": True}
            raise TransportError("Unknown control operation")
        if sender not in peers or sender in blocked or sender == self.node_id:
            raise TransportError("Peer unavailable or outside membership")
        if operation == "exchange" and set(payload) == {"known"}:
            known = payload["known"]
            if type(known) is not list or len(known) > MAX_EVENTS:
                raise TransportError("Invalid inventory")
            return {"known": self.store.inventory(), "batch": self.store.export(known, MAX_BATCH)}
        if operation == "merge" and set(payload) == {"batch"}:
            return self.store.merge(payload["batch"])
        raise TransportError("Peer operation has no authority")

    def turn(self) -> dict:
        if not self.turn_lock.acquire(blocking=False):
            return {"busy": True}
        try:
            with self.lock:
                peers = self.settings["peers"].copy()
                brokers = self.settings["brokers"][:]
                blocked = self.settings["blocked"][:]
                if not peers:
                    return {"contacts": 0}
                number = self.settings["round"]
                self.settings["round"] += 1
                _save(self.settings_path, self.settings)
            choices = [p for p in peers if p != self.node_id]
            if self.mode == "broker":
                choices = [p for p in brokers if p != self.node_id]
            else:
                choices.sort(key=lambda p: _digest([self.node_id, number, p]))
            attempted = []
            for peer in choices[:self.fanout]:
                attempted.append(peer)
                self._count(contacts=1)
                try:
                    if peer in blocked:
                        raise TransportError("Outbound link partitioned")
                    answer, _, _ = request(peers[peer], peer, self.token, "exchange",
                                           {"known": self.store.inventory()}, sender=self.node_id,
                                           account=self._count)
                    if type(answer) is not dict or set(answer) != {"known", "batch"}:
                        raise TransportError("Invalid exchange response")
                    self.store.merge(answer["batch"])
                    batch = self.store.export(answer["known"], MAX_BATCH)
                    if batch["events"]:
                        request(peers[peer], peer, self.token, "merge",
                                {"batch": batch}, sender=self.node_id, account=self._count)
                    # Clients try standby after failure, without dummy healthy contacts.
                    if self.mode == "broker" and self.node_id not in brokers:
                        break
                except (OSError, ValueError, KeyError, TypeError):
                    self._count(failed_contacts=1)
            return {"contacts": len(attempted), "targets": attempted}
        finally:
            self.turn_lock.release()

    def run_ticks(self) -> None:
        while not self.stop.wait(self.interval):
            with self.lock:
                enabled = self.settings["enabled"]
            if enabled:
                try:
                    self.turn()
                except Exception as error:
                    with self.lock:
                        self.fatal_error = type(error).__name__
                    self.stop.set()


class _Server(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True
    request_queue_size = MAX_CONNECTIONS

    def __init__(self, port: int, peer: Peer):
        self.peer = peer
        self.slots = threading.BoundedSemaphore(MAX_CONNECTIONS)
        super().__init__(("127.0.0.1", port), _Handler)

    def process_request(self, stream, address):
        if not self.slots.acquire(blocking=False):
            self.shutdown_request(stream)
            return
        try:
            super().process_request(stream, address)
        except BaseException:
            self.slots.release()
            raise

    def process_request_thread(self, stream, address):
        try:
            super().process_request_thread(stream, address)
        finally:
            self.slots.release()


class _Handler(socketserver.BaseRequestHandler):
    def handle(self):
        peer = cast(_Server, self.server).peer
        deadline = time.monotonic() + DEADLINE_SECONDS
        try:
            envelope, _ = receive(self.request, deadline, peer._count)
            peer._count(incoming_requests=1)
            body = _verified(envelope, peer.token)
            try:
                result, okay = peer.dispatch(body), True
            except (ValueError, KeyError, TypeError):
                result, okay = "Request rejected", False
            response = dict(protocol=PROTOCOL, sender=peer.node_id, recipient=body.get("sender"),
                            nonce=body.get("nonce"), request_sha256=_digest(body), ok=okay, result=result)
            send(self.request, _signed(response, peer.token), deadline, peer._count)
        except (OSError, ValueError, KeyError, TypeError):
            peer._count(invalid_frames=1)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    config = strict_loads(args.config.read_bytes(), max_bytes=32_000)
    if set(config) != {"root", "node_id", "mode", "token", "port", "interval", "fanout"}:
        raise ValueError("Invalid process configuration")
    if type(config["port"]) is not int or not 0 <= config["port"] <= 65535:
        raise ValueError("Invalid port")
    peer = Peer(Path(config["root"]), config["node_id"], config["mode"], config["token"],
                interval=config["interval"], fanout=config["fanout"])
    with _Server(config["port"], peer) as server:
        thread = threading.Thread(target=peer.run_ticks, name="local-anti-entropy", daemon=True)
        thread.start()
        print(json.dumps({"protocol": PROTOCOL, "node_id": peer.node_id,
                          "pid": os.getpid(), "port": server.server_address[1]}), flush=True)
        try:
            server.serve_forever(poll_interval=0.1)
        finally:
            peer.stop.set()
            thread.join(timeout=DEADLINE_SECONDS * 4 + 1)
            peer.owner.close()


if __name__ == "__main__":
    main()
