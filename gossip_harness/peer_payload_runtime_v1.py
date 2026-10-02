"""Actual bounded payload exchange, driven only by arrived immutable notices.

The existing event protocol carries descriptors. This additive adapter moves
verified chunks over the same authenticated loopback links and partition gates.
No request/response conveys a filesystem path or grants coding/Git authority.
"""
from __future__ import annotations

import argparse
import base64
import binascii
import json
import os
from pathlib import Path
import re
import threading
from typing import Any

from .peer_payload_store_v1 import PayloadStore
from .peer_runtime_v1 import Peer, TransportError, _Server, _digest, _save, request
from .peer_store_v1 import PROTOCOL, strict_loads

PAYLOAD_KIND = "payload_available"
MAX_OBJECTS = 128
MAX_CHUNKS = 128
CHUNK_BYTES = 65_536
_EXTRA_COUNTERS = ("payload_contacts", "failed_payload_contacts", "offered_chunks", "validated_chunks")


def _inventory(value: Any) -> dict[str, list[int]]:
    if type(value) is not dict or len(value) > MAX_OBJECTS:
        raise TransportError("Invalid payload inventory")
    for sha, indices in value.items():
        if (type(sha) is not str or re.fullmatch(r"[0-9a-f]{64}", sha) is None
                or type(indices) is not list or len(indices) > MAX_CHUNKS
                or any(type(index) is not int or not 0 <= index < MAX_CHUNKS for index in indices)
                or indices != sorted(set(indices))):
            raise TransportError("Invalid payload chunk inventory")
    return value


class PayloadPeer(Peer):
    """Keep bulk data local until each verified chunk has actually arrived."""

    def __init__(self, root: Path, node_id: str, mode: str, token: str, *,
                 interval: float = 0.1, fanout: int = 2):
        super().__init__(root, node_id, mode, token, interval=interval, fanout=fanout)
        try:
            self.payloads = PayloadStore(root, node_id)
            self.payload_turn_lock = threading.Lock()
            self.telemetry_path = root / "payload-telemetry.json"
            self.stats.update({key: 0 for key in _EXTRA_COUNTERS})
            self.durable: dict[str, Any] = {"protocol": "peer-payload-telemetry-v1", "node_id": node_id,
                                          "boots": 0, "payload_round": 0, "counts": self.stats.copy()}
            if self.telemetry_path.exists():
                value = strict_loads(self.telemetry_path.read_bytes(), max_bytes=4096)
                if (type(value) is not dict or set(value) != set(self.durable)
                        or value["protocol"] != self.durable["protocol"] or value["node_id"] != node_id
                        or any(type(value[k]) is not int or not 0 <= value[k] < 2**63 - 1
                               for k in ("boots", "payload_round"))
                        or type(value["counts"]) is not dict or set(value["counts"]) != set(self.stats)
                        or any(type(n) is not int or not 0 <= n < 2**63 - 1 for n in value["counts"].values())):
                    raise TransportError("Corrupt durable payload telemetry")
                self.durable = value
            self.durable["boots"] += 1
            _save(self.telemetry_path, self.durable)
        except BaseException:
            self.owner.close()
            raise

    def _count(self, **values: int) -> None:
        with self.lock:
            super()._count(**values)
            for key, value in values.items():
                self.durable["counts"][key] += value
            _save(self.telemetry_path, self.durable)

    def publish_payload(self, data: bytes, media_type: str, command_id: str) -> dict:
        """Called by the local producer; the observer has no bulk-write RPC."""
        descriptor = self.payloads.put(data, media_type)
        return self.store.publish(command_id, PAYLOAD_KIND, descriptor)

    def arrived_payloads(self) -> dict[str, dict]:
        descriptors = {}
        for event in self.store.state()["events"]:
            if event["kind"] != PAYLOAD_KIND:
                continue
            descriptor = self.store.artifact(event["artifact_sha256"])
            self.payloads.register(descriptor)
            descriptors[descriptor["sha256"]] = descriptor
        return descriptors

    def payload_inventory(self) -> dict[str, list[int]]:
        output = {}
        for sha, descriptor in self.arrived_payloads().items():
            missing = set(self.payloads.missing(sha))
            output[sha] = [i for i in range(len(descriptor["chunk_sha256"])) if i not in missing]
        return _inventory(output)

    def _offer(self, remote: dict[str, list[int]], number: int) -> dict | None:
        local = self.payload_inventory()
        options = [(sha, index) for sha, present in local.items() if sha in remote
                   for index in present if index not in remote[sha]]
        if not options:
            return None
        sha, index = min(options, key=lambda pair: _digest([self.node_id, number, *pair]))
        data = self.payloads.read_chunk(sha, index)
        self._count(offered_chunks=1)
        return {"sha256": sha, "index": index, "data": base64.b64encode(data).decode("ascii")}

    def _accept(self, chunk: Any) -> None:
        if chunk is None:
            return
        if (type(chunk) is not dict or set(chunk) != {"sha256", "index", "data"}
                or type(chunk["data"]) is not str or len(chunk["data"]) > 4 * ((CHUNK_BYTES + 2) // 3)
                or type(chunk["sha256"]) is not str or chunk["sha256"] not in self.arrived_payloads()):
            raise TransportError("Payload bytes have no arrived descriptor")
        try:
            data = base64.b64decode(chunk["data"], validate=True)
        except (ValueError, binascii.Error):
            raise TransportError("Invalid payload encoding") from None
        if base64.b64encode(data).decode("ascii") != chunk["data"]:
            raise TransportError("Noncanonical payload encoding")
        self.payloads.accept_chunk(chunk["sha256"], chunk["index"], data)
        self._count(validated_chunks=1)

    def dispatch(self, body: dict) -> Any:
        if body.get("operation") not in {"payload_exchange", "payload_push", "payload_state"}:
            return super().dispatch(body)
        # The v1 runtime exposes no validation-only hook. Keep the same exact
        # authenticated-body, immutable membership and bidirectional fault gates.
        if (set(body) != {"protocol", "sender", "receiver", "nonce", "operation", "payload"}
                or body["protocol"] != PROTOCOL or body["receiver"] != self.node_id
                or type(body["nonce"]) is not str or re.fullmatch(r"[0-9a-f]{32}", body["nonce"]) is None
                or type(body["payload"]) is not dict):
            raise TransportError("Invalid request schema")
        sender, operation, payload = body["sender"], body["operation"], body["payload"]
        with self.lock:
            peers, blocked = self.settings["peers"].copy(), self.settings["blocked"][:]
        if sender == "driver":
            if operation != "payload_state" or payload:
                raise TransportError("Observer cannot transfer payload chunks")
            descriptors = self.arrived_payloads()
            with self.lock:
                counters = strict_loads(json.dumps(self.durable))
            return {"objects": {sha: {"size": descriptor["size"], "media_type": descriptor["media_type"],
                                      "chunk_count": len(descriptor["chunk_sha256"]),
                                      "complete": self.payloads.has_complete(sha),
                                      "missing": self.payloads.missing(sha)}
                                for sha, descriptor in descriptors.items()}, "telemetry": counters}
        if sender not in peers or sender in blocked or sender == self.node_id:
            raise TransportError("Peer unavailable or outside membership")
        if operation == "payload_exchange" and set(payload) == {"inventory"}:
            remote = _inventory(payload["inventory"])
            with self.lock:
                number = self.durable["payload_round"]
            return {"inventory": self.payload_inventory(), "chunk": self._offer(remote, number)}
        if operation == "payload_push" and set(payload) == {"chunk"} and payload["chunk"] is not None:
            self._accept(payload["chunk"])
            return {"received": True}
        raise TransportError("Invalid payload operation")

    def payload_turn(self) -> dict:
        if not self.payload_turn_lock.acquire(blocking=False):
            return {"busy": True}
        try:
            with self.lock:
                peers = self.settings["peers"].copy()
                brokers, blocked = self.settings["brokers"][:], self.settings["blocked"][:]
                if not peers:
                    return {"contacts": 0}
                number = self.durable["payload_round"]
                self.durable["payload_round"] += 1
                _save(self.telemetry_path, self.durable)
            choices = [p for p in peers if p != self.node_id]
            if self.mode == "broker":
                choices = [p for p in brokers if p != self.node_id]
            else:
                choices.sort(key=lambda p: _digest([self.node_id, number, p]))
            attempted = []
            for peer in choices[:self.fanout]:
                attempted.append(peer)
                self._count(payload_contacts=1)
                try:
                    if peer in blocked:
                        raise TransportError("Outbound link partitioned")
                    answer, _, _ = request(peers[peer], peer, self.token, "payload_exchange",
                                           {"inventory": self.payload_inventory()}, sender=self.node_id,
                                           account=self._count)
                    if type(answer) is not dict or set(answer) != {"inventory", "chunk"}:
                        raise TransportError("Invalid payload exchange")
                    remote = _inventory(answer["inventory"])
                    self._accept(answer["chunk"])
                    offer = self._offer(remote, number)
                    if offer is not None:
                        reply, _, _ = request(peers[peer], peer, self.token, "payload_push",
                                              {"chunk": offer}, sender=self.node_id, account=self._count)
                        if reply != {"received": True}:
                            raise TransportError("Invalid payload acknowledgment")
                    if self.mode == "broker" and self.node_id not in brokers:
                        break
                except (OSError, ValueError, KeyError, TypeError):
                    self._count(failed_payload_contacts=1)
            return {"contacts": len(attempted), "targets": attempted}
        finally:
            self.payload_turn_lock.release()

    def run_ticks(self) -> None:
        while not self.stop.wait(self.interval):
            with self.lock:
                enabled = self.settings["enabled"]
            if enabled:
                try:
                    self.turn()
                    self.payload_turn()
                except Exception as error:
                    with self.lock:
                        self.fatal_error = type(error).__name__
                    self.stop.set()


class PayloadServer(_Server):
    daemon_threads = False
    block_on_close = True


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    config = strict_loads(args.config.read_bytes(), max_bytes=32_000)
    if (type(config) is not dict or set(config) != {"root", "node_id", "mode", "token", "port", "interval", "fanout"}
            or type(config["port"]) is not int or not 0 <= config["port"] <= 65535):
        raise ValueError("Invalid payload process configuration")
    peer = PayloadPeer(Path(config["root"]), config["node_id"], config["mode"], config["token"],
                       interval=config["interval"], fanout=config["fanout"])
    try:
        with PayloadServer(config["port"], peer) as server:
            thread = threading.Thread(target=peer.run_ticks, name="payload-and-evidence", daemon=True)
            thread.start()
            print(json.dumps({"protocol": "peer-payload-v1", "node_id": peer.node_id,
                              "pid": os.getpid(), "port": server.server_address[1]}), flush=True)
            try:
                server.serve_forever(poll_interval=0.1)
            finally:
                peer.stop.set()
                thread.join()
    finally:
        peer.owner.close()


if __name__ == "__main__":
    main()
