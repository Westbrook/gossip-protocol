"""Peer-local fixture decisions over real transport and a narrow authority.

This is a zero-provider build/review qualification path. Neither role can run
candidate code or authorize Git publication, spending changes or completion.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import threading
from typing import Any, Callable

from .peer_authority_v1 import AuthorityError, authority_request
from .peer_runtime_v1 import Peer, _Server, _save
from .peer_store_v1 import canonical_bytes, strict_loads
from .peer_work_v1 import WorkJournal, plan_action

CRASH_POINTS = {"after_claim", "after_result", "after_publish"}


class _WorkServer(_Server):
    # A graceful shutdown must quiesce handlers before releasing node ownership.
    daemon_threads = False
    block_on_close = True


class WorkPeer(Peer):
    """Own one decision loop and consume only this peer's immutable local view."""

    def __init__(self, root: Path, node_id: str, mode: str, token: str, *,
                 work: dict[str, Any], authority: dict[str, Any], interval: float = 0.1,
                 fanout: int = 2, crash_point: str | None = None,
                 authority_call: Callable[..., dict[str, Any]] = authority_request):
        if (type(authority) is not dict or set(authority) != {"port", "principal", "key", "config_sha256"}
                or type(authority["port"]) is not int or not 1 <= authority["port"] <= 65535
                or authority["principal"] != node_id or type(authority["key"]) is not str
                or not 16 <= len(authority["key"]) <= 128
                or type(authority["config_sha256"]) is not str
                or re.fullmatch(r"[0-9a-f]{64}", authority["config_sha256"]) is None
                or type(work) is not dict or work.get("authority_sha256") != authority["config_sha256"]):
            raise ValueError("Invalid authority endpoint or caller identity")
        if crash_point is not None and crash_point not in CRASH_POINTS:
            raise ValueError("Unknown work crash point")
        super().__init__(root, node_id, mode, token, interval=interval, fanout=fanout)
        self.work_config = strict_loads(canonical_bytes(work))
        self.authority_config = authority.copy()
        self.authority_call = authority_call
        self.work = WorkJournal(root / "work.sqlite", node_id, self.work_config)
        self.work_lock = threading.Lock()
        self.work_crash_point = crash_point
        self.work_error: str | None = None
        self.telemetry_path = root / "work-telemetry.json"
        self.telemetry: dict[str, Any] = {"protocol": "peer-work-telemetry-v1", "boots": 0,
                          "rpc_attempts": 0, "rpc_replies": 0, "transport_errors": 0,
                          "pending_transport_error": False, "last_error": None, "last_operation": None}
        if self.telemetry_path.exists():
            saved = strict_loads(self.telemetry_path.read_bytes(), max_bytes=2048)
            if (type(saved) is not dict or set(saved) != set(self.telemetry)
                    or saved["protocol"] != self.telemetry["protocol"]
                    or any(type(saved[k]) is not int or not 0 <= saved[k] < 2**63 - 1
                           for k in ("boots", "rpc_attempts", "rpc_replies", "transport_errors"))
                    or saved["rpc_replies"] + saved["transport_errors"] > saved["rpc_attempts"]
                    or type(saved["pending_transport_error"]) is not bool
                    or saved["last_operation"] not in {None, "claim", "dispatch"}
                    or saved["last_error"] is not None and (type(saved["last_error"]) is not str
                        or re.fullmatch(r"[A-Za-z]{1,64}", saved["last_error"]) is None)):
                raise ValueError("Corrupt durable work telemetry")
            self.telemetry = saved
        self.telemetry["boots"] += 1
        _save(self.telemetry_path, self.telemetry)
        # Capability identity is durable, but plaintext credentials never enter
        # action/evidence records. Endpoint may change after an explicit restart.
        identity = hashlib.sha256(canonical_bytes({"principal": authority["principal"],
                                                  "key": authority["key"],
                                                  "config_sha256": authority["config_sha256"]})).hexdigest()
        path = root / "work-authority.sha256"
        if path.exists():
            if path.read_text() != identity:
                raise ValueError("Authority capability changed for a retained worker")
        else:
            with path.open("x") as stream:
                stream.write(identity)
                stream.flush()
                os.fsync(stream.fileno())
            descriptor = os.open(root, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)

    def _work_crash(self, point: str) -> None:
        if point == self.work_crash_point:
            os._exit({"after_claim": 80, "after_result": 81, "after_publish": 82}[point])

    def _authority(self, request_id: str, operation: str, payload: dict[str, Any]) -> dict[str, Any]:
        endpoint = self.authority_config
        with self.lock:
            self.telemetry["rpc_attempts"] += 1
            self.telemetry["last_operation"] = operation
            _save(self.telemetry_path, self.telemetry)
        try:
            result = self.authority_call(endpoint["port"], endpoint["principal"], endpoint["key"],
                                         request_id, operation, payload, run_id=self.work_config["run_id"])
        except (OSError, TimeoutError, AuthorityError) as error:
            with self.lock:
                self.telemetry["transport_errors"] += 1
                self.telemetry["last_error"] = type(error).__name__
                self.telemetry["pending_transport_error"] = True
                _save(self.telemetry_path, self.telemetry)
            raise
        if (result.get("run_id") != self.work_config["run_id"]
                or result.get("config_sha256") != endpoint["config_sha256"]):
            raise ValueError("Authority response has a different run or configuration")
        with self.lock:
            self.telemetry["rpc_replies"] += 1
            self.telemetry["pending_transport_error"] = False
            _save(self.telemetry_path, self.telemetry)
        return result

    def dispatch(self, body: dict) -> Any:
        result = super().dispatch(body)
        if body.get("sender") == "driver" and body.get("operation") == "state":
            # State observation does not execute a work step. Only the local
            # timer chooses an action from evidence that has arrived here.
            result["work"] = self.work.read()
            result["work_error"] = self.work_error
            with self.lock:
                result["work_telemetry"] = self.telemetry.copy()
        return result

    def work_tick(self) -> None:
        if not self.work_lock.acquire(blocking=False):
            return
        try:
            record = self.work.read()
            if record is None:
                intent = plan_action(self.store, self.work_config)
                if intent is None:
                    return
                record = self.work.prepare(intent)
            phase, intent = record["phase"], record["intent"]
            action_id = intent["action_id"]
            if phase in {"published", "denied", "unknown"}:
                return
            if phase == "prepared":
                reply = self._authority(action_id + ":claim", "claim", {"task_id": intent["task_id"]})
                if reply.get("status") == "rejected":
                    self.work.record(action_id, "denied", response=reply)
                    return
                if reply.get("status") != "ok" or type(reply.get("lease")) is not dict:
                    raise ValueError("Invalid claim response")
                lease = reply["lease"]
                if (lease.get("worker_id") != self.node_id or lease.get("task_id") != intent["task_id"]
                        or type(lease.get("epoch")) is not int or lease["epoch"] < 1):
                    raise ValueError("Claim does not bind the local action")
                record = self.work.record(action_id, "claimed", claim=reply)
                self._work_crash("after_claim")
                phase = record["phase"]
            if phase == "claimed":
                record = self.work.record(action_id, "dispatch_pending")
                phase = record["phase"]
            if phase == "dispatch_pending":
                # Repeating this exact authority command is reconciliation, not
                # permission to replay a provider. Unknown intents stay reserved.
                lease = record["claim"]["lease"]
                reply = self._authority(action_id + ":dispatch", "dispatch", {
                    "task_id": intent["task_id"], "epoch": lease["epoch"],
                    "action_id": action_id, "request_sha256": intent["request_sha256"],
                    "request": intent["request"]})
                status = reply.get("status")
                if status == "rejected":
                    self.work.record(action_id, "denied", response=reply)
                    return
                if status not in {"dispatching", "unknown", "completed"}:
                    raise ValueError("Invalid dispatch status")
                if (reply.get("task_id") != intent["task_id"] or reply.get("epoch") != lease["epoch"]
                        or reply.get("action_id") != action_id
                        or reply.get("request_sha256") != intent["request_sha256"]
                        or type(reply.get("reservation_id")) is not str):
                    raise ValueError("Dispatch does not bind the local action")
                if status == "dispatching":
                    return
                if status == "unknown":
                    self.work.record(action_id, "unknown", response=reply)
                    return
                if type(reply.get("result")) is not dict or type(reply.get("usage_units")) is not int or reply["usage_units"] != 0:
                    raise ValueError("Only completed zero-provider fixture responses are accepted")
                record = self.work.record(action_id, "result", response=reply, result=reply["result"])
                self._work_crash("after_result")
                phase = record["phase"]
            if phase == "result":
                content = {"protocol": "peer-work-v1", "run_id": intent["run_id"], "generation": 0,
                           "role": intent["role"], "intent": intent, "result": record["result"],
                           "authority": record["response"]}
                record = self.work.set_outbox(action_id, content)
                event = self.store.publish(record["outbox_command_id"], "work_result", record["outbox"])
                self._work_crash("after_publish")
                self.work.mark_published(action_id, event["event_id"])
        except (OSError, TimeoutError, AuthorityError):
            # Transport failure proves neither commit nor rollback. The durable
            # phase retains exact bytes/IDs for the next reconciliation attempt.
            pass
        finally:
            self.work_lock.release()

    def run_ticks(self) -> None:
        while not self.stop.wait(self.interval):
            with self.lock:
                enabled = self.settings["enabled"]
            if enabled:
                try:
                    self.turn()
                    self.work_tick()
                except Exception as error:
                    with self.lock:
                        self.work_error = type(error).__name__
                        self.fatal_error = type(error).__name__
                    self.stop.set()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--crash-point", choices=sorted(CRASH_POINTS))
    args = parser.parse_args()
    config = strict_loads(args.config.read_bytes(), max_bytes=32_000)
    required = {"root", "node_id", "mode", "token", "port", "interval", "fanout", "work", "authority"}
    if type(config) is not dict or set(config) != required:
        raise ValueError("Invalid work process configuration")
    if type(config["port"]) is not int or not 0 <= config["port"] <= 65535:
        raise ValueError("Invalid work port")
    peer = WorkPeer(Path(config["root"]), config["node_id"], config["mode"], config["token"],
                    work=config["work"], authority=config["authority"], interval=config["interval"],
                    fanout=config["fanout"], crash_point=args.crash_point)
    try:
        with _WorkServer(config["port"], peer) as server:
            thread = threading.Thread(target=peer.run_ticks, name="local-work-and-anti-entropy", daemon=True)
            thread.start()
            print(json.dumps({"protocol": "peer-work-v1", "node_id": peer.node_id,
                              "pid": os.getpid(), "port": server.server_address[1]}), flush=True)
            try:
                server.serve_forever(poll_interval=0.1)
            finally:
                peer.stop.set()
                # RPC I/O and SQLite waits are bounded. Retain the owner lock
                # until this loop has actually stopped; timeout is not quiescence.
                thread.join()
    finally:
        peer.owner.close()


if __name__ == "__main__":
    main()
