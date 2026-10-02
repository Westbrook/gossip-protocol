"""Offline actual-Responses-adapter coding, local payload gates and restarts."""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import select
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import uuid

from gossip_harness.peer_authority_v1 import authority_request
from gossip_harness.peer_coding_dispatch_v1 import PayloadUnavailable, canonical_payload
from gossip_harness.peer_coding_runtime_v1 import CodingPeer, MeshPayloads, PROTOCOL, _digest, fixture_workers
from gossip_harness.peer_payload_runtime_v1 import PayloadPeer
from gossip_harness.peer_runtime_v1 import TransportError, request
from gossip_harness.worker import MODEL, WorkerFailure, WorkerRequest

ROOT = Path(__file__).resolve().parents[1]
TOKEN = "mesh-offline-fixture-credential-0001"
KEY = "principal-offline-fixture-key-00001"


def seed_request(large=False):
    return {"task_id": "coding-slot", "instructions": "Change answer to return 42.",
            "allowed_paths": ["app.py"], "files": {"app.py": "def answer():\n    return 0\n" + ("# context\n" * 8000 if large else "")},
            "base_sha": "a" * 40, "attempt": 1, "feedback": ""}


def response_fixture(kind="result"):
    text = {"changes": [{"path": "app.py", "content": "def answer():\n    return 42\n"}],
            "summary": "Offline fixture proposes the requested change."}
    return {"id": "offline_response_1", "status": "completed", "model": MODEL, "service_tier": "default",
            "usage": {"input_tokens": 10, "output_tokens": 10},
            "output": [] if kind == "failure" else [{"type": "message", "role": "assistant", "status": "completed",
                                                       "content": [{"type": "output_text", "text": json.dumps(text)}]}]}


def policy(seed):
    return {"run_id": "coding-mesh-test", "task_id": "coding-slot", "profile_id": "fixture-mini",
            "seed_producer": "origin", "seed_sha256": _digest(seed), "service_producer": "service",
            "authority_sha256": "a" * 64, "dispatch_config_sha256": "b" * 64}


def claim_reply(config):
    return {"run_id": config["run_id"], "config_sha256": config["authority_sha256"], "status": "ok",
            "authority_time": 1.0, "lease": {"task_id": config["task_id"], "worker_id": "worker", "epoch": 1, "expires_at": 31.0}}


def complete_reply(peer, result):
    intent = peer.record["intent"]
    return {"run_id": intent["run_id"], "config_sha256": peer.coding_config["authority_sha256"],
            "dispatch_config_sha256": peer.coding_config["dispatch_config_sha256"], "status": "completed",
            "dispatch_protocol": "peer-coding-dispatch-v1", "principal": "worker",
            "request_id": intent["action_id"] + ":coding_dispatch",
            "task_id": intent["task_id"], "profile_id": intent["profile_id"], "action_id": intent["action_id"],
            "epoch": 1, "payload_sha256": intent["request_sha256"], "worker_request_sha256": intent["worker_request_sha256"],
            "profile_sha256": "c" * 64, "reserved_units": 100, "usage_units": 1,
            "reservation_id": "reserved", "call_id": "call", "result_sha256": _digest(result)}


class CodingRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.peers = []
        self.seed = seed_request()
        self.config = policy(self.seed)
        self.calls = []

    def tearDown(self):
        for peer in self.peers:
            peer.owner.close()
        self.directory.cleanup()

    def peer(self, node="worker", call=None):
        if node == "worker":
            def default(port, principal, key, request_id, operation, payload, **kwargs):
                self.calls.append((request_id, operation, payload))
                return claim_reply(self.config) if operation == "claim" else {
                    "run_id": self.config["run_id"], "config_sha256": self.config["authority_sha256"],
                    "dispatch_config_sha256": self.config["dispatch_config_sha256"], "status": "waiting"}
            peer = CodingPeer(self.root / node, node, "gossip", TOKEN, coding=self.config,
                              authority={"port": 12345, "principal": node, "key": KEY}, authority_call=call or default)
        else:
            peer = PayloadPeer(self.root / node, node, "gossip", TOKEN)
        self.peers.append(peer)
        return peer

    def test_closed_offline_fixture_matches_real_worker_success_and_known_failure_contract(self):
        request = WorkerRequest(**{**self.seed, "allowed_paths": tuple(self.seed["allowed_paths"])})
        for kind in ("result", "failure"):
            with self.subTest(kind=kind):
                workers, _ = fixture_workers({"fixture-mini": {"mode": "offline-response-fixture", "model": MODEL,
                    "max_output_tokens": 1024, "response": response_fixture(kind)}})
                worker = workers["fixture-mini"]
                if kind == "result":
                    result = worker.run(request)
                    self.assertEqual(result.changes, {"app.py": "def answer():\n    return 42\n"})
                    self.assertGreater(result.usage_units, 0)
                    self.assertEqual(result.metadata["model"], MODEL)
                else:
                    with self.assertRaises(WorkerFailure) as failure:
                        worker.run(request)
                    self.assertIs(type(failure.exception.usage_units), int)
                    self.assertGreater(failure.exception.usage_units, 0)
                    self.assertEqual(failure.exception.metadata["model"], MODEL)

    @staticmethod
    def transfer(sender, receiver, sha, *, chunks=True):
        receiver.store.merge(sender.store.export([], limit=8))
        receiver.arrived_payloads()
        if chunks:
            for index in receiver.payloads.missing(sha):
                receiver.payloads.accept_chunk(sha, index, sender.payloads.read_chunk(sha, index))

    def test_no_request_from_global_bytes_or_descriptor_without_arrived_chunks(self):
        worker, origin = self.peer(), self.peer("origin")
        worker.payloads.put(canonical_payload(self.seed), "application/json")
        worker.coding_tick()
        self.assertIsNone(worker.record)
        with patch("gossip_harness.peer_coding_runtime_v1._save", side_effect=OSError("injected journal failure")):
            origin.publish_payload(canonical_payload(self.seed), "application/json", "provided-task")
            self.transfer(origin, worker, self.config["seed_sha256"], chunks=False)
            worker.coding_tick()
        self.assertIsNone(worker.record)
        self.assertEqual(self.calls, [])
        origin.publish_payload(canonical_payload(self.seed), "application/json", "provided-task")
        self.transfer(origin, worker, self.config["seed_sha256"], chunks=False)
        # Bytes placed directly in the local store count only after the matching
        # configured producer's descriptor has actually arrived.
        worker.coding_tick()
        self.assertEqual(worker.record["phase"], "dispatch_pending")
        self.assertEqual(worker.record["intent"]["source_kind"], "provided_worker_request")
        other = self.peer("other")
        self.transfer(origin, other, self.config["seed_sha256"], chunks=False)
        self.assertFalse(other.payloads.has_complete(self.config["seed_sha256"]))
        self.assertEqual(len(self.calls), 2)

    def test_authority_payload_adapter_requires_request_producer_and_complete_bytes(self):
        origin, service = self.peer("origin"), self.peer("service")
        sha = self.config["seed_sha256"]
        origin.publish_payload(canonical_payload(self.seed), "application/json", "provided-task")
        adapter = MeshPayloads(service)
        with self.assertRaises(PayloadUnavailable):
            adapter.read_owned("origin", sha)
        self.transfer(origin, service, sha, chunks=False)
        with self.assertRaises(PayloadUnavailable):
            adapter.read_owned("origin", sha)
        self.transfer(origin, service, sha)
        with self.assertRaises(PayloadUnavailable):
            adapter.read_owned("worker", sha)
        self.assertEqual(adapter.read_owned("origin", sha), canonical_payload(self.seed))

    def test_journal_rejects_boolean_epoch_and_changed_capability_on_reopen(self):
        worker, origin = self.peer(), self.peer("origin")
        origin.publish_payload(canonical_payload(self.seed), "application/json", "provided-task")
        self.transfer(origin, worker, self.config["seed_sha256"])
        worker.coding_tick()
        path = worker.coding_path
        worker.owner.close()
        self.peers.remove(worker)
        with self.assertRaisesRegex(ValueError, "identity"):
            CodingPeer(worker.root, "worker", "gossip", TOKEN, coding=self.config,
                       authority={"port": 12345, "principal": "worker", "key": "x" * 32})
        saved = json.loads(path.read_text())
        saved["data"]["record"]["claim"]["lease"]["epoch"] = True
        saved["sha256"] = _digest(saved["data"])
        path.write_bytes(canonical_payload(saved))
        with self.assertRaisesRegex(ValueError, "claim"):
            self.peer()

    def test_observer_has_no_action_rpc_and_state_does_not_execute(self):
        worker = self.peer()
        body = {"protocol": "gossip-peer-v1", "sender": "driver", "receiver": "worker", "nonce": "1" * 32,
                "operation": "state", "payload": {}}
        self.assertIsNone(worker.dispatch(body)["coding"])
        for op in ("run_action", "coding_dispatch", "coding_tick", "payload_push"):
            with self.assertRaises(TransportError):
                worker.dispatch({**body, "operation": op})
        self.assertEqual(self.calls, [])

    def test_completed_receipt_requires_service_notice_and_actual_result_bytes(self):
        result = {"kind": "result", "payload": {"changes": {"app.py": "return 42"}, "summary": "offline",
                                                  "usage_units": 1, "metadata": {}}}
        def call(port, principal, key, request_id, operation, payload, **kwargs):
            return claim_reply(self.config) if operation == "claim" else complete_reply(worker, result)
        worker, origin, service = self.peer(call=call), self.peer("origin"), self.peer("service")
        origin.publish_payload(canonical_payload(self.seed), "application/json", "provided-task")
        self.transfer(origin, worker, self.config["seed_sha256"])
        worker.coding_tick()
        self.assertEqual(worker.record["phase"], "receipt")
        sha = MeshPayloads(service).put_owned("worker", canonical_payload(result))
        self.transfer(service, worker, sha, chunks=False)
        worker.coding_tick()
        self.assertEqual(worker.record["phase"], "receipt")
        self.transfer(service, worker, sha)
        worker.coding_tick()
        self.assertEqual(worker.record["phase"], "published")
        events = [e for e in worker.store.state()["events"] if e["kind"] == "coding_result"]
        worker.coding_tick()
        self.assertEqual(len(events), 1)
        self.assertEqual(events, [e for e in worker.store.state()["events"] if e["kind"] == "coding_result"])


class CodingCluster:
    def __init__(self, mode="gossip", *, worker_crash=None, response_kind="result"):
        self.root = ROOT / "runs" / "peer-coding-fixtures" / uuid.uuid4().hex
        self.root.mkdir(parents=True)
        self.mode, self.token = mode, uuid.uuid4().hex
        self.key, self.seed = uuid.uuid4().hex, seed_request(large=True)
        self.names = ["service", "origin", "worker"] + (["relay-a", "relay-b"] if mode == "broker" else [])
        self.ports, self.processes, self.logs, self.history, self.configs = {}, {}, [], [], {}
        self.authority_port = 0
        self.authority_config = {"protocol": "peer-authority-v1", "run_id": "coding-process-fixture", "budget_units": 1_000_000,
                                 "lease_seconds": 120, "principals": {"worker": {"key": self.key, "tasks": ["coding-slot"]}},
                                 "tasks": {"coding-slot": {"fixture": "text_build", "reservation_units": 0}}}
        self.profiles = {"fixture-mini": {"mode": "offline-response-fixture", "model": MODEL,
                                          "max_output_tokens": 1024, "response": response_fixture(response_kind)}}
        try:
            ready = self.start("service")
            self.coding = {**policy(self.seed), "run_id": self.authority_config["run_id"],
                           "authority_sha256": ready["config_sha256"], "dispatch_config_sha256": ready["dispatch_config_sha256"]}
            for name in self.names[1:]:
                self.start(name, crash=worker_crash if name == "worker" else None)
            self.enable(False)
        except BaseException:
            self.close()
            raise

    def start(self, name, *, crash=None, request_id=None):
        directory = self.root / name
        directory.mkdir(exist_ok=True)
        config = {"root": str(directory), "node_id": name, "mode": self.mode, "token": self.token,
                  "port": self.ports.get(name, 0), "interval": 0.05, "fanout": 2,
                  "role": "service" if name == "service" else "worker" if name == "worker" else "peer"}
        if name == "service":
            config.update(authority=self.authority_config, authority_port=self.authority_port, profiles=self.profiles,
                          task_specs={"coding-slot": {"allowed_paths": ["app.py"], "profiles": ["fixture-mini"]}})
        elif name == "worker":
            config.update(coding=self.coding, authority={"port": self.authority_port, "principal": "worker", "key": self.key})
        else:
            config["seed"] = self.seed if name == "origin" else None
        path = directory / ("config-" + uuid.uuid4().hex + ".json")
        path.write_bytes(canonical_payload(config))
        self.configs[name] = config
        log = (directory / ("stderr-" + uuid.uuid4().hex + ".log")).open("w")
        self.logs.append(log)
        extra = ["--crash-point", crash] if crash else []
        if request_id:
            extra += ["--crash-request-id", request_id]
        env = {"PATH": os.defpath, "PYTHONPATH": str(ROOT), "PYTHONHASHSEED": "0", "PYTHONDONTWRITEBYTECODE": "1", "LANG": "C.UTF-8"}
        process = subprocess.Popen([sys.executable, "-m", "gossip_harness.peer_coding_runtime_v1", "--config", str(path), *extra],
                                   cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=log, text=True)
        self.processes[name] = process
        self.history.append({"name": name, "pid": process.pid, "process": process, "config": str(path.relative_to(self.root)),
                             "stderr": str(Path(log.name).relative_to(self.root))})
        ready, _, _ = select.select([process.stdout], [], [], 10)
        if not ready:
            raise AssertionError(f"Coding readiness timeout: {name}; retained {directory}")
        line = process.stdout.readline()
        if not line:
            raise AssertionError(f"Coding process exited before readiness: {name}; retained {directory}")
        result = json.loads(line)
        assert result["pid"] == process.pid
        if name in self.ports:
            assert result["port"] == self.ports[name]
        self.ports[name] = result["port"]
        if name == "service":
            if self.authority_port:
                assert self.authority_port == result["authority_port"]
            self.authority_port = result["authority_port"]
        return result

    def call(self, name, operation, payload=None):
        return request(self.ports[name], name, self.token, operation, payload)[0]

    def enable(self, enabled):
        peers = {name: self.ports[name] for name in self.names}
        for name in self.names:
            self.call(name, "configure", {"peers": peers, "brokers": ["relay-a", "relay-b"] if self.mode == "broker" else [],
                                           "enabled": enabled})

    def block(self, name, peers):
        return self.call(name, "block", {"peers": peers})

    def record(self):
        return self.call("worker", "state")["coding"]

    def phase(self, phase):
        record = self.record()
        return record if record and record["phase"] == phase else None

    def wait(self, predicate, timeout=20):
        deadline, wake, last = time.monotonic() + timeout, threading.Event(), None
        while time.monotonic() < deadline:
            try:
                last = predicate()
                if last:
                    return last
            except (OSError, ValueError) as error:
                last = type(error).__name__
            wake.wait(min(0.03, max(0, deadline - time.monotonic())))
        raise AssertionError(f"Coding predicate timeout ({last}); retained {self.root}")

    def kill(self, name):
        process = self.processes[name]
        if process.poll() is None:
            process.kill()
        process.wait(timeout=5)
        if process.stdout:
            process.stdout.close()

    def close(self):
        for name in self.processes:
            self.kill(name)
        for log in self.logs:
            log.close()
        (self.root / "lifecycle.json").write_text(json.dumps({"protocol": PROTOCOL, "mode": self.mode,
            "provider_mode": "offline-response-fixture", "real_provider_requests": 0,
            "incarnations": [{**{k: v for k, v in row.items() if k != "process"}, "returncode": row["process"].returncode}
                             for row in self.history]}, indent=2))

    def snapshot(self):
        with sqlite3.connect(f"file:{self.root / 'service' / 'authority' / 'authority.sqlite'}?mode=ro", uri=True) as db:
            return {"actions": db.execute("SELECT action_id,state,payload_sha,call_id FROM coding_actions_v1").fetchall(),
                    "requests": db.execute("SELECT request_id FROM authority_requests WHERE principal='worker'").fetchall(),
                    "reservations": db.execute("SELECT amount,spent,state FROM reservations").fetchall()}

    def frozen_record(self):
        return json.loads((self.root / "worker" / "coding-state.json").read_text())["data"]["record"]

    def transport_entries(self):
        path = self.root / "service" / "offline-transport.jsonl"
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


@contextmanager
def coding_cluster(**kwargs):
    cluster = CodingCluster(**kwargs)
    try:
        yield cluster
    finally:
        cluster.close()


class CodingRuntimeProcessTests(unittest.TestCase):
    def test_actual_mesh_modes_preserve_source_partition_and_deliver_patch_result(self):
        for mode in ("gossip", "broker"):
            with self.subTest(mode=mode), coding_cluster(mode=mode) as cluster:
                cluster.block("origin", [n for n in cluster.names if n != "origin"])
                cluster.enable(True)
                cluster.wait(lambda: cluster.call("worker", "payload_state")["telemetry"]["payload_round"] >= 3)
                self.assertIsNone(cluster.record())
                self.assertEqual(cluster.snapshot()["actions"], [])
                self.assertEqual(cluster.transport_entries(), [])
                cluster.block("origin", [])
                record = cluster.wait(lambda: cluster.phase("published"))
                self.assertEqual(record["intent"]["source_kind"], "provided_worker_request")
                self.assertEqual(record["response"]["payload_sha256"], record["intent"]["request_sha256"])
                self.assertEqual(len(cluster.snapshot()["actions"]), 1)
                self.assertEqual(len(cluster.transport_entries()), 1)
                self.assertEqual(cluster.snapshot()["reservations"][0][2], "settled")
                local = cluster.call("worker", "payload_state")
                self.assertTrue(local["objects"][record["response"]["result_sha256"]]["complete"])
                self.assertGreater(local["telemetry"]["counts"]["validated_chunks"], 2)
                with self.assertRaises(TransportError):
                    cluster.call("worker", "run_action")

    def test_worker_process_crashes_reconcile_exact_action_and_single_result_event(self):
        for point in ("after_claim", "after_receipt", "after_publish"):
            with self.subTest(point=point), coding_cluster(worker_crash=point) as cluster:
                cluster.enable(True)
                cluster.wait(lambda: cluster.processes["worker"].poll() is not None)
                frozen = cluster.frozen_record()
                old_pid = cluster.processes["worker"].pid
                cluster.kill("worker")
                ready = cluster.start("worker")
                self.assertNotEqual(old_pid, ready["pid"])
                record = cluster.wait(lambda: cluster.phase("published"))
                self.assertEqual(record["intent"], frozen["intent"])
                self.assertEqual(len(cluster.snapshot()["actions"]), 1)
                self.assertEqual(len(cluster.transport_entries()), 1)
                events = cluster.call("worker", "state")["events"]
                self.assertEqual(len([e for e in events if e["producer"] == "worker" and e["kind"] == "coding_result"]), 1)

    def test_service_process_recovers_fsynced_worker_result_without_second_action(self):
        with coding_cluster(worker_crash="after_claim") as cluster:
            cluster.enable(True)
            cluster.wait(lambda: cluster.processes["worker"].poll() is not None)
            frozen = cluster.frozen_record()
            dispatch_id = frozen["intent"]["action_id"] + ":coding_dispatch"
            cluster.kill("service")
            cluster.start("service", crash="after_result_persisted", request_id=dispatch_id)
            cluster.kill("worker")
            cluster.start("worker")
            cluster.wait(lambda: cluster.processes["service"].poll() is not None)
            cluster.kill("service")
            cluster.start("service")
            record = cluster.wait(lambda: cluster.phase("published"))
            self.assertEqual(record["intent"], frozen["intent"])
            snapshot = cluster.snapshot()
            self.assertEqual(len(snapshot["actions"]), 1)
            self.assertEqual(len(cluster.transport_entries()), 1)
            self.assertEqual(snapshot["actions"][0][1], "completed")
            self.assertEqual(len(snapshot["reservations"]), 1)
            self.assertEqual(snapshot["reservations"][0][2], "settled")
            recovered = authority_request(cluster.authority_port, "worker", cluster.key, "lookup-after-restart",
                                          "lookup", {"request_id": dispatch_id}, run_id=cluster.coding["run_id"])
            self.assertEqual(recovered, record["response"])

    def test_authority_request_ahead_of_payload_is_not_cached_or_reserved(self):
        with coding_cluster() as cluster:
            cluster.block("service", [n for n in cluster.names if n != "service"])
            cluster.enable(True)
            pending = cluster.wait(lambda: cluster.phase("dispatch_pending"))
            snapshot = cluster.snapshot()
            self.assertEqual(snapshot["actions"], [])
            self.assertEqual(cluster.transport_entries(), [])
            self.assertEqual(snapshot["reservations"], [])
            self.assertEqual(snapshot["requests"], [(pending["intent"]["action_id"] + ":claim",)])
            cluster.block("service", [])
            completed = cluster.wait(lambda: cluster.phase("published"))
            self.assertEqual(completed["intent"], pending["intent"])
            self.assertEqual(len(cluster.snapshot()["actions"]), 1)
            self.assertEqual(len(cluster.transport_entries()), 1)

    def test_known_failure_and_unknown_restart_are_terminal_with_conservative_accounting(self):
        with coding_cluster(response_kind="failure") as cluster:
            cluster.enable(True)
            record = cluster.wait(lambda: cluster.phase("failed"))
            self.assertGreater(record["response"]["usage_units"], 0)
            self.assertEqual(len(cluster.transport_entries()), 1)
            self.assertEqual(cluster.snapshot()["reservations"][0][2], "settled")
            self.assertFalse(any(e["kind"] == "coding_result" for e in cluster.call("worker", "state")["events"]))
        with coding_cluster(worker_crash="after_claim") as cluster:
            cluster.enable(True)
            cluster.wait(lambda: cluster.processes["worker"].poll() is not None)
            frozen = cluster.frozen_record()
            cluster.kill("service")
            cluster.start("service", crash="before_invoke", request_id=frozen["intent"]["action_id"] + ":coding_dispatch")
            cluster.kill("worker")
            cluster.start("worker")
            cluster.wait(lambda: cluster.processes["service"].poll() is not None)
            cluster.kill("service")
            cluster.start("service")
            cluster.wait(lambda: cluster.phase("unknown"))
            before = cluster.snapshot()
            self.assertEqual(cluster.transport_entries(), [])
            self.assertEqual(before["reservations"][0][2], "reserved")
            cluster.kill("worker")
            cluster.start("worker")
            first_round = cluster.call("worker", "payload_state")["telemetry"]["payload_round"]
            cluster.wait(lambda: cluster.call("worker", "payload_state")["telemetry"]["payload_round"] >= first_round + 3)
            self.assertEqual(cluster.phase("unknown")["intent"], frozen["intent"])
            self.assertEqual(cluster.snapshot(), before)
            self.assertEqual(cluster.transport_entries(), [])
            self.assertFalse(any(e["kind"] == "coding_result" for e in cluster.call("worker", "state")["events"]))


if __name__ == "__main__":
    unittest.main()
