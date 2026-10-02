"""Actual local-process/TCP checks. No model, Docker or candidate execution.

Process fixtures and stderr are retained under runs/peer-runtime-fixtures. Tests
wait for readiness or state predicates with deadlines, never fixed settle sleeps.
"""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import select
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid

from gossip_harness.peer_runtime_v1 import MAX_FRAME, Peer, TransportError, _signed, _verified, receive, request, send
from gossip_harness.peer_store_v1 import MAX_ARTIFACT_DEPTH, MAX_BATCH, PROTOCOL, canonical_bytes

ROOT = Path(__file__).resolve().parents[1]


class Cluster:
    def __init__(self, names, mode="gossip", brokers=()):
        self.root = ROOT / "runs" / "peer-runtime-fixtures" / uuid.uuid4().hex
        self.root.mkdir(parents=True)
        self.names, self.mode, self.brokers = names, mode, list(brokers)
        self.token = uuid.uuid4().hex
        self.processes, self.ports, self.logs, self.history = {}, {}, [], []
        self.started = time.time()
        try:
            for name in names:
                self.start(name)
            self.enable(False)
        except BaseException:
            self.close()
            raise

    def start(self, name):
        node = self.root / name
        node.mkdir(exist_ok=True)
        config = dict(root=str(node), node_id=name, mode=self.mode, token=self.token,
                      port=self.ports.get(name, 0), interval=0.05, fanout=2)
        path = node / "process-config.json"
        path.write_text(json.dumps(config))
        log = (node / ("stderr-" + uuid.uuid4().hex + ".log")).open("w")
        self.logs.append(log)
        env = {"PATH": os.defpath, "PYTHONPATH": str(ROOT), "PYTHONHASHSEED": "0",
               "PYTHONDONTWRITEBYTECODE": "1", "LANG": "C.UTF-8"}
        process = subprocess.Popen([sys.executable, "-m", "gossip_harness.peer_runtime_v1",
                                    "--config", str(path)], cwd=ROOT, env=env,
                                   stdout=subprocess.PIPE, stderr=log, text=True)
        self.processes[name] = process
        incarnation = {"node_id": name, "pid": process.pid, "started_at_unix": time.time(),
                       "stderr_path": str(Path(log.name).relative_to(self.root)),
                       "process": process}
        self.history.append(incarnation)
        ready, _, _ = select.select([process.stdout], [], [], 10)
        if not ready:
            raise AssertionError(f"Node readiness deadline: {name}; retained {node}")
        line = process.stdout.readline()
        if not line:
            raise AssertionError(f"Node exited before readiness: {name}; retained {node}")
        result = json.loads(line)
        assert result["node_id"] == name and result["pid"] == process.pid
        if name in self.ports:
            assert result["port"] == self.ports[name]
        self.ports[name] = result["port"]
        incarnation["ready_port"] = result["port"]
        return process.pid

    def call(self, name, operation, payload=None, **kwargs):
        return request(self.ports[name], name, self.token, operation, payload, **kwargs)[0]

    def enable(self, enabled):
        for name in self.names:
            if self.processes[name].poll() is None:
                self.call(name, "configure", {"peers": self.ports, "brokers": self.brokers,
                                               "enabled": enabled})

    def publish(self, name, command, content=None):
        return self.call(name, "publish", {"command_id": command, "kind": "evidence",
                                           "content": content or {"value": command}})

    def ids(self, name):
        return {row["event_id"] for row in self.call(name, "state")["events"]}

    def wait(self, predicate, timeout=12):
        deadline = time.monotonic() + timeout
        wake = threading.Event()
        last = None
        while time.monotonic() < deadline:
            try:
                last = predicate()
                if last:
                    return last
            except (OSError, ValueError) as error:
                last = type(error).__name__
            wake.wait(min(0.02, max(0, deadline - time.monotonic())))
        raise AssertionError(f"State predicate deadline ({last}); retained {self.root}")

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
        for stream in self.logs:
            stream.close()
        (self.root / "lifecycle.json").write_text(json.dumps({
            "started_at_unix": self.started, "finished_at_unix": time.time(),
            "pids": {k: p.pid for k, p in self.processes.items()},
            "returncodes": {k: p.returncode for k, p in self.processes.items()},
            "incarnations": [{**{k: v for k, v in item.items() if k != "process"},
                              "returncode": item["process"].returncode}
                             for item in self.history],
            "mode": self.mode, "members": self.names}, indent=2))


@contextmanager
def cluster(names, **kwargs):
    value = Cluster(names, **kwargs)
    try:
        yield value
    finally:
        value.close()


class PeerProtocolTests(unittest.TestCase):
    def test_broker_fanout_one_rejected_before_creating_store(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "node"
            with self.assertRaisesRegex(TransportError, "at least two"):
                Peer(root, "alpha", "broker", "test-capability-000000", fanout=1)
            self.assertFalse(root.exists())

    def test_broker_requires_two_distinct_members(self):
        with tempfile.TemporaryDirectory() as directory:
            peer = Peer(Path(directory), "alpha", "broker", "test-capability-000000")
            try:
                with self.assertRaises(TransportError):
                    peer.configure({"peers": {"alpha": 31001, "hubone": 31002},
                                    "brokers": ["hubone"], "enabled": False})
                self.assertEqual(peer.settings["peers"], {})
            finally:
                peer.owner.close()

    def test_authenticated_response_body_rejects_tampering(self):
        token = "test-capability-000000"
        value = _signed({"nonce": "a", "result": {"accepted": False}}, token)
        self.assertEqual(_verified(value, token)["nonce"], "a")
        value["body"]["result"]["accepted"] = True
        with self.assertRaises(TransportError):
            _verified(value, token)

    def test_frame_size_rejected_before_body_allocation(self):
        left, right = socket.socketpair()
        try:
            left.sendall(struct.pack("!I", MAX_FRAME + 1))
            with self.assertRaises(TransportError):
                receive(right, time.monotonic() + 1)
        finally:
            left.close()
            right.close()

    def test_framing_preserves_exact_payload_and_counts(self):
        left, right = socket.socketpair()
        try:
            payload = {"artifact": "content", "nested": [1, False, None]}
            size = send(left, payload, time.monotonic() + 1)
            result, received = receive(right, time.monotonic() + 1)
            self.assertEqual(result, payload)
            self.assertEqual(size, received)
        finally:
            left.close()
            right.close()


class PeerProcessTests(unittest.TestCase):
    def test_gossip_runs_in_separate_processes_without_driver_sync(self):
        with cluster(["alpha", "beta", "gamma"]) as c:
            event = c.publish("alpha", "one", {"source": "immutable data"})
            self.assertEqual(c.ids("beta"), set())
            self.assertEqual(c.ids("gamma"), set())
            self.assertEqual(len({c.call(n, "state")["pid"] for n in c.names}), 3)
            wanted = {event["event_id"]}
            wanted.update(c.publish("alpha", f"backlog-{i}")["event_id"]
                          for i in range(MAX_BATCH + 3))
            c.enable(True)
            c.wait(lambda: all(c.ids(n) == wanted for n in c.names))
            for n in c.names:
                self.assertEqual(c.call(n, "artifact", {"sha256": event["artifact_sha256"]}),
                                 {"source": "immutable data"})
                self.assertIsNone(c.call(n, "state")["fatal_error"])
            self.assertGreater(c.call("alpha", "state")["stats_since_boot"]["sent_bytes"], 0)

    def test_boundary_depth_artifact_replicates_through_full_envelopes(self):
        content = "leaf"
        for _ in range(MAX_ARTIFACT_DEPTH):
            content = {"child": content}
        with cluster(["alpha", "beta"]) as c:
            event = c.publish("alpha", "depth-boundary", content)
            with self.assertRaises(TransportError):
                c.publish("alpha", "too-deep", {"child": content})
            self.assertEqual(c.ids("alpha"), {event["event_id"]})
            c.enable(True)
            c.wait(lambda: c.ids("beta") == {event["event_id"]})
            self.assertEqual(c.call("beta", "artifact", {"sha256": event["artifact_sha256"]}),
                             content)
            self.assertIsNone(c.call("alpha", "state")["fatal_error"])
            self.assertIsNone(c.call("beta", "state")["fatal_error"])

    def test_partition_blocks_cross_group_evidence_then_heals_autonomously(self):
        with cluster(["alpha", "beta", "gamma"]) as c:
            c.call("alpha", "block", {"peers": ["gamma"]})
            c.call("beta", "block", {"peers": ["gamma"]})
            c.call("gamma", "block", {"peers": ["alpha", "beta"]})
            a, g = c.publish("alpha", "alpha-local"), c.publish("gamma", "gamma-local")
            c.enable(True)
            c.wait(lambda: c.ids("beta") == {a["event_id"]}
                   and c.call("gamma", "state")["stats_since_boot"]["failed_contacts"] > 0)
            self.assertEqual(c.ids("gamma"), {g["event_id"]})
            self.assertNotIn(g["event_id"], c.ids("alpha"))
            old_pid = c.processes["gamma"].pid
            c.kill("gamma")
            self.assertNotEqual(c.start("gamma"), old_pid)
            self.assertEqual(c.call("gamma", "state")["routing"]["blocked"], ["alpha", "beta"])
            c.wait(lambda: c.call("gamma", "state")["stats_since_boot"]["failed_contacts"] > 0)
            self.assertEqual(c.ids("gamma"), {g["event_id"]})
            self.assertNotIn(g["event_id"], c.ids("beta"))
            for n in c.names:
                c.call(n, "block", {"peers": []})
            wanted = {a["event_id"], g["event_id"]}
            c.wait(lambda: all(c.ids(n) == wanted for n in c.names))

    def test_durable_broker_failover_and_restarted_primary_catch_up(self):
        with cluster(["alpha", "beta", "hubone", "hubtwo"], mode="broker",
                     brokers=["hubone", "hubtwo"]) as c:
            a = c.publish("alpha", "before-primary-failure")
            c.enable(True)
            c.wait(lambda: all(c.ids(n) == {a["event_id"]} for n in c.names))
            old_pid = c.processes["hubone"].pid
            c.kill("hubone")
            c.enable(False)
            b = c.publish("beta", "during-primary-failure")
            def finished_turn():
                result = c.call("beta", "sync")
                return result if "busy" not in result else None

            turn = c.wait(finished_turn)
            self.assertEqual(turn, {"contacts": 2, "targets": ["hubone", "hubtwo"]})
            c.enable(True)
            wanted = {a["event_id"], b["event_id"]}
            c.wait(lambda: all(c.ids(n) == wanted for n in ["alpha", "beta", "hubtwo"]))
            self.assertGreater(c.call("beta", "state")["stats_since_boot"]["failed_contacts"], 0)
            self.assertNotEqual(c.start("hubone"), old_pid)
            c.wait(lambda: c.ids("hubone") == wanted)

    def test_actual_publish_crashes_before_commit_and_before_acknowledgment(self):
        with cluster(["alpha"]) as c:
            for point, code, expected_count in [("before_commit", 70, 0), ("after_commit", 71, 2)]:
                command = "crash-" + point
                c.call("alpha", "arm_crash", {"point": point, "operation": "publish"})
                with self.assertRaises((OSError, ValueError)):
                    c.publish("alpha", command)
                self.assertEqual(c.processes["alpha"].wait(timeout=5), code)
                c.kill("alpha")
                c.start("alpha")
                self.assertEqual(len(c.ids("alpha")), expected_count)
                event = c.publish("alpha", command)
                self.assertEqual(c.publish("alpha", command), event)
                self.assertEqual(len(c.ids("alpha")), 1 if point == "before_commit" else 2)

    def test_actual_receive_crashes_and_duplicate_replay_are_atomic(self):
        with cluster(["alpha", "beta"]) as c:
            first = c.publish("alpha", "first")
            for point, code, count in [("before_commit", 70, 0), ("after_commit", 71, 2)]:
                if point == "after_commit":
                    c.publish("alpha", "second")
                known = sorted(c.ids("beta"))
                answer = c.call("alpha", "exchange", {"known": known}, sender="beta")
                batch = answer["batch"]
                c.call("beta", "arm_crash", {"point": point, "operation": "merge"})
                with self.assertRaises((OSError, ValueError)):
                    c.call("beta", "merge", {"batch": batch}, sender="alpha")
                self.assertEqual(c.processes["beta"].wait(timeout=5), code)
                c.kill("beta")
                c.start("beta")
                self.assertEqual(len(c.ids("beta")), count)
                result = c.call("beta", "merge", {"batch": batch}, sender="alpha")
                self.assertEqual(result["added"], 1 if point == "before_commit" else 0)
                self.assertIn(first["event_id"], c.ids("beta"))

    def test_reordered_duplicate_batches_converge_over_tcp(self):
        with cluster(["alpha", "beta"]) as c:
            wanted = {c.publish("alpha", f"item-{i}")["event_id"] for i in range(8)}
            batch = c.call("alpha", "exchange", {"known": []}, sender="beta")["batch"]
            batch["events"].reverse()
            self.assertEqual(c.call("beta", "merge", {"batch": batch}, sender="alpha")["added"], 8)
            self.assertEqual(c.call("beta", "merge", {"batch": batch}, sender="alpha")["duplicates"], 8)
            self.assertEqual(c.ids("beta"), wanted)
            self.assertEqual(c.call("alpha", "state")["events"], c.call("beta", "state")["events"])

    def test_bad_frames_auth_and_partial_connections_do_not_change_evidence(self):
        with cluster(["alpha"]) as c:
            with self.assertRaises((OSError, ValueError)):
                request(c.ports["alpha"], "alpha", "incorrect-capability", "state")
            with socket.create_connection(("127.0.0.1", c.ports["alpha"]), timeout=2) as stream:
                stream.sendall(struct.pack("!I", MAX_FRAME + 1))
                stream.settimeout(4)
                self.assertEqual(stream.recv(1), b"")
            with socket.create_connection(("127.0.0.1", c.ports["alpha"]), timeout=2) as stream:
                stream.sendall(struct.pack("!I", 100) + b"{")
                stream.settimeout(5)
                self.assertEqual(stream.recv(1), b"")
            self.assertEqual(c.ids("alpha"), set())
            with self.assertRaises(TransportError):
                c.call("alpha", "accept", {"commit": "fake"})
            self.assertEqual(c.ids("alpha"), set())

    def test_exclusive_process_ownership_rejects_same_node_store(self):
        with cluster(["alpha"]) as c:
            path = c.root / "alpha" / "process-config.json"
            env = {"PATH": os.defpath, "PYTHONPATH": str(ROOT), "PYTHONDONTWRITEBYTECODE": "1"}
            duplicate = subprocess.run([sys.executable, "-m", "gossip_harness.peer_runtime_v1",
                                        "--config", str(path)], env=env, cwd=ROOT,
                                       capture_output=True, text=True, timeout=10)
            self.assertNotEqual(duplicate.returncode, 0)
            self.assertIn("already has an owner", duplicate.stderr)
            self.assertEqual(c.ids("alpha"), set())


if __name__ == "__main__":
    unittest.main()
