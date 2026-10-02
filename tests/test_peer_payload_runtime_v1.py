"""Retained actual-process tests for network payload availability and recovery."""
from __future__ import annotations

from contextlib import contextmanager
import base64
import json
import os
from pathlib import Path
import select
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid

from gossip_harness.peer_payload_runtime_v1 import PAYLOAD_KIND, PayloadPeer, _inventory
from gossip_harness.peer_payload_store_v1 import PayloadStore
from gossip_harness.peer_runtime_v1 import TransportError, request
from gossip_harness.peer_store_v1 import PROTOCOL, Store

ROOT = Path(__file__).resolve().parents[1]


def body(node, sender, operation, payload):
    return dict(protocol=PROTOCOL, receiver=node, sender=sender, operation=operation,
                payload=payload, nonce="a" * 32)


class PayloadRuntimeTests(unittest.TestCase):
    def test_inventory_rejects_unbounded_duplicate_and_bool_indices(self):
        sha = "a" * 64
        for value in ([], {sha: [True]}, {sha: [1, 1]}, {sha: [2, 1]}, {sha: [128]}, {"bad": []}):
            with self.subTest(value=value), self.assertRaises(ValueError):
                _inventory(value)
        self.assertEqual(_inventory({sha: [0, 127]}), {sha: [0, 127]})

    def test_arrived_notice_required_before_accepting_or_offering_chunk(self):
        with tempfile.TemporaryDirectory() as root:
            peer = PayloadPeer(Path(root), "receiver", "gossip", "k" * 32)
            try:
                peer.configure({"peers": {"receiver": 10001, "sender": 10002}, "brokers": [], "enabled": False})
                data = b"bounded private bytes"
                desc = peer.payloads.put(data, "application/octet-stream")
                chunk = {"sha256": desc["sha256"], "index": 0, "data": base64.b64encode(data).decode()}
                self.assertEqual(peer.payload_inventory(), {})
                with self.assertRaises(ValueError):
                    peer._accept(chunk)
                peer.store.publish("notice", PAYLOAD_KIND, desc)
                peer._accept(chunk)
                self.assertEqual(peer.payloads.read(desc["sha256"]), data)
                for sender in ("driver", "unknown", "receiver"):
                    with self.subTest(sender=sender), self.assertRaises(ValueError):
                        peer.dispatch(body("receiver", sender, "payload_push", {"chunk": chunk}))
                peer.dispatch(body("receiver", "driver", "block", {"peers": ["sender"]}))
                with self.assertRaises(ValueError):
                    peer.dispatch(body("receiver", "sender", "payload_push", {"chunk": chunk}))
            finally:
                peer.owner.close()

    def test_telemetry_reopens_and_corruption_fails_closed(self):
        with tempfile.TemporaryDirectory() as root:
            first = PayloadPeer(Path(root), "one", "gossip", "k" * 32)
            first._count(sent_bytes=17, payload_contacts=2)
            first.owner.close()
            second = PayloadPeer(Path(root), "one", "gossip", "k" * 32)
            self.assertEqual(second.durable["boots"], 2)
            self.assertEqual(second.durable["counts"]["sent_bytes"], 17)
            self.assertEqual(second.stats["sent_bytes"], 0)
            second.owner.close()
            value = json.loads((Path(root) / "payload-telemetry.json").read_text())
            value["counts"]["sent_bytes"] = True
            (Path(root) / "payload-telemetry.json").write_text(json.dumps(value))
            with self.assertRaises(ValueError):
                PayloadPeer(Path(root), "one", "gossip", "k" * 32)


class PayloadCluster:
    def __init__(self, *, mode="gossip", data=b"payload", media_type="application/octet-stream"):
        self.root = ROOT / "runs" / "peer-payload-fixtures" / uuid.uuid4().hex
        self.root.mkdir(parents=True)
        self.names = ["origin", "recipient"] + (["relay-a", "relay-b"] if mode == "broker" else [])
        self.mode, self.token = mode, uuid.uuid4().hex
        self.brokers = self.names[2:]
        self.processes, self.ports, self.logs, self.history = {}, {}, [], []
        self.descriptor = PayloadStore(self.root / "origin", "origin").put(data, media_type)
        self.notice = Store(self.root / "origin" / "events.sqlite", "origin").publish("seed-payload", PAYLOAD_KIND, self.descriptor)
        try:
            for name in self.names:
                self.start(name)
            self.enable(False)
        except BaseException:
            self.close()
            raise

    def start(self, name):
        root = self.root / name
        root.mkdir(exist_ok=True)
        config = {"root": str(root), "node_id": name, "mode": self.mode, "token": self.token,
                  "port": self.ports.get(name, 0), "interval": 0.05, "fanout": 2}
        path = root / ("config-" + uuid.uuid4().hex + ".json")
        path.write_text(json.dumps(config))
        log = (root / ("stderr-" + uuid.uuid4().hex + ".log")).open("w")
        self.logs.append(log)
        env = {"PATH": os.defpath, "PYTHONPATH": str(ROOT), "PYTHONHASHSEED": "0", "PYTHONDONTWRITEBYTECODE": "1", "LANG": "C.UTF-8"}
        process = subprocess.Popen([sys.executable, "-m", "gossip_harness.peer_payload_runtime_v1", "--config", str(path)],
                                   cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=log, text=True)
        self.processes[name] = process
        self.history.append({"name": name, "pid": process.pid, "process": process,
                             "config": str(path.relative_to(self.root)), "started_at": time.time(),
                             "stderr": str(Path(log.name).relative_to(self.root))})
        ready, _, _ = select.select([process.stdout], [], [], 10)
        if not ready:
            raise AssertionError(f"Readiness timeout: {name}; retained {root}")
        line = process.stdout.readline()
        if not line:
            raise AssertionError(f"Exited before readiness: {name}; retained {root}")
        result = json.loads(line)
        assert result["pid"] == process.pid and result["node_id"] == name
        if name in self.ports:
            assert result["port"] == self.ports[name]
        self.ports[name] = result["port"]
        return process.pid

    def call(self, node, operation, payload=None, **kwargs):
        return request(self.ports[node], node, self.token, operation, payload, **kwargs)[0]

    def enable(self, value):
        for node in self.names:
            if self.processes[node].poll() is None:
                self.call(node, "configure", {"peers": self.ports, "brokers": self.brokers, "enabled": value})

    def wait(self, predicate, timeout=25):
        deadline, wake, last = time.monotonic() + timeout, threading.Event(), None
        while time.monotonic() < deadline:
            try:
                last = predicate()
                if last:
                    return last
            except (OSError, ValueError) as error:
                last = type(error).__name__
            wake.wait(min(0.02, max(0, deadline - time.monotonic())))
        raise AssertionError(f"Predicate deadline ({last}); retained {self.root}")

    def object(self, node):
        return self.call(node, "payload_state")["objects"].get(self.descriptor["sha256"])

    def complete(self, node):
        value = self.object(node)
        return value if value and value["complete"] else None

    def kill(self, node):
        process = self.processes[node]
        if process.poll() is None:
            process.kill()
        process.wait(timeout=5)
        if process.stdout:
            process.stdout.close()

    def close(self):
        for node in self.processes:
            self.kill(node)
        for log in self.logs:
            log.close()
        (self.root / "lifecycle.json").write_text(json.dumps({"mode": self.mode, "finished_at": time.time(),
            "incarnations": [{**{k: v for k, v in row.items() if k != "process"},
                              "returncode": row["process"].returncode} for row in self.history]}, indent=2))


@contextmanager
def payload_cluster(**kwargs):
    cluster = PayloadCluster(**kwargs)
    try:
        yield cluster
    finally:
        cluster.close()


class PayloadProcessTests(unittest.TestCase):
    def assert_bytes(self, c, node, data):
        self.assertIsNotNone(c.wait(lambda: c.complete(node)))
        local = PayloadStore(c.root / node, node)
        self.assertEqual(local.read(c.descriptor["sha256"]), data)
        self.assertIsNone(c.call(node, "state")["fatal_error"])

    def test_autonomous_large_binary_transfer_over_both_adapters(self):
        data = bytes(range(256)) * 2301
        for mode in ("gossip", "broker"):
            with self.subTest(mode=mode), payload_cluster(mode=mode, data=data) as c:
                self.assertEqual(c.call("recipient", "payload_state")["objects"], {})
                c.enable(True)
                self.assert_bytes(c, "recipient", data)
                stats = c.call("recipient", "payload_state")["telemetry"]["counts"]
                self.assertGreater(stats["received_bytes"], len(data))
                self.assertGreater(stats["validated_chunks"], 1)

    def test_notice_is_not_payload_and_partition_applies_to_bulk_channel(self):
        data = b"must actually arrive" * 10000
        with payload_cluster(data=data) as c:
            # Explicit metadata-only bootstrap isolates the descriptor/byte
            # distinction; subsequent bulk recovery is driven by local timers.
            c.call("recipient", "sync")
            self.assertFalse(c.object("recipient")["complete"])
            c.call("origin", "block", {"peers": ["recipient"]})
            c.call("recipient", "block", {"peers": ["origin"]})
            c.enable(True)
            c.wait(lambda: c.call("recipient", "payload_state")["telemetry"]["payload_round"] >= 3)
            self.assertFalse(c.object("recipient")["complete"])
            c.call("origin", "block", {"peers": []})
            c.call("recipient", "block", {"peers": []})
            self.assert_bytes(c, "recipient", data)

    def test_partial_payload_and_transport_counters_survive_actual_restart(self):
        data = bytes(range(256)) * 12001
        with payload_cluster(data=data) as c:
            c.enable(True)
            def partial():
                value = c.object("recipient")
                return value if value and 0 < len(value["missing"]) < value["chunk_count"] else None
            c.wait(partial)
            c.kill("recipient")
            saved = json.loads((c.root / "recipient" / "payload-telemetry.json").read_text())
            before = PayloadStore(c.root / "recipient", "recipient").missing(c.descriptor["sha256"])
            self.assertGreater(len(before), 0)
            c.start("recipient")
            self.assert_bytes(c, "recipient", data)
            after = c.call("recipient", "payload_state")["telemetry"]
            self.assertEqual(after["boots"], 2)
            for key, count in saved["counts"].items():
                self.assertGreaterEqual(after["counts"][key], count)

    def test_replicated_payload_recovers_after_primary_broker_dies(self):
        data = b"replicated bytes" * 40000
        with payload_cluster(mode="broker", data=data) as c:
            c.call("recipient", "block", {"peers": ["relay-a", "relay-b"]})
            for relay in ("relay-a", "relay-b"):
                c.call(relay, "block", {"peers": ["recipient"]})
            c.enable(True)
            self.assert_bytes(c, "relay-b", data)
            self.assertEqual(c.call("recipient", "payload_state")["objects"], {})
            c.kill("relay-a")
            c.kill("origin")
            c.call("relay-b", "block", {"peers": []})
            c.call("recipient", "block", {"peers": ["relay-a"]})
            self.assert_bytes(c, "recipient", data)
            stats = c.call("recipient", "payload_state")["telemetry"]["counts"]
            self.assertGreater(stats["validated_chunks"], 0)
            self.assertGreater(stats["failed_payload_contacts"], 0)

    def test_observer_and_unknown_peer_cannot_upload_payload_bytes(self):
        with payload_cluster() as c:
            chunk = {"sha256": c.descriptor["sha256"], "index": 0, "data": base64.b64encode(b"payload").decode()}
            for sender in ("driver", "outside", "recipient"):
                with self.subTest(sender=sender), self.assertRaises(TransportError):
                    c.call("recipient", "payload_push", {"chunk": chunk}, sender=sender)
            self.assertEqual(c.call("recipient", "payload_state")["objects"], {})


class PayloadGitProcessTests(unittest.TestCase):
    def test_transferred_bundle_prepares_exact_merged_tree_without_sender_path(self):
        from devtools.test_artifacts import ArtifactDirectory
        from gossip_harness.gitstore import GitStore
        from gossip_harness.peer_git_bundle_v1 import export_bundle, import_bundle

        with ArtifactDirectory("payload-git-integration", retain_success=True) as artifacts:
            baseline = GitStore.create(artifacts.root / "baseline.git", {
                "alpha.json": '{"value":1}\n', "beta.json": '{"value":10}\n'})
            base = baseline.head()
            sender = GitStore.fork(baseline, artifacts.root / "sender.git")
            offered = sender.propose({"alpha.json": '{"value":8}\n'})
            payload, manifest = export_bundle(sender, offered, base)
            target = GitStore.fork(baseline, artifacts.root / "target.git")
            sibling = target.propose({"beta.json": '{"value":9}\n'})
            initial = target.prepare(target, sibling, base, lambda _: (True, "Trusted setup"), ("beta.json",))
            self.assertEqual(target.accept(initial).status, "accepted")
            expected_head = target.head()
            with payload_cluster(data=payload, media_type="application/x-git-bundle") as c:
                offer_event = c.call("origin", "publish", {"command_id": "git-proposal", "kind": "git_proposal",
                    "content": {"protocol": "peer-git-offer-fixture-v1", "manifest": manifest,
                                "payload_sha256": c.descriptor["sha256"]}})
                c.enable(True)
                c.wait(lambda: c.complete("recipient"))
                c.wait(lambda: offer_event["event_id"] in {row["event_id"] for row in c.call("recipient", "state")["events"]})
                # From here the receiver uses only its independently received
                # bytes and metadata. Neither original source remains available.
                c.kill("origin")
                sender.path.rename(artifacts.root / "sender-retained-offline.git")
                local_event = next(row for row in c.call("recipient", "state")["events"] if row["event_id"] == offer_event["event_id"])
                local_offer = c.call("recipient", "artifact", {"sha256": local_event["artifact_sha256"]})
                received = PayloadStore(c.root / "recipient", "recipient").read(local_offer["payload_sha256"])
                quarantine = import_bundle(received, local_offer["manifest"], artifacts.root / "received")
                inspected = []
                def validate(checkout):
                    values = [json.loads((checkout / name).read_text())["value"] for name in ("alpha.json", "beta.json")]
                    inspected.append(values)
                    return values[0] <= values[1], "Trusted data-only merged-tree invariant"
                candidate = target.prepare(quarantine, local_offer["manifest"]["offered_sha"], expected_head,
                                           validate, ("alpha.json",))
                self.assertEqual(candidate.status, "prepared", candidate.detail)
                self.assertEqual(inspected, [[8, 9]])
                self.assertNotEqual(candidate.candidate_sha, offered)
                self.assertEqual(target.head(), expected_head)
                result = target.accept(candidate)
                self.assertEqual(result.status, "accepted")
                self.assertEqual(result.new_head, candidate.candidate_sha)
                (artifacts.root / "integration.json").write_text(json.dumps({"source_available": False,
                    "receiver_payload_sha256": local_offer["payload_sha256"], "offered_sha": offered,
                    "expected_head": expected_head, "tested_and_published_commit": candidate.candidate_sha,
                    "checked_data": inspected, "candidate_code_executed": False,
                    "independent_final_project_acceptance": False}, indent=2))
