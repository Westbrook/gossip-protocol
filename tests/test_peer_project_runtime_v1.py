"""Actual mesh-to-Docker public promotion; no live model or host candidate run."""
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
import uuid

from gossip_harness.gitstore import GitStore
from gossip_harness.blackbox_validator import _canonical as validator_canonical
from gossip_harness.peer_coding_runtime_v1 import _digest
from gossip_harness.peer_project_runtime_v1 import (
    MAX_OBSERVER_BYTES, PROTOCOL, PromotionPeer, _bind_configuration, _promotion_summary,
)
from gossip_harness.peer_runtime_v1 import TransportError, request
from gossip_harness.peer_store_v1 import canonical_bytes
from gossip_harness.pilot import DEFAULT_IMAGE
from gossip_harness.worker import MODEL

ROOT = Path(__file__).resolve().parents[1]
TOKEN = "project-mesh-fixture-credential-0001"
BASE_FILES = {"solution.py": "from helper import OFFSET\ndef solve(value):\n    return value['n'] + OFFSET\n",
              "helper.py": "OFFSET = 0\n"}
GOOD_SOURCE = "from helper import OFFSET\ndef solve(value):\n    return value['n'] + 2 + OFFSET\n"
BAD_SOURCE = "def solve(value):\n    return value['n'] - 1\n"
CASES = [{"id": "positive", "input": {"n": 1}, "expected": 13},
         {"id": "negative", "input": {"n": -4}, "expected": 8}]


def fixture_response(source):
    return {"id": "offline_project_response", "status": "completed", "model": MODEL, "service_tier": "default",
            "usage": {"input_tokens": 10, "output_tokens": 10}, "output": [
                {"type": "message", "role": "assistant", "status": "completed", "content": [
                    {"type": "output_text", "text": json.dumps({"changes": [{"path": "solution.py", "content": source}],
                                                               "summary": "Provided offline coding response."})}]}]}


class ProjectRuntimeTests(unittest.TestCase):
    def test_declared_gate_is_durable_observation_only_and_state_is_bounded_nonblocking(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            peer = PromotionPeer(root, "service", "gossip", TOKEN, initially_enabled=False)
            try:
                class MustNotRun:
                    def state(self):
                        raise AssertionError("Observer blocked on adapter state")

                    def tick(self):
                        raise AssertionError("Gate executed work synchronously")

                peer.promotion = MustNotRun()
                record = {"attempt_id": "a" * 64, "status": "accepted", "offer_event_id": "b" * 64,
                          "offer": {"action_id": "c" * 64, "bundle_manifest": {"offered_sha": "d" * 40}},
                          "candidate": {"candidate_sha": "e" * 40}, "head": "e" * 40,
                          "validation": {"receipt_sha256": "f" * 64, "source_sha256": "0" * 64,
                              "physically_executed": True, "receipt": {"container_name": "gossip-check-test", "passed": True,
                                  "image_id": DEFAULT_IMAGE, "cleanup_verified": True, "status": "passed", "output": "x" * 200_000}},
                          "cases": ["large-suite" * 100_000], "candidate_files": {"solution.py": "x" * 200_000}}
                state = {"protocol": "peer-promotion-v1", "config_sha256": "1" * 64, "attempts": [record] * 128}
                peer.promotion_snapshot = _promotion_summary(state)
                peer.promotion_busy = True
                body = {"protocol": "gossip-peer-v1", "sender": "driver", "receiver": "service", "nonce": "1" * 32,
                        "operation": "state", "payload": {}}
                observed = peer.dispatch(body)
                self.assertTrue(observed["promotion_busy"])
                self.assertEqual(observed["promotion"]["omitted_attempts"], 120)
                self.assertLessEqual(len(canonical_bytes(observed["promotion"])), MAX_OBSERVER_BYTES)
                self.assertNotIn("large-suite", json.dumps(observed))
                self.assertEqual(peer.dispatch({**body, "operation": "promotion_gate", "payload": {"enabled": True}}), {"enabled": True})
                for payload in ({"enabled": 1}, {"enabled": True, "reset": True}):
                    with self.assertRaises(TransportError):
                        peer.dispatch({**body, "operation": "promotion_gate", "payload": payload})
                for operation in ("run_action", "promote", "validate"):
                    with self.assertRaises(TransportError):
                        peer.dispatch({**body, "operation": operation})
                class FailedAdapter:
                    def tick(self):
                        raise RuntimeError("original integration failure")

                    def state(self):
                        raise ValueError("unavailable snapshot")

                previous = peer.promotion_snapshot
                peer.promotion = FailedAdapter()
                peer.settings["enabled"] = True
                peer.run_promotions()
                self.assertEqual(peer.promotion_error, "RuntimeError")
                self.assertFalse(peer.promotion_busy)
                self.assertEqual(peer.promotion_snapshot, previous)
            finally:
                peer.owner.close()
            reopened = PromotionPeer(root, "service", "gossip", TOKEN, initially_enabled=False)
            try:
                self.assertTrue(reopened.promotion_enabled)
            finally:
                reopened.owner.close()

    def test_project_identity_binds_own_root_and_policy_but_permits_restored_port(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = {"root": str(root), "node_id": "origin", "mode": "gossip", "token": TOKEN,
                      "port": 0, "interval": 0.05, "fanout": 2, "role": "peer", "seed": None}
            identity = _bind_configuration(root, config)
            self.assertEqual(identity, _bind_configuration(root, {**config, "port": 12345}))
            for field, value in (("node_id", "different"), ("seed", {"changed": True}), ("fanout", True)):
                with self.subTest(field=field), self.assertRaisesRegex(ValueError, "identity"):
                    _bind_configuration(root, {**config, field: value})


class ProjectCluster:
    def __init__(self, mode="gossip", *, source=GOOD_SOURCE, divergent=True, crash=None):
        self.root = ROOT / "runs" / "peer-project-fixtures" / uuid.uuid4().hex
        self.root.mkdir(parents=True)
        self.mode, self.source = mode, source
        self.token, self.key = uuid.uuid4().hex, uuid.uuid4().hex
        self.names = ["service", "origin", "worker"] + (["relay-a", "relay-b"] if mode == "broker" else [])
        self.processes, self.ports, self.logs, self.history = {}, {}, [], []
        self.authority_port = 0
        self.baseline = GitStore.create(self.root / "bootstrap.git", BASE_FILES)
        self.base_sha = self.baseline.head()
        self.sender = GitStore.fork(self.baseline, self.root / "worker" / "repository.git")
        self.target = GitStore.fork(self.baseline, self.root / "service" / "repository.git")
        if divergent:
            setup = self.target.propose({"helper.py": "OFFSET = 10\n"})
            prepared = self.target.prepare(self.target, setup, self.base_sha,
                                           lambda path: (True, "Trusted bootstrap fixture; no candidate execution"), ("helper.py",))
            assert prepared.status == "prepared"
            assert self.target.accept(prepared).status == "accepted"
        self.initial_head = self.target.head()
        self.seed = {"task_id": "project-slot", "instructions": "Implement solve(value) to add two to integer value['n'] while retaining the imported helper OFFSET.",
                     "allowed_paths": ["solution.py"], "files": BASE_FILES, "base_sha": self.base_sha,
                     "attempt": 1, "feedback": ""}
        self.authority_config = {"protocol": "peer-authority-v1", "run_id": "project-process-fixture", "budget_units": 1_000_000,
            "lease_seconds": 300, "principals": {"worker": {"key": self.key, "tasks": ["project-slot"]}},
            "tasks": {"project-slot": {"fixture": "text_build", "reservation_units": 0}}}
        self.profiles = {"fixture-mini": {"mode": "offline-response-fixture", "model": MODEL,
                                          "max_output_tokens": 1024, "response": fixture_response(source)}}
        try:
            ready = self.start("service", crash=crash)
            self.coding = {"run_id": self.authority_config["run_id"], "task_id": "project-slot", "profile_id": "fixture-mini",
                           "seed_producer": "origin", "seed_sha256": _digest(self.seed), "service_producer": "service",
                           "authority_sha256": ready["config_sha256"], "dispatch_config_sha256": ready["dispatch_config_sha256"]}
            for name in self.names[1:]:
                self.start(name)
            self.enable(False)
        except BaseException:
            self.close()
            raise

    def start(self, name, *, crash=None):
        directory = self.root / name
        directory.mkdir(exist_ok=True)
        config = {"root": str(directory), "node_id": name, "mode": self.mode, "token": self.token,
                  "port": self.ports.get(name, 0), "interval": 0.05, "fanout": 2,
                  "role": "promotion-service" if name == "service" else "candidate-worker" if name == "worker" else "peer"}
        if name == "service":
            config.update(authority=self.authority_config, authority_port=self.authority_port, profiles=self.profiles,
                          task_specs={"project-slot": {"allowed_paths": ["solution.py"], "profiles": ["fixture-mini"]}},
                          promotion={"baseline_sha": self.base_sha, "cases": CASES, "image": DEFAULT_IMAGE,
                                     "timeout_seconds": 30, "case_timeout_seconds": 2, "initially_enabled": False})
        elif name == "worker":
            config.update(coding=self.coding, authority={"port": self.authority_port, "principal": "worker", "key": self.key},
                          candidate={"baseline_sha": self.base_sha, "allowed_paths": ["solution.py"], "generation": 0})
        else:
            config["seed"] = self.seed if name == "origin" else None
        path = directory / ("config-" + uuid.uuid4().hex + ".json")
        path.write_bytes(canonical_bytes(config))
        log = (directory / ("stderr-" + uuid.uuid4().hex + ".log")).open("w")
        self.logs.append(log)
        # Preserve only Docker's trusted local context plus the pinned Python
        # package root; no API key or arbitrary application environment passes.
        env = {key: os.environ[key] for key in ("PATH", "HOME", "DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_CONFIG", "XDG_RUNTIME_DIR")
               if key in os.environ}
        env.update(PYTHONPATH=str(ROOT), PYTHONHASHSEED="0", PYTHONDONTWRITEBYTECODE="1", LANG="C.UTF-8")
        extra = ["--promotion-crash-point", crash] if crash else []
        process = subprocess.Popen([sys.executable, "-m", "gossip_harness.peer_project_runtime_v1", "--config", str(path), *extra],
                                   cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=log, text=True)
        self.processes[name] = process
        self.history.append({"name": name, "pid": process.pid, "process": process, "config": str(path.relative_to(self.root)),
                             "stderr": str(Path(log.name).relative_to(self.root))})
        ready, _, _ = select.select([process.stdout], [], [], 15)
        if not ready:
            raise AssertionError(f"Project readiness timeout: {name}; retained {directory}")
        line = process.stdout.readline()
        if not line:
            raise AssertionError(f"Project exited before readiness: {name}; retained {directory}")
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

    def enable(self, value):
        for name in self.names:
            self.call(name, "configure", {"peers": self.ports, "brokers": ["relay-a", "relay-b"] if self.mode == "broker" else [],
                                           "enabled": value})

    def wait(self, predicate, timeout=60):
        deadline, wake, last = time.monotonic() + timeout, threading.Event(), None
        while time.monotonic() < deadline:
            try:
                last = predicate()
                if last:
                    return last
            except (OSError, ValueError) as error:
                last = type(error).__name__
            wake.wait(min(0.03, max(0, deadline - time.monotonic())))
        raise AssertionError(f"Project predicate timeout ({last}); retained {self.root}")

    def offer_arrived(self):
        state = self.call("service", "state")
        for event in state["events"]:
            if event["kind"] == "candidate_offer" and event["producer"] == "worker":
                offer = self.call("service", "artifact", {"sha256": event["artifact_sha256"]})
                payloads = self.call("service", "payload_state")["objects"]
                if offer["bundle_sha256"] in payloads and payloads[offer["bundle_sha256"]]["complete"]:
                    return {"event": event, "offer": offer, "payload": payloads[offer["bundle_sha256"]]}
        return None

    def attempt(self, status):
        state = self.call("service", "state")
        if state["promotion_error"]:
            raise AssertionError(f"Promotion timer failed: {state['promotion_error']}; retained {self.root}")
        attempts = state["promotion"]["attempts"]
        return next((item for item in attempts if item["status"] == status), None)

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
            "real_provider_requests": 0, "candidate_execution_boundary": "pinned Docker / host oracle",
            "incarnations": [{**{k: v for k, v in row.items() if k != "process"}, "returncode": row["process"].returncode}
                             for row in self.history]}, indent=2))

    def ledger(self):
        path = self.root / "service" / "authority" / "authority.sqlite"
        with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as db:
            db.row_factory = sqlite3.Row
            return {table: [dict(row) for row in db.execute("SELECT * FROM " + table)]
                    for table in ("tasks", "reservations", "intents")}

    def transport_entries(self):
        return [json.loads(line) for line in (self.root / "service" / "offline-transport.jsonl").read_text().splitlines()]

    def records(self):
        return [json.loads(path.read_text())["record"] for path in sorted((self.root / "service" / "promotion-journal").glob("attempt-*.json"))]

    def retain_pipeline(self, outcome, arrived, attempt):
        records = self.records()
        assert len(records) == 1
        record = records[0]
        current = self.target.head()
        value = {"protocol": "peer-project-public-pipeline-receipt-v1", "purpose": "complete_public_integration_rehearsal",
                 "outcome": outcome, "mode": self.mode, "baseline_sha": self.base_sha, "old_head": self.initial_head,
                 "accepted_head": current, "accepted_files_sha256": hashlib.sha256(canonical_bytes(self.target.read_files(current))).hexdigest(),
                 "offer": arrived, "observed_attempt": attempt, "promotion_record": record, "ledger": self.ledger(),
                 "offline_transport_entries": self.transport_entries(), "real_provider_requests": 0,
                 "independent_final_project_acceptance": False, "registered_scientific_rehearsal": False,
                 "evidence_paths": {"lifecycle": "lifecycle.json", "offline_transport": "service/offline-transport.jsonl",
                     "candidate_state": "worker/candidate-journal/candidate-state.json", "promotion_journal": "service/promotion-journal",
                     "authority_ledger": "service/authority/authority.sqlite", "target": "service/repository.git"}}
        (self.root / "pipeline-receipt.json").write_text(json.dumps(value, sort_keys=True, indent=2))
        return value


@contextmanager
def project_cluster(**kwargs):
    cluster = ProjectCluster(**kwargs)
    try:
        yield cluster
    finally:
        cluster.close()


@unittest.skipUnless(os.environ.get("GOSSIP_RUN_DOCKER_TESTS") == "1", "real Docker opt-in")
class ProjectRuntimeDockerTests(unittest.TestCase):
    def test_actual_mesh_promotes_exact_divergent_merged_source_with_sender_unavailable(self):
        for mode in ("gossip", "broker"):
            with self.subTest(mode=mode), project_cluster(mode=mode) as cluster:
                cluster.call("origin", "block", {"peers": [node for node in cluster.names if node != "origin"]})
                cluster.enable(True)
                cluster.wait(lambda: cluster.call("worker", "payload_state")["telemetry"]["payload_round"] >= 3)
                self.assertIsNone(cluster.call("worker", "state")["coding"])
                cluster.call("origin", "block", {"peers": []})
                arrived = cluster.wait(cluster.offer_arrived)
                self.assertEqual(cluster.call("service", "state")["promotion"]["attempt_count"], 0)
                self.assertEqual(cluster.target.head(), cluster.initial_head)
                cluster.kill("worker")
                cluster.sender.path.rename(cluster.root / "worker" / "sender-unavailable.git")
                self.assertFalse(cluster.sender.path.exists())
                cluster.call("service", "promotion_gate", {"enabled": True})
                attempt = cluster.wait(lambda: cluster.attempt("accepted"))
                self.assertNotEqual(cluster.target.head(), arrived["offer"]["bundle_manifest"]["offered_sha"])
                merged_files = cluster.target.read_files(cluster.target.head())
                self.assertEqual(merged_files, {"solution.py": GOOD_SOURCE, "helper.py": "OFFSET = 10\n"})
                # These known fixture formulas are host-side expectations,
                # never imports/execution of candidate Python on the host.
                self.assertTrue(all(case["input"]["n"] + 2 != case["expected"] for case in CASES))
                self.assertTrue(all(case["input"]["n"] + 10 != case["expected"] for case in CASES))
                source_sha = hashlib.sha256(validator_canonical(merged_files).encode()).hexdigest()
                self.assertEqual(attempt["validation"]["source_sha256"], source_sha)
                self.assertEqual(cluster.records()[0]["validation"]["receipt"]["source_sha256"], source_sha)
                self.assertEqual(attempt["candidate_sha"], cluster.target.head())
                self.assertTrue(attempt["validation"]["passed"])
                self.assertTrue(attempt["validation"]["cleanup_verified"])
                self.assertEqual(len(cluster.transport_entries()), 1)
                ledger = cluster.ledger()
                self.assertEqual(ledger["tasks"][0]["status"], "complete")
                self.assertEqual(ledger["tasks"][0]["accepted_commit"], cluster.target.head())
                self.assertEqual(ledger["intents"][0]["state"], "accepted")
                with self.assertRaises(TransportError):
                    cluster.call("service", "run_action")
                cluster.retain_pipeline("accepted", arrived, attempt)

    def test_wrong_answer_is_rejected_without_git_or_task_completion(self):
        with project_cluster(source=BAD_SOURCE) as cluster:
            cluster.enable(True)
            arrived = cluster.wait(cluster.offer_arrived)
            cluster.call("service", "promotion_gate", {"enabled": True})
            attempt = cluster.wait(lambda: cluster.attempt("rejected"))
            self.assertFalse(attempt["validation"]["passed"])
            self.assertTrue(attempt["validation"]["cleanup_verified"])
            self.assertEqual(cluster.target.head(), cluster.initial_head)
            self.assertEqual(cluster.ledger()["intents"], [])
            self.assertNotEqual(cluster.ledger()["tasks"][0]["status"], "complete")
            self.assertEqual(len(cluster.transport_entries()), 1)
            cluster.retain_pipeline("rejected", arrived, attempt)

    def test_actual_service_restart_after_git_recovers_exact_validation_without_second_container(self):
        with project_cluster(crash="after_git") as cluster:
            cluster.enable(True)
            arrived = cluster.wait(cluster.offer_arrived)
            cluster.call("service", "promotion_gate", {"enabled": True})
            cluster.wait(lambda: cluster.processes["service"].poll() is not None)
            self.assertEqual(cluster.processes["service"].returncode, 85)
            records = cluster.records()
            self.assertEqual(len(records), 1)
            persisted = records[0]
            self.assertEqual(persisted["status"], "promoting")
            old_validation = persisted["validation"]
            self.assertTrue(old_validation["receipt"]["passed"])
            self.assertEqual(cluster.target.head(), persisted["candidate"]["candidate_sha"])
            old_pid = cluster.processes["service"].pid
            cluster.kill("service")
            ready = cluster.start("service")
            self.assertNotEqual(old_pid, ready["pid"])
            attempt = cluster.wait(lambda: cluster.attempt("accepted"))
            self.assertEqual(cluster.records()[0]["validation"], old_validation)
            self.assertEqual(attempt["validation"]["container_name"], old_validation["receipt"]["container_name"])
            self.assertEqual(len(cluster.transport_entries()), 1)
            self.assertEqual(len(cluster.ledger()["intents"]), 1)
            self.assertEqual(cluster.ledger()["tasks"][0]["status"], "complete")
            first_round = cluster.call("service", "payload_state")["telemetry"]["payload_round"]
            cluster.wait(lambda: cluster.call("service", "payload_state")["telemetry"]["payload_round"] >= first_round + 3)
            self.assertEqual(cluster.records()[0]["validation"], old_validation)
            self.assertEqual(cluster.attempt("accepted")["candidate_sha"], cluster.target.head())
            cluster.retain_pipeline("accepted_after_git_recovery", arrived, attempt)


if __name__ == "__main__":
    unittest.main()
