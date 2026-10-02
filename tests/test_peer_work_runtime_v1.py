"""Real peer-local build/review decisions, network visibility and crash recovery.

All roles use deterministic trusted text fixtures, not models or candidate code.
Owned process evidence is retained; no observer routes an action or its context.
"""
from __future__ import annotations

from contextlib import contextmanager
import json
import os
from pathlib import Path
import select
import sqlite3
import subprocess
import sys
import threading
import time
import unittest
import uuid

from gossip_harness.peer_authority_v1 import authority_request, config_digest
from gossip_harness.peer_runtime_v1 import request
from gossip_harness.peer_work_v1 import WorkJournal

ROOT = Path(__file__).resolve().parents[1]


class WorkCluster:
    def __init__(self, mode="gossip", *, worker_crash=None, authority_crash=None, lease_seconds=30):
        self.root = ROOT / "runs" / "peer-work-fixtures" / uuid.uuid4().hex
        self.root.mkdir(parents=True)
        self.mode, self.token = mode, uuid.uuid4().hex
        self.names = ["builder", "reviewer"] + (["relay-a", "relay-b"] if mode == "broker" else [])
        self.ports, self.processes, self.logs, self.history = {}, {}, [], []
        self.keys = {node: uuid.uuid4().hex for node in ("builder", "reviewer")}
        self.authority_config = {
            "protocol": "peer-authority-v1", "run_id": "local-chain",
            "budget_units": 10, "lease_seconds": lease_seconds,
            "principals": {node: {"key": self.keys[node], "tasks": [node + "-slot"]}
                           for node in self.keys},
            "tasks": {node + "-slot": {"fixture": "text_build" if node == "builder" else "text_review",
                                       "reservation_units": 2} for node in self.keys}}
        self.profile = config_digest(self.authority_config)
        try:
            self.start_authority(crash=authority_crash)
            for node in self.names:
                self.start(node, crash=worker_crash if node == "builder" else None)
            self.enable(False)
        except BaseException:
            self.close()
            raise

    def launch(self, name, module, config, extra=()):
        directory = self.root / name
        directory.mkdir(exist_ok=True)
        path = directory / ("config-" + uuid.uuid4().hex + ".json")
        path.write_text(json.dumps(config))
        log = (directory / ("stderr-" + uuid.uuid4().hex + ".log")).open("w")
        self.logs.append(log)
        env = {"PATH": os.defpath, "PYTHONPATH": str(ROOT), "PYTHONHASHSEED": "0",
               "PYTHONDONTWRITEBYTECODE": "1", "LANG": "C.UTF-8"}
        process = subprocess.Popen([sys.executable, "-m", module, "--config", str(path), *extra],
                                   cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=log, text=True)
        self.processes[name] = process
        self.history.append({"name": name, "pid": process.pid, "started_at": time.time(),
                             "config": str(path.relative_to(self.root)),
                             "stderr": str(Path(log.name).relative_to(self.root)), "process": process})
        ready, _, _ = select.select([process.stdout], [], [], 10)
        if not ready:
            raise AssertionError(f"Readiness timeout for {name}; retained {directory}")
        line = process.stdout.readline()
        if not line:
            raise AssertionError(f"Exited before readiness: {name}; retained {directory}")
        info = json.loads(line)
        assert info["pid"] == process.pid
        if name in self.ports:
            assert self.ports[name] == info["port"]
        self.ports[name] = info["port"]
        return process.pid

    def start_authority(self, crash=None, request_id=None):
        extra = ["--crash-point", crash, "--crash-request-id", request_id] if crash else []
        return self.launch("authority", "gossip_harness.peer_authority_v1", {
            "root": str(self.root / "authority"), "port": self.ports.get("authority", 0),
            "authority": self.authority_config}, extra)

    def work_config(self, node):
        return {"run_id": "local-chain", "role": node, "task_id": node + "-slot",
                "seed_producer": "builder", "builder_peer": "builder", "authority_sha256": self.profile}

    def start(self, node, crash=None):
        config = {"root": str(self.root / node), "node_id": node, "mode": self.mode,
                  "token": self.token, "port": self.ports.get(node, 0), "interval": 0.05, "fanout": 2}
        module = "gossip_harness.peer_runtime_v1"
        if node in self.keys:
            module = "gossip_harness.peer_work_runtime_v1"
            config.update(work=self.work_config(node), authority={
                "port": self.ports["authority"], "principal": node, "key": self.keys[node],
                "config_sha256": self.profile})
        return self.launch(node, module, config, ["--crash-point", crash] if crash else [])

    def call(self, node, operation, payload=None):
        return request(self.ports[node], node, self.token, operation, payload)[0]

    def enable(self, value):
        peers = {node: self.ports[node] for node in self.names}
        for node in self.names:
            self.call(node, "configure", {"peers": peers, "brokers": ["relay-a", "relay-b"]
                                         if self.mode == "broker" else [], "enabled": value})

    def seed(self, text="evidence from a local inbox"):
        return self.call("builder", "publish", {"command_id": "initial-input", "kind": "work_seed",
                    "content": {"protocol": "peer-work-v1", "run_id": "local-chain", "generation": 0, "text": text}})

    def work(self, node):
        return self.call(node, "state")["work"]

    def published(self, node):
        value = self.work(node)
        return value if value and value["phase"] == "published" else None

    def in_phase(self, node, phase):
        value = self.work(node)
        return value if value and value["phase"] == phase else None

    def wait(self, predicate, timeout=15):
        deadline, wake, last = time.monotonic() + timeout, threading.Event(), None
        while time.monotonic() < deadline:
            try:
                last = predicate()
                if last:
                    return last
            except (OSError, ValueError) as exc:
                last = type(exc).__name__
            wake.wait(min(0.02, max(0, deadline - time.monotonic())))
        raise AssertionError(f"Predicate timeout ({last}); retained {self.root}")

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
            "mode": self.mode, "members": self.names, "finished_at": time.time(),
            "incarnations": [{**{k: v for k, v in row.items() if k != "process"},
                              "returncode": row["process"].returncode} for row in self.history]}, indent=2))

    def lookup(self, node, work):
        return authority_request(self.ports["authority"], node, self.keys[node], uuid.uuid4().hex,
                                 "lookup", {"request_id": work["intent"]["action_id"] + ":dispatch"},
                                 run_id="local-chain")

    def authority_snapshot(self):
        # Output-only trusted observer read; these facts never enter a peer view.
        with sqlite3.connect(f"file:{self.root / 'authority' / 'authority.sqlite'}?mode=ro", uri=True) as db:
            actions = db.execute("SELECT principal,action_id,state FROM authority_actions ORDER BY principal,action_id").fetchall()
            reservations = db.execute("SELECT amount,spent,state FROM reservations ORDER BY id").fetchall()
        return actions, reservations

    def invocation_entries(self):
        with sqlite3.connect(f"file:{self.root / 'authority' / 'authority.sqlite'}?mode=ro", uri=True) as db:
            return db.execute("SELECT principal,action_id,request_sha,fixture FROM authority_invocations ORDER BY principal,action_id").fetchall()


@contextmanager
def work_cluster(**kwargs):
    cluster = WorkCluster(**kwargs)
    try:
        yield cluster
    finally:
        cluster.close()


class PeerWorkProcessTests(unittest.TestCase):
    def assert_chain(self, c, seed):
        builder = c.wait(lambda: c.published("builder"))
        reviewer = c.wait(lambda: c.published("reviewer"))
        self.assertEqual(builder["intent"]["prerequisite_event_ids"], [seed["event_id"]])
        self.assertEqual(set(reviewer["intent"]["prerequisite_event_ids"]),
                         {seed["event_id"], builder["event_id"]})
        self.assertEqual(reviewer["intent"]["request"]["text"], builder["result"]["text"])
        self.assertEqual(reviewer["result"]["decision"], "approve")
        self.assertEqual(reviewer["result"]["text_sha256"], builder["result"]["text_sha256"])
        for node, work in (("builder", builder), ("reviewer", reviewer)):
            self.assertEqual(c.lookup(node, work), work["response"])
            self.assertEqual(work["response"]["usage_units"], 0)
            self.assertIsNone(c.call(node, "state")["work_error"])
        self.assertEqual(c.invocation_entries(), [
            ("builder", builder["intent"]["action_id"], builder["intent"]["request_sha256"], "text_build"),
            ("reviewer", reviewer["intent"]["action_id"], reviewer["intent"]["request_sha256"], "text_review")])
        return builder, reviewer

    def test_partitioned_reviewer_cannot_act_until_real_evidence_arrives(self):
        with work_cluster() as c:
            c.call("builder", "block", {"peers": ["reviewer"]})
            c.call("reviewer", "block", {"peers": ["builder"]})
            seed = c.seed()
            c.enable(True)
            builder = c.wait(lambda: c.published("builder"))
            c.wait(lambda: c.call("reviewer", "state")["routing"]["round"] >= 3)
            isolated = c.call("reviewer", "state")
            self.assertIsNone(isolated["work"])
            self.assertEqual(isolated["events"], [])
            c.call("builder", "block", {"peers": []})
            c.call("reviewer", "block", {"peers": []})
            _, reviewer = self.assert_chain(c, seed)
            self.assertIn(builder["event_id"], reviewer["intent"]["visible_event_ids"])
            self.assertEqual(len({row["pid"] for row in c.history}), 3)

    def test_same_local_policy_runs_through_durable_broker_adapter(self):
        with work_cluster(mode="broker") as c:
            seed = c.seed()
            c.enable(True)
            self.assert_chain(c, seed)
            self.assertEqual(len(c.processes), 5)

    def test_worker_result_survives_death_before_outbox_publication(self):
        with work_cluster(worker_crash="after_result") as c:
            seed = c.seed()
            old_pid = c.processes["builder"].pid
            c.enable(True)
            c.wait(lambda: c.processes["builder"].poll() == 81)
            saved = WorkJournal(c.root / "builder" / "work.sqlite", "builder", c.work_config("builder")).read()
            self.assertEqual(saved["phase"], "result")
            before_stats = json.loads((c.root / "builder" / "work-telemetry.json").read_text())
            self.assertNotEqual(c.start("builder"), old_pid)
            builder, _ = self.assert_chain(c, seed)
            self.assertEqual(builder["result"], saved["result"])
            self.assertEqual(builder["response"], saved["response"])
            stats = c.call("builder", "state")["work_telemetry"]
            self.assertEqual(stats["boots"], 2)
            self.assertEqual(stats["rpc_attempts"], before_stats["rpc_attempts"])
            self.assertEqual(stats["rpc_replies"], before_stats["rpc_replies"])

    def test_publish_before_local_ack_is_idempotent_after_actual_restart(self):
        with work_cluster(worker_crash="after_publish") as c:
            seed = c.seed()
            c.enable(True)
            c.wait(lambda: c.processes["builder"].poll() == 82)
            saved = WorkJournal(c.root / "builder" / "work.sqlite", "builder", c.work_config("builder")).read()
            self.assertEqual(saved["phase"], "result")
            self.assertIsNotNone(saved["outbox"])
            c.start("builder")
            builder, _ = self.assert_chain(c, seed)
            events = c.call("builder", "state")["events"]
            results = [row for row in events if row["producer"] == "builder" and row["kind"] == "work_result"]
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0]["event_id"], builder["event_id"])
            self.assertEqual(c.call("builder", "state")["work_telemetry"]["boots"], 2)

    def test_authority_outage_preserves_prepared_action_until_same_service_returns(self):
        with work_cluster() as c:
            c.kill("authority")
            seed = c.seed()
            c.enable(True)
            prepared = c.wait(lambda: c.work("builder"))
            self.assertEqual(prepared["phase"], "prepared")
            c.wait(lambda: c.call("builder", "state")["work_telemetry"]["transport_errors"] > 0)
            failed = c.call("builder", "state")["work_telemetry"]
            self.assertTrue(failed["pending_transport_error"])
            c.start_authority()
            builder, _ = self.assert_chain(c, seed)
            self.assertEqual(builder["intent"], prepared["intent"])
            recovered = c.call("builder", "state")["work_telemetry"]
            self.assertFalse(recovered["pending_transport_error"])
            self.assertGreaterEqual(recovered["transport_errors"], failed["transport_errors"])

    def test_observer_has_no_run_action_or_final_acceptance_rpc(self):
        with work_cluster() as c:
            for operation in ("run_action", "accept", "release", "settle"):
                with self.subTest(operation=operation), self.assertRaises(ValueError):
                    c.call("builder", operation, {})
            self.assertIsNone(c.work("builder"))
            self.assertIsNone(c.work("reviewer"))

    def test_lost_authority_completion_ack_replays_same_request_after_service_death(self):
        with work_cluster() as c:
            c.kill("authority")
            seed = c.seed()
            c.enable(True)
            prepared = c.wait(lambda: c.work("builder"))
            command = prepared["intent"]["action_id"] + ":dispatch"
            c.start_authority(crash="after_commit", request_id=command)
            c.wait(lambda: c.processes["authority"].poll() == 70)
            actions, reservations = c.authority_snapshot()
            self.assertEqual(len(actions), 1)
            self.assertEqual(actions[0][2], "completed")
            self.assertEqual(reservations, [(2, 0, "settled")])
            self.assertEqual(len(c.invocation_entries()), 1)
            c.start_authority()
            builder, _ = self.assert_chain(c, seed)
            self.assertEqual(builder["intent"], prepared["intent"])
            actions, reservations = c.authority_snapshot()
            self.assertEqual(len(actions), 2)
            self.assertEqual(reservations, [(2, 0, "settled"), (2, 0, "settled")])

    def test_unknown_authority_dispatch_stops_local_action_and_retains_reservation(self):
        for crash_point, expected_entries in (("after_intent", 0), ("after_entry", 1)):
            with self.subTest(crash_point=crash_point), work_cluster() as c:
                c.kill("authority")
                c.seed()
                c.enable(True)
                prepared = c.wait(lambda: c.work("builder"))
                command = prepared["intent"]["action_id"] + ":dispatch"
                c.start_authority(crash=crash_point, request_id=command)
                c.wait(lambda: c.processes["authority"].poll() == 70)
                c.start_authority()
                unknown = c.wait(lambda: c.in_phase("builder", "unknown"))
                self.assertEqual(unknown["intent"], prepared["intent"])
                before_round = c.call("builder", "state")["routing"]["round"]
                c.kill("builder")
                c.start("builder")
                c.wait(lambda: c.call("builder", "state")["routing"]["round"] >= before_round + 3)
                self.assertEqual(c.work("builder")["phase"], "unknown")
                self.assertIsNone(c.work("reviewer"))
                actions, reservations = c.authority_snapshot()
                self.assertEqual(len(actions), 1)
                self.assertEqual(actions[0][2], "unknown")
                self.assertEqual(reservations, [(2, None, "reserved")])
                entries = c.invocation_entries()
                self.assertEqual(len(entries), expected_entries)
                if entries:
                    self.assertEqual(entries[0][1:3], (prepared["intent"]["action_id"],
                                                     prepared["intent"]["request_sha256"]))
                self.assertFalse(any(row["kind"] == "work_result" for row in c.call("builder", "state")["events"]))

    def test_historical_claim_after_worker_death_does_not_grant_new_dispatch(self):
        with work_cluster(worker_crash="after_claim", lease_seconds=0.2) as c:
            c.seed()
            c.enable(True)
            c.wait(lambda: c.processes["builder"].poll() == 80)
            saved = WorkJournal(c.root / "builder" / "work.sqlite", "builder", c.work_config("builder")).read()
            self.assertEqual(saved["phase"], "claimed")
            c.wait(lambda: time.time() > saved["claim"]["lease"]["expires_at"] + 0.01)
            c.start("builder")
            denied = c.wait(lambda: c.in_phase("builder", "denied"))
            self.assertEqual(denied["response"]["reason"], "unavailable")
            self.assertEqual(c.authority_snapshot(), ([], []))
