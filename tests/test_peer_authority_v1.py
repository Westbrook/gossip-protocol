"""Authority contracts and retained, real-process crash/restart qualification.

No model, Docker, candidate execution, or inherited credentials are used. Process
tests own at most two authority children and poll observable state with deadlines.
"""
from __future__ import annotations

from contextlib import closing, contextmanager
from copy import deepcopy
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

from gossip_harness import peer_authority_v1 as authority_module
from gossip_harness.peer_authority_v1 import (
    Authority, AuthorityError, _signed, authority_request, request_digest,
)


ROOT = Path(__file__).resolve().parents[1]
PROTOCOL = "peer-authority-v1"


def configuration(*, lease_seconds=5, budget_units=12):
    return {
        "protocol": PROTOCOL,
        "run_id": "authority-test-run",
        "budget_units": budget_units,
        "lease_seconds": lease_seconds,
        "principals": {
            "alpha": {"key": "alpha-" + "a" * 32, "tasks": ["build", "review"]},
            "beta": {"key": "beta-" + "b" * 32, "tasks": ["build", "review"]},
            "reviewer": {"key": "reviewer-" + "c" * 32, "tasks": ["review"]},
        },
        "tasks": {
            "build": {"fixture": "text_build", "reservation_units": 6},
            "review": {"fixture": "text_review", "reservation_units": 2},
        },
    }


def fixture_request(text="Review this immutable proposal."):
    return {"text": text, "context": {
        "source_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
        "event_ids": [hashlib.sha256(b"fixture evidence").hexdigest()],
    }}


def dispatch_payload(epoch, *, task_id="build", action_id="action-one", request=None):
    value = fixture_request() if request is None else request
    return {"task_id": task_id, "epoch": epoch, "action_id": action_id,
            "request_sha256": request_digest(value), "request": value}


def envelope(config, principal, request_id, operation, payload):
    return _signed({"protocol": PROTOCOL, "run_id": config["run_id"], "principal": principal,
                    "request_id": request_id, "operation": operation, "payload": payload},
                   config["principals"][principal]["key"])


class Clock:
    def __init__(self):
        self.now = 100.0

    def __call__(self):
        return self.now


class AuthorityProcess:
    def __init__(self, config=None):
        self.root = ROOT / "runs" / "peer-authority-fixtures" / uuid.uuid4().hex
        self.root.mkdir(parents=True)
        self.config = configuration(lease_seconds=2) if config is None else config
        self.port = 0
        self.process = None
        self.logs, self.history = [], []
        self.started = time.time()

    def spawn(self, *, crash_point=None, crash_request_id=None, port=None):
        path = self.root / ("process-config-" + uuid.uuid4().hex + ".json")
        path.write_text(json.dumps({"root": str(self.root / "authority"),
                                    "port": self.port if port is None else port,
                                    "authority": self.config}))
        stderr = (self.root / ("stderr-" + uuid.uuid4().hex + ".log")).open("w")
        self.logs.append(stderr)
        argv = [sys.executable, "-m", "gossip_harness.peer_authority_v1", "--config", str(path)]
        if crash_point is not None:
            argv += ["--crash-point", crash_point, "--crash-request-id", crash_request_id]
        process = subprocess.Popen(argv, cwd=ROOT, env={
            "PATH": os.defpath, "PYTHONPATH": str(ROOT), "PYTHONHASHSEED": "0",
            "PYTHONDONTWRITEBYTECODE": "1", "LANG": "C.UTF-8",
        }, stdout=subprocess.PIPE, stderr=stderr, text=True)
        self.history.append({"process": process, "pid": process.pid,
                             "started_at_unix": time.time(), "config": path.name,
                             "stderr": Path(stderr.name).name, "crash_point": crash_point,
                             "crash_request_id": crash_request_id})
        return process

    def start(self, **kwargs):
        if self.process is not None and self.process.poll() is None:
            raise AssertionError("The current authority must stop before restart")
        self.process = self.spawn(**kwargs)
        ready, _, _ = select.select([self.process.stdout], [], [], 10)
        if not ready:
            raise AssertionError(f"Authority readiness deadline; retained {self.root}")
        line = self.process.stdout.readline()
        if not line:
            raise AssertionError(f"Authority exited before readiness; retained {self.root}")
        value = json.loads(line)
        if value["protocol"] != PROTOCOL or value["pid"] != self.process.pid:
            raise AssertionError(f"Unexpected authority identity; retained {self.root}")
        if self.port and value["port"] != self.port:
            raise AssertionError(f"Restart changed authority port; retained {self.root}")
        self.port = value["port"]
        self.history[-1]["ready_port"] = self.port
        return self.process.pid

    def call(self, request_id, operation, payload, *, principal="alpha"):
        return authority_request(self.port, principal, self.config["principals"][principal]["key"],
                                 request_id, operation, payload, run_id=self.config["run_id"])

    def wait(self, predicate, timeout=8):
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
        raise AssertionError(f"Authority predicate deadline ({last}); retained {self.root}")

    def stop(self):
        if self.process is not None:
            if self.process.poll() is None:
                self.process.kill()
            self.process.wait(timeout=5)

    def fixture_entry_count(self, *, principal="alpha", action_id="action-one"):
        """Private durable entry evidence; it does not prove provider invocation."""
        if self.process is not None and self.process.poll() is None:
            raise AssertionError("Stop the owned authority before reading its private journal")
        path = self.root / "authority" / "authority.sqlite"
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
            return db.execute("SELECT COUNT(*) FROM authority_invocations WHERE principal=? AND action_id=?",
                              (principal, action_id)).fetchone()[0]

    def close(self):
        for item in self.history:
            process = item["process"]
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)
            if process.stdout:
                process.stdout.close()
        for log in self.logs:
            log.close()
        (self.root / "lifecycle.json").write_text(json.dumps({
            "started_at_unix": self.started, "finished_at_unix": time.time(),
            "incarnations": [{**{key: value for key, value in item.items() if key != "process"},
                              "returncode": item["process"].returncode} for item in self.history],
        }, indent=2))


@contextmanager
def authority_process(config=None, **kwargs):
    process = AuthorityProcess(config)
    try:
        process.start(**kwargs)
        yield process
    finally:
        process.close()


class PeerAuthorityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.config = configuration()
        self.clock = Clock()
        self.authority = None
        self.addCleanup(self.cleanup_authority)

    def cleanup_authority(self):
        if self.authority is not None:
            self.authority.close()
        self.temp.cleanup()

    def open(self, **kwargs):
        if self.authority is not None:
            self.authority.close()
        self.authority = Authority(self.root, self.config, clock=self.clock, **kwargs)
        return self.authority

    def call(self, request_id, operation, payload, *, principal="alpha"):
        if self.authority is None:
            self.open()
        signed = envelope(self.config, principal, request_id, operation, payload)
        reply = self.authority.handle(signed)
        body = reply["body"]
        self.assertEqual(reply, _signed(body, self.config["principals"][principal]["key"]))
        self.assertEqual(body["protocol"], PROTOCOL)
        self.assertEqual(body["principal"], principal)
        self.assertEqual(body["request_id"], request_id)
        self.assertEqual(body["run_id"], self.config["run_id"])
        self.assertEqual(body["rpc_sha256"], request_digest(signed["body"]))
        self.assertEqual(body["receipt"]["run_id"], self.config["run_id"])
        self.assertRegex(body["receipt"]["config_sha256"], r"^[0-9a-f]{64}$")
        return body["receipt"]

    def assert_rejected(self, receipt):
        self.assertEqual(receipt["status"], "rejected")
        self.assertIsInstance(receipt["reason"], str)
        self.assertTrue(receipt["reason"])
        self.assertLessEqual(len(receipt["reason"]), 256)

    def test_authentication_run_binding_and_task_scope(self):
        self.open()
        original = envelope(self.config, "alpha", "claim-one", "claim", {"task_id": "build"})
        tampered = deepcopy(original)
        tampered["body"]["principal"] = "beta"
        with self.assertRaises(AuthorityError):
            self.authority.handle(tampered)
        wrong_run = deepcopy(original["body"])
        wrong_run["run_id"] = "a-different-run"
        with self.assertRaises(AuthorityError):
            self.authority.handle(_signed(wrong_run, self.config["principals"]["alpha"]["key"]))
        self.assert_rejected(self.call("scope-denied", "claim", {"task_id": "build"},
                                       principal="reviewer"))
        accepted = self.call("claim-one", "claim", {"task_id": "build"})
        self.assertEqual(accepted["status"], "ok")
        self.assertEqual(accepted["lease"], {"task_id": "build", "worker_id": "alpha",
                                            "epoch": 1, "expires_at": 105.0})

    def test_claim_replay_renewal_and_competing_owner(self):
        first = self.call("claim", "claim", {"task_id": "build"})
        self.clock.now += 1
        self.assertEqual(self.call("claim", "claim", {"task_id": "build"}), first)
        self.assert_rejected(self.call("competing", "claim", {"task_id": "build"}, principal="beta"))
        renewed = self.call("renew", "renew", {"task_id": "build", "epoch": 1})
        self.assertEqual(renewed["status"], "ok")
        self.assertEqual(renewed["lease"]["expires_at"], 106.0)
        self.assertEqual(renewed["lease"]["epoch"], 1)
        self.assert_rejected(self.call("foreign-renew", "renew", {"task_id": "build", "epoch": 1},
                                       principal="beta"))
        self.assertEqual(self.call("validate", "validate", {"task_id": "build", "epoch": 1})["status"], "ok")

    def test_expiry_fences_old_epoch_and_clock_rollback_cannot_extend_it(self):
        self.call("claim", "claim", {"task_id": "build"})
        self.clock.now = 104.0
        observed = self.call("observe-clock", "validate", {"task_id": "build", "epoch": 1})
        self.clock.now = 90.0
        backward = self.call("backward-clock", "validate", {"task_id": "build", "epoch": 1})
        self.assertGreaterEqual(backward["authority_time"], observed["authority_time"])
        self.clock.now = 105.0
        self.assert_rejected(self.call("expired", "validate", {"task_id": "build", "epoch": 1}))
        replacement = self.call("replace", "claim", {"task_id": "build"}, principal="beta")
        self.assertEqual(replacement["status"], "ok")
        self.assertEqual(replacement["lease"]["epoch"], 2)
        self.clock.now = 80.0
        self.open()
        self.assert_rejected(self.call("stale-renew", "renew", {"task_id": "build", "epoch": 1}))
        current = self.call("current", "validate", {"task_id": "build", "epoch": 2}, principal="beta")
        self.assertEqual(current["status"], "ok")
        self.assertGreaterEqual(current["authority_time"], replacement["authority_time"])

    def test_request_id_is_full_payload_bound_and_replay_survives_reopen(self):
        first = self.call("bound-id", "claim", {"task_id": "build"})
        self.assert_rejected(self.call("bound-id", "claim", {"task_id": "review"}))
        self.assert_rejected(self.call("bound-id", "validate", {"task_id": "build", "epoch": 1}))
        self.clock.now = 102.0
        self.open()
        self.assertEqual(self.call("bound-id", "claim", {"task_id": "build"}), first)
        self.assertEqual(self.call("lookup-original", "lookup", {"request_id": "bound-id"}), first)
        self.assertEqual(self.call("private-lookup", "lookup", {"request_id": "bound-id"},
                                   principal="beta")["status"], "missing")
        self.assertEqual(self.call("bound-id", "claim", {"task_id": "review"},
                                   principal="beta")["status"], "ok")

    def test_fixture_executes_bound_request_and_completed_receipt_replays(self):
        self.call("claim", "claim", {"task_id": "build"})
        payload = dispatch_payload(1, request=fixture_request("Mixed case café"))
        completed = self.call("dispatch", "dispatch", payload)
        self.assertEqual(completed["status"], "completed")
        self.assertEqual(completed["usage_units"], 0)
        self.assertEqual(completed["result"], {"text": "MIXED CASE CAFÉ",
                         "text_sha256": hashlib.sha256("MIXED CASE CAFÉ".encode()).hexdigest()})
        self.clock.now = 200.0
        self.open()
        self.assertEqual(self.call("dispatch", "dispatch", payload), completed)
        self.assertEqual(self.call("lookup-completed", "lookup", {"request_id": "dispatch"}), completed)

    def test_action_id_cannot_be_aliased_and_other_principal_namespace_is_independent(self):
        self.call("claim-build", "claim", {"task_id": "build"})
        self.call("claim-review", "claim", {"task_id": "review"}, principal="beta")
        payload = dispatch_payload(1)
        completed = self.call("dispatch", "dispatch", payload)
        self.assertEqual(completed["status"], "completed")
        alias = self.call("alias", "dispatch", payload)
        self.assert_rejected(alias)
        self.assertEqual(alias["reason"], "action_conflict")
        foreign = self.call("foreign-action", "dispatch", dispatch_payload(1, task_id="review"),
                            principal="beta")
        self.assertEqual(foreign["status"], "completed")
        self.assertNotEqual(foreign["reservation_id"], completed["reservation_id"])
        self.assertEqual(self.call("dispatch", "dispatch", payload), completed)

    def test_failed_budget_reservation_rolls_back_action_ownership(self):
        self.config["budget_units"] = 3
        self.call("claim-build", "claim", {"task_id": "build"})
        self.call("claim-review", "claim", {"task_id": "review"})
        denied = self.call("over-budget", "dispatch", dispatch_payload(1))
        self.assert_rejected(denied)
        completed = self.call("within-budget", "dispatch", dispatch_payload(1, task_id="review"))
        self.assertEqual(completed["status"], "completed")
        self.assertEqual(completed["usage_units"], 0)
        self.assertEqual(self.call("over-budget", "dispatch", dispatch_payload(1)), denied)

    def test_digest_mismatch_rejection_cannot_consume_action_id(self):
        self.call("claim", "claim", {"task_id": "build"})
        payload = dispatch_payload(1)
        corrupt = deepcopy(payload)
        corrupt["request"] = fixture_request("Changed after hashing")
        self.assert_rejected(self.call("wrong-digest", "dispatch", corrupt))
        self.assertEqual(self.call("valid-digest", "dispatch", payload)["status"], "completed")

    def test_valid_shape_source_hash_must_bind_actual_utf8_text(self):
        self.call("claim", "claim", {"task_id": "build"})
        request = fixture_request("text to bind: café")
        request["context"]["source_sha256"] = hashlib.sha256(b"a different source").hexdigest()
        self.assert_rejected(self.call("wrong-source", "dispatch", dispatch_payload(1, request=request)))
        valid = dispatch_payload(1, request=fixture_request("text to bind: café"))
        self.assertEqual(self.call("valid-source", "dispatch", valid)["status"], "completed")

    def test_request_context_is_strict_bounded_and_content_bound(self):
        self.call("claim", "claim", {"task_id": "build"})
        valid = fixture_request()
        invalid = []
        value = deepcopy(valid)
        value["text"] = "é" * 2049
        value["context"]["source_sha256"] = hashlib.sha256(value["text"].encode()).hexdigest()
        invalid.append(value)
        value = deepcopy(valid)
        value["context"]["event_ids"] *= 2
        invalid.append(value)
        value = deepcopy(valid)
        value["context"]["event_ids"] = [hashlib.sha256(str(i).encode()).hexdigest() for i in range(33)]
        invalid.append(value)
        value = deepcopy(valid)
        value["context"]["source_sha256"] = "not-a-digest"
        invalid.append(value)
        value = deepcopy(valid)
        value["context"]["hidden_instructions"] = "must not become authority input"
        invalid.append(value)
        value = deepcopy(valid)
        value["extra"] = "unknown field"
        invalid.append(value)
        for index, value in enumerate(invalid):
            with self.subTest(index=index):
                payload = {"task_id": "build", "epoch": 1, "action_id": "bounded-action",
                           "request_sha256": hashlib.sha256(json.dumps(value, sort_keys=True,
                               separators=(",", ":"), ensure_ascii=False).encode()).hexdigest(),
                           "request": value}
                self.assert_rejected(self.call(f"invalid-{index}", "dispatch", payload))
        boundary = fixture_request("é" * 2048)
        self.assertEqual(self.call("boundary", "dispatch", dispatch_payload(
            1, action_id="bounded-action", request=boundary))["status"], "completed")

    def test_peer_cannot_settle_expand_budget_or_query_other_tasks(self):
        for operation in ("settle", "budget", "increase_budget", "state", "tasks", "accept", "execute"):
            with self.subTest(operation=operation):
                self.assert_rejected(self.call(f"forbidden-{operation}", operation, {}))
        self.assertEqual(self.call("allowed", "claim", {"task_id": "build"})["status"], "ok")

    def test_config_identity_change_is_rejected_without_losing_receipts(self):
        original = self.call("claim", "claim", {"task_id": "build"})
        self.authority.close()
        self.authority = None
        changed = deepcopy(self.config)
        changed["principals"]["alpha"]["tasks"] = ["review"]
        with self.assertRaises(AuthorityError):
            Authority(self.root, changed, clock=self.clock)
        self.open()
        self.assertEqual(self.call("claim", "claim", {"task_id": "build"}), original)

    def test_closed_authority_cannot_handle_requests_after_replacement_acquires_owner(self):
        old = self.open()
        original = self.call("claim", "claim", {"task_id": "build"})
        old.close()
        self.authority = None
        self.open()
        with self.assertRaises(AuthorityError):
            old.handle(envelope(self.config, "alpha", "after-close", "claim", {"task_id": "review"}))
        self.assertEqual(self.call("claim", "claim", {"task_id": "build"}), original)
        self.assertEqual(self.call("after-close", "claim", {"task_id": "review"})["status"], "ok")

    def test_close_waits_for_active_fixture_and_keeps_exclusive_owner_until_quiescent(self):
        old = self.open()
        self.call("claim", "claim", {"task_id": "build"})
        entered, release = threading.Event(), threading.Event()
        close_waiting, close_finished = threading.Event(), threading.Event()
        outcomes, errors = [], []
        original_fixture = authority_module._fixture
        original_wait = old.lifecycle.wait

        def blocked_fixture(kind, request):
            entered.set()
            if not release.wait(timeout=5):
                raise AssertionError("Fixture release deadline")
            return original_fixture(kind, request)

        def observed_wait(timeout=None):
            close_waiting.set()
            return original_wait(timeout)

        def dispatch():
            try:
                outcomes.append(old.handle(envelope(self.config, "alpha", "dispatch", "dispatch",
                                                     dispatch_payload(1))))
            except BaseException as error:
                errors.append(error)

        def close():
            try:
                old.close()
            except BaseException as error:
                errors.append(error)
            finally:
                close_finished.set()

        worker = threading.Thread(target=dispatch, daemon=True)
        closer = threading.Thread(target=close, daemon=True)
        with patch.object(authority_module, "_fixture", side_effect=blocked_fixture), \
                patch.object(old.lifecycle, "wait", side_effect=observed_wait):
            worker.start()
            try:
                self.assertTrue(entered.wait(timeout=3), "Dispatch never entered fixture")
                closer.start()
                self.assertTrue(close_waiting.wait(timeout=3), "Close never waited for active handler")
                with old.lifecycle:
                    self.assertTrue(old.closing)
                    self.assertFalse(old.closed)
                    self.assertEqual(old.active_handlers, 1)
                self.assertFalse(close_finished.is_set())
                with self.assertRaises(AuthorityError):
                    Authority(self.root, self.config, clock=self.clock)
                with self.assertRaises(AuthorityError):
                    old.handle(envelope(self.config, "alpha", "during-close", "claim", {"task_id": "review"}))
            finally:
                release.set()
                worker.join(timeout=5)
                if closer.ident is not None:
                    closer.join(timeout=5)
        self.assertFalse(worker.is_alive())
        self.assertFalse(closer.is_alive())
        self.assertTrue(close_finished.is_set())
        self.assertEqual(errors, [])
        self.assertEqual(outcomes[0]["body"]["receipt"]["status"], "completed")
        self.authority = None
        self.open()
        with self.assertRaises(AuthorityError):
            old.handle(envelope(self.config, "alpha", "after-close", "claim", {"task_id": "review"}))
        self.assertEqual(self.call("replacement", "claim", {"task_id": "review"})["status"], "ok")


class PeerAuthorityProcessTests(unittest.TestCase):
    def crash_call(self, process, request_id, operation, payload):
        with self.assertRaises((OSError, ValueError, EOFError)):
            process.call(request_id, operation, payload)
        self.assertNotEqual(process.process.wait(timeout=5), 0)

    def test_real_socket_rejects_bad_capability_without_mutating_claim(self):
        with authority_process() as process:
            with self.assertRaises((OSError, ValueError, EOFError)):
                authority_request(process.port, "alpha", "invalid-capability-000000000000",
                                  "unauthenticated", "claim", {"task_id": "build"},
                                  run_id=process.config["run_id"])
            accepted = process.call("authenticated", "claim", {"task_id": "build"})
            self.assertEqual(accepted["status"], "ok")
            self.assertEqual(accepted["lease"]["epoch"], 1)
            self.assertNotEqual(process.process.pid, os.getpid())

    def test_claim_crash_before_commit_leaves_no_receipt_or_lease(self):
        with authority_process(crash_point="before_commit", crash_request_id="crash-claim") as process:
            pid = process.process.pid
            self.crash_call(process, "crash-claim", "claim", {"task_id": "build"})
            self.assertNotEqual(process.start(), pid)
            self.assertEqual(process.call("lookup", "lookup", {"request_id": "crash-claim"})["status"],
                             "missing")
            first = process.call("crash-claim", "claim", {"task_id": "build"})
            self.assertEqual(first["status"], "ok")
            self.assertEqual(first["lease"]["epoch"], 1)

    def test_claim_crash_after_commit_preserves_exact_lost_acknowledgement(self):
        with authority_process(crash_point="after_commit", crash_request_id="crash-claim") as process:
            self.crash_call(process, "crash-claim", "claim", {"task_id": "build"})
            process.start()
            saved = process.call("lookup", "lookup", {"request_id": "crash-claim"})
            self.assertEqual(saved["status"], "ok")
            self.assertEqual(saved["lease"]["epoch"], 1)
            self.assertEqual(process.call("crash-claim", "claim", {"task_id": "build"}), saved)
            self.assertEqual(process.call("foreign", "lookup", {"request_id": "crash-claim"},
                                          principal="beta")["status"], "missing")

    def test_dispatch_crash_before_commit_can_be_retried_without_ambiguous_reservation(self):
        config = configuration(lease_seconds=10, budget_units=6)
        with authority_process(config, crash_point="before_commit", crash_request_id="crash-dispatch") as process:
            lease = process.call("claim", "claim", {"task_id": "build"})["lease"]
            payload = dispatch_payload(lease["epoch"])
            self.crash_call(process, "crash-dispatch", "dispatch", payload)
            self.assertEqual(process.fixture_entry_count(), 0)
            process.start()
            self.assertEqual(process.call("lookup", "lookup", {"request_id": "crash-dispatch"})["status"],
                             "missing")
            completed = process.call("crash-dispatch", "dispatch", payload)
            self.assertEqual(completed["status"], "completed")
            self.assertEqual(completed["usage_units"], 0)
            self.assertEqual(process.call("crash-dispatch", "dispatch", payload), completed)
            process.stop()
            self.assertEqual(process.fixture_entry_count(), 1)

    def test_ambiguous_dispatch_crashes_fence_reexecution_reclaim_and_budget(self):
        for point, expected_entries in (("after_intent", 0), ("after_entry", 1),
                                        ("after_fixture", 1), ("before_result_commit", 1)):
            with self.subTest(point=point), authority_process(
                configuration(lease_seconds=2, budget_units=6),
                crash_point=point, crash_request_id="crash-dispatch",
            ) as process:
                lease = process.call("claim", "claim", {"task_id": "build"})["lease"]
                payload = dispatch_payload(lease["epoch"])
                self.crash_call(process, "crash-dispatch", "dispatch", payload)
                self.assertEqual(process.fixture_entry_count(), expected_entries)
                process.start(crash_point=point, crash_request_id="crash-dispatch")
                unknown = process.call("lookup", "lookup", {"request_id": "crash-dispatch"})
                self.assertEqual(unknown["status"], "unknown")
                self.assertNotIn("result", unknown)
                self.assertEqual(process.call("crash-dispatch", "dispatch", payload), unknown)
                self.assertIsNone(process.process.poll())
                alias = process.call("new-request-same-action", "dispatch", payload)
                self.assertEqual(alias["status"], "rejected")
                new_action = process.call("new-request-new-action", "dispatch", dispatch_payload(
                    lease["epoch"], action_id="must-stay-fenced"))
                self.assertEqual(new_action["status"], "rejected")
                other = process.call("claim-review", "claim", {"task_id": "review"})
                self.assertEqual(other["status"], "ok")
                over_budget = process.call("review-dispatch", "dispatch", dispatch_payload(
                    other["lease"]["epoch"], task_id="review", action_id="review-action"))
                self.assertEqual(over_budget["status"], "rejected")
                self.assertEqual(over_budget["reason"], "budget")

                def expiry_observed():
                    receipt = process.call("observe-" + uuid.uuid4().hex, "renew",
                                           {"task_id": "review", "epoch": other["lease"]["epoch"]})
                    self.assertEqual(receipt["status"], "ok")
                    return receipt["authority_time"] >= lease["expires_at"]

                process.wait(expiry_observed)
                reclaim = process.call("reclaim", "claim", {"task_id": "build"}, principal="beta")
                self.assertEqual(reclaim["status"], "rejected")
                process.stop()
                process.start()
                self.assertEqual(process.call("after-second-restart", "lookup",
                                              {"request_id": "crash-dispatch"}), unknown)
                process.stop()
                self.assertEqual(process.fixture_entry_count(), expected_entries)

    def test_dispatch_crash_after_result_commit_preserves_completion(self):
        with authority_process(crash_point="after_commit", crash_request_id="crash-dispatch") as process:
            lease = process.call("claim", "claim", {"task_id": "build"})["lease"]
            payload = dispatch_payload(lease["epoch"], request=fixture_request("complete this"))
            self.crash_call(process, "crash-dispatch", "dispatch", payload)
            self.assertEqual(process.fixture_entry_count(), 1)
            process.start(crash_point="after_fixture", crash_request_id="crash-dispatch")
            completed = process.call("lookup", "lookup", {"request_id": "crash-dispatch"})
            self.assertEqual(completed["status"], "completed")
            self.assertEqual(completed["result"]["text"], "COMPLETE THIS")
            self.assertEqual(completed["usage_units"], 0)
            self.assertEqual(process.call("crash-dispatch", "dispatch", payload), completed)
            self.assertIsNone(process.process.poll())
            process.stop()
            self.assertEqual(process.fixture_entry_count(), 1)

    def test_lease_expiry_and_epoch_fencing_survive_real_restart(self):
        with authority_process() as process:
            first = process.call("claim", "claim", {"task_id": "build"})
            process.stop()
            process.start()

            def reclaim_when_expired():
                receipt = process.call("compete-" + uuid.uuid4().hex, "claim", {"task_id": "build"},
                                       principal="beta")
                return receipt if receipt["status"] == "ok" else None

            replacement = process.wait(reclaim_when_expired)
            self.assertEqual(replacement["lease"]["epoch"], first["lease"]["epoch"] + 1)
            stale = process.call("stale-dispatch", "dispatch", dispatch_payload(first["lease"]["epoch"]))
            self.assertEqual(stale["status"], "rejected")
            current = process.call("current-dispatch", "dispatch", dispatch_payload(
                replacement["lease"]["epoch"]), principal="beta")
            self.assertEqual(current["status"], "completed")

    def test_second_live_owner_is_rejected_before_serving_same_durable_state(self):
        with authority_process() as process:
            first = process.call("claim", "claim", {"task_id": "build"})
            duplicate = process.spawn(port=0)
            self.assertNotEqual(duplicate.wait(timeout=5), 0)
            self.assertEqual(duplicate.stdout.read(), "")
            stderr = (process.root / process.history[-1]["stderr"]).read_text()
            self.assertRegex(stderr.lower(), r"own|lock")
            self.assertIsNone(process.process.poll())
            self.assertEqual(process.call("claim", "claim", {"task_id": "build"}), first)
