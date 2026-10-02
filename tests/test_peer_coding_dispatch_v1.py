"""Offline coding-dispatch checks using the real worker with injected HTTP.

The in-memory payload boundary enforces principal ownership. These tests invoke
no provider, socket service, candidate code, Docker, or credential discovery.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch

from gossip_harness import peer_coding_dispatch_v1 as dispatch_module
from gossip_harness import peer_store_v1 as store_module
from gossip_harness import verification_journal as journal_module
from gossip_harness.ledger import ClaimRejected
from gossip_harness.peer_authority_v1 import Authority, _signed
from gossip_harness.peer_coding_dispatch_v1 import CodingDispatch, DispatchError, canonical_payload, source_digest
from gossip_harness.worker import HTTPResponse, MODEL, OpenAIWorker


KEY = "offline-test-key-never-a-provider-credential"
PROTOCOL = "peer-authority-v1"


class InjectedCrash(BaseException):
    """A stopped execution path, outside ordinary worker exception handling."""


class Clock:
    def __init__(self):
        self.now = 100.0

    def __call__(self):
        return self.now


class MemoryPayloads:
    def __init__(self):
        self.values = {}
        self.lock = threading.Lock()
        self.fail_publication = False

    def put_owned(self, principal, data):
        if self.fail_publication:
            raise OSError("Injected payload publication failure")
        sha = hashlib.sha256(data).hexdigest()
        with self.lock:
            self.values[principal, sha] = bytes(data)
        return sha

    def read_owned(self, principal, sha):
        with self.lock:
            try:
                return self.values[principal, sha]
            except KeyError:
                raise FileNotFoundError("Payload is unavailable to this principal") from None


def provider_response(proposal=None):
    if proposal is None:
        proposal = {"changes": [{"path": "src/add.py", "content": "def add(a, b): return a + b\n"}],
                    "summary": "Corrected addition."}
    return {"id": "resp_offline", "model": MODEL, "status": "completed", "service_tier": "default",
            "usage": {"input_tokens": 101, "output_tokens": 20, "total_tokens": 121},
            "output": [{"type": "message", "role": "assistant", "status": "completed",
                        "content": [{"type": "output_text", "text": proposal if isinstance(proposal, str)
                                     else json.dumps(proposal)}]}]}


class OfflineTransport:
    def __init__(self):
        self.response = provider_response()
        self.calls = []
        self.error = None
        self.entered = None
        self.release = None

    def __call__(self, request, timeout, maximum):
        self.calls.append({"body": json.loads(request.data), "timeout": timeout, "maximum": maximum})
        if self.entered is not None:
            self.entered.set()
            if not self.release.wait(timeout=5):
                raise AssertionError("Offline transport release deadline")
        if self.error is not None:
            raise self.error
        return HTTPResponse(200, {"X-Request-Id": "req_offline"}, json.dumps(self.response).encode())


def authority_config():
    return {
        "protocol": PROTOCOL, "run_id": "coding-dispatch-test", "budget_units": 1_000_000,
        "lease_seconds": 5,
        "principals": {
            "alpha": {"key": "alpha-" + "a" * 32, "tasks": ["build", "review"]},
            "beta": {"key": "beta-" + "b" * 32, "tasks": ["build", "review"]},
        },
        "tasks": {name: {"fixture": "text_build", "reservation_units": 1}
                  for name in ("build", "review")},
    }


def coding_payload(task_id="build"):
    files = {"src/add.py": "def add(a, b): return a - b\n"}
    return {"worker_request": {"task_id": task_id, "instructions": "Correct the addition function.",
                               "allowed_paths": ["src/add.py"], "files": files,
                               "base_sha": "a" * 40, "attempt": 1, "feedback": ""},
            "context": {"source_sha256": source_digest(files),
                        "event_ids": [hashlib.sha256(b"immutable evidence").hexdigest()]}}


class PeerCodingDispatchTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.clock = Clock()
        self.config = authority_config()
        self.payloads = MemoryPayloads()
        self.transport = OfflineTransport()
        self.worker = OpenAIWorker(KEY, max_output_tokens=64, timeout=2, transport=self.transport)
        self.workers = {"mini": self.worker}
        self.task_specs = {name: {"allowed_paths": ["src/add.py"], "profiles": ["mini"]}
                           for name in ("build", "review")}
        self.authority = None
        self.backend = None
        self.addCleanup(self.cleanup)

    def cleanup(self):
        if self.authority is not None:
            self.authority.close()
        self.temp.cleanup()

    def open(self, **kwargs):
        if self.authority is not None:
            self.authority.close()
        self.authority = Authority(self.root / "authority", self.config, clock=self.clock)
        self.backend = CodingDispatch(self.authority, self.payloads, self.workers, self.task_specs,
                                      journal_root=self.root / "journal", **kwargs)
        return self.backend

    def call_authority(self, request_id, operation, payload, *, principal="alpha"):
        if self.authority is None:
            self.open()
        body = {"protocol": PROTOCOL, "run_id": self.config["run_id"], "principal": principal,
                "request_id": request_id, "operation": operation, "payload": payload}
        return self.authority.handle(_signed(body, self.config["principals"][principal]["key"]))["body"]["receipt"]

    def claim(self, task_id="build", *, principal="alpha", request_id="claim"):
        receipt = self.call_authority(request_id, "claim", {"task_id": task_id}, principal=principal)
        self.assertEqual(receipt["status"], "ok")
        return receipt["lease"]["epoch"]

    def body(self, *, principal="alpha", task_id="build", epoch=1, action_id="coding-one",
             request_id="coding-request", profile_id="mini", value=None):
        data = canonical_payload(coding_payload(task_id) if value is None else value)
        sha = self.payloads.put_owned(principal, data)
        return {"protocol": PROTOCOL, "run_id": self.config["run_id"], "principal": principal,
                "request_id": request_id, "operation": "coding_dispatch",
                "payload": {"task_id": task_id, "epoch": epoch, "action_id": action_id,
                            "profile_id": profile_id, "request_sha256": sha}}

    def execute(self, body):
        receipt = self.backend.execute(body)
        self.assertEqual(receipt["run_id"], self.config["run_id"])
        self.assertRegex(receipt["config_sha256"], r"^[0-9a-f]{64}$")
        return receipt

    def result(self, receipt, *, principal="alpha"):
        data = self.payloads.read_owned(principal, receipt["result_sha256"])
        self.assertEqual(hashlib.sha256(data).hexdigest(), receipt["result_sha256"])
        value = json.loads(data)
        self.assertEqual(canonical_payload(value), data)
        self.assertEqual(set(value), {"kind", "payload"})
        self.assertEqual(value["kind"], "result" if receipt["status"] == "completed" else "failure")
        return value["payload"]

    def test_real_worker_result_is_bound_published_settled_and_replayed(self):
        self.claim()
        body = self.body()
        completed = self.execute(body)
        self.assertEqual(completed["status"], "completed")
        result = self.result(completed)
        self.assertEqual(result["changes"], {"src/add.py": "def add(a, b): return a + b\n"})
        self.assertEqual(result["usage_units"], 166)
        self.assertEqual(self.authority.ledger.budget()["spent_or_reserved"], 166)
        for name in ("dispatch_config_sha256", "profile_sha256", "worker_request_sha256", "payload_sha256"):
            self.assertRegex(completed[name], r"^[0-9a-f]{64}$")
        self.assertEqual(completed["payload_sha256"], body["payload"]["request_sha256"])
        self.assertEqual(self.execute(body), completed)
        self.assertEqual(self.call_authority("lookup", "lookup", {"request_id": "coding-request"}), completed)
        self.assertEqual(len(self.transport.calls), 1)
        submitted = json.loads(self.transport.calls[0]["body"]["input"])
        self.assertEqual(submitted["files"], coding_payload()["worker_request"]["files"])

    def test_payload_ownership_waits_without_reservation_then_exact_request_retries_after_arrival(self):
        self.claim()
        self.claim("review", principal="beta")
        body = self.body(value=coding_payload("review"), task_id="review")
        foreign = deepcopy(body)
        foreign["principal"] = "beta"
        self.assertEqual(self.execute(foreign)["status"], "waiting")
        missing = self.body(request_id="missing")
        missing_sha = missing["payload"]["request_sha256"]
        delayed = self.payloads.values.pop(("alpha", missing_sha))
        self.assertEqual(self.execute(missing)["status"], "waiting")
        self.assertEqual(self.transport.calls, [])
        self.assertEqual(self.authority.ledger.budget()["spent_or_reserved"], 0)
        self.assertEqual(self.call_authority("lookup-waiting", "lookup", {"request_id": "missing"})["status"],
                         "missing")
        self.assertEqual(self.call_authority("lookup-foreign", "lookup", {"request_id": "coding-request"},
                                            principal="beta")["status"], "missing")
        self.assertEqual(self.payloads.put_owned("alpha", delayed), missing_sha)
        self.assertEqual(self.execute(missing)["status"], "completed")
        data = self.payloads.read_owned("alpha", foreign["payload"]["request_sha256"])
        self.assertEqual(self.payloads.put_owned("beta", data), foreign["payload"]["request_sha256"])
        self.assertEqual(self.execute(foreign)["status"], "completed")
        self.assertEqual(len(self.transport.calls), 2)

    def test_request_namespace_is_shared_with_authority_and_payload_bound(self):
        self.claim(request_id="shared-id")
        conflict = self.body(request_id="shared-id")
        self.assertEqual(self.execute(conflict)["status"], "rejected")
        body = self.body()
        completed = self.execute(body)
        changed = deepcopy(body)
        changed["payload"]["action_id"] = "different-action"
        self.assertEqual(self.execute(changed)["status"], "rejected")
        self.assertEqual(self.execute(body), completed)
        self.assertEqual(len(self.transport.calls), 1)

    def test_action_alias_rejects_but_other_principal_has_independent_namespace(self):
        self.claim()
        self.claim("review", principal="beta")
        first = self.execute(self.body())
        alias = self.execute(self.body(request_id="alias"))
        self.assertEqual(alias["status"], "rejected")
        second = self.execute(self.body(principal="beta", task_id="review"))
        self.assertEqual(second["status"], "completed")
        self.assertNotEqual(first["reservation_id"], second["reservation_id"])
        self.assertNotEqual(first["call_id"], second["call_id"])
        self.assertEqual(len(self.transport.calls), 2)

    def test_scope_profile_source_and_strict_context_rejections_do_not_invoke(self):
        self.claim()
        values = []
        value = coding_payload()
        value["worker_request"]["allowed_paths"] = ["trusted/test_add.py"]
        values.append(value)
        value = coding_payload()
        value["context"]["source_sha256"] = "f" * 64
        values.append(value)
        value = coding_payload()
        value["context"]["event_ids"] *= 2
        values.append(value)
        value = coding_payload()
        value["worker_request"]["base_sha"] = "not-a-git-commit"
        values.append(value)
        value = coding_payload()
        value["worker_request"]["attempt"] = True
        values.append(value)
        for index, value in enumerate(values):
            with self.subTest(index=index):
                self.assertEqual(self.execute(self.body(request_id=f"bad-{index}", value=value))["status"],
                                 "rejected")
        self.assertEqual(self.execute(self.body(request_id="bad-profile", profile_id="unapproved"))["status"],
                         "rejected")
        self.assertEqual(self.transport.calls, [])
        self.assertEqual(self.authority.ledger.budget()["spent_or_reserved"], 0)
        self.assertEqual(self.execute(self.body())["status"], "completed")

    def test_budget_denial_leaves_no_transport_or_reservation(self):
        self.config["budget_units"] = 100
        self.claim()
        denied = self.execute(self.body())
        self.assertEqual(denied["status"], "rejected")
        self.assertEqual(self.authority.ledger.budget()["spent_or_reserved"], 0)
        self.assertEqual(self.transport.calls, [])

    def test_expired_epoch_rejects_before_transport_and_current_owner_can_dispatch(self):
        self.claim()
        self.clock.now = 106.0
        self.assertEqual(self.execute(self.body())["status"], "rejected")
        epoch = self.claim(principal="beta", request_id="replacement")
        self.assertEqual(epoch, 2)
        self.assertEqual(self.execute(self.body(principal="beta", epoch=epoch))["status"], "completed")
        self.assertEqual(len(self.transport.calls), 1)

    def test_known_worker_failure_is_charged_and_replayed_without_another_call(self):
        self.transport.response = provider_response("not valid proposal JSON")
        self.claim()
        body = self.body()
        failed = self.execute(body)
        self.assertEqual(failed["status"], "failed")
        self.assertEqual(self.result(failed)["usage_units"], 166)
        self.assertEqual(self.result(failed)["metadata"]["failure_kind"], "proposal")
        self.assertEqual(self.authority.ledger.budget()["spent_or_reserved"], 166)
        self.clock.now = 200.0
        self.open()
        self.assertEqual(self.execute(body), failed)
        self.assertEqual(len(self.transport.calls), 1)

    def test_unknown_usage_retains_reservation_and_persists_runwide_claim_fence(self):
        self.transport.error = OSError("offline unknown response")
        self.claim()
        body = self.body()
        unknown = self.execute(body)
        self.assertEqual(unknown["status"], "unknown")
        reserved = self.authority.ledger.budget()["spent_or_reserved"]
        self.assertGreater(reserved, 166)
        self.clock.now = 200.0
        self.open()
        self.assertEqual(self.execute(body), unknown)
        for task in ("build", "review"):
            with self.subTest(task=task), self.authority.ledger.atomic() as db, self.assertRaises(ClaimRejected):
                self.backend.guard_claim(db, task)
        self.assertEqual(self.authority.ledger.budget()["spent_or_reserved"], reserved)
        self.assertEqual(len(self.transport.calls), 1)

    def test_offline_constructor_rejects_default_https_transport_without_using_it(self):
        self.authority = Authority(self.root / "authority", self.config, clock=self.clock)
        live_worker = OpenAIWorker(KEY, max_output_tokens=64, timeout=2)
        with self.assertRaises((ValueError, RuntimeError)):
            CodingDispatch(self.authority, self.payloads, {"mini": live_worker}, self.task_specs)
        with self.assertRaises((ValueError, RuntimeError)):
            CodingDispatch(self.authority, self.payloads, {"mini": live_worker}, self.task_specs,
                           transport_mode="live", allow_live=True)
        self.assertEqual(self.transport.calls, [])

    def assert_crash_recovery(self, point, *, completed):
        def crash(current_point, request_id):
            if current_point == point and request_id == "coding-request":
                raise InjectedCrash(point)

        self.open(crash_hook=crash)
        self.claim()
        body = self.body()
        with self.assertRaises(InjectedCrash):
            self.execute(body)
        expected_calls = 1 if completed else 0
        self.assertEqual(len(self.transport.calls), expected_calls)
        self.clock.now = 200.0  # Recovery must not require the original live lease.
        self.open()
        recovered = self.execute(body)
        self.assertEqual(recovered["status"], "completed" if completed else "unknown")
        if completed:
            self.assertEqual(self.result(recovered)["usage_units"], 166)
            self.assertEqual(self.authority.ledger.budget()["spent_or_reserved"], 166)
        else:
            self.assertGreater(self.authority.ledger.budget()["spent_or_reserved"], 166)
            with self.authority.ledger.atomic() as db, self.assertRaises(ClaimRejected):
                self.backend.guard_claim(db, "review")
        self.assertEqual(self.execute(body), recovered)
        self.assertEqual(self.call_authority("lookup", "lookup", {"request_id": "coding-request"}), recovered)
        self.assertEqual(len(self.transport.calls), expected_calls)

    def test_crash_after_atomic_admission_becomes_unknown_without_invocation(self):
        self.assert_crash_recovery("after_admission", completed=False)

    def test_crash_before_invoke_never_retries_journal_intent(self):
        self.assert_crash_recovery("before_invoke", completed=False)

    def test_crash_after_result_persisted_recovers_after_lease_expiry_without_call(self):
        self.assert_crash_recovery("after_result_persisted", completed=True)

    def test_crash_after_settlement_preserves_charge_and_replays_result(self):
        self.assert_crash_recovery("after_settlement", completed=True)

    def test_crash_before_receipt_commit_recovers_journal_without_duplicate_charge(self):
        self.assert_crash_recovery("before_receipt_commit", completed=True)

    def test_crash_after_receipt_commit_preserves_completed_receipt(self):
        self.assert_crash_recovery("after_receipt_commit", completed=True)

    def test_payload_publication_failure_recovers_from_journal_without_second_call(self):
        self.claim()
        body = self.body()
        self.payloads.fail_publication = True
        interrupted = self.execute(body)
        self.assertEqual(interrupted["status"], "publication_pending")
        self.assertEqual(self.authority.ledger.budget()["spent_or_reserved"], 166)
        self.assertEqual(len(self.transport.calls), 1)
        self.payloads.fail_publication = False
        self.clock.now = 200.0
        self.open()
        recovered = self.execute(body)
        self.assertEqual(recovered["status"], "completed")
        self.assertEqual(self.result(recovered)["usage_units"], 166)
        self.assertEqual(self.authority.ledger.budget()["spent_or_reserved"], 166)
        self.assertEqual(len(self.transport.calls), 1)
        with self.authority.ledger.atomic() as db, self.assertRaises(ClaimRejected):
            self.backend.guard_claim(db, "review")

    def test_worker_timeout_is_frozen_during_execution_and_across_restart(self):
        self.claim()
        self.worker.timeout = 3
        denied = self.execute(self.body(request_id="changed-profile"))
        self.assertEqual(denied["status"], "rejected")
        self.assertEqual(self.transport.calls, [])
        with self.assertRaises((ValueError, RuntimeError)):
            self.open()
        self.worker.timeout = 2
        self.open()
        completed = self.execute(self.body())
        self.assertEqual(completed["status"], "completed")
        self.assertEqual(len(self.transport.calls), 1)

    def test_concurrent_duplicate_waits_for_first_result_and_never_reinvokes_worker(self):
        self.claim()
        body = self.body()
        self.transport.entered, self.transport.release = threading.Event(), threading.Event()
        duplicate_waiting = threading.Event()
        outcomes, errors = [], []
        original_lock = self.backend.lock

        class ObservedLock:
            def __enter__(self):
                if threading.current_thread() is duplicate:
                    duplicate_waiting.set()
                return original_lock.__enter__()

            def __exit__(self, *args):
                return original_lock.__exit__(*args)

        def invoke():
            try:
                outcomes.append(self.backend.execute(body))
            except BaseException as error:
                errors.append(error)

        self.backend.lock = ObservedLock()
        first = threading.Thread(target=invoke, daemon=True)
        duplicate = threading.Thread(target=invoke, daemon=True)
        first.start()
        try:
            self.assertTrue(self.transport.entered.wait(timeout=3), "First request never reached offline transport")
            duplicate.start()
            self.assertTrue(duplicate_waiting.wait(timeout=3), "Duplicate never reached the dispatch lock")
            with self.authority.lifecycle:
                self.assertEqual(self.authority.active_handlers, 2)
            self.assertEqual(len(self.transport.calls), 1)
        finally:
            self.transport.release.set()
            for worker in (first, duplicate):
                if worker.ident is not None:
                    worker.join(timeout=5)
            self.backend.lock = original_lock
        self.assertFalse(first.is_alive())
        self.assertFalse(duplicate.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(len(outcomes), 2)
        self.assertEqual(outcomes[0], outcomes[1])
        self.assertEqual(outcomes[0]["status"], "completed")
        self.assertEqual(len(self.transport.calls), 1)
        self.assertEqual(self.authority.ledger.budget()["spent_or_reserved"], 166)

    def test_result_journal_write_failure_retains_full_reservation_and_halts_other_tasks(self):
        self.claim()
        self.claim("review", request_id="claim-review")
        body = self.body()
        other = self.body(task_id="review", request_id="review-request", action_id="review-action")
        original_write = journal_module._write
        failures = []

        def fail_result_write(path, value):
            if path.name.endswith(".result.json"):
                self.assertEqual(len(self.transport.calls), 1)
                failures.append(path)
                raise OSError("Injected durable result write failure")
            return original_write(path, value)

        with patch.object(journal_module, "_write", side_effect=fail_result_write):
            unknown = self.execute(body)
        self.assertEqual(len(failures), 1)
        self.assertEqual(unknown["status"], "unknown")
        self.assertEqual(self.authority.ledger.budget()["spent_or_reserved"], unknown["reserved_units"])
        self.assertGreater(unknown["reserved_units"], 166)
        self.assertEqual(self.execute(body), unknown)
        with self.authority.ledger.atomic() as db, self.assertRaises(ClaimRejected):
            self.backend.guard_claim(db, "review")
        self.assertEqual(self.execute(other)["status"], "rejected")
        self.clock.now = 200.0
        self.open()
        self.assertEqual(self.execute(body), unknown)
        with self.authority.ledger.atomic() as db, self.assertRaises(ClaimRejected):
            self.backend.guard_claim(db, "review")
        self.assertEqual(len(self.transport.calls), 1)
        self.assertEqual(self.authority.ledger.budget()["spent_or_reserved"], unknown["reserved_units"])

    def test_second_backend_for_live_authority_is_rejected_and_first_remains_usable(self):
        self.claim()
        first = self.backend
        with self.assertRaises(DispatchError):
            CodingDispatch(self.authority, self.payloads, self.workers, self.task_specs,
                           journal_root=self.root / "journal")
        self.assertIs(self.backend, first)
        self.assertEqual(self.execute(self.body())["status"], "completed")
        self.assertEqual(len(self.transport.calls), 1)

    def test_failed_halt_persistence_still_fences_new_work_in_memory(self):
        self.claim()
        self.claim("review", request_id="claim-review")
        body = self.body()
        other = self.body(task_id="review", request_id="review-request", action_id="review-action")
        self.transport.error = OSError("Unknown offline response")
        with patch.object(self.backend, "_halt", side_effect=sqlite3.OperationalError("Injected halt write failure")):
            unknown = self.execute(body)
        self.assertEqual(unknown["status"], "waiting")
        self.assertEqual(unknown["reason"], "state_persistence_failure")
        self.assertEqual((unknown["principal"], unknown["request_id"]), ("alpha", body["request_id"]))
        self.assertNotIn("result_sha256", unknown)
        self.assertTrue(self.backend.failed_closed)
        with self.authority.ledger.atomic() as db:
            self.assertEqual(db.execute("SELECT value FROM coding_meta_v1 WHERE key='halted'").fetchone()[0], "0")
            with self.assertRaises(ClaimRejected):
                self.backend.guard_claim(db, "review")
        self.assertEqual(self.execute(other)["status"], "rejected")
        self.assertEqual(self.execute(body)["status"], "dispatching")
        self.assertEqual(len(self.transport.calls), 1)
        self.assertGreater(self.authority.ledger.budget()["spent_or_reserved"], 166)

    def test_dispatch_source_and_journal_namespace_are_bound_across_restart(self):
        self.claim()
        self.assertEqual(self.backend.config["journal_namespace"], str((self.root / "journal").resolve()))
        for module in (dispatch_module, store_module):
            path = Path(module.__file__)
            self.assertEqual(self.backend.config["sources"][path.name], hashlib.sha256(path.read_bytes()).hexdigest())
        body = self.body()
        completed = self.execute(body)
        self.authority.close()
        self.authority = Authority(self.root / "authority", self.config, clock=self.clock)
        with self.assertRaises(DispatchError):
            CodingDispatch(self.authority, self.payloads, self.workers, self.task_specs,
                           journal_root=self.root / "different-journal")
        self.open()
        self.assertEqual(self.execute(body), completed)
        self.assertEqual(len(self.transport.calls), 1)

    def terminal_corruption_fixture(self, name):
        """Each corruption subcase owns a fresh authority, journal and transport."""
        if self.authority is not None:
            self.authority.close()
        self.authority = None
        self.root = Path(self.temp.name) / name
        self.payloads = MemoryPayloads()
        self.transport = OfflineTransport()
        self.worker = OpenAIWorker(KEY, max_output_tokens=64, timeout=2, transport=self.transport)
        self.workers = {"mini": self.worker}
        self.claim()
        receipt = self.execute(self.body())
        self.assertEqual(receipt["status"], "completed")
        self.assertEqual(len(self.transport.calls), 1)
        return receipt

    def test_terminal_reservation_tampering_or_deletion_rejects_restart(self):
        for mutation in ("spent", "deleted"):
            with self.subTest(mutation=mutation):
                receipt = self.terminal_corruption_fixture("reservation-" + mutation)
                with self.authority.ledger.atomic() as db:
                    if mutation == "spent":
                        db.execute("UPDATE reservations SET spent=spent+1 WHERE id=?", (receipt["reservation_id"],))
                    else:
                        db.execute("DELETE FROM reservations WHERE id=?", (receipt["reservation_id"],))
                with self.assertRaises(DispatchError):
                    self.open()
                self.assertEqual(len(self.transport.calls), 1)

    def test_missing_terminal_result_or_settlement_journal_rejects_restart(self):
        for missing in ("result", "settled"):
            with self.subTest(missing=missing):
                receipt = self.terminal_corruption_fixture("missing-" + missing)
                path = self.backend.journal.paths(receipt["call_id"])[missing]
                self.assertTrue(path.is_file())
                path.unlink()
                with self.assertRaises((DispatchError, journal_module.JournalError)):
                    self.open()
                self.assertEqual(len(self.transport.calls), 1)

    def test_removed_coding_action_rejects_restart_without_reinvocation(self):
        self.terminal_corruption_fixture("missing-action")
        with self.authority.ledger.atomic() as db:
            db.execute("DELETE FROM coding_actions_v1 WHERE principal=? AND request_id=?",
                       ("alpha", "coding-request"))
        with self.assertRaises(DispatchError):
            self.open()
        self.assertEqual(len(self.transport.calls), 1)
