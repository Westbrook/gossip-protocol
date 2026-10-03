"""Disposable historical-ledger qualification; no providers, credentials or Docker."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict, replace
import fcntl
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from gossip_harness.ledger import ClaimRejected, Ledger
from gossip_harness.peer_financial_authority_v2 import CumulativeAuthorityV2, FinancialError, canonical_payload, ledger_identity
from gossip_harness.peer_project_contract_v2 import ActionRequest, Context, EvidenceRef, WorkKey, identity, to_dict
from gossip_harness.peer_store_v1 import StoreError
from gossip_harness.worker import HTTPResponse, MODEL, STRONG_MODEL, OpenAIWorker


class InjectedCrash(BaseException):
    pass


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

    def read_owned(self, actor, sha):
        with self.lock:
            if (actor, sha) not in self.values:
                raise FileNotFoundError("Missing owned payload")
            return self.values[actor, sha]

    def put_owned(self, actor, data):
        if self.fail_publication:
            raise OSError("Offline publication interruption")
        sha = hashlib.sha256(data).hexdigest()
        with self.lock:
            self.values[actor, sha] = bytes(data)
        return sha


class OfflineTransport:
    def __init__(self):
        self.calls = []
        self.lock = threading.Lock()
        self.release = threading.Event()
        self.release.set()
        self.entered = threading.Event()
        self.response_status = 200
        self.model = MODEL
        self.proposal = {"changes": [{"path": "src/a.py", "content": "fixed\n"}], "summary": "offline patch"}
        self.omit_usage = False
        self.error = None

    def __call__(self, request, timeout, maximum):
        with self.lock:
            self.calls.append(json.loads(request.data))
        self.entered.set()
        if not self.release.wait(5):
            raise RuntimeError("Offline barrier expired")
        if self.error is not None:
            raise self.error
        body = {"id": "resp_offline", "model": self.model, "status": "completed", "service_tier": "default",
                "output": [{"type": "message", "role": "assistant", "status": "completed",
                            "content": [{"type": "output_text", "text": json.dumps(self.proposal)}]}]}
        if not self.omit_usage:
            body["usage"] = {"input_tokens": 101, "output_tokens": 20, "total_tokens": 121}
        return HTTPResponse(self.response_status, {"X-Request-Id": "req_offline"}, json.dumps(body).encode())


class PeerFinancialAuthorityV2Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.path = self.root / "historical.sqlite"
        self.historical = Ledger(self.path, 1_000_000)
        self.historical.add_task("historic")
        old_lease = self.historical.claim("historic", "old-worker", now=1, ttl=1)
        self.historical.reserve("historical-payment", old_lease, 2000, now=1)
        self.historical.settle("historical-payment", 1000)
        self.clock = Clock()
        self.payloads = MemoryPayloads()
        self.transport = OfflineTransport()
        self.worker = OpenAIWorker("offline-fixture-not-a-real-key", max_output_tokens=64, timeout=2, transport=self.transport)
        self.context = Context("a" * 64, "cohort", "trajectory", 0, "b" * 64)
        self.other_context = replace(self.context, milestone=1)
        self.works = [WorkKey("catalog", "requirement", slot, 0) for slot in ("alpha-slot", "beta-slot", "other-slot")]
        self.contract = {"cohort_id": "cohort", "execution_contract_sha256": "a" * 64,
                         "ledger_identity": ledger_identity(self.path), "journal_root": str(self.root / "stable-journal"),
                         "transport_identity": "closed-offline-fixture", "task_specs": []}
        for index, work in enumerate(self.works):
            self.contract["task_specs"].append({
                "context": to_dict(self.other_context if index == 2 else self.context), "work": to_dict(work),
                "actors": ["alpha", "beta"], "kinds": ["build", "review"], "profiles": ["mini"],
                "allowed_paths": ["src/a.py"], "max_reserved_units": 400_000})
        self.authority = None
        self.addCleanup(self.cleanup)

    def cleanup(self):
        self.transport.release.set()
        if self.authority is not None:
            self.authority.close()
        self.temp.cleanup()

    def open(self, **changes):
        if self.authority is not None:
            self.authority.close()
            self.authority = None
        args = {"existing_ledger_path": self.path, "service_root": self.root / "service",
                "cohort_contract": self.contract, "incremental_cap_micro_usd": 900_000,
                "expected_global_cap": 1_000_000, "expected_opening_usage": 1000,
                "payloads": self.payloads, "workers": {"mini": self.worker}, "max_workers": 2,
                "clock": self.clock}
        args.update(changes)
        self.authority = CumulativeAuthorityV2.open(**args)
        return self.authority

    def action(self, index=0, *, actor="alpha", request_id="request-one", action_id="action-one", context=None):
        context = context or (self.other_context if index == 2 else self.context)
        work = self.works[index]
        task_id = self.authority.task_id(context, work)
        request = {"task_id": task_id, "instructions": "Fix the task using arrived evidence.",
                   "allowed_paths": ["src/a.py"], "files": {"src/a.py": "broken\n"},
                   "base_sha": "c" * 40, "attempt": 1, "feedback": ""}
        raw = canonical_payload({"worker_request": request, "view_manifest_sha256": "d" * 64})
        sha = self.payloads.put_owned(actor, raw)
        ref = EvidenceRef(hashlib.sha256((actor + request_id).encode()).hexdigest(), actor, "worker-request", sha)
        return ActionRequest(context, action_id, request_id, actor, "build", work, "mini", ref, "d" * 64)

    def claim(self, index=0, actor="alpha", ttl=60):
        context = self.other_context if index == 2 else self.context
        return self.authority.claim(actor, context, self.works[index], ttl=ttl)

    def terminal(self, action):
        end = time.monotonic() + 5
        while time.monotonic() < end:
            reply = self.authority.lookup(action.actor, action.request_id)
            if reply is not None and reply.state not in {"pending", "waiting"}:
                return reply
            time.sleep(0.005)
        self.fail("Offline executor did not reach retained outcome")

    def rows(self, table):
        with sqlite3.connect(self.path) as db:
            db.row_factory = sqlite3.Row
            return [dict(row) for row in db.execute(f"SELECT * FROM {table}")]

    def test_historical_spending_preserved_real_worker_settles_and_internal_proof(self):
        before = {table: self.rows(table) for table in ("settings", "reservations", "tasks")}
        self.open()
        lease, action = self.claim(), self.action()
        pending = self.authority.submit("alpha", action, lease)
        self.assertEqual(pending.state, "pending")
        reply = self.terminal(action)
        self.assertEqual((reply.state, reply.usage_units), ("completed", 166))
        self.assertEqual(self.historical.budget()["spent_or_reserved"], 1166)
        proof = self.authority.verified_terminal(reply.binding)
        self.assertEqual(proof["result"].changes, {"src/a.py": "fixed\n"})
        self.assertEqual(proof["action"], action)
        for table, rows in before.items():
            for row in rows:
                self.assertIn(row, self.rows(table))
        self.clock.now += 1000
        self.assertEqual(self.authority.submit("alpha", action, lease), reply)
        self.assertEqual(len(self.transport.calls), 1)

    def test_large_ascii_unicode_and_del_selector_contexts_keep_exact_terminal_proofs(self):
        # The twenty-role rehearsal reached a valid settled selector outcome but
        # terminal revalidation accidentally used the 144384-byte peer-wire
        # limit. Also cover the journal's larger ASCII encoding of Unicode.
        self.historical.increase_budget("offline-large-context", 12_000_000,
            expected_old=1_000_000, reason="Disposable fixture full-context reservations", now=self.clock())
        self.transport.model = STRONG_MODEL
        self.transport.proposal = {"changes": [{"path": "selection.json", "content": '{"selected":["catalog"]}'}],
                                  "summary": "Offline selector artifact"}
        self.worker = OpenAIWorker("offline-fixture-not-a-real-key", model=STRONG_MODEL,
            max_output_tokens=64, timeout=2, transport=self.transport)
        for spec in self.contract["task_specs"]:
            spec.update(kinds=["select_source"], profiles=["strong"],
                        allowed_paths=["selection.json"], max_reserved_units=6_000_000)
        self.open(expected_global_cap=12_000_000, incremental_cap_micro_usd=10_000_000,
                  workers={"strong": self.worker})
        records = []
        for index, feedback in enumerate(("e" * 393_000, "é" * 120_000, "\x7f" * 400_000)):
            context = self.other_context if index == 2 else self.context
            lease = self.claim(index)
            request = {"task_id": self.authority.task_id(context, self.works[index]),
                "instructions": "Select from the complete arrived source context.",
                "allowed_paths": ["selection.json"], "files": {"src/a.py": "source\n"},
                "base_sha": "c" * 40, "attempt": 1, "feedback": feedback}
            raw = canonical_payload({"worker_request": request, "view_manifest_sha256": "d" * 64})
            self.assertLess(len(raw), 512_000)
            journal_encoding = json.dumps(request, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode()
            self.assertGreater(len(journal_encoding), (390_000, 600_000, 2_100_000)[index])
            digest = self.payloads.put_owned("alpha", raw)
            ref = EvidenceRef(hashlib.sha256(("large-" + str(index)).encode()).hexdigest(), "alpha", "worker-request", digest)
            action = ActionRequest(context, "large-action-" + str(index), "large-request-" + str(index),
                "alpha", "select_source", self.works[index], "strong", ref, "d" * 64)
            self.assertEqual(self.authority.submit("alpha", action, lease).state, "pending")
            reply = self.terminal(action)
            self.assertEqual((reply.state, reply.usage_units), ("completed", 553))
            proof = self.authority.verified_terminal(reply.binding)
            self.assertEqual(proof["worker_request"].feedback, feedback)
            self.assertEqual(proof["result"].changes, {"selection.json": '{"selected":["catalog"]}'})
            self.assertEqual(self.authority.lookup("alpha", action.request_id), reply)
            self.assertEqual(self.authority.submit("alpha", action, lease), reply)
            records.append((action, lease, reply))
        self.assertEqual(len(self.transport.calls), 3)
        self.assertEqual(self.historical.budget()["spent_or_reserved"], 1000 + 3 * 553)
        self.assertTrue(all(row["state"] == "settled" for row in self.rows("reservations")))
        self.clock.now += 1000
        self.open(recovery=True, expected_global_cap=12_000_000, incremental_cap_micro_usd=10_000_000,
                  workers={"strong": self.worker})
        for action, lease, reply in records:
            self.assertEqual(self.authority.lookup("alpha", action.request_id), reply)
            self.assertEqual(self.authority.verified_terminal(reply.binding)["reply"], reply)
            self.assertEqual(self.authority.submit("alpha", action, lease), reply)
        self.assertEqual(len(self.transport.calls), 3)

    def test_terminal_retained_request_keeps_exact_json_scalar_types(self):
        self.open()
        action, lease = self.action(), self.claim()
        self.authority.submit("alpha", action, lease)
        reply = self.terminal(action)
        self.assertEqual(reply.state, "completed")
        path = self.authority.journal.paths(reply.binding.call_id)["request"]
        request = json.loads(path.read_bytes())
        self.assertEqual(request["request"]["attempt"], 1)
        request["request"]["attempt"] = True
        path.write_text(json.dumps(request), encoding="utf-8")
        # Python dictionary equality would consider True equal to 1. The
        # canonical journal envelope must retain the actual JSON scalar type.
        retained = self.authority.lookup("alpha", action.request_id)
        self.assertEqual((retained.state, retained.reason), ("unknown", "terminal_evidence_unavailable"))
        self.assertEqual(len(self.transport.calls), 1)
        self.assertEqual(self.historical.budget()["spent_or_reserved"], 1166)

    def test_worker_envelope_byte_limit_and_duplicate_json_still_reject_before_admission(self):
        self.open()
        lease = self.claim()
        for index, raw in enumerate((b" " * 600_001,
                b'{"worker_request":{},"worker_request":{},"view_manifest_sha256":"' + b"d" * 64 + b'"}')):
            digest = self.payloads.put_owned("alpha", raw)
            ref = EvidenceRef(hashlib.sha256(("malformed-" + str(index)).encode()).hexdigest(), "alpha", "worker-request", digest)
            action = ActionRequest(self.context, "malformed-action-" + str(index), "malformed-request-" + str(index),
                "alpha", "build", self.works[0], "mini", ref, "d" * 64)
            with self.assertRaises(StoreError):
                self.authority.submit("alpha", action, lease)
        self.assertEqual(len(self.transport.calls), 0)
        self.assertEqual(self.rows("financial_actions_v2"), [])
        self.assertEqual(len(self.rows("reservations")), 1)  # Historical fixture payment only.

    def test_busy_has_no_paid_queue_and_unrelated_workers_overlap(self):
        self.open()
        self.transport.release.clear()
        actions, leases = [self.action(i, request_id=f"req-{i}", action_id=f"action-{i}") for i in range(3)], [self.claim(i) for i in range(3)]
        first = self.authority.submit("alpha", actions[0], leases[0])
        self.assertTrue(self.transport.entered.wait(2))
        second = self.authority.submit("alpha", actions[1], leases[1])
        end = time.monotonic() + 2
        while len(self.transport.calls) < 2 and time.monotonic() < end:
            time.sleep(.005)
        self.assertEqual((first.state, second.state, len(self.transport.calls)), ("pending", "pending", 2))
        waiting = self.authority.submit("alpha", actions[2], leases[2])
        self.assertEqual((waiting.state, waiting.binding), ("waiting", None))
        self.assertEqual(len(self.rows("reservations")), 3)
        self.transport.release.set()
        self.terminal(actions[0]); self.terminal(actions[1])
        self.assertEqual(self.authority.submit("alpha", actions[2], leases[2]).state, "pending")
        self.assertEqual(self.terminal(actions[2]).state, "completed")

    def test_concurrent_identical_requests_singleflight_and_changed_lease_conflicts(self):
        self.open()
        self.transport.release.clear()
        action, lease = self.action(), self.claim()
        replies = []
        threads = [threading.Thread(target=lambda: replies.append(self.authority.submit("alpha", action, lease))) for _ in range(4)]
        for thread in threads: thread.start()
        for thread in threads: thread.join(3)
        self.assertEqual(len(replies), 4)
        # A duplicate arriving before the first admission commits may see the
        # finite executor slots occupied by other concurrent submitters.
        self.assertTrue(all(reply.state in {"pending", "waiting"} for reply in replies))
        admitted = next(reply for reply in replies if reply.state == "pending")
        self.assertEqual(self.authority.submit("alpha", action, lease), admitted)
        self.assertEqual(len(self.rows("financial_actions_v2")), 1)
        with self.assertRaises(FinancialError):
            self.authority.submit("alpha", action, replace(lease, expires_at=lease.expires_at + 1))
        with self.assertRaises(FinancialError):
            self.authority.submit("beta", action, lease)
        self.assertIsNone(self.authority.lookup("beta", action.request_id))
        self.transport.release.set()
        self.assertEqual(self.terminal(action).state, "completed")
        self.assertEqual(len(self.transport.calls), 1)

    def test_same_action_different_request_rejected(self):
        self.open()
        first, lease = self.action(), self.claim()
        self.authority.submit("alpha", first, lease)
        self.terminal(first)
        second = self.action(request_id="different-request")
        with self.assertRaisesRegex(FinancialError, "Action identity"):
            self.authority.submit("alpha", second, lease)
        self.assertEqual(len(self.transport.calls), 1)

    def test_contexts_share_immutable_cohort_cap(self):
        self.open(incremental_cap_micro_usd=400_000)
        self.transport.release.clear()
        first, second = self.action(), self.action(2, request_id="next", action_id="next")
        one, two = self.claim(), self.claim(2)
        self.authority.submit("alpha", first, one)
        self.assertTrue(self.transport.entered.wait(2))
        blocked = self.authority.submit("alpha", second, two)
        self.assertEqual((blocked.state, blocked.reason, blocked.binding), ("waiting", "cohort_budget", None))
        self.assertEqual(len(self.rows("reservations")), 2)
        self.transport.release.set()
        self.terminal(first)

    def test_global_cap_includes_historical_rows(self):
        self.historical.add_task("historic-two")
        old_lease = self.historical.claim("historic-two", "old-worker", now=1, ttl=1)
        self.historical.reserve("historical-two", old_lease, 699_000, now=1)
        self.historical.settle("historical-two", 699_000)
        self.open(expected_opening_usage=700_000)
        action, lease = self.action(), self.claim()
        reply = self.authority.submit("alpha", action, lease)
        self.assertEqual((reply.state, reply.reason), ("waiting", "global_budget"))
        self.assertEqual(len(self.rows("reservations")), 2)
        self.assertEqual(len(self.transport.calls), 0)

    def race_cap(self, *, global_limit=False):
        if global_limit:
            self.historical.add_task("historical-more")
            old = self.historical.claim("historical-more", "old", now=1, ttl=1)
            self.historical.reserve("historical-more", old, 499_000, now=1)
            self.historical.settle("historical-more", 499_000)
        self.open(incremental_cap_micro_usd=900_000 if global_limit else 400_000,
                  expected_opening_usage=500_000 if global_limit else 1000)
        self.transport.release.clear()
        actions = [self.action(i, request_id=f"race-{i}", action_id=f"race-{i}") for i in range(2)]
        leases = [self.claim(i) for i in range(2)]
        barrier = threading.Barrier(2)
        original = self.authority._request
        replies, errors = [], []
        def load(actor, action, lease):
            request = original(actor, action, lease)
            barrier.wait(timeout=3)
            return request
        def submit(index):
            try: replies.append(self.authority.submit("alpha", actions[index], leases[index]))
            except Exception as error: errors.append(error)
        with patch.object(self.authority, "_request", load):
            threads = [threading.Thread(target=submit, args=(i,)) for i in range(2)]
            for thread in threads: thread.start()
            for thread in threads: thread.join(4)
        self.assertEqual(errors, [])
        self.assertEqual(sorted(reply.state for reply in replies), ["pending", "waiting"])
        self.assertEqual(next(reply.reason for reply in replies if reply.state == "waiting"),
                         "global_budget" if global_limit else "cohort_budget")
        self.transport.release.set()
        accepted = next(reply for reply in replies if reply.state == "pending")
        self.assertEqual(self.terminal(accepted.binding.action).state, "completed")
        self.assertEqual(len(self.transport.calls), 1)

    def test_concurrent_admissions_cannot_overdraw_cohort(self):
        self.race_cap()

    def test_concurrent_admissions_cannot_overdraw_global_balance(self):
        self.race_cap(global_limit=True)

    def test_known_worker_failure_charged_and_known_halt_persists(self):
        self.open()
        self.transport.response_status = 429
        action, lease = self.action(), self.claim()
        self.authority.submit("alpha", action, lease)
        failed = self.terminal(action)
        self.assertEqual((failed.state, failed.usage_units), ("failed", 166))
        self.assertEqual(self.historical.budget()["spent_or_reserved"], 1166)
        with self.assertRaises(FinancialError): self.claim(1)
        with self.assertRaises(FinancialError): self.authority.verified_terminal(failed.binding)
        self.open(recovery=True, expected_opening_usage=999)
        self.assertEqual(self.authority.lookup("alpha", action.request_id), failed)
        with self.assertRaises(FinancialError): self.claim(1)
        self.assertEqual(len(self.transport.calls), 1)

    def test_unknown_usage_retains_reservation_and_halt_across_restart(self):
        self.open()
        self.transport.omit_usage = True
        action, lease = self.action(), self.claim(ttl=1)
        pending = self.authority.submit("alpha", action, lease)
        unknown = self.terminal(action)
        self.assertEqual((unknown.state, unknown.usage_units), ("unknown", None))
        self.assertEqual(self.historical.budget()["spent_or_reserved"], 1000 + pending.binding.reserved_units)
        self.clock.now += 10
        with self.assertRaises((FinancialError, ClaimRejected)): self.claim(actor="beta")
        self.open(recovery=True)
        with self.assertRaises(FinancialError): self.claim(1)
        self.assertEqual(self.authority.lookup("alpha", action.request_id).state, "unknown")
        self.assertEqual(len(self.transport.calls), 1)

    def test_committed_admission_without_journal_never_invokes_on_recovery(self):
        def crash(point, request_id):
            if point == "after_admission": raise InjectedCrash()
        self.open(crash_hook=crash)
        action, lease = self.action(), self.claim()
        with self.assertRaises(InjectedCrash): self.authority.submit("alpha", action, lease)
        self.assertEqual(len(self.transport.calls), 0)
        reserved = self.historical.budget()["spent_or_reserved"]
        self.clock.now += 1000
        self.open(recovery=True)
        self.assertEqual(self.authority.lookup("alpha", action.request_id).state, "unknown")
        self.assertEqual(self.historical.budget()["spent_or_reserved"], reserved)
        self.assertEqual(len(self.transport.calls), 0)
        with self.assertRaises(FinancialError): self.claim(1)

    def check_crash_recovery(self, boundary):
        def crash(point, request_id):
            if point == boundary: raise InjectedCrash()
        self.open(crash_hook=crash)
        action, lease = self.action(), self.claim(ttl=1)
        self.authority.submit("alpha", action, lease)
        self.assertEqual(self.terminal(action).state, "unknown")
        calls = len(self.transport.calls)
        self.clock.now += 1000
        self.open(recovery=True, expected_opening_usage=0)
        reply = self.authority.lookup("alpha", action.request_id)
        self.assertEqual((reply.state, reply.usage_units), ("completed", 166))
        self.assertEqual(len(self.transport.calls), calls)
        self.assertEqual(self.historical.budget()["spent_or_reserved"], 1166)
        self.assertEqual(self.authority.verified_terminal(reply.binding)["reply"], reply)

    def test_durable_result_recovery_after_expiry(self):
        self.check_crash_recovery("after_result_persisted")

    def test_settlement_crash_recovery_after_expiry(self):
        self.check_crash_recovery("after_settlement")

    def test_receipt_transaction_crash_recovery_after_expiry(self):
        self.check_crash_recovery("before_receipt_commit")

    def test_publication_failure_recovery_does_not_reinvoke(self):
        self.open()
        action, lease = self.action(), self.claim()
        self.payloads.fail_publication = True
        self.authority.submit("alpha", action, lease)
        reply = self.terminal(action)
        self.assertEqual((reply.state, reply.usage_units), ("publication_pending", 166))
        self.payloads.fail_publication = False
        self.open(recovery=True)
        self.assertEqual(self.authority.lookup("alpha", action.request_id).state, "completed")
        self.assertEqual(len(self.transport.calls), 1)
        with self.assertRaises(FinancialError): self.claim(1)

    def test_lifetime_historical_lock_covers_async_close(self):
        self.open()
        self.transport.release.clear()
        action, lease = self.action(), self.claim()
        self.authority.submit("alpha", action, lease)
        self.assertTrue(self.transport.entered.wait(2))
        closer = threading.Thread(target=self.authority.close)
        closer.start()
        with open(str(self.path) + ".continuation-live.lock", "a") as lock:
            with self.assertRaises(BlockingIOError):
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.assertTrue(closer.is_alive())
            self.transport.release.set()
            closer.join(5)
            self.assertFalse(closer.is_alive())
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def test_second_service_and_existing_historical_owner_excluded(self):
        with open(str(self.path) + ".continuation-live.lock", "a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaises(BlockingIOError): self.open()
        self.open()
        with self.assertRaises(BlockingIOError):
            CumulativeAuthorityV2.open(self.path, self.root / "another", self.contract, 900_000, 1_000_000, 1000,
                                       payloads=self.payloads, workers={"mini": self.worker}, clock=self.clock)

    def test_output_directory_cannot_mint_allowance_or_change_journal(self):
        self.open()
        self.authority.close(); self.authority = None
        with self.assertRaisesRegex(FinancialError, "requires explicit recovery"):
            self.open(service_root=self.root / "new-output")
        self.open(service_root=self.root / "new-output", recovery=True)
        self.assertEqual(len(self.rows("financial_cohorts_v2")), 1)
        changed = deepcopy(self.contract); changed["journal_root"] = str(self.root / "new-journal")
        with self.assertRaisesRegex(FinancialError, "configuration changed"):
            self.open(recovery=True, cohort_contract=changed)
        with self.assertRaisesRegex(FinancialError, "configuration changed"):
            self.open(recovery=True, incremental_cap_micro_usd=900_001)

    def test_missing_aliased_changed_identity_and_schema_rejected(self):
        missing = self.root / "missing.sqlite"
        with self.assertRaises(FinancialError): self.open(existing_ledger_path=missing)
        self.assertFalse(missing.exists())
        alias = self.root / "alias.sqlite"; alias.symlink_to(self.path)
        with self.assertRaises(FinancialError): self.open(existing_ledger_path=alias)
        changed = deepcopy(self.contract); changed["ledger_identity"]["inode"] += 1
        with self.assertRaises(FinancialError): self.open(cohort_contract=changed)
        with sqlite3.connect(self.path) as db: db.execute("ALTER TABLE reservations ADD COLUMN unexpected TEXT")
        with self.assertRaisesRegex(FinancialError, "schema differs"): self.open()

    def test_fresh_opening_usage_and_quiescence_required(self):
        with self.assertRaisesRegex(FinancialError, "quiescent"): self.open(expected_opening_usage=999)
        self.historical.add_task("active-historical")
        lease = self.historical.claim("active-historical", "other", now=100, ttl=100)
        self.historical.reserve("unknown-historical", lease, 10, now=100)
        with self.assertRaisesRegex(FinancialError, "quiescent"): self.open(expected_opening_usage=1010)

    def test_expired_lease_checked_after_sqlite_write_wait(self):
        self.open()
        action, lease = self.action(), self.claim(ttl=1)
        entered, release = threading.Event(), threading.Event()
        original = self.authority._request
        def load(actor, request, current_lease):
            value = original(actor, request, current_lease)
            entered.set(); release.wait(3)
            return value
        errors = []
        def submit():
            try: self.authority.submit("alpha", action, lease)
            except Exception as error: errors.append(error)
        with patch.object(self.authority, "_request", load):
            thread = threading.Thread(target=submit); thread.start()
            self.assertTrue(entered.wait(2))
            db = sqlite3.connect(self.path); db.execute("BEGIN IMMEDIATE")
            release.set(); self.clock.now = 102
            db.commit(); db.close(); thread.join(3)
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], ClaimRejected)
        self.assertEqual(len(self.transport.calls), 0)
        self.assertEqual(len(self.rows("reservations")), 1)

    def test_corrupt_terminal_evidence_cannot_be_release_provenance(self):
        self.open()
        action, lease = self.action(), self.claim()
        self.authority.submit("alpha", action, lease)
        reply = self.terminal(action)
        settled = self.authority.journal.paths(reply.binding.call_id)["settled"]
        original = settled.read_bytes()
        settled.write_bytes(b'{}')
        with self.assertRaises((FinancialError, ValueError)):
            self.authority.verified_terminal(reply.binding)
        self.assertEqual(self.authority.lookup("alpha", action.request_id).state, "unknown")
        settled.write_bytes(original)
        self.open(recovery=True)
        self.assertEqual(self.authority.verified_terminal(reply.binding)["reply"], reply)
        settled.unlink()
        self.open(recovery=True)
        self.assertEqual(self.authority.lookup("alpha", action.request_id).state, "unknown")
        with self.assertRaises(FinancialError): self.claim(1)
        self.assertEqual(len(self.transport.calls), 1)

    def test_deleted_action_cannot_restore_cohort_allowance(self):
        self.open()
        action, lease = self.action(), self.claim()
        self.authority.submit("alpha", action, lease)
        self.terminal(action)
        with sqlite3.connect(self.path) as db:
            db.execute("DELETE FROM financial_actions_v2")
        with self.assertRaisesRegex(FinancialError, "membership"):
            self.open(recovery=True)
        self.assertEqual(self.historical.budget()["spent_or_reserved"], 1166)
        self.assertEqual(len(self.transport.calls), 1)

    def test_deleted_request_and_action_leave_detectable_orphan_reservation(self):
        self.open()
        action, lease = self.action(), self.claim()
        self.authority.submit("alpha", action, lease)
        self.terminal(action)
        with sqlite3.connect(self.path) as db:
            db.execute("DELETE FROM financial_requests_v2")
            db.execute("DELETE FROM financial_actions_v2")
        with self.assertRaisesRegex(FinancialError, "Orphan"):
            self.open(recovery=True)

    def test_altered_action_id_cannot_pass_retained_membership(self):
        self.open()
        action, lease = self.action(), self.claim()
        self.authority.submit("alpha", action, lease)
        reply = self.terminal(action)
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE financial_actions_v2 SET action_id='altered'")
        with self.assertRaises(FinancialError): self.authority.verified_terminal(reply.binding)
        with self.assertRaises(FinancialError): self.open(recovery=True)

    def test_not_yet_invoked_worker_is_fenced_by_halt(self):
        entered, release = threading.Event(), threading.Event()
        def block(point, request_id):
            if point == "before_invoke":
                entered.set(); release.wait(3)
        self.open(crash_hook=block)
        action, lease = self.action(), self.claim()
        self.authority.submit("alpha", action, lease)
        self.assertTrue(entered.wait(2))
        with self.authority.ledger.atomic() as db: self.authority._halt(db)
        release.set()
        reply = self.terminal(action)
        self.assertEqual(reply.state, "unknown")
        self.assertEqual(len(self.transport.calls), 0)
        self.assertEqual(self.historical.budget()["spent_or_reserved"], 1000 + reply.binding.reserved_units)

    def test_inflight_usage_settles_even_after_halt_and_expiry(self):
        self.open()
        self.transport.release.clear()
        action, lease = self.action(), self.claim(ttl=1)
        self.authority.submit("alpha", action, lease)
        self.assertTrue(self.transport.entered.wait(2))
        with self.authority.ledger.atomic() as db: self.authority._halt(db)
        self.clock.now += 100
        self.transport.release.set()
        reply = self.terminal(action)
        self.assertEqual((reply.state, reply.usage_units), ("completed", 166))
        self.assertEqual(self.historical.budget()["spent_or_reserved"], 1166)
        with self.assertRaises(FinancialError): self.claim(1)

    def test_unknown_persistence_failure_keeps_memory_fence_and_original_id(self):
        self.open()
        self.transport.omit_usage = True
        action, lease = self.action(), self.claim()
        original = self.authority._save_reply
        failed = threading.Event()
        def save(db, actor, request_id, reply):
            if reply.state == "unknown":
                failed.set()
                raise sqlite3.OperationalError("Offline unknown persistence fault")
            return original(db, actor, request_id, reply)
        with patch.object(self.authority, "_save_reply", save):
            pending = self.authority.submit("alpha", action, lease)
            self.assertTrue(failed.wait(2))
            end = time.monotonic() + 2
            while ("alpha", action.request_id) not in self.authority.persistence_failed and time.monotonic() < end:
                time.sleep(.005)
            unknown = self.authority.lookup("alpha", action.request_id)
            self.assertEqual((unknown.state, unknown.reason), ("unknown", "state_persistence_failure"))
            self.assertEqual(unknown.binding, pending.binding)
            self.assertEqual(self.historical.budget()["spent_or_reserved"], 1000 + pending.binding.reserved_units)
            self.assertEqual(self.authority.submit("alpha", action, lease), unknown)
            with self.assertRaises(FinancialError): self.claim(1)
        self.open(recovery=True)
        self.assertEqual(self.authority.lookup("alpha", action.request_id).state, "unknown")
        self.assertEqual(len(self.transport.calls), 1)

    def check_tampered_known_usage(self, state):
        units = self.worker.profile_manifest()["reservation_units"]
        self.open(incremental_cap_micro_usd=units + 100)
        first, first_lease = self.action(), self.claim()
        second, second_lease = self.action(1, request_id="second", action_id="second"), self.claim(1)
        if state == "failed":
            self.transport.proposal = "Malformed patch with known usage"
        if state == "publication_pending":
            self.payloads.fail_publication = True
        self.authority.submit("alpha", first, first_lease)
        outcome = self.terminal(first)
        self.assertEqual((outcome.state, outcome.usage_units), (state, 166))
        self.payloads.fail_publication = False
        before = self.authority.submit("alpha", second, second_lease)
        self.assertEqual(before.state, "waiting")
        self.assertEqual(len(self.rows("reservations")), 2)
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE reservations SET spent=0 WHERE id=?", (outcome.binding.reservation_id,))
        with self.assertRaisesRegex(FinancialError, "usage differs"):
            self.authority.submit("alpha", second, second_lease)
        self.assertTrue(self.authority.failed_closed)
        self.assertEqual(self.rows("financial_cohorts_v2")[0]["halted"], 1)
        self.assertEqual(len(self.rows("reservations")), 2)
        self.assertEqual(len(self.transport.calls), 1)

    def test_reduced_completed_spend_cannot_restore_admission_capacity(self):
        self.check_tampered_known_usage("completed")

    def test_reduced_failed_spend_cannot_restore_admission_capacity(self):
        self.check_tampered_known_usage("failed")

    def test_known_publication_pending_usage_is_checked_before_admission(self):
        self.check_tampered_known_usage("publication_pending")

    def test_pending_settled_window_remains_valid_for_other_admissions(self):
        entered, release = threading.Event(), threading.Event()
        def block(point, request_id):
            if point == "after_settlement" and request_id == "request-one":
                entered.set(); release.wait(3)
        self.open(crash_hook=block)
        first, first_lease = self.action(), self.claim()
        second, second_lease = self.action(1, request_id="second", action_id="second"), self.claim(1)
        self.authority.submit("alpha", first, first_lease)
        self.assertTrue(entered.wait(2))
        self.assertEqual(self.authority.lookup("alpha", first.request_id).state, "pending")
        self.assertEqual(self.authority.submit("alpha", second, second_lease).state, "pending")
        release.set()
        self.assertEqual(self.terminal(first).state, "completed")
        self.assertEqual(self.terminal(second).state, "completed")
        self.assertEqual(len(self.transport.calls), 2)
        self.assertEqual(self.historical.budget()["spent_or_reserved"], 1332)

    def test_unknown_provenance_rejection_preserves_exact_reply(self):
        self.open()
        self.transport.omit_usage = True
        action, lease = self.action(), self.claim()
        self.authority.submit("alpha", action, lease)
        unknown = self.terminal(action)
        with self.assertRaisesRegex(FinancialError, "No terminal known outcome"):
            self.authority.verified_terminal(unknown.binding)
        self.assertEqual(self.authority.submit("alpha", action, lease), unknown)
        self.assertEqual(len(self.transport.calls), 1)

    def test_slot_cap_uses_cumulative_commitment(self):
        self.contract["task_specs"][0]["max_reserved_units"] = 300_288
        self.open()
        first, lease = self.action(), self.claim()
        self.authority.submit("alpha", first, lease)
        self.terminal(first)
        second = self.action(request_id="second", action_id="second")
        reply = self.authority.submit("alpha", second, lease)
        self.assertEqual((reply.state, reply.reason), ("waiting", "slot_budget"))
        self.assertEqual(len(self.transport.calls), 1)

    def test_unresolved_old_cohort_blocks_fresh_registration(self):
        self.open()
        action, lease = self.action(), self.claim(ttl=1)
        self.payloads.fail_publication = True
        self.authority.submit("alpha", action, lease)
        self.assertEqual(self.terminal(action).state, "publication_pending")
        self.payloads.fail_publication = False
        self.clock.now += 100
        changed = deepcopy(self.contract)
        changed["cohort_id"] = "new-cohort"
        changed["journal_root"] = str(self.root / "new-journal")
        for spec in changed["task_specs"]: spec["context"]["cohort_id"] = "new-cohort"
        with self.assertRaisesRegex(FinancialError, "quiescent"):
            self.open(cohort_contract=changed, expected_opening_usage=1166)

    def test_changed_column_type_is_not_existing_ledger_schema(self):
        with sqlite3.connect(self.path) as db:
            db.execute("ALTER TABLE reservations RENAME TO old_reservations")
            db.execute("CREATE TABLE reservations(id TEXT PRIMARY KEY,task_id TEXT NOT NULL,epoch INTEGER NOT NULL,amount TEXT NOT NULL,spent INTEGER,state TEXT NOT NULL)")
            db.execute("INSERT INTO reservations SELECT * FROM old_reservations")
        with self.assertRaisesRegex(FinancialError, "schema differs"): self.open()

    def test_committed_terminal_receipt_survives_postcommit_boundary(self):
        def crash(point, request_id):
            if point == "after_receipt_commit": raise InjectedCrash()
        self.open(crash_hook=crash)
        action, lease = self.action(), self.claim()
        self.authority.submit("alpha", action, lease)
        reply = self.terminal(action)
        self.open(recovery=True)
        self.assertEqual(self.authority.lookup("alpha", action.request_id), reply)
        self.assertEqual(reply.state, "completed")
        self.assertEqual(len(self.transport.calls), 1)
        self.assertIsNotNone(self.claim(1))

    def test_inherited_service_instance_rejects_another_process(self):
        self.open()
        with patch("gossip_harness.peer_financial_authority_v2.os.getpid", return_value=-1):
            with self.assertRaisesRegex(FinancialError, "another process"):
                self.claim()

    def test_huge_or_boolean_authority_clock_is_rejected_consistently(self):
        self.clock.now = 10 ** 1000
        with self.assertRaisesRegex(FinancialError, "authority clock"): self.open()
        self.clock.now = True
        with self.assertRaisesRegex(FinancialError, "authority clock"): self.open()
        self.clock.now = 100
        self.open()
        self.clock.now = 10 ** 1000
        with self.assertRaisesRegex(FinancialError, "authority clock"): self.claim()

    def test_live_mode_and_real_transport_rejected(self):
        with self.assertRaisesRegex(FinancialError, "Live"): self.open(mode="live")
        with self.assertRaisesRegex(FinancialError, "fixture transport"):
            self.open(workers={"mini": OpenAIWorker("offline-test-only")})
        self.assertEqual(len(self.rows("reservations")), 1)


if __name__ == "__main__":
    unittest.main()
