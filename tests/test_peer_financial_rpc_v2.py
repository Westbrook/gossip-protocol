"""Offline RPC proof with disposable historical ledgers and real injected workers."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, replace
import hashlib
from pathlib import Path
import socket
import sqlite3
import struct
import tempfile
import threading
import time
import unittest

from gossip_harness.ledger import Ledger
from gossip_harness.peer_financial_authority_v2 import CumulativeAuthorityV2, canonical_payload, ledger_identity
from gossip_harness.peer_financial_rpc_v2 import (
    DEADLINE_SECONDS, MAX_FRAME, PROTOCOL, SCHEMA_VERSION,
    FinancialClient, FinancialDenied, FinancialRPC, FinancialRPCError,
    FinancialServer, FinancialUnknownOutcome, _signed, _verified,
)
from gossip_harness.peer_project_contract_v2 import ActionRequest, Context, EvidenceRef, WorkKey, to_dict
from gossip_harness.worker import OpenAIWorker
from tests.test_peer_financial_authority_v2 import Clock, MemoryPayloads, OfflineTransport


class _Crash(BaseException):
    pass


class RPCFixture:
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.path = self.root / "disposable-historical.sqlite"
        self.historical = Ledger(self.path, 1_000_000)
        self.historical.add_task("historic")
        old = self.historical.claim("historic", "old-worker", now=1, ttl=1)
        self.historical.reserve("historic-payment", old, 2000, now=1)
        self.historical.settle("historic-payment", 1000)
        self.clock, self.payloads, self.transport = Clock(), MemoryPayloads(), OfflineTransport()
        self.worker = OpenAIWorker("offline-fixture-not-a-real-key", max_output_tokens=64, timeout=2, transport=self.transport)
        self.context = Context("a" * 64, "cohort", "trajectory", 0, "b" * 64)
        self.keys = {f"role{index:02d}": hashlib.sha256(f"fixture-capability-{index}".encode()).hexdigest() for index in range(20)}
        self.works = {actor: WorkKey("catalog", "requirement", actor, 0) for actor in self.keys}
        self.contract = {"cohort_id": "cohort", "execution_contract_sha256": "a" * 64,
                         "ledger_identity": ledger_identity(self.path), "journal_root": str(self.root / "journal"),
                         "transport_identity": "closed-offline-fixture", "task_specs": [
                             {"context": to_dict(self.context), "work": to_dict(work), "actors": [actor],
                              "kinds": ["build"], "profiles": ["mini"], "allowed_paths": ["src/a.py"],
                              "max_reserved_units": 400_000} for actor, work in self.works.items()]}
        self.arrived = set()
        self.finance = self.rpc = self.server = self.thread = None
        self.addCleanup(self.cleanup)
        self.open()

    def open(self, recovery=False, **rpc_kwargs):
        self.finance = CumulativeAuthorityV2.open(
            self.path, self.root / "service", self.contract, 900_000, 1_000_000, 1000,
            payloads=self.payloads, workers={"mini": self.worker}, max_workers=2,
            clock=self.clock, recovery=recovery)
        self.rpc = self.new_rpc(self.keys, **rpc_kwargs)

    def new_rpc(self, keys, **kwargs):
        return FinancialRPC(self.finance, keys, request_guard=self.guard, request_guard_sha256="e" * 64, max_requests_per_principal=kwargs.pop("max_requests_per_principal", 4), **kwargs)

    def guard(self, action):
        if action.worker_payload_ref.event_id not in self.arrived:
            raise FileNotFoundError("Exact worker evidence event has not arrived")
        self.payloads.read_owned(action.actor, action.worker_payload_ref.payload_sha256)

    def cleanup(self):
        self.transport.release.set()
        if self.server is not None:
            self.server.shutdown()
            self.server.server_close()
            self.thread.join(3)
        if self.finance is not None:
            self.finance.close()
        self.temp.cleanup()

    def request(self, operation, request_id="command-one", payload=None, actor="role00", nonce="0" * 32):
        return _signed({"protocol": PROTOCOL, "schema_version": SCHEMA_VERSION,
                        "contract_sha256": "a" * 64, "actor": actor, "request_id": request_id,
                        "nonce": nonce, "operation": operation, "payload": payload or {}}, self.keys[actor])

    def call(self, operation, request_id="command-one", payload=None, actor="role00", nonce="0" * 32):
        return _verified(self.rpc.handle(self.request(operation, request_id, payload, actor, nonce)), self.keys[actor])["receipt"]

    def claim_payload(self, actor="role00", ttl=60):
        return {"context": to_dict(self.context), "work": to_dict(self.works[actor]), "ttl": float(ttl)}

    def claim(self, actor="role00", request_id="claim-one", ttl=60):
        return self.finance.claim(actor, self.context, self.works[actor], ttl=ttl)

    def action(self, actor="role00", request_id="submit-one"):
        request = {"task_id": self.finance.task_id(self.context, self.works[actor]),
                   "instructions": "Fix the task using arrived evidence.", "allowed_paths": ["src/a.py"],
                   "files": {"src/a.py": "broken\n"}, "base_sha": "c" * 40, "attempt": 1, "feedback": ""}
        raw = canonical_payload({"worker_request": request, "view_manifest_sha256": "d" * 64})
        sha = self.payloads.put_owned(actor, raw)
        ref = EvidenceRef(hashlib.sha256((actor + request_id).encode()).hexdigest(), actor, "worker-request", sha)
        self.arrived.add(ref.event_id)
        return ActionRequest(self.context, "action-" + request_id, request_id, actor, "build", self.works[actor], "mini", ref, "d" * 64)

    def rows(self, table):
        with sqlite3.connect(self.path) as db:
            db.row_factory = sqlite3.Row
            return [dict(row) for row in db.execute(f"SELECT * FROM {table}")]

    def start(self):
        self.server = FinancialServer(0, self.rpc)
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": 0.01})
        self.thread.start()
        return self.client()

    def client(self, actor="role00", **kwargs):
        return FinancialClient(self.server.server_address[1], actor, self.keys[actor], "a" * 64, **kwargs)

    def terminal(self, client, request_id):
        end = time.monotonic() + 5
        while time.monotonic() < end:
            reply = client.lookup(request_id)
            if reply is not None and reply.state not in {"pending", "waiting"}:
                return reply
            time.sleep(0.005)
        self.fail("Fixture execution did not reach a retained outcome")


class PeerFinancialRPCV2Tests(RPCFixture, unittest.TestCase):
    def test_exact_registration_and_distinct_twenty_role_capabilities(self):
        self.assertEqual(len(self.rpc._keys), 20)
        self.assertEqual(self.new_rpc(self.keys).config_sha256, self.rpc.config_sha256)
        for bad in ({}, {**self.keys, "outsider": "x" * 64}, {**self.keys, "role01": self.keys["role00"]}):
            with self.assertRaises(FinancialRPCError):
                self.new_rpc(bad)
        with self.assertRaises(FinancialRPCError):
            self.new_rpc({**self.keys, "role00": "changed" * 9})
        with self.assertRaises(FinancialRPCError):
            self.new_rpc(self.keys, max_handlers=True)
        with self.assertRaises(FinancialRPCError):
            self.new_rpc(self.keys, max_requests_per_principal=0)
        self.assertNotIn(self.keys["role00"], str(self.rows("financial_rpc_config_v2")))

    def test_authentication_closed_schema_and_contract_fail_before_mutation(self):
        base = self.request("claim", payload=self.claim_payload())
        for change in ({"contract_sha256": "f" * 64}, {"protocol": "peer-authority-v1"},
                       {"schema_version": True}, {"operation": "verified_terminal"},
                       {"nonce": "short"}, {"request_id": "bad/id"}, {"unknown": 1},
                       {"payload": {**self.claim_payload(), "source_path": "/tmp/private"}},
                       {"payload": {**self.claim_payload(), "ttl": True}},
                       {"payload": {**self.claim_payload(), "ttl": 10**400}}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.rpc.handle(_signed({**base["body"], **change}, self.keys["role00"]))
        with self.assertRaises(FinancialRPCError):
            self.rpc.handle(_signed({**base["body"], "actor": "role01"}, self.keys["role00"]))
        self.assertEqual(self.rows("financial_rpc_requests_v2"), [])
        self.assertEqual(self.transport.calls, [])

    def test_claim_lost_ack_exact_replay_does_not_increment_expired_epoch(self):
        payload = self.claim_payload(ttl=1)
        first = self.call("claim", payload=payload)
        self.clock.now = 1000
        replay = self.call("claim", payload=payload, nonce="1" * 32)
        self.assertEqual(first, replay)
        self.assertEqual(first["body"]["epoch"], 1)
        own = [row for row in self.rows("tasks") if row["worker"] == "role00"]
        self.assertEqual(own[0]["epoch"], 1)
        changed = self.call("claim", payload=self.claim_payload(ttl=2))
        self.assertEqual(changed, {"status": "denied", "reason": "request_identity_conflict"})

    def test_renew_lost_ack_and_identity_change_never_extend_twice(self):
        claim = self.call("claim", "claim", self.claim_payload())
        self.clock.now = 110
        payload = {"lease": claim["body"], "ttl": 60.0}
        first = self.call("renew", "renew", payload)
        self.clock.now = 150
        self.assertEqual(self.call("renew", "renew", payload, nonce="2" * 32), first)
        self.assertEqual(first["body"]["expires_at"], 170)
        changed = {**payload, "lease": {**payload["lease"], "expires_at": 161.0}}
        self.assertEqual(self.call("renew", "renew", changed)["reason"], "request_identity_conflict")
        self.assertEqual(self.call("lookup", "renew")["kind"], "absent")

    def test_lease_mutation_and_receipt_rollback_together_before_commit(self):
        def crash(point, request_id):
            if point == "before_lease_commit":
                raise _Crash()
        self.rpc.crash_hook = crash
        with self.assertRaises(_Crash):
            self.call("claim", payload=self.claim_payload())
        self.assertEqual(self.rows("financial_rpc_requests_v2"), [])
        self.assertEqual(self.rows("financial_rpc_config_v2")[0]["request_count"], 0)
        self.assertTrue(all(row["epoch"] == 0 for row in self.rows("tasks") if row["id"] != "historic"))
        self.rpc.crash_hook = None
        self.assertEqual(self.call("claim", payload=self.claim_payload())["body"]["epoch"], 1)

    def test_definitive_denial_remains_bound_after_conditions_change(self):
        self.call("claim", "first", self.claim_payload(ttl=1))
        denied = self.call("claim", "blocked", self.claim_payload(ttl=1))
        self.assertEqual(denied["reason"], "claim_rejected")
        self.clock.now = 1000
        self.assertEqual(self.call("claim", "blocked", self.claim_payload(ttl=1)), denied)
        self.assertEqual(self.call("claim", "new", self.claim_payload(ttl=1))["body"]["epoch"], 2)

    def test_request_namespace_principal_isolation_and_task_permissions(self):
        own = self.call("claim", "same", self.claim_payload())
        other = self.call("claim", "same", self.claim_payload("role01"), actor="role01")
        self.assertNotEqual(own["body"]["task_id"], other["body"]["task_id"])
        denied = self.call("claim", "foreign-task", self.claim_payload("role01"))
        self.assertEqual(denied["reason"], "task_scope")
        with self.assertRaises(FinancialRPCError):
            self.call("renew", "foreign-lease", {"lease": other["body"], "ttl": 60})
        self.assertEqual(self.call("renew", "same", {"lease": own["body"], "ttl": 60})["reason"], "request_identity_conflict")

    def test_submit_intent_precedes_admission_and_lookup_never_invokes(self):
        lease, action = self.claim(), self.action()
        payload = {"action": to_dict(action), "lease": asdict(lease)}
        def crash(point, request_id):
            if point == "after_submit_intent":
                raise _Crash()
        self.rpc.crash_hook = crash
        with self.assertRaises(_Crash):
            self.call("submit", action.request_id, payload)
        self.rpc.crash_hook = None
        self.assertEqual(self.call("lookup", action.request_id)["kind"], "absent")
        self.assertEqual(self.transport.calls, [])
        changed = {**payload, "lease": {**payload["lease"], "expires_at": lease.expires_at + 1}}
        self.assertEqual(self.call("submit", action.request_id, changed)["reason"], "request_identity_conflict")
        self.assertEqual(self.transport.calls, [])

    def test_recovery_preserves_claim_receipt_and_rejects_missing_journal_membership(self):
        first = self.call("claim", payload=self.claim_payload(ttl=1))
        self.finance.close()
        self.open(recovery=True)
        self.clock.now = 1000
        self.assertEqual(self.call("claim", payload=self.claim_payload(ttl=1)), first)
        with sqlite3.connect(self.path) as db:
            db.execute("DELETE FROM financial_rpc_requests_v2")
        with self.assertRaises(FinancialRPCError):
            self.call("claim", payload=self.claim_payload(ttl=1))
        with self.assertRaises(FinancialRPCError):
            self.new_rpc(self.keys)

    def test_orphan_registration_and_corrupt_receipt_fail_closed(self):
        self.call("claim", payload=self.claim_payload())
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE financial_rpc_requests_v2 SET receipt_sha=?", ("f" * 64,))
        with self.assertRaises(FinancialRPCError):
            self.call("claim", payload=self.claim_payload())
        with sqlite3.connect(self.path) as db:
            db.execute("DELETE FROM financial_rpc_config_v2")
        with self.assertRaises(FinancialRPCError):
            self.new_rpc(self.keys)

    def test_quota_preserves_existing_replay_and_lookup(self):
        original = self.call("claim", "claim", self.claim_payload())
        for index in range(3):
            self.call("claim", f"denial-{index}", self.claim_payload())
        self.assertEqual(self.call("claim", "overflow", self.claim_payload())["reason"], "request_quota")
        self.assertEqual(self.call("claim", "claim", self.claim_payload()), original)
        self.assertEqual(self.call("lookup", "absent")["kind"], "absent")
        self.assertEqual(len(self.rows("financial_rpc_requests_v2")), 4)

    def test_valid_receipt_swapping_cannot_rebind_an_exact_request(self):
        self.call("claim", "claim", self.claim_payload())
        other = self.call("claim", "other", self.claim_payload("role01"), actor="role01")
        row = next(row for row in self.rows("financial_rpc_requests_v2") if row["actor"] == "role01")
        self.assertEqual(other["status"], "ok")
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE financial_rpc_requests_v2 SET receipt=?,receipt_sha=? WHERE actor='role00'",
                       (row["receipt"], row["receipt_sha"]))
        with self.assertRaises(FinancialRPCError):
            self.call("claim", "claim", self.claim_payload())

    def test_guard_requires_exact_event_even_if_owned_payload_already_exists(self):
        lease, action = self.claim(), self.action()
        self.arrived.clear()
        payload = {"action": to_dict(action), "lease": asdict(lease)}
        waiting = self.call("submit", action.request_id, payload)
        self.assertEqual(waiting["body"]["state"], "waiting")
        self.assertEqual(waiting["body"]["reason"], "payload_unavailable")
        self.assertEqual(self.rows("financial_actions_v2"), [])
        self.assertEqual(self.transport.calls, [])
        self.assertEqual(self.call("lookup", action.request_id)["kind"], "absent")
        changed = replace(action, worker_payload_ref=replace(action.worker_payload_ref, event_id="f" * 64))
        self.assertEqual(self.call("submit", action.request_id, {**payload, "action": to_dict(changed)})["reason"],
                         "request_identity_conflict")
        self.arrived.add(action.worker_payload_ref.event_id)
        self.transport.release.clear()
        self.assertEqual(self.call("submit", action.request_id, payload)["body"]["state"], "pending")
        self.assertTrue(self.transport.entered.wait(2))
        self.arrived.clear()
        self.assertEqual(self.call("submit", action.request_id, payload)["body"]["state"], "pending")
        self.assertEqual(len(self.transport.calls), 1)

    def test_lookup_rejects_conflicting_direct_finance_identity(self):
        lease, action = self.claim(), self.action()
        payload = {"action": to_dict(action), "lease": asdict(lease)}
        self.arrived.clear()
        self.call("submit", action.request_id, payload)
        self.finance.submit(action.actor, action, replace(lease, expires_at=lease.expires_at + 1))
        with self.assertRaises(FinancialRPCError):
            self.call("lookup", action.request_id)
        self.assertEqual(self.call("submit", action.request_id, payload)["reason"], "request_identity_conflict")


class PeerFinancialRPCTCPV2Tests(RPCFixture, unittest.TestCase):
    def test_twenty_concurrent_principals_and_readiness_without_key_catalog(self):
        self.start()
        barrier = threading.Barrier(20)
        def claim(actor):
            barrier.wait(timeout=5)
            return self.client(actor).claim(self.context, self.works[actor], "claim")
        with ThreadPoolExecutor(max_workers=20) as pool:
            leases = list(pool.map(claim, self.keys))
        self.assertEqual(len({lease.worker_id for lease in leases}), 20)
        self.assertTrue(all(lease.epoch == 1 for lease in leases))
        ready = self.server.readiness()
        self.assertEqual(ready["principals"], 20)
        self.assertTrue(all(key not in str(ready) for key in self.keys.values()))
        self.assertEqual(self.historical.budget()["spent_or_reserved"], 1000)

    def test_lost_submit_reply_does_not_block_service_or_duplicate_real_worker(self):
        client = self.start()
        lease = client.claim(self.context, self.works["role00"], "claim")
        action = self.action()
        self.transport.release.clear()
        dropped = []
        def drop(point, request_id):
            if point == "before_send" and request_id == action.request_id and not dropped:
                dropped.append(request_id)
                raise OSError("Injected lost reply")
        self.rpc.crash_hook = drop
        with self.assertRaises(FinancialUnknownOutcome) as caught:
            client.submit(action, lease)
        self.assertEqual(caught.exception.payload, {"action": to_dict(action), "lease": asdict(lease)})
        self.assertTrue(self.transport.entered.wait(2))
        start = time.monotonic()
        self.assertEqual(client.lookup(action.request_id).state, "pending")
        self.assertIsNone(self.client("role01").lookup(action.request_id))
        replay = client.submit(action, lease)
        self.assertEqual(replay.state, "pending")
        self.assertLess(time.monotonic() - start, DEADLINE_SECONDS)
        self.assertEqual(len(self.transport.calls), 1)
        self.transport.release.set()
        terminal = self.terminal(client, action.request_id)
        self.assertEqual((terminal.state, terminal.usage_units), ("completed", 166))
        self.assertEqual(client.submit(action, lease), terminal)
        self.assertEqual(len(self.transport.calls), 1)
        self.assertEqual(self.historical.budget()["spent_or_reserved"], 1166)

    def test_lost_claim_and_renew_replies_survive_real_tcp_retries(self):
        client = self.start()
        dropped = set()
        def drop(point, request_id):
            if point == "before_send" and request_id not in dropped:
                dropped.add(request_id)
                raise OSError("Injected lost ack")
        self.rpc.crash_hook = drop
        with self.assertRaises(FinancialUnknownOutcome):
            client.claim(self.context, self.works["role00"], "claim", ttl=60)
        self.clock.now = 110
        lease = client.claim(self.context, self.works["role00"], "claim", ttl=60)
        self.assertEqual((lease.epoch, lease.expires_at), (1, 160))
        with self.assertRaises(FinancialUnknownOutcome):
            client.renew(lease, "renew", ttl=60)
        self.clock.now = 150
        renewed = client.renew(lease, "renew", ttl=60)
        self.assertEqual(renewed.expires_at, 170)
        self.assertEqual(renewed.epoch, 1)

    def test_payload_and_actor_impersonation_unknown_to_client_never_charge(self):
        client = self.start()
        lease = client.claim(self.context, self.works["role00"], "claim")
        action = self.action()
        with self.assertRaises(FinancialUnknownOutcome):
            self.client("role01").submit(action, lease)
        with self.assertRaises(FinancialUnknownOutcome):
            client.submit(replace(action, actor="role01", worker_payload_ref=replace(action.worker_payload_ref, producer="role01")), lease)
        bad = FinancialClient(self.server.server_address[1], "role00", self.keys["role01"], "a" * 64)
        with self.assertRaises(FinancialUnknownOutcome):
            bad.lookup("submit-one")
        self.assertEqual(self.transport.calls, [])
        self.assertEqual(self.historical.budget()["spent_or_reserved"], 1000)

    def test_client_rejects_signed_reply_with_wrong_nonce(self):
        client = self.start()
        original = self.rpc.handle
        def wrong(envelope):
            response = original(envelope)
            return _signed({**response["body"], "nonce": "f" * 32}, self.keys["role00"])
        self.rpc.handle = wrong
        with self.assertRaises(FinancialUnknownOutcome):
            client.claim(self.context, self.works["role00"], "claim")
        self.rpc.handle = original
        self.assertEqual(client.claim(self.context, self.works["role00"], "claim").epoch, 1)

    def test_signed_malformed_lease_remains_unknown_and_exact_replay_recovers(self):
        client = self.start()
        original = self.rpc.handle
        def malformed(envelope):
            response = original(envelope)
            body = response["body"]
            body["receipt"]["body"]["expires_at"] = 10**400
            return _signed(body, self.keys["role00"])
        self.rpc.handle = malformed
        with self.assertRaises(FinancialUnknownOutcome):
            client.claim(self.context, self.works["role00"], "claim")
        self.rpc.handle = original
        self.assertEqual(client.claim(self.context, self.works["role00"], "claim").epoch, 1)

    def test_oversize_frame_and_saturated_handler_leave_no_intent(self):
        client = self.start()
        with socket.create_connection(("127.0.0.1", self.server.server_address[1]), timeout=1) as stream:
            stream.sendall(struct.pack("!I", MAX_FRAME + 1))
            self.assertEqual(stream.recv(1), b"")
        for _ in range(self.rpc.max_handlers):
            self.assertTrue(self.server.slots.acquire(blocking=False))
        try:
            with self.assertRaises(FinancialUnknownOutcome):
                client.claim(self.context, self.works["role00"], "claim")
        finally:
            for _ in range(self.rpc.max_handlers):
                self.server.slots.release()
        self.assertEqual(self.rows("financial_rpc_requests_v2"), [])
        self.assertEqual(client.claim(self.context, self.works["role00"], "claim").epoch, 1)


if __name__ == "__main__":
    unittest.main()
