"""Durable role transitions with fake RPC; no providers, keys, Docker or Git."""
from __future__ import annotations

from dataclasses import asdict, replace
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from gossip_harness.ledger import Lease
from gossip_harness.peer_financial_rpc_v2 import FinancialDenied, FinancialUnknownOutcome
from gossip_harness.peer_project_contract_v2 import (
    Context, DispatchBinding, DispatchReply, EvidenceRef, WorkKey, canonical_bytes,
    identity, to_dict, worker_request_digest,
)
from gossip_harness.peer_role_loop_v2 import (
    RoleError, RoleLoop, WorkDirective, directive_id, financial_task_id, materialize, ready_frontier,
)
from gossip_harness.worker import WorkerRequest, WorkerResult


class InjectedCrash(BaseException):
    pass


class FakeMesh:
    def __init__(self, actor="builder"):
        self.actor = actor
        self.refs = []
        self.blobs = {}
        self.commands = {}
        self.wants = []
        self.drop_publication_ack = False

    def arrive(self, producer, kind, payload, *, token=""):
        sha = hashlib.sha256(payload).hexdigest()
        ref = EvidenceRef(hashlib.sha256(f"{producer}:{kind}:{sha}:{token}".encode()).hexdigest(), producer, kind, sha)
        if ref not in self.refs:
            self.refs.append(ref)
        self.blobs[ref.event_id] = payload
        return ref

    def publish(self, kind, payload, command_id):
        signature = kind, payload
        if command_id in self.commands:
            previous, ref = self.commands[command_id]
            if signature != previous:
                raise RoleError("Publication command conflict")
        else:
            ref = self.arrive(self.actor, kind, payload, token=command_id)
            self.commands[command_id] = signature, ref
        if self.drop_publication_ack and kind == "role-result":
            self.drop_publication_ack = False
            raise TimeoutError("Publication persisted, acknowledgment lost")
        return ref

    def arrived(self):
        return tuple(self.refs)

    def want(self, ref):
        self.wants.append(ref)
        return True

    def resolve(self, ref):
        return self.blobs.get(ref.event_id)


class FakeFinance:
    def __init__(self, mesh, clock):
        self.mesh, self.clock = mesh, clock
        self.claims, self.renewals, self.replies = {}, {}, {}
        self.submissions = []
        self.claim_requests = []
        self.renew_requests = []
        self.invocations = 0
        self.waiting = False
        self.drop_claim_ack = False
        self.drop_renew_ack = False
        self.drop_submit_ack = False
        self.forget_reply = False
        self.result = WorkerResult({"src/a.py": "fixed\n"}, "bounded fixture change", 7, {})

    def claim(self, context, work, request_id, *, ttl=60):
        self.claim_requests.append((request_id, context, work, ttl))
        if request_id not in self.claims:
            task_id = financial_task_id(context, work)
            if any(lease.task_id == task_id and lease.expires_at > self.clock() for lease in self.claims.values()):
                raise FinancialDenied("claim_rejected")
            self.claims[request_id] = Lease(financial_task_id(context, work), self.mesh.actor, 1, self.clock() + ttl)
        lease = self.claims[request_id]
        if self.drop_claim_ack:
            self.drop_claim_ack = False
            raise FinancialUnknownOutcome("claim", request_id, {})
        return lease

    def renew(self, lease, request_id, *, ttl=60):
        self.renew_requests.append((request_id, lease, ttl))
        if request_id not in self.renewals:
            self.renewals[request_id] = replace(lease, expires_at=self.clock() + ttl)
        if self.drop_renew_ack:
            self.drop_renew_ack = False
            raise FinancialUnknownOutcome("renew", request_id, {})
        return self.renewals[request_id]

    def submit(self, action, lease):
        self.submissions.append((action, lease))
        prior = self.replies.get(action.request_id)
        if prior and prior.state != "waiting":
            return prior
        if self.waiting:
            reply = DispatchReply(action.request_id, identity(action), "waiting", "executor_capacity")
        else:
            body = json.loads(self.mesh.resolve(action.worker_payload_ref))["worker_request"]
            body["allowed_paths"] = tuple(body["allowed_paths"])
            binding = DispatchBinding(action, lease, worker_request_digest(WorkerRequest(**body)),
                                      "c" * 64, "d" * 64, "call-" + action.request_id, "reservation", 10)
            reply = DispatchReply(action.request_id, identity(action), "pending", "admitted", binding)
            self.invocations += 1
        self.replies[action.request_id] = reply
        if self.drop_submit_ack:
            self.drop_submit_ack = False
            raise FinancialUnknownOutcome("submit", action.request_id, {})
        return reply

    def lookup(self, request_id):
        return None if self.forget_reply else self.replies.get(request_id)

    def finish(self, request_id, *, publish=True, state="completed", producer="finance"):
        prior = self.replies[request_id]
        raw = canonical_bytes({"kind": "result", "payload": asdict(self.result)})
        sha = hashlib.sha256(raw).hexdigest()
        if publish:
            self.mesh.arrive(producer, "financial-result", raw)
        reply = DispatchReply(request_id, prior.action_sha256, state, "durable_outcome", prior.binding,
                              sha if state != "unknown" else None, 7 if state != "unknown" else None)
        self.replies[request_id] = reply
        return raw


class PeerRoleLoopV2Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.now = 100.0
        self.mesh = FakeMesh()
        self.finance = FakeFinance(self.mesh, lambda: self.now)
        self.context = Context("a" * 64, "cohort", "trajectory", 0, "b" * 64)
        self.source_bytes = canonical_bytes({"files": {"src/a.py": "old\n", "README.md": "context\n"}, "base_sha": "a" * 40})
        self.source = self.mesh.arrive("seed", "source-snapshot", self.source_bytes)
        self.evidence_bytes = b'peer findings: source and schema disagree\n'
        self.evidence = self.mesh.arrive("reviewer", "review-findings", self.evidence_bytes)
        self.directive = WorkDirective(self.context, WorkKey("catalog", "requirement", "slot", 0),
                                       "mini", "build", self.source, (self.evidence,), ("src/a.py",), "Fix the schema.")
        self.loop = self.open_loop()

    def tearDown(self):
        self.loop.close()
        self.temp.cleanup()

    def open_loop(self, **kwargs):
        return RoleLoop(self.root, "builder", self.mesh, self.finance, call_limit=kwargs.pop("call_limit", 3),
                        policy_sha256="e" * 64, result_producer="finance", clock=lambda: self.now, **kwargs)

    def restart(self, **kwargs):
        self.loop.close()
        self.loop = self.open_loop(**kwargs)

    def until(self, state, *, limit=30):
        for _ in range(limit):
            result = self.loop.tick()
            if result is not None and result.state == state:
                return result
        self.fail(f"Did not reach {state}: {self.loop.snapshots()}")

    def complete(self):
        current = self.until("pending")
        self.finance.finish(current.action.request_id)
        return self.until("published")

    def test_materializes_full_source_evidence_and_complete_manifest(self):
        self.loop.enqueue(self.directive)
        snapshot = self.until("claim_pending")
        payload = json.loads(self.mesh.resolve(snapshot.action.worker_payload_ref))
        self.assertEqual(set(payload), {"worker_request", "view_manifest_sha256"})
        request = self.loop.worker_request(snapshot.directive_id)
        self.assertEqual(request.files, {"src/a.py": "old\n", "README.md": "context\n"})
        self.assertIn(self.evidence_bytes.decode().rstrip(), request.feedback)
        self.assertEqual(snapshot.view.materialized_content_sha256, worker_request_digest(request))
        self.assertEqual(snapshot.view.evidence_refs, (self.evidence,))
        self.assertEqual(snapshot.view.required_evidence_ids, (self.source.event_id, self.evidence.event_id))
        self.assertEqual(payload["view_manifest_sha256"], identity(snapshot.view))
        self.assertEqual(self.finance.invocations, 0)

    def test_missing_reference_or_bytes_waits_without_claim_or_call(self):
        self.mesh.refs.remove(self.evidence)
        self.loop.enqueue(self.directive)
        self.assertEqual(self.loop.tick().state, "prepared")
        self.assertIn(self.evidence, self.mesh.wants)
        self.mesh.refs.append(self.evidence)
        del self.mesh.blobs[self.evidence.event_id]
        self.assertEqual(self.loop.tick().state, "prepared")
        self.assertEqual(self.finance.claim_requests, [])
        self.assertEqual(self.finance.invocations, 0)
        self.mesh.blobs[self.evidence.event_id] = self.evidence_bytes
        self.assertEqual(self.loop.tick().state, "materialized")

    def test_wrong_provenance_and_changed_bytes_are_not_prompt_context(self):
        self.mesh.refs.remove(self.evidence)
        forged = replace(self.evidence, producer="other")
        self.mesh.refs.append(forged)
        self.assertIsNone(materialize(self.directive, self.mesh, "e" * 64))
        self.mesh.refs[-1] = self.evidence
        self.mesh.blobs[self.evidence.event_id] = b"modified"
        with self.assertRaises(ValueError):
            materialize(self.directive, self.mesh, "e" * 64)

    def test_local_frontier_order_and_durable_assignment_equivalence(self):
        absent = replace(self.evidence, event_id="f" * 64)
        blocked = replace(self.directive, evidence_refs=(absent,))
        ready = replace(self.directive, work=replace(self.directive.work, slot_id="other"))
        self.assertEqual(ready_frontier((blocked, ready), self.mesh.arrived()), (directive_id(ready),))
        self.loop.enqueue(blocked)
        self.loop.enqueue(WorkDirective.from_dict(ready.to_dict()))
        self.restart()
        self.assertEqual(self.loop.tick().directive_id, directive_id(ready))
        other_mesh = FakeMesh("different-role")
        other_mesh.refs = [self.source]
        self.assertEqual(ready_frontier((ready,), other_mesh.arrived()), ())

    def test_arrived_notice_without_bytes_cannot_starve_ready_work(self):
        missing = self.mesh.arrive("reviewer", "review-findings", b"absent payload")
        del self.mesh.blobs[missing.event_id]
        blocked = replace(self.directive, evidence_refs=(missing,))
        ready = replace(self.directive, work=replace(self.directive.work, slot_id="ready"))
        self.loop.enqueue(blocked)
        self.loop.enqueue(ready)
        self.assertEqual(self.loop.tick().directive_id, directive_id(ready))
        self.assertIn(missing, self.mesh.wants)

    def test_lost_claim_ack_restarts_with_exact_claim_identity(self):
        self.finance.drop_claim_ack = True
        self.loop.enqueue(self.directive)
        self.until("claim_pending")
        result = self.loop.tick()
        self.assertEqual(result.state, "claim_pending")
        self.restart()
        self.until("claimed")
        self.assertEqual(len(self.finance.claims), 1)
        self.assertEqual(self.finance.claim_requests[0], self.finance.claim_requests[1])
        self.assertEqual(self.finance.invocations, 0)

    def test_lost_submit_ack_reconciles_one_call_across_restart(self):
        self.finance.drop_submit_ack = True
        self.loop.enqueue(self.directive)
        self.until("submit_pending")
        self.loop.tick()  # lookup before submission
        result = self.loop.tick()  # admission, then dropped acknowledgment
        self.assertEqual(result.state, "submit_pending")
        self.assertEqual(self.finance.invocations, 1)
        self.restart()
        current = self.until("pending")
        self.assertTrue(current.admitted)
        self.assertEqual(len(self.finance.submissions), 1)
        self.finance.finish(current.action.request_id)
        self.until("published")
        self.restart()
        self.assertIsNone(self.loop.tick())
        self.assertEqual(self.finance.invocations, 1)

    def test_capacity_wait_renewal_keeps_original_submitted_lease(self):
        self.finance.waiting = True
        self.loop.enqueue(self.directive)
        self.until("submit_pending")
        self.loop.tick()
        self.loop.tick()
        original = self.finance.submissions[0]
        self.assertEqual(self.finance.invocations, 0)
        self.now = 158.0
        self.loop.tick()  # lookup waiting
        self.assertEqual(self.loop.tick().state, "renew_pending")
        self.finance.drop_renew_ack = True
        self.loop.tick()
        self.restart()
        self.until("submit_pending")
        self.finance.waiting = False
        current = self.until("pending")
        self.assertEqual(self.finance.renew_requests[0], self.finance.renew_requests[1])
        self.assertEqual(self.finance.submissions[-1], original)
        self.assertEqual(current.reply.binding.lease.expires_at, 160.0)
        self.assertEqual(self.finance.invocations, 1)

    def test_pending_missing_reply_and_missing_result_never_reinvoke(self):
        self.loop.enqueue(self.directive)
        current = self.until("pending")
        self.finance.forget_reply = True
        self.assertEqual(self.loop.tick().reason, "admitted_reply_missing_no_reinvoke")
        self.finance.forget_reply = False
        raw = self.finance.finish(current.action.request_id, publish=False)
        self.until("terminal")
        self.restart()
        for _ in range(3):
            self.assertEqual(self.loop.tick().state, "terminal")
        self.mesh.arrive("impostor", "financial-result", raw)
        self.assertEqual(self.loop.tick().state, "terminal")
        self.mesh.arrive("finance", "financial-result", raw)
        self.until("published")
        self.assertEqual(self.finance.invocations, 1)
        self.assertEqual(len(self.finance.submissions), 1)

    def test_publication_lost_ack_keeps_exact_durable_result_reference(self):
        self.loop.enqueue(self.directive)
        current = self.until("pending")
        raw = self.finance.finish(current.action.request_id)
        self.until("terminal")
        self.loop.tick()  # freeze exact arrived result reference before publication
        ref = self.loop.snapshots()[0].result_ref
        self.mesh.drop_publication_ack = True
        self.assertEqual(self.loop.tick().state, "terminal")
        self.mesh.arrive("finance", "financial-result", raw, token="duplicate-announcement")
        self.restart()
        result = self.until("published")
        self.assertEqual(result.result_ref, ref)
        self.assertEqual(len([command for command in self.mesh.commands if command.startswith("result-")]), 1)
        self.assertEqual(self.finance.invocations, 1)

    def test_two_actions_and_call_limit_are_durable_across_restart(self):
        self.loop.close()
        self.root = self.root / "two-call-role"
        self.loop = self.open_loop(call_limit=2)
        for index in range(3):
            self.loop.enqueue(replace(self.directive, work=replace(self.directive.work, slot_id=f"slot-{index}")))
        self.complete()
        self.restart(call_limit=2)
        self.complete()
        stopped = self.until("stopped")
        self.assertEqual(stopped.reason, "role_call_limit")
        self.assertEqual(self.finance.invocations, 2)
        self.assertEqual(sum(value.admitted for value in self.loop.snapshots()), 2)
        with self.assertRaises(RoleError):
            self.restart(call_limit=3)

    def test_failed_attempt_counts_and_attempt_scope_cannot_reset(self):
        directive = replace(self.directive, attempt_limit=2)
        self.loop.enqueue(directive)
        current = self.until("pending")
        self.finance.finish(current.action.request_id, state="failed")
        self.until("published")
        self.loop.enqueue(replace(directive, attempt=2, instructions="Bounded repair with prior evidence."))
        self.complete()
        self.assertEqual(self.finance.invocations, 2)
        self.assertEqual(len(self.finance.claim_requests), 1)
        with self.assertRaises(RoleError):
            self.loop.enqueue(replace(directive, attempt_limit=3, attempt=3))
        with self.assertRaises(RoleError):
            self.loop.enqueue(replace(directive, instructions="Reset first attempt"))

    def test_unknown_outcome_fences_future_actions(self):
        self.loop.enqueue(self.directive)
        self.loop.enqueue(replace(self.directive, work=replace(self.directive.work, slot_id="second")))
        current = self.until("pending")
        self.finance.finish(current.action.request_id, state="unknown", publish=False)
        self.until("terminal")
        self.assertEqual(self.loop.tick().reason, "unknown_outcome_no_reinvoke")
        self.assertEqual(self.loop.tick().reason, "prior_unknown_outcome")
        self.assertEqual(self.finance.invocations, 1)

    def test_late_admission_after_timeout_cannot_free_role_call_slot(self):
        self.loop.close()
        self.root = self.root / "single-call-role"
        self.loop = self.open_loop(call_limit=1, max_renewals=0)
        self.loop.enqueue(self.directive)
        self.loop.enqueue(replace(self.directive, work=replace(self.directive.work, slot_id="second")))
        self.finance.drop_submit_ack = True
        self.until("submit_pending")
        self.loop.tick()
        self.loop.tick()  # Server admits; client does not know it yet.
        self.finance.forget_reply = True  # Models earlier lookup racing the handler.
        self.now = 158.0
        self.loop.tick()
        self.assertEqual(self.loop.tick().reason, "renewal_limit_reconcile_only")
        self.restart(call_limit=1, max_renewals=0)
        self.assertEqual(self.loop.tick().reason, "submitted_reply_missing_no_reinvoke")
        self.assertEqual(self.finance.invocations, 1)
        self.assertEqual(len(self.finance.submissions), 1)
        self.finance.forget_reply = False
        current = self.until("pending")
        self.finance.finish(current.action.request_id)
        self.until("published")
        self.assertEqual(self.loop.tick().reason, "role_call_limit")

    def test_completed_proof_binds_actual_local_request_and_result(self):
        self.loop.enqueue(self.directive)
        finished = self.complete()
        request = self.loop.worker_request(finished.directive_id)
        reply = self.loop.completed_proof(finished.reply.binding, request, self.finance.result,
                                         finished.result_ref.payload_sha256)
        self.assertEqual(reply, finished.reply)
        with self.assertRaises(RoleError):
            self.loop.completed_proof(reply.binding, replace(request, feedback="different"), self.finance.result,
                                      finished.result_ref.payload_sha256)
        with self.assertRaises(RoleError):
            self.loop.completed_proof(reply.binding, request, replace(self.finance.result, summary="forged"),
                                      finished.result_ref.payload_sha256)

    def test_pending_reply_cannot_rebind_admission_or_return_to_waiting(self):
        self.loop.enqueue(self.directive)
        current = self.until("pending")
        reply = current.reply
        self.finance.replies[current.action.request_id] = replace(reply, binding=replace(reply.binding, call_id="another-call"))
        with self.assertRaises(RoleError):
            self.loop.tick()
        self.finance.replies[current.action.request_id] = DispatchReply(current.action.request_id, identity(current.action),
                                                                       "waiting", "executor_capacity")
        with self.assertRaises(RoleError):
            self.loop.tick()

    def test_owner_lock_and_corrupt_journal_fail_closed(self):
        self.loop.enqueue(self.directive)
        with self.assertRaises(RoleError):
            self.open_loop()
        self.loop.close()
        with sqlite3.connect(self.root / "role.sqlite") as db:
            db.execute("UPDATE actions SET body=?", (b'{}',))
        with self.assertRaises(RoleError):
            self.open_loop()

    def test_deleted_config_cannot_rebind_call_allowance(self):
        self.loop.enqueue(self.directive)
        self.until("pending")
        self.loop.close()
        with sqlite3.connect(self.root / "role.sqlite") as db:
            db.execute("DELETE FROM config")
        with self.assertRaises(RoleError):
            self.open_loop(call_limit=4)
        with sqlite3.connect(self.root / "role.sqlite") as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM config").fetchone()[0], 0)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM actions").fetchone()[0], 1)

    def test_dropped_config_table_cannot_be_recreated_on_resume(self):
        self.loop.close()
        with sqlite3.connect(self.root / "role.sqlite") as db:
            db.execute("DROP TABLE config")
        with self.assertRaises(RoleError):
            self.open_loop()
        with sqlite3.connect(self.root / "role.sqlite") as db:
            tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertNotIn("config", tables)

    def test_deleted_admitted_action_fails_retained_membership(self):
        self.loop.enqueue(self.directive)
        self.until("pending")
        self.loop.close()
        with sqlite3.connect(self.root / "role.sqlite") as db:
            db.execute("DELETE FROM actions")
        with self.assertRaisesRegex(RoleError, "membership"):
            self.open_loop()
        self.assertEqual(self.finance.invocations, 1)

    def test_deleted_tail_action_and_history_still_fail_append_anchor(self):
        self.loop.enqueue(self.directive)
        self.loop.enqueue(replace(self.directive, work=replace(self.directive.work, slot_id="tail")))
        self.loop.close()
        with sqlite3.connect(self.root / "role.sqlite") as db:
            db.execute("DELETE FROM history WHERE action_id IN (SELECT id FROM actions WHERE ordinal=2)")
            db.execute("DELETE FROM actions WHERE ordinal=2")
        with self.assertRaisesRegex(RoleError, "membership"):
            self.open_loop()

    def test_retained_owner_with_missing_database_does_not_bootstrap(self):
        self.loop.close()
        (self.root / "role.sqlite").rename(self.root / "retained-role.sqlite")
        with self.assertRaisesRegex(RoleError, "database is missing"):
            self.open_loop()
        self.assertFalse((self.root / "role.sqlite").exists())

    def test_existing_empty_database_is_not_a_fresh_role(self):
        self.loop.close()
        self.root = self.root / "partial-initialization"
        self.root.mkdir()
        with sqlite3.connect(self.root / "role.sqlite"):
            pass
        with self.assertRaisesRegex(RoleError, "schema"):
            self.open_loop()

    def test_orphan_database_sidecar_is_not_a_fresh_role(self):
        self.loop.close()
        self.root = self.root / "orphan-sidecar"
        self.root.mkdir()
        (self.root / "role.sqlite-wal").write_bytes(b"retained but incomplete database")
        with self.assertRaisesRegex(RoleError, "database is missing"):
            self.open_loop()
        self.assertFalse((self.root / "role.sqlite").exists())

    def test_retained_admission_flag_must_match_dispatch_binding(self):
        self.loop.enqueue(self.directive)
        self.until("pending")
        self.loop.close()
        with sqlite3.connect(self.root / "role.sqlite") as db:
            raw = db.execute("SELECT body FROM actions").fetchone()[0]
            body = json.loads(raw)
            body["admitted"] = False
            changed = canonical_bytes(body)
            db.execute("UPDATE actions SET body=?,digest=?", (changed, hashlib.sha256(changed).hexdigest()))
        with self.assertRaisesRegex(RoleError, "admission"):
            self.open_loop()

    def test_failed_append_rolls_back_membership_anchor_atomically(self):
        self.loop.db.execute("""CREATE TRIGGER reject_append BEFORE INSERT ON actions
            BEGIN SELECT RAISE(ABORT, 'fixture append interruption'); END""")
        with self.assertRaises(sqlite3.IntegrityError):
            self.loop.enqueue(self.directive)
        self.assertEqual(self.loop.snapshots(), ())
        self.loop.db.execute("DROP TRIGGER reject_append")
        self.loop.enqueue(self.directive)
        self.restart()
        self.assertEqual(len(self.loop.snapshots()), 1)

    def test_crash_boundaries_resume_exact_action_without_duplicate_call(self):
        self.loop.enqueue(self.directive)
        fired = set()

        def crash(state, key):
            if state not in fired:
                fired.add(state)
                raise InjectedCrash(state)

        self.loop.crash_hook = crash
        published = False
        for _ in range(60):
            try:
                result = self.loop.tick()
            except InjectedCrash:
                self.restart(crash_hook=crash)
                continue
            snapshots = self.loop.snapshots()
            if snapshots[0].state == "pending":
                self.finance.finish(snapshots[0].action.request_id)
            if snapshots[0].state == "published":
                published = True
                break
        self.assertTrue(published)
        self.assertEqual(self.finance.invocations, 1)
        self.assertEqual(len(self.finance.claims), 1)
        self.assertTrue({"materialized", "claim_pending", "claimed", "submit_pending", "pending", "terminal", "published"} <= fired)


if __name__ == "__main__":
    unittest.main()
