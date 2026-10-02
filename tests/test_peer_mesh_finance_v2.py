"""Exact local mesh provenance and service-result recovery; no sockets/provider."""
from __future__ import annotations

from contextlib import ExitStack
from dataclasses import replace
import hashlib
from pathlib import Path
import sqlite3
import tempfile
import unittest

from gossip_harness.peer_mesh_finance_v2 import MeshFinanceError, MeshFinancePayloads
from gossip_harness.peer_mesh_v2 import MeshConfig, MeshLimits, MeshNode
from gossip_harness.peer_project_contract_v2 import ActionRequest, Context, EvidenceRef, WorkKey

CONTRACT = "a" * 64
ROSTER = ("B01", "B02", "finance")


class PeerMeshFinanceV2Tests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.root = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        self.sender = self.node("B01")
        self.finance_mesh = self.node("finance")
        self.payloads = self.adapter()

    def node(self, actor, **changes):
        node = MeshNode(MeshConfig(self.root / actor, actor, "cohort", CONTRACT, ROSTER,
                                   "t" * 32, **changes))
        self.stack.callback(node.close)
        return node

    def adapter(self, **changes):
        adapter = MeshFinancePayloads(self.root / "index", self.finance_mesh, ("B01", "B02"), **changes)
        self.stack.callback(adapter.close)
        return adapter

    def action(self, ref):
        return ActionRequest(Context(CONTRACT, "cohort", "trajectory", 0, "b" * 64),
                             "build", "request", "B01", "build", WorkKey("catalog", "read", "B01", 0),
                             "builder", ref, "c" * 64)

    def request(self, raw=b'{"worker_request":"fixture-only"}'):
        return self.sender.publish("worker-request", raw, "worker-request")

    def deliver(self, ref, *, bytes_too=True):
        notice = self.sender.store.notice(ref)
        self.finance_mesh.store.merge([notice])
        if bytes_too:
            self.assertTrue(self.finance_mesh.want(ref))
            for index in range(len(notice["chunks"])):
                self.finance_mesh.store.accept_chunk(ref.payload_sha256, index,
                                                    self.sender.store.read_chunk(ref.payload_sha256, index))

    def test_guard_requires_exact_arrived_reference_not_matching_bytes(self):
        ref = self.request()
        self.deliver(ref)
        self.payloads.request_guard(self.action(ref))
        fabricated = replace(ref, event_id="f" * 64)
        with self.assertRaises(FileNotFoundError):
            self.payloads.request_guard(self.action(fabricated))
        self.assertEqual(self.payloads.read_owned("B01", ref.payload_sha256), self.sender.resolve(ref))

    def test_notice_without_bytes_waits_and_requests_only_that_payload(self):
        ref = self.request()
        with self.assertRaises(FileNotFoundError):
            self.payloads.request_guard(self.action(ref))
        self.assertEqual(self.finance_mesh.summary()["payloads"], 0)
        self.deliver(ref, bytes_too=False)
        with self.assertRaises(FileNotFoundError):
            self.payloads.request_guard(self.action(ref))
        self.assertEqual(self.finance_mesh.summary()["payloads"], 1)
        self.assertEqual(list(self.payloads.db.execute("SELECT * FROM results")), [])
        self.deliver(ref)
        self.payloads.request_guard(self.action(ref))

    def test_wrong_actor_kind_contract_and_cohort_cannot_borrow_arrival(self):
        ref = self.request()
        self.deliver(ref)
        action = self.action(ref)
        with self.assertRaises(ValueError):
            replace(action, actor="B02")
        with self.assertRaises(ValueError):
            replace(action, worker_payload_ref=replace(ref, kind="source"))
        with self.assertRaises(FileNotFoundError):
            self.payloads.request_guard(replace(action, actor="B02", worker_payload_ref=replace(ref, producer="B02")))
        variants = [replace(action, context=replace(action.context, cohort_id="other")),
                    replace(action, context=replace(action.context, execution_contract_sha256="d" * 64))]
        for changed in variants:
            with self.subTest(action=changed), self.assertRaises(MeshFinanceError):
                self.payloads.request_guard(changed)
        with self.assertRaises(FileNotFoundError):
            self.payloads.read_owned("B02", ref.payload_sha256)

    def test_results_publish_as_service_and_keep_principal_ownership(self):
        data = b'{"kind":"result","payload":{"usage_units":1}}'
        sha = self.payloads.put_owned("B01", data)
        ref, = self.finance_mesh.arrived()
        self.assertEqual((ref.producer, ref.kind, ref.payload_sha256), ("finance", "financial-result", sha))
        self.assertEqual(self.payloads.read_owned("B01", sha), data)
        with self.assertRaises(MeshFinanceError):
            self.payloads.read_owned("B02", sha)
        self.assertEqual(self.payloads.put_owned("B02", data), sha)
        self.assertEqual(self.payloads.read_owned("B02", sha), data)
        self.assertEqual(len(self.finance_mesh.arrived()), 2)

    def test_reopen_preserves_config_and_idempotent_result_publication(self):
        data = b"retained result"
        sha = self.payloads.put_owned("B01", data)
        original = self.finance_mesh.arrived()
        self.payloads.close()
        restarted = self.adapter()
        self.assertEqual(restarted.put_owned("B01", data), sha)
        self.assertEqual(restarted.read_owned("B01", sha), data)
        self.assertEqual(self.finance_mesh.arrived(), original)

    def test_restart_after_publication_before_index_ack_has_one_event(self):
        def crash(point):
            if point == "publication_durable":
                raise RuntimeError("lost service publication acknowledgment")
        self.payloads.close()
        crashing = self.adapter(crash_hook=crash)
        with self.assertRaises(RuntimeError):
            crashing.put_owned("B01", b"known result")
        original = self.finance_mesh.arrived()
        self.assertEqual(len(original), 1)
        crashing.close()
        restarted = self.adapter()
        sha = restarted.put_owned("B01", b"known result")
        self.assertEqual(restarted.read_owned("B01", sha), b"known result")
        self.assertEqual(self.finance_mesh.arrived(), original)

    def test_restart_after_intent_never_claims_unpublished_result_available(self):
        def crash(point):
            if point == "intent_durable":
                raise RuntimeError("stopped before publish")
        self.payloads.close()
        crashing = self.adapter(crash_hook=crash)
        with self.assertRaises(RuntimeError):
            crashing.put_owned("B01", b"known result")
        sha = hashlib.sha256(b"known result").hexdigest()
        with self.assertRaises(FileNotFoundError):
            crashing.read_owned("B01", sha)
        self.assertEqual(self.finance_mesh.arrived(), ())
        crashing.close()
        restarted = self.adapter()
        self.assertEqual(restarted.put_owned("B01", b"known result"), sha)
        self.assertEqual(restarted.read_owned("B01", sha), b"known result")

    def test_deleted_result_index_cannot_fall_back_to_actor_request(self):
        data = b"same bytes from different provenance"
        sha = self.payloads.put_owned("B01", data)
        ref = self.request(data)
        self.deliver(ref)
        with sqlite3.connect(self.root / "index" / "payload-index.sqlite") as db:
            db.execute("DELETE FROM results")
        with self.assertRaisesRegex(MeshFinanceError, "membership"):
            self.payloads.read_owned("B01", sha)
        with self.assertRaisesRegex(MeshFinanceError, "membership"):
            self.payloads.put_owned("B01", data)

    def test_retained_payload_corruption_fails_closed_on_read_and_reopen(self):
        sha = self.payloads.put_owned("B01", b"retained")
        with sqlite3.connect(self.root / "index" / "payload-index.sqlite") as db:
            db.execute("UPDATE results SET payload=?", (b"changed",))
        with self.assertRaises(MeshFinanceError):
            self.payloads.read_owned("B01", sha)
        self.payloads.close()
        with self.assertRaises(MeshFinanceError):
            self.adapter()

    def test_corrupted_registration_blocks_existing_result_replay(self):
        sha = self.payloads.put_owned("B01", b"retained")
        with sqlite3.connect(self.root / "index" / "payload-index.sqlite") as db:
            db.execute("UPDATE registration SET config=?", (b"different config",))
        with self.assertRaisesRegex(MeshFinanceError, "registration"):
            self.payloads.read_owned("B01", sha)
        with self.assertRaisesRegex(MeshFinanceError, "registration"):
            self.payloads.put_owned("B01", b"retained")

    def test_adapter_single_owner_and_roster_are_immutable(self):
        with self.assertRaisesRegex(MeshFinanceError, "owner"):
            self.adapter()
        self.payloads.close()
        with self.assertRaisesRegex(MeshFinanceError, "registration"):
            MeshFinancePayloads(self.root / "index", self.finance_mesh, ("B01",))
        with self.assertRaisesRegex(MeshFinanceError, "distinct"):
            MeshFinancePayloads(self.root / "bad", self.finance_mesh, ("finance",))

    def test_subscription_backpressure_does_not_make_missing_bytes_available(self):
        self.payloads.close()
        self.finance_mesh.close()
        # Separate immutable mesh/index roots; changing a retained limit is forbidden.
        mesh = MeshNode(MeshConfig(self.root / "limited", "finance", "cohort", CONTRACT, ROSTER,
                                   "t" * 32, limits=replace(MeshLimits(), max_payloads=1)))
        self.stack.callback(mesh.close)
        limited = MeshFinancePayloads(self.root / "limited-index", mesh, ("B01", "B02"))
        self.stack.callback(limited.close)
        mesh.publish("source", b"already reserved", "fill")
        ref = self.request()
        mesh.store.merge([self.sender.store.notice(ref)])
        with self.assertRaisesRegex(FileNotFoundError, "backpressure"):
            limited.request_guard(self.action(ref))
        self.assertIsNone(mesh.resolve(ref))
        self.assertEqual(mesh.summary()["payloads"], 1)
