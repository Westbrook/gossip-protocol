"""Real V3 financial ownership over retained V2 wire semantics; no provider use."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict, replace
import hashlib
from pathlib import Path
import sqlite3
import threading
import time
import unittest

from gossip_harness.ledger import Ledger
from gossip_harness.peer_financial_authority_v2 import CumulativeAuthorityV2, ledger_identity
from gossip_harness.peer_financial_authority_v3 import (
    CumulativeAuthorityV3, FIXTURE_TRANSPORT, PERMIT_PROTOCOL, digest, profile_manifest, source_fingerprints,
)
from gossip_harness.peer_financial_rpc_v2 import (
    PROTOCOL as WIRE_PROTOCOL, SCHEMA_VERSION as WIRE_SCHEMA_VERSION,
    FinancialRPC as FinancialRPCV2, FinancialRPCError, FinancialUnknownOutcome, _signed, _verified,
)
from gossip_harness.peer_financial_rpc_v3 import FinancialRPCV3, PROTOCOL, SCHEMA_VERSION, SOURCES
from gossip_harness.peer_project_contract_v2 import to_dict
from tests.test_peer_financial_rpc_v2 import RPCFixture


class RPCV3Fixture(RPCFixture):
    def make_permit(self):
        sources = source_fingerprints()
        repo = Path(__file__).resolve().parent.parent
        sources.update({name: hashlib.sha256((repo / name).read_bytes()).hexdigest() for name in SOURCES})
        profiles = {"mini": profile_manifest(self.worker)}
        design = {"profiles": profiles, "max_workers": 2,
                  "action_limits": {"total": 20, "by_kind": {"build": 20},
                                    "by_kind_generation": {"build": {"0": 20}},
                                    "by_actor": {actor: 1 for actor in self.keys}}}
        return {"protocol": PERMIT_PROTOCOL, "mode": "fixture", "approval_ref": "disposable-rpc-fixture-only",
                "execution_design": design, "execution_design_sha256": digest(design),
                "cohort_contract_sha256": digest(self.contract), "sources": sources, "profiles": profiles,
                "incremental_cap_micro_usd": 900_000, "expected_global_cap": 1_000_000,
                "expected_opening_usage": 1000, "max_workers": 2, "qualification": None}

    def open(self, recovery=False, **rpc_kwargs):
        self.contract["transport_identity"] = FIXTURE_TRANSPORT
        self.permit = self.make_permit()
        self.finance = CumulativeAuthorityV3.open(
            self.path, self.root / "service", self.contract, 900_000, 1_000_000, 1000,
            payloads=self.payloads, workers={"mini": self.worker}, max_workers=2,
            clock=self.clock, recovery=recovery, mode="fixture", permit=self.permit,
            expected_permit_sha256=digest(self.permit))
        self.rpc = self.new_rpc(self.keys, **rpc_kwargs)

    def new_rpc(self, keys, **kwargs):
        return FinancialRPCV3(self.finance, keys,
            expected_financial_config_sha256=kwargs.pop("expected_financial_config_sha256", self.finance.config_sha256),
            request_guard=self.guard, request_guard_sha256="e" * 64,
            max_requests_per_principal=kwargs.pop("max_requests_per_principal", 4), **kwargs)


class PeerFinancialRPCV3Tests(RPCV3Fixture, unittest.TestCase):
    def test_exact_v3_fixture_owner_registers_distinct_adapter_and_unchanged_wire(self):
        self.assertEqual(type(self.finance), CumulativeAuthorityV3)
        self.assertEqual(self.rpc.config["protocol"], PROTOCOL)
        self.assertEqual(self.rpc.config["schema_version"], SCHEMA_VERSION)
        self.assertEqual(self.rpc.config["wire_protocol"], WIRE_PROTOCOL)
        self.assertEqual(self.rpc.config["wire_schema_version"], WIRE_SCHEMA_VERSION)
        self.assertEqual(self.rpc.config["financial_mode"], "fixture")
        self.assertEqual(self.rpc.config["financial_config_sha256"], self.finance.config_sha256)
        self.assertEqual(self.rpc.config["financial_operator_permit_sha256"], digest(self.permit))
        self.assertEqual(self.new_rpc(self.keys).config_sha256, self.rpc.config_sha256)
        self.assertEqual(len(self.rpc.config["adapter_sources"]), 2)
        self.assertTrue(all(key not in str(self.rpc.config) for key in self.keys.values()))

    def test_v2_still_refuses_v3_without_relabeling_owner(self):
        before = self.rows("financial_rpc_config_v2")
        with self.assertRaises(FinancialRPCError):
            FinancialRPCV2(self.finance, self.keys, request_guard=self.guard, request_guard_sha256="e" * 64)
        self.assertEqual(before, self.rows("financial_rpc_config_v2"))
        self.assertEqual(self.finance.config["mode"], "fixture")
        self.assertEqual(self.transport.calls, [])

    def test_constructor_rejects_wrong_explicit_financial_pin_without_mutation(self):
        before = self.rows("financial_rpc_config_v2")
        with self.assertRaises(FinancialRPCError):
            self.new_rpc(self.keys, expected_financial_config_sha256="f" * 64)
        self.assertEqual(before, self.rows("financial_rpc_config_v2"))
        self.assertEqual(self.transport.calls, [])

    def test_current_financial_config_and_permit_drift_refuse_before_rpc_intent(self):
        original = self.finance.config["incremental_cap_micro_usd"]
        self.finance.config["incremental_cap_micro_usd"] = original + 1
        with self.assertRaises(FinancialRPCError):
            self.call("claim", payload=self.claim_payload())
        self.finance.config["incremental_cap_micro_usd"] = original
        self.finance.permit["approval_ref"] = "changed"
        with self.assertRaises(FinancialRPCError):
            self.call("claim", payload=self.claim_payload())
        self.assertEqual(self.rows("financial_rpc_requests_v2"), [])
        self.assertEqual(self.transport.calls, [])

    def test_permit_must_bind_both_rpc_sources_before_registration(self):
        path = self.root / "other-disposable.sqlite"
        Ledger(path, 1_000_000)
        contract = deepcopy(self.contract)
        contract.update(ledger_identity=ledger_identity(path), journal_root=str(self.root / "other-journal"))
        permit = deepcopy(self.permit)
        permit.update(cohort_contract_sha256=digest(contract), expected_opening_usage=0)
        permit["sources"].pop("gossip_harness/peer_financial_rpc_v3.py")
        finance = CumulativeAuthorityV3.open(path, self.root / "other-service", contract, 900_000, 1_000_000, 0,
            payloads=self.payloads, workers={"mini": self.worker}, max_workers=2, clock=self.clock,
            mode="fixture", permit=permit, expected_permit_sha256=digest(permit))
        try:
            with self.assertRaises(FinancialRPCError):
                FinancialRPCV3(finance, self.keys, expected_financial_config_sha256=finance.config_sha256,
                    request_guard=self.guard, request_guard_sha256="e" * 64)
            with sqlite3.connect(path) as db:
                self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='financial_rpc_config_v2'").fetchone())
        finally:
            finance.close()

    def test_v3_adapter_refuses_actual_v2_owner_before_rpc_enrollment(self):
        path = self.root / "legacy-disposable.sqlite"
        Ledger(path, 1_000_000)
        contract = deepcopy(self.contract)
        contract.update(ledger_identity=ledger_identity(path), journal_root=str(self.root / "legacy-journal"),
                        transport_identity="closed-offline-fixture")
        finance = CumulativeAuthorityV2.open(path, self.root / "legacy-service", contract, 900_000, 1_000_000, 0,
            payloads=self.payloads, workers={"mini": self.worker}, max_workers=2, clock=self.clock)
        try:
            with self.assertRaises(FinancialRPCError):
                FinancialRPCV3(finance, self.keys, expected_financial_config_sha256=finance.config_sha256,
                    request_guard=self.guard, request_guard_sha256="e" * 64)
            with sqlite3.connect(path) as db:
                self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='financial_rpc_config_v2'").fetchone())
        finally:
            finance.close()

    def test_adapter_does_not_accept_closing_or_other_process_owner(self):
        self.finance.owner_pid += 1
        with self.assertRaises(FinancialRPCError):
            self.new_rpc(self.keys)
        self.finance.owner_pid -= 1
        self.finance.close()
        with self.assertRaises(FinancialRPCError):
            self.new_rpc(self.keys)
        self.assertEqual(self.transport.calls, [])

    def test_authenticated_request_still_binds_actor_contract_nonce_and_exact_lease(self):
        envelope = self.request("claim", payload=self.claim_payload())
        for change in ({"protocol": PROTOCOL}, {"schema_version": SCHEMA_VERSION},
                       {"contract_sha256": "f" * 64}, {"actor": "role01"}, {"nonce": "short"}):
            with self.subTest(change=change), self.assertRaises(FinancialRPCError):
                self.rpc.handle(_signed({**envelope["body"], **change}, self.keys["role00"]))
        first = self.call("claim", payload=self.claim_payload())
        self.clock.now += 10
        self.assertEqual(self.call("claim", payload=self.claim_payload(), nonce="1" * 32), first)
        self.assertEqual(len(self.rows("financial_rpc_requests_v2")), 1)
        self.assertEqual(self.transport.calls, [])

    def test_exact_request_recovery_keeps_rpc_configuration_and_lease_epoch(self):
        first = self.call("claim", payload=self.claim_payload(ttl=1))
        config_sha = self.rpc.config_sha256
        self.finance.close()
        self.open(recovery=True)
        self.clock.now += 100
        self.assertEqual(self.call("claim", payload=self.claim_payload(ttl=1)), first)
        self.assertEqual(self.rpc.config_sha256, config_sha)
        self.assertEqual(first["body"]["epoch"], 1)
        with self.assertRaises(FinancialRPCError):
            self.new_rpc({**self.keys, "role00": "changed-capability" * 4})

    def test_request_guard_still_precedes_financial_admission(self):
        lease, action = self.claim(), self.action()
        self.arrived.clear()
        reply = self.call("submit", action.request_id, {"action": to_dict(action), "lease": asdict(lease)})
        self.assertEqual(reply["body"]["reason"], "payload_unavailable")
        self.assertEqual(self.rows("financial_actions_v2"), [])
        self.assertEqual(self.transport.calls, [])
        changed = replace(action, worker_payload_ref=replace(action.worker_payload_ref, event_id="f" * 64))
        denied = self.call("submit", action.request_id, {"action": to_dict(changed), "lease": asdict(lease)})
        self.assertEqual(denied["reason"], "request_identity_conflict")

    def test_corrupt_rpc_registration_or_membership_remains_fail_closed(self):
        self.call("claim", payload=self.claim_payload())
        with sqlite3.connect(self.path) as db:
            db.execute("DELETE FROM financial_rpc_requests_v2")
        with self.assertRaises(FinancialRPCError):
            self.call("claim", payload=self.claim_payload())
        with self.assertRaises(FinancialRPCError):
            self.new_rpc(self.keys)
        self.assertEqual(self.transport.calls, [])


class PeerFinancialRPCTCPV3Tests(RPCV3Fixture, unittest.TestCase):
    def test_real_v3_fixture_tcp_claim_submit_terminal_lookup_and_exact_replay(self):
        client = self.start()
        ready = self.server.readiness()
        self.assertEqual(ready["protocol"], WIRE_PROTOCOL)
        self.assertEqual(ready["config_sha256"], self.rpc.config_sha256)
        lease = client.claim(self.context, self.works["role00"], "claim")
        renewed = client.renew(lease, "renew")
        action = self.action()
        submitted = client.submit(action, renewed)
        self.assertIn(submitted.state, {"pending", "completed"})
        result = self.terminal(client, action.request_id)
        self.assertEqual((result.state, result.usage_units), ("completed", 166))
        self.assertEqual(self.finance.verified_terminal(result.binding)["result"].changes, {"src/a.py": "fixed\n"})
        self.assertEqual(client.submit(action, renewed), result)
        self.assertEqual(len(self.transport.calls), 1)
        self.assertEqual(self.historical.budget()["spent_or_reserved"], 1166)

    def test_lost_response_replays_same_financial_call_and_other_roles_remain_responsive(self):
        client = self.start()
        lease = client.claim(self.context, self.works["role00"], "claim")
        action = self.action()
        self.transport.release.clear()
        dropped = []
        def drop(point, request_id):
            if point == "before_send" and request_id == action.request_id and not dropped:
                dropped.append(request_id)
                raise OSError("Fixture lost acknowledgement")
        self.rpc.crash_hook = drop
        with self.assertRaises(FinancialUnknownOutcome):
            client.submit(action, lease)
        self.assertTrue(self.transport.entered.wait(2))
        self.assertEqual(client.lookup(action.request_id).state, "pending")
        self.assertIsNone(self.client("role01").lookup(action.request_id))
        self.assertEqual(client.submit(action, lease).state, "pending")
        self.assertEqual(len(self.transport.calls), 1)
        self.transport.release.set()
        self.assertEqual(self.terminal(client, action.request_id).state, "completed")
        self.assertEqual(len(self.transport.calls), 1)

    def test_twenty_real_principals_share_service_without_provider_calls(self):
        from concurrent.futures import ThreadPoolExecutor
        self.start()
        barrier = threading.Barrier(20)
        def claim(actor):
            barrier.wait(timeout=5)
            return self.client(actor).claim(self.context, self.works[actor], "claim")
        with ThreadPoolExecutor(max_workers=20) as pool:
            leases = list(pool.map(claim, self.keys))
        self.assertEqual({lease.worker_id for lease in leases}, set(self.keys))
        self.assertTrue(all(lease.epoch == 1 for lease in leases))
        self.assertEqual(self.transport.calls, [])
        self.assertEqual(self.historical.budget()["spent_or_reserved"], 1000)

    def test_tampered_signed_response_and_foreign_actor_never_authorize_calls(self):
        client = self.start()
        lease = client.claim(self.context, self.works["role00"], "claim")
        action = self.action()
        with self.assertRaises(FinancialUnknownOutcome):
            self.client("role01").submit(action, lease)
        original = self.rpc.handle
        def wrong_nonce(envelope):
            response = original(envelope)
            actor = response["body"]["actor"]
            return _signed({**response["body"], "nonce": "f" * 32}, self.keys[actor])
        self.rpc.handle = wrong_nonce
        with self.assertRaises(FinancialUnknownOutcome):
            client.lookup(action.request_id)
        self.rpc.handle = original
        self.assertIsNone(client.lookup(action.request_id))
        self.assertEqual(self.transport.calls, [])
