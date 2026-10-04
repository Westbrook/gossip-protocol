from dataclasses import asdict
import unittest

from gossip_harness.peer_financial_rpc_v2 import _signed, PROTOCOL, SCHEMA_VERSION
from gossip_harness.peer_financial_rpc_v5 import FinancialRPCV5
from gossip_harness.peer_financial_terminal_v1 import CohortSealed
from gossip_harness.peer_project_contract_v2 import to_dict
from tests.financial_v5_fixture import Fixture


class PeerFinancialRPCV5Tests(Fixture, unittest.TestCase):
    def rpc(self):
        self.keys = {
            actor: format(index + 1, "064x")
            for index, actor in enumerate(self.child.actors)
        }
        return FinancialRPCV5(
            self.authority,
            self.keys,
            expected_financial_config_sha256=self.authority.config_sha256,
            request_guard=lambda action: self.payloads.read_owned(
                action.actor, action.worker_payload_ref.payload_sha256
            ),
            request_guard_sha256="a" * 64,
        )

    def envelope(self, operation, request_id, payload):
        return _signed(
            {
                "protocol": PROTOCOL,
                "schema_version": SCHEMA_VERSION,
                "contract_sha256": "a" * 64,
                "actor": self.actor,
                "request_id": request_id,
                "nonce": "a" * 32,
                "operation": operation,
                "payload": payload,
            },
            self.keys[self.actor],
        )

    def test_exact_claim_replay_after_seal_new_claim_and_renew_denied(self):
        self.open()
        rpc = self.rpc()
        payload = {
            "context": to_dict(self.context),
            "work": to_dict(self.works[0]),
            "ttl": 600,
        }
        request = self.envelope("claim", "claim0", payload)
        original = rpc.handle(request)["body"]["receipt"]
        self.assertEqual(original["status"], "ok")
        self.seal()
        before = self.snapshot()
        self.assertEqual(rpc.handle(request)["body"]["receipt"], original)
        denied = rpc.handle(self.envelope("claim", "claim1", payload))["body"][
            "receipt"
        ]
        self.assertEqual(denied, {"status": "denied", "reason": "cohort_sealed"})
        renewed = rpc.handle(
            self.envelope("renew", "renew1", {"lease": original["body"], "ttl": 600})
        )["body"]["receipt"]
        self.assertEqual(renewed, denied)
        self.assertEqual(self.snapshot(), before)

    def test_new_rpc_registration_cannot_mutate_sealed_census(self):
        self.open()
        self.seal()
        with self.assertRaises(CohortSealed):
            self.rpc()

    def test_exact_registration_and_completed_dispatch_replay_remain_readable(self):
        self.open()
        rpc = self.rpc()
        action, lease = self.start()
        self.terminal(action)
        request = self.envelope(
            "submit",
            action.request_id,
            {"action": to_dict(action), "lease": asdict(lease)},
        )
        original = rpc.handle(request)["body"]["receipt"]
        self.seal()
        same = self.rpc()
        self.assertEqual(same.handle(request)["body"]["receipt"], original)
        self.assertEqual(len(self.transport.calls), 1)

    def test_existing_waiting_intent_rejected_before_missing_payload_return(self):
        self.open()
        rpc = self.rpc()
        action = self.action()
        lease = self.authority.claim(self.actor, self.context, self.works[0])
        self.payloads.values.clear()
        request = self.envelope(
            "submit",
            action.request_id,
            {"action": to_dict(action), "lease": asdict(lease)},
        )
        waiting = rpc.handle(request)["body"]["receipt"]
        self.assertEqual(waiting["status"], "ok")
        self.seal()
        self.assertEqual(
            rpc.handle(request)["body"]["receipt"],
            {"status": "denied", "reason": "cohort_sealed"},
        )
        self.assertEqual(len(self.transport.calls), 0)

    def test_later_rpc_table_creation_does_not_change_previous_child_census(self):
        self.open()
        self.seal()
        self.select(1)
        self.open()
        self.rpc()
        self.seal()
        self.select(2)
        self.open()

    def test_rpc_committed_before_seal_invalidates_prepared_census(self):
        from gossip_harness.peer_financial_authority_v2 import FinancialError

        self.open()
        rpc = self.rpc()
        prepared = self.authority.prepare_terminal_snapshot(self.evidence())
        payload = {
            "context": to_dict(self.context),
            "work": to_dict(self.works[0]),
            "ttl": 600,
        }
        response = rpc.handle(self.envelope("claim", "racing-claim", payload))["body"][
            "receipt"
        ]
        self.assertEqual(response["status"], "ok")
        with self.assertRaises(FinancialError):
            self.authority.seal_terminal(prepared)
        self.assertEqual(self.rows("financial_terminal_seals_v4"), [])

    def test_rpc_joins_after_sql_seal_denied_without_new_intent(self):
        import threading

        self.open()
        rpc = self.rpc()
        prepared = self.authority.prepare_terminal_snapshot(self.evidence())
        request = self.envelope(
            "claim",
            "racing-after",
            {
                "context": to_dict(self.context),
                "work": to_dict(self.works[0]),
                "ttl": 600,
            },
        )
        started = threading.Event()
        responses = []

        def attempt():
            started.set()
            responses.append(rpc.handle(request)["body"]["receipt"])

        thread = threading.Thread(target=attempt)

        def hook(point, request_id):
            if point == "before_terminal_sql_commit":
                thread.start()
                if not started.wait(2):
                    raise RuntimeError("fixture thread never started")

        self.authority.crash_hook = hook
        self.authority.seal_terminal(prepared)
        thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(responses, [{"status": "denied", "reason": "cohort_sealed"}])
        self.assertEqual(self.rows("financial_rpc_requests_v2"), [])

    def test_known_failed_dispatch_remains_failed_replay_after_completed_seal(self):
        self.open()
        rpc = self.rpc()
        self.transport.proposal = {"summary": "bad", "changes": []}
        action, lease = self.start()
        failed = self.terminal(action)
        self.assertEqual(failed.state, "failed")
        request = self.envelope(
            "submit",
            action.request_id,
            {"action": to_dict(action), "lease": asdict(lease)},
        )
        original = rpc.handle(request)["body"]["receipt"]
        self.seal()
        self.assertEqual(rpc.handle(request)["body"]["receipt"], original)
        self.assertEqual(len(self.transport.calls), 1)
        self.assertEqual(
            self.authority.lookup(self.actor, action.request_id).state, "failed"
        )

    def test_v5_rpc_config_binds_new_protocol_and_closure_policy(self):
        from gossip_harness.peer_financial_terminal_v2 import CLOSURE_POLICY_SHA256

        self.open()
        rpc = self.rpc()
        self.assertEqual(rpc.config["protocol"], "peer-financial-rpc-v5")
        self.assertEqual(
            rpc.config["financial_protocol"], "peer-financial-authority-v5"
        )
        self.assertEqual(
            rpc.config["financial_closure_policy_sha256"], CLOSURE_POLICY_SHA256
        )
