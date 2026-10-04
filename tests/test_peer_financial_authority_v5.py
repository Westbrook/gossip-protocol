"""Isolated accounting/closure controls; no provider or project acceptance."""

import sqlite3
import unittest
from copy import deepcopy
from gossip_harness.peer_financial_authority_v2 import FinancialError, ledger_identity
from gossip_harness.peer_financial_authority_v4 import CumulativeAuthorityV4
from gossip_harness.peer_financial_authority_v5 import validate_qualification
from gossip_harness.peer_financial_terminal_v1 import SealBusy, CohortSealed, digest
from gossip_harness.peer_financial_terminal_v2 import (
    CLOSURE_POLICY_SHA256,
    checked_financial_config,
    checked_policy,
    verified_study_barrier,
)
from tests.financial_v5_fixture import Fixture
from gossip_harness.verification_journal import JournalCorrupt


class PeerFinancialAuthorityV5Tests(Fixture, unittest.TestCase):
    def failed_then_repaired(self):
        self.open()
        self.transport.proposal = {"summary": "bad", "changes": []}
        first, _ = self.start(0)
        failed = self.terminal(first)
        self.assertEqual(failed.state, "failed")
        self.assertIsInstance(failed.usage_units, int)
        self.transport.proposal = {
            "summary": "repaired",
            "changes": [{"path": "src/a.py", "content": "fixed\n"}],
        }
        second, _ = self.start(1)
        completed = self.terminal(second)
        self.assertEqual(completed.state, "completed")
        return first, failed, second, completed

    def test_real_known_failure_and_repair_remain_counted_charged_and_seal_completed(
        self,
    ):
        first, failed, second, completed = self.failed_then_repaired()
        before = self.snapshot()
        seal = self.seal()
        self.assertEqual(len(self.transport.calls), 2)
        self.assertEqual(self.authority.lookup(self.actor, first.request_id), failed)
        self.assertEqual(
            self.authority.lookup(self.actor, second.request_id), completed
        )
        self.assertEqual(
            self.ledger.budget()["spent_or_reserved"],
            1000 + failed.usage_units + completed.usage_units,
        )
        self.assertEqual(
            self.rows("financial_actions_v2"), before["financial_actions_v2"]
        )
        self.assertEqual(self.rows("reservations"), before["reservations"])
        self.assertEqual(
            self.authority.verified_terminal_seal(self.chain.commitment).raw, seal.raw
        )
        checked_financial_config(self.authority.config)
        with self.assertRaises(FinancialError):
            self.authority.verified_terminal(failed.binding)
        self.assertEqual(
            self.authority.verified_known_failure(failed.binding)["reply"], failed
        )
        with self.assertRaises(CohortSealed):
            self.authority.claim(self.actor, self.context, self.works[0])

    def test_old_v4_closure_predicate_rejects_the_same_real_repaired_history(self):
        self.failed_then_repaired()
        with self.authority.ledger.atomic() as db:
            with self.assertRaisesRegex(FinancialError, "failed or unknown"):
                CumulativeAuthorityV4._quiescent(self.authority, db, "completed")
        self.seal()

    def test_recovery_authenticates_known_failure_without_redispatch(self):
        first, failed, _, _ = self.failed_then_repaired()
        seal = self.seal()
        self.open(recovery=True)
        self.assertEqual(self.authority.lookup(self.actor, first.request_id), failed)
        self.assertEqual(
            self.authority.verified_terminal_seal(self.chain.commitment).raw, seal.raw
        )
        self.assertEqual(len(self.transport.calls), 2)

    def test_unknown_usage_still_blocks_completed_closure(self):
        self.open()
        self.transport.omit_usage = True
        action, _ = self.start()
        self.assertEqual(self.terminal(action).state, "unknown")
        with self.assertRaises(FinancialError):
            self.seal()
        self.assertEqual(self.rows("financial_terminal_seals_v4"), [])

    def test_known_halt_with_usage_still_blocks_completed_closure(self):
        self.open()
        self.transport.response_status = 429
        action, _ = self.start()
        reply = self.terminal(action)
        self.assertEqual(reply.state, "failed")
        self.assertIsInstance(reply.usage_units, int)
        with self.assertRaisesRegex(FinancialError, "Halted owner"):
            self.seal()
        self.assertEqual(self.rows("financial_terminal_seals_v4"), [])

    def test_original_failure_journal_missing_blocks_completed_closure(self):
        _, failed, _, _ = self.failed_then_repaired()
        path = self.authority.journal.paths(failed.binding.call_id)["result"]
        path.rename(path.with_suffix(".retained-original"))
        with self.assertRaises((FinancialError, OSError, JournalCorrupt)):
            self.seal()
        self.assertEqual(self.rows("financial_terminal_seals_v4"), [])

    def test_changed_settled_usage_cannot_be_relabelled_known_failure(self):
        _, failed, _, _ = self.failed_then_repaired()
        with sqlite3.connect(self.path) as db:
            db.execute(
                "UPDATE reservations SET spent=spent+1 WHERE id=?",
                (failed.binding.reservation_id,),
            )
        with self.assertRaises(FinancialError):
            self.seal()

    def test_pending_owned_execution_blocks_preparation_until_actual_terminal(self):
        self.open()
        self.transport.release.clear()
        try:
            action, _ = self.start()
            self.assertTrue(self.transport.entered.wait(2))
            with self.assertRaises(SealBusy):
                self.seal()
        finally:
            self.transport.release.set()
        self.assertEqual(self.terminal(action).state, "completed")
        self.seal()

    def test_mutated_policy_cannot_be_repermitted_into_v5(self):
        permit = self.permit(1000)
        permit["execution_design"] = deepcopy(permit["execution_design"])
        permit["execution_design"]["financial_closure_policy"]["unknown"] = "allow"
        permit["execution_design_sha256"] = digest(permit["execution_design"])
        with self.assertRaisesRegex(FinancialError, "Exact versioned"):
            self.open(permit=permit, expected_permit_sha256=digest(permit))
        self.assertEqual(len(self.transport.calls), 0)

    def test_policy_scalar_alias_and_live_qualification_remain_closed(self):
        policy = deepcopy(
            self.permit(1000)["execution_design"]["financial_closure_policy"]
        )
        policy["acceptance_authority"] = 0
        with self.assertRaises(FinancialError):
            checked_policy(policy)
        with self.assertRaisesRegex(FinancialError, "requires an actual pre-lease issued qualification"):
            validate_qualification({}, {}, {})

    def test_pending_git_intent_blocks_repaired_completed_closure(self):
        self.failed_then_repaired()
        with sqlite3.connect(self.path) as db:
            db.execute(
                "INSERT INTO intents VALUES (?,?,?,?,?,?,?)",
                ("pending", "/fixture", "a" * 40, "b" * 40, "[]", "pending", None),
            )
        with self.assertRaisesRegex(FinancialError, "Pending Git"):
            self.seal()

    def test_memory_persistence_fence_blocks_repaired_completed_closure(self):
        self.failed_then_repaired()
        self.authority.persistence_failed.add((self.actor, "unavailable-tail"))
        with self.assertRaisesRegex(FinancialError, "Halted owner"):
            self.seal()


    def test_repermitted_numeric_profile_alias_rejected_before_admission(self):
        permit = deepcopy(self.permit(1000))
        permit['profiles']['mini']['timeout'] = float(permit['profiles']['mini']['timeout'])
        permit['execution_design']['profiles'] = deepcopy(permit['profiles'])
        permit['execution_design_sha256'] = digest(permit['execution_design'])
        with self.assertRaisesRegex(FinancialError, 'V5 permit scope differs'):
            self.open(permit=permit, expected_permit_sha256=digest(permit))
        self.assertEqual(len(self.transport.calls), 0)


    def test_real_failed_call_journal_numeric_aliases_are_not_exact_originals(self):
        from gossip_harness.peer_financial_authority_v2 import CumulativeAuthorityV2
        from gossip_harness import verification_journal as journal
        from gossip_harness.peer_financial_terminal_v1 import sha
        import json
        first, failed, _, _ = self.failed_then_repaired()
        paths = dict(self.authority.journal.paths(failed.binding.call_id))
        paths['owner'] = self.authority.journal.root/(sha(failed.binding.reservation_id.encode())+'.reservation.json')
        originals = {name: paths[name].read_bytes() for name in ('owner','request','result','settled')}
        for name, field, value in (('settled','usage_units',float(failed.usage_units)),
                                    ('owner','schema_version',True),('result','schema_version',True)):
            with self.subTest(name=name):
                changed = json.loads(originals[name])
                changed[field] = value
                paths[name].write_bytes(journal._bytes(changed))
                if name == 'result':
                    settled = json.loads(originals['settled'])
                    settled['result_sha256'] = sha(paths[name].read_bytes())
                    paths['settled'].write_bytes(journal._bytes(settled))
                # Nonvacuous: the frozen verifier accepts the scalar alias.
                self.assertEqual(CumulativeAuthorityV2._verify_terminal(self.authority,self.actor,first.request_id)['reply'],failed)
                with self.assertRaisesRegex(FinancialError,'V5 journal original bytes differ'):
                    self.authority._verify_terminal(self.actor,first.request_id)
                for original_name, raw in originals.items():
                    paths[original_name].write_bytes(raw)
        self.seal()


class PeerFinancialBarrierV5Tests(Fixture, unittest.TestCase):
    def test_six_actual_sequential_v5_seals_authenticate_policy_and_original_wallet(
        self,
    ):
        receipts = []
        for index in range(6):
            self.select(index)
            self.open()
            if index == 0:
                self.transport.proposal = {"summary": "bad", "changes": []}
                first, _ = self.start(0)
                self.assertEqual(self.terminal(first).state, "failed")
                self.transport.proposal = {
                    "summary": "fixed",
                    "changes": [{"path": "src/a.py", "content": "fixed\n"}],
                }
                second, _ = self.start(1)
                self.assertEqual(self.terminal(second).state, "completed")
            receipts.append(self.seal())
        barrier = verified_study_barrier(
            self.roster,
            self.chain,
            self.chain.commitment,
            tuple(receipts),
            existing_ledger_path=self.path,
            expected_ledger_identity=ledger_identity(self.path),
        )
        self.assertEqual(
            barrier["financial_closure_policy_sha256"], CLOSURE_POLICY_SHA256
        )
        self.assertEqual(barrier["financial_protocol"], "peer-financial-authority-v5")
        self.assertEqual(barrier["role_count"], 96)
        self.assertFalse(barrier["acceptance_authority"])
        self.assertEqual(len(self.transport.calls), 2)

    def test_v4_predecessor_cannot_be_adopted_by_v5_child(self):
        from tests.financial_v4_fixture import Fixture as V4Fixture
        from gossip_harness.peer_financial_authority_v4 import (
            PERMIT_PROTOCOL,
            source_fingerprints,
        )

        permit = V4Fixture.permit(self, 1000)
        permit["protocol"] = PERMIT_PROTOCOL
        permit["sources"] = source_fingerprints()
        self.authority = CumulativeAuthorityV4.open(
            existing_ledger_path=self.path,
            service_root=self.root / "legacy",
            cohort_contract=self.contract,
            incremental_cap_micro_usd=800000,
            expected_global_cap=1000000,
            expected_opening_usage=1000,
            payloads=self.payloads,
            workers={"mini": self.worker},
            max_workers=2,
            clock=lambda: 100.0,
            permit=permit,
            expected_permit_sha256=digest(permit),
            mode="fixture",
            terminal_roster=self.roster,
            checkpoint=self.chain,
            expected_checkpoint=self.chain.commitment,
        )
        self.seal()
        self.authority.close()
        self.authority = None
        self.select(1)
        with self.assertRaisesRegex(FinancialError, "Exact V5 financial origin"):
            self.open()

    def test_v5_barrier_rejects_six_valid_v4_structural_seals(self):
        from tests.financial_v4_fixture import Fixture as V4Fixture
        from gossip_harness.peer_financial_authority_v4 import PERMIT_PROTOCOL, source_fingerprints
        receipts = []
        for index in range(6):
            if self.authority is not None:
                self.authority.close()
                self.authority = None
            self.select(index)
            opening = self.ledger.budget()['spent_or_reserved']
            permit = V4Fixture.permit(self, opening)
            permit['protocol'] = PERMIT_PROTOCOL
            permit['sources'] = source_fingerprints()
            self.authority = CumulativeAuthorityV4.open(existing_ledger_path=self.path,
                service_root=self.root/('legacy-'+str(index)), cohort_contract=self.contract,
                incremental_cap_micro_usd=800000, expected_global_cap=1000000, expected_opening_usage=opening,
                payloads=self.payloads, workers={'mini':self.worker}, max_workers=2,
                clock=lambda:100.0, permit=permit, expected_permit_sha256=digest(permit), mode='fixture',
                terminal_roster=self.roster, checkpoint=self.chain, expected_checkpoint=self.chain.commitment)
            receipts.append(self.seal())
        with self.assertRaisesRegex(FinancialError, 'Exact V5 financial origin'):
            verified_study_barrier(self.roster,self.chain,self.chain.commitment,tuple(receipts),
                existing_ledger_path=self.path,expected_ledger_identity=ledger_identity(self.path))
