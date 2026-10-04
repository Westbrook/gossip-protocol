"""Closed qualification guards and real SQL post-audit entry controls.

The audit-duration hook is intentionally isolated; it is not a completed
rehearsal, semantic review, or paid provider execution. OfflineTransport only.
"""
from unittest.mock import patch
import unittest

from gossip_harness import peer_financial_qualification_v5_final_v3 as qualification
from gossip_harness import peer_financial_authority_v5 as financial
from gossip_harness.peer_financial_authority_v4 import CumulativeAuthorityV4
from gossip_harness.peer_financial_terminal_v1 import FinancialError
from tests.financial_v5_fixture import Fixture
from tests.test_peer_financial_authority_v2 import Clock


class PeerFinancialQualificationV5FinalV3Tests(unittest.TestCase):
    def test_historical_or_asserted_pass_is_not_a_qualification_reference(self):
        for reference in ({},{'accepted':True},{'protocol':'peer-financial-qualification-v5','capsule':{}}):
            with self.subTest(reference=reference),self.assertRaisesRegex(FinancialError,'unavailable'):
                qualification.validate_qualification(reference,{}, {})

    def test_policy_must_be_prospectively_pinned_before_capsule_read(self):
        reference={'protocol':qualification.PROTOCOL,'capsule':{},'approved_child':{}}
        with patch.object(qualification.capsule,'audit_closed_capsule') as audit:
            with self.assertRaisesRegex(FinancialError,'prospectively'):
                qualification.validate_qualification(reference,{'runtime':{}},{})
            audit.assert_not_called()

    def test_missing_actual_reader_sources_cannot_authorize_capsule(self):
        reference={'protocol':qualification.PROTOCOL,'capsule':{},'approved_child':{}}
        with patch.object(qualification,'implementation_sources',return_value={'required.py':'a'*64}),\
                patch.object(qualification.capsule,'audit_closed_capsule') as audit:
            with self.assertRaisesRegex(FinancialError,'source contract'):
                qualification.validate_qualification(reference,{'runtime':{'live_qualification_protocol':qualification.PROTOCOL}},{})
            audit.assert_not_called()


class PeerFinancialQualifiedEntryV5Tests(Fixture,unittest.TestCase):
    def setUp(self):
        super().setUp();self.clock=Clock();self.open(clock=self.clock)

    def invoke_with_audit_hook(self,hook):
        # V4's qualification call is replaced only at this exact slow boundary;
        # original admission/SQL/journal/lease and V5 recheck remain actual code.
        with patch.object(CumulativeAuthorityV4,'_before_invoke',autospec=True,side_effect=hook):
            action,lease=self.start()
            result=self.terminal(action)
        return action,lease,result

    def test_fresh_original_request_can_enter_offline_provider_once(self):
        _,_,result=self.invoke_with_audit_hook(lambda *args:None)
        self.assertEqual(result.state,'completed')
        self.assertEqual(len(self.transport.calls),1)

    def test_lease_expiring_during_qualification_never_enters_provider(self):
        def expired(*args):self.clock.now=1000.0
        _,lease,result=self.invoke_with_audit_hook(expired)
        self.assertLess(lease.expires_at,self.clock.now)
        self.assertEqual(len(self.transport.calls),0)
        self.assertEqual(result.state,'unknown')
        self.assertTrue(self.authority.failed_closed)

    def test_request_mutation_during_qualification_never_enters_provider(self):
        def changed(owner,worker,request):request.files['src/a.py']='substituted\n'
        _,_,result=self.invoke_with_audit_hook(changed)
        self.assertEqual(len(self.transport.calls),0)
        self.assertEqual(result.state,'unknown')

    def test_settled_reservation_during_qualification_never_enters_provider(self):
        def settled(owner,worker,request):
            actor,request_id=next(iter(owner.active_executions))
            _,binding,_,_=owner._action(actor,request_id)
            owner.ledger.settle(binding.reservation_id,0)
        _,_,result=self.invoke_with_audit_hook(settled)
        self.assertEqual(len(self.transport.calls),0)
        self.assertEqual(result.state,'unknown')

    def test_no_active_original_cannot_call_provider_entry_directly(self):
        from gossip_harness.worker import WorkerRequest
        request=WorkerRequest('task','instructions',('src/a.py',),{'src/a.py':'pass'},'c'*40,1,'')
        with self.assertRaisesRegex(FinancialError,'exact original active'):
            financial.CumulativeAuthorityV5._recheck_qualified_entry(self.authority,self.worker,request)
        self.assertEqual(self.transport.calls,[])
