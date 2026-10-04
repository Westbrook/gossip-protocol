"""Exact proof owner lifetime controls; audit body is deliberately stubbed."""
from dataclasses import asdict
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from gossip_harness.candidate_checkpoint_chain_v1 import CheckpointChain
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from gossip_harness.cumulative_rehearsal_capsule_v1 import audit_closed_capsule, decode_plan, PROTOCOL
from gossip_harness.cumulative_rehearsal_validator_v1 import RehearsalAudit
from gossip_harness.peer_financial_authority_v2 import canonical_payload
from gossip_harness.peer_financial_terminal_v1 import digest, sha
from tests.financial_rehearsal_fixture_v1 import synthetic_plan, synthetic_design_envelope


class CumulativeRehearsalCapsuleV1Tests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.root=Path(self.temp.name).resolve()
        self.journals=(self.root/'raw',self.root/'delta')
        self.context={'fixture':'closed-proof-owner'}
        self.head=ExternalHead.create(self.root/'anchor',journal_roots=self.journals)
        self.chain=CheckpointChain.create(*self.journals,context=self.context,authority=self.head)
        self.chain.retain('original.json',b'original exact bytes')
        self.expected=self.chain.commitment
        self.limits=self.chain.limits
        self.plan=synthetic_plan()
        self.envelope=synthetic_design_envelope(self.plan)
        self.design=self.envelope['children'][0]['execution_design']
        child=self.plan.roster.children[0]
        self.approved_child={'cohort':child.cohort,'trajectory':child.trajectory}
        raw=canonical_payload(self.envelope)
        path=self.root/'design-envelope.json'
        path.write_bytes(raw)
        self.envelope_ref={'path':str(path),'sha256':sha(raw)}
        self.addCleanup(self.cleanup)

    def cleanup(self):
        if self.chain is not None:
            self.chain.close()
        if self.head is not None:
            self.head.close()
        self.temp.cleanup()

    def close_owners(self):
        self.chain.close()
        self.head.close()
        self.chain=self.head=None

    def capsule(self, change=None):
        value={'protocol':PROTOCOL,'execution_design_sha256':digest(self.design),'sources':self.plan.source_pins,
            'gate':{'unit_stub':True},'ordered_test_classes':['unit-stub'],
            'design_envelope_sha256':self.envelope_ref['sha256'],'approved_child':self.approved_child,
            'proof':{'repository':str(Path(__file__).resolve().parents[1]),'plan':self.plan.record(),
                'ledger_identity':{'path':str(self.root/'unused.sqlite'),'device':0,'inode':0},
                'raw_root':str(self.journals[0]),'delta_root':str(self.journals[1]),'head_root':str(self.root/'anchor'),
                'context':self.context,'limits':asdict(self.limits),'expected':asdict(self.expected)}}
        if change:
            change(value)
        path=self.root/('capsule-'+str(len(list(self.root.glob('capsule-*'))))+'.json')
        raw=canonical_payload(value)
        path.write_bytes(raw)
        return {'path':str(path),'sha256':sha(raw)}

    def audit(self, ref, stub):
        with patch('gossip_harness.cumulative_rehearsal_capsule_v1.audit_original_rehearsal',side_effect=stub):
            return audit_closed_capsule(ref,execution_design=self.design,sources=self.plan.source_pins,
                design_envelope=self.envelope_ref,approved_child=self.approved_child)

    def test_strict_plan_roundtrip_preserves_full_actual_contract(self):
        self.assertEqual(decode_plan(self.plan.record()).record(),self.plan.record())

    def test_unknown_or_reordered_prospective_fields_are_rejected(self):
        changed=self.plan.record()
        changed['cohort']['trajectories'].pop()
        with self.assertRaises(ValueError):
            decode_plan(changed)
        changed=self.plan.record()
        changed['unexpected']=True
        with self.assertRaises(ValueError):
            decode_plan(changed)

    def test_closed_original_owner_reopens_on_caller_thread_and_closes_after_audit(self):
        ref=self.capsule()
        self.close_owners()
        def body(chain, expected, **kwargs):
            self.assertEqual(expected,self.expected)
            self.assertEqual(chain.read('original.json'),b'original exact bytes')
            return RehearsalAudit('fixture',self.plan.sha256,0,0,0,0,0,0,asdict(expected),('stubbed audit body',))
        result=self.audit(ref,body)
        self.assertFalse(result.live_qualification)
        # A second independent owner must be possible only after finally closed.
        head=ExternalHead.reopen(self.root/'anchor',journal_roots=self.journals,expected=self.expected)
        head.close()

    def test_active_owner_cannot_be_borrowed_by_qualification(self):
        with self.assertRaises(BlockingIOError):
            self.audit(self.capsule(),lambda *args,**kwargs:self.fail('active owner entered audit'))

    def test_stale_independent_prefix_rejects_new_original_suffix(self):
        ref=self.capsule()
        self.chain.retain('later.json',b'new')
        self.close_owners()
        with self.assertRaises(ValueError):
            self.audit(ref,lambda *args,**kwargs:self.fail('stale prefix entered audit'))

    def test_exception_closes_proof_lifetime_without_changing_original_bytes(self):
        ref=self.capsule()
        self.close_owners()
        with self.assertRaisesRegex(RuntimeError,'retained failure'):
            self.audit(ref,lambda *args,**kwargs:(_ for _ in ()).throw(RuntimeError('retained failure')))
        self.assertEqual((self.journals[0]/'original.json').read_bytes(),b'original exact bytes')
        head=ExternalHead.reopen(self.root/'anchor',journal_roots=self.journals,expected=self.expected)
        head.close()

    def test_capsule_selfhash_is_not_a_substitute_for_pinned_external_hash(self):
        ref=self.capsule()
        Path(ref['path']).write_bytes(b'{}')
        with self.assertRaises(ValueError):
            self.audit(ref,lambda *args,**kwargs:self.fail('changed capsule entered audit'))

    def test_rebound_context_does_not_reopen_original_chain(self):
        ref=self.capsule(lambda value:value['proof'].update(context={'foreign':True}))
        self.close_owners()
        with self.assertRaises(ValueError):
            self.audit(ref,lambda *args,**kwargs:self.fail('foreign context entered audit'))

    def test_self_described_six_child_designs_do_not_replace_external_approval_pin(self):
        ref=self.capsule()
        changed=self.envelope.copy()
        changed['children']=[dict(item) for item in self.envelope['children']]
        changed['children'][1]['execution_design']={**changed['children'][1]['execution_design'],'max_workers':3}
        Path(self.envelope_ref['path']).write_bytes(canonical_payload(changed))
        with self.assertRaises(ValueError):
            self.audit(ref,lambda *args,**kwargs:self.fail('unapproved envelope entered'))

    def test_approval_for_another_child_cannot_match_by_shared_roster(self):
        ref=self.capsule()
        self.approved_child={'cohort':self.plan.roster.children[1].cohort,'trajectory':self.plan.roster.children[1].trajectory}
        with self.assertRaises(ValueError):
            self.audit(ref,lambda *args,**kwargs:self.fail('wrong child entered'))

    def test_numeric_alias_caller_design_cannot_match_pinned_integer_design(self):
        from copy import deepcopy
        self.design=deepcopy(self.design)
        self.design['max_workers']=4.0
        with self.assertRaisesRegex(ValueError,'corresponding independently approved entry'):
            self.audit(self.capsule(),lambda *args,**kwargs:self.fail('numeric alias entered'))

    def test_numeric_alias_nested_caller_limit_cannot_match_pinned_design(self):
        from copy import deepcopy
        self.design=deepcopy(self.design)
        self.design['action_limits']['total']=512.0
        with self.assertRaisesRegex(ValueError,'corresponding independently approved entry'):
            self.audit(self.capsule(),lambda *args,**kwargs:self.fail('nested numeric alias entered'))

    def test_complete_expected_envelope_rejects_reordering_omission_and_float_workers(self):
        from copy import deepcopy
        from gossip_harness.cumulative_rehearsal_validator_v1 import checked_design_envelope
        for change in (lambda value:value['children'].reverse(),lambda value:value['children'].pop(),
                       lambda value:value['children'][0]['execution_design'].update(max_workers=4.0)):
            value=deepcopy(self.envelope)
            change(value)
            with self.assertRaises(ValueError):
                checked_design_envelope(value,self.plan)
