"""V3 refusal and atomic-publication controls; no semantic review is fabricated.

Method-isolation controls explicitly replace expensive original readers. They
exercise exception ordering and guards, not independent acceptance authority.
"""
from dataclasses import asdict, replace
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

from gossip_harness import candidate_scope_consumer_v1 as consumer
from gossip_harness import cumulative_final_acceptance_v1 as shared
from gossip_harness import cumulative_final_acceptance_v3 as final
from gossip_harness import cumulative_prerequisite_review_v1 as review
from gossip_harness import cumulative_prerequisite_qualification_v1 as qualification
from gossip_harness import cumulative_source_promotion_v1 as promotion
from gossip_harness import cumulative_control_qualification_v1 as control
from gossip_harness import candidate_checkpoint_chain_v1 as checkpoint
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from gossip_harness import project_acceptance_registry_v1 as registry
from gossip_harness.gitstore import GitStore


class CumulativeFinalAcceptanceV3Tests(unittest.TestCase):
    def setUp(self):
        temporary=tempfile.TemporaryDirectory(prefix='gossip-original-v3-');self.addCleanup(temporary.cleanup)
        self.root=Path(temporary.name).resolve()

    def chain(self,name):
        root=self.root/name;root.mkdir()
        head=ExternalHead.create(root/'head',journal_roots=(root/'raw',root/'delta'));self.addCleanup(head.close)
        chain=checkpoint.CheckpointChain.create(root/'raw',root/'delta',context={'explicit_fixture':name},authority=head)
        self.addCleanup(chain.close);return chain

    def isolated_owner(self):
        owner=object.__new__(final.FinalAcceptanceV3)
        owner.chain=self.chain('final');owner.study_chain=self.chain('study')
        owner.scope_owner=SimpleNamespace(chain=self.chain('scope'))
        owner._current_originals=lambda:None
        owner._current=lambda:None
        owner.enrollments={}
        owner.records=SimpleNamespace(read=lambda key:None)
        owner._put=lambda key,value:value
        return owner

    def inert_store(self,path):
        # Only used with the explicit construction stub; no Git evidence claim.
        store=object.__new__(GitStore);store.path=path;return store

    def reviews(self,name):
        chain=self.chain(name)
        return review.OriginalReviewAuthority(chain,chain.commitment)

    def test_binding_lost_ack_never_installs_authorities(self):
        owner=self.isolated_owner();admission=self.reviews('admission');integrated=self.reviews('integration')
        store=self.inert_store(self.root/'harness.git')
        prerequisite=SimpleNamespace(source=consumer.Revision('a'*40,'b'*40,'c'*64))
        owner._put=lambda key,value:(_ for _ in ()).throw(consumer.AuthorityUnavailable('lost acknowledgment'))
        with patch.object(qualification,'PrerequisiteQualification',return_value=prerequisite),patch.object(promotion,'SourcePromotion'):
            with self.assertRaisesRegex(consumer.AuthorityUnavailable,'lost acknowledgment'):
                owner.bind_originals(store=store,admission_reviews=admission,controls=(),promotion_reviews=integrated)
        self.assertIsNone(owner.prerequisite_owner);self.assertIsNone(owner.promotion_owner)
        self.assertEqual(owner.control_owners,());self.assertIsNone(owner.original_authorities_record)

    def test_binding_capabilities_publish_after_durable_record(self):
        owner=self.isolated_owner();admission=self.reviews('admission');integrated=self.reviews('integration')
        store=self.inert_store(self.root/'harness.git')
        prerequisite=SimpleNamespace(source=consumer.Revision('a'*40,'b'*40,'c'*64))
        seen=[]
        def retain(key,value):
            seen.append((owner.prerequisite_owner,owner.promotion_owner,owner.control_owners))
            return value
        owner._put=retain
        with patch.object(qualification,'PrerequisiteQualification',return_value=prerequisite),patch.object(promotion,'SourcePromotion') as make:
            owner.bind_originals(store=store,admission_reviews=admission,controls=(),promotion_reviews=integrated)
            self.assertIs(owner.promotion_owner,make.return_value)
        self.assertEqual(seen,[(None,None,())]);self.assertIs(owner.prerequisite_owner,prerequisite)

    def test_bind_rejects_harness_path_inside_original_review_before_construction(self):
        owner=self.isolated_owner();admission=self.reviews('admission');integrated=self.reviews('integration')
        store=self.inert_store(admission.chain.raw_root/'candidate.git')
        with patch.object(qualification,'PrerequisiteQualification') as construct:
            with self.assertRaisesRegex(consumer.AuthorityError,'disjoint'):
                owner.bind_originals(store=store,admission_reviews=admission,controls=(),promotion_reviews=integrated)
            construct.assert_not_called()

    def test_v3_protected_roots_include_every_bound_original_authority(self):
        owner=self.isolated_owner();admission=self.reviews('admission');integrated=self.reviews('integration')
        control_chain=self.chain('control');control_reviews=self.reviews('control-review')
        owner.prerequisite_owner=SimpleNamespace(store=self.inert_store(self.root/'harness.git'),reviews=admission)
        owner.promotion_owner=SimpleNamespace(reviews=integrated)
        owner.control_owners=((SimpleNamespace(root=self.root/'control-fixture',chain=control_chain),control_reviews),)
        roots=owner._protected_roots()
        for chain in (owner.chain,owner.study_chain,owner.scope_owner.chain,admission.chain,integrated.chain,control_chain,control_reviews.chain):
            self.assertTrue(all(path in roots for path in (chain.raw_root,chain.delta_root,chain.authority.root)))
        self.assertIn(self.root/'harness.git',roots);self.assertIn(self.root/'control-fixture',roots)

    def test_v3_routing_is_closed_and_old_v2_default_is_unchanged(self):
        from gossip_harness.cumulative_final_acceptance_v2 import FinalAcceptanceV2,PROTOCOL
        self.assertEqual(shared._known_contract(object.__new__(FinalAcceptanceV2)).protocol,PROTOCOL)
        self.assertEqual(shared._known_contract(object.__new__(final.FinalAcceptanceV3)).protocol,final.PROTOCOL)
        class Unrecognized(final.FinalAcceptanceV3):pass
        with self.assertRaisesRegex(consumer.AuthorityError,'Unknown final acceptance owner'):
            shared._known_contract(object.__new__(Unrecognized))

    def test_control_definition_requires_actual_harness_purpose(self):
        revision=consumer.Revision('a'*40,'b'*40,'c'*64)
        from tests.test_candidate_scope_consumer_v1 import recipe
        original_recipe=recipe()
        definition=control.ControlDefinition(original_recipe,original_recipe.base,'d'*64,'harness_qualification','e'*64,
            {'explicit_synthetic_definition':True},'f'*64,'0'*64,'1'*64)
        suite,gate,spec=final.control_records(definition,gate_id='control',suite_id='control-suite',physical_slot='control-slot')
        self.assertEqual(gate.logical_gate_ids,('M1-GATE-CONTROL',))
        self.assertEqual(suite.capabilities,('harness-control',))
        self.assertEqual(spec.control_recipe,definition.recipe)
        with self.assertRaises(consumer.AuthorityError):
            final.control_records(replace(definition,purpose='independent_acceptance'),gate_id='control',suite_id='control-suite',physical_slot='control-slot')

    def test_control_failed_reuse_is_refused_without_erasing_raw_diagnostic(self):
        owner=object.__new__(final.FinalAcceptanceV3);owner._request=lambda request:None
        request=SimpleNamespace(gate=SimpleNamespace(logical_gate_ids=('M1-GATE-CONTROL',)))
        evidence=consumer.QualificationEvidence('a'*64,'b'*64,'control-original','c'*64,'d'*64,
            'harness_qualification','completed',(registry.CaseResult('probe','failed'),),
            (registry.CaseResult('semantic_conflict','failed'),),'reused','e'*64)
        owner._control_join=lambda actual:(evidence,{'original_failed':True})
        with self.assertRaisesRegex(consumer.AuthorityUnavailable,'cannot authorize reuse'):
            owner.qualification(request)
        self.assertEqual(owner._control_join(request)[0].outcomes[0].status,'failed')

    def test_successful_control_reuse_remains_explicitly_reused(self):
        owner=object.__new__(final.FinalAcceptanceV3);owner._request=lambda request:None
        request=SimpleNamespace(gate=SimpleNamespace(logical_gate_ids=('M1-GATE-CONTROL',)))
        evidence=consumer.QualificationEvidence('a'*64,'b'*64,'control-original','c'*64,'d'*64,
            'harness_qualification','completed',(registry.CaseResult('probe','passed'),),
            (registry.CaseResult('semantic_conflict','passed'),),'reused','e'*64)
        owner._control_join=lambda actual:(evidence,{})
        result=owner.qualification(request)
        self.assertEqual(result.mode,'reused');self.assertEqual(result.receipt_sha256,'c'*64)
        self.assertEqual(result.reuse_receipt_sha256,'e'*64)

    def control_join_fixture(self):
        # Typed fixture identities and a method-isolated trusted-owner boundary;
        # this is not semantic registration or a real CONTROL execution.
        from tests.test_candidate_scope_consumer_v1 import recipe
        revision=consumer.Revision('a'*40,'b'*40,'c'*64)
        original_recipe=recipe()
        definition=control.ControlDefinition(original_recipe,original_recipe.base,'d'*64,'harness_qualification','e'*64,
            {'explicit_synthetic_definition':True},'f'*64,'0'*64,'1'*64)
        suite,gate,spec=final.control_records(definition,gate_id='control',suite_id='control-suite',physical_slot='control-slot')
        registered=consumer.RegisteredAcceptance(*(['a'*64]*5),(spec,))
        request=consumer.QualificationRequest(registered,gate,suite,spec,'d'*64)
        concrete=object.__new__(control.ControlQualification)
        concrete.config={'product_lineages_sha256':'d'*64}
        concrete.definition=lambda:definition
        observed=[]
        concrete.verify_current=lambda reviewer:observed.append('original-read') or definition
        owner=object.__new__(final.FinalAcceptanceV3);owner._request=lambda item:None
        owner.control_owners=((concrete,None),)
        return owner,request,observed

    def test_control_join_rejects_changed_registered_target_before_original_read(self):
        owner,request,observed=self.control_join_fixture()
        with self.assertRaisesRegex(consumer.AuthorityUnavailable,'lineage unavailable'):
            owner._control_original(replace(request,product_lineages_sha256='f'*64))
        self.assertEqual(observed,[])

    def test_control_join_rejects_each_changed_execution_contract_field(self):
        owner,request,observed=self.control_join_fixture()
        for field in ('runtime_image_sha256','environment_sha256','limits_sha256','seed_sha256'):
            with self.subTest(field=field),self.assertRaisesRegex(consumer.AuthorityError,'contract differs'):
                owner._control_original(replace(request,gate=replace(request.gate,**{field:'2'*64})))
        with self.assertRaisesRegex(consumer.AuthorityError,'contract differs'):
            owner._control_original(replace(request,gate=replace(request.gate,execution_protocol='foreign-control-protocol')))
        self.assertEqual(observed,[])

    def test_control_join_rejects_source_ordered_suite_and_purpose_substitution(self):
        owner,request,observed=self.control_join_fixture()
        changes=(replace(request,specification=replace(request.specification,qualification_source_sha256='2'*64)),
            replace(request,suite=replace(request.suite,ordered_case_ids=request.suite.ordered_case_ids[::-1])),
            replace(request,suite=replace(request.suite,evaluator_sha256='2'*64)),
            replace(request,suite=replace(request.suite,execution_purpose='independent_acceptance')))
        for changed in changes:
            with self.assertRaisesRegex(consumer.AuthorityError,'contract differs'):owner._control_original(changed)
        self.assertEqual(observed,[])


    def test_batch_source_capture_unavailable_is_infrastructure_not_unhandled_or_product_failure(self):
        from gossip_harness.candidate_source_capture_policy_v1 import SourceCaptureUnavailable
        for error_type in (SourceCaptureUnavailable, shared.two_process_capture.SourceCaptureUnavailable):
            @shared.normalize_authority
            def unavailable():
                raise error_type('owned source transport did not complete')
            with self.subTest(error_type=error_type.__module__), self.assertRaisesRegex(
                    consumer.AuthorityUnavailable, 'source transport'):
                unavailable()

    def test_qualification_batch_capture_unavailable_does_not_become_source_revision(self):
        store=self.inert_store(self.root/'harness.git');store.head=lambda:'a'*40
        with patch.object(qualification.capture,'capture_git_source_batch',side_effect=qualification.capture.CaptureError('deadline')):
            with self.assertRaisesRegex(consumer.AuthorityUnavailable,'deadline'):
                qualification.harness_revision(store)
