"""Real prospective factory/admission/owner path with synthetic reviewer originals.

No candidate or Docker execution; fixture owners cannot publish observations.
"""
from dataclasses import replace
from pathlib import Path
import os
import sys
import unittest

from gossip_harness import cumulative_m2_observation_recipe_v1 as factory
from gossip_harness import cumulative_scope_source_v3 as source
from gossip_harness import candidate_m2_product_execution_v1 as execution
from gossip_harness import candidate_m2_product_observation_v1 as observer
from gossip_harness import candidate_m2_product_profile_v1 as profile
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import candidate_scope_consumer_v1 as consumer
from tests import test_candidate_m2_product_execution_v1 as fixtures


class CumulativeM2ObservationRecipeV1Tests(unittest.TestCase):
    setUpClass=classmethod(fixtures.CandidateM2ProductExecutionV1Tests.setUpClass.__func__)
    setUp=fixtures.CandidateM2ProductExecutionV1Tests.setUp
    make=fixtures.CandidateM2ProductExecutionV1Tests.make

    def recipe(self):
        return factory.build_m2_recipe(self.store,self.subject,case_id=self.value.case_id,purpose=self.value.purpose,
            policy=self.policy,runtime={'kind':'fixture-no-Docker'},layout_plan=self.plan,layout_authority=self.review,
            gate_id=self.gate.gate_id,repetition_id=self.registration.repetition_id,cohort_trajectory_ids=fixtures.COHORT)

    def spec(self,value):
        return value.spec(root=self.raw,delta_root=self.delta,cleanup_root=self.root/'cleanup',checkpoint_authority=self.head)

    def test_actual_recipe_to_closed_scope_to_real_fixture_owner_is_positive(self):
        recipe=self.recipe();recipe.revalidate()
        self.assertEqual(recipe.registration,self.registration)
        component=recipe.scope_slice();source.verify_slice(component)
        self.assertIs(type(component),source.ExecutableSlice)
        self.assertTrue(component.assertions)
        self.assertEqual(recipe.record()['scope_factory_protocol'],source.PROTOCOL)
        owner=factory.construct_m2_owner(self.spec(recipe),self.admission,mode='fixture');self.addCleanup(owner.close)
        self.assertIs(type(owner),execution.CandidateM2Execution)
        self.assertEqual(owner.observation_registration,execution.observation_registration(recipe.registration))
        self.assertEqual(owner.config['source_capture_policy']['timeout_seconds'],60)
        with self.assertRaises(execution.ExecutionError):owner.execute_once()
        with self.assertRaises(observer.AuthorityError):observer.M2ObservationSource(owner,owner.checkpoint())

    def test_actual_surrogate_history_flows_through_original_review_recipe_and_owner(self):
        self.root=self.root/'surrogate';self.root.mkdir()
        self.make('m2-refresh-input-boundaries')
        recipe=self.recipe()
        self.assertIn(b'\\ud800',recipe.original_bytes)
        self.assertEqual(recipe.record()['execution_recipe'],profile.recipe_for(self.value.case_id))
        owner=factory.construct_m2_owner(self.spec(recipe),self.admission,mode='fixture');self.addCleanup(owner.close)
        self.assertEqual(owner.profile,self.value)
        self.assertEqual(len(owner.profile.phases),18)
        self.assertFalse(owner.has_retained('intent.json'))

    def test_fresh_batch_capture_is_actual_route_at_build_revalidation_and_construction(self):
        seen=[]
        batch_code=execution.source_capture.batch.capture_git_source_batch.__code__
        legacy_code=execution.source_capture.legacy_capture.__code__
        def trace(frame,event,arg):
            if event=='call' and frame.f_code in (batch_code,legacy_code):seen.append(frame.f_code)
        prior=sys.getprofile();sys.setprofile(trace)
        try:
            recipe=self.recipe();recipe.revalidate()
            owner=factory.construct_m2_owner(self.spec(recipe),self.admission,mode='fixture')
            self.addCleanup(owner.close);owner.current(None)
        finally:sys.setprofile(prior)
        self.assertGreaterEqual(seen.count(batch_code),6)
        self.assertNotIn(legacy_code,seen)

    def test_recipe_pins_transitive_scope_and_shared_spec_implementation(self):
        record=self.recipe().record()
        for name in ('cumulative_scope_source_v1.py','cumulative_scope_source_v2.py','cumulative_scope_source_v3.py',
            'project_acceptance_compiler_v1.py','candidate_scope_consumer_v1.py','cumulative_final_acceptance_v1.py',
            'cumulative_m2_observation_recipe_v1.py','candidate_source_capture_policy_v1.py','candidate_git_source_batch_v1.py'):
            self.assertIn('gossip_harness/'+name,record['sources'])
        self.assertFalse(record['whole_scope_authority'])
        self.assertFalse(record['physical_execution_supplied'])

    def test_mutated_packet_runtime_or_registration_is_rejected_before_spec(self):
        recipe=self.recipe()
        for changed in (replace(recipe,original_bytes=b'{}'),replace(recipe,runtime_bytes=b'{}'),
                replace(recipe,registration=replace(recipe.registration,tree_oid='f'*40))):
            with self.subTest(changed=changed.sha256),self.assertRaises(ValueError):changed.revalidate()

    def test_changed_original_review_revokes_recipe_not_just_owner(self):
        recipe=self.recipe();self.review_journal.retain('later.json',b'{}')
        with self.assertRaises(admission.AdmissionError):recipe.revalidate()
        self.assertFalse(self.raw.exists())

    def test_missing_exact_full_freeze_blocks_independent_construction_before_journal(self):
        self.root=self.root/'independent';self.root.mkdir();self.make(purpose='independent_acceptance')
        recipe=self.recipe()
        with self.assertRaises(admission.AdmissionUnavailable):factory.construct_m2_owner(self.spec(recipe),self.admission,mode='fixture')
        self.assertFalse(self.raw.exists())

    def test_foreign_auxiliary_spec_and_changed_admission_cannot_construct(self):
        recipe=self.recipe();spec=self.spec(recipe)
        with self.assertRaises(consumer.AuthorityError):factory.construct_m2_owner(replace(spec,recipe={}),self.admission,mode='fixture')
        with self.assertRaises(consumer.AuthorityError):factory.construct_m2_owner(replace(spec,kind='storage'),self.admission,mode='fixture')
        self.current['registration']=None
        with self.assertRaises(admission.AdmissionError):factory.construct_m2_owner(spec,self.admission,mode='fixture')
        self.assertFalse(self.raw.exists())

    def test_relative_cleanup_cannot_escape_protected_review_root_check(self):
        recipe=self.recipe();spec=self.spec(recipe)
        original=Path.cwd()
        os.chdir(self.root)
        try:
            changed=replace(spec,cleanup_root=Path('review-raw')/'nested-cleanup')
            with self.assertRaisesRegex(consumer.AuthorityError,'Canonical absolute recipe roots'):
                owner=factory.construct_m2_owner(changed,self.admission,mode='fixture')
                self.addCleanup(owner.close)
        finally:os.chdir(original)
        self.assertFalse(self.raw.exists())
        self.assertFalse((self.review_journal.raw_root/'nested-cleanup').exists())
