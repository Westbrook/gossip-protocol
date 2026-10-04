"""Actual Git/review/recipe/fixture-owner composition; no candidate dispatch.

Review originals are openly synthetic linkage fixtures. No complete semantic
ScopePlan or physical observation is manufactured to obtain a positive control.
"""
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import unittest

from gossip_harness import cumulative_observation_recipe_factory_v1 as factory
from gossip_harness import candidate_storage_product_execution_v1 as storage
from gossip_harness import candidate_storage_product_observation_v1 as observation
from gossip_harness import candidate_storage_review_authority_v1 as review
from gossip_harness import candidate_storage_product_profile_v1 as profile
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import candidate_scope_consumer_v1 as consumer
from gossip_harness import project_acceptance_registry_v1 as registry
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from gossip_harness.gitstore import GitStore
from tests.test_candidate_storage_product_execution_v1 import enroll,COHORT
from tests.test_candidate_storage_observer_v1 import physical_files,SQLITE_LAYOUT


class CumulativeObservationRecipeFactoryV1Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp=tempfile.TemporaryDirectory(prefix='gossip-storage-recipe-')
        cls.addClassCleanup(cls.temp.cleanup)
        cls.base=Path(cls.temp.name).resolve()
        cls.store=GitStore.create(cls.base/'source.git',{
            'library/__init__.py':"raise RuntimeError('candidate must never import on host')\n"})
        cls.commit=cls.store.head()
        cls.tree,cls.files=storage.capture_git_source(cls.store,cls.commit)

    def setUp(self):
        self.root=self.base/self.id().rsplit('.',1)[-1]
        self.root.mkdir()

    def recipe(self, *, family='b01',case_id='rollback',purpose='public_release'):
        value=profile.profile_for(family,case_id,purpose)
        case=next(row for row in profile.b01.definitions() if row['case_id']=='rollback')
        schema=observation.b01_observer.sqlite_schema_sha256(physical_files(SQLITE_LAYOUT,case['before'])['catalog.sqlite'])
        plan=review.LayoutPlan(family,admission.source_sha256(self.files),
            (storage.b01 if family=='b01' else storage.b02).source_sha256(self.files),self.commit,self.tree,
            case_id,value.sha256,SQLITE_LAYOUT,('catalog.sqlite',),schema,
            'forced_schedule_unavailable' if family=='b02' and case_id in storage.b02.FORCED_CASE_IDS else 'ordinary_public_operations',purpose)
        authority,journal,head=enroll(self.root,plan)
        self.addCleanup(head.close);self.addCleanup(journal.close)
        subject=registry.Subject('fixture-cohort',COHORT[0],'M4','a'*64,storage.TARGET_CONTRACT,plan.source_sha256)
        result=factory.build_storage_recipe(self.store,subject,family=family,case_id=case_id,purpose=purpose,
            policy=storage.StoragePolicy(),runtime={'kind':'fixture-no-Docker'},layout_plan=plan,
            layout_authority=authority,gate_id='storage-'+family,repetition_id='fresh-fixture',cohort_trajectory_ids=COHORT)
        return result,authority,journal

    def owner(self,recipe, *, changed_registration=False):
        raw,delta=self.root/'raw',self.root/'delta'
        head=ExternalHead.create(self.root/'head',journal_roots=(raw,delta))
        self.addCleanup(head.close)
        spec=recipe.spec(root=raw,delta_root=delta,cleanup_root=self.root/'cleanup',checkpoint_authority=head)
        registered=spec.observation_registration()
        actual=replace(registered,repetition_id='substituted') if changed_registration else registered
        issued=admission.ObservationAdmission(registered,verify_registration=lambda:actual)
        owner=factory.construct_storage_owner(spec,issued,mode='fixture')
        self.addCleanup(owner.close)
        return owner,spec,issued

    def test_actual_b01_recipe_and_current_admission_construct_real_fixture_owner(self):
        recipe,authority,_=self.recipe()
        packet=recipe.record()
        self.assertEqual(packet['profile']['original_definition_purpose'],'harness_qualification')
        self.assertEqual(packet['ordered_phases'],['before','after','reopened'])
        self.assertEqual(packet['layout_review'],authority.provenance(recipe.layout_plan))
        self.assertEqual(tuple(packet['registration']['gate']['ordered_case_ids']),recipe.registration.gate.ordered_case_ids)
        self.assertFalse(packet['whole_scope_authority'])
        declaration=recipe.scope_slice()
        self.assertEqual(declaration.family,'storage-b01')
        self.assertEqual(declaration.gate,recipe.registration.gate)
        self.assertEqual(declaration.assertions,())
        self.assertEqual(packet['scope_factory_protocol'],'cumulative-scope-source-v2')
        self.assertFalse(packet['physical_execution_supplied'])
        owner,spec,issued=self.owner(recipe)
        self.assertIs(type(owner),storage.CandidateStorageExecution)
        self.assertIs(owner.admission,issued)
        self.assertEqual(owner.observation_registration,spec.observation_registration())
        self.assertEqual(json.loads(owner.read_authenticated('config.json'))['registration'],packet['registration'])
        self.assertEqual(owner.checkpoint().sequence,1)
        self.assertFalse(owner.has_retained('intent.json'))
        with self.assertRaises(storage.ExecutionError):owner.execute_once()
        with self.assertRaises(consumer.AuthorityError):observation.publish_verifier(owner)
        self.assertFalse(owner.has_retained('intent.json'))

    def test_actual_b02_recipe_keeps_full_phases_fixtures_and_unfinished_scope(self):
        case_id=next(name for name in storage.b02.cases.CASE_IDS if name not in storage.b02.FORCED_CASE_IDS)
        recipe,_,_=self.recipe(family='b02',case_id=case_id)
        packet=recipe.record()
        self.assertEqual(packet['execution_recipe'],storage.b02.validate_recipe(storage.b02.cases.execution_recipe(case_id)))
        self.assertTrue(packet['profile']['remaining_coverage'])
        declaration=recipe.scope_slice()
        self.assertEqual(declaration.family,'storage-b02')
        self.assertEqual(declaration.gate,recipe.registration.gate)
        self.assertEqual(len(declaration.selectors),len(recipe.profile.diagnostic_ids)+1)
        self.assertEqual({row['path'] for row in packet['adapter_manifest']},
                         {'intake_store_adapter.py','recipe.json','applicability.json'})
        owner,_,_=self.owner(recipe)
        self.assertEqual(owner.binding.family,'b02')
        self.assertEqual(owner.profile,recipe.profile)
        self.assertFalse(owner.has_retained('intent.json'))

    def test_revoked_original_review_prevents_new_spec(self):
        recipe,_,journal=self.recipe()
        journal.retain('extra.json',b'{}')
        with self.assertRaises(admission.AdmissionError):recipe.revalidate()
        self.assertFalse((self.root/'raw').exists())

    def test_changed_current_admission_prevents_owner_creation(self):
        recipe,_,_=self.recipe()
        with self.assertRaises(admission.AdmissionError):self.owner(recipe,changed_registration=True)
        self.assertFalse((self.root/'raw').exists())

    def test_original_recipe_cannot_relabel_purpose(self):
        recipe,_,_=self.recipe()
        changed=replace(recipe,profile=profile.profile_for('b01','rollback','independent_acceptance'))
        with self.assertRaises((storage.ExecutionError,admission.AdmissionError)):changed.revalidate()

    def test_runtime_substitution_is_caught_by_real_owner_recomputation(self):
        recipe,_,_=self.recipe()
        altered=factory.build_storage_recipe(self.store,recipe.registration.gate.binding.subject,family='b01',
            case_id='rollback',purpose='public_release',policy=recipe.policy,runtime={'kind':'another-runtime'},
            layout_plan=recipe.layout_plan,layout_authority=recipe.layout_authority,gate_id='storage-b01',
            repetition_id='fresh-fixture',cohort_trajectory_ids=COHORT)
        with self.assertRaisesRegex(storage.ExecutionError,'binding differs'):self.owner(altered)

    def test_incomplete_trajectory_roster_cannot_build_recipe(self):
        recipe,_,_=self.recipe()
        with self.assertRaisesRegex(consumer.AuthorityError,'six distinct'):
            factory.build_storage_recipe(self.store,recipe.registration.gate.binding.subject,family='b01',
                case_id='rollback',purpose='public_release',policy=recipe.policy,runtime={'kind':'fixture-no-Docker'},
                layout_plan=recipe.layout_plan,layout_authority=recipe.layout_authority,gate_id='storage-b01',
                repetition_id='fresh-fixture',cohort_trajectory_ids=COHORT[:-1])

    def test_recipe_record_edit_is_not_accepted_as_current_recipe(self):
        recipe,_,_=self.recipe()
        changed=recipe.record();changed['physical_execution_supplied']=True
        with self.assertRaisesRegex(consumer.AuthorityError,'recipe changed'):
            replace(recipe,original_bytes=admission.encoded(changed)).revalidate()

    def test_independent_recipe_without_six_source_freeze_cannot_construct_owner(self):
        recipe,_,_=self.recipe(purpose='independent_acceptance')
        with self.assertRaises(admission.AdmissionUnavailable):self.owner(recipe)
        self.assertFalse((self.root/'raw').exists())

    def test_authored_surrogate_recipe_survives_original_review_owner_and_revalidation(self):
        recipe,_,_=self.recipe(family='b02',case_id='intake-json-deferred-unencodable')
        self.assertIn(b'\\ud800',recipe.original_bytes)
        recipe.revalidate()
        owner,_,_=self.owner(recipe)
        config=profile.decode(owner.read_authenticated('config.json'))
        self.assertEqual(config['registration'],recipe.record()['registration'])
        self.assertEqual(owner.profile,recipe.profile)
        self.assertFalse(owner.has_retained('intent.json'))
        with self.assertRaises(consumer.AuthorityError):observation.publish_verifier(owner)
