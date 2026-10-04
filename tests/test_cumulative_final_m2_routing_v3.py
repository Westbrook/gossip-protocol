"""Real M2 recipe/normalization/fixture owner; no full semantic scope or Docker."""
from dataclasses import replace
import unittest
from unittest.mock import patch

from gossip_harness import cumulative_final_acceptance_v1 as shared
from gossip_harness import cumulative_final_acceptance_v2 as previous
from gossip_harness import cumulative_final_acceptance_v3 as final
from gossip_harness import cumulative_m2_observation_recipe_v1 as factory
from gossip_harness import candidate_m2_product_execution_v1 as execution
from gossip_harness import candidate_m2_product_observation_v1 as observer
from gossip_harness import candidate_scope_consumer_v1 as consumer
from tests import test_candidate_m2_product_execution_v1 as fixtures


class CumulativeFinalM2RoutingV3Tests(unittest.TestCase):
    setUpClass=classmethod(fixtures.CandidateM2ProductExecutionV1Tests.setUpClass.__func__)
    setUp=fixtures.CandidateM2ProductExecutionV1Tests.setUp
    make=fixtures.CandidateM2ProductExecutionV1Tests.make

    def specification(self):
        recipe=factory.build_m2_recipe(self.store,self.subject,case_id=self.value.case_id,purpose=self.value.purpose,
            policy=self.policy,runtime={'kind':'fixture-no-Docker'},layout_plan=self.plan,layout_authority=self.review,
            gate_id=self.gate.gate_id,repetition_id=self.registration.repetition_id,cohort_trajectory_ids=fixtures.COHORT)
        spec=recipe.spec(root=self.raw,delta_root=self.delta,cleanup_root=self.root/'cleanup',checkpoint_authority=self.head)
        return recipe,spec

    def test_actual_recipe_shared_normalization_and_fixture_owner_agree(self):
        recipe,spec=self.specification()
        self.assertIs(type(spec),shared.ObservationSpec)
        registered=spec.observation_registration()
        self.assertEqual(registered,execution.observation_registration(recipe.registration))
        owner=factory.construct_m2_owner(spec,self.admission,mode='fixture');self.addCleanup(owner.close)
        self.assertIs(type(owner),execution.CandidateM2Execution)
        self.assertEqual(owner.observation_registration,registered)
        with self.assertRaises(observer.AuthorityError):observer.M2ObservationSource(owner,owner.checkpoint())
        self.assertFalse(owner.has_retained('intent.json'))

    def test_foreign_family_and_auxiliary_profile_cannot_normalize(self):
        _,spec=self.specification()
        for changed in (replace(spec,kind='storage'),replace(spec,recipe={}),replace(spec,cumulative_profile={}),
                        replace(spec,layout_authority=object()),replace(spec,profile=object())):
            with self.assertRaises(consumer.AuthorityError):changed.observation_registration()
        self.assertFalse(self.raw.exists())

    def test_v1_and_v2_dispatch_refuse_m2_before_any_owner_effect(self):
        _,spec=self.specification()
        for cls in (shared.FinalAcceptance,previous.FinalAcceptanceV2):
            with self.assertRaisesRegex(consumer.AuthorityError,'family is not'):
                shared.FinalAcceptance.dispatch(object.__new__(cls),spec)
        self.assertIn('m2',shared._known_contract(object.__new__(final.FinalAcceptanceV3)).families)
        self.assertFalse(self.raw.exists())

    def test_v3_constructor_routes_only_exact_new_m2_factory(self):
        # Only the choice of constructor is stubbed. The real fixture construction
        # path is exercised above; no physical-mode constructor or Engine call runs.
        _,spec=self.specification();owner=object.__new__(final.FinalAcceptanceV3)
        sentinel=object()
        with patch.object(factory,'construct_m2_owner',return_value=sentinel) as construct:
            self.assertIs(owner._construct(spec,self.admission),sentinel)
            construct.assert_called_once_with(spec,self.admission,mode='physical')
        self.assertFalse(self.raw.exists())
