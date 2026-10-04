"""Closed V3 source decomposition; identity proposals are not semantic approval."""
from dataclasses import asdict, replace
from pathlib import Path
import unittest

from gossip_harness import cumulative_scope_source_v2 as previous
from gossip_harness import cumulative_scope_source_v3 as source
from gossip_harness import candidate_m2_product_execution_v1 as execution
from gossip_harness import candidate_m2_product_profile_v1 as profile
from gossip_harness import project_acceptance_registry_v1 as registry
from tests.test_project_acceptance_compiler_v1 import synthetic_declaration
from tests.test_cumulative_scope_source_v2 import storage_registration, COHORT
from tests.test_cumulative_scope_source_v1 import cli_registration, http_registration, product_registration

ROOT = Path(__file__).resolve().parents[1]


def m2_registration(case_id='m2-query-generation-pages', purpose='public_release'):
    value=profile.profile_for(case_id,purpose)
    binding=execution.M2Binding('a'*64,execution.TARGET_CONTRACT,'M4',purpose,execution.FAMILY,
        case_id,'b'*64,execution.digest(value.record()),value.sha256,'c'*64,'d'*64,'e'*64,
        execution.digest(execution.evaluator_sources()),'f'*64,'1'*64,'2'*64,'3'*64)
    subject=registry.Subject('cohort',COHORT[0],'M4','4'*64,execution.TARGET_CONTRACT,binding.source_sha256)
    gate=execution.gate_for(subject,binding,gate_id='m2-'+source.sha((case_id+purpose).encode())[:20])
    return execution.M2Registration(binding,'a'*40,'b'*40,'fresh-1',gate,COHORT)


class CumulativeScopeSourceV3Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.catalog=source.load_catalog(ROOT)
        cls.component=source.m2_slice(m2_registration())

    def test_all_twelve_histories_materialize_exact_full_inventory_declaration(self):
        components=tuple(source.m2_slice(m2_registration(c['id'])) for c in profile.native.acceptance_cases())
        declaration=source.assemble_declaration(self.catalog,synthetic_declaration(self.catalog.inventory).cohort,
            components,review_sha256='d'*64,capacity_profile=registry.HISTORY_CAPACITY_PROFILE)
        self.assertEqual(len(declaration.obligations),312)
        self.assertEqual(len(declaration.gates),12)
        self.assertEqual(sum(len(c.gate.ordered_case_ids) for c in components),152)
        self.assertEqual(len({a.obligation_id for c in components for a in c.assertions}),23)
        self.assertEqual(len({(c.history_id,a.case_id) for c in components for a in c.assertions}),136)
        census=source.coverage_census(self.catalog,declaration)
        self.assertEqual(tuple(census[k] for k in ('product_requirements','prerequisites','source_units','authority_rules','gap_notes')),(123,3,312,22,188))
        self.assertEqual(census['product_observations'],0)
        self.assertFalse(census['semantic_approval'])
        self.assertGreater(census['unresolved_cells'],0)
        scope=source.scope_review_input(self.catalog,declaration)
        self.assertTrue(all(not row.not_applicable for row in scope.applicability))
        self.assertEqual(declaration.cohort.milestones,('M1','M2','M3','M4'))
        self.assertEqual(len(declaration.cohort.trajectories),6)

    def test_only_direct_public_contract_capability_and_actual_finite_selectors(self):
        suite,gate,edges=self.component.compiler_records(suite_id='m2',physical_slot='m2-slot')
        self.assertEqual(suite.capabilities,('public-contract',))
        lane={g.id:g.lane for g in self.catalog.inventory.logical_gates}
        self.assertTrue(all(lane[g]=='public-contract' for g in gate.logical_gate_ids))
        self.assertTrue(all(e.assertion_selector.startswith('/projection/observations/') for e in edges))
        self.assertEqual(suite.definition_purpose,'independent_acceptance')
        self.assertEqual(suite.execution_purpose,'public_release')

    def test_reopen_and_unmapped_output_never_mint_source_edges(self):
        for case_id,index in [('m2-migrate-aba-receipt',13),('m2-competing-connections',2),('m2-revision-boundary',5),('m2-revision-boundary',4)]:
            item=source.m2_slice(m2_registration(case_id));case=profile.profile_for(case_id).ordered_case_ids[index]
            self.assertIn(case,item.gate.ordered_case_ids)
            self.assertFalse(any(a.case_id==case for a in item.assertions))
        self.assertEqual(len(profile.profile_for('m2-collection-boundary').phases),69)

    def test_old_families_preserve_exact_semantics_and_closed_version_types(self):
        pairs=[(source.cli_slice(cli_registration()),previous.cli_slice(cli_registration())),
            (source.http_slice(*http_registration()),previous.http_slice(*http_registration())),
            (source.product_process_slice(*product_registration()),previous.product_process_slice(*product_registration())),
            (source.storage_slice(storage_registration()),previous.storage_slice(storage_registration()))]
        for new,old in pairs:
            self.assertEqual(asdict(new),asdict(old));source.verify_slice(new)
            with self.assertRaises(source.ScopeSourceError):source.verify_slice(old)
            with self.assertRaises(previous.ScopeSourceError):previous.verify_slice(new)

    def test_mutated_selector_purpose_assertion_or_unknown_family_cannot_roundtrip(self):
        original=self.component
        changed=(replace(original,family='arbitrary'),replace(original,selectors=original.selectors[:-1]),
            replace(original,original_definition_purpose='public_release'),replace(original,assertions=()),
            replace(original,evaluator_sources=()))
        for item in changed:
            with self.subTest(item=item.family),self.assertRaises(source.ScopeSourceError):source.verify_slice(item)

    def test_every_product_purpose_is_bound_before_execution_and_not_relabelled(self):
        identities=set()
        for purpose in registry.PURPOSES:
            component=source.m2_slice(m2_registration(purpose=purpose));source.verify_slice(component)
            identities.add(component.sha256)
            self.assertEqual(component.original_definition_purpose,'independent_acceptance')
        self.assertEqual(len(identities),len(registry.PURPOSES))
        r=m2_registration();changed=replace(r.binding,purpose='independent_acceptance')
        with self.assertRaises(ValueError):source.m2_slice(replace(r,binding=changed))
