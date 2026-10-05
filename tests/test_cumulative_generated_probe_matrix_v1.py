"""Logical phase/cell census only; no candidate, Docker or model execution."""
from dataclasses import replace
import json
import unittest

from gossip_harness import cumulative_generated_probe_matrix_v1 as matrix
from gossip_harness import cumulative_generated_probe_context_v1 as contexts
from gossip_harness import cumulative_generated_probe_values_v2 as values
from gossip_harness import cumulative_study_controller_v2 as study
from tests.test_cumulative_generated_probe_context_v1 import fixture, proposal_change
from tests.test_cumulative_generated_probe_plan_v1 import release
from tests.test_cumulative_generated_probe_values_v2 import admitted, proposal


def probes(templates=None):
    if templates is None:templates=tuple(values.TEMPLATES)
    rows=[matrix.ActiveProbe(values.canonical(admitted(proposal(t))),study.digest(['origin',t]),
                             'M3' if t=='manifest-content-hash-v1' else 'M2') for t in templates]
    return tuple(sorted(rows,key=lambda p:json.loads(p.admitted_raw)['probe_id']))


def compile_matrix(*, arm='S16-G', generation=None, active=None):
    generation=replace(fixture(arm),milestone='M4') if generation is None else generation
    public=release('M4',('M2-REFRESH','M3-BACKUP-RESTORE'))
    public=replace(public,ordered_check_ids=('public-a','public-b'))
    return matrix.compile_matrix(generation,current=public,inherited=release('M3'),
                                 active_probes=probes() if active is None else active)


class GeneratedProbeMatrixTests(unittest.TestCase):
    def test_complete_four_template_matrix_counts_small_and_large_arms(self):
        for arm,expected in [('S4-G',(4,15,5,24)),('S16-G',(16,60,5,81)),('O16-G',(16,60,5,81))]:
            with self.subTest(arm=arm):
                plan=compile_matrix(arm=arm);record=plan.record()
                self.assertEqual(tuple(record['execution_cell_counts'][p] for p in ['before_review','contextual','merged','total']),expected)
                self.assertFalse(record['dispatch_authority']);self.assertFalse(record['acceptance_authority'])
                self.assertIn('aggregate_raw_byte_capacity',record['not_yet_admitted'])
                self.assertEqual(len({c.sha256 for c in (*plan.base_cells,*plan.contextual_cells,*plan.merged_cells)}),expected[-1])

    def test_clients_always_require_authored_checks_even_without_generated_coverage(self):
        plan=compile_matrix()
        for actor in ('B13','B14','B15','B16'):
            rows=[c for c in plan.contextual_cells if c.actor==actor]
            self.assertEqual(len(rows),1);self.assertEqual(rows[0].kind,'authored-public-suite')
            self.assertEqual(rows[0].ordered_check_ids,('public-a','public-b'))
            self.assertEqual(rows[0].use,'required-complete-public-evidence')
        self.assertEqual(matrix.catalog()['unsupported_generated_packages'],['clients'])

    def test_before_review_cells_do_not_depend_on_future_rankings_or_new_probes(self):
        gen=replace(fixture(),milestone='M4')
        unknown=replace(gen,rankings=tuple(replace(r,actors=(),disposition='unknown') for r in gen.rankings))
        early=compile_matrix(generation=unknown,active=())
        later=compile_matrix(generation=gen)
        self.assertEqual(early.build_phase_raw,later.build_phase_raw)
        self.assertEqual([c.sha256 for c in early.base_cells],[c.sha256 for c in later.base_cells])
        self.assertNotEqual(early.post_review_raw,later.post_review_raw)

    def test_pre_review_failures_are_not_an_unconditional_candidate_veto(self):
        plan=compile_matrix()
        self.assertTrue(all(c.use=='inform-review-only-no-unconditional-veto' for c in plan.base_cells))
        self.assertTrue(all(c.phase=='before-review' and c.context_raw is not None for c in plan.base_cells))

    def test_every_eligible_unranked_candidate_remains_in_the_matrix(self):
        gen=replace(fixture(),milestone='M4')
        gen=replace(gen,rankings=tuple(replace(r,actors=r.actors[:1]) for r in gen.rankings))
        record=compile_matrix(generation=gen).record()
        self.assertEqual(record['execution_cell_counts']['total'],81)
        self.assertEqual({c['actor'] for c in record['cells']['contextual']},set(study.actors_for('S16-G')[:-4]))

    def test_missing_anchor_keeps_all_post_review_rows_unavailable(self):
        gen=replace(fixture(),milestone='M4')
        gen=replace(gen,rankings=(replace(gen.rankings[0],actors=(),disposition='unknown'),*gen.rankings[1:]))
        plan=compile_matrix(generation=gen)
        self.assertEqual(len(plan.contextual_cells),60)
        self.assertTrue(all(c.record()['source_binding_status']=='peer_anchor_unavailable' for c in plan.contextual_cells))
        self.assertEqual(json.loads(plan.post_review_raw)['missing_anchor_packages'],['catalog'])
        self.assertEqual(len(plan.merged_cells),5)

    def test_failed_builder_stays_in_role_census_without_an_invented_source(self):
        gen=replace(fixture(),milestone='M4')
        gen=replace(gen,rankings=(replace(gen.rankings[0],actors=('B01',)),*gen.rankings[1:]))
        gen=proposal_change(gen,'B02',disposition='failed',changes=())
        plan=compile_matrix(generation=gen);census=json.loads(plan.post_review_raw)['role_census']
        self.assertEqual(len(census),16);self.assertEqual(census[1]['disposition'],'failed')
        self.assertNotIn('B02',{c.actor for c in (*plan.base_cells,*plan.contextual_cells)})

    def test_fresh_merged_checks_are_reserved_as_distinct_unbound_cells(self):
        plan=compile_matrix(arm='S4-G')
        self.assertTrue(all(c.actor is None and c.context_raw is None for c in plan.merged_cells))
        self.assertTrue(all(c.record()['source_binding_status']=='requires_selected_merged_source' for c in plan.merged_cells))
        self.assertTrue(all(c.record()['fresh_execution_required'] and not c.record()['execution_reuse_granted'] for c in plan.merged_cells))
        self.assertTrue({c.sha256 for c in plan.merged_cells}.isdisjoint(c.sha256 for c in plan.contextual_cells))

    def test_equal_anchor_bytes_never_silently_deduplicate_logical_exposures(self):
        plan=compile_matrix(arm='S4-G')
        sources=[json.loads(c.context_raw)['source_sha256'] for c in plan.contextual_cells]
        self.assertEqual(len(set(sources)),1)
        self.assertEqual(len(plan.contextual_cells),15)
        self.assertEqual(len({c.sha256 for c in plan.contextual_cells}),15)

    def test_duplicate_unsorted_or_unreleased_probe_corpus_is_not_filtered(self):
        active=probes()
        for bad in [(*active,active[0]),tuple(reversed(active))]:
            with self.subTest(bad=bad),self.assertRaisesRegex(ValueError,'sorted_unique'):compile_matrix(active=bad)
        with self.assertRaises(ValueError):
            matrix.compile_matrix(replace(fixture(),milestone='M3'),current=release('M3',('M2-REFRESH',)),
                                  inherited=release('M2'),active_probes=active)

    def test_carry_forward_preserves_original_semantics_and_origin(self):
        active=probes(('refresh-noop-v1',));plan=compile_matrix(active=active)
        row=json.loads(plan.post_review_raw)['active_probes'][0]
        self.assertEqual(values.canonical(row['probe']),active[0].admitted_raw)
        self.assertEqual(row['original_record_sha256'],active[0].original_record_sha256)
        self.assertEqual(row['origin_milestone'],'M2')
        with self.assertRaisesRegex(ValueError,'origin_precedes_requirement'):
            compile_matrix(active=(replace(probes(('manifest-content-hash-v1',))[0],origin_milestone='M2'),))

    def test_release_pair_cannot_skip_a_milestone_or_claim_future_probes(self):
        gen=replace(fixture(),milestone='M4')
        with self.assertRaisesRegex(ValueError,'previous_and_current'):
            matrix.compile_matrix(gen,current=release('M4'),inherited=release('M2'),active_probes=())
        with self.assertRaisesRegex(ValueError,'future_probe_origin'):
            matrix.compile_matrix(fixture(),current=release('M2'),inherited=release('M1'),
                                  active_probes=(replace(probes(('refresh-noop-v1',))[0],origin_milestone='M3'),))

    def test_footprint_records_real_query_dependency_without_claiming_full_requirement(self):
        rows={x['template_id']:x for x in matrix.catalog()['templates']}
        refresh=rows['refresh-identity-v1'];manifest=rows['manifest-content-hash-v1']
        self.assertIn('query',refresh['target_packages']);self.assertIn('Service.refresh_document',refresh['invoked_interfaces'])
        self.assertEqual(refresh['graph_prerequisites'],['M2-IDENTITY-REVISIONS'])
        self.assertNotIn('query',manifest['target_packages'])
        self.assertIn('library.ingestion.local.import_file',manifest['required_imports'])
        self.assertFalse(manifest['whole_requirement_coverage']);self.assertFalse(manifest['setup_or_graph_completion_inferred'])

    def test_returned_records_cannot_rewrite_the_frozen_matrix(self):
        plan=compile_matrix();original=values.canonical(plan.record());record=plan.record()
        record['cells']['merged'].clear();record['post_review']['active_probes'].clear()
        self.assertEqual(values.canonical(plan.record()),original)
