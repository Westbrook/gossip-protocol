"""Closed MAP-B precision and real prospective joins; never candidate acceptance.

These controls are authored for the verification owner. Fixture reviewers are
synthetic originals, and fixture owners cannot dispatch candidate code.
"""
from dataclasses import asdict, fields, replace
from pathlib import Path
import unittest

from gossip_harness import candidate_m2_product_profile_v1 as profile
from gossip_harness import candidate_m2_product_execution_v1 as execution
from gossip_harness import candidate_m2_product_observation_v1 as observer
from gossip_harness import candidate_m2_review_authority_v1 as review
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import cumulative_finite_mapping_v1 as finite
from gossip_harness import cumulative_m2_observation_recipe_v1 as factory
from gossip_harness import cumulative_rehearsal_codec_v1 as codec
from gossip_harness import cumulative_scope_source_v3 as source
from gossip_harness import project_acceptance_registry_v1 as registry
from tests import test_candidate_m2_product_execution_v1 as fixtures


def mapped(case_id, purpose='public_release'):
    return profile.profile_for(case_id, purpose, mapping_profile=finite.M2_MAPPING)


def unit(name, clause):
    return 'M2-' + name + ':clause:' + str(clause)


def associations(value):
    return {(row['observation_index'], facet['source_unit_id'])
        for row in value.selectors() for facet in row['source_unit_facets']}


def registration(value):
    """Identity proposal only; no review or execution authority supplied."""
    binding = execution.M2Binding('a'*64, execution.TARGET_CONTRACT, 'M4', value.purpose,
        execution.FAMILY, value.case_id, 'b'*64, execution.digest(value.record()), value.sha256,
        'c'*64, 'd'*64, 'e'*64, execution.digest(execution.evaluator_sources()), 'f'*64,
        '1'*64, '2'*64, '3'*64,
        protocol=execution.PROTOCOL if value.mapping_profile is None else execution.MAPPED_PROTOCOL)
    subject = registry.Subject('cohort', fixtures.COHORT[0], 'M4', '4'*64,
        execution.TARGET_CONTRACT, binding.source_sha256)
    gate = execution.gate_for(subject, binding, gate_id='finite-m2')
    return execution.M2Registration(binding, 'a'*40, 'b'*40, 'fresh-1', gate, fixtures.COHORT)


class CandidateM2FiniteMappingV2Tests(unittest.TestCase):
    def test_opt_in_preserves_all_twelve_original_histories_and_legacy_dataclass_fields(self):
        cases = profile.native.acceptance_cases()
        self.assertEqual((len(cases), sum(len(c['input']['actions']) for c in cases),
            sum(len(c['expected']['observations']) for c in cases)), (12, 224, 140))
        for case in cases:
            old, new = profile.profile_for(case['id']), mapped(case['id'])
            explicit_none = profile.profile_for(case['id'], mapping_profile=None)
            self.assertIs(type(old), profile.M2Profile)
            self.assertIs(type(new), profile.MappedM2Profile)
            self.assertEqual(profile.encoded(old.record()), profile.encoded(explicit_none.record()))
            self.assertNotIn('mapping_profile', old.record())
            self.assertEqual(asdict(old), asdict(new))
            self.assertEqual([f.name for f in fields(new)], ['case_id', 'purpose'])
            self.assertEqual(new.record()['definition'], case)
            self.assertEqual(new.record()['original_requirement_ids'], case['requirement_ids'])
            self.assertEqual(new.ordered_case_ids, old.ordered_case_ids)
            self.assertEqual(new.phases, old.phases)
            self.assertEqual(new.diagnostic_case_ids, old.diagnostic_case_ids)
            self.assertNotEqual(new.sha256, old.sha256)
            self.assertEqual(new.record()['mapping_sources'], finite.sources())

    def test_exact_selected_association_delta_has_no_other_new_or_removed_unit_edges(self):
        additions = {
            'm2-migrate-aba-receipt': {(12, unit('IDENTITY-REVISIONS', 1)),
                (12, unit('IDENTITY-REVISIONS', 3)), (14, unit('IDENTITY-REVISIONS', 1))},
            'm2-normalized-membership-tombstone': {(22, unit('IDENTITY-REVISIONS', 1))},
            'm2-revision-boundary': {(4, unit('QUERY', 3))},
            'm2-retained-blob-capacity': {(3, unit('IDENTITY-REVISIONS', 3))},
            'm2-service-route-contract': {(10, unit('REFRESH', 2)), (11, unit('REFRESH', 2))},
        }
        removals = {
            'm2-normalized-membership-tombstone': {(i, unit('ANNOTATIONS', j))
                for i in (5, 6, 13, 14) for j in (0, 1)},
            'm2-query-generation-pages': {(i, unit('QUERY', 2)) for i in (13, 14, 15, 16)}
                | {(8, unit('QUERY', 1)), (9, unit('QUERY', 0))},
            'm2-refresh-input-boundaries': {(i, unit('REFRESH', 1)) for i in (14, 15)},
            'm2-service-route-contract': {(i, unit('INTERFACES', 1)) for i in (1, 2)},
            'm2-revision-boundary': {(i, unit('IDENTITY-REVISIONS', 3)) for i in (0, 6)},
        }
        for case in profile.native.acceptance_cases():
            old, new = associations(profile.profile_for(case['id'])), associations(mapped(case['id']))
            with self.subTest(case=case['id']):
                self.assertEqual(new-old, additions.get(case['id'], set()))
                self.assertEqual(old-new, removals.get(case['id'], set()))

    def test_narrow_error_and_query_selectors_do_not_claim_unobserved_subclauses(self):
        cases = [('m2-normalized-membership-tombstone', (5, 6, 13, 14), unit('ANNOTATIONS', 2)),
            ('m2-query-generation-pages', (13, 14, 15, 16, 9), unit('QUERY', 1)),
            ('m2-query-generation-pages', (8,), unit('QUERY', 0)),
            ('m2-refresh-input-boundaries', (14, 15), unit('REFRESH', 0)),
            ('m2-service-route-contract', (1, 2), unit('INTERFACES', 0))]
        for case_id, indices, expected in cases:
            rows = mapped(case_id).selectors()
            for index in indices:
                with self.subTest(case=case_id, index=index):
                    self.assertEqual(rows[index]['source_unit_ids'], [expected])
        old = profile.profile_for('m2-query-generation-pages').selectors()
        self.assertEqual(set(old[8]['source_unit_ids']), {unit('QUERY', 0), unit('QUERY', 1)})

    def test_revision_head_history_and_reopened_head_have_separate_finite_meanings(self):
        value = mapped('m2-revision-boundary')
        rows = value.selectors()
        self.assertEqual(rows[0]['source_unit_ids'], [unit('IDENTITY-REVISIONS', 1)])
        self.assertEqual(rows[6]['source_unit_ids'], [unit('IDENTITY-REVISIONS', 1)])
        self.assertEqual(set(rows[3]['source_unit_ids']),
            {unit('IDENTITY-REVISIONS', 1), unit('IDENTITY-REVISIONS', 3)})
        self.assertEqual(rows[4]['source_unit_ids'], [unit('QUERY', 3)])
        texts = [rows[i]['source_unit_facets'][0]['rationale'] for i in (0, 3, 6)]
        self.assertEqual(len(set(texts)), 3)
        self.assertIn('before reopen', texts[1])
        self.assertIn('no complete history reread', texts[2])
        self.assertEqual(profile.case_definition(value.case_id)['expected']['observations'][4]['generation'], 15)

    def test_only_three_selected_histories_add_requirement_owners_in_stable_order(self):
        additions = {'m2-normalized-membership-tombstone': ('M2-IDENTITY-REVISIONS',),
            'm2-revision-boundary': ('M2-QUERY',), 'm2-service-route-contract': ('M2-REFRESH',)}
        for case in profile.native.acceptance_cases():
            old, new = profile.profile_for(case['id']), mapped(case['id'])
            self.assertEqual(new.requirement_ids, old.requirement_ids + additions.get(case['id'], ()))
            self.assertTrue(all(key.startswith('M2-') for _, key in associations(new)))

    def test_unknown_mapping_and_arbitrary_subclasses_are_rejected(self):
        case_id = 'm2-revision-boundary'
        for marker in ('MAP-B', '', False, {}, finite.STORAGE_MAPPING):
            with self.subTest(marker=marker), self.assertRaises(ValueError):
                profile.profile_for(case_id, mapping_profile=marker)
        class ForeignProfile(profile.MappedM2Profile):
            pass
        value = ForeignProfile(case_id, 'public_release')
        self.assertFalse(profile.accepted_profile(value))
        with self.assertRaises(ValueError): profile.reconstruct(value)
        with self.assertRaises(ValueError): profile.project(value, {}, {})

    def test_named_mapping_preserves_known_failure_unknown_and_setup_diagnostics(self):
        value = mapped('m2-collection-boundary')
        inputs = {0: None, 64: {'error': 'wrong'}}
        result = profile.project(value, inputs, {})
        self.assertEqual(result, profile.project(profile.profile_for(value.case_id), inputs, {}))
        self.assertEqual(result['observations'][0]['disposition'], 'fail')
        self.assertEqual(result['observations'][1]['disposition'], 'unavailable')
        self.assertEqual(result['diagnostics'][0]['disposition'], 'unspecified')

    def test_typed_codec_preserves_exact_named_profile_plan_and_protocol_without_new_fields(self):
        value = mapped('m2-service-route-contract')
        reg = registration(value)
        plan = review.MappedLayoutPlan(execution.FAMILY, 'a'*64, 'b'*64, 'a'*40, 'b'*40,
            value.case_id, value.sha256, 'reviewed-m2-final-sqlite-v1', ('m2/library.sqlite',), 'c'*64)
        transported = codec.unpack(codec.pack((value, plan, reg)))
        self.assertIs(type(transported[0]), profile.MappedM2Profile)
        self.assertIs(type(transported[1]), review.MappedLayoutPlan)
        self.assertEqual(transported, (value, plan, reg))
        self.assertEqual(fields(plan), fields(review.LayoutPlan))
        self.assertEqual(plan.request()['profile'], value.record())
        self.assertEqual(execution.profile_for_binding(transported[2].binding), value)
        with self.assertRaises(ValueError): review.LayoutPlan(**asdict(plan)).request()
        with self.assertRaises(ValueError):
            review.layout_plan_from_record(asdict(plan), mapping_profile='unknown')
        self.assertEqual(review.layout_plan_from_record(asdict(plan), mapping_profile=finite.M2_MAPPING), plan)

    def test_closed_binding_protocol_cannot_drop_or_replace_mapping_identity(self):
        value = mapped('m2-revision-boundary')
        reg = registration(value)
        self.assertEqual(execution.mapping_profile_for_protocol(reg.binding.protocol), finite.M2_MAPPING)
        self.assertEqual(execution.profile_for_binding(reg.binding), value)
        for binding in (replace(reg.binding, protocol=execution.PROTOCOL),
                replace(reg.binding, profile_sha256=profile.profile_for(value.case_id).sha256)):
            with self.assertRaises(ValueError): execution.profile_for_binding(binding)
            with self.assertRaises(ValueError): execution.observation_registration(replace(reg, binding=binding))
        with self.assertRaises(ValueError): replace(reg.binding, protocol='unknown')

    def test_source_factory_joins_new_owners_and_rejects_relabelled_original_binding(self):
        catalog = source.load_catalog(Path(__file__).resolve().parents[1])
        for case_id, unit_id, index in [('m2-revision-boundary', unit('QUERY', 3), 4),
                ('m2-normalized-membership-tombstone', unit('IDENTITY-REVISIONS', 1), 22),
                ('m2-service-route-contract', unit('REFRESH', 2), 10)]:
            value = mapped(case_id)
            reg = registration(value)
            component = source.m2_slice(reg)
            source.verify_slice(component)
            matches = [edge for edge in component.assertions if edge.obligation_id == unit_id
                and edge.case_id == value.ordered_case_ids[index]]
            self.assertEqual(len(matches), 1)
            self.assertTrue(matches[0].assertion.logical_gate_ids)
            self.assertIn(unit_id.split(':')[0], component.gate.requirement_ids)
            self.assertTrue(all(catalog.unit(edge.obligation_id).obligation.id.startswith('M2-')
                for edge in component.assertions))
            rows = observer.selector_catalog(case_id, purpose=value.purpose, mapping_profile=finite.M2_MAPPING)
            self.assertEqual(rows['capabilities'], ['public-contract', 'direct-api', 'reviewed-sqlite-capture'])
            self.assertEqual(rows['profile_sha256'], value.sha256)
            self.assertFalse(rows['semantic_authority'])
            with self.assertRaises(ValueError):
                source.m2_slice(replace(reg, binding=replace(reg.binding, protocol=execution.PROTOCOL)))


class CandidateM2FiniteMappingOwnerV2Tests(unittest.TestCase):
    setUpClass = classmethod(fixtures.CandidateM2ProductExecutionV1Tests.setUpClass.__func__)

    def setUp(self):
        self.root = self.base / self.id().rsplit('.', 1)[-1]
        self.root.mkdir()
        self.make()

    def make(self, purpose='public_release'):
        self.value = mapped('m2-service-route-contract', purpose)
        schema = fixtures.storage_observer.sqlite_schema_sha256(
            profile.fixture_files(self.value.case_id)['seed.sqlite'])
        self.plan = review.MappedLayoutPlan(execution.FAMILY, admission.source_sha256(self.files),
            profile.source_sha256(self.files), self.commit, self.tree, self.value.case_id,
            self.value.sha256, 'reviewed-m2-final-sqlite-v1', ('m2/library.sqlite',), schema,
            'ordinary_public_operations', purpose)
        self.review, journal, review_head = fixtures.enroll(self.root, self.plan)
        self.addCleanup(review_head.close)
        self.addCleanup(journal.close)
        self.policy = execution.M2Policy()
        self.binding = execution.binding_for(self.files, self.value, self.policy,
            {'kind': 'fixture-no-Docker'}, self.plan, review_authority=self.review)
        self.subject = registry.Subject('cohort', fixtures.COHORT[0], 'M4', 'a'*64,
            execution.TARGET_CONTRACT, self.binding.source_sha256)
        self.gate = execution.gate_for(self.subject, self.binding, gate_id='finite-m2-owner')
        self.registration = execution.M2Registration(self.binding, self.commit, self.tree,
            'fresh-1', self.gate, fixtures.COHORT)
        registered = execution.observation_registration(self.registration)
        self.admission = admission.ObservationAdmission(registered,
            verify_registration=lambda: registered, verify_cohort=lambda: None)
        self.raw, self.delta = self.root/'raw', self.root/'delta'
        self.head = fixtures.ExternalHead.create(self.root/'head', journal_roots=(self.raw, self.delta))
        self.addCleanup(self.head.close)

    def recipe(self, **changes):
        args = {'case_id': self.value.case_id, 'purpose': self.value.purpose,
            'policy': self.policy, 'runtime': {'kind': 'fixture-no-Docker'},
            'layout_plan': self.plan, 'layout_authority': self.review, 'gate_id': self.gate.gate_id,
            'repetition_id': self.registration.repetition_id, 'cohort_trajectory_ids': fixtures.COHORT,
            'mapping_profile': finite.M2_MAPPING}
        args.update(changes)
        return factory.build_m2_recipe(self.store, self.subject, **args)

    def spec(self):
        return self.recipe().spec(root=self.raw, delta_root=self.delta,
            cleanup_root=self.root/'cleanup', checkpoint_authority=self.head)

    def test_actual_recipe_scope_spec_fixture_owner_and_reopen_preserve_mapping(self):
        recipe = self.recipe()
        self.assertEqual(recipe.registration, self.registration)
        source.verify_slice(recipe.scope_slice())
        owner = factory.construct_m2_owner(self.spec(), self.admission, mode='fixture')
        self.addCleanup(owner.close)
        self.assertEqual(owner.config['protocol'], execution.MAPPED_PROTOCOL)
        self.assertEqual(owner.config['profile']['mapping_profile'], finite.M2_MAPPING)
        self.assertEqual(owner.config['source_capture_policy'], execution.SOURCE_CAPTURE_POLICY.record())
        self.assertEqual(owner.config['profile']['definition'], profile.case_definition(self.value.case_id))
        self.assertIn('M2-REFRESH', owner.registration.gate.requirement_ids)
        checkpoint = owner.checkpoint()
        owner.close()
        reopened = execution.CandidateM2Execution(self.raw, self.store, self.registration, self.policy,
            value=self.value, plan=self.plan, review_authority=self.review,
            admission_authority=self.admission, checkpoint_authority=self.head,
            delta_root=self.delta, cleanup_root=self.root/'cleanup', mode='fixture', expected_checkpoint=checkpoint)
        self.addCleanup(reopened.close)
        self.assertEqual(reopened.checkpoint(), checkpoint)
        with self.assertRaises(execution.ExecutionError): reopened.execute_once()
        with self.assertRaises(observer.AuthorityError): observer.M2ObservationSource(reopened, checkpoint)

    def test_missing_marker_profile_or_new_owner_is_refused_before_execution_journal(self):
        with self.assertRaises(ValueError): self.recipe(mapping_profile=None)
        spec = self.spec()
        shortened = replace(self.gate, requirement_ids=tuple(
            key for key in self.gate.requirement_ids if key != 'M2-REFRESH'))
        for changed in (replace(spec, profile=profile.profile_for(self.value.case_id)),
                replace(spec, registration=replace(self.registration, gate=shortened)),
                replace(spec, layout_plan=review.LayoutPlan(**asdict(self.plan)))):
            with self.subTest(profile=type(changed.profile).__name__), self.assertRaises(ValueError):
                factory.construct_m2_owner(changed, self.admission, mode='fixture')
            self.assertFalse(self.raw.exists())

    def test_mapped_review_does_not_authenticate_a_legacy_profile_request(self):
        legacy = profile.profile_for(self.value.case_id)
        legacy_plan = review.LayoutPlan(**{**asdict(self.plan), 'profile_sha256': legacy.sha256})
        with self.assertRaises(admission.AdmissionError): self.review.authenticate(legacy_plan)
        self.assertFalse(self.raw.exists())

    def test_independent_mapping_still_requires_the_exact_cohort_freeze(self):
        self.root = self.root/'independent'
        self.root.mkdir()
        self.make('independent_acceptance')
        with self.assertRaises(admission.AdmissionUnavailable):
            factory.construct_m2_owner(self.spec(), self.admission, mode='fixture')
        self.assertFalse(self.raw.exists())
