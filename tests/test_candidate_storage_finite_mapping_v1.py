"""Prospective finite mappings and real fixture ownership; no product approval.

The controller runs these controls after the source-only implementation review.
Synthetic layout enrollment tests byte/source linkage, never semantic adequacy.
"""
from collections import Counter
from copy import deepcopy
from dataclasses import asdict, replace
from pathlib import Path
import tempfile
import unittest

from gossip_harness import candidate_storage_product_profile_v1 as profile
from gossip_harness import candidate_storage_product_execution_v1 as execution
from gossip_harness import candidate_storage_product_observation_v1 as observation
from gossip_harness import candidate_storage_review_authority_v1 as review
from gossip_harness import candidate_source_capture_policy_v1 as capture
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import candidate_scope_consumer_v1 as consumer
from gossip_harness import cumulative_finite_mapping_v1 as finite
from gossip_harness import cumulative_scope_source_v3 as source
from gossip_harness import cumulative_observation_recipe_factory_v2 as factory
from gossip_harness import cumulative_rehearsal_codec_v1 as codec
from gossip_harness import project_acceptance_registry_v1 as registry
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from gossip_harness.gitstore import GitStore
from tests.test_candidate_storage_product_profile_v1 import observations
from tests.test_candidate_storage_product_execution_v1 import enroll, COHORT
from tests.test_candidate_storage_observer_v1 import physical_files, SQLITE_LAYOUT
from tests.test_cumulative_observation_recipe_factory_v2 import capture_calls


def mapped(case_id):
    return profile.profile_for('b02', case_id, 'independent_acceptance', mapping_profile=finite.STORAGE_MAPPING)


def cells(value):
    return {row['check_id']: row for row in value.record()['diagnostics']}


def declared_registration(value, *, batch=True):
    """Identity proposal only; cannot enroll layout/source/semantic authority."""
    protocol = execution.protocol_for(profile.mapping_profile_for(value), capture.BatchCapturePolicy() if batch else None)
    binding = execution.StorageBinding('a' * 64, execution.TARGET_CONTRACT, 'M4', value.purpose,
        value.family, value.case_id, 'b' * 64, execution.digest(value.record()), value.sha256,
        'c' * 64, 'd' * 64, execution.digest(execution.evaluator_sources()), 'e' * 64,
        'f' * 64, '1' * 64, '2' * 64, protocol=protocol)
    subject = registry.Subject('fixture-cohort', COHORT[0], 'M4', '3' * 64,
        execution.TARGET_CONTRACT, binding.source_sha256)
    gate = execution.gate_for(subject, binding, gate_id='finite-' + execution.digest(value.case_id)[:24])
    return execution.StorageRegistration(binding, 'a' * 40, 'b' * 40, 'fresh-fixture', gate, COHORT)


class CandidateStorageFiniteMappingTests(unittest.TestCase):
    def test_exact_24_new_allocations_survive_to_actual_scope_assertions(self):
        expected = {(row.unit_id, row.history_id, row.predicate_id.removeprefix('storage:'),
            row.kind, row.logical_gate_id) for row in finite.allocations('storage-b02')}
        self.assertEqual(len(expected), 24)
        self.assertEqual(Counter(row[0] for row in expected),
            {'m1:M1-J03': 8, 'm1:M1-J09': 12, 'm1:M1-I25': 4})
        actual = set()
        for history in sorted({row[1] for row in expected}):
            value = mapped(history)
            component = source.storage_slice(declared_registration(value))
            source.verify_slice(component)
            declarations = {(row.obligation_id, row.case_id, row.assertion.kind, row.assertion.logical_gate_ids)
                            for row in component.assertions}
            for check, cell in cells(value).items():
                for facet in cell['source_unit_facets']:
                    if facet.get('allocation_group') != 'new-unit-24':
                        continue
                    row = (facet['source_unit_id'], history, check, facet['class'], facet['logical_gate_ids'][0])
                    actual.add(row)
                    self.assertIn((row[0], check, row[3], (row[4],)), declarations)
            suite, _, _ = component.compiler_records(suite_id='finite-fixture', physical_slot='finite-fixture')
            self.assertEqual(suite.capabilities, ('intake', 'jobs', 'durability'))
        self.assertEqual(actual, expected)

    def test_original_recipes_expectations_roster_and_default_profile_are_unchanged(self):
        cases = sorted({row.history_id for row in finite.allocations('storage-b02')} |
            {'store-commit_job-completed-current', 'cancel-retry-token-history', 'job-id-one'})
        for history in cases:
            old = profile.profile_for('b02', history, 'independent_acceptance')
            new = mapped(history)
            self.assertNotIn('mapping_profile', old.record())
            self.assertNotEqual(new.sha256, old.sha256)
            self.assertEqual(new.record()['original_definition'], old.record()['original_definition'])
            self.assertEqual(new.record()['original_case_sha256'], old.record()['original_case_sha256'])
            self.assertEqual(new.diagnostic_ids, old.diagnostic_ids)
            self.assertEqual(new.decisive_ids, old.decisive_ids)
            self.assertEqual(new.native_history_ids, old.native_history_ids)
            self.assertEqual(new.record()['original_requirement_ids'], list(old.requirement_ids))
            self.assertFalse(new.record()['semantic_authority'])

    def test_seven_successful_jobs_keep_exact_job_meaning_without_token_or_error_credit(self):
        successful = ('state-cancel_job-queued', 'state-cancel_job-running', 'state-cancel_job-cancelled',
            'state-retry_job-cancelled', 'state-retry_job-failed', 'job-id-one', 'job-id-max')
        for history in successful:
            found = [row for row in cells(mapped(history))['after.result.0']['source_unit_facets']
                     if row['source_unit_id'] == 'm1:M1-J01']
            self.assertEqual(len(found), 1)
            self.assertEqual(found[0]['observation_meaning'], 'JOB')
            self.assertIn('nullable error', found[0]['scope'])
        for history in ('state-prepare-queued', 'state-prepare-running', 'state-cancel_job-completed', 'job-id-empty'):
            self.assertFalse(any(row['source_unit_id'] == 'm1:M1-J01'
                for row in cells(mapped(history))['after.result.0']['source_unit_facets']))
        self.assertFalse(any(row['source_unit_id'] == 'm1:M1-J01'
            for row in cells(mapped('job-id-empty'))['after.persisted-state']['source_unit_facets']))

    def test_a03_exact_refusals_and_replay_exclude_initial_running_commit(self):
        for state in ('queued', 'running', 'completed', 'cancelled', 'failed'):
            for relation in ('lower', 'current', 'higher'):
                value = mapped(f'store-commit_job-{state}-{relation}')
                facets = [row for row in cells(value)['after.result.0']['source_unit_facets']
                          if row['source_unit_id'] == 'm1:M1-A03']
                if state == 'running' and relation == 'current':
                    self.assertFalse(facets)
                else:
                    self.assertEqual(len(facets), 1)
                    self.assertEqual(facets[0]['class'], 'history' if state == 'completed' and relation == 'current' else 'negative')
                    self.assertEqual(facets[0]['logical_gate_ids'], ['M1-GATE-ATOMIC'])
        fresh = cells(mapped('fresh-value-commit_job'))
        self.assertTrue(any(row['source_unit_id'] == 'm1:M1-A03' for row in fresh['after.result.2']['source_unit_facets']))
        self.assertFalse(any(row['source_unit_id'] == 'm1:M1-A03' for row in fresh['after.result.0']['source_unit_facets']))

    def test_pair_receipt_job_and_exact_input_conservation_selectors(self):
        commit = cells(mapped('store-commit_job-running-current'))
        selected = {check for check, cell in commit.items() for row in cell['source_unit_facets']
                    if row['source_unit_id'] == 'm1:M1-A04'}
        self.assertEqual(selected, {'after.result.0', 'after.persisted-state', 'reopened.persisted-state'})
        scopes = [row['scope'] for row in commit['after.result.0']['source_unit_facets']]
        self.assertTrue(any('receipt.job' in scope for scope in scopes))
        inputs = cells(mapped('input-manifest-snapshot'))
        selected = {check for check, cell in inputs.items() for row in cell['source_unit_facets']
                    if row['source_unit_id'] == 'm1:M1-I25'}
        self.assertEqual(selected, {'after.result.3', 'after.persisted-state', 'reopened.persisted-state', 'reopened.persisted-strings'})
        self.assertNotIn('after.case.immutable-manifest', inputs)
        component = source.storage_slice(declared_registration(mapped('input-manifest-snapshot')))
        self.assertFalse(any(row.case_id in ('after.result.0', 'after.result.2') for row in component.assertions))

    def test_token_history_precise_transition_and_reopened_old_token_credit(self):
        value = mapped('cancel-retry-token-history')
        selected = {}
        for check, cell in cells(value).items():
            if '.result.' in check:
                for facet in cell['source_unit_facets']:
                    selected.setdefault(facet['source_unit_id'], set()).add(check)
        self.assertEqual(selected['m1:M1-J07'], {'after.result.0', 'after.result.3'})
        self.assertEqual(selected['m1:M1-J08'], {'after.result.1', 'after.result.4', 'after.result.7'})
        self.assertEqual(selected['m1:M1-A06'], {'after.result.10', 'after.result.11', 'after.result.12',
            'reopened.result.0', 'reopened.result.1', 'reopened.result.2'})
        for history in ('token-domain-bool', 'token-domain-string', 'token-domain-null'):
            component = source.storage_slice(declared_registration(mapped(history)))
            self.assertFalse(any(row.obligation_id == 'm1:M1-T08' for row in component.assertions))

    def test_mapped_projection_keeps_actual_false_and_missing_tail(self):
        value = mapped('fresh-value-create_job')
        expected = value.record()['original_definition']['expected']
        snapshots = observations('b02', expected)
        responses = deepcopy(expected['results'])
        responses['after'][0]['value']['total'] = 99
        del snapshots['reopened']
        del responses['reopened']
        result = profile.project(value, snapshots, responses)
        self.assertFalse(result['checks']['after.result.0'])
        self.assertIsNone(result['checks']['reopened.result.0'])
        self.assertIsNone(result['checks']['reopened.persisted-state'])
        self.assertFalse(result['execution_authenticated'])

    def test_closed_opt_in_purpose_and_profile_protocol_marker_tampering_fail(self):
        for marker in ('unknown', '', True):
            with self.assertRaises(profile.ProfileError):
                profile.profile_for('b02', 'fresh-value-create_job', 'independent_acceptance', mapping_profile=marker)
        for family, history, purpose in (('b01', 'rollback', 'independent_acceptance'),
            ('b02', 'fresh-value-create_job', 'public_release'), ('b02', 'fresh-value-create_job', 'repeatability')):
            with self.assertRaises(profile.ProfileError):
                profile.profile_for(family, history, purpose, mapping_profile=finite.STORAGE_MAPPING)
        value = mapped('fresh-value-create_job')
        record = value.record()
        record.pop('mapping_profile')
        with self.assertRaises(profile.ProfileError):
            profile.reconstruct(replace(value, _record_bytes=profile.encoded(record)))
        reg = declared_registration(value)
        with self.assertRaises(execution.ExecutionError):
            execution.profile_for_binding(replace(reg.binding, protocol=execution.BATCH_PROTOCOL))
        component = source.storage_slice(reg)
        factory_input = profile.decode(component.factory_input_json.encode())
        factory_input.pop('mapping_profile')
        with self.assertRaises(ValueError):
            source.verify_slice(replace(component, factory_input_json=profile.encoded(factory_input).decode()))


class CandidateStorageFiniteMappingCompositionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix='gossip-finite-storage-')
        cls.addClassCleanup(cls.temp.cleanup)
        cls.base = Path(cls.temp.name).resolve()
        cls.store = GitStore.create(cls.base / 'source.git', {
            'library/__init__.py': "raise RuntimeError('candidate must never import on host')\n"})
        cls.commit = cls.store.head()
        cls.tree, cls.files = execution.capture_source(cls.store, cls.commit, policy=capture.BatchCapturePolicy())

    def setUp(self):
        self.root = self.base / self.id().rsplit('.', 1)[-1]
        self.root.mkdir()

    def recipe(self, *, history='fresh-value-create_job', batch=True):
        value = mapped(history)
        # Only the fixed physical schema is needed here; abstract history rows
        # can contain symbolic job aliases rather than insertable primary keys.
        schema_files = physical_files(SQLITE_LAYOUT, {'documents': [], 'jobs': []})
        schema = observation.b01_observer.sqlite_schema_sha256(schema_files['catalog.sqlite'])
        plan = review.MappedLayoutPlan('b02', admission.source_sha256(self.files), execution.b02.source_sha256(self.files),
            self.commit, self.tree, history, value.sha256, SQLITE_LAYOUT, ('catalog.sqlite',), schema,
            purpose='independent_acceptance', mapping_profile=finite.STORAGE_MAPPING)
        authority, journal, head = enroll(self.root, plan)
        self.addCleanup(head.close)
        self.addCleanup(journal.close)
        subject = registry.Subject('fixture-cohort', COHORT[0], 'M4', 'a' * 64, execution.TARGET_CONTRACT, plan.source_sha256)
        recipe = factory.build_storage_recipe(self.store, subject, family='b02', case_id=history,
            purpose=value.purpose, policy=execution.StoragePolicy(), runtime={'kind': 'fixture-no-Docker'},
            layout_plan=plan, layout_authority=authority, gate_id='finite-fixture', repetition_id='fresh-fixture',
            cohort_trajectory_ids=COHORT, capture_policy=capture.BatchCapturePolicy() if batch else None,
            mapping_profile=finite.STORAGE_MAPPING)
        return recipe, journal

    def owner(self, recipe):
        raw, delta = self.root / 'raw', self.root / 'delta'
        head = ExternalHead.create(self.root / 'head', journal_roots=(raw, delta))
        self.addCleanup(head.close)
        spec = recipe.spec(root=raw, delta_root=delta, cleanup_root=self.root / 'cleanup', checkpoint_authority=head)
        registered = spec.observation_registration()
        # Synthetic authority fixture only; not a real terminal barrier.
        subject = registered.gate.binding.subject
        self.fixture_freeze = registry.CohortFreeze(tuple(replace(subject, trajectory_id=name)
            for name in registered.cohort_trajectory_ids), '1'*64, '2'*64, True)
        issued = admission.ObservationAdmission(registered, verify_registration=lambda: registered,
            verify_cohort=lambda: self.fixture_freeze)
        owner = factory.construct_storage_owner(spec, issued, mode='fixture')
        self.addCleanup(owner.close)
        return owner, spec

    def test_real_recipe_binding_scope_final_spec_codec_and_fixture_owner_keep_mapping(self):
        with capture_calls() as calls:
            recipe, _ = self.recipe()
            owner, spec = self.owner(recipe)
            owner.current(self.fixture_freeze)
        self.assertNotIn(('legacy', None), calls)
        self.assertEqual(recipe.registration.binding.protocol, execution.MAPPED_BATCH_PROTOCOL)
        component = recipe.scope_slice()
        self.assertEqual(profile.decode(component.factory_input_json.encode())['mapping_profile'], finite.STORAGE_MAPPING)
        typed = codec.unpack(codec.pack((spec.registration, spec.profile, spec.layout_plan, component)))
        transported = replace(spec, registration=typed[0], profile=typed[1], layout_plan=typed[2])
        self.assertIs(type(transported.layout_plan), review.MappedLayoutPlan)
        self.assertEqual(transported.observation_registration(), spec.observation_registration())
        source.verify_slice(typed[3])
        config = profile.decode(owner.read_authenticated('config.json'))
        self.assertEqual(config['profile']['mapping_profile'], finite.STORAGE_MAPPING)
        self.assertEqual(config['plan']['mapping_profile'], finite.STORAGE_MAPPING)
        # Config is JSON: tuples in the typed registration become arrays.
        # Compare its exact canonical wire shape independently of the typed
        # codec reconstruction and mapped-layout checks above.
        self.assertEqual(profile.encoded(config['registration']),
            profile.encoded(asdict(recipe.registration)))
        self.assertFalse(owner.has_retained('intent.json'))
        with self.assertRaises(execution.ExecutionError):
            owner.execute_once()
        with self.assertRaises(consumer.AuthorityError):
            observation.publish_verifier(owner)

    def test_mapped_legacy_capture_is_explicit_and_distinct_from_batch(self):
        recipe, _ = self.recipe(batch=False)
        self.assertEqual(recipe.registration.binding.protocol, execution.MAPPED_PROTOCOL)
        self.assertIsNone(recipe.record()['source_capture_policy'])
        self.assertEqual(execution.profile_for_binding(recipe.registration.binding), recipe.profile)

    def test_unpaired_surrogate_roundtrip_preserves_original_input_and_mapping(self):
        recipe, _ = self.recipe(history='intake-json-deferred-unencodable')
        self.assertIn(b'\\ud800', recipe.original_bytes)
        owner, spec = self.owner(recipe)
        transported = codec.unpack(codec.pack((spec.profile, spec.layout_plan, spec.registration)))
        self.assertEqual(transported, (spec.profile, spec.layout_plan, spec.registration))
        self.assertEqual(owner.config['profile']['mapping_profile'], finite.STORAGE_MAPPING)

    def test_legacy_layout_original_cannot_authenticate_mapped_profile(self):
        recipe, _ = self.recipe()
        fields = asdict(recipe.layout_plan)
        fields.pop('mapping_profile')
        legacy = review.LayoutPlan(**fields)
        with self.assertRaises(ValueError):
            recipe.layout_authority.authenticate(legacy)
        class Subclass(review.LayoutPlan):
            pass
        self.assertFalse(review.accepted_layout_plan(Subclass(**fields)))
        with self.assertRaises(ValueError):
            replace(recipe.layout_plan, mapping_profile='unknown')
        with self.assertRaises(ValueError):
            replace(recipe.layout_plan, purpose='public_release')

    def test_purpose_source_review_prefix_and_profile_changes_block_actual_recipe(self):
        recipe, journal = self.recipe()
        with self.assertRaises(ValueError):
            replace(recipe, profile=profile.profile_for('b02', recipe.profile.case_id, 'independent_acceptance')).revalidate()
        with self.assertRaises(ValueError):
            replace(recipe, registration=replace(recipe.registration, tree_oid='f' * 40)).revalidate()
        with self.assertRaises(ValueError):
            replace(recipe.registration.binding, purpose='public_release')
        journal.retain('revocation.json', b'{"revoked":true}')
        with self.assertRaises(ValueError):
            recipe.revalidate()

    def test_actual_mapped_owner_rejects_default_profile_substitution_at_boundary(self):
        recipe, _ = self.recipe()
        owner, _ = self.owner(recipe)
        original = owner.profile
        owner.profile = profile.profile_for('b02', recipe.profile.case_id, 'independent_acceptance')
        try:
            with self.assertRaises(execution.ExecutionError):
                owner.current(self.fixture_freeze)
        finally:
            owner.profile = original
