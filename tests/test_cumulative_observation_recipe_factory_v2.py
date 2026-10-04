"""Real Git, protected synthetic review and batch recipe/owner composition.

No source review, ScopePlan, freeze, or candidate acceptance is fabricated. All
owners are fixtures and no Docker/provider or candidate code is executed.
"""
from contextlib import contextmanager
from dataclasses import replace
import inspect
import os
from pathlib import Path
import sys
import tempfile
import unittest

from gossip_harness import cumulative_observation_recipe_factory_v2 as factory
from gossip_harness import cumulative_observation_recipe_factory_v1 as previous
from gossip_harness import cumulative_scope_source_v3 as scope_source
from gossip_harness import candidate_storage_product_execution_v1 as storage
from gossip_harness import candidate_storage_product_observation_v1 as observation
from gossip_harness import candidate_storage_review_authority_v1 as review
from gossip_harness import candidate_storage_product_profile_v1 as profile
from gossip_harness import candidate_source_capture_policy_v1 as capture_policy
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import candidate_scope_consumer_v1 as consumer
from gossip_harness import project_acceptance_registry_v1 as registry
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from gossip_harness.gitstore import ACCEPTED, GitStore
from tests.test_candidate_storage_product_execution_v1 import enroll, COHORT
from tests.test_candidate_storage_observer_v1 import physical_files, SQLITE_LAYOUT


@contextmanager
def capture_calls():
    """Profile real functions without replacing authenticated evaluator code."""
    route_code = capture_policy.capture_registered_source.__code__
    batch_code = capture_policy.batch.capture_git_source_batch.__code__
    legacy_code = capture_policy.legacy_capture.__code__
    calls = []
    prior = sys.getprofile()
    def trace(frame, event, arg):
        if event == 'call':
            if frame.f_code is route_code:
                calls.append(('route', frame.f_locals['policy']))
            elif frame.f_code is batch_code:
                calls.append(('batch', frame.f_locals['timeout_seconds']))
            elif frame.f_code is legacy_code:
                calls.append(('legacy', None))
        if prior is not None:
            prior(frame, event, arg)
    sys.setprofile(trace)
    try:
        yield calls
    finally:
        sys.setprofile(prior)


class CumulativeObservationRecipeFactoryV2Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix='gossip-storage-recipe-v2-')
        cls.addClassCleanup(cls.temp.cleanup)
        cls.base = Path(cls.temp.name).resolve()
        cls.store = GitStore.create(cls.base / 'source.git', {
            'library/__init__.py': "raise RuntimeError('candidate must never import on host')\n"})
        cls.commit = cls.store.head()
        cls.tree, cls.files = storage.capture_source(cls.store, cls.commit, policy=capture_policy.BatchCapturePolicy())

    def setUp(self):
        self.root = self.base / self.id().rsplit('.', 1)[-1]
        self.root.mkdir()

    def inputs(self, *, family='b01', case_id='rollback', purpose='public_release', store=None):
        store = self.store if store is None else store
        commit = store.head()
        tree, files = storage.capture_source(store, commit, policy=capture_policy.BatchCapturePolicy())
        value = profile.profile_for(family, case_id, purpose)
        case = next(row for row in profile.b01.definitions() if row['case_id'] == 'rollback')
        schema = observation.b01_observer.sqlite_schema_sha256(physical_files(SQLITE_LAYOUT, case['before'])['catalog.sqlite'])
        plan = review.LayoutPlan(family, admission.source_sha256(files),
            (storage.b01 if family == 'b01' else storage.b02).source_sha256(files), commit, tree,
            case_id, value.sha256, SQLITE_LAYOUT, ('catalog.sqlite',), schema,
            'forced_schedule_unavailable' if family == 'b02' and case_id in storage.b02.FORCED_CASE_IDS else 'ordinary_public_operations', purpose)
        authority, journal, head = enroll(self.root, plan)
        self.addCleanup(head.close)
        self.addCleanup(journal.close)
        subject = registry.Subject('fixture-cohort', COHORT[0], 'M4', 'a' * 64, storage.TARGET_CONTRACT, plan.source_sha256)
        kwargs = dict(family=family, case_id=case_id, purpose=purpose, policy=storage.StoragePolicy(),
            runtime={'kind': 'fixture-no-Docker'}, layout_plan=plan, layout_authority=authority,
            gate_id='storage-' + family, repetition_id='fresh-fixture', cohort_trajectory_ids=COHORT)
        return store, subject, kwargs, authority, journal

    def recipe(self, *, family='b01', case_id='rollback', purpose='public_release', store=None, batch=True):
        store, subject, kwargs, authority, journal = self.inputs(family=family, case_id=case_id, purpose=purpose, store=store)
        result = factory.build_storage_recipe(store, subject, **kwargs,
            capture_policy=capture_policy.BatchCapturePolicy() if batch else None)
        return result, authority, journal

    def owner(self, recipe, *, changed_registration=False):
        raw, delta = self.root / 'raw', self.root / 'delta'
        head = ExternalHead.create(self.root / 'head', journal_roots=(raw, delta))
        self.addCleanup(head.close)
        spec = recipe.spec(root=raw, delta_root=delta, cleanup_root=self.root / 'cleanup', checkpoint_authority=head)
        registered = spec.observation_registration()
        actual = replace(registered, repetition_id='substituted') if changed_registration else registered
        issued = admission.ObservationAdmission(registered, verify_registration=lambda: actual)
        owner = factory.construct_storage_owner(spec, issued, mode='fixture')
        self.addCleanup(owner.close)
        return owner, spec, issued

    def test_real_batch_recipe_v3_scope_and_fixture_owner_never_use_legacy_capture(self):
        with capture_calls() as calls:
            recipe, authority, _ = self.recipe()
            recipe.revalidate()
            declared = recipe.scope_slice()
            owner, spec, issued = self.owner(recipe)
            owner.current(None)
        self.assertIs(type(recipe), factory.ProspectiveStorageRecipe)
        self.assertIsInstance(recipe, previous.ProspectiveStorageRecipe)
        self.assertIs(type(declared), scope_source.ExecutableSlice)
        self.assertEqual(declared.family, 'storage-b01')
        self.assertEqual(declared.gate, recipe.registration.gate)
        self.assertEqual(declared.assertions, ())
        packet = recipe.record()
        expected_policy = capture_policy.BatchCapturePolicy().record()
        self.assertEqual(packet['protocol'], factory.PROTOCOL)
        self.assertEqual(packet['scope_factory_protocol'], scope_source.PROTOCOL)
        self.assertEqual(packet['source_capture_policy'], expected_policy)
        for name in ('project_acceptance_compiler_v1.py', 'cumulative_final_acceptance_v1.py'):
            self.assertIn('gossip_harness/' + name, packet['sources'])
        self.assertEqual(packet['layout_review'], authority.provenance(recipe.layout_plan))
        self.assertFalse(packet['whole_scope_authority'])
        self.assertFalse(packet['physical_execution_supplied'])
        self.assertIs(type(owner), storage.CandidateStorageExecution)
        self.assertIs(owner.admission, issued)
        self.assertEqual(owner.observation_registration, spec.observation_registration())
        config = profile.decode(owner.read_authenticated('config.json'))
        self.assertEqual(config['source_capture'], expected_policy)
        self.assertEqual(config['registration'], packet['registration'])
        self.assertEqual(owner.binding.protocol, storage.BATCH_PROTOCOL)
        self.assertFalse(owner.has_retained('intent.json'))
        self.assertNotIn(('legacy', None), calls)
        routes = [value for kind, value in calls if kind == 'route']
        batches = [value for kind, value in calls if kind == 'batch']
        self.assertGreaterEqual(len(routes), 7)
        self.assertEqual(len(routes), len(batches))
        self.assertTrue(all(type(value) is capture_policy.BatchCapturePolicy for value in routes))
        self.assertEqual(set(batches), {60})
        with self.assertRaises(storage.ExecutionError):
            owner.execute_once()
        with self.assertRaises(consumer.AuthorityError):
            observation.publish_verifier(owner)

    def test_unpaired_surrogate_batch_recipe_uses_actual_adapter_protocol_and_owner(self):
        recipe, authority, _ = self.recipe(family='b02', case_id='intake-json-deferred-unencodable')
        self.assertIn(b'\\ud800', recipe.original_bytes)
        recipe.revalidate()
        packet = recipe.record()
        application = {'protocol': storage.BATCH_PROTOCOL, 'decision': 'not-requested',
            'review_sha256': authority.enrollment.report_sha256, 'production_forced_schedule_qualified': False}
        self.assertEqual(packet['adapter_manifest'], admission.source_manifest(
            storage.adapter_files('b02', recipe.profile.case_id, application)))
        declared = recipe.scope_slice()
        self.assertIs(type(declared), scope_source.ExecutableSlice)
        self.assertEqual(declared.family, 'storage-b02')
        self.assertEqual(len(declared.selectors), len(recipe.profile.diagnostic_ids) + 1)
        owner, _, _ = self.owner(recipe)
        config = profile.decode(owner.read_authenticated('config.json'))
        self.assertEqual(config['registration'], packet['registration'])
        self.assertEqual(config['source_capture'], packet['source_capture_policy'])
        self.assertEqual(owner.binding.protocol, storage.BATCH_PROTOCOL)
        self.assertFalse(owner.has_retained('intent.json'))

    def test_explicit_none_preserves_legacy_owner_under_distinct_v3_recipe(self):
        recipe, _, _ = self.recipe(batch=False)
        self.assertIsNone(recipe.record()['source_capture_policy'])
        self.assertEqual(recipe.registration.binding.protocol, storage.PROTOCOL)
        self.assertIs(type(recipe.scope_slice()), scope_source.ExecutableSlice)
        owner, _, _ = self.owner(recipe)
        self.assertIsNone(owner.capture_policy)
        self.assertNotIn('source_capture', profile.decode(owner.read_authenticated('config.json')))
        self.assertEqual(recipe.record()['protocol'], factory.PROTOCOL)

    def test_caller_must_choose_closed_policy_and_callbacks_are_refused_before_capture(self):
        store, subject, kwargs, _, _ = self.inputs()
        self.assertIs(inspect.signature(factory.build_storage_recipe).parameters['capture_policy'].default, inspect.Parameter.empty)
        with capture_calls() as calls:
            with self.assertRaises(TypeError):
                factory.build_storage_recipe(store, subject, **kwargs)
            with self.assertRaisesRegex(consumer.AuthorityError, 'callbacks'):
                factory.build_storage_recipe(store, subject, **kwargs, capture_policy=lambda: None)
        self.assertEqual(calls, [])

    def test_revoked_original_review_prevents_new_recipe_spec_and_owner(self):
        recipe, _, journal = self.recipe()
        journal.retain('extra.json', b'{}')
        with self.assertRaises(admission.AdmissionError):
            recipe.revalidate()
        self.assertFalse((self.root / 'raw').exists())

    def test_changed_protected_git_head_revokes_recipe(self):
        own_store = GitStore.fork(self.store, self.root / 'mutable-source.git')
        recipe, _, _ = self.recipe(store=own_store)
        proposed = own_store.propose({'library/new.py': 'VALUE = 2\n'})
        own_store._git('update-ref', ACCEPTED, proposed, recipe.registration.commit_oid)
        with self.assertRaisesRegex(consumer.AuthorityError, 'current protected final Git head'):
            recipe.revalidate()
        self.assertFalse((self.root / 'raw').exists())

    def test_purpose_cannot_be_changed_after_original_layout_enrollment(self):
        recipe, _, _ = self.recipe()
        changed = replace(recipe, profile=profile.profile_for('b01', 'rollback', 'independent_acceptance'))
        with self.assertRaises((storage.ExecutionError, admission.AdmissionError, consumer.AuthorityError)):
            changed.revalidate()

    def test_changed_current_admission_prevents_actual_fixture_owner_creation(self):
        recipe, _, _ = self.recipe()
        with self.assertRaises(admission.AdmissionError):
            self.owner(recipe, changed_registration=True)
        self.assertFalse((self.root / 'raw').exists())

    def test_capture_policy_record_cannot_be_removed_or_relabelled(self):
        recipe, _, _ = self.recipe()
        for changed_policy in (None, {'protocol': 'caller-supplied-capture'}):
            packet = recipe.record()
            packet['source_capture_policy'] = changed_policy
            with self.subTest(changed_policy=changed_policy), self.assertRaisesRegex(consumer.AuthorityError, 'recipe changed'):
                replace(recipe, original_bytes=storage.encoded(packet)).revalidate()

    def test_batch_binding_cannot_be_downgraded_to_legacy_with_same_limits(self):
        recipe, _, _ = self.recipe()
        changed_binding = replace(recipe.registration.binding, protocol=storage.PROTOCOL)
        changed_gate = storage.gate_for(recipe.registration.gate.binding.subject, changed_binding,
            gate_id=recipe.registration.gate.gate_id)
        changed_registration = replace(recipe.registration, binding=changed_binding, gate=changed_gate)
        with self.assertRaisesRegex(consumer.AuthorityError, 'Complete recipe binding differs'):
            replace(recipe, registration=changed_registration).revalidate()

    def test_independent_purpose_still_requires_six_source_freeze(self):
        recipe, _, _ = self.recipe(purpose='independent_acceptance')
        with self.assertRaises(admission.AdmissionUnavailable):
            self.owner(recipe)
        self.assertFalse((self.root / 'raw').exists())

    def test_v1_factory_cannot_silently_adopt_successor_batch_recipe(self):
        recipe, _, _ = self.recipe()
        old_shape = previous.ProspectiveStorageRecipe(recipe.store, recipe.registration, recipe.policy,
            recipe.profile, recipe.layout_plan, recipe.layout_authority, recipe.runtime_bytes, recipe.original_bytes)
        with self.assertRaisesRegex(consumer.AuthorityError, 'Complete recipe binding differs'):
            old_shape.revalidate()
        self.assertIs(factory.storage_history, previous.storage_history)
        self.assertIs(factory.StorageHistorySummary, previous.StorageHistorySummary)

    def test_relative_cleanup_cannot_escape_protected_review_root_check(self):
        recipe,authority,journal=self.recipe()
        raw,delta=self.root/'raw',self.root/'delta'
        head=ExternalHead.create(self.root/'head',journal_roots=(raw,delta));self.addCleanup(head.close)
        spec=recipe.spec(root=raw,delta_root=delta,cleanup_root=self.root/'cleanup',checkpoint_authority=head)
        registered=spec.observation_registration()
        issued=admission.ObservationAdmission(registered,verify_registration=lambda:registered)
        original=Path.cwd()
        os.chdir(self.root)
        try:
            changed=replace(spec,cleanup_root=Path('review-raw')/'nested-cleanup')
            with self.assertRaisesRegex(consumer.AuthorityError,'Canonical absolute recipe roots'):
                owner=factory.construct_storage_owner(changed,issued,mode='fixture')
                self.addCleanup(owner.close)
        finally:os.chdir(original)
        self.assertFalse(raw.exists())
        self.assertFalse((journal.raw_root/'nested-cleanup').exists())
