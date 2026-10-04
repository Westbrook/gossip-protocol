"""Prospective product-owner capture controls; no candidate/Docker dispatch.

Real Git and durable anchors exercise the route. Synthetic admission callbacks
provide fixture registration only, never independent semantic review authority.
"""
from dataclasses import asdict, replace
import json
import subprocess
import time
import unittest
from unittest.mock import patch

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness import candidate_checkpoint_head_v1 as head
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import candidate_product_process_core_v1 as core
from gossip_harness import candidate_product_process_execution_v1 as execution
from gossip_harness import candidate_product_process_observation_v1 as observer
from gossip_harness import candidate_product_process_reader_v1 as reader
from gossip_harness import candidate_source_capture_policy_v1 as capture
from gossip_harness import cumulative_scope_source_v1 as scope
from gossip_harness import cumulative_scope_source_v2 as scope_v2
from gossip_harness import cumulative_scope_source_v3 as scope_v3
from gossip_harness import project_acceptance_registry_v1 as registry
from gossip_harness.gitstore import GitStore


class CandidateProductProcessBatchCaptureV1Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.artifacts = ArtifactDirectory('product-process-batch-capture-v1', retain_success=True)
        cls.addClassCleanup(cls.artifacts.close)
        cls.base = cls.artifacts.root.resolve()
        files = {'library/__init__.py': "raise RuntimeError('candidate must never import on host')\n"}
        files.update({f'bytes-{i}.txt': f'content {i}\n' for i in range(6)})
        cls.store = GitStore.create(cls.base / 'source.git', files)
        cls.commit = cls.store.head()
        cls.tree, cls.files = execution.capture_git_source(cls.store, cls.commit)

    def setUp(self):
        self.root = self.base / self._testMethodName
        self.delta = self.root.with_name(self.root.name + '-delta')
        self.cleanup = self.root.with_name(self.root.name + '-cleanup')
        self.anchor = head.ExternalHead.create(self.root.with_name(self.root.name + '-head'),
                                              journal_roots=(self.root, self.delta))
        self.addCleanup(self.anchor.close)
        self.profile = execution.HttpProductProfile(core.definitions()[0])
        self.recipe = execution.recipe_from_case(self.profile.case)
        self.policy = execution.HttpPolicy('sha256:' + 'a' * 64, lifetime_seconds=1800)
        self.cohort = tuple('trajectory-' + str(i) for i in range(6))
        self.rebind()

    def rebind(self):
        self.binding = execution.binding_for(self.files, self.recipe, self.policy,
            {'kind': 'fixture-no-Docker'}, requirements_sha256=core.CONTRACT_SHA256,
            profile=self.profile, purpose='public_release')
        subject = registry.Subject('capture-fixture', self.cohort[0], self.profile.milestone,
            'c' * 64, core.CONTRACT_SHA256, execution.source_sha256(self.files))
        self.prospective = execution.observation_registration_for(self.binding, self.profile, self.policy,
            subject=subject, gate_id='whole-product-history', commit_oid=self.commit, tree_oid=self.tree,
            repetition_id='capture-fixture', cohort_trajectory_ids=self.cohort)
        self.registration = execution.HttpRegistration(self.binding, self.commit, self.tree,
            'capture-fixture', self.prospective)
        self.admission = admission.ObservationAdmission(self.prospective,
            verify_registration=lambda: self.prospective, verify_cohort=lambda: None)

    def select_batch(self):
        self.profile = replace(self.profile, capture_policy=capture.BatchCapturePolicy())
        self.rebind()

    def owner(self, **kwargs):
        result = execution.CandidateHttpExecution(self.root, self.store, self.registration,
            self.recipe, self.policy, profile=self.profile, observation_admission=self.admission,
            checkpoint_authority=self.anchor, delta_root=self.delta, cleanup_root=self.cleanup,
            mode='fixture', **kwargs)
        self.addCleanup(result.close)
        return result

    def counted(self, operation):
        original, calls = subprocess.Popen, []
        def launch(argv, *args, **kwargs):
            self.assertNotEqual(argv[0], 'docker', 'Host fixture cannot dispatch Docker')
            if argv[0] == 'git':
                calls.append(tuple(argv))
            return original(argv, *args, **kwargs)
        with patch.object(subprocess, 'Popen', side_effect=launch):
            result = operation()
        return result, calls

    def test_legacy_default_preserves_capture_route_and_record_shapes(self):
        value, calls = self.counted(lambda: execution.capture_source(self.store, self.commit))
        self.assertEqual(value, (self.tree, self.files))
        self.assertEqual(len(calls), 2 + 2 * len(self.files))
        owner, calls = self.counted(self.owner)
        self.assertEqual(len(calls), 2 + 2 * len(self.files))
        _, calls = self.counted(owner._unchanged)
        self.assertEqual(len(calls), 2 + 2 * len(self.files))
        self.assertEqual(self.binding.protocol, execution.PROTOCOL)
        self.assertNotIn('source_capture', self.profile.record())
        self.assertNotIn('source_capture', owner.config)
        self.assertEqual(self.binding.limits_sha256,
            execution.digest({'policy': asdict(self.policy), 'quota': execution.quota_policy()}))

    def test_batch_registration_owner_every_boundary_and_reopen_capture_fresh(self):
        self.select_batch()
        value, calls = self.counted(lambda: execution.capture_source(self.store, self.commit,
                                                                    policy=self.profile.capture_policy))
        self.assertEqual(value, (self.tree, self.files))
        self.assertEqual(len(calls), 4)
        owner, calls = self.counted(self.owner)
        self.assertEqual(len(calls), 4)
        for _ in range(2):
            _, calls = self.counted(owner._unchanged)
            self.assertEqual(len(calls), 4)
        expected = owner.checkpoint()
        owner.close()
        reopened, calls = self.counted(lambda: self.owner(expected_checkpoint=expected))
        self.assertEqual(len(calls), 4)
        self.assertEqual(reopened.checkpoint(), expected)
        self.assertEqual(reopened.config['source_capture'], self.profile.capture_policy.record())

    def test_batch_profile_limits_protocol_and_helper_identity_are_prospective(self):
        original, ordered, recipe = self.binding, self.profile.ordered_case_ids, self.recipe.record()
        self.select_batch()
        self.assertEqual(self.binding.protocol, execution.BATCH_PROTOCOL)
        self.assertNotEqual(self.binding.profile_sha256, original.profile_sha256)
        self.assertNotEqual(self.binding.limits_sha256, original.limits_sha256)
        self.assertEqual(self.binding.source_sha256, original.source_sha256)
        self.assertEqual(self.profile.ordered_case_ids, ordered)
        self.assertEqual(self.recipe.record(), recipe)
        record = self.profile.capture_policy.record()
        self.assertEqual((record['timeout_seconds'], record['cleanup_reap_seconds']), (60, 5))
        self.assertFalse(record['cross_boundary_cache'])
        for name, value in record['sources'].items():
            self.assertEqual(execution.evaluator_sources()[name], value)
        self.assertEqual(self.binding.limits_sha256, execution.digest({
            'policy': asdict(self.policy), 'quota': execution.quota_policy(), 'source_capture': record}))

    def test_registration_protocol_mismatch_fails_before_any_capture_or_intent(self):
        self.profile = replace(self.profile, capture_policy=capture.BatchCapturePolicy())
        with patch.object(execution, 'capture_source', side_effect=AssertionError('must reject before capture')):
            with self.assertRaisesRegex(execution.ExecutionError, 'protocol differs'):
                self.owner()
        self.assertFalse(self.root.exists())

    def test_existing_legacy_journal_cannot_be_reopened_as_batch(self):
        original = self.owner()
        expected = original.checkpoint()
        original.close()
        self.select_batch()
        with self.assertRaises((ValueError, RuntimeError)):
            self.owner(expected_checkpoint=expected)
        self.assertFalse((self.root / 'intent.json').exists())

    def test_unknown_policy_and_numerically_equal_mutation_are_rejected(self):
        with self.assertRaises(execution.ExecutionError):
            replace(self.profile, capture_policy=object())
        for protocol in ('', execution.BATCH_PROTOCOL + '-other'):
            with self.assertRaises(execution.ExecutionError):
                execution.capture_policy_for(protocol)
        selected = capture.BatchCapturePolicy()
        object.__setattr__(selected, 'timeout_seconds', 60.0)
        with self.assertRaises(ValueError):
            replace(self.profile, capture_policy=selected)

    def test_source_capture_unknown_uses_existing_failure_lane_without_fallback(self):
        with patch.object(capture, 'capture_registered_source', side_effect=capture.SourceCaptureUnavailable('late')), \
             patch.object(execution, 'capture_git_source', side_effect=AssertionError('fallback forbidden')):
            with self.assertRaises(execution.ExecutionUnknown) as found:
                execution.capture_source(self.store, self.commit, policy=capture.BatchCapturePolicy())
        self.assertIsInstance(found.exception, ValueError)
        self.assertIsInstance(found.exception.__cause__, capture.SourceCaptureUnavailable)

    def test_capture_interruption_is_never_relabelled_or_retried(self):
        with patch.object(capture, 'capture_registered_source', side_effect=KeyboardInterrupt) as called:
            with self.assertRaises(KeyboardInterrupt):
                execution.capture_source(self.store, self.commit, policy=capture.BatchCapturePolicy())
        self.assertEqual(called.call_count, 1)

    def test_changed_original_git_blob_fails_fresh_boundary(self):
        self.select_batch()
        owner = self.owner()
        with patch.object(capture, 'capture_registered_source', return_value=(self.tree, {'foreign.txt': b'changed'})):
            with self.assertRaisesRegex(execution.ExecutionError, 'immutable Git source changed'):
                owner._unchanged()
        self.assertFalse((self.root / 'intent.json').exists())

    def test_scope_factory_roundtrip_preserves_batch_policy_for_all_three_versions(self):
        self.select_batch()
        for module in (scope, scope_v2, scope_v3):
            with self.subTest(module=module.__name__):
                value = module.product_process_slice(self.registration, self.profile, self.policy)
                module.verify_slice(value)
                self.assertEqual(value.gate.binding.execution_protocol, execution.BATCH_PROTOCOL)
                self.assertEqual(value.profile_sha256, self.profile.sha256)
                declared = json.loads(value.factory_input_json)
                self.assertEqual(declared['registration']['binding']['protocol'], execution.BATCH_PROTOCOL)

    def test_scope_factory_rejects_mismatched_capture_profile(self):
        original = self.profile
        self.select_batch()
        with self.assertRaises((ValueError, RuntimeError)):
            scope.product_process_slice(self.registration, original, self.policy)

    def test_selector_catalog_versions_capture_without_claiming_new_semantics(self):
        before = observer.selector_catalog(self.profile.case.row_id, purpose='public_release')
        self.select_batch()
        after = observer.selector_catalog(self.profile.case.row_id, purpose='public_release',
                                          capture_policy=self.profile.capture_policy)
        self.assertEqual(after['selectors'], before['selectors'])
        self.assertEqual(after['ordered_case_ids'], before['ordered_case_ids'])
        self.assertEqual(after['definition_sha256'], before['definition_sha256'])
        self.assertEqual(after['protocol'], observer.PROTOCOL + '-git-source-batch-v1')
        self.assertEqual(after['profile_sha256'], self.profile.sha256)
        self.assertEqual(after['execution_protocol'], execution.BATCH_PROTOCOL)
        self.assertFalse(after['scope_review_supplied'] or after['acceptance_authority'])

    def test_batch_keeps_finite_window_lifetime_and_cleanup_reserve(self):
        self.select_batch()
        owner = self.owner()
        self.assertEqual(self.policy.lifetime_seconds, 1800)
        self.assertEqual(execution.CLEANUP_SECONDS, 300)
        self.assertEqual(self.policy.cli_timeout_seconds + 10 * self.policy.transport_timeout_seconds + 15, 195)
        owner._work_deadline = time.monotonic() + 194
        with self.assertRaisesRegex(execution.ExecutionError, 'Insufficient history window'):
            owner._finite_window()
        owner._work_deadline = time.monotonic() + 196
        owner._finite_window()

    def test_fixture_unknown_intent_never_becomes_physical_or_replays(self):
        self.select_batch()
        owner = self.owner()
        with patch.object(owner, '_dispatch', side_effect=AssertionError('fixture dispatch forbidden')):
            with self.assertRaises(execution.ExecutionError):
                owner.execute_once()
            with self.assertRaises(execution.ExecutionUnknown):
                owner.execute_once()
        with self.assertRaises((ValueError, RuntimeError)):
            reader.observe_execution(owner, owner.checkpoint())
