"""Actual source capture through storage owner/reopen/factory; no Docker effects.

Layout-review originals are synthetic linkage controls, not semantic authority.
"""
from dataclasses import asdict, replace
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from gossip_harness import candidate_source_capture_policy_v1 as capture
from gossip_harness import candidate_storage_product_execution_v1 as execution
from gossip_harness import candidate_storage_product_observation_v1 as observer
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import cumulative_scope_source_v2 as scope
from gossip_harness import candidate_checkpoint_chain_v1 as chain
from gossip_harness.gitstore import GitStore
from tests import test_candidate_storage_product_execution_v1 as _fixtures


class CandidateStorageBatchCaptureV1Tests(unittest.TestCase):
    setUp = _fixtures.CandidateStorageProductExecutionV1Tests.setUp
    make = _fixtures.CandidateStorageProductExecutionV1Tests.make
    owner = _fixtures.CandidateStorageProductExecutionV1Tests.owner

    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temp.cleanup)
        cls.base = Path(cls.temp.name).resolve()
        files = {'library/__init__.py': "raise RuntimeError('never import candidate on host')\n"}
        files.update({f'file{i}.txt': f'data {i}' for i in range(6)})
        cls.store = GitStore.create(cls.base / 'repo.git', files)
        cls.commit = cls.store.head()
        cls.tree, cls.files = execution.capture_git_source(cls.store, cls.commit)

    def select_batch(self):
        self.capture_policy = capture.BatchCapturePolicy()
        self.binding = execution.binding_for(self.files, self.value, self.policy, {'kind': 'fixture-no-Docker'},
            self.plan, review_authority=self.review, capture_policy=self.capture_policy)
        self.gate = execution.gate_for(self.subject, self.binding, gate_id=self.gate.gate_id)
        self.registration = replace(self.registration, binding=self.binding, gate=self.gate)
        registered = execution.observation_registration(self.registration)
        self.current['registration'] = registered
        self.admission = admission.ObservationAdmission(registered, verify_registration=lambda: self.current['registration'],
            verify_cohort=lambda: self.current['freeze'])

    def counted(self, operation):
        original = subprocess.Popen
        calls = []
        def launch(argv, *args, **kwargs):
            if argv[0] == 'git':
                calls.append(tuple(argv))
            self.assertNotEqual(argv[0], 'docker', 'Offline fixture must never dispatch Docker')
            return original(argv, *args, **kwargs)
        with patch.object(subprocess, 'Popen', side_effect=launch):
            result = operation()
        return result, calls

    def intent(self, owner):
        owner._retain('intent.json', execution.encoded({'protocol': owner.binding.protocol,
            'execution_id': 'fixture-execution', 'source_sha256': self.binding.source_sha256,
            'original_binding': asdict(self.binding), 'registration': asdict(owner.observation_registration),
            'cohort_freeze': None, 'container': 'fixture-container', 'volume': 'fixture-volume',
            'ordered_phases': list(execution.b01.PHASES)}))

    def test_legacy_capture_route_is_preserved_with_explicit_prestart_successor_identity(self):
        result, registration_calls = self.counted(lambda: execution.capture_source(self.store, self.commit))
        self.assertEqual(result, (self.tree, self.files))
        self.assertEqual(len(registration_calls), 2 + 2 * len(self.files))
        owner, initial_calls = self.counted(self.owner)
        _, repeated_calls = self.counted(lambda: owner.current(None))
        self.assertEqual(len(initial_calls), 2 + 2 * len(self.files))
        self.assertEqual(len(repeated_calls), 2 + 2 * len(self.files))
        self.assertIsNone(owner.capture_policy)
        self.assertEqual(owner.binding.protocol, execution.PROTOCOL)
        self.assertTrue(owner.binding.protocol.endswith('-prestart-v2-desktop-inputs-v1'))
        self.assertEqual(owner.config['prestart_policy'], execution.prestart.definition())
        self.assertNotIn('source_capture', owner.config)
        limits = {'policy': asdict(self.policy), 'journal': asdict(execution.LIMITS),
            'chunk_bytes': execution.CHUNK_BYTES, 'capture_bytes': execution.b02.MAX_CAPTURE_BYTES,
            'original_stream_bytes': execution.b01.MAX_STREAM_BYTES,
            'cleanup': asdict(execution.cleanup.CleanupLimits()), 'prestart_policy': execution.prestart.definition()}
        self.assertEqual(self.binding.limits_sha256, execution.digest(limits))
        declaration = scope.storage_slice(self.registration)
        self.assertEqual(set(json.loads(declaration.factory_input_json)), {'registration'})
        scope.verify_slice(declaration)

    def test_batch_registration_owner_each_effect_review_final_and_reopen_read_fresh(self):
        self.select_batch()
        result, calls = self.counted(lambda: execution.capture_source(self.store, self.commit, policy=self.capture_policy))
        self.assertEqual(result, (self.tree, self.files))
        self.assertEqual(len(calls), 4)
        owner, calls = self.counted(self.owner)
        self.assertEqual(len(calls), 4)
        for phase in execution.b01.PHASES:
            for boundary in ('before', 'after'):
                with self.subTest(phase=phase, boundary=boundary):
                    _, calls = self.counted(owner._effect_boundary)
                    self.assertEqual(len(calls), 4)
        self.intent(owner)
        projected, calls = self.counted(lambda: observer.reconstruct(owner))
        self.assertEqual(len(calls), 8)  # Review entry and final original verification.
        self.assertEqual(projected['execution_protocol'], execution.BATCH_PROTOCOL)
        self.assertEqual(projected['mechanics']['status'], 'infrastructure_error')
        with self.assertRaises(observer.AuthorityError):
            observer.StorageObservationSource(owner, owner.checkpoint())
        _, calls = self.counted(lambda: owner.current(None))
        self.assertEqual(len(calls), 4)
        expected = owner.checkpoint()
        owner.close()
        reopened, calls = self.counted(lambda: self.owner(expected=expected))
        self.assertEqual(len(calls), 4)
        self.assertEqual(reopened.checkpoint(), expected)
        self.assertEqual(reopened.config['source_capture'], self.capture_policy.record())
        with self.assertRaises(execution.ExecutionError):
            reopened.execute_once()  # Fixture roots cannot become physical evidence.

    def test_batch_protocol_limits_sources_config_and_scope_request_bind_together(self):
        legacy_binding = self.binding
        self.select_batch()
        self.assertNotEqual(legacy_binding.limits_sha256, self.binding.limits_sha256)
        self.assertEqual(self.binding.protocol, execution.BATCH_PROTOCOL)
        self.assertEqual(self.gate.binding.execution_protocol, execution.BATCH_PROTOCOL)
        owner = self.owner()
        record = self.capture_policy.record()
        self.assertEqual(record['timeout_seconds'], 60)
        self.assertEqual(record['cleanup_reap_seconds'], 5)
        self.assertFalse(record['cross_boundary_cache'])
        for name, fingerprint in record['sources'].items():
            self.assertEqual(owner.sources[name], fingerprint)
        config = json.loads(owner.read_authenticated('config.json'))
        self.assertEqual(config['source_capture'], record)
        self.assertEqual(config['protocol'], self.binding.protocol)
        declaration = scope.storage_slice(self.registration)
        scope.verify_slice(declaration)
        proposal = json.loads(declaration.factory_input_json)
        self.assertEqual(proposal['source_capture'], record)
        _, compiled, _ = declaration.compiler_records(suite_id='batch-suite', physical_slot='batch-slot')
        self.assertEqual(compiled.execution_protocol, self.binding.protocol)
        for mutation in ('missing', 'deadline'):
            changed = json.loads(declaration.factory_input_json)
            if mutation == 'missing':
                del changed['source_capture']
            else:
                changed['source_capture']['timeout_seconds'] = 120
            with self.assertRaises(scope.ScopeSourceError):
                scope.verify_slice(replace(declaration, factory_input_json=scope.encoded(changed).decode()))

    def test_mixed_legacy_binding_with_batch_protocol_is_rejected_before_journal(self):
        changed_binding = replace(self.binding, protocol=execution.BATCH_PROTOCOL)
        changed_gate = execution.gate_for(self.subject, changed_binding, gate_id=self.gate.gate_id)
        changed = replace(self.registration, binding=changed_binding, gate=changed_gate)
        with self.assertRaises(execution.ExecutionError):
            self.owner(registration=changed)
        self.assertFalse(self.raw.exists())

    def test_changed_policy_is_rejected_before_current_capture(self):
        self.select_batch()
        owner = self.owner()
        owner.capture_policy = None
        with self.assertRaisesRegex(execution.ExecutionError, 'policy changed'):
            owner.current(None)

    def test_missing_object_after_initial_capture_is_unavailable_not_cached_success(self):
        self.select_batch()
        owner = self.owner()
        blob = self.store._git('rev-parse', self.commit + ':file0.txt')
        path = self.store.path / 'objects' / blob[:2] / blob[2:]
        raw = path.read_bytes()
        path.unlink()
        try:
            with self.assertRaises(capture.SourceCaptureUnavailable):
                owner.current(None)
        finally:
            path.write_bytes(raw)

    def test_policy_is_closed_and_cannot_weaken_deadline_or_cleanup(self):
        for timeout, cleanup in ((120, 5), (60, 10), (True, 5), (60, True)):
            with self.subTest(timeout=timeout, cleanup=cleanup), self.assertRaises(ValueError):
                capture.BatchCapturePolicy(timeout, cleanup)
        called = []
        class Foreign:
            def record(self):
                called.append(True)
                return {}
        for operation in (
            lambda: execution.capture_source(self.store, self.commit, policy=Foreign()),
            lambda: execution.binding_for(self.files, self.value, self.policy, {'kind': 'fixture-no-Docker'},
                self.plan, review_authority=self.review, capture_policy=Foreign()),
        ):
            with self.assertRaises(ValueError):
                operation()
        self.assertEqual(called, [])
        selected = capture.BatchCapturePolicy()
        for name, value in (('CLEANUP_REAP_SECONDS', 120.0), ('MAX_FILE_BYTES', 4194304),
                            ('DEADLINE_CONTRACT', 'per-command'), ('MAX_FILES', 511.0)):
            with self.subTest(name=name), patch.object(capture.batch, name, value), \
                 patch.object(subprocess, 'Popen') as launch:
                with self.assertRaises(capture.SourceCaptureUnavailable):
                    execution.capture_source(self.store, self.commit, policy=selected)
                launch.assert_not_called()

    def test_both_dispatch_families_stage_exact_trusted_helpers_before_any_engine_effect(self):
        class StoppedBeforeEngine(RuntimeError):
            pass
        for family, case_id in (('b01', 'rollback'), ('b02', execution.b02.cases.CASE_IDS[0])):
            with self.subTest(family=family):
                self.root = self.root / family
                self.root.mkdir()
                self.make(family=family, case_id=case_id)
                self.select_batch()
                owner = self.owner()
                self.intent(owner)
                intent = json.loads(owner.read_authenticated('intent.json'))
                created = []
                validator_type = execution.DockerValidator
                def validator(image, tests, **kwargs):
                    # Exercise the real constructor; an empty dict fails here.
                    result = validator_type(image, tests, **kwargs)
                    created.append(result)
                    return result
                def stop_cleanup(root, **kwargs):
                    workspace, checks, inputs = kwargs['candidate_mount_roots']
                    application = json.loads(owner.read_authenticated('applicability.json'))
                    expected = execution.adapter_files(family, case_id, application)
                    self.assertEqual({name: text.encode() for name, text in created[0].tests.items()}, expected)
                    self.assertEqual({p.name: p.read_bytes() for p in checks.iterdir()}, expected)
                    execution._verify_regular_tree(workspace, self.files)
                    self.assertTrue(inputs.is_dir())
                    raise StoppedBeforeEngine()
                with patch.object(execution, 'DockerValidator', side_effect=validator), \
                     patch.object(execution.cleanup.CleanupChannel, 'create', side_effect=stop_cleanup), \
                     patch.object(execution.process, 'runtime_identity') as runtime, \
                     patch.object(execution._Commands, 'run') as commands:
                    with self.assertRaises(StoppedBeforeEngine):
                        owner._dispatch(intent)
                    runtime.assert_not_called()
                    commands.assert_not_called()
                self.assertEqual(len(created), 1)
                self.assertTrue(owner.has_retained('staging.json'))

    def test_authenticated_config_capture_record_cannot_be_rewritten_on_reopen(self):
        self.select_batch()
        owner = self.owner()
        expected = owner.checkpoint()
        config = json.loads(owner.read_authenticated('config.json'))
        owner.close()
        config['source_capture']['timeout_seconds'] = 120
        (self.raw / 'config.json').write_bytes(execution.encoded(config))
        with self.assertRaises(chain.ChainError):
            self.owner(expected=expected)


if __name__ == '__main__':
    unittest.main()
