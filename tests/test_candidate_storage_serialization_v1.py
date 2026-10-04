"""Actual local originals and Git, synthetic layout review and raw response only.

This tests surrogate-safe protocol mechanics; it executes no candidate or Engine.
"""
from dataclasses import asdict, replace
import json
from pathlib import Path
import tempfile
import unittest

from gossip_harness import candidate_storage_product_profile_v1 as profile
from gossip_harness import candidate_storage_product_execution_v1 as execution
from gossip_harness import candidate_storage_product_observation_v1 as observer
from gossip_harness import candidate_storage_review_authority_v1 as review
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import project_acceptance_registry_v1 as registry
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from gossip_harness.gitstore import GitStore
from tests.test_candidate_storage_product_execution_v1 import enroll, COHORT, SQLITE_LAYOUT
from tests import test_candidate_storage_product_execution_v1 as fixtures


class CandidateStorageSerializationV1Tests(unittest.TestCase):
    def test_explicit_ascii_contract_preserves_unencodable_and_unicode_values(self):
        value = {'input': '\ud800', 'valid': '\u00e9', 'nested': ['\udfff', '\U0001f4e6']}
        raw = execution.encoded(value)
        self.assertTrue(raw.isascii())
        self.assertEqual(json.loads(raw), value)
        self.assertEqual(profile.decode(raw), value)
        self.assertEqual(raw, profile.encoded(value))
        self.assertEqual(raw, review.encoded(value))
        with self.assertRaises(UnicodeEncodeError):
            admission.encoded(value)  # Frozen generic protocol is deliberately unchanged.
        self.assertIn('ascii-json-v1', execution.PROTOCOL)
        self.assertIn('ascii-json-v1', observer.PROTOCOL)
        self.assertIn('ascii-json-v1', review.PROTOCOL)
        for raw in (b'{"x":1,"x":2}', b'{"n":NaN}', b'{"n":1e999}', b'\xef\xbb\xbf{}', b'[' * 65 + b'0' + b']' * 65):
            with self.subTest(raw=raw), self.assertRaises(profile.ProfileError):
                profile.decode(raw)

    def test_real_review_gate_fixture_owner_and_raw_reader_keep_authored_surrogate(self):
        with tempfile.TemporaryDirectory() as path:
            root = Path(path).resolve()
            store = GitStore.create(root / 'repo.git', {'library/__init__.py': '# never imported or executed\n'})
            commit = store.head(); tree, files = execution.capture_git_source(store, commit)
            value = profile.profile_for('b02', 'intake-json-deferred-unencodable', 'public_release')
            original = value.record()
            self.assertIn('\\ud800', profile.encoded(original).decode())
            self.assertEqual(original['original_definition'], profile.b02.case_definition(value.case_id))
            self.assertEqual(original['canonical_json_protocol'], profile.JSON_ENCODING_PROTOCOL)
            plan = review.LayoutPlan('b02', admission.source_sha256(files), execution.b02.source_sha256(files),
                commit, tree, value.case_id, value.sha256, SQLITE_LAYOUT, ('catalog.sqlite',), 'f' * 64)
            authority, review_journal, review_head = enroll(root, plan)
            self.addCleanup(review_head.close); self.addCleanup(review_journal.close)
            policy = execution.StoragePolicy()
            binding = execution.binding_for(files, value, policy, {'kind': 'fixture-no-Docker'}, plan,
                review_authority=authority)
            subject = registry.Subject('cohort', COHORT[0], 'M4', 'a' * 64, execution.TARGET_CONTRACT, binding.source_sha256)
            gate = execution.gate_for(subject, binding, gate_id='surrogate-storage')
            registration = execution.StorageRegistration(binding, commit, tree, 'fresh-1', gate, COHORT)
            actual = execution.observation_registration(registration)
            admitted = admission.ObservationAdmission(actual, verify_registration=lambda: actual, verify_cohort=lambda: None)
            raw, delta = root / 'raw', root / 'delta'
            head = ExternalHead.create(root / 'head', journal_roots=(raw, delta)); self.addCleanup(head.close)
            def open_owner(expected=None):
                owner = execution.CandidateStorageExecution(raw, store, registration, policy, value=value, plan=plan,
                    review_authority=authority, admission_authority=admitted, checkpoint_authority=head,
                    delta_root=delta, cleanup_root=root / 'cleanup', mode='fixture', expected_checkpoint=expected)
                self.addCleanup(owner.close)
                return owner
            owner = open_owner()
            self.assertEqual(json.loads(owner.read_authenticated('config.json'))['profile'], original)
            self.assertTrue(owner.read_authenticated('config.json').isascii())
            owner._retain('intent.json', execution.encoded({'protocol': execution.PROTOCOL, 'execution_id': 'fixture-execution',
                'source_sha256': binding.source_sha256, 'original_binding': asdict(binding), 'registration': asdict(actual),
                'cohort_freeze': None, 'container': 'fixture-container', 'volume': 'fixture-volume',
                'ordered_phases': list(execution.b01.PHASES)}))
            # Keep this surrogate regression on authenticated fixture data after
            # the reader's new created-before-start requirement. No Engine runs.
            def command(fixture_owner, label, argv, raw):
                fixtures.CandidateStorageProductExecutionV1Tests.command(self, fixture_owner, label, argv, raw)
            fixtures.synthetic_created_origin(owner, command, module=execution, family='b02')
            recipe = execution.b02.validate_recipe(execution.b02.cases.execution_recipe(value.case_id))
            application = {'protocol': execution.PROTOCOL, 'decision': 'not-requested', 'review_sha256': owner.review_sha256,
                'production_forced_schedule_qualified': False}
            proof = {'source_manifest': admission.source_manifest(files),
                'helper_manifest': admission.source_manifest(execution.adapter_files('b02', value.case_id, application)),
                'fixtures_sha256': execution.digest(recipe['fixtures'])}
            for boundary in ('before', 'after'):
                owner._retain('after-staging-' + boundary + '.json', execution.encoded(proof))
                owner._retain('after-runtime-' + boundary + '-verified.json', execution.encoded(
                    {'runtime': owner.runtime, 'runtime_sha256': execution.digest(owner.runtime)}))
            owner._retain('after-request.json', execution.encoded({'phase': 'after', 'request': 'after\n'}))
            owner._retain('after-response.json', execution.encoded({'phase': 'after', 'value': [{'error': 'wrong-\ud800'}]}) + b'\n')
            result = observer.reconstruct(owner)
            self.assertTrue(result['mechanics']['prestart_verified'])
            self.assertIs(result['projection']['checks']['after.result.0'], False)
            self.assertEqual(result['profile'], original)
            self.assertEqual(result['mechanics']['status'], 'infrastructure_error')
            verifier = execution.encoded(result)
            owner._retain('fixture-reader-output.json', verifier)
            self.assertEqual(json.loads(owner.read_authenticated('fixture-reader-output.json')), json.loads(verifier))
            checkpoint = owner.checkpoint(); owner.close()
            reopened = open_owner(checkpoint)
            self.assertEqual(execution.encoded(observer.reconstruct(reopened)), verifier)
            with self.assertRaises(execution.ExecutionError):
                reopened.execute_once()
            with self.assertRaises(observer.AuthorityError):
                observer.publish_verifier(reopened)
            with self.assertRaises(observer.AuthorityError):
                observer.StorageObservationSource(reopened, reopened.checkpoint())
            with self.assertRaises(execution.ExecutionError):
                replace(binding, protocol='candidate-storage-product-execution-v1')
