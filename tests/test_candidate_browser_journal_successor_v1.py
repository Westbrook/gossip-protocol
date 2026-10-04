"""Real host-journal seams and pure browser bindings; no physical owner imitation."""
from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

from gossip_harness import candidate_checkpoint_chain_v1 as chain
from gossip_harness import candidate_checkpoint_head_v1 as head
from gossip_harness import candidate_execution_journal_v1 as journal
from gossip_harness import candidate_journal_batch_read_v1 as journal_read
from gossip_harness import candidate_product_browser_execution_v1 as execution
from gossip_harness import candidate_product_browser_observation_v1 as observer
from gossip_harness import candidate_product_browser_cases_v1 as cases
from gossip_harness.library_project_fixture_v1 import RUNTIME_IMAGE


class CandidateBrowserJournalBindingV1Tests(unittest.TestCase):
    def test_legacy_capture_only_and_combined_profiles_are_distinct(self):
        case = cases.all_cases()[0]
        legacy = execution.BrowserProfile(case)
        capture = execution.BrowserProfile(case, execution.source_capture.BatchCapturePolicy())
        batch = execution.BrowserProfile(case, journal_policy=journal_read.BatchReadPolicy())
        combined = replace(capture, journal_policy=journal_read.BatchReadPolicy())
        self.assertNotIn('journal_read', legacy.record())
        self.assertNotIn('journal_read', capture.record())
        self.assertEqual(legacy.execution_protocol, execution.PROTOCOL)
        self.assertEqual(capture.execution_protocol, execution.BATCH_PROTOCOL)
        self.assertEqual(batch.execution_protocol, execution.JOURNAL_PROTOCOL)
        self.assertEqual(combined.execution_protocol, execution.COMBINED_BATCH_PROTOCOL)
        self.assertEqual(combined.record()['definition'], legacy.record()['definition'])
        self.assertEqual(combined.ordered_case_ids, legacy.ordered_case_ids)
        self.assertEqual(len({p.sha256 for p in (legacy, capture, batch, combined)}), 4)
        helper = 'gossip_harness/candidate_journal_batch_read_v1.py'
        self.assertNotIn(helper, execution.evaluator_sources())
        self.assertIn(helper, execution.evaluator_sources(batch.journal_policy))
        self.assertEqual(execution.evaluator_sources(), execution.loaded_sources(None))
        self.assertEqual(execution.evaluator_sources(batch.journal_policy), execution.loaded_sources(batch.journal_policy))

    def test_fresh_interpreter_default_imports_and_construction_do_not_load_helper(self):
        # Actual default journal/head and browser profile/binding construction.
        # A complete BrowserExecution construction queries Engine and is reserved
        # for the physical lane; this control makes no physical-owner claim.
        script = r"""
import builtins, contextlib, pathlib, sys, tempfile
sys.path.insert(0, sys.argv[1])
original_import = builtins.__import__
helper = 'candidate_journal_batch_read_v1'
def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
    if name.split('.')[-1] == helper or helper in (fromlist or ()):
        raise AssertionError('default attempted undeclared helper import')
    return original_import(name, globals, locals, fromlist, level)
builtins.__import__ = guarded_import
from gossip_harness import candidate_checkpoint_chain_v1 as chain
from gossip_harness import candidate_checkpoint_head_v1 as head
from gossip_harness import candidate_execution_journal_v1 as journal
from gossip_harness import candidate_product_browser_execution_v1 as execution
from gossip_harness.library_project_fixture_v1 import RUNTIME_IMAGE
with tempfile.TemporaryDirectory() as temporary:
    root = pathlib.Path(temporary).resolve()
    raw, delta = root/'raw', root/'delta'
    with contextlib.closing(head.ExternalHead.create(root/'head', journal_roots=(raw,delta))) as anchor:
        with journal.OwnerJournal(raw,delta,context={'purpose':'offline-default'},authority=anchor) as owner:
            owner.retain('fact.bin', b'fresh')
            checkpoint = owner.checkpoint()
            assert owner.read('fact.bin') == b'fresh' and anchor.read() == checkpoint
    profile = execution.BrowserProfile(execution.cases.all_cases()[0])
    policy = execution.BrowserPolicy(RUNTIME_IMAGE, str(pathlib.Path(sys.executable).resolve()), str(root))
    binding = execution.binding_for({'source.txt':b'source'}, profile, policy, {}, {})
    assert binding.protocol == execution.PROTOCOL and 'journal_read' not in profile.record()
    assert 'gossip_harness/'+helper+'.py' not in execution.evaluator_sources()
    assert 'gossip_harness.'+helper not in sys.modules
print('default-helper-not-imported')
"""
        result = subprocess.run([sys.executable, '-I', '-c', script, str(Path(execution.__file__).resolve().parents[1])],
                                capture_output=True, timeout=30, check=False)
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertEqual(result.stdout, b'default-helper-not-imported\n')

    def test_selected_loaded_closure_mismatch_rejects_before_source_capture(self):
        from unittest.mock import Mock
        profile = execution.BrowserProfile(cases.all_cases()[0], journal_policy=journal_read.BatchReadPolicy())
        owner = execution.BrowserExecution.__new__(execution.BrowserExecution)
        owner.profile = profile
        owner.checkpoint, owner.admission = Mock(), Mock()
        owner.actual_registration, owner.retained_freeze = object(), object()
        owner.sources = execution.evaluator_sources(profile.journal_policy)
        changed_loaded = {**execution._LOADED_SOURCES, 'gossip_harness/candidate_checkpoint_chain_v1.py':'0'*64}
        with patch.object(execution, '_LOADED_SOURCES', changed_loaded), \
             patch.object(execution, 'capture_source', side_effect=AssertionError('must reject before capture')):
            with self.assertRaisesRegex(ValueError, 'Loaded evaluator changed'):
                owner._unchanged()
        # This private guard fixture never dispatches or claims physical execution.
        changed_leaf = {**execution._LOADED_SOURCES, 'gossip_harness/candidate_http_journal_v3.py':'0'*64}
        with patch.object(execution, '_LOADED_SOURCES', changed_leaf):
            with self.assertRaisesRegex(ValueError, 'Loaded journal dependency changed'):
                execution.loaded_sources(profile.journal_policy)

    def test_missing_wrong_and_undeclared_journal_policy_rejected(self):
        profile = execution.BrowserProfile(cases.all_cases()[0], journal_policy=journal_read.BatchReadPolicy())
        fields = execution.journal_fields(profile)
        good = {'protocol': profile.execution_protocol, 'pipe_diagnostics': execution.pipe_diagnostics_policy(), **fields}
        execution.validate_capture_config(profile, good)
        for value in ({}, {'journal_read': {**fields['journal_read'], 'cross_checkpoint_cache': True}}):
            with self.subTest(value=value), self.assertRaises(ValueError):
                execution.validate_journal_fields(profile, value)
        with self.assertRaises(ValueError):
            execution.validate_journal_fields(execution.BrowserProfile(profile.case), fields)
        with self.assertRaises(ValueError):
            execution.BrowserProfile(profile.case, journal_policy={})
        with self.assertRaises(ValueError):
            execution.validate_capture_config(profile, {**good, 'protocol': execution.PROTOCOL})

    def test_registration_and_limits_bind_exact_opt_in(self):
        case = cases.all_cases()[0]
        legacy = execution.BrowserProfile(case)
        profile = execution.BrowserProfile(case, journal_policy=journal_read.BatchReadPolicy())
        policy = execution.BrowserPolicy(RUNTIME_IMAGE, '/fixture/node', '/fixture/modules')
        files = {'source.txt': b'unchanged'}
        old = execution.binding_for(files, legacy, policy, {}, {})
        binding = execution.binding_for(files, profile, policy, {}, {})
        self.assertEqual(old.source_sha256, binding.source_sha256)
        self.assertEqual(old.recipe_sha256, binding.recipe_sha256)
        self.assertNotEqual(old.evaluator_sha256, binding.evaluator_sha256)
        self.assertNotEqual(old.limits_sha256, binding.limits_sha256)
        subject = execution.registry.Subject('cohort', 'trajectory', 'M4', 'f'*64, cases.CONTRACT_SHA256, binding.source_sha256)
        args = dict(subject=subject, gate_id='browser-journal-unit', commit_oid='a'*40, tree_oid='b'*40,
                    repetition_id='unit', cohort_trajectory_ids=('trajectory', 'peer-1', 'peer-2', 'peer-3', 'peer-4', 'peer-5'))
        registered = execution.observation_registration_for(binding, profile, policy, **args)
        self.assertEqual(registered.gate.binding.execution_protocol, profile.execution_protocol)
        for value, declared in ((replace(binding, protocol=execution.PROTOCOL), profile), (binding, legacy)):
            with self.assertRaises(ValueError):
                execution.observation_registration_for(value, declared, policy, **args)


class CandidateBrowserMissingPrefixV1Tests(unittest.TestCase):
    """Private reader over an actual head/journal, never a fake physical BrowserExecution."""
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='gossip-browser-prefix-control-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.raw, self.delta = self.root/'raw', self.root/'delta'
        self.anchor = head.ExternalHead.create(self.root/'head', journal_roots=(self.raw, self.delta))
        self.addCleanup(self.anchor.close)
        self.journal = journal.OwnerJournal(self.raw, self.delta, context={'purpose': 'offline-reader-control'}, authority=self.anchor)
        self.addCleanup(self.journal.close)
        self.owner = SimpleNamespace(journal=self.journal, checkpoint=self.journal.checkpoint,
                                     read_authenticated=self.journal.read)

    def reader(self):
        return observer._Reader(self.owner, self.journal.checkpoint())

    def test_missing_member_does_not_poison_real_acknowledged_prefix(self):
        self.journal.retain('create-result.json', b'{"container_id":"authored-inert-id"}')
        reader = self.reader()
        self.assertEqual(reader.json('create-result.json'), {'container_id': 'authored-inert-id'})
        with patch.object(self.journal, 'read', wraps=self.journal.read) as consumed:
            self.owner.read_authenticated = self.journal.read
            with self.assertRaisesRegex(observer.MissingOriginalEvidence, 'created-request.bin'):
                reader.raw('created-request.bin')
            self.assertEqual(consumed.call_count, 0)
        self.assertFalse(self.journal.uncertain)
        reader.validate_checkpoint()
        # The exact public gate still refuses this private host-journal fixture.
        with self.assertRaisesRegex(ValueError, 'Exact live physical owner'):
            observer.read_original(self.owner, reader.checkpoint)

    def test_acknowledged_deleted_member_remains_fatal(self):
        self.journal.retain('captured.bin', b'complete')
        reader = self.reader()
        (self.raw/'captured.bin').unlink()
        self.assertTrue(self.journal.has('captured.bin'))
        with self.assertRaises(chain.ChainUnknown):
            reader.raw('captured.bin')
        self.assertTrue(self.journal.uncertain)
        with self.assertRaises(chain.ChainUnknown):
            reader.raw('not-retained.bin')
        with self.assertRaises(chain.ChainUnknown):
            reader.validate_checkpoint()

    def test_acknowledged_tamper_is_not_missing_evidence(self):
        self.journal.retain('captured.bin', b'original')
        reader = self.reader()
        (self.raw/'captured.bin').write_bytes(b'changed!')
        with self.assertRaises(chain.ChainUnknown):
            reader.raw('captured.bin')
        self.assertTrue(self.journal.uncertain)

    def test_foreign_suffix_is_never_adopted_and_final_boundary_rejects(self):
        self.journal.retain('known.bin', b'known')
        reader = self.reader()
        (self.raw/'foreign.bin').write_bytes(b'not acknowledged')
        with self.assertRaises(observer.MissingOriginalEvidence):
            reader.raw('foreign.bin')
        self.assertFalse(self.journal.uncertain)
        with self.assertRaises(chain.ChainUnknown):
            reader.validate_checkpoint()

    def test_append_and_wrong_expected_checkpoint_reject_final_authority(self):
        self.journal.retain('known.bin', b'known')
        reader = self.reader()
        self.journal.retain('later.bin', b'later')
        with self.assertRaisesRegex(ValueError, 'Checkpoint rollback, append or substitution'):
            reader.validate_checkpoint()
        with self.assertRaisesRegex(ValueError, 'Checkpoint rollback, append or substitution'):
            observer._Reader(self.owner, reader.checkpoint)

    def test_wrong_external_head_is_not_masked_by_missing_member(self):
        self.journal.retain('known.bin', b'known')
        reader = self.reader()
        expected = self.anchor.read()
        moved = replace(expected, head_sha256='a'*64, sequence=expected.sequence+1,
                        raw_file_count=expected.raw_file_count+1, external_bytes=expected.external_bytes+1)
        self.assertTrue(self.anchor.compare_and_set(expected, moved))
        with self.assertRaises(chain.ChainUnknown):
            reader.raw('missing.bin')
        self.assertTrue(self.journal.uncertain)

    def test_known_mutation_survives_later_real_missing_dependency(self):
        import base64
        action = next(a for a in cases.case('m1-browser-high-epoch').record['actions'] if a.get('action') == 'commit')
        body = cases.encoded({'epoch': 1})
        message = {'kind': 'request', 'source': 'browser', 'action_id': action['id'], 'request': {
            'method': 'POST', 'target': observer.expected_mutation(action)[0],
            'body_b64': base64.b64encode(body).decode(), 'body_sha256': cases.sha(body)}}
        self.journal.retain('known-message.json', cases.encoded(message))
        self.journal.retain('later-observation.json', b'{"available":false,"process":null}')
        self.journal.retain('cleanup.json', b'{"all_resources_absent":true}')
        reader = self.reader()
        known = reader.json('known-message.json')
        self.assertFalse(reader.json('later-observation.json')['available'])
        wrong, missing = observer.mutation_findings(action, [known])
        self.assertTrue(wrong)
        try:
            reader.raw('later-created-request.bin')
        except observer.MissingOriginalEvidence as error:
            later = str(error)
        else:
            self.fail('Actual unacknowledged dependency was accepted')
        facet = observer._facet('offline-composition', wrong, [*missing, later])
        self.assertEqual(facet.state, 'failed')
        self.assertTrue(facet.discrepancies)
        self.assertTrue(facet.limitations)
        self.assertEqual(reader.json('cleanup.json'), {'all_resources_absent': True})
        # Synthetic JSON is only an authenticated host fact, never physical cleanup proof.
        reader.validate_checkpoint()
        self.assertFalse(self.journal.uncertain)

    def test_missing_at_each_stage_stays_dependency_only(self):
        self.journal.retain('available.json', b'{"status":"recorded"}')
        reader = self.reader()
        for name in ('probe-create-intent.json', 'probe-created-request.bin', 'probe-start-response.bin',
                     'probe-process.json', 'probe-stdout.bin', 'browser-reply.bin'):
            with self.subTest(name=name), self.assertRaises(observer.MissingOriginalEvidence):
                reader.raw(name)
        self.assertEqual(reader.json('available.json'), {'status': 'recorded'})
        reader.validate_checkpoint()
