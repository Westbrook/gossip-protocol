"""Prospective M2 linkage and action-stream controls; no Docker/candidate runs.

Reviewer originals are synthetic fixtures that demonstrate linkage only. The
fixture owner is blocked from every dispatch route and supplies no acceptance.
"""
from dataclasses import asdict, replace
import io
from pathlib import Path
import queue
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from gossip_harness import candidate_m2_product_execution_v1 as execution
from gossip_harness import candidate_m2_product_profile_v1 as profile
from gossip_harness import candidate_m2_review_authority_v1 as review
from gossip_harness import candidate_checkpoint_chain_v1 as chain
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import candidate_storage_observer_v1 as storage_observer
from gossip_harness import project_acceptance_registry_v1 as registry
from gossip_harness.gitstore import GitStore
from tests.test_candidate_storage_product_execution_v1 import synthetic_created_origin

COHORT = tuple('trajectory-' + str(i) for i in range(6))


def enroll(root, plan, *, changes=None, delivery_first=False):
    """Synthetic originals, never a real candidate or semantic approval."""
    raw_root, delta_root = root / 'review-raw', root / 'review-deltas'
    head = ExternalHead.create(root / 'review-head', journal_roots=(raw_root, delta_root))
    journal = chain.CheckpointChain.create(raw_root, delta_root,
        context={'fixture': 'M2 linkage only, not source approval'}, authority=head)
    request = review.encoded(plan.request())
    report = {'protocol': review.PROTOCOL, 'purpose': review.PURPOSE, 'reviewer_id': 'fixture-reviewer',
        'request_sha256': execution.sha(request), 'decisions': [{'id': duty, 'decision': 'approved',
            'rationale': 'Synthetic linkage control; never actual source approval.',
            'source_references': ['fixture-only']} for duty in review.DUTIES],
        'remaining_obligations': ['All actual source and semantic review remains unqualified.']}
    report.update(changes or {})
    report_raw = review.encoded(report)
    delivery = review.encoded({'protocol': review.PROTOCOL, 'purpose': review.PURPOSE,
        'reviewer_id': 'fixture-reviewer', 'request_sha256': execution.sha(request),
        'report_sha256': execution.sha(report_raw), 'origin': 'independently_delivered_host_review'})
    journal.retain('request.json', request)
    for name, raw in ([('delivery.json', delivery), ('report.json', report_raw)] if delivery_first else
                      [('report.json', report_raw), ('delivery.json', delivery)]):
        journal.retain(name, raw)
    enrollment = review.ReviewEnrollment('fixture-reviewer', 'request.json', execution.sha(request),
        'report.json', execution.sha(report_raw), 'delivery.json', execution.sha(delivery))
    return review.M2ReviewAuthority(journal, journal.commitment, enrollment), journal, head


class CandidateM2ProductExecutionV1Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temp.cleanup)
        cls.base = Path(cls.temp.name).resolve()
        cls.store = GitStore.create(cls.base / 'repo.git',
            {'library/__init__.py': "raise RuntimeError('never import candidate on host')\n"})
        cls.commit = cls.store.head()
        cls.tree, cls.files = execution.capture_git_source(cls.store, cls.commit)

    def setUp(self):
        self.root = self.base / self.id().rsplit('.', 1)[-1]
        self.root.mkdir()
        self.make()

    def make(self, case_id='m2-query-generation-pages', purpose='public_release', schema_sha256=None):
        self.value = profile.profile_for(case_id, purpose)
        schema = schema_sha256 or storage_observer.sqlite_schema_sha256(profile.fixture_files(case_id)['seed.sqlite'])
        self.plan = review.LayoutPlan(execution.FAMILY, admission.source_sha256(self.files),
            profile.source_sha256(self.files), self.commit, self.tree, case_id, self.value.sha256,
            'reviewed-m2-final-sqlite-v1', ('m2/library.sqlite',), schema,
            'ordinary_public_operations', purpose)
        self.review, self.review_journal, self.review_head = enroll(self.root, self.plan)
        self.addCleanup(self.review_head.close)
        self.addCleanup(self.review_journal.close)
        self.policy = execution.M2Policy()
        self.binding = execution.binding_for(self.files, self.value, self.policy,
            {'kind': 'fixture-no-Docker'}, self.plan, review_authority=self.review)
        self.subject = registry.Subject('cohort', COHORT[0], 'M4', 'a' * 64,
            execution.TARGET_CONTRACT, self.binding.source_sha256)
        self.gate = execution.gate_for(self.subject, self.binding, gate_id='m2-slice')
        self.registration = execution.M2Registration(self.binding, self.commit, self.tree, 'fresh-1', self.gate, COHORT)
        registered = execution.observation_registration(self.registration)
        self.current = {'registration': registered, 'freeze': None}
        self.admission = admission.ObservationAdmission(registered,
            verify_registration=lambda: self.current['registration'], verify_cohort=lambda: self.current['freeze'])
        self.raw, self.delta = self.root / 'raw', self.root / 'delta'
        self.head = ExternalHead.create(self.root / 'head', journal_roots=(self.raw, self.delta))
        self.addCleanup(self.head.close)

    def owner(self, *, expected=None, registration=None):
        owner = execution.CandidateM2Execution(self.raw, self.store, registration or self.registration, self.policy,
            value=self.value, plan=self.plan, review_authority=self.review, admission_authority=self.admission,
            checkpoint_authority=self.head, delta_root=self.delta, cleanup_root=self.root / 'cleanup',
            mode='fixture', expected_checkpoint=expected)
        self.addCleanup(owner.close)
        return owner

    def intent(self, owner):
        owner._retain('intent.json', execution.encoded({'protocol': execution.PROTOCOL,
            'execution_id': 'fixture-execution', 'source_sha256': self.binding.source_sha256,
            'original_binding': asdict(self.binding), 'registration': asdict(owner.observation_registration),
            'cohort_freeze': None, 'container': 'fixture-container', 'volume': 'fixture-volume',
            'snapshot_protocol': execution.b01.SNAPSHOT_PROTOCOL, 'ordered_phases': list(self.value.phases)}))

    def synthetic_prestart_prefix(self, owner, *, omit_proof=False, proof_after_start=False, wrong_volume_options=False):
        """Synthetic originals only; actual observation tests supply command retention."""
        return synthetic_created_origin(owner, self.command, module=execution, family='m2',
            omit_proof=omit_proof, proof_after_start=proof_after_start, wrong_volume_options=wrong_volume_options)

    def test_m2_identity_and_native_purpose_preserved_in_exact_original_registration(self):
        owner = self.owner()
        self.assertEqual(owner.binding.family, 'm2-direct-api')
        self.assertEqual(owner.binding.source_sha256, admission.source_sha256(self.files))
        self.assertEqual(owner.binding.native_source_sha256, profile.source_sha256(self.files))
        self.assertNotEqual(owner.binding.source_sha256, owner.binding.native_source_sha256)
        self.assertEqual(owner.observation_registration.original_definition_purpose, 'independent_acceptance')
        self.assertEqual(self.value.record()['previous_execution_purpose'], 'authored_reference_qualification')
        self.assertEqual(self.gate.ordered_case_ids,
            self.value.ordered_case_ids + (execution.mechanics_case_id(self.value),))
        self.assertEqual(owner.config['profile']['ordered_actions'], list(self.value.phases))
        capture = execution.SOURCE_CAPTURE_POLICY.record()
        self.assertEqual((capture['timeout_seconds'], capture['cleanup_reap_seconds']), (60, 5))
        self.assertFalse(capture['cross_boundary_cache'])
        self.assertEqual(owner.config['source_capture_policy'], capture)
        self.assertEqual({name: owner.sources[name] for name in capture['sources']}, capture['sources'])

    def test_fixture_owner_cannot_dispatch_through_public_or_internal_routes(self):
        owner = self.owner()
        with patch.object(execution.subprocess, 'Popen', side_effect=AssertionError('No physical operation permitted')):
            for operation in (owner.execute_once, lambda: owner._dispatch({}),
                lambda: execution._Commands(owner).run('fixture', ['docker', 'version']),
                lambda: execution._Session(owner, execution._Commands(owner), ['docker', 'exec'])):
                with self.subTest(operation=operation), self.assertRaisesRegex(execution.ExecutionError, 'Fixture'):
                    operation()
        self.assertFalse(owner.has_retained('intent.json'))
        self.assertFalse(owner.has_retained('session-dispatch.json'))

    def test_storage_family_or_qualification_purpose_cannot_be_relabelled(self):
        for changes in ({'family': 'b02'}, {'purpose': 'harness_qualification'},
                        {'requirements_sha256': 'f' * 64}, {'milestone': 'M2'}):
            with self.subTest(changes=changes), self.assertRaises(execution.ExecutionError):
                replace(self.binding, **changes)
        with self.assertRaises(execution.ExecutionError):
            execution.observation_registration(SimpleNamespace(**asdict(self.registration)))

    def test_changed_gate_and_source_plan_are_rejected_before_owner_journal(self):
        changed = replace(self.registration, gate=replace(self.gate, ordered_case_ids=self.gate.ordered_case_ids[:-1]))
        with self.assertRaisesRegex(execution.ExecutionError, 'gate differs'):
            self.owner(registration=changed)
        self.assertFalse(self.raw.exists())
        with self.assertRaises(admission.AdmissionError):
            execution.binding_for(self.files, self.value, self.policy, {'kind': 'fixture-no-Docker'},
                replace(self.plan, schema_sha256='f' * 64), review_authority=self.review)

    def test_reopen_requires_original_external_prefix_and_full_m2_config(self):
        owner = self.owner()
        expected = owner.checkpoint()
        owner.close()
        with self.assertRaisesRegex(execution.ExecutionError, 'Reopen requires'):
            self.owner()
        reopened = self.owner(expected=expected)
        self.assertEqual(reopened.checkpoint(), expected)
        reopened.close()
        (self.raw / 'config.json').write_bytes(b'{}')
        with self.assertRaises(chain.ChainError):
            self.owner(expected=expected)

    def test_constructor_and_every_current_boundary_use_fresh_fixed_batch_capture(self):
        target = execution.source_capture.capture_registered_source.__code__
        calls = []
        prior = sys.getprofile()
        def trace(frame, event, arg):
            if event == 'call' and frame.f_code is target:
                calls.append((frame.f_locals['store'], frame.f_locals['commit_oid'], frame.f_locals['policy']))
            if prior is not None:
                prior(frame, event, arg)
        # Observe the real loaded function without replacing authenticated code.
        sys.setprofile(trace)
        try:
            owner = self.owner()
            owner.current(None)
            owner.current(None)
        finally:
            sys.setprofile(prior)
        self.assertEqual(calls, [(self.store, self.commit, execution.SOURCE_CAPTURE_POLICY)] * 3)
        with patch.object(execution.source_capture.batch, 'MAX_FILES', 512):
            with self.assertRaises(execution.source_capture.SourceCaptureUnavailable):
                owner.current(None)
        self.assertFalse(owner.has_retained('intent.json'))

    def test_original_review_revocation_stops_current_m2_owner(self):
        owner = self.owner()
        self.review_journal.retain('late.json', b'{}')
        with self.assertRaises(admission.AdmissionError):
            owner.current(None)

    def test_admission_revocation_stops_current_m2_owner(self):
        owner = self.owner()
        self.current['registration'] = None
        with self.assertRaises(admission.AdmissionError):
            owner.current(None)

    def test_fixture_manifest_binds_seed_bytes_without_candidate_import_or_expected_answers(self):
        owner = self.owner()
        self.assertEqual(owner.binding.fixture_sha256, execution.fixture_identity(self.value.case_id))
        files = profile.adapter_files(self.value.case_id)
        self.assertEqual(set(files), {'m2_adapter.py', 'recipe.json', 'seed.sqlite'})
        self.assertEqual(profile.decode(files['recipe.json']), profile.recipe_for(self.value.case_id))
        self.assertNotIn('expected', profile.decode(files['recipe.json']))
        self.assertEqual(files['seed.sqlite'][:16], b'SQLite format 3\x00')
        with patch.object(profile, 'adapter_files', return_value={**files, 'seed.sqlite': files['seed.sqlite'] + b'x'}):
            self.assertNotEqual(owner.binding.fixture_sha256, execution.fixture_identity(self.value.case_id))

    def test_changed_fresh_helper_is_rejected_before_any_docker_or_cleanup_effect(self):
        owner = self.owner()
        original = profile.adapter_files(self.value.case_id)
        changed = {**original, 'seed.sqlite': original['seed.sqlite'] + b'changed-after-binding'}
        intent = {'container': 'fixture-container', 'volume': 'fixture-volume', 'execution_id': 'fixture-guard'}
        # Enter the physical staging branch with all external effects forbidden;
        # no physical candidate owner or candidate execution is created.
        with patch.object(owner, 'mode', 'physical'), patch.object(profile, 'adapter_files', return_value=changed), \
             patch.object(execution._Commands, 'run', side_effect=AssertionError('Docker must not start')) as run, \
             patch.object(execution.cleanup.CleanupChannel, 'create', side_effect=AssertionError('No resource claims')) as create:
            with self.assertRaisesRegex(execution.ExecutionError, 'fixture bytes differ before dispatch'):
                owner._dispatch(intent)
        run.assert_not_called()
        create.assert_not_called()
        self.assertIsNone(owner._cleanup)

    def test_existing_m2_intent_cannot_be_replayed(self):
        owner = self.owner()
        self.intent(owner)
        with self.assertRaisesRegex(execution.ExecutionError, 'Existing intent'):
            owner.execute_once()
        self.assertTrue(owner.has_retained('intent.json'))


class CandidateM2ActionStreamV1Tests(unittest.TestCase):
    def session(self, values=(), *, reject_ack=False):
        events = []
        originals = {}
        owner = SimpleNamespace(profile=SimpleNamespace(phases=('action-000', 'action-001')),
            checkpoint=lambda: events.append('checkpoint'))
        def boundary():
            events.append('post-ack-authority')
            if reject_ack:
                raise admission.AdmissionError('Original admission revoked after acknowledgement')
        owner._effect_boundary = boundary
        def retain(name, raw):
            originals[name] = raw
            events.append('raw-retained')
        session = object.__new__(execution._WireSession)
        session.commands = SimpleNamespace(owner=owner, timeout=0.01, retain=retain)
        session.process = SimpleNamespace(stdin=io.BytesIO())
        session.lines = queue.Queue()
        for value in values:
            session.lines.put(value)
        session.requests = []
        session.errors = set()
        session.counts = {'stdout': 0, 'stderr': 0}
        return session, originals, events

    def line(self, index, result):
        return execution.encoded({'phase': 'action-%03d' % index,
            'value': {'action_index': index, 'result': result}}) + b'\n'

    def test_each_raw_reply_is_retained_before_post_ack_and_parse(self):
        raw = self.line(0, {'error': 'invalid_text', 'authored_surrogate': '\ud800'})
        session, originals, events = self.session([raw])
        result = session.phase('action-000')
        self.assertEqual(result['result']['authored_surrogate'], '\ud800')
        self.assertEqual(originals, {'action-000-response.json': raw})
        self.assertEqual(events, ['raw-retained', 'checkpoint', 'post-ack-authority'])
        self.assertEqual(session.process.stdin.getvalue(), b'action-000\n')

    def test_reordering_and_replay_are_rejected_before_request_bytes(self):
        session, _, _ = self.session([self.line(0, None)])
        with self.assertRaisesRegex(execution.ExecutionError, 'reordering'):
            session.phase('action-001')
        self.assertEqual(session.process.stdin.getvalue(), b'')
        session.phase('action-000')
        with self.assertRaisesRegex(execution.ExecutionError, 'reordering'):
            session.phase('action-000')
        self.assertEqual(session.process.stdin.getvalue(), b'action-000\n')

    def test_bad_shape_bool_index_and_duplicate_json_remain_originals(self):
        raws = (execution.encoded({'phase': 'action-000', 'value': {'action_index': False, 'result': None}}) + b'\n',
            b'{"phase":"action-000","phase":"action-000","value":null}\n',
            b'{"phase":"action-001","value":{"action_index":0,"result":null}}\n',
            self.line(0, None).rstrip(b'\n'))
        for raw in raws:
            with self.subTest(raw=raw):
                session, originals, _ = self.session([raw])
                with self.assertRaises(ValueError):
                    session.phase('action-000')
                self.assertEqual(originals, {'action-000-response.json': raw})

    def test_missing_tail_preserves_prior_raw_failure_without_synthetic_reply(self):
        first = self.line(0, {'error': 'wrong_answer'})
        session, originals, _ = self.session([first, None])
        self.assertEqual(session.phase('action-000')['result'], {'error': 'wrong_answer'})
        with self.assertRaisesRegex(execution.ExecutionError, 'Incomplete'):
            session.phase('action-001')
        self.assertEqual(originals, {'action-000-response.json': first})
        self.assertEqual(len(session.requests), 2)

    def test_revoked_post_ack_authority_retains_reply_but_prevents_value_return(self):
        raw = self.line(0, {'ok': True})
        session, originals, events = self.session([raw], reject_ack=True)
        with self.assertRaises(admission.AdmissionError):
            session.phase('action-000')
        self.assertEqual(originals, {'action-000-response.json': raw})
        self.assertEqual(events[-1], 'post-ack-authority')

    def test_missing_response_timeout_preserves_request_and_no_reply(self):
        session, originals, _ = self.session()
        with self.assertRaisesRegex(execution.ExecutionError, 'timeout'):
            session.phase('action-000')
        self.assertEqual(originals, {})
        self.assertEqual(session.requests[0]['phase'], 'action-000')

    def test_stream_overflow_refuses_further_input(self):
        session, originals, _ = self.session([self.line(0, None)])
        session.counts['stdout'] = execution.b02.MAX_STREAM_BYTES + 1
        with self.assertRaisesRegex(execution.ExecutionError, 'output bound'):
            session.phase('action-000')
        self.assertEqual(session.process.stdin.getvalue(), b'')
        self.assertEqual(originals, {})

    def test_partial_session_reader_startup_reaps_owned_local_process(self):
        events = []
        child = SimpleNamespace(stdin=io.BytesIO(), stdout=io.BytesIO(), stderr=io.BytesIO(),
            poll=lambda: None, kill=lambda: events.append('killed'), wait=lambda **kwargs: events.append('reaped'))
        owner = SimpleNamespace(mode='physical', docker=['docker', '--host', 'unix:///fixture.sock'],
            _effect_boundary=lambda: events.append('boundary'),
            _retain=lambda name, raw: events.append(name), checkpoint=lambda: events.append('checkpoint'))
        with patch.object(execution.subprocess, 'Popen', return_value=child), \
             patch.object(execution.threading, 'Thread', side_effect=RuntimeError('reader allocation failure')):
            with self.assertRaisesRegex(RuntimeError, 'reader allocation'):
                execution._Session(owner, SimpleNamespace(), ['docker', 'exec', 'fixture'])
        self.assertIn('session-dispatch.json', events)
        self.assertEqual(events[-2:], ['killed', 'reaped'])
        self.assertTrue(child.stdin.closed and child.stdout.closed and child.stderr.closed)

    def test_durable_action_request_precedes_child_input(self):
        original, originals, events = self.session([self.line(0, None)])
        owner = original.commands.owner
        owner._retain = lambda name, raw: events.append(('request-retained', name, raw))
        session = object.__new__(execution._Session)
        session.owner, session.original, session.finished = owner, original, False
        session.phase('action-000')
        request_event = next(event for event in events if isinstance(event, tuple))
        self.assertEqual(request_event, ('request-retained', 'action-000-request.json',
            execution.encoded({'phase': 'action-000', 'request': 'action-000\n'})))
        position = events.index(request_event)
        self.assertEqual(events[position + 1:position + 3], ['checkpoint', 'post-ack-authority'])
        self.assertLess(position, events.index('raw-retained'))
        self.assertIn('action-000-response.json', originals)

    def test_failed_action_intent_checkpoint_prevents_child_input(self):
        original, originals, events = self.session([self.line(0, None)])
        owner = original.commands.owner
        owner._retain = lambda name, raw: events.append(name)
        def checkpoint():
            raise chain.ChainError('Durable checkpoint failed')
        owner.checkpoint = checkpoint
        session = object.__new__(execution._Session)
        session.owner, session.original, session.finished = owner, original, False
        with self.assertRaises(chain.ChainError):
            session.phase('action-000')
        self.assertIn('action-000-request.json', events)
        self.assertEqual(original.process.stdin.getvalue(), b'')
        self.assertEqual(originals, {})
