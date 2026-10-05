"""Real inert Git/durable journals, synthetic reviewer and controller capabilities.

No candidate is imported, no production approval is created, and no Docker or
provider request is made. Controlled clocks test state boundaries only.
"""
from dataclasses import asdict, replace
import hashlib
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest import mock

from gossip_harness import candidate_checkpoint_chain_v1 as chain
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import cumulative_generated_probe_plan_v1 as plans
from gossip_harness import cumulative_generated_probe_review_v1 as reviews
from gossip_harness import cumulative_generated_probe_state_v1 as state
from gossip_harness import cumulative_generated_probe_values_v2 as values
from gossip_harness.gitstore import GitStore
from tests.test_cumulative_generated_probe_plan_v1 import declaration, policy, target


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


class GeneratedProbeStateGitTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='synthetic-probe-state-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        files = {'solution.py': 'raise RuntimeError("candidate must not run on host")\n'}
        self.store = GitStore.create(self.root / 'source.git', files)
        commit = self.store.head(); tree = self.store._git('rev-parse', commit + '^{tree}')
        self.plan = declaration(subject=target({k: v.encode() for k, v in files.items()}, commit, tree),
                                limits=replace(policy(), history_seconds=300))
        raw, delta = self.root / 'review-raw', self.root / 'review-delta'
        review_head = ExternalHead.create(self.root / 'review-head', journal_roots=(raw, delta))
        self.addCleanup(review_head.close)
        journal = chain.CheckpointChain.create(raw, delta,
            context={'synthetic_fixture_only': True, 'actual_source_review_supplied': False}, authority=review_head)
        self.addCleanup(journal.close)
        request_raw = values.canonical(self.plan.review_request())
        report_raw = values.canonical({'protocol': reviews.PROTOCOL, 'purpose': plans.REVIEW_PURPOSE,
            'reviewer_id': 'synthetic-unit-reviewer', 'request_sha256': sha(request_raw),
            'decisions': [{'id': duty, 'decision': 'approved', 'rationale': 'Synthetic linkage fixture only.',
                           'source_references': ['synthetic-unit-data:1']} for duty in plans.DUTIES],
            'remaining_obligations': ['Actual independent review and physical execution remain unsupplied.']})
        delivery_raw = values.canonical({'protocol': reviews.PROTOCOL, 'purpose': plans.REVIEW_PURPOSE,
            'reviewer_id': 'synthetic-unit-reviewer', 'request_sha256': sha(request_raw),
            'report_sha256': sha(report_raw), 'origin': 'independently_delivered_host_review'})
        for name, original in [('request.json', request_raw), ('report.json', report_raw), ('delivery.json', delivery_raw)]:
            journal.retain(name, original)
        enrollment = reviews.ReviewEnrollment('synthetic-unit-reviewer', 'request.json', sha(request_raw),
            'report.json', sha(report_raw), 'delivery.json', sha(delivery_raw))
        self.review = reviews.ProbeReviewAuthority(journal, journal.commitment, enrollment)
        self.review_journal = journal
        self.now = time.monotonic_ns()
        self.window = state.ProbeWindow(self.now, self.now + 300_000_000_000)
        self.runtime = {'declared_image': 'synthetic-not-a-runtime-proof'}
        self.environment = {'clock_domain': 'synthetic-unit-test', 'synthetic': True}
        self.binding = state.binding_for(self.plan, self.review, runtime=self.runtime,
                                         environment=self.environment, window=self.window)
        self.registration = state.observation_registration(self.plan, self.binding, gate_id='probe-gate',
            repetition_id='public-development-1', cohort_trajectory_ids=('trajectory', 't2', 't3', 't4', 't5', 't6'))
        self.available = True
        self.on_verify = None
        def verify_registration():
            if self.on_verify is not None:
                self.on_verify()
            return self.registration if self.available else None
        self.admission = admission.ObservationAdmission(self.registration, verify_registration=verify_registration)
        self.raw, self.delta = self.root / 'state-raw', self.root / 'state-delta'
        self.head = ExternalHead.create(self.root / 'state-head', journal_roots=(self.raw, self.delta))
        self.addCleanup(self.head.close)

    def open(self, **overrides):
        args = dict(head=self.head, store=self.store, plan=self.plan, review=self.review,
                    binding=self.binding, registration=self.registration, admission_authority=self.admission,
                    runtime=self.runtime, environment=self.environment)
        args.update(overrides)
        owner = state.ProbeExecutionState(self.raw, self.delta, **args)
        self.addCleanup(owner.close)
        return owner

    def advance(self):
        old = self.store.head(); offered = self.store.propose({'changed.txt': 'new\n'}, old)
        self.store._git('update-ref', 'refs/heads/accepted', offered, old)

    def test_intent_survives_reopen_and_never_permits_second_begin(self):
        owner = self.open(); initial = owner.current()
        intent = owner.begin()
        self.assertFalse(intent['candidate_dispatched'])
        self.assertFalse(intent['execution_authority'])
        self.assertFalse(intent['acceptance_authority'])
        self.assertEqual(owner.journal.read('intent.json'), values.canonical(intent))
        after = owner.current(); self.assertGreater(after.sequence, initial.sequence)
        with self.assertRaisesRegex(ValueError, 'existing_probe_intent'): owner.begin()
        owner.close()
        reopened = self.open(expected_checkpoint=after)
        self.assertEqual(reopened.journal.read('intent.json'), values.canonical(intent))
        with self.assertRaisesRegex(ValueError, 'existing_probe_intent'): reopened.begin()

    def test_unused_state_reopens_only_with_independent_current_prefix(self):
        owner = self.open(); checkpoint = owner.current(); owner.close()
        with self.assertRaisesRegex(ValueError, 'reopen_requires'): self.open()
        reopened = self.open(expected_checkpoint=checkpoint)
        self.assertEqual(reopened.current(), checkpoint)
        self.assertFalse(reopened.journal.has('intent.json'))

    def test_stale_prefix_cannot_adopt_later_intent(self):
        owner = self.open(); stale = owner.current(); owner.begin(); owner.close()
        with self.assertRaises(ValueError): self.open(expected_checkpoint=stale)

    def test_registration_revocation_before_intent_leaves_no_intent(self):
        owner = self.open(); self.available = False
        with self.assertRaises(admission.AdmissionUnavailable): owner.begin()
        self.assertFalse(owner.journal.has('intent.json'))

    def test_revocation_after_durable_intent_fails_without_erasing_intent(self):
        owner = self.open()
        def revoke_after_write():
            if (self.raw / 'intent.json').exists(): self.available = False
        self.on_verify = revoke_after_write
        with self.assertRaises(admission.AdmissionUnavailable): owner.begin()
        self.assertTrue(owner.journal.has('intent.json'))
        self.on_verify = None; self.available = True
        checkpoint = owner.journal.checkpoint(); owner.close()
        reopened = self.open(expected_checkpoint=checkpoint)
        with self.assertRaisesRegex(ValueError, 'existing_probe_intent'): reopened.begin()

    def test_absolute_window_rejects_not_started_and_expired_without_intent(self):
        owner = self.open()
        for instant in (self.window.started_ns - 1, self.window.deadline_ns):
            with self.subTest(instant=instant), mock.patch.object(time, 'monotonic_ns', return_value=instant):
                with self.assertRaisesRegex(ValueError, 'probe_absolute_window'): owner.begin()
        self.assertFalse(owner.journal.has('intent.json'))

    def test_expiry_during_post_intent_validation_preserves_single_attempt(self):
        owner = self.open()
        def expire_after_write():
            if (self.raw / 'intent.json').exists(): self.now = self.window.deadline_ns
        self.on_verify = expire_after_write
        with mock.patch.object(time, 'monotonic_ns', side_effect=lambda: self.now):
            with self.assertRaisesRegex(ValueError, 'probe_absolute_window'): owner.begin()
        self.assertTrue(owner.journal.has('intent.json'))
        with self.assertRaisesRegex(ValueError, 'existing_probe_intent'): owner.begin()

    def test_source_and_review_changes_are_checked_again_at_begin(self):
        owner = self.open(); self.advance()
        with self.assertRaisesRegex(ValueError, 'registered_head_changed'): owner.begin()
        self.assertFalse(owner.journal.has('intent.json'))

    def test_review_revocation_invalidates_previously_open_state(self):
        owner = self.open()
        self.review_journal.retain('revocation.json', values.canonical({'revoked': True}))
        with self.assertRaisesRegex(ValueError, 'independent_review_prefix_changed'): owner.begin()
        self.assertFalse(owner.journal.has('intent.json'))

    def test_runtime_declaration_mismatch_rejected_before_state_creation(self):
        with self.assertRaisesRegex(ValueError, 'probe_runtime_review_or_binding_changed'):
            self.open(runtime={'declared_image': 'another'})
        self.assertFalse(self.raw.exists())

    def test_caller_mutation_cannot_change_retained_declarations(self):
        owner = self.open()
        self.runtime['declared_image'] = 'mutated'; self.environment['synthetic'] = False
        owner.current()
        config = json.loads(owner.journal.read('config.json'))
        self.assertEqual(config['runtime_declaration']['declared_image'], 'synthetic-not-a-runtime-proof')
        self.assertIs(config['environment_declaration']['synthetic'], True)

    def test_registration_substitution_rejected_before_state_creation(self):
        other = replace(self.registration, repetition_id='another')
        with self.assertRaisesRegex(ValueError, 'prospective_probe_registration_differs'):
            self.open(registration=other)
        self.assertFalse(self.raw.exists())

    def test_tampered_config_and_unanchored_suffix_are_not_adopted(self):
        owner = self.open()
        (self.raw / 'intent.json').write_bytes(b'{}')
        with self.assertRaises(ValueError): owner.begin()
        self.assertTrue(owner.journal.uncertain)

    def test_raw_original_tamper_invalidates_current_state(self):
        owner = self.open(); path = self.raw / 'config.json'
        path.chmod(0o600); path.write_bytes(b'{}')
        with self.assertRaises(ValueError): owner.current()

    def test_wrong_thread_and_closed_owner_reject_access(self):
        owner = self.open(); failures = []
        def other_thread():
            for method in (owner.current, owner.begin, owner.close):
                try: method()
                except ValueError as exc: failures.append(str(exc))
        thread = threading.Thread(target=other_thread); thread.start(); thread.join(5)
        self.assertFalse(thread.is_alive()); self.assertEqual(len(failures), 3)
        owner.close()
        with self.assertRaisesRegex(ValueError, 'closed_or_wrong_owner'): owner.begin()

    def test_separate_state_cannot_overlap_source_or_review(self):
        bad_raw = self.store.path / 'state-raw'; bad_delta = self.store.path / 'state-delta'
        bad_head = ExternalHead.create(self.root / 'bad-head', journal_roots=(bad_raw, bad_delta))
        self.addCleanup(bad_head.close)
        with self.assertRaisesRegex(ValueError, 'state_proof_source_roots_overlap'):
            state.ProbeExecutionState(bad_raw, bad_delta, head=bad_head, store=self.store, plan=self.plan,
                review=self.review, binding=self.binding, registration=self.registration,
                admission_authority=self.admission, runtime=self.runtime, environment=self.environment)
        self.assertFalse(bad_raw.exists())


class GeneratedProbeWindowTests(unittest.TestCase):
    def test_window_is_exact_positive_bounded_and_ordered(self):
        for start, end in [(True, 2), (0, 2), (-1, 2), (2, 2), (3, 2), (1, 2**63), (1, 2.0)]:
            with self.subTest(start=start, end=end), self.assertRaises(ValueError): state.ProbeWindow(start, end)
        self.assertEqual(asdict(state.ProbeWindow(1, 2)), {'started_ns': 1, 'deadline_ns': 2})


if __name__ == '__main__':
    unittest.main()
