"""Synthetic review linkage only; no independent approval or execution is supplied."""
from dataclasses import replace
import hashlib
from pathlib import Path
import tempfile
import unittest

from gossip_harness import candidate_checkpoint_chain_v1 as chain
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from gossip_harness import cumulative_generated_probe_plan_v1 as plans
from gossip_harness import cumulative_generated_probe_review_v1 as review
from gossip_harness import cumulative_generated_probe_values_v2 as values
from tests.test_cumulative_generated_probe_plan_v1 import declaration


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


class GeneratedProbeReviewTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='synthetic-generated-probe-review-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.plan = declaration()
        self.count = 0

    def make(self, *, request=None, report_changes=None, delivery_changes=None, reverse=False, request_raw=None):
        self.count += 1
        root = self.root / str(self.count); root.mkdir()
        raw, delta = root / 'raw', root / 'delta'
        head = ExternalHead.create(root / 'head', journal_roots=(raw, delta))
        self.addCleanup(head.close)
        journal = chain.CheckpointChain.create(raw, delta,
            context={'synthetic_fixture_only': True, 'actual_source_review_supplied': False}, authority=head)
        self.addCleanup(journal.close)
        request_raw = values.canonical(self.plan.review_request() if request is None else request) if request_raw is None else request_raw
        report = {'protocol': review.PROTOCOL, 'purpose': plans.REVIEW_PURPOSE,
            'reviewer_id': 'synthetic-unit-reviewer', 'request_sha256': sha(request_raw),
            'decisions': [{'id': duty, 'decision': 'approved', 'rationale': 'Synthetic linkage fixture only.',
                           'source_references': ['synthetic-unit-data:1']} for duty in plans.DUTIES],
            'remaining_obligations': ['Actual independent source review and every execution gate remain unsupplied.']}
        report.update(report_changes or {})
        report_raw = values.canonical(report)
        delivery = {'protocol': review.PROTOCOL, 'purpose': plans.REVIEW_PURPOSE,
            'reviewer_id': 'synthetic-unit-reviewer', 'request_sha256': sha(request_raw),
            'report_sha256': sha(report_raw), 'origin': 'independently_delivered_host_review'}
        delivery.update(delivery_changes or {})
        delivery_raw = values.canonical(delivery)
        journal.retain('request.json', request_raw)
        rows = [('report.json', report_raw), ('delivery.json', delivery_raw)]
        for name, original in reversed(rows) if reverse else rows:
            journal.retain(name, original)
        enrollment = review.ReviewEnrollment('synthetic-unit-reviewer', 'request.json', sha(request_raw),
            'report.json', sha(report_raw), 'delivery.json', sha(delivery_raw))
        return review.ProbeReviewAuthority(journal, journal.commitment, enrollment), journal

    def test_exact_originals_and_chronology_reconstruct_provenance(self):
        authority, _ = self.make()
        pin, provenance = authority.authenticate_with_provenance(self.plan)
        self.assertEqual(pin, authority.enrollment.report_sha256)
        self.assertEqual(provenance['positions'], {'request': 1, 'report': 2, 'delivery': 3})
        self.assertEqual(provenance, authority.provenance(self.plan))
        self.assertIn('no execution or product acceptance', provenance['scope'])

    def test_other_candidate_context_or_stage_cannot_reuse_review(self):
        authority, _ = self.make()
        for change in ({'context_id': 'other'}, {'candidate_id': 'other'}, {'stage': 'merged'}):
            with self.subTest(change=change), self.assertRaisesRegex(ValueError, 'review_request_differs'):
                authority.authenticate(replace(self.plan, target=replace(self.plan.target, **change)))

    def test_rejected_missing_or_ungrounded_duties_are_refused(self):
        for decision in ('rejected', 'pending'):
            rows = [{'id': duty, 'decision': decision, 'rationale': 'Explicit fixture negative.',
                     'source_references': ['fixture:1']} for duty in plans.DUTIES]
            authority, _ = self.make(report_changes={'decisions': rows})
            with self.assertRaises(ValueError): authority.authenticate(self.plan)
        authority, _ = self.make(report_changes={'decisions': []})
        with self.assertRaises(ValueError): authority.authenticate(self.plan)
        rows = [{'id': duty, 'decision': 'approved', 'rationale': '', 'source_references': []} for duty in plans.DUTIES]
        authority, _ = self.make(report_changes={'decisions': rows})
        with self.assertRaises(ValueError): authority.authenticate(self.plan)

    def test_different_protocol_reviewer_or_delivery_is_refused(self):
        for change in ({'protocol': 'candidate-storage-review-authority-v1-ascii-json-v1'},
                       {'reviewer_id': 'different'}, {'request_sha256': '0'*64}):
            authority, _ = self.make(report_changes=change)
            with self.subTest(change=change), self.assertRaises(ValueError): authority.authenticate(self.plan)
        for change in ({'origin': 'candidate-asserted'}, {'report_sha256': '0'*64}, {'purpose': 'independent_acceptance'}):
            authority, _ = self.make(delivery_changes=change)
            with self.subTest(change=change), self.assertRaises(ValueError): authority.authenticate(self.plan)

    def test_enrollment_hash_substitution_is_refused(self):
        authority, journal = self.make()
        bad = replace(authority.enrollment, report_sha256='0'*64)
        replacement = review.ProbeReviewAuthority(journal, authority.expected, bad)
        with self.assertRaisesRegex(ValueError, 'original_report_digest_differs'): replacement.authenticate(self.plan)

    def test_success_is_not_cached_across_external_prefix_change(self):
        authority, journal = self.make(); authority.authenticate(self.plan)
        journal.retain('revocation.json', values.canonical({'revoked': True}))
        with self.assertRaisesRegex(ValueError, 'independent_review_prefix_changed'):
            authority.authenticate(self.plan)

    def test_original_tampering_is_rejected_even_after_success(self):
        authority, journal = self.make(); authority.authenticate(self.plan)
        original = journal.raw_root / 'report.json'
        original.chmod(0o600); original.write_bytes(b'{}')
        with self.assertRaises(ValueError): authority.authenticate(self.plan)

    def test_delivery_before_report_cannot_authorize_execution(self):
        authority, _ = self.make(reverse=True)
        with self.assertRaisesRegex(ValueError, 'review_original_chronology_differs'):
            authority.authenticate(self.plan)

    def test_duplicate_noncanonical_or_overlarge_originals_are_refused(self):
        for raw in (b'{"protocol":"x","protocol":"y"}', b' {"protocol":"x"}'):
            authority, _ = self.make(request_raw=raw)
            with self.subTest(raw=raw), self.assertRaises(ValueError): authority.authenticate(self.plan)
        with self.assertRaises(ValueError): review._original(b' ' * (review.MAX_ORIGINAL_BYTES + 1))

    def test_scope_limits_and_distinct_names_are_required(self):
        authority, _ = self.make(report_changes={'remaining_obligations': []})
        with self.assertRaises(ValueError): authority.authenticate(self.plan)
        with self.assertRaises(ValueError): replace(authority.enrollment, delivery_name='report.json')
        with self.assertRaises(ValueError): replace(authority.enrollment, request_name='../request.json')


if __name__ == '__main__':
    unittest.main()
