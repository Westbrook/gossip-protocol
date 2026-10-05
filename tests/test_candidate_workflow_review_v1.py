"""Original-chain linkage controls, not real independent semantic approvals.

No Engine/provider is involved. Reports and admission capabilities are explicit
synthetic host fixtures. Real chains/Git still exercise authentic bytes, lifetime,
source, chronology and fresh-request boundaries through the actual owner.
"""
from dataclasses import asdict, replace
import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from gossip_harness import candidate_workflow_review_v1 as review
from gossip_harness import candidate_checkpoint_chain_v1 as chain
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import cumulative_scope_authority_v1 as scope_authority
from gossip_harness import cumulative_workflow_exposure_v1 as exposure
from gossip_harness import cumulative_cli_projection_v1 as cli
from gossip_harness import project_acceptance_registry_v1 as registry
from gossip_harness.gitstore import GitStore

COHORT = tuple('trajectory-' + str(i) for i in range(6))


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def synthetic_delivery(root, request, *, product=False, decisions=None, changes=None, reverse=False):
    """Actual protected fixture originals, explicitly not production review."""
    root.mkdir(mode=0o700)
    raw, delta = root / 'raw', root / 'delta'
    head = ExternalHead.create(root / 'head', journal_roots=(raw, delta))
    journal = chain.CheckpointChain.create(raw, delta,
        context={'fixture_only': True, 'semantic_approval': False}, authority=head)
    protocol = review.INSPECTION_PROTOCOL if product else review.PROTOCOL
    purpose = review.INSPECTION_PURPOSE if product else review.PURPOSE
    duties = review.INSPECTION_DUTIES if product else review.DUTIES
    request_raw = review.encoded(request)
    report = {'protocol': protocol, 'purpose': purpose, 'reviewer_id': 'fixture-reviewer',
        'request_sha256': sha(request_raw), 'decisions': decisions or [
            {'id': duty, 'decision': 'approved', 'rationale': 'Synthetic linkage fixture only.',
             'source_references': ['fixture-only:1']} for duty in duties],
        'remaining_obligations': ['Actual independently authorized source/semantic review is not supplied.']}
    report.update(changes or {})
    report_raw = review.encoded(report)
    delivery_raw = review.encoded({'protocol': protocol, 'purpose': purpose, 'reviewer_id': 'fixture-reviewer',
        'request_sha256': sha(request_raw), 'report_sha256': sha(report_raw),
        'origin': 'independently_delivered_host_review'})
    journal.retain('request.json', request_raw)
    rows = [('report.json', report_raw), ('delivery.json', delivery_raw)]
    for name, data in reversed(rows) if reverse else rows:
        journal.retain(name, data)
    enrollment = review.ReviewEnrollment('fixture-reviewer', 'request.json', sha(request_raw),
        'report.json', sha(report_raw), 'delivery.json', sha(delivery_raw))
    cls = review.ProductInspectionDelivery if product else review.WorkflowReviewAuthority
    return cls(journal, journal.commitment, enrollment), journal, head


class CandidateWorkflowReviewTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        value = review.WorkflowInspectionProfile()
        self.plan = review.WorkflowSourcePlan('a'*64, 'b'*64, 'c'*40, 'd'*40, value.case_id,
            value.sha256, 'reviewed-workflow-final-sqlite-v1', ('library.sqlite',), 'e'*64,
            value.purpose, profile_kind='workflow_inspection')

    def make(self, *, product=False, request=None, **kwargs):
        request = request if request is not None else self.plan.request()
        authority, journal, head = synthetic_delivery(self.root/'proof', request, product=product, **kwargs)
        self.addCleanup(head.close)
        self.addCleanup(journal.close)
        return authority, journal

    def test_actual_original_request_report_delivery_and_positions(self):
        authority, journal = self.make()
        self.assertEqual(authority.authenticate(self.plan), authority.enrollment.report_sha256)
        original = authority.provenance(self.plan)
        self.assertEqual(original['positions'], {'request': 1, 'report': 2, 'delivery': 3})
        self.assertEqual(journal.commitment, authority.expected)

    def test_combined_read_matches_both_legacy_identities_and_never_caches(self):
        authority, journal = self.make()
        expected = (authority.authenticate(self.plan), authority.provenance(self.plan))
        with mock.patch.object(authority, '_original', wraps=authority._original) as reads:
            actual = authority.authenticate_with_provenance(self.plan)
            self.assertEqual(actual, expected)
            self.assertEqual(reads.call_count, 1)
            actual[1]['positions']['report'] = 999
            self.assertEqual(authority.authenticate_with_provenance(self.plan), expected)
            self.assertEqual(reads.call_count, 2)
        (journal.raw_root/'report.json').write_bytes(b'{}')
        with self.assertRaises(chain.ChainError):
            authority.authenticate_with_provenance(self.plan)

    def test_combined_read_refuses_changed_plan_and_revoked_prefix(self):
        authority, _ = self.make()
        authority.authenticate_with_provenance(self.plan)
        with self.assertRaises(admission.AdmissionError):
            authority.authenticate_with_provenance(replace(self.plan, source_sha256='f'*64))
        authority.expected = replace(authority.expected, head_sha256='f'*64)
        with self.assertRaises(chain.ChainError):
            authority.authenticate_with_provenance(self.plan)

    def test_rejected_mechanism_duty_cannot_be_approved(self):
        rows = [{'id': duty, 'decision': 'rejected' if i == 0 else 'approved',
            'rationale': 'Fixture only', 'source_references': ['fixture:1']} for i, duty in enumerate(review.DUTIES)]
        authority, _ = self.make(decisions=rows)
        with self.assertRaises(admission.AdmissionError):
            authority.authenticate(self.plan)

    def test_product_rejected_and_unresolved_decisions_are_preserved(self):
        rows = [{'id': duty, 'decision': ('rejected' if i == 0 else 'unresolved' if i == 1 else 'approved'),
            'rationale': 'Fixture only', 'source_references': ['fixture:1']} for i, duty in enumerate(review.INSPECTION_DUTIES)]
        request = {'fixture': 'no actual inspection or semantic authority'}
        delivery, _ = self.make(product=True, request=request, decisions=rows)
        self.assertEqual(delivery.authenticate(request)['report']['decisions'], rows)

    def test_product_cannot_use_mechanism_protocol_report(self):
        delivery, _ = self.make(product=True, changes={'protocol': review.PROTOCOL, 'purpose': review.PURPOSE})
        with self.assertRaises(admission.AdmissionError):
            delivery.authenticate(self.plan.request())

    def test_delivery_before_report_is_rejected_even_with_matching_hashes(self):
        authority, _ = self.make(reverse=True)
        with self.assertRaises(admission.AdmissionError):
            authority.authenticate(self.plan)

    def test_changed_source_profile_or_layout_request_is_rejected(self):
        authority, _ = self.make()
        for field in ('source_sha256', 'native_source_sha256', 'profile_sha256', 'schema_sha256'):
            with self.subTest(field=field), self.assertRaises(admission.AdmissionError):
                authority.authenticate(replace(self.plan, **{field: 'f'*64}))

    def test_tampered_raw_report_is_not_trusted_from_cached_digest(self):
        authority, journal = self.make()
        authority.authenticate(self.plan)
        (journal.raw_root/'report.json').write_bytes(b'{}')
        with self.assertRaises(chain.ChainError):
            authority.authenticate(self.plan)

    def test_new_prefix_revokes_original_review_snapshot(self):
        authority, journal = self.make()
        journal.retain('later.json', b'{}')
        with self.assertRaises(chain.ChainError):
            authority.authenticate(self.plan)

    def test_closed_review_owner_is_rejected(self):
        authority, journal = self.make()
        journal.close()
        with self.assertRaises(chain.ChainError):
            authority.authenticate(self.plan)

    def test_wrong_enrollment_class_never_structurally_substitutes(self):
        authority, journal = self.make()
        self.assertIsNot(review.ReviewEnrollment, scope_authority.ReviewEnrollment)
        with self.assertRaises(admission.AdmissionError):
            review.WorkflowReviewAuthority(journal, journal.commitment, asdict(authority.enrollment))

    def test_boundaries_have_closed_paths_names_events_and_finite_limits(self):
        base = review.WorkflowBoundary('final', 'library/clients/workflow.py', 'solve', 10,
            'line', 'root', 'path', 'solve_exit')
        for changes in ({'source_path': '../escape.py'}, {'root_local': 'obj.root'}, {'event': 'call'},
                        {'occurrences': 0}, {'occurrences': True}, {'occurrences': 257}):
            with self.subTest(changes=changes), self.assertRaises(admission.AdmissionError):
                replace(base, **changes)

    def test_conservation_points_bind_all_four_exact_enrolled_events(self):
        boundary = review.WorkflowBoundary('observed', 'library/clients/workflow.py', 'solve',
            12, 'line', 'root', 'path', 'post_fault', 8)
        points = tuple(review.WorkflowCapturePoint(role, 0, 'observed', occurrence)
            for role, occurrence in (('initial', 0), ('post_fault', 3), ('reopened', 4), ('final', 7)))
        plan = replace(self.plan, case_id='WF19-provisional-fault-boundary', profile_kind='workflow',
            boundaries=(boundary,), capture_points=points)
        self.assertEqual(plan.record()['capture_points'], tuple(asdict(point) for point in points))
        self.assertNotEqual(plan.sha256, replace(plan, capture_points=points[:-1] +
            (replace(points[-1], occurrence=6),)).sha256)
        for invalid in (points[:-1], points + (points[0],),
                        points[:-1] + (replace(points[-1], boundary_id='absent'),),
                        points[:-1] + (replace(points[-1], occurrence=8),),
                        points[:-1] + (replace(points[-1], call_index=1),)):
            with self.subTest(points=invalid), self.assertRaises(admission.AdmissionError):
                replace(plan, capture_points=invalid)
        with self.assertRaises(admission.AdmissionError):
            replace(self.plan, capture_points=points)
        with self.assertRaises(admission.AdmissionError):
            replace(plan, case_id='inherited-text-persistence')

    def test_capture_point_rejects_untyped_or_open_role_and_numeric_selectors(self):
        base = review.WorkflowCapturePoint('post_fault', 0, 'observed', 3)
        for changes in ({'role': 'last_seen'}, {'call_index': True}, {'call_index': -1},
                        {'call_index': 3}, {'occurrence': True}, {'occurrence': -1},
                        {'occurrence': 256}):
            with self.subTest(changes=changes), self.assertRaises(admission.AdmissionError):
                replace(base, **changes)
        with self.assertRaises(admission.AdmissionError):
            replace(self.plan, capture_points=(asdict(base),))

    def test_host_source_selectors_preserve_exact_duties_and_actual_architecture_owners(self):
        value = review.WorkflowInspectionProfile()
        rows = value.selectors()
        self.assertEqual(tuple(row['case_id'] for row in rows), value.ordered_case_ids)
        self.assertEqual(len(rows), len(review.INSPECTION_DUTIES))
        for index, row in enumerate(rows):
            self.assertEqual(row['pointer'], '/outcomes/' + str(index) + '/status')
            self.assertEqual(row['definition_pointer'], '/duties/' + str(index))
            self.assertEqual(row['assertion_kind'], 'inspection')
            self.assertEqual(row['logical_gate_ids'], ['M1-GATE-ARCHITECTURE'])
            self.assertFalse(row['physical_engine_evidence'])
            self.assertFalse(row['whole_source_unit_qualified'])
            self.assertNotIn('m1:M1-ADAPTER-03', row['source_unit_ids'])
        self.assertIn('CLARIFY-INPUT-DOMAIN', rows[-1]['source_unit_ids'])
        self.assertEqual(value.record()['selectors'], rows)

    def test_product_profile_keeps_both_addendum_identities(self):
        value = review.WorkflowInspectionProfile().record()
        self.assertEqual(value['effective_requirements'], cli.manifest())
        self.assertEqual(value['workflow_requirements'], exposure.manifest())
        self.assertEqual(value['combined_effective_requirements'], exposure.combined_manifest())
        self.assertFalse(value['physical_engine_evidence'])
        with self.assertRaises(admission.AdmissionError):
            review.WorkflowInspectionProfile('public_release')


class CandidateWorkflowInspectionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temp.cleanup)
        cls.base = Path(cls.temp.name).resolve()
        cls.store = GitStore.create(cls.base/'repo.git', {'library/__init__.py': '# Never imported on host.\n',
            cli.RELEASE_PATH: cli.text(), exposure.RELEASE_PATH: exposure.text()})
        cls.commit = cls.store.head()
        cls.tree, cls.files = review.capture_git_source(cls.store, cls.commit)

    def setUp(self):
        self.root = self.base/self.id().rsplit('.', 1)[-1]
        self.root.mkdir()
        self.value = review.WorkflowInspectionProfile()
        self.plan = review.WorkflowSourcePlan(admission.source_sha256(self.files), review.source_sha256(self.files),
            self.commit, self.tree, self.value.case_id, self.value.sha256, 'reviewed-workflow-final-sqlite-v1',
            ('library.sqlite',), 'f'*64, self.value.purpose, profile_kind='workflow_inspection')
        self.authority, journal, head = synthetic_delivery(self.root/'mechanism', self.plan.request())
        self.addCleanup(head.close)
        self.addCleanup(journal.close)
        self.subject = registry.Subject('cohort', COHORT[0], 'M4', 'c'*64, exposure.BASE_SHA256,
            admission.source_sha256(self.files))
        self.registration = review.inspection_registration_for(self.files, self.subject, value=self.value,
            plan=self.plan, review_authority=self.authority, commit_oid=self.commit, tree_oid=self.tree,
            repetition_id='fresh-1', gate_id='workflow-inspection', cohort_trajectory_ids=COHORT)
        registered = review.inspection_observation_registration(self.registration)
        self.state = {'registration': registered, 'freeze': registry.CohortFreeze(
            tuple(replace(self.subject, trajectory_id=key) for key in COHORT), '1'*64, '2'*64, True)}
        self.issued = admission.ObservationAdmission(registered,
            verify_registration=lambda: self.state['registration'], verify_cohort=lambda: self.state['freeze'])
        self.raw, self.delta = self.root/'raw', self.root/'delta'
        self.head = ExternalHead.create(self.root/'head', journal_roots=(self.raw, self.delta))
        self.addCleanup(self.head.close)

    def owner(self, *, expected=None, mode='physical', delivery=None):
        owner = review.WorkflowInspectionExecution(self.raw, self.store, self.registration, value=self.value,
            plan=self.plan, review_authority=self.authority, admission_authority=self.issued,
            checkpoint_authority=self.head, delta_root=self.delta, expected_checkpoint=expected,
            mode=mode, product_delivery=delivery)
        self.addCleanup(owner.close)
        return owner

    def deliver(self, request, *, decisions=None, name='product'):
        delivery, journal, head = synthetic_delivery(self.root/name, request, product=True, decisions=decisions)
        self.addCleanup(head.close)
        self.addCleanup(journal.close)
        return delivery

    def test_fresh_begin_complete_original_rejected_and_unknown_are_preserved(self):
        owner = self.owner()
        request = owner.begin_review()
        self.assertFalse(owner.has_retained('terminal.json'))
        rows = [{'id': duty, 'decision': 'rejected' if i == 0 else 'unresolved' if i == 1 else 'approved',
            'rationale': 'Synthetic original', 'source_references': ['fixture:1']} for i, duty in enumerate(review.INSPECTION_DUTIES)]
        owner.complete_review(self.deliver(request, decisions=rows))
        original = owner.read_original()
        self.assertEqual([r['status'] for r in original['outcomes']][:2], ['failed','infrastructure_error'])
        self.assertFalse(original['engine_used'])
        self.assertFalse(original['mechanics']['cleanup_required'])
        self.assertEqual(original['cohort_freeze'], asdict(self.state['freeze']))

    def test_no_freeze_blocks_before_owner_journal_or_request(self):
        self.state['freeze'] = None
        with self.assertRaises(admission.AdmissionError):
            self.owner()
        self.assertFalse((self.raw/'review-request.json').exists())

    def test_post_ack_revocation_blocks_request_publication(self):
        owner = self.owner()
        retain = owner._retain
        def revoked(name, raw):
            retain(name, raw)
            if name == 'intent.json':
                self.state['registration'] = None
        owner._retain = revoked
        with self.assertRaises(admission.AdmissionError):
            owner.begin_review()
        self.assertTrue(owner.has_retained('intent.json'))
        self.assertFalse(owner.has_retained('review-request.json'))

    def test_unacknowledged_intent_exception_prohibits_request_or_retry(self):
        owner = self.owner()
        retain = owner._retain
        def failed(name, raw):
            retain(name, raw)
            if name == 'intent.json':
                raise chain.ChainUnknown('Fixture: acknowledged append whose return was lost')
        owner._retain = failed
        with self.assertRaises(chain.ChainUnknown):
            owner.begin_review()
        owner._retain = retain
        self.assertFalse(owner.has_retained('review-request.json'))
        with self.assertRaises(admission.AdmissionError):
            owner.begin_review()

    def test_preexisting_other_request_cannot_complete_new_inspection(self):
        old = self.deliver({'fixture': 'old report before actual request'})
        owner = self.owner()
        owner.begin_review()
        with self.assertRaises(admission.AdmissionError):
            owner.complete_review(old)
        self.assertFalse(owner.has_retained('terminal.json'))

    def test_freeze_revocation_after_independent_delivery_stops_completion(self):
        owner = self.owner()
        delivery = self.deliver(owner.begin_review())
        self.state['freeze'] = None
        with self.assertRaises(admission.AdmissionError):
            owner.complete_review(delivery)
        self.assertFalse(owner.has_retained('terminal.json'))

    def test_complete_cold_reopen_reconstructs_original_then_closed_owner_refuses(self):
        owner = self.owner()
        delivery = self.deliver(owner.begin_review())
        owner.complete_review(delivery)
        original = owner.read_original()
        owner.retain_verifier(review.encoded(original))
        prefix = owner.checkpoint()
        owner.close()
        reopened = self.owner(expected=prefix, delivery=delivery)
        self.assertEqual(reopened.read_original(), original)
        self.assertEqual(reopened.checkpoint(), prefix)
        reopened.close()
        with self.assertRaises(admission.AdmissionError):
            reopened.read_original()

    def test_fixture_mode_cannot_begin_complete_or_read_observation(self):
        owner = self.owner(mode='fixture')
        for action in (owner.begin_review, owner.read_original):
            with self.assertRaises(admission.AdmissionError):
                action()
        self.assertFalse(owner.has_retained('intent.json'))

    def test_duplicate_begin_or_completion_never_reuses_independent_observation(self):
        owner = self.owner()
        request = owner.begin_review()
        with self.assertRaises(admission.AdmissionError):
            owner.begin_review()
        delivery = self.deliver(request)
        owner.complete_review(delivery)
        with self.assertRaises(admission.AdmissionError):
            owner.complete_review(delivery)

    def test_new_delivery_prefix_revokes_already_completed_observation(self):
        owner = self.owner()
        delivery = self.deliver(owner.begin_review())
        owner.complete_review(delivery)
        delivery.journal.retain('late.json', b'{}')
        with self.assertRaises(chain.ChainError):
            owner.read_original()

    def test_changed_original_config_is_rejected_before_cold_parse(self):
        owner = self.owner()
        prefix = owner.checkpoint()
        owner.close()
        (self.raw/'config.json').write_bytes(b'{}')
        with self.assertRaises(chain.ChainError):
            self.owner(expected=prefix)

    def test_exact_registered_purpose_and_gate_cannot_be_relabelled(self):
        with self.assertRaises(admission.AdmissionError):
            replace(self.registration, gate=replace(self.registration.gate,
                binding=replace(self.registration.gate.binding, purpose='public_release')))
        with self.assertRaises(admission.AdmissionError):
            review.inspection_observation_registration(asdict(self.registration))
