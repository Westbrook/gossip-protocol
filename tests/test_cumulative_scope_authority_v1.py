"""Real local chain/head, fixture review delivery only; no actual scope approval.

Fabricated reviewer originals exercise the protected-enrollment mechanism. They
never qualify the incomplete real declaration or authorize a candidate run.
"""
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import unittest

from gossip_harness import candidate_checkpoint_chain_v1 as chain
from gossip_harness import candidate_checkpoint_head_v1 as head
from gossip_harness import candidate_scope_consumer_v1 as consumer
from gossip_harness import cumulative_scope_authority_v1 as authority
from gossip_harness import cumulative_scope_source_v1 as source
from gossip_harness import project_acceptance_compiler_v1 as compiler
from tests.test_candidate_checkpoint_chain_v1 import MemoryAuthority
from tests.test_project_acceptance_compiler_v1 import synthetic_declaration, subject
from tests.test_cumulative_scope_source_v1 import ROOT, cli_registration


def submission_fixture(catalog, component):
    declaration = source.assemble_declaration(catalog, synthetic_declaration(catalog.inventory).cohort,
                                             (component,), review_sha256='d' * 64)
    # Empty applicability is deliberately incomplete. No blanket N/A fixture can
    # accidentally escape as the real semantic ScopePlan.
    scope = compiler.ScopePlan(catalog.inventory.sha256, compiler.declaration_fingerprint(declaration),
        (), (), tuple(rule.id for rule in catalog.inventory.qualification_rules), (), (), ())
    return authority.ScopeSubmission(catalog, declaration, scope, component.gate.binding.subject, (component,), ())


class CumulativeScopeAuthorityV1Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.catalog = source.load_catalog(ROOT)
        cls.component = source.cli_slice(cli_registration())
        cls.submission = submission_fixture(cls.catalog, cls.component)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='gossip-scope-registration-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.raw, self.delta = self.root / 'raw', self.root / 'delta'
        self.external = head.ExternalHead.create(self.root / 'head', journal_roots=(self.raw, self.delta))
        self.addCleanup(self.external.close)
        self.context = {'protocol': authority.PROTOCOL, 'purpose': 'host-registration-mechanism-fixture'}
        self.chain = chain.CheckpointChain.create(self.raw, self.delta, context=self.context, authority=self.external)
        self.addCleanup(self.chain.close)
        self.owner = authority.ScopeRegistrationController(ROOT, self.chain, self.chain.commitment)

    def fixture_report(self, staged, *, changes=None, decide=None):
        request = json.loads(self.chain.read(staged.request_name))
        decisions = [{'target_id': target['id'], 'target_sha256': target['sha256'], 'decision': 'approved',
            'rationale': 'Synthetic mechanism fixture only; not an actual semantic reviewer assessment.',
            'inspected_references': ['fixture-only:' + target['id']]} for target in request['targets']]
        if decide is not None:
            decide(decisions)
        report = {'protocol': 'complete-scope-semantic-review-v1', 'purpose': authority.REVIEW_PURPOSE,
            'reviewer_id': 'fixture-reviewer', 'reviewed_request_sha256': staged.request_sha256,
            'decisions': decisions, 'limitations': ['Fixture report, never a production semantic review.']}
        report.update(changes or {})
        return report

    def install_fixture_report(self, staged, *, changes=None, decide=None):
        report = self.fixture_report(staged, changes=changes, decide=decide)
        return self.deliver(staged.request_sha256, report)

    def deliver(self, request_sha, report):
        raw = source.encoded(report)
        provenance = source.encoded({'protocol': 'scope-independent-review-delivery-v1',
            'reviewer_id': 'fixture-reviewer', 'role': 'independent_scope_reviewer',
            'report_name': 'fixture-report.json', 'report_sha256': source.sha(raw),
            'request_sha256': request_sha, 'delivery_reference': 'host fixture only, no real review'})
        self.chain.retain('fixture-report.json', raw)
        self.chain.retain('fixture-delivery.json', provenance)
        enrollment = authority.ReviewEnrollment('fixture-reviewer', 'independent_scope_reviewer',
            'fixture-report.json', source.sha(raw), 'fixture-delivery.json', source.sha(provenance))
        # Fresh host controller receives the independently observed post-delivery
        # head. The old snapshot must not adopt it by reading candidate receipts.
        self.owner = authority.ScopeRegistrationController(ROOT, self.chain, self.chain.commitment,
                                                           reviewer_enrollments=(enrollment,))
        return report

    def test_request_preserves_full_source_review_targets_and_original_purposes(self):
        request = self.submission.request()
        targets = request['targets']
        for prefix, count in (('obligation:', 312), ('authority:', 22), ('gap:', 188)):
            self.assertEqual(sum(row['id'].startswith(prefix) for row in targets), count)
        self.assertEqual(sum(row['id'].startswith('purpose:') for row in targets), len(self.catalog.inventory.logical_gates))
        self.assertEqual(len(request['declaration']['obligations']), 312)
        self.assertEqual(len(request['declaration']['cohort']['trajectories']), 6)
        self.assertEqual(request['declaration']['cohort']['milestones'], ('M1', 'M2', 'M3', 'M4'))
        self.assertFalse(request['acceptance_authority'])
        self.assertEqual(request['candidate_observations'], 'none')
        self.assertIn('no applicability', request['ownership_review_scope'])
        self.assertEqual(len(request['implementation_sources']), 9)
        self.assertFalse(any('decision' in row for row in targets))
        self.assertTrue(all(row['sha256'] == source.sha(source.encoded(row['value'])) for row in targets))

    def test_stage_is_durable_replayable_and_incomplete_remains_blocked(self):
        staged = self.owner.stage(self.submission)
        self.assertEqual(staged.request_position, 1)
        self.assertTrue(staged.design.blockers)
        self.assertIsNone(staged.design.registry)
        expected = self.chain.commitment
        self.assertEqual(self.owner.stage(self.submission), staged)
        self.assertEqual(self.chain.commitment, expected)
        self.chain.close(); self.external.close()
        reopened_head = head.ExternalHead.reopen(self.root / 'head', journal_roots=(self.raw, self.delta), expected=expected)
        self.addCleanup(reopened_head.close)
        reopened = chain.CheckpointChain.reopen(self.raw, self.delta, context=self.context,
                                                authority=reopened_head, expected=expected)
        self.addCleanup(reopened.close)
        owner = authority.ScopeRegistrationController(ROOT, reopened, expected)
        self.assertEqual(owner.stage(self.submission), staged)
        self.assertEqual(reopened.commitment, expected)

    def test_memory_head_cannot_be_a_production_registration_controller(self):
        journal = chain.CheckpointChain.create(self.root / 'memory-raw', self.root / 'memory-delta',
                                               context=self.context, authority=MemoryAuthority())
        self.addCleanup(journal.close)
        with self.assertRaisesRegex(consumer.AuthorityError, 'external checkpoint'):
            authority.ScopeRegistrationController(ROOT, journal, journal.commitment)

    def test_no_review_enrollment_or_ownership_only_report_cannot_approve_scope(self):
        staged = self.owner.stage(self.submission)
        with self.assertRaises(authority.RegistrationMissing):
            self.owner.authenticate_review(staged, report_name='not-delivered.json')
        self.install_fixture_report(staged, changes={'purpose': 'independent_normative_review_of_ownership_design'})
        with self.assertRaisesRegex(consumer.AuthorityError, 'Wrong original review scope'):
            self.owner.authenticate_review(staged, report_name='fixture-report.json')

    def test_enrolled_original_delivery_authenticates_only_the_mechanism(self):
        staged = self.owner.stage(self.submission)
        report = self.install_fixture_report(staged)
        self.assertEqual(self.owner.authenticate_review(staged, report_name='fixture-report.json'), report)
        with self.assertRaises(authority.RegistrationMissing):
            self.owner.register(self.submission, report_name='fixture-report.json')
        snapshot = self.owner.open_snapshot()
        self.assertIsNone(snapshot.registration(self.submission.subject))
        self.assertIsNone(snapshot.registration_provenance(self.submission.subject))
        self.assertIsNone(snapshot.freeze())
        self.assertIsNone(snapshot.promotion(self.submission.subject))
        self.assertIsNone(snapshot.observation(self.component.gate, None))

    def test_self_declared_pass_flag_does_not_replace_exact_review_census(self):
        staged = self.owner.stage(self.submission)
        self.install_fixture_report(staged, changes={'passed': True})
        with self.assertRaisesRegex(consumer.AuthorityError, 'Wrong original review scope'):
            self.owner.authenticate_review(staged, report_name='fixture-report.json')

    def test_missing_review_target_cannot_approve_a_reduced_denominator(self):
        staged = self.owner.stage(self.submission)
        self.install_fixture_report(staged, decide=lambda rows: rows.pop())
        with self.assertRaisesRegex(consumer.AuthorityError, 'Incomplete semantic review'):
            self.owner.authenticate_review(staged, report_name='fixture-report.json')

    def test_reordered_targets_and_changed_target_digest_are_rejected(self):
        staged = self.owner.stage(self.submission)
        def substitute(rows):
            rows[0], rows[1] = rows[1], rows[0]
            rows[1]['target_sha256'] = 'f' * 64
        self.install_fixture_report(staged, decide=substitute)
        with self.assertRaisesRegex(consumer.AuthorityError, 'Substituted, vague or reordered'):
            self.owner.authenticate_review(staged, report_name='fixture-report.json')

    def test_rejected_and_unresolved_decisions_remain_distinguishable(self):
        staged = self.owner.stage(self.submission)
        def pending(rows):
            rows[0]['decision'] = 'rejected'
            rows[1]['decision'] = 'unresolved'
        self.install_fixture_report(staged, decide=pending)
        with self.assertRaises(authority.RegistrationMissing) as caught:
            self.owner.authenticate_review(staged, report_name='fixture-report.json')
        self.assertIn(':rejected', str(caught.exception))
        self.assertIn(':unresolved', str(caught.exception))

    def test_wrong_request_cannot_be_relabeled_by_enrollment(self):
        staged = self.owner.stage(self.submission)
        self.install_fixture_report(staged, changes={'reviewed_request_sha256': 'f' * 64})
        with self.assertRaisesRegex(consumer.AuthorityError, 'Wrong original review scope'):
            self.owner.authenticate_review(staged, report_name='fixture-report.json')

    def test_report_must_follow_the_actual_retained_prospective_request(self):
        request = self.submission.request()
        digest = source.sha(source.encoded(request))
        # Deliberately write a correctly bound report before the request exists.
        self.deliver(digest, {'protocol': 'complete-scope-semantic-review-v1', 'purpose': authority.REVIEW_PURPOSE,
            'reviewer_id': 'fixture-reviewer', 'reviewed_request_sha256': digest, 'decisions': [], 'limitations': []})
        staged = self.owner.stage(self.submission)
        with self.assertRaisesRegex(consumer.AuthorityError, 'Review chronology'):
            self.owner.authenticate_review(staged, report_name='fixture-report.json')

    def test_recomputed_hashes_do_not_turn_synthetic_selectors_into_real_coverage(self):
        inv = self.catalog.inventory
        declaration = synthetic_declaration(inv)
        owners = {unit.obligation.id: unit.requirement_ids for unit in self.catalog.units}
        plans = []
        for row in declaration.obligations:
            role = next(unit.role for unit in inv.obligations if unit.id == row.obligation_id)
            keys = tuple(gate.id for gate in inv.logical_gates if gate.role == role and set(gate.requirement_ids) & set(owners[row.obligation_id]))
            plans.append(replace(row, requirement_ids=owners[row.obligation_id],
                assertions=tuple(replace(assertion, logical_gate_ids=keys) for assertion in row.assertions)))
        declaration = replace(declaration, obligations=tuple(plans))
        empty_scope = compiler.ScopePlan(inv.sha256, compiler.declaration_fingerprint(declaration), (), (),
                                         tuple(rule.id for rule in inv.qualification_rules), (), (), ())
        synthetic = authority.ScopeSubmission(self.catalog, declaration, empty_scope,
                                               subject(inv), (), ())
        staged = self.owner.stage(synthetic)
        self.assertTrue(staged.missing_executable_edges)
        with self.assertRaises(authority.RegistrationMissing):
            self.owner.register(synthetic, report_name='invented-original.json')

    def test_changed_source_catalog_is_invalid_not_a_product_failure(self):
        altered = replace(self.submission, catalog=replace(self.catalog, units=self.catalog.units[:-1]))
        before = self.chain.commitment
        with self.assertRaisesRegex(consumer.AuthorityError, 'Full original source'):
            self.owner.stage(altered)
        self.assertEqual(self.chain.commitment, before)

    def test_snapshot_cannot_adopt_a_later_review_head(self):
        staged = self.owner.stage(self.submission)
        snapshot = self.owner.open_snapshot()
        self.install_fixture_report(staged)
        with self.assertRaises(consumer.AuthorityUnavailable):
            snapshot.check_current(snapshot.checkpoint)

    def test_tampered_retained_request_blocks_current_scope_capability(self):
        staged = self.owner.stage(self.submission)
        snapshot = self.owner.open_snapshot()
        path = self.raw / staged.request_name
        value = bytearray(path.read_bytes()); value[-2] ^= 1; path.write_bytes(value)
        with self.assertRaises(consumer.AuthorityUnavailable):
            snapshot.registration(self.submission.subject)
        self.assertTrue(self.chain.uncertain)

    def test_registration_replay_must_follow_delivery_not_only_report(self):
        self.chain.retain('review.json', b'{}')
        self.chain.retain('registration.json', b'{}')
        self.chain.retain('delivery.json', b'{}')
        current = authority.ScopeRegistrationController(ROOT, self.chain, self.chain.commitment)
        with self.assertRaisesRegex(consumer.AuthorityError, 'predates independently retained delivery'):
            current._registration_order('review.json', 'delivery.json', 'registration.json')
        self.chain.retain('later-registration.json', b'{}')
        current = authority.ScopeRegistrationController(ROOT, self.chain, self.chain.commitment)
        current._registration_order('review.json', 'delivery.json', 'later-registration.json')
