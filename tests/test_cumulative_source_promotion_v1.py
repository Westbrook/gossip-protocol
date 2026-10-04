"""Isolated original-reader controls, not complete study or semantic approval.

Finance/Git controls use actual disposable originals. Authority controls use
explicit mechanics stubs and synthetic independent review reports only.
"""
from copy import deepcopy
from dataclasses import asdict, replace
import hashlib
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from gossip_harness import cumulative_source_promotion_v1 as promotion
from gossip_harness import cumulative_prerequisite_review_v1 as review
from gossip_harness import candidate_scope_consumer_v1 as consumer
from gossip_harness import project_acceptance_registry_v1 as registry
from gossip_harness.candidate_checkpoint_chain_v1 import CheckpointChain
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from gossip_harness.cumulative_final_acceptance_v2 import FinalAcceptanceV2
from gossip_harness.cumulative_study_controller_v2 import Records, PACKAGES, digest
from gossip_harness.financial_rehearsal_originals_v1 import FinancialOriginals, OriginalFile
from gossip_harness.gitstore import GitStore
from gossip_harness.peer_financial_authority_v2 import FinancialError, canonical_payload, ledger_identity
from gossip_harness.peer_financial_authority_v5 import CumulativeAuthorityV5
from gossip_harness.peer_financial_terminal_v1 import sha
from gossip_harness.peer_mesh_finance_v2 import MeshFinancePayloads
from gossip_harness.peer_mesh_v2 import MeshConfig, MeshNode
from gossip_harness.peer_project_contract_v2 import ActionRequest
from gossip_harness.verification_journal import RequestJournal
from tests.financial_v5_fixture import Fixture


def chain(test, root, name):
    base = root / name
    base.mkdir()
    head = ExternalHead.create(base / 'head', journal_roots=(base / 'raw', base / 'delta'))
    test.addCleanup(head.close)
    value = CheckpointChain.create(base / 'raw', base / 'delta', context={'fixture': name}, authority=head)
    test.addCleanup(value.close)
    return value


class CumulativeSourcePromotionFinanceV1Tests(Fixture, unittest.TestCase):
    def setUp(self):
        self.nodes, self.real_payloads = [], None
        super().setUp()
        roster = self.child.actors + ('finance',)
        self.sender = MeshNode(MeshConfig(self.root / 'sender', self.actor, self.child.cohort,
            self.context.execution_contract_sha256, roster, 't' * 32))
        self.finance_mesh = MeshNode(MeshConfig(self.root / 'finance-mesh', 'finance', self.child.cohort,
            self.context.execution_contract_sha256, roster, 't' * 32))
        self.nodes.extend((self.sender, self.finance_mesh))
        self.real_payloads = MeshFinancePayloads(self.root / 'payload-index', self.finance_mesh, self.child.actors)
        self.payloads = self.real_payloads

    def cleanup(self):
        if self.authority is not None:
            self.authority.close()
            self.authority = None
        if self.real_payloads is not None:
            self.real_payloads.close()
        for node in self.nodes:
            node.close()
        super().cleanup()

    def action(self, number=0):
        task = self.authority.task_id(self.context, self.works[number])
        raw = canonical_payload({'worker_request': {'task_id': task, 'instructions': 'Fix source.',
            'allowed_paths': ['src/a.py'], 'files': {'src/a.py': 'broken\n'}, 'base_sha': 'c' * 40,
            'attempt': 1, 'feedback': ''}, 'view_manifest_sha256': 'd' * 64})
        ref = self.sender.publish('worker-request', raw, 'request-' + str(number))
        notice = self.sender.store.notice(ref)
        self.finance_mesh.store.merge([notice])
        self.assertTrue(self.finance_mesh.want(ref))
        for index in range(len(notice['chunks'])):
            self.finance_mesh.store.accept_chunk(ref.payload_sha256, index,
                self.sender.store.read_chunk(ref.payload_sha256, index))
        return ActionRequest(self.context, 'action' + str(number), 'request' + str(number), self.actor,
                             'build', self.works[number], 'mini', ref, 'd' * 64)

    def originals(self, failed_first=False):
        self.open()
        replies = []
        if failed_first:
            self.transport.proposal = {'changes': [], 'summary': 'Known failed proposal'}
            action, _ = self.start(0)
            replies.append(self.terminal(action))
            self.assertEqual(replies[-1].state, 'failed')
            self.transport.proposal = {'changes': [{'path': 'src/a.py', 'content': 'fixed\n'}], 'summary': 'Repair'}
        action, _ = self.start(1 if failed_first else 0)
        replies.append(self.terminal(action))
        self.assertEqual(replies[-1].state, 'completed')
        self.seal()
        config, pin = self.authority.config, self.authority.config_sha256
        self.authority.close()
        self.authority = None
        self.real_payloads.close()
        for node in self.nodes:
            node.close()
        journals = tuple(OriginalFile(str(path), sha(path.read_bytes()), path.stat().st_size)
            for path in sorted(Path(self.contract['journal_root']).glob('*.json')))
        return FinancialOriginals(ledger_identity(self.path), self.child.cohort, config, pin,
            ledger_identity(self.root / 'payload-index/payload-index.sqlite'), self.real_payloads.config,
            ledger_identity(self.root / 'finance-mesh/mesh.sqlite'), self.finance_mesh.config.identity(),
            journals, tuple(row.binding for row in replies), tuple(replies))

    def dumps(self, originals):
        result = []
        for expected in (originals.ledger_identity, originals.payload_index_identity, originals.mesh_database_identity):
            with sqlite3.connect(Path(expected['path']).as_uri() + '?mode=ro', uri=True) as db:
                result.append(tuple(db.iterdump()))
        return result

    def test_v5_repaired_failure_and_cost_are_read_without_owner_or_writes(self):
        originals = self.originals(True)
        before = self.dumps(originals)
        with patch.object(CumulativeAuthorityV5, '__init__', side_effect=AssertionError('No owner')), \
                patch.object(RequestJournal, '__init__', side_effect=AssertionError('No journal owner')), \
                patch.object(MeshFinancePayloads, '__init__', side_effect=AssertionError('No payload owner')):
            proof = promotion._audit_finance_v5(originals)
        self.assertEqual((proof.admitted, proof.known_failures), (2, 1))
        self.assertEqual(proof.spent_micro_usd, sum(row.usage_units for row in originals.terminal_replies))
        self.assertFalse(proof.live_qualification)
        self.assertEqual(self.dumps(originals), before)

    def test_failed_action_cannot_disappear_from_role_financial_census(self):
        originals = self.originals(True)
        with self.assertRaises(FinancialError):
            promotion._audit_finance_v5(replace(originals, dispatches=originals.dispatches[1:],
                                                terminal_replies=originals.terminal_replies[1:]))

    def test_v4_protocol_cannot_be_relabelled_as_v5(self):
        originals = self.originals()
        config = {**originals.config, 'protocol': 'peer-financial-authority-v4'}
        with self.assertRaises(FinancialError):
            promotion._audit_finance_v5(replace(originals, config=config, config_sha256=digest(config)))

    def test_canonical_v5_settlement_bytes_are_required_even_when_manifest_rehashed(self):
        originals = self.originals()
        changed = []
        for item in originals.journal_files:
            path = Path(item.path)
            if path.name.endswith('.settled.json'):
                path.write_bytes(b' ' + path.read_bytes())
            changed.append(OriginalFile(item.path, sha(path.read_bytes()), path.stat().st_size))
        with self.assertRaises(FinancialError):
            promotion._audit_finance_v5(replace(originals, journal_files=tuple(changed)))


class CumulativeSourcePromotionGitV1Tests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='source-promotion-git-')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.chain = chain(self, self.root, 'proof')
        self.records = Records(self.chain)
        self.scopes = {name: (name + '.py',) for name in PACKAGES}
        self.files = {name + '.py': 'initial\n' for name in PACKAGES}
        self.store = GitStore.create(self.root / 'protected.git', self.files)
        self.old = self.store.head()
        self.stage = 'child.fixture.M4.g0'

    def retain_integrated(self):
        self.builds, self.reviews, selections, merges = [], [], [], []
        self.records.put(self.stage + '.reviews', {'fixture': 'Original reviewer group marker'})
        for i, package in enumerate(PACKAGES):
            actor = 'fixture.B' + str(i)
            builder = {'actor': actor, 'snapshot': {'reply': {'state': 'completed'}},
                'worker_request': {'base_sha': self.old, 'files': self.files},
                'result_payload': {'payload': {'changes': {package + '.py': 'selected\n'}}},
                'original_ref': {'fixture_builder': i}}
            # Use registered package_for actor names from the concrete controller.
            actor = ['fixture.B01', 'fixture.B05', 'fixture.B09', 'fixture.B13'][i]
            builder['actor'] = actor
            reviewer = ['fixture.R1', 'fixture.R2', 'fixture.R3', 'fixture.R4'][i]
            decision = {'stage_id': self.stage, 'package': package, 'selected_actor': actor,
                        'reasons': 'Synthetic scoped review', 'tests': ['Unexecuted suggestion']}
            reviewed = {'actor': reviewer, 'snapshot': {'reply': {'state': 'completed'}},
                'result_payload': {'payload': {'changes': {'decision.json': canonical_payload(decision).decode()}}},
                'original_ref': {'fixture_review': i}}
            self.builds.append(builder)
            self.reviews.append(reviewed)
            selections.append({'package': package, 'reviewer': reviewer, 'selected_actor': actor,
                'proposal_ref': builder['original_ref'], 'review_ref': reviewed['original_ref'], 'decision': decision})
            key = self.stage + '.merge.' + package
            pin = digest(builder)
            private_path = self.root / 'private-git' / hashlib.sha256((self.stage + actor).encode()).hexdigest()
            self.records.put(key + '.proposal-intent', {'proposal_sha256': pin, 'base_sha': self.old, 'git_path': str(private_path)})
            private = GitStore.fork(self.store, private_path)
            offered = private.propose(builder['result_payload']['payload']['changes'], self.old)
            proposal = {'proposal_sha256': pin, 'base_sha': self.old, 'git_path': str(private_path), 'offered_sha': offered}
            self.records.put(key + '.proposal', proposal)
            candidate = self.store.prepare(private, offered, self.store.head(), lambda _p: (True, 'Fixture mechanics'), self.scopes[package])
            self.records.put(key + '.git-intent', {'proposal_sha256': pin, 'candidate': asdict(candidate)})
            self.assertIn(self.store.accept(candidate).status, ('accepted', 'noop'))
            files = {name: raw.encode() for name, raw in self.store.read_files().items()}
            result = {'proposal_sha256': pin, 'private_git': proposal, 'commit_oid': self.store.head(),
                      'source_sha256': promotion.admission.source_sha256(files)}
            self.records.put(key, result)
            merges.append(result)
        self.records.put(self.stage + '.integration', {'status': 'integrated', 'commit_oid': self.store.head(),
            'source_sha256': promotion.admission.source_sha256(files), 'selections': selections,
            'private_git_merges': merges, 'acceptance_authority': False})

    def audit(self):
        return promotion._integration(promotion._TrackedOriginals(self.chain), SimpleNamespace(package_paths=self.scopes),
            0, self.stage, tuple(self.builds), tuple(self.reviews), self.store, self.old,
            {name: raw.encode() for name, raw in self.files.items()})

    def test_actual_four_private_git_cas_history_reads_without_replaying_mutations(self):
        self.retain_integrated()
        before = self.chain.commitment
        with patch.object(GitStore, 'accept', side_effect=AssertionError('Read only')), \
                patch.object(GitStore, 'prepare', side_effect=AssertionError('Read only')):
            commit, files = self.audit()
        self.assertEqual(commit, self.store.head())
        self.assertEqual(files, {name: b'selected\n' for name in self.files})
        self.assertEqual(self.chain.commitment, before)

    def test_altered_selected_changes_cannot_reuse_existing_git_intents(self):
        self.retain_integrated()
        self.builds[0]['result_payload']['payload']['changes'][PACKAGES[0] + '.py'] = 'forged\n'
        with self.assertRaises(FinancialError):
            self.audit()


class CumulativeSourcePromotionAuthorityV1Tests(unittest.TestCase):
    def setUp(self):
        from tests.test_cumulative_terminal_originals_v2 import synthetic_plan
        temporary = tempfile.TemporaryDirectory(prefix='source-promotion-authority-')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.study, self.journal, self.scope, self.review_chain = [chain(self, self.root, name)
            for name in ('study', 'final', 'scope', 'review')]
        self.reviews = review.OriginalReviewAuthority(self.review_chain, self.review_chain.commitment)
        original = synthetic_plan()
        runtime = {**original.runtime, 'final_promotion_protocol': promotion.PROTOCOL}
        limits = {'horizon_seconds': original.horizon_seconds, 'source_generations': original.source_generations,
            'partition_seconds': original.partition_seconds, 'executor_slots': original.executor_slots, 'runtime': runtime}
        plan = replace(original, runtime=runtime, source_pins={**original.source_pins, **promotion.implementation_sources()},
            cohort=replace(original.cohort, resource_contract_sha256=digest(limits)))
        self.owner = object.__new__(FinalAcceptanceV2)
        self.owner.plan, self.owner.repository = plan, Path(promotion.__file__).resolve().parents[1]
        self.owner.chain, self.owner.expected, self.owner.records = self.journal, self.journal.commitment, Records(self.journal)
        self.owner.study_chain, self.owner.study_expected = self.study, self.study.commitment
        self.owner.scope_owner = SimpleNamespace(chain=self.scope)
        subjects = tuple(registry.Subject(plan.cohort.cohort_id, child.trajectory, 'M4', plan.sha256, 'b' * 64, 'c' * 64)
                         for child in plan.roster.children)
        self.subject = subjects[0]
        self.owner.subjects = subjects
        self.owner.freeze = registry.CohortFreeze(subjects, 'd' * 64, 'e' * 64, True)
        self.owner.originals = SimpleNamespace(freeze_eligible=True)
        self.owner.submissions = {row.trajectory_id: SimpleNamespace(subject=row) for row in subjects}
        self.context = {'protocol': promotion.PROTOCOL, 'subject': asdict(self.subject), 'policy': promotion.POLICY,
            'revision': {'commit_oid': 'a' * 40, 'tree_oid': 'b' * 40, 'source_sha256': self.subject.source_sha256},
            'study_checkpoint': asdict(self.study.commitment), 'package_reviewers': ['fixture.R1'],
            'role_actors': ['fixture.B01', 'fixture.R1'], 'repository': str(self.root / 'candidate' / 'protected.git'),
            'explicit_unit_mechanics_stub': True}
        for method in ('_current', '_current_originals'):
            stub = patch.object(FinalAcceptanceV2, method)
            stub.start(); self.addCleanup(stub.stop)
        stub = patch.object(promotion, '_joined_originals', side_effect=lambda *_: deepcopy(self.context))
        stub.start(); self.addCleanup(stub.stop)
        # The loaded-definition guard correctly rejects the explicit mechanics
        # stubs above. These tests isolate journal/review behavior, not source qualification.
        stub = patch.object(promotion.admission, 'verify_loaded_sources')
        stub.start(); self.addCleanup(stub.stop)
        self.authority = promotion.SourcePromotion(self.owner, reviews=self.reviews)

    def deliver(self, decision='accept', reviewer='independent-reviewer'):
        context = self.authority._context(self.subject)
        request = self.reviews.original(purpose='candidate_source_promotion', role='integration',
            source=consumer.Revision(**context['revision']), context=context)
        report = {'protocol': review.PROTOCOL, 'kind': 'independent-review-report', 'reviewer_id': reviewer,
            'purpose': 'candidate_source_promotion', 'role': 'integration', 'request_sha256': request.sha256,
            'source': context['revision'], 'decisions': [{'duty': duty, 'decision': decision,
                'rationale': 'Synthetic unit review only', 'inspected_references': [request.name]}
                for duty in review.DUTIES[('candidate_source_promotion', 'integration')]], 'limitations': ['Unit fixture']}
        raw = review.encoded(report)
        self.review_chain.retain('unit-report.json', raw)
        delivery = {'protocol': review.PROTOCOL, 'kind': 'independent-review-delivery', 'reviewer_id': reviewer,
            'purpose': 'candidate_source_promotion', 'role': 'integration', 'request_sha256': request.sha256,
            'report_name': 'unit-report.json', 'report_sha256': sha(raw), 'delivery_reference': 'Explicit unit fixture'}
        delivery_raw = review.encoded(delivery)
        self.review_chain.retain('unit-delivery.json', delivery_raw)
        enrollment = review.ReviewEnrollment(reviewer, 'integration', 'candidate_source_promotion',
            'unit-report.json', sha(raw), 'unit-delivery.json', sha(delivery_raw))
        self.reviews = review.OriginalReviewAuthority(self.review_chain, self.review_chain.commitment, enrollments=(enrollment,))
        self.authority = promotion.SourcePromotion(self.owner, reviews=self.reviews)

    def test_missing_review_never_promotes_or_autogenerates_decision(self):
        self.authority.stage(self.subject)
        before = self.journal.commitment, self.review_chain.commitment
        with self.assertRaises(consumer.AuthorityUnavailable):
            self.authority.verify_and_retain(self.subject)
        self.assertIsNone(self.authority.promotion(self.subject))
        self.assertEqual(before, (self.journal.commitment, self.review_chain.commitment))

    def test_original_accept_is_retained_then_graded_without_writes(self):
        self.authority.stage(self.subject)
        self.deliver()
        result = self.authority.verify_and_retain(self.subject)
        self.assertEqual(result.status, 'promoted')
        before = self.journal.commitment, self.review_chain.commitment
        with patch.object(self.owner, '_put', side_effect=AssertionError('No grading writes')), \
                patch.object(self.reviews, 'stage', side_effect=AssertionError('No grading stages')):
            self.assertEqual(self.authority.promotion(self.subject), result)
        self.assertEqual(before, (self.journal.commitment, self.review_chain.commitment))

    def test_rejection_is_preserved_as_rejection(self):
        self.authority.stage(self.subject)
        self.deliver('reject')
        self.assertEqual(self.authority.verify_and_retain(self.subject).status, 'rejected')

    def test_unresolved_review_never_becomes_promoted(self):
        self.authority.stage(self.subject)
        self.deliver('unresolved')
        with self.assertRaises(consumer.AuthorityUnavailable):
            self.authority.verify_and_retain(self.subject)
        self.assertIsNone(self.authority.promotion(self.subject))

    def test_package_reviewer_cannot_supply_independent_integration_approval(self):
        self.authority.stage(self.subject)
        self.deliver(reviewer='fixture.R1')
        with self.assertRaisesRegex(consumer.AuthorityError, 'independent'):
            self.authority.verify_and_retain(self.subject)

    def test_changed_mechanics_cannot_reuse_a_delivered_review(self):
        self.authority.stage(self.subject)
        self.deliver()
        self.context['revision']['commit_oid'] = 'f' * 40
        with self.assertRaisesRegex(consumer.AuthorityError, 'history changed'):
            self.authority.verify_and_retain(self.subject)

    def test_policy_marker_and_full_source_closure_are_mandatory(self):
        del self.owner.plan.runtime['final_promotion_protocol']
        with self.assertRaisesRegex(consumer.AuthorityError, 'prospectively declared'):
            self.authority.promotion(self.subject)

    def test_old_final_owner_cannot_supply_new_promotion_capability(self):
        from gossip_harness.cumulative_final_acceptance_v1 import FinalAcceptance
        with self.assertRaisesRegex(consumer.AuthorityError, 'Exact final V2'):
            promotion.SourcePromotion(object.__new__(FinalAcceptance), reviews=self.reviews)


class CumulativeSourcePromotionHistoryV1Tests(unittest.TestCase):
    """Synthetic four-milestone source model; no physical execution credit."""
    def fixture(self, **changes):
        from tests.test_cumulative_rehearsal_validator_v1 import CumulativeRehearsalValidatorV1Tests
        from tests.test_cumulative_terminal_originals_v2 import synthetic_plan
        helper = CumulativeRehearsalValidatorV1Tests()
        helper.setUp()
        self.addCleanup(helper.doCleanups)
        helper.plan = synthetic_plan()
        helper.child, helper.trajectory = helper.plan.roster.children[0], helper.plan.cohort.trajectories[0]
        helper.retain_history(**changes)
        return helper

    def audit(self, helper):
        from unittest.mock import Mock
        from gossip_harness.cumulative_study_controller_v1 import StudyController as OldController
        def store(path):
            return Mock(path=path, head=lambda: helper.source['commit_oid'], is_ancestor=lambda *_: True,
                _introduced_paths=lambda old, offered: helper.git_changed[(old, offered)],
                _candidate_ref=lambda old, new: 'refs/harness/candidates/' + old + '/' + new,
                _git=lambda *args: args[-1].rsplit('/', 1)[-1])
        capture = lambda _store, oid: ('d' * 40, helper.git_files[oid])
        with patch.object(promotion, 'GitStore', side_effect=store), \
                patch.object(promotion, 'capture_git_source', side_effect=capture), \
                patch('gossip_harness.cumulative_rehearsal_validator_v1.GitStore', side_effect=store), \
                patch('gossip_harness.cumulative_rehearsal_validator_v1.capture_git_source', side_effect=capture), \
                patch.object(OldController, '_directives', side_effect=AssertionError('Never use old V1 producer')):
            return promotion._histories_v2(promotion._TrackedOriginals(helper.chain), helper.plan, 0,
                                          helper.root / 'protected.git')

    def test_complete_v2_history_uses_actual_v2_directive_producer(self):
        helper = self.fixture()
        rows, terminal = self.audit(helper)
        self.assertEqual(set(rows), set(helper.child.actors))
        self.assertTrue(all(len(values) == 4 for values in rows.values()))
        self.assertEqual(terminal['status'], 'completed')

    def test_public_pass_flag_without_original_case_census_is_rejected(self):
        helper = self.fixture(output_wrong=True)
        with self.assertRaises(FinancialError):
            self.audit(helper)

    def test_generation_inheritance_cannot_be_relabelled_at_final_source(self):
        helper = self.fixture(broken_source=True)
        with self.assertRaises(FinancialError):
            self.audit(helper)
