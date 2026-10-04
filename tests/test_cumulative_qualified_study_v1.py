"""Issued-entry lifecycle controls with explicit issuance/runtime stubs only.

Real disposable checkpoints and component-issued grants are exercised; complete
semantic auditors are explicitly stubbed. No complete qualification, financial
owner, provider, role process, Git operation or Docker execution is performed.
"""
from contextlib import ExitStack
from dataclasses import asdict, replace
from pathlib import Path
import platform
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

from gossip_harness import cumulative_qualified_study_v1 as entry
from gossip_harness import cumulative_issued_qualification_v1 as issued
from gossip_harness import cumulative_study_runtime_v2 as runtime
from gossip_harness.candidate_checkpoint_chain_v1 import CheckpointChain
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from gossip_harness.cumulative_study_controller_v2 import Records, StudyController, StudyPlan, StudyError, digest, plain
from gossip_harness.ledger import Ledger
from gossip_harness.peer_financial_authority_v2 import ledger_identity, FinancialError
from gossip_harness.peer_financial_authority_v3 import profile_manifest
from gossip_harness.worker import OpenAIWorker
from tests.test_cumulative_terminal_originals_v2 import synthetic_plan


class CumulativeQualifiedStudyV1Tests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name).resolve()
        self.repository = Path(__file__).resolve().parents[1]
        self.workers = {name: OpenAIWorker('fixture-never-a-real-key', max_output_tokens=64, timeout=2)
                        for name in ('mini', 'strong')}
        base = synthetic_plan()
        identity = {**base.runtime, 'python_executable': sys.executable, 'python_version': platform.python_version(),
            'platform': platform.platform(), 'terminal_drain_seconds': 1,
            'final_acceptance_protocol': 'cumulative-final-acceptance-v3',
            'study_successor_protocol': 'cumulative-study-successor-v3',
            'prerequisite_qualification_protocol': 'cumulative-prerequisite-qualification-v1',
            'final_promotion_protocol': 'cumulative-source-promotion-v1',
            'live_qualification_protocol': issued.PROTOCOL, 'qualified_study_protocol': entry.PROTOCOL,
            'final_acceptance_financial_mode': 'live',
            'final_acceptance_roots': {name: str(self.root / ('final-' + name)) for name in ('raw', 'delta', 'head')}}
        limits = {'horizon_seconds': base.horizon_seconds, 'source_generations': base.source_generations,
            'partition_seconds': base.partition_seconds, 'executor_slots': base.executor_slots, 'runtime': identity}
        self.plan = replace(base, runtime=identity, cohort=replace(base.cohort,
            resource_contract_sha256=digest(limits),
            model_profiles_sha256=digest({name: profile_manifest(worker) for name, worker in self.workers.items()})))
        head = ExternalHead.create(self.root / 'study-head', journal_roots=(self.root / 'study-raw', self.root / 'study-delta'))
        self.addCleanup(head.close)
        self.chain = CheckpointChain.create(self.root / 'study-raw', self.root / 'study-delta',
            context={'fixture': 'entry lifecycle only'}, authority=head)
        self.addCleanup(self.chain.close)
        grant = ExternalHead.create(self.root / 'grant-head', journal_roots=(self.root / 'grant-raw', self.root / 'grant-delta'))
        self.addCleanup(grant.close)
        ledger = self.root / 'original-ledger.sqlite'
        ledger.write_bytes(b'identity fixture only; never opened as a wallet')
        self.args = {'qualification_reference': {'path': str(self.root / 'never-read-capsule.json'), 'sha256': 'a' * 64},
            'grant_root': self.root / 'grant-raw', 'grant_delta_root': self.root / 'grant-delta', 'grant_authority': grant,
            'output': self.root / 'study-output', 'repository': self.repository, 'checkpoint': self.chain,
            'expected_checkpoint': self.chain.commitment, 'existing_ledger_path': ledger,
            'expected_ledger_identity': ledger_identity(ledger), 'workers': self.workers, 'mode': 'live',
            'permit_provider': Mock(side_effect=AssertionError('No permit invented or requested')),
            'evaluator': runtime.DockerPublicChecks(self.plan.runtime['image'], timeout=180)}
        # This object is explicitly not an issued authority. Its lifecycle
        # methods are patched below to test forwarding/order, never admission.
        self.session = object.__new__(issued.IssuedQualification)
        self.events = []

    def validate(self, **changes):
        args = {**self.args, **changes}
        args.pop('qualification_reference')
        with patch.object(entry.successor, 'validate_successor', return_value={}):
            entry._validate_inputs(self.plan, **args)

    def boundaries(self, *, issue_error=None, run_error=None, revoke_error=None, guard_error=None):
        stack = ExitStack()
        self.addCleanup(stack.close)
        stack.enter_context(patch.object(entry.successor, 'validate_successor', return_value={}))
        stack.enter_context(patch.object(StudyPlan, 'verify_sources'))
        stack.enter_context(patch.object(entry, '_validate_reference'))
        def issue(*args, **kwargs):
            self.events.append('issue')
            if issue_error is not None:
                raise issue_error
            return self.session
        def guard(session, plan, **kwargs):
            self.events.append('check-study')
            self.assertIs(session, self.session)
            self.assertIs(plan, self.plan)
            self.assertEqual(kwargs, {'repository': self.repository,
                'ledger_identity': self.args['expected_ledger_identity']})
            if guard_error is not None:
                raise guard_error
        def run(*args, **kwargs):
            self.events.append('runtime')
            self.assertIs(kwargs['qualification_session'], self.session)
            self.assertIs(kwargs['permit_provider'], self.args['permit_provider'])
            if run_error is not None:
                raise run_error
            return {'status': 'acceptance_unavailable', 'accepted': False, 'original': True}
        def revoke(*args, **kwargs):
            self.events.append('revoke')
            if revoke_error is not None:
                raise revoke_error
        self.issue = stack.enter_context(patch.object(issued.IssuedQualification, 'issue', side_effect=issue))
        self.guard = stack.enter_context(patch.object(issued.IssuedQualification, 'check_study', side_effect=guard))
        self.run = stack.enter_context(patch.object(runtime, 'run_study', side_effect=run))
        self.revoke = stack.enter_context(patch.object(issued.IssuedQualification, 'revoke', side_effect=revoke))
        return stack

    def test_valid_fresh_inputs_do_not_create_grant_or_child_records(self):
        before = self.chain.commitment
        self.validate()
        self.assertEqual(self.chain.commitment, before)
        self.assertFalse(self.args['grant_root'].exists())
        self.assertFalse(self.args['output'].exists())

    def test_issue_precedes_runtime_and_original_result_is_preserved(self):
        self.boundaries()
        result = entry.run_qualified_study(self.plan, **self.args)
        self.assertEqual(self.events, ['issue', 'check-study', 'runtime', 'revoke'])
        self.assertEqual(result, {'status': 'acceptance_unavailable', 'accepted': False, 'original': True})
        self.assertTrue(self.args['grant_authority']._closed)
        self.chain.require_current()
        self.args['permit_provider'].assert_not_called()

    def test_issuance_failure_never_starts_runtime_and_closes_owned_grant(self):
        failure = ValueError('missing independent review')
        self.boundaries(issue_error=failure)
        with self.assertRaises(ValueError) as caught:
            entry.run_qualified_study(self.plan, **self.args)
        self.assertIs(caught.exception, failure)
        self.assertEqual(self.events, ['issue'])
        self.run.assert_not_called()
        self.revoke.assert_not_called()
        self.assertTrue(self.args['grant_authority']._closed)
        self.chain.require_current()

    def test_runtime_failure_revokes_and_preserves_original_failure(self):
        failure = ValueError('runtime failed')
        self.boundaries(run_error=failure)
        with self.assertRaises(ValueError) as caught:
            entry.run_qualified_study(self.plan, **self.args)
        self.assertIs(caught.exception, failure)
        self.assertEqual(self.events, ['issue', 'check-study', 'runtime', 'revoke'])
        self.chain.require_current()

    def test_runtime_and_revocation_failure_are_both_reported(self):
        run_failure, revoke_failure = ValueError('runtime'), ValueError('revoke')
        self.boundaries(run_error=run_failure, revoke_error=revoke_failure)
        with self.assertRaises(BaseExceptionGroup) as caught:
            entry.run_qualified_study(self.plan, **self.args)
        self.assertEqual(caught.exception.exceptions, (run_failure, revoke_failure))
        self.assertTrue(self.args['grant_authority']._closed)

    def test_existing_child_deadline_is_rejected_before_issuance(self):
        first = self.plan.roster.children[0]
        Records(self.chain).put('child.' + first.trajectory + '.begin', {'deadline': 1})
        self.args['expected_checkpoint'] = self.chain.commitment
        self.boundaries()
        with self.assertRaises(StudyError):
            entry.run_qualified_study(self.plan, **self.args)
        self.issue.assert_not_called()
        self.assertFalse(self.args['grant_authority']._closed)

    def test_existing_child_root_or_overlapping_proof_root_is_rejected(self):
        with self.assertRaises(StudyError):
            self.validate(output=self.args['grant_root'])
        child_root = self.args['output'] / self.plan.roster.children[-1].trajectory
        child_root.mkdir(parents=True)
        with self.assertRaises(StudyError):
            self.validate()

    def test_wrong_ledger_identity_or_fixture_mode_is_rejected_before_issue(self):
        self.boundaries()
        for change in ({'expected_ledger_identity': {'path': 'foreign'}}, {'mode': 'fixture'}):
            with self.subTest(change=change), self.assertRaises(StudyError):
                entry.run_qualified_study(self.plan, **{**self.args, **change})
        self.issue.assert_not_called()

    def test_capsule_for_another_wallet_is_rejected_before_issuance(self):
        value = {'protocol': entry.capsule.PROTOCOL, 'policy': entry.capsule.POLICY,
            'repository': str(self.repository), 'wallet_authorization': {'ledger_identity': {'path': 'foreign'}}}
        with patch.object(entry.capsule, 'bound', return_value=value), self.assertRaises(StudyError):
            entry._validate_reference(self.args['qualification_reference'], repository=self.repository,
                expected_ledger_identity=self.args['expected_ledger_identity'])

    def test_fixture_transport_cannot_enter_the_live_issuance_path(self):
        self.workers['mini']._transport = lambda *_args, **_kwargs: self.fail('No provider')
        with self.assertRaises(StudyError):
            self.validate()

    def test_runtime_rejects_unissued_live_or_fixture_with_session_before_controller(self):
        forwarded = {key: self.args[key] for key in ('output', 'repository', 'checkpoint', 'expected_checkpoint',
            'existing_ledger_path', 'expected_ledger_identity', 'workers', 'permit_provider', 'evaluator')}
        with patch('gossip_harness.cumulative_study_controller_v2.StudyController') as controller:
            with self.assertRaises(StudyError):
                runtime.run_study(self.plan, **forwarded, mode='live')
            with self.assertRaises(StudyError):
                runtime.run_study(self.plan, **forwarded, mode='fixture', qualification_session=self.session)
            controller.assert_not_called()

    def test_runtime_forwards_same_session_to_child_factory(self):
        forwarded = self.runtime_inputs()
        def forward_only(controller):
            # Method isolation verifies actual construction/factory forwarding;
            # this return is deliberately no study or acceptance verdict.
            controller.runtime_factory(self.plan, 0, controller.records, 123.)
            return {'component_forwarding_only': True}
        with patch.object(StudyController, 'run', forward_only), \
                patch.object(runtime, 'GossipChildRuntime') as child, \
                patch.object(issued.IssuedQualification, 'check_study', return_value={'fixture_guard': True}) as guard:
            runtime.run_study(self.plan, **forwarded, mode='live', qualification_session=self.session)
            guard.assert_called_once_with(self.session, self.plan, repository=self.repository,
                ledger_identity=self.args['expected_ledger_identity'])
            self.assertIs(child.call_args.kwargs['qualification_session'], self.session)
            self.assertEqual(child.call_args.kwargs['mode'], 'live')

    def component_issued_session(self):
        """Real anchored grant; full semantic auditor is explicitly a stub."""
        source_pins = {**self.plan.source_pins, **issued.implementation_sources()}
        plan = replace(self.plan, source_pins=source_pins)
        original_root = self.root / 'component-original-proof'
        original_root.mkdir()
        original = original_root / 'head.json'
        original.write_bytes(b'component original witness; no complete semantic proof')
        witness = issued.FileWitness.capture(original)
        facts = {'protocol': entry.capsule.PROTOCOL, 'capsule': self.args['qualification_reference'],
            'live_study_sha256': plan.sha256, 'sources': source_pins,
            'children': [{'cohort': child.cohort, 'trajectory': child.trajectory} for child in plan.roster.children],
            'execution_designs': [{} for _ in range(6)],
            'wallet_authorization': {'ledger_identity': self.args['expected_ledger_identity']},
            'component_stub': 'complete semantic auditor deliberately substituted'}
        # Loaded-definition checking is disabled solely for these explicit
        # semantic substitutions and effect tripwires. Grant/head/plan checks
        # and issuance/revocation are the real methods throughout.
        self.enterContext(patch.object(issued.admission, 'verify_loaded_sources'))
        with patch.object(issued, '_inputs_and_heads', return_value=(witness,)), \
                patch.object(issued.capsule, 'audit_closed_capsule', return_value=facts):
            session = issued.IssuedQualification.issue(self.args['qualification_reference'],
                root=self.args['grant_root'], delta_root=self.args['grant_delta_root'],
                authority=self.args['grant_authority'], live_plan=plan, repository=self.repository)
        def close():
            if not session._revoked:
                issued.IssuedQualification.revoke(session, 'component guard fixture cleanup')
        self.addCleanup(close)
        return plan, session

    def runtime_inputs(self):
        return {key: self.args[key] for key in ('output', 'repository', 'checkpoint', 'expected_checkpoint',
            'existing_ledger_path', 'expected_ledger_identity', 'workers', 'permit_provider', 'evaluator')}

    def test_actual_guard_rejects_unissued_object_before_controller_and_child_effects(self):
        with patch('gossip_harness.cumulative_study_controller_v2.StudyController') as controller, \
                patch.object(runtime.GossipChildRuntime, '_initialize_source') as source, \
                patch.object(runtime, 'MeshNode') as mesh:
            with self.assertRaisesRegex(FinancialError, 'Actual completed issued capability'):
                runtime.run_study(self.plan, **self.runtime_inputs(), mode='live', qualification_session=self.session)
            with self.assertRaisesRegex(FinancialError, 'Actual completed issued capability'):
                runtime.GossipChildRuntime(self.plan, 0, Records(self.chain), 123.,
                    root=self.args['output'], repository=self.repository,
                    existing_ledger_path=self.args['existing_ledger_path'],
                    expected_ledger_identity=self.args['expected_ledger_identity'], workers=self.workers,
                    mode='live', permit_provider=self.args['permit_provider'], evaluator=self.args['evaluator'],
                    qualification_session=self.session)
            controller.assert_not_called()
            source.assert_not_called()
            mesh.assert_not_called()

    def test_actual_revoked_grant_cannot_construct_controller(self):
        plan, session = self.component_issued_session()
        issued.IssuedQualification.revoke(session, 'component revocation before study')
        with patch('gossip_harness.cumulative_study_controller_v2.StudyController') as controller:
            with self.assertRaisesRegex(FinancialError, 'revoked'):
                runtime.run_study(plan, **self.runtime_inputs(), mode='live', qualification_session=session)
            controller.assert_not_called()

    def test_actual_foreign_study_grant_cannot_construct_controller(self):
        plan, session = self.component_issued_session()
        foreign = replace(plan, cohort=replace(plan.cohort, cohort_id='foreign-study'))
        with patch('gossip_harness.cumulative_study_controller_v2.StudyController') as controller:
            with self.assertRaisesRegex(FinancialError, 'different study'):
                runtime.run_study(foreign, **self.runtime_inputs(), mode='live', qualification_session=session)
            controller.assert_not_called()

    def test_child_rechecks_actual_revocation_before_source_or_mesh_effect(self):
        plan, session = self.component_issued_session()
        issued.IssuedQualification.check_study(session, plan, repository=self.repository,
            ledger_identity=self.args['expected_ledger_identity'])
        issued.IssuedQualification.revoke(session, 'component revocation after outer admission')
        with patch.object(runtime.GossipChildRuntime, '_initialize_source') as source, \
                patch.object(runtime, 'MeshNode') as mesh:
            with self.assertRaisesRegex(FinancialError, 'revoked'):
                runtime.GossipChildRuntime(plan, 0, Records(self.chain), 123.,
                    root=self.args['output'], repository=self.repository,
                    existing_ledger_path=self.args['existing_ledger_path'],
                    expected_ledger_identity=self.args['expected_ledger_identity'], workers=self.workers,
                    mode='live', permit_provider=self.args['permit_provider'], evaluator=self.args['evaluator'],
                    qualification_session=session)
            source.assert_not_called()
            mesh.assert_not_called()

    def test_entry_rechecks_issued_grant_before_runtime_and_revokes_on_refusal(self):
        self.boundaries(guard_error=FinancialError('fresh study admission refused'))
        with self.assertRaisesRegex(FinancialError, 'fresh study admission refused'):
            entry.run_qualified_study(self.plan, **self.args)
        self.assertEqual(self.events, ['issue', 'check-study', 'revoke'])
        self.run.assert_not_called()

    def test_second_child_revocation_precedes_original_begin_deadline_and_financial_activity(self):
        plan, session = self.component_issued_session()
        visited = []
        def isolated_child_prefix(controller, index):
            # Preserve the exact controller prefix schema while isolating all
            # physical child work. No seal or completed-child verdict is made.
            trajectory = controller.plan.cohort.trajectories[index]
            visited.append(index)
            controller.records.put('child.' + trajectory.id + '.begin', {
                'trajectory': plain(asdict(trajectory)), 'started_at': controller.clock(),
                'deadline': controller.clock() + controller.plan.horizon_seconds,
                'contract_sha256': controller.plan.sha256})
            if index != 0:
                self.fail('Revoked study reached the next frozen child prefix')
            issued.IssuedQualification.revoke(session, 'component revocation between child calls')
            return {'component_prefix_only': True}
        with patch.object(StudyController, '_child', isolated_child_prefix), \
                patch.object(runtime, 'GossipChildRuntime') as child, \
                patch.object(runtime.CumulativeAuthorityV5, 'claim') as financial_claim, \
                patch.object(Ledger, 'claim') as lease, \
                patch.object(Ledger, 'reserve') as reserve, \
                patch.object(OpenAIWorker, 'run') as provider:
            # Actual runtime.run_study, constructor and six-child run loop;
            # actual issued-head revocation and check_study; one isolated base
            # child prefix. This never reaches a semantic acceptance boundary.
            with self.assertRaisesRegex(FinancialError, 'revoked'):
                runtime.run_study(plan, **self.runtime_inputs(), mode='live', qualification_session=session)
            self.assertEqual(visited, [0])
            records = Records(self.chain)
            self.assertIsNotNone(records.read('child.' + plan.roster.children[0].trajectory + '.begin'))
            for later in plan.roster.children[1:]:
                self.assertIsNone(records.read('child.' + later.trajectory + '.begin'))
            child.assert_not_called()
            financial_claim.assert_not_called()
            lease.assert_not_called()
            reserve.assert_not_called()
            provider.assert_not_called()
