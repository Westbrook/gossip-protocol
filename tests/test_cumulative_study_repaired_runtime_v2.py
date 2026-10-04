"""V2/V5 composition with patched endpoints; no real runtime or qualification.

Constructor fixtures stop at the selected RPC constructor before any server or
role launch. Finance, mesh, process and Git endpoints are stubbed explicitly.
"""
from contextlib import ExitStack
from copy import deepcopy
from dataclasses import asdict, replace
import hashlib
from pathlib import Path
import platform
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from gossip_harness import cumulative_study_controller_v1 as old_controller
from gossip_harness import cumulative_study_controller_v2 as controller
from gossip_harness import cumulative_study_runtime_v2 as runtime
from gossip_harness.ledger import Lease
from gossip_harness.peer_financial_authority_v2 import ledger_identity
from gossip_harness.peer_financial_authority_v3 import profile_manifest
from gossip_harness.peer_financial_terminal_v2 import CLOSURE_POLICY, CLOSURE_POLICY_SHA256
from gossip_harness.peer_project_contract_v2 import ActionRequest, Context, DispatchBinding, DispatchReply, EvidenceRef, WorkKey, identity, to_dict, worker_request_digest
from gossip_harness.peer_role_loop_v2 import WorkDirective, directive_id, materialize
from gossip_harness.project_acceptance_compiler_v1 import ARMS, BLOCKS, FAULTS, CohortDesign, Trajectory
from gossip_harness.worker import OpenAIWorker


class _ReachedRPC(BaseException):
    pass


class _Records:
    def __init__(self, initial=None):
        self.values = dict(initial or {})
        self.chain = Mock(commitment=object())

    def read(self, key):
        return self.values.get(key)

    def put(self, key, value):
        if key in self.values and self.values[key] != value:
            raise ValueError('Immutable fixture record conflict')
        self.values[key] = deepcopy(value)
        return value


class _MeshEndpoint:
    def __init__(self, config):
        self.config = config

    def start(self):
        return {'port': 12345}


class _SourceMesh:
    def __init__(self, ref, raw):
        self.ref, self.raw = ref, raw

    def arrived(self):
        return (self.ref,)

    def resolve(self, ref):
        return self.raw if ref == self.ref else None

    def want(self, ref):
        raise AssertionError('No subscription expected')


class CumulativeStudyRepairedRuntimeV2Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.repository = Path(__file__).resolve().parents[1]
        self.addCleanup(self.temp.cleanup)
        self.workers = {name: OpenAIWorker('fixture-not-a-real-key', max_output_tokens=64,
            transport=lambda *_args, **_kwargs: self.fail('No provider request')) for name in ('mini', 'strong')}

    def plan(self):
        releases = tuple(controller.Release(m, 'Fixture '+m, {}, {'check.py': '# not executed'},
            ('python', '/checks/check.py'), (m+'-case',), (m+'-requirement',)) for m in controller.MILESTONES)
        runtime_identity = {'protocol': runtime.PROTOCOL, 'financial_protocol': 'peer-financial-authority-v5',
            'financial_rpc_protocol': 'peer-financial-rpc-v5', 'python_executable': sys.executable,
            'python_version': platform.python_version(), 'platform': platform.platform(),
            'max_reserved_units': 1_000_000, 'image': 'sha256:'+'a'*64,
            'public_timeout_seconds': 180, 'terminal_drain_seconds': 1}
        limits = {'horizon_seconds': 30, 'source_generations': 2, 'partition_seconds': .1,
                  'executor_slots': 4, 'runtime': runtime_identity}
        trajectories = tuple(Trajectory(a.lower().replace('-', '')+'.'+b.replace('_', ''), a, b,
            controller.actors_for(a), 'durable_central_scheduler' if a == 'O16-G' else 'peer_local',
            controller.digest({'seed': b}), controller.digest(controller.fault_schedule(b, .1)),
            FAULTS if b == 'compound_recovery' else ()) for a in ARMS for b in BLOCKS)
        cohort = CohortDesign('v5-composition-fixture', trajectories, controller.MILESTONES,
            controller.digest(controller.TRANSPORT_CONTRACT), controller.digest(controller.SHARED_POLICY),
            controller.digest([asdict(x) for x in releases]), controller.digest(limits),
            controller.digest({k: profile_manifest(v) for k, v in self.workers.items()}), 'b'*64, 'c'*64, 'd'*64)
        pins = {name: hashlib.sha256((self.repository/name).read_bytes()).hexdigest() for name in controller.SOURCE_CLOSURE}
        return controller.StudyPlan(cohort, releases, {'base.py': 'pass\n'},
            {p: ('library/'+p+'/impl.py',) for p in controller.PACKAGES}, pins,
            financial_closure_policy=deepcopy(CLOSURE_POLICY), **limits)

    def entry_arguments(self, plan):
        ledger = self.root/'original-ledger.sqlite'
        ledger.write_bytes(b'identity fixture only; not an opened database')
        identity_record = ledger_identity(ledger)
        return {'output': self.root/'output', 'repository': self.repository, 'checkpoint': object(),
            'expected_checkpoint': object(), 'existing_ledger_path': ledger,
            'expected_ledger_identity': identity_record, 'workers': self.workers, 'mode': 'fixture',
            'permit_provider': Mock(), 'evaluator': runtime.DockerPublicChecks(plan.runtime['image'], timeout=180)}

    def constructor_fixture(self, *, change_permit=None, change_config=None):
        plan = self.plan()
        args = self.entry_arguments(plan)
        records = _Records({'ledger': args['expected_ledger_identity']})
        permit = {'protocol': 'peer-financial-operator-permit-v5', 'mode': 'fixture',
            'incremental_cap_micro_usd': 1000, 'expected_global_cap': 2000, 'expected_opening_usage': 0,
            'execution_design': {'financial_closure_policy': deepcopy(CLOSURE_POLICY), 'runtime': deepcopy(plan.runtime)}}
        permit['execution_design_sha256'] = controller.digest(permit['execution_design'])
        if change_permit:
            change_permit(permit)
        config = {'protocol': 'peer-financial-authority-v5', 'financial_closure_policy': deepcopy(CLOSURE_POLICY),
            'financial_closure_policy_sha256': CLOSURE_POLICY_SHA256, 'operator_permit': deepcopy(permit),
            'operator_permit_sha256': controller.digest(permit)}
        if change_config:
            change_config(config)
        finance = SimpleNamespace(config=config, config_sha256=controller.digest(config))
        with ExitStack() as stack:
            # These patch only the effect endpoints of the real V2 constructor.
            stack.enter_context(patch.object(runtime.GossipChildRuntime, '_initialize_source', return_value=object()))
            stack.enter_context(patch.object(runtime.GossipChildRuntime, 'close'))
            stack.enter_context(patch('gossip_harness.cumulative_process_evidence_v1.ProcessEvidence', return_value=Mock()))
            stack.enter_context(patch.object(runtime, 'MeshNode', _MeshEndpoint))
            stack.enter_context(patch.object(runtime, 'MeshFinancePayloads', return_value=Mock(request_guard_sha256='e'*64)))
            opened = stack.enter_context(patch.object(runtime.CumulativeAuthorityV5, 'open', return_value=finance))
            rpc = stack.enter_context(patch.object(runtime, 'FinancialRPCV5', side_effect=_ReachedRPC))
            stack.enter_context(patch.object(runtime, 'FinancialServer', side_effect=AssertionError('No server')))
            stack.enter_context(patch.object(runtime.GossipChildRuntime, '_launch', side_effect=AssertionError('No process')))
            stack.enter_context(patch.object(runtime.subprocess, 'Popen', side_effect=AssertionError('No subprocess')))
            stack.enter_context(patch.object(runtime.threading, 'Thread', side_effect=AssertionError('No thread')))
            try:
                runtime.GossipChildRuntime(plan, 0, records, 4_000_000_000., root=args['output'],
                    repository=args['repository'], existing_ledger_path=args['existing_ledger_path'],
                    expected_ledger_identity=args['expected_ledger_identity'], workers=self.workers, mode='fixture',
                    permit_provider=lambda _contract: permit, evaluator=args['evaluator'])
            except BaseException as error:
                return plan, records, opened, rpc, error
        self.fail('Fixture must stop before RPC/server execution')

    def test_prospective_plan_binds_v5_policy_and_actual_dependency_closure(self):
        plan = self.plan()
        self.assertEqual(plan.record()['protocol'], 'cumulative-study-controller-v2')
        self.assertEqual(plan.record()['financial_closure_policy'], CLOSURE_POLICY)
        self.assertEqual(plan.roster.execution_contract_sha256, plan.sha256)
        for name in ('cumulative_study_controller_v2.py', 'cumulative_study_runtime_v2.py',
                     'peer_financial_authority_v5.py', 'peer_financial_rpc_v5.py', 'peer_financial_terminal_v2.py'):
            self.assertIn('gossip_harness/'+name, plan.source_pins)
        plan.verify_sources(self.repository)

    def test_wrong_or_legacy_runtime_identity_is_rejected_even_with_rehashed_resources(self):
        plan = self.plan()
        for key, value in (('protocol', 'cumulative-study-runtime-v1'),
                           ('financial_protocol', 'peer-financial-authority-v4'),
                           ('financial_rpc_protocol', 'peer-financial-rpc-v4')):
            with self.subTest(key=key):
                changed = {**plan.runtime, key: value}
                resource = {name: getattr(plan, name) for name in
                            ('horizon_seconds', 'source_generations', 'partition_seconds', 'executor_slots')}
                resource['runtime'] = changed
                cohort = replace(plan.cohort, resource_contract_sha256=controller.digest(resource))
                with self.assertRaisesRegex(ValueError, 'Exact V2 runtime'):
                    replace(plan, runtime=changed, cohort=cohort)

    def test_unknown_closure_policy_and_nested_mutation_are_rejected(self):
        plan = self.plan()
        with self.assertRaises(ValueError):
            replace(plan, financial_closure_policy={**CLOSURE_POLICY, 'acceptance_authority': True})
        plan.financial_closure_policy['completed_action_states'].append('unknown')
        with self.assertRaises(ValueError):
            plan.verify_sources(self.repository)

    def test_v1_plan_rejected_before_factory_or_effect_endpoints(self):
        # An object of the frozen V1 class must never be adopted as V2 even if
        # a caller copied similarly named fields into it.
        old = object.__new__(old_controller.StudyPlan)
        with patch.object(controller, 'StudyController', side_effect=AssertionError('No controller')):
            with self.assertRaisesRegex(ValueError, 'Exact V2 prospective plan'):
                runtime.run_study(old, **self.entry_arguments(self.plan()))
        with patch.object(runtime, 'ledger_identity', side_effect=AssertionError('No ledger inspection')):
            with self.assertRaisesRegex(ValueError, 'Exact V2 prospective plan'):
                runtime.GossipChildRuntime(old, 0, None, 1, root=self.root, repository=self.repository,
                    existing_ledger_path=self.root/'unused', expected_ledger_identity={}, workers={}, mode='fixture',
                    permit_provider=Mock(), evaluator=Mock())

    def test_actual_run_study_selects_v2_controller_and_v2_child_factory(self):
        plan = self.plan()
        args = self.entry_arguments(plan)
        child = object()
        with patch.object(controller, 'StudyController') as selected, \
             patch.object(old_controller, 'StudyController', side_effect=AssertionError('No V1 controller')), \
             patch.object(runtime, 'GossipChildRuntime', return_value=child) as child_factory:
            selected.return_value.run.return_value = {'accepted': False, 'status': 'fixture'}
            result = runtime.run_study(plan, **args)
            constructor = selected.call_args
            self.assertIs(constructor.args[0], plan)
            records = _Records()
            self.assertIs(constructor.kwargs['runtime_factory'](plan, 2, records, 123), child)
            self.assertEqual(child_factory.call_args.args, (plan, 2, records, 123))
            self.assertEqual(child_factory.call_args.kwargs['root'], args['output']/plan.cohort.trajectories[2].id)
            self.assertEqual(child_factory.call_args.kwargs['expected_ledger_identity'], args['expected_ledger_identity'])
            self.assertEqual(result, {'accepted': False, 'status': 'fixture'})

    def test_actual_constructor_selects_v5_finance_and_rpc_before_any_role_launch(self):
        plan, records, opened, rpc, error = self.constructor_fixture()
        self.assertIsInstance(error, _ReachedRPC)
        opened.assert_called_once()
        rpc.assert_called_once()
        self.assertIs(rpc.call_args.args[0], opened.return_value)
        self.assertEqual(opened.call_args.kwargs['terminal_roster'], plan.roster)
        self.assertEqual(opened.call_args.kwargs['permit']['execution_design']['runtime'], plan.runtime)
        self.assertEqual(rpc.call_args.kwargs['request_guard_sha256'], controller.digest({
            'payload_guard': 'e'*64, 'deadline': 4_000_000_000., 'protocol': runtime.PROTOCOL}))
        key = 'runtime.'+plan.roster.children[0].trajectory
        self.assertEqual(records.read(key+'.financial-config')['protocol'], 'peer-financial-authority-v5')

    def test_wrong_permit_policy_is_rejected_before_finance_open(self):
        _plan, _records, opened, rpc, error = self.constructor_fixture(
            change_permit=lambda p: p['execution_design'].update(financial_closure_policy={'legacy': True}))
        self.assertIsInstance(error, ValueError)
        opened.assert_not_called()
        rpc.assert_not_called()

    def test_numeric_runtime_alias_is_rejected_before_finance_open(self):
        _plan, _records, opened, rpc, error = self.constructor_fixture(
            change_permit=lambda p: p['execution_design']['runtime'].update(public_timeout_seconds=180.0))
        self.assertIsInstance(error, ValueError)
        opened.assert_not_called()
        rpc.assert_not_called()

    def test_returned_legacy_finance_config_cannot_reach_rpc(self):
        _plan, _records, opened, rpc, error = self.constructor_fixture(
            change_config=lambda config: config.update(protocol='peer-financial-authority-v4'))
        self.assertIsInstance(error, ValueError)
        opened.assert_called_once()
        rpc.assert_not_called()

    def failure_result_fixture(self):
        plan = self.plan()
        actor = plan.roster.children[0].actors[0]
        context = Context(plan.sha256, plan.roster.children[0].cohort, plan.roster.children[0].trajectory,
                          1, plan.releases[0].sha256)
        raw = runtime.canonical_payload({'files': {'library/catalog/impl.py': 'broken\n'}, 'base_sha': 'a'*40})
        ref = EvidenceRef('b'*64, 'seed', 'project-source', hashlib.sha256(raw).hexdigest())
        directive = WorkDirective(context, WorkKey('catalog', 'M1', actor, 0), 'mini', 'build', ref, (),
                                  ('library/catalog/impl.py',), 'Implement public requirements')
        seed = _SourceMesh(ref, raw)
        request, view = materialize(directive, seed, plan.cohort.shared_policy_sha256)
        action = ActionRequest(context, 'action', 'request', actor, 'build', directive.work, 'mini',
            EvidenceRef('c'*64, actor, 'worker-request', 'd'*64), identity(view))
        binding = DispatchBinding(action, Lease(request.task_id, actor, 1, 4_000_000_000.),
            worker_request_digest(request), 'e'*64, 'f'*64, 'call', 'reservation', 100)
        result_payload = {'kind': 'failure', 'payload': {'message': 'known fixture failure', 'usage_units': 7, 'metadata': {}}}
        reply = DispatchReply(action.request_id, identity(action), 'failed', 'durable_outcome', binding,
            hashlib.sha256(runtime.canonical_payload(result_payload)).hexdigest(), 7)
        key = directive_id(directive)
        value = {'actor': actor, 'directive_id': key,
            'snapshot': {'state': 'published', 'reply': to_dict(reply), 'action': to_dict(action), 'view': to_dict(view)},
            'worker_request': controller.plain(asdict(request)), 'result_payload': result_payload}
        current = object.__new__(runtime.GossipChildRuntime)
        current.plan, current.seed, current.key = plan, seed, 'runtime.'+context.trajectory_id
        current.records = _Records({current.key+'.directive.'+key: {'actor': actor, 'directive': directive.to_dict()}})
        current.finance = Mock()
        current.finance.verified_known_failure.return_value = {'reply': reply, 'worker_request': request,
                                                              'result_payload': result_payload}
        return current, value, binding

    def test_known_failure_is_authenticated_and_retained_without_success_proof(self):
        current, value, binding = self.failure_result_fixture()
        before = deepcopy(value)
        current._verify_result(value)
        current.finance.verified_known_failure.assert_called_once_with(binding)
        current.finance.verified_terminal.assert_not_called()
        self.assertEqual(value, before)
        self.assertEqual(value['snapshot']['reply']['state'], 'failed')
        self.assertEqual(value['snapshot']['reply']['usage_units'], 7)

    def test_failure_result_substitution_is_rejected(self):
        current, value, _binding = self.failure_result_fixture()
        value = deepcopy(value)
        value['result_payload']['payload']['usage_units'] = 0
        with self.assertRaisesRegex(ValueError, 'finance request/result'):
            current._verify_result(value)

    def test_known_failure_numeric_aliases_are_rejected(self):
        cases = (
            (('snapshot', 'action', 'context', 'milestone'), 1.0, 'terminal action'),
            (('snapshot', 'reply', 'usage_units'), 7.0, 'terminal action'),
            (('snapshot', 'view', 'context', 'milestone'), True, 'materialization'),
            (('worker_request', 'attempt'), 1.0, 'finance request/result'),
            (('result_payload', 'payload', 'usage_units'), 7.0, 'finance request/result'),
        )
        for path, alias, error in cases:
            with self.subTest(path=path):
                current, original, binding = self.failure_result_fixture()
                value = deepcopy(original)
                target = value
                for part in path[:-1]:
                    target = target[part]
                self.assertEqual(target[path[-1]], alias)
                self.assertIsNot(type(target[path[-1]]), type(alias))
                target[path[-1]] = alias
                self.assertEqual(value, original)  # Python equality alone accepts it.
                with self.assertRaisesRegex(ValueError, error):
                    current._verify_result(value)
                current.finance.verified_known_failure.assert_called_once_with(binding)
                current.finance.verified_terminal.assert_not_called()

    def test_failed_proposal_never_becomes_a_selected_candidate(self):
        current, value, _binding = self.failure_result_fixture()
        stage = 'fixture-stage'
        reviewer = current.plan.roster.children[0].actors[-4]
        review = {'actor': reviewer, 'snapshot': {'reply': {'state': 'completed'}},
            'result_payload': {'payload': {'changes': {'decision.json': runtime.canonical_payload({
                'stage_id': stage, 'package': 'catalog', 'selected_actor': value['actor'], 'reasons': 'fixture', 'tests': []}).decode()}}}}
        with patch.object(current, '_verify_result'), \
             patch.object(current, '_merge_private', side_effect=AssertionError('Failed result cannot merge')):
            result = current.integrate(stage, (value,), (review,))
        self.assertEqual(result['status'], 'selection_rejected')
        self.assertFalse(result['acceptance_authority'])

    def test_global_barrier_is_v5_policy_consumer_and_acceptance_stays_unavailable(self):
        from gossip_harness.peer_financial_terminal_v2 import verified_study_barrier
        self.assertIs(controller.verified_study_barrier, verified_study_barrier)
        result = controller.UnavailableAcceptance().evaluate(self.plan(), {}, ())
        self.assertEqual(result['status'], 'unavailable')
        self.assertFalse(result['accepted'])


if __name__ == '__main__':
    unittest.main()
