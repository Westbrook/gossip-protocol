"""Offline owner integration. Synthetic review/controller and a refused local socket.

No Docker CLI/container or model candidate is executed. The real state/Git and
protected journal are exercised, including failure before the first Engine
resource creation. These fixtures supply no independent source approval.
"""
from dataclasses import asdict, replace
import copy
import json
from pathlib import Path
import socket
import sys
import tempfile
import time
import unittest
from unittest import mock
from contextlib import ExitStack
import subprocess
from types import SimpleNamespace

from tests.test_shared_public_evaluator_capacity_v1 import SharedEvaluatorFinancialTests as _FinanceFixture
from tests.test_cumulative_child_deadline_v1 import selected_plan
from tests.test_cumulative_study_repaired_runtime_v2 import CumulativeStudyRepairedRuntimeV2Tests as _PlanFixture
from gossip_harness import cumulative_child_deadline_v1 as clocks
from gossip_harness import cumulative_study_controller_v2 as study
from gossip_harness.peer_project_contract_v2 import to_dict

from gossip_harness import candidate_client_process_v4 as process
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import cumulative_generated_probe_execution_v1 as execution
from gossip_harness import cumulative_generated_probe_plan_v1 as plans
from gossip_harness import cumulative_generated_probe_state_v1 as state
from gossip_harness import cumulative_generated_probe_values_v2 as values
from tests import test_cumulative_generated_probe_state_v1 as state_fixture


def exec_record():
    return {'ID': 'a'*64, 'ContainerID': 'b'*64, 'Running': True, 'Pid': 123, 'OpenStdin': True,
        'ProcessConfig': {'entrypoint': 'python', 'arguments': ['-I', '-B', '/checks/child_driver.py'],
                          'user': '65534:65534', 'privileged': False, 'tty': False}}


class GeneratedProbeExecutionIdentityTests(unittest.TestCase):
    def test_exact_exec_pid_and_natural_completion(self):
        value = exec_record()
        self.assertEqual(execution.exec_identity(value, container_id='b'*64, exec_id='a'*64, pid=None), 123)
        done = {**value, 'Running': False, 'ExitCode': 0}
        self.assertEqual(execution.exec_identity(done, container_id='b'*64, exec_id='a'*64, pid=123, completed=True), 123)
        for change in ({'ID':'c'*64}, {'ContainerID':'d'*64}, {'Running':False}, {'Pid':True}, {'Pid':0}, {'OpenStdin':False}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                execution.exec_identity({**value, **change}, container_id='b'*64, exec_id='a'*64, pid=123)
        for pid in (None, True, 0, -1):
            with self.subTest(pid=pid), self.assertRaises(ValueError):
                execution.exec_identity(done, container_id='b'*64, exec_id='a'*64, pid=pid, completed=True)
        with self.assertRaises(ValueError): execution.exec_identity(value, container_id='b'*64, exec_id='a'*64, pid=124)
        with self.assertRaises(ValueError): execution.exec_identity({**done, 'ExitCode':1}, container_id='b'*64, exec_id='a'*64, pid=123, completed=True)

    def test_other_command_identity_cannot_supply_probe_values(self):
        for change in ({'user':'0:0'}, {'privileged':True}, {'tty':True}, {'entrypoint':'sh'},
                       {'arguments':['-I', '-B', '/checks/workflow_adapter.py']}):
            value = exec_record(); value['ProcessConfig'].update(change)
            with self.subTest(change=change), self.assertRaises(ValueError):
                execution.exec_identity(value, container_id='b'*64, exec_id='a'*64, pid=None)

    def test_runtime_policy_uses_only_retained_image_and_existing_control_envelope(self):
        policy = execution.RuntimePolicy(10)
        for change in ({'image_id':'python:latest'}, {'timeout_seconds':True}, {'timeout_seconds':31}, {'timeout_seconds':0}):
            with self.subTest(change=change), self.assertRaises(ValueError): replace(policy, **change)
        self.assertEqual(execution.NORMAL_CLEANUP_SECONDS + execution.FALLBACK_LIMITS.total_seconds, 120)


class GeneratedProbeExecutionGitTests(unittest.TestCase):
    def setUp(self, *, child_age_seconds=0):
        # Reuse only the earlier module's explicit inert fixture construction;
        # do not run, inherit or import its test class into this module's census.
        f = state_fixture.GeneratedProbeStateGitTests('test_unused_state_reopens_only_with_independent_current_prefix')
        finance = _FinanceFixture('test_model_submit_waits_while_actual_evaluator_slots_are_held')
        finance.setUp(); self.addCleanup(finance.doCleanups); self.finance_fixture = finance
        helper = _PlanFixture('test_actual_constructor_selects_v5_finance_and_rpc_before_any_role_launch')
        helper.setUp(); self.addCleanup(helper.doCleanups)
        plan = selected_plan(helper.plan())
        runtime = {**plan.runtime, execution.EVALUATOR_CAPACITY_KEY: dict(execution.EVALUATOR_CAPACITY_POLICY)}
        resources = {k:getattr(plan,k) for k in ('horizon_seconds','source_generations','partition_seconds','executor_slots')}
        resources['runtime'] = runtime
        self.study_plan = replace(plan, runtime=runtime,
            cohort=replace(plan.cohort, resource_contract_sha256=study.digest(resources)))
        finance.roster = self.study_plan.roster; finance.select(0)
        finance.context = replace(finance.context, execution_contract_sha256=self.study_plan.sha256)
        finance.contract['execution_contract_sha256'] = self.study_plan.sha256
        for task in finance.contract['task_specs']: task['context'] = to_dict(finance.context)
        original_permit = finance.permit
        def permit(opening):
            p = original_permit(opening);p['execution_design']['runtime'] = dict(self.study_plan.runtime)
            p['execution_design_sha256'] = study.digest(p['execution_design']);return p
        finance.permit = permit
        records = study.Records(finance.chain)
        records.put('contract', self.study_plan.record())
        origin=clocks.begin(self.study_plan,0,time.time)
        # Simulate an already-running child without sleeping or renewing its horizon.
        origin['started_at']-=child_age_seconds;origin['deadline']-=child_age_seconds
        origin['original_clock']['started_ns']-=child_age_seconds*1_000_000_000
        origin['original_clock']['deadline_ns']-=child_age_seconds*1_000_000_000
        records.put('child.'+finance.child.trajectory+'.begin',origin)
        self.executor = finance.open()
        original_target = state_fixture.target
        def financial_target(*args, **kwargs):
            t = original_target(*args, **kwargs)
            return replace(t, subject=replace(t.subject, cohort_id=finance.child.cohort,
                trajectory_id=finance.child.trajectory, execution_contract_sha256=finance.roster.execution_contract_sha256))
        self.addCleanup(f.doCleanups)
        with mock.patch.object(state_fixture, 'target', financial_target): f.setUp()
        self.fixture = f
        self.socket_dir = tempfile.TemporaryDirectory(prefix='probe-sock-', dir='/private/tmp')
        self.addCleanup(self.socket_dir.cleanup)
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM); self.addCleanup(self.sock.close)
        path = str(Path(self.socket_dir.name)/'endpoint.sock'); self.sock.bind(path)
        # Deliberately not listening. There is no Engine behind this owned socket.
        self.endpoint = process.EngineEndpoint.from_url('unix://'+path)
        f.runtime = {'endpoint':asdict(self.endpoint), 'image_id':execution.RuntimePolicy(10).image_id,
                     'kind':'synthetic-no-engine-proof'}
        f.environment = execution.environment_for(f.plan, clock_domain=clocks.clock_domain())
        f.binding = state.binding_for(f.plan, f.review, runtime=f.runtime, environment=f.environment, window=f.window)
        f.registration = state.observation_registration(f.plan, f.binding, gate_id='probe-gate',
            repetition_id='public-development-1', cohort_trajectory_ids=tuple(c.trajectory for c in self.finance_fixture.roster.children))
        f.admission = admission.ObservationAdmission(f.registration, verify_registration=lambda:f.registration if f.available else None)
        self.probe_state = f.open()

    def owner(self, **changes):
        args = {'endpoint':self.endpoint,'cleanup_root':self.fixture.root/'cleanup','executor':self.executor,'study_plan':self.study_plan};args.update(changes)
        result = execution.ProbeExecution(self.probe_state, **args);self.addCleanup(result.close)
        return result

    def test_constructor_binds_physical_environment_but_close_keeps_caller_state(self):
        owner = self.owner(); owner.close()
        self.assertFalse(self.probe_state.closed)
        self.probe_state.current()
        self.assertFalse(self.probe_state.journal.has('intent.json'))
        env = json.loads(self.probe_state.environment_raw)
        self.assertIn('gossip_harness/cumulative_generated_probe_execution_v1.py',env['sources'])
        self.assertIn('gossip_harness/cumulative_generated_probe_pipe_v1.py',env['sources'])
        self.assertFalse(owner.cleanup_root.exists())

    def test_nested_callback_scope_bounds_real_source_and_restores_on_failure(self):
        owner = self.owner()
        outer = time.monotonic_ns() + 60_000_000_000
        inner = outer - 30_000_000_000
        calls = []
        def profile(frame, event, arg):
            if event == 'call' and frame.f_code is plans.verify_current_source.__code__:
                calls.append(frame.f_locals['deadline_ns'])
        prior = sys.getprofile(); sys.setprofile(profile)
        try:
            with owner._deadline_scope(outer):
                with owner._deadline_scope(inner):
                    owner._effect_boundary()
                    self.assertEqual(calls[-1], inner)
                    self.assertLessEqual(owner._operation_deadline(60), inner / 1e9)
                    with owner._deadline_scope(outer+1):
                        self.assertEqual(owner._source_deadline_ns(), inner)
                self.assertEqual(owner._source_deadline_ns(), outer)
                with self.assertRaisesRegex(RuntimeError, 'controlled'):
                    with owner._deadline_scope(inner): raise RuntimeError('controlled')
                self.assertEqual(owner._source_deadline_ns(), outer)
        finally: sys.setprofile(prior)
        self.assertIsNone(owner._active_deadline_ns)
        with self.assertRaisesRegex(ValueError, 'nested_control_deadline'):
            with owner._deadline_scope(time.monotonic_ns()-1): self.fail('expired scope entered')
        self.assertIsNone(owner._active_deadline_ns)

    def test_overlap_and_endpoint_substitution_are_rejected_before_dispatch(self):
        with self.assertRaisesRegex(ValueError,'probe_cleanup_origin_overlap'):
            self.owner(cleanup_root=self.fixture.store.path/'new-cleanup')
        wrong = replace(self.endpoint,inode=self.endpoint.inode+1)
        with self.assertRaisesRegex(ValueError,'registered_probe_runtime_endpoint_differs'): self.owner(endpoint=wrong)
        self.assertFalse(self.probe_state.journal.has('intent.json'))

    def test_changed_owner_runtime_cannot_consume_intent(self):
        owner = self.owner(); owner.runtime = copy.deepcopy(owner.runtime); owner.runtime['kind'] = 'forged'
        with self.assertRaisesRegex(ValueError,'probe_owner_binding_changed'): owner.execute_once()
        self.assertFalse(self.probe_state.journal.has('intent.json'))

    def test_existing_state_intent_forbids_a_new_physical_dispatch(self):
        self.probe_state.begin(); owner = self.owner()
        with self.assertRaisesRegex(ValueError,'existing_probe_intent'): owner.execute_once()
        self.assertFalse(self.probe_state.journal.has('physical-intent.json'))
        self.assertFalse(owner.cleanup_root.exists())

    def test_refused_runtime_preserves_attempt_without_creating_resources(self):
        owner = self.owner(); result = owner.execute_once()
        self.assertFalse(result['qualified_execution_originals'])
        self.assertFalse(result['acceptance_authority']);self.assertFalse(result['cold_reconstruction_supplied'])
        self.assertTrue(result['infrastructure']);self.assertIsNone(result['pipe_result'])
        self.assertTrue(result['container_cleanup']);self.assertTrue(result['volume_cleanup'])
        self.assertTrue(self.probe_state.journal.has('intent.json'))
        self.assertTrue(self.probe_state.journal.has('physical-intent.json'))
        self.assertFalse(self.probe_state.journal.has('volume-before-dispatch.json'))
        self.assertFalse(self.probe_state.journal.has('session-dispatch.json'))
        self.assertEqual(self.probe_state.journal.read('physical-terminal.json'),values.canonical(result))
        with self.assertRaisesRegex(ValueError,'one_shot'): owner.execute_once()
        owner.close();self.assertFalse(self.probe_state.closed)


    def test_dispatch_holds_actual_model_slot_until_refused_engine_cleanup_finishes(self):
        owner=self.owner();observed=[]
        def profile(frame,event,arg):
            if event=='call' and frame.f_code is execution.ProbeExecution._dispatch_probe.__code__:
                observed.append((self.executor.active_evaluations,self.finance_fixture.free_slots()))
        prior=sys.getprofile();sys.setprofile(profile)
        try:result=owner.execute_once()
        finally:sys.setprofile(prior)
        self.assertEqual(observed,[(1,1)])
        self.assertTrue(result['local_cleanup']);self.assertEqual(self.finance_fixture.free_slots(),2)
        self.assertEqual(self.executor.unresolved_evaluations,0)
        self.assertEqual(self.finance_fixture.transport.calls,[])

    def test_full_model_pool_blocks_probe_intent_until_capacity_returns(self):
        import threading
        owner=self.owner();observed=[]
        # Occupy the exact semaphore used by the real financial executor. The
        # financial suite independently exercises real pending model calls.
        self.assertTrue(self.executor.slots.acquire(blocking=False))
        self.assertTrue(self.executor.slots.acquire(blocking=False))
        def release():
            time.sleep(.15)
            observed.append(not (self.probe_state.root/'intent.json').exists())
            self.executor.slots.release();self.executor.slots.release()
        t=threading.Thread(target=release);t.start()
        try:result=owner.execute_once()
        finally:t.join(5)
        self.assertFalse(t.is_alive());self.assertEqual(observed,[True]);self.assertTrue(result['local_cleanup'])
        self.assertEqual(self.finance_fixture.free_slots(),2)

    def test_financial_owner_identity_and_cohort_must_match_before_intent(self):
        owner=self.owner();identity=self.executor.owner_id
        self.executor.owner_id='changed'
        try:
            with self.assertRaisesRegex(ValueError,'original_probe_executor_changed'):owner.execute_once()
        finally:self.executor.owner_id=identity
        self.assertFalse(self.probe_state.journal.has('intent.json'))
        old=self.executor.contract['cohort_id'];self.executor.contract['cohort_id']='other'
        try:
            with self.assertRaisesRegex(ValueError,'cohort_or_trajectory'):self.owner()
        finally:self.executor.contract['cohort_id']=old
        self.assertEqual(self.finance_fixture.free_slots(),2)

    def test_local_cleanup_requires_reaped_process_closed_streams_and_no_pipe_errors(self):
        owner=self.owner()
        child=subprocess.Popen([sys.executable,'-I','-B','-c','pass'],stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        owner._child=child
        try:
            child.wait(timeout=5)
            self.assertFalse(owner._local_cleanup_complete())
            for stream in (child.stdin,child.stdout,child.stderr):stream.close()
            self.assertTrue(owner._local_cleanup_complete())
            owner._pipe=SimpleNamespace(closed=True,cleanup_errors=['local_reap:TimeoutError'])
            self.assertFalse(owner._local_cleanup_complete())
        finally:
            owner._pipe=None
            if child.poll() is None:child.kill();child.wait(timeout=5)
            for stream in (child.stdin,child.stdout,child.stderr):stream.close()


    def test_constructor_requires_original_plan_and_refuses_replacement_before_intent(self):
        with self.assertRaisesRegex(ValueError,'exact_probe_study_plan_required'):self.owner(study_plan=object())
        changed=replace(self.study_plan,initial_files={'base.py':'# different original source\n'})
        with self.assertRaisesRegex(ValueError,'probe_original_study_changed'):self.owner(study_plan=changed)
        self.assertFalse(self.probe_state.journal.has('intent.json'))
        self.assertEqual(self.finance_fixture.free_slots(),2)

    def test_original_child_clock_is_read_from_actual_financial_journal(self):
        owner=self.owner()
        original=clocks.read(study.Records(self.executor.checkpoint_chain),self.study_plan,0,active=True)
        self.assertEqual(owner.original_child_clock,original)
        self.assertLessEqual(original.started_ns,owner.binding.window.started_ns)
        self.assertLessEqual(owner.binding.window.deadline_ns,original.deadline_ns)
        with mock.patch.object(execution.time,'time',return_value=original.deadline_unix):
            with self.assertRaisesRegex(ValueError,'original_child_deadline_expired'):owner.execute_once()
        self.assertFalse(self.probe_state.journal.has('intent.json'))
        self.assertEqual(self.finance_fixture.free_slots(),2)

    def test_probe_window_and_clock_domain_cannot_escape_original_child(self):
        owner=self.owner();original=owner.original_child_clock;binding=owner.binding
        for window in (state.ProbeWindow(original.started_ns-1,original.started_ns+1),
                       state.ProbeWindow(original.deadline_ns-1,original.deadline_ns+1)):
            owner.binding=replace(binding,window=window)
            with self.assertRaisesRegex(ValueError,'outside_original_child'):owner._child_clock_current()
        owner.binding=binding
        owner.environment={**owner.environment,'clock_domain':'other-clock'}
        with self.assertRaisesRegex(ValueError,'outside_original_child'):owner._child_clock_current()
        self.assertFalse(self.probe_state.journal.has('intent.json'))

    def test_child_expiry_while_waiting_releases_unstarted_slot_without_intent(self):
        import threading
        owner=self.owner();wall=[time.time()]
        for _ in range(2):self.assertTrue(self.executor.slots.acquire(blocking=False))
        def release():
            time.sleep(.15);wall[0]=owner.original_child_clock.deadline_unix
            self.executor.slots.release();self.executor.slots.release()
        t=threading.Thread(target=release)
        with mock.patch.object(execution.time,'time',side_effect=lambda:wall[0]):
            t.start()
            try:
                with self.assertRaisesRegex(ValueError,'original_child_deadline_expired'):owner.execute_once()
            finally:t.join(5)
        self.assertFalse(t.is_alive());self.assertFalse(self.probe_state.journal.has('intent.json'))
        self.assertEqual(self.executor.unresolved_evaluations,0);self.assertEqual(self.finance_fixture.free_slots(),2)
        self.assertEqual(self.finance_fixture.transport.calls,[])

    def test_foreign_incarnation_and_substituted_controller_refuse_dispatch(self):
        owner=self.owner()
        with mock.patch.object(clocks,'_INCARNATION','foreign'):
            with self.assertRaisesRegex(ValueError,'foreign_child_clock_incarnation'):owner._effect_boundary()
        saved=self.executor.checkpoint_chain;self.executor.checkpoint_chain=object()
        try:
            with self.assertRaisesRegex(ValueError,'original_controller_or_roster_changed'):owner._child_clock_current()
        finally:self.executor.checkpoint_chain=saved
        self.assertFalse(self.probe_state.journal.has('intent.json'))

    def test_expired_project_still_allows_separately_bounded_cleanup(self):
        owner=self.owner();owner._cleanup_phase=True;owner._cleanup_deadline=time.monotonic()+10
        with mock.patch.object(execution.time,'time',return_value=owner.original_child_clock.deadline_unix):
            owner._effect_boundary()
        owner._cleanup_deadline=time.monotonic()-1
        with self.assertRaisesRegex(ValueError,'cleanup_deadline'):owner._check_deadline()


    def test_original_child_window_reserves_all_sequential_cleanup_allowances(self):
        owner=self.owner();original=owner.original_child_clock;binding=owner.binding
        allowance=execution.cleanup_allowance_ns()
        self.assertEqual(allowance,125_000_000_000)
        self.assertEqual(owner.environment['reserved_cleanup_allowance_ns'],allowance)
        deadline=original.deadline_ns-allowance
        owner.binding=replace(binding,window=state.ProbeWindow(deadline-1_000_000_000,deadline))
        owner._child_clock_current()
        for end in (deadline+1,original.deadline_ns):
            owner.binding=replace(binding,window=state.ProbeWindow(deadline-1_000_000_000,end))
            with self.assertRaisesRegex(ValueError,'no_original_cleanup_allowance'):owner._child_clock_current()
        self.assertFalse(self.probe_state.journal.has('intent.json'))

    def test_cleanup_headroom_refusal_never_acquires_a_model_slot_or_starts_intent(self):
        owner=self.owner();original=owner.original_child_clock
        owner.binding=replace(owner.binding,window=state.ProbeWindow(
            owner.binding.window.started_ns,original.deadline_ns))
        # Physical dispatch rechecks the original bound before waiting or intent.
        with self.assertRaisesRegex(ValueError,'no_original_cleanup_allowance'):owner.execute_once()
        self.assertEqual(self.finance_fixture.free_slots(),2)
        self.assertEqual(self.executor.active_evaluations,0)
        self.assertFalse(self.probe_state.journal.has('intent.json'))
        self.assertEqual(self.finance_fixture.transport.calls,[])


    def test_valid_late_child_probe_state_cannot_dispatch_without_cleanup_headroom(self):
        f=GeneratedProbeExecutionGitTests('test_constructor_binds_physical_environment_but_close_keeps_caller_state')
        self.addCleanup(f.doCleanups);f.setUp(child_age_seconds=250)
        # The authentic state has a fresh 300-second window and remains valid;
        # its enclosing 600-second project has already used 250 seconds.
        f.probe_state.current()
        original=clocks.read(study.Records(f.executor.checkpoint_chain),f.study_plan,0,active=True)
        self.assertLess(f.probe_state.binding.window.deadline_ns,original.deadline_ns)
        with self.assertRaisesRegex(ValueError,'no_original_cleanup_allowance'):f.owner()
        self.assertFalse(f.probe_state.journal.has('intent.json'))
        self.assertEqual(f.finance_fixture.free_slots(),2)


if __name__ == '__main__': unittest.main()
