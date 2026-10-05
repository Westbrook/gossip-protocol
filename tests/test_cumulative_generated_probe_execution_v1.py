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
import tempfile
import unittest

from gossip_harness import candidate_client_process_v4 as process
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import cumulative_generated_probe_execution_v1 as execution
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
    def setUp(self):
        # Reuse only the earlier module's explicit inert fixture construction;
        # do not run, inherit or import its test class into this module's census.
        f = state_fixture.GeneratedProbeStateGitTests('test_unused_state_reopens_only_with_independent_current_prefix')
        self.addCleanup(f.doCleanups); f.setUp(); self.fixture = f
        self.socket_dir = tempfile.TemporaryDirectory(prefix='probe-sock-', dir='/private/tmp')
        self.addCleanup(self.socket_dir.cleanup)
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM); self.addCleanup(self.sock.close)
        path = str(Path(self.socket_dir.name)/'endpoint.sock'); self.sock.bind(path)
        # Deliberately not listening. There is no Engine behind this owned socket.
        self.endpoint = process.EngineEndpoint.from_url('unix://'+path)
        f.runtime = {'endpoint':asdict(self.endpoint), 'image_id':execution.RuntimePolicy(10).image_id,
                     'kind':'synthetic-no-engine-proof'}
        f.environment = execution.environment_for(f.plan, clock_domain='synthetic-offline-clock')
        f.binding = state.binding_for(f.plan, f.review, runtime=f.runtime, environment=f.environment, window=f.window)
        f.registration = state.observation_registration(f.plan, f.binding, gate_id='probe-gate',
            repetition_id='public-development-1', cohort_trajectory_ids=('trajectory','t2','t3','t4','t5','t6'))
        f.admission = admission.ObservationAdmission(f.registration, verify_registration=lambda:f.registration if f.available else None)
        self.probe_state = f.open()

    def owner(self, **changes):
        args = {'endpoint':self.endpoint,'cleanup_root':self.fixture.root/'cleanup'};args.update(changes)
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


if __name__ == '__main__': unittest.main()
