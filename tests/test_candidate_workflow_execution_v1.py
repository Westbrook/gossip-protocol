"""Offline mechanism controls. Fake IO cannot grant physical workflow authority."""
from __future__ import annotations

from dataclasses import replace
import io
import os
import queue
import threading
from types import SimpleNamespace
import unittest
from unittest import mock

from gossip_harness import candidate_workflow_execution_v1 as execution
from gossip_harness import candidate_workflow_observation_v1 as observation
from gossip_harness import candidate_workflow_profile_v1 as profile


class RecordingOwner:
    def __init__(self):
        self.mode = 'physical'
        self.policy = execution.WorkflowPolicy()
        self.docker = ['docker', '--host', 'unix:///inert-test.sock']
        self._deadline = None
        self.profile = SimpleNamespace(phases=('call-000', 'call-001'))
        self.plan = SimpleNamespace(boundaries=())
        self.records = {}
        self.effects = 0
        self.acks = 0
        self.reject_at = None
        self.lose_ack = False

    def _retain(self, name, raw):
        if name in self.records:
            raise AssertionError('duplicate original')
        self.records[name] = raw

    _retain_blob = _retain

    def checkpoint(self):
        self.acks += 1
        if self.lose_ack:
            raise execution.ExecutionUnknown('lost external acknowledgement')

    def _effect_boundary(self):
        self.effects += 1
        if self.effects == self.reject_at:
            raise execution.ExecutionError('source/admission revoked after acknowledgement')


def session_for(owner=None, stdout=b'', stderr=b''):
    owner = owner or RecordingOwner()
    value = object.__new__(execution._Session)
    value.owner = owner
    value.commands = SimpleNamespace(owner=owner)
    value.finished = False
    value.requests = []
    value.lines = queue.Queue(maxsize=execution.MAX_EVENTS + 8)
    value.streams = {'stdout': bytearray(), 'stderr': bytearray()}
    value.counts = {'stdout': 0, 'stderr': 0}
    value.errors = set()
    value.threads = []
    value.arguments = ['docker', 'exec', 'inert']
    value.process = SimpleNamespace(stdout=io.BytesIO(stdout), stderr=io.BytesIO(stderr), stdin=io.BytesIO())
    return value


def exec_value():
    return {'ID': 'e' * 64, 'ContainerID': 'c' * 64, 'Running': True, 'Pid': 17,
        'ExitCode': 0, 'OpenStdin': True,
        'ProcessConfig': {'entrypoint': 'python', 'arguments': ['-I', '-B', '/checks/workflow_adapter.py'],
            'user': '65534:65534', 'privileged': False, 'tty': False}}


class CandidateWorkflowExecutionTests(unittest.TestCase):
    def test_exact_policy_refuses_deadline_or_image_widening(self):
        self.assertEqual(execution.WorkflowPolicy().timeout_seconds, 30)
        for options in ({'timeout_seconds': 31}, {'timeout_seconds': True}, {'image_id': 'python:latest'}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                execution.WorkflowPolicy(**options)

    def test_seven_closed_producer_lengths_and_normalized_support(self):
        self.assertEqual(len(execution.QUALIFICATION_IDS), 7)
        for control in execution.QUALIFICATION_IDS:
            stdout, stderr = execution.qualification_bytes(control)
            self.assertTrue(stdout.startswith(execution._READY))
            self.assertEqual(set(execution.qualification_source_files(control)), {'solution.py'})
            record = execution.WorkflowQualificationProfile(control).record()
            self.assertEqual(record['stdout'], {'bytes': len(stdout), 'sha256': execution.sha(stdout)})
            self.assertEqual(record['stderr'], {'bytes': len(stderr), 'sha256': execution.sha(stderr)})
            self.assertFalse(record['product_acceptance_authority'])
        value = profile.decode(execution.qualification_bytes('WQ-NORMALIZED-61824')[0].splitlines()[1])['value']
        self.assertEqual(len(profile.normalized(value)), 61824)
        self.assertEqual(len(execution.qualification_bytes('WQ-FRAME-EXACT')[0]) - len(execution._READY) - 1, 131072)
        self.assertEqual(len(execution.qualification_bytes('WQ-FRAME-OVER')[0]) - len(execution._READY) - 1, 131073)
        self.assertEqual(len(execution.qualification_bytes('WQ-STDOUT-EXACT')[0]), 524288)
        self.assertEqual(len(execution.qualification_bytes('WQ-STDOUT-OVER')[0]), 524289)
        self.assertEqual(len(execution.qualification_bytes('WQ-STDERR-EXACT')[1]), 65536)
        self.assertEqual(len(execution.qualification_bytes('WQ-STDERR-OVER')[1]), 65537)

    def test_qualifier_cannot_enter_product_profiles_bindings_or_observers(self):
        value = execution.WorkflowQualificationProfile('WQ-FRAME-EXACT')
        self.assertFalse(profile.accepted_profile(value))
        with self.assertRaises(ValueError):
            profile.reconstruct(value)
        with self.assertRaises(ValueError):
            execution.profile_for_binding(value)
        owner = object.__new__(execution.CandidateWorkflowQualificationExecution)
        owner.mode = 'physical'
        with self.assertRaises(ValueError):
            observation.reconstruct(owner)
        with self.assertRaises(ValueError):
            observation.publish_verifier(owner)
        with self.assertRaises(ValueError):
            execution.CandidateWorkflowQualificationExecution.execute_once(owner)
        with self.assertRaises(ValueError):
            execution.WorkflowQualificationProfile('arbitrary-producer')
        with self.assertRaises(ValueError):
            execution.WorkflowQualificationProfile('WQ-FRAME-EXACT', 'independent_acceptance')

    def test_qualifier_source_substitution_rejected_before_binding(self):
        value = execution.WorkflowQualificationProfile('WQ-FRAME-EXACT')
        files = execution.qualification_source_files(value.control_id)
        files['solution.py'] += b'\n# changed\n'
        with self.assertRaisesRegex(ValueError, 'fixed qualifier source'):
            execution.qualification_binding_for(files, value, execution.WorkflowPolicy(), {'kind': 'fixture'})

    def test_actual_exec_identity_and_natural_completion(self):
        value = exec_value()
        self.assertEqual(execution.exec_identity(value, container_id='c' * 64, exec_id='e' * 64, pid=None), 17)
        self.assertEqual(execution.exec_identity(value, container_id='c' * 64, exec_id='e' * 64, pid=17), 17)
        value.update(Running=False, Pid=0, ExitCode=2)
        self.assertEqual(execution.exec_identity(value, container_id='c' * 64, exec_id='e' * 64, pid=17, completed=True), 17)

    def test_exec_id_container_argv_user_and_pid_substitutions(self):
        changes = [('ID', 'f' * 64), ('ContainerID', 'a' * 64), ('Pid', 18), ('Pid', True), ('Running', False),
            ('ProcessConfig', {**exec_value()['ProcessConfig'], 'user': '0:0'}),
            ('ProcessConfig', {**exec_value()['ProcessConfig'], 'arguments': ['-c', 'pass']}),
            ('ProcessConfig', {**exec_value()['ProcessConfig'], 'privileged': True})]
        for key, replacement in changes:
            value = {**exec_value(), key: replacement}
            with self.subTest(key=key, value=replacement), self.assertRaises(ValueError):
                execution.exec_identity(value, container_id='c' * 64, exec_id='e' * 64, pid=17)

    def test_frame_limit_counts_delimiter_separately(self):
        raw = b'{}' + b' ' * (execution.FRAME_BYTES - 2) + b'\n'
        exact = session_for(stdout=raw)
        exact._drain('stdout')
        self.assertEqual(exact.lines.get_nowait(), raw)
        self.assertIsNone(exact.lines.get_nowait())
        self.assertFalse(exact.errors)
        over = session_for(stdout=raw[:-1] + b' \n')
        over._drain('stdout')
        self.assertEqual(over.errors, {'frame-limit'})
        self.assertIsNone(over.lines.get_nowait())

    def test_stdout_and_stderr_retention_exact_and_over(self):
        for control in execution.QUALIFICATION_IDS:
            stdout, stderr = execution.qualification_bytes(control)
            value = session_for(stdout=stdout, stderr=stderr)
            value._drain('stdout')
            value._drain('stderr')
            self.assertEqual(value.counts, {'stdout': len(stdout), 'stderr': len(stderr)})
            self.assertEqual(bytes(value.streams['stdout']), stdout[:execution.STDOUT_BYTES])
            self.assertEqual(bytes(value.streams['stderr']), stderr[:execution.STDERR_BYTES])
            self.assertEqual('stdout-limit' in value.errors, len(stdout) > execution.STDOUT_BYTES)
            self.assertEqual('stderr-limit' in value.errors, len(stderr) > execution.STDERR_BYTES)

    def test_truncated_tail_never_becomes_complete_frame(self):
        value = session_for(stdout=b'{"kind":"result"}')
        value._drain('stdout')
        self.assertEqual(value.errors, {'truncated-frame'})
        self.assertIsNone(value.lines.get_nowait())

    def test_lost_checkpoint_prevents_pipe_dispatch(self):
        owner = RecordingOwner()
        owner.lose_ack = True
        value = session_for(owner)
        with self.assertRaisesRegex(execution.ExecutionUnknown, 'lost external'):
            value._write('call-000', 'call-000-request.json')
        self.assertEqual(value.process.stdin.getvalue(), b'')
        self.assertIn('call-000-request.json', owner.records)

    def test_source_or_admission_revocation_after_ack_prevents_dispatch(self):
        owner = RecordingOwner()
        owner.reject_at = 2
        value = session_for(owner)
        with self.assertRaisesRegex(ValueError, 'revoked after'):
            value._write('call-000', 'call-000-request.json')
        self.assertEqual(owner.acks, 1)
        self.assertEqual(value.process.stdin.getvalue(), b'')

    def test_complete_first_call_retained_before_missing_next_call(self):
        owner = RecordingOwner()
        value = session_for(owner)
        raw = b'{"kind":"result","phase":"call-000","value":{"wrong":true}}\n'
        value.lines.put(raw)
        value.phase('call-000', lambda *_: None)
        self.assertEqual(owner.records['call-000-response.bin'], raw)
        value.lines.put(None)
        with self.assertRaises(ValueError):
            value.phase('call-001', lambda *_: None)
        self.assertEqual(owner.records['call-000-response.bin'], raw)
        self.assertNotIn('call-001-response.bin', owner.records)

    def test_call_replay_refused_before_new_intent(self):
        owner = RecordingOwner()
        value = session_for(owner)
        value.requests = ['call-000']
        with self.assertRaisesRegex(ValueError, 'replay/reordering'):
            value.phase('call-000', lambda *_: None)
        self.assertEqual(owner.records, {})

    def test_partial_reader_start_attempts_owned_teardown(self):
        owner = RecordingOwner()
        child = SimpleNamespace(stdin=io.BytesIO(), stdout=io.BytesIO(), stderr=io.BytesIO())
        first, second = mock.Mock(), mock.Mock()
        second.start.side_effect = RuntimeError('second reader creation failed')
        with mock.patch.object(execution.subprocess, 'Popen', return_value=child), \
                mock.patch.object(execution.threading, 'Thread', side_effect=[first, second]), \
                mock.patch.object(execution.transport, '_stop_local_process') as stop:
            with self.assertRaisesRegex(RuntimeError, 'second reader'):
                execution._Session(owner, SimpleNamespace(), ['docker', 'exec', 'inert'])
        stop.assert_called_once_with(child, [first, second])

    def test_close_attempts_journal_after_cleanup_failure_and_is_idempotent(self):
        owner = object.__new__(execution.CandidateWorkflowExecution)
        owner._pid, owner._thread, owner.closed = os.getpid(), threading.get_ident(), False
        owner._session = None
        owner._cleanup = mock.Mock()
        owner._cleanup.close.side_effect = RuntimeError('cleanup release failed')
        owner.journal = mock.Mock()
        with self.assertRaisesRegex(RuntimeError, 'cleanup release'):
            owner.close()
        owner.journal.close.assert_called_once()
        self.assertTrue(owner.closed)
        self.assertIn('cleanup release', owner.close_errors[0])
        owner.close()
        owner.journal.close.assert_called_once()

    def test_context_close_preserves_primary_baseexception(self):
        owner = object.__new__(execution.CandidateWorkflowExecution)
        owner._pid, owner._thread, owner.closed = os.getpid(), threading.get_ident(), False
        owner._session = None
        owner._cleanup = mock.Mock()
        owner._cleanup.close.side_effect = RuntimeError('secondary cleanup')
        owner.journal = mock.Mock()
        owner.__exit__(KeyboardInterrupt, KeyboardInterrupt(), None)
        owner.journal.close.assert_called_once()
        self.assertTrue(owner.closed)

    def test_normalized_support_is_not_raw_frame_bound(self):
        value = 'x' * 61822
        self.assertEqual(len(profile.normalized(value)), execution.NORMALIZED_BYTES)
        self.assertLess(len(execution.encoded({'kind': 'result', 'phase': 'call-000', 'value': value})), execution.FRAME_BYTES)
        self.assertEqual(len(profile.normalized(value + 'x')), execution.NORMALIZED_BYTES + 1)
        self.assertEqual(replace(execution.WorkflowPolicy(), seed='another-declared-seed').timeout_seconds, 30)
