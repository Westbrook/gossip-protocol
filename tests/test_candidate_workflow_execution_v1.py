"""Offline mechanism controls. Fake IO cannot grant physical workflow authority."""
from __future__ import annotations

from contextlib import ExitStack
from dataclasses import dataclass, replace
import io
import os
from pathlib import Path
import queue
import tempfile
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
        self._active_call_deadline = None
        self._active_call = None
        self._cleanup_phase = False
        self._timings = execution._Timings()
        self.binding = InertBinding()
        self.profile = SimpleNamespace(phases=('call-000', 'call-001'))
        self.plan = SimpleNamespace(boundaries=())
        self.records = {}
        self.effects = 0
        self.acks = 0
        self.reject_at = None
        self.lose_ack = False

    _operation_deadline = execution.CandidateWorkflowExecution._operation_deadline
    _check_deadline = execution.CandidateWorkflowExecution._check_deadline
    _call_window = execution.CandidateWorkflowExecution._call_window
    _eligible = execution.CandidateWorkflowExecution._eligible

    def read_authenticated(self, name):
        return self.records[name]

    def has_retained(self, name):
        return name in self.records

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
    value.process = SimpleNamespace(stdout=io.BytesIO(stdout), stderr=io.BytesIO(stderr), stdin=InertInput())
    return value


@dataclass(frozen=True)
class InertBinding:
    """Only an asdict input for offline enforcement; never an admitted binding."""
    source_sha256: str = 'a' * 64
    evaluator_sha256: str = 'b' * 64


class InertInput:
    """An actual local fd; application sends must use select/os.write."""
    def __init__(self):
        self.file = tempfile.TemporaryFile()

    def fileno(self):
        return self.file.fileno()

    def getvalue(self):
        self.file.seek(0)
        raw = self.file.read()
        self.file.seek(0, os.SEEK_END)
        return raw

    @property
    def closed(self):
        return self.file.closed

    def close(self):
        self.file.close()

    def __del__(self):
        self.close()


class ControlledClock:
    def __init__(self):
        self.now = 100.0

    def monotonic(self):
        return self.now

    def monotonic_ns(self):
        return int(self.now * 1000000000)

    def advance(self, seconds):
        self.now += seconds


def actual_owner(clock, root):
    """Real owner guards; only external IO/source costs are controlled."""
    owner = object.__new__(execution.CandidateWorkflowExecution)
    owner.mode, owner.closed = 'physical', False
    owner._pid, owner._thread = os.getpid(), threading.get_ident()
    owner.root = Path(root)
    owner.policy, owner.binding = execution.WorkflowPolicy(), InertBinding()
    owner.docker = ['docker', '--host', 'unix:///inert-test.sock']
    owner._deadline = clock.now + execution.HISTORY_TIMEOUT_SECONDS
    owner._active_call_deadline, owner._active_call = None, None
    owner._cleanup_phase, owner._freeze = False, None
    owner._timings = execution._Timings()
    owner.diagnostic_path, owner.diagnostic_error = None, None
    owner.profile = SimpleNamespace(phases=('call-000', 'call-001'))
    owner.plan = SimpleNamespace(boundaries=())
    owner.records, owner.events, owner.costs = {}, [], {}

    def visit(name):
        owner.events.append(name)
        values = owner.costs.get(name, [])
        if values:
            clock.advance(values.pop(0))

    def retain(name, raw):
        visit('retain:' + name)
        if name in owner.records:
            raise AssertionError('duplicate original')
        owner.records[name] = raw

    owner.visit = visit
    owner._retain = owner._retain_blob = retain
    owner.read_authenticated = lambda name: owner.records[name]
    owner.has_retained = lambda name: name in owner.records
    owner.checkpoint = lambda: visit('checkpoint')
    owner.current = lambda _freeze: visit('current')
    owner.endpoint = SimpleNamespace(validate=lambda: visit('endpoint'))
    return owner


class InlineDrain:
    def __init__(self, target, args=(), daemon=True):
        self.target, self.args = target, args
        self.ident = None

    def start(self):
        self.ident = 1
        self.target(*self.args)

    def join(self, timeout=None):
        if self.ident is None:
            raise AssertionError('unstarted reader joined')

    def is_alive(self):
        return False


class InertChild:
    def __init__(self, on_wait=None):
        self.stdout, self.stderr, self.stdin = io.BytesIO(b'actual-output'), io.BytesIO(), None
        self.returncode, self.waits, self.killed = None, [], False
        self.on_wait = on_wait

    def wait(self, timeout):
        self.waits.append(timeout)
        if self.on_wait is not None:
            self.on_wait()
        if self.returncode is None:
            self.returncode = 0
        return self.returncode

    def poll(self):
        return self.returncode

    def kill(self):
        self.killed, self.returncode = True, -9


class InertSocket:
    """IO only: real _Wire supplies all timeout and HTTP parsing behavior."""
    def __init__(self, value):
        body = execution.encoded(value)
        self.response = bytearray(b'HTTP/1.1 200 OK\r\nContent-Length: ' + str(len(body)).encode() + b'\r\n\r\n' + body)
        self.connected, self.sent, self.timeouts, self.closed = [], [], [], False

    def settimeout(self, value):
        self.timeouts.append(value)

    def connect(self, path):
        self.connected.append(path)

    def sendall(self, raw):
        self.sent.append(raw)

    def recv(self, size):
        result = bytes(self.response[:size])
        del self.response[:size]
        return result

    def close(self):
        self.closed = True


def runtime_values():
    return [
        {'MinAPIVersion': '1.44', 'ApiVersion': '1.53', 'Os': 'linux', 'GitCommit': '6bc6209',
         'Version': '29.2.1', 'Arch': 'arm64', 'KernelVersion': 'kernel'},
        {'ID': 'daemon', 'OSType': 'linux', 'CgroupVersion': '2', 'CgroupDriver': 'cgroupfs', 'OomKillDisable': False},
        {'Id': execution.WorkflowPolicy().image_id, 'Os': 'linux'},
    ]


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
        def capture(label, _event):
            # Inert session completion prerequisites only; no reader/physical credit.
            owner._retain(label + '-response-eligible.json', b'{}')
            owner._retain(label + '-capture-eligible.json', b'{}')
        value.phase('call-000', capture)
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
        child = SimpleNamespace(stdin=InertInput(), stdout=io.BytesIO(), stderr=io.BytesIO())
        self.addCleanup(child.stdin.close)
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

    def deadline_fixture(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        clock = ControlledClock()
        self.enterContext(mock.patch.object(execution.time, 'monotonic', clock.monotonic))
        self.enterContext(mock.patch.object(execution.time, 'monotonic_ns', clock.monotonic_ns))
        owner = actual_owner(clock, Path(temp.name) / 'raw')
        value = session_for(owner)
        self.addCleanup(value.process.stdin.close)
        return clock, owner, value

    def test_call_deadline_blocks_phase_resume_and_next_after_guard_work(self):
        writes = [('call-000', 'call-000-request.json'),
            ('resume:call-000:0', 'call-000-resume-000.json'), ('next:call-000', 'call-000-next.json')]
        for text, label in writes:
            for stage in ('current', 'checkpoint', 'retain'):
                with self.subTest(text=text, stage=stage):
                    _, owner, value = self.deadline_fixture()
                    owner.costs[stage if stage != 'retain' else 'retain:' + label] = (
                        [0, 30] if stage == 'checkpoint' else [30])
                    with owner._call_window('call-000'), self.assertRaisesRegex(ValueError, 'deadline'):
                        value._write(text, label)
                    self.assertEqual(value.process.stdin.getvalue(), b'')
                    self.assertIsNone(owner._active_call_deadline)
                    self.assertIn('checkpoint', owner.events)
        _, owner, value = self.deadline_fixture()
        owner.costs['current'] = [29.5]
        with owner._call_window('call-000'):
            value._write('call-000', 'call-000-request.json')
        self.assertEqual(value.process.stdin.getvalue(), b'call-000\n')
        self.assertEqual(owner.events.count('current'), 2)

    def test_pipe_select_and_partial_write_cannot_extend_call_window(self):
        for partial in (False, True):
            with self.subTest(partial=partial):
                clock, owner, value = self.deadline_fixture()
                real_write = os.write

                def writable(_read, write, _error, _timeout):
                    if not partial:
                        clock.advance(30)
                    return [], write, []

                def write_one(fd, raw):
                    count = real_write(fd, raw[:1])
                    clock.advance(30)
                    return count

                with owner._call_window('call-000'), \
                        mock.patch.object(execution.select, 'select', side_effect=writable), \
                        mock.patch.object(execution.os, 'write', side_effect=write_one) as sent:
                    with self.assertRaisesRegex(ValueError, 'deadline'):
                        value._write('call-000', 'call-000-request.json')
                self.assertEqual(value.process.stdin.getvalue(), b'c' if partial else b'')
                self.assertEqual(sent.call_count, 1 if partial else 0)

    def test_queued_frame_rechecks_time_after_dequeue_and_guard_retention(self):
        frame = b'{"kind":"result","phase":"call-000","value":{}}\n'
        for stage in ('dequeue', 'retention'):
            with self.subTest(stage=stage):
                clock, owner, value = self.deadline_fixture()
                value.lines.put(frame)
                if stage == 'dequeue':
                    original_get = value.lines.get

                    def delayed_get(*args, **kwargs):
                        raw = original_get(*args, **kwargs)
                        clock.advance(30)
                        return raw

                    self.enterContext(mock.patch.object(value.lines, 'get', side_effect=delayed_get))
                else:
                    owner.costs['retain:call-000-frame-000.bin'] = [30]
                capture = mock.Mock()
                with self.assertRaisesRegex(ValueError, 'deadline'):
                    value.phase('call-000', capture)
                capture.assert_not_called()
                self.assertNotIn('call-000-response.bin', owner.records)
                self.assertNotIn('call-000-next.json', owner.records)
                self.assertEqual(value.process.stdin.getvalue(), b'call-000\n')
                if stage == 'retention':
                    self.assertEqual(owner.records['call-000-frame-000.bin'], frame)

    def test_timely_frame_capture_overrun_cannot_resume_or_complete_call(self):
        for kind in ('boundary', 'result'):
            with self.subTest(kind=kind):
                clock, owner, value = self.deadline_fixture()
                owner.plan = SimpleNamespace(boundaries=(SimpleNamespace(id='point', occurrences=1),))
                event = ({'kind': 'boundary', 'phase': 'call-000', 'ordinal': 0, 'boundary': 'point',
                    'occurrence': 0, 'paths': {'root': None, 'database': None},
                    'path_origins': {'root': None, 'database': None}} if kind == 'boundary' else
                    {'kind': 'result', 'phase': 'call-000', 'value': {'known': 'raw-only'}})
                raw = execution.encoded(event) + b'\n'
                value.lines.put(raw)

                def capture(label, _event):
                    if kind == 'result':
                        # Inert prerequisites exercise the real completion deadline.
                        # They are not source-qualified reader/physical evidence.
                        owner._retain(label + '-response-eligible.json', b'{}')
                        owner._retain(label + '-capture-eligible.json', b'{}')
                    clock.advance(30)

                with self.assertRaisesRegex(ValueError, 'deadline'):
                    value.phase('call-000', capture)
                self.assertEqual(owner.records['call-000-frame-000.bin'], raw)
                self.assertNotIn('call-000-completion-eligible.json', owner.records)
                self.assertNotIn('call-000-next.json', owner.records)
                self.assertNotIn('call-000-resume-000.json', owner.records)
                self.assertEqual(value.process.stdin.getvalue(), b'call-000\n')
                if kind == 'result':
                    self.assertEqual(owner.records['call-000-response.bin'], raw)

    def test_control_deadline_uses_fresh_residual_and_reaps_setup_overrun(self):
        for variant in ('remaining', 'before-spawn', 'after-spawn'):
            with self.subTest(variant=variant):
                clock, owner, _ = self.deadline_fixture()
                command = execution._Commands(owner)
                child = InertChild()
                owner.costs['current'] = [6, 4] if variant == 'remaining' else ([15, 15] if variant == 'before-spawn' else [])

                def spawn(*_args, **_kwargs):
                    if variant == 'after-spawn':
                        clock.advance(30)
                    return child

                with mock.patch.object(execution.transport.subprocess, 'Popen', side_effect=spawn) as popen, \
                        mock.patch.object(execution.transport.threading, 'Thread', InlineDrain):
                    if variant == 'remaining':
                        with owner._call_window('call-000'):
                            record = command.run('inspect', ['docker', 'inspect', 'inert'])
                        self.assertEqual(child.waits, [20])
                        self.assertFalse(record['timed_out'])
                    else:
                        # Without an active call, the original control deadline
                        # itself must survive source/retention/setup cost.
                        with self.assertRaisesRegex(ValueError, 'deadline'):
                            command.run('inspect', ['docker', 'inspect', 'inert'])
                        if variant == 'before-spawn':
                            popen.assert_not_called()
                        else:
                            self.assertTrue(child.killed)
                            self.assertEqual(child.waits, [5])
                            self.assertTrue(command.records['inspect']['timed_out'])
                            self.assertEqual(owner.records['inspect-stdout.bin'], b'actual-output')
                self.assertIsNone(command._control_deadline)

    def test_workflow_runtime_identity_matches_frozen_validation_and_output(self):
        clock, _, _ = self.deadline_fixture()
        endpoint = execution.process.EngineEndpoint('/inert.sock', 1, 2)
        for change in (None, 'api', 'image', 'capability'):
            outcomes = []
            for legacy in (True, False):
                values = runtime_values()
                if change == 'api':
                    values[0]['MinAPIVersion'] = '9.0'
                elif change == 'image':
                    values[2]['Id'] = 'sha256:' + '0' * 64
                elif change == 'capability':
                    values[1]['OomKillDisable'] = 0
                sockets = [InertSocket(row) for row in values]
                originals = {}
                with mock.patch.object(execution.process.EngineEndpoint, 'validate'), \
                        mock.patch.object(execution.process.socket, 'socket', side_effect=sockets):
                    try:
                        if legacy:
                            result = execution.process.runtime_identity(endpoint, execution.WorkflowPolicy().image_id,
                                retain=lambda name, raw: originals.__setitem__(name, raw), label='runtime')
                        else:
                            result = execution._runtime_identity(endpoint, execution.WorkflowPolicy().image_id,
                                deadline=clock.now + 15, retain=lambda name, raw: originals.__setitem__(name, raw), label='runtime')
                    except execution.process.ProcessError:
                        result = 'rejected'
                outcomes.append((result, originals))
                self.assertEqual([len(sock.sent) for sock in sockets], [1, 1, 1])
            self.assertEqual(outcomes[0], outcomes[1])
            self.assertEqual(outcomes[0][0] == 'rejected', change is not None)

    def test_runtime_successive_requests_share_actual_absolute_wire_deadline(self):
        for boundary, stop_after in ((boundary, stage) for boundary in ('history', 'call', 'runtime')
                                     for stage in ('version', 'info', None)):
            with self.subTest(boundary=boundary, stop_after=stop_after):
                clock, owner, _ = self.deadline_fixture()
                owner.endpoint = execution.process.EngineEndpoint('/inert.sock', 1, 2)
                sockets = [InertSocket(row) for row in runtime_values()]
                retained = owner._retain

                def retain(name, raw):
                    retained(name, raw)
                    if stop_after and name == 'runtime-' + stop_after + '-response.bin':
                        clock.advance(budget)

                owner._retain = retain
                with owner._call_window('call-000'), \
                        mock.patch.object(execution.process.EngineEndpoint, 'validate'), \
                        mock.patch.object(execution.process.socket, 'socket', side_effect=sockets):
                    budget = 15 if boundary == 'runtime' else 5
                    if boundary == 'history':
                        owner._deadline = clock.now + budget
                    elif boundary == 'call':
                        clock.advance(25)
                    if stop_after:
                        with self.assertRaisesRegex(TimeoutError, 'transport deadline'):
                            owner._runtime('runtime')
                    else:
                        owner._runtime('runtime')
                completed = {'version': 1, 'info': 2, None: 3}[stop_after]
                self.assertEqual(sum(bool(sock.connected) for sock in sockets), completed)
                self.assertEqual(sum(bool(sock.sent) for sock in sockets), completed)
                self.assertTrue(all(timeout == budget for sock in sockets for timeout in sock.timeouts))
                self.assertEqual('runtime.json' in owner.records, stop_after is None)

    def test_call_window_cannot_reset_and_cleanup_cannot_send_application_bytes(self):
        clock, owner, value = self.deadline_fixture()
        with owner._call_window('call-000') as deadline:
            with self.assertRaisesRegex(ValueError, 'nest or reset'):
                with owner._call_window('call-000'):
                    self.fail('Nested call window entered')
            clock.advance(30)
            self.assertEqual(clock.now, deadline)
            with self.assertRaisesRegex(ValueError, 'observation deadline'):
                owner._effect_boundary()
            owner._cleanup_phase = True
            try:
                owner._effect_boundary()
                with self.assertRaisesRegex(ValueError, 'Cleanup cannot send'):
                    value._write('next:call-000', 'late-next.json')
            finally:
                owner._cleanup_phase = False
        self.assertIsNone(owner._active_call)
        self.assertIsNone(owner._active_call_deadline)
        self.assertEqual(value.process.stdin.getvalue(), b'')
        self.assertNotIn('late-next.json', owner.records)
        # Ordinary guarded operations such as unpause cannot borrow cleanup.
        owner._deadline = clock.now
        with mock.patch.object(execution.transport.subprocess, 'Popen', side_effect=AssertionError('unexpected dispatch')) as popen:
            with self.assertRaisesRegex(ValueError, 'history deadline'):
                execution._Commands(owner).run('unpause', ['docker', 'unpause', 'inert'])
        popen.assert_not_called()

    def test_finish_reuses_active_window_and_retains_natural_late_exit_as_incomplete(self):
        for active, before_wait in ((False, False), (True, False), (True, True)):
            with self.subTest(active=active, before_wait=before_wait):
                clock, owner, value = self.deadline_fixture()
                child = InertChild(on_wait=lambda: clock.advance(2))
                child.stdin = value.process.stdin
                value.process = child
                if before_wait:
                    close = child.stdin.close

                    def slow_close():
                        close()
                        clock.advance(1)

                    child.stdin.close = slow_close
                with ExitStack() as stack:
                    if active:
                        stack.enter_context(owner._call_window('call-000'))
                        clock.advance(29)
                    else:
                        owner.costs['current'] = [29]
                    with self.assertRaisesRegex(ValueError, 'deadline'):
                        value.finish(True, send_finish=not active)
                self.assertEqual(child.waits, [5] if before_wait else [1])
                record = execution.profile.decode(owner.records['session.json'])
                self.assertEqual(record['natural_exit'], not before_wait)
                self.assertEqual(record['exit_code'], -9 if before_wait else 0)
                self.assertTrue(record['timed_out'])
                self.assertEqual(child.killed, before_wait)
                self.assertTrue(value.finished)
                self.assertIsNone(owner._active_call_deadline)
        _, owner, value = self.deadline_fixture()
        child = InertChild()
        child.stdin, value.process = value.process.stdin, child
        primary = execution.ExecutionError('source admission revoked')
        with mock.patch.object(owner, 'current', side_effect=primary):
            with self.assertRaises(execution.ExecutionError) as caught:
                value.finish(True)
        self.assertIs(caught.exception, primary)
        record = execution.profile.decode(owner.records['session.json'])
        self.assertFalse(record['natural_exit'])
        self.assertFalse(record['timed_out'])
        self.assertTrue(child.killed)

    def test_timing_spans_are_bounded_and_sidecar_is_exclusive_nonjournal_diagnostic(self):
        clock, owner, _ = self.deadline_fixture()
        timings = owner._timings
        with ExitStack() as nested:
            for _ in range(17):
                nested.enter_context(timings.span('source'))
        self.assertEqual(len(timings.rows), 16)
        self.assertTrue(timings.omitted)
        for _ in range(execution.TIMING_MAX_RECORDS):
            with timings.span('checkpoint'):
                clock.advance(0.001)
        self.assertEqual(len(timings.rows), execution.TIMING_MAX_RECORDS)
        self.assertEqual(timings.stack, [])
        primary = RuntimeError('original failure')
        small = execution._Timings()
        with self.assertRaises(RuntimeError) as caught:
            with small.span('control'):
                raise primary
        self.assertIs(caught.exception, primary)
        self.assertEqual(small.rows[0]['status'], 'error')
        with small.span('not-a-declared-stage'):
            pass
        self.assertEqual(len(small.rows), 1)
        before = dict(owner.records)
        owner._flush_timings({'execution_id': 'inert'})
        path = Path(owner.diagnostic_path)
        first = path.read_bytes()
        self.assertLessEqual(len(first), execution.TIMING_MAX_BYTES)
        self.assertTrue(execution.profile.decode(first)['truncated'])
        self.assertEqual(owner.records, before)
        self.assertEqual(owner.events, [])
        owner._flush_timings({'execution_id': 'inert'})
        self.assertEqual(owner.diagnostic_error, 'FileExistsError')
        self.assertEqual(path.read_bytes(), first)
        owner._timings.rows = [{'unexpected': 'x' * execution.TIMING_MAX_BYTES}]
        owner.diagnostic_error = None
        owner._flush_timings({'execution_id': 'oversized'})
        self.assertIsNotNone(owner.diagnostic_error)
        self.assertFalse(owner.root.with_name('raw-oversized-timings.json').exists())
        self.assertEqual(owner.records, before)
