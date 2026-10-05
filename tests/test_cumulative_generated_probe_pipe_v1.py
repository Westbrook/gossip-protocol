"""Actual host-authored subprocess controls, never candidate or Docker executions."""
from dataclasses import replace
from contextlib import contextmanager, nullcontext
import json
import subprocess
import sys
import time
import unittest

from gossip_harness import cumulative_generated_probe_pipe_v1 as pipes
from gossip_harness import cumulative_generated_probe_wire_v1 as wire
from tests.test_cumulative_generated_probe_values_v2 import admitted, proposal, values_for
from tests.test_cumulative_generated_probe_wire_v1 import frame

RELEASED = ('M2-REFRESH', 'M3-BACKUP-RESTORE')
CHILD = r'''
import json, os, sys, time
spec = json.loads(sys.argv[1])
mode = spec['mode']
if mode == 'stderr-flood':
    os.write(2, b'e' * 262144)
if mode == 'oversize':
    os.write(1, b'x' * 16385 + b'\n')
    time.sleep(60)
if mode == 'queue-flood':
    os.write(1, b'{}\n' * 100)
    time.sleep(60)
if mode == 'prefetch':
    os.write(1, b''.join(bytes.fromhex(raw) for raw in spec['frames']))
    for raw in spec['frames']:
        slot = json.loads(bytes.fromhex(raw))['slot']
        if sys.stdin.buffer.readline() != ('continue:' + slot + '\n').encode():
            sys.exit(13)
    sys.exit(0)
if mode == 'closed-input':
    os.close(0)
for raw_hex in spec['frames']:
    raw = bytes.fromhex(raw_hex)
    if mode == 'truncated':
        os.write(1, raw[:-1]); sys.exit(0)
    if mode == 'fragmented':
        for offset in range(0, len(raw), 7):
            os.write(1, raw[offset:offset+7]); time.sleep(0.0001)
    else:
        os.write(1, raw)
    if mode == 'closed-input':
        time.sleep(60)
    event = json.loads(raw)
    expected = ('continue:' + event['slot'] + '\n').encode()
    if sys.stdin.buffer.readline() != expected:
        sys.exit(13)
    if mode == 'stderr-noise':
        os.write(2, b'e' * 8192)
if mode == 'extra':
    os.write(1, b'{}\n')
if mode in ('hang', 'bad-then-hang'):
    time.sleep(60)
sys.exit(7 if mode == 'nonzero' else 0)
'''


def rows_for(row):
    vals = values_for(row)
    name = row['template_id']
    if name == 'manifest-content-hash-v1':
        slots = ('submitted', 'capture_job', 'finished')
    elif name == 'completed-receipt-replay-v1':
        slots = ('submitted', 'prepared', 'original_receipt', 'before', 'refresh', 'replay', 'finished')
    else:
        slots = ('imported', 'before', 'refresh', 'finished')
    return [frame(slot, {'closed': True} if slot == 'finished' else
                  {'job_id': 'probe-job', 'database': '/tmp/m2/library.sqlite'} if slot == 'capture_job' else vals[slot])
            for slot in slots]


class GeneratedProbePipeProcessTests(unittest.TestCase):
    def make(self, *, template='refresh-noop-v1', mode='normal', rows=None, seconds=10, policy=None):
        self.row = proposal(template)
        self.rows = rows_for(self.row) if rows is None else rows
        self.originals = {}
        self.boundaries = 0
        policy = policy or pipes.PipePolicy(wire.WireLimits(16384, 131072, 256, 16), 65536, 3)
        spec = json.dumps({'mode': mode, 'frames': [raw.hex() for raw in self.rows]})
        child = subprocess.Popen([sys.executable, '-I', '-B', '-u', '-c', CHILD, spec],
                                 stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        def cleanup_child():
            if child.poll() is None: child.kill()
            child.wait(timeout=5)
            for name in ('stdin', 'stdout', 'stderr'):
                getattr(child, name).close()
        self.addCleanup(cleanup_child)
        owner = pipes.ProbePipe(child, policy, deadline_ns=time.monotonic_ns()+int(seconds*1_000_000_000))
        self.addCleanup(owner.close)
        return owner

    def retain(self, name, raw):
        self.assertNotIn(name, self.originals)
        self.originals[name] = raw

    def boundary(self):
        self.boundaries += 1

    def exchange(self, owner, **overrides):
        options = {'released_requirements': RELEASED, 'before_effect': self.boundary, 'retain': self.retain, 'deadline_scope': lambda deadline: nullcontext()}
        options.update(overrides)
        result = owner.exchange(admitted(self.row), **options)
        self.assertIsNotNone(owner.child.poll())
        self.assertTrue(all(getattr(owner.child, name).closed for name in ('stdin', 'stdout', 'stderr')))
        self.assertEqual(json.loads(self.originals['probe-pipe-terminal.json']), result)
        self.assertFalse(result['execution_authority'])
        self.assertFalse(result['acceptance_authority'])
        self.assertFalse(result['runtime_identity_proven'])
        self.assertFalse(result['container_cleanup_proven'])
        return result

    def test_complete_exchange_reconstructs_raw_frames_and_continuations(self):
        owner = self.make(mode='fragmented'); result = self.exchange(owner)
        self.assertTrue(result['natural_exit']); self.assertTrue(result['mechanics_complete'])
        self.assertEqual(result['value']['disposition'], 'pass')
        self.assertEqual(self.originals['probe-stdout.bin'], b''.join(self.rows))
        self.assertEqual(result['infrastructure'], [])
        for ack in result['acks']:
            raw = self.originals['probe-continue-' + pipes.artifact_slot(ack['slot']) + '-intent.bin']
            self.assertEqual(raw, wire.continuation_bytes(ack['slot']))
            self.assertEqual(ack['written_bytes'], len(raw))
        with self.assertRaises(ValueError): owner.exchange(admitted(self.row), released_requirements=RELEASED,
            before_effect=self.boundary, retain=self.retain, deadline_scope=lambda deadline: nullcontext())

    def test_all_templates_use_complete_finite_handshake(self):
        for template in ('refresh-identity-v1', 'completed-receipt-replay-v1', 'manifest-content-hash-v1'):
            with self.subTest(template=template):
                owner = self.make(template=template)
                def capture():
                    self.assertIn('probe-frame-capture-job.bin', self.originals)
                    self.assertNotIn('probe-continue-capture-job-intent.bin', self.originals)
                    return values_for(self.row)['captured_job']
                result = self.exchange(owner, capture_job=capture)
                self.assertTrue(result['mechanics_complete']); self.assertEqual(result['value']['disposition'], 'pass')

    def test_step_deadline_scope_encloses_retention_validation_and_capture(self):
        owner = self.make(template='manifest-content-hash-v1')
        active = []; deadlines = []; callbacks = []
        @contextmanager
        def scope(deadline):
            self.assertFalse(active)
            self.assertLessEqual(deadline, owner.deadline_ns)
            active.append(deadline); deadlines.append(deadline)
            try: yield
            finally: active.pop()
        def retain(name, raw):
            if name.startswith(('probe-frame-', 'probe-continue-')):
                self.assertTrue(active); callbacks.append(('retain', active[0]))
            self.retain(name, raw)
        def boundary():
            self.assertTrue(active); callbacks.append(('boundary', active[0]))
        def capture():
            self.assertTrue(active); callbacks.append(('capture', active[0]))
            return values_for(self.row)['captured_job']
        result = self.exchange(owner, deadline_scope=scope, retain=retain,
                               before_effect=boundary, capture_job=capture)
        self.assertTrue(result['mechanics_complete'])
        self.assertFalse(active)
        self.assertTrue(all(d in deadlines for _, d in callbacks))
        self.assertEqual(sum(k == 'capture' for k, _ in callbacks), 1)

    def test_stderr_backpressure_is_drained_while_exchanging_stdout(self):
        owner = self.make(mode='stderr-noise'); result = self.exchange(owner)
        self.assertTrue(result['mechanics_complete'])
        self.assertEqual(self.originals['probe-stderr.bin'], b'e' * (8192 * len(self.rows)))

    def test_stderr_overflow_caps_retained_bytes_and_refuses_completion(self):
        policy = pipes.PipePolicy(wire.WireLimits(16384, 131072, 256, 16), 128, 3)
        owner = self.make(mode='stderr-flood', policy=policy); result = self.exchange(owner)
        self.assertFalse(result['mechanics_complete']); self.assertIn('stderr_limit', result['infrastructure'])
        self.assertEqual(len(self.originals['probe-stderr.bin']), 128)
        self.assertTrue(result['streams']['stderr']['truncated'])

    def test_oversized_frame_and_queue_flood_do_not_grow_unbounded(self):
        for mode, expected in [('oversize', 'frame_limit'), ('queue-flood', 'frame_queue_limit')]:
            with self.subTest(mode=mode):
                owner = self.make(mode=mode); result = self.exchange(owner)
                self.assertFalse(result['mechanics_complete']); self.assertIn(expected, result['infrastructure'])
                self.assertLessEqual(len(owner.frames), pipes.FRAME_QUEUE)
                self.assertLessEqual(len(owner.pending), owner.policy.wire.frame_bytes)

    def test_future_frames_before_acknowledgement_are_refused(self):
        owner = self.make(mode='prefetch'); result = self.exchange(owner)
        self.assertFalse(result['mechanics_complete'])
        self.assertEqual(result['acks'], [])
        self.assertTrue(any('unacknowledged_future_output' in error for error in result['infrastructure']))

    def test_truncated_frame_does_not_become_an_observation(self):
        owner = self.make(mode='truncated'); result = self.exchange(owner)
        self.assertFalse(result['mechanics_complete']); self.assertEqual(result['value']['disposition'], 'unavailable')
        self.assertIn('truncated_frame', result['infrastructure'])
        self.assertEqual(result['acks'], [])

    def test_nonzero_exit_and_extra_output_refuse_mechanical_completion(self):
        for mode in ('nonzero', 'extra'):
            with self.subTest(mode=mode):
                owner = self.make(mode=mode); result = self.exchange(owner)
                self.assertFalse(result['mechanics_complete']); self.assertTrue(result['natural_exit'])
                self.assertEqual(result['value']['disposition'], 'pass')
                self.assertTrue(result['infrastructure'])

    def test_hung_process_is_killed_at_absolute_window_without_reset(self):
        owner = self.make(mode='hang', seconds=0.6)
        start = time.monotonic(); result = self.exchange(owner)
        self.assertLess(time.monotonic()-start, 6)
        self.assertFalse(result['natural_exit']); self.assertFalse(result['mechanics_complete'])
        self.assertTrue(any('deadline' in error for error in result['infrastructure']))

    def test_known_bad_value_survives_later_timeout(self):
        row = proposal('refresh-noop-v1'); rows = rows_for(row)
        changed = json.loads(rows[2]); changed['value']['status'] = 'refreshed'
        rows[2] = frame('refresh', changed['value'])
        owner = self.make(rows=rows[:-1], mode='bad-then-hang', seconds=0.6)
        result = self.exchange(owner)
        self.assertEqual(result['value']['disposition'], 'fail')
        self.assertFalse(result['mechanics_complete'])
        self.assertIn('probe-frame-refresh.bin', self.originals)
        self.assertNotIn('probe-frame-finished.bin', self.originals)

    def test_closed_stdin_records_no_completed_continuation(self):
        owner = self.make(mode='closed-input'); result = self.exchange(owner)
        self.assertFalse(result['mechanics_complete'])
        self.assertEqual(result['acks'][0]['written_bytes'], 0)
        self.assertNotIn('probe-continue-imported-written.json', self.originals)

    def test_missing_capture_never_acknowledges_capture_boundary(self):
        owner = self.make(template='manifest-content-hash-v1'); result = self.exchange(owner)
        self.assertFalse(result['mechanics_complete']); self.assertEqual(result['value']['disposition'], 'unavailable')
        self.assertIn('probe-frame-capture-job.bin', self.originals)
        self.assertNotIn('probe-continue-capture-job-intent.bin', self.originals)

    def test_admission_failure_stops_child_without_sending_continuation(self):
        owner = self.make()
        def revoke(): raise ValueError('synthetic registration revoked')
        result = self.exchange(owner, before_effect=revoke)
        self.assertFalse(result['mechanics_complete']); self.assertEqual(result['acks'], [])
        self.assertTrue(any('revoked' in error for error in result['infrastructure']))

    def test_failed_retention_is_not_retried_and_child_is_reaped(self):
        owner = self.make(); attempted = []
        def fail(name, raw):
            attempted.append(name)
            raise ValueError('synthetic uncertain journal')
        with self.assertRaisesRegex(ValueError, 'uncertain journal'):
            owner.exchange(admitted(self.row), released_requirements=RELEASED,
                           before_effect=self.boundary, retain=fail, deadline_scope=lambda deadline: nullcontext())
        self.assertEqual(attempted, ['probe-frame-imported.bin'])
        self.assertIsNotNone(owner.child.poll()); self.assertTrue(owner.closed)

    def test_interruption_closes_owned_process_and_preserves_interruption(self):
        owner = self.make()
        def interrupt(): raise KeyboardInterrupt('controlled test interruption')
        with self.assertRaises(KeyboardInterrupt):
            owner.exchange(admitted(self.row), released_requirements=RELEASED,
                           before_effect=interrupt, retain=self.retain, deadline_scope=lambda deadline: nullcontext())
        self.assertIsNotNone(owner.child.poll()); self.assertTrue(owner.closed)
        self.assertFalse(json.loads(self.originals['probe-pipe-terminal.json'])['mechanics_complete'])

    def test_slow_boundary_cannot_receive_a_fresh_window(self):
        owner = self.make(seconds=0.05)
        def slow(): time.sleep(0.08)
        result = self.exchange(owner, before_effect=slow)
        self.assertFalse(result['mechanics_complete']); self.assertEqual(result['acks'], [])
        self.assertTrue(any('deadline' in error for error in result['infrastructure']))

    def test_invalid_probe_closes_transferred_process(self):
        owner = self.make()
        with self.assertRaises(ValueError):
            owner.exchange({}, released_requirements=RELEASED, before_effect=self.boundary, retain=self.retain, deadline_scope=lambda deadline: nullcontext())
        self.assertTrue(owner.closed); self.assertIsNotNone(owner.child.poll())


class GeneratedProbePipePolicyTests(unittest.TestCase):
    def test_logical_slots_map_injectively_into_actual_journal_filename_contract(self):
        from gossip_harness import candidate_checkpoint_chain_v1 as chain
        slots=sorted({slot for rows in wire._SLOTS.values() for slot in rows})
        labels=[pipes.artifact_slot(slot) for slot in slots]
        self.assertEqual(len(labels),len(set(labels)))
        for slot,label in zip(slots,labels,strict=True):
            for name in ('probe-frame-'+label+'.bin','probe-continue-'+label+'-intent.bin',label+'-staging.json'):
                chain._name(name)
            self.assertEqual(wire.continuation_bytes(slot),('continue:'+slot+'\n').encode())
        self.assertEqual(pipes.artifact_slot('original_receipt'),'original-receipt')
        self.assertEqual(pipes.artifact_slot('capture_job'),'capture-job')
        with self.assertRaises(ValueError):pipes.artifact_slot('original-receipt')

    def test_explicit_policy_rejects_outside_existing_envelopes(self):
        good = pipes.PipePolicy(wire.WireLimits(16384, 131072, 256, 16), 65536, 3)
        for changes in ({'stderr_bytes': 65537}, {'stderr_bytes': True}, {'control_seconds': 31},
                        {'control_seconds': 0}, {'wire': wire.WireLimits(131073, 524288, 256, 16)}):
            with self.subTest(changes=changes), self.assertRaises(ValueError): replace(good, **changes)
        self.assertEqual(pipes.definition(good)['cleanup_seconds'], 5)


if __name__ == '__main__':
    unittest.main()
