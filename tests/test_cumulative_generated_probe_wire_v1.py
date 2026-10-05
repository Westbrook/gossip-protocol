"""Synthetic wire controls; no candidate execution or physical origin credit."""
from copy import deepcopy
import io
import json
from types import SimpleNamespace
import unittest

from gossip_harness import cumulative_generated_probe_driver_v1 as driver
from gossip_harness import cumulative_generated_probe_values_v2 as predicates
from gossip_harness import cumulative_generated_probe_wire_v1 as wire
from tests.test_cumulative_generated_probe_values_v2 import proposal, admitted, values_for

RELEASED = ('M2-REFRESH', 'M3-BACKUP-RESTORE')


def frame(slot, value):
    return json.dumps({'protocol': 'cumulative-generated-probe-wire-v1', 'slot': slot, 'value': value},
                      separators=(',', ':'), ensure_ascii=True).encode() + b'\n'


class GeneratedProbeWireTests(unittest.TestCase):
    limits = wire.WireLimits(frame_bytes=16384, stream_bytes=131072, json_nodes=256, json_depth=16)

    def transcript(self, template='refresh-noop-v1', limits=None):
        return wire.ValueTranscript(admitted(proposal(template)), released_requirements=RELEASED,
                                    limits=self.limits if limits is None else limits)

    def test_request_contains_only_fixed_public_recipe(self):
        probe = admitted(proposal())
        raw = wire.request_bytes(probe, released_requirements=RELEASED)
        self.assertEqual(json.loads(raw), {'protocol': 'cumulative-generated-probe-wire-v1',
            'template_id': 'refresh-identity-v1', 'parameters': {'initial_text': 'old', 'replacement_text': 'new'}})
        for key in ('expectation', 'probe_id', 'normative_refs', 'contract_sha256', 'acceptance_authority'):
            self.assertNotIn(key.encode(), raw)

    def test_mutated_admission_or_release_is_rejected_before_staging(self):
        probe = admitted(proposal())
        for key, value in [('probe_id', '0' * 64), ('contract_sha256', '0' * 64),
                           ('acceptance_authority', True), ('extra', 'secret')]:
            bad = deepcopy(probe); bad[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                driver.adapter_files(bad, released_requirements=RELEASED)
        with self.assertRaises(ValueError):
            driver.adapter_files(probe, released_requirements=())

    def test_all_templates_complete_only_with_every_frame_and_capture(self):
        for template in predicates.TEMPLATES:
            row = proposal(template); vals = values_for(row); transcript = self.transcript(template)
            while transcript.next_slot is not None:
                slot = transcript.next_slot
                if slot == 'finished':
                    self.assertEqual(transcript.result()['disposition'], 'unavailable')
                    value = {'closed': True}
                elif slot == 'capture_job':
                    value = {'job_id': 'probe-job', 'database': '/tmp/m2/library.sqlite'}
                else:
                    value = vals[slot]
                transcript.append(frame(slot, value))
                if slot == 'capture_job':
                    self.assertEqual(transcript.result()['disposition'], 'unavailable')
                    transcript.supply_captured_value(vals['captured_job'])
            result = transcript.result()
            self.assertEqual(result['disposition'], 'pass', template)
            self.assertIs(result['wire_complete'], True)
            for key in ('acceptance_authority', 'observation_authority', 'execution_authority', 'dispatch_authority'):
                self.assertIs(result[key], False)

    def test_slot_reorder_duplicate_and_extra_frame_poison_transcript(self):
        vals = values_for(proposal('refresh-noop-v1'))
        t = self.transcript()
        with self.assertRaises(ValueError): t.append(frame('before', vals['before']))
        with self.assertRaises(ValueError): t.append(frame('imported', vals['imported']))
        self.assertEqual(t.result()['disposition'], 'unavailable')
        t = self.transcript(); t.append(frame('imported', vals['imported']))
        with self.assertRaises(ValueError): t.append(frame('imported', vals['imported']))
        t = self.transcript()
        for slot in ('imported', 'before', 'refresh'):
            t.append(frame(slot, vals[slot]))
        t.append(frame('finished', {'closed': True}))
        self.assertEqual(t.result()['disposition'], 'pass')
        with self.assertRaises(ValueError): t.append(frame('finished', {'closed': True}))
        self.assertEqual(t.result()['disposition'], 'unavailable')

    def test_truncated_multiline_duplicate_nonfinite_and_invalid_utf8_frames(self):
        valid = frame('imported', {})
        invalid = [valid[:-1], valid + valid, b'\xff\n',
            b'{"protocol":"cumulative-generated-probe-wire-v1","slot":"imported","slot":"imported","value":{}}\n',
            b'{"protocol":"cumulative-generated-probe-wire-v1","slot":"imported","value":{"x":1,"x":2}}\n',
            frame('imported', float('inf')),
            b'{"protocol":"cumulative-generated-probe-wire-v1","slot":"imported","value":1e999}\n']
        for raw in invalid:
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                wire.decode_frame(raw, expected_slot='imported', limits=self.limits)

    def test_envelope_and_boundary_shapes_are_exact(self):
        for raw, slot in [(frame('finished', {'closed': 1}), 'finished'),
                          (frame('finished', {'closed': True, 'passed': True}), 'finished'),
                          (frame('capture_job', {'job_id': 'probe-job', 'database': '/tmp/cumulative-probe/library.sqlite'}), 'capture_job'),
                          (frame('capture_job', {'job_id': 'other', 'database': '/tmp/m2/library.sqlite'}), 'capture_job'),
                          (frame('before', {}), 'imported')]:
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                wire.decode_frame(raw, expected_slot=slot, limits=self.limits)
        event = json.loads(frame('imported', {})); event['passed'] = True
        with self.assertRaises(ValueError):
            wire.decode_frame(json.dumps(event).encode() + b'\n', expected_slot='imported', limits=self.limits)

    def test_explicit_limits_reject_bool_zero_and_inconsistent_bounds(self):
        for args in [(True, 100, 10, 5), (10, 100, 0, 5), (101, 100, 10, 5)]:
            with self.subTest(args=args), self.assertRaises(ValueError): wire.WireLimits(*args)
        with self.assertRaises(ValueError): self.transcript(limits={'frame_bytes': 100})

    def test_exact_frame_bound_and_total_stream_quota_do_not_reset(self):
        vals = values_for(proposal('refresh-noop-v1'))
        raw = frame('imported', vals['imported'])
        limits = wire.WireLimits(len(raw), len(raw), 256, 16)
        t = self.transcript(limits=limits); t.append(raw)
        with self.assertRaisesRegex(ValueError, 'stream_limit'):
            t.append(frame('before', vals['before']))
        with self.assertRaises(ValueError):
            wire.decode_frame(raw, expected_slot='imported', limits=wire.WireLimits(len(raw)-1, len(raw), 256, 16))

    def test_structure_limits_apply_to_nested_candidate_values(self):
        raw = frame('imported', {'x': [[[['deep']]]]})
        for limits in (wire.WireLimits(1000, 1000, 3, 100), wire.WireLimits(1000, 1000, 100, 2)):
            with self.subTest(limits=limits), self.assertRaises(ValueError):
                wire.decode_frame(raw, expected_slot='imported', limits=limits)

    def test_capture_marker_cannot_supply_database_values_or_skip_capture(self):
        t = self.transcript('manifest-content-hash-v1')
        vals = values_for(proposal('manifest-content-hash-v1'))
        t.append(frame('submitted', vals['submitted']))
        t.append(frame('capture_job', {'job_id': 'probe-job', 'database': '/tmp/m2/library.sqlite'}))
        self.assertEqual(t.result()['disposition'], 'unavailable')
        with self.assertRaises(ValueError): t.append(frame('finished', {'closed': True}))
        self.assertIs(t.result()['observation_authority'], False)
        with self.assertRaises(ValueError): self.transcript().supply_captured_value(vals['captured_job'])

    def test_capture_value_is_bounded_unique_and_caller_mutation_isolated(self):
        t = self.transcript('manifest-content-hash-v1')
        vals = values_for(proposal('manifest-content-hash-v1'))
        t.append(frame('submitted', vals['submitted']))
        t.append(frame('capture_job', {'job_id': 'probe-job', 'database': '/tmp/m2/library.sqlite'}))
        t.supply_captured_value(vals['captured_job'])
        vals['captured_job']['manifest'] = 'wrong'
        t.append(frame('finished', {'closed': True}))
        self.assertEqual(t.result()['disposition'], 'pass')
        with self.assertRaises(ValueError): t.supply_captured_value({})
        t = self.transcript('manifest-content-hash-v1')
        t.append(frame('submitted', values_for(proposal('manifest-content-hash-v1'))['submitted']))
        t.append(frame('capture_job', {'job_id': 'probe-job', 'database': '/tmp/m2/library.sqlite'}))
        with self.assertRaises(ValueError): t.supply_captured_value('x' * 20000)

    def test_known_value_failure_survives_stopped_or_malformed_tail(self):
        t = self.transcript('completed-receipt-replay-v1')
        vals = values_for(proposal('completed-receipt-replay-v1'))
        for slot in ('submitted', 'prepared', 'original_receipt', 'before'):
            t.append(frame(slot, vals[slot]))
        vals['refresh']['record']['document']['text'] = 'wrong'
        t.append(frame('refresh', vals['refresh']))
        self.assertEqual(t.result()['disposition'], 'fail')
        with self.assertRaises(ValueError): t.append(b'bad\n')
        self.assertEqual(t.result()['disposition'], 'fail')
        self.assertIs(t.result()['wire_complete'], False)

    def test_returned_event_mutation_does_not_change_transcript(self):
        vals = values_for(proposal('refresh-noop-v1')); t = self.transcript()
        event = t.append(frame('imported', vals['imported'])); event['value']['document']['text'] = 'wrong'
        for slot in ('before', 'refresh'): t.append(frame(slot, vals[slot]))
        t.append(frame('finished', {'closed': True}))
        self.assertEqual(t.result()['disposition'], 'pass')

    def test_continuation_is_exact_and_cannot_inject_another_command(self):
        self.assertEqual(wire.continuation_bytes('before'), b'continue:before\n')
        for slot in ('before\ncontinue:refresh', 'unknown', ''):
            with self.assertRaises(ValueError): wire.continuation_bytes(slot)

    def test_fixed_child_source_compiles_and_emit_waits_for_exact_bounded_ack(self):
        namespace = {'__name__': 'host_authored_emit_helper_only'}
        exec(compile(driver.ADAPTER, '<fixed-child-source>', 'exec'), namespace)
        expected = b'continue:before\n'
        for ack in (expected, b'continue:refresh\n', b'', expected[:-1], expected + b'extra'):
            stdin = io.BytesIO(ack); stdout = io.StringIO()
            namespace['sys'] = SimpleNamespace(stdin=SimpleNamespace(buffer=stdin), stdout=stdout)
            if ack.startswith(expected):
                namespace['emit']('before', {'value': 1})
            else:
                with self.assertRaises(ValueError): namespace['emit']('before', {'value': 1})
            self.assertLessEqual(stdin.tell(), len(expected)+1)
            self.assertEqual(json.loads(stdout.getvalue()), {'protocol': 'cumulative-generated-probe-wire-v1',
                'slot': 'before', 'value': {'value': 1}})

    def test_staged_helper_is_fixed_and_requests_are_independent(self):
        one = driver.adapter_files(admitted(proposal()), released_requirements=RELEASED)
        two = driver.adapter_files(admitted(proposal('refresh-noop-v1')), released_requirements=RELEASED)
        self.assertEqual(set(one), {'child_driver.py', 'probe-request.json'})
        self.assertEqual(one['child_driver.py'], two['child_driver.py'])
        self.assertNotEqual(one['probe-request.json'], two['probe-request.json'])
        self.assertIn(b'/tmp/m2/library.sqlite', one['child_driver.py'])


if __name__ == '__main__':
    unittest.main()
