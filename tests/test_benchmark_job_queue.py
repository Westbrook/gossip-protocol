"""Checks for author-written queue fixtures, never model-generated candidates."""
from copy import deepcopy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from gossip_harness import benchmark_job_queue as fixture
from gossip_harness.blackbox_validator import json_equal


class TrustedQueue:
    """Execute only fixtures authored in the imported benchmark module."""

    def __init__(self, files):
        self.temp = tempfile.TemporaryDirectory(prefix='trusted-queue-fixture-')
        self.root = Path(self.temp.name)
        for name, content in files.items():
            target = self.root / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content)

    def solve_many(self, payloads):
        script = "import sys,json; sys.path.insert(0,sys.argv[1]); from solution import solve; print(json.dumps([solve(p) for p in json.load(sys.stdin)],allow_nan=False))"
        result = subprocess.run([sys.executable, '-I', '-c', script, str(self.root)],
                                input=json.dumps(payloads), capture_output=True,
                                text=True, timeout=180)
        if result.returncode:
            raise AssertionError(result.stderr)
        return json.loads(result.stdout)

    def close(self):
        self.temp.cleanup()


class BenchmarkJobQueueTests(unittest.TestCase):
    def test_versioned_scope_and_initial_implemented_baseline(self):
        self.assertEqual(fixture.PROJECT['id'], 'job-queue')
        self.assertEqual(fixture.CONTRACT, 'benchmark-job-queue-v1')
        self.assertEqual(len(fixture.ALLOWED), 3)
        self.assertEqual(len(fixture.STAGES), 2)
        self.assertNotIn("elif op == 'finish'", fixture.INITIAL[fixture.ALLOWED[2]])
        self.assertNotIn("elif op == 'fail'", fixture.INITIAL[fixture.ALLOWED[2]])
        self.assertNotIn('def receipt(', fixture.INITIAL[fixture.ALLOWED[0]])
        self.assertNotIn('CREATE TABLE IF NOT EXISTS receipts', fixture.INITIAL[fixture.ALLOWED[0]])
        self.assertNotIn('def receipt(', fixture.known_files(0)[fixture.ALLOWED[0]])
        for index in (0, 1):
            files = fixture.known_files(index)
            self.assertEqual(set(files), set(fixture.INITIAL))
            for name in files:
                compile(files[name], name, 'exec')
                if name not in fixture.ALLOWED:
                    self.assertEqual(files[name], fixture.INITIAL[name])
        trusted = TrustedQueue(fixture.INITIAL)
        try:
            cases = fixture.BASE_PUBLIC + fixture.BASE_PRIVATE
            answers = trusted.solve_many([case['input'] for case in cases])
            for case, answer in zip(cases, answers, strict=True):
                self.assertTrue(json_equal(answer, case['expected']), case['id'])
        finally:
            trusted.close()

    def test_golden_and_equivalent_match_independent_oracle(self):
        cumulative = []
        for index, stage in enumerate(fixture.STAGES):
            cumulative.extend(stage['visible_cases'] + stage['hidden_cases'])
            for control in fixture.correct_controls(index):
                trusted = TrustedQueue(control['files'])
                try:
                    answers = trusted.solve_many([case['input'] for case in cumulative])
                    for case, answer in zip(cumulative, answers, strict=True):
                        with self.subTest(stage=index, control=control['id'], case=case['id']):
                            self.assertTrue(json_equal(answer, case['expected']), (answer, case['expected']))
                            self.assertTrue(json_equal(fixture.reference(index, case['input']), case['expected']))
                finally:
                    trusted.close()

    def test_public_private_requirements_identity_and_reference_purity(self):
        identities, public, private = set(), set(), set()
        for index, stage in enumerate(fixture.STAGES):
            for group, sink in (('visible_cases', public), ('hidden_cases', private)):
                self.assertEqual(set(stage['requirements']), {case['requirement'] for case in stage[group]})
                for case in stage[group]:
                    self.assertNotIn(case['id'], identities)
                    identities.add(case['id'])
                    sink.add(json.dumps(case['input'], sort_keys=True))
                    payload = deepcopy(case['input'])
                    actual = fixture.reference(index, payload)
                    self.assertEqual(payload, case['input'])
                    self.assertTrue(json_equal(actual, case['expected']))
        self.assertFalse(public & private)
        self.assertEqual(len(identities), 52)

    def test_expiry_boundary_backoff_origin_and_attempt_exhaustion_manual(self):
        payload = {'commands': [fixture.enqueue('a', max_attempts=2, retry_delay=3),
                                fixture.claim(lease=2), fixture.tick(2), fixture.claim(),
                                fixture.tick(3), fixture.claim(lease=2), fixture.tick(10), fixture.SNAP]}
        output = fixture.reference(0, payload)
        self.assertEqual(output[2], {'now': 2, 'expired': ['a']})
        self.assertEqual(output[3], {'job': None})
        self.assertEqual(output[5]['job']['token'], 2)
        row = output[-1]['jobs'][0]
        self.assertEqual((row['status'], row['attempts'], row['available_at']), ('dead', 2, 10))
        self.assertEqual((row['worker'], row['lease_until'], row['seq']), (None, None, 1))

    def test_stale_workers_and_renewal_manual(self):
        payload = {'commands': [fixture.enqueue('a'), fixture.claim(lease=3), fixture.tick(3),
                                fixture.claim(lease=8), fixture.tick(2), fixture.renew(token=2, extend=4),
                                fixture.ack(token=1), fixture.fail(token=1), fixture.renew(token=1), fixture.SNAP]}
        result = fixture.reference(0, payload)
        self.assertEqual(result[5], {'ok': 'renewed', 'id': 'a', 'lease_until': 15})
        self.assertEqual(result[6:9], [{'error': 'stale'}] * 3)
        self.assertEqual(result[-1]['jobs'][0]['attempts'], 2)

    def test_ready_filter_precedes_priority_and_retry_keeps_fifo(self):
        payload = {'commands': [fixture.enqueue('urgent', priority=3, delay=5), fixture.enqueue('z'), fixture.enqueue('a'),
                                fixture.claim(lease=2), fixture.tick(2), fixture.claim(), fixture.SNAP]}
        result = fixture.reference(0, payload)
        self.assertEqual(result[3]['job']['id'], 'z')
        self.assertEqual(result[5]['job']['id'], 'z')
        self.assertEqual(result[5]['job']['seq'], 2)

    def test_complete_rollback_then_retry_and_sequence_manual(self):
        payload = {'commands': [fixture.enqueue('a'), fixture.enqueue('used'), fixture.claim(),
                                fixture.finish('f', 'a', 1, fixture.child('fresh'), fixture.child('used')),
                                fixture.receipt('f'), fixture.SNAP,
                                fixture.finish('f', 'a', 1, fixture.child('z'), fixture.child('b')), fixture.SNAP]}
        result = fixture.reference(1, payload)
        self.assertEqual(result[3], {'error': 'exists'})
        self.assertEqual(result[4], {'found': False})
        self.assertEqual([(row['id'], row['status']) for row in result[5]['jobs']], [('a', 'running'), ('used', 'ready')])
        self.assertEqual(result[6], {'ok': 'finished', 'id': 'a', 'successors': ['z', 'b']})
        rows = {row['id']: row for row in result[7]['jobs']}
        self.assertEqual((rows['z']['seq'], rows['b']['seq']), (3, 4))

    def test_replay_identity_validation_and_conflict_precedence_manual(self):
        first = fixture.finish('f', 'a', 1, fixture.child('b'))
        normalized = fixture.finish('f', 'a', 1, fixture.child('b', priority=0, delay=0, max_attempts=3, retry_delay=0))
        payload = {'commands': [fixture.enqueue('a'), fixture.claim(), first, fixture.tick(20),
                                normalized, fixture.finish('f', 'missing'),
                                dict(first, successors=[fixture.child('b', delay=True)]), fixture.receipt('f'), fixture.SNAP]}
        result = fixture.reference(1, payload)
        self.assertEqual(result[2], result[4])
        self.assertEqual(result[5], {'error': 'conflict'})
        self.assertEqual(result[6], {'error': 'invalid'})
        self.assertEqual(result[7], {'found': True, 'response': result[2]})
        self.assertEqual(result[-1]['jobs'][1]['available_at'], 0)

    def test_typed_invalid_commands_are_atomic(self):
        malformed = [None, [], {'op': []}, {'op': {}}, fixture.enqueue('a', priority=True),
                     fixture.enqueue('a', retry_delay=False), fixture.claim(lease=None), fixture.tick(True),
                     fixture.ack(token=True), {'op': 'snapshot', 'unexpected': 1}, fixture.finish('f', 'a', 1, {'id': 'x'})]
        output = fixture.reference(1, {'commands': malformed + [fixture.SNAP]})
        self.assertEqual(output[:-1], [{'error': 'invalid'}] * len(malformed))
        self.assertEqual(output[-1], {'now': 0, 'jobs': []})

    def test_input_domain_and_future_operations(self):
        for stage in (-1, 2, False):
            with self.assertRaisesRegex(ValueError, 'outside_input_domain'):
                fixture.validate_input(stage, {'commands': [fixture.SNAP]})
        for payload in ({}, {'commands': []}, {'commands': [fixture.SNAP] * 25},
                        {'commands': [{'op': 'x', 'value': float('nan')}]}, {'commands': [fixture.SNAP], 'x': 1}):
            with self.assertRaisesRegex(ValueError, 'outside_input_domain'):
                fixture.validate_input(1, payload)
        for command in (fixture.finish(), fixture.receipt()):
            with self.assertRaisesRegex(ValueError, 'future operation'):
                fixture.validate_input(0, {'commands': [command]})
            fixture.validate_input(1, {'commands': [command]})

    def test_control_sources_and_rehearsal_probes(self):
        frozen = {json.dumps(case['input'], sort_keys=True) for stage in fixture.STAGES
                  for group in ('visible_cases', 'hidden_cases') for case in stage[group]}
        for stage in (0, 1):
            controls = fixture.correct_controls(stage)
            self.assertEqual([control['id'] for control in controls], ['correct', 'equivalent-sort'])
            self.assertEqual(controls[0]['files'], fixture.known_files(stage))
            self.assertNotEqual(controls[0]['files'], controls[1]['files'])
            probes = [fixture.rehearsal_probe(stage, slot) for slot in range(4)]
            self.assertEqual(len({json.dumps(probe, sort_keys=True) for probe in probes}), 4)
            for probe in probes:
                self.assertNotIn(json.dumps(probe, sort_keys=True), frozen)
                self.assertTrue(fixture.reference(stage, probe))

    def test_fault_banks_are_scoped_private_witnessed_and_compile(self):
        for stage in (0, 1):
            variants = fixture.fault_bank(stage)
            self.assertEqual(len(variants), 6)
            self.assertEqual(len({row['family'] for row in variants}), 6)
            hidden = {case['id']: case for group in fixture.STAGES[:stage + 1] for case in group['hidden_cases']}
            for variant in variants:
                changed = {name for name in variant['files'] if variant['files'][name] != fixture.known_files(stage)[name]}
                self.assertTrue(changed)
                self.assertLessEqual(changed, set(fixture.ALLOWED))
                for path, source in variant['files'].items():
                    compile(source, path, 'exec')
                for case in variant['witness_cases']:
                    self.assertEqual(case, hidden[case['id']])

    def test_authored_mutants_survive_public_and_fail_private_witness(self):
        for stage in (0, 1):
            public = [case for group in fixture.STAGES[:stage + 1] for case in group['visible_cases']]
            for variant in fixture.fault_bank(stage):
                cases = public + variant['witness_cases']
                trusted = TrustedQueue(variant['files'])
                try:
                    answers = trusted.solve_many([case['input'] for case in cases])
                finally:
                    trusted.close()
                with self.subTest(stage=stage, fault=variant['id']):
                    for case, answer in zip(public, answers[:len(public)], strict=True):
                        self.assertTrue(json_equal(answer, case['expected']), (case['id'], answer, case['expected']))
                    self.assertTrue(any(not json_equal(answer, case['expected'])
                                        for case, answer in zip(variant['witness_cases'], answers[len(public):], strict=True)))


if __name__ == '__main__':
    unittest.main()
