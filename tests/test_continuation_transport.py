"""Trusted fixture qualification; no model candidates, Docker or API calls."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from gossip_harness.blackbox_validator import json_equal
from gossip_harness.continuation_transport import (
    ADAPTER, BASELINE, BASELINE_SHA256, PROJECT, SAVE, INSPECT, LOAD, STEP,
    _Reference, _restore_reference, cmd, edit, golden_files, publish, reference,
    rehearsal_probe, scenario, validate_input,
)


class TrustedTransport:
    def __init__(self, files):
        self.temp = tempfile.TemporaryDirectory(prefix='trusted-transport-')
        self.root = Path(self.temp.name)
        for name, content in files.items():
            destination = self.root / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_text(content)

    def solve_many(self, payloads):
        script = 'import sys,json; sys.path.insert(0,sys.argv[1]); from solution import solve; print(json.dumps([solve(p) for p in json.load(sys.stdin)],allow_nan=False))'
        result = subprocess.run([sys.executable,'-I','-c',script,str(self.root)], input=json.dumps(payloads),
                                capture_output=True, text=True, timeout=30)
        if result.returncode:
            raise AssertionError(result.stderr)
        return json.loads(result.stdout)

    def run_trusted(self, script):
        result = subprocess.run([sys.executable,'-I','-c', 'import sys; sys.path.insert(0,sys.argv[1]);\n' + script,str(self.root)],
                                capture_output=True, text=True, timeout=30)
        if result.returncode:
            raise AssertionError(result.stderr)

    def close(self):
        self.temp.cleanup()


class ContinuationTransportTests(unittest.TestCase):
    def test_exact_real_baseline_and_fixed_adapter_scope(self):
        self.assertEqual(hashlib.sha256(BASELINE.encode()).hexdigest(), BASELINE_SHA256)
        self.assertEqual(PROJECT['initial_files']['gossip_harness/transport.py'], BASELINE)
        self.assertEqual(PROJECT['allowed_paths'], ('gossip_harness/transport.py',))
        for index in range(2):
            self.assertEqual(golden_files(index)['solution.py'], ADAPTER)
            self.assertEqual(golden_files(index)['gossip_harness/__init__.py'], '')
        self.assertNotIn('expected', ADAPTER)

    def test_golden_matches_independent_reference_all_cumulative_cases(self):
        cumulative = []
        for index, stage in enumerate(PROJECT['stages']):
            cumulative += list(stage['visible_cases']) + list(stage['hidden_cases'])
            trusted = TrustedTransport(stage['known_files'])
            try:
                answers = trusted.solve_many([case['input'] for case in cumulative])
                for case, answer in zip(cumulative, answers, strict=True):
                    with self.subTest(stage=index, case=case['id']):
                        self.assertTrue(json_equal(reference(index, case['input']),case['expected']))
                        self.assertTrue(json_equal(answer,case['expected']),case['id'])
            finally:
                trusted.close()

    def test_baseline_regressions_and_manual_no_cascade(self):
        payload = scenario([publish('c'), STEP, INSPECT, STEP, INSPECT], mode='bus', broker='a')
        expected = reference(0,payload)
        self.assertEqual(expected[1],1)
        self.assertEqual([len(expected[2]['records'][p]) for p in ['a','b','c']],[1,0,1])
        self.assertEqual(expected[3],1)
        self.assertEqual(expected[-1]['stats'],dict(rounds=2,contacts=4,event_deliveries=2,duplicate_deliveries=0))
        has_payload = scenario([publish(),cmd('has',peer='a',sequence=0,query_peer='a'),
                                cmd('has',peer='a',sequence=0,query_peer='b')])
        trusted = TrustedTransport(PROJECT['initial_files'])
        try:
            self.assertTrue(json_equal(trusted.solve_many([payload])[0],expected))
            self.assertTrue(json_equal(trusted.solve_many([has_payload])[0],[True,True,False]))
        finally:
            trusted.close()

    def test_disjoint_cases_requirements_and_model_friendly_probes(self):
        ids, public, private = set(), set(), set()
        for index, stage in enumerate(PROJECT['stages']):
            self.assertEqual(set(stage['requirements']),{c['requirement'] for c in stage['visible_cases']})
            self.assertEqual(set(stage['requirements']),{c['requirement'] for c in stage['hidden_cases']})
            for group, sink in (('visible_cases',public),('hidden_cases',private)):
                for case in stage[group]:
                    self.assertNotIn(case['id'],ids)
                    ids.add(case['id'])
                    sink.add(json.dumps(case['input'],sort_keys=True))
                    validate_input(index,case['input'])
            probes = [rehearsal_probe(index,n) for n in range(4)]
            self.assertEqual(len({json.dumps(p,sort_keys=True) for p in probes}),4)
            for probe in probes:
                self.assertNotIn(json.dumps(probe,sort_keys=True),public | private)
                answer = reference(index,probe)
                self.assertTrue(all(type(a) is bool and a or a == {'error':'invalid_snapshot'} for a in answer))
        self.assertFalse(public & private)
        self.assertEqual(len(ids),36)

    def test_strict_input_domain_does_not_hide_invalid_snapshots(self):
        for payload in (None, {}, {'config':{},'commands':[]}, scenario([dict(op='mystery')]),
                        scenario([cmd('load',name='missing')]), scenario([cmd('step',partitions=[['a'],['b']])]),
                        scenario([publish(sequence=True)]), scenario([publish(payload={'bad':float('nan')})])):
            with self.assertRaisesRegex(ValueError,'outside_input_domain'):
                validate_input(1,payload)
        oversized = scenario([publish(payload={'long':'x'*256}),cmd('advance',rounds=3)] + [INSPECT]*20,
                             peers=['a','b','c','d','e','f'],fanout=6)
        with self.assertRaisesRegex(ValueError,'outside_input_domain'):
            validate_input(1,oversized)
        bad = scenario([SAVE,cmd('load',name='s',edits=[edit([],None)])])
        validate_input(1,bad)
        self.assertEqual(reference(1,bad),[True,{'error':'invalid_snapshot'}])
        with self.assertRaisesRegex(ValueError,'outside_input_domain'):
            validate_input(0,scenario([SAVE,cmd('load',name='s',method='restore')]))
        with self.assertRaisesRegex(ValueError,'outside_input_domain'):
            validate_input(0,scenario([SAVE,cmd('mutate',name='s',edits=[edit(['version'],99)]),LOAD]))

    def test_adapter_rejects_non_json_snapshots_and_bad_edit_paths(self):
        trusted = TrustedTransport(golden_files(1))
        try:
            trusted.run_trusted('''from solution import digest
for value in ({'rng_state':(3,[1],None)}, {1:'coerced'}, {'value':float('inf')}):
 try: digest(value)
 except ValueError: pass
 else: raise AssertionError('accepted non-JSON checkpoint')
''')
        finally:
            trusted.close()
        with self.assertRaisesRegex(ValueError,'outside_input_domain'):
            validate_input(1,scenario([SAVE,cmd('mutate',name='s',edits=[edit(['stats'],1,'append')])]))

    def test_host_reference_is_independent_and_preserves_inputs(self):
        payload = rehearsal_probe(1,4)
        before = deepcopy(payload)
        self.assertTrue(reference(1,payload)[-1])
        self.assertEqual(before,payload)
        checkpoint = _Reference(dict(peers=['x'],mode='gossip')).snapshot()
        self.assertEqual(set(checkpoint),{'version','peers','mode','broker','fanout','batch_size','stats','records','rng_state'})
        self.assertEqual(checkpoint['rng_state'][1][-1],624)
        restored = _restore_reference(checkpoint)
        checkpoint['stats']['rounds'] = 10
        self.assertEqual(restored.stats['rounds'],0)

    def test_rejection_matrix_and_atomicity_direct_trusted_api(self):
        trusted = TrustedTransport(golden_files(1))
        try:
            trusted.run_trusted('''from copy import deepcopy
from gossip_harness.transport import Event, Mesh
m = Mesh(['a','b','c'],'gossip',seed=37)
m.publish('a',Event.create('a',1,'change',{'x':[1]}))
m.step(drop_rate=0.25)
base = m.snapshot()
mutations = [
 lambda s:s.update(version=True), lambda s:s.update(fanout=1.0),
 lambda s:s.update(peers=['b','a','c']), lambda s:s['stats'].update(rounds=True),
 lambda s:s['rng_state'].__setitem__(0,True), lambda s:s['rng_state'][1].__setitem__(0,2**32),
 lambda s:s['rng_state'][1].__setitem__(624,625), lambda s:s['rng_state'].__setitem__(1,s['rng_state'][1][:-1]), lambda s:s['rng_state'].__setitem__(2,float('nan')),
 lambda s:s['rng_state'].__setitem__(2,False), lambda s:s['records']['a'].append(deepcopy(s['records']['a'][0])),
 lambda s:s['records']['a'][0].update(sequence=True), lambda s:s['records']['a'][0]['payload'].update(x=(1,2)),
 lambda s:s.update(rng_state=tuple(s['rng_state'])), lambda s:s.update(records={})]
for mutate in mutations:
 bad = deepcopy(base); mutate(bad)
 for method in (Mesh.from_snapshot,m.restore):
  try: method(bad)
  except ValueError: pass
  else: raise AssertionError('accepted malformed snapshot')
  assert m.snapshot() == base
m.step(); assert m.restore(base) is None; assert m.snapshot() == base
base['records']['a'][0]['payload']['x'][0]=100
assert m.events('a')[0].payload == {'x':[1]}
''')
        finally:
            trusted.close()

    def test_independent_constructed_checkpoint_and_mutation_detection(self):
        trusted = TrustedTransport(golden_files(1))
        checkpoint = _Reference(dict(peers=['a','b'],mode='bus',broker='b',seed=52)).snapshot()
        try:
            trusted.run_trusted('''import json
from gossip_harness.transport import Mesh
value = json.loads(''' + repr(json.dumps(checkpoint)) + ''')
mesh = Mesh.from_snapshot(value)
assert mesh.snapshot() == value
assert mesh.restore(value) is None
''')
        finally:
            trusted.close()
        originals = golden_files(1)
        mutations = {
            'lost_rng':("mesh._rng.setstate((3, tuple(rng[1]), rng[2]))",'pass'),
            'aliased_stats':("mesh._stats = data['stats'].copy()","mesh._stats = data['stats']"),
            'coerced_rng_word':("require(all(type(v) is int and 0 <= v < 2**32 for v in rng[1][:-1]))",'pass'),
            'non_atomic':('replacement = type(self).from_snapshot(data)',"self._stats['rounds'] += 1\n        replacement = type(self).from_snapshot(data)"),
        }
        cases = [c for stage in PROJECT['stages'] for c in stage['hidden_cases']]
        for label, (old,new) in mutations.items():
            files = dict(originals)
            self.assertIn(old,files['gossip_harness/transport.py'])
            files['gossip_harness/transport.py'] = files['gossip_harness/transport.py'].replace(old,new,1)
            trusted = TrustedTransport(files)
            try:
                answers = trusted.solve_many([c['input'] for c in cases])
                self.assertTrue(any(not json_equal(a,c['expected']) for a,c in zip(answers,cases,strict=True)),label)
            finally:
                trusted.close()


if __name__ == '__main__':
    unittest.main()
