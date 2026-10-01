"""Trusted fixture/reference checks only; no model candidate code or API access."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import random
import subprocess
import sys
import tempfile
import unittest

from gossip_harness.blackbox_validator import json_equal
from gossip_harness.verification_buildgraph import (
    PROJECT, MUTANTS, POLICY_V1, POLICY_V2, INTRODUCED, reference, validate_input,
    put, build, plan, commit, imported, target, command, LIST, AUDIT, EXPORT,
)


class TrustedRepository:
    def __init__(self, files):
        self.temporary = tempfile.TemporaryDirectory(prefix='trusted-buildgraph-')
        self.root = Path(self.temporary.name)
        for name, content in files.items():
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)

    def solve(self, payload):
        script = 'import json,sys; sys.path.insert(0,sys.argv[1]); from solution import solve; print(json.dumps(solve(json.load(sys.stdin))))'
        completed = subprocess.run([sys.executable, '-I', '-c', script, str(self.root)],
                                   input=json.dumps(payload), text=True, capture_output=True, timeout=60)
        if completed.returncode:
            raise AssertionError(completed.stderr)
        return json.loads(completed.stdout)

    def close(self):
        self.temporary.cleanup()


class BuildGraphVerificationTests(unittest.TestCase):
    def test_initial_working_repository_requires_extension(self):
        repo = TrustedRepository(PROJECT['initial_files'])
        try:
            answer = repo.solve({'commands':[{'op':'put','id':'a','source':'v','priority':0}, LIST, {'op':'ready'}]})
            self.assertEqual(answer, [{'ok':'put','id':'a','changed':True},
                                     [{'id':'a','source':'v','priority':0,'deps':[],'digest':None}],
                                     {'error':'invalid'}])
        finally:
            repo.close()

    def test_known_solutions_all_cumulative_cases(self):
        cumulative = []
        for index, stage in enumerate(PROJECT['stages']):
            cumulative.extend(stage['visible_cases'] + stage['hidden_cases'])
            repo = TrustedRepository(stage['known_files'])
            try:
                for case in cumulative:
                    with self.subTest(stage=index, case=case['id']):
                        self.assertTrue(json_equal(reference(index, case['input']), case['expected']))
                        self.assertTrue(json_equal(repo.solve(case['input']), case['expected']), case['id'])
            finally:
                repo.close()

    def test_schema_fresh_cases_and_trusted_policy_patch(self):
        all_ids, public, hidden = set(), set(), set()
        self.assertEqual(len(PROJECT['stages']), 4)
        for index, stage in enumerate(PROJECT['stages']):
            self.assertEqual(len(stage['visible_cases']), 6)
            self.assertEqual(len(stage['hidden_cases']), 12)
            self.assertTrue(stage['title'])
            self.assertTrue(stage['specification'])
            for kind, collection in (('visible_cases', public), ('hidden_cases', hidden)):
                self.assertEqual(set(stage['requirements']), {c['requirement'] for c in stage[kind]})
                for case in stage[kind]:
                    self.assertNotIn(case['id'], all_ids)
                    all_ids.add(case['id'])
                    collection.add(json.dumps(case['input'], sort_keys=True))
                    validate_input(index, case['input'])
            expected_policy = POLICY_V2 if index == 3 else POLICY_V1
            self.assertEqual(stage['known_files']['policy.json'], expected_policy)
            updates = stage.get('trusted_updates', {})
            self.assertEqual(updates, {'policy.json':POLICY_V2} if index == 3 else {})
            for name, source in PROJECT['initial_files'].items():
                if name not in PROJECT['allowed_paths'] and name != 'policy.json':
                    self.assertEqual(stage['known_files'][name], source)
        self.assertEqual(len(all_ids), 72)
        self.assertFalse(public & hidden)
        self.assertEqual((len(public),len(hidden)),(24,48))
        self.assertLessEqual(sum(len(PROJECT['stages'][-1]['known_files'][p].splitlines()) for p in PROJECT['allowed_paths']), 500)

    def test_domain_rejects_envelopes_and_future_ops_but_not_bad_fields(self):
        outside = [None, [], {'commands':[],'extra':1}, {'commands':'x'}, {'commands':[{}]*25},
                   {'commands':[{'op':'put','id':'x','source':'a'*129}]},
                   {'commands':[{'op':'put','id':'x','priority':10**400}]},
                   {'commands':[{'op':'put','id':'x','priority':float('nan')}]},
                   {'commands':[{'op':'put','id':'x','deps':[None]*33}]}]
        for payload in outside:
            with self.subTest(payload_type=type(payload).__name__):
                with self.assertRaisesRegex(ValueError, '^outside_input_domain$'):
                    validate_input(3,payload)
        for op, introduced in INTRODUCED.items():
            for earlier in range(introduced):
                with self.assertRaisesRegex(ValueError, '^outside_input_domain$'):
                    validate_input(earlier, {'commands':[{'op':op}]})
        for value in (None, [], {}, True, 1, 1.0, 'unknown'):
            payload={'commands':[{'op':value}]}
            self.assertIsNone(validate_input(3,payload))
            self.assertEqual(reference(3,payload), [{'error':'invalid'}])
        self.assertEqual(reference(0,{'commands':[]}), [])

    def test_reference_manual_precedence_digest_and_retry_checks(self):
        self.assertEqual(reference(0,{'commands':[put('a',deps=['missing','a']),LIST]}), [{'error':'cycle'},[]])
        digest = hashlib.sha256(b'{"deps":[],"source":"v"}').hexdigest()
        self.assertEqual(reference(1,{'commands':[put('a','v'),build('a'),build('a')]}),
                         [{'ok':'put','id':'a','changed':True},{'id':'a','digest':digest,'cached':False},{'id':'a','digest':digest,'cached':True}])
        answer = reference(2,{'commands':[put('a'),commit('k',0,'ghost'),commit('k',1,'ghost'),commit('k',1,'a'),put('a','new'),commit('k',1,'a'),LIST]})
        self.assertEqual(answer[1:3],[{'error':'stale'},{'error':'missing'}])
        self.assertEqual(answer[3],answer[5])
        self.assertIsNone(answer[-1][0]['digest'])
        legacy=command('import',key='k',manifest={'schema':1,'targets':[{'name':'a','body':'x','needs':[]}]})
        answer=reference(3,{'commands':[legacy,build('a'),imported('k',target('a')),EXPORT,AUDIT]})
        self.assertEqual(answer[0],answer[2])
        self.assertEqual(answer[3],{'schema':2,'targets':[target('a')]})
        self.assertEqual([e['op'] for e in answer[4]],['import','build'])

    def test_reference_handles_type_crossproducts_without_mutating_payload(self):
        templates=[put('a'),build('a'),plan('a'),commit('k',0,'a'),imported('k',target('a'))]
        bad_values=(None,False,0,0.0,[],{},'')
        checked=0
        for template in templates:
            for field in template:
                for value in bad_values:
                    item=deepcopy(template)
                    item[field]=deepcopy(value)
                    payload={'commands':[item]}
                    before=deepcopy(payload)
                    validate_input(3,payload)
                    result=reference(3,payload)
                    self.assertEqual(payload,before)
                    self.assertEqual(len(result),1)
                    self.assertIsInstance(result[0],(dict,list,type(None)))
                    checked+=1
        self.assertGreaterEqual(checked,100)

    def test_deterministic_extra_differential_scenarios(self):
        rng=random.Random(9917)
        for stage_index,stage in enumerate(PROJECT['stages']):
            repo=TrustedRepository(stage['known_files'])
            try:
                for repeat in range(3):
                    commands=[put('a',str(rng.randrange(20)),rng.randrange(-3,4)),put('b','leaf',deps=['a'])]
                    if stage_index>=1:
                        commands += [build('a'),build('b'),put('a',str(rng.randrange(20))),build('b')]
                    if stage_index>=2:
                        commands += [plan('b'),commit('try',0,'b'),AUDIT]
                    if stage_index>=3:
                        commands += [imported('fresh',target('x',str(repeat))),EXPORT]
                    commands += [LIST]
                    payload={'commands':commands}
                    self.assertTrue(json_equal(repo.solve(payload),reference(stage_index,payload)))
            finally:
                repo.close()

    def test_reviewed_mutations_preserve_simple_behavior_but_fail_hidden_bar(self):
        cases={c['id'].removeprefix('buildgraph-'):c for stage in PROJECT['stages'] for c in stage['visible_cases']+stage['hidden_cases']}
        failures={'direct-only-invalidation':'hidden-diamond-cache',
                  'priority-discards-cache':'hidden-cache-noop-reordered-deps',
                  'commit-replay-rechecks-state':'hidden-replay-after-removal',
                  'old-policy-quota':'hidden-six-policy-quota'}
        for name,files in MUTANTS.items():
            repo=TrustedRepository(files)
            try:
                for case_id in ('visible-upsert','visible-digest-cache'):
                    case=cases[case_id]
                    self.assertTrue(json_equal(repo.solve(case['input']),case['expected']),name)
                case=cases[failures[name]]
                self.assertFalse(json_equal(repo.solve(case['input']),case['expected']),name)
            finally:
                repo.close()


if __name__=='__main__':
    unittest.main()
