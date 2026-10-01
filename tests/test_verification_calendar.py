"""Only trusted fixture implementations execute on host in these tests."""
from copy import deepcopy
import hashlib
import importlib.util
import json
from pathlib import Path
import random
import tempfile
import unittest

from gossip_harness.verification_calendar import (
    PROJECT, STAGES, MUTANTS, reference, validate_input, book, hold, command,
    item, SNAP, AUDIT,
)


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(",",":"),allow_nan=False)


class TrustedRepository:
    def __init__(self, files):
        self.temp=tempfile.TemporaryDirectory(prefix="trusted-calendar-test-")
        self.root=Path(self.temp.name)
        for name,content in files.items():
            path=self.root/name
            path.parent.mkdir(parents=True,exist_ok=True)
            path.write_text(content)
        spec=importlib.util.spec_from_file_location("trusted_calendar_fixture",self.root/"solution.py")
        self.module=importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.module)
    def solve(self,payload):
        return self.module.solve(deepcopy(payload))
    def close(self):
        self.temp.cleanup()


class CalendarFixtureTests(unittest.TestCase):
    def check_repo(self,files,cases):
        repo=TrustedRepository(files)
        try:
            answers=[repo.solve(c['input']) for c in cases]
        finally:
            repo.close()
        return [canonical(a)==canonical(c['expected']) for a,c in zip(answers,cases)]

    def test_schema_roster_and_frozen_boundaries(self):
        self.assertEqual(PROJECT['id'],'calendar')
        self.assertEqual(len(PROJECT['allowed_paths']),3)
        self.assertEqual(len(STAGES),4)
        seen_ids=set()
        seen_inputs=set()
        for index,stage in enumerate(STAGES):
            self.assertEqual(len(stage['visible_cases']),6)
            self.assertEqual(len(stage['hidden_cases']),12)
            self.assertTrue(stage['specification'])
            self.assertEqual(set(stage['requirements']),{case['requirement'] for case in stage['hidden_cases']})
            for path in ('solution.py','calendar_app/cli.py'):
                self.assertEqual(stage['known_files'][path],PROJECT['initial_files'][path])
            for case in stage['visible_cases']+stage['hidden_cases']:
                self.assertNotIn(case['id'],seen_ids)
                seen_ids.add(case['id'])
                encoded=canonical(case['input'])
                self.assertNotIn(encoded,seen_inputs)
                seen_inputs.add(encoded)
                self.assertIn(case['requirement'],stage['requirements'])
                self.assertIsNone(validate_input(index,case['input']))
                self.assertEqual(canonical(reference(index,case['input'])),canonical(case['expected']))
                self.assertLessEqual(len(case['input']['commands']),24)
        self.assertEqual(STAGES[-1]['trusted_updates'],{'policy.json':STAGES[-1]['known_files']['policy.json']})
        self.assertNotEqual(PROJECT['initial_files']['policy.json'],STAGES[-1]['trusted_updates']['policy.json'])

    def test_known_repositories_pass_all_cumulative_cases(self):
        cumulative=[]
        for index,stage in enumerate(STAGES):
            cumulative.extend(stage['visible_cases']+stage['hidden_cases'])
            matches=self.check_repo(stage['known_files'],cumulative)
            for case,matched in zip(cumulative,matches):
                with self.subTest(stage=index,case=case['id']):
                    self.assertTrue(matched)

    def test_working_baseline_has_specific_maintenance_defects(self):
        basic=[STAGES[0]['visible_cases'][0],STAGES[0]['visible_cases'][2]]
        self.assertEqual(self.check_repo(PROJECT['initial_files'],basic),[True,True])
        repairs=[STAGES[0]['visible_cases'][1],STAGES[0]['visible_cases'][4]]
        self.assertEqual(self.check_repo(PROJECT['initial_files'],repairs),[False,False])

    def test_mutants_pass_basic_and_fail_relevant_private_cases(self):
        probes={
            'closed_interval':(0,'left-adjacency'),
            'autocommit':(1,'bundle-rollback-existing'),
            'stale_fence':(2,'renew-replay'),
            'waitlist_head_blocking':(3,'user-limit-skip'),
        }
        for mutant,(stage_index,suffix) in probes.items():
            with self.subTest(mutant=mutant):
                files=MUTANTS[mutant]
                self.assertEqual(self.check_repo(files,[STAGES[0]['visible_cases'][0]]),[True])
                case=next(c for c in STAGES[stage_index]['hidden_cases'] if c['id'].endswith(suffix))
                self.assertEqual(self.check_repo(files,[case]),[False])

    def test_probe_domain_distinguishes_invalid_from_excluded(self):
        for index in range(4):
            payload={'commands':[None,[],{},True,1,1.5,{'op':[]},{'op':{}},{'op':'unknown'},book(start=True)]}
            self.assertIsNone(validate_input(index,payload))
            self.assertEqual(reference(index,payload),[{'error':'invalid'}]*10)
        for index,op in ((0,'move'),(1,'hold'),(2,'enqueue')):
            for payload in ({'commands':[{'op':op}]},{'commands':[{'op':'unknown','data':{'op':op}}]}):
                with self.assertRaisesRegex(ValueError,'^outside_input_domain$'):
                    validate_input(index,payload)
        for bad in ({'commands':[SNAP]*25},{'commands':{}},{'commands':[],'extra':0},
                    {'commands':[float('nan')]},{'commands':[float('inf')]},
                    {'commands':[{'op':'book','rid':'\ud800'}]}, {'commands':[],'legacy':[]}):
            with self.assertRaisesRegex(ValueError,'^outside_input_domain$'):
                validate_input(0,bad)
        for stage in (-1,4,True,1.0):
            with self.assertRaisesRegex(ValueError,'^outside_input_domain$'):
                validate_input(stage,{'commands':[]})
        # Finite malformed numbers remain domain inputs, including huge floats.
        self.assertEqual(reference(0,{'commands':[1e308,-1e308]}),[{'error':'invalid'}]*2)
        self.assertIsNone(validate_input(3,{'commands':[],'legacy':[]}))

    def test_output_domain_bound_and_active_limit_grandfathering(self):
        commands=[command('batch',str(i),commands=[command('book',**item(f'r-{i}-{j}-'+'x'*20,resource=f'resource-{i}-{j}-'+'x'*15,user='u'*32,start=j,end=j+1)) for j in range(4)]) for i in range(12)]+[SNAP]*12
        with self.assertRaisesRegex(ValueError,'^outside_input_domain$'):
            validate_input(2,{'commands':commands})
        case=next(c for c in STAGES[3]['hidden_cases'] if c['id'].endswith('move-excludes-self'))
        self.assertEqual(case['expected'][:5],[{'ok':True,'token':1},{'ok':True},{'ok':True,'token':2},{'ok':True},{'ok':True}])
        self.assertEqual(self.check_repo(STAGES[3]['known_files'],[case]),[True])

    def test_generated_type_cross_product_matches_sqlite_reference(self):
        commands=[]
        for value in (None,False,1.5,[],{},'',-1,1001):
            commands.append(book(key='k',start=value))
            commands.append(command('cancel','k',rid=value))
        payload={'commands':commands[:16]+[SNAP,AUDIT]}
        expected=reference(3,payload)
        self.assertEqual(self.check_repo(STAGES[3]['known_files'],[{'input':payload,'expected':expected}]),[True])

    def test_independent_random_interleavings_match_sqlite(self):
        rng=random.Random(20261001)
        cases=[]
        for scenario in range(8):
            commands=[]
            for i in range(10):
                op=rng.choice(('book','cancel','move','hold','renew','confirm','expire'))
                rid=rng.choice(('a','b','c'))
                start=rng.randrange(0,10)
                if op=='book':
                    c=book(str(i),rid=rid,start=start,end=start+3)
                elif op=='hold':
                    c=hold(str(i),rid=rid,start=start,end=start+3,now=0,ttl=4)
                elif op=='cancel':
                    c=command(op,str(i),rid=rid)
                elif op=='move':
                    c=command(op,str(i),rid=rid,start=start,end=start+2)
                elif op=='expire':
                    c=command(op,str(i),now=rng.randrange(0,6))
                elif op=='confirm':
                    c=command(op,str(i),rid=rid,token=rng.choice((1,2)),now=rng.randrange(0,6))
                else:
                    c=command(op,str(i),rid=rid,token=rng.choice((1,2)),now=rng.randrange(0,6),ttl=3)
                commands.append(c)
            payload={'commands':commands+[SNAP,AUDIT]}
            cases.append({'input':payload,'expected':reference(3,payload)})
        self.assertEqual(self.check_repo(STAGES[3]['known_files'],cases),[True]*len(cases))

    def test_policy_is_read_from_trusted_file(self):
        # Runtime policy perturbation catches hard-coded values in the trusted
        # solution; this is a reference check, not part of the frozen case pools.
        files=dict(STAGES[3]['known_files'])
        files['policy.json']='{"max_duration":3,"max_active_per_user":1}\n'
        payload={'commands':[book(),book('b',rid='b',end=5),book('c',rid='c',resource='other',user='user-b',end=5),SNAP]}
        repo=TrustedRepository(files)
        try:
            result=repo.solve(payload)
        finally:
            repo.close()
        self.assertEqual(result[:3],[{'error':'policy'},{'ok':True},{'error':'policy'}])

    def test_explicit_reference_expectations_and_input_immutability(self):
        payload={'commands':[hold(),command('renew','r',rid='a',token=1,now=1,ttl=4),command('confirm','c',rid='a',token=1,now=2),command('confirm','d',rid='a',token=2,now=2),AUDIT]}
        original=deepcopy(payload)
        answers=reference(2,payload)
        self.assertEqual(answers[:4],[{'ok':True,'token':1},{'ok':True,'token':2},{'error':'stale'},{'ok':True}])
        self.assertEqual(answers[-1],[{'seq':1,'key':'h','op':'hold'},{'seq':2,'key':'r','op':'renew'},{'seq':3,'key':'d','op':'confirm'}])
        self.assertEqual(payload,original)


if __name__=='__main__':
    unittest.main()
