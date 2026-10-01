"""Supplementary binding tests and trusted stub execution only; no model sources run."""
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
SPEC=importlib.util.spec_from_file_location('persistence_diagnostic',ROOT/'analysis/run_verification_persistence_diagnostic.py')
runner=importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(runner)


def save(path,value):
    path.parent.mkdir(parents=True,exist_ok=True); path.write_text(json.dumps(value))


def story(identity='story'):
    return dict(id=identity,requirements=['R'],contract_basis='test contract',classification='supplementary',
                required_contract_vs_extra_assumptions=dict(required_contract=['R'],extra_assumptions=['handoff']),
                steps=[dict(stage_index=i,command={'op':'observe'}) for i in range(4)],expected_outputs=[1,2,3,4])


def fake_receipt(files,cases,context,passed=True):
    outcomes=[dict(index=i,id=case['id'],requirement=case['requirement'],
                   passed=passed,status='passed' if passed else 'wrong_answer',
                   actual=case['expected'] if passed else None) for i,case in enumerate(cases)]
    return dict(schema_version=1,protocol='gossip-blackbox-v1',passed=passed,status='passed' if passed else 'failed',
                source_sha256=runner.digest(files),suite_sha256=runner.digest(cases),image_id=context['image'],
                adapter_sha256=runner.frozen.ADAPTER_SHA256,case_timeout_seconds=runner.CASE_TIMEOUT,
                timeout_seconds=runner.SUITE_TIMEOUT,cleanup_verified=True,exit_code=0,timed_out=False,
                output_truncated=False,input_delivery_failed=False,case_count=len(cases),outcomes=outcomes)


class FrozenStudy:
    def __init__(self,directory):
        self.root=Path(directory)/'study'; self.output=Path(directory)/'diagnostic'; self.root.mkdir()
        self.stories={p:[story(p+'-1'),story(p+'-2')] for p in runner.PROJECT_IDS}; self.git={}
        projects=[dict(id=p,initial_files={'solution.py':'raise AssertionError("never run model source on host")',
                         'backend.py':'initial','policy.json':'old'},allowed_paths=['backend.py'],
                       stages=[dict(trusted_updates={'policy.json':'new'} if i==3 else {}) for i in range(4)])
                  for p in runner.PROJECT_IDS]
        sources={}
        for name in runner.frozen.CORE:
            raw=(ROOT/'gossip_harness'/name).read_bytes(); sources[name]=runner.sha(raw)
            path=self.root/'source-snapshot'/'gossip_harness'/name; path.parent.mkdir(parents=True,exist_ok=True); path.write_bytes(raw)
        (self.root/'study-plan.json').write_text('{}')
        self.contract=dict(protocol='verification-quality-v1',policies=list(runner.frozen.POLICIES),milestones=4,
             project_ids=list(runner.PROJECT_IDS),initial_candidates={'portfolio-reviewed':4},image='sha256:'+'a'*64,
             sources=sources,plan_sha256=runner.sha(b'{}'),fixture_sha256=runner.digest(projects),
             controller_runtime=runner.frozen.runtime())
        self.report=dict(experiment='verification-quality-v1',mode='live',status='finished',repetitions=3,
                         unexecuted=[],censored=[],active_case=None,contract=self.contract,contract_sha=runner.digest(self.contract),cases=[])
        roster=[]
        for project in projects:
            p=project['id']
            for policy in runner.frozen.POLICIES:
                for repetition in range(3):
                    roster.append((p,policy,repetition)); identity=f'{p}-{policy}-{repetition}'; root=self.root/identity
                    # Both a primary success and primary failure reach stage four.
                    reached=p=='buildgraph' and repetition==0 and policy!='portfolio-reviewed'
                    stages=[]; files=dict(project['initial_files'])
                    for i in range(4 if reached else 1):
                        files={**files,'backend.py':f'model source version {i}',**project['stages'][i]['trusted_updates']}
                        source=root/f'stage-{i}'/'candidate-A.git'; source.mkdir(parents=True)
                        binding=dict(store_path=str(source.resolve()),tip_sha=runner.digest(files)[:40],files_sha256=runner.digest(files))
                        self.git[(str(source.resolve()),binding['tip_sha'])]=dict(files)
                        save(source.with_suffix('.json'),dict(files=files,binding=binding))
                        stage=dict(stage_index=i,completed=i<3 or policy=='strong-reviewed',files=dict(files),
                                   selected='candidate-A',selected_binding=binding,candidates={'candidate-A':{'binding':binding}})
                        stages.append(stage); save(root/f'stage-{i}'/'result.json',stage)
                    state=dict(contract_sha=self.report['contract_sha'],stages=stages,files=files)
                    raw=json.dumps(state).encode(); (root/'trajectory.json').write_bytes(raw)
                    primary=dict(run_id=identity,project_id=p,policy=policy,repetition=repetition,root=str(root.resolve()),
                                 accepted=reached and policy=='strong-reviewed',status='accepted' if reached and policy=='strong-reviewed' else 'failed',
                                 trajectory_sha256=runner.sha(raw),files_sha256=runner.digest(files))
                    save(root/'result.json',primary); self.report['cases'].append(primary)
        save(self.root/'results.json',self.report); save(self.root/'preregistered.json',dict(contract=self.contract,roster=roster))
        save(self.root/'fixtures.json',dict(projects=projects))
    def store(self,path):
        owner=self
        class FakeStore:
            def read_files(self,tip): return deepcopy(owner.git[(str(Path(path).resolve()),tip)])
        return FakeStore()
    def prepare(self):
        with patch.object(runner,'GitStore',side_effect=self.store),patch.object(runner,'load_stories',return_value=(self.stories,{})):
            return runner.prepare(self.root,self.output)


class PersistenceTests(unittest.TestCase):
    def test_all_four_stage_trajectories_including_primary_failure_are_eligible(self):
        with tempfile.TemporaryDirectory() as directory:
            study=FrozenStudy(directory); plan=study.prepare()
            self.assertEqual(len(plan['trajectories']),2); self.assertEqual(len(plan['skipped']),16)
            self.assertEqual({t['primary_accepted'] for t in plan['trajectories']},{True,False})
            self.assertTrue(all(len(t['chain'])==4 for t in plan['trajectories']))
            self.assertFalse(study.output.exists())
            for trajectory in plan['trajectories']:
                self.assertEqual(trajectory['files']['solution.py'],runner.PERSISTENCE_ADAPTER)
                self.assertEqual(trajectory['files']['versions/stage-0/policy.json'],'old')
                self.assertEqual(trajectory['files']['versions/stage-3/policy.json'],'new')

    def test_unfinished_duplicate_or_tampered_study_is_rejected_before_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            study=FrozenStudy(directory)
            study.report['status']='running'; save(study.root/'results.json',study.report)
            with self.assertRaisesRegex(ValueError,'Entire live study'): study.prepare()
            study.report['status']='finished'; study.report['cases'][-1]=study.report['cases'][0]
            save(study.root/'results.json',study.report)
            with self.assertRaisesRegex(ValueError,'roster'): study.prepare()
        with tempfile.TemporaryDirectory() as directory:
            study=FrozenStudy(directory)
            key=next(key for key in study.git if 'strong-reviewed-0/stage-0' in key[0]); study.git[key]['backend.py']='changed'
            with self.assertRaisesRegex(ValueError,'immutable Git files'): study.prepare()

    def test_story_limits_forward_versions_and_host_only_expectations(self):
        cases=[story('a'),story('b')]
        runner.validate_stories(cases)
        execution=runner.execution_cases('calendar',cases)
        self.assertEqual(set(execution[0]['input']),{'project_id','steps'})
        self.assertNotIn('expected_outputs',json.dumps(execution[0]['input']))
        self.assertNotIn('classification',execution[0]['input'])
        bad=deepcopy(cases); bad[0]['steps']=[bad[0]['steps'][0]]*17; bad[0]['expected_outputs']=[0]*17
        with self.assertRaisesRegex(ValueError,'1..16'): runner.validate_stories(bad)
        bad=deepcopy(cases); bad[0]['steps'][1],bad[0]['steps'][2]=bad[0]['steps'][2],bad[0]['steps'][1]
        with self.assertRaisesRegex(ValueError,'forward order'): runner.validate_stories(bad)

    def test_trusted_adapter_keeps_database_and_uses_correct_cli_conventions(self):
        # Every executable here is a literal trusted stub, never model code.
        stub='''import json,sqlite3,sys\nfrom pathlib import Path\ndb=sqlite3.connect(sys.argv[1])\ndb.execute("CREATE TABLE IF NOT EXISTS counter(n INTEGER)")\nn=db.execute("SELECT COUNT(*) FROM counter").fetchone()[0]+1\ndb.execute("INSERT INTO counter VALUES (?)",(n,));db.commit()\ncommand=json.loads(sys.argv[2]) if len(sys.argv)>2 else json.load(sys.stdin)\npolicy=json.loads((Path(__file__).parent.parent/'policy.json').read_text())\nprint(json.dumps(dict(n=n,version=policy['version'],command=command)))\n'''
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); (root/'solution.py').write_text(runner.PERSISTENCE_ADAPTER)
            for i in range(4):
                version=root/'versions'/f'stage-{i}'; version.mkdir(parents=True)
                (version/'policy.json').write_text(json.dumps({'version':i}))
                for project in runner.PROJECT_IDS:
                    package=version/(project+'_app'); package.mkdir(); (package/'cli.py').write_text(stub)
            for project in runner.PROJECT_IDS:
                payload=dict(project_id=project,steps=story()['steps'])
                script='import json,solution,sys;print(json.dumps(solution.solve(json.load(sys.stdin))))'
                result=subprocess.run([sys.executable,'-c',script],input=json.dumps(payload),cwd=root,capture_output=True,text=True,timeout=20)
                self.assertEqual(result.returncode,0,result.stderr); value=json.loads(result.stdout)
                self.assertEqual(value['completed_steps'],4); self.assertIsNone(value['failure'])
                self.assertEqual([v['n'] for v in value['outputs']],[1,2,3,4])
                self.assertEqual([v['version'] for v in value['outputs']],[0,1,2,3])
            failing=root/'versions/stage-2/calendar_app/cli.py'
            failing.write_text('import sys;sys.stderr.write("x"*2048);raise SystemExit(5)')
            result=subprocess.run([sys.executable,'-c',script],input=json.dumps(dict(project_id='calendar',steps=story()['steps'])),
                                  cwd=root,capture_output=True,text=True,timeout=20)
            value=json.loads(result.stdout)
            self.assertEqual(value['completed_steps'],2)
            self.assertEqual([v['n'] for v in value['outputs']],[1,2])
            self.assertEqual(value['failure']['step_index'],2)
            self.assertEqual(value['failure']['kind'],'cli_exit')
            self.assertEqual(len(value['failure']['stderr']),1024)

    def test_golden_qualification_binds_both_receipts_and_adapter_context(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); plan=runner.golden_plan()
            rows=[]
            for trajectory in plan['trajectories']:
                receipt=fake_receipt(trajectory['files'],trajectory['suite'],plan['context'])
                path=root/'receipts'/(trajectory['run_id']+'.json'); save(path,receipt)
                rows.append(dict(run_id=trajectory['run_id'],project_id=trajectory['project_id'],receipt_sha256=runner.sha(path.read_bytes())))
            own=Path(runner.__file__).read_bytes(); helper=Path(runner.frozen.__file__).read_bytes()
            (root/'runner.py').write_bytes(own); (root/'pool-binding-helpers.py').write_bytes(helper)
            manifest=dict(mode='golden-rehearsal',execution_context=plan['context'],runner_sha256=runner.sha(own),
                          binding_helpers_sha256=runner.sha(helper))
            save(root/'manifest.json',manifest)
            save(root/'results.json',dict(status='finished',all_passed=True,trajectories=rows))
            binding=runner.qualify(root,plan)
            self.assertEqual(binding['directory'],str(root.resolve()))
            altered={**plan['context'],'adapter_sha256':'wrong'}
            with self.assertRaisesRegex(ValueError,'does not match'):
                runner.qualify(root,{**plan,'context':altered,'inputs':{}})
            save(root/'manifest.json',{**manifest,'runner_sha256':'old'})
            with self.assertRaisesRegex(ValueError,'implementation changed'):
                runner.qualify(root,{**plan,'inputs':{}})
            save(root/'manifest.json',manifest)
            path=root/'receipts'/(rows[0]['run_id']+'.json')
            receipt=json.loads(path.read_text()); receipt['source_sha256']='wrong'; save(path,receipt)
            rows[0]['receipt_sha256']=runner.sha(path.read_bytes())
            save(root/'results.json',dict(status='finished',all_passed=True,trajectories=rows))
            with self.assertRaisesRegex(ValueError,'source_sha256'):
                runner.qualify(root,{**plan,'inputs':{}})

    def test_failed_receipt_is_retained_before_rejection_and_no_retry(self):
        with tempfile.TemporaryDirectory() as directory:
            study=FrozenStudy(directory); plan=study.prepare(); calls=[]
            class BadValidator:
                def __init__(self,*args,**kwargs): self.last_receipt={}
                def preflight(self): return True,'fake'
                def evaluate(self,files,cases):
                    calls.append(files); return {**fake_receipt(files,cases,plan['context']),'cleanup_verified':False}
            with patch.object(runner,'prepare',return_value=plan),patch.object(runner,'qualify',return_value={}),patch.object(runner,'BlackboxValidator',BadValidator):
                with self.assertRaisesRegex(ValueError,'infrastructure failure'):
                    runner.run(study.root,study.output,qualification=Path(directory)/'fake-proof')
            self.assertEqual(len(calls),1)
            receipts=list((study.output/'receipts').glob('*.json')); self.assertEqual(len(receipts),1)
            self.assertIs(json.loads(receipts[0].read_text())['cleanup_verified'],False)
            result=json.loads((study.output/'results.json').read_text()); self.assertEqual(result['status'],'interrupted')
            self.assertFalse(result['executions'][0]['verified'])

    def test_wrong_answers_are_recorded_without_repair_or_primary_changes(self):
        with tempfile.TemporaryDirectory() as directory:
            study=FrozenStudy(directory); plan=study.prepare()
            class FakeValidator:
                def __init__(self,*args,**kwargs): pass
                def preflight(self): return True,'fake'
                def evaluate(self,files,cases): return fake_receipt(files,cases,plan['context'],passed=False)
            with patch.object(runner,'prepare',return_value=plan),patch.object(runner,'qualify',return_value={}),patch.object(runner,'BlackboxValidator',FakeValidator):
                result=runner.run(study.root,study.output,qualification=Path(directory)/'fake-proof')
            self.assertEqual(result['status'],'finished'); self.assertEqual(result['summary']['failed_trajectories'],2)
            self.assertFalse(result['primary_scores_modified']); self.assertEqual(result['model_calls'],0)
            self.assertIn('required_contract_vs_extra_assumptions',result['trajectories'][0]['stories'][0])
            runner.frozen.unchanged(plan['inputs'])

    def test_missing_qualification_and_existing_output_fail_before_validator(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(runner,'BlackboxValidator') as validator:
                with self.assertRaisesRegex(ValueError,'qualification'): runner.run(Path(directory),Path(directory)/'out')
                with self.assertRaisesRegex(ValueError,'fresh golden'): runner.run(None,Path(directory),golden=True)
            validator.assert_not_called()


if __name__=='__main__': unittest.main()
