"""Stdlib receipt/source audit; execute only following terminal process notice."""
import ast,hashlib,json,sys
from pathlib import Path
ROOT=Path('/Users/westbrook/.codex/worktrees/cumulative-reader-integration/gossip-protocol')
assert len(sys.argv)==3 and sys.argv[1]=='--terminal-notice' and Path(sys.argv[2]).name==sys.argv[2]
RUN=ROOT/'runs/workflow-combined-review-nearest-1'/sys.argv[2]
read=lambda p:json.loads(p.read_bytes())
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
digest=lambda v:hashlib.sha256(json.dumps(v,sort_keys=True).encode()).hexdigest()
s=read(RUN/'summary.json');inputs=read(RUN/'inputs.json');manifest=read(ROOT/'verification-manifest.json')
assert s['status']=='passed' and s['outcomes']=={'passed':103} and s['selected_total']==103
assert s['reused_classes']==0 and s['workers_started']==7
assert not s['stale_inputs'] and not s['stale_runtime']
assert s['fingerprint']==digest(inputs) and read(RUN/'runtime-final.json')==inputs['runtime']
assert s['runtime_reconciliation']=={'initial_sha256':digest(inputs['runtime']),'final_sha256':digest(inputs['runtime']),'matches':True}
exclude={'.git','.venv','venv','__pycache__','runs','results','node_modules'};paths=set()
for glob in manifest['input_globs']:
 paths.update(p for p in ROOT.glob(glob) if p.is_file() and not exclude.intersection(p.relative_to(ROOT).parts))
for fixture in manifest.get('retained_fixtures',[]):
 p=ROOT/fixture;paths.update([p] if p.is_file() else (q for q in p.rglob('*') if q.is_file() and '.git' not in q.parts))
current={p.relative_to(ROOT).as_posix():sha(p) for p in sorted(paths)}
assert current==inputs['inputs'] and len(current)==732
for name,tool in inputs['runtime']['tools'].items():assert sha(Path(tool['path']))==tool['sha256'],name
assert sha(Path(inputs['runtime']['root_runtime']))==inputs['runtime']['executable_sha256']
static=read(RUN/'static.json');assert static['status']=='passed' and all(x['status']=='passed' and not x.get('blocking_diagnostics') for x in static['checks'])
modules=['tests/test_candidate_workflow_review_v1.py','tests/test_cumulative_workflow_recipe_v1.py','tests/test_candidate_workflow_observation_v1.py','tests/test_candidate_workflow_execution_v1.py']
expected={}
for rel in modules:
 for cls in ast.parse((ROOT/rel).read_bytes()).body:
  if not isinstance(cls,ast.ClassDef):continue
  methods=sorted(x.name for x in cls.body if isinstance(x,ast.FunctionDef) and x.name.startswith('test'))
  if methods:
   key=rel+'::'+cls.name;cfg=manifest['classes'][key]
   assert cfg['lane'] in ('fast','git')
   expected[key]=[key+'.'+m for m in methods]
assert len(expected)==7 and sum(map(len,expected.values()))==103
artifacts={}
for job in s['jobs']:
 assert [x['id'] for x in job['tests']]==expected.pop(job['class'])
 assert job['status']=='passed' and job['returncode']==0 and job['physical'] is True and 'reused_from' not in job
 path=Path(job['evidence_path']);log=Path(job['log']);actual=read(path)
 assert path.is_relative_to(RUN) and log==path.with_name('worker.log')
 assert sha(path)==job['evidence_sha256'] and sha(log)==job['log_sha256']
 assert actual['tests']==job['tests'] and actual['status']=='passed' and actual['physical'] is True
 assert actual['cleanup_status']==job['cleanup_status']=='completed' and not actual.get('fixture_errors')
 assert all(x['status']=='passed' for x in actual['tests'])
 request=read(path.with_name('request.json'));assert request['root']==str(ROOT) and request['result']==str(path)
 assert [x['id'] for x in request['job']['tests']]==[x['id'] for x in job['tests']]
 artifacts[str(path.relative_to(ROOT))]=sha(path);artifacts[str(log.relative_to(ROOT))]=sha(log)
assert not expected
report={'protocol':'workflow-combined-review-nearest-audit-v1','status':'passed','session':str(RUN.relative_to(ROOT)),'selected_tests':103,'fresh_tests':103,'reused_tests':0,'classes':7,'source_inputs':len(current),'fingerprint':s['fingerprint'],'summary_sha256':sha(RUN/'summary.json'),'inputs_sha256':sha(RUN/'inputs.json'),'runtime_sha256':digest(inputs['runtime']),'duration_seconds':s['duration_seconds'],'static_status':'passed','artifacts':artifacts,'new_model_quality_samples':0,'limitations':['Local source, ordered-suite, artifact and runtime reconciliation; not scientific acceptance.','Full680check gate and new changed-source physical qualification remain required.','One failed run retained: existing deadline-test mock had not been changed to the new combined API. Production source was unchanged after that test correction.']}
out=ROOT/'analysis/workflow-combined-review-nearest-audit-v1.json'
with out.open('x') as f:json.dump(report,f,indent=2,sort_keys=True);f.write('\n')
print(json.dumps({k:v for k,v in report.items() if k not in ('artifacts','limitations')},indent=2))
