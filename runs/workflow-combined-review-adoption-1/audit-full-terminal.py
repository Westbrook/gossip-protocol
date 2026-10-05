"""Read-only stdlib reconciliation, to run only after terminal notice."""
import ast,collections,hashlib,json,sys
from pathlib import Path
ROOT=Path('/Users/westbrook/.codex/worktrees/cumulative-reader-integration/gossip-protocol')
RUN=ROOT/'runs/workflow-combined-review-nearest-1/20261005T034508-e7c098ac'
NEAR=ROOT/'runs/workflow-combined-review-nearest-1/20261005T034014-26c4e4be'
SELECTION=ROOT/'runs/workflow-combined-review-adoption-1/full-selection.json'
OUT=Path('/private/tmp/workflow-combined-review-full-terminal-audit-1.json')
assert sys.argv[1:]==['--terminal-notice','20261005T034508-e7c098ac']
def read(p):return json.loads(p.read_bytes())
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def digest(v):return hashlib.sha256(json.dumps(v,sort_keys=True).encode()).hexdigest()
def norm(s):return s.replace('::','.').replace('.py','').replace('/','.')
checks={}
def check(n,v):
 checks[n]=bool(v)
 if not v:raise AssertionError(n)
s=read(RUN/'summary.json');inp=read(RUN/'inputs.json');m=read(ROOT/'verification-manifest.json');sel=read(SELECTION)
check('terminal_pass',s['status']=='passed' and s['outcomes']=={'passed':680})
check('session',s['session']==str(RUN))
check('fingerprint',s['fingerprint']==digest(inp))
check('source_runtime_reconciliation',s['stale_inputs'] is False and s['stale_runtime'] is False and read(RUN/'runtime-final.json')==inp['runtime'] and s['runtime_reconciliation']=={'initial_sha256':digest(inp['runtime']),'final_sha256':digest(inp['runtime']),'matches':True})
check('nearest_identity',read(NEAR/'inputs.json')==inp)
exclude={'.git','.venv','venv','__pycache__','runs','results','node_modules'}; paths=set()
for glob in m['input_globs']:paths.update(p for p in ROOT.glob(glob) if p.is_file() and not exclude.intersection(p.relative_to(ROOT).parts))
for fixture in m.get('retained_fixtures',[]):
 p=ROOT/fixture;paths.update([p] if p.is_file() else (q for q in p.rglob('*') if q.is_file() and '.git' not in q.parts))
current={p.relative_to(ROOT).as_posix():sha(p) for p in sorted(paths)}
check('exact732_current_inputs',len(current)==732 and current==inp['inputs'])
for name,h in current.items():check('source:'+name,h==inp['inputs'][name])
check('runner',sha(ROOT/'devtools/verify.py')==inp['runner_sha256'])
for name,t in inp['runtime']['tools'].items():check('binary:'+name,sha(Path(t['path']))==t['sha256'])
check('interpreter',sha(Path(inp['runtime']['root_runtime']))==inp['runtime']['executable_sha256'])
static=read(RUN/'static.json');check('static',static['status']==s['static']['status']=='passed' and all(r['status']=='passed' and not r.get('blocking_diagnostics') for r in static['checks']))
all_tests=[]
for dirname in m['test_roots']:
 for source in sorted((ROOT/dirname).rglob('test*.py')):
  if exclude.intersection(source.relative_to(ROOT/dirname).parts):continue
  rel=source.relative_to(ROOT).as_posix()
  for cls in ast.parse(source.read_bytes()).body:
   if not isinstance(cls,ast.ClassDef):continue
   methods=sorted(n.name for n in cls.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)) and n.name.startswith('test'))
   if not methods:continue
   key=rel+'::'+cls.name;cfg=m['classes'][key]
   for method in methods:
    c={**cfg,**cfg.get('methods',{}).get(method,{})}
    all_tests.append({'id':key+'.'+method,'class':key,'path':rel,'name':cls.name,'method':method,'lane':c['lane'],'weight':c.get('weight',1),'exclusive':c.get('exclusive',False),'timeout_seconds':c.get('timeout_seconds',180)})
check('discovery',len(all_tests)==s['discovered_total'])
selected=[]
for selector in sel['selectors']:
 matched=[t for t in all_tests if t['lane'] in sel['lanes'] and (norm(t['id'])==norm(selector) or norm(t['id']).startswith(norm(selector)+'.'))]
 check('selector:'+selector,bool(matched));selected+=matched
selected=list({t['id']:t for t in selected}.values());check('680_selected',len(selected)==sel['expected_tests']==s['selected_total']==680)
check('lanes',dict(collections.Counter(t['lane'] for t in selected))==sel['expected_lanes']==s['selected_counts'])
groups={}
for t in selected:
 key=(t['class'],t['lane']);j=groups.setdefault(key,{**t,'tests':[],'weight':t['weight']});j['tests'].append(t);j['weight']=max(j['weight'],t['weight']);j['exclusive']=j['exclusive'] or t['exclusive']
lanes=('fast','fixtures','git','docker','report','browser');keys=sorted(groups,key=lambda x:(lanes.index(x[1]),x[0]))
check('65_ordered_classes',len(keys)==65 and keys==[(j['class'],j['lane']) for j in s['jobs']])
reuse=[];fresh=[];artifacts={}
for j in s['jobs']:
 key=(j['class'],j['lane']);name='@'.join(key);expected=groups[key];evidence=Path(j['evidence_path']);log=Path(j['log']);request=evidence.with_name('request.json');original=read(evidence)
 check('order:'+name,[t['id'] for t in j['tests']]==[t['id'] for t in expected['tests']])
 check('request:'+name,read(request)=={'root':str(ROOT),'job':expected,'result':str(evidence)})
 check('hash:'+name,sha(evidence)==j['evidence_sha256'] and sha(log)==j['log_sha256'])
 check('result:'+name,original['tests']==j['tests'] and original['status']==j['status']=='passed' and original['physical'] is True and j['returncode']==0 and original['cleanup_status']==j['cleanup_status']=='completed' and not original.get('fixture_errors') and all(t['status']=='passed' for t in original['tests']))
 origin=Path(j['reused_from']).parent if 'reused_from' in j else RUN
 shard=digest([t['id'] for t in expected['tests']])[:16]
 check('artifact_origin:'+name,evidence==origin/'classes'/shard/'result.json' and log==evidence.with_name('worker.log'))
 if 'reused_from' in j:
  old=read(origin/'summary.json');matches=[x for x in old['jobs'] if (x['class'],x['lane'])==key]
  check('reuse:'+name,origin==NEAR and j['physical'] is False and read(origin/'inputs.json')==inp and old['fingerprint']==s['fingerprint'] and old['status']=='passed' and not old['stale_inputs'] and not old['stale_runtime'] and len(matches)==1 and matches[0]['physical'] is True and matches[0]['tests']==j['tests'] and matches[0]['evidence_sha256']==sha(evidence) and matches[0]['log_sha256']==sha(log))
  reuse.append({'class':j['class'],'tests':len(j['tests']),'original':str(origin/'summary.json')})
 else:
  check('fresh:'+name,j['physical'] is True);fresh.append({'class':j['class'],'tests':len(j['tests'])})
 artifacts[str(evidence.relative_to(ROOT))]=sha(evidence);artifacts[str(log.relative_to(ROOT))]=sha(log)
check('execution_counts',len(reuse)==s['reused_classes']==7 and len(fresh)==s['workers_started']==58 and sum(r['tests'] for r in reuse)==103 and sum(r['tests'] for r in fresh)==577)
report={'protocol':'workflow-combined-review-full-terminal-audit-v1','status':'passed','session':str(RUN),'summary_sha256':sha(RUN/'summary.json'),'inputs_sha256':sha(RUN/'inputs.json'),'fingerprint':s['fingerprint'],'source_count':len(current),'selected_tests':680,'fresh_tests':577,'reused_tests':103,'classes':65,'duration_seconds':s['duration_seconds'],'static_status':'passed','runtime_sha256':digest(inp['runtime']),'selection':str(SELECTION),'selection_sha256':sha(SELECTION),'reused':reuse,'fresh':fresh,'artifact_sha256':artifacts,'checks':checks,'new_model_quality_samples':0,'limits':['Root read-only source/ordered-suite/receipt reconciliation, not independent scientific acceptance.','Physical workflow qualification remains outstanding for changed source.','Reused103 checks retain original executions and match current inputs/runtime/ordered class selection.']}
with OUT.open('x') as f:json.dump(report,f,indent=2,sort_keys=True);f.write('\n')
print(json.dumps({'output':str(OUT),'sha256':sha(OUT),'checks':len(checks),'fresh_tests':577,'reused_tests':103,'duration_seconds':s['duration_seconds']}))
