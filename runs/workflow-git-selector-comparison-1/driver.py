"""Alternating diagnostic of selector backends, with production source unchanged."""
import hashlib,json,selectors,signal,statistics,sys,time
from pathlib import Path
ROOT=Path('/Users/westbrook/.codex/worktrees/cumulative-reader-integration/gossip-protocol')
RUN=ROOT/'runs/workflow-combined-review-physical-1/20261005T041008-7045106e'
CASE=next(RUN.glob('classes/*/artifacts/*/fresh-reopen-inherited-text-persistence'))
OUT=ROOT/'runs/workflow-git-selector-comparison-1'
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
def save(name,value):
 with (OUT/name).open('x') as f:json.dump(value,f,indent=2,sort_keys=True);f.write('\n')
inputs=json.loads((RUN/'inputs.json').read_bytes())['inputs']
for name,h in inputs.items():assert sha(ROOT/name)==h,name
config=json.loads((CASE/'raw/config.json').read_bytes());repo=Path(config['repository']);commit=config['registration']['commit_oid'];tree=config['registration']['tree_oid']
originals={str(p):sha(p) for p in CASE.rglob('*') if p.is_file()}
OUT.mkdir(); original=selectors.DefaultSelector
spec={'protocol':'workflow-git-selector-comparison-v1','pairs':8,'order':['AB' if i%2==0 else 'BA' for i in range(8)],'A':original.__name__,'B':selectors.PollSelector.__name__,'purpose':'Locate transport readiness wait cost; same production two-child capture with diagnostic-only selector factory substitution.','source_inputs':inputs,'original_repository':str(repo),'commit_oid':commit,'tree_oid':tree,'watchdog_seconds':90,'driver_sha256':sha(Path(__file__)),'limits':['One warm fixed repository and host; component diagnostic, not whole-workflow or quality evidence.','No production edit, cross-call cache, changed deadline or skipped check.','Both arms include identical selector-event instrumentation.','No candidate/Engine/provider execution or original authority reopening.']}
save('plan.json',spec)
def alarm(_sig,_frame):raise TimeoutError('Declared90second diagnostic watchdog')
signal.signal(signal.SIGALRM,alarm);signal.setitimer(signal.ITIMER_REAL,90)
sys.path.insert(0,str(ROOT))
from gossip_harness import candidate_git_source_two_process_v1 as capture
rows=[];reference=None
class Traced:
 def __init__(self,backend,records):self.delegate=backend();self.records=records;self.events=[];records.append(self.events)
 def register(self,*a,**kw):return self.delegate.register(*a,**kw)
 def unregister(self,*a,**kw):return self.delegate.unregister(*a,**kw)
 def get_map(self):return self.delegate.get_map()
 def close(self):return self.delegate.close()
 def select(self,timeout=None):
  start=time.perf_counter_ns();result=self.delegate.select(timeout);elapsed=(time.perf_counter_ns()-start)/1e9
  self.events.append({'seconds':elapsed,'events':[[key.data,mask] for key,mask in result]});return result
try:
 for i,order in enumerate(spec['order']):
  for arm in order:
   records=[];backend=original if arm=='A' else selectors.PollSelector
   selectors.DefaultSelector=lambda:Traced(backend,records)
   save(f'{i:02d}-{arm}-intent.json',{'pair':i,'arm':arm,'plan_sha256':sha(OUT/'plan.json')})
   start=time.perf_counter_ns();value=capture.capture_git_source_two_process(repo,commit);elapsed=(time.perf_counter_ns()-start)/1e9
   assert value[0]==tree
   if reference is None:reference=value
   assert value==reference
   row={'pair':i,'arm':arm,'seconds':elapsed,'status':'complete','select_events':records,'files':len(value[1]),'bytes':sum(map(len,value[1].values()))};rows.append(row);save(f'{i:02d}-{arm}-result.json',row)
finally:selectors.DefaultSelector=original;signal.setitimer(signal.ITIMER_REAL,0)
for name,h in inputs.items():assert sha(ROOT/name)==h,name
for name,h in originals.items():assert sha(Path(name))==h,name
assert {str(p) for p in CASE.rglob('*') if p.is_file()}==set(originals)
pairs=[{arm:next(r['seconds'] for r in rows if r['pair']==i and r['arm']==arm) for arm in 'AB'} for i in range(8)]
result={'protocol':spec['protocol'],'status':'complete','A_median_seconds':statistics.median(r['A'] for r in pairs),'B_median_seconds':statistics.median(r['B'] for r in pairs),'median_paired_ratio':statistics.median(r['B']/r['A'] for r in pairs),'B_faster_pairs':sum(r['B']<r['A'] for r in pairs),'source_inputs_unchanged':True,'original_files_unchanged':originals,'new_model_quality_samples':0,'rows':rows}
save('result.json',result)
print(json.dumps({k:v for k,v in result.items() if k not in ['rows','original_files_unchanged']},indent=2))
for arm in 'AB':
 row=next(r for r in rows if r['arm']==arm)
 print(arm,json.dumps({'slowest_selects':sorted([dict(child=i,**event) for i,events in enumerate(row['select_events']) for event in events],key=lambda v:v['seconds'],reverse=True)[:6]}))
