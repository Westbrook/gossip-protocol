"""Diagnostic Git trace to separate child startup latency from Git command work."""
import hashlib,json,signal,subprocess,sys,time
from pathlib import Path
ROOT=Path('/Users/westbrook/.codex/worktrees/cumulative-reader-integration/gossip-protocol')
RUN=ROOT/'runs/workflow-combined-review-physical-1/20261005T041008-7045106e'
CASE=next(RUN.glob('classes/*/artifacts/*/fresh-reopen-inherited-text-persistence'))
OUT=ROOT/'runs/workflow-git-startup-trace-1'
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
def save(name,value):
 with (OUT/name).open('x') as f:json.dump(value,f,indent=2,sort_keys=True);f.write('\n')
inputs=json.loads((RUN/'inputs.json').read_bytes())['inputs']
for name,h in inputs.items():assert sha(ROOT/name)==h,name
config=json.loads((CASE/'raw/config.json').read_bytes());repo=Path(config['repository']);commit=config['registration']['commit_oid']
originals={str(p):sha(p) for p in CASE.rglob('*') if p.is_file()};OUT.mkdir()
save('plan.json',{'protocol':'workflow-git-startup-trace-v1','purpose':'Diagnostic-only trace of two current Git children; no candidate execution or authority reopening.','source_inputs':inputs,'repository':str(repo),'commit_oid':commit,'driver_sha256':sha(Path(__file__)),'watchdog_seconds':30,'calls':1,'trace_limit_bytes_per_child':262144,'limits':['Adds Git trace to new diagnostic output only; output is not acceptance evidence.','One fixed warm fixture and host; no causal speedup claim or changed execution contract.']})
def alarm(_sig,_frame):raise TimeoutError('Diagnostic30second watchdog')
signal.signal(signal.SIGALRM,alarm);signal.setitimer(signal.ITIMER_REAL,30)
sys.path.insert(0,str(ROOT))
from gossip_harness import candidate_git_source_two_process_v1 as capture
original=subprocess.Popen;children=[]
def traced(*a,**kw):
 index=len(children);trace=OUT/f'child-{index:02d}.jsonl';kw['env']=dict(kw['env'],GIT_TRACE2_EVENT=str(trace))
 row={'index':index,'parent_before_epoch':time.time(),'parent_before_mono':time.monotonic(),'trace':str(trace)};children.append(row)
 process=original(*a,**kw);row['parent_popen_return_epoch']=time.time();row['parent_popen_return_mono']=time.monotonic();return process
try:
 subprocess.Popen=traced;start=time.perf_counter_ns();tree,files=capture.capture_git_source_two_process(repo,commit);elapsed=(time.perf_counter_ns()-start)/1e9
finally:subprocess.Popen=original;signal.setitimer(signal.ITIMER_REAL,0)
assert tree==config['registration']['tree_oid'] and len(children)==2
from datetime import datetime
for child in children:
 path=Path(child['trace']);assert path.stat().st_size<=262144
 events=[json.loads(line) for line in path.read_bytes().splitlines()];first=events[0];last=events[-1]
 child['trace_sha256']=sha(path);child['events']=len(events);child['first_event']=first;child['last_event']=last
 child['popen_seconds']=child['parent_popen_return_mono']-child['parent_before_mono'];child['time_until_first_git_event']=datetime.fromisoformat(first['time'].replace('Z','+00:00')).timestamp()-child['parent_before_epoch']
for name,h in inputs.items():assert sha(ROOT/name)==h,name
for name,h in originals.items():assert sha(Path(name))==h,name
assert {str(p) for p in CASE.rglob('*') if p.is_file()}==set(originals)
result={'protocol':'workflow-git-startup-trace-v1','status':'complete','capture_seconds':elapsed,'files':len(files),'bytes':sum(map(len,files.values())),'children':children,'source_inputs_unchanged':True,'original_files_unchanged':originals,'new_model_quality_samples':0}
save('result.json',result);print(json.dumps({k:v for k,v in result.items() if k!='original_files_unchanged'},indent=2))
