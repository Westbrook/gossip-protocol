"""Read-only component diagnostic; never reopens original execution authority."""
import cProfile,hashlib,json,pstats,signal,statistics,sys,time
from pathlib import Path
ROOT=Path('/Users/westbrook/.codex/worktrees/cumulative-reader-integration/gossip-protocol')
RUN=ROOT/'runs/workflow-combined-review-physical-1/20261005T041008-7045106e'
OUT=ROOT/'runs/workflow-git-capture-profile-1'
CASE=next(RUN.glob('classes/*/artifacts/*/fresh-reopen-inherited-text-persistence'))
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
def save(name,value):
 with (OUT/name).open('x') as f:json.dump(value,f,indent=2,sort_keys=True);f.write('\n')
def alarm(_sig,_frame):raise TimeoutError('Declared30second diagnostic watchdog')
inputs=json.loads((RUN/'inputs.json').read_bytes())['inputs']
for name,h in inputs.items():assert sha(ROOT/name)==h,name
config=json.loads((CASE/'raw/config.json').read_bytes())
repo=Path(config['repository']);commit=config['registration']['commit_oid'];tree=config['registration']['tree_oid']
assert repo==CASE/'candidate.git'
originals={str(p):sha(p) for p in CASE.rglob('*') if p.is_file()}
OUT.mkdir()
spec={'protocol':'workflow-git-capture-profile-v1','purpose':'Locate actual time cost inside fresh source capture; no candidate execution, Engine, provider, authority reopening or acceptance credit.','source_inputs':inputs,'original_repository':str(repo),'commit_oid':commit,'tree_oid':tree,'unprofiled_calls':4,'profiled_calls':3,'watchdog_seconds':30,'driver_sha256':sha(Path(__file__)),'limitations':['One fixed repository and host with warm filesystem; diagnostic only.','Profiler changes timing; profiled totals are not performance comparisons.','No source or deadline change; each capture independently uses current production two-process helper.']}
save('plan.json',spec)
signal.signal(signal.SIGALRM,alarm);signal.setitimer(signal.ITIMER_REAL,30)
sys.path.insert(0,str(ROOT))
from gossip_harness import candidate_git_source_two_process_v1 as capture
rows=[];reference=None
for i in range(4):
 save(f'call-{i:02d}-intent.json',{'index':i,'plan_sha256':sha(OUT/'plan.json')})
 start=time.perf_counter_ns();value=capture.capture_git_source_two_process(repo,commit);elapsed=(time.perf_counter_ns()-start)/1e9
 assert value[0]==tree
 if reference is None:reference=value
 assert value==reference
 row={'index':i,'seconds':elapsed,'files':len(value[1]),'bytes':sum(map(len,value[1].values())),'status':'complete'};rows.append(row);save(f'call-{i:02d}-result.json',row)
p=cProfile.Profile()
for i in range(3):
 p.enable();value=capture.capture_git_source_two_process(repo,commit);p.disable();assert value==reference
p.dump_stats(str(OUT/'profile.pstats'))
records=[{'file':f,'line':line,'function':name,'primitive_calls':v[0],'calls':v[1],'internal_seconds':v[2],'cumulative_seconds':v[3]} for (f,line,name),v in pstats.Stats(p).stats.items()]
for name,h in inputs.items():assert sha(ROOT/name)==h,name
for name,h in originals.items():assert sha(Path(name))==h,name
assert {str(p) for p in CASE.rglob('*') if p.is_file()}==set(originals)
signal.setitimer(signal.ITIMER_REAL,0)
result={'protocol':spec['protocol'],'status':'complete','unprofiled_calls':rows,'median_seconds':statistics.median(r['seconds'] for r in rows),'profiled_calls':3,'profile_rows':sorted(records,key=lambda x:x['cumulative_seconds'],reverse=True),'original_files_unchanged':originals,'source_inputs_unchanged':True,'new_model_quality_samples':0}
save('result.json',result)
print(json.dumps({'median_seconds':result['median_seconds'],'files':rows[0]['files'],'bytes':rows[0]['bytes'],'top_profile':result['profile_rows'][:18]},indent=2))
