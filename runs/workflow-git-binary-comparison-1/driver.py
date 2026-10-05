"""Prospective local runtime diagnostic; not production runtime adoption."""
import hashlib,json,signal,statistics,subprocess,sys,time
from pathlib import Path
ROOT=Path('/Users/westbrook/.codex/worktrees/cumulative-reader-integration/gossip-protocol')
RUN=ROOT/'runs/workflow-combined-review-physical-1/20261005T041008-7045106e'
CASE=next(RUN.glob('classes/*/artifacts/*/fresh-reopen-inherited-text-persistence'))
OUT=ROOT/'runs/workflow-git-binary-comparison-1'
BINARIES={'A':Path('/opt/homebrew/Cellar/git/2.54.0/bin/git'),'B':Path('/Library/Developer/CommandLineTools/usr/bin/git')}
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
def save(name,value):
 with (OUT/name).open('x') as f:json.dump(value,f,indent=2,sort_keys=True);f.write('\n')
inputs=json.loads((RUN/'inputs.json').read_bytes())['inputs']
for name,h in inputs.items():assert sha(ROOT/name)==h,name
config=json.loads((CASE/'raw/config.json').read_bytes());repo=Path(config['repository']);commit=config['registration']['commit_oid'];tree=config['registration']['tree_oid']
originals={str(p):sha(p) for p in CASE.rglob('*') if p.is_file()};OUT.mkdir()
binaries={arm:{'path':str(path),'sha256':sha(path),'version':subprocess.check_output([str(path),'--version'],text=True,timeout=5).strip()} for arm,path in BINARIES.items()}
spec={'protocol':'workflow-git-binary-comparison-v1','pairs':8,'order':['AB' if i%2==0 else 'BA' for i in range(8)],'binaries':binaries,'purpose':'Test whether measured pre-Git-startup latency depends on the installed executable; runtime selection remains unchanged.','source_inputs':inputs,'original_repository':str(repo),'commit_oid':commit,'tree_oid':tree,'watchdog_seconds':90,'driver_sha256':sha(Path(__file__)),'limits':['One warm fixed repository and host; component diagnostic, not whole-workflow or quality evidence.','Exact same current production helper, command arguments, environment guards, deadlines and fresh independent reads. Only absolute Git executable changes.','No production runtime adoption, candidate/Engine/provider execution or original authority reopening.','Different Git builds require full current-runtime qualification before any adoption.']}
save('plan.json',spec)
def alarm(_sig,_frame):raise TimeoutError('Declared90second diagnostic watchdog')
signal.signal(signal.SIGALRM,alarm);signal.setitimer(signal.ITIMER_REAL,90)
sys.path.insert(0,str(ROOT))
from gossip_harness import candidate_git_source_two_process_v1 as capture
original=subprocess.Popen;rows=[];reference=None
try:
 for i,order in enumerate(spec['order']):
  for arm in order:
   children=[]
   def selected(argv,**kw):
    assert argv[0]=='git';argv=[str(BINARIES[arm]),*argv[1:]];children.append(argv);return original(argv,**kw)
   subprocess.Popen=selected
   save(f'{i:02d}-{arm}-intent.json',{'pair':i,'arm':arm,'plan_sha256':sha(OUT/'plan.json')})
   start=time.perf_counter_ns();value=capture.capture_git_source_two_process(repo,commit);elapsed=(time.perf_counter_ns()-start)/1e9
   assert value[0]==tree and len(children)==2
   if reference is None:reference=value
   assert value==reference
   row={'pair':i,'arm':arm,'seconds':elapsed,'status':'complete','children':children,'tree_oid':value[0],'manifest':{name:sha256 for name,sha256 in sorted((name,hashlib.sha256(raw).hexdigest()) for name,raw in value[1].items())},'files':len(value[1]),'bytes':sum(map(len,value[1].values()))};rows.append(row);save(f'{i:02d}-{arm}-result.json',row)
finally:subprocess.Popen=original;signal.setitimer(signal.ITIMER_REAL,0)
for name,h in inputs.items():assert sha(ROOT/name)==h,name
for name,h in originals.items():assert sha(Path(name))==h,name
for arm,path in BINARIES.items():assert sha(path)==binaries[arm]['sha256']
assert {str(p) for p in CASE.rglob('*') if p.is_file()}==set(originals)
pairs=[{arm:next(r['seconds'] for r in rows if r['pair']==i and r['arm']==arm) for arm in 'AB'} for i in range(8)]
result={'protocol':spec['protocol'],'status':'complete','A_median_seconds':statistics.median(r['A'] for r in pairs),'B_median_seconds':statistics.median(r['B'] for r in pairs),'median_paired_ratio':statistics.median(r['B']/r['A'] for r in pairs),'B_faster_pairs':sum(r['B']<r['A'] for r in pairs),'source_inputs_unchanged':True,'original_files_unchanged':originals,'binaries_unchanged':True,'new_model_quality_samples':0,'rows':rows}
save('result.json',result);print(json.dumps({k:v for k,v in result.items() if k not in ['rows','original_files_unchanged']},indent=2))
