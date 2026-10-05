"""Identical read/hash corpus at two directory depths; no owner authority."""
import cProfile,hashlib,json,os,pstats,signal,statistics,sys,time
from pathlib import Path
ROOT=Path('/Users/westbrook/.codex/worktrees/cumulative-reader-integration/gossip-protocol')
OUT=ROOT/'runs/workflow-checkpoint-depth-comparison-1'
SHALLOW=Path('/private/tmp/gossip-checkpoint-depth-1')
RUN=ROOT/'runs/workflow-apple-git-physical-1/20261005T043305-99214415'
CASE=next(RUN.glob('classes/*/artifacts/*/after-commit-fault-WF19-provisional-fault-boundary'))
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
def read(p):return json.loads(p.read_bytes())
def save(name,value):
 with (OUT/name).open('x') as f:json.dump(value,f,indent=2,sort_keys=True);f.write('\n')
def identity(p):
 s=p.stat(follow_symlinks=False);return (s.st_dev,s.st_ino,s.st_mode)
inputs=read(RUN/'inputs.json')['inputs']
for name,h in inputs.items():assert sha(ROOT/name)==h,name
originals={str(p):sha(p) for p in CASE.rglob('*') if p.is_file()}
OUT.mkdir();SHALLOW.mkdir()
roots={'A':(CASE/'raw',CASE/'delta'),'B':(SHALLOW/'raw',SHALLOW/'delta')}
records=[]
for root in roots['A']:
 for p in sorted(root.iterdir()):
  assert p.is_file() and not p.is_symlink()
  if p.name!='owner.lock':records.append({'root':root.name,'name':p.name,'bytes':p.stat().st_size,'sha256':sha(p),'bound':65536 if p.name=='genesis.json' else (4096 if root.name=='delta' else 33554432)})
for src,dst in zip(roots['A'],roots['B']):
 dst.mkdir(mode=src.stat().st_mode & 0o777)
 for p in src.iterdir():
  q=dst/p.name
  with q.open('xb') as f:f.write(p.read_bytes())
  q.chmod(p.stat().st_mode & 0o777)
identities={arm:tuple(identity(p) for p in dirs) for arm,dirs in roots.items()}
inventories={arm:{str(p):sorted(x.name for x in p.iterdir()) for p in dirs} for arm,dirs in roots.items()}
spec={'protocol':'workflow-checkpoint-depth-comparison-v1','purpose':'Measure full fresh reader/hash phase sensitivity to actual directory depth; not a complete journal validation or product execution.','pairs':8,'order':['AB' if i%2==0 else 'BA' for i in range(8)],'A':'Original retained deep raw/delta roots, read only','B':'Byte-identical data copies at fresh shallow roots, no owner or head created','roots':{a:[str(p) for p in ps] for a,ps in roots.items()},'root_depths':{a:[len(p.parts)-1 for p in ps] for a,ps in roots.items()},'root_identities':identities,'records':records,'source_inputs':inputs,'watchdog_seconds':60,'profiles':'One additional cProfile call for each arm, excluded from paired timing','driver_sha256':sha(Path(__file__)),'limits':['Copied genesis/delta values retain original context paths and are data, NOT a valid relocated journal or authenticated observation.','No original owner reopened, head modified, candidate/Engine/provider executed, or deadline changed.','Same production reader with all actual ancestors checked before and after every file. No aliases, symlinks, caching or skipped guards.','One fixed warm corpus/host; complete owner/head checks and delta semantic decoding excluded; no end-to-end or model-quality claim.','Shallow fixture retained without deletion; no production execution layout chosen.']}
save('plan.json',spec)
def alarm(_s,_f):raise TimeoutError('Declared60second diagnostic watchdog')
signal.signal(signal.SIGALRM,alarm);signal.setitimer(signal.ITIMER_REAL,60)
sys.path.insert(0,str(ROOT))
from gossip_harness import candidate_journal_batch_read_v1 as batch
policy=batch.BatchReadPolicy()
def phase(arm):
 dirs=roots[arm];ids=identities[arm]
 with batch.CheckpointReader(dirs,ids,policy=policy) as reader:
  assert tuple(identity(p) for p in dirs)==ids
  for p in dirs:assert sorted(x.name for x in p.iterdir())==inventories[arm][str(p)]
  for r in records:
   path=dirs[0 if r['root']=='raw' else 1]/r['name'];raw=reader.read(path,max_bytes=r['bound'])
   assert len(raw)==r['bytes'] and hashlib.sha256(raw).hexdigest()==r['sha256']
  for p in dirs:assert sorted(x.name for x in p.iterdir())==inventories[arm][str(p)]
  assert tuple(identity(p) for p in dirs)==ids
rows=[];profiles={}
try:
 for i,order in enumerate(spec['order']):
  for arm in order:
   save(f'{i:02d}-{arm}-intent.json',{'pair':i,'arm':arm,'plan_sha256':sha(OUT/'plan.json')})
   start=time.perf_counter_ns();phase(arm);elapsed=(time.perf_counter_ns()-start)/1e9
   row={'pair':i,'arm':arm,'seconds':elapsed,'status':'complete','read_count':len(records),'verified_bytes':sum(r['bytes'] for r in records)};rows.append(row);save(f'{i:02d}-{arm}-result.json',row)
 for arm in 'AB':
  profile=cProfile.Profile();profile.enable();phase(arm);profile.disable();profile.dump_stats(str(OUT/f'profile-{arm}.pstats'))
  entries=[{'file':f,'line':line,'function':fn,'calls':v[1],'internal_seconds':v[2],'cumulative_seconds':v[3]} for (f,line,fn),v in pstats.Stats(profile).stats.items()]
  profiles[arm]=sorted(entries,key=lambda x:x['cumulative_seconds'],reverse=True)
 for name,h in inputs.items():assert sha(ROOT/name)==h,name
 for name,h in originals.items():assert sha(Path(name))==h,name
 assert {str(p) for p in CASE.rglob('*') if p.is_file()}==set(originals)
 for arm in 'AB':
  assert tuple(identity(p) for p in roots[arm])==identities[arm]
  for r in records:assert sha(roots[arm][0 if r['root']=='raw' else 1]/r['name'])==r['sha256']
finally:signal.setitimer(signal.ITIMER_REAL,0)
pairs=[{arm:next(r['seconds'] for r in rows if r['pair']==i and r['arm']==arm) for arm in 'AB'} for i in range(8)]
result={'protocol':spec['protocol'],'status':'complete','rows':rows,'profiles':profiles,'A_median_seconds':statistics.median(r['A'] for r in pairs),'B_median_seconds':statistics.median(r['B'] for r in pairs),'median_paired_ratio':statistics.median(r['B']/r['A'] for r in pairs),'B_faster_pairs':sum(r['B']<r['A'] for r in pairs),'original_files_unchanged':originals,'source_inputs_unchanged':True,'new_model_quality_samples':0}
save('result.json',result)
print(json.dumps({k:v for k,v in result.items() if k not in ['rows','profiles','original_files_unchanged']},indent=2))
print(json.dumps({'depths':spec['root_depths'],'read_count':len(records),'bytes':rows[0]['verified_bytes'],'profiles':{arm:values[:7] for arm,values in profiles.items()}},indent=2))
