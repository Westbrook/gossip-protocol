"""Fixed 8-pair read/hash-phase diagnostic; never reopens a journal owner."""
import hashlib,json,os,signal,statistics,sys,time
from contextlib import nullcontext
from pathlib import Path
ROOT=Path('/Users/westbrook/.codex/worktrees/cumulative-reader-integration/gossip-protocol')
OUT=Path('/private/tmp/workflow-checkpoint-reader-comparison-1');OUT.mkdir()
A=ROOT/'runs/workflow-record-physical-failure-audit-1/audit.json'
audit=json.loads(A.read_bytes());assert hashlib.sha256(A.read_bytes()).hexdigest()=='41d3495cff2adbc71f9fcc821797b133b21f7f7605192e936bbd65e4a2d9a364'
CASE=Path(audit['timings']['path']).parent;RAW=CASE/'raw';DELTA=CASE/'delta'
RUN=ROOT/'runs/workflow-record-construction-nearest-1/20261005T020621-ececa927'
inputs=json.loads((RUN/'inputs.json').read_bytes())['inputs']
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def encoded(v):return json.dumps(v,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def identity(p):
 s=p.stat(follow_symlinks=False);return (s.st_dev,s.st_ino,s.st_mode)
prefix=json.loads((CASE/'original-prefix.json').read_bytes());assert prefix==audit['original_prefix']
gen=(DELTA/'genesis.json').read_bytes();context=hashlib.sha256(b'candidate-checkpoint-chain-v1/genesis\0'+gen).hexdigest();head=context
records=[{'path':str(DELTA/'genesis.json'),'bytes':len(gen),'sha256':hashlib.sha256(gen).hexdigest(),'bound':65536}];raw_records=[];external=len(gen);total=0
for i in range(1,prefix['sequence']+1):
 path=DELTA/f'delta-{i:08d}.json';b=path.read_bytes();v=json.loads(b)
 assert encoded(v)==b and v['sequence']==i and v['previous_head_sha256']==head and v['context_sha256']==context and Path(v['name']).name==v['name']
 records.append({'path':str(path),'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest(),'bound':4096})
 raw_records.append({'path':str(RAW/v['name']),'bytes':v['bytes'],'sha256':v['sha256'],'bound':33554432})
 total+=v['bytes'];external+=len(b);head=hashlib.sha256(b'candidate-checkpoint-chain-v1/delta\0'+b).hexdigest()
assert prefix=={'context_sha256':context,'head_sha256':head,'sequence':254,'raw_file_count':254,'raw_bytes':total,'external_bytes':external}
records+=raw_records;assert len(records)==509
roots=(RAW,DELTA);identities=tuple(identity(p) for p in roots)
expected_names={str(RAW):set(Path(x['path']).name for x in raw_records)|{'owner.lock'},str(DELTA):{Path(x['path']).name for x in records if Path(x['path']).parent==DELTA}|{'owner.lock'}}
originals={str(p):sha(p) for p in (CASE/'original-prefix.json',CASE/'head/head.json',A)}
def verify_unchanged():
 assert all(sha(ROOT/k)==v for k,v in inputs.items())
 assert all(sha(Path(k))==v for k,v in originals.items())
 for r in records:
  p=Path(r['path']);assert not p.is_symlink() and p.stat().st_size==r['bytes'] and sha(p)==r['sha256']
 for p in roots:assert set(x.name for x in p.iterdir())==expected_names[str(p)]
 assert tuple(identity(p) for p in roots)==identities
verify_unchanged()
plan={'protocol':'workflow-checkpoint-reader-comparison-v1','purpose':'Compare full ordered file read/hash phase only, not complete owner checkpoint or product execution','pairs':8,'order':['AB' if i%2==0 else 'BA' for i in range(8)],'A':'candidate_http_journal_v3.read, fresh per-file ancestry descriptors','B':'candidate_journal_batch_read_v1.CheckpointReader, new reader per whole phase, all ancestry checks per file','timing':'Includes per-file read/bound/hash verification, root inventory/identity checks and B reader construction/validation/close; policy construction and source/fixture preflight outside timing. No cProfile/syscall audit.','deadline_seconds_whole_diagnostic':60,'source_inputs':inputs,'originals':originals,'records':records,'roots':[str(x) for x in roots],'root_identities':identities,'expected_prefix':prefix,'python':{'path':str(Path(sys.executable).resolve()),'sha256':sha(Path(sys.executable).resolve()),'version':sys.version},'limits':['One retained254-record journal on one host with warmed filesystem cache.','No owner reopen, external-head authority operation, journal append, Engine/provider/candidate execution.','This phase omits live owner checks and per-delta semantic decoding; measurements do not prove end-to-end checkpoint or workflow speed.','Descriptor reuse changes syscall/race observation timing; no claim of identical transient-race detection.','No generated-test or model-quality sample.']}
with (OUT/'plan.json').open('x') as f:json.dump(plan,f,indent=2,sort_keys=True);f.write('\n')
def alarm(_signum,_frame):raise TimeoutError('Declared60second diagnostic watchdog reached')
signal.signal(signal.SIGALRM,alarm);signal.setitimer(signal.ITIMER_REAL,60)
sys.path.insert(0,str(ROOT))
from gossip_harness import candidate_http_journal_v3 as stable
from gossip_harness import candidate_journal_batch_read_v1 as batch
policy=batch.BatchReadPolicy()
assert tuple(stable._directory_identity(p.stat(follow_symlinks=False)) for p in roots)==identities
rows=[]
try:
 for pair,order in enumerate(plan['order']):
  for arm in order:
   intent={'pair':pair,'arm':arm,'status':'intent','plan_sha256':sha(OUT/'plan.json')}
   with (OUT/f'{pair:02d}-{arm}-intent.json').open('x') as f:json.dump(intent,f,sort_keys=True)
   start=time.perf_counter_ns()
   with (batch.CheckpointReader(roots,identities,policy=policy) if arm=='B' else nullcontext()) as reader:
    read=reader.read if reader is not None else stable.read
    assert tuple(identity(p) for p in roots)==identities
    for p in roots:assert set(x.name for x in p.iterdir())==expected_names[str(p)]
    for r in records:
     raw=read(Path(r['path']),max_bytes=r['bound'])
     assert len(raw)==r['bytes'] and hashlib.sha256(raw).hexdigest()==r['sha256']
    for p in roots:assert set(x.name for x in p.iterdir())==expected_names[str(p)]
    assert tuple(identity(p) for p in roots)==identities
   elapsed=time.perf_counter_ns()-start
   row={'pair':pair,'arm':arm,'status':'complete','read_count':len(records),'verified_bytes':sum(r['bytes'] for r in records),'seconds':elapsed/1e9}
   with (OUT/f'{pair:02d}-{arm}-result.json').open('x') as f:json.dump(row,f,indent=2,sort_keys=True);f.write('\n')
   rows.append(row)
 verify_unchanged()
finally:signal.setitimer(signal.ITIMER_REAL,0)
pairs=[{arm:next(r['seconds'] for r in rows if r['pair']==i and r['arm']==arm) for arm in 'AB'} for i in range(8)]
summary={'protocol':plan['protocol'],'status':'complete','rows':rows,'A_median_seconds':statistics.median(x['A'] for x in pairs),'B_median_seconds':statistics.median(x['B'] for x in pairs),'median_paired_difference':statistics.median(x['B']-x['A'] for x in pairs),'median_paired_ratio':statistics.median(x['B']/x['A'] for x in pairs),'B_faster_pairs':sum(x['B']<x['A'] for x in pairs),'unchanged_originals_and_sources':True,'new_model_quality_samples':0}
with (OUT/'result.json').open('x') as f:json.dump(summary,f,indent=2,sort_keys=True);f.write('\n')
print(json.dumps({'output':str(OUT),**{k:v for k,v in summary.items() if k!='rows'}},indent=2))
