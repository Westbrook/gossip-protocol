import hashlib,json,statistics,shutil
from pathlib import Path
root=Path('/Users/westbrook/.codex/worktrees/cumulative-reader-integration/gossip-protocol')
src=Path('/private/tmp/workflow-checkpoint-reader-comparison-1')
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
p=json.loads((src/'plan.json').read_bytes());r=json.loads((src/'result.json').read_bytes())
assert len(r['rows'])==16 and p['pairs']==8
for i,order in enumerate(p['order']):
 assert order==('AB' if i%2==0 else 'BA')
 for offset,arm in enumerate(order):
  row=r['rows'][i*2+offset]
  assert row==json.loads((src/f'{i:02d}-{arm}-result.json').read_bytes())
  assert json.loads((src/f'{i:02d}-{arm}-intent.json').read_bytes())=={'pair':i,'arm':arm,'status':'intent','plan_sha256':sha(src/'plan.json')}
  assert row['status']=='complete' and row['pair']==i and row['arm']==arm and row['read_count']==509 and row['verified_bytes']==843121
for name,value in p['source_inputs'].items():assert sha(root/name)==value,name
for name,value in p['originals'].items():assert sha(Path(name))==value,name
for rec in p['records']:
 path=Path(rec['path']);assert path.stat().st_size==rec['bytes'] and sha(path)==rec['sha256']
pairs=[{a:next(x['seconds'] for x in r['rows'] if x['pair']==i and x['arm']==a) for a in 'AB'} for i in range(8)]
assert r['A_median_seconds']==statistics.median(x['A'] for x in pairs)
assert r['B_median_seconds']==statistics.median(x['B'] for x in pairs)
assert r['median_paired_ratio']==statistics.median(x['B']/x['A'] for x in pairs)
assert r['B_faster_pairs']==sum(x['B']<x['A'] for x in pairs)==8
out=root/'runs/workflow-checkpoint-reader-comparison-1'
shutil.copytree(src,out)
shutil.copy2('/private/tmp/compare-workflow-checkpoint-readers-1.py',out/'driver.py')
shutil.copy2(__file__,out/'audit.py')
audit={'protocol':'workflow-checkpoint-reader-comparison-audit-v1','status':'retained_and_reconciled','pairs':8,'executed_phases':16,'source_inputs_verified':len(p['source_inputs']),'file_records_per_phase':509,'bytes_per_phase':843121,'A_median_seconds':r['A_median_seconds'],'B_median_seconds':r['B_median_seconds'],'median_paired_ratio':r['median_paired_ratio'],'faster_pairs':8,'limits':p['limits'],'new_model_quality_samples':0,'evidence':{str(x.relative_to(root)):sha(x) for x in sorted(out.iterdir())}}
with (root/'analysis/workflow-checkpoint-reader-comparison-v1.json').open('x') as f:json.dump(audit,f,indent=2,sort_keys=True);f.write('\n')
print(json.dumps({k:v for k,v in audit.items() if k not in ('evidence','limits')},indent=2))
