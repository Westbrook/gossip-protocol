"""Bounded synthetic linkage diagnostic; no candidate/provider/Engine authority."""
import cProfile,hashlib,json,pstats,signal,statistics,sys,time
from pathlib import Path
ROOT=Path('/Users/westbrook/.codex/worktrees/cumulative-reader-integration/gossip-protocol')
OUT=ROOT/'runs/workflow-review-read-comparison-1'
OUT.mkdir()
RUN=ROOT/'runs/workflow-batch-read-physical-1/20261005T032028-96a941f3'
PLAN=next(RUN.glob('classes/*/artifacts/*/fresh-reopen-inherited-text-persistence/source-plan.json'))
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
def write(path,value):
 with path.open('x') as f:json.dump(value,f,indent=2,sort_keys=True);f.write('\n')
inputs=json.loads((RUN/'inputs.json').read_bytes())['inputs'];raw_plan=json.loads(PLAN.read_bytes());original_hash=sha(PLAN)
for name,h in inputs.items():assert sha(ROOT/name)==h,name
spec={'protocol':'workflow-review-read-comparison-v1','pairs':8,'order':['AB' if i%2==0 else 'BA' for i in range(8)],'A':'WorkflowReviewAuthority.authenticate(plan), then provenance(plan)','B':'One fresh _original(plan), report digest and returned provenance; unadopted local prototype only','whole_diagnostic_seconds':90,'profiles':'One extra cProfile call per arm after paired measurements; separate from timing comparison','source_inputs':inputs,'retained_plan':{'path':str(PLAN),'sha256':original_hash},'driver_sha256':sha(Path(__file__)),'fixture':'New actual protected journal and external head with explicitly synthetic review/delivery. Retained failed-case source plan is data only; original owner/head untouched.','limits':['One plan, warm filesystem, one host; component mechanism only, not whole-workflow speed or model-quality sample.','B changes two complete authentications to one fresh before/after authenticated read within a single caller operation; no cross-call cache.','No semantic review, independent acceptance, candidate/Git/Engine/provider execution or changed deadline.']}
write(OUT/'plan.json',spec)
def alarm(_sig,_frame):raise TimeoutError('Declared90second diagnostic watchdog reached')
signal.signal(signal.SIGALRM,alarm);signal.setitimer(signal.ITIMER_REAL,90)
sys.path.insert(0,str(ROOT))
from gossip_harness import candidate_workflow_review_v1 as review
from tests.test_candidate_workflow_review_v1 import synthetic_delivery
p=dict(raw_plan);p['storage_paths']=tuple(p['storage_paths']);p['boundaries']=tuple(review.WorkflowBoundary(**x) for x in p['boundaries']);p['capture_points']=tuple(review.WorkflowCapturePoint(**x) for x in p['capture_points']);source_plan=review.WorkflowSourcePlan(**p)
assert json.loads(review.encoded(source_plan.record()))==raw_plan
request=source_plan.request();authority,journal,head=synthetic_delivery(OUT/'synthetic-protected-review',request)
original_files={str(p):sha(p) for directory in (journal.raw_root,journal.delta_root,head.root) for p in directory.iterdir() if p.is_file()}
expected=journal.commitment
def legacy():
 return {'report_sha256':authority.authenticate(source_plan),'provenance':authority.provenance(source_plan)}
def combined():
 original=authority._original(source_plan)
 return {'report_sha256':review.digest(original['report']),'provenance':original['provenance']}
functions={'A':legacy,'B':combined};rows=[];reference=None;profiles={}
try:
 for pair,order in enumerate(spec['order']):
  for arm in order:
   write(OUT/f'{pair:02d}-{arm}-intent.json',{'pair':pair,'arm':arm,'plan_sha256':sha(OUT/'plan.json')})
   started=time.perf_counter_ns();result=functions[arm]();elapsed=(time.perf_counter_ns()-started)/1e9
   if reference is None:reference=result
   assert result==reference and result['report_sha256']==authority.enrollment.report_sha256
   row={'pair':pair,'arm':arm,'seconds':elapsed,'status':'complete','result_sha256':review.digest(result)};rows.append(row);write(OUT/f'{pair:02d}-{arm}-result.json',row)
 for arm in 'AB':
  profiler=cProfile.Profile();profiler.enable();actual=functions[arm]();profiler.disable();assert actual==reference
  profiler.dump_stats(str(OUT/f'profile-{arm}.pstats'))
  stats=pstats.Stats(profiler).stats
  values=[{'file':file,'line':line,'function':name,'primitive_calls':x[0],'calls':x[1],'internal_seconds':x[2],'cumulative_seconds':x[3]} for (file,line,name),x in stats.items() if file.startswith(str(ROOT))]
  profiles[arm]=sorted(values,key=lambda x:x['cumulative_seconds'],reverse=True)[:25]
 assert journal.validate_boundary(expected=expected).commitment==expected
 assert all(sha(Path(name))==h for name,h in original_files.items())
 assert sha(PLAN)==original_hash
 for name,h in inputs.items():assert sha(ROOT/name)==h,name
finally:
 journal.close();head.close();signal.setitimer(signal.ITIMER_REAL,0)
values=[{a:next(x['seconds'] for x in rows if x['pair']==i and x['arm']==a) for a in 'AB'} for i in range(8)]
summary={'protocol':spec['protocol'],'status':'complete','rows':rows,'A_median_seconds':statistics.median(x['A'] for x in values),'B_median_seconds':statistics.median(x['B'] for x in values),'median_paired_ratio':statistics.median(x['B']/x['A'] for x in values),'B_faster_pairs':sum(x['B']<x['A'] for x in values),'result':reference,'profiles':profiles,'source_inputs_unchanged':True,'original_plan_unchanged':True,'fixture_files_unchanged':original_files,'new_model_quality_samples':0}
write(OUT/'result.json',summary)
print(json.dumps({k:v for k,v in summary.items() if k not in ('rows','result','profiles','fixture_files_unchanged')},indent=2))
print(json.dumps({'top_profile_rows':{arm:rows[:8] for arm,rows in profiles.items()}},indent=2))
