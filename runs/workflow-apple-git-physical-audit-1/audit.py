import collections, hashlib, json
from pathlib import Path
ROOT=Path('/Users/westbrook/.codex/worktrees/cumulative-reader-integration/gossip-protocol')
RUN=ROOT/'runs/workflow-apple-git-physical-1/20261005T043305-99214415'
OFFLINE=ROOT/'runs/workflow-apple-git-offline-1/20261005T042301-7dd7e9af'
OUT=Path('/private/tmp/workflow-apple-git-physical-audit-1.json')
def read(p): return json.loads(p.read_bytes())
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def enc(v): return json.dumps(v,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def digest(v): return hashlib.sha256(enc(v)).hexdigest()
def rdigest(v): return hashlib.sha256(json.dumps(v,sort_keys=True).encode()).hexdigest()
checks={}
check_prefix=""
def check(name,truth):
 name=check_prefix+name
 checks[name]=bool(truth)
 if not truth: raise AssertionError(name)
s=read(RUN/'summary.json'); inp=read(RUN/'inputs.json')
check('passed_5_failed_1_unrun_32',s['status']=='failed' and s['outcomes']=={'passed':5,'failed':1,'not_run':32} and s['selected_total']==38)
check('fresh_one_worker',s['workers_started']==1 and s['reused_classes']==0)
check('source_runtime_unchanged_since_offline',inp==read(OFFLINE/'inputs.json') and not s['stale_inputs'] and not s['stale_runtime'])
check('input_fingerprint',rdigest(inp)==s['fingerprint'])
check('runtime_reconciliation',read(RUN/'runtime-final.json')==inp['runtime'] and s['runtime_reconciliation']['matches'])
for name,h in inp['inputs'].items(): check('source:'+name,sha(ROOT/name)==h)
check('runner',sha(ROOT/'devtools/verify.py')==inp['runner_sha256'])
check('static_pass',s['static']['status']=='passed' and s['static']['returncode']==0)
j=s['jobs'][0]
for key,hkey in [('evidence_path','evidence_sha256'),('log','log_sha256')]: check(key,sha(Path(j[key]))==j[hkey])
check('worker_completed_cleanup',j['cleanup_status']=='completed' and not j['fixture_errors'] and j['returncode']==1)
check('ordered_results',read(Path(j['evidence_path']))['tests']==j['tests'])
def audit_case(case, complete):
 raw=case/'raw'; delta=case/'delta'
 gen=read(delta/'genesis.json'); genraw=(delta/'genesis.json').read_bytes()
 check('canonical_genesis',enc(gen)==genraw)
 context=hashlib.sha256(b'candidate-checkpoint-chain-v1/genesis\0'+genraw).hexdigest(); head=context; total=0; external=len(genraw); positions={}
 check('genesis_roots',gen['raw_root']==str(raw) and gen['delta_root']==str(delta))
 prefix=read(case/'original-prefix.json')
 for i in range(1,prefix['sequence']+1):
  p=delta/f'delta-{i:08d}.json'; d=read(p); b=p.read_bytes(); name=d['name']
  check(f'delta:{i}',enc(d)==b and set(d)=={'protocol','kind','context_sha256','sequence','previous_head_sha256','name','bytes','sha256'} and d['protocol']=='candidate-checkpoint-chain-v1' and d['kind']=='raw-file' and d['context_sha256']==context and d['sequence']==i and d['previous_head_sha256']==head and name not in positions and Path(name).name==name)
  target=raw/name; payload=target.read_bytes()
  check('raw:'+name,not target.is_symlink() and len(payload)==d['bytes'] and hashlib.sha256(payload).hexdigest()==d['sha256'])
  total+=len(payload); external+=len(b); positions[name]=i; head=hashlib.sha256(b'candidate-checkpoint-chain-v1/delta\0'+b).hexdigest()
 actual={'context_sha256':context,'head_sha256':head,'sequence':len(positions),'raw_file_count':len(positions),'raw_bytes':total,'external_bytes':external}
 check('prefix',actual==prefix)
 check('head',read(case/'head/head.json')=={'protocol':'candidate-checkpoint-head-v1','journal_roots':[str(raw),str(delta)],'commitment':prefix})
 check('raw_inventory',set(p.name for p in raw.iterdir())==set(positions)|{'owner.lock'})
 check('delta_inventory',set(p.name for p in delta.iterdir())=={'owner.lock','genesis.json'}|{f'delta-{i:08d}.json' for i in range(1,len(positions)+1)})
 config=read(raw/'config.json'); intent=read(raw/'intent.json'); term=read(raw/'terminal.json'); verifier=read(raw/'workflow-verifier.json'); session=read(raw/'session.json'); observation=read(case/'physical-observation.json')
 check('intent_terminal_binding',term['intent_sha256']==sha(raw/'intent.json') and term['execution_id']==intent['execution_id'] and term['source_sha256']==intent['original_binding']['source_sha256'])
 check('verifier_binding',verifier['original_terminal_sha256']==sha(raw/'terminal.json') and verifier['original_intent_sha256']==sha(raw/'intent.json') and verifier['original_config_sha256']==sha(raw/'config.json'))
 census=read(case/'control-outcome.json')
 check('case_identity',census['case_id']==term['case_id'] and term['case_id']==intent['original_binding']['case_id'])
 check('closed_current_batch_policy',config['journal_read_policy']==gen['journal_read']==gen['context']['journal_read'] and config['journal_read_policy']['protocol']=='candidate-journal-batch-read-v1' and config['journal_read_policy']['cross_checkpoint_cache'] is False)
 check('closed_combined_review_policy',config['review_read_policy']=={'protocol':'workflow-current-review-read-v2','scope':'product workflow current-state boundary','read':'one fresh authenticated report and provenance together','checkpoint_guards':'before and after original read','cross_call_cache':False})
 check('config_binding',gen['context']['execution_context']['config_sha256']==sha(raw/'config.json'))
 check('config_canonical',json.dumps(config,sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False).encode()==(raw/'config.json').read_bytes())
 for name,h in config['journal_read_policy']['sources'].items():check('reader_source:'+name,sha(ROOT/name)==h)
 if complete:
  check('completed_case',term['status']=='completed' and term['infrastructure']==[] and verifier['mechanics']['status']=='passed' and verifier['mechanics']['session_complete'] is True and census['qualification_verified'] is True)
  check('natural_exit',session['exit_code']==0 and session['timed_out'] is False and session['natural_exit'] is True and session['errors']==[])
  witness=read(case/'declared-defect-witness.json')
  check('specific_defect_detected',witness['qualified'] is True and witness['generic_unavailable_is_detection'] is False and any(x['case_id']==witness['specific_failed_selector'] and x['status']=='failed' for x in observation['execution']['outcomes']))
  if case.name == 'stop-after-error-WF13-domain-errors-continue':
   unknown = [x['case_id'] for x in observation['execution']['outcomes'] if x['status']=='infrastructure_error']
   check('exact_intentionally_missing_result_slots',unknown==['WF13-domain-errors-continue:call-000:result-%03d' % i for i in range(1,7)])
   check('original_early_stop_shape',read(raw/'call-000-response.bin')=={'kind':'result','phase':'call-000','value':{'results':[{'error':'not_found'}],'documents':[],'jobs':[]}})
   check('shape_failure_not_unknown_is_witness',witness['specific_failed_selector']=='WF13-domain-errors-continue:call-000:shape' and verifier['mechanics']['unavailable']==[])
  else:
   check('no_infrastructure_unknown',all(x['status'] in ('passed','failed') for x in observation['execution']['outcomes']))
  check('final_exec_completed',read(raw/'session-final-exec-verified.json')['completed'] is True)
 else:
  check('observed_infrastructure_error',term['status']=='infrastructure_error' and term['infrastructure']==['WorkflowDeadlineExceeded:Declared workflow observation deadline reached'] and verifier['mechanics']['status']=='infrastructure_error' and verifier['mechanics']['session_complete'] is False and census['qualification_verified'] is False)
  check('candidate_response_without_eligibility','call-000-response.bin' in positions and 'call-000-result-exec-verified.json' in positions and 'call-000-next.json' not in positions and 'session-final-exec-response.bin' not in positions)
  outcomes=observation['execution']['outcomes']
  check('no_completed_qualification',census['status']=='failed' and census['qualification_verified'] is False and observation['execution']['terminal_status']=='infrastructure_error')
  check('unknown_completion_retained',[x['case_id'] for x in outcomes if x['status']=='infrastructure_error']==['workflow-final-m4-v1:WF19-provisional-fault-boundary:mechanics'])
  expected_failed={'WF19-provisional-fault-boundary:call-000:result-004','WF19-provisional-fault-boundary:call-000:result-006'} | {'WF19-provisional-fault-boundary:call-000:capture:'+stage+':'+part for stage in ('post_fault','reopened') for part in ('blobs','documents','jobs')}
  check('partial_fault_observations_retained',{x['case_id'] for x in outcomes if x['status']=='failed'}==expected_failed and all(x['status'] in ('passed','failed','infrastructure_error') for x in outcomes))
  check('phase_incomplete_not_promoted',verifier['phase_facts'][0]['capture_authenticated'] is False and verifier['phase_facts'][0]['response_authenticated'] is True and verifier['phase_facts'][0]['reason']=='Original evidence absent: call-000-next.json')
  check('forced_exit',session['exit_code']==137 and session['timed_out'] is True and session['natural_exit'] is False)
 check('unchanged_deadlines',config['deadline_policy']['call_seconds']==30 and config['deadline_policy']['history_seconds']==300)
 check('cleanup_claim',term['cleanup_verified'] is True and term['container_cleanup'] is True and term['volume_cleanup'] is True)
 def streams(rec):
  for which in ('stdout','stderr'):
   desc=rec[which]; payload=(raw/desc['path']).read_bytes()
   check('stream:'+desc['path'],len(payload)==desc['bytes']==desc['observed_bytes'] and hashlib.sha256(payload).hexdigest()==desc['sha256'] and desc['truncated'] is False)
 streams(session)
 cleanup=[]
 for i,kind in [(0,'volume'),(1,'container')]:
  retired=read(raw/f'emergency-claim-{i:04d}-retired.json'); remove=read(raw/retired['remove_record']); after=read(raw/retired['absence_record']); resource=retired['resource_id']
  check(kind+':retirement',retired['removal_acknowledged'] is True and retired['claim']==f'emergency-claim-{i:04d}.json' and retired['remove_record']==kind+'-remove.json' and retired['absence_record']==kind+'-after.json')
  check(kind+':order',positions[retired['claim']]<positions[retired['remove_record']]<positions[retired['absence_record']]<positions[f'emergency-claim-{i:04d}-retired.json']<positions['terminal.json'])
  for label,record in [('remove',remove),('absence',after)]:
   check(kind+':'+label,record['exit_code']==0 and record['timed_out'] is False and record['capture_complete'] is True and record['arguments']==record['argv']); streams(record)
  check(kind+':removal_target',remove['argv'][-1]==resource and (raw/remove['stdout']['path']).read_bytes()==(resource+'\n').encode())
  expected_name=intent['container'] if kind=='container' else intent['volume']
  check(kind+':absence_target',after['argv'][-1]=='name=^'+('/' if kind=='container' else '')+expected_name+'$' and after['stdout']['bytes']==0 and after['stderr']['bytes']==0)
  cleanup.append({'kind':kind,'resource_id':resource,'retirement_sha256':sha(raw/f'emergency-claim-{i:04d}-retired.json')})
 timefile=next(case.glob('*timings.json')); timings=read(timefile); spans=timings['spans']; ids={r['seq']:r for r in spans}; children=collections.defaultdict(list); exclusive=collections.Counter(); inclusive=collections.Counter(); counts=collections.Counter()
 check('timings_complete_identity',timings['truncated'] is False and timings['execution_id']==intent['execution_id'] and timings['binding_sha256']==digest(intent['original_binding']) and list(ids)==list(range(len(spans))))
 for r in spans:
  check('span:'+str(r['seq']),r['end_ns']>=r['start_ns'] and (r['parent'] is None or (r['parent']<r['seq'] and ids[r['parent']]['start_ns']<=r['start_ns']<=r['end_ns']<=ids[r['parent']]['end_ns'])))
  children[r['parent']].append(r)
 for parent,rows in children.items():
  rows.sort(key=lambda r:r['start_ns']); check('siblings:'+str(parent),all(a['end_ns']<=b['start_ns'] for a,b in zip(rows,rows[1:])))
 for r in spans:
  n=r['end_ns']-r['start_ns']; sub=sum(c['end_ns']-c['start_ns'] for c in children[r['seq']]); check('exclusive:'+str(r['seq']),n>=sub)
  exclusive[r['stage']]+=n-sub; inclusive[r['stage']]+=n; counts[r['stage']]+=1
 union=sum(r['end_ns']-r['start_ns'] for r in children[None]); check('exclusive_partition',sum(exclusive.values())==union)
 return {'case':str(case),'control_status':'qualified_defect_control' if complete else 'failed_infrastructure','original_prefix':prefix,'source_sha256':term['source_sha256'],'cleanup_originals':cleanup,'verifier_mechanics':verifier['mechanics'],'outcomes':observation['execution']['outcomes'],'timings':{'path':str(timefile),'sha256':sha(timefile),'spans':len(spans),'instrumented_union_seconds':union/1e9,'stage_counts':dict(counts),'inclusive_seconds':{k:v/1e9 for k,v in inclusive.items()},'exclusive_seconds':{k:v/1e9 for k,v in exclusive.items()},'error_spans':[r for r in spans if r['status']!='complete']}}
cases=[]
for name,complete in [('lookup-before-token-WF16-absent-token-and-hook-distinction',True),('fresh-reopen-inherited-text-persistence',True),('empty-export-is-all-WF10-defaults-and-export-selection',True),('unbounded-epoch-WF15-existing-job-token-domain',True),('stop-after-error-WF13-domain-errors-continue',True),('after-commit-fault-WF19-provisional-fault-boundary',False)]:
 check_prefix=name+':'
 case=next((RUN/'classes').glob('*/artifacts/*/'+name))
 cases.append(audit_case(case,complete))
report={'protocol':'workflow-apple-git-physical-audit-v1','audit_status':'passed','qualification_status':'failed_infrastructure','session':str(RUN),'summary_sha256':sha(RUN/'summary.json'),'source_input_count':len(inp['inputs']),'ordered_outcomes':s['outcomes'],'cases':cases,'checks':checks,'new_model_quality_samples':0,'limits':['Root stdlib retained-original/source consistency audit, not independent scientific acceptance.','Five fixed defective fixtures were specifically rejected within deadline; this is harness qualification, not model-quality evidence.','Sixth fixture remains infrastructure failure;32 checks unrun; shared6 deferred.','Timing sidecars are diagnostics, not acceptance. Span union is not total workflow duration or a call window.','Local prefix/head consistency is not an independently supplied trust anchor. Historical cleanup was checked; no new live Engine census.','Different histories are not paired performance measurements.'], 'next_action':'Measure fresh checkpoint reader ancestry cost on identical byte corpora at different directory depths before choosing a new execution layout or source change; no unchanged retry or deadline widening.'}
with OUT.open('x') as f:json.dump(report,f,indent=2,sort_keys=True);f.write('\n')
print(json.dumps({'output':str(OUT),'sha256':sha(OUT),'checks':len(checks),'cases':[{'name':Path(c['case']).name,'status':c['control_status'],'raw_files':c['original_prefix']['raw_file_count'],'timings':{k:v for k,v in c['timings'].items() if k not in ('error_spans','inclusive_seconds')}} for c in cases]},indent=2))
