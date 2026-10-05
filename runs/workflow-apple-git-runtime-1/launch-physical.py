"""Declared physical qualification; run only after audited offline completion."""
import hashlib,json,os,shutil,sys
from pathlib import Path
ROOT=Path('/Users/westbrook/.codex/worktrees/cumulative-reader-integration/gossip-protocol')
os.chdir(ROOT)
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
read=lambda p:json.loads(p.read_bytes())
assert sys.argv[1:] in [['workflow'],['shared']]
mode=sys.argv[1]
plan=read(ROOT/'runs/workflow-apple-git-runtime-1/plan.json')
audit=read(ROOT/'analysis/workflow-apple-git-full-terminal-audit-v1.json')
assert audit['status']=='passed' and audit['selected_tests']==775
assert audit['fresh_tests']==680 and audit['reused_tests']==95
run=Path(audit['session']);assert sha(run/'summary.json')==audit['summary_sha256'] and sha(run/'inputs.json')==audit['inputs_sha256']
inputs=read(run/'inputs.json')
for name,h in inputs['inputs'].items():assert sha(ROOT/name)==h,name
selection=read(ROOT/plan['physical_after_offline']['selection'])
for name,h in selection['physical_source_pins'].items():assert sha(ROOT/name)==h,name
binary=Path(plan['git']['path']);assert sha(binary)==plan['git']['sha256']
os.environ['PATH']=plan['private_path_prefix']+os.pathsep+os.environ['PATH']
os.environ.pop('GOSSIP_RUN_DOCKER_TESTS',None)
assert Path(shutil.which('git')).resolve()==binary
sys.path.insert(0,str(ROOT))
from devtools.verify import runtime_fingerprint
assert runtime_fingerprint()==inputs['runtime'],'Runtime differs from audited offline gate'
if mode=='workflow':selectors=selection['workflow']['selectors']
else:
 physical=read(ROOT/'analysis/workflow-apple-git-workflow-terminal-audit-v1.json')
 assert physical['status']=='passed' and physical['selected_tests']==38
 assert read(Path(physical['session'])/'inputs.json')==inputs
 assert sha(Path(physical['session'])/'summary.json')==physical['summary_sha256']
 selectors=selection['shared_transport']['selectors']
args=[sys.executable,'-B','-m','devtools.verify',*selectors,'--lanes','docker','--workers',str(plan['workers']),'--output','runs/workflow-apple-git-physical-1']
os.execv(sys.executable,args)
