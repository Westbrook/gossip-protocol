#!/usr/bin/env python3
"""Supplementary same-database handoff diagnostic after the live study freezes.

Every selected four-stage trajectory is eligible, including primary failures.
Each story carries one database through fresh CLI processes and source versions.
This score never changes primary acceptance, requests repairs, or promotes Git.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
import importlib.util
from pathlib import Path
import re
import sys
import time

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY))
from analysis import run_verification_pool_diagnostic as frozen
from gossip_harness.blackbox_validator import BlackboxValidator
from gossip_harness.gitstore import GitStore
from gossip_harness.pilot import DEFAULT_IMAGE
from gossip_harness.sustained_checkpoint import save_checkpoint

PROTOCOL = 'verification-persistence-diagnostic-v1'
CASE_TIMEOUT = 60
SUITE_TIMEOUT = 300
MAX_STEPS = 16
PROJECT_IDS = ('buildgraph', 'calendar')
CASE_MODULES = {'calendar':'verification_calendar_handoff.py', 'buildgraph':'verification_buildgraph_handoff.py'}
require, digest, sha, decode = frozen.require, frozen.digest, frozen.sha, frozen.decode

# Trusted transport only. CLI outputs remain untrusted values; the expected
# outputs and interpretation metadata never enter the candidate container.
PERSISTENCE_ADAPTER = r'''import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading

LIMIT = 65536
STEP_TIMEOUT = 8

def strict_json(raw):
    def constant(_): raise ValueError("nonfinite JSON")
    def pairs(items):
        result = {}
        for key,value in items:
            if key in result: raise ValueError("duplicate JSON key")
            result[key] = value
        return result
    return json.loads(raw.decode("utf-8"),parse_constant=constant,object_pairs_hook=pairs)

def execute(arguments, payload, cwd):
    process = subprocess.Popen(arguments, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, cwd=cwd)
    buffers = [bytearray(),bytearray()]
    overflow = threading.Event()
    io_failed = threading.Event()
    def drain(stream, index):
        try:
            while chunk := stream.read(8192):
                buffers[index].extend(chunk[:max(0,LIMIT+1-len(buffers[index]))])
                if len(buffers[index]) > LIMIT:
                    overflow.set()
                    process.kill()
        except (OSError,ValueError): io_failed.set()
    readers = [threading.Thread(target=drain,args=(stream,index),daemon=True)
               for index,stream in enumerate((process.stdout,process.stderr))]
    for reader in readers: reader.start()
    failure = None
    try:
        if payload is not None: process.stdin.write(payload)
        process.stdin.close()
        process.wait(timeout=STEP_TIMEOUT)
        if process.returncode: failure = 'cli_exit'
    except subprocess.TimeoutExpired: failure = 'cli_timeout'
    except (BrokenPipeError,OSError): failure = 'cli_io_error'
    finally:
        if process.poll() is None: process.kill()
        process.wait(timeout=2)
        for reader in readers: reader.join(timeout=1)
        if any(reader.is_alive() for reader in readers) or io_failed.is_set(): failure = 'cli_output_error'
        else:
            process.stdout.close()
            process.stderr.close()
    if overflow.is_set(): failure = 'cli_output_limit'
    if failure is not None:
        return None,dict(kind=failure,return_code=process.returncode,
                         stderr=bytes(buffers[1][:1024]).decode('utf-8',errors='replace'))
    try: return strict_json(bytes(buffers[0])),None
    except (ValueError,UnicodeError,RecursionError):
        return None,dict(kind='cli_invalid_json',return_code=process.returncode,
                         stderr=bytes(buffers[1][:1024]).decode('utf-8',errors='replace'))

def solve(payload):
    project = payload['project_id']
    steps = payload['steps']
    if project not in ('calendar','buildgraph') or not 1 <= len(steps) <= 16:
        raise ValueError('Unsupported persistence story')
    outputs = []
    with tempfile.TemporaryDirectory(prefix='persistent-story-') as directory:
        database = str(Path(directory) / 'state.sqlite')
        for index,step in enumerate(steps):
            stage = step['stage_index']
            if type(stage) is not int or not 0 <= stage <= 3: raise ValueError('Invalid stage')
            root = Path(__file__).resolve().parent / 'versions' / ('stage-'+str(stage))
            command = json.dumps(step['command'],ensure_ascii=True,allow_nan=False,separators=(',',':'))
            if project == 'calendar':
                arguments = [sys.executable,str(root/'calendar_app/cli.py'),database,command]
                incoming = None
            else:
                arguments = [sys.executable,'-I',str(root/'buildgraph_app/cli.py'),database]
                incoming = command.encode('utf-8')
            answer,failure = execute(arguments,incoming,directory)
            if failure is not None:
                return dict(outputs=outputs,completed_steps=len(outputs),
                            failure=dict(step_index=index,stage_index=stage,**failure))
            outputs.append(answer)
    return dict(outputs=outputs,completed_steps=len(outputs),failure=None)
'''
ADAPTER_SHA256 = sha(PERSISTENCE_ADAPTER.encode())


def load_stories():
    stories, sources = {}, {}
    for project_id,name in CASE_MODULES.items():
        path = REPOSITORY/'analysis'/name
        require(path.is_file() and not path.is_symlink(), 'Missing trusted story fixture: '+name)
        raw = path.read_bytes()
        spec = importlib.util.spec_from_file_location('_trusted_persistence_'+project_id,path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)  # Only the fixed, trusted data-only fixture module.
        require(module.PROJECT_ID == project_id, 'Story project identity changed')
        stories[project_id] = validate_stories(module.CASES)
        sources[path.resolve()] = raw
    return stories,sources


def validate_stories(cases):
    require(isinstance(cases,(list,tuple)) and len(cases)==2, 'Exactly two stories are required per project')
    result,ids = [],set()
    for case in cases:
        require(isinstance(case,dict) and isinstance(case.get('id'),str) and case['id'] not in ids,
                'Story IDs must be distinct strings')
        ids.add(case['id'])
        require(all(key in case for key in ('requirements','contract_basis','classification',
                    'required_contract_vs_extra_assumptions','steps','expected_outputs')), 'Story interpretation metadata is required')
        steps = case['steps']
        require(isinstance(steps,(list,tuple)) and 1<=len(steps)<=MAX_STEPS
                and len(case['expected_outputs'])==len(steps), 'Story must have 1..16 steps and matching expectations')
        indices=[]
        for step in steps:
            require(isinstance(step,dict) and set(step)=={'stage_index','command'}
                    and type(step['stage_index']) is int and 0<=step['stage_index']<=3
                    and isinstance(step['command'],dict), 'Malformed stage-indexed command')
            indices.append(step['stage_index'])
        require(indices==sorted(indices) and set(indices)=={0,1,2,3}, 'Story must traverse all four versions in forward order')
        digest(case)  # Reject non-finite or non-JSON fixture values before execution.
        result.append(deepcopy(case))
    return result


def execution_cases(project_id, stories):
    return [dict(id=case['id'],requirement='persistent-data-handoff',
                 input=dict(project_id=project_id,steps=deepcopy(case['steps'])),
                 expected=dict(outputs=deepcopy(case['expected_outputs']),completed_steps=len(case['steps']),failure=None))
            for case in stories]


def combine_versions(versions):
    require(len(versions)==4, 'Four selected source versions are required')
    files={'solution.py':PERSISTENCE_ADAPTER}
    for index,version in enumerate(versions):
        require(isinstance(version,dict) and version, 'Missing source version')
        for name,content in version.items():
            require(isinstance(name,str) and isinstance(content,str), 'Source files must be text')
            files[f'versions/stage-{index}/{name}']=content
    # Reuse the existing static safety/size checks; this does not launch Docker.
    BlackboxValidator._inputs(files,[dict(input={},expected=None)])
    return files


def _reader(inputs):
    def read(path):
        path=Path(path)
        require(path.is_file() and not path.is_symlink(), 'Expected ordinary frozen evidence: '+str(path))
        raw=path.read_bytes(); inputs[path.resolve()]=raw
        return raw
    return read,lambda path:decode(read(path))


def context(image, stories, helper_sources):
    return dict(purpose=PROTOCOL,image=image,case_timeout_seconds=CASE_TIMEOUT,suite_timeout_seconds=SUITE_TIMEOUT,
                maximum_steps=MAX_STEPS,adapter_sha256=ADAPTER_SHA256,blackbox_adapter_sha256=frozen.ADAPTER_SHA256,
                stories_sha256=digest(stories),core_sources=helper_sources,runtime=frozen.runtime(),
                environment='Frozen DockerValidator sandbox; expected values and metadata remain on host',
                database_lifetime='New database per story; same pathname and contents across every version/command',
                candidate_process_lifetime='One fresh fixed-CLI subprocess per command')


def prepare(study, output):
    frozen.validate_output(study,output)
    study=Path(study).resolve(); inputs={}; read,data=_reader(inputs)
    read(Path(__file__))
    read(Path(frozen.__file__))
    report=data(study/'results.json'); frozen.completion_gate(report,data(study/'preregistered.json'))
    contract=report['contract']
    require(re.fullmatch(r'sha256:[a-f0-9]{64}',contract.get('image','')) is not None,'Image must be content-addressed')
    require(contract.get('controller_runtime')==frozen.runtime(),'Use the frozen controller runtime')
    require(set(contract['sources'])==set(frozen.CORE),'Frozen helper roster changed')
    for name,expected in contract['sources'].items():
        require(Path(name).name==name and name.endswith('.py'),'Unsafe helper source path')
        require(sha(read(study/'source-snapshot'/'gossip_harness'/name))==expected
                and sha(read(REPOSITORY/'gossip_harness'/name))==expected,'Frozen helper source changed: '+name)
    require(sha(read(study/'study-plan.json'))==contract['plan_sha256'],'Frozen plan changed')
    projects=data(study/'fixtures.json')['projects']
    require(digest(projects)==contract['fixture_sha256'],'Frozen fixture changed')
    projects={p['id']:p for p in projects}
    require(set(projects)==set(PROJECT_IDS),'Unexpected project roster')
    stories,story_sources=load_stories(); inputs.update(story_sources)
    trajectories,skipped=[],[]
    for primary in sorted(report['cases'],key=lambda p:p['run_id']):
        identity=primary['run_id']
        require(identity==f"{primary['project_id']}-{primary['policy']}-{primary['repetition']}",'Unsafe run identity')
        root=study/identity
        require(Path(primary['root']).resolve()==root and data(root/'result.json')==primary,'Primary result binding changed')
        raw=read(root/'trajectory.json')
        require(sha(raw)==primary['trajectory_sha256'],'Frozen trajectory changed')
        state=decode(raw); require(state['contract_sha']==report['contract_sha'],'Trajectory contract changed')
        stages=state['stages']; common={key:primary[key] for key in ('run_id','project_id','policy','repetition')}
        common.update(primary_accepted=primary['accepted'],primary_status=primary['status'])
        require(isinstance(stages,list) and 1<=len(stages)<=4,'Invalid stage count')
        if len(stages)<4:
            require(primary['accepted'] is False,'Accepted trajectory stopped before four stages')
            skipped.append(dict(**common,stages_reached=len(stages),reason='Stopped before stage four; no source version is fabricated'))
            continue
        project=projects[primary['project_id']]; trusted=dict(project['initial_files']); versions=[]; chain=[]
        for index,stage in enumerate(stages):
            require(stage.get('stage_index')==index and type(stage.get('completed')) is bool,'Malformed ordered stage record')
            require(index==0 or stages[index-1]['completed'] is True,'Trajectory continued after an incomplete stage')
            require(data(root/f'stage-{index}'/'result.json')==stage,'Frozen stage result changed')
            binding=stage['selected_binding']; source=Path(binding['store_path']).resolve()
            require(source.is_relative_to(root/f'stage-{index}') and source.suffix=='.git','Selected Git path escapes its stage')
            files=GitStore(source).read_files(binding['tip_sha'])
            require(digest(files)==binding['files_sha256'] and files==stage['files'],'Selected immutable Git files changed')
            require(data(source.with_suffix('.json'))==dict(files=files,binding=binding),'Selected source export changed')
            require(stage['selected'] in stage['candidates'] and stage['candidates'][stage['selected']]['binding']==binding,
                    'Selected alias/binding changed')
            trusted.update(project['stages'][index].get('trusted_updates',{}))
            require(set(files)==set(trusted) and all(files[name]==trusted[name]
                    for name in set(trusted)-set(project['allowed_paths'])),'Fixed CLI or policy version changed')
            versions.append(files)
            chain.append(dict(stage_index=index,completed=stage['completed'],selected=stage['selected'],
                              store_path=str(source),tip_sha=binding['tip_sha'],files_sha256=digest(files),
                              policy_sha256=sha(files['policy.json'].encode())))
        require(state['files']==versions[-1] and digest(versions[-1])==primary['files_sha256'],'Final selected source binding changed')
        trajectories.append(dict(**common,chain=chain,versions=versions,files=combine_versions(versions),
                                 stories=stories[primary['project_id']],suite=execution_cases(primary['project_id'],stories[primary['project_id']])))
    require(len(trajectories)+len(skipped)==18,'Every primary trajectory must be accounted for')
    return dict(report=report,inputs=inputs,stories=stories,trajectories=trajectories,skipped=skipped,
                context=context(contract['image'],stories,contract['sources']))


def golden_plan(image=DEFAULT_IMAGE):
    from gossip_harness.verification_experiment import projects
    stories,inputs=load_stories()
    for path in (Path(__file__),Path(frozen.__file__)):
        inputs[path.resolve()]=path.read_bytes()
    sources={name:sha((REPOSITORY/'gossip_harness'/name).read_bytes()) for name in frozen.CORE}
    for name in frozen.CORE: inputs[(REPOSITORY/'gossip_harness'/name).resolve()]=(REPOSITORY/'gossip_harness'/name).read_bytes()
    trajectories=[]
    for project in projects():
        versions=[deepcopy(stage['known_files']) for stage in project['stages']]
        trajectories.append(dict(run_id='golden-'+project['id'],project_id=project['id'],policy=None,repetition=None,
            primary_accepted=None,primary_status='trusted_golden_qualification',versions=versions,
            chain=[dict(stage_index=i,tip_sha=None,files_sha256=digest(files),policy_sha256=sha(files['policy.json'].encode()))
                   for i,files in enumerate(versions)], files=combine_versions(versions),stories=stories[project['id']],
            suite=execution_cases(project['id'],stories[project['id']])))
    return dict(report=None,inputs=inputs,stories=stories,trajectories=trajectories,skipped=[],
                context=context(image,stories,sources))


def qualify(proof_path,plan):
    root=Path(proof_path).resolve()
    if root.is_file(): root=root.parent
    require(root.is_dir(),'A completed golden qualification directory is required')
    read,data=_reader(plan['inputs'])
    result=data(root/'results.json'); manifest=data(root/'manifest.json')
    require(manifest.get('mode')=='golden-rehearsal' and manifest.get('execution_context')==plan['context']
            and result.get('status')=='finished' and result.get('all_passed') is True,
            'Golden qualification does not match the diagnostic contract')
    for retained,current,key in (('runner.py',Path(__file__),'runner_sha256'),
                                 ('pool-binding-helpers.py',Path(frozen.__file__),'binding_helpers_sha256')):
        require(sha(read(current))==manifest.get(key)==sha(read(root/retained)),
                'Golden qualification implementation changed: '+key)
    rows=result.get('trajectories',[])
    require(len(rows)==2 and {row.get('project_id') for row in rows}==set(PROJECT_IDS), 'Incomplete golden qualification roster')
    from gossip_harness.verification_experiment import projects
    projects={p['id']:p for p in projects()}
    for row in rows:
        project_id=row['project_id']; versions=[stage['known_files'] for stage in projects[project_id]['stages']]
        files=combine_versions(versions); suite=execution_cases(project_id,plan['stories'][project_id])
        require(row['run_id']=='golden-'+project_id,'Unsafe golden receipt identity')
        path=root/'receipts'/(row['run_id']+'.json')
        receipt=data(path)
        require(row['receipt_sha256']==sha(plan['inputs'][path.resolve()]),'Golden receipt binding changed')
        frozen.receipt_rows(receipt,files,suite,plan['context'])
        require(receipt['passed'] is True,'Golden stories did not all pass in Docker')
    return dict(directory=str(root),manifest_sha256=sha(plan['inputs'][(root/'manifest.json').resolve()]),
                results_sha256=sha(plan['inputs'][(root/'results.json').resolve()]))


def run(study,output,*,qualification=None,golden=False,image=DEFAULT_IMAGE):
    output=Path(output)
    if golden:
        require(not output.exists() and not output.is_symlink(),'Use a fresh golden output directory')
        plan=golden_plan(image)
        qualification_binding=None
    else:
        require(qualification is not None,'Pass --qualification with the completed golden rehearsal directory')
        plan=prepare(study,output)
        qualification_binding=qualify(qualification,plan)
    output=output.resolve(); output.mkdir(parents=True,exist_ok=False)
    (output/'receipts').mkdir(); (output/'inputs').mkdir(); (output/'sources').mkdir()
    inventory=[]
    for index,(path,raw) in enumerate(sorted(plan['inputs'].items(),key=lambda pair:str(pair[0]))):
        copied=output/'inputs'/f'{index:04d}-{path.name}'; copied.write_bytes(raw)
        inventory.append(dict(original=str(path),retained=str(copied.relative_to(output)),sha256=sha(raw)))
    own=Path(__file__).read_bytes(); (output/'runner.py').write_bytes(own)
    dependency=Path(frozen.__file__).read_bytes(); (output/'pool-binding-helpers.py').write_bytes(dependency)
    (output/'persistence-adapter.py').write_text(PERSISTENCE_ADAPTER)
    save_checkpoint(output/'stories.json',plan['stories'])
    save_checkpoint(output/'manifest.json',dict(protocol=PROTOCOL,mode='golden-rehearsal' if golden else 'postfreeze-diagnostic',
        preregistered_primary=False,primary_scores_modified=False,model_calls=0,promotions=0,
        original_study=None if golden else str(Path(study).resolve()),
        original_contract_sha=None if golden else plan['report']['contract_sha'],
        execution_context=plan['context'],runner_sha256=sha(own),binding_helpers_sha256=sha(dependency),
        qualification=qualification_binding,inputs=inventory,
        limitations=['Supplementary post-freeze diagnostic; never replaces primary scores.',
                     'All selected four-stage trajectories are eligible, including primary failures; earlier stops are explicitly skipped.',
                     'Literal story expectations may include separately disclosed robustness assumptions.',
                     'A failed story is not automatically an established contract bug.',
                     'Fresh CLI process per step with one persistent SQLite database per story; stories do not share databases.']))
    result=dict(protocol=PROTOCOL,status='running',primary_scores_modified=False,model_calls=0,promotions=0,
                skipped=plan['skipped'],trajectories=[],executions=[],
                unexecuted=[row['run_id'] for row in plan['trajectories']],started_at=time.time())
    save_checkpoint(output/'results.json',result)
    try:
        frozen.unchanged(plan['inputs'])
        validator=BlackboxValidator(plan['context']['image'],timeout_seconds=SUITE_TIMEOUT,case_timeout_seconds=CASE_TIMEOUT)
        okay,detail=validator.preflight(); require(okay,'Docker preflight failed: '+detail)
        for trajectory in plan['trajectories']:
            frozen.unchanged(plan['inputs'])
            identity=trajectory['run_id']; path=output/'receipts'/(identity+'.json')
            save_checkpoint(output/'sources'/(identity+'.json'),dict(files=trajectory['files'],chain=trajectory['chain']))
            execution=dict(run_id=identity,source_sha256=digest(trajectory['files']),suite_sha256=digest(trajectory['suite']),
                           receipt=str(path.relative_to(output)),verified=False)
            result['executions'].append(execution); save_checkpoint(output/'results.json',result)
            try: receipt=validator.evaluate(dict(trajectory['files']),deepcopy(trajectory['suite']))
            except BaseException as error:
                save_checkpoint(path,dict(error=type(error).__name__,last_receipt=getattr(validator,'last_receipt',{})))
                execution.update(raised=type(error).__name__,receipt_sha256=sha(path.read_bytes()))
                raise
            save_checkpoint(path,receipt)  # Retain failed/infrastructure receipts before rejecting them.
            execution['receipt_sha256']=sha(path.read_bytes()); save_checkpoint(output/'results.json',result)
            outcomes=frozen.receipt_rows(receipt,trajectory['files'],trajectory['suite'],plan['context'])
            execution['verified']=True
            row={key:deepcopy(trajectory[key]) for key in ('run_id','project_id','policy','repetition','primary_accepted','primary_status','chain')}
            row.update(combined_source_sha256=digest(trajectory['files']),adapter_sha256=ADAPTER_SHA256,
                       passed=receipt['passed'],receipt=execution['receipt'],receipt_sha256=execution['receipt_sha256'],
                       stories=[{**{key:deepcopy(value) for key,value in story.items() if key not in ('steps','expected_outputs')},
                                     **outcome} for story,outcome in zip(trajectory['stories'],outcomes)])
            result['trajectories'].append(row); result['unexecuted'].remove(identity)
            save_checkpoint(output/'results.json',result)
        frozen.unchanged(plan['inputs'])
        result.update(status='finished',inputs_unchanged=True,all_passed=all(row['passed'] for row in result['trajectories']),
                      summary=dict(eligible_trajectories=len(plan['trajectories']),skipped_trajectories=len(plan['skipped']),
                                   passed_trajectories=sum(row['passed'] for row in result['trajectories']),
                                   failed_trajectories=sum(not row['passed'] for row in result['trajectories'])))
    except BaseException as error:
        result.update(status='interrupted',error_type=type(error).__name__)
        raise
    finally:
        result['finished_at']=time.time(); save_checkpoint(output/'results.json',result)
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run',type=Path,help='Entire finished 18-trajectory live study')
    parser.add_argument('--output',type=Path,help='Fresh supplementary diagnostic directory')
    parser.add_argument('--qualification',type=Path,help='Completed passing golden rehearsal directory')
    parser.add_argument('--golden-rehearsal',type=Path,help='Fresh output directory for trusted golden Docker qualification only')
    args=parser.parse_args()
    if args.golden_rehearsal:
        if args.run or args.output or args.qualification: parser.error('Golden rehearsal is a separate mode')
        result=run(None,args.golden_rehearsal,golden=True)
    else:
        if not args.run or not args.output or not args.qualification: parser.error('--run, --output, and --qualification are required')
        result=run(args.run,args.output,qualification=args.qualification)
    print({'status':result['status'],'summary':result['summary']})
    return 0 if result['status']=='finished' and (not args.golden_rehearsal or result['all_passed']) else 2


if __name__=='__main__': raise SystemExit(main())
