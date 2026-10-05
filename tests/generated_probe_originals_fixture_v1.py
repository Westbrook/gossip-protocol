"""Synthetic complete original chains for reader tests only.

No Engine, candidate import/execution, model or real reviewer is involved. Every
record is deliberately authored by this fixture and linked through real local
Git, state and journals. These records must never enter a scientific cohort.
"""
from contextlib import ExitStack
from copy import deepcopy
from dataclasses import asdict, replace
import io
import json
from pathlib import Path
import sqlite3
import tarfile
import tempfile
import time

from gossip_harness import candidate_checkpoint_chain_v1 as chain
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import cumulative_generated_probe_reader_v1 as reader
from gossip_harness import cumulative_generated_probe_review_v1 as reviews
from gossip_harness import cumulative_generated_probe_state_v1 as state
from gossip_harness import cumulative_generated_probe_values_v2 as values
from gossip_harness import cumulative_generated_probe_wire_v1 as wire
from gossip_harness.gitstore import GitStore
from tests.test_candidate_storage_prestart_v1 import created_fixture
from tests.test_cumulative_generated_probe_plan_v1 import declaration, policy, release, target
from tests.test_cumulative_generated_probe_values_v2 import admitted, proposal, values_for


class SyntheticOriginals:
    def __init__(self, template='refresh-noop-v1', *, defect=False):
        self.stack=ExitStack()
        self.root=Path(self.stack.enter_context(tempfile.TemporaryDirectory(prefix='synthetic-probe-originals-'))).resolve()
        self.row=proposal(template,text='old\ncafé',replacement='new\n🌍');self.probe=admitted(self.row)
        self.vals=values_for(self.row)
        if defect:
            if template=='refresh-noop-v1':self.vals['refresh']['status']='refreshed'
            elif template=='refresh-identity-v1':self.vals['refresh']['record']['document']['document_id']='wrong'
            elif template=='completed-receipt-replay-v1':self.vals['replay']['documents'][0]['text']='wrong'
            else:self.vals['captured_job']['content_hashes']=json.dumps(['0'*64])
        self.files={'solution.py':b'raise RuntimeError("synthetic source must never execute")\n'}
        self.store=GitStore.create(self.root/'source.git',{k:v.decode() for k,v in self.files.items()})
        commit=self.store.head();tree=self.store._git('rev-parse',commit+'^{tree}')
        self.capture_files={};layout=None
        if template=='manifest-content-hash-v1':
            p=self.root/'authored-schema.sqlite';db=sqlite3.connect(p)
            try:
                db.execute('CREATE TABLE jobs (job_id TEXT PRIMARY KEY,manifest TEXT,content_hashes TEXT,receipt TEXT)')
                row=self.vals['captured_job'];db.execute('INSERT INTO jobs VALUES (?,?,?,?)',(values.JOB,row['manifest'],row['content_hashes'],row['receipt']));db.commit()
            finally:db.close()
            raw=p.read_bytes();self.capture_files={'m2/library.sqlite':raw}
            layout=reader.plans.CaptureLayout(('m2/library.sqlite',),reader.sqlite_observation.sqlite_schema_sha256(raw))
        self.plan=declaration(proposed=self.probe,public=release('M4',('M2-REFRESH','M3-BACKUP-RESTORE')),
            subject=target(self.files,commit,tree,milestone='M4'),limits=replace(policy(),history_seconds=300),layout=layout)
        rraw,rdelta=self.root/'review-raw',self.root/'review-delta'
        rh=ExternalHead.create(self.root/'review-head',journal_roots=(rraw,rdelta));self.stack.callback(rh.close)
        rj=chain.CheckpointChain.create(rraw,rdelta,context={'synthetic_fixture_only':True,'actual_source_review_supplied':False},authority=rh);self.stack.callback(rj.close)
        req=values.canonical(self.plan.review_request());sha=reader.transport.sha
        report=values.canonical({'protocol':reviews.PROTOCOL,'purpose':reader.plans.REVIEW_PURPOSE,
            'reviewer_id':'synthetic-unit-reviewer','request_sha256':sha(req),
            'decisions':[{'id':d,'decision':'approved','rationale':'Synthetic complete-record linkage fixture only.',
                          'source_references':['synthetic-originals-fixture:1']} for d in reader.plans.DUTIES],
            'remaining_obligations':['No actual independent approval or physical execution.']})
        delivery=values.canonical({'protocol':reviews.PROTOCOL,'purpose':reader.plans.REVIEW_PURPOSE,
            'reviewer_id':'synthetic-unit-reviewer','request_sha256':sha(req),'report_sha256':sha(report),
            'origin':'independently_delivered_host_review'})
        for name,raw in [('request.json',req),('report.json',report),('delivery.json',delivery)]:rj.retain(name,raw)
        self.review=reviews.ProbeReviewAuthority(rj,rj.commitment,reviews.ReviewEnrollment('synthetic-unit-reviewer',
            'request.json',sha(req),'report.json',sha(report),'delivery.json',sha(delivery)))
        self.image=reader.execution.RuntimePolicy(10).image_id
        bind=reader.prestart.bind_source_policy();self.version=deepcopy(bind['version'])
        self.info={**deepcopy(bind['info']),'ID':'synthetic-daemon'}
        self.image_info={'Id':self.image,'Os':'linux'}
        self.endpoint={'socket_path':'/synthetic/no-engine.sock','device':1,'inode':2}
        self.runtime={'protocol':reader.process.PROTOCOL,'endpoint':self.endpoint,'api_version':reader.process.API_VERSION,
            'os':self.version['Os'],'engine_git_commit':self.version['GitCommit'],'cgroup_version':self.info['CgroupVersion'],
            'cgroup_driver':self.info['CgroupDriver'],'oom_kill_disable_supported':self.info['OomKillDisable'],
            'daemon_id':self.info['ID'],'engine_version':self.version['Version'],'architecture':self.version['Arch'],
            'kernel_version':self.version['KernelVersion'],'image_id':self.image,
            'image_inspect_sha256':reader.process._sha(reader.process._encoded(self.image_info))}
        self.environment=reader.execution.environment_for(self.plan,clock_domain='synthetic-record-clock')
        now=time.monotonic_ns();self.window=state.ProbeWindow(now,now+300_000_000_000)
        self.binding=state.binding_for(self.plan,self.review,runtime=self.runtime,environment=self.environment,window=self.window)
        self.registration=state.observation_registration(self.plan,self.binding,gate_id='synthetic-probe-gate',
            repetition_id='synthetic-record-read',cohort_trajectory_ids=('trajectory','t2','t3','t4','t5','t6'))
        self.available=True
        self.admission=admission.ObservationAdmission(self.registration,verify_registration=lambda:self.registration if self.available else None)
        self.raw,self.delta=self.root/'state-raw',self.root/'state-delta'
        self.head=ExternalHead.create(self.root/'state-head',journal_roots=(self.raw,self.delta));self.stack.callback(self.head.close)
        self.owner=self.open();self.owner.begin()
        self.records={};self.execution_id='probe-'+'1'*32;self.name='gossip-'+self.execution_id;self.volume='gossip-volume-'+self.execution_id
        self.cid='a'*64;self.exec_id='e'*64;self.pid=321
        self.docker=['docker','--host','unix://'+self.endpoint['socket_path']]
        self.helpers=reader.driver.adapter_files(self.probe,released_requirements=('M2-REFRESH','M3-BACKUP-RESTORE'))
        self.staging={'source_manifest':admission.source_manifest(self.files),'helper_manifest':admission.source_manifest(self.helpers)}
        self.mounts={'/workspace':str(self.root/'source-mount'),'/checks':str(self.root/'helper-mount')}
        self.labels={'gossip.execution':self.execution_id,'gossip.source':self.binding.source_sha256,
                     'gossip.fixture':values.digest(admission.source_manifest(self.helpers))}
        self.created=self.created_value();self.running=deepcopy(self.created)
        self.running['State'].update(Status='running',Running=True,Pid=123,StartedAt='2026-10-04T00:00:01.000000001Z')
        self.running['ExecIDs']=[self.exec_id]
        self.build()

    def open(self, expected=None):
        value=state.ProbeExecutionState(self.raw,self.delta,head=self.head,store=self.store,plan=self.plan,
            review=self.review,binding=self.binding,registration=self.registration,admission_authority=self.admission,
            runtime=self.runtime,environment=self.environment,expected_checkpoint=expected)
        self.stack.callback(value.close);return value

    def close(self):self.stack.close()

    def put(self,name,value):
        assert name not in self.records,name
        self.records[name]=value if type(value) is bytes else values.canonical(value)

    def http(self,label,path,value):
        body=values.canonical(value)
        self.put(label+'-request.bin',reader.process._request('GET',path))
        self.put(label+'-response.bin',b'HTTP/1.1 200 OK\r\nContent-Length: '+str(len(body)).encode()+b'\r\n\r\n'+body)

    def command(self,label,argv,stdout=b'',limit=reader.storage.MAX_STREAM_BYTES):
        argv=self.docker+argv[1:];self.put(label+'-dispatch.json',{'argv':argv,'limit':limit})
        streams={}
        for kind,raw in [('stdout',stdout),('stderr',b'')]:
            name=label+'-'+kind+'.bin';self.put(name,raw)
            streams[kind]={'path':name,'bytes':len(raw),'observed_bytes':len(raw),'sha256':reader.transport.sha(raw),'truncated':False}
        self.put(label+'.json',{'argv':argv,'arguments':argv,'exit_code':0,'timed_out':False,'capture_complete':True,**streams})

    def runtime_at(self,label):
        for kind,path,value in [('version','/version',self.version),('info','/info',self.info),('image','/images/'+self.image+'/json',self.image_info)]:
            self.http(label+'-'+kind,path,value)
        self.put(label+'.json',self.runtime);self.put(label+'-verified.json',{'runtime_sha256':values.digest(self.runtime)})

    def identity(self,label,completed=False):
        self.http(label+'-container','/containers/'+self.cid+'/json',self.running)
        value={'ID':self.exec_id,'ContainerID':self.cid,'Running':not completed,'Pid':self.pid,'OpenStdin':True,
               'ProcessConfig':{'entrypoint':'python','arguments':['-I','-B','/checks/child_driver.py'],
                                'user':'65534:65534','privileged':False,'tty':False}}
        if completed:value['ExitCode']=0
        self.http(label+'-exec','/exec/'+self.exec_id+'/json',value)
        self.put(label+'-identity-verified.json',{'exec_id':self.exec_id,'pid':self.pid,'completed':completed,'value_sha256':values.digest(value)})

    def created_value(self):
        value,_=created_fixture();value.update(Id=self.cid,Name='/'+self.name,Image=self.image)
        value['Config'].update(Image=self.image,Labels=self.labels)
        for row in value['HostConfig']['Mounts']:
            row['Source']=self.volume if row['Type']=='volume' else self.mounts[row['Target']]
        for row in value['Mounts']:
            if row['Type']=='volume':row.update(Name=self.volume,Source='/var/lib/docker/volumes/'+self.volume+'/_data')
            else:row['Source']=self.mounts[row['Destination']]
        return value

    def volume_value(self):
        return {'Name':self.volume,'Driver':'local','Scope':'local','Options':reader.storage.VOLUME_OPTIONS,
                'Labels':{'gossip.execution':self.execution_id,'gossip.snapshot':reader.storage.SNAPSHOT_PROTOCOL}}

    def capture(self):
        self.runtime_at('capture-runtime-before');self.identity('capture-before')
        self.command('capture-pause',['docker','pause',self.cid]);paused=deepcopy(self.running);paused['State']['Paused']=True
        self.command('capture-state',['docker','inspect','--format','{{json .}}',self.cid],values.canonical(paused))
        buffer=io.BytesIO()
        with tarfile.open(fileobj=buffer,mode='w',format=tarfile.USTAR_FORMAT) as archive:
            root=tarfile.TarInfo('tmp');root.type=tarfile.DIRTYPE;archive.addfile(root)
            for name,raw in self.capture_files.items():
                item=tarfile.TarInfo('tmp/'+name);item.size=len(raw);archive.addfile(item,io.BytesIO(raw))
        self.command('capture-tar',['docker','cp',self.cid+':/tmp','-'],buffer.getvalue(),reader.storage.MAX_CAPTURE_BYTES)
        self.command('capture-unpause',['docker','unpause',self.cid])
        self.runtime_at('capture-runtime-after');self.identity('capture-after')
        self.put('capture-staging.json',self.staging);self.put('captured-job.json',self.vals['captured_job'])

    def build(self):
        self.put('physical-intent.json',{'protocol':reader.execution.PROTOCOL,'execution_id':self.execution_id,
            'container':self.name,'volume':self.volume,'binding_sha256':values.digest(asdict(self.binding)),
            'state_intent_sha256':reader.transport.sha(self.owner.journal.read('intent.json')),
            'cleanup_root':str(self.root/'cleanup'),'environment':self.environment})
        self.put('staging.json',{'workspace':self.mounts['/workspace'],'checks':self.mounts['/checks'],'proof':self.staging})
        self.runtime_at('runtime')
        self.command('volume-before',['docker','volume','ls','--quiet','--filter','name=^'+self.volume+'$'])
        self.command('container-before',['docker','container','ls','--all','--quiet','--filter','name=^/'+self.name+'$'])
        argv=['docker','volume','create','--driver','local','--label','gossip.execution='+self.execution_id,
              '--label','gossip.snapshot='+reader.storage.SNAPSHOT_PROTOCOL]
        for k,v in reader.storage.VOLUME_OPTIONS.items():argv.extend(('--opt',k+'='+v))
        argv.append(self.volume);self.command('volume-create',argv,self.volume.encode()+b'\n')
        self.command('volume-created',['docker','volume','inspect','--format','{{json .}}',self.volume],values.canonical(self.volume_value()))
        sandbox=reader.DockerValidator(self.image,{k:v.decode() for k,v in self.helpers.items()},command=reader.prestart.COMMAND)
        argv=reader.storage._start_arguments(sandbox,self.name,Path(self.mounts['/workspace']),Path(self.mounts['/checks']),self.volume)
        argv[1]='create';argv.remove('--detach');index=argv.index('--entrypoint')
        for k,v in self.labels.items():argv[index:index]=['--label',k+'='+v];index+=2
        self.command('container-create',argv,self.cid.encode()+b'\n')
        raw=values.canonical(self.created)
        self.command('container-prestart',['docker','inspect','--format','{{json .}}',self.cid],raw,reader.process.CONTROL_LIMIT)
        proof=reader.prestart.proof_for(raw,container_id=self.cid,name=self.name,image_id=self.image,volume=self.volume,
            labels=self.labels,mounts=self.mounts,runtime=self.runtime,
            runtime_originals={n:self.records[n] for n in reader.prestart.RUNTIME_ORIGINAL_NAMES})
        self.put(reader.prestart.PROOF_FILE,proof)
        self.command('container-start',['docker','start',self.cid],self.cid.encode()+b'\n')
        self.put('session-dispatch.json',{'argv':self.docker+['exec','--interactive','--user','65534:65534',self.cid,
                                                          'python','-I','-B','/checks/child_driver.py']})
        transcript=wire.ValueTranscript(self.probe,released_requirements=('M2-REFRESH','M3-BACKUP-RESTORE'),limits=self.plan.policy.wire_limits)
        frames=[];acks=[]
        while (slot:=transcript.next_slot) is not None:
            artifact=reader.pipes.artifact_slot(slot)
            value={'closed':True} if slot=='finished' else {'job_id':values.JOB,'database':wire.DATABASE_PATH} if slot=='capture_job' else self.vals[slot]
            raw=values.canonical({'protocol':wire.PROTOCOL,'slot':slot,'value':value})+b'\n'
            frames.append(raw);self.put('probe-frame-'+artifact+'.bin',raw);self.runtime_at(artifact+'-runtime');self.identity(artifact)
            self.put(artifact+'-staging.json',self.staging);transcript.append(raw)
            if slot=='capture_job':self.capture();transcript.supply_captured_value(self.vals['captured_job'])
            ack=wire.continuation_bytes(slot);self.put('probe-continue-'+artifact+'-intent.bin',ack)
            row={'slot':slot,'requested_bytes':len(ack),'written_bytes':len(ack)};acks.append(row)
            self.put('probe-continue-'+artifact+'-written.json',row)
        streams={}
        for kind,raw in [('stdout',b''.join(frames)),('stderr',b'')]:
            self.put('probe-'+kind+'.bin',raw);streams[kind]={'bytes':len(raw),'observed_bytes':len(raw),
                'sha256':reader.transport.sha(raw),'truncated':False}
        terminal={'protocol':reader.pipes.PROTOCOL,'value':transcript.result(),'natural_exit':True,'mechanics_complete':True,
            'exit_code':0,'local_pipes_closed':True,'infrastructure':[],'acks':acks,'execution_authority':False,
            'acceptance_authority':False,'container_cleanup_proven':False,'runtime_identity_proven':False,'streams':streams}
        self.put('probe-pipe-terminal.json',terminal)
        self.identity('session-final',completed=True);self.runtime_at('runtime-final');self.put('staging-final.json',self.staging)
        self.command('container-remove',['docker','rm','--force',self.cid])
        self.command('container-after',['docker','container','ls','--all','--quiet','--filter','name=^/'+self.name+'$'])
        self.command('volume-cleanup-inspect',['docker','volume','inspect','--format','{{json .}}',self.volume],values.canonical(self.volume_value()))
        self.command('volume-remove',['docker','volume','rm',self.volume])
        self.command('volume-after',['docker','volume','ls','--quiet','--filter','name=^'+self.volume+'$'])
        self.put('physical-terminal.json',{'protocol':reader.execution.PROTOCOL,'execution_id':self.execution_id,
            'pipe_result':terminal,'qualified_execution_originals':True,'container_cleanup':True,'volume_cleanup':True,
            'infrastructure':[],'acceptance_authority':False,'cold_reconstruction_supplied':False})

    def retain(self, *, stop_before=None, drop=(), transform=None):
        for name,raw in self.records.items():
            if name==stop_before:break
            if name not in drop:self.owner.journal.retain(name,transform(name,raw) if transform else raw)
        return self.owner.journal.checkpoint()
