"""Reconstruct public probe facts from anchored originals, never runner verdicts.

Caller supplies the exact state capability and an independently retained prefix.
The reader does not dispatch, renew deadlines, adopt uploaded evidence or issue
acceptance. Reopening state for reading does not permit a second execution.
"""
from __future__ import annotations

from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import re
from typing import Any

from . import candidate_checkpoint_chain_v1 as chain
from . import candidate_client_process_v4 as process
from . import candidate_m2_product_observation_v1 as sqlite_observation
from . import candidate_observation_admission_v1 as admission
from . import candidate_storage_driver_v1 as storage
from . import candidate_storage_prestart_v1 as prestart
from . import candidate_storage_product_execution_v1 as transport
from . import cumulative_generated_probe_driver_v1 as driver
from . import cumulative_generated_probe_execution_v1 as execution
from . import cumulative_generated_probe_pipe_v1 as pipes
from . import cumulative_generated_probe_plan_v1 as plans
from . import cumulative_generated_probe_state_v1 as state
from . import cumulative_generated_probe_values_v2 as values
from . import cumulative_generated_probe_wire_v1 as wire
from .sandbox import DockerValidator

PROTOCOL = 'cumulative-generated-probe-reader-v1-journal-labels-v1'
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


class OriginalError(ValueError):
    """Contradictory or substituted original proof; no observation can be used."""


class Unavailable(ValueError):
    """A healthy prefix lacks a required completed observation boundary."""


def require(ok: bool, detail: str) -> None:
    if not ok:
        raise OriginalError(detail)


def same(left: Any, right: Any, detail: str) -> None:
    require(values.canonical(left) == values.canonical(right), detail)


def sources() -> dict[str, str]:
    path = Path(__file__)
    require(hashlib.sha256(path.read_bytes()).hexdigest() == LOADED_SOURCE_SHA256, 'loaded_probe_reader_changed')
    return {**execution.evaluator_sources(), 'gossip_harness/' + path.name: LOADED_SOURCE_SHA256}


class _Reader:
    def __init__(self, owner: state.ProbeExecutionState, expected: chain.PrefixCommitment):
        require(type(owner) is state.ProbeExecutionState and type(expected) is chain.PrefixCommitment,
                'exact_probe_state_and_independent_checkpoint_required')
        owner._owner()
        require(owner.journal is not None, 'original_probe_journal_unavailable')
        assert owner.journal is not None
        self.owner, self.journal, self.expected = owner, owner.journal, expected
        self.checkpoint()
        # Reading after the absolute window is legal; dispatch/continuation is not.
        owner._validate_current(check_window=False)
        require(self.raw('config.json') == owner.config_raw, 'original_probe_configuration_differs')
        self.record = owner.plan.record()
        self.runtime = json.loads(owner.runtime_raw)
        self.environment = json.loads(owner.environment_raw)
        require(type(self.environment.get('clock_domain')) is str, 'original_clock_domain_required')
        same(self.environment, execution.environment_for(owner.plan, clock_domain=self.environment['clock_domain']),
             'original_physical_environment_differs')
        self.image = execution.RuntimePolicy(owner.plan.policy.control_seconds).image_id
        require(self.runtime.get('image_id') == self.image and type(self.runtime.get('endpoint')) is dict,
                'declared_engine_endpoint_and_image_required')
        endpoint = self.runtime['endpoint']
        require(type(endpoint.get('socket_path')) is str and endpoint['socket_path'].startswith('/'),
                'declared_local_endpoint_required')
        self.docker = ['docker', '--host', 'unix://' + endpoint['socket_path']]
        self.running: dict[str, Any] | None = None
        self.exec_id: str | None = None
        self.exec_pid: int | None = None
        self.created: dict[str, Any] = {}
        self.container_id = ''
        self.intent: dict[str, Any] = {}
        self.staging: dict[str, Any] = {}

    def checkpoint(self) -> None:
        require(self.journal.checkpoint() == self.expected, 'probe_checkpoint_append_rollback_or_substitution')

    def raw(self, name: str) -> bytes:
        if not self.journal.has(name):
            raise Unavailable('missing_original:' + name)
        return self.journal.read(name)

    def obj(self, name: str) -> dict[str, Any]:
        result = process.strict_json_loads(self.raw(name))
        require(type(result) is dict, 'original_object_required:' + name)
        return result

    def position(self, name: str) -> int:
        self.raw(name)
        return self.journal._chain.position(name)

    def order(self, *names: str) -> None:
        positions = [self.position(name) for name in names]
        require(all(a < b for a, b in zip(positions, positions[1:])), 'original_probe_chronology_differs')

    def blob(self, name: str) -> bytes:
        if self.journal.has(name):
            require(not self.journal.has(name + '-chunks.json'), 'ambiguous_original_blob')
            return self.raw(name)
        value = self.obj(name + '-chunks.json')
        require(set(value) == {'bytes', 'sha256', 'parts'} and type(value['parts']) is list
                and 2 <= len(value['parts']) <= 3, 'original_chunk_roster_differs')
        parts = []
        for index, row in enumerate(value['parts']):
            require(type(row) is dict and set(row) == {'name', 'bytes', 'sha256'}
                    and row['name'] == name + '-part-' + str(index), 'original_chunk_identity_differs')
            raw = self.raw(row['name'])
            require(type(row['bytes']) is int and len(raw) == row['bytes'] <= transport.CHUNK_BYTES
                    and transport.sha(raw) == row['sha256'], 'original_chunk_bytes_differ')
            parts.append(raw)
            self.order(row['name'], name + '-chunks.json')
        result = b''.join(parts)
        require(type(value['bytes']) is int and len(result) == value['bytes']
                and transport.CHUNK_BYTES < len(result) <= storage.MAX_CAPTURE_BYTES
                and transport.sha(result) == value['sha256'], 'original_blob_bound_or_digest_differs')
        return result

    def command(self, label: str, arguments: list[str], limit: int = storage.MAX_STREAM_BYTES) -> bytes:
        dispatch, record = self.obj(label + '-dispatch.json'), self.obj(label + '.json')
        argv = self.docker + arguments[1:]
        same(dispatch, {'argv': argv, 'limit': limit}, 'original_command_dispatch_differs:' + label)
        same(record.get('arguments'), argv, 'original_command_arguments_differ:' + label)
        same(record.get('argv'), argv, 'original_command_argv_differs:' + label)
        require(type(record.get('exit_code')) is int and type(record.get('timed_out')) is bool
                and type(record.get('capture_complete')) is bool, 'original_control_completion_types_differ')
        output = {}
        for kind in ('stdout', 'stderr'):
            row = record.get(kind)
            require(type(row) is dict and row.get('path') == label + '-' + kind + '.bin', 'command_stream_path_differs')
            assert isinstance(row, dict)
            raw = self.blob(row['path'])
            require(type(row.get('bytes')) is int and row['bytes'] == len(raw) <= limit
                    and row.get('sha256') == transport.sha(raw)
                    and type(row.get('observed_bytes')) is int and row['observed_bytes'] >= len(raw)
                    and type(row.get('truncated')) is bool
                    and row['truncated'] == (row['observed_bytes'] > len(raw)), 'command_stream_descriptor_differs')
            self.order(label + '-dispatch.json', row['path'] if self.journal.has(row['path']) else row['path'] + '-chunks.json', label + '.json')
            if row['truncated']:
                raise Unavailable('incomplete_command_stream:' + label)
            output[kind] = raw
        if not storage._clean(record):
            raise Unavailable('incomplete_control:' + label)
        return output['stdout']

    def engine(self, label: str, path: str) -> dict[str, Any]:
        require(self.raw(label + '-request.bin') == process._request('GET', path), 'original_engine_request_differs')
        retained = prestart._RetainedRuntimeWire(self.raw(label + '-response.bin'))
        status, headers = retained.headers()
        body = retained.body(status, headers)
        self.order(label + '-request.bin', label + '-response.bin')
        if status != 200:
            raise Unavailable('engine_response_failed:' + label)
        result = process.strict_json_loads(body)
        require(type(result) is dict, 'original_engine_object_required')
        return result

    def runtime_at(self, label: str) -> None:
        version = self.engine(label + '-version', '/version')
        info = self.engine(label + '-info', '/info')
        image = self.engine(label + '-image', '/images/' + self.image + '/json')
        def api(value: Any) -> tuple[int, int]:
            require(type(value) is str and re.fullmatch(r'[0-9]+\.[0-9]+', value) is not None, 'original_engine_api_invalid')
            first, second = value.split('.')
            return int(first), int(second)
        require(api(version.get('MinAPIVersion')) <= api(process.API_VERSION) <= api(version.get('ApiVersion'))
                and version.get('Os') == info.get('OSType') == image.get('Os') == 'linux'
                and image.get('Id') == self.image and type(info.get('ID')) is str and bool(info['ID'])
                and type(version.get('GitCommit')) is str and re.fullmatch(r'[0-9a-f]{7,40}', version['GitCommit']) is not None
                and type(info.get('OomKillDisable')) is bool and info.get('CgroupVersion') in ('1', '2')
                and type(info.get('CgroupDriver')) is str and bool(info['CgroupDriver']), 'original_runtime_invalid')
        value = {'protocol': process.PROTOCOL, 'endpoint': self.runtime['endpoint'], 'api_version': process.API_VERSION,
            'os': version['Os'], 'engine_git_commit': version['GitCommit'], 'cgroup_version': info['CgroupVersion'],
            'cgroup_driver': info['CgroupDriver'], 'oom_kill_disable_supported': info['OomKillDisable'],
            'daemon_id': info['ID'], 'engine_version': version.get('Version'), 'architecture': version.get('Arch'),
            'kernel_version': version.get('KernelVersion'), 'image_id': self.image,
            'image_inspect_sha256': process._sha(process._encoded(image))}
        same(value, self.runtime, 'original_runtime_changed')
        same(self.obj(label + '.json'), value, 'runtime_projection_differs')
        same(self.obj(label + '-verified.json'), {'runtime_sha256': values.digest(value)}, 'runtime_verification_differs')
        self.order(label + '-version-request.bin', label + '-version-response.bin', label + '-info-request.bin',
                   label + '-info-response.bin', label + '-image-request.bin', label + '-image-response.bin',
                   label + '.json', label + '-verified.json')

    def identity(self, label: str, *, completed: bool = False) -> None:
        current = self.engine(label + '-container', '/containers/' + self.container_id + '/json')
        prestart.validate_continuity(self.created, current, self.runtime, previous_running=self.running)
        self.running = current
        if self.exec_id is None:
            ids = current.get('ExecIDs')
            require(type(ids) is list and len(ids) == 1, 'unambiguous_original_probe_exec_required')
            assert isinstance(ids, list)
            plans.registry.sha256(ids[0]); self.exec_id = ids[0]
        assert self.exec_id is not None
        value = self.engine(label + '-exec', '/exec/' + self.exec_id + '/json')
        self.exec_pid = execution.exec_identity(value, container_id=self.container_id, exec_id=self.exec_id,
                                               pid=self.exec_pid, completed=completed)
        same(self.obj(label + '-identity-verified.json'), {'exec_id': self.exec_id, 'pid': self.exec_pid,
            'completed': completed, 'value_sha256': values.digest(value)}, 'original_exec_projection_differs')
        self.order(label + '-container-response.bin', label + '-exec-request.bin', label + '-exec-response.bin',
                   label + '-identity-verified.json')

    def establish(self) -> None:
        binding = self.owner.binding
        expected = {'protocol': state.PROTOCOL, 'binding_sha256': values.digest(asdict(binding)),
            'registration': asdict(self.owner.registration), 'plan_sha256': binding.plan_sha256,
            'window': asdict(binding.window), 'status': 'intent_recorded', 'candidate_dispatched': False,
            'execution_authority': False, 'acceptance_authority': False}
        same(self.obj('intent.json'), expected, 'original_state_intent_differs')
        intent = self.obj('physical-intent.json'); identifier = intent.get('execution_id')
        require(type(identifier) is str and re.fullmatch(r'probe-[0-9a-f]{32}', identifier) is not None,
                'original_execution_id_invalid')
        assert isinstance(identifier, str)
        same({k:v for k,v in intent.items() if k != 'cleanup_root'},
            {'protocol': execution.PROTOCOL, 'execution_id': identifier, 'container': 'gossip-' + identifier,
             'volume': 'gossip-volume-' + identifier, 'binding_sha256': values.digest(asdict(binding)),
             'state_intent_sha256': transport.sha(self.raw('intent.json')), 'environment': self.environment},
            'original_physical_intent_differs')
        require(type(intent.get('cleanup_root')) is str and Path(intent['cleanup_root']).is_absolute(), 'original_cleanup_root_required')
        self.intent = intent
        source = plans.verify_current_source(self.owner.store, self.owner.plan)
        helpers = driver.adapter_files(self.record['probe'], released_requirements=tuple(self.record['released_requirements']))
        self.staging = {'source_manifest': admission.source_manifest(source), 'helper_manifest': admission.source_manifest(helpers)}
        staged = self.obj('staging.json')
        same(staged.get('proof'), self.staging, 'original_staged_source_differs')
        require(set(staged) == {'workspace', 'checks', 'proof'} and all(type(staged[k]) is str
                and Path(staged[k]).is_absolute() for k in ('workspace', 'checks')), 'original_mount_roots_required')
        self.runtime_at('runtime')
        name, volume = intent['container'], intent['volume']
        require(not self.command('volume-before', ['docker','volume','ls','--quiet','--filter','name=^'+volume+'$']).strip()
                and not self.command('container-before', ['docker','container','ls','--all','--quiet','--filter','name=^/'+name+'$']).strip(),
                'original_resource_name_preexists')
        argv = ['docker','volume','create','--driver','local','--label','gossip.execution='+identifier,
                '--label','gossip.snapshot='+storage.SNAPSHOT_PROTOCOL]
        for k,v in storage.VOLUME_OPTIONS.items():argv.extend(('--opt',k+'='+v))
        argv.append(volume)
        require(self.command('volume-create',argv).strip() == volume.encode(), 'original_volume_create_differs')
        volume_value = process.strict_json_loads(self.command('volume-created',['docker','volume','inspect','--format','{{json .}}',volume]))
        require(storage._volume_valid(volume_value,volume,identifier), 'original_volume_ownership_differs')
        labels = {'gossip.execution':identifier,'gossip.source':binding.source_sha256,
                  'gossip.fixture':values.digest(admission.source_manifest(helpers))}
        sandbox = DockerValidator(self.image,{k:v.decode('utf-8') for k,v in helpers.items()},command=prestart.COMMAND)
        argv = storage._start_arguments(sandbox,name,Path(staged['workspace']),Path(staged['checks']),volume)
        argv[1]='create';argv.remove('--detach');index=argv.index('--entrypoint')
        for k,v in labels.items():argv[index:index]=['--label',k+'='+v];index+=2
        self.container_id=self.command('container-create',argv).strip().decode('ascii');plans.registry.sha256(self.container_id)
        raw=self.command('container-prestart',['docker','inspect','--format','{{json .}}',self.container_id],process.CONTROL_LIMIT)
        self.created=process.strict_json_loads(raw)
        proof=prestart.proof_for(raw,container_id=self.container_id,name=name,image_id=self.image,volume=volume,
            labels=labels,mounts={'/workspace':staged['workspace'],'/checks':staged['checks']},runtime=self.runtime,
            runtime_originals={n:self.raw(n) for n in prestart.RUNTIME_ORIGINAL_NAMES})
        same(self.obj(prestart.PROOF_FILE),proof,'original_prestart_proof_differs')
        self.command('container-start',['docker','start',self.container_id])
        same(self.obj('session-dispatch.json'),{'argv':self.docker+['exec','--interactive','--user','65534:65534',
             self.container_id,'python','-I','-B','/checks/child_driver.py']},'original_probe_session_differs')
        prestart.validate_order({n:self.position(n) for n in prestart.ORDER})
        self.order('config.json','intent.json','physical-intent.json','staging.json','runtime-verified.json',
                   'volume-before-dispatch.json','container-before.json','volume-create-dispatch.json',
                   'volume-create.json','volume-created-dispatch.json')

    def capture(self) -> Any:
        layout=self.owner.plan.layout
        require(type(layout) is plans.CaptureLayout,'reviewed_capture_layout_required')
        self.runtime_at('capture-runtime-before');self.identity('capture-before')
        self.command('capture-pause',['docker','pause',self.container_id])
        paused=process.strict_json_loads(self.command('capture-state',['docker','inspect','--format','{{json .}}',self.container_id]))
        require(storage._paused(paused,self.intent['volume'],self.image),'original_paused_capture_required')
        prestart.validate_continuity(self.created,paused,self.runtime,previous_running=self.running)
        captured=self.command('capture-tar',['docker','cp',self.container_id+':/tmp','-'],storage.MAX_CAPTURE_BYTES)
        value=sqlite_observation.sqlite_job_value(storage.parse_capture(captured),layout,values.JOB)
        self.command('capture-unpause',['docker','unpause',self.container_id])
        self.runtime_at('capture-runtime-after');self.identity('capture-after')
        same(self.obj('capture-staging.json'),self.staging,'original_capture_staging_differs')
        same(self.obj('captured-job.json'),value,'original_captured_job_projection_differs')
        self.order('capture-job-staging.json','capture-runtime-before-version-request.bin','capture-runtime-before-verified.json',
            'capture-before-container-request.bin','capture-before-identity-verified.json','capture-pause-dispatch.json',
            'capture-pause.json','capture-state-dispatch.json','capture-state.json','capture-tar-dispatch.json','capture-tar.json',
            'capture-unpause-dispatch.json','capture-unpause.json','capture-runtime-after-version-request.bin',
            'capture-runtime-after-verified.json','capture-after-container-request.bin','capture-after-identity-verified.json',
            'capture-staging.json','captured-job.json')
        return value

    def frame(self, slot: str, prior: str) -> bytes:
        slot=pipes.artifact_slot(slot)
        name='probe-frame-'+slot+'.bin';raw=self.blob(name)
        self.runtime_at(slot+'-runtime');self.identity(slot)
        same(self.obj(slot+'-staging.json'),self.staging,'original_frame_staging_differs')
        self.order(prior,name,slot+'-runtime-version-request.bin',slot+'-runtime-verified.json',
                   slot+'-container-request.bin',slot+'-identity-verified.json',slot+'-staging.json')
        return raw

    def acknowledge(self, slot: str) -> str:
        artifact=pipes.artifact_slot(slot)
        label='probe-continue-'+artifact;raw=self.raw(label+'-intent.bin')
        require(raw==wire.continuation_bytes(slot),'original_continuation_differs')
        row=self.obj(label+'-written.json')
        require(set(row)=={'slot','requested_bytes','written_bytes'} and row['slot']==slot
                and type(row['requested_bytes']) is int and row['requested_bytes']==len(raw)
                and type(row['written_bytes']) is int and 0<=row['written_bytes']<=len(raw),'original_continuation_count_differs')
        self.order('captured-job.json' if slot=='capture_job' else artifact+'-staging.json',label+'-intent.bin',label+'-written.json')
        if row['written_bytes']!=len(raw):raise Unavailable('partial_continuation:'+slot)
        return label+'-written.json'

    def finish(self, transcript: wire.ValueTranscript, frames: list[bytes], slots: list[str]) -> None:
        terminal=self.obj('probe-pipe-terminal.json')
        same(terminal.get('value'),transcript.result(),'original_value_projection_differs')
        require(terminal.get('protocol')==pipes.PROTOCOL,'original_pipe_protocol_differs')
        for kind in ('stdout','stderr'):
            raw=self.blob('probe-'+kind+'.bin');row=terminal.get('streams',{}).get(kind)
            require(type(row) is dict and type(row.get('bytes')) is int and row['bytes']==len(raw)
                    and row.get('sha256')==transport.sha(raw) and type(row.get('observed_bytes')) is int
                    and row['observed_bytes']>=len(raw) and type(row.get('truncated')) is bool
                    and row['truncated']==(row['observed_bytes']>len(raw)), 'original_probe_stream_descriptor_differs')
            limit=self.owner.plan.policy.wire_limits.stream_bytes if kind=='stdout' else self.owner.plan.policy.stderr_bytes
            require(len(raw)<=limit,'original_stream_limit_exceeded')
            if row['truncated']:raise Unavailable('truncated_probe_stream')
            if kind=='stdout':require(raw==b''.join(frames),'original_stdout_and_frames_differ')
            self.order('probe-'+kind+'.bin','probe-pipe-terminal.json')
        same(terminal.get('acks'),[{'slot':slot,'requested_bytes':len(wire.continuation_bytes(slot)),
             'written_bytes':len(wire.continuation_bytes(slot))} for slot in slots],'original_ack_summary_differs')
        if not (terminal.get('natural_exit') is True and terminal.get('mechanics_complete') is True
                and type(terminal.get('exit_code')) is int and terminal['exit_code']==0
                and terminal.get('local_pipes_closed') is True and terminal.get('infrastructure')==[]):
            raise Unavailable('original_pipe_incomplete')
        self.identity('session-final',completed=True);self.runtime_at('runtime-final')
        same(self.obj('staging-final.json'),self.staging,'original_final_staging_differs')
        self.order('probe-continue-finished-written.json','probe-stdout.bin','probe-stderr.bin','probe-pipe-terminal.json',
                   'session-final-container-request.bin','session-final-identity-verified.json',
                   'runtime-final-version-request.bin','runtime-final-verified.json','staging-final.json')
        self.command('container-remove',['docker','rm','--force',self.container_id])
        require(not self.command('container-after',['docker','container','ls','--all','--quiet','--filter',
                'name=^/'+self.intent['container']+'$']).strip(),'original_container_absence_unproven')
        volume=self.intent['volume']
        inspected=process.strict_json_loads(self.command('volume-cleanup-inspect',['docker','volume','inspect','--format','{{json .}}',volume]))
        require(storage._volume_valid(inspected,volume,self.intent['execution_id']),'original_cleanup_volume_ownership_differs')
        self.command('volume-remove',['docker','volume','rm',volume])
        require(not self.command('volume-after',['docker','volume','ls','--quiet','--filter','name=^'+volume+'$']).strip(),
                'original_volume_absence_unproven')
        final=self.obj('physical-terminal.json')
        same(final.get('pipe_result'),terminal,'original_terminal_pipe_copy_differs')
        require(final.get('protocol')==execution.PROTOCOL and final.get('execution_id')==self.intent['execution_id']
                and final.get('acceptance_authority') is False and final.get('cold_reconstruction_supplied') is False,
                'original_terminal_identity_differs')
        if not (final.get('qualified_execution_originals') is True and final.get('container_cleanup') is True
                and final.get('volume_cleanup') is True and final.get('infrastructure')==[]):
            raise Unavailable('original_physical_completion_unavailable')
        self.order('staging-final.json','container-remove-dispatch.json','container-remove.json','container-after-dispatch.json',
            'container-after.json','volume-cleanup-inspect-dispatch.json','volume-cleanup-inspect.json','volume-remove-dispatch.json',
            'volume-remove.json','volume-after-dispatch.json','volume-after.json','physical-terminal.json')


def reconstruct(owner: state.ProbeExecutionState, *, expected: chain.PrefixCommitment) -> dict[str, Any]:
    """Read a complete or interrupted exact prefix; never execute or retain data.

    Known semantic failures survive a missing later lifecycle record. Missing
    evidence prevents a positive observation. Chain uncertainty and contradictory
    identities escape as errors, so neither can be converted into a useful fact.
    """
    source_pins=sources();reader=_Reader(owner,expected)
    transcript=wire.ValueTranscript(reader.record['probe'],released_requirements=tuple(reader.record['released_requirements']),
                                   limits=owner.plan.policy.wire_limits)
    frames: list[bytes]=[];slots: list[str]=[];limitations: list[str]=[];complete=False
    try:
        reader.establish();prior='session-dispatch.json'
        while (slot:=transcript.next_slot) is not None:
            raw=reader.frame(slot,prior)
            try:transcript.append(raw)
            except ValueError as error:raise Unavailable('invalid_probe_frame:'+str(error)) from error
            frames.append(raw);slots.append(slot)
            if slot=='capture_job':transcript.supply_captured_value(reader.capture())
            prior=reader.acknowledge(slot)
        reader.finish(transcript,frames,slots);complete=True
    except Unavailable as error:
        limitations.append(str(error))
    # Revalidate the complete prefix and declaration capabilities after reads;
    # no active window or live socket is needed and no source is executed.
    owner._validate_current(check_window=False);reader.checkpoint()
    require(sources()==source_pins,'probe_reader_sources_changed')
    result=transcript.result()
    disposition=result['disposition'] if result['disposition']=='fail' or complete else 'unavailable'
    return {'protocol':PROTOCOL,'binding_sha256':values.digest(asdict(owner.binding)),
        'checkpoint_sha256':values.digest(asdict(expected)),'reader_sources':source_pins,
        'value':result,'disposition':disposition,'mechanics_complete':complete,
        'qualified_execution_originals':complete,'limitations':limitations,
        'physically_executed_by_reader':False,'independent_acceptance':False,'acceptance_authority':False,
        'scope':'public generated-probe observation only; controller enrollment, scientific independence and product selection remain external'}
