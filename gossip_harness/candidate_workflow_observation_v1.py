"""Original workflow bytes and source-qualified captures; no candidate-owned verdicts.

A complete earlier call remains independently assessable after a later missing
response. All chain uncertainty is fatal to attribution. Physical lifecycle facts
are independent from values returned by solve and never manufactured by traces.
"""
from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
import subprocess
from typing import Any

from . import candidate_workflow_execution_v1 as execution
from . import candidate_workflow_profile_v1 as profile
from . import candidate_workflow_review_v1 as review
from . import candidate_observation_admission_v1 as admission
from . import candidate_source_capture_policy_v1 as capture_policy
from . import candidate_intake_store_observer_v1 as storage
from . import project_acceptance_registry_v1 as registry
from .candidate_scope_consumer_v1 import AuthorityError, AuthorityUnavailable
from .gitstore import GitError
from . import candidate_storage_prestart_v1 as prestart

PROTOCOL = 'candidate-workflow-observation-v1'
VERIFIER_FILE = 'workflow-verifier.json'


def require(ok: bool, message: str) -> None:
    if not ok:
        raise AuthorityError(message)


def _json(raw: bytes) -> Any:
    return execution.process.strict_json_loads(raw)


def _required(owner: Any, names: tuple[str, ...]) -> None:
    # Absence outside a healthy acknowledged prefix is operationally unavailable.
    # An acknowledged member with corrupted/missing bytes still takes the chain's
    # fatal authenticated read path. Never catch ChainUnknown as missing evidence.
    if not all(owner.has_retained(name) for name in names):
        raise AuthorityUnavailable('Original evidence absent: ' + ','.join(names))


def _command(owner: execution.CandidateWorkflowExecution, label: str,
             arguments: list[str]) -> tuple[dict[str, Any], bytes] | None:
    if not owner.has_retained(label + '.json'):
        return None
    _required(owner, (label + '-dispatch.json',))
    record = _json(owner.read_authenticated(label + '.json'))
    dispatch = _json(owner.read_authenticated(label + '-dispatch.json'))
    expected = owner.docker + arguments[1:]
    require(type(record) is dict and record.get('arguments') == expected and record.get('argv') == expected
        and type(dispatch) is dict and set(dispatch) == {'argv', 'limit'} and dispatch['argv'] == expected
        and type(dispatch['limit']) is int and 0 < dispatch['limit'] <= execution.b02.MAX_CAPTURE_BYTES,
        'Original control command identity differs')
    for kind in ('stdout', 'stderr'):
        item = record.get(kind)
        require(type(item) is dict and item.get('path') == label + '-' + kind + '.bin', 'Original stream path differs')
        _required(owner, (item['path'],) if owner.has_retained(item['path']) else (item['path'] + '-chunks.json',))
        raw = owner.read_blob(item['path'])
        require(item.get('sha256') == execution.sha(raw) and item.get('bytes') == len(raw)
            and type(item.get('observed_bytes')) is int and item['observed_bytes'] >= len(raw), 'Original stream bytes differ')
    if not execution.b01._clean(record) or any(record[kind]['observed_bytes'] != record[kind]['bytes'] for kind in ('stdout', 'stderr')):
        return None
    return record, owner.read_blob(record['stdout']['path'])


def _engine(owner: Any, label: str, path: str) -> dict[str, Any]:
    _required(owner, (label + '-request.bin', label + '-response.bin'))
    require(owner.read_authenticated(label + '-request.bin') == execution.process._request('GET', path),
        'Original Engine request identity differs')
    wire = prestart._RetainedRuntimeWire(owner.read_authenticated(label + '-response.bin'))
    status, headers = wire.headers()
    raw = wire.body(status, headers)
    if status != 200:
        raise AuthorityUnavailable('Original Engine response failed')
    value = _json(raw)
    require(type(value) is dict, 'Original Engine object required')
    return value


def _exec(owner: Any, label: str, container_id: str, baseline: dict[str, Any] | None,
          *, completed: bool = False) -> dict[str, Any]:
    if baseline is None:
        census = _engine(owner, label + '-exec-container', '/containers/' + container_id + '/json')
        ids = census.get('ExecIDs')
        if not (census.get('Id') == container_id and type(ids) is list and len(ids) == 1):
            raise AuthorityUnavailable('Unambiguous live solve ExecID unavailable')
        execution.registry.sha256(ids[0])
        exec_id, pid = ids[0], None
    else:
        exec_id, pid = baseline['exec_id'], baseline['pid']
    value = _engine(owner, label + '-exec', '/exec/' + exec_id + '/json')
    try:
        actual_pid = execution.exec_identity(value, container_id=container_id, exec_id=exec_id, pid=pid, completed=completed)
    except execution.ExecutionError as error:
        raise AuthorityUnavailable(str(error)) from error
    _required(owner, (label + '-exec-verified.json',))
    verified = {'exec_id': exec_id, 'pid': actual_pid, 'completed': completed, 'value_sha256': execution.digest(value)}
    require(_json(owner.read_authenticated(label + '-exec-verified.json')) == verified, 'Original solve identity projection differs')
    return {**verified, 'exit_code': value.get('ExitCode')}



def _capture_guard(owner: Any, label: str) -> None:
    expected = {'source_manifest': admission.source_manifest(owner.files),
        'helper_manifest': admission.source_manifest(owner._helpers()),
        'fixtures_sha256': execution.digest(admission.source_manifest(owner._inputs()))}
    positions: list[int] = []
    for side in ('before', 'after'):
        runtime = label + '-runtime-' + side + '-verified.json'
        staged = label + '-staging-' + side + '.json'
        _required(owner, (runtime, staged))
        require(_json(owner.read_authenticated(runtime)) == {
            'runtime': owner.runtime, 'runtime_sha256': execution.digest(owner.runtime)}
            and _json(owner.read_authenticated(staged)) == expected,
            'Original capture source/runtime boundary differs')
        positions.extend((owner.authenticated_position(runtime), owner.authenticated_position(staged)))
    require(all(a < b for a, b in zip(positions, positions[1:])), 'Capture source/runtime guard chronology differs')

def _capture(owner: Any, label: str, container_id: str, name: str, volume: str,
             created: dict[str, Any], previous: dict[str, Any] | None) -> tuple[dict[str, bytes], dict[str, Any]]:
    _capture_guard(owner, label)
    pause = _command(owner, label + '-pause', ['docker', 'pause', container_id])
    state = _command(owner, label + '-state', ['docker', 'inspect', '--format', '{{json .}}', container_id])
    copied = _command(owner, label + '-capture', ['docker', 'cp', container_id + ':/tmp', '-'])
    unpause = _command(owner, label + '-unpause', ['docker', 'unpause', container_id])
    if any(row is None for row in (pause, state, copied, unpause)):
        raise AuthorityUnavailable('Complete pause/inspection/copy/resume unavailable')
    assert state is not None and copied is not None
    actual = _json(state[1])
    if not (execution.b01._paused(actual, volume, owner.policy.image_id)
        and actual.get('Id') == container_id and actual.get('Name') == '/' + name):
        raise AuthorityUnavailable('Exact paused owner unavailable')
    try:
        prestart.validate_continuity(created, actual, owner.runtime, previous_running=previous)
        files = execution.b02.parse_capture(copied[1])
    except (prestart.PrestartError, execution.b02.DriverError, execution.process.ProcessError) as error:
        raise AuthorityUnavailable(str(error)) from error
    order = [owner.authenticated_position(label + suffix) for suffix in
        ('-staging-before.json', '-pause.json', '-state.json', '-capture.json', '-unpause.json', '-runtime-after-verified.json')]
    require(all(a < b for a, b in zip(order, order[1:])), 'Original capture chronology differs')
    return files, actual


def captured_state(files: dict[str, bytes], paths: dict[str, Any], plan: review.WorkflowSourcePlan) -> dict[str, Any]:
    """Read complete reviewed SQLite tables; no source-provided SQL executes.

    Active sidecars cannot be guessed away. Captures at the provisional hook
    may be ineligible for committed-state mapping and remain diagnostic only.
    """
    root, database = paths.get('root'), paths.get('database')
    if not (type(root) is str and type(database) is str and database.startswith(root + '/')):
        raise AuthorityUnavailable('Source-qualified confined root/database unavailable')
    prefix = root.removeprefix('/tmp/') + '/'
    local = {name[len(prefix):]: raw for name, raw in files.items() if name.startswith(prefix)}
    relative = database[len(root) + 1:]
    if relative not in plan.storage_paths or any(name not in local for name in plan.storage_paths):
        raise AuthorityUnavailable('Complete enrolled storage inventory unavailable')
    if any(name.startswith(relative + '-') for name in local):
        raise AuthorityUnavailable('Active SQLite sidecars need a separately qualified coherent mapping')
    try:
        rows, schema, auxiliary = storage._sqlite(local[relative])
        if schema != plan.schema_sha256:
            raise AuthorityUnavailable('Exact independently reviewed SQLite schema differs')
        data, persisted = storage._normalize(*rows)
    except storage.ObservationUnavailable as error:
        raise AuthorityUnavailable(str(error)) from error
    return {'data': data, 'persisted_strings': persisted, 'auxiliary_tables': auxiliary,
        'schema_sha256': schema, 'storage_manifest': admission.source_manifest({name: local[name] for name in plan.storage_paths}),
        'complete_root_manifest': admission.source_manifest(local) if local else [], 'paths': paths}



def selected_captures(plan: review.WorkflowSourcePlan, facts: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    """Only enrolled exact call/boundary/occurrence selects semantic snapshots.

    Missing or incoherent mapped state cannot grant a capture selector a value.
    An earlier independently captured discrepancy survives every absent later row.
    """
    result: dict[int, dict[str, Any]] = {}
    for point in plan.capture_points:
        rows = facts[point.call_index]['boundary_facts'] if point.call_index < len(facts) else []
        found = [row for row in rows if row['event']['boundary'] == point.boundary_id
            and row['event']['occurrence'] == point.occurrence]
        require(len(found) <= 1, 'Ambiguous exact source-qualified capture point')
        if len(found) == 1 and found[0]['storage'] is not None:
            result.setdefault(point.call_index, {})[point.role] = found[0]['storage']
    return result

def reconstruct(owner: execution.CandidateWorkflowExecution) -> dict[str, Any]:
    require(type(owner) is execution.CandidateWorkflowExecution, 'Exact product workflow owner required')
    before = owner.checkpoint()
    _required(owner, ('intent.json',))
    intent_raw = owner.read_authenticated('intent.json')
    intent = _json(intent_raw)
    freeze = owner.retained_freeze()
    owner.current(freeze)
    require(intent['protocol'] == owner.binding.protocol
        and execution.encoded(intent['registration']) == execution.encoded(asdict(owner.observation_registration))
        and execution.encoded(intent['original_binding']) == execution.encoded(asdict(owner.binding))
        and intent['source_sha256'] == owner.binding.source_sha256
        and intent['ordered_phases'] == list(owner.profile.phases), 'Original workflow intent differs')
    terminal = None if not owner.has_retained('terminal.json') else _json(owner.read_authenticated('terminal.json'))
    if terminal is not None:
        require(terminal['protocol'] == owner.binding.protocol and terminal['intent_sha256'] == execution.sha(intent_raw)
            and terminal['source_sha256'] == owner.binding.source_sha256
            and terminal['native_source_sha256'] == owner.binding.native_source_sha256
            and terminal['execution_id'] == intent['execution_id'] and terminal['case_id'] == owner.binding.case_id
            and terminal['family'] == owner.binding.family, 'Original workflow terminal identity differs')
    responses: dict[int, Any] = {}
    unavailable: list[str] = []
    phase_facts: list[dict[str, Any]] = []
    name, volume = intent['container'], intent['volume']
    helpers = execution.adapter_files(owner.binding.case_id, owner.plan)
    fixtures = profile.input_files(owner.binding.case_id)
    require(execution.fixture_identity(owner.binding.case_id, owner.plan) == owner.binding.fixture_sha256,
        'Actual original helper/fixture bytes differ')
    expected_stage = {'source_manifest': admission.source_manifest(owner.files),
        'helper_manifest': admission.source_manifest(helpers),
        'fixtures_sha256': execution.digest(admission.source_manifest(fixtures))}
    expected_labels = {'gossip.execution': intent['execution_id'], 'gossip.source': owner.binding.source_sha256,
        'gossip.fixture': owner.binding.fixture_sha256}
    container_id: str | None = None
    if owner.has_retained('container-create.json'):
        creation = _json(owner.read_authenticated('container-create.json'))
        raw = owner.read_blob('container-create-stdout.bin')
        require(creation['stdout']['sha256'] == execution.sha(raw) and creation['stdout']['bytes'] == len(raw), 'Created identity bytes differ')
        if execution.b01._clean(creation):
            container_id = raw.strip().decode('ascii')
            registry.sha256(container_id)
    prestart_value: dict[str, Any] | None = None
    prestart_error: str | None = None
    try:
        if container_id is None or not all(owner.has_retained(record) for record in prestart.ORDER):
            raise AuthorityUnavailable('Original created-before-start proof or chronology unavailable')
        staging = _json(owner.read_authenticated('staging.json'))
        require(staging.get('proof') == expected_stage and staging.get('source_manifest') == expected_stage['source_manifest'],
                'Original staging plan differs')
        binds = {'/workspace': staging['workspace'], '/checks': staging['checks']}
        binds['/inputs'] = staging['inputs']
        sandbox = execution.DockerValidator(owner.policy.image_id,
            {'workflow_adapter.py': execution.ADAPTER}, command=prestart.COMMAND)
        create_argv = execution.b02._start_arguments(sandbox, name, Path(staging['workspace']), Path(staging['checks']), Path(staging['inputs']), volume)
        create_argv[1] = 'create'
        create_argv.remove('--detach')
        label_index = create_argv.index('--entrypoint')
        for key, val in expected_labels.items():
            create_argv[label_index:label_index] = ['--label', key + '=' + val]
            label_index += 2
        volume_original = _command(owner, 'volume-created', ['docker', 'volume', 'inspect', '--format', '{{json .}}', volume])
        if volume_original is None:
            raise AuthorityUnavailable('Original created volume inspection incomplete')
        require(execution.b01._volume_valid(_json(volume_original[1]), volume, intent['execution_id']),
                'Original owned tmpfs volume options differ')
        creation_original = _command(owner, 'container-create', create_argv)
        inspected = _command(owner, 'container-prestart', ['docker', 'inspect', '--format', '{{json .}}', container_id])
        started = _command(owner, 'container-start', ['docker', 'start', container_id])
        if creation_original is None or inspected is None or started is None:
            raise AuthorityUnavailable('Original create/inspect/start command incomplete')
        require(creation_original[1].strip() == container_id.encode()
            and started[1].strip() == container_id.encode(), 'Original create/start identity differs')
        expected_proof = prestart.proof_for(inspected[1], container_id=container_id, name=name,
            image_id=owner.policy.image_id, volume=volume, labels=expected_labels, mounts=binds,
            runtime=owner.runtime, runtime_originals={name: owner.read_authenticated(name)
                for name in prestart.RUNTIME_ORIGINAL_NAMES if owner.has_retained(name)})
        require(prestart.exact(_json(owner.read_authenticated(prestart.PROOF_FILE)), expected_proof),
                'Retained prestart proof differs from original observed inspection')
        prestart.validate_order({record: owner.authenticated_position(record) for record in prestart.ORDER})
        session_argv = owner.docker + ['exec', '--interactive', '--user', '65534:65534', container_id,
            'python', '-I', '-B', '/checks/workflow_adapter.py']
        require(_json(owner.read_authenticated('session-dispatch.json')) == {'argv': session_argv},
                'Candidate adapter must enter through exact unprivileged exec')
        prestart_value = _json(inspected[1])
    except (AuthorityUnavailable, prestart.PrestartError, execution.process.ProcessError) as error:
        prestart_error = str(error)
        unavailable.append('prestart:' + prestart_error)

    session = None if not owner.has_retained('session.json') else _json(owner.read_authenticated('session.json'))
    session_raw: bytes | None = None
    if session is not None:
        for kind in ('stdout', 'stderr'):
            raw = owner.read_blob('session-' + kind + '.bin')
            item = session[kind]
            require(item['path'] == 'session-' + kind + '.bin' and item['sha256'] == execution.sha(raw)
                and item['bytes'] == len(raw) and type(item['observed_bytes']) is int
                and item['observed_bytes'] >= len(raw), 'Original session bytes differ')
            if kind == 'stdout':
                session_raw = raw
    exec_baseline: dict[str, Any] | None = None
    previous: dict[str, Any] | None = None
    retained_frames: list[bytes] = []
    try:
        if container_id is None or prestart_value is None:
            raise AuthorityUnavailable('Original created/source identity unavailable')
        _required(owner, ('session-ready.bin', 'session-ready-ack.json'))
        ready = owner.read_authenticated('session-ready.bin')
        require(_json(ready) == {'kind': 'ready', 'protocol': execution.ADAPTER_PROTOCOL}, 'Original ready frame differs')
        require(_json(owner.read_authenticated('session-ready-ack.json')) == {'request': 'ready\n'}, 'Ready acknowledgement differs')
        retained_frames.append(ready)
        exec_baseline = _exec(owner, 'session-ready', container_id, None)
        _, previous = _capture(owner, 'session-ready', container_id, name, volume, prestart_value, None)
    except (AuthorityUnavailable, execution.process.ProcessError, prestart.PrestartError) as error:
        unavailable.append('ready:' + str(error))
    for index, phase in enumerate(owner.profile.phases):
        fact: dict[str, Any] = {'phase': phase, 'response_authenticated': False,
            'capture_authenticated': False, 'boundary_facts': [], 'reason': None}
        boundary_rows: list[dict[str, Any]] = []
        try:
            if container_id is None or prestart_value is None or exec_baseline is None:
                raise AuthorityUnavailable('Original solve identity unavailable')
            for boundary in ('before',):
                runtime_name = phase + '-runtime-' + boundary + '-verified.json'
                stage_name = phase + '-staging-' + boundary + '.json'
                _required(owner, (runtime_name, stage_name))
                require(_json(owner.read_authenticated(runtime_name)) == {
                    'runtime': owner.runtime, 'runtime_sha256': execution.digest(owner.runtime)}
                    and _json(owner.read_authenticated(stage_name)) == expected_stage,
                    'Original source/runtime call boundary differs')
            _required(owner, (phase + '-request.json',))
            require(_json(owner.read_authenticated(phase + '-request.json')) == {'request': phase + '\n'},
                'Original call request differs')
            request_position = owner.authenticated_position(phase + '-request.json')
            require(owner.authenticated_position('session-ready-ack.json') < request_position
                and owner.authenticated_position(phase + '-runtime-before-verified.json') < request_position
                and owner.authenticated_position(phase + '-staging-before.json') < request_position,
                'Call precedes admitted session/source boundary')
            if index:
                previous_next = owner.profile.phases[index - 1] + '-next.json'
                _required(owner, (previous_next,))
                require(owner.authenticated_position(previous_next) < request_position,
                    'Later call preceded prior completion handshake')
            seen: dict[str, int] = {}
            carried: dict[str, Any] = {'root': None, 'database': None}
            origins: dict[str, Any] = {'root': None, 'database': None}
            result_seen = False
            last_position = owner.authenticated_position(phase + '-request.json')
            for ordinal in range(execution.MAX_EVENTS + 1):
                frame_name = phase + '-frame-%03d.bin' % ordinal
                _required(owner, (frame_name,))
                raw = owner.read_authenticated(frame_name)
                require(raw.endswith(b'\n') and len(raw) - 1 <= execution.FRAME_BYTES,
                    'Retained frame violates fixed trusted capture bound')
                position = owner.authenticated_position(frame_name)
                require(position > last_position, 'Call frame chronology differs')
                last_position = position
                retained_frames.append(raw)
                try:
                    event = _json(raw)
                except (ValueError, UnicodeError, RecursionError) as error:
                    raise AuthorityUnavailable('Complete attributable result JSON unavailable') from error
                if not (type(event) is dict and event.get('phase') == phase):
                    raise AuthorityUnavailable('Frame not attributable to declared call')
                if event.get('kind') == 'result':
                    require(set(event) == {'kind', 'phase', 'value'}, 'Original closed result fields differ')
                    _required(owner, (phase + '-response.bin',))
                    require(owner.read_authenticated(phase + '-response.bin') == raw,
                        'Original response differs from retained frame')
                    _exec(owner, phase + '-result', container_id, exec_baseline)
                    _capture_guard(owner, phase + '-result')
                    # Grade this complete authenticated call before attempting
                    # later physical captures or terminal mechanics.
                    response_position = owner.authenticated_position(phase + '-response.bin')
                    require(position < response_position
                        < owner.authenticated_position(phase + '-result-exec-request.bin')
                        < owner.authenticated_position(phase + '-result-runtime-after-verified.json')
                        and response_position < owner.authenticated_position(phase + '-result-staging-after.json'),
                        'Original call/source/exec chronology differs')
                    responses[index] = event['value']
                    fact['response_authenticated'] = True
                    result_seen = True
                    _, previous = _capture(owner, phase + '-result', container_id, name, volume, prestart_value, previous)
                    _required(owner, (phase + '-next.json',))
                    require(_json(owner.read_authenticated(phase + '-next.json')) == {'request': 'next:' + phase + '\n'},
                        'Call completion handshake differs')
                    break
                require(event.get('kind') == 'boundary' and set(event) == {
                    'kind', 'phase', 'ordinal', 'boundary', 'occurrence', 'paths', 'path_origins'}
                    and type(event['ordinal']) is int and event['ordinal'] == ordinal,
                    'Original boundary schema differs')
                enrolled = [row for row in owner.plan.boundaries if row.id == event['boundary']]
                require(len(enrolled) == 1, 'Boundary not in enrolled source plan')
                point = enrolled[0]
                occurrence = seen.get(point.id, 0)
                require(type(event['occurrence']) is int and event['occurrence'] == occurrence
                    and occurrence < point.occurrences, 'Original boundary occurrence differs')
                seen[point.id] = occurrence + 1
                require(type(event['paths']) is dict and set(event['paths']) == {'root', 'database'}
                    and type(event['path_origins']) is dict and set(event['path_origins']) == {'root', 'database'},
                    'Closed source-qualified path inventory differs')
                for kind in ('root', 'database'):
                    path = event['paths'][kind]
                    if getattr(point, kind + '_local') is not None:
                        require(path is None or (type(path) is str and path.startswith('/tmp/')
                            and '\\' not in path and '\x00' not in path
                            and all(part not in ('', '.', '..') for part in path.split('/')[1:])),
                            'Unconfined enrolled path')
                        carried[kind], origins[kind] = path, ordinal if path is not None else None
                    require(event['paths'][kind] == carried[kind] and event['path_origins'][kind] == origins[kind],
                        'Carried path differs from its source-qualified origin')
                label = phase + '-boundary-%03d' % ordinal
                _required(owner, (label + '-event.json',))
                require(_json(owner.read_authenticated(label + '-event.json')) == event, 'Boundary event original differs')
                _exec(owner, label, container_id, exec_baseline)
                path_original = _command(owner, label + '-paths', ['docker', 'exec', '--user', '65534:65534', container_id,
                    'python', '-I', '-B', '/checks/workflow_paths.py', execution.encoded(event['paths']).decode('ascii')])
                if path_original is None:
                    raise AuthorityUnavailable('Owned read-only path inspection incomplete')
                _required(owner, (label + '-path-facts.json',))
                require(owner.read_authenticated(label + '-path-facts.json') == path_original[1], 'Path facts differ from actual helper output')
                path_facts = _json(path_original[1])
                require(type(path_facts) is dict and set(path_facts) == {'root', 'database'}, 'Path facts inventory differs')
                captured, previous = _capture(owner, label, container_id, name, volume, prestart_value, previous)
                row: dict[str, Any] = {'event': event, 'path_facts': path_facts, 'meaning': point.meaning,
                    'capture_manifest': admission.source_manifest(captured) if captured else [],
                    'storage': None, 'storage_unavailable': None}
                try:
                    row['storage'] = captured_state(captured, event['paths'], owner.plan)
                except AuthorityUnavailable as error:
                    row['storage_unavailable'] = str(error)
                boundary_rows.append(row)
                _required(owner, (phase + '-resume-%03d.json' % ordinal,))
                require(_json(owner.read_authenticated(phase + '-resume-%03d.json' % ordinal)) == {
                    'request': 'resume:' + phase + ':' + str(ordinal) + '\n'}, 'Boundary resume differs')
            if not result_seen:
                raise AuthorityUnavailable('No complete result frame')
            # Missing declared boundaries never invent positive lifecycle proof.
            fact['capture_authenticated'] = all(seen.get(point.id, 0) > 0 for point in owner.plan.boundaries)
            if not fact['capture_authenticated']:
                unavailable.append(phase + ':declared-source-boundary-absent')
        except execution.chain.ChainUnknown:
            raise
        except (AuthorityUnavailable, execution.process.ProcessError, prestart.PrestartError) as error:
            fact['reason'] = str(error)
            unavailable.append(phase + ':' + str(error))
        for side_name in ('runtime-after-verified', 'staging-after'):
            member = phase + '-' + side_name + '.json'
            if not owner.has_retained(member):
                unavailable.append(phase + ':whole-call-after-boundary-unavailable')
            else:
                expected_after = ({'runtime': owner.runtime, 'runtime_sha256': execution.digest(owner.runtime)}
                    if side_name == 'runtime-after-verified' else expected_stage)
                require(_json(owner.read_authenticated(member)) == expected_after, 'Original whole-call after boundary differs')
        fact['boundary_facts'] = boundary_rows
        phase_facts.append(fact)
    if session_raw is not None:
        # In a truncated tail, every acknowledged original frame still has to be
        # the corresponding exact prefix of the actually drained raw stream.
        joined = b''.join(retained_frames)
        require(session_raw.startswith(joined), 'Retained call frames differ from original stream prefix')
    final_exec = None
    try:
        if container_id is not None and exec_baseline is not None:
            final_exec = _exec(owner, 'session-final', container_id, exec_baseline, completed=True)
    except (AuthorityUnavailable, execution.process.ProcessError, prestart.PrestartError) as error:
        unavailable.append('completion:' + str(error))
    completed_session = bool(session is not None and session.get('natural_exit') is True
        and type(session.get('exit_code')) is int and session['exit_code'] == 0
        and session.get('capture_complete') is True and session.get('timed_out') is False and not session.get('errors')
        and session.get('requests') == list(owner.profile.phases)
        and final_exec is not None and final_exec['exit_code'] == 0
        and session_raw == b''.join(retained_frames)
        and all(session[kind].get('truncated') is False and session[kind]['observed_bytes'] == session[kind]['bytes']
            for kind in ('stdout', 'stderr')))
    cleaned = False
    if container_id is not None:
        removed = _command(owner, 'container-remove', ['docker', 'rm', '--force', container_id])
        absent = _command(owner, 'container-after', ['docker', 'container', 'ls', '--all', '--quiet', '--filter', 'name=^/' + name + '$'])
        volume_removed = _command(owner, 'volume-remove', ['docker', 'volume', 'rm', volume])
        volume_absent = _command(owner, 'volume-after', ['docker', 'volume', 'ls', '--quiet', '--filter', 'name=^' + volume + '$'])
        cleaned = bool(removed is not None and removed[1].strip() == container_id.encode() and absent is not None
            and not absent[1].strip() and volume_removed is not None and volume_removed[1].strip() == volume.encode()
            and volume_absent is not None and not volume_absent[1].strip())
    final_stage = owner.has_retained('staging-final.json') and _json(owner.read_authenticated('staging-final.json')) == expected_stage
    mechanics = bool(completed_session and cleaned and final_stage and not unavailable and terminal is not None
        and not terminal['infrastructure'] and all(row['capture_authenticated'] for row in phase_facts))
    projection = profile.project(owner.profile, responses, selected_captures(owner.plan, phase_facts))
    result = {'protocol': PROTOCOL, 'execution_protocol': owner.binding.protocol,
        'execution_id': intent['execution_id'], 'registration': asdict(owner.observation_registration),
        'original_binding': asdict(owner.binding), 'original_intent_sha256': execution.sha(intent_raw),
        'original_terminal_sha256': None if terminal is None else execution.sha(owner.read_authenticated('terminal.json')),
        'physical_receipt_basis': 'terminal' if terminal is not None else 'durable-intent-partial-observations',
        'original_context_sha256': before.context_sha256, 'original_config_sha256': execution.digest(owner.config),
        'source_review_sha256': owner.review_sha256, 'profile': owner.profile.record(),
        'phase_facts': phase_facts, 'projection': projection, 'solve_exec': exec_baseline,
        'mechanics': {'status': 'passed' if mechanics else 'infrastructure_error', 'cleanup_verified': cleaned,
            'session_complete': completed_session, 'prestart_verified': prestart_value is not None, 'unavailable': unavailable},
        'cohort_freeze': None if freeze is None else asdict(freeze),
        'production_scope_authority': False, 'independent_semantic_scope_review_supplied': False,
        'whole_project_acceptance': False}
    owner.current(freeze)
    require(owner.checkpoint() == before, 'Original workflow journal changed during projection')
    return result


def selector_catalog(case_id: str, *, purpose: str) -> dict[str, Any]:
    value = profile.profile_for(case_id, purpose)
    rows = [{**row, 'value_domain': ['pass', 'fail', 'unavailable'],
        'physical_capture_qualified': False, 'whole_source_unit_qualified': False} for row in value.selectors()]
    rows.append({'case_id': execution.mechanics_case_id(value), 'pointer': '/mechanics/status',
        'definition_pointer': '/limits', 'evidence_kind': 'mechanics', 'assertion_kind': 'history',
        'value_domain': ['passed', 'infrastructure_error'], 'source_unit_ids': [], 'source_unit_facets': [],
        'scope': 'Exact owned solve-process/session/source/capture/cleanup only'})
    return {'protocol': PROTOCOL, 'family': value.family, 'history_id': case_id,
        'original_definition_purpose': value.original_definition_purpose, 'execution_purpose': purpose,
        'target_contract_sha256': execution.TARGET_CONTRACT, 'source_contract_sha256': execution.TARGET_CONTRACT,
        'target_milestone': 'M4', 'profile_sha256': value.sha256, 'definition_sha256': execution.digest(value.record()),
        'evaluator_sources': execution.evaluator_sources(),
        'ordered_case_ids': list(value.ordered_case_ids + (execution.mechanics_case_id(value),)),
        'selectors': rows, 'required_unfinished_coverage': list(profile.LIMITATIONS),
        'capabilities': ['public-contract', 'workflow'], 'scope_factory_registered': False, 'semantic_authority': False}


def publish_verifier(owner: execution.CandidateWorkflowExecution) -> execution.chain.PrefixCommitment:
    require(type(owner) is execution.CandidateWorkflowExecution and owner.mode == 'physical', 'Exact physical workflow owner required')
    raw = execution.encoded(reconstruct(owner))
    if owner.has_retained(VERIFIER_FILE):
        require(owner.read_authenticated(VERIFIER_FILE) == raw, 'Retained original verifier differs')
    else:
        owner._retain(VERIFIER_FILE, raw)
    owner.current(owner.retained_freeze())
    return owner.checkpoint()


class WorkflowObservationSource:
    def __init__(self, owner: execution.CandidateWorkflowExecution, expected_checkpoint: execution.chain.PrefixCommitment):
        require(type(owner) is execution.CandidateWorkflowExecution and owner.mode == 'physical'
            and type(expected_checkpoint) is execution.chain.PrefixCommitment, 'Exact physical owner and independent prefix required')
        require(owner.checkpoint() == expected_checkpoint and owner.has_retained(VERIFIER_FILE), 'Original current verifier unavailable')
        self.owner, self.expected_checkpoint = owner, expected_checkpoint

    def observation(self, gate: registry.Gate, freeze: registry.CohortFreeze | None) -> registry.Observation:
        try:
            return self._observation(gate, freeze)
        except (execution.ExecutionUnknown, execution.chain.ChainUnknown, admission.AdmissionUnavailable,
                capture_policy.SourceCaptureUnavailable, OSError, subprocess.SubprocessError, GitError) as error:
            raise AuthorityUnavailable(str(error)) from error
        except AuthorityError:
            raise
        except (ValueError, KeyError, TypeError) as error:
            raise AuthorityError(str(error)) from error

    def _observation(self, gate: registry.Gate, freeze: registry.CohortFreeze | None) -> registry.Observation:
        owner = self.owner
        require(gate == owner.registration.gate and owner.checkpoint() == self.expected_checkpoint,
            'Exact original gate and external prefix required')
        require(owner.retained_freeze() == freeze, 'Original purpose/freeze differs')
        record = reconstruct(owner)
        raw = execution.encoded(record)
        require(owner.read_authenticated(VERIFIER_FILE) == raw, 'Original host verifier differs')
        outcomes = tuple(registry.CaseResult(row['case_id'], {'pass': 'passed', 'fail': 'failed',
            'unavailable': 'infrastructure_error'}[row['disposition']]) for row in record['projection']['observations'])
        outcomes += (registry.CaseResult(execution.mechanics_case_id(owner.profile), record['mechanics']['status']),)
        require(tuple(item.case_id for item in outcomes) == gate.ordered_case_ids, 'Exact decisive roster differs')
        original_receipt = record['original_terminal_sha256'] or record['original_intent_sha256']
        physical = registry.PhysicalExecution(gate.binding, record['execution_id'], original_receipt, execution.sha(raw),
            'completed' if record['mechanics']['status'] == 'passed' else 'infrastructure_error', outcomes,
            None if freeze is None else freeze.receipt_sha256)
        owner.current(freeze)
        require(owner.checkpoint() == self.expected_checkpoint, 'Current original prefix changed')
        return registry.Observation(gate.gate_id, gate.binding, physical)


def inspection_selector_catalog(purpose: str) -> dict[str, Any]:
    value = review.WorkflowInspectionProfile(purpose)
    # The host-inspection profile owns source duty mapping and exact selectors.
    rows = value.selectors()
    return {'protocol': review.INSPECTION_PROTOCOL, 'family': value.family,
        'original_definition_purpose': review.ORIGINAL_DEFINITION_PURPOSE,
        'execution_purpose': purpose, 'source_contract_sha256': execution.TARGET_CONTRACT,
        'target_contract_sha256': execution.TARGET_CONTRACT, 'target_milestone': 'M4',
        'profile_sha256': value.sha256, 'definition_sha256': execution.digest(value.record()),
        'evaluator_sources': review.inspection_evaluator_sources(), 'ordered_case_ids': list(value.ordered_case_ids),
        'selectors': rows, 'capabilities': ['source-inspection'], 'semantic_authority': False}


def publish_inspection_verifier(owner: review.WorkflowInspectionExecution) -> execution.chain.PrefixCommitment:
    require(type(owner) is review.WorkflowInspectionExecution and owner.mode == 'physical', 'Exact host inspection owner required')
    owner.retain_verifier(execution.encoded(owner.read_original()))
    return owner.checkpoint()


class WorkflowInspectionObservationSource:
    def __init__(self, owner: review.WorkflowInspectionExecution, expected_checkpoint: execution.chain.PrefixCommitment):
        require(type(owner) is review.WorkflowInspectionExecution and owner.mode == 'physical'
            and type(expected_checkpoint) is execution.chain.PrefixCommitment, 'Exact host original owner and prefix required')
        require(owner.checkpoint() == expected_checkpoint and owner.has_retained(review.VERIFIER_FILE),
            'Authenticated original host verifier unavailable')
        self.owner, self.expected_checkpoint = owner, expected_checkpoint

    def observation(self, gate: registry.Gate, freeze: registry.CohortFreeze | None) -> registry.Observation:
        owner = self.owner
        require(gate == owner.registration.gate and owner.checkpoint() == self.expected_checkpoint
            and owner.retained_freeze() == freeze, 'Host source gate/prefix/freeze differs')
        original = owner.read_original()
        raw = execution.encoded(original)
        require(owner.read_authenticated(review.VERIFIER_FILE) == raw and original['engine_used'] is False,
            'Host original verifier/producer differs')
        outcomes = tuple(registry.CaseResult(row['case_id'], row['status']) for row in original['outcomes'])
        require(tuple(row.case_id for row in outcomes) == gate.ordered_case_ids, 'Exact source duty census differs')
        physical = registry.PhysicalExecution(gate.binding, original['execution_id'], original['terminal_sha256'],
            execution.sha(raw), 'completed', outcomes, None if freeze is None else freeze.receipt_sha256)
        owner.current()
        require(owner.checkpoint() == self.expected_checkpoint, 'Host original prefix changed')
        return registry.Observation(gate.gate_id, gate.binding, physical)



def _qualification_semantics(control_id: str, raw_stdout: bytes) -> dict[str, Any]:
    """Decode only the two closed, complete authored semantic controls.

    The caller supplies original authenticated session stdout. Over-frame and
    stream-boundary controls deliberately receive no invented semantic value.
    """
    applicable = control_id in ('WQ-NORMALIZED-61824', 'WQ-FRAME-EXACT')
    result: dict[str, Any] = {'applicable': applicable, 'comparison': None,
        'frame_sha256': None, 'normalized_bytes': None, 'normalized_sha256': None,
        'reason': None}
    if not applicable:
        return result
    if not raw_stdout.startswith(execution._READY):
        result['reason'] = 'Original ready/frame boundary unavailable'
        return result
    frame = raw_stdout[len(execution._READY):]
    result['frame_sha256'] = execution.sha(frame)
    if not (frame.endswith(b'\n') and frame.count(b'\n') == 1 and len(frame) - 1 <= execution.FRAME_BYTES):
        result['reason'] = 'One complete bounded result frame unavailable'
        return result
    try:
        event = _json(frame)
        require(type(event) is dict and set(event) == {'kind', 'phase', 'value'}
            and event['kind'] == 'result' and event['phase'] == 'call-000',
            'Closed qualifier result frame differs')
        value = event['value']
        normalized = profile.normalized(value)
        result['normalized_bytes'] = len(normalized)
        result['normalized_sha256'] = execution.sha(normalized)
        if control_id == 'WQ-NORMALIZED-61824':
            result['comparison'] = (type(value) is str and value == 'x' * 61822
                and len(normalized) == 61824)
        else:
            result['comparison'] = type(value) is dict and value == {} and len(frame) - 1 == 131072
    except (ValueError, UnicodeError, RecursionError) as error:
        result['reason'] = type(error).__name__ + ': complete qualifier semantics unavailable'
    return result


def reconstruct_qualification(owner: execution.CandidateWorkflowQualificationExecution) -> dict[str, Any]:
    """Exact harness records only; this returns no Registry product Observation."""
    require(type(owner) is execution.CandidateWorkflowQualificationExecution and owner.mode == 'physical',
        'Exact physical harness qualification owner required')
    before = owner.checkpoint()
    owner.current(owner.retained_freeze())
    intent_raw = owner.read_authenticated('intent.json')
    intent = _json(intent_raw)
    require(intent['protocol'] == execution.QUALIFICATION_PROTOCOL
        and execution.encoded(intent['registration']) == execution.encoded(asdict(owner.observation_registration))
        and execution.encoded(intent['original_binding']) == execution.encoded(asdict(owner.binding))
        and owner.observation_registration.original_definition_purpose == 'harness_qualification',
        'Original separate harness purpose/binding differs')
    control_id = owner.profile.control_id
    require(owner.files == execution.qualification_source_files(control_id), 'Qualifier source bytes differ')
    terminal = None if not owner.has_retained('terminal.json') else _json(owner.read_authenticated('terminal.json'))
    if terminal is not None:
        require(terminal['protocol'] == execution.QUALIFICATION_PROTOCOL
            and terminal['intent_sha256'] == execution.sha(intent_raw) and terminal['execution_id'] == intent['execution_id']
            and terminal['source_sha256'] == owner.binding.source_sha256, 'Qualifier terminal identity differs')
    helpers = execution.qualification_adapter_files(control_id)
    fixtures = execution.qualification_inputs(control_id)
    expected_stage = {'source_manifest': admission.source_manifest(owner.files),
        'helper_manifest': admission.source_manifest(helpers),
        'fixtures_sha256': execution.digest(admission.source_manifest(fixtures))}
    expected_labels = {'gossip.execution': intent['execution_id'], 'gossip.source': owner.binding.source_sha256,
        'gossip.fixture': owner.binding.fixture_sha256}
    unavailable: list[str] = []
    name, volume = intent['container'], intent['volume']
    container_id: str | None = None
    if owner.has_retained('container-create.json'):
        creation = _json(owner.read_authenticated('container-create.json'))
        raw = owner.read_blob('container-create-stdout.bin')
        require(creation['stdout']['sha256'] == execution.sha(raw) and creation['stdout']['bytes'] == len(raw), 'Created identity bytes differ')
        if execution.b01._clean(creation):
            container_id = raw.strip().decode('ascii')
            registry.sha256(container_id)
    prestart_value: dict[str, Any] | None = None
    prestart_error: str | None = None
    try:
        if container_id is None or not all(owner.has_retained(record) for record in prestart.ORDER):
            raise AuthorityUnavailable('Original created-before-start proof or chronology unavailable')
        staging = _json(owner.read_authenticated('staging.json'))
        require(staging.get('proof') == expected_stage and staging.get('source_manifest') == expected_stage['source_manifest'],
                'Original staging plan differs')
        binds = {'/workspace': staging['workspace'], '/checks': staging['checks']}
        binds['/inputs'] = staging['inputs']
        sandbox = execution.DockerValidator(owner.policy.image_id,
            {'workflow_adapter.py': execution.ADAPTER}, command=prestart.COMMAND)
        create_argv = execution.b02._start_arguments(sandbox, name, Path(staging['workspace']), Path(staging['checks']), Path(staging['inputs']), volume)
        create_argv[1] = 'create'
        create_argv.remove('--detach')
        label_index = create_argv.index('--entrypoint')
        for key, val in expected_labels.items():
            create_argv[label_index:label_index] = ['--label', key + '=' + val]
            label_index += 2
        volume_original = _command(owner, 'volume-created', ['docker', 'volume', 'inspect', '--format', '{{json .}}', volume])
        if volume_original is None:
            raise AuthorityUnavailable('Original created volume inspection incomplete')
        require(execution.b01._volume_valid(_json(volume_original[1]), volume, intent['execution_id']),
                'Original owned tmpfs volume options differ')
        creation_original = _command(owner, 'container-create', create_argv)
        inspected = _command(owner, 'container-prestart', ['docker', 'inspect', '--format', '{{json .}}', container_id])
        started = _command(owner, 'container-start', ['docker', 'start', container_id])
        if creation_original is None or inspected is None or started is None:
            raise AuthorityUnavailable('Original create/inspect/start command incomplete')
        require(creation_original[1].strip() == container_id.encode()
            and started[1].strip() == container_id.encode(), 'Original create/start identity differs')
        expected_proof = prestart.proof_for(inspected[1], container_id=container_id, name=name,
            image_id=owner.policy.image_id, volume=volume, labels=expected_labels, mounts=binds,
            runtime=owner.runtime, runtime_originals={name: owner.read_authenticated(name)
                for name in prestart.RUNTIME_ORIGINAL_NAMES if owner.has_retained(name)})
        require(prestart.exact(_json(owner.read_authenticated(prestart.PROOF_FILE)), expected_proof),
                'Retained prestart proof differs from original observed inspection')
        prestart.validate_order({record: owner.authenticated_position(record) for record in prestart.ORDER})
        session_argv = owner.docker + ['exec', '--interactive', '--user', '65534:65534', container_id,
            'python', '-I', '-B', '/checks/workflow_adapter.py']
        require(_json(owner.read_authenticated('session-dispatch.json')) == {'argv': session_argv},
                'Candidate adapter must enter through exact unprivileged exec')
        prestart_value = _json(inspected[1])
    except (AuthorityUnavailable, prestart.PrestartError, execution.process.ProcessError) as error:
        prestart_error = str(error)
        unavailable.append('prestart:' + prestart_error)

    checks: dict[str, bool | None] = {key: None for key in
        ('created_source_identity', 'solve_identity', 'natural_exit', 'stdout_bytes', 'stderr_bytes',
         'framing_disposition', 'source_runtime_boundaries', 'cleanup')}
    checks['created_source_identity'] = prestart_value is not None
    semantics = _qualification_semantics(control_id, b'')
    if semantics['applicable']:
        checks['semantic_reconstruction'] = None
    try:
        if container_id is None or prestart_value is None:
            raise AuthorityUnavailable('Original qualifier sandbox unavailable')
        first = _exec(owner, 'session-ready', container_id, None)
        _capture(owner, 'session-ready', container_id, name, volume, prestart_value, None)
        final = _exec(owner, 'session-final', container_id, first, completed=True)
        checks['solve_identity'] = True
        _required(owner, ('session.json', 'session-ready.bin', 'session-ready-ack.json', 'call-000-request.json'))
        require(owner.read_authenticated('session-ready.bin') == execution._READY
            and _json(owner.read_authenticated('session-ready-ack.json')) == {'request': 'ready\n'}
            and _json(owner.read_authenticated('call-000-request.json')) == {'request': 'call-000\n'},
            'Original qualifier handshake differs')
        session = _json(owner.read_authenticated('session.json'))
        require(session['argv'] == owner.docker + ['exec', '--interactive', '--user', '65534:65534', container_id,
            'python', '-I', '-B', '/checks/workflow_adapter.py'], 'Original qualifier session argv differs')
        checks['natural_exit'] = session.get('natural_exit') is True and session.get('exit_code') == 0 and final['exit_code'] == 0
        wanted = execution.qualification_bytes(control_id)
        original_streams: dict[str, bytes] = {}
        for index, kind in enumerate(('stdout', 'stderr')):
            raw = owner.read_blob('session-' + kind + '.bin')
            item = session[kind]
            require(item['path'] == 'session-' + kind + '.bin' and item['bytes'] == len(raw)
                and item['sha256'] == execution.sha(raw), 'Qualifier raw descriptor differs')
            original_streams[kind] = raw
            cap = execution.STDOUT_BYTES if kind == 'stdout' else execution.STDERR_BYTES
            checks[kind + '_bytes'] = (raw == wanted[index][:cap]
                and type(item['observed_bytes']) is int and item['observed_bytes'] == len(wanted[index])
                and item['truncated'] is (len(wanted[index]) > cap))
        semantics = _qualification_semantics(control_id, original_streams['stdout'])
        if semantics['applicable']:
            checks['semantic_reconstruction'] = semantics['comparison']
        expected_errors: set[str] = set()
        if control_id == 'WQ-FRAME-OVER':
            expected_errors.add('frame-limit')
        if control_id.startswith('WQ-STDOUT-'):
            expected_errors.update(('frame-limit', 'truncated-frame'))
        if control_id == 'WQ-STDOUT-OVER':
            expected_errors.add('stdout-limit')
        if control_id == 'WQ-STDERR-OVER':
            expected_errors.add('stderr-limit')
        checks['framing_disposition'] = (set(session['errors']) == expected_errors
            and session.get('capture_complete') is (not expected_errors)
            and session.get('requests') == ['call-000'] and session.get('timed_out') is False)
        for phase in ('before', 'after'):
            _required(owner, ('call-000-runtime-' + phase + '-verified.json', 'call-000-staging-' + phase + '.json'))
            require(_json(owner.read_authenticated('call-000-runtime-' + phase + '-verified.json')) == {
                'runtime': owner.runtime, 'runtime_sha256': execution.digest(owner.runtime)}
                and _json(owner.read_authenticated('call-000-staging-' + phase + '.json')) == expected_stage,
                'Original qualifier source/runtime boundary differs')
        checks['source_runtime_boundaries'] = True
        removed = _command(owner, 'container-remove', ['docker', 'rm', '--force', container_id])
        absent = _command(owner, 'container-after', ['docker', 'container', 'ls', '--all', '--quiet', '--filter', 'name=^/' + name + '$'])
        volume_removed = _command(owner, 'volume-remove', ['docker', 'volume', 'rm', volume])
        volume_absent = _command(owner, 'volume-after', ['docker', 'volume', 'ls', '--quiet', '--filter', 'name=^' + volume + '$'])
        checks['cleanup'] = bool(removed is not None and removed[1].strip() == container_id.encode()
            and absent is not None and not absent[1].strip() and volume_removed is not None
            and volume_removed[1].strip() == volume.encode() and volume_absent is not None and not volume_absent[1].strip())
    except execution.chain.ChainUnknown:
        raise
    except (AuthorityUnavailable, execution.process.ProcessError, prestart.PrestartError) as error:
        unavailable.append(str(error))
    result = {'protocol': execution.QUALIFICATION_PROTOCOL, 'control_id': control_id,
        'original_definition_purpose': 'harness_qualification', 'low_level_execution_label': 'public_release',
        'execution_id': intent['execution_id'], 'original_intent_sha256': execution.sha(intent_raw),
        'original_terminal_sha256': None if terminal is None else execution.sha(owner.read_authenticated('terminal.json')),
        'definition': owner.profile.record(), 'checks': checks, 'semantic_reconstruction': semantics, 'unavailable': unavailable,
        'status': 'passed' if all(value is True for value in checks.values()) and not unavailable
            and terminal is not None and not terminal['infrastructure'] else 'unqualified',
        'product_acceptance_authority': False, 'product_history': False}
    owner.current(owner.retained_freeze())
    require(owner.checkpoint() == before, 'Original qualifier prefix changed')
    return result
