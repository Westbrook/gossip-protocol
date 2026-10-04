"""Original-only storage capture projection into a declared final-M4 slice.

Returned candidate values remain untrusted product observations. Only the exact
physical owner plus an independently expected current prefix can supply origins.
All raw captures are reparsed; no StorageExecution dict, terminal success flag or
uploaded fixture becomes a physical execution. Layout adequacy remains the
separate enrolled reviewer responsibility. No full project acceptance is claimed.
"""
from __future__ import annotations

from dataclasses import asdict
import json
from pathlib import Path
import subprocess
from typing import Any

from . import candidate_storage_product_execution_v1 as execution
from . import candidate_storage_product_profile_v1 as profile
from . import candidate_storage_observer_v1 as b01_observer
from . import candidate_intake_store_observer_v1 as b02_observer
from . import candidate_observation_admission_v1 as admission
from . import project_acceptance_registry_v1 as registry
from .candidate_scope_consumer_v1 import AuthorityError, AuthorityUnavailable
from .gitstore import GitError
from . import candidate_storage_prestart_v1 as prestart

PROTOCOL = 'candidate-storage-product-observation-v1-ascii-json-v1-prestart-v2-desktop-inputs-v1'
VERIFIER_FILE = 'storage-product-verifier.json'


def require(ok: bool, message: str) -> None:
    if not ok:
        raise AuthorityError(message)


def _json(raw: bytes) -> Any:
    return execution.process.strict_json_loads(raw)


def _command(owner: execution.CandidateStorageExecution, label: str,
             arguments: list[str]) -> tuple[dict[str, Any], bytes] | None:
    if not owner.has_retained(label + '.json'):
        return None
    record = _json(owner.read_authenticated(label + '.json'))
    dispatch = _json(owner.read_authenticated(label + '-dispatch.json'))
    expected = owner.docker + arguments[1:]
    require(type(record) is dict and record.get('arguments') == expected and record.get('argv') == expected
        and dispatch == {'argv': expected, 'limit': dispatch.get('limit')}, 'Original control command identity differs')
    for kind in ('stdout', 'stderr'):
        item = record.get(kind)
        require(type(item) is dict and item.get('path') == label + '-' + kind + '.bin', 'Original stream path differs')
        raw = owner.read_blob(item['path'])
        require(item.get('sha256') == execution.sha(raw) and item.get('bytes') == len(raw)
            and type(item.get('observed_bytes')) is int and item['observed_bytes'] >= len(raw), 'Original stream bytes differ')
    if not execution.b01._clean(record) or any(record[kind]['observed_bytes'] != record[kind]['bytes'] for kind in ('stdout', 'stderr')):
        return None
    return record, owner.read_blob(record['stdout']['path'])


def reconstruct(owner: execution.CandidateStorageExecution) -> dict[str, Any]:
    """Mechanism is also testable with an exact fixture owner; never a receipt."""
    require(type(owner) is execution.CandidateStorageExecution, 'Exact original storage owner required')
    before = owner.checkpoint()
    intent_raw = owner.read_authenticated('intent.json')
    intent = _json(intent_raw)
    freeze = owner.retained_freeze()
    owner.current(freeze)
    require(intent['protocol'] == owner.binding.protocol and intent['registration'] == json.loads(execution.encoded(asdict(owner.observation_registration)))
        and intent['original_binding'] == json.loads(execution.encoded(asdict(owner.binding)))
        and intent['source_sha256'] == owner.binding.source_sha256
        and intent['ordered_phases'] == list(execution.b01.PHASES), 'Original intent identity differs')
    terminal = None if not owner.has_retained('terminal.json') else _json(owner.read_authenticated('terminal.json'))
    if terminal is not None:
        require(terminal['protocol'] == owner.binding.protocol and terminal['intent_sha256'] == execution.sha(intent_raw)
            and terminal['source_sha256'] == owner.binding.source_sha256
            and terminal['native_source_sha256'] == owner.binding.native_source_sha256
            and terminal['execution_id'] == intent['execution_id'] and terminal['case_id'] == owner.binding.case_id
            and terminal['family'] == owner.binding.family, 'Original terminal identity differs')
    mapper = b01_observer if owner.binding.family == 'b01' else b02_observer
    native = execution.b01 if owner.binding.family == 'b01' else execution.b02
    mapping = owner.review_authority.observer_registration(owner.plan)
    observations: dict[str, Any] = {}
    responses: dict[str, Any] = {}
    unavailable: list[str] = []
    phase_facts: list[dict[str, Any]] = []
    name, volume = intent['container'], intent['volume']
    recipe = None if owner.binding.family == 'b01' else execution.b02.validate_recipe(execution.b02.cases.execution_recipe(owner.binding.case_id))
    application = {'protocol': owner.binding.protocol, 'decision': 'unavailable' if owner.plan.schedule == 'forced_schedule_unavailable' else 'not-requested',
        'review_sha256': owner.review_sha256, 'production_forced_schedule_qualified': False}
    helpers = execution.adapter_files(owner.binding.family, owner.binding.case_id, application)
    expected_stage = {'source_manifest': admission.source_manifest(owner.files),
        'helper_manifest': admission.source_manifest(helpers), 'fixtures_sha256': execution.digest([] if recipe is None else recipe['fixtures'])}
    expected_labels = {'gossip.execution': intent['execution_id'], 'gossip.source': owner.binding.source_sha256,
        'gossip.fixture': execution.digest({'recipe': recipe, 'adapter': execution.sha(native.CHILD_ADAPTER.encode())})}
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
        if recipe is not None:
            binds['/inputs'] = staging['inputs']
        sandbox = execution.DockerValidator(owner.policy.image_id,
            {helper: raw.decode('utf-8') for helper, raw in helpers.items()}, command=prestart.COMMAND)
        create_argv = (execution.b01._start_arguments(sandbox, name, Path(staging['workspace']), Path(staging['checks']), volume)
            if recipe is None else execution.b02._start_arguments(sandbox, name, Path(staging['workspace']),
                Path(staging['checks']), Path(staging['inputs']), volume))
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
            'python', '-I', '-B', '/checks/' + ('storage_adapter.py' if recipe is None else 'intake_store_adapter.py')]
        if recipe is None:
            session_argv.append(owner.binding.case_id)
        require(_json(owner.read_authenticated('session-dispatch.json')) == {'argv': session_argv},
                'Candidate adapter must enter through exact unprivileged exec')
        prestart_value = _json(inspected[1])
    except (AuthorityUnavailable, prestart.PrestartError, execution.process.ProcessError) as error:
        prestart_error = str(error)
        unavailable.append('prestart:' + prestart_error)
    # Correlate exactly three host-driven requests with captured original stdout;
    # an incomplete later session keeps earlier independently bounded lines.
    session = None if not owner.has_retained('session.json') else _json(owner.read_authenticated('session.json'))
    lines: list[bytes] = []
    if session is not None:
        raw = owner.read_blob('session-stdout.bin')
        descriptor = session['stdout']
        require(descriptor['sha256'] == execution.sha(raw) and descriptor['bytes'] == len(raw), 'Session stdout differs')
        lines = raw.splitlines(keepends=True)
    previous_identity = None
    previous_running: dict[str, Any] | None = None
    for index, phase in enumerate(execution.b01.PHASES):
        fact: dict[str, Any] = {'phase': phase, 'capture_authenticated': False, 'response_authenticated': False,
                                'mapping': 'unavailable', 'reason': None}
        try:
            if container_id is None:
                raise AuthorityUnavailable('Original container creation incomplete')
            if prestart_value is None:
                raise AuthorityUnavailable(prestart_error or 'Original prestart proof unavailable')
            for boundary in ('before', 'after'):
                runtime_name = phase + '-runtime-' + boundary + '-verified.json'
                if not owner.has_retained(runtime_name) or _json(owner.read_authenticated(runtime_name)) != {
                        'runtime': owner.runtime, 'runtime_sha256': execution.digest(owner.runtime)}:
                    raise AuthorityUnavailable('Original runtime phase boundary unavailable')
                staged_name = phase + '-staging-' + boundary + '.json'
                if not owner.has_retained(staged_name) or _json(owner.read_authenticated(staged_name)) != expected_stage:
                    raise AuthorityUnavailable('Original phase source/helper/fixture boundary unavailable')
            response_name = phase + '-response.json'
            if not owner.has_retained(response_name) or not owner.has_retained(phase + '-request.json'):
                raise AuthorityUnavailable('Original phase response/request unavailable')
            require(owner.authenticated_position('session-dispatch.json') < owner.authenticated_position(phase + '-request.json'),
                    'Candidate request predates authenticated unprivileged session dispatch')
            require(_json(owner.read_authenticated(phase + '-request.json')) == {'phase': phase, 'request': phase + '\n'}, 'Original phase request differs')
            raw = owner.read_authenticated(response_name)
            response = _json(raw)
            require(type(response) is dict and set(response) == {'phase', 'value'} and response['phase'] == phase,
                    'Original response phase differs')
            if session is not None:
                require(len(lines) > index and lines[index] == raw, 'Phase response differs from session stdout')
            # A phase-response raw record is appended by the fixed trusted reader
            # immediately after its bounded one-line read. Final session framing
            # is a separate mechanics requirement, not authority for earlier data.
            fact['response_authenticated'] = True
            responses[phase] = response['value']
            pause = _command(owner, phase + '-pause', ['docker', 'pause', container_id])
            state = _command(owner, phase + '-state', ['docker', 'inspect', '--format', '{{json .}}', container_id])
            capture = _command(owner, phase + '-capture', ['docker', 'cp', container_id + ':/tmp', '-'])
            unpause = _command(owner, phase + '-unpause', ['docker', 'unpause', container_id])
            if any(value is None for value in (pause, state, capture)):
                raise AuthorityUnavailable('Pause/copy originals incomplete')
            if unpause is None:
                unavailable.append(phase + ':resume-original-unavailable')
            assert state is not None and capture is not None
            inspection = _json(state[1])
            if not (execution.b01._paused(inspection, volume, owner.policy.image_id)
                and inspection.get('Id') == container_id and inspection.get('Name') == '/' + name):
                responses.pop(phase, None)
                fact['response_authenticated'] = False
                raise AuthorityUnavailable('Captured container is not exact paused owner')
            try:
                prestart.validate_continuity(prestart_value, inspection, owner.runtime, previous_running=previous_running)
            except (prestart.PrestartError, execution.process.ProcessError) as error:
                responses.pop(phase, None)
                fact['response_authenticated'] = False
                raise AuthorityUnavailable('Created-to-running sandbox identity differs') from error
            config, host = inspection.get('Config'), inspection.get('HostConfig')
            if not (type(config) is dict and type(host) is dict
                and config.get('Labels') == expected_labels
                and host.get('NetworkMode') == 'none' and host.get('ReadonlyRootfs') is True):
                responses.pop(phase, None)
                fact['response_authenticated'] = False
                raise AuthorityUnavailable('Captured sandbox or execution lineage differs')
            identity = {'Config': config, 'HostConfig': host, 'Mounts': sorted(inspection['Mounts'], key=lambda x: x['Destination']),
                'Pid': inspection['State']['Pid'], 'StartedAt': inspection['State'].get('StartedAt')}
            if previous_identity is not None and identity != previous_identity:
                responses.pop(phase, None)
                fact['response_authenticated'] = False
                raise AuthorityUnavailable('Persistent storage process/container lineage changed')
            previous_identity = identity
            previous_running = inspection
            captured = native.parse_capture(capture[1])
            fact['capture_authenticated'] = True
            observations[phase] = mapper.observe_capture(captured, mapping, source_sha256=owner.binding.native_source_sha256)
            fact['mapping'] = 'available'
        except (AuthorityUnavailable, b01_observer.ObservationUnavailable, b02_observer.ObservationUnavailable,
                execution.b01.CaptureLayoutError) as error:
            fact['reason'] = str(error)
            unavailable.append(phase + ':' + str(error))
        phase_facts.append(fact)
    projection = profile.project(owner.profile, observations, responses)
    completed_session = (session is not None and session.get('exit_code') == 0 and session.get('capture_complete') is True
        and not session.get('errors') and session.get('extra_lines') == 0 and len(lines) == 3
        and all(session[kind].get('truncated') is False and session[kind].get('observed_bytes') == session[kind].get('bytes')
                for kind in ('stdout', 'stderr')))
    cleaned = False
    if container_id is not None:
        removed = _command(owner, 'container-remove', ['docker', 'rm', '--force', container_id])
        absent = _command(owner, 'container-after', ['docker', 'container', 'ls', '--all', '--quiet', '--filter', 'name=^/' + name + '$'])
        volume_removed = _command(owner, 'volume-remove', ['docker', 'volume', 'rm', volume])
        volume_absent = _command(owner, 'volume-after', ['docker', 'volume', 'ls', '--quiet', '--filter', 'name=^' + volume + '$'])
        cleaned = bool(removed is not None and removed[1].strip() == container_id.encode()
            and absent is not None and not absent[1].strip() and volume_removed is not None
            and volume_removed[1].strip() == volume.encode() and volume_absent is not None and not volume_absent[1].strip())
    final_staging = owner.has_retained('staging-final.json') and _json(owner.read_authenticated('staging-final.json')) == expected_stage
    mechanics = bool(completed_session and cleaned and final_staging and not unavailable and terminal is not None
        and not terminal['infrastructure'] and all(row['capture_authenticated'] for row in phase_facts))
    result = {'protocol': PROTOCOL if owner.capture_policy is None else PROTOCOL + '-git-source-batch-v1',
        'execution_protocol': owner.binding.protocol,
        'execution_id': intent['execution_id'], 'registration': asdict(owner.observation_registration),
        'original_binding': asdict(owner.binding), 'original_intent_sha256': execution.sha(intent_raw),
        'original_terminal_sha256': None if terminal is None else execution.sha(owner.read_authenticated('terminal.json')),
        'physical_receipt_basis': 'terminal' if terminal is not None else 'durable-intent-partial-observations',
        'original_context_sha256': before.context_sha256, 'original_config_sha256': execution.digest(owner.config),
        'layout_review_sha256': owner.review_sha256, 'profile': owner.profile.record(),
        'phase_facts': phase_facts, 'projection': projection,
        'mechanics': {'status': 'passed' if mechanics else 'infrastructure_error', 'cleanup_verified': cleaned,
                      'session_complete': completed_session, 'prestart_verified': prestart_value is not None,
                      'unavailable': unavailable},
        'cohort_freeze': None if freeze is None else asdict(freeze),
        'production_scope_authority': False, 'independent_semantic_scope_review_supplied': False,
        'whole_project_acceptance': False}
    owner.current(freeze)
    require(owner.checkpoint() == before, 'Original storage journal changed during projection')
    return result


def selector_catalog(family: str, case_id: str, *, purpose: str,
                     capture_policy: execution.source_capture.BatchCapturePolicy | None = None) -> dict[str, Any]:
    """Closed prospective selectors, with authored partial source-unit facets.

    This supplies concrete compiler review input; installing the new storage
    factory into the complete ScopePlan authority remains an explicit versioned
    integration step. It does not self-enroll as a semantic reviewer.
    """
    value = profile.profile_for(family, case_id, purpose)
    rows = []
    for row in value.record()['diagnostics']:
        rows.append({**row, 'case_id': row['check_id'],
            'observation_pointer': '/projection' + row['selector'],
            'value_domain': [True, False, None], 'physical_capture_qualified': False,
            'whole_source_unit_qualified': False})
    rows.append({'case_id': execution.mechanics_case_id(value), 'applicability': 'normative',
        'observation_pointer': '/mechanics/status', 'value_domain': ['passed', 'infrastructure_error'],
        'source_unit_facets': [], 'scope': 'Owned capture/session/cleanup mechanics only; no semantic clause inferred'})
    result = {'protocol': PROTOCOL, 'family': family, 'history_id': case_id,
        'original_definition_purpose': profile.ORIGINAL_DEFINITION_PURPOSE,
        'execution_purpose': purpose, 'target_contract_sha256': execution.TARGET_CONTRACT,
        'target_milestone': 'M4', 'profile_sha256': value.sha256,
        'definition_sha256': execution.digest(value.record()), 'evaluator_sources': execution.evaluator_sources(),
        'ordered_case_ids': list(value.ordered_case_ids + (execution.mechanics_case_id(value),)),
        'selectors': rows, 'required_unfinished_coverage': list(profile.REMAINING_COVERAGE),
        'scope_factory_registered': False, 'semantic_authority': False}
    if capture_policy is not None:
        require(type(capture_policy) is execution.source_capture.BatchCapturePolicy, 'Exact capture policy required')
        result.update(protocol=PROTOCOL + '-git-source-batch-v1',
            source_capture=capture_policy.record(), execution_protocol=execution.BATCH_PROTOCOL)
    return result


def publish_verifier(owner: execution.CandidateStorageExecution) -> execution.chain.PrefixCommitment:
    require(type(owner) is execution.CandidateStorageExecution and owner.mode == 'physical', 'Physical original owner required')
    record = reconstruct(owner)
    raw = execution.encoded(record)
    if owner.has_retained(VERIFIER_FILE):
        require(owner.read_authenticated(VERIFIER_FILE) == raw, 'Retained original verifier differs')
    else:
        owner._retain(VERIFIER_FILE, raw)
    owner.current(owner.retained_freeze())
    return owner.checkpoint()


class StorageObservationSource:
    def __init__(self, owner: execution.CandidateStorageExecution, expected_checkpoint: execution.chain.PrefixCommitment):
        require(type(owner) is execution.CandidateStorageExecution and owner.mode == 'physical'
            and type(expected_checkpoint) is execution.chain.PrefixCommitment, 'Exact physical source owner and independent prefix required')
        require(owner.checkpoint() == expected_checkpoint and owner.has_retained(VERIFIER_FILE), 'Original verifier/current prefix unavailable')
        self.owner, self.expected_checkpoint = owner, expected_checkpoint

    def observation(self, gate: registry.Gate, freeze: registry.CohortFreeze | None) -> registry.Observation:
        try:
            return self._observation(gate, freeze)
        except (execution.ExecutionUnknown, execution.chain.ChainUnknown, admission.AdmissionUnavailable,
                OSError, subprocess.SubprocessError, GitError, execution.source_capture.SourceCaptureUnavailable) as error:
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
        checks = record['projection']['checks']
        outcomes = tuple(registry.CaseResult(case_id, 'passed' if checks[case_id] is True else
            'failed' if checks[case_id] is False else 'infrastructure_error') for case_id in owner.profile.ordered_case_ids)
        outcomes += (registry.CaseResult(execution.mechanics_case_id(owner.profile), record['mechanics']['status']),)
        require(tuple(item.case_id for item in outcomes) == gate.ordered_case_ids, 'Exact decisive roster differs')
        original_receipt = record['original_terminal_sha256'] or record['original_intent_sha256']
        physical = registry.PhysicalExecution(gate.binding, record['execution_id'], original_receipt, execution.sha(raw),
            'completed' if record['mechanics']['status'] == 'passed' else 'infrastructure_error', outcomes,
            None if freeze is None else freeze.receipt_sha256)
        owner.current(freeze)
        require(owner.checkpoint() == self.expected_checkpoint, 'Current original prefix changed')
        return registry.Observation(gate.gate_id, gate.binding, physical)
