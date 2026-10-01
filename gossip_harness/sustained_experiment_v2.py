"""Versioned sustained study with bounded, exactly bound validation sessions.

The v1 controller remains frozen. This additive entry point retains its provider,
Git promotion, settled accounting and real new-process checkpoint boundaries,
while independently specifying batched initial validations and final matrices.
No paid call is made by rehearsal mode. Validation reuse is opt-in and never
applies to independent final or historical repeatability observations.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict
import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import signal
import subprocess
import sys
import threading
import time

from devtools.study_validation import StudyValidationSession, study_signals, study_validation_contract
from devtools.validation_session import ResourceBudget
from devtools.study_receipts import audit_study_sessions
from .gitstore import GitStore
from .ledger import Ledger
from .pilot import DEFAULT_IMAGE, LeaseKeeper, Trace, _save
from . import sustained_experiment as legacy
from .sustained_experiment import (
    cases_for, checked_evaluate, credential, digest, known_result, local_promote, projects, record_stage,
)
from .sustained_stage_v2 import POLICIES, run_stage
from .worker import MODEL, STRONG_MODEL, OpenAIWorker, WorkerFailure, WorkerRequest

PROTOCOL = 'sustained-quality-v2'
ROOT = Path(__file__).resolve().parents[1]
CORE = (*legacy.CORE, 'sustained_experiment_v2.py', 'sustained_stage_v2.py')
CONTROLLER_DRAIN_SECONDS = 360


@contextmanager
def cancellation_signals():
    """Translate CLI termination into the same retained/drained path as Ctrl-C."""
    previous = {}
    def interrupt(signum, frame):
        raise KeyboardInterrupt(f'Controller received signal {signum}')
    try:
        for number in (signal.SIGTERM, signal.SIGHUP):
            previous[number] = signal.signal(number, interrupt)
        yield
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)


def _run_controller(command, *, cwd):
    """Wait for only the child group we own; cancellation drains its validators.

    Ordinary container deadlines remain authoritative during the grace period.
    A child that cannot drain is stopped and recorded as interrupted; its
    incomplete evidence is retained and is never a correctness judgment.
    """
    process = subprocess.Popen(command, cwd=cwd, start_new_session=True)
    try:
        return process.wait()
    except BaseException:
        # Repeated termination must not interrupt the owned-child cleanup.
        previous = {number: signal.signal(number, signal.SIG_IGN)
                    for number in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
        try:
            # The group may outlive its leader. Address our owned group even
            # when wait/poll already observed leader exit.
            try:
                os.killpg(process.pid, signal.SIGINT)
            except ProcessLookupError:
                pass
            timed_out = False
            try:
                process.wait(timeout=CONTROLLER_DRAIN_SECONDS)
            except subprocess.TimeoutExpired:
                timed_out = True
            finally:
                # A descendant can ignore INT/TERM while its leader drains and
                # exits. A successful leader wait alone never proves cleanup.
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait(timeout=5)
            if timed_out:
                raise RuntimeError('Owned controller did not drain; inspect retained cleanup and ledger evidence')
        finally:
            for number, handler in previous.items():
                signal.signal(number, handler)
        raise


def support_paths():
    return tuple('gossip_harness/' + name for name in CORE)


def contract(image=DEFAULT_IMAGE, *, budget=ResourceBudget(), deterministic_visible=False):
    """Preregister scientific scheduling and exact evaluator dependencies."""
    validation = study_validation_contract(
        image=image, protocol=PROTOCOL, budget=budget, timeout_seconds=300,
        case_timeout_seconds=12, deterministic_visible=deterministic_visible,
        support_paths=support_paths())
    value = legacy.contract(image)
    value.update(
        protocol=PROTOCOL,
        sources={name: hashlib.sha256((ROOT / 'gossip_harness' / name).read_bytes()).hexdigest()
                 for name in CORE},
        validation=validation,
        controller_module='gossip_harness.sustained_experiment_v2',
        validation_scheduling={
            'initial_portfolio': 'One bounded ordered batch after all proposals are parsed and retained',
            'dependent_repairs': 'Serial singleton batches after each preceding observation',
            'final_matrix': 'Frozen trajectory: independent final visible/hidden plus historical repeatability batch',
            'process_lifetime': 'One session per begin/resume PID; one shared parent final session for the roster',
            'reuse': 'Only explicitly deterministic visible source/suite observations; control claims always rechecked',
            'provider_overlap': 'No validation batch overlaps provider invocations',
            'termination': 'Stop queued work and drain only owned controller group before exit',
            'controller_drain_seconds': CONTROLLER_DRAIN_SECONDS,
        })
    return value


def _session(output, config, expected):
    session = StudyValidationSession(
        output, image=config['image'], protocol=PROTOCOL,
        budget=ResourceBudget(**config['validation_budget']), timeout_seconds=300,
        case_timeout_seconds=12, deterministic_visible=config['deterministic_visible'],
        cache=Path(config['validation_cache']), support_paths=support_paths())
    if session.contract != expected['validation']:
        raise ValueError('Validation support changed after preregistration')
    return session


def child(config_path, phase):
    """One controller lifetime. Exit/resume deliberately occurs between stages."""
    from .sustained_checkpoint import load_checkpoint, save_checkpoint
    config = json.loads(Path(config_path).read_text())
    root = Path(config['root'])
    expected = contract(config['image'], budget=ResourceBudget(**config['validation_budget']),
                        deterministic_visible=config['deterministic_visible'])
    if digest(expected) != config['contract_sha']:
        raise ValueError('Source or fixture changed after preregistration')
    project = next(p for p in projects() if p['id'] == config['project_id'])
    if phase == 'resume':
        handoff = json.loads((root / 'handoff.json').read_text())
        state = load_checkpoint(root / 'checkpoint.json', handoff['checkpoint_sha256'],
                                expected_contract_sha=config['contract_sha'], expected_pid=os.getpid())
        if state['next_stage'] != 2:
            raise ValueError('Expected the settled two-milestone checkpoint')
        _save(root / 'resume.json', dict(previous_pid=state['pid'], resumed_pid=os.getpid(),
                                       checkpoint_sha256=handoff['checkpoint_sha256'], head=state['head'],
                                       files_sha256=digest(state['files']), verified=True))
        base = GitStore(Path(state['store_path']))
    else:
        base = GitStore.create(root / 'initial.git', project['initial_files'])
        state = dict(contract_sha=config['contract_sha'], pid=os.getpid(), store_path=str(base.path),
                     head=base.head(), files=project['initial_files'], next_stage=0, completed_stages=[],
                     stages=[], budget_ledger=config['budget_ledger'], invocations=[])
    # Resume rejects corrupt bytes, wrong PID/Git state and unsettled accounting
    # before any ledger bootstrap or Docker setup. Provider work remains behind
    # both the immutable checkpoint gate and a healthy sandbox preflight.
    ledger = Ledger(Path(config['budget_ledger']))
    validation = _session(root / ('validation-' + phase), config, expected)
    okay, detail = validation.preflight()
    if not okay:
        raise RuntimeError('Docker preflight failed: ' + detail)
    trace = Trace(root / f'trace-{phase}.jsonl')
    with study_signals(validation):
        models = {}
        if config['live']:
            key = credential(config['env_file'])
            models = {role: OpenAIWorker(key, model=model, max_output_tokens=8192, timeout=180)
                      for role, model in (('cheap', MODEL), ('strong', STRONG_MODEL))}
        start = state['next_stage']
        stop = 2 if phase == 'begin' else 3
        for stage in range(start, stop):
            validation.raise_if_cancelled()
            stage_root = root / f'stage-{stage}'
            stage_root.mkdir()
            (stage_root / 'requests').mkdir()
            billing = config['namespace'] + f'/stage-{stage}'
            ledger.add_task(billing)
            calls = []
            lock = threading.Lock()
            fatal = threading.Event()
            def emit(event, /, **data):
                data.pop('kind', None)
                trace.emit(event, stage_index=stage, **{k: v for k, v in data.items() if k != 'stage_index'})
            with LeaseKeeper(ledger) as keeper:
                keeper.claim(billing, 'sustained-controller')
                def invoke(*, role, model, files, instructions, allowed_paths, feedback, metadata):
                    validation.raise_if_cancelled()
                    if fatal.is_set():
                        raise RuntimeError('A preceding request requires investigation')
                    call_id = f"{role}-{metadata['candidate_id']}-{metadata['round']}"
                    request = WorkerRequest(billing + '/' + call_id, instructions, tuple(allowed_paths),
                                            files, base.head(), metadata['round'], feedback)
                    if config['live'] and model not in models:
                        raise ValueError('Unknown live model role; refusing reference fallback')
                    worker = models[model] if config['live'] else None
                    units = worker.reservation_units(request) if worker else 0
                    reservation = billing + '/' + call_id
                    with lock:
                        validation.raise_if_cancelled()
                        ledger.reserve(reservation, keeper.check(billing), units, now=time.time())
                    _save(stage_root / 'requests' / (call_id + '.request.json'), asdict(request))
                    emit('request_started', call_id=call_id, model=model, reservation=reservation,
                         request_sha256=digest(asdict(request)), reserved_units=units)
                    began = time.monotonic()
                    print(f"{config['run_id']} stage {stage + 1}: {call_id} ({model})", flush=True)
                    try:
                        result = worker.run(request) if worker is not None else known_result(project, stage, metadata, files)
                    except WorkerFailure as error:
                        if error.usage_units is not None:
                            ledger.settle(reservation, error.usage_units)
                        row = dict(call_id=call_id, reservation=reservation, role=role, model=model,
                                   usage_units=error.usage_units, failure=str(error), metadata=error.metadata,
                                   seconds=time.monotonic() - began)
                        with lock:
                            calls.append(row)
                        _save(stage_root / 'requests' / (call_id + '.result.json'), row)
                        emit('request_failed', **row)
                        if error.usage_units is None or error.metadata.get('halt'):
                            fatal.set()
                            raise RuntimeError('Provider failure requires investigation; no automatic retry') from None
                        return None
                    ledger.settle(reservation, result.usage_units)
                    row = dict(call_id=call_id, reservation=reservation, role=role, model=model,
                               usage_units=result.usage_units, metadata=result.metadata,
                               seconds=time.monotonic() - began)
                    with lock:
                        calls.append(row)
                    _save(stage_root / 'requests' / (call_id + '.result.json'), {**asdict(result), **row})
                    emit('request_finished', **row)
                    validation.raise_if_cancelled()
                    return result

                def retain(files, label):
                    store = GitStore.fork(base, stage_root / (label + '.git'))
                    tip = store.propose({p: files[p] for p in project['allowed_paths']}, message=label)
                    binding = dict(store_path=str(store.path), tip_sha=tip, files_sha256=digest(files))
                    if store.read_files(tip) != files:
                        raise RuntimeError('Retained proposal differs from supplied source')
                    _save(stage_root / (label + '.json'), dict(binding=binding, files=files))
                    return binding

                def evaluate_many(requests):
                    suite = cases_for(project, stage, 'visible')
                    observations = validation.evaluate_many(requests, suite, seed=20260930)
                    for request, observation in zip(requests, observations):
                        label = request['label']
                        _save(stage_root / (label + '.validation.json'), observation['receipt'])
                        _save(stage_root / (label + '.validation-provenance.json'), observation['validation'])
                        emit('visible_validation', label=label, files_sha256=digest(request['files']),
                             cases_sha256=digest(suite), receipt=observation['receipt'],
                             validation=observation['validation'])
                    return observations

                prior = state['stages'][-1] if state['stages'] else {}
                result = run_stage(project, stage, config['policy'], state['files'], prior,
                                   invoke=invoke, evaluate_many=evaluate_many, retain=retain, emit=emit)
                result['stage_index'] = stage
                result['invocations'] = sorted(calls, key=lambda c: c['call_id'])
                _save(stage_root / 'result.json', result)
                record_stage(stage_root, result, ledger, keeper.check(billing))
            state['stages'].append(result)
            state['files'] = result['files']
            state['invocations'].extend(result['invocations'])
            if not result['completed']:
                state['terminal'] = 'max_steps_incomplete'
                _save(root / 'trajectory.json', state)
                return
            base = local_promote(base, result['selected_binding'], stage_root / 'checkpoint.git',
                                 result['files'], project['allowed_paths'])
            state.update(store_path=str(base.path), head=base.head(), next_stage=stage + 1, pid=os.getpid())
            state['completed_stages'].append(dict(completed=True, stage_index=stage, head=base.head()))
        if phase == 'begin':
            sha = save_checkpoint(root / 'checkpoint.json', state)
            _save(root / 'handoff.json', dict(checkpoint_sha256=sha, previous_pid=os.getpid(), head=base.head()))
            print(f"{config['run_id']}: durable handoff after two milestones", flush=True)
        else:
            state['terminal'] = 'visible_complete'
            _save(root / 'trajectory.json', state)


def validate_rehearsal(proof, expected, *, evidence_root=None):
    """Only a full rehearsal of this exact scheduling/reuse contract authorizes live mode."""
    wanted = {(project, policy, 0) for project in expected['project_ids'] for policy in POLICIES}
    cases = proof.get('cases', [])
    actual = [(case.get('project_id'), case.get('policy'), case.get('repetition')) for case in cases]
    if (proof.get('experiment') != PROTOCOL or proof.get('mode') != 'rehearsal'
            or proof.get('contract') != expected or proof.get('contract_sha') != digest(expected)
            or proof.get('status') != 'finished' or proof.get('unexecuted') != []
            or type(proof.get('incremental_micro_usd')) is not int
            or proof['incremental_micro_usd'] != 0
            or set(actual) != wanted or len(actual) != len(wanted)):
        raise ValueError('Complete exact v2 six-project Docker rehearsal required')
    for case in cases:
        project = next(project for project in projects() if project['id'] == case['project_id'])
        handoff = case.get('handoff', {})
        final = [case.get('final_visible_validation', {}), case.get('final_hidden_validation', {})]
        historical = case.get('historical', [])
        if (case.get('status') != 'accepted' or case.get('milestones_completed') != 3
                or not isinstance(case.get('release_head'), str) or not case['release_head']
                or case.get('exact_tested_sha') != case['release_head']
                or type(case.get('usage_micro_usd')) is not int or case['usage_micro_usd'] != 0
                or not case.get('invocations')
                or any(call.get('usage_units') != 0 or call.get('metadata', {}).get('api_calls') != 0
                       for call in case['invocations'])
                or case.get('metrics', {}).get('repairs', 0) < 3
                or case.get('metrics', {}).get('premature_completion', 0) < 3
                or case.get('accepted') is not True or case.get('rehearsal_repair_verified') is not True
                or handoff.get('verified') is not True
                or any(type(handoff.get(key)) is not int or handoff[key] <= 0
                       for key in ('previous_pid', 'resumed_pid'))
                or handoff['previous_pid'] == handoff['resumed_pid']
                or any(item.get('purpose') != 'final' or item.get('physical') is not True
                       or item.get('reuse') is not None or not item.get('execution_id') for item in final)
                or len(historical) != 3
                or [item.get('stage_index') for item in historical] != [0, 1, 2]
                or any(item.get('validation', {}).get('purpose') != 'repeatability'
                       or item.get('validation', {}).get('physical') is not True
                       or item.get('validation', {}).get('reuse') is not None
                       or not item.get('validation', {}).get('execution_id') for item in historical)):
            raise ValueError('V2 rehearsal requires real repairs, new-PID handoffs and independent final observations')
        independent = final + [item['validation'] for item in historical]
        if len({item['execution_id'] for item in independent}) != len(independent):
            raise ValueError('Independent observations require distinct physical executions')
        for stage, kind, receipt, provenance in [
                (2, 'visible', case.get('final_visible'), final[0]),
                (2, 'hidden', case.get('final_hidden'), final[1]),
                *[(stage, 'hidden', item.get('hidden'), item['validation'])
                  for stage, item in enumerate(historical)]]:
            files = dict(project['initial_files'])
            files.update({path: project['stages'][stage]['known_files'][path]
                          for path in project['allowed_paths']})
            suite = cases_for(project, stage, kind)
            class RehearsalReceipt:
                def evaluate(self, supplied_files, supplied_cases):
                    return receipt
            try:
                checked_evaluate(RehearsalReceipt(), files, suite)
            except (RuntimeError, AttributeError, TypeError, KeyError) as error:
                raise ValueError('Rehearsal receipt must bind the exact known source and ordered suite') from error
            if (receipt['passed'] is not True or provenance.get('source_sha256') != digest(files)
                    or provenance.get('suite_sha256') != digest(suite)
                    or stage == 2 and case.get('files_sha256') != digest(files)):
                raise ValueError('Rehearsal evidence does not match final source and suite')


    if evidence_root is not None:
        _audit_rehearsal(proof, expected, Path(evidence_root))


def _audit_rehearsal(proof, expected, evidence_root):
    """Reconcile summary gates with immutable raw session and trajectory evidence."""
    root = evidence_root.resolve()
    case_roots = []
    for case in proof['cases']:
        run_id = f"{case['project_id']}-{case['policy']}-{case['repetition']}"
        if case.get('run_id') != run_id or Path(case.get('root', '')).resolve() != root / run_id:
            raise ValueError('Rehearsal case path does not match its registered roster')
        case_roots.append(root / run_id)
    shared_final = root / 'validation-final'
    session_paths = [path / ('validation-' + phase) for path in case_roots for phase in ('begin', 'resume')]
    audited = audit_study_sessions([*session_paths, shared_final], expected['validation'], evidence_root=root)
    raw = {row['artifact_path']: row for row in audited['observations']}
    final_rows = {row['logical_index']: row for row in audited['observations']
                  if Path(row['artifact_path']).parent == shared_final}
    used_visible, used_final = set(), set()

    def match(provenance, receipt=None):
        row = raw.get(provenance.get('artifact_path'))
        if row is None:
            raise ValueError('Summary observation has no raw execution evidence')
        result = row['result']
        if (any(provenance.get(key) != value for key, value in result.items() if key != 'receipt')
                or receipt is not None and receipt != result['receipt']):
            raise ValueError('Summary observation differs from its raw execution')
        return row

    for case, case_root in zip(proof['cases'], case_roots):
        project = next(item for item in projects() if item['id'] == case['project_id'])
        trajectory_path = case_root / 'trajectory.json'
        if trajectory_path.is_symlink():
            raise ValueError('Trajectory evidence cannot be a symlink')
        trajectory_bytes = trajectory_path.read_bytes()
        if hashlib.sha256(trajectory_bytes).hexdigest() != case['trajectory_sha256']:
            raise ValueError('Trajectory differs from the frozen summary')
        state = json.loads(trajectory_bytes)
        if (state['contract_sha'] != digest(expected) or len(state['completed_stages']) != 3
                or len(state['stages']) != 3 or digest(state['files']) != case['files_sha256']
                or state['invocations'] != case['invocations']):
            raise ValueError('Final source, accounting or trajectory completeness changed')
        for stage, result in enumerate(state['stages']):
            if result['completed'] is not True or result['stage_protocol'] != 'sustained-stage-v2':
                raise ValueError('Incomplete or wrong-version sustained stage')
            selected = match(result['visible_validation'], result['visible_receipt'])
            if selected['job']['files'] != result['files']:
                raise ValueError('Selected stage source differs from its visible observation')
            for proposal in result['trajectory']:
                if proposal['kind'] != 'builder':
                    continue
                row = match(proposal['validation'])
                expected_session = case_root / ('validation-begin' if stage < 2 else 'validation-resume')
                if (Path(row['artifact_path']).parent != expected_session or row['job']['purpose'] != 'visible'
                        or row['job']['cases'] != cases_for(project, stage, 'visible')
                        or digest(row['job']['files']) != proposal['files_sha256']
                        or row['artifact_path'] in used_visible):
                    raise ValueError('Visible proposal matrix is duplicated or incorrectly bound')
                used_visible.add(row['artifact_path'])
        span = case['final_observation_range']
        if (type(span.get('start')) is not int or type(span.get('end')) is not int
                or span['end'] - span['start'] != 5
                or case.get('final_validation_session') != str(shared_final / 'session.json')):
            raise ValueError('Case final observation range is incomplete')
        provenances = [case['final_visible_validation'], case['final_hidden_validation'],
                       *[item['validation'] for item in case['historical']]]
        receipts = [case['final_visible'], case['final_hidden'], *[item['hidden'] for item in case['historical']]]
        labels = ['final-visible', 'final-hidden', 'historical-0', 'historical-1', 'historical-2']
        for index, provenance, receipt, label in zip(range(span['start'], span['end']), provenances, receipts, labels):
            row = match(provenance, receipt)
            if (final_rows.get(index) != row or index in used_final
                    or provenance['label'] != case['run_id'] + '-' + label):
                raise ValueError('Case final matrix is duplicated, reordered or cross-bound')
            used_final.add(index)
    if (used_final != set(final_rows)
            or used_visible != {path for path in raw if Path(path).parent != shared_final}):
        raise ValueError('Rehearsal contains missing or unaccounted validation observations')


def _snapshot_sources(output, expected):
    """Retain the complete bound Python runtime, without importing snapshot code."""
    sources = expected['validation']['support_sha256']
    for relative, expected_sha in sources.items():
        source = ROOT / relative
        data = source.read_bytes()
        if hashlib.sha256(data).hexdigest() != expected_sha:
            raise ValueError('Bound runtime changed before source retention: ' + relative)
        destination = output / 'source-snapshot' / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(data)
    _save(output / 'source-snapshot' / 'manifest.json', dict(
        contract_sha256=digest(expected), sources=sources, complete_runtime=True,
        executed=False, module='gossip_harness.sustained_experiment_v2'))


def finalize_case(root, project, config, validation):
    state = json.loads((root / 'trajectory.json').read_text())
    trajectory_sha = hashlib.sha256((root / 'trajectory.json').read_bytes()).hexdigest()
    # Every provider invocation and stage choice is frozen before hidden work.
    requests = [
        dict(label=config['run_id'] + '-final-visible', files=state['files'], cases=cases_for(project, 2, 'visible'),
             purpose='final', reason='Independent final cumulative visible acceptance'),
        dict(label=config['run_id'] + '-final-hidden', files=state['files'], cases=cases_for(project, 2, 'hidden'),
             purpose='final', reason='Independent final cumulative hidden acceptance'),
        *[dict(label=config['run_id'] + f'-historical-{stage}', files=result['files'],
               cases=cases_for(project, stage, 'hidden'), purpose='repeatability',
               reason='Independent retrospective stage diagnostic; never model feedback')
          for stage, result in enumerate(state['stages'])],
    ]
    observations = validation.evaluate_matrix(requests)
    if (len(observations) != len(requests)
            or hashlib.sha256((root / 'trajectory.json').read_bytes()).hexdigest() != trajectory_sha):
        raise RuntimeError('Frozen trajectory changed or final matrix is incomplete')
    for request, observation in zip(requests, observations):
        provenance = observation['validation']
        if (provenance.get('physical') is not True
                or provenance.get('purpose') != request['purpose']
                or provenance.get('reuse') is not None):
            raise RuntimeError('Independent final and historical observations must execute physically')
    indexes = [observation['validation']['logical_index'] for observation in observations]
    if indexes != list(range(indexes[0], indexes[0] + len(requests))):
        raise RuntimeError('Final observations are not a contiguous ordered batch')
    observation_range = dict(start=indexes[0], end=indexes[-1] + 1)
    final_visible, final_hidden = (observation['receipt'] for observation in observations[:2])
    historical = [dict(stage_index=stage, binding=result['selected_binding'],
                       hidden=observation['receipt'], validation=observation['validation'])
                  for stage, (result, observation) in enumerate(zip(state['stages'], observations[2:]))]
    accepted = (len(state['completed_stages']) == 3 and final_visible['passed'] and final_hidden['passed'])
    release_head = None
    if accepted:
        checkpoint = GitStore(Path(state['store_path']))
        binding = dict(store_path=str(checkpoint.path), tip_sha=checkpoint.head())
        release = local_promote(GitStore(root / 'initial.git'), binding, root / 'release.git',
                                state['files'], project['allowed_paths'])
        release_head = release.head()
        for path, content in state['files'].items():
            destination = root / 'accepted' / path
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_text(content)
    metrics = {name: sum(s['metrics'][name] for s in state['stages']) for name in
               ('builder_calls', 'reviewer_calls', 'repairs', 'premature_completion', 'regressions', 'stagnation_events')}
    hidden_rows = list(zip(cases_for(project, 2, 'hidden'), final_hidden['outcomes']))
    requirements = sorted({c['requirement'] for c, _ in hidden_rows})
    coverage = {r: all(o['passed'] for c, o in hidden_rows if c['requirement'] == r) for r in requirements}
    handoff = json.loads((root / 'resume.json').read_text()) if (root / 'resume.json').exists() else {}
    result = dict(project_id=project['id'], policy=config['policy'], repetition=config['repetition'],
                  run_id=config['run_id'], accepted=accepted, milestones_completed=len(state['completed_stages']),
                  status='accepted' if accepted else ('final_quality_failed' if len(state['completed_stages']) == 3 else state['terminal']),
                  release_head=release_head, exact_tested_sha=release_head, files_sha256=digest(state['files']),
                  trajectory_sha256=trajectory_sha, final_visible=final_visible, final_hidden=final_hidden,
                  historical=historical, requirement_coverage=coverage, handoff=handoff, metrics=metrics,
                  final_visible_validation=observations[0]['validation'],
                  final_hidden_validation=observations[1]['validation'],
                  final_observation_range=observation_range,
                  final_validation_session=str(Path(config.get('final_validation_path', root / 'validation-final')) / 'session.json'),
                  final_validation_summary=dict(scope='this case final matrix', logical_jobs=len(observations),
                                                physical_executions=len(observations), reused_observations=0),
                  validation_session_paths=[str(root / ('validation-' + phase) / 'session.json')
                                            for phase in ('begin', 'resume')],
                  usage_micro_usd=sum(i['usage_units'] or 0 for i in state['invocations']),
                  invocations=state['invocations'], root=str(root),
                  rehearsal_repair_verified=not config['live'] and metrics['repairs'] >= 3 and metrics['premature_completion'] >= 3)
    _save(root / 'result.json', result)
    return result


def run(output, *, live=False, rehearsal=None, repetitions=2, budget_ledger=None,
        budget_units=20_000_000, env_file=Path('.env.local'), image=DEFAULT_IMAGE,
        validation_workers=2, validation_cpus=2, validation_memory_mib=512,
        deterministic_visible=False):
    if type(repetitions) is not int or not 1 <= repetitions <= 4:
        raise ValueError('Repetitions must be one through four')
    budget = ResourceBudget(workers=validation_workers, cpus=validation_cpus,
                            memory_mib=validation_memory_mib, outer_parallelism=1)
    expected = contract(image, budget=budget, deterministic_visible=deterministic_visible)
    if live:
        if not rehearsal:
            raise ValueError('Live mode requires an exact rehearsal')
        validate_rehearsal(json.loads(Path(rehearsal).read_text()), expected,
                           evidence_root=Path(rehearsal).resolve().parent)
        credential(env_file)  # Validate silently before creating result directories.
    else:
        repetitions = 1
    output = Path(output)
    if output.is_symlink():
        raise ValueError('Output cannot be a symlink')
    output = output.resolve()
    if output.exists():
        raise ValueError('Output directory already exists; retained results are never overwritten')
    output.mkdir(parents=True)
    ledger_path = Path(budget_ledger).resolve() if live and budget_ledger else output / 'budget.sqlite'
    ledger = Ledger(ledger_path, budget_units=budget_units if live else 0)
    before = ledger.budget()
    _snapshot_sources(output, expected)
    roster = [(p['id'], policy, repetition) for repetition in range(repetitions)
              for p in projects() for policy in POLICIES]
    random.Random(20260930).shuffle(roster)
    report = dict(schema_version=2, experiment=PROTOCOL, mode='live' if live else 'rehearsal',
                  contract=expected, contract_sha=digest(expected), repetitions=repetitions,
                  cases=[], unexecuted=[list(x) for x in roster], status='running', budget_before=before,
                  budget=before, started_at=time.time())
    _save(output / 'preregistered.json', dict(contract=expected, roster=roster,
                                           budget_cap_micro_usd=before['limit']))
    _save(output / 'fixtures.json', projects())
    _save(output / 'results.json', report)
    namespace = PROTOCOL + '/' + digest(str(output))[:16]
    final_validation = None
    try:
        for project_id, policy, repetition in roster:
            run_id = f'{project_id}-{policy}-{repetition}'
            root = output / run_id
            root.mkdir()
            config = dict(root=str(root), contract_sha=digest(expected), image=image, project_id=project_id,
                          policy=policy, repetition=repetition, run_id=run_id, live=live,
                          env_file=str(Path(env_file).resolve()), budget_ledger=str(ledger_path),
                          namespace=namespace + '/' + run_id, validation_budget=asdict(budget),
                          deterministic_visible=deterministic_visible,
                          validation_cache=str(output / 'validation-cache'),
                          final_validation_path=str(output / 'validation-final'))
            _save(root / 'config.json', config)
            began = time.monotonic()
            for phase in ('begin', 'resume'):
                if phase == 'resume' and (root / 'trajectory.json').exists():
                    break  # An incomplete earlier milestone cannot be skipped.
                returncode = _run_controller(
                    [sys.executable, '-m', 'gossip_harness.sustained_experiment_v2',
                     'child', str(root / 'config.json'), phase], cwd=Path(__file__).parent.parent)
                if returncode:
                    raise RuntimeError(f'Controller {run_id}/{phase} stopped; inspect retained receipts before retrying')
            project = next(p for p in projects() if p['id'] == project_id)
            if final_validation is None:
                final_validation = _session(output / 'validation-final', config, expected)
            with study_signals(final_validation):
                result = finalize_case(root, project, config, final_validation)
            result['elapsed_seconds'] = time.monotonic() - began
            _save(root / 'result.json', result)
            report['cases'].append(result)
            report['unexecuted'].remove([project_id, policy, repetition])
            report['budget'] = ledger.budget()
            _save(output / 'results.json', report)
            print(f"{run_id}: {result['status']}; milestones {result['milestones_completed']}/3", flush=True)
        report['status'] = 'finished'
    except (Exception, KeyboardInterrupt) as error:
        report['status'] = 'interrupted'
        report['failure'] = str(error) or type(error).__name__
        raise
    finally:
        report['budget'] = ledger.budget()
        report['finished_at'] = time.time()
        report['incremental_micro_usd'] = report['budget']['spent_or_reserved'] - before['spent_or_reserved']
        _save(output / 'results.json', report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    child_parser = commands.add_parser('child')
    child_parser.add_argument('config', type=Path)
    child_parser.add_argument('phase', choices=('begin', 'resume'))
    run_parser = commands.add_parser('run')
    run_parser.add_argument('--output', type=Path, required=True)
    run_parser.add_argument('--live', action='store_true')
    run_parser.add_argument('--rehearsal', type=Path)
    run_parser.add_argument('--repetitions', type=int, default=2)
    run_parser.add_argument('--budget-ledger', type=Path, default=Path('runs/first-live-budget.sqlite'))
    run_parser.add_argument('--budget-units', type=int, default=20_000_000)
    run_parser.add_argument('--env-file', type=Path, default=Path('.env.local'))
    run_parser.add_argument('--image', default=DEFAULT_IMAGE)
    run_parser.add_argument('--validation-workers', type=int, default=2)
    run_parser.add_argument('--validation-cpus', type=int, default=2)
    run_parser.add_argument('--validation-memory-mib', type=int, default=512)
    run_parser.add_argument('--deterministic-visible', action='store_true',
                            help='Explicitly permit exact-source visible receipt reuse; changes the rehearsal contract')
    args = parser.parse_args()
    try:
        if args.command == 'child':
            child(args.config, args.phase)
        else:
            if not 0 < args.budget_units <= 50_000_000:
                raise ValueError('Budget cap must be positive and within the prior $50 authorization')
            options = vars(args).copy()
            options.pop('command')
            report = run(**options)
            print(json.dumps(dict(status=report['status'], accepted=sum(c['accepted'] for c in report['cases']),
                                  projects=len(report['cases']), budget=report['budget'])), flush=True)
    except Exception as error:
        print(f'Sustained v2 study stopped: {error}', file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == '__main__':
    with cancellation_signals():
        raise SystemExit(main())
