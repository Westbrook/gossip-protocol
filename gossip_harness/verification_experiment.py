"""Frozen maintenance study with checked probes and persisted-response recovery.

Candidate programs only execute through BlackboxValidator. Hidden evaluation is
strictly after a trajectory freezes. A crash can replay a saved response, never
an unknown provider outcome. The original studies and their sources are retained.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import random
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time

from .blackbox_validator import BlackboxValidator
from .gitstore import GitStore
from .ledger import Ledger
from .pilot import DEFAULT_IMAGE, LeaseKeeper, Trace
from .sustained_checkpoint import save_checkpoint
from .sustained_experiment import (CORE as PREVIOUS_CORE, checked_evaluate, credential,
                                  digest, local_promote, record_stage)
from .worker import MODEL, STRONG_MODEL, OpenAIWorker, WorkerFailure, WorkerRequest, WorkerResult


PROTOCOL = 'verification-quality-v1'
POLICIES = ('strong-reviewed', 'cheap-reviewed', 'portfolio-reviewed')
NEW_CORE = ('verification_experiment.py', 'verification_stage.py', 'verification_probes.py',
            'verification_journal.py', 'verification_integration.py',
            'verification_buildgraph.py', 'verification_calendar.py')
CORE = tuple(sorted(set(PREVIOUS_CORE + NEW_CORE)))
CASE_TIMEOUT = 12
SUITE_TIMEOUT = 300
OUTPUT_TOKENS = 12288
LEASE_SECONDS = 10


def fixture_modules():
    from . import verification_buildgraph, verification_calendar
    return (verification_buildgraph, verification_calendar)


def projects():
    return tuple(module.PROJECT for module in fixture_modules())


def fixture(project_id):
    return next(module for module in fixture_modules() if module.PROJECT['id'] == project_id)


def contract(image=DEFAULT_IMAGE):
    plan_path = Path(__file__).parent.parent / 'verification-study-plan.json'
    return dict(protocol=PROTOCOL,
                sources={name: hashlib.sha256((Path(__file__).parent / name).read_bytes()).hexdigest()
                         for name in CORE}, plan_sha256=hashlib.sha256(plan_path.read_bytes()).hexdigest(),
                fixture_sha256=digest(projects()), image=image, policies=list(POLICIES),
                controller_runtime=dict(python=sys.version, implementation=sys.implementation.name,
                    executable=sys.executable, platform=sys.platform,
                    git=subprocess.check_output(['git', '--version'], text=True).strip()),
                project_ids=[p['id'] for p in projects()], milestones=4,
                initial_candidates={'strong-reviewed': 1, 'cheap-reviewed': 1, 'portfolio-reviewed': 4},
                repair_calls=4, reviewer_calls=6, probes_per_review=4, new_probes_per_stage=8,
                case_timeout_seconds=CASE_TIMEOUT, suite_timeout_seconds=SUITE_TIMEOUT,
                output_tokens=OUTPUT_TOKENS,
                models={role: OpenAIWorker('profile-only', model=model, max_output_tokens=OUTPUT_TOKENS).profile_manifest()
                        for role, model in (('cheap', MODEL), ('strong', STRONG_MODEL))},
                completion='Four completed milestones and exact final source passing every public, active generated, and private case',
                fault='SIGKILL after stage-index-1 first reviewer response fsync before settlement; cached replay in new process',
                hidden_feedback=False, scope='Two synthetic maintenance applications; common review protocol, operational breadth comparison')


def cases_for(project, stage, kind):
    return [case for item in project['stages'][:stage + 1] for case in item[kind + '_cases']]


def save(path, value):
    return save_checkpoint(Path(path), value)


def read(path):
    return json.loads(Path(path).read_text())


def requirements(project, stage):
    return list(dict.fromkeys(item['id'] if isinstance(item, dict) else item
                             for s in project['stages'][:stage + 1] for item in s['requirements']))


class ReplayValidator:
    def __init__(self, receipt):
        self.receipt = receipt

    def evaluate(self, files, cases):
        return self.receipt


def verify_receipt(receipt, files, cases, image):
    # BlackboxValidator binds the executable test contract, not host-only probe
    # provenance. Full evidence metadata has its own stage/checkpoint digest.
    execution_cases = [{key: case[key] for key in ('input', 'expected', 'id', 'requirement')
                        if key in case} for case in cases]
    receipt = checked_evaluate(ReplayValidator(receipt), files, execution_cases)
    if (receipt.get('image_id') != image or receipt.get('case_timeout_seconds') != CASE_TIMEOUT
            or receipt.get('timeout_seconds') != SUITE_TIMEOUT):
        raise ValueError('Receipt execution limits or image changed')
    return receipt


def validate_state(state, expected_sha):
    if state.get('contract_sha') != expected_sha:
        raise ValueError('Checkpoint contract changed')
    n = state.get('next_stage')
    if type(n) is not int or not 0 <= n <= 4 or len(state.get('stages', [])) != n:
        raise ValueError('Checkpoint milestone history is inconsistent')
    if any(s.get('completed') is not True or type(s.get('stage_index')) is not int or s['stage_index'] != i
           for i, s in enumerate(state['stages'])):
        raise ValueError('Cannot skip an incomplete milestone')
    store = GitStore(state['store_path'])
    if store.head() != state['head'] or store.read_files() != state['files']:
        raise ValueError('Checkpoint Git/source binding changed')
    return store


def oracle_gate(module, project):
    from .verification_probes import parse_proposals, revalidate_cases

    def validate(envelope, stage_index, origin):
        common = dict(stage_index=stage_index, reference=module.reference,
                      validate_input=module.validate_input)
        if envelope['mode'] == 'revalidate':
            return revalidate_cases(envelope['probes'], origin=origin, **common)
        return parse_proposals(json.dumps({'probes': envelope['probes']}),
                               requirements=requirements(project, stage_index), origin=origin,
                               existing_cases=envelope['existing_cases'], max_new=envelope['max_new'],
                               namespace=project['id'], **common)
    return validate


def known_result(project, module, stage, metadata):
    """Offline-only proof worker. Never constructed by a live invocation."""
    if metadata['role'] == 'reviewer':
        alias = metadata['alias_by_slot']['slot-0']
        sample = {'commands': [{'op': f'__rehearsal_probe_stage_{stage}__'}]}
        probes = [dict(requirement=requirements(project, stage)[0], input=sample,
                       expected=module.reference(stage, sample))]
        if metadata['round'] == 1:
            probes.extend([dict(probes[0], input={'commands': [{'op': f'__wrong_label_stage_{stage}__'}]},
                                expected=['deliberately incorrect']), dict(probes[0])])
        review = dict(candidate=alias, action='repair' if metadata['round'] == 1 else 'accept',
                      notes='Repair the deliberate rehearsal fault' if metadata['round'] == 1 else 'Checked rehearsal source',
                      remaining=[requirements(project, stage)[0]] if metadata['round'] == 1 else [], probes=probes)
        return WorkerResult({'review.json': json.dumps(review)}, 'Offline verifier', 0, {'api_calls': 0})
    changes = {name: project['stages'][stage]['known_files'][name] for name in project['allowed_paths']}
    if metadata['round'] == 1 and metadata['candidate_id'] == 'slot-0':
        changes[project['allowed_paths'][-1]] = 'raise RuntimeError("deliberate rehearsal fault")\n'
    changes['notes.json'] = json.dumps(dict(notes='Retain the cumulative contract', remaining=[]))
    return WorkerResult(changes, 'Offline known implementation', 0, {'api_calls': 0})


def child(config_path, resumed=False):
    from .verification_integration import integrate_policy, receipt_path
    from .verification_journal import RequestJournal
    from .verification_stage import run_verification_stage

    config = read(config_path)
    root = Path(config['root'])
    if digest(contract(config['image'])) != config['contract_sha']:
        raise ValueError('Source or fixture changed after preregistration')
    module = fixture(config['project_id'])
    project = module.PROJECT
    ledger = Ledger(Path(config['budget_ledger']))
    validator = BlackboxValidator(config['image'], timeout_seconds=SUITE_TIMEOUT, case_timeout_seconds=CASE_TIMEOUT)
    trace = Trace(root / f'trace-{os.getpid()}.jsonl')
    state_path = root / 'state.json'
    if state_path.exists():
        state = read(state_path)
        expected = read(root / 'state-binding.json')['sha256']
        if hashlib.sha256(state_path.read_bytes()).hexdigest() != expected:
            raise ValueError('Checkpoint bytes changed')
        base = validate_state(state, config['contract_sha'])
    else:
        if resumed:
            raise ValueError('Cannot resume without a durable stage checkpoint')
        base = GitStore.create(root / 'initial.git', project['initial_files'])
        state = dict(contract_sha=config['contract_sha'], next_stage=0, stages=[],
                     files=project['initial_files'], store_path=str(base.path), head=base.head())
        save(root / 'state-binding.json', {'sha256': save(state_path, state)})
    if resumed:
        killed = read(root / 'fault-supervisor.json')
        if killed['returncode'] != -signal.SIGKILL or killed['previous_pid'] == os.getpid():
            raise ValueError('Resume must follow the verified death of a different process')
        save(root / 'resume.json', dict(previous_pid=killed['previous_pid'], resumed_pid=os.getpid(),
                                       verified=True, stage_index=state['next_stage'],
                                       initial_head=base.head(), checkpoint_sha256=digest(state)))
    models = {}
    if config['live']:
        key = credential(config['env_file'])
        models = {role: OpenAIWorker(key, model=model, max_output_tokens=OUTPUT_TOKENS, timeout=180)
                  for role, model in (('cheap', MODEL), ('strong', STRONG_MODEL))}
    for stage in range(state['next_stage'], 4):
        stage_root = root / f'stage-{stage}'
        stage_root.mkdir(exist_ok=resumed)
        (stage_root / 'requests').mkdir(exist_ok=resumed)
        updates = project['stages'][stage].get('trusted_updates', {})
        initial_files = dict(state['files'])
        if updates:
            destination = stage_root / 'upstream.git'
            if destination.exists():
                receipt = read(receipt_path(destination))
                base = GitStore(destination)
                if base.head() != receipt['merged_head'] or base.read_files() != {**initial_files, **updates}:
                    raise ValueError('Retained upstream integration changed')
            else:
                base, receipt = integrate_policy(GitStore(root / 'initial.git'), base, updates, destination)
            save(stage_root / 'integration.json', receipt)
            initial_files.update(updates)
        billing = config['namespace'] + f'/stage-{stage}'
        try:
            task = ledger.task(billing)
        except KeyError:
            ledger.add_task(billing)
            task = ledger.task(billing)
        if resumed and task['status'] == 'claimed':
            while time.time() <= ledger.task(billing)['expires']:
                time.sleep(0.1)
        journal = RequestJournal(stage_root / 'journal')
        lock = threading.Lock()
        fatal = threading.Event()
        calls = {}

        def emit(kind, /, **data):
            data.pop('kind', None)
            data.pop('stage_index', None)
            trace.emit(kind, stage_index=stage, **data)

        with LeaseKeeper(ledger, ttl=LEASE_SECONDS) as keeper:
            keeper.claim(billing, 'verification-controller')

            def invoke(*, role, model, files, instructions, allowed_paths, feedback, metadata):
                if fatal.is_set():
                    raise RuntimeError('A provider request requires investigation')
                if config['live'] and model not in models:
                    raise ValueError('Unknown model profile in live mode')
                call_id = f"{role}-{metadata['candidate_id']}-{metadata['round']}"
                reservation = billing + '/' + call_id
                request = WorkerRequest('task-' + digest(reservation)[:24], instructions,
                                        tuple(allowed_paths), files, base.head(), metadata['round'], feedback)
                request_path = stage_root / 'requests' / f'{call_id}.request.json'
                if request_path.exists() and read(request_path) != json.loads(json.dumps(asdict(request))):
                    raise ValueError('Replayed model request changed')
                save(request_path, asdict(request))
                worker = models[model] if config['live'] else None
                units = worker.reservation_units(request) if worker else 0

                def actual_call():
                    print(f"{config['run_id']} stage {stage + 1}: {call_id} ({model})", flush=True)
                    emit('provider_dispatch', call_id=call_id, reservation=reservation,
                         request_sha256=digest(asdict(request)), model=model, role=role)
                    return worker.run(request) if config['live'] else known_result(project, module, stage, metadata)

                def reserve():
                    with lock:
                        ledger.reserve(reservation, keeper.check(billing), units, now=time.time())

                def persisted():
                    if (not resumed and stage == 1 and role == 'reviewer' and metadata['round'] == 1):
                        with sqlite3.connect(Path(config['budget_ledger']).resolve().as_uri() + '?mode=ro', uri=True) as db:
                            other = db.execute("SELECT count(*) FROM reservations WHERE spent IS NULL AND id!=?", (reservation,)).fetchone()[0]
                        if other:
                            raise RuntimeError('Cannot inject a kill while another provider request is unsettled')
                        save(root / 'fault-ready.json', dict(pid=os.getpid(), stage_index=stage,
                             reservation=reservation, call_id=call_id, request_sha256=digest(asdict(request)),
                             journal_paths={k: str(v) for k, v in journal.paths(call_id).items()},
                             other_unsettled=0))
                        print(f"{config['run_id']}: response durably saved; waiting for supervisor SIGKILL", flush=True)
                        while True:
                            signal.pause()

                try:
                    result = journal.execute(call_id, request, reservation, actual_call, reserve,
                                             lambda amount: ledger.settle(reservation, amount), on_persisted=persisted)
                except WorkerFailure as error:
                    row = dict(call_id=call_id, reservation=reservation, role=role, model=model,
                               usage_units=error.usage_units, metadata=error.metadata, failure=str(error))
                    with lock:
                        calls[call_id] = row
                        save(stage_root / 'requests' / f'{call_id}.result.json', row)
                    if error.usage_units is None or error.metadata.get('halt'):
                        fatal.set()
                        raise RuntimeError('Provider failure requires investigation; no automatic retry') from None
                    return None
                row = {**asdict(result), 'call_id': call_id, 'reservation': reservation, 'role': role, 'model': model}
                with lock:
                    calls[call_id] = row
                    save(stage_root / 'requests' / f'{call_id}.result.json', row)
                return result

            def retain(files, label):
                safe = label.replace('/', '-')
                path = stage_root / (safe + '.git')
                saved = stage_root / (safe + '.json')
                if saved.exists():
                    row = read(saved)
                    store = GitStore(path)
                    if row['files'] != files or store.read_files(row['binding']['tip_sha']) != files:
                        raise ValueError('Retained replay proposal changed')
                    return row['binding']
                store = GitStore.fork(base, path)
                tip = store.propose({name: files[name] for name in project['allowed_paths']}, message=label)
                binding = dict(store_path=str(path), tip_sha=tip, files_sha256=digest(files))
                if store.read_files(tip) != files:
                    raise ValueError('Proposal content differs from retained Git tree')
                save(saved, dict(binding=binding, files=files))
                return binding

            def evaluate(files, cases, label):
                key = digest(dict(source=files, cases=cases))
                path = stage_root / 'evaluations' / (key + '.json')
                path.parent.mkdir(exist_ok=True)
                if path.exists():
                    return verify_receipt(read(path), files, cases, config['image'])
                result = validator.evaluate(files, cases)
                save(path, result)
                return verify_receipt(result, files, cases, config['image'])

            prior = state['stages'][-1] if state['stages'] else {}
            seed = int(digest([config['project_id'], config['repetition'], stage])[:16], 16)
            result = run_verification_stage(project, stage, config['policy'], initial_files, prior,
                    invoke=invoke, evaluate=evaluate, retain=retain, validate_probes=oracle_gate(module, project),
                    emit=emit, candidate_seed=seed)
            result['stage_index'] = stage
            result['invocations'] = [calls[key] for key in sorted(calls)]
            save(stage_root / 'result.json', result)
            record_stage(stage_root, result, ledger, keeper.check(billing))
        if not result['completed']:
            state['stages'].append(result)
            state['files'] = result['files']
            state['terminal'] = 'max_steps_incomplete'
            save(root / 'trajectory.json', state)
            return
        base = local_promote(base, result['selected_binding'], stage_root / 'checkpoint.git',
                             result['files'], project['allowed_paths'])
        state['stages'].append(result)
        state.update(files=result['files'], store_path=str(base.path), head=base.head(), next_stage=stage + 1)
        save(root / 'state-binding.json', {'sha256': save(state_path, state)})
    state['terminal'] = 'visible_complete'
    save(root / 'trajectory.json', state)


def supervise(root):
    """Kill only our child at its explicit persisted-response marker."""
    config = root / 'config.json'
    command = [sys.executable, '-m', 'gossip_harness.verification_experiment', 'child', str(config)]
    process = subprocess.Popen(command, cwd=Path(__file__).parent.parent)
    try:
        while process.poll() is None:
            marker_path = root / 'fault-ready.json'
            if marker_path.exists():
                marker = read(marker_path)
                if marker['pid'] != process.pid or marker['other_unsettled'] != 0:
                    raise ValueError('Fault marker is not bound to our quiescent child')
                from .verification_journal import RequestJournal
                paths = RequestJournal(root / 'stage-1' / 'journal').paths(marker['call_id'])
                if (marker['journal_paths'] != {k: str(v) for k, v in paths.items()}
                        or not paths['result'].is_file() or paths['settled'].exists()):
                    raise ValueError('Fault boundary must have a saved response and no settlement marker')
                config_data = read(config)
                with sqlite3.connect(Path(config_data['budget_ledger']).resolve().as_uri() + '?mode=ro', uri=True) as db:
                    db.row_factory = sqlite3.Row
                    unsettled = [dict(row) for row in db.execute('SELECT * FROM reservations WHERE spent IS NULL')]
                if len(unsettled) != 1 or unsettled[0]['id'] != marker['reservation']:
                    raise ValueError('Fault boundary must have exactly its known charge pending')
                process.kill()
                code = process.wait(timeout=15)
                if code != -signal.SIGKILL:
                    raise RuntimeError('Expected an actual SIGKILL exit')
                save(root / 'fault-supervisor.json', dict(previous_pid=process.pid, returncode=code,
                     marker_sha256=digest(marker), at=time.time(), reservation_at_kill=unsettled[0],
                     saved_result_sha256=hashlib.sha256(paths['result'].read_bytes()).hexdigest()))
                resumed = subprocess.run(command + ['--resumed'], cwd=Path(__file__).parent.parent)
                if resumed.returncode:
                    raise RuntimeError('Resumed controller stopped; inspect retained evidence without retrying paid work')
                return
            time.sleep(0.1)
        if process.returncode:
            raise RuntimeError('Controller stopped; inspect retained evidence without retrying paid work')
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=15)


def finalize(root, config, validator):
    project = fixture(config['project_id']).PROJECT
    state = read(root / 'trajectory.json')
    final_public = cases_for(project, 3, 'visible')
    full = len(state['stages']) == 4 and all(s['completed'] for s in state['stages'])
    # A stopped early-stage source is still scored against the full final public
    # and private contracts. Its earlier generated expectations may be obsolete;
    # only a stage-four pool has been checked under the final contract.
    active = state['stages'][-1].get('probe_pool', []) if len(state['stages']) == 4 else []
    def final_evaluate(kind, cases):
        receipt = validator.evaluate(state['files'], cases)
        save(root / ('final-' + kind + '-receipt.json'), receipt)
        return verify_receipt(receipt, state['files'], cases, config['image'])
    final_visible = final_evaluate('visible', final_public + active)
    final_private = final_evaluate('private', cases_for(project, 3, 'hidden'))
    accepted = full and final_visible['passed'] and final_private['passed']
    release = None
    if accepted:
        # The initial-to-final history also includes the trusted policy merge.
        checkpoint = GitStore(state['store_path'])
        allowed = tuple(project['allowed_paths']) + ('policy.json',)
        release = local_promote(GitStore(root / 'initial.git'),
                 dict(store_path=str(checkpoint.path), tip_sha=checkpoint.head()), root / 'release.git',
                 state['files'], allowed)
        for path, content in state['files'].items():
            target = root / 'accepted' / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content)
    pairs = list(zip(cases_for(project, 3, 'hidden'), final_private['outcomes']))
    coverage = {r: all(outcome['passed'] for case, outcome in pairs if case['requirement'] == r)
                for r in sorted({case['requirement'] for case, _ in pairs})}
    calls = [call for stage in state['stages'] for call in stage['invocations']]
    result = dict(run_id=config['run_id'], project_id=config['project_id'], policy=config['policy'],
         repetition=config['repetition'], root=str(root), accepted=accepted,
         status='accepted' if accepted else 'final_quality_failed' if full else state['terminal'],
         milestones_completed=sum(s['completed'] for s in state['stages']), requirement_coverage=coverage,
         files_sha256=digest(state['files']), trajectory_sha256=hashlib.sha256((root / 'trajectory.json').read_bytes()).hexdigest(),
         release_head=release.head() if release else None, final_visible=final_visible, final_hidden=final_private,
         fault=read(root / 'fault-supervisor.json') if (root / 'fault-supervisor.json').exists() else {},
         resume=read(root / 'resume.json') if (root / 'resume.json').exists() else {},
         integration=read(root / 'stage-3' / 'integration.json') if (root / 'stage-3' / 'integration.json').exists() else {},
         invocations=calls, usage_micro_usd=sum(c['usage_units'] or 0 for c in calls),
         metrics={key: sum(s['metrics'].get(key, 0) for s in state['stages'])
                  for key in ('builder_calls', 'reviewer_calls', 'repairs', 'invalid_reviews',
                              'invalid_builder_notes', 'invalid_source_proposals', 'blocked_acceptances',
                              'admitted_new_probes', 'generated_probe_attempts', 'rejected_probes',
                              'retired_probes', 'probe_failures_found', 'probe_discriminating_cases',
                              'acceptances_without_new_probes')})
    save(root / 'result.json', result)
    return result


def validate_rehearsal(proof, expected):
    wanted = {(p, policy, 0) for p in expected['project_ids'] for policy in POLICIES}
    rows = proof.get('cases', [])
    actual = [(r.get('project_id'), r.get('policy'), r.get('repetition')) for r in rows]
    def valid_row(row):
        fault, resume = row.get('fault', {}), row.get('resume', {})
        old, new = fault.get('previous_pid'), resume.get('resumed_pid')
        metrics = row.get('metrics', {})
        return (row.get('accepted') is True and type(row.get('repetition')) is int
                and type(old) is int and old > 0 and type(new) is int and new > 0 and old != new
                and resume.get('previous_pid') == old and resume.get('verified') is True
                and type(resume.get('stage_index')) is int and resume['stage_index'] == 1
                and type(fault.get('returncode')) is int and fault['returncode'] == -signal.SIGKILL
                and metrics.get('repairs', 0) >= 4 and metrics.get('admitted_new_probes', 0) >= 4
                and metrics.get('rejected_probes', 0) >= 8
                and row.get('integration', {}).get('stale_probe_status') == 'stale')
    if (proof.get('experiment') != PROTOCOL or proof.get('mode') != 'rehearsal'
            or proof.get('contract') != expected or proof.get('status') != 'finished'
            or proof.get('unexecuted') or proof.get('censored') or proof.get('active_case')
            or len(actual) != len(wanted) or set(actual) != wanted
            or any(not valid_row(r) for r in rows)):
        raise ValueError('Exact six-case rehearsal with real kills, replay, repairs and full acceptance is required')


def run(output, *, live=False, rehearsal=None, repetitions=3,
        budget_ledger=Path('runs/first-live-budget.sqlite'), budget_units=40_000_000,
        env_file=Path('.env.local'), image=DEFAULT_IMAGE):
    if type(repetitions) is not int or not 1 <= repetitions <= 3:
        raise ValueError('Repetitions must be one through three')
    expected = contract(image)
    if live:
        if not rehearsal:
            raise ValueError('Live work requires the exact passing rehearsal')
        validate_rehearsal(read(rehearsal), expected)
        credential(env_file)
    else:
        repetitions = 1
    output = Path(output).resolve()
    if output.exists():
        raise ValueError('Use a fresh output directory; evidence is never overwritten')
    validator = BlackboxValidator(image, timeout_seconds=SUITE_TIMEOUT, case_timeout_seconds=CASE_TIMEOUT)
    okay, detail = validator.preflight()
    if not okay:
        raise RuntimeError('Docker preflight failed: ' + detail)
    output.mkdir(parents=True)
    ledger_path = Path(budget_ledger).resolve() if live else output / 'budget.sqlite'
    ledger = Ledger(ledger_path, budget_units=budget_units if live else 0)
    before = ledger.budget()
    roster = [(p['id'], policy, rep) for rep in range(repetitions) for p in projects() for policy in POLICIES]
    random.Random(202609301).shuffle(roster)
    snapshot = output / 'source-snapshot' / 'gossip_harness'
    snapshot.mkdir(parents=True)
    for name in CORE:
        shutil.copyfile(Path(__file__).parent / name, snapshot / name)
    shutil.copyfile(Path(__file__).parent.parent / 'verification-study-plan.json', output / 'study-plan.json')
    save(output / 'fixtures.json', {'projects': projects()})
    save(output / 'preregistered.json', dict(contract=expected, roster=roster, budget_before=before))
    report = dict(experiment=PROTOCOL, mode='live' if live else 'rehearsal', contract=expected,
                  contract_sha=digest(expected), repetitions=repetitions, cases=[], unexecuted=roster.copy(),
                  censored=[], active_case=None, status='running', budget_before=before,
                  budget=before, started_at=time.time())
    save(output / 'results.json', report)
    namespace = PROTOCOL + '/' + digest(str(output))[:16]
    try:
        for project_id, policy, rep in roster:
            run_id = f'{project_id}-{policy}-{rep}'
            root = output / run_id
            root.mkdir()
            config = dict(root=str(root), contract_sha=digest(expected), image=image, project_id=project_id,
                          policy=policy, repetition=rep, run_id=run_id, live=live,
                          env_file=str(Path(env_file).resolve()), budget_ledger=str(ledger_path),
                          namespace=namespace + '/' + run_id)
            save(root / 'config.json', config)
            report['unexecuted'].remove((project_id, policy, rep))
            report['active_case'] = dict(run_id=run_id, project_id=project_id, policy=policy,
                                         repetition=rep, root=str(root), phase='controller')
            save(output / 'results.json', report)
            started = time.monotonic()
            supervise(root)
            report['active_case']['phase'] = 'final_evaluation'
            save(output / 'results.json', report)
            result = finalize(root, config, validator)
            result['elapsed_seconds'] = time.monotonic() - started
            save(root / 'result.json', result)
            report['cases'].append(result)
            report['active_case'] = None
            report['budget'] = ledger.budget()
            save(output / 'results.json', report)
            print(f"{run_id}: {result['status']}; milestones {result['milestones_completed']}/4", flush=True)
        report['status'] = 'finished'
    except (Exception, KeyboardInterrupt) as error:
        report.update(status='interrupted', failure=str(error) or type(error).__name__)
        if report['active_case'] is not None:
            report['censored'].append({**report['active_case'], 'reason': report['failure']})
            report['active_case'] = None
        raise
    finally:
        report['budget'] = ledger.budget()
        report['finished_at'] = time.time()
        report['incremental_micro_usd'] = report['budget']['spent_or_reserved'] - before['spent_or_reserved']
        save(output / 'results.json', report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    child_parser = commands.add_parser('child')
    child_parser.add_argument('config', type=Path)
    child_parser.add_argument('--resumed', action='store_true')
    runner = commands.add_parser('run')
    runner.add_argument('--output', type=Path, required=True)
    runner.add_argument('--live', action='store_true')
    runner.add_argument('--rehearsal', type=Path)
    runner.add_argument('--repetitions', type=int, default=3)
    runner.add_argument('--budget-ledger', type=Path, default=Path('runs/first-live-budget.sqlite'))
    runner.add_argument('--budget-units', type=int, default=40_000_000)
    runner.add_argument('--env-file', type=Path, default=Path('.env.local'))
    runner.add_argument('--image', default=DEFAULT_IMAGE)
    args = parser.parse_args()
    try:
        if args.command == 'child':
            child(args.config, args.resumed)
        else:
            if not 0 < args.budget_units <= 50_000_000:
                raise ValueError('Cumulative budget must remain within existing authorization')
            options = vars(args).copy()
            options.pop('command')
            result = run(**options)
            print(json.dumps(dict(status=result['status'], accepted=sum(c['accepted'] for c in result['cases']),
                                  projects=len(result['cases']), budget=result['budget'])), flush=True)
    except Exception as error:
        print(f'Verification study stopped: {error}', file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
