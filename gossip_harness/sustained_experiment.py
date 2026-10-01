"""Quality-first cumulative projects with a settled, cross-process handoff.

The controller persists bounded patch agents. This is not a transport comparison
or evidence of unconstrained autonomous persistence. Hidden checks run only after
the complete project trajectory has been frozen and never enter model feedback.
"""
from __future__ import annotations

from dataclasses import asdict
import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import re
import shutil
import sqlite3
import subprocess
import sys
import threading
import time

from .blackbox_validator import BlackboxValidator, _canonical
from .gitstore import GitStore
from .ledger import Ledger
from .pilot import DEFAULT_IMAGE, LeaseKeeper, Trace, _save
from .promotion import PromotionCoordinator
from .sustained_stage import POLICIES, run_stage
from .worker import MODEL, STRONG_MODEL, OpenAIWorker, WorkerFailure, WorkerRequest, WorkerResult

PROTOCOL = 'sustained-quality-v1'
CORE = ('sustained_experiment.py', 'sustained_stage.py', 'sustained_checkpoint.py',
        'sustained_queue.py', 'sustained_inventory.py', 'blackbox_validator.py',
        'sandbox.py', 'worker.py', 'ledger.py', 'gitstore.py', 'promotion.py', 'pilot.py')


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=True,
                                     allow_nan=False, separators=(',', ':')).encode()).hexdigest()


def projects():
    from .sustained_queue import PROJECT as queue
    from .sustained_inventory import PROJECT as inventory
    return (queue, inventory)


def contract(image=DEFAULT_IMAGE):
    return dict(protocol=PROTOCOL, sources={p: hashlib.sha256((Path(__file__).parent / p).read_bytes()).hexdigest()
                                          for p in CORE},
                fixture_sha256=digest(projects()), image=image, policies=list(POLICIES),
                project_ids=[p['id'] for p in projects()], milestones=3,
                baseline_calls_per_stage=8, portfolio_initial=4, portfolio_repairs=4,
                reviewer_calls_per_stage=5, case_timeout_seconds=12, suite_timeout_seconds=300,
                models={role: OpenAIWorker('profile-only', model=model, max_output_tokens=8192).profile_manifest()
                        for role, model in (('cheap', MODEL), ('strong', STRONG_MODEL))},
                completion='All three milestones and all final cumulative visible and hidden cases pass at exact release commit',
                hidden_feedback=False, intermediate_hidden='Retrospective diagnostic; earlier defects may be repaired',
                interruption='Settled controller exit after milestone two, new PID resumes exact Git checkpoint',
                scope='Bounded stateless patch workers with persistent controller; no gossip transport comparison',
                metrics_order=['final requirement coverage', 'whole-project completion', 'regressions and recovery',
                               'premature completion and stagnation', 'cost', 'elapsed time'])


def cases_for(project, stage, kind):
    return [case for item in project['stages'][:stage + 1] for case in item[kind + '_cases']]


def credential(path):
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        raise ValueError('Approved credential file is missing or is a symlink')
    values = []
    for line in path.read_text().splitlines():
        match = re.match(r'^\s*(?:export\s+)?OPENAI_API_KEY\s*=\s*(.*?)\s*$', line)
        if match:
            values.append(match.group(1).strip().strip('\"\''))
    if len(values) != 1 or not values[0].startswith('sk-'):
        raise ValueError('Expected one configured key; its value will not be displayed')
    return values[0]


def checked_evaluate(validator, files, cases):
    receipt = validator.evaluate(files, cases)
    outcomes = receipt.get('outcomes', [])
    if (receipt.get('status') not in ('passed', 'failed') or receipt.get('cleanup_verified') is not True
            or len(outcomes) != len(cases) or [o.get('index') for o in outcomes] != list(range(len(cases)))
            or any(type(o.get('index')) is not int for o in outcomes)
            or any(type(o.get('passed')) is not bool for o in outcomes)
            or type(receipt.get('passed')) is not bool
            or receipt.get('passed') != all(o['passed'] for o in outcomes)
            or receipt.get('status') != ('passed' if receipt['passed'] else 'failed')
            or receipt.get('source_sha256') != hashlib.sha256(_canonical(files).encode()).hexdigest()
            or receipt.get('suite_sha256') != hashlib.sha256(_canonical(cases).encode()).hexdigest()):
        raise RuntimeError('Sandbox infrastructure or validation receipt failed; study stopped')
    return receipt


def promote_files(base, binding, destination, files, ledger, lease, *, allowed_paths):
    """Publish only the exact tree covered by an already obtained receipt."""
    store = GitStore.fork(base, destination)
    expected = digest(files)
    def gate(checkout):
        actual = {str(p.relative_to(checkout)): p.read_text() for p in checkout.rglob('*')
                  if p.is_file() and '.git' not in p.relative_to(checkout).parts}
        return digest(actual) == expected, 'Exact immutable source binding to prior sandbox receipt'
    prepared = store.prepare(GitStore(Path(binding['store_path'])), binding['tip_sha'],
                             expected_head=store.head(), validator=gate, allowed_paths=tuple(allowed_paths))
    outcome = PromotionCoordinator(ledger).promote(store, prepared, [lease], now=time.time())
    if outcome.status != 'accepted' or store.read_files() != files:
        raise RuntimeError('Exact-tree publication failed')
    return store


def local_promote(base, binding, destination, files, allowed_paths):
    ledger = Ledger(destination.with_suffix('.sqlite'), budget_units=0)
    ledger.add_task('exact-tree')
    lease = ledger.claim('exact-tree', 'controller', now=time.time(), ttl=600)
    return promote_files(base, binding, destination, files, ledger, lease, allowed_paths=allowed_paths)


def record_stage(root, result, ledger, lease):
    """A completed accounting record is not a project acceptance certificate."""
    files = {'record.json': json.dumps({'completed': result['completed'], 'status': result['status'],
                                      'selected_binding': result['selected_binding']}, sort_keys=True)}
    base = GitStore.create(root / 'record-base.git', {'record.json': '{}'})
    proposal = GitStore.fork(base, root / 'record-proposal.git')
    tip = proposal.propose(files)
    promote_files(base, dict(store_path=str(proposal.path), tip_sha=tip), root / 'record.git', files,
                  ledger, lease, allowed_paths=('record.json',))


def known_result(project, stage, metadata, files):
    if metadata['role'] == 'reviewer':
        # Deliberately select the broken c0 once, forcing a complete review/repair loop.
        first = metadata['round'] == 1
        review = dict(candidate='c0', action='repair' if first else 'accept',
                      notes='Repair the deliberate rehearsal fault' if first else 'Rehearsal accepted',
                      remaining=list(project['stages'][stage]['requirements']) if first else [])
        return WorkerResult({'review.json': json.dumps(review)}, 'Offline reviewer', 0, {'api_calls': 0})
    bad = metadata['round'] == 1 and metadata['candidate_id'] in ('single', 'c0')
    changes = {p: project['stages'][stage]['known_files'][p] for p in project['allowed_paths']}
    if bad:
        # Keep exact trusted scaffold and break only a backend file.
        changes[project['allowed_paths'][-1]] = 'raise RuntimeError("deliberate rehearsal fault")\n'
    changes['control.json'] = json.dumps(dict(action='complete', notes='Durable milestone notes', remaining=[]))
    return WorkerResult(changes, 'Offline known implementation with deliberate first failure', 0, {'api_calls': 0})


def child(config_path, phase):
    """One controller lifetime. Exit/resume deliberately occurs between stages."""
    from .sustained_checkpoint import load_checkpoint, save_checkpoint
    config = json.loads(Path(config_path).read_text())
    root = Path(config['root'])
    expected = contract(config['image'])
    if digest(expected) != config['contract_sha']:
        raise ValueError('Source or fixture changed after preregistration')
    project = next(p for p in projects() if p['id'] == config['project_id'])
    ledger = Ledger(Path(config['budget_ledger']))
    validator = BlackboxValidator(config['image'], timeout_seconds=300, case_timeout_seconds=12)
    trace = Trace(root / f'trace-{phase}.jsonl')
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
    models = {}
    if config['live']:
        key = credential(config['env_file'])
        models = {role: OpenAIWorker(key, model=model, max_output_tokens=8192, timeout=180)
                  for role, model in (('cheap', MODEL), ('strong', STRONG_MODEL))}
    start = state['next_stage']
    stop = 2 if phase == 'begin' else 3
    for stage in range(start, stop):
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
                    ledger.reserve(reservation, keeper.check(billing), units, now=time.time())
                _save(stage_root / 'requests' / (call_id + '.request.json'), asdict(request))
                emit('request_started', call_id=call_id, model=model, reservation=reservation,
                     request_sha256=digest(asdict(request)), reserved_units=units)
                began = time.monotonic()
                print(f"{config['run_id']} stage {stage + 1}: {call_id} ({model})", flush=True)
                try:
                    result = worker.run(request) if config['live'] else known_result(project, stage, metadata, files)
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
                return result

            def retain(files, label):
                store = GitStore.fork(base, stage_root / (label + '.git'))
                tip = store.propose({p: files[p] for p in project['allowed_paths']}, message=label)
                binding = dict(store_path=str(store.path), tip_sha=tip, files_sha256=digest(files))
                if store.read_files(tip) != files:
                    raise RuntimeError('Retained proposal differs from supplied source')
                _save(stage_root / (label + '.json'), dict(binding=binding, files=files))
                return binding

            def evaluate(files, label):
                suite = cases_for(project, stage, 'visible')
                receipt = checked_evaluate(validator, files, suite)
                _save(stage_root / (label + '.validation.json'), receipt)
                emit('visible_validation', label=label, files_sha256=digest(files),
                     cases_sha256=digest(suite), receipt=receipt)
                return receipt

            prior = state['stages'][-1] if state['stages'] else {}
            result = run_stage(project, stage, config['policy'], state['files'], prior,
                               invoke=invoke, evaluate=evaluate, retain=retain, emit=emit)
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


def validate_rehearsal(proof, expected):
    wanted = {(p, a, 0) for p in expected['project_ids'] for a in POLICIES}
    actual = [(c.get('project_id'), c.get('policy'), c.get('repetition')) for c in proof.get('cases', [])]
    if (proof.get('experiment') != PROTOCOL or proof.get('mode') != 'rehearsal'
            or proof.get('contract') != expected or proof.get('status') != 'finished'
            or proof.get('unexecuted') or set(actual) != wanted or len(actual) != len(wanted)
            or not all(c.get('accepted') and c.get('handoff', {}).get('verified')
                       and c['handoff']['previous_pid'] != c['handoff']['resumed_pid']
                       and c.get('rehearsal_repair_verified') for c in proof.get('cases', []))):
        raise ValueError('Complete exact six-project Docker rehearsal with repairs and process handoffs required')


def finalize_case(root, project, config, validator):
    state = json.loads((root / 'trajectory.json').read_text())
    trajectory_sha = hashlib.sha256((root / 'trajectory.json').read_bytes()).hexdigest()
    # All provider work for this project is now irrevocably finished.
    final_visible = checked_evaluate(validator, state['files'], cases_for(project, 2, 'visible'))
    final_hidden = checked_evaluate(validator, state['files'], cases_for(project, 2, 'hidden'))
    historical = []
    for stage, result in enumerate(state['stages']):
        historical.append(dict(stage_index=stage, binding=result['selected_binding'],
                               hidden=checked_evaluate(validator, result['files'], cases_for(project, stage, 'hidden'))))
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
                  usage_micro_usd=sum(i['usage_units'] or 0 for i in state['invocations']),
                  invocations=state['invocations'], root=str(root),
                  rehearsal_repair_verified=not config['live'] and metrics['repairs'] >= 3 and metrics['premature_completion'] >= 3)
    _save(root / 'result.json', result)
    return result


def run(output, *, live=False, rehearsal=None, repetitions=2, budget_ledger=None,
        budget_units=20_000_000, env_file=Path('.env.local'), image=DEFAULT_IMAGE):
    if type(repetitions) is not int or not 1 <= repetitions <= 4:
        raise ValueError('Repetitions must be one through four')
    expected = contract(image)
    if live:
        if not rehearsal:
            raise ValueError('Live mode requires an exact rehearsal')
        validate_rehearsal(json.loads(Path(rehearsal).read_text()), expected)
        credential(env_file)  # Validate silently before creating result directories.
    else:
        repetitions = 1
    output = Path(output).resolve()
    if output.exists():
        raise ValueError('Output directory already exists; retained results are never overwritten')
    validator = BlackboxValidator(image, timeout_seconds=300, case_timeout_seconds=12)
    okay, detail = validator.preflight()
    if not okay:
        raise RuntimeError('Docker preflight failed: ' + detail)
    output.mkdir(parents=True)
    ledger_path = Path(budget_ledger).resolve() if live and budget_ledger else output / 'budget.sqlite'
    ledger = Ledger(ledger_path, budget_units=budget_units if live else 0)
    before = ledger.budget()
    snapshot = output / 'source-snapshot' / 'gossip_harness'
    snapshot.mkdir(parents=True)
    for name in CORE:
        shutil.copyfile(Path(__file__).parent / name, snapshot / name)
    roster = [(p['id'], policy, repetition) for repetition in range(repetitions)
              for p in projects() for policy in POLICIES]
    random.Random(20260930).shuffle(roster)
    report = dict(schema_version=1, experiment=PROTOCOL, mode='live' if live else 'rehearsal',
                  contract=expected, contract_sha=digest(expected), repetitions=repetitions,
                  cases=[], unexecuted=[list(x) for x in roster], status='running', budget_before=before,
                  budget=before, started_at=time.time())
    _save(output / 'preregistered.json', dict(contract=expected, roster=roster,
                                           budget_cap_micro_usd=before['limit']))
    _save(output / 'fixtures.json', projects())
    _save(output / 'results.json', report)
    namespace = PROTOCOL + '/' + digest(str(output))[:16]
    try:
        for project_id, policy, repetition in roster:
            run_id = f'{project_id}-{policy}-{repetition}'
            root = output / run_id
            root.mkdir()
            config = dict(root=str(root), contract_sha=digest(expected), image=image, project_id=project_id,
                          policy=policy, repetition=repetition, run_id=run_id, live=live,
                          env_file=str(Path(env_file).resolve()), budget_ledger=str(ledger_path),
                          namespace=namespace + '/' + run_id)
            _save(root / 'config.json', config)
            began = time.monotonic()
            for phase in ('begin', 'resume'):
                if phase == 'resume' and (root / 'trajectory.json').exists():
                    break  # An incomplete earlier milestone cannot be skipped.
                execution = subprocess.run([sys.executable, '-m', 'gossip_harness.sustained_experiment', 'child', str(root / 'config.json'), phase],
                                           cwd=Path(__file__).parent.parent)
                if execution.returncode:
                    raise RuntimeError(f'Controller {run_id}/{phase} stopped; inspect retained receipts before retrying')
            project = next(p for p in projects() if p['id'] == project_id)
            result = finalize_case(root, project, config, validator)
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
        print(f'Sustained study stopped: {error}', file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
