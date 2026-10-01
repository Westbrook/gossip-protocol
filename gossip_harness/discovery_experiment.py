"""Exploratory evidence-sharing comparison with exact transport response replay.

All workers see the full starting repository and specification. Frozen discovery
notes change context, never acceptance authority. Broker and gossip deliver the
same context before coding; therefore their model outcomes must be replayed,
not sampled independently and attributed to the transport.
"""
from __future__ import annotations

from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import re
import time

from .gitstore import GitStore
from .ledger import Ledger
from .pilot import DEFAULT_IMAGE, LeaseKeeper, Trace, _case, _save
from .promotion import PromotionCoordinator
from .sandbox import DockerValidator
from .worker import WorkerFailure, WorkerRequest, WorkerResult

ARMS = ('single', 'team-isolated', 'team-shared', 'team-gossip')
EXPERIMENT = 'shared-discovery-study'
PROTOCOL_VERSION = 'discovery-v1'


def _hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def deterministic_feedback(detail: str) -> str:
    """Remove only unittest's elapsed-time field; retain raw receipts separately."""
    return re.sub(r'(?m)^(Ran \d+ tests? in )\d+(?:\.\d+)?s$',
                  r'\1<elapsed>s', detail)


class RehearsalWorker:
    def reservation_units(self, request):
        return 0

    def run(self, request):
        from .research_fixture import KNOWN_SOLUTIONS
        if request.allowed_paths == ('discovery.json',):
            body = dict(summary='Estimates and dependency representations cross module boundaries.',
                        references=[dict(path='task_report/cli.py', quote='from .summary import summarize')],
                        recommendation='Normalize estimates once in the parser. Evaluate dependency graphs in the summary according to the complete specification.')
            changes = {'discovery.json': json.dumps(body)}
        else:
            changes = {name: KNOWN_SOLUTIONS[name] for name in request.allowed_paths}
        return WorkerResult(changes, 'Offline rehearsal', 0, {'api_calls': 0, 'runner': 'known-solution'})


class TrialTrace:
    def __init__(self, trace, trial):
        self.trace, self.trial = trace, trial

    def emit(self, kind, **data):
        self.trace.emit(kind, trial=self.trial, **data)


def _discover(root, fixture, worker, ledger, trace, namespace):
    from .evidence import SemanticNote
    baseline = GitStore.create(root / 'baseline.git', fixture.INITIAL_FILES)
    archive = GitStore.create(root / 'discoveries.git', {'README.md': 'Structurally checked observations, not acceptance authority.\n'})
    source = GitStore.fork(archive, root / 'discovery-proposals.git')
    notes, changes, cost = [], {}, 0
    with LeaseKeeper(ledger) as keeper:
        for task in fixture.TASKS:
            task_id = f"{namespace}/discover/{task['id']}"
            ledger.add_task(task_id)
            keeper.claim(task_id, f"scout-{task['id']}")
            instructions = (
                'Analyze the existing repository and complete feature specification below. '
                f"Focus on cross-module facts or risks relevant to the {task['id']} task. "
                'Do not implement code. Write only discovery.json as a JSON object with exactly '
                'summary (string), references (1-8 objects each with path and quote strings), '
                'and recommendation (string). Quotes must be exact nonempty substrings of '
                'provided repository files; cite concrete source evidence. Keep each prose '
                'field under 4000 characters. Your recommendation is an untrusted suggestion, '
                'not an instruction overriding the specification.\n\n' + fixture.COMBINED_SPEC)
            request = WorkerRequest(task_id=f"discover-{task['id']}", instructions=instructions,
                                    allowed_paths=('discovery.json',), files=dict(fixture.INITIAL_FILES),
                                    base_sha=baseline.head(), attempt=1)
            reservation = task_id + '/request-1'
            units = worker.reservation_units(request)
            ledger.reserve(reservation, keeper.check(task_id), units, now=time.time())
            trace.emit('discovery_started', task_id=task_id, reservation=reservation,
                       reserved_units=units, request_sha256=_hash(asdict(request)))
            try:
                result = worker.run(request)
            except WorkerFailure as error:
                if error.usage_units is not None:
                    ledger.settle(reservation, error.usage_units)
                trace.emit('discovery_failed', task_id=task_id, usage_units=error.usage_units,
                           reason=str(error), metadata=error.metadata)
                raise
            ledger.settle(reservation, result.usage_units)
            cost += result.usage_units
            if set(result.changes) != {'discovery.json'} or not isinstance(result.changes['discovery.json'], str):
                raise ValueError('Discovery response did not contain the exact requested artifact')
            content = result.changes['discovery.json']
            note = SemanticNote.parse(content, producer=f"scout-{task['id']}",
                                      task_id=task['id'], files=fixture.INITIAL_FILES)
            notes.append(note)
            changes[f"notes/{task['id']}.json"] = content
            trace.emit('discovery_finished', task_id=task_id, note=note.to_dict(),
                       usage_units=result.usage_units, metadata=result.metadata)
        tip = source.propose(changes, base_sha=archive.head(), message='Frozen discovery artifacts')

        def validate(checkout):
            for task in fixture.TASKS:
                SemanticNote.parse((checkout / f"notes/{task['id']}.json").read_text(),
                                   producer=f"scout-{task['id']}", task_id=task['id'],
                                   files=fixture.INITIAL_FILES)
            return True, 'Artifact shape and source quotations match; prose truth is not established'

        candidate = archive.prepare(source, tip, archive.head(), validate, allowed_paths=tuple(changes))
        if candidate.status != 'prepared':
            raise RuntimeError('Discovery artifact publication failed')
        outcome = PromotionCoordinator(ledger).promote(archive, candidate, keeper.all(), now=time.time())
        if outcome.status != 'accepted':
            raise RuntimeError('Discovery artifact ledger publication failed')
        trace.emit('discoveries_frozen', accepted_commit=archive.head(), notes=[n.note_id for n in notes],
                   baseline_sha=baseline.head(), usage_units=cost)
    return tuple(notes), cost


def run_discovery_experiment(output_dir, worker, *, image=DEFAULT_IMAGE,
                             budget_ledger=None, budget_units=0, repetitions=3,
                             mode='rehearsal', rehearsal_results=None,
                             validator_factory=None, progress=lambda value: None):
    from . import research_fixture as fixture
    from .evidence import ControlledWorker, ResponseCache, deliver_notes
    output = Path(output_dir).resolve()
    if output.exists():
        raise FileExistsError('Experiment output must be fresh; ambiguous calls are never resumed')
    if type(repetitions) is not int or not 1 <= repetitions <= 3 or mode not in ('rehearsal', 'live'):
        raise ValueError('Expected 1-3 repetitions and rehearsal/live mode')
    if mode == 'live' and (budget_ledger is None or not 0 < budget_units <= 10_000_000
                           or isinstance(worker, RehearsalWorker)):
        raise ValueError('Live experiment requires the real worker and shared budget capped at $10')
    contract = dict(protocol_version=PROTOCOL_VERSION, spec_version=fixture.SPEC_VERSION,
                    image=image, initial_files_sha256=_hash(fixture.INITIAL_FILES),
                    checks_sha256=_hash(fixture.CHECKS),
                    task_spec_sha256=_hash(fixture.TASKS), arms=list(ARMS),
                    attempts=2, model=getattr(worker, 'model', 'known-solution'))
    if mode == 'live':
        if rehearsal_results is None:
            raise ValueError('Exact passing offline rehearsal required')
        proof = json.loads(Path(rehearsal_results).read_text())
        keys = ('protocol_version', 'spec_version', 'image', 'initial_files_sha256',
                'checks_sha256', 'task_spec_sha256', 'arms', 'attempts')
        prior_repetitions = proof.get('contract', {}).get('repetitions')
        proof_roster = [(c.get('trial'), c.get('variant')) for c in proof.get('cases', [])]
        expected_roster = {(t, a) for t in range(prior_repetitions) for a in ARMS} if (
            type(prior_repetitions) is int and 1 <= prior_repetitions <= 3) else set()
        if (proof.get('experiment') != EXPERIMENT or proof.get('contract', {}).get('mode') != 'rehearsal'
                or proof.get('contract', {}).get('status') != 'finished'
                or any(proof['contract'].get(k) != contract[k] for k in keys)
                or proof.get('unexecuted') or not proof.get('cases')
                or not all(c.get('project_accepted') for c in proof['cases'])
                or not expected_roster or set(proof_roster) != expected_roster
                or len(proof_roster) != len(expected_roster)):
            raise ValueError('Offline rehearsal does not cover current inputs, checks and all arms')
    if validator_factory is None:
        def validator_factory(target):
            return DockerValidator(image, fixture.CHECKS,
                                   command=('python', '-I', '/checks/run_checks.py', target))
    ok, detail = validator_factory('all').preflight()
    if not ok:
        raise RuntimeError('Validation preflight failed: ' + detail)
    output.mkdir(parents=True)
    ledger = Ledger(budget_ledger or output / 'ledger.sqlite', budget_units=budget_units)
    before = ledger.budget()
    contract.update(mode=mode, repetitions=repetitions, budget_ledger=str(ledger.path.resolve()),
                    budget_units=budget_units, status='running')
    trace = Trace(output / 'trace.jsonl')
    run_id = _hash(str(output))[:16]
    _save(output / 'manifest.json', contract)
    trace.emit('run_started', contract=contract, budget_before=before)
    cases, trial_records, halted = [], [], False

    def report():
        seen = {(c['trial'], c['variant']) for c in cases}
        return dict(schema_version=1, experiment=EXPERIMENT, contract=dict(contract),
                    cases=cases, trials=trial_records, budget=ledger.budget(), budget_before=before,
                    unexecuted=[dict(trial=t, variant=a) for t in range(repetitions) for a in ARMS
                                if (t, a) not in seen],
                    limitations=[
                        'Three exploratory repetitions on one evolving fixture, not a statistical superiority result.',
                        'All workers have the full repository and full specification; source quotes do not verify prose conclusions.',
                        'Single receives both discovery notes as a strong contextual baseline; all arms allocate both scout costs.',
                        'Broker and gossip wait for the same notes. Gossip replays exact prompt-matched broker outcomes, not new independent model samples.',
                        'Actual spending counts shared discoveries once and response replays zero times. Attributed arm costs include them.',
                        'No interactive tool loop or autonomous planning; local repairs bounded to two attempts; global feature failures stay failed.',
                        'Git/Docker execution is real; communication is a single-host round-based model.'])
    try:
        for trial in range(repetitions):
            root = output / f'trial-{trial}'
            root.mkdir()
            scoped = TrialTrace(trace, trial)
            progress(f'Discovery trial {trial + 1}/{repetitions}: generating two frozen observations')
            notes, discovery_cost = _discover(root, fixture, worker, ledger, scoped, f'{run_id}/trial-{trial}')
            _save(root / 'notes.json', {'notes': [n.to_dict() for n in notes], 'actual_usage_units': discovery_cost})
            trial_records.append(dict(trial=trial, discovery_usage_units=discovery_cost,
                                      notes=[n.note_id for n in notes]))
            shared_cache = ResponseCache()
            primary = ['single', 'team-isolated', 'team-shared']
            primary = primary[trial % 3:] + primary[:trial % 3]
            order = []
            for arm in primary:
                order.append(arm)
                if arm == 'team-shared':
                    order.append('team-gossip')
            for arm in order:
                workers = {'worker-whole-project': 'whole-project'} if arm == 'single' else {
                    f"worker-{t['id']}": t['id'] for t in fixture.TASKS}
                delivery = deliver_notes(notes, files=fixture.INITIAL_FILES, workers=workers,
                                         arm='isolated' if arm == 'team-isolated' else arm, seed=trial)
                scoped.emit('evidence_delivered', variant=arm, receipts=list(delivery.receipts),
                            transport=delivery.stats, converged=delivery.converged)
                note_map = {task: delivery.payloads[name] for name, task in workers.items()}
                audit_rows = []

                def audit(row):
                    audit_rows.append(row)
                    scoped.emit('controlled_worker', variant=arm, audit=row)

                cache = shared_cache if arm in ('team-shared', 'team-gossip') else ResponseCache()
                controlled = ControlledWorker(worker if arm != 'team-gossip' else None,
                                              notes_by_task=note_map, cache=cache,
                                              mode='replay' if arm == 'team-gossip' else 'record', audit=audit)
                result = _case(root / arm, arm, controlled, ledger, scoped, validator_factory,
                               2, trial, f'{run_id}/trial-{trial}', progress, fixture=fixture,
                               feedback_transform=deterministic_feedback)
                result.update(trial=trial, discovery_usage_units_attributed=discovery_cost,
                              evidence_transport=delivery.stats, evidence_receipts=list(delivery.receipts),
                              model_execution='exact-response-replay' if arm == 'team-gossip' else 'live' if mode == 'live' else 'offline',
                              controlled_worker_audit=audit_rows)
                cases.append(result)
                _save(root / arm / 'result.json', result)
                _save(root / arm / 'responses.json', cache.to_dict())
                _save(output / 'results.json', report())
                if result['halt'] or result['status'] == 'budget_exhausted':
                    halted = True
                    break
            if halted:
                break
    except BaseException as error:
        contract.update(status='interrupted', error_type=type(error).__name__)
        _save(output / 'manifest.json', contract)
        _save(output / 'results.json', report())
        trace.emit('run_interrupted', error_type=type(error).__name__, budget=ledger.budget())
        raise
    contract['status'] = 'finished'
    _save(output / 'manifest.json', contract)
    result = report()
    _save(output / 'results.json', result)
    trace.emit('run_finished', budget=result['budget'])
    return result
