"""Bounded end-to-end coding pilot; fixed tasks, deterministic integration authority.

This is a small practical smoke test, not a productivity benchmark. Local passing
proposals do not complete tasks: the entire fixed project must pass before the
journaled release and all of its task attempts are committed together.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import AbstractContextManager
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import threading
import time
from typing import Callable

from .gitstore import GitStore
from .ledger import BudgetExceeded, ClaimRejected, Ledger
from .promotion import PromotionCoordinator
from .transport import Event, Mesh
from .worker import WorkerFailure, WorkerRequest, WorkerResult

VARIANTS = ('single', 'maintainers-bus', 'maintainers-gossip')
DEFAULT_IMAGE = 'sha256:f77ac9e44ae96ef2c90b8053ea08c31f8be030f824196b0ae4db6d462c84e51f'


def _save(path: Path, value: dict) -> None:
    temporary = path.with_suffix(path.suffix + '.tmp')
    with temporary.open('w') as handle:
        json.dump(value, handle, indent=2, sort_keys=True)
        handle.write('\n')
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


class Trace:
    def __init__(self, path: Path):
        self.path, self.lock, self.index = path, threading.Lock(), 0

    def emit(self, kind: str, **data) -> None:
        with self.lock:
            self.index += 1
            row = dict(index=self.index, kind=kind, at=time.time(), **data)
            with self.path.open('a') as handle:
                handle.write(json.dumps(row, sort_keys=True) + '\n')
                handle.flush()
                os.fsync(handle.fileno())


class LeaseKeeper(AbstractContextManager):
    """Renew staged task attempts while other workers and Git gates are running."""
    def __init__(self, ledger: Ledger, ttl: float = 600):
        self.ledger, self.ttl = ledger, ttl
        self.leases, self.errors = {}, []
        self.lock, self.stop = threading.Lock(), threading.Event()
        self.thread = threading.Thread(target=self._heartbeat, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def claim(self, task_id: str, worker: str):
        with self.lock:
            lease = self.ledger.claim(task_id, worker, now=time.time(), ttl=self.ttl)
            self.leases[task_id] = lease
            return lease

    def check(self, task_id: str):
        with self.lock:
            if self.errors:
                raise ClaimRejected('Lease heartbeat failed; refusing publication')
            lease = self.leases[task_id]
            self.ledger.validate(lease, now=time.time())
            return lease

    def all(self):
        return [self.check(task) for task in sorted(self.leases)]

    def _heartbeat(self):
        while not self.stop.wait(min(15, self.ttl / 3)):
            with self.lock:
                for task, lease in list(self.leases.items()):
                    try:
                        state = self.ledger.task(task)['status']
                        if state in ('complete', 'submitting'):
                            continue
                        self.leases[task] = self.ledger.renew(lease, now=time.time(), ttl=self.ttl)
                    except Exception as error:
                        self.errors.append(type(error).__name__)
                        return

    def __exit__(self, *exc):
        self.stop.set()
        self.thread.join(timeout=5)


class KnownSolutionWorker:
    """Offline rehearsal only; solutions are never part of live request context."""
    def reservation_units(self, request):
        return 0

    def run(self, request):
        from .pilot_fixture import KNOWN_SOLUTIONS
        return WorkerResult(
            changes={path: KNOWN_SOLUTIONS[path] for path in request.allowed_paths},
            summary='Known-solution offline rehearsal', usage_units=0,
            metadata={'runner': 'known-solution', 'api_calls': 0})


def _case(root: Path, variant: str, worker, ledger: Ledger, trace: Trace,
          validator_factory: Callable, attempts: int, seed: int, run_id: str,
          progress: Callable[[str], None], *, fixture=None, feedback_transform=None) -> dict:
    if fixture is None:
        from . import pilot_fixture as fixture
    INITIAL_FILES, TASKS = fixture.INITIAL_FILES, fixture.TASKS
    PARSER_SPEC, SUMMARY_SPEC = fixture.PARSER_SPEC, fixture.SUMMARY_SPEC
    SPEC_VERSION, EXPECTED_TEST_COUNTS = fixture.SPEC_VERSION, fixture.EXPECTED_TEST_COUNTS
    root.mkdir()
    started = time.monotonic()
    base = GitStore.create(root / 'base.git', INITIAL_FILES)
    release = GitStore.fork(base, root / 'release.git')
    integration = GitStore.fork(base, root / 'integration.git')
    tasks = list(TASKS)
    if variant == 'single':
        tasks = [dict(id='whole-project', subsystem='whole-project', dependencies=(),
                      allowed_paths=tuple(p for t in TASKS for p in t['allowed_paths']),
                      instructions='Complete both modules within the combined allowed paths. Keep the CLI and README unchanged.\n\n'
                      + PARSER_SPEC + '\n\n' + SUMMARY_SPEC)]
    peers = ['release', 'integrator', 'observer']
    peers += [f"worker-{t['id']}" for t in tasks] + [f"maintainer-{t['id']}" for t in tasks]
    mesh = Mesh(peers, 'gossip' if variant.endswith('gossip') else 'bus', seed=seed, broker='release')
    mesh_lock = threading.Lock()
    counts_lock = threading.Lock()
    counters = dict(worker_calls=0, validation_calls=0, prepare_calls=0)
    fatal_worker = threading.Event()
    sequence = {p: 0 for p in peers}
    namespace = f'{run_id}/{variant}'
    for task in tasks:
        if task['dependencies']:
            raise ValueError('This pilot requires the fixed independent task roster')
        ledger.add_task(f"{namespace}/{task['id']}")

    def emit(kind, **data):
        trace.emit(kind, variant=variant, **data)

    def announce(sender, receiver, tip):
        with mesh_lock:
            sequence[sender] += 1
            event = Event.create(sender, sequence[sender], 'proposal', {'tip_sha': tip}, topic=variant)
            mesh.publish(sender, event)
            emit('notice_published', event=event.to_dict(), recipient=receiver)
            for _ in range(100):
                if mesh.has(receiver, event.event_id):
                    emit('notice_received', event_id=event.event_id, recipient=receiver, round=mesh.stats['rounds'])
                    return
                mesh.step()
            raise RuntimeError('Evidence delivery exceeded its bounded round limit')

    def prepare(target, source, tip, checker, paths, stage):
        validation = validator_factory(checker)
        with counts_lock:
            counters['prepare_calls'] += 1
        def validate(checkout):
            with counts_lock:
                counters['validation_calls'] += 1
            passed, detail = validation(checkout)
            if passed:
                completions = []
                for line in validation.last_receipt.get('output', '').splitlines():
                    try:
                        row = json.loads(line)
                    except ValueError:
                        continue
                    if isinstance(row, dict) and row.get('spec_version') == SPEC_VERSION:
                        completions.append(row)
                expected = dict(spec_version=SPEC_VERSION, target=checker,
                                tests_run=EXPECTED_TEST_COUNTS[checker], failures=0,
                                errors=0, successful=True)
                if completions != [expected]:
                    passed, detail = False, 'Trusted checker did not report exactly one complete successful test run'
            emit('verification', stage=stage, passed=passed, detail=detail,
                 receipt=validation.last_receipt)
            return passed, detail
        candidate = target.prepare(source, tip, target.head(), validate, allowed_paths=tuple(paths))
        emit('prepared', stage=stage, candidate=asdict(candidate))
        return candidate

    with LeaseKeeper(ledger) as keeper:
        def generate(task):
            task_id = f"{namespace}/{task['id']}"
            sender, recipient = f"worker-{task['id']}", f"maintainer-{task['id']}"
            keeper.claim(task_id, sender)
            source = GitStore.fork(base, root / f"worker-{task['id']}.git")
            staging = GitStore.fork(base, root / f"maintainer-{task['id']}.git")
            feedback, files = '', dict(INITIAL_FILES)
            checker = 'all' if variant == 'single' else task['id']
            for attempt in range(1, attempts + 1):
                if fatal_worker.is_set():
                    return dict(task=task, status='cancelled_after_peer_failure', halt=True)
                request = WorkerRequest(task_id=task_id, instructions=task['instructions'],
                                        allowed_paths=tuple(task['allowed_paths']), files=files,
                                        base_sha=base.head(), attempt=attempt, feedback=feedback)
                reservation = f'{task_id}/attempt-{attempt}'
                try:
                    units = worker.reservation_units(request)
                    ledger.reserve(reservation, keeper.check(task_id), units, now=time.time())
                except BudgetExceeded:
                    emit('budget_exhausted', task_id=task_id, attempt=attempt)
                    return dict(task=task, status='budget_exhausted')
                # This receipt precedes the external call. A restarted run is
                # never auto-resumed: unknown outcomes remain reserved.
                emit('worker_started', task_id=task_id, attempt=attempt, reservation=reservation, reserved_units=units)
                with counts_lock:
                    counters['worker_calls'] += 1
                try:
                    result = worker.run(request)
                except WorkerFailure as error:
                    halt = error.metadata.get('halt', error.usage_units is None)
                    if halt:
                        fatal_worker.set()
                    if error.usage_units is not None:
                        ledger.settle(reservation, error.usage_units)
                    emit('worker_failed', task_id=task_id, attempt=attempt,
                         reason=str(error), usage_units=error.usage_units, metadata=error.metadata)
                    return dict(task=task, status='request_indeterminate' if error.usage_units is None else 'worker_failed',
                                halt=halt)
                # Unexpected process errors deliberately keep the reservation.
                ledger.settle(reservation, result.usage_units)
                keeper.check(task_id)
                if (not result.changes or set(result.changes) - set(task['allowed_paths'])
                        or any(v is not None and not isinstance(v, str) for v in result.changes.values())):
                    emit('worker_failed', task_id=task_id, attempt=attempt, reason='Invalid patch scope', usage_units=result.usage_units)
                    return dict(task=task, status='scope_rejected')
                emit('worker_finished', task_id=task_id, attempt=attempt,
                     usage_units=result.usage_units, metadata=result.metadata, summary=result.summary)
                # Repairs remain based on this subsystem's original accepted
                # base. Retain all prior proposed file edits in the new patch.
                for name, content in result.changes.items():
                    if content is None:
                        files.pop(name, None)
                    else:
                        files[name] = content
                changes = {p: files.get(p) for p in task['allowed_paths']}
                tip = source.propose(changes, base_sha=base.head(), message=f"{task['id']} attempt {attempt}")
                announce(sender, recipient, tip)
                candidate = prepare(staging, source, tip, checker, task['allowed_paths'], f"local:{task['id']}")
                keeper.check(task_id)
                if candidate.status == 'prepared':
                    accepted = staging.accept(candidate)
                    if accepted.status != 'accepted':
                        return dict(task=task, status=accepted.status)
                    emit('local_passed', task_id=task_id, candidate_sha=accepted.new_head)
                    return dict(task=task, status='proposal_ready', store=staging, tip=accepted.new_head)
                feedback = feedback_transform(candidate.detail) if feedback_transform else candidate.detail
                emit('repair_needed', task_id=task_id, attempt=attempt, status=candidate.status, feedback=feedback)
            return dict(task=task, status='validation_failed')

        progress(f'{variant}: generating and validating proposals')
        with ThreadPoolExecutor(max_workers=len(tasks)) as pool:
            proposals = list(pool.map(generate, tasks))
        ready = all(p['status'] == 'proposal_ready' for p in proposals)
        status = 'integration_failed'
        if ready:
            for proposal in proposals:
                task = proposal['task']
                announce(f"maintainer-{task['id']}", 'integrator', proposal['tip'])
                candidate = prepare(integration, proposal['store'], proposal['tip'],
                                    'all' if variant == 'single' else task['id'], task['allowed_paths'],
                                    f"integration:{task['id']}")
                keeper.all()
                if candidate.status != 'prepared':
                    ready = False
                    break
                accepted = integration.accept(candidate)
                if accepted.status != 'accepted':
                    ready = False
                    break
                emit('integration_staged', candidate_sha=accepted.new_head)
        if ready:
            tip = integration.head()
            announce('integrator', 'release', tip)
            candidate = prepare(release, integration, tip, 'all',
                                [p for task in tasks for p in task['allowed_paths']], 'release')
            if candidate.status == 'prepared':
                outcome = PromotionCoordinator(ledger).promote(release, candidate, keeper.all(), now=time.time())
                status = outcome.status
                emit('project_accepted' if status == 'accepted' else 'publication_failed',
                     outcome=asdict(outcome), exact_tested_sha=candidate.candidate_sha)
        if not all(p['status'] == 'proposal_ready' for p in proposals):
            status = next(p['status'] for p in proposals if p['status'] != 'proposal_ready')
        states = {task['id']: ledger.task(f"{namespace}/{task['id']}")['status'] for task in tasks}
        completed = status == 'accepted' and all(v == 'complete' for v in states.values())
        result = dict(variant=variant, status=status, project_accepted=completed,
                      halt=any(p.get('halt', False) for p in proposals),
                      required_tasks=[t['id'] for t in tasks], task_states=states,
                      release_head=release.head(), integration_head=integration.head(),
                      base_sha=base.head(), counters=counters, transport=mesh.stats,
                      elapsed_seconds_diagnostic_only=time.monotonic() - started)
        _save(root / 'result.json', result)
        progress(f"{variant}: {status}; {counters['worker_calls']} worker calls")
        return result


def run_pilot(output_dir: Path, worker, *, image: str = DEFAULT_IMAGE,
              budget_ledger: Path | None = None, budget_units: int = 0,
              variants: tuple[str, ...] = VARIANTS, attempts: int = 2, seed: int = 0,
              mode: str = 'rehearsal', validator_factory=None,
              rehearsal_results: Path | None = None,
              progress: Callable[[str], None] = lambda value: None) -> dict:
    from .pilot_fixture import CHECKS, INITIAL_FILES, SPEC_VERSION
    from .sandbox import DockerValidator
    output = Path(output_dir).resolve()
    if output.exists():
        raise FileExistsError('Pilot output directory must be fresh; ambiguous model requests are never replayed')
    if (not variants or len(set(variants)) != len(variants) or set(variants) - set(VARIANTS)
            or type(attempts) is not int or not 1 <= attempts <= 2 or mode not in ('rehearsal', 'live')):
        raise ValueError('Invalid bounded pilot configuration')
    if mode == 'live' and (budget_ledger is None or not 0 < budget_units <= 10_000_000):
        raise ValueError('Live runs require an explicit shared budget ledger capped at $10')
    if mode == 'live' and isinstance(worker, KnownSolutionWorker):
        raise ValueError('Known solutions cannot be labeled live')
    input_hash = hashlib.sha256(json.dumps(INITIAL_FILES, sort_keys=True).encode()).hexdigest()
    checks_hash = hashlib.sha256(json.dumps(CHECKS, sort_keys=True).encode()).hexdigest()
    if mode == 'live':
        if rehearsal_results is None:
            raise ValueError('A passing complete offline rehearsal is required before live calls')
        rehearsal = json.loads(Path(rehearsal_results).read_text())
        prior = rehearsal.get('contract', {})
        if (rehearsal.get('experiment') != 'practical-coding-pilot'
                or prior.get('mode') != 'rehearsal' or prior.get('image') != image
                or prior.get('initial_files_sha256') != input_hash or prior.get('checks_sha256') != checks_hash
                or {c['variant'] for c in rehearsal.get('cases', []) if c.get('project_accepted')} != set(VARIANTS)):
            raise ValueError('Offline rehearsal does not cover these exact inputs, checks, image and variants')
    if validator_factory is None:
        def validator_factory(target):
            return DockerValidator(image, CHECKS, command=('python', '-I', '/checks/run_checks.py', target))
    validator = validator_factory('all')
    ok, detail = validator.preflight()
    if not ok:
        raise RuntimeError(f'Validation preflight failed: {detail}')
    output.mkdir(parents=True)
    ledger = Ledger(budget_ledger or output / 'ledger.sqlite', budget_units=budget_units)
    trace = Trace(output / 'trace.jsonl')
    run_id = hashlib.sha256(str(output).encode()).hexdigest()[:16]
    contract = dict(spec_version=SPEC_VERSION, mode=mode, variants=variants, attempts=attempts,
                    image=image, seed=seed, budget_units=budget_units, budget_ledger=str(ledger.path.resolve()),
                    initial_files_sha256=input_hash, checks_sha256=checks_hash,
                    model=getattr(worker, 'model', 'known-solution'), status='running')
    _save(output / 'manifest.json', contract)
    trace.emit('run_started', contract=contract)
    cases = []
    try:
        for variant in variants:
            result = _case(output / variant, variant, worker, ledger, trace, validator_factory,
                           attempts, seed, run_id, progress)
            cases.append(result)
            if result['halt'] or result['status'] == 'budget_exhausted':
                break
    except BaseException as error:
        contract['status'] = 'interrupted'
        contract['error_type'] = type(error).__name__
        _save(output / 'manifest.json', contract)
        trace.emit('run_interrupted', error_type=type(error).__name__, budget=ledger.budget())
        raise
    contract['status'] = 'finished'
    _save(output / 'manifest.json', contract)
    report = dict(schema_version=1, experiment='practical-coding-pilot', contract=contract,
                  cases=cases, budget=ledger.budget(),
                  unexecuted_variants=[v for v in variants if v not in {c['variant'] for c in cases}],
                  limitations=['Small fixed project, one run per variant; no superiority claim.',
                               'Maintainer decisions are deterministic gates, not LLM reviewers.',
                               'Worker generation is parallel; acceptance order is fixed.',
                               'Only local validation feedback is repaired; global failures remain failures.',
                               'Transport is simulated; counters stop at required recipients.',
                               'Known API usage is conservatively priced without cache discounts; unknown usage remains reserved.',
                               'No automatic restart of ambiguous requests or out-of-band release writes.'])
    _save(output / 'results.json', report)
    trace.emit('run_finished', budget=report['budget'])
    return report
