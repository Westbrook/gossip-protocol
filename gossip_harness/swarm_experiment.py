"""Frozen candidate selection with host-side, post-selection evaluation.

This is an oracle-assisted small-task experiment, not a production correctness
oracle. No held-out outcomes enter generation, feedback, or winner selection.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import random
import re
import threading
import time

from .gitstore import GitStore
from .ledger import BudgetExceeded, Ledger
from .pilot import DEFAULT_IMAGE, LeaseKeeper, Trace, _save
from .promotion import PromotionCoordinator
from .worker import MODEL, STRONG_MODEL, OpenAIWorker, WorkerFailure, WorkerRequest, WorkerResult

PROTOCOL = 'candidate-selection-v1'
ARMS = ('strong-single', 'cheap-sequential', 'pool-fixed', 'pool-fuzz', 'pool-reviewer')
POLICY_CAP = 6_000_000
DEADLINE_SECONDS = 900
CORE_FILES = ('swarm_experiment.py', 'swarm_fixture.py', 'blackbox_validator.py',
              'selection.py', 'worker.py', 'gitstore.py', 'ledger.py', 'promotion.py',
              'sandbox.py', 'pilot.py', '__main__.py')


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     allow_nan=False).encode()).hexdigest()


def source_signature():
    root = Path(__file__).parent
    return {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in CORE_FILES}


def model_contract():
    # The credential is never used by profile_manifest; no network request occurs.
    return {role: OpenAIWorker('profile-only', model=model, max_output_tokens=4096).profile_manifest()
            for role, model in (('cheap', MODEL), ('strong', STRONG_MODEL))}


def contract(tasks, image):
    from .swarm_fixture import fixture_signature
    return dict(protocol=PROTOCOL, sources=source_signature(), fixture=fixture_signature(),
                task_ids=[t.id for t in tasks], families=sorted({t.family for t in tasks}),
                image=image, models=model_contract(), arms=list(ARMS), candidates=4,
                proposal_slots=6, strong_attempts=2, sequential_attempts=4,
                policy_cap_micro_usd=POLICY_CAP, request_start_deadline_seconds=DEADLINE_SECONDS,
                selection='public gate, fraction of unique valid cases, frozen anonymous order',
                test_label_condition='oracle-assisted; invalid labels rejected without correction')


def validate_rehearsal(proof, expected):
    roster = [(r.get('task_id'), r.get('repetition')) for r in proof.get('cases', [])]
    wanted = {(task, 0) for task in expected['task_ids']}
    if (proof.get('experiment') != PROTOCOL or proof.get('mode') != 'rehearsal'
            or proof.get('status') != 'finished' or proof.get('repetitions') != 1
            or proof.get('contract') != expected or proof.get('unexecuted')
            or set(roster) != wanted or len(roster) != len(wanted)
            or not all(c.get('rehearsal_verified') and c.get('matched_extra_test_counts')
                       and set(c.get('arms', {})) == set(ARMS)
                       and all(a.get('accepted') and isinstance(a.get('release_head'), str)
                               and re.fullmatch(r'[0-9a-f]{40}(?:[0-9a-f]{24})?', a['release_head'])
                               and a.get('release_head') == a.get('exact_tested_sha')
                               for a in c['arms'].values()) for c in proof.get('cases', []))):
        raise ValueError('A complete, exact eight-task offline rehearsal is required')


class HaltRun(RuntimeError):
    pass


class KnownSwarmWorker:
    """Deterministic rehearsal with a bad pool candidate and baseline repairs."""
    def reservation_units(self, request):
        return 0

    def run(self, request):
        from .swarm_fixture import get_task
        task = get_task(request.task_id.split('/')[0])
        parts = request.task_id.split('/')
        role = parts[1]
        cases = task.fuzz_cases[6:12] if role == 'reviewer' else task.fuzz_cases[:6]
        changes = {'tests.json': json.dumps(list(cases))} if 'tests.json' in request.allowed_paths else {}
        if 'solution.py' in request.allowed_paths:
            bad = (role == 'pool' and parts[2] == '0') or (role in ('strong', 'sequential') and request.attempt == 1)
            changes['solution.py'] = 'def solve(payload):\n    return {"deliberately_wrong": True}\n' if bad else task.known_solution
        return WorkerResult(changes, 'Scripted offline proposal', 0, {'runner': 'known-solution', 'api_calls': 0})


def _feedback(receipt, cases):
    return json.dumps([dict(input=cases[o['index']]['input'], expected=cases[o['index']]['expected'],
                            actual=o.get('actual'), status=o.get('status'))
                       for o in receipt['outcomes'] if not o['passed']], ensure_ascii=False)


def _case(root, task, repetition, cheap, strong, ledger, validator, trace, namespace, *, mode, progress):
    from .selection import build_pool, parse_proposals, select_candidate, evaluate_selection
    root.mkdir(parents=True)
    (root / 'requests').mkdir()
    started = time.monotonic()
    base = GitStore.create(root / 'base.git', task.initial_files)
    base_sha = base.head()
    billing_task = namespace + '/study-record'
    ledger.add_task(billing_task)
    lock = threading.Lock()
    fatal = threading.Event()
    costs = dict(pool=0, blind=0, reviewer=0, strong=0, sequential=0)
    invocations = []
    candidates = {}
    public = build_pool(list(task.public_cases))
    order = [f'candidate-{i}' for i in range(4)]
    random.Random(digest([task.id, repetition, 'anonymous-order'])).shuffle(order)
    _save(root / 'preregistered.json', dict(task_id=task.id, repetition=repetition,
                                          candidate_order=order, public_case_ids=[c['id'] for c in public]))

    def emit(kind, **data):
        trace.emit(kind, task_id=task.id, repetition=repetition, **data)

    def evaluate(files, cases, stage, candidate):
        receipt = validator.evaluate(files, cases)
        emit('validation', stage=stage, candidate=candidate, receipt=receipt,
             cases_sha256=digest(cases), files_sha256=digest(files))
        if (not receipt.get('cleanup_verified') or receipt.get('status') not in ('passed', 'failed')
                or len(receipt.get('outcomes', [])) != len(cases)
                or [o.get('index') for o in receipt['outcomes']] != list(range(len(cases)))
                or any(type(o.get('passed')) is not bool for o in receipt['outcomes'])
                or type(receipt.get('passed')) is not bool
                or receipt['passed'] != all(o['passed'] for o in receipt['outcomes'])
                or receipt['status'] != ('passed' if receipt['passed'] else 'failed')):
            raise HaltRun('Validation infrastructure or receipt failure; study stopped')
        return receipt

    with LeaseKeeper(ledger) as keeper:
        keeper.claim(billing_task, 'study-controller')

        def call(role, worker, *, files, paths, instructions, attempt=1, identifier='0', feedback='',
                 deadline_start=None):
            if fatal.is_set():
                raise HaltRun('A prior request requires investigation')
            if deadline_start is not None and time.monotonic() - deadline_start >= DEADLINE_SECONDS:
                return None
            request = WorkerRequest(f'{task.id}/{role}/{identifier}/repeat-{repetition}', instructions,
                                    tuple(paths), dict(files), base_sha, attempt, feedback)
            call_id = f'{role}-{identifier}-{attempt}'
            reservation = namespace + '/' + call_id
            units = worker.reservation_units(request)
            with lock:
                attributed = costs[role]
                if role == 'reviewer':
                    attributed += costs['pool'] + costs['blind']
                if role == 'blind':
                    attributed += costs['pool']
                if attributed + units > POLICY_CAP:
                    return None
                ledger.reserve(reservation, keeper.check(billing_task), units, now=time.time())
            _save(root / 'requests' / (call_id + '.request.json'), asdict(request))
            emit('request_started', role=role, call_id=call_id, reserved_units=units,
                 request_sha256=digest(asdict(request)))
            before = time.monotonic()
            try:
                result = worker.run(request)
            except WorkerFailure as error:
                if error.usage_units is not None:
                    ledger.settle(reservation, error.usage_units)
                row = dict(role=role, call_id=call_id, usage_units=error.usage_units,
                           failure=str(error), metadata=error.metadata,
                           seconds=time.monotonic() - before)
                with lock:
                    costs[role] += error.usage_units or 0
                    invocations.append(row)
                _save(root / 'requests' / (call_id + '.result.json'), row)
                emit('request_failed', **row)
                if error.usage_units is None or error.metadata.get('halt'):
                    fatal.set()
                    raise HaltRun('Provider failure requires investigation; no automatic retry') from None
                return None
            ledger.settle(reservation, result.usage_units)
            row = dict(role=role, call_id=call_id, usage_units=result.usage_units,
                       metadata=result.metadata, seconds=time.monotonic() - before)
            with lock:
                costs[role] += result.usage_units
                invocations.append(row)
            _save(root / 'requests' / (call_id + '.result.json'), {**row, **asdict(result)})
            emit('request_finished', **row)
            return result

        context = {**task.initial_files, 'TASK.md': task.spec,
                   'public-examples.json': json.dumps(list(task.public_cases))}
        code_prompt = ('Implement solution.py exposing solve(payload). Use only the Python standard library. '
                       'Return a JSON-compatible result; do not print. Each input is evaluated in a fresh process. '
                       'Follow the complete task contract and public examples.\n\n' + task.spec)
        test_prompt = ('Write tests.json only: a JSON array with at most six objects having exactly input, expected, '
                       'and requirement. Use valid task inputs, exact expected JSON outputs, and a requirement ID '
                       'from the specification. Propose meaningful edge cases. No executable test code.\n\n' + task.spec)

        def retain_candidate(key, result, *, proposals=False):
            code = result.changes.get('solution.py') if result else None
            if not isinstance(code, str):
                code = 'def solve(payload):\n    raise RuntimeError("No valid model implementation")\n'
            files = {**task.initial_files, 'solution.py': code}
            store = GitStore.fork(base, root / (key + '.git'))
            tip = store.propose({'solution.py': code}, base_sha=base_sha, message='Frozen candidate ' + key)
            item = dict(id=key, files=files, files_sha256=digest(files), tip_sha=tip,
                        store_path=str(store.path), implementation_returned=bool(result and isinstance(result.changes.get('solution.py'), str)))
            if proposals:
                content = result.changes.get('tests.json', '[]') if result else '[]'
                item['test_proposals'] = parse_proposals(content if isinstance(content, str) else '[]', task, key)
            candidates[key] = item
            return item

        def baseline(role, worker, attempts):
            beginning = time.monotonic()
            files, feedback, last = dict(context), '', None
            for attempt in range(1, attempts + 1):
                result = call(role, worker, files=files, paths=('solution.py',), instructions=code_prompt,
                              attempt=attempt, feedback=feedback, deadline_start=beginning)
                if result is None:
                    break
                code = result.changes.get('solution.py')
                if not isinstance(code, str):
                    break
                last = result
                current = {**task.initial_files, 'solution.py': code}
                receipt = evaluate(current, public, 'public-feedback', role)
                if receipt['passed']:
                    break
                feedback = _feedback(receipt, public)
                files = {**context, 'solution.py': code}
            item = retain_candidate(role, last)
            return dict(selected=role, candidates=[role], public_receipt=receipt if last else None,
                        phase_seconds=time.monotonic() - beginning)

        def pool():
            beginning = time.monotonic()
            def generate(index):
                key = f'candidate-{index}'
                result = call('pool', cheap, files=context, paths=('solution.py', 'tests.json'),
                              instructions=code_prompt + '\nAlso write tests.json as at most six data-only cases '
                              'with exactly input, expected, requirement (a specification requirement ID).',
                              identifier=str(index), deadline_start=beginning)
                return retain_candidate(key, result, proposals=True)
            with ThreadPoolExecutor(max_workers=4) as executor:
                items = list(executor.map(generate, range(4)))
            blind = call('blind', cheap, files=context, paths=('tests.json',), instructions=test_prompt,
                         deadline_start=beginning)
            blind_content = blind.changes.get('tests.json', '[]') if blind else '[]'
            blind_tests = parse_proposals(blind_content if isinstance(blind_content, str) else '[]', task, 'blind')
            baseline_cases = build_pool(public, list(task.fuzz_cases[:6]), blind_tests,
                                        *(item['test_proposals'] for item in items))
            matrix = {}
            receipts = {}
            for item in items:
                receipt = evaluate(item['files'], baseline_cases, 'baseline-matrix', item['id'])
                receipts[item['id']] = receipt
                matrix[item['id']] = {case['id']: bool(o['passed']) for case, o in zip(baseline_cases, receipt['outcomes'])}
            selection_args = dict(cases=baseline_cases, public_case_ids=[c['id'] for c in public], candidate_order=order)
            fixed = select_candidate(matrix, **selection_args)
            base_elapsed = time.monotonic() - beginning
            review_context = {'TASK.md': task.spec, 'baseline-evidence.json': json.dumps(dict(cases=baseline_cases, matrix=matrix))}
            review_context.update({key + '.py': candidates[key]['files']['solution.py'] for key in order})
            review = call('reviewer', strong, files=review_context, paths=('tests.json',),
                          instructions=test_prompt + '\nInspect the anonymous candidates and baseline execution matrix. '
                          'Propose up to six additional minimal inputs that distinguish unresolved behavior. '
                          'Do not choose a candidate, modify implementations, or repeat supplied tests.',
                          deadline_start=beginning)
            review_content = review.changes.get('tests.json', '[]') if review else '[]'
            review_tests = parse_proposals(review_content if isinstance(review_content, str) else '[]', task, 'reviewer')
            reviewer_cases = build_pool(baseline_cases, review_tests)
            base_ids = {c['id'] for c in baseline_cases}
            extra_review = [c for c in reviewer_cases if c['id'] not in base_ids]
            # Same six proposal slots and the same number of accepted executions.
            fuzz_proposals = list(task.fuzz_cases[6:12])
            extra_fuzz = [c for c in build_pool(fuzz_proposals) if c['id'] not in base_ids][:len(extra_review)]
            extra_review = extra_review[:len(extra_fuzz)]
            reviewer_cases = build_pool(baseline_cases, extra_review)
            fuzz_cases = build_pool(baseline_cases, extra_fuzz)
            all_extra = build_pool(extra_review, extra_fuzz)
            extended = {k: dict(v) for k, v in matrix.items()}
            for item in items:
                if all_extra:
                    receipt = evaluate(item['files'], all_extra, 'additional-matrix', item['id'])
                    extended[item['id']].update({case['id']: bool(o['passed']) for case, o in zip(all_extra, receipt['outcomes'])})
            def restricted(cases):
                ids = {case['id'] for case in cases}
                return {key: {ident: value for ident, value in row.items() if ident in ids} for key, row in extended.items()}
            reviewer = select_candidate(restricted(reviewer_cases), cases=reviewer_cases, public_case_ids=selection_args['public_case_ids'], candidate_order=order)
            fuzz = select_candidate(restricted(fuzz_cases), cases=fuzz_cases, public_case_ids=selection_args['public_case_ids'], candidate_order=order)
            evidence = dict(candidate_order=order, candidate_commits={i['id']: i['tip_sha'] for i in items},
                            baseline_cases=baseline_cases, baseline_matrix=matrix, extended_matrix=extended,
                            candidate_tests={i['id']: i['test_proposals'] for i in items}, blind_tests=blind_tests,
                            reviewer_tests=review_tests, reviewer_extra=extra_review, fuzz_extra=extra_fuzz,
                            fixed=fixed, reviewer=reviewer, fuzz=fuzz,
                            matched_execution_counts=len(extra_fuzz) == len(extra_review),
                            baseline_phase_seconds=base_elapsed, complete_phase_seconds=time.monotonic() - beginning)
            _save(root / 'selection-evidence.json', evidence)
            return evidence

        branches = ['strong', 'sequential', 'pool']
        offset = repetition % 3
        branches = branches[offset:] + branches[:offset]
        results = {}
        for branch in branches:
            progress(f'{task.id} repeat {repetition + 1}: {branch}')
            results[branch] = pool() if branch == 'pool' else baseline(branch, strong if branch == 'strong' else cheap, 2 if branch == 'strong' else 4)

        evidence = results['pool']
        selected = {'strong-single': 'strong', 'cheap-sequential': 'sequential',
                    'pool-fixed': evidence['fixed']['selected'], 'pool-fuzz': evidence['fuzz']['selected'],
                    'pool-reviewer': evidence['reviewer']['selected']}
        frozen = dict(selections=selected, candidates={k: dict(tip_sha=v['tip_sha'], files_sha256=v['files_sha256']) for k, v in candidates.items()})
        _save(root / 'selection-frozen.json', frozen)
        emit('selection_frozen', sha256=digest(frozen), selections=selected)

        # This is the first use of hidden cases: every winner decision is durable.
        hidden = {}
        for key in sorted(candidates):
            hidden[key] = evaluate(candidates[key]['files'], list(task.hidden_cases), 'heldout', key)
        public_success = {key: all(evidence['baseline_matrix'][key].get(case['id'], False) for case in public) for key in order}
        public_success.update({role: bool(results[role]['public_receipt'] and results[role]['public_receipt']['passed']) for role in ('strong', 'sequential')})
        hidden_success = {k: v['passed'] and public_success[k] for k, v in hidden.items()}
        _save(root / 'heldout.json', hidden)
        pool_success = {k: hidden_success[k] for k in order}
        arms = {}
        for arm, chosen in selected.items():
            total = (costs['strong'] if arm == 'strong-single' else costs['sequential'] if arm == 'cheap-sequential'
                     else costs['pool'] + costs['blind'] + (costs['reviewer'] if arm == 'pool-reviewer' else 0))
            result = dict(selected=chosen, accepted=bool(chosen and hidden_success[chosen]),
                          attributed_micro_usd=total, release_head=None)
            if arm.startswith('pool-'):
                selection = evidence[{'pool-fixed': 'fixed', 'pool-fuzz': 'fuzz', 'pool-reviewer': 'reviewer'}[arm]]
                result['diagnostics'] = evaluate_selection(selection, pool_success)
            if result['accepted']:
                item = candidates[chosen]
                release = GitStore.fork(base, root / (arm + '-release.git'))
                source = GitStore(item['store_path'])
                def exact_content(checkout):
                    observed = {name: (checkout / name).read_text() for name in task.initial_files}
                    return (digest(observed) == item['files_sha256'] and hidden[chosen]['passed'],
                            'Host-evaluated immutable content hash matches this exact checkout')
                prepared = release.prepare(source, item['tip_sha'], release.head(), exact_content, allowed_paths=('solution.py',))
                publication_ledger = Ledger(root / (arm + '-ledger.sqlite'), budget_units=0)
                publication_ledger.add_task(arm)
                lease = publication_ledger.claim(arm, 'verifier', now=time.time(), ttl=600)
                outcome = PromotionCoordinator(publication_ledger).promote(release, prepared, [lease], now=time.time())
                if outcome.status != 'accepted' or release.head() != prepared.candidate_sha:
                    raise HaltRun('Exact selected publication failed')
                result.update(release_head=release.head(), exact_tested_sha=prepared.candidate_sha,
                              tested_files_sha256=item['files_sha256'], release_path=str(release.path))
                emit('project_accepted', arm=arm, **result)
            arms[arm] = result
        result = dict(task_id=task.id, family=task.family, repetition=repetition, arms=arms,
                      pool_candidate_coverage=any(pool_success.values()), pool_hidden_success=pool_success,
                      harmful_review_selection=arms['pool-fixed']['accepted'] and not arms['pool-reviewer']['accepted'],
                      costs=costs, actual_micro_usd=sum(costs.values()), invocations=invocations,
                      matched_extra_test_counts=evidence['matched_execution_counts'],
                      candidate_order=order, public_success=public_success,
                      visible_hidden_input_overlap=sum(digest(c['input']) in {digest(v['input']) for v in evidence['baseline_cases'] + evidence['reviewer_extra'] + evidence['fuzz_extra']} for c in task.hidden_cases),
                      phase_seconds={role: results[role]['phase_seconds'] for role in ('strong', 'sequential')},
                      pool_phase_seconds=evidence['complete_phase_seconds'], elapsed_seconds=time.monotonic() - started,
                      mode=mode, rehearsal_verified=mode == 'rehearsal' and all(a['accepted'] for a in arms.values()))
        _save(root / 'result.json', result)
        # Complete the experiment-record task, even when a candidate is incorrect.
        archive = GitStore.create(root / 'record.git', {'result.json': '{}\n'})
        offer = archive.propose({'result.json': json.dumps(result, sort_keys=True)})
        record = archive.prepare(archive, offer, archive.head(), lambda p: ((p / 'result.json').read_text() == json.dumps(result, sort_keys=True), 'Exact completed experiment record'), allowed_paths=('result.json',))
        publication = PromotionCoordinator(ledger).promote(archive, record, [keeper.check(billing_task)], now=time.time())
        if publication.status != 'accepted':
            raise HaltRun('Completed experiment record could not be journaled')
        emit('case_finished', actual_micro_usd=result['actual_micro_usd'], accepted={k: v['accepted'] for k, v in arms.items()})
        return result


def run_swarm_experiment(output_dir, cheap, strong, *, image=DEFAULT_IMAGE, budget_ledger=None,
                         budget_units=0, repetitions=2, mode='rehearsal', rehearsal_results=None,
                         validator_factory=None, progress=lambda value: None):
    from .swarm_fixture import TASKS
    from .blackbox_validator import BlackboxValidator
    if type(repetitions) is not int or repetitions not in (1, 2) or mode not in ('rehearsal', 'live'):
        raise ValueError('Expected one or two repetitions and rehearsal/live mode')
    if len(TASKS) != 8 or len({t.id for t in TASKS}) != 8 or len({t.family for t in TASKS}) < 3:
        raise ValueError('Expected eight unique tasks across at least three families')
    output = Path(output_dir).resolve()
    if output.exists():
        raise FileExistsError('Output must be fresh; ambiguous requests are never resumed')
    specification = contract(TASKS, image)
    if mode == 'live':
        if (type(cheap) is not OpenAIWorker or type(strong) is not OpenAIWorker or validator_factory is not None
                or budget_ledger is None or type(budget_units) is not int or not 0 < budget_units <= 50_000_000
                or cheap.profile_manifest() != specification['models']['cheap']
                or strong.profile_manifest() != specification['models']['strong']):
            raise ValueError('Live mode requires the pinned model profiles and authorized shared ledger')
        if rehearsal_results is None:
            raise ValueError('Offline rehearsal required')
        validate_rehearsal(json.loads(Path(rehearsal_results).read_text()), specification)
    validator = validator_factory() if validator_factory else BlackboxValidator(image=image)
    ok, detail = validator.preflight()
    if not ok:
        raise RuntimeError('Blackbox preflight failed: ' + detail)
    output.mkdir(parents=True)
    ledger = Ledger(budget_ledger or output / 'ledger.sqlite', budget_units=budget_units)
    before = ledger.budget()
    trace = Trace(output / 'trace.jsonl')
    namespace = digest(str(output))[:16]
    cases = []
    status = 'running'
    def report():
        seen = {(c['task_id'], c['repetition']) for c in cases}
        return dict(schema_version=1, experiment=PROTOCOL, mode=mode, repetitions=repetitions,
                    status=status, contract=specification, cases=cases, budget_before=before, budget=ledger.budget(),
                    unexecuted=[dict(task_id=t.id, repetition=r) for r in range(repetitions) for t in TASKS if (t.id, r) not in seen])
    _save(output / 'manifest.json', {**specification, 'mode': mode, 'repetitions': repetitions, 'budget_before': before})
    try:
        for repetition in range(repetitions):
            for task in TASKS:
                root = output / f'repeat-{repetition}' / task.id
                cases.append(_case(root, task, repetition, cheap, strong, ledger, validator, trace,
                                   namespace + f'/{repetition}/{task.id}', mode=mode, progress=progress))
                _save(output / 'results.json', report())
        status = 'finished'
    except BaseException as error:
        status = 'interrupted'
        trace.emit('run_interrupted', error_type=type(error).__name__, budget=ledger.budget())
        _save(output / 'results.json', report())
        raise
    result = report()
    _save(output / 'results.json', result)
    trace.emit('run_finished', budget=result['budget'])
    return result
