"""Version two candidate selection with bounded, purpose-bound validation.

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

from devtools.study_validation import StudyValidationSession, study_signals, study_validation_contract
from devtools.validation_session import ResourceBudget, digest as validation_digest
from devtools.git_snapshot import read_text_tree
from devtools.study_receipts import audit_study_sessions


PROTOCOL = 'candidate-selection-v2'
ARMS = ('strong-single', 'cheap-sequential', 'pool-fixed', 'pool-fuzz', 'pool-reviewer')
POLICY_CAP = 6_000_000
DEADLINE_SECONDS = 900
SUPPORT_PATHS = ('gossip_harness/swarm_experiment_v2.py',)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     allow_nan=False).encode()).hexdigest()


def source_signature():
    return study_validation_contract(image=DEFAULT_IMAGE, protocol=PROTOCOL,
                                     support_paths=SUPPORT_PATHS)['support_sha256']


def model_contract():
    # The credential is never used by profile_manifest; no network request occurs.
    return {role: OpenAIWorker('profile-only', model=model, max_output_tokens=4096).profile_manifest()
            for role, model in (('cheap', MODEL), ('strong', STRONG_MODEL))}


def contract(tasks, image, *, budget=ResourceBudget(), deterministic_visible=False):
    from .swarm_fixture import fixture_signature
    validation = study_validation_contract(image=image, protocol=PROTOCOL, budget=budget,
                                          deterministic_visible=deterministic_visible,
                                          support_paths=SUPPORT_PATHS)
    return dict(protocol=PROTOCOL, sources=validation['support_sha256'], validation=validation,
                fixture=fixture_signature(),
                task_ids=[t.id for t in tasks], families=sorted({t.family for t in tasks}),
                image=image, models=model_contract(), arms=list(ARMS), candidates=4,
                proposal_slots=6, strong_attempts=2, sequential_attempts=4,
                policy_cap_micro_usd=POLICY_CAP, request_start_deadline_seconds=DEADLINE_SECONDS,
                selection='public gate, fraction of unique valid cases, frozen anonymous order',
                test_label_condition='oracle-assisted; invalid labels rejected without correction',
                execution_order='serial repair attempts; bounded baseline/added/heldout matrices; ordered reduction',
                final_observations='Every frozen candidate executes physically after selection; no final reuse')


def validate_rehearsal(proof, expected):
    roster = [(r.get('task_id'), r.get('repetition')) for r in proof.get('cases', [])]
    wanted = {(task, 0) for task in expected['task_ids']}
    if (proof.get('experiment') != PROTOCOL or proof.get('mode') != 'rehearsal'
            or proof.get('status') != 'finished' or proof.get('repetitions') != 1
            or proof.get('contract') != expected or proof.get('unexecuted')
            or set(roster) != wanted or len(roster) != len(wanted)
            or not all(c.get('rehearsal_verified') and c.get('matched_extra_test_counts')
                       and c.get('validation_counts', {}).get('heldout_physical') == 6
                       and c.get('validation_counts', {}).get('heldout_reused') == 0
                       and set(c.get('arms', {})) == set(ARMS)
                       and all(a.get('accepted') and isinstance(a.get('release_head'), str)
                               and re.fullmatch(r'[0-9a-f]{40}(?:[0-9a-f]{24})?', a['release_head'])
                               and a.get('release_head') == a.get('exact_tested_sha')
                               for a in c['arms'].values()) for c in proof.get('cases', []))):
        raise ValueError('A complete, exact eight-task offline rehearsal is required')


def audit_rehearsal(proof, expected, *, evidence_root):
    """Verify retained execution and frozen trees without rerunning candidates."""
    from .selection import build_pool
    from .swarm_fixture import TASKS
    validate_rehearsal(proof, expected)
    root = Path(evidence_root).resolve()

    def read(relative):
        path = root / relative
        if path.is_symlink() or not path.resolve().is_relative_to(root):
            raise ValueError('Rehearsal evidence must remain within its retained output')
        return json.loads(path.read_text())

    if read('results.json') != proof:
        raise ValueError('Rehearsal summary differs from retained results')
    if any(proof[key].get('spent_or_reserved') != 0 for key in ('budget_before', 'budget')):
        raise ValueError('Qualification must have zero paid usage')
    for relative, sha256 in expected['sources'].items():
        if hashlib.sha256((root / 'source-snapshot' / relative).read_bytes()).hexdigest() != sha256:
            raise ValueError('Rehearsal frozen source differs from qualification contract')
    audit = audit_study_sessions([root / 'validation'], expected['validation'], evidence_root=root)
    observations = {(row['session_id'], row['logical_index']): row for row in audit['observations']}
    trace = [json.loads(line) for line in (root / 'trace.jsonl').read_text().splitlines()]
    if [row['index'] for row in trace] != list(range(1, len(trace) + 1)):
        raise ValueError('Rehearsal trace is incomplete or reordered')
    consumed = set()
    tasks = {task.id: task for task in TASKS}
    for case in proof['cases']:
        task = tasks[case['task_id']]
        prefix = Path('repeat-0') / task.id
        if read(prefix / 'result.json') != case:
            raise ValueError('Case summary differs from retained result')
        if (case['actual_micro_usd'] != 0 or any(case['costs'].values())
                or any(row['usage_units'] != 0 or row['metadata'].get('api_calls') != 0
                       or row['metadata'].get('runner') != 'known-solution'
                       for row in case['invocations'])):
            raise ValueError('Rehearsal requires the no-provider known workers')
        frozen = read(prefix / 'selection-frozen.json')
        candidates = frozen['candidates']
        wanted = {f'candidate-{number}' for number in range(4)} | {'strong', 'sequential'}
        if set(candidates) != wanted or set(frozen['selections']) != set(ARMS):
            raise ValueError('Frozen selection does not contain the complete candidate pool')
        files = {}
        for name, candidate in candidates.items():
            files[name] = read_text_tree(root / prefix / (name + '.git'), candidate['tip_sha'])
            if digest(files[name]) != candidate['files_sha256']:
                raise ValueError('Candidate Git tree differs from its frozen source')
        rows = [row for row in trace if row.get('task_id') == task.id and row.get('repetition') == 0]
        barriers = [row for row in rows if row['kind'] == 'selection_frozen']
        if (len(barriers) != 1 or barriers[0]['sha256'] != digest(frozen)
                or barriers[0]['selections'] != frozen['selections']):
            raise ValueError('Missing exact durable final-evaluation barrier')
        barrier = barriers[0]['index']
        if any(row['index'] > barrier and row['kind'].startswith('request_') for row in rows):
            raise ValueError('Provider work occurred after final selection')
        evidence = read(prefix / 'selection-evidence.json')
        selected = {'strong-single': 'strong', 'cheap-sequential': 'sequential',
                    'pool-fixed': evidence['fixed']['selected'], 'pool-fuzz': evidence['fuzz']['selected'],
                    'pool-reviewer': evidence['reviewer']['selected']}
        if frozen['selections'] != selected:
            raise ValueError('Frozen arm selections differ from the visible selection evidence')
        suites = {
            'public-feedback': build_pool(list(task.public_cases)),
            'baseline-matrix': evidence['baseline_cases'],
            'additional-matrix': build_pool(evidence['reviewer_extra'], evidence['fuzz_extra']),
            'heldout': list(task.hidden_cases),
        }
        expected_counts = {'public-feedback': 4, 'baseline-matrix': 4,
                           'additional-matrix': 4 if suites['additional-matrix'] else 0, 'heldout': 6}
        observed_counts = {key: 0 for key in expected_counts}
        candidate_counts: dict[str, dict[str, int]] = {key: {} for key in expected_counts}
        hidden = read(prefix / 'heldout.json')
        final_candidates = set()
        for event in (row for row in rows if row['kind'] == 'validation'):
            stage, name, validation = event['stage'], event['candidate'], event['validation']
            if stage not in suites or name not in wanted:
                raise ValueError('Unexpected validation stage or candidate')
            identity = (validation['session_id'], validation['logical_index'])
            if identity in consumed or identity not in observations:
                raise ValueError('Validation observation is missing or duplicated')
            consumed.add(identity)
            observation = observations[identity]
            job, result = observation['job'], observation['result']
            executable = [{key: item[key] for key in ('input', 'expected', 'id', 'requirement')
                           if key in item} for item in suites[stage]]
            if (job['cases'] != executable or event['receipt'] != result['receipt']
                    or any(validation.get(key) != value for key, value in result.items() if key != 'receipt')
                    or event['cases_sha256'] != digest(suites[stage])
                    or event['files_sha256'] != digest(job['files'])
                    or job['purpose'] != ('final' if stage == 'heldout' else 'visible')):
                raise ValueError('Validation receipt differs from its exact ordered matrix')
            if stage != 'public-feedback' and job['files'] != files[name]:
                raise ValueError('Matrix evaluated a different frozen candidate')
            observed_counts[stage] += 1
            candidate_counts[stage][name] = candidate_counts[stage].get(name, 0) + 1
            if stage == 'heldout':
                if event['index'] < barrier or name in final_candidates or hidden[name] != result['receipt']:
                    raise ValueError('Heldout evidence violates the final barrier or candidate coverage')
                final_candidates.add(name)
            elif event['index'] > barrier:
                raise ValueError('Visible feedback occurred after final selection')
        if observed_counts != expected_counts or final_candidates != wanted:
            raise ValueError('Rehearsal matrices do not cover all required observations')
        pool_counts = {f'candidate-{number}': 1 for number in range(4)}
        if candidate_counts != {'public-feedback': {'strong': 2, 'sequential': 2},
                                'baseline-matrix': pool_counts,
                                'additional-matrix': pool_counts if suites['additional-matrix'] else {},
                                'heldout': {name: 1 for name in wanted}}:
            raise ValueError('Rehearsal candidate matrix coverage is duplicated or incomplete')
        for arm, result in case['arms'].items():
            selected = frozen['selections'][arm]
            if (result['selected'] != selected or not hidden[selected]['passed']
                    or result['tested_files_sha256'] != digest(files[selected])
                    or read_text_tree(root / prefix / (arm + '-release.git'), result['release_head']) != files[selected]):
                raise ValueError('Published arm differs from the exact selected heldout-tested tree')
    if consumed != set(observations):
        raise ValueError('Session contains unaccounted validation observations')
    return dict(protocol=PROTOCOL, qualified=True, tasks=len(proof['cases']), arms=len(ARMS),
                observations=len(consumed), provider_requests=0)


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
        changes: dict[str, str | None] = {'tests.json': json.dumps(list(cases))} if 'tests.json' in request.allowed_paths else {}
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
    validation_counts = dict(logical=0, physical=0, reused=0,
                             heldout_physical=0, heldout_reused=0)
    public = build_pool(list(task.public_cases))
    order = [f'candidate-{i}' for i in range(4)]
    random.Random(digest([task.id, repetition, 'anonymous-order'])).shuffle(order)
    _save(root / 'preregistered.json', dict(task_id=task.id, repetition=repetition,
                                          candidate_order=order, public_case_ids=[c['id'] for c in public]))

    def emit(kind, **data):
        trace.emit(kind, task_id=task.id, repetition=repetition, **data)

    def retain_observation(envelope, files, cases, stage, candidate):
        receipt, validation = envelope['receipt'], envelope['validation']
        executable_cases = [{key: case[key] for key in ('input', 'expected', 'id', 'requirement')
                             if key in case} for case in cases]
        if (validation.get('source_sha256') != validation_digest(files)
                or validation.get('suite_sha256') != validation_digest(executable_cases)
                or validation.get('purpose') != ('final' if stage == 'heldout' else 'visible')):
            raise HaltRun('Validation result does not bind to the ordered candidate and suite')
        physical = validation.get('physical') is True
        reused = validation.get('reuse') is not None
        if physical == reused or (stage == 'heldout' and not physical):
            raise HaltRun('Validation execution provenance is inconsistent')
        validation_counts['logical'] += 1
        validation_counts['physical' if physical else 'reused'] += 1
        if stage == 'heldout':
            validation_counts['heldout_physical' if physical else 'heldout_reused'] += 1
        emit('validation', stage=stage, candidate=candidate, receipt=receipt, validation=validation,
             cases_sha256=digest(cases), executable_cases_sha256=validation_digest(executable_cases),
             files_sha256=digest(files))
        if (not receipt.get('cleanup_verified') or receipt.get('status') not in ('passed', 'failed')
                or len(receipt.get('outcomes', [])) != len(cases)
                or [o.get('index') for o in receipt['outcomes']] != list(range(len(cases)))
                or any(type(o.get('passed')) is not bool for o in receipt['outcomes'])
                or type(receipt.get('passed')) is not bool
                or receipt['passed'] != all(o['passed'] for o in receipt['outcomes'])
                or receipt['status'] != ('passed' if receipt['passed'] else 'failed')):
            raise HaltRun('Validation infrastructure or receipt failure; study stopped')
        return receipt

    def evaluate_many(items, cases, stage):
        final = stage == 'heldout'
        executable_cases = [{key: case[key] for key in ('input', 'expected', 'id', 'requirement')
                             if key in case} for case in cases]
        envelopes = validator.evaluate_many(
            [dict(files=item['files'], label=f'{task.id}-{repetition}-{stage}-{item["id"]}')
             for item in items], executable_cases, purpose='final' if final else 'visible',
            reason='Independent post-selection heldout observation of every frozen candidate' if final else '')
        if len(envelopes) != len(items):
            raise HaltRun('Validation matrix does not cover the complete ordered candidate pool')
        return [retain_observation(envelope, item['files'], cases, stage, item['id'])
                for item, envelope in zip(items, envelopes)]

    def evaluate(files, cases, stage, candidate):
        return evaluate_many([dict(id=candidate, files=files)], cases, stage)[0]

    with LeaseKeeper(ledger) as keeper:
        keeper.claim(billing_task, 'study-controller')

        def call(role, worker, *, files, paths, instructions, attempt=1, identifier='0', feedback='',
                 deadline_start=None):
            validator.raise_if_cancelled()
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
                validator.raise_if_cancelled()
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
            for item, receipt in zip(items, evaluate_many(items, baseline_cases, 'baseline-matrix')):
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
            if all_extra:
                for item, receipt in zip(items, evaluate_many(items, all_extra, 'additional-matrix')):
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
            validator.raise_if_cancelled()
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
        hidden_order = sorted(candidates)
        hidden = dict(zip(hidden_order, evaluate_many(
            [candidates[key] for key in hidden_order], list(task.hidden_cases), 'heldout')))
        public_success = {key: all(evidence['baseline_matrix'][key].get(case['id'], False) for case in public) for key in order}
        public_success.update({role: bool(results[role]['public_receipt'] and results[role]['public_receipt']['passed']) for role in ('strong', 'sequential')})
        hidden_success = {k: v['passed'] and public_success[k] for k, v in hidden.items()}
        _save(root / 'heldout.json', hidden)
        pool_success = {k: hidden_success[k] for k in order}
        arms = {}
        for arm, chosen in selected.items():
            validator.raise_if_cancelled()
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
                      mode=mode, validation_counts=validation_counts,
                      rehearsal_verified=mode == 'rehearsal' and all(a['accepted'] for a in arms.values()))
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
                         validator_factory=None, progress=lambda value: None,
                         validation_budget=ResourceBudget(), deterministic_visible=False,
                         validation_cache=None):
    from .swarm_fixture import TASKS
    from .blackbox_validator import BlackboxValidator
    if type(repetitions) is not int or repetitions not in (1, 2) or mode not in ('rehearsal', 'live'):
        raise ValueError('Expected one or two repetitions and rehearsal/live mode')
    if len(TASKS) != 8 or len({t.id for t in TASKS}) != 8 or len({t.family for t in TASKS}) < 3:
        raise ValueError('Expected eight unique tasks across at least three families')
    supplied_output = Path(output_dir)
    output = supplied_output.resolve()
    if supplied_output.is_symlink() or output.exists():
        raise FileExistsError('Output must be fresh; ambiguous requests are never resumed')
    specification = contract(TASKS, image, budget=validation_budget,
                             deterministic_visible=deterministic_visible)
    if mode == 'live':
        if (type(cheap) is not OpenAIWorker or type(strong) is not OpenAIWorker or validator_factory is not None
                or budget_ledger is None or type(budget_units) is not int or not 0 < budget_units <= 50_000_000
                or cheap.profile_manifest() != specification['models']['cheap']
                or strong.profile_manifest() != specification['models']['strong']):
            raise ValueError('Live mode requires the pinned model profiles and authorized shared ledger')
        if rehearsal_results is None:
            raise ValueError('Offline rehearsal required')
        proof_path = Path(rehearsal_results)
        audit_rehearsal(json.loads(proof_path.read_text()), specification, evidence_root=proof_path.parent)
    validator = StudyValidationSession(
        output / 'validation', image=image, protocol=PROTOCOL, budget=validation_budget,
        deterministic_visible=deterministic_visible, cache=validation_cache,
        support_paths=SUPPORT_PATHS, validator_factory=validator_factory or BlackboxValidator)
    try:
        with study_signals(validator):
            if validator.contract != specification['validation']:
                raise HaltRun('Validation contract changed before execution')
            ok, detail = validator.preflight()
            if not ok:
                raise RuntimeError('Blackbox preflight failed: ' + detail)
            project = Path(__file__).resolve().parents[1]
            for relative, expected in specification['sources'].items():
                content = (project / relative).read_bytes()
                if hashlib.sha256(content).hexdigest() != expected:
                    raise HaltRun('Study source changed before execution')
                destination = output / 'source-snapshot' / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(content)
            ledger = Ledger(budget_ledger or output / 'ledger.sqlite', budget_units=budget_units)
            before = ledger.budget()
            trace = Trace(output / 'trace.jsonl')
            namespace = digest(str(output))[:16]
            cases: list[dict] = []
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
                        validator.raise_if_cancelled()
                        root = output / f'repeat-{repetition}' / task.id
                        cases.append(_case(root, task, repetition, cheap, strong, ledger, validator, trace,
                                           namespace + f'/{repetition}/{task.id}', mode=mode, progress=progress))
                        _save(output / 'results.json', report())
                if contract(TASKS, image, budget=validation_budget,
                            deterministic_visible=deterministic_visible) != specification:
                    raise HaltRun('Study sources or execution contract changed during run')
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
    except BaseException:
        # The signal context also detects cancellation arriving during the final
        # save. Keep the retained completion status consistent with that exit.
        retained = output / 'results.json'
        if retained.is_file():
            value = json.loads(retained.read_text())
            if value.get('status') == 'finished':
                value['status'] = 'interrupted'
                _save(retained, value)
        raise


def main(argv=None):
    """Complete, no-provider qualification for the exact configured v2 contract."""
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--image', default=DEFAULT_IMAGE)
    parser.add_argument('--workers', type=int, default=2)
    parser.add_argument('--cpus', type=int, default=2)
    parser.add_argument('--memory-mib', type=int, default=512)
    parser.add_argument('--cache', type=Path)
    parser.add_argument('--deterministic-visible', action='store_true',
                        help='Declare known fixtures deterministic; creates a distinct qualification contract')
    args = parser.parse_args(argv)
    report = run_swarm_experiment(
        args.output, KnownSwarmWorker(), KnownSwarmWorker(), image=args.image,
        repetitions=1, mode='rehearsal', deterministic_visible=args.deterministic_visible,
        validation_cache=args.cache,
        validation_budget=ResourceBudget(workers=args.workers, cpus=args.cpus,
                                         memory_mib=args.memory_mib),
        progress=lambda message: print(message, flush=True))
    audit_rehearsal(report, report['contract'], evidence_root=args.output)
    print(json.dumps(dict(experiment=report['experiment'], status=report['status'],
                          tasks=len(report['cases']), arms=len(ARMS),
                          provider_requests=0, output=str(args.output)), sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
