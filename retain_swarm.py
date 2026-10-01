"""Audit and retain a completed candidate-swarm study without copying secrets."""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3

from gossip_harness.gitstore import GitStore
from gossip_harness.selection import build_pool, evaluate_selection, select_candidate
from gossip_harness.swarm_experiment import ARMS, PROTOCOL, digest, validate_rehearsal


ROOT = Path(__file__).resolve().parent
LIMITATIONS = [
    'Eight bounded synthetic JSON-function tasks across three families; two repetitions are not sixteen independent project populations. Accepted means the public and fixed held-out tests passed, not a proof of correctness on every valid input.',
    'Generated expected outputs are vetted by a trusted fixture oracle. This is oracle-assisted selection, not evidence that production reviewers invent correct test oracles.',
    'All three pool policies reuse the same frozen candidates and initial evidence; each policy receives the full attributed pool cost while the physical ledger charges requests once.',
    'Pool-fuzz is a matched fuzz diagnostic: its executed test count depends on valid novel reviewer output, while its attributed API cost excludes the reviewer call. It is not an independently deployable policy or evidence of economic superiority.',
    'Strong-single receives public examples and public repair feedback. A strong-single diagnostic with the pooled evidence is deferred.',
    'Breadth and repair opportunities differ by policy. Report observed cost and quality; do not infer superiority at equal spending or equal latency.',
    'Wall and container timings are local diagnostics. The deadline limits new request starts; an in-flight request can finish after the deadline.',
    'Expected answers remain on the host and each case uses a fresh process, but processes in a candidate evaluation share container tmpfs. This is not adversarial VM isolation.',
    'Generated inputs may overlap held-out inputs; overlap is measured only after selection is frozen and reported separately.',
    'This study tests candidate diversity and evidence-based selection, not the causal benefit of gossip transport.',
]


def read(path):
    if path.is_symlink() or not path.is_file():
        raise ValueError(f'Expected a regular artifact: {path}')
    return json.loads(path.read_text())


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def require(condition, message):
    if not condition:
        raise ValueError(message)


def integer(value):
    return type(value) is int and value >= 0


def proposal_counts(receipts, baseline):
    counts = Counter(batches=len(receipts), proposed=0, eligible=0, rejected_records=0,
                     rejected_cases=0, rejected_batches=0)
    reasons = Counter()
    eligible_ids = []
    for receipt in receipts:
        require(receipt.get('oracle_assisted') is True, 'Proposal receipt is not oracle-assisted')
        counts['proposed'] += receipt['proposed_count']
        counts['eligible'] += len(receipt['eligible'])
        counts['rejected_records'] += len(receipt['rejected'])
        for rejection in receipt['rejected']:
            reasons[rejection['reason']] += 1
            counts['rejected_cases' if rejection['index'] is not None else 'rejected_batches'] += 1
        eligible_ids.extend(case['id'] for case in receipt['eligible'])
    counts['unique_eligible_inputs'] = len(set(eligible_ids))
    counts['duplicates_across_batches'] = len(eligible_ids) - len(set(eligible_ids))
    counts['eligible_already_in_baseline'] = sum(ident in baseline for ident in eligible_ids)
    counts['within_batch_duplicate_rejections'] = reasons['duplicate_input']
    return {**dict(counts), 'rejection_reasons': dict(reasons)}


def inspect_ledger(path, namespace, expected_calls, before, after):
    """Read accounting only; do not bootstrap or alter the shared ledger."""
    require(path.is_file() and not path.is_symlink(), 'Accounting ledger must be a regular existing file')
    with sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True) as db:
        db.row_factory = sqlite3.Row
        db.execute('BEGIN')
        reservations = [dict(row) for row in db.execute('SELECT * FROM reservations')]
        tasks = [dict(row) for row in db.execute('SELECT * FROM tasks')]
        limit = db.execute("SELECT value FROM settings WHERE key='budget'").fetchone()[0]
        pending = db.execute("SELECT count(*) FROM intents WHERE state='pending'").fetchone()[0]
    study = {row['id']: row for row in reservations if row['id'].startswith(namespace + '/')}
    require(set(study) == set(expected_calls), 'Run invocation IDs and ledger reservations differ')
    for ident, usage in expected_calls.items():
        row = study[ident]
        require(row['state'] == 'settled' and row['spent'] == usage,
                'Run usage is unsettled or differs from invocation evidence')
    physical = sum(expected_calls.values())
    require(after['spent_or_reserved'] - before['spent_or_reserved'] == physical,
            'Run invocation cost does not reconcile with recorded global budget delta')
    require(before['limit'] == after['limit'], 'Recorded budget changed during the run')
    used = sum(row['spent'] if row['spent'] is not None else row['amount'] for row in reservations)
    require(used >= after['spent_or_reserved'], 'Current ledger is older than the recorded run')
    study_tasks = [row for row in tasks if row['id'].startswith(namespace + '/')]
    require(all(row['status'] == 'complete' for row in study_tasks), 'Run has incomplete study records')
    return dict(
        cumulative_budget=dict(limit=limit, spent_or_reserved=used, remaining=limit-used),
        ledger_verification=dict(reservations=len(reservations),
                                 unsettled_reservations=sum(row['state'] != 'settled' for row in reservations),
                                 completed_tasks=sum(row['status'] == 'complete' for row in tasks),
                                 pending_intents=pending, study_reservations=len(study),
                                 study_completed_records=len(study_tasks),
                                 study_actual_micro_usd=physical,
                                 recorded_budget_delta_micro_usd=physical,
                                 read_only=True),
    )


def inspect_case(run, case, events):
    from gossip_harness.swarm_fixture import get_task
    task, repetition = case['task_id'], case['repetition']
    fixture = get_task(task)
    directory = run / f'repeat-{repetition}' / task
    require(read(directory / 'result.json') == case, 'Case receipt differs from top-level results')
    frozen = read(directory / 'selection-frozen.json')
    evidence = read(directory / 'selection-evidence.json')
    heldout = read(directory / 'heldout.json')
    preregistered = read(directory / 'preregistered.json')
    require(set(case['arms']) == set(ARMS) and set(frozen['selections']) == set(ARMS), 'Missing arm')
    require(preregistered['candidate_order'] == evidence['candidate_order'] == case['candidate_order'],
            'Anonymous candidate order changed')
    require(set(frozen['candidates']) == set(heldout) == set(case['public_success']), 'Candidate roster mismatch')
    freezing = [(index, row) for index, row in enumerate(events) if row['kind'] == 'selection_frozen']
    require(len(freezing) == 1 and freezing[0][1]['sha256'] == digest(frozen), 'Missing or altered selection freeze')
    hidden_events = [(index, row) for index, row in enumerate(events)
                     if row['kind'] == 'validation' and row['stage'] == 'heldout']
    require(len(hidden_events) == len(heldout) and all(index > freezing[0][0] for index, _ in hidden_events),
            'Held-out evaluation precedes selection freeze or is incomplete')
    require({row['candidate']: row['receipt'] for _, row in hidden_events} == heldout,
            'Held-out receipts differ from trace')
    for _, row in hidden_events:
        require(row['files_sha256'] == frozen['candidates'][row['candidate']]['files_sha256'],
                'Held-out source does not match the frozen candidate')
        require(row['cases_sha256'] == digest(list(fixture.hidden_cases)), 'Held-out suite differs from the fixed fixture')
    public_ids = preregistered['public_case_ids']
    for candidate in evidence['candidate_order']:
        require(case['public_success'][candidate] == all(evidence['baseline_matrix'][candidate][ident] for ident in public_ids),
                'Public gate differs from visible matrix')
    for role in ('strong', 'sequential'):
        public_receipts = [row['receipt'] for row in events if row['kind'] == 'validation'
                           and row['stage'] == 'public-feedback' and row['candidate'] == role]
        require(case['public_success'][role] == bool(public_receipts and public_receipts[-1]['passed']),
                'Baseline public gate differs from feedback receipt')
    correct = {key: bool(value['passed'] and case['public_success'][key]) for key, value in heldout.items()}
    pool_correct = {key: correct[key] for key in evidence['candidate_order']}
    require(pool_correct == case['pool_hidden_success'], 'Pool correctness differs from sealed receipts')
    require(any(pool_correct.values()) == case['pool_candidate_coverage'], 'Candidate coverage mismatch')
    require(case['matched_extra_test_counts'] is True and evidence['matched_execution_counts'] is True
            and len(evidence['reviewer_extra']) == len(evidence['fuzz_extra']), 'Extra test counts are not matched')
    selectors = {}
    for name, extras in (('fixed', []), ('fuzz', evidence['fuzz_extra']), ('reviewer', evidence['reviewer_extra'])):
        cases = evidence['baseline_cases'] if name == 'fixed' else build_pool(evidence['baseline_cases'], extras)
        ids = {item['id'] for item in cases}
        source = evidence['baseline_matrix'] if name == 'fixed' else evidence['extended_matrix']
        matrix = {key: {ident: outcome for ident, outcome in row.items() if ident in ids}
                  for key, row in source.items()}
        selectors[name] = select_candidate(matrix, cases=cases,
                                          public_case_ids=preregistered['public_case_ids'],
                                          candidate_order=evidence['candidate_order'])
        require(selectors[name] == evidence[name], 'Selection cannot be reproduced from visible evidence')
    costs = Counter()
    calls = {}
    request_events = {row['call_id']: row for row in events if row['kind'] in ('request_finished', 'request_failed')}
    starts = [row['call_id'] for row in events if row['kind'] == 'request_started']
    require(len(starts) == len(set(starts)) == len(case['invocations']), 'Duplicate or unfinished invocation')
    for invocation in case['invocations']:
        call_id, usage = invocation['call_id'], invocation['usage_units']
        require(call_id not in calls and integer(usage), 'Ambiguous invocation usage')
        row = read(directory / 'requests' / (call_id + '.result.json'))
        require(all(row.get(key) == value for key, value in invocation.items()), 'Saved model response differs from invocation')
        require(all(request_events[call_id].get(key) == value for key, value in invocation.items()),
                'Trace and invocation usage differ')
        request = read(directory / 'requests' / (call_id + '.request.json'))
        start = next(row for row in events if row['kind'] == 'request_started' and row['call_id'] == call_id)
        require(start['request_sha256'] == digest(request), 'Saved request hash mismatch')
        costs[invocation['role']] += usage
        calls[call_id] = usage
    require(set(calls) == set(starts) == set(request_events), 'Request roster mismatch')
    require(all(costs[key] == value for key, value in case['costs'].items())
            and sum(costs.values()) == case['actual_micro_usd'], 'Role cost does not reconcile')
    exports = []
    for candidate, binding in frozen['candidates'].items():
        store = GitStore(directory / (candidate + '.git'))
        files = store.read_files(binding['tip_sha'])
        require(digest(files) == binding['files_sha256'], 'Frozen candidate source hash mismatch')
        exports.append((Path('candidates') / candidate, files, binding['tip_sha']))
    arm_summaries = {}
    for arm, result in case['arms'].items():
        chosen = result['selected']
        require(chosen == frozen['selections'][arm], 'Final winner differs from frozen selection')
        require(result['accepted'] == bool(chosen and correct[chosen]), 'Acceptance differs from public and sealed verdicts')
        cost = (costs['strong'] if arm == 'strong-single' else costs['sequential'] if arm == 'cheap-sequential'
                else costs['pool'] + costs['blind'] + (costs['reviewer'] if arm == 'pool-reviewer' else 0))
        require(cost == result['attributed_micro_usd'], 'Attributed arm cost mismatch')
        if arm.startswith('pool-'):
            name = arm.removeprefix('pool-')
            require(chosen == selectors[name]['selected'], 'Frozen choice differs from visible selector')
            require(result['diagnostics'] == evaluate_selection(selectors[name], pool_correct), 'Selector diagnostics mismatch')
        if result['accepted']:
            require(result['release_head'] == result['exact_tested_sha'], 'Release differs from tested commit')
            store = GitStore(directory / (arm + '-release.git'))
            require(store.head() == result['release_head'], 'Actual release head differs from receipt')
            files = store.read_files(result['exact_tested_sha'])
            require(digest(files) == result['tested_files_sha256'] == frozen['candidates'][chosen]['files_sha256'],
                    'Accepted release contents differ from evaluated candidate')
            accepted_events = [row for row in events if row['kind'] == 'project_accepted' and row['arm'] == arm]
            require(len(accepted_events) == 1 and all(accepted_events[0].get(key) == value for key, value in result.items()),
                    'Publication trace differs from accepted receipt')
            exports.append((Path('accepted') / arm, files, result['release_head']))
        arm_summaries[arm] = dict(selected=chosen, accepted=result['accepted'], attributed_micro_usd=cost,
                                  selector_regret=result.get('diagnostics', {}).get('selector_regret'),
                                  abstained=chosen is None, release_head=result.get('release_head'))
    baseline_ids = {case['id'] for case in evidence['baseline_cases']}
    initial_receipts = list(evidence['candidate_tests'].values()) + [evidence['blind_tests']]
    candidate_ids = [item['id'] for receipt in initial_receipts for item in receipt['eligible']]
    reviewer = evidence['reviewer_tests']
    reviewer_novel = len({item['id'] for item in reviewer['eligible']} - baseline_ids)
    def signal(cases):
        counts = dict(distinguishing=0, all_pass=0, all_fail=0)
        for item in cases:
            outcomes = {row[item['id']] for row in evidence['extended_matrix'].values()}
            counts['distinguishing' if len(outcomes) > 1 else 'all_pass' if outcomes == {True} else 'all_fail'] += 1
        return counts
    test_signal = {name: signal(values) for name, values in (
        ('baseline', evidence['baseline_cases']), ('reviewer', evidence['reviewer_extra']), ('fuzz', evidence['fuzz_extra']))}
    harmful = arm_summaries['pool-fixed']['accepted'] and not arm_summaries['pool-reviewer']['accepted']
    helpful = not arm_summaries['pool-fixed']['accepted'] and arm_summaries['pool-reviewer']['accepted']
    require(case['harmful_review_selection'] == harmful, 'Harmful-selection flag mismatch')
    visible_inputs = {digest(item['input']) for item in evidence['baseline_cases'] + evidence['reviewer_extra'] + evidence['fuzz_extra']}
    overlap = sum(digest(item['input']) in visible_inputs for item in fixture.hidden_cases)
    require(case['visible_hidden_input_overlap'] == overlap, 'Post-freeze visible/hidden overlap differs from fixture')
    validations = [row['receipt'] for row in events if row['kind'] == 'validation']
    return dict(task_id=task, family=case['family'], repetition=repetition, arms=arm_summaries,
                pool_candidate_coverage=any(pool_correct.values()), correct_pool_candidates=sum(pool_correct.values()),
                candidate_prefix_coverage={str(k): any(pool_correct[key] for key in evidence['candidate_order'][:k])
                                           for k in (1, 2, 4)},
                harmful_review_selection=harmful, helpful_review_selection=helpful,
                reviewer_changed_selection=arm_summaries['pool-fixed']['selected'] != arm_summaries['pool-reviewer']['selected'],
                test_signal=test_signal, actual_micro_usd=case['actual_micro_usd'],
                logical_request_attempts=len(calls), model_request_attempts=len(calls) if case['mode'] == 'live' else 0,
                proposals=dict(initial=proposal_counts(initial_receipts, set()),
                               reviewer=proposal_counts([reviewer], baseline_ids),
                               initial_unique_generated_inputs=len(set(candidate_ids)), baseline_unique_inputs=len(baseline_ids),
                               reviewer_novel_eligible_inputs=reviewer_novel,
                               reviewer_novel_inputs_discarded_for_matching=reviewer_novel-len(evidence['reviewer_extra']),
                               reviewer_extra_executed=len(evidence['reviewer_extra']), fuzz_extra_executed=len(evidence['fuzz_extra']),
                               baseline_distinguishing_inputs=test_signal['baseline']['distinguishing'],
                               reviewer_distinguishing_inputs=test_signal['reviewer']['distinguishing'],
                               fuzz_distinguishing_inputs=test_signal['fuzz']['distinguishing']),
                visible_hidden_input_overlap=case['visible_hidden_input_overlap'],
                elapsed_seconds=case['elapsed_seconds'], phase_seconds=case['phase_seconds'],
                pool_phase_seconds=case['pool_phase_seconds'], validation_calls=len(validations),
                validation_runtime_seconds=sum(value.get('runtime_seconds', 0) for value in validations)), calls, exports


def retain(run, output, accounting_ledger, rehearsal=None):
    run, output, accounting_ledger = Path(run).resolve(), Path(output).absolute(), Path(accounting_ledger)
    require(not output.exists() and not output.is_symlink(), 'Retained output must be fresh')
    results, manifest = read(run / 'results.json'), read(run / 'manifest.json')
    require(results.get('experiment') == PROTOCOL and results.get('status') == 'finished'
            and not results.get('unexecuted') and results.get('mode') in ('live', 'rehearsal'), 'A completed swarm run is required')
    require(all(manifest.get(key) == value for key, value in results['contract'].items())
            and manifest['mode'] == results['mode'] and manifest['repetitions'] == results['repetitions']
            and manifest['budget_before'] == results['budget_before'], 'Manifest and results contract differ')
    roster = [(case['task_id'], case['repetition']) for case in results['cases']]
    expected = {(task, repetition) for task in results['contract']['task_ids'] for repetition in range(results['repetitions'])}
    require(len(roster) == len(set(roster)) and set(roster) == expected, 'Task roster is incomplete or duplicated')
    trace_path = run / 'trace.jsonl'
    require(not trace_path.is_symlink(), 'Trace must not be a symlink')
    trace = [json.loads(line) for line in trace_path.read_text().splitlines()]
    require(trace and trace[-1]['kind'] == 'run_finished' and trace[-1]['budget'] == results['budget'], 'Missing final trace receipt')
    proof = read(Path(rehearsal)) if rehearsal else None
    if results['mode'] == 'live':
        require(proof is not None, 'Retaining a live run requires its exact rehearsal proof')
    if proof is not None:
        validate_rehearsal(proof, results['contract'])
    plan = read(ROOT / 'swarm-study-plan.json')
    require(plan['contract'] == results['contract'], 'Preregistered study plan differs from the run contract')
    sources = sorted((ROOT / 'gossip_harness').glob('*.py')) + sorted((ROOT / 'tests').glob('*.py')) + [Path(__file__).resolve(), ROOT / 'README.md', ROOT / 'swarm-study-plan.json']
    require(all(path.is_file() and not path.is_symlink() for path in sources), 'Source snapshot refuses symlinks')
    source_bytes = {path: path.read_bytes() for path in sources}
    hashes = {path.relative_to(ROOT).as_posix(): hashlib.sha256(value).hexdigest() for path, value in source_bytes.items()}
    for name, sha in results['contract']['sources'].items():
        require(hashes['gossip_harness/' + name] == sha, 'Current core source differs from the executed contract')
    namespace, expected_calls = digest(str(run))[:16], {}
    cases, exports = [], []
    for case in results['cases']:
        require(case['mode'] == results['mode'], 'Case mode differs from run')
        events = [row for row in trace if row.get('task_id') == case['task_id'] and row.get('repetition') == case['repetition']]
        summary, calls, jobs = inspect_case(run, case, events)
        cases.append(summary)
        prefix = Path(f"repeat-{case['repetition']}") / case['task_id']
        exports.extend((prefix / path, files, sha) for path, files, sha in jobs)
        expected_calls.update({f"{namespace}/{case['repetition']}/{case['task_id']}/{key}": value for key, value in calls.items()})
    require(sum(row['kind'] == 'request_started' for row in trace) == len(expected_calls), 'Trace contains unaccounted requests')
    ledger = inspect_ledger(accounting_ledger, namespace, expected_calls, results['budget_before'], results['budget'])
    require(ledger['ledger_verification']['study_completed_records'] == len(cases), 'Completed study record count differs from roster')
    aggregate = {arm: dict(task_runs=len(cases), accepted=sum(case['arms'][arm]['accepted'] for case in cases),
                          attributed_micro_usd=sum(case['arms'][arm]['attributed_micro_usd'] for case in cases),
                          selector_regret=sum(bool(case['arms'][arm]['selector_regret']) for case in cases) if arm.startswith('pool-') else None,
                          abstentions=sum(case['arms'][arm]['abstained'] for case in cases)) for arm in ARMS}
    proposal_totals = {}
    for stage in ('initial', 'reviewer'):
        total, reasons = Counter(), Counter()
        for case in cases:
            total.update({key: value for key, value in case['proposals'][stage].items() if key != 'rejection_reasons'})
            reasons.update(case['proposals'][stage]['rejection_reasons'])
        proposal_totals[stage] = {**dict(total), 'rejection_reasons': dict(reasons)}
    summary = dict(experiment=PROTOCOL, mode=results['mode'], repetitions=results['repetitions'],
                   task_runs=len(cases), distinct_tasks=len(results['contract']['task_ids']), families=results['contract']['families'],
                   by_arm=aggregate, cases=cases, actual_micro_usd=sum(case['actual_micro_usd'] for case in cases),
                   logical_request_attempts=sum(case['logical_request_attempts'] for case in cases),
                   model_request_attempts=sum(case['model_request_attempts'] for case in cases),
                   pool_candidate_coverage=sum(case['pool_candidate_coverage'] for case in cases),
                   candidate_prefix_coverage={str(k): sum(case['candidate_prefix_coverage'][str(k)] for case in cases)
                                              for k in (1, 2, 4)},
                   candidate_prefix_note='Post-freeze coverage for preregistered anonymous-order prefixes; no primary winner changes, hidden-driven subset choice, or claim about savings from actually generating fewer candidates.',
                   harmful_review_selections=sum(case['harmful_review_selection'] for case in cases),
                   helpful_review_selections=sum(case['helpful_review_selection'] for case in cases),
                   reviewer_selection_changes=sum(case['reviewer_changed_selection'] for case in cases),
                   test_signal={stage: {label: sum(case['test_signal'][stage][label] for case in cases)
                                        for label in ('distinguishing', 'all_pass', 'all_fail')}
                                for stage in ('baseline', 'reviewer', 'fuzz')},
                   proposal_totals=proposal_totals,
                   visible_hidden_input_overlap=sum(case['visible_hidden_input_overlap'] for case in cases),
                   matched_extra_test_counts=True,
                   reviewer_extra_executed=sum(case['proposals']['reviewer_extra_executed'] for case in cases),
                   fuzz_extra_executed=sum(case['proposals']['fuzz_extra_executed'] for case in cases),
                   reviewer_distinguishing_inputs=sum(case['proposals']['reviewer_distinguishing_inputs'] for case in cases),
                   fuzz_distinguishing_inputs=sum(case['proposals']['fuzz_distinguishing_inputs'] for case in cases),
                   validation_calls=sum(case['validation_calls'] for case in cases),
                   validation_runtime_seconds=sum(case['validation_runtime_seconds'] for case in cases),
                   elapsed_case_seconds=sum(case['elapsed_seconds'] for case in cases),
                   source_sha256=hashes, source_binding_note='Contract core sources exactly match the executed manifest. Other source and test files are retention-time snapshots.',
                   cost_note='Conservative API token estimates, not invoices or total compute cost. Pool costs are fully attributed to every selection policy; physical requests are charged once. The matched fuzz diagnostic excludes the reviewer API cost despite depending on reviewer output for its test count. Container work is recorded separately.',
                   limitations=LIMITATIONS, **ledger)
    output.mkdir(parents=True)
    for filename in ('manifest.json', 'results.json', 'trace.jsonl'):
        shutil.copyfile(run / filename, output / filename)
    for case in results['cases']:
        prefix = Path(f"repeat-{case['repetition']}") / case['task_id']
        for source in sorted((run / prefix).glob('*.json')) + sorted((run / prefix / 'requests').glob('*.json')):
            require(not source.is_symlink(), 'Artifact copy refuses symlinks')
            target = output / source.relative_to(run)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target)
    for prefix, files, sha in exports:
        for name, content in files.items():
            target = output / prefix / name
            require(target.resolve().is_relative_to(output.resolve()), 'Unsafe exported repository path')
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content)
        (output / prefix / 'SOURCE_COMMIT').write_text(sha + '\n')
    for source, contents in source_bytes.items():
        target = output / 'source-snapshot' / source.relative_to(ROOT)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(contents)
    if proof is not None:
        save(output / 'rehearsal-proof.json', proof)
    save(output / 'summary.json', summary)
    rows = '\n'.join(f"| {arm} | {value['accepted']}/{len(cases)} | ${value['attributed_micro_usd']/1e6:.6f} | {value['selector_regret'] if value['selector_regret'] is not None else '—'} |" for arm, value in aggregate.items())
    (output / 'README.md').write_text(
        f"# Candidate-swarm study ({results['mode']})\n\n"
        f"{len(cases)} task-runs; {summary['distinct_tasks']} distinct synthetic tasks across {len(summary['families'])} families. "
        f"{summary['model_request_attempts']} physical model requests; estimated incremental cost ${summary['actual_micro_usd']/1e6:.6f}.\n\n"
        '| Policy | Accepted | Fully attributed API cost | Selector regret |\n| --- | ---: | ---: | ---: |\n' + rows + '\n\n'
        f"A correct pool candidate existed in {summary['pool_candidate_coverage']}/{len(cases)} runs. "
        f"Reviewer versus fixed selection caused {summary['harmful_review_selections']} harmful changes. "
        f"Post-freeze audit found {summary['visible_hidden_input_overlap']} held-out inputs also present in visible evidence.\n\n"
        'The bundle retains raw requests, model responses (including failed proposals), visible selection matrices, frozen decisions, host-side held-out receipts, every frozen candidate, accepted exact-commit exports, and source hashes. No credentials, Git object stores, or SQLite files are copied.\n\n'
        + '\n'.join('- ' + note for note in LIMITATIONS) + '\n')
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--accounting-ledger', type=Path, default=Path('runs/first-live-budget.sqlite'))
    parser.add_argument('--rehearsal', type=Path)
    args = parser.parse_args()
    summary = retain(args.run, args.output, args.accounting_ledger, args.rehearsal)
    print(json.dumps({key: summary[key] for key in ('mode', 'task_runs', 'by_arm', 'actual_micro_usd',
                                                   'model_request_attempts', 'pool_candidate_coverage',
                                                   'cumulative_budget', 'ledger_verification')}, indent=2))


if __name__ == '__main__':
    main()
