"""Post-hoc hidden-suite diagnosis of frozen milestone portfolios.

This module never calls a model, reads a credential, changes a project, or feeds
results back into the study. Its counterfactual hidden outcomes are diagnostic;
they are not preregistered primary results or a new candidate-selection policy.
Candidate code is read as text and executed only by BlackboxValidator in Docker.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
import hashlib
from itertools import combinations
import json
from pathlib import Path
import re
import sys
import time

from .blackbox_validator import BlackboxValidator
from .gitstore import GitStore
from .pilot import _save
from .sustained_experiment import CORE, PROTOCOL, cases_for, checked_evaluate, digest


DIAGNOSTIC = 'sustained-final-pool-posthoc-v1'
ALL_STAGES_DIAGNOSTIC = 'sustained-all-stage-pool-posthoc-v1'
SLOTS = ('c0', 'c1', 'c2', 'c3')


def _file_sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _load(path, frozen):
    path = Path(path)
    raw = path.read_bytes()
    frozen[path] = hashlib.sha256(raw).hexdigest()
    return json.loads(raw)


def _unchanged(frozen):
    for path, expected in frozen.items():
        if _file_sha(path) != expected:
            raise ValueError(f'Frozen study artifact changed during diagnostic: {path.name}')


def _contained(path, parent):
    path, parent = Path(path).resolve(), Path(parent).resolve()
    if not path.is_relative_to(parent) or path == parent:
        raise ValueError('Candidate or project path is outside its frozen study directory')
    return path


class _Replay:
    def __init__(self, receipt):
        self.receipt = receipt

    def evaluate(self, files, cases):
        return deepcopy(self.receipt)


def _receipt(validator, files, cases, contract):
    receipt = checked_evaluate(validator, files, cases)
    if (receipt.get('image_id') != contract['image']
            or receipt.get('case_timeout_seconds') != contract['case_timeout_seconds']
            or receipt.get('timeout_seconds') != contract['suite_timeout_seconds']):
        raise ValueError('Validation receipt does not match the frozen execution settings')
    return receipt


def _candidate(root, stage_index, row, frozen):
    binding = row['binding']
    stage_root = root / f'stage-{stage_index}'
    store_path = _contained(binding['store_path'], stage_root)
    if store_path.suffix != '.git' or store_path.parent != stage_root:
        raise ValueError('Candidate binding is not a proposal store for this milestone')
    saved = _load(store_path.with_suffix('.json'), frozen)
    if saved.get('binding') != binding:
        raise ValueError('Retained proposal binding differs from frozen trajectory')
    files = GitStore(store_path).read_files(binding['tip_sha'])
    source_sha = digest(files)
    if (saved.get('files') != files or binding.get('files_sha256') != source_sha
            or row.get('files_sha256') != source_sha):
        raise ValueError('Candidate source mismatch between Git, retention, and trajectory')
    visible = _load(store_path.with_suffix('.validation.json'), frozen)
    return dict(slot=row['candidate'], round=row['round'], binding=binding,
                files=files, files_sha256=source_sha, visible_receipt=visible,
                source_valid=row.get('source_valid') is True)


def _stage_plan(root, stage_index, stage, project, selected_receipt, contract, frozen):
    if (stage.get('stage_index') != stage_index
            or _load(root / f'stage-{stage_index}' / 'result.json', frozen) != stage):
        raise ValueError('Milestone result does not match the frozen trajectory')
    latest, rounds = {}, {}
    for row in stage['trajectory']:
        if row.get('kind') != 'builder':
            continue
        slot, round_number = row.get('candidate'), row.get('round')
        if (slot not in SLOTS or type(round_number) is not int
                or round_number <= rounds.get(slot, 0)):
            raise ValueError('Portfolio builder order is invalid')
        latest[slot], rounds[slot] = row, round_number
    if set(latest) != set(SLOTS) or stage.get('selected') not in SLOTS:
        raise ValueError('Portfolio does not contain all four candidate slots')
    candidates = {slot: _candidate(root, stage_index, latest[slot], frozen) for slot in SLOTS}
    selected = candidates[stage['selected']]
    if (selected['binding'] != stage.get('selected_binding')
            or selected['files'] != stage['files']):
        raise ValueError('Selected source is not the latest retained proposal in its slot')
    hidden = cases_for(project, stage_index, 'hidden')
    visible = cases_for(project, stage_index, 'visible')
    case_ids = [item.get('id') for item in hidden]
    if (not hidden or any(not isinstance(identifier, str) or not identifier for identifier in case_ids)
            or len(set(case_ids)) != len(case_ids)):
        raise ValueError('Cumulative hidden cases require distinct stable case IDs')
    selected_hidden = _receipt(_Replay(selected_receipt), selected['files'], hidden, contract)
    for candidate in candidates.values():
        candidate['visible_receipt'] = _receipt(
            _Replay(candidate['visible_receipt']), candidate['files'], visible, contract)
    return dict(status='ready', selected=selected['slot'], candidates=candidates,
                hidden_cases=hidden, selected_hidden=selected_hidden)


def _load_study(run, *, all_stages=False):
    """Verify every selected portfolio and all bindings before container work."""
    frozen = {}
    report = _load(run / 'results.json', frozen)
    registered = _load(run / 'preregistered.json', frozen)
    fixtures = _load(run / 'fixtures.json', frozen)
    contract = report.get('contract', {})
    if (report.get('experiment') != PROTOCOL or report.get('status') != 'finished'
            or report.get('unexecuted') != []
            or report.get('mode') not in ('live', 'rehearsal')
            or report.get('contract_sha') != digest(contract)
            or registered.get('contract') != contract
            or contract.get('protocol') != PROTOCOL
            or contract.get('hidden_feedback') is not False
            or contract.get('milestones') != 3 or contract.get('portfolio_initial') != 4
            or digest(fixtures) != contract.get('fixture_sha256')):
        raise ValueError('Diagnostic requires a finished study with matching contract and fixtures')
    source_hashes = contract.get('sources', {})
    if set(source_hashes) != set(CORE):
        raise ValueError('Frozen contract does not bind the expected execution sources')
    for name, expected in source_hashes.items():
        snapshot = run / 'source-snapshot' / 'gossip_harness' / name
        if _file_sha(snapshot) != expected or _file_sha(Path(__file__).parent / name) != expected:
            raise ValueError(f'Execution source does not match frozen contract: {name}')
        frozen[snapshot] = expected
        frozen[Path(__file__).parent / name] = expected
    projects = {project['id']: project for project in fixtures}
    if (len(projects) != len(fixtures) or list(projects) != contract.get('project_ids')
            or any(not re.fullmatch(r'[A-Za-z0-9_-]+', identifier) for identifier in projects)):
        raise ValueError('Frozen fixture project roster is invalid')
    repetitions = report.get('repetitions')
    if type(repetitions) is not int or not 1 <= repetitions <= 4:
        raise ValueError('Invalid completed study repetition count')
    expected_roster = {(project, policy, repetition) for project in projects
                       for policy in contract['policies'] for repetition in range(repetitions)}
    recorded_roster = [tuple(item) for item in registered.get('roster', [])]
    actual_roster = [(case['project_id'], case['policy'], case['repetition'])
                     for case in report['cases']]
    if (set(recorded_roster) != expected_roster or len(recorded_roster) != len(expected_roster)
            or set(actual_roster) != expected_roster or len(actual_roster) != len(expected_roster)):
        raise ValueError('Whole study roster is incomplete or duplicated')
    plans = []
    for case in report['cases']:
        if case['policy'] != 'reviewed-portfolio':
            continue
        run_id = f"{case['project_id']}-{case['policy']}-{case['repetition']}"
        root = _contained(run / run_id, run)
        if case.get('run_id') != run_id or Path(case['root']).resolve() != root:
            raise ValueError('Project result path does not match its frozen roster entry')
        if _load(root / 'result.json', frozen) != case:
            raise ValueError('Project result differs from completed study summary')
        state = _load(root / 'trajectory.json', frozen)
        if (frozen[root / 'trajectory.json'] != case.get('trajectory_sha256')
                or state.get('contract_sha') != report['contract_sha']
                or digest(state.get('files')) != case.get('files_sha256')):
            raise ValueError('Project trajectory hash, contract, or final source differs')
        stages = state.get('stages', [])
        if not isinstance(stages, list) or len(stages) > 3:
            raise ValueError('Project trajectory has an invalid milestone count')
        historical = {}
        if all_stages:
            history = case.get('historical', [])
            historical = {item['stage_index']: item for item in history}
            if (len(historical) != len(history) or set(historical) != set(range(len(stages)))
                    or any(type(item['stage_index']) is not int for item in history)):
                raise ValueError('Historical hidden evaluations do not match the reached milestones')
        for stage_index in (range(3) if all_stages else (2,)):
            plan = dict(run_id=run_id, project_id=case['project_id'], repetition=case['repetition'],
                        primary_accepted=case['accepted'], trajectory_sha256=case['trajectory_sha256'])
            if all_stages:
                plan.update(stage_index=stage_index, primary_stage_completed=False)
            if stage_index >= len(stages):
                plan.update(status='milestone_not_reached' if all_stages else 'final_milestone_not_reached',
                            candidates={})
                plans.append(plan)
                continue
            stage = stages[stage_index]
            if stage_index == len(stages) - 1 and stage.get('files') != state['files']:
                raise ValueError('Last milestone source differs from the frozen project source')
            selected_receipt = case['final_hidden']
            if all_stages:
                if historical[stage_index].get('binding') != stage.get('selected_binding'):
                    raise ValueError('Historical hidden evaluation binding differs from the selected proposal')
                selected_receipt = historical[stage_index]['hidden']
                plan['primary_stage_completed'] = stage.get('completed') is True
            plan.update(_stage_plan(root, stage_index, stage, projects[case['project_id']],
                                    selected_receipt, contract, frozen))
            plans.append(plan)
    if not plans:
        raise ValueError('Completed study has no reviewed portfolios to diagnose')
    return report, contract, plans, frozen


def _summarize_case(row):
    candidates = row['candidates']
    selected = candidates[row['selected']]
    correct = [slot for slot, candidate in candidates.items() if candidate['hidden_correct']]
    eligible_correct = [slot for slot in correct if candidates[slot]['selection_eligible']]
    row.update(candidate_coverage=bool(correct), correct_candidates=correct,
               eligible_candidate_coverage=bool(eligible_correct),
               eligible_correct_candidates=eligible_correct,
               selected_hidden_correct=selected['hidden_correct'],
               selector_regret=bool(correct and not selected['hidden_correct']),
               eligible_selector_regret=bool(eligible_correct and not selected['hidden_correct']),
               distinct_sources=len({candidate['files_sha256'] for candidate in candidates.values()}))
    pairs = []
    for left, right in combinations(SLOTS, 2):
        a, b = set(candidates[left]['failed_case_ids']), set(candidates[right]['failed_case_ids'])
        pairs.append(dict(left=left, right=right, shared_failed_case_ids=sorted(a & b),
                          shared_failures=len(a & b), union_failures=len(a | b),
                          failure_jaccard=len(a & b) / len(a | b) if a | b else None,
                          identical_failure_pattern=a == b,
                          identical_source=candidates[left]['files_sha256'] == candidates[right]['files_sha256']))
    patterns = {}
    for slot, candidate in candidates.items():
        key = tuple(candidate['failed_case_ids'])
        patterns.setdefault(key, []).append(slot)
    row['pairwise_failure_overlap'] = pairs
    row['shared_nonempty_failure_patterns'] = [dict(candidates=slots, failed_case_ids=list(pattern))
                                               for pattern, slots in patterns.items()
                                               if pattern and len(slots) > 1]
    failed_sets = [set(candidate['failed_case_ids']) for candidate in candidates.values()]
    row['failed_by_every_candidate'] = sorted(set.intersection(*failed_sets))
    row['status'] = 'finished'


def run(run_dir, output, *, validator_factory=None, all_stages=False):
    """Write a fresh post-hoc JSON report; never modify the input study."""
    if type(all_stages) is not bool:
        raise ValueError('all_stages must be a boolean')
    run_dir, output = Path(run_dir).resolve(), Path(output).absolute()
    if output.exists() or output.is_symlink():
        raise ValueError('Diagnostic output already exists; retained results are never overwritten')
    if output.resolve().is_relative_to(run_dir):
        raise ValueError('Diagnostic output must be outside the frozen study directory')
    study, contract, plans, frozen = _load_study(run_dir, all_stages=all_stages)
    factory = validator_factory or BlackboxValidator
    needs_execution = any(plan['status'] == 'ready' and any(
        candidate['files_sha256'] != plan['candidates'][plan['selected']]['files_sha256']
        for candidate in plan['candidates'].values()) for plan in plans)
    validator = None
    if needs_execution:
        validator = factory(contract['image'], timeout_seconds=contract['suite_timeout_seconds'],
                            case_timeout_seconds=contract['case_timeout_seconds'])
        okay, detail = validator.preflight()
        if not okay:
            raise RuntimeError('Docker preflight failed: ' + detail)
    output.parent.mkdir(parents=True, exist_ok=True)
    report = dict(schema_version=1, diagnostic=ALL_STAGES_DIAGNOSTIC if all_stages else DIAGNOSTIC,
                  classification='POSTHOC diagnostic',
                  preregistered_primary=False, study=str(run_dir), study_mode=study['mode'],
                  contract_sha256=study['contract_sha'], fixture_sha256=contract['fixture_sha256'],
                  diagnostic_source_sha256=_file_sha(__file__), status='running', api_calls=0,
                  fresh_validation_calls=0, reused_final_receipts=0, started_at=time.time(), cases=[],
                  interpretation=[
                      'Latest retained proposal per slot in milestone three only; earlier candidates are excluded.',
                      'Candidate coverage and selector regret use all final hidden cases, not a proof of complete correctness.',
                      'Eligible coverage additionally requires valid source and all visible checks; raw regret may include ineligible alternatives.',
                      'Matching selected source reuses its original final hidden receipt, not an independent rerun.',
                      'Failure overlaps are descriptive on one fixed suite; correlated patterns do not establish independent probabilities.',
                      'Results are never fed into repairs, selections, promotions, or primary acceptance outcomes.',
                  ])
    reuse_counter = 'reused_final_receipts'
    if all_stages:
        report.pop('reused_final_receipts')
        reuse_counter = 'reused_historical_receipts'
        report[reuse_counter] = 0
        report['interpretation'] = [
            'Latest retained proposal per slot in each reached milestone; superseded proposals are excluded.',
            'Each pool faces only the cumulative hidden suite through its own milestone, with all outcomes collected after the original trajectory froze.',
            'Matching selected source reuses its retrospective historical receipt, not an independent rerun.',
            'At milestone three the historical receipt is a separate execution from the final acceptance receipt; nondeterministic outcomes can differ and primary acceptance remains unchanged.',
            'Earlier alternative implementations were not carried through later milestones; stage coverage is not counterfactual whole-project completion.',
            'Stage pools are repeated observations within the same project trajectories, not independent projects.',
            *report['interpretation'][2:3], *report['interpretation'][4:],
        ]
    # Preflight can take time; acquire the fresh output exclusively so a report
    # created by another process in the meantime is not replaced.
    with output.open('x'):
        pass
    _save(output, report)
    try:
        for plan in plans:
            _unchanged(frozen)
            row = {key: plan[key] for key in ('run_id', 'project_id', 'repetition', 'primary_accepted',
                                             'trajectory_sha256', 'status')}
            if all_stages:
                row.update(stage_index=plan['stage_index'],
                           primary_stage_completed=plan['primary_stage_completed'])
            report['cases'].append(row)
            if plan['status'] != 'ready':
                _save(output, report)
                continue
            row.update(status='running', selected=plan['selected'], candidates={})
            selected_sha = plan['candidates'][plan['selected']]['files_sha256']
            for slot, candidate in plan['candidates'].items():
                reuse = candidate['files_sha256'] == selected_sha
                receipt = (_receipt(_Replay(plan['selected_hidden']), candidate['files'],
                                    plan['hidden_cases'], contract) if reuse else
                           _receipt(validator, candidate['files'], plan['hidden_cases'], contract))
                report[reuse_counter if reuse else 'fresh_validation_calls'] += 1
                failed = [case['id'] for case, outcome in zip(plan['hidden_cases'], receipt['outcomes'])
                          if not outcome['passed']]
                row['candidates'][slot] = dict(
                    binding=candidate['binding'], files_sha256=candidate['files_sha256'], round=candidate['round'],
                    hidden_correct=receipt['passed'], hidden_cases=len(receipt['outcomes']),
                    hidden_cases_passed=len(receipt['outcomes']) - len(failed), failed_case_ids=sorted(failed),
                    selection_eligible=candidate['source_valid'] and candidate['visible_receipt']['passed'],
                    source_valid=candidate['source_valid'], visible_passed=candidate['visible_receipt']['passed'],
                    reused_final_hidden=reuse, hidden_receipt=receipt)
                if all_stages:
                    row['candidates'][slot]['reused_historical_hidden'] = row['candidates'][slot].pop('reused_final_hidden')
                _save(output, report)
            _summarize_case(row)
            _save(output, report)
        _unchanged(frozen)
        analyzed = [case for case in report['cases'] if case['status'] == 'finished']
        report['summary'] = dict(portfolios=len(plans), analyzed=len(analyzed),
                                 final_milestone_not_reached=len(plans) - len(analyzed),
                                 candidate_coverage=sum(case['candidate_coverage'] for case in analyzed),
                                 eligible_candidate_coverage=sum(case['eligible_candidate_coverage'] for case in analyzed),
                                 selected_hidden_correct=sum(case['selected_hidden_correct'] for case in analyzed),
                                 selector_regret=sum(case['selector_regret'] for case in analyzed),
                                 eligible_selector_regret=sum(case['eligible_selector_regret'] for case in analyzed))
        if all_stages:
            summary = report['summary']
            summary['stage_pools'] = summary.pop('portfolios')
            summary['portfolio_trajectories'] = len({plan['run_id'] for plan in plans})
            summary['milestone_not_reached'] = summary.pop('final_milestone_not_reached')
            summary['by_stage'] = {
                str(stage_index): dict(
                    analyzed=sum(case['stage_index'] == stage_index for case in analyzed),
                    candidate_coverage=sum(case['candidate_coverage'] for case in analyzed if case['stage_index'] == stage_index),
                    eligible_candidate_coverage=sum(case['eligible_candidate_coverage'] for case in analyzed if case['stage_index'] == stage_index),
                    selected_hidden_correct=sum(case['selected_hidden_correct'] for case in analyzed if case['stage_index'] == stage_index),
                    selector_regret=sum(case['selector_regret'] for case in analyzed if case['stage_index'] == stage_index),
                    eligible_selector_regret=sum(case['eligible_selector_regret'] for case in analyzed if case['stage_index'] == stage_index))
                for stage_index in range(3)
            }
        report['status'] = 'finished'
    except (Exception, KeyboardInterrupt) as error:
        report['status'] = 'interrupted'
        report['failure'] = str(error) or type(error).__name__
        raise
    finally:
        report['finished_at'] = time.time()
        _save(output, report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--all-stages', action='store_true',
                        help='Diagnose every reached milestone using cumulative historical hidden suites')
    args = parser.parse_args()
    try:
        report = run(args.run, args.output, all_stages=args.all_stages)
    except Exception as error:
        print(f'Post-hoc diagnostic stopped: {error}', file=sys.stderr)
        return 1
    print(json.dumps(dict(status=report['status'], summary=report['summary'],
                          fresh_validation_calls=report['fresh_validation_calls'], api_calls=0)))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
