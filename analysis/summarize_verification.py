"""Descriptive, post-freeze study summary; never executes candidate code."""
from __future__ import annotations

import argparse
from collections import Counter
import json
from pathlib import Path


def summarize(root: Path) -> dict:
    root = root.resolve()
    report = json.loads((root / 'results.json').read_text())
    fixtures = {p['id']: p for p in json.loads((root / 'fixtures.json').read_text())['projects']}
    policies = {}
    all_reservations = []
    for policy in report['contract']['policies']:
        rows = [case for case in report['cases'] if case['policy'] == policy]
        metrics = Counter()
        rejection_reasons = Counter()
        role_costs = Counter()
        calls = []
        unchanged = prior_regressions = prior_opportunities = 0
        project_rows = []
        for case in rows:
            case_root = Path(case['root'])
            trajectory = json.loads((case_root / 'trajectory.json').read_text())
            for stage in trajectory['stages']:
                metrics.update(stage['metrics'])
                prior_ids = {c['id'] for milestone in fixtures[case['project_id']]['stages'][:stage['stage_index']]
                             for c in milestone['visible_cases']}
                previous_source = {}
                for event in stage['trajectory']:
                    if event['kind'] != 'builder':
                        continue
                    slot = event['slot']
                    if slot in previous_source:
                        unchanged += previous_source[slot] == event['files_sha256']
                    elif prior_ids:
                        prior_opportunities += 1
                        prior_regressions += bool(prior_ids.intersection(event['failures']))
                    previous_source[slot] = event['files_sha256']
                for gate in stage['probe_receipts']:
                    rejection_reasons.update(item['reason'] for item in gate['receipt']['rejected'])
                for invocation in stage['invocations']:
                    calls.append(invocation)
                    role_costs[invocation['role']] += invocation['usage_units'] or 0
            project_rows.append(dict(project_id=case['project_id'], repetition=case['repetition'],
                accepted=case['accepted'], milestones=case['milestones_completed'],
                hidden_passed=sum(o['passed'] for o in case['final_hidden']['outcomes']),
                hidden_total=len(case['final_hidden']['outcomes']),
                requirements_passed=sum(case['requirement_coverage'].values()),
                requirements_total=len(case['requirement_coverage']), cost_micro_usd=case['usage_micro_usd']))
        if len({c['reservation'] for c in calls}) != len(calls):
            raise ValueError('Duplicate invocation in descriptive accounting')
        all_reservations.extend(c['reservation'] for c in calls)
        policies[policy] = dict(planned_trajectories=report['repetitions'] * len(fixtures),
            evaluated_trajectories=len(rows), accepted=sum(c['accepted'] for c in rows),
            milestones_completed=sum(c['milestones_completed'] for c in rows),
            primary_visible_completion_with_private_failure=sum(c['milestones_completed'] == 4
                and c['final_visible']['passed'] and not c['final_hidden']['passed'] for c in rows),
            actual_sigkills=sum(c.get('fault', {}).get('returncode') == -9 for c in rows),
            resumed_trajectories=sum(c.get('resume', {}).get('verified') is True for c in rows),
            stale_evidence_rejections=sum(c.get('integration', {}).get('stale_probe_status') == 'stale' for c in rows),
            invocations=len(calls), cost_micro_usd=sum(c['usage_units'] or 0 for c in calls),
            role_cost_micro_usd=dict(role_costs), metrics=dict(metrics),
            generated_probe_rejections=dict(rejection_reasons), unchanged_source_repairs=unchanged,
            initial_proposals_breaking_prior_public_checks=prior_regressions,
            initial_proposals_tested_against_prior_public_checks=prior_opportunities,
            projects=project_rows)
    if len(set(all_reservations)) != len(all_reservations):
        raise ValueError('Invocation appears under multiple policies')
    recorded_delta = report['budget']['spent_or_reserved'] - report['budget_before']['spent_or_reserved']
    attributed = sum(value['cost_micro_usd'] for value in policies.values())
    return dict(protocol=report['experiment'], mode=report['mode'], status=report['status'],
        contract_sha=report['contract_sha'], policies=policies, budget=report['budget'],
        incremental_micro_usd=report.get('incremental_micro_usd'),
        policy_accounting_scope='Final-evaluated trajectories only; active/censored charges may appear only in the recorded ledger delta.',
        recorded_ledger_delta_micro_usd=recorded_delta,
        ledger_delta_unattributed_to_evaluated_trajectories_micro_usd=recorded_delta - attributed,
        censored=report.get('censored', []), unexecuted=report['unexecuted'],
        active_case=report.get('active_case'),
        limitations=['Descriptive post-freeze aggregation, not an independent audit or statistical superiority test.',
            'Two synthetic application identities; repeated trajectories are not independent repositories.',
            'Generated tests are candidate-aware and oracle-assisted; arms receive unequal model-generated evidence.',
            'Final-pool diagnostics, if run, are separate from these primary results.',
            'Token accounting excludes local compute, benchmark/oracle development, and human work.'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    value = summarize(args.root)
    if args.output.exists():
        raise ValueError('Use a fresh summary path')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(value, indent=2) + '\n')
    print(json.dumps({p: {k: v[k] for k in ('accepted', 'evaluated_trajectories', 'milestones_completed',
                                          'cost_micro_usd')} for p, v in value['policies'].items()}))


if __name__ == '__main__':
    main()
