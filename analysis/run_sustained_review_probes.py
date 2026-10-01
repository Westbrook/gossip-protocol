#!/usr/bin/env python3
"""Source-informed POST-HOC probes: secondary diagnostics, never primary scores.

Run only after the entire live study finishes. No API or credential access occurs.
Candidate source is read as data and executes only through BlackboxValidator.
Pass --cases PROJECT SHA256 ARTIFACT once per project; both projects are required.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import sys
import time

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY))
from gossip_harness.blackbox_validator import BlackboxValidator
from gossip_harness.gitstore import GitStore
from gossip_harness.pilot import _save
from gossip_harness.sustained_experiment import CORE, checked_evaluate, digest

FROZEN_CASE_HASHES = {
    'workflow': 'e77dd4f91e797f0e63f59c35d447a8d2fd66128e9b0966a83cc1b8da4f6901bf',
    'inventory': 'a5fe99cc4cb518cfb9e3f3da95f65b560b6a94b80ced6b1e31bfdf567fe24537',
}
POLICIES = ('strong-single', 'cheap-sequential', 'reviewed-portfolio')
LABEL = 'Source-informed post-hoc secondary diagnostic; primary scores unchanged'


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def completion_gate(report, preregistered):
    require(report.get('experiment') == 'sustained-quality-v1' and report.get('mode') == 'live', 'Expected live sustained study')
    require(report.get('status') == 'finished' and report.get('unexecuted') == [], 'Entire study must finish with no unexecuted trajectories')
    require(type(report.get('repetitions')) is int and report['repetitions'] == 2, 'Exactly two repetitions are required')
    expected = {(project, policy, repetition) for project in ('workflow', 'inventory') for policy in POLICIES for repetition in range(2)}
    rows = report.get('cases', [])
    actual = [(c.get('project_id'), c.get('policy'), c.get('repetition')) for c in rows]
    roster = [tuple(item) for item in preregistered.get('roster', [])]
    require(len(actual) == len(expected) and set(actual) == expected, 'Completed case roster is incomplete, duplicate, or unexpected')
    require(len(roster) == len(expected) and set(roster) == expected, 'Preregistered roster does not match exact study')
    return sorted(rows, key=lambda c: (c['project_id'], POLICIES.index(c['policy']), c['repetition']))


def validate_output(run_root, output):
    require(not output.exists() and not output.is_symlink(), 'Output must be a fresh directory')
    require(output != run_root and run_root not in output.parents and output not in run_root.parents,
            'Output must be outside the frozen study directory and its ancestors')


def prepare_inputs(run_root, output, case_specs):
    """Read and bind every input before creating output or contacting Docker."""
    validate_output(run_root, output)
    raw_inputs = {}

    def read(path):
        require(path.is_file() and not path.is_symlink(), f'Expected ordinary input file: {path}')
        raw = path.read_bytes()
        raw_inputs[path.resolve()] = raw
        return raw

    report = json.loads(read(run_root / 'results.json'))
    preregistered = json.loads(read(run_root / 'preregistered.json'))
    targets = completion_gate(report, preregistered)
    contract = report['contract']
    require(contract == preregistered.get('contract') and digest(contract) == report.get('contract_sha'), 'Contract binding changed')
    require(contract.get('protocol') == 'sustained-quality-v1' and contract.get('policies') == list(POLICIES)
            and contract.get('project_ids') == ['workflow', 'inventory'] and contract.get('milestones') == 3,
            'Unexpected frozen study contract')
    require(re.fullmatch(r'sha256:[0-9a-f]{64}', contract.get('image', '')) is not None, 'Frozen Docker image must be content-addressed')
    require(contract.get('case_timeout_seconds') == 12 and contract.get('suite_timeout_seconds') == 300, 'Unexpected frozen timeouts')
    require(set(contract.get('sources', {})) == set(CORE), 'Frozen contract must bind every helper source')
    for name, expected_hash in contract['sources'].items():
        require(Path(name).name == name and name.endswith('.py'), 'Unsafe contract source path')
        require(sha(read(REPOSITORY / 'gossip_harness' / name)) == expected_hash, f'Current helper changed: {name}')
        require(sha(read(run_root / 'source-snapshot/gossip_harness' / name)) == expected_hash, f'Frozen source changed: {name}')
    fixtures = json.loads(read(run_root / 'fixtures.json'))
    require(digest(fixtures) == contract['fixture_sha256'], 'Frozen fixture binding changed')
    projects = {p['id']: p for p in fixtures}
    require(len(case_specs) == 2 and {item[0] for item in case_specs} == set(FROZEN_CASE_HASHES),
            'Supply exactly one frozen artifact for each project')
    cases_by_project = {}
    for project_id, expected_hash, artifact_path in case_specs:
        require(expected_hash == FROZEN_CASE_HASHES[project_id], f'Unexpected prereviewed artifact hash: {project_id}')
        raw_cases = read(Path(artifact_path).resolve())
        require(sha(raw_cases) == expected_hash, f'Frozen post-hoc cases artifact changed: {project_id}')
        artifact = json.loads(raw_cases)
        cases = artifact['cases']
        require(artifact.get('posthoc') is True and artifact.get('project') == project_id
                and artifact.get('evaluation_stage') == 3 and len(cases) == (3 if project_id == 'workflow' else 2),
                f'Unexpected probe artifact: {project_id}')
        require(len({case['id'] for case in cases}) == len(cases), 'Probe IDs must be unique')
        cases_by_project[project_id] = cases
    bindings, skipped = [], []
    for result in targets:
        project_id = result['project_id']
        project = projects[project_id]
        run_id = f"{project_id}-{result['policy']}-{result['repetition']}"
        require(result.get('run_id') == run_id, f'Unexpected run identifier: {run_id}')
        root = run_root / run_id
        require(Path(result['root']).resolve() == root, f'Unexpected trajectory root: {run_id}')
        require(json.loads(read(root / 'result.json')) == result, f'Per-trajectory result differs: {run_id}')
        trajectory_raw = read(root / 'trajectory.json')
        require(sha(trajectory_raw) == result['trajectory_sha256'], f'Trajectory changed: {run_id}')
        state = json.loads(trajectory_raw)
        require(state['contract_sha'] == report['contract_sha'], f'Trajectory contract differs: {run_id}')
        common = dict(run_id=run_id, project_id=project_id, policy=result['policy'], repetition=result['repetition'],
                      primary_accepted=result['accepted'], primary_status=result['status'],
                      primary_milestones_completed=result['milestones_completed'])
        if len(state.get('stages', [])) < 3:
            require(result['accepted'] is False, f'Accepted trajectory never reached final milestone: {run_id}')
            skipped.append(dict(**common, reason='Final milestone was not reached; earlier code is not substituted'))
            continue
        require(len(state['stages']) == 3 and state['stages'][-1].get('stage_index') == 2,
                f'Unexpected final milestone structure: {run_id}')
        files = state['files']
        file_hash = digest(files)
        require(file_hash == result['files_sha256'], f'Final files binding mismatch: {run_id}')
        require(set(files) == set(project['initial_files']), f'Unexpected source file set: {run_id}')
        for name in set(files) - set(project['allowed_paths']):
            require(files[name] == project['initial_files'][name], f'Fixed transport changed: {run_id}/{name}')
        final_stage = state['stages'][-1]
        require(final_stage['files'] == files and json.loads(read(root / 'stage-2/result.json')) == final_stage,
                f'Final stage binding changed: {run_id}')
        binding = final_stage.get('selected_binding')
        require(isinstance(binding, dict) and binding.get('files_sha256') == file_hash,
                f'Final selected proposal binding missing or mismatched: {run_id}')
        selected_path = Path(binding['store_path']).resolve()
        require(root in selected_path.parents, f'Selected Git binding escapes trajectory: {run_id}')
        selected = GitStore(selected_path)
        require(selected.read_files(binding['tip_sha']) == files, f'Selected Git tree differs: {run_id}')
        if final_stage['completed']:
            checkpoint_path = Path(state['store_path']).resolve()
            require(root in checkpoint_path.parents, f'Checkpoint binding escapes trajectory: {run_id}')
            checkpoint = GitStore(checkpoint_path)
            require(checkpoint.head() == state['head'] and checkpoint.read_files(state['head']) == files,
                    f'Final completed checkpoint differs: {run_id}')
        release_head = None
        if result['accepted']:
            require(result['milestones_completed'] == 3 and final_stage['completed'] is True,
                    f'Accepted trajectory lacks completed milestones: {run_id}')
            release = GitStore(root / 'release.git')
            release_head = release.head()
            require(release_head == result['release_head'] == result['exact_tested_sha']
                    and release.read_files(release_head) == files, f'Release binding differs: {run_id}')
            export = root / 'accepted'
            entries = list(export.rglob('*'))
            require(export.is_dir() and not export.is_symlink() and not any(p.is_symlink() for p in entries),
                    f'Invalid accepted export: {run_id}')
            export_files = {p.relative_to(export).as_posix(): read(p).decode('utf-8') for p in entries if p.is_file()}
            require(export_files == files, f'Accepted export differs from tested source: {run_id}')
        else:
            require(result.get('release_head') is None and result.get('exact_tested_sha') is None,
                    f'Unaccepted trajectory unexpectedly claims a release: {run_id}')
        for kind in ('visible', 'hidden'):
            receipt = result['final_' + kind]
            suite = [case for stage in project['stages'] for case in stage[kind + '_cases']]
            require(type(receipt.get('passed')) is bool and receipt.get('source_sha256') == file_hash
                    and receipt.get('suite_sha256') == digest(suite) and receipt.get('image_id') == contract['image']
                    and receipt.get('case_timeout_seconds') == contract['case_timeout_seconds']
                    and receipt.get('timeout_seconds') == contract['suite_timeout_seconds'],
                    f'Primary receipt binding changed: {run_id}')
        bindings.append(dict(**common, release_head=release_head, evaluation_commit=binding['tip_sha'],
                             selected_binding=binding, files_sha256=file_hash, files=files))
    require(len(bindings) + len(skipped) == 12, 'All twelve final attempts must be accounted for')
    return report, contract, cases_by_project, bindings, skipped, raw_inputs


def unchanged(raw_inputs):
    for path, raw in raw_inputs.items():
        require(path.is_file() and not path.is_symlink() and path.read_bytes() == raw, f'Input changed after snapshot: {path}')


def run(run_root, output, case_specs):
    report, contract, cases_by_project, bindings, skipped, raw_inputs = prepare_inputs(run_root, output, case_specs)
    output.mkdir(parents=True, exist_ok=False)
    snapshots = output / 'inputs'
    snapshots.mkdir()
    inventory = []
    for index, (path, raw) in enumerate(sorted(raw_inputs.items(), key=lambda item: str(item[0]))):
        destination = snapshots / f'{index:03d}-{path.name}'
        destination.write_bytes(raw)
        inventory.append(dict(original=str(path), snapshot=str(destination.relative_to(output)), sha256=sha(raw)))
    runner_raw = Path(__file__).read_bytes()
    (output / 'runner.py').write_bytes(runner_raw)
    _save(output / 'manifest.json', dict(label=LABEL, source_informed=True, primary_scores_modified=False,
          original_study=str(run_root), original_contract=contract, original_contract_sha=report['contract_sha'],
          cases_file_sha256=FROZEN_CASE_HASHES, cases_sha256={p:digest(c) for p,c in cases_by_project.items()},
          runner_sha256=sha(runner_raw), runner_original_path=str(Path(__file__).resolve()),
          case_arguments=[[project, expected, str(Path(path).resolve())] for project, expected, path in case_specs],
          inputs=inventory, skipped=skipped,
          bindings=[{k: v for k, v in b.items() if k != 'files'} for b in bindings]))
    _save(output / 'cases.json', cases_by_project)
    progress = dict(label=LABEL, status='running', original_study=str(run_root), primary_scores_modified=False,
                    executions=[], skipped=skipped, planned_attempts=12, unexecuted=[b['run_id'] for b in bindings], started_at=time.time())
    _save(output / 'results.json', progress)
    validator = BlackboxValidator(contract['image'], timeout_seconds=contract['suite_timeout_seconds'],
                                  case_timeout_seconds=contract['case_timeout_seconds'])
    try:
        unchanged(raw_inputs)
        okay, detail = validator.preflight()
        _save(output / 'docker-preflight.json', dict(okay=okay, detail=detail, image=contract['image']))
        require(okay, 'Docker preflight failed: ' + detail)
        for binding in bindings:
            unchanged(raw_inputs)
            cases = cases_by_project[binding['project_id']]
            receipt_path = output / 'receipts' / (binding['run_id'] + '.json')
            receipt_path.parent.mkdir(exist_ok=True)

            class RecordingValidator:
                def evaluate(self, files, suite):
                    raw_receipt = validator.evaluate(files, suite)
                    _save(receipt_path, raw_receipt)  # Retain even a rejected infrastructure receipt.
                    return raw_receipt

            receipt = checked_evaluate(RecordingValidator(), binding['files'], cases)
            require(receipt.get('image_id') == contract['image'], 'Probe receipt image differs from frozen image')
            result = {k: v for k, v in binding.items() if k != 'files'}
            result.update(all_probes_passed=receipt['passed'], passed_cases=sum(o['passed'] for o in receipt['outcomes']),
                          total_cases=len(cases), outcomes=receipt['outcomes'], receipt=str(receipt_path.relative_to(output)),
                          receipt_sha256=sha(receipt_path.read_bytes()))
            progress['executions'].append(result)
            progress['unexecuted'].remove(binding['run_id'])
            _save(output / 'results.json', progress)
            print(f"{binding['run_id']}: {result['passed_cases']}/{len(cases)} post-hoc probes passed", flush=True)
        unchanged(raw_inputs)
        summaries = {}
        for project_id, cases in cases_by_project.items():
            summaries[project_id] = {}
            for policy in POLICIES:
                rows = [r for r in progress['executions'] if r['project_id'] == project_id and r['policy'] == policy]
                omitted = [r for r in skipped if r['project_id'] == project_id and r['policy'] == policy]
                summaries[project_id][policy] = dict(planned_attempts=2, evaluated=len(rows), unreached_final_stage=len(omitted),
                     projects_passing_all_probes=sum(r['all_probes_passed'] for r in rows),
                     probe_cases_passed=sum(r['passed_cases'] for r in rows), probe_cases_total=sum(r['total_cases'] for r in rows),
                     primary_accepted_unchanged=sum(r['primary_accepted'] for r in rows + omitted),
                     failures_by_probe={case['id']:sum(not r['outcomes'][i]['passed'] for r in rows) for i, case in enumerate(cases)})
        progress.update(status='finished', summaries=summaries, inputs_unchanged=True)
    except BaseException as error:
        progress.update(status='interrupted', failure=str(error) or type(error).__name__)
        raise
    finally:
        progress['finished_at'] = time.time()
        _save(output / 'results.json', progress)
    return progress


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', type=Path, required=True, help='Completed original live study directory')
    parser.add_argument('--output', type=Path, required=True, help='Fresh separate diagnostics directory')
    parser.add_argument('--cases', action='append', nargs=3, metavar=('PROJECT', 'SHA256', 'ARTIFACT'), required=True,
                        help='Repeat once for workflow and once for inventory, using their frozen artifact hashes')
    args = parser.parse_args()
    try:
        result = run(args.run.resolve(), args.output.resolve(), args.cases)
    except Exception as error:
        print(f'Post-hoc probes stopped: {error}', file=sys.stderr)
        return 1
    print(json.dumps(dict(status=result['status'], label=LABEL, summaries=result['summaries'])), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
