#!/usr/bin/env python3
"""Batched, independent post-hoc probes of a completed frozen sustained study.

The original strict input gates still apply. This new diagnostic protocol binds
its own concurrency contract; it neither changes nor resumes the primary study.
Every eligible attempt executes physically, including identical source trees.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
from dataclasses import asdict
import json
from pathlib import Path
import sys
import time
from types import MappingProxyType
from typing import Any

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY))

from analysis import run_sustained_review_probes as legacy
from devtools.study_validation import StudyValidationSession, study_signals, study_validation_contract
from devtools.validation_session import ResourceBudget

PROTOCOL = 'sustained-review-probes-v2'
PURPOSE = 'repeatability'
REASON = ('Independent source-informed post-hoc diagnostic of each frozen final '
          'attempt; never reused as primary acceptance or as another attempt.')
LABEL = legacy.LABEL
FROZEN_CASE_HASHES = legacy.FROZEN_CASE_HASHES
POLICIES = legacy.POLICIES
prepare_inputs = legacy.prepare_inputs
unchanged = legacy.unchanged
require = legacy.require
sha = legacy.sha
digest = legacy.digest
_save = legacy._save


def _summaries(executions, skipped, cases_by_project):
    summaries: dict[str, dict[str, dict[str, Any]]] = {}
    for project_id, cases in cases_by_project.items():
        summaries[project_id] = {}
        for policy in POLICIES:
            rows = [r for r in executions if r['project_id'] == project_id and r['policy'] == policy]
            omitted = [r for r in skipped if r['project_id'] == project_id and r['policy'] == policy]
            summaries[project_id][policy] = dict(
                planned_attempts=2, evaluated=len(rows), unreached_final_stage=len(omitted),
                projects_passing_all_probes=sum(r['all_probes_passed'] for r in rows),
                probe_cases_passed=sum(r['passed_cases'] for r in rows),
                probe_cases_total=sum(r['total_cases'] for r in rows),
                primary_accepted_unchanged=sum(r['primary_accepted'] for r in rows + omitted),
                failures_by_probe={case['id']: sum(not r['outcomes'][i]['passed'] for r in rows)
                                   for i, case in enumerate(cases)})
    return summaries


def run(run_root, output, case_specs, *, budget=ResourceBudget()):
    """Validate once, snapshot once, batch independent checks, and reconcile once.

    The expensive original study accounting runs before output creation and
    before Docker. Its detached snapshot is the sole evaluation input. A final
    source check invalidates the diagnostic if any bound input changes in flight.
    The validator retains started jobs even when another job has an infra error.
    """
    budget.capacity()
    require(not Path(output).is_symlink(), 'Output must be a fresh directory, not a symlink')
    run_root, output = Path(run_root).resolve(), Path(output).resolve()
    report, primary_contract, cases_by_project, bindings, skipped, raw_inputs = prepare_inputs(
        run_root, output, case_specs)
    # Detach mutable dictionaries from the parser and caller without rereading
    # the large primary study. Raw bytes and their path mapping are immutable.
    report, primary_contract, cases_by_project, bindings, skipped = json.loads(json.dumps(
        [report, primary_contract, cases_by_project, bindings, skipped], allow_nan=False))
    runner_paths = (Path(__file__).resolve(), Path(legacy.__file__).resolve())
    tracked_inputs = dict(raw_inputs)
    for path in runner_paths:
        raw = path.read_bytes()
        require(path not in tracked_inputs or tracked_inputs[path] == raw,
                f'Runner source changed during input preparation: {path}')
        tracked_inputs[path] = raw
    frozen_inputs = MappingProxyType(tracked_inputs)
    arguments = dict(image=primary_contract['image'], protocol=PROTOCOL, budget=budget,
                     timeout_seconds=primary_contract['suite_timeout_seconds'],
                     case_timeout_seconds=primary_contract['case_timeout_seconds'],
                     deterministic_visible=False, support_paths=runner_paths)
    adapter_contract = study_validation_contract(**arguments)
    protocol_contract = dict(
        protocol=PROTOCOL, original_contract_sha=report['contract_sha'],
        validation=adapter_contract, purpose=PURPOSE, reason=REASON,
        snapshot_policy='strict-original-accounting; immutable-batch; pre-and-post-check',
        primary_scores_modified=False, ordered_attempts=[b['run_id'] for b in bindings],
        cases_sha256={project: digest(cases) for project, cases in cases_by_project.items()},
        cases_file_sha256=FROZEN_CASE_HASHES,
        runner_sources_sha256={str(p.relative_to(REPOSITORY)): sha(frozen_inputs[p])
                               for p in runner_paths})
    output.mkdir(parents=True, exist_ok=False)
    progress = dict(label=LABEL, protocol=PROTOCOL, status='running', original_study=str(run_root),
                    primary_scores_modified=False, contract_sha256=digest(protocol_contract),
                    executions=[], skipped=skipped, planned_attempts=12,
                    unexecuted=[b['run_id'] for b in bindings],
                    unresolved_attempts=[b['run_id'] for b in bindings], started_at=time.time())
    _save(output / 'results.json', progress)
    session = None
    lifecycle = ExitStack()
    try:
        snapshots = output / 'inputs'
        snapshots.mkdir()
        inventory = []
        for index, (path, raw) in enumerate(sorted(frozen_inputs.items(), key=lambda item: str(item[0]))):
            destination = snapshots / f'{index:03d}-{path.name}'
            destination.write_bytes(raw)
            inventory.append(dict(original=str(path), snapshot=str(destination.relative_to(output)),
                                  sha256=sha(raw), bytes=len(raw)))
        _save(output / 'manifest.json', dict(
            label=LABEL, source_informed=True, primary_scores_modified=False,
            original_study=str(run_root), original_contract=primary_contract,
            original_contract_sha=report['contract_sha'], contract=protocol_contract,
            contract_sha256=digest(protocol_contract),
            case_arguments=[[project, expected, str(Path(path).resolve())]
                            for project, expected, path in case_specs],
            inputs=inventory, skipped=skipped,
            bindings=[{k: v for k, v in b.items() if k != 'files'} for b in bindings],
            snapshot_bytes_staged=sum(len(raw) for raw in frozen_inputs.values())))
        _save(output / 'cases.json', cases_by_project)
        # Check immediately before adapter construction as well as at the end;
        # source preparation and staging must not grant a stale preflight.
        unchanged(frozen_inputs)
        envelopes = []
        if bindings:
            session = StudyValidationSession(output / 'validation', **arguments)
            lifecycle.enter_context(study_signals(session))
            require(session.contract == adapter_contract, 'Validation contract changed before execution')
            envelopes = session.evaluate_many(
                [dict(label=b['run_id'], files=b['files'], source_sha256=b['files_sha256'],
                      cases=cases_by_project[b['project_id']]) for b in bindings],
                purpose=PURPOSE, reason=REASON, deterministic=False)
        require(len(envelopes) == len(bindings), 'Incomplete probe batch')
        for binding, envelope in zip(bindings, envelopes):
            receipt, validation = envelope['receipt'], envelope['validation']
            require(validation['label'] == binding['run_id'] and validation['physical'] is True
                    and validation.get('reuse') is None and validation['purpose'] == PURPOSE
                    and validation['reason'] == REASON, 'Probe execution must be ordered and independent')
            require(receipt.get('image_id') == primary_contract['image'],
                    'Probe receipt image differs from frozen image')
            result = {k: v for k, v in binding.items() if k != 'files'}
            result.update(all_probes_passed=receipt['passed'],
                          passed_cases=sum(o['passed'] for o in receipt['outcomes']),
                          total_cases=len(cases_by_project[binding['project_id']]),
                          outcomes=receipt['outcomes'], validation=validation,
                          receipt_sha256=digest(receipt))
            progress['executions'].append(result)
            progress['unexecuted'].remove(binding['run_id'])
            progress['unresolved_attempts'].remove(binding['run_id'])
        unchanged(frozen_inputs)
        require(study_validation_contract(**arguments) == adapter_contract,
                'Validation contract changed during execution')
        if session is not None:
            session.raise_if_cancelled()
        progress.update(status='finished', inputs_unchanged=True,
                        summaries=_summaries(progress['executions'], skipped, cases_by_project),
                        physical_executions=len(envelopes), reused_executions=0,
                        validation_summary='validation/session.json' if bindings else None)
    except BaseException as error:
        progress.update(status='interrupted', failure=str(error) or type(error).__name__,
                        validation_summary='validation/session.json' if (output / 'validation').exists() else None)
        # An unusable batch can still contain physical attempts. Preserve the
        # distinction between unstarted work and work without usable evidence.
        if session is not None:
            try:
                summary = session.summary()
                if isinstance(summary, dict) and isinstance(summary.get('results'), list):
                    attempts = []
                    for observation in summary['results']:
                        index = observation.get('logical_index')
                        if type(index) is int and 0 <= index < len(bindings):
                            attempts.append(dict(run_id=bindings[index]['run_id'],
                                physical=observation.get('physical') is True,
                                status=observation.get('status'),
                                artifact_path=observation.get('artifact_path')))
                    progress['retained_attempts'] = attempts
                    started = {attempt['run_id'] for attempt in attempts if attempt['physical']}
                    progress['unexecuted'] = [b['run_id'] for b in bindings if b['run_id'] not in started]
            except Exception:
                # Never obscure the original failure if the session itself was
                # interrupted before its first summary was published.
                pass
        raise
    finally:
        try:
            # Pass through an active error so restoration cannot obscure it;
            # a signal arriving at normal exit still invalidates completion.
            lifecycle.__exit__(*sys.exc_info())
        except BaseException as error:
            progress.update(status='interrupted', failure=str(error) or type(error).__name__)
            raise
        finally:
            progress['finished_at'] = time.time()
            _save(output / 'results.json', progress)
    return progress


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--cases', action='append', nargs=3, metavar=('PROJECT', 'SHA256', 'ARTIFACT'),
                        required=True, help='One exact frozen post-hoc artifact per project')
    parser.add_argument('--workers', type=int, default=2)
    parser.add_argument('--cpus', type=int, default=2)
    parser.add_argument('--memory-mib', type=int, default=512)
    parser.add_argument('--outer-parallelism', type=int, default=1)
    args = parser.parse_args()
    budget = ResourceBudget(args.workers, args.cpus, args.memory_mib, args.outer_parallelism)
    try:
        result = run(args.run, args.output, args.cases, budget=budget)
    except Exception as error:
        print(f'Post-hoc probes stopped: {error}', file=sys.stderr)
        return 1
    print(json.dumps(dict(status=result['status'], label=LABEL, protocol=PROTOCOL,
                          resource_budget=asdict(budget), summaries=result['summaries'])), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
