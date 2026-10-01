"""Offline safety regressions for the additive batched post-hoc protocol."""
from __future__ import annotations

from contextlib import ExitStack
import json
import os
from pathlib import Path
import signal
import tempfile
import unittest
from unittest.mock import patch

from analysis import run_sustained_review_probes_v2 as runner


class ProbeBatchSafetyTests(unittest.TestCase):
    def plan(self, directory):
        root, output = Path(directory) / 'primary', Path(directory) / 'secondary'
        root.mkdir()
        tracked = root / 'input.json'
        tracked.write_bytes(b'original')
        files = {'solution.py': 'def solve(payload): return None\n'}
        cases = {project: [dict(id=project + '-probe', input={}, expected=None)]
                 for project in ('workflow', 'inventory')}
        bindings, skipped = [], []
        for project in cases:
            for policy in runner.POLICIES:
                for repetition in range(2):
                    common = dict(run_id=f'{project}-{policy}-{repetition}', project_id=project,
                                  policy=policy, repetition=repetition, primary_accepted=False,
                                  primary_status='max_steps_incomplete', primary_milestones_completed=2)
                    if policy == 'cheap-sequential' and repetition == 1:
                        bindings.append(dict(**common, files=files, files_sha256=runner.digest(files)))
                    else:
                        skipped.append(dict(**common, reason='Final milestone was not reached'))
        contract = dict(image='sha256:' + 'a' * 64, suite_timeout_seconds=300, case_timeout_seconds=12)
        plan = ({'contract_sha': 'c' * 64}, contract, cases, bindings, skipped, {tracked: b'original'})
        arguments = [(project, expected, str(root / (project + '.json')))
                     for project, expected in runner.FROZEN_CASE_HASHES.items()]
        return root, output, tracked, plan, arguments

    def envelope(self, request, *, passed=True):
        cases = request['cases']
        return dict(receipt=dict(passed=passed, image_id='sha256:' + 'a' * 64,
                                outcomes=[dict(index=i, passed=passed) for i in range(len(cases))]),
                    validation=dict(label=request['label'], physical=True, reuse=None,
                                    purpose=runner.PURPOSE, reason=runner.REASON))

    def mocks(self, plan):
        stack = ExitStack()
        prepare = stack.enter_context(patch.object(runner, 'prepare_inputs', return_value=plan))
        contract = stack.enter_context(patch.object(runner, 'study_validation_contract',
                                                    return_value={'test_contract': 1}))
        session = stack.enter_context(patch.object(runner, 'StudyValidationSession'))
        session.return_value.contract = contract.return_value
        session.return_value.evaluate_many.side_effect = lambda requests, **kwargs: [
            self.envelope(request) for request in requests]
        return stack, prepare, contract, session

    def test_incomplete_study_is_rejected_before_output_or_adapter(self):
        with tempfile.TemporaryDirectory() as directory:
            root, output = Path(directory) / 'primary', Path(directory) / 'secondary'
            root.mkdir()
            (root / 'results.json').write_text(json.dumps(dict(
                experiment='sustained-quality-v1', mode='live', status='running')))
            (root / 'preregistered.json').write_text('{}')
            with patch.object(runner, 'StudyValidationSession') as session:
                with self.assertRaisesRegex(ValueError, 'Entire study'):
                    runner.run(root, output, [])
                session.assert_not_called()
            self.assertFalse(output.exists())

    def test_bad_budget_is_rejected_before_reading_primary(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(runner, 'prepare_inputs') as prepare:
                with self.assertRaises(ValueError):
                    runner.run(Path(directory), Path(directory) / 'out', [],
                               budget=runner.ResourceBudget(workers=0))
                prepare.assert_not_called()

    def test_one_ordered_physical_batch_has_distinct_suites_and_unchanged_primary(self):
        with tempfile.TemporaryDirectory() as directory:
            root, output, tracked, plan, arguments = self.plan(directory)
            with self.mocks(plan)[0]:
                # Inspect the mock through the module while the stack is active.
                result = runner.run(root, output, arguments)
                runner.prepare_inputs.assert_called_once()
                runner.StudyValidationSession.assert_called_once()
                call = runner.StudyValidationSession.return_value.evaluate_many.call_args
            requests = call.args[0]
            self.assertEqual([r['label'] for r in requests], [b['run_id'] for b in plan[3]])
            self.assertNotEqual(requests[0]['cases'], requests[1]['cases'])
            self.assertEqual(call.kwargs, dict(purpose='repeatability', reason=runner.REASON,
                                              deterministic=False))
            self.assertEqual(result['status'], 'finished')
            self.assertEqual(result['physical_executions'], 2)
            self.assertEqual(result['reused_executions'], 0)
            self.assertEqual(result['unexecuted'], [])
            self.assertEqual(len(result['skipped']), 10)
            self.assertFalse(result['primary_scores_modified'])
            self.assertEqual(tracked.read_bytes(), b'original')
            self.assertEqual(sorted(p.name for p in root.iterdir()), ['input.json'])
            self.assertEqual(result['summaries']['workflow']['cheap-sequential']['evaluated'], 1)
            self.assertEqual(result['summaries']['workflow']['strong-single']['unreached_final_stage'], 2)

    def test_frozen_input_snapshot_is_detached_from_mutating_parser_objects(self):
        with tempfile.TemporaryDirectory() as directory:
            root, output, tracked, plan, arguments = self.plan(directory)
            stack, _, _, session = self.mocks(plan)
            def evaluate(requests, **kwargs):
                plan[3][0]['files']['solution.py'] = 'changed parser state'
                plan[2]['workflow'][0]['expected'] = 'changed parser state'
                self.assertNotEqual(requests[0]['files']['solution.py'], 'changed parser state')
                self.assertIsNone(requests[0]['cases'][0]['expected'])
                return [self.envelope(request) for request in requests]
            session.return_value.evaluate_many.side_effect = evaluate
            with stack:
                result = runner.run(root, output, arguments)
            self.assertEqual(result['status'], 'finished')
            manifest = json.loads((output / 'manifest.json').read_text())
            self.assertEqual(manifest['contract']['protocol'], runner.PROTOCOL)
            self.assertEqual(manifest['contract_sha256'], runner.digest(manifest['contract']))
            self.assertEqual(manifest['snapshot_bytes_staged'], sum(i['bytes'] for i in manifest['inputs']))
            for item in manifest['inputs']:
                self.assertEqual(runner.sha((output / item['snapshot']).read_bytes()), item['sha256'])

    def test_all_unreached_attempts_finish_explicitly_without_docker(self):
        with tempfile.TemporaryDirectory() as directory:
            root, output, tracked, plan, arguments = self.plan(directory)
            plan[4].extend({k: v for k, v in b.items() if k not in ('files', 'files_sha256')}
                           for b in plan[3])
            plan[3].clear()
            stack, _, _, session = self.mocks(plan)
            with stack:
                result = runner.run(root, output, arguments)
                session.assert_not_called()
            self.assertEqual(result['status'], 'finished')
            self.assertEqual(len(result['skipped']), 12)
            self.assertEqual(result['physical_executions'], 0)
            self.assertIsNone(result['validation_summary'])

    def test_incorrect_candidate_outcomes_are_finished_secondary_observations(self):
        with tempfile.TemporaryDirectory() as directory:
            root, output, tracked, plan, arguments = self.plan(directory)
            stack, _, _, session = self.mocks(plan)
            session.return_value.evaluate_many.side_effect = lambda requests, **kwargs: [
                self.envelope(request, passed=False) for request in requests]
            with stack:
                result = runner.run(root, output, arguments)
            self.assertEqual(result['status'], 'finished')
            self.assertFalse(result['executions'][0]['all_probes_passed'])
            self.assertEqual(result['summaries']['workflow']['cheap-sequential']['failures_by_probe'],
                             {'workflow-probe': 1})

    def test_input_changed_while_staging_prevents_adapter_construction(self):
        with tempfile.TemporaryDirectory() as directory:
            root, output, tracked, plan, arguments = self.plan(directory)
            stack, _, _, session = self.mocks(plan)
            real_save = runner._save
            def save(path, value):
                real_save(path, value)
                if path.name == 'manifest.json':
                    tracked.write_bytes(b'changed')
            with stack, patch.object(runner, '_save', side_effect=save):
                with self.assertRaisesRegex(ValueError, 'Input changed'):
                    runner.run(root, output, arguments)
                session.assert_not_called()
            self.assertEqual(json.loads((output / 'results.json').read_text())['status'], 'interrupted')

    def test_postbatch_input_change_invalidates_finished_diagnostic(self):
        with tempfile.TemporaryDirectory() as directory:
            root, output, tracked, plan, arguments = self.plan(directory)
            stack, _, _, session = self.mocks(plan)
            def evaluate(requests, **kwargs):
                tracked.write_bytes(b'changed')
                return [self.envelope(request) for request in requests]
            session.return_value.evaluate_many.side_effect = evaluate
            with stack:
                with self.assertRaisesRegex(ValueError, 'Input changed'):
                    runner.run(root, output, arguments)
            result = json.loads((output / 'results.json').read_text())
            self.assertEqual(result['status'], 'interrupted')
            self.assertNotIn('inputs_unchanged', result)
            self.assertTrue((output / 'manifest.json').is_file())

    def test_changed_adapter_contract_invalidates_batch(self):
        with tempfile.TemporaryDirectory() as directory:
            root, output, tracked, plan, arguments = self.plan(directory)
            stack, _, contract, session = self.mocks(plan)
            contract.side_effect = [{'test_contract': 1}, {'test_contract': 2}]
            with stack:
                with self.assertRaisesRegex(ValueError, 'contract changed during'):
                    runner.run(root, output, arguments)
            self.assertEqual(json.loads((output / 'results.json').read_text())['status'], 'interrupted')

    def test_reuse_wrong_order_and_missing_results_are_rejected(self):
        for fault in ('reuse', 'order', 'missing'):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as directory:
                root, output, tracked, plan, arguments = self.plan(directory)
                stack, _, _, session = self.mocks(plan)
                def evaluate(requests, **kwargs):
                    results = [self.envelope(request) for request in requests]
                    if fault == 'reuse':
                        results[0]['validation'].update(physical=False, reuse={'kind': 'persisted'})
                    elif fault == 'order':
                        results.reverse()
                    else:
                        results.pop()
                    return results
                session.return_value.evaluate_many.side_effect = evaluate
                with stack:
                    with self.assertRaises(ValueError):
                        runner.run(root, output, arguments)
                self.assertEqual(json.loads((output / 'results.json').read_text())['status'], 'interrupted')

    def test_infrastructure_failure_retains_session_receipts_and_unexecuted_attempts(self):
        with tempfile.TemporaryDirectory() as directory:
            root, output, tracked, plan, arguments = self.plan(directory)
            stack, _, _, session = self.mocks(plan)
            def fail(requests, **kwargs):
                proof = output / 'validation'
                proof.mkdir()
                (proof / 'session.json').write_text(json.dumps({'status': 'infrastructure_failed'}))
                (proof / 'raw-receipt.json').write_text(json.dumps({'status': 'sandbox_error'}))
                raise RuntimeError('Sandbox infrastructure failure')
            session.return_value.evaluate_many.side_effect = fail
            session.return_value.summary.return_value = dict(results=[
                dict(logical_index=0, physical=True, status='infrastructure_failed',
                     artifact_path=str(output / 'validation/raw-receipt.json')),
                dict(logical_index=1, physical=False, status='cancelled', artifact_path=None)])
            with stack:
                with self.assertRaisesRegex(RuntimeError, 'Sandbox infrastructure'):
                    runner.run(root, output, arguments)
            result = json.loads((output / 'results.json').read_text())
            self.assertEqual(result['status'], 'interrupted')
            self.assertEqual(result['unexecuted'], [plan[3][1]['run_id']])
            self.assertEqual(result['unresolved_attempts'], [b['run_id'] for b in plan[3]])
            self.assertEqual(result['retained_attempts'][0]['status'], 'infrastructure_failed')
            self.assertEqual(result['validation_summary'], 'validation/session.json')
            self.assertEqual(json.loads((output / 'validation/raw-receipt.json').read_text()),
                             {'status': 'sandbox_error'})

    def test_existing_output_is_never_replaced(self):
        with tempfile.TemporaryDirectory() as directory:
            root, output = Path(directory) / 'primary', Path(directory) / 'secondary'
            root.mkdir()
            output.mkdir()
            marker = output / 'retained.txt'
            marker.write_text('keep')
            with patch.object(runner, 'StudyValidationSession') as session:
                with self.assertRaisesRegex(ValueError, 'fresh directory'):
                    runner.run(root, output, [])
                session.assert_not_called()
            self.assertEqual(marker.read_text(), 'keep')
            dangling = Path(directory) / 'alias'
            target = Path(directory) / 'never-created'
            dangling.symlink_to(target, target_is_directory=True)
            with self.assertRaisesRegex(ValueError, 'symlink'):
                runner.run(root, dangling, [])
            self.assertFalse(target.exists())

    def test_signal_cancels_batch_persists_interruption_and_restores_handler(self):
        with tempfile.TemporaryDirectory() as directory:
            root, output, tracked, plan, arguments = self.plan(directory)
            stack, _, _, session = self.mocks(plan)
            previous = signal.getsignal(signal.SIGTERM)
            def interrupt(requests, **kwargs):
                os.kill(os.getpid(), signal.SIGTERM)
                session.return_value.cancel.assert_called()
                raise RuntimeError('Batch cancelled and active validators drained')
            session.return_value.evaluate_many.side_effect = interrupt
            with stack:
                with self.assertRaisesRegex(RuntimeError, 'cancelled'):
                    runner.run(root, output, arguments)
            self.assertEqual(signal.getsignal(signal.SIGTERM), previous)
            result = json.loads((output / 'results.json').read_text())
            self.assertEqual(result['status'], 'interrupted')
            self.assertEqual(result['unresolved_attempts'], [b['run_id'] for b in plan[3]])


if __name__ == '__main__':
    unittest.main()
