"""No Docker/candidate execution: only post-hoc runner's fail-closed gates."""
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

PATH = Path(__file__).with_name('run_sustained_review_probes.py')
spec = importlib.util.spec_from_file_location('review_probes', PATH)
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


def completed():
    roster = [(p, a, n) for p in ('workflow', 'inventory') for a in runner.POLICIES for n in range(2)]
    rows = [dict(project_id=p, policy=a, repetition=n, accepted=False) for p, a, n in roster]
    return dict(experiment='sustained-quality-v1', mode='live', status='finished', repetitions=2,
                unexecuted=[], cases=rows), dict(roster=roster)


class ProbeSafetyTests(unittest.TestCase):
    def test_completion_requires_exact_full_study_even_for_unaccepted_results(self):
        report, preregistered = completed()
        self.assertEqual(len(runner.completion_gate(report, preregistered)), 12)
        for mutation in (
            lambda r: r.update(status='running'),
            lambda r: r.update(unexecuted=[['inventory', 'strong-single', 1]]),
            lambda r: r['cases'].pop(),
            lambda r: r['cases'].__setitem__(-1, r['cases'][0]),
            lambda r: r.update(repetitions=True),
        ):
            changed = copy.deepcopy(report)
            mutation(changed)
            with self.assertRaises(ValueError):
                runner.completion_gate(changed, preregistered)

    def test_incomplete_input_cannot_create_output_or_contact_docker(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / 'primary'
            root.mkdir()
            report, preregistered = completed()
            report['status'] = 'running'
            (root / 'results.json').write_text(json.dumps(report))
            (root / 'preregistered.json').write_text(json.dumps(preregistered))
            output = Path(directory) / 'secondary'
            with patch.object(runner, 'BlackboxValidator') as validator:
                with self.assertRaisesRegex(ValueError, 'Entire study'):
                    runner.run(root, output, [])
                validator.assert_not_called()
            self.assertFalse(output.exists())

    def test_no_writes_into_primary_or_existing_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / 'primary'
            root.mkdir()
            for output in (root, root / 'secondary', Path(directory)):
                with self.assertRaises(ValueError):
                    runner.validate_output(root, output)
            runner.validate_output(root, Path(directory) / 'fresh-secondary')

    def test_changed_input_is_detected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'input.json'
            raw = b'{"frozen":true}'
            path.write_bytes(raw)
            runner.unchanged({path: raw})
            path.write_bytes(b'{"frozen":false}')
            with self.assertRaisesRegex(ValueError, 'Input changed'):
                runner.unchanged({path: raw})

    def mock_plan(self, directory):
        root = Path(directory) / 'primary'
        root.mkdir()
        tracked = root / 'input.json'
        tracked.write_bytes(b'original')
        output = Path(directory) / 'secondary'
        cases = {p:[dict(id=p+'-probe', input={}, expected=None)] for p in ('workflow', 'inventory')}
        files = {'solution.py':'def solve(payload): return None\n'}
        binding = dict(run_id='inventory-cheap-sequential-1', project_id='inventory', policy='cheap-sequential',
                       repetition=1, primary_accepted=False, primary_status='max_steps_incomplete',
                       primary_milestones_completed=2, files=files)
        skipped = [dict(run_id='workflow-strong-single-0', project_id='workflow', policy='strong-single',
                        repetition=0, primary_accepted=False, primary_status='max_steps_incomplete',
                        reason='Final milestone was not reached; earlier code is not substituted')]
        contract = dict(image='sha256:'+'a'*64, suite_timeout_seconds=300, case_timeout_seconds=12)
        plan = ({'contract_sha':'c'*64}, contract, cases, [binding], skipped, {tracked:b'original'})
        arguments = [(p, value, str(root / (p+'.json'))) for p,value in runner.FROZEN_CASE_HASHES.items()]
        return root, output, tracked, plan, arguments

    def test_reached_unaccepted_is_evaluated_and_unreached_is_explicit(self):
        with tempfile.TemporaryDirectory() as directory:
            root, output, tracked, plan, arguments = self.mock_plan(directory)
            def receipt(files, cases):
                return dict(status='passed', passed=True, cleanup_verified=True, image_id=plan[1]['image'],
                            source_sha256=runner.digest(files), suite_sha256=runner.digest(cases),
                            outcomes=[dict(index=0, passed=True)])
            with patch.object(runner, 'prepare_inputs', return_value=plan), patch.object(runner, 'BlackboxValidator') as validator:
                validator.return_value.preflight.return_value = (True, 'mock')
                validator.return_value.evaluate.side_effect = receipt
                result = runner.run(root, output, arguments)
            self.assertEqual(result['status'], 'finished')
            self.assertEqual(result['executions'][0]['primary_status'], 'max_steps_incomplete')
            self.assertFalse(result['executions'][0]['primary_accepted'])
            self.assertEqual(result['summaries']['inventory']['cheap-sequential']['evaluated'], 1)
            self.assertEqual(result['summaries']['workflow']['strong-single']['unreached_final_stage'], 1)
            self.assertEqual(validator.return_value.evaluate.call_count, 1)

    def test_post_snapshot_change_prevents_docker_preflight(self):
        with tempfile.TemporaryDirectory() as directory:
            root, output, tracked, plan, arguments = self.mock_plan(directory)
            with patch.object(runner, 'prepare_inputs', return_value=plan), patch.object(runner, 'BlackboxValidator') as validator:
                def mutate(*args, **kwargs):
                    tracked.write_bytes(b'changed')
                    return validator.return_value
                validator.side_effect = mutate
                with self.assertRaisesRegex(ValueError, 'Input changed'):
                    runner.run(root, output, arguments)
                validator.return_value.preflight.assert_not_called()
            self.assertEqual(json.loads((output / 'results.json').read_text())['status'], 'interrupted')

    def test_rejected_sandbox_receipt_is_retained_and_stops(self):
        with tempfile.TemporaryDirectory() as directory:
            root, output, tracked, plan, arguments = self.mock_plan(directory)
            with patch.object(runner, 'prepare_inputs', return_value=plan), patch.object(runner, 'BlackboxValidator') as validator:
                validator.return_value.preflight.return_value = (True, 'mock')
                validator.return_value.evaluate.return_value = {'status':'sandbox_error', 'passed':False}
                with self.assertRaisesRegex(RuntimeError, 'Sandbox infrastructure'):
                    runner.run(root, output, arguments)
            raw = json.loads((output / 'receipts/inventory-cheap-sequential-1.json').read_text())
            self.assertEqual(raw['status'], 'sandbox_error')
            self.assertEqual(json.loads((output / 'results.json').read_text())['status'], 'interrupted')

    def test_committed_probe_artifact_hashes_are_exact(self):
        from devtools.verify import inputs, load_manifest
        bound_inputs = inputs(runner.REPOSITORY, load_manifest(runner.REPOSITORY))
        for project_id, expected in runner.FROZEN_CASE_HASHES.items():
            relative = f'analysis/sustained-posthoc-{project_id}-1/cases.json'
            self.assertEqual(bound_inputs.get(relative), expected,
                             'Committed probe fixtures must be bound in verification snapshots')
            path = runner.REPOSITORY / relative
            self.assertEqual(runner.sha(path.read_bytes()), expected)
            artifact = json.loads(path.read_text())
            self.assertEqual(artifact['project'], project_id)
            self.assertTrue(artifact['posthoc'])
            self.assertFalse(artifact['primary_suites_modified'])


if __name__ == '__main__':
    unittest.main()
