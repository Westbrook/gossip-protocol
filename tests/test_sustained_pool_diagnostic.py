from copy import deepcopy
import hashlib
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from gossip_harness.gitstore import GitStore
from gossip_harness.sustained_experiment import CORE, contract, digest
from gossip_harness.sustained_pool_diagnostic import run


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + '\n')


def receipt(files, cases, settings, *, visible=False):
    tag = files['backend.py']
    failures = {'selected': {'h0'}, 'correlated': {'h0'}, 'other': {'h0', 'h1'},
                'stale': {'h0', 'h1', 'h2'}, 'good': set()}[tag]
    outcomes = [dict(index=index, passed=visible or case['id'] not in failures,
                     status='ok', actual=case['expected']) for index, case in enumerate(cases)]
    passed = all(outcome['passed'] for outcome in outcomes)
    return dict(status='passed' if passed else 'failed', passed=passed, cleanup_verified=True,
                source_sha256=digest(files), suite_sha256=digest(cases), outcomes=outcomes,
                image_id=settings['image'], case_timeout_seconds=settings['case_timeout_seconds'],
                timeout_seconds=settings['suite_timeout_seconds'])


class FakeValidator:
    def __init__(self, settings):
        self.settings = settings
        self.calls = []
        self.preflights = 0

    def preflight(self):
        self.preflights += 1
        return True, 'Offline mock; no container started'

    def evaluate(self, files, cases):
        self.calls.append((deepcopy(files), deepcopy(cases)))
        return receipt(files, cases, self.settings)


class SustainedPoolDiagnosticTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.templates_dir = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.templates_dir.cleanup)
        cls.templates = {}
        for tag in ('selected', 'correlated', 'other', 'stale', 'good'):
            files = {'solution.py': '# source is never executed on the host\n', 'backend.py': tag}
            store = GitStore.create(Path(cls.templates_dir.name) / (tag + '.git'), files)
            cls.templates[tag] = (store.path, store.head(), files)

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.study = self.root / 'study'
        self.study.mkdir()
        self.output = self.root / 'diagnostic.json'
        self.settings = contract()
        stages = []
        for index in range(3):
            stages.append(dict(visible_cases=[dict(id=f'v{index}', input=index, expected=index,
                                                   requirement=f'r{index}')],
                               hidden_cases=[dict(id=f'h{index}', input=index + 10, expected=index,
                                                  requirement=f'r{index}')]))
        self.fixture = dict(id='fixture', stages=stages)
        self.settings.update(project_ids=['fixture'], fixture_sha256=digest([self.fixture]))
        self.visible = [case for stage in stages for case in stage['visible_cases']]
        self.hidden = [case for stage in stages for case in stage['hidden_cases']]
        self.case_root = self.study / 'fixture-reviewed-portfolio-0'
        self.case_root.mkdir()
        (self.case_root / 'stage-2').mkdir()
        self.rows = []
        self.slot_files = {}
        self.add_candidate('c1', 1, 'stale')
        for slot, tag, round_number in (('c0', 'selected', 1), ('c1', 'good', 2),
                                         ('c2', 'correlated', 1), ('c3', 'other', 1)):
            self.add_candidate(slot, round_number, tag)
        self.stage = dict(stage_index=2, files=self.slot_files['c0'], selected='c0',
                          selected_binding=deepcopy(self.rows[1]['binding']), trajectory=self.rows)
        self.state = dict(contract_sha=digest(self.settings), files=self.slot_files['c0'],
                          stages=[{'stage_index': 0}, {'stage_index': 1}, self.stage])
        self.case = dict(project_id='fixture', policy='reviewed-portfolio', repetition=0,
                         run_id=self.case_root.name, root=str(self.case_root), accepted=False,
                         files_sha256=digest(self.slot_files['c0']),
                         final_hidden=receipt(self.slot_files['c0'], self.hidden, self.settings))
        baseline_cases = [dict(project_id='fixture', policy=policy, repetition=0)
                          for policy in self.settings['policies'] if policy != 'reviewed-portfolio']
        self.report = dict(experiment=self.settings['protocol'], status='finished', unexecuted=[],
                           mode='live', contract=self.settings, contract_sha=digest(self.settings),
                           repetitions=1, cases=[*baseline_cases, self.case])
        self.registered = dict(contract=self.settings,
                               roster=[['fixture', policy, 0] for policy in self.settings['policies']])
        write(self.study / 'fixtures.json', [self.fixture])
        write(self.study / 'preregistered.json', self.registered)
        package = Path(__file__).resolve().parents[1] / 'gossip_harness'
        snapshot = self.study / 'source-snapshot' / 'gossip_harness'
        snapshot.mkdir(parents=True)
        for name in CORE:
            shutil.copyfile(package / name, snapshot / name)
        self.save()
        self.validator = FakeValidator(self.settings)
        self.factory_calls = []

    def add_candidate(self, slot, round_number, tag):
        template, tip, files = self.templates[tag]
        label = f'stage-2-reviewed-portfolio-{slot}-build-{round_number}'
        store = self.case_root / 'stage-2' / (label + '.git')
        shutil.copytree(template, store)
        binding = dict(store_path=str(store), tip_sha=tip, files_sha256=digest(files))
        write(store.with_suffix('.json'), dict(binding=binding, files=files))
        write(store.with_suffix('.validation.json'), receipt(files, self.visible, self.settings, visible=True))
        self.rows.append(dict(kind='builder', candidate=slot, round=round_number, binding=binding,
                              files_sha256=digest(files), source_valid=True))
        self.slot_files[slot] = deepcopy(files)

    def save(self):
        write(self.case_root / 'stage-2' / 'result.json', self.stage)
        write(self.case_root / 'trajectory.json', self.state)
        self.case['trajectory_sha256'] = hashlib.sha256((self.case_root / 'trajectory.json').read_bytes()).hexdigest()
        write(self.case_root / 'result.json', self.case)
        write(self.study / 'results.json', self.report)

    def factory(self, image, *, timeout_seconds, case_timeout_seconds):
        self.assertEqual(image, self.settings['image'])
        self.assertEqual(timeout_seconds, self.settings['suite_timeout_seconds'])
        self.assertEqual(case_timeout_seconds, self.settings['case_timeout_seconds'])
        self.factory_calls.append(image)
        return self.validator

    def run_diagnostic(self):
        return run(self.study, self.output, validator_factory=self.factory)

    def populate_earlier_stages(self):
        for stage_index in (0, 1):
            stage_root = self.case_root / f'stage-{stage_index}'
            stage_root.mkdir()
            rows = []
            for slot, tag in (('c0', 'selected'), ('c1', 'good'), ('c2', 'correlated'), ('c3', 'other')):
                template, tip, files = self.templates[tag]
                path = stage_root / f'stage-{stage_index}-reviewed-portfolio-{slot}-build-1.git'
                shutil.copytree(template, path)
                binding = dict(store_path=str(path), tip_sha=tip, files_sha256=digest(files))
                write(path.with_suffix('.json'), dict(binding=binding, files=files))
                write(path.with_suffix('.validation.json'),
                      receipt(files, self.visible[:stage_index + 1], self.settings, visible=True))
                rows.append(dict(kind='builder', candidate=slot, round=1, binding=binding,
                                 files_sha256=digest(files), source_valid=True))
            stage = dict(stage_index=stage_index, files=self.slot_files['c0'], selected='c0',
                         selected_binding=deepcopy(rows[0]['binding']), trajectory=rows, completed=True)
            self.state['stages'][stage_index] = stage
            write(stage_root / 'result.json', stage)
        self.case['historical'] = [dict(stage_index=index, binding=deepcopy(stage['selected_binding']),
                                       hidden=receipt(stage['files'], self.hidden[:index + 1], self.settings))
                                   for index, stage in enumerate(self.state['stages'])]
        self.save()

    def test_latest_candidates_find_regret_and_correlated_failures_without_changing_primary(self):
        before = {path: path.read_bytes() for path in (self.study / 'results.json',
                                                       self.case_root / 'trajectory.json',
                                                       self.case_root / 'result.json')}
        result = self.run_diagnostic()
        self.assertEqual(result['status'], 'finished')
        self.assertEqual(result['classification'], 'POSTHOC diagnostic')
        self.assertFalse(result['preregistered_primary'])
        self.assertEqual(result['api_calls'], 0)
        self.assertEqual(result['fresh_validation_calls'], 3)
        self.assertEqual(result['reused_final_receipts'], 1)
        self.assertEqual(self.validator.preflights, 1)
        self.assertEqual(len(self.validator.calls), 3)
        case = result['cases'][0]
        self.assertEqual(case['correct_candidates'], ['c1'])
        self.assertEqual(case['candidates']['c1']['round'], 2)
        self.assertTrue(case['candidate_coverage'])
        self.assertTrue(case['selector_regret'])
        self.assertTrue(case['eligible_selector_regret'])
        self.assertFalse(case['selected_hidden_correct'])
        self.assertFalse(case['primary_accepted'])
        self.assertEqual(case['distinct_sources'], 4)
        self.assertEqual(case['shared_nonempty_failure_patterns'],
                         [dict(candidates=['c0', 'c2'], failed_case_ids=['h0'])])
        pair = next(pair for pair in case['pairwise_failure_overlap']
                    if (pair['left'], pair['right']) == ('c0', 'c3'))
        self.assertEqual(pair['shared_failed_case_ids'], ['h0'])
        self.assertEqual(pair['failure_jaccard'], 0.5)
        for path, data in before.items():
            self.assertEqual(path.read_bytes(), data)

    def test_candidate_git_source_mismatch_stops_before_any_validation(self):
        original = GitStore.read_files
        def altered(store, commit=None):
            files = original(store, commit)
            if store.path.name.endswith('c1-build-2.git'):
                files['backend.py'] = 'mismatched source'
            return files
        with patch.object(GitStore, 'read_files', altered):
            with self.assertRaisesRegex(ValueError, 'source mismatch'):
                self.run_diagnostic()
        self.assertEqual(self.factory_calls, [])
        self.assertFalse(self.output.exists())

    def test_unfinished_study_or_tampered_trajectory_cannot_run(self):
        self.report['status'] = 'running'
        self.save()
        with self.assertRaisesRegex(ValueError, 'finished study'):
            self.run_diagnostic()
        self.report['status'] = 'finished'
        self.save()
        with (self.case_root / 'trajectory.json').open('a') as stream:
            stream.write(' ')
        with self.assertRaisesRegex(ValueError, 'trajectory hash'):
            self.run_diagnostic()
        self.assertEqual(self.factory_calls, [])

    def test_wrong_selected_receipt_binding_is_not_reused(self):
        self.case['final_hidden']['source_sha256'] = '0' * 64
        self.save()
        with self.assertRaisesRegex(RuntimeError, 'receipt failed'):
            self.run_diagnostic()
        self.assertEqual(self.factory_calls, [])
        self.assertFalse(self.output.exists())

    def test_publicly_failing_correct_alternative_is_not_eligible_regret(self):
        row = next(row for row in self.rows if row['candidate'] == 'c1' and row['round'] == 2)
        path = Path(row['binding']['store_path']).with_suffix('.validation.json')
        visible = json.loads(path.read_text())
        visible['outcomes'][0]['passed'] = False
        visible.update(passed=False, status='failed')
        write(path, visible)
        case = self.run_diagnostic()['cases'][0]
        self.assertTrue(case['selector_regret'])
        self.assertFalse(case['eligible_selector_regret'])
        self.assertFalse(case['eligible_candidate_coverage'])

    def test_matching_selected_sources_reuse_receipt_without_extra_execution(self):
        self.add_candidate('c2', 2, 'selected')
        self.save()
        result = self.run_diagnostic()
        self.assertEqual(result['fresh_validation_calls'], 2)
        self.assertEqual(result['reused_final_receipts'], 2)
        self.assertEqual(result['cases'][0]['distinct_sources'], 3)
        self.assertTrue(result['cases'][0]['candidates']['c2']['reused_final_hidden'])

    def test_early_failure_is_explicitly_skipped_not_counted_as_empty_pool(self):
        self.state['stages'] = self.state['stages'][:2]
        self.save()
        result = self.run_diagnostic()
        self.assertEqual(result['cases'][0]['status'], 'final_milestone_not_reached')
        self.assertEqual(result['summary']['analyzed'], 0)
        self.assertEqual(result['summary']['final_milestone_not_reached'], 1)
        self.assertEqual(result['fresh_validation_calls'], 0)
        self.assertEqual(self.factory_calls, [])

    def test_hidden_passing_incomplete_third_stage_is_not_primary_acceptance(self):
        row = next(row for row in self.rows if row['candidate'] == 'c1' and row['round'] == 2)
        self.stage.update(selected='c1', selected_binding=row['binding'],
                          files=self.slot_files['c1'], completed=False)
        self.state.update(files=self.slot_files['c1'], terminal='max_steps_incomplete')
        self.case.update(files_sha256=digest(self.slot_files['c1']),
                         final_hidden=receipt(self.slot_files['c1'], self.hidden, self.settings))
        self.save()
        case = self.run_diagnostic()['cases'][0]
        self.assertEqual(case['status'], 'finished')
        self.assertTrue(case['selected_hidden_correct'])
        self.assertFalse(case['primary_accepted'])
        self.assertFalse(case['selector_regret'])

    def test_report_created_during_preflight_is_not_overwritten(self):
        def competing_report():
            self.output.write_text('created during preflight')
            return True, 'mock'
        self.validator.preflight = competing_report
        with self.assertRaises(FileExistsError):
            self.run_diagnostic()
        self.assertEqual(self.output.read_text(), 'created during preflight')
        self.assertEqual(self.validator.calls, [])

    def test_fresh_output_and_execution_receipt_fail_closed(self):
        self.output.write_text('preserve this existing report')
        with self.assertRaisesRegex(ValueError, 'already exists'):
            self.run_diagnostic()
        self.assertEqual(self.output.read_text(), 'preserve this existing report')
        self.output.unlink()
        original = self.validator.evaluate
        def invalid(files, cases):
            result = original(files, cases)
            result['case_timeout_seconds'] += 1
            return result
        self.validator.evaluate = invalid
        with self.assertRaisesRegex(ValueError, 'execution settings'):
            self.run_diagnostic()
        saved = json.loads(self.output.read_text())
        self.assertEqual(saved['status'], 'interrupted')
        self.assertEqual(saved['api_calls'], 0)
        self.assertNotIn('summary', saved)

    def test_all_stages_use_cumulative_suites_and_bound_historical_receipts(self):
        self.populate_earlier_stages()
        result = run(self.study, self.output, validator_factory=self.factory, all_stages=True)
        self.assertEqual(result['diagnostic'], 'sustained-all-stage-pool-posthoc-v1')
        self.assertEqual([case['stage_index'] for case in result['cases']], [0, 1, 2])
        self.assertEqual(result['fresh_validation_calls'], 9)
        self.assertEqual(result['reused_historical_receipts'], 3)
        self.assertNotIn('reused_final_receipts', result)
        self.assertEqual([len(cases) for _, cases in self.validator.calls], [1, 1, 1, 2, 2, 2, 3, 3, 3])
        self.assertEqual(result['summary']['portfolio_trajectories'], 1)
        self.assertEqual(result['summary']['stage_pools'], 3)
        self.assertEqual(result['summary']['analyzed'], 3)
        self.assertEqual(result['summary']['selector_regret'], 3)
        for index, case in enumerate(result['cases']):
            self.assertTrue(case['candidates']['c0']['reused_historical_hidden'])
            self.assertEqual(case['candidates']['c0']['hidden_cases'], index + 1)
            self.assertFalse(case['primary_accepted'])
            self.assertEqual(result['summary']['by_stage'][str(index)]['eligible_selector_regret'], 1)

    def test_all_stages_reject_wrong_historical_binding_or_future_suite(self):
        self.populate_earlier_stages()
        original = deepcopy(self.case['historical'][0])
        self.case['historical'][0]['binding']['tip_sha'] = '0' * 40
        self.save()
        with self.assertRaisesRegex(ValueError, 'Historical hidden evaluation binding'):
            run(self.study, self.output, validator_factory=self.factory, all_stages=True)
        self.case['historical'][0] = original
        self.case['historical'][0]['hidden'] = deepcopy(self.case['historical'][2]['hidden'])
        self.save()
        with self.assertRaisesRegex(RuntimeError, 'receipt failed'):
            run(self.study, self.output, validator_factory=self.factory, all_stages=True)
        self.assertEqual(self.factory_calls, [])
        self.assertFalse(self.output.exists())

    def test_all_stages_report_unreached_milestones_without_hiding_early_pool(self):
        self.populate_earlier_stages()
        self.state['stages'] = self.state['stages'][:1]
        self.case['historical'] = self.case['historical'][:1]
        self.save()
        result = run(self.study, self.output, validator_factory=self.factory, all_stages=True)
        self.assertEqual([case['status'] for case in result['cases']],
                         ['finished', 'milestone_not_reached', 'milestone_not_reached'])
        self.assertEqual(result['fresh_validation_calls'], 3)
        self.assertEqual(result['summary']['analyzed'], 1)
        self.assertEqual(result['summary']['milestone_not_reached'], 2)
        self.assertEqual(result['summary']['by_stage']['1']['analyzed'], 0)


if __name__ == '__main__':
    unittest.main()
