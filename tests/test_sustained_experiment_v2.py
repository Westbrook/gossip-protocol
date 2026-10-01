"""Offline gates for v2 orchestration; Docker rehearsal remains a separate proof."""
from copy import deepcopy
from dataclasses import asdict
import hashlib
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from devtools.validation_session import ResourceBudget
from gossip_harness import sustained_experiment_v2 as study


def _validation(purpose, index=0):
    return dict(purpose=purpose, physical=True, execution_id=f'physical-{index}', reuse=None, logical_index=index)


def _proof(expected):
    cases = []
    for project in study.projects():
        for policy in study.POLICIES:
            observations = []
            for index, (stage, kind, purpose) in enumerate(
                    [(2, 'visible', 'final'), (2, 'hidden', 'final'),
                     (0, 'hidden', 'repeatability'), (1, 'hidden', 'repeatability'),
                     (2, 'hidden', 'repeatability')]):
                files = dict(project['initial_files'])
                files.update({path: project['stages'][stage]['known_files'][path]
                              for path in project['allowed_paths']})
                suite = study.cases_for(project, stage, kind)
                receipt = dict(status='passed', passed=True, cleanup_verified=True,
                               source_sha256=study.digest(files), suite_sha256=study.digest(suite),
                               outcomes=[dict(index=index, passed=True) for index in range(len(suite))])
                provenance = dict(_validation(purpose, index), source_sha256=study.digest(files),
                                  suite_sha256=study.digest(suite))
                observations.append((receipt, provenance))
            cases.append(dict(
                project_id=project['id'], policy=policy, repetition=0, accepted=True, status='accepted',
                milestones_completed=3, release_head='a' * 40, exact_tested_sha='a' * 40,
                files_sha256=observations[0][0]['source_sha256'], usage_micro_usd=0,
                invocations=[dict(usage_units=0, metadata=dict(api_calls=0))],
                metrics=dict(repairs=3, premature_completion=3), rehearsal_repair_verified=True,
                handoff=dict(verified=True, previous_pid=123, resumed_pid=234),
                final_visible=observations[0][0], final_hidden=observations[1][0],
                final_visible_validation=observations[0][1], final_hidden_validation=observations[1][1],
                historical=[dict(stage_index=index, hidden=observations[index + 2][0],
                                 validation=observations[index + 2][1]) for index in range(3)]))
    return dict(experiment=study.PROTOCOL, mode='rehearsal', status='finished',
                contract=deepcopy(expected), contract_sha=study.digest(expected), unexecuted=[],
                incremental_micro_usd=0, cases=cases)


def _project():
    return dict(id='fixture', allowed_paths=['solution.py'], initial_files={'solution.py': 'initial'},
                stages=[dict(visible_cases=[dict(input=index, expected=index, requirement=f'r{index}')],
                             hidden_cases=[dict(input=index + 10, expected=index + 10,
                                                requirement=f'r{index}')]) for index in range(3)])


class SustainedV2ContractTests(unittest.TestCase):
    def test_contract_binds_runtime_closure_budget_and_opt_in_reuse(self):
        value = study.contract('sha256:' + 'a' * 64)
        self.assertEqual(value['protocol'], 'sustained-quality-v2')
        self.assertEqual(value['controller_module'], 'gossip_harness.sustained_experiment_v2')
        self.assertEqual(set(value['sources']), set(study.CORE))
        validation = value['validation']
        paths = validation['support_sha256']
        for name in ('devtools/study_validation.py', 'devtools/validation_session.py',
                     'gossip_harness/sustained_experiment_v2.py',
                     'gossip_harness/sustained_stage_v2.py',
                     'gossip_harness/sustained_checkpoint.py',
                     'gossip_harness/sandbox.py'):
            self.assertEqual(paths[name], hashlib.sha256((study.ROOT / name).read_bytes()).hexdigest())
        # Changing either scheduling or reuse eligibility changes exact rehearsal identity.
        self.assertNotEqual(value, study.contract('sha256:' + 'a' * 64, budget=ResourceBudget(workers=1)))
        self.assertNotEqual(value, study.contract('sha256:' + 'a' * 64, deterministic_visible=True))
        with self.assertRaises(ValueError):
            study.contract('sha256:' + 'a' * 64, budget=ResourceBudget(cpus=0))

    def test_v1_wrong_source_or_changed_reuse_contract_cannot_authorize_live(self):
        expected = dict(project_ids=[p['id'] for p in study.projects()], validation={'deterministic_visible': False})
        proof = _proof(expected)
        study.validate_rehearsal(proof, expected)
        for field, value in (('experiment', 'sustained-quality-v1'), ('mode', 'live'),
                             ('status', 'interrupted'), ('unexecuted', [['missing']]),
                             ('contract_sha', '0' * 64)):
            with self.subTest(field=field):
                changed = deepcopy(proof)
                changed[field] = value
                with self.assertRaises(ValueError):
                    study.validate_rehearsal(changed, expected)
        changed = deepcopy(proof)
        changed['contract']['validation']['deterministic_visible'] = True
        with self.assertRaises(ValueError):
            study.validate_rehearsal(changed, expected)

    def test_rehearsal_requires_exact_roster_new_pid_and_physical_final_matrix(self):
        expected = dict(project_ids=[p['id'] for p in study.projects()])
        proof = _proof(expected)
        changes = [lambda p: p['cases'].pop(),
                   lambda p: p['cases'].append(deepcopy(p['cases'][0])),
                   lambda p: p['cases'][0].update(accepted=False),
                   lambda p: p['cases'][0].update(rehearsal_repair_verified=False),
                   lambda p: p['cases'][0]['handoff'].update(resumed_pid=123),
                   lambda p: p['cases'][0]['handoff'].update(previous_pid=True),
                   lambda p: p['cases'][0]['final_visible_validation'].update(physical=False),
                   lambda p: p['cases'][0]['final_hidden_validation'].update(purpose='visible'),
                   lambda p: p['cases'][0]['historical'][2]['validation'].update(reuse={'kind': 'persisted'}),
                   lambda p: p['cases'][0]['historical'].pop(),
                   lambda p: p['cases'][0]['final_hidden'].update(source_sha256='0' * 64),
                   lambda p: p['cases'][0]['final_visible'].update(outcomes=[]),
                   lambda p: p['cases'][0].update(usage_micro_usd=1),
                   lambda p: p['cases'][0].update(milestones_completed=2),
                   lambda p: p['cases'][0].update(exact_tested_sha='other'),
                   lambda p: p['cases'][0]['final_hidden_validation'].update(execution_id='physical-0')]
        for index, change in enumerate(changes):
            with self.subTest(index=index):
                changed = deepcopy(proof)
                change(changed)
                with self.assertRaises(ValueError):
                    study.validate_rehearsal(changed, expected)

    def test_raw_audit_binds_visible_trajectory_and_shared_final_range(self):
        from tests.test_validation_session import FakeFactory, IMAGE
        project = _project()
        for index, stage in enumerate(project['stages']):
            stage['known_files'] = {'solution.py': f'stage-{index}'}
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(study, 'projects', return_value=[project]), \
                patch.object(study, 'POLICIES', ('strong-single',)):
            root = Path(directory).resolve()
            expected = study.contract(IMAGE)
            expected['project_ids'] = ['fixture']
            expected['fixture_sha256'] = study.digest([project])
            proof = _proof(expected)
            case = proof['cases'][0]
            run_id = 'fixture-strong-single-0'
            case_root = root / run_id
            case_root.mkdir()
            def session(path):
                return study.StudyValidationSession(path, image=IMAGE, protocol=study.PROTOCOL,
                    timeout_seconds=300, case_timeout_seconds=12,
                    support_paths=study.support_paths(), validator_factory=FakeFactory(),
                    runtime_identity=lambda: 'offline-audit-fixture')
            begin = session(case_root / 'validation-begin')
            resume = session(case_root / 'validation-resume')
            final = session(root / 'validation-final')
            stages = []
            for index, stage in enumerate(project['stages']):
                files = stage['known_files']
                observation = (begin if index < 2 else resume).evaluate(
                    files, study.cases_for(project, index, 'visible'), label=f'stage-{index}')
                stages.append(dict(files=files, completed=True, stage_protocol='sustained-stage-v2',
                                   visible_receipt=observation['receipt'], visible_validation=observation['validation'],
                                   trajectory=[dict(kind='builder', files_sha256=study.digest(files),
                                                    validation=observation['validation'])]))
            state = dict(contract_sha=study.digest(expected), files=stages[-1]['files'], stages=stages,
                         completed_stages=[{'completed': True}] * 3, invocations=case['invocations'])
            trajectory = case_root / 'trajectory.json'
            trajectory.write_text(json.dumps(state))
            requests = [dict(label=run_id + '-' + label, files=stages[stage]['files'],
                             cases=study.cases_for(project, stage, kind), purpose=purpose,
                             reason='Independent synthetic audit fixture')
                        for label, stage, kind, purpose in (
                            ('final-visible', 2, 'visible', 'final'), ('final-hidden', 2, 'hidden', 'final'),
                            ('historical-0', 0, 'hidden', 'repeatability'),
                            ('historical-1', 1, 'hidden', 'repeatability'),
                            ('historical-2', 2, 'hidden', 'repeatability'))]
            observations = final.evaluate_matrix(requests)
            case.update(run_id=run_id, root=str(case_root),
                        trajectory_sha256=hashlib.sha256(trajectory.read_bytes()).hexdigest(),
                        final_observation_range=dict(start=0, end=5),
                        final_validation_session=str(root / 'validation-final/session.json'),
                        final_visible=observations[0]['receipt'], final_hidden=observations[1]['receipt'],
                        final_visible_validation=observations[0]['validation'],
                        final_hidden_validation=observations[1]['validation'],
                        historical=[dict(stage_index=index, hidden=row['receipt'], validation=row['validation'])
                                    for index, row in enumerate(observations[2:])])
            study.validate_rehearsal(proof, expected, evidence_root=root)
            changed = deepcopy(proof)
            changed['cases'][0]['final_observation_range'] = dict(start=1, end=6)
            with self.assertRaisesRegex(ValueError, 'reordered|cross-bound'):
                study.validate_rehearsal(changed, expected, evidence_root=root)
            state['stages'][0]['trajectory'][0]['files_sha256'] = '0' * 64
            trajectory.write_text(json.dumps(state))
            changed = deepcopy(proof)
            changed['cases'][0]['trajectory_sha256'] = hashlib.sha256(trajectory.read_bytes()).hexdigest()
            with self.assertRaisesRegex(ValueError, 'incorrectly bound'):
                study.validate_rehearsal(changed, expected, evidence_root=root)

    def test_snapshot_preserves_complete_bound_sources_and_rejects_changed_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / 'source'
            source.mkdir()
            (source / 'leaf.py').write_text('never_execute = True\n')
            sha = hashlib.sha256((source / 'leaf.py').read_bytes()).hexdigest()
            expected = {'validation': {'support_sha256': {'leaf.py': sha}}}
            with patch.object(study, 'ROOT', source):
                study._snapshot_sources(root / 'output', expected)
                manifest = json.loads((root / 'output/source-snapshot/manifest.json').read_text())
                self.assertTrue(manifest['complete_runtime'])
                self.assertFalse(manifest['executed'])
                self.assertEqual((root / 'output/source-snapshot/leaf.py').read_bytes(),
                                 (source / 'leaf.py').read_bytes())
                (source / 'leaf.py').write_text('changed = True\n')
                with self.assertRaises(ValueError):
                    study._snapshot_sources(root / 'rejected', expected)
                self.assertFalse((root / 'rejected/source-snapshot/leaf.py').exists())


class SustainedV2ControllerTests(unittest.TestCase):
    def test_invalid_live_rehearsal_stops_before_output_credentials_or_runtime(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            proof = root / 'v1.json'
            proof.write_text(json.dumps(dict(experiment='sustained-quality-v1')))
            with patch.object(study, 'credential') as credential, patch.object(study, '_session') as session:
                with self.assertRaises(ValueError):
                    study.run(root / 'output', live=True, rehearsal=proof)
            credential.assert_not_called()
            session.assert_not_called()
            self.assertFalse((root / 'output').exists())

    def test_live_gate_requires_raw_evidence_before_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            expected = study.contract()
            proof = root / 'summary-only.json'
            proof.write_text(json.dumps(_proof(expected)))
            with patch.object(study, 'credential') as credential, patch.object(study, '_session') as session:
                with self.assertRaises(ValueError):
                    study.run(root / 'live-output', live=True, rehearsal=proof)
            credential.assert_not_called()
            session.assert_not_called()
            self.assertFalse((root / 'live-output').exists())

    def test_run_launches_v2_begin_resume_before_final_matrix_without_provider_calls(self):
        project = _project()
        expected = dict(project_ids=[project['id']], validation={})
        ledger = Mock()
        ledger.budget.return_value = dict(limit=0, spent_or_reserved=0, remaining=0)
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'run'
            events = []
            def execute(command, **kwargs):
                events.append(command[-1])
                self.assertEqual(command[2], 'gossip_harness.sustained_experiment_v2')
                config = json.loads(Path(command[4]).read_text())
                self.assertFalse(config['deterministic_visible'])
                self.assertEqual(config['validation_budget'], asdict(ResourceBudget()))
                return 0
            def finalize(*args):
                events.append('final')
                return dict(accepted=True, status='accepted', milestones_completed=3)
            with patch.object(study, 'contract', return_value=expected), \
                    patch.object(study, 'projects', return_value=[project]), \
                    patch.object(study, '_snapshot_sources'), \
                    patch.object(study, 'Ledger', return_value=ledger), \
                    patch.object(study, '_session') as sessions, \
                    patch.object(study, 'credential') as credential, \
                    patch.object(study, 'OpenAIWorker') as provider, \
                    patch.object(study, '_run_controller', side_effect=execute), \
                    patch.object(study, 'finalize_case', side_effect=finalize):
                report = study.run(output)
            self.assertEqual(events, ['begin', 'resume', 'final'] * 3)
            sessions.assert_called_once()
            self.assertEqual(report['status'], 'finished')
            self.assertEqual(report['incremental_micro_usd'], 0)
            credential.assert_not_called()
            provider.assert_not_called()

    def test_cancellation_interrupts_only_owned_child_and_waits_for_cleanup(self):
        for leader_status in (None, 130):
            with self.subTest(leader_status=leader_status):
                process = Mock(pid=43210)
                process.wait.side_effect = [KeyboardInterrupt(), 130, 130]
                process.poll.return_value = leader_status
                with patch.object(study.subprocess, 'Popen', return_value=process) as launch, \
                        patch.object(study.os, 'killpg') as kill, \
                        patch.object(study.signal, 'signal'):
                    with self.assertRaises(KeyboardInterrupt):
                        study._run_controller(['python', 'owned-child'], cwd=Path('.'))
                self.assertTrue(launch.call_args.kwargs['start_new_session'])
                self.assertEqual([call.args for call in kill.call_args_list],
                                 [(process.pid, study.signal.SIGINT), (process.pid, study.signal.SIGKILL)])
                self.assertEqual([call.kwargs for call in process.wait.call_args_list],
                                 [{}, {'timeout': study.CONTROLLER_DRAIN_SECONDS}, {'timeout': 5}])

    def test_real_parent_termination_drains_owned_child_group(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ready, cancelled, descendant_ready = root / 'ready.json', root / 'cancelled', root / 'descendant.json'
            descendant = ("import json,os,signal\nfrom pathlib import Path\n"
                          "signal.signal(signal.SIGINT,signal.SIG_IGN)\n"
                          "signal.signal(signal.SIGTERM,signal.SIG_IGN)\n"
                          "print('descendant-ready',flush=True)\n"
                          f"Path({str(descendant_ready)!r}).write_text(json.dumps({{'pid':os.getpid()}}))\n"
                          "while True: signal.pause()\n")
            child = ("import json,os,signal,subprocess,sys,time\nfrom pathlib import Path\n"
                     "def stop(number,frame):\n"
                     f" Path({str(cancelled)!r}).write_text('drained')\n"
                     " raise SystemExit(130)\n"
                     "signal.signal(signal.SIGINT,stop)\n"
                     f"subprocess.Popen([sys.executable,'-c',{descendant!r}])\n"
                     f"while not Path({str(descendant_ready)!r}).exists(): time.sleep(.01)\n"
                     f"Path({str(ready)!r}).write_text(json.dumps({{'pid':os.getpid()}}))\n"
                     "signal.pause()\n")
            parent = ("from gossip_harness.sustained_experiment_v2 import cancellation_signals,_run_controller\n"
                      "from pathlib import Path\nimport sys\n"
                      "try:\n with cancellation_signals():\n"
                      f"  _run_controller([sys.executable,'-c',{child!r}],cwd=Path('.'))\n"
                      "except KeyboardInterrupt:\n raise SystemExit(130)\n")
            process = subprocess.Popen([sys.executable, '-c', parent], cwd=study.ROOT,
                                       start_new_session=True, stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE, text=True)
            child_pid = descendant_pid = None
            try:
                deadline = time.monotonic() + 10
                while not ready.exists() and process.poll() is None and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertTrue(ready.exists(), 'Owned child did not reach ready state')
                child_pid = json.loads(ready.read_text())['pid']
                descendant_pid = json.loads(descendant_ready.read_text())['pid']
                process.send_signal(signal.SIGTERM)
                output, error = process.communicate(timeout=10)
                self.assertEqual(process.returncode, 130, error)
                self.assertEqual(output, 'descendant-ready\n')
                self.assertEqual(cancelled.read_text(), 'drained')
                with self.assertRaises(ProcessLookupError):
                    os.kill(child_pid, 0)
                # The descendant deliberately keeps both captured pipes open
                # while ignoring INT/TERM. communicate returning EOF proves it
                # stopped as well, without a process-table query or requiring
                # an orphan to have been reaped already by the platform's init.
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate(timeout=5)
                elif process.stderr is not None:
                    process.stderr.close()
                if child_pid is None and ready.exists():
                    child_pid = json.loads(ready.read_text())['pid']
                if descendant_pid is None and descendant_ready.exists():
                    descendant_pid = json.loads(descendant_ready.read_text())['pid']
                for pid in (child_pid, descendant_pid):
                    if pid is not None:
                        try:
                            os.kill(pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass


    def test_dangling_output_symlink_rejected_without_creating_target(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / 'output'
            target = root / 'missing'
            output.symlink_to(target)
            with self.assertRaisesRegex(ValueError, 'symlink'):
                study.run(output)
            self.assertFalse(target.exists())

    def test_invalid_resume_rejects_before_ledger_or_runtime_setup(self):
        expected = dict(validation={})
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = dict(root=str(root), contract_sha=study.digest(expected), image='image',
                          validation_budget=asdict(ResourceBudget()), deterministic_visible=False,
                          project_id='fixture')
            config_path = root / 'config.json'
            config_path.write_text(json.dumps(config))
            (root / 'handoff.json').write_text(json.dumps(dict(checkpoint_sha256='bad')))
            with patch.object(study, 'contract', return_value=expected), \
                    patch.object(study, 'projects', return_value=[_project()]), \
                    patch.object(study, 'Ledger') as ledger, \
                    patch.object(study, '_session') as session, \
                    patch('gossip_harness.sustained_checkpoint.load_checkpoint', side_effect=ValueError('bad checkpoint')):
                with self.assertRaisesRegex(ValueError, 'bad checkpoint'):
                    study.child(config_path, 'resume')
            ledger.assert_not_called()
            session.assert_not_called()

    def test_child_batches_visible_callbacks_and_keeps_settled_handoff(self):
        project = _project()
        expected = dict(validation={})
        base = Mock()
        base.head.return_value = 'head'
        ledger = Mock()
        keeper = Mock()
        keeper.__enter__ = Mock(return_value=keeper)
        keeper.__exit__ = Mock(return_value=False)
        validation = Mock()
        validation.preflight.return_value = (True, 'ready')
        validation.evaluate_many.return_value = [dict(receipt={'passed': True}, validation={'physical': True})]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = dict(root=str(root), contract_sha=study.digest(expected), image='image',
                          validation_budget=asdict(ResourceBudget()), deterministic_visible=False,
                          validation_cache=str(root / 'cache'), project_id='fixture',
                          budget_ledger=str(root / 'budget.sqlite'), namespace='fixture',
                          policy='strong-single', live=False, run_id='fixture')
            config_path = root / 'config.json'
            config_path.write_text(json.dumps(config))
            def stage(*args, **callbacks):
                callbacks['evaluate_many']([dict(files={'solution.py': 'candidate'}, label='single-1',
                                                 source_sha256=study.digest({'solution.py': 'candidate'}))])
                return dict(completed=True, status='complete', files={'solution.py': 'candidate'},
                            selected_binding={})
            with patch.object(study, 'contract', return_value=expected), \
                    patch.object(study, 'projects', return_value=[project]), \
                    patch.object(study, '_session', return_value=validation), \
                    patch.object(study, 'Ledger', return_value=ledger), \
                    patch.object(study, 'LeaseKeeper', return_value=keeper), \
                    patch.object(study.GitStore, 'create', return_value=base), \
                    patch.object(study, 'record_stage'), \
                    patch.object(study, 'local_promote', return_value=base), \
                    patch.object(study, 'run_stage', side_effect=stage), \
                    patch('gossip_harness.sustained_checkpoint.save_checkpoint', return_value='checkpoint-sha') as save:
                study.child(config_path, 'begin')
            validation.preflight.assert_called_once()
            self.assertEqual(validation.evaluate_many.call_count, 2)
            for index, call in enumerate(validation.evaluate_many.call_args_list):
                self.assertEqual(call.args[1], study.cases_for(project, index, 'visible'))
            saved_state = save.call_args.args[1]
            self.assertEqual(saved_state['next_stage'], 2)
            self.assertEqual(len(saved_state['completed_stages']), 2)
            self.assertTrue((root / 'handoff.json').is_file())
            self.assertFalse((root / 'trajectory.json').exists())


class SustainedV2FinalMatrixTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.project = _project()
        metrics = dict.fromkeys(('builder_calls', 'reviewer_calls', 'repairs',
                                 'premature_completion', 'regressions', 'stagnation_events'), 0)
        self.state = dict(files={'solution.py': 'final'}, stages=[
            dict(files={'solution.py': f'stage-{index}'}, selected_binding={'tip_sha': f'tip-{index}'},
                 metrics=metrics) for index in range(3)], completed_stages=[], invocations=[],
            terminal='max_steps_incomplete')
        (self.root / 'trajectory.json').write_text(json.dumps(self.state))
        self.config = dict(policy='strong-single', repetition=0, run_id='fixture', live=False)
        self.requests = []
        self.validation = Mock()
        self.validation.summary.return_value = {'counts': {'physical_executions': 5}}
        self.validation.evaluate_matrix.side_effect = self.evaluate

    def evaluate(self, requests):
        self.requests = deepcopy(requests)
        return [dict(receipt=dict(passed=True, outcomes=[dict(passed=True) for _ in request['cases']]),
                     validation=_validation(request['purpose'], index))
                for index, request in enumerate(requests)]

    def test_visible_hidden_and_historical_jobs_share_one_post_freeze_batch(self):
        result = study.finalize_case(self.root, self.project, self.config, self.validation)
        self.validation.evaluate_matrix.assert_called_once()
        self.assertEqual([request['purpose'] for request in self.requests],
                         ['final', 'final', 'repeatability', 'repeatability', 'repeatability'])
        self.assertEqual([request['files'] for request in self.requests],
                         [self.state['files']] * 2 + [row['files'] for row in self.state['stages']])
        self.assertEqual([len(request['cases']) for request in self.requests], [3, 3, 1, 2, 3])
        self.assertTrue(all(request['reason'] for request in self.requests))
        self.assertFalse(result['accepted'])
        self.assertFalse((self.root / 'accepted').exists())
        self.assertEqual(result['final_visible_validation']['purpose'], 'final')
        self.assertEqual(len(result['historical']), 3)

    def test_changed_trajectory_during_matrix_never_publishes_result(self):
        def changed(requests):
            observations = self.evaluate(requests)
            (self.root / 'trajectory.json').write_text('{}')
            return observations
        self.validation.evaluate_matrix.side_effect = changed
        with self.assertRaisesRegex(RuntimeError, 'Frozen trajectory'):
            study.finalize_case(self.root, self.project, self.config, self.validation)
        self.assertFalse((self.root / 'result.json').exists())

    def test_reused_or_wrong_purpose_final_observation_never_publishes_result(self):
        for change in (dict(physical=False), dict(purpose='visible'), dict(reuse={'kind': 'persisted'})):
            with self.subTest(change=change):
                def invalid(requests):
                    observations = self.evaluate(requests)
                    observations[0]['validation'].update(change)
                    return observations
                self.validation.evaluate_matrix.side_effect = invalid
                with self.assertRaisesRegex(RuntimeError, 'must execute physically'):
                    study.finalize_case(self.root, self.project, self.config, self.validation)
                self.assertFalse((self.root / 'result.json').exists())
