"""Actual owned host processes and disposable Git/financial records; no provider."""
from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
import unittest
from unittest.mock import patch

from gossip_harness import cumulative_process_evidence_v1 as evidence
from gossip_harness.candidate_checkpoint_chain_v1 import ChainUnknown, Limits, CheckpointChain, BoundsExceeded
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from gossip_harness.candidate_observation_admission_v1 import source_sha256
from gossip_harness.gitstore import GitStore
from gossip_harness.peer_financial_authority_v2 import canonical_payload
from tests.financial_v4_fixture import Fixture

PRELUDE = 'from gossip_harness.cumulative_process_evidence_v1 import publish_role_event\n'
HEALTHY = PRELUDE + 'publish_role_event("ready")\npublish_role_event("final", outcome="stopped")\n'
WAIT_AFTER_FINAL = HEALTHY + 'import time\ntime.sleep(30)\n'
WAIT_AFTER_READY = PRELUDE + 'publish_role_event("ready")\nimport time\ntime.sleep(30)\n'


class CumulativeProcessEvidenceV1Tests(Fixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.store = GitStore.create(self.root / 'protected.git', {'src/a.py': 'answer = 42\n', 'README.md': 'fixture\n'})
        self.bridge = evidence.ProcessEvidence(self.roster, self.child.cohort, self.chain, self.chain.commitment,
                                               protected_store=self.store)
        self.env = {'PATH': os.defpath, 'PYTHONPATH': str(Path(__file__).resolve().parent.parent)}
        self.addCleanup(self.clean_owned)

    def clean_owned(self):
        for _, _, process in self.bridge.handles():
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=5)

    def launch(self, actor=None, code=HEALTHY, generation=0):
        actor = actor or self.actor
        directory = self.root / (actor + '-' + str(generation))
        directory.mkdir()
        process = self.bridge.launch(actor, [sys.executable, '-c', code], cwd=self.root,
            env=self.env, stdout_path=directory / 'stdout.log', generation=generation)
        return process, directory

    def wait_file(self, path, process):
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if path.exists():
                return
            if process.poll() is not None:
                self.fail('Owned fixture exited before expected file: ' + str(path))
            time.sleep(.005)
        self.fail('Owned fixture file deadline: ' + str(path))

    def complete_roles(self, exclude=()):
        for actor in self.child.actors:
            if actor not in exclude:
                self.launch(actor)
                self.bridge.observe_exit(actor, timeout=5)

    def finalize(self, status='completed'):
        return self.bridge.finalize(self.store, status, self.chain.commitment)

    def closure(self):
        return json.loads(self.chain.read(evidence.closure_name(self.child.cohort)))

    def test_actual_eight_role_waits_git_identity_and_financial_seal(self):
        self.open()
        self.complete_roles()
        request = self.finalize()
        self.assertEqual(request, self.bridge.verified_request(self.chain.commitment))
        confirmation = json.loads(self.chain.read(evidence.confirmation_name(self.child.cohort)))
        closure = self.closure()
        source = json.loads(self.chain.read(closure['source']['name']))
        self.assertEqual(confirmation['commit_oid'], source['commit_oid'])
        self.assertEqual(confirmation['request_sha256'], evidence.digest(request.record()))
        self.assertGreater(self.chain.position(evidence.confirmation_name(self.child.cohort)),
                           self.chain.position(evidence.closure_name(self.child.cohort)))
        self.assertEqual(source['commit_oid'], self.store.head())
        self.assertEqual(source['tree_oid'], self.store._git('rev-parse', 'HEAD^{tree}'))
        self.assertEqual(source['source_sha256'], source_sha256({'src/a.py': b'answer = 42\n', 'README.md': b'fixture\n'}))
        self.assertEqual(request.terminal.read(self.chain)['final_source_sha256'], source['source_sha256'])
        self.assertFalse(closure['acceptance_authority'])
        self.assertEqual(len(closure['roles']), 8)
        for role in closure['roles']:
            original = role['incarnations'][0]
            self.assertFalse(role['never_launched'])
            wait = json.loads(self.chain.read(original['wait']['name']))
            self.assertEqual(wait['returncode'], 0)
            self.assertEqual(wait['event_errors'], [])
            self.assertTrue(wait['process_group_empty'])
            order = [self.chain.position(original[k]['name']) for k in ('intent', 'launch', 'ready', 'final', 'wait')]
            self.assertEqual(order, sorted(set(order)))
            self.assertLess(order[-1], self.chain.position(closure['source']['name']))
        preparation = self.authority.prepare_terminal_snapshot(request)
        seal = self.authority.seal_terminal(preparation)
        self.assertEqual(seal, self.authority.verified_terminal_seal(self.chain.commitment))
        self.assertEqual(self.transport.calls, [])

    def test_final_message_does_not_prove_actual_exit(self):
        process, directory = self.launch(code=WAIT_AFTER_FINAL)
        self.wait_file(directory / 'final.json', process)
        self.bridge.observe_ready(self.actor)
        with self.assertRaises(subprocess.TimeoutExpired):
            self.bridge.observe_exit(self.actor)
        with self.assertRaisesRegex(evidence.ProcessEvidenceError, 'stop'):
            self.finalize('stopped_failure')
        self.assertFalse(self.chain.has(evidence.closure_name(self.child.cohort)))

    def test_ready_identity_is_bound_to_actual_owned_pid(self):
        process, directory = self.launch(code=WAIT_AFTER_READY)
        self.wait_file(directory / 'ready.json', process)
        value = json.loads((directory / 'ready.json').read_text())
        value['pid'] += 1
        (directory / 'ready.json').write_bytes(canonical_payload(value))
        with self.assertRaisesRegex(evidence.ProcessEvidenceError, 'identity'):
            self.bridge.observe_ready(self.actor)

    def test_ready_identity_is_bound_to_launch_nonce(self):
        process, directory = self.launch(code=WAIT_AFTER_READY)
        self.wait_file(directory / 'ready.json', process)
        value = json.loads((directory / 'ready.json').read_text())
        value['identity']['launch_id'] = 'foreign-launch'
        (directory / 'ready.json').write_bytes(canonical_payload(value))
        with self.assertRaisesRegex(evidence.ProcessEvidenceError, 'identity'):
            self.bridge.observe_ready(self.actor)

    def test_changed_ready_after_observation_blocks_completed_closure(self):
        process, directory = self.launch(code=WAIT_AFTER_FINAL)
        self.wait_file(directory / 'final.json', process)
        self.bridge.observe_ready(self.actor)
        value = json.loads((directory / 'ready.json').read_text())
        value['pid'] += 1
        (directory / 'ready.json').write_bytes(canonical_payload(value))
        os.killpg(process.pid, signal.SIGTERM)
        wait = self.bridge.observe_exit(self.actor, timeout=5).read(self.chain)
        self.assertEqual(wait['event_errors'], [{'event': 'ready', 'error_type': 'ProcessEvidenceError'}])
        self.complete_roles(exclude=(self.actor,))
        with self.assertRaisesRegex(evidence.ProcessEvidenceError, 'Successful role stop'):
            self.finalize()

    def test_removed_ready_after_observation_blocks_completed_even_zero_exit(self):
        code = HEALTHY + 'from pathlib import Path\nimport time\nwhile not Path("release").exists(): time.sleep(.005)\n'
        process, directory = self.launch(code=code)
        self.wait_file(directory / 'final.json', process)
        self.bridge.observe_ready(self.actor)
        (directory / 'ready.json').unlink()
        (self.root / 'release').touch()
        wait = self.bridge.observe_exit(self.actor, timeout=5).read(self.chain)
        self.assertEqual(wait['returncode'], 0)
        self.assertTrue(wait['event_errors'])
        self.complete_roles(exclude=(self.actor,))
        with self.assertRaisesRegex(evidence.ProcessEvidenceError, 'Successful role stop'):
            self.finalize()

    def test_missing_final_is_stopped_failure_only(self):
        self.launch(code=PRELUDE + 'publish_role_event("ready")\n')
        wait = self.bridge.observe_exit(self.actor, timeout=5).read(self.chain)
        self.assertTrue(wait['event_errors'])
        with self.assertRaises(evidence.ProcessEvidenceError):
            self.finalize()
        request = self.finalize('stopped_failure')
        self.assertEqual(request.terminal.read(self.chain)['status'], 'stopped_failure')
        self.assertEqual(sum(role['never_launched'] for role in self.closure()['roles']), 7)

    def test_unstarted_child_is_explicit_failure_and_not_success(self):
        with self.assertRaisesRegex(evidence.ProcessEvidenceError, 'every registered role'):
            self.finalize()
        self.finalize('stopped_failure')
        self.assertTrue(all(role['never_launched'] for role in self.closure()['roles']))

    def test_planned_restart_retains_both_actual_incarnations(self):
        self.launch(code=PRELUDE + 'publish_role_event("ready")\npublish_role_event("final", outcome="restart")\nraise SystemExit(86)\n')
        self.bridge.observe_exit(self.actor, timeout=5)
        self.launch(generation=1)
        self.bridge.observe_exit(self.actor, generation=1, timeout=5)
        self.complete_roles(exclude=(self.actor,))
        self.finalize()
        role = next(role for role in self.closure()['roles'] if role['actor'] == self.actor)
        waits = [json.loads(self.chain.read(item['wait']['name'])) for item in role['incarnations']]
        self.assertEqual([item['returncode'] for item in waits], [86, 0])
        self.assertNotEqual(waits[0]['identity']['launch_id'], waits[1]['identity']['launch_id'])

    def test_restart_before_authenticated_wait_is_rejected(self):
        self.launch(code=WAIT_AFTER_READY)
        with self.assertRaisesRegex(evidence.ProcessEvidenceError, 'Prior incarnation'):
            self.launch(generation=1)

    def test_postclosure_launch_is_rejected_before_spawning(self):
        self.finalize('stopped_failure')
        with self.assertRaisesRegex(evidence.ProcessEvidenceError, 'permanently closed'):
            self.launch()
        self.assertEqual(self.bridge.handles(), ())

    def test_actual_leader_exit_with_live_descendant_is_not_a_stop(self):
        code = HEALTHY + 'import os, time\nif os.fork() == 0:\n time.sleep(30)\n os._exit(0)\n'
        process, _ = self.launch(code=code)
        process.wait(timeout=5)
        with self.assertRaisesRegex(evidence.ProcessEvidenceError, 'group remains'):
            self.bridge.observe_exit(self.actor)
        with self.assertRaisesRegex(evidence.ProcessEvidenceError, 'stop'):
            self.finalize('stopped_failure')

    def test_known_exec_failure_is_recorded_but_not_completed(self):
        directory = self.root / 'failed-launch'
        directory.mkdir()
        with self.assertRaises(FileNotFoundError):
            self.bridge.launch(self.actor, [str(self.root / 'missing-executable')], cwd=self.root,
                env=self.env, stdout_path=directory / 'stdout.log')
        with self.assertRaises(evidence.ProcessEvidenceError):
            self.finalize()
        self.finalize('stopped_failure')
        role = next(role for role in self.closure()['roles'] if role['actor'] == self.actor)
        self.assertTrue(role['never_launched'])
        failed = json.loads(self.chain.read(role['incarnations'][0]['launch_failed']['name']))
        self.assertEqual(failed['error_type'], 'FileNotFoundError')
        self.assertEqual(self.bridge.handles(), ())

    def test_unclassified_launch_exception_cannot_be_closed_or_retried(self):
        with patch.object(evidence.subprocess, 'Popen', side_effect=RuntimeError('uncertain fixture start')):
            with self.assertRaises(RuntimeError):
                self.launch()
        with self.assertRaisesRegex(evidence.ProcessEvidenceError, 'unproven'):
            self.finalize('stopped_failure')
        directory = self.root / 'retry'
        directory.mkdir()
        with self.assertRaisesRegex(evidence.ProcessEvidenceError, 'already attempted'):
            self.bridge.launch(self.actor, [sys.executable, '-c', HEALTHY], cwd=self.root,
                env=self.env, stdout_path=directory / 'stdout.log')

    def test_postlaunch_checkpoint_failure_keeps_handle_for_owned_cleanup(self):
        original = self.chain.retain
        def retain(name, raw, **kwargs):
            if name.endswith('.launch.json'):
                raise ChainUnknown('fixture publication uncertainty')
            return original(name, raw, **kwargs)
        with patch.object(self.chain, 'retain', side_effect=retain):
            with self.assertRaises(ChainUnknown):
                self.launch(code=WAIT_AFTER_READY)
        self.assertEqual(len(self.bridge.handles()), 1)
        process = self.bridge.handles()[0][2]
        self.assertIsNone(process.poll())
        with self.assertRaisesRegex(evidence.ProcessEvidenceError, 'acknowledged'):
            self.bridge.observe_exit(self.actor)
        with self.assertRaisesRegex(evidence.ProcessEvidenceError, 'unproven'):
            self.finalize('stopped_failure')

    def test_wait_replay_is_read_only(self):
        self.launch()
        first = self.bridge.observe_exit(self.actor, timeout=5)
        head = self.chain.commitment
        with patch.object(subprocess.Popen, 'wait', side_effect=AssertionError('must reuse original')):
            self.assertEqual(first, self.bridge.observe_exit(self.actor))
        self.assertEqual(head, self.chain.commitment)

    def test_directory_replacement_is_rejected(self):
        process, directory = self.launch(code=WAIT_AFTER_READY)
        self.wait_file(directory / 'ready.json', process)
        directory.rename(self.root / 'original-directory')
        directory.mkdir()
        with self.assertRaisesRegex(evidence.ProcessEvidenceError, 'directory changed'):
            self.bridge.observe_ready(self.actor)

    def test_source_change_fences_launch(self):
        with patch.object(evidence, 'source_fingerprints', return_value={}):
            with self.assertRaisesRegex(evidence.ProcessEvidenceError, 'source changed'):
                self.launch()
        self.assertEqual(self.bridge.handles(), ())

    def test_changed_git_head_during_capture_does_not_publish_closure(self):
        original = self.store.head()
        with patch.object(self.store, 'head', side_effect=[original, 'e' * 40]):
            with self.assertRaisesRegex(evidence.ProcessEvidenceError, 'head changed'):
                self.finalize('stopped_failure')
        self.assertFalse(self.chain.has(evidence.closure_name(self.child.cohort)))
        with self.assertRaisesRegex(evidence.ProcessEvidenceError, 'already attempted'):
            self.finalize('stopped_failure')

    def test_foreign_thread_cannot_read_cleanup_handles(self):
        errors = []
        def foreign():
            try:
                self.bridge.handles()
            except evidence.ProcessEvidenceError as error:
                errors.append(str(error))
        thread = threading.Thread(target=foreign)
        thread.start()
        thread.join(3)
        self.assertEqual(len(errors), 1)
        self.assertIn('Foreign', errors[0])

    def test_child_helper_requires_ready_before_final(self):
        self.launch(code=PRELUDE + 'publish_role_event("final", outcome="stopped")\n')
        wait = self.bridge.observe_exit(self.actor, timeout=5).read(self.chain)
        self.assertNotEqual(wait['returncode'], 0)
        self.assertEqual({item['event'] for item in wait['event_errors']}, {'ready', 'final'})

    def test_child_event_is_exclusive_and_cannot_overwrite_original(self):
        self.launch(code=HEALTHY + 'publish_role_event("final", outcome="failed")\n')
        wait = self.bridge.observe_exit(self.actor, timeout=5).read(self.chain)
        self.assertNotEqual(wait['returncode'], 0)
        self.assertEqual(json.loads(self.chain.read(wait['final']['name']))['outcome'], 'stopped')

    def test_remaining_action_prevents_success_even_with_zero_exit(self):
        self.launch(code=PRELUDE + 'publish_role_event("ready")\npublish_role_event("final", outcome="stopped", remaining_action_ids=("unfinished",))\n')
        self.bridge.observe_exit(self.actor, timeout=5)
        self.complete_roles(exclude=(self.actor,))
        with self.assertRaisesRegex(evidence.ProcessEvidenceError, 'Successful role stop'):
            self.finalize()

    def test_boolean_generation_does_not_equal_integer_identity(self):
        process, directory = self.launch(code=WAIT_AFTER_READY)
        self.wait_file(directory / 'ready.json', process)
        value = json.loads((directory / 'ready.json').read_text())
        value['identity']['generation'] = False
        (directory / 'ready.json').write_bytes(canonical_payload(value))
        with self.assertRaisesRegex(evidence.ProcessEvidenceError, 'generation'):
            self.bridge.observe_ready(self.actor)

    def test_repository_cannot_be_substituted_at_closure(self):
        other = GitStore.create(self.root / 'other.git', {'src/a.py': 'other\n'})
        with self.assertRaisesRegex(evidence.ProcessEvidenceError, 'repository changed'):
            self.bridge.finalize(other, 'stopped_failure', self.chain.commitment)
        self.assertFalse(self.chain.has(evidence.closure_name(self.child.cohort)))

    def test_postpublication_git_head_change_cannot_return_completed_request(self):
        original = self.store.head()
        with patch.object(self.store, 'head', side_effect=[original, original, 'e' * 40]):
            with self.assertRaisesRegex(evidence.ProcessEvidenceError, 'changed after closure'):
                self.finalize('stopped_failure')
        # Original attempted publication is retained; it is not a returned/sealed request.
        self.assertTrue(self.chain.has(evidence.closure_name(self.child.cohort)))
        self.assertFalse(self.chain.has(evidence.confirmation_name(self.child.cohort)))
        with self.assertRaisesRegex(evidence.ProcessEvidenceError, 'No successfully finalized'):
            self.bridge.verified_request(self.chain.commitment)
        with self.assertRaisesRegex(evidence.ProcessEvidenceError, 'already attempted'):
            self.finalize('stopped_failure')

    def test_normal_capacity_exhaustion_preserves_real_stop_and_failure_closure(self):
        self.chain.close()
        self.head.close()
        self.head = ExternalHead.create(self.root / 'anchor-small', journal_roots=(self.root / 'raw-small', self.root / 'delta-small'))
        self.chain = CheckpointChain.create(self.root / 'raw-small', self.root / 'delta-small',
            context={'fixture': 'reserved-cleanup'}, authority=self.head,
            limits=Limits(max_files=32, cleanup_files=29))
        self.bridge = evidence.ProcessEvidence(self.roster, self.child.cohort, self.chain, self.chain.commitment,
                                               protected_store=self.store)
        self.launch()
        with self.assertRaises(BoundsExceeded):
            self.chain.retain('normal-overflow.json', b'cannot consume reserve')
        wait = self.bridge.observe_exit(self.actor, timeout=5).read(self.chain)
        self.assertEqual(wait['returncode'], 0)
        self.finalize('stopped_failure')
        self.assertTrue(self.chain.has(evidence.closure_name(self.child.cohort)))

    def test_verified_request_rejects_later_git_head_change(self):
        self.finalize('stopped_failure')
        with patch.object(self.store, 'head', return_value='e' * 40):
            with self.assertRaisesRegex(evidence.ProcessEvidenceError, 'no longer current'):
                self.bridge.verified_request(self.chain.commitment)

    def test_verified_request_rejects_tampered_confirmation(self):
        self.finalize('stopped_failure')
        path = self.chain.raw_root / evidence.confirmation_name(self.child.cohort)
        path.write_bytes(path.read_bytes() + b' ')
        with self.assertRaises(ChainUnknown):
            self.bridge.verified_request(self.chain.commitment)
