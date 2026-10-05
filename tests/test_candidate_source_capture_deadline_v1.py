"""Enclosing-clock controls with inert Git and owned host-authored subprocesses.

No candidate import, Engine execution, provider use or independent approval.
"""
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

from gossip_harness import candidate_source_capture_deadline_v1 as bounded
from gossip_harness import candidate_git_source_batch_v1 as legacy
from gossip_harness import candidate_git_source_two_process_v1 as capture
from gossip_harness.gitstore import GitStore


class EnclosingCapturePolicyTests(unittest.TestCase):
    def test_policy_binds_fresh_head_checks_and_separate_cleanup(self):
        record = bounded.DeadlineCapturePolicy().record()
        self.assertEqual(record['children_per_capture'], 4)
        self.assertTrue(record['enclosing_deadline_required'])
        self.assertEqual(record['cleanup_reap_seconds'], 5)
        self.assertFalse(record['cross_boundary_cache'])
        self.assertIn('candidate_source_capture_deadline_v1.py', '\n'.join(record['sources']))
        for timeout, cleanup in ((True, 5), (59, 5), (60, 4)):
            with self.assertRaises(ValueError): bounded.DeadlineCapturePolicy(timeout, cleanup).record()

    def test_all_phases_get_same_parent_bound_without_renewal(self):
        store = object.__new__(GitStore); store.path = Path('/inert')
        commit = 'a' * 40; observed = []
        def command(path, args, data, limit, deadline):
            observed.append(deadline)
            return (commit + '\n').encode() if args[0] == 'rev-parse' else b'listing'
        def parse(raw, deadline): observed.append(deadline); return []
        def cat(path, oid, entries, deadline): observed.append(deadline); return 'b'*40, {}
        for parent, expected in ((100_250_000_000, 100.25), (300_000_000_000, 160.0)):
            observed.clear()
            with mock.patch.object(time, 'monotonic_ns', return_value=100_000_000_000), \
                    mock.patch.object(time, 'monotonic', return_value=100.0), \
                    mock.patch.object(legacy, '_command', side_effect=command), \
                    mock.patch.object(capture, '_entries', side_effect=parse), \
                    mock.patch.object(capture, '_cat_capture', side_effect=cat):
                bounded.capture_registered_source(store, commit,
                    policy=bounded.DeadlineCapturePolicy(), deadline_ns=parent)
            self.assertEqual(len(observed), 5)
            self.assertEqual(len(set(observed)), 1)
            self.assertLessEqual(observed[0], expected)
            self.assertAlmostEqual(observed[0], expected, places=8)

    def test_policy_validation_time_consumes_parent_and_local_allowance(self):
        store = object.__new__(GitStore); store.path = Path('/inert')
        now = [100.0]
        def validate(_): now[0] = 101.0; return {}
        with mock.patch.object(time, 'monotonic_ns', return_value=100_000_000_000), \
                mock.patch.object(time, 'monotonic', side_effect=lambda: now[0]), \
                mock.patch.object(bounded.DeadlineCapturePolicy, 'record', validate), \
                mock.patch.object(legacy, '_command') as dispatch:
            with self.assertRaisesRegex(ValueError, 'deadline exhausted'):
                bounded.capture_registered_source(store, 'a'*40,
                    policy=bounded.DeadlineCapturePolicy(), deadline_ns=100_100_000_000)
            dispatch.assert_not_called()

    def test_exact_deadline_is_required_before_dispatch(self):
        store = object.__new__(GitStore); store.path = Path('/inert')
        for value in (None, True, 0, -1, 2**63, float('inf'), time.monotonic_ns()-1):
            with self.subTest(value=value), mock.patch.object(legacy, '_command') as dispatch:
                with self.assertRaises(ValueError):
                    bounded.capture_registered_source(store, 'a'*40,
                        policy=bounded.DeadlineCapturePolicy(), deadline_ns=value)
                dispatch.assert_not_called()


class EnclosingCaptureGitTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='enclosing-capture-')
        self.addCleanup(self.temp.cleanup)
        self.store = GitStore.create(Path(self.temp.name)/'source.git', {'source.py': 'raise RuntimeError("inert")\n'})
        self.commit = self.store.head()

    def capture(self, seconds=30):
        return bounded.capture_registered_source(self.store, self.commit,
            policy=bounded.DeadlineCapturePolicy(), deadline_ns=time.monotonic_ns()+int(seconds*1e9))

    def test_real_capture_matches_original_and_uses_four_fresh_children(self):
        expected = capture.capture_git_source_two_process(self.store.path, self.commit)
        original = subprocess.Popen
        with mock.patch.object(subprocess, 'Popen', wraps=original) as launch:
            self.assertEqual(self.capture(), expected)
            self.assertEqual(self.capture(), expected)
        self.assertEqual(launch.call_count, 8)

    def test_accepted_head_change_during_capture_is_refused(self):
        original = capture._cat_capture
        offered = self.store.propose({'new.txt': 'changed'}, self.commit)
        def change(*args):
            result = original(*args)
            self.store._git('update-ref', 'refs/heads/accepted', offered, self.commit)
            return result
        with mock.patch.object(capture, '_cat_capture', side_effect=change):
            with self.assertRaisesRegex(ValueError, 'registered_head_changed'): self.capture()

    def test_each_stalled_git_phase_is_killed_without_a_fresh_sixty_seconds(self):
        # Author-authored sleeper replaces only a selected Git child. This is
        # a real process/reap check, not a candidate or Docker observation.
        original = subprocess.Popen
        for selected in (1, 2, 3, 4):
            children = []
            def launch(argv, **kwargs):
                if len(children)+1 == selected:
                    argv = [sys.executable, '-I', '-B', '-c', 'import time; time.sleep(20)']
                child = original(argv, **kwargs); children.append(child)
                return child
            with self.subTest(phase=selected), mock.patch.object(subprocess, 'Popen', side_effect=launch):
                started = time.monotonic()
                try:
                    with self.assertRaisesRegex(ValueError, 'deadline'):
                        self.capture(seconds=3)
                    self.assertLess(time.monotonic()-started, 8)
                    self.assertEqual(len(children), selected)
                    self.assertTrue(all(c.poll() is not None for c in children))
                    self.assertTrue(all(p is None or p.closed for c in children for p in (c.stdin,c.stdout,c.stderr)))
                finally:
                    for child in children:
                        if child.poll() is None: child.kill()
                        child.wait(timeout=5)

    def test_failed_cleanup_is_not_relabelled_complete(self):
        with mock.patch.object(capture, '_cat_capture', side_effect=legacy.CaptureCleanupError(('process.wait:TimeoutExpired',))):
            with self.assertRaises(legacy.CaptureCleanupError): self.capture()
