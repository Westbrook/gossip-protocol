"""Offline qualification of the explicit Git batch capture boundary."""
from pathlib import Path
import hashlib
import os
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

from gossip_harness import candidate_git_source_batch_v1 as capture
from gossip_harness.candidate_release_execution_v2 import capture_git_source as legacy
from gossip_harness.gitstore import _run


class _Store:
    def __init__(self, path):
        self.path = path

    def _git(self, *args):
        return _run(self.path, *args).stdout.decode().strip()


def _make(root, files):
    root.mkdir()
    _run(None, "init", "--initial-branch=main", str(root))
    for name, raw in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
    _run(root, "add", "--all")
    _run(root, "commit", "-m", "fixture")
    return _Store(root), _run(root, "rev-parse", "HEAD").stdout.decode().strip()


def _responses(files=None):
    files = files or {"a.txt": b"x", "b.dat": b"\x00\xff"}
    commit, tree = "a" * 40, "b" * 40
    listing, sizes, contents = b"", b"", b""
    for name, content in files.items():
        oid = hashlib.sha1(b"blob " + str(len(content)).encode() + b"\0" + content).hexdigest().encode()
        header = oid + b" blob " + str(len(content)).encode() + b"\n"
        listing += b"100644 blob " + oid + b"\t" + name.encode() + b"\0"
        sizes += header
        contents += header + content + b"\n"
    return commit, tree, [("sha1\n" + commit + "\n" + tree + "\n").encode(), listing, sizes, contents]


class CandidateGitSourceBatchV1Tests(unittest.TestCase):
    def test_valid_synthetic_framing_has_known_blob_baseline(self):
        commit, tree, replies = _responses()
        self.assertIn(b"c1b0730e0133447badcfd47fd144e254807b06e1 blob 1\n", replies[2])
        with patch.object(capture, "_command", side_effect=replies) as command:
            self.assertEqual(capture.capture_git_source_batch(Path("/tmp"), commit),
                             (tree, {"a.txt": b"x", "b.dat": b"\x00\xff"}))
        self.assertEqual(command.call_count, 4)

    def test_identity_requires_exact_lf_framing_and_commit_tree(self):
        commit, _, replies = _responses()
        for raw in (replies[0][:-1], replies[0].replace(b"\n", b"\r\n"),
                    replies[0] + b"\n", replies[0].replace(commit.encode(), b"c" * 40),
                    replies[0].replace(b"sha1", b"sha256"), b"\xff"):
            with self.subTest(raw=raw[:20]), patch.object(capture, "_command", return_value=raw) as command:
                with self.assertRaises(capture.CaptureError):
                    capture.capture_git_source_batch(Path("/tmp"), commit)
                self.assertEqual(command.call_count, 1)

    def test_size_rows_require_exact_order_count_lf_type_and_numeric_framing(self):
        commit, _, replies = _responses()
        first, second = replies[2].splitlines(keepends=True)
        changes = [replies[2][:-1], replies[2].replace(b"\n", b"\r\n"), second + first,
                   first, replies[2] + first, replies[2].replace(b" blob ", b" tree ", 1),
                   first.replace(b" blob 1", b" missing") + second,
                   first.replace(b" blob 1", b" blob 01") + second,
                   first.replace(b" blob 1", b" blob +1") + second,
                   first.replace(b" blob 1", b" blob " + b"9" * 5000) + second,
                   first.replace(b" blob 1", b" blob 2097153") + second]
        for index, raw in enumerate(changes):
            with self.subTest(case=index), patch.object(capture, "_command", side_effect=replies[:2] + [raw]) as command:
                with self.assertRaises(capture.CaptureError):
                    capture.capture_git_source_batch(Path("/tmp"), commit)
                self.assertEqual(command.call_count, 3)

    def test_content_header_hash_length_newline_and_suffix_must_match(self):
        commit, _, replies = _responses()
        original = replies[3]
        changes = [original[:-1], original + b"suffix", original.replace(b"\nx\n", b"\ny\n"),
                   original.replace(b" blob 1\n", b" blob 2\n", 1),
                   original.replace(b" blob 1\n", b" blob 1\r\n", 1), original[1:],
                   original.replace(b"\nx\n", b"\nx"), original[:-3]]
        for index, raw in enumerate(changes):
            with self.subTest(case=index), patch.object(capture, "_command", side_effect=replies[:3] + [raw]):
                with self.assertRaises(capture.CaptureError):
                    capture.capture_git_source_batch(Path("/tmp"), commit)

    def test_tree_requires_complete_unique_safe_utf8_ordinary_files(self):
        commit, _, replies = _responses({"a.txt": b"x"})
        original = replies[1]
        changes = [b"", original[:-1], original + original, original + b"\0",
                   original.replace(b"100644", b"120000"), original.replace(b" blob ", b" tree "),
                   original.replace(b"a.txt", b"../a.txt"), original.replace(b"a.txt", b"a/.private"),
                   original.replace(b"a.txt", b"/a.txt"), original.replace(b"a.txt", b"a\\b"),
                   original.replace(b"a.txt", b"a//b"), original.replace(b"a.txt", b"\xff"),
                   original.replace(b"c1b0730e0133447badcfd47fd144e254807b06e1", b"not-an-oid")]
        for index, raw in enumerate(changes):
            with self.subTest(case=index), patch.object(capture, "_command", side_effect=[replies[0], raw]) as command:
                with self.assertRaises(capture.CaptureError):
                    capture.capture_git_source_batch(Path("/tmp"), commit)
                self.assertEqual(command.call_count, 2)

    def test_file_count_and_aggregate_byte_bound_precede_content_dispatch(self):
        commit, _, replies = _responses({f"f{i}": b"x" for i in range(capture.MAX_FILES + 1)})
        with patch.object(capture, "_command", side_effect=replies) as command:
            with self.assertRaises(capture.CaptureError):
                capture.capture_git_source_batch(Path("/tmp"), commit)
            self.assertEqual(command.call_count, 2)
        commit, _, replies = _responses({f"f{i}": b"x" for i in range(9)})
        replies[2] = replies[2].replace(b" blob 1\n", b" blob 2097152\n")
        with patch.object(capture, "_command", side_effect=replies) as command:
            with self.assertRaises(capture.CaptureError):
                capture.capture_git_source_batch(Path("/tmp"), commit)
            self.assertEqual(command.call_count, 3)

    def test_exact_file_per_blob_and_aggregate_limits_are_admitted(self):
        fixtures = [
            {f"f{i}": b"x" for i in range(capture.MAX_FILES)},
            {"exact.bin": b"x" * capture.MAX_FILE_BYTES},
            {f"f{i}": b"x" * capture.MAX_FILE_BYTES for i in range(8)},
        ]
        for files in fixtures:
            with self.subTest(files=len(files), bytes=sum(map(len, files.values()))):
                commit, tree, replies = _responses(files)
                with patch.object(capture, "_command", side_effect=replies) as command:
                    result = capture.capture_git_source_batch(Path("/tmp"), commit)
                self.assertEqual(result, (tree, files))
                self.assertEqual(command.call_count, 4)

    def test_exact_aggregate_limit_plus_one_rejects_before_content_dispatch(self):
        files = {f"f{i}": b"x" * capture.MAX_FILE_BYTES for i in range(8)}
        files["extra"] = b"x"
        self.assertEqual(sum(map(len, files.values())), capture.MAX_SOURCE_BYTES + 1)
        commit, _, replies = _responses(files)
        with patch.object(capture, "_command", side_effect=replies[:3]) as command:
            with self.assertRaisesRegex(capture.CaptureError, "size bound"):
                capture.capture_git_source_batch(Path("/tmp"), commit)
            self.assertEqual(command.call_count, 3)

    def test_invalid_identity_deadline_and_pipe_bounds_reject_before_dispatch(self):
        with patch.object(capture.subprocess, "Popen") as launch:
            for oid, seconds in [("main", 60), ("a" * 40, True), ("a" * 40, 121), ("a" * 40, 0)]:
                with self.assertRaises(capture.CaptureError):
                    capture.capture_git_source_batch(Path("/tmp"), oid, timeout_seconds=seconds)
            for data, limit in [(b"x" * (capture.MAX_INPUT_BYTES + 1), 1), (b"", -1),
                                (b"", capture.MAX_COMMAND_BYTES + 1), (b"", True)]:
                with self.assertRaises(capture.CaptureError):
                    capture._command(Path("/tmp"), (), data, limit, time.monotonic() + 1)
            launch.assert_not_called()

    def test_final_parse_after_deadline_cannot_return_success(self):
        commit, _, replies = _responses()
        clock = [0.0]
        original = capture.hashlib.sha1
        def late_hash(raw):
            clock[0] = 2.0
            return original(raw)
        with patch.object(capture, "_command", side_effect=replies), \
             patch.object(capture.time, "monotonic", side_effect=lambda: clock[0]), \
             patch.object(capture.hashlib, "sha1", side_effect=late_hash):
            with self.assertRaisesRegex(capture.CaptureError, "deadline"):
                capture.capture_git_source_batch(Path("/tmp"), commit, timeout_seconds=1)

    def test_selector_construction_failure_never_launches_child(self):
        with patch.object(capture.selectors, "DefaultSelector", side_effect=OSError("selector")), \
             patch.object(capture.subprocess, "Popen") as launch:
            with self.assertRaises(capture.CaptureError):
                capture._command(Path("/tmp"), (), b"", 1, time.monotonic() + 1)
            launch.assert_not_called()

    def test_spawn_failure_still_closes_allocated_selector(self):
        selector = Mock()
        with patch.object(capture.selectors, "DefaultSelector", return_value=selector), \
             patch.object(capture.subprocess, "Popen", side_effect=OSError("spawn")):
            with self.assertRaises(capture.CaptureError):
                capture._command(Path("/tmp"), (), b"", 1, time.monotonic() + 1)
        selector.close.assert_called_once_with()

    def test_cleanup_faults_cannot_skip_other_release_attempts(self):
        selector = Mock()
        selector.close.side_effect = OSError("selector")
        child = Mock()
        child.poll.side_effect = OSError("poll")
        child.kill.side_effect = OSError("kill")
        child.wait.side_effect = subprocess.TimeoutExpired("git", 5)
        child.stdin.close.side_effect = OSError("stdin")
        child.stdout.close.side_effect = OSError("stdout")
        failures, interruption = capture._release(selector, child)
        self.assertIsNone(interruption)
        self.assertEqual(len(failures), 6)
        for action in (selector.close, child.poll, child.kill, child.wait,
                       child.stdin.close, child.stdout.close, child.stderr.close):
            self.assertEqual(action.call_count, 1)
        self.assertLessEqual(child.wait.call_args.kwargs["timeout"], capture.CLEANUP_REAP_SECONDS)

    def test_cleanup_error_is_not_reported_as_capture_success(self):
        child = Mock()
        for name in ("stdin", "stdout", "stderr"):
            pipe = Mock()
            pipe.fileno.return_value = 3
            setattr(child, name, pipe)
        child.poll.return_value = 0
        child.wait.return_value = 0
        child.returncode = 0
        selector = Mock()
        selector.get_map.return_value = {}
        selector.close.side_effect = OSError("close")
        with patch.object(capture.subprocess, "Popen", return_value=child), \
             patch.object(capture.selectors, "DefaultSelector", return_value=selector), \
             patch.object(capture.os, "set_blocking"):
            with self.assertRaises(capture.CaptureCleanupError) as failure:
                capture._command(Path("/tmp"), (), b"", 1, time.monotonic() + 1)
        self.assertIn("selector.close:OSError", failure.exception.failures)
        self.assertTrue(all(pipe.close.called for pipe in (child.stdin, child.stdout, child.stderr)))

    def test_wait_or_cleanup_completing_late_cannot_return_success(self):
        for late_stage in ("wait", "cleanup"):
            with self.subTest(stage=late_stage):
                clock = [0.0]
                child = Mock()
                for name in ("stdin", "stdout", "stderr"):
                    pipe = Mock()
                    pipe.fileno.return_value = 3
                    setattr(child, name, pipe)
                child.poll.return_value = 0
                child.returncode = 0
                def waited(**kwargs):
                    if late_stage == "wait":
                        clock[0] = 2.0
                    return 0
                child.wait.side_effect = waited
                selector = Mock()
                selector.get_map.return_value = {}
                if late_stage == "cleanup":
                    selector.close.side_effect = lambda: clock.__setitem__(0, 2.0)
                with patch.object(capture.subprocess, "Popen", return_value=child), \
                     patch.object(capture.selectors, "DefaultSelector", return_value=selector), \
                     patch.object(capture.os, "set_blocking"), \
                     patch.object(capture.time, "monotonic", side_effect=lambda: clock[0]):
                    with self.assertRaisesRegex(capture.CaptureError, "deadline"):
                        capture._command(Path("/tmp"), (), b"", 1, 1.0)
                for name in ("stdin", "stdout", "stderr"):
                    self.assertTrue(getattr(child, name).close.called)

    def test_partial_pipe_setup_failure_reaps_owned_real_child(self):
        original = subprocess.Popen
        children = []
        def launch(command, **kwargs):
            child = original([sys.executable, "-I", "-c", "import time;time.sleep(20)"], **kwargs)
            children.append(child)
            return child
        with patch.object(capture.subprocess, "Popen", side_effect=launch), \
             patch.object(capture.os, "set_blocking", side_effect=OSError("partial setup")):
            with self.assertRaises(capture.CaptureError):
                capture._command(Path("/tmp"), (), b"", 1, time.monotonic() + 1)
        self.assertIsNotNone(children[0].poll())
        self.assertTrue(all(pipe.closed for pipe in (children[0].stdin, children[0].stdout, children[0].stderr)))

    def test_interrupted_body_releases_real_child_then_reraises_original_interrupt(self):
        original = subprocess.Popen
        children = []
        interruption = KeyboardInterrupt("body cancelled")
        def launch(command, **kwargs):
            child = original([sys.executable, "-I", "-c", "import time;time.sleep(20)"], **kwargs)
            children.append(child)
            return child
        with patch.object(capture.subprocess, "Popen", side_effect=launch), \
             patch.object(capture.os, "set_blocking", side_effect=interruption):
            with self.assertRaises(KeyboardInterrupt) as failure:
                capture._command(Path("/tmp"), (), b"", 1, time.monotonic() + 1)
        self.assertIs(failure.exception, interruption)
        self.assertIsNotNone(children[0].poll())
        self.assertTrue(all(pipe.closed for pipe in (children[0].stdin, children[0].stdout, children[0].stderr)))

    def test_interrupted_cleanup_finishes_release_then_reraises_first_interrupt(self):
        original_launch = subprocess.Popen
        original_selector = capture.selectors.DefaultSelector
        for interruption in (KeyboardInterrupt("cleanup cancelled"), SystemExit(42), GeneratorExit()):
            with self.subTest(interruption=type(interruption).__name__):
                children = []
                def launch(command, **kwargs):
                    child = original_launch([sys.executable, "-I", "-c", "pass"], **kwargs)
                    children.append(child)
                    return child
                selector = original_selector()
                close = selector.close
                def interrupted_close():
                    close()
                    raise interruption
                with patch.object(capture.subprocess, "Popen", side_effect=launch), \
                     patch.object(capture.selectors, "DefaultSelector", return_value=selector), \
                     patch.object(selector, "close", side_effect=interrupted_close):
                    with self.assertRaises(type(interruption)) as failure:
                        capture._command(Path("/tmp"), (), b"", 1, time.monotonic() + 1)
                self.assertIs(failure.exception, interruption)
                self.assertTrue(any("selector.close" in note for note in failure.exception.__notes__))
                self.assertIsNotNone(children[0].poll())
                self.assertTrue(all(pipe.closed for pipe in (children[0].stdin, children[0].stdout, children[0].stderr)))

    def test_register_failure_reaps_owned_real_child_and_closes_selector(self):
        original = subprocess.Popen
        children = []
        selector = Mock()
        selector.register.side_effect = OSError("register")
        def launch(command, **kwargs):
            child = original([sys.executable, "-I", "-c", "import time;time.sleep(20)"], **kwargs)
            children.append(child)
            return child
        with patch.object(capture.subprocess, "Popen", side_effect=launch), \
             patch.object(capture.selectors, "DefaultSelector", return_value=selector):
            with self.assertRaises(capture.CaptureError):
                capture._command(Path("/tmp"), (), b"x", 1, time.monotonic() + 1)
        self.assertIsNotNone(children[0].poll())
        selector.close.assert_called_once_with()
        self.assertTrue(all(pipe.closed for pipe in (children[0].stdin, children[0].stdout, children[0].stderr)))

    def test_output_stderr_and_deadline_bounds_reap_owned_real_children(self):
        original = subprocess.Popen
        for script, limit, budget in [
            ("import sys,time;sys.stdout.write('x'*100);sys.stdout.flush();time.sleep(20)", 3, 1),
            ("import sys,time;sys.stderr.write('x'*5000);sys.stderr.flush();time.sleep(20)", 3, 1),
            ("import time;time.sleep(20)", 3, 0.03),
        ]:
            with self.subTest(script=script):
                children = []
                def launch(command, **kwargs):
                    child = original([sys.executable, "-I", "-c", script], **kwargs)
                    children.append(child)
                    return child
                with patch.object(capture.subprocess, "Popen", side_effect=launch):
                    with self.assertRaises(capture.CaptureError):
                        capture._command(Path("/tmp"), (), b"", limit, time.monotonic() + budget)
                self.assertIsNotNone(children[0].poll())
                self.assertTrue(all(pipe.closed for pipe in (children[0].stdin, children[0].stdout, children[0].stderr)))


class CandidateGitSourceBatchV1GitTests(unittest.TestCase):
    def test_real_git_exact_legacy_equivalence_binary_unicode_executable_and_path_edges(self):
        files = {"binary.dat": b"\0\xff\x80\r\n", "café/雪.txt": "Café 雪".encode(),
                 "space name.txt": b" a ", "line\nbreak.txt": b"line", "-leading.txt": b"",
                 "tab\tname.txt": b"tab", "run.sh": b"#!/bin/sh\nexit 0\n"}
        with tempfile.TemporaryDirectory() as temporary:
            store, _ = _make(Path(temporary) / "repo", files)
            _run(store.path, "update-index", "--chmod=+x", "run.sh")
            _run(store.path, "commit", "-m", "executable")
            oid = store._git("rev-parse", "HEAD")
            result = capture.capture_git_source_batch(store.path, oid)
            self.assertEqual(result, legacy(store, oid))
            self.assertEqual(result[1], files)

    def test_each_capture_launches_four_git_processes_and_rechecks_new_bytes(self):
        with tempfile.TemporaryDirectory() as temporary:
            store, oid = _make(Path(temporary) / "repo", {f"f{i}.txt": str(i).encode() for i in range(61)})
            original = capture.subprocess.Popen
            with patch.object(capture.subprocess, "Popen", wraps=original) as launch:
                result = capture.capture_git_source_batch(store.path, oid)
            self.assertEqual(launch.call_count, 4)
            self.assertEqual(len(result[1]), 61)
            (store.path / "new.txt").write_bytes(b"new")
            _run(store.path, "add", "--all")
            _run(store.path, "commit", "-m", "next")
            new = store._git("rev-parse", "HEAD")
            self.assertIn("new.txt", capture.capture_git_source_batch(store.path, new)[1])
            self.assertNotIn("new.txt", capture.capture_git_source_batch(store.path, oid)[1])

    def test_oversized_blob_is_rejected_before_content_batch(self):
        with tempfile.TemporaryDirectory() as temporary:
            store, oid = _make(Path(temporary) / "repo", {"large.bin": b"x" * (capture.MAX_FILE_BYTES + 1)})
            with patch.object(capture, "_command", wraps=capture._command) as command:
                with self.assertRaises(capture.CaptureError):
                    capture.capture_git_source_batch(store.path, oid)
            self.assertEqual(command.call_count, 3)

    def test_missing_blob_and_wrong_commit_type_fail(self):
        with tempfile.TemporaryDirectory() as temporary:
            store, oid = _make(Path(temporary) / "repo", {"a.txt": b"a"})
            tree = store._git("rev-parse", oid + "^{tree}")
            with self.assertRaises(capture.CaptureError):
                capture.capture_git_source_batch(store.path, tree)
            blob = store._git("rev-parse", oid + ":a.txt")
            (store.path / ".git/objects" / blob[:2] / blob[2:]).unlink()
            with self.assertRaises(capture.CaptureError):
                capture.capture_git_source_batch(store.path, oid)

    def test_private_symlink_and_gitlink_paths_fail_before_content_read(self):
        for kind in ("private", "symlink", "gitlink"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as temporary:
                store, oid = _make(Path(temporary) / "repo", {"a.txt": b"a"})
                if kind == "private":
                    (store.path / ".private").write_bytes(b"x")
                    _run(store.path, "add", "--all")
                elif kind == "symlink":
                    (store.path / "link").symlink_to("a.txt")
                    _run(store.path, "add", "--all")
                else:
                    _run(store.path, "update-index", "--add", "--cacheinfo", "160000," + oid + ",nested")
                _run(store.path, "commit", "-m", "invalid")
                new = store._git("rev-parse", "HEAD")
                with patch.object(capture, "_command", wraps=capture._command) as command:
                    with self.assertRaises(capture.CaptureError):
                        capture.capture_git_source_batch(store.path, new)
                self.assertEqual(command.call_count, 2)

    def test_inherited_git_redirects_and_replace_objects_cannot_change_capture(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            store, oid = _make(root / "repo", {"a.txt": b"original"})
            _, other_oid = _make(root / "other", {"a.txt": b"other"})
            original_blob = store._git("rev-parse", oid + ":a.txt")
            (store.path / "a.txt").write_bytes(b"replacement")
            _run(store.path, "add", "--all")
            _run(store.path, "commit", "-m", "replace")
            replacement = store._git("rev-parse", "HEAD:a.txt")
            _run(store.path, "replace", original_blob, replacement)
            env = {"GIT_DIR": str(root / "other/.git"), "GIT_WORK_TREE": str(root / "other"),
                   "GIT_CONFIG_COUNT": "1", "GIT_CONFIG_KEY_0": "core.hooksPath", "GIT_CONFIG_VALUE_0": str(root)}
            with patch.dict(os.environ, env):
                self.assertEqual(capture.capture_git_source_batch(store.path, oid)[1], {"a.txt": b"original"})
            self.assertNotEqual(other_oid, oid)


if __name__ == "__main__":
    unittest.main()
