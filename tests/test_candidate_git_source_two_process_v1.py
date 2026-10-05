"""Draft qualification source; authoring this file does not execute any checks.

The offline class exercises the public helper with inert pipes and a selector.
The Git class is intended for the separately selected real-Git verification lane.
Neither class imports candidate code or creates an Engine/provider execution.
"""
from contextlib import ExitStack, contextmanager
import hashlib
import os
from pathlib import Path
import selectors
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from gossip_harness import candidate_git_source_batch_v1 as legacy
from gossip_harness import candidate_git_source_two_process_v1 as capture
from gossip_harness import candidate_source_capture_policy_v1 as legacy_policy
from gossip_harness import candidate_source_capture_policy_v2 as policy_module


COMMIT = "a" * 40
TREE = "b" * 40


def _blob_oid(raw):
    return hashlib.sha1(b"blob " + str(len(raw)).encode() + b"\0" + raw).hexdigest().encode()


def _fixture(files=None):
    if files is None:
        files = {"a.txt": b"x", "b.dat": b"\0\xff"}
    listing = bytearray()
    info = bytearray(COMMIT.encode() + b" commit 123\n" + TREE.encode() + b" tree 66\n")
    contents = bytearray()
    entries = []
    for name, raw in files.items():
        oid = _blob_oid(raw)
        header = oid + b" blob " + str(len(raw)).encode() + b"\n"
        listing.extend(b"100644 blob " + oid + b"\t" + name.encode() + b"\0")
        info.extend(header)
        contents.extend(header + raw + b"\n")
        entries.append((name, oid))
    return bytes(listing), bytes(info), bytes(contents), entries


class _Pipe:
    def __init__(self, transport, name, fd):
        self.transport = transport
        self.name = name
        self.fd = fd
        self.closed = False
        self.close_count = 0

    def fileno(self):
        return self.fd

    def close(self):
        self.close_count += 1
        self.closed = True
        if self.name in self.transport.close_failures:
            raise OSError("injected " + self.name + " close")


class _Selector:
    def __init__(self, transport):
        self.transport = transport
        self.keys = {}
        self.close_count = 0

    def register(self, fileobj, events, data=None):
        fd = fileobj if isinstance(fileobj, int) else fileobj.fileno()
        if fd in self.keys:
            raise KeyError(fd)
        key = SimpleNamespace(fileobj=fileobj, fd=fd, events=events, data=data)
        self.keys[fd] = key
        return key

    def unregister(self, fileobj):
        fd = fileobj if isinstance(fileobj, int) else fileobj.fileno()
        return self.keys.pop(fd)

    def modify(self, fileobj, events, data=None):
        self.unregister(fileobj)
        return self.register(fileobj, events, data)

    def get_map(self):
        return self.keys

    def select(self, timeout=None):
        self.transport.select_timeouts.append(timeout)
        if self.transport.select_failure is not None:
            raise self.transport.select_failure
        result = []
        for key in list(self.keys.values()):
            if key.fd == 10 and key.events & selectors.EVENT_WRITE:
                result.append((key, selectors.EVENT_WRITE))
            elif key.fd in (11, 12) and key.events & selectors.EVENT_READ:
                if (self.transport.buffers[key.fd] or self.transport.stdin.closed
                        or (key.fd == self.transport.early_eof_fd and self.transport.flush_count == 1)):
                    result.append((key, selectors.EVENT_READ))
        return result

    def close(self):
        self.close_count += 1
        if self.transport.on_selector_close is not None:
            self.transport.on_selector_close()
        if "selector" in self.transport.close_failures:
            raise OSError("injected selector close")
        self.keys.clear()


class _Transport:
    """No OS pipe or process: replies become readable only after a flush write."""

    def __init__(self, info, contents, *, read_width=65536, write_width=65536,
                 stderr=b"", exit_code=0, close_failures=(), select_failure=None, early_eof_fd=None):
        self.responses = [info, contents]
        self.buffers = {11: bytearray(), 12: bytearray(stderr)}
        self.read_width = read_width
        self.write_width = write_width
        self.exit_code = exit_code
        self.close_failures = close_failures
        self.select_failure = select_failure
        self.early_eof_fd = early_eof_fd
        self.early_eof_observations = []
        self.stdin = _Pipe(self, "stdin", 10)
        self.stdout = _Pipe(self, "stdout", 11)
        self.stderr = _Pipe(self, "stderr", 12)
        self.selector = _Selector(self)
        self.pending = bytearray()
        self.commands = []
        self.flush_count = 0
        self.launches = []
        self.select_timeouts = []
        self.wait_timeouts = []
        self.kill_count = 0
        self.returncode = None
        self.on_wait = None
        self.on_selector_close = None
        self.info_bytes_read = 0
        self.content_dispatched_after_info_bytes = None
        self.read_failure = None

    def launch(self, argv, **kwargs):
        self.launches.append((argv, kwargs))
        return self

    def write(self, fd, raw):
        if fd != 10 or self.stdin.closed:
            raise AssertionError("write must target the live input pipe")
        accepted = bytes(raw[:self.write_width])
        self.pending.extend(accepted)
        while b"\n" in self.pending:
            line, _, remaining = self.pending.partition(b"\n")
            self.pending[:] = remaining
            self.commands.append(bytes(line))
            if line.startswith(b"contents ") and self.content_dispatched_after_info_bytes is None:
                self.content_dispatched_after_info_bytes = self.info_bytes_read
            if line == b"flush":
                if self.flush_count >= len(self.responses):
                    raise AssertionError("unexpected extra batch flush")
                self.buffers[11].extend(self.responses[self.flush_count])
                self.flush_count += 1
        return len(accepted)

    def read(self, fd, size):
        if self.read_failure is not None:
            failure, self.read_failure = self.read_failure, None
            raise failure
        if fd not in self.buffers:
            raise AssertionError("read must target an output pipe")
        size = min(size, self.read_width)
        raw = bytes(self.buffers[fd][:size])
        del self.buffers[fd][:size]
        if not raw and fd == self.early_eof_fd and self.flush_count == 1:
            self.early_eof_observations.append((fd, self.stdin.closed, self.poll()))
        if fd == 11 and self.flush_count == 1:
            self.info_bytes_read += len(raw)
        return raw

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        self.wait_timeouts.append(timeout)
        if self.on_wait is not None:
            self.on_wait()
        self.returncode = self.exit_code if self.returncode is None else self.returncode
        return self.returncode

    def kill(self):
        self.kill_count += 1
        self.returncode = -9


@contextmanager
def _mock_capture(listing, info, contents, **options):
    transport = _Transport(info, contents, **options)
    with ExitStack() as stack:
        command = stack.enter_context(patch.object(legacy, "_command", return_value=listing))
        stack.enter_context(patch.object(capture.subprocess, "Popen", side_effect=transport.launch))
        stack.enter_context(patch.object(capture.selectors, "DefaultSelector", return_value=transport.selector))
        stack.enter_context(patch.object(capture.os, "set_blocking"))
        stack.enter_context(patch.object(capture.os, "write", side_effect=transport.write))
        stack.enter_context(patch.object(capture.os, "read", side_effect=transport.read))
        yield transport, command


class CandidateGitSourceTwoProcessV1Tests(unittest.TestCase):
    def test_protocol_bounds_and_error_classes_preserve_legacy_contract(self):
        self.assertEqual(capture.PROTOCOL, "candidate-git-source-two-process-v1")
        self.assertIs(capture.CaptureError, legacy.CaptureError)
        self.assertIs(capture.CaptureCleanupError, legacy.CaptureCleanupError)
        for name in ("MAX_FILES", "MAX_FILE_BYTES", "MAX_SOURCE_BYTES", "MAX_TREE_BYTES",
                     "MAX_STDERR_BYTES", "CLEANUP_REAP_SECONDS"):
            self.assertEqual(getattr(capture, name), getattr(legacy, name))

    def test_public_capture_uses_one_listing_and_one_interactive_child(self):
        files = {"a.txt": b"x", "b.dat": b"\0\xff"}
        listing, info, contents, entries = _fixture(files)
        with _mock_capture(listing, info, contents) as (transport, command):
            result = capture.capture_git_source_two_process(Path("/inert/repo"), COMMIT)
        self.assertEqual(result, (TREE, files))
        command.assert_called_once()
        self.assertEqual(command.call_args.args[:4],
                         (Path("/inert/repo"), ("ls-tree", "-r", "-z", COMMIT), b"", legacy.MAX_TREE_BYTES))
        self.assertEqual(len(transport.launches), 1)
        self.assertEqual(transport.launches[0][0][-3:], ["cat-file", "--batch-command", "--buffer"])
        self.assertEqual(transport.commands,
                         [b"info " + COMMIT.encode(), b"info " + COMMIT.encode() + b"^{tree}"]
                         + [b"info " + oid for _, oid in entries] + [b"flush"]
                         + [b"contents " + oid for _, oid in entries] + [b"flush"])
        self.assertEqual(transport.content_dispatched_after_info_bytes, len(info))
        self.assertEqual(transport.kill_count, 0)
        self.assertTrue(all(pipe.closed for pipe in (transport.stdin, transport.stdout, transport.stderr)))

    def test_safe_git_arguments_and_scrubbed_environment_reach_child(self):
        listing, info, contents, _ = _fixture()
        hostile = {"GIT_DIR": "/untrusted", "GIT_CONFIG_COUNT": "1", "GIT_TRACE": "/untrusted/trace"}
        with patch.dict(os.environ, hostile), _mock_capture(listing, info, contents) as (transport, _):
            capture.capture_git_source_two_process(Path("/inert/repo"), COMMIT)
        argv, kwargs = transport.launches[0]
        self.assertEqual(argv[:-3], ["git", "--literal-pathspecs", "-c", "core.hooksPath=" + os.devnull,
                                    "-c", "core.fsmonitor=false", "-c", "protocol.allow=never",
                                    "-c", "protocol.file.allow=always", "-C", "/inert/repo"])
        env = kwargs["env"]
        self.assertFalse(set(hostile) & set(env))
        for key, value in {"GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull,
                           "GIT_TERMINAL_PROMPT": "0", "GIT_NO_REPLACE_OBJECTS": "1",
                           "GIT_NO_LAZY_FETCH": "1", "GIT_ALLOW_PROTOCOL": "file", "LC_ALL": "C"}.items():
            self.assertEqual(env[key], value)

    def test_partial_reads_and_writes_preserve_binary_frames(self):
        files = {"a\nname": b"\0\n\xff", "tab\tname": b"", "same.dat": b"\0\n\xff"}
        listing, info, contents, _ = _fixture(files)
        with _mock_capture(listing, info, contents, read_width=1, write_width=2):
            self.assertEqual(capture.capture_git_source_two_process(Path("/inert"), COMMIT), (TREE, files))

    def test_invalid_commit_and_deadline_reject_before_either_child(self):
        invalid = [("HEAD", 60), ("a" * 64, 60), ("A" * 40, 60), (COMMIT + "\n", 60),
                   (COMMIT, True), (COMMIT, 0), (COMMIT, 121), (COMMIT, 1.0)]
        with patch.object(legacy, "_command") as command, patch.object(capture.subprocess, "Popen") as launch:
            for oid, seconds in invalid:
                with self.subTest(oid=oid, seconds=seconds), self.assertRaises(capture.CaptureError):
                    capture.capture_git_source_two_process(Path("/inert"), oid, timeout_seconds=seconds)
        command.assert_not_called()
        launch.assert_not_called()

    def test_bad_complete_tree_rejects_before_interactive_child(self):
        listing, info, contents, _ = _fixture({"a.txt": b"x"})
        changes = [b"", listing[:-1], listing + listing, listing + b"\0",
                   listing.replace(b"100644", b"120000"), listing.replace(b"100644", b"160000"),
                   listing.replace(b" blob ", b" tree "), listing.replace(b"a.txt", b"../a.txt"),
                   listing.replace(b"a.txt", b".private"), listing.replace(b"a.txt", b"a/.private"),
                   listing.replace(b"a.txt", b"/a.txt"), listing.replace(b"a.txt", b"a\\b"),
                   listing.replace(b"a.txt", b"a//b"), listing.replace(b"a.txt", b"\xff"),
                   listing.replace(_blob_oid(b"x"), b"c" * 64)]
        for index, raw in enumerate(changes):
            with self.subTest(case=index), _mock_capture(raw, info, contents) as (transport, command):
                with self.assertRaises(capture.CaptureError):
                    capture.capture_git_source_two_process(Path("/inert"), COMMIT)
                command.assert_called_once()
                self.assertEqual(transport.launches, [])

    def test_file_count_limit_plus_one_precedes_interactive_dispatch(self):
        listing, info, contents, _ = _fixture({f"f{i}": b"x" for i in range(capture.MAX_FILES + 1)})
        with _mock_capture(listing, info, contents) as (transport, _):
            with self.assertRaises(capture.CaptureError):
                capture.capture_git_source_two_process(Path("/inert"), COMMIT)
        self.assertEqual(transport.launches, [])

    def test_identity_and_info_framing_fail_before_any_contents_command(self):
        listing, info, contents, _ = _fixture()
        rows = info.splitlines(keepends=True)
        changes = [info[:-1], info.replace(b"\n", b"\r\n"), info + rows[-1],
                   b"".join(rows[:-1]), rows[1] + rows[0] + b"".join(rows[2:]),
                   info.replace(COMMIT.encode(), b"c" * 40, 1),
                   info.replace(b" commit ", b" tag ", 1), info.replace(b" tree ", b" blob ", 1),
                   info.replace(TREE.encode(), b"d" * 64, 1),
                   b"".join(rows[:2]) + rows[3] + rows[2],
                   info.replace(b" blob 1\n", b" missing\n", 1),
                   info.replace(b" blob 1\n", b" tree 1\n", 1),
                   info.replace(b" blob 1\n", b" blob 01\n", 1),
                   info.replace(b" blob 1\n", b" blob +1\n", 1),
                   info.replace(b" blob 1\n", b" blob -1\n", 1),
                   info.replace(b" blob 1\n", b" blob " + b"9" * 5000 + b"\n", 1)]
        for index, raw in enumerate(changes):
            with self.subTest(case=index), _mock_capture(listing, raw, contents) as (transport, _):
                with self.assertRaises(capture.CaptureError):
                    capture.capture_git_source_two_process(Path("/inert"), COMMIT)
                self.assertFalse(any(command.startswith(b"contents ") for command in transport.commands))

    def test_blob_and_aggregate_limits_precede_any_contents_command(self):
        fixtures = [({"large": b"x"}, b" blob 2097153\n"),
                    ({f"f{i}": b"x" for i in range(9)}, b" blob 2097152\n")]
        for files, replacement in fixtures:
            listing, info, contents, _ = _fixture(files)
            info = info.replace(b" blob 1\n", replacement)
            with self.subTest(files=len(files)), _mock_capture(listing, info, contents) as (transport, _):
                with self.assertRaises(capture.CaptureError):
                    capture.capture_git_source_two_process(Path("/inert"), COMMIT)
                self.assertFalse(any(command.startswith(b"contents ") for command in transport.commands))

    def test_exact_file_blob_and_aggregate_limits_are_admitted(self):
        fixtures = [{f"f{i}": b"x" for i in range(capture.MAX_FILES)},
                    {"exact": b"x" * capture.MAX_FILE_BYTES},
                    {f"f{i}": b"x" * capture.MAX_FILE_BYTES for i in range(8)}]
        for files in fixtures:
            listing, info, contents, _ = _fixture(files)
            with self.subTest(files=len(files)), _mock_capture(listing, info, contents):
                self.assertEqual(capture.capture_git_source_two_process(Path("/inert"), COMMIT), (TREE, files))

    def test_aggregate_limit_plus_one_rejects_before_contents(self):
        listing, info, contents, _ = _fixture({**{f"f{i}": b"x" for i in range(8)}, "extra": b"y"})
        rows = info.splitlines(keepends=True)
        info = b"".join(rows[:2] + [row.replace(b" blob 1\n", b" blob 2097152\n") for row in rows[2:10]] + rows[10:])
        with _mock_capture(listing, info, contents) as (transport, _):
            with self.assertRaises(capture.CaptureError):
                capture.capture_git_source_two_process(Path("/inert"), COMMIT)
        self.assertFalse(any(command.startswith(b"contents ") for command in transport.commands))

    def test_large_commit_info_never_requests_commit_or_tree_contents(self):
        listing, info, contents, entries = _fixture()
        info = info.replace(b" commit 123\n", b" commit 9000000\n")
        with _mock_capture(listing, info, contents) as (transport, _):
            capture.capture_git_source_two_process(Path("/inert"), COMMIT)
        self.assertEqual([line for line in transport.commands if line.startswith(b"contents ")],
                         [b"contents " + oid for _, oid in entries])

    def test_content_header_hash_length_newline_and_suffix_must_match(self):
        listing, info, contents, _ = _fixture()
        changes = [contents[:-1], contents + b"suffix", contents.replace(b"\nx\n", b"\ny\n"),
                   contents.replace(b" blob 1\n", b" blob 2\n", 1),
                   contents.replace(b" blob 1\n", b" blob 1\r\n", 1), contents[1:],
                   contents.replace(b"\nx\n", b"\nx"), contents[:-3]]
        for index, raw in enumerate(changes):
            with self.subTest(case=index), _mock_capture(listing, info, raw):
                with self.assertRaises(capture.CaptureError):
                    capture.capture_git_source_two_process(Path("/inert"), COMMIT)

    def test_duplicate_blob_oid_still_returns_each_path(self):
        files = {"first": b"same", "second": b"same"}
        listing, info, contents, entries = _fixture(files)
        with _mock_capture(listing, info, contents) as (transport, _):
            self.assertEqual(capture.capture_git_source_two_process(Path("/inert"), COMMIT), (TREE, files))
        self.assertEqual(transport.commands.count(b"contents " + entries[0][1]), 2)

    def test_nonzero_exit_after_complete_output_is_not_success(self):
        listing, info, contents, _ = _fixture()
        with _mock_capture(listing, info, contents, exit_code=1):
            with self.assertRaises(capture.CaptureError):
                capture.capture_git_source_two_process(Path("/inert"), COMMIT)

    def test_early_stdout_or_stderr_eof_rejects_info_phase_and_releases_child(self):
        listing, info, contents, _ = _fixture()
        for fd in (11, 12):
            with self.subTest(channel="stdout" if fd == 11 else "stderr"), \
                    _mock_capture(listing, info[:-1], contents, early_eof_fd=fd) as (transport, _):
                with self.assertRaisesRegex(capture.CaptureError, "closed stream before final"):
                    capture.capture_git_source_two_process(Path("/inert"), COMMIT)
            self.assertEqual(transport.early_eof_observations, [(fd, False, None)])
            self.assertFalse(any(command.startswith(b"contents ") for command in transport.commands))
            self.assertEqual(transport.kill_count, 1)
            self.assertEqual(len(transport.wait_timeouts), 1)
            self.assertLessEqual(transport.wait_timeouts[0], capture.CLEANUP_REAP_SECONDS)
            self.assertEqual(transport.selector.close_count, 1)
            self.assertEqual(transport.selector.get_map(), {})
            self.assertTrue(all(pipe.closed and pipe.close_count == 1
                                for pipe in (transport.stdin, transport.stdout, transport.stderr)))

    def test_stderr_bound_failure_reaps_child_and_closes_every_pipe(self):
        listing, info, contents, _ = _fixture()
        with _mock_capture(listing, info, contents, stderr=b"e" * (capture.MAX_STDERR_BYTES + 1)) as (transport, _):
            with self.assertRaises(capture.CaptureError):
                capture.capture_git_source_two_process(Path("/inert"), COMMIT)
        self.assertEqual(transport.kill_count, 1)
        self.assertTrue(transport.wait_timeouts)
        self.assertTrue(all(pipe.closed for pipe in (transport.stdin, transport.stdout, transport.stderr)))

    def test_interruption_releases_owned_resources_and_preserves_exception(self):
        listing, info, contents, _ = _fixture()
        interruption = KeyboardInterrupt("injected")
        with _mock_capture(listing, info, contents, select_failure=interruption) as (transport, _):
            with self.assertRaises(KeyboardInterrupt) as raised:
                capture.capture_git_source_two_process(Path("/inert"), COMMIT)
        self.assertIs(raised.exception, interruption)
        self.assertEqual(transport.kill_count, 1)
        self.assertTrue(all(pipe.closed for pipe in (transport.stdin, transport.stdout, transport.stderr)))

    def test_cleanup_failure_attempts_other_releases_and_cannot_certify_success(self):
        listing, info, contents, _ = _fixture()
        with _mock_capture(listing, info, contents, close_failures=("selector", "stdout")) as (transport, _):
            with self.assertRaises(capture.CaptureCleanupError) as raised:
                capture.capture_git_source_two_process(Path("/inert"), COMMIT)
        self.assertIn("selector.close:OSError", raised.exception.failures)
        self.assertIn("stdout.close:OSError", raised.exception.failures)
        self.assertTrue(all(pipe.closed for pipe in (transport.stdin, transport.stdout, transport.stderr)))
        self.assertLessEqual(transport.wait_timeouts[-1], capture.CLEANUP_REAP_SECONDS)

    def test_late_wait_cleanup_or_hash_cannot_return_success(self):
        listing, info, contents, _ = _fixture()
        original_sha1 = hashlib.sha1
        for stage in ("wait", "cleanup", "hash"):
            clock = [0.0]
            def late():
                clock[0] = 2.0
            def late_hash(raw):
                result = original_sha1(raw)
                late()
                return result
            with self.subTest(stage=stage), _mock_capture(listing, info, contents) as (transport, _), \
                    patch.object(capture.time, "monotonic", side_effect=lambda: clock[0]):
                if stage == "wait":
                    transport.on_wait = late
                elif stage == "cleanup":
                    transport.on_selector_close = late
                with ExitStack() as stack:
                    if stage == "hash":
                        stack.enter_context(patch.object(capture.hashlib, "sha1", side_effect=late_hash))
                    with self.assertRaisesRegex(capture.CaptureError, "deadline"):
                        capture.capture_git_source_two_process(Path("/inert"), COMMIT, timeout_seconds=1)

    def test_listing_and_interactive_stage_share_one_absolute_deadline(self):
        listing, info, contents, _ = _fixture()
        clock = [0.0]
        def listed(*args):
            self.assertEqual(args[-1], 60.0)
            clock[0] = 59.5
            return listing
        with _mock_capture(listing, info, contents) as (transport, command), \
                patch.object(capture.time, "monotonic", side_effect=lambda: clock[0]):
            command.side_effect = listed
            capture.capture_git_source_two_process(Path("/inert"), COMMIT)
        self.assertTrue(transport.select_timeouts)
        self.assertTrue(all(0 < timeout <= 0.5 for timeout in transport.select_timeouts))

    def test_selector_failure_precedes_spawn_and_spawn_failure_closes_selector(self):
        listing, info, contents, _ = _fixture()
        with _mock_capture(listing, info, contents) as (transport, _), \
                patch.object(capture.selectors, "DefaultSelector", side_effect=OSError("allocate")):
            with self.assertRaises(capture.CaptureError):
                capture.capture_git_source_two_process(Path("/inert"), COMMIT)
        self.assertEqual(transport.launches, [])
        with _mock_capture(listing, info, contents) as (transport, _), \
                patch.object(capture.subprocess, "Popen", side_effect=OSError("spawn")):
            with self.assertRaises(capture.CaptureError):
                capture.capture_git_source_two_process(Path("/inert"), COMMIT)
        self.assertEqual(transport.selector.close_count, 1)


def _git(path, *args, input_data=None):
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull,
               GIT_TERMINAL_PROMPT="0", GIT_NO_REPLACE_OBJECTS="1", LC_ALL="C")
    return subprocess.run(["git", "--literal-pathspecs", "-c", "user.name=Capture Test",
                           "-c", "user.email=capture-test@example.invalid", "-c", "commit.gpgSign=false",
                           "-c", "core.hooksPath=" + os.devnull, "-C", str(path), *args],
                          input=input_data, capture_output=True, check=True, env=env, timeout=60)


def _make_repo(root, files, *, object_format="sha1"):
    root.mkdir()
    _git(root, "init", "--initial-branch=main", "--object-format=" + object_format)
    for name, raw in files.items():
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(raw)
    _git(root, "add", "--all")
    _git(root, "commit", "-m", "fixture")
    return _git(root, "rev-parse", "HEAD").stdout.decode().strip()


class CandidateSourceCapturePolicyV2Tests(unittest.TestCase):
    def test_exact_fixed_policy_record_identifies_phase_order_and_limits(self):
        record = policy_module.TwoProcessCapturePolicy().record()
        self.assertEqual(record["protocol"], "candidate-source-capture-policy-v2")
        self.assertEqual(record["capture_protocol"], capture.PROTOCOL)
        self.assertEqual(record["timeout_seconds"], 60)
        self.assertEqual(record["cleanup_reap_seconds"], 5)
        self.assertEqual(record["children_per_capture"], 2)
        self.assertIs(record["cross_boundary_cache"], False)
        self.assertEqual(record["phase_order"], ["ls-tree", "info-all", "contents-all", "EOF-exit-cleanup"])
        self.assertEqual(record["max_files"], 511)
        self.assertEqual(record["max_file_bytes"], 2 * 1024 * 1024)
        self.assertEqual(record["max_source_bytes"], 16 * 1024 * 1024)
        for seconds, reap in ((1, 5), (60, 4), (60.0, 5), (60, 5.0), (True, 5)):
            with self.subTest(seconds=seconds, reap=reap), self.assertRaises(ValueError):
                policy_module.TwoProcessCapturePolicy(timeout_seconds=seconds, cleanup_reap_seconds=reap)

    def test_required_explicit_policy_rejects_none_foreign_and_subclass_before_dispatch(self):
        store = SimpleNamespace(path=Path("/inert"))
        class DerivedPolicy(policy_module.TwoProcessCapturePolicy):
            pass
        foreign = [None, object(), legacy_policy.BatchCapturePolicy(), object.__new__(DerivedPolicy)]
        with patch.object(capture, "capture_git_source_two_process") as dispatch:
            with self.assertRaises(TypeError):
                policy_module.capture_registered_source(store, COMMIT)
            for policy in foreign:
                with self.subTest(kind=type(policy).__name__), self.assertRaises(ValueError):
                    policy_module.capture_registered_source(store, COMMIT, policy=policy)
        dispatch.assert_not_called()

    def test_evaluator_sources_bind_policy_and_both_helpers(self):
        sources = policy_module.evaluator_sources()
        expected = {"candidate_source_capture_policy_v2.py", "candidate_git_source_two_process_v1.py",
                    "candidate_git_source_batch_v1.py"}
        self.assertEqual(set(sources), {"gossip_harness/" + name for name in expected})
        root = Path(policy_module.__file__).resolve().parent
        for name in expected:
            self.assertEqual(sources["gossip_harness/" + name], hashlib.sha256((root / name).read_bytes()).hexdigest())

    def test_changed_limits_or_types_refuse_before_helper_dispatch(self):
        policy = policy_module.TwoProcessCapturePolicy()
        changes = [(capture, "MAX_FILES", 512), (capture, "MAX_FILE_BYTES", 2097153),
                   (capture, "MAX_SOURCE_BYTES", 16777217), (capture, "MAX_TREE_BYTES", 613201),
                   (capture, "MAX_STDERR_BYTES", 4097), (capture, "MAX_HEADER_BYTES", 129),
                   (capture, "MAX_INFO_BYTES", 65665), (capture, "MAX_REQUEST_BYTES", 32845),
                   (capture, "MAX_CONTENT_BYTES", 16842625), (capture, "CLEANUP_REAP_SECONDS", 5),
                   (legacy, "MAX_FILES", 512), (legacy, "MAX_TREE_BYTES", 613201)]
        with patch.object(capture, "capture_git_source_two_process") as dispatch:
            for module, name, value in changes:
                with self.subTest(module=module.__name__, name=name), patch.object(module, name, value):
                    with self.assertRaises(policy_module.SourceCaptureUnavailable):
                        policy_module.capture_registered_source(SimpleNamespace(path=Path("/inert")), COMMIT, policy=policy)
        dispatch.assert_not_called()

    def test_changed_loaded_source_refuses_before_helper_dispatch(self):
        policy = policy_module.TwoProcessCapturePolicy()
        original_read = Path.read_bytes
        for changed in ("candidate_source_capture_policy_v2.py", "candidate_git_source_two_process_v1.py",
                        "candidate_git_source_batch_v1.py"):
            def changed_bytes(path):
                raw = original_read(path)
                return raw + b"\n# drift\n" if path.name == changed else raw
            with self.subTest(file=changed), patch.object(Path, "read_bytes", autospec=True, side_effect=changed_bytes), \
                    patch.object(capture, "capture_git_source_two_process") as dispatch:
                with self.assertRaises(policy_module.SourceCaptureUnavailable):
                    policy_module.capture_registered_source(SimpleNamespace(path=Path("/inert")), COMMIT, policy=policy)
                dispatch.assert_not_called()

    def test_explicit_policy_dispatches_freshly_with_fixed_timeout(self):
        store = SimpleNamespace(path=Path("/inert"))
        policy = policy_module.TwoProcessCapturePolicy()
        with patch.object(capture, "capture_git_source_two_process", side_effect=[(TREE, {"a": b"first"}),
                                                                                (TREE, {"a": b"second"})]) as dispatch:
            first = policy_module.capture_registered_source(store, COMMIT, policy=policy)
            second = policy_module.capture_registered_source(store, COMMIT, policy=policy)
        self.assertEqual(first[1], {"a": b"first"})
        self.assertEqual(second[1], {"a": b"second"})
        self.assertEqual(dispatch.call_count, 2)
        for call in dispatch.call_args_list:
            self.assertEqual(call.args, (store.path, COMMIT))
            self.assertEqual(call.kwargs, {"timeout_seconds": 60})

    def test_malformed_metadata_and_cleanup_uncertainty_remain_unavailable(self):
        policy = policy_module.TwoProcessCapturePolicy()
        store = SimpleNamespace(path=Path("/inert"))
        listing, info, contents, _ = _fixture()
        for replacement in (b" blob 2097153\n", b" tree 1\n", b" missing\n"):
            raw = info.replace(b" blob 1\n", replacement, 1)
            with self.subTest(replacement=replacement), _mock_capture(listing, raw, contents) as (transport, _):
                with self.assertRaises(policy_module.SourceCaptureUnavailable) as raised:
                    policy_module.capture_registered_source(store, COMMIT, policy=policy)
                self.assertIsInstance(raised.exception.__cause__, capture.CaptureError)
                self.assertFalse(any(command.startswith(b"contents ") for command in transport.commands))
        failure = capture.CaptureCleanupError(("process.wait:TimeoutExpired",))
        with patch.object(capture, "capture_git_source_two_process", side_effect=failure):
            with self.assertRaises(policy_module.SourceCaptureUnavailable) as raised:
                policy_module.capture_registered_source(store, COMMIT, policy=policy)
        self.assertIs(raised.exception.__cause__, failure)


class CandidateGitSourceTwoProcessV1GitTests(unittest.TestCase):
    def test_real_git_matches_legacy_binary_unicode_executable_and_path_edges(self):
        files = {"binary.dat": b"\0\xff\x80\r\n", "café/雪.txt": "Café 雪".encode(),
                 "space name.txt": b" a ", "line\nbreak.txt": b"line", "-leading.txt": b"",
                 "tab\tname.txt": b"tab", "run.sh": b"#!/bin/sh\nexit 0\n"}
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "repo"
            _make_repo(root, files)
            _git(root, "update-index", "--chmod=+x", "run.sh")
            _git(root, "commit", "-m", "executable")
            oid = _git(root, "rev-parse", "HEAD").stdout.decode().strip()
            result = capture.capture_git_source_two_process(root, oid)
            self.assertEqual(result, legacy.capture_git_source_batch(root, oid))
            self.assertEqual(result[1], files)

    def test_each_capture_launches_exactly_two_git_children_and_rechecks_source(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "repo"
            old = _make_repo(root, {f"f{i}": str(i).encode() for i in range(61)})
            original_popen = subprocess.Popen
            with patch.object(capture.subprocess, "Popen", wraps=original_popen) as launched:
                result = capture.capture_git_source_two_process(root, old)
            self.assertEqual(launched.call_count, 2)
            self.assertEqual(launched.call_args_list[0].args[0][-4:], ["ls-tree", "-r", "-z", old])
            self.assertEqual(launched.call_args_list[1].args[0][-3:], ["cat-file", "--batch-command", "--buffer"])
            self.assertEqual(len(result[1]), 61)
            (root / "new.txt").write_bytes(b"new")
            _git(root, "add", "--all")
            _git(root, "commit", "-m", "next")
            new = _git(root, "rev-parse", "HEAD").stdout.decode().strip()
            self.assertIn("new.txt", capture.capture_git_source_two_process(root, new)[1])
            self.assertNotIn("new.txt", capture.capture_git_source_two_process(root, old)[1])

    def test_large_commit_message_does_not_consume_blob_content_budget(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "repo"
            old = _make_repo(root, {"a.txt": b"a"})
            tree = _git(root, "rev-parse", old + "^{tree}").stdout.decode().strip()
            oid = _git(root, "commit-tree", tree, "-p", old,
                       input_data=b"m" * (capture.MAX_FILE_BYTES + 128)).stdout.decode().strip()
            self.assertEqual(capture.capture_git_source_two_process(root, oid), (tree, {"a.txt": b"a"}))

    def test_empty_subtree_preserves_root_tree_identity(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "repo"
            old = _make_repo(root, {"a.txt": b"a"})
            blob = _git(root, "rev-parse", old + ":a.txt").stdout.strip()
            empty = _git(root, "mktree", input_data=b"").stdout.strip()
            tree = _git(root, "mktree", input_data=b"100644 blob " + blob + b"\ta.txt\n"
                        + b"040000 tree " + empty + b"\tempty\n").stdout.decode().strip()
            oid = _git(root, "commit-tree", tree, "-p", old, input_data=b"empty subtree\n").stdout.decode().strip()
            self.assertEqual(capture.capture_git_source_two_process(root, oid), (tree, {"a.txt": b"a"}))

    def test_annotated_tag_tree_blob_and_empty_commit_are_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "repo"
            commit = _make_repo(root, {"a.txt": b"a"})
            _git(root, "tag", "-a", "-m", "tag", "fixture-tag", commit)
            empty_tree = _git(root, "mktree", input_data=b"").stdout.decode().strip()
            empty_commit = _git(root, "commit-tree", empty_tree, input_data=b"empty\n").stdout.decode().strip()
            objects = [_git(root, "rev-parse", expression).stdout.decode().strip()
                       for expression in ("refs/tags/fixture-tag", commit + "^{tree}", commit + ":a.txt")]
            for oid in objects + [empty_commit]:
                with self.subTest(oid=oid), self.assertRaises(capture.CaptureError):
                    capture.capture_git_source_two_process(root, oid)

    def test_sha256_repository_and_identity_are_unsupported(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "repo"
            oid = _make_repo(root, {"a.txt": b"a"}, object_format="sha256")
            self.assertEqual(len(oid), 64)
            with patch.object(capture.subprocess, "Popen") as launched:
                with self.assertRaises(capture.CaptureError):
                    capture.capture_git_source_two_process(root, oid)
            launched.assert_not_called()
            with self.assertRaises(capture.CaptureError):
                capture.capture_git_source_two_process(root, oid[:40])

    def test_private_symlink_gitlink_and_oversized_blob_are_rejected(self):
        for kind in ("private", "symlink", "gitlink", "oversized"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary) / "repo"
                old = _make_repo(root, {"a.txt": b"a"})
                if kind == "private":
                    (root / ".private").write_bytes(b"x")
                elif kind == "symlink":
                    (root / "link").symlink_to("a.txt")
                elif kind == "gitlink":
                    _git(root, "update-index", "--add", "--cacheinfo", "160000," + old + ",nested")
                else:
                    (root / "large.bin").write_bytes(b"x" * (capture.MAX_FILE_BYTES + 1))
                if kind != "gitlink":
                    _git(root, "add", "--all")
                _git(root, "commit", "-m", "rejected source")
                oid = _git(root, "rev-parse", "HEAD").stdout.decode().strip()
                with self.assertRaises(capture.CaptureError):
                    capture.capture_git_source_two_process(root, oid)

    def test_missing_blob_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "repo"
            commit = _make_repo(root, {"a.txt": b"a"})
            blob = _git(root, "rev-parse", commit + ":a.txt").stdout.decode().strip()
            (root / ".git" / "objects" / blob[:2] / blob[2:]).unlink()
            with self.assertRaises(capture.CaptureError):
                capture.capture_git_source_two_process(root, commit)

    def test_inherited_git_redirects_and_replacements_cannot_change_capture(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "repo"
            other = Path(temporary) / "other"
            commit = _make_repo(root, {"a.txt": b"original"})
            _make_repo(other, {"a.txt": b"other"})
            old_blob = _git(root, "rev-parse", commit + ":a.txt").stdout.decode().strip()
            replacement = _git(root, "hash-object", "-w", "--stdin", input_data=b"replacement").stdout.decode().strip()
            _git(root, "replace", old_blob, replacement)
            with patch.dict(os.environ, {"GIT_DIR": str(other / ".git"), "GIT_WORK_TREE": str(other),
                                         "GIT_CONFIG_COUNT": "1", "GIT_CONFIG_KEY_0": "core.hooksPath",
                                         "GIT_CONFIG_VALUE_0": str(other)}):
                self.assertEqual(capture.capture_git_source_two_process(root, commit)[1], {"a.txt": b"original"})


if __name__ == "__main__":
    unittest.main()
