"""Fresh two-child Git capture; no candidate execution or cross-call cache.

The unchanged ls-tree inventory semantics are followed by one interactive
cat-file session. All blob sizes are admitted before any contents request.
The entire capture uses one absolute clock; owned reap retains its separate
five-second bound. This opt-in prototype grants no qualification authority.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import re
import selectors
import subprocess
import time

from . import candidate_git_source_batch_v1 as legacy

PROTOCOL = "candidate-git-source-two-process-v1"
DEADLINE_CONTRACT = legacy.DEADLINE_CONTRACT
MAX_FILES = legacy.MAX_FILES
MAX_FILE_BYTES = legacy.MAX_FILE_BYTES
MAX_SOURCE_BYTES = legacy.MAX_SOURCE_BYTES
MAX_TREE_BYTES = legacy.MAX_TREE_BYTES
MAX_STDERR_BYTES = legacy.MAX_STDERR_BYTES
CLEANUP_REAP_SECONDS = legacy.CLEANUP_REAP_SECONDS
# New wire framing allowances only; candidate inventory/byte limits are unchanged.
MAX_HEADER_BYTES = 128
MAX_INFO_BYTES = (MAX_FILES + 2) * MAX_HEADER_BYTES
MAX_REQUEST_BYTES = (MAX_FILES + 2) * 64 + 12
MAX_CONTENT_BYTES = MAX_SOURCE_BYTES + MAX_FILES * MAX_HEADER_BYTES
_OID = re.compile(r"[0-9a-f]{40}\Z")
CaptureError = legacy.CaptureError
CaptureCleanupError = legacy.CaptureCleanupError
_require, _deadline = legacy._require, legacy._deadline


def _entries(listing: bytes, deadline: float) -> list[tuple[str, bytes]]:
    """Same flattened inventory contract as the frozen four-child reader."""
    _require(listing.endswith(b"\0"), "Incomplete tree framing")
    entries: list[tuple[str, bytes]] = []
    seen: set[str] = set()
    try:
        for entry in listing[:-1].split(b"\0"):
            _deadline(deadline)
            metadata, path_bytes = entry.split(b"\t", 1)
            mode, kind, oid = metadata.split(b" ")
            _require(mode in (b"100644", b"100755") and kind == b"blob",
                     "Only ordinary Git blobs admitted")
            text = path_bytes.decode("utf-8")
            parts = text.split("/")
            oid_text = oid.decode("ascii")
            _require(text not in seen and not text.startswith("/") and "\\" not in text
                     and all(part and part not in (".", "..") and not part.startswith(".") for part in parts),
                     "Unsafe/private/duplicate source path")
            _require(_OID.fullmatch(oid_text) is not None and len(entries) < MAX_FILES,
                     "Object/file bound")
            seen.add(text)
            entries.append((text, oid))
    except (UnicodeError, ValueError) as failure:
        if isinstance(failure, CaptureError):
            raise
        raise CaptureError("Malformed complete tree") from failure
    return entries


def _header(raw: bytes, expected_oid: bytes | None, kind: bytes) -> tuple[bytes, int]:
    _require(0 < len(raw) < MAX_HEADER_BYTES, "Object header bound")
    fields = raw.split(b" ")
    _require(len(fields) == 3 and fields[1] == kind, "Wrong object type or header")
    try:
        oid_text = fields[0].decode("ascii")
    except UnicodeError as failure:
        raise CaptureError("Malformed object identity") from failure
    _require(_OID.fullmatch(oid_text) is not None
             and (expected_oid is None or fields[0] == expected_oid),
             "Object identity or SHA1 format differs")
    _require(bool(fields[2]) and fields[2].isdigit(), "Malformed object size")
    if kind == b"blob":
        _require(len(fields[2]) <= len(str(MAX_FILE_BYTES)), "Blob size header bound")
    size = int(fields[2])
    _require(fields[2] == str(size).encode("ascii"), "Noncanonical object size")
    return fields[0], size


class _Duplex:
    """One bounded, owned session; caller always invokes legacy full cleanup."""
    def __init__(self, selector: selectors.BaseSelector, process: subprocess.Popen[bytes], deadline: float) -> None:
        self.selector = selector
        self.process = process
        self.deadline = deadline
        self.stderr = bytearray()
        self.stdout_open = True
        self.stderr_open = True
        assert process.stdin is not None and process.stdout is not None and process.stderr is not None
        for pipe in (process.stdin, process.stdout, process.stderr):
            os.set_blocking(pipe.fileno(), False)
        selector.register(process.stdout, selectors.EVENT_READ, "output")
        selector.register(process.stderr, selectors.EVENT_READ, "error")

    def exchange(self, request: bytes, *, limit: int, info_rows: int | None) -> bytes:
        _require(type(request) is bytes and 0 < len(request) <= MAX_REQUEST_BYTES,
                 "Bounded command request required")
        _require(type(limit) is int and 0 <= limit <= MAX_CONTENT_BYTES,
                 "Bounded command output required")
        final = info_rows is None
        _deadline(self.deadline)
        _require(self.stdout_open and self.stderr_open, "Session closed before request")
        assert self.process.stdin is not None
        pending = memoryview(request)
        output = bytearray()
        self.selector.register(self.process.stdin, selectors.EVENT_WRITE, "input")
        while True:
            _deadline(self.deadline)
            if not pending:
                if final and not self.stdout_open and not self.stderr_open:
                    break
                if info_rows is not None and output.count(b"\n") >= info_rows:
                    # Every complete row is validated by the caller before the
                    # next request can contain a contents command.
                    _require(self.process.poll() is None, "Session exited after info phase")
                    return bytes(output)
            ready = self.selector.select(_deadline(self.deadline))
            _require(bool(ready), "Capture deadline exhausted")
            for key, _ in ready:
                _deadline(self.deadline)
                if key.data == "input":
                    try:
                        written = os.write(key.fd, pending[:65536])
                    except BlockingIOError:
                        continue
                    except BrokenPipeError as failure:
                        raise CaptureError("Git input closed early") from failure
                    _require(written > 0, "Short Git input")
                    pending = pending[written:]
                    if not pending:
                        self.selector.unregister(key.fileobj)
                        if final:
                            self.process.stdin.close()
                else:
                    target = output if key.data == "output" else self.stderr
                    bound = limit if key.data == "output" else MAX_STDERR_BYTES
                    try:
                        chunk = os.read(key.fd, min(65536, bound - len(target) + 1))
                    except BlockingIOError:
                        continue
                    if not chunk:
                        self.selector.unregister(key.fileobj)
                        if key.data == "output":
                            self.stdout_open = False
                        else:
                            self.stderr_open = False
                        _require(final, "Git closed stream before final contents phase")
                        continue
                    target.extend(chunk)
                    _require(len(target) <= bound, "Git output exceeds bound")
        self.process.wait(timeout=_deadline(self.deadline))
        _require(self.process.returncode == 0, "Git command failed")
        _deadline(self.deadline)
        return bytes(output)


def _cat_capture(path: Path, commit_oid: str, entries: list[tuple[str, bytes]],
                 deadline: float) -> tuple[str, dict[str, bytes]]:
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull,
               GIT_TERMINAL_PROMPT="0", GIT_NO_REPLACE_OBJECTS="1",
               GIT_NO_LAZY_FETCH="1", GIT_ALLOW_PROTOCOL="file", LC_ALL="C")
    argv = ["git", "--literal-pathspecs", "-c", "core.hooksPath=" + os.devnull,
            "-c", "core.fsmonitor=false", "-c", "protocol.allow=never",
            "-c", "protocol.file.allow=always", "-C", str(path),
            "cat-file", "--batch-command", "--buffer"]
    selector: selectors.BaseSelector | None = None
    process: subprocess.Popen[bytes] | None = None
    failure: BaseException | None = None
    result: tuple[str, dict[str, bytes]] | None = None
    try:
        _deadline(deadline)
        selector = selectors.DefaultSelector()
        _deadline(deadline)
        process = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, env=env)
        session = _Duplex(selector, process, deadline)
        commit = commit_oid.encode("ascii")
        request = b"info " + commit + b"\ninfo " + commit + b"^{tree}\n"
        request += b"".join(b"info " + oid + b"\n" for _, oid in entries) + b"flush\n"
        raw_info = session.exchange(request, limit=MAX_INFO_BYTES, info_rows=len(entries) + 2)
        _require(raw_info.endswith(b"\n"), "Incomplete info framing")
        headers = raw_info[:-1].split(b"\n")
        _require(len(headers) == len(entries) + 2, "Missing or extra info rows")
        _header(headers[0], commit, b"commit")
        tree, _tree_size = _header(headers[1], None, b"tree")
        sizes: list[int] = []
        total = 0
        for (_, oid), header in zip(entries, headers[2:]):
            _deadline(deadline)
            _actual, size = _header(header, oid, b"blob")
            total += size
            _require(size <= MAX_FILE_BYTES and total <= MAX_SOURCE_BYTES,
                     "Blob/source size bound before content read")
            sizes.append(size)
        # Crucial ordering: no contents command exists until ALL metadata passed.
        _deadline(deadline)
        contents_request = b"".join(b"contents " + oid + b"\n" for _, oid in entries) + b"flush\n"
        content_limit = total + sum(len(header) + 2 for header in headers[2:])
        raw = session.exchange(contents_request, limit=content_limit, info_rows=None)
        position = 0
        files: dict[str, bytes] = {}
        for (name, oid), size, header in zip(entries, sizes, headers[2:]):
            _deadline(deadline)
            actual = raw[position:position + len(header) + 1]
            _require(actual == header + b"\n", "Content header differs from prior size/type/OID")
            position += len(actual)
            content = raw[position:position + size]
            position += size
            _require(len(content) == size and raw[position:position + 1] == b"\n", "Incomplete framed blob")
            position += 1
            calculated = hashlib.sha1(b"blob " + str(size).encode("ascii") + b"\0" + content).hexdigest().encode("ascii")
            _require(calculated == oid, "Blob bytes do not match registered object identity")
            files[name] = content
        _require(position == len(raw), "Trailing batch bytes")
        _deadline(deadline)
        result = tree.decode("ascii"), files
    except BaseException as caught:
        failure = caught
    finally:
        cleanup_failures, cleanup_interruption = legacy._release(selector, process)
    if isinstance(failure, (KeyboardInterrupt, SystemExit, GeneratorExit)):
        if cleanup_failures:
            failure.add_note(str(CaptureCleanupError(cleanup_failures)))
        raise failure
    if cleanup_interruption is not None:
        cleanup_interruption.add_note(str(CaptureCleanupError(cleanup_failures)))
        raise cleanup_interruption from failure
    if cleanup_failures:
        raise CaptureCleanupError(cleanup_failures) from failure
    if failure is not None:
        if isinstance(failure, (OSError, subprocess.SubprocessError)):
            raise CaptureError("Git transport failed") from failure
        raise failure
    _deadline(deadline)
    assert result is not None
    return result


def capture_git_source_two_process(path: Path, commit_oid: str, *,
                                   timeout_seconds: int = 60) -> tuple[str, dict[str, bytes]]:
    """Capture one commit freshly; no object/session survives this call."""
    _require(type(commit_oid) is str and _OID.fullmatch(commit_oid) is not None,
             "Exact SHA1 commit required")
    _require(type(timeout_seconds) is int and 1 <= timeout_seconds <= 120,
             "Bounded capture deadline required")
    deadline = time.monotonic() + timeout_seconds
    listing = legacy._command(path, ("ls-tree", "-r", "-z", commit_oid), b"", MAX_TREE_BYTES, deadline)
    _deadline(deadline)
    entries = _entries(listing, deadline)
    _deadline(deadline)
    result = _cat_capture(path, commit_oid, entries, deadline)
    _deadline(deadline)
    return result
