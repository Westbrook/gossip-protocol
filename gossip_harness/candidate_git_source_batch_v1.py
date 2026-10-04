"""Bounded Git object capture; protocol candidate-git-source-batch-v1.

Every call independently resolves the exact SHA1 commit and complete tree, checks
all blob types/sizes, then captures and hashes their bytes in four Git children.
No candidate code, hooks, checkout filters, or cross-boundary cache is used.

The 1..120 second budget is one absolute monotonic deadline for the entire
capture, including parsing. Success is checked after command cleanup and after
final parsing. Cleanup has a separate five second reap budget; a deadline cannot
preempt a stuck OS syscall. Failed cleanup is reported, never certified as reaped.
Adopters must bind this module/protocol and deadline contract into new evidence;
this helper alone grants neither product acceptance nor receipt reuse authority.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import re
import selectors
import subprocess
import time
from typing import BinaryIO

PROTOCOL = "candidate-git-source-batch-v1"
DEADLINE_CONTRACT = "whole-capture-monotonic-v1;default=60;range=1..120;cleanup-reap=5"
MAX_FILES = 511
MAX_FILE_BYTES = 2 * 1024 * 1024
MAX_SOURCE_BYTES = 16 * 1024 * 1024
MAX_TREE_BYTES = MAX_FILES * 1200
MAX_INPUT_BYTES = MAX_FILES * 41
MAX_COMMAND_BYTES = MAX_SOURCE_BYTES + MAX_FILES * 128
MAX_STDERR_BYTES = 4096
CLEANUP_REAP_SECONDS = 5.0
_OID = re.compile(r"[0-9a-f]{40}\Z")


class CaptureError(ValueError):
    """Capture unavailable: malformed, oversized, late, or failed transport."""


class CaptureCleanupError(CaptureError):
    """At least one release operation failed; owned-child release is uncertain."""

    def __init__(self, failures: tuple[str, ...]) -> None:
        self.failures = failures
        super().__init__("Git capture cleanup failed: " + ", ".join(failures))


def _require(ok: bool, message: str) -> None:
    if not ok:
        raise CaptureError(message)


def _deadline(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    _require(remaining > 0, "Capture deadline exhausted")
    return remaining


def _release(
    selector: selectors.BaseSelector | None,
    process: subprocess.Popen[bytes] | None,
) -> tuple[tuple[str, ...], BaseException | None]:
    """Try every release, preserving the first interruption until release ends."""
    failures: list[str] = []
    interruptions: list[BaseException] = []

    def record(operation: str, failure: BaseException) -> None:
        failures.append(operation + ":" + type(failure).__name__)
        if isinstance(failure, (KeyboardInterrupt, SystemExit, GeneratorExit)) and not interruptions:
            interruptions.append(failure)
    # Deadline covers reap, not a promise to interrupt a stuck OS cleanup call.
    cleanup_deadline = time.monotonic() + CLEANUP_REAP_SECONDS
    if selector is not None:
        try:
            selector.close()
        except BaseException as failure:
            record("selector.close", failure)
    if process is not None:
        running = True
        try:
            running = process.poll() is None
        except BaseException as failure:
            record("process.poll", failure)
        if running:
            try:
                process.kill()
            except BaseException as failure:
                record("process.kill", failure)
        try:
            process.wait(timeout=max(0.0, cleanup_deadline - time.monotonic()))
        except BaseException as failure:
            record("process.wait", failure)
        # A close failure on any one pipe must not skip the other two.
        for name in ("stdin", "stdout", "stderr"):
            try:
                pipe: BinaryIO | None = getattr(process, name)
                if pipe is not None:
                    pipe.close()
            except BaseException as failure:
                record(name + ".close", failure)
    return tuple(failures), interruptions[0] if interruptions else None


def _command(path: Path, args: tuple[str, ...], data: bytes, limit: int, deadline: float) -> bytes:
    """Bound all pipes concurrently and attempt every owned resource release."""
    _require(type(data) is bytes and len(data) <= MAX_INPUT_BYTES, "Git input exceeds bound")
    _require(type(limit) is int and 0 <= limit <= MAX_COMMAND_BYTES, "Invalid Git output bound")
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull,
               GIT_TERMINAL_PROMPT="0", GIT_NO_REPLACE_OBJECTS="1",
               GIT_NO_LAZY_FETCH="1", GIT_ALLOW_PROTOCOL="file", LC_ALL="C")
    argv = ["git", "--literal-pathspecs", "-c", "core.hooksPath=" + os.devnull,
            "-c", "core.fsmonitor=false", "-c", "protocol.allow=never",
            "-c", "protocol.file.allow=always", "-C", str(path), *args]
    selector: selectors.BaseSelector | None = None
    process: subprocess.Popen[bytes] | None = None
    failure: BaseException | None = None
    output = bytearray()
    error = bytearray()
    pending = memoryview(data)
    try:
        _deadline(deadline)
        # Allocate selector first: its failure cannot leave an untracked child.
        selector = selectors.DefaultSelector()
        _deadline(deadline)
        process = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, env=env)
        assert process.stdin is not None and process.stdout is not None and process.stderr is not None
        for pipe in (process.stdin, process.stdout, process.stderr):
            os.set_blocking(pipe.fileno(), False)
        if pending:
            selector.register(process.stdin, selectors.EVENT_WRITE, "input")
        else:
            process.stdin.close()
        selector.register(process.stdout, selectors.EVENT_READ, "output")
        selector.register(process.stderr, selectors.EVENT_READ, "error")
        while selector.get_map():
            ready = selector.select(_deadline(deadline))
            _require(bool(ready), "Capture deadline exhausted")
            for key, _ in ready:
                _deadline(deadline)
                if key.data == "input":
                    try:
                        written = os.write(key.fd, pending[:65536])
                    except BrokenPipeError:
                        raise CaptureError("Git input closed early") from None
                    _require(written > 0, "Short Git input")
                    pending = pending[written:]
                    if not pending:
                        selector.unregister(key.fileobj)
                        process.stdin.close()
                else:
                    target = output if key.data == "output" else error
                    bound = limit if key.data == "output" else MAX_STDERR_BYTES
                    chunk = os.read(key.fd, min(65536, bound - len(target) + 1))
                    if not chunk:
                        selector.unregister(key.fileobj)
                        # The Popen pipe handles are always released in _release.
                        continue
                    target.extend(chunk)
                    _require(len(target) <= bound, "Git output exceeds bound")
        process.wait(timeout=_deadline(deadline))
        _require(process.returncode == 0, "Git command failed")
        _deadline(deadline)
    except BaseException as caught:
        failure = caught
    finally:
        cleanup_failures, cleanup_interruption = _release(selector, process)
    # A body interruption happened first; a cleanup interruption must likewise
    # survive all remaining release attempts rather than become a capture error.
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
    return bytes(output)


def capture_git_source_batch(
    path: Path, commit_oid: str, *, timeout_seconds: int = 60,
) -> tuple[str, dict[str, bytes]]:
    """Return exact tree identity and bounded ordinary-file bytes for one commit."""
    _require(type(commit_oid) is str and _OID.fullmatch(commit_oid) is not None,
             "Exact SHA1 commit required")
    _require(type(timeout_seconds) is int and 1 <= timeout_seconds <= 120,
             "Bounded capture deadline required")
    deadline = time.monotonic() + timeout_seconds
    identities = _command(path, ("rev-parse", "--show-object-format", commit_oid + "^{commit}",
                                 commit_oid + "^{tree}"), b"", 128, deadline)
    try:
        ids = identities.decode("ascii").split("\n")
    except UnicodeError as failure:
        raise CaptureError("Malformed identity response") from failure
    _require(len(ids) == 4 and ids[0] == "sha1" and ids[1] == commit_oid
             and _OID.fullmatch(ids[2]) is not None and ids[3] == "",
             "Commit/tree identity or framing differs")
    listing = _command(path, ("ls-tree", "-r", "-z", commit_oid), b"", MAX_TREE_BYTES, deadline)
    _require(bool(listing) and listing.endswith(b"\0"), "Empty/incomplete tree")
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
    request = b"".join(oid + b"\n" for _, oid in entries)
    sizes_raw = _command(path, ("cat-file", "--batch-check"), request, MAX_FILES * 128, deadline)
    _require(sizes_raw.endswith(b"\n"), "Incomplete batch size framing")
    headers = sizes_raw[:-1].split(b"\n")
    _require(len(headers) == len(entries), "Missing or extra batch size rows")
    sizes: list[int] = []
    total = 0
    for (_, oid), header in zip(entries, headers):
        _deadline(deadline)
        fields = header.split(b" ")
        _require(len(fields) == 3 and fields[:2] == [oid, b"blob"]
                 and 1 <= len(fields[2]) <= len(str(MAX_FILE_BYTES)) and fields[2].isdigit(),
                 "Missing/wrong-type/oversized batch object")
        size = int(fields[2])
        _require(fields[2] == str(size).encode("ascii"), "Noncanonical object size")
        total += size
        _require(size <= MAX_FILE_BYTES and total <= MAX_SOURCE_BYTES,
                 "Blob/source size bound before content read")
        sizes.append(size)
    raw = _command(path, ("cat-file", "--batch"), request,
                   total + sum(len(header) + 2 for header in headers), deadline)
    position = 0
    files: dict[str, bytes] = {}
    for (name, oid), size, header in zip(entries, sizes, headers):
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
    return ids[2], files
