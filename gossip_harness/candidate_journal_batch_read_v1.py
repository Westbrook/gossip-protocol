"""Opt-in fresh checkpoint directory descriptors; never cached journal evidence.

Every file observes all ancestor entries and descriptors before and after its
fresh bounded read. Descriptor reuse changes the syscall/race observation trace;
this is not an atomic filesystem snapshot. The legacy reader remains default.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import re
import stat
from types import TracebackType
from typing import Any

from . import candidate_http_journal_v3 as stable

PROTOCOL = "candidate-journal-batch-read-v1"
_NAME = re.compile(r"[a-z][a-z0-9.-]{0,180}\Z")
_LOADED_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
_STABLE_SHA256 = hashlib.sha256(Path(stable.__file__).read_bytes()).hexdigest()


def evaluator_sources() -> dict[str, str]:
    result = {"gossip_harness/" + path.name: hashlib.sha256(path.read_bytes()).hexdigest()
              for path in (Path(__file__), Path(stable.__file__))}
    if (result["gossip_harness/" + Path(__file__).name] != _LOADED_SHA256
            or result["gossip_harness/" + Path(stable.__file__).name] != _STABLE_SHA256):
        raise stable.JournalError("Loaded checkpoint reader source changed")
    return result


@dataclass(frozen=True, slots=True)
class BatchReadPolicy:
    """One exact closed policy; no caller reader, callbacks or deadline override."""
    def record(self) -> dict[str, Any]:
        if type(self) is not BatchReadPolicy:
            raise stable.JournalError("Exact checkpoint batch policy required")
        return {"protocol": PROTOCOL, "ancestry_checks": "before-and-after-every-file",
                "descriptor_lifetime": "one-full-checkpoint", "cross_checkpoint_cache": False,
                "fresh_reads": "every-genesis-delta-and-raw-byte", "leaf_guards": stable.PROTOCOL,
                "allocation_bounds": "unchanged-caller-journal-limits", "deadline": "unchanged-caller",
                "sources": evaluator_sources()}

    def __post_init__(self) -> None:
        self.record()


class CheckpointReader:
    """Own only fresh descriptors, never borrowed chain/head/lock descriptors."""
    def __init__(self, roots: tuple[Path, Path], identities: tuple[tuple[int, int, int], ...], *,
                 policy: BatchReadPolicy):
        if type(policy) is not BatchReadPolicy:
            raise stable.JournalError("Exact checkpoint batch policy required")
        policy.record()
        if len(roots) != 2 or len(identities) != 2:
            raise stable.JournalError("Two independent journal roots required")
        if not all(hasattr(os, flag) for flag in ("O_NOFOLLOW", "O_DIRECTORY", "O_NONBLOCK")):
            raise stable.JournalError("Required no-follow descriptor support unavailable")
        self._fds: list[int] = []
        self._roots: dict[Path, int] = {}
        self._ancestors: dict[Path, list[tuple[int, str, int, tuple[int, int, int]]]] = {}
        self._slash: list[tuple[int, tuple[int, int, int]]] = []
        self._closed = False
        self._flags = os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
        try:
            for root, expected in zip(roots, identities, strict=True):
                if (not isinstance(root, Path) or not root.is_absolute() or root.anchor != "/"
                        or ".." in root.parts or "\0" in str(root) or root in self._roots):
                    raise stable.JournalError("Exact distinct absolute journal roots required")
                entry = os.stat("/", follow_symlinks=False)
                parent = self._open("/", self._flags | os.O_DIRECTORY)
                identity = stable._directory_identity(entry)
                if not stat.S_ISDIR(entry.st_mode) or stable._directory_identity(os.fstat(parent)) != identity:
                    raise stable.JournalError("Filesystem root changed during open")
                self._slash.append((parent, identity))
                ancestors = []
                for name in root.parts[1:]:
                    entry = os.stat(name, dir_fd=parent, follow_symlinks=False)
                    if not stat.S_ISDIR(entry.st_mode):
                        raise stable.JournalError("Non-directory or linked journal ancestor")
                    child = self._open(name, self._flags | os.O_DIRECTORY, dir_fd=parent)
                    identity = stable._directory_identity(entry)
                    opened = os.fstat(child)
                    if not stat.S_ISDIR(opened.st_mode) or stable._directory_identity(opened) != identity:
                        raise stable.JournalError("Journal ancestor changed during open")
                    ancestors.append((parent, name, child, identity))
                    parent = child
                if stable._directory_identity(os.fstat(parent)) != expected:
                    raise stable.JournalError("Anchored journal root differs from owner")
                self._roots[root] = parent
                self._ancestors[root] = ancestors
            self.validate()
        except BaseException as error:
            try:
                self.close()
            except BaseException as cleanup:
                error.add_note("Checkpoint descriptor cleanup failed: " + repr(cleanup))
            raise

    def _open(self, path: str, flags: int, *, dir_fd: int | None = None) -> int:
        fd = os.open(path, flags, dir_fd=dir_fd)
        try:
            self._fds.append(fd)
        except BaseException:
            os.close(fd)
            raise
        return fd

    def validate(self, root: Path | None = None) -> None:
        if self._closed:
            raise stable.JournalError("Closed checkpoint reader")
        for descriptor, expected in self._slash:
            if (stable._directory_identity(os.stat("/", follow_symlinks=False)) != expected
                    or stable._directory_identity(os.fstat(descriptor)) != expected):
                raise stable.JournalError("Filesystem root identity changed")
        chosen = self._ancestors.values() if root is None else (self._ancestors[root],)
        for ancestors in chosen:
            for parent, name, child, expected in ancestors:
                entry = os.stat(name, dir_fd=parent, follow_symlinks=False)
                opened = os.fstat(child)
                if (not stat.S_ISDIR(entry.st_mode) or not stat.S_ISDIR(opened.st_mode)
                        or stable._directory_identity(entry) != expected
                        or stable._directory_identity(opened) != expected):
                    raise stable.JournalError("Checkpoint journal ancestry changed")

    def read(self, path: Path | str, *, max_bytes: int = stable.MAX_RECORD_BYTES) -> bytes:
        stable._bound(max_bytes)
        if not isinstance(path, (str, Path)):
            raise stable.JournalError("Exact checkpoint file path required")
        path = Path(path)
        if (path.parent not in self._roots
                or _NAME.fullmatch(path.name) is None or path.name == "owner.lock"):
            raise stable.JournalError("Read must name a declared-root journal record")
        self.validate(path.parent)
        parent = self._roots[path.parent]
        entry = os.stat(path.name, dir_fd=parent, follow_symlinks=False)
        if not stat.S_ISREG(entry.st_mode) or not 0 <= entry.st_size <= max_bytes:
            raise stable.JournalError("Journal is not a bounded regular file")
        descriptor = os.open(path.name, self._flags | os.O_NONBLOCK, dir_fd=parent)
        error: BaseException | None = None
        try:
            opened = os.fstat(descriptor)
            identity = stable._file_identity(entry)
            if not stat.S_ISREG(opened.st_mode) or stable._file_identity(opened) != identity:
                raise stable.JournalError("Journal changed during open")
            chunks: list[bytes] = []
            count = 0
            while count <= opened.st_size:
                chunk = os.read(descriptor, min(stable._READ_CHUNK_BYTES, opened.st_size + 1 - count))
                if not chunk:
                    break
                chunks.append(chunk)
                count += len(chunk)
            if count != opened.st_size or stable._file_identity(os.fstat(descriptor)) != identity:
                raise stable.JournalError("Journal content/size changed during read")
            current = os.stat(path.name, dir_fd=parent, follow_symlinks=False)
            if not stat.S_ISREG(current.st_mode) or stable._file_identity(current) != identity:
                raise stable.JournalError("Journal path changed during read")
            self.validate(path.parent)
            return b"".join(chunks)
        except BaseException as failure:
            error = failure
            raise
        finally:
            try:
                os.close(descriptor)
            except BaseException as cleanup:
                if error is None:
                    raise
                error.add_note("Checkpoint leaf close failed: " + repr(cleanup))

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        errors: list[BaseException] = []
        for descriptor in reversed(self._fds):
            try:
                os.close(descriptor)
            except BaseException as error:
                errors.append(error)
        self._fds.clear()
        if errors:
            for later in errors[1:]:
                errors[0].add_note("Additional checkpoint descriptor close failure: " + repr(later))
            raise errors[0]

    def __enter__(self) -> CheckpointReader:
        try:
            self.validate()
        except BaseException as error:
            try:
                self.close()
            except BaseException as cleanup:
                error.add_note("Checkpoint entry cleanup failed: " + repr(cleanup))
            raise
        return self

    def __exit__(self, _kind: type[BaseException] | None, error: BaseException | None,
                 _traceback: TracebackType | None) -> None:
        try:
            if error is None:
                self.validate()
        except BaseException as final:
            error = final
            raise
        finally:
            try:
                self.close()
            except BaseException as cleanup:
                if error is None:
                    raise
                error.add_note("Checkpoint directory close failed: " + repr(cleanup))
