"""Durable, single-owner host anchor for the compact candidate journal.

The owner directory is separate from both journal roots and must stay outside
candidate mounts. Reopen requires a commitment obtained independently of this
directory. A trusted-host rollback of both that expectation and this directory
is outside this contract. No candidate execution or acceptance authority lives
here. Deltas retain history; this bounded file atomically stores the latest head.
"""
from __future__ import annotations

from contextlib import ExitStack
from dataclasses import asdict
import fcntl
import json
import os
from pathlib import Path
import stat
import threading
from typing import Any

from . import candidate_checkpoint_chain_v1 as chain
from . import candidate_http_journal_v3 as journal

PROTOCOL = "candidate-checkpoint-head-v1"
MAX_HEAD_BYTES = 4096
_KEYS = {"context_sha256", "sequence", "head_sha256", "raw_file_count",
         "raw_bytes", "external_bytes"}


class HeadError(ValueError):
    """Invalid anchor or unavailable current authority; never a product verdict."""


class HeadUnknown(HeadError):
    """A write or integrity observation failed; this owner cannot resume writes."""


def _require(ok: bool, message: str) -> None:
    if not ok:
        raise HeadError(message)


def _encoded(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def _prefix(value: Any) -> chain.PrefixCommitment:
    _require(type(value) is dict and set(value) == _KEYS, "Exact prefix fields required")
    try:
        return chain.PrefixCommitment(**value)
    except (TypeError, ValueError) as error:
        raise HeadError("Invalid prefix commitment") from error


def _validated(value: chain.PrefixCommitment | None) -> chain.PrefixCommitment | None:
    if value is None:
        return None
    _require(type(value) is chain.PrefixCommitment, "Exact PrefixCommitment required")
    return _prefix(asdict(value))


def _path(value: Path) -> Path:
    _require(isinstance(value, Path) and value.is_absolute()
             and ".." not in value.parts and "\0" not in str(value), "Absolute local path required")
    _require(value.anchor == "/" and value != Path("/"), "Local child path required")
    return value


def _directory(path: Path, stack: ExitStack) -> tuple[int, list[tuple[int, str, tuple[int, int, int]]]]:
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    parent = os.open("/", flags)
    stack.callback(os.close, parent)
    observed = []
    for component in path.parts[1:]:
        before = os.stat(component, dir_fd=parent, follow_symlinks=False)
        _require(stat.S_ISDIR(before.st_mode), "Linked or non-directory ancestor")
        child = os.open(component, flags, dir_fd=parent)
        stack.callback(os.close, child)
        identity = (before.st_dev, before.st_ino, before.st_mode)
        after = os.fstat(child)
        _require(identity == (after.st_dev, after.st_ino, after.st_mode), "Directory changed during open")
        observed.append((parent, component, identity))
        parent = child
    return parent, observed


class ExternalHead:
    """Lifetime-excluded, thread-affine HeadAuthority with explicit uncertainty.

    ``compare_and_set`` acknowledges only after file and directory fsync. Any
    failure after a write is attempted poisons this instance, even if rename
    completed. Recovery is a new owner with an independently observed exact
    expected commitment; leftover pending files are preserved and rejected.
    Normal CAS mismatch returns False without writing or weakening ownership.
    Fork children must promptly exec/exit or close inherited descriptors; a
    long-lived fork child can retain the parent's advisory lock after parent
    death. PID guards reject child use, but do not revoke inherited descriptors.
    """
    root: Path
    journal_roots: tuple[Path, ...]
    _stack: ExitStack
    _pid: int
    _thread: int
    _closed: bool
    _busy: bool
    _uncertain: bool
    _seen: chain.PrefixCommitment | None
    _dir: int
    _ancestors: list[tuple[int, str, tuple[int, int, int]]]
    _lock: int
    _lock_identity: tuple[int, int, int]

    def __init__(self) -> None:
        raise HeadError("Use create or reopen")

    @classmethod
    def create(cls, root: Path, *, journal_roots: tuple[Path, Path]) -> ExternalHead:
        return cls._open(root, journal_roots=journal_roots, expected=None, create=True)

    @classmethod
    def reopen(cls, root: Path, *, journal_roots: tuple[Path, Path],
               expected: chain.PrefixCommitment) -> ExternalHead:
        _require(expected is not None, "Independent expected commitment is mandatory on reopen")
        return cls._open(root, journal_roots=journal_roots, expected=expected, create=False)

    @classmethod
    def _open(cls, root: Path, *, journal_roots: tuple[Path, Path],
              expected: chain.PrefixCommitment | None, create: bool) -> ExternalHead:
        root = _path(root)
        _require(type(journal_roots) is tuple and len(journal_roots) == 2,
                 "Both raw and delta roots are required")
        roots = tuple(_path(p) for p in journal_roots)
        for other in roots:
            _require(other.resolve() == other, "Canonical journal roots required")
            _require(root != other and root not in other.parents and other not in root.parents,
                     "Anchor must be disjoint from journal roots")
        _require(len(set(roots)) == 2, "Distinct raw and delta roots required")
        expected = _validated(expected)
        self = object.__new__(cls)
        self.root = root
        self.journal_roots = roots
        self._stack = ExitStack()
        self._pid = os.getpid()
        self._thread = threading.get_ident()
        self._closed = False
        self._busy = False
        self._uncertain = False
        self._seen = expected
        try:
            parent, _ = _directory(root.parent, self._stack)
            if create:
                os.mkdir(root.name, 0o700, dir_fd=parent)
                os.fsync(parent)
            self._dir, self._ancestors = _directory(root, self._stack)
            info = os.fstat(self._dir)
            _require(info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) == 0o700,
                     "Anchor directory must be private and host-owned")
            flags = os.O_RDWR | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
            if create:
                flags |= os.O_CREAT | os.O_EXCL
            self._lock = os.open("head.lock", flags, 0o600, dir_fd=self._dir)
            self._stack.callback(os.close, self._lock)
            info = os.fstat(self._lock)
            _require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_size == 0
                     and info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) == 0o600,
                     "Invalid owner lock")
            self._lock_identity = (info.st_dev, info.st_ino, info.st_mode)
            fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._stack.callback(self._unlock)
            if create:
                _require(set(os.listdir(self._dir)) == {"head.lock"}, "New anchor directory is not empty")
                os.fsync(self._lock)
                self._publish(None)
            self.read()
            if not create:
                self._sync_current()
                self.read()
            return self
        except BaseException:
            self._stack.close()
            raise

    @property
    def uncertain(self) -> bool:
        return self._uncertain

    def _owner(self) -> None:
        _require(not self._closed and os.getpid() == self._pid
                 and threading.get_ident() == self._thread, "Closed or foreign owner")
        if self._uncertain:
            raise HeadUnknown("Anchor authority is uncertain; no retry on this owner")

    def _unlock(self) -> None:
        # flock is inherited as the same open-file description across fork.
        # Child constructor cleanup may close its FD, but must not unlock ours.
        if os.getpid() == self._pid:
            fcntl.flock(self._lock, fcntl.LOCK_UN)

    def _begin(self) -> None:
        self._owner()
        _require(not self._busy, "Reentrant anchor operation is forbidden")
        self._busy = True

    def _sync_current(self) -> None:
        """Re-establish durable storage after an independently authorized reopen."""
        def identity(info: os.stat_result) -> tuple[int, ...]:
            return (info.st_dev, info.st_ino, info.st_mode, info.st_nlink,
                    info.st_size, info.st_mtime_ns, info.st_ctime_ns)
        try:
            self._paths()
            expected = identity(os.stat("head.json", dir_fd=self._dir, follow_symlinks=False))
            descriptor = os.open("head.json", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                                 dir_fd=self._dir)
            try:
                _require(identity(os.fstat(descriptor)) == expected, "Head changed before recovery sync")
                os.fsync(descriptor)
                os.fsync(self._dir)
                _require(identity(os.fstat(descriptor)) == expected
                         == identity(os.stat("head.json", dir_fd=self._dir, follow_symlinks=False)),
                         "Head changed during recovery sync")
            finally:
                os.close(descriptor)
            self._paths()
        except BaseException as error:
            self._uncertain = True
            if not isinstance(error, Exception):
                raise
            raise HeadUnknown("Reopened head durability remains uncertain") from error

    def _paths(self) -> None:
        for parent, name, expected in self._ancestors:
            info = os.stat(name, dir_fd=parent, follow_symlinks=False)
            _require((info.st_dev, info.st_ino, info.st_mode) == expected,
                     "Anchor ancestor changed")
        info = os.stat("head.lock", dir_fd=self._dir, follow_symlinks=False)
        _require((info.st_dev, info.st_ino, info.st_mode) == self._lock_identity
                 and info.st_nlink == 1 and info.st_size == 0
                 and info.st_uid == os.geteuid(), "Owner lock changed")
        info = os.stat("head.json", dir_fd=self._dir, follow_symlinks=False)
        _require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1
                 and info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) == 0o600,
                 "Head must be a private regular file with no aliases")
        with os.scandir(self._dir) as entries:
            names = set()
            for entry in entries:
                names.add(entry.name)
                _require(len(names) <= 2, "Unexpected or interrupted anchor artifact")
        _require(names == {"head.lock", "head.json"}, "Incomplete anchor inventory")

    def _record(self, commitment: chain.PrefixCommitment | None) -> bytes:
        raw = _encoded({"protocol": PROTOCOL, "journal_roots": [str(p) for p in self.journal_roots],
                        "commitment": asdict(commitment) if commitment is not None else None})
        _require(len(raw) <= MAX_HEAD_BYTES, "Anchor record bound exceeded")
        return raw

    def read(self) -> chain.PrefixCommitment | None:
        self._begin()
        try:
            return self._read_current()
        finally:
            self._busy = False

    def _read_current(self) -> chain.PrefixCommitment | None:
        self._owner()
        try:
            self._paths()
            raw = journal.read(self.root / "head.json", max_bytes=MAX_HEAD_BYTES)
            value = journal.decode(raw, max_bytes=MAX_HEAD_BYTES)
            _require(type(value) is dict and set(value) == {"protocol", "journal_roots", "commitment"},
                     "Exact anchor fields required")
            commitment = None if value["commitment"] is None else _prefix(value["commitment"])
            _require(raw == self._record(commitment), "Foreign or noncanonical anchor record")
            _require(commitment == self._seen, "Anchor differs from independent expected head")
            self._paths()
            return commitment
        except BaseException as error:
            self._uncertain = True
            if not isinstance(error, Exception):
                raise
            raise HeadUnknown("Unable to authenticate current external head") from error

    def _publish(self, commitment: chain.PrefixCommitment | None) -> None:
        raw = self._record(commitment)
        descriptor = os.open("pending.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                             0o600, dir_fd=self._dir)
        try:
            position = 0
            while position < len(raw):
                written = os.write(descriptor, raw[position:])
                if written <= 0:
                    raise OSError("Short anchor write")
                position += written
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace("pending.json", "head.json", src_dir_fd=self._dir, dst_dir_fd=self._dir)
        os.fsync(self._dir)

    def compare_and_set(self, expected: chain.PrefixCommitment | None,
                        proposed: chain.PrefixCommitment) -> bool:
        self._begin()
        try:
            return self._compare_and_set(expected, proposed)
        finally:
            self._busy = False

    def _compare_and_set(self, expected: chain.PrefixCommitment | None,
                         proposed: chain.PrefixCommitment) -> bool:
        self._owner()
        expected = _validated(expected)
        proposed_checked = _validated(proposed)
        if proposed_checked is None:
            raise HeadError("Proposed prefix required")
        current = self._read_current()
        if current != expected:
            return False
        if expected is None:
            _require(proposed_checked.sequence == 0 and proposed_checked.raw_bytes == 0,
                     "Initial anchor must be a genesis commitment")
        else:
            _require(proposed_checked.context_sha256 == expected.context_sha256
                     and proposed_checked.sequence == expected.sequence + 1
                     and proposed_checked.raw_bytes >= expected.raw_bytes
                     and proposed_checked.external_bytes > expected.external_bytes
                     and proposed_checked.head_sha256 != expected.head_sha256,
                     "Anchor must advance one prefix in the same context")
        try:
            self._paths()
            self._publish(proposed_checked)
            self._seen = proposed_checked
            self._read_current()
            return True
        except BaseException as error:
            self._uncertain = True
            if not isinstance(error, Exception):
                raise
            raise HeadUnknown("Anchor write or acknowledgement is uncertain") from error

    def close(self) -> None:
        if self._closed:
            return
        _require(os.getpid() == self._pid and threading.get_ident() == self._thread,
                 "Foreign owner cannot release lifetime lock")
        _require(not self._busy, "Reentrant close is forbidden")
        self._closed = True
        self._stack.close()
