"""Compact trusted prefix commitments; no execution or acceptance authority.

An independently owned HeadAuthority is mandatory. Local chain hashes alone do
not establish freshness. retain() durably appends raw bytes and a delta before
asking that authority to CAS its head. A PrefixCommitment is NOT a statement
that all old bytes were just reread; validate_boundary() performs that separate
bounded observation. It is not an adversarial filesystem snapshot.

Reducing scans changes detection latency: tamper restored between boundaries
can escape unless consumed by read(). Owners must validate at dispatch,
observation, reopen and final authority boundaries and keep their existing
source/CAS/intent/cleanup rules. This primitive neither dispatches nor retries.
"""
from __future__ import annotations

from contextlib import ExitStack
from dataclasses import asdict, dataclass, field
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import threading
from typing import TYPE_CHECKING, Any, Protocol

from . import candidate_http_journal_v3 as stable
if TYPE_CHECKING:
    from . import candidate_journal_batch_read_v1 as batch_read

PROTOCOL = "candidate-checkpoint-chain-v1"
_GENESIS_DOMAIN = (PROTOCOL + "/genesis\0").encode()
_DELTA_DOMAIN = (PROTOCOL + "/delta\0").encode()
_INVENTORY_DOMAIN = (PROTOCOL + "/inventory\0").encode()
_NAME = re.compile(r"[a-z][a-z0-9.-]{0,180}\Z")
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_GENESIS_LIMIT = 65536
_DELTA_LIMIT = 4096
_LOCK = "owner.lock"


class ChainError(ValueError):
    """Rejected input, unsafe state or unavailable evidence; never a pass."""


class ChainUnknown(ChainError):
    """A write, anchor acknowledgement or authenticated state is uncertain."""


class BoundsExceeded(ChainError):
    """Admission failed before writing; reserved cleanup remains available."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ChainError(message)


def _encoded(value: Any) -> bytes:
    try:
        raw = json.dumps(value, sort_keys=True, separators=(",", ":"),
                         ensure_ascii=False, allow_nan=False).encode("utf-8")
        # Also rejects duplicate-key coercions, excessive structure and scalars
        # outside the stable journal dialect. Callers never supply decoded code.
        parsed = stable.decode(raw)
        _require(parsed == value, "JSON input changes under encoding")
        _require(raw == json.dumps(parsed, sort_keys=True, separators=(",", ":"),
                                  ensure_ascii=False, allow_nan=False).encode("utf-8"),
                 "Noncanonical JSON input")
        return raw
    except (TypeError, ValueError, OverflowError, RecursionError) as error:
        raise ChainError("Invalid canonical chain JSON") from error


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _positive(value: Any, maximum: int) -> bool:
    return type(value) is int and 0 < value <= maximum


@dataclass(frozen=True)
class Limits:
    max_files: int = 16384
    max_raw_bytes: int = 512 * 1024 * 1024
    max_raw_file_bytes: int = 32 * 1024 * 1024
    max_external_bytes: int = 64 * 1024 * 1024
    cleanup_files: int = 512
    cleanup_raw_bytes: int = 96 * 1024 * 1024
    cleanup_external_bytes: int = 2 * 1024 * 1024
    max_context_bytes: int = 32768

    def __post_init__(self) -> None:
        bounds = {"max_files": 16384, "max_raw_bytes": 512 * 1024 * 1024,
                  "max_raw_file_bytes": stable.MAX_RECORD_BYTES,
                  "max_external_bytes": 64 * 1024 * 1024, "max_context_bytes": 32768}
        for name, maximum in bounds.items():
            _require(_positive(getattr(self, name), maximum), "Invalid " + name)
        _require(self.max_raw_file_bytes <= self.max_raw_bytes, "File bound exceeds raw bound")
        for reserve, total in ((self.cleanup_files, self.max_files),
                               (self.cleanup_raw_bytes, self.max_raw_bytes),
                               (self.cleanup_external_bytes, self.max_external_bytes)):
            _require(type(reserve) is int and 0 <= reserve < total, "Invalid cleanup reserve")


@dataclass(frozen=True)
class PrefixCommitment:
    context_sha256: str
    sequence: int
    head_sha256: str
    raw_file_count: int
    raw_bytes: int
    external_bytes: int

    def __post_init__(self) -> None:
        _require(type(self.context_sha256) is str and _SHA.fullmatch(self.context_sha256) is not None
                 and type(self.head_sha256) is str and _SHA.fullmatch(self.head_sha256) is not None,
                 "Invalid commitment digest")
        for value in (self.sequence, self.raw_file_count, self.raw_bytes, self.external_bytes):
            _require(type(value) is int and value >= 0, "Invalid commitment counter")
        _require(self.sequence == self.raw_file_count, "One delta per raw file required")


@dataclass(frozen=True)
class BoundaryValidation:
    """A complete byte observation at one call, never a future dispatch permit."""
    commitment: PrefixCommitment
    inventory_sha256: str
    acceptance_authority: bool = field(default=False, init=False)


@dataclass(frozen=True)
class PriorFact:
    """Only one previously committed raw fact, including during uncertainty."""
    name: str
    raw: bytes
    sha256: str
    commitment: PrefixCommitment
    prior_only: bool = field(default=True, init=False)
    acceptance_authority: bool = field(default=False, init=False)


class HeadAuthority(Protocol):
    """Trusted external owner with durable, linearizable compare-and-set.

    read() returns its independently retained current commitment, or None only
    before genesis. CAS must durably replace exactly expected and return True;
    False or any raised/lost acknowledgement is not success. The authority must
    be outside candidate control and must bind the proper execution/context.
    No filesystem-local default is provided. Implementations and their own
    concurrency/durability/authorization need separate qualification.
    """
    def read(self) -> PrefixCommitment | None: ...
    def compare_and_set(self, expected: PrefixCommitment | None,
                        proposed: PrefixCommitment) -> bool: ...


def _name(name: str) -> None:
    _require(type(name) is str and _NAME.fullmatch(name) is not None and name != _LOCK,
             "Invalid immutable raw filename")


def _delta_name(sequence: int) -> str:
    return f"delta-{sequence:08d}.json"


class CheckpointChain:
    """Single-thread, advisory single-owner raw/delta journal.

    After any uncertain write/read/anchor, current-authority methods latch shut.
    read_prior() can still return previously committed individual facts; close()
    always releases resources on the owner thread. Process cleanup must use the
    caller's prior trusted ownership evidence and a separately retained fallback
    channel. cleanup=True spends reserves only while healthy; it never heals
    an uncertain suffix. Reopen requires an independently supplied exact head.
    Parents must already exist durably; creation never invents ancestor paths.
    Memory is structurally bounded by max_files, fixed digest/counter fields,
    bounded ASCII names and one current map, plus bounded single-record buffers.
    These are allocation bounds, not a measured or enforced process RSS limit.
    """
    raw_root: Path
    delta_root: Path
    limits: Limits
    authority: HeadAuthority
    read_policy: batch_read.BatchReadPolicy | None
    _read_policy_record: dict[str, Any] | None
    _genesis: bytes
    _context_sha256: str
    _prefix: PrefixCommitment
    _files: dict[str, tuple[int, str]]
    _deltas: dict[str, str]
    _root_fds: list[int]
    _lock_fds: list[int]
    _root_identities: list[tuple[int, int, int]]
    _thread: int
    _pid: int
    _uncertain: bool
    _closed: bool

    def __init__(self) -> None:
        raise ChainError("Use create() or reopen()")

    @classmethod
    def create(cls, raw_root: Path, delta_root: Path, *, context: dict[str, Any],
               authority: HeadAuthority, limits: Limits = Limits(),
               read_policy: batch_read.BatchReadPolicy | None = None) -> CheckpointChain:
        return cls._open(raw_root, delta_root, context, authority, limits, None, read_policy)

    @classmethod
    def reopen(cls, raw_root: Path, delta_root: Path, *, context: dict[str, Any],
               authority: HeadAuthority, expected: PrefixCommitment,
               limits: Limits = Limits(), read_policy: batch_read.BatchReadPolicy | None = None) -> CheckpointChain:
        _require(type(expected) is PrefixCommitment, "Independent expected commitment required")
        return cls._open(raw_root, delta_root, context, authority, limits, expected, read_policy)

    @classmethod
    def _open(cls, raw_root: Path, delta_root: Path, context: dict[str, Any],
              authority: HeadAuthority, limits: Limits,
              expected: PrefixCommitment | None, read_policy: batch_read.BatchReadPolicy | None) -> CheckpointChain:
        _require(type(limits) is Limits and type(context) is dict, "Typed limits and context required")
        if read_policy is not None:
            from . import candidate_journal_batch_read_v1 as batch_read
            _require(type(read_policy) is batch_read.BatchReadPolicy, "Exact read policy required")
        policy_record = None if read_policy is None else read_policy.record()
        limits = Limits(**asdict(limits))  # Revalidate and detach the caller-owned frozen value.
        context_raw = _encoded(context)
        _require(len(context_raw) <= limits.max_context_bytes, "Context bound exceeded")
        roots = (Path(raw_root).absolute(), Path(delta_root).absolute())
        _require(all(p.resolve() == p and not p.is_symlink() for p in roots), "Canonical roots required")
        _require(all(p.parent.is_dir() for p in roots), "Existing durable parent directories required")
        _require(not roots[0].is_relative_to(roots[1]) and not roots[1].is_relative_to(roots[0]),
                 "Raw and external roots must be disjoint")
        declaration = {"protocol": PROTOCOL, "kind": "genesis", "context": json.loads(context_raw),
                       "raw_root": str(roots[0]), "delta_root": str(roots[1]), "limits": asdict(limits)}
        if policy_record is not None:
            declaration["journal_read"] = policy_record
        genesis = _encoded(declaration)
        _require(len(genesis) <= _GENESIS_LIMIT and len(genesis) <= limits.max_external_bytes - limits.cleanup_external_bytes,
                 "Genesis exceeds ordinary external budget")
        self = object.__new__(cls)
        self.raw_root, self.delta_root = roots
        self.limits, self.authority, self.read_policy = limits, authority, read_policy
        self._read_policy_record = policy_record
        self._genesis = genesis
        self._context_sha256 = _sha(_GENESIS_DOMAIN + genesis)
        self._prefix = PrefixCommitment(self._context_sha256, 0, self._context_sha256, 0, 0, len(genesis))
        self._files = {}
        self._deltas = {}
        self._root_fds = []
        self._lock_fds = []
        self._root_identities = []
        self._thread = threading.get_ident()
        self._pid = os.getpid()
        self._uncertain = False
        self._closed = False
        try:
            observed = authority.read()
            _require(observed == expected, "Independent external head differs")
            for root in roots:
                if expected is None:
                    root.mkdir(exist_ok=False)
                    cls._sync_parent(root)
                _require(root.is_dir() and root.resolve() == root, "Missing/canonical root required")
                fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                self._root_fds.append(fd)
                current = os.fstat(fd)
                self._root_identities.append((current.st_dev, current.st_ino, current.st_mode))
                flags = os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK
                if expected is None:
                    flags |= os.O_CREAT | os.O_EXCL
                lock = os.open(_LOCK, flags, 0o600, dir_fd=fd)
                self._lock_fds.append(lock)
                _require(stat.S_ISREG(os.fstat(lock).st_mode) and os.fstat(lock).st_size == 0,
                         "Invalid owner lock")
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                if expected is None:
                    os.fsync(lock)
                    os.fsync(fd)
            self._assert_roots()
            if expected is None:
                self._write_new(1, "genesis.json", genesis)
                self._anchor(None, self._prefix)
            else:
                _require(expected.context_sha256 == self._context_sha256, "Reopen context differs")
                self._prefix = expected
                files, deltas = self._verify_all(expected)
                self._files, self._deltas = files, deltas
            return self
        except BaseException as error:
            self._uncertain = True
            self.close()
            self._fail(error)
        raise AssertionError("unreachable")

    @staticmethod
    def _sync_parent(root: Path) -> None:
        fd = os.open(root.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    @property
    def commitment(self) -> PrefixCommitment:
        """Last locally acknowledged prefix, even if a later suffix is unknown."""
        self._owner()
        return self._prefix

    @property
    def uncertain(self) -> bool:
        return self._uncertain

    def _owner(self) -> None:
        _require(not self._closed and threading.get_ident() == self._thread and os.getpid() == self._pid,
                 "Closed or foreign-thread/process chain")

    def _assert_roots(self) -> None:
        for i, root in enumerate((self.raw_root, self.delta_root)):
            _require(root.resolve() == root and not root.is_symlink(), "Journal root path changed")
            current = root.stat()
            opened = os.fstat(self._root_fds[i])
            identity = self._root_identities[i]
            _require((current.st_dev, current.st_ino, current.st_mode) == identity
                     == (opened.st_dev, opened.st_ino, opened.st_mode), "Journal root identity changed")
            entry = os.stat(_LOCK, dir_fd=self._root_fds[i], follow_symlinks=False)
            held = os.fstat(self._lock_fds[i])
            _require(stat.S_ISREG(entry.st_mode) and entry.st_nlink == held.st_nlink == 1
                     and entry.st_size == held.st_size == 0
                     and (entry.st_dev, entry.st_ino, entry.st_mode) == (held.st_dev, held.st_ino, held.st_mode),
                     "Owner lock identity changed")

    def _fail(self, error: BaseException) -> None:
        self._uncertain = True
        if isinstance(error, Exception):
            raise ChainUnknown("Chain evidence or anchor uncertain; preserve artifacts, never retry implicitly") from error
        raise error

    def require_current(self) -> PrefixCommitment:
        """Check owner and independent head; does not rehash a boundary."""
        self._owner()
        if self._uncertain:
            raise ChainUnknown("Uncertain chain permits only prior facts and close")
        try:
            self._assert_roots()
            _require(self.authority.read() == self._prefix, "Independent head changed")
        except BaseException as error:
            self._fail(error)
        return self._prefix

    def _write_new(self, root_index: int, name: str, raw: bytes) -> None:
        self._assert_roots()
        root_fd = self._root_fds[root_index]
        fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                     0o600, dir_fd=root_fd)
        try:
            with os.fdopen(fd, "wb", closefd=False) as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(fd)
        finally:
            os.close(fd)
        os.fsync(root_fd)
        self._assert_roots()

    def _anchor(self, old: PrefixCommitment | None, new: PrefixCommitment) -> None:
        _require(self.authority.compare_and_set(old, new) is True, "External CAS not acknowledged")
        _require(self.authority.read() == new, "External CAS readback differs")

    def retain(self, name: str, raw: bytes, *, cleanup: bool = False) -> PrefixCommitment:
        """Exclusive durable append; not a full old-byte validation.

        Admission errors have no writes. Once writing begins, any error poisons
        dependent use, including a CAS that committed but lost its reply. The
        local committed map advances only after a confirmed external head.
        """
        self.require_current()
        _name(name)
        _require(type(raw) is bytes and type(cleanup) is bool, "Immutable raw bytes and bool cleanup required")
        _require(name not in self._files, "Raw filename already committed")
        old = self._prefix
        delta = _encoded({"protocol": PROTOCOL, "kind": "raw-file", "context_sha256": self._context_sha256,
                          "sequence": old.sequence + 1, "previous_head_sha256": old.head_sha256,
                          "name": name, "bytes": len(raw), "sha256": _sha(raw)})
        reserve_files = 0 if cleanup else self.limits.cleanup_files
        reserve_raw = 0 if cleanup else self.limits.cleanup_raw_bytes
        reserve_external = 0 if cleanup else self.limits.cleanup_external_bytes
        if (len(raw) > self.limits.max_raw_file_bytes or len(delta) > _DELTA_LIMIT
                or old.raw_file_count + 1 > self.limits.max_files - reserve_files
                or old.raw_bytes + len(raw) > self.limits.max_raw_bytes - reserve_raw
                or old.external_bytes + len(delta) > self.limits.max_external_bytes - reserve_external):
            raise BoundsExceeded("Raw/delta/file bound; protected cleanup capacity preserved")
        new = PrefixCommitment(self._context_sha256, old.sequence + 1,
                               _sha(_DELTA_DOMAIN + delta), old.raw_file_count + 1,
                               old.raw_bytes + len(raw), old.external_bytes + len(delta))
        delta_name = _delta_name(new.sequence)
        try:
            self._write_new(0, name, raw)
            self._write_new(1, delta_name, delta)
            self._anchor(old, new)
            self._assert_roots()
        except BaseException as error:
            self._fail(error)
        self._files[name] = (len(raw), _sha(raw))
        self._deltas[delta_name] = _sha(delta)
        self._prefix = new
        return new

    def _read_committed(self, name: str) -> bytes:
        _name(name)
        _require(name in self._files, "Raw file is outside acknowledged prior prefix")
        self._assert_roots()
        raw = stable.read(self.raw_root / name, max_bytes=self.limits.max_raw_file_bytes)
        size, digest = self._files[name]
        _require(len(raw) == size and _sha(raw) == digest, "Committed raw bytes changed")
        self._assert_roots()
        return raw

    def has(self, name: str) -> bool:
        """Current acknowledged-prefix membership; never a disk absence proof.

        No full inventory scan occurs. Authority decisions about an absent file
        still require validate_boundary(), which rejects foreign suffixes.
        Invalid filename admission has no effect, as with retain(); ownership
        and external-head failures retain the ordinary require_current contract.
        """
        self.require_current()
        _name(name)
        present = name in self._files
        self.require_current()
        return present

    def position(self, name: str) -> int:
        """Authenticate a record and return its acknowledged 1-based order.

        The ordered map advances only after external acknowledgement and is
        reconstructed in verified delta order on reopen. Read the requested
        raw file and its exact committed delta before exposing that position.
        This point observation is not a full inventory or boundary validation.
        """
        self.require_current()
        _name(name)
        try:
            self._read_committed(name)
            sequence = next(index for index, current in enumerate(self._files, 1)
                            if current == name)
            delta_name = _delta_name(sequence)
            raw_delta = stable.read(self.delta_root / delta_name, max_bytes=_DELTA_LIMIT)
            _require(_sha(raw_delta) == self._deltas[delta_name], "Committed delta bytes changed")
            self.require_current()
            return sequence
        except BaseException as error:
            self._fail(error)
        raise AssertionError("unreachable")

    def read(self, name: str) -> bytes:
        """Authenticate one consumed file and current external head on both sides."""
        self.require_current()
        try:
            raw = self._read_committed(name)
            self.require_current()
            return raw
        except BaseException as error:
            self._fail(error)
        raise AssertionError("unreachable")

    def read_prior(self, name: str) -> PriorFact:
        """Individual prior evidence while uncertain; no freshness/full-history claim.

        This method deliberately does not require the external head to remain
        current. Its commitment is only the last acknowledged local prefix.
        Unanchored/ack-uncertain attempted files never enter that map. Root or
        content tamper still fails. No chain healing or boundary permit results.
        """
        self._owner()
        try:
            raw = self._read_committed(name)
            return PriorFact(name, raw, _sha(raw), self._prefix)
        except BaseException as error:
            self._fail(error)
        raise AssertionError("unreachable")

    def _names(self, index: int, limit: int) -> set[str]:
        names: set[str] = set()
        with os.scandir(self._root_fds[index]) as entries:
            for entry in entries:
                names.add(entry.name)
                _require(len(names) <= limit, "Journal inventory file bound")
        return names

    def _verify_all(self, expected: PrefixCommitment) -> tuple[dict[str, tuple[int, str]], dict[str, str]]:
        if self.read_policy is not None:
            from . import candidate_journal_batch_read_v1 as batch_read
            _require(type(self.read_policy) is batch_read.BatchReadPolicy, "Exact checkpoint read policy required")
        actual_policy = None if self.read_policy is None else self.read_policy.record()
        _require(actual_policy == self._read_policy_record, "Checkpoint read policy changed")
        _require(expected.context_sha256 == self._context_sha256
                 and expected.sequence <= self.limits.max_files
                 and expected.raw_bytes <= self.limits.max_raw_bytes
                 and expected.external_bytes <= self.limits.max_external_bytes,
                 "Expected prefix context/bounds differ")
        self._assert_roots()
        _require(self.authority.read() == expected, "Independent expected head differs")
        with ExitStack() as stack:
            read = stable.read
            if self.read_policy is not None:
                reader = stack.enter_context(batch_read.CheckpointReader(
                    (self.raw_root, self.delta_root), tuple(self._root_identities), policy=self.read_policy))
                read = reader.read
            raw_names = self._names(0, self.limits.max_files + 1)
            delta_names = self._names(1, self.limits.max_files + 2)
            expected_deltas = {_LOCK, "genesis.json", *(_delta_name(i) for i in range(1, expected.sequence + 1))}
            _require(delta_names == expected_deltas, "External rollback, gap or unknown suffix")
            _require(read(self.delta_root / "genesis.json", max_bytes=_GENESIS_LIMIT) == self._genesis,
                     "Genesis context changed")
            files: dict[str, tuple[int, str]] = {}
            deltas: dict[str, str] = {}
            previous = self._context_sha256
            raw_bytes = 0
            external_bytes = len(self._genesis)
            for sequence in range(1, expected.sequence + 1):
                name = _delta_name(sequence)
                raw = read(self.delta_root / name, max_bytes=_DELTA_LIMIT)
                value = stable.decode(raw, max_bytes=_DELTA_LIMIT)
                _require(type(value) is dict and set(value) == {"protocol", "kind", "context_sha256", "sequence",
                         "previous_head_sha256", "name", "bytes", "sha256"} and _encoded(value) == raw,
                         "Closed canonical delta required")
                _require(value["protocol"] == PROTOCOL and value["kind"] == "raw-file"
                         and value["context_sha256"] == self._context_sha256
                         and type(value["sequence"]) is int and value["sequence"] == sequence
                         and value["previous_head_sha256"] == previous, "Delta context/order/previous differs")
                _name(value["name"])
                _require(value["name"] not in files and type(value["bytes"]) is int
                         and 0 <= value["bytes"] <= self.limits.max_raw_file_bytes
                         and type(value["sha256"]) is str and _SHA.fullmatch(value["sha256"]) is not None,
                         "Delta duplicate/name/size/hash differs")
                files[value["name"]] = (value["bytes"], value["sha256"])
                raw_bytes += value["bytes"]
                external_bytes += len(raw)
                _require(raw_bytes <= self.limits.max_raw_bytes and external_bytes <= self.limits.max_external_bytes,
                         "Prefix aggregate bounds exceeded")
                deltas[name] = _sha(raw)
                previous = _sha(_DELTA_DOMAIN + raw)
            reconstructed = PrefixCommitment(self._context_sha256, expected.sequence, previous,
                                              len(files), raw_bytes, external_bytes)
            _require(reconstructed == expected and raw_names == {_LOCK, *files}, "Raw suffix/inventory/head differs")
            for name, (size, digest) in files.items():
                raw = read(self.raw_root / name, max_bytes=self.limits.max_raw_file_bytes)
                _require(len(raw) == size and _sha(raw) == digest, "Boundary raw bytes changed")
            _require(self._names(0, self.limits.max_files + 1) == raw_names
                     and self._names(1, self.limits.max_files + 2) == delta_names,
                     "Inventory changed during boundary")
            self._assert_roots()
            _require(self.authority.read() == expected, "External head changed during boundary")
            return files, deltas

    def validate_boundary(self, *, expected: PrefixCommitment | None = None) -> BoundaryValidation:
        """Reconstruct every delta and hash every raw file; keep freshness external."""
        current = self.require_current()
        try:
            _require(expected is None or type(expected) is PrefixCommitment and expected == current,
                     "Requested boundary prefix differs")
            files, deltas = self._verify_all(current)
            _require(files == self._files and deltas == self._deltas, "Acknowledged map differs")
            inventory = [{"name": name, "bytes": size, "sha256": digest}
                         for name, (size, digest) in sorted(files.items())]
            return BoundaryValidation(current, _sha(_INVENTORY_DOMAIN + _encoded(inventory)))
        except BaseException as error:
            self._fail(error)
        raise AssertionError("unreachable")

    def close(self) -> None:
        """Release only this journal's descriptors; never candidate processes."""
        _require(threading.get_ident() == self._thread and os.getpid() == self._pid, "Foreign-thread/process close")
        if self._closed:
            return
        self._closed = True
        for fd in reversed(self._lock_fds + self._root_fds):
            os.close(fd)
        self._lock_fds.clear()
        self._root_fds.clear()

    def __enter__(self) -> CheckpointChain:
        self._owner()
        return self

    def __exit__(self, *_args: Any) -> None:
        self.close()
