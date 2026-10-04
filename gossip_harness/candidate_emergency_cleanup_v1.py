"""One-shot, ownership-bounded emergency cleanup after journal uncertainty.

The main journal is never repaired or extended by run(). Only acknowledged
prior claims authorize inspection and removal. A recovered container ID needs
an exact fresh match to the pre-create claim and proven pre-absence. Volumes
require an acknowledged full identity baseline. Diagnostic failure closes this
channel; it does not turn missing responses into success. This is host cleanup,
never a product observation, acceptance receipt or permission to resume work.
"""
from __future__ import annotations

from contextlib import ExitStack
from dataclasses import asdict, dataclass, field
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import threading
import time
from typing import Any, Protocol

from . import candidate_checkpoint_chain_v1 as chain
from . import candidate_client_process_v4 as engine
from . import candidate_http_journal_v3 as stable

PROTOCOL = "candidate-emergency-cleanup-v1"
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_NAME = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}\Z")
_FILE = re.compile(r"[a-z][a-z0-9.-]{0,180}\Z")
_CHANNEL = "emergency-cleanup-channel.json"


class CleanupError(ValueError):
    """Invalid ownership, exhausted bound or uncertain diagnostic channel."""


class Journal(Protocol):
    @property
    def raw_root(self) -> Path: ...
    @property
    def delta_root(self) -> Path: ...
    @property
    def commitment(self) -> chain.PrefixCommitment: ...
    def read(self, name: str) -> bytes: ...
    def read_prior(self, name: str) -> chain.PriorFact: ...
    def retain(self, name: str, raw: bytes, *, cleanup: bool = False) -> Any: ...


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise CleanupError(message)


def _encoded(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _copy(value: Any) -> Any:
    return stable.decode(_encoded(value))


def _path(value: Path) -> Path:
    _require(isinstance(value, Path) and value.is_absolute() and value.anchor == "/"
             and value != Path("/") and ".." not in value.parts and "\0" not in str(value),
             "Canonical absolute child path required")
    _require(value.resolve(strict=False) == value, "Linked or noncanonical path")
    return value


def _overlaps(left: Path, right: Path) -> bool:
    return left == right or left in right.parents or right in left.parents


def _directory(path: Path, stack: ExitStack) -> tuple[int, list[tuple[int, str, tuple[int, int, int]]]]:
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    parent = os.open("/", flags)
    stack.callback(os.close, parent)
    observed = []
    for component in path.parts[1:]:
        before = os.stat(component, dir_fd=parent, follow_symlinks=False)
        _require(stat.S_ISDIR(before.st_mode), "Linked or non-directory cleanup ancestor")
        child = os.open(component, flags, dir_fd=parent)
        stack.callback(os.close, child)
        identity = (before.st_dev, before.st_ino, before.st_mode)
        after = os.fstat(child)
        _require(identity == (after.st_dev, after.st_ino, after.st_mode), "Cleanup ancestor changed")
        observed.append((parent, component, identity))
        parent = child
    return parent, observed


@dataclass(frozen=True)
class CleanupLimits:
    max_resources: int = 512
    max_files: int = 8192
    max_bytes: int = 128 * 1024 * 1024
    max_record_bytes: int = 3 * 1024 * 1024
    total_seconds: float = 120.0
    request_seconds: float = 5.0

    def __post_init__(self) -> None:
        for value, bound in ((self.max_resources, 512), (self.max_files, 8192),
                             (self.max_bytes, 128 * 1024 * 1024),
                             (self.max_record_bytes, 3 * 1024 * 1024)):
            _require(type(value) is int and 0 < value <= bound, "Invalid cleanup storage bound")
        _require(self.max_record_bytes <= self.max_bytes, "Cleanup record exceeds total byte bound")
        for seconds, maximum in ((self.total_seconds, 120), (self.request_seconds, 15)):
            _require(type(seconds) in (int, float) and math.isfinite(seconds) and 0 < seconds <= maximum,
                     "Invalid cleanup time bound")


@dataclass(frozen=True)
class ResourceDisposition:
    claim: str
    kind: str
    name: str
    status: str
    resource_id: str | None = None
    removal_attempted: bool = False
    error: str | None = None


@dataclass(frozen=True)
class CleanupResult:
    original_failure: str
    dispositions: tuple[ResourceDisposition, ...]
    uncertainties: tuple[str, ...]
    all_resources_absent: bool
    diagnostic_root: str
    acceptance_authority: bool = field(default=False, init=False)
    main_journal_healed: bool = field(default=False, init=False)


class CleanupChannel:
    """Exclusive, thread/process-affine and non-reopenable cleanup owner.

    create/claim/confirm run while the main journal is healthy. run is one-shot
    and uses only read_prior. No arbitrary effect callback or cleanup command
    supplied by callers is executed. Endpoint requests are the fixed inspection
    and deletion operations below. A diagnostic uncertainty prohibits further
    requests. Fresh runtime binding is checked before any resource operation.
    """
    root: Path
    journal: Journal
    endpoint: engine.EngineEndpoint
    limits: CleanupLimits
    _context: dict[str, Any]
    _claims: list[str]
    _confirmations: dict[str, str]
    _normal_attempts: set[str]
    _retired: dict[str, str]
    _names: set[str]
    _bytes: int
    _stack: ExitStack
    _directory_fd: int
    _ancestors: list[tuple[int, str, tuple[int, int, int]]]
    _pid: int
    _thread: int
    _closed: bool
    _ran: bool
    _busy: bool
    _uncertain: bool
    _deadline: float

    def __init__(self) -> None:
        raise CleanupError("Use create")

    @classmethod
    def create(cls, root: Path, *, journal: Journal, endpoint: engine.EngineEndpoint,
               runtime: dict[str, Any], source_sha256: str, fixture_sha256: str,
               execution_id: str, image_id: str, candidate_mount_roots: tuple[Path, ...],
               journal_roots: tuple[Path, ...], limits: CleanupLimits = CleanupLimits()) -> CleanupChannel:
        root = _path(root)
        _require(type(endpoint) is engine.EngineEndpoint and type(runtime) is dict
                 and type(journal.commitment) is chain.PrefixCommitment, "Exact runtime and journal binding required")
        for digest in (source_sha256, fixture_sha256):
            _require(type(digest) is str and _SHA.fullmatch(digest) is not None, "Invalid cleanup source/fixture")
        _require(type(execution_id) is str and _NAME.fullmatch(execution_id) is not None,
                 "Invalid execution identity")
        _require(type(image_id) is str and re.fullmatch(r"sha256:[0-9a-f]{64}", image_id) is not None
                 and runtime.get("image_id") == image_id, "Pinned runtime image required")
        _require(type(candidate_mount_roots) is tuple and bool(candidate_mount_roots)
                 and type(journal_roots) is tuple and len(journal_roots) >= 2,
                 "Explicit candidate and journal origin roots required")
        _require(journal_roots[:2] == (journal.raw_root, journal.delta_root),
                 "Journal origin roots differ from actual owner")
        origins = tuple(_path(path) for path in (*candidate_mount_roots, *journal_roots))
        _require(all(not _overlaps(root, path) for path in origins), "Cleanup root overlaps candidate or journal origin")
        limits = CleanupLimits(**asdict(limits))
        self = object.__new__(cls)
        self.root, self.journal, self.endpoint, self.limits = root, journal, endpoint, limits
        self._context = {"protocol": PROTOCOL, "root": str(root),
            "context_sha256": journal.commitment.context_sha256,
            "source_sha256": source_sha256, "fixture_sha256": fixture_sha256,
            "execution_id": execution_id, "image_id": image_id,
            "runtime": _copy(runtime), "endpoint": asdict(endpoint), "limits": asdict(limits),
            "candidate_mount_roots": [str(path) for path in candidate_mount_roots],
            "journal_roots": [str(path) for path in journal_roots]}
        self._claims, self._confirmations, self._normal_attempts, self._retired = [], {}, set(), {}
        self._names, self._bytes = set(), 0
        self._pid, self._thread = os.getpid(), threading.get_ident()
        self._closed = self._ran = self._busy = self._uncertain = False
        self._deadline = 0.0
        self._stack = ExitStack()
        try:
            parent, _ = _directory(root.parent, self._stack)
            os.mkdir(root.name, mode=0o700, dir_fd=parent)
            os.fsync(parent)
            self._directory_fd, self._ancestors = _directory(root, self._stack)
            mode = os.fstat(self._directory_fd).st_mode
            _require(stat.S_IMODE(mode) == 0o700, "Cleanup root must be private")
            self._retain("context.json", _encoded(self._context))
            journal.retain(_CHANNEL, _encoded(self._context))
            return self
        except BaseException:
            self._stack.close()
            raise

    def _owner(self) -> None:
        _require(not self._closed and self._pid == os.getpid() and self._thread == threading.get_ident(),
                 "Cleanup owner closed or belongs to another process/thread")

    def _roots(self) -> None:
        self._owner()
        for parent, name, identity in self._ancestors:
            current = os.stat(name, dir_fd=parent, follow_symlinks=False)
            _require(identity == (current.st_dev, current.st_ino, current.st_mode), "Cleanup origin changed")
        _require(set(os.listdir(self._directory_fd)) == self._names, "Unexpected cleanup diagnostic suffix")

    def _retain(self, name: str, raw: bytes) -> None:
        self._owner()
        _require(not self._uncertain, "Cleanup diagnostic channel uncertain")
        try:
            _require(type(name) is str and _FILE.fullmatch(name) is not None and name not in self._names
                     and type(raw) is bytes, "Invalid or duplicate cleanup diagnostic")
            _require(len(self._names) < self.limits.max_files and len(raw) <= self.limits.max_record_bytes
                     and self._bytes + len(raw) <= self.limits.max_bytes, "Cleanup diagnostic bound exceeded")
            self._roots()
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
            descriptor = os.open(name, flags, 0o600, dir_fd=self._directory_fd)
            try:
                view = memoryview(raw)
                while view:
                    count = os.write(descriptor, view)
                    _require(count > 0, "Cleanup diagnostic short write")
                    view = view[count:]
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            os.fsync(self._directory_fd)
            self._names.add(name)
            self._bytes += len(raw)
            self._roots()
        except BaseException:
            self._uncertain = True
            raise

    def _prior(self, name: str) -> bytes:
        fact = self.journal.read_prior(name)
        _require(type(fact) is chain.PriorFact and fact.name == name and fact.prior_only
                 and not fact.acceptance_authority and fact.sha256 == _sha(fact.raw)
                 and fact.commitment.context_sha256 == self._context["context_sha256"],
                 "Prior cleanup evidence belongs to a different journal")
        return fact.raw

    def _record(self, name: str, *, prior: bool) -> tuple[dict[str, Any], bytes]:
        read = self._prior if prior else self.journal.read
        record = stable.decode(read(name))
        _require(type(record) is dict and record.get("exit_code") == 0
                 and type(record.get("exit_code")) is int and record.get("timed_out") is False
                 and record.get("capture_complete") is True, "Original control command incomplete")
        for kind in ("stdout", "stderr"):
            item = record.get(kind)
            _require(type(item) is dict and item.get("truncated") is False
                     and type(item.get("path")) is str and _FILE.fullmatch(item["path"]) is not None,
                     "Original control descriptor malformed")
            raw = read(item["path"])
            _require(item.get("sha256") == _sha(raw) and type(item.get("bytes")) is int
                     and item["bytes"] == len(raw) and item.get("observed_bytes") == len(raw),
                     "Original control bytes differ")
        return record, read(record["stdout"]["path"])

    def _base_argv(self) -> list[str]:
        return ["docker", "--host", "unix://" + self.endpoint.socket_path]

    def _absence(self, name: str, kind: str, record_name: str, *, prior: bool) -> None:
        record, raw = self._record(record_name, prior=prior)
        suffix = (["container", "ls", "--all", "--quiet", "--filter", "name=^/" + name + "$"]
                  if kind == "container" else ["volume", "ls", "--quiet", "--filter", "name=^" + name + "$"])
        _require(record.get("argv") == self._base_argv() + suffix and not raw.strip(),
                 "Exact resource pre-absence is not established")

    def _claim(self, value: dict[str, Any], preabsence_record: str) -> str:
        self._owner()
        _require(not self._ran and not self._busy and len(self._claims) < self.limits.max_resources,
                 "Cleanup claims closed or resource bound exceeded")
        _require(type(value["name"]) is str and _NAME.fullmatch(value["name"]) is not None,
                 "Invalid resource name")
        for old in self._claims:
            existing = stable.decode(self.journal.read(old))
            _require(existing["resource"]["name"] != value["name"], "Duplicate resource claim")
        self._absence(value["name"], value["kind"], preabsence_record, prior=False)
        claim_name = "emergency-claim-" + str(len(self._claims)).zfill(4) + ".json"
        claim = {"protocol": PROTOCOL, "channel_sha256": _sha(_encoded(self._context)),
                 "preabsence_record": preabsence_record, "resource": value}
        self.journal.retain(claim_name, _encoded(claim))
        self._claims.append(claim_name)
        return claim_name

    def claim_container(self, *, name: str, labels: dict[str, str], argv: tuple[str, ...],
                        preabsence_record: str) -> str:
        labels = self._labels(labels)
        keeper = labels.get("gossip.role") in ("keeper", "volume-keeper")
        _require(keeper or (labels.get("gossip.source") == self._context["source_sha256"]
                 and labels.get("gossip.fixture") == self._context["fixture_sha256"]),
                 "Container source and fixture labels differ")
        _require(type(argv) is tuple and 0 < len(argv) <= 256
                 and all(type(item) is str and item and "\0" not in item for item in argv)
                 and sum(len(item.encode()) for item in argv) <= 65536, "Invalid declared container argv")
        return self._claim({"kind": "container", "name": name, "image_id": self._context["image_id"],
                            "labels": labels, "argv": list(argv)}, preabsence_record)

    def _labels(self, labels: dict[str, str]) -> dict[str, str]:
        _require(type(labels) is dict and 1 <= len(labels) <= 32
                 and all(type(key) is str and type(value) is str and len(key) <= 128
                         and len(value) <= 256 for key, value in labels.items())
                 and labels.get("gossip.execution") == self._context["execution_id"],
                 "Exact execution labels required")
        for key, expected in (("gossip.source", self._context["source_sha256"]),
                              ("gossip.fixture", self._context["fixture_sha256"])):
            _require(key not in labels or labels[key] == expected, "Cleanup label binding differs")
        return dict(labels)

    def claim_volume(self, *, name: str, labels: dict[str, str], options: dict[str, str],
                     preabsence_record: str) -> str:
        labels = self._labels(labels)
        _require(type(options) is dict and 0 < len(options) <= 8
                 and all(type(key) is str and type(value) is str and len(key) <= 128
                         and len(value) <= 256 for key, value in options.items()), "Invalid volume options")
        return self._claim({"kind": "volume", "name": name, "labels": labels,
                            "driver": "local", "options": dict(options)}, preabsence_record)

    def _known_claim(self, claim: str) -> dict[str, Any]:
        self._owner()
        _require(claim in self._claims and not self._ran, "Unknown or closed cleanup claim")
        return stable.decode(self.journal.read(claim))["resource"]

    def confirm_container(self, claim: str, *, create_record: str) -> None:
        resource = self._known_claim(claim)
        _require(resource["kind"] == "container" and claim not in self._confirmations, "Invalid duplicate confirmation")
        record, raw = self._record(create_record, prior=False)
        resource_id = raw.strip().decode("ascii")
        argv = record.get("argv")
        _require(_SHA.fullmatch(resource_id) is not None and type(argv) is list
                 and argv[:4] == self._base_argv() + ["create"]
                 and argv.count("--name") == 1 and argv[argv.index("--name") + 1] == resource["name"]
                 and resource["image_id"] in argv, "Create response does not bind full owned ID")
        self._confirm(claim, {"kind": "container", "resource_id": resource_id, "create_record": create_record})

    def confirm_volume(self, claim: str, *, inspection_record: str | None = None,
                       baseline_record: str | None = None) -> None:
        resource = self._known_claim(claim)
        _require(resource["kind"] == "volume" and claim not in self._confirmations
                 and (inspection_record is None) != (baseline_record is None), "One volume identity source required")
        if inspection_record is not None:
            record, raw = self._record(inspection_record, prior=False)
            _require(record.get("argv") == self._base_argv() +
                     ["volume", "inspect", "--format", "{{json .}}", resource["name"]],
                     "Volume inspection command differs")
        else:
            assert baseline_record is not None
            raw = self.journal.read(baseline_record)
        baseline = self._volume_identity(stable.decode(raw), resource)
        self._confirm(claim, {"kind": "volume", "baseline": baseline,
                             "inspection_record": inspection_record, "baseline_record": baseline_record})

    def _confirm(self, claim: str, value: dict[str, Any]) -> None:
        name = claim.removesuffix(".json") + "-confirmed.json"
        self.journal.retain(name, _encoded({"claim": claim, "confirmation": value}))
        self._confirmations[claim] = name

    def note_normal_removal(self, claim: str) -> None:
        """Latch an ordinary removal attempt; emergency cleanup cannot retry it.

        Call immediately before the normal owner begins retaining/sending its
        remove request. Even pre-send uncertainty remains conservatively an
        attempt. This memory only denies actions; it never grants ownership.
        """
        self._owner()
        _require(not self._ran and claim in self._claims, "Unknown or closed cleanup claim")
        self._normal_attempts.add(claim)

    def confirm_normal_removal(self, claim: str, *, remove_record: str, absence_record: str) -> None:
        """Acknowledge retirement from original command facts, never a caller bool.

        This does not erase the original attempt or permit another remove. A
        later emergency path still freshly checks absence of this exact ID.
        """
        resource = self._known_claim(claim)
        _require(claim in self._normal_attempts and claim not in self._retired
                 and claim in self._confirmations, "Original confirmed ownership and removal attempt required")
        confirmation = stable.decode(self.journal.read(self._confirmations[claim]))["confirmation"]
        record, raw = self._record(remove_record, prior=False)
        target = confirmation["resource_id"] if resource["kind"] == "container" else resource["name"]
        allowed: tuple[list[Any], ...]
        if resource["kind"] == "container":
            allowed = (self._base_argv() + ["rm", "--force", target], self._base_argv() + ["rm", target])
        else:
            allowed = (self._base_argv() + ["volume", "rm", target],)
        _require(record.get("argv") in allowed and raw.strip() == target.encode("ascii"),
                 "Normal remove command or response differs from owned resource")
        self._absence(resource["name"], resource["kind"], absence_record, prior=False)
        name = claim.removesuffix(".json") + "-retired.json"
        value = {"claim": claim, "resource_id": target, "remove_record": remove_record,
                 "absence_record": absence_record, "removal_acknowledged": True}
        self.journal.retain(name, _encoded(value), cleanup=True)
        self._retired[claim] = name

    def _volume_identity(self, value: Any, resource: dict[str, Any]) -> dict[str, Any]:
        _require(type(value) is dict and value.get("Name") == resource["name"]
                 and value.get("Driver") == resource["driver"] and value.get("Labels") == resource["labels"]
                 and value.get("Options") == resource["options"]
                 and all(type(value.get(key)) is str and bool(value[key]) for key in ("CreatedAt", "Mountpoint", "Scope")),
                 "Full volume ownership identity differs")
        return {key: value[key] for key in ("Name", "Driver", "Labels", "Options", "CreatedAt", "Mountpoint", "Scope")}

    def _request(self, label: str, method: str, path: str) -> tuple[int, bytes]:
        _require(not self._uncertain, "Cleanup diagnostics unavailable")
        self._roots()
        now = time.monotonic()
        _require(now < self._deadline, "Cleanup total deadline exhausted")
        return engine._control(self.endpoint, method, path,
            deadline=min(self._deadline, now + self.limits.request_seconds), retain=self._retain, label=label)

    def _resource(self, claim_name: str, ordinal: int, *, containers_clear: bool) -> ResourceDisposition:
        claim = stable.decode(self._prior(claim_name))
        _require(claim.get("protocol") == PROTOCOL and claim.get("channel_sha256") == _sha(_encoded(self._context)),
                 "Cleanup claim context differs")
        resource = claim["resource"]
        kind, name = resource["kind"], resource["name"]
        self._absence(name, kind, claim["preabsence_record"], prior=True)
        confirmation = None
        if claim_name in self._confirmations:
            confirmed = stable.decode(self._prior(self._confirmations[claim_name]))
            _require(confirmed.get("claim") == claim_name, "Cleanup confirmation claim differs")
            confirmation = confirmed["confirmation"]
        label = "resource-" + str(ordinal).zfill(4)
        if not containers_clear and (kind == "volume" or resource.get("labels", {}).get("gossip.role") in ("keeper", "volume-keeper")):
            return ResourceDisposition(claim_name, kind, name, "unresolved",
                error="Finite container cleanup incomplete; state keeper/volume retained")
        resource_id = confirmation["resource_id"] if kind == "container" and confirmation is not None else None
        target = resource_id or name
        path = "/containers/" + target + "/json" if kind == "container" else "/volumes/" + name
        status, body = self._request(label + "-inspect", "GET", path)
        retired = False
        if claim_name in self._retired:
            retirement = stable.decode(self._prior(self._retired[claim_name]))
            expected_target = resource_id if kind == "container" else name
            _require(retirement.get("claim") == claim_name and retirement.get("resource_id") == expected_target
                     and retirement.get("removal_acknowledged") is True, "Original retirement differs")
            # The retirement record was only written after both original control
            # facts were authenticated. Reread them as prior facts as well.
            self._record(retirement["remove_record"], prior=True)
            self._absence(name, kind, retirement["absence_record"], prior=True)
            retired = True
        if claim_name in self._normal_attempts and not (retired and status == 404):
            return ResourceDisposition(claim_name, kind, name, "prior-removal-unresolved", resource_id,
                error="Normal removal already attempted; no retry or upgrade from its unavailable response")
        if status == 404:
            known = confirmation is not None
            return ResourceDisposition(claim_name, kind, name, "already-absent" if known else "unknown-create-absent", resource_id,
                error=None if known else "Unconfirmed create is not proven absent ownership")
        _require(status == 200, "Fresh resource inspection incomplete")
        value = engine.strict_json_loads(body)
        if kind == "container":
            _require(type(value) is dict and type(value.get("Id")) is str
                     and _SHA.fullmatch(value["Id"]) is not None
                     and (resource_id is None or value["Id"] == resource_id)
                     and value.get("Name") == "/" + name and value.get("Image") == resource["image_id"]
                     and type(value.get("Config")) is dict and value["Config"].get("Labels") == resource["labels"]
                     and value.get("Path") == resource["argv"][0] and value.get("Args") == resource["argv"][1:],
                     "Fresh container identity differs; refusing removal")
            resource_id = value["Id"]
            path = "/containers/" + resource_id
            deletion = path + "?force=1&v=0"
            absent_path = path + "/json"
        else:
            _require(confirmation is not None, "Volume lacks acknowledged identity baseline")
            assert confirmation is not None
            identity = self._volume_identity(value, resource)
            _require(identity == confirmation["baseline"], "Volume identity changed; refusing removal")
            deletion, absent_path = path, path
        self._retain(label + "-removal-intent.json", _encoded({"claim": claim_name,
            "resource_id": resource_id, "inspection_sha256": _sha(body), "method": "DELETE", "path": deletion}))
        try:
            status, _ = self._request(label + "-remove", "DELETE", deletion)
            _require(status == 204, "Removal response did not confirm deletion")
            status, _ = self._request(label + "-absence", "GET", absent_path)
            _require(status == 404, "Post-removal exact identity absence unproven")
        except (ValueError, OSError, RuntimeError) as error:
            return ResourceDisposition(claim_name, kind, name, "removal-unresolved", resource_id, True,
                                       type(error).__name__ + ":" + str(error)[:512])
        return ResourceDisposition(claim_name, kind, name, "removed", resource_id, True)

    def run(self, *, reason: str) -> CleanupResult:
        self._owner()
        _require(not self._ran and not self._busy, "Emergency cleanup is at most once")
        _require(type(reason) is str and 0 < len(reason) <= 4096, "Original failure reason required")
        self._ran = self._busy = True
        self._deadline = time.monotonic() + self.limits.total_seconds
        dispositions: list[ResourceDisposition] = []
        uncertainties: list[str] = []
        try:
            _require(self._prior(_CHANNEL) == _encoded(self._context), "Original cleanup context differs")
            self._retain("emergency-intent.json", _encoded({"reason": reason, "context_sha256": self._context["context_sha256"],
                "last_acknowledged_prefix": asdict(self.journal.commitment), "claims": self._claims,
                "normal_removal_attempts": sorted(self._normal_attempts), "acceptance_authority": False}))
            current = engine.runtime_identity(self.endpoint, self._context["image_id"], retain=self._retain,
                label="runtime", timeout_seconds=min(self.limits.request_seconds, max(0.001, self._deadline - time.monotonic())))
            _require(current == self._context["runtime"], "Fresh runtime identity differs")
            ordered = list(reversed(self._claims))
            # Finite roles precede keepers, which precede volumes, irrespective
            # of admission order. An unresolved child preserves state lifetime.
            def resource_order(name: str) -> int:
                resource = stable.decode(self._prior(name))["resource"]
                if resource["kind"] == "volume":
                    return 2
                return int(resource["labels"].get("gossip.role") in ("keeper", "volume-keeper"))
            ordered.sort(key=resource_order)
            containers_clear = True
            for ordinal, claim_name in enumerate(ordered):
                try:
                    disposition = self._resource(claim_name, ordinal, containers_clear=containers_clear)
                except (ValueError, OSError, RuntimeError) as error:
                    disposition = ResourceDisposition(claim_name, "unknown", "unknown", "unresolved",
                        error=type(error).__name__ + ":" + str(error)[:512])
                dispositions.append(disposition)
                clear = disposition.status in ("removed", "already-absent")
                if disposition.kind != "volume":
                    containers_clear = containers_clear and clear
                if not clear:
                    uncertainties.append(claim_name + ":" + disposition.status + ":" + (disposition.error or ""))
                try:
                    self._retain("resource-" + str(ordinal).zfill(4) + "-disposition.json", _encoded(asdict(disposition)))
                except (ValueError, OSError, RuntimeError) as error:
                    uncertainties.append("diagnostics:" + type(error).__name__ + ":" + str(error)[:512])
                    break
        except (ValueError, OSError, RuntimeError) as error:
            uncertainties.append("cleanup:" + type(error).__name__ + ":" + str(error)[:512])
        finally:
            self._busy = False
        complete = len(dispositions) == len(self._claims) and not uncertainties
        result = CleanupResult(reason, tuple(dispositions), tuple(uncertainties), complete, str(self.root))
        try:
            self._retain("result.json", _encoded(asdict(result)))
        except (ValueError, OSError, RuntimeError) as error:
            result = CleanupResult(reason, tuple(dispositions), tuple([*uncertainties,
                "result-retention:" + type(error).__name__ + ":" + str(error)[:512]]), False, str(self.root))
        return result

    def close(self) -> None:
        if not self._closed:
            self._owner()
            self._closed = True
            self._stack.close()
