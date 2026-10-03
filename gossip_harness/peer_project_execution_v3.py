"""Durable host-owned bridge from physical Docker evaluation to public review.

Candidate programs execute only through the unchanged BlackboxValidator. The
adapter records intent before execution, retains the original receipt, and never
re-executes an ambiguous intent. A receipt survives publication failure and can
be republished with the same mesh command. Fixture mode exercises persistence and
shape checks, but cannot produce PublicExecutionEvidence.

Git/source identity has three deliberately distinct encodings: Git objects;
NamedSource hashes of raw UTF-8 file bytes; and the validator's ensure_ascii=True
JSON source digest. CandidateOffer's ensure_ascii=False digest is also retained,
never confused with the validator digest. Public checks are ordered, immutable
whole-case assertions. This component neither promotes Git nor decides whether
an unsuccessful implementation should be repaired.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import subprocess
import threading
from typing import Any, Callable, Protocol, Sequence
import uuid

from . import blackbox_validator, sandbox
from .blackbox_validator import BlackboxValidator, json_equal
from .gitstore import GitStore, _run
from .peer_candidate_v2 import named_sources
from .peer_coding_dispatch_v1 import source_digest
from .peer_project_contract_v2 import (
    Context, EvidenceRef, NamedSource, ReleaseTarget, from_dict, identifier, sha256, to_dict,
)
from .peer_review_release_v2 import (
    PublicExecutionEvidence, RequiredCheck, RequiredSuite, suite_digest,
)
from .peer_store_v1 import _json_value, strict_loads

PROTOCOL = "peer-project-execution-v3"
MAX_ARTIFACT_BYTES = 8 * 1024 * 1024
MAX_CASE_BYTES = 4 * 1024 * 1024
MAX_EXECUTIONS = 1024
_OID = re.compile(r"[0-9a-f]{40}\Z")
_IMAGE = re.compile(r"sha256:[0-9a-f]{64}\Z")
_PURPOSES = {"public_release", "independent_acceptance", "repeatability"}
_RECEIPT_FIELDS = {
    "schema_version", "protocol", "passed", "status", "container_name", "image_id",
    "adapter_sha256", "source_sha256", "suite_sha256", "staging", "case_count",
    "exit_code", "timed_out", "timeout_seconds", "case_timeout_seconds",
    "cleanup_verified", "output_truncated", "input_delivery_failed", "outcomes",
    "output_bytes", "output_sha256", "runtime_seconds",
}
_INFRA = {"sandbox_error", "timeout", "output_error", "cleanup_failed", "output_limit", "invalid_output"}
_CASE_FAILURES = {"error", "timeout", "output_error", "output_limit", "invalid_output"}


class ExecutionError(ValueError):
    """The retained physical execution does not match the requested assertion."""


class ExecutionUnknown(ExecutionError):
    """Execution was intended but no complete original receipt was retained."""


class ExecutionTransport(Protocol):
    node_id: str

    def publish(self, kind: str, payload: bytes, command_id: str) -> EvidenceRef: ...

    def resolve(self, ref: EvidenceRef) -> bytes | None: ...


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ExecutionError(message)


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _plain(value: Any) -> Any:
    if type(value) is tuple:
        return [_plain(item) for item in value]
    if type(value) is list:
        return [_plain(item) for item in value]
    if type(value) is dict:
        return {key: _plain(item) for key, item in value.items()}
    return value


def _json(value: Any) -> bytes:
    # Blackbox permits depth 64 within each JSON value. Our retained envelopes
    # add several levels; they must not discard an otherwise valid observation.
    plain = _plain(value)
    _json_value(plain, max_depth=80)
    raw = json.dumps(plain, ensure_ascii=False, allow_nan=False, sort_keys=True,
                     separators=(",", ":")).encode("utf-8")
    _require(len(raw) <= MAX_ARTIFACT_BYTES, "Retained execution artifact exceeds its byte bound")
    return raw


def _parse(raw: bytes) -> Any:
    _require(type(raw) is bytes and len(raw) <= MAX_ARTIFACT_BYTES, "Invalid retained execution bytes")

    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            _require(key not in result, "Duplicate retained JSON field")
            result[key] = value
        return result

    def nonfinite(value: str) -> Any:
        raise ExecutionError("Nonfinite retained JSON value")

    value = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs, parse_constant=nonfinite)
    _require(_json(value) == raw, "Noncanonical retained execution artifact")
    return value


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _blackbox_json(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=True, allow_nan=False, sort_keys=True,
                      separators=(",", ":")).encode("utf-8")


def _finite(value: Any) -> bool:
    return type(value) in (int, float) and math.isfinite(value)


def _read(path: Path) -> tuple[Any, bytes]:
    _require(not path.is_symlink() and path.is_file() and path.stat().st_size <= MAX_ARTIFACT_BYTES,
             "Missing, unsafe or oversized retained execution artifact")
    raw = path.read_bytes()
    value = _parse(raw)
    return value, raw


def _save(path: Path, value: Any) -> None:
    """Atomically install immutable bytes without overwriting partial evidence."""
    raw = _json(value)
    if path.exists() or path.is_symlink():
        _require(_read(path)[1] == raw, "Retained execution artifact changed")
        return
    temporary = path.with_name(path.name + ".pending-" + uuid.uuid4().hex)
    with temporary.open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    try:
        os.link(temporary, path)
    except FileExistsError:
        _require(_read(path)[1] == raw, "Concurrent execution artifact differs")
    temporary.unlink()
    _sync_directory(path.parent)


def assertion_digest(case: dict[str, Any]) -> str:
    """Whole normalized case, with the Blackbox validator's ASCII JSON encoding."""
    return _sha(b"peer-project-execution-v3:assertion\0" + _blackbox_json(case))


def _cases(cases: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    _require(isinstance(cases, (list, tuple)) and 1 <= len(cases) <= 256,
             "Expected a bounded nonempty ordered public suite")
    for case in cases:
        _require(type(case) is dict and {"id", "input", "expected"} <= set(case)
                 and set(case) <= {"id", "input", "expected", "requirement"},
                 "Public cases require closed whole-case assertions")
        identifier(case["id"])
        if "requirement" in case:
            identifier(case["requirement"])
    _require(len({case["id"] for case in cases}) == len(cases), "Duplicate public check identifier")
    _, normalized, _ = BlackboxValidator._inputs({"solution.py": ""}, cases)
    _require(len(_blackbox_json(normalized)) <= MAX_CASE_BYTES, "Public suite exceeds retained byte bound")
    # This additionally proves the public transport input bound before intent.
    request = {"protocol": blackbox_validator.PROTOCOL, "case_timeout_seconds": 2,
               "cases": [{"index": i, "input": case["input"]} for i, case in enumerate(normalized)]}
    _require(len(_blackbox_json(request)) <= blackbox_validator.MAX_INPUT_BYTES,
             "Public input batch exceeds the validator bound")
    return normalized


def required_suite(context: Context, cases: Sequence[dict[str, Any]]) -> RequiredSuite:
    return RequiredSuite(context, tuple(RequiredCheck(case["id"], assertion_digest(case))
                                        for case in _cases(cases)))


@dataclass(frozen=True, slots=True)
class ExecutionPolicy:
    image_id: str
    timeout_seconds: float = 60
    case_timeout_seconds: float = 2
    seed: int = 0
    purpose: str = "public_release"

    def __post_init__(self) -> None:
        _require(type(self.image_id) is str and _IMAGE.fullmatch(self.image_id) is not None,
                 "A pinned local Docker image ID is required")
        _require(_finite(self.timeout_seconds) and 0 < self.timeout_seconds <= 3600
                 and _finite(self.case_timeout_seconds) and 0 < self.case_timeout_seconds <= 300,
                 "Execution time limits must be bounded and positive")
        _require(type(self.seed) is int and 0 <= self.seed < 2 ** 63, "Invalid execution seed")
        _require(self.purpose in _PURPOSES, "Unregistered execution purpose")


@dataclass(frozen=True, slots=True)
class ExecutionSubject:
    context: Context
    commit_oid: str
    tree_oid: str
    sources: tuple[NamedSource, ...]
    suite_sha256: str
    evaluator_sha256: str
    provenance_sha256: str
    purpose: str = "public_release"

    def __post_init__(self) -> None:
        _require(type(self.context) is Context, "Invalid execution context")
        _require(type(self.commit_oid) is str and _OID.fullmatch(self.commit_oid) is not None
                 and type(self.tree_oid) is str and _OID.fullmatch(self.tree_oid) is not None,
                 "Execution requires full SHA1 commit and tree identifiers")
        _require(type(self.sources) is tuple and 1 <= len(self.sources) <= 128
                 and all(type(source) is NamedSource for source in self.sources), "Invalid named sources")
        _require(self.sources == tuple(sorted(self.sources, key=lambda source: source.path))
                 and len({source.path for source in self.sources}) == len(self.sources),
                 "Named sources must be complete, unique and sorted")
        for digest in (self.suite_sha256, self.evaluator_sha256, self.provenance_sha256):
            sha256(digest)
        _require(self.purpose in _PURPOSES, "Unregistered execution purpose")


def _subject(value: Any) -> ExecutionSubject:
    _require(type(value) is dict and set(value) == set(ExecutionSubject.__dataclass_fields__),
             "Invalid retained execution subject")
    return ExecutionSubject(from_dict(Context, value["context"]), value["commit_oid"], value["tree_oid"],
                            tuple(from_dict(NamedSource, source) for source in value["sources"]),
                            value["suite_sha256"], value["evaluator_sha256"], value["provenance_sha256"],
                            value["purpose"])


@dataclass(frozen=True, slots=True)
class ExecutionPublication:
    request_id: str
    subject: ExecutionSubject
    receipt_ref: EvidenceRef
    check_results: tuple[tuple[str, str], ...]
    status: str
    physically_executed: bool
    replayed: bool


class ProjectExecution:
    """Single-owner local evidence authority; its callbacks are host capabilities.

    Physical mode instantiates the real validator internally and observes Docker
    runtime/image identity. Fixture mode never contacts Docker and takes an
    explicitly trusted test executor; it is rejected by the release verifier.
    Runtime/config changes require a new root. Reopening a physical root probes
    the same runtime again. Replays read retained evidence, not a new evaluation.
    The seed is an input-contract identity; this adapter does not claim it seeds
    arbitrary candidate code. Cases must supply any required random seed.
    """

    def __init__(self, root: Path, transport: ExecutionTransport, store: GitStore, *,
                 producer: str, execution_contract_sha256: str, cohort_id: str,
                 policy: ExecutionPolicy, mode: str = "physical",
                 fixture_executor: Callable[[dict[str, str], list[dict[str, Any]]], dict[str, Any]] | None = None,
                 crash_hook: Callable[[str], None] | None = None):
        identifier(producer)
        identifier(cohort_id)
        sha256(execution_contract_sha256)
        _require(type(policy) is ExecutionPolicy and mode in {"physical", "fixture"}, "Invalid execution mode")
        _require(transport.node_id == producer, "Only the registered service can publish execution receipts")
        _require((mode == "fixture" and callable(fixture_executor))
                 or (mode == "physical" and fixture_executor is None), "Fixture executors cannot authorize physical runs")
        self.root = Path(root).absolute()
        _require(self.root.resolve() == self.root and not self.root.is_symlink(), "Execution root must be canonical")
        self.root.mkdir(parents=True, exist_ok=True)
        self.transport, self.store, self.producer = transport, store, producer
        self.policy, self.mode, self.fixture_executor, self.crash_hook = policy, mode, fixture_executor, crash_hook
        self.execution_contract_sha256, self.cohort_id = execution_contract_sha256, cohort_id
        self.lock, self.closed = threading.RLock(), False
        lock_path = self.root / "owner.lock"
        _require(not lock_path.is_symlink(), "Unsafe execution owner lock")
        self.owner = lock_path.open("a+b")
        try:
            fcntl.flock(self.owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            self.owner.close()
            raise ExecutionError("Execution journal already has an owner") from error
        try:
            self.validator = BlackboxValidator(policy.image_id, timeout_seconds=policy.timeout_seconds,
                                              case_timeout_seconds=policy.case_timeout_seconds)
            # Bind the complete local execution/verification helper boundary,
            # including the independent Git and two source-digest encodings.
            source_names = ("blackbox_validator", "sandbox", "gitstore", "peer_candidate_v2",
                "peer_coding_dispatch_v1", "peer_project_contract_v2", "peer_review_release_v2",
                "peer_store_v1", "peer_project_execution_v3")
            sources = {name: _sha(Path(__file__).with_name(name + ".py").read_bytes()) for name in source_names}
            self.evaluator_sha256 = _sha(_json({"protocol": PROTOCOL, "sources": sources,
                                               "adapter_sha256": self.validator._sandbox.checks_sha256}))
            self.runtime = self._runtime() if mode == "physical" else {"kind": "fixture-no-Docker"}
            self.environment_sha256 = _sha(_json(self.validator._sandbox._environment()))
            arguments = self.validator._sandbox._arguments("EXECUTION", Path("/WORKSPACE"), Path("/CHECKS"))
            arguments.insert(2, "--interactive")
            self.provenance = {"protocol": PROTOCOL, "mode": mode, "policy": asdict(policy),
                "evaluator_sha256": self.evaluator_sha256, "runtime": self.runtime,
                "environment_sha256": self.environment_sha256, "sandbox_arguments": arguments,
                "host_python": {"implementation": platform.python_implementation(), "version": platform.python_version(),
                                "platform": platform.platform()},
                "limits": {"cases": 256, "source_bytes": blackbox_validator.MAX_SOURCE_BYTES,
                           "input_bytes": blackbox_validator.MAX_INPUT_BYTES,
                           "case_bytes": blackbox_validator.MAX_CASE_BYTES,
                           "output_bytes": blackbox_validator.MAX_OUTPUT_BYTES},
                "seed_semantics": "input-contract-only; candidate random seeds belong in cases",
                "blackbox_protocol": blackbox_validator.PROTOCOL,
                "source_encoding": "Blackbox ASCII JSON; candidate UTF-8 JSON; named raw UTF-8"}
            self.provenance_sha256 = _sha(_json(self.provenance))
            self.config = {"protocol": PROTOCOL, "root": str(self.root), "repository": str(store.path),
                "producer": producer, "execution_contract_sha256": execution_contract_sha256,
                "cohort_id": cohort_id, "provenance": self.provenance,
                "provenance_sha256": self.provenance_sha256, "max_executions": MAX_EXECUTIONS}
            config_path = self.root / "config.json"
            _require(config_path.exists() or set(path.name for path in self.root.iterdir()) == {"owner.lock"},
                     "Foreign or partial execution root")
            _save(config_path, self.config)
            self.jobs = self.root / "executions"
            _require(not self.jobs.is_symlink(), "Unsafe execution journal directory")
            self.jobs.mkdir(exist_ok=True)
            _sync_directory(self.root)
            self._directories()
        except BaseException:
            self.owner.close()
            raise

    def _runtime(self) -> dict[str, Any]:
        env = self.validator._sandbox._environment()
        version = subprocess.run(["docker", "version", "--format", "{{json .Server}}"],
                                 capture_output=True, timeout=15, env=env, check=False)
        inspected = subprocess.run(["docker", "image", "inspect", "--format",
                                    '{{json .}}', self.policy.image_id],
                                   capture_output=True, timeout=15, env=env, check=False)
        _require(version.returncode == 0 and inspected.returncode == 0
                 and len(version.stdout) <= 65536 and len(inspected.stdout) <= MAX_ARTIFACT_BYTES,
                 "Docker runtime or pinned image observation failed")
        server, image = json.loads(version.stdout), json.loads(inspected.stdout)
        _require(type(server) is dict and type(image) is dict and image.get("Id") == self.policy.image_id,
                 "Observed Docker image differs from the immutable policy")
        _require(type(server.get("Version")) is str and bool(server["Version"]), "Missing actual Docker server version")
        return {"server": server, "image": {key: image.get(key) for key in ("Id", "Os", "Architecture", "Variant")}}

    def _open(self) -> None:
        _require(not self.closed, "Execution adapter is closed")
        _require(_read(self.root / "config.json")[1] == _json(self.config), "Execution registration changed")

    def _directories(self) -> tuple[Path, ...]:
        entries = tuple(sorted(self.jobs.iterdir()))
        _require(len(entries) <= MAX_EXECUTIONS, "Execution journal exceeds its bound")
        _require(all(not path.is_symlink() and path.is_dir()
                     and re.fullmatch(r"[0-9a-f]{64}", path.name) for path in entries),
                 "Foreign execution journal entry")
        return entries

    def _crash(self, point: str) -> None:
        if self.crash_hook is not None:
            self.crash_hook(point)

    def _source(self, commit_oid: str) -> tuple[str, dict[str, str], tuple[NamedSource, ...]]:
        _require(type(commit_oid) is str and _OID.fullmatch(commit_oid) is not None,
                 "A full immutable Git commit is required")
        # One exact-object query, one full tree inventory, and one read per blob.
        # No symbolic revision, checkout cache, or duplicate tree walk is used.
        identities = self.store._git("rev-parse", "--show-object-format", f"{commit_oid}^{{commit}}",
                                     f"{commit_oid}^{{tree}}").splitlines()
        _require(len(identities) == 3 and identities[0] == "sha1" and identities[1] == commit_oid
                 and _OID.fullmatch(identities[2]) is not None, "Git commit/tree identity or object format differs")
        tree = identities[2]
        raw = _run(self.store.path, "ls-tree", "-r", "-z", commit_oid).stdout
        files: dict[str, str] = {}
        for entry in raw.split(b"\0"):
            if entry:
                metadata, name = entry.split(b"\t", 1)
                mode, kind, blob = metadata.split(b" ")
                _require(mode in (b"100644", b"100755") and kind == b"blob"
                         and _OID.fullmatch(blob.decode("ascii")) is not None, "Nonordinary Git source is forbidden")
                path = name.decode("utf-8")
                _require(path not in files and len(files) < 128, "Duplicate or excess Git source entry")
                files[path] = _run(self.store.path, "cat-file", "blob", blob.decode("ascii")).stdout.decode("utf-8")
        BlackboxValidator._inputs(files, [{"input": None, "expected": None}])
        return tree, files, named_sources(files)

    def subject(self, context: Context, commit_oid: str, suite: RequiredSuite) -> ExecutionSubject:
        with self.lock:
            self._open()
            _require(type(context) is Context and context == suite.context
                     and context.execution_contract_sha256 == self.execution_contract_sha256
                     and context.cohort_id == self.cohort_id, "Execution context differs from registration")
            tree, _, sources = self._source(commit_oid)
            return ExecutionSubject(context, commit_oid, tree, sources, suite_digest(suite),
                                    self.evaluator_sha256, self.provenance_sha256, self.policy.purpose)

    def _capture(self, subject: ExecutionSubject, suite: RequiredSuite,
                 cases: Sequence[dict[str, Any]]) -> tuple[dict[str, str], list[dict[str, Any]]]:
        normalized = _cases(cases)
        _require(type(subject) is ExecutionSubject and type(suite) is RequiredSuite
                 and required_suite(subject.context, normalized) == suite, "Ordered public assertions differ")
        _require(subject.context.execution_contract_sha256 == self.execution_contract_sha256
                 and subject.context.cohort_id == self.cohort_id, "Execution context differs from registration")
        tree, files, sources = self._source(subject.commit_oid)
        current = ExecutionSubject(subject.context, subject.commit_oid, tree, sources, suite_digest(suite),
                                   self.evaluator_sha256, self.provenance_sha256, self.policy.purpose)
        _require(current == subject, "Execution subject differs from exact Git source or frozen policy")
        BlackboxValidator._inputs(files, normalized)
        request = {"protocol": blackbox_validator.PROTOCOL, "case_timeout_seconds": self.policy.case_timeout_seconds,
                   "cases": [{"index": i, "input": case["input"]} for i, case in enumerate(normalized)]}
        _require(len(_blackbox_json(request)) <= blackbox_validator.MAX_INPUT_BYTES, "Public input batch is oversized")
        return files, normalized

    def _outcomes(self, receipt: Any, files: dict[str, str], cases: list[dict[str, Any]]) -> tuple[tuple[str, str], ...]:
        _require(type(receipt) is dict and set(receipt) == _RECEIPT_FIELDS, "Incomplete original validator receipt")
        _require(type(receipt["schema_version"]) is int and receipt["schema_version"] == 1
                 and receipt["protocol"] == blackbox_validator.PROTOCOL
                 and receipt["image_id"] == self.policy.image_id
                 and receipt["adapter_sha256"] == self.validator._sandbox.checks_sha256,
                 "Original validator protocol/image/adapter differs")
        _require(receipt["source_sha256"] == _sha(_blackbox_json(files))
                 and receipt["suite_sha256"] == _sha(_blackbox_json(cases))
                 and receipt["staging"] == {"files": len(files), "bytes": sum(len(text.encode()) for text in files.values())}
                 and type(receipt["case_count"]) is int and receipt["case_count"] == len(cases),
                 "Original validator source or ordered suite differs")
        _require(type(receipt["container_name"]) is str
                 and re.fullmatch(r"gossip-blackbox-[0-9a-f]{32}", receipt["container_name"]) is not None,
                 "Missing original bounded container identity")
        _require(_finite(receipt["runtime_seconds"]) and receipt["runtime_seconds"] >= 0
                 and _finite(receipt["timeout_seconds"]) and receipt["timeout_seconds"] == self.policy.timeout_seconds
                 and _finite(receipt["case_timeout_seconds"])
                 and receipt["case_timeout_seconds"] == self.policy.case_timeout_seconds,
                 "Original execution timing or limits differ")
        for field in ("passed", "timed_out", "output_truncated", "input_delivery_failed"):
            _require(type(receipt[field]) is bool, "Invalid original execution boolean")
        _require(receipt["cleanup_verified"] is None or type(receipt["cleanup_verified"]) is bool,
                 "Invalid cleanup observation")
        _require(receipt["exit_code"] is None or type(receipt["exit_code"]) is int, "Invalid exit observation")
        _require(type(receipt["output_bytes"]) is int and receipt["output_bytes"] >= 0,
                 "Invalid output byte count")
        sha256(receipt["output_sha256"])
        clean = receipt["status"] in {"passed", "failed"}
        _require(clean or receipt["status"] in _INFRA, "Unknown original execution outcome")
        if clean:
            _require(receipt["cleanup_verified"] is True and receipt["exit_code"] == 0
                     and not any(receipt[field] for field in ("timed_out", "output_truncated", "input_delivery_failed"))
                     and receipt["output_bytes"] <= blackbox_validator.MAX_OUTPUT_BYTES,
                     "Unclean execution cannot provide a correctness judgment")
        else:
            _require(receipt["passed"] is False, "Infrastructure failure cannot pass")
        outcomes = receipt["outcomes"]
        _require(type(outcomes) is list and len(outcomes) == len(cases), "Missing original per-check outcomes")
        results = []
        for index, (outcome, case) in enumerate(zip(outcomes, cases)):
            expected_fields = {"index", "passed", "status", "id"} | ({"requirement"} if "requirement" in case else set())
            _require(type(outcome) is dict and set(outcome) in (expected_fields, expected_fields | {"actual"}),
                     "Invalid original check fields")
            _require(type(outcome["index"]) is int and outcome["index"] == index
                     and outcome["id"] == case["id"] and type(outcome["passed"]) is bool
                     and outcome.get("requirement") == case.get("requirement"), "Original check order or labels differ")
            if clean and outcome["status"] in {"passed", "wrong_answer"}:
                _require("actual" in outcome, "Missing actual blackbox observation")
                passed = json_equal(outcome["actual"], case["expected"])
                _require(outcome["passed"] == passed and outcome["status"] == ("passed" if passed else "wrong_answer"),
                         "Peer assertion disagrees with the host oracle")
            else:
                _require("actual" not in outcome and outcome["passed"] is False
                         and outcome["status"] in (_CASE_FAILURES if clean else {receipt["status"]}),
                         "Unverified outcome cannot pass a mandatory check")
            results.append((case["id"], "passed" if outcome["passed"] else "failed" if clean else "infrastructure_failure"))
        _require(receipt["passed"] == (clean and all(result[1] == "passed" for result in results))
                 and (not clean or receipt["status"] == ("passed" if receipt["passed"] else "failed")),
                 "Aggregate correctness conflicts with ordered observations")
        return tuple(results)

    def _intent(self, request_id: str, subject: ExecutionSubject, suite: RequiredSuite,
                files: dict[str, str], cases: list[dict[str, Any]]) -> dict[str, Any]:
        return {"protocol": PROTOCOL, "request_id": request_id, "subject": asdict(subject),
                "config_sha256": _sha(_json(self.config)), "required_suite": asdict(suite),
                "source_files": files, "cases": cases, "candidate_source_sha256": source_digest(files),
                "blackbox_source_sha256": _sha(_blackbox_json(files)),
                "blackbox_suite_sha256": _sha(_blackbox_json(cases))}

    def _retained(self, directory: Path) -> tuple[dict[str, Any], ExecutionSubject, RequiredSuite, dict[str, str], list[dict[str, Any]]]:
        intent, _ = _read(directory / "intent.json")
        _require(type(intent) is dict and type(intent.get("request_id")) is str,
                 "Invalid retained execution intent")
        identifier(intent["request_id"])
        _require(directory.name == _sha(intent["request_id"].encode()), "Execution request directory differs")
        subject = _subject(intent["subject"])
        cases = _cases(intent["cases"])
        suite = required_suite(subject.context, cases)
        files, cases = self._capture(subject, suite, cases)
        _require(_json(intent) == _json(self._intent(intent["request_id"], subject, suite, files, cases)),
                 "Retained execution intent differs from current exact source/configuration")
        return intent, subject, suite, files, cases

    def _publication(self, directory: Path, *, replayed: bool) -> ExecutionPublication:
        intent, subject, _, files, cases = self._retained(directory)
        receipt_path = directory / "receipt.json"
        if not receipt_path.exists():
            raise ExecutionUnknown("Execution intent has no complete retained receipt; do not reinvoke")
        retained, raw = _read(receipt_path)
        _require(type(retained) is dict and set(retained) == {"protocol", "request_id", "subject", "mode",
                 "provenance_sha256", "original_receipt", "original_receipt_sha256", "intent_sha256"},
                 "Invalid retained physical observation")
        _require(retained["protocol"] == PROTOCOL and retained["request_id"] == intent["request_id"]
                 and retained["subject"] == strict_loads(_json(asdict(subject)), max_bytes=MAX_ARTIFACT_BYTES)
                 and retained["mode"] == self.mode and retained["provenance_sha256"] == self.provenance_sha256
                 and retained["intent_sha256"] == _sha(_json(intent))
                 and retained["original_receipt_sha256"] == _sha(_json(retained["original_receipt"])),
                 "Retained original receipt binding differs")
        checks = self._outcomes(retained["original_receipt"], files, cases)
        command = "execution-" + _sha(_json({"protocol": PROTOCOL, "config": _sha(_json(self.config)),
                                            "request_id": intent["request_id"], "payload_sha256": _sha(raw)}))
        publication_path = directory / "publication.json"
        if publication_path.exists():
            value, _ = _read(publication_path)
            _require(type(value) is dict and set(value) == {"command_id", "receipt_ref"}
                     and value["command_id"] == command, "Retained execution publication differs")
            ref = from_dict(EvidenceRef, value["receipt_ref"])
        else:
            ref = self.transport.publish("execution-receipt", raw, command)
            self._crash("after_publish")
            _save(publication_path, {"command_id": command, "receipt_ref": to_dict(ref)})
        _require(type(ref) is EvidenceRef and ref.producer == self.producer and ref.kind == "execution-receipt"
                 and ref.payload_sha256 == _sha(raw) and self.transport.resolve(ref) == raw,
                 "Exact service-owned execution publication is absent or altered")
        return ExecutionPublication(intent["request_id"], subject, ref, checks, retained["original_receipt"]["status"],
                                    self.mode == "physical" and not replayed, replayed)

    def execute(self, request_id: str, subject: ExecutionSubject, suite: RequiredSuite,
                cases: Sequence[dict[str, Any]]) -> ExecutionPublication:
        with self.lock:
            self._open()
            identifier(request_id)
            files, normalized = self._capture(subject, suite, cases)
            intent = self._intent(request_id, subject, suite, files, normalized)
            directory = self.jobs / _sha(request_id.encode())
            if directory.exists():
                _require(not directory.is_symlink() and directory.is_dir(), "Unsafe retained execution directory")
                _require(_read(directory / "intent.json")[1] == _json(intent), "Request ID replay changed its execution identity")
                return self._publication(directory, replayed=True)
            _require(len(self._directories()) < MAX_EXECUTIONS, "Execution admission bound exhausted")
            if self.mode == "physical":
                _require(self._runtime() == self.runtime
                         and _sha(_json(self.validator._sandbox._environment())) == self.environment_sha256,
                         "Actual runtime/environment changed before execution")
            directory.mkdir()
            _save(directory / "intent.json", intent)
            # Fsync the parent as well: a durable file in a newly created child
            # is not a discoverable intent until the child's directory entry is durable.
            _sync_directory(self.jobs)
            self._crash("after_intent")
            # A raised evaluator error leaves ambiguous intent, never a fabricated
            # failed receipt or an automatically repeatable correctness outcome.
            if self.mode == "physical":
                receipt = self.validator.evaluate(files, normalized)
            else:
                assert self.fixture_executor is not None
                receipt = self.fixture_executor(files, normalized)
            self._crash("after_evaluation")
            retained = {"protocol": PROTOCOL, "request_id": request_id, "subject": asdict(subject), "mode": self.mode,
                        "provenance_sha256": self.provenance_sha256, "original_receipt": receipt,
                        "original_receipt_sha256": _sha(_json(receipt)), "intent_sha256": _sha(_json(intent))}
            _save(directory / "receipt.json", retained)
            self._crash("after_receipt")
            return self._publication(directory, replayed=False)

    def verified_execution(self, target: ReleaseTarget, suite: RequiredSuite) -> PublicExecutionEvidence:
        """Authenticate original physical proof and exact Git bytes; no caller flags."""
        with self.lock:
            self._open()
            _require(self.mode == "physical" and self.policy.purpose == "public_release",
                     "Fixture/independent observations cannot authorize public release")
            _require(type(target) is ReleaseTarget and type(suite) is RequiredSuite
                     and target.context == suite.context and len(target.execution_receipt_refs) == 1,
                     "Public release requires one complete ordered execution receipt")
            ref = target.execution_receipt_refs[0]
            matches = []
            for directory in self._directories():
                path = directory / "publication.json"
                if path.exists():
                    value, _ = _read(path)
                    if value.get("receipt_ref") == to_dict(ref):
                        matches.append(directory)
            _require(len(matches) == 1, "Receipt has no unique host-owned original execution")
            publication = self._publication(matches[0], replayed=True)
            expected = ExecutionSubject(target.context, target.commit_oid, target.tree_oid, target.sources,
                target.required_suite_sha256, target.evaluator_sha256, self.provenance_sha256, target.purpose)
            _require(publication.subject == expected and expected.suite_sha256 == suite_digest(suite)
                     and publication.receipt_ref == ref, "Release target differs from the physically executed subject")
            _require(publication.status in {"passed", "failed"},
                     "Infrastructure observations are not reusable correctness judgments")
            return PublicExecutionEvidence(expected.context, expected.commit_oid, expected.tree_oid, expected.sources,
                expected.suite_sha256, expected.evaluator_sha256, expected.provenance_sha256, (ref,),
                publication.check_results, expected.purpose)

    def close(self) -> None:
        with self.lock:
            if not self.closed:
                self.closed = True
                self.owner.close()

    def __enter__(self) -> ProjectExecution:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
