"""V5 product-purpose finite CLI executor with independently registered argv.

Explicit fork of frozen v4; the transport and assertion semantics are preserved.
Product authority requires prospective admission and original physical journals.
No historical qualification observation is relabelled as product acceptance.

Each invocation is a fresh container main process. Only immutable source and
inputs are mounted; expected values and recipes remain on the trusted host.
One bounded owned tmpfs volume survives the sequential invocations of a history.
Durable intent plus an external checkpoint prevents uncertain redispatch locally.
"""
from __future__ import annotations

import base64
import binascii
from dataclasses import asdict, dataclass
import fcntl
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import tempfile
import threading
from typing import Any, Callable
import uuid

from . import candidate_cli_acceptance_profile_v1 as cases
from . import candidate_observation_admission_v1 as admission
from . import project_acceptance_registry_v1 as registry
from . import candidate_client_observer_v5 as observer
from . import candidate_client_process_v4 as process_transport
from .candidate_release_execution_v2 import capture_git_source, source_manifest
from .gitstore import GitStore
from .sandbox import DockerValidator

FROZEN_V4_SOURCE_SHA256 = "6c3f0b552cc5ab641e30853b7176ba566aa2971ac638263cfe44c0ba2ba787e1"

PROTOCOL = "candidate-client-execution-v5"
SNAPSHOT_PROTOCOL = "docker-owned-finite-cli-tmpfs-keeper-v1"
VOLUME_OPTIONS = {"type": "tmpfs", "device": "tmpfs", "o": "size=32m,mode=1777,nosuid,nodev,noexec"}
MAX_FIXTURE_FILES = 1024
MAX_FIXTURE_BYTES = 8 * 1024 * 1024
MAX_STEPS = 64
MAX_ARGV_BYTES = 65536
MAX_RECORD_BYTES = 32 * 1024 * 1024
MAX_JOURNAL_BYTES = 256 * 1024 * 1024
MAX_JOURNAL_FILES = 8192
CONTROL_LIMIT = 1024 * 1024
SUPPORTED_REQUIREMENTS_SHA256 = frozenset((cases.NORMATIVE_SHA256["library-cumulative-product-v2.json"],))
COMPATIBILITY_PLAN = "analysis/candidate-b03-runtime-compatibility-plan-v1.json"
MOUNT_INVENTORY_PLAN = "analysis/candidate-b03-mount-inventory-plan-v1.json"
EMPTY_COMMAND_PLAN = "analysis/candidate-b03-empty-command-plan-v3.json"
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_OID = re.compile(r"[0-9a-f]{40}\Z")
_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")


class ExecutionError(ValueError):
    """Controller registration or retained evidence is invalid."""


class ExecutionUnknown(ExecutionError):
    """A durable intent has no authenticated terminal; never redispatch."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ExecutionError(message)


def encoded(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def digest(value: Any) -> str:
    return sha256(encoded(value))


def source_sha256(files: dict[str, bytes]) -> str:
    return admission.source_sha256(files)


def evaluator_sources() -> dict[str, str]:
    names = ("candidate_client_execution_v5.py", "candidate_client_process_v4.py",
             "candidate_client_observer_v5.py", "candidate_cli_acceptance_profile_v1.py", "candidate_cli_cases_v1.py",
             "candidate_observation_admission_v1.py", "project_acceptance_registry_v1.py",
             "candidate_client_observation_source_v1.py", "candidate_scope_consumer_v1.py",
             "candidate_release_execution_v2.py", "candidate_release_observer_v2.py", "sandbox.py", "gitstore.py")
    result = {name: sha256(Path(__file__).with_name(name).read_bytes()) for name in names}
    for plan in (COMPATIBILITY_PLAN, MOUNT_INVENTORY_PLAN, EMPTY_COMMAND_PLAN):
        result[plan] = sha256((Path(__file__).resolve().parents[1] / plan).read_bytes())
    return result


def startup_policy_binding() -> dict[str, Any]:
    return {"policy_id": process_transport.STARTUP_POLICY_ID,
            "policy_sha256": process_transport.startup_policy_sha256(),
            "definition": process_transport.startup_policy()}


def identity_policy_binding() -> dict[str, Any]:
    return {"policy_id": process_transport.IDENTITY_POLICY_ID,
            "policy_sha256": process_transport.identity_policy_sha256(),
            "definition": process_transport.identity_policy()}


def command_policy_binding() -> dict[str, Any]:
    return {"policy_id": process_transport.COMMAND_POLICY_ID,
            "policy_sha256": process_transport.command_policy_sha256(),
            "definition": process_transport.command_policy()}


def start_response_policy_binding() -> dict[str, Any]:
    return {"protocol": process_transport.START_RESPONSE_PROTOCOL,
            "policy_sha256": process_transport.start_response_policy_sha256(),
            "definition": process_transport.start_response_policy()}


def exact_identity_equal(before: dict[str, Any], after: dict[str, Any]) -> bool:
    """Full deterministic JSON equality preserves false/zero/null distinctions."""
    return encoded(before) == encoded(after)


def _safe_relative(name: Any) -> bool:
    return (type(name) is str and bool(name) and len(name.encode("utf-8")) <= 1024
            and not name.startswith("/") and "\\" not in name and "\x00" not in name
            and all(part and part not in (".", "..") for part in name.split("/")))


def recipe_for(case_id: str) -> tuple[dict[str, Any], dict[str, bytes]]:
    """Validate the closed evaluator recipe without executing candidate code."""
    recipe = cases.execution_recipe(case_id)
    require(type(recipe) is dict and set(recipe) == {"case_id", "fixtures", "directories", "steps"}
            and recipe["case_id"] == case_id, "Invalid closed CLI recipe")
    fixtures, directories, steps = recipe["fixtures"], recipe["directories"], recipe["steps"]
    require(type(fixtures) is dict and len(fixtures) <= MAX_FIXTURE_FILES, "Fixture file bound exceeded")
    require(type(directories) is list and len(directories) <= MAX_FIXTURE_FILES
            and len(set(directories)) == len(directories) and all(_safe_relative(x) for x in directories),
            "Invalid fixture directories")
    files: dict[str, bytes] = {}
    total = 0
    for name, value in fixtures.items():
        require(_safe_relative(name) and type(value) is str
                and len(value) <= 4 * ((MAX_FIXTURE_BYTES + 2) // 3), "Invalid fixture input")
        try:
            raw = base64.b64decode(value, validate=True)
        except (ValueError, binascii.Error):
            raise ExecutionError("Invalid fixture base64") from None
        require(base64.b64encode(raw).decode("ascii") == value, "Noncanonical fixture base64")
        total += len(raw)
        require(total <= MAX_FIXTURE_BYTES, "Fixture byte bound exceeded")
        files[name] = raw
    all_paths = set(files) | set(directories)
    require(not (set(files) & set(directories)), "Fixture file/directory collision")
    for name in all_paths:
        require(not any("/".join(name.split("/")[:i]) in files for i in range(1, len(name.split("/")))),
                "Fixture ancestor is a file")
    require(type(steps) is list and 1 <= len(steps) <= MAX_STEPS, "Invalid finite CLI step count")
    ids = []
    for step in steps:
        require(type(step) is dict and set(step) == {"step_id", "argv"}, "Invalid CLI step")
        sid, argv = step["step_id"], step["argv"]
        require(type(sid) is str and _ID.fullmatch(sid) is not None, "Invalid CLI step ID")
        require(type(argv) is list and 1 <= len(argv) <= 256 and all(type(x) is str and x and "\x00" not in x for x in argv)
                and sum(len(x.encode("utf-8")) for x in argv) <= MAX_ARGV_BYTES, "Invalid literal CLI argv")
        ids.append(sid)
    require(len(set(ids)) == len(ids), "Duplicate CLI step ID")
    return json.loads(encoded(recipe)), files


def fixture_sha256(recipe: dict[str, Any], files: dict[str, bytes]) -> str:
    return digest({"directories": recipe["directories"], "files": source_manifest(files)})


def ordered_suite_sha256() -> str:
    definitions = cases.definitions()
    ids = [item["case_id"] for item in definitions]
    require(bool(ids) and len(set(ids)) == len(ids), "Invalid ordered CLI suite")
    return digest({"definition_sha256": cases.definition_sha256(), "ordered_case_ids": ids})


@dataclass(frozen=True)
class ClientPolicy:
    image_id: str
    command_timeout_seconds: int = 30
    stream_limit_bytes: int = 4 * 1024 * 1024
    frame_limit_bytes: int = 4 * 1024 * 1024
    transport_timeout_seconds: int = 15
    seed: int = 0

    def __post_init__(self) -> None:
        require(type(self.image_id) is str and re.fullmatch(r"sha256:[0-9a-f]{64}", self.image_id) is not None,
                "Pinned image ID required")
        for value, maximum in ((self.command_timeout_seconds, 120), (self.stream_limit_bytes, 4 * 1024 * 1024),
                               (self.frame_limit_bytes, 4 * 1024 * 1024), (self.transport_timeout_seconds, 60)):
            require(type(value) is int and 1 <= value <= maximum, "Invalid observation resource bound")
        require(type(self.seed) is int and 0 <= self.seed < 2 ** 63, "Invalid deterministic fixture seed")

    def process_policy(self) -> Any:
        return process_transport.ProcessPolicy(image_id=self.image_id, timeout_seconds=self.command_timeout_seconds,
            stream_limit_bytes=self.stream_limit_bytes, frame_limit_bytes=self.frame_limit_bytes,
            transport_timeout_seconds=self.transport_timeout_seconds)


@dataclass(frozen=True)
class ClientBinding:
    source_sha256: str
    requirements_sha256: str
    milestone: str
    purpose: str
    case_id: str
    definition_sha256: str
    ordered_suite_sha256: str
    evaluator_sha256: str
    runtime_sha256: str
    environment_sha256: str
    limits_sha256: str
    seed_sha256: str
    protocol: str = PROTOCOL

    def __post_init__(self) -> None:
        for key, value in asdict(self).items():
            if key.endswith("_sha256"):
                require(type(value) is str and _SHA.fullmatch(value) is not None, "Invalid binding digest")
        require(self.requirements_sha256 in SUPPORTED_REQUIREMENTS_SHA256, "Unrecognized frozen product requirements identity")
        require(self.protocol == PROTOCOL and self.purpose in registry.PURPOSES,
                "A prospectively registered product purpose is required")
        require(self.milestone == "M1" and type(self.case_id) is str and _ID.fullmatch(self.case_id) is not None,
                "Only declared M1 finite CLI cases are supported")


@dataclass(frozen=True)
class ClientRegistration:
    binding: ClientBinding
    commit_oid: str
    tree_oid: str
    repetition_id: str
    gate: registry.Gate
    cohort_trajectory_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        require(type(self.binding) is ClientBinding and type(self.commit_oid) is str and type(self.tree_oid) is str
                and _OID.fullmatch(self.commit_oid) is not None and _OID.fullmatch(self.tree_oid) is not None,
                "Exact complete Git registration required")
        require(type(self.repetition_id) is str and _ID.fullmatch(self.repetition_id) is not None,
                "Explicit product observation repetition ID required")
        require(type(self.gate) is registry.Gate and self.gate == gate_for(
            self.gate.binding.subject, self.binding, gate_id=self.gate.gate_id),
            "Prospective gate differs from complete fixed assertion roster")
        registry.identifiers(self.cohort_trajectory_ids)
        require(self.gate.binding.subject.trajectory_id in self.cohort_trajectory_ids,
                "Subject absent from registered cohort")


@dataclass(frozen=True)
class ControllerCheckpoint:
    files: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class ClientHistoryResult:
    execution_id: str
    case_id: str
    status: str
    observations: tuple[Any, ...]
    missing_step_ids: tuple[str, ...]
    cleanup_verified: bool
    infrastructure: tuple[str, ...]
    terminal_sha256: str
    checkpoint: ControllerCheckpoint


def runtime_identity(endpoint: Any, image_id: str, *, timeout_seconds: int = 15) -> dict[str, Any]:
    return process_transport.runtime_identity(endpoint, image_id, timeout_seconds=timeout_seconds)


def binding_for(files: dict[str, bytes], case_id: str, policy: ClientPolicy, runtime: dict[str, Any], *,
                requirements_sha256: str, milestone: str = "M1", purpose: str = "public_release") -> ClientBinding:
    recipe, fixture_files = recipe_for(case_id)
    environment = {"docker_cli_environment_sha256": digest(DockerValidator._environment()), "runtime": runtime,
        "host_python": [platform.python_implementation(), platform.python_version()],
        "candidate_profile": "fresh-finite-main-process-no-init-no-network-readonly-unprivileged",
        "snapshot_protocol": SNAPSHOT_PROTOCOL, "volume_options": VOLUME_OPTIONS}
    limits = {"policy": asdict(policy), "transport_policy": asdict(policy.process_policy()),
        "fixture_files": MAX_FIXTURE_FILES, "fixture_bytes": MAX_FIXTURE_BYTES, "steps": MAX_STEPS,
        "argv_bytes": MAX_ARGV_BYTES, "record_bytes": MAX_RECORD_BYTES, "journal_bytes": MAX_JOURNAL_BYTES,
        "journal_files": MAX_JOURNAL_FILES, "startup_compatibility_policy": startup_policy_binding(),
        "identity_comparison_policy": identity_policy_binding(),
        "command_validation_policy": command_policy_binding(),
        "start_response_policy": start_response_policy_binding(),
        "keeper_lifetime_seconds": keeper_lifetime_seconds(recipe, policy),
        "keeper_command": keeper_command(recipe, policy), "keeper_volume_readonly": True}
    return ClientBinding(source_sha256(files), requirements_sha256, milestone, purpose, case_id,
        digest({"definitions": cases.definition_sha256(), "definition_sources": cases.definition_sources(),
                "case": cases.case_definition(case_id), "recipe": recipe,
                "fixture_sha256": fixture_sha256(recipe, fixture_files)}),
        ordered_suite_sha256(), digest(evaluator_sources()), digest(runtime), digest(environment), digest(limits),
        digest({"seed": policy.seed, "semantics": "fixed-input-recipe; no candidate random seed implied"}))


def gate_for(subject: registry.Subject, binding: ClientBinding, *, gate_id: str) -> registry.Gate:
    """Derive the one-history gate from the exact original execution binding."""
    require(type(subject) is registry.Subject and type(binding) is ClientBinding
            and subject.source_sha256 == binding.source_sha256
            and subject.requirements_sha256 == binding.requirements_sha256
            and subject.milestone == binding.milestone, "Product subject differs from CLI execution")
    ordered_ids = cases.ordered_assertion_ids(binding.case_id)
    gate_binding = registry.Binding(subject,
        digest({"protocol": PROTOCOL, "case_id": binding.case_id,
                "profile_sha256": cases.profile_sha256(), "definition_sha256": binding.definition_sha256,
                "client_ordered_suite_sha256": binding.ordered_suite_sha256, "ordered_case_ids": ordered_ids}),
        binding.evaluator_sha256, binding.runtime_sha256, binding.environment_sha256,
        binding.limits_sha256, binding.seed_sha256, PROTOCOL, binding.purpose)
    return registry.Gate(gate_id, cases.REQUIREMENT_IDS, ordered_ids, gate_binding)


def observation_registration(registration: ClientRegistration) -> admission.ObservationRegistration:
    """Recompute prospective profile/roster mapping, preserving original purpose."""
    require(type(registration) is ClientRegistration, "Actual typed CLI registration required")
    gate = gate_for(registration.gate.binding.subject, registration.binding,
                    gate_id=registration.gate.gate_id)
    require(gate == registration.gate, "Registered gate changed")
    return admission.ObservationRegistration(gate, registration.commit_oid, registration.tree_oid,
        registration.repetition_id, registration.cohort_trajectory_ids,
        registration.binding.definition_sha256, cases.profile_sha256(),
        cases.ORIGINAL_DEFINITION_PURPOSE,
        admission.binding_sha256(registration.binding, gate=gate))


def keeper_lifetime_seconds(recipe: dict[str, Any], policy: ClientPolicy) -> int:
    """Declared observation envelope, not a product completion deadline."""
    return 120 + len(recipe["steps"]) * (policy.command_timeout_seconds + 24 * policy.transport_timeout_seconds + 30)


def keeper_command(recipe: dict[str, Any], policy: ClientPolicy) -> tuple[str, ...]:
    return ("python", "-I", "-c", "import time;time.sleep(" + str(keeper_lifetime_seconds(recipe, policy)) + ")")


def _sync(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _read(path: Path) -> bytes:
    require(path.is_file() and not path.is_symlink() and path.stat().st_size <= MAX_RECORD_BYTES,
            "Missing, unsafe or oversized retained artifact")
    return path.read_bytes()


def _json(path: Path) -> Any:
    raw = _read(path)
    value = json.loads(raw)
    require(encoded(value) == raw, "Retained JSON is not canonical")
    return value


def _verify_tree(root: Path, files: dict[str, bytes], directories: list[str] | None = None) -> None:
    actual: dict[str, bytes] = {}
    actual_dirs: set[str] = set()
    for parent, dirnames, filenames in os.walk(root, followlinks=False):
        for name in dirnames:
            path = Path(parent) / name
            require(not path.is_symlink(), "Staged directory became a symlink")
            actual_dirs.add(path.relative_to(root).as_posix())
        for name in filenames:
            path = Path(parent) / name
            require(path.is_file() and not path.is_symlink() and path.stat().st_size <= MAX_RECORD_BYTES,
                    "Staged file became unsafe")
            actual[path.relative_to(root).as_posix()] = path.read_bytes()
    expected_dirs = set(directories or [])
    for name in [*files, *(directories or [])]:
        expected_dirs.update("/".join(name.split("/")[:i]) for i in range(1, len(name.split("/"))))
    require(actual == files and actual_dirs == expected_dirs, "Staged files or directories changed")


class CandidateClientExecution:
    """One trusted local controller capability for one prospectively admitted CLI history."""

    def __init__(self, root: Path, store: GitStore, registration: ClientRegistration, policy: ClientPolicy, *,
                 endpoint: Any = None, mode: str = "physical", expected_checkpoint: ControllerCheckpoint | None = None,
                 checkpoint_sink: Callable[[ControllerCheckpoint], None] | None = None,
                 admission_authority: admission.ObservationAdmission):
        require(type(registration) is ClientRegistration and type(policy) is ClientPolicy
                and mode in ("physical", "fixture"), "Typed immutable registration required")
        self.root = Path(root).absolute()
        require(self.root.resolve() == self.root and not self.root.is_symlink(), "Canonical journal root required")
        existed = self.root.exists()
        require(not existed or expected_checkpoint is not None, "Existing journal requires external checkpoint")
        require(existed or expected_checkpoint is None, "Checkpoint root missing")
        self.root.mkdir(parents=True, exist_ok=True)
        require(not (self.root / "owner.lock").is_symlink(), "Unsafe owner lock")
        self.owner = (self.root / "owner.lock").open("a+b")
        try:
            fcntl.flock(self.owner.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.owner.close()
            raise ExecutionError("Journal already owned") from None
        self.closed = False
        self._owner_thread = threading.get_ident()
        self._authenticated: dict[str, str] = {}
        self._retained_bytes = 0
        self._keeper_inspection: dict[str, Any] | None = None
        self.store, self.registration, self.policy, self.mode = store, registration, policy, mode
        self.checkpoint_sink = checkpoint_sink
        self.admission = admission_authority
        try:
            require(type(admission_authority) is admission.ObservationAdmission, "Actual prospective admission owner required")
            self.endpoint = (endpoint or process_transport.EngineEndpoint.from_environment()) if mode == "physical" else None
            self.runtime = runtime_identity(self.endpoint, policy.image_id, timeout_seconds=policy.transport_timeout_seconds) if mode == "physical" else {"kind": "fixture-no-Docker"}
            self.sources = evaluator_sources()
            require(self.sources == _LOADED_SOURCES, "Loaded evaluator sources changed before registration")
            self.definition_sources = cases.definition_sources()
            self.tree, self.files = capture_git_source(store, registration.commit_oid)
            require(self.tree == registration.tree_oid and source_sha256(self.files) == registration.binding.source_sha256,
                    "Registered Git source differs")
            self.recipe, self.fixture_files = recipe_for(registration.binding.case_id)
            self.binding = binding_for(self.files, registration.binding.case_id, policy, self.runtime,
                requirements_sha256=registration.binding.requirements_sha256,
                milestone=registration.binding.milestone, purpose=registration.binding.purpose)
            require(self.binding == registration.binding, "Registered evaluator/suite/runtime/environment/limits differ")
            self.observation_registration = observation_registration(registration)
            require(self.admission.registration == self.observation_registration, "Admission belongs to another observation")
            self.config = {"protocol": PROTOCOL, "mode": mode, "root": str(self.root),
                "repository": str(store.path.resolve()), "registration": asdict(registration), "policy": asdict(policy),
                "endpoint": None if self.endpoint is None else asdict(self.endpoint), "runtime": self.runtime,
                "source_manifest": source_manifest(self.files), "definition_sources": self.definition_sources,
                "evaluator_sources": self.sources, "startup_compatibility_policy": startup_policy_binding(),
                "identity_comparison_policy": identity_policy_binding(),
                "command_validation_policy": command_policy_binding(),
                "start_response_policy": start_response_policy_binding(),
                "recipe": self.recipe,
                "fixture_sha256": fixture_sha256(self.recipe, self.fixture_files),
                "observation_registration": asdict(self.observation_registration)}
            if existed:
                require(_json(self.root / "config.json") == json.loads(encoded(self.config)), "Journal config changed")
                require(type(expected_checkpoint) is ControllerCheckpoint and bool(expected_checkpoint.files),
                        "Exact external checkpoint required")
                assert expected_checkpoint is not None
                require(len(dict(expected_checkpoint.files)) == len(expected_checkpoint.files)
                        and self._inventory() == dict(expected_checkpoint.files), "Journal rollback/change/foreign suffix")
                self._authenticated = dict(expected_checkpoint.files)
                self._retained_bytes = sum(path.stat().st_size for path in self.root.iterdir() if path.name != "owner.lock")
            else:
                require({path.name for path in self.root.iterdir()} == {"owner.lock"}, "Foreign journal root")
                self._retain("config.json", encoded(self.config))
                _sync(self.root.parent)
        except BaseException:
            self.close()
            raise

    def _inventory(self) -> dict[str, str]:
        items = list(self.root.iterdir())
        require(len(items) <= MAX_JOURNAL_FILES + 1, "Journal file count exceeded")
        return {path.name: sha256(_read(path)) for path in sorted(items) if path.name != "owner.lock"}

    def _retain(self, name: str, raw: bytes) -> None:
        require(re.fullmatch(r"[a-z][a-z0-9.-]{0,180}", name) is not None
                and type(raw) is bytes and len(raw) <= MAX_RECORD_BYTES, "Invalid retained artifact")
        require(len(self._authenticated) < MAX_JOURNAL_FILES
                and self._retained_bytes + len(raw) <= MAX_JOURNAL_BYTES, "Journal resource bound exceeded")
        with (self.root / name).open("xb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        _sync(self.root)
        self._authenticated[name] = sha256(raw)
        self._retained_bytes += len(raw)
        if self.checkpoint_sink is not None:
            self.checkpoint_sink(self.checkpoint())

    def checkpoint(self) -> ControllerCheckpoint:
        require(not self.closed and threading.get_ident() == self._owner_thread, "Closed or foreign-thread controller")
        require(self._inventory() == self._authenticated, "Journal differs from original controller writes")
        return ControllerCheckpoint(tuple(sorted(self._authenticated.items())))

    def _unchanged(self) -> None:
        self.checkpoint()
        require(evaluator_sources() == self.sources == _LOADED_SOURCES and cases.definition_sources() == self.definition_sources,
                "Evaluator or normative sources changed")
        require(_json(self.root / "config.json") == json.loads(encoded(self.config)), "Controller config changed")
        tree, files = capture_git_source(self.store, self.registration.commit_oid)
        require(tree == self.tree and files == self.files, "Registered immutable Git source changed")
        runtime = runtime_identity(self.endpoint, self.policy.image_id, timeout_seconds=self.policy.transport_timeout_seconds) if self.mode == "physical" else {"kind": "fixture-no-Docker"}
        require(runtime == self.runtime and binding_for(self.files, self.binding.case_id, self.policy, runtime,
            requirements_sha256=self.binding.requirements_sha256, milestone=self.binding.milestone,
            purpose=self.binding.purpose) == self.binding, "Runtime/environment/recipe binding changed")

    def _current_admission(self, freeze: registry.CohortFreeze | None) -> None:
        actual = observation_registration(self.registration)
        require(actual == self.observation_registration, "Original observation registration changed")
        self.admission.check_current(actual, freeze)

    def retained_freeze(self) -> registry.CohortFreeze | None:
        intent = _json(self.root / "intent.json")
        return admission.freeze_from_record(intent["cohort_freeze"])

    def execute_once(self) -> ClientHistoryResult:
        self._unchanged()
        if (self.root / "intent.json").exists():
            if not (self.root / "terminal.json").exists():
                raise ExecutionUnknown("Existing intent has no authenticated terminal; never redispatch")
            return self.verified_execution()
        freeze = self.admission.before_intent(observation_registration(self.registration))
        execution_id = "client-" + uuid.uuid4().hex
        intent = {"protocol": PROTOCOL, "execution_id": execution_id,
            "config_sha256": digest(self.config), "registration_sha256": digest(asdict(self.registration)),
            "observation_registration": asdict(self.observation_registration),
            "cohort_freeze": None if freeze is None else asdict(freeze),
            "volume": "gossip-" + execution_id + "-volume",
            "keeper": "gossip-" + execution_id + "-keeper",
            "keeper_argv": list(keeper_command(self.recipe, self.policy)),
            "keeper_lifetime_seconds": keeper_lifetime_seconds(self.recipe, self.policy),
            "containers": ["gossip-" + execution_id + "-" + str(i) for i in range(len(self.recipe["steps"]))]}
        self._retain("intent.json", encoded(intent))
        require(self.mode == "physical", "Fixture journal cannot dispatch physical evidence")
        self._current_admission(freeze)
        self._dispatch(intent)
        self._unchanged()
        self._current_admission(freeze)
        return self.verified_execution()

    def _command(self, label: str, argv: list[str], timeout: int | None = None) -> dict[str, Any]:
        """Bound control traffic; candidate streams never pass through this helper."""
        require(self.mode == "physical" and self.endpoint is not None, "Control commands require a physical endpoint")
        assert self.endpoint is not None
        self.endpoint.validate()
        require(bool(argv) and argv[0] == "docker", "Only fixed Docker control commands allowed")
        argv = ["docker", "--host", "unix://" + self.endpoint.socket_path, *argv[1:]]
        environment = {key: value for key, value in DockerValidator._environment().items()
                       if key not in ("DOCKER_HOST", "DOCKER_CONTEXT")}
        timeout = self.policy.transport_timeout_seconds if timeout is None else timeout
        child = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                 stdin=subprocess.DEVNULL, env=environment)
        buffers = {"stdout": bytearray(), "stderr": bytearray()}
        counts = {"stdout": 0, "stderr": 0}
        errors: list[str] = []
        lock = threading.Lock()
        def drain(kind: str, pipe: Any) -> None:
            try:
                while chunk := pipe.read(65536):
                    with lock:
                        counts[kind] += len(chunk)
                        remaining = CONTROL_LIMIT - len(buffers[kind])
                        if remaining > 0:
                            buffers[kind].extend(chunk[:remaining])
                        if counts[kind] > CONTROL_LIMIT:
                            child.kill()
            except OSError:
                errors.append(kind)
            finally:
                pipe.close()
        workers = [threading.Thread(target=drain, args=(kind, getattr(child, kind)), daemon=True) for kind in buffers]
        for worker in workers:
            worker.start()
        timed_out = False
        try:
            child.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            child.kill()
            child.wait(timeout=5)
        for worker in workers:
            worker.join(timeout=5)
        with lock:
            data = {kind: bytes(raw) for kind, raw in buffers.items()}
            observed = dict(counts)
        record: dict[str, Any] = {"argv": argv, "exit_code": child.returncode, "timed_out": timed_out,
            "capture_complete": not errors and all(not worker.is_alive() for worker in workers)}
        for kind, raw in data.items():
            name = label + "-" + kind + ".bin"
            self._retain(name, raw)
            record[kind] = {"path": name, "sha256": sha256(raw), "bytes": len(raw),
                            "observed_bytes": observed[kind], "truncated": observed[kind] != len(raw)}
        self._retain(label + ".json", encoded(record))
        return record

    def _raw(self, record: dict[str, Any], kind: str = "stdout") -> bytes:
        item = record[kind]
        require(type(item) is dict and type(item.get("path")) is str
                and re.fullmatch(r"[a-z][a-z0-9.-]{0,180}", item["path"]) is not None, "Invalid raw artifact reference")
        raw = _read(self.root / item["path"])
        require(sha256(raw) == item["sha256"] and len(raw) == item["bytes"], "Raw artifact differs")
        return raw

    @staticmethod
    def _clean(record: dict[str, Any]) -> bool:
        return (record["exit_code"] == 0 and not record["timed_out"] and record["capture_complete"]
                and not record["stdout"]["truncated"] and not record["stderr"]["truncated"])

    def _checked(self, label: str, argv: list[str]) -> dict[str, Any]:
        record = self._command(label, argv)
        require(self._clean(record), "Docker control command incomplete: " + label)
        return record

    def _volume_valid(self, value: Any, intent: dict[str, Any]) -> bool:
        return (type(value) is dict and value.get("Name") == intent["volume"] and value.get("Driver") == "local"
                and value.get("Options") == VOLUME_OPTIONS
                and value.get("Labels") == {"gossip.execution": intent["execution_id"], "gossip.snapshot": SNAPSHOT_PROTOCOL})

    def _create_argv(self, name: str, workspace: Path, inputs: Path, volume: str,
                     step: dict[str, Any], execution_id: str) -> list[str]:
        sandbox = DockerValidator(self.policy.image_id, {"unused.txt": "never mounted"}, command=tuple(step["argv"]))
        argv = sandbox._arguments(name, workspace, inputs)
        argv[1] = "create"
        argv.remove("--rm")
        argv.remove("--init")
        argv[2:2] = ["--attach", "stdout", "--attach", "stderr"]
        argv = [item for item in argv if not item.startswith("--tmpfs=/tmp:")]
        check_mount = f"type=bind,source={inputs},target=/checks,readonly,bind-propagation=rprivate"
        argv[argv.index(check_mount)] = f"type=bind,source={inputs},target=/inputs,readonly,bind-propagation=rprivate"
        index = argv.index("--entrypoint")
        labels = {"gossip.execution": execution_id, "gossip.source": self.binding.source_sha256,
                  "gossip.fixture": self.config["fixture_sha256"], "gossip.step": step["step_id"]}
        extra = ["--mount", f"type=volume,source={volume},target=/tmp,volume-nocopy"]
        for key, value in labels.items():
            extra.extend(("--label", key + "=" + value))
        argv[index:index] = extra
        return argv

    def _validate_created(self, value: Any, *, container_id: str, name: str, workspace: Path,
                          inputs: Path, intent: dict[str, Any], step: dict[str, Any]) -> None:
        """Match daemon configuration against our recipe before treating it as baseline."""
        labels = {"gossip.execution": intent["execution_id"], "gossip.source": self.binding.source_sha256,
                  "gossip.fixture": self.config["fixture_sha256"], "gossip.step": step["step_id"]}
        require(type(value) is dict and value.get("Id") == container_id and value.get("Name") == "/" + name
                and value.get("Image") == self.policy.image_id and value.get("Path") == step["argv"][0]
                and value.get("Args") == step["argv"][1:] and value.get("Config", {}).get("Labels") == labels,
                "Created container differs from registered source/recipe/identity")
        mounts = value.get("Mounts")
        require(type(mounts) is list and len(mounts) == 3 and all(type(mount) is dict for mount in mounts),
                "Created mount census differs")
        by_target = {mount.get("Destination"): mount for mount in mounts}
        require(set(by_target) == {"/workspace", "/inputs", "/tmp"}
                and by_target["/workspace"].get("Source") == str(workspace)
                and by_target["/inputs"].get("Source") == str(inputs)
                and by_target["/tmp"].get("Name") == intent["volume"], "Created mounts do not match staging/volume")
        process_transport.validate_sandbox(value, self.policy.process_policy(),
            expected_argv=step["argv"], runtime=self.runtime)

    def _keeper_create_argv(self, name: str, workspace: Path, inputs: Path, intent: dict[str, Any]) -> list[str]:
        sandbox = DockerValidator(self.policy.image_id, {"unused.txt": "never mounted"},
                                  command=keeper_command(self.recipe, self.policy))
        original = sandbox._arguments(name, workspace, inputs)
        argv = ["docker", "create"]
        index = 2
        while index < len(original):
            value = original[index]
            if value == "--mount":
                index += 2  # Keeper has no source/input/check bind mounts.
                continue
            if value in ("--rm", "--init") or value.startswith("--tmpfs=/tmp:"):
                index += 1
                continue
            argv.append("--workdir=/" if value == "--workdir=/workspace" else value)
            index += 1
        entry = argv.index("--entrypoint")
        argv[entry:entry] = ["--mount", "type=volume,source=" + intent["volume"] + ",target=/tmp,readonly,volume-nocopy",
            "--label", "gossip.execution=" + intent["execution_id"], "--label", "gossip.role=volume-keeper"]
        return argv

    def _keeper_state(self, label: str, intent: dict[str, Any], container_id: str, *, running: bool = True) -> dict[str, Any]:
        record = self._checked(label, ["docker", "inspect", "--format", "{{json .}}", container_id])
        value = process_transport.strict_json_loads(self._raw(record))
        require(type(value) is dict and value.get("Id") == container_id and value.get("Name") == "/" + intent["keeper"]
                and value.get("Image") == self.policy.image_id and value.get("Path") == intent["keeper_argv"][0]
                and value.get("Args") == intent["keeper_argv"][1:], "Keeper identity/argv differs")
        config, state, mounts, host = value.get("Config", {}), value.get("State", {}), value.get("Mounts", []), value.get("HostConfig", {})
        require(all(type(item) is dict for item in (config, state, host)), "Malformed keeper configuration")
        require(config.get("Image") == self.policy.image_id and config.get("User") == "65534:65534"
                and config.get("WorkingDir") == "/" and config.get("Entrypoint") == intent["keeper_argv"][:1]
                and config.get("Cmd") == intent["keeper_argv"][1:] and config.get("Tty") is False
                and config.get("OpenStdin") is False and config.get("StdinOnce") is False
                and config.get("Healthcheck") == {"Test": ["NONE"]}
                and config.get("Labels") == {"gossip.execution": intent["execution_id"], "gossip.role": "volume-keeper"},
                "Keeper command/configuration differs")
        expected_host = {"NetworkMode": "none", "ReadonlyRootfs": True, "Privileged": False,
            "AutoRemove": False, "Memory": 256 * 1024 * 1024, "MemorySwap": 256 * 1024 * 1024,
            "NanoCpus": 1000000000, "PidsLimit": 64, "CapDrop": ["ALL"],
            "SecurityOpt": ["no-new-privileges:true"], "LogConfig": {"Type": "none", "Config": {}},
            "RestartPolicy": {"Name": "no", "MaximumRetryCount": 0},
            "Ulimits": [{"Name": "nofile", "Hard": 256, "Soft": 256}]}
        process_transport.validate_oom_kill_default(host)
        require(all(key in host and encoded(host[key]) == encoded(expected) for key, expected in expected_host.items())
                and (host.get("Init") is None or host.get("Init") is False) and host.get("IpcMode") in ("", "private")
                and host.get("CgroupnsMode") in ("", "private")
                and all(not host.get(key) for key in ("CapAdd", "Devices", "DeviceRequests", "VolumesFrom", "Links",
                    "PortBindings", "PublishAllPorts", "PidMode", "UTSMode", "UsernsMode")), "Keeper sandbox restrictions differ")
        environment = config.get("Env")
        require(type(environment) is list and all(type(item) is str and "=" in item for item in environment), "Invalid keeper environment")
        env = dict(item.split("=", 1) for item in environment)
        require(len(env) == len(environment) and env.get("HOME") == "/tmp"
                and env.get("PYTHONDONTWRITEBYTECODE") == "1" and env.get("PYTHONNOUSERSITE") == "1"
                and all(env.get(key) == "" for key in ("HTTP_PROXY", "HTTPS_PROXY", "FTP_PROXY", "NO_PROXY", "ALL_PROXY",
                    "http_proxy", "https_proxy", "ftp_proxy", "no_proxy", "all_proxy")), "Keeper credential-free environment differs")
        require(type(mounts) is list and len(mounts) == 1 and type(mounts[0]) is dict
                and mounts[0].get("Type") == "volume" and mounts[0].get("Driver") == "local"
                and mounts[0].get("Name") == intent["volume"] and mounts[0].get("Destination") == "/tmp"
                and mounts[0].get("RW") is False, "Keeper source-free read-only mount differs")
        require(state.get("Running") is running and state.get("Paused") is False and state.get("Restarting") is False
                and state.get("Dead") is False and state.get("OOMKilled") is False and state.get("Error") == ""
                and type(value.get("RestartCount")) is int and value["RestartCount"] == 0
                and type(state.get("StartedAt")) is str, "Keeper lifecycle unproven")
        if running:
            require(state.get("Status") == "running" and bool(state["StartedAt"])
                    and not state["StartedAt"].startswith("0001-"), "Keeper continuous lifetime unproven")
        else:
            require(state.get("Status") == "created" and state["StartedAt"].startswith("0001-"), "Keeper ran before registered start")
        self._keeper_inspection = value
        return {"Id": value["Id"], "Image": value["Image"], "Name": value["Name"], "Path": value["Path"],
                "Args": value["Args"], "Config": config, "HostConfig": host, "Mounts": mounts,
                "StartedAt": state["StartedAt"], "RestartCount": value["RestartCount"]}

    def _keeper_boundary(self, label: str, intent: dict[str, Any], container_id: str,
                         baseline: dict[str, Any], baseline_full: dict[str, Any]) -> bool:
        observed = self._keeper_state(label, intent, container_id)
        require(type(self._keeper_inspection) is dict, "Missing full keeper inspection")
        comparison = process_transport.identity_comparison(baseline, observed, self.runtime,
            phase="keeper-running-to-running", before_full_inspection=baseline_full,
            after_full_inspection=self._keeper_inspection)
        self._retain(label + "-comparison.json", encoded(comparison))
        return comparison["matches"] is True

    def _remove_container(self, label: str, name: str, intent: dict[str, Any], step: dict[str, Any],
                          container_id: str | None) -> bool:
        """Remove only a resource independently matched to this durable invocation."""
        inspected = self._command(label + "-cleanup-inspect", ["docker", "inspect", "--format", "{{json .}}", name])
        if not self._clean(inspected):
            absent = self._command(label + "-absent", ["docker", "container", "ls", "--all", "--quiet", "--filter", "name=^/" + name + "$"])
            return self._clean(absent) and not self._raw(absent).strip()
        value = process_transport.strict_json_loads(self._raw(inspected))
        labels = {"gossip.execution": intent["execution_id"], "gossip.source": self.binding.source_sha256,
                  "gossip.fixture": self.config["fixture_sha256"], "gossip.step": step["step_id"]}
        if step["step_id"] == "volume-keeper":
            labels = {"gossip.execution": intent["execution_id"], "gossip.role": "volume-keeper"}
        require(type(value) is dict and value.get("Name") == "/" + name
                and re.fullmatch(r"[0-9a-f]{64}", value.get("Id", "")) is not None
                and (container_id is None or value["Id"] == container_id)
                and value.get("Image") == self.policy.image_id
                and value.get("Config", {}).get("Labels") == labels,
                "Container cleanup ownership differs; refusing removal")
        removed = self._command(label + "-remove", ["docker", "rm", "--force", value["Id"]])
        absent = self._command(label + "-absent", ["docker", "container", "ls", "--all", "--quiet", "--filter", "name=^/" + name + "$"])
        return self._clean(removed) and self._clean(absent) and not self._raw(absent).strip()

    def _dispatch(self, intent: dict[str, Any]) -> None:
        if self.endpoint is None:
            raise ExecutionError("Physical dispatch requires a local Engine endpoint")
        infrastructure: list[str] = []
        results: list[dict[str, Any]] = []
        cleanup: dict[str, bool] = {}
        volume_created = False
        volume_clean = False
        keeper_claimed = False
        keeper_clean = False
        keeper_id: str | None = None
        keeper_identity: dict[str, Any] | None = None
        with tempfile.TemporaryDirectory(prefix="gossip-client-stage-") as temp:
            staging = Path(temp).resolve()
            workspace, inputs = staging / "workspace", staging / "inputs"
            for root, files, directories in ((workspace, self.files, []), (inputs, self.fixture_files, self.recipe["directories"])):
                root.mkdir(mode=0o755)
                for directory in directories:
                    (root / directory).mkdir(parents=True, exist_ok=True, mode=0o755)
                for name, raw in files.items():
                    path = root / name
                    path.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
                    path.write_bytes(raw)
                    path.chmod(0o444)
                _verify_tree(root, files, directories)
            self._retain("staging.json", encoded({"workspace": str(workspace), "inputs": str(inputs),
                "source_manifest": source_manifest(self.files), "fixture_manifest": source_manifest(self.fixture_files),
                "directories": self.recipe["directories"]}))
            try:
                before = self._checked("volume-before", ["docker", "volume", "ls", "--quiet", "--filter", "name=^" + intent["volume"] + "$"])
                require(not self._raw(before).strip(), "Owned volume name already exists")
                create = ["docker", "volume", "create", "--driver", "local", "--label", "gossip.execution=" + intent["execution_id"],
                          "--label", "gossip.snapshot=" + SNAPSHOT_PROTOCOL]
                for key, value in VOLUME_OPTIONS.items():
                    create.extend(("--opt", key + "=" + value))
                create.append(intent["volume"])
                volume_created = True  # Durable overall intent and pre-absence also bind uncertain creation.
                created = self._checked("volume-create", create)
                require(self._raw(created).strip() == intent["volume"].encode(), "Volume create identity differs")
                inspected = self._checked("volume-created", ["docker", "volume", "inspect", "--format", "{{json .}}", intent["volume"]])
                require(self._volume_valid(process_transport.strict_json_loads(self._raw(inspected)), intent), "Volume ownership/bounds differ")
                keeper_before = self._checked("keeper-before", ["docker", "container", "ls", "--all", "--quiet", "--filter", "name=^/" + intent["keeper"] + "$"])
                require(not self._raw(keeper_before).strip(), "Keeper name already exists")
                keeper_argv = self._keeper_create_argv(intent["keeper"], workspace, inputs, intent)
                self._retain("keeper-intent.json", encoded({"container_name": intent["keeper"], "create_argv": keeper_argv,
                    "argv": intent["keeper_argv"], "lifetime_seconds": intent["keeper_lifetime_seconds"], "volume": intent["volume"]}))
                keeper_claimed = True
                keeper_created = self._checked("keeper-create", keeper_argv)
                keeper_id = self._raw(keeper_created).strip().decode("ascii")
                require(re.fullmatch(r"[0-9a-f]{64}", keeper_id) is not None, "Invalid keeper identity")
                keeper_initial = self._keeper_state("keeper-created", intent, keeper_id, running=False)
                keeper_initial_full = self._keeper_inspection
                require(type(keeper_initial_full) is dict, "Missing full created keeper inspection")
                self._checked("keeper-start", ["docker", "start", keeper_id])
                keeper_identity = self._keeper_state("keeper-running", intent, keeper_id)
                keeper_running_full = self._keeper_inspection
                require(type(keeper_running_full) is dict, "Missing full running keeper inspection")
                assert keeper_running_full is not None
                startup_comparison = process_transport.identity_comparison(
                    {key: value for key, value in keeper_initial.items() if key != "StartedAt"},
                    {key: value for key, value in keeper_identity.items() if key != "StartedAt"},
                    self.runtime, phase="keeper-created-to-running",
                    before_full_inspection=keeper_initial_full, after_full_inspection=keeper_running_full)
                self._retain("keeper-startup-comparison.json", encoded(startup_comparison))
                require(startup_comparison["matches"] is True, "Keeper initial configuration changed outside startup policy")
                self._retain("keeper-identity.json", encoded(keeper_identity))
                for index, step in enumerate(self.recipe["steps"]):
                    label, name = "step-" + str(index).zfill(3), intent["containers"][index]
                    _verify_tree(workspace, self.files)
                    _verify_tree(inputs, self.fixture_files, self.recipe["directories"])
                    require(self._keeper_boundary(label + "-keeper-before", intent, keeper_id, keeper_identity, keeper_running_full),
                            "Keeper restarted or changed before invocation")
                    invocation_claimed = False
                    container_id: str | None = None
                    try:
                        absent = self._checked(label + "-before", ["docker", "container", "ls", "--all", "--quiet", "--filter", "name=^/" + name + "$"])
                        require(not self._raw(absent).strip(), "Owned container name already exists")
                        self._retain(label + "-controller-intent.json", encoded({"step_id": step["step_id"], "step_index": index,
                            "argv": step["argv"], "container_name": name, "volume": intent["volume"],
                            "binding": asdict(self.binding)}))
                        invocation_claimed = True
                        cleanup[name] = False  # Failed cleanup must not disappear from the keeper barrier.
                        created = self._checked(label + "-create", self._create_argv(name, workspace, inputs, intent["volume"], step, intent["execution_id"]))
                        container_id = self._raw(created).strip().decode("ascii")
                        require(re.fullmatch(r"[0-9a-f]{64}", container_id) is not None, "Invalid created container identity")
                        inspected = self._checked(label + "-created", ["docker", "inspect", "--format", "{{json .}}", container_id])
                        expected = process_transport.strict_json_loads(self._raw(inspected))
                        self._validate_created(expected, container_id=container_id, name=name, workspace=workspace,
                            inputs=inputs, intent=intent, step=step)
                        result = process_transport.run_process(self.endpoint, container_id=container_id, expected=expected,
                            policy=self.policy.process_policy(), retain=self._retain, label=label,
                            expected_runtime=self.runtime, expected_argv=step["argv"])
                        result["history_state_verified"] = False
                        try:
                            result["history_state_verified"] = self._keeper_boundary(
                                label + "-keeper-after", intent, keeper_id, keeper_identity, keeper_running_full)
                        except (ExecutionError, OSError, ValueError, subprocess.SubprocessError) as error:
                            infrastructure.append("keeper-after:" + type(error).__name__ + ":" + str(error)[:512])
                        self._retain(label + "-result.json", encoded(result))
                        results.append({"step_id": step["step_id"], "step_index": index, "label": label, "result": result})
                    finally:
                        if invocation_claimed:
                            cleanup[name] = self._remove_container(label, name, intent, step, container_id)
                    _verify_tree(workspace, self.files)
                    _verify_tree(inputs, self.fixture_files, self.recipe["directories"])
                    require(cleanup[name], "Container cleanup unproven")
                    require(result["history_state_verified"], "Keeper lifetime does not prove retained state")
                    require(result["status"] == "completed", "Step observation incomplete: " + step["step_id"])
            except (ExecutionError, OSError, ValueError, subprocess.SubprocessError) as error:
                infrastructure.append(type(error).__name__ + ":" + str(error)[:512])
            finally:
                if keeper_claimed:
                    try:
                        require(all(cleanup.values()), "Finite container cleanup incomplete; keeper retained")
                        keeper_clean = self._remove_container("keeper", intent["keeper"], intent,
                            {"step_id": "volume-keeper"}, keeper_id)
                    except (ExecutionError, OSError, ValueError, subprocess.SubprocessError) as error:
                        infrastructure.append("keeper-cleanup:" + type(error).__name__ + ":" + str(error)[:512])
                if volume_created:
                    try:
                        owned = self._checked("volume-cleanup-inspect", ["docker", "volume", "inspect", "--format", "{{json .}}", intent["volume"]])
                        require(self._volume_valid(process_transport.strict_json_loads(self._raw(owned)), intent), "Volume cleanup ownership differs")
                        require(all(cleanup.values()) and (not keeper_claimed or keeper_clean), "Container cleanup incomplete; volume retained")
                        removed = self._command("volume-remove", ["docker", "volume", "rm", intent["volume"]])
                        absent = self._command("volume-after", ["docker", "volume", "ls", "--quiet", "--filter", "name=^" + intent["volume"] + "$"])
                        volume_clean = self._clean(removed) and self._clean(absent) and not self._raw(absent).strip()
                    except (ExecutionError, OSError, ValueError, subprocess.SubprocessError) as error:
                        infrastructure.append("cleanup:" + type(error).__name__ + ":" + str(error)[:512])
        self._retain("terminal.json", encoded({"protocol": PROTOCOL, "mode": self.mode,
            "intent_sha256": sha256(_read(self.root / "intent.json")),
            "observation_registration": asdict(self.observation_registration),
            "cohort_freeze": intent["cohort_freeze"], "results": results,
            "cleanup": cleanup, "keeper_cleanup": keeper_clean, "keeper_identity": keeper_identity,
            "volume_cleanup": volume_clean, "infrastructure": infrastructure,
            "evaluator_sources_after": evaluator_sources(), "definition_sources_after": cases.definition_sources()}))

    def _step_binding(self, intent: dict[str, Any], index: int) -> dict[str, Any]:
        step = self.recipe["steps"][index]
        return {"protocol": PROTOCOL, "execution_id": intent["execution_id"], "source_sha256": self.binding.source_sha256,
            "commit_oid": self.registration.commit_oid, "tree_oid": self.registration.tree_oid,
            "requirements_sha256": self.binding.requirements_sha256, "milestone": self.binding.milestone,
            "purpose": self.binding.purpose, "definition_sha256": self.binding.definition_sha256,
            "ordered_suite_sha256": self.binding.ordered_suite_sha256, "case_id": self.binding.case_id,
            "step_id": step["step_id"], "step_index": index,
            "ordered_step_ids": [x["step_id"] for x in self.recipe["steps"]], "argv": step["argv"],
            "fixture_sha256": self.config["fixture_sha256"], "runtime_sha256": self.binding.runtime_sha256,
            "environment_sha256": self.binding.environment_sha256, "limits_sha256": self.binding.limits_sha256,
            "evaluator_sha256": self.binding.evaluator_sha256}

    def verified_execution(self) -> ClientHistoryResult:
        self._unchanged()
        require(self.mode == "physical", "Fixture journal cannot authenticate physical evidence")
        intent, terminal = _json(self.root / "intent.json"), _json(self.root / "terminal.json")
        freeze = admission.freeze_from_record(intent["cohort_freeze"])
        self._current_admission(freeze)
        require(intent["protocol"] == PROTOCOL and intent["config_sha256"] == digest(self.config)
                and intent["registration_sha256"] == digest(asdict(self.registration))
                and intent["observation_registration"] == json.loads(encoded(asdict(self.observation_registration))),
                "Intent identity differs")
        require(terminal["protocol"] == PROTOCOL and terminal["mode"] == "physical"
                and terminal["intent_sha256"] == sha256(_read(self.root / "intent.json"))
                and terminal["evaluator_sources_after"] == self.sources
                and terminal["definition_sources_after"] == self.definition_sources
                and terminal["observation_registration"] == intent["observation_registration"]
                and terminal["cohort_freeze"] == intent["cohort_freeze"], "Terminal identity differs")
        results = terminal["results"]
        require(type(results) is list and len(results) <= len(self.recipe["steps"]), "Invalid result census")
        observations = []
        for index, item in enumerate(results):
            step, label = self.recipe["steps"][index], "step-" + str(index).zfill(3)
            require(set(item) == {"step_id", "step_index", "label", "result"} and item["step_id"] == step["step_id"]
                    and item["step_index"] == index and item["label"] == label
                    and item["result"] == _json(self.root / (label + "-result.json")), "Step order/identity/raw result differs")
            result = item["result"]
            observations.append(observer.process_observation(self._step_binding(intent, index), result,
                self._raw(result, "stdout"), self._raw(result, "stderr")))
        expected_containers = set(intent["containers"][:len(results)])
        require(type(terminal["cleanup"]) is dict and expected_containers <= set(terminal["cleanup"])
                and set(terminal["cleanup"]) <= set(intent["containers"])
                and all(type(value) is bool for value in terminal["cleanup"].values()), "Cleanup census differs")
        cleanup = terminal["volume_cleanup"] is True and terminal["keeper_cleanup"] is True and all(terminal["cleanup"].values())
        missing = tuple(x["step_id"] for x in self.recipe["steps"][len(results):])
        completed = not missing and cleanup and not terminal["infrastructure"] and all(x["result"]["status"] == "completed" and x["result"]["history_state_verified"] for x in results)
        self._current_admission(freeze)
        return ClientHistoryResult(intent["execution_id"], self.binding.case_id,
            "completed" if completed else "observation_unavailable", tuple(observations), missing, cleanup,
            tuple(terminal["infrastructure"]), sha256(_read(self.root / "terminal.json")), self.checkpoint())

    def close(self) -> None:
        if not self.closed:
            self.closed = True
            self.owner.close()

    def __enter__(self) -> CandidateClientExecution:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()


_LOADED_SOURCES = evaluator_sources()
