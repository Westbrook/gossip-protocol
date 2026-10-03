"""Controller-owned execution of one registered candidate release gate.

The trusted controller owns this root, registration and freeze capability. Hashes
bind retained observations; they are not signatures or external attestations.
Candidate code only runs in two disposable containers. A paused builder's
capsule is captured, validated and copied into a fresh read-only consumer mount.
Reopening requires an independently retained checkpoint. An interrupted intent
is unknown and never dispatches again. Cross-root slot admission belongs to the
controller; this class cannot prevent a trusted operator making a second root.
"""
from __future__ import annotations

import base64
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

from . import candidate_release_observer_v1 as observer
from .gitstore import GitStore, _run
from .project_acceptance_registry_v1 import (
    Binding, CaseResult, CohortFreeze, Gate, PhysicalExecution, Subject,
)
from .sandbox import DockerValidator

PROTOCOL = "candidate-release-execution-v1"
SNAPSHOT_PROTOCOL = "docker-paused-local-tmpfs-volume-v1"
VOLUME_OPTIONS = {"type": "tmpfs", "device": "tmpfs", "o": "size=32m,mode=1777,nosuid,nodev,noexec"}
MAX_FILES = 511
MAX_SOURCE_BYTES = 16 * 1024 * 1024
MAX_RECORD_BYTES = 4 * 1024 * 1024
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_OID = re.compile(r"[0-9a-f]{40}\Z")


class ExecutionError(ValueError):
    """Registration or retained controller evidence does not verify."""


class ExecutionUnknown(ExecutionError):
    """A durable intent has no authenticated terminal observation."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ExecutionError(message)


def encoded(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def digest(value: Any) -> str:
    return hashlib.sha256(encoded(value)).hexdigest()


def raw_digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def source_manifest(files: dict[str, bytes]) -> list[dict[str, Any]]:
    return [{"path": path, "bytes": len(raw), "sha256": raw_digest(raw)}
            for path, raw in sorted(files.items())]


def source_sha256(files: dict[str, bytes]) -> str:
    return digest({"protocol": PROTOCOL, "kind": "complete-git-blob-inventory", "files": source_manifest(files)})


def _bounded_tree(path: Path, commit_oid: str) -> bytes:
    # A candidate can supply a huge tree even though each blob is small. Bound
    # the listing before allocating it; no hooks or replacement objects run.
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull,
               GIT_TERMINAL_PROMPT="0", GIT_NO_REPLACE_OBJECTS="1")
    process = subprocess.Popen(["git", "--literal-pathspecs", "-c", "core.hooksPath=" + os.devnull,
        "-c", "core.fsmonitor=false", "-C", str(path), "ls-tree", "-r", "-z", commit_oid],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env=env)
    expired = threading.Event()
    def expire() -> None:
        expired.set()
        process.kill()
    timer = threading.Timer(15, expire)
    timer.start()
    try:
        assert process.stdout is not None
        raw = process.stdout.read(MAX_FILES * 1200 + 1)
        if len(raw) > MAX_FILES * 1200:
            process.kill()
        process.wait(timeout=5)
        require(not expired.is_set() and process.returncode == 0 and len(raw) <= MAX_FILES * 1200,
                "Candidate Git tree exceeds capture bounds")
        return raw
    finally:
        timer.cancel()
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        if process.stdout is not None:
            process.stdout.close()


def _verify_staging(root: Path, expected: dict[str, bytes]) -> None:
    actual: dict[str, bytes] = {}
    for parent, directories, names in os.walk(root, followlinks=False):
        for name in directories:
            require(not (Path(parent) / name).is_symlink(), "Staged directory is a link")
        for name in names:
            path = Path(parent) / name
            require(path.is_file() and not path.is_symlink() and path.stat().st_size <= observer.MAX_FILE_BYTES,
                    "Unsafe staged file")
            actual[path.relative_to(root).as_posix()] = path.read_bytes()
    require(actual == expected, "Host filesystem changed staged names or bytes")


def capture_git_source(store: GitStore, commit_oid: str) -> tuple[str, dict[str, bytes]]:
    require(type(commit_oid) is str and _OID.fullmatch(commit_oid) is not None, "Full Git commit required")
    identities = store._git("rev-parse", "--show-object-format", commit_oid + "^{commit}",
                            commit_oid + "^{tree}").splitlines()
    require(len(identities) == 3 and identities[0] == "sha1" and identities[1] == commit_oid,
            "Exact SHA1 Git commit required")
    files: dict[str, bytes] = {}
    total = 0
    raw = _bounded_tree(store.path, commit_oid)
    for entry in raw.split(b"\0"):
        if not entry:
            continue
        metadata, name = entry.split(b"\t", 1)
        mode, kind, oid = metadata.split(b" ")
        require(mode in (b"100644", b"100755") and kind == b"blob", "Only ordinary Git blobs are admitted")
        path = name.decode("utf-8")
        parts = path.split("/")
        require(path not in files and not path.startswith("/") and "\\" not in path
                and all(part and part not in (".", "..") and not part.startswith(".") for part in parts),
                "Unsafe, private or duplicate source path")
        require(len(files) < MAX_FILES, "Source file bound exceeded")
        size = int(store._git("cat-file", "-s", oid.decode("ascii")))
        require(0 <= size <= observer.MAX_FILE_BYTES and total + size <= MAX_SOURCE_BYTES,
                "Source blob byte bound exceeded before reading")
        content = _run(store.path, "cat-file", "blob", oid.decode("ascii")).stdout
        require(len(content) == size, "Git blob changed during capture")
        total += len(content)
        require(total <= MAX_SOURCE_BYTES, "Source byte bound exceeded")
        files[path] = content
    require(bool(files), "Empty candidate source")
    return identities[2], files


@dataclass(frozen=True)
class ReleasePolicy:
    image_id: str
    command_timeout_seconds: int = 30
    build_timeout_seconds: int = 90
    output_limit_bytes: int = observer.MAX_PROCESS_BYTES
    seed: int = 0

    def __post_init__(self) -> None:
        require(type(self.image_id) is str and re.fullmatch(r"sha256:[0-9a-f]{64}", self.image_id) is not None,
                "Pinned local image required")
        require(self.image_id == observer.RUNTIME_IMAGE, "Observer requires its exact declared pinned runtime")
        for value, bound in ((self.command_timeout_seconds, 120), (self.build_timeout_seconds, 300),
                             (self.output_limit_bytes, observer.MAX_PROCESS_BYTES)):
            require(type(value) is int and 1 <= value <= bound, "Invalid bounded execution limit")
        require(type(self.seed) is int and 0 <= self.seed < 2 ** 63, "Invalid registered seed")


@dataclass(frozen=True)
class ControllerCheckpoint:
    files: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class Registration:
    gate: Gate
    commit_oid: str
    tree_oid: str
    repetition_id: str
    cohort_trajectory_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        require(type(self.gate) is Gate and _OID.fullmatch(self.commit_oid) is not None
                and _OID.fullmatch(self.tree_oid) is not None, "Exact registered Git gate required")
        require(type(self.repetition_id) is str and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}",
                                                               self.repetition_id) is not None,
                "Declared repetition identifier required")
        require(type(self.cohort_trajectory_ids) is tuple and len(self.cohort_trajectory_ids) == 6
                and len(set(self.cohort_trajectory_ids)) == 6
                and self.gate.binding.subject.trajectory_id in self.cohort_trajectory_ids,
                "Exactly six registered trajectories required")


def evaluator_sources() -> dict[str, str]:
    names = ("candidate_release_execution_v1.py", "candidate_release_observer_v1.py",
             "project_acceptance_registry_v1.py", "sandbox.py", "gitstore.py")
    return {name: raw_digest(Path(__file__).with_name(name).read_bytes()) for name in names}


def runtime_identity(policy: ReleasePolicy) -> dict[str, Any]:
    environment = DockerValidator._environment()
    values = []
    for command in (["docker", "version", "--format", "{{json .Server}}"],
                    ["docker", "image", "inspect", "--format", "{{json .}}", policy.image_id]):
        result = subprocess.run(command, capture_output=True, env=environment, timeout=15, check=False)
        require(result.returncode == 0 and len(result.stdout) <= 1048576, "Docker runtime unavailable")
        values.append(json.loads(result.stdout))
    server, image = values
    require(type(server) is dict and type(server.get("Version")) is str
            and type(image) is dict and image.get("Id") == policy.image_id, "Docker image/runtime differs")
    return {"server": server, "image": {key: image.get(key) for key in ("Id", "Os", "Architecture", "Variant")}}


def binding_for(subject: Subject, policy: ReleasePolicy, runtime: dict[str, Any], *, purpose: str) -> Binding:
    """Build a prospective binding; execution independently rechecks its values."""
    environment = {"docker_cli_environment_sha256": digest(DockerValidator._environment()),
                   "runtime": runtime, "host_python": [platform.python_implementation(), platform.python_version()],
                   "capsule_consumer": "fresh-readonly-root-idle-candidate-uid65534",
                   "snapshot_protocol": SNAPSHOT_PROTOCOL, "volume_options": VOLUME_OPTIONS}
    suite = {"protocol": observer.PROTOCOL, "ordered_cases": list(observer.CASE_IDS),
             "delivery_profile": "complete-public-source-capsule-v1",
             "fixture_files": source_manifest(observer.fixture_files()),
             "build_command": observer.BUILD_COMMAND, "cli_steps": observer.cli_steps()}
    return Binding(subject, digest(suite), digest(evaluator_sources()), policy.image_id.removeprefix("sha256:"),
                   digest(environment), digest(asdict(policy) | {"source_files": MAX_FILES,
                        "source_bytes": MAX_SOURCE_BYTES, "capsule_bytes": observer.MAX_CAPSULE_BYTES,
                        "sandbox": "DockerValidator._arguments/no-network/cap-drop-all",
                        "snapshot_protocol": SNAPSHOT_PROTOCOL, "volume_options": VOLUME_OPTIONS}),
                   digest({"seed": policy.seed, "semantics": "input-contract-only; no random product seed implied"}),
                   PROTOCOL, purpose)


_LOADED_SOURCES = evaluator_sources()


def _sync(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write(path: Path, raw: bytes) -> None:
    with path.open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    _sync(path.parent)


def _read(path: Path, limit: int = MAX_RECORD_BYTES) -> bytes:
    require(not path.is_symlink() and path.is_file(), "Missing or unsafe retained artifact")
    require(path.stat().st_size <= limit, "Retained artifact exceeds bound")
    return path.read_bytes()


def _json_read(path: Path) -> Any:
    raw = _read(path)
    value = json.loads(raw)
    require(encoded(value) == raw, "Retained record is not exact canonical JSON")
    return value


class CandidateReleaseExecution:
    """One local controller capability, never a candidate- or peer-callable API.

    ``verify_cohort`` must authenticate the durable irreversible six-trajectory
    terminal barrier from the controller's own authority. Passing a fabricated
    callback is outside this trusted-operator model, not cryptographic proof.
    Fixture mode supports admission/crash tests and can never yield physical
    evidence. It does not run product code or accept caller-produced receipts.
    """

    def __init__(self, root: Path, store: GitStore, registration: Registration, policy: ReleasePolicy,
                 *, verify_cohort: Callable[[], CohortFreeze] | None = None,
                 expected_checkpoint: ControllerCheckpoint | None = None, mode: str = "physical",
                 checkpoint_sink: Callable[[ControllerCheckpoint], None] | None = None):
        require(type(registration) is Registration and type(policy) is ReleasePolicy
                and mode in ("physical", "fixture"), "Typed immutable registration required")
        self.root = Path(root).absolute()
        require(self.root.resolve() == self.root and not self.root.is_symlink(), "Canonical controller root required")
        existed = self.root.exists()
        require(not existed or expected_checkpoint is not None, "Existing journal needs external checkpoint")
        require(existed or expected_checkpoint is None, "Checkpoint root missing")
        self.root.mkdir(parents=True, exist_ok=True)
        require(not (self.root / "owner.lock").is_symlink(), "Unsafe journal owner lock")
        self.owner = (self.root / "owner.lock").open("a+b")
        try:
            fcntl.flock(self.owner.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.owner.close()
            raise ExecutionError("Execution journal already has an owner") from None
        self.closed = False
        self._owner_thread = threading.get_ident()
        self._authenticated: dict[str, str] = {}
        self.store, self.registration, self.policy, self.mode = store, registration, policy, mode
        self.verify_cohort = verify_cohort
        self.checkpoint_sink = checkpoint_sink
        try:
            self.sources = evaluator_sources()
            require(self.sources == _LOADED_SOURCES
                    and observer.LOADED_SOURCE_SHA256 == self.sources["candidate_release_observer_v1.py"], "Loaded evaluator sources changed before registration")
            self.runtime = runtime_identity(policy) if mode == "physical" else {"kind": "fixture-no-Docker"}
            self.tree, self.files = capture_git_source(store, registration.commit_oid)
            require(self.tree == registration.tree_oid
                    and source_sha256(self.files) == registration.gate.binding.subject.source_sha256,
                    "Registered source differs from complete Git blobs")
            observer.expected_observations(self.files)
            self.binding = binding_for(registration.gate.binding.subject, policy, self.runtime,
                                       purpose=registration.gate.binding.purpose)
            require(registration.gate.binding == self.binding
                    and registration.gate.ordered_case_ids == observer.CASE_IDS,
                    "Gate differs from fixed observer execution contract")
            self.config = {"protocol": PROTOCOL, "root": str(self.root), "repository": str(store.path.resolve()),
                           "registration": asdict(registration), "policy": asdict(policy), "mode": mode,
                           "evaluator_sources": self.sources, "runtime": self.runtime,
                           "source_manifest": source_manifest(self.files), "snapshot_protocol": SNAPSHOT_PROTOCOL}
            if existed:
                require(_json_read(self.root / "config.json") == json.loads(encoded(self.config)),
                        "Journal registration/configuration changed")
                self._check_checkpoint(expected_checkpoint)
            else:
                require(set(path.name for path in self.root.iterdir()) == {"owner.lock"}, "Foreign journal root")
                self._retain("config.json", encoded(self.config))
                _sync(self.root.parent)
        except BaseException:
            self.close()
            raise

    def _retain(self, name: str, raw: bytes) -> None:
        require(re.fullmatch(r"[a-z][a-z0-9.-]*", name) is not None, "Unsafe retained artifact name")
        _write(self.root / name, raw)
        self._authenticated[name] = raw_digest(raw)
        if self.checkpoint_sink is not None:
            self.checkpoint_sink(self.checkpoint())

    def _inventory(self) -> dict[str, str]:
        files = {}
        for path in sorted(self.root.iterdir()):
            if path.name == "owner.lock":
                continue
            require(path.is_file() and not path.is_symlink(), "Foreign journal entry")
            files[path.name] = raw_digest(_read(path, observer.MAX_CAPSULE_BYTES))
        return files

    def _check_checkpoint(self, checkpoint: ControllerCheckpoint | None) -> None:
        require(type(checkpoint) is ControllerCheckpoint and bool(checkpoint.files), "External checkpoint required")
        assert checkpoint is not None
        require(len(dict(checkpoint.files)) == len(checkpoint.files)
                and self._inventory() == dict(checkpoint.files), "Journal rollback, appended suffix or artifact change")
        self._authenticated = dict(checkpoint.files)

    def checkpoint(self) -> ControllerCheckpoint:
        require(not self.closed and threading.get_ident() == self._owner_thread, "Closed or foreign-thread authority")
        require(self._inventory() == self._authenticated, "Journal differs from controller-owned original writes")
        return ControllerCheckpoint(tuple(sorted(self._authenticated.items())))

    def _freeze(self) -> CohortFreeze | None:
        if self.binding.purpose == "public_release":
            return None
        require(callable(self.verify_cohort), "Independent execution needs trusted cohort authority")
        assert self.verify_cohort is not None
        freeze = self.verify_cohort()
        require(type(freeze) is CohortFreeze and freeze.no_further_model_actions
                and {item.trajectory_id for item in freeze.subjects} == set(self.registration.cohort_trajectory_ids),
                "Complete irreversible six-trajectory barrier unproven")
        subject = self.binding.subject
        require(all(item.cohort_id == subject.cohort_id
                    and item.execution_contract_sha256 == subject.execution_contract_sha256
                    and item.requirements_sha256 == subject.requirements_sha256
                    and item.milestone == subject.milestone for item in freeze.subjects)
                and [item for item in freeze.subjects if item.trajectory_id == subject.trajectory_id] == [subject],
                "Frozen cohort or own final source differs")
        return freeze

    def _unchanged(self) -> None:
        self.checkpoint()
        require(not self.closed and evaluator_sources() == self.sources == _LOADED_SOURCES, "Loaded evaluator sources changed")
        require(_json_read(self.root / "config.json") == json.loads(encoded(self.config)), "Controller config changed")
        tree, files = capture_git_source(self.store, self.registration.commit_oid)
        require(tree == self.tree and files == self.files, "Frozen Git tree changed")
        runtime = runtime_identity(self.policy) if self.mode == "physical" else {"kind": "fixture-no-Docker"}
        require(runtime == self.runtime
                and binding_for(self.binding.subject, self.policy, runtime, purpose=self.binding.purpose) == self.binding,
                "Execution environment/runtime changed")

    def execute_once(self) -> PhysicalExecution:
        self._unchanged()
        if (self.root / "intent.json").exists():
            if not (self.root / "terminal.json").exists():
                raise ExecutionUnknown("Existing intent is unknown; never redispatch")
            return self.verified_execution()
        freeze = self._freeze()
        intent = {"protocol": PROTOCOL, "registration_sha256": digest(asdict(self.registration)),
                  "config_sha256": digest(self.config), "binding": asdict(self.binding),
                  "execution_id": "release-" + uuid.uuid4().hex,
                  "containers": ["gossip-release-build-" + uuid.uuid4().hex,
                                 "gossip-release-use-" + uuid.uuid4().hex],
                  "freeze": None if freeze is None else asdict(freeze),
                  "volume": "gossip-release-volume-" + uuid.uuid4().hex}
        self._retain("intent.json", encoded(intent))
        self._dispatch(intent)
        require(self._freeze() == freeze, "Cohort barrier changed during execution")
        self._unchanged()
        return self.verified_execution()

    def _command(self, label: str, arguments: list[str], *, limit: int, timeout: int) -> dict[str, Any]:
        raw_streams = {"stdout": bytearray(), "stderr": bytearray()}
        counts = {"stdout": 0, "stderr": 0}
        errors: list[str] = []
        capture_lock = threading.Lock()
        process = subprocess.Popen(arguments, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   stdin=subprocess.DEVNULL, env=DockerValidator._environment())
        def drain(kind: str, pipe: Any) -> None:
            try:
                while chunk := pipe.read(65536):
                    with capture_lock:
                        counts[kind] += len(chunk)
                        remaining = limit - len(raw_streams[kind])
                        if remaining > 0:
                            raw_streams[kind].extend(chunk[:remaining])
            except OSError:
                errors.append(kind)
            finally:
                pipe.close()
        threads = [threading.Thread(target=drain, args=(kind, getattr(process, kind)), daemon=True)
                   for kind in raw_streams]
        for thread in threads:
            thread.start()
        timed_out = False
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            process.kill()
            process.wait(timeout=5)
        for thread in threads:
            thread.join(timeout=5)
        complete = not errors and all(not thread.is_alive() for thread in threads)
        record: dict[str, Any] = {"arguments": arguments, "exit_code": process.returncode,
                                  "timed_out": timed_out, "capture_complete": complete}
        with capture_lock:
            snapshots = {kind: bytes(data) for kind, data in raw_streams.items()}
            final_counts = dict(counts)
        for kind, data in snapshots.items():
            filename = label + "-" + kind + ".bin"
            self._retain(filename, bytes(data))
            record[kind] = {"path": filename, "sha256": raw_digest(bytes(data)), "bytes": len(data),
                            "observed_bytes": final_counts[kind], "truncated": final_counts[kind] > len(data)}
        self._retain(label + ".json", encoded(record))
        return record

    @staticmethod
    def _clean(record: dict[str, Any]) -> bool:
        return (not record["timed_out"] and record["capture_complete"]
                and not record["stdout"]["truncated"] and not record["stderr"]["truncated"])

    def _bytes(self, record: dict[str, Any], kind: str) -> bytes:
        item = record[kind]
        raw = _read(self.root / item["path"], observer.MAX_CAPSULE_BYTES)
        require(raw_digest(raw) == item["sha256"] and len(raw) == item["bytes"], "Raw observation changed")
        return raw

    def _observation(self, record: dict[str, Any]) -> dict[str, Any]:
        return {"exit_code": record["exit_code"],
                "stdout_b64": base64.b64encode(self._bytes(record, "stdout")).decode(),
                "stderr_b64": base64.b64encode(self._bytes(record, "stderr")).decode()}

    def _arguments(self, name: str, workspace: Path, checks: Path, *, volume: str | None = None) -> list[str]:
        sandbox = DockerValidator(self.policy.image_id, {"placeholder.txt": "unused"},
                                  command=("python", "-I", "-c", "import time;time.sleep(600)"))
        arguments = sandbox._arguments(name, workspace, checks)
        arguments.remove("--rm")
        if volume is not None:
            arguments = [arg for arg in arguments if not arg.startswith("--tmpfs=/tmp:")]
            index = arguments.index("--entrypoint")
            arguments[index:index] = ["--mount", f"type=volume,source={volume},target=/tmp,volume-nocopy"]
        arguments[arguments.index("--user=65534:65534")] = "--user=0:0"
        index = arguments.index("--entrypoint")
        arguments[index:index] = ["--mount", f"type=bind,source={checks},target=/inputs,readonly,bind-propagation=rprivate"]
        arguments.insert(2, "--detach")
        return arguments

    def _plans(self, intent: dict[str, Any], plan: dict[str, str]) -> dict[str, list[str]]:
        names = intent["containers"]
        checks = Path(plan["inputs"])
        volume = intent["volume"]
        commands = {
            "volume-before": ["docker", "volume", "ls", "--quiet", "--filter", "name=^" + volume + "$"],
            "volume-create": ["docker", "volume", "create", "--driver", "local",
                "--label", "gossip.execution=" + intent["execution_id"], "--label", "gossip.snapshot=" + SNAPSHOT_PROTOCOL,
                "--opt", "type=tmpfs", "--opt", "device=tmpfs", "--opt", "o=" + VOLUME_OPTIONS["o"], volume],
            "volume-created": ["docker", "volume", "inspect", "--format", "{{json .}}", volume],
            "build-start": self._arguments(names[0], Path(plan["source"]), checks, volume=volume),
            "build": ["docker", "exec", "--user", "65534:65534", names[0], *observer.BUILD_COMMAND],
            "build-pause": ["docker", "pause", names[0]],
            "build-state": ["docker", "inspect", "--format", "{{json .}}", names[0]],
            "capsule": ["docker", "cp", names[0] + ":/tmp", "-"],
            "consumer-start": self._arguments(names[1], Path(plan["capsule"]), checks),
        }
        for step, command in observer.cli_steps():
            commands["cli-" + step] = ["docker", "exec", "--user", "65534:65534", names[1], *command]
        commands.update({
            "volume-cleanup-inspect": ["docker", "volume", "inspect", "--format", "{{json .}}", volume],
            "volume-remove": ["docker", "volume", "rm", volume],
            "volume-after": ["docker", "volume", "ls", "--quiet", "--filter", "name=^" + volume + "$"],
        })
        return commands

    def _volume_valid(self, record: dict[str, Any], intent: dict[str, Any]) -> bool:
        try:
            value = json.loads(self._bytes(record, "stdout"))
            return (self._clean(record) and record["exit_code"] == 0 and type(value) is dict
                and value.get("Name") == intent["volume"] and value.get("Driver") == "local"
                and value.get("Scope") == "local" and value.get("Options") == VOLUME_OPTIONS
                and value.get("Labels") == {"gossip.execution": intent["execution_id"], "gossip.snapshot": SNAPSHOT_PROTOCOL})
        except (ValueError, TypeError):
            return False

    def _dispatch(self, intent: dict[str, Any]) -> None:
        require(self.mode == "physical", "Fixture journal cannot dispatch or authenticate physical execution")
        names = intent["containers"]
        commands: list[str] = []
        infrastructure: list[str] = []
        observed: dict[str, Any] = {"protocol": observer.PROTOCOL, "build": None, "manifest": None, "cli": []}
        sandbox = DockerValidator(self.policy.image_id, {"placeholder.txt": "unused"})
        cleanups: dict[str, bool] = {}
        volume_attempted = False
        volume_cleanup = False
        try:
            with tempfile.TemporaryDirectory(prefix="gossip-candidate-release-") as temp:
                staging = Path(temp).resolve()
                plan = {"source": str(staging / "source"), "inputs": str(staging / "inputs"),
                        "capsule": str(staging / "capsule")}
                self._retain("plan.json", encoded(plan))
                planned = self._plans(intent, plan)
                def run(label: str) -> dict[str, Any]:
                    commands.append(label)
                    record = self._command(label, planned[label],
                        limit=observer.MAX_CAPSULE_BYTES if label == "capsule" else self.policy.output_limit_bytes,
                        timeout=self.policy.build_timeout_seconds if label == "build" else self.policy.command_timeout_seconds)
                    if not self._clean(record):
                        infrastructure.append(label + ":incomplete-capture")
                    return record
                for directory in plan.values():
                    Path(directory).mkdir(mode=0o755)
                for root, files in ((Path(plan["source"]), self.files), (Path(plan["inputs"]), observer.fixture_files())):
                    for path, raw in files.items():
                        target = root / path
                        target.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
                        with target.open("xb") as stream:
                            stream.write(raw)
                        target.chmod(0o444)
                _verify_staging(Path(plan["source"]), self.files)
                _verify_staging(Path(plan["inputs"]), observer.fixture_files())
                before = run("volume-before")
                if before["exit_code"] != 0 or self._bytes(before, "stdout").strip():
                    infrastructure.append("volume:absence-unproven")
                if not infrastructure:
                    volume_attempted = True
                    created = run("volume-create")
                    inspected = run("volume-created")
                    if (created["exit_code"] != 0 or self._bytes(created, "stdout").strip() != intent["volume"].encode()
                            or not self._volume_valid(inspected, intent)):
                        infrastructure.append("volume:ownership-or-bounds-unproven")
                if not infrastructure:
                    started = run("build-start")
                    if started["exit_code"] != 0:
                        infrastructure.append("build:start-failed")
                if not infrastructure:
                    build = run("build")
                    observed["build"] = self._observation(build)
                    if build["exit_code"] != 0:
                        infrastructure.append("build:exec-completion-unproven")
                    paused = run("build-pause")
                    state_record = run("build-state")
                    if paused["exit_code"] != 0 or not self._paused(state_record, intent["volume"]):
                        infrastructure.append("build:snapshot-unfrozen")
                    if not infrastructure:
                        copied = run("capsule")
                        if copied["exit_code"] != 0:
                            infrastructure.append("capsule:capture-failed")
                        elif not infrastructure:
                            try:
                                packaged = observer.parse_capsule(self._bytes(copied, "stdout"))
                            except observer.CapsuleTransportError:
                                infrastructure.append("capsule:malformed-transport")
                                packaged = {}
                            except observer.ObservationError:
                                packaged = {}
                            if packaged:
                                observed["manifest"] = observer.capsule_observation(packaged)
                                for path, raw in packaged.items():
                                    target = Path(plan["capsule"]) / path
                                    target.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
                                    with target.open("xb") as stream:
                                        stream.write(raw)
                                    target.chmod(0o444)
                                _verify_staging(Path(plan["capsule"]), packaged)
                cleanups[names[0]] = sandbox._remove(names[0])
                if not cleanups[names[0]]:
                    infrastructure.append("builder:cleanup-unverified")
                if not infrastructure and observed["manifest"] is not None:
                    consumer = run("consumer-start")
                    if consumer["exit_code"] != 0:
                        infrastructure.append("consumer:start-failed")
                    if not infrastructure:
                        for step, _command in observer.cli_steps():
                            result = run("cli-" + step)
                            observed["cli"].append({"step_id": step, **self._observation(result)})
                            if result["exit_code"] != 0:
                                infrastructure.append("cli:" + step + ":exec-completion-unproven")
                            if infrastructure:
                                break
        except (OSError, subprocess.SubprocessError) as error:
            infrastructure.append("controller:" + type(error).__name__)
        finally:
            for name in names:
                if name not in cleanups:
                    cleanups[name] = sandbox._remove(name)
                if not cleanups[name]:
                    infrastructure.append("cleanup-unverified")
            if volume_attempted:
                try:
                    owned = run("volume-cleanup-inspect")
                    if self._volume_valid(owned, intent) and all(cleanups.values()):
                        removed = run("volume-remove")
                        absent = run("volume-after")
                        volume_cleanup = (self._clean(removed) and removed["exit_code"] == 0
                            and self._clean(absent) and absent["exit_code"] == 0 and not self._bytes(absent, "stdout").strip())
                except (OSError, subprocess.SubprocessError):
                    volume_cleanup = False
            if not volume_cleanup:
                infrastructure.append("volume:cleanup-unverified")
        terminal = {"protocol": PROTOCOL, "mode": self.mode, "intent_sha256": raw_digest(_read(self.root / "intent.json")),
                    "commands": commands, "infrastructure": infrastructure, "cleanup": cleanups,
                    "volume_cleanup": volume_cleanup, "observation": observed}
        self._retain("terminal.json", encoded(terminal))

    def _paused(self, record: dict[str, Any], volume: str) -> bool:
        try:
            inspected = json.loads(self._bytes(record, "stdout"))
            if type(inspected) is not dict or type(inspected.get("State")) is not dict or type(inspected.get("Mounts")) is not list:
                return False
            state = inspected["State"]
            mounted = [item for item in inspected["Mounts"] if type(item) is dict and item.get("Destination") == "/tmp"]
            return (record["exit_code"] == 0 and state.get("Running") is True and state.get("Paused") is True
                    and type(state.get("Pid")) is int and state["Pid"] > 0 and len(mounted) == 1
                    and mounted[0].get("Type") == "volume" and mounted[0].get("Name") == volume
                    and mounted[0].get("Driver") == "local" and mounted[0].get("RW") is True)
        except (ValueError, TypeError):
            return False

    def _command_record(self, label: str, planned: list[str]) -> dict[str, Any]:
        record = _json_read(self.root / (label + ".json"))
        require(type(record) is dict and set(record) == {"arguments", "exit_code", "timed_out", "capture_complete", "stdout", "stderr"}
                and record["arguments"] == planned and type(record["exit_code"]) is int
                and type(record["timed_out"]) is bool and type(record["capture_complete"]) is bool,
                "Closed command observation or actual arguments differ")
        bound = observer.MAX_CAPSULE_BYTES if label == "capsule" else self.policy.output_limit_bytes
        for kind in ("stdout", "stderr"):
            item = record[kind]
            require(type(item) is dict and set(item) == {"path", "sha256", "bytes", "observed_bytes", "truncated"}
                    and item["path"] == label + "-" + kind + ".bin"
                    and type(item["sha256"]) is str and _SHA.fullmatch(item["sha256"]) is not None
                    and type(item["bytes"]) is int and 0 <= item["bytes"] <= bound
                    and type(item["observed_bytes"]) is int and item["observed_bytes"] >= item["bytes"]
                    and type(item["truncated"]) is bool and item["truncated"] == (item["observed_bytes"] > item["bytes"]),
                    "Raw stream identity or capture bounds differ")
            self._bytes(record, kind)
        return record

    def verified_execution(self) -> PhysicalExecution:
        self._unchanged()
        require(self.mode == "physical", "Fixture records cannot authenticate physical execution")
        intent = _json_read(self.root / "intent.json")
        if not (self.root / "terminal.json").exists():
            raise ExecutionUnknown("Intent lacks authenticated terminal observation")
        require(type(intent) is dict and set(intent) == {"protocol", "registration_sha256", "config_sha256", "binding",
                "execution_id", "containers", "freeze", "volume"} and intent["protocol"] == PROTOCOL
                and intent["registration_sha256"] == digest(asdict(self.registration))
                and intent["config_sha256"] == digest(self.config)
                and intent["binding"] == json.loads(encoded(asdict(self.binding)))
                and type(intent["execution_id"]) is str and re.fullmatch(r"release-[0-9a-f]{32}", intent["execution_id"]) is not None,
                "Intent binding changed")
        names = intent["containers"]
        require(type(names) is list and len(names) == 2 and all(type(name) is str for name in names)
                and re.fullmatch(r"gossip-release-build-[0-9a-f]{32}", names[0]) is not None
                and re.fullmatch(r"gossip-release-use-[0-9a-f]{32}", names[1]) is not None, "Owned containers changed")
        require(type(intent["volume"]) is str and re.fullmatch(r"gossip-release-volume-[0-9a-f]{32}", intent["volume"]) is not None,
                "Owned volume changed")
        freeze = self._freeze()
        require(intent["freeze"] == (None if freeze is None else json.loads(encoded(asdict(freeze)))), "Cohort barrier changed")
        terminal = _json_read(self.root / "terminal.json")
        require(type(terminal) is dict and set(terminal) == {"protocol", "mode", "intent_sha256", "commands",
                    "infrastructure", "cleanup", "volume_cleanup", "observation"} and terminal["protocol"] == PROTOCOL
                and terminal["mode"] == "physical" and terminal["intent_sha256"] == raw_digest(_read(self.root / "intent.json")),
                "Terminal origin changed")
        require(type(terminal["cleanup"]) is dict and set(terminal["cleanup"]) == set(names)
                and all(type(value) is bool for value in terminal["cleanup"].values()) and type(terminal["volume_cleanup"]) is bool
                and type(terminal["infrastructure"]) is list and len(terminal["infrastructure"]) <= 32
                and all(type(value) is str for value in terminal["infrastructure"]), "Cleanup or infrastructure evidence malformed")
        plan = _json_read(self.root / "plan.json")
        require(type(plan) is dict and set(plan) == {"source", "inputs", "capsule"}
                and all(type(value) is str and Path(value).is_absolute() for value in plan.values()), "Execution staging plan changed")
        parents = {str(Path(value).parent) for value in plan.values()}
        require(len(parents) == 1 and all(Path(value).name == key for key, value in plan.items()), "Staging layout changed")
        planned = self._plans(intent, plan)
        labels = terminal["commands"]
        order = list(planned)
        cleanup_labels = ["volume-cleanup-inspect", "volume-remove", "volume-after"]
        ordinary = order[:-3]
        require(type(labels) is list and all(type(label) is str for label in labels)
                and len(set(labels)) == len(labels) and all(label in planned for label in labels),
                "Unexpected or duplicate execution command")
        body_labels = [label for label in labels if label not in cleanup_labels]
        tail_labels = [label for label in labels if label in cleanup_labels]
        require(body_labels == ordinary[:len(body_labels)] and tail_labels == cleanup_labels[:len(tail_labels)]
                and labels == body_labels + tail_labels, "Command census or order differs from fixed observer")
        if any(not (self.root / (label + ".json")).exists() for label in labels):
            raise ExecutionUnknown("Dispatch lacks a complete original command record")
        records = {label: self._command_record(label, planned[label]) for label in labels}
        def clean(label: str) -> bool:
            return label in records and self._clean(records[label]) and records[label]["exit_code"] == 0
        infrastructure = bool(terminal["infrastructure"]) or not all(terminal["cleanup"].values())
        infrastructure = infrastructure or any(not self._clean(item) for item in records.values())
        volume_bound = (clean("volume-before") and not self._bytes(records["volume-before"], "stdout").strip()
            and clean("volume-create") and self._bytes(records["volume-create"], "stdout").strip() == intent["volume"].encode()
            and clean("volume-created") and self._volume_valid(records["volume-created"], intent))
        volume_clean = (clean("volume-cleanup-inspect") and self._volume_valid(records["volume-cleanup-inspect"], intent)
            and clean("volume-remove") and clean("volume-after") and not self._bytes(records["volume-after"], "stdout").strip())
        require(terminal["volume_cleanup"] == volume_clean, "Volume cleanup summary differs from retained observations")
        transport = (volume_bound and clean("build-start") and clean("build-pause") and clean("build-state")
            and self._paused(records["build-state"], intent["volume"]))
        if not transport:
            infrastructure = True
        observed: dict[str, Any] = {"protocol": observer.PROTOCOL, "build": None, "manifest": None, "cli": []}
        if "build" in records:
            observed["build"] = self._observation(records["build"])
        package_available = False
        if transport and clean("capsule"):
            try:
                packaged = observer.parse_capsule(self._bytes(records["capsule"], "stdout"))
                package_available = True
                if packaged:
                    observed["manifest"] = observer.capsule_observation(packaged)
            except observer.CapsuleTransportError:
                infrastructure = True
            except observer.ObservationError:
                package_available = True  # A fully captured invalid package is a product failure.
        elif "capsule" in records:
            infrastructure = True
        for step, _command in observer.cli_steps():
            if "cli-" + step in records:
                observed["cli"].append({"step_id": step, **self._observation(records["cli-" + step])})
        require(observed == terminal["observation"], "Terminal summary differs from raw observations")
        cli_labels = ["cli-" + step for step, _command in observer.cli_steps()]
        available = (clean("build") and clean("build-start"), package_available,
                     clean("consumer-start") and all(clean(label) for label in cli_labels))
        # Docker exec nonzero alone cannot distinguish candidate exit from daemon
        # failure. Preserve that uncertainty while keeping prior observed failures.
        infrastructure = infrastructure or not all(available)
        cleanup = all(terminal["cleanup"].values()) and volume_clean
        infrastructure = infrastructure or not cleanup
        outcomes = []
        for case, complete in zip(observer.CASE_IDS, available, strict=True):
            if complete:
                try:
                    passed = observer.score_case(case, observed, self.files)
                    status = "passed" if passed else "failed"
                    if passed and not cleanup:
                        status = "infrastructure_error"
                except observer.ObservationError:
                    status = "infrastructure_error"
                    infrastructure = True
            else:
                status = "infrastructure_error"
            outcomes.append(CaseResult(case, status))
        verdict = {"protocol": PROTOCOL, "kind": "controller-verifier", "binding": asdict(self.binding),
                   "intent_sha256": raw_digest(_read(self.root / "intent.json")),
                   "terminal_sha256": raw_digest(_read(self.root / "terminal.json")),
                   "raw_artifacts": {label: raw_digest(_read(self.root / (label + ".json"))) for label in labels},
                   "outcomes": [asdict(item) for item in outcomes], "infrastructure": infrastructure,
                   "trust": "controller-owned-local-journal; no external attestation", "mode": "physical"}
        path = self.root / "verifier.json"
        if path.exists():
            require(_json_read(path) == json.loads(encoded(verdict)), "Retained host verdict changed")
        else:
            self._retain("verifier.json", encoded(verdict))
        return PhysicalExecution(self.binding, intent["execution_id"], raw_digest(_read(self.root / "terminal.json")),
                                 raw_digest(_read(path)), "infrastructure_error" if infrastructure else "completed",
                                 tuple(outcomes), None if freeze is None else freeze.receipt_sha256)

    def close(self) -> None:
        if not self.closed:
            self.closed = True
            self.owner.close()

    def __enter__(self) -> CandidateReleaseExecution:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
