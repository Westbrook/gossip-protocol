"""Source-bound HTTP server lifetimes and isolated fixed probes; no product scoring.

Server stop is an explicit controller action, not a finite-command exit claim.
Only the trusted probe has finite-exit obligations. Frozen v4 is not relaxed.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import fcntl
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import tempfile
import threading
import time
from typing import Any, Callable
import uuid

from . import candidate_client_execution_v4 as finite
from . import candidate_client_process_v4 as engine
from . import candidate_http_transport_v1 as wire
from .candidate_release_execution_v2 import capture_git_source, source_manifest
from .gitstore import GitStore
from .sandbox import DockerValidator

PROTOCOL = "candidate-http-execution-v1"
ROLE_POLICY = "candidate-http-distinct-server-keeper-probe-v1"
SNAPSHOT_PROTOCOL = "docker-owned-http-epochs-tmpfs-keeper-v1"
VOLUME_OPTIONS = dict(finite.VOLUME_OPTIONS)
MAX_RECORD_BYTES = 32 * 1024 * 1024
MAX_JOURNAL_BYTES = 512 * 1024 * 1024
MAX_JOURNAL_FILES = 16384
CONTROL_LIMIT = 1024 * 1024
MAX_STEPS = 64
PROBE_ARGV = ("python", "-I", "/probe/helper.py", "/probe/request.json")
PROBE_TMPFS = "rw,nosuid,nodev,noexec,size=32m,mode=1777"
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_NAME = re.compile(r"[a-z][a-z0-9.-]{0,100}\Z")
ExecutionError = finite.ExecutionError
ExecutionUnknown = finite.ExecutionUnknown
ControllerCheckpoint = finite.ControllerCheckpoint
require = finite.require
encoded = finite.encoded
sha256 = finite.sha256
digest = finite.digest
_read = finite._read
_json = finite._json
_sync = finite._sync


def evaluator_sources() -> dict[str, str]:
    result = {"http/" + name: sha256(Path(__file__).with_name(name).read_bytes()) for name in
              ("candidate_http_execution_v1.py", "candidate_http_transport_v1.py")}
    result.update({"finite/" + name: value for name, value in finite.evaluator_sources().items()})
    return result


def source_sha256(files: dict[str, bytes]) -> str:
    return digest({"protocol": PROTOCOL, "files": source_manifest(files)})


@dataclass(frozen=True)
class HttpPolicy:
    image_id: str
    wire_limits: wire.WireLimits = wire.WireLimits()
    transport_timeout_seconds: int = 15
    probe_timeout_seconds: int = 30
    stream_limit_bytes: int = 16 * 1024 * 1024
    lifetime_seconds: int = 900
    stop_timeout_seconds: int = 5
    seed: int = 0

    def __post_init__(self) -> None:
        engine.ProcessPolicy(self.image_id, timeout_seconds=self.probe_timeout_seconds,
            stream_limit_bytes=self.stream_limit_bytes, frame_limit_bytes=self.stream_limit_bytes,
            transport_timeout_seconds=self.transport_timeout_seconds)
        require(type(self.wire_limits) is wire.WireLimits, "Typed wire bounds required")
        for value, bound in ((self.transport_timeout_seconds, 60), (self.probe_timeout_seconds, 120),
                             (self.lifetime_seconds, 7200), (self.stop_timeout_seconds, 30)):
            require(type(value) is int and 1 <= value <= bound, "Invalid lifecycle observation bound")
        require(type(self.seed) is int and 0 <= self.seed < 2 ** 63, "Invalid fixture seed")
        require(self.probe_timeout_seconds > self.wire_limits.timeout_seconds,
                "Probe process deadline must exceed wire deadline")
        envelope = wire.max_probe_output_bytes(self.wire_limits)
        require(envelope <= self.stream_limit_bytes
                and engine.HEADER_LIMIT + 2 * envelope + engine.FRAME_COUNT_LIMIT * 8 <= MAX_RECORD_BYTES,
                "Derived helper/attach envelope exceeds capture bounds")


@dataclass(frozen=True)
class HttpStep:
    step_id: str
    kind: str
    request_json: bytes = b""

    def __post_init__(self) -> None:
        require(type(self.step_id) is str and _NAME.fullmatch(self.step_id) is not None,
                "Invalid HTTP step identifier")
        require(self.kind in ("start", "probe", "stop") and type(self.request_json) is bytes,
                "Closed lifecycle step required")
        if self.kind == "probe":
            value = engine.strict_json_loads(self.request_json)
            require(type(value) is dict and encoded(value) == self.request_json,
                    "Canonical closed probe request required")
        else:
            require(not self.request_json, "Only probe steps carry requests")

    def record(self) -> dict[str, Any]:
        return {"step_id": self.step_id, "kind": self.kind,
                "request": engine.strict_json_loads(self.request_json) if self.request_json else None}


@dataclass(frozen=True)
class HttpRecipe:
    recipe_id: str
    server_argv: tuple[str, ...]
    fixtures: tuple[tuple[str, bytes], ...]
    directories: tuple[str, ...]
    steps: tuple[HttpStep, ...]
    port: int = 8765
    database_path: str = "/tmp/library.sqlite"
    root_path: str = "/inputs"

    def __post_init__(self) -> None:
        require(type(self.recipe_id) is str and _NAME.fullmatch(self.recipe_id) is not None,
                "Invalid HTTP recipe identifier")
        require(type(self.server_argv) is tuple and 1 <= len(self.server_argv) <= 256
                and all(type(x) is str and x and "\x00" not in x for x in self.server_argv)
                and sum(len(x.encode()) for x in self.server_argv) <= finite.MAX_ARGV_BYTES,
                "Registered literal server argv required")
        require(type(self.port) is int and 1024 <= self.port <= 65535, "Unprivileged service port required")
        require(type(self.database_path) is str and self.database_path.startswith("/tmp/")
                and finite._safe_relative(self.database_path[5:]) and self.root_path == "/inputs",
                "Database/root must bind the declared state/input mounts")
        require(self.database_path in self.server_argv and self.root_path in self.server_argv
                and str(self.port) in self.server_argv, "Server argv must bind explicit DB/root/port")
        require(type(self.fixtures) is tuple and len(self.fixtures) <= finite.MAX_FIXTURE_FILES
                and all(type(row) is tuple and len(row) == 2 and finite._safe_relative(row[0])
                    and type(row[1]) is bytes for row in self.fixtures), "Invalid immutable fixture")
        files = dict(self.fixtures)
        require(len(files) == len(self.fixtures) and sum(map(len, files.values())) <= finite.MAX_FIXTURE_BYTES,
                "Duplicate or excessive fixture")
        require(type(self.directories) is tuple and len(self.directories) <= finite.MAX_FIXTURE_FILES
                and len(set(self.directories)) == len(self.directories)
                and all(finite._safe_relative(x) for x in self.directories), "Invalid fixture directories")
        require(not (set(files) & set(self.directories)), "Fixture file/directory collision")
        for name in (*files, *self.directories):
            require(not any("/".join(name.split("/")[:i]) in files for i in range(1, len(name.split("/")))),
                    "Fixture ancestor is a file")
        require(type(self.steps) is tuple and 3 <= len(self.steps) <= MAX_STEPS
                and all(type(x) is HttpStep for x in self.steps)
                and len({x.step_id for x in self.steps}) == len(self.steps), "Invalid ordered lifecycle")
        active = False
        probes = 0
        for step in self.steps:
            if step.kind == "start":
                require(not active, "A server epoch is already active")
                active = True
            elif step.kind == "stop":
                require(active, "Stop requires an active server epoch")
                active = False
            else:
                require(active, "Probe requires an active server epoch")
                wire.request_bytes(engine.strict_json_loads(step.request_json), self.port)
                probes += 1
        require(not active and probes > 0, "Recipe must stop its final server and contain a probe")

    def record(self) -> dict[str, Any]:
        return {"recipe_id": self.recipe_id, "server_argv": list(self.server_argv),
                "fixture_manifest": source_manifest(dict(self.fixtures)), "directories": list(self.directories),
                "steps": [x.record() for x in self.steps], "port": self.port,
                "database_path": self.database_path, "root_path": self.root_path}


@dataclass(frozen=True)
class HttpBinding:
    source_sha256: str
    requirements_sha256: str
    recipe_sha256: str
    fixture_sha256: str
    evaluator_sha256: str
    helper_sha256: str
    runtime_sha256: str
    environment_sha256: str
    limits_sha256: str
    seed_sha256: str
    purpose: str = "harness_qualification"
    milestone: str = "M1"
    protocol: str = PROTOCOL

    def __post_init__(self) -> None:
        for key, value in asdict(self).items():
            if key.endswith("_sha256"):
                require(type(value) is str and _SHA.fullmatch(value) is not None, "Invalid HTTP binding digest")
        require(self.requirements_sha256 in finite.SUPPORTED_REQUIREMENTS_SHA256
                and self.purpose == "harness_qualification" and self.milestone == "M1"
                and self.protocol == PROTOCOL, "Only declared M1 infrastructure qualification is authorized")


@dataclass(frozen=True)
class HttpRegistration:
    binding: HttpBinding
    commit_oid: str
    tree_oid: str
    repetition_id: str

    def __post_init__(self) -> None:
        require(type(self.binding) is HttpBinding and all(type(x) is str and re.fullmatch(r"[0-9a-f]{40}", x)
                for x in (self.commit_oid, self.tree_oid)), "Exact Git source registration required")
        require(type(self.repetition_id) is str and _NAME.fullmatch(self.repetition_id) is not None,
                "Explicit repetition identity required")


def binding_for(files: dict[str, bytes], recipe: HttpRecipe, policy: HttpPolicy, runtime: dict[str, Any], *,
                requirements_sha256: str) -> HttpBinding:
    require(type(recipe) is HttpRecipe and type(policy) is HttpPolicy, "Typed recipe and policy required")
    for step in recipe.steps:
        if step.kind == "probe":
            wire.build_probe_input(engine.strict_json_loads(step.request_json), recipe.port, policy.wire_limits)
    return HttpBinding(source_sha256(files), requirements_sha256, digest(recipe.record()),
        digest({"manifest": source_manifest(dict(recipe.fixtures)), "directories": recipe.directories}),
        digest(evaluator_sources()), wire.helper_sha256(), digest(runtime),
        digest({"environment": DockerValidator._environment(), "roles": ROLE_POLICY,
                "volume_options": VOLUME_OPTIONS, "probe_argv": PROBE_ARGV}), digest(asdict(policy)),
        digest({"seed": policy.seed, "meaning": "fixed fixture; no candidate random seed implied"}))


@dataclass(frozen=True)
class RoleSpec:
    role: str
    name: str
    argv: tuple[str, ...]
    labels: tuple[tuple[str, str], ...]
    binds: tuple[tuple[str, str], ...]
    volume: str = ""
    server_id: str = ""

    def __post_init__(self) -> None:
        require(self.role in ("server", "keeper", "probe") and _NAME.fullmatch(self.name) is not None,
                "Closed container role required")
        require(type(self.argv) is tuple and bool(self.argv) and all(type(x) is str and x and "\x00" not in x for x in self.argv),
                "Literal role argv required")
        require(type(self.labels) is tuple and type(self.binds) is tuple
                and all(type(row) is tuple and len(row) == 2 and all(type(x) is str for x in row)
                        for row in (*self.labels, *self.binds))
                and len(dict(self.labels)) == len(self.labels), "Malformed or duplicate role metadata")
        for target, source in self.binds:
            require(target in ("/workspace", "/inputs", "/probe") and Path(source).is_absolute()
                    and str(Path(source).resolve()) == source and "," not in source, "Invalid staging bind")
        targets = {x[0] for x in self.binds}
        require(len(targets) == len(self.binds), "Duplicate staging target")
        if self.role == "probe":
            require(self.argv == PROBE_ARGV and targets == {"/probe"} and not self.volume
                    and _SHA.fullmatch(self.server_id) is not None, "Probe may share only a specific server network")
        else:
            require(not self.server_id and _NAME.fullmatch(self.volume) is not None
                    and targets == ({"/workspace", "/inputs"} if self.role == "server" else set()),
                    "Server/keeper mount profile differs")


def create_argv(spec: RoleSpec, image_id: str) -> list[str]:
    """Closed, literal Docker configuration; no shell or candidate-selected flags."""
    argv = ["docker", "create", "--name", spec.name, "--pull=never", "--attach=stdout", "--attach=stderr",
        "--network=" + ("container:" + spec.server_id if spec.role == "probe" else "none"),
        "--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges:true", "--user=65534:65534",
        "--pids-limit=64", "--memory=256m", "--memory-swap=256m", "--cpus=1", "--ulimit=nofile=256:256",
        "--no-healthcheck", "--restart=no", "--log-driver=none", "--ipc=private", "--cgroupns=private",
        "--env=HOME=/tmp", "--env=PYTHONDONTWRITEBYTECODE=1", "--env=PYTHONNOUSERSITE=1",
        "--workdir=" + ("/workspace" if spec.role == "server" else "/")]
    argv += ["--env=" + key + "=" for key in ("HTTP_PROXY", "HTTPS_PROXY", "FTP_PROXY", "NO_PROXY", "ALL_PROXY",
                "http_proxy", "https_proxy", "ftp_proxy", "no_proxy", "all_proxy")]
    for target, source in spec.binds:
        argv += ["--mount", f"type=bind,source={source},target={target},readonly,bind-propagation=rprivate"]
    if spec.role == "probe":
        argv += ["--tmpfs=/tmp:" + PROBE_TMPFS]
    else:
        argv += ["--mount", "type=volume,source=" + spec.volume + ",target=/tmp,volume-nocopy"
                 + (",readonly" if spec.role == "keeper" else "")]
    for key, value in spec.labels:
        argv += ["--label", key + "=" + value]
    argv += ["--entrypoint", spec.argv[0], image_id, *spec.argv[1:]]
    return argv


def validate_role(value: dict[str, Any], spec: RoleSpec, image_id: str, runtime: dict[str, Any]) -> None:
    require(type(spec) is RoleSpec and engine._startup_runtime_valid(runtime, image_id),
            "Typed role and qualified runtime required")
    engine.immutable_inspection(value)
    config, host, mounts = value["Config"], value["HostConfig"], value["Mounts"]
    require(type(config) is dict and type(host) is dict and type(mounts) is list, "Malformed role inspection")
    engine.validate_oom_kill_default(host)
    require(_SHA.fullmatch(value.get("Id", "")) is not None and value["Name"] == "/" + spec.name
            and value["Image"] == image_id and config.get("Image") == image_id
            and value["Path"] == spec.argv[0] and value["Args"] == list(spec.argv[1:])
            and "Entrypoint" in config and "Cmd" in config and config.get("Entrypoint") == [spec.argv[0]]
            and (config.get("Cmd") == list(spec.argv[1:]) or (len(spec.argv) == 1 and config.get("Cmd") is None))
            and config.get("Labels") == dict(spec.labels), "Role identity/registered argv differs")
    expected_config = {"User": "65534:65534", "WorkingDir": "/workspace" if spec.role == "server" else "/",
        "Tty": False, "OpenStdin": False, "StdinOnce": False, "AttachStdout": True, "AttachStderr": True,
        "Healthcheck": {"Test": ["NONE"]}}
    require(all(key in config and encoded(config[key]) == encoded(expected) for key, expected in expected_config.items()),
            "Role process restrictions differ")
    expected_host = {"NetworkMode": "container:" + spec.server_id if spec.role == "probe" else "none",
        "ReadonlyRootfs": True, "Privileged": False, "AutoRemove": False, "Memory": 256 * 1024 * 1024,
        "MemorySwap": 256 * 1024 * 1024, "NanoCpus": 1000000000, "PidsLimit": 64,
        "CapDrop": ["ALL"], "SecurityOpt": ["no-new-privileges:true"], "LogConfig": {"Type": "none", "Config": {}},
        "RestartPolicy": {"Name": "no", "MaximumRetryCount": 0},
        "Ulimits": [{"Name": "nofile", "Hard": 256, "Soft": 256}], "IpcMode": "private", "CgroupnsMode": "private"}
    require(all(key in host and encoded(host[key]) == encoded(expected) for key, expected in expected_host.items())
            and (host.get("Init") is None or host.get("Init") is False)
            and all(not host.get(key) for key in ("CapAdd", "Devices", "DeviceRequests", "VolumesFrom", "Links",
                "PortBindings", "PublishAllPorts", "PidMode", "UTSMode", "UsernsMode", "ExtraHosts", "Dns")),
            "Role namespace/resource restrictions differ")
    require((host.get("Tmpfs") == {"/tmp": PROBE_TMPFS} if spec.role == "probe"
             else host.get("Tmpfs") is None or host.get("Tmpfs") == {}),
            "Role private temporary filesystem differs")
    environment = config.get("Env")
    require(type(environment) is list and all(type(x) is str and "=" in x for x in environment), "Malformed environment")
    env = dict(x.split("=", 1) for x in environment)
    require(len(env) == len(environment) and env.get("HOME") == "/tmp" and env.get("PYTHONDONTWRITEBYTECODE") == "1"
            and env.get("PYTHONNOUSERSITE") == "1" and all(env.get(k) == "" for k in ("HTTP_PROXY", "HTTPS_PROXY",
                "FTP_PROXY", "NO_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "ftp_proxy", "no_proxy", "all_proxy")),
            "Role environment differs")
    require(all(type(row) is dict for row in mounts), "Malformed mount inventory")
    inventory = {row.get("Destination"): row for row in mounts}
    expected_targets = {x[0] for x in spec.binds} | ({"/tmp"} if spec.role != "probe" else set())
    require(len(inventory) == len(mounts) and set(inventory) == expected_targets, "Role mount inventory differs")
    for target, source in spec.binds:
        row = inventory[target]
        require(row.get("Type") == "bind" and row.get("Source") == source and row.get("RW") is False
                and row.get("Propagation") == "rprivate", "Read-only exact staging mount differs")
    if spec.role != "probe":
        row = inventory["/tmp"]
        require(row.get("Type") == "volume" and row.get("Driver") == "local" and row.get("Name") == spec.volume
                and row.get("RW") is (spec.role == "server"), "Owned DB volume role differs")


def validate_state(value: dict[str, Any], state_name: str) -> None:
    state = value.get("State")
    require(type(state) is dict and state_name in ("created", "running", "exited"), "Unknown lifecycle state")
    assert isinstance(state, dict)
    require(state.get("Status") == state_name and state.get("Running") is (state_name == "running")
            and all(state.get(k) is False for k in ("Paused", "Restarting", "Dead", "OOMKilled"))
            and state.get("Error") == "" and type(value.get("RestartCount")) is int and value["RestartCount"] == 0,
            "Role lifecycle state differs")
    require(type(state.get("Pid")) is int and (state["Pid"] > 0 if state_name == "running" else state["Pid"] == 0),
            "Role PID state differs")
    started = engine._timestamp(state.get("StartedAt"))
    if state_name == "created":
        require(started is None and str(state.get("StartedAt", "")).startswith("0001-"), "Role ran before dispatch")
    else:
        created = engine._timestamp(value.get("Created"))
        require(started is not None and created is not None and created <= started, "Role start chronology differs")
    if state_name == "exited":
        finished = engine._timestamp(state.get("FinishedAt"))
        require(finished is not None and started is not None and started <= finished, "Role stop chronology differs")


def role_identity_comparison(before: dict[str, Any], after: dict[str, Any], spec: RoleSpec,
                             runtime: dict[str, Any], phase: str) -> dict[str, Any]:
    """Only mount order and pinned OOM startup default vary; full rows are retained."""
    require(phase in ("created-to-prestart", "created-to-running", "created-to-exited",
                      "running-to-running", "running-to-exited"), "Unqualified role transition")
    validate_role(before, spec, runtime["image_id"], runtime)
    validate_role(after, spec, runtime["image_id"], runtime)
    left, right = (engine.strict_json_loads(encoded(engine.immutable_inspection(x))) for x in (before, after))
    transformations: list[str] = []
    for value in (left, right):
        value["Mounts"] = {x["Destination"]: x for x in value["Mounts"]}
    old, new = left["HostConfig"]["OomKillDisable"], right["HostConfig"]["OomKillDisable"]
    if old is False and new is None and phase in ("created-to-running", "created-to-exited"):
        right["HostConfig"]["OomKillDisable"] = False
        transformations.append("pinned-runtime-startup-oom-false-to-null")
    else:
        require(old is new, "Unqualified OOM default transition")
    matches = encoded(left) == encoded(right)
    if phase.startswith("running-"):
        matches = matches and encoded(before["State"].get("StartedAt")) == encoded(after["State"].get("StartedAt"))
        if phase == "running-to-running":
            matches = matches and type(before["State"].get("Pid")) is int and before["State"]["Pid"] > 0 \
                and before["State"]["Pid"] == after["State"].get("Pid")
    return {"policy": ROLE_POLICY, "role": spec.role, "phase": phase, "matches": bool(matches),
            "before_sha256": digest(before), "after_sha256": digest(after), "runtime_sha256": digest(runtime),
            "transformations": transformations, "comparison_sha256": digest([left, right])}


@dataclass(frozen=True)
class HttpHistoryResult:
    execution_id: str
    status: str
    observations: tuple[dict[str, Any], ...]
    missing_step_ids: tuple[str, ...]
    cleanup_verified: bool
    infrastructure: tuple[str, ...]
    terminal_sha256: str
    checkpoint: ControllerCheckpoint


def run_probe(endpoint: engine.EngineEndpoint, *, expected: dict[str, Any], spec: RoleSpec,
              policy: HttpPolicy, runtime: dict[str, Any], retain: Callable[[str, bytes], None],
              label: str) -> dict[str, Any]:
    """A distinct finite trusted role, authenticated by raw Engine attach/wait/inspect."""
    require(spec.role == "probe", "Only the fixed probe has finite completion authority")
    validate_role(expected, spec, policy.image_id, runtime)
    validate_state(expected, "created")
    limit = wire.max_probe_output_bytes(policy.wire_limits)
    require(limit <= policy.stream_limit_bytes, "Trusted transcript exceeds declared process capture")
    process_policy = engine.ProcessPolicy(policy.image_id, timeout_seconds=policy.probe_timeout_seconds,
        stream_limit_bytes=limit, frame_limit_bytes=limit, transport_timeout_seconds=policy.transport_timeout_seconds)
    decoder = engine.MultiplexDecoder(process_policy)
    prefix = "/containers/" + expected["Id"]
    raw_wire: engine._Wire | None = None
    reader: threading.Thread | None = None
    errors: list[BaseException] = []
    started = False
    eof_after_start: bool | None = None
    final: dict[str, Any] = {}
    waited: dict[str, Any] = {}
    comparison: dict[str, Any] | None = None
    start_response: dict[str, Any] | None = None
    evidence: dict[str, dict[str, Any]] = {}
    def save(name: str, raw: bytes) -> None:
        path = label + "-" + name
        require(name not in evidence, "Repeated probe evidence")
        retain(path, raw)
        evidence[name] = {"path": path, "bytes": len(raw), "sha256": sha256(raw)}
    def response_observed(code: int) -> None:
        nonlocal started
        if code == 204:
            started = True
    def drain() -> None:
        nonlocal eof_after_start
        assert raw_wire is not None
        try:
            while True:
                if raw_wire.buffer:
                    chunk = bytes(raw_wire.buffer)
                    raw_wire.buffer.clear()
                    decoder.feed(chunk)
                if raw_wire.eof:
                    eof_after_start = started
                    require(eof_after_start is True, "Probe attach EOF preceded start acknowledgement")
                    decoder.eof()
                    return
                raw_wire.receive()
        except (OSError, ValueError) as error:
            errors.append(error)
    status = "completion_unproven"
    error_text: str | None = None
    try:
        save("intent.json", encoded({"protocol": PROTOCOL, "role": asdict(spec), "expected": digest(expected),
            "runtime": runtime, "policy": asdict(policy), "helper_stdout_envelope": limit,
            "helper_sha256": wire.helper_sha256()}))
        before = engine._json_control(endpoint, prefix + "/json", deadline=time.monotonic() + policy.transport_timeout_seconds,
                                      retain=save, label="prestart")
        validate_state(before, "created")
        prestart = role_identity_comparison(expected, before, spec, runtime, "created-to-prestart")
        save("prestart-comparison.json", encoded(prestart))
        require(prestart["matches"], "Probe changed before start")
        request = engine._request("POST", prefix + "/attach?stream=1&stdout=1&stderr=1&stdin=0&logs=0", upgrade=True)
        save("attach-request.bin", request)
        raw_wire = engine._Wire(endpoint, time.monotonic() + policy.transport_timeout_seconds,
            engine.HEADER_LIMIT + 2 * limit + engine.FRAME_COUNT_LIMIT * 8)
        raw_wire.send(request)
        code, headers = raw_wire.headers()
        require(code == 101 and headers.get("connection", "").lower() == "upgrade"
                and headers.get("upgrade", "").lower() == "tcp"
                and headers.get("content-type") in ("application/vnd.docker.raw-stream", "application/vnd.docker.multiplexed-stream")
                and "content-length" not in headers and "transfer-encoding" not in headers and not raw_wire.buffer,
                "Probe raw attachment failed")
        raw_wire.socket.settimeout(0)
        try:
            raw_wire.socket.recv(1, socket.MSG_PEEK)
        except BlockingIOError:
            pass
        else:
            raise ExecutionError("Probe attachment ended or produced data before start")
        finally:
            raw_wire._timeout()
        deadline = time.monotonic() + policy.probe_timeout_seconds
        raw_wire.deadline = deadline
        reader = threading.Thread(target=drain, daemon=True)
        reader.start()
        require(not errors and not decoder.complete, "Probe attachment failed before dispatch")
        code, body = engine._control(endpoint, "POST", prefix + "/start", deadline=deadline,
            retain=save, label="start", on_response=response_observed)
        start_response = {"status": code, "body_bytes": len(body), "body_sha256": sha256(body),
            "framing_complete": True, "eof_observed": True,
            "request": evidence["start-request.bin"], "response": evidence["start-response.bin"]}
        save("start-completion.json", encoded(start_response))
        require(code == 204, "Probe start rejected")
        waited = engine._json_control(endpoint, prefix + "/wait?condition=not-running", deadline=deadline,
                                       retain=save, label="wait", method="POST")
        final = engine._json_control(endpoint, prefix + "/json", deadline=time.monotonic() + policy.transport_timeout_seconds,
                                    retain=save, label="final")
        comparison = role_identity_comparison(expected, final, spec, runtime, "created-to-exited")
        save("identity-comparison.json", encoded(comparison))
        reader.join(timeout=max(0, deadline - time.monotonic()))
        require(not reader.is_alive() and not errors and decoder.complete, "Probe complete attach EOF unproven")
        completed = engine.completion_evidence(waited, final, started=started, killed=False,
                                               identity_ok=comparison["matches"] is True)
        require(completed["natural"] and completed["inspect_exit_code"] == 0, "Trusted helper did not complete normally")
        status = "completed"
    except (OSError, ValueError) as error:
        error_text = type(error).__name__ + ":" + str(error)[:512]
    finally:
        if raw_wire is not None:
            try:
                raw_wire.socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            if reader is not None:
                reader.join(timeout=policy.transport_timeout_seconds)
                if reader.is_alive():
                    status, error_text = "completion_unproven", "Probe reader did not stop"
            save("attach-response.bin", bytes(raw_wire.raw))
            raw_wire.close()
    result: dict[str, Any] = {"protocol": PROTOCOL, "role": "probe", "status": status, "error": error_text,
        "container_id": expected["Id"], "start_response": start_response,
        "completion": engine.completion_evidence(waited, final, started=started, killed=False,
            identity_ok=comparison is not None and comparison["matches"] is True),
        "identity_comparison": comparison, "attach_eof_after_start_confirmation": eof_after_start,
        "capture_complete": decoder.complete and not errors, "helper_stdout_envelope": limit}
    for kind, data in decoder.streams.items():
        save(kind + ".bin", bytes(data))
        result[kind] = {**evidence[kind + ".bin"], "observed_bytes": decoder.counts[kind],
            "truncated": decoder.counts[kind] > len(data), "complete": decoder.complete and not errors}
    result["evidence"] = dict(evidence)
    save("process.json", encoded(result))
    return result


class CandidateHttpExecution:
    """Exclusive controller journal for one registered ordered server history."""

    def __init__(self, root: Path, store: GitStore, registration: HttpRegistration,
                 recipe: HttpRecipe, policy: HttpPolicy, *, endpoint: Any = None, mode: str = "physical",
                 expected_checkpoint: ControllerCheckpoint | None = None,
                 checkpoint_sink: Callable[[ControllerCheckpoint], None] | None = None):
        require(type(registration) is HttpRegistration and type(recipe) is HttpRecipe and type(policy) is HttpPolicy
                and mode in ("physical", "fixture"), "Typed immutable HTTP registration required")
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
        self.store, self.registration, self.recipe, self.policy, self.mode = store, registration, recipe, policy, mode
        self.checkpoint_sink = checkpoint_sink
        try:
            self.endpoint = (endpoint or engine.EngineEndpoint.from_environment()) if mode == "physical" else None
            self.runtime = self._runtime()
            self.sources = evaluator_sources()
            require(self.sources == _LOADED_SOURCES, "Loaded HTTP evaluator changed")
            require(wire.max_probe_output_bytes(policy.wire_limits) <= policy.stream_limit_bytes,
                    "Trusted probe stdout capture is undersized")
            self.tree, self.files = capture_git_source(store, registration.commit_oid)
            self.fixture_files = dict(recipe.fixtures)
            require(self.tree == registration.tree_oid and source_sha256(self.files) == registration.binding.source_sha256,
                    "Registered Git source differs")
            self.binding = binding_for(self.files, recipe, policy, self.runtime,
                requirements_sha256=registration.binding.requirements_sha256)
            require(self.binding == registration.binding, "Registered runtime/evaluator/recipe/bounds differ")
            self.config = {"protocol": PROTOCOL, "mode": mode, "root": str(self.root),
                "repository": str(store.path.resolve()), "registration": asdict(registration), "policy": asdict(policy),
                "endpoint": None if self.endpoint is None else asdict(self.endpoint), "runtime": self.runtime,
                "source_manifest": source_manifest(self.files), "evaluator_sources": self.sources,
                "role_policy": ROLE_POLICY, "recipe": recipe.record(), "snapshot_protocol": SNAPSHOT_PROTOCOL,
                "helper_sha256": wire.helper_sha256(), "helper_stdout_envelope": wire.max_probe_output_bytes(policy.wire_limits)}
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

    def _runtime(self) -> dict[str, Any]:
        if self.mode == "fixture":
            return {"kind": "fixture-no-Docker"}
        require(self.endpoint is not None, "Physical runtime requires an endpoint")
        assert self.endpoint is not None
        return engine.runtime_identity(self.endpoint, self.policy.image_id,
            timeout_seconds=self.policy.transport_timeout_seconds)

    def _unchanged(self) -> None:
        self.checkpoint()
        require(self.sources == evaluator_sources() == _LOADED_SOURCES, "Loaded HTTP evaluator changed")
        require(_json(self.root / "config.json") == json.loads(encoded(self.config)), "Controller config changed")
        tree, files = capture_git_source(self.store, self.registration.commit_oid)
        require(tree == self.tree and files == self.files, "Registered immutable Git source changed")
        observed_runtime = self._runtime()
        require(encoded(observed_runtime) == encoded(self.runtime)
                and binding_for(files, self.recipe, self.policy, observed_runtime,
                    requirements_sha256=self.binding.requirements_sha256) == self.binding,
                "Runtime/environment/recipe binding changed")

    def execute_once(self) -> HttpHistoryResult:
        self._unchanged()
        if (self.root / "intent.json").exists():
            if not (self.root / "terminal.json").exists():
                raise ExecutionUnknown("Existing HTTP intent lacks an authenticated terminal; never redispatch")
            return self.verified_execution()
        execution_id = "http-" + uuid.uuid4().hex
        intent = {"protocol": PROTOCOL, "execution_id": execution_id,
            "config_sha256": digest(self.config), "registration_sha256": digest(asdict(self.registration)),
            "volume": "gossip-" + execution_id + "-volume", "keeper": "gossip-" + execution_id + "-keeper",
            "containers": ["gossip-" + execution_id + "-" + str(i) for i in range(len(self.recipe.steps))],
            "planned_steps": len(self.recipe.steps), "planned_requests": sum(x.kind == "probe" for x in self.recipe.steps),
            "planned_servers": sum(x.kind == "start" for x in self.recipe.steps), "planned_keepers": 1}
        self._retain("intent.json", encoded(intent))
        require(self.mode == "physical", "Fixture journal cannot dispatch physical evidence")
        self._dispatch(intent)
        self._unchanged()
        return self.verified_execution()

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


    def close(self) -> None:
        if not self.closed:
            self.closed = True
            self.owner.close()


    def __enter__(self) -> CandidateHttpExecution:
        return self


    def __exit__(self, *args: Any) -> None:
        self.close()


    def _inspect(self, label: str, container_id: str) -> dict[str, Any]:
        require(self.endpoint is not None and _SHA.fullmatch(container_id) is not None, "Full owned ID required")
        assert self.endpoint is not None
        return engine._json_control(self.endpoint, "/containers/" + container_id + "/json",
            deadline=time.monotonic() + self.policy.transport_timeout_seconds, retain=self._retain, label=label)

    def _control(self, label: str, container_id: str, operation: str) -> tuple[int, bytes]:
        require(self.endpoint is not None and _SHA.fullmatch(container_id) is not None
                and operation in ("start", "stop?t=" + str(self.policy.stop_timeout_seconds)), "Closed lifecycle action required")
        assert self.endpoint is not None
        return engine._control(self.endpoint, "POST", "/containers/" + container_id + "/" + operation,
            deadline=time.monotonic() + self.policy.transport_timeout_seconds + self.policy.stop_timeout_seconds,
            retain=self._retain, label=label)

    def _compare(self, label: str, before: dict[str, Any], after: dict[str, Any], spec: RoleSpec, phase: str) -> dict[str, Any]:
        comparison = role_identity_comparison(before, after, spec, self.runtime, phase)
        self._retain(label + ".json", encoded(comparison))
        require(comparison["matches"] is True, "Role identity/epoch continuity differs")
        return comparison

    def _boundary(self, label: str, baseline: dict[str, Any], spec: RoleSpec) -> dict[str, Any]:
        value = self._inspect(label, baseline["Id"])
        validate_state(value, "running")
        return self._compare(label + "-continuity", baseline, value, spec, "running-to-running")

    def _create(self, label: str, spec: RoleSpec, owned: dict[str, tuple[RoleSpec, str | None]]) -> dict[str, Any]:
        absent = self._checked(label + "-before", ["docker", "container", "ls", "--all", "--quiet", "--filter", "name=^/" + spec.name + "$"])
        require(not self._raw(absent).strip(), "Container name already exists")
        argv = create_argv(spec, self.policy.image_id)
        self._retain(label + "-create-intent.json", encoded({"spec": asdict(spec), "create_argv": argv}))
        owned[spec.name] = (spec, None)  # Preabsence + durable intent cover an uncertain create response.
        record = self._checked(label + "-create", argv)
        container_id = self._raw(record).strip().decode("ascii")
        require(_SHA.fullmatch(container_id) is not None, "Invalid created container ID")
        owned[spec.name] = (spec, container_id)
        value = self._inspect(label + "-created", container_id)
        validate_role(value, spec, self.policy.image_id, self.runtime)
        validate_state(value, "created")
        require(value["Id"] == container_id, "Created inspect names a different container")
        return value

    def _start_role(self, label: str, created: dict[str, Any], spec: RoleSpec) -> dict[str, Any]:
        require(spec.role in ("server", "keeper"), "Long-lived start requires server or keeper role")
        before = self._inspect(label + "-prestart", created["Id"])
        validate_state(before, "created")
        self._compare(label + "-prestart-comparison", created, before, spec, "created-to-prestart")
        self._retain(label + "-start-intent.json", encoded({"container_id": created["Id"], "spec": asdict(spec),
            "prestart_sha256": digest(before), "controller_action": "start-long-lived-role"}))
        code, body = self._control(label + "-start", created["Id"], "start")
        self._retain(label + "-start-completion.json", encoded({"status": code, "body_sha256": sha256(body),
            "framing_complete": True, "eof_observed": True, "container_id": created["Id"]}))
        require(code == 204, "Long-lived role start rejected")
        running = self._inspect(label + "-running", created["Id"])
        validate_state(running, "running")
        self._compare(label + "-startup-comparison", created, running, spec, "created-to-running")
        self._retain(label + "-running.json", encoded(running))
        return running

    def _stop_role(self, label: str, running: dict[str, Any], spec: RoleSpec) -> dict[str, Any]:
        self._boundary(label + "-before", running, spec)
        self._retain(label + "-intent.json", encoded({"controller_action": "explicit-stop",
            "container_id": running["Id"], "running_sha256": digest(running), "timeout_seconds": self.policy.stop_timeout_seconds,
            "natural_exit_zero_required": False}))
        code, body = self._control(label, running["Id"], "stop?t=" + str(self.policy.stop_timeout_seconds))
        require(code == 204, "Explicit server stop response incomplete or server already stopped")
        final = self._inspect(label + "-final", running["Id"])
        validate_state(final, "exited")
        comparison = self._compare(label + "-comparison", running, final, spec, "running-to-exited")
        result = {"controller_action": "explicit-stop", "status": code, "response_body_sha256": sha256(body),
            "container_id": running["Id"], "exit_code": final["State"].get("ExitCode"),
            "natural_exit_zero_required": False, "comparison": comparison, "final_inspection_sha256": digest(final)}
        self._retain(label + "-result.json", encoded(result))
        return result

    def _remove(self, label: str, spec: RoleSpec, container_id: str | None, *, force: bool) -> bool:
        """Remove only independently matched resources from the durable create intent."""
        inspected = self._command(label + "-inspect", ["docker", "inspect", "--format", "{{json .}}", spec.name])
        if not self._clean(inspected):
            absent = self._command(label + "-absence", ["docker", "container", "ls", "--all", "--quiet", "--filter", "name=^/" + spec.name + "$"])
            return self._clean(absent) and not self._raw(absent).strip()
        value = engine.strict_json_loads(self._raw(inspected))
        require(type(value) is dict and _SHA.fullmatch(value.get("Id", "")) is not None
                and (container_id is None or value["Id"] == container_id) and value.get("Name") == "/" + spec.name
                and value.get("Image") == self.policy.image_id and value.get("Config", {}).get("Labels") == dict(spec.labels),
                "Cleanup ownership differs; refusing removal")
        self._retain(label + "-intent.json", encoded({"container_id": value["Id"], "spec": asdict(spec),
            "force": force, "inspection_sha256": digest(value)}))
        removed = self._command(label + "-remove", ["docker", "rm", *(["--force"] if force else []), value["Id"]])
        absent = self._command(label + "-absence", ["docker", "container", "ls", "--all", "--quiet", "--filter", "name=^/" + spec.name + "$"])
        return self._clean(removed) and self._clean(absent) and not self._raw(absent).strip()

    def _volume_valid(self, value: Any, intent: dict[str, Any]) -> bool:
        return (type(value) is dict and value.get("Name") == intent["volume"] and value.get("Driver") == "local"
                and value.get("Options") == VOLUME_OPTIONS
                and value.get("Labels") == {"gossip.execution": intent["execution_id"], "gossip.snapshot": SNAPSHOT_PROTOCOL})

    def _labels(self, intent: dict[str, Any], role: str, epoch: int, step_id: str) -> tuple[tuple[str, str], ...]:
        return tuple(sorted({"gossip.execution": intent["execution_id"], "gossip.role": role,
            "gossip.source": self.binding.source_sha256, "gossip.fixture": self.binding.fixture_sha256,
            "gossip.epoch": str(epoch), "gossip.step": step_id, "gossip.helper": self.binding.helper_sha256}.items()))

    def _dispatch(self, intent: dict[str, Any]) -> None:
        require(self.endpoint is not None, "Physical execution requires Engine endpoint")
        assert self.endpoint is not None
        owned: dict[str, tuple[RoleSpec, str | None]] = {}
        cleanup: dict[str, bool] = {}
        results: list[dict[str, Any]] = []
        observations: list[dict[str, Any]] = []
        infrastructure: list[str] = []
        epochs: list[dict[str, Any]] = []
        volume_claimed = volume_clean = False
        keeper: dict[str, Any] | None = None
        active: dict[str, Any] | None = None
        active_spec: RoleSpec | None = None
        keeper_spec = RoleSpec("keeper", intent["keeper"],
            ("python", "-I", "-c", "import time;time.sleep(" + str(self.policy.lifetime_seconds) + ")"),
            self._labels(intent, "keeper", 0, "state-lifetime"), (), intent["volume"])
        deadline = time.monotonic() + self.policy.lifetime_seconds
        with tempfile.TemporaryDirectory(prefix="gossip-http-stage-") as temporary:
            staging = Path(temporary).resolve()
            workspace, inputs = staging / "workspace", staging / "inputs"
            for root, files, dirs in ((workspace, self.files, ()), (inputs, self.fixture_files, self.recipe.directories)):
                root.mkdir(mode=0o755)
                for name in dirs:
                    (root / name).mkdir(parents=True, exist_ok=True, mode=0o755)
                for name, raw in files.items():
                    path = root / name
                    path.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
                    path.write_bytes(raw)
                    path.chmod(0o444)
                finite._verify_tree(root, files, list(dirs))
            self._retain("staging.json", encoded({"workspace": str(workspace), "inputs": str(inputs),
                "source_manifest": source_manifest(self.files), "fixture_manifest": source_manifest(self.fixture_files)}))
            try:
                before = self._checked("volume-before", ["docker", "volume", "ls", "--quiet", "--filter", "name=^" + intent["volume"] + "$"])
                require(not self._raw(before).strip(), "Volume name already exists")
                create = ["docker", "volume", "create", "--driver", "local", "--label", "gossip.execution=" + intent["execution_id"],
                          "--label", "gossip.snapshot=" + SNAPSHOT_PROTOCOL]
                for key, value in VOLUME_OPTIONS.items():
                    create += ["--opt", key + "=" + value]
                create.append(intent["volume"])
                self._retain("volume-intent.json", encoded({"argv": create, "volume": intent["volume"]}))
                volume_claimed = True
                created_volume = self._checked("volume-create", create)
                require(self._raw(created_volume).strip() == intent["volume"].encode(), "Created volume identity differs")
                inspected = self._checked("volume-created", ["docker", "volume", "inspect", "--format", "{{json .}}", intent["volume"]])
                require(self._volume_valid(engine.strict_json_loads(self._raw(inspected)), intent), "Volume bounds/identity differ")
                keeper_created = self._create("keeper", keeper_spec, owned)
                keeper = self._start_role("keeper", keeper_created, keeper_spec)
                epoch = 0
                for index, step in enumerate(self.recipe.steps):
                    label = "step-" + str(index).zfill(3)
                    self._unchanged()
                    require(time.monotonic() < deadline, "HTTP history observation lifetime exceeded")
                    finite._verify_tree(workspace, self.files)
                    finite._verify_tree(inputs, self.fixture_files, list(self.recipe.directories))
                    self._boundary(label + "-keeper-before", keeper, keeper_spec)
                    row: dict[str, Any] = {"step_id": step.step_id, "step_index": index, "kind": step.kind, "label": label}
                    if step.kind == "start":
                        require(active is None and active_spec is None, "Previous server not removed")
                        epoch += 1
                        active_spec = RoleSpec("server", intent["containers"][index], self.recipe.server_argv,
                            self._labels(intent, "server", epoch, step.step_id),
                            (("/workspace", str(workspace)), ("/inputs", str(inputs))), intent["volume"])
                        created = self._create(label, active_spec, owned)
                        active = self._start_role(label, created, active_spec)
                        require(active["Id"] not in {x["server_id"] for x in epochs}, "Server epoch reused an ID")
                        epoch_record = {"epoch": epoch, "server_id": active["Id"], "name": active_spec.name,
                            "started_at": active["State"]["StartedAt"], "pid": active["State"]["Pid"],
                            "database_volume": intent["volume"], "database_path": self.recipe.database_path,
                            "source_sha256": self.binding.source_sha256, "fixture_sha256": self.binding.fixture_sha256,
                            "running_inspection_sha256": digest(active), "keeper_id": keeper["Id"],
                            "keeper_started_at": keeper["State"]["StartedAt"]}
                        epochs.append(epoch_record)
                        row.update(epoch_record)
                    elif step.kind == "stop":
                        require(active is not None and active_spec is not None, "Missing active server")
                        assert active is not None and active_spec is not None
                        row.update(epoch=epoch, server_id=active["Id"],
                            stop=self._stop_role(label + "-stop", active, active_spec))
                        cleanup[active_spec.name] = self._remove(label + "-retire", active_spec, active["Id"], force=False)
                        require(cleanup[active_spec.name], "Server removal unproven before next epoch")
                        row["removed"] = True
                        active, active_spec = None, None
                    else:
                        require(active is not None and active_spec is not None, "Missing active server")
                        assert active is not None and active_spec is not None
                        row.update(epoch=epoch, server_id=active["Id"], database_volume=intent["volume"],
                            database_path=self.recipe.database_path, source_sha256=self.binding.source_sha256,
                            fixture_sha256=self.binding.fixture_sha256, commit_oid=self.registration.commit_oid,
                            tree_oid=self.registration.tree_oid, helper_sha256=self.binding.helper_sha256,
                            binding=asdict(self.binding), keeper_id=keeper["Id"], authenticated=False)
                        row["continuity_before"] = self._boundary(label + "-server-before", active, active_spec)
                        request = engine.strict_json_loads(step.request_json)
                        probe_input = wire.build_probe_input(request, self.recipe.port, self.policy.wire_limits)
                        probe_files = {"helper.py": wire.helper_source(), "request.json": probe_input}
                        probe_root = staging / label
                        probe_root.mkdir(mode=0o755)
                        for name, raw in probe_files.items():
                            (probe_root / name).write_bytes(raw)
                            (probe_root / name).chmod(0o444)
                        finite._verify_tree(probe_root, probe_files)
                        self._retain(label + "-probe-input.json", probe_input)
                        self._retain(label + "-probe-source.json", encoded({"helper_sha256": sha256(probe_files["helper.py"]),
                            "request_input_sha256": sha256(probe_input), "request_sha256": sha256(wire.request_bytes(request, self.recipe.port)),
                            "argv": PROBE_ARGV, "server_epoch": epoch, "server_id": active["Id"]}))
                        probe_spec = RoleSpec("probe", intent["containers"][index], PROBE_ARGV,
                            self._labels(intent, "probe", epoch, step.step_id), (("/probe", str(probe_root)),), server_id=active["Id"])
                        probe_created = self._create(label + "-probe", probe_spec, owned)
                        row["probe_id"] = probe_created["Id"]
                        process_result = run_probe(self.endpoint, expected=probe_created, spec=probe_spec, policy=self.policy,
                            runtime=self.runtime, retain=self._retain, label=label + "-probe")
                        row["process"] = process_result
                        # Retain the request observation even if server continuity or helper completion subsequently fails.
                        try:
                            row["continuity_after"] = self._boundary(label + "-server-after", active, active_spec)
                            row["keeper_continuity"] = self._boundary(label + "-probe-keeper-after", keeper, keeper_spec)
                            finite._verify_tree(probe_root, probe_files)
                            finite._verify_tree(workspace, self.files)
                            finite._verify_tree(inputs, self.fixture_files, list(self.recipe.directories))
                            self._unchanged()
                            require(process_result["status"] == "completed", "Trusted helper completion unavailable")
                            require(self._raw(process_result, "stderr") == b"", "Trusted helper emitted diagnostics")
                            observation = wire.decode_probe_output(self._raw(process_result), request, self.recipe.port, self.policy.wire_limits)
                            row["exchange_complete"] = observation.exchange_complete
                            row["sent_complete"] = observation.sent_complete
                            row["authenticated"] = True
                        except (OSError, ValueError) as error:
                            row["observation_error"] = type(error).__name__ + ":" + str(error)[:512]
                        try:
                            cleanup[probe_spec.name] = self._remove(label + "-probe-retire", probe_spec, probe_created["Id"], force=True)
                        except (OSError, ValueError, subprocess.SubprocessError) as error:
                            cleanup[probe_spec.name] = False
                            row["cleanup_error"] = type(error).__name__ + ":" + str(error)[:512]
                        row["probe_removed"] = cleanup[probe_spec.name]
                        observations.append(row)
                        self._retain(label + "-observation.json", encoded(row))
                        require(row["authenticated"] and cleanup[probe_spec.name], "Probe observation/provenance unavailable")
                    self._boundary(label + "-keeper-after", keeper, keeper_spec)
                    finite._verify_tree(workspace, self.files)
                    finite._verify_tree(inputs, self.fixture_files, list(self.recipe.directories))
                    self._retain(label + "-result.json", encoded(row))
                    results.append(row)
            except (OSError, ValueError, subprocess.SubprocessError) as error:
                infrastructure.append(type(error).__name__ + ":" + str(error)[:512])
            finally:
                # Clean each owned resource once; preserve failed cleanup evidence and do not retry until green.
                for ordinal, (name, (spec, container_id)) in enumerate(reversed(list(owned.items()))):
                    if name in cleanup:
                        continue
                    try:
                        if spec.role == "keeper" and keeper is not None:
                            require(all(cleanup.values()), "Other container cleanup incomplete; keeper retained")
                            try:
                                self._stop_role("keeper-stop", keeper, keeper_spec)
                            except (OSError, ValueError, subprocess.SubprocessError) as error:
                                infrastructure.append("keeper-stop:" + type(error).__name__ + ":" + str(error)[:512])
                        cleanup[name] = self._remove("cleanup-" + str(ordinal).zfill(3), spec, container_id, force=True)
                    except (OSError, ValueError, subprocess.SubprocessError) as error:
                        cleanup[name] = False
                        infrastructure.append("cleanup:" + type(error).__name__ + ":" + str(error)[:512])
                if volume_claimed:
                    try:
                        require(all(cleanup.values()), "Container cleanup incomplete; volume retained")
                        inspected = self._command("volume-cleanup-inspect", ["docker", "volume", "inspect", "--format", "{{json .}}", intent["volume"]])
                        if self._clean(inspected):
                            require(self._volume_valid(engine.strict_json_loads(self._raw(inspected)), intent), "Volume cleanup ownership differs")
                            removed = self._command("volume-remove", ["docker", "volume", "rm", intent["volume"]])
                            require(self._clean(removed), "Volume removal failed")
                        absent = self._checked("volume-absence", ["docker", "volume", "ls", "--quiet", "--filter", "name=^" + intent["volume"] + "$"])
                        volume_clean = not self._raw(absent).strip()
                    except (OSError, ValueError, subprocess.SubprocessError) as error:
                        infrastructure.append("volume-cleanup:" + type(error).__name__ + ":" + str(error)[:512])
        self._retain("terminal.json", encoded({"protocol": PROTOCOL, "mode": self.mode,
            "intent_sha256": sha256(_read(self.root / "intent.json")), "steps": results, "observations": observations,
            "epochs": epochs, "cleanup": cleanup, "volume_cleanup": volume_clean, "infrastructure": infrastructure,
            "planned_requests": intent["planned_requests"], "observed_probe_records": len(observations),
            "observed_requests": sum(row.get("authenticated") is True and row.get("sent_complete") is True for row in observations),
            "unknown_request_outcomes": sum(row.get("authenticated") is not True for row in observations),
            "create_intents": len(owned), "unconfirmed_creates": sum(cid is None for _, cid in owned.values()),
            "created_helpers": sum(spec.role == "probe" and cid is not None for spec, cid in owned.values()),
            "created_servers": sum(spec.role == "server" and cid is not None for spec, cid in owned.values()),
            "created_keepers": sum(spec.role == "keeper" and cid is not None for spec, cid in owned.values()),
            "start_response_records": [{"path": name, "sha256": value, "status": _json(self.root / name)["status"]}
                for name, value in sorted(self._authenticated.items()) if name.endswith("-start-completion.json")],
            "evaluator_sources_after": evaluator_sources()}))

    def verified_execution(self) -> HttpHistoryResult:
        self._unchanged()
        require(self.mode == "physical", "Fixture journal cannot authenticate physical evidence")
        intent, terminal = _json(self.root / "intent.json"), _json(self.root / "terminal.json")
        require(intent["protocol"] == PROTOCOL and intent["config_sha256"] == digest(self.config)
                and intent["registration_sha256"] == digest(asdict(self.registration)), "HTTP intent identity differs")
        require(terminal["protocol"] == PROTOCOL and terminal["mode"] == "physical"
                and terminal["intent_sha256"] == sha256(_read(self.root / "intent.json"))
                and terminal["evaluator_sources_after"] == self.sources, "HTTP terminal identity differs")
        rows = terminal["steps"]
        require(type(rows) is list and len(rows) <= len(self.recipe.steps), "Invalid step census")
        for index, row in enumerate(rows):
            step = self.recipe.steps[index]
            label = "step-" + str(index).zfill(3)
            require(row["step_id"] == step.step_id and row["step_index"] == index and row["kind"] == step.kind
                    and row["label"] == label and row == _json(self.root / (label + "-result.json")), "Step order/evidence differs")
        observations: list[dict[str, Any]] = []
        for row in terminal["observations"]:
            index = row["step_index"]
            require(type(index) is int and 0 <= index < len(self.recipe.steps), "Invalid probe index")
            step = self.recipe.steps[index]
            require(step.kind == "probe" and row["step_id"] == step.step_id
                    and row == _json(self.root / (row["label"] + "-observation.json")), "Probe observation differs")
            observed = dict(row)
            observed["wire"] = None
            if row["authenticated"] is True:
                require(row["process"]["status"] == "completed" and row["process"]["completion"]["natural"] is True
                        and row["process"]["completion"]["inspect_exit_code"] == 0
                        and all(row[k]["matches"] is True for k in ("continuity_before", "continuity_after", "keeper_continuity")),
                        "Authenticated probe is missing its completion/continuity proof")
                observed["wire"] = wire.decode_probe_output(self._raw(row["process"]),
                    engine.strict_json_loads(step.request_json), self.recipe.port, self.policy.wire_limits)
            observations.append(observed)
        cleanup = terminal["volume_cleanup"] is True and bool(terminal["cleanup"]) and all(x is True for x in terminal["cleanup"].values())
        missing = tuple(x.step_id for x in self.recipe.steps[len(rows):])
        completed = not missing and cleanup and not terminal["infrastructure"] and all(x["authenticated"] is True for x in observations)
        return HttpHistoryResult(intent["execution_id"], "completed" if completed else "observation_unavailable",
            tuple(observations), missing, cleanup, tuple(terminal["infrastructure"]),
            sha256(_read(self.root / "terminal.json")), self.checkpoint())


_LOADED_SOURCES = evaluator_sources()
