"""Source-bound finite candidate processes over one explicitly local Docker socket.

This module never creates/removes containers or grades candidate output. The
controller owns immutable staging, volume verification, container creation and
cleanup. Raw Engine control traffic is distinct from candidate stdout/stderr.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
from datetime import datetime
import json
import math
import os
import posixpath
from pathlib import Path
import re
import socket
import stat
import subprocess
import threading
import time
from typing import Any, Callable

from .sandbox import DockerValidator

PROTOCOL = "candidate-client-process-v4"
API_VERSION = "1.47"
COMMAND_POLICY_ID = "docker-explicit-entrypoint-empty-command-v1"
START_RESPONSE_PROTOCOL = "docker-start-http-observation-v1"
EMPTY_COMMAND_PLAN_SHA256 = "6c99fe8badb01281f0adb82265e5260d1022a818cf4edc18cf97da41912981d3"
IDENTITY_POLICY_ID = "docker-inspect-mount-inventory-v1"
MOUNT_PLAN_SHA256 = "f574060cfa8192c7e67da8a000309da5a7d2f01e4fe69edff4d2ea011c35a521"
STARTUP_POLICY_ID = "docker-hostconfig-oom-kill-default-v1"
COMPATIBILITY_PLAN_SHA256 = "7b8d89a9003594551c2677a9975d715b4b4e3584746874cc446601722f411b2d"
_ENGINE_COMMIT = "6bc6209b88a7a834c91f77d848e025c79e0227a1"
_STARTUP_PHASES = ("keeper-created-to-running", "candidate-created-to-exited")
_IDENTITY_PHASES = _STARTUP_PHASES + ("candidate-created-to-prestart", "keeper-running-to-running")
HEADER_LIMIT = 32 * 1024
CONTROL_LIMIT = 1024 * 1024
FRAME_COUNT_LIMIT = 65536
READ_CHUNK = 65536
_IMAGE = re.compile(r"sha256:[0-9a-f]{64}\Z")
_ID = re.compile(r"[0-9a-f]{64}\Z")
_LABEL = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}\Z")
Retain = Callable[[str, bytes], None]


class ProcessError(ValueError):
    """Transport/qualification failure, never a candidate product verdict."""


class OutputLimit(ProcessError):
    """A declared observation resource bound was reached."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ProcessError(message)


def _encoded(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
                      allow_nan=False).encode()


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        _require(key not in result, "Duplicate Engine JSON key")
        result[key] = value
    return result


def _invalid_constant(value: str) -> Any:
    raise ProcessError("Nonfinite Engine JSON number")


def strict_json_loads(raw: bytes | str) -> Any:
    """Decode bounded raw JSON without discarding duplicate object keys."""
    _require(type(raw) in (bytes, str), "Raw JSON bytes or text required")
    try:
        body = raw if isinstance(raw, bytes) else raw.encode("utf-8")
        _require(len(body) <= CONTROL_LIMIT, "JSON body exceeds control bound")
        value = json.loads(body.decode("utf-8"), object_pairs_hook=_strict_object,
                           parse_constant=_invalid_constant)
        _comparison_bytes(value)
        return value
    except (ValueError, UnicodeError, RecursionError, OverflowError) as error:
        raise ProcessError("Invalid bounded strict JSON: " + str(error)) from error


def _timestamp(value: Any) -> tuple[datetime, int] | None:
    if type(value) is not str:
        return None
    match = re.fullmatch(r"([1-9][0-9]{3}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(?:\.([0-9]{1,9}))?Z", value)
    if match is None:
        return None
    try:
        return datetime.fromisoformat(match[1] + "+00:00"), int((match[2] or "0").ljust(9, "0"))
    except ValueError:
        return None



def evaluator_sources() -> dict[str, str]:
    result = {"gossip_harness/" + name: _sha(Path(__file__).with_name(name).read_bytes())
              for name in ("candidate_client_process_v4.py", "sandbox.py")}
    for plan in ("analysis/candidate-b03-runtime-compatibility-plan-v1.json",
                 "analysis/candidate-b03-mount-inventory-plan-v1.json",
                 "analysis/candidate-b03-empty-command-plan-v3.json"):
        result[plan] = _sha((Path(__file__).parent.parent / plan).read_bytes())
    return result


@dataclass(frozen=True)
class ProcessPolicy:
    image_id: str
    timeout_seconds: float = 30
    stream_limit_bytes: int = 4 * 1024 * 1024
    frame_limit_bytes: int = 4 * 1024 * 1024
    transport_timeout_seconds: float = 15

    def __post_init__(self) -> None:
        _require(type(self.image_id) is str and bool(_IMAGE.fullmatch(self.image_id)),
                 "Full pinned image ID required")
        for number in (self.timeout_seconds, self.transport_timeout_seconds):
            _require(type(number) in (int, float) and math.isfinite(number)
                     and 0 < number <= 120, "Invalid process deadline")
        for number in (self.stream_limit_bytes, self.frame_limit_bytes):
            _require(type(number) is int and 1 <= number <= 16 * 1024 * 1024,
                     "Invalid process capture bound")


@dataclass(frozen=True)
class EngineEndpoint:
    socket_path: str
    device: int
    inode: int

    @classmethod
    def from_url(cls, value: str) -> EngineEndpoint:
        _require(type(value) is str and value.startswith("unix:///"),
                 "An explicit local Unix Docker endpoint is required")
        raw = value[7:]
        _require(not any(c in raw for c in ("\x00", "?", "#", "\n", "\r")),
                 "Invalid Unix Docker endpoint")
        path = Path(raw).resolve(strict=True)
        info = path.stat()
        _require(stat.S_ISSOCK(info.st_mode), "Docker endpoint is not a Unix socket")
        return cls(str(path), info.st_dev, info.st_ino)

    @classmethod
    def from_environment(cls) -> EngineEndpoint:
        # Docker's explicit context overrides DOCKER_HOST. Never substitute a
        # default socket after a selected endpoint fails or names a remote host.
        environment = DockerValidator._environment()
        if not environment.get("DOCKER_CONTEXT") and environment.get("DOCKER_HOST"):
            return cls.from_url(environment["DOCKER_HOST"])
        process = subprocess.Popen(
            ["docker", "context", "inspect", "--format", "{{json .Endpoints.docker.Host}}"],
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            env=environment)
        streams = [bytearray(), bytearray()]
        exceeded = threading.Event()
        def drain(index: int) -> None:
            pipe = process.stdout if index == 0 else process.stderr
            assert pipe is not None
            try:
                while chunk := pipe.read(4096):
                    available = HEADER_LIMIT - len(streams[index])
                    streams[index].extend(chunk[:max(0, available)])
                    if len(chunk) > available:
                        exceeded.set()
                        process.kill()
                        break
            finally:
                pipe.close()
        readers = [threading.Thread(target=drain, args=(i,), daemon=True) for i in range(2)]
        for reader in readers:
            reader.start()
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired as error:
            process.kill()
            process.wait(timeout=5)
            raise ProcessError("Docker context resolution timed out") from error
        finally:
            for reader in readers:
                reader.join(timeout=5)
        _require(process.returncode == 0 and not exceeded.is_set()
                 and all(not reader.is_alive() for reader in readers), "Docker context resolution failed")
        try:
            value = json.loads(bytes(streams[0]))
        except (ValueError, UnicodeError) as error:
            raise ProcessError("Invalid Docker context endpoint") from error
        return cls.from_url(value)

    def validate(self) -> None:
        _require(type(self.socket_path) is str and self.socket_path.startswith("/")
                 and str(Path(self.socket_path).resolve(strict=True)) == self.socket_path,
                 "Canonical Docker socket required")
        info = Path(self.socket_path).stat()
        _require(stat.S_ISSOCK(info.st_mode) and (info.st_dev, info.st_ino) == (self.device, self.inode),
                 "Docker endpoint identity changed")


class MultiplexDecoder:
    """Incremental Docker non-TTY frames; no candidate-provided delimiter."""

    def __init__(self, policy: ProcessPolicy):
        self.policy = policy
        self.pending = bytearray()
        self.streams = {"stdout": bytearray(), "stderr": bytearray()}
        self.counts = {"stdout": 0, "stderr": 0}
        self.frames = 0
        self.remaining = 0
        self.kind = ""
        self.complete = False

    def feed(self, chunk: bytes) -> None:
        _require(not self.complete, "Bytes after stream EOF")
        self.pending.extend(chunk)
        while self.pending:
            if not self.remaining:
                if len(self.pending) < 8:
                    return
                header = bytes(self.pending[:8])
                del self.pending[:8]
                _require(header[0] in (1, 2) and header[1:4] == b"\0\0\0",
                         "Invalid Docker stream frame")
                self.frames += 1
                if self.frames > FRAME_COUNT_LIMIT:
                    raise OutputLimit("Docker frame count exceeded")
                self.kind = "stdout" if header[0] == 1 else "stderr"
                self.remaining = int.from_bytes(header[4:], "big")
                if self.remaining > self.policy.frame_limit_bytes:
                    raise OutputLimit("Docker frame size exceeded")
                if not self.remaining:
                    continue
            size = min(self.remaining, len(self.pending))
            raw = bytes(self.pending[:size])
            del self.pending[:size]
            self.remaining -= size
            self.counts[self.kind] += size
            available = self.policy.stream_limit_bytes - len(self.streams[self.kind])
            self.streams[self.kind].extend(raw[:max(0, available)])
            if size > available:
                raise OutputLimit("Candidate stream capture exceeded")

    def eof(self) -> None:
        _require(not self.remaining and not self.pending, "Truncated Docker frame")
        self.complete = True


class _Wire:
    def __init__(self, endpoint: EngineEndpoint, deadline: float, limit: int):
        endpoint.validate()
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.deadline = deadline
        self.limit = limit
        self.raw = bytearray()
        self.buffer = bytearray()
        self.eof = False
        try:
            self._timeout()
            self.socket.connect(endpoint.socket_path)
        except BaseException:
            self.socket.close()
            raise

    def _timeout(self) -> None:
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Docker transport deadline reached")
        self.socket.settimeout(remaining)

    def send(self, value: bytes) -> None:
        self._timeout()
        self.socket.sendall(value)

    def receive(self) -> None:
        self._timeout()
        # At most one sentinel byte beyond the prospective wire bound is read.
        chunk = self.socket.recv(min(READ_CHUNK, max(1, self.limit - len(self.raw) + 1)))
        if not chunk:
            self.eof = True
            return
        available = self.limit - len(self.raw)
        self.raw.extend(chunk[:max(0, available)])
        if len(chunk) > available:
            raise OutputLimit("Docker wire response exceeded")
        self.buffer.extend(chunk)

    def line(self, limit: int) -> bytes:
        while True:
            index = self.buffer.find(b"\r\n")
            if index >= 0:
                _require(index + 2 <= limit, "HTTP line exceeded")
                value = bytes(self.buffer[:index])
                del self.buffer[:index + 2]
                return value
            _require(len(self.buffer) < limit and not self.eof, "Truncated or excessive HTTP line")
            self.receive()

    def exact(self, size: int) -> bytes:
        while len(self.buffer) < size and not self.eof:
            self.receive()
        _require(len(self.buffer) >= size, "Truncated HTTP body")
        value = bytes(self.buffer[:size])
        del self.buffer[:size]
        return value

    def headers(self) -> tuple[int, dict[str, str]]:
        first = self.line(HEADER_LIMIT)
        match = re.fullmatch(rb"HTTP/1\.[01] ([0-9]{3}) [^\r\n]*", first)
        _require(match is not None, "Invalid HTTP response line")
        assert match is not None
        size = len(first) + 2
        result: dict[str, str] = {}
        for _ in range(128):
            raw = self.line(HEADER_LIMIT - size)
            size += len(raw) + 2
            if not raw:
                return int(match[1]), result
            _require(b":" in raw and raw[:1] not in (b" ", b"\t"), "Invalid HTTP header")
            key, value = raw.split(b":", 1)
            _require(bool(re.fullmatch(rb"[!#$%&'*+.^_`|~0-9A-Za-z-]+", key)), "Invalid HTTP header name")
            key_text = key.decode("ascii").lower()
            _require(key_text not in result and not any(c < 32 and c != 9 for c in value),
                     "Duplicate or malformed HTTP header")
            result[key_text] = value.decode("latin1").strip()
        raise ProcessError("HTTP header count exceeded")

    def body(self, status: int, headers: dict[str, str]) -> bytes:
        length, transfer = headers.get("content-length"), headers.get("transfer-encoding")
        _require(not (length is not None and transfer is not None), "Ambiguous HTTP framing")
        result = bytearray()
        if status in (204, 304):
            _require(transfer is None and length in (None, "0"), "Invalid no-body response")
        elif transfer is not None:
            _require(transfer.lower() == "chunked", "Unsupported HTTP transfer encoding")
            for _ in range(FRAME_COUNT_LIMIT):
                line = self.line(128)
                _require(bool(re.fullmatch(rb"[0-9a-fA-F]{1,16}", line)), "Invalid HTTP chunk size")
                size = int(line, 16)
                if not size:
                    _require(self.line(2) == b"", "HTTP trailers unsupported")
                    break
                if len(result) + size > CONTROL_LIMIT:
                    raise OutputLimit("Docker control body exceeded")
                result.extend(self.exact(size))
                _require(self.exact(2) == b"\r\n", "Invalid HTTP chunk terminator")
            else:
                raise OutputLimit("HTTP chunk count exceeded")
        elif length is not None:
            _require(bool(re.fullmatch(r"[0-9]{1,12}", length)), "Invalid HTTP content length")
            if int(length) > CONTROL_LIMIT:
                raise OutputLimit("Docker control body exceeded")
            result.extend(self.exact(int(length)))
        else:
            while not self.eof:
                if len(result) + len(self.buffer) > CONTROL_LIMIT:
                    raise OutputLimit("Docker control body exceeded")
                result.extend(self.buffer)
                self.buffer.clear()
                self.receive()
            result.extend(self.buffer)
            self.buffer.clear()
        # Every control request asks Connection: close. Prove complete framing,
        # including absence of a second body or trailing bytes, before trusting it.
        while not self.eof:
            _require(not self.buffer, "Trailing HTTP response bytes")
            self.receive()
        _require(not self.buffer and len(result) <= CONTROL_LIMIT, "Trailing/excessive HTTP body")
        return bytes(result)

    def close(self) -> None:
        self.socket.close()


def _request(method: str, path: str, *, upgrade: bool = False) -> bytes:
    connection = "Upgrade" if upgrade else "close"
    extra = "Upgrade: tcp\r\n" if upgrade else ""
    return (f"{method} /v{API_VERSION}{path} HTTP/1.1\r\nHost: docker\r\n"
            f"Connection: {connection}\r\n{extra}Content-Length: 0\r\n\r\n").encode("ascii")


def _control(endpoint: EngineEndpoint, method: str, path: str, *, deadline: float,
             retain: Retain, label: str,
             on_response: Callable[[int], None] | None = None) -> tuple[int, bytes]:
    request = _request(method, path)
    retain(label + "-request.bin", request)
    wire = _Wire(endpoint, deadline, HEADER_LIMIT + CONTROL_LIMIT + FRAME_COUNT_LIMIT * 20)
    try:
        wire.send(request)
        status, headers = wire.headers()
        body = wire.body(status, headers)
        if on_response is not None:
            on_response(status)
        return status, body
    finally:
        try:
            retain(label + "-response.bin", bytes(wire.raw))
        finally:
            wire.close()


def _json_control(endpoint: EngineEndpoint, path: str, *, deadline: float,
                  retain: Retain, label: str, method: str = "GET") -> dict[str, Any]:
    status, body = _control(endpoint, method, path, deadline=deadline, retain=retain, label=label)
    _require(status == 200, "Engine control response failed")
    try:
        value = strict_json_loads(body)
    except (ValueError, UnicodeError) as error:
        raise ProcessError("Invalid Engine JSON response") from error
    _require(type(value) is dict, "Engine object response required")
    return value


def runtime_identity(endpoint: EngineEndpoint, image_id: str, *,
                     retain: Retain | None = None, label: str = "runtime",
                     timeout_seconds: float = 15) -> dict[str, Any]:
    ProcessPolicy(image_id, transport_timeout_seconds=timeout_seconds)
    _require(bool(_LABEL.fullmatch(label)), "Unsafe runtime label")
    sink: Retain = retain if retain is not None else lambda _name, _raw: None
    deadline = time.monotonic() + timeout_seconds
    version = _json_control(endpoint, "/version", deadline=deadline, retain=sink, label=label + "-version")
    info = _json_control(endpoint, "/info", deadline=deadline, retain=sink, label=label + "-info")
    image = _json_control(endpoint, "/images/" + image_id + "/json", deadline=deadline,
                          retain=sink, label=label + "-image")
    def api_tuple(value: Any) -> tuple[int, int]:
        _require(type(value) is str and bool(re.fullmatch(r"[0-9]+\.[0-9]+", value)),
                 "Invalid Engine API version")
        first, second = value.split(".")
        return int(first), int(second)
    _require(api_tuple(version.get("MinAPIVersion")) <= api_tuple(API_VERSION)
             <= api_tuple(version.get("ApiVersion")), "Pinned API unsupported")
    _require(version.get("Os") == "linux" and info.get("OSType") == "linux"
             and type(info.get("ID")) is str and bool(info["ID"])
             and image.get("Id") == image_id and image.get("Os") == "linux",
             "Runtime or image identity differs")
    _require(type(version.get("GitCommit")) is str
             and bool(re.fullmatch(r"[0-9a-f]{7,40}", version["GitCommit"]))
             and type(info.get("OomKillDisable")) is bool
             and type(info.get("CgroupVersion")) is str and info["CgroupVersion"] in ("1", "2")
             and type(info.get("CgroupDriver")) is str and bool(info["CgroupDriver"]),
             "Runtime compatibility capabilities unavailable")
    result = {"protocol": PROTOCOL, "endpoint": asdict(endpoint), "api_version": API_VERSION,
              "os": version["Os"], "engine_git_commit": version["GitCommit"],
              "cgroup_version": info["CgroupVersion"], "cgroup_driver": info["CgroupDriver"],
              "oom_kill_disable_supported": info["OomKillDisable"],
              "daemon_id": info["ID"], "engine_version": version.get("Version"),
              "architecture": version.get("Arch"), "kernel_version": version.get("KernelVersion"),
              "image_id": image_id, "image_inspect_sha256": _sha(_encoded(image))}
    sink(label + ".json", _encoded(result))
    return result



def startup_policy() -> dict[str, Any]:
    """Return a fresh, prospective rule; no candidate expectation is changed."""
    return {"policy_id": STARTUP_POLICY_ID, "protocol": PROTOCOL,
        "compatibility_plan_sha256": COMPATIBILITY_PLAN_SHA256,
        "allowed_phases": list(_STARTUP_PHASES), "path": "HostConfig.OomKillDisable",
        "allowed_raw_values": [False, None], "strict_json_types": True,
        "allowed_transition": {"before": False, "after": None, "comparison_after": False},
        "unchanged_pairs": [[False, False], [None, None]],
        "runtime_gate": {"os": "linux", "engine_version": "29.2.1", "api_version": API_VERSION,
            "engine_git_commit_values": [_ENGINE_COMMIT[:7], _ENGINE_COMMIT],
            "cgroup_version": "2", "oom_kill_disable_supported": False},
        "source_commit": _ENGINE_COMMIT,
        "scope": "OomKillDisable startup subrule; composed with the separate mount inventory policy",
        "other_fields": "this subrule changes no other field; identity policy separately permits top-level Mounts order",
        "prestart": "OomKillDisable raw-exact",
        "subsequent_keeper_boundaries": "OomKillDisable raw-exact"}


def startup_policy_sha256() -> str:
    return _sha(_encoded(startup_policy()))


def validate_oom_kill_default(hostconfig: Any) -> None:
    """Only explicit JSON false or null is admissible; never bool-coerce."""
    _require(type(hostconfig) is dict and "OomKillDisable" in hostconfig,
             "OomKillDisable field missing")
    value = hostconfig["OomKillDisable"]
    _require(value is False or value is None, "OomKillDisable must be explicit false or null")


def _comparison_bytes(value: Any) -> bytes:
    count = 0
    def validate(item: Any, depth: int) -> None:
        nonlocal count
        count += 1
        _require(count <= 100000 and depth <= 32, "Comparison structure exceeds bounds")
        if item is None or type(item) in (str, bool, int):
            return
        if type(item) is float:
            _require(math.isfinite(item), "Nonfinite comparison number")
            return
        if type(item) is list:
            for child in item:
                validate(child, depth + 1)
            return
        _require(type(item) is dict and all(type(key) is str for key in item),
                 "Comparison requires strict JSON values and string keys")
        for child in item.values():
            validate(child, depth + 1)
    validate(value, 0)
    raw = _encoded(value)
    _require(len(raw) <= CONTROL_LIMIT, "Comparison exceeds control body bound")
    return raw


def _startup_runtime_valid(runtime: dict[str, Any], image_id: Any) -> bool:
    gate = startup_policy()["runtime_gate"]
    scalar_keys = ("os", "engine_version", "api_version", "cgroup_version")
    endpoint = runtime.get("endpoint")
    return (runtime.get("protocol") == PROTOCOL
        and all(type(runtime.get(key)) is str and runtime[key] == gate[key] for key in scalar_keys)
        and type(runtime.get("engine_git_commit")) is str
        and runtime["engine_git_commit"] in gate["engine_git_commit_values"]
        and runtime.get("oom_kill_disable_supported") is False
        and all(type(runtime.get(key)) is str and bool(runtime[key]) for key in
                ("daemon_id", "architecture", "kernel_version", "cgroup_driver"))
        and type(runtime.get("image_id")) is str and bool(_IMAGE.fullmatch(runtime["image_id"]))
        and runtime["image_id"] == image_id
        and type(runtime.get("image_inspect_sha256")) is str
        and bool(_ID.fullmatch(runtime["image_inspect_sha256"]))
        and type(endpoint) is dict and set(endpoint) == {"socket_path", "device", "inode"}
        and type(endpoint["socket_path"]) is str and endpoint["socket_path"].startswith("/")
        and type(endpoint["device"]) is int and endpoint["device"] >= 0
        and type(endpoint["inode"]) is int and endpoint["inode"] > 0)


def identity_policy() -> dict[str, Any]:
    return {"policy_id": IDENTITY_POLICY_ID, "protocol": PROTOCOL,
        "mount_plan_sha256": MOUNT_PLAN_SHA256, "startup_policy": startup_policy(),
        "startup_policy_sha256": startup_policy_sha256(), "allowed_phases": list(_IDENTITY_PHASES),
        "path": "Mounts", "comparison_format": "destination-keyed-map-of-complete-rows",
        "unique_destinations": True, "canonical_absolute_posix_destinations": True,
        "strict_raw_json_decoder": "duplicate-keys-rejected-before-dict-conversion",
        "candidate_destinations": ["/inputs", "/tmp", "/workspace"], "keeper_destinations": ["/tmp"],
        "all_other_arrays": "type-and-order-exact", "all_other_fields": "type-exact",
        "runtime_gate": startup_policy()["runtime_gate"],
        "prestart_oom": "raw-exact", "running_keeper_oom": "raw-exact"}


def identity_policy_sha256() -> str:
    return _sha(_encoded(identity_policy()))


def _mount_inventory(value: Any, role: str) -> tuple[dict[str, Any], dict[str, Any]]:
    _require(type(value) is list, "Mount inventory requires an array")
    inventory: dict[str, Any] = {}
    order: list[str] = []
    for row in value:
        _require(type(row) is dict, "Mount inventory requires object rows")
        destination = row.get("Destination")
        _require(type(destination) is str and destination.startswith("/")
            and not destination.startswith("//") and "\x00" not in destination
            and posixpath.normpath(destination) == destination, "Noncanonical mount destination")
        _require(destination not in inventory, "Duplicate mount destination")
        # Keep every field and nested array; no row coercion or list sorting.
        inventory[destination] = row
        order.append(destination)
    required = {"/tmp"} if role == "keeper" else {"/workspace", "/inputs", "/tmp"}
    _require(set(inventory) == required, "Mount inventory differs from role profile")
    return inventory, {"order": order,
        "row_sha256": {key: _sha(_encoded(row)) for key, row in inventory.items()},
        "inventory_sha256": _sha(_encoded(inventory))}


def _full_inspection_sha256(full: Any, projection: dict[str, Any]) -> str | None:
    if full is None:
        return None
    raw = _comparison_bytes(full)
    _require(type(full) is dict, "Full inspection must be an object")
    for key, value in projection.items():
        if key == "StartedAt":
            state = full.get("State")
            _require(type(state) is dict and key in state, "Full inspection lifecycle field missing")
            observed = state[key]
        else:
            _require(key in full, "Full inspection projection field missing")
            observed = full[key]
        _require(_encoded(observed) == _encoded(value), "Full inspection and raw projection differ")
    return _sha(raw)


def identity_comparison(before_projection: dict[str, Any], after_projection: dict[str, Any],
                        runtime: dict[str, Any], phase: str, *,
                        before_full_inspection: dict[str, Any] | None = None,
                        after_full_inspection: dict[str, Any] | None = None) -> dict[str, Any]:
    """Compare full supplied projections using only explicitly versioned rules.

    Raw records stay unchanged. Duplicate JSON keys must have been rejected at
    their raw decoding boundary; dictionaries cannot recover discarded keys.
    """
    record: dict[str, Any] = {"matches": False, "policy_id": IDENTITY_POLICY_ID,
        "policy_sha256": identity_policy_sha256(), "startup_policy_id": STARTUP_POLICY_ID,
        "startup_policy_sha256": startup_policy_sha256(),
        "phase": phase if type(phase) is str else None, "role": None,
        "before_sha256": None, "after_sha256": None, "runtime_sha256": None,
        "before_full_inspection_sha256": None, "after_full_inspection_sha256": None,
        "mounts_before": None, "mounts_after": None,
        "comparison_before_sha256": None, "comparison_after_sha256": None,
        "transformations": [], "reasons": []}
    try:
        before_raw, after_raw, runtime_raw = (_comparison_bytes(value)
            for value in (before_projection, after_projection, runtime))
        record.update(before_sha256=_sha(before_raw), after_sha256=_sha(after_raw),
                      runtime_sha256=_sha(runtime_raw))
        _require(type(before_projection) is dict and type(after_projection) is dict and type(runtime) is dict,
                 "Identity comparison requires object projections and runtime")
        required = {"Id", "Image", "Name", "Path", "Args", "Config", "HostConfig", "Mounts"}
        _require(required <= before_projection.keys() and required <= after_projection.keys(),
                 "Identity projection is incomplete")
        for value in (before_projection, after_projection):
            _require(type(value["Id"]) is str and bool(_ID.fullmatch(value["Id"]))
                and type(value["Image"]) is str and bool(_IMAGE.fullmatch(value["Image"]))
                and type(value["Name"]) is str and value["Name"].startswith("/")
                and type(value["Path"]) is str and bool(value["Path"])
                and type(value["Args"]) is list and all(type(arg) is str for arg in value["Args"])
                and type(value["Config"]) is dict, "Malformed identity projection")
        _require(type(phase) is str and phase in _IDENTITY_PHASES, "Unqualified identity phase")
        role = "keeper" if phase.startswith("keeper-") else "candidate"
        record["role"] = role
        validate_oom_kill_default(before_projection["HostConfig"])
        validate_oom_kill_default(after_projection["HostConfig"])
        _require(_startup_runtime_valid(runtime, before_projection["Image"]),
                 "Unqualified startup runtime profile")
        record["before_full_inspection_sha256"] = _full_inspection_sha256(before_full_inspection, before_projection)
        record["after_full_inspection_sha256"] = _full_inspection_sha256(after_full_inspection, after_projection)
        before, after = strict_json_loads(before_raw), strict_json_loads(after_raw)
        before["Mounts"], record["mounts_before"] = _mount_inventory(before["Mounts"], role)
        after["Mounts"], record["mounts_after"] = _mount_inventory(after["Mounts"], role)
        if record["mounts_before"]["order"] != record["mounts_after"]["order"]:
            record["transformations"].append({"path": "Mounts", "rule_id": IDENTITY_POLICY_ID,
                "comparison_format": "destination-keyed-map-of-complete-rows",
                "before_order": record["mounts_before"]["order"],
                "after_order": record["mounts_after"]["order"], "phase": phase,
                "runtime_sha256": record["runtime_sha256"]})
        old, new = before["HostConfig"]["OomKillDisable"], after["HostConfig"]["OomKillDisable"]
        if old is False and new is None and phase in _STARTUP_PHASES:
            after["HostConfig"]["OomKillDisable"] = False
            record["transformations"].append({"path": "HostConfig.OomKillDisable", "before": False,
                "after": None, "comparison_after": False, "phase": phase,
                "runtime_sha256": record["runtime_sha256"]})
        else:
            _require(old is new, "Unqualified OomKillDisable transition")
        before_comparison, after_comparison = _encoded(before), _encoded(after)
        record["comparison_before_sha256"] = _sha(before_comparison)
        record["comparison_after_sha256"] = _sha(after_comparison)
        record["matches"] = before_comparison == after_comparison
        if not record["matches"]:
            record["reasons"].append("Other identity fields differ")
    except (ProcessError, ValueError, TypeError, OverflowError, RecursionError) as error:
        record["reasons"].append(str(error))
    record["comparison_sha256"] = _sha(_encoded(record))
    return record


def startup_identity_comparison(before_projection: dict[str, Any], after_projection: dict[str, Any],
                                runtime: dict[str, Any], phase: str, *,
                                before_full_inspection: dict[str, Any] | None = None,
                                after_full_inspection: dict[str, Any] | None = None) -> dict[str, Any]:
    # Compatibility entry point only for the two explicitly defined startup phases.
    return identity_comparison(before_projection, after_projection, runtime,
        phase if type(phase) is str and phase in _STARTUP_PHASES else "invalid-startup-phase",
        before_full_inspection=before_full_inspection, after_full_inspection=after_full_inspection)


def immutable_inspection(value: dict[str, Any]) -> dict[str, Any]:
    keys = ("Id", "Name", "Created", "Image", "Path", "Args", "Config", "HostConfig", "Mounts")
    _require(type(value) is dict and all(key in value for key in keys), "Incomplete container identity")
    return {key: value[key] for key in keys}


def command_policy() -> dict[str, Any]:
    """Declare argv admission separately from raw identity comparison."""
    return {"policy_id": COMMAND_POLICY_ID, "protocol": PROTOCOL,
        "empty_command_plan_sha256": EMPTY_COMMAND_PLAN_SHA256,
        "runtime_gate": startup_policy()["runtime_gate"],
        "declared_argv": "independent required nonempty list of strings without NUL",
        "entrypoint": "explicit singleton equal to declared executable and Path",
        "args": "explicit list equal to declared argv tail with exact type/order/value",
        "cmd": "explicit null only for singleton argv and empty Args; otherwise exact Args list",
        "identity": "no Cmd normalization; null and [] remain different raw identities"}


def command_policy_sha256() -> str:
    return _sha(_encoded(command_policy()))


def start_response_policy() -> dict[str, Any]:
    """Describe durable HTTP observations without grading any error response."""
    return {"policy_id": START_RESPONSE_PROTOCOL, "protocol": PROTOCOL,
        "empty_command_plan_sha256": EMPTY_COMMAND_PLAN_SHA256,
        "api_version": API_VERSION, "header_bytes": HEADER_LIMIT,
        "nonempty_header_lines": 127, "body_bytes": CONTROL_LIMIT,
        "chunks": FRAME_COUNT_LIMIT,
        "wire_bytes": HEADER_LIMIT + CONTROL_LIMIT + FRAME_COUNT_LIMIT * 20,
        "completion": "normal _control return after framing, EOF, retention and socket close",
        "receipt": "start-response-completion.json retained before rejection branch/finally cleanup",
        "result": "start_response object plus start_response_receipt descriptor after finally cleanup",
        "statuses": "all complete HTTP responses; no control-specific status/message predicate",
        "authority": "HTTP observation only; never candidate output or process exit"}


def start_response_policy_sha256() -> str:
    return _sha(_encoded(start_response_policy()))


def _declared_argv(expected_argv: Any) -> list[str]:
    _require(type(expected_argv) is list and bool(expected_argv)
             and all(type(arg) is str and "\x00" not in arg for arg in expected_argv)
             and bool(expected_argv[0]), "Independent declared argv required")
    _comparison_bytes(expected_argv)
    return list(expected_argv)


def validate_sandbox(value: dict[str, Any], policy: ProcessPolicy, *,
                     expected_argv: list[str], runtime: dict[str, Any]) -> None:
    """Reject absent/weakened restrictions and unregistered argv before start."""
    declared = _declared_argv(expected_argv)
    _require(type(runtime) is dict and _startup_runtime_valid(runtime, policy.image_id),
             "Unqualified invocation runtime")
    immutable_inspection(value)
    config, host, mounts = value["Config"], value["HostConfig"], value["Mounts"]
    _require(type(config) is dict and type(host) is dict and type(mounts) is list,
             "Malformed container configuration")
    validate_oom_kill_default(host)
    _require(value["Image"] == policy.image_id and config.get("Image") == policy.image_id
             and config.get("User") == "65534:65534" and config.get("WorkingDir") == "/workspace"
             and config.get("Tty") is False and config.get("OpenStdin") is False
             and config.get("StdinOnce") is False and config.get("AttachStdout") is True
             and config.get("AttachStderr") is True and config.get("Healthcheck") == {"Test": ["NONE"]},
             "Candidate process configuration differs")
    _require("Entrypoint" in config and "Cmd" in config, "Candidate command fields missing")
    entrypoint, cmd, args = config["Entrypoint"], config["Cmd"], value["Args"]
    _require(type(entrypoint) is list and entrypoint == [declared[0]]
             and all(type(arg) is str for arg in entrypoint)
             and type(value["Path"]) is str and value["Path"] == declared[0]
             and type(args) is list and all(type(arg) is str for arg in args)
             and args == declared[1:], "Candidate argv differs")
    _require((cmd is None and len(declared) == 1 and args == [])
             or (type(cmd) is list and all(type(arg) is str for arg in cmd) and cmd == args),
             "Candidate Cmd differs")
    labels = config.get("Labels")
    _require(type(labels) is dict and all(type(labels.get(key)) is str and bool(labels[key])
             for key in ("gossip.execution", "gossip.source", "gossip.fixture", "gossip.step"))
             and all(bool(re.fullmatch(r"[0-9a-f]{64}", labels[key]))
                     for key in ("gossip.source", "gossip.fixture")), "Missing source/fixture labels")
    expected_host = {"NetworkMode": "none", "ReadonlyRootfs": True, "Privileged": False,
        "AutoRemove": False, "Memory": 256 * 1024 * 1024, "MemorySwap": 256 * 1024 * 1024,
        "NanoCpus": 1000000000, "PidsLimit": 64, "CapDrop": ["ALL"],
        "SecurityOpt": ["no-new-privileges:true"], "LogConfig": {"Type": "none", "Config": {}},
        "RestartPolicy": {"Name": "no", "MaximumRetryCount": 0},
        "Ulimits": [{"Name": "nofile", "Hard": 256, "Soft": 256}]}
    _require(all(key in host and _encoded(host[key]) == _encoded(expected)
                 for key, expected in expected_host.items())
             and (host.get("Init") is None or host.get("Init") is False)
             and host.get("IpcMode") in ("", "private")
             and host.get("CgroupnsMode") in ("", "private")
             and all(not host.get(key) for key in ("CapAdd", "Devices", "DeviceRequests", "VolumesFrom",
                 "Links", "PortBindings", "PublishAllPorts", "PidMode", "UTSMode", "UsernsMode")),
             "Candidate sandbox restrictions differ")
    _require(len(mounts) == 3 and {row.get("Destination") for row in mounts if type(row) is dict}
             == {"/workspace", "/inputs", "/tmp"}, "Unexpected candidate mount")
    for mount in mounts:
        if mount["Destination"] == "/tmp":
            _require(mount.get("Type") == "volume" and mount.get("Driver") == "local"
                     and mount.get("RW") is True and bool(_LABEL.fullmatch(mount.get("Name", ""))),
                     "Candidate state volume differs")
        else:
            _require(mount.get("Type") == "bind" and mount.get("RW") is False
                     and mount.get("Propagation") == "rprivate" and type(mount.get("Source")) is str
                     and Path(mount["Source"]).is_absolute()
                     and str(Path(mount["Source"]).resolve()) == mount["Source"],
                     "Candidate read-only staging differs")
    environment = config.get("Env")
    _require(type(environment) is list and all(type(s) is str and "=" in s for s in environment),
             "Malformed candidate environment")
    names = [s.split("=", 1)[0] for s in environment]
    _require(len(names) == len(set(names)), "Duplicate candidate environment key")
    env = dict(s.split("=", 1) for s in environment)
    _require(env.get("HOME") == "/tmp" and env.get("PYTHONDONTWRITEBYTECODE") == "1"
             and env.get("PYTHONNOUSERSITE") == "1" and all(env.get(key) == "" for key in (
                 "HTTP_PROXY", "HTTPS_PROXY", "FTP_PROXY", "NO_PROXY", "ALL_PROXY",
                 "http_proxy", "https_proxy", "ftp_proxy", "no_proxy", "all_proxy")),
             "Candidate environment differs")


def completion_evidence(wait: dict[str, Any], final: dict[str, Any], *, started: bool,
                        killed: bool, identity_ok: bool) -> dict[str, Any]:
    state = final.get("State", {})
    if type(state) is not dict:
        state = {}
    wait_code, final_code = wait.get("StatusCode"), state.get("ExitCode")
    finished, beginning = state.get("FinishedAt"), state.get("StartedAt")
    begin_stamp, finish_stamp = _timestamp(beginning), _timestamp(finished)
    created_stamp = _timestamp(final.get("Created"))
    stamps = (created_stamp is not None and begin_stamp is not None and finish_stamp is not None
              and created_stamp <= begin_stamp <= finish_stamp)
    wait_error = wait.get("Error")
    wait_ok = wait_error is None or (type(wait_error) is dict and wait_error.get("Message") == "")
    natural = (started and not killed and identity_ok and stamps and wait_ok
        and type(wait_code) is int and type(final_code) is int and 0 <= wait_code < 128
        and wait_code == final_code and state.get("Status") == "exited"
        and state.get("Running") is False and state.get("Paused") is False
        and state.get("Restarting") is False and state.get("Dead") is False
        and state.get("OOMKilled") is False and state.get("Error") == ""
        and type(final.get("RestartCount")) is int and final["RestartCount"] == 0
        and type(state.get("Pid")) is int and state["Pid"] == 0)
    return {"container_id": final.get("Id"), "started": started, "natural": bool(natural),
        "wait_status_code": wait_code, "inspect_exit_code": final_code, "finished_at": finished,
        "started_at": beginning, "oom_killed": state.get("OOMKilled"), "state_error": state.get("Error"),
        "killed_by_controller": killed, "identity_verified": identity_ok, "wait_error": wait_error,
        "signal_exit_ambiguous": type(final_code) is int and final_code >= 128}


def run_process(endpoint: EngineEndpoint, *, container_id: str, expected: dict[str, Any],
                policy: ProcessPolicy, retain: Retain, label: str,
                expected_runtime: dict[str, Any], expected_argv: list[str]) -> dict[str, Any]:
    """Start exactly once after attachment and retain independent completion evidence.

    A completed process is not a semantic pass. Source/fixture bytes and final
    container/volume removal are verified by the caller. No automatic retries.
    """
    _require(type(container_id) is str and bool(_ID.fullmatch(container_id)), "Full container ID required")
    _require(type(label) is str and bool(_LABEL.fullmatch(label)), "Unsafe process label")
    _require(evaluator_sources() == _LOADED_EVALUATOR_SOURCES, "Loaded process evaluator changed")
    declared_argv = _declared_argv(expected_argv)
    expected_runtime = strict_json_loads(_comparison_bytes(expected_runtime))
    _require(type(expected_runtime) is dict
             and _encoded(expected_runtime.get("endpoint")) == _encoded(asdict(endpoint)),
             "Declared runtime endpoint differs")
    validate_sandbox(expected, policy, expected_argv=declared_argv, runtime=expected_runtime)
    _require(expected["Id"] == container_id and expected.get("State", {}).get("Status") == "created",
             "Fresh created container required")
    frozen = _encoded(immutable_inspection(expected))
    evidence: list[str] = []
    descriptors: dict[str, dict[str, Any]] = {}
    def save(name: str, raw: bytes) -> None:
        path = label + "-" + name
        _require(path not in evidence, "Duplicate process evidence")
        retain(path, raw)
        evidence.append(path)
        descriptors[name] = {"path": path, "bytes": len(raw), "sha256": _sha(raw)}
    save("intent.json", _encoded({"protocol": PROTOCOL, "container_id": container_id,
        "expected_inspection_sha256": _sha(frozen), "expected_runtime": expected_runtime,
        "policy": asdict(policy), "fixed_limits": {"headers": HEADER_LIMIT, "control_body": CONTROL_LIMIT,
        "frame_count": FRAME_COUNT_LIMIT}, "sources": evaluator_sources(),
        "startup_policy": startup_policy(), "startup_policy_sha256": startup_policy_sha256(),
        "identity_policy": identity_policy(), "identity_policy_sha256": identity_policy_sha256(),
        "expected_full_inspection_sha256": _sha(_comparison_bytes(expected)),
        "expected_argv": declared_argv, "expected_argv_sha256": _sha(_encoded(declared_argv)),
        "command_policy": command_policy(), "command_policy_sha256": command_policy_sha256(),
        "start_response_policy": start_response_policy(),
        "start_response_policy_sha256": start_response_policy_sha256()}))
    prefix = "/containers/" + container_id
    decoder = MultiplexDecoder(policy)
    wire: _Wire | None = None
    reader: threading.Thread | None = None
    stream_error: list[BaseException] = []
    status = "completion_unproven"
    started = killed = identity_ok = False
    stage = "setup"
    eof_after_start_confirmation: bool | None = None
    wait: dict[str, Any] = {}
    final: dict[str, Any] = {}
    control_error: str | None = None
    startup_comparison: dict[str, Any] | None = None
    prestart_comparison: dict[str, Any] | None = None
    start_response: dict[str, Any] | None = None
    start_response_receipt: dict[str, Any] | None = None
    def start_response_observed(code: int) -> None:
        nonlocal started
        # Capture causal acknowledgement before durable response retention can
        # block on filesystem fsync/checkpoint work. Full framing has passed.
        if code == 204:
            started = True

    def drain() -> None:
        nonlocal eof_after_start_confirmation
        assert wire is not None
        try:
            while True:
                if wire.buffer:
                    chunk = bytes(wire.buffer)
                    wire.buffer.clear()
                    decoder.feed(chunk)
                if wire.eof:
                    eof_after_start_confirmation = started
                    _require(eof_after_start_confirmation is True, "Attach EOF before successful start confirmation")
                    decoder.eof()
                    return
                wire.receive()
        except (OSError, ProcessError) as error:
            stream_error.append(error)
    try:
        observed_runtime = runtime_identity(endpoint, policy.image_id, retain=save, label="runtime-before",
                                            timeout_seconds=policy.transport_timeout_seconds)
        _require(_encoded(observed_runtime) == _encoded(expected_runtime), "Runtime identity changed")
        before = _json_control(endpoint, prefix + "/json", deadline=time.monotonic() + policy.transport_timeout_seconds,
                               retain=save, label="inspect-before")
        prestart_comparison = identity_comparison(strict_json_loads(frozen), immutable_inspection(before),
            observed_runtime, phase="candidate-created-to-prestart",
            before_full_inspection=expected, after_full_inspection=before)
        save("prestart-comparison.json", _encoded(prestart_comparison))
        _require(prestart_comparison["matches"] is True and before.get("State", {}).get("Status") == "created",
                 "Container changed before start")
        validate_sandbox(before, policy, expected_argv=declared_argv, runtime=observed_runtime)
        request = _request("POST", prefix + "/attach?stream=1&stdout=1&stderr=1&stdin=0&logs=0", upgrade=True)
        save("attach-request.bin", request)
        wire = _Wire(endpoint, time.monotonic() + policy.transport_timeout_seconds,
                     HEADER_LIMIT + 2 * policy.stream_limit_bytes + FRAME_COUNT_LIMIT * 8)
        wire.send(request)
        code, headers = wire.headers()
        _require(code == 101 and headers.get("connection", "").lower() == "upgrade"
                 and headers.get("upgrade", "").lower() == "tcp"
                 and headers.get("content-type") in ("application/vnd.docker.raw-stream",
                                                      "application/vnd.docker.multiplexed-stream")
                 and "content-length" not in headers and "transfer-encoding" not in headers,
                 "Docker raw stream attach failed")
        _require(not wire.buffer, "Attach payload arrived before candidate start")
        # An upgraded connection can close without yielding any frames. Verify
        # it is idle/live before start so a closed attachment cannot masquerade
        # as a complete empty candidate stream.
        wire.socket.settimeout(0)
        try:
            pending = wire.socket.recv(1, socket.MSG_PEEK)
        except BlockingIOError:
            pass
        else:
            save("attach-prestart-probe.bin", pending)
            raise ProcessError("Attach ended or produced data before start")
        finally:
            wire._timeout()
        deadline = time.monotonic() + policy.timeout_seconds
        wire.deadline = deadline
        reader = threading.Thread(target=drain, daemon=True)
        reader.start()
        _require(not stream_error and not decoder.complete, "Attach failed before start dispatch")
        stage = "start"
        code, _body = _control(endpoint, "POST", prefix + "/start", deadline=deadline,
                              retain=save, label="start", on_response=start_response_observed)
        # _control's finally has durably retained the full raw response and
        # closed its socket before returning. File EOF alone grants no proof.
        _require("start-request.bin" in descriptors and "start-response.bin" in descriptors,
                 "Start control raw evidence missing")
        observed_start = {"protocol": START_RESPONSE_PROTOCOL, "method": "POST",
            "request_path": "/v" + API_VERSION + prefix + "/start", "container_id": container_id,
            "endpoint_sha256": _sha(_encoded(asdict(endpoint))),
            "runtime_sha256": _sha(_encoded(observed_runtime)),
            "request": descriptors["start-request.bin"], "response": descriptors["start-response.bin"],
            "status": code, "body_bytes": len(_body), "body_sha256": _sha(_body),
            "framing_complete": True, "eof_observed": True, "durably_retained": True}
        save("start-response-completion.json", _encoded(observed_start))
        # Failed completion retention leaves both fields null and still enters
        # finally cleanup. No diagnostic HTTP status is a candidate exit.
        start_response = observed_start
        start_response_receipt = descriptors["start-response-completion.json"]
        if code != 204:
            status = "start_error"
            raise ProcessError("Docker start failed")
        started = True
        stage = "wait"
        wait = _json_control(endpoint, prefix + "/wait?condition=not-running", deadline=deadline,
                             retain=save, label="wait", method="POST")
        final = _json_control(endpoint, prefix + "/json", deadline=time.monotonic() + policy.transport_timeout_seconds,
                              retain=save, label="inspect-final")
        startup_comparison = startup_identity_comparison(strict_json_loads(frozen), immutable_inspection(final),
            observed_runtime, phase="candidate-created-to-exited",
            before_full_inspection=expected, after_full_inspection=final)
        save("startup-comparison.json", _encoded(startup_comparison))
        identity_ok = (startup_comparison["matches"] is True
                       and evaluator_sources() == _LOADED_EVALUATOR_SOURCES)
        reader.join(timeout=max(0, deadline - time.monotonic()))
        if reader.is_alive():
            raise TimeoutError("Candidate stream EOF unobserved")
        completion = completion_evidence(wait, final, started=started, killed=False, identity_ok=identity_ok)
        if stream_error:
            raise stream_error[0]
        _require(decoder.complete, "Candidate stream EOF unobserved")
        status = "completed" if completion["natural"] else "completion_unproven"
    except TimeoutError:
        status = "timeout"
        control_error = "observation deadline reached"
    except OutputLimit:
        status = "output_limit"
        control_error = "observation bound reached"
    except (OSError, ProcessError, ValueError) as error:
        if status != "start_error":
            status = "start_error" if stage == "start" else "transport_error"
        control_error = type(error).__name__ + ": " + str(error)
    finally:
        natural = completion_evidence(wait, final, started=started, killed=False, identity_ok=identity_ok)["natural"]
        if not natural:
            # The start request can have succeeded even if its response was lost.
            # Kill the owned ID on every uncertain outcome, never merely the CLI.
            killed = True
            try:
                _control(endpoint, "POST", prefix + "/kill?signal=SIGKILL",
                    deadline=time.monotonic() + policy.transport_timeout_seconds, retain=save, label="kill")
            except (OSError, ProcessError):
                pass
            try:
                terminal = _json_control(endpoint, prefix + "/json",
                    deadline=time.monotonic() + policy.transport_timeout_seconds,
                    retain=save, label="inspect-after-kill")
                save("cleanup-state.json", _encoded(terminal))
            except (OSError, ProcessError):
                pass
        if wire is not None:
            try:
                wire.socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            if reader is not None:
                reader.join(timeout=policy.transport_timeout_seconds)
                if reader.is_alive():
                    status = "transport_error"
                    control_error = "attach reader failed to stop"
            save("attach-response.bin", bytes(wire.raw))
            wire.close()
    completion = completion_evidence(wait, final, started=started, killed=killed, identity_ok=identity_ok)
    record: dict[str, Any] = {"protocol": PROTOCOL, "status": status,
        "exit_code": completion["inspect_exit_code"] if completion["natural"] else None,
        "capture_complete": decoder.complete and not stream_error,
        "completion": completion, "error": control_error, "frames": decoder.frames,
        "attach_eof_after_start_confirmation": eof_after_start_confirmation,
        "startup_comparison": startup_comparison, "prestart_comparison": prestart_comparison,
        "start_response": start_response, "start_response_receipt": start_response_receipt}
    for kind, data in decoder.streams.items():
        raw = bytes(data)
        name = kind + ".bin"
        save(name, raw)
        record[kind] = {"path": label + "-" + name, "sha256": _sha(raw), "bytes": len(raw),
                        "observed_bytes": decoder.counts[kind], "truncated": decoder.counts[kind] > len(raw),
                        "complete": decoder.complete and not stream_error}
    record["evidence"] = list(evidence)
    save("process.json", _encoded(record))
    return record


_LOADED_EVALUATOR_SOURCES = evaluator_sources()
_require(_LOADED_EVALUATOR_SOURCES["analysis/candidate-b03-runtime-compatibility-plan-v1.json"]
         == COMPATIBILITY_PLAN_SHA256, "Compatibility plan source changed")

_require(_LOADED_EVALUATOR_SOURCES["analysis/candidate-b03-mount-inventory-plan-v1.json"]
         == MOUNT_PLAN_SHA256, "Mount inventory plan source changed")

_require(_LOADED_EVALUATOR_SOURCES["analysis/candidate-b03-empty-command-plan-v3.json"]
         == EMPTY_COMMAND_PLAN_SHA256, "Empty command plan source changed")
