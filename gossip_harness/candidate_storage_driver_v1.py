"""Physical B01 observations from registered source in a fresh Docker boundary.

Only the host controls source staging, command order, pause/copy and raw capture
identities. The persistent adapter imports candidate code inside the container;
its returned values are untrusted product data, never acceptance flags. A pause
follows a response, not atomically a method return. Close/reopen is not a crash.
The driver does not authenticate cohort admission or independent final purpose.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import queue
import re
import subprocess
import tarfile
import tempfile
import threading
from dataclasses import dataclass
from typing import Any
import uuid

from .candidate_storage_cases_v1 import CASE_IDS, definition_sha256
from .sandbox import DockerValidator

PROTOCOL = "candidate-storage-driver-v1"
SNAPSHOT_PROTOCOL = "docker-paused-local-tmpfs-volume-v1"
RUNTIME_IMAGE = "sha256:f77ac9e44ae96ef2c90b8053ea08c31f8be030f824196b0ae4db6d462c84e51f"
VOLUME_OPTIONS = {"type": "tmpfs", "device": "tmpfs", "o": "size=32m,mode=1777,nosuid,nodev,noexec"}
MAX_SOURCE_FILES = 511
MAX_SOURCE_BYTES = 16 * 1024 * 1024
MAX_FILE_BYTES = 8 * 1024 * 1024
MAX_CAPTURE_BYTES = 40 * 1024 * 1024
MAX_STREAM_BYTES = 1024 * 1024
PHASES = ("before", "after", "reopened")

# No expected state enters this adapter. Eight public histories are explicit and
# versioned, rather than a candidate-defined executable hook or arbitrary shell.
CHILD_ADAPTER = r'''import json
from pathlib import Path
import sys
sys.path.insert(0, '/workspace')
from library.catalog.store import Store
from library.ingestion.jobs import JobManager
from library.common import LibraryError

case = sys.argv[1]
path = Path('/tmp/catalog.sqlite')
store = None
jobs = None
token = None
pair = [{'source':'new-a.txt','text':'same'}, {'source':'new-b.txt','text':'same'}]
caller = None

def setup():
    global store, jobs, token, caller
    store = Store(path)
    jobs = JobManager(store)
    if case == 'capacity':
        for index in range(254):
            store.insert(str(index) + '.txt', b'shared')
        jobs.submit('case', pair)
        token = jobs.prepare('case')
        other = Store(path)
        try:
            other.insert('external.txt', b'external')
        finally:
            other.close()
        return None
    store.insert('keep.txt', b'keep')
    if case == 'invalid-admission':
        return None
    if case == 'deferred-semantic':
        jobs.submit('case', [{'source':'ok.txt','text':'OK'}, {'source':'../bad.txt','text':'bad'}])
        return None
    if case == 'canonical-replay':
        caller = [dict(row) for row in reversed(pair)]
        jobs.submit('case', caller)
        caller[0]['text'] = 'mutated caller input'
        caller.append({'source':'intruder.txt','text':'intruder'})
        return None
    jobs.submit('case', [] if case == 'empty-batch' else pair)
    token = jobs.prepare('case')
    if case in ('completed-replay', 'value-mutation'):
        jobs.commit(token)
    return None

def action():
    if case == 'invalid-admission':
        return jobs.submit('case', [{'source':'new.txt','text':'new','extra':True}])
    if case == 'deferred-semantic':
        return jobs.prepare('case')
    if case == 'canonical-replay':
        return jobs.submit('case', [dict(row) for row in reversed(pair)])
    if case == 'value-mutation':
        public = store.get_job('case')
        public['state'] = 'corrupted'
        manifest = store.job_manifest('case')
        manifest[0]['text'] = 'corrupted'
        manifest.append({'source':'intruder.txt','text':'intruder'})
        receipt = store.commit_job('case', 1)
        receipt['job']['completed'] = -1
        receipt['documents'][0]['text'] = 'corrupted'
        receipt['documents'].clear()
        return {'job':store.get_job('case'), 'manifest':store.job_manifest('case'),
                'receipt':store.commit_job('case', 1)}
    return jobs.commit(token, fail_before_commit=(case == 'rollback'))

def reopen():
    global store, jobs
    if store is not None:
        store.close()
    store = Store(path)
    jobs = JobManager(store)
    return None

for phase, function in (('before', setup), ('after', action), ('reopened', reopen)):
    request = sys.stdin.buffer.readline(128)
    if request != (phase + '\n').encode():
        raise ValueError('unexpected controller phase')
    try:
        value = function()
    except LibraryError as error:
        value = {'error':error.code}
    except BaseException as error:
        value = {'unexpected_exception':type(error).__name__}
    print(json.dumps({'phase':phase,'value':value},ensure_ascii=True,allow_nan=False,separators=(',',':')),flush=True)
# Hold the reopened Store until the controller captures it and explicitly exits.
if sys.stdin.buffer.readline(128) != b'finish\n':
    raise ValueError('unexpected controller finish')
if store is not None:
    store.close()
'''


class DriverError(ValueError):
    """Invalid registration, incomplete transport or controller evidence."""


class CaptureLayoutError(DriverError):
    """A complete transport contains unsupported candidate storage entries."""


def encoded(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
                      allow_nan=False).encode("utf-8")


def sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise DriverError(message)


def _safe_path(name: str) -> bool:
    return (type(name) is str and bool(name) and len(name) <= 1024 and "\\" not in name
            and "\x00" not in name and not PurePosixPath(name).is_absolute()
            and all(part and part not in (".", "..") and not part.startswith(".")
                    for part in name.split("/")))


def source_sha256(files: dict[str, bytes]) -> str:
    """This protocol's full byte inventory identity, not another driver's hash."""
    require(type(files) is dict and 0 < len(files) <= MAX_SOURCE_FILES, "Source file bound")
    total = 0
    manifest = []
    for name, raw in sorted(files.items()):
        require(_safe_path(name) and type(raw) is bytes and len(raw) <= MAX_FILE_BYTES, "Invalid source file")
        require(not any(str(parent) in files for parent in PurePosixPath(name).parents), "File ancestor collision")
        total += len(raw)
        require(total <= MAX_SOURCE_BYTES, "Source byte bound")
        manifest.append({"path": name, "bytes": len(raw), "sha256": sha256(raw)})
    return sha256(encoded({"protocol": PROTOCOL, "complete_source": manifest}))


def driver_sources() -> dict[str, str]:
    return {name: sha256(Path(__file__).with_name(name).read_bytes())
            for name in ("candidate_storage_driver_v1.py", "candidate_storage_cases_v1.py", "sandbox.py")}


_LOADED_DRIVER_SOURCES = driver_sources()


def parse_capture(raw: bytes) -> dict[str, bytes]:
    """Validate complete Docker tar framing, then return ordinary /tmp bytes.

    Nothing is extracted onto the host. A complete but unsupported link/special
    file layout is distinct from an incomplete transport. SQLite sidecars are
    preserved here; the declared layout observer decides coherence eligibility.
    """
    require(type(raw) is bytes and 0 < len(raw) <= MAX_CAPTURE_BYTES and len(raw) % 512 == 0,
            "Invalid capture length")
    members: list[tarfile.TarInfo] = []
    try:
        with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as archive:
            for member in archive:
                if len(members) <= 2048:
                    members.append(member)
            trailer = raw[archive.offset:]
            require(len(trailer) >= 1024 and not any(trailer), "Incomplete capture trailer")
            if len(members) > 2048:
                raise CaptureLayoutError("Capture member bound")
            files: dict[str, bytes] = {}
            seen: set[str] = set()
            directories: set[str] = set()
            total = 0
            for member in members:
                name = member.name.rstrip("/") if member.isdir() else member.name
                if not _safe_path(name) or name in seen or (name != "tmp" and not name.startswith("tmp/")):
                    raise CaptureLayoutError("Unsafe or duplicate capture member")
                seen.add(name)
                if member.isdir():
                    if member.size != 0:
                        raise CaptureLayoutError("Nonempty directory entry")
                    directories.add(name)
                    continue
                if (not member.isreg() or member.issparse() or type(member.size) is not int
                        or not 0 <= member.size <= MAX_FILE_BYTES):
                    raise CaptureLayoutError("Unsupported capture member")
                total += member.size
                if total > 32 * 1024 * 1024 or len(files) >= 1024:
                    raise CaptureLayoutError("Capture file bound")
                stream = archive.extractfile(member)
                require(stream is not None, "Missing capture bytes")
                assert stream is not None
                with stream:
                    content = stream.read(member.size + 1)
                require(len(content) == member.size, "Truncated capture member")
                files[name[4:]] = content
            require("tmp" in directories, "Missing capture root")
            for name in files:
                if "tmp/" + name in directories or any(str(parent) in files for parent in PurePosixPath(name).parents):
                    raise CaptureLayoutError("Capture ancestor collision")
            return files
    except DriverError:
        raise
    except (tarfile.TarError, OSError, EOFError, ValueError, UnicodeError) as error:
        raise DriverError("Malformed capture transport") from error


@dataclass(frozen=True)
class StorageExecution:
    case_id: str
    source_sha256: str
    output_root: str
    snapshots: dict[str, dict[str, bytes]]
    responses: dict[str, Any]
    infrastructure: tuple[str, ...]
    layout_errors: dict[str, str]
    cleanup_verified: bool
    terminal_sha256: str

    @property
    def completed(self) -> bool:
        return not self.infrastructure and self.cleanup_verified


class _Commands:
    def __init__(self, root: Path, timeout: int) -> None:
        self.root = root
        self.timeout = timeout
        self.records: dict[str, dict[str, Any]] = {}

    def retain(self, name: str, raw: bytes) -> None:
        with (self.root / name).open("xb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())

    def run(self, label: str, arguments: list[str], limit: int = MAX_STREAM_BYTES) -> dict[str, Any]:
        require(label not in self.records, "Duplicate controller command")
        process = subprocess.Popen(arguments, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   stdin=subprocess.DEVNULL, env=DockerValidator._environment())
        streams: dict[str, bytearray] = {"stdout": bytearray(), "stderr": bytearray()}
        counts: dict[str, int] = {"stdout": 0, "stderr": 0}
        errors: list[str] = []
        def drain(kind: str) -> None:
            pipe = getattr(process, kind)
            try:
                while chunk := pipe.read(65536):
                    counts[kind] += len(chunk)
                    streams[kind].extend(chunk[:max(0, limit - len(streams[kind]))])
            except OSError:
                errors.append(kind)
            finally:
                pipe.close()
        threads = [threading.Thread(target=drain, args=(kind,), daemon=True) for kind in streams]
        for thread in threads:
            thread.start()
        timed_out = False
        try:
            process.wait(timeout=self.timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            process.kill()
            process.wait(timeout=5)
        for thread in threads:
            thread.join(timeout=5)
        complete = not errors and all(not thread.is_alive() for thread in threads)
        record: dict[str, Any] = {"arguments": arguments, "exit_code": process.returncode,
                                  "timed_out": timed_out, "capture_complete": complete}
        for kind, data in streams.items():
            raw = bytes(data)
            name = label + "-" + kind + ".bin"
            self.retain(name, raw)
            record[kind] = {"path": name, "bytes": len(raw), "observed_bytes": counts[kind],
                            "sha256": sha256(raw), "truncated": counts[kind] > len(raw)}
        self.retain(label + ".json", encoded(record))
        self.records[label] = record
        return record

    def raw(self, record: dict[str, Any], kind: str = "stdout") -> bytes:
        value = record[kind]
        raw = (self.root / value["path"]).read_bytes()
        require(sha256(raw) == value["sha256"] and len(raw) == value["bytes"], "Changed observation")
        return raw


def _clean(record: dict[str, Any]) -> bool:
    return (record["exit_code"] == 0 and not record["timed_out"] and record["capture_complete"]
            and not record["stdout"]["truncated"] and not record["stderr"]["truncated"])


class _Session:
    """Host-controlled persistent stdin session with bounded untrusted streams."""
    def __init__(self, commands: _Commands, arguments: list[str]) -> None:
        self.commands = commands
        self.arguments = arguments
        self.process = subprocess.Popen(arguments, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, env=DockerValidator._environment())
        self.lines: queue.Queue[bytes | None] = queue.Queue(maxsize=8)
        self.streams = {"stdout": bytearray(), "stderr": bytearray()}
        self.counts = {"stdout": 0, "stderr": 0}
        self.errors: set[str] = set()
        self.threads = [threading.Thread(target=self._drain, args=(kind,), daemon=True) for kind in self.streams]
        for thread in self.threads:
            thread.start()
        self.requests: list[dict[str, Any]] = []

    def _drain(self, kind: str) -> None:
        pipe = getattr(self.process, kind)
        try:
            while raw := (pipe.readline(MAX_STREAM_BYTES + 1) if kind == "stdout" else pipe.read(65536)):
                self.counts[kind] += len(raw)
                self.streams[kind].extend(raw[:max(0, MAX_STREAM_BYTES - len(self.streams[kind]))])
                if kind == "stdout":
                    try:
                        self.lines.put_nowait(raw)
                    except queue.Full:
                        self.errors.add("extra-response")
        except OSError:
            self.errors.add(kind)
        finally:
            pipe.close()
            if kind == "stdout":
                try:
                    self.lines.put_nowait(None)
                except queue.Full:
                    self.errors.add("extra-response")

    def phase(self, phase: str) -> Any:
        require(not self.errors and all(count <= MAX_STREAM_BYTES for count in self.counts.values()), "Session output bound")
        assert self.process.stdin is not None
        request = (phase + "\n").encode()
        self.process.stdin.write(request)
        self.process.stdin.flush()
        self.requests.append({"ordinal": len(self.requests), "phase": phase, "request_sha256": sha256(request)})
        try:
            raw = self.lines.get(timeout=self.commands.timeout)
        except queue.Empty as error:
            raise DriverError("Candidate phase response timeout") from error
        require(raw is not None and len(raw) <= MAX_STREAM_BYTES and raw.endswith(b"\n"), "Incomplete candidate response")
        assert raw is not None
        try:
            def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
                result: dict[str, Any] = {}
                for key, item in items:
                    if key in result:
                        raise ValueError("Duplicate response key")
                    result[key] = item
                return result
            value = json.loads(raw, object_pairs_hook=pairs)
            encoded(value)  # Reject NaN/infinity, including exponent overflow.
        except (ValueError, UnicodeError, RecursionError) as error:
            raise DriverError("Malformed candidate response") from error
        require(type(value) is dict and set(value) == {"phase", "value"} and value["phase"] == phase,
                "Candidate phase response differs")
        self.commands.retain(phase + "-response.json", raw)
        return value["value"]

    def finish(self, success: bool) -> bool:
        if self.process.poll() is None:
            if success:
                try:
                    assert self.process.stdin is not None
                    self.process.stdin.write(b"finish\n")
                    self.process.stdin.flush()
                    self.process.stdin.close()
                    self.process.wait(timeout=self.commands.timeout)
                except (OSError, subprocess.SubprocessError):
                    self.process.kill()
                    self.process.wait(timeout=5)
            else:
                self.process.kill()
                self.process.wait(timeout=5)
        if self.process.stdin is not None and not self.process.stdin.closed:
            self.process.stdin.close()
        for thread in self.threads:
            thread.join(timeout=5)
        complete = all(not thread.is_alive() for thread in self.threads)
        record: dict[str, Any] = {"arguments": self.arguments, "exit_code": self.process.returncode,
                                  "requests": self.requests, "capture_complete": complete, "errors": sorted(self.errors)}
        for kind, data in self.streams.items():
            raw = bytes(data)
            name = "session-" + kind + ".bin"
            self.commands.retain(name, raw)
            record[kind] = {"path": name, "sha256": sha256(raw), "bytes": len(raw),
                            "observed_bytes": self.counts[kind], "truncated": self.counts[kind] > len(raw)}
        # Three responses plus an end-of-stream sentinel are the entire protocol.
        pending: list[bytes | None] = []
        while not self.lines.empty():
            pending.append(self.lines.get_nowait())
        record["extra_lines"] = sum(item is not None for item in pending)
        self.commands.retain("session.json", encoded(record))
        return (success and complete and self.process.returncode == 0 and not self.errors
                and not record["extra_lines"] and all(count <= MAX_STREAM_BYTES for count in self.counts.values()))


def _volume_valid(value: Any, volume: str, execution_id: str) -> bool:
    return (type(value) is dict and value.get("Name") == volume and value.get("Driver") == "local"
            and value.get("Scope") == "local" and value.get("Options") == VOLUME_OPTIONS
            and value.get("Labels") == {"gossip.execution": execution_id, "gossip.snapshot": SNAPSHOT_PROTOCOL})


def _paused(value: Any, volume: str, image_id: str) -> bool:
    if type(value) is not dict or type(value.get("State")) is not dict or type(value.get("Mounts")) is not list:
        return False
    state = value["State"]
    mounts = [item for item in value["Mounts"] if type(item) is dict and item.get("Destination") == "/tmp"]
    return (value.get("Image") == image_id and state.get("Running") is True and state.get("Paused") is True
            and type(state.get("Pid")) is int and state["Pid"] > 0 and len(mounts) == 1
            and mounts[0].get("Type") == "volume" and mounts[0].get("Name") == volume
            and mounts[0].get("Driver") == "local" and mounts[0].get("RW") is True)


def _start_arguments(sandbox: DockerValidator, name: str, workspace: Path, checks: Path, volume: str) -> list[str]:
    arguments = sandbox._arguments(name, workspace, checks)
    arguments.remove("--rm")
    arguments = [item for item in arguments if not item.startswith("--tmpfs=/tmp:")]
    arguments[arguments.index("--user=65534:65534")] = "--user=0:0"
    index = arguments.index("--entrypoint")
    arguments[index:index] = ["--mount", "type=volume,source=" + volume + ",target=/tmp,volume-nocopy"]
    arguments.insert(2, "--detach")
    return arguments


def run_storage_case(files: dict[str, bytes], case_id: str, output_root: Path, *,
                     expected_source_sha256: str, image_id: str = RUNTIME_IMAGE,
                     timeout_seconds: int = 30) -> StorageExecution:
    """Execute one fixed case exactly once; output must be a new directory.

    A completed result can contain product/layout failures. The caller owns the
    declared observer, independently expected semantics and full admission gate.
    There is no automatic retry and no receipt reuse or acceptance conversion.
    """
    require(driver_sources() == _LOADED_DRIVER_SOURCES, "Loaded driver source differs")
    require(type(files) is dict, "Exact source mapping required")
    files = dict(files)  # Freeze the caller-owned map; each admitted bytes value is immutable.
    actual_source = source_sha256(files)
    require(actual_source == expected_source_sha256, "Registered source bytes differ")
    require(case_id in CASE_IDS and type(case_id) is str, "Unknown fixed case")
    require(image_id == RUNTIME_IMAGE, "Exact pinned image required")
    require(type(timeout_seconds) is int and 1 <= timeout_seconds <= 120, "Invalid timeout")
    root = Path(output_root).absolute()
    require(not root.exists() and not root.is_symlink() and root.parent.is_dir()
            and root.parent.resolve() == root.parent, "Fresh canonical output root required")
    root.mkdir(mode=0o700)
    commands = _Commands(root, timeout_seconds)
    execution_id = "storage-" + uuid.uuid4().hex
    name, volume = "gossip-" + execution_id, "gossip-volume-" + execution_id
    sources = driver_sources()
    intent = {"protocol": PROTOCOL, "execution_id": execution_id, "source_sha256": actual_source,
              "case_id": case_id, "ordered_phases": list(PHASES), "case_definition_sha256": definition_sha256(),
              "driver_sources": sources, "adapter_sha256": sha256(CHILD_ADAPTER.encode()),
              "image_id": image_id, "timeout_seconds": timeout_seconds, "container": name,
              "volume": volume, "snapshot_protocol": SNAPSHOT_PROTOCOL, "volume_options": VOLUME_OPTIONS,
              "limits": {"source_files": MAX_SOURCE_FILES, "source_bytes": MAX_SOURCE_BYTES,
                         "capture_bytes": MAX_CAPTURE_BYTES, "stream_bytes": MAX_STREAM_BYTES}}
    commands.retain("intent.json", encoded(intent))
    errors: list[str] = []
    snapshots: dict[str, dict[str, bytes]] = {}
    responses: dict[str, Any] = {}
    layout_errors: dict[str, str] = {}
    runtime: dict[str, Any] = {}
    session: _Session | None = None
    sandbox = DockerValidator(image_id, {"storage_adapter.py": CHILD_ADAPTER},
                              command=("python", "-I", "-c", "import time;time.sleep(600)"))
    volume_attempted = False
    container_attempted = False
    container_cleanup = False
    volume_cleanup = False
    def checked(label: str, arguments: list[str], limit: int = MAX_STREAM_BYTES) -> dict[str, Any]:
        record = commands.run(label, arguments, limit)
        require(_clean(record), label + ": incomplete or failed controller command")
        return record
    def json_record(record: dict[str, Any]) -> Any:
        try:
            return json.loads(commands.raw(record))
        except (ValueError, UnicodeError) as error:
            raise DriverError("Malformed Docker inspection") from error
    try:
        with tempfile.TemporaryDirectory(prefix="gossip-storage-stage-") as temporary:
            staging = Path(temporary).resolve()
            workspace, checks = staging / "source", staging / "checks"
            workspace.mkdir(mode=0o755)
            checks.mkdir(mode=0o755)
            for path, raw in files.items():
                target = workspace / path
                target.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
                target.write_bytes(raw)
                target.chmod(0o444)
            (checks / "storage_adapter.py").write_text(CHILD_ADAPTER, encoding="utf-8")
            (checks / "storage_adapter.py").chmod(0o444)
            staged = {path.relative_to(workspace).as_posix(): path.read_bytes()
                      for path in workspace.rglob("*") if path.is_file()}
            require(staged == files and source_sha256(staged) == expected_source_sha256, "Staged source identity differs")
            commands.retain("plan.json", encoded({"workspace": str(workspace), "checks": str(checks)}))
            runtime["server"] = json_record(checked("runtime-server", ["docker", "version", "--format", "{{json .Server}}"]))
            runtime["image"] = json_record(checked("runtime-image", ["docker", "image", "inspect", "--format", "{{json .}}", image_id]))
            require(type(runtime["server"]) is dict and type(runtime["server"].get("Version")) is str
                    and type(runtime["image"]) is dict and runtime["image"].get("Id") == image_id, "Runtime identity differs")
            absent = checked("volume-before", ["docker", "volume", "ls", "--quiet", "--filter", "name=^" + volume + "$"])
            require(not commands.raw(absent).strip(), "Volume absence unproven")
            volume_attempted = True
            made = checked("volume-create", ["docker", "volume", "create", "--driver", "local",
                "--label", "gossip.execution=" + execution_id, "--label", "gossip.snapshot=" + SNAPSHOT_PROTOCOL,
                "--opt", "type=tmpfs", "--opt", "device=tmpfs", "--opt", "o=" + VOLUME_OPTIONS["o"], volume])
            require(commands.raw(made).strip() == volume.encode(), "Volume creation differs")
            inspected = checked("volume-created", ["docker", "volume", "inspect", "--format", "{{json .}}", volume])
            require(_volume_valid(json_record(inspected), volume, execution_id), "Volume ownership or bounds differ")
            container_attempted = True
            checked("container-start", _start_arguments(sandbox, name, workspace, checks, volume))
            session = _Session(commands, ["docker", "exec", "--interactive", "--user", "65534:65534", name,
                                           "python", "-I", "-B", "/checks/storage_adapter.py", case_id])
            for phase in PHASES:
                responses[phase] = session.phase(phase)
                checked(phase + "-pause", ["docker", "pause", name])
                state = checked(phase + "-state", ["docker", "inspect", "--format", "{{json .}}", name])
                require(_paused(json_record(state), volume, image_id), "Container snapshot not frozen")
                capture = checked(phase + "-capture", ["docker", "cp", name + ":/tmp", "-"], MAX_CAPTURE_BYTES)
                try:
                    snapshots[phase] = parse_capture(commands.raw(capture))
                except CaptureLayoutError as error:
                    layout_errors[phase] = str(error)
                checked(phase + "-unpause", ["docker", "unpause", name])
            session_finished = session.finish(True)
            session = None
            require(session_finished, "Candidate session termination incomplete")
    except (DriverError, OSError, subprocess.SubprocessError) as error:
        errors.append(type(error).__name__ + ":" + str(error)[:500])
    finally:
        if session is not None:
            # Remove the container before joining the exec streams; killing only
            # the Docker CLI does not terminate its candidate child in-container.
            if container_attempted:
                container_cleanup = sandbox._remove(name)
            try:
                session.finish(False)
            except (OSError, subprocess.SubprocessError, DriverError) as error:
                errors.append("session-cleanup:" + type(error).__name__)
        if container_attempted and not container_cleanup:
            container_cleanup = sandbox._remove(name)
        elif not container_attempted:
            container_cleanup = True
        if not container_cleanup:
            errors.append("container:cleanup-unverified")
        if volume_attempted:
            try:
                owned = checked("volume-cleanup-inspect", ["docker", "volume", "inspect", "--format", "{{json .}}", volume])
                require(_volume_valid(json_record(owned), volume, execution_id), "Volume cleanup ownership differs")
                require(container_cleanup, "Container cleanup required before volume removal")
                checked("volume-remove", ["docker", "volume", "rm", volume])
                absent = checked("volume-after", ["docker", "volume", "ls", "--quiet", "--filter", "name=^" + volume + "$"])
                volume_cleanup = not commands.raw(absent).strip()
            except (DriverError, OSError, subprocess.SubprocessError) as error:
                errors.append("volume-cleanup:" + type(error).__name__)
        else:
            volume_cleanup = True
        if not volume_cleanup:
            errors.append("volume:cleanup-unverified")
    if driver_sources() != sources:
        errors.append("driver:source-changed")
    terminal = {"protocol": PROTOCOL, "intent_sha256": sha256((root / "intent.json").read_bytes()),
        "case_id": case_id, "source_sha256": actual_source, "responses": responses, "runtime": runtime,
        "captures": {phase: [{"path": path, "bytes": len(raw), "sha256": sha256(raw)}
                     for path, raw in sorted(snapshot.items())] for phase, snapshot in snapshots.items()},
        "layout_errors": layout_errors, "infrastructure": errors,
        "container_cleanup": container_cleanup, "volume_cleanup": volume_cleanup,
        "commands": list(commands.records)}
    raw_terminal = encoded(terminal)
    commands.retain("terminal.json", raw_terminal)
    return StorageExecution(case_id, actual_source, str(root), snapshots, responses, tuple(errors),
                            layout_errors, container_cleanup and volume_cleanup, sha256(raw_terminal))
