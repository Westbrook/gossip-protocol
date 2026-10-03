"""One post-freeze browser observation with candidate Python confined to Docker.

Chromium's sandboxed renderer runs on the host, in a fresh credential-free
context. Its HTTP requests are routed through a bounded Docker-exec client to
the real server's private loopback. This does not qualify published-port HTTP,
arbitrary hostile browser exploits, or the complete four-milestone project.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import signal
import subprocess
import tempfile
import threading
import time
from typing import Any, Callable, Iterator
import uuid

from .blackbox_validator import BlackboxValidator
from .library_m1_acceptance_v4 import FrozenSubject, source_sha256
from .sandbox import DockerValidator

PROTOCOL = "library-m1-independent-browser-v4"
PURPOSE = "independent_acceptance"
ROOT = Path(__file__).resolve().parents[1]
DRIVER = ROOT / "devtools/browser/library_m1_acceptance.cjs"

# This trusted bootstrap creates input files and starts the real candidate CLI.
# It supplies no oracle or acceptance decision to the application container.
BOOTSTRAP = r'''import json,os,runpy,sys,zipfile
from pathlib import Path
root=Path('/tmp/intake'); root.mkdir()
(root/'folder').mkdir()
(root/'folder/a.txt').write_text('browser directory alpha',encoding='utf-8')
(root/'folder/b.md').write_text('browser directory beta',encoding='utf-8')
with zipfile.ZipFile(root/'bundle.zip','w') as archive:
    archive.writestr('z.txt','browser archive omega')
(root/'bundle.json').write_text(json.dumps({'entries':[
    {'source':'json/literal.html','text':'<img src=x onerror="window.__injected=true"> literal browser text'}]}))
(root/'one.txt').write_text('browser single-file import',encoding='utf-8')
os.chdir('/tmp')
sys.path.insert(0,'/workspace')
sys.argv=['library','--db','/tmp/browser.sqlite','--root',str(root),'serve','--port','8765']
runpy.run_module('library',run_name='__main__')
'''


def _bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=True, allow_nan=False,
                      separators=(",", ":")).encode()


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _save(path: Path, value: Any) -> None:
    _save_raw(path, _bytes(value))


def _save_raw(path: Path, raw: bytes) -> None:
    with path.open("xb") as stream:
        stream.write(raw); stream.flush(); os.fsync(stream.fileno())
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


@dataclass(frozen=True, slots=True)
class BrowserAcceptancePolicy:
    image_id: str
    node_executable: str
    timeout_seconds: int = 180
    node_modules_path: str | None = None

    def __post_init__(self) -> None:
        if (type(self.image_id) is not str
                or not re.fullmatch(r"sha256:[0-9a-f]{64}", self.image_id)):
            raise ValueError("Immutable local image ID required")
        if (type(self.node_executable) is not str or not Path(self.node_executable).is_absolute()
                or not Path(self.node_executable).is_file()):
            raise ValueError("An existing absolute Node executable is required")
        if type(self.timeout_seconds) is not int or not 60 <= self.timeout_seconds <= 300:
            raise ValueError("Browser timeout must be 60..300 integer seconds")
        if self.node_modules_path is not None and (
                type(self.node_modules_path) is not str
                or not Path(self.node_modules_path).is_absolute()
                or not Path(self.node_modules_path).is_dir()):
            raise ValueError("Explicit Node modules path must be an existing absolute directory")


def _environment(policy: BrowserAcceptancePolicy) -> dict[str, str]:
    environment = DockerValidator._environment()
    # NODE_PATH locates the already installed pinned package. NODE_OPTIONS and
    # provider credentials are deliberately absent. Chromium gets another,
    # smaller environment with a fresh HOME from the Node driver.
    modules = (Path(policy.node_modules_path) if policy.node_modules_path is not None
               else Path(policy.node_executable).resolve().parent.parent / "node_modules")
    if modules.is_dir():
        environment["NODE_PATH"] = str(modules.resolve())
    if "PLAYWRIGHT_BROWSERS_PATH" in os.environ:
        environment["PLAYWRIGHT_BROWSERS_PATH"] = os.environ["PLAYWRIGHT_BROWSERS_PATH"]
    return environment


def runtime_identity(policy: BrowserAcceptancePolicy) -> dict[str, Any]:
    """Read pinned Node/Playwright/Chromium identities without launching a browser.

    No Docker, provider, key loading, installation or global environment change.
    The same deterministic module path is used by the physical driver later.
    """
    if type(policy) is not BrowserAcceptancePolicy:
        raise ValueError("Typed browser policy required")
    environment = _environment(policy)
    result = subprocess.run([policy.node_executable, str(DRIVER), "--probe"],
        capture_output=True, timeout=30, env=environment, check=False)
    if result.returncode or not 1 <= len(result.stdout) <= 1_048_576:
        raise ValueError("Pinned browser runtime qualification failed")
    value = json.loads(result.stdout)
    if type(value) is not dict:
        raise ValueError("Invalid browser runtime identity")
    value["nodeModulesPath"] = environment.get("NODE_PATH")
    return value


def _probe(policy: BrowserAcceptancePolicy) -> dict[str, Any]:
    environment = _environment(policy)
    commands = {
        "docker_server": ["docker", "version", "--format", "{{json .Server}}"],
        "image": ["docker", "image", "inspect", "--format", "{{json .}}", policy.image_id],
    }
    values: dict[str, Any] = {}
    for name, command in commands.items():
        result = subprocess.run(command, capture_output=True, timeout=30,
                                env=environment, check=False)
        if result.returncode or not 1 <= len(result.stdout) <= 1_048_576:
            raise ValueError("Browser/Docker runtime qualification failed: " + name)
        values[name] = json.loads(result.stdout)
    if values["image"].get("Id") != policy.image_id:
        raise ValueError("Docker image differs from pinned image")
    values["image"] = {key: values["image"].get(key)
                       for key in ("Id", "Os", "Architecture", "Variant")}
    values["browser"] = runtime_identity(policy)
    return values


def _sandbox(policy: BrowserAcceptancePolicy) -> DockerValidator:
    return DockerValidator(policy.image_id, {"bootstrap.py": BOOTSTRAP},
        command=("python", "-I", "/checks/bootstrap.py"),
        timeout_seconds=policy.timeout_seconds)


def _files_from_directory(directory: Path) -> dict[str, str]:
    files: dict[str, str] = {}
    for entry in sorted(directory.rglob("*")):
        if entry.is_symlink():
            raise ValueError("Browser source staging gained a symlink")
        if entry.is_file():
            files[entry.relative_to(directory).as_posix()] = entry.read_bytes().decode("utf-8")
    return files


@dataclass
class _SignalState:
    cleanup: bool = False
    pending: KeyboardInterrupt | None = None


@contextmanager
def _owned_signals() -> Iterator[_SignalState]:
    if threading.current_thread() is not threading.main_thread():
        raise ValueError("Browser acceptance must own cancellation on the main thread")
    signals = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
    previous = {number: signal.getsignal(number) for number in signals}
    state = _SignalState()

    def interrupt(number: int, _frame: Any) -> None:
        # One cancellation owns the bounded cleanup; repeated signals must not
        # interrupt removal of the driver's browser or the owned container.
        for repeated in signals:
            signal.signal(repeated, signal.SIG_IGN)
        state.pending = KeyboardInterrupt("Browser acceptance cancelled by signal " + str(number))
        if not state.cleanup:
            raise state.pending

    try:
        for number in signals:
            signal.signal(number, interrupt)
        yield state
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)


def _stop_driver(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill(); process.wait(timeout=5)


def _invoke(policy: BrowserAcceptancePolicy, name: str, output: Path) -> dict[str, Any]:
    # File-backed output is never a candidate log channel: the driver emits only
    # bounded authored diagnostics, and candidate responses are capped in Node.
    with (output / "driver.log").open("xb") as log:
        process = subprocess.Popen([policy.node_executable, str(DRIVER),
            "--container", name, "--output", str(output / "browser")],
            stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
            env=_environment(policy))
        try:
            code = process.wait(timeout=policy.timeout_seconds)
        except subprocess.TimeoutExpired:
            _stop_driver(process)
            return {"passed": False, "status": "driver_timeout", "cleanup": {"browser": "unverified"}}
        except BaseException as original:
            try:
                _stop_driver(process)
            except Exception as cleanup_error:
                original.add_note("Owned browser driver cleanup failed: " + type(cleanup_error).__name__)
            raise
    receipt_file = output / "browser/receipt.json"
    if not receipt_file.is_file() or receipt_file.stat().st_size > 1_048_576:
        return {"passed": False, "status": "missing_driver_receipt", "exit_code": code}
    receipt = json.loads(receipt_file.read_bytes())
    if type(receipt) is not dict or receipt.get("container") != name:
        raise ValueError("Browser driver receipt has wrong container binding")
    if code != 0:
        receipt["passed"] = False
    receipt["exit_code"] = code
    return receipt


_LOADED_SOURCES = {str(file.relative_to(ROOT)): _sha(file.read_bytes()) for file in (
    Path(__file__), DRIVER, ROOT / "devtools/browser/lifecycle.cjs",
    ROOT / "devtools/browser/package.json", ROOT / "devtools/browser/package-lock.json",
    Path(__file__).with_name("library_m1_acceptance_v4.py"),
    Path(__file__).with_name("sandbox.py"), Path(__file__).with_name("blackbox_validator.py"))}


def _unchanged_sources() -> dict[str, str]:
    current = {relative: _sha((ROOT / relative).read_bytes()) for relative in _LOADED_SOURCES}
    if current != _LOADED_SOURCES:
        raise ValueError("Loaded browser evaluator source changed")
    return current


def run_once(output: Path, files: dict[str, str], subject: FrozenSubject,
             freeze_receipt: bytes, verify_freeze: Callable[[FrozenSubject, bytes], bool],
             policy: BrowserAcceptancePolicy) -> dict[str, Any]:
    """Fresh observation; caller must verify the durable complete-cohort freeze.

    Exact purpose, source, Git subject, evaluator/runtime and freeze receipt are
    persisted before candidate execution. Existing intents are never replayed.
    The callback is trusted harness code, never supplied by a candidate.
    """
    if type(subject) is not FrozenSubject or type(policy) is not BrowserAcceptancePolicy:
        raise ValueError("Typed browser subject and policy required")
    if threading.current_thread() is not threading.main_thread():
        raise ValueError("Browser acceptance must own cancellation on the main thread")
    files, _, _ = BlackboxValidator._inputs(files, [{"input": {}, "expected": None}])
    if source_sha256(files) != subject.source_sha256:
        raise ValueError("Browser source differs from frozen subject")
    if type(freeze_receipt) is not bytes or not 1 <= len(freeze_receipt) <= 1_048_576:
        raise ValueError("Bounded durable freeze receipt required")
    if verify_freeze(subject, freeze_receipt) is not True:
        raise ValueError("Complete-cohort source freeze is not verified")
    output = Path(output)
    if not output.is_absolute() or output != output.resolve() or output.exists():
        raise ValueError("A new canonical absolute browser output is required")
    bindings = _unchanged_sources()
    runtime = _probe(policy)
    sandbox = _sandbox(policy)
    name = "gossip-browser-" + uuid.uuid4().hex
    arguments = sandbox._arguments(name, Path("/WORKSPACE"), Path("/CHECKS"))
    arguments.insert(2, "--detach")
    intent = {"protocol": PROTOCOL, "purpose": PURPOSE, "subject": asdict(subject),
        "freeze_receipt_sha256": _sha(freeze_receipt), "policy": asdict(policy),
        "runtime": runtime, "evaluator_sources": bindings,
        "sandbox_arguments": arguments, "bootstrap_sha256": _sha(BOOTSTRAP.encode()),
        "environment_sha256": _sha(_bytes(_environment(policy))),
        "host_python": {"implementation": platform.python_implementation(), "version": platform.python_version()},
        "container": name, "reuse_allowed": False,
        "transport": "Playwright route fulfillment via bounded Docker-exec HTTP to private loopback",
        "candidate_python_boundary": "network-none read-only Docker",
        "candidate_browser_boundary": "fresh sandboxed host Chromium renderer; outbound proxy closed",
        "resource_limitations": ["host Chromium memory is not OS-capped", "trace and screenshot disk volume is not OS-capped"],
        "unqualified_dimensions": ["published-port browser HTTP", "full_M1_requirements", "M2", "M3", "M4"]}
    output.mkdir(parents=False, exist_ok=False)
    descriptor = os.open(output.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    _save(output / "intent.json", intent)
    _save_raw(output / "freeze-receipt.bin", freeze_receipt)
    started = time.monotonic()
    answer: dict[str, Any] = {"protocol": PROTOCOL, "purpose": PURPOSE,
        "intent_sha256": _sha(_bytes(intent)), "source_sha256": subject.source_sha256,
        "status": "not_started", "browser_checks_passed": False, "reused": False,
        "whole_project_acceptance": False, "physically_executed": False,
        "physical_execution_requested": False, "evaluation_completed": False,
        "cleanup_verified": False}
    interruption: BaseException | None = None
    owned_signals = _owned_signals()
    signal_state = owned_signals.__enter__()
    try:
        with tempfile.TemporaryDirectory(prefix="gossip-browser-source-") as temporary:
            base = Path(temporary).resolve()
            workspace, checks = base / "workspace", base / "checks"
            workspace.mkdir(mode=0o755); checks.mkdir(mode=0o755)
            for relative, content in files.items():
                file = workspace / relative
                file.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
                file.write_text(content, encoding="utf-8"); file.chmod(0o444)
            (checks / "bootstrap.py").write_text(BOOTSTRAP, encoding="utf-8")
            (checks / "bootstrap.py").chmod(0o444)
            if source_sha256(_files_from_directory(workspace)) != subject.source_sha256:
                raise ValueError("Staged browser source differs from frozen source")
            if verify_freeze(subject, freeze_receipt) is not True:
                raise ValueError("Complete-cohort source freeze changed before dispatch")
            _unchanged_sources()
            actual = sandbox._arguments(name, workspace, checks)
            actual.insert(2, "--detach")
            answer["physical_execution_requested"] = True
            launched = subprocess.run(actual, capture_output=True, timeout=30,
                                      env=sandbox._environment(), check=False)
            if launched.returncode or not re.fullmatch(rb"[0-9a-f]{64}\n?", launched.stdout):
                raise ValueError("Candidate browser server container did not start")
            answer["physically_executed"] = True
            answer["container_id"] = launched.stdout.decode().strip()
            browser = _invoke(policy, name, output)
            _save(output / "driver-receipt.json", browser)
            answer["driver_receipt_sha256"] = _sha(_bytes(browser))
            answer["status"] = "passed" if browser.get("passed") is True else browser.get("status", "infrastructure_failure")
            answer["browser_checks_passed"] = browser.get("passed") is True
            answer["evaluation_completed"] = browser.get("evaluationCompleted") is True
            answer["browser_cleanup_verified"] = browser.get("cleanup", {}).get("browser") == "complete"
            if source_sha256(_files_from_directory(workspace)) != subject.source_sha256:
                raise ValueError("Browser source changed during observation")
            if verify_freeze(subject, freeze_receipt) is not True:
                raise ValueError("Complete-cohort source freeze changed")
            _unchanged_sources()
            if _probe(policy) != runtime:
                raise ValueError("Browser runtime changed during observation")
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        answer["status"] = "infrastructure_failure"
        answer["browser_checks_passed"] = False
        answer["error"] = type(error).__name__ + ": " + str(error)[:512]
    except BaseException as error:
        answer["status"] = "cancelled"
        answer["browser_checks_passed"] = False
        answer["error"] = type(error).__name__
        interruption = error
    finally:
        # The first signal may arrive during Docker removal or fsync. Defer it
        # until cleanup and durable evidence finish rather than raising through
        # this finally block. A signal after receipt commit gets its own record.
        signal_state.cleanup = True
        try:
            answer["cleanup_verified"] = sandbox._remove(name)
            if signal_state.pending is not None:
                interruption = signal_state.pending
                answer["status"] = "cancelled"
                answer["browser_checks_passed"] = False
                answer["error"] = "KeyboardInterrupt"
            if not answer["cleanup_verified"] or (answer["physically_executed"]
                    and not answer.get("browser_cleanup_verified", False)):
                answer["browser_checks_passed"] = False
                answer["status"] = "cleanup_failed"
            answer["elapsed_seconds"] = round(time.monotonic() - started, 6)
            _save(output / "receipt.json", answer)
            if signal_state.pending is not None:
                interruption = signal_state.pending
                _save(output / "cancellation.json", {"reason": str(signal_state.pending),
                    "terminal_receipt_sha256": _sha((output / "receipt.json").read_bytes()),
                    "cleanup_completed_before_propagation": True})
        finally:
            owned_signals.__exit__(None, None, None)
    if interruption is not None:
        raise interruption
    return answer
