"""Run candidate functions in Docker; keep the oracle on the host.

Only candidate source, a generic transport adapter, and JSON inputs enter the
container. Every returned byte is untrusted: a zero exit code or a candidate's
own ``passed`` field is never an acceptance signal. Each case gets a fresh Python
process. Cases share the candidate's container and tmpfs, so this is not a claim
of a separate security boundary between cases or VM-grade isolation.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import hashlib
import json
import math
from pathlib import Path, PurePosixPath
import subprocess
import tempfile
import threading
import time
import uuid

from .pilot import DEFAULT_IMAGE
from .sandbox import DockerValidator


PROTOCOL = "gossip-blackbox-v1"
MAX_CASES = 512
MAX_INPUT_BYTES = 1_048_576
MAX_CASE_BYTES = 65_536
MAX_SOURCE_BYTES = 1_048_576
MAX_OUTPUT_BYTES = 4_194_304

CHILD_ADAPTER = r'''import importlib.util
import json
import sys

payload = json.loads(sys.stdin.buffer.read(65537))
sys.path.insert(0, "/workspace")
spec = importlib.util.spec_from_file_location("solution", "/workspace/solution.py")
module = importlib.util.module_from_spec(spec)
sys.modules["solution"] = module
spec.loader.exec_module(module)
answer = module.solve(payload)
sys.stdout.write(json.dumps(answer, ensure_ascii=True, allow_nan=False, separators=(",", ":")))
sys.stdout.write("\n")
'''

# The supervisor never imports candidate code. Child stdout is a single JSON
# value, not a result receipt. Imports that print, duplicate values, NaNs,
# exceptions, hangs, and excess output all fail closed. Child stderr is bounded
# and deliberately omitted from the supervisor's output.
SUPERVISOR_ADAPTER = r'''import json
import os
import signal
import subprocess
import sys
import tempfile
import threading

LIMIT = 65536

def reject_constant(_):
    raise ValueError("nonfinite JSON")

def pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result

def strict_json(raw):
    return json.loads(raw.decode("utf-8"), parse_constant=reject_constant,
                      object_pairs_hook=pairs)

def stop(process):
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass

def execute(item, timeout):
    output = bytearray()
    counts = [0, 0]
    overflow = threading.Event()
    readers_failed = threading.Event()
    result = {"index": item["index"], "status": "error"}
    with tempfile.TemporaryDirectory(prefix="case-", dir="/tmp") as directory:
        process = subprocess.Popen(
            [sys.executable, "-I", "/checks/child.py"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            cwd=directory, start_new_session=True,
        )
        def drain(stream, which):
            try:
                while True:
                    chunk = stream.read(8192)
                    if not chunk:
                        break
                    counts[which] += len(chunk)
                    if which == 0 and len(output) < LIMIT:
                        output.extend(chunk[:LIMIT - len(output)])
                    if counts[which] > LIMIT:
                        overflow.set()
                        stop(process)
            except (OSError, ValueError):
                readers_failed.set()

        threads = [threading.Thread(target=drain, args=(stream, which), daemon=True)
                   for which, stream in enumerate((process.stdout, process.stderr))]
        for thread in threads:
            thread.start()
        try:
            raw = json.dumps(item["input"], ensure_ascii=True, allow_nan=False,
                             separators=(",", ":")).encode("utf-8")
            process.stdin.write(raw)
            process.stdin.close()
            process.wait(timeout=timeout)
            result["status"] = "error" if process.returncode else "ok"
        except subprocess.TimeoutExpired:
            result["status"] = "timeout"
        except (BrokenPipeError, OSError):
            result["status"] = "error"
        finally:
            stop(process)  # Also remove child processes left after solve() returns.
            process.wait(timeout=2)
            for thread in threads:
                thread.join(timeout=2)
            if any(thread.is_alive() for thread in threads) or readers_failed.is_set():
                result["status"] = "output_error"
            for stream in (process.stdout, process.stderr):
                stream.close()
        if overflow.is_set():
            result["status"] = "output_limit"
        if result["status"] == "ok":
            try:
                result["output"] = strict_json(bytes(output))
            except (ValueError, UnicodeError, RecursionError):
                result["status"] = "invalid_output"
        return result

request = strict_json(sys.stdin.buffer.read(1048577))
results = [execute(item, request["case_timeout_seconds"]) for item in request["cases"]]
sys.stdout.write(json.dumps({"protocol": "gossip-blackbox-v1", "results": results},
                          ensure_ascii=True, allow_nan=False, separators=(",", ":")))
sys.stdout.write("\n")
'''


def _canonical(value) -> str:
    return json.dumps(value, ensure_ascii=True, allow_nan=False, sort_keys=True,
                      separators=(",", ":"))


def _validate_json(value, depth=0, *, max_depth=64):
    if depth > max_depth:
        raise ValueError("JSON nesting exceeds the limit")
    if value is None or type(value) in (str, bool, int):
        return
    if type(value) is float and math.isfinite(value):
        return
    if type(value) is list:
        for item in value:
            _validate_json(item, depth + 1, max_depth=max_depth)
        return
    if type(value) is dict and all(type(key) is str for key in value):
        for item in value.values():
            _validate_json(item, depth + 1, max_depth=max_depth)
        return
    raise ValueError("Only finite JSON values are supported")


def _strict_json(raw: bytes):
    def reject_constant(_):
        raise ValueError("Nonfinite JSON output")

    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("Duplicate JSON key")
            result[key] = value
        return result

    value = json.loads(raw.decode("utf-8"), parse_constant=reject_constant,
                       object_pairs_hook=pairs)
    # The transport contributes three levels above an individual output value.
    _validate_json(value, max_depth=67)
    return value


def json_equal(actual, expected) -> bool:
    """Compare JSON-decoded types exactly, including integer output contracts."""
    if type(actual) is not type(expected):
        return False
    if isinstance(actual, dict):
        return (actual.keys() == expected.keys()
                and all(json_equal(actual[key], expected[key]) for key in actual))
    if isinstance(actual, list):
        return len(actual) == len(expected) and all(
            json_equal(left, right) for left, right in zip(actual, expected))
    return actual == expected


class BlackboxValidator:
    """One bounded Docker invocation per candidate/suite, with a host oracle.

    ``evaluate`` takes UTF-8 source files (including ``solution.py``), plus case
    dictionaries with ``input`` and ``expected``. Optional ``id`` and
    ``requirement`` fields are retained in the host receipt only. Invalid harness
    inputs raise ValueError before starting Docker. Runtime failures return a
    complete failed receipt. Instances may be reused sequentially.
    """

    def __init__(self, image: str = DEFAULT_IMAGE, *, timeout_seconds: float = 60,
                 case_timeout_seconds: float = 2):
        if (isinstance(case_timeout_seconds, bool)
                or not isinstance(case_timeout_seconds, (int, float))
                or not math.isfinite(case_timeout_seconds)
                or case_timeout_seconds <= 0):
            raise ValueError("Case timeout must be finite and positive")
        self._sandbox = DockerValidator(
            image, {"supervisor.py": SUPERVISOR_ADAPTER, "child.py": CHILD_ADAPTER},
            command=("python", "-I", "/checks/supervisor.py"),
            timeout_seconds=timeout_seconds,
        )
        self.image = image
        self.timeout_seconds = timeout_seconds
        self.case_timeout_seconds = case_timeout_seconds
        self.last_receipt = {}

    def preflight(self) -> tuple[bool, str]:
        return self._sandbox.preflight()

    @staticmethod
    def _inputs(files, cases):
        if not isinstance(files, dict) or not files or "solution.py" not in files:
            raise ValueError("Candidate source must include solution.py")
        if len(files) > 128:
            raise ValueError("Candidate exceeds the file limit")
        total = 0
        for filename, content in files.items():
            if (not isinstance(filename, str) or not filename or "\x00" in filename
                    or "\\" in filename or PurePosixPath(filename).is_absolute()
                    or any(part in {"", ".", ".."} or part.startswith(".")
                           for part in filename.split("/"))
                    or not isinstance(content, str)):
                raise ValueError("Candidate files require safe relative paths and text")
            try:
                total += len(content.encode("utf-8"))
            except UnicodeError:
                raise ValueError("Candidate source must be valid UTF-8") from None
        if total > MAX_SOURCE_BYTES:
            raise ValueError("Candidate exceeds the source byte limit")
        if (not isinstance(cases, Sequence) or isinstance(cases, (str, bytes))
                or not 1 <= len(cases) <= MAX_CASES):
            raise ValueError("A bounded nonempty case sequence is required")
        prepared = []
        for index, case in enumerate(cases):
            if not isinstance(case, Mapping) or not {"input", "expected"} <= case.keys():
                raise ValueError("Every case requires input and expected values")
            for key in ("input", "expected"):
                _validate_json(case[key])
                if len(_canonical(case[key]).encode()) > MAX_CASE_BYTES:
                    raise ValueError("Case input or expectation exceeds the byte limit")
            if any(key in case and not isinstance(case[key], str)
                   for key in ("id", "requirement")):
                raise ValueError("Optional case labels must be strings")
            prepared.append({key: case[key] for key in
                             ("input", "expected", "id", "requirement") if key in case})
        # Detach from caller-owned mutable values before starting the process.
        return dict(files), json.loads(_canonical(prepared)), total

    def evaluate(self, files: dict[str, str], cases: Sequence[dict]) -> dict:
        files, cases, source_bytes = self._inputs(files, cases)
        request = {"protocol": PROTOCOL, "case_timeout_seconds": self.case_timeout_seconds,
                   "cases": [{"index": index, "input": case["input"]}
                             for index, case in enumerate(cases)]}
        payload = _canonical(request).encode("utf-8")
        if len(payload) > MAX_INPUT_BYTES:
            raise ValueError("The input batch exceeds the byte limit")
        started = time.monotonic()
        name = "gossip-blackbox-" + uuid.uuid4().hex
        receipt = {
            "schema_version": 1, "protocol": PROTOCOL, "passed": False,
            "status": "starting", "container_name": name, "image_id": self.image,
            "adapter_sha256": self._sandbox.checks_sha256,
            "source_sha256": hashlib.sha256(_canonical(files).encode()).hexdigest(),
            "suite_sha256": hashlib.sha256(_canonical(cases).encode()).hexdigest(),
            "staging": {"files": len(files), "bytes": source_bytes},
            "case_count": len(cases), "exit_code": None, "timed_out": False,
            "timeout_seconds": self.timeout_seconds,
            "case_timeout_seconds": self.case_timeout_seconds,
            "cleanup_verified": None, "output_truncated": False,
            "input_delivery_failed": False, "outcomes": [],
        }
        self.last_receipt = receipt
        process = None
        output = bytearray()
        output_count = [0]
        io_errors = []
        readers = []
        try:
            with tempfile.TemporaryDirectory(prefix="gossip-blackbox-") as temporary:
                staging = Path(temporary).resolve()
                workspace, adapters = staging / "workspace", staging / "checks"
                workspace.mkdir(mode=0o755)
                adapters.mkdir(mode=0o755)
                for directory, contents in ((workspace, files),
                                            (adapters, self._sandbox.tests)):
                    for filename, content in contents.items():
                        destination = directory / filename
                        destination.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
                        destination.write_text(content, encoding="utf-8")
                        destination.chmod(0o444)
                arguments = self._sandbox._arguments(name, workspace, adapters)
                arguments.insert(2, "--interactive")
                process = subprocess.Popen(
                    arguments, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    stdin=subprocess.PIPE, env=self._sandbox._environment(), cwd=staging,
                )

                def drain():
                    try:
                        while chunk := process.stdout.read(8192):
                            output_count[0] += len(chunk)
                            if len(output) < MAX_OUTPUT_BYTES:
                                output.extend(chunk[:MAX_OUTPUT_BYTES - len(output)])
                    except (OSError, ValueError):
                        io_errors.append("read")

                def feed():
                    try:
                        process.stdin.write(payload)
                        process.stdin.close()
                    except (OSError, ValueError):
                        io_errors.append("write")

                readers = [threading.Thread(target=drain, daemon=True),
                           threading.Thread(target=feed, daemon=True)]
                for reader in readers:
                    reader.start()
                try:
                    receipt["exit_code"] = process.wait(timeout=self.timeout_seconds)
                    receipt["status"] = "evaluated" if process.returncode == 0 else "sandbox_error"
                except subprocess.TimeoutExpired:
                    receipt["timed_out"] = True
                    receipt["status"] = "timeout"
                    process.kill()
                    process.wait(timeout=5)
                finally:
                    receipt["cleanup_verified"] = self._sandbox._remove(name)
                    for reader in readers:
                        reader.join(timeout=5)
                    if any(reader.is_alive() for reader in readers) or io_errors:
                        receipt["status"] = "output_error"
                    receipt["input_delivery_failed"] = "write" in io_errors
                    if not any(reader.is_alive() for reader in readers):
                        process.stdout.close()
        except (OSError, ValueError, subprocess.SubprocessError):
            receipt["status"] = "sandbox_error"
        finally:
            if process is not None:
                try:
                    if process.poll() is None:
                        process.kill()
                        process.wait(timeout=5)
                except (OSError, subprocess.SubprocessError):
                    receipt["status"] = "sandbox_error"
                if receipt["cleanup_verified"] is None:
                    receipt["cleanup_verified"] = self._sandbox._remove(name)
                if not receipt["cleanup_verified"]:
                    receipt["status"] = "cleanup_failed"
            receipt["output_truncated"] = output_count[0] > MAX_OUTPUT_BYTES
            receipt["output_bytes"] = output_count[0]
            if receipt["output_truncated"] and receipt["status"] == "evaluated":
                receipt["status"] = "output_limit"
            receipt["output_sha256"] = hashlib.sha256(output).hexdigest()

        results = None
        if receipt["status"] == "evaluated":
            try:
                envelope = _strict_json(bytes(output))
                if (not isinstance(envelope, dict) or set(envelope) != {"protocol", "results"}
                        or envelope["protocol"] != PROTOCOL
                        or not isinstance(envelope["results"], list)
                        or len(envelope["results"]) != len(cases)):
                    raise ValueError("Invalid output envelope")
                results = envelope["results"]
                for index, result in enumerate(results):
                    if (not isinstance(result, dict) or type(result.get("index")) is not int
                            or result["index"] != index
                            or not isinstance(result.get("status"), str)
                            or result.get("status") not in {
                                "ok", "error", "timeout", "output_error", "output_limit",
                                "invalid_output"}
                            or set(result) != ({"index", "status", "output"}
                                              if result.get("status") == "ok"
                                              else {"index", "status"})):
                        raise ValueError("Invalid case envelope")
                    if result["status"] == "ok":
                        _validate_json(result["output"])
            except (ValueError, UnicodeError, RecursionError):
                receipt["status"] = "invalid_output"
                results = None
        for index, case in enumerate(cases):
            outcome = {"index": index, "passed": False,
                       "status": receipt["status"] if results is None else results[index]["status"]}
            for key in ("id", "requirement"):
                if key in case:
                    outcome[key] = case[key]
            if results is not None and results[index]["status"] == "ok":
                outcome["actual"] = results[index]["output"]
                outcome["passed"] = json_equal(outcome["actual"], case["expected"])
                outcome["status"] = "passed" if outcome["passed"] else "wrong_answer"
            receipt["outcomes"].append(outcome)
        if receipt["status"] == "evaluated":
            receipt["passed"] = all(item["passed"] for item in receipt["outcomes"])
            receipt["status"] = "passed" if receipt["passed"] else "failed"
        receipt["runtime_seconds"] = round(time.monotonic() - started, 6)
        return receipt
