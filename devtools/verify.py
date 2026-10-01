"""Fast-first, class-isolated verification with exact-input evidence receipts.

Run ``python -m devtools.verify --help``. No test module is imported in the
controller. An unchanged successful class may be reused only by explicit opt-in;
Docker is always a fresh observation. Queued work is cancelled on failure while
already started workers drain, preserving their cleanup and evidence.
"""
from __future__ import annotations

import argparse
import ast
from collections import Counter
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import importlib.util
import json
import math
import os
from pathlib import Path
import platform
import signal
import shutil
import subprocess
import sys
import time
import traceback
from typing import Any, cast
import unittest
import uuid

# Workers run this file by absolute path from disposable fixture/report roots.
# Resolve shared tooling from this runner's source tree before reading the job.
if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from devtools.telemetry import IOAccounting, ProcessTelemetry

SCHEMA = 1
OFFLINE = ("fast", "fixtures", "git")
LANES = (*OFFLINE, "docker", "report", "browser")
EXCLUDED = {".git", ".venv", "venv", "__pycache__", "runs", "results", "node_modules"}


def dump(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def load_manifest(root: Path) -> dict:
    manifest = json.loads((root / "verification-manifest.json").read_text())
    if manifest.get("schema_version") != SCHEMA or not manifest.get("test_roots"):
        raise ValueError("invalid verification manifest schema or roots")
    return manifest


def discover(root: Path, manifest: dict) -> list[dict]:
    """Parse explicit roots without importing test modules or running fixtures."""
    found = []
    seen = set()
    for directory in manifest["test_roots"]:
        path = root / directory
        if not path.is_dir() or not path.resolve().is_relative_to(root.resolve()):
            raise ValueError(f"missing or outside test root: {directory}")
        for source in sorted(path.rglob("test*.py")):
            if not source.resolve().is_relative_to(root.resolve()):
                raise ValueError(f"test source escapes project: {source}")
            if any(part in EXCLUDED for part in source.relative_to(path).parts):
                continue
            relative = source.relative_to(root).as_posix()
            module = ast.parse(source.read_bytes(), filename=relative)
            if any(isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "load_tests" for node in module.body):
                raise ValueError(f"load_tests is unsupported by explicit AST inventory: {relative}")
            case_classes = {node.name for node in module.body if isinstance(node, ast.ClassDef)
                            and any(ast.unparse(base) in ("unittest.TestCase", "TestCase") for base in node.bases)}
            for cls in module.body:
                if not isinstance(cls, ast.ClassDef):
                    continue
                if any(ast.unparse(base) in case_classes for base in cls.bases):
                    raise ValueError(f"inherited test classes require explicit direct methods: {relative}::{cls.name}")
                if any(isinstance(node, (ast.Assign, ast.AnnAssign)) and any(
                        isinstance(child, ast.Name) and child.id.startswith("test") for child in ast.walk(node))
                        for node in cls.body):
                    raise ValueError(f"dynamic test assignments unsupported: {relative}::{cls.name}")
                methods = sorted(node.name for node in cls.body
                                 if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                                 and node.name.startswith("test"))
                if not methods:
                    continue
                if not any(ast.unparse(base) in ("unittest.TestCase", "TestCase") for base in cls.bases):
                    raise ValueError(f"test methods require direct unittest.TestCase: {relative}::{cls.name}")
                if any(isinstance(node, ast.AsyncFunctionDef) and node.name.startswith("test") for node in cls.body):
                    raise ValueError(f"async tests are unsupported: {relative}::{cls.name}")
                key = f"{relative}::{cls.name}"
                if key in seen:
                    raise ValueError(f"duplicate test class: {key}")
                seen.add(key)
                if key not in manifest["classes"]:
                    raise ValueError(f"unmapped test class: {key}")
                config = manifest["classes"][key]
                overrides = config.get("methods", {})
                if set(overrides) - set(methods):
                    raise ValueError(f"unknown method overrides: {key}")
                for method in methods:
                    settings = {**config, **overrides.get(method, {})}
                    lane = settings["lane"]
                    if lane not in LANES or type(settings.get("weight", 1)) is not int or settings.get("weight", 1) < 1:
                        raise ValueError(f"invalid lane or resource weight: {key}.{method}")
                    if type(settings.get("exclusive", False)) is not bool:
                        raise ValueError(f"exclusive must be boolean: {key}.{method}")
                    timeout = settings.get("timeout_seconds", 180)
                    if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
                        raise ValueError(f"invalid timeout: {key}.{method}")
                    found.append({"id": f"{key}.{method}", "class": key,
                                  "path": relative, "name": cls.name, "method": method,
                                  "lane": lane, "weight": settings.get("weight", 1),
                                  "exclusive": settings.get("exclusive", False),
                                  "timeout_seconds": settings.get("timeout_seconds", 180)})
    obsolete = set(manifest["classes"]) - seen
    if obsolete:
        raise ValueError(f"manifest classes no longer discovered: {sorted(obsolete)}")
    if not found:
        raise ValueError("test inventory is empty")
    return found


def select(inventory: list[dict], lanes: list[str], selectors: list[str]) -> list[dict]:
    chosen = [test for test in inventory if test["lane"] in lanes]
    if selectors:
        selected = []
        for selector in selectors:
            prefix = selector.replace("::", ".").replace(".py", "").replace("/", ".")
            matches = [test for test in chosen if (
                test["id"].replace("::", ".").replace(".py", "").replace("/", ".") == prefix
                or test["id"].replace("::", ".").replace(".py", "").replace("/", ".").startswith(prefix + "."))]
            if not matches:
                raise ValueError(f"selector matched no tests in selected lanes: {selector}")
            selected.extend(matches)
        chosen = list({test["id"]: test for test in selected}.values())
    if not chosen:
        raise ValueError("selection contains no tests")
    return chosen


def inputs(root: Path, manifest: dict, accounting: IOAccounting | None = None) -> dict:
    paths = set()
    for pattern in manifest["input_globs"]:
        for path in root.glob(pattern):
            if path.is_file() and not any(part in EXCLUDED for part in path.relative_to(root).parts):
                if not path.resolve().is_relative_to(root.resolve()):
                    raise ValueError(f"input source escapes project: {path}")
                paths.add(path)
    for fixture in manifest.get("retained_fixtures", []):
        path = root / fixture
        if not path.resolve().is_relative_to(root.resolve()):
            raise ValueError(f"fixture escapes project: {fixture}")
        if not path.exists():
            raise ValueError(f"missing retained fixture: {fixture}")
        paths.update([path] if path.is_file() else (p for p in path.rglob("*") if p.is_file() and ".git" not in p.parts))
    if any(not path.resolve().is_relative_to(root.resolve()) for path in paths):
        raise ValueError("input symlink escapes project")
    return {path.relative_to(root).as_posix(): hashlib.sha256(accounting.read_bytes(path, "source_fingerprints") if accounting else path.read_bytes()).hexdigest()
            for path in sorted(paths)}


def worker_environment() -> dict[str, str]:
    environment = dict(os.environ)
    environment["PATH"] = str(Path(sys.executable).parent) + os.pathsep + environment.get("PATH", "")
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    environment.pop("GOSSIP_RUN_DOCKER_TESTS", None)
    return environment


def runtime_fingerprint() -> dict:
    """Bind installed distributions and runtime, without retaining environment values."""
    packages = sorted((item.metadata["Name"] or "", item.version,
                       hashlib.sha256((item.read_text("RECORD") or "").encode()).hexdigest())
                      for item in importlib.metadata.distributions())
    executable = Path(sys.executable).resolve()
    environment = worker_environment()
    tools = {}
    for name in ("git", "python", "python3", "node"):
        located = shutil.which(name, path=environment["PATH"])
        if located:
            binary = Path(located).resolve()
            version = subprocess.run([located, "--version"], capture_output=True, text=True, timeout=10, check=True)
            tools[name] = {"path": str(binary), "sha256": hashlib.sha256(binary.read_bytes()).hexdigest(), "version": version.stdout.strip()}
    return {"python": sys.version, "executable_sha256": hashlib.sha256(executable.read_bytes()).hexdigest(),
            "platform": platform.platform(), "packages_sha256": digest(packages),
            "environment_sha256": digest(environment), "root_runtime": str(executable), "tools": tools}


def make_jobs(selected: list[dict]) -> list[dict]:
    groups: dict[tuple[str, str], dict] = {}
    for test in selected:
        # A class with a method-level Docker opt-in is partitioned once per lane.
        key = (test["class"], test["lane"])
        group = groups.setdefault(key, {**test, "tests": [], "weight": test["weight"]})
        group["tests"].append(test)
        group["weight"] = max(group["weight"], test["weight"])
        group["exclusive"] = group.get("exclusive", False) or test.get("exclusive", False)
    return sorted(groups.values(), key=lambda job: (LANES.index(job["lane"]), job["class"]))


def _not_run(job: dict, reason: str) -> dict:
    return {"class": job["class"], "lane": job["lane"], "status": "not_run", "reason": reason,
            "tests": [{"id": test["id"], "status": "not_run"} for test in job["tests"]],
            "physical": False}


def _cache(output: Path, fingerprint: str) -> dict:
    available = {}
    for summary in sorted(output.glob("*/summary.json")):
        try:
            saved = json.loads(summary.read_text())
            if (saved.get("schema_version") != SCHEMA or saved.get("fingerprint") != fingerprint
                    or saved.get("stale_inputs") is not False or saved.get("error")
                    or saved.get("static", {}).get("status") != "passed"):
                continue
            for job in saved.get("jobs", []):
                if job.get("status") != "passed" or job.get("lane") not in OFFLINE or job.get("returncode") != 0 or job.get("cleanup_status") != "completed":
                    continue
                evidence = Path(job["evidence_path"])
                if hashlib.sha256(evidence.read_bytes()).hexdigest() != job["evidence_sha256"]:
                    continue
                if hashlib.sha256(Path(job["log"]).read_bytes()).hexdigest() != job["log_sha256"]:
                    continue
                original = json.loads(evidence.read_text())
                ids = [test["id"] for test in job["tests"]]
                if (not ids or len(set(ids)) != len(ids) or original.get("schema_version") != SCHEMA or original.get("physical") is not True or original.get("status") != "passed" or original.get("cleanup_status") != "completed"
                        or [test["id"] for test in original["tests"]] != ids
                        or any(test["status"] != "passed" for test in original["tests"] + job["tests"])):
                    continue
                key = digest(ids)
                available[key] = {**job, "physical": False, "reuse_lookup_seconds": 0.0,
                                  "original_duration_seconds": original["duration_seconds"],
                                  "duration_seconds": 0.0, "wall_seconds": 0.0, "queue_seconds": 0.0,
                                  "reused_from": job.get("reused_from", str(summary))}
        except (OSError, ValueError, KeyError, TypeError):
            continue
    return available


def run(root: Path, *, lanes: list[str] | None = None, selectors: list[str] | None = None, workers: int = 4,
        output: Path | None = None, reuse: bool = False, static_command: list[str] | None = None) -> dict:
    """Run one retained session. ``static_command`` is for disposable self-test fixtures."""
    root = root.resolve()
    output = (Path(output) if output else root / "runs/verification").resolve()
    if workers < 1:
        raise ValueError("workers must be positive")
    started = time.monotonic()
    resources = ProcessTelemetry("verification controller; child CPU overlaps worker observations")
    accounting = IOAccounting("verification controller source fingerprint reads")
    session = output / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8])
    session.mkdir(parents=True)
    summary: dict[str, Any] = {"schema_version": SCHEMA, "session": str(session), "status": "failed", "jobs": [],
               "workers": workers, "workers_started": 0, "peak_resource_tokens": 0, "first_negative_seconds": None,
               "discovered_total": 0, "selected_total": 0, "coverage": "none", "stale_inputs": False,
               "stale_runtime": False}
    jobs = []
    try:
        manifest = load_manifest(root)
        baseline = inputs(root, manifest, accounting)
        # A syntax error anywhere in authored inputs blocks even targeted runs.
        for name in baseline:
            if name.endswith(".py") and not any(part in EXCLUDED for part in Path(name).parts):
                ast.parse((root / name).read_bytes(), filename=name)
        inventory = discover(root, manifest)
        lane_names = list(lanes or manifest.get("default_lanes", OFFLINE))
        if "all" in lane_names:
            lane_names = list(LANES)
        if set(lane_names) - set(LANES):
            raise ValueError(f"unknown lanes: {sorted(set(lane_names) - set(LANES))}")
        auxiliary_inputs = {}
        if set(lane_names) & {"report", "browser"}:
            from devtools.verify_auxiliary import fingerprint as auxiliary_fingerprint, inventory as auxiliary_inventory
            inventory += auxiliary_inventory(root, lane_names)
            auxiliary_inputs = auxiliary_fingerprint(root, lane_names)
        selected = select(inventory, lane_names, list(selectors or []))
        jobs = make_jobs(selected)
        summary.update(discovered_total=len(inventory), selected_total=len(selected), lanes=lane_names)
        oversized = [job["class"] for job in jobs if job["weight"] > workers]
        if oversized:
            required = max(job["weight"] for job in jobs)
            raise ValueError(f"selected classes require {required} resource slots: {oversized}; use --workers {required} or narrower selectors")
        summary.update(discovered_total=len(inventory), selected_total=len(selected), lanes=lane_names,
                       inventory_counts=dict(Counter(test["lane"] for test in inventory)),
                       selected_counts=dict(Counter(test["lane"] for test in selected)),
                       coverage="full" if len(selected) == len(inventory) else
                       "offline" if set(test["id"] for test in selected) ==
                       {test["id"] for test in inventory if test["lane"] in OFFLINE} else "targeted")
        runtime = runtime_fingerprint()
        identity = {"inputs": baseline, "auxiliary_inputs": auxiliary_inputs, "runtime": runtime, "workers": workers,
                    "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                    "static_command": static_command}
        summary["fingerprint"] = digest(identity)
        dump(session / "inputs.json", identity)
        command = static_command or [sys.executable, "-m", "devtools.static_checks", "--output", str(session / "static.json")]
        static_started = time.monotonic()
        with (session / "static.log").open("w") as log:
            gate = subprocess.Popen(command, cwd=root, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            try:
                gate.wait(timeout=180)
            except (subprocess.TimeoutExpired, KeyboardInterrupt):
                _stop_owned(gate)
                raise
        summary["static"] = {"status": "passed" if gate.returncode == 0 else "failed", "returncode": gate.returncode,
                             "duration_seconds": time.monotonic() - static_started}
        if gate.returncode:
            summary["first_negative_seconds"] = time.monotonic() - started
            summary["jobs"] = [_not_run(job, "static gate failed") for job in jobs]
        else:
            cached = _cache(output, summary["fingerprint"]) if reuse else {}
            _schedule(root, session, jobs, workers, cached, summary, started)
        summary["stale_inputs"] = inputs(root, manifest, accounting) != baseline
        if auxiliary_inputs:
            summary["stale_inputs"] = summary["stale_inputs"] or auxiliary_fingerprint(root, lane_names) != auxiliary_inputs
        try:
            final_runtime = runtime_fingerprint()
        except Exception as error:
            summary["stale_runtime"] = True
            summary["stale_inputs"] = True
            raise RuntimeError(f"runtime identity could not be reconciled; evidence is stale: {error}") from error
        dump(session / "runtime-final.json", final_runtime)
        summary["stale_runtime"] = final_runtime != runtime
        summary["runtime_reconciliation"] = {
            "initial_sha256": digest(runtime), "final_sha256": digest(final_runtime),
            "matches": not summary["stale_runtime"],
        }
        summary["stale_inputs"] = summary["stale_inputs"] or summary["stale_runtime"]
        if summary["stale_inputs"]:
            summary["error"] = ("runtime changed during verification; evidence is stale" if summary["stale_runtime"]
                                else "inputs changed during verification; evidence is stale")
            summary["first_negative_seconds"] = summary["first_negative_seconds"] or time.monotonic() - started
        summary["status"] = "passed" if (summary["static"]["status"] == "passed" and not summary["stale_inputs"]
                                           and all(job["status"] == "passed" for job in summary["jobs"])) else "failed"
    except (Exception, KeyboardInterrupt) as error:
        summary["error"] = f"{type(error).__name__}: {error}"
        summary["first_negative_seconds"] = summary["first_negative_seconds"] or time.monotonic() - started
        if not summary["jobs"]:
            summary["jobs"] = [_not_run(job, "configuration or syntax failure") for job in jobs]
    summary["resources"] = resources.snapshot()
    summary["io_accounting"] = accounting.snapshot()
    summary["duration_seconds"] = time.monotonic() - started
    summary["outcomes"] = dict(Counter(test["status"] for job in summary["jobs"] for test in job["tests"]))
    summary["reused_classes"] = sum("reused_from" in job for job in summary["jobs"])
    dump(session / "summary.json", summary)
    return summary


def _stop_owned(process, grace=20):
    """Signal only our session leader, allow finally blocks, then reap its group."""
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=grace)
        except subprocess.TimeoutExpired:
            pass
    # The leader can exit before descendants. Always reap the owned group too.
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait(timeout=grace)


def _schedule(root, session, jobs, workers, cached, summary, started):
    pending = list(jobs)
    running: list[dict] = []
    failed = False
    queued_at = time.monotonic()
    completed = {}
    try:
        while pending or running:
            # Stop launching on any failed shard. Running jobs drain normally.
            used = sum(item["tokens"] for item in running)
            while pending and not failed:
                lane = pending[0]["lane"]
                if running and any(item["job"]["lane"] != lane for item in running):
                    break
                ready = None
                for index, candidate in enumerate(pending):
                    if candidate["lane"] != lane:
                        break
                    tokens = workers if candidate.get("exclusive", False) else candidate["weight"]
                    if used + tokens <= workers:
                        ready = (index, candidate, tokens)
                        break
                    if candidate.get("exclusive", False):
                        break  # Never bypass an explicit isolation barrier.
                if ready is None:
                    break
                index, job, tokens = ready
                pending.pop(index)
                key = digest([test["id"] for test in job["tests"]])
                if key in cached:
                    completed[(job["class"], job["lane"])] = cached[key]
                    continue
                shard = session / "classes" / key[:16]
                shard.mkdir(parents=True)
                job_root = Path(job.get("root", root))
                request = {"root": str(job_root), "job": job, "result": str(shard / "result.json")}
                dump(shard / "request.json", request)
                env = worker_environment()
                env.pop("GOSSIP_RUN_DOCKER_TESTS", None)
                if job["lane"] == "docker":
                    env["GOSSIP_RUN_DOCKER_TESTS"] = "1"
                env["GOSSIP_TEST_ARTIFACTS"] = str(shard / "artifacts")
                env["GOSSIP_VERIFY_WORKERS"] = str(tokens)
                env["GOSSIP_VERIFY_NEGATIVE"] = str(shard / "negative.json")
                # Import the runner by absolute path; fixture roots need no installation.
                command = [sys.executable, str(Path(__file__).resolve()), "--worker", str(shard / "request.json")]
                log = (shard / "worker.log").open("w")
                process = subprocess.Popen(command, cwd=job_root, env=env, stdout=log,
                                           stderr=subprocess.STDOUT, start_new_session=True)
                launched = time.monotonic()
                running.append({"process": process, "log": log, "job": job, "shard": shard,
                                "tokens": tokens, "started": launched,
                                "queue_seconds": launched - queued_at})
                summary["workers_started"] += 1
                used += tokens
                summary["peak_resource_tokens"] = max(summary["peak_resource_tokens"], used)
            for item in list(running):
                process = item["process"]
                if (item["shard"] / "negative.json").exists():
                    failed = True
                timed_out = time.monotonic() - item["started"] > item["job"]["timeout_seconds"]
                if timed_out and process.poll() is None:
                    _stop_owned(process)
                if process.poll() is None:
                    continue
                item["log"].close()
                result_path = item["shard"] / "result.json"
                try:
                    result = json.loads(result_path.read_text())
                except (OSError, ValueError):
                    result = _not_run(item["job"], "worker exited without result")
                    result.update(status="error", cleanup_status="unknown")
                expected_ids = [test["id"] for test in item["job"]["tests"]]
                if (not isinstance(result, dict) or [test.get("id") for test in result.get("tests", [])] != expected_ids):
                    result = _not_run(item["job"], "worker emitted incomplete or unexpected outcomes")
                    result["status"] = "error"
                if result.get("status") == "passed" and any(test.get("status") != "passed" for test in result["tests"]):
                    result["status"] = "error"
                if result_path.exists():
                    result["evidence_path"] = str(result_path)
                    result["evidence_sha256"] = hashlib.sha256(result_path.read_bytes()).hexdigest()
                result["log_sha256"] = hashlib.sha256((item["shard"] / "worker.log").read_bytes()).hexdigest()
                result.update(physical=True, queue_seconds=item["queue_seconds"],
                              wall_seconds=time.monotonic() - item["started"],
                              log=str(item["shard"] / "worker.log"), returncode=process.returncode,
                              tokens=item["tokens"])
                if timed_out or process.returncode:
                    result["status"] = "error"
                if timed_out:
                    result.update(reason="class deadline exceeded", cleanup_status="unknown after bounded termination")
                completed[(item["job"]["class"], item["job"]["lane"])] = result
                running.remove(item)
                if result["status"] != "passed":
                    failed = True
                    observed_negative = item["started"] - started + result.get("preparation_seconds", 0) + (result.get("first_negative_seconds") or (time.monotonic() - item["started"]))
                    previous = summary["first_negative_seconds"]
                    summary["first_negative_seconds"] = min(previous, observed_negative) if previous is not None else observed_negative
            if failed and pending:
                for job in pending:
                    completed[(job["class"], job["lane"])] = _not_run(job, "cancelled after earlier failure")
                pending.clear()
            if running:
                time.sleep(0.02)
    except BaseException:
        # Do not wait indefinitely after interruption, and keep partial receipts.
        for item in running:
            _stop_owned(item["process"])
            item["log"].close()
            try:
                result = json.loads((item["shard"] / "result.json").read_text())
            except (OSError, ValueError):
                result = _not_run(item["job"], "interrupted")
            result.update(status="error", physical=True, cleanup_status="unknown after interruption",
                          log=str(item["shard"] / "worker.log"))
            completed[(item["job"]["class"], item["job"]["lane"])] = result
        raise
    finally:
        summary["jobs"] = [completed.get((job["class"], job["lane"]), _not_run(job, "interrupted")) for job in jobs]


class RecordingResult(unittest.TextTestResult):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.outcomes = {}
        self.phases = {}
        self.negative_at = None
        self.started = time.monotonic()

    def startTest(self, test):
        super().startTest(test)
        self.phases[test.id()] = {"setup_seconds": 0.0, "run_seconds": 0.0, "cleanup_seconds": 0.0}
        for method, phase in (("_callSetUp", "setup_seconds"), ("_callTestMethod", "run_seconds"),
                              ("_callTearDown", "cleanup_seconds"), ("_callCleanup", "cleanup_seconds")):
            original = getattr(test, method)
            def timed(*args, _original=original, _phase=phase, **kwargs):
                start = time.monotonic()
                try:
                    return _original(*args, **kwargs)
                finally:
                    self.phases[test.id()][_phase] += time.monotonic() - start
            setattr(test, method, timed)

    def _record(self, test, status, detail=None):
        previous = self.outcomes.get(test.id(), {})
        if previous.get("status") in ("failed", "error") and status == "passed":
            return
        self.outcomes[test.id()] = {"status": status, **({"detail": detail} if detail else {})}
        if status != "passed" and self.negative_at is None:
            self.negative_at = time.monotonic() - self.started
            if os.environ.get("GOSSIP_VERIFY_NEGATIVE"):
                dump(Path(os.environ["GOSSIP_VERIFY_NEGATIVE"]), {"first_negative_seconds": self.negative_at})

    def addSuccess(self, test):
        super().addSuccess(test)
        self._record(test, "passed")

    def addFailure(self, test, err):
        super().addFailure(test, err)
        self._record(test, "failed", "".join(traceback.format_exception(*err)))

    def addError(self, test, err):
        super().addError(test, err)
        self._record(test, "error", "".join(traceback.format_exception(*err)))

    def addSkip(self, test, reason):
        super().addSkip(test, reason)
        self._record(test, "skipped", reason)

    def addExpectedFailure(self, test, err):
        super().addExpectedFailure(test, err)
        self._record(test, "expected_failure", "".join(traceback.format_exception(*err)))

    def addUnexpectedSuccess(self, test):
        super().addUnexpectedSuccess(test)
        self._record(test, "unexpected_success")

    def addSubTest(self, test, subtest, err):
        super().addSubTest(test, subtest, err)
        if err is not None:
            self._record(test, "failed", "".join(traceback.format_exception(*err)))


def _browser_command(command, root):
    """Let Node run Playwright's detached-browser cleanup before force fallback."""
    process = subprocess.Popen(command, cwd=root)
    try:
        return process.wait()
    except BaseException:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        raise


def worker(request_path: Path) -> int:
    resources = ProcessTelemetry("isolated verification worker; child CPU includes only OS-accounted waited children")
    request = json.loads(request_path.read_text())
    root, job = Path(request["root"]), request["job"]
    start = time.monotonic()
    def terminate(_signum, _frame):
        raise KeyboardInterrupt("verification worker cancelled")
    signal.signal(signal.SIGTERM, terminate)
    counters: Counter[str] = Counter()
    def audit(event, args):
        if event == "subprocess.Popen":
            executable = Path(str(args[0])).name.lower()
            category = "python" if "python" in executable else "docker" if executable == "docker" else "git" if executable == "git" else "other"
            counters[category] += 1
    sys.addaudithook(audit)
    response = _not_run(job, "worker failed before test execution")
    response["status"] = "error"
    response["physical"] = True
    class_phases = {"setup_seconds": 0.0, "cleanup_seconds": 0.0}
    captured: list[RecordingResult] = []
    runtime_ids: dict[str, str] = {}
    try:
        sys.path.insert(0, str(root))
        sys.path.insert(0, str(root / Path(job["path"]).parent))
        if "command" in job:
            browser_path = Path(request["result"]).with_name("browser.json")
            browser_returncode = _browser_command([*job["command"], "--output", str(browser_path)], root)
            from devtools.verify_auxiliary import validate_browser_receipt
            browser = validate_browser_receipt(browser_path)
            passed = browser_returncode == 0 and browser["passed"]
            response.update(status="passed" if passed else "failed", cleanup_status="completed" if passed else "unknown",
                            tests=[{"id": test["id"], "status": "passed" if passed else "failed"} for test in job["tests"]],
                            browser_receipt=str(browser_path), browser_results=browser.get("results", []),
                            duration_seconds=time.monotonic() - start, schema_version=SCHEMA,
                            setup_seconds=0.0, run_seconds=time.monotonic() - start, cleanup_seconds=0.0)
            response["resources"] = resources.snapshot()
            dump(Path(request["result"]), response)
            return 0 if passed else 1
        module_name = Path(job["path"]).stem
        spec = importlib.util.spec_from_file_location(module_name, root / job["path"])
        if spec is None or spec.loader is None:
            raise ImportError(job["path"])
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        cls = getattr(module, job["name"])
        for method, phase in (("setUpClass", "setup_seconds"), ("tearDownClass", "cleanup_seconds"),
                              ("doClassCleanups", "cleanup_seconds")):
            original = getattr(cls, method)
            def timed_class(_cls, _original=original, _phase=phase):
                began = time.monotonic()
                try:
                    return _original()
                finally:
                    class_phases[_phase] += time.monotonic() - began
            setattr(cls, method, classmethod(timed_class))
        instances = [cls(test["method"]) for test in job["tests"]]
        runtime_ids = {test.id(): manifest_test["id"] for test, manifest_test in zip(instances, job["tests"])}
        preparation_seconds = time.monotonic() - start
        def capture_result(*args, **kwargs):
            result = RecordingResult(*args, **kwargs)
            captured.append(result)
            return result
        runner = unittest.TextTestRunner(verbosity=2, resultclass=capture_result, failfast=True)
        result = cast(RecordingResult, runner.run(unittest.TestSuite(instances)))
        response["tests"] = [{"id": stable, **result.outcomes.get(runtime_id, {"status": "not_run"}),
                              **result.phases.get(runtime_id, {})} for runtime_id, stable in runtime_ids.items()]
        response.update(status="passed" if result.wasSuccessful() and all(test["status"] == "passed" for test in response["tests"]) else "failed",
                        setup_seconds=class_phases["setup_seconds"], cleanup_seconds=class_phases["cleanup_seconds"],
                        run_seconds=sum(phases["run_seconds"] for phases in result.phases.values()),
                        preparation_seconds=preparation_seconds, first_negative_seconds=result.negative_at,
                        fixture_errors=[value for key, value in result.outcomes.items() if key not in runtime_ids],
                        cleanup_status="completed")
        response.pop("reason", None)
    except BaseException:
        if captured:
            recorded = captured[0]
            response["tests"] = [{"id": stable, **recorded.outcomes.get(runtime_id, {
                "status": "interrupted" if runtime_id in recorded.phases else "not_run"}),
                **recorded.phases.get(runtime_id, {})} for runtime_id, stable in runtime_ids.items()]
            response.update(setup_seconds=class_phases["setup_seconds"], cleanup_seconds=class_phases["cleanup_seconds"],
                            cleanup_status="unknown after interruption")
        response["error"] = traceback.format_exc()
        print(response["error"], file=sys.stderr)
    response["resources"] = resources.snapshot()
    response.update(schema_version=SCHEMA, duration_seconds=time.monotonic() - start, subprocess_starts=dict(counters),
                    subprocess_counter_scope="direct child launches observed by this worker only")
    dump(Path(request["result"]), response)
    return 0 if response["status"] == "passed" else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("selectors", nargs="*", help="path[::Class[.method]] or dotted module/class/method")
    parser.add_argument("--lanes", default=",".join(OFFLINE), help="comma-separated fast,fixtures,git,docker,report,browser or all")
    parser.add_argument("--workers", type=int, default=4, help="global resource-token budget (default 4; nested pools reserve their full weight)")
    parser.add_argument("--output", type=Path, help="retained sessions directory (default runs/verification)")
    parser.add_argument("--reuse", action="store_true", help="reuse exact matching successful offline class evidence")
    parser.add_argument("--list", action="store_true", help="show inventory and selected tests without imports or execution")
    parser.add_argument("--worker", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if args.worker:
        return worker(args.worker)
    root = Path.cwd()
    if args.list:
        try:
            inventory = discover(root, load_manifest(root))
            lanes = list(LANES) if args.lanes == "all" else args.lanes.split(",")
            if set(lanes) & {"report", "browser"}:
                from devtools.verify_auxiliary import inventory as auxiliary_inventory
                inventory += auxiliary_inventory(root, lanes)
            chosen = select(inventory, lanes, args.selectors)
            print(json.dumps({"discovered_total": len(inventory), "selected_total": len(chosen), "tests": chosen}, indent=2))
            return 0
        except (ValueError, OSError, SyntaxError) as error:
            print(str(error), file=sys.stderr)
            return 2
    def terminate(_signum, _frame):
        raise KeyboardInterrupt("verification controller cancelled")
    previous = signal.signal(signal.SIGTERM, terminate)
    try:
        summary = run(root, lanes=args.lanes.split(","), selectors=args.selectors, workers=args.workers,
                      output=args.output, reuse=args.reuse)
    finally:
        signal.signal(signal.SIGTERM, previous)
    print(json.dumps({key: summary[key] for key in ("status", "coverage", "discovered_total", "selected_total",
                                                   "outcomes", "workers_started", "reused_classes", "duration_seconds", "session")}, indent=2))
    if summary.get("error"):
        print(summary["error"], file=sys.stderr)
    return 0 if summary["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
