"""Reproduce the small, no-provider validation efficiency measurement.

Run after the project's static/offline gates, on a quiet host with the pinned
Docker image already present. Six cold batches alternate capacity 1/2, followed
by one warm batch. Each contains two identical visible jobs and one independent
final observation. No tool installation, image pull, daemon start/stop, or paid
request is performed. Existing outputs are refused and failures are retained.

``--seeded-failure syntax`` or ``--seeded-failure types`` instead runs a separate
cheap negative static gate, in a retained disposable source tree without Docker.
An expected failure makes this diagnostic command succeed; its receipt still
reports the gate failure rather than converting it into passing project code.
"""
from __future__ import annotations

import argparse
from collections import Counter
from collections.abc import Callable
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import signal
import sys
import threading
import time
import tomllib
from types import FrameType
from typing import Any

from devtools.static_checks import run_checks
from devtools.telemetry import ProcessTelemetry, distribution
from devtools.validation_session import ResourceBudget, ValidationJob, ValidationSession, digest, support_digests


DEFAULT_IMAGE = "sha256:f77ac9e44ae96ef2c90b8053ea08c31f8be030f824196b0ae4db6d462c84e51f"
PROTOCOL = "verification-validation-benchmark-v1"
ROOT = Path(__file__).resolve().parents[1]


def fixture_jobs() -> list[ValidationJob]:
    """Exact candidate/oracle bytes used by the original one-case measurement."""
    common: dict[str, Any] = dict(files={"solution.py": "def solve(payload):\n    return sum(payload)\n"},
                  cases=[{"id": "sum", "input": [2, 3], "expected": 5}],
                  deterministic=True, seed=1, protocol="offline-sum-diagnostic-v1")
    return [ValidationJob("visible-first", **common),
            ValidationJob("visible-identical", **common),
            ValidationJob("final-independent", purpose="final",
                          reason="Independent final observation remains a fresh physical execution", **common)]


def source_binding() -> dict[str, str]:
    bound = support_digests()
    for name in ("devtools/benchmark_validation.py", "devtools/static_checks.py", "pyproject.toml"):
        bound[name] = hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
    return dict(sorted(bound.items()))


def _save(path: Path, value: dict) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n")
    temporary.replace(path)


class _Startups:
    """One gated CPython audit observer; never monkey-patches subprocess APIs."""
    def __init__(self) -> None:
        self.enabled = True
        self.counts: Counter[str] = Counter()
        self.lock = threading.Lock()
        sys.addaudithook(self._audit)

    def _audit(self, event: str, args: tuple) -> None:
        if not self.enabled or event != "subprocess.Popen":
            return
        executable = Path(os.fsdecode(args[0])).name.lower()
        category = "docker" if executable == "docker" else "git" if executable == "git" else (
            "python" if "python" in executable else "browser" if "chrom" in executable else "other")
        with self.lock:
            self.counts[category] += 1

    def snapshot(self) -> Counter[str]:
        with self.lock:
            return self.counts.copy()


class _ObservedSession(ValidationSession):
    def __init__(self, *args: Any, on_negative: Callable[[], None], **kwargs: Any) -> None:
        self.on_negative = on_negative
        super().__init__(*args, **kwargs)

    def _retain(self, item: dict, result: dict) -> None:
        if result["status"] != "passed":
            self.on_negative()
        super()._retain(item, result)


def _check_observations(result: dict, *, warm: bool, expected_visible: str | None,
                        previous_finals: set[str]) -> str:
    counts = result["counts"]
    expected = {"logical_jobs": 3, "physical_executions": 1 if warm else 2,
                "preflights": 1, "reused_persisted": 2 if warm else 0,
                "reused_singleflight": 0 if warm else 1,
                "failed": 0, "infrastructure_failed": 0, "cancelled": 0}
    if not result["passed"] or any(counts.get(key) != value for key, value in expected.items()):
        raise ValueError("Required logical/physical observation counts differ from benchmark contract")
    jobs, observations = fixture_jobs(), result["results"]
    if [row["name"] for row in observations] != [job.name for job in jobs]:
        raise ValueError("Ordered observation set differs from benchmark contract")
    for job, row in zip(jobs, observations):
        if (row["purpose"] != job.purpose or row["status"] != "passed"
                or row["binding"]["source_sha256"] != digest(job.files)
                or row["binding"]["suite_sha256"] != digest(job.cases)):
            raise ValueError("Observation source/oracle/purpose differs from benchmark fixture")
    visible = observations[0]["execution_id"]
    final = observations[2]
    if not visible or visible != observations[1]["execution_id"]:
        raise ValueError("Identical visible jobs did not reference the same physical execution")
    if warm and visible != expected_visible:
        raise ValueError("Warm visible evidence does not reference its cold original")
    if (final["physical"] is not True or final["reuse"] is not None
            or not final["execution_id"] or final["execution_id"] == visible
            or final["execution_id"] in previous_finals):
        raise ValueError("Independent final observation was reused or lacks physical provenance")
    previous_finals.add(final["execution_id"])
    return visible


def run_benchmark(output: Path, *, image: str = DEFAULT_IMAGE,
                  session_factory: Callable[..., Any] = _ObservedSession) -> dict:
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", image):
        raise ValueError("A full locally provisioned immutable image ID is required")
    output = output.absolute()
    output.mkdir(parents=True, exist_ok=False)
    (output / "caches").mkdir()
    started = time.monotonic()
    resources = ProcessTelemetry("complete benchmark controller; excludes remote Docker/container CPU")
    source = source_binding()
    contract = dict(protocol=PROTOCOL, image=image, repeats_per_capacity=3,
                    cold_capacities=[1, 2, 1, 2, 1, 2], warm_capacity=2,
                    timeout_seconds=60, case_timeout_seconds=2,
                    jobs=[job.snapshot() for job in fixture_jobs()],
                    source_sha256=source,
                    host=dict(platform=platform.platform(), python=sys.version, cpu_count=os.cpu_count()),
                    seed_meaning="Fixed fixture identity; not a random generator or evidence of determinism")
    _save(output / "contract.json", contract)
    startups = _Startups()
    summary: dict[str, Any] = dict(protocol=PROTOCOL, passed=False, status="running",
        contract_sha256=digest(contract), contract=str(output / "contract.json"), cold_runs=[], warm_run=None,
        scope="Three alternating samples per capacity of one fixed local case; no project-wide or causal claim",
        limitations=["No p95 estimate from three samples", "RSS is process-lifetime, not an interval/tree peak",
                     "Host measurements exclude Docker daemon/container CPU and uninstrumented I/O",
                     "Subprocess counts observe direct launches only", "External host load is not controlled"],
        first_negative_seconds=None)
    previous_finals: set[str] = set()
    expected_visible: str | None = None
    runtime_contract: dict | None = None
    active: Any = None
    handlers = {}

    def negative() -> None:
        if summary["first_negative_seconds"] is None:
            summary["first_negative_seconds"] = time.monotonic() - started

    def cancel(_signum: int, _frame: FrameType | None) -> None:
        negative()
        if active is not None:
            active.cancel()
        raise KeyboardInterrupt("benchmark cancelled; completed evidence retained")

    try:
        if threading.current_thread() is threading.main_thread():
            for signum in (signal.SIGINT, signal.SIGTERM):
                handlers[signum] = signal.signal(signum, cancel)
        for index, capacity in enumerate([1, 2, 1, 2, 1, 2, 2]):
            if source_binding() != source:
                raise ValueError("Benchmark support changed before the next batch")
            warm = index == 6
            name = "warm-workers-2" if warm else f"workers-{capacity}-repeat-{index // 2}"
            cache = output / "caches" / ("workers-2-repeat-2" if warm else name)
            before_starts = startups.snapshot()
            began = time.monotonic()
            row: dict[str, Any] = dict(name=name, workers=capacity, cache=str(cache), passed=False,
                                      receipt=str(output / name / "session.json"))
            if warm:
                summary["warm_run"] = row
            else:
                summary["cold_runs"].append(row)
            active = session_factory(output / name, image=image, timeout_seconds=60, case_timeout_seconds=2,
                cache=cache, budget=ResourceBudget(workers=capacity, cpus=capacity, memory_mib=256 * capacity),
                on_negative=negative)
            result = active.run(fixture_jobs())
            active = None
            row.update(elapsed_seconds=time.monotonic() - began, counts=result["counts"],
                       candidate_passed=result["passed"], resources=result.get("resources"),
                       io_accounting=result.get("io_accounting"),
                       direct_subprocess_starts=dict(startups.snapshot() - before_starts),
                       receipt_sha256=hashlib.sha256(Path(row["receipt"]).read_bytes()).hexdigest())
            current_runtime = dict(session=result["runtime_contract"],
                                   daemon=result["results"][0]["binding"]["runtime_identity"])
            if runtime_contract is None:
                runtime_contract = current_runtime
            elif current_runtime != runtime_contract:
                raise ValueError("Runtime contract changed between benchmark samples")
            expected_visible = _check_observations(result, warm=warm, expected_visible=expected_visible,
                                                  previous_finals=previous_finals)
            row["passed"] = True
            _save(output / "summary.json", summary)
        if source_binding() != source:
            raise ValueError("Benchmark support changed during the final batch")
        summary.update(passed=True, status="passed", source_unchanged=True)
    except (Exception, KeyboardInterrupt) as error:
        negative()
        summary.update(status="failed", error=f"{type(error).__name__}: {error}")
    finally:
        startups.enabled = False
        for signum, previous in handlers.items():
            signal.signal(signum, previous)
        summary.update(duration_seconds=time.monotonic() - started, resources=resources.snapshot(),
                       direct_subprocess_starts=dict(startups.snapshot()),
                       startup_scope="Direct subprocess.Popen audit events in this controller only",
                       first_negative_scope="First failed retained observation or controller reconciliation, including preflight failure",
                       statistics={str(capacity): distribution([row["elapsed_seconds"] for row in summary["cold_runs"]
                           if row["workers"] == capacity and row["passed"] and "elapsed_seconds" in row])
                           for capacity in (1, 2)})
        _save(output / "summary.json", summary)
    return summary


def run_seeded_failure(output: Path, kind: str) -> dict:
    if kind not in {"syntax", "types"}:
        raise ValueError("Seeded failure must be syntax or types")
    output = output.absolute()
    output.mkdir(parents=True, exist_ok=False)
    workspace = output / "workspace"
    workspace.mkdir()
    source = 'return 1\n' if kind == "syntax" else 'value: int = "deliberately wrong"\n'
    (workspace / "sample.py").write_text(source)
    dependencies = tomllib.loads((ROOT / "pyproject.toml").read_text())["dependency-groups"]["dev"]
    configuration = ("[dependency-groups]\n" + f"dev = {json.dumps(dependencies)}\n" +
        '[tool.devtools.static]\nroots = ["sample.py"]\ntimeout_seconds = 30\n' +
        '[tool.ruff]\ncache-dir = ' + json.dumps(str(ROOT / ".cache/ruff")) + '\n' +
        '[tool.ruff.lint]\nselect = ["E9", "F63", "F7", "F82"]\n' +
        '[tool.mypy]\nfiles = ["sample.py"]\npython_version = "3.11"\n' +
        'check_untyped_defs = true\nincremental = true\ncache_dir = ' + json.dumps(str(ROOT / ".cache/mypy")) + '\n')
    (workspace / "pyproject.toml").write_text(configuration)
    startups = _Startups()
    began = time.monotonic()
    try:
        gate = run_checks(workspace)
    finally:
        startups.enabled = False
    elapsed = time.monotonic() - began
    _save(output / "static.json", gate)
    failed = [check["name"] for check in gate["checks"] if check["status"] != "passed"]
    summary = dict(protocol=PROTOCOL, diagnostic="seeded-" + kind,
                   passed=gate["status"] == "failed" and failed == [kind],
                   gate_status=gate["status"], failing_checks=failed, first_negative_seconds=elapsed,
                   first_negative_scope="Observed when the complete cheap gate returns; upper bound on individual checker failure latency",
                   candidate_executions=0, source_sha256=source_binding(),
                   fixture_sha256=digest({"sample.py": source, "pyproject.toml": configuration}),
                   direct_subprocess_starts=dict(startups.snapshot()), resources=gate.get("resources"),
                   receipt=str(output / "static.json"),
                   receipt_sha256=hashlib.sha256((output / "static.json").read_bytes()).hexdigest())
    _save(output / "summary.json", summary)
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="fresh retained benchmark directory")
    parser.add_argument("--image", default=DEFAULT_IMAGE, help="existing full local Docker image ID")
    parser.add_argument("--seeded-failure", choices=("syntax", "types"), help="separate cheap diagnostic; no Docker")
    args = parser.parse_args(argv)
    try:
        result = run_seeded_failure(args.output, args.seeded_failure) if args.seeded_failure else run_benchmark(args.output, image=args.image)
    except (ValueError, OSError) as error:
        parser.error(str(error))
    print(json.dumps({"passed": result["passed"], "receipt": str(args.output.absolute() / "summary.json")}, sort_keys=True))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
