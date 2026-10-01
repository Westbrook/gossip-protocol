"""Versioned, offline-only orchestration of exact-source BlackboxValidator jobs.

This is an opt-in diagnostic adapter, not a replacement for a frozen study
protocol. Jobs contain source and an ordered host-side oracle, never proposal
control/completion claims. Only explicitly deterministic visible observations
can reuse evidence. Final and repeatability jobs always execute independently.
"""

from __future__ import annotations

from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
import ast
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import threading
import time
from typing import Any, Callable
import uuid

from gossip_harness.blackbox_validator import (
    BlackboxValidator, CHILD_ADAPTER, PROTOCOL as BLACKBOX_PROTOCOL,
    SUPERVISOR_ADAPTER, _canonical, json_equal,
)
from gossip_harness.sandbox import DockerValidator
from gossip_harness.sustained_experiment import checked_evaluate


from devtools.telemetry import IOAccounting, ProcessTelemetry


SESSION_PROTOCOL = "validation-session-v1"
ADAPTER_VERSION = "blackbox-batch-v1"
PURPOSES = frozenset({"visible", "final", "repeatability"})
_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,79}\Z")
_ROOT = Path(__file__).resolve().parents[1]
_ADAPTER_DIGEST = hashlib.sha256(_canonical({
    "supervisor.py": SUPERVISOR_ADAPTER, "child.py": CHILD_ADAPTER,
}).encode()).hexdigest()


def digest(value: object) -> str:
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


def _write(path: Path, value: object, accounting: IOAccounting | None = None) -> None:
    """Publish a complete local JSON receipt without a partial reader view."""
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    try:
        contents = (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n").encode("utf-8")
        if accounting is None:
            temporary.write_bytes(contents)
        else:
            accounting.write_bytes(temporary, contents, "retained_receipts")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def support_digests(root: Path | None = None) -> dict[str, str]:
    """Hash the evaluator's forward import closure, including package initializers.

    Reverse dependencies such as unrelated studies do not affect this adapter.
    Function-local imports are included conservatively. Dynamic Python imports
    in this closure are unsupported and fail closed; introducing one requires
    an explicit extension of this versioned dependency resolver.
    """
    root = _ROOT if root is None else Path(root)
    pending = ["devtools.validation_session", "devtools.validate_batch",
               "gossip_harness.blackbox_validator", "gossip_harness.sandbox",
               "gossip_harness.sustained_experiment"]
    digests: dict[str, str] = {}

    def module_path(module: str) -> Path:
        path = root.joinpath(*module.split("."))
        file = path.with_suffix(".py")
        return file if file.is_file() else path / "__init__.py"

    while pending:
        module = pending.pop()
        path = module_path(module)
        relative = str(path.relative_to(root))
        if relative in digests:
            continue
        source = path.read_bytes()
        digests[relative] = hashlib.sha256(source).hexdigest()
        parsed = ast.parse(source, filename=relative)
        package = path.parent.relative_to(root).parts
        for count in range(1, len(package) + 1):
            initializer = root.joinpath(*package[:count], "__init__.py")
            if initializer.is_file():
                pending.append(".".join(package[:count]))
        dynamic_aliases = {"__import__"}
        for node in ast.walk(parsed):
            if isinstance(node, ast.ImportFrom) and node.module == "importlib":
                dynamic_aliases.update(alias.asname or alias.name for alias in node.names
                                       if alias.name == "import_module")
        for node in ast.walk(parsed):
            if isinstance(node, ast.Call) and (
                    isinstance(node.func, ast.Name) and node.func.id in dynamic_aliases
                    or isinstance(node.func, ast.Attribute) and node.func.attr == "import_module"):
                raise ValueError("Dynamic imports require an explicit support contract: " + relative)
            imported: list[str] = []
            if isinstance(node, ast.Import):
                imported.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                prefix = list(package[:len(package) - node.level + 1]) if node.level else []
                base = ".".join(prefix + ([node.module] if node.module else []))
                imported.append(base)
                imported.extend(base + "." + alias.name for alias in node.names
                                if module_path(base + "." + alias.name).is_file())
            for imported_module in imported:
                if imported_module.split(".")[0] in {"gossip_harness", "devtools"}:
                    pending.append(imported_module)
    return dict(sorted(digests.items()))


def docker_runtime_identity() -> str:
    """Bind persisted evidence to the actual daemon/kernel, not a context name."""
    result = subprocess.run(
        ["docker", "info", "--format",
         "{{.ID}}|{{.ServerVersion}}|{{.OSType}}|{{.Architecture}}|{{.KernelVersion}}"],
        capture_output=True, text=True, timeout=15, check=False,
        env=DockerValidator._environment(),
    )
    identity = result.stdout.strip()
    if result.returncode or len(identity.split("|")) != 5 or not all(identity.split("|")):
        raise RuntimeError("Docker runtime identity could not be established")
    return identity


@dataclass(frozen=True)
class ResourceBudget:
    """Partition shared CPU/memory across explicitly declared outer batches.

    ``workers`` is the requested ceiling for THIS batch. CPU and memory are
    aggregate ceilings shared by all ``outer_parallelism`` concurrent batches.
    Callers must declare the actual outer parallelism; this is not a global
    cross-process semaphore or an automatic host resource detector.
    """

    workers: int = 2
    cpus: int = 2
    memory_mib: int = 512
    outer_parallelism: int = 1

    def capacity(self) -> int:
        if any(type(value) is not int or value < 1 for value in (
                self.workers, self.cpus, self.memory_mib, self.outer_parallelism)):
            raise ValueError("Resource budget values must be positive integers")
        capacity = min(self.workers, self.cpus // self.outer_parallelism,
                       self.memory_mib // (256 * self.outer_parallelism))
        if capacity < 1:
            raise ValueError("Each outer batch needs at least one CPU and 256 MiB")
        return capacity


@dataclass(frozen=True)
class ValidationJob:
    name: str
    files: dict[str, str]
    cases: list[dict]
    purpose: str = "visible"
    deterministic: bool = False
    seed: int | str | None = None
    protocol: str = "offline-diagnostic-v1"
    reason: str = ""

    def snapshot(self) -> dict:
        if not isinstance(self.name, str) or not _NAME.fullmatch(self.name):
            raise ValueError("Job names must be safe unique short identifiers")
        if self.purpose not in PURPOSES or type(self.deterministic) is not bool:
            raise ValueError("Invalid observation purpose or deterministic declaration")
        if not isinstance(self.protocol, str) or not self.protocol.strip():
            raise ValueError("An explicit job protocol is required")
        if type(self.seed) not in (int, str, type(None)):
            raise ValueError("Seed must be an integer, string, or null")
        if not isinstance(self.reason, str) or (self.purpose != "visible" and not self.reason.strip()):
            raise ValueError("Independent observations require a reason")
        files, cases, _ = BlackboxValidator._inputs(self.files, self.cases)
        # Reject unknown fields instead of silently changing an oracle contract.
        if any(set(case) - {"input", "expected", "id", "requirement"} for case in self.cases):
            raise ValueError("Unknown case fields are not part of this adapter protocol")
        return dict(name=self.name, files=files, cases=cases, purpose=self.purpose,
                    deterministic=self.deterministic, seed=self.seed,
                    protocol=self.protocol, reason=self.reason)


class _ReceiptValidator:
    def __init__(self, receipt):
        self.receipt = receipt

    def evaluate(self, files, cases):
        return self.receipt


def verify_receipt(receipt: dict, job: dict, binding: dict) -> dict:
    """Reconcile reuse and physical receipts against exact host-side answers."""
    checked_evaluate(_ReceiptValidator(receipt), job["files"], job["cases"])
    required = {
        "schema_version": 1, "protocol": BLACKBOX_PROTOCOL,
        "image_id": binding["image"], "adapter_sha256": binding["adapter_sha256"],
        "timeout_seconds": binding["timeout_seconds"],
        "case_timeout_seconds": binding["case_timeout_seconds"],
        "case_count": len(job["cases"]), "exit_code": 0,
        "timed_out": False, "output_truncated": False, "input_delivery_failed": False,
    }
    if any(not json_equal(receipt.get(key), expected) for key, expected in required.items()):
        raise ValueError("Receipt runtime contract does not match")
    for outcome, case in zip(receipt["outcomes"], job["cases"]):
        status = outcome.get("status")
        if status in ("passed", "wrong_answer"):
            if ("actual" not in outcome
                    or outcome["passed"] != json_equal(outcome["actual"], case["expected"])
                    or status != ("passed" if outcome["passed"] else "wrong_answer")):
                raise ValueError("Receipt does not reconcile with the host oracle")
        elif status not in ("error", "timeout", "output_error", "output_limit", "invalid_output"):
            raise ValueError("Receipt contains an unknown case status")
        elif outcome["passed"] or "actual" in outcome:
            raise ValueError("Error outcome cannot pass")
        for label in ("id", "requirement"):
            if (label in case) != (label in outcome) or outcome.get(label) != case.get(label):
                raise ValueError("Receipt case labels do not match the suite")
    return receipt


class ValidationSession:
    """Bounded scheduler; one validator per physical job and one healthy preflight.

    Cancellation stops queued launches and drains started validators, whose own
    timeouts and cleanup remain authoritative. The optional proof directory is
    trusted local evidence, not a signed attestation or an untrusted import API.
    A session must not be shared by concurrently calling controllers. Optional
    external_contract identity and finalization_guard are supplied together:
    the identity binds every proof, and the guard runs before admission and
    before final results/cache publication to cover caller-owned dependencies.
    """

    def __init__(self, output: Path, *, image: str, timeout_seconds: float = 60,
                 case_timeout_seconds: float = 2, adapter_version: str = ADAPTER_VERSION,
                 budget: ResourceBudget = ResourceBudget(), cache: Path | None = None,
                 validator_factory: Callable = BlackboxValidator,
                 runtime_identity: Callable[[], str] = docker_runtime_identity,
                 finalization_guard: Callable[[], bool] | None = None,
                 external_contract: dict | None = None):
        if not isinstance(adapter_version, str) or not adapter_version.strip():
            raise ValueError("Adapter version must be explicit")
        # Enforce the actual sandbox contract even with a test factory.
        BlackboxValidator(image, timeout_seconds=timeout_seconds,
                          case_timeout_seconds=case_timeout_seconds)
        for value in (timeout_seconds, case_timeout_seconds):
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ValueError("Timeouts must be finite and positive")
        if (finalization_guard is None) != (external_contract is None):
            raise ValueError("An external contract identity and finalization guard are required together")
        if finalization_guard is not None and not callable(finalization_guard):
            raise ValueError("The external contract guard must be callable")
        if external_contract is not None and (not isinstance(external_contract, dict) or not external_contract):
            raise ValueError("The external contract identity must be a nonempty mapping")
        external_snapshot = json.loads(_canonical(external_contract)) if external_contract is not None else None
        self.finalization_guard = finalization_guard
        self.capacity = budget.capacity()
        self.output = Path(output)
        self.output.mkdir(parents=True, exist_ok=False)
        self.cache = Path(cache) if cache is not None else None
        if self.cache:
            self.cache.mkdir(parents=True, exist_ok=True)
        self.session_id = uuid.uuid4().hex
        self.factory = validator_factory
        self.runtime_identity = runtime_identity
        self.image = image
        self.timeout_seconds = timeout_seconds
        self.case_timeout_seconds = case_timeout_seconds
        self.base_binding = dict(
            session_protocol=SESSION_PROTOCOL, adapter_version=adapter_version,
            adapter_sha256=_ADAPTER_DIGEST, support_sha256=support_digests(),
            image=image, timeout_seconds=timeout_seconds,
            case_timeout_seconds=case_timeout_seconds,
            resources={"cpus": 1, "memory_mib": 256, "pids": 64},
            environment_sha256=digest(DockerValidator._environment()),
            host_python=sys.version, host_platform=platform.platform(),
            command=["python", "-I", "/checks/supervisor.py"],
        )
        if external_snapshot is not None:
            self.base_binding["external_contract"] = external_snapshot
        self.budget = dict(workers=budget.workers, cpus=budget.cpus,
                           memory_mib=budget.memory_mib,
                           outer_parallelism=budget.outer_parallelism, capacity=self.capacity)
        self._proofs: dict[str, str] = {}
        self._pending_proofs: dict[str, dict] = {}
        self._healthy = False
        self._runtime: str | None = None
        self._cancelled = threading.Event()
        self._run_lock = threading.Lock()
        self._results: list[dict] = []
        self._preflights: list[dict] = []
        self._started = time.monotonic()
        self._resources = ProcessTelemetry("validation session host process after preparation; excludes Docker daemon/container CPU")
        self._io = IOAccounting("instrumented validation receipt reads/writes and controller admission; snapshot precedes current summary write")
        self._save_summary()

    def cancel(self) -> None:
        self._cancelled.set()

    def _new_validator(self):
        return self.factory(self.image, timeout_seconds=self.timeout_seconds,
                            case_timeout_seconds=self.case_timeout_seconds)

    def _preflight(self) -> bool:
        if self._healthy:
            try:
                if self.runtime_identity() == self._runtime:
                    return True
            except Exception:
                pass
            # A changed/unavailable daemon invalidates only the health proof;
            # cached observations still remain bound to their old identity.
            self._healthy = False
            self._proofs.clear()
        started = time.monotonic()
        try:
            okay, detail = self._new_validator().preflight()
            runtime = self.runtime_identity() if okay else None
            if okay and (not isinstance(runtime, str) or not runtime):
                raise ValueError("Missing runtime identity")
            self._runtime = runtime
        except Exception as error:
            okay, detail = False, "Preflight failed: " + type(error).__name__
        self._healthy = bool(okay)
        self._preflights.append(dict(passed=self._healthy, detail=detail,
                                     seconds=time.monotonic() - started))
        return self._healthy

    def _binding(self, job: dict) -> dict:
        return dict(self.base_binding, runtime_identity=self._runtime,
                    source_sha256=digest(job["files"]), suite_sha256=digest(job["cases"]),
                    purpose=job["purpose"], deterministic=job["deterministic"],
                    seed=job["seed"], protocol=job["protocol"])

    @staticmethod
    def _eligible(job: dict) -> bool:
        return job["purpose"] == "visible" and job["deterministic"]

    def _read_proof(self, job: dict, binding: dict, directory: Path) -> tuple[dict | None, str | None, bool]:
        key = digest(binding)
        raw = self._proofs.get(key)
        kind = "in_session"
        path = self.cache / (key + ".json") if self.cache else None
        if raw is None and path is not None and path.is_file():
            try:
                raw, kind = self._io.read_bytes(path, "persisted_proofs").decode("utf-8"), "persisted"
            except (OSError, UnicodeError):
                return None, None, True
        if raw is None:
            return None, None, False
        try:
            proof = json.loads(raw)
            if (proof["schema"] != SESSION_PROTOCOL or proof["key_sha256"] != key
                    or proof["binding"] != binding or proof["physical"] is not True
                    or not isinstance(proof["execution_id"], str) or not proof["execution_id"]
                    or not isinstance(proof["origin_session_id"], str) or not proof["origin_session_id"]
                    or not isinstance(proof["artifact_path"], str) or not proof["artifact_path"]
                    or proof["receipt_sha256"] != digest(proof["receipt"])):
                raise ValueError("Proof binding does not match")
            verify_receipt(proof["receipt"], job, binding)
            return proof, kind, False
        except (ValueError, TypeError, KeyError, RuntimeError, AttributeError):
            self._io.write_bytes(directory / "rejected-cache.txt", raw.encode("utf-8"), "rejected_proofs")
            self._proofs.pop(key, None)
            return None, None, True

    def _result(self, item: dict, *, status: str, physical: bool = False,
                receipt: dict | None = None, execution_id: str | None = None,
                reuse: dict | None = None, error: str | None = None,
                execution_seconds: float = 0) -> dict:
        return dict(schema=SESSION_PROTOCOL, session_id=self.session_id,
                    logical_index=item["index"], name=item["job"]["name"],
                    purpose=item["job"]["purpose"], reason=item["job"]["reason"],
                    key_sha256=digest(item["binding"]), binding=item["binding"],
                    physical=physical, execution_id=execution_id, reuse=reuse,
                    status=status, passed=bool(receipt and receipt.get("passed") is True
                                               and status == "passed"),
                    receipt=receipt, error=error,
                    cache_rejected=item.get("cache_rejected", False),
                    timing={"queue_seconds": max(0.0, time.monotonic() - item["queued"]
                                                  - execution_seconds),
                            "validator_wall_seconds": execution_seconds},
                    artifact_path=str(item["directory"]))

    def _execute(self, item: dict) -> dict:
        if self._cancelled.is_set():
            return self._result(item, status="cancelled")
        started = time.monotonic()
        execution_id = uuid.uuid4().hex
        validator = None
        receipt = None
        physical = False
        try:
            if digest(DockerValidator._environment()) != self.base_binding["environment_sha256"]:
                raise RuntimeError("Runtime environment changed during session")
            validator = self._new_validator()
            physical = True
            receipt = checked_evaluate(validator, item["job"]["files"], item["job"]["cases"])
            verify_receipt(receipt, item["job"], item["binding"])
            return self._result(item, status=receipt["status"], physical=True,
                                receipt=receipt, execution_id=execution_id,
                                execution_seconds=time.monotonic() - started)
        except Exception as error:
            if receipt is None and validator is not None:
                receipt = getattr(validator, "last_receipt", None)
            return self._result(item, status="infrastructure_failed", physical=physical,
                                receipt=receipt, execution_id=execution_id if physical else None,
                                error=type(error).__name__,
                                execution_seconds=time.monotonic() - started)

    def _remember(self, item: dict, result: dict) -> dict:
        proof = dict(schema=SESSION_PROTOCOL, key_sha256=result["key_sha256"],
                     binding=result["binding"], physical=True,
                     execution_id=result["execution_id"], receipt=result["receipt"],
                     receipt_sha256=digest(result["receipt"]),
                     origin_session_id=self.session_id, artifact_path=result["artifact_path"])
        self._proofs[result["key_sha256"]] = _canonical(proof)
        self._pending_proofs[result["key_sha256"]] = proof
        return proof

    def _reused(self, item: dict, proof: dict, kind: str) -> dict:
        # Each logical receipt is detached from mutable callers and other jobs.
        proof = json.loads(_canonical(proof))
        return self._result(item, status=proof["receipt"]["status"], receipt=proof["receipt"],
                            execution_id=proof["execution_id"],
                            reuse=dict(kind=kind, origin_session_id=proof["origin_session_id"],
                                       artifact_path=proof["artifact_path"],
                                       execution_id=proof["execution_id"]))

    def _retain(self, item: dict, result: dict) -> None:
        _write(item["directory"] / "result.json", result, self._io)
        self._results.append(result)

    def _finalize(self, items: list[dict]) -> dict:
        """Publish reuse evidence only after a complete unchanged-input check."""
        try:
            unchanged = (support_digests() == self.base_binding["support_sha256"]
                         and digest(DockerValidator._environment()) == self.base_binding["environment_sha256"]
                         and (self._runtime is None or self.runtime_identity() == self._runtime)
                         and (self.finalization_guard is None or self.finalization_guard() is True))
        except Exception:
            unchanged = False
        if not unchanged:
            self._healthy = False
            self._proofs.clear()
            indexes = {item["index"] for item in items}
            for result in self._results:
                if result["logical_index"] in indexes and result["status"] != "cancelled":
                    result["observed_status"] = result["status"]
                    result["status"], result["passed"] = "infrastructure_failed", False
                    result["error"] = "session_inputs_changed"
                    _write(Path(result["artifact_path"]) / "result.json", result, self._io)
        elif self.cache:
            for key, proof in self._pending_proofs.items():
                _write(self.cache / (key + ".json"), proof, self._io)
        self._pending_proofs.clear()
        return self._save_summary()

    def _save_summary(self) -> dict:
        results = sorted(self._results, key=lambda result: result["logical_index"])
        counts = dict(logical_jobs=len(results),
                      physical_executions=sum(result["physical"] for result in results),
                      preflights=len(self._preflights),
                      cache_rejections=sum(result["cache_rejected"] for result in results))
        for status in ("passed", "failed", "infrastructure_failed", "cancelled"):
            counts[status] = sum(result["status"] == status for result in results)
        for kind in ("in_session", "persisted", "singleflight"):
            counts["reused_" + kind] = sum(bool(result["reuse"])
                and result["reuse"]["kind"] == kind for result in results)
        summary = dict(schema=SESSION_PROTOCOL, session_id=self.session_id,
                       resource_budget=self.budget, runtime_contract=self.base_binding,
                       preflights=self._preflights, counts=counts,
                       cancelled=self._cancelled.is_set(),
                       resources=self._resources.snapshot(), io_accounting=self._io.snapshot(),
                       wall_seconds=time.monotonic() - self._started, results=results,
                       measurement_notes={
                           "physical_executions": "Validator evaluate calls, including failed launch attempts",
                           "validator_wall_seconds": "Wall time including staging, runtime and verified cleanup; phases are not separately measured",
                           "queue_seconds": "Elapsed after input retention until this observation starts or reuses evidence",
                       },
                       passed=bool(results) and all(result["status"] == "passed" for result in results))
        _write(self.output / "session.json", summary, self._io)
        return json.loads(_canonical(summary))

    def run(self, jobs: list[ValidationJob]) -> dict:
        with self._io.acquire(self._run_lock, "controller_admission", blocking=False) as acquired:
            if not acquired:
                raise RuntimeError("Concurrent controllers must share one run and resource budget")
            return self._run(jobs)

    def _run(self, jobs: list[ValidationJob]) -> dict:
        snapshots = [job.snapshot() for job in jobs]
        if not snapshots or len({job["name"] for job in snapshots}) != len(snapshots):
            raise ValueError("A batch needs nonempty, uniquely named jobs")
        if support_digests() != self.base_binding["support_sha256"]:
            raise RuntimeError("Checker support changed; create a new session")
        if digest(DockerValidator._environment()) != self.base_binding["environment_sha256"]:
            raise RuntimeError("Runtime environment changed; create a new session")
        if self.finalization_guard is not None and self.finalization_guard() is not True:
            raise RuntimeError("External validation contract changed; create a new session")
        self._pending_proofs.clear()
        ready = not self._cancelled.is_set() and self._preflight()
        items: list[dict[str, Any]] = []
        offset = len(self._results)
        for index, job in enumerate(snapshots, offset):
            directory = self.output / f"{index:05d}-{job['name']}"
            directory.mkdir()
            binding = self._binding(job)
            _write(directory / "input.json", dict(job=job, binding=binding), self._io)
            items.append(dict(index=index, job=job, directory=directory, binding=binding,
                              queued=time.monotonic()))
        if not ready:
            for item in items:
                self._retain(item, self._result(item, status="cancelled" if self._cancelled.is_set()
                                                else "infrastructure_failed", error="preflight"))
            return self._finalize(items)
        groups: dict[str, list[dict]] = {}
        for item in items:
            job = item["job"]
            eligible = self._eligible(job)
            if eligible:
                proof, kind, rejected = self._read_proof(job, item["binding"], item["directory"])
                item["cache_rejected"] = rejected
                if proof is not None and kind is not None:
                    self._retain(item, self._reused(item, proof, kind))
                    continue
            key = digest(item["binding"]) if eligible else str(item["index"])
            groups.setdefault(key, []).append(item)
        queued = deque(groups.values())
        active: dict[Future[dict], list[dict]] = {}
        with ThreadPoolExecutor(max_workers=self.capacity, thread_name_prefix="validation") as pool:
            while queued or active:
                try:
                    while queued and len(active) < self.capacity and not self._cancelled.is_set():
                        if not self._healthy:
                            previous_runtime = self._runtime
                            if not self._preflight() or self._runtime != previous_runtime:
                                while queued:
                                    for item in queued.popleft():
                                        self._retain(item, self._result(item,
                                            status="infrastructure_failed", error="runtime_health_changed"))
                                break
                        group = queued.popleft()
                        active[pool.submit(self._execute, group[0])] = group
                    if self._cancelled.is_set():
                        while queued:
                            for item in queued.popleft():
                                self._retain(item, self._result(item, status="cancelled"))
                    if not active:
                        break
                    finished, _ = wait(active, return_when=FIRST_COMPLETED)
                    for future in sorted(finished, key=lambda entry: active[entry][0]["index"]):
                        group = active.pop(future)
                        item, remaining = group[0], group[1:]
                        result = future.result()
                        self._retain(item, result)
                        if result["status"] == "infrastructure_failed":
                            self._healthy = False
                        if result["status"] in ("passed", "failed") and self._eligible(item["job"]):
                            proof = self._remember(item, result)
                            for sibling in remaining:
                                self._retain(sibling, self._result(sibling, status="cancelled")
                                             if self._cancelled.is_set()
                                             else self._reused(sibling, proof, "singleflight"))
                        elif remaining:
                            # Infrastructure failures are never reused, even in a
                            # singleflight group. Every remaining request retries
                            # only its own previously unstarted observation.
                            queued.append(remaining)
                except KeyboardInterrupt:
                    self.cancel()
        return self._finalize(items)
