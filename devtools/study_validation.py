"""Exact-contract bridge between versioned studies and bounded validation.

The original study modules remain unchanged. Callers preregister this adapter's
complete local source closure and resource policy before provider work. Results
are returned in request order, with source evidence distinct from proposal or
control validity. Only explicitly deterministic visible jobs may reuse proof.
"""
from __future__ import annotations

import ast
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import re
import signal
import threading
from typing import Any, Callable

from gossip_harness.blackbox_validator import BlackboxValidator
from devtools.validation_session import (
    ADAPTER_VERSION, SESSION_PROTOCOL, ResourceBudget, ValidationJob,
    ValidationSession, digest, docker_runtime_identity, support_digests,
    verify_receipt,
)

STUDY_ADAPTER_PROTOCOL = "study-validation-v2"
_ROOT = Path(__file__).resolve().parents[1]
_FIELDS = frozenset({"files", "label", "source_sha256", "cases", "purpose",
                     "reason", "deterministic", "seed"})


class StudyValidationError(RuntimeError):
    """No study decision may consume this batch; raw session evidence remains."""

    def __init__(self, message: str, *, summary: dict | None = None):
        super().__init__(message)
        self.summary = summary


def _support_closure(paths: Iterable[str | Path]) -> dict[str, str]:
    """Resolve authored local imports without importing or executing modules."""
    pending = [Path(__file__), *(_ROOT / path for path in support_digests()),
               *(Path(path) if Path(path).is_absolute() else _ROOT / path for path in paths)]
    found: dict[str, str] = {}

    def local_module(name: str) -> Path | None:
        path = _ROOT.joinpath(*name.split("."))
        for candidate in (path.with_suffix(".py"), path / "__init__.py"):
            if candidate.is_file():
                return candidate
        return None

    while pending:
        path = pending.pop()
        if path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(_ROOT):
            raise ValueError("Study support must be an existing authored project file")
        relative = path.resolve().relative_to(_ROOT).as_posix()
        if any(part in {"runs", "results", ".git", ".venv", "node_modules"}
               for part in Path(relative).parts):
            raise ValueError("Generated or historical files cannot be runtime support")
        if relative in found:
            continue
        source = path.read_bytes()
        found[relative] = hashlib.sha256(source).hexdigest()
        if path.suffix != ".py":
            continue
        parsed = ast.parse(source, filename=relative)
        package = Path(relative).parent.parts
        for count in range(1, len(package) + 1):
            initializer = _ROOT.joinpath(*package[:count], "__init__.py")
            if initializer.is_file():
                pending.append(initializer)
        dynamic_aliases = {"__import__"}
        for node in ast.walk(parsed):
            if isinstance(node, ast.ImportFrom) and node.module == "importlib":
                dynamic_aliases.update(alias.asname or alias.name for alias in node.names
                                       if alias.name == "import_module")
        for node in ast.walk(parsed):
            if isinstance(node, ast.Call) and (
                    isinstance(node.func, ast.Name) and node.func.id in dynamic_aliases
                    or isinstance(node.func, ast.Attribute) and node.func.attr == "import_module"):
                raise ValueError("Dynamic imports require an explicit study support contract: " + relative)
            names: list[str] = []
            if isinstance(node, ast.Import):
                names.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                if node.level > len(package) + 1:
                    raise ValueError("Relative import escapes project support")
                prefix = list(package[:len(package) - node.level + 1]) if node.level else []
                base = ".".join(prefix + ([node.module] if node.module else []))
                names.append(base)
                names.extend(".".join(filter(None, (base, alias.name))) for alias in node.names)
            for name in names:
                dependency = local_module(name) if name else None
                if dependency is not None:
                    pending.append(dependency)
    return dict(sorted(found.items()))


def study_validation_contract(*, image: str, protocol: str,
                              budget: ResourceBudget = ResourceBudget(),
                              timeout_seconds: float = 60,
                              case_timeout_seconds: float = 2,
                              deterministic_visible: bool = False,
                              support_paths: Iterable[str | Path] = ()) -> dict:
    """Build the source/resource contract without creating output or preflight.

    ``support_sha256`` includes transitive local Python imports and package
    initializers, in project-relative form, suitable for a frozen source bundle.
    Per-job bindings additionally include the actual daemon, host environment,
    exact source, ordered oracle, seed and purpose; result provenance records the
    independent-observation reason.
    """
    if not isinstance(protocol, str) or not protocol.strip():
        raise ValueError("The versioned study protocol must be explicit")
    if type(deterministic_visible) is not bool:
        raise ValueError("Visible determinism must be an explicit boolean")
    # Constructor validation is pure: it checks immutable image/limit syntax.
    BlackboxValidator(image, timeout_seconds=timeout_seconds,
                      case_timeout_seconds=case_timeout_seconds)
    capacity = budget.capacity()
    return dict(
        protocol=STUDY_ADAPTER_PROTOCOL, study_protocol=protocol,
        session_protocol=SESSION_PROTOCOL, evaluator_adapter=ADAPTER_VERSION,
        image=image, timeout_seconds=timeout_seconds,
        case_timeout_seconds=case_timeout_seconds,
        deterministic_visible=deterministic_visible,
        resource_budget=dict(workers=budget.workers, cpus=budget.cpus,
                             memory_mib=budget.memory_mib,
                             outer_parallelism=budget.outer_parallelism, capacity=capacity),
        per_validator_resources=dict(cpus=1, memory_mib=256, pids=64),
        independent_purposes=["final", "repeatability"],
        observation_policy="Visible reuse requires explicit determinism; final and repeatability execute physically with a reason",
        reduction="request order; fail closed on incomplete or infrastructure results",
        support_sha256=_support_closure(support_paths),
    )


study_contract = study_validation_contract


@contextmanager
def study_signals(session: StudyValidationSession) -> Iterator[None]:
    """Cancel queued validation on termination and restore previous handlers.

    Started validators drain through their normal timeout/cleanup path. Callers
    must also call ``raise_if_cancelled`` before provider work or reservations.
    This is a main-thread controller context, not a process-wide background hook.
    """
    if threading.current_thread() is not threading.main_thread():
        raise ValueError("Study signal ownership requires the main controller thread")
    signals = [signal.SIGINT, signal.SIGTERM]
    if hasattr(signal, "SIGHUP"):
        signals.append(signal.SIGHUP)
    previous = {number: signal.getsignal(number) for number in signals}
    def cancel(number, frame):
        session.cancel()
    try:
        for number in signals:
            signal.signal(number, cancel)
        yield
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)
    session.raise_if_cancelled()


class StudyValidationSession:
    """One controller owns this session; nested capacity is declared explicitly."""

    def __init__(self, output: Path, *, image: str, protocol: str,
                 budget: ResourceBudget = ResourceBudget(), timeout_seconds: float = 60,
                 case_timeout_seconds: float = 2, deterministic_visible: bool = False,
                 cache: Path | None = None, support_paths: Iterable[str | Path] = (),
                 validator_factory: Callable = BlackboxValidator,
                 runtime_identity: Callable[[], str] = docker_runtime_identity):
        self._support_paths = tuple(support_paths)
        self._contract_options: dict[str, Any] = dict(image=image, protocol=protocol, budget=budget,
            timeout_seconds=timeout_seconds, case_timeout_seconds=case_timeout_seconds,
            deterministic_visible=deterministic_visible, support_paths=self._support_paths)
        self.contract = study_validation_contract(**self._contract_options)
        self.contract_sha256 = digest(self.contract)
        self.protocol = protocol
        self._count = 0
        self._stopped = False
        self.deterministic_visible = deterministic_visible
        self.session = ValidationSession(output, image=image, budget=budget,
            timeout_seconds=timeout_seconds, case_timeout_seconds=case_timeout_seconds,
            cache=cache, validator_factory=validator_factory, runtime_identity=runtime_identity,
            finalization_guard=self._support_unchanged,
            external_contract=dict(protocol=STUDY_ADAPTER_PROTOCOL,
                                   study_contract_sha256=self.contract_sha256))
        self._save_contract()

    def _save_contract(self) -> None:
        (self.session.output / "study-contract.json").write_text(
            json.dumps(self.contract, indent=2, sort_keys=True, allow_nan=False) + "\n",
            encoding="utf-8")

    def _support_unchanged(self) -> bool:
        return (digest(self.contract) == self.contract_sha256
                and study_validation_contract(**self._contract_options) == self.contract)

    def _assert_support(self) -> None:
        if self._stopped:
            raise StudyValidationError("This study validation session stopped after an invalid batch")
        if not self._support_unchanged():
            raise StudyValidationError("Study validation contract changed; create a new qualified session")

    def preflight(self) -> tuple[bool, str]:
        """Optional health gate before provider work; execution reuses its proof."""
        self.raise_if_cancelled()
        self._assert_support()
        passed = self.session._preflight()
        self._assert_support()
        self.session._save_summary()
        detail = self.session._preflights[-1]["detail"] if self.session._preflights else "No preflight"
        return passed, str(detail)

    def summary(self) -> dict:
        """Read the retained, detached cumulative session summary."""
        value = json.loads((self.session.output / "session.json").read_text(encoding="utf-8"))
        return dict(value, cancelled=self.cancelled, study_adapter_protocol=STUDY_ADAPTER_PROTOCOL,
                    study_contract_sha256=self.contract_sha256)

    @property
    def cancelled(self) -> bool:
        return self.session._cancelled.is_set()

    def raise_if_cancelled(self) -> None:
        if self.cancelled:
            raise StudyValidationError("Study cancelled; no further provider or validation work may start",
                                       summary=self.summary())

    def cancel(self) -> None:
        self.session.cancel()

    def evaluate(self, files: dict[str, str], cases: list[dict], *, label: str,
                 purpose: str = "visible", reason: str = "", deterministic: bool | None = None,
                 seed: int | str | None = None) -> dict:
        return self.evaluate_many([dict(files=files, label=label)], cases,
            purpose=purpose, reason=reason, deterministic=deterministic, seed=seed)[0]

    def evaluate_matrix(self, requests: list[dict]) -> list[dict]:
        return self.evaluate_many(requests)

    def evaluate_many(self, requests: list[dict], cases: list[dict] | None = None, *,
                      purpose: str = "visible", reason: str = "",
                      deterministic: bool | None = None,
                      seed: int | str | None = None) -> list[dict]:
        """Execute an immutable ordered batch, allowing a distinct suite per row.

        Requests use ``files`` and a batch-unique ``label``; optional per-row
        fields override shared defaults. A common ``cases`` argument and per-row
        cases are mutually exclusive. Returned envelopes are independent copies.
        Infrastructure failures retain all raw results and raise one batch error.
        """
        self._assert_support()
        if not isinstance(requests, list) or not requests:
            raise ValueError("A study batch must contain at least one request")
        jobs: list[ValidationJob] = []
        labels: list[str] = []
        for request in requests:
            if not isinstance(request, dict) or set(request) - _FIELDS:
                raise ValueError("Unknown study validation request fields")
            label = request.get("label")
            if not isinstance(label, str) or not label.strip() or len(label) > 500:
                raise ValueError("A nonempty label of at most 500 characters is required")
            if label in labels:
                raise ValueError("Study validation labels must be unique within a batch")
            if cases is not None and "cases" in request:
                raise ValueError("Use shared cases or per-request cases, not both")
            selected_cases = request.get("cases", cases)
            if not isinstance(selected_cases, list):
                raise ValueError("Each study request needs an ordered case list")
            selected_purpose = request.get("purpose", purpose)
            selected_deterministic = request.get("deterministic", deterministic)
            if selected_deterministic is None:
                selected_deterministic = self.deterministic_visible
            if type(selected_deterministic) is not bool:
                raise ValueError("Determinism must be explicitly boolean")
            if selected_purpose == "visible" and selected_deterministic and not self.deterministic_visible:
                raise ValueError("Visible reuse was not enabled by the preregistered study contract")
            # Independent purposes cannot accidentally inherit a visible reuse declaration.
            if selected_purpose in ("final", "repeatability"):
                selected_deterministic = False
            name = re.sub(r"[^A-Za-z0-9_.-]", "-", label).strip("-.")[:48] or "observation"
            name = "job-" + name + "-" + digest(label)[:16]
            files = request.get("files")
            if not isinstance(files, dict):
                raise ValueError("Study validation files must be a source mapping")
            job = ValidationJob(name=name, files=files, cases=selected_cases,
                purpose=selected_purpose, reason=request.get("reason", reason),
                deterministic=selected_deterministic, seed=request.get("seed", seed),
                protocol=self.protocol + ":" + self.contract_sha256)
            snapshot = job.snapshot()
            if ("source_sha256" in request
                    and request["source_sha256"] != digest(snapshot["files"])):
                raise ValueError("Declared source identity differs from the requested files")
            jobs.append(ValidationJob(**snapshot))
            labels.append(label)
        summary = None
        try:
            summary = self.session.run(jobs)
            # Inner finalization is the authoritative source guard before cache
            # publication. Rechecking after that boundary would reject already
            # completed evidence merely because a later edit has begun.
            envelopes = self._project(summary, jobs, labels)
        except Exception as error:
            self._stopped = True
            raise StudyValidationError("Incomplete, infrastructure or mismatched study validation result",
                                       summary=summary if summary is not None else self.summary()) from error
        self._count += len(jobs)
        return envelopes

    def _project(self, summary: dict, jobs: list[ValidationJob], labels: list[str]) -> list[dict]:
        expected_count = self._count + len(jobs)
        results = summary["results"]
        if (summary["schema"] != SESSION_PROTOCOL or summary["session_id"] != self.session.session_id
                or not isinstance(results, list) or len(results) != expected_count
                or type(summary["counts"]["logical_jobs"]) is not int
                or summary["counts"]["logical_jobs"] != expected_count
                or summary.get("cancelled") is not False):
            raise ValueError("Incomplete study session")
        envelopes: list[dict] = []
        for offset, (job, label, result) in enumerate(zip(jobs, labels, results[self._count:]), self._count):
            snapshot = job.snapshot()
            binding = result["binding"]
            expected = self.session._binding(snapshot)
            if (digest(binding) != digest(expected) or result["key_sha256"] != digest(binding)
                    or result["schema"] != SESSION_PROTOCOL or result["session_id"] != self.session.session_id
                    or type(result["logical_index"]) is not int or result["logical_index"] != offset
                    or result["name"] != job.name or result["purpose"] != job.purpose
                    or result["reason"] != job.reason or result["status"] not in ("passed", "failed")
                    or type(result["physical"]) is not bool or type(result["passed"]) is not bool
                    or not isinstance(result["execution_id"], str) or not result["execution_id"]
                    or not isinstance(result["artifact_path"], str) or not result["artifact_path"]):
                raise ValueError("Invalid ordered result or execution identity")
            receipt = verify_receipt(result["receipt"], snapshot, binding)
            if result["status"] != receipt["status"] or result["passed"] != receipt["passed"]:
                raise ValueError("Result and receipt disagree")
            if result["physical"]:
                if result["reuse"] is not None:
                    raise ValueError("Physical execution cannot be reused")
            else:
                reuse = result["reuse"]
                if (job.purpose != "visible" or not job.deterministic or not isinstance(reuse, dict)
                        or reuse.get("kind") not in ("singleflight", "in_session", "persisted")
                        or reuse.get("execution_id") != result["execution_id"]
                        or not all(isinstance(reuse.get(key), str) and reuse[key]
                                   for key in ("origin_session_id", "artifact_path"))):
                    raise ValueError("Reuse lacks an eligible original execution")
            validation = {key: value for key, value in result.items() if key != "receipt"}
            validation.update(label=label, source_sha256=binding["source_sha256"],
                              suite_sha256=binding["suite_sha256"],
                              study_adapter_protocol=STUDY_ADAPTER_PROTOCOL,
                              study_contract_sha256=self.contract_sha256)
            envelopes.append(dict(receipt=receipt, validation=validation))
        return json.loads(json.dumps(envelopes, sort_keys=True, allow_nan=False))
