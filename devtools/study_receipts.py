"""Read-only audit of retained v2 study validation evidence.

Candidate files and archived Python remain data. This module neither imports nor
executes them and never contacts Docker or providers. The caller owns roster,
barrier, candidate/selection, and provider-accounting checks. Origins of reused
observations must remain inside the explicitly supplied evidence root.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
from typing import Any, Iterable

from .validation_session import (
    ADAPTER_VERSION, SESSION_PROTOCOL, ResourceBudget, ValidationJob,
    _ADAPTER_DIGEST, digest, support_digests, verify_receipt,
)

AUDIT_PROTOCOL = "study-retained-receipts-v1"
_SHA = re.compile(r"[0-9a-f]{64}\Z")


class StudyReceiptError(ValueError):
    """Retained evidence cannot qualify this study contract."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise StudyReceiptError(message)


def _pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for key, value in values:
        _require(key not in output, "Duplicate retained JSON key")
        output[key] = value
    return output


def _nonfinite(value: str) -> None:
    raise StudyReceiptError("Nonfinite retained JSON number: " + value)


def _read(raw: bytes, path: Path) -> dict:
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs,
                           parse_constant=_nonfinite)
    except (OSError, UnicodeError, ValueError, RecursionError) as error:
        raise StudyReceiptError("Cannot read ordinary retained JSON: " + str(path)) from error
    _require(isinstance(value, dict), "Retained JSON must be an object: " + str(path))
    return value


def _same(left: object, right: object) -> bool:
    return digest(left) == digest(right)


def audit_study_sessions(session_paths: Iterable[str | Path], expected_contract: dict, *,
                         evidence_root: str | Path) -> dict:
    """Audit complete ordered sessions and recursively verify physical origins.

    ``session_paths`` names directories or their ``session.json`` files. Returned
    ``observations`` contain ``job`` and ``result`` with exact candidate/suite
    data so callers can perform their study-specific barrier and selection gates.
    Current evaluator bytes must still match the expected rehearsal contract.
    Raw files are read once per audit, then checked unchanged before returning.
    """
    try:
        return _audit(session_paths, expected_contract, evidence_root=Path(evidence_root))
    except StudyReceiptError:
        raise
    except (KeyError, TypeError, ValueError, OSError, AttributeError, RecursionError, RuntimeError) as error:
        raise StudyReceiptError("Malformed retained study validation evidence") from error


def _audit(session_paths: Iterable[str | Path], expected_contract: dict, *, evidence_root: Path) -> dict:
    _require(not evidence_root.is_symlink() and evidence_root.is_dir(), "Evidence root must be an ordinary directory")
    root = evidence_root.resolve()
    expected = json.loads(json.dumps(expected_contract, allow_nan=False))
    _require(expected.get("protocol") == "study-validation-v2"
             and expected.get("session_protocol") == SESSION_PROTOCOL
             and expected.get("evaluator_adapter") == ADAPTER_VERSION,
             "Unexpected study/evaluator protocol")
    _require(isinstance(expected.get("study_protocol"), str) and bool(expected["study_protocol"])
             and type(expected.get("deterministic_visible")) is bool,
             "Invalid study observation policy")
    _require(expected.get("independent_purposes") == ["final", "repeatability"],
             "Independent observation policy changed")
    expected_support = expected["support_sha256"]
    project_root = Path(__file__).resolve().parents[1]
    _require(isinstance(expected_support, dict) and bool(expected_support), "Missing study support contract")
    source_bytes: dict[Path, bytes] = {}
    for relative, sha in expected_support.items():
        _require(isinstance(relative, str) and not Path(relative).is_absolute()
                 and ".." not in Path(relative).parts and isinstance(sha, str)
                 and _SHA.fullmatch(sha) is not None, "Invalid study support path or digest")
        source = project_root / relative
        _require(source.is_file() and not source.is_symlink(), "Study support is not an ordinary source file")
        raw = source.read_bytes()
        _require(hashlib.sha256(raw).hexdigest() == sha, "Qualified study support changed: " + relative)
        source_bytes[source] = raw
    evaluator_support = support_digests()
    _require(isinstance(expected_support, dict) and all(
        expected_support.get(path) == value for path, value in evaluator_support.items()),
        "Expected study does not bind the current complete evaluator")
    resources = expected["resource_budget"]
    budget = ResourceBudget(**{key: resources[key] for key in
                              ("workers", "cpus", "memory_mib", "outer_parallelism")})
    _require(type(resources.get("capacity")) is int and resources["capacity"] == budget.capacity(),
             "Invalid retained validation resource budget")
    _require(_same(expected["per_validator_resources"], {"cpus": 1, "memory_mib": 256, "pids": 64}),
             "Unexpected validator resource limits")
    expected_protocol = expected["study_protocol"] + ":" + digest(expected)
    retained_bytes: dict[Path, bytes] = {}
    loaded: dict[Path, dict] = {}
    observations_by_path: dict[Path, dict] = {}
    physical_ids: dict[str, Path] = {}

    def confined(value: str | Path, *, directory: bool = False) -> Path:
        path = Path(value)
        if not path.is_absolute():
            path = root / path
        _require(path.is_relative_to(root), "Retained path escapes evidence root")
        _require(all(part not in (".", "..") for part in path.relative_to(root).parts),
                 "Retained path traversal is forbidden")
        current = root
        for part in path.relative_to(root).parts:
            current = current / part
            _require(not current.is_symlink(), "Retained evidence must not traverse a symlink")
        _require(path.is_dir() if directory else path.is_file(), "Missing retained evidence: " + str(path))
        return path

    def read(path: Path) -> dict:
        path = confined(path)
        raw = path.read_bytes()
        if path in retained_bytes:
            _require(retained_bytes[path] == raw, "Retained evidence changed during audit")
        else:
            retained_bytes[path] = raw
        return _read(raw, path)

    def load_session(value: str | Path) -> dict:
        path = Path(value)
        if path.name == "session.json":
            path = path.parent
        path = confined(path, directory=True)
        if path in loaded:
            return loaded[path]
        contract = read(path / "study-contract.json")
        _require(_same(contract, expected), "Retained study contract differs from expected rehearsal")
        summary = read(path / "session.json")
        _require(summary.get("schema") == SESSION_PROTOCOL
                 and isinstance(summary.get("session_id"), str) and bool(summary["session_id"])
                 and summary.get("cancelled") is False, "Invalid or cancelled retained session")
        _require(_same(summary["resource_budget"], resources), "Retained session budget differs")
        runtime = summary["runtime_contract"]
        required_runtime = dict(session_protocol=SESSION_PROTOCOL, adapter_version=ADAPTER_VERSION,
            adapter_sha256=_ADAPTER_DIGEST, support_sha256=evaluator_support,
            image=expected["image"], timeout_seconds=expected["timeout_seconds"],
            case_timeout_seconds=expected["case_timeout_seconds"],
            resources=expected["per_validator_resources"],
            external_contract=dict(protocol="study-validation-v2", study_contract_sha256=digest(expected)),
            command=["python", "-I", "/checks/supervisor.py"])
        _require(all(_same(runtime.get(key), value) for key, value in required_runtime.items()),
                 "Retained evaluator runtime differs from the qualified contract")
        _require(isinstance(runtime.get("environment_sha256"), str)
                 and _SHA.fullmatch(runtime["environment_sha256"]) is not None
                 and all(isinstance(runtime.get(key), str) and runtime[key]
                         for key in ("host_python", "host_platform")), "Incomplete host/runtime identity")
        _require(set(runtime) == set(required_runtime) | {"environment_sha256", "host_python", "host_platform"},
                 "Unexpected runtime binding fields")
        results, counts = summary["results"], summary["counts"]
        _require(isinstance(results, list) and bool(results), "A qualified session needs complete observations")
        _require(isinstance(summary.get("preflights"), list) and bool(summary["preflights"])
                 and all(item.get("passed") is True for item in summary["preflights"]),
                 "Retained runtime health check failed or is missing")
        rows = []
        directories = set()
        for index, reported in enumerate(results):
            _require(isinstance(reported, dict) and type(reported.get("logical_index")) is int
                     and reported["logical_index"] == index, "Retained observations are incomplete or out of order")
            directory = confined(reported["artifact_path"], directory=True)
            _require(directory.parent == path, "Observation artifact is not owned by its session")
            result = read(directory / "result.json")
            request = read(directory / "input.json")
            _require(_same(result, reported), "Session summary differs from retained observation")
            job = ValidationJob(**request["job"]).snapshot()
            _require(directory.name == f"{index:05d}-{job['name']}", "Observation directory identity mismatch")
            _require(job["protocol"] == expected_protocol, "Observation lacks exact study protocol binding")
            _require(not (job["purpose"] == "visible" and job["deterministic"]
                          and not expected["deterministic_visible"]),
                     "Visible reuse was not enabled by the preregistered contract")
            binding = result["binding"]
            expected_binding = dict(runtime, runtime_identity=binding.get("runtime_identity"),
                source_sha256=digest(job["files"]), suite_sha256=digest(job["cases"]),
                purpose=job["purpose"], deterministic=job["deterministic"], seed=job["seed"], protocol=expected_protocol)
            _require(isinstance(binding.get("runtime_identity"), str) and bool(binding["runtime_identity"])
                     and _same(binding, expected_binding) and _same(request["binding"], binding)
                     and result["key_sha256"] == digest(binding), "Source, suite or runtime binding mismatch")
            _require(result.get("schema") == SESSION_PROTOCOL and result.get("session_id") == summary["session_id"]
                     and result.get("name") == job["name"] and result.get("purpose") == job["purpose"]
                     and result.get("reason") == job["reason"] and result.get("status") in ("passed", "failed")
                     and result.get("error") is None and type(result.get("physical")) is bool
                     and type(result.get("passed")) is bool and type(result.get("cache_rejected")) is bool
                     and isinstance(result.get("execution_id"), str) and bool(result["execution_id"]),
                     "Infrastructure failure or malformed retained observation")
            receipt = verify_receipt(result["receipt"], job, binding)
            _require(result["status"] == receipt["status"] and result["passed"] is receipt["passed"],
                     "Retained result disagrees with its receipt")
            if job["purpose"] in ("final", "repeatability"):
                _require(job["deterministic"] is False, "Independent observation inherited visible reuse policy")
            if result["physical"]:
                _require(result.get("reuse") is None, "Physical observation cannot claim reuse")
                old = physical_ids.setdefault(result["execution_id"], directory)
                _require(old == directory, "Multiple physical observations share one execution identity")
            else:
                reuse = result.get("reuse")
                _require(job["purpose"] == "visible" and job["deterministic"] is True
                         and isinstance(reuse, dict) and reuse.get("kind") in ("in_session", "singleflight", "persisted")
                         and reuse.get("execution_id") == result["execution_id"]
                         and all(isinstance(reuse.get(key), str) and reuse[key]
                                 for key in ("origin_session_id", "artifact_path")),
                         "Independent or ineligible observation was reused")
            row = dict(session_id=summary["session_id"], logical_index=index,
                       artifact_path=str(directory), job=job, result=result)
            rows.append(row)
            observations_by_path[directory] = row
            directories.add(directory.name)
        actual_directories = {item.name for item in path.iterdir()
                              if item.is_dir() and re.match(r"^\d{5}-", item.name)}
        _require(actual_directories == directories, "Session contains unaccounted observation artifacts")
        calculated = dict(logical_jobs=len(results), physical_executions=sum(r["physical"] for r in results),
            preflights=len(summary["preflights"]), cache_rejections=sum(r["cache_rejected"] for r in results))
        calculated.update({status: sum(r["status"] == status for r in results)
                           for status in ("passed", "failed", "infrastructure_failed", "cancelled")})
        calculated.update({"reused_" + kind: sum(bool(r["reuse"]) and r["reuse"]["kind"] == kind for r in results)
                           for kind in ("in_session", "persisted", "singleflight")})
        _require(all(type(counts.get(key)) is int and counts[key] == value for key, value in calculated.items()),
                 "Retained session counts do not reconcile")
        _require(summary.get("passed") is all(r["status"] == "passed" for r in results),
                 "Retained session pass flag does not reconcile")
        record = dict(path=str(path), session_id=summary["session_id"], counts=calculated, observations=rows)
        loaded[path] = record
        return record

    requested = [load_session(path) for path in session_paths]
    _require(bool(requested) and len({row["path"] for row in requested}) == len(requested),
             "Audit requires unique retained sessions")
    # Loading an origin session adds records; traverse until every reuse has a
    # verified physical origin. Reuse chains are never a substitute for one.
    checked: set[Path] = set()
    while len(checked) < len(observations_by_path):
        for directory, row in list(observations_by_path.items()):
            if directory in checked:
                continue
            checked.add(directory)
            result = row["result"]
            if result["physical"]:
                continue
            reuse = result["reuse"]
            origin_path = confined(reuse["artifact_path"], directory=True)
            origin_session = load_session(origin_path.parent)
            origin = observations_by_path.get(origin_path)
            _require(origin is not None, "Reused observation has no retained origin record")
            assert origin is not None
            original = origin["result"]
            _require(original["physical"] is True and original["reuse"] is None
                     and origin_session["session_id"] == reuse["origin_session_id"]
                     and original["execution_id"] == result["execution_id"]
                     and _same(original["binding"], result["binding"])
                     and _same(original["receipt"], result["receipt"]),
                     "Reused observation does not match its original physical execution")
            if reuse["kind"] in ("in_session", "singleflight"):
                _require(origin["session_id"] == row["session_id"]
                         and origin["logical_index"] < row["logical_index"],
                         "In-session reuse must reference an earlier physical observation")
    for path, raw in retained_bytes.items():
        confined(path)
        _require(path.read_bytes() == raw, "Retained evidence changed during audit")
    for path, raw in source_bytes.items():
        _require(path.is_file() and not path.is_symlink() and path.read_bytes() == raw,
                 "Study source changed during receipt audit")
    observations = [row for record in requested for row in record["observations"]]
    return dict(protocol=AUDIT_PROTOCOL, study_contract_sha256=digest(expected),
        evidence_root=str(root), sessions=[{key: value for key, value in record.items() if key != "observations"}
                                          for record in requested],
        observations=observations, verified_origin_sessions=len(loaded),
        counts=dict(logical_jobs=len(observations), physical_executions=sum(row["result"]["physical"] for row in observations),
                    reused_observations=sum(not row["result"]["physical"] for row in observations)),
        retained_files_verified=len(retained_bytes), candidate_code_executed=False)
