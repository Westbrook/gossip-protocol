"""Join the explicit test inventory to selected retained evidence without running tests.

No receipt trees are searched by default. --receipts-root reads only immediate
session children, so generated repositories and historical studies are untouched.
This report is an audit aid, never a replacement for the verification cache gate.
"""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
import math
from pathlib import Path
import statistics
from typing import Any

from devtools.verify import digest, discover, inputs, load_manifest, make_jobs

SCHEMA = 1
TIMINGS = ("preparation_seconds", "setup_seconds", "run_seconds", "cleanup_seconds", "duration_seconds")


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError(f"Expected a JSON object: {path}")
    return value


def _number(value: Any) -> bool:
    return type(value) in (int, float) and math.isfinite(value) and value >= 0


def _stats(values: list[float]) -> dict[str, Any]:
    if not values:
        return {"samples": 0, "median": None, "minimum": None, "maximum": None,
                "p95": None, "p95_reason": "At least 20 unique physical samples required"}
    ordered = sorted(values)
    return {"samples": len(values), "median": statistics.median(values),
            "minimum": ordered[0], "maximum": ordered[-1],
            "p95": ordered[math.ceil(len(values) * .95) - 1] if len(values) >= 20 else None,
            "p95_reason": "Nearest rank; selected observations only" if len(values) >= 20 else
            "At least 20 unique physical samples required"}


def receipt_paths(receipts: list[Path], roots: list[Path]) -> list[Path]:
    """Explicit files, session directories, or one shallow level of sessions."""
    selected = [path / "summary.json" if path.is_dir() else path for path in receipts]
    for root in roots:
        if not root.is_dir():
            raise ValueError(f"Receipt root does not exist: {root}")
        if (root / "summary.json").is_file():
            selected.append(root / "summary.json")
        selected.extend(sorted(root.glob("*/summary.json")))
    paths = sorted({path.resolve() for path in selected})
    if any(not path.is_file() for path in paths):
        raise ValueError("Every selected receipt must be an existing summary.json file")
    return paths


def _identity(summary_path: Path, summary: dict[str, Any], current: dict[str, str],
              target: dict[str, Any] | None) -> dict[str, Any]:
    path = summary_path.with_name("inputs.json")
    result: dict[str, Any] = {"path": str(path), "status": "unavailable", "source_status": "unknown",
                              "execution_identity_status": "not_compared"}
    try:
        identity = _object(path)
        if not isinstance(identity.get("inputs"), dict) or digest(identity) != summary.get("fingerprint"):
            raise ValueError("Summary fingerprint does not bind the retained inputs")
        recorded = identity["inputs"]
        result.update(status="verified", sha256=_hash(path), fingerprint=summary["fingerprint"],
                      source_status="exact" if recorded == current else "different",
                      changed_inputs=sorted(key for key in set(recorded) | set(current)
                                            if recorded.get(key) != current.get(key)))
        if target is not None:
            result["execution_identity_status"] = "exact" if identity == target else "different"
    except (OSError, ValueError, TypeError) as error:
        result.update(status="unavailable", detail=str(error))
    return result


def _physical(job: dict[str, Any], summary_path: Path) -> tuple[dict[str, Any], dict[str, Any] | None]:
    evidence: dict[str, Any] = {"status": "unavailable", "path": job.get("evidence_path"),
                                "sha256": job.get("evidence_sha256")}
    try:
        path = Path(job["evidence_path"])
        log = Path(job["log"])
        # Runner receipts use absolute paths. Relative archived receipts resolve
        # against their session, never an unrelated caller working directory.
        if not path.is_absolute():
            path = summary_path.parent / path
        if not log.is_absolute():
            log = summary_path.parent / log
        if _hash(path) != job["evidence_sha256"] or _hash(log) != job["log_sha256"]:
            raise ValueError("Physical result or log digest mismatch")
        original = _object(path)
        ids = [test["id"] for test in job["tests"]]
        if (not ids or len(ids) != len(set(ids)) or original.get("schema_version") != SCHEMA
                or original.get("physical") is not True or original.get("class") != job.get("class")
                or original.get("lane") != job.get("lane")
                or [test["id"] for test in original["tests"]] != ids):
            raise ValueError("Physical result has inconsistent identity or ordered methods")
        evidence.update(status="verified", path=str(path.resolve()), log=str(log.resolve()),
                        log_sha256=job["log_sha256"], physical_status=original.get("status"),
                        cleanup_status=original.get("cleanup_status"))
        return evidence, original
    except (OSError, ValueError, KeyError, TypeError) as error:
        evidence["detail"] = str(error)
        return evidence, None


def build_inventory(root: Path, *, receipts: list[Path] | None = None,
                    receipts_roots: list[Path] | None = None,
                    current_identity: dict[str, Any] | None = None) -> dict[str, Any]:
    """Describe every current class/lane and its explicitly selected observations.

    Matching source bytes alone are not called current verification: environment,
    runtime, ordered methods and purpose still matter. An optional current identity
    is the exact inputs.json produced by the verification owner, not a guessed one.
    """
    root = root.resolve()
    manifest = load_manifest(root)
    inventory = discover(root, manifest)
    current = inputs(root, manifest)
    if current_identity is not None and current_identity.get("inputs") != current:
        raise ValueError("Comparison identity does not bind the current authored inputs")
    rows: dict[tuple[str, str], dict[str, Any]] = {}
    for job in make_jobs(inventory):
        config = manifest["classes"][job["class"]]
        ids = [test["id"] for test in job["tests"]]
        rows[(job["class"], job["lane"])] = {
            "class": job["class"], "lane": job["lane"], "methods": ids,
            "invariant": config.get("invariant"), "expected_outcome": "Every selected method passes its asserted contract",
            "distinct_defect": config.get("distinct_defect"), "boundary": config.get("boundary"),
            "disposition": config.get("disposition"), "overlap_reason": config.get("overlap_reason", config.get("overlap")),
            "isolation": config.get("isolation"), "resource_weight": job["weight"],
            "exclusive": job.get("exclusive", False),
            "timeout_seconds": {test["id"]: test["timeout_seconds"] for test in job["tests"]},
            "affected_input_binding": "current_inputs", "observations": [],
        }
    selected = receipt_paths(receipts or [], receipts_roots or [])
    sessions = []
    unmatched = []
    for path in selected:
        summary = _object(path)
        if summary.get("schema_version") != SCHEMA or not isinstance(summary.get("jobs"), list):
            raise ValueError(f"Unsupported verification summary: {path}")
        binding = _identity(path, summary, current, current_identity)
        session = {"path": str(path), "sha256": _hash(path), "status": summary.get("status"),
                   "first_negative_seconds": summary.get("first_negative_seconds"), "binding": binding,
                   "static_status": summary.get("static", {}).get("status"),
                   "stale_inputs": summary.get("stale_inputs"), "error": summary.get("error"),
                   "resources": summary.get("resources"), "io_accounting": summary.get("io_accounting")}
        sessions.append(session)
        for job in summary["jobs"]:
            key = (job.get("class"), job.get("lane"))
            if key not in rows:
                unmatched.append({"summary": str(path), "class": key[0], "lane": key[1], "status": job.get("status")})
                continue
            evidence, original = _physical(job, path) if job.get("physical") or job.get("reused_from") else (
                {"status": "not_executed"}, None)
            tests = job.get("tests", [])
            selected_ids = [test.get("id") for test in tests]
            current_methods = rows[key]["methods"]
            clean_pass = (original is not None and original.get("status") == "passed"
                          and original.get("cleanup_status") == "completed" and job.get("status") == "passed"
                          and job.get("returncode") == 0 and job.get("cleanup_status") == "completed"
                          and all(test.get("status") == "passed" for test in original["tests"] + tests))
            observation = {
                "summary": str(path), "status": job.get("status"), "reason": job.get("reason"),
                "physical": job.get("physical") is True, "reused_from": job.get("reused_from"),
                "methods": tests, "current_method_selection": selected_ids == current_methods,
                "source_status": binding["source_status"], "execution_identity_status": binding["execution_identity_status"],
                "recorded_fingerprint": binding.get("fingerprint"),
                "current_complete_pass": bool(clean_pass and selected_ids == current_methods and binding["source_status"] == "exact"
                                              and binding["execution_identity_status"] == "exact" and summary.get("stale_inputs") is False
                                              and not summary.get("error") and session["static_status"] == "passed"),
                "evidence": evidence,
                "timeout_observed": job.get("reason") == "class deadline exceeded" or any(
                    "TimeoutExpired" in str(test.get("detail", "")) or "timed out" in str(test.get("detail", "")).lower()
                    for test in tests),
                "controller_wall_seconds": job.get("wall_seconds"), "queue_seconds": job.get("queue_seconds"),
                "current_lookup_seconds": job.get("reuse_lookup_seconds"),
                "physical_cost": ({key: original.get(key) for key in (*TIMINGS, "subprocess_starts", "subprocess_counter_scope", "resources", "io_accounting")}
                                  if original is not None else None),
            }
            rows[key]["observations"].append(observation)
    for row in rows.values():
        observations = row["observations"]
        physical: dict[tuple[str, str], dict[str, Any]] = {}
        for observed in observations:
            evidence = observed["evidence"]
            if evidence["status"] == "verified":
                physical[(evidence["path"], evidence["sha256"])] = observed
        row["cost"] = {key: _stats([item["physical_cost"][key] for item in physical.values()
                                    if _number(item["physical_cost"][key])]) for key in TIMINGS}
        row["cost"]["scope"] = ("Unique verified physical receipt paths and digests; selected history only. "
                                "Setup/cleanup are class phases; method phases remain in observations.methods.")
        strata: dict[str, dict[str, Any]] = {}
        for item in physical.values():
            comparison = {"fingerprint": item["recorded_fingerprint"],
                          "ordered_methods": [test["id"] for test in item["methods"]],
                          "physical_status": item["evidence"]["physical_status"]}
            stratum = strata.setdefault(digest(comparison), {**comparison, "observations": []})
            stratum["observations"].append(item)
        row["cost_by_execution_identity"] = [
            {"fingerprint": group["fingerprint"], "ordered_methods": group["ordered_methods"],
             "physical_status": group["physical_status"],
             "phases": {key: _stats([item["physical_cost"][key] for item in group["observations"]
                                     if _number(item["physical_cost"][key])]) for key in TIMINGS}}
            for group in strata.values()]
        for group in row["cost_by_execution_identity"]:
            if group["fingerprint"] is None:
                for phase in TIMINGS:
                    group["phases"][phase].update(p95=None, p95_reason="Execution identity unavailable")
        if len(strata) != 1 or any(group["fingerprint"] is None for group in strata.values()):
            for phase in TIMINGS:
                row["cost"][phase].update(p95=None, p95_reason="No pooled percentile across missing or different execution identities/selections")
        row["physical_observations"] = len(physical)
        row["reuse_observations"] = sum(bool(item["reused_from"]) for item in observations)
        row["failure_history"] = [item for item in observations if item["status"] in {"failed", "error"}]
        row["timeout_observations"] = sum(item["timeout_observed"] for item in observations)
        row["flaky_rate"] = None
        row["flaky_rate_reason"] = "Selected history is not a controlled same-input repetition sample; no flakiness inferred"
        row["current_complete_pass"] = any(item["current_complete_pass"] for item in observations)
    return {
        "schema_version": SCHEMA, "root": str(root), "discovered_methods": len(inventory),
        "class_lane_groups": len(rows), "lane_counts": dict(Counter(test["lane"] for test in inventory)),
        "current_inputs": {"scope": "Conservative full authored dependency closure from manifest, including retained fixtures",
                           "sha256": digest(current), "files": current},
        "execution_comparison": "provided identity" if current_identity is not None else "not requested; source matches alone are historical evidence",
        "selected_sessions": sessions, "rows": list(rows.values()), "unmatched_historical_jobs": unmatched,
        "interpretation": "Evidence audit only; does not run tests, infer defect uniqueness from test counts, or authorize receipt reuse",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--receipt", type=Path, action="append", default=[])
    parser.add_argument("--receipts-root", type=Path, action="append", default=[])
    parser.add_argument("--current-inputs", type=Path, help="Owner-produced inputs.json for an exact execution-identity comparison")
    parser.add_argument("--output", type=Path, required=True, help="Fresh JSON output; existing evidence is never replaced")
    args = parser.parse_args(argv)
    result = build_inventory(args.root, receipts=args.receipt, receipts_roots=args.receipts_root,
                             current_identity=_object(args.current_inputs) if args.current_inputs else None)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x") as handle:
        json.dump(result, handle, sort_keys=True, indent=2)
        handle.write("\n")
    print(json.dumps({"output": str(args.output), "methods": result["discovered_methods"],
                      "groups": result["class_lane_groups"], "selected_sessions": len(result["selected_sessions"])}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
