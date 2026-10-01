"""Compose unchanged physical recovery evidence with newly executed entry guards.

This does not replay candidates. Independently pinned inputs establish which
existing evidence and pre-change execution-source baseline the caller trusts.
Any original execution change rejects composition and needs a new rehearsal.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib
import json
from pathlib import Path
import re
import sqlite3
import tempfile
from typing import Any, Sequence
from unittest.mock import patch

from devtools.historical_audit import _complete, _decode, _sources
from retain_experiments import InputSnapshot


ROOT = Path(__file__).resolve().parents[1]
# Fixed compatibility boundary: imported legacy execution modules keep their
# own type-coverage policy; no module name comes from retained evidence.
guarded = importlib.import_module("gossip_harness.recovery_experiment_v2")
PROTOCOL = "recovery-entry-composition-v1"
_SHA = re.compile(r"[0-9a-f]{64}\Z")


def _hash(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def current_signature(recorded: dict[str, Any]) -> dict[str, Any]:
    original = guarded.original
    seeds, profiles = recorded.get("seeds"), recorded.get("fault_profiles")
    image, attempts = recorded.get("image_id"), recorded.get("attempts")
    if (not isinstance(seeds, list) or not seeds or any(type(seed) is not int for seed in seeds)
            or len(set(seeds)) != len(seeds) or not isinstance(profiles, list) or not profiles
            or any(not isinstance(profile, str) for profile in profiles)
            or len(set(profiles)) != len(profiles) or not set(profiles).issubset(original.FAULT_PROFILES)
            or type(attempts) is not int or attempts not in (1, 2)
            or not isinstance(image, str) or re.fullmatch(r"sha256:[0-9a-f]{64}", image) is None):
        raise ValueError("Original rehearsal parameters are malformed")
    files = original.fixture_files()
    return dict(spec_version=original.SPEC_VERSION, image_id=image, source_commit=original.SOURCE_COMMIT,
        fixture_sha256=original._hash(files), injected_sha256=original._hash(original.injected_changes(files)),
        checks_sha256=original._hash(original.CHECKS), contract_sha256=original._hash([original.SPEC, original.PARSER_SPEC]),
        implementation_sha256=_hash(Path(original.__file__).read_bytes()), seeds=seeds,
        fault_profiles=profiles, attempts=attempts, replay_concurrency=2)


def physical_guard_probes(output_dir: Path) -> dict[str, Any]:
    """Exercise real disposable ledgers and ensure no candidate preparation starts."""
    passed: list[str] = []
    with tempfile.TemporaryDirectory(prefix="guard-probes-", dir=output_dir) as directory:
        root = Path(directory)
        ledger = guarded.Ledger(root / "ledger.sqlite", 1000)
        ledger.add_task("task")
        lease = ledger.claim("task", "worker", now=1, ttl=100)
        ledger.reserve("reserved", lease, 100, now=2)
        with sqlite3.connect(ledger.path) as db:
            before = list(db.iterdump())
        read = guarded.inspect_live_ledger(ledger)
        with sqlite3.connect(ledger.path) as db:
            if list(db.iterdump()) != before or read["spent_or_reserved"] != 100 or not read["read_only"]:
                raise ValueError("Valid ledger probe changed durable accounting")
        passed.append("valid-read-only-accounting")
        def rejected(name: str, **options: Any) -> None:
            with patch.object(guarded.original, "run_recovery_experiment",
                              side_effect=AssertionError("Guard reached candidate preparation")) as original:
                try:
                    guarded.run_recovery_experiment(root / name, object(), mode="live", **options)
                except ValueError:
                    pass
                else:
                    raise ValueError("Negative guard probe did not reject: " + name)
                if original.call_count or (root / name).exists():
                    raise ValueError("Negative guard probe launched preparation: " + name)
            passed.append(name)
        rejected("missing-ledger", budget_ledger=root / "absent.sqlite")
        link = root / "symlink.sqlite"
        link.symlink_to(ledger.path)
        rejected("symlink-ledger", budget_ledger=link)
        for cap in (0, guarded.MAX_LIVE_BUDGET + 1):
            with sqlite3.connect(ledger.path) as db:
                db.execute("UPDATE settings SET value=? WHERE key='budget'", (cap,))
            rejected("invalid-cap-" + str(cap), budget_ledger=ledger)
        with sqlite3.connect(ledger.path) as db:
            db.execute("UPDATE settings SET value=1000 WHERE key='budget'")
            db.execute("UPDATE reservations SET spent=101,state='settled'")
        rejected("invalid-accounting", budget_ledger=ledger)
        with sqlite3.connect(ledger.path) as db:
            db.execute("UPDATE reservations SET spent=NULL,state='reserved'")
        rejected("budget-change", budget_ledger=ledger, budget_units=2000)
        rejected("missing-v2-proof", budget_ledger=ledger)
        rejected("custom-validator-ineligible", budget_ledger=ledger, validator_factory=lambda _: None)
    return dict(passed=True, physical=True, probes=passed, count=len(passed),
                candidate_executions=0, container_executions=0, provider_requests=0)


def qualify(original_proof: Path, trusted_proof_sha256: str, execution_baseline: Path,
            trusted_baseline_sha256: str, regression_receipt: Path, trusted_regression_sha256: str,
            output: Path) -> dict[str, Any]:
    output = output.absolute()
    if output.exists() or output.is_symlink():
        raise FileExistsError("Composed qualification output must be fresh")
    snapshot = InputSnapshot()
    references: dict[str, dict[str, str]] = {}
    def read(name: str, path: Path, expected: str | None = None) -> bytes:
        path = path.absolute()
        raw = snapshot.read(path, path.parent)
        sha = _hash(raw)
        if expected is not None and (_SHA.fullmatch(expected) is None or sha != expected):
            raise ValueError("Independently pinned input differs: " + name)
        references[name] = dict(path=str(path), sha256=sha)
        return raw
    proof_raw = read("original_proof", original_proof, trusted_proof_sha256)
    proof = _decode(proof_raw)
    if not isinstance(proof, dict) or not isinstance(proof.get("signature"), dict):
        raise ValueError("Original rehearsal is malformed")
    signature = current_signature(proof["signature"])
    if not guarded.original._valid_rehearsal(proof, signature) or proof.get("unexecuted") != []:
        raise ValueError("Original execution signature or complete rehearsal differs")
    manifest = _decode(read("original_manifest", original_proof.parent / "manifest.json"))
    if (not isinstance(manifest, dict) or manifest.get("signature") != signature
            or manifest.get("mode") != "rehearsal" or manifest.get("status") != "finished"):
        raise ValueError("Original physical manifest differs from the rehearsal")
    baseline = _decode(read("execution_baseline", execution_baseline, trusted_baseline_sha256))
    execution = guarded.execution_sources()
    if not isinstance(baseline, dict) or not isinstance(baseline.get("sources"), dict) or any(
            baseline["sources"].get(name) != value for name, value in execution.items()):
        raise ValueError("Original execution sources changed; a new physical rehearsal is required")
    closure = _complete(_sources(ROOT, InputSnapshot()), "gossip_harness.recovery_experiment")
    closure_files = {module.replace(".", "/") + ("/__init__.py" if module == "gossip_harness" else ".py") for module in closure}
    if closure_files != set(execution):
        raise ValueError("Original execution import boundary changed; composition is ineligible")
    trace = [_decode(line) for line in read("original_trace", original_proof.parent / "trace.jsonl").splitlines() if line]
    final = {}
    for event in trace:
        if not isinstance(event, dict) or event.get("kind") != "validation" or event.get("stage") != "final-repair":
            continue
        validation = event.get("receipt")
        if (event.get("case") in final or event.get("passed") is not True or event.get("target") != "all"
                or not isinstance(validation, dict) or validation.get("test_only_host_validator")
                or validation.get("image_id") != signature["image_id"] or validation.get("checks_sha256") != signature["checks_sha256"]
                or validation.get("status") != "passed" or validation.get("cleanup_verified") is not True
                or validation.get("timed_out") is not False or validation.get("exit_code") != 0
                or validation.get("command") != ["python", "-I", "/checks/run_checks.py", "all"]
                or not guarded.original._completion_valid(validation, "all")):
            raise ValueError("Original final Docker receipt is incomplete or test-only")
        final[event["case"]] = validation
    if set(final) != {case["case"] for case in proof["cases"]}:
        raise ValueError("Original final Docker evidence does not cover the complete roster")
    regression = _decode(read("regression_receipt", regression_receipt, trusted_regression_sha256))
    expected_tests = {name: _hash((ROOT / name).read_bytes()) for name in (
        "gossip_harness/recovery_experiment_v2.py", "tests/test_recovery_experiment_v2.py")}
    if (not isinstance(regression, dict) or regression.get("returncode") != 0
            or regression.get("static_passed") is not True or regression.get("unchanged") is not True
            or type(regression.get("tests")) is not int or regression["tests"] < 1
            or any(regression.get("source_sha256", {}).get(name) != sha for name, sha in expected_tests.items())):
        raise ValueError("New entry regression receipt is not successful and source-bound")
    contract = guarded.entry_contract(image=signature["image_id"], seeds=tuple(signature["seeds"]),
        fault_profiles=tuple(signature["fault_profiles"]), attempts=signature["attempts"])
    qualification_sources = {name: _hash((ROOT / name).read_bytes()) for name in (
        "devtools/qualify_recovery_entry.py", "tests/test_recovery_experiment_v2.py")}
    snapshot.verify()
    output.mkdir(parents=True, exist_ok=False)
    receipt: dict[str, Any] = dict(schema_version=2, protocol=guarded.PROTOCOL, composition_protocol=PROTOCOL,
        qualification_mode="composed-entry-qualification", contract=contract, contract_sha256=guarded._digest(contract),
        mode="rehearsal", passed=False, original_executions="reused", references=references,
        result_sha256=_hash(proof_raw), qualification_sources=qualification_sources,
        counts=dict(reused_original_cases=len(proof["cases"]), new_candidate_executions=0,
                    new_container_executions=0, provider_requests=0))
    try:
        probes = physical_guard_probes(output)
        if probes.get("passed") is not True or probes.get("physical") is not True:
            raise ValueError("Physical entry guard probes failed")
        receipt["guard_probes"] = probes
        receipt["counts"]["physical_guard_probes"] = probes["count"]
        snapshot.verify()
        if guarded.entry_contract(image=signature["image_id"], seeds=tuple(signature["seeds"]),
                fault_profiles=tuple(signature["fault_profiles"]), attempts=signature["attempts"]) != contract:
            raise ValueError("Entry or original execution sources changed during qualification")
        if any(_hash((ROOT / name).read_bytes()) != sha for name, sha in qualification_sources.items()):
            raise ValueError("Qualification sources changed during qualification")
        with (output / "results.json").open("xb") as results_stream:
            results_stream.write(proof_raw)
        receipt["passed"] = True
    except BaseException as error:
        receipt["failure"] = str(error) or type(error).__name__
        raise
    finally:
        with (output / "entry-guard.json").open("x") as guard_stream:
            json.dump(receipt, guard_stream, indent=2, sort_keys=True)
            guard_stream.write("\n")
    return receipt


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("original-proof", "execution-baseline", "regression-receipt", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    for name in ("proof", "baseline", "regression"):
        parser.add_argument("--trusted-" + name + "-sha256", required=True)
    args = parser.parse_args(argv)
    print(json.dumps(qualify(args.original_proof, args.trusted_proof_sha256,
        args.execution_baseline, args.trusted_baseline_sha256,
        args.regression_receipt, args.trusted_regression_sha256, args.output), indent=2))


if __name__ == "__main__":
    main()
