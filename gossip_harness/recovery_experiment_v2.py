"""Additive, versioned early-accounting guard for the original recovery study.

The original runner, candidate semantics, replay concurrency and exact-rehearsal
gate are unchanged. Live entry requires an existing caller-owned shared ledger;
this wrapper never creates, resets or raises a live budget. Its own matching
rehearsal receipt is required in addition to the original runner's proof.
"""
from __future__ import annotations

from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
from typing import Any, Callable, Sequence

from . import recovery_experiment as original
from .ledger import Ledger


PROTOCOL = "integration-recovery-entry-v2"
MAX_LIVE_BUDGET = 10_000_000
EXECUTION_MODULES = ("__init__", "gitstore", "ledger", "pilot", "pilot_fixture", "promotion",
                     "recovery_experiment", "sandbox", "transport", "worker")


def execution_sources() -> dict[str, str]:
    """Protected original execution dependencies; a change invalidates a guard proof."""
    package = Path(__file__).parent
    return {f"gossip_harness/{name}.py": hashlib.sha256((package / f"{name}.py").read_bytes()).hexdigest()
            for name in EXECUTION_MODULES}


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False,
                                    separators=(",", ":")).encode()).hexdigest()


def _owned_file(path: Path) -> os.stat_result:
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
        raise ValueError("Shared ledger and sidecars must be regular files owned by the caller")
    return info


def inspect_live_ledger(ledger: Ledger | Path) -> dict[str, Any]:
    """Check one read-only SQLite snapshot before any Docker preparation."""
    path = Path(ledger.path if isinstance(ledger, Ledger) else ledger).absolute()
    try:
        info = _owned_file(path)
        for suffix in ("-wal", "-shm", ".schema.lock"):
            sidecar = Path(str(path) + suffix)
            if sidecar.exists() or sidecar.is_symlink():
                _owned_file(sidecar)
        with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as db:
            db.execute("PRAGMA query_only=ON")
            db.execute("BEGIN")
            if db.execute("PRAGMA quick_check").fetchone() != ("ok",):
                raise ValueError("Shared ledger failed SQLite integrity check")
            row = db.execute("SELECT value FROM settings WHERE key='budget'").fetchone()
            if row is None or type(row[0]) is not int or not 0 < row[0] <= MAX_LIVE_BUDGET:
                raise ValueError("Live shared cap must be >0 and <=$10")
            cap = row[0]
            reservations = db.execute("SELECT r.id,r.task_id,r.epoch,r.amount,r.spent,r.state,t.epoch "
                                      "FROM reservations r LEFT JOIN tasks t ON t.id=r.task_id").fetchall()
            used = 0
            for ident, task, epoch, amount, spent, state, task_epoch in reservations:
                if (not isinstance(ident, str) or not ident or not isinstance(task, str) or not task
                        or type(epoch) is not int or epoch < 1 or type(task_epoch) is not int or epoch > task_epoch
                        or type(amount) is not int or amount < 0
                        or state not in {"reserved", "settled"}
                        or (state == "reserved" and spent is not None)
                        or (state == "settled" and (type(spent) is not int or not 0 <= spent <= amount))):
                    raise ValueError("Shared ledger has invalid reservation accounting or task ownership")
                used += amount if spent is None else spent
            if used > cap:
                raise ValueError("Shared ledger accounting exceeds its cap")
            # Unsettled work and pending promotion remain valid durable states;
            # they count against capacity and are never erased by this guard.
            pending = db.execute("SELECT count(*) FROM intents WHERE state='pending'").fetchone()[0]
            after = _owned_file(path)
            if (info.st_dev, info.st_ino) != (after.st_dev, after.st_ino):
                raise ValueError("Shared ledger identity changed during its read-only check")
            return dict(path=str(path.resolve()), owner_uid=info.st_uid, device=info.st_dev,
                        inode=info.st_ino, limit=cap, spent_or_reserved=used, remaining=cap-used,
                        reservations=len(reservations), unsettled=sum(r[5] == "reserved" for r in reservations),
                        pending_promotions=pending, read_only=True)
    except (OSError, sqlite3.Error) as error:
        raise ValueError("Existing shared ledger is missing, malformed or unreadable: " + str(error)) from error


def entry_contract(*, image: str, seeds: tuple[int, ...], fault_profiles: tuple[str, ...],
                   attempts: int) -> dict[str, Any]:
    return dict(protocol=PROTOCOL, entry_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                original_sha256=hashlib.sha256(Path(original.__file__).read_bytes()).hexdigest(),
                execution_sources=execution_sources(), evaluator="original-pinned-docker",
                image=image, seeds=list(seeds), fault_profiles=list(fault_profiles), attempts=attempts,
                existing_owned_ledger_required=True, maximum_live_budget_units=MAX_LIVE_BUDGET,
                validation_order="read-only-ledger-then-original-exact-rehearsal-and-preparation",
                original_candidate_semantics=True, replay_concurrency=2)


def run_recovery_experiment(output_dir: str | Path, worker: Any, *, image: str = original.DEFAULT_IMAGE,
        budget_ledger: Ledger | Path | None = None, budget_units: int = 0, mode: str = "rehearsal",
        rehearsal_results: str | Path | None = None, seeds: Sequence[int] = (0, 1, 2),
        fault_profiles: Sequence[str] = original.FAULT_PROFILES, attempts: int = 2,
        validator_factory: Callable | None = None, progress: Callable[[str], Any] = print) -> dict[str, Any]:
    if mode not in {"rehearsal", "live"}:
        raise ValueError("Invalid recovery entry mode")
    output = Path(output_dir).absolute()
    if output.exists() or output.is_symlink():
        raise FileExistsError("Recovery output must be fresh")
    seeds, fault_profiles = tuple(seeds), tuple(fault_profiles)
    contract = entry_contract(image=image, seeds=seeds, fault_profiles=fault_profiles, attempts=attempts)
    before = None
    if mode == "live":
        if validator_factory is not None:
            raise ValueError("A custom validator is test-only and cannot qualify or execute live v2 recovery")
        if isinstance(worker, original.KnownRepairWorker):
            raise ValueError("KnownRepairWorker is an offline oracle and cannot be labeled live")
        if budget_ledger is None:
            raise ValueError("Live v2 recovery requires an existing shared ledger")
        before = inspect_live_ledger(budget_ledger)
        if budget_units not in (0, before["limit"]):
            raise ValueError("Live v2 entry cannot change the existing ledger budget")
        if rehearsal_results is None:
            raise ValueError("Live v2 entry requires a matching v2 rehearsal")
        rehearsal = Path(rehearsal_results)
        try:
            proof = json.loads((rehearsal.parent / "entry-guard.json").read_text())
            if (proof.get("schema_version") != 2 or proof.get("protocol") != PROTOCOL
                    or proof.get("qualification_mode") not in {"physical-rehearsal", "composed-entry-qualification"}
                    or proof.get("contract") != contract or proof.get("mode") != "rehearsal"
                    or proof.get("passed") is not True or proof.get("result_sha256") != hashlib.sha256(rehearsal.read_bytes()).hexdigest()):
                raise ValueError("Live v2 entry requires an exact matching v2 rehearsal")
            if proof["qualification_mode"] == "composed-entry-qualification":
                references = proof.get("references")
                if not isinstance(references, dict) or set(references) != {
                        "original_proof", "original_manifest", "original_trace", "execution_baseline", "regression_receipt"}:
                    raise ValueError("Composed guard qualification has incomplete reference bindings")
                for reference in references.values():
                    location = Path(reference["path"])
                    if location.is_symlink() or not location.is_file() or hashlib.sha256(location.read_bytes()).hexdigest() != reference["sha256"]:
                        raise ValueError("Composed guard qualification reference changed")
                if references["original_proof"]["sha256"] != proof["result_sha256"]:
                    raise ValueError("Composed guard qualification changed the original proof bytes")
                sources = proof.get("qualification_sources")
                required = {"devtools/qualify_recovery_entry.py", "tests/test_recovery_experiment_v2.py"}
                root = Path(__file__).resolve().parents[1]
                if not isinstance(sources, dict) or set(sources) != required or any(
                        hashlib.sha256((root / name).read_bytes()).hexdigest() != value for name, value in sources.items()):
                    raise ValueError("Composed guard qualification sources changed")
        except (OSError, json.JSONDecodeError, KeyError, TypeError) as error:
            raise ValueError("Live v2 entry requires an exact matching v2 rehearsal") from error
    receipt: dict[str, Any] = dict(schema_version=2, protocol=PROTOCOL, contract=contract, contract_sha256=_digest(contract),
        mode=mode, ledger_preflight=before, passed=False, original_executions="physical",
        qualification_mode="test-only-custom-validator" if validator_factory is not None else
            "physical-rehearsal" if mode == "rehearsal" else "live-entry")
    try:
        result = original.run_recovery_experiment(output, worker, image=image, budget_ledger=budget_ledger,
            budget_units=budget_units, mode=mode, rehearsal_results=rehearsal_results, seeds=seeds,
            fault_profiles=fault_profiles, attempts=attempts, validator_factory=validator_factory, progress=progress)
        if entry_contract(image=image, seeds=seeds, fault_profiles=fault_profiles, attempts=attempts) != contract:
            raise ValueError("Recovery entry source changed during execution")
        receipt["passed"] = result.get("status") == "accepted" and validator_factory is None
        saved = output / "results.json"
        if saved.is_file():
            receipt["result_sha256"] = hashlib.sha256(saved.read_bytes()).hexdigest()
        return result
    except BaseException as error:
        receipt["failure"] = str(error) or type(error).__name__
        raise
    finally:
        if output.is_dir():
            with (output / "entry-guard.json").open("x") as stream:
                json.dump(receipt, stream, indent=2, sort_keys=True)
                stream.write("\n")
