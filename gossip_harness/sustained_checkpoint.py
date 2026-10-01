"""Durable, fail-closed handoff between settled sustained-study stages.

A checkpoint authenticates local state with an out-of-band SHA-256. Its loader
does not authorize more API work or reconcile ambiguous calls. The caller must
keep the study quiescent while handing ownership to the next process.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import sqlite3
import tempfile

from .gitstore import GitStore


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


def save_checkpoint(path: Path, state: dict) -> str:
    """Atomically replace a checkpoint, fsync it and its directory, and hash it."""
    if not isinstance(state, dict):
        raise ValueError("Checkpoint state must be a JSON object")
    data = (json.dumps(state, sort_keys=True, indent=2, ensure_ascii=False,
                       allow_nan=False) + "\n").encode("utf-8")
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(mode="wb", prefix=f".{target.name}.",
                                         suffix=".tmp", dir=target.parent,
                                         delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
        temporary = None
        directory = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return hashlib.sha256(data).hexdigest()


def _object(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Checkpoint JSON contains duplicate keys")
        result[key] = value
    return result


def _invalid_constant(value: str) -> None:
    raise ValueError(f"Checkpoint JSON contains a nonfinite value: {value}")


def _settled_ledger(path: str) -> None:
    """Inspect the existing ledger without schema bootstrap or writes."""
    ledger_path = Path(path)
    if not ledger_path.is_file():
        raise ValueError("Checkpoint budget ledger does not exist")
    try:
        db = sqlite3.connect(ledger_path.resolve().as_uri() + "?mode=ro", uri=True, timeout=10)
        try:
            db.execute("PRAGMA query_only=ON")
            db.execute("BEGIN")
            if db.execute("SELECT 1 FROM reservations WHERE spent IS NULL OR state!='settled' LIMIT 1").fetchone():
                raise ValueError("Checkpoint budget ledger has unsettled reservations")
            if db.execute("SELECT 1 FROM intents WHERE state='pending' LIMIT 1").fetchone():
                raise ValueError("Checkpoint budget ledger has pending promotion intents")
        finally:
            db.close()
    except sqlite3.Error as error:
        raise ValueError("Checkpoint budget ledger could not be verified") from error


def load_checkpoint(path: Path, expected_sha: str, *, expected_contract_sha: str,
                    expected_pid: int | None = None) -> dict:
    """Verify handoff bytes, contract, stages, Git state, and settled accounting.

    When supplied, ``expected_pid`` is the receiving process's PID. It must
    differ from the PID recorded by the process that saved the checkpoint.
    """
    if not isinstance(expected_sha, str) or not _SHA256.fullmatch(expected_sha):
        raise ValueError("A full checkpoint SHA-256 is required")
    if not isinstance(expected_contract_sha, str) or not _SHA256.fullmatch(expected_contract_sha):
        raise ValueError("A full expected contract SHA-256 is required")
    if expected_pid is not None and (type(expected_pid) is not int or expected_pid <= 0):
        raise ValueError("The receiving process PID must be a positive integer")
    data = Path(path).read_bytes()
    if not hmac.compare_digest(hashlib.sha256(data).hexdigest(), expected_sha):
        raise ValueError("Checkpoint bytes do not match the expected SHA-256")
    state = json.loads(data, object_pairs_hook=_object, parse_constant=_invalid_constant)
    if not isinstance(state, dict):
        raise ValueError("Checkpoint state must be a JSON object")
    if state.get("contract_sha") != expected_contract_sha:
        raise ValueError("Checkpoint contract does not match the expected contract")
    pid = state.get("pid")
    if type(pid) is not int or pid <= 0:
        raise ValueError("Checkpoint PID must be a positive integer")
    if pid == expected_pid:
        raise ValueError("Checkpoint must resume in a different process")
    next_stage = state.get("next_stage")
    stages = state.get("completed_stages")
    if type(next_stage) is not int or not 0 <= next_stage <= 3:
        raise ValueError("Checkpoint next_stage must be an integer from 0 through 3")
    if (not isinstance(stages, list) or len(stages) != next_stage
            or any(not isinstance(stage, dict) or stage.get("completed") is not True for stage in stages)):
        raise ValueError("Checkpoint completed stages must match next_stage and all be complete")
    for field in ("store_path", "head", "budget_ledger"):
        if not isinstance(state.get(field), str) or not state[field].strip():
            raise ValueError(f"Checkpoint {field} must be a nonempty string")
    files = state.get("files")
    if (not isinstance(files, dict)
            or any(not isinstance(name, str) or not isinstance(content, str) for name, content in files.items())):
        raise ValueError("Checkpoint files must map text paths to text contents")
    store = GitStore(state["store_path"])
    if store.head() != state["head"]:
        raise ValueError("Checkpoint Git accepted head has changed")
    if store.read_files(state["head"]) != files:
        raise ValueError("Checkpoint files do not match the recorded Git commit")
    _settled_ledger(state["budget_ledger"])
    if store.head() != state["head"]:
        raise ValueError("Checkpoint Git accepted head changed during verification")
    return state
