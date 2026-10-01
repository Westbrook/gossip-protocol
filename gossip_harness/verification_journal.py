"""Durable local replay of provider responses before accounting settlement.

This journal never retries a dispatch whose outcome is unknown. It provides
single local dispatch and persisted-response replay under a per-call process
lock; it cannot provide provider-side exactly-once execution. Checksums detect
accidental corruption, not malicious replacement of the trusted host journal.
"""

from __future__ import annotations

from dataclasses import asdict
import fcntl
import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import Callable

from .worker import WorkerFailure, WorkerRequest, WorkerResult


class JournalError(RuntimeError):
    """The journal cannot safely proceed."""


class JournalCorrupt(JournalError):
    """A durable receipt is malformed, inconsistent, or has changed."""


class JournalConflict(JournalError):
    """A call ID was reused with a different request or reservation."""


class JournalUnknownOutcome(JournalError):
    """Dispatch may have happened; no durable response permits safe replay."""


def _bytes(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, ensure_ascii=True,
                       allow_nan=False, separators=(",", ":")) + "\n").encode("utf-8")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError("Duplicate JSON key")
        result[key] = value
    return result


def _constant(value):
    raise ValueError("Nonfinite JSON value")


def _read(path: Path) -> tuple[dict, bytes]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw, object_pairs_hook=_pairs, parse_constant=_constant)
        if not isinstance(value, dict):
            raise ValueError("Receipt must be an object")
        return value, raw
    except (OSError, ValueError, RecursionError) as error:
        raise JournalCorrupt("Journal receipt could not be decoded") from error


def _write(path: Path, value: dict) -> bytes:
    data = _bytes(value)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="wb", prefix=f".{path.name}.",
                                         suffix=".tmp", dir=path.parent, delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        temporary = None
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return data


def _result(payload: dict) -> WorkerResult:
    if (not isinstance(payload, dict) or set(payload) != {"changes", "summary", "usage_units", "metadata"}
            or not isinstance(payload["changes"], dict)
            or any(not isinstance(path, str) or (value is not None and not isinstance(value, str))
                   for path, value in payload["changes"].items())
            or not isinstance(payload["summary"], str)
            or type(payload["usage_units"]) is not int or payload["usage_units"] < 0
            or not isinstance(payload["metadata"], dict)):
        raise JournalCorrupt("Worker result has invalid fields or usage")
    return WorkerResult(**payload)


def _failure(payload: dict) -> WorkerFailure:
    if (not isinstance(payload, dict) or set(payload) != {"message", "usage_units", "metadata"}
            or not isinstance(payload["message"], str) or not isinstance(payload["metadata"], dict)
            or (payload["usage_units"] is not None
                and (type(payload["usage_units"]) is not int or payload["usage_units"] < 0))):
        raise JournalCorrupt("Worker failure has invalid fields or usage")
    return WorkerFailure(**payload)


class RequestJournal:
    """Own one trusted local directory of request, response, and settled receipts.

    A reservation has exactly one call owner within this directory. Callers must
    use one journal for a shared reservation namespace, or guarantee disjoint
    reservation IDs across separate journals; opaque callbacks cannot expose a
    common ledger for cross-directory ownership checks.
    """

    def __init__(self, root: Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def paths(self, call_id: str) -> dict[str, Path]:
        """Return receipt paths without embedding caller text in filenames."""
        if not isinstance(call_id, str) or not call_id.strip():
            raise ValueError("A nonempty call ID is required")
        stem = _sha(call_id.encode("utf-8"))
        return {kind: self.root / f"{stem}.{kind}.json" for kind in ("request", "result", "settled")}

    def _claim_reservation(self, identity: dict) -> None:
        stem = _sha(identity["reservation_id"].encode("utf-8"))
        path = self.root / f"{stem}.reservation.json"
        with path.with_suffix(".lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            if path.exists():
                owner, _ = _read(path)
                if _bytes(owner) != _bytes(identity):
                    raise JournalConflict("Reservation ID already belongs to a different call or request")
            else:
                _write(path, identity)

    def execute(self, call_id: str, request: WorkerRequest, reservation_id: str,
                invoke: Callable[[], WorkerResult], reserve: Callable[[], None],
                settle: Callable[[int], None], on_persisted: Callable[[], None] | None = None) -> WorkerResult:
        """Invoke once or replay a durable outcome, reconciling settlement.

        Callbacks reserve/settle must use the supplied stable reservation ID and
        both must be idempotent. A crash after reserve but before intent storage
        can repeat reserve, without repeating provider dispatch. WorkerRequest
        contains no model/profile settings, so the caller's frozen contract or
        call namespace must also bind those provider settings.
        ``on_persisted`` runs only on the initial
        durable response, before settlement, and is never called during replay.
        WorkerFailure is persisted and re-raised with the same sanitized fields.
        Unknown usage keeps its reservation and is never settled as zero.
        """
        paths = self.paths(call_id)
        if not isinstance(request, WorkerRequest):
            raise ValueError("A WorkerRequest is required")
        if not isinstance(reservation_id, str) or not reservation_id.strip():
            raise ValueError("A nonempty reservation ID is required")
        # Serialize once so mutations to request.files during invocation cannot
        # change the durable request identity halfway through this execution.
        request_data = json.loads(_bytes(asdict(request)))
        request_sha = _sha(_bytes(request_data))
        identity = dict(schema_version=1, call_id=call_id, reservation_id=reservation_id,
                        request_sha256=request_sha)
        expected_request = {**identity, "request": request_data}
        lock_path = paths["request"].with_suffix(".lock")
        with lock_path.open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            self._claim_reservation(identity)
            if paths["request"].exists():
                stored_request, _ = _read(paths["request"])
                if _bytes(stored_request) != _bytes(expected_request):
                    raise JournalConflict("Call ID does not match the durable request and reservation")
                if not paths["result"].exists():
                    if paths["settled"].exists():
                        raise JournalCorrupt("A settled receipt has no corresponding durable result")
                    raise JournalUnknownOutcome("Dispatch intent exists without a durable response; automatic retry is forbidden")
                receipt, raw = _read(paths["result"])
            else:
                if paths["result"].exists() or paths["settled"].exists():
                    raise JournalCorrupt("Response or settlement exists without its request")
                reserve()
                _write(paths["request"], expected_request)
                try:
                    outcome = invoke()
                except WorkerFailure as error:
                    kind = "failure"
                    payload = dict(message=str(error), usage_units=error.usage_units, metadata=error.metadata)
                    _failure(payload)
                except Exception:
                    raise JournalUnknownOutcome("Invocation ended without a durable response; automatic retry is forbidden") from None
                else:
                    if not isinstance(outcome, WorkerResult):
                        raise JournalCorrupt("Invocation did not return a WorkerResult")
                    kind = "result"
                    payload = asdict(outcome)
                    _result(payload)
                receipt = {**identity, "kind": kind, "payload": payload, "payload_sha256": _sha(_bytes(payload))}
                raw = _write(paths["result"], receipt)
                if on_persisted is not None:
                    on_persisted()
            if (set(receipt) != {*identity, "kind", "payload", "payload_sha256"}
                    or any(_bytes(receipt.get(key)) != _bytes(value) for key, value in identity.items())
                    or receipt["payload_sha256"] != _sha(_bytes(receipt["payload"]))):
                raise JournalCorrupt("Result receipt identity or checksum does not match")
            if receipt["kind"] == "result":
                outcome = _result(receipt["payload"])
            elif receipt["kind"] == "failure":
                outcome = _failure(receipt["payload"])
            else:
                raise JournalCorrupt("Unknown journal outcome kind")
            usage = outcome.usage_units
            if usage is None:
                if paths["settled"].exists():
                    raise JournalCorrupt("Unknown usage cannot have a settled receipt")
                raise outcome
            settled = {**identity, "result_sha256": _sha(raw), "usage_units": usage}
            if paths["settled"].exists():
                prior_settled, _ = _read(paths["settled"])
                if _bytes(prior_settled) != _bytes(settled):
                    raise JournalCorrupt("Settlement receipt does not match the durable result")
            settle(usage)
            if not paths["settled"].exists():
                _write(paths["settled"], settled)
            if isinstance(outcome, WorkerFailure):
                raise outcome
            return outcome
