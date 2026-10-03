"""Bounded loopback RPC for the offline v2 financial owner.

Only reference-bearing actions cross this boundary. Each role holds its own
capability; the complete catalog belongs to the service. The RPC journal shares
one SQLite transaction with claim/renew. This deliberately uses the authority's
narrow private guard seam: calling its public methods inside ``atomic`` would
nest transactions. Provider execution stays in the unchanged authority executor.

A lost response is an unknown outcome, never permission to replace a request or
lease. Clients perform one transport attempt; exact retries have stable semantic
identities and fresh response-binding nonces. Live mode remains unavailable.
"""
from __future__ import annotations

from dataclasses import asdict
import hashlib
import hmac
import math
from pathlib import Path
import re
import socket
import socketserver
import sqlite3
import struct
import threading
import time
from typing import Any, Callable, cast
import uuid

from .ledger import ClaimRejected, Lease
from .peer_financial_authority_v2 import CumulativeAuthorityV2, FinancialError
from .peer_project_contract_v2 import ActionRequest, Context, DispatchReply, WorkKey, encode, from_dict, identity, to_dict
from .peer_store_v1 import canonical_bytes, strict_loads

PROTOCOL = "peer-financial-rpc-v2"
SCHEMA_VERSION = 2
MAX_PRINCIPALS = 64
MAX_HANDLERS = 32
MAX_REQUESTS_PER_PRINCIPAL = 4096
MAX_FRAME = 65_536
MAX_REQUEST = 16_384
MAX_RESPONSE = 32_768
DEADLINE_SECONDS = 2.0
_SAFE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,95}\Z")
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_NONCE = re.compile(r"[0-9a-f]{32}\Z")
_METHODS = {"claim", "renew", "submit", "lookup"}


class FinancialRPCError(ValueError):
    """Invalid RPC configuration, schema, authentication or retained identity."""


class FinancialDenied(FinancialRPCError):
    """An authenticated, definitive request rejection."""
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


class FinancialUnknownOutcome(FinancialRPCError):
    """No authenticated reply: retain this exact operation and payload."""
    def __init__(self, operation: str, request_id: str, payload: dict):
        self.operation, self.request_id = operation, request_id
        self.payload = strict_loads(_bytes(payload, MAX_REQUEST))
        super().__init__("No bound financial reply; preserve the exact request and lease")


def _bytes(value: Any, limit: int = MAX_FRAME) -> bytes:
    try:
        return canonical_bytes(value, max_bytes=limit)
    except (TypeError, ValueError) as error:
        raise FinancialRPCError("Invalid bounded RPC JSON") from error


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _name(value: Any) -> str:
    if type(value) is not str or _SAFE.fullmatch(value) is None:
        raise FinancialRPCError("Invalid RPC identifier")
    return value


def _digest(value: Any) -> str:
    if type(value) is not str or _SHA.fullmatch(value) is None:
        raise FinancialRPCError("Invalid RPC digest")
    return value


def _key(value: Any) -> str:
    if type(value) is not str or not 32 <= len(value) <= 128 or not value.isascii():
        raise FinancialRPCError("Invalid role capability")
    return value


def _ttl(value: Any) -> float:
    if type(value) not in (int, float) or not 0.05 <= value <= 3600 or not math.isfinite(value):
        raise FinancialRPCError("Invalid bounded lease duration")
    return float(value)


def _lease(value: Any) -> Lease:
    if type(value) is not dict or set(value) != {"task_id", "worker_id", "epoch", "expires_at"}:
        raise FinancialRPCError("Invalid lease fields")
    _name(value["task_id"])
    _name(value["worker_id"])
    if (type(value["epoch"]) is not int or not 1 <= value["epoch"] < 2**63
            or type(value["expires_at"]) not in (int, float)
            or not 0 < value["expires_at"] <= 1e12 or not math.isfinite(value["expires_at"])):
        raise FinancialRPCError("Invalid lease values")
    return Lease(**value)


def _signed(body: dict, key: str) -> dict:
    return {"body": body, "mac": hmac.new(key.encode(), _bytes(body), hashlib.sha256).hexdigest()}


def _verified(value: Any, key: str) -> dict:
    if type(value) is not dict or set(value) != {"body", "mac"} or type(value["body"]) is not dict:
        raise FinancialRPCError("Invalid authenticated envelope")
    _digest(value["mac"])
    if not hmac.compare_digest(value["mac"], _signed(value["body"], key)["mac"]):
        raise FinancialRPCError("Authentication failed")
    return value["body"]


def _record(record: Context | ActionRequest | DispatchReply, contract: str) -> dict:
    # Use shared record validation/encoding, but keep the RPC body JSON primitives.
    encode(record, execution_contract_sha256=contract)
    return to_dict(record)


class FinancialRPC:
    """Service-side durable adapter; does not own or close the financial owner."""
    def __init__(self, finance: CumulativeAuthorityV2, capabilities: dict[str, str], *,
                 request_guard: Callable[[ActionRequest], None], request_guard_sha256: str,
                 max_requests_per_principal: int = MAX_REQUESTS_PER_PRINCIPAL,
                 max_handlers: int = MAX_HANDLERS,
                 crash_hook: Callable[[str, str], None] | None = None):
        self._validate_finance(finance)
        if (type(capabilities) is not dict or not 1 <= len(capabilities) <= MAX_PRINCIPALS
                or set(capabilities) != finance.actors):
            raise FinancialRPCError("Capabilities must match the registered financial actor roster")
        self._keys = {_name(actor): _key(key) for actor, key in capabilities.items()}
        if len(set(self._keys.values())) != len(self._keys):
            raise FinancialRPCError("Every role needs a distinct capability")
        if (type(max_requests_per_principal) is not int
                or not 1 <= max_requests_per_principal <= MAX_REQUESTS_PER_PRINCIPAL
                or type(max_handlers) is not int or not 1 <= max_handlers <= MAX_HANDLERS):
            raise FinancialRPCError("Invalid RPC resource bounds")
        if not callable(request_guard):
            raise FinancialRPCError("Trusted exact evidence guard is required")
        self.request_guard = request_guard
        self.finance, self.max_handlers, self.crash_hook = finance, max_handlers, crash_hook
        self.contract_sha256 = _digest(finance.contract["execution_contract_sha256"])
        self.cohort = finance.cohort_id
        self.config = {"protocol": PROTOCOL, "schema_version": SCHEMA_VERSION,
                       "execution_contract_sha256": self.contract_sha256,
                       "financial_config_sha256": finance.config_sha256,
                       "request_guard_sha256": _digest(request_guard_sha256),
                       "source_sha256": _sha(Path(__file__).read_bytes()),
                       "capabilities": {actor: _sha(key.encode()) for actor, key in self._keys.items()},
                       "max_requests_per_principal": max_requests_per_principal,
                       "max_handlers": max_handlers, "max_frame": MAX_FRAME,
                       "max_request": MAX_REQUEST, "max_response": MAX_RESPONSE,
                       "deadline_seconds": DEADLINE_SECONDS,
                       "replay_policy": "exact-semantic-request-fresh-transport-nonce-v2"}
        self.config = self._finalize_config(self.config)
        self.config_sha256 = _sha(_bytes(self.config))
        with finance._active(), finance.ledger.atomic() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS financial_rpc_config_v2 (
                cohort TEXT PRIMARY KEY, config BLOB NOT NULL, config_sha TEXT NOT NULL,
                request_count INTEGER NOT NULL DEFAULT 0)""")
            db.execute("""CREATE TABLE IF NOT EXISTS financial_rpc_requests_v2 (
                cohort TEXT NOT NULL, actor TEXT NOT NULL, request_id TEXT NOT NULL,
                request BLOB NOT NULL, request_sha TEXT NOT NULL,
                receipt BLOB, receipt_sha TEXT,
                PRIMARY KEY(cohort,actor,request_id))""")
            prior = db.execute("SELECT * FROM financial_rpc_config_v2 WHERE cohort=?", (self.cohort,)).fetchone()
            if prior is None:
                if db.execute("SELECT 1 FROM financial_rpc_requests_v2 WHERE cohort=? LIMIT 1", (self.cohort,)).fetchone():
                    raise FinancialRPCError("Orphan RPC journal cannot be adopted")
                db.execute("INSERT INTO financial_rpc_config_v2 VALUES (?,?,?,0)",
                           (self.cohort, _bytes(self.config), self.config_sha256))
            elif prior["config"] != _bytes(self.config) or prior["config_sha"] != self.config_sha256:
                raise FinancialRPCError("Immutable RPC registration differs")
            self._registration(db)
            for row in db.execute("SELECT * FROM financial_rpc_requests_v2 WHERE cohort=?", (self.cohort,)):
                self._retained(row)
                semantic = strict_loads(row["request"], max_bytes=MAX_REQUEST)
                if (semantic["actor"] != row["actor"] or semantic["request_id"] != row["request_id"]
                        or semantic["operation"] not in {"claim", "renew", "submit"}):
                    raise FinancialRPCError("Retained RPC namespace differs")
                self._request(_signed({**semantic, "nonce": "0" * 32}, self._keys[row["actor"]]))
                if semantic["operation"] != "submit" and row["receipt"] is None:
                    raise FinancialRPCError("Missing atomic lease receipt")

    def _validate_finance(self, finance: CumulativeAuthorityV2) -> None:
        if type(finance) is not CumulativeAuthorityV2 or finance.config["mode"] != "offline":
            raise FinancialRPCError("An existing offline financial owner is required")

    def _finalize_config(self, config: dict) -> dict:
        """Versioned adapters bind their identity before RPC enrollment."""
        return config

    def _registration(self, db: sqlite3.Connection) -> None:
        row = db.execute("SELECT * FROM financial_rpc_config_v2 WHERE cohort=?", (self.cohort,)).fetchone()
        count = db.execute("SELECT COUNT(*) FROM financial_rpc_requests_v2 WHERE cohort=?", (self.cohort,)).fetchone()[0]
        if (row is None or row["config"] != _bytes(self.config) or row["config_sha"] != self.config_sha256
                or row["request_count"] != count):
            raise FinancialRPCError("Retained RPC registration or membership differs")

    def _boundary(self, point: str, request_id: str) -> None:
        if self.crash_hook is not None:
            self.crash_hook(point, request_id)

    def _request(self, envelope: Any) -> tuple[dict, dict, bytes]:
        _bytes(envelope)
        if type(envelope) is not dict or type(envelope.get("body")) is not dict:
            raise FinancialRPCError("Invalid RPC envelope")
        actor = _name(envelope["body"].get("actor"))
        if actor not in self._keys:
            raise FinancialRPCError("Unknown RPC principal")
        body = _verified(envelope, self._keys[actor])
        if (set(body) != {"protocol", "schema_version", "contract_sha256", "actor", "request_id", "nonce", "operation", "payload"}
                or body["protocol"] != PROTOCOL or type(body["schema_version"]) is not int
                or body["schema_version"] != SCHEMA_VERSION or body["contract_sha256"] != self.contract_sha256
                or type(body["nonce"]) is not str or _NONCE.fullmatch(body["nonce"]) is None
                or type(body["operation"]) is not str or body["operation"] not in _METHODS
                or type(body["payload"]) is not dict):
            raise FinancialRPCError("Invalid closed RPC request")
        _name(body["request_id"])
        _bytes(body, MAX_REQUEST)
        if (self.finance.config_sha256 != self.config["financial_config_sha256"]
                or self.finance.contract["execution_contract_sha256"] != self.contract_sha256):
            raise FinancialRPCError("Financial registration changed")
        payload, operation = body["payload"], body["operation"]
        parsed: dict[str, Any] = {}
        if operation == "claim":
            if set(payload) != {"context", "work", "ttl"}:
                raise FinancialRPCError("Invalid claim fields")
            parsed = {"context": from_dict(Context, payload["context"]),
                      "work": from_dict(WorkKey, payload["work"]), "ttl": _ttl(payload["ttl"])}
            _record(parsed["context"], self.contract_sha256)
        elif operation == "renew":
            if set(payload) != {"lease", "ttl"}:
                raise FinancialRPCError("Invalid renewal fields")
            parsed = {"lease": _lease(payload["lease"]), "ttl": _ttl(payload["ttl"])}
        elif operation == "submit":
            if set(payload) != {"action", "lease"}:
                raise FinancialRPCError("Invalid submit fields")
            parsed = {"action": from_dict(ActionRequest, payload["action"]), "lease": _lease(payload["lease"])}
            _record(parsed["action"], self.contract_sha256)
            if parsed["action"].request_id != body["request_id"] or parsed["action"].actor != actor:
                raise FinancialRPCError("Action differs from authenticated request")
        elif payload:
            raise FinancialRPCError("Lookup accepts only its own request identifier")
        if "lease" in parsed and parsed["lease"].worker_id != actor:
            raise FinancialRPCError("Lease differs from authenticated actor")
        semantic = {name: value for name, value in body.items() if name != "nonce"}
        return body, parsed, _bytes(semantic, MAX_REQUEST)

    def _row(self, db: sqlite3.Connection, actor: str, request_id: str) -> sqlite3.Row | None:
        self._registration(db)
        return db.execute("SELECT * FROM financial_rpc_requests_v2 WHERE cohort=? AND actor=? AND request_id=?",
                          (self.cohort, actor, request_id)).fetchone()

    def _retained(self, row: sqlite3.Row, raw: bytes | None = None) -> dict | None:
        if _sha(row["request"]) != row["request_sha"]:
            raise FinancialRPCError("Retained RPC identity checksum differs")
        if raw is not None and row["request"] != raw:
            raise FinancialDenied("request_identity_conflict")
        if row["receipt"] is None:
            if row["receipt_sha"] is not None:
                raise FinancialRPCError("Incomplete RPC receipt")
            return None
        if self._receipt_digest(row["request_sha"], row["receipt"]) != row["receipt_sha"]:
            raise FinancialRPCError("Retained RPC receipt binding differs")
        result = strict_loads(row["receipt"], max_bytes=MAX_RESPONSE)
        if type(result) is not dict:
            raise FinancialRPCError("Invalid retained RPC receipt")
        if result.get("status") == "denied":
            if set(result) != {"status", "reason"}:
                raise FinancialRPCError("Invalid retained rejection")
            _name(result["reason"])
        elif set(result) == {"status", "kind", "body"} and result["status"] == "ok" and result["kind"] == "lease":
            lease = _lease(result["body"])
            semantic = strict_loads(row["request"], max_bytes=MAX_REQUEST)
            if semantic["operation"] == "claim":
                task = self.finance.task_id(from_dict(Context, semantic["payload"]["context"]),
                                            from_dict(WorkKey, semantic["payload"]["work"]))
                valid = lease.task_id == task and lease.worker_id == row["actor"]
            elif semantic["operation"] == "renew":
                original = _lease(semantic["payload"]["lease"])
                valid = (lease.task_id, lease.worker_id, lease.epoch) == (original.task_id, row["actor"], original.epoch)
            else:
                valid = False
            if not valid:
                raise FinancialRPCError("Retained lease request binding differs")
        else:
            raise FinancialRPCError("Invalid retained receipt schema")
        return result

    def _receipt_digest(self, request_sha: str, receipt: bytes) -> str:
        return _sha(_bytes({"config_sha256": self.config_sha256, "request_sha256": request_sha,
                           "receipt": strict_loads(receipt, max_bytes=MAX_RESPONSE)}))

    def _insert(self, db: sqlite3.Connection, actor: str, request_id: str, raw: bytes) -> None:
        count = db.execute("SELECT COUNT(*) FROM financial_rpc_requests_v2 WHERE cohort=? AND actor=?",
                           (self.cohort, actor)).fetchone()[0]
        if count >= self.config["max_requests_per_principal"]:
            raise FinancialDenied("request_quota")
        db.execute("INSERT INTO financial_rpc_requests_v2 VALUES (?,?,?,?,?,NULL,NULL)",
                   (self.cohort, actor, request_id, raw, _sha(raw)))
        db.execute("UPDATE financial_rpc_config_v2 SET request_count=request_count+1 WHERE cohort=?", (self.cohort,))

    def _save(self, db: sqlite3.Connection, actor: str, request_id: str, receipt: dict) -> None:
        raw = _bytes(receipt, MAX_RESPONSE)
        row = self._row(db, actor, request_id)
        if row is None:
            raise FinancialRPCError("Missing RPC intent")
        db.execute("UPDATE financial_rpc_requests_v2 SET receipt=?,receipt_sha=? WHERE cohort=? AND actor=? AND request_id=?",
                   (raw, self._receipt_digest(row["request_sha"], raw), self.cohort, actor, request_id))

    def _lease_operation(self, body: dict, parsed: dict, raw: bytes) -> dict:
        actor, request_id = body["actor"], body["request_id"]
        finance = self.finance
        with finance._active(), finance.ledger.atomic() as db:
            row = self._row(db, actor, request_id)
            if row is not None:
                retained = self._retained(row, raw)
                if retained is None:
                    raise FinancialRPCError("Lease receipt missing from atomic journal")
                return retained
            self._insert(db, actor, request_id, raw)
            # Guard sequence is the unchanged CumulativeAuthorityV2 lease seam.
            try:
                finance._actor(actor)
                if finance._halted(db):
                    raise FinancialDenied("cohort_halted")
                if body["operation"] == "claim":
                    task_id = finance.task_id(parsed["context"], parsed["work"])
                    if task_id not in finance.task_specs or actor not in finance.task_specs[task_id]["actors"]:
                        raise FinancialDenied("task_scope")
                    finance._guard_task(db, task_id)
                    lease = finance.ledger.claim(task_id, actor, now=finance._now(db), ttl=parsed["ttl"])
                else:
                    original = parsed["lease"]
                    if original.task_id not in finance.task_specs or actor not in finance.task_specs[original.task_id]["actors"]:
                        raise FinancialDenied("task_scope")
                    lease = finance.ledger.renew(original, now=finance._now(db), ttl=parsed["ttl"])
                receipt = {"status": "ok", "kind": "lease", "body": asdict(lease)}
            except (FinancialDenied, ClaimRejected) as error:
                receipt = {"status": "denied", "reason": error.reason if isinstance(error, FinancialDenied) else "claim_rejected"}
            self._save(db, actor, request_id, receipt)
            self._boundary("before_lease_commit", request_id)
        self._boundary("after_lease_commit", request_id)
        return receipt

    def _dispatch(self, body: dict, parsed: dict, raw: bytes) -> dict:
        actor, request_id = body["actor"], body["request_id"]
        with self.finance._active(), self.finance.ledger.atomic() as db:
            row = self._row(db, actor, request_id)
            if row is None:
                self._insert(db, actor, request_id, raw)
            else:
                self._retained(row, raw)
        self._boundary("after_submit_intent", request_id)
        with self.finance.ledger.atomic() as db:
            financial = self.finance._row(db, actor, request_id)
            if financial is not None:
                expected = _bytes({"action": to_dict(parsed["action"]), "lease": asdict(parsed["lease"])})
                if financial["identity"] != expected:
                    raise FinancialDenied("request_identity_conflict")
            admitted = financial is not None and self.finance._reply(financial).state != "waiting"
        if not admitted:
            try:
                self.request_guard(parsed["action"])
            except FileNotFoundError:
                waiting = DispatchReply(request_id, identity(parsed["action"]), "waiting", "payload_unavailable")
                return {"status": "ok", "kind": "dispatch", "body": _record(waiting, self.contract_sha256)}
        # Never enclose submit in our transaction: admission and executor workers
        # own their existing transactions and never wait for provider completion.
        reply = self.finance.submit(actor, parsed["action"], parsed["lease"])
        if reply.state in {"completed", "failed"}:
            checked = self.finance.lookup(actor, request_id)
            if checked is None:
                raise FinancialRPCError("Admitted reply disappeared")
            reply = checked
        return {"status": "ok", "kind": "dispatch", "body": _record(reply, self.contract_sha256)}

    def _lookup(self, actor: str, request_id: str) -> dict:
        with self.finance._active(), self.finance.ledger.atomic() as db:
            row = self._row(db, actor, request_id)
            if row is None:
                return {"status": "ok", "kind": "absent", "body": None}
            self._retained(row)
            semantic = strict_loads(row["request"], max_bytes=MAX_REQUEST)
            if semantic["operation"] != "submit":
                return {"status": "ok", "kind": "absent", "body": None}
            financial = self.finance._row(db, actor, request_id)
            if financial is None:
                return {"status": "ok", "kind": "absent", "body": None}
            if financial["identity"] != _bytes(semantic["payload"]):
                raise FinancialRPCError("Dispatch lookup identity differs")
        reply = self.finance.lookup(actor, request_id)
        if reply is None:
            return {"status": "ok", "kind": "absent", "body": None}
        return {"status": "ok", "kind": "dispatch", "body": _record(reply, self.contract_sha256)}

    def handle(self, envelope: Any) -> dict:
        body, parsed, raw = self._request(envelope)
        try:
            if body["operation"] in {"claim", "renew"}:
                receipt = self._lease_operation(body, parsed, raw)
            elif body["operation"] == "submit":
                with self.finance._active():
                    receipt = self._dispatch(body, parsed, raw)
            else:
                receipt = self._lookup(body["actor"], body["request_id"])
        except FinancialDenied as error:
            receipt = {"status": "denied", "reason": error.reason}
        _bytes(receipt, MAX_RESPONSE)
        response = {"protocol": PROTOCOL, "schema_version": SCHEMA_VERSION,
                    "contract_sha256": self.contract_sha256, "actor": body["actor"],
                    "request_id": body["request_id"], "nonce": body["nonce"],
                    "rpc_sha256": _sha(_bytes(body, MAX_REQUEST)), "receipt": receipt}
        self._boundary("before_send", body["request_id"])
        return _signed(response, self._keys[body["actor"]])


def _remaining(deadline: float) -> float:
    left = deadline - time.monotonic()
    if left <= 0:
        raise TimeoutError("Financial RPC deadline")
    return left


def _read_exact(stream: socket.socket, size: int, deadline: float) -> bytes:
    parts = []
    while size:
        stream.settimeout(_remaining(deadline))
        part = stream.recv(min(size, 8192))
        if not part:
            raise FinancialRPCError("Incomplete RPC frame")
        parts.append(part)
        size -= len(part)
    return b"".join(parts)


def _receive(stream: socket.socket, deadline: float) -> dict:
    size = struct.unpack("!I", _read_exact(stream, 4, deadline))[0]
    if not 1 <= size <= MAX_FRAME:
        raise FinancialRPCError("RPC frame limit")
    value = strict_loads(_read_exact(stream, size, deadline), max_bytes=MAX_FRAME)
    if type(value) is not dict:
        raise FinancialRPCError("Invalid RPC frame")
    return value


def _send(stream: socket.socket, value: dict, deadline: float) -> None:
    raw = _bytes(value)
    stream.settimeout(_remaining(deadline))
    stream.sendall(struct.pack("!I", len(raw)) + raw)


class FinancialClient:
    """One role's capability and a single-attempt typed RPC client."""
    def __init__(self, port: int, actor: str, key: str, contract_sha256: str, *, timeout: float = DEADLINE_SECONDS):
        if type(port) is not int or not 1 <= port <= 65535:
            raise FinancialRPCError("Invalid service port")
        if type(timeout) not in (int, float) or not 0.05 <= timeout <= 10 or not math.isfinite(timeout):
            raise FinancialRPCError("Invalid RPC timeout")
        self.port, self.actor, self._key = port, _name(actor), _key(key)
        self.contract_sha256, self.timeout = _digest(contract_sha256), float(timeout)

    def _call(self, operation: str, request_id: str, payload: dict) -> dict:
        _name(request_id)
        body = {"protocol": PROTOCOL, "schema_version": SCHEMA_VERSION,
                "contract_sha256": self.contract_sha256, "actor": self.actor,
                "request_id": request_id, "nonce": uuid.uuid4().hex,
                "operation": operation, "payload": payload}
        _bytes(body, MAX_REQUEST)
        deadline = time.monotonic() + self.timeout
        try:
            with socket.create_connection(("127.0.0.1", self.port), timeout=_remaining(deadline)) as stream:
                _send(stream, _signed(body, self._key), deadline)
                response = _verified(_receive(stream, deadline), self._key)
            if (set(response) != {"protocol", "schema_version", "contract_sha256", "actor", "request_id", "nonce", "rpc_sha256", "receipt"}
                    or any(response[name] != body[name] for name in ("protocol", "schema_version", "contract_sha256", "actor", "request_id", "nonce"))
                    or type(response["schema_version"]) is not int
                    or response["rpc_sha256"] != _sha(_bytes(body, MAX_REQUEST))
                    or type(response["receipt"]) is not dict):
                raise FinancialRPCError("Reply does not bind this exact RPC")
            receipt = response["receipt"]
            _bytes(receipt, MAX_RESPONSE)
            if receipt.get("status") == "denied":
                if set(receipt) != {"status", "reason"}:
                    raise FinancialRPCError("Malformed denial")
                raise FinancialDenied(_name(receipt["reason"]))
            if (set(receipt) != {"status", "kind", "body"} or receipt["status"] != "ok"
                    or receipt["kind"] not in {"lease", "dispatch", "absent"}):
                raise FinancialRPCError("Malformed RPC receipt")
            return receipt
        except FinancialDenied:
            raise
        except (OSError, ValueError, TypeError, KeyError) as error:
            raise FinancialUnknownOutcome(operation, request_id, payload) from error

    def claim(self, context: Context, work: WorkKey, request_id: str, *, ttl: float = 60) -> Lease:
        payload = {"context": _record(context, self.contract_sha256), "work": to_dict(work), "ttl": _ttl(ttl)}
        receipt = self._call("claim", request_id, payload)
        try:
            if receipt["kind"] != "lease":
                raise FinancialRPCError("Expected a lease")
            lease = _lease(receipt["body"])
            task_id = "financial-v2-" + _sha(_bytes({"context": payload["context"], "work": payload["work"]}))
            if lease.worker_id != self.actor or lease.task_id != task_id:
                raise FinancialRPCError("Lease recipient or task differs")
            return lease
        except ValueError as error:
            raise FinancialUnknownOutcome("claim", request_id, payload) from error

    def renew(self, lease: Lease, request_id: str, *, ttl: float = 60) -> Lease:
        payload = {"lease": asdict(_lease(asdict(lease))), "ttl": _ttl(ttl)}
        receipt = self._call("renew", request_id, payload)
        try:
            if receipt["kind"] != "lease":
                raise FinancialRPCError("Expected a lease")
            renewed = _lease(receipt["body"])
            if (renewed.task_id, renewed.worker_id, renewed.epoch) != (lease.task_id, self.actor, lease.epoch):
                raise FinancialRPCError("Renewed lease identity differs")
            return renewed
        except ValueError as error:
            raise FinancialUnknownOutcome("renew", request_id, payload) from error

    def submit(self, action: ActionRequest, lease: Lease) -> DispatchReply:
        payload = {"action": _record(action, self.contract_sha256), "lease": asdict(_lease(asdict(lease)))}
        receipt = self._call("submit", action.request_id, payload)
        reply = self._dispatch_reply(receipt, "submit", action.request_id, payload)
        if (reply.action_sha256 != identity(action) or (reply.binding is not None
                and (reply.binding.action != action or reply.binding.lease != lease))):
            raise FinancialUnknownOutcome("submit", action.request_id, payload)
        return reply

    def _dispatch_reply(self, receipt: dict, operation: str, request_id: str, payload: dict) -> DispatchReply:
        try:
            if receipt["kind"] != "dispatch":
                raise FinancialRPCError("Expected a dispatch reply")
            reply = from_dict(DispatchReply, receipt["body"])
            _record(reply, self.contract_sha256)
            if reply.request_id != request_id or (reply.binding is not None and reply.binding.action.actor != self.actor):
                raise FinancialRPCError("Dispatch reply recipient differs")
            return reply
        except ValueError as error:
            raise FinancialUnknownOutcome(operation, request_id, payload) from error

    def lookup(self, request_id: str) -> DispatchReply | None:
        receipt = self._call("lookup", request_id, {})
        if receipt["kind"] == "absent" and receipt["body"] is None:
            return None
        return self._dispatch_reply(receipt, "lookup", request_id, {})


class FinancialServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    """Bounded short RPC handlers. Stop/join this server before closing finance."""
    allow_reuse_address = True
    daemon_threads = False
    block_on_close = True
    request_queue_size = MAX_HANDLERS

    def __init__(self, port: int, rpc: FinancialRPC):
        if type(port) is not int or not 0 <= port <= 65535:
            raise FinancialRPCError("Invalid service port")
        self.rpc = rpc
        self.slots = threading.BoundedSemaphore(rpc.max_handlers)
        super().__init__(("127.0.0.1", port), _Handler)

    def readiness(self) -> dict:
        return {"protocol": PROTOCOL, "schema_version": SCHEMA_VERSION,
                "port": self.server_address[1], "contract_sha256": self.rpc.contract_sha256,
                "config_sha256": self.rpc.config_sha256, "principals": len(self.rpc._keys),
                "max_handlers": self.rpc.max_handlers}

    def process_request(self, stream: socket.socket | tuple[bytes, socket.socket], address: Any) -> None:
        if not self.slots.acquire(blocking=False):
            self.shutdown_request(stream)
            return
        try:
            super().process_request(stream, address)
        except BaseException:
            self.slots.release()
            raise

    def process_request_thread(self, stream: socket.socket | tuple[bytes, socket.socket], address: Any) -> None:
        try:
            super().process_request_thread(stream, address)
        finally:
            self.slots.release()


class _Handler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        deadline = time.monotonic() + DEADLINE_SECONDS
        try:
            reply = cast(FinancialServer, self.server).rpc.handle(_receive(self.request, deadline))
            _send(self.request, reply, deadline)
        except (OSError, ValueError, TypeError, KeyError, sqlite3.Error):
            return  # No stack, path, capability catalog or accounting internals on wire.
