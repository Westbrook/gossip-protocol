"""Single-host authenticated lease and zero-provider dispatch authority.

No task selection, provider implementation, peer settlement, candidate execution
or publication is exposed. The existing Ledger runs inside the same SQLite
transaction as the request journal; no database/Git atomicity is claimed.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import asdict
import fcntl
import hashlib
import hmac
import json
import math
import os
from pathlib import Path
import re
import socket
import socketserver
import sqlite3
import struct
import threading
import time
from typing import Any, Callable, Iterator, cast

from .ledger import BudgetExceeded, ClaimRejected, Ledger, Lease
from .peer_store_v1 import canonical_bytes, strict_loads

PROTOCOL = "peer-authority-v1"
MAX_FRAME = 65_536
MAX_REQUEST = 16_384
MAX_RESPONSE = 24_576
MAX_REQUESTS_PER_PRINCIPAL = 256
MAX_CONNECTIONS = 4
DEADLINE_SECONDS = 2.0
_SAFE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,95}\Z")
_SHA = re.compile(r"[0-9a-f]{64}\Z")


class AuthorityError(ValueError):
    """Invalid local protocol/authentication or corrupt trusted authority state."""


class _Denied(Exception):
    def __init__(self, reason: str):
        self.reason = reason


def _bytes(value: Any, limit: int = MAX_FRAME) -> bytes:
    try:
        return canonical_bytes(value, max_bytes=limit)
    except (ValueError, TypeError) as error:
        raise AuthorityError("Invalid bounded JSON") from error


def request_digest(request: dict) -> str:
    return hashlib.sha256(_bytes(request, MAX_REQUEST)).hexdigest()


def _identifier(value: Any) -> str:
    if type(value) is not str or _SAFE.fullmatch(value) is None:
        raise AuthorityError("Invalid identifier")
    return value


def _signed(body: dict, key: str) -> dict:
    return {"body": body, "mac": hmac.new(key.encode(), _bytes(body), hashlib.sha256).hexdigest()}


def _verified(envelope: Any, key: str) -> dict:
    if type(envelope) is not dict or set(envelope) != {"body", "mac"} or type(envelope["body"]) is not dict:
        raise AuthorityError("Invalid authenticated envelope")
    mac = envelope["mac"]
    if type(mac) is not str or _SHA.fullmatch(mac) is None or not hmac.compare_digest(mac, _signed(envelope["body"], key)["mac"]):
        raise AuthorityError("Authentication failed")
    return envelope["body"]


def _configuration(value: Any) -> dict:
    fields = {"protocol", "run_id", "budget_units", "lease_seconds", "principals", "tasks"}
    if type(value) is not dict or set(value) != fields or value["protocol"] != PROTOCOL:
        raise AuthorityError("Invalid authority configuration")
    _identifier(value["run_id"])
    if type(value["budget_units"]) is not int or not 0 <= value["budget_units"] <= 1_000_000_000:
        raise AuthorityError("Invalid generic budget")
    ttl = value["lease_seconds"]
    if type(ttl) not in (int, float) or not math.isfinite(ttl) or not 0.05 <= ttl <= 3600:
        raise AuthorityError("Invalid authority lease duration")
    tasks, principals = value["tasks"], value["principals"]
    if type(tasks) is not dict or not 1 <= len(tasks) <= 64:
        raise AuthorityError("Invalid fixed task catalog")
    for task, spec in tasks.items():
        _identifier(task)
        if (type(spec) is not dict or set(spec) != {"fixture", "reservation_units"}
                or spec["fixture"] not in {"text_build", "text_review"}
                or type(spec["reservation_units"]) is not int
                or not 0 <= spec["reservation_units"] <= 1_000_000):
            raise AuthorityError("Invalid fixed fixture")
    if type(principals) is not dict or not 1 <= len(principals) <= 16:
        raise AuthorityError("Invalid principal roster")
    keys = []
    for name, spec in principals.items():
        _identifier(name)
        if (type(spec) is not dict or set(spec) != {"key", "tasks"}
                or type(spec["key"]) is not str or not 32 <= len(spec["key"]) <= 128
                or type(spec["tasks"]) is not list or not 1 <= len(spec["tasks"]) <= 64
                or any(type(task) is not str or task not in tasks for task in spec["tasks"])
                or len(set(spec["tasks"])) != len(spec["tasks"])):
            raise AuthorityError("Invalid principal capability")
        keys.append(spec["key"])
    if len(set(keys)) != len(keys):
        raise AuthorityError("Principal keys must be distinct")
    return strict_loads(_bytes(value))


def config_digest(config: dict) -> str:
    safe = _configuration(config)
    for principal in safe["principals"].values():
        principal["key"] = hashlib.sha256(principal["key"].encode()).hexdigest()
    return hashlib.sha256(_bytes(safe)).hexdigest()


def _fixture_request(value: Any) -> dict:
    _bytes(value, MAX_REQUEST)
    if (type(value) is not dict or set(value) != {"text", "context"}
            or type(value["text"]) is not str or len(value["text"].encode("utf-8")) > 4096):
        raise _Denied("malformed")
    context = value["context"]
    if (type(context) is not dict or set(context) != {"source_sha256", "event_ids"}
            or type(context["source_sha256"]) is not str or _SHA.fullmatch(context["source_sha256"]) is None
            or type(context["event_ids"]) is not list or len(context["event_ids"]) > 32
            or any(type(item) is not str or _SHA.fullmatch(item) is None for item in context["event_ids"])
            or len(set(context["event_ids"])) != len(context["event_ids"])):
        raise _Denied("malformed")
    if context["source_sha256"] != hashlib.sha256(value["text"].encode("utf-8")).hexdigest():
        raise _Denied("source_digest")
    return value


def _fixture(kind: str, request: dict) -> dict:
    text = request["text"]
    if kind == "text_build":
        result = text.upper()
        return {"text": result, "text_sha256": hashlib.sha256(result.encode()).hexdigest()}
    nonempty, uppercase = bool(text), text == text.upper()
    return {"text_sha256": hashlib.sha256(text.encode()).hexdigest(),
            "nonempty": nonempty, "is_uppercase": uppercase,
            "decision": "approve" if nonempty and uppercase else "reject"}


class _JoinedLedger(Ledger):
    """Reuse frozen Ledger mutations on one enclosing journal transaction.

    Only mutation methods are called while joined. Inherited read-session methods
    are intentionally not used there: they would create another connection.
    """
    def __init__(self, path: Path, budget: int):
        self.context = threading.local()
        super().__init__(path, budget)

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        current = getattr(self.context, "db", None)
        if current is not None:
            yield current
        else:
            with super()._transaction() as db:
                yield db

    @contextmanager
    def atomic(self) -> Iterator[sqlite3.Connection]:
        if getattr(self.context, "db", None) is not None:
            raise AuthorityError("Nested authority transaction")
        with super()._transaction() as db:
            self.context.db = db
            try:
                yield db
            finally:
                del self.context.db


class Authority:
    def __init__(self, root: Path, config: dict, *, clock: Callable[[], float] = time.time,
                 crash_hook: Callable[[str, str], None] | None = None):
        self.config = _configuration(config)
        self.config_sha256 = config_digest(config)
        self.root, self.clock, self.crash_hook = Path(root), clock, crash_hook
        self.lifecycle = threading.Condition()
        self.active_handlers = 0
        self.closing = False
        self.closed = False
        self.root.mkdir(parents=True, exist_ok=True)
        self.owner = (self.root / "authority.lock").open("a")
        try:
            fcntl.flock(self.owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.owner.close()
            raise AuthorityError("Authority already has an exclusive owner") from None
        try:
            path = self.root / "authority.sqlite"
            if path.exists():
                db = sqlite3.connect(path)
                try:
                    tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                finally:
                    db.close()
                authority_tables = {"authority_meta", "authority_requests", "authority_actions", "authority_invocations"}
                ledger_tables = {"settings", "tasks", "dependencies", "reservations", "intents", "budget_changes"}
                if tables & authority_tables and not (authority_tables | ledger_tables) <= tables:
                    raise AuthorityError("Incomplete authority schema")
            self.ledger = _JoinedLedger(path, self.config["budget_units"])
            self._bootstrap()
        except BaseException:
            self.owner.close()
            raise

    def close(self) -> None:
        with self.lifecycle:
            self.closing = True
            while self.active_handlers:
                self.lifecycle.wait()
            if not self.closed:
                self.owner.close()
                self.closed = True

    def _receipt(self, **value: Any) -> dict:
        return {"run_id": self.config["run_id"], "config_sha256": self.config_sha256, **value}

    def _now(self, db: sqlite3.Connection) -> float:
        sampled = self.clock()  # Called only after BEGIN IMMEDIATE owns the lock.
        if type(sampled) not in (int, float) or not math.isfinite(sampled) or not 0 <= sampled <= 1e12:
            raise AuthorityError("Invalid authority clock")
        previous = float(db.execute("SELECT value FROM authority_meta WHERE key='clock'").fetchone()[0])
        now = max(float(sampled), previous)
        db.execute("UPDATE authority_meta SET value=? WHERE key='clock'", (str(now),))
        return now

    def _bootstrap(self) -> None:
        with self.ledger.atomic() as db:
            db.execute("CREATE TABLE IF NOT EXISTS authority_meta (key TEXT PRIMARY KEY,value TEXT NOT NULL)")
            db.execute("""CREATE TABLE IF NOT EXISTS authority_requests (
                principal TEXT NOT NULL, request_id TEXT NOT NULL, sha TEXT NOT NULL,
                body BLOB NOT NULL, receipt BLOB NOT NULL, receipt_sha TEXT NOT NULL,
                PRIMARY KEY(principal,request_id))""")
            db.execute("""CREATE TABLE IF NOT EXISTS authority_actions (
                action_id TEXT NOT NULL, principal TEXT NOT NULL, request_id TEXT NOT NULL,
                task_id TEXT NOT NULL, epoch INTEGER NOT NULL, request_sha TEXT NOT NULL,
                reservation_id TEXT NOT NULL UNIQUE, state TEXT NOT NULL,
                PRIMARY KEY(principal,action_id), UNIQUE(principal,request_id))""")
            db.execute("""CREATE TABLE IF NOT EXISTS authority_invocations (
                principal TEXT NOT NULL, action_id TEXT NOT NULL, request_sha TEXT NOT NULL,
                fixture TEXT NOT NULL, entered_at REAL NOT NULL,
                PRIMARY KEY(principal,action_id))""")
            prior = db.execute("SELECT value FROM authority_meta WHERE key='config_sha256'").fetchone()
            if prior is None:
                # A crashed empty Ledger bootstrap can resume; adopted work cannot.
                if any(db.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone()
                       for table in ("tasks", "reservations", "intents", "authority_requests", "authority_actions", "authority_invocations")):
                    raise AuthorityError("Cannot adopt an existing work ledger")
                db.executemany("INSERT INTO authority_meta VALUES (?,?)", [
                    ("config_sha256", self.config_sha256), ("clock", "0")])
                for task in self.config["tasks"]:
                    self.ledger.add_task(task)
            elif prior[0] != self.config_sha256:
                raise AuthorityError("Authority configuration changed across restart")
            now = self._now(db)
            for action in db.execute("SELECT * FROM authority_actions WHERE state='dispatching'").fetchall():
                row = db.execute("SELECT * FROM authority_requests WHERE principal=? AND request_id=?",
                                 (action["principal"], action["request_id"])).fetchone()
                receipt = self._read_receipt(row)
                if receipt["status"] != "dispatching":
                    raise AuthorityError("Dispatch journal is inconsistent")
                receipt.update(status="unknown", authority_time=now)
                self._update_receipt(db, action["principal"], action["request_id"], receipt)
                db.execute("UPDATE authority_actions SET state='unknown' WHERE principal=? AND action_id=?",
                           (action["principal"], action["action_id"]))

    def _read_receipt(self, row: sqlite3.Row | None) -> dict:
        if row is None:
            raise AuthorityError("Missing durable request")
        if (hashlib.sha256(row["body"]).hexdigest() != row["sha"]
                or hashlib.sha256(row["receipt"]).hexdigest() != row["receipt_sha"]):
            raise AuthorityError("Durable request checksum mismatch")
        receipt = strict_loads(row["receipt"], max_bytes=MAX_RESPONSE)
        if (type(receipt) is not dict or receipt.get("run_id") != self.config["run_id"]
                or receipt.get("config_sha256") != self.config_sha256):
            raise AuthorityError("Durable receipt identity mismatch")
        return receipt

    @staticmethod
    def _update_receipt(db: sqlite3.Connection, principal: str, request_id: str, receipt: dict) -> None:
        data = _bytes(receipt, MAX_RESPONSE)
        db.execute("UPDATE authority_requests SET receipt=?,receipt_sha=? WHERE principal=? AND request_id=?",
                   (data, hashlib.sha256(data).hexdigest(), principal, request_id))

    def _boundary(self, point: str, request_id: str) -> None:
        if self.crash_hook:
            self.crash_hook(point, request_id)

    def handle(self, envelope: Any) -> dict:
        with self.lifecycle:
            if self.closing:
                raise AuthorityError("Authority is closed")
            self.active_handlers += 1
        try:
            return self._handle(envelope)
        finally:
            with self.lifecycle:
                self.active_handlers -= 1
                self.lifecycle.notify_all()

    def _handle(self, envelope: Any) -> dict:
        try:
            principal = envelope["body"]["principal"]
            spec = self.config["principals"].get(principal)
        except (KeyError, TypeError):
            raise AuthorityError("Authentication failed") from None
        if spec is None:
            raise AuthorityError("Authentication failed")
        body = _verified(envelope, spec["key"])
        if (set(body) != {"protocol", "run_id", "principal", "request_id", "operation", "payload"}
                or body["protocol"] != PROTOCOL or body["run_id"] != self.config["run_id"]
                or type(body["operation"]) is not str or type(body["payload"]) is not dict):
            raise AuthorityError("Invalid request envelope")
        _identifier(body["request_id"])
        _bytes(body, MAX_REQUEST + 2048)
        receipt = self._process(body)
        answer = {"protocol": PROTOCOL, "run_id": self.config["run_id"], "principal": principal,
                  "request_id": body["request_id"], "rpc_sha256": hashlib.sha256(_bytes(body)).hexdigest(),
                  "receipt": receipt}
        _bytes(answer, MAX_RESPONSE + 2048)
        return _signed(answer, spec["key"])

    def _process(self, body: dict) -> dict:
        principal, request_id = body["principal"], body["request_id"]
        operation, payload = body["operation"], body["payload"]
        raw = _bytes(body, MAX_REQUEST + 2048)
        sha = hashlib.sha256(raw).hexdigest()
        dispatch = False
        with self.ledger.atomic() as db:
            now = self._now(db)
            if operation == "lookup":
                if set(payload) != {"request_id"}:
                    return self._receipt(status="rejected", reason="malformed")
                try:
                    target = _identifier(payload["request_id"])
                except AuthorityError:
                    return self._receipt(status="rejected", reason="malformed")
                row = db.execute("SELECT * FROM authority_requests WHERE principal=? AND request_id=?",
                                 (principal, target)).fetchone()
                return self._read_receipt(row) if row else self._receipt(status="missing")
            prior = db.execute("SELECT * FROM authority_requests WHERE principal=? AND request_id=?",
                               (principal, request_id)).fetchone()
            if prior is not None:
                if prior["sha"] != sha:
                    return self._receipt(status="rejected", reason="request_conflict")
                return self._read_receipt(prior)
            count = db.execute("SELECT COUNT(*) FROM authority_requests WHERE principal=?", (principal,)).fetchone()[0]
            if count >= MAX_REQUESTS_PER_PRINCIPAL:
                return self._receipt(status="rejected", reason="capacity")
            db.execute("SAVEPOINT authority_action")
            try:
                receipt, dispatch = self._operate(db, principal, request_id, operation, payload, now)
            except (_Denied, ClaimRejected, BudgetExceeded, AuthorityError) as error:
                db.execute("ROLLBACK TO authority_action")
                reason = error.reason if isinstance(error, _Denied) else (
                    "budget" if isinstance(error, BudgetExceeded) else
                    "unavailable" if isinstance(error, ClaimRejected) else "malformed")
                receipt = self._receipt(status="rejected", reason=reason)
            db.execute("RELEASE authority_action")
            response = _bytes(receipt, MAX_RESPONSE)  # Bound before any commit.
            db.execute("INSERT INTO authority_requests VALUES (?,?,?,?,?,?)",
                       (principal, request_id, sha, raw, response, hashlib.sha256(response).hexdigest()))
            self._boundary("before_commit", request_id)
        if dispatch:
            self._boundary("after_intent", request_id)
            kind = self.config["tasks"][payload["task_id"]]["fixture"]
            with self.ledger.atomic() as db:
                entered_at = self._now(db)
                db.execute("INSERT INTO authority_invocations VALUES (?,?,?,?,?)",
                           (principal, payload["action_id"], payload["request_sha256"], kind, entered_at))
            self._boundary("after_entry", request_id)
            result = _fixture(kind, payload["request"])
            self._boundary("after_fixture", request_id)
            with self.ledger.atomic() as db:
                now = self._now(db)
                # Only this service's initial dispatch path owns invocation. RPC
                # replays return the durable state and never enter this block.
                row = db.execute("SELECT * FROM authority_requests WHERE principal=? AND request_id=?",
                                 (principal, request_id)).fetchone()
                receipt = self._read_receipt(row)
                if receipt["status"] != "dispatching":
                    raise AuthorityError("Dispatch outcome changed unexpectedly")
                receipt.update(status="completed", result=result, usage_units=0, authority_time=now)
                _bytes(receipt, MAX_RESPONSE)
                self.ledger.settle(receipt["reservation_id"], 0)
                self._update_receipt(db, principal, request_id, receipt)
                db.execute("UPDATE authority_actions SET state='completed' WHERE principal=? AND action_id=?",
                           (principal, payload["action_id"]))
                self._boundary("before_result_commit", request_id)
        self._boundary("after_commit", request_id)
        return receipt

    def _operate(self, db: sqlite3.Connection, principal: str, request_id: str,
                 operation: str, payload: dict, now: float) -> tuple[dict, bool]:
        shapes = {"claim": {"task_id"}, "renew": {"task_id", "epoch"}, "validate": {"task_id", "epoch"},
                  "dispatch": {"task_id", "epoch", "action_id", "request_sha256", "request"}}
        if operation not in shapes:
            raise _Denied("forbidden")
        if set(payload) != shapes[operation]:
            raise _Denied("malformed")
        task = _identifier(payload["task_id"])
        if task not in self.config["principals"][principal]["tasks"]:
            raise _Denied("forbidden")
        if operation == "claim":
            if db.execute("SELECT 1 FROM authority_actions WHERE task_id=? AND state IN ('dispatching','unknown') LIMIT 1", (task,)).fetchone():
                raise _Denied("unavailable")
            lease = self.ledger.claim(task, principal, now=now, ttl=self.config["lease_seconds"])
        else:
            epoch = payload["epoch"]
            if type(epoch) is not int or not 1 <= epoch <= (1 << 63) - 1:
                raise _Denied("malformed")
            lease = Lease(task, principal, epoch, 0)
            row = Ledger._validate(db, lease, now)
            if operation == "renew":
                lease = self.ledger.renew(lease, now=now, ttl=self.config["lease_seconds"])
            else:
                lease = Lease(task, principal, epoch, row["expires"])
            if operation == "dispatch":
                action = _identifier(payload["action_id"])
                supplied = payload["request_sha256"]
                request = _fixture_request(payload["request"])
                if type(supplied) is not str or supplied != request_digest(request):
                    raise _Denied("request_digest")
                if db.execute("SELECT 1 FROM authority_actions WHERE principal=? AND action_id=?", (principal, action)).fetchone():
                    raise _Denied("action_conflict")
                if db.execute("SELECT 1 FROM authority_actions WHERE task_id=? AND state IN ('dispatching','unknown') LIMIT 1", (task,)).fetchone():
                    raise _Denied("unavailable")
                reservation = hashlib.sha256(_bytes([PROTOCOL, self.config["run_id"], principal, action])).hexdigest()
                self.ledger.reserve(reservation, lease, self.config["tasks"][task]["reservation_units"], now=now)
                db.execute("INSERT INTO authority_actions VALUES (?,?,?,?,?,?,?,'dispatching')",
                           (action, principal, request_id, task, epoch, supplied, reservation))
                return self._receipt(status="dispatching", task_id=task, epoch=epoch, action_id=action,
                                     reservation_id=reservation, request_sha256=supplied, authority_time=now), True
        return self._receipt(status="ok", lease=asdict(lease), authority_time=now), False


def _remaining(deadline: float) -> float:
    value = deadline - time.monotonic()
    if value <= 0:
        raise TimeoutError("Authority frame deadline")
    return value


def _read_exact(stream: socket.socket, count: int, deadline: float) -> bytes:
    parts = []
    while count:
        stream.settimeout(_remaining(deadline))
        part = stream.recv(min(count, 8192))
        if not part:
            raise AuthorityError("Incomplete authority frame")
        parts.append(part)
        count -= len(part)
    return b"".join(parts)


def _receive(stream: socket.socket, deadline: float) -> dict:
    size = struct.unpack("!I", _read_exact(stream, 4, deadline))[0]
    if not 1 <= size <= MAX_FRAME:
        raise AuthorityError("Authority frame limit")
    try:
        value = strict_loads(_read_exact(stream, size, deadline), max_bytes=MAX_FRAME)
    except ValueError as error:
        raise AuthorityError("Invalid authority JSON") from error
    if type(value) is not dict:
        raise AuthorityError("Invalid authority frame")
    return value


def _send(stream: socket.socket, value: dict, deadline: float) -> None:
    data = _bytes(value)
    stream.settimeout(_remaining(deadline))
    stream.sendall(struct.pack("!I", len(data)) + data)


def authority_request(port: int, principal: str, key: str, request_id: str, operation: str,
                      payload: dict, *, run_id: str, timeout: float = DEADLINE_SECONDS) -> dict:
    """One RPC, never an automatic retry. Timeouts do not imply rollback."""
    body = {"protocol": PROTOCOL, "run_id": run_id, "principal": principal,
            "request_id": request_id, "operation": operation, "payload": payload}
    deadline = time.monotonic() + timeout
    with socket.create_connection(("127.0.0.1", port), timeout=_remaining(deadline)) as stream:
        _send(stream, _signed(body, key), deadline)
        response = _verified(_receive(stream, deadline), key)
    if (set(response) != {"protocol", "run_id", "principal", "request_id", "rpc_sha256", "receipt"}
            or response["protocol"] != PROTOCOL or response["run_id"] != run_id
            or response["principal"] != principal or response["request_id"] != request_id
            or response["rpc_sha256"] != hashlib.sha256(_bytes(body)).hexdigest()
            or type(response["receipt"]) is not dict):
        raise AuthorityError("Authority response does not bind request")
    _bytes(response["receipt"], MAX_RESPONSE)
    return response["receipt"]


class _Server(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True
    request_queue_size = MAX_CONNECTIONS

    def __init__(self, port: int, authority: Authority):
        self.authority = authority
        self.slots = threading.BoundedSemaphore(MAX_CONNECTIONS)
        super().__init__(("127.0.0.1", port), _Handler)

    def process_request(self, stream, address):
        if not self.slots.acquire(blocking=False):
            self.shutdown_request(stream)
            return
        try:
            super().process_request(stream, address)
        except BaseException:
            self.slots.release()
            raise

    def process_request_thread(self, stream, address):
        try:
            super().process_request_thread(stream, address)
        finally:
            self.slots.release()


class _Handler(socketserver.BaseRequestHandler):
    def handle(self):
        authority = cast(_Server, self.server).authority
        deadline = time.monotonic() + DEADLINE_SECONDS
        try:
            response = authority.handle(_receive(self.request, deadline))
            _send(self.request, response, deadline)
        except (OSError, ValueError, TypeError, KeyError):
            return  # Auth/transport failures disclose no trusted-host details.


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--crash-point", choices=["before_commit", "after_intent", "after_entry", "after_fixture",
                                                "before_result_commit", "after_commit"])
    parser.add_argument("--crash-request-id")
    args = parser.parse_args()
    config = strict_loads(args.config.read_bytes(), max_bytes=MAX_FRAME)
    if type(config) is not dict or set(config) != {"root", "port", "authority"}:
        raise AuthorityError("Invalid service configuration")
    if type(config["port"]) is not int or not 0 <= config["port"] <= 65535:
        raise AuthorityError("Invalid service port")
    if bool(args.crash_point) != bool(args.crash_request_id):
        raise AuthorityError("Both trusted fault arguments are required")

    def crash(point: str, request_id: str) -> None:
        if point == args.crash_point and request_id == args.crash_request_id:
            os._exit(70)

    authority = Authority(Path(config["root"]), config["authority"], crash_hook=crash)
    try:
        with _Server(config["port"], authority) as server:
            print(json.dumps({"protocol": PROTOCOL, "port": server.server_address[1],
                              "pid": os.getpid(), "run_id": config["authority"]["run_id"],
                              "config_sha256": authority.config_sha256}), flush=True)
            server.serve_forever(poll_interval=0.1)
    finally:
        authority.close()


if __name__ == "__main__":
    main()
