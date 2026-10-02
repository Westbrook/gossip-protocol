"""Trusted coding-dispatch backend, separate from peer RPC authentication.

The caller owns one Authority and authenticated routing. This backend reuses its
exact Ledger instance plus RequestJournal and OpenAIWorker. It never executes
proposed code, chooses tasks/models, or publishes Git. Offline mode forbids the
worker's real HTTPS transport; a live financial-ledger bridge is unimplemented.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict
import hashlib
from pathlib import Path
import re
import sqlite3
import threading
from typing import Any, Callable, Iterator, Protocol

from . import ledger as ledger_module
from . import peer_authority_v1 as authority_module
from . import peer_store_v1 as store_module
from . import verification_journal as journal_module
from . import worker as worker_module
from .ledger import BudgetExceeded, ClaimRejected, Ledger, Lease
from .peer_authority_v1 import Authority, AuthorityError
from .peer_store_v1 import canonical_bytes, strict_loads
from .verification_journal import JournalUnknownOutcome, RequestJournal
from .worker import OpenAIWorker, WorkerFailure, WorkerRequest

PROTOCOL = "peer-coding-dispatch-v1"
MAX_PAYLOAD_BYTES = 600_000
MAX_RESULT_BYTES = 2_100_000
MAX_ACTIONS_PER_PRINCIPAL = 256
_SAFE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,95}\Z")
_SHA = re.compile(r"[0-9a-f]{64}\Z")


class Payloads(Protocol):
    def read_owned(self, principal: str, sha: str) -> bytes: ...
    def put_owned(self, principal: str, data: bytes) -> str: ...


class DispatchError(AuthorityError):
    """Trusted configuration, storage, or accounting cannot safely proceed."""


class PayloadUnavailable(Exception):
    """The caller-owned payload has not arrived; no dispatch was admitted."""


def canonical_payload(value: Any) -> bytes:
    return canonical_bytes(value, max_bytes=MAX_RESULT_BYTES)


def source_digest(files: dict[str, str]) -> str:
    return hashlib.sha256(canonical_payload(files)).hexdigest()


def _digest(value: Any) -> str:
    return hashlib.sha256(canonical_payload(value)).hexdigest()


def _identifier(value: Any) -> str:
    if type(value) is not str or _SAFE.fullmatch(value) is None:
        raise DispatchError("Invalid dispatch identifier")
    return value


def _sha(value: Any) -> str:
    if type(value) is not str or _SHA.fullmatch(value) is None:
        raise DispatchError("Invalid payload digest")
    return value


class CodingDispatch:
    def __init__(self, authority: Authority, payloads: Payloads,
                 workers: dict[str, OpenAIWorker], task_specs: dict, *,
                 transport_mode: str = "offline", transport_identity: str = "offline-responses-v1",
                 journal_root: Path | None = None,
                 crash_hook: Callable[[str, str], None] | None = None,
                 allow_live: bool = False):
        self.authority, self.ledger, self.payloads = authority, authority.ledger, payloads
        self.workers, self.crash_hook = workers.copy(), crash_hook
        self.transport_mode, self.transport_identity = transport_mode, _identifier(transport_identity)
        self.lock = threading.RLock()
        self.failed_closed = False
        self.journal = RequestJournal((journal_root or authority.root / "coding-journal-v1").resolve())
        self.task_specs = strict_loads(canonical_payload(task_specs))
        if transport_mode != "offline" or allow_live:
            raise DispatchError("Live provider dispatch is unavailable: no qualified shared financial ledger bridge")
        if not 1 <= len(workers) <= 8 or not self.task_specs or len(self.task_specs) > 64:
            raise DispatchError("Invalid dispatch catalog size")
        self.profiles = {name: self._worker_identity(name, worker) for name, worker in workers.items()}
        for task, spec in self.task_specs.items():
            _identifier(task)
            if (task not in authority.config["tasks"] or type(spec) is not dict
                    or set(spec) != {"allowed_paths", "profiles"}
                    or type(spec["allowed_paths"]) is not list or not 1 <= len(spec["allowed_paths"]) <= 64
                    or any(not worker_module._path_valid(p) for p in spec["allowed_paths"])
                    or len(set(spec["allowed_paths"])) != len(spec["allowed_paths"])
                    or type(spec["profiles"]) is not list or not spec["profiles"]
                    or any(p not in self.profiles for p in spec["profiles"])
                    or len(set(spec["profiles"])) != len(spec["profiles"])):
                raise DispatchError("Invalid fixed coding task scope")
        source_bindings = {}
        for module in (ledger_module, authority_module, journal_module, worker_module, store_module):
            source_path = module.__file__
            if source_path is None:
                raise DispatchError("Execution source cannot be bound")
            path = Path(source_path)
            source_bindings[path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
        source_bindings[Path(__file__).name] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        self.config = {"protocol": PROTOCOL, "run_id": authority.config["run_id"],
                       "authority_config_sha256": authority.config_sha256,
                       "profiles": self.profiles, "tasks": self.task_specs,
                       "transport_mode": transport_mode, "transport_identity": transport_identity,
                       "accounting_units": "microUSD_worker_estimate",
                       "sources": source_bindings, "max_payload_bytes": MAX_PAYLOAD_BYTES,
                       "journal_namespace": str(self.journal.root),
                       "max_result_bytes": MAX_RESULT_BYTES, "maximum_concurrent_invocations": 1,
                       "live_provider_available": False}
        self.config_sha256 = _digest(self.config)
        with self._active():
            with authority.lifecycle:
                if getattr(authority, "_coding_dispatch_v1_owner", None) is not None:
                    raise DispatchError("Authority already owns a coding dispatch backend")
                # Failed startup requires closing/reopening the authority, never
                # another recovery pass competing with a possibly active owner.
                setattr(authority, "_coding_dispatch_v1_owner", self)
            self._bootstrap()
            self._validate_retained()
            self._recover_pending()

    def _worker_identity(self, name: str, worker: OpenAIWorker) -> dict:
        _identifier(name)
        if not isinstance(worker, OpenAIWorker):
            raise DispatchError("Coding dispatch requires the existing OpenAIWorker adapter")
        real = worker._transport is worker_module._https_transport
        if (self.transport_mode == "offline" and real) or (self.transport_mode == "live" and not real):
            raise DispatchError("Worker transport does not match the frozen mode")
        return {"manifest": worker.profile_manifest(), "timeout": worker.timeout,
                "max_response_bytes": worker_module.MAX_RESPONSE_BYTES,
                "transport_mode": self.transport_mode, "transport_identity": self.transport_identity}

    @contextmanager
    def _active(self) -> Iterator[None]:
        # Share the actual authority lifecycle; never release its exclusive
        # service ownership while a backend provider/journal operation is active.
        with self.authority.lifecycle:
            if self.authority.closing:
                raise DispatchError("Authority is closing")
            self.authority.active_handlers += 1
        try:
            yield
        finally:
            with self.authority.lifecycle:
                self.authority.active_handlers -= 1
                self.authority.lifecycle.notify_all()

    def _receipt(self, **values: Any) -> dict:
        return self.authority._receipt(dispatch_protocol=PROTOCOL,
                                       dispatch_config_sha256=self.config_sha256, **values)

    def _boundary(self, point: str, request_id: str) -> None:
        if self.crash_hook:
            self.crash_hook(point, request_id)

    def _bootstrap(self) -> None:
        with self.ledger.atomic() as db:
            tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            own = {"coding_meta_v1", "coding_actions_v1"}
            if tables & own and not own <= tables:
                raise DispatchError("Incomplete coding dispatch schema")
            db.execute("CREATE TABLE IF NOT EXISTS coding_meta_v1 (key TEXT PRIMARY KEY,value TEXT NOT NULL)")
            db.execute("""CREATE TABLE IF NOT EXISTS coding_actions_v1 (
                principal TEXT NOT NULL, action_id TEXT NOT NULL, request_id TEXT NOT NULL,
                task_id TEXT NOT NULL, epoch INTEGER NOT NULL, profile_id TEXT NOT NULL,
                payload_sha TEXT NOT NULL, worker_request BLOB NOT NULL, binding_sha TEXT NOT NULL,
                call_id TEXT NOT NULL UNIQUE, reservation_id TEXT NOT NULL UNIQUE,
                reserved_units INTEGER NOT NULL, state TEXT NOT NULL,
                PRIMARY KEY(principal,action_id), UNIQUE(principal,request_id))""")
            row = db.execute("SELECT value FROM coding_meta_v1 WHERE key='config_sha256'").fetchone()
            if row is None:
                if db.execute("SELECT 1 FROM coding_actions_v1 LIMIT 1").fetchone():
                    raise DispatchError("Unbound existing coding actions")
                db.executemany("INSERT INTO coding_meta_v1 VALUES (?,?)",
                               [("config_sha256", self.config_sha256), ("halted", "0")])
            elif row[0] != self.config_sha256:
                raise DispatchError("Coding execution contract changed across restart")

    def guard_claim(self, db: sqlite3.Connection, task_id: str) -> None:
        """Root wrapper calls inside existing Authority._operate claim transaction."""
        if getattr(self.ledger.context, "db", None) is not db:
            raise DispatchError("Claim guard must join this authority transaction")
        if self.failed_closed:
            raise ClaimRejected("Coding dispatch failed closed")
        if db.execute("SELECT value FROM coding_meta_v1 WHERE key='halted'").fetchone()[0] != "0":
            raise ClaimRejected("Coding dispatch is halted")
        if db.execute("SELECT 1 FROM coding_actions_v1 WHERE task_id=? AND state NOT IN ('completed','failed') LIMIT 1",
                      (task_id,)).fetchone():
            raise ClaimRejected("Coding task has unresolved dispatch")

    def _halt(self, db: sqlite3.Connection) -> None:
        db.execute("UPDATE coding_meta_v1 SET value='1' WHERE key='halted'")

    def _validate_retained(self) -> None:
        """Check retained identities/accounting without replaying or mutating them."""
        with self.ledger.atomic() as db:
            halted = db.execute("SELECT value FROM coding_meta_v1 WHERE key='halted'").fetchone()
            if halted is None or halted[0] not in {"0", "1"}:
                raise DispatchError("Missing coding halt state")
            rows = db.execute("SELECT principal,request_id FROM coding_actions_v1").fetchall()
            for row in db.execute("SELECT * FROM authority_requests").fetchall():
                receipt = self.authority._read_receipt(row)
                body = strict_loads(row["body"], max_bytes=authority_module.MAX_REQUEST + 2048)
                if body.get("operation") == "coding_dispatch" and receipt.get("status") != "rejected":
                    if not db.execute("SELECT 1 FROM coding_actions_v1 WHERE principal=? AND request_id=?",
                                      (row["principal"], row["request_id"])).fetchone():
                        raise DispatchError("Coding request has no retained action")
        for row in rows:
            action, receipt = self._action(row["principal"], row["request_id"])
            if action["state"] not in {"dispatching", "publication_pending", "completed", "failed", "unknown"}:
                raise DispatchError("Invalid retained coding action state")
            if receipt.get("status") != action["state"]:
                raise DispatchError("Coding action and receipt state differ")
            if action["state"] in {"unknown", "publication_pending"} and halted[0] != "1":
                raise DispatchError("Unresolved coding outcome has no durable halt")
            self._verify_reservation(action, require_lease=False)
            if action["state"] in {"completed", "failed"}:
                self._validate_terminal(action, receipt)

    def _validate_terminal(self, action: dict, receipt: dict) -> None:
        paths = self.journal.paths(action["call_id"])
        identity = {"schema_version": 1, "call_id": action["call_id"],
                    "reservation_id": action["reservation_id"],
                    "request_sha256": receipt["journal_request_sha256"]}
        expected_request = {**identity, "request": strict_loads(action["worker_request"], max_bytes=MAX_PAYLOAD_BYTES)}
        owner_path = self.journal.root / (hashlib.sha256(action["reservation_id"].encode()).hexdigest() + ".reservation.json")
        owner, _ = journal_module._read(owner_path)
        request, _ = journal_module._read(paths["request"])
        result, result_raw = journal_module._read(paths["result"])
        settled, _ = journal_module._read(paths["settled"])
        expected_kind = "result" if action["state"] == "completed" else "failure"
        if (journal_module._bytes(owner) != journal_module._bytes(identity)
                or journal_module._bytes(request) != journal_module._bytes(expected_request)
                or set(result) != {*identity, "kind", "payload", "payload_sha256"}
                or any(result.get(key) != value for key, value in identity.items())
                or result.get("kind") != expected_kind
                or result["payload_sha256"] != hashlib.sha256(journal_module._bytes(result["payload"])).hexdigest()):
            raise DispatchError("Terminal coding journal binding mismatch")
        outcome = (journal_module._result(result["payload"]) if expected_kind == "result"
                   else journal_module._failure(result["payload"]))
        usage = outcome.usage_units
        expected_settled = {**identity, "result_sha256": hashlib.sha256(result_raw).hexdigest(), "usage_units": usage}
        expected_result = canonical_payload({"kind": expected_kind, "payload": result["payload"]})
        if (type(usage) is not int or receipt.get("usage_units") != usage
                or journal_module._bytes(settled) != journal_module._bytes(expected_settled)
                or receipt.get("result_sha256") != hashlib.sha256(expected_result).hexdigest()):
            raise DispatchError("Terminal coding result binding mismatch")
        with self.ledger.atomic() as db:
            reservation = db.execute("SELECT * FROM reservations WHERE id=?", (action["reservation_id"],)).fetchone()
            if reservation is None or reservation["state"] != "settled" or reservation["spent"] != usage:
                raise DispatchError("Terminal coding usage does not match accounting")
            if outcome.metadata.get("halt") and db.execute("SELECT value FROM coding_meta_v1 WHERE key='halted'").fetchone()[0] != "1":
                raise DispatchError("Terminal provider halt was removed")

    def _load_request(self, principal: str, task_id: str, profile_id: str, sha: str) -> WorkerRequest:
        if principal not in self.authority.config["principals"]:
            raise DispatchError("Unknown principal")
        if (task_id not in self.authority.config["principals"][principal]["tasks"]
                or task_id not in self.task_specs or profile_id not in self.task_specs[task_id]["profiles"]):
            raise DispatchError("Task or profile outside principal scope")
        worker = self.workers[profile_id]
        if self._worker_identity(profile_id, worker) != self.profiles[profile_id]:
            raise DispatchError("Worker profile changed after binding")
        try:
            data = self.payloads.read_owned(principal, _sha(sha))
        except (FileNotFoundError, KeyError) as error:
            raise PayloadUnavailable("Owned request payload is not available") from error
        if type(data) is not bytes or len(data) > MAX_PAYLOAD_BYTES or hashlib.sha256(data).hexdigest() != sha:
            raise DispatchError("Owned request payload identity mismatch")
        value = strict_loads(data, max_bytes=MAX_PAYLOAD_BYTES)
        if canonical_payload(value) != data or type(value) is not dict or set(value) != {"worker_request", "context"}:
            raise DispatchError("Request payload must be exact canonical coding JSON")
        fields = value["worker_request"]
        context = value["context"]
        if (type(fields) is not dict or set(fields) != {"task_id", "instructions", "allowed_paths", "files", "base_sha", "attempt", "feedback"}
                or fields["task_id"] != task_id or fields["allowed_paths"] != self.task_specs[task_id]["allowed_paths"]
                or type(fields["base_sha"]) is not str or re.fullmatch(r"[0-9a-f]{40}", fields["base_sha"]) is None
                or type(context) is not dict or set(context) != {"source_sha256", "event_ids"}
                or context["source_sha256"] != source_digest(fields["files"])
                or type(context["event_ids"]) is not list or len(context["event_ids"]) > 256
                or any(type(item) is not str or _SHA.fullmatch(item) is None for item in context["event_ids"])
                or len(set(context["event_ids"])) != len(context["event_ids"])):
            raise DispatchError("Coding source, scope or context binding mismatch")
        fields["allowed_paths"] = tuple(fields["allowed_paths"])
        request = WorkerRequest(**fields)
        worker.reservation_units(request)  # Full existing payload/scope/byte preflight.
        return request

    def execute(self, body: dict) -> dict:
        """Authenticated wrapper only; returns a principal-scoped small receipt.

        Root must route coding_dispatch here before base Authority._process and
        deny its text-fixture dispatch. Base lookup safely reads the same journal.
        """
        with self._active(), self.lock:
            return self._execute(body)

    def _execute(self, body: dict) -> dict:
        if (type(body) is not dict or set(body) != {"protocol", "run_id", "principal", "request_id", "operation", "payload"}
                or body["protocol"] != authority_module.PROTOCOL or body["run_id"] != self.authority.config["run_id"]
                or body["operation"] != "coding_dispatch" or type(body["payload"]) is not dict):
            raise DispatchError("Invalid authenticated coding envelope")
        principal, request_id = _identifier(body["principal"]), _identifier(body["request_id"])
        if principal not in self.authority.config["principals"]:
            raise DispatchError("Unknown authenticated principal")
        raw = authority_module._bytes(body, authority_module.MAX_REQUEST + 2048)
        rpc_sha = hashlib.sha256(raw).hexdigest()
        payload = body["payload"]
        with self.ledger.atomic() as db:
            prior = db.execute("SELECT * FROM authority_requests WHERE principal=? AND request_id=?",
                               (principal, request_id)).fetchone()
            if prior is not None:
                if prior["sha"] != rpc_sha:
                    return self._receipt(status="rejected", reason="request_conflict")
                return self.authority._read_receipt(prior)
        request: WorkerRequest | None = None
        try:
            if set(payload) != {"task_id", "epoch", "action_id", "profile_id", "request_sha256"}:
                raise DispatchError("Invalid coding dispatch fields")
            task_id, action_id, profile_id = (_identifier(payload[key]) for key in ("task_id", "action_id", "profile_id"))
            epoch = payload["epoch"]
            if type(epoch) is not int or not 1 <= epoch <= (1 << 63) - 1:
                raise DispatchError("Invalid lease epoch")
            request = self._load_request(principal, task_id, profile_id, payload["request_sha256"])
            units = self.workers[profile_id].reservation_units(request)
        except PayloadUnavailable:
            return self._receipt(status="waiting", reason="payload_unavailable")
        except (ValueError, KeyError, WorkerFailure):
            return self._record_rejection(principal, request_id, rpc_sha, raw, "request")
        request_value = asdict(request)
        request_value["allowed_paths"] = list(request.allowed_paths)
        request_data = canonical_payload(request_value)
        binding = {"protocol": PROTOCOL, "run_id": self.authority.config["run_id"],
                   "dispatch_config_sha256": self.config_sha256, "principal": principal,
                   "request_id": request_id, "task_id": task_id, "epoch": epoch,
                   "action_id": action_id, "profile_id": profile_id,
                   "profile_sha256": _digest(self.profiles[profile_id]),
                   "payload_sha256": payload["request_sha256"],
                   "worker_request_sha256": hashlib.sha256(request_data).hexdigest(),
                   "journal_request_sha256": hashlib.sha256(journal_module._bytes(asdict(request))).hexdigest(),
                   "reserved_units": units}
        binding_sha = _digest(binding)
        call_id, reservation_id = "coding-" + binding_sha, "coding-" + binding_sha
        with self.ledger.atomic() as db:
            now = self.authority._now(db)
            # Repeat the unified request check under the mutation lock.
            prior = db.execute("SELECT * FROM authority_requests WHERE principal=? AND request_id=?",
                               (principal, request_id)).fetchone()
            if prior is not None:
                return (self.authority._read_receipt(prior) if prior["sha"] == rpc_sha
                        else self._receipt(status="rejected", reason="request_conflict"))
            count = db.execute("SELECT COUNT(*) FROM authority_requests WHERE principal=?", (principal,)).fetchone()[0]
            if count >= MAX_ACTIONS_PER_PRINCIPAL:
                return self._receipt(status="rejected", reason="capacity")
            db.execute("SAVEPOINT coding_admission")
            try:
                self.guard_claim(db, task_id)
                if db.execute("SELECT 1 FROM coding_actions_v1 WHERE principal=? AND action_id=?", (principal, action_id)).fetchone():
                    raise DispatchError("Action ID already bound")
                lease = Lease(task_id, principal, epoch, 0)
                self.ledger.reserve(reservation_id, lease, units, now=now)
                receipt = self._receipt(status="dispatching", **{key: value for key, value in binding.items()
                                                                 if key not in {"protocol", "run_id", "dispatch_config_sha256"}},
                                        call_id=call_id, reservation_id=reservation_id, authority_time=now)
                db.execute("INSERT INTO coding_actions_v1 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                           (principal, action_id, request_id, task_id, epoch, profile_id,
                            payload["request_sha256"], request_data, binding_sha, call_id, reservation_id, units, "dispatching"))
            except (ClaimRejected, BudgetExceeded, DispatchError) as error:
                db.execute("ROLLBACK TO coding_admission")
                reason = "budget" if isinstance(error, BudgetExceeded) else "unavailable"
                receipt = self._receipt(status="rejected", reason=reason)
            db.execute("RELEASE coding_admission")
            self._insert_receipt(db, principal, request_id, rpc_sha, raw, receipt)
        if receipt["status"] == "rejected":
            return receipt
        try:
            self._boundary("after_admission", request_id)
        except Exception:
            return self._unknown(principal, request_id, "admission_boundary")
        return self._run_journal(principal, request_id, recovery=False)

    def _insert_receipt(self, db: sqlite3.Connection, principal: str, request_id: str,
                        sha: str, body: bytes, receipt: dict) -> None:
        data = authority_module._bytes(receipt, authority_module.MAX_RESPONSE)
        db.execute("INSERT INTO authority_requests VALUES (?,?,?,?,?,?)",
                   (principal, request_id, sha, body, data, hashlib.sha256(data).hexdigest()))

    def _record_rejection(self, principal: str, request_id: str, sha: str, raw: bytes, reason: str) -> dict:
        with self.ledger.atomic() as db:
            self.authority._now(db)
            prior = db.execute("SELECT * FROM authority_requests WHERE principal=? AND request_id=?", (principal, request_id)).fetchone()
            if prior is not None:
                return self.authority._read_receipt(prior) if prior["sha"] == sha else self._receipt(status="rejected", reason="request_conflict")
            receipt = self._receipt(status="rejected", reason=reason)
            if db.execute("SELECT COUNT(*) FROM authority_requests WHERE principal=?", (principal,)).fetchone()[0] >= MAX_ACTIONS_PER_PRINCIPAL:
                return self._receipt(status="rejected", reason="capacity")
            self._insert_receipt(db, principal, request_id, sha, raw, receipt)
            return receipt

    def _action(self, principal: str, request_id: str) -> tuple[dict, dict]:
        with self.ledger.atomic() as db:
            row = db.execute("SELECT * FROM coding_actions_v1 WHERE principal=? AND request_id=?", (principal, request_id)).fetchone()
            request_row = db.execute("SELECT * FROM authority_requests WHERE principal=? AND request_id=?",
                                     (principal, request_id)).fetchone()
            receipt = self.authority._read_receipt(request_row)
            if row is None:
                raise DispatchError("Missing coding action")
            action = dict(row)
            expected_rpc = {"protocol": authority_module.PROTOCOL, "run_id": self.authority.config["run_id"],
                            "principal": principal, "request_id": request_id, "operation": "coding_dispatch",
                            "payload": {"task_id": action["task_id"], "epoch": action["epoch"],
                                        "action_id": action["action_id"], "profile_id": action["profile_id"],
                                        "request_sha256": action["payload_sha"]}}
            if request_row["body"] != authority_module._bytes(expected_rpc, authority_module.MAX_REQUEST + 2048):
                raise DispatchError("Coding RPC and action binding differ")
            binding = {"protocol": PROTOCOL, "run_id": self.authority.config["run_id"],
                       "dispatch_config_sha256": self.config_sha256, "principal": principal,
                       "request_id": request_id, "task_id": action["task_id"], "epoch": action["epoch"],
                       "action_id": action["action_id"], "profile_id": action["profile_id"],
                       "profile_sha256": _digest(self.profiles[action["profile_id"]]),
                       "payload_sha256": action["payload_sha"],
                       "worker_request_sha256": hashlib.sha256(action["worker_request"]).hexdigest(),
                       "journal_request_sha256": hashlib.sha256(journal_module._bytes(
                           strict_loads(action["worker_request"], max_bytes=MAX_PAYLOAD_BYTES))).hexdigest(),
                       "reserved_units": action["reserved_units"]}
            wanted = _digest(binding)
            if (action["binding_sha"] != wanted or action["call_id"] != "coding-" + wanted
                    or action["reservation_id"] != "coding-" + wanted
                    or receipt.get("call_id") != action["call_id"]
                    or receipt.get("reservation_id") != action["reservation_id"]
                    or any(receipt.get(key) != value for key, value in binding.items() if key != "protocol")):
                raise DispatchError("Coding action binding mismatch")
            return action, receipt

    def _verify_reservation(self, action: dict, *, require_lease: bool) -> None:
        with self.ledger.atomic() as db:
            row = db.execute("SELECT * FROM reservations WHERE id=?", (action["reservation_id"],)).fetchone()
            current = db.execute("SELECT * FROM coding_actions_v1 WHERE principal=? AND request_id=?",
                                 (action["principal"], action["request_id"])).fetchone()
            if (row is None or current is None or dict(current) != action
                    or (row["task_id"], row["epoch"], row["amount"]) !=
                       (action["task_id"], action["epoch"], action["reserved_units"])):
                raise DispatchError("Existing reservation binding mismatch")
            if ((row["spent"] is None and row["state"] != "reserved")
                    or (row["spent"] is not None and (row["state"] != "settled"
                        or type(row["spent"]) is not int or not 0 <= row["spent"] <= row["amount"]))):
                raise DispatchError("Invalid reservation settlement state")
            if require_lease:
                halted = db.execute("SELECT value FROM coding_meta_v1 WHERE key='halted'").fetchone()
                if (row["spent"] is not None or current["state"] != "dispatching"
                        or self.failed_closed or halted is None or halted[0] != "0"):
                    raise DispatchError("Cannot invoke a settled, unresolved or halted reservation")
                Ledger._validate(db, Lease(action["task_id"], action["principal"], action["epoch"], 0), self.authority._now(db))

    def _run_journal(self, principal: str, request_id: str, *, recovery: bool) -> dict:
        try:
            return self._dispatch_journal(principal, request_id, recovery=recovery)
        except Exception:
            # Includes fsync/replace, SQLite and unexpected accounting failures
            # anywhere after admission. BaseException crash hooks remain crashes.
            return self._unknown(principal, request_id, "journal_or_accounting")

    def _dispatch_journal(self, principal: str, request_id: str, *, recovery: bool) -> dict:
        action, receipt = self._action(principal, request_id)
        values = strict_loads(action["worker_request"], max_bytes=MAX_PAYLOAD_BYTES)
        values["allowed_paths"] = tuple(values["allowed_paths"])
        request = WorkerRequest(**values)
        worker = self.workers[action["profile_id"]]
        if self._worker_identity(action["profile_id"], worker) != self.profiles[action["profile_id"]]:
            raise DispatchError("Worker changed during dispatch")

        def reserve() -> None:
            if recovery:
                raise JournalUnknownOutcome("Interrupted before a recoverable provider result")
            self._verify_reservation(action, require_lease=True)

        def invoke():
            if recovery:
                raise JournalUnknownOutcome("Recovery cannot invoke a provider")
            self._verify_reservation(action, require_lease=True)
            self._boundary("before_invoke", request_id)
            return worker.run(request)

        def settle(usage: int) -> None:
            self._verify_reservation(action, require_lease=False)
            self.ledger.settle(action["reservation_id"], usage)
            self._boundary("after_settlement", request_id)

        try:
            outcome = self.journal.execute(action["call_id"], request, action["reservation_id"], invoke, reserve, settle,
                                           on_persisted=lambda: self._boundary("after_result_persisted", request_id))
            result, status, halt = asdict(outcome), "completed", False
            usage: int | None = outcome.usage_units
        except WorkerFailure as failure:
            usage = failure.usage_units
            result = {"message": str(failure), "usage_units": usage, "metadata": failure.metadata}
            status, halt = ("unknown" if usage is None else "failed"), (usage is None or bool(failure.metadata.get("halt")))
        try:
            data = canonical_payload({"kind": "result" if status == "completed" else "failure", "payload": result})
            if len(data) > MAX_RESULT_BYTES:
                raise DispatchError("Result exceeds payload limit")
            result_sha = self.payloads.put_owned(principal, data)
            if result_sha != hashlib.sha256(data).hexdigest():
                raise DispatchError("Result payload publication mismatch")
        except Exception:
            # A journal result remains recoverable without another invocation.
            return self._unknown(principal, request_id, "result_publication", recoverable=True)
        with self.ledger.atomic() as db:
            now = self.authority._now(db)
            receipt.pop("reason", None)
            receipt.update(status=status, result_sha256=result_sha, usage_units=usage, authority_time=now)
            if halt:
                self._halt(db)
            self.authority._update_receipt(db, principal, request_id, receipt)
            db.execute("UPDATE coding_actions_v1 SET state=? WHERE principal=? AND request_id=?", (status, principal, request_id))
            self._boundary("before_receipt_commit", request_id)
        self._boundary("after_receipt_commit", request_id)
        return receipt

    def _unknown(self, principal: str, request_id: str, reason: str, *, recoverable: bool = False) -> dict:
        # If storage cannot retain the halt, the live instance still cannot
        # start another invocation. Startup recovery fences durable pending work.
        self.failed_closed = True
        try:
            with self.ledger.atomic() as db:
                now = self.authority._now(db)
                receipt = self.authority._read_receipt(db.execute("SELECT * FROM authority_requests WHERE principal=? AND request_id=?",
                                                                 (principal, request_id)).fetchone())
                receipt.update(status="publication_pending" if recoverable else "unknown", reason=reason, authority_time=now)
                self.authority._update_receipt(db, principal, request_id, receipt)
                db.execute("UPDATE coding_actions_v1 SET state=? WHERE principal=? AND request_id=?",
                           ("publication_pending" if recoverable else "unknown", principal, request_id))
                self._halt(db)
                return receipt
        except Exception:
            # No terminal receipt was durably recorded. Keep the caller on its
            # original request ID for lookup/recovery, never a fresh dispatch.
            return self._receipt(status="waiting", reason="state_persistence_failure",
                                 principal=principal, request_id=request_id)

    def _recover_pending(self) -> None:
        with self.ledger.atomic() as db:
            pending = [(row["principal"], row["request_id"]) for row in db.execute(
                "SELECT principal,request_id FROM coding_actions_v1 WHERE state IN ('dispatching','publication_pending') ORDER BY principal,request_id")]
        for principal, request_id in pending:
            self._run_journal(principal, request_id, recovery=True)

    def lookup(self, principal: str, request_id: str) -> dict:
        with self._active(), self.ledger.atomic() as db:
            if principal not in self.authority.config["principals"]:
                raise DispatchError("Unknown authenticated principal")
            _identifier(request_id)
            row = db.execute("SELECT * FROM authority_requests WHERE principal=? AND request_id=?", (principal, request_id)).fetchone()
            return self.authority._read_receipt(row) if row else self._receipt(status="missing")
