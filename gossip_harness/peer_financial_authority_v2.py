"""Offline-qualified asynchronous bridge to one existing cumulative ledger.

The authenticated RPC seam supplies ``actor``; this module never trusts an actor
inside an action in its place. Service-owned threads retain the historical live
lock until all accounting ends. A cohort has one immutable allowance and journal
namespace regardless of evidence/output directories. All registered trajectory
and milestone contexts share that cohort allowance.
Live provider dispatch remains unavailable pending combined qualification.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import asdict
import fcntl
import hashlib
import math
import os
from pathlib import Path
import re
import sqlite3
import threading
import time
from typing import Any, Callable, Iterator, Protocol
import uuid

from . import verification_journal as journal_module
from . import worker as worker_module
from .ledger import BudgetExceeded, ClaimRejected, Ledger, Lease
from .peer_authority_v1 import _JoinedLedger
from .peer_store_v1 import canonical_bytes, strict_loads
from .peer_project_contract_v2 import (
    ActionRequest, Context, DispatchBinding, DispatchReply, WorkKey,
    decode, encode, from_dict, identity, to_dict, worker_request_digest,
)
from .verification_journal import JournalUnknownOutcome, RequestJournal
from .worker import OpenAIWorker, WorkerFailure, WorkerRequest

PROTOCOL = "peer-financial-authority-v2"
MAX_BYTES = 2_100_000
_SAFE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,95}\Z")
_BASE_SCHEMA = {
    "settings": ["key", "value"],
    "tasks": ["id", "status", "worker", "epoch", "expires", "accepted_commit", "intent_id"],
    "dependencies": ["task_id", "prerequisite"],
    "reservations": ["id", "task_id", "epoch", "amount", "spent", "state"],
    "intents": ["id", "repository", "old_head", "new_head", "leases", "state", "result"],
    "budget_changes": ["id", "old_budget", "new_budget", "reason", "changed_at", "spent_or_reserved"],
}
_TABLES = {"financial_cohorts_v2", "financial_tasks_v2", "financial_requests_v2", "financial_actions_v2"}


class FinancialError(ValueError):
    """Immutable identity, scope, ownership or retained accounting is invalid."""


class Payloads(Protocol):
    def read_owned(self, principal: str, sha: str) -> bytes: ...
    def put_owned(self, principal: str, data: bytes) -> str: ...


def canonical_payload(value: Any) -> bytes:
    return canonical_bytes(value, max_bytes=MAX_BYTES)


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _digest(value: Any) -> str:
    return _sha(canonical_payload(value))


def _name(value: Any) -> str:
    if type(value) is not str or _SAFE.fullmatch(value) is None:
        raise FinancialError("Invalid financial identifier")
    return value


def ledger_identity(path: Path) -> dict:
    """Read a trusted existing-file identity, never create or adopt a wallet."""
    path = Path(path)
    if not path.is_absolute() or str(path) != str(path.resolve()) or not path.is_file():
        raise FinancialError("An existing canonical absolute ledger path is required")
    stat = path.stat()
    return {"path": str(path), "device": stat.st_dev, "inode": stat.st_ino}


class CumulativeAuthorityV2:
    @classmethod
    def open(cls, existing_ledger_path: Path, service_root: Path, cohort_contract: dict,
             incremental_cap_micro_usd: int, expected_global_cap: int, expected_opening_usage: int,
             *, payloads: Payloads, workers: dict[str, OpenAIWorker], max_workers: int = 4,
             recovery: bool = False, mode: str = "offline", clock: Callable[[], float] = time.time,
             crash_hook: Callable[[str, str], None] | None = None) -> CumulativeAuthorityV2:
        return cls(existing_ledger_path, service_root, cohort_contract, incremental_cap_micro_usd,
                   expected_global_cap, expected_opening_usage, payloads=payloads, workers=workers,
                   max_workers=max_workers, recovery=recovery, mode=mode, clock=clock, crash_hook=crash_hook)

    def __init__(self, existing_ledger_path: Path, service_root: Path, cohort_contract: dict,
                 incremental_cap_micro_usd: int, expected_global_cap: int, expected_opening_usage: int,
                 *, payloads: Payloads, workers: dict[str, OpenAIWorker], max_workers: int = 4,
                 recovery: bool = False, mode: str = "offline", clock: Callable[[], float] = time.time,
                 crash_hook: Callable[[str, str], None] | None = None):
        if mode != "offline":
            raise FinancialError("Live financial dispatch is not qualified or enabled")
        for value in (incremental_cap_micro_usd, expected_global_cap, expected_opening_usage):
            if type(value) is not int or value < 0:
                raise FinancialError("Financial amounts must be nonnegative integers")
        if type(max_workers) is not int or not 1 <= max_workers <= 32:
            raise FinancialError("Invalid executor bound")
        self.path, self.root = Path(existing_ledger_path), Path(service_root)
        self.payloads, self.workers, self.clock, self.crash_hook = payloads, workers.copy(), clock, crash_hook
        self.contract = strict_loads(canonical_payload(cohort_contract), max_bytes=MAX_BYTES)
        if type(self.contract) is not dict or set(self.contract) != {
            "cohort_id", "execution_contract_sha256", "ledger_identity", "journal_root", "transport_identity", "task_specs"
        }:
            raise FinancialError("Invalid closed cohort contract")
        self.cohort_id = _name(self.contract["cohort_id"])
        if not re.fullmatch(r"[0-9a-f]{64}", self.contract["execution_contract_sha256"]):
            raise FinancialError("Invalid execution contract identity")
        self._identity = ledger_identity(self.path)
        if self.contract["ledger_identity"] != self._identity:
            raise FinancialError("Registered ledger identity differs")
        _name(self.contract["transport_identity"])
        journal_root = Path(self.contract["journal_root"])
        if not journal_root.is_absolute() or journal_root != journal_root.resolve():
            raise FinancialError("Journal namespace must be a canonical absolute path")
        if not workers or len(workers) > 8:
            raise FinancialError("Invalid worker catalog")
        self.profiles = {name: self._profile(name, worker) for name, worker in self.workers.items()}
        self.task_specs: dict[str, dict] = {}
        self.actors: set[str] = set()
        specs = self.contract["task_specs"]
        if type(specs) is not list or not 1 <= len(specs) <= 1024:
            raise FinancialError("Invalid task catalog")
        for spec in specs:
            if type(spec) is not dict or set(spec) != {
                "context", "work", "actors", "kinds", "profiles", "allowed_paths", "max_reserved_units"
            }:
                raise FinancialError("Invalid closed task specification")
            context = from_dict(Context, spec["context"])
            if (context.cohort_id != self.cohort_id
                    or context.execution_contract_sha256 != self.contract["execution_contract_sha256"]):
                raise FinancialError("Task context exceeds registered cohort")
            work = from_dict(WorkKey, spec["work"])
            task_id = self.task_id(context, work)
            if task_id in self.task_specs:
                raise FinancialError("Duplicate task identity")
            for key in ("actors", "kinds", "profiles", "allowed_paths"):
                values = spec[key]
                if type(values) is not list or not values or len(set(values)) != len(values):
                    raise FinancialError("Invalid task permissions")
            if (any(_name(actor) != actor for actor in spec["actors"])
                    or any(kind not in {"plan", "build", "select_tests", "select_source", "review", "repair"}
                           for kind in spec["kinds"])
                    or any(profile not in self.profiles for profile in spec["profiles"])
                    or any(not worker_module._path_valid(path) for path in spec["allowed_paths"])
                    or type(spec["max_reserved_units"]) is not int or spec["max_reserved_units"] < 0):
                raise FinancialError("Task permissions exceed registered capabilities")
            self.task_specs[task_id] = spec
            self.actors.update(spec["actors"])
        sources = {}
        for module_path in (Path(__file__), Path(journal_module.__file__), Path(worker_module.__file__),
                            Path(__file__).with_name("ledger.py"), Path(__file__).with_name("peer_authority_v1.py"),
                            Path(__file__).with_name("peer_project_contract_v2.py")):
            sources[module_path.name] = _sha(module_path.read_bytes())
        self.config = {"protocol": PROTOCOL, "contract": self.contract, "profiles": self.profiles,
                       "incremental_cap_micro_usd": incremental_cap_micro_usd,
                       "expected_global_cap": expected_global_cap, "max_workers": max_workers,
                       "clock_policy": "monotonic-authority-after-begin-immediate-v1",
                       "mode": mode, "sources": sources}
        self.config_sha256 = _digest(self.config)
        self.owner_id = uuid.uuid4().hex
        self.owner_pid = os.getpid()
        self.lifecycle = threading.Condition()
        self.closing, self.closed, self.failed_closed = False, False, False
        self.active_handlers = 0
        self.persistence_failed: set[tuple[str, str]] = set()
        self.slots = threading.BoundedSemaphore(max_workers)
        self.owner = open(str(self.path) + ".continuation-live.lock", "a")
        try:
            fcntl.flock(self.owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if ledger_identity(self.path) != self._identity:
                raise FinancialError("Ledger changed while acquiring ownership")
            self._validate_schema()
            self.ledger = _JoinedLedger(self.path, expected_global_cap)
            self._bootstrap(expected_opening_usage, recovery)
            self.root.mkdir(parents=True, exist_ok=True)
            self.journal = RequestJournal(journal_root)
            self._recover()
            self.executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="financial-v2")
        except BaseException:
            self.owner.close()
            raise

    def _profile(self, name: str, worker: OpenAIWorker) -> dict:
        _name(name)
        if type(worker) is not OpenAIWorker or worker._transport is worker_module._https_transport:
            raise FinancialError("Offline mode requires the real OpenAIWorker with a fixture transport")
        return {"manifest": worker.profile_manifest(), "timeout": worker.timeout,
                "transport_identity": self.contract["transport_identity"]}

    def _validate_schema(self) -> None:
        with sqlite3.connect(f"file:{self.path}?mode=rw", uri=True) as db:
            tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if not set(_BASE_SCHEMA) <= tables or (tables & _TABLES and not _TABLES <= tables):
                raise FinancialError("Missing or incomplete existing ledger schema")
            integer_columns = {"value", "epoch", "amount", "spent", "old_budget", "new_budget", "spent_or_reserved"}
            for table, columns in _BASE_SCHEMA.items():
                info = list(db.execute(f"PRAGMA table_info({table})"))
                types = ["INTEGER" if name in integer_columns else "REAL" if name in {"expires", "changed_at"}
                         else "TEXT" for name in columns]
                if [row[1] for row in info] != columns or [row[2] for row in info] != types:
                    raise FinancialError("Existing ledger schema differs")
            if db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise FinancialError("Existing ledger integrity failed")

    def _bootstrap(self, expected_opening: int, recovery: bool) -> None:
        with self.ledger.atomic() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS financial_cohorts_v2 (
                cohort TEXT PRIMARY KEY, config BLOB NOT NULL, config_sha TEXT NOT NULL,
                historical BLOB NOT NULL, historical_sha TEXT NOT NULL, opening INTEGER NOT NULL,
                halted INTEGER NOT NULL DEFAULT 0, clock REAL NOT NULL DEFAULT 0)""")
            db.execute("""CREATE TABLE IF NOT EXISTS financial_tasks_v2 (
                cohort TEXT NOT NULL, task_id TEXT PRIMARY KEY, work BLOB NOT NULL)""")
            db.execute("""CREATE TABLE IF NOT EXISTS financial_requests_v2 (
                cohort TEXT NOT NULL, actor TEXT NOT NULL, request_id TEXT NOT NULL,
                identity BLOB NOT NULL, identity_sha TEXT NOT NULL, reply BLOB NOT NULL,
                reply_sha TEXT NOT NULL, PRIMARY KEY(cohort,actor,request_id))""")
            db.execute("""CREATE TABLE IF NOT EXISTS financial_actions_v2 (
                cohort TEXT NOT NULL, actor TEXT NOT NULL, action_id TEXT NOT NULL,
                request_id TEXT NOT NULL, task_id TEXT NOT NULL, binding BLOB NOT NULL,
                binding_sha TEXT NOT NULL, worker_request BLOB NOT NULL,
                reservation_id TEXT NOT NULL UNIQUE, executor_owner TEXT NOT NULL, state TEXT NOT NULL,
                PRIMARY KEY(cohort,actor,action_id), UNIQUE(cohort,actor,request_id))""")
            prior = db.execute("SELECT * FROM financial_cohorts_v2 WHERE cohort=?", (self.cohort_id,)).fetchone()
            if prior is not None:
                if not recovery:
                    raise FinancialError("Existing cohort requires explicit recovery")
                if prior["config"] != canonical_payload(self.config) or prior["config_sha"] != self.config_sha256:
                    raise FinancialError("Immutable cohort configuration changed")
                if prior["historical_sha"] != _sha(prior["historical"]):
                    raise FinancialError("Historical adoption evidence changed")
                historical = strict_loads(prior["historical"], max_bytes=MAX_BYTES)
                for table, rows in historical.items():
                    current = [dict(row) for row in db.execute(f"SELECT * FROM {table}")]
                    if any(row not in current for row in rows):
                        raise FinancialError("Historical ledger rows changed")
                self.failed_closed = bool(prior["halted"])
            else:
                if recovery:
                    raise FinancialError("Cannot recover an unregistered cohort")
                usage = db.execute("SELECT COALESCE(SUM(COALESCE(spent,amount)),0) FROM reservations").fetchone()[0]
                now = self._sample_time()
                if (usage != expected_opening
                        or db.execute("SELECT 1 FROM reservations WHERE spent IS NULL LIMIT 1").fetchone()
                        or db.execute("SELECT 1 FROM tasks WHERE status='submitting' OR (status='claimed' AND expires>?) LIMIT 1", (now,)).fetchone()
                        or db.execute("SELECT 1 FROM intents WHERE state='pending' LIMIT 1").fetchone()
                        or db.execute("SELECT 1 FROM financial_actions_v2 WHERE state IN ('pending','publication_pending','unknown') LIMIT 1").fetchone()
                        or db.execute("SELECT 1 FROM financial_cohorts_v2 WHERE halted=1 LIMIT 1").fetchone()):
                    raise FinancialError("Fresh cohort requires exact quiescent opening usage")
                historical = {table: [dict(row) for row in db.execute(f"SELECT * FROM {table} ORDER BY rowid")]
                              for table in _BASE_SCHEMA}
                raw = canonical_payload(historical)
                db.execute("INSERT INTO financial_cohorts_v2 VALUES (?,?,?,?,?,?,0,0)",
                           (self.cohort_id, canonical_payload(self.config), self.config_sha256, raw, _sha(raw), usage))
                for task_id, spec in self.task_specs.items():
                    self.ledger.add_task(task_id)
                    db.execute("INSERT INTO financial_tasks_v2 VALUES (?,?,?)",
                               (self.cohort_id, task_id, canonical_payload({"context": spec["context"], "work": spec["work"]})))
            self._now(db)
            actual = {row["task_id"]: strict_loads(row["work"]) for row in db.execute(
                "SELECT * FROM financial_tasks_v2 WHERE cohort=?", (self.cohort_id,))}
            if actual != {task: {"context": spec["context"], "work": spec["work"]} for task, spec in self.task_specs.items()}:
                raise FinancialError("Retained task membership differs")
            self._audit_membership(db)

    def _audit_membership(self, db: sqlite3.Connection) -> None:
        """Check both directions, so deleted membership never restores allowance."""
        actions = {(row["actor"], row["request_id"]): row for row in db.execute(
            "SELECT * FROM financial_actions_v2 WHERE cohort=?", (self.cohort_id,))}
        requests = {(row["actor"], row["request_id"]): row for row in db.execute(
            "SELECT * FROM financial_requests_v2 WHERE cohort=?", (self.cohort_id,))}
        reservations = {row["id"]: row for row in db.execute("""SELECT r.* FROM reservations r
            JOIN financial_tasks_v2 t ON t.task_id=r.task_id WHERE t.cohort=?""", (self.cohort_id,))}
        admitted = set()
        try:
            for key, row in requests.items():
                reply = self._reply(row)
                if reply.state == "waiting":
                    if key in actions:
                        raise FinancialError("Waiting request has admitted work")
                    continue
                action = actions.get(key)
                binding = reply.binding
                if (action is None or binding is None or action["binding"] != encode(binding)
                        or action["binding_sha"] != identity(binding) or action["state"] != reply.state
                        or binding.reservation_id != action["reservation_id"]
                        or binding.lease.task_id != action["task_id"]
                        or binding.action.action_id != action["action_id"]
                        or binding.lease.task_id != self.task_id(binding.action.context, binding.action.work)
                        or binding.action.actor != key[0] or binding.action.request_id != key[1]):
                    raise FinancialError("Admitted request lost its exact membership")
                reservation = self._reservation(db, binding)
                if (reply.state in {"completed", "failed", "publication_pending"}
                        and reply.usage_units is not None
                        and reservation["spent"] != reply.usage_units):
                    raise FinancialError("Retained outcome usage differs from cumulative accounting")
                # Pending may already be settled between the journal callback
                # and receipt commit; it has no terminal usage assertion yet.
                admitted.add(binding.reservation_id)
            if set(actions) != {key for key, row in requests.items() if self._reply(row).state != "waiting"} or admitted != set(reservations):
                raise FinancialError("Orphan cumulative reservation or action")
        except Exception:
            self.failed_closed = True
            raise

    def _sample_time(self) -> float:
        sampled = self.clock()
        if type(sampled) not in (int, float) or not 0 <= sampled <= 1e12:
            raise FinancialError("Invalid authority clock")
        if not math.isfinite(sampled):
            raise FinancialError("Invalid authority clock")
        return float(sampled)

    def _now(self, db: sqlite3.Connection) -> float:
        sampled = self._sample_time()
        prior = db.execute("SELECT clock FROM financial_cohorts_v2 WHERE cohort=?", (self.cohort_id,)).fetchone()[0]
        now = max(float(sampled), prior)
        db.execute("UPDATE financial_cohorts_v2 SET clock=? WHERE cohort=?", (now, self.cohort_id))
        return now

    @contextmanager
    def _active(self) -> Iterator[None]:
        with self.lifecycle:
            if self.closing or os.getpid() != self.owner_pid:
                raise FinancialError("Financial authority is closing or belongs to another process")
            self.active_handlers += 1
        try:
            yield
        finally:
            with self.lifecycle:
                self.active_handlers -= 1
                self.lifecycle.notify_all()

    def close(self) -> None:
        with self.lifecycle:
            self.closing = True
            while self.active_handlers:
                self.lifecycle.wait()
        self.executor.shutdown(wait=True)
        with self.lifecycle:
            if not self.closed:
                self.owner.close()
                self.closed = True

    def task_id(self, context: Context, work: WorkKey) -> str:
        return "financial-v2-" + _digest({"context": to_dict(context), "work": to_dict(work)})

    def _actor(self, actor: str) -> None:
        if _name(actor) not in self.actors:
            raise FinancialError("Unknown authenticated actor")

    def _halted(self, db: sqlite3.Connection) -> bool:
        return self.failed_closed or bool(db.execute(
            "SELECT halted FROM financial_cohorts_v2 WHERE cohort=?", (self.cohort_id,)).fetchone()[0])

    def _guard_task(self, db: sqlite3.Connection, task_id: str) -> None:
        if db.execute("SELECT 1 FROM financial_actions_v2 WHERE cohort=? AND task_id=? AND state IN ('pending','publication_pending','unknown')",
                      (self.cohort_id, task_id)).fetchone():
            raise ClaimRejected("Task has unresolved paid work")

    def claim(self, actor: str, context: Context, work: WorkKey, *, ttl: float = 60) -> Lease:
        self._actor(actor)
        task_id = self.task_id(context, work)
        if task_id not in self.task_specs or actor not in self.task_specs[task_id]["actors"]:
            raise FinancialError("Task outside actor permissions")
        with self._active(), self.ledger.atomic() as db:
            if self._halted(db):
                raise FinancialError("Cohort is halted")
            self._guard_task(db, task_id)
            return self.ledger.claim(task_id, actor, now=self._now(db), ttl=ttl)

    def renew(self, actor: str, lease: Lease, *, ttl: float = 60) -> Lease:
        self._actor(actor)
        if lease.worker_id != actor or lease.task_id not in self.task_specs:
            raise FinancialError("Lease outside actor permissions")
        with self._active(), self.ledger.atomic() as db:
            if self._halted(db):
                raise FinancialError("Cohort is halted")
            return self.ledger.renew(lease, now=self._now(db), ttl=ttl)

    def _row(self, db: sqlite3.Connection, actor: str, request_id: str) -> sqlite3.Row | None:
        return db.execute("SELECT * FROM financial_requests_v2 WHERE cohort=? AND actor=? AND request_id=?",
                          (self.cohort_id, actor, request_id)).fetchone()

    def _reply(self, row: sqlite3.Row) -> DispatchReply:
        if _sha(row["identity"]) != row["identity_sha"] or _sha(row["reply"]) != row["reply_sha"]:
            raise FinancialError("Retained request checksum differs")
        return decode(row["reply"], DispatchReply, expected_contract_sha256=self.contract["execution_contract_sha256"])

    def _save_reply(self, db: sqlite3.Connection, actor: str, request_id: str, reply: DispatchReply) -> None:
        raw = encode(reply, execution_contract_sha256=self.contract["execution_contract_sha256"])
        db.execute("UPDATE financial_requests_v2 SET reply=?,reply_sha=? WHERE cohort=? AND actor=? AND request_id=?",
                   (raw, _sha(raw), self.cohort_id, actor, request_id))

    def lookup(self, actor: str, request_id: str) -> DispatchReply | None:
        self._actor(actor)
        _name(request_id)
        with self._active():
            with self.ledger.atomic() as db:
                row = self._row(db, actor, request_id)
                reply = self._reply(row) if row else None
            if reply is not None and (actor, request_id) in self.persistence_failed:
                return DispatchReply(request_id, reply.action_sha256, "unknown", "state_persistence_failure", reply.binding)
            if reply is not None and reply.state in {"completed", "failed"}:
                try:
                    self._verify_terminal(actor, request_id)
                except Exception:
                    retained = self._unknown(actor, request_id, "terminal_evidence_unavailable")
                    return retained or DispatchReply(request_id, reply.action_sha256, "unknown", "state_persistence_failure", reply.binding)
            return reply

    def _request(self, actor: str, action: ActionRequest, lease: Lease) -> WorkerRequest:
        if (action.actor != actor or lease.worker_id != actor
                or action.context.cohort_id != self.cohort_id
                or action.context.execution_contract_sha256 != self.contract["execution_contract_sha256"]
                or action.worker_payload_ref.producer != actor
                or action.worker_payload_ref.kind != "worker-request"):
            raise FinancialError("Action differs from authenticated actor or cohort")
        task_id = self.task_id(action.context, action.work)
        spec = self.task_specs.get(task_id)
        if (spec is None or lease.task_id != task_id or actor not in spec["actors"]
                or action.kind not in spec["kinds"] or action.profile_id not in spec["profiles"]):
            raise FinancialError("Action exceeds registered task/profile scope")
        raw = self.payloads.read_owned(actor, action.worker_payload_ref.payload_sha256)
        if type(raw) is not bytes or _sha(raw) != action.worker_payload_ref.payload_sha256:
            raise FinancialError("Owned request bytes differ")
        value = strict_loads(raw, max_bytes=600_000)
        if (canonical_payload(value) != raw or type(value) is not dict
                or set(value) != {"worker_request", "view_manifest_sha256"}
                or value["view_manifest_sha256"] != action.view_manifest_sha256):
            raise FinancialError("Worker payload must bind the exact local view")
        fields = value["worker_request"]
        if (type(fields) is not dict or set(fields) != {
            "task_id", "instructions", "allowed_paths", "files", "base_sha", "attempt", "feedback"
        } or fields["task_id"] != task_id or fields["allowed_paths"] != spec["allowed_paths"]):
            raise FinancialError("Worker task or path scope differs")
        fields["allowed_paths"] = tuple(fields["allowed_paths"])
        request = WorkerRequest(**fields)
        worker = self.workers[action.profile_id]
        if self._profile(action.profile_id, worker) != self.profiles[action.profile_id]:
            raise FinancialError("Worker profile changed")
        worker.reservation_units(request)
        return request

    def submit(self, actor: str, action: ActionRequest, lease: Lease) -> DispatchReply:
        self._actor(actor)
        if not isinstance(action, ActionRequest) or not isinstance(lease, Lease):
            raise FinancialError("Typed action and lease required")
        if action.actor != actor or lease.worker_id != actor:
            raise FinancialError("Authenticated actor differs")
        raw = canonical_payload({"action": to_dict(action), "lease": asdict(lease)})
        waiting = DispatchReply(action.request_id, identity(action), "waiting", "executor_capacity")
        with self._active():
            with self.ledger.atomic() as db:
                row = self._row(db, actor, action.request_id)
                if row is not None:
                    if row["identity"] != raw:
                        raise FinancialError("Request identity conflict")
                    prior = self._reply(row)
                    if (actor, action.request_id) in self.persistence_failed:
                        return DispatchReply(action.request_id, identity(action), "unknown", "state_persistence_failure", prior.binding)
                    if prior.state != "waiting":
                        return prior
                else:
                    db.execute("INSERT INTO financial_requests_v2 VALUES (?,?,?,?,?,?,?)",
                               (self.cohort_id, actor, action.request_id, raw, _sha(raw), encode(waiting, execution_contract_sha256=self.contract["execution_contract_sha256"]), _sha(encode(waiting, execution_contract_sha256=self.contract["execution_contract_sha256"]))))
            if not self.slots.acquire(blocking=False):
                return waiting
            scheduled = False
            try:
                try:
                    request = self._request(actor, action, lease)
                except (FileNotFoundError, KeyError):
                    return DispatchReply(action.request_id, identity(action), "waiting", "payload_unavailable")
                units = self.workers[action.profile_id].reservation_units(request)
                with self.ledger.atomic() as db:
                    row = self._row(db, actor, action.request_id)
                    if row is None or row["identity"] != raw:
                        raise FinancialError("Request identity conflict")
                    prior = self._reply(row)
                    if prior.state != "waiting":
                        return prior
                    now = self._now(db)  # Fresh only after the joined write lock.
                    self._audit_membership(db)
                    if self._halted(db):
                        return DispatchReply(action.request_id, identity(action), "waiting", "cohort_halted")
                    self._guard_task(db, lease.task_id)
                    if db.execute("SELECT 1 FROM financial_actions_v2 WHERE cohort=? AND actor=? AND action_id=?",
                                  (self.cohort_id, actor, action.action_id)).fetchone():
                        raise FinancialError("Action identity conflict")
                    committed = db.execute("""SELECT COALESCE(SUM(COALESCE(r.spent,r.amount)),0)
                        FROM reservations r JOIN financial_actions_v2 a ON a.reservation_id=r.id WHERE a.cohort=?""",
                        (self.cohort_id,)).fetchone()[0]
                    slot_committed = db.execute("""SELECT COALESCE(SUM(COALESCE(r.spent,r.amount)),0)
                        FROM reservations r JOIN financial_actions_v2 a ON a.reservation_id=r.id
                        WHERE a.cohort=? AND a.task_id=?""", (self.cohort_id, lease.task_id)).fetchone()[0]
                    if slot_committed + units > self.task_specs[lease.task_id]["max_reserved_units"]:
                        return DispatchReply(action.request_id, identity(action), "waiting", "slot_budget")
                    if committed + units > self.config["incremental_cap_micro_usd"]:
                        return DispatchReply(action.request_id, identity(action), "waiting", "cohort_budget")
                    call_id = "financial-v2-" + _digest({"identity": strict_loads(raw, max_bytes=MAX_BYTES), "config": self.config_sha256})
                    binding = DispatchBinding(action, lease, worker_request_digest(request), _digest(self.profiles[action.profile_id]),
                                              self.config_sha256, call_id, call_id, units)
                    try:
                        self.ledger.reserve(call_id, lease, units, now=now)
                    except BudgetExceeded:
                        return DispatchReply(action.request_id, identity(action), "waiting", "global_budget")
                    pending = DispatchReply(action.request_id, identity(action), "pending", "admitted", binding)
                    fields = asdict(request)
                    fields["allowed_paths"] = list(request.allowed_paths)
                    db.execute("INSERT INTO financial_actions_v2 VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                               (self.cohort_id, actor, action.action_id, action.request_id, lease.task_id, encode(binding), identity(binding),
                                canonical_payload(fields), call_id, self.owner_id, "pending"))
                    self._save_reply(db, actor, action.request_id, pending)
                try:
                    self._boundary("after_admission", action.request_id)
                    self.executor.submit(self._execute, actor, action.request_id)
                    scheduled = True
                except BaseException:
                    self._unknown(actor, action.request_id, "executor_not_started")
                    raise
                return pending
            except FinancialError:
                if self.failed_closed:
                    # The failed admission transaction rolled back. Retain its
                    # integrity halt separately before returning the rejection.
                    with self.ledger.atomic() as db:
                        self._halt(db)
                raise
            finally:
                if not scheduled:
                    self.slots.release()

    def _boundary(self, name: str, request_id: str) -> None:
        if self.crash_hook is not None:
            self.crash_hook(name, request_id)

    def _action(self, actor: str, request_id: str) -> tuple[dict, DispatchBinding, DispatchReply, WorkerRequest]:
        with self.ledger.atomic() as db:
            row = db.execute("SELECT * FROM financial_actions_v2 WHERE cohort=? AND actor=? AND request_id=?",
                             (self.cohort_id, actor, request_id)).fetchone()
            request_row = self._row(db, actor, request_id)
            if row is None or request_row is None:
                raise FinancialError("Missing admitted identity")
            action = dict(row)
            binding = decode(action["binding"], DispatchBinding, expected_contract_sha256=self.contract["execution_contract_sha256"])
            reply = self._reply(request_row)
            fields = strict_loads(action["worker_request"], max_bytes=600_000)
            fields["allowed_paths"] = tuple(fields["allowed_paths"])
            request = WorkerRequest(**fields)
            if (identity(binding) != action["binding_sha"] or reply.binding != binding
                    or binding.authority_config_sha256 != self.config_sha256
                    or binding.action.actor != actor or binding.action.request_id != request_id
                    or binding.reservation_id != action["reservation_id"]
                    or binding.action.action_id != action["action_id"]
                    or binding.action.actor != action["actor"]
                    or binding.action.context.cohort_id != self.cohort_id
                    or action["state"] != reply.state
                    or binding.profile_sha256 != _digest(self.profiles[binding.action.profile_id])
                    or binding.call_id != binding.reservation_id
                    or binding.call_id != "financial-v2-" + _digest({"identity": strict_loads(request_row["identity"], max_bytes=MAX_BYTES), "config": self.config_sha256})
                    or binding.lease.task_id != action["task_id"]
                    or binding.lease.task_id != self.task_id(binding.action.context, binding.action.work)
                    or binding.lease.task_id not in self.task_specs
                    or binding.normalized_worker_request_sha256 != worker_request_digest(request)
                    or request_row["identity"] != canonical_payload({"action": to_dict(binding.action), "lease": asdict(binding.lease)})):
                raise FinancialError("Retained dispatch binding differs")
            self._reservation(db, binding)
            return action, binding, reply, request

    def _reservation(self, db: sqlite3.Connection, binding: DispatchBinding) -> sqlite3.Row:
        row = db.execute("SELECT * FROM reservations WHERE id=?", (binding.reservation_id,)).fetchone()
        if (row is None or (row["task_id"], row["epoch"], row["amount"]) !=
                (binding.lease.task_id, binding.lease.epoch, binding.reserved_units)
                or (row["spent"] is None and row["state"] != "reserved")
                or (row["spent"] is not None and (row["state"] != "settled" or not 0 <= row["spent"] <= row["amount"]))):
            raise FinancialError("Cumulative reservation binding differs")
        return row

    def _execute(self, actor: str, request_id: str) -> None:
        try:
            self._replay(actor, request_id, recovery=False)
        except BaseException:
            try:
                self._verify_terminal(actor, request_id)
            except Exception:
                self._unknown(actor, request_id, "execution_interrupted")
        finally:
            self.slots.release()

    def _replay(self, actor: str, request_id: str, *, recovery: bool) -> DispatchReply:
        action, binding, _, request = self._action(actor, request_id)
        worker = self.workers[binding.action.profile_id]
        if _digest(self._profile(binding.action.profile_id, worker)) != binding.profile_sha256:
            raise FinancialError("Bound profile differs")

        def reserve() -> None:
            if os.getpid() != self.owner_pid:
                raise FinancialError("Inherited authority cannot invoke in another process")
            if recovery:
                raise JournalUnknownOutcome("Committed admission has no recoverable result")
            with self.ledger.atomic() as db:
                reservation = self._reservation(db, binding)
                if action["executor_owner"] != self.owner_id or reservation["spent"] is not None:
                    raise FinancialError("Executor does not own this reservation")
                if self._halted(db):
                    raise FinancialError("Halt prevents a not-yet-invoked worker from entering")
                # Already-entered calls still settle through the separate callback.
                Ledger._validate(db, binding.lease, self._now(db))

        def invoke():
            if recovery:
                raise JournalUnknownOutcome("Recovery never invokes provider work")
            self._boundary("before_invoke", request_id)
            reserve()
            return worker.run(request)

        def settle(usage: int) -> None:
            with self.ledger.atomic() as db:
                self._reservation(db, binding)
                self.ledger.settle(binding.reservation_id, usage)
            self._boundary("after_settlement", request_id)

        usage: int | None
        try:
            outcome = self.journal.execute(binding.call_id, request, binding.reservation_id, invoke, reserve, settle,
                                          on_persisted=lambda: self._boundary("after_result_persisted", request_id))
            payload, state, usage, halt = asdict(outcome), "completed", outcome.usage_units, bool(outcome.metadata.get("halt"))
        except WorkerFailure as failure:
            payload = {"message": str(failure), "usage_units": failure.usage_units, "metadata": failure.metadata}
            usage = failure.usage_units
            state, halt = ("unknown" if usage is None else "failed"), (usage is None or bool(failure.metadata.get("halt")))
        data = canonical_payload({"kind": "result" if state == "completed" else "failure", "payload": payload})
        digest = _sha(data)
        try:
            if self.payloads.put_owned(actor, data) != digest:
                raise FinancialError("Result publication differs")
            self._boundary("after_result_publication", request_id)
        except Exception:
            retained = self._unknown(actor, request_id, "result_publication", result_sha=digest, usage=usage)
            return retained or DispatchReply(request_id, identity(binding.action), "unknown", "state_persistence_failure", binding)
        reply = DispatchReply(request_id, identity(binding.action), state, "durable_outcome", binding, digest, usage)
        with self.ledger.atomic() as db:
            if halt:
                self._halt(db)
            self._save_reply(db, actor, request_id, reply)
            db.execute("UPDATE financial_actions_v2 SET state=? WHERE cohort=? AND actor=? AND request_id=?",
                       (state, self.cohort_id, actor, request_id))
            self._boundary("before_receipt_commit", request_id)
        self._boundary("after_receipt_commit", request_id)
        return reply

    def _halt(self, db: sqlite3.Connection) -> None:
        self.failed_closed = True
        db.execute("UPDATE financial_cohorts_v2 SET halted=1 WHERE cohort=?", (self.cohort_id,))

    def _unknown(self, actor: str, request_id: str, reason: str, *, result_sha: str | None = None,
                 usage: int | None = None) -> DispatchReply | None:
        self.failed_closed = True
        try:
            with self.ledger.atomic() as db:
                row = self._row(db, actor, request_id)
                if row is None:
                    return None
                old = self._reply(row)
                state = "publication_pending" if result_sha is not None else "unknown"
                reply = DispatchReply(request_id, old.action_sha256, state, reason, old.binding, result_sha, usage)
                self._save_reply(db, actor, request_id, reply)
                db.execute("UPDATE financial_actions_v2 SET state=? WHERE cohort=? AND actor=? AND request_id=?",
                           (state, self.cohort_id, actor, request_id))
                self._halt(db)
                return reply
        except Exception:
            self.persistence_failed.add((actor, request_id))
            return None  # In-memory fence persists; retained pending admission fences recovery.

    def _recover(self) -> None:
        with self.ledger.atomic() as db:
            actions = [(row["actor"], row["request_id"], row["state"]) for row in db.execute(
                "SELECT * FROM financial_actions_v2 WHERE cohort=? ORDER BY actor,request_id", (self.cohort_id,))]
        for actor, request_id, state in actions:
            try:
                if state in {"completed", "failed"}:
                    self._verify_terminal(actor, request_id)
                else:
                    self._replay(actor, request_id, recovery=True)
            except Exception:
                self._unknown(actor, request_id, "retained_evidence_unavailable")

    def _verify_terminal(self, actor: str, request_id: str) -> dict:
        _, binding, reply, request = self._action(actor, request_id)
        if reply.state not in {"completed", "failed"}:
            raise FinancialError("No terminal known outcome")
        paths = self.journal.paths(binding.call_id)
        journal_identity = {"schema_version": 1, "call_id": binding.call_id, "reservation_id": binding.reservation_id,
                            "request_sha256": _sha(journal_module._bytes(asdict(request)))}
        owner, _ = journal_module._read(self.journal.root / (_sha(binding.reservation_id.encode()) + ".reservation.json"))
        retained_request, _ = journal_module._read(paths["request"])
        result, result_raw = journal_module._read(paths["result"])
        settled, _ = journal_module._read(paths["settled"])
        kind = "result" if reply.state == "completed" else "failure"
        # Compare canonical journal bytes rather than reparsing our own ASCII
        # encoding under a wire limit. Escaping may expand an already admitted,
        # digest-bound UTF-8 request; its admission limits remain unchanged.
        # Byte equality also preserves JSON scalar types (unlike True == 1).
        expected_request = journal_module._bytes({**journal_identity, "request": asdict(request)})
        if (owner != journal_identity or journal_module._bytes(retained_request) != expected_request
                or set(result) != {*journal_identity, "kind", "payload", "payload_sha256"}
                or any(result.get(key) != value for key, value in journal_identity.items())
                or result["kind"] != kind or result["payload_sha256"] != _sha(journal_module._bytes(result["payload"]))):
            raise FinancialError("Terminal journal evidence differs")
        outcome = journal_module._result(result["payload"]) if kind == "result" else journal_module._failure(result["payload"])
        expected_settled = {**journal_identity, "result_sha256": _sha(result_raw), "usage_units": outcome.usage_units}
        data = canonical_payload({"kind": kind, "payload": result["payload"]})
        if (settled != expected_settled or outcome.usage_units != reply.usage_units
                or _sha(data) != reply.result_payload_sha256
                or self.payloads.read_owned(actor, _sha(data)) != data):
            raise FinancialError("Terminal result publication or settlement differs")
        with self.ledger.atomic() as db:
            reservation = self._reservation(db, binding)
            if reservation["spent"] != outcome.usage_units or (outcome.metadata.get("halt") and not self._halted(db)):
                raise FinancialError("Terminal result disagrees with cumulative accounting")
        return {"binding": binding, "action": binding.action, "reply": reply, "result": outcome,
                "worker_request": request, "result_payload": strict_loads(data, max_bytes=MAX_BYTES)}

    def verified_terminal(self, binding: DispatchBinding) -> dict:
        """Trusted release seam only; never expose this as a peer mutation/RPC."""
        with self._active():
            with self.ledger.atomic() as db:
                row = self._row(db, binding.action.actor, binding.action.request_id)
                reply = self._reply(row) if row is not None else None
                if reply is None or reply.state not in {"completed", "failed"}:
                    raise FinancialError("No terminal known outcome")
            try:
                proof = self._verify_terminal(binding.action.actor, binding.action.request_id)
            except Exception:
                self._unknown(binding.action.actor, binding.action.request_id, "terminal_evidence_unavailable")
                raise
            if proof["binding"] != binding or proof["reply"].state != "completed":
                raise FinancialError("Release requires this exact successful dispatch")
            return proof
