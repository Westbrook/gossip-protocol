"""Transactional local task claims, reserved credits and promotion intents.

Units are generic test credits, not estimated dollars. All callers must use the
ledger; this is a single-host authority, not a distributed consensus service.
An intent freezes its task attempts until its Git CAS is reconciled. Expired
claims cannot start an intent. No SQLite transaction is held while Git runs.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, dataclass
import hashlib
import fcntl
import json
import math
from pathlib import Path
import sqlite3
from typing import Iterator, Sequence


class ClaimRejected(ValueError):
    pass


class BudgetExceeded(ValueError):
    pass


@dataclass(frozen=True)
class Lease:
    task_id: str
    worker_id: str
    epoch: int
    expires_at: float


class Ledger:
    def __init__(self, path: Path, budget_units: int | None = None):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._bootstrap() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY, status TEXT NOT NULL DEFAULT 'ready',
                    worker TEXT, epoch INTEGER NOT NULL DEFAULT 0, expires REAL,
                    accepted_commit TEXT, intent_id TEXT);
                CREATE TABLE IF NOT EXISTS dependencies (
                    task_id TEXT NOT NULL REFERENCES tasks(id),
                    prerequisite TEXT NOT NULL REFERENCES tasks(id),
                    PRIMARY KEY(task_id, prerequisite));
                CREATE TABLE IF NOT EXISTS reservations (
                    id TEXT PRIMARY KEY, task_id TEXT NOT NULL, epoch INTEGER NOT NULL,
                    amount INTEGER NOT NULL, spent INTEGER, state TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS intents (
                    id TEXT PRIMARY KEY, repository TEXT NOT NULL, old_head TEXT NOT NULL,
                    new_head TEXT NOT NULL, leases TEXT NOT NULL, state TEXT NOT NULL,
                    result TEXT);
                CREATE TABLE IF NOT EXISTS budget_changes (
                    id TEXT PRIMARY KEY, old_budget INTEGER NOT NULL,
                    new_budget INTEGER NOT NULL, reason TEXT NOT NULL,
                    changed_at REAL NOT NULL, spent_or_reserved INTEGER NOT NULL);
            """)
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT value FROM settings WHERE key='budget'").fetchone()
            if row is None:
                if type(budget_units) is not int or budget_units < 0:
                    raise ValueError("A new ledger requires a nonnegative integer budget")
                db.execute("INSERT INTO settings VALUES ('budget', ?)", (budget_units,))
            elif budget_units is not None and row[0] != budget_units:
                raise ValueError("Existing ledger budget cannot be silently changed")

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA synchronous=FULL")
        return db

    @contextmanager
    def _bootstrap(self) -> Iterator[sqlite3.Connection]:
        # Concurrent process startup must not race journal-mode conversion or
        # the first budget insertion. Later connections use ordinary SQLite
        # transactions and do not change journal mode.
        with open(str(self.path) + ".schema.lock", "a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            with self._session() as db:
                db.execute("PRAGMA journal_mode=WAL")
                yield db

    @contextmanager
    def _session(self) -> Iterator[sqlite3.Connection]:
        db = self._connect()
        try:
            with db:
                yield db
        finally:
            db.close()

    @staticmethod
    def _time(now: float, ttl: float = 1) -> None:
        if not math.isfinite(now) or not math.isfinite(ttl) or ttl <= 0:
            raise ValueError("Finite timestamps and positive finite duration required")

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        db = self._connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def add_task(self, task_id: str, dependencies: Sequence[str] = ()) -> None:
        if not task_id or task_id in dependencies:
            raise ValueError("Task IDs must be nonempty and cannot depend on themselves")
        with self._transaction() as db:
            db.execute("INSERT INTO tasks (id) VALUES (?)", (task_id,))
            for prerequisite in dependencies:
                db.execute("INSERT INTO dependencies VALUES (?,?)", (task_id, prerequisite))

    def claim(self, task_id: str, worker_id: str, *, now: float, ttl: float) -> Lease:
        self._time(now, ttl)
        if not worker_id or ttl <= 0:
            raise ValueError("A worker and positive lease duration are required")
        with self._transaction() as db:
            row = db.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
            if row is None:
                raise ClaimRejected("Unknown task")
            if row["status"] in {"complete", "submitting"}:
                raise ClaimRejected("Task is complete or an integration intent is pending")
            if row["status"] == "claimed" and row["expires"] > now:
                raise ClaimRejected("Task already has an unexpired owner")
            blocked = db.execute("""SELECT 1 FROM dependencies d JOIN tasks t
                ON d.prerequisite=t.id WHERE d.task_id=? AND t.status!='complete'""", (task_id,)).fetchone()
            if blocked:
                raise ClaimRejected("Required dependency has not been accepted")
            lease = Lease(task_id, worker_id, row["epoch"] + 1, now + ttl)
            db.execute("UPDATE tasks SET status='claimed',worker=?,epoch=?,expires=? WHERE id=?",
                       (worker_id, lease.epoch, lease.expires_at, task_id))
            return lease

    @staticmethod
    def _validate(db: sqlite3.Connection, lease: Lease, now: float) -> sqlite3.Row:
        Ledger._time(now)
        row = db.execute("SELECT * FROM tasks WHERE id=?", (lease.task_id,)).fetchone()
        if (row is None or row["status"] != "claimed" or row["epoch"] != lease.epoch
                or row["worker"] != lease.worker_id or row["expires"] <= now):
            raise ClaimRejected("Expired, superseded, or unavailable task attempt")
        return row

    def validate(self, lease: Lease, *, now: float) -> None:
        with self._transaction() as db:
            self._validate(db, lease, now)

    def renew(self, lease: Lease, *, now: float, ttl: float) -> Lease:
        self._time(now, ttl)
        if ttl <= 0:
            raise ValueError("Lease duration must be positive")
        with self._transaction() as db:
            self._validate(db, lease, now)
            db.execute("UPDATE tasks SET expires=? WHERE id=?", (now + ttl, lease.task_id))
            return Lease(lease.task_id, lease.worker_id, lease.epoch, now + ttl)

    def reserve(self, reservation_id: str, lease: Lease, units: int, *, now: float) -> None:
        if not reservation_id or type(units) is not int or units < 0:
            raise ValueError("Reservation ID and nonnegative integer units are required")
        with self._transaction() as db:
            self._validate(db, lease, now)
            prior = db.execute("SELECT * FROM reservations WHERE id=?", (reservation_id,)).fetchone()
            if prior:
                if (prior["task_id"], prior["epoch"], prior["amount"]) != (lease.task_id, lease.epoch, units):
                    raise ValueError("Reservation ID was reused with different parameters")
                return
            committed = db.execute("SELECT COALESCE(SUM(COALESCE(spent,amount)),0) FROM reservations").fetchone()[0]
            cap = db.execute("SELECT value FROM settings WHERE key='budget'").fetchone()[0]
            if committed + units > cap:
                raise BudgetExceeded("Budget is fully spent or reserved")
            db.execute("INSERT INTO reservations VALUES (?,?,?,?,NULL,'reserved')",
                       (reservation_id, lease.task_id, lease.epoch, units))

    def settle(self, reservation_id: str, spent: int) -> None:
        """Trusted accounting records actual spend; expired workers still cost money."""
        with self._transaction() as db:
            row = db.execute("SELECT * FROM reservations WHERE id=?", (reservation_id,)).fetchone()
            if row is None or type(spent) is not int or not 0 <= spent <= row["amount"]:
                raise ValueError("Actual spend must fit an existing reservation")
            if row["spent"] is not None and row["spent"] != spent:
                raise ValueError("Settled spend is immutable")
            db.execute("UPDATE reservations SET spent=?,state='settled' WHERE id=?", (spent, reservation_id))

    def budget(self) -> dict:
        with self._session() as db:
            cap = db.execute("SELECT value FROM settings WHERE key='budget'").fetchone()[0]
            committed = db.execute("SELECT COALESCE(SUM(COALESCE(spent,amount)),0) FROM reservations").fetchone()[0]
            return {"limit": cap, "spent_or_reserved": committed, "remaining": cap - committed}

    def increase_budget(self, change_id: str, new_budget_units: int, *,
                        expected_old: int, reason: str, now: float) -> dict:
        """Explicitly raise a quiescent ledger's cap and retain an audit receipt.

        The caller owns authorization and the meaning of the integer units.
        Every argument, including ``now``, is part of the idempotency contract;
        retry an uncertain outcome with the original arguments. A successful
        retry returns that original receipt even if later ledger work started.
        Settled charges, task state, and promotion history are never rewritten.
        """
        if not isinstance(change_id, str) or not change_id.strip():
            raise ValueError("A nonempty budget change ID is required")
        if (type(expected_old) is not int or type(new_budget_units) is not int
                or not 0 <= expected_old < new_budget_units <= 2**63 - 1):
            raise ValueError("Budget must increase between nonnegative SQLite integer units")
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("A nonempty reason for the budget increase is required")
        if type(now) not in (int, float) or now < 0:
            raise ValueError("A finite nonnegative budget change timestamp is required")
        self._time(now)
        with self._transaction() as db:
            prior = db.execute("SELECT * FROM budget_changes WHERE id=?", (change_id,)).fetchone()
            if prior is not None:
                if (prior["old_budget"], prior["new_budget"], prior["reason"], prior["changed_at"]) != (
                        expected_old, new_budget_units, reason, now):
                    raise ValueError("Budget change ID was reused with different parameters")
                return dict(prior)
            cap = db.execute("SELECT value FROM settings WHERE key='budget'").fetchone()[0]
            if cap != expected_old:
                raise ValueError("Budget changed since the expected old cap")
            if db.execute("SELECT 1 FROM reservations WHERE spent IS NULL OR state!='settled' LIMIT 1").fetchone():
                raise ValueError("Budget cannot increase while reservations are unsettled")
            if db.execute("SELECT 1 FROM intents WHERE state='pending' LIMIT 1").fetchone():
                raise ValueError("Budget cannot increase while promotion intents are pending")
            committed = db.execute("SELECT COALESCE(SUM(spent),0) FROM reservations").fetchone()[0]
            db.execute("UPDATE settings SET value=? WHERE key='budget' AND value=?",
                       (new_budget_units, expected_old))
            db.execute("INSERT INTO budget_changes VALUES (?,?,?,?,?,?)",
                       (change_id, expected_old, new_budget_units, reason, now, committed))
            return dict(db.execute("SELECT * FROM budget_changes WHERE id=?", (change_id,)).fetchone())

    def begin_intent(self, leases: Sequence[Lease], repository: Path,
                     old_head: str, new_head: str, *, now: float) -> dict:
        if not leases or len({v.task_id for v in leases}) != len(leases):
            raise ValueError("A promotion requires distinct task attempts")
        data = {"repository": str(Path(repository).resolve()), "old_head": old_head,
                "new_head": new_head, "leases": sorted((asdict(v) for v in leases), key=lambda v: v["task_id"])}
        intent_id = hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()
        with self._transaction() as db:
            # Pending and successful publications are idempotent. A rejected
            # attempt keeps its receipt, while a valid retry gets a new durable
            # generation rather than being permanently stuck on that receipt.
            while True:
                prior = db.execute("SELECT * FROM intents WHERE id=?", (intent_id,)).fetchone()
                if prior is None:
                    break
                if prior["state"] != "rejected":
                    return dict(prior)
                intent_id = hashlib.sha256(intent_id.encode()).hexdigest()
            for lease in leases:
                self._validate(db, lease, now)
            db.execute("INSERT INTO intents VALUES (?,?,?,?,?,'pending',NULL)",
                       (intent_id, data["repository"], old_head, new_head, json.dumps(data["leases"])))
            for lease in leases:
                db.execute("UPDATE tasks SET status='submitting',intent_id=? WHERE id=?", (intent_id, lease.task_id))
            return dict(db.execute("SELECT * FROM intents WHERE id=?", (intent_id,)).fetchone())

    def finish_intent(self, intent_id: str, accepted: bool, detail: str) -> None:
        with self._transaction() as db:
            row = db.execute("SELECT * FROM intents WHERE id=?", (intent_id,)).fetchone()
            if row is None:
                raise ValueError("Unknown intent")
            target = "accepted" if accepted else "rejected"
            if row["state"] != "pending":
                if row["state"] != target:
                    raise ValueError("Promotion outcome is immutable")
                return
            for lease in json.loads(row["leases"]):
                task = db.execute("SELECT * FROM tasks WHERE id=?", (lease["task_id"],)).fetchone()
                if task["intent_id"] != intent_id or task["epoch"] != lease["epoch"]:
                    raise ValueError("Promotion ownership invariant violated")
                db.execute("UPDATE tasks SET status=?,accepted_commit=?,intent_id=NULL WHERE id=?",
                           ("complete" if accepted else "claimed", row["new_head"] if accepted else None, lease["task_id"]))
            db.execute("UPDATE intents SET state=?,result=? WHERE id=?", (target, detail, intent_id))

    def pending_intents(self) -> list[dict]:
        with self._session() as db:
            return [dict(r) for r in db.execute("SELECT * FROM intents WHERE state='pending' ORDER BY id")]

    def task(self, task_id: str) -> dict:
        with self._session() as db:
            row = db.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
            if row is None:
                raise KeyError(task_id)
            return dict(row)
