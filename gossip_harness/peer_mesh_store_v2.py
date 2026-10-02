"""Bounded, append-only local evidence for the v2 mesh.

Only explicit subscriptions reserve remote payload storage. Each process owns
one root; same-database restart preserves commands, but reconstructing a lost
node database from peer history is deliberately unsupported. Opening audits the
whole retained store; normal operations read only their relevant rows/chunks.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, dataclass
import fcntl
import hashlib
from pathlib import Path
import sqlite3
import threading
from typing import Any, Iterator

from .peer_project_contract_v2 import EvidenceRef, identifier, sha256
from .peer_store_v1 import StoreError, canonical_bytes, strict_loads

PROTOCOL = "peer-mesh-v2"
NOTICE_BYTES = 24_000
_SCHEMA = {
    "metadata": "CREATE TABLE metadata (key TEXT PRIMARY KEY, value BLOB NOT NULL)",
    "events": """CREATE TABLE events (
        arrival INTEGER PRIMARY KEY, event_id TEXT NOT NULL UNIQUE,
        producer TEXT NOT NULL, sequence INTEGER NOT NULL, sha TEXT NOT NULL,
        notice BLOB NOT NULL, UNIQUE(producer, sequence))""",
    "events_sha": "CREATE INDEX events_sha ON events(sha)",
    "commands": """CREATE TABLE commands (command_id TEXT PRIMARY KEY,
        request_sha TEXT NOT NULL, event_id TEXT NOT NULL UNIQUE REFERENCES events(event_id))""",
    "payloads": """CREATE TABLE payloads (sha TEXT PRIMARY KEY, size INTEGER NOT NULL,
        descriptor BLOB NOT NULL, complete INTEGER NOT NULL)""",
    "chunks": """CREATE TABLE chunks (sha TEXT NOT NULL REFERENCES payloads(sha),
        idx INTEGER NOT NULL, data BLOB NOT NULL, PRIMARY KEY(sha, idx))""",
}


class MeshError(StoreError):
    """Invalid configuration, evidence, durable state, or wire operation."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise MeshError(message)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value, max_bytes=2_097_152)).hexdigest()


@dataclass(frozen=True)
class MeshLimits:
    max_events: int = 4096
    max_payloads: int = 512
    max_reserved_bytes: int = 268_435_456
    max_payload_bytes: int = 8_388_608
    chunk_bytes: int = 65_536
    notice_batch: int = 8
    chunk_batch: int = 4
    max_frame_bytes: int = 1_048_576
    max_handlers: int = 16
    deadline_seconds: float = 2.0

    def __post_init__(self) -> None:
        bounds = {
            "max_events": (1, 16_384), "max_payloads": (1, 4096),
            "max_reserved_bytes": (1, 1_073_741_824),
            "max_payload_bytes": (1, 8_388_608), "chunk_bytes": (1024, 65_536),
            "notice_batch": (1, 16), "chunk_batch": (1, 8),
            "max_frame_bytes": (65_536, 2_097_152), "max_handlers": (1, 32),
        }
        for name, (low, high) in bounds.items():
            value = getattr(self, name)
            require(type(value) is int and low <= value <= high, f"Invalid limit: {name}")
        require(type(self.deadline_seconds) in (float, int)
                and 0.1 <= self.deadline_seconds <= 10, "Invalid frame deadline")
        require(self.max_payload_bytes <= self.chunk_bytes * 256, "Too many payload chunks")
        envelope = self.notice_batch * NOTICE_BYTES + self.chunk_batch * (
            (self.chunk_bytes + 2) // 3 * 4 + 2048) + 8192
        require(envelope <= self.max_frame_bytes, "Frame limit cannot hold configured batches")


class MeshStore:
    """WAL/FULL store; root lock and thread lock last until close()."""

    def __init__(self, root: Path, identity: dict[str, Any], limits: MeshLimits) -> None:
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.path = self.root / "mesh.sqlite"
        self.limits, self.identity = limits, identity
        self.node_id = identity["node_id"]
        self.roster = tuple(identity["roster"])
        self.lock = threading.RLock()
        self.closed = False
        self._arrived_cache: list[EvidenceRef] = []
        lock_path = self.root / "mesh.owner.lock"
        require(not lock_path.is_symlink(), "Store lock may not be a symlink")
        self.owner = lock_path.open("a+b")
        try:
            fcntl.flock(self.owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, BlockingIOError) as error:
            self.owner.close()
            raise MeshError("Store already has an owner") from error
        try:
            for suffix in ("", "-wal", "-shm", "-journal"):
                require(not Path(str(self.path) + suffix).is_symlink(), "Database symlink refused")
            existed = self.path.exists()
            require(not existed or self.path.stat().st_size > 0, "Existing empty database refused")
            self.db = sqlite3.connect(self.path, timeout=limits.deadline_seconds,
                                      isolation_level=None, check_same_thread=False)
            self.db.execute("PRAGMA foreign_keys=ON")
            self.db.execute("PRAGMA synchronous=FULL")
            require(self.db.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal", "WAL unavailable")
            with self._transaction() as db:
                tables = {row[0] for row in db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
                expected = {"metadata", "events", "commands", "payloads", "chunks"}
                require((not existed and not tables) or tables == expected, "Foreign or incomplete store")
                if not existed:
                    for statement in _SCHEMA.values():
                        db.execute(statement)
                    db.executemany("INSERT INTO metadata VALUES (?, ?)", [
                        ("identity", canonical_bytes(identity, max_bytes=32_000)),
                        ("next_sequence", b"1"), ("membership", b"{}"), ("blocked", b"[]")])
            self.audit()
        except BaseException:
            if hasattr(self, "db"):
                self.db.close()
            self.owner.close()
            raise

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self.lock:
            require(not self.closed, "Store is closed")
            try:
                self.db.execute("BEGIN IMMEDIATE")
                yield self.db
                self.db.commit()
            except BaseException:
                self.db.rollback()
                raise

    def close(self) -> None:
        with self.lock:
            if not self.closed:
                self.db.close()
                self.owner.close()
                self.closed = True

    def _descriptor(self, notice: dict[str, Any]) -> dict[str, Any]:
        return {name: notice[name] for name in ("payload_sha256", "size", "chunks")}

    def _notice(self, value: Any) -> dict[str, Any]:
        fields = {"protocol", "cohort_id", "execution_contract_sha256", "producer", "sequence",
                  "kind", "payload_sha256", "size", "chunks", "event_id"}
        require(type(value) is dict and set(value) == fields, "Invalid notice fields")
        require(value["protocol"] == PROTOCOL and value["cohort_id"] == self.identity["cohort_id"]
                and value["execution_contract_sha256"] == self.identity["execution_contract_sha256"],
                "Notice execution identity mismatch")
        require(value["producer"] in self.roster, "Producer absent from roster")
        identifier(value["kind"])
        require(type(value["sequence"]) is int and 1 <= value["sequence"] < (1 << 63) - 1,
                "Invalid sequence")
        require(type(value["size"]) is int and 0 <= value["size"] <= self.limits.max_payload_bytes,
                "Invalid payload size")
        sha256(value["payload_sha256"])
        sha256(value["event_id"])
        hashes = value["chunks"]
        count = (value["size"] + self.limits.chunk_bytes - 1) // self.limits.chunk_bytes
        require(type(hashes) is list and len(hashes) == count, "Invalid chunk manifest")
        for part in hashes:
            sha256(part)
        require(value["size"] != 0 or value["payload_sha256"] == hashlib.sha256(b"").hexdigest(),
                "Invalid empty payload digest")
        body = {key: item for key, item in value.items() if key != "event_id"}
        require(digest(body) == value["event_id"], "Notice digest mismatch")
        canonical_bytes(value, max_bytes=NOTICE_BYTES)
        return value

    @staticmethod
    def ref(notice: dict[str, Any]) -> EvidenceRef:
        return EvidenceRef(notice["event_id"], notice["producer"], notice["kind"], notice["payload_sha256"])

    def _lookup(self, ref: EvidenceRef) -> dict[str, Any] | None:
        require(type(ref) is EvidenceRef, "Expected exact EvidenceRef")
        row = self.db.execute("SELECT notice FROM events WHERE event_id=?", (ref.event_id,)).fetchone()
        if row is None:
            return None
        notice = self._notice(strict_loads(row[0], max_bytes=NOTICE_BYTES))
        return notice if self.ref(notice) == ref else None

    def notice(self, ref: EvidenceRef) -> dict[str, Any] | None:
        with self.lock:
            return self._lookup(ref)

    def _reserve(self, notice: dict[str, Any]) -> bool:
        sha, size = notice["payload_sha256"], notice["size"]
        descriptor = canonical_bytes(self._descriptor(notice), max_bytes=NOTICE_BYTES)
        prior = self.db.execute("SELECT notice FROM events WHERE sha=? LIMIT 1", (sha,)).fetchone()
        if prior is not None:
            require(self._descriptor(strict_loads(prior[0], max_bytes=NOTICE_BYTES))
                    == self._descriptor(notice), "Conflicting retained payload descriptor")
        row = self.db.execute("SELECT descriptor FROM payloads WHERE sha=?", (sha,)).fetchone()
        if row is not None:
            require(row[0] == descriptor, "Conflicting payload descriptor")
            return True
        count, total = self.db.execute("SELECT count(*), coalesce(sum(size), 0) FROM payloads").fetchone()
        if count >= self.limits.max_payloads or total + size > self.limits.max_reserved_bytes:
            return False
        self.db.execute("INSERT INTO payloads VALUES (?, ?, ?, ?)",
                        (sha, size, descriptor, int(size == 0)))
        return True

    def publish(self, kind: str, payload: bytes, command_id: str) -> EvidenceRef:
        identifier(kind)
        identifier(command_id)
        require(type(payload) is bytes and len(payload) <= self.limits.max_payload_bytes,
                "Invalid publication bytes")
        sha = hashlib.sha256(payload).hexdigest()
        request_sha = digest({"kind": kind, "payload_sha256": sha})
        with self._transaction() as db:
            old = db.execute("SELECT request_sha, event_id FROM commands WHERE command_id=?",
                             (command_id,)).fetchone()
            if old is not None:
                require(old[0] == request_sha, "Conflicting command replay")
                row = db.execute("SELECT notice FROM events WHERE event_id=?", (old[1],)).fetchone()
                require(row is not None, "Lost command event")
                return self.ref(self._notice(strict_loads(row[0], max_bytes=NOTICE_BYTES)))
            require(db.execute("SELECT count(*) FROM events").fetchone()[0] < self.limits.max_events,
                    "Event capacity exhausted")
            sequence = int(db.execute("SELECT value FROM metadata WHERE key='next_sequence'").fetchone()[0])
            chunks = [payload[index:index + self.limits.chunk_bytes]
                      for index in range(0, len(payload), self.limits.chunk_bytes)]
            notice = {"protocol": PROTOCOL, "cohort_id": self.identity["cohort_id"],
                      "execution_contract_sha256": self.identity["execution_contract_sha256"],
                      "producer": self.node_id, "sequence": sequence, "kind": kind,
                      "payload_sha256": sha, "size": len(payload),
                      "chunks": [hashlib.sha256(part).hexdigest() for part in chunks]}
            notice["event_id"] = digest(notice)
            self._notice(notice)
            require(self._reserve(notice), "Payload publication capacity exhausted")
            db.execute("INSERT INTO events(event_id,producer,sequence,sha,notice) VALUES (?,?,?,?,?)",
                       (notice["event_id"], self.node_id, sequence, sha,
                        canonical_bytes(notice, max_bytes=NOTICE_BYTES)))
            for index, part in enumerate(chunks):
                prior_chunk = db.execute("SELECT data FROM chunks WHERE sha=? AND idx=?", (sha, index)).fetchone()
                require(prior_chunk is None or prior_chunk[0] == part, "Conflicting retained chunk bytes")
                db.execute("INSERT OR IGNORE INTO chunks VALUES (?,?,?)", (sha, index, part))
            db.execute("UPDATE payloads SET complete=1 WHERE sha=?", (sha,))
            db.execute("INSERT INTO commands VALUES (?,?,?)", (command_id, request_sha, notice["event_id"]))
            db.execute("UPDATE metadata SET value=? WHERE key='next_sequence'", (str(sequence + 1).encode(),))
            return self.ref(notice)

    def merge(self, values: list[dict[str, Any]]) -> int:
        require(type(values) is list and len(values) <= self.limits.notice_batch, "Invalid notice batch")
        notices = [self._notice(strict_loads(canonical_bytes(value, max_bytes=NOTICE_BYTES),
                                            max_bytes=NOTICE_BYTES)) for value in values]
        with self._transaction() as db:
            added = 0
            count = db.execute("SELECT count(*) FROM events").fetchone()[0]
            for notice in notices:
                encoded = canonical_bytes(notice, max_bytes=NOTICE_BYTES)
                row = db.execute("SELECT notice FROM events WHERE producer=? AND sequence=?",
                                 (notice["producer"], notice["sequence"])).fetchone()
                if row is not None:
                    require(row[0] == encoded, "Origin sequence equivocation")
                    continue
                require(notice["producer"] != self.node_id, "Unknown own-origin notice refused")
                require(count + added < self.limits.max_events, "Event capacity exhausted")
                prior = db.execute("SELECT notice FROM events WHERE sha=? LIMIT 1",
                                   (notice["payload_sha256"],)).fetchone()
                if prior is not None:
                    prior_notice = strict_loads(prior[0], max_bytes=NOTICE_BYTES)
                    require(self._descriptor(prior_notice) == self._descriptor(notice),
                            "Conflicting retained payload descriptor")
                db.execute("INSERT INTO events(event_id,producer,sequence,sha,notice) VALUES (?,?,?,?,?)",
                           (notice["event_id"], notice["producer"], notice["sequence"],
                            notice["payload_sha256"], encoded))
                added += 1
            return added

    def export(self, after: int) -> tuple[int, list[dict[str, Any]]]:
        require(type(after) is int and 0 <= after <= self.limits.max_events, "Invalid event cursor")
        with self.lock:
            rows = self.db.execute("SELECT arrival,notice FROM events WHERE arrival>? ORDER BY arrival LIMIT ?",
                                   (after, self.limits.notice_batch)).fetchall()
            return (rows[-1][0] if rows else after,
                    [self._notice(strict_loads(row[1], max_bytes=NOTICE_BYTES)) for row in rows])

    def arrived(self) -> tuple[EvidenceRef, ...]:
        with self.lock:
            # Immutable arrival rows are audited once, then only newly committed
            # rows enter this cache. Role polling must not rehash all history.
            rows = self.db.execute("SELECT notice FROM events WHERE arrival>? ORDER BY arrival",
                                   (len(self._arrived_cache),)).fetchall()
            added = [self.ref(self._notice(strict_loads(row[0], max_bytes=NOTICE_BYTES))) for row in rows]
            self._arrived_cache.extend(added)
            return tuple(self._arrived_cache)

    def want(self, ref: EvidenceRef) -> bool:
        with self._transaction():
            notice = self._lookup(ref)
            return notice is not None and self._reserve(notice)

    def needs(self, turn: int) -> list[dict[str, Any]]:
        """Rotate subscriptions so an unavailable object cannot starve others."""
        with self.lock:
            rows = self.db.execute("SELECT sha,descriptor FROM payloads WHERE complete=0 ORDER BY sha").fetchall()
            if not rows:
                return []
            start = turn % len(rows)
            rows = rows[start:] + rows[:start]
            wanted: list[dict[str, Any]] = []
            # One missing chunk per object per pass, then fill remaining slots.
            candidates: list[tuple[str, list[int]]] = []
            for sha, raw in rows:
                descriptor = strict_loads(raw, max_bytes=NOTICE_BYTES)
                have = {row[0] for row in self.db.execute("SELECT idx FROM chunks WHERE sha=?", (sha,))}
                candidates.append((sha, [i for i in range(len(descriptor["chunks"])) if i not in have]))
            for offset in range(self.limits.chunk_batch):
                for sha, missing in candidates:
                    if offset < len(missing):
                        wanted.append({"sha": sha, "index": missing[offset]})
                        if len(wanted) == self.limits.chunk_batch:
                            return wanted
            return wanted

    def read_chunk(self, sha: str, index: int) -> bytes | None:
        sha256(sha)
        require(type(index) is int and 0 <= index < 256, "Invalid chunk index")
        with self.lock:
            row = self.db.execute("SELECT data FROM chunks WHERE sha=? AND idx=?", (sha, index)).fetchone()
            if row is None:
                return None
            raw = self.db.execute("SELECT descriptor FROM payloads WHERE sha=?", (sha,)).fetchone()[0]
            self._check_chunk(strict_loads(raw, max_bytes=NOTICE_BYTES), index, row[0])
            return bytes(row[0])

    def _check_chunk(self, descriptor: dict[str, Any], index: int, data: bytes) -> None:
        hashes = descriptor["chunks"]
        require(type(index) is int and 0 <= index < len(hashes), "Invalid chunk index")
        size = min(self.limits.chunk_bytes, descriptor["size"] - index * self.limits.chunk_bytes)
        require(type(data) is bytes and len(data) == size
                and hashlib.sha256(data).hexdigest() == hashes[index], "Chunk integrity mismatch")

    def accept_chunk(self, sha: str, index: int, data: bytes) -> None:
        sha256(sha)
        with self._transaction() as db:
            row = db.execute("SELECT descriptor,complete FROM payloads WHERE sha=?", (sha,)).fetchone()
            require(row is not None, "Unsubscribed payload chunk")
            descriptor = strict_loads(row[0], max_bytes=NOTICE_BYTES)
            self._check_chunk(descriptor, index, data)
            prior = db.execute("SELECT data FROM chunks WHERE sha=? AND idx=?", (sha, index)).fetchone()
            require(prior is None or prior[0] == data, "Immutable chunk conflict")
            db.execute("INSERT OR IGNORE INTO chunks VALUES (?,?,?)", (sha, index, data))
            if not row[1] and db.execute("SELECT count(*) FROM chunks WHERE sha=?", (sha,)).fetchone()[0] == len(descriptor["chunks"]):
                self._read_payload(descriptor)
                db.execute("UPDATE payloads SET complete=1 WHERE sha=?", (sha,))

    def _read_payload(self, descriptor: dict[str, Any]) -> bytes:
        rows = self.db.execute("SELECT idx,data FROM chunks WHERE sha=? ORDER BY idx",
                               (descriptor["payload_sha256"],)).fetchall()
        require([row[0] for row in rows] == list(range(len(descriptor["chunks"]))), "Incomplete payload")
        for index, data in rows:
            self._check_chunk(descriptor, index, data)
        payload = b"".join(row[1] for row in rows)
        require(len(payload) == descriptor["size"]
                and hashlib.sha256(payload).hexdigest() == descriptor["payload_sha256"], "Payload integrity mismatch")
        return payload

    def resolve(self, ref: EvidenceRef) -> bytes | None:
        with self.lock:
            notice = self._lookup(ref)
            if notice is None:
                return None
            row = self.db.execute("SELECT complete FROM payloads WHERE sha=?", (ref.payload_sha256,)).fetchone()
            return self._read_payload(self._descriptor(notice)) if row is not None and row[0] == 1 else None

    def setting(self, key: str) -> Any:
        require(key in {"membership", "blocked"}, "Unknown mesh setting")
        with self.lock:
            raw = self.db.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()[0]
            return strict_loads(raw, max_bytes=32_000)

    def set_setting(self, key: str, value: Any) -> None:
        require(key in {"membership", "blocked"}, "Unknown mesh setting")
        with self._transaction() as db:
            db.execute("UPDATE metadata SET value=? WHERE key=?",
                       (canonical_bytes(value, max_bytes=32_000), key))

    def summary(self) -> dict[str, int]:
        with self.lock:
            count, reserved, complete = self.db.execute(
                "SELECT count(*),coalesce(sum(size),0),coalesce(sum(complete),0) FROM payloads").fetchone()
            return {"events": self.db.execute("SELECT count(*) FROM events").fetchone()[0],
                    "commands": self.db.execute("SELECT count(*) FROM commands").fetchone()[0],
                    "payloads": count, "reserved_bytes": reserved, "complete_payloads": complete,
                    "stored_bytes": self.db.execute("SELECT coalesce(sum(length(data)),0) FROM chunks").fetchone()[0]}

    def audit(self) -> dict[str, int]:
        """Full retained-history verification, performed once on every open."""
        with self.lock:
            limits = self.limits
            require(not self.db.execute(
                "SELECT 1 FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' AND length(sql)>4096 LIMIT 1").fetchone(),
                "Oversized store schema")
            schema = dict(self.db.execute("SELECT name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"))
            require(set(schema) == set(_SCHEMA) and all(
                type(schema[name]) is str and " ".join(schema[name].split()) == " ".join(statement.split())
                for name, statement in _SCHEMA.items()), "Foreign store schema or constraints")
            bounds = {
                "metadata": (4, {"key": ("text", 32), "value": ("blob", 32_000)}),
                "events": (limits.max_events, {"arrival": ("integer", 20), "event_id": ("text", 64),
                    "producer": ("text", 96), "sequence": ("integer", 20), "sha": ("text", 64),
                    "notice": ("blob", NOTICE_BYTES)}),
                "commands": (limits.max_events, {"command_id": ("text", 96), "request_sha": ("text", 64),
                    "event_id": ("text", 64)}),
                "payloads": (limits.max_payloads, {"sha": ("text", 64), "size": ("integer", 20),
                    "descriptor": ("blob", NOTICE_BYTES), "complete": ("integer", 1)}),
                "chunks": (limits.max_reserved_bytes // limits.chunk_bytes + limits.max_payloads,
                    {"sha": ("text", 64), "idx": ("integer", 20), "data": ("blob", limits.chunk_bytes)}),
            }
            for table, (maximum, columns) in bounds.items():
                require(self.db.execute(f"SELECT count(*) FROM {table}").fetchone()[0] <= maximum,
                        "Retained row limit exceeded")
                checks = " OR ".join(f"typeof({col})!='{kind}' OR length(CAST({col} AS BLOB))>{size}"
                                     for col, (kind, size) in columns.items())
                require(not self.db.execute(f"SELECT 1 FROM {table} WHERE {checks} LIMIT 1").fetchone(),
                        "Retained field bound exceeded")
            require(self.db.execute("PRAGMA integrity_check").fetchone()[0] == "ok", "SQLite integrity failure")
            require(not self.db.execute("PRAGMA foreign_key_check").fetchone(), "Orphan store rows")
            metadata = dict(self.db.execute("SELECT key,value FROM metadata"))
            require(set(metadata) == {"identity", "next_sequence", "membership", "blocked"}, "Invalid metadata")
            require(metadata["identity"] == canonical_bytes(self.identity, max_bytes=32_000), "Durable identity mismatch")
            notices: dict[str, dict[str, Any]] = {}
            descriptors: dict[str, dict[str, Any]] = {}
            arrivals: list[int] = []
            sequence = 1
            for arrival, event_id, producer, origin_seq, sha, raw in self.db.execute("SELECT * FROM events ORDER BY arrival"):
                notice = self._notice(strict_loads(raw, max_bytes=NOTICE_BYTES))
                require(canonical_bytes(notice, max_bytes=NOTICE_BYTES) == raw
                        and (notice["event_id"], notice["producer"], notice["sequence"], notice["payload_sha256"])
                        == (event_id, producer, origin_seq, sha), "Corrupt notice columns")
                descriptor = self._descriptor(notice)
                require(sha not in descriptors or descriptors[sha] == descriptor, "Conflicting descriptor")
                descriptors[sha] = descriptor
                notices[event_id] = notice
                arrivals.append(arrival)
                if producer == self.node_id:
                    sequence = max(sequence, origin_seq + 1)
            require(arrivals == list(range(1, len(arrivals) + 1)), "Deleted or corrupt event history")
            require(metadata["next_sequence"] == str(sequence).encode(), "Corrupt publication sequence")
            commanded: set[str] = set()
            for command, request_sha, event_id in self.db.execute("SELECT * FROM commands"):
                identifier(command)
                command_notice = notices.get(event_id)
                require(command_notice is not None and command_notice["producer"] == self.node_id, "Corrupt command provenance")
                assert command_notice is not None
                require(request_sha == digest({"kind": command_notice["kind"], "payload_sha256": command_notice["payload_sha256"]}),
                        "Corrupt command request")
                commanded.add(event_id)
            require(commanded == {key for key, value in notices.items() if value["producer"] == self.node_id},
                    "Missing local command binding")
            for sha, size, raw, complete in self.db.execute("SELECT * FROM payloads"):
                descriptor = strict_loads(raw, max_bytes=NOTICE_BYTES)
                require(sha in descriptors and descriptor == descriptors[sha] and size == descriptor["size"]
                        and raw == canonical_bytes(descriptor, max_bytes=NOTICE_BYTES) and complete in (0, 1),
                        "Invalid retained payload descriptor")
                chunks = self.db.execute("SELECT idx,data FROM chunks WHERE sha=? ORDER BY idx", (sha,)).fetchall()
                for index, data in chunks:
                    self._check_chunk(descriptor, index, data)
                is_complete = len(chunks) == len(descriptor["chunks"])
                require(bool(complete) == is_complete, "Corrupt completeness marker")
                if is_complete:
                    self._read_payload(descriptor)
            for notice in notices.values():
                if notice["producer"] == self.node_id:
                    require(self.resolve(self.ref(notice)) is not None, "Missing locally published payload")
            summary = self.summary()
            require(summary["reserved_bytes"] <= limits.max_reserved_bytes, "Retained byte capacity exceeded")
            return summary
