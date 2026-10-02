"""Bounded durable fact storage for the v1 peer transport.

This module stores immutable data only. It grants no execution, Git, or release
permission. A runtime must separately enforce exclusive ownership of a node.
"""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
import re
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

PROTOCOL = "gossip-peer-v1"
MAX_EVENTS = 256
MAX_ARTIFACT_BYTES = 16_000
# Content root is depth 0; reserve 16 wire levels for transport envelopes.
MAX_ARTIFACT_DEPTH = 48
MAX_BATCH = 8
MAX_WIRE_BYTES = MAX_BATCH * (MAX_ARTIFACT_BYTES + 1024) + 8192
_MAX_SEQUENCE = (1 << 63) - 1
_SAFE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}\Z")
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_EVENT_FIELDS = {"protocol", "producer", "sequence", "kind", "artifact_sha256", "event_id"}


class StoreError(ValueError):
    """An invalid message, conflicting immutable fact, or corrupt store."""


def _json_value(value: Any, depth: int = 0, *, max_depth: int = 64) -> None:
    if depth > max_depth:
        raise StoreError(f"JSON nesting exceeds {max_depth} levels")
    if type(value) in (type(None), bool, int, str):
        return
    if type(value) is float:
        if not math.isfinite(value):
            raise StoreError("nonfinite JSON number")
        return
    if type(value) is list:
        for item in value:
            _json_value(item, depth + 1, max_depth=max_depth)
        return
    if type(value) is dict:
        for key, item in value.items():
            if type(key) is not str:
                raise StoreError("JSON object keys must be strings")
            _json_value(item, depth + 1, max_depth=max_depth)
        return
    raise StoreError("unsupported JSON value")


def canonical_bytes(value: Any, *, max_bytes: int = MAX_WIRE_BYTES) -> bytes:
    """Encode strict JSON; no coercion of keys, tuples, NaN, or infinity."""
    _json_value(value)
    try:
        encoded = json.dumps(value, ensure_ascii=False, allow_nan=False,
                             sort_keys=True, separators=(",", ":")).encode("utf-8")
    except (ValueError, UnicodeError, OverflowError, RecursionError) as exc:
        raise StoreError("invalid JSON encoding") from exc
    if len(encoded) > max_bytes:
        raise StoreError("JSON byte limit exceeded")
    return encoded


def strict_loads(data: bytes | str, *, max_bytes: int = MAX_WIRE_BYTES) -> Any:
    """Decode bounded JSON while detecting duplicate members at every depth."""
    if type(data) not in (bytes, str):
        raise StoreError("JSON input must be bytes or text")
    try:
        raw = data.encode("utf-8") if isinstance(data, str) else data
        if len(raw) > max_bytes:
            raise StoreError("JSON byte limit exceeded")
        text = raw.decode("utf-8")

        def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
            result: dict[str, Any] = {}
            for key, value in items:
                if key in result:
                    raise StoreError("duplicate JSON member")
                result[key] = value
            return result

        def nonfinite(value: str) -> Any:
            raise StoreError("nonfinite JSON number")

        result = json.loads(text, object_pairs_hook=pairs, parse_constant=nonfinite)
        canonical_bytes(result, max_bytes=max_bytes)
        return result
    except (UnicodeError, ValueError, RecursionError) as exc:
        if isinstance(exc, StoreError):
            raise
        raise StoreError("invalid JSON") from exc


def _digest(value: Any, *, max_bytes: int = MAX_WIRE_BYTES) -> str:
    return hashlib.sha256(canonical_bytes(value, max_bytes=max_bytes)).hexdigest()


def _safe(value: Any, label: str) -> str:
    if type(value) is not str or _SAFE.fullmatch(value) is None:
        raise StoreError(f"invalid {label}")
    return value


def _sha(value: Any) -> str:
    if type(value) is not str or _SHA.fullmatch(value) is None:
        raise StoreError("invalid SHA256")
    return value


def _event(value: Any) -> dict[str, Any]:
    if type(value) is not dict or set(value) != _EVENT_FIELDS:
        raise StoreError("invalid event fields")
    if value["protocol"] != PROTOCOL:
        raise StoreError("unsupported event protocol")
    _safe(value["producer"], "producer")
    _safe(value["kind"], "kind")
    sequence = value["sequence"]
    if type(sequence) is not int or not 1 <= sequence <= _MAX_SEQUENCE:
        raise StoreError("invalid origin sequence")
    _sha(value["artifact_sha256"])
    _sha(value["event_id"])
    body = {key: item for key, item in value.items() if key != "event_id"}
    if _digest(body, max_bytes=1024) != value["event_id"]:
        raise StoreError("event hash mismatch")
    return value


def _artifact(value: Any, sha: str) -> dict[str, Any]:
    _sha(sha)
    if (type(value) is not dict or set(value) != {"protocol", "content"}
            or value["protocol"] != PROTOCOL or type(value["content"]) is not dict):
        raise StoreError("invalid artifact envelope")
    _json_value(value["content"], max_depth=MAX_ARTIFACT_DEPTH)
    if _digest(value, max_bytes=MAX_ARTIFACT_BYTES) != sha:
        raise StoreError("artifact hash mismatch")
    return value


def _request_sha(kind: str, artifact_sha: str) -> str:
    return _digest({"protocol": PROTOCOL, "kind": kind, "artifact_sha256": artifact_sha})


class Store:
    """A small WAL/FULL SQLite store with one connection per operation.

    Artifacts on the wire are SHA256 -> {protocol, content} envelopes. The
    artifact() convenience method returns their original content object.
    """

    def __init__(self, path: Path, node_id: str,
                 crash_hook: Callable[[str, str], None] | None = None) -> None:
        self.path = Path(path)
        self.node_id = _safe(node_id, "node ID")
        self.crash_hook = crash_hook
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connection() as db:
            if db.execute("PRAGMA journal_mode=WAL").fetchone()[0] != "wal":
                raise StoreError("WAL journal mode unavailable")
            db.execute("BEGIN IMMEDIATE")
            tables = {row[0] for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
            is_new = not tables
            if tables and tables != {"metadata", "artifacts", "events", "commands"}:
                raise StoreError("incomplete or foreign store schema")
            db.execute("CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            db.execute("CREATE TABLE IF NOT EXISTS artifacts (sha TEXT PRIMARY KEY, payload BLOB NOT NULL)")
            db.execute("""CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY, producer TEXT NOT NULL, sequence INTEGER NOT NULL,
                artifact_sha TEXT NOT NULL REFERENCES artifacts(sha), payload BLOB NOT NULL,
                UNIQUE(producer, sequence))""")
            db.execute("""CREATE TABLE IF NOT EXISTS commands (
                command_id TEXT PRIMARY KEY, request_sha TEXT NOT NULL,
                event_id TEXT NOT NULL UNIQUE REFERENCES events(event_id))""")
            if is_new:
                db.executemany("INSERT INTO metadata VALUES (?, ?)", [
                    ("protocol", PROTOCOL), ("node_id", self.node_id), ("next_sequence", "1")])
            self._snapshot(db)
            db.commit()

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        db: sqlite3.Connection | None = None
        try:
            db = sqlite3.connect(self.path, timeout=10, isolation_level=None)
            db.execute("PRAGMA synchronous=FULL")
            db.execute("PRAGMA foreign_keys=ON")
            yield db
        except sqlite3.Error as exc:
            raise StoreError("SQLite store failure") from exc
        finally:
            if db is not None:
                db.close()

    def _snapshot(self, db: sqlite3.Connection) -> tuple[dict[str, dict[str, Any]],
                                                       dict[str, dict[str, Any]], int]:
        # Check counts and cell sizes in SQLite before materializing corrupt rows.
        bounds = {
            "metadata": (3, {"key": ("text", 32), "value": ("text", 64)}),
            "artifacts": (MAX_EVENTS, {"sha": ("text", 64), "payload": ("blob", MAX_ARTIFACT_BYTES)}),
            "events": (MAX_EVENTS, {"event_id": ("text", 64), "producer": ("text", 64),
                                     "sequence": ("integer", 20), "artifact_sha": ("text", 64),
                                     "payload": ("blob", 1024)}),
            "commands": (MAX_EVENTS, {"command_id": ("text", 64), "request_sha": ("text", 64),
                                       "event_id": ("text", 64)}),
        }
        for table, (maximum, columns) in bounds.items():
            if db.execute(f"SELECT count(*) FROM {table}").fetchone()[0] > maximum:
                raise StoreError("stored row capacity exceeded")
            conditions = " OR ".join(
                f"typeof({column}) != '{kind}' OR length(CAST({column} AS BLOB)) > {limit}"
                for column, (kind, limit) in columns.items())
            if db.execute(f"SELECT 1 FROM {table} WHERE {conditions} LIMIT 1").fetchone():
                raise StoreError("stored field size or type invalid")
        metadata = dict(db.execute("SELECT key, value FROM metadata"))
        if (set(metadata) != {"protocol", "node_id", "next_sequence"}
                or metadata["protocol"] != PROTOCOL or metadata["node_id"] != self.node_id):
            raise StoreError("store identity mismatch")
        artifacts: dict[str, dict[str, Any]] = {}
        events: dict[str, dict[str, Any]] = {}
        for sha, raw in db.execute("SELECT sha, payload FROM artifacts"):
            value = strict_loads(raw, max_bytes=MAX_ARTIFACT_BYTES)
            _artifact(value, sha)
            if canonical_bytes(value, max_bytes=MAX_ARTIFACT_BYTES) != raw:
                raise StoreError("noncanonical stored artifact")
            artifacts[sha] = value
        origins: set[tuple[str, int]] = set()
        for event_id, producer, sequence, artifact_sha, raw in db.execute(
                "SELECT event_id, producer, sequence, artifact_sha, payload FROM events"):
            value = _event(strict_loads(raw, max_bytes=1024))
            if (value["event_id"] != event_id or value["producer"] != producer
                    or value["sequence"] != sequence or value["artifact_sha256"] != artifact_sha
                    or artifact_sha not in artifacts or canonical_bytes(value) != raw):
                raise StoreError("corrupt stored event")
            origin = (producer, sequence)
            if origin in origins:
                raise StoreError("origin sequence equivocation")
            origins.add(origin)
            events[event_id] = value
        if (len(events) > MAX_EVENTS or len(artifacts) > MAX_EVENTS
                or set(artifacts) != {event["artifact_sha256"] for event in events.values()}):
            raise StoreError("invalid store capacity or orphan artifact")
        expected_sequence = max((event["sequence"] for event in events.values()
                                 if event["producer"] == self.node_id), default=0) + 1
        if metadata["next_sequence"] != str(expected_sequence):
            raise StoreError("corrupt next sequence")
        commands = list(db.execute("SELECT command_id, request_sha, event_id FROM commands"))
        if len(commands) > MAX_EVENTS:
            raise StoreError("command capacity exceeded")
        for command_id, request_sha, event_id in commands:
            _safe(command_id, "command ID")
            event = events.get(event_id)
            if (event is None or event["producer"] != self.node_id
                    or request_sha != _request_sha(event["kind"], event["artifact_sha256"])):
                raise StoreError("corrupt durable command binding")
        return events, artifacts, expected_sequence

    def _commit(self, db: sqlite3.Connection, operation: str) -> None:
        if self.crash_hook is not None:
            self.crash_hook("before_commit", operation)
        db.commit()
        if self.crash_hook is not None:
            self.crash_hook("after_commit", operation)

    def publish(self, command_id: str, kind: str, content: dict[str, Any]) -> dict[str, Any]:
        _safe(command_id, "command ID")
        _safe(kind, "kind")
        if type(content) is not dict:
            raise StoreError("artifact content must be an object")
        _json_value(content, max_depth=MAX_ARTIFACT_DEPTH)
        envelope = {"protocol": PROTOCOL, "content": content}
        artifact_bytes = canonical_bytes(envelope, max_bytes=MAX_ARTIFACT_BYTES)
        artifact_sha = hashlib.sha256(artifact_bytes).hexdigest()
        request_sha = _request_sha(kind, artifact_sha)
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            events, _, sequence = self._snapshot(db)
            old = db.execute("SELECT request_sha, event_id FROM commands WHERE command_id=?",
                             (command_id,)).fetchone()
            if old is not None:
                if old[0] != request_sha:
                    raise StoreError("conflicting command ID reuse")
                db.commit()
                return events[old[1]]
            if len(events) >= MAX_EVENTS or sequence > _MAX_SEQUENCE:
                raise StoreError("event capacity or sequence exhausted")
            event = {"protocol": PROTOCOL, "producer": self.node_id, "sequence": sequence,
                     "kind": kind, "artifact_sha256": artifact_sha}
            event["event_id"] = _digest(event, max_bytes=1024)
            db.execute("INSERT OR IGNORE INTO artifacts VALUES (?, ?)", (artifact_sha, artifact_bytes))
            db.execute("INSERT INTO events VALUES (?, ?, ?, ?, ?)",
                       (event["event_id"], self.node_id, sequence, artifact_sha, canonical_bytes(event)))
            db.execute("INSERT INTO commands VALUES (?, ?, ?)", (command_id, request_sha, event["event_id"]))
            db.execute("UPDATE metadata SET value=? WHERE key='next_sequence'", (str(sequence + 1),))
            self._commit(db, "publish")
            return event

    def merge(self, batch: dict[str, Any]) -> dict[str, int]:
        # Detach caller-owned containers and enforce aggregate byte/depth bounds.
        value = strict_loads(canonical_bytes(batch))
        if type(value) is not dict or set(value) != {"events", "artifacts"}:
            raise StoreError("invalid batch fields")
        incoming, artifacts = value["events"], value["artifacts"]
        if (type(incoming) is not list or type(artifacts) is not dict
                or len(incoming) > MAX_BATCH or len(artifacts) > MAX_BATCH):
            raise StoreError("batch limit or shape invalid")
        for event in incoming:
            _event(event)
        for sha, artifact in artifacts.items():
            _artifact(artifact, sha)
        referenced = {event["artifact_sha256"] for event in incoming}
        if set(artifacts) - referenced:
            raise StoreError("unreferenced batch artifact")
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            known, stored_artifacts, next_sequence = self._snapshot(db)
            origins = {(event["producer"], event["sequence"]): event["event_id"]
                       for event in known.values()}
            new: dict[str, dict[str, Any]] = {}
            for event in incoming:
                event_id, sha = event["event_id"], event["artifact_sha256"]
                if sha not in artifacts and sha not in stored_artifacts:
                    raise StoreError("missing artifact")
                if (sha in artifacts and sha in stored_artifacts
                        and artifacts[sha] != stored_artifacts[sha]):
                    raise StoreError("conflicting artifact")
                origin = (event["producer"], event["sequence"])
                if origin in origins and origins[origin] != event_id:
                    raise StoreError("origin sequence equivocation")
                origins[origin] = event_id
                if event_id not in known:
                    new[event_id] = event
            if len(known) + len(new) > MAX_EVENTS:
                raise StoreError("event capacity exceeded")
            for event in new.values():
                sha = event["artifact_sha256"]
                if sha not in stored_artifacts:
                    db.execute("INSERT OR IGNORE INTO artifacts VALUES (?, ?)",
                               (sha, canonical_bytes(artifacts[sha], max_bytes=MAX_ARTIFACT_BYTES)))
                db.execute("INSERT INTO events VALUES (?, ?, ?, ?, ?)",
                           (event["event_id"], event["producer"], event["sequence"], sha,
                            canonical_bytes(event)))
                if event["producer"] == self.node_id:
                    next_sequence = max(next_sequence, event["sequence"] + 1)
            if new:
                db.execute("UPDATE metadata SET value=? WHERE key='next_sequence'", (str(next_sequence),))
                self._commit(db, "merge")
            else:
                db.commit()
            return {"added": len(new), "duplicates": len(incoming) - len(new)}

    def export(self, known: list[str], limit: int = MAX_BATCH) -> dict[str, Any]:
        if type(known) is not list or len(known) > MAX_EVENTS:
            raise StoreError("invalid known inventory")
        for event_id in known:
            _sha(event_id)
        if type(limit) is not int or not 1 <= limit <= MAX_BATCH:
            raise StoreError("invalid export limit")
        with self._connection() as db:
            db.execute("BEGIN")
            events, artifacts, _ = self._snapshot(db)
            excluded = set(known)
            selected = [events[event_id] for event_id in sorted(events) if event_id not in excluded][:limit]
            return {"events": selected, "artifacts": {
                sha: artifacts[sha] for sha in sorted({event["artifact_sha256"] for event in selected})}}

    def state(self) -> dict[str, Any]:
        with self._connection() as db:
            db.execute("BEGIN")
            events, artifacts, sequence = self._snapshot(db)
            return {"node_id": self.node_id, "count": len(events),
                    "events": [events[event_id] for event_id in sorted(events)],
                    "artifact_ids": sorted(artifacts), "next_sequence": sequence}

    def inventory(self) -> list[str]:
        return [event["event_id"] for event in self.state()["events"]]

    def artifact(self, sha: str) -> dict[str, Any]:
        _sha(sha)
        with self._connection() as db:
            db.execute("BEGIN")
            _, artifacts, _ = self._snapshot(db)
            if sha not in artifacts:
                raise StoreError("unknown artifact")
            return artifacts[sha]["content"]
