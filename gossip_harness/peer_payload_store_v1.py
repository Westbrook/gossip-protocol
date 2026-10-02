"""Bounded immutable byte payloads for the v1 peer coding transport.

Only a caller-configured root and a fixed SQLite filename touch the filesystem;
wire descriptors contain no paths. Hashes detect damage and conflicting data,
not malicious changes by another process with access to this host's database.
Payload possession grants no Git, execution, review, or publication authority.
"""
from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
import hashlib
from pathlib import Path
import re
import sqlite3
from typing import Any

from .peer_store_v1 import StoreError, canonical_bytes, strict_loads

PROTOCOL = "peer-payload-v1"
CHUNK_SIZE = 65_536
MAX_OBJECT_BYTES = 8 * 1024 * 1024
MAX_CHUNKS = MAX_OBJECT_BYTES // CHUNK_SIZE
MAX_OBJECTS = 128
MAX_RESERVED_BYTES = 64 * 1024 * 1024
MAX_DESCRIPTOR_BYTES = 16_000
MEDIA_TYPES = frozenset({"application/json", "application/x-git-bundle", "application/octet-stream"})
_EMPTY_SHA = hashlib.sha256(b"").hexdigest()
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_SAFE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}\Z")
_FIELDS = {"protocol", "sha256", "size", "chunk_size", "chunk_sha256", "media_type"}


class PayloadError(StoreError):
    """Invalid payload data, exhausted capacity, or corrupt durable storage."""


def _sha(value: Any) -> str:
    if type(value) is not str or _SHA.fullmatch(value) is None:
        raise PayloadError("invalid payload SHA256")
    return value


def _media_type(value: Any) -> str:
    if type(value) is not str or value not in MEDIA_TYPES:
        raise PayloadError("unsupported payload media type")
    return value


def _descriptor(value: Any) -> dict[str, Any]:
    # Check the shallow, fixed schema before traversing or encoding wire values.
    if type(value) is not dict or len(value) != len(_FIELDS) or set(value) != _FIELDS:
        raise PayloadError("invalid payload descriptor fields")
    if type(value["protocol"]) is not str or value["protocol"] != PROTOCOL:
        raise PayloadError("unsupported payload protocol")
    _sha(value["sha256"])
    size = value["size"]
    if type(size) is not int or not 0 <= size <= MAX_OBJECT_BYTES:
        raise PayloadError("invalid payload size")
    if type(value["chunk_size"]) is not int or value["chunk_size"] != CHUNK_SIZE:
        raise PayloadError("invalid payload chunk size")
    hashes = value["chunk_sha256"]
    if (type(hashes) is not list or len(hashes) > MAX_CHUNKS
            or len(hashes) != (size + CHUNK_SIZE - 1) // CHUNK_SIZE):
        raise PayloadError("invalid payload chunk count")
    for sha in hashes:
        _sha(sha)
    if size == 0 and value["sha256"] != _EMPTY_SHA:
        raise PayloadError("empty payload hash mismatch")
    _media_type(value["media_type"])
    # Detached containers prevent mutation of either an accepted or returned fact.
    result = {**value, "chunk_sha256": list(hashes)}
    _encode(result)
    return result


def _encode(descriptor: dict[str, Any]) -> bytes:
    try:
        return canonical_bytes(descriptor, max_bytes=MAX_DESCRIPTOR_BYTES)
    except StoreError as error:
        raise PayloadError("invalid payload descriptor JSON") from error


def _index(index: int) -> None:
    if type(index) is not int or not 0 <= index < MAX_CHUNKS:
        raise PayloadError("invalid payload chunk index")


def _chunk(descriptor: dict[str, Any], index: int, data: bytes) -> None:
    _index(index)
    if index >= len(descriptor["chunk_sha256"]):
        raise PayloadError("payload chunk index outside descriptor")
    expected_size = min(CHUNK_SIZE, descriptor["size"] - index * CHUNK_SIZE)
    if type(data) is not bytes or len(data) != expected_size:
        raise PayloadError("payload chunk size mismatch")
    if hashlib.sha256(data).hexdigest() != descriptor["chunk_sha256"][index]:
        raise PayloadError("payload chunk hash mismatch")


def _complete(descriptor: dict[str, Any], chunks: dict[int, bytes]) -> bool:
    if len(chunks) != len(descriptor["chunk_sha256"]):
        return False
    digest = hashlib.sha256()
    for index in range(len(chunks)):
        digest.update(chunks[index])
    if digest.hexdigest() != descriptor["sha256"]:
        raise PayloadError("complete payload hash mismatch")
    return True


class PayloadStore:
    """One node's WAL/FULL payload store, with a connection per operation.

    Registration atomically reserves declared bytes, including missing chunks.
    Duplicates do not consume capacity. A failed final checksum rolls back the
    incoming chunk while retaining previously committed, relayable chunks.
    The resolved root and node ID are durable bindings, not authentication.
    """

    def __init__(self, root: Path, node_id: str) -> None:
        if type(node_id) is not str or _SAFE.fullmatch(node_id) is None:
            raise PayloadError("invalid payload node ID")
        self.node_id = node_id
        try:
            self.root = Path(root).resolve()
            if len(str(self.root).encode("utf-8")) > 4096:
                raise PayloadError("payload root identity is too long")
            self.root.mkdir(parents=True, exist_ok=True)
        except (OSError, UnicodeError, RuntimeError) as error:
            raise PayloadError("invalid payload root") from error
        self.path = self.root / "payloads.sqlite"
        existed = self.path.exists()
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            tables = {row[0] for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
            if tables and tables != {"metadata", "objects", "chunks"}:
                raise PayloadError("incomplete or foreign payload schema")
            if existed and not tables:
                raise PayloadError("missing payload schema in existing database")
            if not tables:
                db.execute("CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
                db.execute("""CREATE TABLE objects (
                    sha TEXT PRIMARY KEY, size INTEGER NOT NULL, descriptor BLOB NOT NULL)""")
                db.execute("""CREATE TABLE chunks (
                    sha TEXT NOT NULL REFERENCES objects(sha), chunk_index INTEGER NOT NULL,
                    data BLOB NOT NULL, PRIMARY KEY (sha, chunk_index))""")
                db.executemany("INSERT INTO metadata VALUES (?, ?)", [
                    ("protocol", PROTOCOL), ("node_id", self.node_id), ("root", str(self.root))])
            catalog = self._catalog(db)
            for descriptor in catalog.values():
                self._chunks(db, descriptor)
            db.commit()

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        db: sqlite3.Connection | None = None
        try:
            # These are local configuration checks, not a hostile-host sandbox.
            if self.root.resolve() != self.root or not self.root.is_dir():
                raise PayloadError("payload root identity mismatch")
            for suffix in ("", "-wal", "-shm", "-journal"):
                target = self.root / (self.path.name + suffix)
                if target.is_symlink() or target.exists() and not target.is_file():
                    raise PayloadError("invalid payload database path")
            db = sqlite3.connect(self.path, timeout=10, isolation_level=None)
            if db.execute("PRAGMA journal_mode=WAL").fetchone()[0] != "wal":
                raise PayloadError("payload WAL journal mode unavailable")
            db.execute("PRAGMA synchronous=FULL")
            db.execute("PRAGMA foreign_keys=ON")
            yield db
        except sqlite3.Error as error:
            raise PayloadError("SQLite payload store failure") from error
        except OSError as error:
            raise PayloadError("payload filesystem failure") from error
        finally:
            if db is not None:
                db.close()

    def _catalog(self, db: sqlite3.Connection) -> dict[str, dict[str, Any]]:
        # SQL bounds precede materializing descriptors or chunk blobs. Reservation
        # is the sum of immutable object sizes, with no mutable counter to drift.
        bounds = {
            "metadata": (3, {"key": ("text", 32), "value": ("text", 4096)}),
            "objects": (MAX_OBJECTS, {"sha": ("text", 64), "size": ("integer", 20),
                                      "descriptor": ("blob", MAX_DESCRIPTOR_BYTES)}),
            "chunks": (MAX_OBJECTS * MAX_CHUNKS,
                       {"sha": ("text", 64), "chunk_index": ("integer", 20),
                        "data": ("blob", CHUNK_SIZE)}),
        }
        for table, (maximum, columns) in bounds.items():
            if db.execute(f"SELECT count(*) FROM {table}").fetchone()[0] > maximum:
                raise PayloadError("stored payload row capacity exceeded")
            conditions = " OR ".join(
                f"typeof({column}) != '{kind}' OR length(CAST({column} AS BLOB)) > {limit}"
                for column, (kind, limit) in columns.items())
            if db.execute(f"SELECT 1 FROM {table} WHERE {conditions} LIMIT 1").fetchone():
                raise PayloadError("stored payload field size or type invalid")
        for table, keys in (("metadata", "key"), ("objects", "sha"), ("chunks", "sha, chunk_index")):
            if db.execute(f"SELECT 1 FROM {table} GROUP BY {keys} HAVING count(*) != 1 LIMIT 1").fetchone():
                raise PayloadError("duplicate immutable payload row")
        metadata = dict(db.execute("SELECT key, value FROM metadata"))
        if metadata != {"protocol": PROTOCOL, "node_id": self.node_id, "root": str(self.root)}:
            raise PayloadError("payload store identity mismatch")
        if db.execute("SELECT 1 FROM objects WHERE size < 0 OR size > ? LIMIT 1",
                      (MAX_OBJECT_BYTES,)).fetchone():
            raise PayloadError("stored payload size invalid")
        if db.execute("SELECT coalesce(sum(size), 0) FROM objects").fetchone()[0] > MAX_RESERVED_BYTES:
            raise PayloadError("stored payload reserved capacity exceeded")
        if db.execute("""SELECT 1 FROM chunks LEFT JOIN objects ON chunks.sha=objects.sha
            WHERE objects.sha IS NULL OR chunk_index < 0 OR chunk_index >= ?
            OR chunk_index * ? >= objects.size
            OR length(data) != min(?, objects.size - chunk_index * ?) LIMIT 1""",
                      (MAX_CHUNKS, CHUNK_SIZE, CHUNK_SIZE, CHUNK_SIZE)).fetchone():
            raise PayloadError("stored payload chunk shape invalid")
        catalog: dict[str, dict[str, Any]] = {}
        for sha, size, raw in db.execute("SELECT sha, size, descriptor FROM objects"):
            try:
                descriptor = _descriptor(strict_loads(raw, max_bytes=MAX_DESCRIPTOR_BYTES))
            except StoreError as error:
                raise PayloadError("invalid stored payload descriptor") from error
            if descriptor["sha256"] != sha or descriptor["size"] != size or _encode(descriptor) != raw:
                raise PayloadError("corrupt or noncanonical stored payload descriptor")
            catalog[sha] = descriptor
        return catalog

    def _chunks(self, db: sqlite3.Connection, descriptor: dict[str, Any]) -> dict[int, bytes]:
        chunks: dict[int, bytes] = {}
        for index, data in db.execute("SELECT chunk_index, data FROM chunks WHERE sha=? ORDER BY chunk_index",
                                      (descriptor["sha256"],)):
            _chunk(descriptor, index, data)
            chunks[index] = data
        _complete(descriptor, chunks)
        return chunks

    def _register(self, db: sqlite3.Connection, descriptor: dict[str, Any],
                  catalog: dict[str, dict[str, Any]]) -> None:
        sha = descriptor["sha256"]
        if sha in catalog:
            if _encode(catalog[sha]) != _encode(descriptor):
                raise PayloadError("conflicting immutable payload descriptor")
            return
        if len(catalog) >= MAX_OBJECTS:
            raise PayloadError("payload object capacity exceeded")
        if sum(item["size"] for item in catalog.values()) + descriptor["size"] > MAX_RESERVED_BYTES:
            raise PayloadError("payload reserved byte capacity exceeded")
        db.execute("INSERT INTO objects VALUES (?, ?, ?)", (sha, descriptor["size"], _encode(descriptor)))

    @staticmethod
    def _lookup(catalog: dict[str, dict[str, Any]], sha: str) -> dict[str, Any]:
        if sha not in catalog:
            raise PayloadError("unknown payload")
        return catalog[sha]

    def put(self, data: bytes, media_type: str) -> dict[str, Any]:
        """Atomically reserve and persist a locally available complete object."""
        if type(data) is not bytes or len(data) > MAX_OBJECT_BYTES:
            raise PayloadError("invalid payload bytes or object size")
        _media_type(media_type)
        chunks = [data[offset:offset + CHUNK_SIZE] for offset in range(0, len(data), CHUNK_SIZE)]
        descriptor = _descriptor({"protocol": PROTOCOL, "sha256": hashlib.sha256(data).hexdigest(),
                                  "size": len(data), "chunk_size": CHUNK_SIZE,
                                  "chunk_sha256": [hashlib.sha256(chunk).hexdigest() for chunk in chunks],
                                  "media_type": media_type})
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            catalog = self._catalog(db)
            self._register(db, descriptor, catalog)
            stored = self._chunks(db, descriptor)
            for index, chunk in enumerate(chunks):
                if index in stored:
                    if stored[index] != chunk:
                        raise PayloadError("conflicting immutable payload chunk")
                else:
                    db.execute("INSERT INTO chunks VALUES (?, ?, ?)", (descriptor["sha256"], index, chunk))
            db.commit()
        return descriptor

    def register(self, descriptor: dict[str, Any]) -> None:
        """Reserve one immutable descriptor; registration does not fetch bytes."""
        value = _descriptor(descriptor)
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            self._register(db, value, self._catalog(db))
            self._chunks(db, value)
            db.commit()

    def descriptor(self, sha: str) -> dict[str, Any]:
        _sha(sha)
        with self._connection() as db:
            db.execute("BEGIN")
            descriptor = self._lookup(self._catalog(db), sha)
            self._chunks(db, descriptor)
            return descriptor

    def has_complete(self, sha: str) -> bool:
        _sha(sha)
        with self._connection() as db:
            db.execute("BEGIN")
            catalog = self._catalog(db)
            if sha not in catalog:
                return False
            descriptor = catalog[sha]
            return _complete(descriptor, self._chunks(db, descriptor))

    def missing(self, sha: str) -> list[int]:
        _sha(sha)
        with self._connection() as db:
            db.execute("BEGIN")
            descriptor = self._lookup(self._catalog(db), sha)
            chunks = self._chunks(db, descriptor)
            return [index for index in range(len(descriptor["chunk_sha256"])) if index not in chunks]

    def read_chunk(self, sha: str, index: int) -> bytes:
        _sha(sha)
        _index(index)
        with self._connection() as db:
            db.execute("BEGIN")
            descriptor = self._lookup(self._catalog(db), sha)
            if index >= len(descriptor["chunk_sha256"]):
                raise PayloadError("payload chunk index outside descriptor")
            chunks = self._chunks(db, descriptor)
            if index not in chunks:
                raise PayloadError("payload chunk unavailable")
            return chunks[index]

    def accept_chunk(self, sha: str, index: int, data: bytes) -> None:
        _sha(sha)
        _index(index)
        if type(data) is not bytes or len(data) > CHUNK_SIZE:
            raise PayloadError("invalid payload chunk bytes")
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            descriptor = self._lookup(self._catalog(db), sha)
            _chunk(descriptor, index, data)
            chunks = self._chunks(db, descriptor)
            if index in chunks:
                if chunks[index] != data:
                    raise PayloadError("conflicting immutable payload chunk")
            else:
                db.execute("INSERT INTO chunks VALUES (?, ?, ?)", (sha, index, data))
                chunks[index] = data
                _complete(descriptor, chunks)
            db.commit()

    def read(self, sha: str) -> bytes:
        _sha(sha)
        with self._connection() as db:
            db.execute("BEGIN")
            descriptor = self._lookup(self._catalog(db), sha)
            chunks = self._chunks(db, descriptor)
            if not _complete(descriptor, chunks):
                raise PayloadError("payload incomplete")
            return b"".join(chunks[index] for index in range(len(chunks)))
