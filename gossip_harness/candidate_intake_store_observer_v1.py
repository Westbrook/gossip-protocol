"""B02 bounded evaluator-owned raw storage mapping, never candidate-side scoring.

A controller supplies frozen bytes captured outside candidate processes. Only a
source-reviewed registered mapping is eligible; an unknown representation is an
observation limitation, not a product failure. Registration is not authentication
or proof of source review: the acceptance controller must supply those authorities.
No candidate module is imported or candidate audit JSON trusted by this module.
"""
from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
from dataclasses import dataclass
from typing import Any


PROTOCOL = "candidate-intake-store-observer-v1"
FORKED_FROM_SHA256 = "84952ce86ca8bd6df4434ca76d44e25febde2c4f88af2664f8765681fa131a15"
SQLITE_LAYOUT = "reviewed-m1-sqlite-v1"
JSON_LAYOUT = "authored-split-json-control-v1"
V2_SQLITE_LAYOUT = "reviewed-v2-sqlite-locks-v1"
V2_STORAGE_PATHS = ("catalog.sqlite", "catalog.sqlite.maintenance/maintenance.lock",
                    "catalog.sqlite.maintenance/worker.lock")
# B02's aggregate 524288 UTF-8 bytes can expand sixfold under JSON control-
# character escaping. SQLite LENGTH limits whole rows too: the manifest and
# completed receipt together require more than 6 MiB. Eight MiB bounds one
# row/field; 32 MiB bounds the complete physical capture and serialized census.
# This is a separate execution contract; frozen B01 retains its original limits.
MAX_CAPTURE_BYTES = 32 * 1024 * 1024
MAX_FILES = 8
MAX_ROWS = 1024
MAX_FIELD_BYTES = 8 * 1024 * 1024
PHASES = ("before", "after", "reopened")
SQLITE_COLUMNS = {
    "blobs": ("blob_id", "content"),
    "documents": ("document_id", "source_id", "source", "blob_id", "title"),
    "jobs": ("job_id", "epoch", "state", "total", "completed", "error", "manifest", "content_hashes", "receipt"),
}


class ObservationUnavailable(ValueError):
    """Unsupported or unqualified capture; must never become a passing judgment."""


@dataclass(frozen=True)
class Registration:
    source_sha256: str
    layout: str
    storage_paths: tuple[str, ...]
    review_sha256: str
    # Authority comes from an authenticated external source review. It confirms
    # these files/tables enumerate ALL content, job, manifest and receipt state.
    # Schema hash binds a reviewed SQLite schema or the split-JSON key schema.
    schema_sha256: str


@dataclass(frozen=True)
class Observation:
    data: dict[str, Any]
    persisted_strings: dict[str, dict[str, str | None]]
    files: tuple[dict[str, Any], ...]
    schema_sha256: str
    registration_sha256: str
    auxiliary_tables: dict[str, list[dict[str, Any]]]
    layout: str


def encoded(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def registration_sha256(registration: Registration) -> str:
    return _sha(encoded(vars(registration)))


def _digest(value: Any) -> bool:
    return type(value) is str and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ObservationUnavailable("duplicate persisted JSON key")
        result[key] = value
    return result


def _json(raw: str | bytes) -> Any:
    if len(raw.encode("utf-8", errors="surrogatepass") if isinstance(raw, str) else raw) > MAX_FIELD_BYTES:
        raise ObservationUnavailable("persisted field bound")
    try:
        return json.loads(raw, object_pairs_hook=_pairs,
                          parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))
    except (ValueError, TypeError, RecursionError, UnicodeError) as error:
        raise ObservationUnavailable("invalid persisted JSON") from error


def _capture(files: dict[str, bytes], registration: Registration, source_sha256: str) -> None:
    if not all(_digest(value) for value in (source_sha256, registration.source_sha256,
                                          registration.review_sha256, registration.schema_sha256)):
        raise ObservationUnavailable("missing exact source/schema/review binding")
    if source_sha256 != registration.source_sha256:
        raise ObservationUnavailable("mapper source mismatch")
    if (type(files) is not dict or not 1 <= len(files) <= MAX_FILES
            or sum(len(raw) for raw in files.values() if type(raw) is bytes) > MAX_CAPTURE_BYTES):
        raise ObservationUnavailable("capture bounds")
    for path, raw in files.items():
        if (type(path) is not str or len(path) > 1024 or path.startswith("/") or "\\" in path
                or any(part in ("", ".", "..") for part in path.split("/"))
                or type(raw) is not bytes or len(raw) > MAX_CAPTURE_BYTES):
            raise ObservationUnavailable("invalid captured path or bytes")
    if (type(registration.storage_paths) is not tuple
            or any(type(path) is not str for path in registration.storage_paths)):
        raise ObservationUnavailable("invalid registered paths")
    if tuple(sorted(files)) != tuple(sorted(registration.storage_paths)):
        raise ObservationUnavailable("unregistered storage file or missing capture")


def _rows(value: Any, name: str) -> list[dict[str, Any]]:
    if type(value) is not list or len(value) > MAX_ROWS or any(type(row) is not dict for row in value):
        raise ObservationUnavailable("invalid/bounded " + name + " rows")
    return value


def _normalize(blobs: list[dict[str, Any]], documents: list[dict[str, Any]],
               jobs: list[dict[str, Any]]) -> tuple[dict[str, Any], dict[str, dict[str, str | None]]]:
    contents: dict[str, bytes] = {}
    normalized_blobs: list[dict[str, Any]] = []
    for row in blobs:
        bid, raw = row["blob_id"], row["content"]
        if type(bid) is not str or type(raw) is not bytes or bid in contents:
            raise ObservationUnavailable("invalid persisted blob identity/content")
        contents[bid] = raw
        normalized_blobs.append({"blob_id": bid, "bytes": len(raw), "sha256": _sha(raw)})
    normalized_documents: list[dict[str, Any]] = []
    for row in documents:
        doc = {key: row[key] for key in SQLITE_COLUMNS["documents"]}
        raw = contents.get(doc["blob_id"])
        try:
            doc["text"] = None if raw is None else raw.decode("utf-8")
        except UnicodeError:
            # A structurally captured invalid content value is an observed
            # product mismatch, not silently hidden by a lossy decoder.
            assert raw is not None
            doc["text"] = {"invalid_utf8_sha256": _sha(raw)}
        normalized_documents.append(doc)
    strings: dict[str, dict[str, str | None]] = {}
    normalized_jobs: list[dict[str, Any]] = []
    for row in jobs:
        public = {key: row[key] for key in SQLITE_COLUMNS["jobs"][:6]}
        jid = public["job_id"]
        if type(jid) is not str or jid in strings:
            raise ObservationUnavailable("invalid persisted job identity")
        raw_fields = {key: row[key] for key in ("manifest", "content_hashes", "receipt")}
        if any(type(value) is not str and value is not None for value in raw_fields.values()):
            raise ObservationUnavailable("persisted manifest/receipt must be mapped to exact serialized text")
        if raw_fields["manifest"] is None or raw_fields["content_hashes"] is None:
            raise ObservationUnavailable("missing persisted manifest or hashes")
        strings[jid] = raw_fields
        normalized_jobs.append({"public": public, "manifest": _json(raw_fields["manifest"]),
                                "content_hashes": _json(raw_fields["content_hashes"]),
                                "receipt": None if raw_fields["receipt"] is None else _json(raw_fields["receipt"])})
    try:
        data = {"blobs": sorted(normalized_blobs, key=lambda row: row["blob_id"]),
                "documents": sorted(normalized_documents, key=lambda row: row["source"]),
                "jobs": sorted(normalized_jobs, key=lambda row: row["public"]["job_id"])}
        if len(encoded(data)) > MAX_CAPTURE_BYTES:
            raise ObservationUnavailable("normalized size bound")
        return data, strings
    except (TypeError, ValueError, UnicodeError) as error:
        raise ObservationUnavailable("invalid normalized storage value") from error


def sqlite_schema_sha256(raw: bytes) -> str:
    """Inspect only; a computed hash does not authorize its own registration."""
    return _sqlite(raw)[1]


def _sqlite(raw: bytes) -> tuple[tuple[list[dict[str, Any]], ...], str, dict[str, list[dict[str, Any]]]]:
    if type(raw) is not bytes or not 100 <= len(raw) <= MAX_CAPTURE_BYTES or not raw.startswith(b"SQLite format 3\0"):
        raise ObservationUnavailable("not a bounded SQLite main database")
    with tempfile.TemporaryDirectory(prefix="storage-observer-") as directory:
        path = Path(directory) / "capture.sqlite"
        path.write_bytes(raw)
        connection = sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True)
        connection.row_factory = sqlite3.Row
        try:
            connection.enable_load_extension(False)
            connection.execute("PRAGMA trusted_schema=OFF")
            connection.execute("PRAGMA query_only=ON")
            connection.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, MAX_FIELD_BYTES)
            connection.setlimit(sqlite3.SQLITE_LIMIT_SQL_LENGTH, 4096)
            connection.setlimit(sqlite3.SQLITE_LIMIT_COLUMN, 128)
            instructions = 0
            def progress() -> int:
                nonlocal instructions
                instructions += 1000
                return int(instructions > 200000)
            connection.set_progress_handler(progress, 1000)
            allowed = {sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ}
            connection.set_authorizer(lambda action, a, b, c, d: sqlite3.SQLITE_OK if action in allowed else sqlite3.SQLITE_DENY)
            schema = [dict(row) for row in connection.execute(
                "SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name").fetchmany(129)]
            if len(schema) > 128 or any(row["type"] == "view" for row in schema):
                raise ObservationUnavailable("unqualified schema object or schema bound")
            if any(row["type"] == "table" and (not isinstance(row["sql"], str) or "VIRTUAL TABLE" in row["sql"].upper()) for row in schema):
                raise ObservationUnavailable("virtual table refused")
            tables = {row["name"] for row in schema if row["type"] == "table"}
            if not set(SQLITE_COLUMNS) <= tables:
                raise ObservationUnavailable("mapper required tables absent")
            result = []
            observed_bytes = 0
            def bounded_rows(query: str) -> list[dict[str, Any]]:
                nonlocal observed_bytes
                rows: list[dict[str, Any]] = []
                for value in connection.execute(query):
                    row = dict(value)
                    serialized = {key: {"sqlite_blob_base64": base64.b64encode(item).decode("ascii")}
                                  if isinstance(item, bytes) else item for key, item in row.items()}
                    observed_bytes += len(encoded(serialized))
                    if len(rows) >= MAX_ROWS or observed_bytes > MAX_CAPTURE_BYTES:
                        raise ObservationUnavailable("persisted census bound")
                    rows.append(row)
                return rows
            for table, columns in SQLITE_COLUMNS.items():
                query = "SELECT " + ",".join(columns) + " FROM " + table + " NOT INDEXED"
                result.append(bounded_rows(query))
            auxiliary = {}
            for table in sorted(tables - set(SQLITE_COLUMNS)):
                # Identifier quoting is data escaping; no schema-provided SQL is
                # executed. Ordinary table rows include all later-v2 controls.
                quoted = '"' + table.replace('"', '""') + '"'
                values = bounded_rows("SELECT * FROM " + quoted + " NOT INDEXED")
                normalized = [{key: {"sqlite_blob_base64": base64.b64encode(item).decode("ascii")}
                               if isinstance(item, bytes) else item for key, item in row.items()} for row in values]
                auxiliary[table] = sorted(normalized, key=encoded)
            return tuple(result), _sha(encoded(schema)), auxiliary
        except (sqlite3.Error, ValueError, UnicodeError, OverflowError) as error:
            raise ObservationUnavailable("SQLite observation refused or incomplete") from error
        finally:
            connection.close()


def _split_json(files: dict[str, bytes]) -> tuple[tuple[list[dict[str, Any]], ...], str]:
    if set(files) != {"content.json", "catalog.json", "jobs.json"}:
        raise ObservationUnavailable("split-JSON paths")
    blobs = _rows(_json(files["content.json"]), "content")
    for row in blobs:
        if set(row) != {"key", "payload_base64"}:
            raise ObservationUnavailable("split-JSON content schema")
    try:
        converted = [{"blob_id": row["key"], "content": base64.b64decode(row["payload_base64"], validate=True)} for row in blobs]
        documents = _rows(_json(files["catalog.json"]), "catalog")
        jobs = _rows(_json(files["jobs.json"]), "jobs")
        if any(set(row) != set(SQLITE_COLUMNS["documents"]) for row in documents):
            raise ObservationUnavailable("split-JSON catalog schema")
        if any(set(row) != set(SQLITE_COLUMNS["jobs"]) for row in jobs):
            raise ObservationUnavailable("split-JSON jobs schema")
        return (converted, documents, jobs), _sha(encoded({"content": ["key", "payload_base64"],
                                                          "catalog": SQLITE_COLUMNS["documents"], "jobs": SQLITE_COLUMNS["jobs"]}))
    except (ValueError, TypeError, KeyError) as error:
        raise ObservationUnavailable("invalid split-JSON persisted representation") from error


def split_json_schema_sha256() -> str:
    return _split_json({"content.json": b"[]", "catalog.json": b"[]", "jobs.json": b"[]"})[1]


def observe_capture(files: dict[str, bytes], registration: Registration, source_sha256: str) -> Observation:
    _capture(files, registration, source_sha256)
    try:
        if registration.layout == SQLITE_LAYOUT:
            if registration.storage_paths != ("catalog.sqlite",):
                raise ObservationUnavailable("SQLite mapper supports quiescent main database only; sidecars unqualified")
            rows, schema_sha, auxiliary = _sqlite(files["catalog.sqlite"])
        elif registration.layout == V2_SQLITE_LAYOUT:
            if tuple(sorted(registration.storage_paths)) != V2_STORAGE_PATHS:
                raise ObservationUnavailable("v2 registered storage paths mismatch")
            if any(files[path] != b"" for path in V2_STORAGE_PATHS[1:]):
                raise ObservationUnavailable("v2 synchronization files must match reviewed empty representation")
            rows, schema_sha, auxiliary = _sqlite(files["catalog.sqlite"])
        elif registration.layout == JSON_LAYOUT:
            rows, schema_sha = _split_json(files)
            auxiliary = {}
        else:
            raise ObservationUnavailable("unknown storage mapper")
        # Ordinary write triggers are retained in the exact schema binding. SELECT
        # with query_only/read-only/authorizer cannot execute those write hooks.
        if schema_sha != registration.schema_sha256:
            raise ObservationUnavailable("unreviewed storage schema")
        data, strings = _normalize(*rows)
        inventory = tuple({"path": path, "bytes": len(raw), "sha256": _sha(raw)} for path, raw in sorted(files.items()))
        return Observation(data, strings, inventory, schema_sha, registration_sha256(registration), auxiliary, registration.layout)
    except (KeyError, TypeError, RecursionError) as error:
        raise ObservationUnavailable("unmappable persisted records") from error

