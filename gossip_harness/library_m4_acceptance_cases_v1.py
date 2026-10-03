"""Independent contract-derived M4 development histories and observation adapter.

No M4 product implementation or implementation tests were inspected to author
these definitions. Frozen public migration inventories supply initial expected
facts. Expectations stay on the host; the child runs public APIs in a caller-
owned sandbox. These finite histories are neither held-out statistical evidence
nor complete project acceptance.
"""
from __future__ import annotations

import base64
from copy import deepcopy
import hashlib
import json
from typing import Any
import zlib

PROTOCOL = "library-m4-independent-cases-v1"
PURPOSE = "independent_acceptance"
CONTRACT_SHA256 = "3353dbaa0c2474527cbffb0d87a4091b5f77ed3f98418fff2f216d90b6a8487c"
REQUIREMENT_IDS = ("M4-API-SCHEMA", "M4-MIGRATION", "M4-COMPATIBILITY")
LIMITATIONS = (
    "Development qualification only; source, runtime, limits, provenance and the complete cohort barrier require caller authentication.",
    "No M4-RELEASE-HANDOFF install/browser journey: that requires independent qualification against the exact release bytes.",
    "Service.request checks public routing; actual HTTP wire behavior and browser behavior require separate lanes.",
    "No forced migration crash, live legacy process, overlapping maintenance lock or power-loss schedule.",
    "Finite corrupt snapshots do not exhaust every SQLite corruption or graph invariant.",
    "No unspecified signed-counter ceiling, absent-owner idle/stopped distinction, or multi-root backup registry interpretation is imposed.",
    "Public v0/M2 fixture inventories are shared compatibility facts; expected histories are independently composed, not concealed held-out data.",
)


def _bytes(value: Any, *, ascii: bool = True) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=ascii, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def _sha(value: Any) -> str:
    return hashlib.sha256(_bytes(value)).hexdigest()


def _document(source: str, text: str) -> dict[str, Any]:
    return {"document_id": "doc-" + hashlib.sha256(b"document\0" + source.encode()).hexdigest(),
            "source_id": "src-" + hashlib.sha256(b"source\0" + source.encode()).hexdigest(),
            "source": source, "title": source.rsplit("/", 1)[-1],
            "blob_id": "blob-" + hashlib.sha256(text.encode()).hexdigest(), "text": text}


def _record(source: str, text: str, *, revision: int = 1, version: int = 1,
            deleted: bool = False, notes: str = "", tags: list[str] | None = None,
            collections: list[str] | None = None) -> dict[str, Any]:
    return {"document": _document(source, text), "revision": revision,
            "edit_version": version, "deleted": deleted, "notes": notes,
            "tags": tags or [], "collections": collections or []}


def _revision(doc: dict[str, Any], number: int) -> dict[str, Any]:
    identity = (b"revision\0" + doc["document_id"].encode("ascii") + b"\0" +
                str(number).encode("ascii") + b"\0" + doc["blob_id"].encode("ascii"))
    return {"revision_id": "rev-" + hashlib.sha256(identity).hexdigest(),
            "revision": number, "blob_id": doc["blob_id"], "text": doc["text"]}


def _v1(record: dict[str, Any]) -> dict[str, Any]:
    doc = record["document"]
    return {**{key: doc[key] for key in ("document_id", "source_id", "source", "title")},
            "current_revision": _revision(doc, record["revision"]),
            **{key: deepcopy(record[key]) for key in ("edit_version", "deleted", "notes", "tags", "collections")}}


def _listing(records: list[dict[str, Any]], generation: int) -> dict[str, Any]:
    return {"records": deepcopy(records), "total": len(records), "generation": generation}


def _call(method: str, *args: Any, target: str = "service", observe: bool = True,
          select: list[str] | None = None, **kwargs: Any) -> dict[str, Any]:
    action: dict[str, Any] = {"op": "call", "method": method, "args": list(args),
                              "kwargs": kwargs, "target": target, "observe": observe}
    if select is not None:
        action["select"] = select
    return action


def _cli(*args: str) -> dict[str, Any]:
    return {"op": "cli", "args": list(args)}


def _cli_result(value: Any, code: int = 0) -> dict[str, Any]:
    return {"exit": code, "value": value, "other_stream_empty": True}


def _error(code: str) -> dict[str, str]:
    return {"error": code}


def _encoded_fixture(raw: bytes) -> dict[str, Any]:
    return {"encoding": "zlib-base64", "bytes": len(raw),
            "sha256": hashlib.sha256(raw).hexdigest(),
            "data": base64.b64encode(zlib.compress(raw, 9)).decode("ascii")}


def _case(identifier: str, fixture: bytes | None, actions: list[dict[str, Any]],
          expected: list[Any], requirements: tuple[str, ...] = REQUIREMENT_IDS,
          *, files: list[dict[str, str]] | None = None, defects: list[str]) -> dict[str, Any]:
    return {"id": identifier, "requirement_ids": list(requirements), "targeted_defects": defects,
            "input": {"fixture": _encoded_fixture(fixture) if fixture is not None else None,
                      "files": files or [], "actions": actions},
            "expected": {"observations": expected}}


# acceptance_cases and registry_manifest are defined below the generic observer.
CHILD_ADAPTER = r'''import base64
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import zlib

WORKSPACE = sys.argv[1] if len(sys.argv) == 2 else "/workspace"
sys.path.insert(0, WORKSPACE)
raw_input = sys.stdin.buffer.read(65537)
if len(raw_input) > 65536:
    raise ValueError("Case input bound")
payload = json.loads(raw_input)
from library.catalog.store import Store
from library.common import LibraryError
from library.ingestion.jobs import JobManager
from library.ingestion.local import import_file
from library.query.service import Service

def encode(value, ascii=True):
    return json.dumps(value, sort_keys=True, ensure_ascii=ascii, separators=(",", ":"), allow_nan=False).encode("utf-8")

def strict_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("Duplicate JSON key")
            result[key] = value
        return result
    def constant(_):
        raise ValueError("Nonfinite JSON")
    result = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs, parse_constant=constant)
    json.dumps(result, allow_nan=False)
    return result

def confined(root, value):
    path = Path(value)
    if path.is_absolute() or not value or any(part in (".", "..", "") for part in value.split("/")):
        raise ValueError("Unsafe authored fixture path")
    return root / path

def fixture_bytes(fixture):
    if set(fixture) != {"encoding", "bytes", "sha256", "data"} or fixture["encoding"] != "zlib-base64":
        raise ValueError("Unknown fixture encoding")
    if type(fixture["bytes"]) is not int or not 0 < fixture["bytes"] <= 1048576:
        raise ValueError("Fixture size bound")
    decoder = zlib.decompressobj()
    raw = decoder.decompress(base64.b64decode(fixture["data"], validate=True), 1048577)
    if len(raw) != fixture["bytes"] or not decoder.eof or decoder.unused_data or decoder.unconsumed_tail:
        raise ValueError("Invalid fixture stream")
    if hashlib.sha256(raw).hexdigest() != fixture["sha256"]:
        raise ValueError("Fixture hash mismatch")
    return raw

def cli_data(db, root, backups, arguments):
    bootstrap = "import runpy,sys;sys.path.insert(0,sys.argv.pop(1));runpy.run_module('library',run_name='__main__')"
    command = [sys.executable, "-I", "-c", bootstrap, WORKSPACE, "--db", str(db), "--root", str(root),
               "--backup-dir", str(backups), *arguments]
    with tempfile.TemporaryFile() as out, tempfile.TemporaryFile() as err:
        process = subprocess.run(command, stdout=out, stderr=err, stdin=subprocess.DEVNULL,
                                 timeout=8, check=False)
        out.seek(0); err.seek(0)
        raw, error = out.read(65537), err.read(65537)
    if len(raw) > 65536 or len(error) > 65536:
        raise ValueError("CLI observation output bound")
    return {"exit": process.returncode,
            "value": strict_json(raw if process.returncode == 0 else error),
            "other_stream_empty": not (error if process.returncode == 0 else raw)}

def invoke(action, store, root, backups):
    if store is None:
        raise ValueError("Observation requires an explicitly opened Store")
    if action["target"] == "import":
        function = lambda source: import_file(store, root, source)
    else:
        receiver = store if action["target"] == "store" else (Service(store, root, backup_dir=backups)
                    if action["target"] == "service" else JobManager(store))
        function = getattr(receiver, action["method"])
    try:
        return function(*action["args"], **action["kwargs"])
    except LibraryError as error:
        return {"error": error.code}

def project_result(result, action):
    if "select" in action:
        result = {key: result[key] for key in action["select"]}
    if action.get("canonical", False):
        raw = encode(result, ascii=False)
        result = {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
    return result

def read_database(db, op, action):
    connection = sqlite3.connect("file:" + str(db) + "?mode=ro", uri=True)
    try:
        if op == "schema":
            return {"schema": connection.execute("SELECT value FROM metadata WHERE key='schema'").fetchone()[0]}
        if op == "job_serialization":
            row = connection.execute("SELECT manifest,content_hashes,receipt FROM jobs WHERE job_id=?",
                                     (action["job_id"],)).fetchone()
            return dict(zip(("manifest_json", "content_hashes_json", "receipt_json"), row))
        if op == "normalized_rows":
            return {"revisions": [list(row) for row in connection.execute(
                        "SELECT revision_id,document_id,revision,blob_id FROM document_revisions ORDER BY document_id,revision")],
                    "heads": [list(row) for row in connection.execute(
                        "SELECT document_id,head_revision_id,edit_version FROM document_state ORDER BY document_id")],
                    "mirrors": [list(row) for row in connection.execute(
                        "SELECT document_id,blob_id FROM documents ORDER BY document_id")]}
    finally:
        connection.close()
    raise ValueError("Unknown database observation")

with tempfile.TemporaryDirectory() as directory:
    base = Path(directory); root = base / "input"; root.mkdir()
    backups = base / "backups"; backups.mkdir(); db = base / "library.sqlite"
    if payload["fixture"] is not None:
        db.write_bytes(fixture_bytes(payload["fixture"]))
    for file in payload["files"]:
        path = confined(root, file["path"]); path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(file["text"], encoding="utf-8")
    store = None; observations = []
    try:
        for action in payload["actions"]:
            op = action["op"]
            if op == "call":
                result = invoke(action, store, root, backups)
            elif op == "open":
                if store is not None:
                    store.close(); store = None
                try:
                    store = Store(db); result = {"opened": True}
                except LibraryError as error:
                    result = {"error": error.code}
            elif op == "close":
                if store is not None:
                    store.close(); store = None
                result = {"closed": True}
            elif op == "file_digest":
                data = db.read_bytes()
                result = {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}
            elif op in {"schema", "job_serialization", "normalized_rows"}:
                result = read_database(db, op, action)
            elif op == "read_backup":
                with confined(backups, action["name"]).open("rb") as stream:
                    raw = stream.read(65537)
                if len(raw) > 65536:
                    raise ValueError("Backup observation bound")
                result = strict_json(raw)
            elif op == "cli":
                result = cli_data(db, root, backups, action["args"])
            else:
                raise ValueError("Unknown observation action")
            result = project_result(result, action)
            if action.get("observe", True):
                observations.append(result)
    finally:
        if store is not None:
            store.close()
    output = encode({"observations": observations})
    if len(output) > 65536:
        raise ValueError("Case observation bound")
    sys.stdout.buffer.write(output + b"\n")
'''


def _histories(records: list[dict[str, Any]], texts: dict[str, list[str]], *, v1: bool) -> dict[str, Any]:
    result = {}
    for record in records:
        source = record["document"]["source"]
        rows = [_revision(_document(source, text), number) for number, text in enumerate(texts[source], 1)]
        if not v1:
            rows = [{key: row[key] for key in ("revision", "blob_id", "text")} for row in rows]
        result[record["document"]["document_id"]] = {"document_id": record["document"]["document_id"], "revisions": rows}
    return result


def _export(records: list[dict[str, Any]], generation: int, histories: dict[str, Any], *, v1: bool) -> dict[str, Any]:
    return {"format": "local-research-library-export-v4" if v1 else "local-research-library-export-v2",
            "generation": generation, "documents": [
                {"record": _v1(record) if v1 else deepcopy(record),
                 "revisions": deepcopy(histories.get(record["document"]["document_id"], {"revisions": []})["revisions"])}
                for record in records]}


def _canonical_observation(value: Any) -> dict[str, Any]:
    raw = _bytes(value, ascii=False)
    return {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}


def _normalized_rows(records: list[dict[str, Any]], histories: dict[str, Any]) -> dict[str, Any]:
    return {"revisions": [[row["revision_id"], docid, row["revision"], row["blob_id"]]
                           for docid, history in sorted(histories.items()) for row in history["revisions"]],
            "heads": sorted([[row["document"]["document_id"], _v1(row)["current_revision"]["revision_id"], row["edit_version"]]
                             for row in records]),
            "mirrors": sorted([[row["document"]["document_id"], row["document"]["blob_id"]] for row in records])}


def _fresh_api_cases() -> list[dict[str, Any]]:
    a, b = _record("a.txt", "café"), _record("b.txt", "café")
    aid, bid = a["document"]["document_id"], b["document"]["document_id"]
    ah1, bh1 = _histories([a, b], {"a.txt": ["café"], "b.txt": ["café"]}, v1=True).values()
    changed = _record("a.txt", "新", revision=2, version=2)
    annotated = deepcopy(changed); annotated.update(edit_version=3, notes="literal <i>draft</i>")
    histories4 = _histories([annotated, b], {"a.txt": ["café", "新"], "b.txt": ["café"]}, v1=True)
    histories2 = _histories([annotated, b], {"a.txt": ["café", "新"], "b.txt": ["café"]}, v1=False)
    base = "/api/v1/documents/" + aid
    actions = [{"op": "open"}, _call("request", "GET", "/health"),
               _call("diagnostics", select=["schema", "generation", "jobs"]),
               _call("migrate", target="store"),
               _call("import", "a.txt", target="import", observe=False),
               _call("import", "b.txt", target="import", observe=False),
               _call("list_v1"), _call("revisions_v1", aid), _call("revisions_v1", bid),
               _call("revisions_v1", aid, bh1["revisions"][0]["revision_id"]),
               _call("refresh_document", aid, 1, text="新"),
               _call("request", "POST", base + "/annotations", {"expected_version": 2,
                     "notes": "literal <i>draft</i>", "tags": [], "collections": []}),
               _call("show_v1", aid), _call("lifecycle_show", aid),
               _call("revisions_v1", aid), _call("revision_history", aid),
               _call("request", "GET", "/api/documents/" + aid),
               _call("request", "GET", base + "/revisions/" + ah1["revisions"][0]["revision_id"]),
               _call("request", "GET", base + "/revisions/" + bh1["revisions"][0]["revision_id"]),
               {"op": "normalized_rows"}, {"op": "open"},
               _call("list_v1"), _call("migrate", target="store")]
    expected = [{"opened": True}, [200, {"status": "ok", "schema": 4}],
                {"schema": 4, "generation": 0, "jobs": dict.fromkeys(("queued", "running", "completed", "cancelled", "failed"), 0)},
                {"from_schema": 4, "to_schema": 4, "migrated": False, "documents": 0, "jobs": 0},
                _listing([_v1(a), _v1(b)], 2), ah1, bh1, _error("not_found"),
                {"status": "refreshed", "record": changed}, [200, {"status": "updated", "record": _v1(annotated)}],
                _v1(annotated), annotated, histories4[aid], histories2[aid], [200, annotated["document"]],
                [200, ah1["revisions"][0]], [404, _error("not_found")],
                _normalized_rows([annotated, b], histories4), {"opened": True},
                _listing([_v1(annotated), _v1(b)], 4),
                {"from_schema": 4, "to_schema": 4, "migrated": False, "documents": 2, "jobs": 0}]
    cases = [_case("m4-fresh-revision-identity-and-numbered-adapters", None, actions, expected,
        files=[{"path": "a.txt", "text": "café"}, {"path": "b.txt", "text": "café"}],
        defects=["shared blob collapses distinct revision identities", "annotation invents content revision",
                 "wrong-document revision disclosed", "legacy shape gains revision fields", "schema4 reopen mutates tokens"])]

    bundle4 = _export([a, b], 2, _histories([a, b], {"a.txt": ["café"], "b.txt": ["café"]}, v1=True), v1=True)
    bundle2 = _export([a, b], 2, _histories([a, b], {"a.txt": ["café"], "b.txt": ["café"]}, v1=False), v1=False)
    byte_count = len(_bytes(bundle4, ascii=False))
    export_args: dict[str, Any] = {"ids": None, "include_deleted": False, "include_history": True, "max_bytes": byte_count}
    actions = [{"op": "open", "observe": False}, _call("import", "a.txt", target="import", observe=False),
               _call("import", "b.txt", target="import", observe=False),
               _call("export_v1", []), _call("export_v1", **export_args),
               dict(_call("export_v1", **export_args), canonical=True),
               _call("export_v1", **dict(export_args, max_bytes=byte_count - 1)),
               _call("export_bundle", None, include_history=True),
               _call("export"), _cli("export"), _cli("export-bundle", "--include-history"),
               _cli("export-v1", "--include-history"),
               _call("request", "POST", "/api/v1/export", export_args),
               _call("request", "GET", "/api/v1/documents?limit=01"),
               _call("request", "GET", "/api/v1/documents?limit=1&limit=1"),
               _call("request", "GET", "/api/v1/documents?offset=%2B1"),
               _call("request", "POST", base + "/delete", {"expected_version": True}),
               _call("request", "POST", base + "/delete", {"expected_version": 1, "extra": 0}),
               _call("request", "GET", "/api/v1/documents?generation=1")]
    legacy = {"format": "local-research-library-v0", "documents": [a["document"], b["document"]]}
    page = _listing([_v1(a)], 2); page["total"] = 2
    expected = [_export([], 2, {}, v1=True), bundle4, _canonical_observation(bundle4), _error("too_large"),
                bundle2, legacy, _cli_result(legacy), _cli_result(bundle2), _cli_result(bundle4),
                [200, bundle4], [200, page], *[[400, _error("invalid_request")] for _ in range(4)],
                [409, _error("stale_generation")]]
    cases.append(_case("m4-opt-in-export-and-strict-v1-routes", None, actions, expected,
        ("M4-API-SCHEMA", "M4-COMPATIBILITY"),
        files=[{"path": "a.txt", "text": "café"}, {"path": "b.txt", "text": "café"}],
        defects=["new version silently replaces legacy export", "byte cap counts Unicode characters", "empty selection means all",
                 "new routes permit duplicate query or boolean version", "new route drops pagination fence"]))

    first = _record("a.txt", "first alternate", revision=2, version=2)
    restored = _record("a.txt", "café", version=3)
    second = _record("a.txt", "second alternate", revision=2, version=4)
    h4 = _histories([second], {"a.txt": ["café", "second alternate"]}, v1=True)
    actions = [{"op": "open", "observe": False}, _call("import", "a.txt", target="import", observe=False),
               _call("backup", "initial.json", select=["name", "generation"]),
               _call("refresh_document", aid, 1, text="first alternate"),
               _call("show_v1", aid), _call("restore_backup", "initial.json", 2),
               {"op": "open"}, _call("show_v1", aid),
               _call("refresh_document", aid, 2, text="stale writer"),
               _call("refresh_document", aid, 3, text="second alternate"),
               _call("show_v1", aid), _call("revisions_v1", aid),
               _call("revisions_v1", aid, _v1(first)["current_revision"]["revision_id"]),
               _call("revision_history", aid), {"op": "normalized_rows"},
               _call("diagnostics", select=["schema", "generation", "documents", "revisions"])]
    expected = [{"name": "initial.json", "generation": 1}, {"status": "refreshed", "record": first},
                _v1(first), {"restored": True, "generation": 3, "documents": 1, "jobs": 0},
                {"opened": True}, _v1(restored), _error("stale_version"),
                {"status": "refreshed", "record": second}, _v1(second), h4[aid], _error("not_found"),
                _histories([second], {"a.txt": ["café", "second alternate"]}, v1=False)[aid],
                _normalized_rows([second], h4), {"schema": 4, "generation": 4, "documents": {"active": 1, "deleted": 0}, "revisions": 2}]
    cases.append(_case("m4-restore-reused-number-new-content-identity", None, actions, expected,
        ("M4-API-SCHEMA", "M4-COMPATIBILITY"),
        files=[{"path": "a.txt", "text": "café"}],
        defects=["restored revision number reuses removed content identity", "restore rewinds edit token", "compatibility mirror points at removed head"]))
    return cases


def _v0_jobs_fixture() -> tuple[bytes, list[dict[str, Any]], list[dict[str, Any]]]:
    """Independently construct public schema0 plus authored M1 jobs, never product code."""
    import sqlite3
    doc = _document("receipt.txt", "original receipt text")
    rows: list[dict[str, Any]] = []
    for name, source, text, state in (("complete", "receipt.txt", doc["text"], "completed"),
                                      ("pending", "future.txt", "must not run", "queued")):
        job = {"job_id": name, "epoch": 1, "state": state, "total": 1,
               "completed": int(state == "completed"), "error": None}
        manifest = _bytes([{"source": source, "text": text}]).decode()
        hashes = _bytes([hashlib.sha256(text.encode()).hexdigest()]).decode()
        receipt = _bytes({"job": job, "documents": [doc]}).decode() if state == "completed" else None
        rows.append({"job": job, "manifest_json": manifest, "content_hashes_json": hashes,
                     "receipt_json": receipt})
    connection = sqlite3.connect(":memory:")
    try:
        connection.executescript("""
            PRAGMA page_size=4096;
            CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
            INSERT INTO metadata VALUES('schema','0');
            CREATE TABLE blobs(blob_id TEXT PRIMARY KEY,content BLOB NOT NULL);
            CREATE TABLE documents(document_id TEXT PRIMARY KEY,source_id TEXT UNIQUE NOT NULL,
              source TEXT UNIQUE NOT NULL,blob_id TEXT NOT NULL,title TEXT NOT NULL);
            CREATE TABLE jobs(job_id TEXT PRIMARY KEY,epoch INTEGER NOT NULL,state TEXT NOT NULL,
              total INTEGER NOT NULL,completed INTEGER NOT NULL,error TEXT,manifest TEXT NOT NULL,
              content_hashes TEXT NOT NULL,receipt TEXT);
        """)
        connection.execute("INSERT INTO blobs VALUES (?,?)", (doc["blob_id"], doc["text"].encode()))
        connection.execute("INSERT INTO documents VALUES (?,?,?,?,?)", tuple(doc[key] for key in
                           ("document_id", "source_id", "source", "blob_id", "title")))
        for row in rows:
            connection.execute("INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?,?)",
                tuple(row["job"][key] for key in ("job_id", "epoch", "state", "total", "completed", "error")) +
                tuple(row[key] for key in ("manifest_json", "content_hashes_json", "receipt_json")))
        connection.commit()
        return connection.serialize(), [_record(doc["source"], doc["text"])], rows
    finally:
        connection.close()


def _legacy_job_case() -> dict[str, Any]:
    raw, records, rows = _v0_jobs_fixture()
    complete, pending = rows
    response = {"from_schema": 0, "to_schema": 4, "migrated": True, "documents": 1, "jobs": 2}
    repeat = dict(response, from_schema=4, migrated=False)
    serialization = {key: complete[key] for key in ("manifest_json", "content_hashes_json", "receipt_json")}
    receipt = json.loads(complete["receipt_json"])
    actions = [{"op": "schema"}, _cli("migrate"), {"op": "open"},
               _call("list_v1"), _call("get", "pending", target="jobs"),
               {"op": "job_serialization", "job_id": "complete"},
               _call("commit", {"job_id": "complete", "epoch": 1}, target="jobs"),
               _call("commit", {"job_id": "complete", "epoch": 2}, target="jobs"),
               _cli("job-commit", "complete", "1"), _cli("migrate"),
               {"op": "open"}, {"op": "job_serialization", "job_id": "complete"},
               _call("get", "pending", target="jobs"), _call("list_v1")]
    expected = [{"schema": "0"}, _cli_result(response), {"opened": True}, _listing([_v1(records[0])], 0),
                pending["job"], serialization, receipt, _error("stale_epoch"), _cli_result(receipt),
                _cli_result(repeat), {"opened": True}, serialization, pending["job"], _listing([_v1(records[0])], 0)]
    return _case("m4-authored-m1-schema0-receipt-and-queued-job", raw, actions, expected,
        defects=["schema0 with jobs mistaken for corrupt or new empty DB", "migration silently runs queued job",
                 "completed replay replaced by revision projection", "stored receipt serialization rewritten"])


def _frozen_migration_cases() -> list[dict[str, Any]]:
    from gossip_harness.library_m4_fixture_v1 import fixture_files, snapshot_inventory
    files = fixture_files()
    cases = []
    for name in ("v0", "m2"):
        inventory = snapshot_inventory(name)
        records2, records4 = inventory["records2"], inventory["records4"]
        generation = inventory["generation"]
        histories4 = {docid: {"document_id": docid, "revisions": rows}
                      for docid, rows in inventory["histories4"].items()}
        job_counts = {state: sum(row["job"]["state"] == state for row in inventory["jobs"])
                      for state in ("queued", "running", "completed", "cancelled", "failed")}
        counts = inventory["counts"]
        diagnostics = {"schema": 4, "generation": generation,
                       "documents": {"active": counts["active"], "deleted": counts["deleted"]},
                       "revisions": counts["revisions"], "blobs": {"count": counts["blobs"], "bytes": counts["blob_bytes"]},
                       "jobs": job_counts, "worker_generation": 0}
        actions = [{"op": "schema"}, _cli("migrate"), {"op": "schema"}, {"op": "open"},
                   _call("diagnostics", select=list(diagnostics)),
                   _call("lifecycle_list", deleted="all"), _call("list_v1", deleted="all"),
                   _call("listing"), _call("list_collections")]
        expected = [{"schema": "0" if name == "v0" else "2"},
                    _cli_result(inventory["migration_response"]), {"schema": "4"}, {"opened": True},
                    diagnostics, _listing(records2, generation), _listing(records4, generation),
                    {"documents": inventory["active_documents"], "total": counts["active"]},
                    {"collections": inventory["collections"], "generation": generation}]
        for record2, record4 in zip(records2, records4):
            docid = record2["document"]["document_id"]
            actions.extend([_call("lifecycle_show", docid), _call("show_v1", docid),
                            _call("revision_history", docid), _call("revisions_v1", docid)])
            expected.extend([record2, record4, {"document_id": docid, "revisions": inventory["histories2"][docid]},
                             histories4[docid]])
        normalized = _normalized_rows(records2, histories4)
        actions.extend([{ "op": "normalized_rows"}, _cli("migrate"), {"op": "open"},
                        _call("migrate", target="store"), _call("diagnostics", select=list(diagnostics)),
                        _call("list_v1", deleted="all"), {"op": "normalized_rows"}])
        expected.extend([normalized, _cli_result(inventory["repeat_migration_response"]), {"opened": True},
                         inventory["repeat_migration_response"], diagnostics, _listing(records4, generation), normalized])
        cases.append(_case("m4-frozen-" + name + "-migration-preserves-inventory", files["compatibility/" + name + ".sqlite3"],
            actions, expected, defects=["migration loses shared or historical bytes", "migration replaces annotations or tombstones",
                                       "migration creates wrong revision IDs", "migration changes catalog generation or runs jobs",
                                       "repeated schema4 migration changes logical rows"]))
    return cases


def _snapshot_backup(inventory: dict[str, Any]) -> dict[str, Any]:
    payload = {"schema": 3, "generation": inventory["generation"], "documents": inventory["records2"],
               "revisions": [{"document_id": docid, "revision": revision["revision"], "blob_id": revision["blob_id"]}
                             for docid, rows in sorted(inventory["histories2"].items()) for revision in rows],
               "blobs": [{"blob_id": row["blob_id"], "text": row["text"]} for row in inventory["blobs"]],
               "collections": [row["name"] for row in inventory["collections"]],
               "jobs": [dict(deepcopy(row), enrolled=False) for row in inventory["jobs"]]}
    return {"format": "local-research-library-backup-v3", "payload": payload,
            "payload_sha256": hashlib.sha256(_bytes(payload, ascii=False)).hexdigest()}


def _frozen_receipt_case() -> dict[str, Any]:
    from gossip_harness.library_m4_fixture_v1 import fixture_files, snapshot_inventory
    inventory = snapshot_inventory("m2")
    actions: list[dict[str, Any]] = [{"op": "open"}]
    expected: list[Any] = [{"opened": True}]
    for row in inventory["jobs"]:
        job = row["job"]
        actions.extend([_call("get", job["job_id"], target="jobs"),
                        {"op": "job_serialization", "job_id": job["job_id"]}])
        expected.extend([job, {key: row[key] for key in ("manifest_json", "content_hashes_json", "receipt_json")}])
        if job["state"] == "completed":
            receipt = json.loads(row["receipt_json"])
            token = {"job_id": job["job_id"], "epoch": job["epoch"]}
            actions.extend([_call("commit", token, target="jobs"),
                            _call("commit", dict(token, epoch=job["epoch"] + 1), target="jobs"),
                            _cli("job-commit", job["job_id"], str(job["epoch"]))])
            expected.extend([receipt, _error("stale_epoch"), _cli_result(receipt)])
    generation = inventory["generation"]
    actions.extend([_call("backup", "migrated.json", select=["name", "generation"]),
                    {"op": "read_backup", "name": "migrated.json"},
                    _call("restore_backup", "migrated.json", generation), {"op": "open"}])
    expected.extend([{"name": "migrated.json", "generation": generation}, _snapshot_backup(inventory),
                     {"restored": True, "generation": generation + 1, "documents": 5, "jobs": 7}, {"opened": True}])
    for row in inventory["jobs"]:
        job = row["job"]
        actions.append({"op": "job_serialization", "job_id": job["job_id"]})
        expected.append({key: row[key] for key in ("manifest_json", "content_hashes_json", "receipt_json")})
        if job["state"] == "completed":
            actions.append(_call("commit", {"job_id": job["job_id"], "epoch": job["epoch"]}, target="jobs"))
            expected.append(json.loads(row["receipt_json"]))
    actions.append(_call("diagnostics", select=["schema", "generation", "jobs", "worker_generation"]))
    expected.append({"schema": 4, "generation": generation + 1,
                     "jobs": {state: sum(row["job"]["state"] == state for row in inventory["jobs"])
                              for state in ("queued", "running", "completed", "cancelled", "failed")},
                     "worker_generation": 0})
    return _case("m4-frozen-m2-receipt-bytes-through-backup-restore", fixture_files()["compatibility/m2.sqlite3"],
        actions, expected, defects=["completed historical receipt projected from current head", "receipt text reserialized",
                                   "empty completed receipt lost", "deferred surrogate job rejected before preparation",
                                   "schema4 backup leaks physical revision tables", "completed epoch rewritten by restore"])


def _corrupt_fixture(raw: bytes, statements: list[str]) -> bytes:
    """Mutate an isolated in-memory SQLite snapshot, without evaluated application code."""
    import sqlite3
    connection = sqlite3.connect(":memory:")
    try:
        connection.deserialize(raw)
        for statement in statements:
            connection.execute(statement)
        connection.commit()
        return connection.serialize()
    finally:
        connection.close()


def _corruption_cases() -> list[dict[str, Any]]:
    from gossip_harness.library_m4_fixture_v1 import corruption_recipes, fixture_files
    files = fixture_files()
    cases = []
    for recipe in corruption_recipes():
        raw = _corrupt_fixture(files["compatibility/" + recipe["fixture"] + ".sqlite3"], recipe["sql"])
        digest = {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
        code = recipe["error"]
        actions = [{"op": "file_digest"}, _cli("migrate"), {"op": "file_digest"},
                   {"op": "open"}, {"op": "file_digest"}]
        expected = [digest, _cli_result(_error(code), 2), digest, _error(code), digest]
        cases.append(_case("m4-reject-" + recipe["id"] + "-without-replacement", raw, actions, expected,
                           ("M4-MIGRATION",), defects=["migration repairs recognized corruption silently",
                                                       "failed open or migrate changes original database bytes"]))
    return cases


def acceptance_cases() -> list[dict[str, Any]]:
    """Fresh independent definitions; evaluated application code is never imported here."""
    return (_frozen_migration_cases() + [_legacy_job_case(), _frozen_receipt_case(), _cli_projection_case(),
            _v1_mutation_case()] + _fresh_api_cases() + _corruption_cases())


def registry_manifest() -> dict[str, Any]:
    from gossip_harness.library_m4_fixture_v1 import fixture_manifest
    cases = acceptance_cases()
    return {"protocol": PROTOCOL, "purpose": PURPOSE, "status": "unqualified_definition",
            "contract_sha256": CONTRACT_SHA256,
            "public_fixture_files_sha256": fixture_manifest()["files_sha256"],
            "case_ids": [case["id"] for case in cases],
            "ordered_inputs_sha256": _sha([{"id": case["id"], "input": case["input"]} for case in cases]),
            "ordered_expected_sha256": _sha([{"id": case["id"], "expected": case["expected"]} for case in cases]),
            "ordered_cases_sha256": _sha(cases), "adapter_sha256": hashlib.sha256(CHILD_ADAPTER.encode()).hexdigest(),
            "requirement_cases": {key: [case["id"] for case in cases if key in case["requirement_ids"]]
                                  for key in REQUIREMENT_IDS},
            "uncovered_requirements": ["M4-RELEASE-HANDOFF"], "limitations": list(LIMITATIONS)}


def _cli_projection_case() -> dict[str, Any]:
    from gossip_harness.library_m4_fixture_v1 import fixture_files, snapshot_inventory
    inventory = snapshot_inventory("m2")
    alpha = inventory["records2"][0]
    docid = alpha["document"]["document_id"]
    other = inventory["records2"][1]["document"]["document_id"]
    revisions = inventory["histories4"][docid]
    wrong_id = inventory["histories4"][other][0]["revision_id"]
    actions = [_cli("migrate"), _cli("documents-v1", "--deleted", "all"),
               _cli("document-v1", docid), _cli("revisions-v1", docid),
               _cli("revisions-v1", docid, "--revision-id", revisions[1]["revision_id"]),
               _cli("revisions-v1", docid, "--revision-id", wrong_id),
               _cli("document", docid), _cli("revisions", docid), _cli("show", docid),
               _cli("documents", "--deleted", "all")]
    expected = [_cli_result(inventory["migration_response"]),
                _cli_result(_listing(inventory["records4"], inventory["generation"])),
                _cli_result(inventory["records4"][0]), _cli_result({"document_id": docid, "revisions": revisions}),
                _cli_result(revisions[1]), _cli_result(_error("not_found"), 2),
                _cli_result(alpha), _cli_result({"document_id": docid, "revisions": inventory["histories2"][docid]}),
                _cli_result(alpha["document"]), _cli_result(_listing(inventory["records2"], inventory["generation"]))]
    return _case("m4-cli-v1-document-revision-adapters", fixture_files()["compatibility/m2.sqlite3"], actions, expected,
        defects=["explicit new CLI commands missing", "single revision CLI does not scope document identity",
                 "legacy CLI unexpectedly switches to revision-ID shape"])


def _v1_mutation_case() -> dict[str, Any]:
    initial = _record("a.txt", "one")
    refreshed = _record("a.txt", "two", revision=2, version=2)
    deleted = deepcopy(refreshed); deleted.update(deleted=True, edit_version=3)
    restored = deepcopy(refreshed); restored.update(edit_version=4)
    annotated = deepcopy(refreshed); annotated.update(edit_version=5, notes="new notes")
    docid = initial["document"]["document_id"]
    base = "/api/v1/documents/" + docid
    actions = [{"op": "open", "observe": False}, _call("import", "a.txt", target="import", observe=False),
               _call("request", "POST", base + "/refresh", {"expected_version": 1, "text": "two"}),
               _call("request", "POST", base + "/delete", {"expected_version": 2}),
               _call("request", "GET", base), _call("request", "GET", "/api/v1/documents"),
               _call("request", "POST", base + "/restore", {"expected_version": 3}),
               _call("request", "POST", base + "/annotations", {"expected_version": 4,
                     "notes": "new notes", "tags": [], "collections": []}),
               _call("request", "POST", base + "/refresh", {
                   "expected_version": _v1(annotated)["current_revision"]["revision_id"], "text": "wrong token type"}),
               _call("request", "POST", base + "/refresh", {"expected_version": 4, "text": "stale token"}),
               _call("lifecycle_show", docid), _call("revisions_v1", docid)]
    expected = [[200, {"status": "refreshed", "record": _v1(refreshed)}],
                [200, {"status": "deleted", "record": _v1(deleted)}], [200, _v1(deleted)],
                [200, _listing([], 3)], [200, {"status": "restored", "record": _v1(restored)}],
                [200, {"status": "updated", "record": _v1(annotated)}], [400, _error("invalid_request")],
                [409, _error("stale_version")], annotated,
                _histories([annotated], {"a.txt": ["one", "two"]}, v1=True)[docid]]
    return _case("m4-v1-mutations-preserve-edit-token-authority", None, actions, expected,
        ("M4-API-SCHEMA", "M4-COMPATIBILITY"), files=[{"path": "a.txt", "text": "one"}],
        defects=["v1 mutation returns numbered response", "revision ID used as edit token", "delete/restore invents revision",
                 "annotation accepts previous token", "tombstone appears in active v1 listing"])
