"""Independent SQL authoring recipe. Never imports an evaluated application.

Run only against a NEW empty directory. Committed snapshots are the byte source
of truth; rebuilding on another SQLite version is not an evidence substitute.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sqlite3
import sys
from typing import Any

CONTRACT_SHA256 = "3353dbaa0c2474527cbffb0d87a4091b5f77ed3f98418fff2f216d90b6a8487c"
VERSION = "library-m4-compatibility-v1"
LEGACY_SQL = """
CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE blobs(blob_id TEXT PRIMARY KEY,content BLOB NOT NULL);
CREATE TABLE documents(document_id TEXT PRIMARY KEY,source_id TEXT UNIQUE NOT NULL,source TEXT UNIQUE NOT NULL,blob_id TEXT NOT NULL,title TEXT NOT NULL);
"""
JOBS_SQL = """
CREATE TABLE jobs(job_id TEXT PRIMARY KEY,epoch INTEGER NOT NULL,state TEXT NOT NULL,total INTEGER NOT NULL,completed INTEGER NOT NULL,error TEXT,manifest TEXT NOT NULL,content_hashes TEXT NOT NULL,receipt TEXT);
"""
M2_SQL = """
CREATE TABLE lifecycle(document_id TEXT PRIMARY KEY REFERENCES documents(document_id),current_revision INTEGER NOT NULL,edit_version INTEGER NOT NULL,deleted INTEGER NOT NULL,notes TEXT NOT NULL,tags TEXT NOT NULL,collections TEXT NOT NULL);
CREATE TABLE revisions(document_id TEXT NOT NULL REFERENCES documents(document_id),revision INTEGER NOT NULL,blob_id TEXT NOT NULL REFERENCES blobs(blob_id),PRIMARY KEY(document_id,revision));
CREATE TABLE collections(name TEXT PRIMARY KEY);
"""


def canonical(value: Any, *, ascii: bool = True) -> str:
    return json.dumps(value, ensure_ascii=ascii, sort_keys=True, separators=(",", ":"), allow_nan=False)


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def document(source: str, text: str) -> dict[str, Any]:
    encoded = source.encode("utf-8")
    return {"document_id": "doc-" + sha(b"document\0" + encoded),
            "source_id": "src-" + sha(b"source\0" + encoded), "source": source,
            "blob_id": "blob-" + sha(text.encode("utf-8")),
            "title": source.rsplit("/", 1)[-1], "text": text}


def revision4(doc_id: str, number: int, text: str) -> dict[str, Any]:
    blob_id = "blob-" + sha(text.encode("utf-8"))
    raw = b"revision\0" + doc_id.encode("ascii") + b"\0" + str(number).encode("ascii") + b"\0" + blob_id.encode("ascii")
    return {"revision_id": "rev-" + sha(raw), "revision": number, "blob_id": blob_id, "text": text}


def record(source: str, texts: list[str], *, edit: int = 1, deleted: bool = False,
           notes: str = "", tags: list[str] | None = None, collections: list[str] | None = None) -> dict[str, Any]:
    return {"record": {"document": document(source, texts[-1]), "revision": len(texts),
                       "edit_version": edit, "deleted": deleted, "notes": notes,
                       "tags": tags or [], "collections": collections or []},
            "revisions": [{"revision": i, "blob_id": "blob-" + sha(text.encode("utf-8")), "text": text}
                          for i, text in enumerate(texts, 1)]}


def job(job_id: str, entries: list[dict[str, str]], *, state: str = "queued", epoch: int = 1,
        error: str | None = None) -> dict[str, Any]:
    entries = sorted(entries, key=lambda entry: (entry["source"], entry["text"]))
    public = {"job_id": job_id, "epoch": epoch, "state": state,
              "total": len(entries), "completed": len(entries) if state == "completed" else 0, "error": error}
    hashes: list[str | None] = []
    for entry in entries:
        try:
            hashes.append(sha(entry["text"].encode("utf-8")))
        except UnicodeEncodeError:
            hashes.append(None)
    receipt = {"job": public, "documents": [document(e["source"], e["text"]) for e in entries]} if state == "completed" else None
    return {"job": public, "manifest_json": canonical(entries), "content_hashes_json": canonical(hashes),
            "receipt_json": canonical(receipt) if receipt is not None else None}


def definitions(name: str) -> dict[str, Any]:
    shared = "Shared café\nline two\r\n"
    if name == "v0":
        rows = [record("empty.txt", [""]), record("literal.md", ["<script>throw new Error('literal')</script>\n"]),
                record("shared-a.txt", [shared]), record("shared-b.txt", [shared]),
                record("unicode/猫.md", ["e\u0301 versus é\n東京 🧪\n"])]
        return {"schema": 0, "generation": 0, "records": rows, "collections": [], "jobs": [],
                "authored_history": ["Five valid v0 imports; two sources share identical bytes; no jobs table exists.",
                                      "The empty document is legal; non-ASCII source/text and decomposed/composed Unicode remain exact.",
                                      "Migration creates one revision and edit_version 1 per document, with generation 0."]}
    if name != "m2":
        raise ValueError("Unknown fixture")
    original = [{"source": "alpha.md", "text": shared}, {"source": "empty.txt", "text": ""},
                {"source": "shared-a.txt", "text": shared}, {"source": "shared-b.txt", "text": shared},
                {"source": "tombstone/旧.md", "text": "Original tombstone text\n"}]
    rows = [record("alpha.md", [shared, "Alpha revised Ω\n", shared], edit=6,
                   notes="<b>Literal note</b>\ne\u0301 / é 🧪", tags=["café", "strasse"], collections=["papers", "études"]),
            record("empty.txt", [""]),
            record("shared-a.txt", [shared], edit=2, notes="Shared does not mean the same document.",
                   tags=["shared"], collections=["papers"]),
            record("shared-b.txt", [shared]),
            record("tombstone/旧.md", ["Original tombstone text\n", "Revised tombstone text\r\n"], edit=4,
                   deleted=True, notes="Retained on deletion", tags=["archive"], collections=["études"])]
    jobs = [job("batch-done", original, state="completed", epoch=3),
            job("empty-done", [], state="completed"),
            job("queue-later", [{"source": "future/queued.txt", "text": "Queued, not imported\n"}]),
            job("queue-surrogate", [{"source": "../deferred.txt", "text": "\ud800"}]),
            job("running-later", [{"source": "future/running.html", "text": "<i>Not committed</i>"}], state="running", epoch=3),
            job("cancelled-later", [{"source": "future/cancelled.txt", "text": "Cancelled"}], state="cancelled", epoch=4),
            job("failed-type", [{"source": "future/tool.exe", "text": "Not a supported source"}], state="failed", error="unsupported_type")]
    history = ["Before M2: submit/cancel/retry/prepare/commit batch-done at epoch 3; all five receipt documents reflect original bytes.",
               "Before M2: empty-done completes an empty batch at epoch 1; migration to schema 2 starts catalog_generation 0.",
               "generation 1: create normalized collection papers; generation 2: create normalized collection études.",
               "generations 3 and 4: refresh alpha.md twice (original -> revised -> original); edit_version becomes 3.",
               "generation 5: replace alpha annotations; generation 6: delete alpha; generation 7: restore alpha; edit_version 6.",
               "generation 8: replace shared-a annotations; edit_version 2.",
               "generation 9: refresh tombstone; generation 10: replace its annotations; generation 11: delete it; edit_version 4.",
               "Job-only transitions do not change generation: queue-later and queue-surrogate are newly admitted; running-later was cancelled, retried and prepared; cancelled-later was cancelled, retried and cancelled again; failed-type fails validation.",
               "queue-surrogate is legal deferred admission: escaped unpaired surrogate text has null content hash; source validation and UTF-8 rejection are deferred, not migration work."]
    return {"schema": 2, "generation": 11, "records": rows, "collections": ["papers", "études"],
            "jobs": sorted(jobs, key=lambda item: item["job"]["job_id"]), "authored_history": history}


def inventory(definition: dict[str, Any]) -> dict[str, Any]:
    records = definition["records"]
    blobs = {revision["blob_id"]: revision["text"] for row in records for revision in row["revisions"]}
    records4 = []
    histories4 = {}
    for item in records:
        old = item["record"]; doc = old["document"]
        revisions = [revision4(doc["document_id"], rev["revision"], rev["text"]) for rev in item["revisions"]]
        current = {key: doc[key] for key in ("document_id", "source_id", "source", "title")}
        current.update({key: old[key] for key in ("edit_version", "deleted", "notes", "tags", "collections")})
        current["current_revision"] = revisions[-1]
        records4.append(current); histories4[doc["document_id"]] = revisions
    return {"generation": definition["generation"], "records2": [row["record"] for row in records],
            "histories2": {row["record"]["document"]["document_id"]: row["revisions"] for row in records},
            "records4": records4, "histories4": histories4,
            "active_documents": [row["record"]["document"] for row in records if not row["record"]["deleted"]],
            "blobs": [{"blob_id": key, "text": text, "bytes": len(text.encode("utf-8"))} for key, text in sorted(blobs.items())],
            "collections": [{"name": name, "total": sum(name in row["record"]["collections"] for row in records)}
                            for name in definition["collections"]],
            "jobs": definition["jobs"],
            "counts": {"documents": len(records), "active": sum(not row["record"]["deleted"] for row in records),
                       "deleted": sum(row["record"]["deleted"] for row in records),
                       "revisions": sum(len(row["revisions"]) for row in records), "blobs": len(blobs),
                       "blob_bytes": sum(len(text.encode("utf-8")) for text in blobs.values()),
                       "jobs": len(definition["jobs"])},
            "migration_response": {"from_schema": definition["schema"], "to_schema": 4, "migrated": True,
                                   "documents": len(records), "jobs": len(definition["jobs"])},
            "repeat_migration_response": {"from_schema": 4, "to_schema": 4, "migrated": False,
                                          "documents": len(records), "jobs": len(definition["jobs"] )}}


def write_snapshot(path: Path, definition: dict[str, Any]) -> None:
    # Exclusive creation prevents accidental replacement of a frozen original.
    with path.open("xb"):
        pass
    db = sqlite3.connect(path)
    try:
        db.execute("PRAGMA page_size=4096"); db.execute("PRAGMA journal_mode=DELETE")
        db.execute("PRAGMA auto_vacuum=NONE"); db.execute("PRAGMA encoding='UTF-8'")
        db.execute("PRAGMA foreign_keys=ON")
        db.executescript(LEGACY_SQL)
        if definition["schema"] == 2:
            db.executescript(JOBS_SQL + M2_SQL)
        with db:
            db.execute("INSERT INTO metadata VALUES('schema', ?)", (str(definition["schema"]),))
            if definition["schema"] == 2:
                db.execute("INSERT INTO metadata VALUES('catalog_generation', ?)", (str(definition["generation"]),))
            expected = inventory(definition)
            db.executemany("INSERT INTO blobs VALUES(?,?)", [(row["blob_id"], row["text"].encode("utf-8")) for row in expected["blobs"]])
            for item in definition["records"]:
                row = item["record"]; doc = row["document"]
                db.execute("INSERT INTO documents VALUES(?,?,?,?,?)", tuple(doc[k] for k in ("document_id", "source_id", "source", "blob_id", "title")))
                if definition["schema"] == 2:
                    db.execute("INSERT INTO lifecycle VALUES(?,?,?,?,?,?,?)", (doc["document_id"], row["revision"], row["edit_version"], int(row["deleted"]), row["notes"], canonical(row["tags"], ascii=False), canonical(row["collections"], ascii=False)))
                    db.executemany("INSERT INTO revisions VALUES(?,?,?)", [(doc["document_id"], rev["revision"], rev["blob_id"]) for rev in item["revisions"]])
            if definition["schema"] == 2:
                db.executemany("INSERT INTO collections VALUES(?)", [(name,) for name in definition["collections"]])
                for item in definition["jobs"]:
                    row = item["job"]
                    db.execute("INSERT INTO jobs VALUES(?,?,?,?,?,?,?,?,?)", tuple(row[k] for k in ("job_id", "epoch", "state", "total", "completed", "error")) + (item["manifest_json"], item["content_hashes_json"], item["receipt_json"]))
        assert db.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
        assert db.execute("PRAGMA foreign_key_check").fetchall() == []
        db.execute("VACUUM")
    finally:
        db.close()


def build(directory: Path) -> None:
    if not directory.is_dir() or any(directory.iterdir()):
        raise ValueError("Authoring requires an existing empty output directory")
    builder_digest = sha(Path(__file__).read_bytes())
    for name in ("v0", "m2"):
        definition = definitions(name)
        path = directory / (name + ".sqlite3")
        write_snapshot(path, definition)
        with sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True) as db:
            schema = [{"name": row[0], "sql": row[1]} for row in db.execute("SELECT name,sql FROM sqlite_master WHERE type='table' ORDER BY name")]
        manifest = {"fixture_version": VERSION, "name": name, "purpose": "public_compatibility",
                    "schema": definition["schema"], "application_path": "compatibility/" + path.name,
                    "snapshot_sha256": sha(path.read_bytes()), "snapshot_bytes": path.stat().st_size,
                    "contract_sha256": CONTRACT_SHA256, "constructor_sha256": builder_digest,
                    "constructor_runtime": {"python": sys.version.split()[0], "sqlite": sqlite3.sqlite_version},
                    "construction": "Independently authored SQL and semantic records; no evaluated application imports or calls.",
                    "additional_tables": [], "tables": schema, "authored_history": definition["authored_history"],
                    "expected": inventory(definition),
                    "limits": {"max_snapshot_bytes": 1048576, "max_corruption_sql_statements": 2,
                               "corruption_destination": "fresh isolated copy; never the frozen original"}}
        (directory / (name + ".manifest.json")).write_text(json.dumps(manifest, sort_keys=True, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: build_snapshots.py NEW_EMPTY_DIRECTORY")
    build(Path(sys.argv[1]))
