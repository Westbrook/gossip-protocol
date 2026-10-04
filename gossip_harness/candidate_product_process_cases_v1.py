"""Public, host-only M3/M4 process histories for the cumulative v2 product.

Definitions derive from the public contracts and frozen public snapshot
inventories. They neither import candidate modules nor execute any process.
The runner receives only each ``input``. Exact expected values, diagnostic
projections and canonical export bytes stay on the host. Each factory call
returns independent JSON-compatible dictionaries; definition hashes freeze the
content rather than granting mutable caller data authority.
"""
from __future__ import annotations

import base64
from copy import deepcopy
import hashlib
import json
from typing import Any

PROTOCOL = "candidate-product-process-cases-v1"
PURPOSE = "public_product_definition"
CONTRACT_SHA256 = "2d88ce0775888f148b0ec3caf90b3d5c82d8fed71f53bec5f7e75f492ae998dc"
LIMITATIONS = (
    "Public development definitions, not held-out statistical samples or complete product acceptance.",
    "Finite worker --once processes and server restarts do not prove live daemon recovery, abrupt commit-boundary crashes, competing owners, stale claims or power-loss durability.",
    "No private Store/Service call, candidate import, fabricated child assertion, SQL inspection or candidate-generated expected value is used.",
    "Root rebinding covers an empty registry; populated-registry validation, pending-artifact provenance and interrupted publication need separate histories.",
    "Backup metadata observes exact logical payload digest and bounded envelope length, not an unspecified envelope whitespace encoding.",
    "Snapshot migration uses immutable public v0/M2 inputs; migration crash atomicity and schema3 live-handle incarnation fencing remain separate obligations.",
    "A host evaluator must authenticate raw process/wire captures and the exact input/source/runtime; these dictionaries alone authenticate no observation.",
)
DIAGNOSTIC_KEYS = ("schema", "generation", "documents", "revisions", "blobs", "jobs",
                   "worker_generation", "worker_state", "index_state", "last_error", "recovery_action")


def _canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def _document(source: str, text: str) -> dict[str, Any]:
    raw = source.encode("utf-8")
    return {"document_id": "doc-" + hashlib.sha256(b"document\0" + raw).hexdigest(),
            "source_id": "src-" + hashlib.sha256(b"source\0" + raw).hexdigest(),
            "source": source, "title": source.rsplit("/", 1)[-1], "text": text,
            "blob_id": "blob-" + hashlib.sha256(text.encode("utf-8")).hexdigest()}


def _id(source: str) -> str:
    return str(_document(source, "")["document_id"])


def _record(source: str, text: str, *, revision: int = 1, version: int = 1,
            deleted: bool = False) -> dict[str, Any]:
    return {"document": _document(source, text), "revision": revision, "edit_version": version,
            "deleted": deleted, "notes": "", "tags": [], "collections": []}


def _revision(source: str, text: str, number: int, *, v4: bool = False) -> dict[str, Any]:
    doc = _document(source, text)
    result = {"revision": number, "blob_id": doc["blob_id"], "text": text}
    if v4:
        identity = (b"revision\0" + doc["document_id"].encode("ascii") + b"\0" +
                    str(number).encode("ascii") + b"\0" + doc["blob_id"].encode("ascii"))
        result["revision_id"] = "rev-" + hashlib.sha256(identity).hexdigest()
    return result


def _v4(record: dict[str, Any]) -> dict[str, Any]:
    doc = record["document"]
    return {**{key: doc[key] for key in ("document_id", "source_id", "source", "title")},
            "current_revision": _revision(doc["source"], doc["text"], record["revision"], v4=True),
            **{key: deepcopy(record[key]) for key in ("edit_version", "deleted", "notes", "tags", "collections")}}


def _job(name: str, *, state: str = "queued", epoch: int = 1,
         total: int = 1, error: str | None = None) -> dict[str, Any]:
    return {"job_id": name, "epoch": epoch, "state": state, "total": total,
            "completed": total if state == "completed" else 0, "error": error}


def _listing(records: list[dict[str, Any]], generation: int) -> dict[str, Any]:
    return {"records": deepcopy(records), "total": len(records), "generation": generation}


def _index(published: int | None, target: int, processed: int, total: int,
           source: str | None, *, completed: bool = False) -> dict[str, Any]:
    return {"state": "completed" if completed else "running", "generation": published,
            "target_generation": target, "processed": processed, "total": total,
            "cursor": _id(source) if source is not None else None}


def _error(code: str) -> dict[str, str]:
    return {"error": code}


class _History:
    """Construction helper only; action inputs never contain expected responses."""
    def __init__(self, identifier: str, requirements: list[str], defects: list[str],
                 *, files: list[dict[str, str]] | None = None,
                 snapshot: dict[str, Any] | None = None) -> None:
        self.value: dict[str, Any] = {
            "id": identifier, "milestone": "M4", "requirement_ids": requirements, "targeted_defects": defects,
            "input": {"snapshot": snapshot, "files": files or [], "actions": []},
            "expected": {"observations": []},
        }

    def add(self, action: dict[str, Any], expectation: dict[str, Any]) -> None:
        self.value["input"]["actions"].append(action)
        self.value["expected"]["observations"].append(expectation)

    def cli(self, args: list[str], value: Any, *, exit: int = 0,
            backup_dir: str = "/tmp/backups", subset: bool = False,
            required_keys: tuple[str, ...] = (), value_only: bool = False) -> None:
        expected = {"kind": "cli", "exit": exit}
        expected.update({"json_value_only": True} if value_only else
                        {"json_subset" if subset else "json": value})
        if required_keys:
            expected["required_keys"] = list(required_keys)
        self.add({"op": "cli", "args": args, "backup_dir": backup_dir}, expected)

    def start(self) -> None:
        self.add({"op": "server_start"}, {"kind": "server_start"})

    def stop(self) -> None:
        self.add({"op": "server_stop"}, {"kind": "server_stop"})

    def http(self, method: str, path: str, value: Any, *, body: Any = None,
             status: int = 200, subset: bool = False,
             required_keys: tuple[str, ...] = (), canonical: bool = False,
             integer_ranges: dict[str, list[int]] | None = None) -> None:
        expected = {"kind": "http", "status": status,
                    "json_subset" if subset else "json": value}
        if required_keys:
            expected["required_keys"] = list(required_keys)
        if canonical:
            expected["body_utf8"] = _canonical(value).decode("utf-8")
        if integer_ranges:
            expected["integer_ranges"] = integer_ranges
        self.add({"op": "http", "method": method, "path": path, "body": body}, expected)

    def diag(self, value: dict[str, Any]) -> None:
        self.http("GET", "/api/maintenance/diagnostics", value, subset=True,
                  required_keys=DIAGNOSTIC_KEYS)

    def submit(self, name: str, entries: list[tuple[str, str]]) -> None:
        self.http("POST", "/api/jobs", _job(name, total=len(entries)),
                  body={"job_id": name, "entries": [{"source": s, "text": t} for s, t in entries]})

    def import_file(self, source: str, text: str) -> None:
        self.http("POST", "/api/import", {"status": "imported", "document": _document(source, text)},
                  body={"source": source})


def _worker_order() -> dict[str, Any]:
    h = _History("process-worker-explicit-enrollment-order",
                 ["M3-WORKER-RECOVERY", "M3-DIAGNOSTICS", "M3-INTERFACES", "V2-WORKER-LIVENESS"],
                 ["normal server consumes manual jobs", "enrollment lost across processes", "submission order wins",
                  "resume advances public epoch", "once drains whole queue", "persistent running row fakes live owner"])
    h.start()
    for name, text in (("manual", "manual"), ("b", "B"), ("a", "A")):
        h.submit(name, [(name + ".txt", text)])
    h.http("POST", "/api/jobs/a/prepare", {"job_id": "a", "epoch": 1}, body={})
    for name, status in (("b", "enqueued"), ("a", "enqueued"), ("a", "unchanged")):
        h.http("POST", "/api/maintenance/jobs/" + name + "/enqueue",
               {"status": status, "job": _job(name, state="running" if name == "a" else "queued")}, body={})
    h.diag({"generation": 0, "worker_generation": 0, "worker_state": "stopped"})
    h.stop()
    h.cli(["worker", "--once"], {"processed": "a", "job": _job("a", state="completed")})
    h.start()
    for name, state in (("a", "completed"), ("b", "queued"), ("manual", "queued")):
        h.http("GET", "/api/jobs/" + name, _job(name, state=state))
    h.http("GET", "/api/lifecycle/documents", _listing([_record("a.txt", "A")], 1))
    h.stop()
    h.cli(["worker", "--once"], {"processed": "b", "job": _job("b", state="completed")})
    h.cli(["worker", "--once"], {"processed": None, "job": None})
    h.start()
    h.http("POST", "/api/maintenance/jobs/a/enqueue", _error("job_state"), body={}, status=409)
    h.http("GET", "/api/jobs/manual", _job("manual"))
    h.diag({"generation": 2, "worker_generation": 3, "worker_state": "stopped",
            "jobs": {"queued": 1, "running": 0, "completed": 2, "failed": 0, "cancelled": 0}})
    h.stop()
    return h.value


def _worker_terminal() -> dict[str, Any]:
    h = _History("process-worker-terminal-retry-fencing", ["M3-WORKER-RECOVERY", "M3-DIAGNOSTICS"],
                 ["automatic terminal retry", "retry drops enrollment", "old epoch regains authority", "invalid admission mutates catalog"])
    h.start()
    h.submit("cancelled", [("cancel.txt", "saved")])
    h.submit("bad", [("../escape.txt", "rejected")])
    for name in ("cancelled", "bad"):
        h.http("POST", "/api/maintenance/jobs/" + name + "/enqueue", {"status": "enqueued", "job": _job(name)}, body={})
    h.http("POST", "/api/jobs/cancelled/cancel", _job("cancelled", state="cancelled", epoch=2), body={})
    h.http("POST", "/api/jobs/bad/prepare", _error("invalid_source"), body={}, status=400)
    h.stop()
    h.cli(["worker", "--once"], {"processed": None, "job": None})
    h.cli(["job-retry", "cancelled"], _job("cancelled", epoch=3))
    h.cli(["worker", "--once"], {"processed": "cancelled", "job": _job("cancelled", state="completed", epoch=3)})
    h.start()
    h.http("POST", "/api/jobs/cancelled/commit", _error("stale_epoch"), body={"epoch": 1}, status=409)
    h.http("GET", "/api/jobs/bad", _job("bad", state="failed", error="invalid_source"))
    h.http("GET", "/api/lifecycle/documents", _listing([_record("cancel.txt", "saved")], 1))
    h.diag({"generation": 1, "worker_generation": 2, "worker_state": "stopped",
            "documents": {"active": 1, "deleted": 0}, "revisions": 1,
            "blobs": {"count": 1, "bytes": 5}})
    h.stop()
    return h.value


def _reindex_restart() -> dict[str, Any]:
    files = [{"path": name + ".txt", "text": text} for name, text in (("a", "alpha"), ("b", "bravo"), ("c", "charlie"))]
    h = _History("process-reindex-resume-and-generation-restart", ["M3-REINDEX", "M3-DIAGNOSTICS", "M2-QUERY"],
                 ["lost durable cursor", "stale shadow published", "limit ignored on generation restart", "partial index answers query"], files=files)
    h.start()
    for f in files:
        h.import_file(f["path"], f["text"])
    h.http("POST", "/api/maintenance/reindex", _index(None, 3, 1, 3, "a.txt"), body={"limit": 1})
    h.stop()
    h.cli(["reindex", "--limit", "1"], _index(None, 3, 2, 3, "b.txt"))
    changed = _record("a.txt", "changed", revision=2, version=2)
    h.cli(["refresh", _id("a.txt"), "--expected-version", "1", "--text", "changed"], {"status": "refreshed", "record": changed})
    h.start()
    h.http("GET", "/api/lifecycle/documents?q=changed", _listing([changed], 4))
    h.http("POST", "/api/maintenance/reindex", _index(None, 4, 1, 3, "a.txt"), body={"limit": 1})
    deleted = _record("b.txt", "bravo", version=2, deleted=True)
    h.http("POST", "/api/lifecycle/documents/" + _id("b.txt") + "/delete",
           {"status": "deleted", "record": deleted}, body={"expected_version": 1})
    h.http("POST", "/api/maintenance/reindex", _index(None, 5, 1, 2, "a.txt"), body={"limit": 1})
    h.stop()
    h.cli(["reindex", "--limit", "64"], _index(5, 5, 2, 2, "c.txt", completed=True))
    h.start()
    h.diag({"generation": 5, "index_state": "current", "worker_state": "stopped"})
    h.http("GET", "/api/lifecycle/documents?q=bravo", _listing([], 5))
    h.http("GET", "/api/lifecycle/documents?deleted=all", _listing([changed, deleted, _record("c.txt", "charlie")], 5))
    h.stop()
    return h.value


def _export_boundary() -> dict[str, Any]:
    files = [{"path": "z.txt", "text": "literal <b>x</b>"}, {"path": "é.md", "text": "café"}]
    h = _History("process-export-canonical-selection-and-byte-limit", ["M3-EXPORT", "M3-INTERFACES", "M4-API-SCHEMA"],
                 ["Unicode byte limit uses character count", "partial oversized export", "selection order leaks", "empty selection means all", "history or tombstone omitted"], files=files)
    h.start()
    for f in files:
        h.import_file(f["path"], f["text"])
    active = _record("é.md", "新\n", revision=2, version=2)
    deleted = _record("z.txt", "literal <b>x</b>", version=2, deleted=True)
    h.http("POST", "/api/lifecycle/documents/" + _id("é.md") + "/refresh", {"status": "refreshed", "record": active}, body={"expected_version": 1, "text": "新\n"})
    h.http("POST", "/api/lifecycle/documents/" + _id("z.txt") + "/delete", {"status": "deleted", "record": deleted}, body={"expected_version": 1})
    histories = {"z.txt": ["literal <b>x</b>"], "é.md": ["café", "新\n"]}
    payload = {"format": "local-research-library-export-v2", "generation": 4,
               "documents": [{"record": r, "revisions": [_revision(r["document"]["source"], text, n) for n, text in enumerate(histories[r["document"]["source"]], 1)]} for r in (deleted, active)]}
    size = len(_canonical(payload))
    body = {"ids": [_id("é.md"), _id("z.txt")], "include_deleted": True, "include_history": True, "max_bytes": size}
    h.http("POST", "/api/export-bundle", payload, body=body, canonical=True)
    h.http("POST", "/api/export-bundle", _error("too_large"), body={**body, "max_bytes": size - 1}, status=400)
    h.http("POST", "/api/export-bundle", _error("not_found"), body={**body, "include_deleted": False}, status=404)
    h.http("POST", "/api/export-bundle", {"format": "local-research-library-export-v2", "generation": 4, "documents": []}, body={**body, "ids": []}, canonical=True)
    payload4 = {"format": "local-research-library-export-v4", "generation": 4,
                "documents": [{"record": _v4(r), "revisions": [_revision(r["document"]["source"], text, n, v4=True) for n, text in enumerate(histories[r["document"]["source"]], 1)]} for r in (deleted, active)]}
    h.http("POST", "/api/v1/export", payload4, body={**body, "max_bytes": 16777216}, canonical=True)
    h.stop()
    args = ["export-bundle", _id("é.md"), _id("z.txt"), "--include-deleted", "--include-history", "--max-bytes", str(size)]
    h.cli(args, payload)
    h.cli(args[:-1] + [str(size - 1)], _error("too_large"), exit=2)
    h.cli(["export-v1", "--include-deleted", "--include-history"], payload4)
    return h.value


def _root_adoption() -> dict[str, Any]:
    h = _History("process-backup-root-explicit-adoption-and-cas", ["V2-BACKUP-ROOT", "M3-BACKUP-RESTORE", "M3-INTERFACES"],
                 ["configuration implicitly binds ownership", "outdated expected root overwrites binding", "binding disappears on restart", "backup mismatch disables catalog"])
    h.start()
    h.http("GET", "/api/maintenance/backups", _error("backup_root_unbound"), status=409)
    h.http("POST", "/api/maintenance/backups", _error("backup_root_unbound"), body={"name": "new.json"}, status=409)
    h.http("GET", "/api/lifecycle/documents", _listing([], 0))
    h.stop()
    h.cli(["backup-root-adopt", "--expect-unbound"], {"adopted": True, "registered": 0})
    h.cli(["backups"], {"backups": [], "total": 0})
    h.cli(["backup-root-adopt", "--expect-root", "/tmp/backups"], {"adopted": False, "registered": 0})
    h.cli(["backup-root-adopt", "--expect-unbound"], _error("backup_root_mismatch"), exit=2, backup_dir="/tmp/next")
    h.cli(["backup-root-adopt", "--expect-root", "/tmp/backups"], {"adopted": True, "registered": 0}, backup_dir="/tmp/next")
    h.cli(["backups"], _error("backup_root_mismatch"), exit=2)
    h.cli(["backups"], {"backups": [], "total": 0}, backup_dir="/tmp/next")
    h.cli(["documents"], _listing([], 0))
    h.start()
    h.http("GET", "/api/maintenance/backups", _error("backup_root_mismatch"), status=409)
    h.diag({"generation": 0, "worker_generation": 0, "worker_state": "stopped"})
    h.http("GET", "/api/lifecycle/documents", _listing([], 0))
    h.stop()
    return h.value


def _backup_restore() -> dict[str, Any]:
    h = _History("process-backup-restore-fences-edits-and-removed-id", ["M3-BACKUP-RESTORE", "M2-IDENTITY-REVISIONS", "M4-COMPATIBILITY", "V2-BACKUP-ROOT"],
                 ["restore rewinds edit token", "post-snapshot record survives", "removed ID loses high-water", "restore downgrades schema", "repeat restore accepts stale generation"],
                 files=[{"path": "saved.txt", "text": "saved"}, {"path": "gone.txt", "text": "gone"}])
    h.cli(["backup-root-adopt", "--expect-unbound"], {"adopted": True, "registered": 0})
    h.start()
    h.import_file("saved.txt", "saved")
    saved = _record("saved.txt", "saved")
    payload = {"schema": 3, "generation": 1, "documents": [saved],
               "revisions": [{"document_id": _id("saved.txt"), "revision": 1, "blob_id": saved["document"]["blob_id"]}],
               "blobs": [{"blob_id": saved["document"]["blob_id"], "text": "saved"}], "collections": [], "jobs": []}
    metadata = {"name": "saved.json", "payload_sha256": hashlib.sha256(_canonical(payload)).hexdigest(), "generation": 1}
    h.http("POST", "/api/maintenance/backups", metadata, body={"name": "saved.json"}, subset=True,
           required_keys=("name", "bytes", "payload_sha256", "generation"), integer_ranges={"/bytes": [1, 67108864]})
    later = _record("saved.txt", "later", revision=2, version=2)
    h.http("POST", "/api/lifecycle/documents/" + _id("saved.txt") + "/refresh", {"status": "refreshed", "record": later}, body={"expected_version": 1, "text": "later"})
    h.import_file("gone.txt", "gone")
    h.stop()
    h.cli(["backup", "saved.json"], _error("already_exists"), exit=2)
    h.cli(["restore-backup", "saved.json", "--expected-generation", "3"], {"restored": True, "generation": 4, "documents": 1, "jobs": 0})
    restored = _record("saved.txt", "saved", version=3)
    h.start()
    h.http("GET", "/api/v1/documents", _listing([_v4(restored)], 4))
    h.http("GET", "/api/lifecycle/documents/" + _id("gone.txt"), _error("not_found"), status=404)
    h.http("POST", "/api/lifecycle/documents/" + _id("saved.txt") + "/refresh", _error("stale_version"), body={"expected_version": 2, "text": "obsolete token"}, status=409)
    h.http("POST", "/api/maintenance/restore", _error("stale_generation"), body={"name": "saved.json", "expected_generation": 3}, status=409)
    h.diag({"schema": 4, "generation": 4, "documents": {"active": 1, "deleted": 0}, "revisions": 1})
    h.stop()
    h.cli(["document", _id("saved.txt")], restored)
    h.cli(["import", "gone.txt"], None, value_only=True)
    h.cli(["document", _id("gone.txt")], _record("gone.txt", "gone", version=2))
    return h.value


def _migrations() -> list[dict[str, Any]]:
    # This loader authenticates independent public files, never candidate source.
    from gossip_harness.library_m4_fixture_v1 import fixture_files, snapshot_inventory
    files = fixture_files()
    result = []
    for name in ("v0", "m2"):
        raw = files["compatibility/" + name + ".sqlite3"]
        inventory = snapshot_inventory(name)
        snapshot = {"encoding": "base64", "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest(),
                    "data": base64.b64encode(raw).decode("ascii")}
        h = _History("process-public-" + name + "-migration-and-restart",
                     ["M4-MIGRATION", "M4-API-SCHEMA", "M4-COMPATIBILITY", "V2-BACKUP-ROOT"],
                     ["migration loses snapshot history", "revision IDs change after restart", "normal open runs jobs", "repeat migration mutates generation"], snapshot=snapshot)
        h.cli(["migrate"], inventory["migration_response"])
        h.start()
        h.http("GET", "/api/lifecycle/documents?deleted=all", _listing(inventory["records2"], inventory["generation"]))
        h.http("GET", "/api/v1/documents?deleted=all", _listing(inventory["records4"], inventory["generation"]))
        jobs = [row["job"] for row in inventory["jobs"]]
        h.http("GET", "/api/jobs", {"jobs": jobs})
        counts = inventory["counts"]
        h.diag({"schema": 4, "generation": inventory["generation"], "worker_generation": 0, "worker_state": "stopped",
                "documents": {"active": counts["active"], "deleted": counts["deleted"]}, "revisions": counts["revisions"],
                "blobs": {"count": counts["blobs"], "bytes": counts["blob_bytes"]}})
        for docid, rows in sorted(inventory["histories4"].items()):
            h.http("GET", "/api/v1/documents/" + docid + "/revisions", {"document_id": docid, "revisions": rows})
        h.stop()
        h.cli(["migrate"], inventory["repeat_migration_response"])
        h.cli(["documents-v1", "--deleted", "all"], _listing(inventory["records4"], inventory["generation"]))
        h.start()
        h.http("GET", "/api/jobs", {"jobs": jobs})
        h.http("GET", "/api/v1/documents?deleted=all", _listing(inventory["records4"], inventory["generation"]))
        h.http("GET", "/api/maintenance/backups", _error("backup_root_unbound"), status=409)
        h.stop()
        result.append(h.value)
    return result


def acceptance_cases() -> list[dict[str, Any]]:
    """Eight substantive real-process plans; no execution or acceptance claim."""
    return deepcopy([_worker_order(), _worker_terminal(), _reindex_restart(), _export_boundary(),
                     _root_adoption(), _backup_restore(), *_migrations()])


def registry_manifest() -> dict[str, Any]:
    cases = acceptance_cases()
    return {"protocol": PROTOCOL, "purpose": PURPOSE, "contract_sha256": CONTRACT_SHA256,
            "limitations": list(LIMITATIONS), "case_count": len(cases),
            "cases": [{"id": case["id"], "requirement_ids": case["requirement_ids"],
                       "action_count": len(case["input"]["actions"]),
                       "input_sha256": hashlib.sha256(_canonical(case["input"])).hexdigest(),
                       "expected_sha256": hashlib.sha256(_canonical(case["expected"])).hexdigest()}
                      for case in cases]}
