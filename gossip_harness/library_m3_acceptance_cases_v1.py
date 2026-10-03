"""Public-contract-derived M3 histories with a separate observation program.

Authored without reading M3 reference implementation or implementation tests.
These finite development qualification definitions are not candidate acceptance,
held-out evidence, full crash qualification, or proof of swarm superiority.
The host retains expectations; the child observes real public application APIs.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from typing import Any

PROTOCOL = "library-m3-independent-cases-v1"
PURPOSE = "independent_acceptance"
CONTRACT_SHA256 = "3353dbaa0c2474527cbffb0d87a4091b5f77ed3f98418fff2f216d90b6a8487c"
REQUIREMENT_IDS = (
    "M3-WORKER-RECOVERY", "M3-REINDEX", "M3-EXPORT", "M3-BACKUP-RESTORE",
    "M3-DIAGNOSTICS", "M3-INTERFACES",
)
LIMITATIONS = (
    "No candidate acceptance: caller must authenticate source/runtime/limits and the complete cohort freeze.",
    "Real worker --once and CLI processes are used; no forced crash at commit/publication boundaries or live-owner lock race.",
    "Reindex reopen and intervening second-connection writes are observed; no forced overlapping transaction schedule.",
    "No browser downloads, HTTP wire transport, backup fsync/power-loss or complete staging-cleanup qualification.",
    "Backup comparisons cover logical envelope and selected metadata, not a privately imposed envelope whitespace encoding.",
    "Export canonical-byte observations encode the returned Service object; actual browser download bytes require a separate lane.",
    "The contract does not distinguish idle from stopped absent a live owner; these histories do not impose either state.",
    "An ordinary pre-restore handle must detect incarnation changes, but its exact error is unspecified; histories reopen before writes.",
    "Finite valid/malformed backup fixtures do not exhaust every graph invariant, capacity bound or deferred Unicode state.",
    "Authored legacy snapshots inherit M2 fixture content_hashes_json as bare SHA256 hex arrays; that encoding needs explicit portable-fixture qualification before general candidate acceptance.",
)


def _bytes(value: Any, *, ascii: bool = True) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=ascii, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def _sha(value: Any) -> str:
    return hashlib.sha256(_bytes(value)).hexdigest()


def _document(source: str, text: str) -> dict[str, Any]:
    raw = source.encode("utf-8")
    return {"document_id": "doc-" + hashlib.sha256(b"document\0" + raw).hexdigest(),
            "source_id": "src-" + hashlib.sha256(b"source\0" + raw).hexdigest(),
            "source": source, "title": source.rsplit("/", 1)[-1],
            "blob_id": "blob-" + hashlib.sha256(text.encode("utf-8")).hexdigest(), "text": text}


def _id(source: str) -> str:
    return str(_document(source, "")["document_id"])


def _record(source: str, text: str, *, revision: int = 1, version: int = 1,
            deleted: bool = False, notes: str = "", tags: list[str] | None = None,
            collections: list[str] | None = None) -> dict[str, Any]:
    return {"document": _document(source, text), "revision": revision,
            "edit_version": version, "deleted": deleted, "notes": notes,
            "tags": tags or [], "collections": collections or []}


def _job(name: str, *, epoch: int = 1, state: str = "queued", total: int = 1,
         error: str | None = None) -> dict[str, Any]:
    return {"job_id": name, "epoch": epoch, "state": state, "total": total,
            "completed": total if state == "completed" else 0, "error": error}


def _job_row(name: str, entries: list[tuple[str, str]], *, epoch: int = 1,
             state: str = "queued", error: str | None = None,
             enrolled: bool = False) -> dict[str, Any]:
    manifest = [{"source": s, "text": t} for s, t in sorted(entries)]
    job = _job(name, epoch=epoch, state=state, total=len(entries), error=error)
    receipt = {"job": job, "documents": [_document(s, t) for s, t in sorted(entries)]} if state == "completed" else None
    return {"job": job, "manifest_json": _bytes(manifest).decode(),
            "content_hashes_json": _bytes([hashlib.sha256(t.encode()).hexdigest() for _, t in sorted(entries)]).decode(),
            "receipt_json": _bytes(receipt).decode() if receipt else None, "enrolled": enrolled}


def _backup(records: list[dict[str, Any]], histories: dict[str, list[str]], generation: int,
            *, jobs: list[dict[str, Any]] | None = None,
            collections: list[str] | None = None) -> dict[str, Any]:
    blobs: dict[str, str] = {}
    revisions = []
    for source, texts in histories.items():
        for revision, text in enumerate(texts, 1):
            doc = _document(source, text)
            blobs[doc["blob_id"]] = text
            revisions.append({"document_id": doc["document_id"], "revision": revision,
                              "blob_id": doc["blob_id"]})
    for row in jobs or []:
        if row["receipt_json"]:
            for doc in json.loads(row["receipt_json"])["documents"]:
                blobs[doc["blob_id"]] = doc["text"]
    payload = {"schema": 3, "generation": generation,
               "documents": sorted(deepcopy(records), key=lambda r: (r["document"]["source"], r["document"]["document_id"])),
               "revisions": sorted(revisions, key=lambda r: (r["document_id"], r["revision"])),
               "blobs": [{"blob_id": key, "text": blobs[key]} for key in sorted(blobs)],
               "collections": collections or [],
               "jobs": sorted(deepcopy(jobs or []), key=lambda r: r["job"]["job_id"])}
    return {"format": "local-research-library-backup-v3", "payload": payload,
            "payload_sha256": hashlib.sha256(_bytes(payload, ascii=False)).hexdigest()}


def _rehash(backup: dict[str, Any]) -> dict[str, Any]:
    backup["payload_sha256"] = hashlib.sha256(_bytes(backup["payload"], ascii=False)).hexdigest()
    return backup


def _call(method: str, *args: Any, target: str = "service", observe: bool = True,
          select: list[str] | None = None, **kwargs: Any) -> dict[str, Any]:
    result = {"op": "call", "target": target, "method": method, "args": list(args),
              "kwargs": kwargs, "observe": observe}
    if select is not None:
        result["select"] = select
    return result


def _error(code: str) -> dict[str, str]:
    return {"error": code}


def _listing(records: list[dict[str, Any]], generation: int) -> dict[str, Any]:
    return {"records": records, "total": len(records), "generation": generation}


def _index(state: str, published: int | None, target: int, processed: int,
           total: int, source: str | None) -> dict[str, Any]:
    return {"state": state, "generation": published, "target_generation": target,
            "processed": processed, "total": total, "cursor": _id(source) if source else None}


def _export(records: list[dict[str, Any]], generation: int,
            histories: dict[str, list[str]] | None = None) -> dict[str, Any]:
    documents = []
    for record in sorted(records, key=lambda r: (r["document"]["source"], r["document"]["document_id"])):
        source = record["document"]["source"]
        revisions = [{"revision": n, "blob_id": _document(source, text)["blob_id"], "text": text}
                     for n, text in enumerate((histories or {}).get(source, []), 1)]
        documents.append({"record": record, "revisions": revisions})
    return {"format": "local-research-library-export-v2", "generation": generation,
            "documents": documents}


def _canonical_observation(value: Any) -> dict[str, Any]:
    raw = _bytes(value, ascii=False)
    return {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}


def _cli(*args: str, observe: bool = True, json_data: bool = True,
         select: list[str] | None = None) -> dict[str, Any]:
    result = {"op": "cli", "args": list(args), "observe": observe, "json_data": json_data}
    if select is not None:
        result["select"] = select
    return result


def _cli_result(value: Any, code: int = 0) -> dict[str, Any]:
    return {"exit": code, "value": value, "other_stream_empty": True}


def _fixture(entries: list[tuple[str, str]], jobs: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {"entries": [list(entry) for entry in entries], "jobs": jobs or []}


def _case(identifier: str, requirements: list[str], actions: list[dict[str, Any]],
          expected: list[Any], *, entries: list[tuple[str, str]] | None = None,
          jobs: list[dict[str, Any]] | None = None, files: list[dict[str, Any]] | None = None,
          backups: list[dict[str, Any]] | None = None,
          defects: list[str]) -> dict[str, Any]:
    return {"id": identifier, "requirement_ids": requirements, "targeted_defects": defects,
            "input": {"fixture": _fixture(entries or [], jobs), "files": files or [],
                      "backups": backups or [], "actions": actions},
            "expected": {"observations": expected}}


def acceptance_cases() -> list[dict[str, Any]]:
    """Return fresh inputs and separate host-only expected observations."""
    cases = []
    a, b, c = (_record(s, t) for s, t in (("a.txt", "alpha"), ("b.txt", "bravo"), ("c.txt", "charlie")))
    changed = _record("a.txt", "changed", revision=2, version=2)
    tombstone = _record("b.txt", "bravo", version=2, deleted=True)
    cases.append(_case("m3-reindex-reopen-generation-change", ["M3-REINDEX", "M3-DIAGNOSTICS"], [
        _call("diagnostics", select=["generation", "index_state"]),
        _call("reindex_step", 1), _call("lifecycle_list"), {"op": "reopen"},
        _call("reindex_step", 1),
        {"op": "separate", "call": _call("refresh_document", _id("a.txt"), 1, text="changed")},
        _call("lifecycle_list", "changed"), _call("reindex_step", 1),
        _call("diagnostics", select=["generation", "index_state"]),
        _call("delete_document", _id("b.txt"), 1), _call("reindex_step", 1),
        {"op": "reopen"}, _call("reindex_step", 1), _call("lifecycle_list", "bravo"),
        _call("diagnostics", select=["generation", "index_state"]),
        _call("lifecycle_list", deleted="all"),
    ], [{"generation": 0, "index_state": "absent"}, _index("running", None, 0, 1, 3, "a.txt"),
        _listing([a, b, c], 0), {"reopened": True}, _index("running", None, 0, 2, 3, "b.txt"),
        {"status": "refreshed", "record": changed}, _listing([changed], 1),
        _index("running", None, 1, 1, 3, "a.txt"), {"generation": 1, "index_state": "building"},
        {"status": "deleted", "record": tombstone}, _index("running", None, 2, 1, 2, "a.txt"),
        {"reopened": True}, _index("completed", 2, 2, 2, 2, "c.txt"),
        _listing([], 2), {"generation": 2, "index_state": "current"},
        _listing([changed, tombstone, c], 2)], entries=[("c.txt", "charlie"), ("b.txt", "bravo"), ("a.txt", "alpha")],
        defects=["lost restart cursor", "unbounded restart step", "partial index used as authority", "stale generation publication"]))

    actions = [_call("reindex_step", x) for x in (0, 65, True, "1")]
    actions += [_call("reindex_step", 1), {"op": "reopen"}, _call("reindex_step", 64),
                _call("diagnostics", select=["generation", "index_state", "documents", "revisions", "blobs"])]
    cases.append(_case("m3-empty-index-and-limit-types", ["M3-REINDEX", "M3-DIAGNOSTICS"], actions,
        [_error("invalid_request")] * 4 + [_index("completed", 0, 0, 0, 0, None), {"reopened": True},
        _index("completed", 0, 0, 0, 0, None), {"generation": 0, "index_state": "current",
          "documents": {"active": 0, "deleted": 0}, "revisions": 0, "blobs": {"count": 0, "bytes": 0}}],
        defects=["boolean step admitted", "empty index never completes", "manufactured cursor"]))

    active = _record("é.md", "新\n", revision=2, version=2)
    deleted = _record("z.txt", "literal <b>x</b>", version=2, deleted=True)
    selected = _export([active, deleted], 2, {"é.md": ["café", "新\n"], "z.txt": ["literal <b>x</b>"]})
    selected_bytes = len(_bytes(selected, ascii=False))
    exact = _call("export_bundle", [_id("é.md"), _id("z.txt")], include_deleted=True,
                  include_history=True, max_bytes=selected_bytes)
    canonical = dict(exact, canonical=True)
    cases.append(_case("m3-export-selection-canonical-byte-bound", ["M3-EXPORT", "M3-INTERFACES"], [
        _call("refresh_document", _id("é.md"), 1, text="新\n", observe=False),
        _call("delete_document", _id("z.txt"), 1, observe=False),
        _call("export_bundle", []), _call("export_bundle", None),
        _call("export_bundle", [_id("z.txt")]), exact, canonical,
        _call("export_bundle", None, include_deleted=True, include_history=True, max_bytes=selected_bytes - 1),
        _call("export_bundle", [_id("é.md"), _id("é.md")]),
        _call("export_bundle", [_id("é.md"), "doc-" + "0" * 64]),
        _call("export_bundle", [], include_deleted=1), _call("export_bundle", [], max_bytes=True),
        _call("export_bundle", [], max_bytes=16777217),
        _call("export_bundle", [], max_bytes=1),
        _call("export_bundle", None, include_deleted=True),
    ], [_export([], 2), _export([active], 2), _error("not_found"), selected,
        _canonical_observation(selected), _error("too_large"), _error("invalid_request"),
        _error("not_found"), _error("invalid_request"), _error("invalid_request"),
        _error("invalid_request"), _error("too_large"), _export([active, deleted], 2)],
        entries=[("é.md", "café"), ("z.txt", "literal <b>x</b>")],
        defects=["empty selection exports all", "Unicode character count used as byte cap", "partial selection before validation",
                 "deleted record leak", "history silently omitted", "caller order overrides source ordering"]))

    job = _job_row("done", [("a.txt", "old")], state="completed")
    refreshed = _record("a.txt", "new", revision=2, version=3, deleted=True)
    envelope = _backup([refreshed], {"a.txt": ["old", "new"]}, 2, jobs=[job])
    digest = envelope["payload_sha256"]
    metadata = {"name": "snapshot.json", "payload_sha256": digest, "generation": 2}
    cases.append(_case("m3-backup-preserves-tombstone-history-receipt", ["M3-BACKUP-RESTORE", "M3-DIAGNOSTICS"], [
        _call("refresh_document", _id("a.txt"), 1, text="new", observe=False),
        _call("delete_document", _id("a.txt"), 2, observe=False),
        _call("backup", "snapshot.json", select=["name", "payload_sha256", "generation"]),
        {"op": "read_backup", "name": "snapshot.json"}, {"op": "reopen"},
        {"op": "backup_listing", "offset": 0, "limit": 1},
        {"op": "backup_listing", "offset": 1, "limit": 1},
        _call("backup", "snapshot.json"),
        _call("diagnostics", select=["generation", "documents", "revisions", "blobs", "last_error"]),
        {"op": "read_backup", "name": "snapshot.json"},
        _call("commit", {"job_id": "done", "epoch": 1}, target="jobs"),
        _call("backup", "next.json", select=["name", "generation"]),
        _call("diagnostics", select=["last_error"]),
    ], [metadata, envelope, {"reopened": True}, {"backups": [metadata], "total": 1},
        {"backups": [], "total": 1}, _error("already_exists"),
        {"generation": 2, "documents": {"active": 0, "deleted": 1}, "revisions": 2,
         "blobs": {"count": 2, "bytes": 6}, "last_error": {"operation": "backup", "code": "already_exists"}},
        envelope, json.loads(job["receipt_json"]), {"name": "next.json", "generation": 2}, {"last_error": None}],
        entries=[("a.txt", "old")], jobs=[job],
        defects=["backup only active heads", "receipt rewritten from current content", "backup overwrite", "stale diagnostics error after success"]))

    # Hand-authored portable envelope, never derived from implementation output.
    valid = _backup([_record("saved.txt", "saved")], {"saved.txt": ["saved"]}, 0)
    corruptions: list[tuple[str, dict[str, Any], str]] = []
    wrong_digest = deepcopy(valid); wrong_digest["payload_sha256"] = "0" * 64
    corruptions.append(("digest.json", wrong_digest, "invalid_backup"))
    missing_blob = deepcopy(valid); missing_blob["payload"]["blobs"] = []
    corruptions.append(("missing_blob.json", _rehash(missing_blob), "invalid_backup"))
    wrong_head = deepcopy(valid); wrong_head["payload"]["documents"][0]["document"]["blob_id"] = "blob-" + "0" * 64
    corruptions.append(("head.json", _rehash(wrong_head), "invalid_backup"))
    duplicate = deepcopy(valid); duplicate["payload"]["revisions"].append(deepcopy(duplicate["payload"]["revisions"][0]))
    corruptions.append(("duplicate.json", _rehash(duplicate), "invalid_backup"))
    gap = deepcopy(valid); gap["payload"]["revisions"][0]["revision"] = 2
    corruptions.append(("gap.json", _rehash(gap), "invalid_backup"))
    extra = deepcopy(valid); extra["extra"] = None
    corruptions.append(("extra.json", extra, "invalid_backup"))
    unsupported = deepcopy(valid); unsupported["format"] = "local-research-library-backup-v999"
    corruptions.append(("unsupported.json", unsupported, "unsupported_schema"))
    bad_schema = deepcopy(valid); bad_schema["payload"]["schema"] = 999
    corruptions.append(("schema.json", _rehash(bad_schema), "unsupported_schema"))
    unsorted = _backup([_record("b.txt", "b"), _record("a.txt", "a")], {"b.txt": ["b"], "a.txt": ["a"]}, 0)
    unsorted["payload"]["documents"].reverse()
    corruptions.append(("order.json", _rehash(unsorted), "invalid_backup"))
    actions = []; expected = []
    stable = {"generation": 0, "documents": {"active": 1, "deleted": 0},
              "revisions": 1, "blobs": {"count": 1, "bytes": 4}}
    for name, _, error in corruptions:
        actions += [_call("restore_backup", name, 0),
                    _call("diagnostics", select=list(stable)), _call("lifecycle_show", _id("live.txt"))]
        expected += [_error(error), stable, _record("live.txt", "live")]
    actions += [{"op": "backup_listing", "offset": 0, "limit": 100},
                _call("restore_backup", "valid.json", 1), _call("lifecycle_list"),
                _call("restore_backup", "valid.json", 0), {"op": "reopen"}, _call("lifecycle_list")]
    expected += [{"backups": [], "total": 0}, _error("stale_generation"), _listing([_record("live.txt", "live")], 0),
                 {"restored": True, "generation": 1, "documents": 1, "jobs": 0},
                 {"reopened": True}, _listing([_record("saved.txt", "saved", version=2)], 1)]
    cases.append(_case("m3-manual-backup-corrupt-graph-no-mutation", ["M3-BACKUP-RESTORE", "M3-DIAGNOSTICS"],
        actions, expected, entries=[("live.txt", "live")],
        backups=[{"name": name, "value": value} for name, value, _ in corruptions] + [{"name": "valid.json", "value": valid}],
        defects=["digest only validation", "dangling graph activation", "unsupported schema misclassification",
                 "partial restore mutation", "restore requires local registration", "unowned directory entry listed as completed backup"]))

    done = _job_row("done", [("kept.txt", "old")], state="completed")
    queued = _job_row("kept_job", [("future.txt", "future")])
    snap = _backup([_record("kept.txt", "new", revision=2, version=2)],
                   {"kept.txt": ["old", "new"]}, 1, jobs=[done, queued])
    kept_restored = _record("kept.txt", "new", revision=2, version=4)
    removed = _record("gone.txt", "gone", version=3)
    cases.append(_case("m3-restore-removal-edit-and-job-aba", ["M3-BACKUP-RESTORE", "M3-WORKER-RECOVERY"], [
        _call("refresh_document", _id("kept.txt"), 1, text="new", observe=False),
        _call("backup", "snap.json", observe=False), {"op": "read_backup", "name": "snap.json"},
        _call("refresh_document", _id("kept.txt"), 2, text="later", observe=False),
        _call("import", "gone.txt", target="import", observe=False),
        _call("refresh_document", _id("gone.txt"), 1, text="gone2", observe=False),
        _call("submit", "removed_job", [{"source": "uncommitted.txt", "text": "never"}], target="jobs", observe=False),
        _call("cancel", "removed_job", target="jobs", observe=False),
        _call("retry", "removed_job", target="jobs", observe=False),
        _call("prepare", "removed_job", target="jobs"),
        _call("prepare", "kept_job", target="jobs"),
        _call("restore_backup", "snap.json", 4), {"op": "reopen"},
        _call("lifecycle_list", deleted="all"),
        _call("refresh_document", _id("kept.txt"), 3, text="old token"),
        _call("commit", {"job_id": "kept_job", "epoch": 1}, target="jobs"),
        _call("get", "kept_job", target="jobs"),
        _call("get", "removed_job", target="jobs"),
        _call("commit", {"job_id": "done", "epoch": 1}, target="jobs"),
        {"op": "job_serialization", "job_id": "done"},
        _call("submit", "removed_job", [{"source": "uncommitted.txt", "text": "new"}], target="jobs"),
        _call("prepare", "removed_job", target="jobs"),
        _call("commit", {"job_id": "removed_job", "epoch": 3}, target="jobs"),
        _call("import", "gone.txt", target="import", observe=False),
        _call("lifecycle_show", _id("gone.txt")),
        _call("refresh_document", _id("gone.txt"), 2, text="old edit token"),
        _call("restore_backup", "snap.json", 4),
        _call("lifecycle_list", deleted="all"),
    ], [snap, {"job_id": "removed_job", "epoch": 3}, {"job_id": "kept_job", "epoch": 1},
        {"restored": True, "generation": 5, "documents": 1, "jobs": 2}, {"reopened": True},
        _listing([kept_restored], 5), _error("stale_version"), _error("stale_epoch"),
        _job("kept_job", epoch=2), _error("not_found"), json.loads(done["receipt_json"]),
        {k: done[k] for k in ("manifest_json", "content_hashes_json", "receipt_json")},
        _job("removed_job", epoch=4), {"job_id": "removed_job", "epoch": 4}, _error("stale_epoch"),
        removed, _error("stale_version"), _error("stale_generation"), _listing([removed, kept_restored], 6)],
        entries=[("kept.txt", "old")], jobs=[done, queued], files=[{"path": "gone.txt", "text": "gone"}],
        defects=["restore rewinds edit counter", "absent ID loses job fencing tombstone", "absent document loses edit fencing tombstone",
                 "restore keeps post-snapshot records", "completed receipt rewritten", "repeat restore ignores generation"]))

    invalid_job = _job_row("deferred", [("../escape.txt", "not yet validated")])
    manual = _backup([], {}, 0, jobs=[invalid_job])
    cases.append(_case("m3-restore-deferred-invalid-manifest", ["M3-BACKUP-RESTORE", "M3-WORKER-RECOVERY"], [
        _call("restore_backup", "deferred.json", 0), {"op": "reopen"},
        _call("get", "deferred", target="jobs"),
        _call("prepare", "deferred", target="jobs"), _call("get", "deferred", target="jobs"),
        _call("lifecycle_list"),
        _call("diagnostics", select=["documents", "revisions", "blobs"]),
    ], [{"restored": True, "generation": 1, "documents": 0, "jobs": 1}, {"reopened": True},
        _job("deferred", epoch=2), _error("invalid_source"),
        _job("deferred", epoch=2, state="failed", error="invalid_source"), _listing([], 1),
        {"documents": {"active": 0, "deleted": 0}, "revisions": 0, "blobs": {"count": 0, "bytes": 0}}],
        backups=[{"name": "deferred.json", "value": manual}],
        defects=["restore applies prepare validation to admitted manifest", "failed prepare leaves orphan blobs"]))

    a_job, b_job, manual_job = (_job(name) for name in ("a", "b", "manual"))
    completed_a, completed_b = (_job(name, state="completed") for name in ("a", "b"))
    cases.append(_case("m3-worker-once-explicit-enrollment-order", ["M3-WORKER-RECOVERY", "M3-INTERFACES"], [
        _call("submit", "manual", [{"source": "manual.txt", "text": "manual"}], target="jobs", observe=False),
        _call("submit", "b", [{"source": "b.txt", "text": "B"}], target="jobs", observe=False),
        _call("submit", "a", [{"source": "a.txt", "text": "A"}], target="jobs", observe=False),
        _call("prepare", "a", target="jobs"),
        _call("enqueue_job", "b"), _call("enqueue_job", "a"), _call("enqueue_job", "a"),
        {"op": "reopen"}, _call("get", "manual", target="jobs"),
        _call("get", "a", target="jobs"), _call("get", "b", target="jobs"),
        _cli("worker", "--once"), _call("get", "manual", target="jobs"),
        _call("lifecycle_list"), _cli("worker", "--once"), _cli("worker", "--once"),
        _call("enqueue_job", "a"), _call("get", "manual", target="jobs"),
        _call("diagnostics", select=["worker_generation", "jobs", "documents", "revisions"]),
    ], [{"job_id": "a", "epoch": 1}, {"status": "enqueued", "job": b_job},
        {"status": "enqueued", "job": _job("a", state="running")},
        {"status": "unchanged", "job": _job("a", state="running")},
        {"reopened": True}, manual_job, _job("a", state="running"), b_job,
        _cli_result({"processed": "a", "job": completed_a}), manual_job,
        _listing([_record("a.txt", "A")], 1), _cli_result({"processed": "b", "job": completed_b}),
        _cli_result({"processed": None, "job": None}), _error("job_state"), manual_job,
        {"worker_generation": 3, "jobs": {"queued": 1, "running": 0, "completed": 2, "cancelled": 0, "failed": 0},
         "documents": {"active": 2, "deleted": 0}, "revisions": 2}],
        defects=["opening runs un-enrolled jobs", "worker chooses submission order", "running job epoch advanced on resume",
                 "worker once drains whole queue", "completed enrollment allows retry"]))

    cases.append(_case("m3-worker-terminal-retry-remains-explicit", ["M3-WORKER-RECOVERY", "M3-DIAGNOSTICS"], [
        _call("submit", "cancelled", [{"source": "cancel.txt", "text": "saved"}], target="jobs", observe=False),
        _call("enqueue_job", "cancelled", observe=False), _call("cancel", "cancelled", target="jobs"),
        _call("submit", "bad", [{"source": "../escape.txt", "text": "rejected"}], target="jobs", observe=False),
        _call("enqueue_job", "bad", observe=False), _call("prepare", "bad", target="jobs"),
        _cli("worker", "--once"), _call("get", "bad", target="jobs"),
        _call("get", "cancelled", target="jobs"),
        _call("enqueue_job", "bad"), _call("enqueue_job", "cancelled"),
        _call("retry", "cancelled", target="jobs"), _cli("worker", "--once"),
        _call("commit", {"job_id": "cancelled", "epoch": 1}, target="jobs"),
        _call("get", "bad", target="jobs"), _call("lifecycle_list"),
    ], [_job("cancelled", state="cancelled", epoch=2), _error("invalid_source"),
        _cli_result({"processed": None, "job": None}), _job("bad", state="failed", error="invalid_source"),
        _job("cancelled", state="cancelled", epoch=2), _error("job_state"), _error("job_state"),
        _job("cancelled", epoch=3), _cli_result({"processed": "cancelled", "job": _job("cancelled", state="completed", epoch=3)}),
        _error("stale_epoch"), _job("bad", state="failed", error="invalid_source"),
        _listing([_record("cancel.txt", "saved")], 1)],
        defects=["terminal enrolled job retried without explicit transition", "retry loses enrollment", "old token regains authority"]))

    commands = [
        _cli("reindex", "--limit", "01"),
        _cli("reindex", "--limit", "-1", json_data=False, select=["exit"]),
        _cli("export-bundle", "--max-bytes", "1"),
        _cli("export-bundle"), _cli("backups", "--offset", "00", "--limit", "01"),
        _call("backup", "../escape.json"), _call("backup", "nested/escape.json"),
        _call("restore_backup", "../escape.json", 0),
    ]
    # Invalid CLI token syntax may be rejected by argparse with non-JSON usage
    # diagnostics. Only the specified exit code is compared for that action.
    cases.append(_case("m3-cli-maintenance-arguments", ["M3-INTERFACES", "M3-EXPORT", "M3-REINDEX"], commands,
        [_cli_result(_index("completed", 0, 0, 0, 0, None)), {"exit": 2},
         _cli_result(_error("too_large"), 2), _cli_result(_export([], 0)),
         _cli_result({"backups": [], "total": 0}),
         _error("invalid_source"), _error("invalid_source"), _error("invalid_source")],
        defects=["CLI signed integer accepted", "leading-zero decimal rejected", "backup path escapes root"]))
    return cases


def registry_manifest() -> dict[str, Any]:
    cases = acceptance_cases()
    return {"protocol": PROTOCOL, "purpose": PURPOSE, "status": "unqualified_definition",
            "contract_sha256": CONTRACT_SHA256, "case_ids": [c["id"] for c in cases],
            "ordered_inputs_sha256": _sha([{"id": c["id"], "input": c["input"]} for c in cases]),
            "ordered_expected_sha256": _sha([{"id": c["id"], "expected": c["expected"]} for c in cases]),
            "ordered_cases_sha256": _sha(cases), "adapter_sha256": hashlib.sha256(CHILD_ADAPTER.encode()).hexdigest(),
            "requirement_cases": {key: [c["id"] for c in cases if key in c["requirement_ids"]] for key in REQUIREMENT_IDS},
            "limitations": list(LIMITATIONS)}


CHILD_ADAPTER = r'''import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile

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

def identities(source, text):
    raw = source.encode("utf-8")
    return ("doc-" + hashlib.sha256(b"document\0" + raw).hexdigest(),
            "src-" + hashlib.sha256(b"source\0" + raw).hexdigest(),
            "blob-" + hashlib.sha256(text.encode("utf-8")).hexdigest())

def seed_database(path, fixture):
    con = sqlite3.connect(path)
    con.executescript("""
        CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        INSERT INTO metadata VALUES('schema','0');
        CREATE TABLE blobs(blob_id TEXT PRIMARY KEY,content BLOB NOT NULL);
        CREATE TABLE documents(document_id TEXT PRIMARY KEY,source_id TEXT UNIQUE NOT NULL,
          source TEXT UNIQUE NOT NULL,blob_id TEXT NOT NULL,title TEXT NOT NULL);
        CREATE TABLE jobs(job_id TEXT PRIMARY KEY,epoch INTEGER NOT NULL,state TEXT NOT NULL,
          total INTEGER NOT NULL,completed INTEGER NOT NULL,error TEXT,manifest TEXT NOT NULL,
          content_hashes TEXT NOT NULL,receipt TEXT);
    """)
    for source, text in fixture["entries"]:
        docid, srcid, blob = identities(source, text)
        con.execute("INSERT OR IGNORE INTO blobs VALUES (?,?)", (blob, text.encode()))
        con.execute("INSERT INTO documents VALUES (?,?,?,?,?)", (docid,srcid,source,blob,source.rsplit('/',1)[-1]))
    for row in fixture["jobs"]:
        job = row["job"]
        con.execute("INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?,?)",
                    tuple(job[k] for k in ("job_id","epoch","state","total","completed","error")) +
                    (row["manifest_json"], row["content_hashes_json"], row["receipt_json"]))
    con.commit(); con.close()

def confined(root, value):
    path = Path(value)
    if path.is_absolute() or not value or any(part in (".", "..", "") for part in value.split("/")):
        raise ValueError("Unsafe authored fixture path")
    return root / path

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

def cli_data(db, root, backups, arguments, json_data=True):
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
    if not json_data:
        return {"exit": process.returncode, "stdout": raw.decode("utf-8"), "stderr": error.decode("utf-8")}
    return {"exit": process.returncode,
            "value": strict_json(raw if process.returncode == 0 else error),
            "other_stream_empty": not (error if process.returncode == 0 else raw)}

def invoke(action, store, root, backups):
    target = action["target"]
    if target == "import":
        function = lambda source: import_file(store, root, source)
    else:
        receiver = store if target == "store" else (Service(store, root, backup_dir=backups) if target == "service" else JobManager(store))
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

with tempfile.TemporaryDirectory() as directory:
    base = Path(directory); root = base / "input"; root.mkdir()
    backups = base / "backups"; backups.mkdir(); db = base / "library.sqlite"
    seed_database(db, payload["fixture"])
    for file in payload["files"]:
        path = confined(root, file["path"]); path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(file["text"], encoding="utf-8")
    for backup in payload["backups"]:
        confined(backups, backup["name"]).write_bytes(encode(backup["value"], ascii=False))
    store = Store(db); observations = []
    try:
        for action in payload["actions"]:
            op = action["op"]
            if op == "call":
                result = invoke(action, store, root, backups)
            elif op == "reopen":
                store.close(); store = Store(db); result = {"reopened": True}
            elif op == "separate":
                separate = Store(db)
                try:
                    result = invoke(action["call"], separate, root, backups)
                finally:
                    separate.close()
            elif op == "read_backup":
                with confined(backups, action["name"]).open("rb") as stream:
                    data = stream.read(65537)
                if len(data) > 65536:
                    raise ValueError("Authored backup observation bound")
                result = strict_json(data)
            elif op == "backup_listing":
                value = Service(store, root, backup_dir=backups).list_backups(action["offset"], action["limit"])
                result = {"backups": [{key: item[key] for key in ("name", "payload_sha256", "generation")}
                                      for item in value["backups"]], "total": value["total"]}
            elif op == "job_serialization":
                connection = sqlite3.connect(db)
                try:
                    row = connection.execute("SELECT manifest,content_hashes,receipt FROM jobs WHERE job_id=?", (action["job_id"],)).fetchone()
                    result = dict(zip(("manifest_json", "content_hashes_json", "receipt_json"), row))
                finally:
                    connection.close()
            elif op == "cli":
                result = cli_data(db, root, backups, action["args"], action["json_data"])
            else:
                raise ValueError("Unknown observation action")
            result = project_result(result, action)
            if action.get("observe", True):
                observations.append(result)
    finally:
        store.close()
    output = encode({"observations": observations})
    if len(output) > 65536:
        raise ValueError("Case observation bound")
    sys.stdout.buffer.write(output + b"\n")
'''
