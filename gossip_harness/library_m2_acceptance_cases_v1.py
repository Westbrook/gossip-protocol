"""Independently authored M2 observations and host-only expected values.

This registry was authored from the cumulative normative contract, without
reading the M2 implementation. It is a development qualification instrument,
not held-out statistical evidence or whole-project acceptance. The generic
child imports real application APIs only inside a caller-owned sandbox; no
candidate import occurs on the host. Inputs contain fixture recipes and actions,
never expected answers. A trusted caller must bind immutable source, evaluator,
resource limits and the complete cohort freeze before independent acceptance.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from typing import Any

PROTOCOL = "library-m2-independent-cases-v1"
PURPOSE = "independent_acceptance"
CONTRACT_SHA256 = "3353dbaa0c2474527cbffb0d87a4091b5f77ed3f98418fff2f216d90b6a8487c"
REQUIREMENT_IDS = (
    "M2-IDENTITY-REVISIONS", "M2-REFRESH", "M2-ANNOTATIONS", "M2-COLLECTIONS",
    "M2-DELETE-RESTORE", "M2-QUERY", "M2-INTERFACES",
)
# Explicitly incomplete: mapping a requirement to a case is not proof that every
# clause, frontend or adversarial timing of that requirement has been checked.
LIMITATIONS = (
    "No browser observations or actual HTTP transport/content-type/body bounds.",
    "Service.request observations cover routing, not server wire parsing or CLI.",
    "No process kill, migration power-loss, malicious candidate or cohort barrier qualification.",
    "Races overlap calls from two real SQLite connections; no forced internal transaction schedule.",
    "Boundary snapshots use only declared portable schema0/schema2 tables; they do not prove every construction path.",
    "No complete source/path/error Cartesian product, and no oracle-specified precedence among unrelated malformed fields.",
)


def _bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def _sha(value: Any) -> str:
    return hashlib.sha256(_bytes(value)).hexdigest()


def _document(source: str, text: str) -> dict[str, Any]:
    return {"document_id": "doc-" + hashlib.sha256(b"document\0" + source.encode()).hexdigest(),
            "source_id": "src-" + hashlib.sha256(b"source\0" + source.encode()).hexdigest(),
            "source": source, "title": source.rsplit("/", 1)[-1],
            "blob_id": "blob-" + hashlib.sha256(text.encode()).hexdigest(), "text": text}


def _record(source: str, text: str, revision: int = 1, version: int = 1,
            deleted: bool = False, notes: str = "", tags: list[str] | None = None,
            collections: list[str] | None = None) -> dict[str, Any]:
    return {"document": _document(source, text), "revision": revision,
            "edit_version": version, "deleted": deleted, "notes": notes,
            "tags": tags or [], "collections": collections or []}


def _id(source: str) -> str:
    return str(_document(source, "")["document_id"])


def _error(code: str) -> dict[str, str]:
    return {"error": code}


def _call(method: str, *args: Any, target: str = "service", observe: bool = True,
          **kwargs: Any) -> dict[str, Any]:
    return {"op": "call", "target": target, "method": method, "args": list(args),
            "kwargs": kwargs, "observe": observe}


def _digest(action: dict[str, Any]) -> dict[str, Any]:
    return action | {"digest": True}


def _list(records: list[dict[str, Any]], generation: int, total: int | None = None) -> dict[str, Any]:
    return {"records": records, "total": len(records) if total is None else total,
            "generation": generation}


def _history(source: str, texts: list[str]) -> dict[str, Any]:
    return {"document_id": _id(source), "revisions": [
        {"revision": n, "blob_id": _document(source, text)["blob_id"], "text": text}
        for n, text in enumerate(texts, 1)]}


def _case(identifier: str, requirements: tuple[str, ...], actions: list[dict[str, Any]],
          expected: list[Any], *, entries: list[tuple[str, str]] | None = None,
          fixture: dict[str, Any] | None = None, files: list[dict[str, Any]] | None = None,
          defect_probes: tuple[str, ...] = ()) -> dict[str, Any]:
    return {"id": identifier, "requirement_ids": list(requirements),
            "input": {"fixture": fixture or {"kind": "schema0", "entries": [list(entry) for entry in entries or []]},
                      "files": files or [], "actions": actions},
            "expected": {"observations": expected}, "targeted_defects": list(defect_probes)}


def acceptance_cases() -> list[dict[str, Any]]:
    """Fresh deterministic scenarios; expected outputs never enter child inputs."""
    cases: list[dict[str, Any]] = []
    source, original = "archive/Ä.md", "first\n"
    docid = _id(source)
    first = _record(source, original)
    second = _record(source, "second", 2, 2)
    again = _record(source, original, 3, 3)
    deleted = _record(source, original, 3, 4, True)
    restored = _record(source, original, 3, 5)
    completed = {"job_id": "old_job", "epoch": 1, "state": "completed", "total": 1,
                 "completed": 1, "error": None}
    receipt = {"job": completed, "documents": [_document(source, original)]}
    manifest_text = json.dumps([{"source": source, "text": original}], ensure_ascii=True,
                               sort_keys=True, separators=(",", ":"))
    content_text = json.dumps([hashlib.sha256(original.encode()).hexdigest()], separators=(",", ":"))
    receipt_text = json.dumps(receipt, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    cases.append(_case("m2-migrate-aba-receipt", REQUIREMENT_IDS[:2] + REQUIREMENT_IDS[4:6], [
        _call("lifecycle_list"), _call("lifecycle_show", docid),
        _call("refresh_document", docid, 1, text="second"),
        _call("refresh_document", docid, 2, text=original),
        _call("refresh_document", docid, 1, text="stale"),
        _call("refresh_document", docid, 3, text=original),
        _call("delete_document", docid, 3),
        _call("import", source, target="import"),
        _call("commit", {"job_id": "old_job", "epoch": 1}, target="jobs"),
        _call("listing"), _call("show", docid, target="store"),
        _call("export", [docid]), _call("revision_history", docid),
        {"op": "reopen"}, _call("lifecycle_show", docid),
        _call("restore_document", docid, 4), _call("restore_document", docid, 5),
        _call("delete_document", docid, 4), _call("lifecycle_list"),
        {"op": "job_serialization", "job_id": "old_job"},
    ], [_list([first], 0), first, {"status": "refreshed", "record": second},
        {"status": "refreshed", "record": again}, _error("stale_version"),
        {"status": "unchanged", "record": again}, {"status": "deleted", "record": deleted},
        {"status": "unchanged", "document": _document(source, original)}, receipt,
        {"documents": [], "total": 0}, _error("not_found"), _error("not_found"),
        _history(source, [original, "second", original]), {"reopened": True}, deleted,
        {"status": "restored", "record": restored}, {"status": "unchanged", "record": restored},
        _error("stale_version"), _list([restored], 4),
        {"manifest": manifest_text, "content_hashes": content_text, "receipt": receipt_text}],
        fixture={"kind": "schema0", "entries": [[source, original]],
                 "jobs": [{"job": completed, "manifest": manifest_text,
                           "content_hashes": content_text, "receipt": receipt_text}]},
        files=[{"path": source, "text": original}],
        defect_probes=("content-derived identity", "content-only ABA token", "mutable completed receipt",
                       "import resurrects tombstone", "migration rewrites job serialization")))

    source = "literal.html"; docid = _id(source)
    literal = "<b>body</b>"
    changed = _record(source, literal, version=2, notes="<script>notes</script>",
                      tags=["strasse", "é"], collections=["équipe"])
    tombstone = deepcopy(changed); tombstone.update(edit_version=3, deleted=True)
    restored = deepcopy(changed); restored.update(edit_version=4)
    clear = _record(source, literal, version=5)
    cases.append(_case("m2-normalized-membership-tombstone", REQUIREMENT_IDS[2:5] + ("M2-QUERY",), [
        _call("create_collection", " E\u0301QUIPE ", 0),
        _call("create_collection", "équipe", 1),
        _call("replace_annotations", docid, 1, "<script>notes</script>", [" STRAßE ", "e\u0301"], ["ÉQUIPE"]),
        _call("replace_annotations", docid, 2, "<script>notes</script>", ["É", "strasse"], ["équipe"]),
        _call("replace_annotations", docid, 2, "bad", ["SS", "ß"], []),
        _call("replace_annotations", docid, 1, "", [], ["missing"]),
        _call("replace_annotations", docid, 2, "", [], ["missing"]),
        _call("listing", "notes"), _call("listing", "STRASSE"),
        _call("lifecycle_list", tag="É", collection=" ÉQUIPE "),
        _call("delete_document", docid, 2), _call("list_collections"),
        _call("remove_collection", "équipe", 3),
        _call("replace_annotations", docid, 3, "", [], ["missing"]),
        _call("replace_annotations", docid, 2, "", [], ["missing"]),
        _call("lifecycle_list", deleted="deleted"),
        _call("restore_document", docid, 3),
        _call("replace_annotations", docid, 4, "", [], []),
        _call("remove_collection", "ÉQUIPE", 5), _call("remove_collection", "absent", 5),
        _call("remove_collection", "absent", 6), _call("list_collections"),
        _call("revision_history", docid),
    ], [{"status": "created", "name": "équipe", "generation": 1},
        {"status": "unchanged", "name": "équipe", "generation": 1},
        {"status": "updated", "record": changed}, {"status": "unchanged", "record": changed},
        _error("invalid_request"), _error("stale_version"), _error("collection_not_found"),
        {"documents": [], "total": 0}, {"documents": [], "total": 0}, _list([changed], 2),
        {"status": "deleted", "record": tombstone},
        {"collections": [{"name": "équipe", "total": 1}], "generation": 3},
        _error("collection_not_empty"), _error("document_deleted"), _error("stale_version"),
        _list([tombstone], 3), {"status": "restored", "record": restored},
        {"status": "updated", "record": clear}, {"status": "removed", "name": "équipe", "generation": 6},
        _error("stale_generation"), _error("not_found"), {"collections": [], "generation": 6},
        _history(source, [literal])], entries=[(source, literal)],
        defect_probes=("normalization before casefold only", "annotation write is partial", "notes enter legacy search",
                       "tombstone releases membership", "lookup precedes generation/token fence")))

    entries = [("z.txt", "old phrase"), ("A.txt", "Straße"), ("ä.md", "tail")]
    records = [_record(s, t) for s, t in sorted(entries)]
    znew = _record("z.txt", "new phrase", 2, 2)
    zannotated = deepcopy(znew); zannotated.update(edit_version=3, notes="old phrase")
    cases.append(_case("m2-query-generation-pages", ("M2-QUERY", "M2-REFRESH", "M2-IDENTITY-REVISIONS"), [
        _call("lifecycle_list", limit=1), _call("lifecycle_list", offset=1, limit=1, generation=0),
        _call("lifecycle_list", "STRASSE"), _call("lifecycle_list", tag="absent"),
        _call("lifecycle_list", collection="absent"),
        _call("refresh_document", _id("z.txt"), 1, text="new phrase"),
        _call("lifecycle_list", offset=2, limit=1, generation=0),
        _call("replace_annotations", _id("z.txt"), 2, "old phrase", [], []),
        _call("listing", "old phrase"), _call("lifecycle_list", "old phrase"),
        _call("lifecycle_list", offset=1, limit=1, generation=2),
        _call("lifecycle_list", offset=100), _call("lifecycle_list", generation=True),
        _call("lifecycle_list", offset=True), _call("lifecycle_list", limit=0),
        _call("lifecycle_list", "x" * 257), _call("lifecycle_list", deleted="unknown"),
        {"op": "fresh", "call": _call("lifecycle_show", _id("z.txt")), "path": ["tags"], "value": ["ghost"]},
    ], [_list(records[:1], 0, 3), _list(records[1:2], 0, 3), _list(records[:1], 0),
        _list([], 0), _list([], 0), {"status": "refreshed", "record": znew},
        _error("stale_generation"), {"status": "updated", "record": zannotated},
        {"documents": [], "total": 0}, _list([], 2), _list([zannotated], 2, 3),
        _list([], 2, 3), *[_error("invalid_request") for _ in range(5)], zannotated], entries=entries,
        defect_probes=("total after slice", "lexical order uses locale", "generation ignores annotations",
                       "revision history enters search", "mutable shared return values")))

    source = "concurrent.txt"; docid = _id(source)
    changed = _record(source, "original", version=2, notes="winner")
    race_call = _call("replace_annotations", docid, 1, "winner", [], [])
    race_expected = sorted([_error("stale_version"), {"status": "updated", "record": changed}], key=_bytes)
    cases.append(_case("m2-competing-connections", ("M2-IDENTITY-REVISIONS", "M2-ANNOTATIONS", "M2-QUERY"), [
        {"op": "race", "call": race_call}, _call("lifecycle_list"), {"op": "reopen"},
        _call("lifecycle_show", docid),
    ], [race_expected, _list([changed], 1), {"reopened": True}, changed], entries=[(source, "original")],
        defect_probes=("version check outside transaction", "last writer wins", "double generation increment")))

    source = "versions.txt"; docid = _id(source)
    actions = [_call("refresh_document", docid, n, text=str(n + 1), observe=False) for n in range(1, 16)]
    full = _record(source, "16", 16, 16)
    actions.extend([_call("lifecycle_show", docid), _call("refresh_document", docid, 16, text="17"),
                    _call("refresh_document", docid, 16, text="16"), _call("revision_history", docid),
                    _call("lifecycle_list"), {"op": "reopen"}, _call("lifecycle_show", docid)])
    cases.append(_case("m2-revision-boundary", ("M2-IDENTITY-REVISIONS", "M2-REFRESH"), actions,
        [full, _error("revision_capacity"), {"status": "unchanged", "record": full},
         _history(source, [str(i) for i in range(1, 17)]), _list([full], 15), {"reopened": True}, full],
        entries=[(source, "1")], defect_probes=("revision pruning", "off-by-one revision cap", "noop consumes revision")))

    source = "cap/000.txt"; docid = _id(source)
    tomb = _record(source, "shared", version=2, deleted=True)
    cases.append(_case("m2-document-capacity-tombstones", ("M2-IDENTITY-REVISIONS", "M2-DELETE-RESTORE", "M2-QUERY"), [
        _call("delete_document", docid, 1), _call("import", "overflow.txt", target="import"),
        _call("lifecycle_list", deleted="all", limit=1), _call("listing", "", 0, 1),
        _call("lifecycle_show", docid),
    ], [{"status": "deleted", "record": tomb}, _error("capacity"), _list([tomb], 1, 256),
        {"documents": [_document("cap/001.txt", "shared")], "total": 255}, tomb],
        fixture={"kind": "document_capacity"}, files=[{"path": "overflow.txt", "text": "new"}],
        defect_probes=("deleted documents release capacity", "content dedup collapses source identity")))

    source = "blob/032.txt"; current_text = _blob_text(32, 1)
    current = _record(source, current_text)
    replacement = _blob_text(0, 1)
    replaced = _record(source, replacement, 2, 2)
    cases.append(_case("m2-retained-blob-capacity", ("M2-IDENTITY-REVISIONS", "M2-REFRESH"), [
        _digest(_call("refresh_document", _id(source), 1, text="one new byte")),
        _digest(_call("lifecycle_show", _id(source))),
        _digest(_call("refresh_document", _id(source), 1, text=replacement)),
        _digest(_call("revision_history", _id(source))),
    ], [{"sha256": _sha(value)} for value in [_error("capacity"), current,
         {"status": "refreshed", "record": replaced}, _history(source, [current_text, replacement])]], fixture={"kind": "blob_capacity"},
        defect_probes=("counts current blobs only", "double counts shared blobs", "failed capacity publishes head")))

    source = "original.md"; docid = _id(source)
    changed = _record(source, "<i>replacement</i>\n", 2, 2)
    files: list[dict[str, Any]] = [{"path": "other.html", "text": "<i>replacement</i>\n"},
             {"path": "invalid.txt", "hex": "ff"}, {"path": "large.txt", "repeat": ["x", 32769]},
             {"path": "unsupported.bin", "text": "a"}, {"path": "link.txt", "symlink": "other.html"}]
    actions = [_call("refresh_document", docid, 1, path="other.html")]
    errors = [("../x.txt", "invalid_source"), ("/x.txt", "invalid_source"),
              ("a\\b.txt", "invalid_source"), ("a//b.txt", "invalid_source"),
              ("/".join(["a"] * 16 + ["x.txt"]), "invalid_source"),
              ("unsupported.bin", "unsupported_type"), ("missing.txt", "io_error"),
              ("invalid.txt", "invalid_utf8"), ("large.txt", "too_large"), ("link.txt", "invalid_source")]
    actions.extend(_call("refresh_document", docid, 2, path=path) for path, _ in errors)
    actions += [_call("refresh_document", docid, 2), _call("refresh_document", docid, 2, text="x", path="other.html"),
                _call("refresh_document", docid, True, text="x"),
                _call("refresh_document", docid, 2, text={"$repeat": ["é", 16385]}),
                _call("refresh_document", docid, 2, text="\ud800"),
                _call("lifecycle_show", docid), _call("revision_history", docid)]
    cases.append(_case("m2-refresh-input-boundaries", ("M2-REFRESH", "M2-IDENTITY-REVISIONS"), actions,
        [{"status": "refreshed", "record": changed}, *[_error(code) for _, code in errors],
         _error("invalid_request"), _error("invalid_request"), _error("invalid_request"),
         _error("too_large"), _error("invalid_utf8"), changed, _history(source, ["start", "<i>replacement</i>\n"])],
        entries=[(source, "start")], files=files,
        defect_probes=("refresh path replaces logical source", "symlink follow", "character-count byte bound", "partial failed refresh")))

    source = "shape.txt"; docid = _id(source)
    invalid_annotations = [("", [""], []), ("", ["a\x00b"], []), ("", ["a\x7fb"], []),
        ("", ["é" * 33], []), ("", [str(i) for i in range(33)], []),
        ("", [], [str(i) for i in range(17)]), ("", "tag", []), ("", [], None)]
    actions = [_call("replace_annotations", docid, 1, *values) for values in invalid_annotations]
    actions += [_call("replace_annotations", docid, 1, {"$repeat": ["é", 8193]}, [], []),
                _call("replace_annotations", docid, 1, "\ud800", [], []),
                _call("lifecycle_show", docid), _call("lifecycle_list")]
    cases.append(_case("m2-annotation-input-boundaries", ("M2-ANNOTATIONS", "M2-QUERY"), actions,
        [*[_error("invalid_request") for _ in invalid_annotations], _error("too_large"),
         _error("invalid_utf8"), _record(source, "x"), _list([_record(source, "x")], 0)], entries=[(source, "x")],
        defect_probes=("partial annotation update", "byte/count off-by-one", "names allow control bytes")))

    actions = [_call("create_collection", f"c{i:02}", i, observe=False) for i in range(64)]
    actions += [_call("create_collection", "overflow", 64), _call("create_collection", "C00", 64),
                _call("remove_collection", "c63", 64), _call("create_collection", "new", 65),
                _call("list_collections")]
    cases.append(_case("m2-collection-boundary", ("M2-COLLECTIONS", "M2-QUERY"), actions,
        [_error("capacity"), {"status": "unchanged", "name": "c00", "generation": 64},
         {"status": "removed", "name": "c63", "generation": 65},
         {"status": "created", "name": "new", "generation": 66},
         {"collections": [{"name": f"c{i:02}", "total": 0} for i in range(63)] + [{"name": "new", "total": 0}], "generation": 66}],
        defect_probes=("collection capacity off-by-one", "noop fails at capacity", "generation increments on errors")))

    source = "route.txt"; docid = _id(source); base = "/api/lifecycle/documents/" + docid
    initial = _record(source, "body"); deleted = _record(source, "body", version=2, deleted=True)
    requests = [("GET", "/api/lifecycle/documents?limit=01", None),
        ("GET", base, None), ("GET", base + "/revisions", None),
        ("GET", "/api/lifecycle/documents?limit=1&limit=1", None),
        ("GET", "/api/lifecycle/documents?offset=%2B1", None),
        ("GET", "/api/lifecycle/documents?limit=١", None),
        ("GET", "/api/lifecycle/documents?unknown=x", None),
        ("POST", base + "/delete", {"expected_version": True}),
        ("POST", base + "/delete", {"expected_version": 1, "extra": 0}),
        ("POST", base + "/delete", {"expected_version": 1}),
        ("POST", base + "/refresh", {"expected_version": 1, "text": "x"}),
        ("POST", base + "/refresh", {"expected_version": 2, "text": "x"}),
        ("GET", "/api/documents/" + docid, None),
        ("POST", "/api/lifecycle/collections/remove", {"name": "absent", "expected_generation": 0}),
        ("GET", "/api/lifecycle/documents?generation=0", None),
        ("POST", base + "/restore", {"expected_version": 2}),
        ("POST", base + "/annotations", {"expected_version": 3, "notes": "", "tags": [], "collections": ["absent"]}),
        ("PUT", base, None)]
    route_values = [[200, _list([initial], 0)], [200, initial], [200, _history(source, ["body"])],
        *[[400, _error("invalid_request")] for _ in range(6)],
        [200, {"status": "deleted", "record": deleted}], [409, _error("stale_version")],
        [409, _error("document_deleted")], [404, _error("not_found")],
        [409, _error("stale_generation")], [409, _error("stale_generation")],
        [200, {"status": "restored", "record": _record(source, "body", version=3)}],
        [400, _error("collection_not_found")], [404, _error("not_found")]]
    cases.append(_case("m2-service-route-contract", ("M2-INTERFACES", "M2-DELETE-RESTORE", "M2-QUERY"),
        [_call("request", method, path, body) for method, path, body in requests], route_values,
        entries=[(source, "body")], defect_probes=("duplicate query accepted", "bool is integer", "wrong HTTP conflict mapping",
                                               "unknown fields accepted", "signed or Unicode decimal accepted")))

    # One batch is one catalog transaction; empty/replayed/job-only work is none.
    entry = {"source": "batch/a.txt", "text": "same"}
    entry2 = {"source": "batch/b.txt", "text": "same"}
    job = {"job_id": "pair", "epoch": 1, "state": "completed", "total": 2, "completed": 2, "error": None}
    pair_receipt = {"job": job, "documents": [_document(e["source"], e["text"]) for e in (entry, entry2)]}
    cases.append(_case("m2-batch-generation-replay", ("M2-QUERY", "M2-IDENTITY-REVISIONS", "M2-REFRESH"), [
        _call("submit", "pair", [entry2, entry], target="jobs", observe=False),
        _call("lifecycle_list"), _call("prepare", "pair", target="jobs", observe=False),
        _call("lifecycle_list"), _call("commit", {"job_id": "pair", "epoch": 1}, target="jobs"),
        _call("lifecycle_list"), _call("commit", {"job_id": "pair", "epoch": 1}, target="jobs"),
        _call("submit", "empty", [], target="jobs", observe=False),
        _call("prepare", "empty", target="jobs", observe=False),
        _call("commit", {"job_id": "empty", "epoch": 1}, target="jobs", observe=False),
        _call("lifecycle_list"),
    ], [_list([], 0), _list([], 0), pair_receipt,
        _list([_record(e["source"], e["text"]) for e in (entry, entry2)], 1), pair_receipt,
        _list([_record(e["source"], e["text"]) for e in (entry, entry2)], 1)],
        defect_probes=("generation per document rather than transaction", "job-only transition increments generation")))
    return cases


def _blob_text(document: int, revision: int) -> str:
    prefix = f"{document:03}:{revision:02}:"
    return prefix + "x" * (32768 - len(prefix))


def registry_manifest() -> dict[str, Any]:
    cases = acceptance_cases()
    return {"protocol": PROTOCOL, "purpose": PURPOSE, "status": "unqualified_definition",
            "contract_sha256": CONTRACT_SHA256, "case_ids": [c["id"] for c in cases],
            "ordered_inputs_sha256": _sha([{ "id": c["id"], "input": c["input"]} for c in cases]),
            "ordered_expected_sha256": _sha([{ "id": c["id"], "expected": c["expected"]} for c in cases]),
            "ordered_cases_sha256": _sha(cases), "adapter_sha256": hashlib.sha256(CHILD_ADAPTER.encode()).hexdigest(),
            "requirement_cases": {key: [c["id"] for c in cases if key in c["requirement_ids"]] for key in REQUIREMENT_IDS},
            "limitations": list(LIMITATIONS)}


# Fixed observer, not an oracle. It never computes a pass/fail status. The caller
# must execute it in a fresh resource-bounded sandbox for each independent case.
CHILD_ADAPTER = r'''import concurrent.futures
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading

WORKSPACE = sys.argv[1] if len(sys.argv) == 2 else "/workspace"
sys.path.insert(0, WORKSPACE)
payload = json.loads(sys.stdin.buffer.read(65537))
from library.catalog.store import Store
from library.common import LibraryError
from library.ingestion.jobs import JobManager
from library.ingestion.local import import_file
from library.query.service import Service

def encode(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(",", ":"), allow_nan=False)

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
    kind = fixture["kind"]
    entries = fixture.get("entries", [])
    if kind == "document_capacity":
        entries = [("cap/%03d.txt" % i, "shared") for i in range(256)]
    elif kind == "blob_capacity":
        # Reachable history: migrate 33 legacy documents at generation0, then
        # 31*15 +14 changed refreshes. Each refresh advances one document and
        # the catalog generation once; revisions and edit tokens equal below.
        con.executescript("""
            UPDATE metadata SET value='2' WHERE key='schema';
            INSERT INTO metadata VALUES('catalog_generation','479');
            CREATE TABLE lifecycle(document_id TEXT PRIMARY KEY REFERENCES documents(document_id),
              current_revision INTEGER NOT NULL,edit_version INTEGER NOT NULL,deleted INTEGER NOT NULL,
              notes TEXT NOT NULL,tags TEXT NOT NULL,collections TEXT NOT NULL);
            CREATE TABLE revisions(document_id TEXT NOT NULL REFERENCES documents(document_id),
              revision INTEGER NOT NULL,blob_id TEXT NOT NULL REFERENCES blobs(blob_id),
              PRIMARY KEY(document_id,revision));
            CREATE TABLE collections(name TEXT PRIMARY KEY);
        """)
        for d in range(33):
            count = 16 if d < 31 else (15 if d == 31 else 1)
            source = "blob/%03d.txt" % d
            docid, srcid, _ = identities(source, "")
            for r in range(1, count + 1):
                prefix = "%03d:%02d:" % (d, r)
                content = prefix + ("x" * (32768 - len(prefix)))
                _, _, blob = identities(source, content)
                con.execute("INSERT INTO blobs VALUES (?,?)", (blob, content.encode()))
                con.execute("INSERT INTO revisions VALUES (?,?,?)", (docid, r, blob))
            con.execute("INSERT INTO documents VALUES (?,?,?,?,?)", (docid, srcid, source, blob, source.rsplit('/',1)[-1]))
            con.execute("INSERT INTO lifecycle VALUES (?,?,?,?,?,?,?)", (docid,count,count,0,"","[]","[]"))
    elif kind != "schema0":
        raise ValueError("Unknown fixture")
    for source, content in entries:
        docid, srcid, blob = identities(source, content)
        con.execute("INSERT OR IGNORE INTO blobs VALUES (?,?)", (blob, content.encode()))
        con.execute("INSERT INTO documents VALUES (?,?,?,?,?)", (docid,srcid,source,blob,source.rsplit('/',1)[-1]))
    for row in fixture.get("jobs", []):
        job = row["job"]
        con.execute("INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?,?)",
                    tuple(job[k] for k in ("job_id","epoch","state","total","completed","error")) +
                    (row["manifest"],row["content_hashes"],row["receipt"]))
    con.commit(); con.close()

def confined(root, value):
    # This bounds authored fixture writes; candidate confinement is exercised by
    # the unchanged action arguments passed to the application's public APIs.
    path = Path(value)
    if path.is_absolute() or not value or any(x in (".","..","") for x in value.split("/")):
        raise ValueError("Unsafe fixture path")
    return root / path

def expand(value):
    if type(value) is dict and set(value) == {"$repeat"}:
        text, count = value["$repeat"]
        if type(text) is not str or type(count) is not int or not 0 <= count <= 32769:
            raise ValueError("Invalid bounded text recipe")
        return text * count
    if type(value) is dict:
        return {key: expand(item) for key, item in value.items()}
    if type(value) is list:
        return [expand(item) for item in value]
    return value

def invoke(action, store, root):
    target = action["target"]
    if target == "import":
        function = lambda source: import_file(store, root, source)
    else:
        receiver = store if target == "store" else (Service(store, root) if target == "service" else JobManager(store))
        function = getattr(receiver, action["method"])
    try:
        return function(*expand(action["args"]), **expand(action["kwargs"]))
    except LibraryError as error:
        return {"error": error.code}

with tempfile.TemporaryDirectory() as directory:
    base = Path(directory); root = base / "input"; root.mkdir(); db = base / "library.sqlite"
    seed_database(db, payload["fixture"])
    for file in payload["files"]:
        path = confined(root, file["path"]); path.parent.mkdir(parents=True, exist_ok=True)
        if "symlink" in file:
            path.symlink_to(file["symlink"])
        elif "hex" in file:
            path.write_bytes(bytes.fromhex(file["hex"]))
        elif "repeat" in file:
            path.write_text(file["repeat"][0] * file["repeat"][1], encoding="utf-8")
        else:
            path.write_text(file["text"], encoding="utf-8")
    store = Store(db)
    observations = []
    try:
        for action in payload["actions"]:
            op = action["op"]
            if op == "call":
                result = invoke(action, store, root)
            elif op == "reopen":
                store.close(); store = Store(db); result = {"reopened": True}
            elif op == "job_serialization":
                connection = sqlite3.connect(db)
                try:
                    row = connection.execute("SELECT manifest,content_hashes,receipt FROM jobs WHERE job_id=?",
                                             (action["job_id"],)).fetchone()
                    result = dict(zip(("manifest","content_hashes","receipt"),row))
                finally:
                    connection.close()
            elif op == "fresh":
                prior = invoke(action["call"], store, root); node = prior
                for component in action["path"][:-1]:
                    node = node[component]
                node[action["path"][-1]] = action["value"]
                result = invoke(action["call"], store, root)
            elif op == "race":
                barrier = threading.Barrier(2, timeout=5)
                def competitor(_):
                    own = Store(db)
                    try:
                        barrier.wait()
                        return invoke(action["call"], own, root)
                    finally:
                        own.close()
                with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                    result = sorted(pool.map(competitor, range(2)), key=encode)
            else:
                raise ValueError("Unknown observation action")
            if action.get("digest", False):
                result = {"sha256": hashlib.sha256(encode(result).encode("utf-8")).hexdigest()}
            if action.get("observe", True):
                observations.append(result)
    finally:
        store.close()
    print(encode({"observations": observations}))
'''
