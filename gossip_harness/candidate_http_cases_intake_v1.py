"""Literal prospective histories for the 27 HTTP-INTAKE-ROUTES rows.

These declarations derive requests and expected public state from the reviewed
V0/M1 contract and immutable evaluator fixtures. They perform no candidate
execution and establish no acceptance authority. Discovery failures and deferred
semantic failures deliberately have different admission histories.
"""
from __future__ import annotations

from . import candidate_http_fixtures_v1 as f
from . import candidate_http_semantics_v1 as s
from .candidate_http_cases_core_v1 import Builder, LiteralCase

FAMILY_ID = "HTTP-INTAKE-ROUTES"
REQUIREMENT_IDS = ("M1-HTTP-01", "M1-HTTP-02", "M1-I30", "M1-I31")
_SUCCESS = s.Expectation("unspecified", 200, None)


def _base(suffix: str, *, files: tuple[f.DataFile, ...] = (),
          directories: tuple[str, ...] = (), guarded: bool = True) -> Builder:
    builder = Builder(f"{FAMILY_ID}/{suffix}", REQUIREMENT_IDS)
    for item in files:
        builder.add_file(item.path, item.data)
    for path in directories:
        builder.add_directory(path)
    builder.start()
    if guarded:
        builder.guard()
    builder.note("Exact HTTP submit/action wrappers are unspecified; public jobs and documents "
                 "are checked independently. Public state does not prove hidden manifest, blob, "
                 "receipt, transaction or internal-delegation obligations.")
    return builder


def _body(kind: str, path: str) -> dict[str, object]:
    body: dict[str, object] = {"job_id": "subject", kind: path}
    if kind in ("directory", "zip"):
        body["namespace"] = "ns"
    return body


def _admit(builder: Builder, kind: str, manifest: tuple[f.Entry, ...],
           path: str = "") -> None:
    if kind == "entries":
        builder.submit_job("subject", manifest, label="submit-subject")
    else:
        after = f.submit(builder.state, "subject", manifest).state
        builder.post("/api/jobs", _body(kind, path), _SUCCESS,
                     after=after, label="submit-subject")


def _complete(builder: Builder) -> LiteralCase:
    # Every mutation has independently declared before/after public censuses.
    # Epoch one is fixed by this prospective history, never learned from a reply.
    builder.action("prepare", "subject", label="prepare-subject")
    builder.action("commit", "subject", epoch=1, label="commit-subject-epoch-one")
    return builder.finish()


def _ordinary(kind: str, *, empty: bool,
              files: tuple[f.DataFile, ...]) -> LiteralCase:
    suffix = f"{kind}-{'empty' if empty else 'nonempty'}"
    manifest = () if empty else f.CANONICAL_BATCH
    path = ""
    selected: tuple[f.DataFile, ...] = ()
    directories: tuple[str, ...] = ()
    if kind == "directory":
        path = "empty" if empty else "batch"
        if empty:
            directories = ("empty",)
        else:
            selected = tuple(item for item in files if item.path.startswith("batch/"))
    elif kind in ("zip", "json"):
        path = f"{'empty' if empty else 'batch'}.{kind}"
        selected = tuple(item for item in files if item.path == path)
    builder = _base(suffix, files=selected, directories=directories)
    _admit(builder, kind, manifest, path)
    return _complete(builder)


def _discovery_failure(suffix: str, kind: str, path: str, code: str,
                       files: tuple[f.DataFile, ...]) -> LiteralCase:
    selected = tuple(item for item in files
                     if item.path == path or (kind == "directory" and item.path.startswith(path + "/")))
    builder = _base(suffix, files=selected)
    builder.post("/api/jobs", _body(kind, path), s.Expectation("error", 400, None, code),
                 after=builder.state, label="discovery-rejects-before-admission")
    builder.note("The subject job must remain absent; rejection preserves the sentinel job "
                 "and the complete guarded document catalog.")
    return builder.finish()


def _deferred(kind: str, fault: str) -> LiteralCase:
    # The proposal uses one bad entry, not the two-entry FAILED_MANIFEST used by
    # the separate state/action family. Preserve this row's exact total of one.
    source = "../bad.txt" if fault == "traversal" else "bad.exe"
    manifest = (f.Entry(source, "bad"),)
    selected = (f.DataFile("deferred.json", f.json_bundle(manifest)),) if kind == "json" else ()
    builder = _base(f"{kind}-semantic-deferred-{fault}", files=selected)
    _admit(builder, kind, manifest, "deferred.json" if kind == "json" else "")
    builder.action("prepare", "subject", label="prepare-persists-semantic-failure")
    builder.note("Shape-valid admission is queued at epoch one with total one; prepare "
                 "then persists failed/completed zero without adding a document.")
    return builder.finish()


def _count(kind: str, count: int, files: tuple[f.DataFile, ...]) -> LiteralCase:
    path = f"count{count}" + ("" if kind == "directory" else f".{kind}")
    selected = tuple(item for item in files
                     if item.path == path or (kind == "directory" and item.path.startswith(path + "/")))
    builder = _base(f"{kind}-file-count-{count}", files=selected)
    if count == 64:
        _admit(builder, kind, f.member_count_manifest(64), path)
        return _complete(builder)
    builder.post("/api/jobs", _body(kind, path), s.Expectation("error", 400, None, "invalid_batch"),
                 after=builder.state, label="sixty-five-files-no-admission")
    builder.note("All 65 distinct one-byte .txt members are present in the fixture. "
                 "The file-count discovery error rejects before job admission, including JSON.")
    return builder.finish()


def definitions() -> tuple[LiteralCase, ...]:
    """Return every exact intake row in the reviewed catalog's stable order."""
    files = f.intake_files()
    cases: list[LiteralCase] = []
    for kind in ("entries", "directory", "zip", "json"):
        for empty in (False, True):
            cases.append(_ordinary(kind, empty=empty, files=files))

    lexical = _base("lexical-jobs-order", guarded=False)
    for job_id in ("b", "aa", "a"):
        lexical.submit_job(job_id, (), label=f"submit-lexical-{job_id}")
    lexical.census(label="lexical-order-a-aa-b")
    cases.append(lexical.finish())

    for suffix, kind, path, code in (
        ("zip-syntax", "zip", "invalid.zip", "invalid_archive"),
        ("json-syntax", "json", "invalid.json", "invalid_json"),
        ("json-schema", "json", "schema.json", "invalid_json"),
        ("zip-bad-utf8", "zip", "bad-utf8.zip", "invalid_utf8"),
        ("directory-bad-utf8", "directory", "bad-utf8", "invalid_utf8"),
        ("zip-duplicate-key", "zip", "duplicate.zip", "invalid_batch"),
        ("zip-symlink-member", "zip", "symlink.zip", "invalid_source"),
        ("zip-nonregular-member", "zip", "fifo.zip", "invalid_archive"),
    ):
        cases.append(_discovery_failure(suffix, kind, path, code, files))

    for kind in ("entries", "json"):
        for fault in ("traversal", "unsupported"):
            cases.append(_deferred(kind, fault))
    for kind in ("directory", "zip", "json"):
        for count in (64, 65):
            cases.append(_count(kind, count, files))
    return tuple(cases)
