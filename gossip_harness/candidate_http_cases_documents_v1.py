"""Literal prospective document, empty-state, error and query HTTP histories.

The 51 row identities retain their order in the reviewed 276-row catalog. Every
case starts from an independent empty database and constructs its own public
state. Expected values come from immutable source fixtures, never observations.
Construction performs no candidate requests and grants no execution authority.
"""
from __future__ import annotations

from urllib.parse import quote

from gossip_harness import candidate_http_fixtures_v1 as f
from gossip_harness import candidate_http_semantics_v1 as s
from gossip_harness.candidate_http_cases_core_v1 import Builder, LiteralCase, encode


_EMPTY_TARGETS = ("V0-HTTP-01", "V0-HTTP-02", "V0-HTTP-04", "M1-HTTP-01", "M1-HTTP-02")
_DOCUMENT_TARGETS = ("V0-HTTP-01", "V0-HTTP-02")
_ERROR_TARGETS = ("V0-HTTP-02", "M1-HTTP-02")


def _empty_cases() -> tuple[LiteralCase, ...]:
    empty = f.ExpectedState()
    rows = (
        ("documents", "/api/documents", s.success("documents", encode(empty.listing()))),
        ("export", "/api/export", s.success("export", encode(empty.export()))),
        ("jobs", "/api/jobs", s.success("jobs", encode(empty.jobs_json()))),
        ("health", "/health", s.success("health")),
        ("missing-documents", "/api/documents/missing", s.classified_error("not_found")),
        ("missing-jobs", "/api/jobs/missing", s.classified_error("not_found")),
    )
    cases = []
    for name, target, expectation in rows:
        builder = Builder(f"HTTP-EMPTY-HEALTH/{name}", _EMPTY_TARGETS)
        builder.start()
        builder.request("GET", target, expectation, label="subject")
        cases.append(builder.finish())
    return tuple(cases)


def _corpus_builder(row_name: str, *, family: str = "HTTP-DOCUMENT-ROUTES") -> Builder:
    builder = Builder(f"{family}/{row_name}", _DOCUMENT_TARGETS)
    builder.start()
    # Exactly four documents and zero jobs. A guard would change this family's
    # prospectively prescribed bodies, result totals and export selections.
    builder.corpus()
    return builder


def _query_target(query: str, offset: int = 0, limit: int = 100) -> str:
    return f"/api/documents?q={quote(query, safe='')}&offset={offset}&limit={limit}"


def _document_cases() -> tuple[LiteralCase, ...]:
    cases = []
    for index, entry in enumerate(f.DOCUMENT_CORPUS):
        builder = Builder(f"HTTP-DOCUMENT-ROUTES/import-{index}", _DOCUMENT_TARGETS)
        for staged in f.DOCUMENT_CORPUS:
            builder.add_file(staged.source, staged.text.encode("utf-8"))
        builder.start()
        after = f.import_document(builder.state, entry.source, entry.text).state
        builder.post("/api/import", {"source": entry.source}, s.success("unspecified"),
                     label="subject", after=after)
        cases.append(builder.finish())

    builder = _corpus_builder("unchanged-import")
    builder.post("/api/import", {"source": "Alpha.txt"}, s.success("unspecified"), label="subject")
    builder.note("Public catalog equality does not prove physical blob conservation or an exact import wrapper.")
    cases.append(builder.finish())

    queries = (
        ("all", "", 0, 100),
        ("none", "absent-needle", 0, 100),
        ("one", "CAFÉ", 0, 100),
        ("casefold", "STRASSE", 0, 100),
        ("literal", "[a.b]", 0, 100),
        ("unicode-casefold", "café", 0, 100),
        ("offset-one", "", 1, 100),
        ("limit-one", "", 0, 1),
        ("offset-beyond", "", 9, 100),
        ("offset-positive-limit", "", 1, 1),
        ("query-256", "é" * 256, 0, 100),
    )
    for name, query, offset, limit in queries:
        builder = _corpus_builder(name)
        expected = builder.state.listing(query=query, offset=offset, limit=limit)
        builder.request("GET", _query_target(query, offset, limit),
                        s.success("documents", encode(expected)), label="subject")
        cases.append(builder.finish())

    for index, entry in enumerate(f.DOCUMENT_CORPUS):
        builder = _corpus_builder(f"show-{index}")
        document = f.document(entry.source, entry.text)
        builder.request("GET", f"/api/documents/{document.document_id}",
                        s.success("document", encode(document.as_json())), label="subject")
        cases.append(builder.finish())

    builder = _corpus_builder("export-all")
    builder.request("GET", "/api/export", s.success("export", encode(builder.state.export())), label="subject")
    cases.append(builder.finish())

    documents = f.corpus_state().documents
    selections = (
        ("export-empty", ()),
        ("export-reverse", (documents[3].document_id, documents[0].document_id)),
        ("export-one", (documents[2].document_id,)),
    )
    for name, ids in selections:
        builder = _corpus_builder(name)
        builder.post("/api/export", {"ids": list(ids)},
                     s.success("export", encode(builder.state.export(ids))), label="subject")
        cases.append(builder.finish())

    invalid_selections = (
        ("export-duplicate", (documents[0].document_id, documents[0].document_id)),
        ("export-missing-after-valid", (documents[0].document_id, "doc-missing")),
    )
    for name, ids in invalid_selections:
        builder = _corpus_builder(name)
        builder.post("/api/export", {"ids": list(ids)}, s.unclassified_error(), label="subject")
        builder.note("Selected export must reject without partial success; exact error code and fixed status are unspecified.")
        cases.append(builder.finish())
    return tuple(cases)


def _error_builder(name: str) -> Builder:
    builder = Builder(f"HTTP-ERROR-STATUS/{name}", _ERROR_TARGETS)
    builder.start()
    builder.guard()
    return builder


def _completed_subject(builder: Builder) -> None:
    builder.submit_job("subject", f.VALID_MANIFEST, label="setup-submit")
    builder.action("prepare", "subject", label="setup-prepare")
    builder.action("commit", "subject", epoch=1, label="setup-commit")


def _error_cases() -> tuple[LiteralCase, ...]:
    cases = []
    for name, target in (("missing-document", "/api/documents/missing"),
                         ("missing-job", "/api/jobs/missing")):
        builder = _error_builder(name)
        builder.request("GET", target, s.classified_error("not_found"), label="subject")
        cases.append(builder.finish())

    builder = _error_builder("source-changed")
    # No file rewrite: the original catalog value is created through the job
    # interface while the independently staged input always contains "second".
    builder.add_file("same.txt", b"second")
    builder.submit_job("subject", (f.Entry("same.txt", "first"),), label="setup-submit")
    builder.action("prepare", "subject", label="setup-prepare")
    builder.action("commit", "subject", epoch=1, label="setup-commit")
    builder.post("/api/import", {"source": "same.txt"}, s.classified_error("source_changed"), label="subject")
    cases.append(builder.finish())

    builder = _error_builder("job-conflict")
    builder.submit_job("subject", f.VALID_MANIFEST, label="setup-submit")
    builder.post("/api/jobs", {"job_id": "subject", "entries": [
        {"source": "a.txt", "text": "different"}, {"source": "b.txt", "text": "b"},
    ]}, s.classified_error("job_conflict"), label="subject")
    cases.append(builder.finish())

    builder = _error_builder("job-state")
    _completed_subject(builder)
    builder.post("/api/jobs/subject/prepare", {}, s.classified_error("job_state"), label="subject")
    cases.append(builder.finish())

    builder = _error_builder("stale-epoch")
    _completed_subject(builder)
    builder.post("/api/jobs/subject/commit", {"epoch": 2}, s.classified_error("stale_epoch"), label="subject")
    cases.append(builder.finish())

    builder = _error_builder("invalid-request")
    builder.post("/api/import", {}, s.classified_error("invalid_request"), label="subject")
    cases.append(builder.finish())

    builder = _error_builder("unknown-route")
    builder.request("GET", "/api/does-not-exist", s.unclassified_error(404), label="subject")
    builder.note("Unsupported route fixes 404 without assigning not_found or enforcing the generic code/status relation.")
    cases.append(builder.finish())

    for form, path in (("directory", "definitely-absent"), ("zip", "definitely-absent.zip"),
                       ("json", "definitely-absent.json")):
        builder = _error_builder(f"io-error-{form}")
        body = {"job_id": "subject", form: path}
        if form != "json":
            body["namespace"] = "ns"
        builder.post("/api/jobs", body, s.classified_error("io_error"), label="subject")
        cases.append(builder.finish())
    return tuple(cases)


def _query_error_cases() -> tuple[LiteralCase, ...]:
    cases = []
    values = (("offset", "abc"), ("limit", "abc"), ("offset", "-1"),
              ("limit", "0"), ("limit", "101"), ("q", "é" * 257),
              ("offset", "true"), ("limit", "1.5"))
    for index, (field, value) in enumerate(values):
        builder = _corpus_builder(f"{index}-{field}", family="HTTP-QUERY-VALUES")
        target = f"/api/documents?{field}={quote(value, safe='')}"
        builder.request("GET", target, s.classified_error("invalid_request"), label="subject")
        cases.append(builder.finish())
    return tuple(cases)


def definitions() -> tuple[LiteralCase, ...]:
    """Materialize these four catalog families without dispatching any work."""
    return _empty_cases() + _document_cases() + _error_cases() + _query_error_cases()
