"""Literal POST-shape and body-wire histories from the 276-row M1 catalog.

These independent declarations do not dispatch requests or grant execution or
acceptance authority. Exact wire-body bytes and setup state are authored before
observation; unknown parser and wrong-path-type error codes stay unspecified.
"""
from __future__ import annotations

from typing import Literal

from . import candidate_http_fixtures_v1 as fixtures
from . import candidate_http_semantics_v1 as semantics
from .candidate_http_cases_core_v1 import Builder, LiteralCase

SHAPES_FAMILY = "HTTP-POST-SHAPES"
WIRE_FAMILY = "HTTP-BODY-WIRE"
SHAPES_REQUIREMENTS = ("V0-HTTP-02", "M1-HTTP-01", "M1-I30")
WIRE_REQUIREMENTS = ("V0-HTTP-03",)
_SUCCESS = semantics.Expectation("unspecified", 200, None)


def _builder(family: str, name: str) -> Builder:
    interactions = ("M1-A08",) if name.startswith("offline-hook-") else ()
    requirements = SHAPES_REQUIREMENTS if family == SHAPES_FAMILY else WIRE_REQUIREMENTS
    builder = Builder(f"{family}/{name}", requirements, interactions)
    # Every selectable source is valid independently, isolating shape faults.
    for entry in (fixtures.GUARD,) + fixtures.DOCUMENT_CORPUS:
        builder.add_file(entry.source, entry.text.encode("utf-8"))
    builder.add_file("batch.zip", fixtures.canonical_zip())
    builder.add_file("batch.json", fixtures.json_bundle(fixtures.CANONICAL_BATCH))
    for directory in ("batch", "batch.directory"):
        for entry in fixtures.CANONICAL_BATCH:
            builder.add_file(directory + "/" + entry.source.removeprefix("ns/"),
                             entry.text.encode("utf-8"))
    builder.start()
    builder.guard()
    return builder


def _shape(name: str, payload: dict[str, object], *, target: str = "/api/jobs",
           initial: Literal["queued", "running", "cancelled"] | None = None,
           code: str | None = "invalid_request", admitted_job: str | None = None) -> LiteralCase:
    builder = _builder(SHAPES_FAMILY, name)
    if initial is not None:
        builder.submit_job("shape", fixtures.VALID_MANIFEST, label="setup-shape")
        if initial == "running":
            builder.action("prepare", "shape", label="setup-running")
        elif initial == "cancelled":
            builder.action("cancel", "shape", label="setup-cancelled")
    if admitted_job is None:
        builder.post(target, payload, semantics.Expectation("error", 400, None, code),
                     label="subject-request", after=builder.state)
    else:
        expected = fixtures.submit(builder.state, admitted_job, ()).state
        builder.post(target, payload, _SUCCESS, label="subject-request", after=expected)
    if name.startswith("offline-hook-"):
        builder.note("M1-A08 interaction covers HTTP hook nonexposure only; it does not prove rollback or atomicity.")
    return builder.finish()


def _shape_cases() -> tuple[LiteralCase, ...]:
    cases = [
        _shape("import-missing-source", {}, target="/api/import"),
        _shape("import-extra", {"source": "Alpha.txt", "extra": 1}, target="/api/import"),
        _shape("export-missing-ids", {}, target="/api/export"),
        _shape("export-extra", {"ids": [], "extra": 1}, target="/api/export"),
    ]
    forms: tuple[tuple[str, dict[str, object]], ...] = (
        ("entries", {"job_id": "shape", "entries": []}),
        ("directory", {"job_id": "shape", "directory": "batch", "namespace": "ns"}),
        ("zip", {"job_id": "shape", "zip": "batch.zip", "namespace": "ns"}),
        ("json", {"job_id": "shape", "json": "batch.json"}),
    )
    for form, body in forms:
        for key in body:
            cases.append(_shape(f"submit-{form}-missing-{key}",
                                {name: value for name, value in body.items() if name != key}))
        cases.append(_shape(f"submit-{form}-extra", {**body, "extra": 1}))
    cases.extend((
        _shape("prepare-extra", {"extra": 1}, target="/api/jobs/shape/prepare", initial="queued"),
        _shape("commit-missing-epoch", {}, target="/api/jobs/shape/commit", initial="running"),
        _shape("commit-extra", {"epoch": 1, "extra": 1},
               target="/api/jobs/shape/commit", initial="running"),
        _shape("cancel-extra", {"extra": 1}, target="/api/jobs/shape/cancel", initial="queued"),
        _shape("retry-extra", {"extra": 1}, target="/api/jobs/shape/retry", initial="cancelled"),
    ))
    # Literal paths intentionally retain the proposal's batch.directory spelling.
    selectors: tuple[tuple[str, object], ...] = (
        ("entries", []), ("directory", "batch.directory"), ("zip", "batch.zip"), ("json", "batch.json"),
    )
    for index, (first, first_value) in enumerate(selectors):
        for second, second_value in selectors[index + 1:]:
            body = {"job_id": "shape", first: first_value, second: second_value}
            if first in ("directory", "zip") or second in ("directory", "zip"):
                body["namespace"] = "ns"
            cases.append(_shape(f"multiple-{first}-{second}", body))
    cases.extend((
        _shape("no-selector", {"job_id": "shape"}),
        _shape("json-namespace", {"job_id": "shape", "json": "batch.json", "namespace": "ns"}),
    ))
    invalid_ids: tuple[tuple[str, object], ...] = (
        ("empty", ""), ("65-ascii", "a" * 65), ("non-ascii", "é"), ("space", "a b"),
        ("slash", "a/b"), ("boolean", True), ("null", None), ("number", 3),
    )
    for name, job_id in invalid_ids:
        cases.append(_shape(f"job-id-{name}", {"job_id": job_id, "entries": []}))
    for name, job_id in (("one", "a"), ("64-ascii", "a" * 64), ("punctuation", "A_a-9")):
        cases.append(_shape(f"job-id-valid-{name}", {"job_id": job_id, "entries": []}, admitted_job=job_id))
    invalid_entries: tuple[tuple[str, object], ...] = (
        ("object", {}), ("null", None), ("string", "bad"), ("number", 1),
        ("entry-nonobject", [3]), ("entry-missing-text", [{"source": "a.txt"}]),
    )
    for name, entries in invalid_entries:
        cases.append(_shape(f"entries-shape-{name}", {"job_id": "shape", "entries": entries}))
    for selector in ("directory", "zip", "json"):
        for name, value in (("null", None), ("number", 3)):
            body = {"job_id": "shape", selector: value}
            if selector != "json":
                body["namespace"] = "ns"
            cases.append(_shape(f"{selector}-path-type-{name}", body, code=None))
    for value in (False, True):
        cases.append(_shape(f"offline-hook-{str(value).lower()}",
                            {"epoch": 1, "fail_before_commit": value},
                            target="/api/jobs/shape/commit", initial="running"))
    return tuple(cases)


def _wire_cases() -> tuple[LiteralCase, ...]:
    cases: list[LiteralCase] = []
    for size in (65536, 65537):
        builder = _builder(WIRE_FAMILY, f"raw-bytes-{size}")
        body = fixtures.request_body_boundary(size)
        expected = (fixtures.submit(builder.state, "wire", (fixtures.Entry("wire.txt", "é" * 4096),)).state
                    if size == 65536 else builder.state)
        expectation = _SUCCESS if size == 65536 else semantics.Expectation("error", 400, None)
        headers: tuple[tuple[str, str], ...] = (("Host", f"127.0.0.1:{builder.port}"), ("Connection", "close"),
                   ("Content-Type", "application/json"), ("Content-Length", str(size)))
        builder.request("POST", "/api/jobs", expectation, body=body, headers=headers,
                        label="subject-request", after=expected)
        if size == 65536:
            # Keep the immediate queued readback before establishing content effects.
            builder.action("prepare", "wire", label="confirm-accepted-manifest")
            builder.action("commit", "wire", epoch=1, label="confirm-accepted-content")
            builder.note("Later public document and export checks establish the accepted text effect; no hidden manifest identity is inferred.")
        builder.note("The 65536-byte product body bound is separate from the larger harness request-wire limit.")
        cases.append(builder.finish())
    bodies: tuple[tuple[str, bytes, str | None], ...] = (
        ("wrong-media", b'{"job_id":"wire","entries":[]}', "text/plain"),
        ("missing-media", b'{"job_id":"wire","entries":[]}', None),
        ("malformed-json", b'{"job_id":"wire","entries":[', "application/json"),
        ("array", b"[]", "application/json"),
        ("string", b'"x"', "application/json"),
        ("null", b"null", "application/json"),
        ("number", b"1", "application/json"),
    )
    for name, body, media in bodies:
        builder = _builder(WIRE_FAMILY, name)
        headers = (("Host", f"127.0.0.1:{builder.port}"), ("Connection", "close"),
                   ("Content-Length", str(len(body))))
        if media is not None:
            headers += (("Content-Type", media),)
        builder.request("POST", "/api/jobs", semantics.Expectation("error", 400, None),
                        body=body, headers=headers, label="subject-request", after=builder.state)
        builder.note("The error envelope and generic 400 are prescribed; the HTTP parser/media error code is unspecified.")
        cases.append(builder.finish())
    return tuple(cases)


def definitions() -> tuple[LiteralCase, ...]:
    """Return all 56 shape rows followed by all 9 wire rows in proposal order."""
    return _shape_cases() + _wire_cases()
