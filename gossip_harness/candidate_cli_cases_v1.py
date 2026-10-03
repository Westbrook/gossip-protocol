"""Prospective, host-only finite CLI fixtures derived from frozen V0/M1 prose.

This provider never imports candidate/reference/oracle code. See the companion
notes for its limited immutable-seed exposure disclosure and ungraded facets.
The output is development qualification data, not independent acceptance.
"""
from __future__ import annotations

import base64
from copy import deepcopy
import hashlib
import io
import json
from pathlib import Path
from typing import Any
import zipfile

PROTOCOL = "candidate-cli-cases-v1"
PURPOSE = "harness_qualification"
ROOT = Path(__file__).resolve().parents[1]
REQUIREMENT_IDS = ("V0-CLI-01", "V0-CLI-02", "V0-CLI-03", "M1-CLI-01")
NORMATIVE_SHA256 = {
    "gossip_harness/library_project_fixture_v1.py": "fa4440f1ac7e63e3b4ef7d9978a8970e8d6cd688efe9c2e1b6d1e75c0f757caf",
    "library-m1-acceptance-inventory-v1.json": "f5d808866d6f18f3fd095de5d447bb5b0e2a43e20771761fe91a38cf4f59ad4a",
    "library-cumulative-product-v1.json": "3353dbaa0c2474527cbffb0d87a4091b5f77ed3f98418fff2f216d90b6a8487c",
    "library-cumulative-product-v2.json": "2d88ce0775888f148b0ec3caf90b3d5c82d8fed71f53bec5f7e75f492ae998dc",
}
# Historical review provenance only. Runtime definitions never read runs/.
HISTORICAL_POLICY_PROVENANCE = {
    "path": "runs/candidate-b03-cli-wrapper-policy-review-1.json",
    "sha256": "ae42de0d0d107355868ac7a8820406d61306b26cf2a84a2c68d954909f0d61b5",
}
SEMANTIC_POLICY = {
    "version": "candidate-cli-semantic-policy-v1",
    "success": "Natural exit0 and one stdout JSON value; compare supported values semantically, without canonical raw bytes or an empty-other-stream requirement.",
    "import_and_jobs_wrappers": "Exact CLI wrappers are inferential and unspecified; a wrapper mismatch alone cannot fail a mandatory assertion. Later supported public-state observations do not prove unobserved wrapper/status/census facets.",
    "domain_errors": "Exit2 and prescribed stderr error shape; exact code only where supplied by frozen norms. Whole-stderr JSON is sufficient. Mixed diagnostic framing stays unqualified; no substring extraction or observed-code allowlist.",
    "usage_or_rejection": "Exit2; raw streams retained with presentation unspecified when the frozen clause does not distinguish usage and domain framing.",
    "observation_limits": "Timeouts, capture caps and missing completion do not create product bounds or correctness passes. Preserve independently observed failures beside unavailable or unspecified facets.",
}
AUTHORING_DISCLOSURE = "The first prose read extended into immutable seed _COMMON source_key and the start of identity. No expectations use that snippet; no candidate/reference files or execution outputs were inspected. This provider is not claimed fully source-blind."
CITATIONS = {
    "identity": "gossip_harness/library_project_fixture_v1.py:40-63",
    "cli": "gossip_harness/library_project_fixture_v1.py:65-70",
    "intake": "gossip_harness/library_project_fixture_v1.py:95-111,181-204",
    "jobs": "gossip_harness/library_project_fixture_v1.py:113-175",
    "compatibility": "library-cumulative-product-v1.json:729-732; library-cumulative-product-v2.json:760-764",
}
UNQUALIFIED_FACETS = (
    "serve/listener address ownership and HTTP/browser behavior",
    "genuine concurrency, process-kill recovery and physical storage integrity",
    "blanket empty-other-stream or canonical raw JSON serialization",
    "mixed stderr diagnostic/error framing beyond whole-value JSON qualification",
    "numeric completion deadlines and raw-output caps as product requirements",
    "CLI import/jobs exact wrapper shapes beyond one JSON value",
)
FAMILY_COUNTS = {
    "empty": 1, "legacy": 1, "configuration": 1, "changed-source": 1,
    "selection-rejection": 2, "missing-show": 1, "intake-success": 3,
    "job-census": 1, "action-matrix": 20, "failed-retry": 1,
    "prepare-import-conflict": 1, "intake-io": 3, "namespace": 3,
    "invalid-kind": 1, "grammar": 7, "pagination-query": 4,
    "syntax": 2, "utf8": 3, "epoch-fence": 1,
}


def encoded(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _document(source: str, text: str) -> dict[str, Any]:
    raw_source = source.encode("utf-8")
    return {
        "document_id": "doc-" + hashlib.sha256(b"document\0" + raw_source).hexdigest(),
        "source_id": "src-" + hashlib.sha256(b"source\0" + raw_source).hexdigest(),
        "source": source, "title": source.rsplit("/", 1)[-1], "text": text,
        "blob_id": "blob-" + hashlib.sha256(text.encode("utf-8")).hexdigest(),
    }


def _ordered(documents: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(documents, key=lambda d: (d["source"], d["document_id"]))


def _listing(documents: list[dict[str, Any]], *, offset: int = 0, limit: int = 100) -> dict[str, Any]:
    return {"documents": _ordered(documents)[offset:offset + limit], "total": len(documents)}


def _export(documents: list[dict[str, Any]]) -> dict[str, Any]:
    return {"format": "local-research-library-v0", "documents": _ordered(documents)}


def _job(job_id: str, state: str = "queued", epoch: int = 1, total: int = 1,
         error: str | None = None) -> dict[str, Any]:
    return {"job_id": job_id, "epoch": epoch, "state": state, "total": total,
            "completed": total if state == "completed" else 0, "error": error}


def _token(job_id: str, epoch: int = 1) -> dict[str, Any]:
    return {"job_id": job_id, "epoch": epoch}


def _receipt(job_id: str, documents: list[dict[str, Any]], epoch: int = 1) -> dict[str, Any]:
    return {"job": _job(job_id, "completed", epoch, len(documents)), "documents": _ordered(documents)}


def _success(value: Any = None, *, wrapper_unresolved: bool = False) -> dict[str, Any]:
    unspecified = ["stdout.wrapper"] if wrapper_unresolved else []
    return {
        "kind": "success", "exit_code": 0, "value": value,
        "error_code": None, "semantic_value_supported": not wrapper_unresolved,
        "assertion_ids": ["process.exit", "stdout.json"] + (["stdout.value"] if not wrapper_unresolved else unspecified),
        "unspecified_assertion_ids": unspecified,
    }


def _domain(code: str | None) -> dict[str, Any]:
    return {
        "kind": "domain_error", "exit_code": 2, "value": None,
        "error_code": code, "semantic_value_supported": False,
        "assertion_ids": ["process.exit", "stderr.error", "stderr.error_code"],
        "unspecified_assertion_ids": ["stderr.error_code"] if code is None else [],
    }


def _usage() -> dict[str, Any]:
    return {
        "kind": "usage_or_rejection", "exit_code": 2, "value": None,
        "error_code": None, "semantic_value_supported": False,
        "assertion_ids": ["process.exit", "streams.framing"],
        "unspecified_assertion_ids": ["streams.framing"],
    }


def _bundle(entries: list[tuple[str, str]]) -> bytes:
    return encoded({"entries": [{"source": s, "text": t} for s, t in entries]})


def _zip(entries: list[tuple[str, bytes]]) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_STORED) as archive:
        for name, data in entries:
            info = zipfile.ZipInfo(name, (2020, 1, 1, 0, 0, 0))
            info.create_system = 3
            info.external_attr = 0o100644 << 16
            archive.writestr(info, data)
    return output.getvalue()


class _Case:
    def __init__(self, case_id: str, family: str, *, fixtures: dict[str, bytes] | None = None,
                 directories: tuple[str, ...] = ("root-a",)) -> None:
        self.case_id = case_id
        self.family = family
        self.fixtures = fixtures or {}
        self.directories = directories
        self.steps: list[dict[str, Any]] = []
        self.expectations: dict[str, dict[str, Any]] = {}

    def step(self, args: list[str], expected: dict[str, Any], *, root: str = "root-a", db: str = "a") -> None:
        step_id = f"s{len(self.steps) + 1:02d}"
        self.steps.append({"step_id": step_id, "argv": ["python", "-m", "library", "--db", f"/tmp/db-{db}.sqlite", "--root", f"/inputs/{root}", *args]})
        self.expectations[step_id] = expected

    def value(self) -> dict[str, Any]:
        return {
            "case_id": self.case_id, "family_id": self.family,
            "requirement_ids": list(REQUIREMENT_IDS), "purpose": PURPOSE,
            "recipe": {
                "case_id": self.case_id,
                "fixtures": {p: base64.b64encode(b).decode("ascii") for p, b in sorted(self.fixtures.items())},
                "directories": list(self.directories), "steps": self.steps,
            },
            "expectations": self.expectations,
            "citations": dict(CITATIONS),
            "unsupported_facets": list(UNQUALIFIED_FACETS),
            "claim": "Only declared finite CLI subfacets; no full requirement or B03 closure.",
        }


def _catalog_checks(case: _Case, documents: list[dict[str, Any]], *, root: str = "root-a", db: str = "a") -> None:
    case.step(["list"], _success(_listing(documents)), root=root, db=db)
    case.step(["export"], _success(_export(documents)), root=root, db=db)


def _submit(case: _Case, job_id: str, path: str = "batch.json", *, total: int = 1) -> None:
    case.step(["job-submit", job_id, "--kind", "json", path], _success(_job(job_id, total=total)))


def _make_definitions() -> tuple[dict[str, Any], ...]:
    cases: list[_Case] = []
    c = _Case("cli-empty", "empty")
    c.step(["list"], _success(_listing([])))
    c.step(["list", "--offset", "2", "--limit", "1"], _success(_listing([])))
    c.step(["search", "absent"], _success(_listing([])))
    c.step(["export"], _success(_export([])))
    cases.append(c)

    common = "Straße [a.*] <b>&\n"
    docs = [_document("β.md", common), _document("a.txt", ""), _document("same.txt", common)]
    c = _Case("cli-legacy-persistence", "legacy", fixtures={"root-a/" + d["source"]: d["text"].encode("utf-8") for d in docs})
    for d in docs:
        c.step(["import", d["source"]], _success(wrapper_unresolved=True))
    _catalog_checks(c, docs)
    c.step(["list", "--offset", "1", "--limit", "1"], _success(_listing(docs, offset=1, limit=1)))
    c.step(["search", "STRASSE"], _success(_listing([docs[0], docs[2]])))
    c.step(["search", "[a.*]", "--offset", "1", "--limit", "1"], _success(_listing([docs[0], docs[2]], offset=1, limit=1)))
    c.step(["search", "not present"], _success(_listing([])))
    for d in docs:
        c.step(["show", d["document_id"]], _success(d))
    c.step(["export", docs[0]["document_id"], docs[1]["document_id"]], _success(_export([docs[0], docs[1]])))
    c.step(["import", docs[0]["source"]], _success(wrapper_unresolved=True))
    _catalog_checks(c, docs)
    cases.append(c)

    original, changed = _document("same.txt", "first\n"), _document("same.txt", "second\n")
    roots = {"root-a/same.txt": b"first\n", "root-b/same.txt": b"second\n"}
    c = _Case("cli-db-root-isolation", "configuration", fixtures=roots, directories=("root-a", "root-b"))
    c.step(["import", "same.txt"], _success(wrapper_unresolved=True))
    c.step(["show", original["document_id"]], _success(original), root="root-b")
    c.step(["list"], _success(_listing([])), root="root-b", db="b")
    c.step(["import", "same.txt"], _success(wrapper_unresolved=True), root="root-b", db="b")
    c.step(["show", changed["document_id"]], _success(changed), root="root-a", db="b")
    _catalog_checks(c, [original])
    _catalog_checks(c, [changed], root="root-b", db="b")
    cases.append(c)
    c = _Case("cli-source-changed", "changed-source", fixtures=roots, directories=("root-a", "root-b"))
    c.step(["import", "same.txt"], _success(wrapper_unresolved=True))
    c.step(["import", "same.txt"], _domain("source_changed"), root="root-b")
    c.step(["show", original["document_id"]], _success(original))
    _catalog_checks(c, [original])
    cases.append(c)

    selected = _document("a.txt", "kept")
    for defect, ids in [("duplicate", [selected["document_id"], selected["document_id"]]), ("missing", [selected["document_id"], "doc-" + "0" * 64])]:
        c = _Case("cli-export-" + defect, "selection-rejection", fixtures={"root-a/a.txt": b"kept"})
        c.step(["import", "a.txt"], _success(wrapper_unresolved=True))
        c.step(["export", *ids], _domain(None))
        _catalog_checks(c, [selected])
        cases.append(c)
    c = _Case("cli-show-missing", "missing-show")
    c.step(["show", "doc-" + "0" * 64], _domain("not_found"))
    _catalog_checks(c, [])
    cases.append(c)

    for kind in ("directory", "zip", "json"):
        entries = [("b.html", "<b>literal</b>"), ("a.txt", "")]
        prefix = "ns/" if kind != "json" else ""
        batch_docs = [_document(prefix + s, t) for s, t in entries]
        if kind == "directory":
            fixtures = {"root-a/batch/" + s: t.encode("utf-8") for s, t in entries}
            path = "batch"
        elif kind == "zip":
            fixtures = {"root-a/batch.zip": _zip([(s, t.encode("utf-8")) for s, t in entries])}
            path = "batch.zip"
        else:
            fixtures = {"root-a/batch.json": _bundle(entries)}
            path = "batch.json"
        c = _Case("cli-intake-" + kind, "intake-success", fixtures=fixtures)
        submit = ["job-submit", "intake", "--kind", kind, path] + (["--namespace", "ns"] if kind != "json" else [])
        c.step(submit, _success(_job("intake", total=2)))
        c.step(["job-show", "intake"], _success(_job("intake", total=2)))
        c.step(["jobs"], _success(wrapper_unresolved=True))
        c.step(["job-prepare", "intake"], _success(_token("intake")))
        c.step(["job-show", "intake"], _success(_job("intake", "running", total=2)))
        c.step(["job-prepare", "intake"], _success(_token("intake")))
        c.step(["job-commit", "intake", "1"], _success(_receipt("intake", batch_docs)))
        c.step(["job-show", "intake"], _success(_job("intake", "completed", total=2)))
        c.step(["job-commit", "intake", "1"], _success(_receipt("intake", batch_docs)))
        _catalog_checks(c, batch_docs)
        cases.append(c)

    batch = [("item.txt", "one")]
    batch_doc = _document(*batch[0])
    c = _Case("cli-jobs-replay-conflict", "job-census", fixtures={"root-a/batch.json": _bundle(batch), "root-a/other.json": _bundle([("other.txt", "two")])})
    for job_id in ("z-job", "A-job", "m-job"):
        _submit(c, job_id)
    c.step(["jobs"], _success(wrapper_unresolved=True))
    for job_id in ("A-job", "m-job", "z-job"):
        c.step(["job-show", job_id], _success(_job(job_id)))
    _submit(c, "z-job")
    c.step(["job-submit", "z-job", "--kind", "json", "other.json"], _domain("job_conflict"))
    c.step(["job-show", "z-job"], _success(_job("z-job")))
    c.step(["jobs"], _success(wrapper_unresolved=True))
    _catalog_checks(c, [])
    cases.append(c)

    for state in ("queued", "running", "completed", "cancelled", "failed"):
        for action in ("prepare", "commit", "cancel", "retry"):
            entries = [("../bad.txt", "bad")] if state == "failed" else batch
            c = _Case(f"cli-action-{state}-{action}", "action-matrix", fixtures={"root-a/batch.json": _bundle(entries)})
            _submit(c, "job")
            if state in ("running", "completed"):
                c.step(["job-prepare", "job"], _success(_token("job")))
            if state == "completed":
                c.step(["job-commit", "job", "1"], _success(_receipt("job", [batch_doc])))
            if state == "cancelled":
                c.step(["job-cancel", "job"], _success(_job("job", "cancelled", 2)))
            if state == "failed":
                c.step(["job-prepare", "job"], _domain("invalid_source"))
            epoch = 2 if state == "cancelled" else 1
            after = _job("job", state, epoch, error="invalid_source" if state == "failed" else None)
            documents = [batch_doc] if state == "completed" else []
            expected = _domain("job_state")
            if action == "prepare" and state in ("queued", "running"):
                expected = _success(_token("job", epoch)); after = _job("job", "running", epoch)
            elif action == "commit" and state in ("running", "completed"):
                expected = _success(_receipt("job", [batch_doc], epoch)); after = _job("job", "completed", epoch); documents = [batch_doc]
            elif action == "cancel" and state in ("queued", "running", "cancelled"):
                next_epoch = epoch if state == "cancelled" else epoch + 1
                after = _job("job", "cancelled", next_epoch); expected = _success(after)
            elif action == "retry" and state in ("failed", "cancelled"):
                after = _job("job", "queued", epoch + 1); expected = _success(after)
            args = ["job-" + action, "job"] + ([str(epoch)] if action == "commit" else [])
            c.step(args, expected)
            c.step(["job-show", "job"], _success(after))
            _catalog_checks(c, documents)
            cases.append(c)

    c = _Case("cli-failed-prepare-retry", "failed-retry", fixtures={"root-a/batch.json": _bundle([("../bad.txt", "bad")])})
    _submit(c, "failed")
    for epoch in (1, 2):
        c.step(["job-prepare", "failed"], _domain("invalid_source"))
        c.step(["job-show", "failed"], _success(_job("failed", "failed", epoch, error="invalid_source")))
        _catalog_checks(c, [])
        if epoch == 1:
            c.step(["job-retry", "failed"], _success(_job("failed", "queued", 2)))
    cases.append(c)

    intervening = _document("item.txt", "changed after preparation")
    c = _Case("cli-prepare-import-conflict", "prepare-import-conflict", fixtures={"root-a/batch.json": _bundle(batch), "root-a/item.txt": intervening["text"].encode()})
    _submit(c, "conflict")
    c.step(["job-prepare", "conflict"], _success(_token("conflict")))
    c.step(["import", "item.txt"], _success(wrapper_unresolved=True))
    c.step(["job-commit", "conflict", "1"], _domain("source_changed"))
    c.step(["job-show", "conflict"], _success(_job("conflict", "failed", error="source_changed")))
    _catalog_checks(c, [intervening])
    cases.append(c)

    for kind in ("directory", "zip", "json"):
        suffix = "" if kind == "directory" else "." + kind
        c = _Case("cli-intake-io-" + kind, "intake-io")
        args = ["job-submit", "missing", "--kind", kind, "missing" + suffix]
        if kind != "json": args += ["--namespace", "ns"]
        c.step(args, _domain("io_error"))
        c.step(["job-show", "missing"], _domain("not_found"))
        _catalog_checks(c, [])
        cases.append(c)

    for kind in ("directory", "zip", "json"):
        fixtures = {"root-a/batch/a.txt": b"a", "root-a/batch.zip": _zip([("a.txt", b"a")]), "root-a/batch.json": _bundle([("a.txt", "a")])}
        c = _Case("cli-namespace-" + kind, "namespace", fixtures=fixtures)
        path = "batch" if kind == "directory" else "batch." + kind
        args = ["job-submit", "bad", "--kind", kind, path] + (["--namespace", "forbidden"] if kind == "json" else [])
        c.step(args, _usage())
        c.step(["job-show", "bad"], _domain("not_found"))
        _catalog_checks(c, [])
        cases.append(c)

    c = _Case("cli-invalid-kind", "invalid-kind")
    c.step(["job-submit", "bad", "--kind", "tar", "batch.tar"], _usage())
    c.step(["job-show", "bad"], _domain("not_found"))
    cases.append(c)
    grammar = [("missing-command", []), ("unknown-command", ["unknown-command"]),
               ("missing-import-source", ["import"]), ("missing-show-id", ["show"]),
               ("missing-epoch", ["job-commit", "job"]), ("noninteger-epoch", ["job-commit", "job", "not-an-integer"]),
               ("unknown-flag", ["list", "--unknown-flag"])]
    for label, args in grammar:
        c = _Case("cli-grammar-" + label, "grammar")
        c.step(args, _usage()); cases.append(c)
    for label, args in [("offset-negative", ["list", "--offset", "-1"]), ("limit-zero", ["list", "--limit", "0"]),
                        ("limit-over", ["search", "x", "--limit", "101"]), ("query-over", ["search", "x" * 257])]:
        c = _Case("cli-invalid-" + label, "pagination-query")
        c.step(args, _usage()); _catalog_checks(c, []); cases.append(c)

    for kind, content, code in [("zip", b"not a zip", "invalid_archive"), ("json", b"{", "invalid_json")]:
        c = _Case("cli-syntax-" + kind, "syntax", fixtures={"root-a/bad." + kind: content})
        args = ["job-submit", "bad", "--kind", kind, "bad." + kind] + (["--namespace", "ns"] if kind == "zip" else [])
        c.step(args, _domain(code)); c.step(["job-show", "bad"], _domain("not_found")); _catalog_checks(c, []); cases.append(c)
    for kind in ("directory", "zip", "json"):
        path = "batch" if kind == "directory" else "batch." + kind
        content_path = "root-a/batch/a.txt" if kind == "directory" else "root-a/" + path
        content = _zip([("a.txt", b"\xff")]) if kind == "zip" else b"\xff"
        c = _Case("cli-utf8-" + kind, "utf8", fixtures={content_path: content})
        args = ["job-submit", "bad", "--kind", kind, path] + (["--namespace", "ns"] if kind != "json" else [])
        c.step(args, _domain("invalid_utf8")); c.step(["job-show", "bad"], _domain("not_found")); _catalog_checks(c, []); cases.append(c)

    c = _Case("cli-cancel-retry-epoch-fence", "epoch-fence", fixtures={"root-a/batch.json": _bundle(batch)})
    _submit(c, "fenced")
    c.step(["job-prepare", "fenced"], _success(_token("fenced")))
    c.step(["job-cancel", "fenced"], _success(_job("fenced", "cancelled", 2)))
    c.step(["job-cancel", "fenced"], _success(_job("fenced", "cancelled", 2)))
    c.step(["job-retry", "fenced"], _success(_job("fenced", "queued", 3)))
    c.step(["job-commit", "fenced", "1"], _domain("stale_epoch"))
    c.step(["job-prepare", "fenced"], _success(_token("fenced", 3)))
    c.step(["job-commit", "fenced", "1"], _domain("stale_epoch"))
    c.step(["job-commit", "fenced", "3"], _success(_receipt("fenced", [batch_doc], 3)))
    c.step(["job-show", "fenced"], _success(_job("fenced", "completed", 3)))
    _catalog_checks(c, [batch_doc])
    cases.append(c)
    return tuple(c.value() for c in cases)


def definitions() -> tuple[dict[str, Any], ...]:
    """Return fresh host-only definitions, never candidate or oracle imports."""
    return _make_definitions()


def case_definition(case_id: str) -> dict[str, Any]:
    for case in definitions():
        if case["case_id"] == case_id:
            return case
    raise KeyError(case_id)


def execution_recipe(case_id: str) -> dict[str, Any]:
    """Return only inert fixture/argv instructions; expected answers stay host-only."""
    return deepcopy(case_definition(case_id)["recipe"])


def definition_sha256() -> str:
    return hashlib.sha256(encoded({"protocol": PROTOCOL, "purpose": PURPOSE, "definitions": definitions(), "normative_sources": NORMATIVE_SHA256, "semantic_policy": SEMANTIC_POLICY, "historical_policy_provenance": HISTORICAL_POLICY_PROVENANCE, "authoring_disclosure": AUTHORING_DISCLOSURE})).hexdigest()


def definition_sources() -> dict[str, str]:
    """Verify tracked norms and provider source; no historical run receipt needed."""
    expected = dict(NORMATIVE_SHA256)
    sources = {path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest() for path in expected}
    if sources != expected:
        raise ValueError("frozen normative source identity mismatch")
    sources["gossip_harness/candidate_cli_cases_v1.py"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    return sources
