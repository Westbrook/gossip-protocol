"""Literal declarations for all 69 prospective M1 root/path catalog rows.

These are immutable, nondispatchable recipes. A staged path is an evaluator-owned
layout descriptor, not a product source key and not a host filesystem operation.
The qualified staging/observation bridge must still establish roots, link targets,
attribution and non-following; rejection and public state do not establish those
physical facts by themselves.
"""
from __future__ import annotations

from . import candidate_http_cases_core_v1 as core
from . import candidate_http_fixtures_v1 as f
from . import candidate_http_semantics_v1 as s

FAMILY = "HTTP-ROOT-PATH"
REQUIREMENTS = ("M1-I31",)
ROOT = "/inputs/root"

# Preserve the retained proposal's request strings. In ZIP/JSON lexical rows the
# missing bundle suffix is an additional fault: only rejection and generic 400
# are asserted; no validation precedence or exact error classification is added.
_PATH_VARIANTS = (
    "nested", "absolute", "traversal", "direct-link", "ancestor-link", "wrong-kind",
    "lexical-backslash", "lexical-empty-segment", "lexical-dot", "lexical-nul", "lexical-empty",
)
_LEXICAL_PATHS = (
    ("lexical-backslash", "nested\\batch"),
    ("lexical-empty-segment", "nested//batch"),
    ("lexical-dot", "nested/./batch"),
    ("lexical-nul", "nested/\0batch"),
    ("lexical-empty", ""),
)
_BOUNDARY_TEXT = "boundary\n"


def _builder(suffix: str) -> core.Builder:
    return core.Builder(f"{FAMILY}/{suffix}", REQUIREMENTS, root=ROOT)


def _body(form: str, path: str, namespace: str = "ns") -> dict[str, object]:
    result: dict[str, object] = {"job_id": "subject", form: path}
    if form in ("directory", "zip"):
        result["namespace"] = namespace
    return result


def _stage_batch(b: core.Builder, form: str, path: str,
                 entries: tuple[f.Entry, ...]) -> None:
    """Stage literal discovery input. Entry sources carry the fixed ns/ prefix."""
    if form == "directory":
        b.add_directory(path)
        for entry in entries:
            b.add_file(path + "/" + entry.source.removeprefix("ns/"), entry.text.encode("utf-8"))
    elif form == "zip":
        b.add_file(path, f.stored_zip(tuple(
            f.ZipMember(entry.source.removeprefix("ns/"), entry.text.encode("utf-8"))
            for entry in entries)))
    else:
        b.add_file(path, f.json_bundle(entries))


def _path_layout(b: core.Builder, form: str, variant: str) -> str:
    extension = {"directory": "", "zip": ".zip", "json": ".json"}[form]
    leaf = "batch" + extension
    # Distinct bytes under the same logical keys distinguish the requested root
    # from both the owned outside target and the input mount's top-level decoy.
    selected = f.CANONICAL_BATCH
    outside = tuple(f.Entry(entry.source, "owned-other-sentinel:" + entry.text) for entry in selected)
    decoy = tuple(f.Entry(entry.source, "input-mount-decoy:" + entry.text) for entry in selected)
    _stage_batch(b, form, "root/nested/" + leaf, selected)
    _stage_batch(b, form, "other/" + leaf, outside)
    _stage_batch(b, form, "nested/" + leaf, decoy)
    if variant == "direct-link":
        b.add_link("root/link" + extension, "../other/" + leaf)
    elif variant == "ancestor-link":
        b.add_link("root/linked", "../other")
    elif variant == "wrong-kind":
        if form == "directory":
            b.add_file("root/wrong", b"owned regular file, not a directory\n")
        else:
            b.add_directory("root/wrong" + extension)
            b.add_file("root/wrong" + extension + "/sentinel.txt", b"owned directory, not a bundle\n")
    elif variant == "wrong-suffix":
        _stage_batch(b, form, "root/valid-content.bin", selected)
    paths = {
        "nested": "nested/" + leaf,
        "absolute": "/inputs/other/" + leaf,
        "traversal": "../other/" + leaf,
        "direct-link": "link" + extension,
        "ancestor-link": "linked/" + leaf,
        "wrong-kind": "wrong" + extension,
        "wrong-suffix": "valid-content.bin",
        **dict(_LEXICAL_PATHS),
    }
    return paths[variant]


def _submit_valid(b: core.Builder, body: dict[str, object], manifest: tuple[f.Entry, ...]) -> None:
    admitted = f.submit(b.state, "subject", manifest)
    b.post("/api/jobs", body, s.success("unspecified"), after=admitted.state, label="submit")
    b.action("prepare", "subject")
    b.action("commit", "subject", epoch=1)


def _path_case(form: str, variant: str) -> core.LiteralCase:
    b = _builder(form + "-" + variant)
    path = _path_layout(b, form, variant)
    b.start()
    b.guard()
    if variant == "nested":
        _submit_valid(b, _body(form, path), f.CANONICAL_BATCH)
    else:
        expectation = (s.classified_error("invalid_source")
                       if variant in ("traversal", "direct-link", "ancestor-link")
                       else s.unclassified_error(400))
        b.post("/api/jobs", _body(form, path), expectation, label="rejected-submit")
    if variant.startswith("lexical-") and form != "directory":
        b.note("Retained literal PATH has lexical and bundle-suffix faults; generic400/rejection only, "
               "without an exact code or precedence assertion.")
    if variant in ("direct-link", "ancestor-link"):
        b.note("Data-only symlink target is evaluator-owned /inputs/other; rejected request and public "
               "conservation alone do not prove the target was not followed or read.")
    b.note("Explicit nested Service.root=/inputs/root; root-selected, outside-sentinel and input-mount "
           "decoy manifests have distinct bytes for the same source keys.")
    return b.finish()


def _member_fixture(b: core.Builder, form: str, member: str) -> str:
    if form == "directory":
        b.add_directory("root/batch")
        b.add_file("root/batch/" + member, _BOUNDARY_TEXT.encode("utf-8"))
        return "batch"
    b.add_file("root/batch.zip", f.stored_zip((f.ZipMember(member, _BOUNDARY_TEXT.encode("utf-8")),)))
    return "batch.zip"


def _namespace_case(form: str, name: str, namespace: str) -> core.LiteralCase:
    b = _builder(form + "-namespace-" + name)
    path = _member_fixture(b, form, "a.txt")
    b.start()
    b.guard()
    # Reviewed R02 assigns traversal independently; other lexical classifications
    # remain unknown even though no-admission and generic 400 are prescribed.
    expectation = s.classified_error("invalid_source") if name == "parent" else s.unclassified_error(400)
    b.post("/api/jobs", _body(form, path, namespace), expectation, label="rejected-submit")
    return b.finish()


def _combined_case(form: str, boundary: f.KeyBoundary) -> core.LiteralCase:
    b = _builder(form + "-" + boundary.name)
    path = _member_fixture(b, form, boundary.member)
    b.start()
    b.guard()
    body = _body(form, path, boundary.namespace)
    if boundary.name in ("combined-bytes-256", "combined-segments-16"):
        _submit_valid(b, body, (f.Entry(boundary.source, _BOUNDARY_TEXT),))
    else:
        b.post("/api/jobs", body, s.unclassified_error(400), label="rejected-submit")
    b.note("Staging-path depth is independent of product source depth; the asserted key is precisely "
           "namespace + '/' + member. No content too_large classification is transferred to key bounds.")
    return b.finish()


def _source_case(form: str, name: str, source: str) -> core.LiteralCase:
    b = _builder(form + "-source-" + name)
    manifest = (f.Entry(source, _BOUNDARY_TEXT),)
    if form == "json":
        b.add_file("root/boundary.json", f.json_bundle(manifest))
        body = _body("json", "boundary.json")
    else:
        body = {"job_id": "subject", "entries": [entry.as_json() for entry in manifest]}
    b.start()
    b.guard()
    if name in ("bytes256", "segments16"):
        _submit_valid(b, body, manifest)
    else:
        admitted = f.submit(b.state, "subject", manifest)
        b.post("/api/jobs", body, s.success("unspecified"), after=admitted.state, label="submit")
        b.census("before-prepare")
        error_step = b.post("/api/jobs/subject/prepare", {}, s.unclassified_error(400),
                            check=False, label="prepare")
        b.partial_failure("subject", error_step, label="after-failure")
        b.note("Schema-valid invalid source is admitted queued, then fails prepare. The relation fixes "
               "response.error == subject JOB.error prospectively while checking every other job field "
               "and unaffected public state exactly; it never assigns an expected CODE.")
    return b.finish()


def definitions() -> tuple[core.LiteralCase, ...]:
    """All 69 exact retained row IDs in their prospective catalog order."""
    cases: list[core.LiteralCase] = []
    for form in ("directory", "zip", "json"):
        for variant in _PATH_VARIANTS:
            cases.append(_path_case(form, variant))
        if form in ("zip", "json"):
            cases.append(_path_case(form, "wrong-suffix"))
    for form in ("directory", "zip"):
        for name, namespace in f.INVALID_NAMESPACES:
            cases.append(_namespace_case(form, name, namespace))
        for boundary in f.KEY_BOUNDARIES:
            cases.append(_combined_case(form, boundary))
    for form in ("entries", "json"):
        for name, source in f.DIRECT_SOURCE_BOUNDARIES:
            cases.append(_source_case(form, name, source))
    return tuple(cases)
