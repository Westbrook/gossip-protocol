"""Pure, independently authored prospective M1 HTTP fixture values.

No candidate/reference imports, filesystem access, dispatch, observation, or
acceptance authority. Host constructor errors describe unsupported fixture
recipes; they are never classifications of arbitrary HTTP input. Normative
sources are the V0/M1 requirement strings and their reviewed HTTP policy.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
import binascii
import hashlib
import json
import re
import struct
from typing import Literal

VERSION = "candidate-c03-http-fixtures-v1"
SOURCE_PINS = (
    ("gossip_harness/library_project_fixture_v1.py",
     "fa4440f1ac7e63e3b4ef7d9978a8970e8d6cd688efe9c2e1b6d1e75c0f757caf"),
    ("analysis/candidate-b03-c03-policy-review-v1.json",
     "bec2139ba2b6e99d5ac6da01296842cc690af5c84f3428aaa5183eaf2ec54570"),
)
EXECUTION_AUTHORITY = False
ACCEPTANCE_AUTHORITY = False
JobState = Literal["queued", "running", "completed", "cancelled", "failed"]
Action = Literal["prepare", "commit", "cancel", "retry"]
STATES: tuple[JobState, ...] = ("queued", "running", "completed", "cancelled", "failed")
ACTIONS: tuple[Action, ...] = ("prepare", "commit", "cancel", "retry")
_JOB_ID = re.compile(r"[A-Za-z0-9_-]{1,64}\Z")


class FixtureError(ValueError):
    """Invalid authoring input, not a product failure or HTTP error verdict."""


class UnspecifiedFixture(FixtureError):
    """The source does not select one exact error for this fixture's faults."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise FixtureError(message)


def _utf8(value: str) -> bytes:
    try:
        return value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise FixtureError("fixture text must be encodable as strict UTF-8") from exc


def _job_id(value: str) -> None:
    _require(type(value) is str and _JOB_ID.fullmatch(value) is not None, "invalid fixture job ID")


def _valid_key(source: str) -> bool:
    parts = source.split("/")
    return (1 <= len(_utf8(source)) <= 256 and "\\" not in source and "\0" not in source
            and len(parts) <= 16 and all(p not in ("", ".", "..")
                                       and len(_utf8(p)) <= 128 for p in parts))


@dataclass(frozen=True)
class Entry:
    source: str
    text: str

    def __post_init__(self) -> None:
        _require(type(self.source) is str and type(self.text) is str, "fixture entries require exact strings")

    def as_json(self) -> dict[str, str]:
        return {"source": self.source, "text": self.text}


def canonical_manifest(entries: tuple[Entry, ...]) -> tuple[Entry, ...]:
    """Canonicalize shape-valid input without performing deferred validation."""
    _require(type(entries) is tuple and all(type(e) is Entry for e in entries), "immutable entries required")
    return tuple(sorted(entries, key=lambda e: (e.source, e.text)))


@dataclass(frozen=True)
class Document:
    source: str
    text: str

    def __post_init__(self) -> None:
        _require(type(self.source) is str and type(self.text) is str, "document strings required")
        _require(_valid_key(self.source), "document requires a valid M1 source key")
        _require(self.source.endswith((".txt", ".md", ".html")), "document fixture suffix is unsupported")
        _require(len(_utf8(self.text)) <= 32768, "document fixture exceeds member bytes")

    @property
    def document_id(self) -> str:
        return "doc-" + hashlib.sha256(b"document\0" + _utf8(self.source)).hexdigest()

    @property
    def source_id(self) -> str:
        return "src-" + hashlib.sha256(b"source\0" + _utf8(self.source)).hexdigest()

    @property
    def blob_id(self) -> str:
        return "blob-" + hashlib.sha256(_utf8(self.text)).hexdigest()

    def as_json(self) -> dict[str, str]:
        return {"document_id": self.document_id, "source_id": self.source_id,
                "source": self.source, "blob_id": self.blob_id,
                "title": self.source.rsplit("/", 1)[-1], "text": self.text}


def document(source: str, text: str) -> Document:
    return Document(source, text)


@dataclass(frozen=True)
class Job:
    job_id: str
    epoch: int
    state: JobState
    total: int
    completed: int
    error: str | None

    def __post_init__(self) -> None:
        _job_id(self.job_id)
        _require(type(self.epoch) is int and self.epoch >= 1, "fixture epoch must be a positive integer")
        _require(self.state in STATES and type(self.state) is str, "unknown fixture job state")
        _require(type(self.total) is int and self.total >= 0, "fixture total must be nonnegative integer")
        _require(type(self.completed) is int and self.completed == (self.total if self.state == "completed" else 0),
                 "M1 fixtures have no partial progress")
        _require((type(self.error) is str and bool(self.error)) if self.state == "failed" else self.error is None,
                 "only failed fixtures carry an error")

    def as_json(self) -> dict[str, object]:
        return {"job_id": self.job_id, "epoch": self.epoch, "state": self.state,
                "total": self.total, "completed": self.completed, "error": self.error}


@dataclass(frozen=True)
class Token:
    job_id: str
    epoch: int

    def __post_init__(self) -> None:
        _job_id(self.job_id)
        _require(type(self.epoch) is int and self.epoch >= 1, "positive fixture token epoch required")

    def as_json(self) -> dict[str, object]:
        return {"job_id": self.job_id, "epoch": self.epoch}


@dataclass(frozen=True)
class Receipt:
    job: Job
    documents: tuple[Document, ...]

    def __post_init__(self) -> None:
        _require(type(self.job) is Job and self.job.state == "completed", "completed fixture job required")
        _require(type(self.documents) is tuple and all(type(d) is Document for d in self.documents),
                 "immutable receipt documents required")
        _require(self.documents == tuple(sorted(self.documents, key=lambda d: (d.source, d.document_id))),
                 "receipt documents must be source ordered")
        _require(len({d.source for d in self.documents}) == len(self.documents) == self.job.total,
                 "receipt must contain each batch source once")

    def as_json(self) -> dict[str, object]:
        return {"job": self.job.as_json(), "documents": [d.as_json() for d in self.documents]}


@dataclass(frozen=True)
class JobRecord:
    job: Job
    manifest: tuple[Entry, ...]
    receipt: Receipt | None = None

    def __post_init__(self) -> None:
        _require(type(self.job) is Job, "immutable job required")
        _require(self.manifest == canonical_manifest(self.manifest), "manifest must already be canonical")
        _require(self.job.total == len(self.manifest), "manifest count must match job total")
        _require((self.receipt is not None) == (self.job.state == "completed"), "completed job requires receipt")
        if self.receipt is not None:
            _require(type(self.receipt) is Receipt and self.receipt.job == self.job, "receipt must bind this completed job")
            _require(self.receipt.documents == tuple(document(e.source, e.text) for e in self.manifest),
                     "receipt documents must match the immutable manifest")


@dataclass(frozen=True)
class ExpectedState:
    documents: tuple[Document, ...] = ()
    jobs: tuple[JobRecord, ...] = ()

    def __post_init__(self) -> None:
        _require(type(self.documents) is tuple and all(type(d) is Document for d in self.documents),
                 "immutable documents required")
        _require(type(self.jobs) is tuple and all(type(j) is JobRecord for j in self.jobs), "immutable jobs required")
        _require(self.documents == tuple(sorted(self.documents, key=lambda d: (d.source, d.document_id))),
                 "documents must be in source/ID order")
        _require(len({d.source for d in self.documents}) == len(self.documents) <= 256, "unique bounded sources required")
        _require(self.jobs == tuple(sorted(self.jobs, key=lambda r: r.job.job_id)), "jobs must be in ID order")
        _require(len({r.job.job_id for r in self.jobs}) == len(self.jobs), "duplicate fixture jobs")
        _require(all(r.receipt is None or all(d in self.documents for d in r.receipt.documents) for r in self.jobs),
                 "completed receipt documents must exist unchanged in expected catalog")

    def get(self, job_id: str) -> JobRecord | None:
        return next((r for r in self.jobs if r.job.job_id == job_id), None)

    def jobs_json(self) -> dict[str, object]:
        return {"jobs": [r.job.as_json() for r in self.jobs]}

    def listing(self, *, query: str = "", offset: int = 0, limit: int = 100) -> dict[str, object]:
        # These query types/bounds, including bool rejection, are explicit V0 text.
        _require(type(query) is str and len(query) <= 256, "invalid fixture query")
        _require(type(offset) is int and offset >= 0, "invalid fixture offset")
        _require(type(limit) is int and 1 <= limit <= 100, "invalid fixture limit")
        selected = tuple(d for d in self.documents if query.casefold() in (d.source + "\n" + d.text).casefold())
        return {"documents": [d.as_json() for d in selected[offset:offset + limit]], "total": len(selected)}

    def export(self, ids: tuple[str, ...] | None = None) -> dict[str, object]:
        """Project valid selected IDs; invalid authoring IDs get no product code."""
        selected = self.documents
        if ids is not None:
            _require(type(ids) is tuple and all(type(i) is str for i in ids), "immutable fixture IDs required")
            _require(len(set(ids)) == len(ids), "duplicate fixture selection")
            _require(set(ids) <= {d.document_id for d in selected}, "unknown fixture selection")
            selected = tuple(d for d in selected if d.document_id in ids)
        return {"format": "local-research-library-v0", "documents": [d.as_json() for d in selected]}

    def census_offsets(self) -> tuple[int, ...]:
        """Coverage chosen from independent expected size, never observed total."""
        return (0,) if len(self.documents) <= 100 else ((0, 100) if len(self.documents) <= 200 else (0, 100, 200))


@dataclass(frozen=True)
class Transition:
    state: ExpectedState
    error: str | None = None
    value: Job | Token | Receipt | Document | None = None
    replay: bool = False

    def __post_init__(self) -> None:
        _require(type(self.state) is ExpectedState, "immutable transition state required")
        _require(self.error is None or (type(self.error) is str and bool(self.error)), "fixture error must be a code or None")
        _require(self.value is None or type(self.value) in (Job, Token, Receipt, Document), "immutable transition value required")
        _require(type(self.replay) is bool, "fixture replay flag must be boolean")
        _require(self.error is None or (self.value is None and not self.replay), "error has no successful value/replay")

    @property
    def http_status(self) -> int:
        if self.error is None:
            return 200
        if self.error == "not_found":
            return 404
        if self.error in ("source_changed", "job_conflict", "job_state", "stale_epoch"):
            return 409
        return 400


def _replace_job(state: ExpectedState, record: JobRecord) -> ExpectedState:
    jobs = tuple(r for r in state.jobs if r.job.job_id != record.job.job_id) + (record,)
    return ExpectedState(state.documents, tuple(sorted(jobs, key=lambda r: r.job.job_id)))


def submit(state: ExpectedState, job_id: str, entries: tuple[Entry, ...]) -> Transition:
    """Admission for authored shape-valid entries; semantic validation is deferred."""
    _job_id(job_id)
    manifest = canonical_manifest(entries)
    old = state.get(job_id)
    if old is not None:
        return (Transition(state, value=old.job, replay=True) if old.manifest == manifest
                else Transition(state, error="job_conflict"))
    job = Job(job_id, 1, "queued", len(manifest), 0, None)
    return Transition(_replace_job(state, JobRecord(job, manifest)), value=job)


def _validation_error(state: ExpectedState, manifest: tuple[Entry, ...]) -> str | None:
    """Only isolated source-assigned faults; never invent multi-error precedence."""
    errors: set[str] = set()
    if len(manifest) > 64 or len({e.source for e in manifest}) != len(manifest):
        errors.add("invalid_batch")
    total_bytes = 0
    for entry in manifest:
        if not _valid_key(entry.source):
            if ".." in entry.source.split("/"):
                errors.add("invalid_source")
            else:
                raise UnspecifiedFixture("source-key fault has no uniquely reviewed error classification")
        if not entry.source.endswith((".txt", ".md", ".html")):
            errors.add("unsupported_type")
        size = len(_utf8(entry.text))
        total_bytes += size
        if size > 32768:
            errors.add("too_large")
        if any(d.source == entry.source and d.text != entry.text for d in state.documents):
            errors.add("source_changed")
    if total_bytes > 524288:
        errors.add("too_large")
    if len({d.source for d in state.documents} | {e.source for e in manifest}) > 256:
        errors.add("capacity")
    if len(errors) > 1:
        raise UnspecifiedFixture("multiple distinct faults lack source-defined validation precedence")
    return next(iter(errors), None)


def apply_action(state: ExpectedState, action: Action, job_id: str, *, epoch: int | None = None) -> Transition:
    """Derive known-state transitions. Values describe methods, not HTTP wrappers."""
    _require(action in ACTIONS, "unknown fixture action")
    _job_id(job_id)
    if action == "commit":
        _require(type(epoch) is int and epoch >= 1, "authoring commit needs a known positive integer epoch")
    else:
        _require(epoch is None, "only commit fixtures carry epoch")
    old = state.get(job_id)
    if old is None:
        return Transition(state, error="not_found")
    job = old.job
    if action == "commit" and epoch != job.epoch:
        return Transition(state, error="stale_epoch")
    if action == "prepare" and job.state == "running":
        return Transition(state, value=Token(job.job_id, job.epoch), replay=True)
    if action == "commit" and job.state == "completed":
        return Transition(state, value=old.receipt, replay=True)
    if action == "cancel" and job.state == "cancelled":
        return Transition(state, value=job, replay=True)
    allowed = ((action == "prepare" and job.state == "queued")
               or (action == "commit" and job.state == "running")
               or (action == "cancel" and job.state in ("queued", "running"))
               or (action == "retry" and job.state in ("cancelled", "failed")))
    if not allowed:
        return Transition(state, error="job_state")
    if action in ("prepare", "commit"):
        code = _validation_error(state, old.manifest)
        if code is not None:
            failed = replace(job, state="failed", completed=0, error=code)
            return Transition(_replace_job(state, JobRecord(failed, old.manifest)), error=code)
    if action == "prepare":
        running = replace(job, state="running")
        return Transition(_replace_job(state, JobRecord(running, old.manifest)), value=Token(job.job_id, job.epoch))
    if action == "commit":
        completed = replace(job, state="completed", completed=job.total)
        batch = tuple(document(e.source, e.text) for e in old.manifest)
        receipt = Receipt(completed, batch)
        sources = {d.source for d in state.documents}
        documents = state.documents + tuple(d for d in batch if d.source not in sources)
        after = _replace_job(ExpectedState(tuple(sorted(documents, key=lambda d: (d.source, d.document_id))), state.jobs),
                             JobRecord(completed, old.manifest, receipt))
        return Transition(after, value=receipt)
    successor: JobState = "cancelled" if action == "cancel" else "queued"
    changed = replace(job, epoch=job.epoch + 1, state=successor, completed=0, error=None)
    return Transition(_replace_job(state, JobRecord(changed, old.manifest)), value=changed)


def import_document(state: ExpectedState, source: str, text: str) -> Transition:
    """Known valid regular-file V0 import; no path/discovery classification."""
    value = document(source, text)
    _require(source.endswith((".txt", ".md")), "this fixture import models explicit V0 suffixes")
    old = next((d for d in state.documents if d.source == source), None)
    if old is not None:
        return (Transition(state, value=old, replay=True) if old.text == text
                else Transition(state, error="source_changed"))
    if len(state.documents) == 256:
        raise UnspecifiedFixture("full V0 catalog rejects import, but its exact error code is not prescribed")
    return Transition(ExpectedState(tuple(sorted(state.documents + (value,), key=lambda d: (d.source, d.document_id))),
                                    state.jobs), value=value)


def reopen(state: ExpectedState) -> ExpectedState:
    """Expected persistence identity only; proves no actual process/DB reopen."""
    return state


DOCUMENT_CORPUS = (Entry("Alpha.txt", "Straße [a.b]\n"), Entry("alpha.md", "Straße [a.b]\n"),
                   Entry("nested/é.md", "CAFÉ\r\n"), Entry("z.txt", ""))
CANONICAL_BATCH = (Entry("ns/z.txt", ""), Entry("ns/nested/a.md", "café\r\n"), Entry("ns/a.html", "<b>literal</b>\n"))
VALID_MANIFEST = (Entry("a.txt", "a"), Entry("b.txt", "b"))
FAILED_MANIFEST = (Entry("../bad.txt", "bad"), Entry("a.txt", "a"))
GUARD = Entry("guard.txt", "guard\n")


def corpus_state() -> ExpectedState:
    return ExpectedState(tuple(document(e.source, e.text) for e in DOCUMENT_CORPUS))


def guarded_state() -> ExpectedState:
    return submit(ExpectedState((document(GUARD.source, GUARD.text),)), "sentinel", ()).state


def expected_state(state: JobState, *, guarded: bool = True) -> ExpectedState:
    """Five literal base scenarios: epochs queued/running/completed/failed1, cancelled2."""
    _require(state in STATES, "unknown base state")
    initial = guarded_state() if guarded else ExpectedState()
    result = submit(initial, "subject", FAILED_MANIFEST if state == "failed" else VALID_MANIFEST).state
    if state in ("running", "completed", "failed"):
        result = apply_action(result, "prepare", "subject").state
    if state == "completed":
        result = apply_action(result, "commit", "subject", epoch=1).state
    if state == "cancelled":
        result = apply_action(result, "cancel", "subject").state
    return result


@dataclass(frozen=True)
class ZipMember:
    name: str
    data: bytes
    unix_mode: int = 0o100644

    def __post_init__(self) -> None:
        _require(type(self.name) is str and 1 <= len(_utf8(self.name)) <= 65535, "bounded ZIP name required")
        _require(type(self.data) is bytes and len(self.data) < 2 ** 32, "ZIP member bytes required")
        _require(type(self.unix_mode) is int and 0 <= self.unix_mode <= 65535, "ZIP Unix mode required")


def stored_zip(members: tuple[ZipMember, ...]) -> bytes:
    """Literal ZIP32 STORED bytes, preserving order and duplicates for negative fixtures.

    All names UTF-8, flags 0x0800, method0, DOS1980-01-01 00:00:00, Unix creator
    3/version20, extraction20, no extra/comment/descriptor/encryption/ZIP64.
    No extraction, compression library, clock, platform mode, or candidate code.
    """
    _require(type(members) is tuple and all(type(m) is ZipMember for m in members)
             and len(members) < 65535, "immutable ZIP32 members required")
    local = bytearray()
    central = bytearray()
    for member in members:
        name = _utf8(member.name)
        size = len(member.data)
        crc = binascii.crc32(member.data) & 0xffffffff
        offset = len(local)
        local.extend(struct.pack("<IHHHHHIIIHH", 0x04034b50, 20, 0x800, 0, 0, 33,
                                 crc, size, size, len(name), 0))
        local.extend(name)
        local.extend(member.data)
        external = (member.unix_mode << 16) | (0x10 if member.name.endswith("/") else 0)
        central.extend(struct.pack("<IHHHHHHIIIHHHHHII", 0x02014b50, 0x0314, 20, 0x800, 0, 0, 33,
                                   crc, size, size, len(name), 0, 0, 0, 0, external, offset))
        central.extend(name)
    _require(len(local) < 2 ** 32 and len(central) < 2 ** 32, "fixture ZIP32 size exceeded")
    end = struct.pack("<IHHHHIIH", 0x06054b50, 0, 0, len(members), len(members), len(central), len(local), 0)
    return bytes(local + central) + end


def json_bundle(entries: tuple[Entry, ...]) -> bytes:
    canonical_manifest(entries)  # shape check; preserve supplied order in bytes.
    return json.dumps({"entries": [e.as_json() for e in entries]}, ensure_ascii=False,
                      separators=(",", ":")).encode("utf-8")


def canonical_zip() -> bytes:
    return stored_zip(tuple(ZipMember(e.source.removeprefix("ns/"), _utf8(e.text)) for e in CANONICAL_BATCH))


def member_count_manifest(count: int, *, namespace: str = "ns") -> tuple[Entry, ...]:
    _require(type(count) is int and count in (0, 64, 65), "named member boundary is 0,64,65")
    _require(type(namespace) is str and _valid_key(namespace), "fixture namespace must be valid")
    return tuple(Entry(f"{namespace}/m{i:02d}.txt", "x") for i in range(count))


@dataclass(frozen=True)
class KeyBoundary:
    name: str
    namespace: str
    member: str

    def __post_init__(self) -> None:
        _require(type(self.name) is str and bool(self.name) and type(self.namespace) is str and type(self.member) is str,
                 "immutable named key boundary strings required")

    @property
    def source(self) -> str:
        return self.namespace + "/" + self.member

    @property
    def source_bytes(self) -> int:
        return len(_utf8(self.source))

    @property
    def segment_bytes(self) -> tuple[int, ...]:
        return tuple(len(_utf8(p)) for p in self.source.split("/"))


KEY_BOUNDARIES = (
    KeyBoundary("combined-bytes-256", "n" * 127, "é" * 62 + ".txt"),
    KeyBoundary("combined-bytes-257", "n" * 128, "é" * 62 + ".txt"),
    KeyBoundary("combined-segments-16", "ns", "/".join(("p",) * 14 + ("a.txt",))),
    KeyBoundary("combined-segments-17", "ns", "/".join(("p",) * 15 + ("a.txt",))),
)
DIRECT_SOURCE_BOUNDARIES = (
    ("bytes256", KEY_BOUNDARIES[0].source), ("bytes257", KEY_BOUNDARIES[1].source),
    ("segments16", "/".join(("p",) * 15 + ("a.txt",))),
    ("segments17", "/".join(("p",) * 16 + ("a.txt",))),
)
INVALID_NAMESPACES = (("empty", ""), ("absolute", "/n"), ("backslash", "n\\m"), ("nul", "n\0m"),
                      ("empty-segment", "n//m"), ("dot", "n/./m"), ("parent", "n/../m"),
                      ("trailing", "n/"), ("segment129", "n" * 129))


def request_body_boundary(length: int) -> bytes:
    _require(type(length) is int and length in (65536, 65537), "named POST byte boundary required")
    data = json.dumps({"job_id": "wire", "entries": [{"source": "wire.txt", "text": "é" * 4096}]},
                      ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return data + b" " * (length - len(data))


@dataclass(frozen=True)
class DataFile:
    path: str
    data: bytes

    def __post_init__(self) -> None:
        _require(type(self.path) is str and _valid_key(self.path), "owned staging file path must be valid")
        _require(type(self.data) is bytes, "immutable fixture file bytes required")

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.data).hexdigest()


def intake_files() -> tuple[DataFile, ...]:
    """Literal files for the catalog's intake, count, path and wire families.

    Paths are staging descriptors only. Empty directory descriptors, symlinks,
    server roots and wrong-kind arrangements require separate qualified mechanics.
    """
    files = [DataFile("batch.zip", canonical_zip()), DataFile("batch.json", json_bundle(CANONICAL_BATCH)),
             DataFile("empty.zip", stored_zip(())), DataFile("empty.json", json_bundle(())),
             DataFile("invalid.zip", b"not a ZIP"), DataFile("invalid.json", b'{"entries":['),
             DataFile("schema.json", b'{"entries":[],"extra":1}'),
             DataFile("bad-utf8.zip", stored_zip((ZipMember("a.txt", b"\xff"),))),
             DataFile("bad-utf8/a.txt", b"\xff"),
             DataFile("duplicate.zip", stored_zip((ZipMember("a.txt", b"a"), ZipMember("a.txt", b"a")))),
             DataFile("symlink.zip", stored_zip((ZipMember("a.txt", b"target.txt", 0o120777),))),
             DataFile("fifo.zip", stored_zip((ZipMember("a.txt", b"", 0o010644),))),
             DataFile("failed.json", json_bundle(FAILED_MANIFEST)),
             DataFile("unsupported.json", json_bundle((Entry("bad.exe", "bad"),))),
             DataFile("post65536.json", request_body_boundary(65536)),
             DataFile("post65537.json", request_body_boundary(65537))]
    files.extend(DataFile("batch/" + e.source.removeprefix("ns/"), _utf8(e.text)) for e in CANONICAL_BATCH)
    for count in (64, 65):
        entries = member_count_manifest(count)
        files.append(DataFile(f"count{count}.json", json_bundle(entries)))
        files.append(DataFile(f"count{count}.zip", stored_zip(tuple(
            ZipMember(e.source.removeprefix("ns/"), _utf8(e.text)) for e in entries))))
        files.extend(DataFile(f"count{count}/" + e.source.removeprefix("ns/"), _utf8(e.text)) for e in entries)
    return tuple(files)


EMPTY_DIRECTORIES = ("empty",)
