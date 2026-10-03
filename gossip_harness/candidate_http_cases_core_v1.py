"""Closed literal C03 declarations and pure source-derived history authoring.

No dispatch, filesystem staging, candidate imports, or acceptance authority.
The versioned declaration includes mechanics that frozen v2 cannot perform.
"""
from __future__ import annotations

import base64
from dataclasses import dataclass, field
import hashlib
import json
import posixpath
import re
from typing import Any

from . import candidate_http_fixtures_v1 as fixtures
from . import candidate_http_semantics_v1 as semantics
from . import candidate_http_relations_v1 as relations
from . import candidate_http_transport_v1 as wire

PROTOCOL = "candidate-c03-http-literal-cases-v1"
PORT = 18765
DATABASE = "/tmp/catalog.sqlite"
MAX_DECLARED_STEPS = 4096
_MAX_FILE_BYTES = 8 * 1024 * 1024
_TARGETS = frozenset(("V0-HTTP-01", "V0-HTTP-02", "V0-HTTP-03", "V0-HTTP-04",
                     "M1-HTTP-01", "M1-HTTP-02", "M1-I30", "M1-I31"))
_INTERACTIONS = frozenset(("V0-CLI-03", "M1-A08"))
_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
_ROW = re.compile(r"HTTP-[A-Z-]+/[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
_CITATIONS = ("V0-HTTP", "V0-QUERY-EXPORT", "M1-JOBS", "M1-STORE", "M1-HTTP-BODIES")


class CatalogError(ValueError):
    """Invalid evaluator declaration, never a candidate failure."""


def require(value: bool, message: str) -> None:
    if not value:
        raise CatalogError(message)


def encode(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def _binary(raw: bytes) -> dict[str, Any]:
    return {"base64": base64.b64encode(raw).decode("ascii"), "bytes": len(raw),
            "sha256": hashlib.sha256(raw).hexdigest()}


def _path(path: str) -> None:
    require(type(path) is str and 1 <= len(path.encode("utf-8")) <= 4096
            and "\\" not in path and "\0" not in path and not path.startswith("/")
            and len(path.split("/")) <= 256
            and all(part not in ("", ".", "..") and len(part.encode("utf-8")) <= 255
                    for part in path.split("/")), "Unsafe owned fixture path")


def _root(path: str) -> None:
    require(type(path) is str and (path == "/inputs" or path.startswith("/inputs/")),
            "Root must be the owned input mount or its literal descendant")
    if path != "/inputs":
        _path(path[8:])


def _id(value: str) -> None:
    require(type(value) is str and _ID.fullmatch(value) is not None, "Invalid step identifier")


@dataclass(frozen=True, slots=True)
class Fixture:
    path: str
    kind: str
    data: bytes = b""
    target: str = ""

    def __post_init__(self) -> None:
        _path(self.path)
        require(self.kind in ("file", "directory", "symlink") and type(self.kind) is str,
                "Closed fixture kind required")
        require(type(self.data) is bytes and len(self.data) <= _MAX_FILE_BYTES
                and type(self.target) is str, "Immutable fixture payload required")
        if self.kind == "file":
            require(not self.target, "Files cannot carry link targets")
        elif self.kind == "directory":
            require(not self.data and not self.target, "Directory has no payload")
        else:
            require(not self.data and bool(self.target) and not self.target.startswith("/")
                    and "\\" not in self.target and "\0" not in self.target
                    and len(self.target.encode()) <= 4096, "Relative data-only link target required")
            resolved = posixpath.normpath(posixpath.join(posixpath.dirname(self.path), self.target))
            require(resolved not in ("", ".", "..") and not resolved.startswith("../"),
                    "Link target escapes owned /inputs staging")
            _path(resolved)

    def record(self) -> dict[str, Any]:
        return {"path": self.path, "kind": self.kind,
                "content": _binary(self.data) if self.kind == "file" else None,
                "target": self.target if self.kind == "symlink" else None}


@dataclass(frozen=True, slots=True)
class HttpRequest:
    method: str
    target: str
    headers: tuple[tuple[str, str], ...]
    body: bytes
    port: int = PORT

    def __post_init__(self) -> None:
        require(type(self.headers) is tuple and all(type(p) is tuple and len(p) == 2
                and all(type(v) is str for v in p) for p in self.headers)
                and type(self.body) is bytes, "Immutable request bytes/header pairs required")
        wire.request_bytes(self.recipe(), self.port)

    def recipe(self) -> dict[str, Any]:
        return {"method": self.method, "target": self.target,
                "headers": [list(pair) for pair in self.headers],
                "body_b64": base64.b64encode(self.body).decode("ascii")}

    def wire_bytes(self) -> bytes:
        return wire.request_bytes(self.recipe(), self.port)

    def record(self) -> dict[str, Any]:
        return {"method": self.method, "target": self.target,
                "headers": [list(pair) for pair in self.headers], "body": _binary(self.body),
                "port": self.port, "wire": _binary(self.wire_bytes())}


@dataclass(frozen=True, slots=True)
class Dependency:
    step_id: str
    facet: str = "body_shape_value"

    def __post_init__(self) -> None:
        _id(self.step_id)
        require(self.facet in ("body_shape_value", "status", "json_syntax"), "Unknown declaration prerequisite facet")

    def record(self) -> dict[str, str]:
        return {"step_id": self.step_id, "facet": self.facet}


def _relation_record(raw: bytes) -> dict[str, Any]:
    require(type(raw) is bytes, "Relation must be immutable canonical JSON bytes")
    obj = json.loads(raw)
    require(encode(obj) == raw and type(obj) is dict
            and set(obj) == {"protocol", "kind", "jobs_step_id", "jobs"}
            and obj["kind"] == "jobs-with-related-errors"
            and obj["protocol"] == "candidate-http-relations-v1", "Closed jobs relation required")
    _id(obj["jobs_step_id"])
    require(type(obj["jobs"]) is list, "Literal jobs array required")
    rules = []
    for row in obj["jobs"]:
        require(type(row) is dict and set(row) == {"job_id", "epoch", "state", "total", "completed", "error"},
                "Exact six-field relation JOB required")
        error = row["error"]
        if type(error) is dict:
            require(set(error) == {"from_error_step", "expected_status"}, "Closed source error marker required")
            _id(error["from_error_step"])
            error = relations.ErrorReference(error["from_error_step"], error["expected_status"])
        rules.append(relations.JobRule(row["job_id"], row["epoch"], row["state"],
                                       row["total"], row["completed"], error))
    typed = relations.JobsExpectation(obj["jobs_step_id"], tuple(rules))
    require(typed.record() == obj, "Relation declaration must round-trip the registered typed protocol")
    return obj


@dataclass(frozen=True, slots=True)
class HttpExpectation:
    semantic: semantics.Expectation | None = None
    relation_json: bytes = b""
    raw_facts_only: bool = False
    citations: tuple[str, ...] = _CITATIONS
    content_requires: tuple[Dependency, ...] = ()

    def __post_init__(self) -> None:
        require((self.semantic is None or type(self.semantic) is semantics.Expectation)
                and type(self.relation_json) is bytes and type(self.raw_facts_only) is bool,
                "Typed immutable expectation required")
        require(sum((self.semantic is not None, bool(self.relation_json), self.raw_facts_only)) == 1,
                "Exactly one semantic, relation or raw-facts expectation required")
        require(type(self.citations) is tuple and bool(self.citations)
                and all(type(x) is str and bool(x) for x in self.citations), "Citations required")
        require(type(self.content_requires) is tuple
                and all(type(x) is Dependency for x in self.content_requires), "Immutable prerequisites required")
        if self.relation_json:
            _relation_record(self.relation_json)

    def record(self) -> dict[str, Any]:
        semantic = self.semantic
        return {"kind": "semantic" if semantic is not None else "jobs_relation" if self.relation_json else "raw_facts_only",
                "semantic": {"shape": semantic.shape, "status": semantic.status, "code": semantic.code,
                             "expected_body": _binary(semantic.expected_body) if semantic.expected_body is not None else None}
                if semantic is not None else None,
                "relation": _relation_record(self.relation_json) if self.relation_json else None,
                "citations": list(self.citations),
                "content_requires": [x.record() for x in self.content_requires],
                "independent_status_and_syntax": not self.raw_facts_only}


def _public_state(state: fixtures.ExpectedState) -> dict[str, Any]:
    return {"jobs": state.jobs_json(), "documents": [doc.as_json() for doc in state.documents],
            "export": state.export()}


def _cli_state_record(raw: bytes) -> dict[str, Any]:
    require(type(raw) is bytes and bool(raw), "Every CLI step needs an inert known public-state snapshot")
    obj = json.loads(raw)
    require(encode(obj) == raw and type(obj) is dict and set(obj) == {"before", "after"},
            "Closed canonical CLI snapshots required")
    for phase in ("before", "after"):
        state = obj[phase]
        require(type(state) is dict and set(state) == {"jobs", "documents", "export"}
                and type(state["documents"]) is list, "Closed public-state snapshot required")
        semantics.success("jobs", encode(state["jobs"]))
        semantics.success("export", encode(state["export"]))
        for document in state["documents"]:
            semantics.success("document", encode(document))
    return obj


@dataclass(frozen=True, slots=True)
class Step:
    step_id: str
    kind: str
    epoch: int
    root: str
    argv: tuple[str, ...] = ()
    request: HttpRequest | None = None
    expectation: HttpExpectation | None = None
    cli_state_json: bytes = b""

    def __post_init__(self) -> None:
        _id(self.step_id)
        _root(self.root)
        require(self.kind in ("start", "request", "stop", "cli") and type(self.kind) is str,
                "Closed literal lifecycle kind required")
        require(type(self.epoch) is int and self.epoch >= 1
                and type(self.argv) is tuple and all(type(x) is str and "\0" not in x for x in self.argv),
                "Literal epoch and argv required")
        require(type(self.cli_state_json) is bytes, "Immutable CLI state declaration required")
        if self.kind == "cli":
            _cli_state_record(self.cli_state_json)
        else:
            require(not self.cli_state_json, "Only CLI steps carry CLI state snapshots")
        if self.kind == "request":
            require(type(self.request) is HttpRequest and type(self.expectation) is HttpExpectation
                    and not self.argv, "Every request needs exact bytes and one expectation")
            require(self.request is not None and self.request.port == PORT, "Request port must match declared server/listener port")
        else:
            require(self.request is None and self.expectation is None, "Only requests have HTTP payloads")
            if self.kind in ("start", "cli"):
                require(self.argv[:3] == ("python", "-m", "library") and self.argv[3:7]
                        == ("--db", DATABASE, "--root", self.root), "Literal source/db/root-bound argv required")
                require(len(self.argv) >= 8, "Missing candidate command")
                if self.kind == "start":
                    require(self.argv[7:] == ("serve", "--port", str(PORT)), "Fixed explicit server port required")
            else:
                require(not self.argv, "Stop is an owned lifecycle operation, not a command")

    def record(self) -> dict[str, Any]:
        return {"step_id": self.step_id, "kind": self.kind, "server_epoch": self.epoch,
                "root": self.root, "database": DATABASE, "source_ref": "same-declared-candidate-source",
                "volume_ref": "same-owned-history-volume", "keeper_ref": "continuous-owned-history-keeper",
                "argv": list(self.argv), "request": self.request.record() if self.request else None,
                "expectation": self.expectation.record() if self.expectation else None,
                "lifecycle": "stop-and-remove-before-successor" if self.kind == "stop" else None,
                "cli_expectation": {"exit": 0, "stdout": "one-complete-JSON-value",
                                    "exact_outer_wrapper": "unspecified", "stderr_exact_bytes": "unspecified",
                                    "public_state": _cli_state_record(self.cli_state_json),
                                    "state_interpretation_authority": "prospective-value-only-no-CLI-wrapper-adapter"}
                if self.kind == "cli" else None,
                "listener_observation": {"port": PORT, "families": ["IPv4", "IPv6"],
                                         "phases": ["connected_pre_request", "post_observation"],
                                         "required_address": "127.0.0.1",
                                         "scope": "observed-source-bound-server-epoch-interval"}
                if self.kind == "request" else None}


@dataclass(frozen=True, slots=True)
class LiteralCase:
    row_id: str
    requirement_ids: tuple[str, ...]
    interaction_ids: tuple[str, ...]
    fixtures: tuple[Fixture, ...]
    steps: tuple[Step, ...]
    notes: tuple[str, ...] = ()
    protocol: str = field(default=PROTOCOL, init=False)
    dispatch_authority: bool = field(default=False, init=False)
    acceptance_authority: bool = field(default=False, init=False)
    fresh_execution: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        require(type(self.row_id) is str and _ROW.fullmatch(self.row_id) is not None, "Exact retained FAMILY/row ID required")
        require(type(self.requirement_ids) is tuple and bool(self.requirement_ids)
                and len(set(self.requirement_ids)) == len(self.requirement_ids)
                and all(x in _TARGETS for x in self.requirement_ids), "Known unique target labels required")
        require(type(self.interaction_ids) is tuple and len(set(self.interaction_ids)) == len(self.interaction_ids)
                and all(x in _INTERACTIONS for x in self.interaction_ids), "Known separate interaction labels required")
        require(type(self.fixtures) is tuple and all(type(x) is Fixture for x in self.fixtures)
                and len(self.fixtures) <= 4096 and sum(len(x.data) for x in self.fixtures) <= _MAX_FILE_BYTES,
                "Bounded immutable staging descriptors required")
        paths = {x.path: x for x in self.fixtures}
        require(len(paths) == len(self.fixtures), "Duplicate fixture destination")
        for item in self.fixtures:
            parent = posixpath.dirname(item.path)
            while parent:
                require(parent in paths and paths[parent].kind == "directory", "Explicit regular parent directories required")
                parent = posixpath.dirname(parent)
            if item.kind == "symlink":
                # Resolve the raw components before normalization: a/linked/..
                # may escape even when normpath makes the final string safe.
                parent_parts = item.path.split("/")[:-1]
                components = item.target.split("/")
                for index, part in enumerate(components):
                    if part in ("", "."):
                        continue
                    if part == "..":
                        require(bool(parent_parts), "Raw link traversal escapes owned input mount")
                        parent_parts.pop()
                        continue
                    parent_parts.append(part)
                    intermediate = "/".join(parent_parts)
                    require(intermediate in paths and paths[intermediate].kind != "symlink",
                            "Raw link traversal must use explicit regular owned fixtures")
                    if index != len(components) - 1:
                        require(paths[intermediate].kind == "directory", "Raw link ancestor must be a regular directory")
                target = posixpath.normpath(posixpath.join(posixpath.dirname(item.path), item.target))
                require(target in paths and paths[target].kind != "symlink", "Link target must be an explicit regular owned fixture")
        require(type(self.steps) is tuple and 3 <= len(self.steps) <= MAX_DECLARED_STEPS
                and all(type(x) is Step for x in self.steps), "Complete bounded literal lifecycle required")
        require(type(self.notes) is tuple and all(type(x) is str and bool(x) for x in self.notes), "Immutable notes required")
        active = False
        epoch = 0
        root = ""
        earlier: dict[str, Step] = {}
        for step in self.steps:
            require(step.root == "/inputs" or step.root[8:] in paths and paths[step.root[8:]].kind == "directory",
                    "Descendant root must be an explicit regular fixture directory")
            require(step.step_id not in earlier, "Repeated step ID")
            if step.kind == "start":
                require(not active and step.epoch == epoch + 1, "Server epoch order differs")
                active = True
                epoch = step.epoch
                root = step.root
            elif step.kind == "stop":
                require(active and step.epoch == epoch and step.root == root, "Stop not bound to current epoch")
                active = False
            elif step.kind == "cli":
                require(not active and step.epoch == epoch and step.root == root,
                        "CLI must follow removed server on same root/source/database lineage")
            else:
                require(active and step.epoch == epoch and step.root == root,
                        "Request not bound to current server epoch")
                expectation = step.expectation
                if expectation is None:
                    raise CatalogError("Missing request expectation")
                for dep in expectation.content_requires:
                    require(dep.step_id in earlier and earlier[dep.step_id].kind == "request",
                            "Content prerequisite must refer to earlier HTTP evidence")
                    source = earlier[dep.step_id].expectation
                    require(source is not None and not source.raw_facts_only,
                            "Prerequisite facet is absent from raw-facts-only expectation")
                if expectation.relation_json:
                    relation = _relation_record(expectation.relation_json)
                    require(relation["jobs_step_id"] == step.step_id, "Relation census step differs")
                    for job in relation["jobs"]:
                        if type(job["error"]) is dict:
                            ref = job["error"]["from_error_step"]
                            require(ref in earlier and earlier[ref].kind == "request",
                                    "Relation must name an earlier error response")
                            source = earlier[ref].expectation
                            require(source is not None and source.semantic is not None and source.semantic.shape == "error"
                                    and source.semantic.status == job["error"]["expected_status"],
                                    "Relation source must declare the same supported error response status")
            earlier[step.step_id] = step
        require(not active and epoch >= 1, "Complete case must stop/remove final server")
        require(any(x.kind == "request" for x in self.steps), "Case has no HTTP request")

    @property
    def family_id(self) -> str:
        return self.row_id.split("/", 1)[0]

    @property
    def counts(self) -> dict[str, int]:
        requests = sum(x.kind == "request" for x in self.steps)
        servers = sum(x.kind == "start" for x in self.steps)
        cli = sum(x.kind == "cli" for x in self.steps)
        return {"steps": len(self.steps), "http_requests": requests, "server_epochs": servers,
                "server_stops": sum(x.kind == "stop" for x in self.steps), "finite_cli_processes": cli,
                "planned_probe_processes": requests, "planned_keeper_processes": 1,
                "planned_total_processes": requests + servers + cli + 1,
                "fixture_files": sum(x.kind == "file" for x in self.fixtures),
                "fixture_directories": sum(x.kind == "directory" for x in self.fixtures),
                "fixture_symlinks": sum(x.kind == "symlink" for x in self.fixtures),
                "fixture_bytes": sum(len(x.data) for x in self.fixtures),
                "request_body_bytes": sum(len(x.request.body) for x in self.steps if x.request),
                "request_wire_bytes": sum(len(x.request.wire_bytes()) for x in self.steps if x.request)}

    @property
    def required_mechanics(self) -> tuple[str, ...]:
        needed = ["qualified-prospective-v3-bridge", "source-runtime-purpose-registration", "continuous-keeper",
                  "prerequisite-attribution", "listener-IPv4-IPv6"]
        if any(s.root != "/inputs" for s in self.steps):
            needed.append("literal-descendant-root")
        if len({s.root for s in self.steps}) > 1:
            needed.append("per-epoch-root-argv")
        if sum(s.kind == "start" for s in self.steps) > 1:
            needed.append("same-db-source-epoch-restart")
        if any(x.kind == "symlink" for x in self.fixtures):
            needed.append("confined-data-links-lstat-readlink")
        if any(s.kind == "cli" for s in self.steps):
            needed.append("authorized-qualified-same-db-CLI-handoff")
        if len(self.steps) > 64:
            needed.append("versioned-step-bound-above-v2-64")
        if any(s.expectation and s.expectation.raw_facts_only for s in self.steps):
            needed.append("non-JSON-raw-fact-observation")
        if any(s.expectation and s.expectation.relation_json for s in self.steps):
            needed.append("registered-jobs-error-relation")
        return tuple(needed)

    def record(self) -> dict[str, Any]:
        return {"protocol": self.protocol, "row_id": self.row_id, "family_id": self.family_id,
                "requirement_ids": list(self.requirement_ids), "interaction_ids": list(self.interaction_ids),
                "fixtures": [x.record() for x in self.fixtures], "steps": [x.record() for x in self.steps],
                "notes": list(self.notes), "counts": self.counts, "required_mechanics": list(self.required_mechanics),
                "dispatch_authority": False, "acceptance_authority": False, "fresh_execution": False,
                "v2_CasePlan_compatible": False}

    @property
    def definition_sha256(self) -> str:
        return hashlib.sha256(encode(self.record())).hexdigest()


class Builder:
    """Mutable authoring helper; only immutable LiteralCase values leave finish()."""
    def __init__(self, row_id: str, requirement_ids: tuple[str, ...], interaction_ids: tuple[str, ...] = (),
                 root: str = "/inputs") -> None:
        _root(root)
        self.row_id = row_id
        self.requirement_ids = requirement_ids
        self.interaction_ids = interaction_ids
        self.root = root
        self.port = PORT
        self.database = DATABASE
        self.state = fixtures.ExpectedState()
        self._fixtures: dict[str, Fixture] = {}
        self._steps: list[Step] = []
        self._notes: list[str] = []
        self._epoch = 0
        self._active = False
        self._anchors: tuple[Dependency, ...] = ()
        self._partial_jobs: bytes = b""

    def _next(self, label: str) -> str:
        safe = re.sub(r"[^A-Za-z0-9_.:-]", "-", label)[:80] or "step"
        return f"s{len(self._steps):04d}-{safe}"

    def _add(self, fixture: Fixture) -> None:
        old = self._fixtures.get(fixture.path)
        require(old is None or old == fixture, "Immutable fixture collision")
        self._fixtures[fixture.path] = fixture
        parent = posixpath.dirname(fixture.path)
        while parent:
            directory = Fixture(parent, "directory")
            old = self._fixtures.get(parent)
            require(old is None or old == directory, "Non-directory fixture ancestor")
            self._fixtures[parent] = directory
            parent = posixpath.dirname(parent)

    def add_file(self, path: str, data: bytes) -> None:
        self._add(Fixture(path, "file", data))

    def add_directory(self, path: str) -> None:
        self._add(Fixture(path, "directory"))

    def add_link(self, path: str, target: str) -> None:
        self._add(Fixture(path, "symlink", target=target))

    def note(self, text: str) -> None:
        require(type(text) is str and bool(text), "Nonempty note required")
        self._notes.append(text)

    def start(self, root: str | None = None) -> str:
        require(not self._active, "Server already active")
        if root is not None:
            _root(root)
            self.root = root
        if self.root != "/inputs":
            self.add_directory(self.root[8:])
        self._epoch += 1
        self._active = True
        sid = self._next("start")
        self._steps.append(Step(sid, "start", self._epoch, self.root,
            ("python", "-m", "library", "--db", self.database, "--root", self.root, "serve", "--port", str(self.port))))
        return sid

    def stop(self) -> str:
        require(self._active, "No active server")
        sid = self._next("stop")
        self._steps.append(Step(sid, "stop", self._epoch, self.root))
        self._active = False
        return sid

    def _literal(self, method: str, target: str, body: bytes, content_type: str | None,
                 headers: tuple[tuple[str, str], ...] | None) -> HttpRequest:
        if headers is None:
            pairs = [("Host", f"127.0.0.1:{self.port}"), ("Connection", "close")]
            if method == "POST" or body:
                if content_type is not None:
                    pairs.append(("Content-Type", content_type))
                pairs.append(("Content-Length", str(len(body))))
            headers = tuple(pairs)
        return HttpRequest(method, target, headers, body, self.port)

    def _probe(self, request: HttpRequest, expectation: HttpExpectation, label: str) -> str:
        require(self._active, "HTTP request requires active server")
        sid = self._next(label)
        self._steps.append(Step(sid, "request", self._epoch, self.root, request=request, expectation=expectation))
        return sid

    def request(self, method: str, target: str, expectation: semantics.Expectation | None, *,
                body: bytes = b"", content_type: str | None = "application/json",
                headers: tuple[tuple[str, str], ...] | None = None, label: str = "request",
                after: fixtures.ExpectedState | None = None, check: bool = True) -> str:
        require(not self._partial_jobs, "No further generic request after partial-state declaration")
        if check:
            self.census(label + "-before")
        expected = HttpExpectation(semantic=expectation, raw_facts_only=expectation is None,
                                   content_requires=self._anchors)
        sid = self._probe(self._literal(method, target, body, content_type, headers), expected, label)
        if after is not None:
            require(type(after) is fixtures.ExpectedState, "Exact independently derived after state required")
            self.state = after
        if check:
            self.census(label + "-after")
        return sid

    def post(self, target: str, value: Any, expectation: semantics.Expectation, **kwargs: Any) -> str:
        return self.request("POST", target, expectation, body=encode(value), **kwargs)

    def census(self, label: str = "census") -> tuple[str, ...]:
        require(self._active, "HTTP census requires server")
        requests: list[str] = []
        prior = self._anchors
        sid = self._next(label + "-jobs")
        if self._partial_jobs:
            relation = json.loads(self._partial_jobs)
            relation["jobs_step_id"] = sid
            exp = HttpExpectation(relation_json=encode(relation), content_requires=prior)
        else:
            exp = HttpExpectation(semantic=semantics.success("jobs", encode(self.state.jobs_json())), content_requires=prior)
        requests.append(self._probe(self._literal("GET", "/api/jobs", b"", None, None), exp, label + "-jobs"))
        for offset in self.state.census_offsets():
            requests.append(self._probe(self._literal("GET", f"/api/documents?offset={offset}&limit=100", b"", None, None),
                HttpExpectation(semantic=semantics.success("documents", encode(self.state.listing(offset=offset))),
                                content_requires=prior), label + f"-documents-{offset}"))
        requests.append(self._probe(self._literal("GET", "/api/export", b"", None, None),
            HttpExpectation(semantic=semantics.success("export", encode(self.state.export())), content_requires=prior), label + "-export"))
        self._anchors = tuple(Dependency(sid) for sid in requests)
        return tuple(requests)

    def _transition_expectation(self, transition: fixtures.Transition) -> semantics.Expectation:
        return semantics.classified_error(transition.error) if transition.error else semantics.success("unspecified")

    def import_source(self, source: str, text: str, label: str = "import") -> str:
        prefix = "" if self.root == "/inputs" else self.root[8:] + "/"
        self.add_file(prefix + source, text.encode("utf-8"))
        transition = fixtures.import_document(self.state, source, text)
        return self.post("/api/import", {"source": source}, self._transition_expectation(transition),
                         after=transition.state, label=label)

    def submit_job(self, job_id: str, entries: tuple[fixtures.Entry, ...], label: str = "submit") -> str:
        transition = fixtures.submit(self.state, job_id, entries)
        return self.post("/api/jobs", {"job_id": job_id, "entries": [e.as_json() for e in entries]},
                         self._transition_expectation(transition), after=transition.state, label=label)

    def action(self, action: fixtures.Action, job_id: str, epoch: int | None = None, label: str = "action") -> str:
        transition = fixtures.apply_action(self.state, action, job_id, epoch=epoch)
        body = {"epoch": epoch} if action == "commit" else {}
        return self.post(f"/api/jobs/{job_id}/{action}", body, self._transition_expectation(transition),
                         after=transition.state, label=label + "-" + action)

    def guard(self) -> None:
        require(self.state == fixtures.ExpectedState(), "Guard setup requires initially empty state")
        self.import_source(fixtures.GUARD.source, fixtures.GUARD.text, label="guard-import")
        self.submit_job("sentinel", (), label="guard-job")

    def corpus(self) -> None:
        require(self.state == fixtures.ExpectedState(), "Corpus setup requires initially empty state")
        for index, entry in enumerate(fixtures.DOCUMENT_CORPUS):
            self.import_source(entry.source, entry.text, label=f"corpus-{index}")

    def partial_failure(self, job_id: str, error_step_id: str, label: str = "partial-failure") -> tuple[str, ...]:
        require(not self._partial_jobs and self.state.get(job_id) is not None, "Known queued subject required")
        rows = [record.job.as_json() for record in self.state.jobs]
        for row in rows:
            if row["job_id"] == job_id:
                require(row["state"] == "queued", "Deferred failure subject must be queued")
                row.update(state="failed", completed=0,
                           error={"from_error_step": error_step_id, "expected_status": 400})
        self._partial_jobs = encode({"protocol": "candidate-http-relations-v1", "kind": "jobs-with-related-errors", "jobs_step_id": "pending", "jobs": rows})
        return self.census(label)

    def cli(self, command: tuple[str, ...], *, after: fixtures.ExpectedState | None = None, label: str = "cli") -> str:
        require(not self._active and self._epoch >= 1 and not self._partial_jobs
                and type(command) is tuple and bool(command), "CLI requires a stopped owned HTTP epoch")
        sid = self._next(label)
        if after is not None:
            require(type(after) is fixtures.ExpectedState, "Known CLI after state required")
        next_state = after if after is not None else self.state
        snapshots = encode({"before": _public_state(self.state), "after": _public_state(next_state)})
        self._steps.append(Step(sid, "cli", self._epoch, self.root,
            ("python", "-m", "library", "--db", self.database, "--root", self.root, *command),
            cli_state_json=snapshots))
        self.state = next_state
        return sid

    def finish(self) -> LiteralCase:
        if self._active:
            self.stop()
        return LiteralCase(self.row_id, self.requirement_ids, self.interaction_ids,
                           tuple(self._fixtures[p] for p in sorted(self._fixtures)), tuple(self._steps), tuple(self._notes))
