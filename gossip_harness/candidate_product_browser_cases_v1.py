"""Closed, prospective M1 browser actions; earlier browser suites are unchanged."""
from __future__ import annotations
from dataclasses import dataclass
import hashlib
import io
import json
from typing import Any
import zipfile
from . import candidate_product_browser_fixture_v1 as fixtures

PROTOCOL = "candidate-product-browser-cases-v1"
ORIGINAL_PURPOSE = "public_product_definition"
CONTRACT_SHA256 = "2d88ce0775888f148b0ec3caf90b3d5c82d8fed71f53bec5f7e75f492ae998dc"
PORT = 8765
ORIGIN = "http://127.0.0.1:8765"
SERVER_ARGV = ("python", "-B", "-m", "library", "--db", "/tmp/library.sqlite", "--root", "/inputs", "serve", "--port", "8765")
LITERAL = '<img src=x onerror="window.__candidate_browser_injected=true"> literal browser text'
DOCUMENTS = (("archive/z.txt", "browser archive omega"), ("directory/a.txt", "browser directory alpha"),
             ("directory/b.md", "browser directory beta"), ("json/literal.html", LITERAL),
             ("one.txt", "browser single-file import"))


def encoded(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def inputs() -> dict[str, bytes]:
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_STORED) as out:
        info = zipfile.ZipInfo("z.txt", (2020, 1, 1, 0, 0, 0))
        info.external_attr = 0o100644 << 16
        out.writestr(info, DOCUMENTS[0][1].encode())
    return {"folder/a.txt": DOCUMENTS[1][1].encode(), "folder/b.md": DOCUMENTS[2][1].encode(),
            "bundle.zip": archive.getvalue(), "bundle.json": encoded({"entries": [{"source": DOCUMENTS[3][0], "text": LITERAL}]}),
            "one.txt": DOCUMENTS[4][1].encode(),
            "bad.json": encoded({"entries": [{"source": "bad.bin", "text": "unsupported deferred entry"}]})}


def job(identifier: str, state: str, epoch: int, total: int, error: str | None = None) -> dict[str, Any]:
    return {"job_id": identifier, "state": state, "epoch": epoch, "total": total,
            "completed": total if state == "completed" else 0, "error": error}


def _definitions() -> tuple[dict[str, Any], ...]:
    first: list[dict[str, Any]] = []
    def add(op: str, **values: Any) -> None:
        first.append({"id": "a" + str(len(first)).zfill(3), "op": op, **values})
    add("open", jobs=[], documents=[])
    identifier = "browser_directory"
    add("submit", kind="directory", path="folder", namespace="directory", job=job(identifier, "queued", 1, 2), documents=[])
    add("action", action="prepare", job=job(identifier, "running", 1, 2), documents=[])
    add("action", action="cancel", job=job(identifier, "cancelled", 2, 2), documents=[])
    add("reload", jobs=[job(identifier, "cancelled", 2, 2)], documents=[])
    add("action", action="retry", job=job(identifier, "queued", 3, 2), documents=[])
    add("action", action="prepare", job=job(identifier, "running", 3, 2), documents=[])
    docs = [list(pair) for pair in DOCUMENTS[1:3]]
    add("action", action="commit", job=job(identifier, "completed", 3, 2), documents=docs)
    jobs = [job(identifier, "completed", 3, 2)]
    for ident, kind, path, namespace, document in (("browser_zip", "zip", "bundle.zip", "archive", DOCUMENTS[0]),
                                                 ("browser_json", "json", "bundle.json", None, DOCUMENTS[3])):
        add("submit", kind=kind, path=path, namespace=namespace, job=job(ident, "queued", 1, 1), documents=list(docs))
        add("action", action="prepare", job=job(ident, "running", 1, 1), documents=list(docs))
        docs = sorted([*docs, list(document)])
        add("action", action="commit", job=job(ident, "completed", 1, 1), documents=list(docs))
        jobs.append(job(ident, "completed", 1, 1))
    add("import", path="one.txt", documents=[list(pair) for pair in DOCUMENTS])
    add("search_open", query="literal browser text", source=DOCUMENTS[3][0], literal=LITERAL,
        visible_documents=[DOCUMENTS[3][0]], documents=[list(pair) for pair in DOCUMENTS])
    add("submit_error", identifier="browser_invalid", kind="json", path="missing.json", namespace=None,
        error="io_error", visible_documents=[DOCUMENTS[3][0]], jobs=sorted(jobs, key=lambda value: value["job_id"]), documents=[list(pair) for pair in DOCUMENTS])
    add("clear_search", replaces_error="io_error", documents=[list(pair) for pair in DOCUMENTS])
    add("fresh_context", jobs=sorted(jobs, key=lambda value: value["job_id"]), documents=[list(pair) for pair in DOCUMENTS])
    second: list[dict[str, Any]] = []
    def badadd(op: str, **values: Any) -> None:
        second.append({"id": "a" + str(len(second)).zfill(3), "op": op, **values})
    ident = "browser_failed"
    badadd("open", jobs=[], documents=[])
    badadd("submit", kind="json", path="bad.json", namespace=None, job=job(ident, "queued", 1, 1), documents=[])
    badadd("action", action="prepare", job=job(ident, "failed", 1, 1, "unsupported_type"), error="unsupported_type", documents=[])
    badadd("action", action="retry", job=job(ident, "queued", 2, 1), documents=[])
    badadd("action", action="prepare", job=job(ident, "failed", 2, 1, "unsupported_type"), error="unsupported_type", documents=[])
    badadd("reload", jobs=[job(ident, "failed", 2, 1, "unsupported_type")], documents=[])
    return tuple({"protocol": PROTOCOL, "identifier": name, "requirements": ["M1-UI-AUTOMATION-V4"],
                  "original_definition_purpose": ORIGINAL_PURPOSE, "actions": actions, "seed_database": None,
                  "server_argv": list(SERVER_ARGV), "port": PORT,
                  "facets": [a["id"] + ":" + facet for a in actions for facet in ("dom", "api", "mutation")],
                  "unqualified": ["native-browser-network", "application-server-restart", "M2-M4-browser-workflows",
                                  "counter-above-2pow53-until-public-fixture-review", "global-independent-purpose"]}
                 for name, actions in (("m1-browser-intake-state-literal-reload", first),
                                       ("m1-browser-failed-job-action-matrix", second)))


def _counter_definitions() -> tuple[dict[str, Any], ...]:
    result = []
    for name, epoch in (("high", 9007199254740993), ("maximum", 9223372036854775807)):
        identifier = "browser_counter"
        actions: list[dict[str, Any]] = [{"id": "a000", "op": "open", "jobs": [job(identifier, "queued", epoch, 0)], "documents": []}]
        if name == "high":
            for verb, state in (("cancel", "cancelled"), ("retry", "queued")):
                epoch += 1
                actions.append({"id": "a" + str(len(actions)).zfill(3), "op": "action", "action": verb,
                                "job": job(identifier, state, epoch, 0), "documents": []})
        actions.append({"id": "a" + str(len(actions)).zfill(3), "op": "action", "action": "prepare",
                        "job": job(identifier, "running", epoch, 0), "documents": []})
        if name == "maximum":
            actions.append({"id": "a" + str(len(actions)).zfill(3), "op": "action", "action": "cancel",
                            "job": job(identifier, "running", epoch, 0), "error": "counter_exhausted", "documents": []})
        actions.append({"id": "a" + str(len(actions)).zfill(3), "op": "action", "action": "commit",
                        "job": job(identifier, "completed", epoch, 0), "documents": [],
                        "replaces_error": "counter_exhausted" if name == "maximum" else None})
        actions.append({"id": "a" + str(len(actions)).zfill(3), "op": "fresh_context",
                        "jobs": [job(identifier, "completed", epoch, 0)], "documents": []})
        result.append({"protocol": PROTOCOL, "identifier": "m1-browser-" + name + "-epoch",
                       "requirements": ["M1-UI-AUTOMATION-V4", "V2-COUNTER-DOMAIN"],
                       "original_definition_purpose": ORIGINAL_PURPOSE, "actions": actions,
                       "seed_database": name, "seed_inventory": fixtures.inventory(name),
                       "server_argv": list(SERVER_ARGV), "port": PORT,
                       "facets": [a["id"] + ":" + facet for a in actions for facet in ("dom", "api", "mutation")],
                       "unqualified": ["native-browser-network", "application-server-restart", "M2-M4-browser-workflows",
                                       "global-independent-purpose"]})
    return tuple(result)


_DEFINITIONS = tuple(encoded(value) for value in (*_definitions(), *_counter_definitions()))


@dataclass(frozen=True)
class BrowserCase:
    definition: bytes

    def __post_init__(self) -> None:
        if type(self.definition) is not bytes or self.definition not in _DEFINITIONS:
            raise ValueError("Only exact prospective browser histories are admitted")

    @property
    def record(self) -> dict[str, Any]:
        self.__post_init__()
        return json.loads(self.definition)

    @property
    def identifier(self) -> str:
        return str(self.record["identifier"])

    @property
    def sha256(self) -> str:
        return sha(self.definition)

    @property
    def facet_ids(self) -> tuple[str, ...]:
        return tuple(self.identifier + ":" + value for value in self.record["facets"])

    def inputs(self) -> dict[str, bytes]:
        result = inputs()
        name = self.record["seed_database"]
        if name is not None:
            result["initial.sqlite"] = fixtures.seed(name)
        return result


def all_cases() -> tuple[BrowserCase, ...]:
    return tuple(BrowserCase(raw) for raw in _DEFINITIONS)


def case(identifier: str) -> BrowserCase:
    for value in all_cases():
        if value.identifier == identifier:
            return value
    raise ValueError("Unknown closed browser history")
