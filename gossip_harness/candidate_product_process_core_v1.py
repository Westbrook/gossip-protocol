"""Closed public-product process histories and independent host expectations.

A new declaration protocol, not a successor label for a reference execution.
Original fixture snapshots remain public data; only process/wire mechanics enter
candidate roles. Semantic scope review and physical qualification are separate.
"""
from __future__ import annotations

import base64
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
from typing import Any

from . import candidate_http_cases_core_v1 as prior
from . import candidate_http_inputs_v1 as staging

PROTOCOL = "candidate-product-process-cases-v1"
ORIGINAL_DEFINITION_PURPOSE = "public_product_definition"
CONTRACT_FILE = "library-cumulative-product-v2.json"
CONTRACT_SHA256 = "2d88ce0775888f148b0ec3caf90b3d5c82d8fed71f53bec5f7e75f492ae998dc"
PORT = prior.PORT
DATABASE = prior.DATABASE
ROOT = "/inputs"
MAX_DECLARED_STEPS = 128
MAX_EXPECTED_BYTES = 1024 * 1024
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
Fixture = prior.Fixture
HttpRequest = prior.HttpRequest
_relation_record = prior._relation_record
encode = prior.encode
_TARGETS = frozenset(("M2-IDENTITY-REVISIONS", "M2-REFRESH", "M2-ANNOTATIONS", "M2-COLLECTIONS",
    "M2-DELETE-RESTORE", "M2-QUERY", "M2-INTERFACES", "M3-WORKER-RECOVERY", "M3-REINDEX",
    "M3-EXPORT", "M3-BACKUP-RESTORE", "M3-DIAGNOSTICS", "M3-INTERFACES", "M4-API-SCHEMA",
    "M4-MIGRATION", "M4-COMPATIBILITY", "M4-RELEASE-HANDOFF"))
_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")


class DefinitionError(ValueError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise DefinitionError(message)


def digest(value: Any) -> str:
    return hashlib.sha256(encode(value)).hexdigest()


def definition_sources() -> dict[str, str]:
    root = Path(__file__).resolve().parents[1]
    contract = root / CONTRACT_FILE
    require(hashlib.sha256(contract.read_bytes()).hexdigest() == CONTRACT_SHA256,
            "Prospective normative product contract changed")
    modules = ("candidate_product_process_core_v1.py", "candidate_product_process_cases_v1.py")
    result = {"gossip_harness/" + name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
              for name in modules}
    require(result["gossip_harness/candidate_product_process_core_v1.py"] == LOADED_SOURCE_SHA256,
            "Loaded product declarations changed; fresh controller required")
    result[CONTRACT_FILE] = CONTRACT_SHA256
    return result


@dataclass(frozen=True, slots=True)
class Expectation:
    """Immutable host-only expectations. No execution authority is conferred."""
    json_bytes: bytes

    def __post_init__(self) -> None:
        require(type(self.json_bytes) is bytes and 0 < len(self.json_bytes) <= MAX_EXPECTED_BYTES,
                "Bounded immutable host expectation required")
        value = json.loads(self.json_bytes)
        require(type(value) is dict and encode(value) == self.json_bytes,
                "Canonical host expectation required")
        require(value.get("kind") in ("cli", "http", "server_start", "server_stop"),
                "Unknown observed product surface")
        allowed = {"kind", "exit", "json", "json_subset", "required_keys", "integer_ranges",
                   "json_value_only", "status", "body_utf8"}
        require(set(value) <= allowed and sum(key in value for key in ("json", "json_subset", "json_value_only")) <= 1,
                "Closed noncontradictory expectation fields required")
        if value["kind"] in ("server_start", "server_stop"):
            require(set(value) == {"kind"}, "Lifecycle cannot claim unobserved product content")
        else:
            scalar = "status" if value["kind"] == "http" else "exit"
            require(type(value.get(scalar)) is int and (100 <= value[scalar] <= 599
                    if scalar == "status" else 0 <= value[scalar] <= 255), "Exact expected status required")
            require(sum(key in value for key in ("json", "json_subset", "json_value_only")) == 1,
                    "Exactly one declared JSON comparison policy required")
            if "json_value_only" in value:
                require(value["kind"] == "cli" and value["exit"] == 0 and value["json_value_only"] is True,
                        "Unspecified wrapper permits only successful CLI JSON grammar credit")
            if "json_subset" in value:
                require(type(value["json_subset"]) is dict and type(value.get("required_keys")) is list
                        and len(value["required_keys"]) == len(set(value["required_keys"]))
                        and set(value["json_subset"]) <= set(value["required_keys"]),
                        "Partial value assertions must declare exact outer-key census")
            if "integer_ranges" in value:
                require(type(value["integer_ranges"]) is dict and all(type(key) is str and key.startswith("/")
                    and type(bounds) is list and len(bounds) == 2 and all(type(n) is int for n in bounds)
                    and bounds[0] <= bounds[1] for key, bounds in value["integer_ranges"].items()),
                    "Explicit integer intervals required")
            if "body_utf8" in value:
                require(value["kind"] == "http" and type(value["body_utf8"]) is str,
                        "Exact response bytes apply only to HTTP")

    def record(self) -> dict[str, Any]:
        return json.loads(self.json_bytes)


@dataclass(frozen=True, slots=True)
class Step:
    step_id: str
    kind: str
    epoch: int
    root: str
    argv: tuple[str, ...] = ()
    request: HttpRequest | None = None
    expectation: Expectation | None = None

    def __post_init__(self) -> None:
        require(type(self.step_id) is str and _NAME.fullmatch(self.step_id) is not None,
                "Exact bounded step identity required")
        require(self.kind in ("start", "request", "stop", "cli") and type(self.epoch) is int
                and 0 <= self.epoch <= MAX_DECLARED_STEPS and self.root == ROOT,
                "Declared process kind, epoch and fixed input root required")
        require(type(self.argv) is tuple and all(type(x) is str and x and "\0" not in x for x in self.argv),
                "Literal safe argv required")
        require(type(self.expectation) is Expectation, "Every step requires a host-only expectation")
        expected_kind = {"start": "server_start", "stop": "server_stop", "request": "http", "cli": "cli"}[self.kind]
        assert self.expectation is not None
        require(self.expectation.record()["kind"] == expected_kind, "Expected observation surface differs")
        if self.kind == "request":
            require(type(self.request) is HttpRequest and not self.argv and self.epoch >= 1,
                    "Only an active server can receive a literal request")
        else:
            require(self.request is None, "Non-request process step has request data")
            if self.kind == "stop":
                require(not self.argv and self.epoch >= 1, "Server retirement is an owned operation")
            else:
                prefix = ("python", "-m", "library", "--db", DATABASE, "--root", ROOT,
                          "--backup-dir", "/tmp/backups")
                require(len(self.argv) > len(prefix) and self.argv[:8] == prefix[:8]
                        and self.argv[8] in ("/tmp/backups", "/tmp/next"),
                        "Candidate argv must explicitly bind source, DB, input and backup roots")
                if self.kind == "start":
                    require(self.epoch >= 1 and self.argv[len(prefix):] == ("serve", "--port", str(PORT)),
                            "Exact declared HTTP listener command required")

    def record(self) -> dict[str, Any]:
        assert self.expectation is not None
        return {"step_id": self.step_id, "kind": self.kind, "server_epoch": self.epoch,
                "root": self.root, "database": DATABASE, "source_ref": "same-selected-candidate-source",
                "volume_ref": "same-owned-history-volume", "keeper_ref": "continuous-owned-history-keeper",
                "argv": list(self.argv), "request": None if self.request is None else self.request.record(),
                "expectation": self.expectation.record(),
                "lifecycle": "stop-and-remove-before-successor" if self.kind == "stop" else None}


@dataclass(frozen=True, slots=True)
class LiteralCase:
    row_id: str
    milestone: str
    requirement_ids: tuple[str, ...]
    fixtures: tuple[Fixture, ...]
    steps: tuple[Step, ...]
    seed_database_fixture: str | None
    original_json: bytes
    sources_json: bytes
    protocol: str = PROTOCOL

    def __post_init__(self) -> None:
        require(type(self.row_id) is str and _NAME.fullmatch(self.row_id) is not None
                and self.milestone in ("M2", "M3", "M4") and self.protocol == PROTOCOL,
                "Versioned cumulative product identity required")
        require(type(self.requirement_ids) is tuple and bool(self.requirement_ids)
                and len(set(self.requirement_ids)) == len(self.requirement_ids)
                and set(self.requirement_ids) <= _TARGETS, "Declared product requirement IDs required")
        require(type(self.fixtures) is tuple, "Immutable input fixtures required")
        staging.validate(self.fixtures)
        require(type(self.steps) is tuple and 3 <= len(self.steps) <= MAX_DECLARED_STEPS
                and all(type(x) is Step for x in self.steps)
                and len({x.step_id for x in self.steps}) == len(self.steps), "Complete ordered process roster required")
        require(self.seed_database_fixture in (None, "initial.sqlite"), "Fixed initial snapshot path required")
        if self.seed_database_fixture is not None:
            seeds = [x for x in self.fixtures if x.path == self.seed_database_fixture]
            require(len(seeds) == 1 and seeds[0].kind == "file" and 0 < len(seeds[0].data) <= 262144,
                    "Bounded original SQLite snapshot required")
        active = False
        epoch = 0
        for step in self.steps:
            if step.kind == "start":
                require(not active and step.epoch == epoch + 1, "Process epoch must advance exactly once")
                active, epoch = True, step.epoch
            elif step.kind == "stop":
                require(active and step.epoch == epoch, "Retirement must identify current server")
                active = False
            elif step.kind == "request":
                require(active and step.epoch == epoch, "HTTP request must identify a live epoch")
            else:
                require(not active and step.epoch == epoch, "Finite candidate CLI requires retired server")
        require(not active and epoch > 0 and any(x.kind == "request" for x in self.steps),
                "Complete physical HTTP history with final retirement required")
        require(type(self.original_json) is bytes and encode(json.loads(self.original_json)) == self.original_json
                and type(self.sources_json) is bytes and encode(json.loads(self.sources_json)) == self.sources_json,
                "Complete frozen source declarations required")

    def record(self) -> dict[str, Any]:
        return {"protocol": PROTOCOL, "row_id": self.row_id, "milestone": self.milestone,
                "requirement_ids": list(self.requirement_ids),
                "associated_amendment_ids": [x for x in json.loads(self.original_json)["requirement_ids"] if x.startswith("V2-")],
                "fixtures": [x.record() for x in self.fixtures], "steps": [x.record() for x in self.steps],
                "seed_database_fixture": self.seed_database_fixture,
                "original_definition_purpose": ORIGINAL_DEFINITION_PURPOSE,
                "original_definition": json.loads(self.original_json),
                "definition_sources": json.loads(self.sources_json),
                "claims_excluded": ["whole-product coverage", "actual daemon death or owner contention",
                    "private acceptance novelty", "old reference qualification reuse"]}

    def check_current(self) -> None:
        require(json.loads(self.sources_json) == definition_sources(), "Frozen definition source changed")
        from . import candidate_product_process_cases_v1 as definitions
        found = [row for row in definitions.acceptance_cases() if row["id"] == self.row_id]
        require(len(found) == 1 and encode(found[0]) == self.original_json,
                "Declared case differs from the versioned closed factory")
        require(self.record() == _case_definition(self.row_id, check_current=False).record(),
                "Typed mechanics or host expectations differ from the closed source definition")


def _case_definition(case_id: str, *, check_current: bool) -> LiteralCase:
    from . import candidate_product_process_cases_v1 as definitions
    rows = [row for row in definitions.acceptance_cases() if row["id"] == case_id]
    require(len(rows) == 1, "Unknown closed product history")
    value = rows[0]
    fixtures: dict[str, Fixture] = {}
    for item in value["input"]["files"]:
        path = item["path"]
        require(path != "initial.sqlite", "Reserved snapshot input path")
        fixtures[path] = Fixture(path, "file", item["text"].encode("utf-8"))
        parts = path.split("/")
        for i in range(1, len(parts)):
            name = "/".join(parts[:i])
            require(name not in fixtures or fixtures[name].kind == "directory", "Input fixture path collision")
            fixtures[name] = Fixture(name, "directory")
    snapshot = value["input"]["snapshot"]
    seed = None
    if snapshot is not None:
        require(set(snapshot) == {"encoding", "bytes", "sha256", "data"} and snapshot["encoding"] == "base64",
                "Closed public snapshot declaration required")
        raw = base64.b64decode(snapshot["data"], validate=True)
        require(type(snapshot["bytes"]) is int and len(raw) == snapshot["bytes"] <= 262144
                and hashlib.sha256(raw).hexdigest() == snapshot["sha256"], "Public snapshot identity differs")
        seed = "initial.sqlite"
        fixtures[seed] = Fixture(seed, "file", raw)
    steps = []
    epoch = 0
    prefix = ("python", "-m", "library", "--db", DATABASE, "--root", ROOT,
              "--backup-dir", "/tmp/backups")
    actions, observations = value["input"]["actions"], value["expected"]["observations"]
    require(len(actions) == len(observations), "Every actual step must have one prospective observation")
    for index, (action, expectation) in enumerate(zip(actions, observations, strict=True)):
        kind = {"server_start": "start", "server_stop": "stop", "http": "request", "cli": "cli"}.get(action["op"])
        require(kind is not None, "Unsupported physical action")
        assert kind is not None
        request = None
        argv: tuple[str, ...] = ()
        if kind == "start":
            epoch += 1
            argv = (*prefix, "serve", "--port", str(PORT))
        elif kind == "cli":
            selected_root = action.get("backup_dir", "/tmp/backups")
            require(selected_root in ("/tmp/backups", "/tmp/next"), "Fixed declared CLI root required")
            argv = (*prefix[:8], selected_root, *action["args"])
        elif kind == "request":
            body = b"" if action["body"] is None else encode(action["body"])
            headers = [("Host", "127.0.0.1:" + str(PORT)), ("Connection", "close")]
            if body or action["method"] == "POST":
                headers += [("Content-Type", "application/json"), ("Content-Length", str(len(body)))]
            request = HttpRequest(action["method"], action["path"], tuple(headers), body, PORT)
        steps.append(Step("s" + str(index).zfill(3) + "-" + action["op"], kind, epoch, ROOT,
                          argv, request, Expectation(encode(expectation))))
    result = LiteralCase(case_id, value.get("milestone", "M4"), tuple(x for x in value["requirement_ids"] if not x.startswith("V2-")),
        tuple(fixtures[name] for name in sorted(fixtures)), tuple(steps), seed, encode(value), encode(definition_sources()))
    if check_current:
        result.check_current()
    return result


def case_definition(case_id: str) -> LiteralCase:
    return _case_definition(case_id, check_current=True)


def definitions() -> tuple[LiteralCase, ...]:
    from . import candidate_product_process_cases_v1 as authored
    return tuple(case_definition(row["id"]) for row in authored.acceptance_cases())
