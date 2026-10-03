"""Read original, externally anchored HTTP journals without executing anything.

Physical origin means historical qualification only. This adapter grants no
prospective semantic expectation, fresh execution or acceptance authority.
"""
from __future__ import annotations

import base64
from dataclasses import asdict, dataclass, field
import hashlib
import os
from pathlib import Path
import re
import stat
import sys
import types
import weakref
from typing import Any

from . import candidate_http_execution_v2 as execution
from . import candidate_http_head_v1 as head
from . import candidate_http_semantics_v1 as semantics
from . import candidate_http_transport_v1 as wire

PROTOCOL = "candidate-http-observation-v1"
ORIGIN_MANIFEST = "analysis/candidate-c03-http-qualification-origin-v1.json"
# Root supplies the reviewed exact manifest digest before qualification.
ORIGIN_MANIFEST_SHA256 = "3b0e8bc9c3c825c453cff7246731c1fdc6777773cb043a319fda21948af564bb"
TEMPORAL_AUTHORITY = "original-source-bound-controller-and-external-checkpoint-sink"
MAX_MANIFEST_BYTES = 32 * 1024 * 1024
MAX_SOURCE_BYTES = 16 * 1024 * 1024
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_SEAL = object()
_ISSUED: dict[int, tuple[weakref.ReferenceType[BoundJournal], str]] = {}
engine = execution.engine
encoded = execution.encoded
digest = execution.digest
sha256 = execution.sha256


class ObservationError(ValueError):
    """Original evidence or its admission is invalid; no candidate retry follows."""


class ObservationUnknown(ObservationError):
    """Original intent has no terminal. No request may be redispatched."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ObservationError(message)


def _same(left: Any, right: Any, message: str) -> None:
    _require(encoded(left) == encoded(right), message)


def _name(value: Any) -> str:
    _require(execution.finite._safe_relative(value), "Unsafe evidence path")
    return str(value)


def _read(root: Path, name: str, limit: int = execution.MAX_RECORD_BYTES) -> bytes:
    """Bounded regular-file read, rejecting symlinks in every path component."""
    parts = _name(name).split("/")
    absolute = root.absolute()
    _require(absolute.resolve() == absolute, "Evidence root must be canonical")
    descriptors: list[int] = []
    try:
        directory = os.open(absolute, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        descriptors.append(directory)
        for part in parts[:-1]:
            directory = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
            descriptors.append(directory)
        descriptor = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        descriptors.append(descriptor)
        info = os.fstat(descriptor)
        _require(stat.S_ISREG(info.st_mode) and info.st_size <= limit, "Evidence file kind/size differs")
        chunks: list[bytes] = []
        remaining = limit + 1
        while remaining:
            chunk = os.read(descriptor, min(65536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        _require(len(raw) == info.st_size and len(raw) <= limit, "Evidence changed or exceeded its bound")
        return raw
    except OSError as error:
        raise ObservationError("Evidence unavailable: " + name) from error
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _json(raw: bytes) -> dict[str, Any]:
    try:
        result = engine.strict_json_loads(raw)
    except (ValueError, UnicodeError) as error:
        raise ObservationError("Malformed evidence JSON") from error
    _require(type(result) is dict, "Evidence JSON must be an object")
    return result


def _inventory(root: Path) -> dict[str, bytes]:
    _require(root.is_dir() and not root.is_symlink(), "Evidence directory unavailable")
    files: dict[str, bytes] = {}
    total = 0
    for path in sorted(root.rglob("*")):
        _require(not path.is_symlink(), "Symlink in evidence inventory")
        if path.is_dir():
            continue
        name = path.relative_to(root).as_posix()
        raw = _read(root, name)
        if name == "owner.lock":
            _require(not raw, "Unexpected owner lock content")
            continue
        total += len(raw)
        _require(total <= execution.MAX_JOURNAL_BYTES and len(files) < execution.MAX_JOURNAL_FILES,
                 "Evidence inventory bound exceeded")
        files[name] = raw
    return files


def _hashes(files: dict[str, bytes]) -> dict[str, str]:
    return {name: sha256(raw) for name, raw in files.items()}


def reanalysis_sources() -> tuple[tuple[str, str], ...]:
    return tuple((name, sha256(Path(__file__).with_name(name).read_bytes())) for name in
                 ("candidate_http_observation_v1.py", "candidate_http_head_v1.py", "candidate_http_semantics_v1.py"))


@dataclass(frozen=True)
class BoundJournal:
    """An admitted immutable snapshot; constructor is not a public authority API."""
    origin_id: str
    physical_origin: bool
    definition_json: bytes
    files: tuple[tuple[str, bytes], ...]
    checkpoints_json: tuple[bytes, ...]
    origin_manifest_sha256: str | None
    reanalysis_source_pins: tuple[tuple[str, str], ...]
    _seal: object = field(repr=False, compare=False)


@dataclass(frozen=True)
class FieldEvidence:
    field: str
    artifact_path: str
    artifact_sha256: str
    artifact_bytes: int
    json_pointer: str
    decoded_sha256: str
    decoded_bytes: int
    stage: str
    ranges: tuple[tuple[int, int], ...] = ()


@dataclass(frozen=True)
class StepObservation:
    step_id: str
    step_index: int
    eligible: bool
    diagnostic: bool
    physical_origin: bool
    facts: semantics.ResponseFacts | None
    limitations: tuple[str, ...]
    provenance: tuple[tuple[str, str], ...]
    head_ranges: tuple[tuple[str, int, int], ...] = ()
    sent_complete: bool | None = None
    exchange_complete: bool | None = None
    connection_attempts: int | None = None
    connection_refusals: int | None = None
    field_evidence: tuple[FieldEvidence, ...] = ()


@dataclass(frozen=True)
class HistoryObservation:
    origin_id: str
    origin_kind: str
    physical_origin: bool
    observations: tuple[StepObservation, ...]
    missing_step_ids: tuple[str, ...]
    cleanup_verified: bool
    infrastructure: tuple[str, ...]
    census: tuple[tuple[str, int], ...]
    reanalysis_source_pins: tuple[tuple[str, str], ...]
    original_manifest_sha256: str | None
    protocol: str = field(default=PROTOCOL, init=False)
    acceptance_authority: bool = field(default=False, init=False)
    comparison_authority: str = field(default="not_registered", init=False)
    fresh_execution: bool = field(default=False, init=False)
    temporal_authority: str = field(default=TEMPORAL_AUTHORITY, init=False)


def _admitted_fingerprint(bound: BoundJournal) -> str:
    return digest({"origin_id": bound.origin_id, "physical_origin": bound.physical_origin,
        "definition": sha256(bound.definition_json), "files": [(n, sha256(b)) for n, b in bound.files],
        "checkpoints": [sha256(x) for x in bound.checkpoints_json],
        "manifest": bound.origin_manifest_sha256, "sources": bound.reanalysis_source_pins})


def _checkpoint_rows(checkpoints: tuple[bytes, ...], files: dict[str, bytes]) -> dict[str, int]:
    _require(bool(checkpoints), "Original external checkpoint sequence required")
    previous: dict[str, str] = {}
    first: dict[str, int] = {}
    for index, raw in enumerate(checkpoints):
        value = _json(raw)
        _require(set(value) == {"files"} and type(value["files"]) is list, "Malformed checkpoint")
        pairs = value["files"]
        _require(all(type(row) is list and len(row) == 2 and type(row[0]) is str
                     and type(row[1]) is str and _SHA.fullmatch(row[1]) for row in pairs), "Invalid checkpoint entry")
        current = dict(pairs)
        _require(len(current) == len(pairs) and pairs == sorted(pairs), "Duplicate or unordered checkpoint name")
        _require(len(current) == len(previous) + 1 and all(current.get(k) == v for k, v in previous.items()),
                 "Checkpoint rollback/change/nonexclusive addition")
        for name, fingerprint in current.items():
            _require(name in files and sha256(files[name]) == fingerprint, "Checkpoint raw evidence differs")
            first.setdefault(name, index)
        previous = current
    _same(previous, _hashes(files), "Foreign suffix or incomplete final checkpoint")
    return first


def _admit(root: Path, origin_id: str, definition: dict[str, Any], checkpoints: tuple[bytes, ...],
           physical: bool, manifest_hash: str | None) -> BoundJournal:
    _require(type(definition) is dict and definition.get("kind") in ("journal", "manual_zero_request"),
             "Unknown original evidence kind")
    _loaded_sources_unchanged()
    files = _inventory(root)
    _same(_hashes(files), definition.get("files"), "Original inventory differs")
    if definition["kind"] == "journal":
        _checkpoint_rows(checkpoints, files)
    else:
        _require(not checkpoints, "Manual zero-request control has no controller checkpoints")
    if "registration" not in definition:
        _require(definition["kind"] == "manual_zero_request"
                 and definition["registration_path"] == "prospective-registration.json",
                 "Manual registration reference differs")
        registration_raw = files.get("prospective-registration.json", b"")
        _require(sha256(registration_raw) == definition["registration_sha256"], "Original manual registration changed")
        definition = dict(definition, registration=_json(registration_raw))
    _require(definition.get("temporal_authority") == TEMPORAL_AUTHORITY,
             "Original controller temporal-event authority not declared")
    result = BoundJournal(origin_id, physical, encoded(definition), tuple(sorted(files.items())), checkpoints,
                          manifest_hash, reanalysis_sources(), _SEAL)
    identity = id(result)
    _ISSUED[identity] = (weakref.ref(result, lambda _ref: _ISSUED.pop(identity, None)), _admitted_fingerprint(result))
    return result


def admit_fixture(root: Path, definition: dict[str, Any], checkpoints: tuple[dict[str, Any], ...]) -> BoundJournal:
    """Synthetic fixture admission; cannot grant historical physical origin."""
    _require(type(checkpoints) is tuple, "Immutable fixture checkpoint sequence required")
    return _admit(Path(root), "synthetic-fixture", definition, tuple(encoded(x) for x in checkpoints), False, None)


def admit_historical_qualification(origin_id: str, *, evidence_root: Path | None = None) -> BoundJournal:
    """Read one pre-registered historical origin; relocation changes no authority.

    No caller-supplied manifest, digest, authentication flag or purpose is accepted.
    The optional evidence root is a bit-identical copy of the registered inventory.
    Checkpoints/anchors still resolve through the fixed trusted origin manifest.
    """
    repo = Path(__file__).resolve().parents[1]
    _require(_SHA.fullmatch(ORIGIN_MANIFEST_SHA256) is not None, "No historical origin manifest registered")
    raw = _read(repo, ORIGIN_MANIFEST, MAX_MANIFEST_BYTES)
    _require(sha256(raw) == ORIGIN_MANIFEST_SHA256, "Historical origin manifest changed")
    manifest = _json(raw)
    _require(manifest.get("protocol") == "candidate-http-qualification-origins-v1"
             and manifest.get("purpose") == "harness_qualification"
             and manifest.get("acceptance_authority") is False, "Unsupported origin authority")
    for name, fingerprint in manifest["sources"].items():
        _require(sha256(_read(repo, name)) == fingerprint, "Frozen dependency source drift: " + name)
    _require(bool(manifest["anchors"]), "Independent original audit/freeze anchors missing")
    for item in manifest["anchors"]:
        _require(sha256(_read(repo, item["path"])) == item["sha256"], "Original authority anchor changed")
    _require(type(origin_id) is str and origin_id in manifest["origins"], "Unregistered historical origin")
    definition = manifest["origins"][origin_id]
    checkpoints: list[bytes] = []
    for descriptor in definition.get("checkpoints", []):
        data = _read(repo, descriptor["path"])
        _require(sha256(data) == descriptor["sha256"], "Original external checkpoint changed")
        checkpoints.append(data)
    location = repo / _name(definition["journal_path"]) if evidence_root is None else Path(evidence_root)
    return _admit(location, origin_id, definition, tuple(checkpoints), True, ORIGIN_MANIFEST_SHA256)


def _b64files(value: Any) -> dict[str, bytes]:
    _require(type(value) is dict and len(value) <= execution.finite.MAX_FIXTURE_FILES, "Invalid source archive")
    files: dict[str, bytes] = {}
    for name, content in value.items():
        _name(name)
        _require(type(content) is str, "Source bytes must be base64")
        try:
            files[name] = base64.b64decode(content, validate=True)
        except ValueError as error:
            raise ObservationError("Invalid source encoding") from error
    _require(sum(map(len, files.values())) <= MAX_SOURCE_BYTES, "Source archive bound exceeded")
    return files


def _git_oid(kind: str, raw: bytes) -> str:
    return hashlib.sha1(kind.encode() + b" " + str(len(raw)).encode() + b"\0" + raw).hexdigest()


def _git_tree(files: dict[str, bytes]) -> str:
    roots: dict[str, bytes] = {}
    directories: dict[str, dict[str, bytes]] = {}
    for path, raw in files.items():
        if "/" in path:
            name, rest = path.split("/", 1)
            directories.setdefault(name, {})[rest] = raw
        else:
            roots[path] = raw
    _require(not (roots.keys() & directories.keys()), "Git source file/directory collision")
    entries = [(name.encode(), b"100644 " + name.encode() + b"\0" + bytes.fromhex(_git_oid("blob", raw)))
               for name, raw in roots.items()]
    entries.extend((name.encode() + b"/", b"40000 " + name.encode() + b"\0" + bytes.fromhex(_git_tree(children)))
                   for name, children in directories.items())
    return _git_oid("tree", b"".join(raw for _, raw in sorted(entries)))


def _git_identity(definition: dict[str, Any], commit: str, tree: str) -> dict[str, bytes]:
    files = _b64files(definition["source_files"])
    payload = base64.b64decode(definition["git_commit_payload_b64"], validate=True)
    _require(len(payload) <= execution.CONTROL_LIMIT and _git_oid("commit", payload) == commit,
             "Original Git commit bytes differ")
    _require(payload.startswith(b"tree " + tree.encode() + b"\n") and _git_tree(files) == tree,
             "Original Git tree/source bytes differ")
    return files


def _role(record: dict[str, Any]) -> execution.RoleSpec:
    return execution.RoleSpec(record["role"], record["name"], tuple(record["argv"]),
        tuple(tuple(x) for x in record["labels"]), tuple(tuple(x) for x in record["binds"]),
        record["volume"], record["server_id"])


def _policy(record: dict[str, Any]) -> execution.HttpPolicy:
    return execution.HttpPolicy(**{**record, "wire_limits": wire.WireLimits(**record["wire_limits"])})


class _Reader:
    def __init__(self, bound: BoundJournal):
        self.bound = bound
        self.files = dict(bound.files)
        self.definition = _json(bound.definition_json)
        self.first = (_checkpoint_rows(bound.checkpoints_json, self.files)
                      if self.definition["kind"] == "journal" else {})

    def raw(self, name: str) -> bytes:
        _name(name)
        _require(name in self.files, "Missing original evidence: " + name)
        return self.files[name]

    def json(self, name: str) -> dict[str, Any]:
        return _json(self.raw(name))

    def descriptor(self, item: dict[str, Any]) -> bytes:
        raw = self.raw(item["path"])
        _require(type(item["bytes"]) is int and len(raw) == item["bytes"] and sha256(raw) == item["sha256"],
                 "Raw descriptor mismatch")
        return raw

    def before(self, earlier: str, later: str) -> None:
        _require(earlier in self.first and later in self.first and self.first[earlier] < self.first[later],
                 "Original checkpoint order differs: " + earlier)

    def control(self, label: str, cid: str, operation: str = "json", method: str = "GET", status: int = 200) -> bytes:
        expected = engine._request(method, "/containers/" + cid + "/" + operation)
        _require(self.raw(label + "-request.bin") == expected, "Raw Engine request identity differs")
        raw = self.raw(label + "-response.bin")
        return _control_body(raw, status)

    def inspect(self, label: str, cid: str) -> dict[str, Any]:
        value = _json(self.control(label, cid))
        _require(value.get("Id") == cid, "Raw inspection full ID differs")
        return value

    def comparison(self, name: str, before: dict[str, Any], after: dict[str, Any], spec: execution.RoleSpec,
                   runtime: dict[str, Any], phase: str, donor: execution.ProbeDonorEvidence | None = None) -> None:
        comparison = execution.role_identity_comparison(before, after, spec, runtime, phase, donor=donor)
        _require(comparison["matches"] is True, "Reconstructed role identity differs")
        _same(self.json(name + ".json"), comparison, "Claimed role comparison differs")

    def command(self, label: str, expected: list[str] | None = None) -> tuple[dict[str, Any], bytes]:
        record = self.json(label + ".json")
        _require(type(record["argv"]) is list and record["argv"][:1] == ["docker"], "Invalid retained command")
        if expected is not None:
            endpoint = self.json("config.json")["endpoint"] if "config.json" in self.files else self.definition["registration"]["runtime"]["endpoint"]
            _same(record["argv"], ["docker", "--host", "unix://" + endpoint["socket_path"], *expected[1:]],
                  "Original Docker command differs")
        for channel in ("stdout", "stderr"):
            raw = self.descriptor(record[channel])
            _require(record[channel].get("truncated") is False and record[channel].get("observed_bytes") == len(raw),
                     "Command capture incomplete")
        _require(record.get("exit_code") == 0 and type(record.get("exit_code")) is int
                 and record.get("timed_out") is False and record.get("capture_complete") is True,
                 "Original command did not complete")
        return record, self.descriptor(record["stdout"])


class _RetainedWire(engine._Wire):
    """Frozen parser methods over immutable retained bytes; no socket exists.

    File exhaustion supports parser reconstruction only. Actual control EOF was
    observed by the original trusted controller, whose source/checkpoint lineage
    supplies temporal authority; this class does not manufacture fresh evidence.
    """
    def __init__(self, raw: bytes):
        _require(len(raw) <= execution.MAX_RECORD_BYTES, "Retained wire bound exceeded")
        self.buffer = bytearray(raw)
        self.eof = False

    def receive(self) -> None:
        self.eof = True


def _control_body(raw: bytes, status: int) -> bytes:
    retained = _RetainedWire(raw)
    observed_status, headers = retained.headers()
    _require(observed_status == status, "Retained Engine response status differs")
    return retained.body(observed_status, headers)


def _registration(reader: _Reader) -> tuple[dict[str, Any], execution.HttpRegistration, execution.HttpRecipe, execution.HttpPolicy]:
    definition = reader.definition
    prospective = definition["registration"]
    config = reader.json("config.json")
    registration = execution.HttpRegistration(execution.HttpBinding(**prospective["registration"]["binding"]),
        prospective["registration"]["commit_oid"], prospective["registration"]["tree_oid"],
        prospective["registration"]["repetition_id"])
    files = _git_identity(definition, registration.commit_oid, registration.tree_oid)
    fixtures = _b64files(definition["fixture_files"])
    rec = prospective["recipe"]
    recipe = execution.HttpRecipe(rec["recipe_id"], tuple(rec["server_argv"]), tuple(sorted(fixtures.items())),
        tuple(rec["directories"]), tuple(execution.HttpStep(x["step_id"], x["kind"], encoded(x["request"])
            if x["kind"] == "probe" else b"") for x in rec["steps"]), rec["port"], rec["database_path"], rec["root_path"])
    policy = _policy(prospective["policy"])
    runtime = config["runtime"]
    _require(engine._startup_runtime_valid(runtime, policy.image_id), "Original runtime outside frozen profile")
    _same(config["endpoint"], runtime["endpoint"], "Original runtime endpoint differs")
    sources = execution.evaluator_sources()
    _same(config["evaluator_sources"], sources, "Frozen evaluator source drift")
    _same(config["source_manifest"], execution.source_manifest(files), "Original staged-source manifest differs")
    staging = reader.json("staging.json")
    _same(staging["source_manifest"], execution.source_manifest(files), "Original staging source inventory differs")
    _same(staging["fixture_manifest"], execution.source_manifest(fixtures), "Original staging fixture inventory differs")
    _same(config["recipe"], recipe.record(), "Original recipe changed")
    _same(rec, recipe.record(), "Registered fixture/request manifest differs")
    _same(config["policy"], asdict(policy), "Original policy changed")
    _same(config["registration"], asdict(registration), "Original registration changed")
    _same(config["role_policy"], execution.role_policy_identity(), "Original role policy changed")
    _require(config["protocol"] == execution.PROTOCOL and config["mode"] == "physical"
             and config["snapshot_protocol"] == execution.SNAPSHOT_PROTOCOL, "Original protocol/mode differs")
    _require(config["helper_sha256"] == wire.helper_sha256()
             and config["helper_stdout_envelope"] == wire.max_probe_output_bytes(policy.wire_limits), "Helper definition differs")
    binding = asdict(registration.binding)
    expected = dict(binding, source_sha256=execution.source_sha256(files), recipe_sha256=digest(recipe.record()),
        fixture_sha256=digest({"manifest": execution.source_manifest(fixtures), "directories": recipe.directories}),
        evaluator_sha256=digest(sources), helper_sha256=wire.helper_sha256(), runtime_sha256=digest(runtime),
        environment_sha256=definition["environment_sha256"], limits_sha256=digest(asdict(policy)),
        seed_sha256=digest({"seed": policy.seed, "meaning": "fixed fixture; no candidate random seed implied"}),
        role_policy_sha256=digest(execution.role_policy_definition()),
        role_policy_source_sha256=sha256(Path(execution.__file__).read_bytes()))
    _same(binding, expected, "Original execution binding differs")
    _require(_SHA.fullmatch(definition["docker_command_environment_sha256"]) is not None,
             "Original command environment identity unavailable")
    return config, registration, recipe, policy


def _long_role(reader: _Reader, prefix: str, runtime: dict[str, Any], image: str) -> tuple[execution.RoleSpec, dict[str, Any]]:
    creation = reader.json(prefix + "-create-intent.json")
    spec = _role(creation["spec"])
    _same(creation["create_argv"], execution.create_argv(spec, image), "Role creation recipe differs")
    _, output = reader.command(prefix + "-create", execution.create_argv(spec, image))
    cid = output.strip().decode("ascii")
    created = reader.inspect(prefix + "-created", cid)
    before = reader.inspect(prefix + "-prestart", cid)
    for value in (created, before):
        execution.validate_state(value, "created")
    reader.comparison(prefix + "-prestart-comparison", created, before, spec, runtime, "created-to-prestart")
    start_body = reader.control(prefix + "-start", cid, "start", "POST", 204)
    start = reader.json(prefix + "-start-completion.json")
    _same(start, {"status": 204, "body_sha256": sha256(start_body), "framing_complete": True,
                  "eof_observed": True, "container_id": cid}, "Role start completion differs")
    reader.before(prefix + "-prestart-response.bin", prefix + "-start-intent.json")
    reader.before(prefix + "-start-intent.json", prefix + "-start-request.bin")
    running = reader.inspect(prefix + "-running", cid)
    execution.validate_state(running, "running")
    reader.comparison(prefix + "-startup-comparison", created, running, spec, runtime, "created-to-running")
    _same(reader.json(prefix + "-running.json"), running, "Role running copy differs")
    return spec, running


def _probe_process(reader: _Reader, row: dict[str, Any], spec: execution.RoleSpec,
                   donor: execution.ProbeDonorEvidence, policy: execution.HttpPolicy, runtime: dict[str, Any]) -> bytes:
    label, cid = row["label"] + "-probe", row["probe_id"]
    created = reader.inspect(label + "-created", cid)
    before = reader.inspect(label + "-prestart", cid)
    execution.validate_state(created, "created")
    execution.validate_state(before, "created")
    reader.comparison(label + "-prestart-comparison", created, before, spec, runtime, "created-to-prestart")
    intent = reader.json(label + "-intent.json")
    envelope = wire.max_probe_output_bytes(policy.wire_limits)
    _same(intent, {"protocol": execution.PROTOCOL, "role": asdict(spec), "expected": digest(created),
        "runtime": runtime, "policy": asdict(policy), "role_policy": execution.role_policy_identity(),
        "donor": donor.record(), "helper_stdout_envelope": envelope, "helper_sha256": wire.helper_sha256()},
        "Original helper intent differs")
    _require(reader.raw(label + "-attach-request.bin") == engine._request("POST", "/containers/" + cid
        + "/attach?stream=1&stdout=1&stderr=1&stdin=0&logs=0", upgrade=True), "Helper attachment identity differs")
    attach = reader.raw(label + "-attach-response.bin")
    retained_attach = _RetainedWire(attach)
    status, values = retained_attach.headers()
    _require(status == 101 and values.get("connection", "").lower() == "upgrade"
             and values.get("upgrade", "").lower() == "tcp"
             and values.get("content-type") in ("application/vnd.docker.raw-stream", "application/vnd.docker.multiplexed-stream")
             and "content-length" not in values and "transfer-encoding" not in values, "Helper upgrade headers differ")
    start_body = reader.control(label + "-start", cid, "start", "POST", 204)
    waited = _json(reader.control(label + "-wait", cid, "wait?condition=not-running", "POST"))
    final = reader.inspect(label + "-final", cid)
    reader.comparison(label + "-identity-comparison", created, final, spec, runtime, "created-to-exited", donor)
    process = reader.json(label + "-process.json")
    _same(process, row["process"], "Original process copy differs")
    _same(process["donor"], donor.record(), "Process donor differs")
    _require(process["protocol"] == execution.PROTOCOL and process["role"] == "probe"
             and process["container_id"] == cid and process["status"] == "completed" and process["error"] is None,
             "Helper completion unavailable")
    _same(process["role_policy"], execution.role_policy_identity(), "Helper role policy differs")
    _same(process["identity_comparison"], reader.json(label + "-identity-comparison.json"), "Process comparison differs")
    _same(process["completion"], engine.completion_evidence(waited, final, started=True, killed=False, identity_ok=True),
          "Raw helper wait/final completion differs")
    _require(process["completion"]["natural"] is True and process["completion"]["inspect_exit_code"] == 0,
             "Helper natural exit0 unavailable")
    _require(process["attach_eof_after_start_confirmation"] is True and process["capture_complete"] is True,
             "Original trusted attach EOF/capture event unavailable")
    _require(process["helper_stdout_envelope"] == envelope, "Helper envelope differs")
    start = reader.json(label + "-start-completion.json")
    _same(start, process["start_response"], "Process start response differs")
    _require(start["status"] == 204 and start["body_bytes"] == len(start_body)
             and start["body_sha256"] == sha256(start_body) and start["framing_complete"] is True
             and start["eof_observed"] is True, "Helper start evidence differs")
    _require(reader.descriptor(start["request"]) == reader.raw(label + "-start-request.bin")
             and reader.descriptor(start["response"]) == reader.raw(label + "-start-response.bin"), "Start raw binding differs")
    for name, descriptor in process["evidence"].items():
        _require(descriptor["path"] == label + "-" + name, "Process evidence path differs")
        reader.descriptor(descriptor)
    decoder = engine.MultiplexDecoder(engine.ProcessPolicy(policy.image_id,
        timeout_seconds=policy.probe_timeout_seconds, stream_limit_bytes=envelope, frame_limit_bytes=envelope,
        transport_timeout_seconds=policy.transport_timeout_seconds))
    decoder.feed(bytes(retained_attach.buffer))
    # EOF is admitted from the ORIGINAL trusted capture event above, not inferred
    # from the retained file ending. Synthetic origins remain synthetic.
    decoder.eof()
    for channel in ("stdout", "stderr"):
        record = process[channel]
        _require(reader.descriptor(record) == bytes(decoder.streams[channel])
                 and record["observed_bytes"] == decoder.counts[channel]
                 and record["truncated"] is False and record["complete"] is True,
                 "Helper raw frame/stream descriptor differs")
    _require(not decoder.streams["stderr"], "Trusted helper emitted diagnostics")
    reader.before(label + "-intent.json", label + "-attach-request.bin")
    reader.before(label + "-attach-request.bin", label + "-start-request.bin")
    reader.before(label + "-start-completion.json", label + "-wait-request.bin")
    reader.before(label + "-final-response.bin", label + "-process.json")
    return bytes(decoder.streams["stdout"])


def _listeners(value: wire.ListenerSnapshot) -> semantics.ListenerFacts:
    return semantics.ListenerFacts(tuple(address for _, address, _ in value.listeners),
        ";".join(value.limitations) or (None if value.complete else "listener coverage incomplete"))


def _facts(observation: wire.WireObservation, limits: wire.WireLimits) -> tuple[semantics.ResponseFacts, head.HeadFacts]:
    extracted = head.extract_response_head(observation.received, limits)
    body: bytes | semantics.Missing = (observation.response.body if observation.sent_complete
        and observation.response.body_complete and observation.response.framing_complete
        else semantics.Missing(";".join(observation.limitations) or "complete response framing unavailable"))
    before: semantics.ListenerFacts | semantics.Missing = semantics.Missing("before snapshot is connection-failed diagnostic")
    after: semantics.ListenerFacts | semantics.Missing = semantics.Missing("no connected request interval")
    if observation.connected and observation.listeners_before_stage == "connected_pre_request":
        before, after = _listeners(observation.listeners_before), _listeners(observation.listeners_after)
    return semantics.ResponseFacts(extracted.status, extracted.headers, body, before, after), extracted



def _cleanup_and_census(reader: _Reader, terminal: dict[str, Any], intent: dict[str, Any],
                        runtime: dict[str, Any], policy: execution.HttpPolicy) -> bool:
    """Reconstruct declared cleanup separately from per-request eligibility."""
    owned: dict[str, tuple[execution.RoleSpec, str | None]] = {}
    for name in reader.files:
        if not name.endswith("-create-intent.json"):
            continue
        creation = reader.json(name)
        if "spec" not in creation:
            continue
        spec = _role(creation["spec"])
        _require(spec.name not in owned, "Duplicate resource creation identity")
        _same(creation["create_argv"], execution.create_argv(spec, policy.image_id), "Resource create profile differs")
        prefix = name.removesuffix("-create-intent.json")
        cid = None
        if prefix + "-create.json" in reader.files:
            record = reader.json(prefix + "-create.json")
            if execution.CandidateHttpExecution._clean(record):
                _, raw = reader.command(prefix + "-create", execution.create_argv(spec, policy.image_id))
                cid = raw.strip().decode("ascii")
                _require(_SHA.fullmatch(cid) is not None, "Created resource full ID differs")
                value = reader.inspect(prefix + "-created", cid)
                execution.validate_role(value, spec, policy.image_id, runtime)
                execution.validate_state(value, "created")
        owned[spec.name] = spec, cid
    _require(terminal["create_intents"] == len(owned)
             and terminal["unconfirmed_creates"] == sum(cid is None for _, cid in owned.values()), "Resource creation census differs")
    for role, key in (("probe", "created_helpers"), ("server", "created_servers"), ("keeper", "created_keepers")):
        _require(terminal[key] == sum(spec.role == role and cid is not None for spec, cid in owned.values()),
                 "Confirmed role creation count differs")
    _require(set(terminal["cleanup"]) <= set(owned), "Cleanup names a foreign resource")
    starts = [{"path": name, "sha256": sha256(raw), "status": reader.json(name)["status"]}
              for name, raw in sorted(reader.files.items()) if name.endswith("-start-completion.json")]
    _same(terminal["start_response_records"], starts, "Original start-response census differs")
    for resource, successful in terminal["cleanup"].items():
        _require(type(successful) is bool, "Cleanup result must be explicit")
        if not successful:
            continue
        spec, cid = owned[resource]
        candidates = []
        expected_absence = ["docker", "container", "ls", "--all", "--quiet", "--filter", "name=^/" + resource + "$"]
        for name in reader.files:
            if name.endswith("-absence.json"):
                item = reader.json(name)
                if item.get("argv", [])[-1:] == expected_absence[-1:]:
                    candidates.append(name.removesuffix("-absence.json"))
        _require(len(candidates) == 1, "Successful cleanup lacks unique original absence evidence")
        prefix = candidates[0]
        _, absent = reader.command(prefix + "-absence", expected_absence)
        _require(not absent.strip(), "Resource still present after cleanup")
        inspect_record = reader.json(prefix + "-inspect.json")
        if execution.CandidateHttpExecution._clean(inspect_record):
            _, raw = reader.command(prefix + "-inspect", ["docker", "inspect", "--format", "{{json .}}", resource])
            inspected = _json(raw)
            _require(inspected["Name"] == "/" + resource and (cid is None or inspected["Id"] == cid)
                     and inspected["Image"] == policy.image_id and inspected["Config"]["Labels"] == dict(spec.labels),
                     "Cleanup ownership differs")
            removal = reader.json(prefix + "-intent.json")
            _same(removal["spec"], asdict(spec), "Cleanup spec differs")
            _require(removal["container_id"] == inspected["Id"] and removal["inspection_sha256"] == digest(inspected)
                     and type(removal["force"]) is bool, "Cleanup intent differs")
            argv = ["docker", "rm", *(["--force"] if removal["force"] else []), inspected["Id"]]
            reader.command(prefix + "-remove", argv)
            reader.before(prefix + "-inspect.json", prefix + "-intent.json")
            reader.before(prefix + "-intent.json", prefix + "-remove.json")
            reader.before(prefix + "-remove.json", prefix + "-absence.json")
    all_clean = bool(owned) and set(terminal["cleanup"]) == set(owned) and all(terminal["cleanup"].values())
    if terminal["volume_cleanup"] is True:
        _require(all_clean, "Volume removed while an owned container remains")
        _, absent = reader.command("volume-absence", ["docker", "volume", "ls", "--quiet", "--filter", "name=^" + intent["volume"] + "$"])
        _require(not absent.strip(), "Owned volume still present")
        checked = reader.json("volume-cleanup-inspect.json")
        if execution.CandidateHttpExecution._clean(checked):
            _, raw = reader.command("volume-cleanup-inspect", ["docker", "volume", "inspect", "--format", "{{json .}}", intent["volume"]])
            volume = _json(raw)
            _require(volume.get("Name") == intent["volume"] and volume.get("Driver") == "local"
                     and volume.get("Options") == execution.VOLUME_OPTIONS and volume.get("Labels") == {
                         "gossip.execution": intent["execution_id"], "gossip.snapshot": execution.SNAPSHOT_PROTOCOL},
                     "Volume cleanup ownership differs")
            reader.command("volume-remove", ["docker", "volume", "rm", intent["volume"]])
            reader.before("volume-remove.json", "volume-absence.json")
        reader.before("volume-absence.json", "terminal.json")
    else:
        _require(terminal["volume_cleanup"] is False, "Volume cleanup result must be explicit")
    return all_clean and terminal["volume_cleanup"] is True


def _stopped_epoch(reader: _Reader, row: dict[str, Any], baseline: dict[str, Any], spec: execution.RoleSpec,
                   runtime: dict[str, Any], policy: execution.HttpPolicy) -> None:
    label, cid = row["label"] + "-stop", baseline["Id"]
    before = reader.inspect(label + "-before", cid)
    execution.validate_state(before, "running")
    reader.comparison(label + "-before-continuity", baseline, before, spec, runtime, "running-to-running")
    stop_intent = reader.json(label + "-intent.json")
    _same(stop_intent, {"controller_action": "explicit-stop", "container_id": cid,
        "running_sha256": digest(baseline), "timeout_seconds": policy.stop_timeout_seconds,
        "natural_exit_zero_required": False}, "Server stop intent differs")
    body = reader.control(label, cid, "stop?t=" + str(policy.stop_timeout_seconds), "POST", 204)
    final = reader.inspect(label + "-final", cid)
    execution.validate_state(final, "exited")
    reader.comparison(label + "-comparison", baseline, final, spec, runtime, "running-to-exited")
    result = {"controller_action": "explicit-stop", "status": 204, "response_body_sha256": sha256(body),
        "container_id": cid, "exit_code": final["State"]["ExitCode"], "natural_exit_zero_required": False,
        "comparison": reader.json(label + "-comparison.json"), "final_inspection_sha256": digest(final)}
    _same(row["stop"], result, "Original server stop result differs")
    _same(reader.json(label + "-result.json"), result, "Retained server stop result differs")
    _require(row["removed"] is True, "Retired server removal unproved")
    retirement = row["label"] + "-retire-absence"
    _, absence = reader.command(retirement, ["docker", "container", "ls", "--all", "--quiet", "--filter", "name=^/" + spec.name + "$"])
    _require(not absence.strip(), "Retired server absence unavailable before next epoch")
    reader.before(label + "-result.json", retirement + ".json")


def _journal_history(reader: _Reader) -> HistoryObservation:
    config, registration, recipe, policy = _registration(reader)
    if "intent.json" not in reader.files or "terminal.json" not in reader.files:
        raise ObservationUnknown("Original intent/terminal incomplete; never redispatch")
    intent, terminal = reader.json("intent.json"), reader.json("terminal.json")
    _require(type(intent["execution_id"]) is str and re.fullmatch(r"http-[a-z0-9]+", intent["execution_id"]) is not None,
             "Original execution identity malformed")
    _require(intent["volume"] == "gossip-" + intent["execution_id"] + "-volume"
             and intent["keeper"] == "gossip-" + intent["execution_id"] + "-keeper"
             and intent["containers"] == ["gossip-" + intent["execution_id"] + "-" + str(i) for i in range(len(recipe.steps))],
             "Original owned resource roster differs")
    _require(intent["protocol"] == execution.PROTOCOL and intent["config_sha256"] == digest(config)
             and intent["registration_sha256"] == digest(asdict(registration)), "Original execution intent differs")
    _same(intent["role_policy"], execution.role_policy_identity(), "Intent role policy differs")
    _require(terminal["protocol"] == execution.PROTOCOL and terminal["mode"] == "physical"
             and terminal["intent_sha256"] == sha256(reader.raw("intent.json")), "Original terminal identity differs")
    _same(terminal["role_policy"], execution.role_policy_identity(), "Terminal role policy differs")
    _same(terminal["evaluator_sources_after"], config["evaluator_sources"], "Original evaluator drift")
    planned = [index for index, step in enumerate(recipe.steps) if step.kind == "probe"]
    _require(intent["planned_requests"] == len(planned) == terminal["planned_requests"]
             and intent["planned_steps"] == len(recipe.steps)
             and intent["planned_servers"] == sum(step.kind == "start" for step in recipe.steps)
             and intent["planned_keepers"] == 1, "Original planned census differs")
    rows = terminal["steps"]
    _require(type(rows) is list and len(rows) <= len(recipe.steps), "Invalid original step census")
    for index, row in enumerate(rows):
        step = recipe.steps[index]
        _require(type(row["step_index"]) is int and row["step_index"] == index and row["step_id"] == step.step_id
                 and row["kind"] == step.kind and row["label"] == f"step-{index:03d}", "Original step order differs")
        _same(row, reader.json(row["label"] + "-result.json"), "Original result copy differs")
    observations = terminal["observations"]
    _require(type(observations) is list and len(observations) <= len(planned), "Invalid original observation census")
    _require([row["step_index"] for row in observations] == planned[:len(observations)], "Duplicate/missing/reordered probe observation")
    _require(len({row["probe_id"] for row in observations}) == len(observations), "Helper ID reused")
    runtime = config["runtime"]
    keeper_spec, keeper = _long_role(reader, "keeper", runtime, policy.image_id)
    _require(keeper_spec.role == "keeper" and keeper_spec.volume == intent["volume"]
             and keeper_spec.name == intent["keeper"]
             and keeper_spec.argv == ("python", "-I", "-c", "import time;time.sleep(" + str(policy.lifetime_seconds) + ")"),
             "Original keeper identity differs")
    _same(dict(keeper_spec.labels), {"gossip.execution": intent["execution_id"], "gossip.role": "keeper",
        "gossip.source": registration.binding.source_sha256, "gossip.fixture": registration.binding.fixture_sha256,
        "gossip.epoch": "0", "gossip.step": "state-lifetime", "gossip.helper": wire.helper_sha256()}, "Keeper label binding differs")
    epochs: dict[int, tuple[execution.RoleSpec, dict[str, Any], dict[str, Any]]] = {}
    for row in rows:
        if row["kind"] != "start":
            continue
        spec, baseline = _long_role(reader, row["label"], runtime, policy.image_id)
        epoch = len(epochs) + 1
        _require(type(row["epoch"]) is int and row["epoch"] == epoch and row["server_id"] == baseline["Id"]
                 and baseline["Id"] not in [item[1]["Id"] for item in epochs.values()], "Original server epoch reused/changed")
        staging = reader.json("staging.json")
        _require(dict(spec.binds) == {"/workspace": staging["workspace"], "/inputs": staging["inputs"]},
                 "Server source/fixture mount differs from original staging")
        _require(spec.role == "server" and spec.argv == recipe.server_argv and spec.volume == intent["volume"]
                 and spec.name == intent["containers"][row["step_index"]],
                 "Server registered argv/DB volume differs")
        labels = dict(spec.labels)
        _require(len(labels) == 7, "Unexpected server identity labels")
        for key, value in (("gossip.execution", intent["execution_id"]), ("gossip.role", "server"),
                           ("gossip.epoch", str(epoch)), ("gossip.source", registration.binding.source_sha256),
                           ("gossip.fixture", registration.binding.fixture_sha256), ("gossip.helper", wire.helper_sha256()),
                           ("gossip.step", row["step_id"])):
            _require(labels.get(key) == value, "Server original label differs")
        epoch_record = {"epoch": epoch, "server_id": baseline["Id"], "name": spec.name,
            "started_at": baseline["State"]["StartedAt"], "pid": baseline["State"]["Pid"],
            "database_volume": intent["volume"], "database_path": recipe.database_path,
            "source_sha256": registration.binding.source_sha256, "fixture_sha256": registration.binding.fixture_sha256,
            "running_inspection_sha256": digest(baseline), "keeper_id": keeper["Id"],
            "keeper_started_at": keeper["State"]["StartedAt"]}
        _same(terminal["epochs"][epoch - 1], epoch_record, "Original epoch record differs")
        for name, value in epoch_record.items():
            _same(row[name], value, "Original start row differs")
        epochs[epoch] = spec, baseline, row
    _require(len(epochs) == len(terminal["epochs"]), "Extra original epoch")
    for row in rows:
        if row["kind"] == "stop":
            _require(type(row["epoch"]) is int and row["epoch"] in epochs, "Stopped unknown server epoch")
            spec, baseline, _start = epochs[row["epoch"]]
            _require(row["server_id"] == baseline["Id"], "Stopped foreign server")
            _stopped_epoch(reader, row, baseline, spec, runtime, policy)
    for epoch in sorted(epochs):
        if epoch > 1:
            retired = [row for row in rows if row["kind"] == "stop" and row["epoch"] == epoch - 1]
            _require(len(retired) == 1, "Prior server not uniquely retired")
            reader.before(retired[0]["label"] + "-retire-absence.json", epochs[epoch][2]["label"] + "-create-intent.json")
    output: list[StepObservation] = []
    for row in observations:
        index = row["step_index"]
        _require(type(index) is int and 0 <= index < len(recipe.steps), "Invalid probe index")
        step = recipe.steps[index]
        label = f"step-{index:03d}"
        _require(row["kind"] == "probe" and row["step_id"] == step.step_id and row["label"] == label,
                 "Original probe recipe/label differs")
        _same(row, reader.json(label + "-observation.json"), "Original observation copy differs")
        _require(type(row["authenticated"]) is bool, "Original attribution event type differs")
        _require(type(row["epoch"]) is int and row["epoch"] in epochs, "Probe server epoch unavailable")
        spec, baseline, epoch_row = epochs[row["epoch"]]
        _require(epoch_row["step_index"] < index and not any(x["kind"] == "stop" and x["step_index"] < index
                 and x.get("epoch") == row["epoch"] for x in rows), "Probe after retired server epoch")
        for key, value in (("server_id", baseline["Id"]), ("keeper_id", keeper["Id"]),
            ("source_sha256", registration.binding.source_sha256), ("fixture_sha256", registration.binding.fixture_sha256),
            ("commit_oid", registration.commit_oid), ("tree_oid", registration.tree_oid),
            ("helper_sha256", wire.helper_sha256()), ("database_volume", intent["volume"]),
            ("database_path", recipe.database_path), ("binding", asdict(registration.binding))):
            _same(row[key], value, "Probe source/epoch/request binding differs")
        provenance = (("execution_id", intent["execution_id"]), ("step_id", step.step_id),
            ("request_sha256", sha256(wire.request_bytes(engine.strict_json_loads(step.request_json), recipe.port, policy.wire_limits))),
            ("server_id", baseline["Id"]), ("epoch", str(row["epoch"])),
            ("binding_sha256", digest(asdict(registration.binding))), ("terminal_sha256", sha256(reader.raw("terminal.json"))),
            ("original_checkpoint_sha256", sha256(reader.bound.checkpoints_json[-1])),
            ("temporal_authority", TEMPORAL_AUTHORITY), ("purpose", registration.binding.purpose))
        if not row["authenticated"]:
            # Never turn M05 or other unattributed raw bytes into product facts.
            output.append(StepObservation(step.step_id, index, False, True, reader.bound.physical_origin, None,
                (row.get("observation_error", "original request attribution unavailable"),), provenance))
            continue
        before = reader.inspect(label + "-server-before", baseline["Id"])
        execution.validate_state(before, "running")
        reader.comparison(label + "-server-before-continuity", baseline, before, spec, runtime, "running-to-running")
        donor = execution.bind_probe_donor(baseline=baseline, current=before, server_spec=spec,
            registration=registration, runtime=runtime, epoch=row["epoch"])
        _same(row["donor"], donor.record(), "Original donor differs")
        _same(reader.json(label + "-donor.json"), donor.record(), "Retained donor differs")
        _same(reader.json(label + "-donor-inspection.json"), before, "Donor raw copy differs")
        for name in (label + "-server-before-response.bin", label + "-donor-inspection.json", label + "-donor.json"):
            reader.before(name, label + "-probe-create-intent.json")
        probe_creation = reader.json(label + "-probe-create-intent.json")
        probe = _role(probe_creation["spec"])
        donor.validate_probe(probe, runtime)
        _require(probe.name == intent["containers"][index] and dict(probe.labels).get("gossip.step") == step.step_id
                 and len(probe.labels) == 7, "Probe declared step/resource identity differs")
        staging = reader.json("staging.json")
        _require(dict(probe.binds) == {"/probe": str(Path(staging["workspace"]).parent / label)},
                 "Probe input/source mount differs from original staged helper")
        _same(probe_creation["create_argv"], execution.create_argv(probe, policy.image_id), "Probe create argv differs")
        _, created_output = reader.command(label + "-probe-create", execution.create_argv(probe, policy.image_id))
        _require(created_output.strip().decode("ascii") == row["probe_id"], "Probe created ID differs")
        request = engine.strict_json_loads(step.request_json)
        input_raw = wire.build_probe_input(request, recipe.port, policy.wire_limits)
        _require(reader.raw(label + "-probe-input.json") == input_raw, "Original helper input differs")
        _same(reader.json(label + "-probe-source.json"), {"helper_sha256": wire.helper_sha256(),
            "request_input_sha256": sha256(input_raw), "request_sha256": sha256(wire.request_bytes(request, recipe.port)),
            "argv": execution.PROBE_ARGV, "server_epoch": row["epoch"], "server_id": baseline["Id"]},
            "Original helper source/request record differs")
        stdout = _probe_process(reader, row, probe, donor, policy, runtime)
        for suffix, expected, role in (("-server-after", baseline, spec), ("-probe-keeper-after", keeper, keeper_spec)):
            after = reader.inspect(label + suffix, expected["Id"])
            execution.validate_state(after, "running")
            reader.comparison(label + suffix + "-continuity", expected, after, role, runtime, "running-to-running")
        for field_name, suffix in (("continuity_before", "-server-before-continuity"),
            ("continuity_after", "-server-after-continuity"), ("keeper_continuity", "-probe-keeper-after-continuity")):
            _same(row[field_name], reader.json(label + suffix + ".json"), "Observation continuity copy differs")
        observation = wire.decode_probe_output(stdout, request, recipe.port, policy.wire_limits)
        _require(row["sent_complete"] is observation.sent_complete and row["exchange_complete"] is observation.exchange_complete,
                 "Original wire counters differ")
        facts, extracted = _facts(observation, policy.wire_limits)
        ranges = tuple((name, value.start, value.end) for name, value in
            (("status", extracted.status_range), ("headers", extracted.headers_range)) if value is not None)
        control = _json(stdout)
        stream_path = row["process"]["stdout"]["path"]
        def reference(field_name: str, pointer: str, raw: bytes, stage: str,
                      spans: tuple[tuple[int, int], ...] = ()) -> FieldEvidence:
            return FieldEvidence(field_name, stream_path, sha256(stdout), len(stdout), pointer,
                                 sha256(raw), len(raw), stage, spans)
        field_evidence = [reference("status", "/received", observation.received, "response",
                            ((extracted.status_range.start, extracted.status_range.end),) if extracted.status_range else ()),
            reference("headers", "/received", observation.received, "response",
                      ((extracted.headers_range.start, extracted.headers_range.end),) if extracted.headers_range else ()),
            reference("body", "/received", observation.received, "transfer-decoded only when framing complete")]
        for stage, snapshot in (("before", observation.listeners_before), ("after", observation.listeners_after)):
            for family, raw in (("tcp", snapshot.tcp), ("tcp6", snapshot.tcp6)):
                field_evidence.append(reference("listener_" + stage + "." + family,
                    "/listeners_" + stage + "/tables/" + family + "/raw", raw,
                    observation.listeners_before_stage if stage == "before" else "after_probe"))
        output.append(StepObservation(step.step_id, index, True, False, reader.bound.physical_origin, facts,
            tuple(dict.fromkeys((*observation.limitations, *extracted.limitations))),
            provenance + (("helper_stdout_sha256", sha256(stdout)), ("received_sha256", sha256(observation.received)),
                ("listeners_before_stage", observation.listeners_before_stage)), ranges,
            observation.sent_complete, observation.exchange_complete, control["connect_attempts"], control["connect_refused_attempts"], tuple(field_evidence)))
    _require(terminal["observed_probe_records"] == len(output)
             and terminal["observed_requests"] == sum(x.eligible and x.sent_complete is True for x in output)
             and terminal["unknown_request_outcomes"] == sum(not x.eligible for x in output), "Original observation census differs")
    cleanup = _cleanup_and_census(reader, terminal, intent, runtime, policy)
    return HistoryObservation(reader.bound.origin_id, "historical_physical_qualification_reanalysis" if reader.bound.physical_origin
        else "synthetic_fixture", reader.bound.physical_origin, tuple(output), tuple(x.step_id for x in recipe.steps[len(rows):]),
        cleanup, tuple(terminal["infrastructure"]), (("planned_requests", len(planned)), ("observed_rows", len(output)),
        ("authenticated_sent_complete", sum(x.eligible and x.sent_complete is True for x in output)),
        ("unknown_request_outcomes", sum(not x.eligible for x in output)),
        ("complete_exchanges", sum(x.eligible and x.exchange_complete is True for x in output))),
        reader.bound.reanalysis_source_pins, reader.bound.origin_manifest_sha256)



def _manual_cleanup(reader: _Reader, registration: dict[str, Any], probe: execution.RoleSpec,
                    probe_value: dict[str, Any], running: dict[str, dict[str, Any]],
                    policy: execution.HttpPolicy) -> bool:
    cleanup = reader.json("cleanup.json")
    census = reader.json("physical-census.json")
    _same(cleanup["containers"], census["cleanup"], "Manual cleanup census differs")
    _same(cleanup["errors"], census["cleanup_errors"], "Manual cleanup error census differs")
    if cleanup["errors"]:
        return False
    specs = {"probe": probe, "server": _role(registration["server"]), "keeper": _role(registration["keeper"])}
    values = {"probe": probe_value, **running}
    _require(len(cleanup["containers"]) == 3 and {x["role"] for x in cleanup["containers"]} == set(specs),
             "Manual owned cleanup roster differs")
    for item in cleanup["containers"]:
        role, cid = item["role"], item["container_id"]
        spec = specs[role]
        _require(cid == values[role]["Id"] and item["absence_verified"] is True,
                 "Manual cleanup ID or absence differs")
        _, raw = reader.command(role + "-cleanup-owned", ["docker", "inspect", "--format", "{{json .}}", cid])
        owned = _json(raw)
        _require(owned["Id"] == cid and owned["Name"] == "/" + spec.name and owned["Image"] == policy.image_id
                 and owned["Config"]["Labels"] == dict(spec.labels), "Manual cleanup ownership differs")
        if role == "probe":
            execution.validate_state(owned, "created")
        else:
            reader.command(role + "-stop", ["docker", "stop", "--time", "5", cid])
            _, stopped_raw = reader.command(role + "-stopped", ["docker", "inspect", "--format", "{{json .}}", cid])
            stopped = _json(stopped_raw)
            _require(stopped["Id"] == cid, "Manual stopped resource ID differs")
            execution.validate_state(stopped, "exited")
        _require(item["stop_proven_or_never_started"] is True and item["forced_remove_requested"] is False,
                 "Manual removal lifecycle differs")
        reader.command(role + "-remove", ["docker", "rm", cid])
        _, absence = reader.command(role + "-absent", ["docker", "container", "ls", "--all", "--no-trunc",
            "--filter", "id=" + cid, "--format", "{{.ID}}"])
        _require(not absence.strip(), "Manual removed resource still present")
    volume = registration["volume"]
    _, raw = reader.command("volume-cleanup-inspect", ["docker", "volume", "inspect", "--format", "{{json .}}", volume])
    observed = _json(raw)
    _require(observed["Name"] == volume and observed["Options"] == execution.VOLUME_OPTIONS
             and observed["Labels"] == {"gossip.execution": dict(probe.labels)["gossip.execution"]},
             "Manual volume ownership differs")
    reader.command("volume-remove", ["docker", "volume", "rm", volume])
    _, absent = reader.command("volume-absent", ["docker", "volume", "ls", "--filter",
        "name=^" + volume + "$", "--format", "{{.Name}}"])
    _require(not absent.strip() and cleanup["volume_absence_verified"] is True,
             "Manual volume absence unavailable")
    return True


def _manual_history(reader: _Reader) -> HistoryObservation:
    registration = reader.definition["registration"]
    _same(reader.json("prospective-registration.json"), registration, "Original manual registration differs")
    _require(registration["purpose"] == "harness_qualification" and registration["declared_requests"] == 0
             and registration["declared_helper_starts"] == 0 and registration["control"]["targets"] == [],
             "Manual control cannot supply request observations")
    files = _git_identity(reader.definition, registration["source_commit"], registration["source_tree"])
    _require(execution.source_sha256(files) == registration["source_sha256"] and registration["helper_sha256"] == wire.helper_sha256(),
             "Manual source/helper identity differs")
    for name, raw in files.items():
        _require(reader.raw("source/" + name) == raw, "Manual staged source differs")
    _require(reader.raw("probe/helper.py") == wire.helper_source(), "Manual helper source differs")
    policy, runtime = _policy(registration["policy"]), registration["runtime"]
    prospective = reader.json("probe-prospective-role.json")
    probe = _role(prospective["expected_role"])
    values: list[dict[str, Any]] = []
    for label in ("probe-created", "probe-after-rejection"):
        _, raw = reader.command(label)
        value = _json(raw)
        reader.command(label, ["docker", "inspect", "--format", "{{json .}}", value["Id"]])
        execution.validate_state(value, "created")
        _require(value["HostConfig"]["NetworkMode"] == "none", "Manual wrong-network control changed")
        try:
            execution.validate_role(value, probe, policy.image_id, runtime)
        except ValueError:
            pass
        else:
            raise ObservationError("Original wrong-network role unexpectedly admitted")
        corrected = _json(encoded(value))
        corrected["HostConfig"]["NetworkMode"] = "container:" + probe.server_id
        execution.validate_role(corrected, probe, policy.image_id, runtime)
        values.append(value)
    _same(values[0], values[1], "Manual rejected helper changed or started")
    _same(prospective["correct_argv"], execution.create_argv(probe, policy.image_id), "Manual expected probe argv differs")
    actual = ["--network=none" if x == "--network=container:" + probe.server_id else x
              for x in execution.create_argv(probe, policy.image_id)]
    _same(prospective["actual_create_argv"], actual, "Manual altered fields exceed network control")
    manual_running: dict[str, dict[str, Any]] = {}
    for role_name in ("server", "keeper"):
        spec = _role(registration[role_name])
        snapshots = []
        for suffix in ("running", "after-rejection"):
            _, raw = reader.command(role_name + "-" + suffix)
            value = _json(raw)
            reader.command(role_name + "-" + suffix, ["docker", "inspect", "--format", "{{json .}}", value["Id"]])
            execution.validate_state(value, "running")
            execution.validate_role(value, spec, policy.image_id, runtime)
            snapshots.append(value)
        _require(execution.role_identity_comparison(snapshots[0], snapshots[1], spec, runtime, "running-to-running")["matches"],
                 "Manual donor/keeper continuity differs")
        manual_running[role_name] = snapshots[0]
        if role_name == "server":
            _require(snapshots[0]["Id"] == probe.server_id, "Manual donor ID differs")
    _require(not any("probe-start" in name or "attach-response" in name for name in reader.files), "Manual helper unexpectedly started")
    census = reader.json("physical-census.json")
    _require(all(census[k] == 0 and type(census[k]) is int for k in
        ("planned_declared_requests", "observed_probe_records", "authenticated_sent_complete_requests", "helper_starts")),
        "Manual zero-request census differs")
    rejection = reader.json("observed-rejection.json")
    _require(rejection["actual_inspection_sha256"] == digest(values[0]) and rejection["declared_request_count"] == 0
             and rejection["helper_start_count"] == 0, "Manual rejection record differs")
    return HistoryObservation(reader.bound.origin_id, "historical_physical_qualification_reanalysis" if reader.bound.physical_origin
        else "synthetic_fixture", reader.bound.physical_origin, (), (), _manual_cleanup(reader, registration, probe, values[0], manual_running, policy), (),
        (("planned_requests", 0), ("observed_rows", 0), ("authenticated_sent_complete", 0),
         ("unknown_request_outcomes", 0), ("complete_exchanges", 0)), reader.bound.reanalysis_source_pins,
        reader.bound.origin_manifest_sha256)


def observe_history(bound: BoundJournal) -> HistoryObservation:
    """Reconstruct eligible facts; never dispatch or infer missing product scope."""
    _require(type(bound) is BoundJournal and bound._seal is _SEAL, "Admitted original journal required")
    issued = _ISSUED.get(id(bound))
    _require(issued is not None and issued[0]() is bound and issued[1] == _admitted_fingerprint(bound),
             "Exact unchanged admitted object required; caller copies confer no authority")
    _loaded_sources_unchanged()
    _same(bound.reanalysis_source_pins, reanalysis_sources(), "Loaded reanalysis source drift")
    reader = _Reader(bound)
    try:
        return _manual_history(reader) if reader.definition["kind"] == "manual_zero_request" else _journal_history(reader)
    except ObservationError:
        raise
    except (ValueError, KeyError, TypeError, IndexError, UnicodeError) as error:
        raise ObservationError("Original evidence structure/proof invalid: " + str(error)) from error



def _loaded_definitions_match(module: Any) -> None:
    """Compare source-declared code with an already imported frozen module.

    Compile trusted evaluator source only, without executing it. This catches
    pre-adapter imports of different code even if the on-disk hash now matches.
    Generated dataclass methods are not source declarations; source-defined
    methods, properties and wrapped functions are checked recursively.
    """
    source = Path(module.__file__).read_bytes()
    expected = compile(source, module.__file__, "exec", dont_inherit=True)
    def check(code: types.CodeType, namespace: dict[str, Any]) -> None:
        for item in code.co_consts:
            if not isinstance(item, types.CodeType) or item.co_name.startswith("<"):
                continue
            target = namespace.get(item.co_name)
            if isinstance(target, type):
                check(item, dict(vars(target)))
                continue
            if isinstance(target, (classmethod, staticmethod)):
                target = target.__func__
            if isinstance(target, property):
                target = target.fget
            while hasattr(target, "__wrapped__"):
                target = getattr(target, "__wrapped__")
            _require(isinstance(target, types.FunctionType) and target.__code__ == item,
                     "Previously imported evaluator code differs: " + module.__name__ + "." + item.co_qualname)
    check(expected, vars(module))


def _loaded_sources_unchanged() -> None:
    _same(_LOADED_REANALYSIS_SOURCES, reanalysis_sources(), "Loaded reanalysis source drift")
    _require(head.LOADED_SOURCE_SHA256 == dict(_LOADED_REANALYSIS_SOURCES)["candidate_http_head_v1.py"]
             and semantics.LOADED_SOURCE_SHA256 == dict(_LOADED_REANALYSIS_SOURCES)["candidate_http_semantics_v1.py"],
             "Previously imported semantic/head code differs from source")
    _same(execution.evaluator_sources(), execution._LOADED_SOURCES, "Loaded frozen evaluator source drift")
    for name in execution._LOADED_SOURCES:
        filename = name.split("/", 1)[1]
        if "/" not in filename and filename.endswith(".py"):
            module = sys.modules.get("gossip_harness." + filename.removesuffix(".py"))
            if module is not None:
                _loaded_definitions_match(module)


_LOADED_REANALYSIS_SOURCES = reanalysis_sources()
