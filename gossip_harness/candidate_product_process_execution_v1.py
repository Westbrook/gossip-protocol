"""Prospectively admitted cumulative-product CLI/HTTP process histories.

Explicit fork of HTTP execution v4. Provenance, admission, checkpoints, distinct
candidate processes, transport evidence, bounded lifetime and emergency cleanup
remain mandatory. A versioned trusted keeper setup seeds declared initial state
before candidate dispatch; the keeper never imports or executes candidate code.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, replace
import base64
import json
from pathlib import Path
import re
import socket
import subprocess
import tempfile
import threading
import time
from typing import Any, Callable
import uuid

from . import candidate_client_execution_v4 as finite
from . import candidate_observation_admission_v1 as admission
from . import project_acceptance_registry_v1 as registry
from . import candidate_client_process_v4 as engine
from . import candidate_http_transport_v1 as wire
from . import candidate_product_process_core_v1 as core
from . import candidate_http_inputs_v1 as input_staging
from . import candidate_http_journal_v3 as journal
from . import candidate_checkpoint_chain_v1 as compact
from . import candidate_checkpoint_head_v1 as head
from . import candidate_execution_journal_v1 as owner_journal
from . import candidate_emergency_cleanup_v1 as emergency
from . import candidate_retention_process_v1 as retained_process
from . import candidate_source_capture_policy_v1 as source_capture
from .candidate_release_execution_v2 import capture_git_source, source_manifest
from . import cumulative_scope_source_v1 as map_a
from .gitstore import GitStore
from .sandbox import DockerValidator

PROTOCOL = "candidate-product-process-execution-v1"
BATCH_PROTOCOL = PROTOCOL + "-git-source-batch-v1"
TARGET_CONTRACT_SHA256 = "2d88ce0775888f148b0ec3caf90b3d5c82d8fed71f53bec5f7e75f492ae998dc"
ROLE_POLICY = "candidate-product-distinct-server-seeded-keeper-probe-cli-v1"
SNAPSHOT_PROTOCOL = "docker-owned-product-epochs-seeded-tmpfs-keeper-v1"
VOLUME_OPTIONS = dict(finite.VOLUME_OPTIONS)
MAX_RECORD_BYTES = 32 * 1024 * 1024
MAX_JOURNAL_BYTES = 512 * 1024 * 1024
MAX_JOURNAL_FILES = 16384
CONTROL_LIMIT = 1024 * 1024
MAX_STEPS = 128
CLEANUP_RESERVE_BYTES = 96 * 1024 * 1024
CLEANUP_RESERVE_FILES = 512
TERMINAL_LIMIT_BYTES = 1024 * 1024
OPERATION_LIMIT_BYTES = 128 * 1024 * 1024
OPERATION_LIMIT_FILES = 256
CLEANUP_SECONDS = 300
CLEANUP_METADATA_LIMIT = 1024 * 1024
ENGINE_CONTROL_WIRE_LIMIT = engine.HEADER_LIMIT + CONTROL_LIMIT + engine.FRAME_COUNT_LIMIT * 20
PROBE_ARGV = ("python", "-I", "/probe/helper.py", "/probe/request.json")
PROBE_TMPFS = "rw,nosuid,nodev,noexec,size=32m,mode=1777"
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_STEP = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
_NAME = re.compile(r"[a-z][a-z0-9.-]{0,100}\Z")
ExecutionError = finite.ExecutionError
ExecutionUnknown = finite.ExecutionUnknown
ControllerCheckpoint = compact.PrefixCommitment
require = finite.require
encoded = finite.encoded
sha256 = finite.sha256
digest = finite.digest
_read = journal.read


SETUP_PROTOCOL = "candidate-product-initial-state-setup-v1"
MAX_SEED_BYTES = 256 * 1024
SETUP_DIRECTORIES = ("/tmp/backups", "/tmp/next")
# This program is controller-owned and receives literal data, never code or SQL.
# Chunking avoids Linux's per-argument bound for the largest declared seed.
SETUP_SOURCE = """import base64,hashlib,json,os,stat,sys
spec=json.loads(sys.argv[1])
assert spec['protocol']=='candidate-product-initial-state-setup-v1'
assert spec['directories']==['/tmp/backups','/tmp/next']
assert not os.listdir('/tmp')
for path in spec['directories']:
    os.mkdir(path,0o700)
seed=spec['seed']
if seed is not None:
    raw=base64.b64decode(''.join(sys.argv[2:]),validate=True)
    assert len(raw)==seed['bytes'] and len(raw)<=262144
    assert hashlib.sha256(raw).hexdigest()==seed['sha256']
    fd=os.open(spec['database_path'],os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'wb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    info=os.lstat(spec['database_path'])
    assert stat.S_ISREG(info.st_mode) and info.st_size==len(raw)
    with open(spec['database_path'],'rb') as stream:
        retained=stream.read(262145)
    assert retained==raw
else:
    assert len(sys.argv)==2
fd=os.open('/tmp',os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
os.fsync(fd)
os.close(fd)
ack={'protocol':spec['protocol'],'setup_sha256':hashlib.sha256(sys.argv[1].encode()).hexdigest(),'ready':True}
print(json.dumps(ack,sort_keys=True,separators=(',',':')))
"""


def setup_definition(recipe: HttpRecipe) -> dict[str, Any]:
    """Closed data-only initial-state policy, bound into the immutable recipe."""
    seed_name = recipe.seed_database_fixture
    require(seed_name is None or seed_name == "initial.sqlite", "Closed initial database fixture required")
    seed: dict[str, Any] | None = None
    if seed_name is not None:
        entries = tuple(x for x in (recipe.input_entries or ()) if x.path == seed_name)
        require(len(entries) == 1 and entries[0].kind == "file"
                and type(entries[0].data) is bytes and 0 < len(entries[0].data) <= MAX_SEED_BYTES,
                "Initial database must be a bounded declared regular fixture")
        raw = entries[0].data
        seed = {"fixture_path": seed_name, "bytes": len(raw), "sha256": sha256(raw)}
    return {"protocol": SETUP_PROTOCOL, "database_path": recipe.database_path,
            "directories": list(SETUP_DIRECTORIES), "seed": seed,
            "bootstrap_sha256": sha256(SETUP_SOURCE.encode()),
            "candidate_code_executed": False}


def setup_argv(recipe: HttpRecipe, keeper_id: str) -> list[str]:
    require(type(keeper_id) is str and _SHA.fullmatch(keeper_id) is not None, "Exact keeper ID required")
    declaration = encoded(setup_definition(recipe)).decode()
    raw = dict(recipe.fixtures)[recipe.seed_database_fixture] if recipe.seed_database_fixture else b""
    data = base64.b64encode(raw).decode("ascii")
    chunks = [data[index:index + 32768] for index in range(0, len(data), 32768)]
    return ["docker", "exec", "--user=65534:65534", keeper_id, "python", "-I", "-c", SETUP_SOURCE,
            declaration, *chunks]


def setup_expected_output(recipe: HttpRecipe) -> bytes:
    return encoded({"protocol": SETUP_PROTOCOL, "setup_sha256": digest(setup_definition(recipe)),
                    "ready": True}) + b"\n"


def _json(path: Path) -> dict[str, Any]:
    value = journal.read_json(path)
    require(type(value) is dict, "Retained JSON object required")
    return value

_sync = finite._sync


def evaluator_sources() -> dict[str, str]:
    names = ("candidate_product_process_execution_v1.py", "candidate_product_process_observation_v1.py",
        "candidate_product_process_reader_v1.py", "candidate_product_process_core_v1.py",
        "candidate_product_process_cases_v1.py", "library_m4_fixture_v1.py",
        "candidate_observation_admission_v1.py",
        "project_acceptance_registry_v1.py", "candidate_scope_consumer_v1.py", "candidate_http_head_v1.py",
        "candidate_http_transport_v1.py", "candidate_http_inputs_v1.py", "candidate_http_journal_v3.py",
        "candidate_checkpoint_chain_v1.py", "candidate_checkpoint_head_v1.py",
        "candidate_execution_journal_v1.py", "candidate_emergency_cleanup_v1.py",
        "candidate_retention_process_v1.py", "candidate_http_cases_core_v1.py",
        "candidate_http_fixtures_v1.py", "candidate_http_semantics_v1.py", "candidate_http_relations_v1.py")
    result = {"gossip_harness/" + name: sha256(Path(__file__).with_name(name).read_bytes()) for name in names}
    result.update({"finite/" + name: value for name, value in finite.evaluator_sources().items()})
    result.update(source_capture.evaluator_sources())
    contract = Path(__file__).resolve().parents[1] / "library-cumulative-product-v2.json"
    result["library-cumulative-product-v2.json"] = sha256(contract.read_bytes())
    require(result["library-cumulative-product-v2.json"] == TARGET_CONTRACT_SHA256,
            "Exact cumulative v2 contract required")
    result.update(map_a.map_a_sources())
    return result


def capture_policy_for(protocol: str) -> source_capture.BatchCapturePolicy | None:
    require(protocol in (PROTOCOL, BATCH_PROTOCOL), "Unknown product source-capture protocol")
    return None if protocol == PROTOCOL else source_capture.BatchCapturePolicy()


def capture_source(store: GitStore, commit_oid: str, *,
                   policy: source_capture.BatchCapturePolicy | None = None) -> tuple[str, dict[str, bytes]]:
    """Fresh source capture at every original boundary; no fallback or cache."""
    try:
        return source_capture.capture_registered_source(store, commit_oid, policy=policy)
    except source_capture.SourceCaptureUnavailable as failure:
        # Existing physical paths retain ValueError-family unknown observations
        # and perform their ordinary protected cleanup. Preserve interruptions.
        raise ExecutionUnknown("Complete product source capture unavailable") from failure


def role_policy_definition() -> dict[str, Any]:
    """Closed runtime compatibility rule, grounded in the exact inspected Engine."""
    return {"policy": ROLE_POLICY, "protocol": PROTOCOL,
        "plan_sha256": "567b9c72ce36fa123e10b85297931f8d6f98f4cabe3d0a56e615e5cf696b194b",
        "runtime_gate": engine.startup_policy()["runtime_gate"],
        "engine_source_commit": "6bc6209b88a7a834c91f77d848e025c79e0227a1",
        "role": "probe", "phase": "created-to-exited", "path": "Config.Hostname",
        "before": "explicit own full ID's first twelve characters",
        "after": "explicit hostname from independently bound current running server",
        "domain": "Config.Domainname exists, is an empty string, and remains exact for both roles",
        "donor": "full current inspection and baseline, runtime, source/fixture/Git, epoch and role spec",
        "other_fields": "exact immutable projection except existing complete mount-row permutation and OOM startup policy",
        "keeper_setup": {"protocol": SETUP_PROTOCOL, "volume_access": "read-write",
            "candidate_mounts": [], "candidate_code": False, "before_candidate_dispatch": True,
            "seed_bytes_limit": MAX_SEED_BYTES, "directories": list(SETUP_DIRECTORIES),
            "bootstrap_sha256": sha256(SETUP_SOURCE.encode())},
        "source_paths": ["daemon/container.go:117-126", "daemon/container_operations.go:411-424",
                         "daemon/container_operations_unix.go:543-547"]}


def role_policy_identity() -> dict[str, Any]:
    definition = role_policy_definition()
    return {"policy": ROLE_POLICY, "definition": definition, "definition_sha256": digest(definition),
            "source_sha256": sha256(Path(__file__).read_bytes())}


def source_sha256(files: dict[str, bytes]) -> str:
    return admission.source_sha256(files)


@dataclass(frozen=True)
class HttpPolicy:
    image_id: str
    wire_limits: wire.WireLimits = wire.WireLimits()
    transport_timeout_seconds: int = 15
    probe_timeout_seconds: int = 30
    stream_limit_bytes: int = 16 * 1024 * 1024
    lifetime_seconds: int = 7200
    cli_timeout_seconds: int = 30
    cli_stream_limit_bytes: int = 4 * 1024 * 1024
    stop_timeout_seconds: int = 5
    seed: int = 0

    def __post_init__(self) -> None:
        engine.ProcessPolicy(self.image_id, timeout_seconds=self.probe_timeout_seconds,
            stream_limit_bytes=self.stream_limit_bytes, frame_limit_bytes=self.stream_limit_bytes,
            transport_timeout_seconds=self.transport_timeout_seconds)
        require(type(self.wire_limits) is wire.WireLimits, "Typed wire bounds required")
        for value, bound in ((self.transport_timeout_seconds, 60), (self.probe_timeout_seconds, 120),
                             (self.cli_timeout_seconds, 120),
                             (self.lifetime_seconds, 7200), (self.stop_timeout_seconds, 30)):
            require(type(value) is int and 1 <= value <= bound, "Invalid lifecycle observation bound")
        require(self.lifetime_seconds > CLEANUP_SECONDS, "History must reserve bounded cleanup time")
        engine.ProcessPolicy(self.image_id, timeout_seconds=self.cli_timeout_seconds,
            stream_limit_bytes=self.cli_stream_limit_bytes, frame_limit_bytes=self.cli_stream_limit_bytes,
            transport_timeout_seconds=self.transport_timeout_seconds)
        require(self.cli_stream_limit_bytes <= 4 * 1024 * 1024, "Finite CLI capture envelope exceeds v3 bound")
        require(type(self.seed) is int and 0 <= self.seed < 2 ** 63, "Invalid fixture seed")
        require(self.probe_timeout_seconds > self.wire_limits.timeout_seconds,
                "Probe process deadline must exceed wire deadline")
        envelope = wire.max_probe_output_bytes(self.wire_limits)
        require(envelope <= self.stream_limit_bytes
                and engine.HEADER_LIMIT + 2 * envelope + engine.FRAME_COUNT_LIMIT * 8 <= MAX_RECORD_BYTES,
                "Derived helper/attach envelope exceeds capture bounds")


def compact_limits() -> compact.Limits:
    return compact.Limits(max_raw_file_bytes=MAX_RECORD_BYTES,
        max_raw_bytes=MAX_JOURNAL_BYTES, max_files=MAX_JOURNAL_FILES,
        cleanup_raw_bytes=CLEANUP_RESERVE_BYTES, cleanup_files=CLEANUP_RESERVE_FILES)


def quota_policy() -> dict[str, Any]:
    from .candidate_product_process_observation_v1 import SEMANTIC_LIMITS
    return {"semantic_limits": dict(SEMANTIC_LIMITS), "protocol": "candidate-product-process-joint-quota-v1", "journal_bytes": MAX_JOURNAL_BYTES,
        "checkpoint_protocol": compact.PROTOCOL, "owner_journal_protocol": owner_journal.PROTOCOL,
        "emergency_cleanup_protocol": emergency.PROTOCOL,
        "emergency_cleanup_limits": asdict(emergency.CleanupLimits()),
        "retention_process_protocol": retained_process.PROTOCOL,
        "compact_limits": asdict(compact_limits()),
        "journal_files": MAX_JOURNAL_FILES, "record_bytes": MAX_RECORD_BYTES,
        "cleanup_bytes": CLEANUP_RESERVE_BYTES, "cleanup_files": CLEANUP_RESERVE_FILES,
        "terminal_bytes": TERMINAL_LIMIT_BYTES, "operation_bytes": OPERATION_LIMIT_BYTES,
        "operation_files": OPERATION_LIMIT_FILES, "cleanup_seconds": CLEANUP_SECONDS,
        "max_unretired_containers": 3,
        "cleanup_derivation": {"docker_commands": 12, "docker_raw_bytes_per_command": 2 * CONTROL_LIMIT,
            "docker_json_bytes_per_command": CLEANUP_METADATA_LIMIT, "engine_controls": 3,
            "engine_wire_bytes_per_control": ENGINE_CONTROL_WIRE_LIMIT,
            "engine_request_bytes_per_control": 4096, "additional_metadata_records": 10,
            "metadata_bytes_per_record": CLEANUP_METADATA_LIMIT,
            "bytes_upper_bound": 12 * (2 * CONTROL_LIMIT + CLEANUP_METADATA_LIMIT)
                + 3 * (ENGINE_CONTROL_WIRE_LIMIT + 4096) + 10 * CLEANUP_METADATA_LIMIT + TERMINAL_LIMIT_BYTES,
            "files_upper_bound": 12 * 4 + 3 * 2 + 10 + 1,
            "time_meaning": "300-second bounded cleanup opportunity, not guaranteed success at all policy maxima"},
        "journal_json": {"protocol": journal.PROTOCOL, "max_depth": journal.MAX_DEPTH, "max_nodes": journal.MAX_NODES},
        "meaning": "joint observation bounds; exhaustion is unavailable"}


@dataclass(frozen=True)
class HttpStep:
    step_id: str
    kind: str
    request_json: bytes = b""
    epoch: int = 0
    root_path: str = ""
    argv: tuple[str, ...] = ()
    declaration_json: bytes = b""

    def __post_init__(self) -> None:
        require(type(self.step_id) is str and _STEP.fullmatch(self.step_id) is not None,
                "Invalid literal step identifier")
        require(type(self.kind) is str and self.kind in ("start", "probe", "stop", "cli")
                and type(self.request_json) is bytes and type(self.declaration_json) is bytes,
                "Closed lifecycle step required")
        require(type(self.epoch) is int and 0 <= self.epoch <= MAX_STEPS
                and type(self.root_path) is str and type(self.argv) is tuple
                and all(type(x) is str and x and "\x00" not in x for x in self.argv)
                and len(self.argv) <= 256 and sum(len(x.encode()) for x in self.argv) <= finite.MAX_ARGV_BYTES,
                "Invalid literal epoch/root/argv")
        if self.kind == "probe":
            value = engine.strict_json_loads(self.request_json)
            require(type(value) is dict and encoded(value) == self.request_json and not self.argv,
                    "Canonical closed probe request required")
        else:
            require(not self.request_json, "Only probe steps carry requests")
        require(self.kind != "stop" or not self.argv, "Stop is a lifecycle action")
        if self.declaration_json:
            value = journal.decode(self.declaration_json)
            require(type(value) is dict and core.encode(value) == self.declaration_json,
                    "Canonical full literal step declaration required")

    def record(self) -> dict[str, Any]:
        return {"step_id": self.step_id, "kind": self.kind, "epoch": self.epoch,
                "root_path": self.root_path, "argv": list(self.argv),
                "request": engine.strict_json_loads(self.request_json) if self.request_json else None,
                "declaration": journal.decode(self.declaration_json) if self.declaration_json else None,
                "declaration_sha256": sha256(self.declaration_json) if self.declaration_json else None}


@dataclass(frozen=True)
class HttpRecipe:
    recipe_id: str
    server_argv: tuple[str, ...]
    fixtures: tuple[tuple[str, bytes], ...]
    directories: tuple[str, ...]
    steps: tuple[HttpStep, ...]
    port: int = 8765
    database_path: str = "/tmp/library.sqlite"
    root_path: str = "/inputs"
    input_entries: tuple[core.Fixture, ...] | None = None
    row_id: str = ""
    definition_json: bytes = b""
    seed_database_fixture: str | None = None

    def __post_init__(self) -> None:
        require(type(self.recipe_id) is str and _NAME.fullmatch(self.recipe_id) is not None,
                "Invalid safe artifact recipe identifier")
        require(type(self.server_argv) is tuple and bool(self.server_argv)
                and all(type(x) is str and x and "\x00" not in x for x in self.server_argv), "Literal server argv required")
        require(type(self.port) is int and 1024 <= self.port <= 65535, "Unprivileged service port required")
        require(type(self.database_path) is str and self.database_path.startswith("/tmp/")
                and finite._safe_relative(self.database_path[5:]), "Database must bind the state mount")
        require(type(self.fixtures) is tuple and all(type(x) is tuple and len(x) == 2
                and type(x[0]) is str and type(x[1]) is bytes for x in self.fixtures)
                and len(dict(self.fixtures)) == len(self.fixtures), "Invalid immutable file fixture")
        require(type(self.directories) is tuple and all(type(x) is str for x in self.directories)
                and len(set(self.directories)) == len(self.directories), "Invalid fixture directories")
        if self.input_entries is None:
            directories = set(self.directories)
            for name in (*dict(self.fixtures), *self.directories):
                directories.update("/".join(name.split("/")[:i]) for i in range(1, len(name.split("/"))))
            entries = tuple(core.Fixture(name, "directory") for name in sorted(directories)) + tuple(
                core.Fixture(name, "file", raw) for name, raw in self.fixtures)
            object.__setattr__(self, "input_entries", entries)
        assert self.input_entries is not None
        input_staging.validate(self.input_entries)
        require(dict(self.fixtures) == {x.path: x.data for x in self.input_entries if x.kind == "file"}
                and set(self.directories) <= {x.path for x in self.input_entries if x.kind == "directory"},
                "File/directory projections differ from typed input entries")
        require(type(self.steps) is tuple and 3 <= len(self.steps) <= MAX_STEPS
                and all(type(x) is HttpStep for x in self.steps)
                and len({x.step_id for x in self.steps}) == len(self.steps), "Invalid complete ordered lifecycle")
        setup_definition(self)
        active, epoch, current_root, probes = False, 0, self.root_path, 0
        resolved: list[HttpStep] = []
        roots = {"/inputs"} | {"/inputs/" + x.path for x in self.input_entries if x.kind == "directory"}
        for step in self.steps:
            root = step.root_path or current_root
            require(root in roots, "Root must name a declared real input directory")
            if step.kind == "start":
                require(not active, "Server already active")
                epoch += 1
                active, current_root = True, root
                argv = step.argv or self.server_argv
                require(self.database_path in argv and root in argv and str(self.port) in argv,
                        "Server argv must bind DB/root/port")
            elif step.kind == "cli":
                require(not active and root == current_root and bool(step.argv),
                        "CLI requires no active server and same root lineage")
                argv = step.argv
                require(self.database_path in argv and root in argv, "CLI argv must bind DB/root")
            else:
                require(active and root == current_root, "Step needs current server/root")
                argv = ()
                if step.kind == "stop":
                    active = False
                else:
                    wire.request_bytes(engine.strict_json_loads(step.request_json), self.port)
                    probes += 1
            require(step.epoch in (0, epoch), "Declared epoch differs")
            resolved.append(replace(step, epoch=epoch, root_path=root, argv=argv))
        require(not active and probes > 0, "Complete history must stop final server and contain a request")
        object.__setattr__(self, "steps", tuple(resolved))
        require(type(self.row_id) is str and type(self.definition_json) is bytes, "Immutable row declaration required")
        if self.definition_json:
            declaration = journal.decode(self.definition_json)
            require(type(declaration) is dict and core.encode(declaration) == self.definition_json
                    and declaration.get("protocol") == core.PROTOCOL and declaration.get("row_id") == self.row_id
                    and declaration.get("seed_database_fixture") == self.seed_database_fixture
                    and core.encode(declaration.get("fixtures")) == core.encode([x.record() for x in self.input_entries])
                    and core.encode(declaration.get("steps")) == core.encode([journal.decode(x.declaration_json) for x in self.steps]),
                    "Literal row/fixture/step declaration differs")
            for step in self.steps:
                item = journal.decode(step.declaration_json)
                require(item["step_id"] == step.step_id and item["kind"] == ("request" if step.kind == "probe" else step.kind)
                        and type(item["server_epoch"]) is int and item["server_epoch"] == step.epoch and item["root"] == step.root_path
                        and item["database"] == self.database_path and item["argv"] == list(step.argv),
                        "Literal mechanics projection differs")
                if step.kind == "probe":
                    request = engine.strict_json_loads(step.request_json)
                    expected = core.HttpRequest(request["method"], request["target"],
                        tuple(tuple(x) for x in request["headers"]), base64.b64decode(request["body_b64"], validate=True), self.port)
                    require(core.encode(item["request"]) == core.encode(expected.record()), "Literal request bytes differ")
                else:
                    require(item["request"] is None, "Unexpected request declaration")
        else:
            require(not self.row_id and all(not x.declaration_json for x in self.steps),
                    "Named literal rows need full independent declarations")

    @property
    def definition_sha256(self) -> str:
        return sha256(self.definition_json) if self.definition_json else digest(self.mechanics_record())

    def mechanics_record(self) -> dict[str, Any]:
        assert self.input_entries is not None
        return {"recipe_id": self.recipe_id, "server_argv": list(self.server_argv),
                "input_manifest": input_staging.manifest(self.input_entries), "steps": [x.record() for x in self.steps],
                "port": self.port, "database_path": self.database_path, "root_path": self.root_path,
                "initial_state_setup": setup_definition(self)}

    def record(self) -> dict[str, Any]:
        return {**self.mechanics_record(), "row_id": self.row_id or self.recipe_id,
                "definition_sha256": self.definition_sha256,
                "definition": journal.decode(self.definition_json) if self.definition_json else None,
                "acceptance_authority": False}


def recipe_from_case(case: core.LiteralCase) -> HttpRecipe:
    require(type(case) is core.LiteralCase, "Typed immutable literal case required")
    case.check_current()
    first = next((step for step in case.steps if step.kind == "start"), None)
    require(first is not None, "Literal history must start a server")
    assert first is not None
    return HttpRecipe("case-" + sha256(case.row_id.encode())[:32], first.argv,
        tuple((x.path, x.data) for x in case.fixtures if x.kind == "file"),
        tuple(x.path for x in case.fixtures if x.kind == "directory"),
        tuple(HttpStep(x.step_id, "probe" if x.kind == "request" else x.kind,
            encoded(x.request.recipe()) if x.request is not None else b"", x.epoch, x.root, x.argv,
            core.encode(x.record())) for x in case.steps),
        core.PORT, core.DATABASE, case.steps[0].root, case.fixtures, case.row_id, core.encode(case.record()),
        case.seed_database_fixture)


PROFILE_PROTOCOL = "candidate-product-process-profile-v1"


@dataclass(frozen=True)
class HttpProductProfile:
    """Exact public cumulative-product definition, never a held-out relabeling."""
    case: core.LiteralCase
    original_definition_purpose: str = "public_product_definition"
    cumulative_profile: None = None
    capture_policy: source_capture.BatchCapturePolicy | None = None
    mapping_profile: str | None = None

    def __post_init__(self) -> None:
        require(type(self.case) is core.LiteralCase, "Complete typed product case required")
        require(self.original_definition_purpose == "public_product_definition"
                and self.cumulative_profile is None, "Exact public product definition required")
        require(len(self.case.steps) <= MAX_STEPS, "Case exceeds this executor's declared allocation")
        self.check_current()
        recipe_from_case(self.case)

    def check_current(self, *, purpose: str | None = None, requirements_sha256: str | None = None) -> None:
        self.case.check_current()
        require(self.mapping_profile in (None, map_a.MAP_A_MAPPING), "Unknown mapping profile")
        require(self.mapping_profile is None or self.milestone == "M4", "MAP-A requires final-M4 source")
        require(self.case.milestone in ("M2", "M3", "M4"), "Cumulative-product milestone required")
        require(self.original_definition_purpose == "public_product_definition"
                and self.cumulative_profile is None, "Public product identity changed")
        require(self.capture_policy is None or type(self.capture_policy) is source_capture.BatchCapturePolicy,
                "Exact closed product source-capture policy required")
        if self.capture_policy is not None:
            self.capture_policy.record()
        require(purpose is None or purpose in registry.PURPOSES, "Registered product purpose required")
        require(requirements_sha256 is None or requirements_sha256 == TARGET_CONTRACT_SHA256,
                "Exact cumulative v2 requirements required")

    @property
    def milestone(self) -> str:
        return str(self.case.milestone)

    @property
    def execution_protocol(self) -> str:
        return PROTOCOL if self.capture_policy is None else BATCH_PROTOCOL

    @property
    def diagnostic_case_ids(self) -> tuple[str, ...]:
        prefix = "http-" + sha256(self.case.row_id.encode())[:16]
        return tuple(prefix + "-" + str(index).zfill(3) for index in range(len(self.case.steps)))

    @property
    def mechanics_case_id(self) -> str:
        return "http-" + sha256(self.case.row_id.encode())[:16] + "-mechanics"

    @property
    def ordered_case_ids(self) -> tuple[str, ...]:
        return (*self.diagnostic_case_ids, self.mechanics_case_id)

    def record(self) -> dict[str, Any]:
        self.check_current()
        result: dict[str, Any] = {"protocol": PROFILE_PROTOCOL, "definition": self.case.record(),
            "original_definition_purpose": self.original_definition_purpose,
            "target_milestone": self.milestone, "target_contract_sha256": TARGET_CONTRACT_SHA256,
            "held_out_claim": False, "ordered_case_ids": list(self.ordered_case_ids),
            "mechanics_guard": {"protocol": "candidate-product-process-complete-mechanics-v1",
                "case_id": self.mechanics_case_id,
                "scope": "Every step: trusted setup, provenance, capture, continuity and complete cleanup",
                "semantic_credit": False},
            "aggregation": "all-required-facets-pass;known-failure-dominates-unavailable;unentered-is-infrastructure",
            "whole_project_acceptance": False,
            "diagnostic_limits": ["explicitly-unspecified-facets-do-not-prove-product-obligations",
                "public-product-definitions-are-not-independent-heldout"]}
        if self.capture_policy is not None:
            result.update(protocol=PROFILE_PROTOCOL + "-git-source-batch-v1",
                          execution_protocol=BATCH_PROTOCOL, source_capture=self.capture_policy.record())
        if self.mapping_profile is not None:
            result.update(protocol=result["protocol"] + "-map-a-v1", mapping_profile=self.mapping_record())
        return result

    def mapping_record(self) -> dict[str, Any]:
        return map_a.map_a_profile_record('product-process', self)

    @property
    def requirement_ids(self) -> tuple[str, ...]:
        return self.case.requirement_ids if self.mapping_profile is None else tuple(self.mapping_record()['requirement_ids'])

    @property
    def sha256(self) -> str:
        return digest(self.record())


@dataclass(frozen=True)
class HttpBinding:
    source_sha256: str
    requirements_sha256: str
    recipe_sha256: str
    fixture_sha256: str
    evaluator_sha256: str
    helper_sha256: str
    runtime_sha256: str
    environment_sha256: str
    limits_sha256: str
    seed_sha256: str
    role_policy_sha256: str
    role_policy_source_sha256: str
    profile_sha256: str
    purpose: str = "public_release"
    milestone: str = "M4"
    protocol: str = PROTOCOL
    cumulative_profile_sha256: str | None = None
    target_definition_sha256: str | None = None

    def __post_init__(self) -> None:
        for key, value in asdict(self).items():
            if key.endswith("_sha256"):
                if key in ("cumulative_profile_sha256", "target_definition_sha256") and value is None:
                    continue
                require(type(value) is str and _SHA.fullmatch(value) is not None, "Invalid HTTP binding digest")
        require(self.requirements_sha256 == TARGET_CONTRACT_SHA256 and self.purpose in registry.PURPOSES,
                "Only prospectively admitted cumulative-product observations are authorized")
        require(self.milestone in ("M2", "M3", "M4") and self.protocol in (PROTOCOL, BATCH_PROTOCOL)
                and self.cumulative_profile_sha256 is None and self.target_definition_sha256 is None,
                "Exact versioned cumulative-product binding required")


@dataclass(frozen=True)
class HttpRegistration:
    binding: HttpBinding
    commit_oid: str
    tree_oid: str
    repetition_id: str
    observation: admission.ObservationRegistration

    def __post_init__(self) -> None:
        require(type(self.binding) is HttpBinding and all(type(x) is str and re.fullmatch(r"[0-9a-f]{40}", x)
                for x in (self.commit_oid, self.tree_oid)), "Exact Git source registration required")
        require(type(self.repetition_id) is str and _NAME.fullmatch(self.repetition_id) is not None,
                "Explicit repetition identity required")
        require(type(self.observation) is admission.ObservationRegistration, "Product admission registration required")


def binding_for(files: dict[str, bytes], recipe: HttpRecipe, policy: HttpPolicy, runtime: dict[str, Any], *,
                requirements_sha256: str, profile: HttpProductProfile, purpose: str) -> HttpBinding:
    require(type(profile) is HttpProductProfile and recipe == recipe_from_case(profile.case),
            "The exact whole literal case must bind the acceptance profile")
    require(type(recipe) is HttpRecipe and type(policy) is HttpPolicy, "Typed recipe and policy required")
    profile.check_current(purpose=purpose, requirements_sha256=requirements_sha256)
    assert recipe.input_entries is not None
    for step in recipe.steps:
        if step.kind == "probe":
            wire.build_probe_input(engine.strict_json_loads(step.request_json), recipe.port, policy.wire_limits)
    limits = {"policy": asdict(policy), "quota": quota_policy()}
    if profile.capture_policy is not None:
        limits["source_capture"] = profile.capture_policy.record()
    return HttpBinding(source_sha256(files), requirements_sha256, digest(recipe.record()),
        digest(input_staging.manifest(recipe.input_entries)),
        digest(evaluator_sources()), wire.helper_sha256(), digest(runtime),
        digest({"environment": DockerValidator._environment(), "roles": ROLE_POLICY,
                "volume_options": VOLUME_OPTIONS, "probe_argv": PROBE_ARGV}), digest(limits),
        digest({"seed": policy.seed, "meaning": "fixed fixture; no candidate random seed implied"}),
        digest(role_policy_definition()), sha256(Path(__file__).read_bytes()), profile.sha256, purpose,
        milestone=profile.milestone, protocol=profile.execution_protocol)


def observation_registration_for(binding: HttpBinding, profile: HttpProductProfile,
        policy: HttpPolicy, *, subject: registry.Subject, gate_id: str, commit_oid: str,
        tree_oid: str, repetition_id: str, cohort_trajectory_ids: tuple[str, ...]) -> admission.ObservationRegistration:
    """Derive the entire normalized gate from independently recomputed inputs."""
    require(type(binding) is HttpBinding and type(profile) is HttpProductProfile
            and type(subject) is registry.Subject and binding.profile_sha256 == profile.sha256,
            "Exact product execution and profile required")
    profile.check_current(purpose=binding.purpose, requirements_sha256=binding.requirements_sha256)
    require(binding.milestone == profile.milestone and binding.protocol == profile.execution_protocol
            and binding.cumulative_profile_sha256 is None and binding.target_definition_sha256 is None,
            "Binding must consume the exact declared product profile")
    require(subject.milestone == binding.milestone and subject.requirements_sha256 == binding.requirements_sha256
            and subject.source_sha256 == binding.source_sha256, "Exact cumulative-product subject required")
    actual_subject = replace(subject, source_sha256=binding.source_sha256,
        requirements_sha256=binding.requirements_sha256, milestone=binding.milestone)
    gate_binding = registry.Binding(actual_subject,
        digest({"profile": profile.record(), "recipe_sha256": binding.recipe_sha256}),
        binding.evaluator_sha256, policy.image_id.removeprefix("sha256:"),
        digest({"environment_sha256": binding.environment_sha256, "runtime_sha256": binding.runtime_sha256}),
        binding.limits_sha256, binding.seed_sha256, binding.protocol, binding.purpose)
    gate = registry.Gate(gate_id, profile.requirement_ids,
        profile.ordered_case_ids, gate_binding)
    return admission.ObservationRegistration(gate, commit_oid, tree_oid, repetition_id,
        cohort_trajectory_ids, recipe_from_case(profile.case).definition_sha256,
        profile.sha256, profile.original_definition_purpose,
        admission.binding_sha256(binding, gate=gate))


@dataclass(frozen=True)
class RoleSpec:
    role: str
    name: str
    argv: tuple[str, ...]
    labels: tuple[tuple[str, str], ...]
    binds: tuple[tuple[str, str], ...]
    volume: str = ""
    server_id: str = ""

    def __post_init__(self) -> None:
        require(self.role in ("server", "keeper", "probe", "cli") and _NAME.fullmatch(self.name) is not None,
                "Closed container role required")
        require(type(self.argv) is tuple and bool(self.argv) and all(type(x) is str and x and "\x00" not in x for x in self.argv),
                "Literal role argv required")
        require(type(self.labels) is tuple and type(self.binds) is tuple
                and all(type(row) is tuple and len(row) == 2 and all(type(x) is str for x in row)
                        for row in (*self.labels, *self.binds))
                and len(dict(self.labels)) == len(self.labels), "Malformed or duplicate role metadata")
        for target, source in self.binds:
            require(target in ("/workspace", "/inputs", "/probe") and Path(source).is_absolute()
                    and str(Path(source).resolve()) == source and "," not in source, "Invalid staging bind")
        targets = {x[0] for x in self.binds}
        require(len(targets) == len(self.binds), "Duplicate staging target")
        if self.role == "probe":
            require(self.argv == PROBE_ARGV and targets == {"/probe"} and not self.volume
                    and _SHA.fullmatch(self.server_id) is not None, "Probe may share only a specific server network")
        else:
            require(not self.server_id and _NAME.fullmatch(self.volume) is not None
                    and targets == ({"/workspace", "/inputs"} if self.role in ("server", "cli") else set()),
                    "Server/keeper mount profile differs")


def create_argv(spec: RoleSpec, image_id: str) -> list[str]:
    """Closed, literal Docker configuration; no shell or candidate-selected flags."""
    argv = ["docker", "create", "--name", spec.name, "--pull=never", "--attach=stdout", "--attach=stderr",
        "--network=" + ("container:" + spec.server_id if spec.role == "probe" else "none"),
        "--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges:true", "--user=65534:65534",
        "--pids-limit=64", "--memory=256m", "--memory-swap=256m", "--cpus=1", "--ulimit=nofile=256:256",
        "--no-healthcheck", "--restart=no", "--log-driver=none", "--ipc=private", "--cgroupns=private",
        "--env=HOME=/tmp", "--env=PYTHONDONTWRITEBYTECODE=1", "--env=PYTHONNOUSERSITE=1",
        "--workdir=" + ("/workspace" if spec.role in ("server", "cli") else "/")]
    argv += ["--env=" + key + "=" for key in ("HTTP_PROXY", "HTTPS_PROXY", "FTP_PROXY", "NO_PROXY", "ALL_PROXY",
                "http_proxy", "https_proxy", "ftp_proxy", "no_proxy", "all_proxy")]
    for target, source in spec.binds:
        argv += ["--mount", f"type=bind,source={source},target={target},readonly,bind-propagation=rprivate"]
    if spec.role == "probe":
        argv += ["--tmpfs=/tmp:" + PROBE_TMPFS]
    else:
        argv += ["--mount", "type=volume,source=" + spec.volume + ",target=/tmp,volume-nocopy"]
    for key, value in spec.labels:
        argv += ["--label", key + "=" + value]
    argv += ["--entrypoint", spec.argv[0], image_id, *spec.argv[1:]]
    return argv


def validate_role(value: dict[str, Any], spec: RoleSpec, image_id: str, runtime: dict[str, Any]) -> None:
    require(type(spec) is RoleSpec and engine._startup_runtime_valid(runtime, image_id),
            "Typed role and qualified runtime required")
    engine.immutable_inspection(value)
    config, host, mounts = value["Config"], value["HostConfig"], value["Mounts"]
    require(type(config) is dict and type(host) is dict and type(mounts) is list, "Malformed role inspection")
    engine.validate_oom_kill_default(host)
    require("Hostname" in config and type(config["Hostname"]) is str and bool(config["Hostname"])
            and "Domainname" in config and type(config["Domainname"]) is str and config["Domainname"] == "",
            "Explicit hostname and empty domain required")
    require(_SHA.fullmatch(value.get("Id", "")) is not None and value["Name"] == "/" + spec.name
            and value["Image"] == image_id and config.get("Image") == image_id
            and value["Path"] == spec.argv[0] and value["Args"] == list(spec.argv[1:])
            and "Entrypoint" in config and "Cmd" in config and config.get("Entrypoint") == [spec.argv[0]]
            and (config.get("Cmd") == list(spec.argv[1:]) or (len(spec.argv) == 1 and config.get("Cmd") is None))
            and config.get("Labels") == dict(spec.labels), "Role identity/registered argv differs")
    expected_config = {"User": "65534:65534", "WorkingDir": "/workspace" if spec.role in ("server", "cli") else "/",
        "Tty": False, "OpenStdin": False, "StdinOnce": False, "AttachStdout": True, "AttachStderr": True,
        "Healthcheck": {"Test": ["NONE"]}}
    require(all(key in config and encoded(config[key]) == encoded(expected) for key, expected in expected_config.items()),
            "Role process restrictions differ")
    expected_host = {"NetworkMode": "container:" + spec.server_id if spec.role == "probe" else "none",
        "ReadonlyRootfs": True, "Privileged": False, "AutoRemove": False, "Memory": 256 * 1024 * 1024,
        "MemorySwap": 256 * 1024 * 1024, "NanoCpus": 1000000000, "PidsLimit": 64,
        "CapDrop": ["ALL"], "SecurityOpt": ["no-new-privileges:true"], "LogConfig": {"Type": "none", "Config": {}},
        "RestartPolicy": {"Name": "no", "MaximumRetryCount": 0},
        "Ulimits": [{"Name": "nofile", "Hard": 256, "Soft": 256}], "IpcMode": "private", "CgroupnsMode": "private"}
    require(all(key in host and encoded(host[key]) == encoded(expected) for key, expected in expected_host.items())
            and (host.get("Init") is None or host.get("Init") is False)
            and all(not host.get(key) for key in ("CapAdd", "Devices", "DeviceRequests", "VolumesFrom", "Links",
                "PortBindings", "PublishAllPorts", "PidMode", "UTSMode", "UsernsMode", "ExtraHosts", "Dns")),
            "Role namespace/resource restrictions differ")
    require((host.get("Tmpfs") == {"/tmp": PROBE_TMPFS} if spec.role == "probe"
             else host.get("Tmpfs") is None or host.get("Tmpfs") == {}),
            "Role private temporary filesystem differs")
    environment = config.get("Env")
    require(type(environment) is list and all(type(x) is str and "=" in x for x in environment), "Malformed environment")
    env = dict(x.split("=", 1) for x in environment)
    require(len(env) == len(environment) and env.get("HOME") == "/tmp" and env.get("PYTHONDONTWRITEBYTECODE") == "1"
            and env.get("PYTHONNOUSERSITE") == "1" and all(env.get(k) == "" for k in ("HTTP_PROXY", "HTTPS_PROXY",
                "FTP_PROXY", "NO_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "ftp_proxy", "no_proxy", "all_proxy")),
            "Role environment differs")
    require(all(type(row) is dict for row in mounts), "Malformed mount inventory")
    inventory = {row.get("Destination"): row for row in mounts}
    expected_targets = {x[0] for x in spec.binds} | ({"/tmp"} if spec.role != "probe" else set())
    require(len(inventory) == len(mounts) and set(inventory) == expected_targets, "Role mount inventory differs")
    for target, source in spec.binds:
        row = inventory[target]
        require(row.get("Type") == "bind" and row.get("Source") == source and row.get("RW") is False
                and row.get("Propagation") == "rprivate", "Read-only exact staging mount differs")
    if spec.role != "probe":
        row = inventory["/tmp"]
        require(row.get("Type") == "volume" and row.get("Driver") == "local" and row.get("Name") == spec.volume
                and row.get("RW") is True, "Owned DB volume role differs")


def validate_state(value: dict[str, Any], state_name: str) -> None:
    state = value.get("State")
    require(type(state) is dict and state_name in ("created", "running", "exited"), "Unknown lifecycle state")
    assert isinstance(state, dict)
    require(state.get("Status") == state_name and state.get("Running") is (state_name == "running")
            and all(state.get(k) is False for k in ("Paused", "Restarting", "Dead", "OOMKilled"))
            and state.get("Error") == "" and type(value.get("RestartCount")) is int and value["RestartCount"] == 0,
            "Role lifecycle state differs")
    require(type(state.get("Pid")) is int and (state["Pid"] > 0 if state_name == "running" else state["Pid"] == 0),
            "Role PID state differs")
    started = engine._timestamp(state.get("StartedAt"))
    if state_name == "created":
        require(started is None and str(state.get("StartedAt", "")).startswith("0001-"), "Role ran before dispatch")
    else:
        created = engine._timestamp(value.get("Created"))
        require(started is not None and created is not None and created <= started, "Role start chronology differs")
    if state_name == "exited":
        finished = engine._timestamp(state.get("FinishedAt"))
        require(finished is not None and started is not None and started <= finished, "Role stop chronology differs")


def role_identity_comparison(before: dict[str, Any], after: dict[str, Any], spec: RoleSpec,
                             runtime: dict[str, Any], phase: str, *,
                             donor: ProbeDonorEvidence | None = None) -> dict[str, Any]:
    """Permit only declared typed transitions, preserving complete raw evidence."""
    require(phase in ("created-to-prestart", "created-to-running", "created-to-exited",
                      "running-to-running", "running-to-exited"), "Unqualified role transition")
    require(donor is None or (spec.role == "probe" and phase == "created-to-exited"),
            "Donor authority is restricted to probe created-to-exited")
    validate_role(before, spec, runtime["image_id"], runtime)
    validate_role(after, spec, runtime["image_id"], runtime)
    left, right = (engine.strict_json_loads(encoded(engine.immutable_inspection(x))) for x in (before, after))
    transformations: list[str] = []
    for value in (left, right):
        value["Mounts"] = {x["Destination"]: x for x in value["Mounts"]}
    old, new = left["HostConfig"]["OomKillDisable"], right["HostConfig"]["OomKillDisable"]
    if old is False and new is None and phase in ("created-to-running", "created-to-exited"):
        right["HostConfig"]["OomKillDisable"] = False
        transformations.append("pinned-runtime-startup-oom-false-to-null")
    else:
        require(old is new, "Unqualified OOM default transition")
    changed_fields: list[dict[str, Any]] = []
    if spec.role == "probe" and phase == "created-to-exited":
        require(type(donor) is ProbeDonorEvidence, "Independent current-server donor evidence required")
        assert donor is not None
        donor.validate_probe(spec, runtime)
        validate_state(before, "created")
        validate_state(after, "exited")
        require(before["Id"] == after["Id"] and before["Id"] != donor.server_id
                and left["Config"]["Hostname"] == before["Id"][:12]
                and right["Config"]["Hostname"] == donor.hostname,
                "Probe hostname does not follow independently bound donor startup")
        changed_fields.append({"path": "Config.Hostname", "before": left["Config"]["Hostname"],
            "after": right["Config"]["Hostname"], "comparison_after": left["Config"]["Hostname"],
            "donor_inspection_sha256": digest(donor.inspection), "donor_record_sha256": digest(donor.record())})
        right["Config"]["Hostname"] = left["Config"]["Hostname"]
        transformations.append("pinned-runtime-probe-donor-hostname")
    matches = encoded(left) == encoded(right)
    if phase.startswith("running-"):
        matches = matches and encoded(before["State"].get("StartedAt")) == encoded(after["State"].get("StartedAt"))
        if phase == "running-to-running":
            matches = matches and type(before["State"].get("Pid")) is int and before["State"]["Pid"] > 0 \
                and before["State"]["Pid"] == after["State"].get("Pid")
    return {"policy": ROLE_POLICY, "role": spec.role, "phase": phase, "matches": bool(matches),
            "before_sha256": digest(before), "after_sha256": digest(after), "runtime_sha256": digest(runtime),
            "transformations": transformations, "changed_fields": changed_fields,
            "policy_identity": role_policy_identity(),
            "donor_sha256": digest(donor.record()) if donor is not None else None,
            "comparison_sha256": digest([left, right])}


@dataclass(frozen=True)
class ProbeDonorEvidence:
    """Immutable complete independently observed donor; hashes alone confer no authority."""
    server_spec: RoleSpec
    registration: HttpRegistration
    epoch: int
    baseline_json: bytes
    inspection_json: bytes
    runtime_json: bytes

    def __post_init__(self) -> None:
        require(type(self.server_spec) is RoleSpec and self.server_spec.role == "server"
                and type(self.registration) is HttpRegistration and type(self.epoch) is int
                and 1 <= self.epoch <= MAX_STEPS, "Typed source-bound server donor required")
        for raw in (self.baseline_json, self.inspection_json, self.runtime_json):
            require(type(raw) is bytes, "Canonical complete donor evidence required")
            value = engine.strict_json_loads(raw)
            require(type(value) is dict and encoded(value) == raw, "Canonical complete donor evidence required")
        baseline = engine.strict_json_loads(self.baseline_json)
        current, runtime = self.inspection, self.runtime
        binding = self.registration.binding
        require(digest(runtime) == binding.runtime_sha256
                and binding.role_policy_sha256 == digest(role_policy_definition())
                and binding.role_policy_source_sha256 == sha256(Path(__file__).read_bytes()),
                "Donor runtime or policy differs from registration")
        labels = dict(self.server_spec.labels)
        require(labels.get("gossip.role") == "server" and labels.get("gossip.source") == binding.source_sha256
                and labels.get("gossip.fixture") == binding.fixture_sha256
                and labels.get("gossip.helper") == binding.helper_sha256
                and labels.get("gossip.epoch") == str(self.epoch)
                and type(labels.get("gossip.execution")) is str and bool(labels["gossip.execution"]),
                "Donor source, fixture or current epoch differs")
        for value in (baseline, current):
            validate_role(value, self.server_spec, runtime["image_id"], runtime)
            validate_state(value, "running")
        require(role_identity_comparison(baseline, current, self.server_spec, runtime,
                    "running-to-running")["matches"] is True, "Donor current running continuity differs")

    @property
    def inspection(self) -> dict[str, Any]:
        return engine.strict_json_loads(self.inspection_json)

    @property
    def runtime(self) -> dict[str, Any]:
        return engine.strict_json_loads(self.runtime_json)

    @property
    def server_id(self) -> str:
        return str(self.inspection["Id"])

    @property
    def hostname(self) -> str:
        return str(self.inspection["Config"]["Hostname"])

    def validate_probe(self, spec: RoleSpec, runtime: dict[str, Any]) -> None:
        # Revalidate full immutable evidence, never merely compare caller-supplied hashes.
        self.__post_init__()
        require(type(spec) is RoleSpec and spec.role == "probe" and spec.server_id == self.server_id
                and encoded(runtime) == self.runtime_json, "Probe donor ID, role or runtime differs")
        server_labels, probe_labels = dict(self.server_spec.labels), dict(spec.labels)
        require(probe_labels.get("gossip.role") == "probe" and all(probe_labels.get(key) == server_labels[key]
                for key in ("gossip.execution", "gossip.source", "gossip.fixture", "gossip.epoch", "gossip.helper")),
                "Probe donor source or current epoch differs")

    def record(self) -> dict[str, Any]:
        return {"protocol": "candidate-http-probe-donor-v3", "policy": role_policy_identity(),
            "server_id": self.server_id, "epoch": self.epoch, "hostname": self.hostname,
            "domainname": self.inspection["Config"]["Domainname"], "inspection_sha256": sha256(self.inspection_json),
            "baseline_sha256": sha256(self.baseline_json), "runtime_sha256": sha256(self.runtime_json),
            "binding": asdict(self.registration.binding), "commit_oid": self.registration.commit_oid,
            "tree_oid": self.registration.tree_oid, "server_spec": asdict(self.server_spec),
            "baseline": engine.strict_json_loads(self.baseline_json), "inspection": self.inspection, "runtime": self.runtime}


def bind_probe_donor(*, baseline: dict[str, Any], current: dict[str, Any], server_spec: RoleSpec,
                     registration: HttpRegistration, runtime: dict[str, Any], epoch: int) -> ProbeDonorEvidence:
    return ProbeDonorEvidence(server_spec, registration, epoch, encoded(baseline), encoded(current), encoded(runtime))


@dataclass(frozen=True)
class HttpHistoryResult:
    execution_id: str
    status: str
    observations: tuple[dict[str, Any], ...]
    missing_step_ids: tuple[str, ...]
    cleanup_verified: bool
    infrastructure: tuple[str, ...]
    terminal_sha256: str
    checkpoint: ControllerCheckpoint
    cli_observations: tuple[dict[str, Any], ...] = ()


def run_probe(endpoint: engine.EngineEndpoint, *, expected: dict[str, Any], spec: RoleSpec,
              policy: HttpPolicy, runtime: dict[str, Any], retain: Callable[[str, bytes], None],
              label: str, donor: ProbeDonorEvidence, history_deadline: float | None = None,
              execution_protocol: str = PROTOCOL) -> dict[str, Any]:
    """A distinct finite trusted role, authenticated by raw Engine attach/wait/inspect."""
    def bounded(seconds: float) -> float:
        deadline = time.monotonic() + seconds
        if history_deadline is not None:
            deadline = min(deadline, history_deadline)
        require(deadline > time.monotonic(), "History observation deadline exhausted")
        return deadline
    require(spec.role == "probe", "Only the fixed probe has finite completion authority")
    require(execution_protocol in (PROTOCOL, BATCH_PROTOCOL), "Closed HTTP execution protocol required")
    validate_role(expected, spec, policy.image_id, runtime)
    validate_state(expected, "created")
    require(type(donor) is ProbeDonorEvidence, "Typed precreation donor required")
    donor.validate_probe(spec, runtime)
    limit = wire.max_probe_output_bytes(policy.wire_limits)
    require(limit <= policy.stream_limit_bytes, "Trusted transcript exceeds declared process capture")
    process_policy = engine.ProcessPolicy(policy.image_id, timeout_seconds=policy.probe_timeout_seconds,
        stream_limit_bytes=limit, frame_limit_bytes=limit, transport_timeout_seconds=policy.transport_timeout_seconds)
    decoder = engine.MultiplexDecoder(process_policy)
    prefix = "/containers/" + expected["Id"]
    raw_wire: engine._Wire | None = None
    reader: threading.Thread | None = None
    errors: list[BaseException] = []
    started = False
    eof_after_start: bool | None = None
    final: dict[str, Any] = {}
    waited: dict[str, Any] = {}
    comparison: dict[str, Any] | None = None
    start_response: dict[str, Any] | None = None
    evidence: dict[str, dict[str, Any]] = {}
    retention_failure: BaseException | None = None
    def save(name: str, raw: bytes) -> None:
        nonlocal retention_failure
        if retention_failure is not None:
            raise retention_failure
        path = label + "-" + name
        require(name not in evidence, "Repeated probe evidence")
        try:
            retain(path, raw)
        except BaseException as error:
            retention_failure = error
            raise
        evidence[name] = {"path": path, "bytes": len(raw), "sha256": sha256(raw)}
    def response_observed(code: int) -> None:
        nonlocal started
        if code == 204:
            started = True
    def drain() -> None:
        nonlocal eof_after_start
        assert raw_wire is not None
        try:
            while True:
                if raw_wire.buffer:
                    chunk = bytes(raw_wire.buffer)
                    raw_wire.buffer.clear()
                    decoder.feed(chunk)
                if raw_wire.eof:
                    eof_after_start = started
                    require(eof_after_start is True, "Probe attach EOF preceded start acknowledgement")
                    decoder.eof()
                    return
                raw_wire.receive()
        except (OSError, ValueError) as error:
            errors.append(error)
    status = "completion_unproven"
    error_text: str | None = None
    try:
        save("intent.json", encoded({"protocol": execution_protocol, "role": asdict(spec), "expected": digest(expected),
            "runtime": runtime, "policy": asdict(policy), "role_policy": role_policy_identity(),
            "donor": donor.record(), "helper_stdout_envelope": limit,
            "helper_sha256": wire.helper_sha256()}))
        before = engine._json_control(endpoint, prefix + "/json", deadline=bounded(policy.transport_timeout_seconds),
                                      retain=save, label="prestart")
        validate_state(before, "created")
        prestart = role_identity_comparison(expected, before, spec, runtime, "created-to-prestart")
        save("prestart-comparison.json", encoded(prestart))
        require(prestart["matches"], "Probe changed before start")
        request = engine._request("POST", prefix + "/attach?stream=1&stdout=1&stderr=1&stdin=0&logs=0", upgrade=True)
        save("attach-request.bin", request)
        raw_wire = engine._Wire(endpoint, bounded(policy.transport_timeout_seconds),
            engine.HEADER_LIMIT + 2 * limit + engine.FRAME_COUNT_LIMIT * 8)
        raw_wire.send(request)
        code, headers = raw_wire.headers()
        require(code == 101 and headers.get("connection", "").lower() == "upgrade"
                and headers.get("upgrade", "").lower() == "tcp"
                and headers.get("content-type") in ("application/vnd.docker.raw-stream", "application/vnd.docker.multiplexed-stream")
                and "content-length" not in headers and "transfer-encoding" not in headers and not raw_wire.buffer,
                "Probe raw attachment failed")
        raw_wire.socket.settimeout(0)
        try:
            raw_wire.socket.recv(1, socket.MSG_PEEK)
        except BlockingIOError:
            pass
        else:
            raise ExecutionError("Probe attachment ended or produced data before start")
        finally:
            raw_wire._timeout()
        deadline = bounded(policy.probe_timeout_seconds)
        raw_wire.deadline = deadline
        reader = threading.Thread(target=drain, daemon=True)
        reader.start()
        require(not errors and not decoder.complete, "Probe attachment failed before dispatch")
        code, body = engine._control(endpoint, "POST", prefix + "/start", deadline=deadline,
            retain=save, label="start", on_response=response_observed)
        start_response = {"status": code, "body_bytes": len(body), "body_sha256": sha256(body),
            "framing_complete": True, "eof_observed": True,
            "request": evidence["start-request.bin"], "response": evidence["start-response.bin"]}
        save("start-completion.json", encoded(start_response))
        require(code == 204, "Probe start rejected")
        waited = engine._json_control(endpoint, prefix + "/wait?condition=not-running", deadline=deadline,
                                       retain=save, label="wait", method="POST")
        final = engine._json_control(endpoint, prefix + "/json", deadline=bounded(policy.transport_timeout_seconds),
                                    retain=save, label="final")
        comparison = role_identity_comparison(expected, final, spec, runtime, "created-to-exited", donor=donor)
        save("identity-comparison.json", encoded(comparison))
        reader.join(timeout=max(0, deadline - time.monotonic()))
        require(not reader.is_alive() and not errors and decoder.complete, "Probe complete attach EOF unproven")
        completed = engine.completion_evidence(waited, final, started=started, killed=False,
                                               identity_ok=comparison["matches"] is True)
        require(completed["natural"] and completed["inspect_exit_code"] == 0, "Trusted helper did not complete normally")
        status = "completed"
    except (OSError, ValueError) as error:
        error_text = type(error).__name__ + ":" + str(error)[:512]
    finally:
        if raw_wire is not None:
            try:
                raw_wire.socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            if reader is not None:
                reader.join(timeout=max(0, min(policy.transport_timeout_seconds,
                    (history_deadline - time.monotonic()) if history_deadline is not None else policy.transport_timeout_seconds)))
                if reader.is_alive():
                    status, error_text = "completion_unproven", "Probe reader did not stop"
            try:
                save("attach-response.bin", bytes(raw_wire.raw))
            finally:
                raw_wire.close()
    result: dict[str, Any] = {"protocol": execution_protocol, "role": "probe", "status": status, "error": error_text,
        "container_id": expected["Id"], "start_response": start_response,
        "role_policy": role_policy_identity(), "donor": donor.record(),
        "completion": engine.completion_evidence(waited, final, started=started, killed=False,
            identity_ok=comparison is not None and comparison["matches"] is True),
        "identity_comparison": comparison, "attach_eof_after_start_confirmation": eof_after_start,
        "capture_complete": decoder.complete and not errors, "helper_stdout_envelope": limit}
    for kind, data in decoder.streams.items():
        save(kind + ".bin", bytes(data))
        result[kind] = {**evidence[kind + ".bin"], "observed_bytes": decoder.counts[kind],
            "truncated": decoder.counts[kind] > len(data), "complete": decoder.complete and not errors}
    result["evidence"] = dict(evidence)
    save("process.json", encoded(result))
    return result


class CandidateHttpExecution:
    """Exclusive controller journal for one registered ordered server history."""

    def __init__(self, root: Path, store: GitStore, registration: HttpRegistration,
                 recipe: HttpRecipe, policy: HttpPolicy, *, profile: HttpProductProfile,
                 observation_admission: admission.ObservationAdmission,
                 checkpoint_authority: compact.HeadAuthority, delta_root: Path, cleanup_root: Path,
                 endpoint: Any = None, mode: str = "physical",
                 expected_checkpoint: ControllerCheckpoint | None = None):
        require(type(registration) is HttpRegistration and type(recipe) is HttpRecipe and type(policy) is HttpPolicy
                and mode in ("physical", "fixture") and type(profile) is HttpProductProfile
                and type(observation_admission) is admission.ObservationAdmission,
                "Typed immutable HTTP registration and admission required")
        profile.check_current(purpose=registration.binding.purpose,
                              requirements_sha256=registration.binding.requirements_sha256)
        require(profile.execution_protocol == registration.binding.protocol,
                "Registered product source-capture protocol differs from profile")
        self.root = Path(root).absolute()
        self.delta_root, self.cleanup_root = Path(delta_root).absolute(), Path(cleanup_root).absolute()
        require(self.root.resolve() == self.root and not self.root.is_symlink(), "Canonical journal root required")
        require(self.delta_root.resolve() == self.delta_root and self.cleanup_root.resolve() == self.cleanup_root,
                "Canonical external delta and cleanup roots required")
        roots = (self.root, self.delta_root, self.cleanup_root)
        require(all(not a.is_relative_to(b) and not b.is_relative_to(a)
                    for index, a in enumerate(roots) for b in roots[index + 1:]), "Disjoint owner roots required")
        require(mode != "physical" or type(checkpoint_authority) is head.ExternalHead,
                "Physical HTTP owner requires independently owned durable ExternalHead")
        if type(checkpoint_authority) is head.ExternalHead:
            require(checkpoint_authority.journal_roots == (self.root, self.delta_root)
                    and not self.cleanup_root.is_relative_to(checkpoint_authority.root)
                    and not checkpoint_authority.root.is_relative_to(self.cleanup_root),
                    "External head roots differ or alias cleanup")
        existed = self.root.exists()
        require(not existed or expected_checkpoint is not None, "Existing journal requires external checkpoint")
        require(existed or expected_checkpoint is None, "Checkpoint root missing")
        self.closed = False
        self.journal: owner_journal.OwnerJournal | None = None
        self.checkpoint_authority = checkpoint_authority
        self.emergency_cleanup: emergency.CleanupChannel | None = None
        self.emergency_result: Any = None
        self._cleanup_claims: dict[str, str] = {}
        self._start_responses: list[str] = []
        self._cleanup_mode = False
        self._history_deadline: float | None = None
        self._work_deadline: float | None = None
        self._operation_bytes: int | None = None
        self._operation_files: int | None = None
        self.store, self.registration, self.recipe, self.policy, self.mode = store, registration, recipe, policy, mode
        self.profile, self.admission = profile, observation_admission
        self.retained_freeze: registry.CohortFreeze | None = None
        try:
            self.endpoint = (endpoint or engine.EngineEndpoint.from_environment()) if mode == "physical" else None
            self.runtime = self._runtime()
            self.sources = evaluator_sources()
            require(self.sources == _LOADED_SOURCES, "Loaded HTTP evaluator changed")
            require(wire.max_probe_output_bytes(policy.wire_limits) <= policy.stream_limit_bytes,
                    "Trusted probe stdout capture is undersized")
            self.tree, self.files = capture_source(store, registration.commit_oid, policy=profile.capture_policy)
            self.fixture_files = dict(recipe.fixtures)
            assert recipe.input_entries is not None
            self.input_entries = recipe.input_entries
            require(self.tree == registration.tree_oid and source_sha256(self.files) == registration.binding.source_sha256,
                    "Registered Git source differs")
            self.binding = binding_for(self.files, recipe, policy, self.runtime,
                requirements_sha256=registration.binding.requirements_sha256, profile=profile, purpose=registration.binding.purpose)
            require(self.binding == registration.binding, "Registered runtime/evaluator/recipe/bounds differ")
            self.actual_registration = self._registration()
            require(self.actual_registration == registration.observation == self.admission.registration,
                    "Actual product gate differs from prospective admission")
            self.retained_freeze = self.admission.before_intent(self.actual_registration)
            self.config = {"protocol": self.binding.protocol, "mode": mode, "root": str(self.root),
                "checkpoint_protocol": compact.PROTOCOL, "owner_journal_protocol": owner_journal.PROTOCOL,
                "retention_process_protocol": retained_process.PROTOCOL,
                "delta_root": str(self.delta_root), "cleanup_root": str(self.cleanup_root),
                "head_root": str(checkpoint_authority.root) if type(checkpoint_authority) is head.ExternalHead else None,
                "repository": str(store.path.resolve()), "registration": asdict(registration), "policy": asdict(policy),
                "product_profile": profile.record(), "observation_registration": asdict(self.actual_registration),
                "cohort_freeze": None if self.retained_freeze is None else asdict(self.retained_freeze),
                "endpoint": None if self.endpoint is None else asdict(self.endpoint), "runtime": self.runtime,
                "source_manifest": source_manifest(self.files), "evaluator_sources": self.sources,
                "role_policy": role_policy_identity(), "recipe": recipe.record(), "snapshot_protocol": SNAPSHOT_PROTOCOL,
                "quota_policy": quota_policy(), "helper_sha256": wire.helper_sha256(), "helper_stdout_envelope": wire.max_probe_output_bytes(policy.wire_limits)}
            if profile.capture_policy is not None:
                self.config["source_capture"] = profile.capture_policy.record()
            # Context contains prospective declarations, never an unauthenticated
            # config read or a self-referential config digest/current head.
            self.journal = owner_journal.OwnerJournal(self.root, self.delta_root,
                context={"protocol": self.binding.protocol, "mode": mode,
                    "config_sha256": digest(self.config),
                    "registration_sha256": digest(asdict(registration)),
                    "cohort_freeze_sha256": digest(self.config["cohort_freeze"]),
                    "purpose": self.binding.purpose, "source_sha256": self.binding.source_sha256,
                    "runtime_sha256": digest(self.runtime), "evaluator_sources_sha256": digest(self.sources),
                    "policy_sha256": digest(asdict(policy)),
                    "profile_sha256": digest(profile.record()), "recipe_sha256": digest(recipe.record()),
                    "owner_journal_protocol": owner_journal.PROTOCOL,
                    "emergency_cleanup_protocol": emergency.PROTOCOL},
                authority=checkpoint_authority, expected=expected_checkpoint,
                limits=compact_limits())
            if existed:
                require(self.json_authenticated("config.json") == json.loads(encoded(self.config)), "Journal config changed")
            else:
                self._retain("config.json", encoded(self.config))
            self.checkpoint()
        except BaseException:
            self.close()
            raise

    def _registration(self) -> admission.ObservationRegistration:
        declared = self.registration.observation
        return observation_registration_for(self.binding, self.profile, self.policy,
            subject=declared.gate.binding.subject, gate_id=declared.gate.gate_id,
            commit_oid=self.registration.commit_oid, tree_oid=self.tree,
            repetition_id=self.registration.repetition_id,
            cohort_trajectory_ids=declared.cohort_trajectory_ids)

    def _freeze(self) -> registry.CohortFreeze | None:
        self.admission.check_current(self._registration(), self.retained_freeze)
        return self.retained_freeze

    def _runtime(self) -> dict[str, Any]:
        if self.mode == "fixture":
            return {"kind": "fixture-no-Docker"}
        require(self.endpoint is not None, "Physical runtime requires an endpoint")
        assert self.endpoint is not None
        return engine.runtime_identity(self.endpoint, self.policy.image_id,
            timeout_seconds=self._remaining(self.policy.transport_timeout_seconds))

    def _unchanged(self) -> None:
        self.checkpoint()
        self.admission.check_current(self._registration(), self.retained_freeze)
        require(self.sources == evaluator_sources() == _LOADED_SOURCES, "Loaded HTTP evaluator changed")
        require(self.json_authenticated("config.json") == json.loads(encoded(self.config)), "Controller config changed")
        tree, files = capture_source(self.store, self.registration.commit_oid, policy=self.profile.capture_policy)
        require(tree == self.tree and files == self.files, "Registered immutable Git source changed")
        observed_runtime = self._runtime()
        require(encoded(observed_runtime) == encoded(self.runtime)
                and binding_for(files, self.recipe, self.policy, observed_runtime,
                    requirements_sha256=self.binding.requirements_sha256, profile=self.profile, purpose=self.binding.purpose) == self.binding,
                "Runtime/environment/recipe binding changed")

    def execute_once(self) -> HttpHistoryResult:
        self._unchanged()
        if self.has_authenticated("intent.json"):
            if not self.has_authenticated("terminal.json"):
                raise ExecutionUnknown("Existing HTTP intent lacks an authenticated terminal; never redispatch")
            return self.verified_execution()
        self.admission.check_current(self._registration(), self.retained_freeze)
        execution_id = "http-" + uuid.uuid4().hex
        intent = {"protocol": self.binding.protocol, "execution_id": execution_id, "role_policy": role_policy_identity(),
            "config_sha256": digest(self.config), "registration_sha256": digest(asdict(self.registration)),
            "observation_registration": asdict(self.actual_registration),
            "cohort_freeze": None if self.retained_freeze is None else asdict(self.retained_freeze),
            "volume": "gossip-" + execution_id + "-volume", "keeper": "gossip-" + execution_id + "-keeper",
            "containers": ["gossip-" + execution_id + "-" + str(i) for i in range(len(self.recipe.steps))],
            "planned_steps": len(self.recipe.steps), "planned_requests": sum(x.kind == "probe" for x in self.recipe.steps),
            "planned_servers": sum(x.kind == "start" for x in self.recipe.steps), "planned_keepers": 1,
            "planned_cli": sum(x.kind == "cli" for x in self.recipe.steps),
            "row_id": self.recipe.row_id or self.recipe.recipe_id, "definition_sha256": self.recipe.definition_sha256}
        self._retain("intent.json", encoded(intent))
        require(self.mode == "physical", "Fixture journal cannot dispatch physical evidence")
        self.admission.check_current(self._registration(), self.retained_freeze)
        self.checkpoint()
        try:
            self._dispatch(intent)
            self._unchanged()
            return self.verified_execution()
        except BaseException as error:
            if self.journal is not None and self.journal.uncertain:
                self._emergency_after_failure(error)
            raise

    def _emergency_after_failure(self, original: BaseException) -> None:
        if self.emergency_cleanup is not None and self.emergency_result is None:
            try:
                self.emergency_result = self.emergency_cleanup.run(
                    reason="HTTP original uncertain: " + type(original).__name__)
            except BaseException as error:
                # Emergency diagnostics cannot replace the original exception
                # or resurrect ordinary current-journal authority.
                self.emergency_result = {"cleanup_unavailable": type(error).__name__ + ":" + str(error)[:512]}

    def _remaining(self, seconds: float) -> float:
        deadline = self._history_deadline if self._cleanup_mode else self._work_deadline
        remaining = seconds if deadline is None else min(seconds, deadline - time.monotonic())
        require(remaining > 0, "HTTP history observation deadline exhausted")
        return remaining

    def _deadline_after(self, seconds: float) -> float:
        return time.monotonic() + self._remaining(seconds)

    def _finite_window(self) -> None:
        # Frozen v4 has separate bounded runtime/prestart/attach/final/kill controls.
        # Reserve their complete worst sequential observation window, rather than
        # silently shortening the registered finite command deadline.
        needed = self.policy.cli_timeout_seconds + 10 * self.policy.transport_timeout_seconds + 15
        require(self._remaining(needed) >= needed, "Insufficient history window for finite CLI")

    def _begin_operation(self) -> None:
        require(not self._cleanup_mode, "Cleanup cannot dispatch history operations")
        prefix = self.checkpoint()
        self._remaining(1)
        require(prefix.raw_bytes + OPERATION_LIMIT_BYTES <= MAX_JOURNAL_BYTES - CLEANUP_RESERVE_BYTES
                and prefix.raw_file_count + OPERATION_LIMIT_FILES <= MAX_JOURNAL_FILES - CLEANUP_RESERVE_FILES,
                "Insufficient protected journal capacity for next operation")
        self._operation_bytes = prefix.raw_bytes
        self._operation_files = prefix.raw_file_count

    def _descriptor(self, name: str) -> dict[str, Any]:
        raw = self.read_authenticated(name)
        return {"path": name, "bytes": len(raw), "sha256": sha256(raw)}

    def _row(self, descriptor: Any) -> dict[str, Any]:
        require(type(descriptor) is dict and set(descriptor) == {"path", "bytes", "sha256"},
                "Closed retained row descriptor required")
        raw = self._raw({"stdout": descriptor})
        value = journal.decode(raw)
        require(type(value) is dict and encoded(value) == raw, "Canonical retained row required")
        return value

    def _retain(self, name: str, raw: bytes) -> None:
        require(re.fullmatch(r"[a-z][a-z0-9.-]{0,180}", name) is not None
                and type(raw) is bytes and len(raw) <= MAX_RECORD_BYTES, "Invalid retained artifact")
        assert self.journal is not None
        prefix = self.journal.commitment
        if self._cleanup_mode and name.endswith(".json"):
            require(len(raw) <= CLEANUP_METADATA_LIMIT, "Cleanup metadata observation bound exceeded")
        if name == "terminal.json":
            require(self._cleanup_mode and len(raw) <= TERMINAL_LIMIT_BYTES, "Compact cleanup terminal required")
        if not self._cleanup_mode and self._operation_bytes is not None:
            assert self._operation_files is not None
            require(prefix.raw_bytes + len(raw) - self._operation_bytes <= OPERATION_LIMIT_BYTES
                    and prefix.raw_file_count + 1 - self._operation_files <= OPERATION_LIMIT_FILES,
                    "Operation observation allocation exhausted")
        self.journal.retain(name, raw, cleanup=self._cleanup_mode)
        # Frozen transport functions parse their in-memory response only after
        # this callback returns. Authenticate the corresponding original first.
        require(self.read_authenticated(name) == raw, "Acknowledged raw evidence changed before use")
        if name.endswith("-start-completion.json"):
            self._start_responses.append(name)
        if name.endswith("-request.bin"):
            self.checkpoint()  # The callback returns directly to Engine send.


    def checkpoint(self) -> ControllerCheckpoint:
        require(not self.closed and self.journal is not None, "Closed controller")
        assert self.journal is not None
        return self.journal.checkpoint()

    def read_authenticated(self, name: str) -> bytes:
        require(not self.closed and self.journal is not None, "Closed controller")
        assert self.journal is not None
        return self.journal.read(name)

    def json_authenticated(self, name: str) -> dict[str, Any]:
        raw = self.read_authenticated(name)
        value = journal.decode(raw)
        require(type(value) is dict and encoded(value) == raw, "Canonical retained JSON object required")
        return value

    def has_authenticated(self, name: str) -> bool:
        require(not self.closed and self.journal is not None, "Closed controller")
        assert self.journal is not None
        return self.journal.has(name)

    def retain_verifier(self, name: str, raw: bytes) -> ControllerCheckpoint:
        """Anchor semantic evidence in this original chain using reserved space."""
        self.checkpoint()
        require(self.has_authenticated("terminal.json") and name == "semantic-verifier.json"
                and type(raw) is bytes and len(raw) <= CLEANUP_METADATA_LIMIT,
                "Complete original and bounded fixed verifier required")
        assert self.journal is not None
        self.journal.retain(name, raw, cleanup=True)
        require(self.read_authenticated(name) == raw, "Verifier changed after acknowledgement")
        return self.checkpoint()

    def read_prior(self, name: str) -> compact.PriorFact:
        """Prior diagnostic/ownership evidence only, never current authority."""
        require(not self.closed and self.journal is not None, "Closed controller")
        assert self.journal is not None
        return self.journal.read_prior(name)


    def _command(self, label: str, argv: list[str], timeout: float | None = None) -> dict[str, Any]:
        """Bound control traffic; candidate streams never pass through this helper."""
        self.checkpoint()
        require(self.mode == "physical" and self.endpoint is not None, "Control commands require a physical endpoint")
        assert self.endpoint is not None
        self.endpoint.validate()
        require(bool(argv) and argv[0] == "docker", "Only fixed Docker control commands allowed")
        argv = ["docker", "--host", "unix://" + self.endpoint.socket_path, *argv[1:]]
        environment = {key: value for key, value in DockerValidator._environment().items()
                       if key not in ("DOCKER_HOST", "DOCKER_CONTEXT")}
        timeout = self._remaining(self.policy.transport_timeout_seconds if timeout is None else timeout)
        command_deadline = self._deadline_after(timeout)
        self._retain(label + "-command-intent.json", encoded({"argv": argv,
            "runtime_sha256": digest(self.runtime), "timeout_seconds": timeout}))
        self.checkpoint()
        child = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                 stdin=subprocess.DEVNULL, env=environment)
        buffers = {"stdout": bytearray(), "stderr": bytearray()}
        counts = {"stdout": 0, "stderr": 0}
        errors: list[str] = []
        lock = threading.Lock()
        def drain(kind: str, pipe: Any) -> None:
            try:
                while chunk := pipe.read(65536):
                    with lock:
                        counts[kind] += len(chunk)
                        remaining = CONTROL_LIMIT - len(buffers[kind])
                        if remaining > 0:
                            buffers[kind].extend(chunk[:remaining])
                        if counts[kind] > CONTROL_LIMIT:
                            child.kill()
            except OSError:
                errors.append(kind)
            finally:
                pipe.close()
        workers = [threading.Thread(target=drain, args=(kind, getattr(child, kind)), daemon=True) for kind in buffers]
        for worker in workers:
            worker.start()
        timed_out = False
        try:
            child.wait(timeout=max(0, command_deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            timed_out = True
            child.kill()
            try:
                child.wait(timeout=max(0, command_deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                errors.append("reap-unproven")
        for worker in workers:
            worker.join(timeout=max(0, command_deadline - time.monotonic()))
        with lock:
            data = {kind: bytes(raw) for kind, raw in buffers.items()}
            observed = dict(counts)
        record: dict[str, Any] = {"argv": argv, "exit_code": child.returncode, "timed_out": timed_out,
            "capture_complete": not errors and all(not worker.is_alive() for worker in workers),
            "observation_deadline_monotonic": command_deadline, "control_errors": errors}
        for kind, raw in data.items():
            name = label + "-" + kind + ".bin"
            self._retain(name, raw)
            record[kind] = {"path": name, "sha256": sha256(raw), "bytes": len(raw),
                            "observed_bytes": observed[kind], "truncated": observed[kind] != len(raw)}
        self._retain(label + ".json", encoded(record))
        self.checkpoint()
        return record


    def _raw(self, record: dict[str, Any], kind: str = "stdout") -> bytes:
        item = record[kind]
        require(type(item) is dict and type(item.get("path")) is str
                and re.fullmatch(r"[a-z][a-z0-9.-]{0,180}", item["path"]) is not None, "Invalid raw artifact reference")
        raw = self.read_authenticated(item["path"])
        require(sha256(raw) == item["sha256"] and len(raw) == item["bytes"], "Raw artifact differs")
        return raw


    @staticmethod
    def _clean(record: dict[str, Any]) -> bool:
        return (record["exit_code"] == 0 and not record["timed_out"] and record["capture_complete"]
                and not record["stdout"]["truncated"] and not record["stderr"]["truncated"])


    def _checked(self, label: str, argv: list[str]) -> dict[str, Any]:
        record = self._command(label, argv)
        require(self._clean(record), "Docker control command incomplete: " + label)
        return record


    def close(self) -> None:
        if not self.closed:
            if self.emergency_cleanup is not None:
                self.emergency_cleanup.close()
            if self.journal is not None:
                self.journal.close()
            self.closed = True


    def __enter__(self) -> CandidateHttpExecution:
        return self


    def __exit__(self, *args: Any) -> None:
        self.close()


    def _inspect(self, label: str, container_id: str) -> dict[str, Any]:
        self.checkpoint()
        require(self.endpoint is not None and _SHA.fullmatch(container_id) is not None, "Full owned ID required")
        assert self.endpoint is not None
        result = engine._json_control(self.endpoint, "/containers/" + container_id + "/json",
            deadline=self._deadline_after(self.policy.transport_timeout_seconds), retain=self._retain, label=label)
        self.checkpoint()
        return result

    def _control(self, label: str, container_id: str, operation: str) -> tuple[int, bytes]:
        self.checkpoint()
        require(self.endpoint is not None and _SHA.fullmatch(container_id) is not None
                and operation in ("start", "stop?t=" + str(self.policy.stop_timeout_seconds)), "Closed lifecycle action required")
        assert self.endpoint is not None
        result = engine._control(self.endpoint, "POST", "/containers/" + container_id + "/" + operation,
            deadline=self._deadline_after(self.policy.transport_timeout_seconds + self.policy.stop_timeout_seconds),
            retain=self._retain, label=label)
        self.checkpoint()
        return result

    def _compare(self, label: str, before: dict[str, Any], after: dict[str, Any], spec: RoleSpec, phase: str) -> dict[str, Any]:
        comparison = role_identity_comparison(before, after, spec, self.runtime, phase)
        self._retain(label + ".json", encoded(comparison))
        require(comparison["matches"] is True, "Role identity/epoch continuity differs")
        return comparison

    def _boundary(self, label: str, baseline: dict[str, Any], spec: RoleSpec) -> dict[str, Any]:
        value = self._inspect(label, baseline["Id"])
        validate_state(value, "running")
        return self._compare(label + "-continuity", baseline, value, spec, "running-to-running")

    def _create(self, label: str, spec: RoleSpec, owned: dict[str, tuple[RoleSpec, str | None]]) -> dict[str, Any]:
        absent = self._checked(label + "-before", ["docker", "container", "ls", "--all", "--quiet", "--filter", "name=^/" + spec.name + "$"])
        require(not self._raw(absent).strip(), "Container name already exists")
        argv = create_argv(spec, self.policy.image_id)
        self._retain(label + "-create-intent.json", encoded({"spec": asdict(spec), "create_argv": argv}))
        require(self.emergency_cleanup is not None, "Independent cleanup ownership channel required")
        assert self.emergency_cleanup is not None
        claim = self.emergency_cleanup.claim_container(name=spec.name, labels=dict(spec.labels), argv=spec.argv,
            preabsence_record=label + "-before.json")
        self._cleanup_claims[spec.name] = claim
        owned[spec.name] = (spec, None)  # Preabsence + durable intent cover an uncertain create response.
        record = self._checked(label + "-create", argv)
        container_id = self._raw(record).strip().decode("ascii")
        require(_SHA.fullmatch(container_id) is not None, "Invalid created container ID")
        self.emergency_cleanup.confirm_container(claim, create_record=label + "-create.json")
        owned[spec.name] = (spec, container_id)
        value = self._inspect(label + "-created", container_id)
        validate_role(value, spec, self.policy.image_id, self.runtime)
        validate_state(value, "created")
        require(value["Id"] == container_id, "Created inspect names a different container")
        return value

    def _start_role(self, label: str, created: dict[str, Any], spec: RoleSpec) -> dict[str, Any]:
        require(spec.role in ("server", "keeper"), "Long-lived start requires server or keeper role")
        before = self._inspect(label + "-prestart", created["Id"])
        validate_state(before, "created")
        self._compare(label + "-prestart-comparison", created, before, spec, "created-to-prestart")
        self._retain(label + "-start-intent.json", encoded({"container_id": created["Id"], "spec": asdict(spec),
            "prestart_sha256": digest(before), "controller_action": "start-long-lived-role"}))
        code, body = self._control(label + "-start", created["Id"], "start")
        self._retain(label + "-start-completion.json", encoded({"status": code, "body_sha256": sha256(body),
            "framing_complete": True, "eof_observed": True, "container_id": created["Id"]}))
        require(code == 204, "Long-lived role start rejected")
        running = self._inspect(label + "-running", created["Id"])
        validate_state(running, "running")
        self._compare(label + "-startup-comparison", created, running, spec, "created-to-running")
        self._retain(label + "-running.json", encoded(running))
        return running

    def _stop_role(self, label: str, running: dict[str, Any], spec: RoleSpec) -> dict[str, Any]:
        self._boundary(label + "-before", running, spec)
        self._retain(label + "-intent.json", encoded({"controller_action": "explicit-stop",
            "container_id": running["Id"], "running_sha256": digest(running), "timeout_seconds": self.policy.stop_timeout_seconds,
            "natural_exit_zero_required": False}))
        code, body = self._control(label, running["Id"], "stop?t=" + str(self.policy.stop_timeout_seconds))
        require(code == 204, "Explicit server stop response incomplete or server already stopped")
        final = self._inspect(label + "-final", running["Id"])
        validate_state(final, "exited")
        comparison = self._compare(label + "-comparison", running, final, spec, "running-to-exited")
        result = {"controller_action": "explicit-stop", "status": code, "response_body_sha256": sha256(body),
            "container_id": running["Id"], "exit_code": final["State"].get("ExitCode"),
            "natural_exit_zero_required": False, "comparison": comparison, "final_inspection_sha256": digest(final)}
        self._retain(label + "-result.json", encoded(result))
        return result

    def _remove(self, label: str, spec: RoleSpec, container_id: str | None, *, force: bool) -> bool:
        """Remove only independently matched resources from the durable create intent."""
        inspected = self._command(label + "-inspect", ["docker", "inspect", "--format", "{{json .}}", spec.name])
        if not self._clean(inspected):
            absent = self._command(label + "-absence", ["docker", "container", "ls", "--all", "--quiet", "--filter", "name=^/" + spec.name + "$"])
            return self._clean(absent) and not self._raw(absent).strip()
        value = engine.strict_json_loads(self._raw(inspected))
        require(type(value) is dict and _SHA.fullmatch(value.get("Id", "")) is not None
                and (container_id is None or value["Id"] == container_id) and value.get("Name") == "/" + spec.name
                and value.get("Image") == self.policy.image_id and value.get("Config", {}).get("Labels") == dict(spec.labels),
                "Cleanup ownership differs; refusing removal")
        self._retain(label + "-intent.json", encoded({"container_id": value["Id"], "spec": asdict(spec),
            "force": force, "inspection_sha256": digest(value)}))
        require(self.emergency_cleanup is not None and spec.name in self._cleanup_claims,
                "Original acknowledged cleanup claim required")
        assert self.emergency_cleanup is not None
        self.emergency_cleanup.note_normal_removal(self._cleanup_claims[spec.name])
        removed = self._command(label + "-remove", ["docker", "rm", *(["--force"] if force else []), value["Id"]])
        absent = self._command(label + "-absence", ["docker", "container", "ls", "--all", "--quiet", "--filter", "name=^/" + spec.name + "$"])
        complete = self._clean(removed) and self._clean(absent) and not self._raw(absent).strip()
        if complete:
            self.emergency_cleanup.confirm_normal_removal(self._cleanup_claims[spec.name],
                remove_record=label + "-remove.json", absence_record=label + "-absence.json")
        return complete

    def _volume_valid(self, value: Any, intent: dict[str, Any], baseline: dict[str, Any] | None = None) -> bool:
        return (type(value) is dict and value.get("Name") == intent["volume"] and value.get("Driver") == "local"
                and value.get("Options") == VOLUME_OPTIONS
                and value.get("Labels") == {"gossip.execution": intent["execution_id"], "gossip.snapshot": SNAPSHOT_PROTOCOL}
                and (baseline is None or encoded(value) == encoded(baseline)))

    def _labels(self, intent: dict[str, Any], role: str, epoch: int, step_id: str) -> tuple[tuple[str, str], ...]:
        return tuple(sorted({"gossip.execution": intent["execution_id"], "gossip.role": role,
            "gossip.source": self.binding.source_sha256, "gossip.fixture": self.binding.fixture_sha256,
            "gossip.epoch": str(epoch), "gossip.step": step_id, "gossip.helper": self.binding.helper_sha256}.items()))

    def _dispatch(self, intent: dict[str, Any]) -> None:
        require(self.endpoint is not None, "Physical execution requires Engine endpoint")
        assert self.endpoint is not None
        owned: dict[str, tuple[RoleSpec, str | None]] = {}
        cleanup: dict[str, bool] = {}
        results: list[dict[str, Any]] = []
        observations: list[dict[str, Any]] = []
        cli_observations: list[dict[str, Any]] = []
        infrastructure: list[str] = []
        original_error: BaseException | None = None
        epochs: list[dict[str, Any]] = []
        volume_claimed = volume_clean = False
        volume_baseline: dict[str, Any] | None = None
        entered_step_indices: list[int] = []
        probe_attempts: list[dict[str, Any]] = []
        cli_attempts: list[dict[str, Any]] = []
        keeper: dict[str, Any] | None = None
        active: dict[str, Any] | None = None
        active_spec: RoleSpec | None = None
        keeper_spec = RoleSpec("keeper", intent["keeper"],
            ("python", "-I", "-c", "import time;time.sleep(" + str(self.policy.lifetime_seconds) + ")"),
            self._labels(intent, "keeper", 0, "state-lifetime"), (), intent["volume"])
        self._history_deadline = time.monotonic() + self.policy.lifetime_seconds
        self._work_deadline = self._history_deadline - CLEANUP_SECONDS
        self._begin_operation()
        with tempfile.TemporaryDirectory(prefix="gossip-http-stage-") as temporary:
            staging = Path(temporary).resolve()
            workspace, inputs = staging / "workspace", staging / "inputs"
            workspace.mkdir(mode=0o755)
            for name, raw in self.files.items():
                path = workspace / name
                path.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
                path.write_bytes(raw)
                path.chmod(0o444)
            finite._verify_tree(workspace, self.files)
            inputs_root = inputs
            inputs_root.mkdir(mode=0o755)
            # Only immutable data fixtures gain confined symlink support.
            input_staging.stage(inputs_root, self.input_entries)
            require(type(self.checkpoint_authority) is head.ExternalHead and self.journal is not None,
                    "Physical cleanup requires exact external host head")
            assert type(self.checkpoint_authority) is head.ExternalHead and self.journal is not None
            self.emergency_cleanup = emergency.CleanupChannel.create(self.cleanup_root, journal=self.journal,
                endpoint=self.endpoint, runtime=self.runtime, source_sha256=self.binding.source_sha256,
                fixture_sha256=self.binding.fixture_sha256, execution_id=intent["execution_id"],
                image_id=self.policy.image_id, candidate_mount_roots=(staging,),
                journal_roots=(self.root, self.delta_root, self.checkpoint_authority.root))
            self._retain("staging.json", encoded({"workspace": str(workspace), "inputs": str(inputs_root),
                "source_manifest": source_manifest(self.files), "input_manifest": input_staging.manifest(self.input_entries)}))
            try:
                before = self._checked("volume-before", ["docker", "volume", "ls", "--quiet", "--filter", "name=^" + intent["volume"] + "$"])
                require(not self._raw(before).strip(), "Volume name already exists")
                create = ["docker", "volume", "create", "--driver", "local", "--label", "gossip.execution=" + intent["execution_id"],
                          "--label", "gossip.snapshot=" + SNAPSHOT_PROTOCOL]
                for key, value in VOLUME_OPTIONS.items():
                    create += ["--opt", key + "=" + value]
                create.append(intent["volume"])
                self._retain("volume-intent.json", encoded({"argv": create, "volume": intent["volume"]}))
                self._cleanup_claims[intent["volume"]] = self.emergency_cleanup.claim_volume(name=intent["volume"],
                    labels={"gossip.execution": intent["execution_id"], "gossip.snapshot": SNAPSHOT_PROTOCOL},
                    options=VOLUME_OPTIONS, preabsence_record="volume-before.json")
                volume_claimed = True
                created_volume = self._checked("volume-create", create)
                require(self._raw(created_volume).strip() == intent["volume"].encode(), "Created volume identity differs")
                inspected = self._checked("volume-created", ["docker", "volume", "inspect", "--format", "{{json .}}", intent["volume"]])
                volume_baseline = engine.strict_json_loads(self._raw(inspected))
                require(self._volume_valid(volume_baseline, intent), "Volume bounds/identity differ")
                self._retain("volume-baseline.json", encoded(volume_baseline))
                self.emergency_cleanup.confirm_volume(self._cleanup_claims[intent["volume"]],
                    baseline_record="volume-baseline.json")
                keeper_created = self._create("keeper", keeper_spec, owned)
                keeper = self._start_role("keeper", keeper_created, keeper_spec)
                # The keeper has no candidate source/input mount. Only this fixed
                # data bootstrap may use exec; candidate CLI always uses attach.
                self._boundary("initial-state-keeper-before", keeper, keeper_spec)
                setup = setup_definition(self.recipe)
                argv = setup_argv(self.recipe, keeper["Id"])
                self._retain("initial-state-intent.json", encoded({"protocol": SETUP_PROTOCOL,
                    "keeper_id": keeper["Id"], "volume": intent["volume"], "setup": setup,
                    "setup_sha256": digest(setup), "argv": argv,
                    "recipe_sha256": self.binding.recipe_sha256, "fixture_sha256": self.binding.fixture_sha256}))
                command = self._checked("initial-state-bootstrap", argv)
                require(self._raw(command) == setup_expected_output(self.recipe)
                        and self._raw(command, "stderr") == b"", "Initial state bootstrap acknowledgment differs")
                continuity = self._boundary("initial-state-keeper-after", keeper, keeper_spec)
                volume_after = self._checked("initial-state-volume-after",
                    ["docker", "volume", "inspect", "--format", "{{json .}}", intent["volume"]])
                require(self._volume_valid(engine.strict_json_loads(self._raw(volume_after)), intent, volume_baseline),
                        "Initial state setup volume identity differs")
                self._retain("initial-state-complete.json", encoded({"protocol": SETUP_PROTOCOL,
                    "setup_sha256": digest(setup), "keeper_id": keeper["Id"], "volume": intent["volume"],
                    "command": self._descriptor("initial-state-bootstrap.json"),
                    "ack_sha256": sha256(self._raw(command)), "keeper_continuity": continuity}))
                epoch = 0
                for index, step in enumerate(self.recipe.steps):
                    label = "step-" + str(index).zfill(3)
                    self._begin_operation()
                    entered_step_indices.append(index)
                    self._unchanged()
                    self._remaining(1)
                    finite._verify_tree(workspace, self.files)
                    input_staging.verify(inputs, self.input_entries)
                    self._boundary(label + "-keeper-before", keeper, keeper_spec)
                    row: dict[str, Any] = {"step_id": step.step_id, "step_index": index, "kind": step.kind, "label": label,
                        "root_path": step.root_path, "step_sha256": digest(step.record()),
                        "definition_sha256": self.recipe.definition_sha256, "row_id": self.recipe.row_id or self.recipe.recipe_id}
                    if step.kind == "start":
                        require(active is None and active_spec is None, "Previous server not removed")
                        epoch += 1
                        require(epoch == step.epoch, "Server epoch declaration differs")
                        active_spec = RoleSpec("server", intent["containers"][index], step.argv,
                            self._labels(intent, "server", epoch, step.step_id),
                            (("/workspace", str(workspace)), ("/inputs", str(inputs))), intent["volume"])
                        created = self._create(label, active_spec, owned)
                        active = self._start_role(label, created, active_spec)
                        require(active["Id"] not in {x["server_id"] for x in epochs}, "Server epoch reused an ID")
                        epoch_record = {"epoch": epoch, "step_label": label, "server_id": active["Id"], "name": active_spec.name,
                            "started_at": active["State"]["StartedAt"], "pid": active["State"]["Pid"],
                            "database_volume": intent["volume"], "database_path": self.recipe.database_path,
                            "root_path": step.root_path, "argv": list(step.argv),
                            "source_sha256": self.binding.source_sha256, "fixture_sha256": self.binding.fixture_sha256,
                            "running_inspection_sha256": digest(active), "keeper_id": keeper["Id"],
                            "keeper_started_at": keeper["State"]["StartedAt"]}
                        self._retain(label + "-epoch.json", encoded(epoch_record))
                        epochs.append(epoch_record)
                        row.update(epoch_record)
                    elif step.kind == "stop":
                        require(active is not None and active_spec is not None, "Missing active server")
                        assert active is not None and active_spec is not None
                        row.update(epoch=epoch, server_id=active["Id"],
                            stop=self._stop_role(label + "-stop", active, active_spec))
                        cleanup[active_spec.name] = False
                        cleanup[active_spec.name] = self._remove(label + "-retire", active_spec, active["Id"], force=False)
                        require(cleanup[active_spec.name], "Server removal unproven before next epoch")
                        row["removed"] = True
                        active, active_spec = None, None
                    elif step.kind == "cli":
                        require(active is None and active_spec is None and epoch == step.epoch,
                                "CLI requires removed server and exact epoch")
                        cli_spec = RoleSpec("cli", intent["containers"][index], step.argv,
                            self._labels(intent, "cli", epoch, step.step_id),
                            (("/workspace", str(workspace)), ("/inputs", str(inputs))), intent["volume"])
                        row.update(epoch=epoch, database_volume=intent["volume"], database_path=self.recipe.database_path,
                            source_sha256=self.binding.source_sha256, fixture_sha256=self.binding.fixture_sha256,
                            keeper_id=keeper["Id"], binding=asdict(self.binding), authenticated=False,
                            expected_argv=list(step.argv), product_verdict="not_evaluated")
                        self._retain(label + "-cli-intent.json", encoded(row))
                        created = self._create(label + "-cli", cli_spec, owned)
                        finite._verify_tree(workspace, self.files)
                        input_staging.verify(inputs, self.input_entries)
                        self._unchanged()
                        self._boundary(label + "-cli-keeper-prestart", keeper, keeper_spec)
                        volume = self._checked(label + "-cli-volume-before", ["docker", "volume", "inspect", "--format", "{{json .}}", intent["volume"]])
                        require(self._volume_valid(engine.strict_json_loads(self._raw(volume)), intent, volume_baseline), "CLI state volume identity differs")
                        self._finite_window()
                        cli_attempts.append({"step_index": index, "label": label, "container_id": created["Id"],
                            "artifact_prefix": label + "-cli-process-", "meaning": "finite transport invoked; start/exit not inferred"})
                        process = retained_process.run_process(self.endpoint, container_id=created["Id"], expected=created,
                            policy=engine.ProcessPolicy(self.policy.image_id, timeout_seconds=self.policy.cli_timeout_seconds,
                                stream_limit_bytes=self.policy.cli_stream_limit_bytes,
                                frame_limit_bytes=self.policy.cli_stream_limit_bytes,
                                transport_timeout_seconds=self.policy.transport_timeout_seconds),
                            retain=self._retain, label=label + "-cli-process", expected_runtime=self.runtime,
                            expected_argv=list(step.argv))
                        self.checkpoint()
                        row.update(cli_id=created["Id"], process=process)
                        # Save raw finite evidence before any lineage checks can fail.
                        self._retain(label + "-cli-process-result.json", encoded(process))
                        try:
                            row["keeper_continuity"] = self._boundary(label + "-cli-keeper-after", keeper, keeper_spec)
                            volume = self._checked(label + "-cli-volume-after", ["docker", "volume", "inspect", "--format", "{{json .}}", intent["volume"]])
                            require(self._volume_valid(engine.strict_json_loads(self._raw(volume)), intent, volume_baseline), "CLI state volume identity differs")
                            finite._verify_tree(workspace, self.files)
                            input_staging.verify(inputs, self.input_entries)
                            self._unchanged()
                            require(process["completion"]["natural"] is True and process["capture_complete"] is True,
                                    "Finite CLI observation unavailable")
                            self._raw(process)
                            self._raw(process, "stderr")
                            row["authenticated"] = True
                        except (OSError, ValueError) as error:
                            row["observation_error"] = type(error).__name__ + ":" + str(error)[:512]
                        cleanup[cli_spec.name] = False
                        try:
                            cleanup[cli_spec.name] = self._remove(label + "-cli-retire", cli_spec, created["Id"], force=True)
                        except (OSError, ValueError, subprocess.SubprocessError) as error:
                            row["cleanup_error"] = type(error).__name__ + ":" + str(error)[:512]
                        row["cli_removed"] = cleanup[cli_spec.name]
                        self._retain(label + "-cli-observation.json", encoded(row))
                        cli_observations.append(row)
                        require(row["authenticated"] and cleanup[cli_spec.name], "CLI observation/retirement unavailable")
                    else:
                        require(active is not None and active_spec is not None, "Missing active server")
                        assert active is not None and active_spec is not None
                        row.update(epoch=epoch, server_id=active["Id"], database_volume=intent["volume"],
                            database_path=self.recipe.database_path, source_sha256=self.binding.source_sha256,
                            fixture_sha256=self.binding.fixture_sha256, commit_oid=self.registration.commit_oid,
                            tree_oid=self.registration.tree_oid, helper_sha256=self.binding.helper_sha256,
                            binding=asdict(self.binding), keeper_id=keeper["Id"], authenticated=False)
                        donor_current = self._inspect(label + "-server-before", active["Id"])
                        validate_state(donor_current, "running")
                        row["continuity_before"] = self._compare(label + "-server-before-continuity",
                            active, donor_current, active_spec, "running-to-running")
                        donor = bind_probe_donor(baseline=active, current=donor_current, server_spec=active_spec,
                            registration=self.registration, runtime=self.runtime, epoch=epoch)
                        self._retain(label + "-donor-inspection.json", donor.inspection_json)
                        self._retain(label + "-donor.json", encoded(donor.record()))
                        row["donor"] = donor.record()
                        request = engine.strict_json_loads(step.request_json)
                        probe_input = wire.build_probe_input(request, self.recipe.port, self.policy.wire_limits)
                        probe_files = {"helper.py": wire.helper_source(), "request.json": probe_input}
                        probe_root = staging / label
                        probe_root.mkdir(mode=0o755)
                        for name, raw in probe_files.items():
                            (probe_root / name).write_bytes(raw)
                            (probe_root / name).chmod(0o444)
                        finite._verify_tree(probe_root, probe_files)
                        self._retain(label + "-probe-input.json", probe_input)
                        self._retain(label + "-probe-source.json", encoded({"helper_sha256": sha256(probe_files["helper.py"]),
                            "request_input_sha256": sha256(probe_input), "request_sha256": sha256(wire.request_bytes(request, self.recipe.port)),
                            "argv": PROBE_ARGV, "server_epoch": epoch, "server_id": active["Id"]}))
                        probe_spec = RoleSpec("probe", intent["containers"][index], PROBE_ARGV,
                            self._labels(intent, "probe", epoch, step.step_id), (("/probe", str(probe_root)),), server_id=active["Id"])
                        # Recheck staged source/fixture/runtime immediately before helper creation.
                        finite._verify_tree(workspace, self.files)
                        input_staging.verify(inputs, self.input_entries)
                        self._unchanged()
                        donor.validate_probe(probe_spec, self.runtime)
                        probe_created = self._create(label + "-probe", probe_spec, owned)
                        row["probe_id"] = probe_created["Id"]
                        probe_attempts.append({"step_index": index, "label": label, "container_id": probe_created["Id"],
                            "artifact_prefix": label + "-probe-", "meaning": "probe transport invoked; start/send not inferred"})
                        process_result = run_probe(self.endpoint, expected=probe_created, spec=probe_spec, policy=self.policy,
                            runtime=self.runtime, retain=self._retain, label=label + "-probe", donor=donor,
                            history_deadline=self._work_deadline, execution_protocol=self.binding.protocol)
                        self.checkpoint()
                        row["process"] = process_result
                        # Retain the request observation even if server continuity or helper completion subsequently fails.
                        try:
                            row["continuity_after"] = self._boundary(label + "-server-after", active, active_spec)
                            row["keeper_continuity"] = self._boundary(label + "-probe-keeper-after", keeper, keeper_spec)
                            finite._verify_tree(probe_root, probe_files)
                            finite._verify_tree(workspace, self.files)
                            input_staging.verify(inputs, self.input_entries)
                            self._unchanged()
                            require(process_result["status"] == "completed", "Trusted helper completion unavailable")
                            require(self._raw(process_result, "stderr") == b"", "Trusted helper emitted diagnostics")
                            observation = wire.decode_probe_output(self._raw(process_result), request, self.recipe.port, self.policy.wire_limits)
                            row["exchange_complete"] = observation.exchange_complete
                            row["sent_complete"] = observation.sent_complete
                            row["authenticated"] = True
                        except (OSError, ValueError) as error:
                            row["observation_error"] = type(error).__name__ + ":" + str(error)[:512]
                        cleanup[probe_spec.name] = False
                        try:
                            cleanup[probe_spec.name] = self._remove(label + "-probe-retire", probe_spec, probe_created["Id"], force=True)
                        except (OSError, ValueError, subprocess.SubprocessError) as error:
                            cleanup[probe_spec.name] = False
                            row["cleanup_error"] = type(error).__name__ + ":" + str(error)[:512]
                        row["probe_removed"] = cleanup[probe_spec.name]
                        self._retain(label + "-observation.json", encoded(row))
                        observations.append(row)
                        require(row["authenticated"] and cleanup[probe_spec.name], "Probe observation/provenance unavailable")
                    self._boundary(label + "-keeper-after", keeper, keeper_spec)
                    finite._verify_tree(workspace, self.files)
                    input_staging.verify(inputs, self.input_entries)
                    self._retain(label + "-result.json", encoded(row))
                    results.append(row)
            except BaseException as error:
                original_error = error
                infrastructure.append(type(error).__name__ + ":" + str(error)[:512])
            finally:
                self._cleanup_mode = True
                # Clean each owned resource once; preserve failed cleanup evidence and do not retry until green.
                for ordinal, (name, (spec, container_id)) in enumerate(reversed(list(owned.items()))):
                    if self.journal.uncertain:
                        break
                    if name in cleanup:
                        continue
                    try:
                        if spec.role == "keeper" and keeper is not None:
                            require(all(cleanup.values()), "Other container cleanup incomplete; keeper retained")
                            try:
                                self._stop_role("keeper-stop", keeper, keeper_spec)
                            except (OSError, ValueError, subprocess.SubprocessError) as error:
                                infrastructure.append("keeper-stop:" + type(error).__name__ + ":" + str(error)[:512])
                        cleanup[name] = False
                        cleanup[name] = self._remove("cleanup-" + str(ordinal).zfill(3), spec, container_id, force=True)
                    except BaseException as error:
                        cleanup[name] = False
                        if original_error is None:
                            original_error = error
                        infrastructure.append("cleanup:" + type(error).__name__ + ":" + str(error)[:512])
                if volume_claimed and not self.journal.uncertain:
                    try:
                        require(all(cleanup.values()), "Container cleanup incomplete; volume retained")
                        inspected = self._command("volume-cleanup-inspect", ["docker", "volume", "inspect", "--format", "{{json .}}", intent["volume"]])
                        if self._clean(inspected):
                            require(self._volume_valid(engine.strict_json_loads(self._raw(inspected)), intent, volume_baseline), "Volume cleanup ownership differs")
                            self.emergency_cleanup.note_normal_removal(self._cleanup_claims[intent["volume"]])
                            removed = self._command("volume-remove", ["docker", "volume", "rm", intent["volume"]])
                            require(self._clean(removed), "Volume removal failed")
                        absent = self._checked("volume-absence", ["docker", "volume", "ls", "--quiet", "--filter", "name=^" + intent["volume"] + "$"])
                        volume_clean = not self._raw(absent).strip()
                        if volume_clean and self._clean(inspected):
                            self.emergency_cleanup.confirm_normal_removal(self._cleanup_claims[intent["volume"]],
                                remove_record="volume-remove.json", absence_record="volume-absence.json")
                    except BaseException as error:
                        if original_error is None:
                            original_error = error
                        infrastructure.append("volume-cleanup:" + type(error).__name__ + ":" + str(error)[:512])
                if self.journal.uncertain:
                    # This separate channel can only inspect/remove resources
                    # claimed before effects. It cannot heal the main journal.
                    self._emergency_after_failure(original_error or compact.ChainUnknown("HTTP journal uncertain"))
        assert self.journal is not None
        if self.journal.uncertain:
            if original_error is not None:
                raise original_error
            raise compact.ChainUnknown("Original HTTP journal uncertain; fallback cleanup grants no acceptance") from original_error
        if original_error is not None and not isinstance(original_error, (OSError, ValueError, subprocess.SubprocessError)):
            raise original_error
        self.checkpoint()
        self._retain("terminal.json", encoded({"protocol": self.binding.protocol, "mode": self.mode,
            "observation_registration": asdict(self.actual_registration),
            "cohort_freeze": None if self.retained_freeze is None else asdict(self.retained_freeze),
            "role_policy": role_policy_identity(),
            "intent_sha256": sha256(self.read_authenticated("intent.json")),
            "steps": [self._descriptor(x["label"] + "-result.json") for x in results],
            "observations": [self._descriptor(x["label"] + "-observation.json") for x in observations],
            "cli_observations": [self._descriptor(x["label"] + "-cli-observation.json") for x in cli_observations],
            "definition_sha256": self.recipe.definition_sha256, "quota_policy": quota_policy(),
            "initial_state_setup": self._descriptor("initial-state-complete.json")
                if self.has_authenticated("initial-state-complete.json") else None,
            "epochs": [self._descriptor(x["step_label"] + "-epoch.json") for x in epochs], "cleanup": cleanup, "volume_cleanup": volume_clean, "infrastructure": infrastructure,
            "planned_requests": intent["planned_requests"], "observed_probe_records": len(observations),
            "observed_requests": sum(row.get("authenticated") is True and row.get("sent_complete") is True for row in observations),
            "unknown_request_outcomes": len(probe_attempts) - sum(row.get("authenticated") is True for row in observations),
            "entered_step_indices": entered_step_indices,
            "unentered_step_ids": [x.step_id for x in self.recipe.steps[len(entered_step_indices):]],
            "probe_transport_attempts": probe_attempts, "cli_transport_attempts": cli_attempts,
            "attempted_probes": len(probe_attempts), "attempted_cli": len(cli_attempts),
            "unavailable_cli_attempts": len(cli_attempts) - sum(row.get("authenticated") is True for row in cli_observations),
            "attempt_evidence": "Full original artifacts remain discoverable by each prefix in the authenticated external checkpoint; attempts are not starts or sends.",
            "create_intents": len(owned), "unconfirmed_creates": sum(cid is None for _, cid in owned.values()),
            "created_helpers": sum(spec.role == "probe" and cid is not None for spec, cid in owned.values()),
            "created_servers": sum(spec.role == "server" and cid is not None for spec, cid in owned.values()),
            "created_cli": sum(spec.role == "cli" and cid is not None for spec, cid in owned.values()),
            "planned_cli": intent["planned_cli"], "observed_cli_records": len(cli_observations),
            "created_keepers": sum(spec.role == "keeper" and cid is not None for spec, cid in owned.values()),
            "start_response_records": [{"path": name, "sha256": sha256(self.read_authenticated(name)),
                "status": self.json_authenticated(name)["status"]} for name in sorted(self._start_responses)],
            "evaluator_sources_after": evaluator_sources()}))
        self._history_deadline = self._work_deadline = None

    def verified_execution(self) -> HttpHistoryResult:
        self._unchanged()
        require(self.mode == "physical", "Fixture journal cannot authenticate physical evidence")
        if not self.has_authenticated("terminal.json"):
            raise ExecutionUnknown("Original HTTP intent lacks terminal; never redispatch")
        intent, terminal = self.json_authenticated("intent.json"), self.json_authenticated("terminal.json")
        for record in (intent, terminal):
            require(record.get("observation_registration") == json.loads(encoded(asdict(self.actual_registration)))
                    and record.get("cohort_freeze") == (None if self.retained_freeze is None else json.loads(encoded(asdict(self.retained_freeze)))),
                    "Retained admission or cohort barrier changed")
        require(intent["protocol"] == self.binding.protocol and intent["config_sha256"] == digest(self.config)
                and intent["registration_sha256"] == digest(asdict(self.registration))
                and intent["role_policy"] == role_policy_identity(), "HTTP intent identity differs")
        require(terminal["protocol"] == self.binding.protocol and terminal["mode"] == "physical"
                and terminal["intent_sha256"] == sha256(self.read_authenticated("intent.json"))
                and terminal["evaluator_sources_after"] == self.sources
                and terminal["role_policy"] == role_policy_identity()
                and terminal["definition_sha256"] == self.recipe.definition_sha256
                and terminal["quota_policy"] == quota_policy(), "HTTP terminal identity differs")
        require(terminal.get("initial_state_setup") is None or terminal["initial_state_setup"] ==
                self._descriptor("initial-state-complete.json"), "Initial state descriptor differs")
        if terminal["entered_step_indices"]:
            require(terminal.get("initial_state_setup") is not None,
                    "Candidate steps lack completed trusted initial setup")
        require(type(terminal["steps"]) is list and len(terminal["steps"]) <= MAX_STEPS, "Bounded step references required")
        rows = [self._row(item) for item in terminal["steps"]]
        entered = terminal["entered_step_indices"]
        require(type(entered) is list and all(type(i) is int for i in entered)
                and entered == list(range(len(entered))) and len(rows) <= len(entered) <= min(len(rows) + 1, len(self.recipe.steps))
                and terminal["unentered_step_ids"] == [x.step_id for x in self.recipe.steps[len(entered):]],
                "Entered/unentered step census differs")
        for field, kind, counter in (("probe_transport_attempts", "probe", "attempted_probes"),
                                      ("cli_transport_attempts", "cli", "attempted_cli")):
            attempts = terminal[field]
            require(type(attempts) is list and len(attempts) <= len(entered)
                    and type(terminal[counter]) is int and terminal[counter] == len(attempts), "Invalid transport attempt census")
            indices: list[Any] = [x.get("step_index") for x in attempts if type(x) is dict]
            require(len(indices) == len(attempts) and all(type(i) is int and i in entered
                    and self.recipe.steps[i].kind == kind for i in indices)
                    and indices == sorted(set(indices)), "Transport attempts are not unique chronological entered steps")
        require(type(rows) is list and len(rows) <= len(self.recipe.steps), "Invalid step census")
        for index, row in enumerate(rows):
            step = self.recipe.steps[index]
            label = "step-" + str(index).zfill(3)
            require(row["step_id"] == step.step_id and row["step_index"] == index and row["kind"] == step.kind
                    and row["label"] == label and row["step_sha256"] == digest(step.record())
                    and row["definition_sha256"] == self.recipe.definition_sha256 and row["root_path"] == step.root_path
                    and row == self.json_authenticated(label + "-result.json"), "Step order/evidence differs")
        for field, kind in (("observations", "probe"), ("cli_observations", "cli")):
            require(type(terminal[field]) is list and len(terminal[field]) <= MAX_STEPS, "Bounded observation references required")
            observation_indices = [self._row(item).get("step_index") for item in terminal[field]]
            expected_indices = [i for i in range(len(rows)) if self.recipe.steps[i].kind == kind]
            possible = [expected_indices]
            if len(rows) < len(self.recipe.steps) and self.recipe.steps[len(rows)].kind == kind:
                possible.append([*expected_indices, len(rows)])
            require(all(type(i) is int for i in observation_indices) and observation_indices in possible,
                    "Observation descriptors must preserve exact-once ordered history prefix")
        epoch_refs = terminal["epochs"]
        starts = [i for i in entered if self.recipe.steps[i].kind == "start"]
        completed_starts = sum(x.kind == "start" for x in self.recipe.steps[:len(rows)])
        require(type(epoch_refs) is list and completed_starts <= len(epoch_refs) <= len(starts), "Invalid retained epoch census")
        epoch_ids: set[str] = set()
        for ordinal, descriptor in enumerate(epoch_refs):
            epoch_record = self._row(descriptor)
            index = starts[ordinal]
            step = self.recipe.steps[index]
            label = "step-" + str(index).zfill(3)
            require(descriptor["path"] == label + "-epoch.json"
                    and epoch_record["epoch"] == step.epoch and epoch_record["step_label"] == label
                    and epoch_record["root_path"] == step.root_path and epoch_record["argv"] == list(step.argv)
                    and epoch_record["database_path"] == self.recipe.database_path and epoch_record["database_volume"] == intent["volume"]
                    and epoch_record["source_sha256"] == self.binding.source_sha256
                    and epoch_record["fixture_sha256"] == self.binding.fixture_sha256
                    and epoch_record["server_id"] not in epoch_ids, "Epoch descriptor identity/order differs")
            epoch_ids.add(epoch_record["server_id"])
            if index < len(rows):
                require(all(encoded(rows[index].get(key)) == encoded(value) for key, value in epoch_record.items()),
                        "Epoch and complete step differ")
        observations: list[dict[str, Any]] = []
        for descriptor in terminal["observations"]:
            row = self._row(descriptor)
            index = row["step_index"]
            require(type(index) is int and 0 <= index < len(self.recipe.steps), "Invalid probe index")
            step = self.recipe.steps[index]
            require(step.kind == "probe" and row["step_id"] == step.step_id
                    and row == self.json_authenticated(row["label"] + "-observation.json"), "Probe observation differs")
            observed = dict(row)
            observed["wire"] = None
            if row["authenticated"] is True:
                require(row["donor"] == self.json_authenticated(row["label"] + "-donor.json")
                        and row["donor"]["inspection"] == self.json_authenticated(row["label"] + "-donor-inspection.json")
                        and row["donor"]["binding"] == asdict(self.binding)
                        and row["process"]["donor"] == row["donor"]
                        and row["process"]["identity_comparison"]["donor_sha256"] == digest(row["donor"]),
                        "Authenticated donor provenance differs")
                require(row["process"]["status"] == "completed" and row["process"]["completion"]["natural"] is True
                        and row["process"]["completion"]["inspect_exit_code"] == 0
                        and all(row[k]["matches"] is True for k in ("continuity_before", "continuity_after", "keeper_continuity")),
                        "Authenticated probe is missing its completion/continuity proof")
                observed["wire"] = wire.decode_probe_output(self._raw(row["process"]),
                    engine.strict_json_loads(step.request_json), self.recipe.port, self.policy.wire_limits)
            observations.append(observed)
        cli_observations: list[dict[str, Any]] = []
        for descriptor in terminal["cli_observations"]:
            row = self._row(descriptor)
            index = row["step_index"]
            require(type(index) is int and 0 <= index < len(self.recipe.steps), "Invalid CLI index")
            step = self.recipe.steps[index]
            require(step.kind == "cli" and row["step_id"] == step.step_id
                    and row["step_sha256"] == digest(step.record())
                    and row["definition_sha256"] == self.recipe.definition_sha256
                    and row["expected_argv"] == list(step.argv) and row["root_path"] == step.root_path
                    and row["database_volume"] == intent["volume"] and row["database_path"] == self.recipe.database_path
                    and row["binding"] == asdict(self.binding)
                    and row == self.json_authenticated(row["label"] + "-cli-observation.json")
                    and row["process"] == self.json_authenticated(row["label"] + "-cli-process-result.json"),
                    "CLI observation/lineage differs")
            if row["authenticated"] is True:
                require(row["process"]["protocol"] == engine.PROTOCOL
                        and row["process"]["completion"]["natural"] is True
                        and row["process"]["capture_complete"] is True
                        and row["keeper_continuity"]["matches"] is True, "CLI natural observation proof missing")
                self._raw(row["process"])
                self._raw(row["process"], "stderr")
            cli_observations.append(row)
        require(terminal["unknown_request_outcomes"] == terminal["attempted_probes"] - sum(x["authenticated"] is True for x in observations)
                and terminal["unavailable_cli_attempts"] == terminal["attempted_cli"] - sum(x["authenticated"] is True for x in cli_observations),
                "Unavailable attempt census differs")
        cleanup = terminal["volume_cleanup"] is True and bool(terminal["cleanup"]) and all(x is True for x in terminal["cleanup"].values())
        missing = tuple(x.step_id for x in self.recipe.steps[len(rows):])
        completed = not missing and cleanup and not terminal["infrastructure"] and all(x["authenticated"] is True for x in (*observations, *cli_observations))
        self._unchanged()
        return HttpHistoryResult(intent["execution_id"], "completed" if completed else "observation_unavailable",
            tuple(observations), missing, cleanup, tuple(terminal["infrastructure"]),
            sha256(self.read_authenticated("terminal.json")), self.checkpoint(), tuple(cli_observations))


_LOADED_SOURCES = evaluator_sources()
