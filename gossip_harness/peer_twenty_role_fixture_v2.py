"""Physical twenty-role offline plumbing qualification, never a quality study.

Twenty owned subprocesses have private meshes, role journals and (for sixteen
builders) Git stores. A seed and financial service supply two infrastructure
mesh members in the parent. Trusted fixed directives contain references only;
source, evidence, worker requests, results and bundles move through TCP gossip.
The injected transport emits deterministic patches and never contacts a model.
Run explicitly after the combined cheap gates, with a fresh --output directory.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import platform
import subprocess
import sys
import threading
import time
import traceback
import uuid
from typing import Any
import urllib.request

from .gitstore import GitStore
from .ledger import Ledger
from .peer_candidate_v2 import CandidatePublisher, strictdecode_candidate_notice
from .peer_coding_dispatch_v1 import source_digest
from .peer_financial_authority_v2 import CumulativeAuthorityV2, canonical_payload, ledger_identity
from .peer_financial_rpc_v2 import FinancialClient, FinancialRPC, FinancialServer, FinancialUnknownOutcome
from .peer_git_bundle_v1 import import_bundle
from .peer_mesh_finance_v2 import MeshFinancePayloads
from .peer_mesh_v2 import MeshConfig, MeshNode, observer_request
from .peer_project_contract_v2 import (
    ActionRequest, CandidateOffer, Context, DispatchBinding, DispatchReply, EvidenceRef, LocalViewManifest, WorkKey,
    from_dict, identity, strict_loads, to_dict, worker_request_digest,
)
from .peer_role_loop_v2 import RoleLoop, WorkDirective, financial_task_id
from .worker import HTTPResponse, MODEL, OpenAIWorker, WorkerRequest, WorkerResult

PROTOCOL = "peer-twenty-role-fixture-v2"
BUILDERS = tuple(f"B{number:02d}" for number in range(1, 17))
REVIEWERS = tuple(f"R{number}" for number in range(1, 5))
ROLES = BUILDERS + REVIEWERS
ROSTER = ROLES + ("seed", "finance")
PACKAGES = ("catalog", "ingestion", "query", "clients")
EVIDENCE_MARKER = "fixture-read-only-evidence-8713"
PROBE_MARKER = b"gossip-continues-while-financial-workers-wait"
SOURCES = (
    "__init__.py", "peer_twenty_role_fixture_v2.py", "peer_mesh_v2.py", "peer_mesh_store_v2.py",
    "peer_mesh_finance_v2.py", "peer_financial_rpc_v2.py", "peer_financial_authority_v2.py",
    "peer_role_loop_v2.py", "peer_candidate_v2.py", "peer_project_contract_v2.py",
    "worker.py", "ledger.py", "peer_authority_v1.py", "verification_journal.py",
    "gitstore.py", "peer_git_bundle_v1.py", "peer_store_v1.py", "peer_coding_dispatch_v1.py",
)


class FixtureError(RuntimeError):
    """Qualification stopped; retained artifacts must not be reused as a pass."""


def _require(condition: bool, detail: str) -> None:
    if not condition:
        raise FixtureError(detail)


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _write(path: Path, value: Any) -> None:
    """Atomically publish one immutable checkpoint; collisions retain both files."""
    temporary = path.with_name(path.name + ".pending-" + uuid.uuid4().hex)
    with temporary.open("xb") as stream:
        stream.write(canonical_payload(value))
        stream.flush()
        os.fsync(stream.fileno())
    os.link(temporary, path)
    temporary.unlink()
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _read(path: Path) -> Any:
    return strict_loads(path.read_bytes())


def fixture_seed() -> dict[str, str]:
    return {**{f"{package}/part.py": "VALUE = 'seed'\n" for package in PACKAGES},
            "README.txt": "Trusted plumbing fixture; candidate code is never executed.\n"}


def package_for(actor: str) -> str:
    _require(actor in ROLES, "Unregistered role")
    return PACKAGES[(BUILDERS.index(actor) // 4) if actor in BUILDERS else REVIEWERS.index(actor)]


def role_path(actor: str) -> str:
    return f"{package_for(actor)}/part.py" if actor in BUILDERS else "review.json"


@dataclass(frozen=True)
class FixtureConfig:
    deadline_seconds: float = 240.0
    interval: float = 0.15
    executor_slots: int = 4

    def __post_init__(self) -> None:
        _require(type(self.deadline_seconds) in (int, float) and 60 <= self.deadline_seconds <= 600,
                 "Deadline must be finite and between 60 and 600 seconds")
        _require(type(self.interval) in (int, float) and 0.04 <= self.interval <= 0.25,
                 "Mesh interval outside declared fixture bounds")
        _require(type(self.executor_slots) is int and self.executor_slots == 4,
                 "This qualification contract requires exactly four executor slots")


def source_fingerprints() -> dict[str, str]:
    directory = Path(__file__).parent
    return {name: _sha((directory / name).read_bytes()) for name in SOURCES}


def runtime_identity() -> dict[str, str]:
    git = shutil.which("git")
    assert git is not None, "Git executable unavailable"
    version = subprocess.run(["git", "--version"], capture_output=True, check=True, timeout=5)
    return {"python_version": sys.version, "python_executable": str(Path(sys.executable).resolve()),
            "platform": platform.platform(), "git_executable": str(Path(git).resolve()),
            "git_version": version.stdout.decode("utf-8").strip()}


def attest_execution(expected_sources: dict[str, str], expected_runtime: dict[str, str]) -> dict[str, Any]:
    sources, runtime = source_fingerprints(), runtime_identity()
    _require(sources == expected_sources, "Execution source inventory changed")
    _require(runtime == expected_runtime, "Execution runtime identity changed")
    return {"sources": sources, "runtime": runtime}


def execution_contract(config: FixtureConfig) -> dict[str, Any]:
    return {"protocol": PROTOCOL, "purpose": "independent-offline-plumbing-qualification",
            "roles": list(ROLES), "builders": list(BUILDERS), "reviewers": list(REVIEWERS),
            "infrastructure": ["seed", "finance"], "settings": asdict(config),
            "sources": source_fingerprints(), "runtime": runtime_identity(), "seed_sha256": _sha(canonical_payload(fixture_seed())),
            "transport": "injected-deterministic-no-provider", "candidate_execution": False,
            "claims_excluded": ["software-quality", "scope-approval", "release-gate", "live-model",
                                "statistical-superiority", "process-restart", "partition-recovery"]}


class DropAcknowledgment:
    """Simulate application ACK loss by discarding one authenticated admitted reply.

    This does not claim a TCP disconnect or a financial process crash.
    """
    def __init__(self, client: FinancialClient):
        self.client = client
        self.dropped = False

    def claim(self, context: Context, work: WorkKey, request_id: str, *, ttl: float = 60) -> Any:
        return self.client.claim(context, work, request_id, ttl=ttl)

    def renew(self, lease: Any, request_id: str, *, ttl: float = 60) -> Any:
        return self.client.renew(lease, request_id, ttl=ttl)

    def lookup(self, request_id: str) -> DispatchReply | None:
        return self.client.lookup(request_id)

    def submit(self, action: ActionRequest, lease: Any) -> DispatchReply:
        reply = self.client.submit(action, lease)
        if reply.binding is not None and not self.dropped:
            self.dropped = True
            raise FinancialUnknownOutcome("submit", action.request_id,
                                          {"action": to_dict(action), "lease": asdict(lease)})
        return reply


def _child(config_path: Path) -> int:
    raw = _read(config_path)
    root = config_path.parent
    node = MeshNode(MeshConfig.from_dict(raw["mesh"]))
    loop: RoleLoop | None = None
    deadline = raw["deadline_monotonic"]
    try:
        attestation = attest_execution(raw["source_inventory"], raw["runtime_identity"])
        _write(root / "ready.json", {**node.start(), "attestation": attestation})
        while not (root / "directive.json").exists():
            _require(time.monotonic() < deadline, "Role bootstrap deadline")
            time.sleep(0.03)
        directive = WorkDirective.from_dict(_read(root / "directive.json"))
        client = FinancialClient(raw["finance_port"], raw["actor"], raw["capability"], raw["contract_sha256"])
        dropped = DropAcknowledgment(client)
        loop = RoleLoop(root / "journal", raw["actor"], node, dropped, call_limit=1,
                        policy_sha256=raw["policy_sha256"], result_producer="finance", lease_ttl=180)
        key = loop.enqueue(directive)
        probe: dict[str, Any] | None = None
        while time.monotonic() < deadline:
            snapshot = loop.tick()
            if probe is None:
                for ref in node.arrived():
                    if ref.producer == "seed" and ref.kind == "fixture-late-evidence":
                        node.want(ref)
                        payload = node.resolve(ref)
                        if payload is not None:
                            _require(payload == PROBE_MARKER, "Late evidence bytes differ")
                            probe = {"ref": to_dict(ref), "arrived_at": time.monotonic(),
                                     "role_state": snapshot.state if snapshot else "none",
                                     "mesh_ticks": node.summary()["stats_since_boot"]["ticks"]}
                            _write(root / "probe.json", probe)
            if snapshot is not None and snapshot.state in ("published", "stopped"):
                _require(snapshot.state == "published" and snapshot.reply is not None
                         and snapshot.reply.state == "completed" and snapshot.reply.binding is not None,
                         "Role did not publish a successful bounded completion")
                break
            time.sleep(0.04)
        else:
            raise FixtureError("Role action deadline")
        assert snapshot is not None and snapshot.reply is not None and snapshot.reply.binding is not None
        assert snapshot.result_ref is not None and snapshot.action is not None and snapshot.view is not None
        request = loop.worker_request(key)
        result_raw = node.resolve(snapshot.result_ref)
        assert result_raw is not None, "Completed result bytes absent"
        result_body = strict_loads(result_raw)
        _require(result_body["kind"] == "result", "Result envelope differs")
        result = WorkerResult(**result_body["payload"])
        binding = snapshot.reply.binding
        assert snapshot.reply.result_payload_sha256 is not None and snapshot.publication_ref is not None
        replay = client.submit(snapshot.action, binding.lease)
        _require(replay == snapshot.reply and client.lookup(snapshot.action.request_id) == replay,
                 "Exact terminal RPC replay differs")
        candidate: dict[str, Any] | None = None
        git_path: str | None = None
        if raw["actor"] in BUILDERS:
            git_path = str(root / "git")
            store = GitStore.create(Path(git_path), request.files)
            _require(store.head() == request.base_sha, "Arrived seed does not reproduce private Git baseline")
            publisher = CandidatePublisher(root / "candidate", node, store, actor=raw["actor"],
                                           baseline_sha=request.base_sha,
                                           package_scopes={package: (f"{package}/part.py",) for package in PACKAGES},
                                           execution_contract_sha256=raw["contract_sha256"],
                                           completed_proof=loop.completed_proof)
            publication = publisher.publish(binding, request, result, snapshot.reply.result_payload_sha256)
            _require(publisher.publish(binding, request, result, snapshot.reply.result_payload_sha256) == publication,
                     "Exact candidate publication replay differs")
            candidate = {"offer": to_dict(publication.offer), "offer_ref": to_dict(publication.offer_ref)}
        _write(root / "result.json", {
            "actor": raw["actor"], "pid": os.getpid(), "root": str(root), "git_path": git_path,
            "attestation": attest_execution(raw["source_inventory"], raw["runtime_identity"]),
            "mesh_root": str(node.config.root), "journal_root": str(loop.root),
            "request_sha256": worker_request_digest(request), "view": to_dict(snapshot.view),
            "action": to_dict(snapshot.action), "reply": to_dict(snapshot.reply),
            "result_ref": to_dict(snapshot.result_ref), "publication_ref": to_dict(snapshot.publication_ref),
            "source_materialized": request.files == fixture_seed(),
            "readonly_evidence_materialized": EVIDENCE_MARKER in request.feedback,
            "dropped_acknowledgment": dropped.dropped, "exact_terminal_replay": True,
            "candidate": candidate, "history": list(loop.history()), "probe": probe,
            "mesh_summary": node.summary(), "finished_at": time.monotonic(),
        })
        while not (root / "stop").exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        return 0
    except BaseException:
        _write(root / "failure.json", {"pid": os.getpid(), "traceback": traceback.format_exc()})
        return 1
    finally:
        try:
            if loop is not None:
                loop.close()
        finally:
            node.close()


class FixtureTransport:
    """Deterministic completion transport, with a measurable first-wave barrier."""
    def __init__(self, tasks: dict[str, str], release: threading.Event):
        self.tasks, self.release = tasks, release
        self.lock = threading.Lock()
        self.calls: list[dict[str, Any]] = []
        self.active = 0
        self.peak = 0

    def __call__(self, request: urllib.request.Request, timeout: float, maximum: int) -> HTTPResponse:
        raw_data = request.data
        assert isinstance(raw_data, bytes), "Injected request data must be bytes"
        payload = json.loads(raw_data)
        task = json.loads(payload["input"])
        actor = self.tasks[task["task_id"]]
        _require(task["files"] == fixture_seed() and task["allowed_paths"] == [role_path(actor)]
                 and EVIDENCE_MARKER in task["validation_feedback"],
                 "Injected worker did not receive complete source and read-only evidence")
        with self.lock:
            call = {"actor": actor, "task_id": task["task_id"], "started_at": time.monotonic(),
                    "finished_at": None, "request_sha256": _sha(raw_data),
                    "materialized": True, "held": len(self.calls) < 4}
            self.calls.append(call)
            self.active += 1
            self.peak = max(self.peak, self.active)
        try:
            if call["held"]:
                _require(self.release.wait(90), "First-wave gossip observation barrier expired")
            time.sleep(0.1)
            content = (f"VALUE = {actor!r}\n" if actor in BUILDERS else
                       json.dumps({"protocol": "fixture-review-artifact", "actor": actor,
                                   "scope_approval": False, "evidence_seen": EVIDENCE_MARKER}))
            proposal = {"changes": [{"path": role_path(actor), "content": content}],
                        "summary": "Deterministic offline plumbing artifact; no quality judgment."}
            body = {"id": "resp_fixture_" + actor, "model": MODEL, "status": "completed",
                    "service_tier": "default", "usage": {"input_tokens": 101, "output_tokens": 20,
                                                            "total_tokens": 121},
                    "output": [{"type": "message", "role": "assistant", "status": "completed",
                                "content": [{"type": "output_text", "text": json.dumps(proposal)}]}]}
            return HTTPResponse(200, {"X-Request-Id": "req_fixture_" + actor}, json.dumps(body).encode())
        finally:
            with self.lock:
                call["finished_at"] = time.monotonic()
                self.active -= 1

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return {"calls": [dict(call) for call in self.calls], "peak": self.peak, "active": self.active}


def verify_candidate_source(offer: CandidateOffer, request: WorkerRequest, result: WorkerResult,
                            reply: DispatchReply, imported_files: dict[str, str],
                            imported_base_files: dict[str, str], manifest_base: str,
                            base_is_ancestor: bool) -> None:
    """Receiver binds actual imported bytes to the independently retained completion."""
    actor = offer.dispatch.action.actor
    _require(actor in BUILDERS and offer.dispatch.action.work.package_id == package_for(actor),
             "Candidate actor or package differs")
    _require(reply.state == "completed" and reply.binding == offer.dispatch
             and reply.result_payload_sha256 == offer.result_payload_sha256
             and _sha(canonical_payload({"kind": "result", "payload": asdict(result)})) == offer.result_payload_sha256
             and worker_request_digest(request) == offer.dispatch.normalized_worker_request_sha256,
             "Candidate differs from the retained financial completion")
    _require(manifest_base == request.base_sha and base_is_ancestor is True
             and imported_base_files == request.files, "Imported candidate base differs")
    _require(request.allowed_paths == (role_path(actor),)
             and bool(result.changes) and all(path in request.allowed_paths for path in result.changes),
             "Candidate patch exceeds exact registered scope")
    expected = dict(request.files)
    for path, content in result.changes.items():
        if content is None:
            expected.pop(path, None)
        else:
            expected[path] = content
    _require(expected != request.files and imported_files == expected
             and source_digest(imported_files) == offer.source_sha256,
             "Imported candidate source differs from the actual settled patch")


def validate_observations(roles: list[dict[str, Any]], transport: dict[str, Any],
                          candidates: list[dict[str, Any]]) -> dict[str, Any]:
    """Pure qualification assertions; an infrastructure failure cannot become a pass."""
    _require(len(roles) == 20 and {role["actor"] for role in roles} == set(ROLES), "Expected twenty role receipts")
    _require(all(type(role["pid"]) is int and role["pid"] > 0 for role in roles)
             and len({role["pid"] for role in roles}) == 20, "Roles did not have twenty unique PIDs")
    for field in ("root", "mesh_root", "journal_root"):
        _require(len({role[field] for role in roles}) == 20, "Role state roots overlap")
    _require(all(role["source_materialized"] is True and role["readonly_evidence_materialized"] is True
                 and role["dropped_acknowledgment"] is True and role["exact_terminal_replay"] is True
                 and role["reply"]["state"] == "completed" and not role["view"]["omitted_required_ids"]
                 and role["mesh_summary"]["fatal_error"] is None for role in roles),
             "Incomplete evidence, replay or role completion")
    calls = transport["calls"]
    _require(len(calls) == 20 and {call["actor"] for call in calls} == set(ROLES)
             and len({call["task_id"] for call in calls}) == 20
             and all(call["materialized"] is True and call["finished_at"] is not None for call in calls),
             "Missing or duplicate injected provider invocations")
    _require(2 <= transport["peak"] <= 4 and transport["active"] == 0, "No bounded overlap proof")
    held = [call for call in calls if call["held"]]
    _require(len(held) == 4, "First-wave barrier membership differs")
    indexed = {role["actor"]: role for role in roles}
    for call in held:
        probe = indexed[call["actor"]]["probe"]
        _require(probe is not None and call["started_at"] < probe["arrived_at"] < call["finished_at"]
                 and probe["mesh_ticks"] > 0 and probe["role_state"] in ("pending", "submit_pending"),
                 "Evidence did not arrive during an outstanding coding call")
    _require(len(candidates) == 16 and {item["actor"] for item in candidates} == set(BUILDERS)
             and all(item["verified"] is True for item in candidates), "Private candidate bundles were not verified")
    _require(len({role["git_path"] for role in roles if role["actor"] in BUILDERS}) == 16
             and all(role["git_path"] is not None for role in roles if role["actor"] in BUILDERS),
             "Builders did not own sixteen private Git repositories")
    return {"role_processes": 20, "builder_processes": 16, "reviewer_processes": 4,
            "infrastructure_mesh_members": 2, "injected_calls": 20, "peak_injected_calls": transport["peak"],
            "lost_acknowledgments_reconciled": 20, "exact_terminal_replays": 20,
            "gossip_during_wait_observations": 4, "verified_candidate_bundles": 16,
            "api_calls": 0, "api_spend_micro_usd": 0, "candidate_executions": 0,
            "scope_approvals": 0, "protected_releases": 0}


def qualify_before_deadline(roles: list[dict[str, Any]], transport: dict[str, Any],
                            candidates: list[dict[str, Any]], deadline: float) -> dict[str, Any]:
    """Final qualification includes time spent in runtime attestation and assertions."""
    counts = validate_observations(roles, transport, candidates)
    _require(time.monotonic() < deadline, "Scenario deadline exceeded before qualification")
    return counts


def _wait(predicate: Any, deadline: float, processes: dict[str, subprocess.Popen[Any]]) -> None:
    while True:
        _require(time.monotonic() < deadline, "Scenario deadline exceeded")
        failed = [actor for actor, process in processes.items() if process.poll() is not None]
        _require(not failed, "Role exited before qualification: " + ", ".join(failed))
        ready = predicate()
        _require(time.monotonic() < deadline, "Scenario deadline exceeded")
        if ready:
            return
        time.sleep(0.05)


def run_fixture(output: Path, config: FixtureConfig = FixtureConfig()) -> dict[str, Any]:
    output = output.absolute()
    _require(output == output.resolve() and not output.exists(), "Output must be a fresh canonical path")
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    started = time.monotonic()
    deadline = started + config.deadline_seconds
    contract = execution_contract(config)
    contract_sha = _sha(canonical_payload(contract))
    _write(output / "execution-contract.json", contract)
    transport_key, observer_key = secrets.token_hex(32), secrets.token_hex(32)
    capabilities = {actor: secrets.token_hex(32) for actor in ROLES}
    processes: dict[str, subprocess.Popen[Any]] = {}
    streams: list[Any] = []
    nodes: list[MeshNode] = []
    finance: CumulativeAuthorityV2 | None = None
    payloads: MeshFinancePayloads | None = None
    server: FinancialServer | None = None
    server_thread: threading.Thread | None = None
    release = threading.Event()
    transport: FixtureTransport | None = None
    receipt: dict[str, Any] = {"protocol": PROTOCOL, "physically_executed": True,
                               "execution_contract_sha256": contract_sha, "passed": False}
    cleanup_errors: list[str] = []
    try:
        def mesh_config(actor: str, root: Path) -> MeshConfig:
            return MeshConfig(root, actor, "twenty-role-fixture", contract_sha, ROSTER, transport_key,
                              observer_key=observer_key, interval=config.interval, fanout=2)
        seed = MeshNode(mesh_config("seed", output / "seed-mesh"))
        nodes.append(seed)
        financial_mesh = MeshNode(mesh_config("finance", output / "finance-mesh"))
        nodes.append(financial_mesh)
        seed_ready, finance_ready = seed.start(), financial_mesh.start()
        base = GitStore.create(output / "seed-git", fixture_seed()).head()
        source_ref = seed.publish("source", canonical_payload({"files": fixture_seed(), "base_sha": base}), "seed-source")
        evidence_ref = seed.publish("fixture-evidence", canonical_payload({"readonly": EVIDENCE_MARKER}), "seed-evidence")
        context = Context(contract_sha, "twenty-role-fixture", "healthy-plumbing", 0,
                          _sha(canonical_payload({"source": to_dict(source_ref), "evidence": to_dict(evidence_ref)})))
        directives = {actor: WorkDirective(context, WorkKey(package_for(actor), "fixture", actor, 0),
                                           "mini", "build" if actor in BUILDERS else "review", source_ref,
                                           (evidence_ref,), (role_path(actor),),
                                           f"Produce the declared deterministic plumbing artifact for {actor}.")
                      for actor in ROLES}
        tasks = {financial_task_id(context, directive.work): actor for actor, directive in directives.items()}
        transport = FixtureTransport(tasks, release)
        worker = OpenAIWorker("offline-fixture-not-a-real-key", max_output_tokens=128, timeout=95, transport=transport)
        ledger_path = output / "disposable-ledger.sqlite"
        Ledger(ledger_path, 10_000_000)
        cohort = {"cohort_id": context.cohort_id, "execution_contract_sha256": contract_sha,
                  "ledger_identity": ledger_identity(ledger_path), "journal_root": str(output / "financial-journal"),
                  "transport_identity": "twenty-role-injected-fixture", "task_specs": [
                      {"context": to_dict(context), "work": to_dict(directive.work), "actors": [actor],
                       "kinds": [directive.kind], "profiles": ["mini"],
                       "allowed_paths": list(directive.allowed_paths), "max_reserved_units": 400_000}
                      for actor, directive in directives.items()]}
        payloads = MeshFinancePayloads(output / "financial-payloads", financial_mesh, ROLES)
        finance = CumulativeAuthorityV2.open(ledger_path, output / "financial-service", cohort,
                                            10_000_000, 10_000_000, 0, payloads=payloads,
                                            workers={"mini": worker}, max_workers=config.executor_slots)
        rpc = FinancialRPC(finance, capabilities, request_guard=payloads.request_guard,
                           request_guard_sha256=payloads.request_guard_sha256)
        server = FinancialServer(0, rpc)
        server_thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
        server_thread.start()
        policy_sha = _sha(b"trusted-fixed-local-directive-fixture-policy-v2")
        env = {name: value for name, value in os.environ.items()
               if name not in {"OPENAI_API_KEY", "OPENAI_ORG_ID", "OPENAI_PROJECT_ID"}}
        for actor in ROLES:
            role_root = output / "roles" / actor
            role_root.mkdir(parents=True)
            config_path = role_root / "config.json"
            mesh = mesh_config(actor, role_root / "mesh")
            mesh_raw = asdict(mesh)
            mesh_raw["root"], mesh_raw["roster"] = str(mesh.root), list(mesh.roster)
            mesh_raw["brokers"] = list(mesh.brokers)
            _write(config_path, {"actor": actor, "mesh": mesh_raw, "capability": capabilities[actor],
                                  "finance_port": server.server_address[1], "contract_sha256": contract_sha,
                                  "policy_sha256": policy_sha, "deadline_monotonic": deadline,
                                  "source_inventory": contract["sources"], "runtime_identity": contract["runtime"]})
            stream = (role_root / "process.log").open("xb")
            streams.append(stream)
            processes[actor] = subprocess.Popen([sys.executable, "-m", "gossip_harness.peer_twenty_role_fixture_v2",
                                                 "--role-config", str(config_path)], stdout=stream, stderr=stream,
                                                env=env, cwd=Path(__file__).resolve().parents[1])
        _wait(lambda: all((output / "roles" / actor / "ready.json").exists() for actor in ROLES), deadline, processes)
        ready = {actor: _read(output / "roles" / actor / "ready.json") for actor in ROLES}
        _require(all(ready[actor]["pid"] == processes[actor].pid for actor in ROLES), "Child PID readiness differs")
        peers = {**{actor: value["port"] for actor, value in ready.items()},
                 "seed": seed_ready["port"], "finance": finance_ready["port"]}
        seed.configure(peers)
        financial_mesh.configure(peers)
        for actor in ROLES:
            observer_request(peers[actor], actor, observer_key, "configure", {"peers": peers})
        for actor in ROLES:
            _write(output / "roles" / actor / "directive.json", directives[actor].to_dict())
        _wait(lambda: len(transport.snapshot()["calls"]) >= 4, deadline, processes)
        probe_ref = seed.publish("fixture-late-evidence", PROBE_MARKER, "probe-during-first-wave")
        held = [call["actor"] for call in transport.snapshot()["calls"] if call["held"]]
        _wait(lambda: all((output / "roles" / actor / "probe.json").exists() for actor in held), deadline, processes)
        _write(output / "held-wave-observation.json", {"probe_ref": to_dict(probe_ref), "held": held,
                "observed_at": time.monotonic(), "transport": transport.snapshot(),
                "role_probes": {actor: _read(output / "roles" / actor / "probe.json") for actor in held}})
        release.set()
        _wait(lambda: all((output / "roles" / actor / "result.json").exists() for actor in ROLES), deadline, processes)
        roles = [_read(output / "roles" / actor / "result.json") for actor in ROLES]
        candidates: list[dict[str, Any]] = []
        for role in roles:
            binding = from_dict(DispatchBinding, role["reply"]["binding"])
            terminal = finance.verified_terminal(binding)
            view = from_dict(LocalViewManifest, role["view"])
            view.require_complete()
            _require(identity(view) == binding.action.view_manifest_sha256
                     and view.materialized_content_sha256 == role["request_sha256"]
                     and view.source_ref == source_ref and view.evidence_refs == (evidence_ref,),
                     "Materialized role view differs from the bound required evidence")
            _require(worker_request_digest(terminal["worker_request"]) == role["request_sha256"]
                     and identity(from_dict(Context, role["action"]["context"])) == identity(context),
                     "Role receipt differs from independently retained financial completion")
            if role["actor"] not in BUILDERS:
                continue
            offer_ref = from_dict(EvidenceRef, role["candidate"]["offer_ref"])
            def arrived(ref: EvidenceRef) -> bool:
                return ref in seed.arrived() and seed.want(ref) and seed.resolve(ref) is not None
            _wait(lambda: arrived(offer_ref), deadline, processes)
            offer_raw = seed.resolve(offer_ref)
            assert offer_raw is not None
            offer, manifest = strictdecode_candidate_notice(offer_raw, expected_contract_sha256=contract_sha,
                                                            expected_actor=role["actor"])
            _require(offer.dispatch == binding and to_dict(offer) == role["candidate"]["offer"], "Candidate notice differs")
            _wait(lambda: arrived(offer.bundle_ref), deadline, processes)
            bundle_raw = seed.resolve(offer.bundle_ref)
            assert bundle_raw is not None
            imported = import_bundle(bundle_raw, manifest, output / "quarantine" / role["actor"])
            verify_candidate_source(offer, terminal["worker_request"], terminal["result"], terminal["reply"],
                                    imported.read_files(offer.commit_oid), imported.read_files(manifest["base_sha"]),
                                    manifest["base_sha"], imported.is_ancestor(manifest["base_sha"], offer.commit_oid))
            candidates.append({"actor": role["actor"], "verified": True, "commit_oid": offer.commit_oid,
                               "source_sha256": offer.source_sha256, "offer_ref": to_dict(offer_ref),
                               "bundle_ref": to_dict(offer.bundle_ref), "quarantine": str(imported.path)})
        _require(time.monotonic() < deadline, "Scenario deadline exceeded after bundle imports")
        attest_execution(contract["sources"], contract["runtime"])
        observations = transport.snapshot()
        counts = qualify_before_deadline(roles, observations, candidates, deadline)
        receipt.update({"passed": True, "counts": counts, "roles": roles, "transport": observations,
                        "candidates": candidates, "disposable_ledger_budget": finance.ledger.budget(),
                        "scope": contract["claims_excluded"], "runtime": contract["runtime"],
                        "elapsed_seconds": time.monotonic() - started})
    except BaseException:
        receipt["failure"] = traceback.format_exc()
        if transport is not None:
            receipt["transport"] = transport.snapshot()
    finally:
        release.set()
        for actor in processes:
            try:
                (output / "roles" / actor / "stop").touch(exist_ok=False)
            except BaseException as error:
                cleanup_errors.append("Stop marker " + actor + ": " + repr(error))
        stop_deadline = time.monotonic() + 12
        for actor, process in processes.items():
            try:
                try:
                    process.wait(timeout=max(0.01, stop_deadline - time.monotonic()))
                except subprocess.TimeoutExpired:
                    process.terminate()
                    try:
                        process.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=3)
                    cleanup_errors.append("Role required forced cleanup: " + actor)
                if process.returncode != 0:
                    cleanup_errors.append(f"Role {actor} exited {process.returncode}")
            except BaseException as error:
                cleanup_errors.append("Process cleanup " + actor + ": " + repr(error))
        for stream in streams:
            try:
                stream.close()
            except BaseException as error:
                cleanup_errors.append("Log close: " + repr(error))
        if server is not None:
            try:
                if server_thread is not None and server_thread.is_alive():
                    server.shutdown()
            except BaseException as error:
                cleanup_errors.append("Financial server shutdown: " + repr(error))
            try:
                server.server_close()
            except BaseException as error:
                cleanup_errors.append("Financial server close: " + repr(error))
        if server_thread is not None and server_thread.ident is not None:
            try:
                server_thread.join(3)
                _require(not server_thread.is_alive(), "Financial server did not stop")
            except BaseException as error:
                cleanup_errors.append("Financial thread cleanup: " + repr(error))
        for resource in (finance, payloads, *reversed(nodes)):
            if resource is not None:
                try:
                    resource.close()
                except BaseException as error:
                    cleanup_errors.append(type(error).__name__ + ": " + str(error))
        receipt["cleanup_errors"] = cleanup_errors
        receipt["owned_process_exit_codes"] = {actor: process.returncode for actor, process in processes.items()}
        receipt["passed"] = receipt["passed"] and not cleanup_errors
        _write(output / "receipt.json", receipt)
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--output", type=Path)
    mode.add_argument("--role-config", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--deadline-seconds", type=float, default=240)
    options = parser.parse_args()
    if options.role_config is not None:
        return _child(options.role_config)
    receipt = run_fixture(options.output, FixtureConfig(deadline_seconds=options.deadline_seconds))
    print(json.dumps({"passed": receipt["passed"], "receipt": str(options.output.absolute() / "receipt.json"),
                      "counts": receipt.get("counts"), "cleanup_errors": receipt["cleanup_errors"]}))
    return 0 if receipt["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
