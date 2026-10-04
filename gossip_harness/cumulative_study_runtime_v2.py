"""Actual gossip/OS-process/V5-finance/Git composition for the cumulative study.

This adapter runs only with explicit caller-owned workers, original ledger,
prospective permits, authored public checks and the process-evidence bridge.
No key/environment discovery, wallet initialization, provider retry, authored
product solver or alternative-to-failed-public-check fallback is supplied.
"""
from __future__ import annotations

from dataclasses import asdict
import hashlib
import json
import os
import platform
from pathlib import Path
import secrets
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from typing import Any, Callable

from .candidate_observation_admission_v1 import source_sha256
from .cumulative_study_controller_v2 import (PACKAGES, PUBLIC_PURPOSE, REVIEWERS,
    ChildTerminalSeal, PublicResult, Records, Release, StudyPlan,
    StudyStop, StudyUnknown, TRANSPORT_CONTRACT, digest, package_for, plain, require)
from .gitstore import GitStore, Candidate
from .peer_financial_authority_v2 import canonical_payload, ledger_identity
from .peer_financial_authority_v3 import FIXTURE_TRANSPORT, LIVE_TRANSPORT, profile_manifest
from .peer_financial_authority_v5 import CumulativeAuthorityV5
from .peer_financial_terminal_v1 import SealBusy
from .peer_financial_terminal_v2 import checked_policy, checked_financial_config
from .peer_financial_rpc_v2 import FinancialServer, FinancialDenied
from .peer_financial_rpc_v5 import FinancialRPCV5
from .peer_mesh_finance_v2 import MeshFinancePayloads
from .peer_mesh_v2 import MeshConfig, MeshNode, observer_request
from .peer_mesh_store_v2 import MeshLimits
from .peer_project_contract_v2 import (Context, DispatchBinding, EvidenceRef, WorkKey,
    from_dict, resolve_local, strict_loads, to_dict, worker_request_digest, identity)
from .peer_role_loop_v2 import WorkDirective, directive_id, materialize
from .worker import OpenAIWorker

PROTOCOL = "cumulative-study-runtime-v2"
ROLE_PROTOCOL = "cumulative-study-role-v1"


def _source(store: GitStore) -> dict[str, Any]:
    head = store.head()
    files = store.read_files(head)
    require(store.head() == head, "Protected source moved while capturing")
    return {"commit_oid": head, "source_sha256": source_sha256({p:s.encode() for p,s in files.items()}), "files": files}


def _write_exact(path: Path, value: dict[str, Any]) -> None:
    raw = canonical_payload(plain(value))
    if path.exists():
        require(path.is_file() and not path.is_symlink() and path.read_bytes() == raw, "Immutable local config changed")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class DockerPublicChecks:
    """Exact caller-authored cumulative public suite through existing sandbox.

    The suite must emit one canonical JSON object with ordered outcomes. An exit
    status alone cannot prove that all declared checks ran. All raw sandbox
    output stays in the original receipt. This is public feedback only.
    """
    def __init__(self, image: str, *, timeout: float = 180):
        self.image, self.timeout = image, timeout

    def __call__(self, store: GitStore, release: Release) -> PublicResult:
        from .sandbox import DockerValidator
        source = _source(store)
        if not release.checks:
            return PublicResult("unavailable", source["commit_oid"], source["source_sha256"],
                release.sha256, release.ordered_check_ids,
                tuple((key, "unknown") for key in release.ordered_check_ids), {"reason": "No authored public suite"})
        validator = DockerValidator(self.image, release.checks, release.command, self.timeout)
        with store._checkout(source["commit_oid"]) as checkout:
            passed, detail = validator(checkout)
        receipt = dict(validator.last_receipt)
        # Frozen sandbox exposes detail/output without a structured case API.
        # A separate authored wrapper must place its exact JSON in stdout.
        raw = receipt.get("output")
        try:
            require(type(raw) is str, "Public result output missing")
            assert isinstance(raw, str)
            value = strict_loads(raw.encode())
            require(type(value) is dict and set(value) == {"purpose", "outcomes"}
                    and value["purpose"] == PUBLIC_PURPOSE, "Wrong public result format")
            outcomes = tuple(tuple(x) for x in value["outcomes"])
            require(tuple(x[0] for x in outcomes) == release.ordered_check_ids
                    and all(x[1] in ("passed", "failed") for x in outcomes), "Incomplete public check census")
            require(not receipt.get("output_truncated") and receipt.get("cleanup_verified") is True
                    and receipt.get("status") in ("passed", "failed")
                    and passed == all(x[1] == "passed" for x in outcomes), "Sandbox status and public outcomes disagree")
            status = "completed"
        except (TypeError, ValueError, KeyError):
            status, outcomes = "infrastructure_error", tuple((x, "unknown") for x in release.ordered_check_ids)
        return PublicResult(status, source["commit_oid"], source["source_sha256"], release.sha256,
            release.ordered_check_ids, outcomes, {"sandbox": receipt, "detail": detail})


class GossipChildRuntime:
    def __init__(self, plan: StudyPlan, index: int, records: Records, deadline: float, *,
                 root: Path, repository: Path, existing_ledger_path: Path,
                 expected_ledger_identity: dict[str, Any], workers: dict[str, OpenAIWorker], mode: str,
                 permit_provider: Callable[[dict[str, Any]], dict[str, Any]],
                 evaluator: Callable[[GitStore, Release], PublicResult],
                 clock: Callable[[], float] = time.time):
        from .cumulative_process_evidence_v1 import ProcessEvidence
        require(type(plan) is StudyPlan, "Exact V2 prospective plan required")
        plan.__post_init__()
        require(mode in ("fixture", "live") and set(workers) == {"mini", "strong"}, "Explicit worker/mode roster required")
        require(all(type(x) is OpenAIWorker for x in workers.values()), "Actual financial worker adapter required")
        plan.verify_sources(repository)
        require(ledger_identity(existing_ledger_path) == expected_ledger_identity
                and records.read("ledger") == expected_ledger_identity,
                "Original cumulative ledger identity differs before runtime admission")
        self.ledger, self.expected_ledger_identity = existing_ledger_path, dict(expected_ledger_identity)
        require(plan.runtime.get("python_executable") == sys.executable
                and plan.runtime.get("python_version") == platform.python_version()
                and plan.runtime.get("platform") == platform.platform(), "Actual role runtime differs from prospective identity")
        require(type(evaluator) is DockerPublicChecks and evaluator.image == plan.runtime.get("image")
                and evaluator.timeout == plan.runtime.get("public_timeout_seconds"), "Public runtime differs from prospective identity")
        require(type(plan.runtime.get("terminal_drain_seconds")) in (int, float)
                and 0 < plan.runtime["terminal_drain_seconds"] <= 300, "Explicit bounded terminal drain required")
        require(plan.cohort.model_profiles_sha256 == digest({k:profile_manifest(v) for k,v in workers.items()}),
                "Worker profiles differ from prospective cohort")
        self.plan, self.index, self.records, self.deadline = plan, index, records, deadline
        self.root, self.repository, self.clock = root.resolve(), repository.resolve(), clock
        self.child = plan.roster.children[index]
        self.trajectory = plan.cohort.trajectories[index]
        self.key = "runtime." + self.child.trajectory
        self.root.mkdir(parents=True, exist_ok=True)
        self.closed = False
        self.nodes: list[MeshNode] = []
        self.streams: list[Any] = []
        self.processes: dict[str, subprocess.Popen[bytes]] = {}
        self.generations: dict[str, int] = dict.fromkeys(self.child.actors, 0)
        self.evaluator, self.mode = evaluator, mode
        self.protected = self._initialize_source()
        self.bridge = ProcessEvidence(plan.roster, self.child.cohort, records.chain, records.chain.commitment,
                                      protected_store=self.protected)
        self.finance: CumulativeAuthorityV5 | None = None
        self.server: FinancialServer | None = None
        self.server_thread: threading.Thread | None = None
        self.payloads: MeshFinancePayloads | None = None
        self.observed_exits: set[tuple[str, int]] = set()
        self.restarted = False
        self.partition_until: float | None = None
        self.partitioned = False
        self.partition_publication_pending = False
        try:
            state = records.read(self.key + ".transport")
            if state is None:
                state = records.put(self.key + ".transport", {"transport_key": secrets.token_hex(32),
                    "observer_key": secrets.token_hex(32),
                    "capabilities": {a:secrets.token_hex(32) for a in self.child.actors}})
            self.transport = state
            roster = (*self.child.actors, "seed", "finance")
            def mesh(actor: str, directory: Path) -> MeshConfig:
                return MeshConfig(directory, actor, self.child.cohort, plan.sha256, roster,
                    state["transport_key"], observer_key=state["observer_key"], interval=.05, fanout=2,
                    limits=MeshLimits(max_payloads=TRANSPORT_CONTRACT["max_payloads"],
                        max_events=TRANSPORT_CONTRACT["max_events"],
                        max_reserved_bytes=TRANSPORT_CONTRACT["max_reserved_bytes"]))
            self.seed = MeshNode(mesh("seed", self.root / "seed-mesh"))
            self.nodes.append(self.seed)
            finance_node = MeshNode(mesh("finance", self.root / "finance-mesh"))
            self.nodes.append(finance_node)
            seed_ready, finance_ready = self.seed.start(), finance_node.start()
            self._require_original_ledger()
            contract = self._financial_contract(existing_ledger_path, mode)
            original_permit = records.read(self.key + ".permit")
            if original_permit is None:
                original_permit = permit_provider(contract)
                records.put(self.key + ".permit", original_permit)
            require(original_permit["mode"] == mode, "Permit mode differs")
            design = original_permit["execution_design"]
            require(digest(checked_policy(design.get("financial_closure_policy"))) == digest(plan.financial_closure_policy)
                    and digest(design.get("runtime")) == digest(plan.runtime),
                    "Financial permit differs from prospective closure policy/runtime")
            records.put(self.key + ".financial-contract", contract)
            self.payloads = MeshFinancePayloads(self.root / "financial-payloads", finance_node, self.child.actors)
            self._require_original_ledger()
            self.finance = CumulativeAuthorityV5.open(existing_ledger_path, self.root / "financial-service", contract,
                original_permit["incremental_cap_micro_usd"], original_permit["expected_global_cap"],
                original_permit["expected_opening_usage"], payloads=self.payloads, workers=workers,
                permit=original_permit, expected_permit_sha256=digest(original_permit),
                max_workers=plan.executor_slots, mode=mode,
                recovery=records.read(self.key + ".financial-config") is not None,
                terminal_roster=plan.roster, checkpoint=records.chain, expected_checkpoint=records.chain.commitment)
            checked_financial_config(self.finance.config)
            records.put(self.key + ".financial-config", self.finance.config)
            rpc = FinancialRPCV5(self.finance, state["capabilities"],
                expected_financial_config_sha256=self.finance.config_sha256,
                request_guard=self._request_guard, request_guard_sha256=digest({"payload_guard": self.payloads.request_guard_sha256,
                    "deadline": deadline, "protocol": PROTOCOL}))
            old_port = records.read(self.key + ".finance-port")
            self.server = FinancialServer(old_port["port"] if old_port else 0, rpc)
            finance_port = self.server.server_address[1]
            records.put(self.key + ".finance-port", {"port": finance_port})
            self.server_thread = threading.Thread(target=self.server.serve_forever,
                kwargs={"poll_interval": .05}, daemon=True)
            self.server_thread.start()
            self.role_paths: dict[str, Path] = {}
            for actor in self.child.actors:
                config = {"protocol": ROLE_PROTOCOL, "actor": actor, "trajectory_id": self.child.trajectory,
                    "placement": self.trajectory.decision_placement,
                    "mesh": plain({**asdict(mesh(actor, self.root / "roles" / actor / "mesh")),
                                   "root": str(self.root / "roles" / actor / "mesh")}),
                    "finance": {"port": finance_port, "capability": state["capabilities"][actor], "contract_sha256": plan.sha256},
                    "policy_sha256": plan.cohort.shared_policy_sha256,
                    "call_limit": 4 * plan.source_generations, "max_actions": 4 * plan.source_generations,
                    "requirements_by_milestone": {str(i):r.sha256 for i,r in enumerate(plan.releases,1)},
                    "deadline_unix": deadline,
                    "crash_once": self.trajectory.block == "compound_recovery" and actor.endswith(".B01")}
                path = self.root / "roles" / actor / "config.json"
                _write_exact(path, config)
                records.put(self.key + ".role-config." + actor, config)
                self.role_paths[actor] = path
                self._launch(actor, 0)
            ready = {}
            for actor in self.child.actors:
                self._wait_ready(actor)
                ready[actor] = json.loads(self._ready_path(actor).read_bytes())
                require(ready[actor].get("pid") == self.processes[actor].pid
                        and ready[actor].get("config_sha256") == digest(records.read(self.key + ".role-config." + actor)),
                        "Role readiness differs from exact launched configuration")
                self.bridge.observe_ready(actor, 0)
            self.peers = {**{a:ready[a]["port"] for a in self.child.actors},
                          "seed": seed_ready["port"], "finance": finance_ready["port"]}
            self.seed.configure(self.peers)
            finance_node.configure(self.peers)
            for actor in self.child.actors:
                observer_request(self.peers[actor], actor, state["observer_key"], "configure", {"peers": self.peers})
            records.put(self.key + ".membership", {"peers": self.peers, "mode": "gossip"})
        except BaseException:
            self.close()
            raise

    def _initialize_source(self) -> GitStore:
        path = self.root / "protected.git"
        key = self.key + ".source-initialization"
        require(not path.exists() and not path.is_symlink()
                and self.records.read(key + ".intent") is None,
                "Preexisting Git source or interrupted initialization cannot be adopted")
        self.records.put(key + ".intent", {"path": str(path),
            "initial_files_sha256": digest(self.plan.initial_files)})
        self.records.chain.validate_boundary()
        store = GitStore.create(path, self.plan.initial_files)
        original = _source(store)
        require(original["files"] == self.plan.initial_files, "Actual initial Git bytes differ")
        self.records.put(key + ".result", original)
        self.records.chain.validate_boundary()
        return store

    def _require_original_ledger(self) -> None:
        require(ledger_identity(self.ledger) == self.expected_ledger_identity,
                "Original cumulative ledger identity changed")

    def _request_guard(self, action: Any) -> None:
        if self.clock() >= self.deadline:
            raise FinancialDenied("deadline_exhausted")
        if ledger_identity(self.ledger) != self.expected_ledger_identity:
            raise FinancialDenied("Original cumulative ledger identity changed")
        assert self.payloads is not None
        self.payloads.request_guard(action)

    def _financial_contract(self, ledger: Path, mode: str) -> dict[str, Any]:
        specs = []
        for number, release in enumerate(self.plan.releases, 1):
            context = Context(self.plan.sha256, self.child.cohort, self.child.trajectory, number, release.sha256)
            for generation in range(self.plan.source_generations):
                for actor in self.child.actors:
                    role, package = actor.rsplit(".",1)[-1], package_for(actor)
                    review = role in REVIEWERS
                    specs.append({"context": to_dict(context), "work": to_dict(WorkKey(package, release.milestone, role, generation)),
                        "actors": [actor], "kinds": ["review" if review else "build" if generation == 0 else "repair"],
                        "profiles": ["strong" if review else "mini"],
                        "allowed_paths": ["decision.json"] if review else list(self.plan.package_paths[package]),
                        "max_reserved_units": self.plan.runtime["max_reserved_units"]})
        return {"cohort_id": self.child.cohort, "execution_contract_sha256": self.plan.sha256,
            "ledger_identity": self.expected_ledger_identity, "journal_root": str(self.root / "provider-journals"),
            "transport_identity": FIXTURE_TRANSPORT if mode == "fixture" else LIVE_TRANSPORT, "task_specs": specs}

    def _launch(self, actor: str, generation: int) -> None:
        env = {k:v for k,v in os.environ.items() if k in ("PATH", "HOME", "LANG", "LC_ALL", "TMPDIR")}
        self.records.chain.validate_boundary()
        (self.root / "processes" / actor / str(generation)).mkdir(parents=True, exist_ok=False)
        self.records.put(self.key + ".launcher." + actor + "." + str(generation),
            {"argv": [sys.executable, "-m", "gossip_harness.cumulative_study_role_v1", "--role-config", str(self.role_paths[actor])],
             "cwd": str(self.repository), "environment": env, "actor": actor, "generation": generation})
        process = self.bridge.launch(actor, [sys.executable, "-m", "gossip_harness.cumulative_study_role_v1",
            "--role-config", str(self.role_paths[actor])], cwd=self.repository, env=env,
            stdout_path=self.root / "processes" / actor / str(generation) / "stdout.log", generation=generation)
        self.processes[actor], self.generations[actor] = process, generation

    def _wait_ready(self, actor: str) -> None:
        self._wait(lambda: self._ready_path(actor).exists(), startup=True)

    def _ready_path(self, actor: str) -> Path:
        return self.root / "roles" / actor / ("ready-" + str(self.processes[actor].pid) + ".json")

    def _tick(self, startup: bool = False) -> None:
        if self.clock() >= self.deadline:
            raise StudyStop("deadline_exhausted")
        if (self.partition_until is not None and self.clock() >= self.partition_until
                and not self.partition_publication_pending):
            self._partition(False)
        for actor, process in tuple(self.processes.items()):
            code = process.poll()
            if code is None:
                continue
            if (not startup and code == 86 and actor.endswith(".B01") and
                    self.trajectory.block == "compound_recovery" and not self.restarted):
                marker = self.root / "roles" / actor / "restart-fault.json"
                require(marker.is_file(), "Restart exited without original known-terminal marker")
                self._verify_restart_marker(actor, process, marker)
                self.records.put(self.key + ".restart-original", json.loads(marker.read_bytes()))
                self.bridge.observe_exit(actor, self.generations[actor], timeout=1)
                self.observed_exits.add((actor, self.generations[actor]))
                self.restarted = True
                self._launch(actor, 1)
                self._wait(lambda: self._ready_path(actor).exists(), startup=True)
                self.bridge.observe_ready(actor, 1)
                # Original MeshStore membership fixes the same port on restart.
                require(json.loads(self._ready_path(actor).read_bytes())["port"] == self.peers[actor]
                        and json.loads(self._ready_path(actor).read_bytes())["config_sha256"] ==
                            digest(self.records.read(self.key + ".role-config." + actor)),
                        "Restart changed immutable gossip membership")
                self.records.put(self.key + ".restart-completed", {"actor": actor, "generation": 1})
            else:
                raise StudyStop("role_process_exited:" + actor + ":" + str(code))

    def _verify_restart_marker(self, actor: str, process: subprocess.Popen[bytes], marker: Path) -> None:
        value = json.loads(marker.read_bytes())
        config = self.records.read(self.key + ".role-config." + actor)
        require(value.get("protocol") == ROLE_PROTOCOL and value.get("actor") == actor
                and value.get("pid") == process.pid and value.get("config_sha256") == digest(config)
                and value.get("fault") == "known_result_before_publication_builder_restart",
                "Restart marker identity differs")
        registered = self.records.read(self.key + ".directive." + value["directive_id"])
        require(registered is not None and registered["actor"] == actor, "Restart directive was never offered")
        assert registered is not None
        binding = from_dict(DispatchBinding, value["reply"]["binding"])
        require(binding.action.actor == actor and binding.action.context.milestone == 1
                and binding.action.work.generation == 0 and binding.action.kind == "build", "Restart boundary differs")
        assert self.finance is not None
        proof = self.finance.verified_terminal(binding)
        require(to_dict(proof["reply"]) == value["reply"], "Restart has no original known completion")
        path = self.root / "roles" / actor / "journal" / "role.sqlite"
        with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as db:
            row = db.execute("SELECT body FROM actions WHERE id=?", (value["directive_id"],)).fetchone()
            require(row is not None, "Restart action journal absent")
            action = json.loads(row[0])
            require(action["state"] == "terminal" and action["reply"] == value["reply"]
                    and action["publication_ref"] is None and action["directive"] == registered["directive"],
                    "Restart was not before publication of the exact completed action")

    def _wait(self, predicate: Callable[[], bool], *, startup: bool = False) -> None:
        while True:
            self._tick(startup)
            if predicate():
                return
            time.sleep(.025)

    def _partition(self, enabled: bool) -> None:
        target = next(a for a in self.child.actors if a.endswith(".B01"))
        prefix = self.key + (".partition-start" if enabled else ".partition-end")
        def apply() -> dict[str, Any]:
            receipts = []
            for actor, port in self.peers.items():
                blocked = [p for p in self.peers if p != target] if actor == target else [target]
                response = observer_request(port, actor, self.transport["observer_key"], "block",
                                            {"peers": blocked if enabled else []})
                require(response == {"blocked": sorted(blocked) if enabled else []},
                        "Partition acknowledgement differs from exact prescribed edges")
                receipts.append({"actor": actor, "response": response})
            return {"enabled": enabled, "target": target, "receipts": receipts, "observed_at": self.clock()}
        result = self.records.effect(prefix, {"enabled": enabled, "target": target}, apply)
        self.partition_until = result["observed_at"] + self.plan.partition_seconds if enabled else None
        self.partitioned = True

    def source(self) -> dict[str, Any]:
        return _source(self.protected)

    def release(self, release: Release) -> dict[str, Any]:
        key = self.key + ".release." + release.milestone
        old = self.records.read(key)
        if old is not None:
            require(old["release_sha256"] == release.sha256 and self.protected.is_ancestor(old["commit_oid"], self.protected.head()),
                    "Original cumulative release disappeared")
            return old
        source = self.source()
        for name, value in release.files.items():
            require(name not in source["files"] or source["files"][name] == value, "Released fixture cannot rewrite prior source")
        return self._git_effect(key, {p:v for p,v in release.files.items()}, tuple(release.files), {"release_sha256": release.sha256})

    def _git_effect(self, key: str, changes: dict[str, str | None], paths: tuple[str, ...], extra: dict[str, Any]) -> dict[str, Any]:
        done = self.records.read(key)
        if done is not None:
            require(self.protected.is_ancestor(done["commit_oid"], self.protected.head()), "Retained Git effect is no longer inherited")
            return done
        intent = self.records.read(key + ".git-intent")
        if intent is None:
            old = self.protected.head()
            if self.records.read(key + ".proposal-intent") is not None:
                raise StudyUnknown("Git proposal result missing: " + key)
            self.records.put(key + ".proposal-intent", {"old": old, "changes_sha256": digest(changes)})
            offered = self.protected.propose(changes, old, message=key)
            staged = self.protected.prepare(self.protected, offered, old, lambda _p:(True,"Mechanically staged; public/independent tests separate"), paths or None)
            require(staged.status in ("prepared", "noop"), "Git integration failed: " + staged.status)
            intent = self.records.put(key + ".git-intent", {"candidate": asdict(staged), **extra})
        else:
            require(all(intent.get(k) == v for k,v in extra.items()), "Git effect binding differs")
            staged = Candidate(**{**intent["candidate"], "changed_paths": tuple(intent["candidate"]["changed_paths"])})
        head = self.protected.head()
        require(head in (staged.old_head, staged.candidate_sha), "Git CAS crossed an unknown successor")
        if head == staged.old_head:
            self.records.chain.validate_boundary()
            result = self.protected.accept(staged)
            require(result.status in ("accepted", "noop"), "Protected Git CAS failed")
        self.records.chain.validate_boundary()
        return self.records.put(key, {"commit_oid": self.protected.head(), "source_sha256": self.source()["source_sha256"], **extra})

    def publish(self, kind: str, value: dict[str, Any], key: str) -> EvidenceRef:
        raw = canonical_payload(plain(value))
        request = {"kind": kind, "payload_sha256": hashlib.sha256(raw).hexdigest(), "command": key}
        old = self.records.read("publish." + key)
        self.records.put("publish." + key + ".intent", request)
        self.records.chain.validate_boundary()
        ref = self.seed.publish(kind, raw, hashlib.sha256(key.encode()).hexdigest())
        value_record = {"ref": to_dict(ref), "payload": plain(value), **request}
        require(old is None or old == value_record, "Mesh exact replay changed")
        self.records.put("publish." + key, value_record)
        return ref

    def work(self, stage: str, directives: tuple[tuple[str, WorkDirective], ...]) -> tuple[dict[str, Any], ...]:
        self.plan.verify_sources(self.repository)
        self._tick()
        self.records.chain.validate_boundary()
        directives = tuple(sorted(directives, key=lambda x: digest({"seed": self.trajectory.block_seed_sha256, "actor": x[0]})))
        for actor, directive in directives:
            self.records.put(self.key + ".directive." + directive_id(directive), {"actor": actor, "directive": directive.to_dict()})
        context = directives[0][1].context
        require(all(d.context == context for _,d in directives), "Work contexts differ")
        groups: dict[str, list[tuple[str, WorkDirective]]] = {}
        for actor, directive in directives:
            scope = digest({"source": to_dict(directive.source_ref), "evidence": [to_dict(x) for x in directive.evidence_refs]})
            groups.setdefault(scope, []).append((actor,directive))
        initial_fault_frontier = (context.milestone == 1 and directives[0][1].work.generation == 0
            and stage.endswith(".build") and self.trajectory.block == "compound_recovery")
        publication_keys = []
        if initial_fault_frontier:
            for scope, members in groups.items():
                group_stage = stage if len(groups) == 1 else stage + "." + scope[:12]
                publication_keys.append(group_stage + ".work")
                if self.trajectory.decision_placement == "durable_central_scheduler":
                    for actor, _directive in members:
                        publication_keys.extend((group_stage + ".decision." + actor, group_stage + ".assignment." + actor))
            # Keep the initial frontier under the same partition even if its
            # durable publication takes longer than the nominal minimum time.
            # Deadline checks still stop the stage; uncertainty never rearms it.
            require(not self.partitioned and self.records.read(self.key + ".partition-start.intent") is None,
                    "Initial fault frontier cannot be retried or rearmed")
            require(all(self.records.read("publish." + key + suffix) is None
                        for key in publication_keys for suffix in (".intent", "")),
                    "Initial work publication predates prescribed partition")
            self.partition_publication_pending = True
            self._partition(True)
            start_name = self.records.name(self.key + ".partition-start.result")
            self.records.put(self.key + ".partition-frontier", {"stage": stage,
                "partition_start": {"name": start_name,
                    "sha256": hashlib.sha256(self.records.chain.read(start_name)).hexdigest(),
                    "position": self.records.chain.position(start_name)},
                "publication_keys": publication_keys})
            self.records.chain.validate_boundary()
        result_stages = {}
        for scope, members in groups.items():
            first = members[0][1]
            group_stage = stage if len(groups) == 1 else stage + "." + scope[:12]
            for actor, _directive in members:
                result_stages[actor] = group_stage
            envelope = {"protocol": ROLE_PROTOCOL, "stage_id": group_stage, "context": to_dict(context),
                "source_ref": to_dict(first.source_ref), "evidence_refs": [to_dict(x) for x in first.evidence_refs],
                "directives": [{"actor": a, "directive": d.to_dict()} for a,d in members],
                "placement": self.trajectory.decision_placement}
            ref = self.publish("cumulative-work", envelope, group_stage + ".work")
            if initial_fault_frontier and group_stage + ".work" == publication_keys[0]:
                self.records.put(self.key + ".partition-first-publication", {
                    "boundary": "first-work-publication-durably-acknowledged",
                    "publication_key": publication_keys[0], "ref": to_dict(ref), "observed_at": self.clock()})
            if self.trajectory.decision_placement == "durable_central_scheduler":
                # Actual central durable admission; S only exposes the same
                # public work frontier and waits for each role-local decision.
                for actor, directive in members:
                    self._tick()
                    decision = {"protocol": ROLE_PROTOCOL, "stage_id": group_stage, "actor": actor,
                        "work_ref": to_dict(ref), "directive_sha256": directive_id(directive)}
                    self.records.put(group_stage + ".central-decision." + actor, decision)
                    decision_ref = self.publish("cumulative-central-decision", decision, group_stage + ".decision." + actor)
                    self.publish("cumulative-assignment", {"protocol": ROLE_PROTOCOL, "stage_id": group_stage,
                        "actor": actor, "directive": directive.to_dict(), "work_ref": to_dict(ref),
                        "central_decision_ref": to_dict(decision_ref)}, group_stage + ".assignment." + actor)
        if initial_fault_frontier:
            frontier_position = self.records.chain.position(self.records.name(self.key + ".partition-frontier"))
            require(all(self.records.chain.position(self.records.name("publish." + key)) > frontier_position
                        for key in publication_keys), "Publication precedes original partition frontier")
            self.records.put(self.key + ".partition-frontier-complete", {"stage": stage,
                "publications": [{"name": self.records.name("publish." + key),
                    "sha256": hashlib.sha256(self.records.chain.read(self.records.name("publish." + key))).hexdigest(),
                    "position": self.records.chain.position(self.records.name("publish." + key))}
                    for key in publication_keys]})
            self.partition_publication_pending = False
        expected = {a:directive_id(d) for a,d in directives}
        found: dict[str, dict[str, Any]] = {}
        def collect() -> bool:
            for item in self.seed.arrived():
                if item.kind != "cumulative-result" or item.producer not in expected:
                    continue
                raw = self.seed.resolve(item)
                if raw is None:
                    self.seed.want(item)
                    continue
                value = strict_loads(resolve_local(item, self.seed.arrived(), raw))
                if value.get("stage_id") != result_stages[item.producer]:
                    continue
                require(value["actor"] == item.producer and value["directive_id"] == expected[item.producer],
                        "Role result rebound to another stage")
                self._verify_result(value)
                record = {**value, "original_ref": to_dict(item)}
                require(item.producer not in found or found[item.producer] == record, "Conflicting role results")
                found[item.producer] = record
            return len(found) == len(expected)
        self._wait(collect)
        self.plan.verify_sources(self.repository)
        self.records.chain.validate_boundary()
        return tuple(found[a] for a,_ in directives)

    def _verify_result(self, value: dict[str, Any]) -> None:
        registered = self.records.read(self.key + ".directive." + value["directive_id"])
        require(registered is not None and registered["actor"] == value["actor"], "Result lacks exact original directive")
        assert registered is not None
        directive = WorkDirective.from_dict(registered["directive"])
        snapshot = value["snapshot"]
        require(snapshot["state"] in ("published", "stopped"), "Nonterminal role result")
        reply = snapshot.get("reply")
        if reply is None or reply["state"] not in ("completed", "failed"):
            return  # Complete negative/unknown evidence retained, not an eligible proposal.
        require(self.finance is not None, "Financial owner missing")
        binding = from_dict(DispatchBinding, reply["binding"])
        assert self.finance is not None
        proof = (self.finance.verified_terminal(binding) if reply["state"] == "completed"
                 else self.finance.verified_known_failure(binding))
        require(binding.action.actor == value["actor"] and binding.action.context == directive.context
                and binding.action.work == directive.work and binding.action.kind == directive.kind
                and binding.action.profile_id == directive.profile_id
                and digest(snapshot["action"]) == digest(to_dict(binding.action))
                and digest(reply) == digest(to_dict(proof["reply"])),
                "Result is a different known terminal action")
        materialized = materialize(directive, self.seed, self.plan.cohort.shared_policy_sha256)
        require(materialized is not None, "Original complete local view unavailable")
        assert materialized is not None
        request, view = materialized
        require(digest(snapshot["view"]) == digest(to_dict(view)) and proof["worker_request"] == request
                and binding.action.view_manifest_sha256 == identity(view), "Original local materialization differs")
        require(digest(plain(asdict(proof["worker_request"]))) == digest(value["worker_request"])
                and digest(proof["result_payload"]) == digest(value["result_payload"])
                and binding.normalized_worker_request_sha256 == worker_request_digest(proof["worker_request"]),
                "Role result differs from original finance request/result")

    def integrate(self, stage: str, builds: tuple[dict[str, Any], ...], reviews: tuple[dict[str, Any], ...]) -> dict[str, Any]:
        for value in (*builds, *reviews):
            self._verify_result(value)
        selected_builds = []
        all_changes: dict[str, str | None] = {}
        selections = []
        for package in PACKAGES:
            candidates = {x["actor"]:x for x in builds if package_for(x["actor"]) == package
                          and x["snapshot"].get("reply") is not None and x["snapshot"]["reply"]["state"] == "completed"}
            decisions = [x for x in reviews if package_for(x["actor"]) == package]
            require(len(decisions) == 1, "Exactly one scoped reviewer required")
            review = decisions[0]
            try:
                require(review["snapshot"]["reply"]["state"] == "completed", "Reviewer unavailable")
                proposal = review["result_payload"]["payload"]
                decision = strict_loads(proposal["changes"]["decision.json"].encode())
                require(set(decision) == {"stage_id", "package", "selected_actor", "reasons", "tests"}
                        and decision["stage_id"] == stage and decision["package"] == package
                        and type(decision["tests"]) is list and type(decision["reasons"]) is str,
                        "Reviewer selection format differs")
                selected = candidates[decision["selected_actor"]]
                changes = selected["result_payload"]["payload"]["changes"]
                require(type(changes) is dict and all(type(p) is str and any(p == root.rstrip("/") or p.startswith(root.rstrip("/") + "/")
                    for root in self.plan.package_paths[package]) for p in changes), "Builder exceeded package ownership")
                require(not set(changes) & set(all_changes), "Selected proposals overlap")
                all_changes.update(changes)
                selected_builds.append(selected)
                selections.append({"package": package, "reviewer": review["actor"], "selected_actor": selected["actor"],
                    "proposal_ref": selected["original_ref"], "review_ref": review["original_ref"], "decision": decision})
            except (KeyError, TypeError, ValueError) as error:
                return {"status": "selection_rejected", "package": package, "reason": str(error),
                        "selections": selections, "acceptance_authority": False}
        # The same deterministic mechanical merge obeys all four local reviewer
        # choices; it does not independently select a preferred proposal.
        merged = []
        for selected in selected_builds:
            package = package_for(selected["actor"])
            merged.append(self._merge_private(stage, selected, package))
        return {"status": "integrated", "commit_oid": self.protected.head(),
            "source_sha256": self.source()["source_sha256"], "selections": selections,
            "private_git_merges": merged, "acceptance_authority": False}

    def _merge_private(self, stage: str, selected: dict[str, Any], package: str) -> dict[str, Any]:
        key = stage + ".merge." + package
        pin = digest(selected)
        done = self.records.read(key)
        if done is not None:
            require(done["proposal_sha256"] == pin
                    and self.protected.is_ancestor(done["commit_oid"], self.protected.head()),
                    "Private proposal history or identity changed")
            return done
        request = selected["worker_request"]
        require(self.protected.read_files(request["base_sha"]) == request["files"],
                "Candidate base differs from exact original worker source")
        path = self.root / "private-git" / hashlib.sha256((stage + selected["actor"]).encode()).hexdigest()
        proposal = self.records.read(key + ".proposal")
        if proposal is None:
            if self.records.read(key + ".proposal-intent") is not None:
                raise StudyUnknown("Private Git proposal result unavailable: " + key)
            self.records.put(key + ".proposal-intent", {"proposal_sha256": pin,
                "base_sha": request["base_sha"], "git_path": str(path)})
            self.records.chain.validate_boundary()
            private = GitStore.fork(self.protected, path)
            offered = private.propose(selected["result_payload"]["payload"]["changes"],
                                      request["base_sha"], message=stage + " " + selected["actor"])
            proposal = self.records.put(key + ".proposal", {"proposal_sha256": pin,
                "git_path": str(path), "offered_sha": offered, "base_sha": request["base_sha"]})
        require(proposal["proposal_sha256"] == pin, "Private proposal rebound")
        private = GitStore(Path(proposal["git_path"]))
        merge = self.records.read(key + ".git-intent")
        if merge is None:
            old = self.protected.head()
            staged = self.protected.prepare(private, proposal["offered_sha"], old,
                lambda _p: (True, "Mechanical scoped integration; public/independent tests remain separate"),
                allowed_paths=self.plan.package_paths[package])
            require(staged.status in ("prepared", "noop"), "Private Git merge rejected: " + staged.status)
            merge = self.records.put(key + ".git-intent", {"proposal_sha256": pin, "candidate": asdict(staged)})
        require(merge["proposal_sha256"] == pin, "Private merge rebound")
        staged = Candidate(**{**merge["candidate"], "changed_paths": tuple(merge["candidate"]["changed_paths"])})
        head = self.protected.head()
        require(head in (staged.old_head, staged.candidate_sha), "Private merge CAS crossed unknown history")
        self.records.chain.validate_boundary()
        if head == staged.old_head:
            accepted = self.protected.accept(staged)
            require(accepted.status in ("accepted", "noop"), "Private merge CAS failed")
        self.records.chain.validate_boundary()
        return self.records.put(key, {"proposal_sha256": pin, "private_git": proposal,
            "commit_oid": self.protected.head(), "source_sha256": self.source()["source_sha256"]})

    def evaluate(self, release: Release) -> PublicResult:
        self._tick()
        self.records.chain.validate_boundary()
        result = self.evaluator(self.protected, release)
        source = self.source()
        result.validate(release, source["commit_oid"], source["source_sha256"])
        self.records.chain.validate_boundary()
        return result

    def stop_and_seal(self, status: str, milestone_records: tuple[str, ...]) -> ChildTerminalSeal:
        if self.partition_until is not None:
            if status == "completed":
                self._wait(lambda: self.partition_until is None)
            else:
                self._partition(False)
        for actor in self.child.actors:
            path = self.root / "roles" / actor / "stop"
            if not path.exists():
                _write_exact(path, {"reason": "trajectory_terminal"})
        for actor, process in self.processes.items():
            try:
                self.bridge.observe_exit(actor, self.generations[actor], timeout=20)
                self.observed_exits.add((actor, self.generations[actor]))
            except (subprocess.TimeoutExpired, ValueError):
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGTERM)
                raise StudyUnknown("Role stop acknowledgement unavailable: " + actor)
        require(self.finance is not None, "Financial owner missing")
        if status == "completed" and self.trajectory.block == "compound_recovery":
            require(self.restarted and self.partitioned and self.partition_until is None,
                    "Compound trajectory omitted an actual prescribed fault")
        request = self.bridge.finalize(self.protected, status, self.records.chain.commitment)
        self.records.put(self.key + ".terminal-request", {"request": request.record(),
            "milestones": milestone_records, "original_paths": self.original_paths()})
        assert self.finance is not None and self.payloads is not None
        drain_deadline = time.monotonic() + self.plan.runtime["terminal_drain_seconds"]
        while True:
            try:
                require(self.bridge.verified_request(self.records.chain.commitment) == request,
                        "Post-publication process/Git confirmation changed")
                preparation = self.finance.prepare_terminal_snapshot(request)
                break
            except SealBusy:
                if time.monotonic() >= drain_deadline:
                    raise StudyUnknown("Admitted financial work did not become quiescent within terminal drain") from None
                time.sleep(.05)
        seal = self.finance.seal_terminal(preparation)
        financial_config = dict(self.finance.config)
        payload_config = dict(self.payloads.config)
        finance_mesh_identity = self.payloads.mesh.config.identity()
        # Sealing fences admission; close all writer threads before hashing any
        # child-local SQLite/file originals. The shared ledger gets inode+census.
        self.close()
        self.records.put(self.key + ".originals", self._originals(financial_config, payload_config, finance_mesh_identity))
        return seal

    def _originals(self, financial_config: dict[str, Any], payload_config: dict[str, Any],
                   finance_mesh_identity: dict[str, Any]) -> dict[str, Any]:
        require(self.closed, "Original census requires stopped writer lifetime")
        files: list[dict[str, Any]] = []
        database_ids = {}
        total = 0
        for directory in ("provider-journals", "financial-payloads", "seed-mesh", "finance-mesh", "roles", "processes", "private-git", "protected.git"):
            for path in sorted((self.root / directory).rglob("*")):
                require(not path.is_symlink(), "Indirect original artifact")
                if not path.is_file():
                    continue
                stat = path.stat()
                total += stat.st_size
                require(len(files) < 20000 and stat.st_size <= 1073741824 and total <= 4294967296,
                        "Original artifact census exceeded frozen bound")
                hashed = hashlib.sha256()
                with path.open("rb") as stream:
                    for raw in iter(lambda: stream.read(1024 * 1024), b""):
                        hashed.update(raw)
                after = path.stat()
                require((after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) ==
                        (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns), "Original changed while hashing")
                files.append({"path": str(path), "sha256": hashed.hexdigest(), "size": stat.st_size})
                if path.suffix == ".sqlite":
                    database_ids[str(path)] = ledger_identity(path)
        ledger = Path(financial_config["contract"]["ledger_identity"]["path"])
        require(ledger_identity(ledger) == financial_config["contract"]["ledger_identity"], "Original ledger replaced")
        with sqlite3.connect(ledger.as_uri() + "?mode=ro", uri=True) as db:
            bindings = [json.loads(row[0]) for row in db.execute(
                "SELECT binding FROM financial_actions_v2 WHERE cohort=? ORDER BY actor,request_id", (self.child.cohort,))]
        journals = [x for x in files if Path(x["path"]).parent == self.root / "provider-journals" and x["path"].endswith(".json")]
        return {"protocol": PROTOCOL + "-originals", "writer_lifetimes_closed": True,
            "ledger_identity": financial_config["contract"]["ledger_identity"], "cohort": self.child.cohort,
            "config": financial_config, "config_sha256": digest(financial_config),
            "payload_index_identity": ledger_identity(self.root / "financial-payloads" / "payload-index.sqlite"),
            "payload_config": payload_config,
            "mesh_database_identity": ledger_identity(self.root / "finance-mesh" / "mesh.sqlite"),
            "mesh_identity": finance_mesh_identity, "journal_files": journals, "dispatches": bindings,
            "database_identities": database_ids, "complete_child_files": files,
            "original_paths": self.original_paths()}

    def original_paths(self) -> dict[str, Any]:
        return {"runtime_root": str(self.root), "financial_service": str(self.root / "financial-service"),
            "provider_journals": str(self.root / "provider-journals"),
            "financial_payloads": str(self.root / "financial-payloads"),
            "finance_mesh": str(self.root / "finance-mesh"), "seed_mesh": str(self.root / "seed-mesh"),
            "protected_git": str(self.root / "protected.git"),
            "roles": {a:str(self.root / "roles" / a) for a in self.child.actors},
            "processes": str(self.root / "processes")}

    def close(self) -> None:
        if self.closed:
            return
        if getattr(self, "cleanup_attempted", False):
            raise StudyUnknown("Prior owned cleanup is unresolved; originals cannot be frozen")
        self.cleanup_attempted = True
        errors: list[str] = []
        def attempt(name: str, operation: Callable[[], Any]) -> None:
            try:
                operation()
            except BaseException as error:
                errors.append(name + ": " + type(error).__name__ + ": " + str(error))
        # Every owned resource receives its own cleanup attempt even if an earlier
        # one fails. A partially closed runtime can never publish stable originals.
        def stop_process(process: subprocess.Popen[bytes]) -> None:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait(timeout=3)
        for actor, generation, process in self.bridge.handles():
            if (actor, generation) not in self.observed_exits:
                attempt("owned process " + actor + "." + str(generation), lambda: stop_process(process))
        if self.server is not None:
            if self.server_thread is not None and self.server_thread.is_alive():
                attempt("financial server shutdown", self.server.shutdown)
            attempt("financial server close", self.server.server_close)
        server_thread = self.server_thread
        if server_thread is not None and server_thread.ident is not None:
            attempt("financial server join", lambda: server_thread.join(timeout=5))
            if server_thread.is_alive():
                errors.append("Financial server thread did not stop")
        if self.finance is not None:
            attempt("financial executor close", self.finance.close)
        if self.payloads is not None:
            attempt("payload index close", self.payloads.close)
        for index, node in enumerate(reversed(self.nodes)):
            attempt("mesh close " + str(index), node.close)
            if ((node.tick_thread is not None and node.tick_thread.is_alive())
                    or (node.server_thread is not None and node.server_thread.is_alive())):
                errors.append("Gossip writer thread did not stop: " + str(index))
        self.cleanup_errors = tuple(errors)
        self.closed = not errors
        if errors:
            raise StudyUnknown("Owned cleanup unresolved: " + "; ".join(errors))


def run_study(plan: StudyPlan, *, output: Path, repository: Path,
              checkpoint: Any, expected_checkpoint: Any, existing_ledger_path: Path,
              expected_ledger_identity: dict[str, Any], workers: dict[str, OpenAIWorker],
              mode: str, permit_provider: Callable[[dict[str, Any]], dict[str, Any]],
              evaluator: Callable[[GitStore, Release], PublicResult]) -> dict[str, Any]:
    """Explicit whole-study entry; reuses caller-owned ledger/head and workers.

    Output is never removed/reset. The caller provides a full four-release plan,
    actual authorized financial permits and physically runnable public checks.
    Calling this function performs work; import and contract construction do not.
    A missing full acceptance authority always leaves the overall goal unfinished.
    """
    from .cumulative_study_controller_v2 import StudyController
    require(type(plan) is StudyPlan, "Exact V2 prospective plan required")
    plan.__post_init__()
    require(output.is_absolute() and output.resolve() == output, "Canonical isolated study output required")
    def factory(current: StudyPlan, index: int, records: Records, deadline: float) -> GossipChildRuntime:
        return GossipChildRuntime(current, index, records, deadline,
            root=output / current.cohort.trajectories[index].id, repository=repository,
            existing_ledger_path=existing_ledger_path, expected_ledger_identity=expected_ledger_identity,
            workers=workers, mode=mode,
            permit_provider=permit_provider, evaluator=evaluator)
    controller = StudyController(plan, checkpoint=checkpoint, expected_checkpoint=expected_checkpoint,
        repository=repository, existing_ledger_path=existing_ledger_path,
        expected_ledger_identity=expected_ledger_identity, runtime_factory=factory)
    return controller.run()
