"""Durable local/central role decisions over the common authenticated mesh.

This driver executes no candidate source. Finance owns every provider call;
the parent independently verifies returned bindings and performs Git integration.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
import hashlib
import math
import os
from pathlib import Path
import sqlite3
import tempfile
import time
from typing import Any

from .peer_financial_rpc_v2 import FinancialClient
from .peer_mesh_v2 import MeshConfig, MeshNode
from .peer_project_contract_v2 import (
    Context, EvidenceRef, from_dict, identifier, resolve_local, sha256, to_dict,
)
from .peer_role_loop_v2 import RoleLoop, WorkDirective, directive_id, materialize
from .peer_store_v1 import canonical_bytes as _canonical_bytes, strict_loads as _strict_loads

PROTOCOL = "cumulative-study-role-v1"
PLACEMENTS = ("peer_local", "durable_central_scheduler")


class RoleDeadline(ValueError):
    """The frozen horizon forbids further role admission."""


def canonical_bytes(value: Any) -> bytes:
    def plain(item: Any) -> Any:
        if type(item) in (tuple, list):
            return [plain(part) for part in item]
        return {key: plain(part) for key, part in item.items()} if type(item) is dict else item
    return _canonical_bytes(plain(value), max_bytes=8_388_608)


def strict_loads(raw: bytes) -> Any:
    return _strict_loads(raw, max_bytes=8_388_608)


def require(condition: bool, reason: str) -> None:
    if not condition:
        raise ValueError(reason)


def closed(value: Any, fields: str) -> dict[str, Any]:
    require(type(value) is dict and set(value) == set(fields.split()), "Closed fields differ")
    return value


def immutable_write(path: Path, value: Any) -> None:
    """Publish complete bytes without replacing a retained artifact."""
    raw = canonical_bytes(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        require(not path.is_symlink() and path.read_bytes() == raw, "Immutable artifact changed")
        return
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".publishing-", delete=False) as handle:
        temporary = Path(handle.name)
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    try:
        try:
            os.link(temporary, path)
        except FileExistsError:
            require(path.read_bytes() == raw, "Concurrent immutable artifact changed")
        descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        temporary.unlink()


def checked_config(config: dict[str, Any]) -> MeshConfig:
    closed(config, "protocol actor trajectory_id placement mesh finance policy_sha256 call_limit max_actions requirements_by_milestone crash_once deadline_unix")
    require(config["protocol"] == PROTOCOL and config["placement"] in PLACEMENTS, "Unknown role protocol/placement")
    identifier(config["actor"])
    identifier(config["trajectory_id"])
    sha256(config["policy_sha256"])
    require(type(config["crash_once"]) is bool, "Crash policy must be boolean")
    require(type(config["deadline_unix"]) in (int, float) and math.isfinite(config["deadline_unix"])
            and config["deadline_unix"] > 0, "Invalid role deadline")
    requirements = closed(config["requirements_by_milestone"], "1 2 3 4")
    for digest in requirements.values():
        sha256(digest)
    finance = closed(config["finance"], "port capability contract_sha256")
    sha256(finance["contract_sha256"])
    require(type(finance["port"]) is int and 1 <= finance["port"] <= 65535,
            "Invalid finance endpoint")
    require(type(finance["capability"]) is str and 32 <= len(finance["capability"]) <= 128,
            "Invalid finance capability")
    require(type(config["call_limit"]) is int and type(config["max_actions"]) is int
            and 1 <= config["call_limit"] <= config["max_actions"] <= 4096, "Invalid action bounds")
    mesh = MeshConfig.from_dict(config["mesh"])
    require(mesh.node_id == config["actor"] and {"seed", "finance"} <= set(mesh.roster)
            and mesh.node_id not in ("seed", "finance")
            and mesh.execution_contract_sha256 == finance["contract_sha256"], "Role domain differs")
    return mesh


class RoleDriver:
    """The injected mesh/finance seam permits offline decision verification."""
    def __init__(self, root: Path, config: dict[str, Any], mesh: Any, finance: Any, *, publish_role_event: Any = None):
        self.root, self.config, self.mesh = Path(root), config, mesh
        self.publish_role_event = publish_role_event
        self.mesh_config = checked_config(config)
        self.deadline_unix = config["deadline_unix"]
        self.actor, self.placement = config["actor"], config["placement"]
        self.config_raw = canonical_bytes(config)
        self.config_sha256 = hashlib.sha256(self.config_raw).hexdigest()
        self.root.mkdir(parents=True, exist_ok=True)
        retained = (self.root / "journal").exists()
        database = self.root / "decisions.sqlite"
        require(not retained or database.is_file(), "Retained role lost decision journal")
        require(not database.exists() or retained, "Orphan decision journal")
        require(not any(Path(str(database) + suffix).is_symlink() for suffix in ("", "-wal", "-shm")), "Symlink decision journal")
        self.loop = RoleLoop(self.root / "journal", self.actor, mesh, finance,
            call_limit=config["call_limit"], max_actions=config["max_actions"],
            policy_sha256=config["policy_sha256"], result_producer="finance", lease_ttl=180,
            crash_hook=self._crash)
        try:
            self.db = sqlite3.connect(database)
            self.db.execute("PRAGMA synchronous=FULL")
            self.db.execute("PRAGMA journal_mode=WAL")
            with self.db:
                if not retained:
                    self.db.execute("CREATE TABLE config (id INTEGER PRIMARY KEY, raw BLOB NOT NULL, count INTEGER NOT NULL)")
                    self.db.execute("CREATE TABLE decisions (stage TEXT PRIMARY KEY, directive TEXT UNIQUE NOT NULL, raw BLOB NOT NULL)")
                    self.db.execute("INSERT INTO config VALUES (1,?,0)", (self.config_raw,))
            self._audit()
            for stage, _, raw in self.db.execute("SELECT stage,directive,raw FROM decisions ORDER BY rowid").fetchall():
                value = strict_loads(raw)
                try:
                    self._record(stage, from_dict(EvidenceRef, value["work_ref"]),
                        WorkDirective.from_dict(value["directive"]), tuple(value["ready_directive_ids"]),
                        from_dict(EvidenceRef, value["central_decision_ref"]) if value["central_decision_ref"] else None)
                except RoleDeadline:
                    break  # Retain original decisions without admitting new actions.
        except BaseException:
            if hasattr(self, "db"):
                self.db.close()
            self.loop.close()
            raise

    def _audit(self) -> None:
        rows = self.db.execute("SELECT id,raw,count FROM config").fetchall()
        require(len(rows) == 1 and rows[0][:2] == (1, self.config_raw)
                and rows[0][2] == self.db.execute("SELECT COUNT(*) FROM decisions").fetchone()[0],
                "Frozen role config or decision membership differs")
        for stage, key, raw in self.db.execute("SELECT stage,directive,raw FROM decisions"):
            value = strict_loads(raw)
            require(value["stage_id"] == stage and value["actor"] == self.actor
                    and value["placement"] == self.placement and value["protocol"] == PROTOCOL
                    and directive_id(WorkDirective.from_dict(value["directive"])) == key, "Decision identity differs")

    def close(self) -> None:
        self.db.close()
        self.loop.close()

    def final_event(self, outcome: str) -> None:
        snapshots = self.loop.snapshots()
        unqueued = tuple(key for (key,) in self.db.execute("SELECT directive FROM decisions ORDER BY rowid")
                         if key not in {item.directive_id for item in snapshots})
        if self.publish_role_event is not None:
            self.publish_role_event("final", outcome=outcome,
                completed_action_ids=tuple(item.action.request_id for item in snapshots
                    if item.action is not None and item.state == "published"),
                remaining_action_ids=tuple(item.action.request_id if item.action else item.directive_id for item in snapshots
                    if item.state != "published") + unqueued)

    def _before_admission(self) -> None:
        if time.time() >= self.deadline_unix:
            raise RoleDeadline("Role deadline exhausted; no new admission")

    def _body(self, ref: EvidenceRef) -> bytes | None:
        arrived = self.mesh.arrived()
        if ref not in arrived or self.mesh.resolve(ref) is None:
            self.mesh.want(ref)
            return None
        return resolve_local(ref, arrived, self.mesh.resolve(ref))

    def _context(self, value: Any) -> Context:
        context = from_dict(Context, value)
        require(context.execution_contract_sha256 == self.mesh_config.execution_contract_sha256
                and context.cohort_id == self.mesh_config.cohort_id
                and context.trajectory_id == self.config["trajectory_id"]
                and str(context.milestone) in self.config["requirements_by_milestone"]
                and context.requirements_sha256 == self.config["requirements_by_milestone"][str(context.milestone)],
                "Work outside frozen role context")
        return context

    def _work(self, ref: EvidenceRef, work: Any) -> tuple[WorkDirective, ...]:
        require(ref.producer == "seed" and ref.kind == "cumulative-work", "Work needs seed authority")
        raw = self._body(ref)
        require(raw is not None and strict_loads(raw) == work, "Work differs from arrived bytes")
        closed(work, "protocol stage_id context source_ref evidence_refs directives placement")
        require(work["protocol"] == PROTOCOL and work["placement"] == self.placement, "Work placement differs")
        identifier(work["stage_id"])
        context = self._context(work["context"])
        require(type(work["directives"]) is list and 0 < len(work["directives"]) <= 64, "Invalid work roster")
        mine, keys = [], set()
        for item in work["directives"]:
            closed(item, "actor directive")
            require(item["actor"] in self.mesh_config.roster and item["actor"] not in ("seed", "finance"), "Unknown work actor")
            directive = WorkDirective.from_dict(item["directive"])
            key = directive_id(directive)
            require(key not in keys and directive.context == context
                    and to_dict(directive.source_ref) == work["source_ref"]
                    and [to_dict(part) for part in directive.evidence_refs] == work["evidence_refs"],
                    "Work/directive binding differs")
            keys.add(key)
            if item["actor"] == self.actor:
                mine.append(directive)
        return tuple(sorted(mine, key=directive_id))

    def _record(self, stage: str, work_ref: EvidenceRef, directive: WorkDirective,
                ready: tuple[str, ...], central: EvidenceRef | None = None) -> str:
        self._before_admission()
        key = directive_id(directive)
        value = {"protocol": PROTOCOL, "stage_id": stage, "actor": self.actor, "placement": self.placement,
                 "work_ref": to_dict(work_ref), "directive": directive.to_dict(),
                 "ready_directive_ids": list(ready), "central_decision_ref": to_dict(central) if central else None}
        raw = canonical_bytes(value)
        self._audit()
        existing = self.db.execute("SELECT raw FROM decisions WHERE stage=?", (stage,)).fetchone()
        if existing:
            prior = strict_loads(existing[0])
            require(prior["work_ref"] == value["work_ref"] and prior["directive"] == value["directive"]
                    and prior["central_decision_ref"] == value["central_decision_ref"], "Durable decision rebound")
            raw = existing[0]
        else:
            with self.db:
                self.db.execute("INSERT INTO decisions VALUES (?,?,?)", (stage, key, raw))
                self.db.execute("UPDATE config SET count=count+1 WHERE id=1")
        self._before_admission()
        immutable_write(self.root / "decisions" / (stage + ".json"), strict_loads(raw))
        self._before_admission()
        self.mesh.publish("cumulative-decision", raw, "cumulative-decision-" + key)
        self._before_admission()
        self.loop.enqueue(directive)
        return key

    def choose_local(self, work_ref: EvidenceRef, work: Any) -> str | None:
        require(self.placement == "peer_local", "Central role cannot choose locally")
        directives = self._work(work_ref, work)
        existing = self.db.execute("SELECT directive,raw FROM decisions WHERE stage=?", (work["stage_id"],)).fetchone()
        if existing:
            prior = strict_loads(existing[1])
            require(prior["work_ref"] == to_dict(work_ref), "Stage work rebound")
            return self._record(work["stage_id"], work_ref, WorkDirective.from_dict(prior["directive"]),
                                tuple(prior["ready_directive_ids"]))
        ready = tuple(d for d in directives if materialize(d, self.mesh, self.config["policy_sha256"]) is not None)
        return self._record(work["stage_id"], work_ref, ready[0], tuple(map(directive_id, ready))) if ready else None

    def accept_assignment(self, ref: EvidenceRef, assignment: Any) -> str | None:
        require(self.placement == "durable_central_scheduler", "Peer-local role cannot accept assignment")
        require(ref.producer == "seed" and ref.kind == "cumulative-assignment", "Assignment needs seed authority")
        raw = self._body(ref)
        require(raw is not None and strict_loads(raw) == assignment, "Assignment differs from arrived bytes")
        closed(assignment, "protocol stage_id actor directive work_ref central_decision_ref")
        require(assignment["protocol"] == PROTOCOL, "Assignment protocol differs")
        if assignment["actor"] != self.actor:
            return None
        work_ref = from_dict(EvidenceRef, assignment["work_ref"])
        decision_ref = from_dict(EvidenceRef, assignment["central_decision_ref"])
        require(decision_ref.producer == "seed" and decision_ref.kind == "cumulative-central-decision", "Decision needs seed authority")
        work_raw, decision_raw = self._body(work_ref), self._body(decision_ref)
        if work_raw is None or decision_raw is None:
            return None
        work = strict_loads(work_raw)
        directives = self._work(work_ref, work)
        directive = WorkDirective.from_dict(assignment["directive"])
        require(assignment["stage_id"] == work["stage_id"] and directive in directives, "Assignment differs from offered work")
        expected = {"protocol": PROTOCOL, "stage_id": work["stage_id"], "actor": self.actor,
                    "work_ref": to_dict(work_ref), "directive_sha256": directive_id(directive)}
        require(strict_loads(decision_raw) == expected, "Central decision binding differs")
        if materialize(directive, self.mesh, self.config["policy_sha256"]) is None:
            return None
        return self._record(work["stage_id"], work_ref, directive, (directive_id(directive),), decision_ref)

    def _crash(self, state: str, key: str) -> None:
        marker = self.root / "restart-fault.json"
        if not self.config["crash_once"] or not (self.actor == "B01" or self.actor.endswith(".B01")) or state != "terminal":
            return
        if marker.exists():
            prior = strict_loads(marker.read_bytes())
            require(prior.get("config_sha256") == self.config_sha256 and prior.get("actor") == self.actor,
                    "Retained restart marker differs")
            return
        snapshot = next(item for item in self.loop.snapshots() if item.directive_id == key)
        if (snapshot.action is not None and snapshot.reply is not None and snapshot.reply.state == "completed"
                and snapshot.action.context.milestone == 1 and snapshot.action.work.generation == 0):
            immutable_write(marker, {"protocol": PROTOCOL, "actor": self.actor, "pid": os.getpid(), "config_sha256": self.config_sha256,
                "directive_id": key, "reply": to_dict(snapshot.reply), "fault": "known_result_before_publication_builder_restart"})
            self.final_event("restart")
            os._exit(86)

    def tick(self) -> None:
        self._before_admission()
        for ref in self.mesh.arrived():
            if ref.kind not in ("cumulative-work", "cumulative-assignment"):
                continue
            require(ref.producer == "seed", "Control event needs seed authority")
            raw = self._body(ref)
            if raw is not None:
                if self.placement == "peer_local" and ref.kind == "cumulative-work":
                    self.choose_local(ref, strict_loads(raw))
                elif self.placement == "durable_central_scheduler" and ref.kind == "cumulative-assignment":
                    self.accept_assignment(ref, strict_loads(raw))
        self._before_admission()
        self.loop.tick()
        for snapshot in self.loop.snapshots():
            if snapshot.state not in ("published", "stopped"):
                continue
            row = self.db.execute("SELECT stage FROM decisions WHERE directive=?", (snapshot.directive_id,)).fetchone()
            require(row is not None, "Action has no durable decision")
            request = asdict(self.loop.worker_request(snapshot.directive_id)) if snapshot.view is not None else None
            raw = self._body(snapshot.result_ref) if snapshot.result_ref is not None else None
            if snapshot.result_ref is not None and raw is None:
                continue
            value = {"protocol": PROTOCOL, "stage_id": row[0], "actor": self.actor,
                     "directive_id": snapshot.directive_id, "snapshot": asdict(snapshot),
                     "worker_request": request, "result_payload": strict_loads(raw) if raw is not None else None}
            immutable_write(self.root / "results" / (snapshot.directive_id + ".json"), value)
            self.mesh.publish("cumulative-result", canonical_bytes(value), "cumulative-result-" + snapshot.directive_id)


def main(argv: list[str] | None = None) -> int:
    from .cumulative_process_evidence_v1 import publish_role_event

    parser = argparse.ArgumentParser()
    parser.add_argument("--role-config", required=True, type=Path)
    args = parser.parse_args(argv)
    config = strict_loads(args.role_config.read_bytes())
    mesh = MeshNode(checked_config(config))
    driver = None
    try:
        finance = config["finance"]
        driver = RoleDriver(args.role_config.parent, config, mesh,
                            FinancialClient(finance["port"], config["actor"], finance["capability"], finance["contract_sha256"]),
                            publish_role_event=publish_role_event)
        ready = mesh.start()
        publish_role_event("ready")
        immutable_write(args.role_config.parent / ("ready-" + str(os.getpid()) + ".json"),
                        {**ready, "config_sha256": driver.config_sha256})
        while not (args.role_config.parent / "stop").exists() and time.time() < driver.deadline_unix:
            try:
                driver.tick()
            except RoleDeadline:
                break
            time.sleep(0.04)
        immutable_write(args.role_config.parent / ("final-" + str(os.getpid()) + ".json"),
            {"protocol": PROTOCOL, "actor": config["actor"], "pid": os.getpid(), "config_sha256": driver.config_sha256,
             "snapshots": [asdict(item) for item in driver.loop.snapshots()], "history": driver.loop.history()})
        driver.final_event("stopped")
        return 0
    except BaseException:
        if driver is not None:
            driver.final_event("failed")
        raise
    finally:
        if driver is not None:
            driver.close()
        mesh.close()


if __name__ == "__main__":
    raise SystemExit(main())
