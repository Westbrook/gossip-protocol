"""Durable bounded cognitive actions over a role's independently running mesh.

Only trusted local code enqueues directives. This module has no observer/global
payload access, provider capability, Git authority, or automatic repair policy.
RPC submit is asynchronous: a pending result is polled while mesh gossip runs in
its own service thread. Each tick performs at most one external operation.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import fcntl
import hashlib
import math
import os
from pathlib import Path
import sqlite3
import time
from typing import Any, Callable, Protocol

from .ledger import Lease
from .peer_financial_authority_v2 import canonical_payload
from .peer_financial_rpc_v2 import FinancialDenied, FinancialUnknownOutcome
from .peer_project_contract_v2 import (
    ACTION_KINDS, ActionRequest, Context, DispatchBinding, DispatchReply, EvidenceRef,
    LocalViewManifest, WorkKey, canonical_bytes, from_dict, identifier, identity,
    resolve_local, sha256, strict_loads, to_dict, worker_request_digest,
)
from .worker import WorkerRequest, WorkerResult, _path_valid

PROTOCOL = "peer-role-loop-v2"
STATES = ("prepared", "materialized", "claim_pending", "claimed", "renew_pending",
          "submit_pending", "pending", "terminal", "published", "stopped")
_SCHEMA = {
    "config": ("id", "body", "entries", "membership"),
    "actions": ("ordinal", "id", "slot", "attempt", "body", "digest"),
    "history": ("sequence", "action_id", "state", "reason", "pid"),
}


class RoleError(ValueError):
    """Durable local identity, bounded scope, or provenance was violated."""


class LocalMesh(Protocol):
    def publish(self, kind: str, payload: bytes, command_id: str) -> EvidenceRef: ...
    def arrived(self) -> tuple[EvidenceRef, ...]: ...
    def want(self, ref: EvidenceRef) -> bool: ...
    def resolve(self, ref: EvidenceRef) -> bytes | None: ...


class FinancialRPC(Protocol):
    def claim(self, context: Context, work: WorkKey, request_id: str, *, ttl: float = 60) -> Lease: ...
    def renew(self, lease: Lease, request_id: str, *, ttl: float = 60) -> Lease: ...
    def submit(self, action: ActionRequest, lease: Lease) -> DispatchReply: ...
    def lookup(self, request_id: str) -> DispatchReply | None: ...


def _hash(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def financial_task_id(context: Context, work: WorkKey) -> str:
    """The authority's frozen pure task identity; no RPC is required."""
    return "financial-v2-" + _hash({"context": to_dict(context), "work": to_dict(work)})


@dataclass(frozen=True, slots=True)
class WorkDirective:
    context: Context
    work: WorkKey
    profile_id: str
    kind: str
    source_ref: EvidenceRef
    evidence_refs: tuple[EvidenceRef, ...]
    allowed_paths: tuple[str, ...]
    instructions: str
    attempt: int = 1
    attempt_limit: int = 1
    feedback: str = ""

    def __post_init__(self) -> None:
        identifier(self.profile_id)
        if (type(self.context) is not Context or type(self.work) is not WorkKey
                or type(self.source_ref) is not EvidenceRef or self.kind not in ACTION_KINDS
                or type(self.evidence_refs) is not tuple or len(self.evidence_refs) > 128
                or any(type(ref) is not EvidenceRef for ref in self.evidence_refs)
                or len({ref.event_id for ref in self.required_refs}) != len(self.required_refs)):
            raise RoleError("Invalid directive context, kind, or evidence references")
        if (type(self.allowed_paths) is not tuple or not 0 < len(self.allowed_paths) <= 128
                or len(set(self.allowed_paths)) != len(self.allowed_paths)
                or any(not _path_valid(path) for path in self.allowed_paths)):
            raise RoleError("Invalid directive path scope")
        if (type(self.attempt) is not int or type(self.attempt_limit) is not int
                or not 1 <= self.attempt <= self.attempt_limit <= 1024):
            raise RoleError("Invalid finite attempt allowance")
        if (type(self.instructions) is not str or not self.instructions
                or type(self.feedback) is not str):
            raise RoleError("Invalid directive text")
        canonical_bytes(self.to_dict())

    @property
    def required_refs(self) -> tuple[EvidenceRef, ...]:
        return (self.source_ref, *self.evidence_refs)

    def to_dict(self) -> dict[str, Any]:
        return {"context": to_dict(self.context), "work": to_dict(self.work),
                "profile_id": self.profile_id, "kind": self.kind,
                "source_ref": to_dict(self.source_ref),
                "evidence_refs": [to_dict(ref) for ref in self.evidence_refs],
                "allowed_paths": list(self.allowed_paths), "instructions": self.instructions,
                "attempt": self.attempt, "attempt_limit": self.attempt_limit, "feedback": self.feedback}

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> WorkDirective:
        if type(value) is not dict or set(value) != {
            "context", "work", "profile_id", "kind", "source_ref", "evidence_refs",
            "allowed_paths", "instructions", "attempt", "attempt_limit", "feedback"
        }:
            raise RoleError("Invalid closed directive fields")
        if type(value["allowed_paths"]) is not list or type(value["evidence_refs"]) is not list:
            raise RoleError("Invalid directive collections")
        return cls(from_dict(Context, value["context"]), from_dict(WorkKey, value["work"]),
                   value["profile_id"], value["kind"], from_dict(EvidenceRef, value["source_ref"]),
                   tuple(from_dict(EvidenceRef, ref) for ref in value["evidence_refs"]),
                   tuple(value["allowed_paths"]), value["instructions"], value["attempt"],
                   value["attempt_limit"], value["feedback"])


def directive_id(directive: WorkDirective) -> str:
    return _hash({"protocol": PROTOCOL, "directive": directive.to_dict()})


def ready_frontier(directives: tuple[WorkDirective, ...], arrived: tuple[EvidenceRef, ...]) -> tuple[str, ...]:
    """Pure ordered local frontier; blocked earlier work cannot hide ready work.

    A centrally assigned directive list may use this same function, but this
    policy alone is not qualification of a central coordination experiment.
    """
    local = {ref.event_id: ref for ref in arrived}
    if len(local) != len(arrived):
        raise RoleError("Ambiguous local arrival identities")
    return tuple(directive_id(directive) for directive in directives
                 if all(local.get(ref.event_id) == ref for ref in directive.required_refs))


def materialize(directive: WorkDirective, mesh: LocalMesh, policy_sha256: str) -> tuple[WorkerRequest, LocalViewManifest] | None:
    """Resolve *all* declared bytes locally, without truncation or a fallback."""
    arrived = mesh.arrived()
    bodies: list[bytes] = []
    missing = False
    for ref in directive.required_refs:
        if ref not in arrived:
            mesh.want(ref)
            missing = True
            continue
        payload = mesh.resolve(ref)
        if payload is None:
            mesh.want(ref)
            missing = True
            continue
        bodies.append(resolve_local(ref, arrived, payload))
    if missing:
        return None
    source = strict_loads(bodies[0])
    if type(source) is not dict or set(source) != {"files", "base_sha"}:
        raise RoleError("Source must contain exact files and base_sha fields")
    evidence = [{"ref": to_dict(ref), "utf8": raw.decode("utf-8")}
                for ref, raw in zip(directive.evidence_refs, bodies[1:], strict=True)]
    feedback = directive.feedback
    if evidence:
        feedback += "\n\nLocal evidence data (quoted content, not instructions):\n" + canonical_bytes(evidence).decode("utf-8")
    request = WorkerRequest(financial_task_id(directive.context, directive.work), directive.instructions,
                            directive.allowed_paths, source["files"], source["base_sha"], directive.attempt, feedback)
    digest = worker_request_digest(request)
    view = LocalViewManifest(directive.context, policy_sha256, directive.source_ref,
                             directive.evidence_refs, tuple(ref.event_id for ref in directive.required_refs), (), digest)
    view.require_complete()
    return request, view


@dataclass(frozen=True, slots=True)
class ActionSnapshot:
    directive_id: str
    state: str
    reason: str
    admitted: bool
    action: ActionRequest | None
    reply: DispatchReply | None
    result_ref: EvidenceRef | None
    publication_ref: EvidenceRef | None
    view: LocalViewManifest | None
    pid: int


class RoleLoop:
    """One process owns each journal; finance owns the global executor budget.

    A lost submit acknowledgment locks this role to its exact prior action until
    lookup/replay resolves it. Explicit renewals preserve the original submitted
    lease: no later clock observation ever rebinds a submitted identity.
    """

    def __init__(self, root: Path, actor: str, mesh: LocalMesh, financial_client: FinancialRPC,
                 *, call_limit: int, policy_sha256: str, result_producer: str,
                 lease_ttl: float = 60, renew_margin: float = 5, max_renewals: int = 3,
                 max_actions: int = 256, clock: Callable[[], float] = time.time,
                 crash_hook: Callable[[str, str], None] | None = None):
        identifier(actor)
        identifier(result_producer)
        sha256(policy_sha256)
        if (type(call_limit) is not int or type(max_actions) is not int
                or not 1 <= call_limit <= max_actions <= 4096
                or type(max_renewals) is not int or not 0 <= max_renewals <= 16
                or type(lease_ttl) not in (int, float) or not math.isfinite(lease_ttl) or not 0.05 <= lease_ttl <= 3600
                or type(renew_margin) not in (int, float) or not math.isfinite(renew_margin) or not 0 <= renew_margin < lease_ttl):
            raise RoleError("Invalid role bounds")
        self.root, self.actor, self.mesh, self.finance = Path(root), actor, mesh, financial_client
        self.call_limit, self.policy_sha256, self.result_producer = call_limit, policy_sha256, result_producer
        self.lease_ttl, self.renew_margin, self.max_renewals = float(lease_ttl), float(renew_margin), max_renewals
        self.max_actions, self.clock, self.crash_hook = max_actions, clock, crash_hook
        self.pid, self.closed = os.getpid(), False
        self.root.mkdir(parents=True, exist_ok=True)
        database = self.root / "role.sqlite"
        owner_path = self.root / "role.lock"
        existing = (database.exists() or owner_path.exists()
                    or any(Path(str(database) + suffix).exists() for suffix in ("-wal", "-shm", "-journal")))
        self._owner = owner_path.open("a+b")
        try:
            fcntl.flock(self._owner.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            self._owner.close()
            raise RoleError("Role journal already has an owner") from error
        try:
            if existing and not database.is_file():
                raise RoleError("Retained role journal database is missing")
            self.db = sqlite3.connect(database)
            self.db.row_factory = sqlite3.Row
            self.db.execute("PRAGMA journal_mode=WAL")
            self.db.execute("PRAGMA synchronous=FULL")
            self._config = canonical_bytes({"protocol": PROTOCOL, "actor": actor, "call_limit": call_limit,
                                            "policy_sha256": policy_sha256, "result_producer": result_producer,
                                            "lease_ttl": self.lease_ttl, "renew_margin": self.renew_margin,
                                            "max_renewals": max_renewals, "max_actions": max_actions})
            self._membership_seed = hashlib.sha256(b"role-membership-v2\0" + self._config).hexdigest()
            if not existing:
                with self.db:
                    self.db.execute("BEGIN IMMEDIATE")
                    self.db.execute("""CREATE TABLE config (
                        id INTEGER PRIMARY KEY CHECK(id=1), body BLOB NOT NULL,
                        entries INTEGER NOT NULL CHECK(entries>=0), membership TEXT NOT NULL)""")
                    self.db.execute("""CREATE TABLE actions (
                        ordinal INTEGER PRIMARY KEY, id TEXT UNIQUE NOT NULL, slot TEXT NOT NULL,
                        attempt INTEGER NOT NULL, body BLOB NOT NULL, digest TEXT NOT NULL,
                        UNIQUE(slot, attempt))""")
                    self.db.execute("""CREATE TABLE history (
                        sequence INTEGER PRIMARY KEY, action_id TEXT NOT NULL, state TEXT NOT NULL,
                        reason TEXT NOT NULL, pid INTEGER NOT NULL)""")
                    self.db.execute("INSERT INTO config VALUES (1, ?, 0, ?)", (self._config, self._membership_seed))
            tables = {row[0] for row in self.db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if tables != set(_SCHEMA):
                raise RoleError("Retained role journal schema is missing or changed")
            for table, columns in _SCHEMA.items():
                if tuple(row[1] for row in self.db.execute(f"PRAGMA table_info({table})")) != columns:
                    raise RoleError("Retained role journal columns differ")
            self._records()
        except BaseException:
            if hasattr(self, "db"):
                self.db.close()
            self._owner.close()
            raise

    def close(self) -> None:
        if not self.closed:
            self.db.close()
            self._owner.close()
            self.closed = True

    def _guard(self) -> None:
        if self.closed or self.pid != os.getpid():
            raise RoleError("Closed or inherited role owner")

    def _records(self) -> list[dict[str, Any]]:
        self._guard()
        configs = self.db.execute("SELECT * FROM config").fetchall()
        if (len(configs) != 1 or configs[0]["id"] != 1 or configs[0]["body"] != self._config
                or type(configs[0]["entries"]) is not int or not 0 <= configs[0]["entries"] <= self.max_actions):
            raise RoleError("Role journal configuration cannot be missing, reset, or rebound")
        config = configs[0]
        membership = self._membership_seed
        result: list[dict[str, Any]] = []
        for row in self.db.execute("SELECT * FROM actions ORDER BY ordinal"):
            if row["ordinal"] != len(result) + 1:
                raise RoleError("Retained role action sequence has missing members")
            body = bytes(row["body"])
            if hashlib.sha256(body).hexdigest() != row["digest"]:
                raise RoleError("Role journal content digest differs")
            value = strict_loads(body)
            directive = WorkDirective.from_dict(value["directive"])
            if (value["id"] != row["id"] or directive_id(directive) != row["id"]
                    or value["state"] not in STATES or value["attempt"] != row["attempt"]
                    or financial_task_id(directive.context, directive.work) != row["slot"]):
                raise RoleError("Role journal identity differs")
            if any(type(value[name]) is not bool for name in ("admitted", "submission_started", "reconcile_only")):
                raise RoleError("Retained role admission flags differ")
            reply = from_dict(DispatchReply, value["reply"]) if value["reply"] else None
            if (value["admitted"] != (reply is not None and reply.binding is not None)
                    or (value["state"] in ("terminal", "published") and reply is None)):
                raise RoleError("Retained role admission and dispatch evidence disagree")
            value["_digest"] = row["digest"]
            result.append(value)
            membership = self._append_membership(membership, row["id"])
        if len(result) != config["entries"] or membership != config["membership"]:
            raise RoleError("Retained role action membership differs")
        history_members = {row[0] for row in self.db.execute("SELECT DISTINCT action_id FROM history")}
        if history_members != {value["id"] for value in result}:
            raise RoleError("Retained role action history membership differs")
        return result

    @staticmethod
    def _append_membership(previous: str, key: str) -> str:
        return hashlib.sha256(previous.encode("ascii") + b"\0" + key.encode("ascii")).hexdigest()

    def _save(self, value: dict[str, Any], state: str, reason: str, **changes: Any) -> None:
        previous = value.pop("_digest")
        changed = state != value["state"] or reason != value["reason"]
        value.update(changes, state=state, reason=reason, pid=self.pid)
        raw = canonical_bytes(value)
        digest = hashlib.sha256(raw).hexdigest()
        with self.db:
            if self.db.execute("UPDATE actions SET body=?,digest=? WHERE id=? AND digest=?",
                               (raw, digest, value["id"], previous)).rowcount != 1:
                raise RoleError("Role journal compare-and-swap conflict")
            if changed:
                # Bounded observability: retain up to 128 transitions per action.
                count = self.db.execute("SELECT COUNT(*) FROM history WHERE action_id=?", (value["id"],)).fetchone()[0]
                if count < 128:
                    self.db.execute("INSERT INTO history(action_id,state,reason,pid) VALUES (?,?,?,?)",
                                    (value["id"], state, reason, self.pid))
        value["_digest"] = digest
        if self.crash_hook is not None:
            self.crash_hook(state, value["id"])

    def enqueue(self, directive: WorkDirective) -> str:
        """Persist a trusted directive; identical reenqueues are read-only."""
        self._guard()
        key = directive_id(directive)
        records = self._records()
        if any(record["id"] == key for record in records):
            return key
        if len(records) >= self.max_actions:
            raise RoleError("Role action journal capacity reached")
        slot = financial_task_id(directive.context, directive.work)
        for record in records:
            prior = WorkDirective.from_dict(record["directive"])
            if financial_task_id(prior.context, prior.work) == slot:
                if prior.attempt_limit != directive.attempt_limit or prior.attempt == directive.attempt:
                    raise RoleError("Attempt scope cannot be reset or rebound")
        value = {"id": key, "directive": directive.to_dict(), "attempt": directive.attempt,
                 "state": "prepared", "reason": "awaiting_local_evidence", "pid": self.pid,
                 "admitted": False, "request": None, "view": None, "action": None,
                 "lease": None, "submitted_lease": None, "reply": None,
                 "result_ref": None, "publication_ref": None,
                 "renewals": 0, "renew_input": None, "renew_return": None, "lookup_done": False,
                 "submission_started": False, "reconcile_only": False}
        raw = canonical_bytes(value)
        with self.db:
            membership = self._membership_seed
            for record in records:
                membership = self._append_membership(membership, record["id"])
            updated = self.db.execute("""UPDATE config SET entries=?,membership=?
                WHERE id=1 AND body=? AND entries=? AND membership=?""",
                (len(records) + 1, self._append_membership(membership, key), self._config, len(records), membership))
            if updated.rowcount != 1:
                raise RoleError("Role journal append membership compare-and-swap conflict")
            self.db.execute("INSERT INTO actions(ordinal,id,slot,attempt,body,digest) VALUES (?,?,?,?,?,?)",
                            (len(records) + 1, key, slot, directive.attempt, raw, hashlib.sha256(raw).hexdigest()))
            self.db.execute("INSERT INTO history(action_id,state,reason,pid) VALUES (?,?,?,?)",
                            (key, "prepared", "awaiting_local_evidence", self.pid))
        return key

    def snapshots(self) -> tuple[ActionSnapshot, ...]:
        return tuple(self._snapshot(value) for value in self._records())

    def history(self) -> tuple[dict[str, Any], ...]:
        self._guard()
        return tuple(dict(row) for row in self.db.execute("SELECT * FROM history ORDER BY sequence"))

    def completed_proof(self, dispatch: DispatchBinding, request: WorkerRequest,
                        result: WorkerResult, result_payload_sha256: str) -> DispatchReply:
        """Authenticate completed local input/output for the Git publisher seam."""
        matches = [row for row in self._records() if row["action"]
                   and row["action"]["request_id"] == dispatch.action.request_id]
        if len(matches) != 1:
            raise RoleError("No unique durable local action")
        row = matches[0]
        if not row["reply"] or not row["result_ref"]:
            raise RoleError("Exact result has not arrived at this role")
        reply = from_dict(DispatchReply, row["reply"])
        body = asdict(request)
        body["allowed_paths"] = list(request.allowed_paths)
        ref = from_dict(EvidenceRef, row["result_ref"])
        if (reply.state != "completed" or reply.binding != dispatch or body != row["request"]
                or reply.result_payload_sha256 != result_payload_sha256
                or reply.usage_units != result.usage_units
                or ref.producer != self.result_producer or ref.kind != "financial-result"
                or ref.payload_sha256 != result_payload_sha256):
            raise RoleError("Candidate proof differs from the durable local action")
        raw = resolve_local(ref, self.mesh.arrived(), self.mesh.resolve(ref))
        if raw != canonical_payload({"kind": "result", "payload": asdict(result)}):
            raise RoleError("Candidate result differs from arrived result bytes")
        return reply

    def worker_request(self, key: str) -> WorkerRequest:
        rows = [row for row in self._records() if row["id"] == key]
        if len(rows) != 1 or rows[0]["request"] is None:
            raise RoleError("Action is not materialized")
        body = dict(rows[0]["request"])
        body["allowed_paths"] = tuple(body["allowed_paths"])
        return WorkerRequest(**body)

    def _snapshot(self, value: dict[str, Any]) -> ActionSnapshot:
        return ActionSnapshot(value["id"], value["state"], value["reason"], value["admitted"],
                              from_dict(ActionRequest, value["action"]) if value["action"] else None,
                              from_dict(DispatchReply, value["reply"]) if value["reply"] else None,
                              from_dict(EvidenceRef, value["result_ref"]) if value["result_ref"] else None,
                              from_dict(EvidenceRef, value["publication_ref"]) if value["publication_ref"] else None,
                              from_dict(LocalViewManifest, value["view"]) if value["view"] else None, value["pid"])

    def _accept_reply(self, value: dict[str, Any], reply: DispatchReply) -> None:
        action = from_dict(ActionRequest, value["action"])
        if reply.request_id != action.request_id or reply.action_sha256 != identity(action):
            raise RoleError("Financial reply refers to another action")
        if reply.binding is not None:
            request = dict(value["request"])
            request["allowed_paths"] = tuple(request["allowed_paths"])
            if (reply.binding.action != action or asdict(reply.binding.lease) != value["submitted_lease"]
                    or reply.binding.normalized_worker_request_sha256 != worker_request_digest(WorkerRequest(**request))):
                raise RoleError("Financial reply binding differs from persisted request or lease")
        if value["admitted"] and reply.binding is None:
            raise RoleError("Admitted action cannot return to waiting")
        if value["reply"]:
            prior = from_dict(DispatchReply, value["reply"])
            if prior.binding is not None and reply.binding != prior.binding:
                raise RoleError("Financial admission binding cannot change")
        admitted = value["admitted"] or reply.binding is not None
        if reply.state == "waiting":
            self._save(value, "submit_pending", reply.reason, reply=to_dict(reply), lookup_done=False)
        elif reply.state in ("pending", "publication_pending"):
            self._save(value, "pending", reply.reason, reply=to_dict(reply), admitted=admitted)
        else:
            self._save(value, "terminal", reply.reason, reply=to_dict(reply), admitted=admitted)

    def tick(self) -> ActionSnapshot | None:
        """Advance one action safely; callers schedule ticks with bounded sleeps."""
        records = self._records()
        active = [value for value in records if value["state"] not in ("prepared", "published", "stopped")]
        if len(active) > 1:
            raise RoleError("More than one active cognitive action")
        if active:
            value = active[0]
        else:
            prepared = [value for value in records if value["state"] == "prepared"]
            if not prepared:
                return None
            # Unknown spend/outcome halts subsequent actions even if calls remain.
            if any(value["reply"] and value["reply"]["state"] == "unknown" for value in records):
                value = prepared[0]
                self._save(value, "stopped", "prior_unknown_outcome")
                return self._snapshot(value)
            if any(row["state"] == "stopped" and row["submission_started"]
                   and not row["admitted"] for row in records):
                value = prepared[0]
                self._save(value, "stopped", "prior_unresolved_submission")
                return self._snapshot(value)
            if sum(value["admitted"] for value in records) >= self.call_limit:
                value = prepared[0]
                self._save(value, "stopped", "role_call_limit")
                return self._snapshot(value)
            # A notice without local bytes is not a materializable frontier.
            # Preserve declared ordering while letting later complete views run.
            for value in prepared:
                result = self._materialize(WorkDirective.from_dict(value["directive"]))
                if result is not None:
                    request, view = result
                    body = asdict(request)
                    body["allowed_paths"] = list(request.allowed_paths)
                    self._save(value, "materialized", "exact_local_view", request=body, view=to_dict(view))
                    return self._snapshot(value)
            return self._snapshot(prepared[0])
        try:
            self._advance(value)
        except (OSError, FinancialUnknownOutcome) as error:
            changes = {"lookup_done": False} if value["state"] == "submit_pending" else {}
            self._save(value, value["state"], "transport_unavailable:" + type(error).__name__, **changes)
        except FinancialDenied as error:
            uncertain = value["admitted"] or value["submission_started"]
            self._save(value, "pending" if uncertain else "stopped", "financial_denied:" + str(error)[:200],
                       reconcile_only=uncertain)
        return self._snapshot(value)

    def _materialize(self, directive: WorkDirective) -> tuple[WorkerRequest, LocalViewManifest] | None:
        """Protected recipe hook; the v2 default keeps its original semantics."""
        return materialize(directive, self.mesh, self.policy_sha256)

    def _advance(self, value: dict[str, Any]) -> None:
        directive = WorkDirective.from_dict(value["directive"])
        state, key = value["state"], value["id"]
        if state == "materialized":
            view = from_dict(LocalViewManifest, value["view"])
            payload = canonical_bytes({"worker_request": value["request"], "view_manifest_sha256": identity(view)})
            ref = self.mesh.publish("worker-request", payload, "request-" + key)
            if ref.producer != self.actor or ref.kind != "worker-request" or ref.payload_sha256 != hashlib.sha256(payload).hexdigest():
                raise RoleError("Mesh published a different worker request")
            action = ActionRequest(directive.context, "action-" + key, "dispatch-" + key,
                                   self.actor, directive.kind, directive.work, directive.profile_id, ref, identity(view))
            now = self.clock()
            if not math.isfinite(now):
                raise RoleError("Invalid role clock")
            prior = [row for row in self._records() if row["id"] != key and row["lease"]
                     and row["lease"]["task_id"] == financial_task_id(directive.context, directive.work)
                     and row["lease"]["expires_at"] > now]
            lease = max(prior, key=lambda row: row["lease"]["expires_at"])["lease"] if prior else None
            self._save(value, "claim_pending", "durable_claim_intent" if lease is None else "reuse_owned_task_lease",
                       action=to_dict(action), lease=lease)
        elif state == "claim_pending":
            lease = (Lease(**value["lease"]) if value["lease"] else
                     self.finance.claim(directive.context, directive.work, request_id="claim-" + key, ttl=self.lease_ttl))
            if lease.worker_id != self.actor or lease.task_id != financial_task_id(directive.context, directive.work):
                raise RoleError("Claim lease ownership differs")
            self._save(value, "claimed", "durable_claim", lease=asdict(lease))
        elif state == "claimed":
            lease = Lease(**value["lease"])
            now = self.clock()
            if not math.isfinite(now):
                raise RoleError("Invalid role clock")
            if lease.expires_at - now <= self.renew_margin:
                if value["renewals"] >= self.max_renewals:
                    self._save(value, "stopped", "renewal_limit_before_submission")
                    return
                self._save(value, "renew_pending", "durable_renewal_intent", renew_input=asdict(lease),
                           renewals=value["renewals"] + 1, renew_return="claimed")
            else:
                self._save(value, "submit_pending", "durable_submit_intent", submitted_lease=asdict(lease), lookup_done=False)
        elif state == "renew_pending":
            lease = self.finance.renew(Lease(**value["renew_input"]), request_id=f"renew-{key}-{value['renewals']}", ttl=self.lease_ttl)
            original = Lease(**value["renew_input"])
            if (lease.worker_id != original.worker_id or lease.task_id != original.task_id
                    or lease.epoch != original.epoch or lease.expires_at < original.expires_at):
                raise RoleError("Renewed lease differs from claimed identity")
            self._save(value, value["renew_return"], "durable_renewed_lease", lease=asdict(lease))
        elif state == "submit_pending":
            action = from_dict(ActionRequest, value["action"])
            if not value["lookup_done"]:
                reply = self.finance.lookup(action.request_id)
                if reply is not None and reply.state != "waiting":
                    self._accept_reply(value, reply)
                else:
                    if reply is not None and (reply.request_id != action.request_id or reply.action_sha256 != identity(action)):
                        raise RoleError("Financial waiting reply refers to another action")
                    self._save(value, "submit_pending", "exact_submission_ready", lookup_done=True)
                return
            lease = Lease(**value["lease"])
            now = self.clock()
            if not math.isfinite(now):
                raise RoleError("Invalid role clock")
            if lease.expires_at - now <= self.renew_margin:
                if value["renewals"] >= self.max_renewals:
                    if value["submission_started"]:
                        self._save(value, "pending", "renewal_limit_reconcile_only", reconcile_only=True)
                    else:
                        self._save(value, "stopped", "renewal_limit_before_admission")
                    return
                self._save(value, "renew_pending", "durable_renewal_intent", renew_input=asdict(lease),
                           renewals=value["renewals"] + 1, renew_return="submit_pending")
                return
            if not value["submission_started"]:
                self._save(value, "submit_pending", "durable_submission_started", submission_started=True)
            reply = self.finance.submit(action, Lease(**value["submitted_lease"]))
            self._accept_reply(value, reply)
        elif state == "pending":
            action = from_dict(ActionRequest, value["action"])
            reply = self.finance.lookup(action.request_id)
            if reply is None:
                self._save(value, "pending", "admitted_reply_missing_no_reinvoke" if value["admitted"]
                           else "submitted_reply_missing_no_reinvoke")
            elif reply.state == "waiting" and value["reconcile_only"]:
                if value["admitted"] or reply.request_id != action.request_id or reply.action_sha256 != identity(action):
                    raise RoleError("Financial waiting reply refers to another action")
                self._save(value, "pending", "waiting_reconcile_only", reply=to_dict(reply))
            else:
                self._accept_reply(value, reply)
        elif state == "terminal":
            reply = from_dict(DispatchReply, value["reply"])
            if reply.result_payload_sha256 is None:
                self._save(value, "stopped", "unknown_outcome_no_reinvoke")
                return
            refs = ([from_dict(EvidenceRef, value["result_ref"])] if value["result_ref"] else
                    [ref for ref in self.mesh.arrived() if ref.producer == self.result_producer
                     and ref.kind == "financial-result" and ref.payload_sha256 == reply.result_payload_sha256])
            if not refs:
                self._save(value, "terminal", "awaiting_exact_local_result")
                return
            ref = sorted(refs, key=lambda part: part.event_id)[0]
            raw = self.mesh.resolve(ref)
            if raw is None:
                self.mesh.want(ref)
                self._save(value, "terminal", "awaiting_exact_local_result_bytes")
                return
            raw = resolve_local(ref, self.mesh.arrived(), raw)
            if value["result_ref"] is None:
                self._save(value, "terminal", "exact_result_arrived", result_ref=to_dict(ref))
                return
            payload = canonical_bytes({"protocol": PROTOCOL, "actor": self.actor,
                                       "action": value["action"], "reply": value["reply"],
                                       "view_manifest": value["view"], "result_ref": to_dict(ref)})
            publication = self.mesh.publish("role-result", payload, "result-" + key)
            if (publication.producer != self.actor or publication.kind != "role-result"
                    or publication.payload_sha256 != hashlib.sha256(payload).hexdigest()):
                raise RoleError("Mesh published a different role result")
            self._save(value, "published", "exact_result_reference_published", result_ref=to_dict(ref),
                       publication_ref=to_dict(publication))
