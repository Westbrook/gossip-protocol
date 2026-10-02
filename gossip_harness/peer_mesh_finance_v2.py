"""Local mesh payload adapter for the offline financial authority.

The RPC request guard proves arrival of the *exact* worker-request reference.
The authority's older digest-only payload interface cannot establish that fact
on its own. Results are published by the finance service, with a durable private
principal-to-result index; no role identity is impersonated on the mesh.
"""
from __future__ import annotations

import fcntl
import hashlib
from pathlib import Path
import sqlite3
import threading
from typing import Callable

from .peer_financial_authority_v2 import MAX_BYTES, canonical_payload
from .peer_mesh_v2 import MeshNode
from .peer_project_contract_v2 import (
    ActionRequest, EvidenceRef, from_dict, identifier, sha256, strict_loads, to_dict,
)

PROTOCOL = "peer-mesh-finance-v2"
MAX_RESULTS = 4096


class MeshFinanceError(ValueError):
    """Local identity, retained index or arrived evidence differs."""


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class MeshFinancePayloads:
    def __init__(self, root: Path, mesh: MeshNode, principals: tuple[str, ...], *,
                 crash_hook: Callable[[str], None] | None = None):
        if (type(principals) is not tuple or not 1 <= len(principals) <= 64
                or len(set(principals)) != len(principals)):
            raise MeshFinanceError("Invalid principal roster")
        for actor in principals:
            identifier(actor)
        if mesh.node_id in principals or any(actor not in mesh.config.roster for actor in principals):
            raise MeshFinanceError("Finance and registered role identities must be distinct")
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.mesh, self.principals, self.crash_hook = mesh, frozenset(principals), crash_hook
        self.lock = threading.RLock()
        self.closed = False
        self.config = {"protocol": PROTOCOL, "root": str(self.root),
                       "mesh_identity": mesh.config.identity(), "principals": sorted(principals),
                       "source_sha256": _sha(Path(__file__).read_bytes()), "max_results": MAX_RESULTS}
        self.config_bytes = canonical_payload(self.config)
        self.config_sha256 = _sha(self.config_bytes)
        lock_path = self.root / "payload-owner.lock"
        if lock_path.is_symlink():
            raise MeshFinanceError("Symlink owner lock refused")
        self.owner = lock_path.open("a+b")
        try:
            fcntl.flock(self.owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            self.owner.close()
            raise MeshFinanceError("Payload adapter already has an owner") from error
        try:
            path = self.root / "payload-index.sqlite"
            if any(Path(str(path) + suffix).is_symlink() for suffix in ("", "-wal", "-shm")):
                raise MeshFinanceError("Symlink database refused")
            existed = path.exists()
            self.db = sqlite3.connect(path, check_same_thread=False, timeout=2)
            self.db.row_factory = sqlite3.Row
            self.db.execute("PRAGMA synchronous=FULL")
            if self.db.execute("PRAGMA journal_mode=WAL").fetchone()[0] != "wal":
                raise MeshFinanceError("WAL unavailable")
            tables = {row[0] for row in self.db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
            if (existed and tables != {"registration", "results"}) or (not existed and tables):
                raise MeshFinanceError("Foreign or incomplete payload index")
            with self.db:
                if not existed:
                    self.db.execute("CREATE TABLE registration (id INTEGER PRIMARY KEY CHECK(id=1), config BLOB, count INTEGER)")
                    self.db.execute("""CREATE TABLE results (
                        principal TEXT NOT NULL, sha TEXT NOT NULL, command TEXT NOT NULL UNIQUE,
                        payload BLOB NOT NULL, reference BLOB, PRIMARY KEY(principal,sha))""")
                    self.db.execute("INSERT INTO registration VALUES (1,?,0)", (self.config_bytes,))
                self._audit()
        except BaseException:
            if hasattr(self, "db"):
                self.db.close()
            self.owner.close()
            raise

    @property
    def request_guard_sha256(self) -> str:
        """Bind the guard implementation and its immutable local configuration."""
        return self.config_sha256

    def _open(self) -> None:
        if self.closed:
            raise MeshFinanceError("Payload adapter closed")

    def _principal(self, principal: str) -> None:
        self._open()
        if principal not in self.principals:
            raise MeshFinanceError("Unknown financial principal")

    def _command(self, principal: str, sha: str) -> str:
        return "result-" + _sha(canonical_payload({"protocol": PROTOCOL, "principal": principal,
                                                   "sha": sha, "config": self.config_sha256}))

    def _row(self, row: sqlite3.Row) -> EvidenceRef | None:
        self._principal(row["principal"])
        sha256(row["sha"])
        if (type(row["payload"]) is not bytes or _sha(row["payload"]) != row["sha"]
                or not 0 < len(row["payload"]) <= MAX_BYTES
                or row["command"] != self._command(row["principal"], row["sha"])):
            raise MeshFinanceError("Retained result intent differs")
        if row["reference"] is None:
            return None
        raw = row["reference"]
        ref = from_dict(EvidenceRef, strict_loads(raw))
        if (canonical_payload(to_dict(ref)) != raw or ref.producer != self.mesh.node_id
                or ref.kind != "financial-result" or ref.payload_sha256 != row["sha"]):
            raise MeshFinanceError("Retained service publication differs")
        return ref

    def _registration(self) -> None:
        registrations = list(self.db.execute("SELECT * FROM registration"))
        count = self.db.execute("SELECT COUNT(*) FROM results").fetchone()[0]
        if (len(registrations) != 1 or registrations[0]["id"] != 1
                or registrations[0]["config"] != self.config_bytes
                or registrations[0]["count"] != count or count > MAX_RESULTS):
            raise MeshFinanceError("Retained registration or membership differs")

    def _audit(self) -> None:
        self._registration()
        for row in self.db.execute("SELECT * FROM results"):
            self._row(row)

    def _resolve(self, ref: EvidenceRef) -> bytes:
        if ref not in self.mesh.arrived():
            raise FileNotFoundError("Exact reference has not arrived")
        if not self.mesh.want(ref):
            raise FileNotFoundError("Payload subscription is under backpressure")
        raw = self.mesh.resolve(ref)
        if raw is None:
            raise FileNotFoundError("Referenced payload has not arrived")
        if type(raw) is not bytes or _sha(raw) != ref.payload_sha256:
            raise MeshFinanceError("Arrived payload digest differs")
        return raw

    def request_guard(self, action: ActionRequest) -> None:
        with self.lock:
            if type(action) is not ActionRequest:
                raise MeshFinanceError("Typed action required")
            self._principal(action.actor)
            self._registration()
            if (action.context.cohort_id != self.mesh.config.cohort_id
                    or action.context.execution_contract_sha256 != self.mesh.config.execution_contract_sha256
                    or action.worker_payload_ref.producer != action.actor
                    or action.worker_payload_ref.kind != "worker-request"):
                raise MeshFinanceError("Request reference differs from registered actor or cohort")
            self._resolve(action.worker_payload_ref)

    def read_owned(self, principal: str, sha: str) -> bytes:
        with self.lock:
            self._principal(principal)
            self._registration()
            sha256(sha)
            row = self.db.execute("SELECT * FROM results WHERE principal=? AND sha=?", (principal, sha)).fetchone()
            if row is not None:
                ref = self._row(row)
                if ref is None:
                    raise FileNotFoundError("Service result publication is incomplete")
                return self._resolve(ref)
            arrived = self.mesh.arrived()
            if any(ref.producer == self.mesh.node_id and ref.kind == "financial-result"
                   and ref.payload_sha256 == sha for ref in arrived):
                raise MeshFinanceError("Service result has no matching principal index")
            refs = [ref for ref in arrived if ref.producer == principal
                    and ref.kind == "worker-request" and ref.payload_sha256 == sha]
            if not refs:
                raise FileNotFoundError("Owned request has not arrived")
            return self._resolve(refs[0])

    def put_owned(self, principal: str, data: bytes) -> str:
        with self.lock:
            self._principal(principal)
            self._registration()
            if type(data) is not bytes or not 0 < len(data) <= MAX_BYTES:
                raise MeshFinanceError("Invalid bounded result bytes")
            sha = _sha(data)
            command = self._command(principal, sha)
            with self.db:
                row = self.db.execute("SELECT * FROM results WHERE principal=? AND sha=?", (principal, sha)).fetchone()
                if row is None:
                    count = self.db.execute("SELECT count FROM registration WHERE id=1").fetchone()[0]
                    if count >= MAX_RESULTS:
                        raise MeshFinanceError("Result index quota exceeded")
                    self.db.execute("INSERT INTO results VALUES (?,?,?,?,NULL)", (principal, sha, command, data))
                    self.db.execute("UPDATE registration SET count=count+1 WHERE id=1")
                elif row["payload"] != data:
                    raise MeshFinanceError("Result intent identity conflict")
                else:
                    self._row(row)
            if self.crash_hook is not None:
                self.crash_hook("intent_durable")
            ref = self.mesh.publish("financial-result", data, command)
            if ref.producer != self.mesh.node_id or ref.kind != "financial-result" or ref.payload_sha256 != sha:
                raise MeshFinanceError("Service publication identity differs")
            if self.crash_hook is not None:
                self.crash_hook("publication_durable")
            raw_ref = canonical_payload(to_dict(ref))
            with self.db:
                prior = self.db.execute("SELECT reference FROM results WHERE principal=? AND sha=?", (principal, sha)).fetchone()
                if prior[0] is not None and prior[0] != raw_ref:
                    raise MeshFinanceError("Result replay returned another reference")
                self.db.execute("UPDATE results SET reference=? WHERE principal=? AND sha=?", (raw_ref, principal, sha))
            return sha

    def close(self) -> None:
        with self.lock:
            if not self.closed:
                self.closed = True
                self.db.close()
                self.owner.close()
