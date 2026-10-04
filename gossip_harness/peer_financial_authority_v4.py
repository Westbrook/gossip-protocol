"""Versioned normal closure over the existing V3 cumulative financial owner.

No wallet creation, cap changes, provider retries, or role-facing seal method.
The host checkpoint capability belongs to the controller thread; worker threads
use only the financial gate and joined SQLite admission/token fences.
"""
from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
import sqlite3
import threading
from typing import Any

from . import peer_financial_authority_v3 as v3
from .candidate_checkpoint_chain_v1 import CheckpointChain, PrefixCommitment
from .peer_financial_authority_v2 import FinancialError, _BASE_SCHEMA, canonical_payload
from .peer_financial_terminal_v1 import (
    ChildTerminalSeal, ChildTerminalSealRequest, CohortSealed, SealBusy,
    TerminalPreparation, TerminalRoster, authenticate_request, census,
    config_record, create_tables, digest, prefix, require, seal_name, seal_row, sha, authenticate_seal,
)
from .peer_project_contract_v2 import ActionRequest, DispatchReply, to_dict
from .peer_store_v1 import strict_loads
from .ledger import Lease
from .worker import OpenAIWorker, WorkerRequest

PROTOCOL = 'peer-financial-authority-v4'
PERMIT_PROTOCOL = 'peer-financial-operator-permit-v4'
QUALIFICATION_PROTOCOL = 'peer-financial-qualification-v4'
REQUIRED_SOURCES = (*v3.REQUIRED_SOURCES,
    'gossip_harness/peer_financial_authority_v4.py',
    'gossip_harness/peer_financial_terminal_v1.py',
    'gossip_harness/peer_financial_rpc_v2.py',
    'gossip_harness/peer_financial_rpc_v4.py',
    'gossip_harness/candidate_checkpoint_chain_v1.py',
    'gossip_harness/candidate_checkpoint_head_v1.py',
    'gossip_harness/candidate_http_journal_v3.py')
REQUIRED_TEST_CLASSES = (*v3.REQUIRED_TEST_CLASSES,
    'tests/test_peer_financial_terminal_v1.py::PeerFinancialTerminalV1Tests',
    'tests/test_peer_financial_authority_v4.py::PeerFinancialAuthorityV4Tests',
    'tests/test_peer_financial_rpc_v4.py::PeerFinancialRPCV4Tests')
_REPOSITORY = Path(__file__).resolve().parent.parent


def source_fingerprints() -> dict[str, str]:
    return {name: sha((_REPOSITORY / name).read_bytes()) for name in REQUIRED_SOURCES}


def validate_qualification(reference: dict, design: dict, sources: dict) -> dict:
    """Fail closed until the complete V4 six-child rehearsal validator exists.

    V3 qualification proves a different execution contract. A collection of
    asserted passed flags, child configs or checkpoint-retained seal-shaped
    JSON cannot qualify SQL origin, original provider/accounting journals and
    the full sequential barrier. This draft intentionally has no live permit
    route; fixture qualification and review cannot turn this into live entry.
    """
    raise FinancialError('V4 live qualification unavailable: full six-child rehearsal validator required')


class CumulativeAuthorityV4(v3.CumulativeAuthorityV3):
    @classmethod
    def open(cls, *args: Any, **kwargs: Any) -> CumulativeAuthorityV4:
        return cls(*args, **kwargs)

    def __init__(self, *args: Any, terminal_roster: TerminalRoster,
                 checkpoint: CheckpointChain, expected_checkpoint: PrefixCommitment, **kwargs: Any):
        self.terminal_gate = threading.RLock()
        self.active_executions: set[tuple[str, str]] = set()
        require(type(checkpoint) is CheckpointChain, 'Caller-owned exact checkpoint chain required')
        checkpoint.validate_boundary(expected=expected_checkpoint)
        self.checkpoint_chain = checkpoint
        # The contract argument can be positional or keyword, like the base API.
        contract = kwargs.get('cohort_contract', args[2] if len(args) > 2 else None)
        require(type(contract) is dict, 'Explicit cohort contract required')
        assert isinstance(contract, dict)
        self.terminal_config = config_record(terminal_roster, contract['cohort_id'], checkpoint)
        require(contract.get('execution_contract_sha256') == terminal_roster.execution_contract_sha256,
                'Child execution contract differs from prospective study roster')
        self.terminal_roster = terminal_roster
        self._sealed_at_open = False
        self._external_seal = (checkpoint.read(seal_name(contract['cohort_id']))
                               if checkpoint.has(seal_name(contract['cohort_id'])) else None)
        self._prior_publications = {
            child.cohort: checkpoint.read(seal_name(child.cohort))
            for child in terminal_roster.children[:self.terminal_config['child_index']]
            if checkpoint.has(seal_name(child.cohort))}
        for raw in self._prior_publications.values():
            authenticate_seal(checkpoint, raw)
        super().__init__(*args, **kwargs)

    @classmethod
    def preflight_permit(cls, cohort_contract: dict, incremental_cap_micro_usd: int,
                         expected_global_cap: int, expected_opening_usage: int, *,
                         workers: dict[str, OpenAIWorker], permit: dict,
                         expected_permit_sha256: str, max_workers: int = 4,
                         mode: str = 'live', terminal_roster: TerminalRoster | None = None) -> dict:
        require(type(terminal_roster) is TerminalRoster, 'V4 preflight requires prospective terminal roster')
        assert isinstance(terminal_roster, TerminalRoster)
        TerminalRoster(terminal_roster.execution_contract_sha256, terminal_roster.children, terminal_roster.policy)
        require(cohort_contract.get('execution_contract_sha256') == terminal_roster.execution_contract_sha256,
                'Child execution contract differs from prospective study roster')
        checking = object.__new__(cls)
        checking.terminal_roster = terminal_roster
        checking._prepare_permit(cohort_contract, incremental_cap_micro_usd, expected_global_cap,
            expected_opening_usage, workers=workers, permit=permit, expected_permit_sha256=expected_permit_sha256,
            max_workers=max_workers, mode=mode)
        checking.contract = strict_loads(canonical_payload(cohort_contract), max_bytes=2_100_000)
        require(set(checking.contract) == {'cohort_id', 'execution_contract_sha256', 'ledger_identity',
                'journal_root', 'transport_identity', 'task_specs'}, 'Closed V4 cohort contract required')
        child = [item for item in terminal_roster.children if item.cohort == cohort_contract['cohort_id']]
        require(len(child) == 1 and set(child[0].actors) == {
            actor for spec in cohort_contract['task_specs'] for actor in spec['actors']}, 'V4 preflight child identity differs')
        profiles = {name: checking._profile(name, worker) for name, worker in workers.items()}
        return {'protocol': PROTOCOL, 'mode': mode, 'operator_permit_sha256': expected_permit_sha256,
                'execution_design_sha256': checking.permit['execution_design_sha256'],
                'cohort_contract_sha256': digest(cohort_contract), 'profiles': profiles,
                'terminal_roster_sha256': terminal_roster.sha256, 'ledger_opened': False, 'provider_invoked': False}

    def _prepare_permit(self, cohort_contract: dict, incremental_cap_micro_usd: int,
                        expected_global_cap: int, expected_opening_usage: int, *,
                        workers: dict[str, OpenAIWorker], permit: dict | None,
                        expected_permit_sha256: str | None, max_workers: int, mode: str) -> None:
        self._validate_mode(mode)
        require(type(permit) is dict and set(permit) == v3._PERMIT_FIELDS
                and v3._hash(expected_permit_sha256), 'Explicit closed V4 permit required')
        self._permit_bytes = canonical_payload(permit)
        require(sha(self._permit_bytes) == expected_permit_sha256, 'V4 permit pin differs')
        self.permit = strict_loads(self._permit_bytes, max_bytes=2_100_000)
        self._permit_sha256, self._mode = expected_permit_sha256, mode
        profiles = {name: v3.profile_manifest(worker) for name, worker in workers.items()}
        require(self.permit['protocol'] == PERMIT_PROTOCOL and self.permit['mode'] == mode
                and type(self.permit['approval_ref']) is str and 1 <= len(self.permit['approval_ref']) <= 1024
                and self.permit['cohort_contract_sha256'] == digest(cohort_contract)
                and self.permit['profiles'] == profiles, 'V4 permit scope differs')
        for key, value in (('incremental_cap_micro_usd', incremental_cap_micro_usd),
                           ('expected_global_cap', expected_global_cap),
                           ('expected_opening_usage', expected_opening_usage), ('max_workers', max_workers)):
            require(type(value) is int and type(self.permit[key]) is int and self.permit[key] == value,
                    'V4 limits differ')
        require(0 <= expected_opening_usage <= expected_global_cap
                and 0 < incremental_cap_micro_usd <= expected_global_cap - expected_opening_usage,
                'V4 allowance exceeds cumulative cap')
        design = self.permit['execution_design']
        require(type(design) is dict and design.get('profiles') == profiles
                and type(design.get('max_workers')) is int and design['max_workers'] == max_workers
                and self.permit['execution_design_sha256'] == digest(design)
                and design.get('terminal_roster') == self.terminal_roster.record(), 'V4 design differs')
        v3._action_limits(design.get('action_limits'))
        self._guard_evidence()

    def _guard_evidence(self) -> None:
        require(canonical_payload(self.permit) == self._permit_bytes, 'V4 permit changed')
        require(set(REQUIRED_SOURCES) <= set(self.permit['sources']), 'V4 source closure incomplete')
        v3._source_map(self.permit['sources'])
        if self._mode == 'live':
            require(type(self.permit['qualification']) is dict, 'V4 live qualification missing')
            validate_qualification(self.permit['qualification'], self.permit['execution_design'], self.permit['sources'])
        else:
            require(self.permit['qualification'] is None, 'Fixture cannot claim live qualification')

    def _finalize_config(self, config: dict) -> dict:
        result = super()._finalize_config(config)
        child = self.terminal_config['child']
        require(self.contract['execution_contract_sha256'] == self.terminal_roster.execution_contract_sha256,
                'Child execution contract differs from prospective study roster')
        require(set(child['actors']) == self.actors
                and all(spec['context']['trajectory_id'] == child['trajectory'] for spec in self.task_specs.values()),
                'Financial roles or trajectories differ from terminal registration')
        return {**result, 'protocol': PROTOCOL, 'terminal_config': self.terminal_config}

    def _validate_terminal_config(self, db: sqlite3.Connection) -> None:
        row = db.execute('SELECT * FROM financial_terminal_config_v4 WHERE cohort=?', (self.cohort_id,)).fetchone()
        raw = canonical_payload(self.terminal_config)
        require(row is not None and row['config'] == raw and row['config_sha'] == sha(raw),
                'Missing or changed terminal registration')

    def assert_mutations_open(self, db: sqlite3.Connection) -> None:
        self._validate_terminal_config(db)
        if seal_row(db, self.cohort_id) is not None or self._external_seal is not None:
            raise CohortSealed()

    def _recover(self) -> None:
        if self._sealed_at_open:
            with self.ledger.atomic() as db:
                self._validate_sealed_census(db)
            self._verify_originals()
            return
        super()._recover()

    def _validate_sealed_census(self, db: sqlite3.Connection) -> sqlite3.Row:
        self._validate_terminal_config(db)
        row = seal_row(db, self.cohort_id)
        require(row is not None and row['snapshot_sha'] == sha(row['snapshot'])
                and row['receipt_sha'] == sha(row['receipt']), 'Missing or changed SQL seal')
        assert row is not None
        receipt = strict_loads(row['receipt'], max_bytes=2_100_000)
        require(receipt['seal_id'] == row['seal_id'] and receipt['snapshot_sha256'] == row['snapshot_sha']
                and canonical_payload(receipt['boundary']) == row['boundary']
                and receipt['financial_config_sha256'] == self.config_sha256
                and receipt['terminal_config'] == self.terminal_config
                and census(db, self.cohort_id, self.config_sha256) == row['snapshot'], 'Sealed accounting changed')
        require(not self.persistence_failed and (not self.failed_closed or receipt['terminal_status'] == 'stopped_failure'),
                'In-memory integrity fence invalidates seal proof')
        material = {key: value for key, value in receipt.items() if key != 'seal_id'}
        require(row['seal_id'] == digest(material), 'Seal identity differs')
        if self._external_seal is not None:
            require(self._external_seal == row['receipt'], 'External seal disagrees with SQL')
        return row

    def claim(self, actor: str, context: Any, work: Any, *, ttl: float = 60) -> Lease:
        with self.terminal_gate, self._active():
            with self.ledger.atomic() as db:
                self.assert_mutations_open(db)
            return super().claim(actor, context, work, ttl=ttl)

    def renew(self, actor: str, lease: Lease, *, ttl: float = 60) -> Lease:
        with self.terminal_gate, self._active():
            with self.ledger.atomic() as db:
                self.assert_mutations_open(db)
            return super().renew(actor, lease, ttl=ttl)

    def submit(self, actor: str, action: ActionRequest, lease: Lease) -> DispatchReply:
        with self.terminal_gate, self._active():
            self._actor(actor)
            require(type(action) is ActionRequest and type(lease) is Lease
                    and action.actor == lease.worker_id == actor, 'Exact actor/action/lease required')
            raw = canonical_payload({'action': to_dict(action), 'lease': asdict(lease)})
            with self.ledger.atomic() as db:
                row = self._row(db, actor, action.request_id)
                if row is not None:
                    require(row['identity'] == raw, 'Request identity conflict')
                    prior = self._reply(row)
                    if (actor, action.request_id) in self.persistence_failed:
                        return DispatchReply(action.request_id, prior.action_sha256, 'unknown',
                                             'state_persistence_failure', prior.binding)
                    if prior.state != 'waiting':
                        return prior
                self.assert_mutations_open(db)
            return super().submit(actor, action, lease)

    def _admission_reason(self, db: sqlite3.Connection, actor: str, action: ActionRequest) -> str | None:
        self.assert_mutations_open(db)
        return super()._admission_reason(db, actor, action)

    def _execute(self, actor: str, request_id: str) -> None:
        token = (actor, request_id)
        registered = False
        try:
            with self.terminal_gate:
                with self.ledger.atomic() as db:
                    self.assert_mutations_open(db)
                require(token not in self.active_executions, 'Duplicate active execution')
                self.active_executions.add(token)
                registered = True
        except BaseException:
            self._unknown(actor, request_id, 'execution_token_unavailable')
            raise
        finally:
            if not registered:
                # The base only releases its acquired slot if it is entered.
                self.slots.release()
        try:
            super()._execute(actor, request_id)
        finally:
            with self.terminal_gate:
                self.active_executions.remove(token)

    def _before_invoke(self, worker: OpenAIWorker, request: WorkerRequest) -> None:
        with self.terminal_gate:
            with self.ledger.atomic() as db:
                self.assert_mutations_open(db)
            require(bool(self.active_executions), 'Provider entry without active execution token')
        super()._before_invoke(worker, request)

    def _unknown(self, actor: str, request_id: str, reason: str, *, result_sha: str | None = None,
                 usage: int | None = None) -> DispatchReply | None:
        self.failed_closed = True
        try:
            with self.terminal_gate:
                with self.ledger.atomic() as db:
                    if seal_row(db, self.cohort_id) is not None or self._external_seal is not None:
                        self._halt(db)
                        self.persistence_failed.add((actor, request_id))
                        return None
                return super()._unknown(actor, request_id, reason, result_sha=result_sha, usage=usage)
        except Exception:
            self.persistence_failed.add((actor, request_id))
            return None

    def _verify_originals(self) -> None:
        with self.ledger.atomic() as db:
            self._audit_membership(db)
            actions = [(row['actor'], row['request_id'], row['state']) for row in db.execute(
                'SELECT * FROM financial_actions_v2 WHERE cohort=? ORDER BY actor,request_id', (self.cohort_id,))]
        for actor, request_id, state in actions:
            if state in {'completed', 'failed'}:
                self._verify_terminal(actor, request_id)

    def _quiescent(self, db: sqlite3.Connection, status: str) -> None:
        if self.active_executions or db.execute(
            "SELECT 1 FROM financial_actions_v2 WHERE cohort=? AND state IN ('pending','publication_pending') LIMIT 1",
            (self.cohort_id,)).fetchone():
            raise SealBusy('Active, queued or publication-pending work')
        self._audit_membership(db)
        for row in db.execute('''SELECT a.state,r.spent FROM financial_actions_v2 a
            JOIN reservations r ON r.id=a.reservation_id WHERE a.cohort=?''', (self.cohort_id,)):
            require(row['state'] in {'completed', 'failed', 'unknown'}, 'Unclassified admitted state')
            require(row['spent'] is not None or row['state'] == 'unknown', 'Unclassified conservative reservation')
            if row['state'] != 'completed' and status == 'completed':
                raise FinancialError('A failed or unknown action cannot produce successful closure')
        if status == 'completed':
            require(not self._halted(db) and not self.persistence_failed, 'Halted owner cannot claim successful closure')
        require(not db.execute("SELECT 1 FROM intents WHERE state='pending' LIMIT 1").fetchone(),
                'Pending Git transaction prevents terminal closure')

    def prepare_terminal_snapshot(self, request: ChildTerminalSealRequest) -> TerminalPreparation:
        with self.terminal_gate, self._active():
            self._guard_evidence()
            chain = self.checkpoint_chain
            chain.validate_boundary()
            terminal = authenticate_request(chain, self.terminal_config, request)
            self._verify_originals()  # Never nest these joined transactions.
            with self.ledger.atomic() as db:
                self.assert_mutations_open(db)
                self._quiescent(db, terminal['status'])
                raw = census(db, self.cohort_id, self.config_sha256)
            name = 'financial-' + sha(self.cohort_id.encode())[:16] + '-' + sha(raw) + '.prepare.json'
            preparation_raw = canonical_payload({'protocol': 'financial-seal-preparation-v1',
                'snapshot_sha256': sha(raw), 'financial_config_sha256': self.config_sha256,
                'request': request.record()})
            if chain.has(name):
                require(chain.read(name) == preparation_raw, 'Preparation identity conflict')
            else:
                chain.retain(name, preparation_raw)
            return TerminalPreparation(raw, request, name, chain.validate_boundary())

    def seal_terminal(self, preparation: TerminalPreparation) -> ChildTerminalSeal:
        require(type(preparation) is TerminalPreparation, 'Exact preparation required')
        with self.terminal_gate, self._active():
            self._guard_evidence()
            chain = self.checkpoint_chain
            terminal = authenticate_request(chain, self.terminal_config, preparation.request)
            self._verify_originals()
            material = {'protocol': 'financial-normal-seal-v4', 'financial_config_sha256': self.config_sha256,
                        'terminal_config': self.terminal_config, 'snapshot_sha256': sha(preparation.snapshot),
                        'terminal_status': terminal['status'],
                        'request': preparation.request.record(), 'preparation_name': preparation.record_name,
                        'boundary': {'commitment': prefix(preparation.boundary.commitment),
                                     'inventory_sha256': preparation.boundary.inventory_sha256}}
            raw = canonical_payload({**material, 'seal_id': digest(material)})
            expected_preparation = canonical_payload({'protocol': 'financial-seal-preparation-v1',
                'snapshot_sha256': sha(preparation.snapshot), 'financial_config_sha256': self.config_sha256,
                'request': preparation.request.record()})
            require(chain.read(preparation.record_name) == expected_preparation, 'Preparation original differs')
            with self.ledger.atomic() as db:
                prior = seal_row(db, self.cohort_id)
            if prior is None:
                require(chain.validate_boundary(expected=preparation.boundary.commitment) == preparation.boundary,
                        'Full preparation boundary differs')
            else:
                chain.validate_boundary()
            chain.require_current()
            with self.ledger.atomic() as db:
                prior = seal_row(db, self.cohort_id)
                if prior is None:
                    self.assert_mutations_open(db)
                    self._quiescent(db, terminal['status'])
                    require(census(db, self.cohort_id, self.config_sha256) == preparation.snapshot,
                            'Accounting changed after preparation')
                    db.execute('INSERT INTO financial_terminal_seals_v4 VALUES (?,?,?,?,?,?,?)',
                        (self.cohort_id, digest(material), preparation.snapshot, sha(preparation.snapshot),
                         raw, sha(raw), canonical_payload(material['boundary'])))
                    self._boundary('before_terminal_sql_commit', self.cohort_id)
                else:
                    require(prior['receipt'] == raw and prior['snapshot'] == preparation.snapshot,
                            'Immutable seal replay differs')
                    self._validate_sealed_census(db)
            self._boundary('after_terminal_sql_commit', self.cohort_id)
            chain.require_current()
            return self.publish_terminal_seal()

    def publish_terminal_seal(self) -> ChildTerminalSeal:
        """Explicit exact publication/recovery only; never retries a poisoned chain."""
        with self.terminal_gate, self._active():
            chain = self.checkpoint_chain
            chain.validate_boundary()
            self._verify_originals()
            with self.ledger.atomic() as db:
                row = self._validate_sealed_census(db)
                raw = bytes(row['receipt'])
            value = authenticate_seal(chain, raw)
            request = value['request']
            from .peer_financial_terminal_v1 import EvidenceReference
            authenticate_request(chain, self.terminal_config, ChildTerminalSealRequest(
                tuple(EvidenceReference(**item) for item in request['role_stops']),
                EvidenceReference(**request['terminal']), request['purpose']))
            name = seal_name(self.cohort_id)
            if chain.has(name):
                require(chain.read(name) == raw, 'Published seal differs')
            else:
                stored = value['boundary']
                observed = chain.validate_boundary(expected=PrefixCommitment(**stored['commitment']))
                require(observed.inventory_sha256 == stored['inventory_sha256'], 'Original preseal byte boundary differs')
                chain.retain(name, raw)
            boundary = chain.validate_boundary()
            self._external_seal = raw
            return ChildTerminalSeal(raw, name, boundary.commitment)

    def verified_terminal_seal(self, expected: PrefixCommitment) -> ChildTerminalSeal:
        with self.terminal_gate, self._active():
            self.checkpoint_chain.validate_boundary(expected=expected)
            self._verify_originals()
            with self.ledger.atomic() as db:
                row = self._validate_sealed_census(db)
                raw = bytes(row['receipt'])
            authenticate_seal(self.checkpoint_chain, raw)
            require(self.checkpoint_chain.read(seal_name(self.cohort_id)) == raw,
                    'SQL seal is not independently published')
            return ChildTerminalSeal(raw, seal_name(self.cohort_id), expected)

    def _bootstrap(self, expected_opening: int, recovery: bool) -> None:
        with self.ledger.atomic() as db:
            create_tables(db)
            db.execute("""CREATE TABLE IF NOT EXISTS financial_cohorts_v2 (
                cohort TEXT PRIMARY KEY, config BLOB NOT NULL, config_sha TEXT NOT NULL,
                historical BLOB NOT NULL, historical_sha TEXT NOT NULL, opening INTEGER NOT NULL,
                halted INTEGER NOT NULL DEFAULT 0, clock REAL NOT NULL DEFAULT 0)""")
            db.execute("""CREATE TABLE IF NOT EXISTS financial_tasks_v2 (
                cohort TEXT NOT NULL, task_id TEXT PRIMARY KEY, work BLOB NOT NULL)""")
            db.execute("""CREATE TABLE IF NOT EXISTS financial_requests_v2 (
                cohort TEXT NOT NULL, actor TEXT NOT NULL, request_id TEXT NOT NULL,
                identity BLOB NOT NULL, identity_sha TEXT NOT NULL, reply BLOB NOT NULL,
                reply_sha TEXT NOT NULL, PRIMARY KEY(cohort,actor,request_id))""")
            db.execute("""CREATE TABLE IF NOT EXISTS financial_actions_v2 (
                cohort TEXT NOT NULL, actor TEXT NOT NULL, action_id TEXT NOT NULL,
                request_id TEXT NOT NULL, task_id TEXT NOT NULL, binding BLOB NOT NULL,
                binding_sha TEXT NOT NULL, worker_request BLOB NOT NULL,
                reservation_id TEXT NOT NULL UNIQUE, executor_owner TEXT NOT NULL, state TEXT NOT NULL,
                PRIMARY KEY(cohort,actor,action_id), UNIQUE(cohort,actor,request_id))""")
            prior = db.execute("SELECT * FROM financial_cohorts_v2 WHERE cohort=?", (self.cohort_id,)).fetchone()
            if prior is not None:
                if not recovery:
                    raise FinancialError("Existing cohort requires explicit recovery")
                if prior["config"] != canonical_payload(self.config) or prior["config_sha"] != self.config_sha256:
                    raise FinancialError("Immutable cohort configuration changed")
                if prior["historical_sha"] != sha(prior["historical"]):
                    raise FinancialError("Historical adoption evidence changed")
                historical = strict_loads(prior["historical"], max_bytes=2_100_000)
                for table, rows in historical.items():
                    current = [dict(row) for row in db.execute(f"SELECT * FROM {table}")]
                    if any(row not in current for row in rows):
                        raise FinancialError("Historical ledger rows changed")
                self.failed_closed = bool(prior["halted"])
                self._validate_terminal_config(db)
                self._sealed_at_open = seal_row(db, self.cohort_id) is not None
                require(self._external_seal is None or self._sealed_at_open,
                        "Externally published seal is missing from SQL")
            else:
                if recovery:
                    raise FinancialError("Cannot recover an unregistered cohort")
                require(self._external_seal is None, "External sealed cohort cannot be re-enrolled")
                prior_sealed_tasks = self._prior_sealed_tasks(db)
                usage = db.execute("SELECT COALESCE(SUM(COALESCE(spent,amount)),0) FROM reservations").fetchone()[0]
                now = self._sample_time()
                if (usage != expected_opening
                        or db.execute("SELECT 1 FROM reservations WHERE spent IS NULL LIMIT 1").fetchone()
                        or any(row["status"] == "submitting" or row["id"] not in prior_sealed_tasks
                               for row in db.execute("SELECT * FROM tasks WHERE status='submitting' OR (status='claimed' AND expires>?)", (now,)))
                        or db.execute("SELECT 1 FROM intents WHERE state='pending' LIMIT 1").fetchone()
                        or db.execute("SELECT 1 FROM financial_actions_v2 WHERE state IN ('pending','publication_pending','unknown') LIMIT 1").fetchone()
                        or db.execute("SELECT 1 FROM financial_cohorts_v2 WHERE halted=1 LIMIT 1").fetchone()):
                    raise FinancialError("Fresh cohort requires exact quiescent opening usage")
                historical = {table: [dict(row) for row in db.execute(f"SELECT * FROM {table} ORDER BY rowid")]
                              for table in _BASE_SCHEMA}
                raw = canonical_payload(historical)
                db.execute("INSERT INTO financial_cohorts_v2 VALUES (?,?,?,?,?,?,0,0)",
                           (self.cohort_id, canonical_payload(self.config), self.config_sha256, raw, sha(raw), usage))
                terminal_raw = canonical_payload(self.terminal_config)
                db.execute("INSERT INTO financial_terminal_config_v4 VALUES (?,?,?)",
                           (self.cohort_id, terminal_raw, sha(terminal_raw)))
                for task_id, spec in self.task_specs.items():
                    self.ledger.add_task(task_id)
                    db.execute("INSERT INTO financial_tasks_v2 VALUES (?,?,?)",
                               (self.cohort_id, task_id, canonical_payload({"context": spec["context"], "work": spec["work"]})))
            if not self._sealed_at_open:
                self._now(db)
            actual = {row["task_id"]: strict_loads(row["work"]) for row in db.execute(
                "SELECT * FROM financial_tasks_v2 WHERE cohort=?", (self.cohort_id,))}
            if actual != {task: {"context": spec["context"], "work": spec["work"]} for task, spec in self.task_specs.items()}:
                raise FinancialError("Retained task membership differs")
            self._audit_membership(db)


    def _prior_sealed_tasks(self, db: sqlite3.Connection) -> set[str]:
        # Public checkpoint reads precede this transaction in __init__; this
        # helper consumes only that independently validated immutable prefix.
        allowed: set[str] = set()
        for child in self.terminal_roster.children[:self.terminal_config['child_index']]:
            raw = self._prior_publications.get(child.cohort)
            require(raw is not None, 'Previous sequential child has no independent seal publication')
            assert raw is not None
            row = seal_row(db, child.cohort)
            require(row is not None and row['receipt'] == raw and row['receipt_sha'] == sha(raw)
                    and row['snapshot_sha'] == sha(row['snapshot']), 'Previous child SQL seal differs')
            assert row is not None
            value = strict_loads(raw, max_bytes=2_100_000)
            financial = db.execute('SELECT config,config_sha FROM financial_cohorts_v2 WHERE cohort=?',
                                   (child.cohort,)).fetchone()
            require(row['snapshot_sha'] == value['snapshot_sha256'] and row['seal_id'] == value['seal_id']
                    and row['boundary'] == canonical_payload(value['boundary'])
                    and financial is not None and sha(financial['config']) == financial['config_sha']
                    and financial['config_sha'] == value['financial_config_sha256'], 'Prior SQL origin differs')
            prior_config = strict_loads(financial['config'], max_bytes=2_100_000)
            require(prior_config['contract']['execution_contract_sha256'] == self.terminal_roster.execution_contract_sha256,
                    'Prior child execution contract differs from prospective study roster')
            require(prior_config['contract']['ledger_identity'] == self._identity
                    and prior_config['terminal_config'] == value['terminal_config'], 'Prior financial config differs')
            require(value['terminal_config']['roster'] == self.terminal_roster.record()
                    and value['terminal_config']['child'] == child.record()
                    and census(db, child.cohort, value['financial_config_sha256']) == row['snapshot'],
                    'Previous child census changed')
            allowed.update(row['task_id'] for row in db.execute(
                'SELECT task_id FROM financial_tasks_v2 WHERE cohort=?', (child.cohort,)))
        return allowed
