"""Host-only, per-child financial closure. No private acceptance authority.

A receipt is data. Authority comes from the caller-owned checkpoint chain and
its independently retained expected head. SQL closure and external publication
are separate: a committed SQL seal closes admission even if publication fails.
"""
from __future__ import annotations

from contextlib import closing
from dataclasses import asdict, dataclass, field
import hashlib
import re
import sqlite3
from typing import Any

from .candidate_checkpoint_chain_v1 import BoundaryValidation, CheckpointChain, PrefixCommitment
from .peer_financial_authority_v2 import FinancialError, canonical_payload
from .peer_store_v1 import strict_loads

PROTOCOL = 'peer-financial-terminal-v1'
POLICY = 'six-sequential-children-96-distinct-roles-v1'
_HASH = re.compile(r'[0-9a-f]{64}\Z')
_NAME = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,95}\Z')


class SealBusy(FinancialError):
    """Admitted work is not quiescent. Does not halt or cancel it."""


class CohortSealed(FinancialError):
    def __init__(self) -> None:
        super().__init__('cohort_sealed')


def require(condition: bool, message: str) -> None:
    if not condition:
        raise FinancialError(message)


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def digest(value: Any) -> str:
    return sha(canonical_payload(value))


def checked_hash(value: str) -> str:
    require(type(value) is str and _HASH.fullmatch(value) is not None, 'Invalid SHA256')
    return value


def checked_name(value: str) -> str:
    require(type(value) is str and _NAME.fullmatch(value) is not None, 'Invalid registered name')
    return value


@dataclass(frozen=True)
class ChildRegistration:
    cohort: str
    trajectory: str
    actors: tuple[str, ...]

    def __post_init__(self) -> None:
        checked_name(self.cohort)
        checked_name(self.trajectory)
        require(type(self.actors) is tuple and len(self.actors) in (8, 20), 'A child has exactly 8 or 20 roles')
        require(len(set(self.actors)) == len(self.actors), 'Duplicate child role')
        for actor in self.actors:
            checked_name(actor)

    def record(self) -> dict:
        return {'cohort': self.cohort, 'trajectory': self.trajectory, 'actors': list(self.actors)}


@dataclass(frozen=True)
class TerminalRoster:
    execution_contract_sha256: str
    children: tuple[ChildRegistration, ...]
    policy: str = POLICY

    def __post_init__(self) -> None:
        checked_hash(self.execution_contract_sha256)
        require(self.policy == POLICY and type(self.children) is tuple and len(self.children) == 6,
                'Exactly six prospectively ordered sequential children required')
        for item in self.children:
            require(type(item) is ChildRegistration, 'Exact child registration required')
            ChildRegistration(item.cohort, item.trajectory, item.actors)
        require(len({item.cohort for item in self.children}) == 6
                and len({item.trajectory for item in self.children}) == 6, 'Duplicate child identity')
        actors = [actor for item in self.children for actor in item.actors]
        require(len(actors) == len(set(actors)) == 96, 'Exactly 96 distinct globally namespaced roles required')

    def record(self) -> dict:
        return {'protocol': PROTOCOL, 'execution_contract_sha256': self.execution_contract_sha256,
                'policy': self.policy, 'children': [item.record() for item in self.children]}

    @property
    def sha256(self) -> str:
        return digest(self.record())


@dataclass(frozen=True)
class EvidenceReference:
    name: str
    sha256: str

    def read(self, chain: CheckpointChain) -> dict:
        checked_hash(self.sha256)
        raw = chain.read(self.name)
        require(sha(raw) == self.sha256, 'Independent evidence digest differs')
        value = strict_loads(raw, max_bytes=2_100_000)
        require(type(value) is dict and canonical_payload(value) == raw, 'Evidence must be canonical object')
        return value


@dataclass(frozen=True)
class ChildTerminalSealRequest:
    role_stops: tuple[EvidenceReference, ...]
    terminal: EvidenceReference
    purpose: str = 'normal-financial-terminal-v1'

    def record(self) -> dict:
        require(self.purpose == 'normal-financial-terminal-v1' and type(self.role_stops) is tuple,
                'Wrong terminal purpose or stop roster')
        require(all(type(item) is EvidenceReference for item in self.role_stops)
                and type(self.terminal) is EvidenceReference, 'Typed original references required')
        return {'role_stops': [asdict(item) for item in self.role_stops],
                'terminal': asdict(self.terminal), 'purpose': self.purpose}


@dataclass(frozen=True)
class TerminalPreparation:
    snapshot: bytes
    request: ChildTerminalSealRequest
    record_name: str
    boundary: BoundaryValidation


@dataclass(frozen=True)
class ChildTerminalSeal:
    raw: bytes
    publication_name: str
    commitment: PrefixCommitment
    acceptance_authority: bool = field(default=False, init=False)

    @property
    def seal_id(self) -> str:
        return str(strict_loads(self.raw, max_bytes=2_100_000)['seal_id'])


def prefix(value: PrefixCommitment) -> dict:
    require(type(value) is PrefixCommitment, 'Exact external prefix required')
    return asdict(PrefixCommitment(**asdict(value)))


def seal_name(cohort: str) -> str:
    return 'financial-' + sha(checked_name(cohort).encode()) + '.seal.json'


def config_record(roster: TerminalRoster, cohort: str, chain: CheckpointChain) -> dict:
    require(type(roster) is TerminalRoster and type(chain) is CheckpointChain,
            'Typed prospective roster and caller-owned exact checkpoint chain required')
    TerminalRoster(roster.execution_contract_sha256, roster.children, roster.policy)
    matches = [i for i, child in enumerate(roster.children) if child.cohort == cohort]
    require(len(matches) == 1, 'Cohort is absent from prospective study')
    return {'protocol': PROTOCOL, 'roster': roster.record(), 'roster_sha256': roster.sha256,
            'child_index': matches[0], 'child': roster.children[matches[0]].record(),
            'checkpoint_context_sha256': chain.commitment.context_sha256}


def create_tables(db: sqlite3.Connection) -> None:
    tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    expected = {'financial_terminal_config_v4', 'financial_terminal_seals_v4'}
    require(not (tables & expected) or expected <= tables, 'Partial terminal schema')
    db.execute('''CREATE TABLE IF NOT EXISTS financial_terminal_config_v4 (
        cohort TEXT PRIMARY KEY, config BLOB NOT NULL, config_sha TEXT NOT NULL)''')
    db.execute('''CREATE TABLE IF NOT EXISTS financial_terminal_seals_v4 (
        cohort TEXT PRIMARY KEY, seal_id TEXT NOT NULL UNIQUE, snapshot BLOB NOT NULL,
        snapshot_sha TEXT NOT NULL, receipt BLOB NOT NULL, receipt_sha TEXT NOT NULL,
        boundary BLOB NOT NULL)''')
    for table, columns in (
        ('financial_terminal_config_v4', ('cohort', 'config', 'config_sha')),
        ('financial_terminal_seals_v4', ('cohort', 'seal_id', 'snapshot', 'snapshot_sha', 'receipt', 'receipt_sha', 'boundary')),
    ):
        info = list(db.execute('PRAGMA table_info(' + table + ')'))
        require([row[1] for row in info] == list(columns)
                and [row[2] for row in info] == ['BLOB' if name in {'config', 'snapshot', 'receipt', 'boundary'} else 'TEXT'
                                              for name in columns], 'Terminal schema differs')


def seal_row(db: sqlite3.Connection, cohort: str) -> sqlite3.Row | None:
    return db.execute('SELECT * FROM financial_terminal_seals_v4 WHERE cohort=?', (cohort,)).fetchone()


def assert_open(db: sqlite3.Connection, cohort: str) -> None:
    if seal_row(db, cohort) is not None:
        raise CohortSealed()


def _rows(db: sqlite3.Connection, query: str, cohort: str) -> list[dict]:
    # Every cell is explicitly typed; arbitrary BLOBs cannot collide with JSON.
    result = []
    for row in db.execute(query, (cohort,)):
        result.append({key: {'blob': value.hex()} if type(value) is bytes else value
                       for key, value in dict(row).items()})
    return sorted(result, key=canonical_payload)


def census(db: sqlite3.Connection, cohort: str, config_sha256: str) -> bytes:
    queries = {
        'cohort': 'SELECT * FROM financial_cohorts_v2 WHERE cohort=?',
        'terminal_config': 'SELECT * FROM financial_terminal_config_v4 WHERE cohort=?',
        'membership': 'SELECT * FROM financial_tasks_v2 WHERE cohort=?',
        'requests': 'SELECT * FROM financial_requests_v2 WHERE cohort=?',
        'actions': 'SELECT * FROM financial_actions_v2 WHERE cohort=?',
        'tasks': 'SELECT t.* FROM tasks t JOIN financial_tasks_v2 f ON f.task_id=t.id WHERE f.cohort=?',
        'reservations': 'SELECT r.* FROM reservations r JOIN financial_tasks_v2 f ON f.task_id=r.task_id WHERE f.cohort=?',
        'dependencies': 'SELECT d.* FROM dependencies d JOIN financial_tasks_v2 f ON f.task_id=d.task_id WHERE f.cohort=?',
    }
    tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if 'financial_rpc_config_v2' in tables:
        queries['rpc_config'] = 'SELECT * FROM financial_rpc_config_v2 WHERE cohort=?'
        queries['rpc_requests'] = 'SELECT * FROM financial_rpc_requests_v2 WHERE cohort=?'
    rows = {name: _rows(db, query, cohort) for name, query in queries.items()}
    rows.setdefault('rpc_config', [])
    rows.setdefault('rpc_requests', [])
    return canonical_payload({'protocol': PROTOCOL, 'financial_config_sha256': config_sha256, 'rows': rows})


def authenticate_request(chain: CheckpointChain, config: dict, request: ChildTerminalSealRequest) -> dict:
    request.record()
    actors = config['child']['actors']
    require(len(request.role_stops) == len(actors), 'Missing role stop originals')
    require(len({item.name for item in request.role_stops}) == len(actors), 'Duplicate stop reference')
    for actor, reference in zip(actors, request.role_stops, strict=True):
        value = reference.read(chain)
        require(set(value) == {'protocol', 'roster_sha256', 'cohort', 'trajectory', 'actor', 'stopped'}
                and value == {'protocol': 'financial-role-stop-v1', 'roster_sha256': config['roster_sha256'],
                              'cohort': config['child']['cohort'], 'trajectory': config['child']['trajectory'],
                              'actor': actor, 'stopped': True}
                and value['stopped'] is True, 'Role stop differs from exact prospective identity')
    terminal = request.terminal.read(chain)
    require(set(terminal) == {'protocol', 'roster_sha256', 'cohort', 'trajectory', 'status', 'final_source_sha256'}
            and terminal['protocol'] == 'financial-child-terminal-v1'
            and terminal['roster_sha256'] == config['roster_sha256']
            and terminal['cohort'] == config['child']['cohort']
            and terminal['trajectory'] == config['child']['trajectory']
            and terminal['status'] in {'completed', 'stopped_failure'}, 'Child terminal identity differs')
    checked_hash(terminal['final_source_sha256'])
    return terminal


def verified_study_barrier(roster: TerminalRoster, chain: CheckpointChain,
                           expected: PrefixCommitment, receipts: tuple[ChildTerminalSeal, ...], *,
                           existing_ledger_path: Any, expected_ledger_identity: dict) -> dict:
    """Authenticate one original-ledger snapshot; never a private quality verdict."""
    chain.validate_boundary(expected=expected)
    require(type(receipts) is tuple and len(receipts) == 6, 'All six original child publications required')
    from .peer_financial_authority_v2 import ledger_identity
    require(ledger_identity(existing_ledger_path) == expected_ledger_identity, 'Study ledger identity differs')
    # One explicit read transaction observes all six cohorts in one SQLite
    # snapshot. Path identity is checked around that observation; this does not
    # claim immunity to a transient host-controlled replace/restore (ABA).
    with closing(sqlite3.connect('file:' + str(existing_ledger_path) + '?mode=ro', uri=True)) as db:
        db.row_factory = sqlite3.Row
        require(ledger_identity(existing_ledger_path) == expected_ledger_identity,
                'Study ledger identity changed while opening')
        db.execute('BEGIN')
        ids = []
        for child, receipt in zip(roster.children, receipts, strict=True):
            require(type(receipt) is ChildTerminalSeal and receipt.publication_name == seal_name(child.cohort)
                    and chain.read(receipt.publication_name) == receipt.raw, 'Original child publication differs')
            value = authenticate_seal(chain, receipt.raw)
            row = seal_row(db, child.cohort)
            financial = db.execute('SELECT config,config_sha FROM financial_cohorts_v2 WHERE cohort=?',
                                   (child.cohort,)).fetchone()
            require(row is not None and row['receipt'] == receipt.raw and row['receipt_sha'] == sha(receipt.raw)
                    and row['seal_id'] == value['seal_id'] and row['snapshot_sha'] == sha(row['snapshot'])
                    and row['snapshot_sha'] == value['snapshot_sha256']
                    and row['boundary'] == canonical_payload(value['boundary'])
                    and financial is not None and sha(financial['config']) == financial['config_sha']
                    and financial['config_sha'] == value['financial_config_sha256']
                    and census(db, child.cohort, financial['config_sha']) == row['snapshot'],
                    'Original SQL seal or financial census differs')
            config_value = strict_loads(financial['config'], max_bytes=2_100_000)
            require(config_value['contract']['execution_contract_sha256'] == roster.execution_contract_sha256,
                    'Child execution contract differs from prospective study roster')
            require(config_value['contract']['ledger_identity'] == expected_ledger_identity
                    and config_value['terminal_config'] == value['terminal_config'], 'Financial origin differs')
            config = value['terminal_config']
            require(config['roster'] == roster.record() and config['child'] == child.record(), 'Global child identity differs')
            request = value['request']
            authenticate_request(chain, config, ChildTerminalSealRequest(
                tuple(EvidenceReference(**item) for item in request['role_stops']),
                EvidenceReference(**request['terminal']), request['purpose']))
            ids.append(value['seal_id'])
        require(len(set(ids)) == 6, 'Duplicate original seal')
        require(ledger_identity(existing_ledger_path) == expected_ledger_identity,
                'Study ledger identity changed during observation')
        db.rollback()
    chain.require_current()
    return {'protocol': PROTOCOL, 'roster_sha256': roster.sha256, 'seal_ids': ids,
            'role_count': 96, 'checkpoint': prefix(expected), 'acceptance_authority': False}


def authenticate_seal(chain: CheckpointChain, raw: bytes) -> dict:
    """Authenticate receipt syntax/identity and its original retained inputs.

    This alone is not SQL-origin proof; a financial owner or study barrier also
    compares the immutable SQL row and exact current per-child accounting census.
    """
    value = strict_loads(raw, max_bytes=2_100_000)
    require(type(value) is dict and set(value) == {'protocol', 'financial_config_sha256', 'terminal_config',
            'snapshot_sha256', 'request', 'preparation_name', 'boundary', 'seal_id', 'terminal_status'}
            and canonical_payload(value) == raw and value['protocol'] == 'financial-normal-seal-v4',
            'Invalid normal seal schema')
    checked_hash(value['financial_config_sha256'])
    checked_hash(value['snapshot_sha256'])
    require(value['seal_id'] == digest({key: item for key, item in value.items() if key != 'seal_id'}),
            'Normal seal identity differs')
    boundary = value['boundary']
    require(type(boundary) is dict and set(boundary) == {'commitment', 'inventory_sha256'}, 'Invalid seal boundary')
    committed = PrefixCommitment(**boundary['commitment'])
    checked_hash(boundary['inventory_sha256'])
    require(committed.context_sha256 == chain.commitment.context_sha256
            and committed.sequence <= chain.commitment.sequence, 'Seal boundary belongs to a foreign or future chain')
    config = value['terminal_config']
    require(type(config) is dict and set(config) == {'protocol', 'roster', 'roster_sha256', 'child_index',
            'child', 'checkpoint_context_sha256'} and config['protocol'] == PROTOCOL
            and config['checkpoint_context_sha256'] == committed.context_sha256
            and config['roster_sha256'] == digest(config['roster']), 'Invalid seal terminal registration')
    request = value['request']
    require(type(request) is dict and set(request) == {'role_stops', 'terminal', 'purpose'}, 'Invalid seal request')
    terminal = authenticate_request(chain, config, ChildTerminalSealRequest(
        tuple(EvidenceReference(**item) for item in request['role_stops']),
        EvidenceReference(**request['terminal']), request['purpose']))
    require(value['terminal_status'] == terminal['status'], 'Seal terminal status differs from original')
    preparation_raw = canonical_payload({'protocol': 'financial-seal-preparation-v1',
        'snapshot_sha256': value['snapshot_sha256'], 'financial_config_sha256': value['financial_config_sha256'],
        'request': request})
    require(chain.read(value['preparation_name']) == preparation_raw, 'Original sealed preparation differs')
    return value
