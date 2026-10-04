"""Read-only original financial evidence reconciliation for a V4 rehearsal.

This component never qualifies live entry. Its caller must authenticate these
references against the prospective controller contract and independent final
checkpoint, then verify processes, history, source, barrier and private tests.
No financial/mesh/journal constructor or recovery method is invoked here.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
import hashlib
import os
import stat
from pathlib import Path
import sqlite3
import threading
from types import SimpleNamespace
from typing import Any, Iterator, cast

from . import candidate_http_journal_v3 as stable
from . import peer_mesh_finance_v2 as payload_module
from .peer_financial_authority_v2 import CumulativeAuthorityV2, FinancialError, MAX_BYTES, canonical_payload, ledger_identity
from .peer_financial_authority_v4 import PROTOCOL as FINANCIAL_PROTOCOL
from .peer_financial_terminal_v1 import census, digest, require, sha, seal_row
from .peer_mesh_finance_v2 import MeshFinancePayloads, MAX_RESULTS, PROTOCOL as PAYLOAD_PROTOCOL
from .peer_mesh_store_v2 import MeshLimits, MeshStore
from .peer_project_contract_v2 import DispatchBinding, DispatchReply, EvidenceRef, encode, identity
from .peer_store_v1 import strict_loads
from .verification_journal import RequestJournal

PROTOCOL = 'financial-rehearsal-originals-v1'
MAX_CALLS = 1024
MAX_SQLITE_ROW_BYTES = 4 * MAX_BYTES
MAX_SCHEMA_OBJECTS = 128
MAX_SCHEMA_BYTES = 16_384

# Only these tables are read by the reused financial methods and census. Other
# historical ledger tables remain outside this component's per-cohort scan.
_LEDGER_COLUMNS = {
    'settings': ('key', 'value'),
    'tasks': ('id', 'status', 'worker', 'epoch', 'expires', 'accepted_commit', 'intent_id'),
    'dependencies': ('task_id', 'prerequisite'),
    'reservations': ('id', 'task_id', 'epoch', 'amount', 'spent', 'state'),
    'financial_cohorts_v2': ('cohort', 'config', 'config_sha', 'historical', 'historical_sha', 'opening', 'halted', 'clock'),
    'financial_tasks_v2': ('cohort', 'task_id', 'work'),
    'financial_requests_v2': ('cohort', 'actor', 'request_id', 'identity', 'identity_sha', 'reply', 'reply_sha'),
    'financial_actions_v2': ('cohort', 'actor', 'action_id', 'request_id', 'task_id', 'binding', 'binding_sha',
                             'worker_request', 'reservation_id', 'executor_owner', 'state'),
    'financial_terminal_config_v4': ('cohort', 'config', 'config_sha'),
    'financial_terminal_seals_v4': ('cohort', 'seal_id', 'snapshot', 'snapshot_sha', 'receipt', 'receipt_sha', 'boundary'),
    'financial_rpc_config_v2': ('cohort', 'config', 'config_sha', 'request_count'),
    'financial_rpc_requests_v2': ('cohort', 'actor', 'request_id', 'request', 'request_sha', 'receipt', 'receipt_sha'),
}
_PAYLOAD_COLUMNS = {'registration': ('id', 'config', 'count'),
                    'results': ('principal', 'sha', 'command', 'payload', 'reference')}
_BLOB_COLUMNS = {'config', 'historical', 'work', 'identity', 'reply', 'binding', 'worker_request',
                 'snapshot', 'receipt', 'boundary', 'request', 'payload', 'reference'}
_INTEGER_COLUMNS = {'value', 'epoch', 'amount', 'spent', 'opening', 'halted', 'request_count', 'count'}
_REAL_COLUMNS = {'expires', 'clock'}
_NULLABLE_COLUMNS = {'worker', 'expires', 'accepted_commit', 'intent_id', 'spent', 'reference'}


def _schema_bound(db: sqlite3.Connection) -> None:
    require(db.execute('SELECT count(*) FROM (SELECT 1 FROM sqlite_master LIMIT ?)',
                       (MAX_SCHEMA_OBJECTS + 1,)).fetchone()[0] <= MAX_SCHEMA_OBJECTS,
            'Oversized original database schema')
    require(not db.execute('''SELECT 1 FROM sqlite_master WHERE
        length(CAST(name AS BLOB))>256 OR length(CAST(tbl_name AS BLOB))>256
        OR length(CAST(sql AS BLOB))>? LIMIT 1''', (MAX_SCHEMA_BYTES,)).fetchone(),
        'Oversized original schema field')


def _table_schema(db: sqlite3.Connection, table: str, columns: tuple[str, ...]) -> None:
    row = db.execute('SELECT type,sql FROM sqlite_master WHERE name=?', (table,)).fetchone()
    require(row is not None and row['type'] == 'table' and type(row['sql']) is str
            and row['sql'].lstrip().upper().startswith('CREATE TABLE'), 'Original table schema missing or indirect')
    info = db.execute('SELECT name,type FROM pragma_table_info(?) LIMIT ?', (table, len(columns) + 1)).fetchall()
    def declared(name: str) -> str:
        if name in _BLOB_COLUMNS:
            return 'BLOB'
        if name in _INTEGER_COLUMNS or (table == 'registration' and name == 'id'):
            return 'INTEGER'
        return 'REAL' if name in _REAL_COLUMNS else 'TEXT'
    require([(item['name'], item['type']) for item in info] == [(name, declared(name)) for name in columns],
            'Original table columns differ')


def _table_bound(db: sqlite3.Connection, table: str, columns: tuple[str, ...], *,
                 where: str, parameters: tuple[Any, ...], maximum: int,
                 aggregate: int | None = None) -> None:
    # Table, column and predicate text are internal constants; caller data are
    # parameters. Scalar SQL guards run before any BLOB is fetched into Python.
    require(db.execute(f'SELECT count(*) FROM (SELECT 1 FROM {table} WHERE {where} LIMIT ?)',
                       (*parameters, maximum + 1)).fetchone()[0] <= maximum,
            'Oversized original row census: ' + table)
    invalid = []
    lengths = []
    for name in columns:
        kind, bound = ('blob', MAX_BYTES) if name in _BLOB_COLUMNS else ('text', 4096)
        if name in _INTEGER_COLUMNS or (table == 'registration' and name == 'id'):
            kind, bound = 'integer', 20
        elif name in _REAL_COLUMNS:
            kind, bound = 'real', 32
        if name == 'worker_request':
            bound = 600_000
        nullable = name in _NULLABLE_COLUMNS or (table == 'financial_rpc_requests_v2' and name in {'receipt', 'receipt_sha'})
        types = f"'{kind}','null'" if nullable else f"'{kind}'"
        invalid.append(f"typeof({name}) NOT IN ({types}) OR length(CAST({name} AS BLOB))>{bound}")
        lengths.append(f'coalesce(length(CAST({name} AS BLOB)),0)')
    require(not db.execute(f'SELECT 1 FROM {table} WHERE ({where}) AND ({" OR ".join(invalid)}) LIMIT 1',
                           parameters).fetchone(), 'Oversized or mistyped original cell: ' + table)
    if aggregate is not None:
        require(db.execute(f'SELECT coalesce(sum({"+".join(lengths)}),0) FROM {table} WHERE {where}',
                           parameters).fetchone()[0] <= aggregate, 'Oversized original aggregate: ' + table)


def _financial_bounds(ledger: sqlite3.Connection, index: sqlite3.Connection, cohort: str) -> None:
    tables = {row[0] for row in ledger.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    rpc = {'financial_rpc_config_v2', 'financial_rpc_requests_v2'}
    require(not (rpc & tables) or rpc <= tables, 'Partial original RPC schema')
    for table, columns in _LEDGER_COLUMNS.items():
        if table not in rpc or table in tables:
            _table_schema(ledger, table, columns)
    for table, columns in _LEDGER_COLUMNS.items():
        if table in rpc and table not in tables:
            continue
        maximum = MAX_CALLS
        where = 'cohort=?'
        parameters: tuple[Any, ...] = (cohort,)
        if table == 'settings':
            where, parameters, maximum = "key='budget'", (), 1
        elif table in {'tasks', 'dependencies', 'reservations'}:
            member = 'id' if table == 'tasks' else 'task_id'
            where = f'{member} IN (SELECT task_id FROM financial_tasks_v2 WHERE cohort=?)'
            if table == 'dependencies':
                maximum = MAX_CALLS * MAX_CALLS
        elif table in {'financial_cohorts_v2', 'financial_terminal_config_v4', 'financial_terminal_seals_v4',
                       'financial_rpc_config_v2'}:
            maximum = 1
        elif table == 'financial_rpc_requests_v2':
            maximum = 8 * MAX_CALLS
        # The seal table includes the encoded census itself and is not included
        # in that census. All other scoped rows must fit its existing byte cap.
        aggregate = MAX_SQLITE_ROW_BYTES if table == 'financial_terminal_seals_v4' else MAX_BYTES
        _table_bound(ledger, table, columns, where=where, parameters=parameters,
                     maximum=maximum, aggregate=aggregate)
    payload_tables = {row[0] for row in index.execute("SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'")}
    require(payload_tables == set(_PAYLOAD_COLUMNS), 'Foreign original payload schema')
    for table, columns in _PAYLOAD_COLUMNS.items():
        _table_schema(index, table, columns)
        _table_bound(index, table, columns, where='1', parameters=(), maximum=1 if table == 'registration' else MAX_RESULTS)


@dataclass(frozen=True)
class OriginalFile:
    """An expected original supplied by an independently authenticated manifest."""
    path: str
    sha256: str
    size: int

    def read(self) -> bytes:
        path = Path(self.path)
        require(path.is_absolute() and path.resolve() == path and type(self.size) is int
                and 0 <= self.size <= stable.MAX_RECORD_BYTES, 'Invalid bounded original file')
        raw = stable.read(path, max_bytes=stable.MAX_RECORD_BYTES)
        require(len(raw) == self.size and sha(raw) == self.sha256, 'Original file digest or length differs')
        return raw


@dataclass(frozen=True)
class FinancialOriginals:
    ledger_identity: dict
    cohort: str
    config: dict
    config_sha256: str
    payload_index_identity: dict
    payload_config: dict
    mesh_database_identity: dict
    mesh_identity: dict
    journal_files: tuple[OriginalFile, ...]
    dispatches: tuple[DispatchBinding, ...]
    terminal_replies: tuple[DispatchReply, ...]


@dataclass(frozen=True)
class FinancialAudit:
    cohort: str
    config_sha256: str
    seal_id: str
    admitted: int
    known_failures: int
    spent_micro_usd: int
    opening_micro_usd: int
    global_cap_micro_usd: int
    dispatch_sha256: tuple[str, ...]
    live_qualification: bool = field(default=False, init=False)


@contextmanager
def readonly_database(expected: dict) -> Iterator[sqlite3.Connection]:
    """One original-inode SQLite read snapshot, with bounded lock waiting.

    Rechecks detect persistent pathname replacement, not transient host ABA.
    SQLite mode=ro and no constructors prevent schema, recovery or money writes.
    """
    require(type(expected) is dict and set(expected) == {'path', 'device', 'inode'}, 'Original database identity required')
    path = Path(expected['path'])
    require(ledger_identity(path) == expected, 'Original database identity differs')
    for suffix in ('-wal', '-shm', '-journal'):
        require(not Path(str(path) + suffix).is_symlink(), 'Indirect SQLite companion refused')
    db = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=2)
    try:
        db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, MAX_SQLITE_ROW_BYTES)
        require(ledger_identity(path) == expected, 'Database replaced during open')
        db.row_factory = sqlite3.Row
        db.execute('BEGIN')
        _schema_bound(db)
        yield db
        require(ledger_identity(path) == expected, 'Database replaced during observation')
        db.rollback()
    except sqlite3.DatabaseError as error:
        raise FinancialError('Original SQLite observation failed its read bounds or integrity checks') from error
    finally:
        db.close()


class _JoinedRead:
    """Only the read transaction seam used by frozen terminal verification."""
    def __init__(self, db: sqlite3.Connection):
        self.db = db

    @contextmanager
    def atomic(self) -> Iterator[sqlite3.Connection]:
        yield self.db


class _ReadPayloads(MeshFinancePayloads):
    def __init__(self, originals: FinancialOriginals, index: sqlite3.Connection, mesh: sqlite3.Connection):
        config = originals.payload_config
        expected = {'protocol': PAYLOAD_PROTOCOL, 'root': str(Path(originals.payload_index_identity['path']).parent),
                    'mesh_identity': originals.mesh_identity,
                    'principals': sorted({actor for spec in originals.config['contract']['task_specs'] for actor in spec['actors']}),
                    'source_sha256': sha(Path(payload_module.__file__).read_bytes()),
                    'max_results': MAX_RESULTS}
        require(canonical_payload(config) == canonical_payload(expected), 'Original payload configuration differs')
        require(originals.mesh_database_identity['path'] == str(Path(originals.mesh_identity['root']) / 'mesh.sqlite'),
                'Mesh original path differs from its registration')
        require(originals.mesh_identity['cohort_id'] == originals.cohort
                and originals.mesh_identity['execution_contract_sha256'] == originals.config['contract']['execution_contract_sha256'],
                'Finance mesh execution binding differs')
        self.root = Path(config['root'])
        self.config = config
        self.config_bytes = canonical_payload(config)
        self.config_sha256 = sha(self.config_bytes)
        self.principals = frozenset(config['principals'])
        self.lock = threading.RLock()
        self.closed = False
        self.db = index
        # Reuse audited read methods only; never initialize MeshStore's writer,
        # ownership lock, schema, subscriptions, transport or recovery state.
        store = object.__new__(MeshStore)
        store.db = mesh
        store.identity = originals.mesh_identity
        store.node_id = originals.mesh_identity['node_id']
        store.roster = tuple(originals.mesh_identity['roster'])
        store.limits = MeshLimits(**originals.mesh_identity['limits'])
        store.lock = threading.RLock()
        store.closed = False
        store._arrived_cache = []
        store.audit()
        self._readonly_store = store
        self.mesh = cast(Any, SimpleNamespace(node_id=store.node_id,
            config=SimpleNamespace(cohort_id=originals.cohort,
                                   execution_contract_sha256=originals.config['contract']['execution_contract_sha256']),
            arrived=store.arrived))
        self._audit()

    def _resolve(self, ref: EvidenceRef) -> bytes:
        require(ref in self._readonly_store.arrived(), 'Exact original payload reference never arrived')
        raw = self._readonly_store.resolve(ref)
        require(type(raw) is bytes, 'Original arrived payload is incomplete')
        assert isinstance(raw, bytes)
        return raw

    def put_owned(self, principal: str, data: bytes) -> str:
        raise FinancialError('Read-only rehearsal audit cannot publish')


class _JournalPath:
    """Only path identity and a bounded authenticated consumed-byte seam.

    Frozen journal decoding calls read_bytes on this facade. It never receives
    an ordinary Path that could reopen unchecked bytes, and no module globals
    are patched. Each consumed file is authenticated directly, not by metadata.
    """
    def __init__(self, path: Path, files: dict[str, OriginalFile]):
        self.path, self.files = path, files

    def __truediv__(self, name: str) -> _JournalPath:
        return _JournalPath(self.path / name, self.files)

    def __str__(self) -> str:
        return str(self.path)

    def with_suffix(self, suffix: str) -> Path:
        return self.path.with_suffix(suffix)

    def read_bytes(self) -> bytes:
        require(str(self.path) in self.files, 'Consumed journal file absent from independently anchored inventory')
        return self.files[str(self.path)].read()


class _FinancialRead(CumulativeAuthorityV2):
    def __init__(self, originals: FinancialOriginals, db: sqlite3.Connection, payloads: _ReadPayloads):
        # This is a private method-reuse view, never a dispatch-capable owner.
        self.ledger = cast(Any, _JoinedRead(db))
        self.config = originals.config
        self.config_sha256 = originals.config_sha256
        self.contract = self.config['contract']
        self.cohort_id = originals.cohort
        require(type(originals.config['profiles']) is dict, 'Original profiles must be a mapping')
        self.profiles = cast(dict[str, dict], originals.config['profiles'])
        self.payloads = payloads
        self.failed_closed = bool(db.execute('SELECT halted FROM financial_cohorts_v2 WHERE cohort=?',
                                             (self.cohort_id,)).fetchone()[0])
        self.task_specs = {self.task_id_from_spec(spec): spec for spec in self.contract['task_specs']}
        journal = object.__new__(RequestJournal)
        journal.root = cast(Any, _JournalPath(Path(self.contract['journal_root']),
            {item.path: item for item in originals.journal_files}))
        self.journal = journal

    def task_id_from_spec(self, spec: dict) -> str:
        return 'financial-v2-' + digest({'context': spec['context'], 'work': spec['work']})

    def _unknown(self, *args: Any, **kwargs: Any) -> Any:
        raise FinancialError('Read-only rehearsal audit cannot recover or relabel an outcome')


def audit_financial_originals(originals: FinancialOriginals) -> FinancialAudit:
    """Reconcile actual immutable V4 SQL, request journals and mesh publications.

    The manifest's exact dispatch list must come from the authenticated original
    controller and role journals; this function proves its equality to financial
    admission. It does not prove controller execution or qualification by itself.
    """
    require(type(originals) is FinancialOriginals, 'Typed financial originals required')
    config = originals.config
    require(type(config) is dict and config.get('protocol') == FINANCIAL_PROTOCOL
            and config.get('mode') == 'fixture' and config.get('observation_kind') == 'simulated'
            and digest(config) == originals.config_sha256, 'Exact V4 fixture financial configuration required')
    contract = config['contract']
    require(contract['cohort_id'] == originals.cohort and contract['ledger_identity'] == originals.ledger_identity
            and contract['execution_contract_sha256'] == config['terminal_config']['roster']['execution_contract_sha256'],
            'Original financial cohort/study/ledger binding differs')
    require(type(originals.dispatches) is tuple and len(originals.dispatches) <= MAX_CALLS
            and all(type(item) is DispatchBinding for item in originals.dispatches), 'Invalid bounded dispatch census')
    require(type(originals.terminal_replies) is tuple and len(originals.terminal_replies) == len(originals.dispatches)
            and all(type(reply) is DispatchReply and reply.binding is not None for reply in originals.terminal_replies),
            'Independent original role terminal reply census required')
    require(type(originals.journal_files) is tuple and len(originals.journal_files) <= 4 * MAX_CALLS
            and all(type(item) is OriginalFile for item in originals.journal_files), 'Invalid original journal inventory')
    files = {item.path: item for item in originals.journal_files}
    require(len(files) == len(originals.journal_files), 'Duplicate original journal path')
    for item in files.values():
        item.read()
    with readonly_database(originals.ledger_identity) as ledger, \
            readonly_database(originals.payload_index_identity) as index, \
            readonly_database(originals.mesh_database_identity) as mesh:
        _financial_bounds(ledger, index, originals.cohort)
        cohort = ledger.execute('SELECT * FROM financial_cohorts_v2 WHERE cohort=?', (originals.cohort,)).fetchone()
        require(cohort is not None and cohort['config'] == canonical_payload(config)
                and cohort['config_sha'] == originals.config_sha256 and cohort['halted'] == 0,
                'Original cohort configuration is missing, changed or halted')
        terminal = seal_row(ledger, originals.cohort)
        require(terminal is not None and terminal['snapshot_sha'] == sha(terminal['snapshot'])
                and terminal['receipt_sha'] == sha(terminal['receipt'])
                and census(ledger, originals.cohort, originals.config_sha256) == terminal['snapshot'],
                'Original financial seal/census changed')
        assert terminal is not None
        seal = strict_loads(terminal['receipt'], max_bytes=2_100_000)
        require(seal['snapshot_sha256'] == terminal['snapshot_sha'] and seal['seal_id'] == terminal['seal_id']
                and seal['financial_config_sha256'] == originals.config_sha256
                and digest({key: value for key, value in seal.items() if key != 'seal_id'}) == seal['seal_id'],
                'Seal does not bind original financial snapshot')
        request_count = ledger.execute('SELECT COUNT(*) FROM financial_requests_v2 WHERE cohort=?',
                                       (originals.cohort,)).fetchone()[0]
        require(request_count <= MAX_CALLS, 'Oversized original request census')
        payloads = _ReadPayloads(originals, index, mesh)
        view = _FinancialRead(originals, ledger, payloads)
        view._audit_membership(ledger)
        requested = {(item.action.actor, item.action.request_id): item for item in originals.dispatches}
        require(len(requested) == len(originals.dispatches), 'Duplicate controller dispatch identity')
        replies = {(reply.binding.action.actor, reply.request_id): reply for reply in originals.terminal_replies
                   if reply.binding is not None}
        require(len(replies) == len(originals.terminal_replies) and set(replies) == set(requested),
                'Independent role reply/dispatch membership differs')
        rows = list(ledger.execute('SELECT * FROM financial_actions_v2 WHERE cohort=? ORDER BY actor,request_id',
                                   (originals.cohort,)))
        require(len(rows) == request_count == len(requested)
                and {(row['actor'], row['request_id']) for row in rows} == set(requested),
                'Controller, financial requests and admitted actions differ')
        journal_paths: set[str] = set()
        journal_locks: set[str] = set()
        result_keys: set[tuple[str, str]] = set()
        spent = failed = 0
        for row in rows:
            require(row['state'] in {'completed', 'failed'}, 'Incomplete or unknown admitted work cannot qualify')
            _, binding, reply, request = view._action(row['actor'], row['request_id'])
            require(encode(binding) == encode(requested[(row['actor'], row['request_id'])]),
                    'Original controller dispatch binding differs')
            require(encode(reply) == encode(replies[(row['actor'], row['request_id'])]),
                    'Original role reply differs from financial/provider terminal outcome')
            paths = view.journal.paths(binding.call_id)
            paths['reservation'] = view.journal.root / (hashlib.sha256(binding.reservation_id.encode()).hexdigest() + '.reservation.json')
            journal_paths.update(str(path) for path in paths.values())
            journal_locks.update(str(paths[kind].with_suffix('.lock')) for kind in ('request', 'reservation'))
            require(all(str(path) in files for path in paths.values()), 'Missing original request journal file')
            payloads.request_guard(binding.action)
            worker_fields = asdict(request)
            worker_fields['allowed_paths'] = list(request.allowed_paths)
            expected_request = canonical_payload({'worker_request': worker_fields,
                'view_manifest_sha256': binding.action.view_manifest_sha256})
            require(payloads._resolve(binding.action.worker_payload_ref) == expected_request,
                    'Original owned request does not match admitted worker bytes')
            proof = view._verify_terminal(row['actor'], row['request_id'])
            require(proof['binding'] == binding and type(reply.usage_units) is int
                    and 0 <= reply.usage_units <= binding.reserved_units, 'Unknown or invalid original usage')
            assert isinstance(reply.usage_units, int) and reply.result_payload_sha256 is not None
            spent += reply.usage_units
            failed += int(reply.state == 'failed')
            result_keys.add((row['actor'], reply.result_payload_sha256))
        require(set(files) == journal_paths, 'Journal manifest contains missing or extra call evidence')
        journal_root = Path(contract['journal_root'])
        require(journal_root.is_absolute() and journal_root.resolve() == journal_root, 'Indirect original journal root')
        with os.scandir(journal_root) as entries:
            actual_names: set[str] = set()
            for entry in entries:
                require(len(actual_names) < MAX_CALLS * 6 and entry.is_file(follow_symlinks=False)
                        and stat.S_ISREG(entry.stat(follow_symlinks=False).st_mode), 'Unexpected or oversized journal inventory')
                actual_names.add(str(journal_root / entry.name))
        require(actual_names == journal_paths | journal_locks, 'Original journal contains unregistered files or missing call locks')
        require({(row['principal'], row['sha']) for row in index.execute('SELECT * FROM results')} == result_keys,
                'Original result publication membership differs')
        require(not ledger.execute('''SELECT 1 FROM reservations r JOIN financial_tasks_v2 t
            ON t.task_id=r.task_id WHERE t.cohort=? AND r.spent IS NULL LIMIT 1''', (originals.cohort,)).fetchone(),
            'Unsettled original reservation cannot qualify')
        require(ledger.execute("SELECT value FROM settings WHERE key='budget'").fetchone()[0] == config['expected_global_cap'],
                'Original cumulative cap changed')
        for item in files.values():
            item.read()
        return FinancialAudit(originals.cohort, originals.config_sha256, terminal['seal_id'], len(rows), failed, spent,
                              cohort['opening'], config['expected_global_cap'],
                              tuple(identity(requested[key]) for key in sorted(requested)))
