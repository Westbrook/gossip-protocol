"""Original terminal outcomes for all six planned subjects, never qualification.

Known failure closures can include never-launched roles or missing child-final
records only when the pinned host producer proves every actual owned process
exited. Missing seals, unknown money, and absent subjects prevent a cohort freeze.
No owner, process, provider or Git mutation is created by this reader.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import sqlite3
import sys

from .candidate_checkpoint_chain_v1 import CheckpointChain, PrefixCommitment
from .candidate_observation_admission_v1 import PROTOCOL as SOURCE_PROTOCOL, source_manifest, source_sha256
from .candidate_release_execution_v2 import capture_git_source
from .cumulative_process_evidence_v1 import ROLE_PROTOCOL, MAX_GENERATIONS, closure_name, confirmation_name, source_fingerprints
from .cumulative_rehearsal_process_v1 import _closed, _ref, _kind, _actions
from .cumulative_rehearsal_inventory_v1 import audit_child_inventory
from .cumulative_study_controller_v1 import StudyPlan, Records, SOURCE_CLOSURE
from .financial_rehearsal_originals_v1 import (readonly_database, _LEDGER_COLUMNS, _table_schema, _table_bound,
    MAX_CALLS, MAX_BYTES, MAX_SQLITE_ROW_BYTES)
from .gitstore import GitStore
from .peer_financial_authority_v2 import canonical_payload
from .peer_financial_terminal_v1 import (PROTOCOL as TERMINAL_PROTOCOL, ChildTerminalSeal, TerminalRoster, authenticate_seal, census,
    seal_name, seal_row, require, digest, sha, FinancialError)

TERMINAL_READER_SOURCES = tuple(sorted(set(SOURCE_CLOSURE) | {
    'gossip_harness/cumulative_terminal_originals_v1.py',
    'gossip_harness/cumulative_rehearsal_process_v1.py',
    'gossip_harness/cumulative_rehearsal_inventory_v1.py',
    'gossip_harness/financial_rehearsal_originals_v1.py',
    'gossip_harness/candidate_checkpoint_chain_v1.py',
    'gossip_harness/candidate_checkpoint_head_v1.py',
    'gossip_harness/candidate_http_journal_v3.py'}))


def terminal_reader_sources() -> dict[str, str]:
    repository = Path(__file__).resolve().parents[1]
    return {name: sha((repository / name).read_bytes()) for name in TERMINAL_READER_SOURCES}


@dataclass(frozen=True)
class TerminalSlot:
    cohort: str
    trajectory: str
    planned_actors: tuple[str, ...]
    outcome: str
    final_source: dict | None
    financial_seal: ChildTerminalSeal | None
    process_counts: dict
    evidence_errors: tuple[str, ...]
    financial_origin: dict | None = None
    acceptance_authority: bool = field(default=False, init=False)


@dataclass(frozen=True)
class TerminalCohortOriginals:
    execution_contract_sha256: str
    roster_sha256: str
    slots: tuple[TerminalSlot, ...]
    checkpoint: PrefixCommitment
    barrier: dict | None
    barrier_position: int | None
    barrier_name: str | None
    barrier_sha256: str | None
    evidence_errors: tuple[str, ...] = ()
    acceptance_authority: bool = field(default=False, init=False)

    @property
    def freeze_eligible(self) -> bool:
        return (len(self.slots) == 6 and not self.evidence_errors and self.barrier is not None
            and self.barrier_name is not None and self.barrier_sha256 is not None and self.barrier_position is not None
            and all(slot.outcome in ('completed', 'stopped_failure') and slot.final_source is not None
                and slot.financial_seal is not None and not slot.evidence_errors for slot in self.slots))


def _process(chain: CheckpointChain, expected: PrefixCommitment, *, roster: TerminalRoster,
                          cohort: str, expected_sources: dict[str, str],
                          expected_source: dict, runtime_root: Path, execution_repository: Path,
                          python_executable: str, status: str) -> tuple[dict, dict, dict, int]:
    """Authenticate actual owned terminal stops, including honest failure tails."""
    require(status in ('completed', 'stopped_failure'), 'Unsupported terminal outcome')
    require(type(chain) is CheckpointChain and type(roster) is TerminalRoster, 'Exact checkpoint and roster required')
    chain.validate_boundary(expected=expected)
    TerminalRoster(roster.execution_contract_sha256, roster.children, roster.policy)
    children = [child for child in roster.children if child.cohort == cohort]
    require(len(children) == 1, 'Unknown rehearsal child')
    child = children[0]
    closure_raw = chain.read(closure_name(cohort))
    from .peer_store_v1 import strict_loads
    closure = strict_loads(closure_raw, max_bytes=2_100_000)
    _closed(closure, 'protocol kind registration source roles request launch_admission_closed acceptance_authority')
    _kind(closure, 'closure')
    closure_position = chain.position(closure_name(cohort))
    require(closure['launch_admission_closed'] is True and closure['acceptance_authority'] is False,
            'Closure is not a completed owner boundary')
    registration_ref, registration, registration_position = _ref(chain, closure['registration'])
    _closed(registration, 'protocol kind roster roster_sha256 child sources max_generations parent_pid parent_thread protected_repository')
    _kind(registration, 'registration')
    require(registration['roster'] == roster.record() and registration['roster_sha256'] == roster.sha256
            and registration['child'] == child.record() and registration['sources'] == expected_sources
            and registration['max_generations'] == MAX_GENERATIONS
            and type(registration['parent_pid']) is int and registration['parent_pid'] > 0
            and type(registration['parent_thread']) is int, 'Original process registration binding differs')
    require(registration_position < closure_position, 'Closure predates registration')
    confirmation = strict_loads(chain.read(confirmation_name(cohort)), max_bytes=2_100_000)
    _closed(confirmation, 'protocol kind closure source request_sha256 protected_repository commit_oid')
    _kind(confirmation, 'closure-confirmation')
    confirmation_position = chain.position(confirmation_name(cohort))
    require(confirmation['closure'] == {'name': closure_name(cohort), 'sha256': sha(closure_raw)}
            and confirmation['source'] == closure['source']
            and confirmation['request_sha256'] == digest(closure['request'])
            and confirmation['protected_repository'] == registration['protected_repository']
            and closure_position < confirmation_position, 'Original closure lacks successful producer confirmation')
    source_ref, source, source_position = _ref(chain, closure['source'])
    _closed(source, 'protocol kind registration repository commit_oid tree_oid source_identity_protocol source_sha256 files')
    _kind(source, 'final-source')
    require(source['registration'] == closure['registration'] and source['source_identity_protocol'] == SOURCE_PROTOCOL,
            'Final source producer binding differs')
    require(source['source_sha256'] == expected_source['source_sha256']
            and source['commit_oid'] == expected_source['commit_oid'], 'Controller and process final source differ')
    require(registration_position < source_position < closure_position, 'Final source order differs')
    rows = closure['roles']
    require(type(rows) is list and tuple(row.get('actor') for row in rows) == child.actors,
            'Original role order or membership differs')
    request = _closed(closure['request'], 'role_stops terminal purpose')
    require(request['purpose'] == 'normal-financial-terminal-v1'
            and type(request['role_stops']) is list and len(request['role_stops']) == len(rows),
            'Original terminal request differs')
    counts = dict(planned=len(child.actors), launched=0, exited=0, never_launched=0, known_launch_failed=0, missing_final=0)
    launch_ids: set[str] = set()
    count = 0
    intervals: list[tuple[int, int, int]] = []
    for row, stop_ref in zip(rows, request['role_stops'], strict=True):
        _closed(row, 'actor stop incarnations never_launched')
        require(type(row['never_launched']) is bool and row['stop'] == stop_ref, 'Original role stop differs')
        actor = row['actor']
        _, stop, stop_position = _ref(chain, stop_ref)
        require(stop == {'protocol': 'financial-role-stop-v1', 'roster_sha256': roster.sha256,
                'cohort': cohort, 'trajectory': child.trajectory, 'actor': actor, 'stopped': True},
                'Financial role stop is not bound to exact original role')
        incarnations = row['incarnations']
        require(type(incarnations) is list and (1 if status == 'completed' else 0) <= len(incarnations) <= MAX_GENERATIONS
                and [item.get('generation') for item in incarnations] == list(range(len(incarnations))),
                'Missing or changed incarnation order')
        previous_wait = registration_position
        actor_launched = False
        for ordinal, item in enumerate(incarnations):
            _closed(item, 'generation intent launch ready final wait launch_failed')
            require(type(item['generation']) is int and item['intent'] is not None, 'Invalid original incarnation')
            if status == 'completed':
                require(item['launch_failed'] is None and all(item[key] is not None for key in ('launch','ready','final','wait')),
                        'Completed role requires all successful original records')
            _, intent, intent_position = _ref(chain, item['intent'])
            _closed(intent, 'protocol kind identity registration argv_sha256 cwd environment_sha256 stdout_path directory directory_identity start_new_session')
            _kind(intent, 'launch-intent')
            identity = _closed(intent['identity'], 'cohort trajectory execution_contract_sha256 actor generation launch_id')
            require({key: identity[key] for key in ('cohort', 'trajectory', 'execution_contract_sha256', 'actor', 'generation')}
                    == {'cohort': cohort, 'trajectory': child.trajectory,
                        'execution_contract_sha256': roster.execution_contract_sha256, 'actor': actor, 'generation': ordinal},
                    'Incarnation original context differs')
            require(type(identity['generation']) is int and type(identity['launch_id']) is str and len(identity['launch_id']) == 48
                    and identity['launch_id'] not in launch_ids, 'Launch identity is missing or reused')
            launch_ids.add(identity['launch_id'])
            launcher_key = 'runtime.' + child.trajectory + '.launcher.' + actor + '.' + str(ordinal)
            launcher = Records(chain).read(launcher_key)
            _closed(launcher, 'argv cwd environment actor generation')
            assert launcher is not None
            directory = runtime_root / 'processes' / actor / str(ordinal)
            environment = launcher['environment']
            require(type(environment) is dict and set(environment) <= {'PATH', 'HOME', 'LANG', 'LC_ALL', 'TMPDIR'}
                    and all(type(k) is type(v) is str for k, v in environment.items()), 'Unregistered role environment')
            argv = [python_executable, '-m', 'gossip_harness.cumulative_study_role_v1',
                    '--role-config', str(runtime_root / 'roles' / actor / 'config.json')]
            require(launcher['argv'] == argv and launcher['cwd'] == str(execution_repository)
                    and launcher['actor'] == actor and type(launcher['generation']) is int
                    and launcher['generation'] == ordinal and intent['argv_sha256'] == digest(argv)
                    and intent['cwd'] == launcher['cwd'] and intent['directory'] == str(directory)
                    and intent['stdout_path'] == str(directory / 'stdout.log')
                    and chain.position(Records.name(launcher_key)) < intent_position
                    and intent['environment_sha256'] == digest({**environment,
                        'GOSSIP_ROLE_EVIDENCE_CONTEXT': canonical_payload(identity).decode(),
                        'GOSSIP_ROLE_EVIDENCE_DIR': str(directory)}), 'Original launch preimage differs')
            info = directory.stat()
            require(directory.resolve() == directory and intent['directory_identity'] == [info.st_dev, info.st_ino],
                    'Original incarnation directory changed')
            require(intent['registration'] == closure['registration'] and intent['start_new_session'] is True,
                    'Intent is outside original process owner')
            if item['launch'] is None:
                require(status == 'stopped_failure' and item['launch_failed'] is not None
                        and all(item[key] is None for key in ('ready', 'final', 'wait')),
                        'Unknown launch cannot be converted to known failure')
                _, failed, failure_position = _ref(chain, item['launch_failed'])
                _closed(failed, 'protocol kind identity intent error_type errno parent_pid')
                _kind(failed, 'launch-failed')
                require(digest(failed['identity']) == digest(identity) and failed['intent'] == item['intent']
                        and failed['parent_pid'] == registration['parent_pid'] and type(failed['error_type']) is str
                        and bool(failed['error_type']) and (failed['errno'] is None or type(failed['errno']) is int)
                        and previous_wait < intent_position < failure_position < source_position < stop_position < closure_position,
                        'Known launch failure is not original owned evidence')
                require(ordinal == len(incarnations) - 1, 'Failed launch cannot justify a successor incarnation')
                counts['known_launch_failed'] += 1
                count += 1
                continue
            require(item['launch_failed'] is None and item['wait'] is not None,
                    'Actual launch requires original owned wait and no failure relabel')
            actor_launched = True
            counts['launched'] += 1
            _, launch, launch_position = _ref(chain, item['launch'])
            _closed(launch, 'protocol kind identity intent pid process_group parent_pid')
            _kind(launch, 'launch')
            require(launch['identity'] == identity and launch['intent'] == item['intent']
                    and type(launch['pid']) is int and launch['pid'] > 0
                    and launch['process_group'] == launch['pid']
                    and launch['parent_pid'] == registration['parent_pid'], 'Actual owned launch binding differs')
            events = {}
            for event in ('ready', 'final'):
                if item[event] is None:
                    continue
                _, value, position = _ref(chain, item[event])
                keys = 'protocol event identity pid'
                if event == 'final':
                    keys += ' outcome completed_action_ids remaining_action_ids'
                _closed(value, keys)
                require(value['protocol'] == ROLE_PROTOCOL and value['event'] == event
                        and digest(value['identity']) == digest(identity) and type(value['pid']) is int
                        and value['pid'] == launch['pid'], 'Original child event differs from owned launch')
                from . import candidate_http_journal_v3 as stable
                event_path = Path(intent['directory']) / (event + '.json')
                require(event_path.is_absolute() and event_path.resolve() == event_path
                        and stable.read(event_path, max_bytes=2_100_000) == chain.read(item[event]['name']),
                        'Retained child event bytes changed')
                events[event] = (value, position)
            if 'final' in events:
                final = events['final'][0]
                completed = _actions(final['completed_action_ids'])
                remaining = _actions(final['remaining_action_ids'])
                require(final['outcome'] in ('stopped', 'restart', 'failed') and not set(completed).intersection(remaining),
                        'Invalid original final action sets/outcome')
            else:
                counts['missing_final'] += 1
            _, wait, wait_position = _ref(chain, item['wait'])
            _closed(wait, 'protocol kind identity launch ready final pid returncode reaped_with process_group_empty event_errors parent_pid')
            _kind(wait, 'wait')
            require(digest(wait['identity']) == digest(identity) and all(wait[key] == item[key] for key in ('launch','ready','final'))
                    and type(wait['pid']) is int and wait['pid'] == launch['pid'] and wait['parent_pid'] == registration['parent_pid']
                    and wait['reaped_with'] == 'Popen.wait' and wait['process_group_empty'] is True
                    and type(wait['returncode']) is int and type(wait['event_errors']) is list,
                    'Actual owned wait evidence differs')
            errors = wait['event_errors']
            require(len(errors) <= 2 and all(type(error) is dict and set(error) == {'event','error_type'}
                    and error['event'] in ('ready','final') and type(error['error_type']) is str and bool(error['error_type'])
                    for error in errors) and len({error['event'] for error in errors}) == len(errors)
                    and all(any(error['event'] == event for error in errors) for event in ('ready','final') if item[event] is None),
                    'Missing child event is not explained by original owner observation')
            require(previous_wait < intent_position < launch_position < wait_position < source_position < stop_position < closure_position
                    and all(launch_position < position < wait_position for _,position in events.values()),
                    'Original owned lifecycle order differs')
            if len(events) == 2:
                require(events['ready'][1] < events['final'][1], 'Final event preceded ready')
            if status == 'completed':
                require(not errors and len(events) == 2, 'Completed role has missing/error events')
                final = events['final'][0]
                if ordinal == len(incarnations) - 1:
                    require(wait['returncode'] == 0 and final['outcome'] == 'stopped' and not final['remaining_action_ids'],
                            'Latest role did not complete successfully')
                else:
                    require(final['outcome'] == 'restart' and wait['returncode'] == 86,
                            'Unexpected completed recovery outcome')
            counts['exited'] += 1
            intervals.append((launch['pid'], launch_position, wait_position))
            previous_wait = wait_position
            count += 1
        require(row['never_launched'] is (not actor_launched), 'Never-launched classification contradicts owned launch')
        if not actor_launched:
            counts['never_launched'] += 1
        require(registration_position < source_position < stop_position < closure_position,
                'Original role stop precedes source or registration')
    # OS PIDs can legitimately recur after a prior incarnation exited.
    for index, (pid, start, end) in enumerate(intervals):
        require(not any(pid == other and max(start, begin) < min(end, finish)
                        for other, begin, finish in intervals[index + 1:]), 'Concurrent original PID reuse')
    _, terminal, terminal_position = _ref(chain, request['terminal'])
    require(terminal == {'protocol': 'financial-child-terminal-v1', 'roster_sha256': roster.sha256,
            'cohort': cohort, 'trajectory': child.trajectory, 'status': status,
            'final_source_sha256': source['source_sha256']}, 'Original completed financial terminal differs')
    require(all(chain.position(ref['name']) < terminal_position for ref in request['role_stops'])
            and terminal_position < closure_position, 'Terminal precedes original role stops')
    repository = Path(source['repository'])
    require(repository.is_absolute() and repository.resolve() == repository and repository.is_dir(),
            'Original protected Git repository missing or indirect')
    protected = _closed(registration['protected_repository'], 'path device inode marker_sha256')
    from . import candidate_http_journal_v3 as stable
    info = repository.stat()
    require(protected == {'path': str(repository), 'device': info.st_dev, 'inode': info.st_ino,
        'marker_sha256': sha(stable.read(repository / 'gossip-harness-store', max_bytes=4096))}
        and confirmation['commit_oid'] == source['commit_oid'], 'Original protected repository identity changed')
    store = GitStore(repository)  # Read-only constructor and commands only.
    tree, files = capture_git_source(store, source['commit_oid'])
    require(store.head() == source['commit_oid'] and tree == source['tree_oid']
            and source_manifest(files) == source['files'] and source_sha256(files) == source['source_sha256'],
            'Actual final protected Git source differs')
    require(digest(expected_source) == digest({'commit_oid': source['commit_oid'],
        'source_sha256': source['source_sha256'], 'files': {name:raw.decode() for name,raw in files.items()}}),
        'Controller final source full bytes differ')
    chain.validate_boundary(expected=expected)
    return source, counts, closure, confirmation_position


def _ledger_bounds(db: sqlite3.Connection, cohort: str) -> None:
    """Bound only the original ledger tables consumed by terminal census."""
    tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    rpc = {'financial_rpc_config_v2', 'financial_rpc_requests_v2'}
    require(not (tables & rpc) or rpc <= tables, 'Partial original RPC schema')
    for table, columns in _LEDGER_COLUMNS.items():
        if table in rpc and table not in tables:
            continue
        _table_schema(db, table, columns)
        where, maximum = 'cohort=?', MAX_CALLS
        parameters: tuple = (cohort,)
        if table == 'settings':
            where, parameters, maximum = "key='budget'", (), 1
        elif table in {'tasks', 'dependencies', 'reservations'}:
            member = 'id' if table == 'tasks' else 'task_id'
            where = f'{member} IN (SELECT task_id FROM financial_tasks_v2 WHERE cohort=?)'
            if table == 'dependencies':
                maximum = MAX_CALLS * MAX_CALLS
        elif table in {'financial_cohorts_v2', 'financial_terminal_config_v4', 'financial_terminal_seals_v4', 'financial_rpc_config_v2'}:
            maximum = 1
        elif table == 'financial_rpc_requests_v2':
            maximum = 8 * MAX_CALLS
        _table_bound(db, table, columns, where=where, parameters=parameters, maximum=maximum,
                     aggregate=MAX_SQLITE_ROW_BYTES if table == 'financial_terminal_seals_v4' else MAX_BYTES)


def _sealed_child(db: sqlite3.Connection, chain: CheckpointChain, *, roster: TerminalRoster, index: int,
                  ledger_identity: dict, terminal: dict, config: dict) -> tuple[ChildTerminalSeal, dict, tuple[str, ...], int, int]:
    from .peer_financial_authority_v3 import FIXTURE_TRANSPORT, LIVE_TRANSPORT
    child = roster.children[index]
    require(config['mode'] in ('fixture','live')
            and config['observation_kind'] == ('provider' if config['mode'] == 'live' else 'simulated')
            and config['operator_permit']['mode'] == config['mode']
            and config['operator_permit_sha256'] == digest(config['operator_permit'])
            and config['operator_permit']['execution_design_sha256'] == digest(config['operator_permit']['execution_design'])
            and config['operator_permit']['cohort_contract_sha256'] == digest(config['contract']),
            'Original financial observation/permit mode or design differs')
    transport = LIVE_TRANSPORT if config['mode'] == 'live' else FIXTURE_TRANSPORT
    expected_profiles = {name: {**profile, 'transport_identity': transport, 'observation_kind': config['observation_kind']}
                         for name, profile in config['operator_permit']['profiles'].items()}
    require(config['contract']['transport_identity'] == transport and digest(config['profiles']) == digest(expected_profiles),
            'Original enriched profiles differ from approved bare manifests')
    declaration = _closed(terminal['financial_seal'], 'raw_utf8 publication_name commitment')
    # The wrapper's historical commitment is authenticated producer data, not
    # an independently reconstructed prefix. Return a fresh current observation
    # after SQL/raw joins, like V4.verified_terminal_seal, and never use that old
    # declaration for order or admission. The caller supplies this current head.
    PrefixCommitment(**declaration['commitment'])
    receipt = ChildTerminalSeal(declaration['raw_utf8'].encode(), declaration['publication_name'], chain.commitment)
    require(receipt.publication_name == seal_name(child.cohort) and chain.read(receipt.publication_name) == receipt.raw,
            'Original externally published SQL seal differs')
    value = authenticate_seal(chain, receipt.raw)
    require(value['terminal_status'] == terminal['status'], 'Retained terminal seal outcome differs')
    row = seal_row(db, child.cohort)
    financial = db.execute('SELECT * FROM financial_cohorts_v2 WHERE cohort=?', (child.cohort,)).fetchone()
    require(row is not None and financial is not None and row['receipt'] == receipt.raw
            and row['receipt_sha'] == sha(receipt.raw) and row['seal_id'] == value['seal_id']
            and row['snapshot_sha'] == sha(row['snapshot']) == value['snapshot_sha256']
            and row['boundary'] == canonical_payload(value['boundary'])
            and financial['config'] == canonical_payload(config) and financial['config_sha'] == digest(config)
            and financial['config_sha'] == value['financial_config_sha256']
            and census(db, child.cohort, financial['config_sha']) == row['snapshot'],
            'Original sealed financial census/configuration differs')
    terminal_config = value['terminal_config']
    require(digest(terminal_config['roster']) == digest(roster.record()) and terminal_config['child_index'] == index
            and digest(terminal_config['child']) == digest(child.record()) and config['terminal_config'] == terminal_config
            and config['contract']['execution_contract_sha256'] == roster.execution_contract_sha256
            and config['contract']['ledger_identity'] == ledger_identity
            and config['contract']['cohort_id'] == child.cohort,
            'Original sealed child is outside this prospective study/wallet')
    require(db.execute("SELECT value FROM settings WHERE key='budget'").fetchone()[0] == config['expected_global_cap']
            and financial['opening'] == config['operator_permit']['expected_opening_usage'],
            'Original cumulative wallet cap/opening changed')
    rows = db.execute('''SELECT a.state,r.spent,r.amount FROM financial_actions_v2 a
        JOIN reservations r ON r.id=a.reservation_id WHERE a.cohort=?''', (child.cohort,)).fetchall()
    errors = []
    if financial['halted'] or any(row['state'] not in ('completed','failed') or row['spent'] is None for row in rows):
        errors.append('financial_halt_or_unknown_action')
    if terminal['status'] == 'completed' and any(row['state'] != 'completed' for row in rows):
        errors.append('completed_child_contains_failed_action')
    return receipt, value, tuple(errors), financial['opening'], sum(row['spent'] if row['spent'] is not None else row['amount'] for row in rows)



def _current_wallet(db: sqlite3.Connection, cohorts: tuple[str, ...], *, expected_total: int | None,
                    expected_cap: int | None) -> None:
    """Reconcile original adoption rows and the complete current wallet snapshot.

    Child censuses authenticate their own reservations; original adoption rows
    authenticate the earlier wallet. Neither permits an unowned late suffix.
    All base-table bytes are bounded before fetching them into Python.
    """
    from .peer_financial_authority_v2 import _BASE_SCHEMA
    from .peer_store_v1 import strict_loads
    integers = {'value','epoch','amount','spent','old_budget','new_budget','spent_or_reserved'}
    reals = {'expires','changed_at'}
    nullable = {'worker','expires','accepted_commit','intent_id','spent','result'}
    current: dict[str, list[dict]] = {}
    for table, columns in _BASE_SCHEMA.items():
        declared = [(name, 'INTEGER' if name in integers else 'REAL' if name in reals else 'TEXT') for name in columns]
        schema = db.execute('SELECT type,sql FROM sqlite_master WHERE name=?', (table,)).fetchone()
        require(schema is not None and schema['type'] == 'table' and type(schema['sql']) is str
                and schema['sql'].lstrip().upper().startswith('CREATE TABLE'), 'Original wallet table is missing or indirect')
        info = db.execute('SELECT name,type FROM pragma_table_info(?) LIMIT ?', (table,len(columns)+1)).fetchall()
        require([(row['name'],row['type']) for row in info] == declared, 'Original wallet table columns differ')
        require(db.execute(f'SELECT count(*) FROM (SELECT 1 FROM {table} LIMIT ?)',
                           (6 * MAX_CALLS + 1,)).fetchone()[0] <= 6 * MAX_CALLS, 'Original wallet row bound exceeded')
        invalid, lengths = [], []
        for name, kind in declared:
            types = "'" + kind.lower() + "'" + (",'null'" if name in nullable else '')
            bound = 20 if kind == 'INTEGER' else 32 if kind == 'REAL' else 4096
            invalid.append(f'typeof({name}) NOT IN ({types}) OR length(CAST({name} AS BLOB))>{bound}')
            lengths.append(f'coalesce(length(CAST({name} AS BLOB)),0)')
        require(not db.execute(f'SELECT 1 FROM {table} WHERE {" OR ".join(invalid)} LIMIT 1').fetchone(),
                'Original wallet cell is oversized or mistyped')
        require(db.execute(f'SELECT coalesce(sum({"+".join(lengths)}),0) FROM {table}').fetchone()[0] <= 6 * MAX_BYTES,
                'Original wallet aggregate exceeds bound')
        current[table] = [dict(row) for row in db.execute(f'SELECT * FROM {table}')]
    reservations = current['reservations']
    require(len({row['id'] for row in reservations}) == len(reservations),
            'Current wallet reservation identity is duplicated')
    require(all(row['amount'] >= 0 and type(row['spent']) is int and 0 <= row['spent'] <= row['amount']
                and row['epoch'] >= 0 and row['state'] == 'settled' for row in reservations),
            'Current wallet contains invalid or unsettled reservation')
    total = sum(row['spent'] for row in reservations)
    if expected_total is not None:
        require(total == expected_total, 'Current wallet usage differs from final cumulative opening and spend')
    if expected_cap is not None:
        require(total <= expected_cap and {'key':'budget','value':expected_cap} in current['settings'],
                'Current wallet cap differs or is exceeded')
    current_rows = {table: {canonical_payload(row) for row in rows} for table,rows in current.items()}
    original_reservations: set[str] = set()
    for index, cohort in enumerate(cohorts):
        financial = db.execute('SELECT historical,historical_sha,opening FROM financial_cohorts_v2 WHERE cohort=?', (cohort,)).fetchone()
        require(financial is not None and type(financial['historical']) is bytes
                and len(financial['historical']) <= MAX_BYTES and sha(financial['historical']) == financial['historical_sha'],
                'Original historical adoption bytes differ')
        historical = strict_loads(financial['historical'], max_bytes=MAX_BYTES)
        require(type(historical) is dict and set(historical) == set(_BASE_SCHEMA), 'Original historical table census differs')
        for table, rows in historical.items():
            require(type(rows) is list and len(rows) <= 6 * MAX_CALLS
                    and all(type(row) is dict and set(row) == set(_BASE_SCHEMA[table]) for row in rows),
                    'Original historical row census differs')
            require(all(canonical_payload(row) in current_rows[table] for row in rows), 'Historical wallet rows changed')
        require(len({row['id'] for row in historical['reservations']}) == len(historical['reservations']),
                'Historical reservation identity is duplicated')
        require(sum(row['spent'] for row in historical['reservations']) == financial['opening'],
                'Original historical usage differs from adopted opening')
        if index == 0:
            original_reservations = {row['id'] for row in historical['reservations']}
        original_reservations.update(row[0] for row in db.execute("""SELECT r.id FROM reservations r
            JOIN financial_tasks_v2 f ON f.task_id=r.task_id WHERE f.cohort=?""", (cohort,)))
    if len(cohorts) == 6:
        require({row['id'] for row in reservations} == original_reservations,
                'Current wallet contains an unowned reservation suffix')


def _original_presence(chain: CheckpointChain, plan: StudyPlan) -> set[str]:
    """Bounded acknowledged-record census; absence is never inferred from begin alone."""
    from .candidate_http_journal_v3 import decode
    found: set[str] = set()
    children = plan.roster.children
    count = 0
    for path in chain.raw_root.iterdir():
        count += 1
        require(count <= chain.limits.max_files + 1, 'Original record census exceeds checkpoint bound')
        name = path.name
        if name.startswith('study-') and name.endswith('.json'):
            value = _closed(decode(chain.read(name)), 'slot value')
            require(type(value['slot']) is str and len(value['slot']) <= 4096 and Records.name(value['slot']) == name,
                    'Original controller slot name differs')
            for child in children:
                if value['slot'].startswith(('child.' + child.trajectory + '.', 'runtime.' + child.trajectory + '.',
                                             'publish.child.' + child.trajectory + '.')):
                    found.add(child.trajectory)
        else:
            for child in children:
                stem = closure_name(child.cohort).removesuffix('.closure.json')
                if name.startswith(stem + '.') or name.startswith(stem + '-') or name == seal_name(child.cohort):
                    chain.read(name)  # Membership + raw digest + current external head.
                    found.add(child.trajectory)
    return found


def audit_terminal_cohort(chain: CheckpointChain, expected: PrefixCommitment, *, plan: StudyPlan,
                          ledger_identity: dict, repository: Path) -> TerminalCohortOriginals:
    """Audit one original ledger snapshot and six prospective outcome slots.

    A missing/invalid child stays in the denominator. No capsule, seal, public
    success, or returned dataclass opens financial admission or grants quality
    acceptance. Callers must retain a fresh independent verifier observation and
    reauthenticate this head/source before an acceptance dispatch.
    """
    require(type(chain) is CheckpointChain and type(plan) is StudyPlan, 'Exact owned chain/plan required')
    plan.__post_init__()
    chain.validate_boundary(expected=expected)
    plan.verify_sources(repository)
    records = Records(chain)
    require(digest(records.read('contract')) == digest(plan.record()) and records.read('ledger') == ledger_identity,
            'Original study or cumulative wallet identity differs')
    process_sources = {name: plan.source_pins[name] for name in source_fingerprints()}
    presence = _original_presence(chain, plan)
    slots: list[TerminalSlot] = []
    errors: list[str] = []
    seals: list[ChildTerminalSeal] = []
    barriers: dict | None = None
    barrier_position = None
    barrier_name = None
    barrier_sha256 = None
    prior_terminal_position: int | None = None
    previous_opening: int | None = None
    global_cap: int | None = None
    with readonly_database(ledger_identity) as db:
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master")}
        for table in ('financial_cohorts_v2','financial_terminal_seals_v4','financial_actions_v2'):
            if table in tables:
                _table_schema(db, table, _LEDGER_COLUMNS[table])
        for index, child in enumerate(plan.roster.children):
            ckey, rkey = 'child.' + child.trajectory, 'runtime.' + child.trajectory
            counts = dict(planned=len(child.actors), launched=0, exited=0, never_launched=0, known_launch_failed=0, missing_final=0)
            final_source = None
            financial_origin = None
            seal = None
            outcome = 'evidence_unknown'
            child_errors: tuple[str, ...] = ()
            try:
                financial = (db.execute('SELECT 1 FROM financial_cohorts_v2 WHERE cohort=?', (child.cohort,)).fetchone()
                             if 'financial_cohorts_v2' in tables else None)
                begin, terminal = records.read(ckey + '.begin'), records.read(ckey + '.terminal')
                if begin is None:
                    require(child.trajectory not in presence and terminal is None and financial is None
                            and ('financial_terminal_seals_v4' not in tables or seal_row(db, child.cohort) is None)
                            and records.read(rkey + '.source-initialization.intent') is None
                            and not chain.has(closure_name(child.cohort)) and not chain.has(seal_name(child.cohort)),
                            'Missing begin record contradicts actual child evidence')
                    outcome = 'unattempted'
                    counts['never_launched'] = len(child.actors)
                else:
                    _ledger_bounds(db, child.cohort)
                    require(index == 0 or prior_terminal_position is not None,
                            'Later child cannot bypass missing original predecessor closure')
                    if prior_terminal_position is not None:
                        require(prior_terminal_position < chain.position(Records.name(ckey + '.begin')),
                                'Sequential child order differs')
                    require(terminal is not None and terminal['trajectory_id'] == child.trajectory
                            and terminal['status'] in ('completed', 'stopped_failure') and terminal['acceptance_authority'] is False,
                            'Actual child terminal is missing or unsupported')
                    assert terminal is not None
                    manifest = records.read(rkey + '.originals')
                    require(type(manifest) is dict and manifest['writer_lifetimes_closed'] is True
                            and manifest['ledger_identity'] == ledger_identity and manifest['cohort'] == child.cohort,
                            'Closed original child inventory unavailable')
                    assert manifest is not None
                    runtime_root = Path(manifest['original_paths']['runtime_root'])
                    require(runtime_root.is_absolute() and runtime_root.resolve() == runtime_root
                            and manifest['original_paths']['protected_git'] == str(runtime_root / 'protected.git'),
                            'Original protected repository path differs')
                    audit_child_inventory(runtime_root, manifest['complete_child_files'])
                    config = records.read(rkey + '.financial-config')
                    require(type(config) is dict and manifest['config_sha256'] == digest(config)
                            and digest(manifest['config']) == digest(config)
                            and digest(config['operator_permit']) == digest(records.read(rkey + '.permit'))
                            and config['operator_permit_sha256'] == digest(config['operator_permit'])
                            and config['operator_permit']['sources'] == plan.source_pins
                            and digest(config['operator_permit']['profiles']) == plan.cohort.model_profiles_sha256
                            and type(config['operator_permit']['max_workers']) is int
                            and config['operator_permit']['max_workers'] == plan.executor_slots
                            and config['contract']['execution_contract_sha256'] == plan.sha256,
                            'Original child financial source/permit/config differs')
                    assert config is not None
                    seal, seal_value, financial_errors, opening, spent = _sealed_child(db, chain, roster=plan.roster,
                        index=index, ledger_identity=ledger_identity, terminal=terminal, config=config)
                    require((previous_opening is None or opening == previous_opening)
                            and (global_cap is None or global_cap == config['expected_global_cap']),
                            'Sequential cumulative wallet opening or cap differs')
                    previous_opening, global_cap = opening + spent, config['expected_global_cap']
                    config_name = Records.name(rkey + '.financial-config')
                    financial_origin = {'mode': config['mode'], 'observation_kind': config['observation_kind'],
                        'config_sha256': digest(config), 'permit_sha256': config['operator_permit_sha256'],
                        'execution_design_sha256': config['operator_permit']['execution_design_sha256'],
                        'ledger_identity': ledger_identity, 'config_name': config_name, 'config_raw_sha256': sha(chain.read(config_name))}
                    final_source, counts, closure, confirmation_position = _process(chain, expected, roster=plan.roster,
                        cohort=child.cohort, expected_sources=process_sources, expected_source=terminal['final_source'],
                        runtime_root=runtime_root, execution_repository=repository, python_executable=sys.executable,
                        status=terminal['status'])
                    require(final_source['repository'] == str(runtime_root / 'protected.git')
                            and digest(closure['request']) == digest(seal_value['request'])
                            and confirmation_position < chain.position(seal.publication_name)
                            < chain.position(Records.name(rkey + '.originals')) < chain.position(Records.name(ckey + '.terminal')),
                            'Actual process/SQL/writer/controller terminal order differs')
                    audit_child_inventory(runtime_root, manifest['complete_child_files'])
                    child_errors = financial_errors
                    outcome = terminal['status'] if not financial_errors else 'evidence_unknown'
                    seals.append(seal)
                    prior_terminal_position = chain.position(Records.name(ckey + '.terminal')) if not financial_errors else None
            except (FinancialError, ValueError, KeyError, TypeError, OSError, sqlite3.DatabaseError) as error:
                child_errors = (type(error).__name__ + ': ' + str(error),)
                outcome = 'evidence_unknown'
                prior_terminal_position = None
            slots.append(TerminalSlot(child.cohort, child.trajectory, child.actors, outcome, final_source, seal, counts, child_errors, financial_origin))
            if outcome in ('unattempted', 'evidence_unknown'):
                prior_terminal_position = None
        # An unknown earlier/historical financial effect stays a global halt.
        if (('financial_cohorts_v2' in tables and db.execute('SELECT 1 FROM financial_cohorts_v2 WHERE halted=1 LIMIT 1').fetchone())
                or ('financial_actions_v2' in tables and db.execute("SELECT 1 FROM financial_actions_v2 WHERE state IN ('pending','publication_pending','unknown') LIMIT 1").fetchone())):
            errors.append('cumulative_wallet_halted_or_unsettled')
        try:
            _table_schema(db, 'intents', ('id','repository','old_head','new_head','leases','state','result'))
            _table_bound(db, 'intents', ('state',), where='1', parameters=(), maximum=MAX_CALLS * MAX_CALLS)
            require(not db.execute("SELECT 1 FROM intents WHERE state NOT IN ('pending','accepted','rejected') LIMIT 1").fetchone(),
                    'Original Git intent state is invalid')
            if db.execute("SELECT 1 FROM intents WHERE state='pending' LIMIT 1").fetchone():
                errors.append('cumulative_wallet_pending_git_intent')
        except (FinancialError, ValueError, sqlite3.DatabaseError) as error:
            errors.append('global_git_intent_evidence_unavailable: ' + type(error).__name__ + ': ' + str(error))
        try:
            authenticated_cohorts = tuple(slot.cohort for slot in slots if slot.financial_seal is not None)
            _current_wallet(db, authenticated_cohorts, expected_total=previous_opening if len(seals) == 6 else None,
                            expected_cap=global_cap)
        except (FinancialError, ValueError, KeyError, TypeError, sqlite3.DatabaseError) as error:
            errors.append('global_wallet_evidence_unavailable: ' + type(error).__name__ + ': ' + str(error))
        try:
            retained = records.read('complete-cohort-barrier')
            if len(seals) == 6 and all(slot.outcome in ('completed','stopped_failure') and not slot.evidence_errors for slot in slots) and not errors:
                require(retained is not None, 'Complete original cohort lacks retained global barrier')
                assert retained is not None
                require(len({receipt.seal_id for receipt in seals}) == 6, 'Duplicate original child seal')
                expected_body = {'protocol': TERMINAL_PROTOCOL, 'roster_sha256': plan.roster.sha256,
                    'seal_ids': [receipt.seal_id for receipt in seals], 'role_count': 96, 'acceptance_authority': False}
                # All six exact SQL seals/censuses were authenticated through this
                # single original-inode transaction. No separate path reopen here.
                require(digest({key:value for key,value in retained.items() if key != 'checkpoint'}) == digest(expected_body),
                        'Retained complete cohort barrier differs from original six SQL seals')
                barrier_name = Records.name('complete-cohort-barrier')
                barrier_position = chain.position(barrier_name)
                require(prior_terminal_position is not None and prior_terminal_position < barrier_position,
                        'Global barrier precedes original final child')
                barriers, barrier_sha256 = retained, sha(chain.read(barrier_name))
            elif retained is not None:
                errors.append('retained_barrier_lacks_complete_current_originals')
        except (FinancialError, ValueError, KeyError, TypeError) as error:
            errors.append('global_barrier_unavailable: ' + type(error).__name__ + ': ' + str(error))
            barriers = barrier_position = barrier_name = barrier_sha256 = None
    chain.validate_boundary(expected=expected)
    plan.verify_sources(repository)
    return TerminalCohortOriginals(plan.sha256, plan.roster.sha256, tuple(slots), expected,
        barriers, barrier_position, barrier_name, barrier_sha256, tuple(errors))
