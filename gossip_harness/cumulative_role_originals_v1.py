"""Read-only cross-binding of terminal role originals, never live qualification.

Caller authenticates config, complete file inventory and DB identities against an
independent checkpoint after all writers stop. This reader proves final stored
membership and bytes, not past-time availability or Byzantine producer identity.
"""
from __future__ import annotations

from contextlib import ExitStack
from dataclasses import asdict, dataclass, field
import hashlib
import os
from pathlib import Path
import sqlite3
import threading

from . import candidate_http_journal_v3 as stable
from .cumulative_study_role_v1 import PROTOCOL as ROLE_PROTOCOL, RoleDriver, canonical_bytes, checked_config, closed, strict_loads
from .financial_rehearsal_originals_v1 import readonly_database
from .peer_mesh_store_v2 import MeshStore
from .peer_project_contract_v2 import ActionRequest, DispatchBinding, DispatchReply, EvidenceRef, from_dict, identity, resolve_local, to_dict, worker_request_digest
from .peer_role_loop_v2 import PROTOCOL as LOOP_PROTOCOL, STATES, RoleLoop, WorkDirective, directive_id, materialize

MAX_BYTES = 8_388_608
MAX_AGGREGATE = 32 * 1024 * 1024
DATABASE_PATHS = ('decisions.sqlite', 'journal/role.sqlite', 'mesh/mesh.sqlite')


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


@dataclass(frozen=True)
class RoleAudit:
    actor: str
    dispatches: tuple[DispatchBinding, ...]
    terminal_replies: tuple[DispatchReply, ...]
    directive_ids: tuple[str, ...]
    stages: tuple[str, ...]
    placement: str
    observed_process_ids: tuple[int, ...]
    live_qualification: bool = field(default=False, init=False)


def _bounds(db: sqlite3.Connection, schema: dict[str, dict[str, tuple[str, int]]], maxima: dict[str, int]) -> None:
    objects = list(db.execute("SELECT name,type,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"))
    require({row['name'] for row in objects} == set(schema)
            and all(row['type'] == 'table' and row['sql'].lstrip().upper().startswith('CREATE TABLE')
                    for row in objects), 'Indirect or foreign role schema')
    for table, columns in schema.items():
        info = db.execute('SELECT name,type FROM pragma_table_info(?) LIMIT ?', (table, len(columns) + 1)).fetchall()
        require([(r['name'], r['type'].lower()) for r in info] == [(n, k) for n, (k, _) in columns.items()],
                'Role columns differ')
        require(db.execute(f'SELECT count(*) FROM (SELECT 1 FROM {table} LIMIT ?)', (maxima[table] + 1,)).fetchone()[0]
                <= maxima[table], 'Oversized role row census: ' + table)
        invalid = ' OR '.join(f"typeof({name})!='{kind}' OR length(CAST({name} AS BLOB))>{bound}"
                              for name, (kind, bound) in columns.items())
        require(not db.execute(f'SELECT 1 FROM {table} WHERE {invalid} LIMIT 1').fetchone(),
                'Oversized or mistyped role cell: ' + table)
        sizes = '+'.join(f'length(CAST({name} AS BLOB))' for name in columns)
        require(db.execute(f'SELECT coalesce(sum({sizes}),0) FROM {table}').fetchone()[0] <= MAX_AGGREGATE,
                'Oversized role aggregate: ' + table)


class _ReadMesh:
    def __init__(self, store: MeshStore):
        self.store = store

    def arrived(self) -> tuple[EvidenceRef, ...]:
        return self.store.arrived()

    def resolve(self, ref: EvidenceRef) -> bytes | None:
        return self.store.resolve(ref)

    def want(self, ref: EvidenceRef) -> bool:
        raise ValueError('Required role evidence is absent; audit never requests payloads')

    def publish(self, kind: str, payload: bytes, command_id: str) -> EvidenceRef:
        raise ValueError('Audit cannot publish')

    def body(self, ref: EvidenceRef) -> bytes:
        return resolve_local(ref, self.arrived(), self.resolve(ref))

    def command(self, command: str, kind: str, raw: bytes) -> EvidenceRef:
        rows = self.store.db.execute('SELECT event_id FROM commands WHERE command_id=?', (command,)).fetchall()
        require(len(rows) == 1, 'Missing original role command: ' + command)
        refs = [ref for ref in self.arrived() if ref.event_id == rows[0][0]]
        require(len(refs) == 1 and refs[0].producer == self.store.node_id and refs[0].kind == kind
                and self.body(refs[0]) == raw, 'Original role publication differs')
        return refs[0]


def _original(path: Path, raw: bytes) -> None:
    require(path.is_absolute() and path.resolve() == path, 'Indirect role original path')
    require(stable.read(path, max_bytes=MAX_BYTES) == raw, 'Role original file differs: ' + path.name)


def _exact_files(root: Path, expected: set[str], maximum: int) -> None:
    require(root.is_dir() and root.resolve() == root, 'Missing or indirect role original directory')
    names: set[str] = set()
    with os.scandir(root) as entries:
        for entry in entries:
            require(len(names) < maximum and entry.is_file(follow_symlinks=False), 'Unexpected role original member')
            names.add(entry.name)
    require(names == expected, 'Role original file membership differs')


def audit_role_originals(*, actor: str, role_config: dict, role_root: Path,
                        database_identities: dict[str, dict], controller_results: tuple[dict, ...],
                        expected_final_action_ids: tuple[str, ...]) -> RoleAudit:
    """Reconcile published known outcomes; incomplete/stopped originals fail closed.

    Database identity keys are absolute original paths, as emitted by runtime.
    Additional identities belong to the caller's complete child inventory.
    Dispatch and stage order follows original journal admission order.
    """
    mesh_config = checked_config(role_config)
    require(actor == role_config['actor'] and role_root.is_absolute() and role_root.resolve() == role_root
            and mesh_config.root == role_root / 'mesh', 'Original role identity/root differs')
    maximum = role_config['max_actions']
    require(type(controller_results) is tuple and 0 < len(controller_results) <= maximum
            and type(expected_final_action_ids) is tuple and len(expected_final_action_ids) <= maximum,
            'Invalid bounded controller role membership')
    require(len(set(expected_final_action_ids)) == len(expected_final_action_ids), 'Duplicate final action identity')
    config_raw = canonical_bytes(role_config)
    _original(role_root / 'config.json', config_raw)
    identities = []
    for relative in DATABASE_PATHS:
        path = str(role_root / relative)
        require(path in database_identities and database_identities[path].get('path') == path,
                'Original role database identity missing')
        identities.append(database_identities[path])
    with ExitStack() as stack:
        decisions, journal, mesh_db = [stack.enter_context(readonly_database(item)) for item in identities]
        integer, text, blob = ('integer', 20), ('text', 4096), ('blob', 512_000)
        _bounds(decisions, {'config': {'id': integer, 'raw': blob, 'count': integer},
            'decisions': {'stage': text, 'directive': text, 'raw': ('blob', MAX_BYTES)}},
            {'config': 1, 'decisions': maximum})
        _bounds(journal, {'config': {'id': integer, 'body': blob, 'entries': integer, 'membership': text},
            'actions': {'ordinal': integer, 'id': text, 'slot': text, 'attempt': integer, 'body': blob, 'digest': text},
            'history': {'sequence': integer, 'action_id': text, 'state': text, 'reason': text, 'pid': integer}},
            {'config': 1, 'actions': maximum, 'history': maximum * 128})
        store = object.__new__(MeshStore)
        store.db, store.identity, store.limits = mesh_db, mesh_config.identity(), mesh_config.limits
        store.node_id, store.roster = actor, mesh_config.roster
        store.lock, store.closed, store._arrived_cache = threading.RLock(), False, []
        store.audit()
        mesh = _ReadMesh(store)
        driver = object.__new__(RoleDriver)
        driver.root, driver.config, driver.mesh = role_root, role_config, mesh
        driver.mesh_config, driver.actor, driver.placement = mesh_config, actor, role_config['placement']
        driver.config_raw, driver.db = config_raw, decisions
        driver._audit()
        loop = object.__new__(RoleLoop)
        loop.db, loop.pid, loop.closed, loop.max_actions = journal, os.getpid(), False, maximum
        loop._config = canonical_bytes({'protocol': LOOP_PROTOCOL, 'actor': actor,
            'call_limit': role_config['call_limit'], 'policy_sha256': role_config['policy_sha256'],
            'result_producer': 'finance', 'lease_ttl': 180.0, 'renew_margin': 5.0,
            'max_renewals': 3, 'max_actions': maximum})
        loop._membership_seed = hashlib.sha256(b'role-membership-v2\0' + loop._config).hexdigest()
        records = loop._records()
        require(0 < len(records) == len(controller_results) <= role_config['call_limit'], 'Role action/controller census differs')
        history = loop.history()
        require([item['sequence'] for item in history] == list(range(1, len(history) + 1)),
                'Original role history sequence differs')
        history_by_action: dict[str, list[dict]] = {}
        for item in history:
            history_by_action.setdefault(item['action_id'], []).append(item)
        for record in records:
            closed(record, 'id directive attempt state reason pid admitted request view action lease submitted_lease '
                   'reply result_ref publication_ref renewals renew_input renew_return lookup_done submission_started reconcile_only _digest')
            require(type(record['pid']) is int and record['pid'] > 0 and type(record['lookup_done']) is bool
                    and type(record['renewals']) is int and 0 <= record['renewals'] <= 3,
                    'Original role action fields differ')
            raw = canonical_bytes({key: value for key, value in record.items() if key != '_digest'})
            require(hashlib.sha256(raw).hexdigest() == record['_digest'], 'Noncanonical role action')
            transitions = history_by_action[record['id']]
            require(0 < len(transitions) <= 128 and transitions[0]['state'] == 'prepared'
                    and transitions[0]['reason'] == 'awaiting_local_evidence'
                    and all(item['pid'] > 0 and item['state'] in STATES for item in transitions), 'Original role history differs')
            # The writer intentionally caps transitions at 128. A capped history
            # cannot prove its terminal tail, so this success audit fails closed.
            require(len(transitions) < 128 and all(transitions[-1][name] == record[name]
                    for name in ('state', 'reason', 'pid')), 'Role terminal history is absent or truncated')
        decision_rows = list(decisions.execute('SELECT stage,directive,raw FROM decisions ORDER BY stage'))
        require(len(decision_rows) == len(records) and {r['directive'] for r in decision_rows} == {r['id'] for r in records},
                'Role decision/action membership differs')
        by_key = {r['directive']: r for r in decision_rows}
        results: dict[str, dict] = {}
        total = 0
        for result in controller_results:
            closed(result, 'protocol stage_id actor directive_id snapshot worker_request result_payload original_ref')
            total += len(canonical_bytes(result))
            require(total <= MAX_AGGREGATE and result['actor'] == actor and type(result['directive_id']) is str
                    and result['directive_id'] not in results, 'Duplicate, foreign or oversized controller result')
            results[result['directive_id']] = result
        require(set(results) == set(by_key), 'Controller result membership differs')
        dispatches, replies, stages, final_ids = [], [], [], []
        commands = set()
        for record in records:
            key = record['id']
            decision_row = by_key[key]
            stage, raw = decision_row['stage'], bytes(decision_row['raw'])
            decision = closed(strict_loads(raw), 'protocol stage_id actor placement work_ref directive ready_directive_ids central_decision_ref')
            require(canonical_bytes(decision) == raw and decision['directive'] == record['directive'], 'Original role directive differs')
            work_ref = from_dict(EvidenceRef, decision['work_ref'])
            work_raw = mesh.body(work_ref)
            work = strict_loads(work_raw)
            require(canonical_bytes(work) == work_raw, 'Noncanonical original work')
            mine = driver._work(work_ref, work)
            directive = WorkDirective.from_dict(record['directive'])
            mine_ids = tuple(directive_id(item) for item in mine)
            ready = decision['ready_directive_ids']
            require(stage == work['stage_id'] and directive in mine and type(ready) is list and 0 < len(ready) <= len(mine)
                    and all(type(item) is str for item in ready) and ready == sorted(set(ready))
                    and set(ready) <= set(mine_ids) and key == ready[0], 'Original ready decision differs')
            if driver.placement == 'peer_local':
                require(decision['central_decision_ref'] is None, 'Local decision carries central authority')
            else:
                central = from_dict(EvidenceRef, decision['central_decision_ref'])
                expected = {'protocol': ROLE_PROTOCOL, 'stage_id': stage, 'actor': actor,
                            'work_ref': to_dict(work_ref), 'directive_sha256': key}
                require(ready == [key] and central.producer == 'seed' and central.kind == 'cumulative-central-decision'
                        and mesh.body(central) == canonical_bytes(expected), 'Original central decision differs')
                assignment = {'protocol': ROLE_PROTOCOL, 'stage_id': stage, 'actor': actor,
                    'directive': directive.to_dict(), 'work_ref': to_dict(work_ref), 'central_decision_ref': to_dict(central)}
                wanted = canonical_bytes(assignment)
                require(any(mesh.body(ref) == wanted for ref in mesh.arrived()
                            if ref.producer == 'seed' and ref.kind == 'cumulative-assignment'
                            and ref.payload_sha256 == hashlib.sha256(wanted).hexdigest()), 'Original central assignment missing')
            _original(role_root / 'decisions' / (stage + '.json'), raw)
            decision_command = 'cumulative-decision-' + key
            mesh.command(decision_command, 'cumulative-decision', raw)
            commands.add(decision_command)
            built = materialize(directive, mesh, role_config['policy_sha256'])
            require(built is not None, 'Original materialized source is missing')
            assert built is not None
            request, view = built
            require(canonical_bytes(asdict(request)) == canonical_bytes(record['request'])
                    and to_dict(view) == record['view'], 'Original local request/view differs')
            snapshot = loop._snapshot(record)
            action, reply = snapshot.action, snapshot.reply
            require(snapshot.state == 'published' and snapshot.admitted and action is not None and reply is not None
                    and reply.state in ('completed', 'failed') and record['submission_started'] is True,
                    'Role lacks a published known terminal outcome')
            assert action is not None and reply is not None and reply.binding is not None
            request_raw = canonical_bytes({'worker_request': record['request'], 'view_manifest_sha256': identity(view)})
            request_ref = mesh.command('request-' + key, 'worker-request', request_raw)
            expected_action = ActionRequest(directive.context, 'action-' + key, 'dispatch-' + key, actor,
                directive.kind, directive.work, directive.profile_id, request_ref, identity(view))
            require(action == expected_action and reply.binding.action == action
                    and reply.binding.normalized_worker_request_sha256 == worker_request_digest(request)
                    and asdict(reply.binding.lease) == record['submitted_lease']
                    and reply.binding.lease.task_id == request.task_id, 'Original dispatch/request/lease differs')
            result_ref = snapshot.result_ref
            require(result_ref is not None and result_ref.producer == 'finance' and result_ref.kind == 'financial-result'
                    and result_ref.payload_sha256 == reply.result_payload_sha256, 'Original finance result reference differs')
            assert result_ref is not None
            result_payload = strict_loads(mesh.body(result_ref))
            publication_raw = canonical_bytes({'protocol': LOOP_PROTOCOL, 'actor': actor, 'action': record['action'],
                'reply': record['reply'], 'view_manifest': record['view'], 'result_ref': to_dict(result_ref)})
            publication = mesh.command('result-' + key, 'role-result', publication_raw)
            require(snapshot.publication_ref == publication, 'Original role-result reference differs')
            value = {'protocol': ROLE_PROTOCOL, 'stage_id': stage, 'actor': actor, 'directive_id': key,
                'snapshot': asdict(snapshot), 'worker_request': asdict(request), 'result_payload': result_payload}
            result_raw = canonical_bytes(value)
            _original(role_root / 'results' / (key + '.json'), result_raw)
            original_ref = mesh.command('cumulative-result-' + key, 'cumulative-result', result_raw)
            require(canonical_bytes(results[key]) == canonical_bytes({**value, 'original_ref': to_dict(original_ref)}),
                    'Controller return differs from original role result')
            commands.update(('request-' + key, 'result-' + key, 'cumulative-result-' + key))
            dispatches.append(reply.binding)
            replies.append(reply)
            stages.append(stage)
            final_ids.append(action.request_id)
        require(tuple(final_ids) == expected_final_action_ids, 'Final process action membership/order differs')
        require({r[0] for r in mesh_db.execute('SELECT command_id FROM commands')} == commands,
                'Orphan original role publication')
        _exact_files(role_root / 'decisions', {stage + '.json' for stage in stages}, maximum)
        _exact_files(role_root / 'results', {key + '.json' for key in by_key}, maximum)
        return RoleAudit(actor, tuple(dispatches), tuple(replies), tuple(r['id'] for r in records), tuple(stages), driver.placement,
                         tuple(sorted({item['pid'] for item in history})))
