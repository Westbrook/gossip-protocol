"""Real disposable role journals and mesh stores; no sockets/provider/runtime."""
from copy import deepcopy
from dataclasses import asdict, replace
import hashlib
import os
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import patch

from gossip_harness.cumulative_role_originals_v1 import audit_role_originals
from gossip_harness.cumulative_study_role_v1 import PROTOCOL, RoleDriver, canonical_bytes, strict_loads
from gossip_harness.ledger import Lease
from gossip_harness.peer_financial_authority_v2 import ledger_identity
from gossip_harness.peer_mesh_v2 import MeshConfig, MeshNode
from gossip_harness.peer_mesh_store_v2 import MeshStore
from gossip_harness.peer_project_contract_v2 import Context, DispatchBinding, DispatchReply, WorkKey, identity, to_dict, worker_request_digest
from gossip_harness.peer_role_loop_v2 import RoleLoop, WorkDirective, directive_id, financial_task_id
from gossip_harness.worker import WorkerRequest, WorkerResult


class _Finance:
    def __init__(self, fixture):
        self.fixture = fixture
        self.replies = {}

    def claim(self, context, work, request_id, *, ttl=60):
        return Lease(financial_task_id(context, work), self.fixture.actor, 1, time.time() + ttl)

    def renew(self, lease, request_id, *, ttl=60):
        return replace(lease, expires_at=time.time() + ttl)

    def lookup(self, request_id):
        return self.replies.get(request_id)

    def submit(self, action, lease):
        body = strict_loads(self.fixture.role.resolve(action.worker_payload_ref))['worker_request']
        body['allowed_paths'] = tuple(body['allowed_paths'])
        binding = DispatchBinding(action, lease, worker_request_digest(WorkerRequest(**body)),
            'c' * 64, 'd' * 64, 'call-' + action.request_id, 'reservation', 10)
        raw = canonical_bytes({'kind': 'result', 'payload': asdict(WorkerResult({'src/a.py': 'fixed\n'}, 'fixture', 7, {}))})
        self.fixture.arrive(self.fixture.finance_node, 'financial-result', raw, 'financial-' + action.request_id)
        reply = DispatchReply(action.request_id, identity(action), 'completed', 'fixture', binding,
                              hashlib.sha256(raw).hexdigest(), 7)
        self.replies[action.request_id] = reply
        return reply


class CumulativeRoleOriginalsV1Tests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.role_root = self.root / 'role'
        self.actor = 'child.B01'
        self.config = {'protocol': PROTOCOL, 'actor': self.actor, 'trajectory_id': 'S4-G.healthy',
            'placement': 'peer_local', 'policy_sha256': 'd' * 64, 'call_limit': 4, 'max_actions': 8,
            'requirements_by_milestone': {str(i): str(i) * 64 for i in range(1, 5)}, 'crash_once': False,
            'deadline_unix': 4_000_000_000.0,
            'finance': {'port': 12345, 'capability': 'c' * 32, 'contract_sha256': 'a' * 64},
            'mesh': {'root': str(self.role_root / 'mesh'), 'node_id': self.actor, 'cohort_id': 'cohort',
                'execution_contract_sha256': 'a' * 64, 'roster': [self.actor, 'seed', 'finance'], 'transport_key': 'b' * 32}}
        mesh_config = MeshConfig.from_dict(self.config['mesh'])
        self.role = MeshNode(mesh_config)
        self.seed = MeshNode(replace(mesh_config, root=self.root / 'seed', node_id='seed'))
        self.finance_node = MeshNode(replace(mesh_config, root=self.root / 'finance', node_id='finance'))
        self.nodes = [self.role, self.seed, self.finance_node]
        self.driver = None

    def tearDown(self):
        if self.driver is not None:
            self.driver.close()
        for node in self.nodes:
            node.close()
        self.temporary.cleanup()

    def arrive(self, sender, kind, raw, command):
        ref = sender.publish(kind, raw, command)
        notice = sender.store.notice(ref)
        self.role.store.merge([notice])
        self.assertTrue(self.role.want(ref))
        for index in range(len(notice['chunks'])):
            self.role.store.accept_chunk(ref.payload_sha256, index, sender.store.read_chunk(ref.payload_sha256, index))
        return ref

    def originals(self, placement='peer_local'):
        self.config['placement'] = placement
        self.role_root.mkdir(exist_ok=True)
        (self.role_root / 'config.json').write_bytes(canonical_bytes(self.config))
        source = self.arrive(self.seed, 'project-source', canonical_bytes({'files': {'src/a.py': 'old\n'},
            'base_sha': 'a' * 40}), 'source')
        context = Context('a' * 64, 'cohort', 'S4-G.healthy', 1, '1' * 64)
        directive = WorkDirective(context, WorkKey('catalog', 'req', self.actor, 0),
            'mini', 'build', source, (), ('src/a.py',), 'Implement public requirement')
        key = directive_id(directive)
        work = {'protocol': PROTOCOL, 'stage_id': 'M1-build', 'context': to_dict(context), 'source_ref': to_dict(source),
            'evidence_refs': [], 'directives': [{'actor': self.actor, 'directive': directive.to_dict()}], 'placement': placement}
        work_ref = self.arrive(self.seed, 'cumulative-work', canonical_bytes(work), 'work')
        if placement == 'durable_central_scheduler':
            central = {'protocol': PROTOCOL, 'stage_id': 'M1-build', 'actor': self.actor,
                'work_ref': to_dict(work_ref), 'directive_sha256': key}
            central_ref = self.arrive(self.seed, 'cumulative-central-decision', canonical_bytes(central), 'central')
            assignment = {'protocol': PROTOCOL, 'stage_id': 'M1-build', 'actor': self.actor,
                'directive': directive.to_dict(), 'work_ref': to_dict(work_ref), 'central_decision_ref': to_dict(central_ref)}
            self.arrive(self.seed, 'cumulative-assignment', canonical_bytes(assignment), 'assignment')
        self.driver = RoleDriver(self.role_root, self.config, self.role, _Finance(self))
        for _ in range(32):
            self.driver.tick()
            snapshots = self.driver.loop.snapshots()
            if snapshots and snapshots[0].state == 'published':
                break
        self.assertEqual(snapshots[0].state, 'published')
        result = strict_loads((self.role_root / 'results' / (key + '.json')).read_bytes())
        ref, = (r for r in self.role.arrived() if r.producer == self.actor and r.kind == 'cumulative-result')
        returned = {**result, 'original_ref': to_dict(ref)}
        self.driver.close()
        self.driver = None
        for node in self.nodes:
            node.close()
        paths = [self.role_root / p for p in ('decisions.sqlite', 'journal/role.sqlite', 'mesh/mesh.sqlite')]
        return {'actor': self.actor, 'role_config': deepcopy(self.config), 'role_root': self.role_root,
            'database_identities': {str(path): ledger_identity(path) for path in paths},
            'controller_results': (returned,), 'expected_final_action_ids': (snapshots[0].action.request_id,)}

    def dumps(self, values):
        result = []
        for path in values['database_identities']:
            with sqlite3.connect(Path(path).as_uri() + '?mode=ro', uri=True) as db:
                result.append(tuple(db.iterdump()))
        return result

    def test_actual_role_originals_cross_bind_without_owner_or_mutation(self):
        values = self.originals()
        before = self.dumps(values)
        with patch.object(RoleDriver, '__init__', side_effect=AssertionError('No role owner')), \
             patch.object(RoleLoop, '__init__', side_effect=AssertionError('No loop owner')), \
             patch.object(MeshStore, '__init__', side_effect=AssertionError('No store owner')), \
             patch.object(MeshStore, '_transaction', side_effect=AssertionError('No write transactions')):
            proof = audit_role_originals(**values)
        self.assertEqual(len(proof.dispatches), 1)
        self.assertEqual(proof.stages, ('M1-build',))
        self.assertEqual(proof.observed_process_ids, (os.getpid(),))
        self.assertFalse(proof.live_qualification)
        self.assertEqual(self.dumps(values), before)

    def test_actual_central_assignment_and_original_decision_are_bound(self):
        values = self.originals('durable_central_scheduler')
        self.assertEqual(audit_role_originals(**values).placement, 'durable_central_scheduler')

    def test_controller_omission_duplicate_and_substitution_rejected(self):
        values = self.originals()
        altered = deepcopy(values['controller_results'][0])
        altered['worker_request']['instructions'] = 'forged'
        for results in ((), values['controller_results'] * 2, (altered,)):
            with self.subTest(results=len(results)), self.assertRaises(ValueError):
                audit_role_originals(**{**values, 'controller_results': results})

    def test_final_process_action_membership_is_exact(self):
        values = self.originals()
        with self.assertRaisesRegex(ValueError, 'Final process action'):
            audit_role_originals(**{**values, 'expected_final_action_ids': ()})

    def test_original_database_inode_and_config_are_required(self):
        values = self.originals()
        identities = deepcopy(values['database_identities'])
        identities[str(self.role_root / 'decisions.sqlite')]['inode'] += 1
        with self.assertRaises(ValueError):
            audit_role_originals(**{**values, 'database_identities': identities})
        (self.role_root / 'config.json').write_bytes(b'{}')
        with self.assertRaisesRegex(ValueError, 'original file differs'):
            audit_role_originals(**values)

    def test_orphan_original_result_file_is_rejected(self):
        values = self.originals()
        (self.role_root / 'results' / 'orphan.json').write_bytes(b'{}')
        with self.assertRaisesRegex(ValueError, 'file membership'):
            audit_role_originals(**values)

    def test_missing_original_decision_and_result_do_not_recover(self):
        values = self.originals()
        (self.role_root / 'decisions' / 'M1-build.json').unlink()
        with self.assertRaises((OSError, ValueError)):
            audit_role_originals(**values)
        self.assertFalse((self.role_root / 'decisions' / 'M1-build.json').exists())

    def test_oversized_action_blob_is_rejected_before_private_records(self):
        values = self.originals()
        with sqlite3.connect(self.role_root / 'journal/role.sqlite') as db:
            db.execute('UPDATE actions SET body=zeroblob(512001)')
        with patch.object(RoleLoop, '_records', side_effect=AssertionError('Bound first')):
            with self.assertRaisesRegex(ValueError, 'role cell: actions'):
                audit_role_originals(**values)

    def test_oversized_decision_census_is_rejected_before_private_reads(self):
        values = self.originals()
        with sqlite3.connect(self.role_root / 'decisions.sqlite') as db:
            db.executemany('INSERT INTO decisions SELECT ?,?,raw FROM decisions WHERE stage=?',
                          [('extra-' + str(i), str(i), 'M1-build') for i in range(8)])
        with patch.object(RoleDriver, '_audit', side_effect=AssertionError('Bound first')):
            with self.assertRaisesRegex(ValueError, 'row census: decisions'):
                audit_role_originals(**values)

    def test_rehashed_action_substitution_cannot_escape_original_materialization(self):
        values = self.originals()
        with sqlite3.connect(self.role_root / 'journal/role.sqlite') as db:
            raw, = db.execute('SELECT body FROM actions').fetchone()
            value = strict_loads(raw)
            value['request']['files']['src/a.py'] = 'substituted\n'
            raw = canonical_bytes(value)
            db.execute('UPDATE actions SET body=?,digest=?', (raw, hashlib.sha256(raw).hexdigest()))
        with self.assertRaisesRegex(ValueError, 'local request/view'):
            audit_role_originals(**values)

    def test_missing_local_request_command_does_not_pass_with_only_report_hash(self):
        values = self.originals()
        with sqlite3.connect(self.role_root / 'mesh/mesh.sqlite') as db:
            db.execute("DELETE FROM commands WHERE command_id LIKE 'request-%'")
        with self.assertRaisesRegex(ValueError, 'Missing local command'):
            audit_role_originals(**values)

    def test_history_tail_must_match_the_original_terminal_snapshot(self):
        values = self.originals()
        with sqlite3.connect(self.role_root / 'journal/role.sqlite') as db:
            db.execute('DELETE FROM history WHERE sequence=(SELECT max(sequence) FROM history)')
        with self.assertRaisesRegex(ValueError, 'terminal history'):
            audit_role_originals(**values)

    def test_noncanonical_rehashed_action_is_rejected(self):
        values = self.originals()
        with sqlite3.connect(self.role_root / 'journal/role.sqlite') as db:
            raw, = db.execute('SELECT body FROM actions').fetchone()
            raw += b' '
            db.execute('UPDATE actions SET body=?,digest=?', (raw, hashlib.sha256(raw).hexdigest()))
        with self.assertRaisesRegex(ValueError, 'Noncanonical role action'):
            audit_role_originals(**values)

    def test_absent_arrived_source_payload_never_starts_a_subscription(self):
        values = self.originals()
        with sqlite3.connect(self.role_root / 'mesh/mesh.sqlite') as db:
            rows = db.execute('SELECT sha,notice FROM events WHERE producer=?', ('seed',)).fetchall()
            source_sha, = (sha for sha, raw in rows if strict_loads(raw)['kind'] == 'project-source')
            db.execute('DELETE FROM chunks WHERE sha=?', (source_sha,))
            db.execute('DELETE FROM payloads WHERE sha=?', (source_sha,))
        with patch.object(MeshStore, 'want', side_effect=AssertionError('No writable wants')):
            with self.assertRaisesRegex(ValueError, 'evidence is absent'):
                audit_role_originals(**values)


if __name__ == '__main__':
    unittest.main()
