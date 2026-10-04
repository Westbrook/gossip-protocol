"""Actual disposable financial/mesh originals; no sockets or provider calls."""
from dataclasses import replace
from pathlib import Path
import sqlite3
import unittest
from unittest.mock import patch

from gossip_harness.financial_rehearsal_originals_v1 import FinancialOriginals, OriginalFile, MAX_CALLS, audit_financial_originals
from gossip_harness.peer_financial_authority_v2 import FinancialError, canonical_payload, ledger_identity
from gossip_harness.peer_financial_authority_v4 import CumulativeAuthorityV4
from gossip_harness.peer_financial_terminal_v1 import sha
from gossip_harness.peer_mesh_finance_v2 import MeshFinancePayloads
from gossip_harness.peer_mesh_v2 import MeshConfig, MeshNode
from gossip_harness.peer_project_contract_v2 import ActionRequest
from gossip_harness.verification_journal import RequestJournal
from tests.financial_v4_fixture import Fixture


class FinancialRehearsalOriginalsV1Tests(Fixture, unittest.TestCase):
    def setUp(self):
        self.nodes = []
        self.real_payloads = None
        super().setUp()
        roster = self.child.actors + ('finance',)
        self.sender = MeshNode(MeshConfig(self.root / 'sender', self.actor, self.child.cohort,
                                         self.context.execution_contract_sha256, roster, 't' * 32))
        self.nodes.append(self.sender)
        self.finance_mesh = MeshNode(MeshConfig(self.root / 'finance-mesh', 'finance', self.child.cohort,
                                               self.context.execution_contract_sha256, roster, 't' * 32))
        self.nodes.append(self.finance_mesh)
        self.real_payloads = MeshFinancePayloads(self.root / 'payload-index', self.finance_mesh, self.child.actors)
        self.payloads = self.real_payloads

    def cleanup(self):
        self.transport.release.set()
        if self.authority is not None:
            self.authority.close()
            self.authority = None
        if self.real_payloads is not None:
            self.real_payloads.close()
        for node in self.nodes:
            node.close()
        super().cleanup()

    def action(self, number=0):
        task = self.authority.task_id(self.context, self.works[number])
        raw = canonical_payload({'worker_request': {'task_id': task, 'instructions': 'Fix source.',
            'allowed_paths': ['src/a.py'], 'files': {'src/a.py': 'broken\n'}, 'base_sha': 'c' * 40,
            'attempt': 1, 'feedback': ''}, 'view_manifest_sha256': 'd' * 64})
        ref = self.sender.publish('worker-request', raw, 'request-' + str(number))
        notice = self.sender.store.notice(ref)
        self.finance_mesh.store.merge([notice])
        self.assertTrue(self.finance_mesh.want(ref))
        for index in range(len(notice['chunks'])):
            self.finance_mesh.store.accept_chunk(ref.payload_sha256, index,
                                                 self.sender.store.read_chunk(ref.payload_sha256, index))
        return ActionRequest(self.context, 'action' + str(number), 'request' + str(number), self.actor,
                             'build', self.works[number], 'mini', ref, 'd' * 64)

    def originals(self):
        self.open()
        action, _ = self.start()
        reply = self.terminal(action)
        self.seal()
        config = self.authority.config
        config_sha = self.authority.config_sha256
        self.authority.close()
        self.authority = None
        self.real_payloads.close()
        for node in self.nodes:
            node.close()
        journal_files = tuple(OriginalFile(str(path), sha(path.read_bytes()), path.stat().st_size)
                              for path in sorted(Path(self.contract['journal_root']).glob('*.json')))
        return FinancialOriginals(ledger_identity(self.path), self.child.cohort, config, config_sha,
            ledger_identity(self.root / 'payload-index/payload-index.sqlite'), self.real_payloads.config,
            ledger_identity(self.root / 'finance-mesh/mesh.sqlite'), self.finance_mesh.config.identity(),
            journal_files, (reply.binding,), (reply,))

    def dumps(self, originals):
        result = []
        for expected in (originals.ledger_identity, originals.payload_index_identity, originals.mesh_database_identity):
            with sqlite3.connect('file:' + expected['path'] + '?mode=ro', uri=True) as db:
                result.append(tuple(db.iterdump()))
        return result

    def test_actual_originals_reconcile_without_owner_construction_or_mutation(self):
        originals = self.originals()
        before = self.dumps(originals)
        with patch.object(CumulativeAuthorityV4, '__init__', side_effect=AssertionError('No owner creation')), \
                patch.object(RequestJournal, '__init__', side_effect=AssertionError('No journal creation')), \
                patch.object(MeshFinancePayloads, '__init__', side_effect=AssertionError('No payload creation')):
            proof = audit_financial_originals(originals)
        self.assertEqual((proof.admitted, proof.known_failures, proof.spent_micro_usd), (1, 0, 166))
        self.assertEqual(proof.opening_micro_usd, 1000)
        self.assertFalse(proof.live_qualification)
        self.assertEqual(self.dumps(originals), before)

    def test_missing_or_substituted_controller_dispatch_is_rejected(self):
        originals = self.originals()
        with self.assertRaises(FinancialError):
            audit_financial_originals(replace(originals, dispatches=()))
        foreign = replace(originals.dispatches[0], call_id='foreign')
        with self.assertRaises(FinancialError):
            audit_financial_originals(replace(originals, dispatches=(foreign,)))

    def test_missing_or_changed_original_journal_file_is_rejected(self):
        originals = self.originals()
        with self.assertRaises(FinancialError):
            audit_financial_originals(replace(originals, journal_files=originals.journal_files[:-1]))
        path = Path(originals.journal_files[0].path)
        path.write_bytes(b'{}')
        with self.assertRaises(FinancialError):
            audit_financial_originals(originals)

    def test_orphan_unregistered_request_journal_does_not_disappear_from_census(self):
        originals = self.originals()
        (Path(self.contract['journal_root']) / 'unregistered.request.json').write_bytes(b'{}')
        with self.assertRaises(FinancialError):
            audit_financial_originals(originals)

    def test_result_hash_without_original_principal_publication_is_insufficient(self):
        originals = self.originals()
        with sqlite3.connect(originals.payload_index_identity['path']) as db:
            db.execute('DELETE FROM results')
            db.execute('UPDATE registration SET count=0')
        with self.assertRaises((FinancialError, ValueError)):
            audit_financial_originals(originals)

    def test_sql_snapshot_rehash_cannot_change_published_seal_binding(self):
        from gossip_harness.peer_financial_terminal_v1 import census
        originals = self.originals()
        with sqlite3.connect(self.path) as db:
            db.row_factory = sqlite3.Row
            db.execute('UPDATE reservations SET spent=spent+1 WHERE id<>?', ('historical-payment',))
            raw = census(db, self.child.cohort, originals.config_sha256)
            db.execute('UPDATE financial_terminal_seals_v4 SET snapshot=?,snapshot_sha=?', (raw, sha(raw)))
        with self.assertRaises(FinancialError):
            audit_financial_originals(originals)

    def test_original_database_inode_is_required(self):
        originals = self.originals()
        foreign = {**originals.payload_index_identity, 'inode': originals.payload_index_identity['inode'] + 1}
        with self.assertRaises(FinancialError):
            audit_financial_originals(replace(originals, payload_index_identity=foreign))

    def test_unsealed_actual_accounting_is_not_terminal_evidence(self):
        originals = self.originals()
        with sqlite3.connect(self.path) as db:
            db.execute('DELETE FROM financial_terminal_seals_v4')
        with self.assertRaises(FinancialError):
            audit_financial_originals(originals)

    def test_oversized_action_blob_is_rejected_before_census_or_private_reads(self):
        originals = self.originals()
        with sqlite3.connect(self.path) as db:
            db.execute('UPDATE financial_actions_v2 SET worker_request=zeroblob(600001)')
        with patch('gossip_harness.financial_rehearsal_originals_v1.census',
                   side_effect=AssertionError('Oversized BLOB must not reach census')):
            with self.assertRaisesRegex(FinancialError, 'original cell: financial_actions_v2'):
                audit_financial_originals(originals)

    def test_oversized_request_census_is_rejected_before_blob_reads(self):
        originals = self.originals()
        with sqlite3.connect(self.path) as db:
            db.executemany('''INSERT INTO financial_requests_v2
                SELECT cohort,actor,?,identity,identity_sha,reply,reply_sha
                FROM financial_requests_v2 WHERE request_id=?''',
                [('extra-request-' + str(i), originals.dispatches[0].action.request_id) for i in range(MAX_CALLS)])
        with patch('gossip_harness.financial_rehearsal_originals_v1.census',
                   side_effect=AssertionError('Oversized membership must not reach census')):
            with self.assertRaisesRegex(FinancialError, 'original row census: financial_requests_v2'):
                audit_financial_originals(originals)

    def test_oversized_payload_cell_is_rejected_before_inherited_audit(self):
        originals = self.originals()
        with sqlite3.connect(originals.payload_index_identity['path']) as db:
            db.execute('UPDATE results SET payload=zeroblob(2100001)')
        with patch.object(MeshFinancePayloads, '_audit', side_effect=AssertionError('Payload bytes must be bounded first')):
            with self.assertRaisesRegex(FinancialError, 'original cell: results'):
                audit_financial_originals(originals)

    def test_indirect_financial_table_is_rejected_before_census(self):
        originals = self.originals()
        with sqlite3.connect(self.path) as db:
            db.execute('ALTER TABLE financial_tasks_v2 RENAME TO hidden_membership')
            db.execute('CREATE VIEW financial_tasks_v2 AS SELECT * FROM hidden_membership')
        with patch('gossip_harness.financial_rehearsal_originals_v1.census',
                   side_effect=AssertionError('Indirect tables must not reach census')):
            with self.assertRaisesRegex(FinancialError, 'table schema missing or indirect'):
                audit_financial_originals(originals)

    def test_frozen_verifier_consumes_only_bounded_authenticated_journal_paths(self):
        from gossip_harness import verification_journal
        originals = self.originals()
        original_read = verification_journal._read
        consumed = []
        def bounded(path):
            self.assertNotIsInstance(path, Path)
            consumed.append(str(path))
            return original_read(path)
        with patch.object(verification_journal, '_read', side_effect=bounded):
            audit_financial_originals(originals)
        self.assertEqual(set(consumed), {item.path for item in originals.journal_files})

    def test_journal_replaced_after_precheck_is_rejected_at_consumption(self):
        from gossip_harness.financial_rehearsal_originals_v1 import _FinancialRead
        originals = self.originals()
        target = Path(next(item.path for item in originals.journal_files if item.path.endswith('.result.json')))
        original_verify = _FinancialRead._verify_terminal
        def replace_then_verify(view, actor, request_id):
            retained = target.with_suffix('.retained-for-test')
            target.rename(retained)
            target.symlink_to(retained)
            return original_verify(view, actor, request_id)
        with patch.object(_FinancialRead, '_verify_terminal', replace_then_verify):
            from gossip_harness.verification_journal import JournalCorrupt
            with self.assertRaises(JournalCorrupt):
                audit_financial_originals(originals)

    def test_same_binding_role_usage_cannot_disagree_with_financial_original(self):
        originals = self.originals()
        changed = replace(originals.terminal_replies[0], usage_units=originals.terminal_replies[0].usage_units + 1)
        with self.assertRaisesRegex(FinancialError, 'role reply differs'):
            audit_financial_originals(replace(originals, terminal_replies=(changed,)))

    def test_same_binding_role_result_digest_cannot_disagree_with_provider_original(self):
        originals = self.originals()
        changed = replace(originals.terminal_replies[0], result_payload_sha256='a' * 64)
        with self.assertRaisesRegex(FinancialError, 'role reply differs'):
            audit_financial_originals(replace(originals, terminal_replies=(changed,)))

    def test_same_binding_role_failure_cannot_relabel_completed_financial_original(self):
        originals = self.originals()
        changed = replace(originals.terminal_replies[0], state='failed')
        with self.assertRaisesRegex(FinancialError, 'role reply differs'):
            audit_financial_originals(replace(originals, terminal_replies=(changed,)))
