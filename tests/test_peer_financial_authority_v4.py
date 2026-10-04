from dataclasses import replace
import sqlite3
import threading
import unittest
from unittest.mock import patch

from gossip_harness.candidate_checkpoint_chain_v1 import ChainError
from gossip_harness.peer_financial_authority_v2 import CumulativeAuthorityV2, FinancialError
from gossip_harness.peer_financial_authority_v4 import digest
from gossip_harness.peer_financial_terminal_v1 import CohortSealed, SealBusy
from tests.financial_v4_fixture import Fixture


class PeerFinancialAuthorityV4Tests(Fixture, unittest.TestCase):
    def test_completed_replay_postseal_no_admission_or_cost_changes(self):
        self.open()
        action, lease = self.start()
        reply = self.terminal(action)
        self.seal()
        before = self.snapshot()
        self.assertEqual(self.authority.submit(self.actor, action, lease), reply)
        self.assertEqual(self.authority.lookup(self.actor, action.request_id), reply)
        with self.assertRaises(CohortSealed):
            self.authority.renew(self.actor, lease)
        with self.assertRaises(CohortSealed):
            self.authority.claim(self.actor, self.context, self.works[1])
        with self.assertRaises(CohortSealed):
            self.authority.submit(self.actor, self.action(1), lease)
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(len(self.transport.calls), 1)

    def test_active_workers_remain_parallel_and_prevent_seal(self):
        self.open()
        self.transport.release.clear()
        first, _ = self.start(0)
        second, _ = self.start(1)
        self.assertTrue(self.transport.entered.wait(2))
        # Wait for both worker entry events without depending on provider timing.
        import time
        until = time.monotonic() + 2
        while len(self.transport.calls) < 2 and time.monotonic() < until:
            time.sleep(.002)
        self.assertEqual(len(self.transport.calls), 2)
        with self.assertRaises(SealBusy):
            self.authority.prepare_terminal_snapshot(self.evidence())
        self.transport.release.set()
        self.terminal(first)
        self.terminal(second)
        self.seal()
        self.assertTrue(self.authority.slots.acquire(False))
        self.assertTrue(self.authority.slots.acquire(False))
        self.assertFalse(self.authority.slots.acquire(False))
        self.authority.slots.release()
        self.authority.slots.release()

    def test_queued_admission_blocks_seal_before_execute_token(self):
        self.open()
        with patch.object(self.authority.executor, 'submit') as queue:
            self.start()
            queue.assert_called_once()
            self.assertFalse(self.authority.active_executions)
            with self.assertRaises(SealBusy):
                self.authority.prepare_terminal_snapshot(self.evidence())
        self.authority.slots.release()  # Fixture queue deliberately never ran.

    def test_before_invoke_gap_has_active_token(self):
        reached, release = threading.Event(), threading.Event()
        def hook(point, request):
            if point == 'before_invoke':
                reached.set()
                if not release.wait(3):
                    raise RuntimeError('fixture deadline')
        self.open(crash_hook=hook)
        action, _ = self.start()
        try:
            self.assertTrue(reached.wait(2))
            self.assertTrue(self.authority.active_executions)
            with self.assertRaises(SealBusy):
                self.authority.prepare_terminal_snapshot(self.evidence())
            self.assertEqual(len(self.transport.calls), 0)
        finally:
            release.set()
        self.terminal(action)

    def test_changed_census_after_preparation_is_not_sealed(self):
        self.open()
        prepared = self.authority.prepare_terminal_snapshot(self.evidence())
        self.authority.claim(self.actor, self.context, self.works[0])
        with self.assertRaises(FinancialError):
            self.authority.seal_terminal(prepared)
        self.assertEqual(self.rows('financial_terminal_seals_v4'), [])

    def test_rollback_before_sql_commit_preserves_accounting(self):
        self.open()
        prepared = self.authority.prepare_terminal_snapshot(self.evidence())
        before = self.snapshot()
        def crash(point, request):
            if point == 'before_terminal_sql_commit':
                raise RuntimeError('fixture rollback')
        self.authority.crash_hook = crash
        with self.assertRaises(RuntimeError):
            self.authority.seal_terminal(prepared)
        self.assertEqual(self.rows('financial_terminal_seals_v4'), [])
        self.assertEqual(self.snapshot(), before)

    def test_sql_commit_publication_crash_recovery_stays_sealed(self):
        self.open()
        prepared = self.authority.prepare_terminal_snapshot(self.evidence())
        def crash(point, request):
            if point == 'after_terminal_sql_commit':
                raise RuntimeError('fixture SQL committed before publication')
        self.authority.crash_hook = crash
        with self.assertRaises(RuntimeError):
            self.authority.seal_terminal(prepared)
        self.assertEqual(len(self.rows('financial_terminal_seals_v4')), 1)
        before = self.snapshot()
        with patch.object(CumulativeAuthorityV2, '_recover', side_effect=AssertionError('mutable recovery forbidden')):
            self.open(recovery=True)
        self.assertEqual(self.snapshot(), before)
        with self.assertRaises(CohortSealed):
            self.authority.claim(self.actor, self.context, self.works[0])
        seal = self.authority.publish_terminal_seal()
        self.assertEqual(self.authority.verified_terminal_seal(self.chain.commitment).raw, seal.raw)
        self.assertEqual(len(self.transport.calls), 0)

    def test_lost_anchor_ack_leaves_sql_closed_and_no_implicit_retry(self):
        self.open()
        prepared = self.authority.prepare_terminal_snapshot(self.evidence())
        original = self.head.compare_and_set
        def lost(expected, proposed):
            original(expected, proposed)
            raise OSError('fixture lost acknowledgement')
        with patch.object(self.head, 'compare_and_set', side_effect=lost):
            with self.assertRaises(ChainError):
                self.authority.seal_terminal(prepared)
        self.assertTrue(self.chain.uncertain)
        with self.assertRaises(CohortSealed):
            self.authority.claim(self.actor, self.context, self.works[0])
        with self.assertRaises(ChainError):
            self.authority.publish_terminal_seal()
        self.assertEqual(len(self.rows('financial_terminal_seals_v4')), 1)

    def test_published_seal_deletion_and_terminal_config_deletion_fail_recovery(self):
        self.open()
        self.seal()
        self.authority.close()
        self.authority = None
        with sqlite3.connect(self.path) as db:
            db.execute('DELETE FROM financial_terminal_seals_v4')
        with self.assertRaises(FinancialError):
            self.open(recovery=True)
        with sqlite3.connect(self.path) as db:
            db.execute('DELETE FROM financial_terminal_config_v4')
        with self.assertRaises(FinancialError):
            self.open(recovery=True)

    def test_unknown_preserves_reservation_halt_and_blocks_next_child(self):
        self.transport.error = TimeoutError('fixture unknown provider outcome')
        self.open()
        action, _ = self.start()
        reply = self.terminal(action)
        self.assertEqual(reply.state, 'unknown')
        before = self.rows('reservations')
        with self.assertRaises(FinancialError):
            self.seal()
        self.seal('stopped_failure')
        self.assertEqual(self.rows('reservations'), before)
        self.assertTrue(self.authority.failed_closed)
        self.select(1)
        with self.assertRaises(FinancialError):
            self.open()

    def test_postseal_evidence_failure_does_not_rewrite_accounting_outcomes(self):
        self.open()
        action, _ = self.start()
        reply = self.terminal(action)
        self.seal()
        before = self.rows('financial_actions_v2'), self.rows('financial_requests_v2'), self.rows('reservations')
        self.payloads.values.pop((self.actor, reply.result_payload_sha256))
        self.assertEqual(self.authority.lookup(self.actor, action.request_id).state, 'unknown')
        self.assertEqual((self.rows('financial_actions_v2'), self.rows('financial_requests_v2'), self.rows('reservations')), before)
        with self.assertRaises((FinancialError, FileNotFoundError)):
            self.authority.verified_terminal_seal(self.chain.commitment)

    def test_exact_seal_replay_and_substituted_boundary_rejected(self):
        self.open()
        prepared = self.authority.prepare_terminal_snapshot(self.evidence())
        seal = self.authority.seal_terminal(prepared)
        self.assertEqual(self.authority.seal_terminal(prepared), seal)
        changed = replace(prepared, boundary=replace(prepared.boundary, inventory_sha256='0' * 64))
        with self.assertRaises(FinancialError):
            self.authority.seal_terminal(changed)

    def test_foreign_suffix_revokes_seal_publication_authority(self):
        self.open()
        prepared = self.authority.prepare_terminal_snapshot(self.evidence())
        (self.root / 'raw/foreign.json').write_bytes(b'{}')
        with self.assertRaises(ChainError):
            self.authority.seal_terminal(prepared)
        self.assertEqual(self.rows('financial_terminal_seals_v4'), [])

    def test_v3_permit_and_unqualified_live_mode_are_rejected(self):
        permit = self.permit(1000)
        permit['protocol'] = 'peer-financial-operator-permit-v3'
        with self.assertRaises(FinancialError):
            self.open(permit=permit, expected_permit_sha256=digest(permit))
        permit = self.permit(1000)
        permit['mode'] = 'live'
        with self.assertRaises(FinancialError):
            self.open(permit=permit, expected_permit_sha256=digest(permit), mode='live')
        self.assertEqual(len(self.transport.calls), 0)

    def test_closed_or_foreign_process_cannot_seal(self):
        self.open()
        prepared = self.authority.prepare_terminal_snapshot(self.evidence())
        with patch('gossip_harness.peer_financial_authority_v2.os.getpid', return_value=-1):
            with self.assertRaises(FinancialError):
                self.authority.seal_terminal(prepared)
        self.authority.close()
        with self.assertRaises(FinancialError):
            self.authority.seal_terminal(prepared)

    def test_waiting_request_postseal_cannot_be_admitted(self):
        self.open()
        action = self.action()
        lease = self.authority.claim(self.actor, self.context, self.works[0])
        self.authority.slots.acquire(False)
        self.authority.slots.acquire(False)
        try:
            self.assertEqual(self.authority.submit(self.actor, action, lease).state, 'waiting')
        finally:
            self.authority.slots.release()
            self.authority.slots.release()
        self.seal()
        before = self.snapshot()
        with self.assertRaises(CohortSealed):
            self.authority.submit(self.actor, action, lease)
        self.assertEqual(self.snapshot(), before)

    def test_publication_pending_is_busy_even_for_stopped_failure(self):
        self.open()
        self.payloads.fail_publication = True
        # Prepare request bytes before deliberately failing result publication.
        self.payloads.fail_publication = False
        action = self.action()
        lease = self.authority.claim(self.actor, self.context, self.works[0])
        self.payloads.fail_publication = True
        self.authority.submit(self.actor, action, lease)
        reply = self.terminal(action)
        self.assertEqual(reply.state, 'publication_pending')
        with self.assertRaises(SealBusy):
            self.authority.prepare_terminal_snapshot(self.evidence('stopped_failure'))

    def test_unknown_seal_check_failure_preserves_in_memory_fence(self):
        self.open()
        with patch.object(self.authority.ledger, 'atomic', side_effect=OSError('fixture DB unavailable')):
            self.assertIsNone(self.authority._unknown(self.actor, 'request0', 'fixture'))
        self.assertTrue(self.authority.failed_closed)
        self.assertIn((self.actor, 'request0'), self.authority.persistence_failed)

    def test_token_registration_failure_records_unknown_and_releases_once(self):
        self.open()
        with patch.object(self.authority.executor, 'submit'):
            action, _ = self.start()
        original = self.authority.assert_mutations_open
        with patch.object(self.authority, 'assert_mutations_open', side_effect=FinancialError('fixture registration failure')):
            with self.assertRaises(FinancialError):
                self.authority._execute(self.actor, action.request_id)
        self.assertEqual(self.authority.lookup(self.actor, action.request_id).state, 'unknown')
        self.assertTrue(self.authority.failed_closed)
        self.assertEqual(len(self.transport.calls), 0)
        self.assertTrue(self.authority.slots.acquire(False))
        self.assertTrue(self.authority.slots.acquire(False))
        self.assertFalse(self.authority.slots.acquire(False))
        self.authority.slots.release()
        self.authority.slots.release()
        self.assertTrue(callable(original))

    def test_persistence_failure_replay_never_returns_pending_or_success(self):
        self.open()
        with patch.object(self.authority.executor, 'submit'):
            action, lease = self.start()
        self.authority.persistence_failed.add((self.actor, action.request_id))
        self.assertEqual(self.authority.submit(self.actor, action, lease).state, 'unknown')
        self.authority.slots.release()

    def test_recovery_cannot_publish_unrelated_later_prefix(self):
        self.open()
        prepared = self.authority.prepare_terminal_snapshot(self.evidence())
        def crash(point, request):
            if point == 'after_terminal_sql_commit':
                raise RuntimeError('fixture before publication')
        self.authority.crash_hook = crash
        with self.assertRaises(RuntimeError):
            self.authority.seal_terminal(prepared)
        self.chain.retain('unrelated.json', b'{}')
        with self.assertRaises(ChainError):
            self.authority.publish_terminal_seal()
        with self.assertRaises(CohortSealed):
            self.authority.claim(self.actor, self.context, self.works[0])

    def test_recovery_after_lost_ack_needs_independently_observed_exact_head(self):
        from gossip_harness.candidate_checkpoint_chain_v1 import CheckpointChain
        self.open()
        prepared = self.authority.prepare_terminal_snapshot(self.evidence())
        original = self.head.compare_and_set
        def lost(expected, proposed):
            original(expected, proposed)
            raise OSError('fixture acknowledgement lost')
        with patch.object(self.head, 'compare_and_set', side_effect=lost):
            with self.assertRaises(ChainError):
                self.authority.seal_terminal(prepared)
        self.authority.close()
        self.authority = None
        independently_observed = self.head.read()
        self.chain.close()
        self.chain = CheckpointChain.reopen(self.root / 'raw', self.root / 'delta',
            context={'fixture_study': self.roster.sha256}, authority=self.head, expected=independently_observed)
        self.open(recovery=True)
        receipt = self.authority.publish_terminal_seal()
        self.assertEqual(receipt.commitment, independently_observed)
        self.assertEqual(len(self.transport.calls), 0)

    def test_memory_only_integrity_halt_revokes_completed_seal_proof(self):
        self.open()
        self.seal()
        with patch.object(self.authority.ledger, 'atomic', side_effect=OSError('fixture failed integrity-halt write')):
            self.authority._unknown(self.actor, 'request0', 'fixture corruption')
        with self.assertRaises(FinancialError):
            self.authority.verified_terminal_seal(self.chain.commitment)
        with self.assertRaises(FinancialError):
            self.authority.publish_terminal_seal()

    def test_changed_prior_census_and_rehashed_sql_snapshot_cannot_authorize_handoff(self):
        from gossip_harness.peer_financial_terminal_v1 import census, sha
        self.open()
        seal = self.seal()
        financial_sha = self.authority.config_sha256
        task = self.authority.task_id(self.context, self.works[0])
        with sqlite3.connect(self.path) as db:
            db.row_factory = sqlite3.Row
            db.execute('UPDATE tasks SET epoch=epoch+1 WHERE id=?', (task,))
            raw = census(db, self.child.cohort, financial_sha)
            db.execute('UPDATE financial_terminal_seals_v4 SET snapshot=?,snapshot_sha=? WHERE cohort=?',
                       (raw, sha(raw), self.child.cohort))
        self.assertEqual(self.chain.read(seal.publication_name), seal.raw)
        self.select(1)
        with self.assertRaises(FinancialError):
            self.open()

    def test_live_qualification_cannot_be_enabled_by_schematic_receipts(self):
        from gossip_harness.peer_financial_authority_v4 import validate_qualification
        with self.assertRaisesRegex(FinancialError, 'unavailable'):
            validate_qualification({'protocol': 'peer-financial-qualification-v4', 'passed': True}, {}, {})

    def test_keyless_preflight_preserves_v4_identity_and_requires_roster(self):
        from gossip_harness.peer_financial_authority_v4 import CumulativeAuthorityV4
        permit = self.permit(1000)
        arguments = dict(workers={'mini': self.worker}, permit=permit, expected_permit_sha256=digest(permit),
                         max_workers=2, mode='fixture')
        with self.assertRaises(FinancialError):
            CumulativeAuthorityV4.preflight_permit(self.contract, 800_000, 1_000_000, 1000, **arguments)
        result = CumulativeAuthorityV4.preflight_permit(self.contract, 800_000, 1_000_000, 1000,
                    terminal_roster=self.roster, **arguments)
        self.assertEqual(result['protocol'], 'peer-financial-authority-v4')
        self.assertFalse(result['ledger_opened'])

    def test_rebound_child_contract_cannot_enroll_under_different_study_contract(self):
        # Rebuild every child-side binding coherently; only the unchanged
        # prospective study roster distinguishes this from a valid permit.
        self.contract['execution_contract_sha256'] = 'c' * 64
        self.context = replace(self.context, execution_contract_sha256='c' * 64)
        for spec in self.contract['task_specs']:
            spec['context']['execution_contract_sha256'] = 'c' * 64
        before = self.rows('tasks'), self.rows('reservations')
        with self.assertRaisesRegex(FinancialError, 'execution contract differs'):
            self.open()  # Fixture recomputes the entire permit and its pin.
        self.assertEqual((self.rows('tasks'), self.rows('reservations')), before)
        with sqlite3.connect(self.path) as db:
            self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='financial_cohorts_v2'").fetchone())

    def test_rebound_child_contract_cannot_pass_keyless_preflight(self):
        from gossip_harness.peer_financial_authority_v4 import CumulativeAuthorityV4
        self.contract['execution_contract_sha256'] = 'c' * 64
        self.context = replace(self.context, execution_contract_sha256='c' * 64)
        for spec in self.contract['task_specs']:
            spec['context']['execution_contract_sha256'] = 'c' * 64
        permit = self.permit(1000)
        with self.assertRaisesRegex(FinancialError, 'execution contract differs'):
            CumulativeAuthorityV4.preflight_permit(self.contract, 800_000, 1_000_000, 1000,
                workers={'mini': self.worker}, permit=permit, expected_permit_sha256=digest(permit),
                max_workers=2, mode='fixture', terminal_roster=self.roster)
