from dataclasses import replace
import unittest

from gossip_harness.peer_financial_authority_v2 import FinancialError, ledger_identity
from gossip_harness.peer_financial_terminal_v1 import ChildRegistration, TerminalRoster, verified_study_barrier
from tests.financial_v4_fixture import Fixture


class PeerFinancialTerminalV1Tests(Fixture, unittest.TestCase):
    def test_exact_six_children_96_unique_roles(self):
        self.assertEqual(sum(len(c.actors) for c in self.roster.children), 96)
        with self.assertRaises(FinancialError):
            replace(self.roster, children=self.roster.children[:-1])
        with self.assertRaises(FinancialError):
            ChildRegistration('child', 'trajectory', ('same',) * 8)
        with self.assertRaises(FinancialError):
            TerminalRoster('a' * 64, (self.roster.children[0],) * 6)

    def test_missing_duplicate_or_wrong_order_role_stops_rejected(self):
        self.open()
        request = self.evidence()
        for changed in (replace(request, role_stops=request.role_stops[:-1]),
                        replace(request, role_stops=(request.role_stops[0],) * 8),
                        replace(request, role_stops=tuple(reversed(request.role_stops)))):
            with self.assertRaises(FinancialError):
                self.authority.prepare_terminal_snapshot(changed)
        self.assertEqual(self.rows('financial_terminal_seals_v4'), [])

    def test_six_sequential_originals_form_scoped_barrier_and_preserve_wallet(self):
        old = self.rows('reservations')
        seals = []
        for index in range(6):
            self.select(index)
            self.open()
            self.authority.claim(self.actor, self.context, self.works[0], ttl=600)
            seals.append(self.seal())
        barrier = verified_study_barrier(self.roster, self.chain, self.chain.commitment, tuple(seals), existing_ledger_path=self.path, expected_ledger_identity=ledger_identity(self.path))
        self.assertEqual(barrier['role_count'], 96)
        self.assertFalse(barrier['acceptance_authority'])
        self.assertEqual(self.rows('reservations'), old)
        self.assertEqual(self.ledger.budget()['spent_or_reserved'], 1000)
        with self.assertRaises(FinancialError):
            verified_study_barrier(self.roster, self.chain, self.chain.commitment, tuple(reversed(seals)), existing_ledger_path=self.path, expected_ledger_identity=ledger_identity(self.path))

    def test_successor_requires_exact_prior_publication(self):
        self.select(1)
        with self.assertRaises(FinancialError):
            self.open()

    def test_declared_evidence_is_not_private_quality_acceptance(self):
        self.open()
        seal = self.seal()
        self.assertFalse(seal.acceptance_authority)
        with self.assertRaises(TypeError):
            type(seal)(seal.raw, seal.publication_name, seal.commitment, acceptance_authority=True)

    def test_forged_but_independently_retained_seal_is_not_a_financial_original(self):
        from gossip_harness.peer_financial_terminal_v1 import ChildTerminalSeal, seal_name
        from gossip_harness.peer_financial_authority_v2 import canonical_payload
        self.open()
        request = self.evidence()
        raw = canonical_payload({'seal_id': 'invented', 'terminal_config': self.authority.terminal_config,
                                 'request': request.record()})
        name = seal_name(self.child.cohort)
        self.chain.retain(name, raw)
        receipt = ChildTerminalSeal(raw, name, self.chain.commitment)
        with self.assertRaises(FinancialError):
            verified_study_barrier(self.roster, self.chain, self.chain.commitment, (receipt,) * 6,
                existing_ledger_path=self.path, expected_ledger_identity=ledger_identity(self.path))

    def complete_six(self):
        seals = []
        for index in range(6):
            self.select(index)
            self.open()
            seals.append(self.seal())
        self.authority.close()
        self.authority = None
        return tuple(seals)

    def test_persistent_ledger_replacement_during_barrier_is_rejected(self):
        import os
        import sqlite3
        from unittest.mock import patch
        from gossip_harness import peer_financial_terminal_v1 as terminal
        seals = self.complete_six()
        expected_identity = ledger_identity(self.path)
        replacement = self.root / 'replacement.sqlite'
        with sqlite3.connect(self.path) as source, sqlite3.connect(replacement) as target:
            source.backup(target)
        original = terminal.seal_row
        replaced = False
        def replace_after_first_read(db, cohort):
            nonlocal replaced
            row = original(db, cohort)
            if not replaced:
                os.replace(replacement, self.path)
                replaced = True
            return row
        with patch.object(terminal, 'seal_row', side_effect=replace_after_first_read):
            with self.assertRaisesRegex(FinancialError, 'Study ledger identity changed'):
                verified_study_barrier(self.roster, self.chain, self.chain.commitment, seals,
                    existing_ledger_path=self.path, expected_ledger_identity=expected_identity)
        self.assertTrue(replaced)
        self.assertNotEqual(ledger_identity(self.path), expected_identity)

    def test_all_six_barrier_reads_share_one_explicit_sql_snapshot(self):
        import sqlite3
        from unittest.mock import patch
        from gossip_harness import peer_financial_terminal_v1 as terminal
        seals = self.complete_six()
        expected_identity = ledger_identity(self.path)
        original_connect = sqlite3.connect
        original_row = terminal.seal_row
        connections, statements = [], []
        changed = False
        def observed_connect(*args, **kwargs):
            db = original_connect(*args, **kwargs)
            db.set_trace_callback(statements.append)
            connections.append(db)
            return db
        def mutate_after_first_read(db, cohort):
            nonlocal changed
            row = original_row(db, cohort)
            if not changed:
                # This write commits after the reader's first SELECT. It must
                # not create a mixture of old and new child observations.
                with original_connect(self.path) as writer:
                    writer.execute('UPDATE financial_terminal_seals_v4 SET seal_id=? WHERE cohort=?',
                                   ('0' * 64, self.roster.children[-1].cohort))
                changed = True
            return row
        with patch.object(terminal.sqlite3, 'connect', side_effect=observed_connect), \
                patch.object(terminal, 'seal_row', side_effect=mutate_after_first_read):
            barrier = verified_study_barrier(self.roster, self.chain, self.chain.commitment, seals,
                existing_ledger_path=self.path, expected_ledger_identity=expected_identity)
        self.assertEqual(barrier['role_count'], 96)
        self.assertEqual(len(connections), 1)
        self.assertEqual(sum(item == 'BEGIN' for item in statements), 1)
        with self.assertRaises(sqlite3.ProgrammingError):
            connections[0].execute('SELECT 1')  # Read connection is closed.
        with self.assertRaises(FinancialError):
            verified_study_barrier(self.roster, self.chain, self.chain.commitment, seals,
                existing_ledger_path=self.path, expected_ledger_identity=expected_identity)
