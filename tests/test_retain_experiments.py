"""Transactional retention checks without Docker, providers, or candidate code."""
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import Mock, patch

import retain_experiments as retention


class RetainExperimentsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.discovery, self.recovery = self.root / 'discovery', self.root / 'recovery'
        self.output, self.ledger = self.root / 'retained', self.root / 'ledger.sqlite'
        self.source = self.root / 'source'
        (self.source / 'gossip_harness').mkdir(parents=True)
        self.source_bytes = b'# exact bytes, including CRLF\r\nVALUE = "caf\xc3\xa9"\r\n'
        (self.source / 'gossip_harness/example.py').write_bytes(self.source_bytes)
        self.case = dict(trial=0, variant='shared', project_accepted=True,
                         controlled_worker_audit=[dict(kind='result', original_usage_units=10, usage_units=5)],
                         required_tasks=['parse'], discovery_usage_units_attributed=2,
                         counters=dict(worker_calls=2, validation_calls=3),
                         evidence_transport='shared', release_head='accepted-sha', model_execution='fixture')
        self.discovery_result = dict(cases=[self.case], budget=dict(spent_or_reserved=7),
                                     budget_before=dict(spent_or_reserved=0), contract=dict(mode='rehearsal', budget_ledger=str(self.ledger)))
        self.recovery_case = dict(case='repair', project_accepted=True, release_head='accepted-sha', exact_tested_sha='accepted-sha')
        self.recovery_result = dict(cases=[self.recovery_case], mode='rehearsal', budget=dict(spent_or_reserved=10),
                                    repair=dict(status='accepted', release_head='accepted-sha', exact_tested_sha='accepted-sha'))
        self.write_json(self.discovery / 'results.json', self.discovery_result)
        self.write_json(self.discovery / 'manifest.json', dict(mode='rehearsal'))
        self.write_json(self.discovery / 'trace.jsonl', dict(kind='worker_finished', usage_units=7))
        with (self.discovery / 'trace.jsonl').open('a') as stream:
            stream.write(json.dumps(dict(kind='project_accepted', trial=0, variant='shared', exact_tested_sha='accepted-sha')) + '\n')
        self.write_json(self.discovery / 'trial-0/notes.json', dict(notes=[]))
        self.write_json(self.discovery / 'trial-0/shared/responses.json', dict(responses=[]))
        self.write_json(self.recovery / 'results.json', self.recovery_result)
        self.write_json(self.recovery / 'manifest.json', dict(budget_before=dict(spent_or_reserved=7)))
        self.write_json(self.recovery / 'trace.jsonl', dict(kind='worker_result', usage_units=3))
        self.write_json(self.recovery / 'frozen-repair.json', dict(changes={'solution.py': 'pass\n'}))
        with closing(sqlite3.connect(self.ledger)) as db, db:
            db.executescript("""
                CREATE TABLE reservations (amount INTEGER, spent INTEGER, state TEXT);
                CREATE TABLE tasks (status TEXT);
                CREATE TABLE intents (state TEXT);
                CREATE TABLE settings (key TEXT, value INTEGER);
                INSERT INTO reservations VALUES (10, 10, 'settled');
                INSERT INTO tasks VALUES ('complete');
                INSERT INTO settings VALUES ('budget', 100);
            """)
        self.store = Mock()
        self.store.head.return_value = 'accepted-sha'
        self.store.path = self.root / 'mock-release.git'
        self.read_tree = self.enterContext(patch.object(retention, 'read_text_tree',
                                                      return_value={'solution.py': 'VALUE = "caf\u00e9"\n'}))
        self.git = self.enterContext(patch.object(retention, 'GitStore', return_value=self.store))

    @staticmethod
    def write_json(path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value) + '\n')

    def retain(self):
        return retention.retain(self.discovery, self.recovery, self.output, self.ledger, source_root=self.source)

    def assert_no_output(self):
        self.assertFalse(self.output.exists())
        self.assertEqual(list(self.root.glob('.retained.staging-*')), [])

    def test_success_preserves_summary_and_exact_export_provenance(self):
        summary = self.retain()
        expected_case = dict(trial=0, variant='shared', accepted=True, coding_usage_units_attributed=10,
                             incremental_coding_usage_units=5, discovery_usage_units_attributed=2,
                             total_usage_units_attributed=12, logical_calls=2, repair_attempts=1, validation_calls=3,
                             evidence_transport='shared', release_head='accepted-sha', model_execution='fixture')
        expected = dict(discovery_cases=[expected_case],
                        discovery_by_arm={'shared': dict(trials=1, accepted=1, logical_calls=2, repair_attempts=1,
                                                        validation_calls=3, coding_usage_units_attributed=10,
                                                        discovery_usage_units_attributed=2, total_usage_units_attributed=12)},
                        discovery_actual_micro_usd=7, recovery_actual_micro_usd=3, continuation_actual_micro_usd=10,
                        modes=dict(discovery='rehearsal', recovery='rehearsal'),
                        cumulative_budget=dict(limit=100, spent_or_reserved=10, remaining=90),
                        ledger_verification=dict(reservations=1, unsettled_reservations=0, completed_tasks=1, pending_intents=0),
                        discovery_model_request_attempts=0, recovery_model_request_attempts=0,
                        recovery_cases=[self.recovery_case], source_sha256={'gossip_harness/example.py': hashlib.sha256(self.source_bytes).hexdigest()},
                        cost_note='Conservative token-based estimates, not invoices. Attributed arm costs include reused scouts and replayed coding usage; actual ledger charges them only once.')
        self.assertEqual(summary, expected)
        self.assertEqual(json.loads((self.output / 'summary.json').read_bytes()), expected)
        for prefix in ('discovery/trial-0/shared/accepted', 'recovery/accepted'):
            self.assertEqual((self.output / prefix / 'SOURCE_COMMIT').read_text(), 'accepted-sha\n')
            self.assertEqual((self.output / prefix / 'solution.py').read_bytes(), 'VALUE = "caf\u00e9"\n'.encode())
        self.assertEqual((self.output / 'source-snapshot/gossip_harness/example.py').read_bytes(), self.source_bytes)
        for call in self.read_tree.call_args_list:
            self.assertEqual(call.args, (self.store.path, 'accepted-sha'))
        provenance = json.loads((self.output / 'retention-manifest.json').read_bytes())
        for name, digest in provenance['input_sha256'].items():
            self.assertEqual(hashlib.sha256((self.output / name).read_bytes()).hexdigest(), digest)
        self.assertEqual(provenance['publication'], 'atomic-exclusive')

    def test_missing_or_invalid_ledger_rejects_before_git_and_output(self):
        self.ledger.unlink()
        with self.assertRaisesRegex(ValueError, 'Accounting ledger'):
            self.retain()
        self.assertFalse(self.ledger.exists())
        self.assert_no_output()
        self.git.assert_not_called()
        self.ledger.write_bytes(b'not a database')
        with self.assertRaises(sqlite3.DatabaseError):
            self.retain()
        self.assert_no_output()
        self.git.assert_not_called()

    def test_invalid_budget_rejects_before_git_and_output(self):
        with closing(sqlite3.connect(self.ledger)) as db, db:
            db.execute('DELETE FROM settings')
        with self.assertRaisesRegex(ValueError, 'valid budget'):
            self.retain()
        self.assert_no_output()
        self.git.assert_not_called()

    def test_accounting_mismatches_reject_before_git_and_output(self):
        for which in ('discovery', 'recovery'):
            with self.subTest(which=which):
                result = self.discovery_result if which == 'discovery' else self.recovery_result
                root = self.discovery if which == 'discovery' else self.recovery
                result['budget']['spent_or_reserved'] += 1
                self.write_json(root / 'results.json', result)
                with self.assertRaisesRegex(ValueError, 'accounting does not reconcile'):
                    self.retain()
                self.assert_no_output()
                self.git.assert_not_called()
                result['budget']['spent_or_reserved'] -= 1
                self.write_json(root / 'results.json', result)

    def test_unrelated_or_stale_ledger_rejects_before_git_and_output(self):
        self.discovery_result['contract']['budget_ledger'] = str(self.root / 'different.sqlite')
        self.write_json(self.discovery / 'results.json', self.discovery_result)
        with self.assertRaisesRegex(ValueError, 'differs from the recorded discovery ledger'):
            self.retain()
        self.assert_no_output()
        self.git.assert_not_called()
        self.discovery_result['contract']['budget_ledger'] = str(self.ledger)
        self.write_json(self.discovery / 'results.json', self.discovery_result)
        with closing(sqlite3.connect(self.ledger)) as db, db:
            db.execute('UPDATE reservations SET spent=9')
        with self.assertRaisesRegex(ValueError, 'older than the recorded runs'):
            self.retain()
        self.assert_no_output()
        self.git.assert_not_called()

    def test_reserved_and_case_colliding_git_paths_reject_without_output(self):
        for files in ({'source_commit': 'collision'}, {'A.py': 'one', 'a.py': 'two'},
                      {'data': 'file', 'DATA/part.py': 'child'}, {'x.py.': 'alias'},
                      {'x.py:stream.py': 'stream'}, {'CON.txt': 'device'}):
            with self.subTest(files=files):
                self.read_tree.return_value = files
                with self.assertRaisesRegex(ValueError, 'Invalid accepted source path|colliding paths|conflicting file|path component'):
                    self.retain()
                self.assert_no_output()

    def test_trace_binding_rejects_before_git_and_output(self):
        self.case['release_head'] = 'wrong-sha'
        self.write_json(self.discovery / 'results.json', self.discovery_result)
        with self.assertRaisesRegex(ValueError, 'Discovery release differs'):
            self.retain()
        self.assert_no_output()
        self.git.assert_not_called()

    def test_recovery_binding_rejects_before_git_and_output(self):
        self.recovery_case['exact_tested_sha'] = 'wrong-sha'
        self.write_json(self.recovery / 'results.json', self.recovery_result)
        with self.assertRaisesRegex(ValueError, 'Recovery release differs'):
            self.retain()
        self.assert_no_output()
        self.git.assert_not_called()

    def test_actual_git_binding_rejects_without_output(self):
        self.store.head.return_value = 'wrong-sha'
        with self.assertRaisesRegex(ValueError, 'Accepted Git head differs'):
            self.retain()
        self.assert_no_output()
        self.read_tree.assert_not_called()

    def test_partial_write_failure_cleans_staging_and_preserves_inputs(self):
        before = (self.discovery / 'results.json').read_bytes()
        def fail(root, files):
            (root / 'partial').write_text('incomplete')
            raise OSError('disk failure')
        with patch.object(retention, '_write_export', side_effect=fail):
            with self.assertRaisesRegex(OSError, 'disk failure'):
                self.retain()
        self.assert_no_output()
        self.assertEqual((self.discovery / 'results.json').read_bytes(), before)
        self.retain()  # A failure does not poison the fresh-destination retry.

    def test_input_change_during_preparation_prevents_publication(self):
        write = retention._write_export
        def mutate(root, files):
            write(root, files)
            (self.discovery / 'trial-0/notes.json').write_text('{"changed": true}')
        with patch.object(retention, '_write_export', side_effect=mutate):
            with self.assertRaisesRegex(ValueError, 'Input evidence changed'):
                self.retain()
        self.assert_no_output()
        self.assertEqual(json.loads((self.discovery / 'trial-0/notes.json').read_text()), {'changed': True})

    def test_release_head_change_during_preparation_prevents_publication(self):
        write = retention._write_export
        def mutate(root, files):
            write(root, files)
            self.store.head.return_value = 'advanced-sha'
        with patch.object(retention, '_write_export', side_effect=mutate):
            with self.assertRaisesRegex(ValueError, 'Accepted Git head changed'):
                self.retain()
        self.assert_no_output()

    def test_existing_or_racing_destination_is_never_replaced(self):
        self.output.mkdir()
        with self.assertRaises(FileExistsError):
            self.retain()
        self.output.rmdir()
        publish = retention._publish_directory
        def race(staging, destination):
            destination.mkdir()  # Even an empty racing directory must survive.
            publish(staging, destination)
        with patch.object(retention, '_publish_directory', side_effect=race):
            with self.assertRaises(FileExistsError):
                self.retain()
        self.assertTrue(self.output.is_dir())
        self.assertEqual(list(self.output.iterdir()), [])
        self.assertEqual(list(self.root.glob('.retained.staging-*')), [])

    def test_symlink_or_path_traversal_is_rejected_without_output(self):
        notes = self.discovery / 'trial-0/notes.json'
        notes.unlink()
        notes.symlink_to(self.recovery / 'results.json')
        with self.assertRaisesRegex(ValueError, 'symlinks'):
            self.retain()
        self.assert_no_output()
        notes.unlink()
        self.write_json(notes, {})
        self.case['variant'] = '../escape'
        self.write_json(self.discovery / 'results.json', self.discovery_result)
        with self.assertRaisesRegex(ValueError, 'path component'):
            self.retain()
        self.assert_no_output()


if __name__ == '__main__':
    unittest.main()
