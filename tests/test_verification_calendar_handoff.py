"""Qualify data-only handoff stories using trusted known implementations only."""
from copy import deepcopy
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest

from analysis.verification_calendar_handoff import CASES, CLI, PROJECT_ID
from gossip_harness.verification_calendar import PROJECT, reference, validate_input


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(",",":"),ensure_ascii=True,allow_nan=False)


def run_trusted(case, versions=None, reset_between_versions=False):
    """Never accept model source here: callers below use fixture known_files only."""
    versions = versions or [stage['known_files'] for stage in PROJECT['stages']]
    with tempfile.TemporaryDirectory(prefix='trusted-calendar-handoff-') as directory:
        root=Path(directory)
        paths=[]
        for index,files in enumerate(versions):
            version=root/f'stage-{index}'
            paths.append(version)
            for name,content in files.items():
                path=version/name
                path.parent.mkdir(parents=True,exist_ok=True)
                path.write_text(content)
        database=root/'actual-calendar.sqlite'
        answers=[]
        prior=None
        for step in case['steps']:
            index=step['stage_index']
            if reset_between_versions and prior is not None and index!=prior and database.exists():
                database.unlink()
            result=subprocess.run([sys.executable,str(paths[index]/CLI['relative_path']),str(database),json.dumps(step['command'])],capture_output=True,text=True,timeout=5,check=True)
            answers.append(json.loads(result.stdout))
            prior=index
        # This is the only database used. Its presence proves the fixed CLI,
        # rather than the per-scenario solve adapter, created real persisted data.
        assert database.is_file()
        with sqlite3.connect(database) as db:
            count=db.execute('SELECT COUNT(*) FROM bookings').fetchone()[0]
        db.close()
        return answers,count


class CalendarHandoffTests(unittest.TestCase):
    def test_bounded_data_schema_and_every_source_stage(self):
        self.assertEqual(PROJECT_ID,'calendar')
        self.assertEqual(len(CASES),2)
        self.assertEqual([len(c['steps']) for c in CASES],[15,16])
        for case in CASES:
            self.assertTrue(case['contract_basis'])
            self.assertTrue(case['requirements'])
            self.assertEqual(len(case['steps']),len(case['expected_outputs']))
            indices=[step['stage_index'] for step in case['steps']]
            self.assertEqual(indices,sorted(indices))
            self.assertEqual(set(indices),{0,1,2,3})
            self.assertEqual(case['steps'][0]['command']['op'],'book')
            for step in case['steps']:
                self.assertEqual(set(step),{'stage_index','command'})
                self.assertIsNone(validate_input(step['stage_index'],{'commands':[step['command']]}))
                self.assertNotEqual(step['command']['op'],'audit')
            canonical(case)

    def test_manual_expectations_match_independent_reference_prefixes(self):
        first,second=CASES
        # Before the policy change, the independent in-memory reference can
        # replay the entire history under milestone three with identical rules.
        for case,length in ((first,7),(second,11)):
            commands=[step['command'] for step in case['steps'][:length]]
            self.assertEqual(canonical(reference(2,{'commands':commands})),canonical(case['expected_outputs'][:length]))
        # The confirmed-booking policy suffix is independently reproducible via
        # valid grandfathered rows. This is oracle cross-checking only; the real
        # diagnostic never uses a legacy seed or resets its live database.
        legacy=[{k:row[k] for k in ('rid','resource','user','start','end')} for row in first['expected_outputs'][6]['bookings']]
        indices=[7,9,10,11,12,13,14]  # original-key replay has no legacy equivalent
        commands=[first['steps'][i]['command'] for i in indices]
        self.assertEqual(canonical(reference(3,{'legacy':legacy,'commands':commands})),canonical([first['expected_outputs'][i] for i in indices]))

    def test_trusted_source_versions_share_real_database(self):
        for case in CASES:
            with self.subTest(case=case['id']):
                actual,count=run_trusted(case)
                self.assertEqual(canonical(actual),canonical(case['expected_outputs']))
                self.assertEqual(count,len(case['expected_outputs'][-1]['bookings']))

    def test_resetting_database_between_versions_is_detected(self):
        for case in CASES:
            with self.subTest(case=case['id']):
                actual,_=run_trusted(case,reset_between_versions=True)
                self.assertNotEqual(canonical(actual),canonical(case['expected_outputs']))
                self.assertNotEqual(actual[7],case['expected_outputs'][7])

    def test_losing_prior_request_receipts_is_detected(self):
        versions=[deepcopy(stage['known_files']) for stage in PROJECT['stages']]
        # A realistic upgrade mistake preserves bookings but recreates request
        # storage. Exact-key replay after the upgrade must expose the loss.
        storage='calendar_app/storage.py'
        old=versions[1][storage]
        marker='db.execute("CREATE TABLE IF NOT EXISTS requests('
        self.assertIn(marker,old)
        versions[1][storage]=old.replace(marker,'db.execute("DROP TABLE IF EXISTS requests")\n    '+marker)
        actual,_=run_trusted(CASES[0],versions)
        self.assertEqual(actual[7],CASES[0]['expected_outputs'][7])  # bookings survived
        self.assertEqual(actual[8],{'error':'duplicate'})
        self.assertEqual(CASES[0]['expected_outputs'][8],{'ok':True})


if __name__=='__main__':
    unittest.main()
