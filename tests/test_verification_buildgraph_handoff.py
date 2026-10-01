"""Qualify data-only upgrade stories using trusted known source, never model code."""
from copy import deepcopy
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from gossip_harness.blackbox_validator import json_equal
from gossip_harness.verification_buildgraph import PROJECT, POLICY_V1, POLICY_V2, reference, validate_input

DATA_PATH = Path(__file__).resolve().parents[1] / 'analysis/verification_buildgraph_handoff.py'
_spec = importlib.util.spec_from_file_location('buildgraph_handoff_data', DATA_PATH)
DATA = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(DATA)


def source_hashes(files):
    return {name:hashlib.sha256(content.encode()).hexdigest() for name,content in files.items()}


def canonical_hash(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,ensure_ascii=True,separators=(',',':')).encode()).hexdigest()


def trusted_sources():
    return [{'stage_index':index, 'fixture_stage_id':stage['id'],
             'source_kind':'trusted fixture known_files; not model candidate code',
             'files_sha256':canonical_hash(stage['known_files']),
             'file_hashes':source_hashes(stage['known_files']),
             'policy_text':stage['known_files']['policy.json']}
            for index,stage in enumerate(PROJECT['stages'])]


def run_trusted_known_case(case, *, reset_on_stage_change=False, old_policy_at_stage_four=False):
    """Host execution is restricted to this fixture's trusted known_files only."""
    manifests=trusted_sources()
    with tempfile.TemporaryDirectory(prefix='trusted-buildgraph-handoff-') as directory:
        root=Path(directory)
        versions=[]
        for index,stage in enumerate(PROJECT['stages']):
            version=root / 'versions' / f'stage-{index}'
            files=deepcopy(stage['known_files'])
            if index==3 and old_policy_at_stage_four:
                files['policy.json']=POLICY_V1
            manifests[index].update(files_sha256=canonical_hash(files), file_hashes=source_hashes(files),
                                    policy_text=files['policy.json'])
            for name,content in files.items():
                path=version / name
                path.parent.mkdir(parents=True,exist_ok=True)
                path.write_text(content)
            versions.append(version)
        database=root / 'persisted.sqlite'
        previous_stage=None
        identity=None
        outputs=[]
        trace=[]
        for index,step in enumerate(case['steps']):
            stage=step['stage_index']
            if reset_on_stage_change and previous_stage is not None and stage!=previous_stage:
                database.unlink()
                identity=None
            cli=versions[stage] / DATA.CLI_RELATIVE_PATH
            result=subprocess.run([sys.executable,'-I',str(cli),str(database)],
                                  input=json.dumps(step['command']),capture_output=True,text=True,timeout=10)
            if result.returncode:
                raise AssertionError(f'Trusted CLI failed at step {index}: {result.stderr}')
            actual=json.loads(result.stdout)
            current_identity=(database.stat().st_dev,database.stat().st_ino)
            if identity is not None and current_identity!=identity:
                raise AssertionError('The trusted story unexpectedly replaced its SQLite file')
            identity=current_identity
            outputs.append(actual)
            trace.append({'step_index':index,'stage_index':stage,'command':step['command'],
                          'actual_output':actual,'cli_path':str(cli),'database_path':str(database),
                          'database_device':identity[0],'database_inode':identity[1],
                          'source_files_sha256':manifests[stage]['files_sha256'],
                          'policy_sha256':hashlib.sha256((versions[stage]/'policy.json').read_bytes()).hexdigest()})
            previous_stage=stage
        return {'id':case['id'],'source_kind':'trusted known implementation only',
                'matched_expected':json_equal(outputs,case['expected_outputs']),
                'actual_outputs':outputs,'stage_sources':manifests,'trace':trace,
                'reset_database_negative_control':reset_on_stage_change,
                'old_policy_negative_control':old_policy_at_stage_four}


class BuildGraphHandoffTests(unittest.TestCase):
    def test_schema_bounds_and_stage_admission(self):
        self.assertEqual(DATA.PROJECT_ID,'buildgraph')
        self.assertEqual(len(DATA.CASES),2)
        for case in DATA.CASES:
            self.assertLessEqual(len(case['steps']),16)
            self.assertEqual(len(case['steps']),len(case['expected_outputs']))
            stages=[s['stage_index'] for s in case['steps']]
            self.assertEqual(stages,sorted(stages))
            self.assertEqual(set(stages),{0,1,2,3})
            for step in case['steps']:
                self.assertEqual(set(step),{'stage_index','command'})
                validate_input(step['stage_index'],{'commands':[step['command']]})
            distinction=case['required_contract_vs_extra_assumptions']
            self.assertTrue(distinction['established_command_contract'])
            self.assertTrue(distinction['additional_robustness_assumptions'])
            self.assertTrue(distinction['historical_version_anchor']['direct_counter_dependent_steps'])
            self.assertIn('None',case['contract_basis']['primary_score_effect'])

    def test_flat_pure_reference_agrees_under_stated_counter_assumption(self):
        for case in DATA.CASES:
            expected=reference(3,{'commands':[s['command'] for s in case['steps']]})
            self.assertTrue(json_equal(expected,case['expected_outputs']))

    def test_manual_cache_and_receipt_checkpoints(self):
        def digest(source,deps):
            raw=json.dumps({'source':source,'deps':deps},sort_keys=True,ensure_ascii=True,separators=(',',':'))
            return hashlib.sha256(raw.encode()).hexdigest()
        first,second=[case['expected_outputs'] for case in DATA.CASES]
        root1=digest('core-v1',[])
        root2=digest('core-v2',[])
        app1=digest('app-v1',[['root',root1]])
        app2=digest('app-v1',[['root',root2]])
        self.assertEqual(first[3]['digest'],root1)
        self.assertEqual(first[4]['digest'],app1)
        self.assertTrue(all(row['digest'] is None for row in first[6]))
        self.assertEqual(first[7],{'order':['root','app'],'version':5})
        self.assertEqual(first[8],{'key':'release','built':['root','app'],'version':6,'digests':{'app':app2}})
        self.assertEqual(first[8],first[10])
        self.assertEqual(first[8],first[12])
        self.assertEqual(first[11],{'id':'app','digest':app2,'cached':True})
        self.assertEqual(first[13],json.loads(POLICY_V2))
        self.assertEqual(first[14]['schema'],2)
        self.assertEqual(second[6]['built'],[])
        self.assertEqual(second[6]['digests'],{'seed':digest('seed-body',[])})
        self.assertEqual(second[6],second[10])
        self.assertEqual(second[9],{'ok':'imported','key':'migration','count':1,'version':8})
        self.assertEqual(second[9],second[11])
        self.assertEqual(second[9],second[13])
        self.assertEqual(second[14],[{'id':'migrated','source':'new-body','priority':0,'deps':[],
                                      'digest':digest('new-body',[])}])

    def test_trusted_cli_uses_one_database_across_four_source_versions(self):
        for case in DATA.CASES:
            proof=run_trusted_known_case(case)
            self.assertTrue(proof['matched_expected'],case['id'])
            self.assertEqual(len({(t['database_path'],t['database_device'],t['database_inode']) for t in proof['trace']}),1)
            self.assertEqual(len({t['cli_path'] for t in proof['trace']}),4)
            for index,source in enumerate(proof['stage_sources']):
                self.assertEqual(source['file_hashes'],source_hashes(PROJECT['stages'][index]['known_files']))
                self.assertEqual(source['policy_text'],POLICY_V2 if index==3 else POLICY_V1)

    def test_resetting_database_does_not_satisfy_handoff(self):
        proof=run_trusted_known_case(DATA.CASES[0],reset_on_stage_change=True)
        self.assertFalse(proof['matched_expected'])
        self.assertEqual(proof['actual_outputs'][3],{'error':'missing'})

    def test_stale_policy_is_detected(self):
        proof=run_trusted_known_case(DATA.CASES[0],old_policy_at_stage_four=True)
        self.assertFalse(proof['matched_expected'])
        self.assertEqual(proof['actual_outputs'][13],json.loads(POLICY_V1))
        self.assertEqual(proof['actual_outputs'][14]['schema'],1)


if __name__=='__main__':
    unittest.main()
