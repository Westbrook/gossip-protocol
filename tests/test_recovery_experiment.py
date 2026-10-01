"""Offline recovery controls and trusted-fixture tests; no provider requests."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness import recovery_experiment as recovery
from gossip_harness.ledger import Ledger
from gossip_harness.gitstore import GitStore
from gossip_harness.pilot import Trace
from gossip_harness.transport import Event
from gossip_harness.worker import WorkerFailure, WorkerRequest, WorkerResult


class TrustedFixtureValidator:
    """Host execution is exclusively for fixed trusted test fixtures, never live patches."""
    def __init__(self, target):
        self.target, self.last_receipt = target, {}
    def __call__(self, checkout):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, content in recovery.CHECKS.items(): (root/name).write_text(content)
            script = ('import sys;sys.path.insert(0,sys.argv[1]);import run_checks;'
                      'raise SystemExit(run_checks.run_checks(sys.argv[2],sys.argv[3]))')
            result = subprocess.run(['python3','-I','-c',script,str(root),self.target,str(checkout)],
                                    capture_output=True,text=True,timeout=20)
            output = result.stdout + result.stderr
            self.last_receipt = dict(output=output, exit_code=result.returncode, test_only_host_validator=True)
            return result.returncode == 0, output


class RecoveryTests(unittest.TestCase):
    def test_scripted_conflict_and_known_repair_preserve_regressions(self):
        files = recovery.fixture_files()
        changes = recovery.injected_changes(files)
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            for name,content in {**files,**changes}.items():
                path=root/name; path.parent.mkdir(parents=True,exist_ok=True); path.write_text(content)
            for target in ('parser','summary'):
                validator=TrustedFixtureValidator(target)
                passed, detail=validator(root)
                self.assertTrue(passed,detail)
                self.assertTrue(recovery._completion_valid(validator.last_receipt,target))
            validator=TrustedFixtureValidator('all')
            passed, detail=validator(root)
            self.assertFalse(passed)
            self.assertIn('TypeError',detail)
            self.assertIn('"tests_run": 28',detail)
            request=WorkerRequest('test',recovery.SPEC,recovery.PATHS,{**files,**changes},'base',1,detail)
            for name,content in recovery.KnownRepairWorker().run(request).changes.items(): (root/name).write_text(content)
            self.assertTrue(validator(root)[0],validator.last_receipt)
            self.assertTrue(recovery._completion_valid(validator.last_receipt,'all'))
            # A seemingly correct implementation that recycles its return dict
            # violates call independence and must fail the same acceptance suite.
            oracle = recovery.KnownRepairWorker().run(request).changes[recovery.PATHS[1]]
            mutant = oracle.replace('def summarize(tasks):','def _summarize(tasks):')
            mutant += '\n_shared = {}\ndef summarize(tasks):\n    _shared.clear()\n    _shared.update(_summarize(tasks))\n    return _shared\n'
            (root/recovery.PATHS[1]).write_text(mutant)
            passed, detail = validator(root)
            self.assertFalse(passed)
            self.assertIn('test_independent_calls',detail)

    def test_invalid_worker_patch_settles_known_usage_and_stops(self):
        files=recovery.fixture_files()
        fake=SimpleNamespace(head=lambda:'base',read_files=lambda:files)
        for invalid in (None, [], {recovery.PATHS[1]: 123}, {recovery.PATHS[1]:None}):
            class InvalidWorker:
                calls=0
                def reservation_units(self,request): return 100
                def run(self,request):
                    self.calls+=1
                    return WorkerResult(invalid,'Malformed worker patch',17,{})
            with tempfile.TemporaryDirectory() as directory:
                root=Path(directory); worker=InvalidWorker(); ledger=Ledger(root/'ledger.sqlite',500)
                with (patch.object(recovery,'_seed',return_value=(fake,fake,fake,'actual failure')),
                      patch.object(recovery.GitStore,'fork',return_value=fake)):
                    result=recovery._repair(root/'generation',worker,ledger,files,{},None,
                        Trace(root/'trace.jsonl'),2,'run',lambda _:None)
                self.assertEqual(result['status'],'invalid_patch')
                self.assertTrue(result['halt']); self.assertEqual(worker.calls,1)
                self.assertEqual(ledger.budget()['spent_or_reserved'],17)

    def test_completion_requires_full_checker_and_unique_receipt(self):
        receipt=dict(spec_version=recovery.SPEC_VERSION,target='all',tests_run=28,failures=0,errors=0,successful=True)
        self.assertTrue(recovery._completion_valid({'output':json.dumps(receipt)},'all'))
        self.assertFalse(recovery._completion_valid({'output':''},'all'))
        self.assertFalse(recovery._completion_valid({'output':json.dumps(receipt)+'\n'+json.dumps(receipt)},'all'))
        receipt['tests_run']=1
        self.assertFalse(recovery._completion_valid({'output':json.dumps(receipt)},'all'))

    def test_old_base_and_obsolete_version_evidence_is_rejected(self):
        note=dict(kind='contract_observation',base_sha='current',spec_version=recovery.SPEC_VERSION)
        self.assertTrue(recovery._valid_note(note,'current'))
        self.assertFalse(recovery._valid_note({**note,'base_sha':'old'},'current'))
        self.assertFalse(recovery._valid_note({**note,'spec_version':'v1'},'current'))

    def test_superseded_lease_wrongbase_and_disputed_tip_notices_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger=Ledger(Path(directory)/'ledger.sqlite',0); ledger.add_task('task')
            old=ledger.claim('task','old',now=1,ttl=1)
            current=ledger.claim('task','repair',now=3,ttl=100)
            manifest=dict(task_id='task',epoch=current.epoch,base_sha='base',tip_sha='tip')
            for payload,reason in (({**manifest,'epoch':old.epoch},'stale_task_epoch'),
                                   ({**manifest,'base_sha':'wrong'},'wrong_base'),
                                   ({**manifest,'tip_sha':'disputed'},'manifest_mismatch')):
                event=Event.create('repair',1,'repair_proposal',payload)
                self.assertEqual(recovery._valid_notice(event,manifest,current),(False,reason))
            event=Event.create('repair',2,'repair_proposal',manifest)
            self.assertTrue(recovery._valid_notice(event,manifest,current)[0])

    def test_failed_request_keeps_unknown_usage_reserved_and_no_replay(self):
        class FailingWorker:
            calls=0
            def reservation_units(self,request): return 200
            def run(self,request):
                self.calls+=1
                raise WorkerFailure('Unknown transport outcome',usage_units=None)
        with ArtifactDirectory(self.id()) as artifacts:
            root=artifacts.root; worker=FailingWorker(); ledger=Ledger(root/'shared.sqlite',500)
            report=recovery.run_recovery_experiment(root/'run',worker,budget_ledger=ledger,
                seeds=(0,),fault_profiles=('healthy',),validator_factory=TrustedFixtureValidator,progress=lambda _:None)
            self.assertEqual(report['status'],'worker_failed')
            self.assertEqual(report['cases'],[])
            self.assertEqual(len(report['unexecuted']),2)
            self.assertEqual(worker.calls,1)
            self.assertEqual(ledger.budget()['spent_or_reserved'],200)
            self.assertTrue(report['repair']['halt'])

    def test_live_requires_matching_successful_rehearsal_before_work(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            with self.assertRaisesRegex(ValueError,'exact passing rehearsal'):
                recovery.run_recovery_experiment(root/'run',object(),mode='live',
                    budget_ledger=root/'ledger.sqlite',budget_units=1000,
                    validator_factory=lambda _:self.fail('Validation must not start'))
            self.assertFalse((root/'run').exists())
            (root/'bad.json').write_text(json.dumps({'signature':{},'status':'accepted','mode':'rehearsal','cases':[]}))
            with self.assertRaisesRegex(ValueError,'does not match'):
                recovery.run_recovery_experiment(root/'run',object(),mode='live',
                    budget_ledger=root/'ledger.sqlite',budget_units=1000,rehearsal_results=root/'bad.json',
                    validator_factory=lambda _:self.fail('Validation must not start'))

    def test_successful_repair_frozen_before_replay_and_shared_cost_settles(self):
        class CostedKnown(recovery.KnownRepairWorker):
            def reservation_units(self,request): return 200
            def run(self,request):
                result=super().run(request)
                return WorkerResult(result.changes,result.summary,17,result.metadata)
        with ArtifactDirectory(self.id()) as artifacts:
            root=artifacts.root; ledger=Ledger(root/'shared.sqlite',500)
            with patch.object(recovery,'_replay',side_effect=lambda *args: {
                    'status':'accepted','case':f'{args[4]}-{args[5]}-{args[6]}','project_accepted':True}) as replay:
                report=recovery.run_recovery_experiment(root/'run',CostedKnown(),budget_ledger=ledger,
                    seeds=(0,),fault_profiles=('healthy',),validator_factory=TrustedFixtureValidator,progress=lambda _:None)
            self.assertEqual(report['status'],'accepted'); self.assertEqual(replay.call_count,2)
            self.assertEqual(report['repair']['task_status'],'complete')
            self.assertEqual(report['repair']['release_head'],report['repair']['exact_tested_sha'])
            self.assertEqual(ledger.budget()['spent_or_reserved'],17)
            self.assertEqual(ledger.pending_intents(),[])
            trace=[json.loads(line) for line in (root/'run/trace.jsonl').read_text().splitlines()]
            notes=next(row for row in trace if row['kind']=='evidence_filtered')
            self.assertEqual(len(notes['included']),1); self.assertEqual(len(notes['rejected']),1)
            self.assertTrue((root/'run/frozen-repair.json').is_file())
            self.assertEqual(report['unexecuted'],[])

    def test_live_cannot_be_labeled_with_offline_oracle(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError,'offline oracle'):
                recovery.run_recovery_experiment(Path(directory)/'run',recovery.KnownRepairWorker(),mode='live')

    def test_rehearsal_requires_unique_complete_roster_and_exact_publication(self):
        signature={'fault_profiles':['healthy','partition_heal'],'seeds':[0]}
        cases=[dict(case=f'{mode}-{fault}-0',transport=mode,fault=fault,seed=0,status='accepted',
                    project_accepted=True,task_status='complete',release_head='tested',exact_tested_sha='tested')
               for mode in ('bus','gossip') for fault in signature['fault_profiles']]
        report=dict(experiment='integration-recovery-study',signature=signature,mode='rehearsal',status='accepted',
                    cases=cases,repair=dict(status='accepted',task_status='complete',release_head='repair',exact_tested_sha='repair'))
        self.assertTrue(recovery._valid_rehearsal(report,signature))
        self.assertFalse(recovery._valid_rehearsal({**report,'cases':[cases[0]]*4},signature))
        self.assertFalse(recovery._valid_rehearsal({**report,'cases':cases[:-1]},signature))
        for change in ({'project_accepted':False},{'task_status':'claimed'},{'exact_tested_sha':'other'}):
            self.assertFalse(recovery._valid_rehearsal({**report,'cases':[{**cases[0],**change}]+cases[1:]},signature))
        self.assertFalse(recovery._valid_rehearsal({**report,'repair':{**report['repair'],'task_status':'claimed'}},signature))

    def test_transport_replay_partition_and_broker_outage(self):
        with ArtifactDirectory(self.id()) as artifacts:
            root=artifacts.root; files=recovery.fixture_files(); injected=recovery.injected_changes(files)
            request=WorkerRequest('test',recovery.SPEC,recovery.PATHS,{**files,**injected},'base',1)
            repair=recovery.KnownRepairWorker().run(request).changes
            trace=Trace(root/'trace.jsonl')
            base,integration,_,_=recovery._seed(root/'seed',files,injected,TrustedFixtureValidator,trace,'repair')
            seed_context=dict(base_path=base.path,integration_path=integration.path,base_sha=base.head(),staging_sha=integration.head())
            for mode,fault in (('bus','broker_outage'),('gossip','broker_outage'),('gossip','partition_heal')):
                result=recovery._replay(root/f'{mode}-{fault}',files,injected,repair,mode,fault,0,TrustedFixtureValidator,trace,seed_context)
                self.assertEqual(result['status'],'accepted')
                self.assertEqual(result['task_status'],'complete')
                self.assertEqual(result['release_head'],result['exact_tested_sha'])
                self.assertEqual(result['replay_api_calls'],0)
                self.assertEqual(result['release_promotions'],1)
                self.assertEqual(result['stale_notices_rejected'],2)
                self.assertTrue(result['global_rejection_evidence']['reused'])
                self.assertEqual(result['final_prepare_calls'],1)
                self.assertTrue(result['duplicate_event_id_reprocessed_safely'])
                if mode=='gossip' and fault=='broker_outage':
                    self.assertTrue(result['preheal_published'])
                else:
                    self.assertFalse(result['preheal_published'])
                    self.assertGreater(result['published_round'],recovery.FAULT_ROUNDS)

    def test_shared_seed_is_bound_to_frozen_refs_and_actual_fixture(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            base=GitStore.create(root/'base.git',{'file':'original'})
            context=dict(base_path=base.path,integration_path=base.path,base_sha=base.head(),staging_sha=base.head())
            for files,change in (({'file':'wrong'},{}),({'file':'original'},{'file':'changed'})):
                with self.assertRaisesRegex(ValueError,'does not match'):
                    recovery._replay(root/'case',files,change,{},'bus','healthy',0,
                        lambda _:self.fail('No validator before seed binding'),Trace(root/'trace.jsonl'),context)
            with self.assertRaisesRegex(ValueError,'does not match'):
                recovery._replay(root/'case',{'file':'original'},{},{},'bus','healthy',0,None,
                    Trace(root/'trace.jsonl'),{**context,'staging_sha':'wrong'})
            self.assertFalse((root/'case').exists())

    def test_existing_output_never_resumes_requests(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(FileExistsError):
                recovery.run_recovery_experiment(directory,recovery.KnownRepairWorker())

    @unittest.skipUnless(os.environ.get('GOSSIP_RUN_DOCKER_TESTS')=='1','Explicit real Docker verification only')
    def test_real_docker_repair_and_partition_replay(self):
        with ArtifactDirectory(type(self).__name__, retain_success=True) as artifacts:
            report=recovery.run_recovery_experiment(artifacts.output,recovery.KnownRepairWorker(),
                seeds=(0,),fault_profiles=('partition_heal',),progress=lambda _:None)
            self.assertEqual(report['status'],'accepted')
            self.assertTrue(all(not c['preheal_published'] for c in report['cases']))


if __name__=='__main__': unittest.main()
