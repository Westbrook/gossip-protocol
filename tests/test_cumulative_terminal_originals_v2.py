"""Actual six SQL seals with repaired failure; process/Git are explicit stubs."""
from dataclasses import asdict,replace
from pathlib import Path
import sqlite3
import unittest
from unittest.mock import patch
from gossip_harness.cumulative_study_controller_v2 import (StudyPlan,Release,Records,SOURCE_CLOSURE,SHARED_POLICY,
    fault_schedule,plain)
from gossip_harness.cumulative_process_evidence_v1 import PROTOCOL
from gossip_harness.candidate_observation_admission_v1 import PROTOCOL as SOURCE_PROTOCOL
from gossip_harness.cumulative_terminal_originals_v2 import audit_terminal_cohort
from gossip_harness.peer_financial_authority_v2 import canonical_payload,ledger_identity,FinancialError
from gossip_harness.peer_financial_terminal_v1 import sha,digest
from gossip_harness.peer_financial_terminal_v2 import CLOSURE_POLICY,CLOSURE_POLICY_SHA256
from tests.financial_v5_fixture import Fixture
from tests.financial_rehearsal_fixture_v1 import synthetic_plan as old_plan


def synthetic_plan():
    old=old_plan()
    runtime={**old.runtime,'protocol':'cumulative-study-runtime-v2',
        'financial_protocol':'peer-financial-authority-v5','financial_rpc_protocol':'peer-financial-rpc-v5'}
    limits={'horizon_seconds':old.horizon_seconds,'source_generations':old.source_generations,
        'partition_seconds':old.partition_seconds,'executor_slots':old.executor_slots,'runtime':runtime}
    trajectories=tuple(replace(t,fault_schedule_sha256=digest(fault_schedule(t.block,old.partition_seconds)))
        for t in old.cohort.trajectories)
    cohort=replace(old.cohort,trajectories=trajectories,shared_policy_sha256=digest(SHARED_POLICY),resource_contract_sha256=digest(limits))
    root=Path(__file__).resolve().parents[1]
    return StudyPlan(cohort,tuple(Release(**asdict(x)) for x in old.releases),old.initial_files,old.package_paths,
        {name:sha((root/name).read_bytes()) for name in SOURCE_CLOSURE},financial_closure_policy=CLOSURE_POLICY,**limits)


class CumulativeTerminalRepairedV2Tests(Fixture,unittest.TestCase):
    def setUp(self):
        super().setUp()
        from dataclasses import replace
        from gossip_harness.peer_financial_authority_v3 import profile_manifest
        base=synthetic_plan()
        self.plan=replace(base,cohort=replace(base.cohort,model_profiles_sha256=digest({'mini':profile_manifest(self.worker)})))
        self.roster=self.plan.roster
        self.records=Records(self.chain)
        self.records.put('contract',self.plan.record())
        self.records.put('ledger',ledger_identity(self.path))
        self.process_stubs={}

    def permit(self,opening):
        value=super().permit(opening)
        value['sources']=self.plan.source_pins
        value['max_workers']=value['execution_design']['max_workers']=self.plan.executor_slots
        value['execution_design']['runtime']=self.plan.runtime
        value['execution_design_sha256']=digest(value['execution_design'])
        return value

    def evidence(self,status='completed'):
        from gossip_harness.peer_financial_terminal_v1 import ChildTerminalSealRequest, EvidenceReference
        stops=[]
        for actor in self.child.actors:
            value={'protocol':'financial-role-stop-v1','roster_sha256':self.roster.sha256,'cohort':self.child.cohort,
                'trajectory':self.child.trajectory,'actor':actor,'stopped':True}
            raw=canonical_payload(value)
            name='unit-stop-'+sha(actor.encode())+'.json'
            self.chain.retain(name,raw)
            stops.append(EvidenceReference(name,sha(raw)))
        value={'protocol':'financial-child-terminal-v1','roster_sha256':self.roster.sha256,'cohort':self.child.cohort,
            'trajectory':self.child.trajectory,'status':status,'final_source_sha256':'f'*64}
        raw=canonical_payload(value)
        name='unit-terminal-'+sha(self.child.cohort.encode())+'.json'
        self.chain.retain(name,raw)
        return ChildTerminalSealRequest(tuple(stops),EvidenceReference(name,sha(raw)))

    def complete_six(self,barrier_mode="valid"):
        from dataclasses import replace
        from gossip_harness.peer_project_contract_v2 import to_dict
        from gossip_harness.peer_financial_terminal_v2 import verified_study_barrier
        receipts=[]
        for index,child in enumerate(self.roster.children):
            self.select(index)
            self.context=replace(self.context,execution_contract_sha256=self.plan.sha256)
            self.contract['execution_contract_sha256']=self.plan.sha256
            for spec in self.contract['task_specs']:
                spec['context']=to_dict(self.context)
            ckey,rkey='child.'+child.trajectory,'runtime.'+child.trajectory
            self.records.put(ckey+'.begin',{'trajectory':plain(asdict(self.plan.cohort.trajectories[index])),
                'started_at':0,'deadline':30,'contract_sha256':self.plan.sha256})
            self.open(max_workers=self.plan.executor_slots)
            self.records.put(rkey+'.permit',self.last_permit)
            self.records.put(rkey+'.financial-config',self.authority.config)
            status='completed'
            if index == 0:
                self.transport.proposal={'changes':[],'summary':'known failed proposal'}
                first,_=self.start(0)
                self.failed=self.terminal(first)
                self.assertEqual(self.failed.state,'failed')
                self.transport.proposal={'changes':[{'path':'src/a.py','content':'fixed\n'}],'summary':'repair'}
                second,_=self.start(1)
                self.repaired=self.terminal(second)
                self.assertEqual(self.repaired.state,'completed')
            request=self.evidence(status)
            name='unit-process-confirmation-'+str(index)+'.json'
            self.chain.retain(name,b'{"unit_process_stub":true}')
            confirmation=self.chain.position(name)
            seal=self.authority.seal_terminal(self.authority.prepare_terminal_snapshot(request))
            config=dict(self.authority.config)
            self.authority.close()
            self.authority=None
            runtime_root=self.root/('child-'+str(index))
            for directory in ('provider-journals','financial-payloads','seed-mesh','finance-mesh','roles','processes','private-git','protected.git'):
                (runtime_root/directory).mkdir(parents=True)
            file=runtime_root/'protected.git'/'unit-source'
            file.write_bytes(b'unit source only')
            inventory=[{'path':str(file),'sha256':sha(file.read_bytes()),'size':file.stat().st_size}]
            source={'protocol':PROTOCOL,'kind':'final-source','registration':{'unit_stub':True},
                'repository':str(runtime_root/'protected.git'),'commit_oid':'c'*40,'tree_oid':'f'*40,
                'source_identity_protocol':SOURCE_PROTOCOL,'source_sha256':'f'*64,'files':[]}
            counts=dict(planned=len(child.actors),launched=len(child.actors),exited=len(child.actors),
                        never_launched=0,known_launch_failed=0,missing_final=0)
            self.process_stubs[child.cohort]=(source,counts,{'request':request.record()},confirmation)
            self.records.put(rkey+'.originals',{'writer_lifetimes_closed':True,'ledger_identity':ledger_identity(self.path),
                'cohort':child.cohort,'config':config,'config_sha256':digest(config),'complete_child_files':inventory,
                'original_paths':{'runtime_root':str(runtime_root),'protected_git':str(runtime_root/'protected.git')}})
            self.records.put(ckey+'.terminal',{'trajectory_id':child.trajectory,'status':status,'acceptance_authority':False,
                'final_source':{'unit_stub':True},'financial_seal':{'raw_utf8':seal.raw.decode(),
                    'publication_name':seal.publication_name,'commitment':asdict(seal.commitment)}})
            receipts.append(seal)
        barrier=verified_study_barrier(self.roster,self.chain,self.chain.commitment,tuple(receipts),
            existing_ledger_path=self.path,expected_ledger_identity=ledger_identity(self.path))
        if barrier_mode=='invalid':
            barrier={**barrier,'role_count':95}
        if barrier_mode!='missing':
            self.records.put('complete-cohort-barrier',barrier)

    def observe(self):
        with patch('gossip_harness.cumulative_terminal_originals_v2._process',
                   side_effect=lambda *args,**kwargs:self.process_stubs[kwargs['cohort']]):
            return audit_terminal_cohort(self.chain,self.chain.commitment,plan=self.plan,
                ledger_identity=ledger_identity(self.path),repository=Path(__file__).resolve().parents[1])

    def test_actual_repaired_six_child_financial_history_survives_reader_with_cost_and_failure_count(self):
        self.complete_six()
        facts=self.observe()
        self.assertTrue(facts.freeze_eligible,(facts.evidence_errors,[x.evidence_errors for x in facts.slots]))
        self.assertEqual([x.outcome for x in facts.slots],['completed']*6)
        self.assertEqual(facts.slots[0].financial_origin['action_counts'],{'completed':1,'failed':1})
        self.assertEqual(facts.slots[0].financial_origin['financial_closure_policy_sha256'],CLOSURE_POLICY_SHA256)
        self.assertEqual(self.ledger.budget()['spent_or_reserved'],1000+self.failed.usage_units+self.repaired.usage_units)
        self.assertEqual(len(self.transport.calls),2)
        self.assertFalse(facts.acceptance_authority)

    def test_old_terminal_reader_cannot_adopt_the_new_typed_plan(self):
        from gossip_harness.cumulative_terminal_originals_v1 import audit_terminal_cohort as old_reader
        with self.assertRaisesRegex(FinancialError,'Exact owned chain/plan'):
            old_reader(self.chain,self.chain.commitment,plan=self.plan,
                ledger_identity=ledger_identity(self.path),repository=Path(__file__).resolve().parents[1])

    def test_repaired_success_still_rejects_late_unknown_wallet_usage(self):
        self.complete_six()
        with sqlite3.connect(self.path) as db:
            db.execute("INSERT INTO reservations VALUES ('unrelated','historical',1,100,NULL,'reserved')")
        facts=self.observe()
        self.assertFalse(facts.freeze_eligible)
        self.assertEqual([x.outcome for x in facts.slots],['completed']*6)
        self.assertTrue(any('unsettled reservation' in x for x in facts.evidence_errors))

    def test_repaired_success_without_global_barrier_preserves_denominator_without_freeze(self):
        self.complete_six(barrier_mode='missing')
        facts=self.observe()
        self.assertFalse(facts.freeze_eligible)
        self.assertEqual(len(facts.slots),6)
        self.assertEqual(facts.slots[0].financial_origin['action_counts']['failed'],1)

    def test_old_format_global_barrier_cannot_imply_v5_policy(self):
        self.complete_six(barrier_mode='missing')
        self.records.put('complete-cohort-barrier',{'protocol':'financial-terminal-v1','self_asserted':True})
        facts=self.observe()
        self.assertFalse(facts.freeze_eligible)
        self.assertIsNone(facts.barrier)
