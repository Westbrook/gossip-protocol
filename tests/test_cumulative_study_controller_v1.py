"""Synthetic controller mechanics only; no product or physical-study evidence."""
from dataclasses import asdict, replace
import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from gossip_harness.candidate_checkpoint_chain_v1 import CheckpointChain
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from gossip_harness.cumulative_study_controller_v1 import (
    MILESTONES, PACKAGES, SHARED_POLICY, SOURCE_CLOSURE, TRANSPORT_CONTRACT, PublicResult, Records,
    Release, StudyController, StudyError, StudyPlan, StudyUnknown, actors_for, digest, fault_schedule,
)
from gossip_harness.peer_financial_terminal_v1 import ChildTerminalSeal
from gossip_harness.peer_financial_authority_v2 import ledger_identity
from gossip_harness.peer_project_contract_v2 import EvidenceRef
from gossip_harness.project_acceptance_compiler_v1 import ARMS, BLOCKS, FAULTS, CohortDesign, Trajectory


def plan(repository):
    (repository / 'pinned.txt').write_text('controller-fixture')
    import shutil
    source=Path(__file__).resolve().parents[1]
    for name in SOURCE_CLOSURE:
        target=repository/name
        target.parent.mkdir(parents=True,exist_ok=True)
        shutil.copyfile(source/name,target)
    releases = tuple(Release(m, 'Authored ' + m, {}, {'check.py':'# fixture only'},
        ('python', '/checks/check.py'), (m + '-public',), (m + '-obligation',)) for m in MILESTONES)
    runtime = {'max_reserved_units': 1000000, 'image':'sha256:' + 'a'*64}
    limits = {'horizon_seconds': 30, 'source_generations': 2, 'partition_seconds': .1,
              'executor_slots': 4, 'runtime': runtime}
    trajectories = tuple(Trajectory(a.lower().replace('-','')+'.'+b.replace('_',''), a, b,
        actors_for(a), 'durable_central_scheduler' if a == 'O16-G' else 'peer_local',
        digest({'seed':b}), digest(fault_schedule(b,.1)), FAULTS if b == 'compound_recovery' else ())
        for a in ARMS for b in BLOCKS)
    cohort = CohortDesign('synthetic-study', trajectories, MILESTONES, digest(TRANSPORT_CONTRACT),
        digest(SHARED_POLICY), digest([asdict(x) for x in releases]), digest(limits),
        'a'*64, 'b'*64, 'c'*64, 'd'*64)
    return StudyPlan(cohort, releases, {'base.py':'pass\n'},
        {p:('library/'+p+'/impl.py',) for p in PACKAGES},
        {name:hashlib.sha256((repository/name).read_bytes()).hexdigest() for name in (*SOURCE_CLOSURE,'pinned.txt')}, **limits)


class SyntheticRuntime:
    def __init__(self, study, index, records, deadline, *, statuses=None):
        self.plan, self.index, self.records = study, index, records
        self.current = {'files':study.initial_files, 'commit_oid':'a'*40, 'source_sha256':'a'*64}
        self.statuses = statuses or {}
        self.works = []
        self.releases = []
        self.closed = False
        self.seals = []
    def source(self):
        return self.current
    def release(self, release):
        self.releases.append(release.milestone)
        return {'release_sha256':release.sha256, 'commit_oid':self.current['commit_oid']}
    def publish(self, kind, value, key):
        return EvidenceRef(digest({'key':key}), 'seed', kind, digest(value))
    def work(self, stage, directives):
        self.works.append((stage,directives))
        return tuple({'actor':a, 'directive':d.to_dict(), 'directive_id':digest(d.to_dict()), 'original_ref':{'fixture':a},
            'snapshot':{'synthetic':True}, 'result_payload':{'fixture':True}} for a,d in directives)
    def integrate(self, stage, builds, reviews):
        return {'status':'integrated', 'stage':stage}
    def evaluate(self, release):
        status, outcome = self.statuses.get(release.milestone, ('completed','passed'))
        return PublicResult(status, self.current['commit_oid'], self.current['source_sha256'],
            release.sha256, release.ordered_check_ids, tuple((x,outcome) for x in release.ordered_check_ids),
            {'synthetic':True})
    def stop_and_seal(self, status, milestone_records):
        self.seals.append((status,milestone_records))
        return ChildTerminalSeal(b'{"synthetic":true}', 'synthetic-'+str(self.index)+'.json', self.records.chain.commitment)
    def close(self):
        self.closed=True


class CumulativeStudyControllerV1Tests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.root=Path(self.tmp.name).resolve()
        self.plan=plan(self.root)
        self.head=ExternalHead.create(self.root/'anchor', journal_roots=(self.root/'raw',self.root/'delta'))
        self.chain=CheckpointChain.create(self.root/'raw', self.root/'delta', context={'fixture':'controller'},authority=self.head)
        self.runtimes=[]
        self.statuses={}
        def factory(*args):
            result=SyntheticRuntime(*args,statuses=self.statuses)
            self.runtimes.append(result)
            return result
        self.factory=factory
        (self.root/'fixture.sqlite').write_bytes(b'owned offline identity fixture')
        self.controller=StudyController(self.plan, checkpoint=self.chain, expected_checkpoint=self.chain.commitment,
            repository=self.root,existing_ledger_path=self.root/'fixture.sqlite',expected_ledger_identity=ledger_identity(self.root/'fixture.sqlite'),
            runtime_factory=factory,clock=lambda:100.0)
    def tearDown(self):
        self.chain.close()
        self.head.close()
        self.tmp.cleanup()
    def test_interrupt_during_work_closes_owned_runtime_without_inventing_seal(self):
        with patch.object(SyntheticRuntime,'work',side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):self.controller._child(0)
        self.assertTrue(self.runtimes[0].closed)
        self.assertEqual(self.runtimes[0].seals,[])
        self.assertIsNone(self.controller.records.read('child.'+self.plan.cohort.trajectories[0].id+'.terminal'))
    def test_replaced_ledger_prevents_runtime_admission(self):
        path=self.root/'fixture.sqlite'
        path.rename(self.root/'retained-original.sqlite')
        path.write_bytes(b'replacement')
        with self.assertRaisesRegex(StudyError,'ledger identity'):self.controller._child(0)
        self.assertEqual(self.runtimes,[])
    def test_public_result_after_horizon_is_retained_but_not_completed(self):
        now=[100.0]
        self.controller.clock=lambda:now[0]
        original=SyntheticRuntime.evaluate
        def evaluate(runtime,release):
            result=original(runtime,release)
            now[0]=100+self.plan.horizon_seconds
            return result
        with patch.object(SyntheticRuntime,'evaluate',evaluate):result=self.controller._child(0)
        self.assertEqual(result['status'],'stopped_failure')
        self.assertEqual(result['reason'],'deadline_exhausted_after_public_evaluation')
        key='child.'+self.plan.cohort.trajectories[0].id+'.M1.g0.public-evaluation.result'
        self.assertEqual(self.controller.records.read(key)['status'],'completed')
        self.assertEqual([self.controller.records.read(x)['status'] for x in result['milestone_records']],['unfinished']*4)

    def test_exact_full_matrix_and_96_namespaced_roles(self):
        self.assertEqual([len(x.actors) for x in self.plan.roster.children],[8,8,20,20,20,20])
        self.assertEqual(len({a for x in self.plan.roster.children for a in x.actors}),96)
        self.assertEqual(self.plan.cohort.milestones,MILESTONES)
    def test_reject_missing_milestone_arm_role_or_wrong_placement(self):
        for change in ({'milestones':MILESTONES[:-1]}, {'trajectories':self.plan.cohort.trajectories[:-1]},
                       {'trajectories':(replace(self.plan.cohort.trajectories[0],decision_placement='durable_central_scheduler'),
                           *self.plan.cohort.trajectories[1:])},
                       {'trajectories':(replace(self.plan.cohort.trajectories[0],role_ids=('B01','R1')),
                           *self.plan.cohort.trajectories[1:])}):
            with self.subTest(change=change), self.assertRaises(StudyError):
                replace(self.plan,cohort=replace(self.plan.cohort,**change))
    def test_transport_policy_resources_and_release_pins_are_real_contract(self):
        for field in ('transport_sha256','shared_policy_sha256','requirements_release_sha256','resource_contract_sha256'):
            with self.subTest(field=field), self.assertRaises(StudyError):
                replace(self.plan,cohort=replace(self.plan.cohort,**{field:'f'*64}))
    def test_faults_and_matched_block_cannot_be_relabelled(self):
        first=self.plan.cohort.trajectories[1]
        for changed in (replace(first,faults=()),replace(first,block_seed_sha256='f'*64)):
            with self.assertRaises(StudyError):
                replace(self.plan,cohort=replace(self.plan.cohort,trajectories=(self.plan.cohort.trajectories[0],changed,*self.plan.cohort.trajectories[2:])))
    def test_source_inventory_cannot_omit_actual_controller(self):
        with self.assertRaises(StudyError):
            replace(self.plan,source_pins={'pinned.txt':self.plan.source_pins['pinned.txt']})
    def test_writable_scope_requires_exact_nonempty_files(self):
        for bad in ((),('library/catalog/',)):
            with self.assertRaises(StudyError):replace(self.plan,package_paths={**self.plan.package_paths,'catalog':bad})
    def test_source_change_prevents_any_child(self):
        (self.root/'pinned.txt').write_text('changed')
        with self.assertRaises(StudyError): self.controller.run()
        self.assertEqual(self.runtimes,[])
    def test_exact_four_milestones_carry_same_source_and_roles(self):
        record=self.controller._child(0)
        runtime=self.runtimes[0]
        self.assertEqual(runtime.releases,list(MILESTONES))
        self.assertEqual(len(runtime.works),8)
        for stage,directives in runtime.works:
            self.assertEqual(len(directives),4)
            self.assertTrue(all(d.context.trajectory_id==self.plan.cohort.trajectories[0].id for _,d in directives))
        self.assertEqual(record['status'],'completed')
        self.assertEqual(len(record['milestone_records']),4)
        self.assertTrue(runtime.closed)
        self.assertFalse(record['acceptance_authority'])
    def test_second_child_requires_original_first_seal(self):
        with self.assertRaises(StudyError):self.controller._child(1)
        self.assertEqual(self.runtimes,[])
    def test_failed_public_check_exhausts_repairs_and_keeps_four_unfinished(self):
        self.statuses={'M1':('completed','failed')}
        result=self.controller._child(0)
        self.assertEqual(result['status'],'stopped_failure')
        self.assertEqual(len(self.runtimes[0].works),4)
        self.assertEqual([self.controller.records.read(x)['status'] for x in result['milestone_records']],['unfinished']*4)
    def test_infrastructure_error_is_not_product_failure_or_success(self):
        self.statuses={'M2':('infrastructure_error','unknown')}
        result=self.controller._child(0)
        self.assertEqual(result['status'],'stopped_failure')
        self.assertIn('infrastructure_error',result['reason'])
        self.assertEqual([self.controller.records.read(x)['status'] for x in result['milestone_records']],
                         ['public_completed','unfinished','unfinished','unfinished'])
    def test_unknown_evaluation_intent_never_reexecutes(self):
        key='child.'+self.plan.cohort.trajectories[0].id+'.M1.g0.public-evaluation'
        self.controller.records.put(key+'.intent',{'source_sha256':'a'*64,'commit_oid':'a'*40,
            'release_sha256':self.plan.releases[0].sha256,'purpose':'public-cumulative-development-v1'})
        with patch.object(SyntheticRuntime,'evaluate',side_effect=AssertionError('must not execute')):
            result=self.controller._child(0)
        self.assertEqual(result['status'],'stopped_failure')
        self.assertIn('Unresolved effect',result['reason'])
    def test_terminal_resume_does_not_launch_roles(self):
        original=self.controller._child(0)
        self.assertEqual(self.controller._child(0),original)
        self.assertEqual(len(self.runtimes),1)
    def test_six_child_barrier_precedes_unavailable_acceptance(self):
        def barrier(roster,chain,expected,receipts,**kwargs):
            self.assertEqual(len(receipts),6)
            self.assertEqual(len(self.runtimes),6)
            self.assertTrue(all(x.closed for x in self.runtimes))
            return {'protocol':'synthetic-barrier','roster_sha256':roster.sha256,
                    'seal_ids':[str(i) for i in range(6)],'role_count':96,'checkpoint':asdict(expected)}
        with patch('gossip_harness.cumulative_study_controller_v1.verified_study_barrier',side_effect=barrier):
            result=self.controller.run()
        self.assertEqual(result['status'],'acceptance_unavailable')
        self.assertFalse(result['accepted'])
        self.assertEqual(result['milestone_count'],24)
        self.assertEqual(result['public_completed'],6)
        self.assertEqual(sum(len(x.works) for x in self.runtimes),48)
    def test_arbitrary_acceptance_callback_cannot_grant_pass(self):
        class Forged:
            def evaluate(self,*args):return {'accepted':True,'status':'accepted'}
        self.controller.acceptance=Forged()
        with patch('gossip_harness.cumulative_study_controller_v1.verified_study_barrier',return_value={'protocol':'synthetic'}):
            with self.assertRaises(StudyError):self.controller.run()
    def test_records_detect_tamper_and_unknown_effect(self):
        records=Records(self.chain)
        records.put('sample',{'x':1})
        with self.assertRaises(StudyError):records.put('sample',{'x':2})
        records.put('effect.intent',{'a':1})
        with self.assertRaises(StudyUnknown):records.effect('effect',{'a':1},lambda:{'must':'not run'})
    def test_evaluator_cannot_shrink_or_rebind_case_roster(self):
        release=self.plan.releases[0]
        result=PublicResult('completed','a'*40,'a'*64,release.sha256,(),(),{})
        with self.assertRaises(StudyError):result.validate(release,'a'*40,'a'*64)

if __name__=='__main__':unittest.main()
