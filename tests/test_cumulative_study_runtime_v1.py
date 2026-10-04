"""Scoped mechanical API checks; no Docker/provider/process dispatch."""
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from gossip_harness.candidate_checkpoint_chain_v1 import CheckpointChain
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from gossip_harness.cumulative_study_controller_v1 import PACKAGES, Records, StudyError, StudyUnknown
from gossip_harness.cumulative_study_runtime_v1 import DockerPublicChecks, GossipChildRuntime
from gossip_harness.gitstore import GitStore
from tests.test_cumulative_study_controller_v1 import plan


class CumulativeStudyRuntimeV1Tests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.root=Path(self.tmp.name).resolve()
        self.plan=plan(self.root)
        self.head=ExternalHead.create(self.root/'anchor',journal_roots=(self.root/'raw',self.root/'delta'))
        self.chain=CheckpointChain.create(self.root/'raw',self.root/'delta',context={'fixture':'runtime'},authority=self.head)
        self.runtime=object.__new__(GossipChildRuntime)
        self.runtime.plan,self.runtime.root=self.plan,self.root
        self.runtime.records=Records(self.chain)
        self.runtime.protected=GitStore.create(self.root/'protected.git',self.plan.initial_files)
        self.runtime._verify_result=lambda _:None  # isolated original-finance boundary
    def tearDown(self):
        self.chain.close();self.head.close();self.tmp.cleanup()
    def proposals(self,stage='generation'):
        source=self.runtime.source()
        builds=[];reviews=[]
        for i,p in enumerate(PACKAGES):
            actor=f'fixture.B{1+4*i:02}'
            builds.append({'actor':actor,'worker_request':{'base_sha':source['commit_oid'],'files':source['files']},
                'snapshot':{'reply':{'state':'completed'}},'original_ref':{'fixture':actor},
                'result_payload':{'payload':{'changes':{'library/'+p+'/impl.py':'VALUE = '+str(i)+'\n'}}}})
            import json
            decision={'stage_id':stage,'package':p,'selected_actor':actor,'reasons':'fixture opinion','tests':['not executed']}
            reviews.append({'actor':'fixture.R'+str(i+1),'snapshot':{'reply':{'state':'completed'}},
                'original_ref':{'fixture':'review'+str(i)},'result_payload':{'payload':{'changes':{'decision.json':json.dumps(decision)}}}})
        return tuple(builds),tuple(reviews)
    def test_original_success_cannot_be_swapped_to_a_different_directive(self):
        from dataclasses import asdict, replace
        from types import SimpleNamespace
        import json
        from gossip_harness.cumulative_study_controller_v1 import plain
        from gossip_harness.peer_project_contract_v2 import Context, WorkKey, canonical_bytes
        from gossip_harness.peer_role_loop_v2 import RoleLoop, WorkDirective, directive_id
        from tests.test_peer_role_loop_v2 import FakeMesh, FakeFinance
        mesh=FakeMesh('fixture.B01')
        source=mesh.arrive('seed','project-source',canonical_bytes({'files':{'src/a.py':'old'},'base_sha':'a'*40}))
        directive=WorkDirective(Context(self.plan.sha256,'fixture','fixture',1,self.plan.releases[0].sha256),
            WorkKey('catalog','M1','B01',0),'mini','build',source,(),('src/a.py',),'Build fixture')
        finance=FakeFinance(mesh,lambda:100.0)
        loop=RoleLoop(self.root/'result-loop','fixture.B01',mesh,finance,call_limit=2,
            policy_sha256=self.plan.cohort.shared_policy_sha256,result_producer='finance',clock=lambda:100.0)
        self.addCleanup(loop.close)
        key=loop.enqueue(directive)
        for _ in range(20):
            snapshot=loop.tick()
            if snapshot and snapshot.reply and snapshot.reply.state=='pending':break
        snapshot=loop.snapshots()[0]
        finance.finish(snapshot.action.request_id)
        for _ in range(20):
            loop.tick()
            if loop.snapshots()[0].state=='published':break
        snapshot=loop.snapshots()[0]
        request=loop.worker_request(key)
        payload=json.loads(mesh.resolve(snapshot.result_ref))
        value={'actor':'fixture.B01','directive_id':key,'snapshot':plain(asdict(snapshot)),
               'worker_request':plain(asdict(request)),'result_payload':payload}
        self.runtime.key='runtime.fixture'
        self.runtime.seed=mesh
        self.runtime.finance=SimpleNamespace(verified_terminal=lambda _: {'reply':snapshot.reply,'worker_request':request,'result_payload':payload})
        self.runtime.records.put('runtime.fixture.directive.'+key,{'actor':'fixture.B01','directive':directive.to_dict()})
        GossipChildRuntime._verify_result(self.runtime,value)
        changed=replace(directive,work=replace(directive.work,generation=1))
        newkey=directive_id(changed)
        self.runtime.records.put('runtime.fixture.directive.'+newkey,{'actor':'fixture.B01','directive':changed.to_dict()})
        value['directive_id']=newkey
        with self.assertRaises(StudyError):GossipChildRuntime._verify_result(self.runtime,value)
    def test_deadline_guard_prevents_new_financial_admission(self):
        from types import SimpleNamespace
        from gossip_harness.peer_financial_rpc_v2 import FinancialDenied
        seen=[]
        self.runtime.clock=lambda:100.0
        self.runtime.deadline=100.0
        self.runtime.payloads=SimpleNamespace(request_guard=lambda action:seen.append(action))
        with self.assertRaises(FinancialDenied):self.runtime._request_guard(object())
        self.assertEqual(seen,[])
    def test_work_horizon_checked_before_any_publication(self):
        self.runtime.repository=self.root
        self.runtime.clock=lambda:100.0
        self.runtime.deadline=100.0
        with patch.object(self.runtime,'publish',side_effect=AssertionError('no dispatch')):
            with self.assertRaises(StudyError):self.runtime.work('late',())
    def _initial_fault_work(self, arm):
        from types import SimpleNamespace
        from gossip_harness.peer_project_contract_v2 import Context, WorkKey, EvidenceRef
        from gossip_harness.peer_role_loop_v2 import WorkDirective
        trajectory=next(t for t in self.plan.cohort.trajectories if t.arm==arm and t.block=='compound_recovery')
        runtime=self.runtime
        runtime.trajectory=trajectory
        runtime.key='runtime.'+trajectory.id
        actor=trajectory.id+'.B01'
        runtime.child=SimpleNamespace(actors=(actor,))
        runtime.peers={'seed':1001,'finance':1002,actor:1003}
        runtime.transport={'observer_key':'fixture'}
        runtime.repository=self.root
        runtime.partition_until=None
        runtime.partitioned=False
        runtime.partition_publication_pending=False
        runtime.processes={}
        runtime.clock=lambda:100.0
        runtime.deadline=9999
        source=EvidenceRef('a'*64,'seed','project-source','b'*64)
        directive=WorkDirective(Context(self.plan.sha256,'fixture',trajectory.id,1,self.plan.releases[0].sha256),
            WorkKey('catalog','M1','B01',0),'mini','build',source,(),('library/catalog/impl.py',),'Build fixture')
        return actor,directive
    def test_partition_is_acknowledged_before_all_initial_publications_for_both_placements(self):
        from types import SimpleNamespace
        from gossip_harness.peer_project_contract_v2 import EvidenceRef
        from gossip_harness.cumulative_study_controller_v1 import digest
        class StopAfterDispatch(Exception):pass
        for arm in ('S4-G','O16-G'):
            with self.subTest(arm=arm):
                actor,directive=self._initial_fault_work(arm)
                runtime=self.runtime
                now=[100.0]
                runtime.clock=lambda:now[0]
                edges={}
                publications=[]
                def block(_port,node,_key,_operation,payload):
                    edges[node]=payload['peers']
                    return {'blocked':sorted(payload['peers'])}
                def publish(kind,raw,command):
                    self.assertEqual(set(edges),set(runtime.peers))
                    self.assertTrue(all(edges.values()))
                    start=self.chain.position(Records.name(runtime.key+'.partition-start.result'))
                    frontier=self.chain.position(Records.name(runtime.key+'.partition-frontier'))
                    self.assertLess(start,frontier)
                    self.assertTrue(runtime.partition_publication_pending)
                    publications.append(kind)
                    now[0]+=1  # Exceed the nominal partition time before O's first assignment.
                    return EvidenceRef(command,'seed',kind,digest({'raw':raw.hex()}))
                runtime.seed=SimpleNamespace(publish=publish)
                runtime._wait=lambda _:(_ for _ in ()).throw(StopAfterDispatch())
                with patch('gossip_harness.cumulative_study_runtime_v1.observer_request',side_effect=block):
                    with self.assertRaises(StopAfterDispatch):runtime.work(arm+'.build',((actor,directive),))
                    self.assertFalse(runtime.partition_publication_pending)
                    first=runtime.records.read(runtime.key+'.partition-first-publication')
                    self.assertEqual(first['boundary'],'first-work-publication-durably-acknowledged')
                    self.assertEqual(first['observed_at'],101.0)
                    frontier=runtime.records.read(runtime.key+'.partition-frontier')
                    complete=runtime.records.read(runtime.key+'.partition-frontier-complete')
                    for publication in complete['publications']:
                        self.assertGreater(publication['position'],self.chain.position(Records.name(runtime.key+'.partition-frontier')))
                    self.assertEqual(len(frontier['publication_keys']),len(publications))
                    runtime._tick()
                    self.assertTrue(all(not blocked for blocked in edges.values()))
                self.assertEqual(publications,['cumulative-work'] if arm=='S4-G' else
                    ['cumulative-work','cumulative-central-decision','cumulative-assignment'])
    def test_incomplete_partition_acknowledgement_prevents_work_and_cannot_retry(self):
        actor,directive=self._initial_fault_work('S4-G')
        with patch('gossip_harness.cumulative_study_runtime_v1.observer_request',return_value={'blocked':[]}) as observer:
            with patch.object(self.runtime,'publish',side_effect=AssertionError('no work')):
                with self.assertRaisesRegex(StudyError,'acknowledgement'):self.runtime.work('fixture.build',((actor,directive),))
                count=observer.call_count
                with self.assertRaisesRegex(StudyError,'retried'):self.runtime.work('fixture.build',((actor,directive),))
                self.assertEqual(observer.call_count,count)
        self.assertIsNotNone(self.runtime.records.read(self.runtime.key+'.partition-start.intent'))
        self.assertIsNone(self.runtime.records.read(self.runtime.key+'.partition-start.result'))

    def test_preexisting_initial_publication_is_rejected_before_partition(self):
        actor,directive=self._initial_fault_work('S4-G')
        self.runtime.records.put('publish.fixture.build.work.intent',{'old':'publication'})
        with patch('gossip_harness.cumulative_study_runtime_v1.observer_request',side_effect=AssertionError('no activation')):
            with self.assertRaisesRegex(StudyError,'predates'):self.runtime.work('fixture.build',((actor,directive),))
        self.assertIsNone(self.runtime.records.read(self.runtime.key+'.partition-start.intent'))

    def test_unrelated_existing_git_source_cannot_be_adopted(self):
        self.runtime.key='runtime.fixture'
        before=self.runtime.protected.head()
        with self.assertRaisesRegex(StudyError,'Preexisting Git'):self.runtime._initialize_source()
        self.assertEqual(self.runtime.protected.head(),before)
        self.assertIsNone(self.runtime.records.read(self.runtime.key+'.source-initialization.intent'))
    def test_fresh_git_seed_is_bound_before_any_runtime_effect(self):
        self.runtime.root=self.root/'fresh-child'
        self.runtime.root.mkdir()
        self.runtime.key='runtime.fresh'
        store=self.runtime._initialize_source()
        original=self.runtime.records.read(self.runtime.key+'.source-initialization.result')
        self.assertEqual(original['files'],self.plan.initial_files)
        self.assertEqual(original['commit_oid'],store.head())
        with self.assertRaisesRegex(StudyError,'cannot be adopted'):self.runtime._initialize_source()

    def test_cleanup_never_signals_already_observed_empty_incarnation(self):
        from types import SimpleNamespace
        from unittest.mock import Mock
        import signal
        old=SimpleNamespace(pid=111,wait=Mock())
        current=SimpleNamespace(pid=222,wait=Mock())
        self.runtime.closed=False
        self.runtime.observed_exits={('fixture.B01',0)}
        self.runtime.bridge=SimpleNamespace(handles=lambda:(('fixture.B01',0,old),('fixture.B01',1,current)))
        self.runtime.server=self.runtime.server_thread=self.runtime.finance=self.runtime.payloads=None
        self.runtime.nodes=[]
        with patch('gossip_harness.cumulative_study_runtime_v1.os.killpg') as kill:
            self.runtime.close()
        kill.assert_called_once_with(222,signal.SIGTERM)
        old.wait.assert_not_called()
        current.wait.assert_called_once_with(timeout=3)
    def test_cleanup_attempts_later_writers_after_an_earlier_failure(self):
        from types import SimpleNamespace
        from unittest.mock import Mock
        self.runtime.closed=False
        self.runtime.observed_exits=set()
        self.runtime.bridge=SimpleNamespace(handles=lambda:())
        self.runtime.server=SimpleNamespace(server_close=Mock(side_effect=OSError('close failed')))
        self.runtime.server_thread=None
        self.runtime.finance=SimpleNamespace(close=Mock())
        self.runtime.payloads=SimpleNamespace(close=Mock())
        node=SimpleNamespace(close=Mock(),tick_thread=None,server_thread=None)
        self.runtime.nodes=[node]
        with self.assertRaises(StudyUnknown):self.runtime.close()
        self.runtime.finance.close.assert_called_once()
        self.runtime.payloads.close.assert_called_once()
        node.close.assert_called_once()
        self.assertFalse(self.runtime.closed)
        with self.assertRaises(StudyUnknown):self.runtime.close()
        self.runtime.finance.close.assert_called_once()
    def test_real_distributed_git_scoped_merge_and_exact_resume(self):
        builds,reviews=self.proposals()
        result=self.runtime.integrate('generation',builds,reviews)
        self.assertEqual(result['status'],'integrated')
        self.assertEqual(len(result['private_git_merges']),4)
        self.assertEqual(len(list((self.root/'private-git').iterdir())),4)
        current=self.runtime.protected.head()
        replay=self.runtime.integrate('generation',builds,reviews)
        self.assertEqual(result,replay)
        self.assertEqual(current,self.runtime.protected.head())
        self.assertFalse(result['acceptance_authority'])
    def test_out_of_scope_candidate_cannot_change_source(self):
        builds,reviews=self.proposals()
        builds[0]['result_payload']['payload']['changes']={'base.py':'corrupt'}
        before=self.runtime.protected.head()
        result=self.runtime.integrate('generation',builds,reviews)
        self.assertEqual(result['status'],'selection_rejected')
        self.assertEqual(before,self.runtime.protected.head())
    def test_reviewer_cannot_select_unknown_candidate(self):
        import json
        builds,reviews=self.proposals()
        row=json.loads(reviews[0]['result_payload']['payload']['changes']['decision.json'])
        row['selected_actor']='unregistered'
        reviews[0]['result_payload']['payload']['changes']['decision.json']=json.dumps(row)
        self.assertEqual(self.runtime.integrate('generation',builds,reviews)['status'],'selection_rejected')
    def test_candidate_base_must_match_original_source(self):
        builds,reviews=self.proposals()
        builds[0]['worker_request']['files']={'base.py':'other'}
        with self.assertRaises(StudyError):self.runtime.integrate('generation',builds,reviews)
    def test_private_git_unknown_proposal_never_reproposes(self):
        builds,_=self.proposals()
        self.runtime.records.put('generation.merge.catalog.proposal-intent',{'fixture':'interrupted'})
        with self.assertRaises(StudyUnknown):self.runtime._merge_private('generation',builds[0],'catalog')
        self.assertFalse((self.root/'private-git').exists())
    def test_public_sandbox_cleanup_failure_cannot_become_pass(self):
        import json
        release=self.plan.releases[0]
        class Validator:
            def __init__(self,*args):self.last_receipt={}
            def __call__(self,path):
                self.last_receipt={'output':json.dumps({'purpose':release.purpose,'outcomes':[[release.ordered_check_ids[0],'passed']]}),
                    'status':'cleanup_failed','cleanup_verified':False,'output_truncated':False}
                return False,'cleanup unknown'
        with patch('gossip_harness.sandbox.DockerValidator',Validator):
            result=DockerPublicChecks('sha256:'+'a'*64)(self.runtime.protected,release)
        self.assertEqual(result.status,'infrastructure_error')
        self.assertFalse(result.passed)
    def test_public_sandbox_requires_full_ordered_output(self):
        import json
        release=self.plan.releases[0]
        class Validator:
            def __init__(self,*args):self.last_receipt={}
            def __call__(self,path):
                self.last_receipt={'output':json.dumps({'purpose':release.purpose,'outcomes':[]}),
                    'status':'passed','cleanup_verified':True,'output_truncated':False}
                return True,'exit0'
        with patch('gossip_harness.sandbox.DockerValidator',Validator):
            result=DockerPublicChecks('sha256:'+'a'*64)(self.runtime.protected,release)
        self.assertEqual(result.status,'infrastructure_error')
    def test_public_failed_assertion_preserved(self):
        import json
        release=self.plan.releases[0]
        class Validator:
            def __init__(self,*args):self.last_receipt={}
            def __call__(self,path):
                self.last_receipt={'output':json.dumps({'purpose':release.purpose,'outcomes':[[release.ordered_check_ids[0],'failed']]}),
                    'status':'failed','cleanup_verified':True,'output_truncated':False}
                return False,'assertion failed'
        with patch('gossip_harness.sandbox.DockerValidator',Validator):
            result=DockerPublicChecks('sha256:'+'a'*64)(self.runtime.protected,release)
        self.assertEqual(result.status,'completed')
        self.assertFalse(result.passed)
        self.assertEqual(result.outcomes[0][1],'failed')

if __name__=='__main__':unittest.main()
