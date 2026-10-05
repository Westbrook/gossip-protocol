"""Original anchored clock and actual controller paths; runtime endpoints synthetic.

No Engine, provider call, model-quality observation or execution approval.
"""
from dataclasses import asdict, replace
import threading
import time
import unittest
from unittest import mock

from gossip_harness import cumulative_child_deadline_v1 as clocks
from gossip_harness import cumulative_study_controller_v2 as study
from gossip_harness import cumulative_study_runtime_v2 as runtime
from gossip_harness import cumulative_generated_probe_accounting_v1 as accounting
from gossip_harness import cumulative_generated_probe_capacity_v1 as capacity
from gossip_harness import cumulative_generated_probe_corpus_v1 as corpus
from gossip_harness import cumulative_generated_probe_matrix_v1 as matrices
from gossip_harness import cumulative_generated_probe_ranked_originals_v1 as ranked
from gossip_harness import cumulative_generated_probe_values_v2 as values
from gossip_harness.candidate_checkpoint_chain_v1 import CheckpointChain
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from gossip_harness.peer_financial_authority_v2 import ledger_identity
from tests import test_cumulative_study_controller_v1 as controller_fixture
from tests.test_cumulative_study_repaired_runtime_v2 import CumulativeStudyRepairedRuntimeV2Tests as _PlanFixture
from tests.ranked_originals_fixture_v1 import OriginalFixture
from tests.test_cumulative_generated_probe_capacity_v1 import budget
from tests.test_cumulative_generated_probe_corpus_v1 import probe_plan


def selected_plan(plan):
    rt = {**plan.runtime, clocks.POLICY_KEY: dict(clocks.POLICY)}
    resource = {'horizon_seconds': 600, 'source_generations': plan.source_generations,
                'partition_seconds': plan.partition_seconds, 'executor_slots': plan.executor_slots, 'runtime': rt}
    return replace(plan, runtime=rt, horizon_seconds=600,
                   cohort=replace(plan.cohort, resource_contract_sha256=study.digest(resource)))


class OriginalChildClockTests(unittest.TestCase):
    def setUp(self):
        f = _PlanFixture('test_actual_constructor_selects_v5_finance_and_rpc_before_any_role_launch')
        f.setUp(); self.addCleanup(f.doCleanups); self.fixture=f
        self.plan=selected_plan(f.plan());self.root=f.root
        self.head=ExternalHead.create(self.root/'head', journal_roots=(self.root/'raw',self.root/'delta'))
        self.addCleanup(self.head.close)
        self.chain=CheckpointChain.create(self.root/'raw',self.root/'delta',context={'synthetic_clock':True},authority=self.head)
        self.addCleanup(self.chain.close);self.records=study.Records(self.chain)
        self.records.put('contract',self.plan.record())
        self.slot='child.'+self.plan.cohort.trajectories[0].id+'.begin'

    def start(self, **changes):
        row=clocks.begin(self.plan,0,lambda:100.0);row.update(changes)
        self.records.put(self.slot,row)
        return clocks.read(self.records,self.plan,0,active=True)

    def test_original_horizon_is_sampled_once_and_never_rebased(self):
        wall=mock.Mock(return_value=100.0)
        row=clocks.begin(self.plan,0,wall);wall.assert_called_once_with()
        self.records.put(self.slot,row);first=clocks.read(self.records,self.plan,0,active=True)
        with mock.patch.object(time,'monotonic_ns',return_value=first.started_ns+20_000_000_000):
            later=clocks.read(self.records,self.plan,0,active=True)
        self.assertEqual(first,later)
        self.assertEqual(first.deadline_ns-first.started_ns,600_000_000_000)
        self.assertEqual(first.deadline_unix,700.0)

    def test_wall_rollback_cannot_extend_and_wall_forward_jump_can_stop(self):
        clock=self.start()
        with mock.patch.object(time,'monotonic_ns',return_value=clock.deadline_ns):
            self.assertTrue(clock.expired(0.0))
        with mock.patch.object(time,'monotonic_ns',return_value=clock.started_ns+1):
            self.assertTrue(clock.expired(clock.deadline_unix))
            self.assertFalse(clock.expired(0.0))

    def test_foreign_incarnation_is_cold_readable_but_never_active(self):
        row=clocks.begin(self.plan,0,lambda:100.0);row['original_clock']['clock_domain']='child-clock-foreign'
        self.records.put(self.slot,row)
        cold=clocks.read(self.records,self.plan,0)
        self.assertEqual(cold.clock_domain,'child-clock-foreign')
        with self.assertRaisesRegex(ValueError,'foreign_child_clock'):
            clocks.read(self.records,self.plan,0,active=True)
        with self.assertRaisesRegex(ValueError,'foreign_child_clock'):cold.expired(100.0)

    def test_changed_horizon_identity_or_incomplete_legacy_start_refused(self):
        row=clocks.begin(self.plan,0,lambda:100.0);row['original_clock']['deadline_ns']+=1
        self.records.put(self.slot,row)
        with self.assertRaisesRegex(ValueError,'monotonic_horizon'):
            clocks.read(self.records,self.plan,0)

    def test_late_child_origin_after_runtime_setup_refused(self):
        self.records.put('runtime.'+self.plan.cohort.trajectories[0].id+'.source-initialization.intent',{'synthetic':True})
        with self.assertRaisesRegex(ValueError,'follows_runtime_work'):self.start()

    def test_clock_prefix_tampering_fails_closed(self):
        self.start();path=self.chain.raw_root/self.records.name(self.slot)
        path.chmod(0o600);path.write_bytes(b'{}')
        with self.assertRaises(ValueError):clocks.read(self.records,self.plan,0)

    def test_immutable_projection_is_safe_for_rpc_thread_without_journal_access(self):
        original=self.start();answers=[]
        def target():answers.append(original.expired(100.0))
        t=threading.Thread(target=target);t.start();t.join(5)
        self.assertFalse(t.is_alive());self.assertEqual(answers,[False])

    def test_policy_must_be_explicit_exact_and_prospectively_pinned(self):
        self.assertFalse(clocks.selected(self.fixture.plan()))
        with mock.patch.dict(self.plan.runtime,{clocks.POLICY_KEY:{'protocol':clocks.PROTOCOL}}):
            with self.assertRaisesRegex(ValueError,'exact_original_child_deadline_policy'):
                clocks.selected(self.plan)
        with self.assertRaises(ValueError):
            replace(self.plan,source_pins={**self.plan.source_pins,clocks.SOURCE:'a'*64})


class OriginalChildControllerIntegrationTests(unittest.TestCase):
    def setUp(self):
        f=OriginalChildClockTests('test_original_horizon_is_sampled_once_and_never_rebased')
        f.setUp();self.addCleanup(f.doCleanups);self.fixture=f
        self.plan,self.records=f.plan,f.records
        self.ledger=f.root/'ledger.sqlite';self.ledger.write_bytes(b'inert ledger identity fixture')
        self.runtimes=[]
        def factory(*args):
            result=controller_fixture.SyntheticRuntime(*args,statuses={})
            self.runtimes.append(result);return result
        self.controller=study.StudyController(self.plan,checkpoint=f.chain,expected_checkpoint=f.chain.commitment,
            repository=f.fixture.repository,existing_ledger_path=self.ledger,
            expected_ledger_identity=ledger_identity(self.ledger),runtime_factory=factory,clock=lambda:100.0)

    def test_original_is_retained_before_runtime_factory(self):
        factory=self.controller.runtime_factory;seen=[]
        def inspect(*args):
            seen.append(clocks.read(self.records,self.plan,0,active=True))
            return factory(*args)
        self.controller.runtime_factory=inspect
        result=self.controller._child(0)
        self.assertEqual(result['status'],'completed');self.assertEqual(len(seen),1)
        self.assertFalse(result['acceptance_authority'])

    def test_monotonic_expiry_after_public_result_retains_result_without_completion_credit(self):
        prior=controller_fixture.SyntheticRuntime.evaluate
        now=[time.monotonic_ns()]
        def evaluate(child,release):
            result=prior(child,release)
            now[0]=clocks.read(self.records,self.plan,0).deadline_ns
            return result
        with mock.patch.object(time,'monotonic_ns',side_effect=lambda:now[0]), \
                mock.patch.object(controller_fixture.SyntheticRuntime,'evaluate',evaluate):
            result=self.controller._child(0)
        self.assertEqual(result['reason'],'deadline_exhausted_after_public_evaluation')
        key='child.'+self.plan.cohort.trajectories[0].id+'.M1.g0.public-evaluation.result'
        self.assertEqual(self.records.read(key)['status'],'completed')
        self.assertTrue(all(self.records.read(k)['status']=='unfinished' for k in result['milestone_records']))

    def test_expired_start_is_not_renewed_before_factory(self):
        clock=self.fixture.start();before=self.fixture.chain.commitment
        with mock.patch.object(time,'monotonic_ns',return_value=clock.deadline_ns):
            with self.assertRaisesRegex(ValueError,'before_runtime_construction'):self.controller._child(0)
        self.assertEqual(self.runtimes,[]);self.assertEqual(self.fixture.chain.commitment,before)

    def test_runtime_rejects_substituted_wall_deadline_before_setup(self):
        clock=self.fixture.start();f=self.fixture
        with mock.patch.object(runtime.GossipChildRuntime,'_initialize_source') as setup:
            with self.assertRaisesRegex(ValueError,'deadline differs'):
                runtime.GossipChildRuntime(self.plan,0,self.records,clock.deadline_unix+1,
                    root=f.root/'runtime',repository=f.fixture.repository,existing_ledger_path=self.ledger,
                    expected_ledger_identity=ledger_identity(self.ledger),workers=f.fixture.workers,mode='fixture',
                    permit_provider=mock.Mock(),evaluator=runtime.DockerPublicChecks(self.plan.runtime['image'],timeout=180),clock=lambda:100.0)
            setup.assert_not_called()

    def test_runtime_guard_uses_original_monotonic_deadline_in_rpc_thread(self):
        clock=self.fixture.start();owner=object.__new__(runtime.GossipChildRuntime)
        owner.plan,owner.original_child_clock=self.plan,clock
        owner._original_child_clock_identity=study.digest(asdict(clock));owner.deadline=clock.deadline_unix
        owner.clock=lambda:0.0;owner.ledger=self.ledger;owner.expected_ledger_identity=ledger_identity(self.ledger)
        owner.payloads=mock.Mock();errors=[]
        def guarded():
            try:owner._request_guard(object())
            except Exception as e:errors.append(str(e))
        with mock.patch.object(time,'monotonic_ns',return_value=clock.deadline_ns):
            t=threading.Thread(target=guarded);t.start();t.join(5)
        self.assertFalse(t.is_alive());self.assertEqual(errors,['deadline_exhausted'])
        owner.payloads.request_guard.assert_not_called()


class OriginalChildProbeAccountingTests(unittest.TestCase):
    def fixture(self):
        quotas=corpus.Quotas(2,8)
        def plan_change(plan):
            plan=selected_plan(probe_plan(plan))
            return replace(plan,source_pins={**plan.source_pins,**accounting.sources()})
        def install(f):
            o=f.owner;o.records.put('contract',o.plan.record())
            row=clocks.begin(o.plan,o.index,time.time)
            o.records.put('child.'+o.trajectory.id+'.begin',row)
            o.original_child_clock=clocks.read(o.records,o.plan,o.index,active=True)
            o._original_child_clock_identity=study.digest(asdict(o.original_child_clock))
            o.deadline=row['deadline'];o.clock=time.time
            corpus.install_contract(o,expected=f.chain.commitment,quotas=quotas,limits=f.limits)
            f.book=capacity.ReservationLedger(o.records,budget(),clock_domain=clocks.clock_domain(),expected=f.chain.commitment,create=True)
        def freeze(f):
            ranked.freeze_build_phase(f.owner,expected=f.chain.commitment,milestone=f.milestone,generation=f.generation,limits=f.limits)
            compiled,_=ranked.reconstruct_frozen_builds(f.owner,expected=f.chain.commitment,milestone=f.milestone,generation=f.generation,limits=f.limits)
            n=study.MILESTONES.index(f.milestone)
            f.matrix=matrices.compile_matrix(compiled,current=f.plan.releases[n],inherited=f.plan.releases[n-1],active_probes=())
        f=OriginalFixture(plan_change=plan_change,before_contract=install,at_build_boundary=freeze,builds_only=True)
        self.addCleanup(f.close);f.quotas=quotas
        window=f.owner.probe_capacity_window(review_ns=3,selection_ns=0)
        charges=tuple(capacity.Charge(c.sha256,100,20,30,10,5) for c in f.matrix.base_cells)
        f.declaration=capacity.declaration(f.matrix,charges,window,phase='before-review')
        return f

    def reserve(self,f,body):
        return accounting.reserve(f.owner,f.book,values.canonical(body),expected=f.chain.commitment,
            milestone=f.milestone,generation=f.generation,phase='before-review',quotas=f.quotas,limits=f.limits)

    def test_actual_admission_and_cold_read_bind_original_child_window(self):
        f=self.fixture();r=self.reserve(f,f.declaration)
        self.assertTrue(r['whole_child_deadline_bound']);self.assertFalse(r['executor_leases_issued'])
        original=f.owner.original_child_clock
        with mock.patch.object(time,'monotonic_ns',return_value=original.deadline_ns+1):
            cold=accounting.inspect(f.owner,f.book,r['decision']['slot'],expected=f.chain.commitment,
                milestone=f.milestone,generation=f.generation,phase='before-review',quotas=f.quotas,limits=f.limits)
        self.assertEqual(cold['original_child_clock'],r['original_child_clock'])
        self.assertTrue(cold['whole_child_deadline_bound'])

    def test_self_consistent_extended_or_restarted_window_cannot_reserve(self):
        f=self.fixture()
        for key in ('started_ns','deadline_ns'):
            body=values.canonical(f.declaration)
            import json
            changed=json.loads(body);changed['window'][key]+=1
            before=f.chain.commitment
            with self.assertRaisesRegex(ValueError,'window_differs_from_original'):
                self.reserve(f,changed)
            self.assertEqual(f.chain.commitment,before)
        self.assertEqual(f.book.snapshot()['entries'],[])

    def test_expired_original_stops_window_derivation_and_new_allocation(self):
        f=self.fixture();before=f.chain.commitment
        with mock.patch.object(time,'monotonic_ns',return_value=f.owner.original_child_clock.deadline_ns):
            with self.assertRaisesRegex(ValueError,'deadline_exhausted_before_probe'):
                f.owner.probe_capacity_window(review_ns=3,selection_ns=0)
            with self.assertRaisesRegex(ValueError,'deadline_exhausted_before_probe'):
                self.reserve(f,f.declaration)
        self.assertEqual(f.chain.commitment,before)
