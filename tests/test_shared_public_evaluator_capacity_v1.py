"""Actual joined ledger/model executor; synthetic evaluator, no provider/Engine."""
from contextlib import ExitStack
from dataclasses import asdict, replace
import threading
import time
import unittest
from unittest.mock import Mock, patch

from gossip_harness import cumulative_study_runtime_v2 as runtime
from gossip_harness import cumulative_study_controller_v2 as study
from gossip_harness import cumulative_child_deadline_v1 as clocks
from gossip_harness.peer_financial_authority_v5 import EVALUATOR_CAPACITY_KEY, EVALUATOR_CAPACITY_POLICY
from gossip_harness.peer_financial_terminal_v1 import SealBusy
from tests.financial_v5_fixture import Fixture
from tests.test_cumulative_child_deadline_v1 import OriginalChildClockTests as _ClockFixture
from tests import test_cumulative_child_deadline_v1 as clock_fixture


class _SharedEvaluatorFixture(Fixture):
    def permit(self, opening):
        result=super().permit(opening)
        result['execution_design']['runtime']={EVALUATOR_CAPACITY_KEY:dict(EVALUATOR_CAPACITY_POLICY)}
        result['execution_design_sha256']=study.digest(result['execution_design'])
        return result

    def deadline(self):
        return time.monotonic_ns()+5_000_000_000

    def free_slots(self):
        count=0
        while self.authority.slots.acquire(blocking=False):count+=1
        for _ in range(count):self.authority.slots.release()
        return count


class SharedEvaluatorFinancialTests(_SharedEvaluatorFixture, unittest.TestCase):
    def test_model_submit_waits_while_actual_evaluator_slots_are_held(self):
        a=self.open();before=self.ledger.budget()
        with ExitStack() as stack:
            first=stack.enter_context(a.evaluation_slot(deadline_ns=self.deadline()))
            second=stack.enter_context(a.evaluation_slot(deadline_ns=self.deadline()))
            action,lease=self.start()
            self.assertEqual(a.lookup(self.actor,action.request_id).reason,'executor_capacity')
            self.assertEqual(self.transport.calls,[]);self.assertEqual(self.ledger.budget(),before)
            first.confirm_cleanup();second.confirm_cleanup()
        self.assertEqual(self.free_slots(),2)
        self.assertEqual(a.submit(self.actor,action,lease).state,'pending')
        self.assertEqual(self.terminal(action).state,'completed')

    def test_real_pending_models_block_evaluator_until_they_finish(self):
        a=self.open();self.transport.release.clear();entered=threading.Event();done=threading.Event();errors=[]
        actions=[self.start(i)[0] for i in range(2)]
        def evaluation():
            try:
                with a.evaluation_slot(deadline_ns=self.deadline()) as slot:
                    entered.set();slot.confirm_cleanup()
            except BaseException as e:errors.append(e)
            finally:done.set()
        t=threading.Thread(target=evaluation);t.start()
        try:
            self.assertFalse(entered.wait(.1));self.assertEqual(a.active_evaluations,0)
        finally:self.transport.release.set()
        self.assertTrue(done.wait(5));t.join(1);self.assertEqual(errors,[]);self.assertTrue(entered.is_set())
        for action in actions:self.assertEqual(self.terminal(action).state,'completed')
        self.assertEqual(self.free_slots(),2)

    def test_expired_wait_does_not_dispatch_or_leak_capacity(self):
        a=self.open();self.transport.release.clear();actions=[self.start(i)[0] for i in range(2)]
        try:
            with self.assertRaisesRegex(ValueError,'deadline exhausted'):
                with a.evaluation_slot(deadline_ns=time.monotonic_ns()+30_000_000):self.fail('No evaluator entry')
            self.assertEqual(a.active_evaluations,0);self.assertEqual(a.unresolved_evaluations,0)
        finally:self.transport.release.set()
        for action in actions:self.terminal(action)
        self.assertEqual(self.free_slots(),2)

    def test_unknown_cleanup_keeps_slot_and_halts_without_allowing_terminal_seal(self):
        a=self.open()
        with self.assertRaisesRegex(RuntimeError,'lost evaluator'):
            with a.evaluation_slot(deadline_ns=self.deadline()):raise RuntimeError('lost evaluator')
        self.assertEqual(a.unresolved_evaluations,1);self.assertEqual(self.free_slots(),1)
        self.assertTrue(a.failed_closed)
        with self.assertRaisesRegex(ValueError,'halted'):
            with a.evaluation_slot(deadline_ns=self.deadline()):self.fail('No readmission')
        with self.assertRaises(SealBusy):self.seal()
        self.assertEqual(self.transport.calls,[])

    def test_terminal_seal_refused_while_evaluator_is_active_then_allowed_after_cleanup(self):
        a=self.open()
        with a.evaluation_slot(deadline_ns=self.deadline()) as slot:
            with self.assertRaises(SealBusy):self.seal()
            slot.confirm_cleanup()
        self.seal()
        with self.assertRaises(ValueError):
            with a.evaluation_slot(deadline_ns=self.deadline()):self.fail('Sealed execution refused')
        self.assertEqual(self.free_slots(),2)

    def test_close_waits_for_active_evaluator_scope(self):
        a=self.open();closed=threading.Event()
        with a.evaluation_slot(deadline_ns=self.deadline()) as slot:
            t=threading.Thread(target=lambda:(a.close(),closed.set()));t.start()
            try:self.assertFalse(closed.wait(.1))
            finally:slot.confirm_cleanup()
        self.assertTrue(closed.wait(5));t.join(1)
        self.assertEqual(self.free_slots(),2)

    def test_changed_pool_or_policy_is_refused_before_admission(self):
        a=self.open();old=a.slots;a.slots=threading.BoundedSemaphore(2)
        try:
            with self.assertRaisesRegex(ValueError,'owner changed'):
                with a.evaluation_slot(deadline_ns=self.deadline()):self.fail('No replaced pool')
        finally:a.slots=old
        self.assertEqual(self.free_slots(),2)
        a.permit['execution_design']['runtime'][EVALUATOR_CAPACITY_KEY]={'protocol':'other'}
        with self.assertRaisesRegex(ValueError,'Prospective shared evaluator policy'):
            with a.evaluation_slot(deadline_ns=self.deadline()):self.fail('No changed policy')

    def test_expired_free_slot_is_rejected_and_confirmed_cleanup_is_one_scope_only(self):
        a=self.open()
        with self.assertRaisesRegex(ValueError,'deadline exhausted'):
            with a.evaluation_slot(deadline_ns=time.monotonic_ns()-1):self.fail('No expired entry')
        with a.evaluation_slot(deadline_ns=self.deadline()) as slot:slot.confirm_cleanup()
        with self.assertRaises(ValueError):slot.confirm_cleanup()
        self.assertEqual(self.free_slots(),2)


class SharedEvaluatorRuntimeTests(_SharedEvaluatorFixture, unittest.TestCase):
    def owner(self, *, cleanup=True):
        self.open()
        f=_ClockFixture('test_original_horizon_is_sampled_once_and_never_rebased')
        prior=clock_fixture.selected_plan
        def selected(plan):
            p=prior(plan);rt={**p.runtime,EVALUATOR_CAPACITY_KEY:dict(EVALUATOR_CAPACITY_POLICY)}
            resource={k:getattr(p,k) for k in ('horizon_seconds','source_generations','partition_seconds','executor_slots')}
            resource['runtime']=rt
            return replace(p,runtime=rt,cohort=replace(p.cohort,resource_contract_sha256=study.digest(resource)))
        with patch.object(clock_fixture,'selected_plan',selected):f.setUp()
        self.addCleanup(f.doCleanups)
        original=f.start();owner=object.__new__(runtime.GossipChildRuntime)
        owner.plan=f.plan
        owner.index=0;owner.records=f.records;owner.original_child_clock=original
        owner._original_child_clock_identity=study.digest(asdict(original));owner.deadline=original.deadline_unix
        owner.clock=lambda:100.0;owner.finance=self.authority;owner._shared_evaluator_finance=self.authority
        owner.protected=object();owner._tick=Mock();release=f.plan.releases[0]
        owner.source=Mock(return_value={'commit_oid':'a'*40,'source_sha256':'b'*64})
        owner.evaluator=Mock(return_value=study.PublicResult('completed','a'*40,'b'*64,release.sha256,
            release.ordered_check_ids,tuple((x,'passed') for x in release.ordered_check_ids),{'sandbox':{'cleanup_verified':cleanup}}))
        return owner,release

    def test_actual_runtime_evaluate_uses_financial_slot_through_cleanup(self):
        owner,release=self.owner();result=owner.evaluator.return_value
        def run(*_args):
            self.assertEqual(self.free_slots(),1);self.assertEqual(self.authority.active_evaluations,1)
            return result
        owner.evaluator.side_effect=run
        self.assertTrue(owner.evaluate(release).passed);self.assertEqual(self.free_slots(),2)

    def test_actual_runtime_uncertain_cleanup_poison_keeps_capacity(self):
        owner,release=self.owner(cleanup=False)
        with self.assertRaisesRegex(ValueError,'cleanup is unresolved'):owner.evaluate(release)
        self.assertEqual(self.authority.unresolved_evaluations,1);self.assertEqual(self.free_slots(),1)

    def test_wall_expiry_after_wait_stops_before_evaluator_and_releases_unstarted_slot(self):
        owner,release=self.owner();owner._tick.side_effect=[None,study.StudyStop('wall time exhausted')]
        with self.assertRaisesRegex(ValueError,'wall time'):owner.evaluate(release)
        owner.evaluator.assert_not_called();self.assertEqual(self.free_slots(),2)
        self.assertEqual(self.authority.unresolved_evaluations,0)

    def test_runtime_finance_substitution_refused(self):
        owner,release=self.owner();owner._shared_evaluator_finance=object()
        with self.assertRaisesRegex(ValueError,'original shared financial'):owner.evaluate(release)
        owner.evaluator.assert_not_called();self.assertEqual(self.free_slots(),2)

    def test_prospective_policy_requires_exact_contract_and_original_clock(self):
        f=_ClockFixture('test_original_horizon_is_sampled_once_and_never_rebased');f.setUp();self.addCleanup(f.doCleanups)
        def changed(rt):
            resource={k:getattr(f.plan,k) for k in ('horizon_seconds','source_generations','partition_seconds','executor_slots')}
            resource['runtime']=rt
            return replace(f.plan,runtime=rt,cohort=replace(f.plan.cohort,resource_contract_sha256=study.digest(resource)))
        good={**f.plan.runtime,EVALUATOR_CAPACITY_KEY:dict(EVALUATOR_CAPACITY_POLICY)}
        changed(good)
        for rt in ({**good,EVALUATOR_CAPACITY_KEY:{}},{k:v for k,v in good.items() if k!=clocks.POLICY_KEY}):
            with self.assertRaisesRegex(ValueError,'Exact shared evaluator policy'):changed(rt)
