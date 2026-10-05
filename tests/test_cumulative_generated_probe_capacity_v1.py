"""Declared envelopes and real anchored accounting; no candidate/provider runs."""
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import time
import unittest

from gossip_harness import candidate_checkpoint_chain_v1 as chain
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from gossip_harness import cumulative_generated_probe_capacity_v1 as capacity
from gossip_harness import cumulative_generated_probe_values_v2 as values
from gossip_harness import cumulative_study_controller_v2 as study
from tests.test_cumulative_generated_probe_matrix_v1 import compile_matrix
from tests.test_cumulative_generated_probe_context_v1 import fixture


def budget(**changes):
    return capacity.Budget(**{'retained_raw_bytes':100000,'journal_delta_bytes':100000,
        'workspace_bytes':100000,'execution_cells':1000,'max_requests':20,**changes})


def request(plan=None, *, phase='before-review', window=None, active_ns=10, cleanup_ns=5):
    plan=compile_matrix(arm='S4-G',active=()) if plan is None else plan
    cells=plan.base_cells if phase=='before-review' else (*plan.contextual_cells,*plan.merged_cells)
    charges=tuple(capacity.Charge(c.sha256,100,20,30,active_ns,cleanup_ns) for c in cells)
    window=capacity.Window('test-clock',100,10000,0,0) if window is None else window
    return values.canonical(capacity.declaration(plan,charges,window,phase=phase))


def assess(raw, limits=None, **changes):
    args={'used':{k:0 for k in ('retained_raw_bytes','journal_delta_bytes','workspace_bytes','execution_cells')},
          'busy_until':(0,0,0,0),'now_ns':100};args.update(changes)
    return capacity.assess(raw,budget() if limits is None else limits,**args)


class GeneratedProbeCapacityPlanningTests(unittest.TestCase):
    def test_small_and_large_phases_keep_every_logical_cell(self):
        for arm,before,after in [('S4-G',4,20),('S16-G',16,65),('O16-G',16,65)]:
            with self.subTest(arm=arm):
                plan=compile_matrix(arm=arm)
                pre=assess(request(plan));post=assess(request(plan,phase='after-review'))
                self.assertEqual(pre['charges']['execution_cells'],before)
                self.assertEqual(post['charges']['execution_cells'],after)
                self.assertEqual(len(pre['schedule'])+len(post['schedule']),before+after)

    def test_omitted_reordered_duplicate_or_extra_charges_are_rejected(self):
        plan=compile_matrix(arm='S4-G',active=());raw=request(plan);body=json.loads(raw)
        charges=tuple(capacity.Charge(**c) for c in body['charges']);window=capacity.Window(**body['window'])
        for bad in (charges[:-1],tuple(reversed(charges)),(*charges,charges[0])):
            with self.subTest(count=len(bad)),self.assertRaisesRegex(ValueError,'complete_ordered_unique'):
                capacity.declaration(plan,bad,window,phase='before-review')

    def test_each_aggregate_limit_declines_the_whole_unchanged_matrix(self):
        raw=request(phase='after-review')
        for field in ('retained_raw_bytes','journal_delta_bytes','workspace_bytes','execution_cells'):
            with self.subTest(field=field):
                result=assess(raw,budget(**{field:1}))
                self.assertEqual(result['status'],'declined');self.assertIn(field+'_capacity',result['reasons'])
                self.assertEqual(len(result['schedule']),5)

    def test_cleanup_review_and_selection_barriers_use_real_slot_time(self):
        pre=assess(request(window=capacity.Window('test-clock',100,1000,20,0)))
        self.assertEqual([r['slot'] for r in pre['schedule']],[0,1,2,3])
        self.assertEqual(pre['proposed_busy_until'],[135]*4)
        post=assess(request(phase='after-review',window=capacity.Window('test-clock',100,1000,0,7)),
                    used=pre['charges'],busy_until=tuple(pre['proposed_busy_until']))
        self.assertEqual([r['started_ns'] for r in post['schedule']],[135,135,135,135,157])
        self.assertEqual(post['schedule'][-1]['released_ns'],172)

    def test_other_reservations_are_never_overlapped_and_review_waits_for_all_slots(self):
        result=assess(request(window=capacity.Window('test-clock',100,1000,20,0)),busy_until=(200,0,0,0))
        self.assertEqual([r['slot'] for r in result['schedule']],[1,2,3,1])
        self.assertEqual(result['review_barrier']['started_ns'],200)
        self.assertEqual(result['proposed_busy_until'],[220]*4)

    def test_expiry_and_full_schedule_overrun_never_erase_unattempted_cells(self):
        raw=request(phase='after-review',window=capacity.Window('test-clock',100,120,0,0))
        result=assess(raw);self.assertIn('complete_schedule_exceeds_deadline',result['reasons'])
        self.assertEqual(len(result['schedule']),5)
        result=assess(raw,now_ns=120);self.assertIn('absolute_deadline_expired',result['reasons'])

    def test_pre_review_identity_does_not_depend_on_later_rankings(self):
        gen=replace(fixture('S4-G'),milestone='M4')
        unknown=replace(gen,rankings=tuple(replace(r,actors=(),disposition='unknown') for r in gen.rankings))
        early=request(compile_matrix(generation=unknown,active=()))
        later=request(compile_matrix(generation=gen,active=()),phase='after-review')
        self.assertEqual(json.loads(early)['generation_id'],json.loads(later)['generation_id'])
        self.assertEqual(assess(early)['status'],'reserved_declaration')
        blocked=assess(request(compile_matrix(generation=unknown),phase='after-review'))
        self.assertIn('peer_anchor_unavailable',blocked['reasons'])
        self.assertEqual(len(blocked['schedule']),20)

    def test_unqualified_envelopes_never_become_measurements_or_dispatch_authority(self):
        result=assess(request())
        for key in ('raw_bounds_are_measured','executor_leases_issued','dispatch_authority','acceptance_authority'):
            self.assertIs(result[key],False)
        self.assertIn('not_qualified',json.loads(request())['qualification_status'])

    def test_exact_integer_and_existing_four_slot_contract_is_enforced(self):
        for change in ({'executor_slots':5},{'max_requests':109},{'execution_cells':True},{'workspace_bytes':-1}):
            with self.subTest(change=change),self.assertRaises(ValueError):budget(**change)
        with self.assertRaises(ValueError):capacity.Charge('a'*64,1,1,1,True,1)
        with self.assertRaises(ValueError):assess(request(),busy_until=(0,0,0))
        with self.assertRaises(ValueError):assess(request(),used={})

    def test_phase_overheads_cannot_add_another_review_or_selection(self):
        with self.assertRaisesRegex(ValueError,'phase_overhead_scope'):
            assess(request(phase='after-review',window=capacity.Window('test-clock',100,1000,1,0)))
        with self.assertRaisesRegex(ValueError,'phase_overhead_scope'):
            assess(request(window=capacity.Window('test-clock',100,1000,0,1)))

    def test_mutated_matrix_or_charge_snapshot_is_not_accepted(self):
        body=json.loads(request());body['matrix']['cells']['before_review'].pop()
        with self.assertRaisesRegex(ValueError,'matrix_changed'):assess(values.canonical(body))
        body=json.loads(request());body['charges'].pop()
        with self.assertRaisesRegex(ValueError,'complete_ordered_unique'):assess(values.canonical(body))

    def test_same_declared_inputs_have_deterministic_schedule_and_limits(self):
        self.assertEqual(assess(request(phase='after-review')),assess(request(phase='after-review')))


    def test_unbounded_or_deep_request_is_rejected_before_accounting(self):
        with self.assertRaisesRegex(ValueError,'bounded_immutable_request'):
            assess(b' '*(capacity.MAX_REQUEST_BYTES+1))
        with self.assertRaises(ValueError):assess(b'['*100+b'0'+b']'*100)



class GeneratedProbeCapacityLedgerTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='probe-capacity-');self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name).resolve();raw=self.root/'raw';delta=self.root/'delta'
        self.head=ExternalHead.create(self.root/'head',journal_roots=(raw,delta));self.addCleanup(self.head.close)
        self.chain=chain.CheckpointChain.create(raw,delta,context={'capacity_fixture':True},authority=self.head)
        self.addCleanup(self.chain.close);self.records=study.Records(self.chain)
        now=time.monotonic_ns();self.window=capacity.Window('test-clock',now,now+60_000_000_000,0,0)

    def book(self, limits=None, *, create=True):
        return capacity.ReservationLedger(self.records,budget() if limits is None else limits,
            clock_domain='test-clock',expected=self.chain.commitment,create=create)

    def raw(self, phase='before-review', **changes):
        return request(phase=phase,window=self.window,**changes)

    def reserve(self, book, raw):
        return book.reserve(raw,expected=self.chain.commitment)

    def test_both_phases_charge_once_and_new_owner_replays_originals(self):
        book=self.book();self.reserve(book,self.raw());self.reserve(book,self.raw('after-review'))
        state=book.snapshot();self.assertEqual(state['used']['execution_cells'],9)
        self.assertEqual(state['used']['retained_raw_bytes'],900)
        other=self.book(create=False);self.assertEqual(other.snapshot(),state)
        with self.assertRaisesRegex(ValueError,'already_attempted'):self.reserve(other,self.raw())
        self.assertFalse(state['executor_leases_issued'])

    def test_post_review_cannot_skip_pre_review_enrollment(self):
        book=self.book();before=self.chain.commitment
        with self.assertRaisesRegex(ValueError,'prior_pre_review'):self.reserve(book,self.raw('after-review'))
        self.assertEqual(self.chain.commitment,before)

    def test_cumulative_overrun_retains_decline_without_refund_or_partial_allocation(self):
        book=self.book(budget(execution_cells=8));self.reserve(book,self.raw())
        result=self.reserve(book,self.raw('after-review'))
        self.assertEqual(result['status'],'declined');state=book.snapshot()
        self.assertEqual(state['used']['execution_cells'],4);self.assertEqual(len(state['entries']),2)
        self.assertEqual(len(result['schedule']),5)
        with self.assertRaisesRegex(ValueError,'already_attempted'):self.reserve(book,self.raw('after-review'))

    def test_absolute_window_cannot_be_reset_between_phases(self):
        book=self.book();self.reserve(book,self.raw())
        changed=request(phase='after-review',window=replace(self.window,deadline_ns=self.window.deadline_ns+1))
        with self.assertRaisesRegex(ValueError,'absolute_generation_window_changed'):self.reserve(book,changed)

    def test_controller_accounting_contract_must_be_prospective(self):
        self.records.put('generated-probe.ranking-contract',{'already':'started'})
        with self.assertRaisesRegex(ValueError,'must_precede_ranked_work'):self.book()
        self.assertIsNone(self.records.read(capacity.CONFIG_SLOT))

    def test_budget_replacement_cannot_increase_existing_allowance(self):
        book=self.book();book.budget=budget(execution_cells=2000)
        with self.assertRaisesRegex(ValueError,'owner_configuration_changed'):book.snapshot()
        with self.assertRaisesRegex(ValueError,'original_capacity_contract_differs'):
            self.book(budget(execution_cells=2000),create=False)

    def test_cold_replay_rejects_a_well_formed_but_wrong_accounting_decision(self):
        book=self.book();raw=self.raw();decision=assess(raw,now_ns=time.monotonic_ns())
        decision['charges']['retained_raw_bytes']=0
        self.records.put(book._slot(0),{'request':json.loads(raw),'decision':decision})
        with self.assertRaisesRegex(ValueError,'original_capacity_decision_differs'):self.book(create=False)

    def test_history_gap_cannot_hide_a_reservation(self):
        book=self.book();raw=self.raw();decision=assess(raw,now_ns=time.monotonic_ns())
        self.records.put(book._slot(1),{'request':json.loads(raw),'decision':decision})
        with self.assertRaisesRegex(ValueError,'history_gap'):book.snapshot()

    def test_stale_independent_checkpoint_is_not_a_new_allocation_permit(self):
        book=self.book();old=self.chain.commitment;self.reserve(book,self.raw())
        with self.assertRaises(ValueError):book.reserve(self.raw('after-review'),expected=old)

    def test_request_limit_is_not_reset_by_opening_another_owner(self):
        limits=budget(max_requests=1);book=self.book(limits);self.reserve(book,self.raw())
        other=self.book(limits,create=False)
        with self.assertRaisesRegex(ValueError,'request_limit'):self.reserve(other,self.raw('after-review'))


    def test_cold_replay_refuses_a_capacity_contract_recorded_after_ranked_work(self):
        self.records.put('generated-probe.ranking-contract',{'already':'started'})
        from dataclasses import asdict
        self.records.put(capacity.CONFIG_SLOT,{'protocol':capacity.PROTOCOL,'budget':asdict(budget()),
            'clock_domain':'test-clock','source_sha256':capacity.LOADED_SOURCE_SHA256,'execution_authority':False})
        with self.assertRaisesRegex(ValueError,'capacity_contract_must_precede_ranked_work'):
            self.book(create=False)
