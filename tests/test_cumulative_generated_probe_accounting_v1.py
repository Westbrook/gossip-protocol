"""Original-bound accounting with inert Git/journals and synthetic finance/mesh."""
from dataclasses import asdict, replace
import json
import time
import unittest

from gossip_harness import cumulative_generated_probe_accounting_v1 as accounting
from gossip_harness import cumulative_child_deadline_v1 as clocks
from gossip_harness import cumulative_generated_probe_context_v1 as contexts
from gossip_harness import cumulative_generated_probe_plan_v1 as plans
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import project_acceptance_registry_v1 as registry
from gossip_harness.gitstore import GitStore
from tests.test_cumulative_child_deadline_v1 import selected_plan
from tests.test_cumulative_generated_probe_plan_v1 import policy
from gossip_harness.candidate_checkpoint_chain_v1 import ChainUnknown
from gossip_harness import cumulative_generated_probe_capacity_v1 as capacity
from gossip_harness import cumulative_generated_probe_corpus_v1 as corpus
from gossip_harness import cumulative_generated_probe_matrix_v1 as matrices
from gossip_harness import cumulative_generated_probe_ranked_originals_v1 as ranked
from gossip_harness import cumulative_generated_probe_values_v2 as values
from gossip_harness import cumulative_study_controller_v2 as study
from tests.ranked_originals_fixture_v1 import OriginalFixture
from tests.test_cumulative_generated_probe_capacity_v1 import budget, assess
from tests.test_cumulative_generated_probe_corpus_v1 import offered, probe_plan


class GeneratedProbeOriginalAccountingGitTests(unittest.TestCase):
    quotas = corpus.Quotas(2, 8)

    def fixture(self, *, before=True, limits=None, pinned=True, builds_only=False, late_before=False, original_clock=False):
        holder = {}
        def install(f):
            holder['fixture'] = f
            corpus.install_contract(f.owner, expected=f.chain.commitment, quotas=self.quotas, limits=f.limits)
            domain = 'original-accounting-fixture'
            if original_clock:
                f.owner.records.put('contract', f.plan.record())
                f.owner.records.put('child.'+f.owner.trajectory.id+'.begin', clocks.begin(f.plan,f.index,time.time))
                clock = clocks.read(f.owner.records,f.plan,f.index,active=True)
                f.owner.original_child_clock=clock;f.owner._original_child_clock_identity=study.digest(asdict(clock))
                f.owner.deadline=clock.deadline_unix;f.owner.clock=time.time;domain=clock.clock_domain
            f.book = capacity.ReservationLedger(f.owner.records, budget() if limits is None else limits,
                clock_domain=domain, expected=f.chain.commitment, create=True)
            now = time.monotonic_ns()
            f.window = (f.owner.probe_capacity_window(review_ns=0,selection_ns=0) if original_clock
                else capacity.Window(domain,now,now+600_000_000_000,0,0))

        def freeze(f):
            ranked.freeze_build_phase(f.owner, expected=f.chain.commitment,
                milestone=f.milestone, generation=f.generation, limits=f.limits)
            compiled, _ = ranked.reconstruct_frozen_builds(f.owner, expected=f.chain.commitment,
                milestone=f.milestone, generation=f.generation, limits=f.limits)
            number = study.MILESTONES.index(f.milestone)
            f.before_matrix = matrices.compile_matrix(compiled, current=f.plan.releases[number],
                inherited=f.plan.releases[number-1], active_probes=())
            f.before_raw = self.raw(f, f.before_matrix, 'before-review')
            if before:
                f.before_receipt = self.reserve(f, f.before_raw, 'before-review')

        def plan_change(plan):
            plan = probe_plan(plan)
            if original_clock:plan = selected_plan(plan)
            return replace(plan, source_pins={**plan.source_pins, **accounting.sources()}) if pinned else plan

        review_count = 0
        def during_directive(directive):
            nonlocal review_count
            f = holder['fixture']
            if directive.kind == 'review':
                review_count += 1
            if late_before and directive.kind == 'review' and review_count == 2:
                # The first review directive is durably recorded; insert before
                # the second directive and the eventual complete review roster.
                f.late_receipt = f.book.reserve(f.before_raw, expected=f.chain.commitment)
            return directive

        f = OriginalFixture(before_contract=install, at_build_boundary=freeze, plan_change=plan_change,
            review_change=lambda d: d.update(probes=[offered()]), builds_only=builds_only,
            directive_change=during_directive)
        self.addCleanup(f.close)
        if late_before:
            self.assertEqual(f.late_receipt['status'], 'reserved_declaration')
            freeze_position = f.chain.position(f.owner.records.name(f.stage + '.before-review-freeze'))
            allocation_position = f.chain.position(f.owner.records.name(f.late_receipt['slot']))
            self.assertGreater(allocation_position, freeze_position + 1)
        return f

    @staticmethod
    def raw(f, matrix, phase):
        cells = matrix.base_cells if phase == 'before-review' else (*matrix.contextual_cells, *matrix.merged_cells)
        charges = tuple(capacity.Charge(c.sha256, 100, 20, 30, 10, 5) for c in cells)
        return values.canonical(capacity.declaration(matrix, charges, f.window, phase=phase))

    def reserve(self, f, raw, phase, **changes):
        args = {'expected': f.chain.commitment, 'milestone': f.milestone, 'generation': f.generation,
            'phase': phase, 'quotas': self.quotas, 'limits': f.limits}
        args.update(changes)
        return accounting.reserve(f.owner, f.book, raw, **args)

    def inspect(self, f, slot, phase):
        return accounting.inspect(f.owner, f.book, slot, expected=f.chain.commitment,
            milestone=f.milestone, generation=f.generation, phase=phase, quotas=self.quotas, limits=f.limits)

    def after(self, f):
        corpus.enroll(f.owner, expected=f.chain.commitment, milestone=f.milestone,
            generation=f.generation, quotas=self.quotas, limits=f.limits)
        _, matrix, _ = corpus.read(f.owner, expected=f.chain.commitment,
            milestone=f.milestone, generation=f.generation, quotas=self.quotas, limits=f.limits)
        return self.raw(f, matrix, 'after-review')

    @staticmethod
    def shrink(raw, phase):
        body = json.loads(raw)
        cells = body['matrix']['cells'][phase]
        removed = cells.pop()['cell_sha256']
        body['charges'] = [c for c in body['charges'] if c['cell_sha256'] != removed]
        body['matrix']['execution_cell_counts'][phase] -= 1
        body['matrix']['execution_cell_counts']['total'] -= 1
        body['matrix_sha256'] = study.digest(body['matrix'])
        return values.canonical(body)

    def test_both_phases_bind_full_original_census_and_cold_inspection(self):
        f = self.fixture()
        after = self.reserve(f, self.after(f), 'after-review')
        self.assertEqual(f.book.snapshot()['used']['execution_cells'], 13)
        for phase, receipt in [('before-review', f.before_receipt), ('after-review', after)]:
            original = self.inspect(f, receipt['decision']['slot'], phase)
            self.assertTrue(original['original_matrix_bound'])
            self.assertEqual(original['original'], receipt['original'])
            for key in ('executor_leases_issued', 'whole_child_deadline_bound', 'dispatch_authority', 'acceptance_authority'):
                self.assertFalse(original[key])

    def test_self_consistent_omitted_candidate_passes_generic_math_but_not_original_binding(self):
        f = self.fixture(before=False, builds_only=True)
        raw = self.shrink(f.before_raw, 'before_review')
        self.assertEqual(assess(raw)['status'], 'reserved_declaration')
        expected = f.chain.commitment
        with self.assertRaisesRegex(ValueError, 'capacity_matrix_differs'):
            self.reserve(f, raw, 'before-review')
        self.assertEqual(f.chain.commitment, expected)
        self.assertEqual(f.book.snapshot()['entries'], [])

    def test_post_review_cannot_drop_an_applicable_generated_probe(self):
        f = self.fixture()
        raw = self.shrink(self.after(f), 'merged')
        self.assertEqual(assess(raw)['status'], 'reserved_declaration')
        expected = f.chain.commitment
        with self.assertRaisesRegex(ValueError, 'capacity_matrix_differs'):
            self.reserve(f, raw, 'after-review')
        self.assertEqual(f.chain.commitment, expected)

    def test_cold_inspection_rejects_generic_unbound_allocation(self):
        f = self.fixture(before=False, builds_only=True)
        row = f.book.reserve(self.shrink(f.before_raw, 'before_review'), expected=f.chain.commitment)
        self.assertEqual(row['status'], 'reserved_declaration')
        with self.assertRaisesRegex(ValueError, 'capacity_matrix_differs'):
            self.inspect(f, row['slot'], 'before-review')

    def test_late_generic_allocation_cannot_claim_original_enrollment_boundary(self):
        f = self.fixture(before=False, builds_only=True)
        f.owner.records.put('unrelated-boundary', {'value': 1})
        with self.assertRaisesRegex(ValueError, 'immediately_follow'):
            self.reserve(f, f.before_raw, 'before-review')
        row = f.book.reserve(f.before_raw, expected=f.chain.commitment)
        with self.assertRaisesRegex(ValueError, 'not_at_enrollment_boundary'):
            self.inspect(f, row['slot'], 'before-review')

    def test_post_review_requires_complete_successful_before_review_accounting(self):
        f = self.fixture(before=False)
        raw = self.after(f)
        expected = f.chain.commitment
        with self.assertRaisesRegex(ValueError, 'prior_pre_review_reservation'):
            self.reserve(f, raw, 'after-review')
        self.assertEqual(f.chain.commitment, expected)

    def test_post_review_rejects_matching_but_late_generic_before_review_allocation(self):
        f = self.fixture(before=False, late_before=True)
        raw = self.after(f)
        expected = f.chain.commitment
        with self.assertRaisesRegex(ValueError, 'not_at_enrollment_boundary'):
            self.reserve(f, raw, 'after-review')
        self.assertEqual(f.chain.commitment, expected)

    def test_cold_post_review_inspection_also_rejects_late_pre_review_allocation(self):
        f = self.fixture(before=False, late_before=True)
        raw = self.after(f)
        receipt = f.book.reserve(raw, expected=f.chain.commitment)
        with self.assertRaisesRegex(ValueError, 'not_at_enrollment_boundary'):
            self.inspect(f, receipt['slot'], 'after-review')

    def test_post_review_reservation_cannot_follow_accepted_source_movement(self):
        f = self.fixture()
        raw = self.after(f)
        old = f.owner.protected.head()
        new = f.owner.protected.propose({'base.py': '# next accepted source\n'}, old)
        f.owner.protected._git('update-ref', 'refs/heads/accepted', new, old)
        expected = f.chain.commitment
        with self.assertRaisesRegex(ValueError, 'accepted_source_moved'):
            self.reserve(f, raw, 'after-review')
        self.assertEqual(f.chain.commitment, expected)

    def test_decline_is_original_bound_and_cannot_be_retried_smaller(self):
        f = self.fixture(limits=budget(execution_cells=1), builds_only=True)
        receipt = f.before_receipt
        self.assertEqual(receipt['decision']['status'], 'declined')
        self.assertEqual(len(receipt['decision']['schedule']), 4)
        self.assertEqual(self.inspect(f, receipt['decision']['slot'], 'before-review')['decision']['status'], 'declined')
        expected = f.chain.commitment
        with self.assertRaises(ValueError):
            self.reserve(f, f.before_raw, 'before-review')
        self.assertEqual(f.chain.commitment, expected)

    def test_prior_financial_original_is_rechecked_after_accounting(self):
        f = self.fixture(builds_only=True)
        value = next(iter(f.proofs.values()))
        value['result_payload'] = {'kind': 'failure', 'payload': {}}
        with self.assertRaisesRegex(ValueError, 'finance request/result'):
            self.inspect(f, f.before_receipt['decision']['slot'], 'before-review')

    def test_source_advance_keeps_historical_inspection_bound_to_old_base(self):
        f = self.fixture(builds_only=True)
        old = f.owner.protected.head()
        new = f.owner.protected.propose({'base.py': '# accepted descendant\n'}, old)
        f.owner.protected._git('update-ref', 'refs/heads/accepted', new, old)
        result = self.inspect(f, f.before_receipt['decision']['slot'], 'before-review')
        self.assertTrue(result['original_matrix_bound'])
        self.assertEqual(f.owner.protected.head(), new)

    def test_prospective_source_pin_is_required_before_any_charge(self):
        f = self.fixture(before=False, pinned=False, builds_only=True)
        expected = f.chain.commitment
        with self.assertRaisesRegex(ValueError, 'not_prospectively_pinned'):
            self.reserve(f, f.before_raw, 'before-review')
        self.assertEqual(f.chain.commitment, expected)

    def test_foreign_journal_identity_and_stale_checkpoint_are_refused(self):
        f = self.fixture(before=False, builds_only=True)
        expected = f.chain.commitment
        f.book.records = study.Records(f.chain)
        with self.assertRaisesRegex(ValueError, 'same_original_controller'):
            self.reserve(f, f.before_raw, 'before-review')
        f.book.records = f.owner.records
        f.owner.records.put('later', {'value': 1})
        with self.assertRaises(ValueError):
            self.reserve(f, f.before_raw, 'before-review', expected=expected)
        # The actual journal intentionally latches uncertainty on a stale
        # boundary; no later ordinary read or new accounting is permitted.
        with self.assertRaises(ChainUnknown):
            f.book.snapshot()
        self.assertFalse((f.chain.raw_root / f.owner.records.name(f.book._slot(0))).exists())

    def test_wrong_generation_phase_and_missing_slot_do_not_grant_original_binding(self):
        f = self.fixture(builds_only=True)
        with self.assertRaises(ValueError):
            self.inspect(f, f.before_receipt['decision']['slot'], 'after-review')
        with self.assertRaisesRegex(ValueError, 'original_capacity_slot_missing'):
            self.inspect(f, 'generated-probe.capacity-request.19', 'before-review')
        with self.assertRaises(ValueError):
            self.reserve(f, f.before_raw, 'before-review', generation=1)


    def cell_fixture(self, *, phase='contextual', original_clock=True, limits=None):
        f=self.fixture(original_clock=original_clock,limits=limits)
        receipt=self.reserve(f,self.after(f),'after-review')
        active,matrix,_=corpus.read(f.owner,expected=f.chain.commitment,milestone=f.milestone,
            generation=f.generation,quotas=self.quotas,limits=f.limits)
        cells=matrix.contextual_cells if phase=='contextual' else matrix.merged_cells
        cell=next(c for c in cells if c.kind=='generated-history')
        gen,_=ranked.reconstruct_frozen(f.owner,expected=f.chain.commitment,milestone=f.milestone,
            generation=f.generation,limits=f.limits)
        selected=gen.anchor_vector if phase=='merged' else None
        context=contexts.compose(gen,phase,actor=cell.actor,selected=selected)
        commit=f.owner.protected.propose(contexts.delta_from_base(gen,context),gen.base.commit_oid)
        r=context.record()
        subject=registry.Subject(f.owner.child.cohort,f.owner.trajectory.id,f.milestone,f.plan.sha256,
            values.PRODUCT_SHA256,r['source_sha256'])
        target=plans.ProbeTarget(subject,f.generation,cell.actor if phase=='contextual' else 'merged',
            context.sha256,'candidate-context' if phase=='contextual' else 'merged',commit,r['tree_oid'])
        release=f.plan.releases[study.MILESTONES.index(f.milestone)]
        f.cell_store=GitStore.fork(f.owner.protected,f.root/'probe-view.git')
        f.cell_store._git('fetch','--no-tags',str(f.owner.protected.path),commit+':refs/heads/accepted')
        plan=plans.prepare_plan(f.cell_store,target,active[0].read(release),release,policy())
        return f,receipt['decision']['slot'],cell,plan,selected

    def inspect_cell(self,f,slot,cell,plan,selected=None,**changes):
        args={'candidate_store':f.cell_store,'expected':f.chain.commitment,'milestone':f.milestone,'generation':f.generation,
            'quotas':self.quotas,'limits':f.limits,'selected':selected};args.update(changes)
        return accounting.inspect_probe_cell(f.owner,f.book,slot,cell.sha256,plan,**args)

    def test_reserved_context_cell_binds_original_probe_complete_matrix_and_actual_git(self):
        f,slot,cell,plan,selected=self.cell_fixture();before=f.chain.commitment
        result=self.inspect_cell(f,slot,cell,plan,selected)
        self.assertTrue(result['original_source_bound']);self.assertTrue(result['original_matrix_bound'])
        self.assertEqual(result['materialized_source']['commit_oid'],plan.target.commit_oid)
        self.assertEqual(result['declared_charge']['cell_sha256'],cell.sha256)
        self.assertEqual(len(result['reservation']['decision']['schedule']),9)
        for key in ('unique_execution_intent_retained','qualified_resource_envelope','selection_authority',
                    'dispatch_authority','acceptance_authority'):self.assertFalse(result[key])
        self.assertEqual(f.chain.commitment,before)

    def test_merged_cell_requires_complete_endorsed_vector_but_does_not_grant_selection(self):
        f,slot,cell,plan,selected=self.cell_fixture(phase='merged')
        result=self.inspect_cell(f,slot,cell,plan,selected)
        self.assertFalse(result['selection_authority']);self.assertEqual(result['context']['vector'],list(selected))
        with self.assertRaisesRegex(ValueError,'complete_simultaneous_choice_vector'):
            self.inspect_cell(f,slot,cell,plan)
        with self.assertRaisesRegex(ValueError,'merged_choice_not_endorsed'):
            self.inspect_cell(f,slot,cell,plan,('unendorsed',*selected[1:]))

    def test_wrong_cell_kind_or_identity_cannot_borrow_successful_reservation(self):
        f,slot,cell,plan,_=self.cell_fixture()
        raw=f.owner.records.read(slot)['request']['matrix']['cells']['contextual']
        public=next(c for c in raw if c['kind']=='authored-public-suite')
        args={'candidate_store':f.cell_store,'expected':f.chain.commitment,'milestone':f.milestone,'generation':f.generation,
            'quotas':self.quotas,'limits':f.limits}
        for identifier,error in ((public['cell_sha256'],'generated_probe_cell_required'),('0'*64,'exact_reserved_probe_cell')):
            with self.assertRaisesRegex(ValueError,error):
                accounting.inspect_probe_cell(f.owner,f.book,slot,identifier,plan,**args)

    def test_matching_decline_and_unbound_child_window_cannot_supply_probe_cell(self):
        f,slot,cell,plan,_=self.cell_fixture(limits=budget(execution_cells=4))
        with self.assertRaisesRegex(ValueError,'successful_probe_reservation'):
            self.inspect_cell(f,slot,cell,plan)
        f,slot,cell,plan,_=self.cell_fixture(original_clock=False)
        with self.assertRaisesRegex(ValueError,'original_child_bound_probe_reservation'):
            self.inspect_cell(f,slot,cell,plan)

    def test_cell_rejects_changed_actor_context_stage_and_generation(self):
        f,slot,cell,plan,_=self.cell_fixture()
        for change in ({'candidate_id':'other'},{'context_id':'other'},{'stage':'merged'},{'generation':1}):
            with self.subTest(change=change),self.assertRaises(ValueError):
                self.inspect_cell(f,slot,cell,replace(plan,target=replace(plan.target,**change)))
        with self.assertRaisesRegex(ValueError,'candidate_context_differs'):
            self.inspect_cell(f,slot,cell,plan,('catalog0','ingestion0','query0','clients0'))

    def test_cell_rejects_self_consistent_wrong_git_source(self):
        f,slot,cell,plan,_=self.cell_fixture()
        commit=f.owner.protected.propose({'base.py':'# altered materialization\n'},plan.target.commit_oid)
        files={k:v.encode() for k,v in f.owner.protected.read_files(commit).items()}
        target=replace(plan.target,commit_oid=commit,tree_oid=f.owner.protected._git('rev-parse',commit+'^{tree}'),
            subject=replace(plan.target.subject,source_sha256=admission.source_sha256(files)))
        changed=replace(plan,target=target)
        f.cell_store._git('fetch','--no-tags',str(f.owner.protected.path),commit+':refs/heads/accepted')
        plans.verify_current_source(f.cell_store,changed)
        with self.assertRaisesRegex(ValueError,'materialized_context_source_differs'):
            self.inspect_cell(f,slot,cell,changed)

    def test_cell_rejects_replaced_probe_definition_and_public_release(self):
        f,slot,cell,plan,_=self.cell_fixture()
        different=values.admit(offered('other text'),released_requirements=('M2-REFRESH',),contract_sha256=values.PRODUCT_SHA256)
        with self.assertRaisesRegex(ValueError,'definition_or_cases_differ'):
            self.inspect_cell(f,slot,cell,replace(plan,admitted_raw=values.canonical(different)))
        release=json.loads(plan.release_raw);release['instructions']='Different public instructions'
        with self.assertRaisesRegex(ValueError,'reserved_probe_public_release_differs'):
            self.inspect_cell(f,slot,cell,replace(plan,release_raw=values.canonical(release)))

    def test_cell_rechecks_original_financial_materialization_and_writes_no_intent(self):
        f,slot,cell,plan,_=self.cell_fixture();before=f.chain.commitment
        self.inspect_cell(f,slot,cell,plan)
        value=next(iter(f.proofs.values()));value['result_payload']={'kind':'failure','payload':{}}
        with self.assertRaisesRegex(ValueError,'finance request/result'):
            self.inspect_cell(f,slot,cell,plan)
        self.assertEqual(f.chain.commitment,before)


    def test_candidate_view_must_be_separate_and_its_head_must_match_without_project_promotion(self):
        f,slot,cell,plan,_=self.cell_fixture();accepted=f.owner.protected.head();before=f.chain.commitment
        self.assertNotEqual(accepted,plan.target.commit_oid)
        with self.assertRaisesRegex(ValueError,'separate_probe_candidate_view'):
            self.inspect_cell(f,slot,cell,plan,candidate_store=f.owner.protected)
        f.cell_store._git('update-ref','refs/heads/accepted',accepted,plan.target.commit_oid)
        with self.assertRaisesRegex(ValueError,'registered_head_changed'):
            self.inspect_cell(f,slot,cell,plan)
        self.assertEqual(f.owner.protected.head(),accepted);self.assertEqual(f.chain.commitment,before)
