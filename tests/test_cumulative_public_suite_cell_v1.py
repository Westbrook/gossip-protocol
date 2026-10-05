"""Original suite/source bindings; inert Git, synthetic financial/mesh originals."""
from dataclasses import replace
import sys
import unittest

from gossip_harness import cumulative_public_suite_cell_v1 as public
from gossip_harness import cumulative_generated_probe_context_v1 as contexts
from gossip_harness import cumulative_generated_probe_accounting_v1 as accounting
from gossip_harness.gitstore import GitStore
from tests import test_cumulative_generated_probe_accounting_v1 as fixtures


class PublicSuiteCellOriginalGitTests(unittest.TestCase):
    def fixture(self,phase='contextual',*,original_clock=True,limits=None):
        helper=fixtures.GeneratedProbeOriginalAccountingGitTests()
        self.addCleanup(helper.doCleanups)
        before=phase=='before-review'
        f=helper.fixture(original_clock=original_clock,builds_only=before,limits=limits)
        if before:
            slot=f.before_receipt['decision']['slot'];matrix=f.before_matrix
            gen,_=accounting.ranked.reconstruct_frozen_builds(f.owner,expected=f.chain.commitment,
                milestone=f.milestone,generation=f.generation,limits=f.limits)
            cells=matrix.base_cells
        else:
            slot=helper.reserve(f,helper.after(f),'after-review')['decision']['slot']
            _,matrix,_=accounting.corpus.read(f.owner,expected=f.chain.commitment,
                milestone=f.milestone,generation=f.generation,quotas=helper.quotas,limits=f.limits)
            gen,_=accounting.ranked.reconstruct_frozen(f.owner,expected=f.chain.commitment,
                milestone=f.milestone,generation=f.generation,limits=f.limits)
            cells=matrix.contextual_cells if phase=='contextual' else matrix.merged_cells
        cell=next(c for c in cells if c.kind=='authored-public-suite')
        selected=gen.anchor_vector if phase=='merged' else None
        context=contexts.compose(gen,'base-overlay' if before else phase,actor=cell.actor,selected=selected)
        commit=f.owner.protected.propose(contexts.delta_from_base(gen,context),gen.base.commit_oid)
        store=GitStore.fork(f.owner.protected,f.root/'public-view.git')
        store._git('fetch','--no-tags',str(f.owner.protected.path),commit+':refs/heads/accepted')
        f.matrix=matrix;f.cell=cell;f.compiled=gen
        target=public.PublicSuiteCell(f.owner,f.book,slot,cell.sha256,store,commit,
            f.milestone,f.generation,helper.quotas,f.limits,selected)
        return f,target

    def test_before_review_binds_full_inherited_suite_and_base_overlay_without_future_reviews(self):
        f,target=self.fixture('before-review');checkpoint=f.chain.commitment;accepted=f.owner.protected.head()
        result=target.inspect(expected=checkpoint)
        self.assertEqual(result['release_sha256'],f.plan.releases[0].sha256)
        self.assertEqual(result['release']['milestone'],'M1')
        self.assertEqual(result['context']['phase'],'base-overlay')
        self.assertEqual(result['cell']['use'],'inform-review-only-no-unconditional-veto')
        self.assertIsNone(f.owner.records.read(f.stage+'.reviews'))
        self.assertEqual(f.chain.commitment,checkpoint);self.assertEqual(f.owner.protected.head(),accepted)
        for k in ('qualified_resource_envelope','dispatch_authority','selection_authority','acceptance_authority'):
            self.assertFalse(result[k])

    def test_contextual_suite_is_current_and_bound_to_actual_frozen_peer_source(self):
        f,target=self.fixture();result=target.inspect(expected=f.chain.commitment)
        self.assertEqual(result['release_sha256'],f.plan.releases[1].sha256)
        self.assertEqual(result['release']['checks'],f.plan.releases[1].checks)
        self.assertEqual(result['release']['ordered_check_ids'],list(f.plan.releases[1].ordered_check_ids))
        self.assertEqual(result['context']['vector'],list(f.compiled.anchor_vector))
        self.assertEqual(result['materialized_source']['commit_oid'],target.commit_oid)
        self.assertTrue(result['fresh_execution_required']);self.assertFalse(result['execution_reuse_authority'])

    def test_merged_suite_requires_its_own_endorsed_construction_vector(self):
        f,target=self.fixture('merged');result=target.inspect(expected=f.chain.commitment)
        self.assertEqual(result['context']['phase'],'merged')
        self.assertEqual(result['context']['vector'],list(f.compiled.anchor_vector))
        self.assertFalse(result['selection_authority'])
        with self.assertRaisesRegex(ValueError,'complete_simultaneous_choice_vector'):
            replace(target,selected=None).inspect(expected=f.chain.commitment)
        with self.assertRaisesRegex(ValueError,'merged_choice_not_endorsed'):
            replace(target,selected=('foreign',*target.selected[1:])).inspect(expected=f.chain.commitment)

    def test_generated_or_foreign_cell_cannot_borrow_public_suite_binding(self):
        f,target=self.fixture()
        generated=next(c for c in f.matrix.contextual_cells if c.kind=='generated-history')
        for identifier in (generated.sha256,'a'*64):
            with self.assertRaisesRegex(ValueError,'exact_reserved_authored_public_cell'):
                replace(target,cell_sha256=identifier).inspect(expected=f.chain.commitment)

    def test_candidate_head_and_full_source_must_match_the_original_context(self):
        f,target=self.fixture();old=target.candidate_store.head()
        bad=target.candidate_store.propose({'base.py':'# wrong public source\n'},old)
        target.candidate_store._git('update-ref','refs/heads/accepted',bad,old)
        with self.assertRaisesRegex(ValueError,'public_candidate_head_differs'):
            target.inspect(expected=f.chain.commitment)
        with self.assertRaisesRegex(ValueError,'materialized_context_source_differs'):
            replace(target,commit_oid=bad).inspect(expected=f.chain.commitment)

    def test_accepted_store_cannot_stand_in_for_an_unapproved_candidate_view(self):
        f,target=self.fixture();accepted=f.owner.protected.head()
        with self.assertRaisesRegex(ValueError,'separate_public_candidate_view'):
            replace(target,candidate_store=f.owner.protected,commit_oid=accepted).inspect(expected=f.chain.commitment)
        with self.assertRaisesRegex(ValueError,'contextual_public_cell_has_no_selected_vector'):
            replace(target,selected=f.compiled.anchor_vector).inspect(expected=f.chain.commitment)

    def test_prior_source_is_inspectable_after_promotion_but_cannot_gain_reuse_authority(self):
        f,target=self.fixture();old=f.owner.protected.head()
        new=f.owner.protected.propose({'base.py':'# later accepted source\n'},old)
        f.owner.protected._git('update-ref','refs/heads/accepted',new,old)
        result=target.inspect(expected=f.chain.commitment)
        self.assertEqual(result['materialized_source']['commit_oid'],target.commit_oid)
        self.assertEqual(f.owner.protected.head(),new);self.assertFalse(result['execution_reuse_authority'])

    def test_missing_original_child_clock_cannot_supply_a_public_cell(self):
        f,target=self.fixture(original_clock=False)
        with self.assertRaisesRegex(ValueError,'successful_original_child_public_reservation'):
            target.inspect(expected=f.chain.commitment)

    def test_financial_original_change_during_source_capture_invalidates_the_join(self):
        f,target=self.fixture();seen=[]
        def profile(frame,event,arg):
            if event=='return' and frame.f_code is contexts.verify_materialized.__code__ and not seen:
                seen.append(True)
                next(iter(f.proofs.values()))['result_payload']={'kind':'failure','payload':{}}
        prior=sys.getprofile();sys.setprofile(profile)
        try:
            with self.assertRaisesRegex(ValueError,'finance request/result'):
                target.inspect(expected=f.chain.commitment)
        finally:sys.setprofile(prior)
        self.assertEqual(seen,[True])


if __name__=='__main__':unittest.main()
