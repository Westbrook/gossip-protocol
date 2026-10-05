"""Real original-reader joins over synthetic, explicitly nonphysical evidence."""
from dataclasses import replace
import sys
import unittest

from gossip_harness import cumulative_generated_probe_matrix_reader_v1 as matrix_reader
from gossip_harness import cumulative_generated_probe_accounting_v1 as accounting
from gossip_harness import cumulative_generated_probe_context_v1 as contexts
from gossip_harness import cumulative_generated_probe_plan_v1 as plans
from gossip_harness import cumulative_generated_probe_values_v2 as values
from gossip_harness import project_acceptance_registry_v1 as registry
from gossip_harness.gitstore import GitStore
from tests import test_cumulative_generated_probe_accounting_v1 as helpers
from tests.generated_probe_originals_fixture_v1 import SyntheticOriginals
from tests.test_cumulative_generated_probe_plan_v1 import policy


class GeneratedProbeMatrixReaderGitTests(unittest.TestCase):
    def fixture(self):
        helper=helpers.GeneratedProbeOriginalAccountingGitTests()
        self.addCleanup(helper.doCleanups)
        f=helper.fixture(original_clock=True)
        f.slot=helper.reserve(f,helper.after(f),'after-review')['decision']['slot']
        f.active,f.matrix,_=accounting.corpus.read(f.owner,expected=f.chain.commitment,
            milestone=f.milestone,generation=f.generation,quotas=helper.quotas,limits=f.limits)
        f.compiled,_=accounting.ranked.reconstruct_frozen(f.owner,expected=f.chain.commitment,
            milestone=f.milestone,generation=f.generation,limits=f.limits)
        f.quotas=helper.quotas
        return f

    def observe(self,f,cell,*,defect=False,incomplete=False):
        phase=cell.phase;selected=f.compiled.anchor_vector if phase=='merged' else None
        context=contexts.compose(f.compiled,phase,actor=cell.actor,selected=selected)
        commit=f.owner.protected.propose(contexts.delta_from_base(f.compiled,context),f.compiled.base.commit_oid)
        r=context.record()
        subject=registry.Subject(f.owner.child.cohort,f.owner.trajectory.id,f.milestone,f.plan.sha256,
            values.PRODUCT_SHA256,r['source_sha256'])
        target=plans.ProbeTarget(subject,f.generation,cell.actor if phase=='contextual' else 'merged',
            context.sha256,'candidate-context' if phase=='contextual' else 'merged',commit,r['tree_oid'])
        store=GitStore.fork(f.owner.protected,f.root/('view-'+cell.sha256+'.git'))
        store._git('fetch','--no-tags',str(f.owner.protected.path),commit+':refs/heads/accepted')
        release=f.plan.releases[1]
        probe=next(p.read(release) for p in f.active if values.digest(p.read(release))==cell.definition_sha256)
        plan=plans.prepare_plan(store,target,probe,release,replace(policy(),history_seconds=300))
        enrollment=accounting.ProbeCellEnrollment(f.owner,f.book,f.slot,cell.sha256,
            f.root/('execution-'+cell.sha256),f.milestone,f.generation,f.quotas,f.limits,selected)
        physical=SyntheticOriginals(defect=defect,enrolled=(enrollment,plan,store))
        self.addCleanup(physical.close)
        prefix=physical.retain(drop=('volume-after.json',) if incomplete else ())
        return matrix_reader.OriginalObservation(physical.owner,prefix,enrollment)

    def read(self,f,observations=(),*,phase='contextual',**kw):
        args=dict(expected=f.chain.commitment,milestone=f.milestone,generation=f.generation,
            phase=phase,quotas=f.quotas,limits=f.limits,observations=observations,
            selected=f.compiled.anchor_vector if phase=='merged' else None)
        args.update(kw)
        return matrix_reader.reconstruct(f.owner,f.book,f.slot,**args)

    def cells(self,f):
        return tuple(c for c in f.matrix.contextual_cells if c.kind=='generated-history')

    def test_complete_contextual_census_is_read_only_order_independent_and_not_selection(self):
        f=self.fixture();observations=tuple(self.observe(f,c) for c in self.cells(f));prefix=f.chain.commitment
        result=self.read(f,tuple(reversed(observations)))
        self.assertEqual(result['counts'],{'pass':3,'fail':0,'unavailable':0})
        self.assertEqual([r['cell_sha256'] for r in result['cells']],[c.sha256 for c in self.cells(f)])
        self.assertEqual(result['disposition'],'pass');self.assertEqual(f.chain.commitment,prefix)
        self.assertEqual(len(result['authored_public_cells_outstanding']),4)
        for k in ('selection_authority','acceptance_authority','complete_comparison','qualified_resource_envelope'):
            self.assertFalse(result[k])

    def test_omitted_originals_stay_in_the_denominator(self):
        f=self.fixture();observation=self.observe(f,self.cells(f)[0]);result=self.read(f,(observation,))
        self.assertEqual(result['counts'],{'pass':1,'fail':0,'unavailable':2})
        self.assertEqual(result['required_cells'],3);self.assertEqual(result['supplied_cells'],1)
        self.assertEqual(result['disposition'],'unavailable');self.assertFalse(result['mechanics_complete'])
        self.assertEqual(sum(r['missing_original'] for r in result['cells']),2)

    def test_failure_survives_incomplete_cleanup_and_missing_peer_observations(self):
        f=self.fixture();observation=self.observe(f,self.cells(f)[0],defect=True,incomplete=True)
        result=self.read(f,(observation,))
        self.assertEqual(result['counts'],{'pass':0,'fail':1,'unavailable':2})
        self.assertEqual(result['disposition'],'fail');self.assertFalse(result['mechanics_complete'])

    def test_merged_phase_needs_its_own_original_even_for_identical_context_bytes(self):
        f=self.fixture();contextual=self.observe(f,self.cells(f)[0])
        with self.assertRaisesRegex(ValueError,'another_original_comparison'):
            self.read(f,(contextual,),phase='merged')
        self.assertEqual(self.read(f,phase='merged')['disposition'],'unavailable')
        cell=next(c for c in f.matrix.merged_cells if c.kind=='generated-history')
        merged=self.observe(f,cell);result=self.read(f,(merged,),phase='merged')
        self.assertEqual(result['disposition'],'pass');self.assertEqual(result['required_cells'],1)
        self.assertEqual(result['selected_context']['vector'],list(f.compiled.anchor_vector))
        self.assertFalse(result['complete_comparison'])

    def test_duplicate_foreign_and_supplied_verdicts_are_rejected(self):
        f=self.fixture();observation=self.observe(f,self.cells(f)[0])
        with self.assertRaisesRegex(ValueError,'duplicate_or_foreign_probe_cell'):
            self.read(f,(observation,observation))
        changed=replace(observation,enrollment=replace(observation.enrollment,cell_sha256='a'*64))
        with self.assertRaisesRegex(ValueError,'duplicate_or_foreign_probe_cell'):self.read(f,(changed,))
        with self.assertRaisesRegex(ValueError,'original_observation_roster_required'):
            self.read(f,({'disposition':'pass'},))

    def test_earlier_execution_append_during_later_read_invalidates_complete_census(self):
        f=self.fixture();observations=tuple(self.observe(f,c) for c in self.cells(f)[:2]);seen=[]
        def profile(frame,event,arg):
            if event=='call' and frame.f_code is matrix_reader.reader.reconstruct_enrolled.__code__:
                seen.append(True)
                if len(seen)==2:observations[0].state.journal.retain('late-matrix-record.json',b'{}')
        prior=sys.getprofile();sys.setprofile(profile)
        try:
            with self.assertRaisesRegex(ValueError,'probe_changed_during_matrix_read'):
                self.read(f,observations)
        finally:sys.setprofile(prior)
        self.assertEqual(len(seen),2)

    def test_missing_all_originals_never_produces_a_passing_census(self):
        f=self.fixture();result=self.read(f)
        self.assertEqual(result['counts'],{'pass':0,'fail':0,'unavailable':3})
        self.assertEqual(result['disposition'],'unavailable')
        with self.assertRaisesRegex(ValueError,'contextual_phase_cannot_borrow'):
            self.read(f,selected=f.compiled.anchor_vector)


if __name__=='__main__':unittest.main()
