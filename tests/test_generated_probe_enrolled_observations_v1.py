"""Actual controller/observation joins over deliberately synthetic originals.

No reader or enrollment function is mocked. Financial/mesh/source-review and
Engine records are authored fixture data; this is not physical qualification.
"""
from dataclasses import replace
import json
import sys
import unittest
from unittest import mock

from gossip_harness import candidate_checkpoint_chain_v1 as chain
from gossip_harness import cumulative_generated_probe_accounting_v1 as accounting
from gossip_harness import cumulative_generated_probe_reader_v1 as reader
from gossip_harness import cumulative_generated_probe_values_v2 as values
from tests import test_cumulative_generated_probe_accounting_v1 as accounting_fixture
from tests.generated_probe_originals_fixture_v1 import SyntheticOriginals


class GeneratedProbeEnrolledObservationGitTests(unittest.TestCase):
    def make(self, *, defect=False, late_window=False):
        helper=accounting_fixture.GeneratedProbeOriginalAccountingGitTests(
            'test_cell_enrollment_is_one_durable_assignment_and_inspection_never_reissues')
        self.addCleanup(helper.doCleanups)
        f,slot,cell,plan,selected=helper.cell_fixture()
        plan=replace(plan,policy=replace(plan.policy,history_seconds=300))
        locator=accounting.ProbeCellEnrollment(f.owner,f.book,slot,cell.sha256,f.root/'enrolled-execution',
            f.milestone,f.generation,helper.quotas,f.limits,selected)
        if late_window:
            clock=reader.child_clocks.read(f.owner.records,f.plan,f.index,active=False)
            with mock.patch.object(reader.state.time,'monotonic_ns',return_value=clock.deadline_ns-300_000_000_000):
                physical=SyntheticOriginals(defect=defect,enrolled=(locator,plan,f.cell_store))
        else:
            physical=SyntheticOriginals(defect=defect,enrolled=(locator,plan,f.cell_store))
        self.addCleanup(physical.close)
        return f,locator,physical

    def observe(self,f,locator,physical,**changes):
        return reader.reconstruct_enrolled(physical.owner,expected=physical.owner.journal.checkpoint(),
            enrollment=locator,controller_expected=f.chain.commitment,**changes)

    def test_complete_original_join_is_read_only_and_allows_cold_read_after_expiry(self):
        f,locator,physical=self.make();execution_prefix=physical.retain();controller_prefix=f.chain.commitment
        with mock.patch.object(reader.state.time,'monotonic_ns',return_value=physical.binding.window.deadline_ns+1):
            result=self.observe(f,locator,physical)
        self.assertEqual(result['observation']['disposition'],'pass')
        self.assertTrue(result['original_cell_bound']);self.assertEqual(result['cell_sha256'],locator.cell_sha256)
        self.assertEqual(result['enrollment'],physical.enrollment_reference)
        self.assertEqual(f.chain.commitment,controller_prefix)
        self.assertEqual(physical.owner.journal.checkpoint(),execution_prefix)
        for key in ('qualified_resource_envelope','selection_authority','acceptance_authority','physically_executed_by_reader'):
            self.assertFalse(result[key])
        with self.assertRaisesRegex(ValueError,'roots_differ_from_enrollment'):
            self.observe(f,replace(locator,execution_root=f.root/'different-execution'),physical)

    def test_matching_enrollment_cannot_turn_incomplete_good_observation_into_pass(self):
        f,locator,physical=self.make();physical.retain(drop=('volume-after.json',))
        result=self.observe(f,locator,physical)
        self.assertEqual(result['observation']['disposition'],'unavailable')
        self.assertFalse(result['observation']['mechanics_complete'])
        self.assertTrue(result['original_cell_bound'])

    def test_known_defect_remains_a_failure_with_missing_later_cleanup(self):
        f,locator,physical=self.make(defect=True);physical.retain(drop=('volume-after.json',))
        result=self.observe(f,locator,physical)
        self.assertEqual(result['observation']['disposition'],'fail')
        self.assertFalse(result['observation']['mechanics_complete'])

    def test_self_consistent_execution_cannot_borrow_another_enrollment_record(self):
        f,locator,physical=self.make()
        body=json.loads(physical.records['physical-intent.json'])
        body['cell_enrollment']['record_sha256']='c'*64
        physical.records['physical-intent.json']=values.canonical(body);physical.retain()
        with self.assertRaisesRegex(reader.OriginalError,'enrollment_differs_from_original'):
            self.observe(f,locator,physical)

    def test_recorded_cleanup_root_must_match_original_assignment(self):
        f,locator,physical=self.make()
        body=json.loads(physical.records['physical-intent.json']);body['cleanup_root']=str(f.root/'different-cleanup')
        physical.records['physical-intent.json']=values.canonical(body);physical.retain()
        with self.assertRaisesRegex(reader.OriginalError,'cleanup_root_differs_from_original'):
            self.observe(f,locator,physical)

    def test_execution_prefix_does_not_implicitly_adopt_new_records(self):
        f,locator,physical=self.make();prior=physical.owner.journal.checkpoint();physical.retain()
        with self.assertRaisesRegex(reader.OriginalError,'checkpoint_append'):
            reader.reconstruct_enrolled(physical.owner,expected=prior,enrollment=locator,controller_expected=f.chain.commitment)

    def test_controller_append_during_reconstruction_invalidates_the_join(self):
        f,locator,physical=self.make();physical.retain();seen=[]
        def profile(frame,event,arg):
            if event=='call' and frame.f_code is reader.reconstruct.__code__ and not seen:
                seen.append(True)
                f.owner.records.put('synthetic-intervening-controller-record',{'fixture_only':True})
        prior=sys.getprofile();sys.setprofile(profile)
        try:
            with self.assertRaises(chain.ChainUnknown):self.observe(f,locator,physical)
        finally:sys.setprofile(prior)
        self.assertEqual(seen,[True]);self.assertTrue(f.chain.uncertain)

    def test_original_child_cleanup_headroom_is_independently_checked(self):
        f,locator,physical=self.make(late_window=True);physical.retain()
        with self.assertRaisesRegex(reader.OriginalError,'clock_or_cleanup_differs'):
            self.observe(f,locator,physical)


if __name__=='__main__':unittest.main()
