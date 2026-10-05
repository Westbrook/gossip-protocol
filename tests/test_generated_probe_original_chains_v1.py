"""Whole original-reader path over explicitly synthetic complete record chains.

No reader method is patched. These fixtures use real Git/journals/SQLite parsing
but never execute candidates, Docker or providers, and give no real approval.
"""
import json
import unittest

from gossip_harness import cumulative_generated_probe_reader_v1 as reader
from gossip_harness import cumulative_generated_probe_values_v2 as values
from tests.generated_probe_originals_fixture_v1 import SyntheticOriginals


class GeneratedProbeWholeOriginalChainTests(unittest.TestCase):
    def make(self,template='refresh-noop-v1',**kwargs):
        fixture=SyntheticOriginals(template,**kwargs);self.addCleanup(fixture.close);return fixture

    def observe(self,fixture,**kwargs):
        prefix=fixture.retain(**kwargs);result=reader.reconstruct(fixture.owner,expected=prefix)
        self.assertEqual(fixture.owner.journal.checkpoint(),prefix)
        self.assertFalse(result['acceptance_authority']);self.assertFalse(result['independent_acceptance'])
        self.assertFalse(result['physically_executed_by_reader'])
        return result

    def test_all_four_complete_original_chains_reconstruct_without_reader_mocks(self):
        for template in values.TEMPLATES:
            with self.subTest(template=template):
                fixture=self.make(template);result=self.observe(fixture)
                self.assertEqual(result['disposition'],'pass');self.assertTrue(result['mechanics_complete'])
                self.assertEqual(result['limitations'],[])

    def test_each_semantic_defect_reconstructs_from_original_values_or_sqlite(self):
        for template in values.TEMPLATES:
            with self.subTest(template=template):
                result=self.observe(self.make(template,defect=True))
                self.assertEqual(result['disposition'],'fail');self.assertTrue(result['mechanics_complete'])

    def test_good_interrupted_history_cannot_be_credited(self):
        result=self.observe(self.make(),stop_before='probe-frame-finished.bin')
        self.assertEqual(result['disposition'],'unavailable');self.assertFalse(result['mechanics_complete'])

    def test_observed_defect_survives_interruption_before_finish(self):
        result=self.observe(self.make(defect=True),stop_before='probe-frame-finished.bin')
        self.assertEqual(result['disposition'],'fail');self.assertFalse(result['mechanics_complete'])

    def test_missing_cleanup_original_defeats_forged_positive_terminal(self):
        result=self.observe(self.make(),drop=('volume-after.json',))
        self.assertEqual(result['disposition'],'unavailable');self.assertFalse(result['qualified_execution_originals'])

    def test_known_defect_survives_missing_cleanup_original(self):
        result=self.observe(self.make(defect=True),drop=('container-remove.json',))
        self.assertEqual(result['disposition'],'fail');self.assertFalse(result['mechanics_complete'])

    def test_changed_runtime_original_is_fatal_despite_unchanged_projection(self):
        fixture=self.make()
        def change(name,raw):
            if name=='refresh-runtime-info-response.bin':return raw.replace(b'synthetic-daemon',b'forgedxxx-daemon')
            return raw
        prefix=fixture.retain(transform=change)
        with self.assertRaisesRegex(reader.OriginalError,'runtime_changed'):
            reader.reconstruct(fixture.owner,expected=prefix)

    def test_exec_incarnation_change_is_fatal(self):
        fixture=self.make()
        def change(name,raw):
            if name=='refresh-exec-response.bin':return raw.replace(b'"Pid":321',b'"Pid":322')
            return raw
        prefix=fixture.retain(transform=change)
        with self.assertRaisesRegex(ValueError,'stable_live_probe_pid'):
            reader.reconstruct(fixture.owner,expected=prefix)

    def test_database_projection_cannot_override_original_sqlite_row(self):
        fixture=self.make('manifest-content-hash-v1')
        def change(name,raw):
            if name=='captured-job.json':
                body=json.loads(raw);body['content_hashes']=json.dumps(['0'*64]);return values.canonical(body)
            return raw
        prefix=fixture.retain(transform=change)
        with self.assertRaisesRegex(reader.OriginalError,'captured_job_projection'):
            reader.reconstruct(fixture.owner,expected=prefix)

    def test_missing_capture_resume_makes_good_capture_unavailable(self):
        fixture=self.make('manifest-content-hash-v1');result=self.observe(fixture,drop=('capture-unpause.json',))
        self.assertEqual(result['disposition'],'unavailable');self.assertFalse(result['mechanics_complete'])

    def test_closed_complete_state_reopens_for_reading_without_redispatch(self):
        fixture=self.make();prefix=fixture.retain();first=reader.reconstruct(fixture.owner,expected=prefix)
        fixture.owner.close();reopened=fixture.open(prefix)
        second=reader.reconstruct(reopened,expected=prefix)
        self.assertEqual(first,second);self.assertEqual(reopened.journal.checkpoint(),prefix)
        with self.assertRaisesRegex(ValueError,'existing_probe_intent'):reopened.begin()

    def test_frame_identity_guard_cannot_precede_raw_frame(self):
        fixture=self.make();items=list(fixture.records.items());frame='probe-frame-refresh.bin';staging='refresh-staging.json'
        one=next(i for i,(name,_) in enumerate(items) if name==frame)
        two=next(i for i,(name,_) in enumerate(items) if name==staging)
        items[one],items[two]=items[two],items[one];fixture.records=dict(items)
        prefix=fixture.retain()
        with self.assertRaisesRegex(reader.OriginalError,'chronology'):
            reader.reconstruct(fixture.owner,expected=prefix)


if __name__=='__main__':unittest.main()
