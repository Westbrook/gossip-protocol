"""Original corpus and finite host comparators; no candidate imports or execution."""
from dataclasses import replace
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from gossip_harness import candidate_m2_product_profile_v1 as profile
from gossip_harness import candidate_m2_product_observation_v1 as observer
from gossip_harness import cumulative_scope_source_v1 as scope


class CandidateM2ProductProfileV1Tests(unittest.TestCase):
    def test_complete_native_roster_and_input_expected_bytes_are_preserved(self):
        cases = profile.native.acceptance_cases()
        self.assertEqual(len(cases), 12)
        self.assertEqual(sum(len(c['input']['actions']) for c in cases), 224)
        self.assertEqual(sum(len(c['expected']['observations']) for c in cases), 140)
        for case in cases:
            value = profile.profile_for(case['id'])
            self.assertEqual(value.record()['definition'], case)
            self.assertEqual(profile.recipe_for(case['id']), case['input'])
            self.assertEqual(len(value.phases), len(case['input']['actions']))
            self.assertEqual(len(value.selectors()), len(case['expected']['observations']))
        self.assertEqual([r['action_index'] for r in profile.profile_for('m2-batch-generation-replay').selectors()], [1,3,4,5,6,10])
        self.assertEqual([r['action_index'] for r in profile.profile_for('m2-collection-boundary').selectors()], list(range(64,69)))

    def test_all_declared_unit_edges_resolve_in_full_source_catalog(self):
        catalog = scope.load_catalog(Path(__file__).resolve().parents[1])
        for case in profile.native.acceptance_cases():
            value = profile.profile_for(case['id'])
            for row in value.selectors():
                for unit in row['source_unit_ids']:
                    self.assertEqual(catalog.unit(unit).obligation.id, unit)
                    self.assertIn(unit.split(':')[0], value.requirement_ids)
                    self.assertFalse(row['semantically_reviewed'])

    def test_host_full_value_digest_matches_all_four_original_digest_expectations(self):
        n = profile.native
        source = 'blob/032.txt'
        first, replacement_text = n._blob_text(32,1), n._blob_text(0,1)
        values = {0: {'error':'capacity'}, 1:n._record(source,first),
            2:{'status':'refreshed','record':n._record(source,replacement_text,2,2)},
            3:n._history(source,[first,replacement_text])}
        value = profile.profile_for('m2-retained-blob-capacity')
        result = profile.project(value, values, {})
        self.assertTrue(all(row['disposition']=='pass' for row in result['observations']))
        forged = {i: row for i,row in enumerate(profile.case_definition(value.case_id)['expected']['observations'])}
        rejected = profile.project(value, forged, {})
        self.assertTrue(all(row['disposition']=='fail' for row in rejected['observations']))

    def test_known_false_survives_missing_tail_and_setup_stays_unspecified(self):
        value = profile.profile_for('m2-collection-boundary')
        result = profile.project(value, {0: None,64:{'error':'wrong'}}, {})
        self.assertEqual(result['observations'][0]['disposition'],'fail')
        self.assertEqual(result['observations'][1]['disposition'],'unavailable')
        self.assertEqual(result['diagnostics'][0]['disposition'],'unspecified')
        self.assertEqual(len(result['diagnostics']),69)
        self.assertEqual(len(result['known_failed']),1)

    def test_type_exact_and_json_shape_failures_not_python_equality(self):
        self.assertFalse(profile.exact({'value':1}, {'value':True}))
        self.assertFalse(profile.exact([1],(1,)))
        self.assertFalse(profile.exact({'a':1},{'a':1,'b':2}))
        self.assertTrue(profile.exact({'a':[1,None,'é']},{'a':[1,None,'é']}))

    def test_original_purposes_are_disclosed_and_execution_purpose_changes_identity(self):
        value = profile.profile_for('m2-competing-connections')
        independent = profile.profile_for(value.case_id,'independent_acceptance')
        self.assertNotEqual(value.sha256,independent.sha256)
        self.assertEqual(value.record()['original_definition_purpose'],'independent_acceptance')
        self.assertEqual(value.record()['previous_execution_purpose'],'authored_reference_qualification')
        self.assertFalse(value.record()['independent_semantic_scope_review_supplied'])
        with self.assertRaises(ValueError): profile.profile_for(value.case_id,'authored_reference_qualification')
        with self.assertRaises(ValueError): profile.profile_for('made-up')

    def test_seed_is_built_on_host_before_candidate_import_and_large_fixture_is_complete(self):
        imported = []
        real_import = __import__
        def guard(name,*args,**kwargs):
            imported.append(name)
            if name.startswith('library.'): raise AssertionError('candidate imported on host')
            return real_import(name,*args,**kwargs)
        with patch('builtins.__import__', side_effect=guard):
            raw = profile.fixture_files('m2-retained-blob-capacity')['seed.sqlite']
        self.assertGreater(len(raw),16*1024*1024)
        self.assertEqual(raw[:16],b'SQLite format 3\0')
        self.assertFalse(any(name.startswith('library.') for name in imported))
        self.assertLess(profile.ADAPTER.index('shutil.copyfile'),profile.ADAPTER.index('from library.'))
        self.assertNotIn('seed_database',profile.ADAPTER)
        self.assertNotIn('hashlib.sha256',profile.ADAPTER)
        self.assertNotIn('SELECT manifest',profile.ADAPTER)

    def test_surrogate_inputs_remain_exact_in_input_only_ascii_recipe(self):
        recipe = profile.recipe_for('m2-refresh-input-boundaries')
        raw = profile.encoded(recipe)
        self.assertIn(b'\\ud800',raw)
        self.assertEqual(profile.decode(raw),recipe)
        self.assertNotIn('expected',recipe)
        self.assertEqual(profile.ADAPTER.count("print(encode({'phase'"),1)

    def test_reopen_and_direct_route_selectors_do_not_get_transport_or_crash_credit(self):
        value = profile.profile_for('m2-competing-connections')
        self.assertEqual(value.selectors()[2]['source_unit_ids'],[])
        catalog = observer.selector_catalog('m2-service-route-contract',purpose='public_release')
        for forbidden in ('http','cli','browser','durability'):
            self.assertNotIn(forbidden,catalog['capabilities'])
        self.assertFalse(catalog['semantic_authority'])


class CandidateM2SQLiteObservationV1Tests(unittest.TestCase):
    def seed(self):
        raw=profile.fixture_files('m2-migrate-aba-receipt')['seed.sqlite']
        plan=SimpleNamespace(storage_paths=('m2/library.sqlite',),schema_sha256=observer.sqlite_schema_sha256(raw))
        recipe=profile.recipe_for('m2-migrate-aba-receipt')
        job_id=recipe['actions'][19]['job_id']
        return raw,plan,job_id

    def mutate(self,raw,statement):
        with tempfile.TemporaryDirectory() as temp:
            p=Path(temp)/'db';p.write_bytes(raw)
            c=sqlite3.connect(p);c.execute(statement);c.commit();c.close()
            return p.read_bytes()

    def test_original_sqlite_strings_are_read_externally_and_match_authored_expectation(self):
        raw,plan,job_id=self.seed()
        actual=observer.sqlite_job_value({'m2/library.sqlite':raw},plan,job_id)
        value=profile.profile_for('m2-migrate-aba-receipt')
        result=profile.project(value,{19:{'raw_capture_required':True}},{19:actual})
        self.assertEqual(result['observations'][19]['disposition'],'pass')
        self.assertEqual(actual,profile.case_definition(value.case_id)['expected']['observations'][19])

    def test_absent_row_and_null_receipt_are_known_failures_not_unavailable(self):
        for statement in ('DELETE FROM jobs','UPDATE jobs SET receipt=NULL'):
            raw,plan,job_id=self.seed()
            changed=self.mutate(raw,statement)
            actual=observer.sqlite_job_value({'m2/library.sqlite':changed},plan,job_id)
            result=profile.project(profile.profile_for('m2-migrate-aba-receipt'),
                {19:{'raw_capture_required':True}},{19:actual})
            self.assertEqual(result['observations'][19]['disposition'],'fail')

    def test_schema_or_unregistered_path_and_wal_are_unavailable(self):
        raw,plan,job_id=self.seed()
        wrong=SimpleNamespace(storage_paths=plan.storage_paths,schema_sha256='a'*64)
        with self.assertRaises(observer.AuthorityUnavailable): observer.sqlite_job_value({'m2/library.sqlite':raw},wrong,job_id)
        with self.assertRaises(observer.AuthorityUnavailable): observer.sqlite_job_value({'m2/library.sqlite':raw,'hidden':b'x'},plan,job_id)
        wal=SimpleNamespace(storage_paths=('m2/library.sqlite','m2/library.sqlite-wal'),schema_sha256=plan.schema_sha256)
        with self.assertRaises(observer.AuthorityUnavailable): observer.sqlite_job_value({'m2/library.sqlite':raw,'m2/library.sqlite-wal':b'x'},wal,job_id)

    def test_child_job_marker_alone_cannot_supply_raw_sqlite_fact(self):
        value=profile.profile_for('m2-migrate-aba-receipt')
        expected=profile.case_definition(value.case_id)['expected']['observations'][19]
        result=profile.project(value,{19:expected},{})
        self.assertEqual(result['observations'][19]['disposition'],'unavailable')
        result=profile.project(value,{19:{'raw_capture_required':True}},{})
        self.assertEqual(result['observations'][19]['disposition'],'unavailable')


class CandidateM2ReviewAuthorityV1Tests(unittest.TestCase):
    def setUp(self):
        from gossip_harness import candidate_m2_review_authority_v1 as review
        self.review=review
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name).resolve()
        value=profile.profile_for('m2-query-generation-pages')
        self.plan=review.LayoutPlan('m2-direct-api','a'*64,'b'*64,'c'*40,'d'*40,value.case_id,value.sha256,
            'reviewed-m2-final-sqlite-v1',('m2/library.sqlite',),'e'*64)

    def authority(self,**kwargs):
        from tests.test_candidate_m2_product_execution_v1 import enroll
        authority,journal,head=enroll(self.root,self.plan,**kwargs)
        self.addCleanup(head.close);self.addCleanup(journal.close)
        return authority,journal

    def test_original_review_linkage_binds_m2_instrumentation_duty_and_retained_prefix(self):
        authority,journal=self.authority()
        self.assertEqual(authority.authenticate(self.plan),authority.enrollment.report_sha256)
        self.assertIn('public_direct_api_instrumentation_and_untrusted_same_process_output',self.plan.request()['duties'])
        self.assertEqual(journal.commitment,authority.expected)

    def test_wrong_purpose_or_source_plan_cannot_relabel_an_enrolled_review(self):
        authority,_=self.authority(changes={'purpose':'independent_final_m4_storage_layout_and_invocation'})
        with self.assertRaises(ValueError): authority.authenticate(self.plan)

    def test_report_must_predate_actual_independent_delivery(self):
        authority,_=self.authority(delivery_first=True)
        with self.assertRaisesRegex(ValueError,'order'): authority.authenticate(self.plan)

    def test_authoritative_prefix_growth_revokes_review(self):
        authority,journal=self.authority()
        journal.retain('late.json',b'{}')
        with self.assertRaises(ValueError): authority.authenticate(self.plan)
