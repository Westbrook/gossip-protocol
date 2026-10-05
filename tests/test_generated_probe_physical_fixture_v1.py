"""Preparation controls only: no generated candidate imports or Engine requests."""
import ast
import json
from pathlib import Path
import tempfile
import unittest

from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import cumulative_generated_probe_values_v2 as values
from gossip_harness import cumulative_generated_probe_wire_v1 as wire
from tests import generated_probe_physical_fixture_v1 as fixture


class GeneratedProbePhysicalFixtureTests(unittest.TestCase):
    def test_closed_roster_covers_each_template_with_reference_and_defect(self):
        rows=fixture.controls();self.assertEqual(len(rows),12);self.assertEqual(len({r['id'] for r in rows}),12)
        for template in values.TEMPLATES:
            pair=[r for r in rows if r['template']==template and r['id'][0] in ('P','D')]
            self.assertEqual({r['expected_value_disposition'] for r in pair},{'pass','fail'})
            self.assertTrue(all(r['expected_mechanics_complete'] for r in pair))
        bad=next(r for r in rows if r['id']=='N02')
        self.assertEqual(bad['expected_value_disposition'],'fail');self.assertFalse(bad['expected_mechanics_complete'])
        rows[0]['variant']='mutated';self.assertEqual(fixture.controls()[0]['variant'],'reference')

    def test_mutants_change_only_the_named_store_extension(self):
        original,_=fixture.candidate_files('reference')
        for variant in fixture.MUTATIONS:
            with self.subTest(variant=variant):
                files,origin=fixture.candidate_files(variant)
                self.assertEqual(set(files),set(original))
                self.assertEqual({n for n in files if files[n]!=original[n]},{fixture.SOURCE_PATH})
                self.assertEqual(files[fixture.SOURCE_PATH],original[fixture.SOURCE_PATH]+fixture.MUTATIONS[variant].encode('utf-8'))
                ast.parse(files[fixture.SOURCE_PATH])
                self.assertFalse(origin['candidate_executed']);self.assertFalse(origin['approval_supplied'])
                self.assertEqual(origin['scientific_samples'],0)
        with self.assertRaisesRegex(ValueError,'closed_qualification_variant'):
            fixture.candidate_files('arbitrary-upload')

    def test_real_public_contract_and_probe_request_keep_expectations_host_side(self):
        release=fixture.release();self.assertEqual(release.milestone,'M4')
        for template in values.TEMPLATES:
            proposed=fixture.proposal(template)
            admitted=values.admit(proposed,released_requirements=release.requirement_ids,contract_sha256=values.PRODUCT_SHA256)
            request=json.loads(wire.request_bytes(admitted,released_requirements=release.requirement_ids))
            self.assertEqual(set(request),{'protocol','template_id','parameters'})
            self.assertNotIn('expectation',request)
        self.assertEqual(fixture.policy().history_seconds,300)
        self.assertEqual(fixture.policy().control_seconds,30)

    def test_schema_is_authored_and_rejects_changed_schema_source(self):
        files,_=fixture.candidate_files('manifest')
        schema,record=fixture.base.schema_fixture(files);layout=fixture.capture_layout(schema)
        self.assertEqual(layout.storage_paths,('m2/library.sqlite','m2/library.sqlite.maintenance/maintenance.lock',
                                              'm2/library.sqlite.maintenance/worker.lock'))
        self.assertEqual(layout.schema_sha256,record['schema_sha256'])
        changed=dict(files);changed['library/catalog/m4_store.py']+=b'\n# changed\n'
        with self.assertRaisesRegex(ValueError,'schema source differs'):fixture.base.schema_fixture(changed)


class GeneratedProbePhysicalPreparationGitTests(unittest.TestCase):
    def test_complete_inert_source_request_and_helper_census_survive_preparation(self):
        with tempfile.TemporaryDirectory(prefix='probe-review-preparation-') as temp:
            root=Path(temp).resolve()/'packet'
            census=fixture.prepare(root,control_ids=('D04',))
            self.assertEqual(census['selected_control_ids'],['D04'])
            self.assertFalse(census['approval_supplied']);self.assertEqual(census['candidate_executions'],0)
            self.assertEqual(len(census['not_yet_executed_host_schedules']),8)
            row=census['controls'][0];directory=root/'D04'
            request=(directory/'request.json').read_bytes()
            self.assertEqual(fixture.hashlib.sha256(request).hexdigest(),row['request_sha256'])
            self.assertEqual(values.canonical(json.loads(request)['plan']),values.canonical(row['plan']))
            source={p.relative_to(directory/'source').as_posix():p.read_bytes()
                    for p in (directory/'source').rglob('*') if p.is_file()}
            self.assertEqual(admission.source_sha256(source),row['target']['subject']['source_sha256'])
            helpers={p.name:p.read_bytes() for p in (directory/'helpers').iterdir()}
            self.assertEqual(admission.source_manifest(helpers),row['plan']['helper_manifest'])
            self.assertFalse((directory/'report.json').exists());self.assertFalse((directory/'delivery.json').exists())
            with self.assertRaises(FileExistsError):fixture.prepare(root,control_ids=('D04',))
            self.assertEqual((directory/'request.json').read_bytes(),request)

    def test_invalid_selection_does_not_create_a_packet(self):
        with tempfile.TemporaryDirectory(prefix='probe-review-selection-') as temp:
            root=Path(temp).resolve()/'packet'
            for selection in ((),('P01','P01'),('unknown',)):
                with self.subTest(selection=selection),self.assertRaises(ValueError):fixture.prepare(root,control_ids=selection)
                self.assertFalse(root.exists())


if __name__=='__main__':unittest.main()
