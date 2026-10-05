"""Inert source/review preparation for generated-probe physical qualification.

Generated candidate Python is only parsed and committed as data. Only the
existing host-authored literal schema fixture is evaluated. No candidate is
imported, no Engine operation occurs, and no review approval is manufactured.
"""
from __future__ import annotations

import ast
from copy import deepcopy
from dataclasses import asdict
import hashlib
from pathlib import Path
from typing import Any

from gossip_harness import candidate_m2_product_observation_v1 as sqlite_observation
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import cumulative_generated_probe_driver_v1 as driver
from gossip_harness import cumulative_generated_probe_execution_v1 as execution
from gossip_harness import cumulative_generated_probe_plan_v1 as plans
from gossip_harness import cumulative_generated_probe_values_v2 as values
from gossip_harness import cumulative_generated_probe_wire_v1 as wire
from gossip_harness import cumulative_study_controller_v2 as study
from gossip_harness import library_cumulative_public_fixture_v2 as public
from gossip_harness import project_acceptance_registry_v1 as registry
from tests import candidate_storage_m2_physical_v1 as base

PROTOCOL = 'generated-probe-physical-qualification-fixtures-v1'
SOURCE_PATH = 'library/catalog/store.py'
COHORT = ('qualification-original', *(f'qualification-reserved-{i}' for i in range(1, 6)))
SCIENTIFIC_SAMPLES = 0
MUTATIONS = {
    'identity': '''\nclass Store(Store):
    def refresh_document(self, document_id, expected_version, *, text):
        value = super().refresh_document(document_id, expected_version, text=text)
        value['record']['document']['document_id'] = 'deliberately-wrong-document'
        return value
''',
    'noop': '''\nclass Store(Store):
    def refresh_document(self, document_id, expected_version, *, text):
        value = super().refresh_document(document_id, expected_version, text=text)
        value['status'] = 'refreshed'
        return value
''',
    'replay': '''\nclass Store(Store):
    def commit_job(self, job_id, epoch, *, fail_before_commit=False):
        completed = self.get_job(job_id)['state'] == 'completed'
        value = super().commit_job(job_id, epoch, fail_before_commit=fail_before_commit)
        if completed:
            value['documents'] = [self.lifecycle_show(row['document_id'])['document'] for row in value['documents']]
        return value
''',
    'manifest': '''\nclass Store(Store):
    def create_job(self, job_id, entries):
        import json
        value = super().create_job(job_id, entries)
        self.db.execute('UPDATE jobs SET content_hashes=? WHERE job_id=?', (json.dumps(['0' * 64]), job_id))
        return value
''',
    'hang': '''\nclass Store(Store):
    def refresh_document(self, document_id, expected_version, *, text):
        import time
        while True:
            time.sleep(60)
''',
    'bad-then-hang': '''\nclass Store(Store):
    def refresh_document(self, document_id, expected_version, *, text):
        value = super().refresh_document(document_id, expected_version, text=text)
        value['status'] = 'refreshed'
        return value
    def close(self):
        import time
        while True:
            time.sleep(60)
''',
    'stdout-flood': '''\nclass Store(Store):
    def refresh_document(self, document_id, expected_version, *, text):
        import sys
        sys.stdout.write('x' * 1048576)
        sys.stdout.flush()
        return super().refresh_document(document_id, expected_version, text=text)
''',
    'abrupt-exit': '''\nclass Store(Store):
    def refresh_document(self, document_id, expected_version, *, text):
        import os
        os._exit(17)
''',
}


def controls() -> tuple[dict[str, Any], ...]:
    rows = []
    for index, template, defect in ((1, 'refresh-identity-v1', 'identity'), (2, 'refresh-noop-v1', 'noop'),
                                   (3, 'completed-receipt-replay-v1', 'replay'), (4, 'manifest-content-hash-v1', 'manifest')):
        for prefix, variant, expected in (('P', 'reference', 'pass'), ('D', defect, 'fail')):
            rows.append({'id': f'{prefix}{index:02d}', 'template': template, 'variant': variant,
                         'expected_value_disposition': expected, 'expected_mechanics_complete': True})
    for index, variant, expected in ((1, 'hang', 'unavailable'), (2, 'bad-then-hang', 'fail'),
                                     (3, 'stdout-flood', 'unavailable'), (4, 'abrupt-exit', 'unavailable')):
        rows.append({'id': f'N{index:02d}', 'template': 'refresh-noop-v1', 'variant': variant,
                     'expected_value_disposition': expected, 'expected_mechanics_complete': False})
    return tuple(deepcopy(rows))


def proposal(template: str) -> dict[str, Any]:
    plans.require(template in values.TEMPLATES, 'closed_qualification_template_required')
    changed = template in ('refresh-identity-v1', 'completed-receipt-replay-v1')
    return {'template_id': template,
        'requirement_id': 'M3-BACKUP-RESTORE' if template == 'manifest-content-hash-v1' else 'M2-REFRESH',
        'parameters': {'initial_text': 'before\ncafé', 'replacement_text': 'after\n🌍'} if changed else {'text': 'same\ncafé'},
        'expectation': {'kind': 'exact_scalar', 'value': 'unchanged'} if template == 'refresh-noop-v1' else
                       {'kind': 'contract_relation', 'value': True}}


def release() -> study.Release:
    files = public.public_files()
    return study.Release('M4', 'Authored generated-probe qualification slice only. Read the complete frozen product contract. '
        'The two declared requirements scope these probes; neither the release nor these controls establish complete product coverage.',
        {'library-cumulative-product-v2.json': public.public_manifest()['normative_contract_utf8']}, files,
        ('python', 'test_cumulative_public.py'), ('generated-probe-qualification-public-slice',),
        ('M2-REFRESH', 'M3-BACKUP-RESTORE'))


def policy() -> plans.ProbePolicy:
    return plans.ProbePolicy(wire.WireLimits(131072, 524288, 65536, 64), 65536, 300, 30,
                             'generated-probe-physical-qualification-v1')


def candidate_files(variant: str) -> tuple[dict[str, bytes], dict[str, Any]]:
    plans.require(variant == 'reference' or variant in MUTATIONS, 'closed_qualification_variant_required')
    files, origin = base.candidate_files('reference')
    mutations = []
    if variant != 'reference':
        before = files[SOURCE_PATH]
        addition = MUTATIONS[variant].encode('utf-8')
        files[SOURCE_PATH] = before + addition
        ast.parse(files[SOURCE_PATH], filename=SOURCE_PATH)
        mutations.append({'path':SOURCE_PATH, 'operation':'append_exact_bytes', 'addition':addition.decode('utf-8'),
                          'before_sha256':hashlib.sha256(before).hexdigest(), 'after_sha256':hashlib.sha256(files[SOURCE_PATH]).hexdigest()})
    return files, {'protocol':PROTOCOL, 'variant':variant, 'base_provenance':origin,
        'mutations':mutations, 'source_manifest':admission.source_manifest(files),
        'fixture_generator_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'candidate_executed':False, 'approval_supplied':False, 'scientific_samples':0}


def capture_layout(schema: bytes) -> plans.CaptureLayout:
    paths = tuple('m2/'+name.replace('catalog.sqlite','library.sqlite') for name in base.mapper.V2_STORAGE_PATHS)
    return plans.CaptureLayout(paths, sqlite_observation.sqlite_schema_sha256(schema))


def write_json(path: Path, value: Any) -> bytes:
    raw = values.canonical(value)
    base.write_raw(path, raw)
    return raw


def prepare(root: Path, *, control_ids: tuple[str, ...] | None = None) -> dict[str, Any]:
    root = Path(root)
    plans.require(root.is_absolute() and root.resolve() == root, 'canonical_qualification_root_required')
    roster = controls(); by_id = {row['id']:row for row in roster}
    selected = tuple(by_id) if control_ids is None else control_ids
    plans.require(type(selected) is tuple and bool(selected) and len(set(selected)) == len(selected)
                  and all(key in by_id for key in selected), 'exact_nonempty_qualification_selection_required')
    root.mkdir(exist_ok=False)
    frozen_release = release(); limits = policy(); owner_sources = execution.evaluator_sources()
    base.write_raw(root/'release.json', study.canonical_payload(study.plain(asdict(frozen_release))))
    write_json(root/'owner-sources.json', owner_sources)
    retained = []
    for key in selected:
        row = by_id[key]; directory = root/key; directory.mkdir()
        files, provenance = candidate_files(row['variant'])
        store = base.make_store(directory/'candidate.git', files)
        commit = store.head(); tree = store._git('rev-parse', commit+'^{tree}')
        subject = registry.Subject('generated-probe-qualification', COHORT[0], 'M4',
            values.digest({'qualification_fixture':PROTOCOL}), values.PRODUCT_SHA256, admission.source_sha256(files))
        target = plans.ProbeTarget(subject, 0, key, 'qualification-reference-context', 'candidate-context', commit, tree)
        admitted = values.admit(proposal(row['template']), released_requirements=frozen_release.requirement_ids,
                                contract_sha256=values.PRODUCT_SHA256)
        layout = None
        if row['template'] == 'manifest-content-hash-v1':
            schema, schema_record = base.schema_fixture(files)
            base.write_raw(directory/'schema.sqlite', schema)
            write_json(directory/'schema-profile.json', schema_record)
            layout = capture_layout(schema)
        plan = plans.prepare_plan(store, target, admitted, frozen_release, limits, layout)
        source_root = directory/'source'; source_root.mkdir()
        for name, raw in sorted(files.items()):
            path = source_root/name;path.parent.mkdir(parents=True,exist_ok=True);base.write_raw(path,raw)
        helpers = driver.adapter_files(admitted,released_requirements=frozen_release.requirement_ids)
        helper_root=directory/'helpers';helper_root.mkdir()
        for name,raw in sorted(helpers.items()):base.write_raw(helper_root/name,raw)
        request_raw = write_json(directory/'request.json',plan.review_request())
        write_json(directory/'source-provenance.json',provenance)
        record = {**row, 'target':asdict(target), 'plan':plan.record(), 'request_sha256':hashlib.sha256(request_raw).hexdigest(),
            'owner_sources_sha256':values.digest(owner_sources), 'fixture_only':True, 'approval_supplied':False,
            'candidate_executions':0,'scientific_samples':0}
        write_json(directory/'control.json',record);retained.append(record)
    plans.require(execution.evaluator_sources()==owner_sources,'owner_changed_during_qualification_preparation')
    census={'protocol':PROTOCOL,'selected_control_ids':list(selected),'complete_control_roster':list(by_id),
        'controls':retained,'owner_sources_sha256':values.digest(owner_sources),'release_sha256':frozen_release.sha256,
        'candidate_executions':0,'provider_calls':0,'scientific_samples':0,'approval_supplied':False,'product_acceptance':False,
        'review_contract':'Independently inspect complete source, fixed helper invocation and applicable SQLite layout/schema for each exact request. Return actual report and delivery originals; preparation supplies neither.',
        'not_yet_executed_host_schedules':[
            {'id':'H01','boundary':'before durable intent','fault':'registration revoked','requires':'no resource creation or intent'},
            {'id':'H02','boundary':'after durable intent','fault':'host process interruption/reopen','requires':'no redispatch; owned cleanup separately proven'},
            {'id':'H03','boundary':'after resource create request before acknowledgement','fault':'lost response','requires':'no guessed ownership; prior-only cleanup or explicit uncertainty'},
            {'id':'H04','boundary':'before start','fault':'incorrect created sandbox settings','requires':'candidate never starts; owned resource removal'},
            {'id':'H05','boundary':'after first frame','fault':'runtime/exec incarnation changed','requires':'no continuation or observation credit'},
            {'id':'H06','boundary':'paused database capture','fault':'truncated tar, extra path, mismatched schema or nonempty transaction sidecar','requires':'unavailable capture; no invented row; owned teardown'},
            {'id':'H07','boundary':'raw retention or external checkpoint','fault':'write interruption or authenticated-original tamper','requires':'no observation; no journal healing; separate cleanup'},
            {'id':'H08','boundary':'normal cleanup','fault':'removal failure/unknown state','requires':'bounded prior-only fallback; no acceptance credit'}],
        'limits':['One authored reference family and deliberate derived mutants, not independent task families or model candidates.',
                  'Expected dispositions are prospective requirements, not results.',
                  'Host fault schedules are listed obligations, not implemented or executed tests.',
                  'Actual controller slot/aggregate quota, physical qualification and independent cold reconstruction remain required.']}
    write_json(root/'census.json',census)
    return census


if __name__ == '__main__':
    import argparse
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('output',type=Path)
    args=parser.parse_args();result=prepare(args.output.resolve())
    print({'prepared':len(result['controls']),'candidate_executions':0,'approval_supplied':False})
