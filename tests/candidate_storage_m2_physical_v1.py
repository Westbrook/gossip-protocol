"""Preparation only: inert authored candidate bytes and exact independent review requests.

This module neither approves layouts nor dispatches Docker. Prepared sources and
requests must be independently reviewed before the separate physical test lane.
"""
from __future__ import annotations

import ast
from dataclasses import asdict
import json
import os
import sqlite3
import tempfile
from pathlib import Path
from typing import Any

from gossip_harness import candidate_m2_product_execution_v1 as m2_execution
from gossip_harness import candidate_m2_product_profile_v1 as m2_profile
from gossip_harness import candidate_m2_review_authority_v1 as m2_review
from gossip_harness import candidate_storage_product_execution_v1 as storage_execution
from gossip_harness import candidate_storage_product_profile_v1 as storage_profile
from gossip_harness import candidate_storage_review_authority_v1 as storage_review
from gossip_harness import candidate_storage_observer_v1 as mapper
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness.candidate_source_capture_policy_v1 import BatchCapturePolicy
from gossip_harness.library_v2_json_reference_v2 import corrected_v2_files, corrected_v2_binary_files, corrected_v2_source_inputs
from tests.test_candidate_clients_docker_v4 import make_store
from tests.test_candidate_storage_driver_v1 import authored_schema_profile

PROTOCOL = 'candidate-storage-m2-physical-qualification-v1'
PURPOSE = 'public_release'
COHORT = ('qualification-original', *(f'qualification-reserved-{i}' for i in range(1, 6)))
SOURCE_PATH = 'library/catalog/store.py'
ADDITIONS = {
    'orphan': '''\nclass Store(Store):
    def commit_job(self, job_id, epoch, *, fail_before_commit=False):
        from library.common import LibraryError
        import hashlib
        try:
            return super().commit_job(job_id, epoch, fail_before_commit=fail_before_commit)
        except LibraryError as error:
            if error.code == 'injected_failure':
                raw = b'unreferenced-after-rollback'
                self.db.execute('INSERT INTO blobs VALUES (?,?)', ('blob-' + hashlib.sha256(raw).hexdigest(), raw))
            raise
''',
    'receipt-alias': '''\nclass Store(Store):
    def commit_job(self, job_id, epoch, *, fail_before_commit=False):
        value = super().commit_job(job_id, epoch, fail_before_commit=fail_before_commit)
        if type(value) is not dict or 'documents' not in value:
            return value
        if not hasattr(self, '_qualification_receipt_alias'):
            self._qualification_receipt_alias = value
        return self._qualification_receipt_alias
''',
    'lifecycle-response': '''\nclass Store(Store):
    def lifecycle_show(self, document_id):
        value = dict(super().lifecycle_show(document_id))
        value['notes'] = 'qualification-corrupted-response'
        return value
''',
}


def controls() -> tuple[dict[str, Any], ...]:
    # Finite qualification controls, not substituted final-study history slots.
    return (
        {'id': 'P01', 'family': 'b01', 'case_id': 'rollback', 'variant': 'reference', 'expected_failed': []},
        {'id': 'D01', 'family': 'b01', 'case_id': 'rollback', 'variant': 'orphan',
         'expected_failed': ['after.persisted-state', 'reopened.persisted-state']},
        {'id': 'P02', 'family': 'b02', 'case_id': 'fresh-value-commit_job', 'variant': 'reference', 'expected_failed': []},
        {'id': 'D02', 'family': 'b02', 'case_id': 'fresh-value-commit_job', 'variant': 'receipt-alias',
         'expected_failed': ['after.result.2']},
        {'id': 'P03', 'family': 'm2-direct-api', 'case_id': 'm2-competing-connections', 'variant': 'reference', 'expected_failed': []},
        {'id': 'D03', 'family': 'm2-direct-api', 'case_id': 'm2-competing-connections', 'variant': 'lifecycle-response',
         'expected_failed': ['m2-competing-connections:observation-003']},
    )


def candidate_files(variant: str) -> tuple[dict[str, bytes], dict[str, Any]]:
    inputs = corrected_v2_source_inputs()
    texts, binaries = corrected_v2_files(), corrected_v2_binary_files()
    if set(texts) & set(binaries):
        raise ValueError('Source path collision')
    original = {name: value.encode('utf-8') for name, value in texts.items()} | binaries
    files = dict(original)
    mutation = None
    if variant != 'reference':
        addition = ADDITIONS[variant].encode('utf-8')
        files[SOURCE_PATH] += addition
        ast.parse(files[SOURCE_PATH], filename=SOURCE_PATH)
        mutation = {'path': SOURCE_PATH, 'operation': 'append_exact_bytes', 'addition': addition.decode(),
                    'before_sha256': storage_execution.sha(original[SOURCE_PATH]),
                    'after_sha256': storage_execution.sha(files[SOURCE_PATH])}
    return files, {'variant': variant, 'generator_inputs': inputs,
        'original_source_manifest': admission.source_manifest(original),
        'source_manifest': admission.source_manifest(files), 'mutation': mutation,
        'authored_candidate_fixture': True, 'scientific_samples': 0}


def write_new(path: Path, value: Any) -> bytes:
    raw = storage_execution.encoded(value)
    write_raw(path, raw)
    return raw


def write_raw(path: Path, raw: bytes) -> None:
    with path.open('xb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


def schema_fixture(files: dict[str, bytes], *, m2_case: str | None = None) -> tuple[bytes, dict[str, Any]]:
    # Only fixed trusted literal SQL from authored fixture generators is executed.
    # Source-specific equality prevents promoting another candidate's schema.
    raw, record = authored_schema_profile()
    for name, pin in record['reviewed_sources'].items():
        if storage_execution.sha(files[name]) != pin:
            raise ValueError('Declared fixture schema source differs: ' + name)
    if m2_case is not None:
        # This selected schema0 migration preserves the seed's four base tables.
        # Rebuilding them from _BASE would impose the fresh-store representation
        # and falsely reject legal persisted SQL spelling/FK differences.
        if m2_case != 'm2-competing-connections':
            raise ValueError('Only the reviewed schema0 qualification migration is declared')
        seed = m2_profile.fixture_files(m2_case)['seed.sqlite']
        statements = record['statements'][4:]
        with tempfile.TemporaryDirectory(prefix='authored-m2-migration-schema-') as directory:
            path = Path(directory) / 'schema.sqlite'
            path.write_bytes(seed)
            connection = sqlite3.connect(path)
            try:
                before = list(connection.execute("SELECT name,sql FROM sqlite_schema WHERE type='table' ORDER BY name"))
                if [row[0] for row in before] != ['blobs', 'documents', 'jobs', 'metadata']:
                    raise ValueError('Trusted schema0 seed table census changed')
                for statement in statements:
                    connection.execute(statement)
                connection.commit()
                after = dict(connection.execute("SELECT name,sql FROM sqlite_schema WHERE type='table' ORDER BY name"))
                if any(after[name] != sql for name, sql in before) or len(after) != 14:
                    raise ValueError('Declared migration schema did not preserve original four tables')
            finally:
                connection.close()
            raw = path.read_bytes()
        record = {**record, 'purpose': 'authored-schema0-to-final-m4-sqlite-schema-qualification-only',
            'schema_sha256': mapper.sqlite_schema_sha256(raw), 'statements': statements,
            'trusted_seed_sha256': storage_execution.sha(seed), 'preserved_seed_tables': before,
            'migration_source': 'library/catalog/m4_store.py:NormalizedStore._migrate_storage schema==0',
            'seed_generator_source_sha256': storage_execution.sha(Path(m2_profile.native.__file__).read_bytes()),
            'scope': 'Schema construction only; this database is not an actual candidate migration or accepted state'}
    return raw, record


def plan_for(row: dict[str, Any], files: dict[str, bytes], commit: str, tree: str, schema: bytes) -> Any:
    common = (row['family'], admission.source_sha256(files))
    if row['family'] == 'm2-direct-api':
        from gossip_harness import candidate_m2_product_observation_v1 as m2_observer
        m2_value = m2_profile.profile_for(row['case_id'], PURPOSE)
        paths = tuple('m2/' + name.replace('catalog.sqlite', 'library.sqlite') for name in mapper.V2_STORAGE_PATHS)
        return m2_review.LayoutPlan(*common, m2_profile.source_sha256(files), commit, tree, row['case_id'],
            m2_value.sha256, 'reviewed-m2-final-sqlite-v1', paths, m2_observer.sqlite_schema_sha256(schema))
    value = storage_profile.profile_for(row['family'], row['case_id'], PURPOSE)
    native = storage_execution.b01 if row['family'] == 'b01' else storage_execution.b02
    return storage_review.LayoutPlan(*common, native.source_sha256(files), commit, tree, row['case_id'],
        value.sha256, mapper.V2_SQLITE_LAYOUT, mapper.V2_STORAGE_PATHS, mapper.sqlite_schema_sha256(schema))


def prepare(root: Path) -> dict[str, Any]:
    """Create exclusive inert Git/source/request bundle; never approval or execution."""
    root.mkdir()
    rows = []
    for row in controls():
        directory = root / row['id']; directory.mkdir()
        files, provenance = candidate_files(row['variant'])
        store = make_store(directory / 'candidate.git', files)
        commit = store.head()
        tree, actual = storage_execution.capture_source(store, commit, policy=BatchCapturePolicy())
        if actual != files:
            raise ValueError('Git source differs from authored bytes')
        source = directory / 'source'; source.mkdir()
        for name, raw in sorted(files.items()):
            path = source / name; path.parent.mkdir(parents=True, exist_ok=True); write_raw(path, raw)
        schema, schema_record = schema_fixture(files, m2_case=row['case_id'] if row['family'] == 'm2-direct-api' else None)
        write_raw(directory / 'schema.sqlite', schema)
        write_new(directory / 'schema-profile.json', schema_record)
        plan = plan_for(row, files, commit, tree, schema)
        request = write_new(directory / 'request.json', plan.request())
        write_new(directory / 'source-provenance.json', provenance)
        from gossip_harness import candidate_storage_product_observation_v1 as so
        from gossip_harness import candidate_m2_product_observation_v1 as mo
        selectors = (mo.selector_catalog(row['case_id'], purpose=PURPOSE) if row['family'] == 'm2-direct-api' else
            so.selector_catalog(row['family'], row['case_id'], purpose=PURPOSE, capture_policy=BatchCapturePolicy()))
        write_new(directory / 'selectors.json', selectors)
        metadata = {**row, 'plan': asdict(plan), 'request_sha256': storage_execution.sha(request),
            'source_sha256': admission.source_sha256(files), 'commit_oid': commit, 'tree_oid': tree,
            'schema_file_sha256': storage_execution.sha(schema)}
        write_new(directory / 'control.json', metadata)
        rows.append(metadata)
    census = {'protocol': PROTOCOL, 'purpose': PURPOSE, 'controls': rows,
        'authored_candidate_fixture': True, 'scientific_samples': 0, 'whole_project_acceptance': False,
        'physical_executions': 0, 'approval_supplied': False,
        'review_package_required': 'independently delivered report.json and delivery.json for each exact request',
        'negative_controls': ['N01 revoked admission before intent (fresh no-dispatch attempt)',
                              'N02 authenticated-original corruption (parser reread of P01 original; no new execution)'],
        'source_capture': BatchCapturePolicy().record()}
    write_new(root / 'census.json', census)
    return census


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    print(json.dumps(prepare(args.output.resolve()), sort_keys=True, indent=2))
