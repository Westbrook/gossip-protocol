"""Prospective inherited M2 direct-API histories on final M4 candidate source.

Native definitions are public development material. Fresh independent-purpose
execution does not make them held out. This profile supplies finite assertions,
not full-clause semantic approval or coverage of the V2 amendments.
"""
from __future__ import annotations

import ast
import base64
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
from typing import Any

from . import library_m2_acceptance_cases_v1 as native
from . import library_v2_inherited_cases_v1 as inherited
from . import candidate_observation_admission_v1 as admission
from .candidate_storage_product_profile_v1 import encoded, decode, digest
from . import project_acceptance_registry_v1 as registry
from . import cumulative_finite_mapping_v1 as finite

PROTOCOL = 'candidate-m2-product-profile-v1-ascii-json-v1'
MAPPED_PROTOCOL = PROTOCOL + '-' + finite.M2_MAPPING
ADAPTER_PROTOCOL = 'candidate-m2-action-adapter-v1'
ORIGINAL_DEFINITION_PURPOSE = native.PURPOSE
TARGET_CONTRACT = inherited.CONTRACT_SHA256
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
LIMITATIONS = tuple(native.LIMITATIONS) + (
    'Native original purpose independent_acceptance is disclosed; earlier executions were authored_reference_qualification.',
    'These published finite histories have no held-out-family or full source-unit adequacy status.',
    'The exact M2 inputs/expectations are inherited unchanged into V2 final M4; new M4/V2 obligations stay mandatory.',
    'All 84 unobserved setup actions remain in dispatch and mechanics; their return values are diagnostic only.',
    'Only independently reviewed SQLite paths/schema may supply the one original job-serialization observation.',
    'Paused snapshots follow action output; they are not atomic method-return or pre-commit crash boundaries.',
    'Candidate-collocated API output is data, not an authority; external original capture proves provenance, not adversarial instrumentation isolation.',
)


def source_sha256(files: dict[str, bytes]) -> str:
    return digest({'protocol': native.PROTOCOL, 'files': admission.source_manifest(files)})


def case_definition(case_id: str) -> dict[str, Any]:
    # The inherited factory authenticates frozen source and the complete corpus.
    cases = inherited.acceptance_cases('m2')
    admission.require(cases == native.acceptance_cases(), 'Unexpected inherited M2 compatibility change')
    found = [case for case in cases if case['id'] == case_id]
    admission.require(len(found) == 1, 'Closed M2 case required')
    return found[0]


def recipe_for(case_id: str) -> dict[str, Any]:
    return case_definition(case_id)['input']


def phases_for(case_id: str) -> tuple[str, ...]:
    return tuple('action-%03d' % i for i, _ in enumerate(recipe_for(case_id)['actions']))


def _native_function(name: str) -> ast.FunctionDef:
    functions = [node for node in ast.parse(native.CHILD_ADAPTER).body
                 if isinstance(node, ast.FunctionDef) and node.name == name]
    admission.require(len(functions) == 1, 'Frozen helper function absent')
    return functions[0]


def fixture_files(case_id: str) -> dict[str, bytes]:
    """Run only frozen evaluator fixture functions, before any candidate import.

    The candidate module imports and native action loop are deliberately absent
    from this AST. SQLite byte output is retained in the prospective helper
    manifest; no candidate-provided database is used to seed its own test.
    """
    fixture = recipe_for(case_id)['fixture']
    namespace: dict[str, Any] = {'hashlib': hashlib, 'sqlite3': sqlite3}
    module = ast.Module(body=[_native_function('identities'), _native_function('seed_database')], type_ignores=[])
    exec(compile(module, '<frozen-m2-fixture-functions>', 'exec', dont_inherit=True), namespace)
    with tempfile.TemporaryDirectory(prefix='m2-authored-seed-') as directory:
        path = Path(directory) / 'seed.sqlite'
        namespace['seed_database'](path, fixture)
        return {'seed.sqlite': path.read_bytes()}


def input_fixtures(case_id: str) -> list[dict[str, Any]]:
    result = []
    for row in recipe_for(case_id)['files']:
        if 'symlink' in row:
            item = {'kind': 'symlink', 'path': row['path'], 'target': row['symlink']}
        else:
            raw = (bytes.fromhex(row['hex']) if 'hex' in row else
                (row['repeat'][0] * row['repeat'][1] if 'repeat' in row else row['text']).encode('utf-8'))
            item = {'kind': 'file', 'path': row['path'], 'bytes_base64': base64.b64encode(raw).decode('ascii')}
        result.append(item)
    return result


# Fixed observer code, never expected answers. Reusing only named frozen
# functions keeps unchanged invocation/error semantics without importing the
# candidate on the host. Fixture copying occurs before candidate imports.
_FUNCTIONS = '\n\n'.join(ast.unparse(_native_function(name)) for name in ('encode', 'expand', 'invoke'))
ADAPTER = '''import concurrent.futures
import json
from pathlib import Path
import shutil
import sys
import threading

payload = json.loads(Path('/checks/recipe.json').read_bytes())
base = Path('/tmp/m2'); base.mkdir()
db = base / 'library.sqlite'
shutil.copyfile('/checks/seed.sqlite', db)
root = Path('/inputs')
sys.path.insert(0, '/workspace')
from library.catalog.store import Store
from library.common import LibraryError
from library.ingestion.jobs import JobManager
from library.ingestion.local import import_file
from library.query.service import Service
''' + _FUNCTIONS + '''
store = Store(db)
try:
    for index, action in enumerate(payload['actions']):
        phase = 'action-%03d' % index
        if sys.stdin.buffer.readline(128) != (phase + '\\n').encode('ascii'):
            raise ValueError('Exact ordered action request required')
        op = action['op']
        if op == 'call':
            result = invoke(action, store, root)
        elif op == 'reopen':
            store.close(); store = Store(db); result = {'reopened': True}
        elif op == 'job_serialization':
            result = {'raw_capture_required': True}
        elif op == 'fresh':
            prior = invoke(action['call'], store, root); node = prior
            for component in action['path'][:-1]:
                node = node[component]
            node[action['path'][-1]] = action['value']
            result = invoke(action['call'], store, root)
        elif op == 'race':
            barrier = threading.Barrier(2, timeout=5)
            def competitor(_):
                own = Store(db)
                try:
                    barrier.wait()
                    return invoke(action['call'], own, root)
                finally:
                    own.close()
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                result = sorted(pool.map(competitor, range(2)), key=encode)
        else:
            raise ValueError('Closed action operation required')
        print(encode({'phase': phase, 'value': {'action_index': index, 'result': result}}), flush=True)
    if sys.stdin.buffer.readline(128) != b'finish\\n':
        raise ValueError('Exact finish request required')
finally:
    store.close()
'''


def adapter_files(case_id: str) -> dict[str, bytes]:
    return {'m2_adapter.py': ADAPTER.encode('utf-8'), 'recipe.json': encoded(recipe_for(case_id)),
            **fixture_files(case_id)}


# Independently mapped finite original observation positions. These are review
# inputs with clause/subfacet limits, not claims every clause is satisfied.
_UNIT_NAMES = {'ID': 'IDENTITY-REVISIONS', 'R': 'REFRESH', 'A': 'ANNOTATIONS',
               'C': 'COLLECTIONS', 'D': 'DELETE-RESTORE', 'Q': 'QUERY', 'I': 'INTERFACES'}
_FACETS: dict[str, tuple[tuple[tuple[int, ...], str, str], ...]] = {
    'm2-migrate-aba-receipt': (
        ((0, 1), 'ID0', 'Finite defaults and source identity after portable migration.'),
        ((2, 3, 4, 5), 'ID1 ID2 R2', 'Changed refresh, ABA token refusal and declared no-op results.'),
        ((6, 15, 16, 17), 'D0 ID2', 'Exact delete/restore token responses.'),
        ((7, 8), 'R3', 'Tombstone import and this replay; content returned to original before replay.'),
        ((9, 10, 11, 12, 14), 'D1', 'Declared tombstone visibility and history projections.'),
        ((18,), 'Q1 Q3', 'Restored listing and this generation value.'),
        ((19,), 'R3', 'Exact stored manifest/content_hashes/receipt strings from reviewed external capture.')),
    'm2-normalized-membership-tombstone': (
        ((0, 1, 18, 19, 20, 21), 'C1 C2', 'Declared normalized mutation/no-op/generation ordering results.'),
        ((2, 3, 4, 5, 6, 13, 14, 17), 'A0 A1 A2', 'Exact normalization/replacement/error results.'),
        ((7, 8), 'Q0', 'Notes/tags excluded from these legacy queries.'),
        ((9, 15), 'Q1', 'Declared filter and page values.'),
        ((10, 16), 'D0', 'Declared delete/restore responses.'),
        ((11, 12), 'C0 C1', 'Tombstone membership count and removal refusal.'),
        ((22,), 'A2 D1', 'Unchanged revision history after annotations and tombstone sequence.')),
    'm2-query-generation-pages': (
        ((0, 1, 2, 3, 4, 10, 11, 12, 13, 14, 15, 16), 'Q1 Q2', 'Finite filter/total/page/generation validation results; no transaction-snapshot proof.'),
        ((5,), 'ID1 ID2 R2', 'Declared changed refresh result.'),
        ((6,), 'Q2', 'Exact stale-generation response.'),
        ((7,), 'ID2', 'Annotation token increments once here.'),
        ((8, 9), 'Q0 Q1', 'Current-text haystack and finite listing.'),
        ((17,), 'ID4', 'Mutating returned tags does not change next returned value.')),
    'm2-competing-connections': (
        ((0,), 'ID2 ID4 A2', 'Two overlapping connections return one updated and one stale result; schedule not forced.'),
        ((1,), 'Q3', 'Exactly one catalog generation increment in this run.'),
        ((3,), 'ID1 A2', 'Finite persistence after same-process close/open, not process crash.')),
    'm2-revision-boundary': (
        ((0, 3, 6), 'ID1 ID3', 'Exact declared retained sixteen revisions and reopen projection.'),
        ((1,), 'ID3', 'Declared revision-capacity refusal.'),
        ((2,), 'ID1 ID2 R2', 'Unchanged text remains no-op at capacity.')),
    'm2-document-capacity-tombstones': (
        ((0,), 'D0 ID2', 'One tombstone/token response.'),
        ((1,), 'ID3 D1', 'Deleted document still occupies declared capacity.'),
        ((2,), 'ID3 Q1', 'Total256 with selected tombstone.'),
        ((3,), 'D1 Q0', 'Legacy active listing has255 entries.'),
        ((4,), 'D1', 'Declared tombstone remains addressable.')),
    'm2-retained-blob-capacity': (
        ((0,), 'ID3', 'Declared distinct retained-byte capacity refusal.'),
        ((1,), 'ID4', 'Exact current value after this refusal.'),
        ((2,), 'ID1 ID3 R2', 'Refresh reuses an already retained blob.'),
        ((3,), 'ID1', 'Exact retained history; all four digests computed on host from full values.')),
    'm2-refresh-input-boundaries': (
        ((0,), 'R0', 'Declared identity-preserving path refresh.'),
        ((1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 'R1', 'This authored path/file error only; not every path class.'),
        ((11, 12, 13), 'R0 R2', 'Declared input shape/token response.'),
        ((14, 15), 'R0 R1', 'Declared UTF-8 byte/surrogate rejection.'),
        ((16, 17), 'ID1 ID4', 'Final record/history conservation after this sequence, not each-failure atomicity.')),
    'm2-annotation-input-boundaries': (
        (tuple(range(10)), 'A0 A1', 'Exact authored type/name/count/UTF-8 boundary.'),
        ((10,), 'A0 A2', 'Final unchanged record after this sequence.'),
        ((11,), 'Q3', 'Generation remains zero after the declared refusals.')),
    'm2-collection-boundary': (
        ((0,), 'C0 C1', 'Declared capacity64 refusal.'),
        ((1,), 'C1 C2', 'Normalized no-op at capacity.'),
        ((2, 3), 'C1 C2', 'Effective remove/create responses.'),
        ((4,), 'C0 C2 Q3', 'Exact sorted64 names/counts/generation66.')),
    'm2-service-route-contract': (
        (tuple(range(9)) + (17,), 'I0 I1', 'Returned Python route result only, no wire/CLI/browser authority.'),
        ((9, 15), 'D0 I0', 'Returned direct-routing delete/restore value.'),
        ((10, 11, 12, 13, 16), 'I1', 'Direct Python error/status value mapping.'),
        ((12,), 'D1', 'Direct route tombstone exclusion.'),
        ((14,), 'Q2 I1', 'Direct route stale-generation response.')),
    'm2-batch-generation-replay': (
        ((0, 1), 'Q3', 'Job-only transitions leave catalog generation zero.'),
        ((2,), 'ID0', 'Finite completed-receipt identity/shape.'),
        ((3,), 'ID0 Q1 Q3', 'Two defaults and one generation increment.'),
        ((4,), 'R3', 'Exact declared receipt replay value.'),
        ((5,), 'Q3', 'Replay and empty batch leave generation one.')),
}


# MAP-B is a closed prospective mapping, never a mutation of the original table
# or its twelve histories. Each replacement below names the actual observation.
_MAPPED_FACETS = {
    **_FACETS,
    'm2-migrate-aba-receipt': _FACETS['m2-migrate-aba-receipt'] + (
        ((12,), 'ID1 ID3', 'Exact three-entry ABA revision history before reopen; this target history only, not a global retained-blob census.'),
        ((14,), 'ID1', 'Exact reopened tombstone current record; no reread of complete revision history.')),
    'm2-normalized-membership-tombstone': (
        ((0, 1, 18, 19, 20, 21), 'C1 C2', 'Declared normalized mutation/no-op/generation ordering results.'),
        ((2, 3, 4, 17), 'A0 A1 A2', 'Exact authored annotation replacement, normalized no-op or duplicate-normalized-name rejection.'),
        ((5,), 'A2', 'Exact stale_version for the old token before missing-collection lookup; no normalization or conservation inferred.'),
        ((6,), 'A2', 'Exact collection_not_found with the current token; no normalization or conservation inferred.'),
        ((13,), 'A2', 'Exact document_deleted with the current tombstone token; no normalization or conservation inferred.'),
        ((14,), 'A2', 'Exact stale_version for the old tombstone token; no normalization or conservation inferred.'),
        ((7, 8), 'Q0', 'Notes/tags excluded from these legacy queries.'),
        ((9, 15), 'Q1', 'Declared filter and page values.'),
        ((10, 16), 'D0', 'Declared delete/restore responses.'),
        ((11, 12), 'C0 C1', 'Tombstone membership count and removal refusal.'),
        ((22,), 'A2 D1 ID1', 'Exact unchanged one-entry revision history after this annotation/tombstone sequence; no per-refusal atomicity claim.')),
    'm2-query-generation-pages': (
        ((0, 1, 2, 3, 4, 10, 11, 12), 'Q1 Q2', 'Finite filter/total/page/generation validation results; no transaction-snapshot proof.'),
        ((13,), 'Q1', 'Exact invalid_request for boolean offset; no generation-fence observation.'),
        ((14,), 'Q1', 'Exact invalid_request for limit zero; no generation-fence observation.'),
        ((15,), 'Q1', 'Exact invalid_request for the 257-character query; no generation-fence observation.'),
        ((16,), 'Q1', 'Exact invalid_request for unknown deleted filter; no generation-fence observation.'),
        ((5,), 'ID1 ID2 R2', 'Declared changed refresh result.'),
        ((6,), 'Q2', 'Exact stale-generation response.'),
        ((7,), 'ID2', 'Annotation token increments once here.'),
        ((8,), 'Q0', 'Exact empty legacy listing for old text retained only in history/notes.'),
        ((9,), 'Q1', 'Exact empty lifecycle listing and generation2 for that old-text query.'),
        ((17,), 'ID4', 'Mutating returned tags does not change next returned value.')),
    'm2-revision-boundary': (
        ((0,), 'ID1', 'Current head has text16, revision16 and token16 after the fifteen setup refreshes; no retained-history enumeration.'),
        ((3,), 'ID1 ID3', 'Exact sixteen-entry ordered revision history with full text/blob identities before reopen.'),
        ((6,), 'ID1', 'Reopened current head has text16, revision16 and token16; no complete history reread or process restart.'),
        ((1,), 'ID3', 'Declared revision-capacity refusal.'),
        ((2,), 'ID1 ID2 R2', 'Unchanged text remains no-op at capacity.'),
        ((4,), 'Q3', 'Exact lifecycle listing has catalog generation15 after fifteen effective refreshes, capacity refusal and no-op.')),
    'm2-retained-blob-capacity': _FACETS['m2-retained-blob-capacity'] + (
        ((3,), 'ID3', 'Exact two-entry retained history of this target document, host-hashed from the full returned value; no global blob or receipt-only retention census.'),),
    'm2-refresh-input-boundaries': (
        ((0,), 'R0', 'Declared identity-preserving path refresh.'),
        ((1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 'R1', 'This authored path/file error only; not every path class.'),
        ((11, 12, 13), 'R0 R2', 'Declared input shape/token response.'),
        ((14,), 'R0', 'Exact too_large for direct text containing 16385 multibyte characters; no file/path decoding observation.'),
        ((15,), 'R0', 'Exact invalid_utf8 for direct text containing an unencodable surrogate; no file/path decoding observation.'),
        ((16, 17), 'ID1 ID4', 'Final record/history conservation after this sequence, not each-failure atomicity.')),
    'm2-service-route-contract': (
        ((0, 3, 4, 5, 6, 7, 8, 17), 'I0 I1', 'Returned Python route syntax/error/status result only, no wire/CLI/browser authority.'),
        ((1,), 'I0', 'Exact successful direct Python lifecycle-show route value; no syntax/error validation inferred.'),
        ((2,), 'I0', 'Exact successful direct Python revision-history route value; no syntax/error validation inferred.'),
        ((9, 15), 'D0 I0', 'Returned direct-routing delete/restore value.'),
        ((10,), 'I1 R2', 'Exact direct Python route [409, stale_version] for refresh with the old tombstone token.'),
        ((11,), 'I1 R2', 'Exact direct Python route [409, document_deleted] for refresh with the current tombstone token.'),
        ((12, 13, 16), 'I1', 'Direct Python error/status value mapping.'),
        ((12,), 'D1', 'Direct route tombstone exclusion.'),
        ((14,), 'Q2 I1', 'Direct route stale-generation response.')),
}


def _facets(case_id: str, observation_index: int, mapping_profile: str | None = None) -> list[dict[str, str]]:
    result = []
    table = _FACETS if mapping_profile is None else _MAPPED_FACETS
    for indices, tokens, rationale in table[case_id]:
        if observation_index in indices:
            for token in tokens.split():
                alias = token[:-1]
                result.append({'source_unit_id': 'M2-' + _UNIT_NAMES[alias] + ':clause:' + token[-1],
                               'rationale': rationale})
    return result


@dataclass(frozen=True, slots=True)
class M2Profile:
    case_id: str
    purpose: str

    @property
    def mapping_profile(self) -> str | None:
        return None

    @property
    def family(self) -> str:
        return 'm2-direct-api'

    @property
    def phases(self) -> tuple[str, ...]:
        return phases_for(self.case_id)

    @property
    def requirement_ids(self) -> tuple[str, ...]:
        native_ids = case_definition(self.case_id)['requirement_ids']
        mapped_ids = [facet['source_unit_id'].split(':')[0] for row in self.selectors() for facet in row['source_unit_facets']]
        return tuple(dict.fromkeys(native_ids + mapped_ids))

    @property
    def ordered_case_ids(self) -> tuple[str, ...]:
        return tuple(row['case_id'] for row in self.selectors())

    @property
    def diagnostic_case_ids(self) -> tuple[str, ...]:
        return tuple(self.case_id + ':' + phase for phase in self.phases)

    def selectors(self) -> list[dict[str, Any]]:
        output = []
        observation_index = 0
        for index, action in enumerate(recipe_for(self.case_id)['actions']):
            if not action.get('observe', True):
                continue
            output.append({'case_id': self.case_id + ':observation-%03d' % observation_index,
                'action_index': index, 'observation_index': observation_index,
                'pointer': '/projection/observations/' + str(observation_index) + '/disposition',
                'source_unit_ids': list(dict.fromkeys(row['source_unit_id'] for row in _facets(self.case_id, observation_index, self.mapping_profile))),
                'source_unit_facets': _facets(self.case_id, observation_index, self.mapping_profile),
                'evidence_kind': 'captured_sqlite' if action['op'] == 'job_serialization' else 'direct_api',
                'comparison': 'host_canonical_sha256' if action.get('digest') else 'exact_typed_json',
                'rationale': 'Exact result of this declared finite action only; no whole-clause, wire, browser, forced schedule or crash credit.',
                'semantically_reviewed': False})
            observation_index += 1
        return output

    def record(self) -> dict[str, Any]:
        case = case_definition(self.case_id)
        result = {'protocol': PROTOCOL, 'family': self.family, 'case_id': self.case_id,
            'purpose': self.purpose, 'original_definition_purpose': ORIGINAL_DEFINITION_PURPOSE,
            'previous_execution_purpose': inherited.PURPOSE, 'original_contract_sha256': native.CONTRACT_SHA256,
            'target_contract_sha256': TARGET_CONTRACT, 'target_milestone': 'M4',
            'compatibility': 'exact unchanged inherited M2 definition; no newly added obligation discharged',
            'definition': case, 'definition_sha256': digest(case),
            'adapter_protocol': ADAPTER_PROTOCOL, 'adapter_sha256': hashlib.sha256(ADAPTER.encode()).hexdigest(),
            'native_adapter_sha256': hashlib.sha256(native.CHILD_ADAPTER.encode()).hexdigest(),
            'native_definition_sha256': inherited.PRIOR_SOURCE_SHA256['library_m2_acceptance_cases_v1.py'],
            'inherited_factory_sha256': hashlib.sha256(Path(inherited.__file__).read_bytes()).hexdigest(),
            'ordered_actions': list(self.phases), 'selectors': self.selectors(),
            'requirement_ids': list(self.requirement_ids), 'limitations': list(LIMITATIONS),
            'independent_semantic_scope_review_supplied': False, 'whole_project_acceptance': False}
        if self.mapping_profile is not None:
            result.update(protocol=MAPPED_PROTOCOL, mapping_profile=self.mapping_profile,
                original_requirement_ids=list(case['requirement_ids']), mapping_sources=finite.sources(),
                mapping_scope='Selected MAP-B per-selector precision and existing-observation owner joins only; no shared constants or V2 amendment credit.')
        return result

    @property
    def sha256(self) -> str:
        return digest(self.record())


@dataclass(frozen=True, slots=True)
class MappedM2Profile(M2Profile):
    """Exact named opt-in; legacy dataclass fields and bytes stay unchanged."""

    @property
    def mapping_profile(self) -> str:
        return finite.M2_MAPPING


def accepted_profile(value: Any) -> bool:
    return type(value) in (M2Profile, MappedM2Profile)


def profile_for(case_id: str, purpose: str = 'public_release', *, mapping_profile: str | None = None) -> M2Profile:
    admission.require(mapping_profile is None or (type(mapping_profile) is str and mapping_profile == finite.M2_MAPPING),
                      'Closed M2 mapping profile required')
    admission.require(purpose in registry.PURPOSES, 'Prospective product purpose required')
    case_definition(case_id)
    return M2Profile(case_id, purpose) if mapping_profile is None else MappedM2Profile(case_id, purpose)


def reconstruct(value: M2Profile) -> M2Profile:
    admission.require(accepted_profile(value), 'Exact named M2 profile required')
    return profile_for(value.case_id, value.purpose, mapping_profile=value.mapping_profile)


def exact(expected: Any, actual: Any) -> bool:
    if type(expected) is not type(actual):
        return False
    if type(expected) is dict:
        return expected.keys() == actual.keys() and all(exact(value, actual[key]) for key, value in expected.items())
    if type(expected) is list:
        return len(expected) == len(actual) and all(exact(a, b) for a, b in zip(expected, actual, strict=True))
    return bool(expected == actual)


def project(value: M2Profile, responses: dict[int, Any], sqlite_values: dict[int, Any]) -> dict[str, Any]:
    admission.require(accepted_profile(value) and value == reconstruct(value), 'Exact profile required')
    case = case_definition(value.case_id)
    rows, diagnostics = [], []
    observation_index = 0
    for index, action in enumerate(case['input']['actions']):
        available = index in responses
        actual = responses.get(index)
        diagnostic = {'action_index': index, 'observed': action.get('observe', True), 'available': available,
            'value': actual if available else None, 'disposition': 'unspecified' if available else 'unavailable'}
        if action.get('observe', True):
            if action['op'] == 'job_serialization':
                available = available and exact(actual, {'raw_capture_required': True}) and index in sqlite_values
                actual = sqlite_values.get(index)
            elif action.get('digest') and available:
                actual = {'sha256': digest(actual)}
            expected = case['expected']['observations'][observation_index]
            passed = None if not available else exact(expected, actual)
            disposition = 'unavailable' if passed is None else ('pass' if passed else 'fail')
            rows.append({'action_index': index, 'observation_index': observation_index,
                'case_id': value.ordered_case_ids[observation_index], 'disposition': disposition,
                'actual': actual if available else None})
            diagnostic['disposition'] = disposition
            observation_index += 1
        diagnostics.append(diagnostic)
    return {'observations': rows, 'diagnostics': diagnostics,
        'known_failed': [row['case_id'] for row in rows if row['disposition'] == 'fail'],
        'unavailable': [row['case_id'] for row in rows if row['disposition'] == 'unavailable']}
