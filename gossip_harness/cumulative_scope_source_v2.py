"""Versioned full-denominator factory extension for original storage selectors.

Existing closed CLI/HTTP/process factories retain their exact declarations. New
storage declarations describe finite authored facets for independent review;
they do not grant layout review, applicability, execution or semantic authority.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, fields
import json
from pathlib import Path
from typing import Any

from . import cumulative_scope_source_v1 as previous
from . import project_acceptance_compiler_v1 as compiler
from . import project_acceptance_registry_v1 as registry
from . import candidate_observation_admission_v1 as admission

PROTOCOL = 'cumulative-scope-source-v2'
LOADED_SOURCE_SHA256 = previous.sha(Path(__file__).read_bytes())
ScopeSourceError = previous.ScopeSourceError
SourceCatalog = previous.SourceCatalog
SourceUnit = previous.SourceUnit
Selector = previous.Selector
AssertionDeclaration = previous.AssertionDeclaration
require, encoded, sha = previous.require, previous.encoded, previous.sha
load_catalog = previous.load_catalog
scope_review_input = previous.scope_review_input
STORAGE_LANES = ('intake', 'jobs', 'durability')


def _values(value: previous.ExecutableSlice) -> dict[str, Any]:
    return {field.name: getattr(value, field.name) for field in fields(previous.ExecutableSlice)}


@dataclass(frozen=True, slots=True)
class ExecutableSlice(previous.ExecutableSlice):
    @property
    def sha256(self) -> str:
        return sha(encoded({'protocol': PROTOCOL, 'slice': asdict(self)}))

    def compiler_records(self, *, suite_id: str, physical_slot: str) -> tuple[compiler.SuiteDefinition, compiler.ExecutionGate, tuple[compiler.CoverageEdge, ...]]:
        if self.family not in ('storage-b01', 'storage-b02'):
            return previous.ExecutableSlice(**_values(self)).compiler_records(suite_id=suite_id, physical_slot=physical_slot)
        registry.identifier(suite_id)
        registry.identifier(physical_slot)
        logical = tuple(dict.fromkeys(key for item in self.assertions for key in item.assertion.logical_gate_ids))
        binding = self.gate.binding
        suite = compiler.SuiteDefinition(suite_id, self.gate.ordered_case_ids, binding.ordered_suite_sha256,
            self.definition_sha256, self.original_definition_purpose, binding.purpose,
            self.source_contract_sha256, binding.evaluator_sha256, True, True, STORAGE_LANES)
        execution = compiler.ExecutionGate(self.gate.gate_id, physical_slot, suite_id, 'product', logical,
            binding.runtime_image_sha256, binding.environment_sha256, binding.limits_sha256,
            binding.seed_sha256, binding.execution_protocol)
        edges = tuple(compiler.CoverageEdge(item.obligation_id, item.assertion.id,
            self.gate.gate_id, item.case_id, item.observation_pointer) for item in self.assertions)
        return suite, execution, edges


def cli_slice(registration: Any) -> ExecutableSlice:
    return ExecutableSlice(**_values(previous.cli_slice(registration)))


def http_slice(registration: Any, profile: Any, policy: Any) -> ExecutableSlice:
    return ExecutableSlice(**_values(previous.http_slice(registration, profile, policy)))


def product_process_slice(registration: Any, profile: Any, policy: Any) -> ExecutableSlice:
    return ExecutableSlice(**_values(previous.product_process_slice(registration, profile, policy)))


def storage_slice(registration: Any) -> ExecutableSlice:
    """Reconstruct prospective original selectors; no receipt/review is accepted here.

    Binding is an identity proposal only. The actual owner/recipe must separately
    authenticate Git and enrolled layout originals before dispatch. No candidate
    result, fabricated review callback or historical receipt is read by this API.
    """
    from . import candidate_storage_product_execution_v1 as execution
    from . import candidate_storage_product_observation_v1 as observer
    from . import candidate_storage_product_profile_v1 as profiles
    require(type(registration) is execution.StorageRegistration, 'Exact storage registration required')
    actual = execution.observation_registration(registration)
    binding = registration.binding
    value = profiles.profile_for(binding.family, binding.case_id, binding.purpose)
    capture_policy = execution.capture_policy_for(binding.protocol)
    record = observer.selector_catalog(binding.family, binding.case_id, purpose=binding.purpose,
        capture_policy=capture_policy)
    sources = execution.evaluator_sources()
    admission.verify_loaded_sources(sources)
    require(binding.definition_sha256 == execution.digest(value.record()) == record['definition_sha256']
        and binding.profile_sha256 == value.sha256 == record['profile_sha256']
        and binding.evaluator_sha256 == execution.digest(sources)
        and record['evaluator_sources'] == sources
        and tuple(record['ordered_case_ids']) == actual.gate.ordered_case_ids,
        'Storage definition/profile/evaluator/ordered roster differs')
    catalog = load_catalog(Path(__file__).resolve().parents[1])
    selectors, assertions = [], []
    definition = value.record()['original_definition']
    recipe = None if binding.family == 'b01' else profiles.b02.execution_recipe(binding.case_id)
    for index, row in enumerate(record['selectors']):
        mechanics = row['case_id'] == execution.mechanics_case_id(value)
        selectors.append(Selector(row['case_id'], row['observation_pointer'], row['applicability'],
            '/diagnostics' if mechanics else '/diagnostics/' + str(index), 'mechanics' if mechanics else 'semantic'))
        if mechanics or row['applicability'] != 'normative':
            continue
        for facet in row['source_unit_facets']:
            unit = catalog.unit(facet['source_unit_id'])
            require(facet['full_source_unit'] is False and facet['source_sha256'] == unit.obligation.source_sha256
                and facet['class'] in compiler.PRODUCT_KINDS and facet['lane'] == 'direct-api-storage',
                'Unsupported original storage source facet')
            # Wrong-kind policy does not prescribe io_error. Its incidental state
            # conservation cannot recover that deliberately unspecified label.
            if binding.case_id in profiles.b02.POLICY_LIMITED_CASE_IDS and unit.obligation.id == 'm1:M1-I28':
                continue
            result_index = None
            if row['case_id'].startswith('after.result.') and row['case_id'].removeprefix('after.result.').isdigit():
                result_index = int(row['case_id'].removeprefix('after.result.'))
            if result_index is not None and recipe is not None and recipe['phases']['after'][result_index].get('op') in ('bind', 'mutate'):
                continue  # Adapter-owned literal None, not a candidate semantic result.
            if binding.case_id == 'signatures-offline-hook' and row['case_id'] != 'after.result.0':
                continue
            if unit.obligation.id in ('m1:M1-I27', 'm1:M1-I28'):
                expected = None if result_index is None else definition['expected']['results']['after'][result_index]
                codes = ('invalid_archive', 'invalid_json') if unit.obligation.id == 'm1:M1-I27' else ('io_error',)
                if type(expected) is not dict or set(expected) != {'error'} or expected['error'] not in codes:
                    continue  # State conservation cannot prove an exact error classification.
            logical = tuple(gate.id for gate in catalog.inventory.logical_gates
                if gate.role == 'product' and gate.lane in STORAGE_LANES
                and set(unit.requirement_ids) & set(gate.requirement_ids) & set(value.requirement_ids))
            require(bool(logical), 'Authored facet has no supported storage logical lane')
            identifier = 'storage-' + sha(encoded([binding.family, binding.case_id, row['case_id'],
                row['observation_pointer'], facet]))[:24]
            assertions.append(AssertionDeclaration(unit.obligation.id,
                compiler.Assertion(identifier, facet['class'], logical), row['case_id'], row['observation_pointer'],
                'Authored finite B02 facet: ' + facet['scope'] + '. Exact check: ' + row['case_id']
                + '. This selector supplies only its declared response or paused persisted-state comparison; '
                'it does not prove every case label or the whole source unit. Independent relevance, '
                'M4 inheritance, layout completeness and lane adequacy remain review duties under the complete history conjunction. '
                'Store reopen is not process death; ordinary phase captures are not forced interleavings.'))
    factory_input = {'registration': asdict(registration)}
    if capture_policy is not None:
        factory_input['source_capture'] = capture_policy.record()
    return ExecutableSlice('storage-' + binding.family, binding.case_id, actual.gate,
        actual.definition_sha256, actual.original_definition_purpose, profiles.ORIGINAL_CONTRACT_SHA256,
        value.sha256, tuple(sorted(sources.items())), tuple(selectors), tuple(assertions),
        tuple(record['required_unfinished_coverage']) + (
            'B01 has no per-check source-unit mapping; its histories remain declared without semantic edges.',
            'Direct API storage supplies no HTTP, browser, source-inspection, workflow or process-restart authority.',),
        encoded(factory_input).decode())


def verify_slice(value: previous.ExecutableSlice) -> None:
    require(type(value) is ExecutableSlice, 'Exact version2 source-derived slice required')
    if value.family in ('cli', 'http', 'product-process'):
        previous.verify_slice(previous.ExecutableSlice(**_values(value)))
        return
    require(value.family in ('storage-b01', 'storage-b02'), 'Unknown version2 executable factory')
    from . import candidate_storage_product_execution_v1 as execution
    record = json.loads(value.factory_input_json)
    require(type(record) is dict and set(record) in ({'registration'}, {'registration', 'source_capture'}),
            'Unexpected storage factory input')
    row = record['registration']
    registration = execution.StorageRegistration(execution.StorageBinding(**row['binding']),
        row['commit_oid'], row['tree_oid'], row['repetition_id'], previous._gate(row['gate']),
        tuple(row['cohort_trajectory_ids']))
    require(storage_slice(registration) == value, 'Source-derived storage selectors/purpose were substituted')


def coverage_census(catalog: SourceCatalog, declaration: compiler.Declaration) -> dict[str, Any]:
    result = previous.coverage_census(catalog, declaration)
    result['protocol'] = PROTOCOL
    result['capacity_contract'] = registry.capacity_manifest(declaration.capacity_profile)
    return result

def assemble_declaration(catalog: SourceCatalog, cohort: compiler.CohortDesign,
                         slices: tuple[ExecutableSlice, ...], *, review_sha256: str,
                         purposes: tuple[compiler.PurposeAssignment, ...] = (),
                         compatibility: tuple[compiler.CompatibilityOverride, ...] = (),
                         additional_plans: tuple[compiler.ObligationPlan, ...] = (),
                         additional_suites: tuple[compiler.SuiteDefinition, ...] = (),
                         additional_gates: tuple[compiler.ExecutionGate, ...] = (),
                         additional_edges: tuple[compiler.CoverageEdge, ...] = (),
                         capacity_profile: str = registry.LEGACY_CAPACITY_PROFILE) -> compiler.Declaration:
    """Combine concrete reviewed component declarations without shrinking full scope.

    review_sha256 references an earlier independent component review, NOT this
    declaration's future complete scope review (which would create a hash cycle).
    No review authority follows from calling this structural constructor.
    Additional modules must supply actual source-bound selectors and get the
    later full semantic review; missing observations remain empty assertion plans.
    """
    registry.sha256(review_sha256)
    registry.capacity_manifest(capacity_profile)
    require(type(catalog) is SourceCatalog and type(cohort) is compiler.CohortDesign
            and type(slices) is tuple and all(type(row) is ExecutableSlice for row in slices),
            'Typed source catalog/cohort/physical slices required')
    require(len({row.gate.gate_id for row in slices}) == len(slices), 'Duplicate physical gate')
    groups: dict[str, list[compiler.Assertion]] = {unit.obligation.id: [] for unit in catalog.units}
    suites, gates, edges = list(additional_suites), list(additional_gates), list(additional_edges)
    for component in slices:
        verify_slice(component)
        suite, gate, component_edges = component.compiler_records(
            suite_id='suite-' + sha(component.gate.gate_id.encode())[:24], physical_slot=component.gate.gate_id)
        suites.append(suite); gates.append(gate); edges.extend(component_edges)
        valid_selectors = {(row.case_id, row.observation_pointer): row for row in component.selectors}
        for claim in component.assertions:
            require(claim.obligation_id in groups, 'Assertion targets foreign obligation')
            selected = valid_selectors.get((claim.case_id, claim.observation_pointer))
            require(selected is not None and selected.disposition == 'normative'
                    and selected.evidence_kind == 'semantic' and claim.case_id in component.gate.ordered_case_ids,
                    'Unspecified/unqualified/mechanics selector cannot mint semantic credit')
            if claim.assertion not in groups[claim.obligation_id]:
                groups[claim.obligation_id].append(claim.assertion)
    additions = {row.obligation_id: row for row in additional_plans}
    require(len(additions) == len(additional_plans) and set(additions) <= set(groups), 'Foreign/duplicate additional plan')
    plans = []
    for unit in catalog.units:
        extra = additions.get(unit.obligation.id)
        if extra is not None:
            require(extra.requirement_ids == unit.requirement_ids, 'Additional plan changed source/reviewed owners')
            groups[unit.obligation.id].extend(item for item in extra.assertions if item not in groups[unit.obligation.id])
        plans.append(compiler.ObligationPlan(unit.obligation.id, unit.requirement_ids,
            tuple(groups[unit.obligation.id]), extra.review_sha256 if extra is not None else review_sha256,
            unit.ownership_reason))
    return compiler.Declaration(catalog.inventory.sha256, tuple(plans), tuple(suites), tuple(gates),
                                tuple(edges), purposes, compatibility, cohort, capacity_profile=capacity_profile)

