"""Closed M2/direct-API extension of the complete cumulative source factory.

All prior families and the full inventory remain mandatory. These factories
materialize finite assertion edges, never semantic relevance, applicability or
production qualification. Complete independent scope enrollment remains separate.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path
from typing import Any

from . import cumulative_scope_source_v2 as previous
from . import project_acceptance_compiler_v1 as compiler
from . import project_acceptance_registry_v1 as registry
from . import candidate_observation_admission_v1 as admission

PROTOCOL = 'cumulative-scope-source-v3'
LOADED_SOURCE_SHA256 = previous.sha(Path(__file__).read_bytes())
ScopeSourceError = previous.ScopeSourceError
SourceCatalog = previous.SourceCatalog
SourceUnit = previous.SourceUnit
Selector = previous.Selector
AssertionDeclaration = previous.AssertionDeclaration
require, encoded, sha = previous.require, previous.encoded, previous.sha
load_catalog, scope_review_input = previous.load_catalog, previous.scope_review_input
M2_LANES = ('public-contract',)
_values = previous._values


@dataclass(frozen=True, slots=True)
class ExecutableSlice(previous.ExecutableSlice):
    @property
    def sha256(self) -> str:
        return sha(encoded({'protocol': PROTOCOL, 'slice': asdict(self)}))

    def compiler_records(self, *, suite_id: str, physical_slot: str) -> tuple[compiler.SuiteDefinition, compiler.ExecutionGate, tuple[compiler.CoverageEdge, ...]]:
        if self.family != 'm2-direct-api':
            return previous.ExecutableSlice(**_values(self)).compiler_records(suite_id=suite_id, physical_slot=physical_slot)
        registry.identifier(suite_id); registry.identifier(physical_slot)
        logical = tuple(dict.fromkeys(key for row in self.assertions for key in row.assertion.logical_gate_ids))
        binding = self.gate.binding
        suite = compiler.SuiteDefinition(suite_id, self.gate.ordered_case_ids, binding.ordered_suite_sha256,
            self.definition_sha256, self.original_definition_purpose, binding.purpose,
            self.source_contract_sha256, binding.evaluator_sha256, True, True, M2_LANES)
        gate = compiler.ExecutionGate(self.gate.gate_id, physical_slot, suite_id, 'product', logical,
            binding.runtime_image_sha256, binding.environment_sha256, binding.limits_sha256,
            binding.seed_sha256, binding.execution_protocol)
        edges = tuple(compiler.CoverageEdge(row.obligation_id, row.assertion.id,
            self.gate.gate_id, row.case_id, row.observation_pointer) for row in self.assertions)
        return suite, gate, edges


def cli_slice(registration: Any) -> ExecutableSlice:
    return ExecutableSlice(**_values(previous.cli_slice(registration)))


def http_slice(registration: Any, profile: Any, policy: Any) -> ExecutableSlice:
    return ExecutableSlice(**_values(previous.http_slice(registration, profile, policy)))


def product_process_slice(registration: Any, profile: Any, policy: Any) -> ExecutableSlice:
    return ExecutableSlice(**_values(previous.product_process_slice(registration, profile, policy)))


def storage_slice(registration: Any) -> ExecutableSlice:
    return ExecutableSlice(**_values(previous.storage_slice(registration)))


def _m2_kind(case_id: str, observation_index: int, expected: Any) -> str:
    # This is a prospective finite assertion class, independently reviewed with
    # the exact whole history; it supplies no broad boundary/negative coverage.
    if case_id == 'm2-retained-blob-capacity' and observation_index == 0:
        return 'boundary'  # Frozen expectation is the host hash of capacity error.
    value = expected[1] if type(expected) is list and len(expected) == 2 and type(expected[0]) is int else expected
    if type(value) is dict and set(value) == {'error'}:
        return 'boundary' if value['error'] in ('capacity', 'revision_capacity', 'too_large') else 'negative'
    return 'history'


def m2_slice(registration: Any) -> ExecutableSlice:
    """Actual closed selector reconstruction; layout identity alone is not proof.

    A recipe must separately authenticate Git and original enrolled layout
    review. Full scope registration must independently approve every missing or
    proposed applicability cell. This routine accepts no observed candidate data.
    """
    from . import candidate_m2_product_execution_v1 as execution
    from . import candidate_m2_product_observation_v1 as observer
    from . import candidate_m2_product_profile_v1 as profile
    require(type(registration) is execution.M2Registration, 'Exact M2 prospective registration required')
    normalized = execution.observation_registration(registration)
    binding = registration.binding
    value = profile.profile_for(binding.case_id, binding.purpose)
    catalog_record = observer.selector_catalog(binding.case_id, purpose=binding.purpose)
    sources = execution.evaluator_sources()
    admission.verify_loaded_sources(sources)
    require(binding.definition_sha256 == execution.digest(value.record()) == catalog_record['definition_sha256']
        and binding.profile_sha256 == value.sha256 == catalog_record['profile_sha256']
        and binding.evaluator_sha256 == execution.digest(sources)
        and catalog_record['evaluator_sources'] == sources
        and tuple(catalog_record['ordered_case_ids']) == normalized.gate.ordered_case_ids,
        'M2 definition/profile/evaluator/ordered roster differs')
    full = load_catalog(Path(__file__).resolve().parents[1])
    selectors, assertions = [], []
    definition = profile.case_definition(binding.case_id)
    for row in catalog_record['selectors']:
        mechanics = row['case_id'] == execution.mechanics_case_id(value)
        if mechanics:
            selectors.append(Selector(row['case_id'], row['pointer'], 'normative', '/ordered_actions', 'mechanics'))
            continue
        index = row['observation_index']
        require(type(index) is int and row['action_index'] == value.selectors()[index]['action_index'],
                'M2 original action/observation projection differs')
        selectors.append(Selector(row['case_id'], row['pointer'], 'normative',
            '/definition/expected/observations/' + str(index),
            'mechanics' if definition['input']['actions'][row['action_index']]['op'] == 'reopen' else 'semantic'))
        kind = _m2_kind(binding.case_id, index, definition['expected']['observations'][index])
        for facet in row['source_unit_facets']:
            unit = full.unit(facet['source_unit_id'])
            require(unit.obligation.role == 'product' and unit.obligation.id.startswith('M2-'),
                    'M2 finite facet cannot mint other milestone authority')
            logical = tuple(gate.id for gate in full.inventory.logical_gates
                if gate.role == 'product' and gate.lane in M2_LANES
                and set(unit.requirement_ids) & set(gate.requirement_ids) & set(value.requirement_ids))
            require(bool(logical), 'Mapped source facet has no direct public-contract logical gate')
            identity = 'm2-' + sha(encoded([binding.case_id, row['case_id'], row['pointer'], facet, kind]))[:24]
            assertions.append(AssertionDeclaration(unit.obligation.id,
                compiler.Assertion(identity, kind, logical), row['case_id'], row['pointer'],
                facet['rationale'] + ' Only this original action/result in the complete declared history is claimed. '
                'Independent full-clause relevance, inherited M4 compatibility and lane adequacy remain review duties; '
                'no HTTP/CLI/browser/crash/forced-schedule or V2 amendment credit follows.'))
    return ExecutableSlice('m2-direct-api', binding.case_id, normalized.gate,
        normalized.definition_sha256, normalized.original_definition_purpose, profile.native.CONTRACT_SHA256,
        value.sha256, tuple(sorted(sources.items())), tuple(selectors), tuple(assertions),
        tuple(profile.LIMITATIONS) + ('All unrepresented mandatory units and applicability cells remain explicit in the full source denominator.',),
        encoded({'registration': asdict(registration)}).decode())


def verify_slice(value: previous.previous.ExecutableSlice) -> None:
    require(type(value) is ExecutableSlice, 'Exact version3 source-derived slice required')
    if value.family in ('cli', 'http', 'product-process', 'storage-b01', 'storage-b02'):
        previous.verify_slice(previous.ExecutableSlice(**_values(value)))
        return
    require(value.family == 'm2-direct-api', 'Unknown version3 executable factory')
    from . import candidate_m2_product_execution_v1 as execution
    record = json.loads(value.factory_input_json)
    require(type(record) is dict and set(record) == {'registration'}, 'Unexpected M2 factory input')
    row = record['registration']
    registration = execution.M2Registration(execution.M2Binding(**row['binding']), row['commit_oid'],
        row['tree_oid'], row['repetition_id'], previous.previous._gate(row['gate']), tuple(row['cohort_trajectory_ids']))
    require(m2_slice(registration) == value, 'Source-derived M2 selectors/purpose were substituted')


def coverage_census(catalog: SourceCatalog, declaration: compiler.Declaration) -> dict[str, Any]:
    result = previous.coverage_census(catalog, declaration)
    result['protocol'] = PROTOCOL
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

