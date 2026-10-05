"""Complete logical test census; no resource reservation or execution authority.

Base overlays precede review and keep their own stable build-phase identity.
Post-review candidate matrices use a fixed peer anchor. Clients retain authored
public suites even though the finite generated driver does not exercise them.
Exact merged-source binding, qualified prerequisites, raw/time capacity and
original-observation selection are still required outside this declaration.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import Any

from . import cumulative_generated_probe_context_v1 as contexts
from . import cumulative_generated_probe_values_v2 as values
from . import cumulative_generated_probe_wire_v1 as wire
from . import cumulative_study_controller_v2 as study
from . import project_acceptance_registry_v1 as registry

PROTOCOL = 'cumulative-generated-probe-matrix-v1'
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
DRIVER_SOURCE_SHA256 = 'f1a81a8891e976815b73a121db344624679544d14a16c74b689da3510f9590e4'
ROOT = Path(__file__).resolve().parents[1]
require = contexts.require
# These are necessary executable dependencies of the pinned finite driver,
# not a claim to exercise every requirement owned by these packages.
FOOTPRINTS = (
    ('refresh-identity-v1', ('catalog', 'ingestion', 'query')),
    ('refresh-noop-v1', ('catalog', 'ingestion', 'query')),
    ('completed-receipt-replay-v1', ('catalog', 'ingestion', 'query')),
    ('manifest-content-hash-v1', ('catalog', 'ingestion')),
)


def catalog() -> dict[str, Any]:
    require(hashlib.sha256(Path(__file__).read_bytes()).hexdigest() == LOADED_SOURCE_SHA256,
            'loaded_matrix_compiler_changed')
    driver = ROOT/'gossip_harness/cumulative_generated_probe_driver_v1.py'
    require(hashlib.sha256(driver.read_bytes()).hexdigest() == DRIVER_SOURCE_SHA256,
            'driver_footprint_needs_versioned_review')
    product = (ROOT/'library-cumulative-product-v2.json').read_bytes()
    require(hashlib.sha256(product).hexdigest() == values.PRODUCT_SHA256, 'normative_product_changed')
    graph = json.loads(product)['dependency_graph']['slots']
    by_id = {row['id']:row for row in graph}
    def closure(identifier: str) -> list[str]:
        reached: set[str] = set(); pending = list(by_id[identifier]['depends_on_slots'])
        while pending:
            parent = pending.pop()
            require(parent != identifier, 'cyclic_requirement_graph')
            if parent not in reached:
                reached.add(parent); pending.extend(by_id[parent]['depends_on_slots'])
        return [row['id'] for row in graph if row['id'] in reached]
    rows=[]
    for template, packages in FOOTPRINTS:
        requirement = values.TEMPLATES[template][0]
        calls = ['Store.__init__', 'Store.close']
        imports = ['library.catalog.store.Store', 'library.ingestion.local.import_file',
                   'library.ingestion.jobs.JobManager']
        if template == 'manifest-content-hash-v1':
            calls += ['JobManager.__init__', 'JobManager.submit']
            witness_slots = ['submitted', 'capture_job']
        else:
            imports += ['library.query.service.Service']
            calls += ['Service.__init__', 'Service.lifecycle_show', 'Service.refresh_document']
            if template == 'completed-receipt-replay-v1':
                calls += ['JobManager.__init__', 'JobManager.submit', 'JobManager.prepare', 'JobManager.commit']
                witness_slots = ['submitted', 'prepared', 'original_receipt', 'before']
            else:
                calls += ['import_file']; witness_slots = ['imported', 'before']
        rows.append({'template_id':template,'requirement_release_gate':requirement,
            'target_packages':list(packages),'required_imports':imports,'invoked_interfaces':calls,
            'required_history_slots':witness_slots,'graph_prerequisites':closure(requirement),
            'whole_requirement_coverage':False,'setup_or_graph_completion_inferred':False})
    return {'protocol':PROTOCOL,'compiler_source_sha256':LOADED_SOURCE_SHA256,
            'driver_source_sha256':DRIVER_SOURCE_SHA256,'product_sha256':values.PRODUCT_SHA256,
            'templates':rows,'unsupported_generated_packages':['clients'],
            'scope':'finite driver footprint only; physical qualification and exact setup witnesses remain required'}


@dataclass(frozen=True, slots=True)
class ActiveProbe:
    admitted_raw: bytes
    original_record_sha256: str
    origin_milestone: str

    def __post_init__(self) -> None:
        registry.sha256(self.original_record_sha256)
        require(type(self.admitted_raw) is bytes and self.origin_milestone in ('M2','M3','M4'),
                'exact_original_probe_declaration_required')

    def read(self, release: study.Release) -> dict[str, Any]:
        require(study.MILESTONES.index(self.origin_milestone) <= study.MILESTONES.index(release.milestone),
                'future_probe_origin')
        value=json.loads(self.admitted_raw,object_pairs_hook=values._pairs,parse_constant=values._constant)
        require(values.canonical(value)==self.admitted_raw,'canonical_original_probe_required')
        # Revalidate unchanged original semantics against the current release.
        # Unsupported carry-forward cannot disappear through filtering.
        checked=wire.checked_probe(value,released_requirements=release.requirement_ids)
        require(study.MILESTONES.index(checked['requirement_id'].split('-')[0])
                <= study.MILESTONES.index(self.origin_milestone), 'probe_origin_precedes_requirement_release')
        return checked


@dataclass(frozen=True, slots=True)
class Cell:
    phase: str
    kind: str
    actor: str | None
    context_raw: bytes | None
    definition_sha256: str
    ordered_check_ids: tuple[str, ...]
    use: str

    def record(self) -> dict[str, Any]:
        return {'phase':self.phase,'kind':self.kind,'actor':self.actor,
            'context':json.loads(self.context_raw) if self.context_raw is not None else None,
            'source_binding_status':('declared_context_needs_actual_git_binding' if self.context_raw is not None
                                     else 'requires_selected_merged_source' if self.phase=='merged'
                                     else 'peer_anchor_unavailable'),
            'definition_sha256':self.definition_sha256,'ordered_check_ids':list(self.ordered_check_ids),
            'use':self.use,'fresh_execution_required':True,'execution_reuse_granted':False}

    @property
    def sha256(self) -> str:
        return study.digest(self.record())


@dataclass(frozen=True, slots=True)
class Matrix:
    build_phase_raw: bytes
    post_review_raw: bytes
    base_cells: tuple[Cell, ...]
    contextual_cells: tuple[Cell, ...]
    merged_cells: tuple[Cell, ...]

    def record(self) -> dict[str, Any]:
        groups={'before_review':self.base_cells,'contextual':self.contextual_cells,'merged':self.merged_cells}
        return {'protocol':PROTOCOL,'build_phase':json.loads(self.build_phase_raw),
            'post_review':json.loads(self.post_review_raw),
            'cells':{phase:[{'cell_sha256':c.sha256,**c.record()} for c in rows] for phase,rows in groups.items()},
            'execution_cell_counts':{**{phase:len(rows) for phase,rows in groups.items()},
                                     'total':sum(len(rows) for rows in groups.values())},
            'assertion_counts':{phase:sum(len(c.ordered_check_ids) for c in rows) for phase,rows in groups.items()},
            'count_meaning':'one generated history or one complete authored public-suite execution per cell; not independent samples',
            'not_yet_admitted':['actual_source_commits','qualified_setup_witnesses','aggregate_raw_byte_capacity',
                                'absolute_deadline_capacity','shared_executor_reservations','durable_controller_slots'],
            'dispatch_authority':False,'acceptance_authority':False}


def compile_matrix(generation: contexts.Generation, *, current: study.Release,
                   inherited: study.Release, active_probes: tuple[ActiveProbe, ...]) -> Matrix:
    require(type(generation) is contexts.Generation and type(current) is study.Release
            and type(inherited) is study.Release,'exact_generation_and_releases_required')
    require(current.milestone==generation.milestone
            and study.MILESTONES.index(inherited.milestone)+1==study.MILESTONES.index(current.milestone),
            'exact_previous_and_current_releases_required')
    require(type(active_probes) is tuple and all(type(p) is ActiveProbe for p in active_probes),
            'complete_active_probe_roster_required')
    footprint=catalog()
    probes=[(p,p.read(current)) for p in active_probes]
    identifiers=[value['probe_id'] for _,value in probes]
    require(identifiers==sorted(set(identifiers)), 'sorted_unique_active_probes_required_no_silent_dedup')
    eligible=[p for p in generation.proposals if p.disposition=='eligible']
    def public(phase: str, actor: str | None, context: contexts.Context | None, release: study.Release) -> Cell:
        return Cell(phase,'authored-public-suite',actor,values.canonical(context.record()) if context else None,
                    release.sha256,release.ordered_check_ids,
                    'inform-review-only-no-unconditional-veto' if phase=='before-review' else 'required-complete-public-evidence')
    def generated(phase: str, actor: str | None, context: contexts.Context | None, value: dict[str,Any]) -> Cell:
        identifier=value['probe_id']
        return Cell(phase,'generated-history',actor,values.canonical(context.record()) if context else None,
                    values.digest(value),('probe-'+identifier+'-value','probe-'+identifier+'-mechanics'),
                    'required-complete-applicable-public-evidence')
    before=[];contextual=[];merged=[public('merged',None,None,current)]
    for proposal in eligible:
        before.append(public('before-review',proposal.actor,contexts.compose(generation,'base-overlay',actor=proposal.actor),inherited))
        context=(contexts.compose(generation,'contextual',actor=proposal.actor)
                 if generation.anchor_vector is not None else None)
        contextual.append(public('contextual',proposal.actor,context,current))
        for _, value in probes:
            if study.package_for(proposal.actor) in dict(FOOTPRINTS)[value['template_id']]:
                contextual.append(generated('contextual',proposal.actor,context,value))
    for _, value in probes:merged.append(generated('merged',None,None,value))
    # Keep pre-review identity independent of later ranks and novel probes.
    common={'protocol':PROTOCOL,'catalog_sha256':study.digest(footprint),'dispatch_authority':False}
    build={**common,'build_phase_sha256':generation.build_phase_sha256,'release_sha256':inherited.sha256,
           'role_census':[{'actor':p.actor,'disposition':p.disposition,'original_sha256':p.original_sha256}
                          for p in generation.proposals]}
    post={**common,'generation_sha256':generation.sha256,'current_release_sha256':current.sha256,
          'role_census':build['role_census'],'missing_anchor_packages':[r.package for r in generation.rankings if not r.actors],
          'active_probes':[{'probe':value,'original_record_sha256':p.original_record_sha256,
                           'origin_milestone':p.origin_milestone} for p,value in probes],
          'footprint':footprint,'unranked_eligible_candidates_remain_in_matrix':True,
          'simultaneous_choice_required':True,'merged_checks_conditional_on_complete_choice':True,
          'merged_checks_never_satisfied_by_contextual_receipts':True}
    return Matrix(values.canonical(build),values.canonical(post),tuple(before),tuple(contextual),tuple(merged))
