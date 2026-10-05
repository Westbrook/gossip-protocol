"""Original-backed cumulative probe enrollment; no selection or dispatch.

Recompute every carried definition from its original ranked review, financial
materialization and historical Git context. A prior corpus summary is compared
with that reconstruction, never used as its own authentication. Prospective
quotas retain all rejected/duplicate offers without granting another model call.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from pathlib import Path
from typing import Any

from . import candidate_checkpoint_chain_v1 as chain
from . import candidate_observation_admission_v1 as admission
from . import cumulative_generated_probe_context_v1 as contexts
from . import cumulative_generated_probe_matrix_v1 as matrices
from . import cumulative_generated_probe_ranked_originals_v1 as ranked
from . import cumulative_generated_probe_values_v2 as values
from . import cumulative_generated_probe_wire_v1 as wire
from . import cumulative_study_controller_v2 as study
from . import cumulative_study_runtime_v2 as runtime

PROTOCOL = 'cumulative-generated-probe-corpus-v1'
CONTRACT_SLOT = 'generated-probe.corpus-contract'
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
require = study.require


@dataclass(frozen=True, slots=True)
class Quotas:
    novel_per_package_milestone: int
    distinct_per_trajectory: int

    def __post_init__(self) -> None:
        require(all(type(v) is int and 0 < v <= 16384 for v in asdict(self).values()),
                'explicit_bounded_positive_corpus_quotas_required')


def contract(plan: study.StudyPlan, quotas: Quotas, limits: ranked.ReviewLimits) -> dict[str, Any]:
    require(type(plan) is study.StudyPlan and type(quotas) is Quotas and type(limits) is ranked.ReviewLimits,
            'exact_corpus_plan_quotas_and_review_limits_required')
    pins={**ranked.sources(), 'gossip_harness/cumulative_generated_probe_corpus_v1.py':LOADED_SOURCE_SHA256}
    require(hashlib.sha256(Path(__file__).read_bytes()).hexdigest()==LOADED_SOURCE_SHA256,
            'loaded_corpus_owner_changed')
    admission.verify_loaded_sources(pins)
    return {'protocol':PROTOCOL,'parent_study_sha256':plan.sha256,'quotas':asdict(quotas),
        'review_limits':asdict(limits),'sources':pins,'offer_order':'milestone-generation-package-proposal-index',
        'duplicate_policy':'keep-first-original-and-retain-every-later-offer-no-new-model-slot',
        'unsupported_carry':'stop-with-explicit-unavailable-corpus-never-silently-drop',
        'legacy_policy_automatically_amended':False,'dispatch_authority':False,'acceptance_authority':False}


def install_contract(owner: runtime.GossipChildRuntime, *, expected: chain.PrefixCommitment,
                     quotas: Quotas, limits: ranked.ReviewLimits) -> None:
    require(type(owner) is runtime.GossipChildRuntime and type(owner.records) is study.Records,
            'actual_corpus_controller_required')
    owner.records.chain.validate_boundary(expected=expected)
    require(owner.records.read(CONTRACT_SLOT) is None and owner.records.read(ranked.CONTRACT_SLOT) is None,
            'corpus_contract_must_precede_ranked_work')
    owner.plan.verify_sources(owner.repository)
    declared=contract(owner.plan,quotas,limits)
    owner.records.chain.validate_boundary(expected=expected)
    owner.records.put(CONTRACT_SLOT,declared)
    owner.records.chain.validate_boundary()


def _stage(owner: runtime.GossipChildRuntime, milestone: str, generation: int) -> str:
    return 'child.'+owner.trajectory.id+'.'+milestone+'.g'+str(generation)


def _coordinates(owner: runtime.GossipChildRuntime, milestone: str, generation: int) -> list[tuple[str,int]]:
    require(milestone in ('M2','M3','M4') and type(generation) is int
            and 0 <= generation < owner.plan.source_generations,'released_corpus_generation_required')
    all_rows=[(m,g) for m in study.MILESTONES[1:] for g in range(owner.plan.source_generations)]
    return all_rows[:all_rows.index((milestone,generation))+1]


def _advance(active: list[dict[str, Any]], gen: contexts.Generation, proof: dict[str, Any],
             quotas: Quotas, release: study.Release) -> tuple[list[dict[str, Any]],list[dict[str, Any]]]:
    # This pure reducer is not an authority. The owning reader supplies the
    # authenticated joins and replays every ancestor; callers cannot inject a
    # prior corpus into enroll() or read().
    active=json.loads(values.canonical(active));by_id={row['probe']['probe_id']:row for row in active}
    require(len(by_id)==len(active),'duplicate_original_active_probe')
    for row in active: wire.checked_probe(row['probe'],released_requirements=release.requirement_ids)
    counts={p:sum(row['origin']['package']==p and row['origin']['milestone']==gen.milestone for row in active)
            for p in study.PACKAGES}
    rankings={r.package:r for r in gen.rankings};events=[]
    notes=proof['review_definitions'];packages=[row['package'] for row in notes]
    require(len(packages)==len(study.PACKAGES) and set(packages)==set(study.PACKAGES),
            'complete_unique_review_definition_census_required')
    by_package={row['package']:row for row in notes}
    # Original journal order follows the seeded runtime schedule. Quota order
    # is independently fixed by this prospective policy, never arrival timing.
    for package in study.PACKAGES:
        decision=by_package[package]['decision'];ranking=rankings[package]
        if decision is None:
            events.append({'package':package,'reviewer':ranking.reviewer,'status':'no_valid_review',
                           'original_sha256':ranking.original_sha256})
            continue
        for offered in decision['probes']:
            origin={'stage':proof['stage'],'milestone':gen.milestone,'generation':gen.generation,
                    'package':package,'reviewer':ranking.reviewer,'original_sha256':ranking.original_sha256,
                    'decision_sha256':decision['decision_sha256'],'proposal_index':offered['index']}
            event: dict[str, Any]={'origin':origin,'status':offered['status']}
            if offered['status'] != 'admitted_definition':
                event['reason']=offered['reason'];events.append(event);continue
            probe=wire.checked_probe(offered['probe'],released_requirements=release.requirement_ids)
            identifier=probe['probe_id'];event['probe_id']=identifier
            if identifier in by_id:
                require(values.exact(by_id[identifier]['probe'],probe),'same_probe_id_changed_meaning')
                event.update(status='duplicate_definition',first_origin_sha256=values.digest(by_id[identifier]['origin']))
            elif counts[package] >= quotas.novel_per_package_milestone:
                event.update(status='quota_rejected',reason='package_milestone_quota')
            elif len(active) >= quotas.distinct_per_trajectory:
                event.update(status='quota_rejected',reason='trajectory_quota')
            else:
                row={'probe':probe,'origin':origin};active.append(row);by_id[identifier]=row;counts[package]+=1
                event['status']='active_definition'
            events.append(event)
    return sorted(active,key=lambda r:r['probe']['probe_id']),events


def active_probes(rows: list[dict[str, Any]]) -> tuple[matrices.ActiveProbe, ...]:
    return tuple(matrices.ActiveProbe(values.canonical(row['probe']),values.digest(row['origin']),
                                    row['origin']['milestone']) for row in rows)


def _core(owner: runtime.GossipChildRuntime, gen: contexts.Generation, proof: dict[str, Any],
          active: list[dict[str, Any]], declared: dict[str, Any], quotas: Quotas) -> tuple[dict[str, Any],matrices.Matrix]:
    number=study.MILESTONES.index(gen.milestone);release=owner.plan.releases[number]
    active,offers=_advance(active,gen,proof,quotas,release)
    matrix=matrices.compile_matrix(gen,current=release,inherited=owner.plan.releases[number-1],active_probes=active_probes(active))
    return {'protocol':PROTOCOL,'contract_sha256':study.digest(declared),'stage':proof['stage'],
        'generation_sha256':gen.sha256,'before_review_freeze':proof['before_review_freeze'],
        'review_originals':proof['review_originals'],'active':active,'offers':offers,
        'matrix':matrix.record(),'no_additional_model_actions':True,'resource_admission':False,
        'dispatch_authority':False,'acceptance_authority':False},matrix


def _policy(owner: runtime.GossipChildRuntime, expected: chain.PrefixCommitment,
            quotas: Quotas, limits: ranked.ReviewLimits) -> dict[str, Any]:
    require(type(owner) is runtime.GossipChildRuntime and type(expected) is chain.PrefixCommitment,
            'actual_runtime_and_independent_corpus_checkpoint_required')
    owner.records.chain.validate_boundary(expected=expected)
    declared=contract(owner.plan,quotas,limits)
    require(values.exact(owner.records.read(CONTRACT_SLOT),declared),'original_corpus_contract_missing_or_changed')
    require(owner.records.chain.position(owner.records.name(CONTRACT_SLOT))
            < owner.records.chain.position(owner.records.name(ranked.CONTRACT_SLOT)),
            'corpus_contract_must_precede_ranked_work')
    return declared


def _verify_record(owner: runtime.GossipChildRuntime, stage: str, core: dict[str, Any],
                   expected: chain.PrefixCommitment) -> None:
    record=owner.records.read(stage+'.corpus')
    require(type(record) is dict and set(record)==set(core)|{'input_checkpoint'},'complete_original_corpus_record_required')
    assert record is not None
    require(values.exact({k:v for k,v in record.items() if k!='input_checkpoint'},core),
            'original_corpus_differs_from_review_originals')
    prefix=chain.PrefixCommitment(**record['input_checkpoint'])
    position=owner.records.chain.position(owner.records.name(stage+'.corpus'))
    require(owner.records.chain.position(owner.records.name(stage+'.reviews'))==prefix.sequence
            and position==prefix.sequence+1,'corpus_freeze_not_at_review_boundary')
    require(prefix==ranked._original_prefix(owner,prefix.sequence,expected),'corpus_predecessor_differs')


def _fold(owner: runtime.GossipChildRuntime, expected: chain.PrefixCommitment, milestone: str,
          generation: int, quotas: Quotas, limits: ranked.ReviewLimits, *, include_current: bool
          ) -> tuple[list[dict[str, Any]],dict[str, Any] | None,matrices.Matrix | None]:
    declared=_policy(owner,expected,quotas,limits);rows=_coordinates(owner,milestone,generation)
    active: list[dict[str, Any]]=[]
    last: tuple[str,int] | None=None
    core: dict[str,Any] | None=None
    matrix: matrices.Matrix | None=None
    observed_head=owner.protected.head()
    for coordinate in rows if include_current else rows[:-1]:
        m,g=coordinate;stage=_stage(owner,m,g);originals=owner.records.read(stage+'.reviews');saved=owner.records.read(stage+'.corpus')
        if originals is None:
            require(saved is None,'corpus_without_original_reviews');continue
        require(saved is not None,'earlier_review_corpus_missing')
        require((last is None and coordinate==('M2',0)) or (last is not None and
            ((m==last[0] and g==last[1]+1) or (study.MILESTONES.index(m)==study.MILESTONES.index(last[0])+1 and g==0))),
            'corpus_history_gap')
        gen,proof=ranked.reconstruct_frozen(owner,expected=expected,milestone=m,generation=g,limits=limits,historical=True)
        core,matrix=_core(owner,gen,proof,active,declared,quotas);_verify_record(owner,stage,core,expected)
        active=core['active'];last=coordinate
    target=(milestone,generation)
    if include_current:
        require(last==target,'target_corpus_missing')
    else:
        require((last is None and target==('M2',0)) or (last is not None and
            ((target[0]==last[0] and target[1]==last[1]+1) or
             (study.MILESTONES.index(target[0])==study.MILESTONES.index(last[0])+1 and target[1]==0))),
            'corpus_history_gap')
    require(owner.protected.head()==observed_head,'accepted_source_moved_during_corpus_read')
    owner.records.chain.validate_boundary(expected=expected)
    return active,core,matrix


def enroll(owner: runtime.GossipChildRuntime, *, expected: chain.PrefixCommitment,
           milestone: str, generation: int, quotas: Quotas, limits: ranked.ReviewLimits) -> dict[str, Any]:
    """Freeze full current corpus/matrix after authenticating all prior originals."""
    declared=_policy(owner,expected,quotas,limits);stage=_stage(owner,milestone,generation)
    require(owner.records.read(stage+'.corpus') is None,'corpus_already_enrolled')
    require(owner.records.chain.position(owner.records.name(stage+'.reviews'))==expected.sequence,
            'corpus_enrollment_must_immediately_follow_reviews')
    active,_,_=_fold(owner,expected,milestone,generation,quotas,limits,include_current=False)
    gen,proof=ranked.reconstruct_frozen(owner,expected=expected,milestone=milestone,generation=generation,limits=limits)
    core,_=_core(owner,gen,proof,active,declared,quotas)
    owner.records.chain.validate_boundary(expected=expected)
    require(owner.protected.head()==gen.base.commit_oid,'accepted_source_moved_before_corpus_freeze')
    owner.records.put(stage+'.corpus',{**core,'input_checkpoint':asdict(expected)})
    owner.records.chain.validate_boundary()
    return {'slot':stage+'.corpus','corpus_sha256':study.digest(core),'active_definitions':len(core['active']),
            'execution_cells':core['matrix']['execution_cell_counts'],'resource_admission':False,'dispatch_authority':False}


def read(owner: runtime.GossipChildRuntime, *, expected: chain.PrefixCommitment,
         milestone: str, generation: int, quotas: Quotas, limits: ranked.ReviewLimits
         ) -> tuple[tuple[matrices.ActiveProbe,...],matrices.Matrix,dict[str, Any]]:
    active,core,matrix=_fold(owner,expected,milestone,generation,quotas,limits,include_current=True)
    assert core is not None and matrix is not None
    return active_probes(active),matrix,core
