"""Closed ranked reviews and an original-record join for a prospective successor.

No legacy directive is upgraded after the fact. The separately retained ranking
contract and exact new instructions must precede the original role work. This
module can freeze authenticated build evidence before review. It does not install
that contract in the legacy controller, dispatch a probe,
select a winner, or authorize acceptance. Full successor/cell/budget adoption is
still required before any live use.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from pathlib import Path
from typing import Any

from . import candidate_checkpoint_chain_v1 as chain
from .candidate_checkpoint_head_v1 import ExternalHead
from . import candidate_observation_admission_v1 as admission
from . import cumulative_generated_probe_context_v1 as contexts
from . import cumulative_generated_probe_matrix_v1 as matrix
from . import cumulative_generated_probe_values_v2 as values
from . import cumulative_study_controller_v2 as study
from . import cumulative_study_runtime_v2 as runtime
from .peer_financial_authority_v2 import ledger_identity
from .peer_project_contract_v2 import EvidenceRef, from_dict, resolve_local, strict_loads, to_dict
from .peer_role_loop_v2 import WorkDirective, directive_id

PROTOCOL = 'cumulative-generated-probe-ranked-originals-v1-historical-join-v1'
DECISION_PROTOCOL = 'cumulative-generated-probe-ranked-decision-v1'
CONTRACT_SLOT = 'generated-probe.ranking-contract'
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
require = contexts.require


@dataclass(frozen=True, slots=True)
class ReviewLimits:
    review_bytes: int
    proposal_count: int
    json_nodes: int
    json_depth: int

    def __post_init__(self) -> None:
        require(all(type(v) is int and v > 0 for v in asdict(self).values()), 'explicit_positive_review_limits_required')


def sources() -> dict[str, str]:
    root = Path(__file__).resolve().parents[1]
    names = set(study.SOURCE_CLOSURE) | {
        'gossip_harness/cumulative_generated_probe_ranked_originals_v1.py',
        'gossip_harness/cumulative_generated_probe_context_v1.py',
        'gossip_harness/cumulative_generated_probe_values_v2.py',
        'gossip_harness/cumulative_generated_probe_matrix_v1.py',
        'gossip_harness/cumulative_generated_probe_wire_v1.py',
        'gossip_harness/cumulative_generated_probe_driver_v1.py'}
    result = {name: hashlib.sha256((root/name).read_bytes()).hexdigest() for name in sorted(names)}
    require(result['gossip_harness/cumulative_generated_probe_ranked_originals_v1.py'] == LOADED_SOURCE_SHA256,
            'loaded_ranked_reader_changed')
    admission.verify_loaded_sources(result)
    return result


def contract(plan: study.StudyPlan, limits: ReviewLimits) -> dict[str, Any]:
    require(type(plan) is study.StudyPlan and type(limits) is ReviewLimits, 'exact_plan_and_limits_required')
    return {'protocol': PROTOCOL, 'decision_protocol': DECISION_PROTOCOL,
            'parent_study_sha256': plan.sha256, 'parent_public_policy_sha256': plan.cohort.shared_policy_sha256,
            'limits': asdict(limits), 'sources': sources(), 'milestones': ['M2', 'M3', 'M4'],
            'policy': 'first-complete-passing-endorsed-alternative-against-one-frozen-peer-anchor',
            'reviewer_actions_per_package_generation': 1, 'requires_fresh_merged_retest': True,
            'legacy_policy_automatically_amended': False, 'dispatch_authority': False, 'acceptance_authority': False}


def instructions(release: study.Release, stage: str, package: str, limits: ReviewLimits) -> str:
    require(type(release) is study.Release and release.milestone in ('M2', 'M3', 'M4')
            and package in study.PACKAGES and type(limits) is ReviewLimits, 'exact_released_ranked_scope_required')
    # Template data only; no arbitrary comparator, executable code or file path.
    templates = {k: {'requirement_id': req, 'parameter_names': list(params)}
                 for k, (req, params) in values.TEMPLATES.items() if req in release.requirement_ids}
    spec = {'protocol': DECISION_PROTOCOL, 'stage_id': stage, 'package': package,
            'fields': ['protocol', 'stage_id', 'package', 'ranked_actors', 'reasons', 'probes'],
            'limits': asdict(limits), 'templates': templates}
    return (release.instructions + '\nReview every complete locally arrived proposal for ' + package
        + '. Return only decision.json matching this closed contract: ' + values.canonical(spec).decode('ascii')
        + '. ranked_actors is an ordered unique array of full actor aliases from this package frontier; '
          'include only defensible candidates, or [] for no endorsement. The first endorsement supplies '
          'fixed peer context; later selection requires complete passing evidence. reasons is a string. '
          'probes is an array of closed objects with template_id, requirement_id, parameters, expectation. '
          'For refresh-noop-v1 use expectation {"kind":"exact_scalar","value":"unchanged"}; '
          'other listed templates use {"kind":"contract_relation","value":true}. '
          'No executable tests, commands, arbitrary paths or extra model actions. '
          'Do not claim execution of unrun tests. This response is the one review action for this generation.')


def decode_decision(raw: bytes, *, stage: str, package: str, eligible: tuple[str, ...],
                    release: study.Release, limits: ReviewLimits) -> dict[str, Any]:
    """Decode bounded reviewer data; semantic probe rejection never reveals a fix."""
    require(type(limits) is ReviewLimits and type(raw) is bytes and len(raw) <= limits.review_bytes,
            'review_bytes_limit')
    require(type(eligible) is tuple and all(type(a) is str for a in eligible)
            and len(set(eligible)) == len(eligible), 'exact_eligible_alias_roster_required')
    try:
        decision = json.loads(raw.decode('utf-8'), object_pairs_hook=values._pairs, parse_constant=values._constant)
        values._bounded(decision, max_nodes=limits.json_nodes, max_depth=limits.json_depth)
    except (UnicodeError, RecursionError) as error:
        raise ValueError('invalid_bounded_review_json') from error
    require(type(decision) is dict and set(decision) == {
        'protocol', 'stage_id', 'package', 'ranked_actors', 'reasons', 'probes'}
        and decision['protocol'] == DECISION_PROTOCOL and decision['stage_id'] == stage
        and decision['package'] == package and type(decision['reasons']) is str,
        'closed_ranked_decision_required')
    ranks, proposals = decision['ranked_actors'], decision['probes']
    require(type(ranks) is list and all(type(a) is str and a in eligible for a in ranks)
            and len(set(ranks)) == len(ranks), 'invalid_ranked_aliases')
    require(type(proposals) is list and len(proposals) <= limits.proposal_count, 'probe_proposal_count_limit')
    probes = []; seen = set()
    for index, proposal in enumerate(proposals):
        try:
            admitted = values.admit(proposal, released_requirements=release.requirement_ids,
                                    contract_sha256=values.PRODUCT_SHA256)
            require(admitted['probe_id'] not in seen, 'duplicate_probe')
            seen.add(admitted['probe_id'])
            probes.append({'index': index, 'status': 'admitted_definition', 'probe': admitted})
        except (ValueError, TypeError, KeyError):
            probes.append({'index': index, 'status': 'rejected_definition',
                           'reason': 'unsupported_invalid_or_duplicate_probe'})
    return {'decision_sha256': hashlib.sha256(raw).hexdigest(), 'ranked_actors': tuple(ranks),
            'reasons': decision['reasons'], 'probes': probes, 'dispatch_authority': False,
            'acceptance_authority': False, 'admitted_definition_is_qualified_execution': False}


def _roster(records: study.Records, stage: str, kind: str, actors: tuple[str, ...],
            seed: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    key = stage + '.' + kind
    roster = records.read(key)
    require(type(roster) is dict and set(roster) == {'originals', 'count'}
            and type(roster['count']) is int and roster['count'] == len(actors)
            and type(roster['originals']) is list and len(roster['originals']) == len(actors),
            'complete_original_roster_required')
    assert roster is not None
    expected = sorted(actors, key=lambda actor: study.digest({'seed': seed, 'actor': actor}))
    results = []; provenance = []
    roster_position = records.chain.position(records.name(key))
    for index, (row, actor) in enumerate(zip(roster['originals'], expected)):
        slot = key + '.original.' + str(index); name = records.name(slot)
        require(type(row) is dict and set(row) == {'slot', 'name', 'sha256'}
                and row['slot'] == slot and row['name'] == name, 'original_slot_substitution')
        value = records.read(slot)
        require(type(value) is dict and study.digest(value) == row['sha256']
                and value.get('actor') == actor, 'original_actor_or_bytes_substitution')
        assert value is not None
        position = records.chain.position(name)
        require(position < roster_position, 'original_roster_chronology')
        results.append(value)
        provenance.append({'slot': slot, 'name': name, 'sha256': row['sha256'], 'position': position})
    return results, provenance


def _resolved(owner: runtime.GossipChildRuntime, ref: EvidenceRef) -> dict[str, Any]:
    arrived = owner.seed.arrived()
    require(ref in arrived, 'original_local_evidence_unavailable')
    raw = owner.seed.resolve(ref)
    require(raw is not None, 'original_local_bytes_unavailable')
    result = strict_loads(resolve_local(ref, arrived, raw))
    require(type(result) is dict, 'original_object_required')
    return result


def _authenticate(owner: runtime.GossipChildRuntime, value: dict[str, Any], *, stage: str,
                  release: study.Release, generation: int, base: contexts.Base,
                  contract_position: int, original_position: int, limits: ReviewLimits, review: bool) -> WorkDirective:
    record = owner.records.read(owner.key + '.directive.' + value['directive_id'])
    require(type(record) is dict and record.get('actor') == value['actor'], 'original_directive_missing')
    assert record is not None
    directive = WorkDirective.from_dict(record['directive'])
    role = value['actor'].removeprefix(owner.trajectory.id + '.')
    package = study.package_for(role)
    require(directive_id(directive) == value['directive_id']
            and directive.context.execution_contract_sha256 == owner.plan.sha256
            and directive.context.trajectory_id == owner.trajectory.id
            and directive.context.cohort_id == owner.child.cohort
            and directive.context.milestone == study.MILESTONES.index(release.milestone) + 1
            and directive.context.requirements_sha256 == release.sha256
            and directive.work.package_id == package and directive.work.requirement_id == release.milestone
            and directive.work.slot_id == role and directive.work.generation == generation,
            'original_directive_scope_substitution')
    position = owner.records.chain.position(owner.records.name(owner.key + '.directive.' + value['directive_id']))
    require(contract_position < position, 'ranking_contract_must_precede_role_work')
    require(position < original_position, 'result_precedes_original_directive')
    expected_kind = 'review' if review else 'build' if generation == 0 else 'repair'
    require(directive.attempt == 1 and directive.attempt_limit == 1 and directive.feedback == ''
            and directive.kind == expected_kind and directive.profile_id == ('strong' if review else 'mini')
            and directive.allowed_paths == (('decision.json',) if review else owner.plan.package_paths[package]),
            'original_role_or_scope_differs')
    if review:
        scope = study.digest({'source': to_dict(directive.source_ref),
                              'evidence': [to_dict(x) for x in directive.evidence_refs]})
        expected_stage = stage + '.review.' + scope[:12]
        require(owner.records.chain.position(owner.records.name(stage+'.builds')) < position,
                'review_directive_precedes_builder_roster')
        require(directive.instructions == instructions(release, stage, package, limits), 'ranked_review_not_prospectively_requested')
    else:
        expected_stage = stage + '.build'
        expected_instructions = (release.instructions + '\nImplement all currently released inherited and new requirements for '
            + package + '. Read complete current source and public feedback. Preserve other packages and immutable fixtures. '
            'Return full changed-file contents within the exact writable path roster.')
        require(directive.instructions == expected_instructions, 'original_build_instructions_differ')
    require(value.get('stage_id') == expected_stage, 'original_result_stage_differs')
    source = _resolved(owner, directive.source_ref)
    require(set(source) == {'base_sha', 'files'} and source['base_sha'] == base.commit_oid
            and type(source['files']) is dict and all(type(v) is str for v in source['files'].values())
            and tuple(sorted((p, v.encode('utf-8')) for p, v in source['files'].items())) == base.files,
            'original_generation_source_differs')
    for ref in directive.evidence_refs:
        _resolved(owner, ref)  # Require complete local view before the financial verifier.
    original_ref = from_dict(EvidenceRef, value['original_ref'])
    require(original_ref.kind == 'cumulative-result' and original_ref.producer == value['actor']
            and values.exact(_resolved(owner, original_ref), {k:v for k,v in value.items() if k != 'original_ref'}),
            'mesh_result_original_differs')
    # Preserve the existing actual V5 terminal/known-failure proof and complete
    # materialization checks. No caller boolean supplies financial authenticity.
    runtime.GossipChildRuntime._verify_result(owner, value)
    return directive


def _build_inputs(owner: runtime.GossipChildRuntime, *, expected: chain.PrefixCommitment,
                  milestone: str, generation: int, limits: ReviewLimits, historical: bool = False) -> tuple[
                      dict[str, Any], int, study.Release, str, list[dict[str, Any]],
                      list[dict[str, Any]], contexts.Base, dict[str, contexts.Proposal]]:
    """Authenticate builds without requiring or inspecting future reviews."""
    require(type(owner) is runtime.GossipChildRuntime and type(owner.records) is study.Records
            and type(owner.records.chain) is chain.CheckpointChain
            and type(owner.records.chain.authority) is ExternalHead and type(expected) is chain.PrefixCommitment,
            'actual_runtime_and_independent_checkpoint_required')
    require(type(generation) is int and 0 <= generation < owner.plan.source_generations
            and milestone in ('M2', 'M3', 'M4'), 'released_generation_required')
    owner.records.chain.validate_boundary(expected=expected)
    owner.plan.verify_sources(owner.repository)
    require(ledger_identity(owner.ledger) == owner.expected_ledger_identity
            and owner.records.read('ledger') == owner.expected_ledger_identity, 'original_ledger_identity_changed')
    declared = contract(owner.plan, limits)
    require(values.exact(owner.records.read(CONTRACT_SLOT), declared), 'prospective_ranking_contract_missing_or_changed')
    contract_position = owner.records.chain.position(owner.records.name(CONTRACT_SLOT))
    release = owner.plan.releases[study.MILESTONES.index(milestone)]
    stage = 'child.' + owner.trajectory.id + '.' + milestone + '.g' + str(generation)
    actors = owner.child.actors
    builds, build_refs = _roster(owner.records, stage, 'builds', actors[:-4], owner.trajectory.block_seed_sha256)
    require(type(historical) is bool, 'explicit_historical_read_mode_required')
    observed_head = owner.protected.head()
    previous = owner.records.read(stage+'.before-review-freeze') if historical else None
    if historical:
        require(type(previous) is dict, 'historical_read_requires_original_build_freeze')
        assert previous is not None
        base = contexts.capture_base(owner.protected, commit_oid=previous['build_phase']['base']['commit_oid'])
    else:
        base = contexts.capture_base(owner.protected)
    proposals = {}
    for value, provenance in zip(builds, build_refs, strict=True):
        _authenticate(owner, value, stage=stage, release=release, generation=generation, base=base,
                      contract_position=contract_position, original_position=provenance['position'], limits=limits, review=False)
        alias = value['actor'].removeprefix(owner.trajectory.id + '.')
        reply = value['snapshot'].get('reply')
        disposition = 'stopped' if value['snapshot']['state'] == 'stopped' else 'unknown'
        changes: contexts.Delta = ()
        if reply is not None and reply.get('state') in ('completed', 'failed'):
            disposition = 'failed'
            if reply['state'] == 'completed':
                try:
                    delta = value['result_payload']['payload']['changes']
                    require(type(delta) is dict, 'proposal_changes_required')
                    changes = tuple(sorted(delta.items()))
                    contexts.entries(changes, source=False)
                    require(all(contexts.under(p, owner.plan.package_paths[study.package_for(alias)]) for p, _ in changes),
                            'proposal_scope')
                    disposition = 'eligible'
                except (ValueError, TypeError, KeyError):
                    changes = ()
        proposals[alias] = contexts.Proposal(alias, disposition, base.sha256, study.digest(value), changes)
    require(owner.protected.head() == observed_head, 'generation_base_moved_during_original_join')
    owner.records.chain.validate_boundary(expected=expected)
    require(values.exact(contract(owner.plan, limits), declared), 'ranked_contract_changed_during_join')
    return declared, contract_position, release, stage, builds, build_refs, base, proposals


def reconstruct_builds(owner: runtime.GossipChildRuntime, *, expected: chain.PrefixCommitment,
                       milestone: str, generation: int, limits: ReviewLimits,
                       historical: bool = False) -> tuple[contexts.Generation, dict[str, Any]]:
    declared, _, _, stage, _, build_refs, base, proposals = _build_inputs(
        owner, expected=expected, milestone=milestone, generation=generation, limits=limits, historical=historical)
    # Unknown placeholders are excluded from build_phase_record() and are never
    # evidence that a review happened. No future review receipt is required.
    compiled = contexts.Generation(owner.child.cohort, owner.trajectory.id, milestone, generation, owner.trajectory.arm,
        base, tuple((p, tuple(sorted(owner.plan.package_paths[p]))) for p in study.PACKAGES),
        tuple(proposals[a] for a in study.actors_for(owner.trajectory.arm)[:-4]),
        tuple(contexts.Ranking(p, r, '0'*64, 'unknown', ()) for p, r in zip(study.PACKAGES, study.REVIEWERS, strict=True)))
    return compiled, {'protocol': PROTOCOL, 'contract_sha256': study.digest(declared),
        'original_checkpoint': asdict(expected), 'stage': stage, 'build_originals': build_refs,
        'build_phase_sha256': compiled.build_phase_sha256, 'review_originals': [],
        'durably_frozen': False, 'dispatch_authority': False, 'acceptance_authority': False}


def _build_freeze_value(owner: runtime.GossipChildRuntime, compiled: contexts.Generation) -> dict[str, Any]:
    number = study.MILESTONES.index(compiled.milestone)
    census = matrix.compile_matrix(compiled, current=owner.plan.releases[number],
        inherited=owner.plan.releases[number-1], active_probes=())
    return {'protocol': PROTOCOL, 'parent_study_sha256': owner.plan.sha256,
        'build_phase': compiled.build_phase_record(), 'build_phase_sha256': compiled.build_phase_sha256,
        'matrix_build_phase': json.loads(census.build_phase_raw),
        'before_review_cells': [{'cell_sha256': c.sha256, **c.record()} for c in census.base_cells],
        'resource_admission': False, 'dispatch_authority': False, 'acceptance_authority': False}


def freeze_build_phase(owner: runtime.GossipChildRuntime, *, expected: chain.PrefixCommitment,
                       milestone: str, generation: int, limits: ReviewLimits) -> dict[str, Any]:
    """Retain exact original-backed checks immediately after the builder roster.

    This records what must be tested. It allocates no resource, supplies no
    observation capability, and does not assert that any check executed.
    A repeated call is refused; inspection uses the original record.
    """
    compiled, proof = reconstruct_builds(owner, expected=expected,
        milestone=milestone, generation=generation, limits=limits)
    slot = proof['stage'] + '.before-review-freeze'
    require(owner.records.read(slot) is None, 'build_phase_already_frozen')
    require(owner.records.chain.position(owner.records.name(proof['stage']+'.builds')) == expected.sequence,
            'build_freeze_must_immediately_follow_roster')
    record = {**_build_freeze_value(owner, compiled), 'input_checkpoint': asdict(expected)}
    # Recheck after compilation and immediately before the one durable write.
    owner.records.chain.validate_boundary(expected=expected)
    require(owner.protected.head() == compiled.base.commit_oid, 'generation_base_moved_before_freeze')
    owner.records.put(slot, record)
    owner.records.chain.validate_boundary()
    return {'slot': slot, 'record_sha256': study.digest(record),
        'checkpoint': asdict(owner.records.chain.commitment),
        'build_phase_frozen': True, 'resource_admission': False, 'dispatch_authority': False}


def reconstruct(owner: runtime.GossipChildRuntime, *, expected: chain.PrefixCommitment,
                milestone: str, generation: int, limits: ReviewLimits, historical: bool = False) -> tuple[contexts.Generation, dict[str, Any]]:
    """Join ranked originals; use reconstruct_frozen to require pre-review freeze.

    Neither reader admits resources, dispatches, or selects a candidate.
    """
    observed_head = owner.protected.head()
    declared, contract_position, release, stage, builds, build_refs, base, proposals = _build_inputs(
        owner, expected=expected, milestone=milestone, generation=generation, limits=limits, historical=historical)
    reviews, review_refs = _roster(owner.records, stage, 'reviews', owner.child.actors[-4:], owner.trajectory.block_seed_sha256)
    require(owner.records.chain.position(owner.records.name(stage+'.builds'))
            < min(row['position'] for row in review_refs), 'review_precedes_complete_builder_roster')
    rankings = {}; notes = []
    for value, provenance in zip(reviews, review_refs, strict=True):
        directive = _authenticate(owner, value, stage=stage, release=release, generation=generation, base=base,
                                  contract_position=contract_position, original_position=provenance['position'], limits=limits, review=True)
        alias = value['actor'].removeprefix(owner.trajectory.id + '.'); package = study.package_for(alias)
        # Authenticate the complete frontier, not merely the reviewer's summary.
        require(len(directive.evidence_refs) == 1, 'single_complete_scoped_frontier_required')
        frontier = _resolved(owner, directive.evidence_refs[0])
        scoped = [{'actor': x['actor'], 'directive_id': x['directive_id'], 'original_ref': x['original_ref'],
                   'snapshot': x['snapshot'], 'proposal': x['result_payload']} for x in builds if study.package_for(x['actor']) == package]
        require(values.exact(frontier, {'stage_id':stage, 'package':package, 'builds':scoped,
                                      'shared_source_ref':to_dict(directive.source_ref)}), 'review_frontier_omitted_or_changed')
        ranks: tuple[str, ...] = (); decision = None
        reply = value['snapshot'].get('reply')
        disposition = 'stopped' if value['snapshot']['state'] == 'stopped' else 'unknown'
        if reply is not None and reply.get('state') in ('completed', 'failed'):
            disposition = 'failed'
        try:
            require(value['snapshot']['reply']['state'] == 'completed', 'review_unavailable')
            review_changes = value['result_payload']['payload']['changes']
            require(type(review_changes) is dict and set(review_changes) == {'decision.json'} and type(review_changes['decision.json']) is str,
                    'review_file_schema')
            eligible = tuple(owner.trajectory.id+'.'+p.actor for p in proposals.values()
                             if p.disposition == 'eligible' and study.package_for(p.actor) == package)
            decision = decode_decision(review_changes['decision.json'].encode('utf-8'), stage=stage, package=package,
                                       eligible=eligible, release=release, limits=limits)
            ranks = tuple(a.removeprefix(owner.trajectory.id+'.') for a in decision['ranked_actors'])
            disposition = 'completed'
        except (ValueError, TypeError, KeyError, UnicodeError):
            pass  # Invalid/unavailable reviewer data grants no endorsement.
        rankings[package] = contexts.Ranking(package, alias, study.digest(value), disposition, ranks)
        notes.append({'package':package, 'decision':decision, 'status':'decoded' if decision else 'no_valid_endorsement'})
    compiled = contexts.Generation(owner.child.cohort, owner.trajectory.id, milestone, generation, owner.trajectory.arm,
        base, tuple((p, tuple(sorted(owner.plan.package_paths[p]))) for p in study.PACKAGES),
        tuple(proposals[a] for a in study.actors_for(owner.trajectory.arm)[:-4]),
        tuple(rankings[p] for p in study.PACKAGES))
    require(owner.protected.head() == observed_head, 'generation_base_moved_during_original_join')
    owner.records.chain.validate_boundary(expected=expected)
    require(values.exact(contract(owner.plan, limits), declared), 'ranked_contract_changed_during_join')
    return compiled, {'protocol':PROTOCOL, 'contract_sha256':study.digest(declared),
        'original_checkpoint':asdict(expected), 'stage':stage, 'generation_sha256':compiled.sha256,
        'build_originals':build_refs, 'review_originals':review_refs, 'review_definitions':notes,
        'durably_frozen':False, 'dispatch_authority':False, 'acceptance_authority':False}


def _original_prefix(owner: runtime.GossipChildRuntime, sequence: int,
                     expected: chain.PrefixCommitment) -> chain.PrefixCommitment:
    """Reconstruct a historical prefix from the currently authenticated chain.

    The embedded checkpoint is a claim; it cannot authenticate itself. Exact
    deltas supply its head and byte counts, within the chain's existing bounds.
    """
    journal = owner.records.chain
    require(type(sequence) is int and 0 <= sequence <= expected.sequence, 'historical_prefix_out_of_range')
    journal.validate_boundary(expected=expected)
    genesis = chain.stable.read(journal.delta_root/'genesis.json', max_bytes=chain._GENESIS_LIMIT)
    prefix = chain.PrefixCommitment(expected.context_sha256, 0, expected.context_sha256, 0, 0, len(genesis))
    for number in range(1, sequence+1):
        raw = chain.stable.read(journal.delta_root/chain._delta_name(number), max_bytes=chain._DELTA_LIMIT)
        delta = chain.stable.decode(raw, max_bytes=chain._DELTA_LIMIT)
        # position() checks the committed raw file and exact acknowledged delta.
        require(journal.position(delta['name']) == number
                and delta['previous_head_sha256'] == prefix.head_sha256,
                'historical_prefix_original_order_differs')
        prefix = chain.PrefixCommitment(expected.context_sha256, number,
            hashlib.sha256(chain._DELTA_DOMAIN+raw).hexdigest(), number,
            prefix.raw_bytes+delta['bytes'], prefix.external_bytes+len(raw))
    journal.validate_boundary(expected=expected)
    return prefix


def _verify_build_freeze(owner: runtime.GossipChildRuntime, expected: chain.PrefixCommitment,
                         compiled: contexts.Generation, proof: dict[str, Any]) -> dict[str, Any]:
    slot = proof['stage'] + '.before-review-freeze'
    record = owner.records.read(slot)
    require(type(record) is dict and set(record) == set(_build_freeze_value(owner, compiled)) | {'input_checkpoint'},
            'original_build_freeze_missing_or_changed')
    assert record is not None
    require(values.exact({k: v for k, v in record.items() if k != 'input_checkpoint'}, _build_freeze_value(owner, compiled)),
            'original_build_freeze_differs')
    position = owner.records.chain.position(owner.records.name(slot))
    prefix = chain.PrefixCommitment(**record['input_checkpoint'])
    require(prefix.context_sha256 == expected.context_sha256 and prefix.sequence + 1 == position
            and owner.records.chain.position(owner.records.name(proof['stage']+'.builds')) == prefix.sequence,
            'original_build_freeze_not_at_roster_boundary')
    require(prefix == _original_prefix(owner, prefix.sequence, expected),
            'original_build_freeze_predecessor_differs')
    for original in proof['review_originals']:
        value = owner.records.read(original['slot'])
        assert value is not None
        directive = owner.records.name(owner.key + '.directive.' + value['directive_id'])
        require(position < owner.records.chain.position(directive), 'build_freeze_must_precede_review_directives')
    owner.records.chain.validate_boundary(expected=expected)
    return {**proof, 'before_review_freeze': {'slot': slot, 'record_sha256': study.digest(record),
        'position': position}, 'before_review_frozen': True, 'resource_admission': False}


def reconstruct_frozen_builds(owner: runtime.GossipChildRuntime, *, expected: chain.PrefixCommitment,
                             milestone: str, generation: int, limits: ReviewLimits,
                             historical: bool = False) -> tuple[contexts.Generation, dict[str, Any]]:
    """Authenticate the enrolled build census without requiring future reviews."""
    compiled, proof = reconstruct_builds(owner, expected=expected, milestone=milestone,
        generation=generation, limits=limits, historical=historical)
    return compiled, _verify_build_freeze(owner, expected, compiled, proof)


def reconstruct_frozen(owner: runtime.GossipChildRuntime, *, expected: chain.PrefixCommitment,
                       milestone: str, generation: int, limits: ReviewLimits, historical: bool = False) -> tuple[contexts.Generation, dict[str, Any]]:
    """Require original pre-review enrollment, preserving independent chronology."""
    compiled, proof = reconstruct(owner, expected=expected,
        milestone=milestone, generation=generation, limits=limits, historical=historical)
    return compiled, _verify_build_freeze(owner, expected, compiled, proof)
