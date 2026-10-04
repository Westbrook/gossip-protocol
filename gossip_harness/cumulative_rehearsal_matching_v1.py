"""Prospective closed fixture-to-live instance relation; no verdict transfer."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict
from typing import Any

from .cumulative_study_controller_v2 import StudyPlan, digest
from .cumulative_rehearsal_validator_v2 import checked_design_envelope
from .peer_financial_authority_v3 import FIXTURE_TRANSPORT, LIVE_TRANSPORT
from .peer_financial_terminal_v1 import require

PROTOCOL = 'cumulative-rehearsal-instance-mapping-v1'


def mapping_for(fixture_cohort: str, live_cohort: str, trajectories: tuple[tuple[str,str], ...],
                fixture_roots: dict[str,str], live_roots: dict[str,str]) -> dict:
    """Declare before constructing either plan; resulting digest goes in both."""
    return {'protocol':PROTOCOL,'cohort_ids':[fixture_cohort,live_cohort],
        'trajectory_ids':[list(row) for row in trajectories], 'final_roots':[fixture_roots,live_roots],
        'financial_modes':['fixture','live'],'provider_transports':[FIXTURE_TRANSPORT,LIVE_TRANSPORT],
        'purpose':'matching-harness-rehearsal-for-live-admission','candidate_acceptance_transfer':False}


def validate_mapping(mapping: dict, fixture: StudyPlan, live: StudyPlan,
                     fixture_envelope: dict, live_envelope: dict) -> tuple[tuple[dict,...],tuple[dict,...]]:
    require(type(fixture) is StudyPlan and type(live) is StudyPlan, 'Exact V2 instance plans required')
    fixture.__post_init__(); live.__post_init__()
    pairs = tuple((left.id,right.id) for left,right in zip(fixture.cohort.trajectories,live.cohort.trajectories,strict=True))
    expected = mapping_for(fixture.cohort.cohort_id,live.cohort.cohort_id,pairs,
        fixture.runtime['final_acceptance_roots'],live.runtime['final_acceptance_roots'])
    require(type(mapping) is dict and digest(mapping) == digest(expected), 'Unauthorized instance-mapping fields or values')
    require(fixture.cohort.cohort_id != live.cohort.cohort_id
        and fixture.runtime['final_acceptance_financial_mode'] == 'fixture'
        and live.runtime['final_acceptance_financial_mode'] == 'live', 'Distinct fixture/live financial origins required')
    require(fixture.runtime.get('rehearsal_instance_mapping_sha256') == digest(mapping)
        and live.runtime.get('rehearsal_instance_mapping_sha256') == digest(mapping),
        'Instance mapping was not prospectively pinned by both execution plans')
    from .cumulative_final_originals_v1 import path
    roots = tuple(path(value) for item in mapping['final_roots'] for value in item.values())
    require(len(roots) == 6 and all(not a.is_relative_to(b) and not b.is_relative_to(a)
        for i,a in enumerate(roots) for b in roots[i+1:]), 'Fixture/live final proof roots must be distinct and disjoint')
    # The allowlist is expressed by constructing the only eligible live plan,
    # rather than deleting fields or accepting caller-selected JSON pointers.
    transformed = deepcopy(asdict(fixture))
    transformed['cohort']['cohort_id'] = live.cohort.cohort_id
    for record,(_,target_id) in zip(transformed['cohort']['trajectories'],pairs,strict=True):
        record['id'] = target_id
    transformed['runtime']['final_acceptance_financial_mode'] = 'live'
    transformed['runtime']['final_acceptance_roots'] = deepcopy(live.runtime['final_acceptance_roots'])
    transformed['cohort']['resource_contract_sha256'] = digest({
        'horizon_seconds':fixture.horizon_seconds,'source_generations':fixture.source_generations,
        'partition_seconds':fixture.partition_seconds,'executor_slots':fixture.executor_slots,
        'runtime':transformed['runtime']})
    require(digest(transformed) == digest(asdict(live)),
        'Fixture/live source, limits, seed, suites or semantic study design differ outside the closed instance mapping')
    left = checked_design_envelope(fixture_envelope,fixture)
    right = checked_design_envelope(live_envelope,live)
    for index,(original,target) in enumerate(zip(left,right,strict=True)):
        projected: dict[str,Any] = deepcopy(original)
        projected['runtime'] = deepcopy(live.runtime)
        projected['terminal_roster'] = live.roster.record()
        source_actors = fixture.roster.children[index].actors
        target_actors = live.roster.children[index].actors
        projected['action_limits']['by_actor'] = {new:original['action_limits']['by_actor'][old]
            for old,new in zip(source_actors,target_actors,strict=True)}
        require(digest(projected) == digest(target),
                'Live child design differs outside declared runtime/roster/actor instance substitutions')
    return left,right


WALLET_FIELDS = ('expected_opening_usage','expected_global_cap','incremental_cap_micro_usd','max_workers')
WALLET_POLICY = 'same-effective-child-cap-explicit-original-wallet-instance-v1'


def validate_wallet_mapping(mapping: dict, fixture_config: dict, live_limits: dict) -> dict:
    """Map accounting instances, retaining the identical effective child budget.

    Both wallets must have room for the same whole child allowance. Thus neither
    global remainder truncates the matched child budget. Opening usage and the
    separately approved cumulative cap remain explicit original-instance data;
    this mapping cannot enroll a ledger, increase an allowance, or spend funds.
    """
    require(type(mapping) is dict and set(mapping) == {'protocol','fixture','live'}
        and mapping['protocol'] == WALLET_POLICY, 'Closed prospective wallet substitution required')
    require(type(live_limits) is dict and set(live_limits) == set(WALLET_FIELDS), 'Exact approved live wallet limits required')
    fixture = {name:fixture_config[name] for name in WALLET_FIELDS}
    require(mapping['fixture'] == fixture and mapping['live'] == live_limits,
            'Wallet substitution differs from original fixture or actual live permit')
    for record in (fixture,live_limits):
        require(all(type(record[name]) is int for name in WALLET_FIELDS)
            and 0 <= record['expected_opening_usage'] <= record['expected_global_cap']
            and 0 < record['incremental_cap_micro_usd'] <= record['expected_global_cap']-record['expected_opening_usage']
            and 1 <= record['max_workers'] <= 4, 'Wallet substitution violates the original bounded allowance')
    require(fixture['incremental_cap_micro_usd'] == live_limits['incremental_cap_micro_usd']
        and fixture['max_workers'] == live_limits['max_workers'], 'Fixture/live effective child allowance or concurrency differs')
    return mapping
