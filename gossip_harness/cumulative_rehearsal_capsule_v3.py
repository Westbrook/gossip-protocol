"""One complete cold rehearsal audit before issuing any live worker lease.

The returned evidence is input to the separately anchored issued-proof producer.
It is not a reusable candidate verdict or a per-invocation raw regrading claim.
"""
from __future__ import annotations
from contextlib import ExitStack
from dataclasses import asdict
import threading
import time
from . import candidate_observation_admission_v1 as admission
from . import cumulative_rehearsal_codec_v1 as codec
from . import cumulative_workflow_exposure_v1 as workflow
from . import cumulative_rehearsal_capsule_v2 as previous
from . import cumulative_final_originals_v1 as originals
from . import cumulative_rehearsal_matching_v1 as matching
from . import cumulative_rehearsal_validator_v2 as mechanics
from . import cumulative_study_successor_v3 as successor
from .cumulative_study_controller_v2 import StudyPlan, digest
from .peer_financial_terminal_v1 import require

PROTOCOL = 'cumulative-rehearsal-capsule-v3-issued-proof-v1'
WALLET_POLICY = 'six-child-wallet-instance-monotonic-opening-v1'
MAX_AUDIT_SECONDS = 1200
POLICY = {'protocol':PROTOCOL, 'complete_trajectories':6, 'planned_roles':96,
    'milestone_histories':24, 'provider_calls':0, 'candidate_acceptance_transfer':False,
    'audit':'complete-originals-once-before-study-deadlines-and-leases',
    'elapsed_boundary_seconds':MAX_AUDIT_SECONDS,
    'deadline_semantics':'checked-after-reader-return; not a hard timeout',
    'wallet_policy':WALLET_POLICY}
_LOCK = threading.Lock()
bound = previous.bound


def wallet_authorization(value: dict, fixture_configs: tuple[dict,...],
                         live_designs: tuple[dict,...], plan: StudyPlan) -> dict:
    originals.closed(value,'protocol ledger_identity expected_global_cap initial_opening_usage children')
    require(value['protocol'] == WALLET_POLICY and type(value['expected_global_cap']) is int
        and type(value['initial_opening_usage']) is int
        and 0 <= value['initial_opening_usage'] < value['expected_global_cap'],
        'Invalid prospective original live wallet')
    originals.closed(value['ledger_identity'],'path device inode')
    originals.path(value['ledger_identity']['path'])
    require(all(type(value['ledger_identity'][key]) is int and value['ledger_identity'][key] > 0
        for key in ('device','inode')), 'Invalid original live ledger identity')
    require(type(value['children']) is list and len(value['children']) == 6
        and len(fixture_configs) == len(live_designs) == 6, 'All six wallet substitutions required')
    for row,config,design,child in zip(value['children'],fixture_configs,live_designs,plan.roster.children,strict=True):
        originals.closed(row,'cohort trajectory incremental_cap_micro_usd max_workers')
        require(row['cohort'] == child.cohort and row['trajectory'] == child.trajectory
            and type(row['incremental_cap_micro_usd']) is int and type(row['max_workers']) is int
            and row['incremental_cap_micro_usd'] == config['incremental_cap_micro_usd']
            and row['max_workers'] == config['max_workers'] == design['max_workers']
            and 1 <= row['max_workers'] <= 4
            and 0 < row['incremental_cap_micro_usd'] <= config['expected_global_cap']-config['expected_opening_usage']
            and row['incremental_cap_micro_usd'] <= value['expected_global_cap']-value['initial_opening_usage'],
            'Prospective fixture/live effective child allowance or concurrency differs')
    return value


def audit_closed_capsule(reference: dict, *, live_plan: StudyPlan, repository: object) -> dict:
    # Contention is an issuance failure before child deadlines, claims or reserve.
    require(_LOCK.acquire(blocking=False), 'A cold issuance audit is active; no live request has been admitted')
    started=time.monotonic()
    def boundary() -> None:
        require(time.monotonic()-started <= MAX_AUDIT_SECONDS, 'Cold issuance audit exceeded its elapsed boundary')
    try:
        value=bound(reference)
        originals.closed(value,'protocol policy fixture_plan live_plan mapping fixture_envelope live_envelope repository ledger_identity study_proof final_packet gate ordered_test_classes wallet_authorization')
        require(value['protocol'] == PROTOCOL and value['policy'] == POLICY, 'Exact issued-proof capsule policy required')
        fixture,live=codec.read(value['fixture_plan']),codec.read(value['live_plan'])
        require(type(fixture) is StudyPlan and type(live) is StudyPlan and type(live_plan) is StudyPlan
            and live == live_plan, 'Exact independently supplied live study required')
        mapping=bound(value['mapping'])
        fixture_envelope,live_envelope=bound(value['fixture_envelope']),bound(value['live_envelope'])
        _,designs=matching.validate_mapping(mapping,fixture,live,fixture_envelope,live_envelope)
        require(fixture.source_pins == live.source_pins, 'Complete prospective source closure differs')
        repo=originals.path(value['repository'])
        require(repo == repository, 'Independently selected repository differs')
        successor.validate_successor(fixture,repo); successor.validate_successor(live,repo)
        boundary()
        with ExitStack() as stack:
            pool=originals.ProofPool({'study':value['study_proof']},stack)
            chain=pool.open('study')
            facts=mechanics.audit_cohort_originals(chain,chain.commitment,plan=fixture,
                ledger_identity=value['ledger_identity'],repository=repo,gate=value['gate'],
                ordered_test_classes=tuple(value['ordered_test_classes']),expected_design_envelope=fixture_envelope)
            wallet=wallet_authorization(value['wallet_authorization'],facts.financial_configs,designs,live)
            study_expected=asdict(chain.commitment); pool.current()
        boundary()
        packet=bound(value['final_packet'])
        fixture_exposure, live_exposure = workflow.contract_fields(fixture), workflow.contract_fields(live)
        if fixture_exposure:
            fixture_exposure = {**workflow.cli.contract_fields(fixture),**fixture_exposure}
        if live_exposure:
            live_exposure = {**workflow.cli.contract_fields(live),**live_exposure}
        require(fixture_exposure == live_exposure
            and all(packet.get(key) == item for key,item in fixture_exposure.items()),
            'Rehearsal workflow/CLI combined identity differs before cold authority reconstruction')
        require(packet['repository'] == str(repo) and packet['ledger_identity'] == value['ledger_identity']
            and packet['proofs'][packet['study']] == value['study_proof'], 'Final proof references a different original study')
        final=originals.audit_final_originals(packet,plan=fixture)
        boundary()
        require(bound(reference) == value, 'Capsule changed during audit')
        fixture.verify_sources(repo); live.verify_sources(repo)
        admission.verify_loaded_sources(live.source_pins)
        require(final['study_sha256'] == facts.execution_contract_sha256 == fixture.sha256,
                'Final/public original execution contracts differ')
        return {'protocol':PROTOCOL,'capsule':reference,'fixture_study_sha256':fixture.sha256,
            'live_study_sha256':live.sha256,'sources':live.source_pins,
            'execution_designs':list(designs),'children':[{'cohort':row.cohort,'trajectory':row.trajectory}
                for row in live.roster.children], 'wallet_authorization':wallet,
            'study_checkpoint':study_expected,'final_originals':final,
            'simulated_usage_micro_usd':sum(row.spent_micro_usd for row in facts.financial_audits),
            'known_failed_fixture_calls':sum(row.known_failures for row in facts.financial_audits),
            'candidate_acceptance_transferred':False,'api_calls':0,'api_spend_micro_usd':0,
            'elapsed_seconds':time.monotonic()-started}
    finally:
        _LOCK.release()
