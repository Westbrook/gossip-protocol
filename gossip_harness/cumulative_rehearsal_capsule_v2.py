"""Cold complete V2/V5 + FinalV3 rehearsal proof, with explicit live mapping.

This module reads existing originals only. The fixture was physically executed
with simulated provider transport; its candidate correctness is not transferred
to any live candidate. All final product purposes remain original and fresh.
"""
from __future__ import annotations

from contextlib import ExitStack
from dataclasses import asdict
from pathlib import Path
import threading
import time

from . import candidate_http_journal_v3 as stable
from . import candidate_observation_admission_v1 as admission
from . import cumulative_rehearsal_codec_v1 as codec
from . import cumulative_final_originals_v1 as final_originals
from . import cumulative_rehearsal_matching_v1 as matching
from . import cumulative_rehearsal_validator_v2 as mechanics
from . import cumulative_study_successor_v3 as successor
from .cumulative_study_controller_v2 import StudyPlan, digest
from .peer_financial_terminal_v1 import require, sha

PROTOCOL = 'cumulative-rehearsal-capsule-v2-final-v3'
MAX_AUDIT_SECONDS = 1200
POLICY = {'protocol':PROTOCOL,'single_auditor':'nonblocking-process-lock',
    'acceptance_elapsed_ceiling_seconds':MAX_AUDIT_SECONDS,'candidate_acceptance_transfer':False,
    'provider_calls':0,'mode':'fixture','complete_trajectories':6,'planned_roles':96,'milestone_histories':24,
    'recheck':'full-original-cohort-and-final-physical-readers-each-invocation',
    'deadline_boundary':'after-each-complete-original-reader; existing inner operation bounds remain required'}
_LOCK = threading.Lock()


def bound(reference: dict) -> dict:
    final_originals.closed(reference,'path sha256')
    raw = stable.read(final_originals.path(reference['path']),max_bytes=32*1024*1024)
    require(sha(raw) == reference['sha256'], 'Independently pinned rehearsal capsule bytes changed')
    value = stable.decode(raw,max_bytes=32*1024*1024)
    require(type(value) is dict and codec.encoded(value) == raw, 'Canonical closed rehearsal capsule required')
    return value


def audit_closed_capsule(reference: dict, *, execution_design: dict, sources: dict,
                         approved_child: dict, live_limits: dict | None = None) -> dict:
    require(_LOCK.acquire(blocking=False), 'Another full qualification audit is active; no unbounded admission queue')
    started = time.monotonic()
    def current_budget() -> None:
        require(time.monotonic()-started <= MAX_AUDIT_SECONDS, 'Full original qualification exceeded its bound')
    try:
        capsule = bound(reference)
        final_originals.closed(capsule,'protocol policy fixture_plan live_plan mapping fixture_envelope live_envelope repository ledger_identity study_proof final_packet gate ordered_test_classes approved_child wallet_mapping')
        require(capsule['protocol'] == PROTOCOL and capsule['policy'] == POLICY
            and capsule['approved_child'] == approved_child, 'Qualification policy/independently approved child differs')
        fixture,live = codec.read(capsule['fixture_plan']),codec.read(capsule['live_plan'])
        require(type(fixture) is StudyPlan and type(live) is StudyPlan, 'Actual fixture/live plans required')
        mapping = bound(capsule['mapping'])
        fixture_envelope,live_envelope = bound(capsule['fixture_envelope']),bound(capsule['live_envelope'])
        _,live_designs = matching.validate_mapping(mapping,fixture,live,fixture_envelope,live_envelope)
        require(fixture.source_pins == live.source_pins == sources, 'Exact complete prospective source closure differs')
        final_originals.closed(approved_child,'cohort trajectory')
        indices = [i for i,child in enumerate(live.roster.children)
            if approved_child == {'cohort':child.cohort,'trajectory':child.trajectory}]
        require(len(indices) == 1 and digest(live_designs[indices[0]]) == digest(execution_design),
                'Qualification does not cover this exact live child design')
        repository = final_originals.path(capsule['repository'])
        successor.validate_successor(fixture,repository)
        successor.validate_successor(live,repository)
        current_budget()
        proof = capsule['study_proof']
        with ExitStack() as stack:
            pool = final_originals.ProofPool({'study':proof},stack)
            chain = pool.open('study')
            facts = mechanics.audit_cohort_originals(chain,chain.commitment,plan=fixture,
                ledger_identity=capsule['ledger_identity'],repository=repository,gate=capsule['gate'],
                ordered_test_classes=tuple(capsule['ordered_test_classes']),expected_design_envelope=fixture_envelope)
            require(live_limits is not None, 'Actual original live permit limits are required')
            assert live_limits is not None
            wallet_mapping = matching.validate_wallet_mapping(capsule['wallet_mapping'],
                facts.financial_configs[indices[0]],live_limits)
            study_expected = asdict(chain.commitment)
            pool.current()
        current_budget()
        packet = bound(capsule['final_packet'])
        require(packet['repository'] == str(repository) and packet['ledger_identity'] == capsule['ledger_identity']
            and packet['proofs'][packet['study']] == proof, 'Final proof references a different study original')
        final = final_originals.audit_final_originals(packet,plan=fixture)
        current_budget()
        # The target design and sources remain exact after all potentially slow
        # original reads. Financial entry independently rechecks its live lease.
        require(bound(reference) == capsule, 'Qualification capsule changed during audit')
        fixture.verify_sources(repository); live.verify_sources(repository)
        admission.verify_loaded_sources(sources)
        require(final['study_sha256'] == facts.execution_contract_sha256 == fixture.sha256,
                'Final and public original execution contracts differ')
        return {'protocol':PROTOCOL,'capsule':reference,'fixture_study_sha256':fixture.sha256,
            'live_study_sha256':live.sha256,'approved_child':approved_child,
            'execution_design_sha256':digest(execution_design),'sources':sources,'wallet_mapping':wallet_mapping,
            'study_checkpoint':study_expected,'final_originals':final,
            'simulated_usage_micro_usd':sum(row.spent_micro_usd for row in facts.financial_audits),
            'known_failed_fixture_calls':sum(row.known_failures for row in facts.financial_audits),
            'api_calls':0,'api_spend_micro_usd':0,'candidate_acceptance_transferred':False,
            'scope':'complete matching harness rehearsal; no live candidate verdict'}
    finally:
        _LOCK.release()
