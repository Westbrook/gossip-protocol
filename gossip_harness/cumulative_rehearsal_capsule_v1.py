"""Serialized original-proof ownership for a closed historical rehearsal.

Expected head/context/roots are supplied in an independently hash-pinned capsule;
no local summary or anchor alone supplies freshness. ExternalHead.reopen takes
an exclusive lock and fsyncs current state, so this is evidence-byte-preserving,
not side-effect-free. Active owner locks must remain in force. Live qualification
stays unavailable until the complete final acceptance producer exists.
"""
from __future__ import annotations

from dataclasses import fields
from pathlib import Path
import threading
from typing import Any

from . import candidate_http_journal_v3 as stable
from .candidate_checkpoint_chain_v1 import CheckpointChain, PrefixCommitment, Limits
from .candidate_checkpoint_head_v1 import ExternalHead
from .cumulative_rehearsal_validator_v1 import audit_original_rehearsal, checked_design_envelope, RehearsalAudit
from .cumulative_study_controller_v1 import StudyPlan, Release, PROTOCOL as STUDY_PROTOCOL
from .peer_financial_authority_v2 import canonical_payload
from .peer_financial_terminal_v1 import digest, require, sha
from .project_acceptance_compiler_v1 import CohortDesign, Trajectory

PROTOCOL = 'cumulative-rehearsal-capsule-v1'
_LOCK = threading.Lock()
MAX_CAPSULE_BYTES = 16_000_000


def _closed(value: Any, names: set[str]) -> dict:
    require(type(value) is dict and set(value) == names, 'Closed qualification reference fields differ')
    return value


def decode_plan(value: dict) -> StudyPlan:
    """Reconstruct and validate the actual controller classes, not a copy schema."""
    value = dict(_closed(value, {'protocol'} | {item.name for item in fields(StudyPlan)}))
    require(value.pop('protocol') == STUDY_PROTOCOL, 'Unknown actual study protocol')
    cohort = dict(_closed(value['cohort'], {item.name for item in fields(CohortDesign)}))
    require(type(cohort['trajectories']) is list and len(cohort['trajectories']) == 6,
            'Complete six-child prospective roster required')
    trajectories = []
    for item in cohort['trajectories']:
        row = dict(_closed(item, {part.name for part in fields(Trajectory)}))
        require(type(row['role_ids']) is list and type(row['faults']) is list,
                'Role and fault arrays required')
        trajectories.append(Trajectory(**{**row, 'role_ids': tuple(row['role_ids']), 'faults': tuple(row['faults'])}))
    require(type(cohort['milestones']) is list, 'Ordered original milestones required')
    cohort['trajectories'], cohort['milestones'] = tuple(trajectories), tuple(cohort['milestones'])
    value['cohort'] = CohortDesign(**cohort)
    require(type(value['releases']) is list and len(value['releases']) == 4, 'All four original releases required')
    releases = []
    for item in value['releases']:
        row = dict(_closed(item, {part.name for part in fields(Release)}))
        for key in ('command', 'ordered_check_ids', 'requirement_ids'):
            require(type(row[key]) is list, 'Ordered original release arrays required')
            row[key] = tuple(row[key])
        releases.append(Release(**row))
    value['releases'] = tuple(releases)
    require(type(value['package_paths']) is dict and all(type(item) is list for item in value['package_paths'].values()),
            'Exact package path arrays required')
    value['package_paths'] = {key: tuple(item) for key, item in value['package_paths'].items()}
    return StudyPlan(**value)


def audit_closed_capsule(reference: dict, *, execution_design: dict, sources: dict,
                         design_envelope: dict, approved_child: dict) -> RehearsalAudit:
    """Open, audit and close historical proof on the calling thread; never live."""
    _closed(reference, {'path', 'sha256'})
    path = Path(reference['path'])
    require(path.is_absolute() and path.resolve() == path, 'Canonical pinned capsule path required')
    raw = stable.read(path, max_bytes=MAX_CAPSULE_BYTES)
    require(sha(raw) == reference['sha256'], 'Independently pinned capsule bytes changed')
    capsule = stable.decode(raw, max_bytes=MAX_CAPSULE_BYTES)
    _closed(capsule, {'protocol', 'execution_design_sha256', 'sources', 'gate', 'ordered_test_classes', 'proof', 'design_envelope_sha256', 'approved_child'})
    require(capsule['protocol'] == PROTOCOL and capsule['execution_design_sha256'] == digest(execution_design)
            and capsule['sources'] == sources and canonical_payload(capsule) == raw,
            'Exact approved design/source/capsule bytes differ')
    proof = _closed(capsule['proof'], {'repository', 'plan', 'ledger_identity', 'raw_root', 'delta_root',
                                      'head_root', 'context', 'limits', 'expected'})
    plan = decode_plan(proof['plan'])
    require(plan.source_pins == sources and execution_design.get('terminal_roster') == plan.roster.record(),
            'Prospective design/plan/source linkage differs')
    _closed(design_envelope, {'path', 'sha256'})
    envelope_path = Path(design_envelope['path'])
    require(envelope_path.is_absolute() and envelope_path.resolve() == envelope_path,
            'Canonical independently approved envelope path required')
    envelope_raw = stable.read(envelope_path, max_bytes=MAX_CAPSULE_BYTES)
    require(sha(envelope_raw) == design_envelope['sha256'] == capsule['design_envelope_sha256'],
            'Independent complete design-envelope pin differs')
    envelope = stable.decode(envelope_raw, max_bytes=MAX_CAPSULE_BYTES)
    require(canonical_payload(envelope) == envelope_raw, 'Canonical complete design envelope required')
    designs = checked_design_envelope(envelope, plan)
    _closed(approved_child, {'cohort', 'trajectory'})
    require(capsule['approved_child'] == approved_child, 'Caller child approval binding differs')
    matches = [i for i, child in enumerate(plan.roster.children)
               if approved_child == {'cohort': child.cohort, 'trajectory': child.trajectory}]
    require(len(matches) == 1 and digest(designs[matches[0]]) == digest(execution_design),
            'Caller child design is not the corresponding independently approved entry')
    require(type(capsule['ordered_test_classes']) is list, 'Ordered qualification classes required')
    ordered = tuple(capsule['ordered_test_classes'])
    expected = PrefixCommitment(**_closed(proof['expected'], {item.name for item in fields(PrefixCommitment)}))
    limits = Limits(**_closed(proof['limits'], {item.name for item in fields(Limits)}))
    roots = tuple(Path(proof[key]) for key in ('raw_root', 'delta_root', 'head_root'))
    require(all(path.is_absolute() and path.resolve() == path for path in roots), 'Canonical proof roots required')
    repository = Path(proof['repository'])
    require(repository.is_absolute() and repository.resolve() == repository, 'Canonical execution repository required')
    # A live controller still owns its external/chain locks. Reopen must reject
    # that lifetime rather than borrowing its owner or weakening freshness.
    with _LOCK:
        authority = ExternalHead.reopen(roots[2], journal_roots=(roots[0], roots[1]), expected=expected)
        try:
            chain = CheckpointChain.reopen(roots[0], roots[1], context=proof['context'], authority=authority,
                                           expected=expected, limits=limits)
            try:
                return audit_original_rehearsal(chain, expected, plan=plan,
                    ledger_identity=proof['ledger_identity'], repository=repository,
                    gate=capsule['gate'], ordered_test_classes=ordered, expected_design_envelope=envelope)
            finally:
                chain.close()
        finally:
            authority.close()
