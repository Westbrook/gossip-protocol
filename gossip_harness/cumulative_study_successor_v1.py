"""Explicit successor composition around the unchanged public study controller.

The public controller still returns acceptance_unavailable. This entry introduces
an independently journaled final phase under a prospectively pinned protocol.
It never overwrites or reinterprets that original public-controller receipt.
"""
from __future__ import annotations

from dataclasses import asdict
import hashlib
from pathlib import Path
from typing import Any

from . import candidate_scope_consumer_v1 as consumer
from . import cumulative_final_acceptance_v1 as final
from .cumulative_study_controller_v1 import StudyPlan, digest

PROTOCOL = 'cumulative-study-successor-v1'
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def validate_successor(plan: StudyPlan, repository: Path) -> dict[str, Any]:
    """Require prior inclusion in the original financial/execution contract."""
    consumer.require(type(plan) is StudyPlan, 'Exact V1 prospective study plan required')
    plan.__post_init__()  # Nested plan dictionaries remain mutable after construction.
    consumer.require(plan.runtime.get('final_acceptance_protocol') == final.PROTOCOL
        and plan.runtime.get('study_successor_protocol') == PROTOCOL
        and plan.runtime.get('final_acceptance_financial_mode') in ('live', 'fixture'),
        'A historical public-only study cannot be relabeled as this successor')
    roots = plan.runtime.get('final_acceptance_roots')
    consumer.require(type(roots) is dict and set(roots) == {'raw', 'delta', 'head'}
        and all(type(value) is str and Path(value).is_absolute() and str(Path(value).resolve()) == value
                for value in roots.values()), 'Original canonical acceptance session roots required')
    assert isinstance(roots, dict)
    paths = tuple(Path(value) for value in roots.values())
    consumer.require(all(not a.is_relative_to(b) and not b.is_relative_to(a)
        for index, a in enumerate(paths) for b in paths[index + 1:]),
        'Prospective acceptance roots must be pairwise disjoint')
    sources = {**final.implementation_sources(), **final.terminal_implementation_sources(),
        'gossip_harness/cumulative_study_successor_v1.py': hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    consumer.require(all(plan.source_pins.get(name) == pin for name, pin in sources.items()),
                     'Successor and final authority were not prospectively source-bound')
    plan.verify_sources(repository)
    final.admission.verify_loaded_sources(sources)
    return {'protocol': PROTOCOL, 'public_phase_protocol': 'cumulative-study-controller-v1',
        'final_phase_protocol': final.PROTOCOL, 'original_execution_contract_sha256': plan.sha256,
        'original_terminal_roster_sha256': plan.roster.sha256, 'sources': sources,
        'interpretation': 'public phase remains diagnostic; independent final evidence determines registry acceptance'}


def run_public_phase(plan: StudyPlan, *, repository: Path, **runtime_inputs: Any) -> dict[str, Any]:
    """Explicit real study invocation; this can spend through caller permits.

    No keys, wallets or permits are created. All original V4 financial controls
    and the complete matching rehearsal remain required by the concrete runtime.
    """
    from .cumulative_study_runtime_v1 import run_study
    contract = validate_successor(plan, repository)
    consumer.require(runtime_inputs.get('mode') == plan.runtime['final_acceptance_financial_mode'],
                     'Runtime financial mode differs from original final interpretation')
    public = run_study(plan, repository=repository, **runtime_inputs)
    consumer.require(public['status'] == 'acceptance_unavailable' and public['accepted'] is False,
                     'Original public controller changed its declared boundary')
    return {'protocol': PROTOCOL, 'contract': contract, 'contract_sha256': digest(contract),
            'public_phase': public, 'final_phase': 'not_executed', 'accepted': False}


def run_final_phase(owner: final.FinalAcceptance, specifications: tuple[final.ObservationSpec, ...]) -> dict[str, Any]:
    """Use actual installed complete scope and owners; no factory/result callback.

    Missing authorities preserve an incomplete outcome. A failed physical call
    is retained by its existing intent and is never retried. Callers own final
    owner.close() after inspection; all six planned slots remain in the result.
    A crash after final.contract cannot reopen this acceptance session; retained
    originals remain diagnostic pending a separately versioned recovery reader.
    """
    consumer.require(type(owner) is final.FinalAcceptance and type(specifications) is tuple
                     and all(type(item) is final.ObservationSpec for item in specifications),
                     'Exact prospective final owner and execution specifications required')
    return _run_final_phase(owner, specifications)


def _run_final_phase(owner: final.FinalAcceptance,
                     specifications: tuple[final.ObservationSpec, ...]) -> dict[str, Any]:
    # Only the two source-defined public compositions are eligible.
    protocol, validate = _known_successor(owner)
    consumer.require(type(specifications) is tuple
        and all(type(item) is final.ObservationSpec for item in specifications), 'Exact observation roster required')
    contract: dict[str, Any] = {'protocol': protocol, 'unverified_contract': True}
    try:
        contract = final.normalize_authority(validate)(owner.plan, owner.repository)
        final.normalize_authority(owner._put)('successor.contract', {**contract, 'contract_sha256': digest(contract)})
        final.normalize_authority(owner.prepare)()
        registrations = [asdict(final.normalize_authority(spec.observation_registration)()) for spec in specifications]
        final.normalize_authority(owner._put)('successor.observation-roster', {'protocol': protocol,
            'registrations': registrations})
    except (consumer.AuthorityError, consumer.AuthorityUnavailable) as error:
        return _incomplete(owner, contract, [{'gate': None, 'phase': 'pre_dispatch',
            'status': 'invalid' if isinstance(error, consumer.AuthorityError) else 'unavailable',
            'error': type(error).__name__ + ': ' + str(error)}])
    errors = []
    for spec in specifications:
        gate = None
        try:
            gate = asdict(final.normalize_authority(spec.observation_registration)().gate)
            final.normalize_authority(owner.dispatch)(spec)
            if owner.dispatch_halt is not None:
                errors.append({'gate': gate, 'status': 'unavailable', 'error': owner.dispatch_halt['reason']})
                break
        except (consumer.AuthorityError, consumer.AuthorityUnavailable) as error:
            errors.append({'gate': gate, 'status': 'invalid' if isinstance(error, consumer.AuthorityError)
                           else 'unavailable', 'error': type(error).__name__ + ': ' + str(error)})
            try:
                final.normalize_authority(owner._current_originals)()
            except (consumer.AuthorityError, consumer.AuthorityUnavailable) as current_error:
                errors.append({'gate': None, 'phase': 'authority_revalidation',
                    'status': 'invalid' if isinstance(current_error, consumer.AuthorityError) else 'unavailable',
                    'error': type(current_error).__name__ + ': ' + str(current_error)})
                return _incomplete(owner, contract, errors)
            break  # Never dispatch more work after uncertain physical cleanup.
    try:
        assessed = final.normalize_authority(owner.assess)()
        final.normalize_authority(owner._current_originals)()
    except (consumer.AuthorityError, consumer.AuthorityUnavailable) as error:
        errors.append({'gate': None, 'phase': 'assessment',
            'status': 'invalid' if isinstance(error, consumer.AuthorityError) else 'unavailable',
            'error': type(error).__name__ + ': ' + str(error)})
        return _incomplete(owner, contract, errors)
    return {'protocol': protocol, 'contract_sha256': digest(contract),
        'original_execution_contract_sha256': owner.plan.sha256, 'final_phase': assessed,
        'observation_errors': errors, 'accepted': assessed['accepted'] and not errors,
        'completed_and_accepted': assessed['completed_and_accepted'] and not errors}


def _incomplete(owner: final.FinalAcceptance, contract: dict[str, Any], errors: list[dict[str, Any]]) -> dict[str, Any]:
    """Return all planned slots without treating revoked originals as current."""
    protocol, _ = _known_successor(owner)
    return {'protocol': protocol, 'contract_sha256': digest(contract), 'accepted': False,
        'completed_and_accepted': False, 'status': 'invalid_evidence' if any(row['status'] == 'invalid' for row in errors)
            else 'unavailable', 'observation_errors': errors,
        'planned_trajectories': [{'trajectory': child.trajectory, 'current_status': 'evidence_unknown',
            'accepted_against_registry': False,
            'last_authenticated_physical_diagnostics': [asdict(row.observation) for row in owner.enrollments.values()
                if row.observation is not None and row.registration.gate.binding.subject.trajectory_id == child.trajectory]}
            for child in owner.plan.roster.children]}


def _known_successor(owner: final.FinalAcceptance) -> tuple[str, Any]:
    if type(owner) is final.FinalAcceptance:
        return PROTOCOL, validate_successor
    from . import cumulative_final_acceptance_v2 as v2
    from . import cumulative_study_successor_v2 as successor_v2
    if type(owner) is v2.FinalAcceptanceV2:
        return successor_v2.PROTOCOL, successor_v2.validate_successor
    from . import cumulative_final_acceptance_v3 as v3
    from . import cumulative_study_successor_v3 as successor_v3
    if type(owner) is v3.FinalAcceptanceV3:
        return successor_v3.PROTOCOL, successor_v3.validate_successor
    raise consumer.AuthorityError('Unknown final acceptance successor')
