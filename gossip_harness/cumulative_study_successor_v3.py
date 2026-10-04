"""V3 entry: unchanged V2/V5 public study, original-qualified final dispatch."""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from . import candidate_scope_consumer_v1 as consumer
from . import cumulative_final_acceptance_v1 as mechanics
from . import cumulative_final_acceptance_v3 as final
from . import cumulative_study_successor_v1 as shared
from . import cumulative_prerequisite_qualification_v1 as qualification
from . import cumulative_source_promotion_v1 as promotion
from .cumulative_study_controller_v2 import StudyPlan, PROTOCOL as PUBLIC_PROTOCOL, digest

PROTOCOL = 'cumulative-study-successor-v3'
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def validate_successor(plan: StudyPlan, repository: Path) -> dict[str, Any]:
    consumer.require(type(plan) is StudyPlan, 'Exact V2/V5 prospective study plan required')
    plan.__post_init__()
    consumer.require(plan.runtime.get('final_acceptance_protocol') == final.PROTOCOL
        and plan.runtime.get('study_successor_protocol') == PROTOCOL
        and plan.runtime.get('prerequisite_qualification_protocol') == qualification.PROTOCOL
        and plan.runtime.get('final_promotion_protocol') == promotion.PROTOCOL
        and plan.runtime.get('final_acceptance_financial_mode') in ('live','fixture'),
        'Original qualification/promotion successor was not prospectively bound')
    roots = plan.runtime.get('final_acceptance_roots')
    consumer.require(type(roots) is dict and set(roots) == {'raw','delta','head'}
        and all(type(value) is str and Path(value).is_absolute() and str(Path(value).resolve()) == value
                for value in roots.values()), 'Canonical original final roots required')
    assert isinstance(roots, dict)
    paths = tuple(Path(value) for value in roots.values())
    consumer.require(all(not a.is_relative_to(b) and not b.is_relative_to(a)
        for i,a in enumerate(paths) for b in paths[i+1:]), 'Final proof roots must be disjoint')
    sources = {**final.implementation_sources(), **final.terminal_implementation_sources(),
        'gossip_harness/cumulative_study_successor_v1.py':hashlib.sha256(Path(shared.__file__).read_bytes()).hexdigest(),
        'gossip_harness/cumulative_study_successor_v3.py':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    consumer.require(all(plan.source_pins.get(name) == pin for name,pin in sources.items()),
        'Complete original authority/successor source was not prospectively pinned')
    plan.verify_sources(repository)
    mechanics.admission.verify_loaded_sources(sources)
    return {'protocol':PROTOCOL,'public_phase_protocol':PUBLIC_PROTOCOL,'final_phase_protocol':final.PROTOCOL,
        'original_execution_contract_sha256':plan.sha256,'original_terminal_roster_sha256':plan.roster.sha256,
        'sources':sources,'interpretation':'public results remain diagnostic; actual original prerequisites and final evidence are mandatory'}


def run_public_phase(plan: StudyPlan, *, repository: Path, **runtime_inputs: Any) -> dict[str, Any]:
    from .cumulative_study_runtime_v2 import run_study
    contract = validate_successor(plan,repository)
    consumer.require(runtime_inputs.get('mode') == plan.runtime['final_acceptance_financial_mode'],
        'Financial origin differs from original interpretation')
    public = run_study(plan,repository=repository,**runtime_inputs)
    consumer.require(public['status'] == 'acceptance_unavailable' and public['accepted'] is False,
        'Public-only controller changed its authority boundary')
    return {'protocol':PROTOCOL,'contract':contract,'contract_sha256':digest(contract),
        'public_phase':public,'final_phase':'not_executed','accepted':False}


def run_final_phase(owner: final.FinalAcceptanceV3,
                    specifications: tuple[mechanics.ObservationSpec, ...]) -> dict[str, Any]:
    """Existing original-qualified phases must be supplied; never invent reviews.

    Host first prepares the six-source freeze, binds exact original capabilities,
    executes the separately registered qualification fixtures, obtains enrolled
    independent reviews, and publishes their verifiers. This method dispatches
    only after those originals qualify each prospective product gate.
    """
    consumer.require(type(owner) is final.FinalAcceptanceV3, 'Exact V3 original authority required')
    return shared._run_final_phase(owner,specifications)
