"""Prospectively bound V2 study and final acceptance with V5 finance.

The actual public runtime keeps its acceptance_unavailable result. Independent
final observations determine acceptance through the concrete V2 final owner.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from . import candidate_observation_admission_v1 as admission
from . import candidate_scope_consumer_v1 as consumer
from . import cumulative_final_acceptance_v2 as final
from . import cumulative_study_successor_v1 as shared
from .cumulative_final_acceptance_v1 import ObservationSpec
from .cumulative_study_controller_v2 import PROTOCOL as PUBLIC_PROTOCOL, StudyPlan, digest

PROTOCOL = 'cumulative-study-successor-v2'
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def validate_successor(plan: StudyPlan, repository: Path) -> dict[str, Any]:
    """Require the actual V2/V5 execution and complete prospective authority."""
    consumer.require(type(plan) is StudyPlan, 'Exact V2 prospective study plan required')
    plan.__post_init__()  # Recheck mutable policy/runtime fields at this boundary.
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
    sources: dict[str, str] = {}
    for group in (final.implementation_sources(), final.terminal_implementation_sources(), {
        'gossip_harness/cumulative_study_successor_v1.py':
            hashlib.sha256(Path(shared.__file__).read_bytes()).hexdigest(),
        'gossip_harness/cumulative_study_successor_v2.py':
            hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }):
        for name, pin in group.items():
            consumer.require(name not in sources or sources[name] == pin,
                             'Conflicting successor authority source identity')
            sources[name] = pin
    consumer.require(all(plan.source_pins.get(name) == pin for name, pin in sources.items()),
                     'Successor and final authority were not prospectively source-bound')
    plan.verify_sources(repository)
    admission.verify_loaded_sources(sources)
    return {'protocol': PROTOCOL, 'public_phase_protocol': PUBLIC_PROTOCOL,
        'final_phase_protocol': final.PROTOCOL, 'original_execution_contract_sha256': plan.sha256,
        'original_terminal_roster_sha256': plan.roster.sha256, 'sources': sources,
        'interpretation': 'public phase remains diagnostic; independent final evidence determines registry acceptance'}


def run_public_phase(plan: StudyPlan, *, repository: Path, **runtime_inputs: Any) -> dict[str, Any]:
    """Run the concrete V2 runtime using the caller's original V5 permits."""
    from .cumulative_study_runtime_v2 import run_study
    contract = validate_successor(plan, repository)
    consumer.require(runtime_inputs.get('mode') == plan.runtime['final_acceptance_financial_mode'],
                     'Runtime financial mode differs from original final interpretation')
    public = run_study(plan, repository=repository, **runtime_inputs)
    consumer.require(public['status'] == 'acceptance_unavailable' and public['accepted'] is False,
                     'Original public controller changed its declared boundary')
    return {'protocol': PROTOCOL, 'contract': contract, 'contract_sha256': digest(contract),
            'public_phase': public, 'final_phase': 'not_executed', 'accepted': False}


def run_final_phase(owner: final.FinalAcceptanceV2,
                    specifications: tuple[ObservationSpec, ...]) -> dict[str, Any]:
    """Use the exact V2 final owner and shared closed final-phase mechanics."""
    consumer.require(type(owner) is final.FinalAcceptanceV2 and type(specifications) is tuple
                     and all(type(item) is ObservationSpec for item in specifications),
                     'Exact V2 prospective final owner and execution specifications required')
    return shared._run_final_phase(owner, specifications)
