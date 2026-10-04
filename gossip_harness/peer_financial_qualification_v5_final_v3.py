"""Exact prospective live-admission qualification for V5 and FinalV3.

No historical V3/V4, unavailable-final, failed or infrastructure rehearsal can
enter here. A reference is installed inside the separately approved V5 permit.
The returned object qualifies harness execution only, never a live candidate.
"""
from __future__ import annotations

from pathlib import Path

from . import candidate_observation_admission_v1 as admission
from . import cumulative_rehearsal_capsule_v2 as capsule
from . import cumulative_rehearsal_validator_v2 as mechanics
from . import cumulative_final_acceptance_v3 as final
from .peer_financial_terminal_v1 import require, sha

PROTOCOL = 'peer-financial-qualification-v5-final-v3-v1'
SOURCES = ('gossip_harness/cumulative_rehearsal_codec_v1.py',
    'gossip_harness/cumulative_rehearsal_export_v1.py',
    'gossip_harness/cumulative_final_originals_v1.py',
    'gossip_harness/cumulative_rehearsal_matching_v1.py',
    'gossip_harness/cumulative_rehearsal_capsule_v2.py',
    'gossip_harness/cumulative_rehearsal_validator_v2.py',
    'gossip_harness/peer_financial_qualification_v5_final_v3.py')


def implementation_sources() -> dict[str,str]:
    root = Path(__file__).resolve().parents[1]
    result = {**mechanics.implementation_sources(),**final.implementation_sources(),
        **final.terminal_implementation_sources()}
    result.update({name:sha((root/name).read_bytes()) for name in SOURCES})
    return result


def validate_qualification(reference: dict, execution_design: dict, sources: dict, *, live_limits: dict | None = None) -> dict:
    require(type(reference) is dict and set(reference) == {'protocol','capsule','approved_child'}
        and reference['protocol'] == PROTOCOL,
        'V5 live qualification unavailable: explicit V5/FinalV3 qualification reference required')
    require(execution_design.get('runtime',{}).get('live_qualification_protocol') == PROTOCOL,
            'Live qualification policy was not prospectively pinned')
    current = implementation_sources()
    require(all(sources.get(name) == pin for name,pin in current.items()),
            'Complete actual qualification reader/source contract was not pinned')
    admission.verify_loaded_sources(current)
    result = capsule.audit_closed_capsule(reference['capsule'],execution_design=execution_design,
        sources=sources,approved_child=reference['approved_child'],live_limits=live_limits)
    require(implementation_sources() == current, 'Qualification source changed during original audit')
    admission.verify_loaded_sources(current)
    return {'protocol':PROTOCOL,'reference':reference,'originals':result,
            'candidate_acceptance_authority':False}
