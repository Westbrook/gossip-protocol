"""Explicit V2 study/V5 finance acceptance using the original final mechanics.

This version admits the independently reviewed storage observation family. It
still requires complete semantic scope, six authentic final sources and original
qualification/promotion evidence; a repaired financial closure supplies none of
those authorities by itself. The V1 default remains its original closed contract.
"""
from __future__ import annotations

import hashlib
from pathlib import Path

from . import cumulative_final_acceptance_v1 as shared
from . import cumulative_scope_authority_v2 as scope_authority
from .cumulative_study_controller_v2 import StudyPlan

PROTOCOL = 'cumulative-final-acceptance-v2'
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def implementation_sources() -> dict[str, str]:
    catalog = scope_authority.source.load_catalog(Path(__file__).resolve().parents[1])
    return shared._canonical_sources((shared.implementation_sources(),
        {'gossip_harness/cumulative_final_acceptance_v2.py': hashlib.sha256(Path(__file__).read_bytes()).hexdigest()},
        scope_authority.implementation_sources(), dict(catalog.source_pins), terminal_implementation_sources()))


def terminal_implementation_sources() -> dict[str, str]:
    return shared._terminal_sources('cumulative_terminal_originals_v2')


class FinalAcceptanceV2(shared.FinalAcceptance):
    """Exact V2 type selects only the source-defined V2/V5 contract."""
