"""Prospective product observation of the existing finite CLI assertion profile.

The input recipes and semantics were authored for harness qualification and are
public. Executing them afresh after a cohort freeze is fresh independent-purpose
execution, not a held-out or source-blind test. Historical qualification results
are never admitted by this profile. All unspecified cells stay in the roster.
This profile covers declared CLI subfacets, not full B03 or product acceptance.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
from pathlib import Path
from typing import Any

from . import candidate_cli_cases_v1 as original

PROTOCOL = "candidate-cli-acceptance-profile-v1"
PURPOSE = "prospective_product_observation"
ORIGINAL_DEFINITION_PURPOSE = original.PURPOSE
REQUIREMENT_IDS = original.REQUIREMENT_IDS
NORMATIVE_SHA256 = original.NORMATIVE_SHA256
AUTHORING_DISCLOSURE = (
    "The complete existing public qualification recipe and assertion catalog is reused "
    "prospectively without semantic changes. It is not held-out or source-blind; "
    "independent_acceptance denotes fresh execution after a complete cohort freeze. "
    "Historical qualification observations do not have product authority. "
    + original.AUTHORING_DISCLOSURE
)
UNQUALIFIED_FACETS = original.UNQUALIFIED_FACETS


def encoded(value: Any) -> bytes:
    return original.encoded(value)


def definitions() -> tuple[dict[str, Any], ...]:
    values = deepcopy(original.definitions())
    for value in values:
        value["original_definition_purpose"] = ORIGINAL_DEFINITION_PURPOSE
        value["purpose"] = PURPOSE
        value["profile_protocol"] = PROTOCOL
        value["authoring_disclosure"] = AUTHORING_DISCLOSURE
    return values


def case_definition(case_id: str) -> dict[str, Any]:
    for value in definitions():
        if value["case_id"] == case_id:
            return value
    raise KeyError(case_id)


def execution_recipe(case_id: str) -> dict[str, Any]:
    return deepcopy(case_definition(case_id)["recipe"])


def assertion_roster(case_id: str) -> tuple[tuple[str, str, str, bool], ...]:
    """(Registry case ID, ordered step ID, assertion ID, unspecified) cells."""
    value = case_definition(case_id)
    return tuple((case_id + ":" + step["step_id"] + ":" + assertion,
                  step["step_id"], assertion,
                  assertion in value["expectations"][step["step_id"]]["unspecified_assertion_ids"])
                 for step in value["recipe"]["steps"]
                 for assertion in value["expectations"][step["step_id"]]["assertion_ids"])


def ordered_assertion_ids(case_id: str) -> tuple[str, ...]:
    return tuple(cell[0] for cell in assertion_roster(case_id))


def definition_sources() -> dict[str, str]:
    sources = original.definition_sources()
    sources["gossip_harness/candidate_cli_acceptance_profile_v1.py"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    return sources


def definition_sha256() -> str:
    return hashlib.sha256(encoded({"protocol": PROTOCOL, "purpose": PURPOSE,
        "original_definition_purpose": ORIGINAL_DEFINITION_PURPOSE,
        "original_definition_sha256": original.definition_sha256(),
        "definition_sources": definition_sources(), "definitions": definitions(),
        "authoring_disclosure": AUTHORING_DISCLOSURE})).hexdigest()


def profile_sha256() -> str:
    return definition_sha256()
