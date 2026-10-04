"""Pure M4 composition of the frozen HTTP comparator and cumulative profile.

Only the three source-declared health successors change a body-value comparison.
Original M1 expectations, all other facets and all ambiguity/decoder policies
remain frozen. Supplied facts gain no provenance or acceptance authority here.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
from pathlib import Path

from . import candidate_http_cases_v1 as catalog
from . import candidate_http_semantics_v1 as semantics
from . import cumulative_observation_profile_v1 as profiles

PROTOCOL = "candidate-http-m4-semantics-v1"
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
_SEMANTICS_SOURCE = "gossip_harness/candidate_http_semantics_v1.py"


class ComparatorError(ValueError):
    """Invalid comparator binding or supplied input, never a product failure."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ComparatorError(message)


@dataclass(frozen=True, slots=True)
class HttpM4Comparator:
    """Bind once, compare semantic steps, then recheck at owner boundaries.

    The execution owner must call ``check_current`` before and after diagnosis
    and separately bind this module's source. No complete catalog/profile is
    rebuilt per response. Relations, raw facts and lifecycle steps retain their
    existing owner adapters and cannot be routed through this comparator.
    """

    profile: profiles.CumulativeProfile
    case: catalog.LiteralCase = field(init=False)
    _successors: tuple[profiles.HealthSuccessor, ...] = field(init=False, repr=False)
    protocol: str = field(default=PROTOCOL, init=False)
    authority: str = field(default="pure_value_comparison_only", init=False)
    acceptance_authority: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        _require(type(self.profile) is profiles.CumulativeProfile
                 and self.profile.family == "http", "Exact cumulative HTTP profile required")
        profiles.assert_profile_current(self.profile)
        cases = tuple(case for case in catalog.definitions() if case.row_id == self.profile.case_id)
        _require(len(cases) == 1, "Original HTTP history must resolve exactly once")
        object.__setattr__(self, "case", cases[0])
        object.__setattr__(self, "_successors", self.profile.health_successors)
        self._check_binding()

    def _check_binding(self) -> None:
        record = self.profile.record()
        _require(self.case.record() == record["original_definition"],
                 "Original HTTP history differs from the complete profile")
        _require(record["definition_sources"][_SEMANTICS_SOURCE] == semantics.LOADED_SOURCE_SHA256,
                 "Loaded frozen HTTP semantics differs from the profile source")
        _require(hashlib.sha256(Path(__file__).read_bytes()).hexdigest() == LOADED_SOURCE_SHA256,
                 "Loaded M4 comparator source changed; use a fresh worker")
        _require(self._successors == self.profile.health_successors,
                 "Bound health successors differ from the current profile")

    def check_current(self) -> None:
        """Detect source/profile drift without granting execution authority."""
        profiles.assert_profile_current(self.profile)
        self._check_binding()

    def compare(self, step_index: int, facts: semantics.ResponseFacts) -> semantics.Comparison:
        _require(type(step_index) is int and 0 <= step_index < len(self.case.steps),
                 "Exact in-range original step index required")
        _require(type(facts) is semantics.ResponseFacts, "Exact frozen response facts required")
        step = self.case.steps[step_index]
        _require(step.expectation is not None and step.expectation.semantic is not None,
                 "Only original semantic HTTP steps are comparable")
        assert step.expectation is not None and step.expectation.semantic is not None
        original = semantics.compare(facts, step.expectation.semantic)
        successor = next((item for item in self._successors if item.step_index == step_index), None)
        if successor is None or original.facet("json_syntax").disposition != "pass":
            return original
        # The frozen syntax facet has already enforced complete decodable bytes,
        # representation metadata and allocation limits. Its exact policy wins.
        assert isinstance(facts.body, bytes)
        outcome = semantics.semantic_compare(semantics.parse_json(facts.body),
                                             semantics.parse_json(successor.target_body))
        body = semantics.Facet("body_shape_value", outcome,
            "exact source-declared M4 health successor with frozen JSON value policy",
            ("V0-HTTP", "M4-API-SCHEMA:clause:2", "shared:compatibility:2"))
        return semantics.Comparison(tuple(body if facet.name == "body_shape_value" else facet
                                          for facet in original.facets))
