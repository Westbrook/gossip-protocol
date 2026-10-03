"""Pure partial-job comparisons with prospectively declared error equality.

Only the fixed top-level error CODE and a literal subject JOB.error are related.
Observed codes never become expected values. These supplied-value diagnostics
prove no chronology, causal applicability, physical execution or acceptance.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
import re
from typing import Any

from . import candidate_http_semantics_v1 as sem

PROTOCOL = "candidate-http-relations-v1"
MAX_JOBS = 4096  # Evaluator declaration bound, never a product job limit.
_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:/-]{0,255}\Z")
_JOB_ID = re.compile(r"[A-Za-z0-9_-]{1,64}\Z")
_STATES = ("queued", "running", "completed", "cancelled", "failed")
_CITATIONS = ("M1-JOBS", "M1-HTTP-INTERFACES", "M1-HTTP-STATUS")


class RelationError(ValueError):
    """Invalid author declarations or supplied-value labels, not product failure."""


def _require(value: bool, message: str) -> None:
    if not value:
        raise RelationError(message)


def _identifier(value: str) -> None:
    _require(type(value) is str and _ID.fullmatch(value) is not None, "Literal step identifier required")


def _job_id(value: str) -> None:
    _require(type(value) is str and _JOB_ID.fullmatch(value) is not None, "Literal job identifier required")


def encoded(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
                      allow_nan=False).encode("ascii")


@dataclass(frozen=True, slots=True)
class ErrorReference:
    step_id: str
    expected_status: int | None = 400

    def __post_init__(self) -> None:
        _identifier(self.step_id)
        _require(self.expected_status is None or (
            type(self.expected_status) is int and self.expected_status in (400, 404)),
            "Only supported unclassified error status declarations are allowed")

    def record(self) -> dict[str, object]:
        return {"from_error_step": self.step_id, "expected_status": self.expected_status}


@dataclass(frozen=True, slots=True)
class JobRule:
    job_id: str
    epoch: int
    state: str
    total: int
    completed: int
    error: str | None | ErrorReference

    def __post_init__(self) -> None:
        _job_id(self.job_id)
        _require(type(self.epoch) is int and self.epoch >= 1, "Known positive integer epoch required")
        _require(type(self.state) is str and self.state in _STATES, "Known job state required")
        _require(type(self.total) is int and self.total >= 0, "Known nonnegative total required")
        _require(type(self.completed) is int and self.completed == (
            self.total if self.state == "completed" else 0), "Known M1 completed count required")
        # The source's error envelope prescribes a string, not a nonempty string.
        _require(type(self.error) in (str, ErrorReference) if self.state == "failed" else self.error is None,
                 "Only a failed job carries a literal or related error string")

    def record(self) -> dict[str, object]:
        return {"job_id": self.job_id, "epoch": self.epoch, "state": self.state,
                "total": self.total, "completed": self.completed,
                "error": self.error.record() if type(self.error) is ErrorReference else self.error}


@dataclass(frozen=True, slots=True)
class JobsExpectation:
    step_id: str
    jobs: tuple[JobRule, ...]

    def __post_init__(self) -> None:
        _identifier(self.step_id)
        _require(type(self.jobs) is tuple and 1 <= len(self.jobs) <= MAX_JOBS
                 and all(type(job) is JobRule for job in self.jobs), "Bounded immutable job rules required")
        ids = tuple(job.job_id for job in self.jobs)
        _require(ids == tuple(sorted(set(ids))), "Jobs must have unique literal IDs in source order")
        references: dict[str, ErrorReference] = {}
        for job in self.jobs:
            if type(job.error) is ErrorReference:
                _require(job.error.step_id != self.step_id, "An error source cannot be its own jobs census")
                previous = references.get(job.error.step_id)
                _require(previous is None or previous == job.error, "Conflicting expectations for an error source")
                references[job.error.step_id] = job.error
        _require(bool(references), "A partial jobs expectation needs an explicit error relation")

    def record(self) -> dict[str, object]:
        return {"protocol": PROTOCOL, "kind": "jobs-with-related-errors", "jobs_step_id": self.step_id,
                "jobs": [job.record() for job in self.jobs]}

    @property
    def sha256(self) -> str:
        return hashlib.sha256(encoded(self.record())).hexdigest()


@dataclass(frozen=True, slots=True)
class ObservedResponse:
    step_id: str
    facts: sem.ResponseFacts

    def __post_init__(self) -> None:
        _identifier(self.step_id)
        _require(type(self.facts) is sem.ResponseFacts, "Pure typed response facts required")


@dataclass(frozen=True, slots=True)
class ErrorDiagnostic:
    step_id: str
    comparison: sem.Comparison

    def __post_init__(self) -> None:
        _identifier(self.step_id)
        _require(type(self.comparison) is sem.Comparison, "Typed source comparison required")


@dataclass(frozen=True, slots=True)
class CodeRelation:
    source_step_id: str
    jobs_step_id: str
    job_id: str
    facet: sem.Facet

    def __post_init__(self) -> None:
        _identifier(self.source_step_id)
        _identifier(self.jobs_step_id)
        _job_id(self.job_id)
        _require(self.source_step_id != self.jobs_step_id, "Relation needs distinct literal steps")
        _require(type(self.facet) is sem.Facet and self.facet.name == "error_code_equality",
                 "Typed fixed equality facet required")


@dataclass(frozen=True, slots=True)
class RelationDiagnostic:
    jobs_step_id: str
    jobs: sem.Comparison
    sources: tuple[ErrorDiagnostic, ...]
    relations: tuple[CodeRelation, ...]
    protocol: str = field(default=PROTOCOL, init=False)
    authority: str = field(default="supplied_values_only", init=False)
    acceptance_authority: bool = field(default=False, init=False)
    fresh_execution: bool = field(default=False, init=False)
    chronology_verified: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        _identifier(self.jobs_step_id)
        _require(type(self.jobs) is sem.Comparison, "Typed census comparison required")
        _require(type(self.sources) is tuple and 1 <= len(self.sources) <= MAX_JOBS
                 and all(type(item) is ErrorDiagnostic for item in self.sources), "Immutable source diagnostics required")
        _require(type(self.relations) is tuple and 1 <= len(self.relations) <= MAX_JOBS
                 and all(type(item) is CodeRelation for item in self.relations), "Immutable relation diagnostics required")
        source_ids = tuple(item.step_id for item in self.sources)
        _require(len(set(source_ids)) == len(source_ids) and self.jobs_step_id not in source_ids,
                 "Unique source steps distinct from the census required")
        _require({item.source_step_id for item in self.relations} == set(source_ids)
                 and all(item.jobs_step_id == self.jobs_step_id for item in self.relations),
                 "Diagnostic relations must bind the declared census and sources")
        _require(len({item.job_id for item in self.relations}) == len(self.relations), "Repeated relation subject")


def _combine(values: list[sem.Disposition]) -> sem.Disposition:
    for status in ("fail", "unavailable", "unspecified"):
        if status in values:
            return status
    return "pass"


def _member(node: sem.Node, key: str) -> sem.Node | sem.Missing:
    if type(node) is not sem.JsonObject:
        return sem.Missing("required object is unavailable or has a different shape")
    values = [value for name, value in node.pairs if name == key]
    if len(values) != 1:
        return sem.Missing("required member is missing or ambiguous: " + key)
    return values[0]


def _job_compare(node: sem.Node, rule: JobRule) -> sem.Disposition:
    if type(node) is not sem.JsonObject:
        return "fail"
    outcomes: list[sem.Disposition] = []
    if {key for key, _ in node.pairs} != sem.JOB_KEYS:
        outcomes.append("fail")
    expected = rule.record()
    for key in sem.JOB_KEYS:
        values = [value for name, value in node.pairs if name == key]
        if not values:
            outcomes.append("fail")
        elif len(values) > 1:
            outcomes.append("unspecified")
        elif key == "error" and type(rule.error) is ErrorReference:
            outcomes.append("pass" if type(values[0]) is str else "fail")
        else:
            outcomes.append(sem.semantic_compare(values[0], sem.parse_json(encoded(expected[key]))))
    return _combine(outcomes)


def _jobs_compare(node: sem.Node, expected: JobsExpectation) -> sem.Disposition:
    if type(node) is not sem.JsonObject:
        return "fail"
    outcomes: list[sem.Disposition] = []
    if {key for key, _ in node.pairs} != {"jobs"}:
        outcomes.append("fail")
    values = [value for name, value in node.pairs if name == "jobs"]
    if not values:
        return "fail"
    if len(values) > 1:
        return _combine(outcomes + ["unspecified"])
    jobs = values[0]
    if type(jobs) is not list:
        return "fail"
    if len(jobs) != len(expected.jobs):
        outcomes.append("fail")
    outcomes.extend(_job_compare(actual, rule) for actual, rule in zip(jobs, expected.jobs))
    return _combine(outcomes)


def _subject_errors(node: sem.Node, wanted: tuple[str, ...]) -> dict[str, str | sem.Missing]:
    """One duplicate-aware pass; candidate array size never multiplies references."""
    jobs = _member(node, "jobs")
    if type(jobs) is not list:
        return {name: sem.Missing("jobs array is missing, ambiguous or not an array") for name in wanted}
    wanted_ids = set(wanted)
    found: dict[str, str | sem.Missing] = {}
    ambiguous: set[str] = set()
    for job in jobs:
        if type(job) is not sem.JsonObject:
            continue
        ids = [value for name, value in job.pairs if name == "job_id"]
        matches = {value for value in ids if type(value) is str and value in wanted_ids}
        if len(ids) != 1:
            ambiguous.update(matches)
        elif matches:
            name = next(iter(matches))
            if name in found:
                ambiguous.add(name)
            else:
                value = _member(job, "error")
                found[name] = value if type(value) is str else sem.Missing(
                    "subject error is missing, ambiguous or not a string")
    return {name: (sem.Missing("literal subject job is missing or ambiguous")
                   if name in ambiguous or name not in found else found[name]) for name in wanted}


def _decoded(facts: sem.ResponseFacts, comparison: sem.Comparison) -> sem.Node | sem.Missing:
    # Reuse exactly the held semantic representation/body bounds and coding
    # rules. No duplicate-discarding decoder or independent permissive fallback.
    if comparison.facet("json_syntax").disposition != "pass":
        return sem.Missing("complete supported JSON body is unavailable")
    assert type(facts.body) is bytes
    return sem.parse_json(facts.body)


def _missing_facts() -> sem.ResponseFacts:
    missing = sem.Missing("step facts not supplied")
    return sem.ResponseFacts(missing, missing, missing)


def compare(expected: JobsExpectation, supplied: tuple[ObservedResponse, ...]) -> RelationDiagnostic:
    """Compare supplied values only; required full consumption is a future gate.

    Literal census structure/values, each source error response, and each equality
    have separate facets. All are required by the future acceptance consumer; an
    equality pass cannot replace a failed envelope, status, state or sibling.
    """
    _require(type(expected) is JobsExpectation, "Typed partial jobs declaration required")
    _require(type(supplied) is tuple and len(supplied) <= MAX_JOBS + 1
             and all(type(item) is ObservedResponse for item in supplied), "Immutable supplied response tuple required")
    _require(len({item.step_id for item in supplied}) == len(supplied), "Duplicate supplied step")
    refs = tuple(dict.fromkeys(job.error for job in expected.jobs if type(job.error) is ErrorReference))
    wanted = {expected.step_id} | {ref.step_id for ref in refs}
    _require(all(item.step_id in wanted for item in supplied), "Unexpected supplied step")
    facts_by_step = {item.step_id: item.facts for item in supplied}
    jobs_facts = facts_by_step.get(expected.step_id, _missing_facts())
    common = sem.compare(jobs_facts, sem.success("unspecified"))
    jobs_node = _decoded(jobs_facts, common)
    body_outcome: sem.Disposition = "unavailable" if isinstance(jobs_node, sem.Missing) else _jobs_compare(jobs_node, expected)
    body_facet = sem.Facet("body_shape_value", body_outcome,
        "exact ordered jobs and literal fields; related error slots still require strings", _CITATIONS)
    jobs_comparison = sem.Comparison(tuple(body_facet if item.name == "body_shape_value" else item
                                          for item in common.facets))
    sources: list[ErrorDiagnostic] = []
    source_codes: dict[str, str | sem.Missing] = {}
    for ref in refs:
        facts = facts_by_step.get(ref.step_id, _missing_facts())
        comparison = sem.compare(facts, sem.unclassified_error(ref.expected_status))
        sources.append(ErrorDiagnostic(ref.step_id, comparison))
        node = _decoded(facts, comparison)
        value = sem.Missing("source body is unavailable") if isinstance(node, sem.Missing) else _member(node, "error")
        source_codes[ref.step_id] = value if type(value) is str else sem.Missing(
            "source error is missing, ambiguous or not a string")
    subjects = ({job.job_id: sem.Missing("jobs body is unavailable") for job in expected.jobs
                 if type(job.error) is ErrorReference} if isinstance(jobs_node, sem.Missing) else
                _subject_errors(jobs_node, tuple(job.job_id for job in expected.jobs
                                                if type(job.error) is ErrorReference)))
    relations: list[CodeRelation] = []
    for job in expected.jobs:
        if type(job.error) is not ErrorReference:
            continue
        source_code = source_codes[job.error.step_id]
        subject_code = subjects[job.job_id]
        if type(source_code) is sem.Missing or type(subject_code) is sem.Missing:
            outcome: sem.Disposition = "unavailable"
            reason = "; ".join(value.reason for value in (source_code, subject_code) if type(value) is sem.Missing)
        else:
            outcome = "pass" if source_code == subject_code else "fail"
            reason = "equality at the two prospectively declared fixed error fields; exact CODE remains unspecified"
        relations.append(CodeRelation(job.error.step_id, expected.step_id, job.job_id,
                                      sem.Facet("error_code_equality", outcome, reason, _CITATIONS)))
    return RelationDiagnostic(expected.step_id, jobs_comparison, tuple(sources), tuple(relations))
