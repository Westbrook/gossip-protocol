"""Concrete original-journal HTTP observation source for registered product gates.

The pure diagnostic function has no admission authority. Only the concrete source
accepts an exact execution owner and externally retained terminal checkpoint,
reconstructs original raw observations, and retains its complete verifier receipt.
It never claims whole-project scope, qualified control, or held-out task novelty.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING, Any

from . import candidate_http_cases_core_v1 as core
from . import candidate_http_semantics_v1 as semantics
from . import candidate_http_relations_v1 as relations
from . import project_acceptance_registry_v1 as registry

if TYPE_CHECKING:
    from . import candidate_http_execution_v4 as execution
    from . import candidate_http_observation_v2 as reader

PROTOCOL = "candidate-http-observation-source-v1-compact-v1"


@dataclass(frozen=True)
class StepDiagnostic:
    case_id: str
    step_id: str
    step_index: int
    kind: str
    state: str
    facets: tuple[semantics.Facet, ...]
    required_facets: tuple[str, ...]
    status: str
    limitations: tuple[str, ...]


def _missing() -> semantics.ResponseFacts:
    value = semantics.Missing("authenticated original step observation unavailable")
    return semantics.ResponseFacts(value, value, value, value, value)


def _relation(raw: bytes) -> relations.JobsExpectation:
    value = core._relation_record(raw)
    rules = []
    for row in value["jobs"]:
        error = row["error"]
        if type(error) is dict:
            error = relations.ErrorReference(error["from_error_step"], error["expected_status"])
        rules.append(relations.JobRule(row["job_id"], row["epoch"], row["state"], row["total"], row["completed"], error))
    return relations.JobsExpectation(value["jobs_step_id"], tuple(rules))


def _facet(name: str, disposition: semantics.Disposition, reason: str) -> semantics.Facet:
    return semantics.Facet(name, disposition, reason, ("prospective-literal-case",))


def _semantic_required(expectation: semantics.Expectation) -> tuple[str, ...]:
    # Requirement applicability comes only from the prospective declaration,
    # never from whether an observed comparison happens to be unspecified.
    excluded = {"response_media_type"}
    if expectation.status is None:
        excluded.add("status")
    if expectation.shape == "unspecified":
        excluded.add("body_shape_value")
    if expectation.shape == "error" and expectation.code is None:
        excluded.add("error_code")
        if expectation.status == 404:
            excluded.add("reported_code_status_relation")
    return tuple(item.name for item in semantics.compare(_missing(), expectation).facets if item.name not in excluded)


def _status(facets: tuple[semantics.Facet, ...], required: tuple[str, ...], state: str) -> str:
    """Known authenticated failures survive unavailable sibling prerequisites."""
    by_name = {item.name: item for item in facets}
    if any(by_name[name].disposition == "fail" for name in required):
        return "failed"
    if state != "authenticated" or any(by_name[name].disposition in ("unavailable", "unspecified") for name in required):
        return "infrastructure_error"
    return "passed" if required else "skipped"


def diagnose(profile: execution.HttpProductProfile, history: reader.HistoryObservation) -> tuple[StepDiagnostic, ...]:
    """Pure supplied-value comparison only; callers cannot use this as authority."""
    from . import candidate_http_execution_v4 as execution
    from . import candidate_http_observation_v2 as reader
    execution.require(type(profile) is execution.HttpProductProfile and type(history) is reader.HistoryObservation,
                      "Exact immutable diagnostic inputs required")
    expected = profile.case.steps
    execution.require(len(history.steps) == len(expected), "Full step roster required, including unavailable suffix")
    observed = {step.step_id: step for step in history.steps}
    execution.require(len(observed) == len(expected), "Repeated original step")
    results: list[StepDiagnostic] = []
    prior: dict[str, dict[str, semantics.Facet]] = {}
    for index, (step, actual) in enumerate(zip(expected, history.steps, strict=True)):
        execution.require((actual.step_id, actual.step_index, "request" if actual.kind == "probe" else actual.kind) == (step.step_id, index, step.kind)
                          and actual.state in ("authenticated", "unavailable", "unentered"),
                          "Original step order, kind or state differs")
        rows: list[semantics.Facet] = []
        required: list[str] = []
        if step.kind in ("start", "stop"):
            rows.append(_facet("lifecycle", "pass" if actual.state == "authenticated" else "unavailable",
                               "source-bound original lifecycle, continuity and retirement observation"))
            required.append("lifecycle")
        elif step.kind == "cli":
            known = actual.state == "authenticated"
            exit_proof = dict(actual.provenance).get("finite_exit_proof_sha256", "")
            known_exit = known or (actual.state == "unavailable" and execution._SHA.fullmatch(exit_proof) is not None)
            rows.append(_facet("exit_status", "unavailable" if not known_exit or actual.exit_code is None else
                               "pass" if actual.exit_code == 0 else "fail", "declared CLI exit zero"))
            syntax: semantics.Disposition = "unavailable"
            if known and type(actual.stdout) is bytes:
                try:
                    semantics.parse_json(actual.stdout)
                    syntax = "pass"
                except (UnicodeDecodeError, RecursionError):
                    syntax = "unavailable"
                except ValueError:
                    syntax = "fail"
            rows.extend((_facet("json_syntax", syntax, "one complete original CLI stdout JSON value"),
                _facet("exact_outer_wrapper", "unspecified", "literal case does not prescribe CLI wrapper"),
                _facet("stderr_exact_bytes", "unspecified", "literal case does not prescribe stderr bytes"),
                _facet("public_state", "unspecified", "supplied state snapshots have no qualified CLI wrapper adapter")))
            required.extend(("exit_status", "json_syntax"))
        else:
            assert step.expectation is not None
            expectation = step.expectation
            facts = actual.facts if actual.state == "authenticated" and actual.facts is not None else _missing()
            if expectation.raw_facts_only:
                rows.append(_facet("raw_facts", "unspecified", "no product expectation is declared for this request"))
            elif expectation.semantic is not None:
                rows.extend(semantics.compare(facts, expectation.semantic).facets)
                required.extend(_semantic_required(expectation.semantic))
            else:
                relation = _relation(expectation.relation_json)
                wanted = {step.step_id} | {job.error.step_id for job in relation.jobs
                    if type(job.error) is relations.ErrorReference}
                supplied_rows: list[relations.ObservedResponse] = []
                for name in sorted(wanted):
                    if name in observed and observed[name].state == "authenticated":
                        known_facts = observed[name].facts
                        if known_facts is not None:
                            supplied_rows.append(relations.ObservedResponse(name, known_facts))
                supplied = tuple(supplied_rows)
                result = relations.compare(relation, supplied)
                rows.extend(result.jobs.facets)
                required.extend(item.name for item in result.jobs.facets if item.name != "response_media_type")
                for source in result.sources:
                    reference = next(job.error for job in relation.jobs
                        if type(job.error) is relations.ErrorReference and job.error.step_id == source.step_id)
                    source_required = _semantic_required(semantics.unclassified_error(reference.expected_status))
                    for facet in source.comparison.facets:
                        name = "source:" + source.step_id + ":" + facet.name
                        rows.append(semantics.Facet(name, facet.disposition, facet.reason, facet.citations))
                        if facet.name in source_required:
                            required.append(name)
                for edge in result.relations:
                    name = "relation:" + edge.source_step_id + ":" + edge.job_id + ":error_code_equality"
                    rows.append(semantics.Facet(name, edge.facet.disposition, edge.facet.reason, edge.facet.citations))
                    required.append(name)
            # Conditions constrain content interpretation, never independent
            # status, syntax, listeners, or any earlier independent failure.
            dependencies = [prior.get(item.step_id, {}).get(item.facet) for item in expectation.content_requires]
            if dependencies and any(item is None or item.disposition != "pass" for item in dependencies):
                rows = [semantics.Facet(item.name, "unavailable", "declared earlier content prerequisite unavailable",
                                        item.citations)
                    if item.name == "body_shape_value" and item.disposition != "unspecified" else item
                    for item in rows]
        facets = tuple(rows)
        prior[step.step_id] = {item.name: item for item in facets}
        names = tuple(required)
        results.append(StepDiagnostic(profile.ordered_case_ids[index], step.step_id, index, step.kind,
            actual.state, facets, names, _status(facets, names, actual.state), actual.limitations))
    return tuple(results)


class HttpObservationSource:
    """Exact original owner capability; no dictionary or fixture admission path."""

    def __init__(self, owner: execution.CandidateHttpExecution, checkpoint: execution.ControllerCheckpoint,
                 *, receipt_path: Path):
        from . import candidate_http_execution_v4 as execution
        execution.require(type(owner) is execution.CandidateHttpExecution
                          and type(checkpoint) is execution.ControllerCheckpoint,
                          "Exact HTTP owner and external checkpoint required")
        self.owner, self.checkpoint = owner, checkpoint
        self.receipt_path = Path(receipt_path).absolute()
        execution.require(self.receipt_path.resolve() == self.receipt_path and not self.receipt_path.is_symlink()
                          and self.receipt_path == owner.root / "semantic-verifier.json",
                          "Verifier receipt must be the fixed original-chain artifact")

    def observation(self, gate: registry.Gate, freeze: registry.CohortFreeze | None) -> registry.Observation:
        from . import candidate_http_execution_v4 as execution
        from . import candidate_observation_admission_v1 as admission
        from .candidate_scope_consumer_v1 import AuthorityError, AuthorityUnavailable
        from .gitstore import GitError
        try:
            return self._observation(gate, freeze)
        except (admission.AdmissionUnavailable, execution.ExecutionUnknown, execution.compact.ChainUnknown,
                OSError, GitError, subprocess.SubprocessError) as error:
            raise AuthorityUnavailable(str(error)) from error
        except (ValueError, TypeError, LookupError) as error:
            raise AuthorityError(str(error)) from error

    def _observation(self, gate: registry.Gate, freeze: registry.CohortFreeze | None) -> registry.Observation:
        from . import candidate_http_execution_v4 as execution
        from . import candidate_http_observation_v2 as reader
        from . import candidate_observation_admission_v1 as admission
        owner = self.owner
        execution.require(type(owner) is execution.CandidateHttpExecution, "Exact original HTTP owner required")
        execution.require(self.receipt_path.resolve() == self.receipt_path and not self.receipt_path.is_symlink(),
                          "Verifier receipt path changed")
        execution.require(type(gate) is registry.Gate and owner.actual_registration.gate == gate,
                          "Requested gate differs from prospective original registration")
        execution.require(owner.checkpoint() == self.checkpoint and owner._freeze() == freeze,
                          "Original external checkpoint or cohort barrier differs")
        history = reader.observe_execution(owner, self.checkpoint)
        execution.require(history.original_binding_sha256 == owner.actual_registration.original_binding_sha256
                          and history.checkpoint_sha256 == execution.digest(asdict(self.checkpoint)),
                          "Raw observation differs from original binding or external checkpoint")
        execution.require(execution.evaluator_sources() == owner.sources == execution._LOADED_SOURCES,
                          "Loaded semantic evaluator changed before comparison")
        admission.verify_loaded_sources(owner.sources)
        diagnostics = diagnose(owner.profile, history)
        admission.verify_loaded_sources(owner.sources)
        outcomes = tuple(registry.CaseResult(item.case_id, item.status) for item in diagnostics)
        execution.require(tuple(item.case_id for item in outcomes) == gate.ordered_case_ids,
                          "Normalized outcome roster differs")
        execution.require(owner.checkpoint() == self.checkpoint and owner._freeze() == freeze
                          and execution.evaluator_sources() == owner.sources == execution._LOADED_SOURCES,
                          "Original evidence, loaded evaluator or admission changed during semantic verification")
        record = {"protocol": PROTOCOL, "original_registration": asdict(owner.actual_registration),
            "original_execution_id": history.execution_id, "original_terminal_sha256": history.terminal_sha256,
            "original_binding_sha256": history.original_binding_sha256,
            "original_journal_context_sha256": self.checkpoint.context_sha256,
            "original_config_sha256": execution.sha256(owner.read_authenticated("config.json")),
            "original_journal": str(owner.root), "product_profile": owner.profile.record(),
            "cohort_freeze": None if freeze is None else asdict(freeze),
            "diagnostics": [asdict(item) for item in diagnostics],
            "raw_provenance": [{"step_id": item.step_id, "provenance": item.provenance} for item in history.steps],
            "cleanup_verified": history.cleanup_verified, "infrastructure": history.infrastructure,
            "missing_step_ids": history.missing_step_ids,
            "evaluator_sources": execution.evaluator_sources(),
            "physical_execution_reused": False, "whole_project_acceptance": False,
            "held_out_claim": False}
        raw = execution.encoded(record)
        if owner.has_authenticated(self.receipt_path.name):
            execution.require(owner.read_authenticated(self.receipt_path.name) == raw,
                              "Existing anchored verifier differs or aliases another origin")
        else:
            self.checkpoint = owner.retain_verifier(self.receipt_path.name, raw)
        owner.verified_execution()
        execution.require(owner.read_authenticated(self.receipt_path.name) == raw and owner.checkpoint() == self.checkpoint
                          and owner._freeze() == freeze
                          and execution.evaluator_sources() == owner.sources == execution._LOADED_SOURCES,
                          "Verifier receipt, evaluator or original evidence changed")
        admission.verify_loaded_sources(owner.sources)
        physical = registry.PhysicalExecution(gate.binding, history.execution_id, history.terminal_sha256,
            execution.sha256(raw), "completed" if history.cleanup_verified and not history.infrastructure
            and not history.missing_step_ids else "infrastructure_error", outcomes,
            None if freeze is None else freeze.receipt_sha256)
        return registry.Observation(gate.gate_id, gate.binding, physical)
