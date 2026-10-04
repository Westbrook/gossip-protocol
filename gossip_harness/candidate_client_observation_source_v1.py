"""Concrete C06 bridge from original v5 CLI journals into Registry observations.

Only the exact physical controller owner and a separately retained checkpoint are
admitted. The verifier is host-authored from complete original process bytes;
caller-provided verdict dictionaries, fixtures, aliases, and qualification-era
journals have no entry point. This adapter supplies no ScopePlan or promotion.
"""
from __future__ import annotations

from dataclasses import asdict
import json
import subprocess
from typing import Any

from . import candidate_cli_acceptance_profile_v1 as profile
from . import candidate_observation_admission_v1 as admission
from . import candidate_client_execution_v5 as execution
from . import candidate_client_observer_v5 as observer
from . import project_acceptance_registry_v1 as registry
from . import candidate_observation_admission_v1 as admission
from .candidate_scope_consumer_v1 import AuthorityError, AuthorityUnavailable
from .gitstore import GitError

PROTOCOL = "candidate-client-observation-source-v1"
VERIFIER_FILE = "product-verifier.json"


class ObservationError(AuthorityError):
    """Original product observation authority is absent, stale, or invalid."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ObservationError(message)


def _mapped_cells(case_id: str, observations: tuple[Any, ...]) -> tuple[dict[str, Any], ...]:
    """Pure reconciliation, never an authority to issue a Registry observation."""
    definition = profile.case_definition(case_id)
    steps = definition["recipe"]["steps"]
    require(type(observations) is tuple and len(observations) <= len(steps), "Invalid observation prefix")
    result = []
    for index, step in enumerate(steps):
        observed = observations[index] if index < len(observations) else None
        if observed is not None:
            require(type(observed) is observer.ProcessObservation, "Wrong observation version")
            binding = json.loads(observed.binding_json)
            require(binding["case_id"] == case_id and binding["step_id"] == step["step_id"]
                    and binding["step_index"] == index
                    and binding["ordered_step_ids"] == [item["step_id"] for item in steps]
                    and binding["argv"] == step["argv"], "Observation order/argv differs from prospective recipe")
        graded = observer.observe_cli_step(definition["expectations"][step["step_id"]], observed)
        for assertion in definition["expectations"][step["step_id"]]["assertion_ids"]:
            value = graded["assertions"][assertion]
            disposition = graded["assertion_dispositions"][assertion]
            status = ("skipped" if disposition == "unspecified" else "passed" if value is True
                      else "failed" if value is False else "infrastructure_error")
            result.append({"case_id": case_id + ":" + step["step_id"] + ":" + assertion,
                "step_id": step["step_id"], "step_index": index, "assertion_id": assertion,
                "status": status, "value": value, "disposition": disposition,
                "reason": graded["assertion_reasons"][assertion],
                "expectation_sha256": graded["expectation_sha256"],
                "observation_sha256": graded["observation_sha256"],
                "step_entered": observed is not None})
    require(tuple(item["case_id"] for item in result) == profile.ordered_assertion_ids(case_id),
            "Complete assertion roster differs")
    return tuple(result)


def _record(owner: execution.CandidateClientExecution) -> tuple[dict[str, Any], execution.ClientHistoryResult]:
    require(type(owner) is execution.CandidateClientExecution and owner.mode == "physical",
            "Actual physical v5 journal owner required")
    before = owner.checkpoint()
    original = owner.verified_execution()
    require(original.checkpoint == before and owner.checkpoint() == before,
            "Original journal changed during verification")
    freeze = owner.retained_freeze()
    owner._current_admission(freeze)
    admission.verify_loaded_sources(owner.sources)
    cells = _mapped_cells(original.case_id, original.observations)
    record = {"protocol": PROTOCOL, "execution_protocol": execution.PROTOCOL,
        "execution_id": original.execution_id, "root": str(owner.root),
        "registration": asdict(owner.observation_registration),
        "original_binding": asdict(owner.binding),
        "original_terminal_sha256": original.terminal_sha256,
        "original_checkpoint": {"files": [list(item) for item in before.files if item[0] != VERIFIER_FILE]},
        "cohort_freeze": None if freeze is None else asdict(freeze),
        "profile_sha256": profile.profile_sha256(),
        "authoring_disclosure": profile.AUTHORING_DISCLOSURE,
        "production_scope_authority": False, "held_out_or_source_blind": False,
        "history_status": original.status, "missing_step_ids": list(original.missing_step_ids),
        "cleanup_verified": original.cleanup_verified, "infrastructure": list(original.infrastructure),
        "cells": list(cells)}
    admission.verify_loaded_sources(owner.sources)
    owner._current_admission(freeze)
    require(owner.checkpoint() == before, "Original journal changed during reconciliation")
    return record, original


def publish_verifier(owner: execution.CandidateClientExecution) -> execution.ControllerCheckpoint:
    """Retain the original host verifier, then return its externally retainable checkpoint.

    This never dispatches candidate code or consumes a caller verdict. Repeated
    publication checks the existing exact record; it cannot replace an original.
    """
    record, _ = _record(owner)
    raw = execution.encoded(record)
    if (owner.root / VERIFIER_FILE).exists():
        require(execution._read(owner.root / VERIFIER_FILE) == raw, "Original verifier differs")
    else:
        owner._retain(VERIFIER_FILE, raw)
    owner._current_admission(owner.retained_freeze())
    return owner.checkpoint()


class ClientObservationSource:
    """Bridge for AuthoritySnapshot.observation; no other authority is supplied."""

    def __init__(self, owner: execution.CandidateClientExecution,
                 expected_checkpoint: execution.ControllerCheckpoint):
        require(type(owner) is execution.CandidateClientExecution and owner.mode == "physical"
                and type(expected_checkpoint) is execution.ControllerCheckpoint,
                "Original physical v5 owner and external checkpoint required")
        require(VERIFIER_FILE in dict(expected_checkpoint.files), "Retained host verifier is required")
        self.owner, self.expected_checkpoint = owner, expected_checkpoint

    def observation(self, gate: registry.Gate, freeze: registry.CohortFreeze | None) -> registry.Observation:
        # Consumer isolation is per gate: an unavailable or invalid sibling must
        # not abort assessment and discard an earlier authenticated failure.
        try:
            return self._observation(gate, freeze)
        except (admission.AdmissionUnavailable, execution.ExecutionUnknown, OSError, GitError, subprocess.SubprocessError) as error:
            raise AuthorityUnavailable(str(error)) from error
        except AuthorityError:
            raise
        except (ValueError, KeyError, TypeError) as error:
            raise ObservationError(str(error)) from error

    def _observation(self, gate: registry.Gate, freeze: registry.CohortFreeze | None) -> registry.Observation:
        require(type(gate) is registry.Gate and gate == self.owner.registration.gate, "Exact original gate required")
        require(self.owner.checkpoint() == self.expected_checkpoint, "Original journal changed since checkpoint")
        require(self.owner.retained_freeze() == freeze, "Original purpose/freeze differs")
        record, original = _record(self.owner)
        raw = execution.encoded(record)
        require(execution._read(self.owner.root / VERIFIER_FILE) == raw, "Host verifier differs from original evidence")
        outcomes = tuple(registry.CaseResult(cell["case_id"], cell["status"]) for cell in record["cells"])
        physical = registry.PhysicalExecution(gate.binding, original.execution_id,
            original.terminal_sha256, execution.sha256(raw),
            "completed" if original.status == "completed" else "infrastructure_error", outcomes,
            None if freeze is None else freeze.receipt_sha256)
        require(self.owner.checkpoint() == self.expected_checkpoint, "Uncheckpointed journal suffix")
        return registry.Observation(gate.gate_id, gate.binding, physical)
