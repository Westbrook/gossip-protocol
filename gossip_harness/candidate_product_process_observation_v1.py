"""Actual candidate CLI/HTTP history comparison and original-journal bridge.

Pure supplied-value functions cannot authenticate observations. The concrete
bridge reads the exact physical owner, external checkpoint, original wire and
finite-process records, and preserves original source/purpose/epoch provenance.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path
import subprocess
from typing import TYPE_CHECKING, Any

from . import candidate_product_process_core_v1 as core
from . import candidate_http_semantics_v1 as semantics
from . import candidate_http_transport_v1 as wire
from . import project_acceptance_registry_v1 as registry

if TYPE_CHECKING:
    from . import candidate_product_process_execution_v1 as execution
    from . import candidate_product_process_reader_v1 as reader

PROTOCOL = "candidate-product-process-observation-v1"
M4_PROTOCOL = PROTOCOL
SEMANTIC_LIMITS = {"json_bytes": 16 * 1024 * 1024, "json_depth": 64, "structural_tokens": 100000}


class AmbiguousJSON(ValueError):
    """Ambiguous members or unsupported numeric representation are ungraded."""


class ObservationLimit(ValueError):
    """Declared observer allocation exhausted; never a product correctness failure."""


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


@dataclass(frozen=True)
class MechanicsDiagnostic:
    case_id: str
    facets: tuple[semantics.Facet, ...]
    required_facets: tuple[str, ...]
    status: str


@dataclass(frozen=True)
class HttpOutcomeProjection:
    diagnostics: tuple[StepDiagnostic, ...]
    decisive_outcomes: tuple[registry.CaseResult, ...]
    mechanics_guard: MechanicsDiagnostic


def _facet(name: str, disposition: semantics.Disposition, reason: str) -> semantics.Facet:
    return semantics.Facet(name, disposition, reason, ("prospective-product-process-definition",))


def _status(facets: tuple[semantics.Facet, ...], required: tuple[str, ...], state: str) -> str:
    by_name = {item.name: item for item in facets}
    if any(by_name[name].disposition == "fail" for name in required):
        return "failed"
    if state != "authenticated" or any(by_name[name].disposition != "pass" for name in required):
        return "infrastructure_error"
    return "passed" if required else "skipped"


def _strict_json(raw: bytes) -> Any:
    """Exact integer types; duplicate keys/nonfinite numbers never become values."""
    if len(raw) > SEMANTIC_LIMITS["json_bytes"]:
        raise ObservationLimit("Declared JSON capture allocation exceeded")
    # A byte scan bounds nesting and structural allocation before json.loads.
    # Bytes within escaped/string content cannot increase these counters.
    depth = tokens = 0
    quoted = escaped = False
    for byte in raw:
        if quoted:
            if escaped:
                escaped = False
            elif byte == 92:
                escaped = True
            elif byte == 34:
                quoted = False
        elif byte == 34:
            quoted = True
        elif byte in (91, 123):
            depth += 1
            tokens += 1
        elif byte in (93, 125):
            depth -= 1
        elif byte in (44, 58):
            tokens += 1
        if depth > SEMANTIC_LIMITS["json_depth"] or tokens > SEMANTIC_LIMITS["structural_tokens"]:
            raise ObservationLimit("Declared JSON structural allocation exceeded")
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise AmbiguousJSON("Duplicate JSON response member")
            result[key] = value
        return result
    def constant(_: str) -> None:
        raise ValueError("Nonfinite JSON number")
    value = json.loads(raw.decode("utf-8", "strict"), object_pairs_hook=pairs, parse_constant=constant)
    # A finite JSON exponent can exceed this observer's float representation.
    # Its valid grammar must not become a candidate syntax failure.
    try:
        json.dumps(value, allow_nan=False)
    except ValueError as error:
        raise AmbiguousJSON("Numeric representation overflow") from error
    return value


def _exact(expected: Any, actual: Any, depth: int = 0) -> bool:
    if depth > 64:
        raise ValueError("Comparison depth exceeded")
    if type(expected) is not type(actual):
        return False
    if type(expected) is dict:
        return expected.keys() == actual.keys() and all(_exact(v, actual[k], depth + 1) for k, v in expected.items())
    if type(expected) is list:
        return len(expected) == len(actual) and all(_exact(a, b, depth + 1) for a, b in zip(expected, actual))
    return bool(expected == actual)


def _at(value: Any, pointer: str) -> Any:
    for token in pointer[1:].split("/"):
        key = token.replace("~1", "/").replace("~0", "~")
        if type(value) is dict:
            value = value[key]
        elif type(value) is list and key.isascii() and key.isdecimal():
            value = value[int(key)]
        else:
            raise ValueError("Range selector does not address supplied JSON")
    return value


def declared_facets(expected: dict[str, Any]) -> tuple[str, ...]:
    """Prospective roster; missing or failing responses cannot shrink coverage."""
    result = ["mechanics_provenance_continuity", "mechanics_capture"]
    if expected["kind"] in ("server_start", "server_stop"):
        return (*result, "lifecycle")
    result.append("status" if expected["kind"] == "http" else "exit_status")
    if expected["kind"] == "http":
        result.extend(("listener_before", "listener_after"))
    else:
        result.append("auxiliary_stream_unspecified")
    result.append("json_syntax")
    if "json" in expected:
        result.append("json_exact")
    elif expected.get("json_value_only") is True:
        result.append("json_wrapper_unspecified")
    else:
        result.append("json_outer_keys")
        result.extend("json_field:" + key for key in sorted(expected["json_subset"]))
    result.extend("integer_range:" + key for key in sorted(expected.get("integer_ranges", {})))
    if "body_utf8" in expected:
        result.append("canonical_wire_bytes")
    return tuple(result)


def compare_step(expected: dict[str, Any], actual: reader.StepObservation) -> tuple[semantics.Facet, ...]:
    """Pure exact supplied-value checks, with no physical authority."""
    known = actual.state == "authenticated"
    captured = known and not actual.limitations
    facts = actual.facts
    if expected["kind"] == "http":
        observation = actual.wire_observation
        captured = (captured and type(observation) is wire.WireObservation and observation.sent_complete
            and observation.exchange_complete and observation.response.headers_complete
            and observation.response.framing_complete and observation.response.body_complete)
    elif expected["kind"] == "cli":
        captured = captured and type(actual.stdout) is bytes and type(actual.stderr) is bytes and type(actual.exit_code) is int
    rows = [_facet("mechanics_provenance_continuity", "pass" if known else "unavailable", "Original owner/reader proves source, epoch and state continuity"),
            _facet("mechanics_capture", "pass" if captured else "unavailable", "Complete original lifecycle, streams or intended wire exchange")]
    if expected["kind"] in ("server_start", "server_stop"):
        return (*rows, _facet("lifecycle", "pass" if known else "unavailable", "Authenticated owned process activation/retirement"))
    raw: bytes | None = None
    if expected["kind"] == "http":
        status = facts.status if known and facts is not None else None
        rows.append(_facet("status", "unavailable" if type(status) is not int else
                           "pass" if status == expected["status"] else "fail", "Exact public route status"))
        missing = semantics.Missing("Authenticated listener snapshot unavailable")
        rows.extend((semantics.listener("listener_before", facts.listener_before if known and facts else missing),
                     semantics.listener("listener_after", facts.listener_after if known and facts else missing)))
        if known and facts is not None and type(facts.body) is bytes:
            if type(facts.headers) is tuple:
                codings = [coding.strip().lower() for name, value in facts.headers if name.lower() == "content-encoding"
                           for coding in value.split(",")]
                if all(coding == "identity" for coding in codings):
                    raw = facts.body
    else:
        exit_proof = dict(actual.provenance).get("finite_exit_proof_sha256", "")
        independent_exit = actual.state == "unavailable" and len(exit_proof) == 64 and all(c in "0123456789abcdef" for c in exit_proof)
        rows.append(_facet("exit_status", "unavailable" if (not known and not independent_exit) or type(actual.exit_code) is not int
            else "pass" if actual.exit_code == expected["exit"] else "fail", "Exact finite-process exit, with independently authenticated failure preserved"))
        if known and type(actual.stdout) is bytes and type(actual.stderr) is bytes:
            # The prospective expected branch fixes the intended public stream;
            # a wrong actual exit cannot redirect interpretation to a convenient stream.
            raw = actual.stdout if expected["exit"] == 0 else actual.stderr
        rows.append(_facet("auxiliary_stream_unspecified", "unspecified",
                           "Public CLI contract does not require auxiliary-stream emptiness"))
    parsed: Any = None
    syntax: semantics.Disposition = "unavailable"
    if raw is not None:
        try:
            parsed = _strict_json(raw)
        except (ObservationLimit, AmbiguousJSON, RecursionError):
            syntax = "unavailable"
        except (ValueError, UnicodeError):
            if expected["kind"] == "cli" and expected["exit"] != 0:
                # Same conservative omission rule as frozen client observer v4:
                # no opening object token proves absence; mixed diagnostics do
                # not select a JSON substring or receive false product failure.
                try:
                    syntax = "fail" if "{" not in raw.decode("utf-8", "strict") else "unavailable"
                except UnicodeError:
                    syntax = "unavailable"
            else:
                syntax = "fail"
        else:
            syntax = "pass"
    rows.append(_facet("json_syntax", syntax, "Complete UTF-8 JSON value; duplicate members and mixed domain-error framing remain ungraded"))
    def compare(name: str, wanted: Any, value: Any) -> None:
        disposition: semantics.Disposition = "unavailable"
        if syntax == "pass":
            try:
                disposition = "pass" if _exact(wanted, value) else "fail"
            except ValueError:
                pass
        rows.append(_facet(name, disposition, "Source-declared host expectation, never derived from candidate output"))
    if "json" in expected:
        compare("json_exact", expected["json"], parsed)
    elif expected.get("json_value_only") is True:
        rows.append(_facet("json_wrapper_unspecified", "unspecified",
                           "Frozen public CLI supplement leaves the import wrapper unspecified"))
    else:
        compare("json_outer_keys", sorted(expected["required_keys"]), sorted(parsed) if type(parsed) is dict else None)
        for key, value in sorted(expected["json_subset"].items()):
            compare("json_field:" + key, value, parsed.get(key) if type(parsed) is dict else None)
    for pointer, bounds in sorted(expected.get("integer_ranges", {}).items()):
        disposition: semantics.Disposition = "unavailable"
        if syntax == "pass":
            try:
                value = _at(parsed, pointer)
            except (KeyError, IndexError, ValueError):
                disposition = "fail"
            else:
                disposition = "pass" if type(value) is int and bounds[0] <= value <= bounds[1] else "fail"
        rows.append(_facet("integer_range:" + pointer, disposition, "Declared exact integer interval; booleans and floats excluded"))
    if "body_utf8" in expected:
        rows.append(_facet("canonical_wire_bytes", "unavailable" if raw is None else
            "pass" if raw == expected["body_utf8"].encode("utf-8") else "fail", "Actual complete HTTP entity bytes equal canonical export payload"))
    result = tuple(rows)
    core.require(tuple(row.name for row in result) == declared_facets(expected), "Comparator facet roster drift")
    return result


def diagnose(profile: execution.HttpProductProfile, history: reader.HistoryObservation) -> tuple[StepDiagnostic, ...]:
    from . import candidate_product_process_execution_v1 as execution
    from . import candidate_product_process_reader_v1 as reader
    core.require(type(profile) is execution.HttpProductProfile and type(history) is reader.HistoryObservation,
                 "Exact product profile and original-reader observation types required")
    profile.check_current()
    core.require(len(history.steps) == len(profile.case.steps), "Complete step roster including unavailable suffix required")
    result = []
    for index, (step, actual) in enumerate(zip(profile.case.steps, history.steps, strict=True)):
        core.require((actual.step_id, actual.step_index, "request" if actual.kind == "probe" else actual.kind)
                     == (step.step_id, index, step.kind) and actual.state in ("authenticated", "unavailable", "unentered"),
                     "Original step identity, order, surface or state differs")
        assert step.expectation is not None
        expected = step.expectation.record()
        facets = compare_step(expected, actual)
        required = tuple(name for name in declared_facets(expected) if not name.endswith("_unspecified"))
        result.append(StepDiagnostic(profile.diagnostic_case_ids[index], step.step_id, index, step.kind,
            actual.state, facets, required, _status(facets, required, actual.state), actual.limitations))
    profile.check_current()
    return tuple(result)


def project_outcomes(profile: execution.HttpProductProfile, history: reader.HistoryObservation) -> HttpOutcomeProjection:
    diagnostics = diagnose(profile, history)
    facets = []
    for item in diagnostics:
        for facet in item.facets:
            if facet.name.startswith("mechanics_") or facet.name in ("listener_before", "listener_after"):
                facets.append(_facet(item.case_id + ":" + facet.name, facet.disposition, facet.reason))
    for name, known in (("complete_step_census", not history.missing_step_ids),
                        ("complete_cleanup", history.cleanup_verified),
                        ("complete_infrastructure", not history.infrastructure)):
        facets.append(_facet(name, "pass" if known else "unavailable", "Complete original physical history required"))
    complete = tuple(facets)
    required = tuple(x.name for x in complete)
    guard = MechanicsDiagnostic(profile.mechanics_case_id, complete, required,
                                _status(complete, required, "authenticated"))
    outcomes = tuple(registry.CaseResult(x.case_id, x.status) for x in diagnostics) + (registry.CaseResult(guard.case_id, guard.status),)
    core.require(tuple(x.case_id for x in outcomes) == profile.ordered_case_ids, "Full product/physical roster differs")
    return HttpOutcomeProjection(diagnostics, outcomes, guard)


def selector_catalog(case_id: str, *, purpose: str) -> dict[str, Any]:
    """Exact source-derived selectors; independent review/qualification remain mandatory."""
    from . import candidate_product_process_execution_v1 as execution
    case = core.case_definition(case_id)
    profile = execution.HttpProductProfile(case, core.ORIGINAL_DEFINITION_PURPOSE)
    profile.check_current(purpose=purpose, requirements_sha256=core.CONTRACT_SHA256)
    selectors = []
    for index, step in enumerate(case.steps):
        assert step.expectation is not None
        for facet_index, name in enumerate(declared_facets(step.expectation.record())):
            selectors.append({"case_id": profile.diagnostic_case_ids[index], "facet": name,
                "observation_pointer": "/diagnostics/" + str(index) + "/facets/" + str(facet_index) + "/disposition",
                "definition_pointer": "/definition/steps/" + str(index) + "/expectation",
                "disposition": "unspecified" if name.endswith("_unspecified") else "normative", "evidence_kind": "physical_mechanics" if name.startswith("mechanics_")
                    or name.startswith("listener_") or name == "lifecycle" else "physical_http" if step.kind == "request" else "physical_cli",
                "value_domain": ["pass", "fail", "unavailable", "unspecified"],
                "semantic_adequacy_reviewed": False, "physically_qualified": False})
    selectors.append({"case_id": profile.mechanics_case_id, "facet": "whole_history_mechanics",
        "observation_pointer": "/mechanics_guard/status", "definition_pointer": "/definition/steps",
        "disposition": "normative", "evidence_kind": "physical_mechanics",
        "semantic_adequacy_reviewed": False, "physically_qualified": False})
    return {"protocol": PROTOCOL, "case_id": case_id, "milestone": case.milestone,
        "source_contract_sha256": core.CONTRACT_SHA256, "target_contract_sha256": core.CONTRACT_SHA256,
        "original_definition_purpose": core.ORIGINAL_DEFINITION_PURPOSE, "execution_purpose": purpose,
        "definition": case.record(), "definition_sha256": core.digest(case.record()),
        "definition_sources": core.definition_sources(), "profile_sha256": profile.sha256,
        "evaluator_sources": execution.evaluator_sources(), "ordered_case_ids": list(profile.ordered_case_ids),
        "requirement_ids": list(case.requirement_ids), "selectors": selectors,
        "scope_review_supplied": False, "dispatch_authority": False, "acceptance_authority": False,
        "semantic_limits": dict(SEMANTIC_LIMITS),
        "required_unfinished_coverage": ["Abrupt live-worker death and owner contention", "Power-loss/commit-boundary recovery",
            "Schema3 live-handle incarnation fences", "Complete backup registry and ownership conservation",
            "Unasserted diagnostics values and HTTP response Content-Type",
            "Completed receipt replay and original manifest/hash/receipt serialization preservation",
            "Restore job-epoch and receipt fencing (this restore history has no jobs)",
            "Proof of no forbidden file I/O before backup-root refusal",
            "Unspecified CLI auxiliary streams and import wrappers",
            "Browser downloads and release handoff", "Full independent scope review and physical qualification"]}


class HttpObservationSource:
    """Exact original owner capability; no dictionary or fixture admission path."""

    def __init__(self, owner: execution.CandidateHttpExecution, checkpoint: execution.ControllerCheckpoint,
                 *, receipt_path: Path):
        from . import candidate_product_process_execution_v1 as execution
        execution.require(type(owner) is execution.CandidateHttpExecution
                          and type(checkpoint) is execution.ControllerCheckpoint,
                          "Exact HTTP owner and external checkpoint required")
        self.owner, self.checkpoint = owner, checkpoint
        self.receipt_path = Path(receipt_path).absolute()
        execution.require(self.receipt_path.resolve() == self.receipt_path and not self.receipt_path.is_symlink()
                          and self.receipt_path == owner.root / "semantic-verifier.json",
                          "Verifier receipt must be the fixed original-chain artifact")

    def observation(self, gate: registry.Gate, freeze: registry.CohortFreeze | None) -> registry.Observation:
        from . import candidate_product_process_execution_v1 as execution
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
        from . import candidate_product_process_execution_v1 as execution
        from . import candidate_product_process_reader_v1 as reader
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
        owner.profile.check_current(purpose=gate.binding.purpose,
                                    requirements_sha256=gate.binding.subject.requirements_sha256)
        projection = project_outcomes(owner.profile, history)
        diagnostics = projection.diagnostics
        admission.verify_loaded_sources(owner.sources)
        outcomes = projection.decisive_outcomes
        execution.require(tuple(item.case_id for item in outcomes) == gate.ordered_case_ids,
                          "Normalized outcome roster differs")
        execution.require(owner.checkpoint() == self.checkpoint and owner._freeze() == freeze
                          and execution.evaluator_sources() == owner.sources == execution._LOADED_SOURCES,
                          "Original evidence, loaded evaluator or admission changed during semantic verification")
        record = {"protocol": PROTOCOL if owner.profile.cumulative_profile is None else M4_PROTOCOL,
            "original_registration": asdict(owner.actual_registration),
            "original_execution_id": history.execution_id, "original_terminal_sha256": history.terminal_sha256,
            "original_binding_sha256": history.original_binding_sha256,
            "original_journal_context_sha256": self.checkpoint.context_sha256,
            "original_config_sha256": execution.sha256(owner.read_authenticated("config.json")),
            "original_journal": str(owner.root), "product_profile": owner.profile.record(),
            "cohort_freeze": None if freeze is None else asdict(freeze),
            "diagnostics": [asdict(item) for item in diagnostics],
            "decisive_outcomes": [asdict(item) for item in outcomes],
            "mechanics_guard": None if projection.mechanics_guard is None else asdict(projection.mechanics_guard),
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
