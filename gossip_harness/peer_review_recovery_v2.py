"""Pure, prospective recovery for a complete but malformed reviewer response.

This does not alter the frozen continuation controller or authorize release.
Only a known complete response is eligible for schema correction. Reservations
consume the caller's existing reviewer-call allowance *before* dispatch, and a
correction also consumes its separately declared, finite correction allowance.

The host must persist each returned state with compare-and-swap before dispatch,
own provider completion/accounting evidence, and resume the same durable chain.
Restoring state supplies no dispatch permission. A trusted current checkpoint
digest is required on restore: a checksum alone cannot prevent disk rollback,
and this pure module cannot prevent callers creating replacement chains. Source,
target, view, ordered suite and schema digests refer to immutable host artifacts;
the host must rematerialize that exact context for a correction request.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, replace
import hashlib
import json
import re
from typing import Callable, Literal


PROTOCOL = "peer-review-recovery-v2"
MAX_RESPONSE_BYTES = 65536
MAX_STATE_BYTES = 16384
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
Phase = Literal["ready", "pending", "schema_invalid", "validated", "context_changed",
                "exhausted", "provider_unknown", "infrastructure_failure"]
CallKind = Literal["fresh", "correction"]
ProviderStatus = Literal["complete", "provider_unknown", "infrastructure_failure"]
_PHASES = {"ready", "pending", "schema_invalid", "validated", "context_changed",
           "exhausted", "provider_unknown", "infrastructure_failure"}


class RecoveryError(ValueError):
    """Invalid state, evidence binding, replay, or transition."""


def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
                      allow_nan=False)


def _digest(domain: str, value: object) -> str:
    return hashlib.sha256((PROTOCOL + ":" + domain + ":" + _canonical(value)).encode()).hexdigest()


def _check_digest(value: object) -> None:
    if type(value) is not str or _DIGEST.fullmatch(value) is None:
        raise RecoveryError("Expected a lowercase SHA256 digest")


def _integer(value: object, minimum: int = 0) -> None:
    if type(value) is not int or not minimum <= value <= 2**53 - 1:
        raise RecoveryError("Budget values must be finite nonnegative safe integers")


def _errors(values: object) -> None:
    if (type(values) is not tuple or len(values) > 16
            or any(type(item) is not str or not item or len(_canonical(item)) > 256 for item in values)):
        raise RecoveryError("Validation errors must be a bounded tuple of nonempty strings")


def _pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise RecoveryError("Duplicate JSON key")
        result[key] = value
    return result


def _constant(value: str) -> None:
    raise RecoveryError("Nonfinite JSON number")


def _decode(text: str, maximum: int) -> object:
    try:
        if type(text) is not str or len(text.encode("utf-8")) > maximum:
            raise RecoveryError("JSON text exceeds its size limit")
        return json.loads(text, object_pairs_hook=_pairs, parse_constant=_constant)
    except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
        raise RecoveryError("Invalid bounded strict JSON") from exc


@dataclass(frozen=True)
class ReviewBinding:
    source_sha256: str
    target_sha256: str
    view_sha256: str
    suite_sha256: str
    schema_sha256: str

    def __post_init__(self) -> None:
        for value in asdict(self).values():
            _check_digest(value)


@dataclass(frozen=True)
class ValidationOutcome:
    """A trusted validator's verdict about the entire response, not its semantics."""

    valid: bool
    errors: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _errors(self.errors)
        if type(self.valid) is not bool or self.valid == bool(self.errors):
            raise RecoveryError("Valid responses have no errors; invalid responses require errors")


@dataclass(frozen=True)
class SchemaValidator:
    """The host owns this pure full-response validator and its schema identity."""

    schema_sha256: str
    validate: Callable[[str], ValidationOutcome]

    def __post_init__(self) -> None:
        _check_digest(self.schema_sha256)
        if not callable(self.validate):
            raise RecoveryError("A schema validator must be callable")


@dataclass(frozen=True)
class ProviderOutcome:
    """Classify provider/journal completion before attempting schema validation.

    ``complete`` means the host has durable completed/accounted call evidence,
    not merely that it found some output bytes. Unknown calls remain spent and
    blocked; infrastructure failures are not malformed reviewer decisions.
    """

    status: ProviderStatus
    response: str | None = None
    detail: str = ""

    def __post_init__(self) -> None:
        if self.status not in {"complete", "provider_unknown", "infrastructure_failure"}:
            raise RecoveryError("Unknown provider outcome")
        if self.status == "complete":
            if type(self.response) is not str or self.detail:
                raise RecoveryError("A complete outcome requires response text only")
        elif (self.response is not None or type(self.detail) is not str or not self.detail
              or len(_canonical(self.detail)) > 256):
            raise RecoveryError("Failure outcomes require a bounded detail and no response")


@dataclass(frozen=True)
class RecoveryState:
    recovery_id: str
    binding: ReviewBinding
    total_call_limit: int
    correction_limit: int
    calls_used: int
    corrections_used: int = 0
    calls_used_before_chain: int = 0
    phase: Phase = "ready"
    pending_call_id: str | None = None
    response_sha256: str | None = None
    validation_errors: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if type(self.recovery_id) is not str or not 1 <= len(self.recovery_id) <= 128:
            raise RecoveryError("A durable recovery identity is required")
        if type(self.binding) is not ReviewBinding or self.phase not in _PHASES:
            raise RecoveryError("Invalid binding or phase")
        _integer(self.total_call_limit, 1)
        for value in (self.correction_limit, self.calls_used, self.corrections_used,
                      self.calls_used_before_chain):
            _integer(value)
        if (not self.calls_used_before_chain <= self.calls_used <= self.total_call_limit
                or self.correction_limit > self.total_call_limit
                or self.corrections_used > self.correction_limit
                or self.corrections_used > max(0, self.calls_used - self.calls_used_before_chain - 1)):
            raise RecoveryError("Inconsistent reviewer/correction budget")
        _errors(self.validation_errors)
        if self.pending_call_id is not None:
            _check_digest(self.pending_call_id)
        if self.response_sha256 is not None:
            _check_digest(self.response_sha256)
        if (self.phase == "pending") != (self.pending_call_id is not None):
            raise RecoveryError("Only a pending state has a reserved call")
        if self.phase == "pending" and (self.response_sha256 is not None or self.validation_errors):
            raise RecoveryError("A pending call cannot already have a response or outcome")
        if self.phase == "provider_unknown" and self.response_sha256 is not None:
            raise RecoveryError("An unknown provider outcome has no completed response")
        if self.phase in {"provider_unknown", "infrastructure_failure"} and not self.validation_errors:
            raise RecoveryError("A failed provider or validator outcome requires its diagnostic")
        if (self.phase == "exhausted" and self.calls_used < self.total_call_limit
                and self.corrections_used < self.correction_limit):
            raise RecoveryError("An exhausted phase requires an exhausted allowance")
        if self.phase in {"schema_invalid", "validated", "context_changed"} and self.response_sha256 is None:
            raise RecoveryError("A completed response digest is required")
        if self.phase == "schema_invalid" and not self.validation_errors:
            raise RecoveryError("A malformed response requires validation errors")
        if self.phase == "validated" and self.validation_errors:
            raise RecoveryError("A validated response cannot contain validation errors")
        if self.phase == "ready" and (self.calls_used != self.calls_used_before_chain
                                      or self.corrections_used or self.response_sha256
                                      or self.validation_errors):
            raise RecoveryError("Ready state cannot erase prior attempts")
        if self.phase != "ready" and self.calls_used == self.calls_used_before_chain:
            raise RecoveryError("A non-ready phase requires a consumed call")

    @property
    def checkpoint_sha256(self) -> str:
        return _digest("state", {"protocol": PROTOCOL, **asdict(self)})

    def to_json(self) -> str:
        return _canonical({"state": {"protocol": PROTOCOL, **asdict(self)},
                           "sha256": self.checkpoint_sha256})

    @classmethod
    def from_json(cls, text: str, *, expected_sha256: str) -> RecoveryState:
        """Restore only against a host-trusted current checkpoint, never a default."""
        _check_digest(expected_sha256)
        envelope = _decode(text, MAX_STATE_BYTES)
        if type(envelope) is not dict or set(envelope) != {"state", "sha256"}:
            raise RecoveryError("Invalid checkpoint envelope")
        data = envelope["state"]
        if (type(data) is not dict or data.get("protocol") != PROTOCOL
                or envelope["sha256"] != expected_sha256
                or _digest("state", data) != expected_sha256):
            raise RecoveryError("Checkpoint identity or integrity mismatch")
        try:
            values = dict(data)
            del values["protocol"]
            if type(values.get("binding")) is not dict or type(values.get("validation_errors")) is not list:
                raise RecoveryError("Malformed checkpoint fields")
            values["binding"] = ReviewBinding(**values["binding"])
            values["validation_errors"] = tuple(values["validation_errors"])
            if set(values) != set(cls.__dataclass_fields__):
                raise RecoveryError("Checkpoint fields must be complete and exact")
            return cls(**values)
        except (TypeError, KeyError) as exc:
            raise RecoveryError("Malformed checkpoint fields") from exc


@dataclass(frozen=True)
class CorrectionPrompt:
    binding: ReviewBinding
    previous_response_sha256: str
    validation_errors: tuple[str, ...]
    instruction: str = (
        "The previous complete response failed the declared response schema. "
        "Return one complete replacement response using exactly the same immutable "
        "source, target, reviewer view, suite and schema. Reassess all required "
        "fields; no previous partial decision is accepted. Semantic rejection or "
        "insufficient evidence is permitted. Do not change code or review context."
    )


@dataclass(frozen=True)
class ReviewRequest:
    call_id: str
    kind: CallKind
    binding: ReviewBinding
    correction: CorrectionPrompt | None


@dataclass(frozen=True)
class ReservationResult:
    state: RecoveryState
    request: ReviewRequest | None


@dataclass(frozen=True)
class ResponseResult:
    state: RecoveryState
    # This is a fully schema-validated semantic response, never release permission.
    validated_response: str | None = None


def new_recovery(*, recovery_id: str, binding: ReviewBinding, total_call_limit: int,
                 correction_limit: int, calls_already_used: int = 0) -> RecoveryState:
    """Start once under host ownership; restore this state on subsequent restarts."""
    return RecoveryState(recovery_id, binding, total_call_limit, correction_limit,
                         calls_already_used, calls_used_before_chain=calls_already_used)


def _reserve(state: RecoveryState, binding: ReviewBinding, kind: CallKind) -> ReservationResult:
    if state.calls_used >= state.total_call_limit:
        # A ready chain with no remaining allowance stays ready, with no request.
        stopped = replace(state, phase="exhausted") if state.calls_used > state.calls_used_before_chain else state
        return ReservationResult(stopped, None)
    call_id = _digest("call", {"checkpoint": state.checkpoint_sha256, "kind": kind,
                               "binding": asdict(binding), "number": state.calls_used + 1})
    prompt = None
    if kind == "correction":
        assert state.response_sha256 is not None
        prompt = CorrectionPrompt(binding, state.response_sha256, state.validation_errors)
    updated = replace(state, binding=binding, calls_used=state.calls_used + 1,
                      corrections_used=state.corrections_used + int(kind == "correction"),
                      phase="pending", pending_call_id=call_id, response_sha256=None,
                      validation_errors=())
    return ReservationResult(updated, ReviewRequest(call_id, kind, binding, prompt))


def reserve_fresh_review(state: RecoveryState, binding: ReviewBinding) -> ReservationResult:
    """A changed context needs a fresh counted review; correction budget persists."""
    if state.phase not in {"ready", "context_changed", "validated", "exhausted"}:
        raise RecoveryError("This state does not permit a fresh review")
    if state.phase != "ready" and binding == state.binding:
        raise RecoveryError("A terminal semantic review cannot be retried as a format correction")
    return _reserve(state, binding, "fresh")


def reserve_correction(state: RecoveryState, current_binding: ReviewBinding) -> ReservationResult:
    """Return at most one counted request after a known complete malformed response."""
    if state.phase not in {"schema_invalid", "exhausted"}:
        raise RecoveryError("Only a complete schema-invalid response permits correction")
    if state.phase == "exhausted":
        return ReservationResult(state, None)
    if current_binding != state.binding:
        return ReservationResult(replace(state, phase="context_changed"), None)
    if state.corrections_used >= state.correction_limit or state.calls_used >= state.total_call_limit:
        return ReservationResult(replace(state, phase="exhausted"), None)
    return _reserve(state, current_binding, "correction")


def record_response(state: RecoveryState, *, call_id: str, current_binding: ReviewBinding,
                    outcome: ProviderOutcome, validator: SchemaValidator | None = None) -> ResponseResult:
    """Resolve exactly the pending call. Stale/replayed responses fail closed."""
    if state.phase != "pending" or call_id != state.pending_call_id:
        raise RecoveryError("Response does not identify the pending reserved call")
    if type(outcome) is not ProviderOutcome:
        raise RecoveryError("Provider completion must be explicitly classified")
    if outcome.status != "complete":
        return ResponseResult(replace(state, phase=outcome.status, pending_call_id=None,
                                      validation_errors=(outcome.detail,)))
    assert outcome.response is not None
    response_sha256 = _digest("response", outcome.response)
    if current_binding != state.binding:
        return ResponseResult(replace(state, phase="context_changed", pending_call_id=None,
                                      response_sha256=response_sha256))
    if type(validator) is not SchemaValidator or validator.schema_sha256 != state.binding.schema_sha256:
        raise RecoveryError("The exact bound full-response schema validator is required")
    try:
        if len(outcome.response.encode("utf-8")) > MAX_RESPONSE_BYTES:
            validation = ValidationOutcome(False, ("Response exceeds maximum encoded size",))
        else:
            validation = validator.validate(outcome.response)
        if type(validation) is not ValidationOutcome:
            raise RecoveryError("Validator returned no typed full-response outcome")
    except Exception:
        # A validator crash is infrastructure failure, never permission to retry.
        return ResponseResult(replace(state, phase="infrastructure_failure", pending_call_id=None,
                                      response_sha256=response_sha256,
                                      validation_errors=("Full-response validator failed",)))
    phase: Phase = "validated" if validation.valid else "schema_invalid"
    updated = replace(state, phase=phase, pending_call_id=None, response_sha256=response_sha256,
                      validation_errors=validation.errors)
    return ResponseResult(updated, outcome.response if validation.valid else None)


# A small strict fixture adapter; production scoped verdicts require their own
# full-response validator and schema digest. Policy transitions do not inspect
# decisions or coerce notes, and cannot turn approval into release authorization.
SIMPLE_REVIEW_SCHEMA_SHA256 = _digest("fixture-schema", {
    "fields": ["decision", "notes", "remaining"],
    "decisions": ["approve", "request_changes", "insufficient_evidence"],
    "notes": "string, max 8192 characters",
    "remaining": "unique nonempty strings, max 128 items, max 256 characters each",
})


def validate_simple_review(text: str) -> ValidationOutcome:
    try:
        value = _decode(text, MAX_RESPONSE_BYTES)
        if type(value) is not dict or set(value) != {"decision", "notes", "remaining"}:
            return ValidationOutcome(False, ("Expected exactly decision, notes, remaining",))
        errors = []
        if type(value["decision"]) is not str or value["decision"] not in {"approve", "request_changes", "insufficient_evidence"}:
            errors.append("decision must be a supported semantic decision")
        if type(value["notes"]) is not str or len(value["notes"]) > 8192:
            errors.append("notes must be a string of at most 8192 characters")
        remaining = value["remaining"]
        if (type(remaining) is not list or len(remaining) > 128
                or any(type(item) is not str or not 1 <= len(item) <= 256 for item in remaining)
                or len(set(remaining)) != len(remaining)):
            errors.append("remaining must be a bounded list of unique requirement IDs")
        return ValidationOutcome(not errors, tuple(errors))
    except (RecoveryError, TypeError):
        return ValidationOutcome(False, ("Response must be one complete strict JSON object",))


SIMPLE_REVIEW_VALIDATOR = SchemaValidator(SIMPLE_REVIEW_SCHEMA_SHA256, validate_simple_review)
