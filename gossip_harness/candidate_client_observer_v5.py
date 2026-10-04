"""V5 product-purpose binding for unchanged host-only finite CLI assertions.

Forked from frozen observer v4. Only observer/execution protocol domains and purpose admission change;
the product assertions, supported/unspecified dispositions and limits are the
same. The v5 controller owns complete start-response and execution provenance.

This module never starts candidate code, reads candidate files or authenticates a
journal. The execution controller supplies authenticated raw process provenance.
Structural validation here is necessary but cannot turn a fabricated dictionary
into an authenticated execution. Only the controller-owned C06 bridge may issue Registry evidence.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
from typing import Any, Mapping

FROZEN_V4_SOURCE_SHA256 = "214342a2f000f2da3c5d656a7edb1e22ebbbbe1cd2a5645ca406d64a31208235"

PROTOCOL = "candidate-client-observer-v5"
BINDING_PROTOCOL = "candidate-client-execution-v5-compact-v1"
M4_BINDING_PROTOCOL = "candidate-client-execution-v5-compact-m4-v1"
# An observer allocation bound, not a product output-size requirement.
MAX_OBSERVATION_BYTES = 32 * 1024 * 1024
MAX_JSON_DEPTH = 256
STATUSES = frozenset({"completed", "timeout", "output_limit", "transport_error",
                      "start_error", "completion_unproven"})
_DIGEST_FIELDS = frozenset({"source_sha256", "requirements_sha256", "definition_sha256",
                           "ordered_suite_sha256", "fixture_sha256", "runtime_sha256",
                           "environment_sha256", "limits_sha256", "evaluator_sha256"})
_BINDING_FIELDS = _DIGEST_FIELDS | frozenset({"protocol", "execution_id", "commit_oid", "tree_oid",
    "milestone", "purpose", "case_id", "step_id", "step_index", "ordered_step_ids", "argv"})


class ObservationUnavailable(ValueError):
    """Malformed/unbound raw observation, never a product correctness verdict."""


class InvalidExpectation(ValueError):
    """Malformed evaluator expectation; no candidate assertion may be emitted."""


class _AmbiguousJSON(ValueError):
    pass


@dataclass(frozen=True)
class ProcessObservation:
    """Immutable bytes only; authenticity must come from the owning controller."""

    binding_json: bytes
    transport_json: bytes
    stdout: bytes
    stderr: bytes


def encoded(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"),
                      allow_nan=False).encode("ascii")


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _digest(value: Any, length: int = 64) -> bool:
    return type(value) is str and len(value) == length and all(c in "0123456789abcdef" for c in value)


def _identifier(value: Any) -> bool:
    return (type(value) is str and 0 < len(value) <= 256
            and all(c in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.:-" for c in value))


def _canonical_mapping(value: Mapping[str, Any]) -> bytes:
    if type(value) is not dict:
        raise ObservationUnavailable("metadata must be a plain JSON object")
    try:
        result = encoded(value)
        if len(result) > 1024 * 1024:
            raise ObservationUnavailable("metadata observation bound")
        return result
    except (ValueError, TypeError, RecursionError, UnicodeError) as error:
        raise ObservationUnavailable("metadata is not finite JSON") from error


def _validate_binding(binding: dict[str, Any]) -> None:
    m4 = binding.get("protocol") == M4_BINDING_PROTOCOL
    extra = frozenset({"profile_sha256", "target_definition_sha256"}) if m4 else frozenset()
    if set(binding) != _BINDING_FIELDS | extra or binding["protocol"] not in (BINDING_PROTOCOL, M4_BINDING_PROTOCOL):
        raise ObservationUnavailable("execution binding field/protocol mismatch")
    if any(not _digest(binding[key]) for key in _DIGEST_FIELDS | extra):
        raise ObservationUnavailable("missing exact digest binding")
    if binding["milestone"] != ("M4" if m4 else "M1"):
        raise ObservationUnavailable("explicit execution milestone/protocol mismatch")
    if any(not _digest(binding[key], 40) for key in ("commit_oid", "tree_oid")):
        raise ObservationUnavailable("missing Git identity")
    if any(not _identifier(binding[key]) for key in ("execution_id", "milestone", "case_id", "step_id")):
        raise ObservationUnavailable("invalid execution identity")
    if binding["purpose"] not in ("public_release", "independent_acceptance", "repeatability"):
        raise ObservationUnavailable("v5 requires a prospectively registered product purpose")
    steps, index, argv = binding["ordered_step_ids"], binding["step_index"], binding["argv"]
    if (type(steps) is not list or not steps or len(steps) > 1024
            or any(not _identifier(step) for step in steps) or len(set(steps)) != len(steps)
            or type(index) is not int or not 0 <= index < len(steps) or steps[index] != binding["step_id"]):
        raise ObservationUnavailable("ordered step identity mismatch")
    if (type(argv) is not list or not argv or len(argv) > 256
            or any(type(arg) is not str or "\0" in arg or len(arg) > 65536 for arg in argv)):
        raise ObservationUnavailable("invalid bound argv")


def _validate_stream(descriptor: Any, raw: bytes) -> None:
    if type(descriptor) is not dict:
        raise ObservationUnavailable("missing raw stream descriptor")
    if (type(descriptor.get("path")) is not str or not descriptor["path"]
            or not _digest(descriptor.get("sha256")) or descriptor["sha256"] != _sha(raw)
            or type(descriptor.get("bytes")) is not int or descriptor["bytes"] != len(raw)
            or type(descriptor.get("observed_bytes")) is not int or descriptor["observed_bytes"] < len(raw)
            or type(descriptor.get("truncated")) is not bool):
        raise ObservationUnavailable("raw stream descriptor/hash mismatch")
    if "complete" in descriptor and type(descriptor["complete"]) is not bool:
        raise ObservationUnavailable("invalid raw stream completeness")
    if not descriptor["truncated"] and descriptor["observed_bytes"] != len(raw):
        raise ObservationUnavailable("unmarked stream truncation")
    if descriptor.get("complete") is True and descriptor["truncated"]:
        raise ObservationUnavailable("truncated stream claims complete")


def _natural_exit(transport: dict[str, Any]) -> int | None:
    completion = transport.get("completion")
    if type(completion) is not dict:
        return None
    wait, inspect = completion.get("wait_status_code"), completion.get("inspect_exit_code")
    if (completion.get("started") is not True or completion.get("natural") is not True
            or completion.get("oom_killed") is not False or completion.get("state_error") != ""
            or completion.get("identity_verified") is not True or completion.get("killed_by_controller") is not False
            or completion.get("signal_exit_ambiguous") is not False
            or not (completion.get("wait_error") is None or (type(completion.get("wait_error")) is dict
                    and completion["wait_error"].get("Message") == ""))
            or type(wait) is not int or not 0 <= wait < 128 or type(inspect) is not int or wait != inspect
            or transport.get("exit_code") != wait or type(transport.get("exit_code")) is not int
            or type(completion.get("container_id")) is not str or not completion["container_id"]
            or type(completion.get("finished_at")) is not str or not completion["finished_at"]):
        return None
    return wait


def _stream_complete(transport: dict[str, Any], channel: str) -> bool:
    descriptor = transport[channel]
    if descriptor["truncated"]:
        return False
    if "complete" in descriptor:
        return descriptor["complete"] is True
    return transport.get("capture_complete") is True


def _validate_transport(transport: dict[str, Any], stdout: bytes, stderr: bytes) -> None:
    if type(transport.get("status")) is not str or transport["status"] not in STATUSES:
        raise ObservationUnavailable("unknown process transport status")
    if ("history_state_verified" in transport
            and type(transport["history_state_verified"]) is not bool):
        raise ObservationUnavailable("invalid shared-history state provenance")
    if type(transport.get("completion")) is not dict:
        raise ObservationUnavailable("missing independent completion metadata")
    if type(transport.get("capture_complete")) is not bool:
        raise ObservationUnavailable("missing capture completeness metadata")
    if transport.get("exit_code") is not None and type(transport["exit_code"]) is not int:
        raise ObservationUnavailable("invalid process exit representation")
    _validate_stream(transport.get("stdout"), stdout)
    _validate_stream(transport.get("stderr"), stderr)
    if transport["capture_complete"] and not all(_stream_complete(transport, c) for c in ("stdout", "stderr")):
        raise ObservationUnavailable("contradictory aggregate capture completeness")
    if transport["status"] == "completed" and (
            _natural_exit(transport) is None or not transport["capture_complete"]):
        raise ObservationUnavailable("completed transport lacks completion/capture proof")


def process_observation(binding: Mapping[str, Any], transport: Mapping[str, Any],
                        stdout: bytes, stderr: bytes) -> ProcessObservation:
    """Freeze authenticated controller inputs; do not authenticate arbitrary data.

    The caller verifies external journals, before/after source/runtime/mount
    identities, process ownership and retained artifact provenance. Candidate
    stdout is never parsed as that supervisor envelope.
    """
    if type(stdout) is not bytes or type(stderr) is not bytes:
        raise ObservationUnavailable("raw streams must be immutable bytes")
    if len(stdout) + len(stderr) > MAX_OBSERVATION_BYTES:
        raise ObservationUnavailable("observer allocation bound")
    binding_json, transport_json = _canonical_mapping(binding), _canonical_mapping(transport)
    _validate_binding(json.loads(binding_json))
    _validate_transport(json.loads(transport_json), stdout, stderr)
    return ProcessObservation(binding_json, transport_json, stdout, stderr)


def observation_sha256(observation: ProcessObservation) -> str:
    # Domain-separated JSON hashes bind lengths and bytes without concatenation ambiguity.
    checked = _checked(observation)
    return _sha(encoded({"protocol": PROTOCOL, "binding_sha256": _sha(checked.binding_json),
        "transport_sha256": _sha(checked.transport_json),
        "stdout": {"sha256": _sha(checked.stdout), "bytes": len(checked.stdout)},
        "stderr": {"sha256": _sha(checked.stderr), "bytes": len(checked.stderr)}}))


def _checked(observation: ProcessObservation) -> ProcessObservation:
    if (type(observation) is not ProcessObservation or type(observation.binding_json) is not bytes
            or type(observation.transport_json) is not bytes):
        raise ObservationUnavailable("not a process observation")
    try:
        checked = process_observation(json.loads(observation.binding_json), json.loads(observation.transport_json),
                                      observation.stdout, observation.stderr)
    except (ValueError, TypeError, RecursionError) as error:
        raise ObservationUnavailable("malformed immutable observation") from error
    if checked != observation:
        raise ObservationUnavailable("observation metadata is not canonical")
    return checked


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _AmbiguousJSON("duplicate JSON member")
        result[key] = value
    return result


def _constant(value: str) -> Any:
    raise ValueError("non-JSON constant " + value)


def _depth(value: Any, level: int = 0) -> None:
    if level > MAX_JSON_DEPTH:
        raise _AmbiguousJSON("JSON observer depth bound")
    if type(value) is list:
        for item in value:
            _depth(item, level + 1)
    elif type(value) is dict:
        for item in value.values():
            _depth(item, level + 1)


def _json(raw: bytes) -> tuple[str, Any]:
    try:
        text = raw.decode("utf-8")
        if not text.strip(" \t\r\n"):
            return "absent", None
        value = json.loads(text, object_pairs_hook=_pairs, parse_int=Decimal, parse_float=Decimal, parse_constant=_constant)
        _depth(value)
        return "value", value
    except (_AmbiguousJSON, RecursionError, InvalidOperation):
        return "unsupported", None
    except (UnicodeError, ValueError):
        return "invalid", None


def _error_object_absent(raw: bytes) -> bool:
    # A complete UTF-8 stream with no object-opening token cannot contain any
    # JSON error object. This proves omission only; it never extracts a record.
    try:
        return "{" not in raw.decode("utf-8")
    except UnicodeError:
        return False


def _same_json(left: Any, right: Any) -> bool:
    # JSON has a single number type. Booleans are never equal to 0/1.
    numbers = (int, float, Decimal)
    if type(left) in numbers and type(right) in numbers:
        if type(right) is float and not math.isfinite(right):
            return False
        return Decimal(str(left)) == Decimal(str(right))
    if type(left) is not type(right):
        return False
    if type(left) is dict:
        return set(left) == set(right) and all(_same_json(left[k], right[k]) for k in left)
    if type(left) is list:
        return len(left) == len(right) and all(_same_json(a, b) for a, b in zip(left, right))
    return bool(left == right)


def _expectation(expected: Mapping[str, Any]) -> tuple[list[str], list[str]]:
    if type(expected) is not dict:
        raise InvalidExpectation("expectation must be an object")
    try:
        encoded(expected)
    except (ValueError, TypeError, RecursionError) as error:
        raise InvalidExpectation("expectation is not finite JSON") from error
    kind = expected.get("kind")
    if kind not in ("success", "domain_error", "usage_or_rejection"):
        raise InvalidExpectation("unknown expectation kind")
    exit_code = expected.get("exit_code")
    if type(exit_code) is not int or exit_code != (0 if kind == "success" else 2):
        raise InvalidExpectation("unsupported CLI exit expectation")
    if type(expected.get("semantic_value_supported")) is not bool:
        raise InvalidExpectation("missing semantic support discriminator")
    code = expected.get("error_code")
    if code is not None and (type(code) is not str or not code):
        raise InvalidExpectation("invalid error-code expectation")
    unspecified, ids = expected.get("unspecified_assertion_ids"), expected.get("assertion_ids")
    if (type(unspecified) is not list or type(ids) is not list
            or any(not _identifier(key) for key in unspecified + ids)
            or len(set(unspecified)) != len(unspecified) or len(set(ids)) != len(ids)):
        raise InvalidExpectation("invalid assertion census")
    supported = ["process.exit"]
    if kind == "success":
        supported.append("stdout.json")
        if expected["semantic_value_supported"]:
            try:
                encoded(expected["value"])
            except (ValueError, TypeError, KeyError, RecursionError) as error:
                raise InvalidExpectation("unsupported expected JSON value") from error
            supported.append("stdout.value")
    elif kind == "domain_error":
        supported.append("stderr.error")
        if code is not None:
            supported.append("stderr.error_code")
        elif "stderr.error_code" not in unspecified:
            raise InvalidExpectation("unspecified exact error code must remain explicit")
    if (set(supported) & set(unspecified) or set(ids) != set(supported) | set(unspecified)
            or not ids):
        raise InvalidExpectation("assertion census omits/adds a supported predicate")
    return supported, unspecified


def observe_cli_step(expected: Mapping[str, Any], observation: ProcessObservation | None) -> dict[str, Any]:
    """Grade one finite CLI invocation, preserving false beside unknown siblings.

    Missing execution, caps and deadlines do not assert a new product fault.
    Pure whole-stderr error JSON is supported evidence; mixed diagnostic framing
    remains unavailable without substring extraction. No other-stream emptiness
    check is created. The caller prefixes assertion IDs with the ordered step ID.
    """
    supported, unspecified = _expectation(expected)
    assertions: dict[str, bool | None] = {key: None for key in expected["assertion_ids"]}
    reasons = {key: "unspecified by the frozen product contract" if key in unspecified
               else "process observation unavailable" for key in assertions}
    obs_digest: str | None = None
    if observation is not None:
        observation = _checked(observation)
        transport = json.loads(observation.transport_json)
        obs_digest = observation_sha256(observation)
        actual_exit = _natural_exit(transport)
        if actual_exit is not None:
            assertions["process.exit"] = actual_exit == expected["exit_code"]
            reasons["process.exit"] = "independent natural wait/inspect exit"
        kind = expected["kind"]
        if transport.get("history_state_verified") is False:
            for key in supported:
                if key != "process.exit":
                    reasons[key] = "shared-history state provenance unavailable"
        channel = "stdout" if kind == "success" else "stderr"
        if (kind != "usage_or_rejection" and actual_exit is not None
                and transport.get("history_state_verified", True) is True
                and _stream_complete(transport, channel)):
            framing, value = _json(getattr(observation, channel))
            if kind == "success":
                if framing != "unsupported":
                    assertions["stdout.json"] = framing == "value"
                    reasons["stdout.json"] = "complete whole stdout JSON grammar"
                if "stdout.value" in supported and framing == "value":
                    assertions["stdout.value"] = _same_json(value, expected["value"])
                    reasons["stdout.value"] = "semantic JSON comparison"
                elif "stdout.value" in supported and framing in ("absent", "invalid"):
                    assertions["stdout.value"] = False
                    reasons["stdout.value"] = "complete stdout lacks the prescribed JSON value"
            elif framing in ("value", "absent") or (
                    framing == "invalid" and _error_object_absent(observation.stderr)):
                valid_error = (type(value) is dict and set(value) == {"error"}
                               and type(value["error"]) is str and bool(value["error"]))
                assertions["stderr.error"] = valid_error
                reasons["stderr.error"] = "complete stderr proves error-object absence or contains one known JSON value"
                if "stderr.error_code" in supported:
                    assertions["stderr.error_code"] = (type(value) is dict and "error" in value
                                                        and _same_json(value["error"], expected["error_code"]))
                    reasons["stderr.error_code"] = "explicitly prescribed code in observed stderr JSON"
            else:
                reasons["stderr.error"] = "mixed, invalid or unsupported stderr framing is ungraded"
                if "stderr.error_code" in supported:
                    reasons["stderr.error_code"] = reasons["stderr.error"]
    unavailable = [key for key in supported if assertions[key] is None]
    failures = [key for key in supported if assertions[key] is False]
    status = ("product-failed" if failures else "observation-unavailable" if unavailable
              else "partial-observation" if unspecified else "observed-match")
    return {"protocol": PROTOCOL, "expectation_sha256": _sha(encoded(expected)),
            "observation_sha256": obs_digest, "status": status,
            "assertions": assertions,
            "assertion_dispositions": {key: "unspecified" if key in unspecified else
                "unavailable" if assertions[key] is None else "supported" for key in assertions},
            "assertion_reasons": reasons, "failed_assertion_ids": failures,
            "unavailable_assertion_ids": unavailable, "unspecified_assertion_ids": list(unspecified),
            "supported_assertions_passed": all(assertions[key] is True for key in supported),
            "all_local_assertions_passed": all(value is True for value in assertions.values()),
            "production_acceptance_authority": False}
