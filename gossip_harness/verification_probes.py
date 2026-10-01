"""Bounded, data-only admission of reviewer probes against a trusted oracle.

This is deliberately oracle-assisted. No candidate code, private cases, or model
calls enter this module. Rejected expectations are never corrected or returned.
The engine owns cumulative stage budgets; this gate also enforces the remaining
``max_new`` allowance supplied on each call and deduplicates against prior cases.
"""

from __future__ import annotations

from copy import deepcopy
import hashlib
import json
import math
import re

from .blackbox_validator import json_equal


MAX_PROPOSALS = 4
MAX_STAGE_PROBES = 8
MAX_BATCH_BYTES = 65_536
MAX_VALUE_BYTES = 16_384
VALIDATION_POLICY = "verification-fixture-oracle-v1"


def canonical_json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=True, allow_nan=False,
                      separators=(",", ":"))


def _sha(value):
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def _bounded(value, *, byte_limit=MAX_VALUE_BYTES):
    count = 0

    def visit(item, depth):
        nonlocal count
        count += 1
        if count > 2048 or depth > 10:
            raise ValueError("JSON tree exceeds limits")
        if item is None or type(item) is bool:
            return
        if type(item) is int:
            if abs(item) > 2**63 - 1:
                raise ValueError("JSON integer exceeds limits")
        elif type(item) is float:
            if not math.isfinite(item) or abs(item) > 1e100:
                raise ValueError("JSON number exceeds limits")
        elif type(item) is str:
            if len(item) > 4096:
                raise ValueError("JSON string exceeds limits")
            item.encode("utf-8")
        elif type(item) is list:
            if len(item) > 256:
                raise ValueError("JSON array exceeds limits")
            for child in item:
                visit(child, depth + 1)
        elif type(item) is dict:
            if len(item) > 128 or any(type(key) is not str for key in item):
                raise ValueError("JSON object exceeds limits")
            for key, child in item.items():
                visit(key, depth + 1)
                visit(child, depth + 1)
        else:
            raise ValueError("Not JSON data")

    try:
        visit(value, 0)
        if len(canonical_json(value).encode()) > byte_limit:
            raise ValueError("JSON bytes exceed limits")
    except (UnicodeError, RecursionError) as exc:
        raise ValueError("Invalid bounded JSON") from exc


def _stage(stage_index):
    if type(stage_index) is not int or not 0 <= stage_index <= 3:
        raise ValueError("stage_index must be an integer in 0..3")


def _namespace(namespace):
    if type(namespace) is not str or not re.fullmatch(r"[A-Za-z0-9_.-]{1,96}", namespace):
        raise ValueError("Invalid probe namespace")


def _origin(origin, stage_index=None):
    if type(origin) is not dict or not origin:
        raise ValueError("Probe origin must be a nonempty JSON object")
    _bounded(origin, byte_limit=4096)
    if (stage_index is not None and "stage_index" in origin
            and (type(origin["stage_index"]) is not int
                 or origin["stage_index"] != stage_index)):
        raise ValueError("Origin stage does not match admission stage")


def _pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError("Duplicate JSON member")
        result[key] = value
    return result


def _constant(value):
    raise ValueError("Nonfinite JSON")


def _receipt(stage_index, origin, mode):
    return {"mode": mode, "stage_index": stage_index,
            "origin": deepcopy(origin), "validation_policy": VALIDATION_POLICY,
            "oracle_assisted": True, "proposed_count": 0,
            "admitted_cases": [], "rejected": [], "retired": []}


def _case_binding(case):
    return {key: value for key, value in case.items() if key != "validation_sha256"}


def _verify_case(case):
    fields = {"id", "namespace", "input", "expected", "requirement", "origin",
              "proposal_index", "proposal_sha256", "input_sha256",
              "admitted_stage_index", "validated_stage_index",
              "validation_policy", "validation_sha256"}
    if type(case) is not dict or set(case) != fields:
        raise ValueError("Invalid admitted probe schema")
    _stage(case["admitted_stage_index"])
    _stage(case["validated_stage_index"])
    _namespace(case["namespace"])
    _origin(case["origin"], case["admitted_stage_index"])
    if (case["validated_stage_index"] < case["admitted_stage_index"]
            or type(case["proposal_index"]) is not int
            or not 0 <= case["proposal_index"] < MAX_PROPOSALS
            or type(case["requirement"]) is not str
            or not case["requirement"] or len(case["requirement"]) > 256):
        raise ValueError("Invalid admitted probe metadata")
    _bounded(case["input"])
    _bounded(case["expected"])
    proposal = {key: case[key] for key in ("input", "expected", "requirement")}
    identity = _sha(case["input"])
    if (case["validation_policy"] != VALIDATION_POLICY
            or case["input_sha256"] != identity
            or case["id"] != f"{case['namespace']}:{identity}"
            or case["proposal_sha256"] != _sha(proposal)
            or case["validation_sha256"] != _sha(_case_binding(case))):
        raise ValueError("Admitted probe binding mismatch")


class _OutsideInputDomain(Exception):
    """Only the explicit input validator may produce this normal rejection."""


def _oracle(stage_index, payload, reference, validate_input):
    # Independent copies prevent trusted normalization from changing retained
    # evidence or passing a normalized value to the reference by accident.
    try:
        validate_input(stage_index, deepcopy(payload))
    except ValueError:
        raise _OutsideInputDomain from None
    expected = reference(stage_index, deepcopy(payload))
    try:
        _bounded(expected)
    except ValueError as exc:
        raise RuntimeError("Trusted oracle returned unbounded JSON") from exc
    return expected


def parse_proposals(content, *, stage_index, requirements, reference,
                    validate_input, origin, existing_cases=(), max_new=8,
                    namespace="verification-probe"):
    """Admit strict ``{'probes': [{requirement,input,expected}, ...]}`` data.

    At most four proposals may be submitted per review, and ``max_new`` is the
    engine's remaining stage allowance (0..8). Existing cases may be admitted
    probes or trusted public cases; only their inputs are used for deduplication.
    Input-domain ``ValueError`` becomes a generic rejection. Unexpected callback
    failures propagate, rather than masquerading as bad model proposals.
    """
    _stage(stage_index)
    _namespace(namespace)
    _origin(origin, stage_index)
    if (type(max_new) is not int or not 0 <= max_new <= MAX_STAGE_PROBES
            or not callable(reference) or not callable(validate_input)):
        raise ValueError("Invalid gate configuration")
    if (not isinstance(requirements, (list, tuple, set, frozenset)) or not requirements
            or any(type(item) is not str or not item or len(item) > 256
                   for item in requirements)):
        raise ValueError("Known requirement labels are required")
    if not isinstance(existing_cases, (list, tuple)):
        raise ValueError("Existing cases must be a sequence")
    seen = set()
    for case in existing_cases:
        if type(case) is not dict or "input" not in case:
            raise ValueError("Invalid existing case")
        if "validation_policy" in case:
            _verify_case(case)
        _bounded(case["input"])
        seen.add(_sha(case["input"]))
    receipt = _receipt(stage_index, origin, "new")
    receipt.update(content_sha256=None, max_new=max_new, namespace=namespace)

    def reject(index, reason, proposal=None):
        item = {"index": index, "reason": reason}
        if proposal is not None:
            try:
                item["proposal_sha256"] = _sha(proposal)
            except (ValueError, TypeError, RecursionError):
                pass
        receipt["rejected"].append(item)

    if type(content) is not str:
        reject(None, "batch_not_text")
        return receipt
    try:
        encoded = content.encode("utf-8")
    except UnicodeError:
        reject(None, "batch_invalid_utf8")
        return receipt
    receipt["content_sha256"] = hashlib.sha256(encoded).hexdigest()
    if len(encoded) > MAX_BATCH_BYTES:
        reject(None, "batch_too_large")
        return receipt
    try:
        envelope = json.loads(content, object_pairs_hook=_pairs, parse_constant=_constant)
    except (ValueError, RecursionError):
        reject(None, "batch_invalid_json")
        return receipt
    if (type(envelope) is not dict or set(envelope) != {"probes"}
            or type(envelope["probes"]) is not list):
        reject(None, "batch_schema")
        return receipt
    proposals = envelope["probes"]
    receipt["proposed_count"] = len(proposals)
    if len(proposals) > MAX_PROPOSALS:
        reject(None, "batch_too_many_cases")
        return receipt
    for index, proposal in enumerate(proposals):
        if type(proposal) is not dict or set(proposal) != {"input", "expected", "requirement"}:
            reject(index, "case_schema", proposal)
            continue
        if type(proposal["requirement"]) is not str or proposal["requirement"] not in requirements:
            reject(index, "unsupported_requirement", proposal)
            continue
        try:
            _bounded(proposal["input"])
            _bounded(proposal["expected"])
        except ValueError:
            reject(index, "case_exceeds_json_limits", proposal)
            continue
        identity = _sha(proposal["input"])
        if identity in seen:
            reject(index, "duplicate_input", proposal)
            continue
        if len(receipt["admitted_cases"]) >= max_new:
            reject(index, "stage_cap", proposal)
            continue
        try:
            expected = _oracle(stage_index, proposal["input"], reference, validate_input)
        except _OutsideInputDomain:
            reject(index, "oracle_rejected_input", proposal)
            continue
        if not json_equal(proposal["expected"], expected):
            reject(index, "wrong_expectation", proposal)
            continue
        case = {**deepcopy(proposal), "id": f"{namespace}:{identity}",
                "namespace": namespace, "origin": deepcopy(origin),
                "proposal_index": index, "proposal_sha256": _sha(proposal),
                "input_sha256": identity, "admitted_stage_index": stage_index,
                "validated_stage_index": stage_index,
                "validation_policy": VALIDATION_POLICY}
        case["validation_sha256"] = _sha(_case_binding(case))
        receipt["admitted_cases"].append(case)
        seen.add(identity)
    return receipt


def revalidate_cases(cases, *, stage_index, reference, validate_input, origin=None):
    """Keep unchanged oracle-valid probes; retire changed contracts generically.

    IDs, proposed labels and provenance remain stable. A retired record contains
    no corrected expected value. A corrupt retained binding or unexpected oracle
    failure is a controller error, not a normal contract-change retirement.
    """
    _stage(stage_index)
    if (not isinstance(cases, (list, tuple)) or not callable(reference)
            or not callable(validate_input)):
        raise ValueError("Invalid revalidation configuration")
    if origin is not None:
        _origin(origin, stage_index)
    receipt = _receipt(stage_index, origin, "revalidate")
    receipt["proposed_count"] = len(cases)
    seen = set()
    # Check all retained bindings before any reference call, failing atomically
    # when the trusted saved pool is inconsistent.
    for case in cases:
        _verify_case(case)
        if stage_index < case["validated_stage_index"] or case["input_sha256"] in seen:
            raise ValueError("Invalid revalidation stage or duplicate input")
        seen.add(case["input_sha256"])
    for case in cases:
        try:
            expected = _oracle(stage_index, case["input"], reference, validate_input)
            retained = json_equal(case["expected"], expected)
        except _OutsideInputDomain:
            retained = False
        if not retained:
            receipt["retired"].append({"id": case["id"], "reason": "contract_changed",
                                       "input_sha256": case["input_sha256"],
                                       "previous_validation_sha256": case["validation_sha256"],
                                       "stage_index": stage_index})
            continue
        accepted = deepcopy(case)
        accepted["validated_stage_index"] = stage_index
        accepted["validation_sha256"] = _sha(_case_binding(accepted))
        receipt["admitted_cases"].append(accepted)
    return receipt
