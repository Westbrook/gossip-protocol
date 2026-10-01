"""Data-only, oracle-assisted test eligibility and frozen-pool selection.

Generated expectations are checked against the trusted fixture oracle *before*
being used for selection. This is deliberately an oracle-assisted experiment:
provenance hashes establish integrity, not independent semantic validation.
Only visible case outcomes enter ``select_candidate``. Held-out correctness is
accepted by the separate, post-selection ``evaluate_selection`` function.
"""

from __future__ import annotations

import hashlib
import json
import math
from typing import Mapping

from .blackbox_validator import json_equal


MAX_PROPOSALS = 6
MAX_PROPOSAL_BYTES = 65_536
MAX_VALUE_BYTES = 16_384
MAX_DEPTH = 10
MAX_NODES = 2048
VALIDATION_POLICY = "fixture-oracle-v1"


def canonical_json(value) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=True, allow_nan=False,
                      separators=(",", ":"))


def _hash(value) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def _text(value, maximum=256):
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        return False
    try:
        value.encode("utf-8")
    except UnicodeError:
        return False
    return True


def _bounded_json(value):
    """Bound recursive values, including booleans distinct from integers."""
    count = 0

    def visit(item, depth):
        nonlocal count
        count += 1
        if count > MAX_NODES or depth > MAX_DEPTH:
            raise ValueError("JSON nesting or node count exceeds limit")
        if item is None or type(item) is bool:
            return
        if type(item) is int:
            if abs(item) > 2**63 - 1:
                raise ValueError("JSON integer exceeds limit")
            return
        if type(item) is float:
            if not math.isfinite(item) or abs(item) > 1e100:
                raise ValueError("JSON number exceeds limit")
            return
        if isinstance(item, str):
            if len(item) > 4096:
                raise ValueError("JSON string exceeds limit")
            try:
                item.encode("utf-8")
            except UnicodeError:
                raise ValueError("JSON string is not valid UTF-8") from None
            return
        if isinstance(item, list):
            if len(item) > 256:
                raise ValueError("JSON array exceeds limit")
            for child in item:
                visit(child, depth + 1)
            return
        if isinstance(item, dict):
            if len(item) > 128:
                raise ValueError("JSON object exceeds limit")
            for key, child in item.items():
                if not isinstance(key, str):
                    raise ValueError("JSON object keys must be strings")
                visit(key, depth + 1)
                visit(child, depth + 1)
            return
        raise ValueError("Value is not JSON data")

    visit(value, 0)
    if len(canonical_json(value).encode()) > MAX_VALUE_BYTES:
        raise ValueError("JSON value exceeds byte limit")


def case_id(case: Mapping) -> str:
    """ID binds the entire input, preserving input number types and key order independence."""
    if not isinstance(case, Mapping) or "input" not in case:
        raise ValueError("Case requires input")
    _bounded_json(case["input"])
    return _hash(case["input"])


def _pairs(pairs):
    value = {}
    for key, child in pairs:
        if key in value:
            raise ValueError("Duplicate JSON member")
        value[key] = child
    return value


def _constant(value):
    raise ValueError("Non-finite JSON number")


def _binding(case):
    return {key: case[key] for key in (
        "input", "expected", "requirement", "origin", "index", "proposal_sha256",
        "claimed_expected_sha256", "oracle_expected_sha256", "task_contract_sha256",
        "validation_policy",
    )}


def parse_proposals(content: str, task, origin: str) -> dict:
    """Return a JSON receipt for a strict batch of at most six proposed cases.

    ``task`` supplies ``id``, ``spec``, ``requirements`` and ``oracle(input)``.
    Invalid batches return an empty eligible list and a batch rejection. Invalid
    individual cases are retained as rejections; their siblings remain usable.
    Duplicate inputs within a batch are rejected after the first eligible case.
    Expectations must match the oracle with exact decoded JSON types; this
    function never silently repairs an expectation or coerces floats to ints.
    """
    if not _text(origin):
        raise ValueError("Proposal origin must be bounded nonempty text")
    requirements = tuple(task.requirements)
    if not requirements or any(not _text(label) for label in requirements):
        raise ValueError("Task requires known requirement labels")
    contract = _hash({"id": task.id, "spec": task.spec, "requirements": requirements})
    receipt = {"origin": origin, "task_id": task.id, "task_contract_sha256": contract,
               "validation_policy": VALIDATION_POLICY, "oracle_assisted": True,
               "content_sha256": None, "eligible": [], "rejected": [],
               "proposed_count": 0}

    def reject(index, reason, proposal=None):
        item = {"index": index, "reason": reason}
        if proposal is not None:
            try:
                item["proposal_sha256"] = _hash(proposal)
            except (ValueError, TypeError, RecursionError):
                pass
        receipt["rejected"].append(item)

    if not isinstance(content, str):
        reject(None, "batch_not_text")
        return receipt
    try:
        encoded = content.encode("utf-8")
    except UnicodeError:
        reject(None, "batch_invalid_utf8")
        return receipt
    receipt["content_sha256"] = hashlib.sha256(encoded).hexdigest()
    if len(encoded) > MAX_PROPOSAL_BYTES:
        reject(None, "batch_too_large")
        return receipt
    try:
        proposals = json.loads(content, object_pairs_hook=_pairs, parse_constant=_constant)
    except (ValueError, RecursionError):
        reject(None, "batch_invalid_json")
        return receipt
    if not isinstance(proposals, list):
        reject(None, "batch_not_list")
        return receipt
    receipt["proposed_count"] = len(proposals)
    if len(proposals) > MAX_PROPOSALS:
        reject(None, "batch_too_many_cases")
        return receipt
    seen = set()
    for index, proposal in enumerate(proposals):
        if not isinstance(proposal, dict) or set(proposal) != {"input", "expected", "requirement"}:
            reject(index, "case_schema", proposal)
            continue
        if not isinstance(proposal["requirement"], str) or proposal["requirement"] not in requirements:
            reject(index, "unsupported_requirement", proposal)
            continue
        try:
            _bounded_json(proposal["input"])
            _bounded_json(proposal["expected"])
        except (ValueError, RecursionError):
            reject(index, "case_exceeds_json_limits", proposal)
            continue
        identity = case_id(proposal)
        if identity in seen:
            reject(index, "duplicate_input", proposal)
            continue
        try:
            # Copy to keep an oracle that normalizes inputs from altering evidence.
            expected = task.oracle(json.loads(canonical_json(proposal["input"])))
            _bounded_json(expected)
        except Exception:
            # Oracle exceptions mean the proposal is outside this bounded fixture.
            # Do not expose arbitrary exception reprs in retained protocol data.
            reject(index, "oracle_rejected_input", proposal)
            continue
        if not json_equal(proposal["expected"], expected):
            reject(index, "wrong_expectation", proposal)
            continue
        case = {"input": proposal["input"], "expected": expected,
                "requirement": proposal["requirement"], "origin": origin, "index": index,
                "proposal_sha256": _hash(proposal),
                "claimed_expected_sha256": _hash(proposal["expected"]),
                "oracle_expected_sha256": _hash(expected), "task_contract_sha256": contract,
                "validation_policy": VALIDATION_POLICY}
        case["id"] = identity
        case["validation_sha256"] = _hash(_binding(case))
        receipt["eligible"].append(case)
        seen.add(identity)
    return receipt


def build_pool(*sources) -> list[dict]:
    """Union eligible proposal receipts and trusted fixture case lists by input.

    Fixture cases require input/expected/requirement and may include ``origin``.
    If absent, their provenance is ``trusted:<source ordinal>``. Generated-case
    validation hashes are rechecked. Raw proposals must pass parse_proposals;
    a plain list here is an explicit trusted-runner boundary, not a test oracle.
    Conflicting expectations for an identical input fail closed.
    """
    pooled = {}
    for source_index, source in enumerate(sources):
        is_receipt = isinstance(source, dict)
        if is_receipt:
            if "eligible" not in source or not isinstance(source["eligible"], list):
                raise ValueError("Source must be a proposal receipt or trusted case list")
            cases = source["eligible"]
        else:
            if not isinstance(source, (list, tuple)):
                raise ValueError("Trusted cases must be a list or tuple")
            cases = source
        for index, case in enumerate(cases):
            if not isinstance(case, dict) or not {"input", "expected", "requirement"} <= set(case):
                raise ValueError("Pool case does not match schema")
            _bounded_json(case["input"])
            _bounded_json(case["expected"])
            if not _text(case["requirement"]):
                raise ValueError("Pool case requirement must be bounded text")
            identity = case_id(case)
            origin = case.get("origin", f"trusted:{source_index}")
            if not _text(origin):
                raise ValueError("Pool case origin must be bounded text")
            if is_receipt or "validation_sha256" in case:
                try:
                    valid = (case["id"] == identity and case["validation_sha256"] == _hash(_binding(case))
                             and case["oracle_expected_sha256"] == _hash(case["expected"])
                             and case["validation_policy"] == VALIDATION_POLICY)
                    if is_receipt:
                        valid = valid and (source.get("oracle_assisted") is True
                                           and source.get("origin") == origin
                                           and source.get("task_contract_sha256") == case["task_contract_sha256"])
                except KeyError:
                    valid = False
                if not valid:
                    raise ValueError("Proposal validation binding mismatch")
            provenance = {"origin": origin, "index": case.get("index", index),
                          "proposal_sha256": case.get("proposal_sha256", _hash({
                              "input": case["input"], "expected": case["expected"],
                              "requirement": case["requirement"]})),
                          "validation_sha256": case.get("validation_sha256"),
                          "task_contract_sha256": case.get("task_contract_sha256"),
                          "validation_policy": case.get("validation_policy", "trusted-fixture")}
            if identity not in pooled:
                pooled[identity] = {"id": identity, "input": case["input"], "expected": case["expected"],
                                    "requirement": case["requirement"], "requirements": [],
                                    "origins": [], "provenance": []}
            item = pooled[identity]
            if not json_equal(item["expected"], case["expected"]):
                raise ValueError("Conflicting expected behavior for identical input")
            if case["requirement"] not in item["requirements"]:
                item["requirements"].append(case["requirement"])
            if origin not in item["origins"]:
                item["origins"].append(origin)
            if provenance not in item["provenance"]:
                item["provenance"].append(provenance)
    # Stable source order preserves the declared public prefix, but identities
    # and score weights do not depend on how often any input was proposed.
    return json.loads(canonical_json(list(pooled.values())))


def select_candidate(matrix: Mapping[str, Mapping[str, bool]], *, cases: list[dict],
                     public_case_ids: list[str], candidate_order: list[str] | None = None) -> dict:
    """Rank anonymous frozen candidates; abstain unless every public check passes.

    The runner builds ``matrix[candidate_id][case_id] = bool`` from sandbox
    outcomes. Missing outcomes fail. Extra case IDs (including hidden outcomes)
    and non-boolean values are rejected. ``candidate_order`` is the preregistered
    tie order; rotate it between repetitions. Defaults to sorted anonymous IDs.
    No code length, proposal count, claimed quality, or hidden result is used.
    """
    if not isinstance(matrix, Mapping) or not matrix:
        raise ValueError("Selection requires a frozen candidate roster")
    if any(not _text(candidate) for candidate in matrix):
        raise ValueError("Candidate IDs must be bounded text")
    order = sorted(matrix) if candidate_order is None else list(candidate_order)
    if len(order) != len(set(order)) or set(order) != set(matrix):
        raise ValueError("Tie order must contain exactly the frozen candidate roster")
    if not cases or not isinstance(cases, list):
        raise ValueError("Selection requires visible cases")
    identities = [case_id(case) for case in cases]
    if len(identities) != len(set(identities)):
        raise ValueError("Selection pool contains duplicate inputs")
    if any(case.get("id", identity) != identity for case, identity in zip(cases, identities)):
        raise ValueError("Pool input identity mismatch")
    public = set(public_case_ids)
    if not public or len(public) != len(public_case_ids) or not public <= set(identities):
        raise ValueError("Public case IDs must be a nonempty unique subset of the pool")
    index = {candidate: ordinal for ordinal, candidate in enumerate(order)}
    normalized = {}
    ranking = []
    for candidate, outcomes in matrix.items():
        if not isinstance(outcomes, Mapping) or not set(outcomes) <= set(identities):
            raise ValueError("Candidate outcomes contain unknown or held-out case IDs")
        if any(type(outcome) is not bool for outcome in outcomes.values()):
            raise ValueError("Candidate outcomes must be booleans")
        normalized[candidate] = {identity: outcomes.get(identity, False) for identity in identities}
        passed = sum(normalized[candidate].values())
        public_passed = sum(normalized[candidate][identity] for identity in public)
        eligible = public_passed == len(public)
        ranking.append({"candidate_id": candidate, "eligible": eligible,
                        "public_passed": public_passed, "public_total": len(public),
                        "passed": passed, "total": len(identities), "pass_fraction": passed / len(identities),
                        "missing_outcomes": len(identities) - len(outcomes), "tie_ordinal": index[candidate]})
    ranking.sort(key=lambda row: (not row["eligible"], -row["passed"], row["tie_ordinal"]))
    eligible = [row["candidate_id"] for row in ranking if row["eligible"]]
    return {"policy": "public-gated-unique-input-v1", "selected": eligible[0] if eligible else None,
            "abstained": not eligible, "eligible": eligible, "ranking": ranking,
            "candidate_order": order, "public_case_ids": sorted(public),
            "pool_sha256": _hash(cases), "matrix_sha256": _hash(normalized),
            "oracle_assisted": True, "hidden_outcomes_used": False}


def evaluate_selection(selection: dict, hidden_success: Mapping[str, bool]) -> dict:
    """Post-selection empirical availability and regret, using sealed verdicts.

    pass_at_k is the per-pool indicator that any candidate is correct, not an
    estimator extrapolated to a larger population. Regret is one exactly when a
    correct candidate existed but the policy did not select a correct candidate.
    """
    candidates = selection["candidate_order"]
    if set(hidden_success) != set(candidates) or any(type(v) is not bool for v in hidden_success.values()):
        raise ValueError("Sealed verdicts must exactly cover the frozen candidate roster")
    selected = selection["selected"]
    if selected is not None and selected not in hidden_success:
        raise ValueError("Selection is outside the frozen candidate roster")
    correct = [candidate for candidate in candidates if hidden_success[candidate]]
    selected_correct = selected is not None and hidden_success[selected]
    return {"candidate_count": len(candidates), "correct_candidates": correct,
            "pass_at_k": bool(correct), "selected_correct": selected_correct,
            "selector_regret": bool(correct) and not selected_correct,
            "abstained": selected is None, "selection_sha256": _hash(selection),
            "sealed_verdicts_sha256": _hash(dict(hidden_success)), "evaluation_only": True}
