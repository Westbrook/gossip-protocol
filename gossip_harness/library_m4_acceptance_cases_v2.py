"""Versioned correction of two v1 observation assumptions, with host-only scoring.

Frozen v1 definitions and failed execution evidence remain unchanged. V2 was
calibrated after v1 execution exposed the assumptions; it is not held-out data.
No M4 implementation or implementation tests were inspected for this correction.
"""
from __future__ import annotations

import hashlib
import inspect
import json
from typing import Any

from gossip_harness import library_m4_acceptance_cases_v1 as prior

PROTOCOL = "library-m4-independent-cases-v2"
PURPOSE = prior.PURPOSE
CONTRACT_SHA256 = prior.CONTRACT_SHA256
REQUIREMENT_IDS = prior.REQUIREMENT_IDS
SCORER_PROTOCOL = "library-m4-host-observation-scorer-v2"
PRIOR_SOURCE_SHA256 = "a87ac7fcabe683c4c8da8d54dbe0f102db3db02d0b5e6b3a78f6fe0bf5bd2832"
PRIOR_CASES_SHA256 = "bc967ec65cce876366ebb66c15caea1d42bd2400ea06aabed6823f4f0823d11f"
DOMAIN_CASE_ID = "m4-frozen-m2-receipt-bytes-through-backup-restore"
DOMAIN_OBSERVATION = 34
DOMAIN_FIELD = "worker_generation"
MINIMUM_MARKER = "$integer_at_least"
LIMITATIONS = (*prior.LIMITATIONS,
    "V2 was corrected after v1 observations revealed two invalid observer assumptions; no held-out independence is claimed.",
    "The only non-exact value is post-restore worker_generation: a typed integer >= its known initial target value 0.",
    "This zero-baseline history does not independently demonstrate retention of a positive pre-restore worker fence.",
    "Host scoring covers observations only; caller must reject infrastructure/error outcomes before consulting score_case.",
)
CORRECTIONS = (
    {"id": "legacy-export-json-representation", "scope": "child Service.export bytes only",
     "reason": "The inherited public export contract specifies the JSON value; the observer must decode a serialized helper result before comparing that value.",
     "authority": ["gossip_harness/library_project_fixture_v1.py:V0_SPEC export and CLI/HTTP paragraphs",
                   "library-cumulative-product-v1.json:requirements[M4-COMPATIBILITY].clauses[0]"],
     "unchanged": "Legacy CLI export, route outputs, versioned exports and all parsed legacy fields remain exact."},
    {"id": "post-restore-worker-generation-domain", "scope": DOMAIN_CASE_ID + ":observations[34].worker_generation",
     "reason": "Fresh controls default to 0, but restore may advance the durable target worker fence. No exact post-restore increment is prescribed.",
     "authority": ["library-cumulative-product-v1.json:value_types.DIAGNOSTICS3.worker_generation",
                   "library-cumulative-product-v1.json:persistence.schema3[0]",
                   "library-cumulative-product-v1.json:persistence.fencing",
                   "library-cumulative-product-v1.json:requirements[M3-BACKUP-RESTORE].clauses[3:5]"],
     "unchanged": "Initial migration generation remains exactly 0; all other post-restore fields remain exact."},
)


def _bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def _sha(value: Any) -> str:
    return hashlib.sha256(_bytes(value)).hexdigest()


def _adapter() -> str:
    old = '        return function(*action["args"], **action["kwargs"])'
    new = '''        result = function(*action["args"], **action["kwargs"])
        if action["target"] == "service" and action["method"] == "export" and type(result) is bytes:
            result = strict_json(result)
        return result'''
    if prior.CHILD_ADAPTER.count(old) != 1:
        raise ValueError("Frozen v1 adapter seam changed")
    return prior.CHILD_ADAPTER.replace(old, new)


CHILD_ADAPTER = _adapter()


def acceptance_cases() -> list[dict[str, Any]]:
    """Keep every v1 input and exact expectation except the declared domain leaf."""
    cases = prior.acceptance_cases()
    if _sha(cases) != PRIOR_CASES_SHA256:
        raise ValueError("Frozen v1 definitions changed")
    case = next(case for case in cases if case["id"] == DOMAIN_CASE_ID)
    value = case["expected"]["observations"][DOMAIN_OBSERVATION]
    if value[DOMAIN_FIELD] != 0:
        raise ValueError("Frozen v1 post-restore observation changed")
    value[DOMAIN_FIELD] = {MINIMUM_MARKER: 0}
    return cases


def _exact_json(expected: Any, actual: Any) -> bool:
    """Exact JSON shapes and scalar types; booleans never equal integer tokens."""
    if type(expected) is not type(actual):
        return False
    if type(expected) is dict:
        return expected.keys() == actual.keys() and all(_exact_json(value, actual[key])
                                                      for key, value in expected.items())
    if type(expected) is list:
        return len(expected) == len(actual) and all(_exact_json(left, right)
                                                   for left, right in zip(expected, actual))
    return bool(expected == actual)


def score_case(case: dict[str, Any], actual: Any) -> bool:
    """Host-only observation comparison; never converts infrastructure failure to acceptance.

    Only the explicitly bound case, index, key and marker admit a domain value.
    Other marker-looking objects remain ordinary exact JSON. Execution status,
    source authentication and resource limits are independently owned by callers.
    """
    expected = case["expected"]
    if case["id"] != DOMAIN_CASE_ID:
        return _exact_json(expected, actual)
    if type(actual) is not dict or actual.keys() != expected.keys():
        return False
    observations = actual.get("observations")
    expected_observations = expected.get("observations")
    if type(observations) is not list or type(expected_observations) is not list or len(observations) != len(expected_observations):
        return False
    for index, (wanted, value) in enumerate(zip(expected_observations, observations)):
        if index != DOMAIN_OBSERVATION:
            if not _exact_json(wanted, value):
                return False
            continue
        if type(value) is not dict or type(wanted) is not dict or value.keys() != wanted.keys():
            return False
        if wanted.get(DOMAIN_FIELD) != {MINIMUM_MARKER: 0}:
            return False
        generation = value[DOMAIN_FIELD]
        if type(generation) is not int or generation < 0:
            return False
        if not all(_exact_json(item, value[key]) for key, item in wanted.items() if key != DOMAIN_FIELD):
            return False
    return True


def registry_manifest() -> dict[str, Any]:
    cases = acceptance_cases()
    scorer_source = inspect.getsource(_exact_json) + inspect.getsource(score_case)
    return {"protocol": PROTOCOL, "purpose": PURPOSE, "status": "unqualified_definition",
            "contract_sha256": CONTRACT_SHA256,
            "public_fixture_files_sha256": prior.registry_manifest()["public_fixture_files_sha256"],
            "prior_protocol": prior.PROTOCOL, "prior_source_sha256": PRIOR_SOURCE_SHA256,
            "prior_cases_sha256": PRIOR_CASES_SHA256,
            "case_ids": [case["id"] for case in cases],
            "ordered_inputs_sha256": _sha([{"id": case["id"], "input": case["input"]} for case in cases]),
            "ordered_expected_sha256": _sha([{"id": case["id"], "expected": case["expected"]} for case in cases]),
            "ordered_cases_sha256": _sha(cases), "adapter_sha256": hashlib.sha256(CHILD_ADAPTER.encode()).hexdigest(),
            "scorer_protocol": SCORER_PROTOCOL, "scorer_source": scorer_source,
            "scorer_sha256": hashlib.sha256(scorer_source.encode()).hexdigest(),
            "requirement_cases": {key: [case["id"] for case in cases if key in case["requirement_ids"]]
                                  for key in REQUIREMENT_IDS},
            "uncovered_requirements": ["M4-RELEASE-HANDOFF"], "limitations": list(LIMITATIONS),
            "corrections": list(CORRECTIONS)}
