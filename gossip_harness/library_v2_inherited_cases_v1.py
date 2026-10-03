"""Prospectively frozen cumulative histories for product contract v2.

Only seven histories acquire an explicit process-only root-adoption step. The
frozen v1 cases and adapters are never edited. No reference implementation is
imported to derive an expectation. These are development regressions, not a
private acceptance bank, full scope coverage, or statistical study outcomes.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path
from typing import Any

from gossip_harness.blackbox_validator import CHILD_ADAPTER as M1_ADAPTER
from gossip_harness import library_m2_acceptance_cases_v1 as m2
from gossip_harness import library_m3_acceptance_cases_v1 as m3
from gossip_harness import library_m4_acceptance_cases_v3 as m4
from gossip_harness.library_project_fixture_v1 import public_cases

PROTOCOL = "library-v2-inherited-cases-v1"
CONTRACT_SHA256 = "2d88ce0775888f148b0ec3caf90b3d5c82d8fed71f53bec5f7e75f492ae998dc"
PURPOSE = "authored_reference_qualification"
PRIOR_CASES_SHA256 = {
    "m1": "97de89a67c0074290543230f5620a110b0d88dc0bbf546b31aafce1ca607093b",
    "m2": "91ea061bb949a4ce42fdd3e8a8d49e765b7348e602f251df0cb091c5ef20df9c",
    "m3": "6b9bbf052e8f23f6c99248eb1579d363682657432f53f8ca51b7595fcf10e78e",
    "m4": "87104867f4bfdf573a7ff05d6af09e03c0afe2f48270ca47ed8da285f77139d5",
}
PRIOR_SOURCE_SHA256 = {
    "library_project_fixture_v1.py": "fa4440f1ac7e63e3b4ef7d9978a8970e8d6cd688efe9c2e1b6d1e75c0f757caf",
    "library_m2_acceptance_cases_v1.py": "ae3bd3381caf6149b9e5136ce50aa37b9c4d80c4b8589ddfda7f1a77a628d09b",
    "library_m3_acceptance_cases_v1.py": "51a7d547b5683514dec7fd193436c8e1b55f0ccc71118cc1b22f499a91d7ef88",
    "library_m4_acceptance_cases_v1.py": "a87ac7fcabe683c4c8da8d54dbe0f102db3db02d0b5e6b3a78f6fe0bf5bd2832",
    "library_m4_acceptance_cases_v2.py": "aa735cb889b09f29d97f771830bdcaaf637060a97c333112ff950437148accba",
    "library_m4_acceptance_cases_v3.py": "5bda5c0bd03847d3ada13034343f89d109166b9cf4a4df027452be20ff546793",
}
# Explicit authority mapping fixed before any v2 reference execution. Positions
# are (action index, observation index), retaining unobserved setup actions.
ADOPTION_POSITIONS = {
    "m3-backup-preserves-tombstone-history-receipt": (0, 0),
    "m3-manual-backup-corrupt-graph-no-mutation": (0, 0),
    "m3-restore-removal-edit-and-job-aba": (0, 0),
    "m3-restore-deferred-invalid-manifest": (0, 0),
    "m3-cli-maintenance-arguments": (0, 0),
    "m4-frozen-m2-receipt-bytes-through-backup-restore": (1, 1),
    "m4-restore-reused-number-new-content-identity": (1, 0),
}
ADOPTION_EXPECTED = {"exit": 0, "value": {"adopted": True, "registered": 0},
                     "other_stream_empty": True}
LIMITATIONS = (
    "Eight M1, twelve M2, ten M3 and eighteen M4 finite histories are not all mandatory product obligations.",
    "Seven histories are adapted before execution to require process-only initial root adoption; they are not unchanged v1 reuse.",
    "All other inputs and observations remain inherited, including the narrowly typed M4 worker-generation domain.",
    "Unobserved six-amendment behavior needs a separate independent oracle; these histories do not invent liveness or stale-handle assertions.",
    "The adapters remain generic observers; no expected values, host scorer or outcome decisions enter candidate inputs.",
    "Frozen v1 cases were previously qualified and are known public regressions, not held-out candidate acceptance.",
)


def _sha(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=True,
        separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _exact(expected: Any, actual: Any) -> bool:
    if type(expected) is not type(actual):
        return False
    if type(expected) is dict:
        return expected.keys() == actual.keys() and all(_exact(v, actual[k]) for k, v in expected.items())
    if type(expected) is list:
        return len(expected) == len(actual) and all(_exact(a, b) for a, b in zip(expected, actual))
    return bool(expected == actual)


def _prior(milestone: str) -> list[dict[str, Any]]:
    if milestone not in PRIOR_CASES_SHA256:
        raise ValueError("Unknown inherited milestone")
    for name, expected in PRIOR_SOURCE_SHA256.items():
        if hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest() != expected:
            raise ValueError("Frozen inherited source changed: " + name)
    cases = {"m1": lambda: public_cases("m1"), "m2": m2.acceptance_cases,
             "m3": m3.acceptance_cases, "m4": m4.acceptance_cases}[milestone]()
    if _sha(cases) != PRIOR_CASES_SHA256[milestone]:
        raise ValueError("Frozen inherited definitions changed")
    return cases


def child_adapter(milestone: str) -> str:
    try:
        return {"m1": M1_ADAPTER, "m2": m2.CHILD_ADAPTER,
                "m3": m3.CHILD_ADAPTER, "m4": m4.CHILD_ADAPTER}[milestone]
    except KeyError as error:
        raise ValueError("Unknown inherited milestone") from error


def acceptance_cases(milestone: str) -> list[dict[str, Any]]:
    cases = _prior(milestone)
    for case in cases:
        position = ADOPTION_POSITIONS.get(case["id"])
        if position is None:
            continue
        action_index, observation_index = position
        if action_index == 1 and case["input"]["actions"][0].get("op") != "open":
            raise ValueError("Frozen initial Store opening changed")
        action = {"op": "cli", "args": ["backup-root-adopt", "--expect-unbound"], "observe": True}
        if milestone == "m3":
            action["json_data"] = True
        case["input"]["actions"].insert(action_index, action)
        case["expected"]["observations"].insert(observation_index, deepcopy(ADOPTION_EXPECTED))
    return cases


def score_case(milestone: str, case: dict[str, Any], actual: Any) -> bool:
    """Score exact derived definitions, preserving the inherited typed M4 domain."""
    fixed = next((c for c in acceptance_cases(milestone) if c["id"] == case.get("id")), None)
    if fixed is None or not _exact(fixed, case):
        return False
    if milestone != "m4":
        return _exact(case["expected"], actual)
    old = next(c for c in _prior(milestone) if c["id"] == case["id"])
    position = ADOPTION_POSITIONS.get(case["id"])
    if position is not None:
        observation_index = position[1]
        if (type(actual) is not dict or actual.keys() != {"observations"}
                or type(actual["observations"]) is not list
                or len(actual["observations"]) != len(case["expected"]["observations"])
                or not _exact(ADOPTION_EXPECTED, actual["observations"][observation_index])):
            return False
        actual = deepcopy(actual)
        del actual["observations"][observation_index]
    return m4.score_case(old, actual)


def registry_manifest(milestone: str) -> dict[str, Any]:
    cases = acceptance_cases(milestone)
    changes = [{"case_id": c["id"], "action_index": ADOPTION_POSITIONS[c["id"]][0],
                "observation_index": ADOPTION_POSITIONS[c["id"]][1],
                "authority": "library-cumulative-product-v2.json:V2-BACKUP-ROOT",
                "reason": "Root selection no longer implies adoption; explicitly adopt an empty unbound registry before backup I/O.",
                "unchanged": "Every prior operation and observation; adoption does not rotate incarnation or increment counters."}
               for c in cases if c["id"] in ADOPTION_POSITIONS]
    return {"protocol": PROTOCOL, "milestone": milestone, "purpose": PURPOSE,
            "status": "definition-not-execution", "contract_sha256": CONTRACT_SHA256,
            "prior_cases_sha256": PRIOR_CASES_SHA256[milestone],
            "prior_source_sha256": dict(PRIOR_SOURCE_SHA256),
            "case_ids": [c["id"] for c in cases], "ordered_cases_sha256": _sha(cases),
            "ordered_inputs_sha256": _sha([{"id": c["id"], "input": c["input"]} for c in cases]),
            "ordered_expected_sha256": _sha([{"id": c["id"], "expected": c["expected"]} for c in cases]),
            "adapter_sha256": hashlib.sha256(child_adapter(milestone).encode()).hexdigest(),
            "compatibility_changes": changes, "limitations": list(LIMITATIONS)}
