"""B02 v3: explicit partial qualification of three unspecified error-code cells.

Recipes and legacy expected histories are unchanged. The versioned policy grades
supported rejection/state facets while retaining the unsupported exact-code
comparison as a diagnostic. It never converts unknown presentation into a pass.
"""
from __future__ import annotations

from copy import deepcopy
from functools import lru_cache
import hashlib
import json
from pathlib import Path
from typing import Any

from . import candidate_intake_store_cases_v2 as v2
from .candidate_intake_store_observer_v1 import Observation

PROTOCOL = "candidate-intake-store-cases-v3"
PRODUCT_SHA256 = v2.PRODUCT_SHA256
INVENTORY_SHA256 = v2.INVENTORY_SHA256
PHASES = v2.PHASES
TARGET_IDS = v2.TARGET_IDS
CASE_IDS = v2.CASE_IDS
INHERITED_DEFINITION_SHA256 = "d582d6143939b56ef834d951799b305a893bd71578904b497c7c6f020f33db30"
INHERITED_SOURCES = {
    **v2.INHERITED_SOURCES,
    "gossip_harness/candidate_intake_store_cases_v2.py": "159ba708edb050a0fe9c715d70c297182874410bac1b6add70e98afb09ebe201",
}
NORMATIVE_SOURCES = {
    "gossip_harness/library_project_fixture_v1.py": "fa4440f1ac7e63e3b4ef7d9978a8970e8d6cd688efe9c2e1b6d1e75c0f757caf",
    "library-m1-acceptance-inventory-v1.json": "f5d808866d6f18f3fd095de5d447bb5b0e2a43e20771761fe91a38cf4f59ad4a",
    "library-cumulative-product-v1.json": "3353dbaa0c2474527cbffb0d87a4091b5f77ed3f98418fff2f216d90b6a8487c",
    "library-cumulative-product-v2.json": "2d88ce0775888f148b0ec3caf90b3d5c82d8fed71f53bec5f7e75f492ae998dc",
}
POLICY_LIMITED_CASE_IDS = ("intake-directory-wrong-kind", "intake-zip-wrong-kind", "intake-json-wrong-kind")
UNSPECIFIED_ASSERTION_IDS = ("after.result.0.exact-error-code",)
NATIVE_REJECTION_ASSERTION_ID = "after.result.0.native-error-rejection"
LEGACY_EXACT_ASSERTION_ID = "after.result.0"


def encoded(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _sha(value: Any) -> str:
    return hashlib.sha256(encoded(value)).hexdigest()


@lru_cache(maxsize=1)
def _policy_bytes() -> bytes:
    return encoded({
        "protocol": "b02-top-level-wrong-kind-partial-policy-v1",
        "case_ids": list(POLICY_LIMITED_CASE_IDS),
        "legacy_exact_check": LEGACY_EXACT_ASSERTION_ID,
        "supported_rejection_check": NATIVE_REJECTION_ASSERTION_ID,
        "unspecified_checks": list(UNSPECIFIED_ASSERTION_IDS),
        "scope": "normalized-native-error-frame-and-supported-state-facets",
        "supported_native_frame": "Exactly one error key containing a nonempty string; no observed-code allowlist.",
        "known_success_frame": "Exactly one value key containing a valid six-field JOB; empty jobs are successful JOB returns too.",
        "unqualified_presentations": "Other value envelopes, malformed frames, unexpected exceptions, unavailable and not-run markers cannot qualify native rejection; direct APIs are not required to use an invented exception class.",
        "exact_code": None,
        "exact_code_status": "unspecified",
        "exact_code_credit": False,
        "product_requirement_closure": False,
        "rules": [
            "Keep every invocation, fixture, case order and baseline expected history unchanged.",
            "Retain the old exact comparison as diagnostic data even when it happens to match.",
            "Observe the actual code without inferring the required code from it.",
            "Preserve every independent result/state/manifest/auxiliary check and earlier failure.",
            "Separate unspecified exact-code nulls from unavailable native observations.",
            "Keep all_local_assertions_passed false; supported_assertions_passed measures only the supported conjunction.",
            "Do not relabel any historical execution, supported error representation or unknown cell as an ordinary pass.",
        ],
        "citations": [
            {"path": "gossip_harness/library_project_fixture_v1.py", "lines": [97, 111], "scope": "Supported discovery and bounded input classes."},
            {"path": "gossip_harness/library_project_fixture_v1.py", "lines": [181, 198], "scope": "Direct intake, no-admission, error categories; no exact top-level wrong-kind classification or universal native exception class."},
            {"path": "library-m1-acceptance-inventory-v1.json", "requirement_ids": ["M1-I01", "M1-I26", "M1-I28"], "scope": "Coverage-gap examples cannot add a normative exact code."},
            {"path": "library-cumulative-product-v1.json", "lines": [1329, 1329], "scope": "No evaluator invention."},
            {"path": "library-cumulative-product-v2.json", "lines": [1368, 1368], "scope": "Reference behavior/evaluator convenience cannot silently amend the contract."},
        ],
        "normative_source_sha256": NORMATIVE_SOURCES,
    })


def evaluation_policy() -> dict[str, Any]:
    _check_sources()
    return json.loads(_policy_bytes())


def evaluation_policy_sha256() -> str:
    _check_sources()
    return hashlib.sha256(_policy_bytes()).hexdigest()


def definition_sources() -> dict[str, str]:
    root = Path(__file__).resolve().parents[1]
    sources = v2.definition_sources()
    for path in NORMATIVE_SOURCES:
        sources[path] = hashlib.sha256((root / path).read_bytes()).hexdigest()
    sources["gossip_harness/candidate_intake_store_cases_v3.py"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    return sources


def _check_sources() -> None:
    if v2.definition_sources() != INHERITED_SOURCES or v2.definition_sha256() != INHERITED_DEFINITION_SHA256:
        raise RuntimeError("frozen B02 v2 definition dependency mismatch")
    current = definition_sources()
    if any(current[path] != expected for path, expected in NORMATIVE_SOURCES.items()):
        raise RuntimeError("frozen B02 normative source mismatch")
    if current != _LOADED_DEFINITION_SOURCES:
        raise RuntimeError("loaded B02 v3 source mismatch")


def _with_disposition(base: dict[str, Any]) -> dict[str, Any]:
    if base["case_id"] not in POLICY_LIMITED_CASE_IDS:
        return base
    row = deepcopy(base)
    row["evaluation_disposition"] = {
        "evaluation_policy_sha256": hashlib.sha256(_policy_bytes()).hexdigest(),
        "baseline_protocol": v2.PROTOCOL,
        "baseline_definition_sha256": INHERITED_DEFINITION_SHA256,
        "baseline_case_sha256": _sha(base),
        "baseline_recipe_sha256": _sha(base["recipe"]),
        "baseline_expected_history_sha256": _sha(base["expected"]),
        "expected_history_role": "Unmodified v2 baseline; exact after.result.0 code is diagnostic, overridden only by this explicit v3 policy.",
        "supported_assertion_ids": [NATIVE_REJECTION_ASSERTION_ID],
        "unspecified_assertion_ids": list(UNSPECIFIED_ASSERTION_IDS),
        "partial_qualification": True,
        "exact_code_credit": False,
    }
    for assertion in row["assertions"]:
        if assertion["check_id"] == LEGACY_EXACT_ASSERTION_ID:
            if assertion["requirement_id"] == "M1-I28":
                assertion["check_id"] = UNSPECIFIED_ASSERTION_IDS[0]
                assertion["qualification"] = "unspecified-no-exact-code-credit"
            else:
                assertion["check_id"] = NATIVE_REJECTION_ASSERTION_ID
                assertion["qualification"] = "normalizer-qualified-supported-rejection-only"
            assertion["scope"] = "Top-level wrong-kind input: supported rejection with no admission and state conservation; exact code remains unspecified."
    row["omissions"].append("V3 does not award M1-I28 exact-code credit for this wrong-kind input, even when the observed code equals the historical io_error expectation.")
    return row


def definitions() -> list[dict[str, Any]]:
    _check_sources()
    return [_with_disposition(row) for row in v2.definitions()]


def case_definition(case_id: str) -> dict[str, Any]:
    _check_sources()
    return _with_disposition(v2.case_definition(case_id))


def execution_recipe(case_id: str) -> dict[str, Any]:
    return case_definition(case_id)["recipe"]


def select_expected_case(case_id: str, results: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    _check_sources()
    return _with_disposition(v2.select_expected_case(case_id, results))


@lru_cache(maxsize=1)
def _semantic_sha256() -> str:
    return _sha({"protocol": PROTOCOL, "product_sha256": PRODUCT_SHA256, "inventory_sha256": INVENTORY_SHA256,
                 "inherited_definition_sha256": INHERITED_DEFINITION_SHA256,
                 "inherited_source_sha256": INHERITED_SOURCES, "evaluation_policy": json.loads(_policy_bytes()),
                 "cases": definitions()})


def definition_sha256() -> str:
    _check_sources()
    return _semantic_sha256()


def evaluation_contract_sha256(case_id: str, results: dict[str, list[dict[str, Any]]]) -> str:
    case = case_definition(case_id)
    selected = select_expected_case(case_id, results)
    return _sha({"protocol": PROTOCOL, "definition_sha256": definition_sha256(), "case_id": case_id,
                 "case_definition_sha256": _sha(case), "recipe_sha256": _sha(case["recipe"]),
                 "selected_baseline_expected_history_sha256": _sha(selected["expected"]),
                 "evaluation_policy_sha256": evaluation_policy_sha256(),
                 "policy_applies": case_id in POLICY_LIMITED_CASE_IDS})


def _is_job(value: Any) -> bool:
    if type(value) is not dict or set(value) != {"job_id", "epoch", "state", "total", "completed", "error"}:
        return False
    jid = value["job_id"]
    if type(jid) is not str or not 1 <= len(jid) <= 64 or any(ch not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-" for ch in jid):
        return False
    if type(value["epoch"]) is not int or not 1 <= value["epoch"] <= 2**63 - 1:
        return False
    if type(value["total"]) is not int or value["total"] < 0 or type(value["completed"]) is not int:
        return False
    state = value["state"]
    if type(state) is not str or state not in ("queued", "running", "completed", "cancelled", "failed"):
        return False
    if value["completed"] != (value["total"] if state == "completed" else 0):
        return False
    return (type(value["error"]) is str and bool(value["error"])) if state == "failed" else value["error"] is None


def _native_rejection(frame: Any, dependent: bool) -> tuple[bool | None, str, str | None]:
    if dependent:
        return None, "dependency-unavailable", None
    if type(frame) is dict and set(frame) == {"error"} and type(frame["error"]) is str and frame["error"]:
        return True, "native-error", frame["error"]
    if type(frame) is dict and set(frame) == {"value"} and _is_job(frame["value"]):
        return False, "job-success", None
    return None, "unqualified", None


def evaluate_case(case_id: str, before: Observation, after: Observation, reopened: Observation,
                  results: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    case = case_definition(case_id)
    result = v2.evaluate_case(case_id, before, after, reopened, results)
    result["protocol"] = PROTOCOL
    result["definition_sha256"] = definition_sha256()
    result["case_definition_sha256"] = _sha(case)
    result["baseline_expected_history_sha256"] = result["expected_history_sha256"]
    result["evaluation_policy_sha256"] = evaluation_policy_sha256()
    result["evaluation_contract_sha256"] = evaluation_contract_sha256(case_id, results)
    result["partial_qualification"] = case_id in POLICY_LIMITED_CASE_IDS
    result["unspecified_assertions"] = []
    result["supported_assertions_passed"] = result["all_local_assertions_passed"]
    if case_id not in POLICY_LIMITED_CASE_IDS:
        return result
    checks = result["checks"]
    comparison = checks.pop(LEGACY_EXACT_ASSERTION_ID)
    frame = deepcopy(results["after"][0]) if results["after"] else None
    rejection, status, code = _native_rejection(frame, comparison is None)
    checks[NATIVE_REJECTION_ASSERTION_ID] = rejection
    checks[UNSPECIFIED_ASSERTION_IDS[0]] = None
    result["legacy_exact_error_diagnostic"] = {
        "protocol": v2.PROTOCOL, "definition_sha256": INHERITED_DEFINITION_SHA256,
        "check_id": LEGACY_EXACT_ASSERTION_ID, "matched": comparison,
        "expected_frame": deepcopy(case["expected"]["results"]["after"][0]),
        "observed_frame": frame, "qualification_credit": False,
    }
    # Preserve existing unavailable entries except the superseded exact-result
    # check, which now has distinct supported and explicitly unspecified cells.
    result["observation_unavailable"] = [row for row in result["observation_unavailable"]
                                         if row["check_id"] != LEGACY_EXACT_ASSERTION_ID]
    if rejection is None:
        result["observation_unavailable"].append({"check_id": NATIVE_REJECTION_ASSERTION_ID,
            "reason": "dependent-observation-unavailable" if comparison is None else "normalized-native-error-frame-unqualified"})
    result["unspecified_assertions"] = list(UNSPECIFIED_ASSERTION_IDS)
    result["supported_domain_rejection"] = rejection
    result["native_error_frame_qualified"] = status == "native-error"
    result["normalizer_frame_status"] = status
    result["observed_error_code"] = code
    result["observed_error_frame"] = frame
    result["exact_error_status"] = "unspecified"
    result["exact_error_code"] = None
    result["exact_code_credit"] = False
    result["qualification_scope"] = "normalized-native-error-frame-and-supported-state-facets"
    result["supported_assertions_passed"] = all(value is True for key, value in checks.items()
                                                  if key not in UNSPECIFIED_ASSERTION_IDS)
    result["all_local_assertions_passed"] = False
    result["local_outcome"] = ("partial-supported" if result["supported_assertions_passed"] else
        "supported-failed" if any(value is False for value in checks.values()) else "observation-unavailable")
    return result


_LOADED_DEFINITION_SOURCES = definition_sources()
