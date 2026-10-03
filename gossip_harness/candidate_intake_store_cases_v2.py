"""Public B02 v2 definitions: exact v1 inheritance plus three JSON-wire probes.

No candidate implementation supplies an answer. Fixed scorer aliases reuse only
identical expected histories and call sequences; different fixture bytes, actual
case identities and source/recipe digests remain explicit in every new result.
"""
from __future__ import annotations

import base64
from copy import deepcopy
from functools import lru_cache
import hashlib
import json
from pathlib import Path
from typing import Any

from . import candidate_intake_store_cases_v1 as v1
from .candidate_intake_store_observer_v1 import Observation, ObservationUnavailable

PROTOCOL = "candidate-intake-store-cases-v2"
PRODUCT_SHA256 = v1.PRODUCT_SHA256
INVENTORY_SHA256 = v1.INVENTORY_SHA256
PHASES = v1.PHASES
TARGET_IDS = v1.TARGET_IDS
INHERITED_DEFINITION_SHA256 = "528ad3d27e8244fa4b9f1c5fa8789add0eb430c3cf0e84c284073e328588128d"
INHERITED_SOURCES = {
    "gossip_harness/candidate_intake_store_cases_v1.py": "291811e6c6714e068619c16acac6ad1d2877b949c4ef481e109163bb9324f0fc",
    "gossip_harness/candidate_intake_fixtures_v1.py": "ab93758a3e4cfb8404c1fdabc9ca65db22b74e624e6454357cbd4744f8c7f59f",
    "gossip_harness/candidate_intake_store_observer_v1.py": "1e873d61e404e57c83afe025f0a78126fb38cfc5fe4a65f4e61e72606bafb98e",
    "gossip_harness/candidate_storage_cases_v1.py": "42c92d98f88e178d2fc1ae6afe5013ab2271a85cfd22d5ef4d34f64eb8cd90a4",
}
EXTRA_ALIASES = (
    ("intake-json-large-whitespace-valid", "intake-json-literal"),
    ("intake-json-large-whitespace-syntax", "intake-json-syntax"),
    ("intake-json-large-whitespace-utf8", "intake-json-invalid-utf8"),
)
EXTRA_CASE_IDS = tuple(case_id for case_id, _ in EXTRA_ALIASES)


def encoded(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def _sha(value: Any) -> str:
    return hashlib.sha256(encoded(value)).hexdigest()


def definition_sources() -> dict[str, str]:
    sources = v1.definition_sources()
    sources["gossip_harness/candidate_intake_store_cases_v2.py"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    return sources


def _check_sources() -> None:
    if v1.definition_sources() != INHERITED_SOURCES or v1.definition_sha256() != INHERITED_DEFINITION_SHA256:
        raise RuntimeError("frozen B02 v1 definition dependency mismatch")
    if definition_sources() != _LOADED_DEFINITION_SOURCES:
        raise RuntimeError("loaded B02 v2 definition sources changed")


def _padded_json() -> bytes:
    # The original small fixture's decoded content/expected history is frozen.
    # Padding comprises only JSON whitespace (space, HT, CR and LF), in both
    # leading and trailing positions. The size is a finite representative, not a
    # newly declared product input limit.
    base = v1.case_definition("intake-json-literal")
    fixture = next(row for row in base["recipe"]["fixtures"] if row["path"] == "batch")
    raw = base64.b64decode(fixture["bytes_base64"], validate=True)
    whitespace = b" \t\r\n" * 786433
    return whitespace + raw + whitespace


def _extra_case(case_id: str, base_case_id: str) -> dict[str, Any]:
    base = v1.case_definition(base_case_id)
    case = deepcopy(base)
    raw = _padded_json()
    if case_id == "intake-json-large-whitespace-syntax":
        raw += b"!"
    elif case_id == "intake-json-large-whitespace-utf8":
        raw += b"\xff"
    elif case_id != "intake-json-large-whitespace-valid":
        raise ValueError("unknown fixed B02 v2 extension")
    fixtures = case["recipe"]["fixtures"]
    positions = [index for index, fixture in enumerate(fixtures) if fixture["path"] == "batch"]
    if len(positions) != 1:
        raise RuntimeError("fixed base fixture is not unique")
    fixtures[positions[0]] = {"path": "batch", "kind": "file", "bytes_base64": base64.b64encode(raw).decode("ascii")}
    case["case_id"] = case_id
    case["facet"] = {
        "intake-json-large-whitespace-valid": "Legal JSON whitespace beyond 6MiB does not change the small decoded bundle or its complete admission/commit/reopen history.",
        "intake-json-large-whitespace-syntax": "A non-JSON trailing token after legal padded content produces invalid_json without admission or storage mutation.",
        "intake-json-large-whitespace-utf8": "A raw invalid UTF-8 byte in padded JSON produces invalid_utf8 without admission or storage mutation.",
    }[case_id]
    for assertion in case["assertions"]:
        assertion["case_id"] = case_id
        assertion["scope"] = case["facet"]
    case["omissions"].append(
        "This finite >6MiB JSON-whitespace example declares no raw JSON size ceiling and is not a proof for arbitrary input lengths or parser streaming complexity.")
    case["scorer_alias"] = {
        "case_id": case_id,
        "base_case_id": base_case_id,
        "base_protocol": v1.PROTOCOL,
        "base_definition_sha256": INHERITED_DEFINITION_SHA256,
        "base_case_sha256": _sha(base),
        "base_recipe_sha256": _sha(base["recipe"]),
        "recipe_sha256": _sha(case["recipe"]),
        "expected_history_sha256": _sha(case["expected"]),
        "phase_recipe_sha256": _sha(case["recipe"]["phases"]),
        "fixture_path": "batch",
        "fixture_sha256": hashlib.sha256(raw).hexdigest(),
        "fixture_bytes": len(raw),
        "authority": "fixed-source-bound-expected-history-delegation-only",
    }
    _validate_alias(case, base)
    return case


def _validate_alias(case: dict[str, Any], base: dict[str, Any]) -> None:
    alias = case["scorer_alias"]
    fixed = dict(EXTRA_ALIASES)
    if fixed.get(case["case_id"]) != base["case_id"] or alias["case_id"] != case["case_id"] or alias["base_case_id"] != base["case_id"]:
        raise RuntimeError("B02 v2 alias identity mismatch")
    expected_bindings = {
        "base_protocol": v1.PROTOCOL,
        "base_definition_sha256": INHERITED_DEFINITION_SHA256,
        "base_case_sha256": _sha(base),
        "base_recipe_sha256": _sha(base["recipe"]),
        "recipe_sha256": _sha(case["recipe"]),
        "expected_history_sha256": _sha(case["expected"]),
        "phase_recipe_sha256": _sha(case["recipe"]["phases"]),
    }
    if any(alias.get(key) != value for key, value in expected_bindings.items()):
        raise RuntimeError("B02 v2 alias digest mismatch")
    if encoded(case["expected"]) != encoded(base["expected"]) or encoded(case["recipe"]["phases"]) != encoded(base["recipe"]["phases"]):
        raise RuntimeError("B02 v2 alias may not change its expected history or invocation sequence")
    actual_fixtures = {fixture["path"]: fixture for fixture in case["recipe"]["fixtures"]}
    base_fixtures = {fixture["path"]: fixture for fixture in base["recipe"]["fixtures"]}
    if len(actual_fixtures) != len(case["recipe"]["fixtures"]) or set(actual_fixtures) != set(base_fixtures):
        raise RuntimeError("B02 v2 alias fixture set mismatch")
    if any(encoded(actual_fixtures[path]) != encoded(fixture) for path, fixture in base_fixtures.items() if path != "batch"):
        raise RuntimeError("B02 v2 alias changed an undeclared fixture")
    fixture = actual_fixtures["batch"]
    raw = base64.b64decode(fixture["bytes_base64"], validate=True)
    if fixture.get("kind") != "file" or alias.get("fixture_path") != "batch" or alias.get("fixture_sha256") != hashlib.sha256(raw).hexdigest() or alias.get("fixture_bytes") != len(raw):
        raise RuntimeError("B02 v2 alias fixture binding mismatch")


@lru_cache(maxsize=1)
def _extra_bytes() -> tuple[bytes, ...]:
    return tuple(encoded(_extra_case(case_id, base_id)) for case_id, base_id in EXTRA_ALIASES)


def definitions() -> list[dict[str, Any]]:
    _check_sources()
    return v1.definitions() + [json.loads(raw) for raw in _extra_bytes()]


def case_definition(case_id: str) -> dict[str, Any]:
    _check_sources()
    if case_id in v1.CASE_IDS:
        return v1.case_definition(case_id)
    try:
        return json.loads(_extra_bytes()[EXTRA_CASE_IDS.index(case_id)])
    except ValueError as error:
        raise ValueError("unknown B02 v2 case") from error


def execution_recipe(case_id: str) -> dict[str, Any]:
    return case_definition(case_id)["recipe"]


def select_expected_case(case_id: str, results: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    _check_sources()
    if case_id in v1.CASE_IDS:
        return v1.select_expected_case(case_id, results)
    return case_definition(case_id)


@lru_cache(maxsize=1)
def _semantic_sha256() -> str:
    return _sha({"protocol": PROTOCOL, "product_sha256": PRODUCT_SHA256,
                 "inventory_sha256": INVENTORY_SHA256, "inherited_definition_sha256": INHERITED_DEFINITION_SHA256,
                 "inherited_source_sha256": INHERITED_SOURCES, "cases": definitions()})


def definition_sha256() -> str:
    _check_sources()
    return _semantic_sha256()


def evaluate_case(case_id: str, before: Observation, after: Observation, reopened: Observation,
                  results: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    """Delegate fixed equal histories while preserving actual recipe identity."""
    case = case_definition(case_id)
    alias = case.get("scorer_alias")
    base_case_id = case_id if alias is None else alias["base_case_id"]
    if alias is not None:
        _validate_alias(case, v1.case_definition(base_case_id))
    result = v1.evaluate_case(base_case_id, before, after, reopened, results)
    result["protocol"] = PROTOCOL
    result["case_id"] = case_id
    result["definition_sha256"] = definition_sha256()
    result["case_definition_sha256"] = _sha(case)
    result["recipe_sha256"] = _sha(case["recipe"])
    selected_case = select_expected_case(case_id, results)
    result["expected_history_sha256"] = _sha(selected_case["expected"])
    result["scorer_alias"] = deepcopy(alias)
    return result


_LOADED_DEFINITION_SOURCES = definition_sources()
CASE_IDS = tuple(v1.CASE_IDS) + EXTRA_CASE_IDS
