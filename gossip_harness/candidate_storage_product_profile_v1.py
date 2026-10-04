"""Prospective inherited B01/B02 assertion slice for the exact V2/M4 product.

This module declares comparisons, not execution, layout review or acceptance
authority. Only a separately authenticated owner may supply original captures
and responses to the observers and then call ``project``. Candidate-dependent
native check dictionaries never define the prospective assertion census.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from functools import lru_cache
import hashlib
import json
from pathlib import Path
from typing import Any

from . import candidate_storage_cases_v1 as b01
from . import candidate_storage_observer_v1 as storage_observer
from . import candidate_intake_store_cases_v1 as legacy_b02
from . import candidate_intake_store_cases_v3 as b02
from . import candidate_intake_store_observer_v1 as intake_observer
from . import candidate_observation_admission_v1 as admission
from . import project_acceptance_registry_v1 as registry
from . import candidate_http_journal_v3 as journal_json

PROTOCOL = "candidate-storage-product-profile-v1-ascii-json-v1"
JSON_ENCODING_PROTOCOL = "storage-canonical-ascii-json-v1"
ROOT = Path(__file__).resolve().parents[1]
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
ORIGINAL_CONTRACT = "library-m1-acceptance-inventory-v1.json"
ORIGINAL_CONTRACT_SHA256 = "f5d808866d6f18f3fd095de5d447bb5b0e2a43e20771761fe91a38cf4f59ad4a"
TARGET_CONTRACT = "library-cumulative-product-v2.json"
TARGET_CONTRACT_SHA256 = b01.PRODUCT_SHA256
ORIGINAL_DEFINITION_PURPOSE = "harness_qualification"
PHASES = ("before", "after", "reopened")
REMAINING_COVERAGE = (
    "Independent source/layout, semantic facet, purpose and actual execution authority review",
    "All 123 product requirements, three prerequisites, 312 source units and 22 authorities remain mandatory",
    "All four milestones and six trajectories remain mandatory; this is only a finite inherited storage slice",
    "M4 schema4, catalog generation, revision and job-control transitions need independently reviewed auxiliary expectations",
    "V2 counter exhaustion, incarnation fences, exact hash serialization, migration diagnostics, worker liveness and backup-root semantics",
    "Actual concurrency, forced schedules unavailable on a candidate, process death and recovery",
    "Branch-specific stored-string conservation beyond the common declared alternative-history selectors",
    "Full scope registration, controller barriers, qualification and fresh independent acceptance",
)
_MODULES = (
    "candidate_storage_cases_v1.py", "candidate_storage_observer_v1.py", "candidate_storage_driver_v1.py",
    "candidate_intake_store_cases_v1.py", "candidate_intake_store_cases_v2.py", "candidate_intake_store_cases_v3.py",
    "candidate_intake_fixtures_v1.py", "candidate_intake_store_observer_v1.py",
    "candidate_intake_store_driver_v1.py", "candidate_intake_store_driver_v2.py", "candidate_intake_store_driver_v3.py",
    "candidate_intake_store_profile_v1.py", "candidate_observation_admission_v1.py",
    "project_acceptance_registry_v1.py", "candidate_storage_product_profile_v1.py", "candidate_http_journal_v3.py",
)


class ProfileError(ValueError):
    """Invalid prospective profile/input; never a product judgment."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ProfileError(message)


def encoded(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(",", ":"), allow_nan=False).encode()



def decode(raw: bytes) -> Any:
    """Bounded product-profile JSON; escaped surrogate test data remains data.

    Reuse the frozen lexical/size/depth/node and duplicate/nonfinite guards. The
    journal's Unicode-scalar restriction intentionally does not apply to these
    authored invalid-input payloads. No global journal/parser policy is changed.
    """
    require(type(raw) is bytes and len(raw) <= journal_json.MAX_RECORD_BYTES,
            "Storage JSON requires bounded immutable bytes")
    try:
        text = raw.decode("utf-8", errors="strict")
        journal_json._budget(text)
        return json.loads(text, object_pairs_hook=journal_json._object,
                          parse_constant=journal_json._constant, parse_float=journal_json._float)
    except (ValueError, UnicodeError, RecursionError, OverflowError) as error:
        raise ProfileError("Invalid bounded storage JSON") from error


def digest(value: Any) -> str:
    return hashlib.sha256(encoded(value)).hexdigest()


def source_sha256(files: dict[str, bytes]) -> str:
    """Common C06 blob identity, never source admission or layout authority."""
    return admission.source_sha256(files)


def definition_sources() -> dict[str, str]:
    sources = b02.definition_sources()
    for name in _MODULES:
        path = "gossip_harness/" + name
        sources[path] = hashlib.sha256((ROOT / path).read_bytes()).hexdigest()
    require(sources[ORIGINAL_CONTRACT] == ORIGINAL_CONTRACT_SHA256
            and sources[TARGET_CONTRACT] == TARGET_CONTRACT_SHA256, "Normative contract differs")
    require(sources["gossip_harness/candidate_storage_product_profile_v1.py"] == LOADED_SOURCE_SHA256,
            "Loaded storage product profile changed; use a fresh worker")
    return dict(sorted(sources.items()))


def _check_sources() -> None:
    require(definition_sources() == _LOADED_SOURCES, "Loaded profile dependency changed")
    # These accessors also check the frozen imported v1/v2 dependencies.
    b02.evaluation_policy_sha256()
    admission.verify_loaded_sources(_LOADED_SOURCES)


def _pointer(value: str) -> str:
    return value.replace("~", "~0").replace("/", "~1")


def _jobs(expected: dict[str, Any], phase: str) -> dict[str, Any]:
    return {row["public"]["job_id"]: row for row in expected[phase]["jobs"]}


def _b02_ids(case: dict[str, Any], expected: dict[str, Any]) -> tuple[str, ...]:
    """Mirror the authored scorer's order using expected data only."""
    ids = ["admission-variant"] if "expected_alternatives" in case else []
    for phase in PHASES:
        ids.extend((phase + ".persisted-state", phase + ".result-count"))
        ids.extend(f"{phase}.result.{i}" for i in range(len(expected["results"][phase])))
    ids.extend(phase + ".persisted-string-job-set" for phase in PHASES)
    for jid, original in _jobs(expected, "before").items():
        for phase in ("after", "reopened"):
            ids.extend(phase + "." + jid + ".immutable-" + field for field in ("manifest", "content_hashes"))
            other = _jobs(expected, phase).get(jid)
            if other is not None and encoded(original["receipt"]) == encoded(other["receipt"]):
                ids.append(phase + "." + jid + ".receipt-conservation")
    ids.append("reopened.persisted-strings")
    if encoded(expected["before"]) == encoded(expected["after"]):
        ids.append("after.auxiliary-conservation")
    ids.append("reopened.auxiliary-conservation")
    if case["case_id"] in b02.POLICY_LIMITED_CASE_IDS:
        ids.remove(b02.LEGACY_EXACT_ASSERTION_ID)
        ids.extend((b02.NATIVE_REJECTION_ASSERTION_ID, *b02.UNSPECIFIED_ASSERTION_IDS))
    return tuple(ids)


def _census(family: str, case: dict[str, Any]) -> tuple[tuple[str, ...], set[str]]:
    if family == "b01":
        ids = [phase + ".persisted-state" for phase in PHASES] + ["result"]
        for row in case["before"]["jobs"]:
            jid = row["public"]["job_id"]
            ids.extend(jid + ".immutable-" + field for field in ("manifest", "content_hashes"))
            if case["case_id"] != "empty-batch":
                ids.append(jid + ".receipt-conservation")
        ids.extend(("reopened.persisted-strings", "after.auxiliary-conservation", "reopened.auxiliary-conservation",
                    "after.auxiliary-file-conservation", "reopened.auxiliary-file-conservation"))
        return tuple(ids), set(ids)
    variants = [case["expected"], *case.get("expected_alternatives", {}).values()]
    rows = [_b02_ids(case, expected) for expected in variants]
    return tuple(dict.fromkeys(key for row in rows for key in row)), set.intersection(*(set(row) for row in rows))


def _dependencies(family: str, check: str, case: dict[str, Any]) -> list[dict[str, str]]:
    phases: tuple[str, ...]
    if check == "admission-variant":
        item = case["branch_discriminator"]
        return [{"path": item["phase"] + "-response.json", "pointer": "/value/" + str(item["result_index"])}]
    if check == "result":
        return [{"path": "after-response.json", "pointer": "/value"}]
    phase = check.split(".")[0]
    if ".result" in check:
        suffix = check.split(".")
        pointer = "/value/" + suffix[2] if len(suffix) >= 3 else "/value"
        return [{"path": phase + "-response.json", "pointer": pointer}]
    if check == "reopened.persisted-strings":
        phases, projection = ("after", "reopened"), "/persisted_strings"
    elif ".immutable-" in check or check.endswith(".receipt-conservation"):
        phases = PHASES if family == "b01" else ("before", phase)
        projection = "/persisted_strings"
    elif "auxiliary" in check:
        phases = ("before", "after") if phase == "after" else ("after", "reopened")
        projection = "/files" if "file" in check else "/auxiliary_tables"
    else:
        phases, projection = (phase,), "/persisted_strings" if "string-job-set" in check else "/data"
    return [{"path": item + "-capture-stdout.bin", "pointer": "", "observer_projection": projection}
            for item in phases]


@dataclass(frozen=True, slots=True)
class StorageProductProfile:
    family: str
    case_id: str
    purpose: str
    _record_bytes: bytes

    def record(self) -> dict[str, Any]:
        return json.loads(self._record_bytes)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self._record_bytes).hexdigest()

    @property
    def ordered_case_ids(self) -> tuple[str, ...]:
        return self.decisive_ids

    @property
    def native_history_ids(self) -> tuple[str, ...]:
        return tuple(self.record()["native_history_ids"])

    @property
    def diagnostic_ids(self) -> tuple[str, ...]:
        return tuple(row["check_id"] for row in self.record()["diagnostics"])

    @property
    def decisive_ids(self) -> tuple[str, ...]:
        return tuple(row["check_id"] for row in self.record()["diagnostics"] if row["applicability"] == "normative")

    @property
    def requirement_ids(self) -> tuple[str, ...]:
        return tuple(self.record()["requirement_ids"])


@lru_cache(maxsize=768)
def _profile_bytes(family: str, case_id: str, purpose: str) -> bytes:
    if family == "b01":
        case = next(row for row in b01.definitions() if row["case_id"] == case_id)
        native_protocol, native_definition = b01.PROTOCOL, b01.definition_sha256()
        evaluator = "gossip_harness/candidate_storage_observer_v1.py"
    else:
        case = b02.case_definition(case_id)
        native_protocol, native_definition = b02.PROTOCOL, b02.definition_sha256()
        evaluator = "gossip_harness/candidate_intake_store_cases_v3.py"
    ids, common = _census(family, case)
    cells = []
    for check in ids:
        applicability, reason = "normative", "Inherited finite result/state or stored-string facet; independent applicability review remains required"
        if case_id in b02.POLICY_LIMITED_CASE_IDS and check in b02.UNSPECIFIED_ASSERTION_IDS:
            applicability, reason = "unspecified", "Exact wrong-kind error code is explicitly unspecified by the original v3 policy"
        elif "auxiliary" in check:
            applicability, reason = "unqualified", "M4 auxiliary transitions require independent source/layout expectations; legacy conservation is diagnostic only"
        elif check not in common:
            applicability, reason = "unqualified", "Branch-specific native selector is not common to every prospectively declared admission history"
        assertions = [deepcopy(row) for row in case.get("assertions", []) if row["check_id"] == check]
        cells.append({"check_id": check, "selector": "/checks/" + _pointer(check),
                      "applicability": applicability, "reason": reason,
                      "raw_dependencies": _dependencies(family, check, case),
                      "evaluation_response_dependencies": ([{"path": phase + "-response.json", "pointer": "/value",
                          "role": "complete response census, declared branch selection and unavailable-boundary masking"}
                          for phase in PHASES] if family == "b02" else []),
                      "original_assertions": assertions,
                      "source_unit_facets": [{"source_unit_id": "m1:" + row["requirement_id"],
                          "source_sha256": ORIGINAL_CONTRACT_SHA256, "scope": row["scope"],
                          "class": row["class"], "lane": row["lane"], "full_source_unit": False}
                          for row in assertions if applicability == "normative"],
                      "semantic_review_required": True})
    contract = json.loads((ROOT / TARGET_CONTRACT).read_bytes())
    return encoded({"protocol": PROTOCOL, "canonical_json_protocol": JSON_ENCODING_PROTOCOL, "family": family, "case_id": case_id,
        "execution_purpose": purpose, "original_definition_purpose": ORIGINAL_DEFINITION_PURPOSE,
        "original_milestone": "M1", "target_milestone": "M4",
        "original_contract": ORIGINAL_CONTRACT, "original_contract_sha256": ORIGINAL_CONTRACT_SHA256,
        "target_contract": TARGET_CONTRACT, "target_contract_sha256": TARGET_CONTRACT_SHA256,
        "ordered_case_ids": [row["check_id"] for row in cells if row["applicability"] == "normative"],
        "native_history_ids": list(b01.CASE_IDS if family == "b01" else b02.CASE_IDS),
        "requirement_ids": case["requirement_ids"], "original_definition": case,
        "original_case_sha256": digest(case), "native_definition_protocol": native_protocol,
        "native_definition_sha256": native_definition,
        "native_evaluator_source": evaluator, "native_evaluator_sha256": _LOADED_SOURCES[evaluator],
        "native_source_identity_protocol": "candidate-storage-driver-v1" if family == "b01" else "candidate-intake-store-driver-v3",
        "common_source_helper": {"module": "gossip_harness.candidate_observation_admission_v1", "function": "source_sha256",
            "source_sha256": _LOADED_SOURCES["gossip_harness/candidate_observation_admission_v1.py"]},
        "definition_sources": _LOADED_SOURCES, "diagnostics": cells,
        "decisive_ids": [row["check_id"] for row in cells if row["applicability"] == "normative"],
        "source_unit_mapping_limit": "B02 preserves only authored per-check finite facets. B01 provides case-level requirement provenance only; no invented per-check source-unit mapping.",
        "m4_expectation_transformations": [],
        "compatibility_clauses": [{"source": TARGET_CONTRACT, "source_sha256": TARGET_CONTRACT_SHA256,
            "pointer": "/" + field, "value": contract[field]} for field in ("compatibility", "amendments")],
        "excluded_fixture_projection": {"source": "gossip_harness/candidate_intake_store_profile_v1.py",
            "source_sha256": _LOADED_SOURCES["gossip_harness/candidate_intake_store_profile_v1.py"],
            "purpose": "authored-v2-b02-fixture-qualification-only", "assertion_count": 40,
            "reason": "Authored private auxiliary representation is not a generic candidate product expectation"},
        "remaining_coverage": list(REMAINING_COVERAGE), "semantic_authority": False,
        "execution_authenticated": False, "full_requirement_verdict": None})


def profile_for(family: str, case_id: str, purpose: str) -> StorageProductProfile:
    require(type(family) is str and family in ("b01", "b02"), "Unknown storage family")
    require(type(case_id) is str and case_id in (b01.CASE_IDS if family == "b01" else b02.CASE_IDS), "Unknown storage case")
    require(type(purpose) is str and purpose in registry.PURPOSES, "Unknown product execution purpose")
    _check_sources()
    return StorageProductProfile(family, case_id, purpose, _profile_bytes(family, case_id, purpose))


def _same(left: Any, right: Any) -> bool:
    return encoded(left) == encoded(right)


def _partial_b01(case: dict[str, Any], observations: dict[str, Any], responses: dict[str, Any]) -> dict[str, bool | None]:
    checks: dict[str, bool | None] = {}
    for phase in PHASES:
        checks[phase + ".persisted-state"] = _same(observations[phase].data, case[phase]) if phase in observations else None
    checks["result"] = _same(responses["after"], case["result"]) if "after" in responses else None
    for row in case["before"]["jobs"]:
        jid = row["public"]["job_id"]
        for field in ("manifest", "content_hashes", "receipt"):
            if field == "receipt" and case["case_id"] == "empty-batch":
                continue
            key = jid + (".receipt-conservation" if field == "receipt" else ".immutable-" + field)
            available = [observations[phase].persisted_strings.get(jid, {}) for phase in PHASES if phase in observations]
            # An already observed missing field or changed string remains false
            # even if a later capture never arrives.
            if any(field not in value for value in available):
                checks[key] = False
            elif "before" in observations and any(value[field] != observations["before"].persisted_strings[jid][field] for value in available):
                checks[key] = False
            else:
                checks[key] = True if len(available) == 3 else None
    checks["reopened.persisted-strings"] = (observations["after"].persisted_strings == observations["reopened"].persisted_strings
        if "after" in observations and "reopened" in observations else None)
    for phase, previous in (("after", "before"), ("reopened", "after")):
        complete = phase in observations and previous in observations
        checks[phase + ".auxiliary-conservation"] = (_same(observations[previous].auxiliary_tables, observations[phase].auxiliary_tables) if complete else None)
        ancillary = lambda value: tuple(row for row in value.files if row["path"] != "catalog.sqlite")
        checks[phase + ".auxiliary-file-conservation"] = (ancillary(observations[previous]) == ancillary(observations[phase])
            if complete and observations[previous].layout in (storage_observer.SQLITE_LAYOUT, storage_observer.V2_SQLITE_LAYOUT) else None)
    return checks


def _partial_b02(case: dict[str, Any], observations: dict[str, Any], responses: dict[str, Any],
                 *, expected_override: dict[str, Any] | None = None) -> dict[str, bool | None]:
    results = {phase: value for phase, value in responses.items() if type(value) is list}
    selected = b02.select_expected_case(case["case_id"], results)
    expected = selected["expected"] if expected_override is None else expected_override
    checks: dict[str, bool | None] = {}
    missing_discriminator = False
    if "expected_alternatives" in case:
        discriminator = case["branch_discriminator"]
        frames = results.get(discriminator["phase"])
        missing_discriminator = frames is None and expected_override is None
        checks["admission-variant"] = None if missing_discriminator else selected.get("expected_variant") is not None
    if missing_discriminator:
        # Missing branch selection must not erase an outcome established under
        # every authored alternative (e.g. a wrong result count or changed raw
        # strings across reopen). Compare each fixed selector independently.
        branches = [_partial_b02(case, observations, responses, expected_override=value)
                    for value in case["expected_alternatives"].values()]
        keys = tuple(dict.fromkeys(key for branch in branches for key in branch))
        common = {key: branches[0].get(key) if all(branch.get(key) is branches[0].get(key)
                    for branch in branches[1:]) else None for key in keys}
        common["admission-variant"] = None
        return common
    boundary: tuple[int, int] | None = None
    for ordinal, phase in enumerate(PHASES):
        for index, frame in enumerate(results.get(phase, [])):
            if legacy_b02._unavailable_reason(frame) is not None:
                boundary = (ordinal, index)
                break
        if boundary is not None:
            break
    for phase in PHASES:
        checks[phase + ".persisted-state"] = _same(observations[phase].data, expected[phase]) if phase in observations else None
        frames = results.get(phase)
        checks[phase + ".result-count"] = len(frames) == len(expected["results"][phase]) if frames is not None else None
        for index, frame in enumerate(expected["results"][phase]):
            checks[f"{phase}.result.{index}"] = (index < len(frames) and legacy_b02._matches_result(frames[index], frame)) if frames is not None else None
    for phase in PHASES:
        checks[phase + ".persisted-string-job-set"] = (set(observations[phase].persisted_strings) == set(_jobs(expected, phase)) if phase in observations else None)
    for jid, original_expected in _jobs(expected, "before").items():
        original = observations["before"].persisted_strings.get(jid, {}) if "before" in observations else None
        for phase in ("after", "reopened"):
            current = observations[phase].persisted_strings.get(jid, {}) if phase in observations else None
            for field in ("manifest", "content_hashes", "receipt"):
                other = _jobs(expected, phase).get(jid)
                if field == "receipt" and (other is None or not _same(original_expected["receipt"], other["receipt"])):
                    continue
                key = phase + "." + jid + (".receipt-conservation" if field == "receipt" else ".immutable-" + field)
                checks[key] = (bool(field in original and field in current and original[field] == current[field])
                    if original is not None and current is not None else None)
    checks["reopened.persisted-strings"] = (observations["after"].persisted_strings == observations["reopened"].persisted_strings
        if "after" in observations and "reopened" in observations else None)
    if _same(expected["before"], expected["after"]):
        checks["after.auxiliary-conservation"] = (_same(observations["before"].auxiliary_tables, observations["after"].auxiliary_tables)
            if "before" in observations and "after" in observations else None)
    checks["reopened.auxiliary-conservation"] = (_same(observations["after"].auxiliary_tables, observations["reopened"].auxiliary_tables)
        if "after" in observations and "reopened" in observations else None)
    if boundary is not None:
        ordinal, index = boundary
        for check in checks:
            phase = check.split(".")[0]
            if phase in PHASES:
                current = PHASES.index(phase)
                suffix = check[len(phase) + 1:]
                if current > ordinal or (current == ordinal and (not suffix.startswith("result.") or int(suffix.split(".")[1]) >= index)):
                    checks[check] = None
            if check == "admission-variant" and ordinal == 0 and index <= 2:
                checks[check] = None
    if case["case_id"] in b02.POLICY_LIMITED_CASE_IDS:
        comparison = checks.pop(b02.LEGACY_EXACT_ASSERTION_ID)
        frames = results.get("after", [])
        rejection, _, _ = b02._native_rejection(frames[0] if frames else None, comparison is None)
        checks[b02.NATIVE_REJECTION_ASSERTION_ID] = rejection
        checks[b02.UNSPECIFIED_ASSERTION_IDS[0]] = None
    return checks


def project(profile: StorageProductProfile, observations: dict[str, Any], responses: dict[str, Any]) -> dict[str, Any]:
    """Project only actual available inputs; callers authenticate all provenance.

    ``responses`` contains driver-returned phase values (B01 result at ``after``;
    B02 lists of normalized frames), not outer ``{phase, value}`` wire envelopes.
    Raw captures/responses remain the owner's evidence; these objects and hashes
    alone are never accepted as proof that a candidate executed.
    """
    require(type(profile) is StorageProductProfile, "Exact storage profile required")
    expected_profile = profile_for(profile.family, profile.case_id, profile.purpose)
    require(profile == expected_profile, "Profile differs from complete prospective declaration")
    require(type(observations) is dict and set(observations) <= set(PHASES), "Unknown observation phases")
    require(type(responses) is dict and set(responses) <= set(PHASES), "Unknown response phases")
    observation_type = storage_observer.Observation if profile.family == "b01" else intake_observer.Observation
    require(all(type(value) is observation_type for value in observations.values()), "Wrong observer value type")
    require(len({(value.registration_sha256, value.schema_sha256, value.layout) for value in observations.values()}) <= 1,
            "Snapshot source/layout registration mismatch")
    record = profile.record()
    case = record["original_definition"]
    native: dict[str, bool | None]
    if profile.family == "b01":
        native = _partial_b01(case, observations, responses)
    elif set(observations) == set(PHASES) and set(responses) == set(PHASES) and all(type(value) is list for value in responses.values()):
        native = b02.evaluate_case(profile.case_id, observations["before"], observations["after"],
                                   observations["reopened"], responses)["checks"]
    else:
        native = _partial_b02(case, observations, responses)
    # Native result keys are filtered through the fixed prospective declaration.
    # Unqualified auxiliary comparisons stay visible as observed diagnostics but
    # cannot become a decisive verdict even if their old comparison is true.
    rows = []
    checks: dict[str, bool | None] = {}
    unavailable = []
    unspecified = []
    for cell in record["diagnostics"]:
        key, applicability = cell["check_id"], cell["applicability"]
        observed = native.get(key)
        require(observed is None or type(observed) is bool, "Non-boolean native comparison")
        value = observed if applicability == "normative" else None
        checks[key] = value
        if applicability == "unspecified":
            unspecified.append(key)
        elif value is None:
            unavailable.append({"check_id": key, "reason": cell["reason"] if applicability == "unqualified"
                                else "required-actual-observation-or-schedule-unavailable"})
        rows.append({**cell, "value": value, "observed_comparison": observed})
    output = {"protocol": PROTOCOL, "profile_sha256": profile.sha256, "family": profile.family,
        "case_id": profile.case_id, "purpose": profile.purpose, "checks": checks, "diagnostics": rows,
        "diagnostic_ids": list(profile.diagnostic_ids), "decisive_ids": list(profile.decisive_ids),
        "unspecified_assertions": unspecified, "observation_unavailable": unavailable,
        "available_capture_phases": [phase for phase in PHASES if phase in observations],
        "available_response_phases": [phase for phase in PHASES if phase in responses],
        "authority": "unregistered-source-derived-product-slice-only", "execution_authenticated": False,
        "full_requirement_verdict": None, "remaining_coverage": list(REMAINING_COVERAGE)}
    if profile.family == "b02" and profile.case_id in b02.POLICY_LIMITED_CASE_IDS:
        frames = responses.get("after")
        expected_frame = case["expected"]["results"]["after"][0]
        actual_frame = frames[0] if type(frames) is list and frames else None
        before_frames = responses.get("before")
        prior_frames = (before_frames if type(before_frames) is list else []) + (frames[:1] if type(frames) is list else [])
        dependent = any(legacy_b02._unavailable_reason(frame) is not None for frame in prior_frames)
        output["legacy_exact_error_diagnostic"] = {"check_id": b02.LEGACY_EXACT_ASSERTION_ID,
            "expected_frame": deepcopy(expected_frame), "observed_frame": deepcopy(actual_frame),
            "matched": (_same(actual_frame, expected_frame) if type(frames) is list and not dependent else None),
            "qualification_credit": False}
    return output


_LOADED_SOURCES = definition_sources()
