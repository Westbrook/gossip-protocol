"""Pure public predicates for reviewer-generated tests; no observation authentication or execution.

The future host owner must authenticate exact source, ordered raw observations,
deadline eligibility and cleanup BEFORE using these values for selection. These
functions supply no product acceptance, source review or execution authority.
"""
from __future__ import annotations

import hashlib
import json
import math
from typing import Any

PROTOCOL = "cumulative-generated-probe-values-v1"
PRODUCT_SHA256 = "2d88ce0775888f148b0ec3caf90b3d5c82d8fed71f53bec5f7e75f492ae998dc"
SOURCE = "probe/document.txt"
JOB = "probe-job"
MAX_FILE_BYTES = 32768  # Existing product constant, not a new study allowance.
TEMPLATES = {
    "refresh-identity-v1": ("M2-REFRESH", ("initial_text", "replacement_text")),
    "refresh-noop-v1": ("M2-REFRESH", ("text",)),
    "completed-receipt-replay-v1": ("M2-REFRESH", ("initial_text", "replacement_text")),
    "manifest-content-hash-v1": ("M3-BACKUP-RESTORE", ("text",)),
}
NORMATIVE_REFS = {
    "refresh-identity-v1": ("/requirements/0/clauses", "/requirements/1/clauses"),
    "refresh-noop-v1": ("/requirements/0/clauses", "/requirements/1/clauses"),
    "completed-receipt-replay-v1": ("/requirements/1/clauses",),
    # requirement_id is a release gate, not a claim of backup/restore coverage.
    "manifest-content-hash-v1": ("/amendments/3/clauses/0", "/amendments/3/clauses/1"),
}


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=True, allow_nan=False,
                      separators=(",", ":")).encode("ascii")


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def exact(left: Any, right: Any) -> bool:
    if type(left) is not type(right):
        return False
    if type(left) is dict:
        return left.keys() == right.keys() and all(exact(left[k], right[k]) for k in left)
    if type(left) is list:
        return len(left) == len(right) and all(exact(a, b) for a, b in zip(left, right))
    return bool(left == right)


def _pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in values:
        if key in result:
            raise ValueError("duplicate_member")
        result[key] = value
    return result


def _constant(_value: str) -> Any:
    raise ValueError("nonfinite_number")


def _bounded(value: Any, *, max_nodes: int, max_depth: int) -> None:
    pending, count = [(value, 0)], 0
    while pending:
        item, depth = pending.pop()
        count += 1
        if count > max_nodes or depth > max_depth:
            raise ValueError("json_structure_limit")
        if type(item) is dict:
            pending.extend((child, depth + 1) for child in item.values())
        elif type(item) is list:
            pending.extend((child, depth + 1) for child in item)
        elif type(item) is float and not math.isfinite(item):
            raise ValueError("nonfinite_number")


def parse_batch(raw: bytes, *, limits: dict[str, int]) -> list[dict[str, Any]]:
    """Limits are mandatory frozen host configuration; no implicit 4/8 defaults.

    Does not spend an admission slot or call a semantic oracle. The adapter owns
    cumulative quotas, deterministic processing, rejection receipts and dedup.
    """
    required = {"review_bytes", "proposal_count", "json_nodes", "json_depth"}
    if (type(limits) is not dict or set(limits) != required
            or any(type(v) is not int or v < 1 for v in limits.values())):
        raise ValueError("missing_frozen_parser_limits")
    if type(raw) is not bytes or len(raw) > limits["review_bytes"]:
        raise ValueError("review_bytes_limit")
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs,
                           parse_constant=_constant)
        _bounded(value, max_nodes=limits["json_nodes"], max_depth=limits["json_depth"])
    except (UnicodeError, RecursionError) as error:
        raise ValueError("invalid_bounded_json") from error
    if (type(value) is not dict or set(value) != {"protocol", "probes"}
            or value["protocol"] != PROTOCOL or type(value["probes"]) is not list
            or len(value["probes"]) > limits["proposal_count"]):
        raise ValueError("batch_schema")
    return value["probes"]


def admit(proposal: dict[str, Any], *, released_requirements: tuple[str, ...],
          contract_sha256: str) -> dict[str, Any]:
    """Validate one closed public proposal; execution remains separately gated."""
    if contract_sha256 != PRODUCT_SHA256:
        raise ValueError("different_product_contract")
    if type(proposal) is not dict or set(proposal) != {"template_id", "requirement_id", "parameters", "expectation"}:
        raise ValueError("proposal_schema")
    template = proposal["template_id"]
    if type(template) is not str or template not in TEMPLATES:
        raise ValueError("unsupported_template")
    requirement, keys = TEMPLATES[template]
    if proposal["requirement_id"] != requirement or requirement not in released_requirements:
        raise ValueError("unreleased_or_unsupported_requirement")
    parameters = proposal["parameters"]
    if type(parameters) is not dict or set(parameters) != set(keys):
        raise ValueError("parameters_schema")
    for value in parameters.values():
        if type(value) is not str:
            raise ValueError("text_required")
        try:
            if len(value.encode("utf-8")) > MAX_FILE_BYTES:
                raise ValueError("product_file_bound")
        except UnicodeError as error:
            raise ValueError("unencodable_text_not_in_initial_domain") from error
    if "replacement_text" in parameters and parameters["initial_text"] == parameters["replacement_text"]:
        raise ValueError("changed_refresh_required")
    expected = ({"kind": "exact_scalar", "value": "unchanged"}
                if template == "refresh-noop-v1" else {"kind": "contract_relation", "value": True})
    if not exact(proposal["expectation"], expected):
        raise ValueError("inadmissible_expectation")
    normalized = {"protocol": PROTOCOL, "contract_sha256": PRODUCT_SHA256,
                  "template_id": template, "requirement_id": requirement,
                  "normative_refs": list(NORMATIVE_REFS[template]),
                  "parameters": dict(parameters), "expectation": expected}
    return {**normalized, "probe_id": digest(normalized), "acceptance_authority": False}


def _document(text: str) -> dict[str, Any]:
    return {"document_id": "doc-" + hashlib.sha256(b"document\0" + SOURCE.encode()).hexdigest(),
            "source_id": "src-" + hashlib.sha256(b"source\0" + SOURCE.encode()).hexdigest(),
            "source": SOURCE, "blob_id": "blob-" + hashlib.sha256(text.encode()).hexdigest(),
            "title": "document.txt", "text": text}


def _record(value: Any) -> bool:
    keys = {"document", "revision", "edit_version", "deleted", "notes", "tags", "collections"}
    return (type(value) is dict and set(value) == keys and type(value["document"]) is dict
            and type(value["revision"]) is int and 1 <= value["revision"] <= 16
            and type(value["edit_version"]) is int and 1 <= value["edit_version"] <= 9223372036854775807
            and type(value["deleted"]) is bool and type(value["notes"]) is str
            and type(value["tags"]) is list and all(type(item) is str for item in value["tags"])
            and type(value["collections"]) is list and all(type(item) is str for item in value["collections"]))


def prerequisite_value(admitted: dict[str, Any], slot: str, value: Any) -> dict[str, Any]:
    """Host checks the original value before acknowledging its continuation.

    A failing prerequisite is retained even when subsequent slots never arrive.
    This is still a value predicate, never authentication of an invocation.
    """
    params = admitted["parameters"]
    text = params.get("initial_text", params.get("text"))
    job = {"job_id": JOB, "epoch": 1, "state": "completed", "total": 1,
           "completed": 1, "error": None}
    expected = {
        "imported": {"status": "imported", "document": _document(text)},
        "submitted": {**job, "state": "queued", "completed": 0},
        "prepared": {"job_id": JOB, "epoch": 1},
        "original_receipt": {"job": job, "documents": [_document(text)]},
        "before": {"document": _document(text), "revision": 1, "edit_version": 1,
                   "deleted": False, "notes": "", "tags": [], "collections": []},
    }
    if slot not in expected:
        raise ValueError("not_a_declared_prerequisite_slot")
    return {"slot": slot, "disposition": "pass" if exact(value, expected[slot]) else "fail",
            "acceptance_authority": False, "observation_authority": False}


def evaluate_values(admitted: dict[str, Any], values: dict[str, Any]) -> dict[str, Any]:
    """Score already bounded values; caller still owes original authentication.

    Missing slots are unavailable. Present malformed observations fail the
    declared value assertion. No result in this module authenticates a caller's
    supplied dictionary, candidate invocation, stored state or time eligibility.
    """
    template, params = admitted["template_id"], admitted["parameters"]
    names = {
        "refresh-identity-v1": ("imported", "before", "refresh"),
        "refresh-noop-v1": ("imported", "before", "refresh"),
        "completed-receipt-replay-v1": ("submitted", "prepared", "original_receipt", "before", "refresh", "replay"),
        "manifest-content-hash-v1": ("submitted", "captured_job"),
    }
    if template not in names:
        raise ValueError("unsupported_template")
    base = {"probe_id": admitted["probe_id"], "acceptance_authority": False,
            "observation_authority": False}
    if type(values) is dict:
        for name in names[template]:
            if name in values and name in {"imported", "submitted", "prepared", "original_receipt", "before"}:
                if prerequisite_value(admitted, name, values[name])["disposition"] == "fail":
                    return {**base, "disposition": "fail", "reason": "prerequisite_value_contradicted:" + name}
    if type(values) is not dict or any(name not in values for name in names[template]):
        return {**base, "disposition": "unavailable", "reason": "required_original_value_missing"}
    if template == "manifest-content-hash-v1":
        row = values["captured_job"]
        try:
            if type(row) is not dict or set(row) != {"manifest", "content_hashes", "receipt"}:
                raise ValueError("captured_row_schema")
            if type(row["manifest"]) is not str or type(row["content_hashes"]) is not str:
                raise ValueError("captured_sqlite_types")
            manifest = json.loads(row["manifest"], object_pairs_hook=_pairs, parse_constant=_constant)
            hashes = json.loads(row["content_hashes"], object_pairs_hook=_pairs, parse_constant=_constant)
            expected_manifest = [{"source": SOURCE, "text": params["text"]}]
            passed = exact(manifest, expected_manifest) and exact(hashes, [hashlib.sha256(params["text"].encode()).hexdigest()])
        except (ValueError, TypeError, RecursionError):
            passed = False
    else:
        before, response = values["before"], values["refresh"]
        after = response.get("record") if type(response) is dict else None
        if type(after) is not dict:
            return {**base, "disposition": "fail",
                    "reason": "declared_value_relation_contradicted"}
        passed = (_record(before) and _record(after) and type(response) is dict
                  and set(response) == {"status", "record"}
                  and before["deleted"] is False and after["deleted"] is False)
        if passed and template == "refresh-noop-v1":
            passed = (response["status"] == "unchanged"
                      and exact(before["document"], _document(params["text"]))
                      and exact(after, before))
        elif passed:
            passed = (response["status"] == "refreshed"
                      and exact(before["document"], _document(params["initial_text"]))
                      and exact(after["document"], _document(params["replacement_text"]))
                      and all(exact(before["document"][key], after["document"][key])
                              for key in ("document_id", "source_id", "source", "title")))
            if passed and template == "completed-receipt-replay-v1":
                receipt = values["original_receipt"]
                passed = (type(receipt) is dict and set(receipt) == {"job", "documents"}
                          and exact(receipt["documents"], [_document(params["initial_text"])])
                          and exact(receipt["job"], {"job_id": JOB, "epoch": 1, "state": "completed",
                                                   "total": 1, "completed": 1, "error": None})
                          and exact(values["replay"], receipt))
    return {**base, "disposition": "pass" if passed else "fail",
            "reason": "declared_value_relation_holds" if passed else "declared_value_relation_contradicted"}
