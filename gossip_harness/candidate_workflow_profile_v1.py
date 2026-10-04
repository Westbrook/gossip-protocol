"""Closed prospective workflow definitions; expectations never enter the child.

The unchanged public eight are historical examples, not historical receipts.
Supplemental V2 values below are authored from the approved rules independently
of candidate output and of the frozen guard's modeled answers. Calling this
module grants no source-delegation, purpose-conversion, or semantic authority.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import Any

from . import library_project_fixture_v1 as native
from . import project_acceptance_registry_v1 as registry
from . import cumulative_workflow_exposure_v1 as exposure
from .candidate_storage_product_profile_v1 import (
    ProfileError as ProfileError, decode as decode, digest as digest, encoded as encoded, require,
)

PROTOCOL = "candidate-workflow-profile-v1-ascii-json-v1"
FAMILY = "workflow-final-m4-v1"
INPUT_PROTOCOL = "workflow-input-v1"
ORIGINAL_DEFINITION_PURPOSE = "public_development"
SUPPLEMENT_DEFINITION_PURPOSE = "public_contract_regression"
ORIGINAL_SOURCE_SHA256 = "fa4440f1ac7e63e3b4ef7d9978a8970e8d6cd688efe9c2e1b6d1e75c0f757caf"
TARGET_CONTRACT_SHA256 = "2d88ce0775888f148b0ec3caf90b3d5c82d8fed71f53bec5f7e75f492ae998dc"
WORKFLOW_ADDENDUM_SHA256 = "dfba7ee907eb3fb6b60795e6c8fe1b2ed78fe566e1009b7fc8c9774de268d6cf"
CLI_ADDENDUM_SHA256 = "d568aabfddfbf87b0a1bb8cddd11d22cfdf5e0d5b5b6830124ecbf1df86f7295"
ROOT = Path(__file__).resolve().parents[1]
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
MAX_EPOCH = 9223372036854775807
LIMITS = {
    "operations_per_call": 64, "original_input_bytes": 61440,
    "original_answer_bytes": 61440, "normalized_v2_answer_bytes": 61824,
    "raw_frame_payload_bytes": 131072, "history_stdout_bytes": 524288,
    "history_stderr_bytes": 65536, "call_timeout_seconds": 30,
    "history_timeout_seconds": 300, "calls_per_history": 3,
}
SOURCE_UNIT_IDS = (
    "m1:V0-ADAPTER-01", "m1:V0-ADAPTER-02", "m1:M1-ADAPTER-03",
    "CLARIFY-INPUT-DOMAIN", "M4-COMPATIBILITY:clause:2",
)
ORIGINAL_CASE_IDS = (
    "inherited-text-persistence", "identity-and-replay", "errors-preserve-catalog",
    "m1-cancel-fences-old-token", "m1-atomic-conflict-after-prepare",
    "m1-per-source-provenance", "m1-invalid-member-rolls-back",
    "m1-injected-transaction-failure",
)
SUPPLEMENT_CASE_IDS = (
    "WF09-empty-modes", "WF10-defaults-and-export-selection",
    "WF11-v0-unsupported-m1-continuation", "WF12-mixed-job-state-order-and-reopen",
    "WF13-domain-errors-continue", "WF14-exact64-operations",
    "WF15-existing-job-token-domain", "WF16-absent-token-and-hook-distinction",
    "WF17-normalized-answer-growth", "WF18-same-process-call-isolation",
    "WF19-provisional-fault-boundary", "WF20-invalid-query-options-continue",
)
CASE_IDS = ORIGINAL_CASE_IDS + SUPPLEMENT_CASE_IDS
OPERATION_COUNTS = ((5,), (6,), (6,), (9,), (6,), (5,), (4,), (8,), (0, 0),
                    (9,), (9,), (23,), (7,), (64,), (10,), (16,), (2,), (4, 6, 0), (8,), (5,))
LIMITATIONS = (
    "Twenty public histories, twenty-three solve calls and 212 operations; no held-out-family claim.",
    "Original admission, normalized V2 support and physical raw capture are three separate quantities.",
    "WF17 witnesses 61446 normalized bytes; it does not qualify the required 61824-byte ceiling.",
    "Complete solve output does not prove real application delegation, isolated roots or hook placement.",
    "Exact-source independent inspection and actual lifecycle/storage originals remain separate duties.",
    "No archive-parser, HTTP, CLI, browser, forced concurrency or process-durability credit is inferred.",
    "All five workflow units, other 307 units, 22 authorities and 188 gap duties remain subject to full scope review.",
    "Fresh independent acceptance/repeatability requires separately enrolled purpose conversion and barriers.",
)


# Finite output associations for the actual final-M4 workflow owner. These
# select seven unchanged original outputs, not every selector in the family.
M4_COMPATIBILITY_UNIT = "M4-COMPATIBILITY:clause:2"
M4_COMPATIBILITY_GATE = "M4-COMPATIBILITY.public-contract"
M4_COMPATIBILITY_CLAUSE_SHA256 = "c2b8c12c68c266311258b9a02f15996963292d3c62339a1e1bbc6b25d1f5a419"
M4_COMPATIBILITY_RESULTS = {
    "inherited-text-persistence": {
        0: "Original import result and six-field document with exact UTF-8 text.",
        2: "Original source-ordered search result after this declared reopen.",
        4: "Original v0 export format and historical document objects.",
    },
    "m1-cancel-fences-old-token": {
        0: "Original M1 submit JOB shape/value at epoch1.",
        1: "Original M1 prepare TOKEN shape/value at epoch1.",
        7: "Original M1 commit RECEIPT and six-field historical document at epoch3.",
        8: "Original completed-epoch replay returns the same historical RECEIPT value.",
    },
}
_REQUIREMENT_BY_UNIT = {
    "m1:V0-ADAPTER-01": "V0-ADAPTER-01", "m1:V0-ADAPTER-02": "V0-ADAPTER-02",
    "m1:M1-ADAPTER-03": "M1-ADAPTER-03", M4_COMPATIBILITY_UNIT: "M4-COMPATIBILITY",
}


def normalized(value: Any) -> bytes:
    """Original oracle size-accounting spelling, separate from canonical hashes."""
    return json.dumps(value, ensure_ascii=True, allow_nan=False).encode("utf-8")


def _frozen_source() -> None:
    require(hashlib.sha256(Path(native.__file__).read_bytes()).hexdigest() == ORIGINAL_SOURCE_SHA256,
            "Frozen original workflow definition source differs")
    require(hashlib.sha256(Path(__file__).read_bytes()).hexdigest() == LOADED_SOURCE_SHA256,
            "Loaded workflow profile source changed; use a fresh worker")


def _doc(source: str, text: str) -> dict[str, str]:
    # Published source identity/blob rules only; no candidate/oracle call.
    return {"document_id": "doc-" + hashlib.sha256(b"document\0" + source.encode()).hexdigest(),
            "source_id": "src-" + hashlib.sha256(b"source\0" + source.encode()).hexdigest(),
            "source": source, "blob_id": "blob-" + hashlib.sha256(text.encode()).hexdigest(),
            "title": source.rsplit("/", 1)[-1], "text": text}


def _job(jid: str, state: str = "queued", *, epoch: int = 1,
         total: int = 0, completed: int = 0, error: str | None = None) -> dict[str, Any]:
    return {"job_id": jid, "epoch": epoch, "state": state, "total": total,
            "completed": completed, "error": error}


def _error(code: str) -> dict[str, str]:
    return {"error": code}


def _listing(docs: list[dict[str, str]], total: int | None = None) -> dict[str, Any]:
    return {"documents": deepcopy(docs), "total": len(docs) if total is None else total}


def _export(docs: list[dict[str, str]]) -> dict[str, Any]:
    return {"format": "local-research-library-v0", "documents": deepcopy(docs)}


def _receipt(job: dict[str, Any], docs: list[dict[str, str]]) -> dict[str, Any]:
    return {"job": deepcopy(job), "documents": deepcopy(docs)}


def _answer(results: list[Any], docs: list[dict[str, str]],
            jobs: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    result = {"results": deepcopy(results), "documents": deepcopy(docs)}
    if jobs is not None:
        result["jobs"] = deepcopy(jobs)
    return result


def _call(operations: list[dict[str, Any]], expected: dict[str, Any], *,
          milestone: str | None = "m1") -> dict[str, Any]:
    payload: dict[str, Any] = {"operations": deepcopy(operations)}
    if milestone is not None:
        payload["milestone"] = milestone
    return {"input": payload, "expected": deepcopy(expected)}


def _supplements(originals: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    """Authored expected snapshots, not a second transition interpreter.

    Every state transition is explicit. The one exact historical repeat (WF19)
    is copied from the unchanged original fixture and marked as such below.
    """
    empty = _answer([], [], [])
    wf09 = [_call([], _answer([], []), milestone=None), _call([], empty)]
    a, b = _doc("a.txt", "Alpha"), _doc("b.md", "Beta")
    wf10 = [_call([
        {"op": "import", "source": "a.txt", "text": "Alpha"},
        {"op": "import", "source": "b.md", "text": "Beta"},
        {"op": "list"}, {"op": "search", "query": ""}, {"op": "export"},
        {"op": "export", "sources": []}, {"op": "show", "source": "a.txt"},
        {"op": "list", "offset": 1, "limit": 1}, {"op": "export", "sources": ["b.md"]},
    ], _answer([{"status": "imported", "document": a}, {"status": "imported", "document": b},
                _listing([a, b]), _listing([a, b]), _export([a, b]), _export([]), a,
                _listing([b], 2), _export([b])], [a, b]), milestone=None)]
    v = _doc("v.txt", "v0")
    wf11 = [_call([
        {"op": "submit", "job_id": "unused", "entries": []},
        {"op": "prepare", "job_id": "unused"},
        {"op": "commit", "job_id": "unused", "epoch": 1},
        {"op": "cancel", "job_id": "unused"}, {"op": "retry", "job_id": "unused"},
        {"op": "job", "job_id": "unused"}, {"op": "reopen"},
        {"op": "import", "source": "v.txt", "text": "v0"}, {"op": "list"},
    ], _answer([_error("unsupported_operation") for _ in range(6)] + [
        {"reopened": True}, {"status": "imported", "document": v}, _listing([v])], [v]), milestone=None)]
    y, z = _doc("y.txt", "Y"), _doc("z.txt", "Z")
    jq = _job("z-queued")
    jr0, jr = _job("r-running", total=2), _job("r-running", "running", total=2)
    jf0 = _job("f-failed", total=1)
    jf = _job("f-failed", "failed", total=1, error="invalid_source")
    jc0 = _job("c-completed", total=2)
    jc = _job("c-completed", "completed", total=2, completed=2)
    jx0 = _job("x-cancelled", total=1)
    jx2 = _job("x-cancelled", "cancelled", epoch=2, total=1)
    jx3 = _job("x-cancelled", epoch=3, total=1)
    jx4 = _job("x-cancelled", "cancelled", epoch=4, total=1)
    wf12 = [_call([
        {"op": "submit", "job_id": "z-queued", "entries": []},
        {"op": "submit", "job_id": "r-running", "entries": [
            {"source": "b.txt", "text": "B"}, {"source": "a.txt", "text": "A"}]},
        {"op": "prepare", "job_id": "r-running"},
        {"op": "submit", "job_id": "f-failed", "entries": [{"source": "../bad.txt", "text": "bad"}]},
        {"op": "prepare", "job_id": "f-failed"},
        {"op": "submit", "job_id": "c-completed", "entries": [
            {"source": "z.txt", "text": "Z"}, {"source": "y.txt", "text": "Y"}]},
        {"op": "prepare", "job_id": "c-completed"}, {"op": "commit", "job_id": "c-completed", "epoch": 1},
        {"op": "submit", "job_id": "x-cancelled", "entries": [{"source": "c.txt", "text": "C"}]},
        {"op": "prepare", "job_id": "x-cancelled"}, {"op": "cancel", "job_id": "x-cancelled"},
        {"op": "reopen"}, {"op": "job", "job_id": "z-queued"}, {"op": "job", "job_id": "r-running"},
        {"op": "job", "job_id": "f-failed"}, {"op": "job", "job_id": "c-completed"},
        {"op": "job", "job_id": "x-cancelled"}, {"op": "retry", "job_id": "x-cancelled"},
        {"op": "prepare", "job_id": "x-cancelled"}, {"op": "commit", "job_id": "x-cancelled", "epoch": 2},
        {"op": "cancel", "job_id": "x-cancelled"}, {"op": "reopen"}, {"op": "list"},
    ], _answer([jq, jr0, {"job_id": "r-running", "epoch": 1}, jf0, _error("invalid_source"), jc0,
                {"job_id": "c-completed", "epoch": 1}, _receipt(jc, [y, z]), jx0,
                {"job_id": "x-cancelled", "epoch": 1}, jx2, {"reopened": True}, jq, jr, jf, jc, jx2,
                jx3, {"job_id": "x-cancelled", "epoch": 3}, _error("stale_epoch"), jx4,
                {"reopened": True}, _listing([y, z])], [y, z], [jc, jf, jr, jx4, jq]))]
    kept = _doc("kept.txt", "kept")
    wf13 = [_call([
        {"op": "show", "source": "missing.txt"}, {"op": "import", "source": "../bad.txt", "text": "bad"},
        {"op": "import", "source": "kept.txt", "text": "kept"},
        {"op": "import", "source": "kept.txt", "text": "changed"},
        {"op": "show", "source": "kept.txt"}, {"op": "export", "sources": ["missing.txt"]}, {"op": "list"},
    ], _answer([_error("not_found"), _error("invalid_source"), {"status": "imported", "document": kept},
                _error("source_changed"), kept, _error("not_found"), _listing([kept])], [kept], []))]
    wf14 = [_call([{"op": "list"} for _ in range(64)],
                  _answer([_listing([]) for _ in range(64)], [], []))]
    queued, done = _job("empty"), _job("empty", "completed")
    receipt = _receipt(done, [])
    wf15 = [_call([
        {"op": "submit", "job_id": "empty", "entries": []},
        {"op": "commit", "job_id": "empty", "epoch": 1},
        {"op": "commit", "job_id": "empty", "epoch": MAX_EPOCH},
        {"op": "commit", "job_id": "empty", "epoch": MAX_EPOCH + 1},
        {"op": "prepare", "job_id": "empty"}, {"op": "commit", "job_id": "empty", "epoch": 1},
        {"op": "commit", "job_id": "empty", "epoch": 1},
        {"op": "commit", "job_id": "empty", "epoch": MAX_EPOCH},
        {"op": "commit", "job_id": "empty", "epoch": MAX_EPOCH + 1}, {"op": "job", "job_id": "empty"},
    ], _answer([queued, _error("job_state"), _error("stale_epoch"), _error("invalid_request"),
                {"job_id": "empty", "epoch": 1}, receipt, receipt, _error("stale_epoch"),
                _error("invalid_request"), done], [], [done]))]
    epochs: list[Any] = [1, MAX_EPOCH, MAX_EPOCH + 1, 0, -1, True, 1.0, "1", None]
    hooks: list[tuple[int, Any]] = [(1, 0), (0, 1), (1, None), (0, "true"), (1, []), (0, {})]
    wf16 = [_call(
        [{"op": "commit", "job_id": "absent", "epoch": epoch} for epoch in epochs] +
        [{"op": "commit", "job_id": "absent", "epoch": epoch, "fail_before_commit": flag}
         for epoch, flag in hooks] + [{"op": "list"}],
        _answer([_error("not_found"), _error("not_found")] + [_error("invalid_request") for _ in range(7)] +
                [_error("not_found"), _error("invalid_request"), _error("not_found"),
                 _error("invalid_request"), _error("not_found"), _error("invalid_request"), _listing([])], [], []))]
    large = _doc("a.txt", "x" * 30363)
    wf17 = [_call([{"op": "import", "source": "a.txt", "text": "x" * 30363},
                   {"op": "commit", "job_id": "absent", "epoch": MAX_EPOCH + 1}],
                  _answer([{"status": "imported", "document": large}, _error("invalid_request")], [large], []))]
    first, second = _doc("first.txt", "first"), _doc("second.txt", "second")
    shared_q = _job("shared", total=1)
    shared_done = _job("shared", "completed", total=1, completed=1)
    wf18 = [
        _call([{"op": "submit", "job_id": "shared", "entries": [{"source": "first.txt", "text": "first"}]},
               {"op": "prepare", "job_id": "shared"}, {"op": "commit", "job_id": "shared", "epoch": 1}, {"op": "list"}],
              _answer([shared_q, {"job_id": "shared", "epoch": 1}, _receipt(shared_done, [first]),
                       _listing([first])], [first], [shared_done])),
        _call([{"op": "job", "job_id": "shared"}, {"op": "list"},
               {"op": "submit", "job_id": "shared", "entries": [{"source": "second.txt", "text": "second"}]},
               {"op": "prepare", "job_id": "shared"}, {"op": "commit", "job_id": "shared", "epoch": 1}, {"op": "reopen"}],
              _answer([_error("not_found"), _listing([]), shared_q, {"job_id": "shared", "epoch": 1},
                       _receipt(shared_done, [second]), {"reopened": True}], [second], [shared_done])),
        _call([], empty),
    ]
    fault = originals[7]
    wf19 = [{"input": deepcopy(fault["input"]), "expected": deepcopy(fault["expected"])}]
    wf20 = [_call([{"op": "list", "offset": True}, {"op": "list", "limit": 0},
                   {"op": "search", "query": "x" * 257}, {"op": "list"}, {"op": "search", "query": ""}],
                  _answer([_error("invalid_request") for _ in range(3)] + [_listing([]), _listing([])], [], []))]
    return [wf09, wf10, wf11, wf12, wf13, wf14, wf15, wf16, wf17, wf18, wf19, wf20]


def historical_admission(payload: Any) -> dict[str, Any]:
    """Frozen guard only. A rejection is an evaluator boundary, never a product fail."""
    _frozen_source()
    try:
        answer = native.oracle(deepcopy(payload))
    except (ValueError, TypeError, UnicodeError, OverflowError) as error:
        return {"admitted": False, "guard_source_sha256": ORIGINAL_SOURCE_SHA256,
                "reason": str(error), "historical_expected": None,
                "historical_answer_bytes": None, "historical_answer_sha256": None}
    return {"admitted": True, "guard_source_sha256": ORIGINAL_SOURCE_SHA256,
            "reason": None, "historical_expected": answer,
            "historical_answer_bytes": len(normalized(answer)), "historical_answer_sha256": digest(answer)}


def definitions() -> tuple[dict[str, Any], ...]:
    _frozen_source()
    originals = native.public_cases("m1")
    require(tuple(row["id"] for row in originals) == ORIGINAL_CASE_IDS, "Original eight roster differs")
    groups = [[{"input": deepcopy(row["input"]), "expected": deepcopy(row["expected"])}]
              for row in originals] + _supplements(originals)
    output = []
    for index, (case_id, calls) in enumerate(zip(CASE_IDS, groups, strict=True)):
        require(tuple(len(call["input"]["operations"]) for call in calls) == OPERATION_COUNTS[index],
                "Closed workflow operation census differs")
        for call_index, call in enumerate(calls):
            call["call_id"] = "call-%03d" % call_index
            call["historical_admission"] = historical_admission(call["input"])
            require(call["historical_admission"]["admitted"], "Closed history lost original admission")
            call["normalized_expected_bytes"] = len(normalized(call["expected"]))
            require(call["normalized_expected_bytes"] <= LIMITS["normalized_v2_answer_bytes"],
                    "Required admitted V2 answer exceeds support; profile unqualified, input not removed")
        output.append({"id": case_id, "case_id": case_id, "original_definition_purpose": ORIGINAL_DEFINITION_PURPOSE
                       if index < 8 else SUPPLEMENT_DEFINITION_PURPOSE,
                       "original_case_id": case_id if index < 8 else (
                           "m1-injected-transaction-failure" if index == 18 else None),
                       "expectation_origin": "unchanged frozen public example" if index < 8 or index == 18
                       else "prospectively authored V2 values from approved workflow addendum",
                       "calls": calls, "capture_expectations": capture_expectations(case_id)})
    return tuple(output)


def case_definition(case_id: str) -> dict[str, Any]:
    require(type(case_id) is str and case_id in CASE_IDS, "Closed workflow case required")
    return definitions()[CASE_IDS.index(case_id)]


def public_case(case_id: str) -> dict[str, Any]:
    """Host-side public definition; this includes expectations and is not a child recipe."""
    return case_definition(case_id)


def recipe_for(case_id: str) -> dict[str, Any]:
    return {"protocol": INPUT_PROTOCOL, "case_id": case_id,
            "calls": [deepcopy(call["input"]) for call in case_definition(case_id)["calls"]]}


def input_files(case_id: str) -> dict[str, bytes]:
    return {"workflow-input.json": encoded(recipe_for(case_id))}


def expected_for(case_id: str, call_index: int) -> dict[str, Any]:
    calls = case_definition(case_id)["calls"]
    require(type(call_index) is int and 0 <= call_index < len(calls), "Closed workflow call required")
    return deepcopy(calls[call_index]["expected"])


def definition_sources() -> dict[str, str]:
    _frozen_source()
    pins = {"gossip_harness/library_project_fixture_v1.py": ORIGINAL_SOURCE_SHA256,
            "library-cumulative-product-v2.json": TARGET_CONTRACT_SHA256,
            "docs/cumulative-workflow-v2-addendum-v4.txt": WORKFLOW_ADDENDUM_SHA256}
    for path, expected in pins.items():
        require(hashlib.sha256((ROOT / path).read_bytes()).hexdigest() == expected,
                "Workflow definition/normative source changed: " + path)
    for path in ("gossip_harness/candidate_workflow_profile_v1.py",
                 "gossip_harness/candidate_storage_product_profile_v1.py",
                 "gossip_harness/candidate_http_journal_v3.py",
                 "gossip_harness/project_acceptance_registry_v1.py"):
        pins[path] = hashlib.sha256((ROOT / path).read_bytes()).hexdigest()
    pins.update(exposure.definition_sources())
    return dict(sorted(pins.items()))


def definition_sha256() -> str:
    return digest({"protocol": PROTOCOL, "sources": definition_sources(), "definitions": definitions(),
                   "admission_controls": admission_controls(), "mechanism_controls": mechanism_controls(),
                   "limits": LIMITS})


def exact(expected: Any, actual: Any) -> bool:
    if type(expected) is not type(actual):
        return False
    if type(expected) is dict:
        return expected.keys() == actual.keys() and all(exact(value, actual[key]) for key, value in expected.items())
    if type(expected) is list:
        return len(expected) == len(actual) and all(exact(a, b) for a, b in zip(expected, actual, strict=True))
    return bool(expected == actual)


def _unit_for(operation: dict[str, Any], milestone: str) -> str:
    job_op = operation["op"] in ("submit", "prepare", "commit", "cancel", "retry", "job")
    return "m1:M1-ADAPTER-03" if job_op and milestone == "m1" else "m1:V0-ADAPTER-02"


@dataclass(frozen=True, slots=True)
class WorkflowProfile:
    case_id: str
    purpose: str

    @property
    def original_definition_purpose(self) -> str:
        return case_definition(self.case_id)["original_definition_purpose"]

    @property
    def family(self) -> str:
        return FAMILY

    @property
    def calls(self) -> tuple[dict[str, Any], ...]:
        return tuple(case_definition(self.case_id)["calls"])

    @property
    def phases(self) -> tuple[str, ...]:
        return tuple(call["call_id"] for call in self.calls)

    @property
    def requirement_ids(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(_REQUIREMENT_BY_UNIT[unit] for row in self.selectors()
                                   for unit in row["source_unit_ids"]))

    @property
    def ordered_case_ids(self) -> tuple[str, ...]:
        return tuple(row["case_id"] for row in self.selectors())

    @property
    def diagnostic_case_ids(self) -> tuple[str, ...]:
        return self.ordered_case_ids

    def selectors(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for call_index, call in enumerate(self.calls):
            prefix = self.case_id + ":" + call["call_id"]
            def append(kind: str, suffix: str, path: list[str | int], units: list[str], rationale: str,
                       operation_index: int | None = None) -> None:
                rows.append({"case_id": prefix + ":" + suffix, "call_index": call_index,
                             "call_id": call["call_id"], "kind": kind, "value_path": path,
                             "operation_index": operation_index,
                             "definition_pointer": "/calls/" + str(call_index) + "/expected" +
                                 ("" if not path else "/" + "/".join(str(part) for part in path)),
                             "assertion_kind": "history",
                             "comparison_kind": "structural" if kind == "shape" else "semantic",
                             "lanes": ["public-contract", "workflow"]
                                 if self.case_id in ORIGINAL_CASE_IDS else ["workflow"],
                             "logical_gate_ids": ["M1-GATE-PUBLIC", "M1-GATE-ADAPTER"]
                                 if self.case_id in ORIGINAL_CASE_IDS else ["M1-GATE-ADAPTER"],
                             "pointer": "/projection/observations/" + str(len(rows)) + "/disposition",
                             "source_unit_ids": units,
                             "source_unit_facets": [{"source_unit_id": unit, "rationale": rationale,
                                 "logical_gate_ids": ["M1-GATE-PUBLIC", "M1-GATE-ADAPTER"]
                                 if self.case_id in ORIGINAL_CASE_IDS else ["M1-GATE-ADAPTER"]} for unit in units],
                             "evidence_kind": "complete_workflow_call", "comparison": "exact_typed_json",
                             "rationale": rationale, "semantically_reviewed": False})
            append("shape", "shape", [], ["m1:V0-ADAPTER-01"],
                   "Exact outer keys and list types for this complete call only; no lifecycle/delegation inference.")
            for operation_index, operation in enumerate(call["input"]["operations"]):
                append("result", "result-%03d" % operation_index, ["results", operation_index],
                       [_unit_for(operation, call["input"].get("milestone", "v0"))], "Exact typed result at this ordered operation; no per-operation physical trace inferred.",
                       operation_index)
            append("documents", "documents", ["documents"], ["m1:V0-ADAPTER-02"],
                   "Exact source-ordered final six-field documents for this call; not proof of real persistence.")
            if "jobs" in call["expected"]:
                append("jobs", "jobs", ["jobs"], ["m1:M1-ADAPTER-03"],
                       "Exact job-ID-ordered final JOB values for this call; no archive parser or durability credit.")
        mapped = M4_COMPATIBILITY_RESULTS.get(self.case_id, {})
        for selector in rows:
            operation = selector["operation_index"]
            if selector["kind"] != "result" or selector["call_index"] != 0 or operation not in mapped:
                continue
            rationale = (mapped[operation] + " Exact selected output of the unchanged public workflow on the "
                         "authenticated final M4 subject; finite grammar/output compatibility only. "
                         "No real-module delegation, input isolation, complete clause, CLI/HTTP, migration, "
                         "or whole-release coverage is inferred from this value.")
            selector["source_unit_ids"].append(M4_COMPATIBILITY_UNIT)
            selector["logical_gate_ids"].append(M4_COMPATIBILITY_GATE)
            selector["source_unit_facets"].append({
                "source_unit_id": M4_COMPATIBILITY_UNIT,
                "logical_gate_ids": [M4_COMPATIBILITY_GATE], "rationale": rationale,
                "source_reference": {"path": "library-cumulative-product-v2.json",
                    "json_pointer": "/requirements/15/clauses/2",
                    "value_sha256": M4_COMPATIBILITY_CLAUSE_SHA256}})
        for selector in capture_selectors(self.case_id):
            selector["pointer"] = "/projection/observations/" + str(len(rows)) + "/disposition"
            rows.append(selector)
        return rows

    def record(self) -> dict[str, Any]:
        definition = case_definition(self.case_id)
        return {"protocol": PROTOCOL, "family": self.family, "case_id": self.case_id, "purpose": self.purpose,
                "original_definition_purpose": definition["original_definition_purpose"],
                "definition": definition, "definition_sha256": digest(definition),
                "definition_sources": definition_sources(), "limits": deepcopy(LIMITS),
                "base_contract_sha256": TARGET_CONTRACT_SHA256, "workflow_addendum_sha256": WORKFLOW_ADDENDUM_SHA256,
                "cli_addendum_sha256": CLI_ADDENDUM_SHA256,
                "effective_requirements": exposure.cli.manifest(),
                "workflow_requirements": exposure.manifest(),
                "combined_effective_requirements": exposure.combined_manifest(),
                "ordered_calls": list(self.phases), "selectors": self.selectors(),
                "ordered_case_ids": list(self.ordered_case_ids), "requirement_ids": list(self.requirement_ids),
                "input_recipe_sha256": digest(recipe_for(self.case_id)), "seed": 0,
                "m4_compatibility_output_associations": [
                    {"operation_index": index, "rationale": rationale}
                    for index, rationale in M4_COMPATIBILITY_RESULTS.get(self.case_id, {}).items()],
                "capture_contract": {"protocol": "wf19-committed-graph-conservation-v1",
                    "roles": list(capture_roles(self.case_id)),
                    "components": list(CAPTURE_COMPONENTS) if capture_roles(self.case_id) else [],
                    "limitations": list(CAPTURE_LIMITATIONS)},
                "purpose_conversion_authority_supplied": False, "independent_semantic_scope_review_supplied": False,
                "whole_project_acceptance": False, "limitations": list(LIMITATIONS)}

    @property
    def sha256(self) -> str:
        return digest(self.record())


def accepted_profile(value: Any) -> bool:
    return type(value) is WorkflowProfile


def profile_for(case_id: str, purpose: str = "public_release") -> WorkflowProfile:
    require(type(purpose) is str and purpose in registry.PURPOSES, "Prospective product purpose required")
    case_definition(case_id)
    return WorkflowProfile(case_id, purpose)


def reconstruct(value: WorkflowProfile) -> WorkflowProfile:
    require(accepted_profile(value), "Exact named workflow profile required")
    return profile_for(value.case_id, value.purpose)


def admission_controls() -> tuple[dict[str, Any], ...]:
    """Eighteen evaluator-only families. Admission truth is declared before running the guard."""
    def row(name: str, payload: dict[str, Any], admitted: bool, rationale: str) -> dict[str, Any]:
        return {"control_id": name, "input": payload, "expected_admitted": admitted,
                "purpose": "harness_qualification", "product_history": False, "rationale": rationale}
    def m1(ops: list[dict[str, Any]]) -> dict[str, Any]:
        return {"milestone": "m1", "operations": ops}
    def input_boundary(size: int) -> dict[str, Any]:
        # v0 rejects dispatch of submit, so this input-size witness does not grow its answer.
        payload: dict[str, Any] = {"operations": [{"op": "submit", "job_id": "j", "entries": [{"source": "a.txt", "text": ""}]}]}
        payload["operations"][0]["entries"][0]["text"] = "x" * (size - len(normalized(payload)))
        return payload
    controls = [
        row("exact64ops", m1([{"op": "list"} for _ in range(64)]), True, "Inclusive operation ceiling."),
        row("over65ops", m1([{"op": "list"} for _ in range(65)]), False, "One operation beyond frozen ceiling."),
        row("input61440", input_boundary(61440), True, "Exact original JSON input bytes; small unsupported-operation answer."),
        row("input61441", input_boundary(61441), False, "One original JSON input byte over limit."),
        row("historical-answer61440", m1([
            {"op": "import", "source": "a.txt", "text": "x" * 30363},
            {"op": "commit", "job_id": "absent", "epoch": MAX_EPOCH + 1}]), True,
            "WF17 historical model is exactly61440; prospective answer is61446."),
        row("historical-answer61441", m1([
            {"op": "import", "source": "a.txt", "text": "x" * 30361},
            {"op": "show", "source": "../bad.txt"}]), False,
            "Two fewer repeated text bytes remove4, invalid_source versus not_found adds5."),
        row("unknown-operation", m1([{"op": "unknown"}]), False, "No new operation admitted."),
        row("missing-operation-field", m1([{"op": "import", "source": "a.txt"}]), False, "Required text absent."),
        row("extra-operation-field", m1([{"op": "list", "extra": 1}]), False, "Unknown operation field."),
    ]
    bad_tokens: tuple[tuple[str, Any], ...] = (("0", 0), ("minus1", -1), ("bool", True),
                                             ("float", 1.0), ("string", "1"), ("null", None))
    for name, epoch in bad_tokens:
        controls.append(row("present-epoch-" + name if name != "0" else "present-epoch0", m1([
            {"op": "submit", "job_id": "present", "entries": []},
            {"op": "commit", "job_id": "present", "epoch": epoch}]), False,
            "Frozen guard reaches token-shape validation only after finding an existing job."))
    controls.extend([
        row("present-malformed-fault-flag", m1([{"op": "submit", "job_id": "present", "entries": []},
            {"op": "commit", "job_id": "present", "epoch": 1, "fail_before_commit": 1}]), False,
            "Existing job reaches frozen exact-boolean fault flag validation."),
        row("invalid-milestone", {"milestone": "m4", "operations": []}, False, "No new workflow milestone name."),
        row("malformed-entries-shape", m1([{"op": "submit", "job_id": "present", "entries": [{}]}]), False,
            "M1 submit requires exact source/text entry fields."),
    ])
    return tuple(controls)


MECHANISM_CONTROL_IDS = (
    "normalized-support61824", "normalized-over61825-incomplete-qualification", "equivalent-JSON-spelling",
    "raw-capture-exact-limit", "raw-over-limit-unavailable", "truncated-result-unavailable",
    "complete-known-failure-before-missing-later-call", "natural-exit-missing", "source/runtime-after-ack-revocation",
    "lost-ack-before-dispatch", "partial-reader-start-teardown", "closed-review-owner-refusal",
)


def mechanism_controls() -> tuple[dict[str, Any], ...]:
    """Closed qualification families; never additional product solve histories."""
    expectations = (
        "Retain/reconstruct exact authored61824 normalized bytes; physical qualification required.",
        "61825 independently authored expected bytes leave qualification unfinished before dispatch; no admission narrowing.",
        "Alternative legal JSON spelling with the same exact typed value preserves semantic judgment.",
        "Retain exact131072 frame payload,524288 stdout and65536 stderr byte controls at their capture layer.",
        "Frame131073/stdout524289/stderr65537 preserve incomplete/unavailable, never truncated semantic pass.",
        "Incomplete JSON has no complete semantic value and remains unavailable.",
        "Retain an authenticated complete failed first call if a later call is operationally missing.",
        "Missing natural process exit leaves lifecycle unavailable even when values are complete.",
        "Changing exact source/runtime after acknowledgement revokes dispatch or attribution.",
        "Absent durable acknowledgement prevents candidate dispatch and redispatch.",
        "Failure after one reader starts still tears down all owned resources and retains cleanup uncertainty.",
        "Closed independent review owner cannot publish or revive an observation.",
    )
    return tuple({"control_id": name, "purpose": "harness_qualification", "product_history": False,
                  "expected": expectation} for name, expectation in zip(MECHANISM_CONTROL_IDS, expectations, strict=True))


def source_sha256(files: dict[str, bytes]) -> str:
    """Canonical workflow-native identity; common source identity stays separate."""
    from .candidate_workflow_review_v1 import source_sha256 as reviewed_source_sha256
    return reviewed_source_sha256(files)



CAPTURE_CASE_ID = "WF19-provisional-fault-boundary"
CAPTURE_ROLES = ("initial", "post_fault", "reopened", "final")
CAPTURE_COMPONENTS = ("blobs", "documents", "jobs", "persisted_fields")
CAPTURE_LIMITATIONS = (
    "Only exact enrolled call/boundary/occurrence roles selected from authenticated captures may reach this scorer.",
    "Post-fault means after injected_failure was caught and before the next operation; reopened precedes later successful commit.",
    "Normalized primary content/job graph plus declared stored fields only; auxiliary tables remain unscored diagnostics.",
    "These snapshots corroborate rollback conservation; they do not prove the hook followed provisional writes.",
    "Exact-source independent hook-placement inspection remains mandatory, including fake-error and early-hook controls.",
)


def capture_roles(case_id: str) -> tuple[str, ...]:
    require(type(case_id) is str and case_id in CASE_IDS, "Closed workflow case required")
    return CAPTURE_ROLES if case_id == CAPTURE_CASE_ID else ()


def capture_expectations(case_id: str) -> dict[str, dict[str, Any]]:
    """Independent WF19 storage snapshots, authored from the published transition.

    No expected bytes come from candidate/reference output or an observed
    snapshot. Receipt formatting is not invented: data.jobs.receipt is compared
    as exact typed JSON, without a raw-receipt spelling claim. Only newly
    created content-hash arrays have a fixed spelling here; manifests are
    compared semantically through data.jobs, with raw text retained diagnostically.
    Source-specific SQLite auxiliary tables are not asserted.
    """
    if not capture_roles(case_id):
        return {}
    keep, new = _doc("keep.txt", "keep"), _doc("new.txt", "new")
    manifest = [{"source": "new.txt", "text": "new"}]
    hashes = [hashlib.sha256(b"new").hexdigest()]
    running = _job("fault", "running", total=1)
    completed = _job("fault", "completed", total=1, completed=1)
    receipt = _receipt(completed, [new])

    def state(docs: list[dict[str, str]], job: dict[str, Any] | None,
              stored_receipt: dict[str, Any] | None) -> dict[str, Any]:
        blobs: list[dict[str, Any]] = [{"blob_id": doc["blob_id"], "bytes": len(doc["text"].encode("utf-8")),
                         "sha256": hashlib.sha256(doc["text"].encode("utf-8")).hexdigest()}
                        for doc in docs]
        blobs.sort(key=lambda row: row["blob_id"])
        jobs = [] if job is None else [{"public": deepcopy(job), "manifest": deepcopy(manifest),
                "content_hashes": list(hashes), "receipt": deepcopy(stored_receipt)}]
        persisted = {} if job is None else {"fault": {
            "content_hashes": encoded(hashes).decode("ascii")}}
        return {"data": {"blobs": blobs, "documents": deepcopy(sorted(docs, key=lambda doc: doc["source"])),
                         "jobs": jobs}, "persisted_fields": persisted}

    return {"initial": state([], None, None), "post_fault": state([keep], running, None),
            "reopened": state([keep], running, None), "final": state([keep, new], completed, receipt)}


def capture_selectors(case_id: str) -> tuple[dict[str, Any], ...]:
    """Sixteen named WF19 snapshot comparisons, no family-wide capture credit."""
    rows = []
    for role in capture_roles(case_id):
        for component in CAPTURE_COMPONENTS:
            path = ["persisted_fields"] if component == "persisted_fields" else ["data", component]
            rationale = ("Exact independently authored WF19 " + role + " " + component +
                         "; source-qualified committed-state capture only, no hook-placement or auxiliary-table claim.")
            rows.append({"case_id": case_id + ":call-000:capture:" + role + ":" + component,
                         "call_index": 0, "call_id": "call-000", "kind": "capture",
                         "capture_role": role, "capture_component": component,
                         "value_path": path, "operation_index": None,
                         "definition_pointer": "/capture_expectations/" + role + "/" + "/".join(path),
                         "assertion_kind": "history", "comparison_kind": "semantic",
                         "lanes": ["workflow"], "logical_gate_ids": ["M1-GATE-ADAPTER"],
                         "source_unit_ids": ["m1:M1-ADAPTER-03"],
                         "source_unit_facets": [{"source_unit_id": "m1:M1-ADAPTER-03",
                                                 "logical_gate_ids": ["M1-GATE-ADAPTER"], "rationale": rationale}],
                         "evidence_kind": "captured_sqlite", "comparison": "exact_typed_json",
                         "rationale": rationale, "semantically_reviewed": False})
    return tuple(rows)


def _captured_component(storage: Any, component: str) -> tuple[bool, Any]:
    """Project an already authenticated complete mapper result, never a marker."""
    if type(storage) is not dict:
        return False, None
    if component != "persisted_fields":
        data = storage.get("data")
        if type(data) is not dict or component not in data:
            return False, None
        return True, data[component]
    strings = storage.get("persisted_strings")
    if type(strings) is not dict:
        return False, None
    result: dict[str, Any] = {}
    for jid, fields in strings.items():
        if (type(jid) is not str or type(fields) is not dict
                or set(fields) != {"manifest", "content_hashes", "receipt"}
                or type(fields["manifest"]) is not str or type(fields["content_hashes"]) is not str):
            return False, None
        result[jid] = {"content_hashes": fields["content_hashes"]}
    return True, result


def project(value: WorkflowProfile, responses: dict[int, Any],
            capture_facts: dict[int, Any] | None = None) -> dict[str, Any]:
    """Compare authenticated originals; absent later evidence cannot erase false.

    The owner authenticates complete responses and independently selects each
    closed capture role through the enrolled call/boundary/occurrence mapping.
    Capture input is {call_index: {role: captured_state_result}}. Arbitrary raw
    boundary lists and candidate markers cannot supply these semantic values.
    Ordinary captures stay diagnostic; only WF19's sixteen named comparisons
    score the declared primary graph. No lifecycle or hook-placement inference.
    """
    require(accepted_profile(value) and value == reconstruct(value), "Exact workflow profile required")
    require(type(responses) is dict and all(type(key) is int and 0 <= key < len(value.calls)
                                          for key in responses), "Closed response call indices required")
    require(capture_facts is None or type(capture_facts) is dict, "Host capture mapping required")
    captures = {} if capture_facts is None else capture_facts
    require(all(type(key) is int and 0 <= key < len(value.calls) for key in captures),
            "Closed capture call indices required")
    calls = value.calls
    capture_expected = capture_expectations(value.case_id)
    rows: list[dict[str, Any]] = []
    for selector in value.selectors():
        index = selector["call_index"]
        if selector["kind"] == "capture":
            role = selector["capture_role"]
            roles = captures.get(index)
            storage = roles.get(role) if type(roles) is dict else None
            available, observed = _captured_component(storage, selector["capture_component"])
            expected = capture_expected[role]
            for part in selector["value_path"]:
                expected = expected[part]
            passed = exact(expected, observed) if available else None
        else:
            available = index in responses
            actual = responses.get(index)
            expected = calls[index]["expected"]
            if selector["kind"] == "shape":
                passed = (type(actual) is dict and set(actual) == set(expected)
                          and all(type(actual[key]) is list for key in expected)
                          and len(actual["results"]) == len(expected["results"])) if available else None
                observed = None if not available else {"exact_outer_shape": passed}
            else:
                for part in selector["value_path"]:
                    expected = expected[part]
                    if not available:
                        continue
                    if type(part) is str and type(actual) is dict and part in actual:
                        actual = actual[part]
                    elif type(part) is int and type(actual) is list and part < len(actual):
                        actual = actual[part]
                    else:
                        available = False
                passed = exact(expected, actual) if available else None
                observed = actual if available else None
        disposition = "unavailable" if passed is None else "pass" if passed else "fail"
        row = {"case_id": selector["case_id"], "call_index": index,
               "call_id": selector["call_id"], "kind": selector["kind"],
               "operation_index": selector["operation_index"],
               "disposition": disposition, "actual": deepcopy(observed)}
        if selector["kind"] == "capture":
            row["capture_role"] = selector["capture_role"]
            row["capture_component"] = selector["capture_component"]
        rows.append(row)
    return {"observations": rows, "known_failed": [row["case_id"] for row in rows if row["disposition"] == "fail"],
            "unavailable": [row["case_id"] for row in rows if row["disposition"] == "unavailable"],
            "diagnostics": {"complete_call_indices": sorted(responses), "capture_facts": deepcopy(captures),
                            "scored_capture_roles": list(capture_roles(value.case_id)),
                            "capture_facts_grant_semantic_or_lifecycle_credit": False,
                            "closed_capture_selectors_grant_only_declared_graph_comparisons": True,
                            "capture_limitations": list(CAPTURE_LIMITATIONS)}}
