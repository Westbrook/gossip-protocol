"""Prospective public B02 cases. No candidate source or outputs define answers.

Recipes are evaluator data; only bounded fixtures and calls enter the candidate
container. Expected observations remain in the host control plane. These finite
facets are not a full requirement verdict, registered acceptance, or held-out data.
"""
from __future__ import annotations

import base64
from copy import deepcopy
from functools import lru_cache
import hashlib
import io
import json
from pathlib import Path
import stat
import struct
from typing import Any
import zipfile

from .candidate_intake_store_observer_v1 import Observation, ObservationUnavailable

PROTOCOL = "candidate-intake-store-cases-v1"
PRODUCT_SHA256 = "2d88ce0775888f148b0ec3caf90b3d5c82d8fed71f53bec5f7e75f492ae998dc"
INVENTORY_SHA256 = "f5d808866d6f18f3fd095de5d447bb5b0e2a43e20771761fe91a38cf4f59ad4a"
PHASES = ("before", "after", "reopened")
TARGET_IDS = (
    "M1-A06", "M1-A08", "M1-I01", "M1-I02", "M1-I03", "M1-I04", "M1-I05",
    "M1-I06", "M1-I08", "M1-I09", "M1-I10", "M1-I11", "M1-I12", "M1-I13",
    "M1-I14", "M1-I15", "M1-I16", "M1-I17", "M1-I18", "M1-I19", "M1-I20",
    "M1-I21", "M1-I22", "M1-I23", "M1-I24", "M1-I26", "M1-I27", "M1-I28",
    "M1-I29", "M1-J01", "M1-J02", "M1-J04", "M1-J05", "M1-J07", "M1-J08",
    "M1-T02", "M1-T03", "M1-T04", "M1-T05", "M1-T06", "M1-T08", "M1-T09",
)
OMISSIONS = (
    "Public development definitions, not concealed final acceptance or whole-ID closure.",
    "No HTTP, CLI, browser rendering/network-attempt observations, process death or later milestone closure.",
    "No scheduler race census; controlled public-method interposition covers only its declared boundaries.",
    "No duplicate JSON object-key or malformed ZIP filename precedence absent normative clarification.",
    "No direct Store hook-type or arbitrary fail_job code-domain rule beyond the frozen signature.",
    "No aggregate-only ratio failure: under identical member accounting its inequality follows from member bounds.",
    "No exhaustion fixture at COUNTER_MAX; range inputs are covered, durable max-state setup remains separate.",
    "Storage suitability and schema/profile review remain independent prerequisites; no private-layout mandate.",
)


def encoded(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def document(source: str, text: str) -> dict[str, Any]:
    raw = text.encode("utf-8")
    return {"document_id": "doc-" + hashlib.sha256(b"document\0" + source.encode()).hexdigest(),
            "source_id": "src-" + hashlib.sha256(b"source\0" + source.encode()).hexdigest(),
            "source": source, "blob_id": "blob-" + hashlib.sha256(raw).hexdigest(),
            "title": source.rsplit("/", 1)[-1], "text": text}


def stored_job(job_id: str, entries: list[dict[str, str]], state: str = "queued",
               epoch: int = 1, error: str | None = None) -> dict[str, Any]:
    entries = sorted(deepcopy(entries), key=lambda row: (row["source"], row["text"]))
    hashes: list[str | None] = []
    for row in entries:
        try:
            hashes.append(hashlib.sha256(row["text"].encode("utf-8")).hexdigest())
        except UnicodeEncodeError:
            hashes.append(None)
    public = {"job_id": job_id, "epoch": epoch, "state": state, "total": len(entries),
              "completed": len(entries) if state == "completed" else 0, "error": error}
    return {"public": public, "manifest": entries, "content_hashes": hashes,
            "receipt": {"job": deepcopy(public), "documents": [document(**row) for row in entries]}
            if state == "completed" else None}


def file_fixture(path: str, raw: bytes) -> dict[str, str]:
    return {"path": path, "kind": "file", "bytes_base64": base64.b64encode(raw).decode("ascii")}


def zip_bytes(members: list[dict[str, Any]], *, pad_to: int | None = None,
              encrypted: bool = False) -> bytes:
    """Build valid bounded archives; flag edits isolate encryption metadata only.

    Padding is a legal leading self-extracting prefix, not a huge decompressed
    member. ZIP offsets are adjusted by the standard reader's prefix support.
    """
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        for member in members:
            info = zipfile.ZipInfo(member["name"], date_time=(2020, 1, 1, 0, 0, 0))
            info.create_system = 3
            info.external_attr = int(member.get("mode", stat.S_IFREG | 0o600)) << 16
            info.compress_type = member.get("compression", zipfile.ZIP_STORED)
            archive.writestr(info, member.get("raw", b""))
    raw = bytearray(stream.getvalue())
    if encrypted:
        # Modify both matching headers; encrypted status is checked before
        # decoding. Deliberately no password/ciphertext rule is inferred.
        for signature, offset in ((b"PK\x03\x04", 6), (b"PK\x01\x02", 8)):
            pos = raw.find(signature)
            if pos < 0:
                raise ValueError("encryption fixture needs a member")
            flags = struct.unpack_from("<H", raw, pos + offset)[0]
            struct.pack_into("<H", raw, pos + offset, flags | 1)
    if pad_to is not None:
        if len(raw) > pad_to:
            raise ValueError("archive padding target smaller than archive")
        raw = bytearray(b"S" * (pad_to - len(raw))) + raw
    return bytes(raw)


class _Case:
    def __init__(self, case_id: str, family: str, requirements: list[str],
                 assertion_class: str, facet: str) -> None:
        self.row: dict[str, Any] = {
            "case_id": case_id, "family": family, "requirement_ids": requirements,
            "assertion_class": assertion_class, "facet": facet,
            "recipe": {"fixtures": [file_fixture("keep.txt", b"keep")],
                       "phases": {phase: [] for phase in PHASES}},
            "expected": {"results": {phase: [] for phase in PHASES}},
            "assertions": [], "omissions": list(OMISSIONS),
        }
        self.documents: dict[str, dict[str, Any]] = {"keep.txt": document("keep.txt", "keep")}
        self.jobs: dict[str, dict[str, Any]] = {"untouched": stored_job("untouched", [])}
        self.call("before", {"op": "import", "source": "keep.txt"},
                  {"status": "imported", "document": document("keep.txt", "keep")})
        self.call("before", self.op("store", "create_job", "untouched", []),
                  self.jobs["untouched"]["public"])

    @staticmethod
    def op(target: str, method: str, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return {"target": target, "method": method, "args": list(args), "kwargs": kwargs}

    def call(self, phase: str, operation: dict[str, Any], value: Any = None,
             error: str | None = None) -> None:
        self.row["recipe"]["phases"][phase].append(deepcopy(operation))
        self.row["expected"]["results"][phase].append(
            {"error": error} if error is not None else {"value": deepcopy(value)})

    def put_job(self, job_id: str, entries: list[dict[str, str]], state: str = "queued",
                epoch: int = 1, error: str | None = None) -> dict[str, Any]:
        job = stored_job(job_id, entries, state, epoch, error)
        self.jobs[job_id] = job
        if state == "completed":
            for row in entries:
                self.documents[row["source"]] = document(**row)
        return job

    def snapshot(self, phase: str) -> None:
        docs = sorted(self.documents.values(), key=lambda row: row["source"])
        blobs = {doc["blob_id"]: doc["text"].encode("utf-8") for doc in docs}
        self.row["expected"][phase] = deepcopy({
            "documents": docs,
            "blobs": [{"blob_id": bid, "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
                      for bid, raw in sorted(blobs.items())],
            "jobs": sorted(self.jobs.values(), key=lambda row: row["public"]["job_id"]),
        })

    def finish(self) -> dict[str, Any]:
        self.snapshot("after")
        self.snapshot("reopened")
        self.row["requirement_ids"] = list(dict.fromkeys(self.row["requirement_ids"]))
        if self.row["assertion_class"] == "matrix":
            self.row["assertion_class"] = ("negative" if any("error" in row for row in
                self.row["expected"]["results"]["after"]) else "positive")
        for requirement in self.row["requirement_ids"]:
            # Each assertion names one actual check. Setup checks still run, but
            # do not become a spurious product mapping for unrelated requirements.
            selectors = [(f"/results/after/{index}", f"after.result.{index}")
                         for index in range(len(self.row["expected"]["results"]["after"]))]
            selectors.extend((f"/{phase}", phase + ".persisted-state") for phase in ("after", "reopened"))
            for selector, check in selectors:
                self.row["assertions"].append({"requirement_id": requirement,
                    "class": self.row["assertion_class"], "lane": "direct-api-storage",
                    "case_id": self.row["case_id"], "selector": selector, "check_id": check,
                    "scope": self.row["facet"], "full_requirement": False,
                    "conjunction": "all-required-case-checks"})
        return self.row


def _setup_job(case: _Case, state: str, entries: list[dict[str, str]], *, epoch: int = 2) -> None:
    """Reach epoch2 through public cancel/retry/fail/start calls, never private SQL."""
    current = case.put_job("case", entries)
    case.call("before", case.op("store", "create_job", "case", entries), current["public"])
    if epoch == 2:
        if state == "cancelled":
            current = case.put_job("case", entries, "cancelled", 2)
            case.call("before", case.op("store", "cancel_job", "case"), current["public"])
            return
        current = case.put_job("case", entries, "failed", 1, "invalid_source")
        case.call("before", case.op("store", "fail_job", "case", 1, "invalid_source"), current["public"])
        current = case.put_job("case", entries, "queued", 2)
        case.call("before", case.op("store", "retry_job", "case"), current["public"])
    if state in ("running", "completed"):
        case.put_job("case", entries, "running", epoch)
        case.call("before", case.op("store", "start_job", "case", epoch), {"job_id": "case", "epoch": epoch})
    if state == "completed":
        current = case.put_job("case", entries, "completed", epoch)
        case.call("before", case.op("store", "commit_job", "case", epoch), current["receipt"])
    if state == "failed":
        current = case.put_job("case", entries, "failed", epoch, "invalid_source")
        case.call("before", case.op("store", "fail_job", "case", epoch, "invalid_source"), current["public"])
    if state == "cancelled":
        current = case.put_job("case", entries, "cancelled", epoch + 1)
        case.call("before", case.op("store", "cancel_job", "case"), current["public"])


PAIR = [{"source": "new-b.md", "text": "same"}, {"source": "new-a.txt", "text": "same"}]
STATES = ("queued", "running", "completed", "cancelled", "failed")


def _store_cases() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for method, requirement in (("start_job", "M1-T05"), ("fail_job", "M1-T06"),
                                ("commit_job", "M1-T08")):
        for state in STATES:
            for relation, epoch in (("lower", 1), ("current", 2), ("higher", 3)):
                case = _Case(f"store-{method}-{state}-{relation}", "state-epoch", [requirement, "M1-T08", "M1-T02"],
                             "matrix", f"Direct {method}: {state}, {relation} epoch; exact result and persisted transition.")
                _setup_job(case, state, PAIR)
                case.snapshot("before")
                args = ["case", epoch] + (["invalid_utf8"] if method == "fail_job" else [])
                if relation != "current":
                    case.call("after", case.op("store", method, *args), error="stale_epoch")
                elif method == "start_job" and state in ("queued", "running"):
                    case.put_job("case", PAIR, "running", 2)
                    case.call("after", case.op("store", method, *args), {"job_id": "case", "epoch": 2})
                elif method == "fail_job" and state in ("queued", "running"):
                    job = case.put_job("case", PAIR, "failed", 2, "invalid_utf8")
                    case.call("after", case.op("store", method, *args), job["public"])
                elif method == "commit_job" and state in ("running", "completed"):
                    job = case.put_job("case", PAIR, "completed", 2)
                    case.call("after", case.op("store", method, *args), job["receipt"])
                else:
                    case.call("after", case.op("store", method, *args), error="job_state")
                rows.append(case.finish())
    for state in STATES:
        for action, rid in (("cancel_job", "M1-J07"), ("retry_job", "M1-J08"), ("prepare", "M1-J05")):
            case = _Case(f"state-{action}-{state}", "state-action", [rid, "M1-J01", "M1-T02"],
                         "matrix", f"{action} from {state} with exact epoch/state/manifest.")
            _setup_job(case, state, PAIR)
            case.snapshot("before")
            target = "manager" if action == "prepare" else "store"
            if action == "cancel_job" and state in ("queued", "running", "cancelled"):
                epoch = 2 if state == "cancelled" else 3
                value = case.put_job("case", PAIR, "cancelled", epoch)["public"]
                case.call("after", case.op(target, action, "case"), value)
            elif action == "retry_job" and state in ("failed", "cancelled"):
                value = case.put_job("case", PAIR, "queued", 3)["public"]
                case.call("after", case.op(target, action, "case"), value)
                case.call("after", case.op(target, action, "case"), error="job_state")
            elif action == "prepare" and state in ("queued", "running"):
                case.put_job("case", PAIR, "running", 2)
                case.call("after", case.op(target, action, "case"), {"job_id": "case", "epoch": 2})
                case.call("after", case.op(target, action, "case"), {"job_id": "case", "epoch": 2})
            else:
                case.call("after", case.op(target, action, "case"), error="job_state")
            rows.append(case.finish())
        case = _Case(f"replay-{state}", "replay-conflict", ["M1-J04", "M1-T04"], "history",
                     f"Canonical equal replay and source/text/add/remove conflicts while {state}.")
        _setup_job(case, state, PAIR)
        case.snapshot("before")
        case.call("after", case.op("store", "create_job", "case", list(reversed(PAIR))), case.jobs["case"]["public"])
        for changed in ([dict(PAIR[0], source="changed.md"), PAIR[1]],
                        [dict(PAIR[0], text="changed"), PAIR[1]], PAIR + [{"source": "extra.txt", "text": "x"}], PAIR[:1]):
            case.call("after", case.op("store", "create_job", "case", changed), error="job_conflict")
        rows.append(case.finish())
    missing_operations: tuple[tuple[str, list[Any]], ...] = (("get_job", []), ("job_manifest", []), ("start_job", [1]),
                         ("fail_job", [1, "invalid_source"]), ("cancel_job", []), ("retry_job", []),
                         ("commit_job", [1]))
    for method, missing_args in missing_operations:
        case = _Case(f"missing-{method}", "missing", ["M1-T08", "M1-T02"], "negative", "Missing job via actual direct API.")
        case.snapshot("before")
        case.call("after", case.op("store", method, "missing", *missing_args), error="not_found")
        rows.append(case.finish())
    case = _Case("missing-prepare", "missing", ["M1-J05", "M1-T08"], "negative", "Missing manager prepare.")
    case.snapshot("before")
    case.call("after", case.op("manager", "prepare", "missing"), error="not_found")
    rows.append(case.finish())
    for label, value in (("zero", 0), ("negative", -1), ("overflow", 2**63), ("bool", True),
                         ("float", 1.0), ("string", "1"), ("null", None)):
        case = _Case(f"token-domain-{label}", "token-domain", ["M1-T08"], "boundary",
                     "V2 typed positive signed64 input domain, before missing-job/state access; not a max-state fixture.")
        case.snapshot("before")
        for method, tail in (("start_job", []), ("fail_job", ["invalid_source"]), ("commit_job", [])):
            case.call("after", case.op("store", method, "missing", value, *tail), error="invalid_request")
        rows.append(case.finish())
    job_id_cases: tuple[tuple[str, Any, bool], ...] = (("one", "a", True), ("max", "Z" * 62 + "_-", True), ("empty", "", False),
                             ("over", "a" * 65, False), ("space", "a b", False), ("punctuation", "a.b", False),
                             ("unicode", "é", False), ("integer", 7, False))
    for label, jid, valid in job_id_cases:
        case = _Case(f"job-id-{label}", "admission", ["M1-J02", "M1-J01"], "boundary", "Actual direct create_job ID domain.")
        case.snapshot("before")
        if valid:
            case.call("after", case.op("store", "create_job", jid, []), case.put_job(jid, [])["public"])
        else:
            case.call("after", case.op("store", "create_job", jid, []), error="invalid_request")
        rows.append(case.finish())
    malformed: tuple[tuple[str, Any], ...] = (("object", {}), ("null", None), ("member-type", [1]),
                 ("missing-source", [{"text": "x"}]), ("missing-text", [{"source": "a.txt"}]),
                 ("extra-field", [{"source": "a.txt", "text": "x", "extra": 1}]),
                 ("source-type", [{"source": 1, "text": "x"}]), ("text-type", [{"source": "a.txt", "text": None}]))
    for label, entries in malformed:
        case = _Case(f"manifest-shape-{label}", "admission", ["M1-T04"], "negative", "Structural manifest error is immediate and state-conserving.")
        case.snapshot("before")
        case.call("after", case.op("store", "create_job", "case", entries), error="invalid_request")
        rows.append(case.finish())
    case = _Case("manifest-source-text-ties", "admission", ["M1-T04"], "positive", "Canonical source/text sorting retains semantic duplicate for deferred rejection.")
    entries = [{"source": "same.txt", "text": "z"}, {"source": "same.txt", "text": "a"}]
    case.snapshot("before")
    case.call("after", case.op("store", "create_job", "case", entries), case.put_job("case", entries)["public"])
    case.call("after", case.op("store", "job_manifest", "case"), sorted(entries, key=lambda row: (row["source"], row["text"])))
    rows.append(case.finish())
    case = _Case("signatures-offline-hook", "signatures", ["M1-A08", "M1-T02"], "positive", "Required offline commit default and keyword-only flag; no undocumented type/HTTP rule.")
    case.snapshot("before")
    case.call("after", {"op": "signature"}, {"store_commit_keyword_only": True, "manager_commit_keyword_only": True,
                                                "store_default_false": True, "manager_default_false": True})
    rows.append(case.finish())
    return rows


def _history_cases() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for producer in ("create_job", "get_job", "list_jobs", "job_manifest", "start_job", "fail_job",
                     "cancel_job", "retry_job", "commit_job"):
        case = _Case("fresh-value-" + producer, "value-mutation", ["M1-T03", "M1-T02"], "history",
                     f"Mutate direct {producer} return object, then reread through required API and reopen.")
        if producer != "create_job":
            _setup_job(case, "failed" if producer == "retry_job" else "running" if producer == "commit_job" else "queued", PAIR)
        case.snapshot("before")
        args: list[Any] = ["case"]
        if producer == "create_job":
            args.append(PAIR)
            value = case.put_job("case", PAIR)["public"]
        elif producer == "get_job":
            value = case.jobs["case"]["public"]
        elif producer == "list_jobs":
            args = []
            value = [case.jobs[jid]["public"] for jid in sorted(case.jobs)]
        elif producer == "job_manifest":
            value = case.jobs["case"]["manifest"]
        elif producer == "start_job":
            args.append(2)
            case.put_job("case", PAIR, "running", 2)
            value = {"job_id": "case", "epoch": 2}
        elif producer == "fail_job":
            args.extend([2, "invalid_source"])
            value = case.put_job("case", PAIR, "failed", 2, "invalid_source")["public"]
        elif producer == "cancel_job":
            value = case.put_job("case", PAIR, "cancelled", 3)["public"]
        elif producer == "retry_job":
            value = case.put_job("case", PAIR, "queued", 3)["public"]
        else:
            args.append(2)
            value = case.put_job("case", PAIR, "completed", 2)["receipt"]
        operation = case.op("store", producer, *args)
        operation["as"] = "returned"
        case.call("after", operation, value)
        path: list[Any] = [0, "text"] if producer == "job_manifest" else [0, "state"] if producer == "list_jobs" else ["documents", 0, "text"] if producer == "commit_job" else ["epoch"]
        case.call("after", {"op": "mutate", "ref": "returned", "path": path, "value": "MUTATED"})
        if producer in ("create_job", "get_job", "list_jobs", "job_manifest", "start_job", "cancel_job", "commit_job"):
            case.call("after", case.op("store", producer, *args), value)
        case.call("after", case.op("store", "get_job", "case"), case.jobs["case"]["public"])
        case.call("after", case.op("store", "job_manifest", "case"), case.jobs["case"]["manifest"])
        case.call("reopened", case.op("store", "get_job", "case"), case.jobs["case"]["public"])
        rows.append(case.finish())
    case = _Case("input-manifest-snapshot", "value-mutation", ["M1-T03", "M1-T04"], "history",
                 "Mutating the exact original create_job input list after return does not rewrite its persisted snapshot.")
    case.snapshot("before")
    case.call("after", {"op": "bind", "as": "input", "value": PAIR})
    case.call("after", case.op("store", "create_job", "case", {"$ref": "input"}), case.put_job("case", PAIR)["public"])
    case.call("after", {"op": "mutate", "ref": "input", "path": [0, "text"], "value": "MUTATED"})
    case.call("after", case.op("store", "job_manifest", "case"), case.jobs["case"]["manifest"])
    rows.append(case.finish())
    case = _Case("cancel-retry-token-history", "token-history", ["M1-A06", "M1-J08", "M1-J07"], "history",
                 "Two real cancel/retry cycles and failed/retry fence old tokens through final completed state.")
    _setup_job(case, "running", PAIR, epoch=1)
    case.snapshot("before")
    epoch = 1
    for _ in range(2):
        epoch += 1
        job = case.put_job("case", PAIR, "cancelled", epoch)
        case.call("after", case.op("store", "cancel_job", "case"), job["public"])
        epoch += 1
        job = case.put_job("case", PAIR, "queued", epoch)
        case.call("after", case.op("store", "retry_job", "case"), job["public"])
        case.put_job("case", PAIR, "running", epoch)
        case.call("after", case.op("store", "start_job", "case", epoch), {"job_id": "case", "epoch": epoch})
    job = case.put_job("case", PAIR, "failed", epoch, "invalid_utf8")
    case.call("after", case.op("store", "fail_job", "case", epoch, "invalid_utf8"), job["public"])
    epoch += 1
    job = case.put_job("case", PAIR, "queued", epoch)
    case.call("after", case.op("store", "retry_job", "case"), job["public"])
    case.put_job("case", PAIR, "running", epoch)
    case.call("after", case.op("store", "start_job", "case", epoch), {"job_id": "case", "epoch": epoch})
    job = case.put_job("case", PAIR, "completed", epoch)
    case.call("after", case.op("store", "commit_job", "case", epoch), job["receipt"])
    for old_epoch in (1, 3, 5):
        case.call("after", case.op("store", "commit_job", "case", old_epoch), error="stale_epoch")
        case.call("reopened", case.op("store", "commit_job", "case", old_epoch), error="stale_epoch")
    rows.append(case.finish())
    case = _Case("running-prepare-after-conflict", "prepare-history", ["M1-T09", "M1-J05"], "history",
                 "Running prepare returns existing token after an actual second Store import creates a source conflict.")
    entries = [{"source": "external.txt", "text": "planned"}]
    case.row["recipe"]["fixtures"].append(file_fixture("external.txt", b"different"))
    _setup_job(case, "running", entries)
    case.snapshot("before")
    case.call("after", {"op": "import", "source": "external.txt", "connection": "secondary"},
              {"status": "imported", "document": document("external.txt", "different")})
    case.documents["external.txt"] = document("external.txt", "different")
    case.call("after", case.op("manager", "prepare", "case"), {"job_id": "case", "epoch": 2})
    rows.append(case.finish())
    for boundary, entries, requirements in (
        ("start_job", PAIR, ["M1-T05", "M1-T09"]),
        ("fail_job", [{"source": "../bad.txt", "text": "bad"}], ["M1-T06", "M1-T09"]),
    ):
        for retry in (False, True):
            case = _Case(f"interfere-{boundary}-cancel" + ("-retry" if retry else ""), "controlled-interference",
                         requirements, "history", "Second connection changes epoch at the required public Store write boundary after validation.")
            _setup_job(case, "queued", entries)
            case.snapshot("before")
            second = [{"value": stored_job("case", entries, "cancelled", 3)["public"]}]
            actions = ["cancel_job"]
            final = case.put_job("case", entries, "cancelled", 3)
            if retry:
                actions.append("retry_job")
                final = case.put_job("case", entries, "queued", 4)
                second.append({"value": final["public"]})
            case.call("after", {"op": "interfere", "boundary": boundary, "job_id": "case", "second_actions": actions},
                      {"boundary_calls": 1, "second_results": second, "prepare": {"error": "stale_epoch"}})
            rows.append(case.finish())
    case = _Case("interfere-prepare-prepare", "controlled-interference", ["M1-T05", "M1-T09", "M1-J05"], "history",
                 "Second connection prepares at the first caller's public start_job seam; both return the same token.")
    _setup_job(case, "queued", PAIR)
    case.snapshot("before")
    case.put_job("case", PAIR, "running", 2)
    token = {"job_id": "case", "epoch": 2}
    case.call("after", {"op": "interfere", "boundary": "start_job", "job_id": "case", "second_actions": ["prepare"]},
              {"boundary_calls": 1, "second_results": [{"value": token}], "prepare": {"value": token}})
    rows.append(case.finish())
    case = _Case("two-client-admission", "two-client-ordering", ["M1-J04", "M1-T04"], "history",
                 "Two independently opened Stores submit identical and conflicting same-ID manifests in controlled order.")
    _setup_job(case, "queued", PAIR)
    case.snapshot("before")
    case.call("after", case.op("secondary_store", "create_job", "case", list(reversed(PAIR))), case.jobs["case"]["public"])
    case.call("after", case.op("secondary_store", "create_job", "case", [{"source": "other.txt", "text": "other"}]), error="job_conflict")
    rows.append(case.finish())
    case = _Case("two-client-cancel", "two-client-ordering", ["M1-J07"], "history",
                 "Two independently opened Stores cancel in controlled order; exactly one epoch increment.")
    _setup_job(case, "running", PAIR)
    case.snapshot("before")
    cancelled = case.put_job("case", PAIR, "cancelled", 3)["public"]
    case.call("after", case.op("store", "cancel_job", "case"), cancelled)
    case.call("after", case.op("secondary_store", "cancel_job", "case"), cancelled)
    rows.append(case.finish())

    return rows


AMBIGUOUS_JSON_CASES = frozenset({
    "intake-json-duplicate-same", "intake-json-duplicate-different", "intake-json-count-over",
    "intake-json-member-bytes-over", "intake-json-total-bytes-over", "intake-json-deferred-unencodable",
})


def _phase_ambiguity(row: dict[str, Any]) -> dict[str, Any]:
    if row["case_id"] not in AMBIGUOUS_JSON_CASES:
        return row
    row["requirement_ids"] = [rid for rid in row["requirement_ids"] if rid != "M1-I29"]
    row["assertions"] = [assertion for assertion in row["assertions"] if assertion["requirement_id"] != "M1-I29"]
    deferred = deepcopy(row["expected"])
    immediate = deepcopy(deferred)
    error = deferred["results"]["after"][0]["error"]
    immediate["results"]["before"][2:] = [{"error": error}, {"error": "not_found"}, {"error": "not_found"}]
    immediate["results"]["after"] = [{"error": "not_found"}]
    for phase in PHASES:
        immediate[phase]["jobs"] = [job for job in immediate[phase]["jobs"] if job["public"]["job_id"] != "case"]
    row["expected_alternatives"] = {"deferred": deferred, "immediate": immediate}
    row["branch_discriminator"] = {"phase": "before", "result_index": 2}
    row["snapshot_semantics"] = {
        "before": "Immediately after submit and independent lookups: exact queued admission or exact refusal with no job.",
        "after": "After prepare: exact semantic failed job or not_found with no job, according to the frozen selected branch.",
        "reopened": "Same selected branch state after closing and reopening the Store.",
    }
    row["facet"] = ("Exact declared JSON error and conservation under two complete prospectively frozen admission histories; "
                    "no uniquely required admission timing is claimed.")
    row["ambiguity_disposition"] = {
        "source": "M1_REQUIREMENTS: invalid discovery/archive/JSON performs no admission; semantic source errors deferred",
        "clarification": "CLARIFY-UNSPECIFIED-ERRORS",
        "rule": "Submit error/no job OR exact queued admission then exact prepare failure; no hybrid acceptance.",
        "independent_derivation": True,
    }
    for assertion in row["assertions"]:
        assertion["scope"] = row["facet"]
    return row


@lru_cache(maxsize=1)
def _definition_bytes() -> tuple[bytes, ...]:
    from .candidate_intake_fixtures_v1 import intake_cases
    return tuple(encoded(_phase_ambiguity(row)) for row in (_store_cases() + _history_cases() + intake_cases()))


def definition_sources() -> dict[str, str]:
    root = Path(__file__).parent
    paths = ("candidate_intake_store_cases_v1.py", "candidate_intake_fixtures_v1.py",
             "candidate_intake_store_observer_v1.py", "candidate_storage_cases_v1.py")
    return {"gossip_harness/" + name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in paths}


def _check_loaded_sources() -> None:
    if definition_sources() != _LOADED_DEFINITION_SOURCES:
        raise RuntimeError("loaded B02 definition sources changed")


def definitions() -> list[dict[str, Any]]:
    _check_loaded_sources()
    return [json.loads(raw) for raw in _definition_bytes()]


@lru_cache(maxsize=1)
def _case_positions() -> dict[str, int]:
    positions = {json.loads(raw)["case_id"]: index for index, raw in enumerate(_definition_bytes())}
    if len(positions) != len(_definition_bytes()):
        raise ValueError("duplicate B02 case identifier")
    return positions


def case_definition(case_id: str) -> dict[str, Any]:
    _check_loaded_sources()
    try:
        return json.loads(_definition_bytes()[_case_positions()[case_id]])
    except KeyError as error:
        raise ValueError("unknown B02 case") from error


def select_expected_case(case_id: str, results: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    case = case_definition(case_id)
    variants = case.get("expected_alternatives")
    if variants is None:
        return case
    phase = case["branch_discriminator"]["phase"]
    index = case["branch_discriminator"]["result_index"]
    observations = results.get(phase, [])
    matched = [name for name, expected in variants.items() if isinstance(observations, list)
               and len(observations) > index
               and encoded(observations[index]) == encoded(expected["results"][phase][index])]
    case["expected_variant"] = matched[0] if len(matched) == 1 else None
    if len(matched) == 1:
        case["expected"] = deepcopy(variants[matched[0]])
    return case


def execution_recipe(case_id: str) -> dict[str, Any]:
    return case_definition(case_id)["recipe"]


@lru_cache(maxsize=1)
def _semantic_sha256() -> str:
    return hashlib.sha256(encoded({"protocol": PROTOCOL, "product_sha256": PRODUCT_SHA256,
                                  "inventory_sha256": INVENTORY_SHA256,
                                  "cases": [json.loads(raw) for raw in _definition_bytes()]})).hexdigest()


def definition_sha256() -> str:
    _check_loaded_sources()
    return _semantic_sha256()


def _matches_result(actual: Any, expected: dict[str, Any]) -> bool:
    if expected == {"error_present": True}:
        return (type(actual) is dict and set(actual) == {"error"}
                and type(actual["error"]) is str and bool(actual["error"]))
    return encoded(actual) == encoded(expected)


def _unavailable_reason(value: Any) -> str | None:
    if isinstance(value, dict):
        if "observation_unavailable" in value:
            return str(value["observation_unavailable"])
        if value.get("not_run") == "dependency_unavailable":
            return "dependency_unavailable"
        for item in value.values():
            reason = _unavailable_reason(item)
            if reason is not None:
                return reason
    elif isinstance(value, list):
        for item in value:
            reason = _unavailable_reason(item)
            if reason is not None:
                return reason
    return None


def evaluate_case(case_id: str, before: Observation, after: Observation, reopened: Observation,
                  results: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    """Typed local assertions, never an authenticated execution or acceptance."""
    if type(results) is not dict or set(results) != set(PHASES) or any(type(results[phase]) is not list for phase in PHASES):
        raise ObservationUnavailable("phase result envelope mismatch")
    case = select_expected_case(case_id, results)
    snapshots = (before, after, reopened)
    if len({value.registration_sha256 for value in snapshots}) != 1:
        raise ObservationUnavailable("snapshot registration mismatch")
    if len({(value.schema_sha256, value.layout) for value in snapshots}) != 1:
        raise ObservationUnavailable("snapshot schema/layout mismatch")
    if type(results) is not dict or set(results) != set(PHASES):
        raise ObservationUnavailable("phase result envelope mismatch")
    checks: dict[str, bool | None] = {}
    if "expected_alternatives" in case:
        checks["admission-variant"] = case.get("expected_variant") is not None
    for phase, value in zip(PHASES, snapshots):
        checks[phase + ".persisted-state"] = encoded(value.data) == encoded(case["expected"][phase])
        actual_results = results[phase]
        expected_results = case["expected"]["results"][phase]
        if type(actual_results) is not list:
            raise ObservationUnavailable("phase results are not a list")
        checks[phase + ".result-count"] = len(actual_results) == len(expected_results)
        for index, expected in enumerate(expected_results):
            checks[f"{phase}.result.{index}"] = (index < len(actual_results)
                and _matches_result(actual_results[index], expected))
    expected_jobs = {phase: {row["public"]["job_id"]: row for row in case["expected"][phase]["jobs"]}
                     for phase in PHASES}
    for phase, snapshot in zip(PHASES, snapshots):
        checks[phase + ".persisted-string-job-set"] = set(snapshot.persisted_strings) == set(expected_jobs[phase])
    for jid, expected_before in expected_jobs["before"].items():
        original = before.persisted_strings.get(jid)
        for phase, value in (("after", after), ("reopened", reopened)):
            for field in ("manifest", "content_hashes"):
                checks[phase + "." + jid + ".immutable-" + field] = bool(
                    original is not None and field in original and field in value.persisted_strings.get(jid, {})
                    and value.persisted_strings[jid][field] == original[field])
            expected_after = expected_jobs[phase].get(jid)
            if expected_after is not None and encoded(expected_before["receipt"]) == encoded(expected_after["receipt"]):
                checks[phase + "." + jid + ".receipt-conservation"] = bool(
                    original is not None and "receipt" in original and "receipt" in value.persisted_strings.get(jid, {})
                    and value.persisted_strings[jid]["receipt"] == original["receipt"])
    checks["reopened.persisted-strings"] = after.persisted_strings == reopened.persisted_strings
    # Only no-change action intervals constrain every reviewed auxiliary table.
    # Effective writes may legitimately change candidate-specific control rows.
    if encoded(case["expected"]["before"]) == encoded(case["expected"]["after"]):
        checks["after.auxiliary-conservation"] = encoded(before.auxiliary_tables) == encoded(after.auxiliary_tables)
    checks["reopened.auxiliary-conservation"] = encoded(after.auxiliary_tables) == encoded(reopened.auxiliary_tables)
    unavailable: list[dict[str, str]] = []
    unavailable_phases: list[str] = []
    boundary: tuple[int, int, str] | None = None
    for phase_index, phase in enumerate(PHASES):
        for result_index, result in enumerate(results[phase]):
            reason = _unavailable_reason(result)
            if reason is not None:
                boundary = (phase_index, result_index, reason)
                break
        if boundary is not None:
            break
    if boundary is not None:
        phase_index, result_index, reason = boundary
        unavailable_phases = list(PHASES[phase_index:])
        for check_id in checks:
            mask = False
            for index, phase in enumerate(PHASES):
                if check_id.startswith(phase + "."):
                    if index > phase_index:
                        mask = True
                    elif index == phase_index:
                        suffix = check_id[len(phase) + 1:]
                        mask = not suffix.startswith("result.") or int(suffix.split(".")[1]) >= result_index
            if check_id == "admission-variant" and phase_index == 0 and result_index <= 2:
                mask = True
            if mask:
                checks[check_id] = None
                unavailable.append({"check_id": check_id, "reason": reason})
    return {"protocol": PROTOCOL, "case_id": case_id, "requirement_facets": case["requirement_ids"],
            "checks": checks, "all_local_assertions_passed": all(value is True for value in checks.values()),
            "expected_variant": case.get("expected_variant"), "observation_unavailable": unavailable,
            "unavailable_phases": unavailable_phases,
            "authority": "unregistered-local-observation-only", "full_requirement_verdict": None}

# Cache only immutable encoded bytes. Every public accessor returns a fresh
# parsed object and refuses after an on-disk definition dependency changes.
_LOADED_DEFINITION_SOURCES = definition_sources()
CASE_IDS = tuple(_case_positions())
