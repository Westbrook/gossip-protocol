"""Public, prospectively declared B01 cases, independent of candidate output.

These finite histories qualify observation sensitivity; they do not close every
B01 ID or substitute for independent final acceptance. Expectations are derived
from the public M1/v2 contract, never from a candidate's result or stored receipt.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

PROTOCOL = "candidate-storage-cases-v1"
PRODUCT_SHA256 = "2d88ce0775888f148b0ec3caf90b3d5c82d8fed71f53bec5f7e75f492ae998dc"
CASE_IDS = ("rollback", "capacity", "invalid-admission", "deferred-semantic",
            "empty-batch", "canonical-replay", "completed-replay", "value-mutation")


def encoded(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def document(source: str, text: str) -> dict[str, Any]:
    return {"document_id": "doc-" + hashlib.sha256(b"document\0" + source.encode()).hexdigest(),
            "source_id": "src-" + hashlib.sha256(b"source\0" + source.encode()).hexdigest(),
            "source": source, "blob_id": "blob-" + hashlib.sha256(text.encode()).hexdigest(),
            "title": source.rsplit("/", 1)[-1], "text": text}


def job(manifest: list[dict[str, str]], state: str = "running", error: str | None = None) -> dict[str, Any]:
    entries = sorted(manifest, key=lambda row: (row["source"], row["text"]))
    public = {"job_id": "case", "epoch": 1, "state": state, "total": len(entries),
              "completed": len(entries) if state == "completed" else 0, "error": error}
    receipt = None if state != "completed" else {
        "job": public, "documents": [document(row["source"], row["text"]) for row in entries]}
    return {"public": public, "manifest": entries,
            "content_hashes": [hashlib.sha256(row["text"].encode()).hexdigest() for row in entries],
            "receipt": receipt}


def state(documents: list[dict[str, Any]], jobs: list[dict[str, Any]]) -> dict[str, Any]:
    blobs = {doc["blob_id"]: doc["text"].encode() for doc in documents}
    return {"documents": sorted(documents, key=lambda row: row["source"]),
            "blobs": [{"blob_id": bid, "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
                      for bid, raw in sorted(blobs.items())],
            "jobs": sorted(jobs, key=lambda row: row["public"]["job_id"])}


def definitions() -> list[dict[str, Any]]:
    keep = document("keep.txt", "keep")
    pair = [{"source": "new-a.txt", "text": "same"}, {"source": "new-b.txt", "text": "same"}]
    completed_docs = [keep] + [document(row["source"], row["text"]) for row in pair]
    empty_state = state([keep], [])
    running = state([keep], [job(pair)])
    completed = state(completed_docs, [job(pair, "completed")])
    mixed = [{"source": "ok.txt", "text": "OK"}, {"source": "../bad.txt", "text": "bad"}]
    capacity_docs = [document(str(index) + ".txt", "shared") for index in range(254)]
    capacity_docs.append(document("external.txt", "external"))
    rows = [
        ("rollback", ["M1-A01", "M1-A07"], "mutation-failure", running, running,
         {"error": "injected_failure"}),
        ("capacity", ["M1-A01", "M1-A05", "M1-T07"], "mutation-failure",
         state(capacity_docs, [job(pair)]), state(capacity_docs, [job(pair, "failed", "capacity")]),
         {"error": "capacity"}),
        ("invalid-admission", ["M1-T04"], "input-rejection", empty_state, empty_state,
         {"error": "invalid_request"}),
        ("deferred-semantic", ["M1-J06"], "deferred-semantic-failure",
         state([keep], [job(mixed, "queued")]), state([keep], [job(mixed, "failed", "invalid_source")]),
         {"error": "invalid_source"}),
        ("empty-batch", ["M1-A04"], "success",
         state([keep], [job([])]), state([keep], [job([], "completed")]), job([], "completed")["receipt"]),
        ("canonical-replay", ["M1-J04", "M1-T04"], "success",
         state([keep], [job(pair, "queued")]), state([keep], [job(pair, "queued")]), job(pair, "queued")["public"]),
        ("completed-replay", ["M1-A03"], "success",
         completed, completed, job(pair, "completed")["receipt"]),
        ("value-mutation", ["M1-T03"], "success", completed, completed, {"job": job(pair, "completed")["public"], "manifest": pair,
                                               "receipt": job(pair, "completed")["receipt"]}),
    ]
    return [{"case_id": name, "requirement_ids": requirements, "failure_phase": phase,
             "before": before, "after": after, "reopened": after, "result": result}
            for name, requirements, phase, before, after, result in rows]


def definition_sha256() -> str:
    return hashlib.sha256(encoded({"protocol": PROTOCOL, "product_sha256": PRODUCT_SHA256,
                                  "cases": definitions()})).hexdigest()
