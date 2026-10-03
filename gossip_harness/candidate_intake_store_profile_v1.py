"""Exact authored-v2 auxiliary mapping for B02 fixture qualification only.

The public cases define product expectations independently. This reviewed mapper
translates those expected transitions into the authored representation; candidate
results never define its expected rows. The sole observational parameter is the
initial random UUID, which must remain unchanged. This is neither a portable
product schema requirement nor production source-review/acceptance authority.
"""
from __future__ import annotations

import ast
import hashlib
from pathlib import Path
import re
import sqlite3
import tempfile
from typing import Any

from .candidate_intake_store_observer_v1 import (
    Observation, ObservationUnavailable, V2_SQLITE_LAYOUT, V2_STORAGE_PATHS,
    sqlite_schema_sha256,
)
from .candidate_storage_cases_v1 import encoded
from .library_v2_reference_v1 import v2_files

PROTOCOL = "candidate-intake-store-profile-v1"
PURPOSE = "authored-v2-b02-fixture-qualification-only"
PHASES = ("before", "after", "reopened")
REVIEWED_SOURCES = {
    "library/catalog/m4_store.py": "988756723389f5d77f072cfa96a5395ff9b920a4c429aa79a00df68bee0d9e2c",
    "library/catalog/m4_control.py": "3649e23a7da17653611aab1b56b6c91fce9de8761f736e0faa709e7f151bda95",
    "library/catalog/m4_legacy.py": "411820af7d03f73d10a1711b994c07f9e830c28e494a18a3e23f10fbac2ef389",
    "library/catalog/legacy_m1.py": "ae7511167063569b3688e726e1b526b96504d9d1e3cb157441969340acdfd6e5",
}
TABLES = frozenset(("metadata", "collections", "document_revisions", "document_state", "control",
                   "job_control", "document_control", "maintenance_artifacts", "backups",
                   "search_state", "search_entries"))
STORE_METHODS = frozenset(("insert", "documents", "show", "create_job", "get_job", "list_jobs",
    "job_manifest", "start_job", "fail_job", "cancel_job", "retry_job", "commit_job"))
MANAGER_METHODS = frozenset(("submit", "get", "prepare", "commit", "cancel", "retry",
    "submit_directory", "submit_zip", "submit_json"))


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def authored_schema_profile() -> tuple[bytes, dict[str, Any]]:
    """Parse pinned authored Python as data; execute only fixed schema DDL.

    No candidate Python is imported or run. Source edits invalidate this mapping
    until separately reviewed; a matching digest does not supply review authority.
    """
    sources = v2_files()
    hashes = {name: _sha(sources[name].encode()) for name in REVIEWED_SOURCES}
    if hashes != REVIEWED_SOURCES:
        raise ObservationUnavailable("authored auxiliary source profile changed")
    declarations: dict[str, Any] = {}
    for node in ast.parse(sources["library/catalog/m4_store.py"]).body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            if node.targets[0].id in ("_BASE", "_NORMALIZED"):
                declarations[node.targets[0].id] = ast.literal_eval(node.value)
    controls = [node for node in ast.walk(ast.parse(sources["library/catalog/m4_control.py"]))
                if isinstance(node, ast.FunctionDef) and node.name == "_initialize_control"]
    if len(controls) != 1:
        raise ObservationUnavailable("authored control schema seam changed")
    loops = [node for node in controls[0].body if isinstance(node, ast.For)
             and isinstance(node.target, ast.Name) and node.target.id == "statement"]
    if len(loops) != 1 or set(declarations) != {"_BASE", "_NORMALIZED"}:
        raise ObservationUnavailable("authored literal schema seam changed")
    values = ast.literal_eval(loops[0].iter)
    statements = list(declarations["_BASE"]) + list(declarations["_NORMALIZED"])
    statements += [value for value in values if value.startswith("CREATE TABLE ")]
    if len(statements) != 14 or any(type(value) is not str or not value.startswith("CREATE TABLE ")
                                    for value in statements):
        raise ObservationUnavailable("exactly fourteen authored tables required")
    for table, field, target, key, high, extra in (
        ("jobs", "epoch", "job_control", "job_id", "epoch_high_water", ",0"),
        ("document_state", "edit_version", "document_control", "document_id", "edit_high_water", ""),
    ):
        for event in ("INSERT", "UPDATE"):
            name = "fence_" + table + "_" + event.lower()
            statements.append("CREATE TRIGGER " + name + " AFTER " + event + " ON " + table +
                " BEGIN INSERT INTO " + target + " VALUES (NEW." + key + ",NEW." + field + extra + ") "
                "ON CONFLICT(" + key + ") DO UPDATE SET " + high + "=MAX(" + high + ",excluded." + high + "); END")
    with tempfile.TemporaryDirectory(prefix="authored-b02-schema-") as temporary:
        path = Path(temporary) / "schema.sqlite"
        connection = sqlite3.connect(path)
        try:
            for statement in statements:
                connection.execute(statement)
            connection.commit()
        finally:
            connection.close()
        raw = path.read_bytes()
    return raw, {"protocol": PROTOCOL, "purpose": PURPOSE,
                 "schema_sha256": sqlite_schema_sha256(raw), "statements": statements,
                 "reviewed_sources": hashes}


def _generation_by_phase(case: dict[str, Any]) -> dict[str, int]:
    """Each successful document-changing public call is one catalog transaction.

    Only prospective expected returns are used. A batch with several new documents
    advances once, replay/empty batches do not, and a failed call cannot advance.
    No edit/delete/restore/worker operations belong to this fixture profile.
    """
    generation = 0
    documents: dict[str, dict[str, Any]] = {}
    epochs: dict[str, int] = {}
    answer = {}
    for phase in PHASES:
        operations = case["recipe"]["phases"][phase]
        results = case["expected"]["results"][phase]
        if len(operations) != len(results):
            raise ObservationUnavailable("prospective operation/result count mismatch")
        for operation, result in zip(operations, results):
            special = operation.get("op")
            method = operation.get("method")
            target = operation.get("target")
            if special is not None:
                if special not in ("bind", "mutate", "import", "signature", "interfere"):
                    raise ObservationUnavailable("unreviewed auxiliary operation")
            elif not ((target in ("store", "secondary_store") and method in STORE_METHODS)
                      or (target in ("manager", "secondary_manager") and method in MANAGER_METHODS)):
                raise ObservationUnavailable("unreviewed auxiliary method")
            value = result.get("value")
            additions = []
            if (special == "import" or method == "insert") and isinstance(value, dict):
                if value.get("status") not in ("imported", "unchanged"):
                    raise ObservationUnavailable("unreviewed expected insert result")
                additions = [value["document"]]
                if (value["status"] == "imported") == (value["document"]["document_id"] in documents):
                    raise ObservationUnavailable("inconsistent expected insert status")
            elif method in ("commit_job", "commit") and isinstance(value, dict) and "job" in value:
                if value["job"]["state"] != "completed":
                    raise ObservationUnavailable("unreviewed expected commit result")
                additions = value["documents"]
            changed = False
            for document in additions:
                key = document["document_id"]
                if key in documents and encoded(documents[key]) != encoded(document):
                    raise ObservationUnavailable("document revision outside reviewed B02 profile")
                changed = changed or key not in documents
                documents[key] = document
            generation += int(changed)
        snapshot = case["expected"][phase]
        expected_docs = {row["document_id"]: row for row in snapshot["documents"]}
        if encoded(documents) != encoded(expected_docs):
            raise ObservationUnavailable("expected document history outside reviewed profile")
        current = {row["public"]["job_id"]: row["public"]["epoch"] for row in snapshot["jobs"]}
        if any(key not in current or current[key] < value for key, value in epochs.items()):
            raise ObservationUnavailable("job removal or epoch regression outside reviewed profile")
        epochs = current
        answer[phase] = generation
    return answer


def expected_auxiliary(snapshot: dict[str, Any], generation: int, incarnation: str | None) -> dict[str, list[dict[str, Any]]]:
    """Reviewed representation of a B02 snapshot, with no observed after inputs."""
    rows: dict[str, list[dict[str, Any]]] = {name: [] for name in TABLES}
    rows["metadata"] = [{"key": "schema", "value": "4"},
                        {"key": "catalog_generation", "value": str(generation)}]
    rows["control"] = [{"key": key, "value": value} for key, value in (
        ("incarnation", incarnation), ("worker_generation", "0"),
        ("last_error", "null"), ("cleanup_cursor", "null"))]
    rows["search_state"] = [{"singleton": 1, "published_generation": None, "target_generation": None,
                             "processed": 0, "total": 0, "cursor": None}]
    for document in snapshot["documents"]:
        did, bid = document["document_id"], document["blob_id"]
        rid = "rev-" + _sha(b"revision\0" + did.encode("ascii") + b"\0" + b"1\0" + bid.encode("ascii"))
        rows["document_revisions"].append({"revision_id": rid, "document_id": did, "revision": 1, "blob_id": bid})
        rows["document_state"].append({"document_id": did, "head_revision_id": rid, "edit_version": 1,
                                       "deleted": 0, "notes": "", "tags": "[]", "collections": "[]"})
        rows["document_control"].append({"document_id": did, "edit_high_water": 1})
    for job in snapshot["jobs"]:
        rows["job_control"].append({"job_id": job["public"]["job_id"],
                                    "epoch_high_water": job["public"]["epoch"], "enrolled": 0})
    return {name: sorted(values, key=encoded) for name, values in rows.items()}


def evaluate_auxiliary(case: dict[str, Any], before: Observation, after: Observation,
                       reopened: Observation, *, unavailable_phases: tuple[str, ...] = ()) -> dict[str, bool | None]:
    """Assess all 11 auxiliary tables and both lock files in all three phases.

    The 3 content tables are evaluated separately by the product-case scorer.
    Unknown schemas/layouts are unavailable, never silently ignored or failed as
    a product verdict. Callers must separately supply source review authority.
    """
    if type(unavailable_phases) is not tuple or unavailable_phases not in ((), PHASES, PHASES[1:], PHASES[2:]):
        raise ObservationUnavailable("invalid auxiliary dependency mask")
    _, profile = authored_schema_profile()
    observations = (before, after, reopened)
    if len({value.registration_sha256 for value in observations}) != 1:
        raise ObservationUnavailable("auxiliary registration mismatch")
    if any(value.layout != V2_SQLITE_LAYOUT or value.schema_sha256 != profile["schema_sha256"]
           for value in observations):
        raise ObservationUnavailable("unknown authored auxiliary mapping")
    controls = before.auxiliary_tables.get("control", [])
    incarnations = [row.get("value") for row in controls if row.get("key") == "incarnation"]
    valid = (len(incarnations) == 1 and type(incarnations[0]) is str
             and re.fullmatch(r"[0-9a-f]{12}4[0-9a-f]{3}[89ab][0-9a-f]{15}", incarnations[0]) is not None)
    incarnation = incarnations[0] if valid else None
    checks: dict[str, bool | None] = {"before.auxiliary-incarnation": valid}
    generations = _generation_by_phase(case)
    expected_files = sorted([{"path": path, "bytes": 0, "sha256": _sha(b"")}
                             for path in V2_STORAGE_PATHS if path != "catalog.sqlite"], key=encoded)
    for phase, observation in zip(PHASES, observations):
        expected = expected_auxiliary(case["expected"][phase], generations[phase], incarnation)
        checks[phase + ".auxiliary-table-census"] = set(observation.auxiliary_tables) == TABLES
        for table in sorted(TABLES):
            checks[phase + ".auxiliary." + table] = encoded(observation.auxiliary_tables.get(table)) == encoded(expected[table])
        actual_files = sorted([row for row in observation.files if row["path"] != "catalog.sqlite"], key=encoded)
        checks[phase + ".auxiliary-files"] = encoded(actual_files) == encoded(expected_files)
    # A failed qualification precondition prevents interpreting its dependent
    # state transitions. Preserve independently observed earlier failures.
    for key in checks:
        if key.split(".", 1)[0] in unavailable_phases:
            checks[key] = None
    return checks
