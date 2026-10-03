"""Frozen PUBLIC M4 compatibility inputs; no application imports or execution.

Binary SQLite snapshots are read and hash-checked, never synthesized on load.
Independent concealed histories and candidate qualification are separate work.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path
from typing import Any

FIXTURE_VERSION = "library-m4-compatibility-v1"
CONTRACT_SHA256 = "3353dbaa0c2474527cbffb0d87a4091b5f77ed3f98418fff2f216d90b6a8487c"
CONSTRUCTOR_SHA256 = "10b013d1284851ad874c23466b61ea248cd1b7f53f694de335a38efb4b493941"
_FIXTURE_DIRECTORY = Path(__file__).resolve().parents[1] / "fixtures" / FIXTURE_VERSION
_FILE_SHA256 = {
    "compatibility/README.md": "c59d7bce0ec8d40b753da1202c914aed159e66c7d36653dd50514835449ea267",
    "compatibility/m2.manifest.json": "754146fb8643f36faeb6bb76c2f3dbb354a4aed4464189eb2d68b8f4981f9644",
    "compatibility/m2.sqlite3": "4d52246ca238887d51649fbc63b923a8dfbdfd545238a83cb6c5e0a5df94c1c2",
    "compatibility/v0.manifest.json": "29bcdfd24eeac87d025926f72c4d712be161d14d70dace30c31dcef04411f81a",
    "compatibility/v0.sqlite3": "5a550a1a681d1ea0c119394906580c859af51caaf0eabd83e437513dc5b53429",
}
FIXTURE_PATHS = tuple(sorted(_FILE_SHA256))
BINARY_PATHS = ("compatibility/m2.sqlite3", "compatibility/v0.sqlite3")


def _checked_bytes(path: Path, digest: str) -> bytes:
    if path.is_symlink() or not path.is_file():
        raise ValueError("Frozen fixture must be an ordinary file")
    data = path.read_bytes()
    if hashlib.sha256(data).hexdigest() != digest:
        raise ValueError("Frozen public fixture bytes changed: " + path.name)
    return data


def fixture_files() -> dict[str, bytes]:
    """Exact five immutable compatibility paths; caller injects binary bytes."""
    return {name: _checked_bytes(_FIXTURE_DIRECTORY / Path(name).name, _FILE_SHA256[name])
            for name in FIXTURE_PATHS}


def fixture_text_files() -> dict[str, str]:
    """Only UTF-8 README/manifests, safe to combine with textual source maps."""
    return {name: raw.decode("utf-8") for name, raw in fixture_files().items() if name not in BINARY_PATHS}


def snapshot_inventory(name: str) -> dict[str, Any]:
    """Fresh expected semantic inventory authored before M4 execution."""
    if name not in ("v0", "m2"):
        raise ValueError("Unknown public compatibility snapshot")
    manifest = json.loads(fixture_files()["compatibility/" + name + ".manifest.json"])
    return dict(manifest["expected"])


def fixture_manifest() -> dict[str, Any]:
    """Source-bound byte inventory and explicit limits on evidence claims."""
    files = fixture_files()
    _checked_bytes(_FIXTURE_DIRECTORY / "build_snapshots.py", CONSTRUCTOR_SHA256)
    rows = [{"path": name, "sha256": _FILE_SHA256[name], "bytes": len(raw)} for name, raw in files.items()]
    encoded = json.dumps(rows, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return {"fixture_version": FIXTURE_VERSION, "status": "frozen_public_fixture",
            "purpose": "public_compatibility", "contract_sha256": CONTRACT_SHA256,
            "constructor_sha256": CONSTRUCTOR_SHA256, "files": rows,
            "files_sha256": hashlib.sha256(encoded).hexdigest(),
            "binary_paths": list(BINARY_PATHS),
            "independence": "Authored SQL/semantic records; no evaluated implementation imported or executed.",
            "claims_excluded": ["held_out_acceptance", "candidate_migration_qualified", "statistical_superiority"]}


_CORRUPTIONS = (
    {"id": "unknown-schema", "fixture": "v0", "error": "unsupported_schema",
     "sql": ["UPDATE metadata SET value='99' WHERE key='schema'"]},
    {"id": "missing-inherited-table", "fixture": "v0", "error": "invalid_database",
     "sql": ["DROP TABLE blobs"]},
    {"id": "wrong-content-digest", "fixture": "v0", "error": "invalid_database",
     "sql": ["UPDATE blobs SET content=X'78' WHERE blob_id=(SELECT blob_id FROM documents WHERE source='empty.txt')"]},
    {"id": "missing-history-member", "fixture": "m2", "error": "invalid_database",
     "sql": ["DELETE FROM revisions WHERE revision=2 AND document_id=(SELECT document_id FROM documents WHERE source='alpha.md')"]},
    {"id": "wrong-current-head", "fixture": "m2", "error": "invalid_database",
     "sql": ["UPDATE documents SET blob_id=(SELECT blob_id FROM documents WHERE source='empty.txt') WHERE source='alpha.md'"]},
    {"id": "invalid-edit-token", "fixture": "m2", "error": "invalid_database",
     "sql": ["UPDATE lifecycle SET edit_version=0 WHERE document_id=(SELECT document_id FROM documents WHERE source='alpha.md')"]},
    {"id": "non-normalized-tags", "fixture": "m2", "error": "invalid_database",
     "sql": ["UPDATE lifecycle SET tags='[\"UPPER\"]' WHERE document_id=(SELECT document_id FROM documents WHERE source='alpha.md')"]},
    {"id": "missing-collection", "fixture": "m2", "error": "invalid_database",
     "sql": ["DELETE FROM collections WHERE name='papers'"]},
    {"id": "missing-completed-receipt", "fixture": "m2", "error": "invalid_database",
     "sql": ["UPDATE jobs SET receipt=NULL WHERE job_id='batch-done'"]},
)


def corruption_recipes() -> tuple[dict[str, Any], ...]:
    """Public, bounded SQL mutations for fresh isolated copies, not originals."""
    return deepcopy(_CORRUPTIONS)
