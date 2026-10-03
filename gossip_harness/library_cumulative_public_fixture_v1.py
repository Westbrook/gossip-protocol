"""Separately versioned public cumulative adapter and bounded release checks.

This is public development evidence, never concealed acceptance or an alternate
solver. Definitions are authored from the normative contract and independently
constructed public compatibility fixtures, before executing the M4 reference.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path
from textwrap import dedent
from typing import Any

FIXTURE_VERSION = "local-research-library-cumulative-public-v1"
WORKFLOW_FORMAT = "local-research-library-public-workflow-v1"
CONTRACT_SHA256 = "3353dbaa0c2474527cbffb0d87a4091b5f77ed3f98418fff2f216d90b6a8487c"
PUBLIC_PATHS = ("cumulative-public-contract.json", "test_cumulative_public.py")


def workflow_contract() -> dict[str, Any]:
    return {
        "format": WORKFLOW_FORMAT, "max_operations": 64, "max_json_bytes": 61440,
        "asset_paths": ["release/dataset/welcome.txt", "release/dataset/notes.md",
                        "release/dataset/literal.html"],
        "operation_fields": {
            "import": ["op", "source", "path"],
            "submit": ["op", "job_id", "entries"],
            "prepare": ["op", "job_id"], "commit": ["op", "job_id", "epoch"],
            "annotate": ["op", "source", "expected_version", "notes", "tags", "collections"],
            "refresh": ["op", "source", "expected_version", "text|path"],
        },
        "entry_fields": ["source", "path"],
        "semantics": [
            "Run only real persistent Store, Service, importer and JobManager APIs.",
            "Source keys identify logical documents; import paths identify immutable release assets.",
            "Import copies asset bytes under a fresh isolated input root; originals remain unchanged.",
            "Submit reads declared asset bytes into an immutable real M1 job manifest.",
            "Refresh path reads a declared release asset copied beneath the configured root.",
            "Annotations and refresh use explicit expected_version, never a silently refreshed token.",
            "Two executions use separate fresh roots and databases; no hidden expected cases are installed.",
        ],
    }


PUBLIC_TEST_SOURCE = dedent(r'''
    """Public positive cumulative checks; not exhaustive or independent acceptance.

    Run python test_cumulative_public.py from an extracted release. Each database
    is disposable and every migration starts from a copy of an immutable input.
    """
    import hashlib
    import json
    from pathlib import Path
    import shutil
    import sqlite3
    import subprocess
    import sys
    import tempfile
    import unittest

    ROOT = Path(__file__).resolve().parent
    WORKFLOW_FORMAT = "local-research-library-public-workflow-v1"
    ASSETS = frozenset(("release/dataset/welcome.txt", "release/dataset/notes.md",
                        "release/dataset/literal.html"))

    def canonical(value):
        return json.dumps(value, ensure_ascii=False, sort_keys=True,
                          separators=(",", ":"), allow_nan=False).encode("utf-8")

    def strict_json(raw):
        def pairs(items):
            result = {}
            for key, value in items:
                if key in result:
                    raise ValueError("duplicate JSON key")
                result[key] = value
            return result
        return json.loads(raw, object_pairs_hook=pairs,
                          parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))

    def asset_bytes(path):
        if path not in ASSETS:
            raise ValueError("undeclared workflow asset")
        target = ROOT
        for part in path.split("/"):
            target = target / part
            if target.is_symlink():
                raise ValueError("symlink workflow asset")
        with target.open("rb") as handle:
            raw = handle.read(32769)
        if len(raw) > 32768:
            raise ValueError("oversized workflow asset")
        raw.decode("utf-8")
        return raw

    def validate_workflow(value):
        if type(value) is not dict or set(value) != {"format", "operations"}:
            raise ValueError("workflow envelope")
        if value["format"] != WORKFLOW_FORMAT or type(value["operations"]) is not list:
            raise ValueError("workflow format")
        if len(value["operations"]) > 64 or len(canonical(value)) > 61440:
            raise ValueError("workflow bound")
        fields = {
            "import": {"op", "source", "path"}, "submit": {"op", "job_id", "entries"},
            "prepare": {"op", "job_id"}, "commit": {"op", "job_id", "epoch"},
            "annotate": {"op", "source", "expected_version", "notes", "tags", "collections"},
        }
        def source(key):
            if (type(key) is not str or not key or "\\" in key or "\x00" in key
                    or any(part in ("", ".", "..") for part in key.split("/"))
                    or len(key.encode("utf-8")) > 256
                    or any(len(part.encode("utf-8")) > 128 for part in key.split("/"))):
                raise ValueError("workflow source")
        for op in value["operations"]:
            if type(op) is not dict or type(op.get("op")) is not str:
                raise ValueError("workflow operation")
            kind = op["op"]
            if kind == "refresh":
                if set(op) not in ({"op", "source", "expected_version", "text"},
                                   {"op", "source", "expected_version", "path"}):
                    raise ValueError("refresh shape")
            elif kind not in fields or set(op) != fields[kind]:
                raise ValueError("workflow operation shape")
            if "source" in op:
                source(op["source"])
            if "path" in op:
                if type(op["path"]) is not str or op["path"] not in ASSETS:
                    raise ValueError("workflow asset")
            if "job_id" in op:
                job = op["job_id"]
                if (type(job) is not str or not 1 <= len(job) <= 64
                        or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for c in job)):
                    raise ValueError("workflow job ID")
            for key in ("epoch", "expected_version"):
                if key in op and (type(op[key]) is not int or op[key] < 1):
                    raise ValueError("workflow explicit token")
            if kind == "submit":
                if type(op["entries"]) is not list or len(op["entries"]) > 64:
                    raise ValueError("workflow entries")
                for entry in op["entries"]:
                    if type(entry) is not dict or set(entry) != {"source", "path"}:
                        raise ValueError("workflow entry shape")
                    source(entry["source"])
                    if type(entry["path"]) is not str or entry["path"] not in ASSETS:
                        raise ValueError("workflow entry asset")
            if kind == "annotate":
                if type(op["notes"]) is not str or any(
                        type(op[key]) is not list or any(type(x) is not str for x in op[key])
                        for key in ("tags", "collections")):
                    raise ValueError("workflow annotations")
            if "text" in op and type(op["text"]) is not str:
                raise ValueError("workflow text")
        return value

    def execute_workflow(workflow, directory):
        """Run real application operations; does not compute expected answers."""
        validate_workflow(workflow)
        from library.catalog.store import Store
        from library.common import identity
        from library.ingestion.local import import_file
        from library.ingestion.jobs import JobManager
        from library.query.service import Service
        home = Path(directory)
        home.mkdir(exist_ok=False)
        root = home / "input"
        root.mkdir()
        backups = home / "backups"
        backups.mkdir()
        store = Store(home / "library.sqlite3")
        service = Service(store, root, backup_dir=backups)
        jobs = JobManager(store)
        results = []
        def copy_asset(source, path):
            target = root.joinpath(*source.split("/"))
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(asset_bytes(path))
        try:
            for index, op in enumerate(workflow["operations"]):
                kind = op["op"]
                if kind == "import":
                    copy_asset(op["source"], op["path"])
                    result = import_file(store, root, op["source"])
                elif kind == "submit":
                    result = jobs.submit(op["job_id"], [
                        {"source": entry["source"], "text": asset_bytes(entry["path"]).decode("utf-8")}
                        for entry in op["entries"]])
                elif kind == "prepare":
                    result = jobs.prepare(op["job_id"])
                elif kind == "commit":
                    result = jobs.commit({"job_id": op["job_id"], "epoch": op["epoch"]})
                elif kind == "annotate":
                    result = service.replace_annotations(identity("document", op["source"]),
                        op["expected_version"], op["notes"], op["tags"], op["collections"])
                else:
                    kwargs = {"text": op["text"]} if "text" in op else {}
                    if "path" in op:
                        relative = "workflow-assets/%d.txt" % index
                        copy_asset(relative, op["path"])
                        kwargs["path"] = relative
                    result = service.refresh_document(identity("document", op["source"]),
                                                       op["expected_version"], **kwargs)
                results.append(result)
            final = {
                "records": service.list_v1(deleted="all"),
                "legacy": service.export(), "jobs": store.list_jobs(),
                "histories": {record["document_id"]: service.revisions_v1(record["document_id"])
                              for record in service.list_v1(deleted="all")["records"]},
            }
        finally:
            store.close()
        store = Store(home / "library.sqlite3")
        try:
            reopened = Service(store, root, backup_dir=backups)
            if reopened.list_v1(deleted="all") != final["records"]:
                raise AssertionError("workflow changed on durable reopen")
            if store.list_jobs() != final["jobs"]:
                raise AssertionError("workflow jobs changed on durable reopen")
        finally:
            store.close()
        return {"results": results, "final": final}

    def cli(database, root, *arguments):
        code = "import runpy,sys;sys.path.insert(0,sys.argv.pop(1));runpy.run_module('library',run_name='__main__')"
        process = subprocess.run([sys.executable, "-I", "-c", code, str(ROOT),
            "--db", str(database), "--root", str(root), *arguments],
            cwd=ROOT, capture_output=True, timeout=15, check=False)
        if process.returncode != 0 or process.stderr:
            raise AssertionError((process.returncode, process.stderr.decode("utf-8", "replace")))
        return strict_json(process.stdout)

    class CumulativePublicTests(unittest.TestCase):
        def test_public_integrity(self):
            contract = strict_json((ROOT / "cumulative-public-contract.json").read_bytes())
            self.assertEqual(contract["status"], "unqualified_definition")
            self.assertEqual(hashlib.sha256(contract["normative_contract_utf8"].encode()).hexdigest(),
                             contract["product_contract_sha256"])
            self.assertEqual(hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                             contract["adapter_sha256"])
            for record in contract["compatibility_files"]:
                raw = (ROOT / record["path"]).read_bytes()
                self.assertEqual(len(raw), record["bytes"])
                self.assertEqual(hashlib.sha256(raw).hexdigest(), record["sha256"])

        def test_frozen_migrations_and_original_receipts(self):
            from library.catalog.store import Store
            from library.ingestion.jobs import JobManager
            from library.query.service import Service
            for name in ("v0", "m2"):
                with self.subTest(fixture=name), tempfile.TemporaryDirectory() as temporary:
                    original = ROOT / "compatibility" / (name + ".sqlite3")
                    before = original.read_bytes()
                    manifest = strict_json((ROOT / "compatibility" / (name + ".manifest.json")).read_bytes())
                    expected = manifest["expected"]
                    home = Path(temporary)
                    database = home / "copy.sqlite3"
                    shutil.copyfile(original, database)
                    root = home / "input"
                    root.mkdir()
                    self.assertEqual(cli(database, root, "migrate"), expected["migration_response"])
                    store = Store(database)
                    try:
                        service = Service(store, root)
                        self.assertEqual(service.lifecycle_list(deleted="all"), {
                            "records": expected["records2"], "total": len(expected["records2"]),
                            "generation": expected["generation"]})
                        self.assertEqual(service.list_v1(deleted="all"), {
                            "records": expected["records4"], "total": len(expected["records4"]),
                            "generation": expected["generation"]})
                        self.assertEqual(service.export(), {"format": "local-research-library-v0",
                                                           "documents": expected["active_documents"]})
                        for document_id, history in expected["histories2"].items():
                            self.assertEqual(service.revision_history(document_id), {"document_id": document_id, "revisions": history})
                            self.assertEqual(service.revisions_v1(document_id), {"document_id": document_id,
                                "revisions": expected["histories4"][document_id]})
                        self.assertEqual(store.list_jobs(), [row["job"] for row in expected["jobs"]])
                        manager = JobManager(store)
                        for row in expected["jobs"]:
                            if row["job"]["state"] == "completed":
                                self.assertEqual(manager.commit({"job_id": row["job"]["job_id"],
                                    "epoch": row["job"]["epoch"]}), strict_json(row["receipt_json"]))
                    finally:
                        store.close()
                    with sqlite3.connect(database) as db:
                        raw_jobs = db.execute("SELECT job_id,manifest,content_hashes,receipt FROM jobs ORDER BY job_id").fetchall()
                    self.assertEqual(raw_jobs, [(row["job"]["job_id"], row["manifest_json"],
                        row["content_hashes_json"], row["receipt_json"]) for row in expected["jobs"]])
                    self.assertEqual(cli(database, root, "migrate"), expected["repeat_migration_response"])
                    self.assertEqual(cli(database, root, "documents-v1", "--deleted", "all"), {
                        "records": expected["records4"], "total": len(expected["records4"]),
                        "generation": expected["generation"]})
                    self.assertEqual(original.read_bytes(), before)

        def test_representative_workflow_repeat(self):
            path = ROOT / "release/dataset/workflow.json"
            with path.open("rb") as handle:
                raw = handle.read(61441)
            self.assertLessEqual(len(raw), 61440)
            workflow = validate_workflow(strict_json(raw))
            originals = {path: asset_bytes(path) for path in ASSETS}
            with tempfile.TemporaryDirectory() as temporary:
                first = execute_workflow(workflow, Path(temporary) / "one")
                second = execute_workflow(workflow, Path(temporary) / "two")
                self.assertEqual(first, second)
            self.assertEqual({path: asset_bytes(path) for path in ASSETS}, originals)
            self.assertEqual(originals["release/dataset/welcome.txt"], originals["release/dataset/notes.md"])
            self.assertEqual(len(workflow["operations"]), 7)
            self.assertEqual([op["op"] for op in workflow["operations"]],
                             ["import", "import", "submit", "prepare", "commit", "annotate", "refresh"])
            imported = first["results"][:2]
            self.assertEqual(imported[0]["document"]["blob_id"], imported[1]["document"]["blob_id"])
            self.assertNotEqual(imported[0]["document"]["document_id"], imported[1]["document"]["document_id"])
            records = {r["source"]: r for r in first["final"]["records"]["records"]}
            self.assertEqual(set(records), {"welcome.txt", "notes.md", "literal.html"})
            self.assertEqual(records["welcome.txt"]["edit_version"], 3)
            self.assertEqual(records["welcome.txt"]["current_revision"]["revision"], 2)
            self.assertEqual(records["welcome.txt"]["current_revision"]["text"], workflow["operations"][6]["text"])
            self.assertEqual(records["welcome.txt"]["notes"], workflow["operations"][5]["notes"])
            self.assertEqual(records["welcome.txt"]["tags"], workflow["operations"][5]["tags"])
            self.assertEqual(records["literal.html"]["current_revision"]["text"],
                             originals["release/dataset/literal.html"].decode("utf-8"))

    if __name__ == "__main__":
        unittest.main()
''').lstrip()


def public_manifest() -> dict[str, Any]:
    """Return definitions and byte identities, with no execution claim."""
    from gossip_harness.library_m4_fixture_v1 import fixture_files

    raw = (Path(__file__).resolve().parents[1] / "library-cumulative-product-v1.json").read_bytes()
    if hashlib.sha256(raw).hexdigest() != CONTRACT_SHA256:
        raise ValueError("normative contract changed; version the cumulative public fixture")
    contract = json.loads(raw)
    files = fixture_files()
    return {
        "format": FIXTURE_VERSION, "status": "unqualified_definition",
        "purpose": "public_development", "classification": "public positive regression; not held-out evidence",
        "product_contract_sha256": CONTRACT_SHA256, "normative_contract_utf8": raw.decode("utf-8"),
        "runtime": deepcopy(contract["inherited_contract"]),
        "ownership": deepcopy(contract["release_ownership"]),
        "frozen_m1_adapter_and_allowed_paths": "unchanged; these are separate cumulative fixture extensions",
        "workflow": workflow_contract(),
        "compatibility_files": [{"path": path, "bytes": len(value),
                                 "sha256": hashlib.sha256(value).hexdigest()}
                                for path, value in sorted(files.items())],
        "adapter_sha256": hashlib.sha256(PUBLIC_TEST_SOURCE.encode("utf-8")).hexdigest(),
        "public_checks": ["test_public_integrity", "test_frozen_migrations_and_original_receipts",
                          "test_representative_workflow_repeat"],
        "coverage": ["public v0/M2 positive migration inventories", "numbered and revision-ID projections",
                     "original completed receipt replay and raw serialization retention",
                     "migration repeat and real CLI projection", "isolated real-module dataset workflow and reopen"],
        "coverage_gaps": ["exhaustive M2-M4 public boundary and invalid-input cases", "HTTP wire and browser",
                          "abrupt process crash and recovery", "independent concealed acceptance",
                          "whole-cohort provenance and promotion barriers", "six comparative trajectories",
                          "statistical inference and independent repeatability"],
        "execution": None,
    }


def public_files() -> dict[str, str]:
    return {"cumulative-public-contract.json": json.dumps(public_manifest(), ensure_ascii=False,
             sort_keys=True, indent=2, allow_nan=False) + "\n",
            "test_cumulative_public.py": PUBLIC_TEST_SOURCE}
