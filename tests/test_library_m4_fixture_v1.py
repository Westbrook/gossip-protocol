"""Independent fixture integrity checks, never evaluated application execution."""
from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unicodedata
import unittest

from gossip_harness.library_m4_fixture_v1 import (
    BINARY_PATHS, CONTRACT_SHA256, CONSTRUCTOR_SHA256, FIXTURE_PATHS, FIXTURE_VERSION,
    corruption_recipes, fixture_files, fixture_manifest, fixture_text_files, snapshot_inventory,
)

ROOT = Path(__file__).resolve().parents[1]
DIRECTORY = ROOT / "fixtures" / FIXTURE_VERSION


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def normalize(name):
    return unicodedata.normalize("NFC", unicodedata.normalize("NFC", name).strip().casefold())


class LibraryM4FixtureTests(unittest.TestCase):
    def test_declared_paths_and_byte_freeze(self):
        contract_bytes = (ROOT / "library-cumulative-product-v1.json").read_bytes()
        self.assertEqual(digest(contract_bytes), CONTRACT_SHA256)
        owned = json.loads(contract_bytes)["release_ownership"]["future_fixture_owned_paths"]
        files = fixture_files(); manifest = fixture_manifest()
        self.assertEqual(tuple(files), FIXTURE_PATHS)
        self.assertTrue(set(files) <= set(owned))
        self.assertEqual(len(files), 5)
        self.assertEqual(manifest["purpose"], "public_compatibility")
        self.assertEqual(manifest["status"], "frozen_public_fixture")
        self.assertEqual(digest((DIRECTORY / "build_snapshots.py").read_bytes()), CONSTRUCTOR_SHA256)
        for name in ("v0", "m2"):
            frozen = json.loads(files[f"compatibility/{name}.manifest.json"])
            raw = files[f"compatibility/{name}.sqlite3"]
            self.assertEqual(digest(raw), frozen["snapshot_sha256"])
            self.assertEqual(len(raw), frozen["snapshot_bytes"])
            self.assertLessEqual(len(raw), 1048576)
            self.assertEqual(frozen["constructor_sha256"], CONSTRUCTOR_SHA256)
            self.assertEqual(frozen["contract_sha256"], CONTRACT_SHA256)
        text = fixture_text_files()
        self.assertEqual(set(text), set(files) - set(BINARY_PATHS))
        self.assertEqual({k: v.encode("utf-8") for k, v in text.items()}, {k: v for k, v in files.items() if k not in BINARY_PATHS})

    def test_sqlite_graph_and_expected_inventories(self):
        originals = fixture_files()
        for name in ("v0", "m2"):
            with self.subTest(fixture=name):
                manifest = json.loads(originals[f"compatibility/{name}.manifest.json"])
                expected = snapshot_inventory(name)
                path = DIRECTORY / (name + ".sqlite3")
                with sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True) as db:
                    self.assertEqual(db.execute("PRAGMA integrity_check").fetchall(), [("ok",)])
                    self.assertEqual(db.execute("PRAGMA foreign_key_check").fetchall(), [])
                    self.assertEqual(db.execute("SELECT value FROM metadata WHERE key='schema'").fetchone()[0], str(manifest["schema"]))
                    tables = [{"name": row[0], "sql": row[1]} for row in db.execute("SELECT name,sql FROM sqlite_master WHERE type='table' ORDER BY name")]
                    self.assertEqual(tables, manifest["tables"])
                    self.assertEqual(manifest["additional_tables"], [])
                    expected_names = {"metadata", "documents", "blobs"} | ({"jobs", "lifecycle", "revisions", "collections"} if name == "m2" else set())
                    self.assertEqual({row["name"] for row in tables}, expected_names)
                    docs = db.execute("SELECT d.document_id,d.source_id,d.source,d.blob_id,d.title,b.content FROM documents d JOIN blobs b USING(blob_id) ORDER BY d.source,d.document_id").fetchall()
                    wanted = [row["document"] for row in expected["records2"]]
                    self.assertEqual([dict(zip(("document_id", "source_id", "source", "blob_id", "title", "text"), (*row[:-1], row[-1].decode("utf-8")))) for row in docs], wanted)
                    for doc in wanted:
                        source_bytes = doc["source"].encode("utf-8")
                        self.assertEqual(doc["document_id"], "doc-" + digest(b"document\0" + source_bytes))
                        self.assertEqual(doc["source_id"], "src-" + digest(b"source\0" + source_bytes))
                        self.assertEqual(doc["title"], doc["source"].split("/")[-1])
                    blobs = db.execute("SELECT blob_id,content FROM blobs ORDER BY blob_id").fetchall()
                    self.assertEqual([{"blob_id": row[0], "text": row[1].decode("utf-8"), "bytes": len(row[1])} for row in blobs], expected["blobs"])
                    for blob_id, raw in blobs:
                        self.assertEqual(blob_id, "blob-" + digest(raw))
                    for item in expected["records2"]:
                        doc_id = item["document"]["document_id"]
                        history = expected["histories2"][doc_id]
                        self.assertEqual([row["revision"] for row in history], list(range(1, item["revision"] + 1)))
                        self.assertGreaterEqual(item["edit_version"], item["revision"])
                        self.assertEqual(item["document"]["blob_id"], history[-1]["blob_id"])
                        for number, revision in enumerate(expected["histories4"][doc_id], 1):
                            raw = b"revision\0" + doc_id.encode("ascii") + b"\0" + str(number).encode("ascii") + b"\0" + history[number - 1]["blob_id"].encode("ascii")
                            self.assertEqual(revision["revision_id"], "rev-" + digest(raw))
                        if name == "m2":
                            row = db.execute("SELECT current_revision,edit_version,deleted,notes,tags,collections FROM lifecycle WHERE document_id=?", (doc_id,)).fetchone()
                            self.assertEqual(row[:4], (item["revision"], item["edit_version"], int(item["deleted"]), item["notes"]))
                            self.assertEqual(json.loads(row[4]), item["tags"])
                            self.assertEqual(json.loads(row[5]), item["collections"])
                            for field in ("tags", "collections"):
                                self.assertEqual(item[field], sorted(set(map(normalize, item[field]))))
                            actual = db.execute("SELECT revision,blob_id FROM revisions WHERE document_id=? ORDER BY revision", (doc_id,)).fetchall()
                            self.assertEqual(actual, [(rev["revision"], rev["blob_id"]) for rev in history])
                    if name == "m2":
                        self.assertEqual(db.execute("SELECT value FROM metadata WHERE key='catalog_generation'").fetchone()[0], str(expected["generation"]))
                        names = [row[0] for row in db.execute("SELECT name FROM collections ORDER BY name")]
                        self.assertEqual(names, [row["name"] for row in expected["collections"]])
                        for row in expected["records2"]:
                            self.assertTrue(set(row["collections"]) <= set(names))
                        for item in expected["jobs"]:
                            row = db.execute("SELECT job_id,epoch,state,total,completed,error,manifest,content_hashes,receipt FROM jobs WHERE job_id=?", (item["job"]["job_id"],)).fetchone()
                            self.assertEqual(row[:6], tuple(item["job"][key] for key in ("job_id", "epoch", "state", "total", "completed", "error")))
                            self.assertEqual(row[6:], tuple(item[key] for key in ("manifest_json", "content_hashes_json", "receipt_json")))
        self.assertEqual(fixture_files(), originals)

    def test_job_histories_are_original_and_all_states_are_reachable(self):
        inventory = snapshot_inventory("m2"); jobs = {row["job"]["job_id"]: row for row in inventory["jobs"]}
        self.assertEqual({row["job"]["state"] for row in jobs.values()}, {"queued", "running", "completed", "cancelled", "failed"})
        for item in jobs.values():
            public = item["job"]; entries = json.loads(item["manifest_json"])
            self.assertEqual(entries, sorted(entries, key=lambda row: (row["source"], row["text"])))
            self.assertEqual(public["total"], len(entries))
            hashes = []
            for entry in entries:
                try:
                    hashes.append(digest(entry["text"].encode("utf-8")))
                except UnicodeEncodeError:
                    hashes.append(None)
            self.assertEqual(json.loads(item["content_hashes_json"]), hashes)
            if public["state"] != "completed":
                self.assertIsNone(item["receipt_json"])
                self.assertEqual(public["completed"], 0)
                continue
            receipt = json.loads(item["receipt_json"])
            self.assertEqual(receipt["job"], public)
            self.assertEqual(public["completed"], public["total"])
            self.assertEqual([(d["source"], d["text"]) for d in receipt["documents"]], [(e["source"], e["text"]) for e in entries])
            blobs = {row["blob_id"] for row in inventory["blobs"]}
            self.assertTrue(all(doc["blob_id"] in blobs for doc in receipt["documents"]))
        original = json.loads(jobs["batch-done"]["receipt_json"])
        receipt_tombstone = next(doc for doc in original["documents"] if doc["source"] == "tombstone/旧.md")
        current = next(row for row in inventory["records2"] if row["document"]["source"] == "tombstone/旧.md")
        self.assertNotEqual(receipt_tombstone["text"], current["document"]["text"])
        self.assertTrue(current["deleted"])
        self.assertEqual(jobs["batch-done"]["job"]["epoch"], 3)
        self.assertEqual(jobs["cancelled-later"]["job"]["epoch"], 4)
        self.assertEqual(jobs["running-later"]["job"]["epoch"], 3)
        self.assertEqual(jobs["empty-done"]["job"]["total"], 0)
        self.assertEqual(json.loads(jobs["queue-surrogate"]["content_hashes_json"]), [None])
        self.assertEqual(inventory["generation"], 11)

    def test_unicode_empty_shared_bytes_and_content_aba(self):
        expected = snapshot_inventory("v0")
        docs = {row["source"]: row for row in expected["active_documents"]}
        self.assertEqual(docs["empty.txt"]["text"], "")
        self.assertEqual(docs["shared-a.txt"]["blob_id"], docs["shared-b.txt"]["blob_id"])
        self.assertNotEqual(docs["shared-a.txt"]["document_id"], docs["shared-b.txt"]["document_id"])
        self.assertIn("e\u0301 versus é", docs["unicode/猫.md"]["text"])
        expected = snapshot_inventory("m2")
        alpha = next(row for row in expected["records2"] if row["document"]["source"] == "alpha.md")
        history = expected["histories4"][alpha["document"]["document_id"]]
        self.assertEqual(history[0]["blob_id"], history[2]["blob_id"])
        self.assertNotEqual(history[0]["revision_id"], history[2]["revision_id"])
        self.assertEqual(alpha["edit_version"], 6)
        self.assertEqual(expected["counts"], {"documents": 5, "active": 4, "deleted": 1, "revisions": 8, "blobs": 5, "blob_bytes": 88, "jobs": 7})

    def test_constructor_is_independent_reproducible_and_refuses_overwrite(self):
        constructor = DIRECTORY / "build_snapshots.py"
        tree = ast.parse(constructor.read_text())
        imports = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.extend(item.name.split(".")[0] for item in node.names)
            elif isinstance(node, ast.ImportFrom):
                imports.append((node.module or "").split(".")[0])
        self.assertTrue(set(imports) <= {"__future__", "hashlib", "json", "pathlib", "sqlite3", "sys", "typing"})
        self.assertNotIn("exec(", constructor.read_text())
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run([sys.executable, "-I", str(constructor), directory], capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)
            for name in ("v0", "m2"):
                rebuilt = json.loads((Path(directory) / (name + ".manifest.json")).read_bytes())
                frozen = json.loads((DIRECTORY / (name + ".manifest.json")).read_bytes())
                # File layout can legitimately differ between SQLite versions;
                # only the original frozen bytes are migration study inputs.
                if rebuilt["constructor_runtime"]["sqlite"] == frozen["constructor_runtime"]["sqlite"]:
                    self.assertEqual((Path(directory) / (name + ".sqlite3")).read_bytes(), (DIRECTORY / (name + ".sqlite3")).read_bytes())
                self.assertEqual(rebuilt["expected"], snapshot_inventory(name))
                with sqlite3.connect(f"file:{Path(directory) / (name + '.sqlite3')}?mode=ro&immutable=1", uri=True) as db:
                    self.assertEqual(db.execute("PRAGMA integrity_check").fetchall(), [("ok",)])
                    self.assertEqual(db.execute("PRAGMA foreign_key_check").fetchall(), [])
                    self.assertEqual(db.execute("SELECT count(*) FROM documents").fetchone()[0], rebuilt["expected"]["counts"]["documents"])
            before = {p.name: p.read_bytes() for p in Path(directory).iterdir()}
            second = subprocess.run([sys.executable, "-I", str(constructor), directory], capture_output=True, text=True, timeout=10)
            self.assertNotEqual(second.returncode, 0)
            self.assertEqual({p.name: p.read_bytes() for p in Path(directory).iterdir()}, before)

    def test_corruption_recipes_are_bounded_and_never_change_originals(self):
        originals = fixture_files()
        recipes = corruption_recipes()
        self.assertEqual(len({row["id"] for row in recipes}), len(recipes))
        self.assertEqual(len(recipes), 9)
        for recipe in recipes:
            with self.subTest(case=recipe["id"]), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "copy.sqlite3"
                before = originals[f"compatibility/{recipe['fixture']}.sqlite3"]
                path.write_bytes(before)
                self.assertIn(recipe["error"], ("unsupported_schema", "invalid_database"))
                self.assertTrue(1 <= len(recipe["sql"]) <= 2)
                with sqlite3.connect(path) as db:
                    db.execute("PRAGMA foreign_keys=OFF")
                    for statement in recipe["sql"]:
                        db.execute(statement)
                self.assertNotEqual(path.read_bytes(), before)
        self.assertEqual(fixture_files(), originals)
        recipes[0]["sql"].clear()
        self.assertTrue(corruption_recipes()[0]["sql"])
        mutated = snapshot_inventory("m2"); mutated["records2"].clear()
        self.assertEqual(len(snapshot_inventory("m2")["records2"]), 5)
        with self.assertRaises(ValueError):
            snapshot_inventory("unknown")
