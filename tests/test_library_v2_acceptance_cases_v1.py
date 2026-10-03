"""Pure definition/observer checks; never run a candidate or reference package."""
from __future__ import annotations

import ast
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from typing import Any

from gossip_harness import library_v2_acceptance_cases_v1 as cases

ROOT = Path(__file__).resolve().parents[1]


def materialize(expected: Any, observations: list[Any]) -> Any:
    if type(expected) is dict and set(expected)=={"$ref"}:
        value: Any=observations
        for part in expected["$ref"]: value=value[part]
        return deepcopy(value)
    if type(expected) is dict and set(expected)=={"$kind"}: return "a"*64
    if type(expected) is dict and set(expected)=={"$nonempty_not"}: return "new-installation"
    if type(expected) is dict: return {k:materialize(v,observations) for k,v in expected.items()}
    if type(expected) is list: return [materialize(v,observations) for v in expected]
    return deepcopy(expected)


def example_actual(case: dict[str,Any]) -> dict[str,Any]:
    observations: list[Any]=[]
    for expected in case["expected"]["observations"]:
        observations.append(materialize(expected,observations))
    return {"observations":observations}


class LibraryV2AcceptanceDefinitionTests(unittest.TestCase):
    def test_definition_pins_public_contract_and_all_six_amendments(self) -> None:
        raw=(ROOT/"library-cumulative-product-v2.json").read_bytes()
        self.assertEqual(hashlib.sha256(raw).hexdigest(),cases.CONTRACT_SHA256)
        contract=json.loads(raw)
        self.assertEqual(set(cases.AMENDMENT_IDS),{r["id"] for r in contract["amendments"]})
        manifest=cases.registry_manifest()
        self.assertTrue(all(manifest["amendment_cases"].values()))
        self.assertEqual(manifest["purpose"],"development_qualification")
        self.assertIn("public",manifest["status"])

    def test_fresh_case_records_order_census_and_inputs_exclude_expectations(self) -> None:
        values=cases.acceptance_cases()
        self.assertEqual(tuple(v["id"] for v in values),cases.CASE_IDS)
        self.assertEqual(len(set(cases.CASE_IDS)),len(cases.CASE_IDS))
        for value in values:
            self.assertEqual(set(value["input"]),{"fixture","files","actions"})
            self.assertLessEqual(len(json.dumps(value["input"]).encode()),65536)
            self.assertEqual(sum(a.get("observe",True) for a in value["input"]["actions"]),len(value["expected"]["observations"]))
        values[0]["input"]["fixture"]["control"]["incarnation"]="changed"
        self.assertEqual(cases.acceptance_cases()[0]["input"]["fixture"]["control"]["incarnation"],"portable-v2-initial")

    def test_adapter_compiles_and_has_no_host_expectations(self) -> None:
        tree=ast.parse(cases.CHILD_ADAPTER)
        compile(tree,"independent-observer","exec")
        constants={node.value for node in ast.walk(tree) if isinstance(node,ast.Constant) and isinstance(node.value,str)}
        self.assertNotIn("expected",constants)
        self.assertNotIn("passed",constants)
        self.assertNotIn("correct",constants)
        self.assertNotIn("score_case",{n.id for n in ast.walk(tree) if isinstance(n,ast.Name)})

    def test_portable_fixture_constructor_uses_real_sqlite_without_candidate(self) -> None:
        tree=ast.parse(cases.CHILD_ADAPTER)
        selected=[]
        for node in tree.body:
            if isinstance(node,ast.FunctionDef) and node.name in {"encode","make_fixture"}: selected.append(node)
            if isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id=="TABLES" for t in node.targets): selected.append(node)
        module=ast.fix_missing_locations(ast.Module(body=selected,type_ignores=[]))
        for value in cases.acceptance_cases():
            with self.subTest(case=value["id"]),tempfile.TemporaryDirectory() as directory:
                db=Path(directory)/"fixture.sqlite"
                scope:dict[str,Any]={"sqlite3":sqlite3,"json":json,"hashlib":hashlib,"db":db}
                exec(compile(module,"portable-fixture-only","exec"),scope)
                scope["make_fixture"](deepcopy(value["input"]["fixture"]))
                with sqlite3.connect(db) as connection:
                    self.assertEqual(connection.execute("PRAGMA foreign_key_check").fetchall(),[])
                    self.assertEqual(connection.execute("SELECT value FROM metadata WHERE key='schema'").fetchone()[0],str(value["input"]["fixture"]["schema"]))

    def test_host_scorer_accepts_exact_examples_rejects_missing_extra_and_type_changes(self) -> None:
        for value in cases.acceptance_cases():
            with self.subTest(case=value["id"]):
                actual=example_actual(value)
                self.assertTrue(cases.score_case(value,actual))
                self.assertFalse(cases.score_case(value,{**actual,"extra":True}))
                self.assertFalse(cases.score_case(value,{"observations":actual["observations"][:-1]}))
                wrong=deepcopy(actual);wrong["observations"][0]=None
                self.assertFalse(cases.score_case(value,wrong))
        self.assertFalse(cases._exact(1,True))
        self.assertFalse(cases._exact(1,1.0))
        self.assertFalse(cases._exact([1],(1,)))

    def test_snapshot_relation_rejects_changed_state_and_invalid_digest(self) -> None:
        value=next(c for c in cases.acceptance_cases() if c["id"]=="v2-maximum-noops-atomic-exhaustion-and-stale-precedence")
        actual=example_actual(value)
        actual["observations"][-1]="b"*64
        self.assertFalse(cases.score_case(value,actual))
        actual=example_actual(value);actual["observations"][1]="not-a-digest"
        self.assertFalse(cases.score_case(value,actual))

    def test_incarnation_predicate_requires_new_nonempty_string(self) -> None:
        value=next(c for c in cases.acceptance_cases() if c["id"]=="v2-migration-at-maximum")
        for wrong in ("portable-v2-initial","",True,4):
            actual=example_actual(value);actual["observations"][2]=[[wrong]]
            self.assertFalse(cases.score_case(value,actual))

    def test_hash_vectors_include_unicode_null_and_noncanonical_valid_spelling(self) -> None:
        value=next(c for c in cases.acceptance_cases() if c["id"]=="v2-portable-hash-spelling-preserved-migration-backup")
        rows={r["job"]["job_id"]:r for r in value["input"]["fixture"]["jobs"]}
        self.assertEqual(json.loads(rows["empty"]["content_hashes_json"]),[])
        self.assertEqual(json.loads(rows["empty-text"]["content_hashes_json"]),[hashlib.sha256(b"").hexdigest()])
        self.assertIn("\\u0065",rows["empty-text"]["content_hashes_json"])
        self.assertEqual(json.loads(rows["unicode"]["content_hashes_json"]),[hashlib.sha256("é\n".encode()).hexdigest()])
        self.assertEqual(json.loads(rows["surrogate"]["content_hashes_json"]),[None])
        self.assertEqual(json.loads(rows["suffix"]["content_hashes_json"]),[hashlib.sha256(b"encoded").hexdigest()])

    def test_last_page_registry_fault_is_after_one_hundred_valid_files(self) -> None:
        value=next(c for c in cases.acceptance_cases() if c["id"]=="v2-adoption-validates-beyond-first-page")
        self.assertEqual(len(value["input"]["fixture"]["backups"]),102)
        self.assertEqual(len(value["input"]["files"]),102)
        for file in value["input"]["files"][:-1]:
            self.assertEqual(json.loads(file["text"])["format"],"local-research-library-backup-v3")
        self.assertEqual(value["input"]["files"][-1]["text"],"{}")

    def test_manifest_fingerprints_recompute_and_limits_disclose_missing_lanes(self) -> None:
        manifest=cases.registry_manifest()
        self.assertEqual(manifest["adapter_sha256"],hashlib.sha256(cases.CHILD_ADAPTER.encode()).hexdigest())
        self.assertEqual(manifest["ordered_cases_sha256"],cases._sha(cases.acceptance_cases()))
        text=" ".join(manifest["limitations"])
        for scope in ("browser","active claim","power-loss","statistical","authentication"):
            self.assertIn(scope,text)
