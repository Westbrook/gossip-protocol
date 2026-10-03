"""Check the prospective v2 declaration, never candidate/product correctness."""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
PREDECESSOR_SHA256 = "3353dbaa0c2474527cbffb0d87a4091b5f77ed3f98418fff2f216d90b6a8487c"
AMENDMENTS = {
    "V2-MIGRATION-FENCE", "V2-MIGRATION-DIAGNOSTIC", "V2-COUNTER-DOMAIN",
    "V2-M1-HASH-SERIALIZATION", "V2-WORKER-LIVENESS", "V2-BACKUP-ROOT",
}


def read_contract(name):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("Duplicate JSON key: " + key)
            result[key] = value
        return result
    return json.loads((ROOT / name).read_text(), object_pairs_hook=unique)


def pointer(value, path):
    for key in path.strip("/").split("/"):
        value = value[int(key)] if isinstance(value, list) else value[key]
    return value


def assign(value, path, replacement):
    parts = path.strip("/").split("/")
    for key in parts[:-1]:
        value = value[int(key)] if isinstance(value, list) else value[key]
    key = parts[-1]
    if isinstance(value, list):
        index = int(key)
        if index == len(value):
            value.append(copy.deepcopy(replacement))
        else:
            value[index] = copy.deepcopy(replacement)
    else:
        value[key] = copy.deepcopy(replacement)


def validate_revision(old, new):
    """Reject undeclared drift, lost obligations and unowned semantic edits."""
    revision = new["revision"]
    amendment_ids = {row["id"] for row in new["amendments"]}
    if amendment_ids != AMENDMENTS or len(new["amendments"]) != len(AMENDMENTS):
        raise ValueError("Incomplete or duplicate amendment set")
    if [r["id"] for r in old["requirements"]] != [r["id"] for r in new["requirements"]]:
        raise ValueError("Changed requirement inventory")
    result = copy.deepcopy(old)
    for path, owners in revision["semantic_change_map"].items():
        if not owners or len(owners) != len(set(owners)) or not set(owners) <= amendment_ids:
            raise ValueError("Unowned semantic change")
        assign(result, path, pointer(new, path))
    for path in revision["metadata_change_paths"]:
        assign(result, path, pointer(new, path))
    for section in revision["added_normative_sections"]:
        result[section] = copy.deepcopy(new[section])
    result["revision"] = copy.deepcopy(revision)
    if result != new:
        raise ValueError("Unlisted contract change")
    for before, after in zip(old["requirements"], new["requirements"]):
        for key in ("id", "milestone", "title", "mandatory", "status", "packages",
                    "depends_on", "evidence_lanes"):
            if before[key] != after[key]:
                raise ValueError("Changed inherited requirement scope")
    for section in ("inherited_contract", "dependency_graph", "release_ownership",
                    "acceptance_boundary"):
        if old[section] != new[section]:
            raise ValueError("Changed inherited scope or evidence boundary")


class ProspectiveProductContractV2Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.old = read_contract("library-cumulative-product-v1.json")
        cls.new = read_contract("library-cumulative-product-v2.json")

    def test_predecessor_is_exact_and_v2_is_not_execution_evidence(self):
        self.assertEqual(hashlib.sha256((ROOT / "library-cumulative-product-v1.json")
                                       .read_bytes()).hexdigest(), PREDECESSOR_SHA256)
        revision = self.new["revision"]
        self.assertEqual(revision["superseded_sha256"], PREDECESSOR_SHA256)
        self.assertEqual(revision["superseded_path"], "library-cumulative-product-v1.json")
        self.assertIs(revision["frozen_m1_pilot_affected"], False)
        self.assertEqual(self.new["status"], "prospective-unqualified")
        self.assertEqual(self.new["schema_version"], 2)
        self.assertEqual(self.new["contract_id"], "library-cumulative-product-v2")
        for flag in ("no_execution_claim", "no_private_cases", "no_new_spending_authority"):
            self.assertIs(self.new["freeze_policy"][flag], True)
        self.assertTrue(self.new["freeze_policy"]["qualification_missing"])
        for name, expected in self.new["inherited_contract"]["source_sha256"].items():
            self.assertEqual(hashlib.sha256((ROOT / name).read_bytes()).hexdigest(), expected)

    def test_all_inherited_scope_and_declared_semantic_changes_are_preserved(self):
        validate_revision(self.old, self.new)
        self.assertEqual(len(self.new["requirements"]), 17)
        self.assertEqual(self.new["revision"]["unchanged_requirements"],
                         [row["id"] for row in self.old["requirements"]])
        self.assertEqual({name for owners in self.new["revision"]["semantic_change_map"]
                          .values() for name in owners}, AMENDMENTS)

    def test_revision_guard_rejects_undeclared_changes_and_lost_obligations(self):
        changes = []
        changed = copy.deepcopy(self.new)
        changed["constants"]["max_documents"] = 100
        changes.append(changed)
        changed = copy.deepcopy(self.new)
        changed["requirements"].pop()
        changes.append(changed)
        changed = copy.deepcopy(self.new)
        changed["requirements"][0]["evidence_lanes"] = ["public-contract"]
        changes.append(changed)
        changed = copy.deepcopy(self.new)
        next(iter(changed["revision"]["semantic_change_map"].values())).append("invented")
        changes.append(changed)
        for value in changes:
            with self.subTest(change=changes.index(value)), self.assertRaises(ValueError):
                validate_revision(self.old, value)

    def test_six_amendments_have_closed_shapes_and_existing_requirement_links(self):
        rules = self.new["registry_rules"]
        requirements = {row["id"] for row in self.new["requirements"]}
        linked = set()
        for amendment in self.new["amendments"]:
            self.assertEqual(set(amendment), set(rules["amendment_record_keys"]))
            self.assertTrue(amendment["clauses"])
            self.assertTrue(amendment["required_observations"])
            self.assertTrue(amendment["requirement_ids"])
            self.assertLessEqual(set(amendment["requirement_ids"]), requirements)
            linked.update(amendment["requirement_ids"])
        self.assertGreaterEqual(len(linked), 10)
        self.assertEqual(set(self.new["acceptance_boundary"]["mandatory_requirement_ids"]),
                         requirements)

    def test_full_prose_contains_each_normative_requirement_and_amendment(self):
        prose = (ROOT / "docs/library-cumulative-product-v2.md").read_text()
        for row in self.new["requirements"] + self.new["amendments"]:
            with self.subTest(record=row["id"]):
                self.assertIn(row["id"], prose)
                for text in row["clauses"]:
                    self.assertIn(text, prose)
        self.assertIn(PREDECESSOR_SHA256, prose)
        self.assertIn("prospective-unqualified", prose)

    def test_counter_domains_do_not_round_or_silently_narrow_legacy_offsets(self):
        domains = self.new["counter_domains"]
        self.assertIs(type(domains["maximum"]), int)
        self.assertEqual(domains["maximum"], (1 << 63) - 1)
        self.assertEqual(domains["maximum"], self.new["constants"]["counter_max"])
        self.assertGreater(domains["maximum"], (1 << 53) - 1)
        self.assertFalse(set(domains["positive"]) & set(domains["nonnegative"]))
        self.assertLessEqual(set(domains["nullable"]), set(domains["nonnegative"]))
        self.assertIs(domains["legacy_offset_unchanged"], True)
        self.assertIs(domains["browser_lossless_required"], True)
        self.assertEqual(domains["overflow_error"], "counter_exhausted")
        self.assertEqual(domains["overflow_http_status"], 409)
        self.assertEqual(domains["bad_request_error"], "invalid_request")
        self.assertEqual(domains["bad_database_error"], "invalid_database")
        self.assertEqual(domains["bad_backup_error"], "invalid_backup")

    def test_public_hash_vectors_pin_utf8_digests_and_deferred_null(self):
        rules = self.new["registry_rules"]["hash_vector_record_keys"]
        vectors = self.new["portable_m1_hash_vectors"]
        self.assertEqual(len({v["id"] for v in vectors}), len(vectors))
        self.assertEqual({v["id"] for v in vectors},
                         {"empty-manifest", "empty-text", "unicode-newline",
                          "deferred-invalid-unicode"})
        saw_null = False
        for row in vectors:
            self.assertEqual(set(row), set(rules))
            expected = []
            for text in row["texts"]:
                try:
                    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
                except UnicodeError:
                    digest = None
                    saw_null = True
                expected.append(digest)
            self.assertEqual(row["expected_hashes"], expected)
            self.assertEqual(json.loads(row["content_hashes_json"]), expected)
            self.assertEqual(len(expected), len(row["texts"]))
            for digest in expected:
                if digest is not None:
                    self.assertRegex(digest, r"\A[0-9a-f]{64}\Z")
        self.assertTrue(saw_null)
        # Equivalent escaping changes serialized bytes, not the pinned digest.
        row = next(v for v in vectors if v["id"] == "empty-text")
        escaped = row["content_hashes_json"].replace("e", "\\u0065", 1)
        self.assertNotEqual(escaped, row["content_hashes_json"])
        self.assertEqual(json.loads(escaped), row["expected_hashes"])

    def test_worker_liveness_matrix_is_total_and_does_not_infer_owner_from_claim(self):
        rows = self.new["worker_state_table"]
        self.assertEqual(len(rows), 4)
        mapping = {}
        for row in rows:
            self.assertEqual(set(row), set(self.new["registry_rules"]["worker_state_record_keys"]))
            self.assertIs(type(row["live_matching_owner"]), bool)
            self.assertIs(type(row["matching_active_claim"]), bool)
            key = row["live_matching_owner"], row["matching_active_claim"]
            self.assertNotIn(key, mapping)
            mapping[key] = row["state"]
        self.assertEqual(mapping, {(False, False): "stopped", (False, True): "stopped",
                                   (True, False): "idle", (True, True): "running"})

    def test_adoption_is_process_only_and_retains_the_single_name_keyed_registry(self):
        before, after = self.old["interfaces"]["m3"], self.new["interfaces"]["m3"]
        self.assertEqual(after[:-1], before)
        row = after[-1]
        self.assertEqual(set(row), set(self.new["registry_rules"]["interface_record_keys"]))
        self.assertIn("process-only", row["http"])
        self.assertIn("--expect-unbound", row["cli"])
        self.assertIn("--expect-root CURRENT", row["cli"])
        self.assertIn("explicit --backup-dir required", row["cli"])
        root = self.new["persistence"]["backup_root"]
        self.assertEqual(root["mode"], "one durable active root")
        self.assertIn("name PRIMARY KEY", root["registry"])
        self.assertEqual(root["errors"]["unbound"], "backup_root_unbound")
        self.assertEqual(root["errors"]["selected_or_expected_mismatch"], "backup_root_mismatch")
        self.assertEqual(root["errors"]["pending_or_live_owner"], "maintenance_busy")
        self.assertEqual(self.new["persistence"]["schema3"][3],
                         self.old["persistence"]["schema3"][3])

    def test_retained_feature_graph_remains_closed_and_has_frontier_three(self):
        rows = {r["id"]: r for r in self.new["dependency_graph"]["slots"]}
        self.assertEqual(len(rows), 17)
        self.assertEqual(set(rows), {r["id"] for r in self.new["requirements"]})
        pending, visited, maximum = [frozenset()], set(), 0
        while pending:
            done = pending.pop()
            if done in visited:
                continue
            visited.add(done)
            ready = {name for name, row in rows.items() if name not in done
                     and set(row["depends_on_slots"]) <= done}
            maximum = max(maximum, len(ready))
            pending.extend(done | {name} for name in ready)
        self.assertIn(frozenset(rows), visited)
        self.assertEqual(maximum, 3)
        self.assertEqual(maximum, self.new["dependency_graph"]
                         ["expected_max_requirement_ready_frontier"])


if __name__ == "__main__":
    unittest.main()
