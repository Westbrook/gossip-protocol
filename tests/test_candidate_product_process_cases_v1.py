"""Host definition checks only; these tests execute no candidate or subprocess."""
from __future__ import annotations

import base64
from copy import deepcopy
import hashlib
import importlib.util
import json
from pathlib import Path
import unittest

_PATH = Path(__file__).resolve().parents[1] / "gossip_harness" / "candidate_product_process_cases_v1.py"
_SPEC = importlib.util.spec_from_file_location("process_case_definitions", _PATH)
assert _SPEC is not None and _SPEC.loader is not None
cases = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(cases)


class CandidateProductProcessDefinitionTests(unittest.TestCase):
    def test_every_cli_has_removed_server_and_lifetimes_balance(self) -> None:
        for case in cases.acceptance_cases():
            with self.subTest(case=case["id"]):
                running = False
                starts = stops = 0
                for action in case["input"]["actions"]:
                    if action["op"] == "server_start":
                        self.assertFalse(running)
                        running = True
                        starts += 1
                    elif action["op"] == "server_stop":
                        self.assertTrue(running)
                        running = False
                        stops += 1
                    elif action["op"] == "http":
                        self.assertTrue(running)
                    elif action["op"] == "cli":
                        self.assertFalse(running)
                    else:
                        self.fail("undeclared action")
                self.assertFalse(running)
                self.assertEqual(starts, stops)
                self.assertGreaterEqual(starts, 1)

    def test_inputs_and_expectations_are_separate_and_definitions_fresh(self) -> None:
        original = cases.acceptance_cases()
        altered = cases.acceptance_cases()
        altered[0]["input"]["actions"][0]["op"] = "mutated"
        altered[0]["expected"]["observations"][1]["json"]["epoch"] = 99
        self.assertEqual(original, cases.acceptance_cases())
        forbidden = {"expected", "json_subset", "required_keys", "integer_ranges", "body_utf8", "stdout_json", "json_value_only"}
        def check(value: object) -> None:
            if isinstance(value, dict):
                self.assertFalse(forbidden.intersection(value))
                for child in value.values():
                    check(child)
            elif isinstance(value, list):
                for child in value:
                    check(child)
        for case in original:
            check(case["input"])
            self.assertEqual(case["milestone"], "M4")
        self.assertEqual(cases.PURPOSE, "public_product_definition")

    def test_every_action_has_one_typed_assertion_and_declared_subset_keys(self) -> None:
        values = cases.acceptance_cases()
        self.assertEqual(len(values), 8)
        self.assertEqual(len({case["id"] for case in values}), 8)
        for case in values:
            self.assertEqual(len(case["input"]["actions"]), len(case["expected"]["observations"]))
            for action, expected in zip(case["input"]["actions"], case["expected"]["observations"]):
                self.assertEqual(action["op"], expected["kind"])
                if action["op"] in {"cli", "http"}:
                    self.assertEqual(sum(key in expected for key in ("json", "json_subset", "json_value_only")), 1)
                    if "json_subset" in expected:
                        self.assertTrue(expected["required_keys"])
                        self.assertLessEqual(set(expected["json_subset"]), set(expected["required_keys"]))
                if action["op"] == "cli":
                    self.assertIn(expected["exit"], (0, 2))
                    self.assertNotIn("other_stream_empty", expected)

    def test_export_byte_boundary_is_utf8_and_not_character_count(self) -> None:
        case = next(c for c in cases.acceptance_cases() if c["id"] == "process-export-canonical-selection-and-byte-limit")
        found = []
        for action, expected in zip(case["input"]["actions"], case["expected"]["observations"]):
            if action["op"] == "http" and action["path"] == "/api/export-bundle" and action["body"]["ids"]:
                found.append((action, expected))
        success, too_small = found[:2]
        raw = success[1]["body_utf8"].encode("utf-8")
        self.assertEqual(success[0]["body"]["max_bytes"], len(raw))
        self.assertGreater(len(raw), len(success[1]["body_utf8"]))
        self.assertEqual(too_small[0]["body"]["max_bytes"], len(raw) - 1)
        self.assertEqual(too_small[1]["json"], {"error": "too_large"})
        self.assertEqual(json.loads(raw), success[1]["json"])
        self.assertFalse(raw.endswith(b"\n"))
        self.assertEqual([row["record"]["document"]["source"] for row in success[1]["json"]["documents"]], ["z.txt", "é.md"])

    def test_snapshot_seed_is_exact_frozen_bytes_before_migrate(self) -> None:
        from gossip_harness.library_m4_fixture_v1 import fixture_files
        frozen = fixture_files()
        for case in cases.acceptance_cases():
            snapshot = case["input"]["snapshot"]
            if snapshot is None:
                continue
            raw = base64.b64decode(snapshot["data"], validate=True)
            self.assertEqual(snapshot["encoding"], "base64")
            self.assertEqual(len(raw), snapshot["bytes"])
            self.assertEqual(hashlib.sha256(raw).hexdigest(), snapshot["sha256"])
            name = "v0" if "-v0-" in case["id"] else "m2"
            self.assertEqual(raw, frozen["compatibility/" + name + ".sqlite3"])
            self.assertEqual(case["input"]["actions"][0]["args"], ["migrate"])
            self.assertEqual(case["expected"]["observations"][0]["json"]["from_schema"], 0 if name == "v0" else 2)

    def test_worker_once_census_preserves_manual_and_resume_epoch(self) -> None:
        order = next(c for c in cases.acceptance_cases() if c["id"] == "process-worker-explicit-enrollment-order")
        workers = [(a, e) for a, e in zip(order["input"]["actions"], order["expected"]["observations"])
                   if a["op"] == "cli" and a["args"] == ["worker", "--once"]]
        self.assertEqual([e["json"]["processed"] for _, e in workers], ["a", "b", None])
        self.assertEqual([e["json"]["job"]["epoch"] for _, e in workers[:2]], [1, 1])
        terminal = next(c for c in cases.acceptance_cases() if c["id"] == "process-worker-terminal-retry-fencing")
        values = [e.get("json") for e in terminal["expected"]["observations"]]
        self.assertIn({"error": "stale_epoch"}, values)
        self.assertIn({"processed": "cancelled", "job": {"job_id": "cancelled", "epoch": 3,
                      "state": "completed", "total": 1, "completed": 1, "error": None}}, values)

    def test_restore_expected_fencing_does_not_rewind_tokens(self) -> None:
        case = next(c for c in cases.acceptance_cases() if c["id"] == "process-backup-restore-fences-edits-and-removed-id")
        values = [e.get("json") for e in case["expected"]["observations"]]
        self.assertIn({"restored": True, "generation": 4, "documents": 1, "jobs": 0}, values)
        self.assertIn({"error": "stale_version"}, values)
        self.assertIn({"error": "stale_generation"}, values)
        final = case["expected"]["observations"][-1]["json"]
        self.assertEqual(final["document"]["source"], "gone.txt")
        self.assertEqual(final["edit_version"], 2)
        restored = case["expected"]["observations"][-3]["json"]
        self.assertEqual(restored["edit_version"], 3)
        self.assertEqual(restored["revision"], 1)

    def test_manifest_binds_expectations_separately_from_inputs(self) -> None:
        manifest = cases.registry_manifest()
        self.assertEqual(manifest["case_count"], 8)
        for case, row in zip(cases.acceptance_cases(), manifest["cases"]):
            def digest(value: object) -> str:
                return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                    separators=(",", ":"), allow_nan=False).encode("utf-8")).hexdigest()
            self.assertEqual(row["input_sha256"], digest(case["input"]))
            self.assertEqual(row["expected_sha256"], digest(case["expected"]))
            changed = deepcopy(case["expected"])
            changed["observations"][0]["changed"] = True
            self.assertNotEqual(row["expected_sha256"], digest(changed))
        self.assertTrue(any("abrupt" in limit for limit in manifest["limitations"]))


if __name__ == "__main__":
    unittest.main()
