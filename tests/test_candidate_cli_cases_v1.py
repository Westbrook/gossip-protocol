"""Host-only definition checks; no candidate, subprocess, Docker or API calls."""
from __future__ import annotations

import base64
from collections import Counter
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from gossip_harness import candidate_cli_cases_v1 as cases


class CandidateCliV1DefinitionTests(unittest.TestCase):
    def test_closed_roster_and_order(self) -> None:
        rows = cases.definitions()
        self.assertEqual(len(rows), 57)
        self.assertEqual(len({r["case_id"] for r in rows}), 57)
        self.assertEqual(Counter(r["family_id"] for r in rows), cases.FAMILY_COUNTS)
        self.assertEqual(rows[0]["case_id"], "cli-empty")
        self.assertEqual(rows[-1]["case_id"], "cli-cancel-retry-epoch-fence")
        self.assertEqual(sum(len(r["recipe"]["steps"]) for r in rows), 305)
        self.assertEqual(max(len(r["recipe"]["steps"]) for r in rows), 16)

    def test_recipes_never_include_expected_answers(self) -> None:
        for row in cases.definitions():
            recipe = cases.execution_recipe(row["case_id"])
            self.assertEqual(set(recipe), {"case_id", "fixtures", "directories", "steps"})
            for step in recipe["steps"]:
                self.assertEqual(set(step), {"step_id", "argv"})
                self.assertEqual(step["argv"][:4], ["python", "-m", "library", "--db"])
                self.assertIn(step["argv"][4], {"/tmp/db-a.sqlite", "/tmp/db-b.sqlite"})
                self.assertEqual(step["argv"][5], "--root")
                self.assertIn(step["argv"][6], {"/inputs/root-a", "/inputs/root-b"})
                self.assertTrue(all(type(arg) is str and "\0" not in arg for arg in step["argv"]))

    def test_recipes_and_definitions_are_fresh(self) -> None:
        before = cases.definition_sha256()
        altered = cases.case_definition("cli-empty")
        altered["recipe"]["steps"][0]["argv"].append("--poison")
        altered["expectations"]["s01"]["value"]["documents"].append("wrong")
        recipe = cases.execution_recipe("cli-empty")
        recipe["fixtures"]["wrong"] = "wrong"
        self.assertEqual(cases.definition_sha256(), before)
        self.assertNotIn("wrong", cases.execution_recipe("cli-empty")["fixtures"])
        with self.assertRaises(KeyError):
            cases.case_definition("unknown")

    def test_step_expectation_and_assertion_census(self) -> None:
        for row in cases.definitions():
            steps = row["recipe"]["steps"]
            self.assertEqual([s["step_id"] for s in steps], [f"s{i:02d}" for i in range(1, len(steps) + 1)])
            self.assertEqual(set(row["expectations"]), {s["step_id"] for s in steps})
            for expected in row["expectations"].values():
                ids = expected["assertion_ids"]
                unknown = expected["unspecified_assertion_ids"]
                self.assertEqual(len(ids), len(set(ids)))
                self.assertTrue(set(unknown) <= set(ids))
                self.assertIn("process.exit", ids)
                self.assertNotIn("other_stream_empty", ids)
                if expected["kind"] == "success":
                    self.assertEqual(expected["exit_code"], 0)
                    self.assertIn("stdout.json", ids)
                    self.assertEqual("stdout.value" in ids, expected["semantic_value_supported"])
                elif expected["kind"] == "domain_error":
                    self.assertEqual(expected["exit_code"], 2)
                    self.assertEqual(set(ids), {"process.exit", "stderr.error", "stderr.error_code"})
                    self.assertEqual("stderr.error_code" in unknown, expected["error_code"] is None)
                else:
                    self.assertEqual(expected["kind"], "usage_or_rejection")
                    self.assertEqual(set(ids) - set(unknown), {"process.exit"})

    def test_source_pins_and_policy_disclosure(self) -> None:
        sources = cases.definition_sources()
        for path, digest in cases.NORMATIVE_SHA256.items():
            self.assertEqual(sources[path], digest)
        self.assertIn("not claimed fully source-blind", cases.AUTHORING_DISCLOSURE)
        self.assertEqual(len(cases.definition_sha256()), 64)
        self.assertEqual(cases.definition_sha256(), cases.definition_sha256())

    def test_fresh_checkout_needs_no_historical_run_receipt(self) -> None:
        original = cases.definition_sources()
        definition = cases.definition_sha256()
        with tempfile.TemporaryDirectory() as temporary:
            checkout = Path(temporary)
            for relative in cases.NORMATIVE_SHA256:
                target = checkout / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes((cases.ROOT / relative).read_bytes())
            self.assertFalse((checkout / "runs").exists())
            with patch.object(cases, "ROOT", checkout):
                self.assertEqual(cases.definition_sources(), original)
                self.assertEqual(cases.definition_sha256(), definition)
                self.assertEqual(len(cases.definitions()), 57)
        self.assertFalse(any(path.startswith("runs/") for path in original))
        self.assertEqual(cases.SEMANTIC_POLICY["version"], "candidate-cli-semantic-policy-v1")
        self.assertEqual(cases.HISTORICAL_POLICY_PROVENANCE["sha256"], "ae42de0d0d107355868ac7a8820406d61306b26cf2a84a2c68d954909f0d61b5")

    def test_fixture_scope_and_bounded_sizes(self) -> None:
        for row in cases.definitions():
            recipe = row["recipe"]
            self.assertLessEqual(len(recipe["fixtures"]), 1024)
            total = 0
            for relative, encoded in recipe["fixtures"].items():
                path = PurePosixPath(relative)
                self.assertFalse(path.is_absolute())
                self.assertFalse(set(path.parts) & {".", ".."})
                self.assertIn(path.parts[0], recipe["directories"])
                total += len(base64.b64decode(encoded, validate=True))
            self.assertLessEqual(total, 8 * 1024 * 1024)
            self.assertLessEqual(len(recipe["steps"]), 64)

    def test_lawful_zip_and_json_intake_fixtures(self) -> None:
        z = cases.execution_recipe("cli-intake-zip")
        with zipfile.ZipFile(io.BytesIO(base64.b64decode(z["fixtures"]["root-a/batch.zip"]))) as archive:
            self.assertEqual(archive.namelist(), ["b.html", "a.txt"])
            self.assertEqual(archive.read("b.html"), b"<b>literal</b>")
            self.assertEqual(archive.read("a.txt"), b"")
        j = cases.execution_recipe("cli-intake-json")
        self.assertEqual(json.loads(base64.b64decode(j["fixtures"]["root-a/batch.json"])), {
            "entries": [{"source": "b.html", "text": "<b>literal</b>"}, {"source": "a.txt", "text": ""}],
        })

    def test_literal_casefold_pagination_and_selected_order(self) -> None:
        row = cases.case_definition("cli-legacy-persistence")
        expected = row["expectations"]
        self.assertEqual([d["source"] for d in expected["s04"]["value"]["documents"]], ["a.txt", "same.txt", "β.md"])
        self.assertEqual([d["source"] for d in expected["s06"]["value"]["documents"]], ["same.txt"])
        self.assertEqual(expected["s06"]["value"]["total"], 3)
        self.assertEqual([d["source"] for d in expected["s07"]["value"]["documents"]], ["same.txt", "β.md"])
        self.assertEqual([d["source"] for d in expected["s08"]["value"]["documents"]], ["β.md"])
        self.assertEqual(expected["s08"]["value"]["total"], 2)
        self.assertEqual([d["source"] for d in expected["s13"]["value"]["documents"]], ["a.txt", "β.md"])

    def test_identity_and_shared_blob_follow_normative_bytes(self) -> None:
        row = cases.case_definition("cli-legacy-persistence")
        docs = row["expectations"]["s04"]["value"]["documents"]
        a, same, beta = docs
        self.assertEqual(a["blob_id"], "blob-e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
        self.assertEqual(same["blob_id"], beta["blob_id"])
        self.assertNotEqual(same["document_id"], beta["document_id"])
        self.assertEqual(beta["document_id"], "doc-" + hashlib.sha256(b"document\0" + "β.md".encode()).hexdigest())
        self.assertEqual(beta["source_id"], "src-" + hashlib.sha256(b"source\0" + "β.md".encode()).hexdigest())
        self.assertEqual(set(beta), {"document_id", "source_id", "source", "blob_id", "title", "text"})

    def test_wrapper_and_unknown_error_facets_not_promoted(self) -> None:
        count = 0
        for row in cases.definitions():
            for step in row["recipe"]["steps"]:
                expected = row["expectations"][step["step_id"]]
                command = step["argv"][7] if len(step["argv"]) > 7 else None
                if command in {"import", "jobs"} and expected["kind"] == "success":
                    count += 1
                    self.assertFalse(expected["semantic_value_supported"])
                    self.assertIsNone(expected["value"])
                    self.assertEqual(expected["unspecified_assertion_ids"], ["stdout.wrapper"])
        self.assertGreater(count, 10)
        for case_id in ("cli-export-duplicate", "cli-export-missing"):
            expected = cases.case_definition(case_id)["expectations"]["s02"]
            self.assertIsNone(expected["error_code"])
            self.assertIn("stderr.error_code", expected["unspecified_assertion_ids"])

    def test_state_action_outcomes_and_terminal_receipts(self) -> None:
        allowed = {("queued", "prepare"), ("running", "prepare"), ("running", "commit"),
                   ("completed", "commit"), ("queued", "cancel"), ("running", "cancel"),
                   ("cancelled", "cancel"), ("cancelled", "retry"), ("failed", "retry")}
        for state in ("queued", "running", "completed", "cancelled", "failed"):
            for action in ("prepare", "commit", "cancel", "retry"):
                row = cases.case_definition(f"cli-action-{state}-{action}")
                action_step = row["recipe"]["steps"][-4]
                expected = row["expectations"][action_step["step_id"]]
                self.assertEqual(expected["kind"] == "success", (state, action) in allowed)
                if (state, action) not in allowed:
                    self.assertEqual(expected["error_code"], "job_state")
                if action == "commit" and state == "completed":
                    self.assertEqual(expected["value"]["job"]["state"], "completed")
                    self.assertEqual(expected["value"]["job"]["epoch"], 1)

    def test_cancel_retry_fence_expected_epochs(self) -> None:
        row = cases.case_definition("cli-cancel-retry-epoch-fence")
        expected = row["expectations"]
        self.assertEqual(expected["s03"]["value"]["epoch"], 2)
        self.assertEqual(expected["s04"]["value"]["epoch"], 2)
        self.assertEqual(expected["s05"]["value"]["epoch"], 3)
        self.assertEqual(expected["s06"]["error_code"], "stale_epoch")
        self.assertEqual(expected["s07"]["value"], {"job_id": "fenced", "epoch": 3})
        self.assertEqual(expected["s08"]["error_code"], "stale_epoch")
        self.assertEqual(expected["s09"]["value"]["job"]["completed"], 1)

    def test_db_root_isolation_retains_identity_changes_blob(self) -> None:
        row = cases.case_definition("cli-db-root-isolation")
        a = row["expectations"]["s02"]["value"]
        b = row["expectations"]["s05"]["value"]
        self.assertEqual(a["document_id"], b["document_id"])
        self.assertEqual(a["source_id"], b["source_id"])
        self.assertNotEqual(a["blob_id"], b["blob_id"])
        self.assertEqual(a["text"], "first\n")
        self.assertEqual(b["text"], "second\n")
        self.assertEqual(row["recipe"]["steps"][2]["argv"][4], "/tmp/db-b.sqlite")
        self.assertEqual(row["recipe"]["steps"][4]["argv"][6], "/inputs/root-a")

    def test_rejections_retain_completion_and_qualification_limits(self) -> None:
        for row in cases.definitions():
            self.assertIn("no full requirement or B03 closure", row["claim"])
            self.assertTrue(any("physical storage" in facet for facet in row["unsupported_facets"]))
            for expected in row["expectations"].values():
                self.assertIn(expected["exit_code"], {0, 2})
                if expected["kind"] == "usage_or_rejection":
                    self.assertEqual(expected["unspecified_assertion_ids"], ["streams.framing"])


if __name__ == "__main__":
    unittest.main()
