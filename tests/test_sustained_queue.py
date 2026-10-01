"""Execute trusted fixture implementations only; never API candidate code."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from gossip_harness.blackbox_validator import json_equal
from gossip_harness.sustained_queue import PROJECT, MUTANTS, add, added, row


class TrustedRepository:
    def __init__(self, files):
        self.directory = tempfile.TemporaryDirectory(prefix="trusted-workflow-")
        self.root = Path(self.directory.name)
        for name, content in files.items():
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)

    def solve(self, payload):
        script = "import json,sys; sys.path.insert(0,sys.argv[1]); from solution import solve; print(json.dumps(solve(json.load(sys.stdin))))"
        result = subprocess.run([sys.executable, "-I", "-c", script, str(self.root)],
                                input=json.dumps(payload), text=True, capture_output=True, timeout=30)
        if result.returncode:
            raise AssertionError(result.stderr)
        return json.loads(result.stdout)

    def close(self):
        self.directory.cleanup()


class WorkflowFixtureTests(unittest.TestCase):
    def test_initial_repository_is_working_and_incomplete(self):
        repo = TrustedRepository(PROJECT["initial_files"])
        try:
            answer = repo.solve({"commands": [add("old", 2), {"op": "list"}, {"op": "ready"}]})
            self.assertEqual(answer, [added("old"), [row("old", 2)], {"error": "invalid"}])
        finally:
            repo.close()

    def test_known_stages_meet_current_and_all_previous_requirements(self):
        cumulative = []
        for stage in PROJECT["stages"]:
            cumulative.extend(stage["visible_cases"] + stage["hidden_cases"])
            repo = TrustedRepository(stage["known_files"])
            try:
                for case in cumulative:
                    with self.subTest(stage=stage["id"], case=case["id"]):
                        self.assertTrue(json_equal(repo.solve(case["input"]), case["expected"]), case["id"])
            finally:
                repo.close()

    def test_frozen_transport_and_case_coverage(self):
        self.assertEqual(PROJECT["id"], "workflow")
        self.assertEqual(len(PROJECT["allowed_paths"]), 3)
        self.assertEqual(len(PROJECT["stages"]), 3)
        ids, visible_inputs, hidden_inputs = set(), set(), set()
        fixed = set(PROJECT["initial_files"]) - set(PROJECT["allowed_paths"])
        for stage in PROJECT["stages"]:
            self.assertEqual(len(stage["visible_cases"]), 8)
            self.assertEqual(len(stage["hidden_cases"]), 10)
            self.assertEqual(set(stage["requirements"]), {c["requirement"] for c in stage["visible_cases"]})
            self.assertEqual(set(stage["requirements"]), {c["requirement"] for c in stage["hidden_cases"]})
            for filename in fixed:
                self.assertEqual(stage["known_files"][filename], PROJECT["initial_files"][filename])
            for group, inputs in (("visible_cases", visible_inputs), ("hidden_cases", hidden_inputs)):
                for case in stage[group]:
                    self.assertNotIn(case["id"], ids)
                    ids.add(case["id"])
                    self.assertIn(case["requirement"], stage["requirements"])
                    self.assertLessEqual(len(case["input"]["commands"]), 12)
                    self.assertEqual(len(case["input"]["commands"]), len(case["expected"]))
                    inputs.add(json.dumps(case["input"], sort_keys=True))
        self.assertFalse(visible_inputs & hidden_inputs)
        self.assertEqual(len(ids), 54)
        final = PROJECT["stages"][-1]["known_files"]
        self.assertLessEqual(sum(len(final[path].splitlines()) for path in PROJECT["allowed_paths"]), 400)

    def test_realistic_mutants_pass_simple_checks_but_fail_quality_bar(self):
        targets = {
            "inclusive-expiry": (1, "expiry-fence", "expired-not-ready"),
            "partial-batch-commit": (2, "atomic-rollback", "batch-cycle-rollback"),
            "receipt-reapplied": (1, "retry-receipt", "legacy-restart-retry"),
        }
        cases = {case["id"]: case for stage in PROJECT["stages"] for case in stage["visible_cases"] + stage["hidden_cases"]}
        simple = cases["claim-complete"]
        for name, files in MUTANTS.items():
            repo = TrustedRepository(files)
            try:
                self.assertTrue(json_equal(repo.solve(simple["input"]), simple["expected"]), name)
                _, failing, hidden_failing = targets[name]
                for case_id in (failing, hidden_failing):
                    case = cases[case_id]
                    self.assertFalse(json_equal(repo.solve(case["input"]), case["expected"]), (name, case_id))
                # These bugs preserve the entire earlier dependency milestone.
                for early in PROJECT["stages"][0]["visible_cases"]:
                    self.assertTrue(json_equal(repo.solve(early["input"]), early["expected"]), name)
            finally:
                repo.close()


if __name__ == "__main__":
    unittest.main()
