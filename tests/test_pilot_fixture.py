"""Offline fixture/checker sanity tests; no generated agent code executes here."""

import json
from pathlib import Path
import subprocess
import sys
import tempfile
from textwrap import dedent
import unittest

from gossip_harness.pilot_fixture import (
    CHECKS, EXPECTED_TEST_COUNTS, INITIAL_FILES, KNOWN_SOLUTIONS, SPEC_VERSION, TASKS,
)


class PilotFixtureTests(unittest.TestCase):
    def check_fixture(self, patches, target):
        with tempfile.TemporaryDirectory(prefix="pilot-fixture-test-") as directory:
            root = Path(directory)
            workspace = root / "workspace"
            checks = root / "checks"
            for base, files in ((workspace, {**INITIAL_FILES, **patches}), (checks, CHECKS)):
                for name, content in files.items():
                    path = base / name
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(content, encoding="utf-8")
            bootstrap = (
                "import runpy,sys;checker=runpy.run_path(sys.argv[1],run_name='trusted_checks');"
                "sys.exit(checker['run_checks'](sys.argv[2],workspace=sys.argv[3]))"
            )
            result = subprocess.run(
                [sys.executable, "-I", "-c", bootstrap, str(checks / "run_checks.py"),
                 target, str(workspace)],
                capture_output=True, text=True, timeout=30,
            )
            return result, json.loads(result.stdout)

    def test_solution_passes_all_external_and_cli_checks(self):
        result, receipt = self.check_fixture(KNOWN_SOLUTIONS, "all")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(receipt, {
            "spec_version": SPEC_VERSION, "target": "all", "tests_run": 23,
            "failures": 0, "errors": 0, "successful": True,
        })

    def test_each_task_is_independently_checkable(self):
        for task in TASKS:
            with self.subTest(task=task["id"]):
                patches = {path: KNOWN_SOLUTIONS[path] for path in task["allowed_paths"]}
                result, receipt = self.check_fixture(patches, task["id"])
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(receipt["tests_run"], EXPECTED_TEST_COUNTS[task["id"]])
                self.assertTrue(receipt["successful"])

    def test_unsolved_project_fails_with_checks_actually_executed(self):
        result, receipt = self.check_fixture({}, "all")
        self.assertEqual(result.returncode, 1)
        self.assertFalse(receipt["successful"])
        self.assertEqual(receipt["tests_run"], EXPECTED_TEST_COUNTS["all"])
        self.assertGreater(receipt["errors"] + receipt["failures"], 0)

    def test_individual_success_does_not_satisfy_integrated_project(self):
        parser_only = {"task_report/parser.py": KNOWN_SOLUTIONS["task_report/parser.py"]}
        result, receipt = self.check_fixture(parser_only, "all")
        self.assertEqual(result.returncode, 1)
        self.assertEqual(receipt["tests_run"], EXPECTED_TEST_COUNTS["all"])
        self.assertFalse(receipt["successful"])

    def test_checks_reject_float_results_and_reused_output(self):
        base = KNOWN_SOLUTIONS["task_report/summary.py"].replace("def summarize(", "def _compute(")
        wrappers = (
            """
            def summarize(tasks):
                result = _compute(tasks)
                result["counts"] = {key: float(value) for key, value in result["counts"].items()}
                result["total_estimate"] = float(result["total_estimate"])
                result["remaining_estimate"] = float(result["remaining_estimate"])
                return result
            """,
            """
            shared = {}
            def summarize(tasks):
                shared.clear()
                shared.update(_compute(tasks))
                return shared
            """,
        )
        for wrapper in wrappers:
            with self.subTest(wrapper=wrapper):
                result, receipt = self.check_fixture(
                    {"task_report/summary.py": base + "\n" + dedent(wrapper)}, "summary",
                )
                self.assertEqual(result.returncode, 1)
                self.assertEqual(receipt["errors"], 0)
                self.assertGreater(receipt["failures"], 0)

    def test_worker_inputs_exclude_solutions_and_external_checks(self):
        self.assertEqual(set(CHECKS), {"run_checks.py"})
        self.assertFalse(set(CHECKS) & set(INITIAL_FILES))
        allowed = {path for task in TASKS for path in task["allowed_paths"]}
        self.assertEqual(allowed, set(KNOWN_SOLUTIONS))
        self.assertEqual({task["id"] for task in TASKS}, {"parser", "summary"})
        for task in TASKS:
            self.assertEqual(task["dependencies"], ())
            self.assertEqual(len(task["allowed_paths"]), 1)
            for path in task["allowed_paths"]:
                self.assertIn("raise NotImplementedError", INITIAL_FILES[path])
                self.assertNotEqual(INITIAL_FILES[path], KNOWN_SOLUTIONS[path])
                self.assertNotIn(KNOWN_SOLUTIONS[path], task["instructions"])
        for filename, source in INITIAL_FILES.items():
            if filename.endswith(".py"):
                compile(source, filename, "exec")
        self.assertIn(SPEC_VERSION, INITIAL_FILES["README.md"])


if __name__ == "__main__":
    unittest.main()
