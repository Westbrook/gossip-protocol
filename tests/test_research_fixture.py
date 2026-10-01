"""Trusted offline checks for the frozen v1-to-v2 feature fixture and its oracle."""

import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from textwrap import dedent
import unittest

from gossip_harness.research_fixture import (
    BASELINE_COMMIT, BASELINE_FILES, BASELINE_FILES_SHA256, CHECKS, COMBINED_SPEC,
    EXPECTED_TEST_COUNTS, INITIAL_FILES, KNOWN_SOLUTIONS, SPEC_VERSION, TASKS,
)


class ResearchFixtureTests(unittest.TestCase):
    def check_fixture(self, patches, target="all", *, legacy=False):
        with tempfile.TemporaryDirectory(prefix="research-fixture-test-") as directory:
            root = Path(directory)
            workspace, checks = root / "workspace", root / "checks"
            for base, files in ((workspace, {**INITIAL_FILES, **patches}), (checks, CHECKS)):
                for name, content in files.items():
                    path = base / name
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(content, encoding="utf-8")
            script = "legacy_checks.py" if legacy else "run_checks.py"
            bootstrap = (
                "import runpy,sys;checks=runpy.run_path(sys.argv[1],run_name='trusted_checks');"
                "sys.exit(checks['run_checks'](sys.argv[2],workspace=sys.argv[3]))"
            )
            result = subprocess.run(
                [sys.executable, "-I", "-c", bootstrap, str(checks / script), target, str(workspace)],
                capture_output=True, text=True, timeout=30,
            )
            receipts = []
            for line in result.stdout.splitlines():
                try:
                    row = json.loads(line)
                except ValueError:
                    continue
                if isinstance(row, dict) and row.get("spec_version") == (
                        "task-report-v1" if legacy else SPEC_VERSION):
                    receipts.append(row)
            self.assertEqual(len(receipts), 1,
                             f"Checker must emit one complete JSON receipt: {result.stdout}\n{result.stderr}")
            return result, receipts[0]

    def test_baseline_is_exact_accepted_project_with_only_new_document(self):
        self.assertEqual(BASELINE_COMMIT, "c007e925c58b4a051d9be853d74e71fe0eeb3bc0")
        self.assertEqual(hashlib.sha256(json.dumps(BASELINE_FILES, sort_keys=True).encode()).hexdigest(),
                         BASELINE_FILES_SHA256)
        accepted = Path(__file__).resolve().parents[1] / "results/practical-live-1/accepted/single"
        self.assertEqual((accepted / "SOURCE_COMMIT").read_text().strip(), BASELINE_COMMIT)
        for name, source in BASELINE_FILES.items():
            self.assertEqual(source, (accepted / name).read_text())
            self.assertEqual(INITIAL_FILES[name], source)
        self.assertEqual(set(INITIAL_FILES) - set(BASELINE_FILES), {"FEATURE_SPEC.md"})
        self.assertNotIn("raise NotImplementedError", INITIAL_FILES["task_report/parser.py"])
        self.assertNotIn("raise NotImplementedError", INITIAL_FILES["task_report/summary.py"])

    def test_baseline_passes_v1_but_requires_real_feature_work(self):
        result, receipt = self.check_fixture({}, legacy=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(receipt["tests_run"], 23)
        result, receipt = self.check_fixture({})
        self.assertEqual(result.returncode, 1)
        self.assertEqual(receipt["tests_run"], EXPECTED_TEST_COUNTS["all"])
        self.assertFalse(receipt["successful"])

    def test_known_upgrade_passes_all_old_and_new_checks(self):
        result, receipt = self.check_fixture(KNOWN_SOLUTIONS)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(receipt, {
            "spec_version": SPEC_VERSION, "target": "all", "tests_run": 36,
            "failures": 0, "errors": 0, "successful": True,
        })
        self.assertEqual(result.stderr, "")
        self.assertIn("Ran 36 tests", result.stdout)
        self.assertIn("\nOK\n", result.stdout)
        self.assertEqual(result.stdout.splitlines()[-1], json.dumps(receipt, sort_keys=True))

    def test_local_checks_require_no_peer_upgrade(self):
        for task in TASKS:
            with self.subTest(task=task["id"]):
                patches = {name: KNOWN_SOLUTIONS[name] for name in task["allowed_paths"]}
                result, receipt = self.check_fixture(patches, task["id"])
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(receipt["tests_run"], EXPECTED_TEST_COUNTS[task["id"]])
                self.assertTrue(receipt["successful"])

    def test_both_modules_must_upgrade_for_the_full_project(self):
        for task in TASKS:
            with self.subTest(task=task["id"]):
                patches = {name: KNOWN_SOLUTIONS[name] for name in task["allowed_paths"]}
                result, receipt = self.check_fixture(patches)
                self.assertEqual(result.returncode, 1)
                self.assertEqual(receipt["tests_run"], 36)
                self.assertFalse(receipt["successful"])

    def test_all_workers_see_full_contract_but_not_oracle_or_acceptance_checks(self):
        self.assertIn(COMBINED_SPEC, INITIAL_FILES["FEATURE_SPEC.md"])
        self.assertFalse(set(CHECKS) & set(INITIAL_FILES))
        self.assertEqual({p for task in TASKS for p in task["allowed_paths"]}, set(KNOWN_SOLUTIONS))
        for task in TASKS:
            self.assertIn(COMBINED_SPEC, task["instructions"])
            self.assertEqual(task["dependencies"], ())
            self.assertEqual(task["allowed_paths"], (f"task_report/{task['id']}.py",))
            for solution in KNOWN_SOLUTIONS.values():
                self.assertNotIn(solution, task["instructions"])
                self.assertNotIn(solution, INITIAL_FILES.values())
        for name, content in INITIAL_FILES.items():
            if name.endswith(".py"):
                compile(content, name, "exec")

    def test_checks_reject_deliberate_units_graph_order_type_and_alias_mutants(self):
        parser = KNOWN_SOLUTIONS["task_report/parser.py"]
        summary = KNOWN_SOLUTIONS["task_report/summary.py"]
        renamed = summary.replace("def summarize(", "def _compute(")

        def wrapped(body):
            return renamed + "\n" + dedent(body)

        mutants = {
            "hours-not-converted": ("parser", parser.replace("* 60 + minutes", "* 1 + minutes")),
            "references-sorted": ("parser", parser.replace('result["depends_on"] = refs',
                                                         'result["depends_on"] = sorted(refs)')),
            "shared-empty-references": ("parser", "_EMPTY = []\n" + parser.replace(
                'result["depends_on"] = refs', 'result["depends_on"] = refs if refs else _EMPTY')),
            "any-prerequisite-enough": ("summary", summary.replace(
                "ready = all(", 'ready = not task["depends_on"] or any(')),
            "cycles-ignored": ("summary", summary.replace('raise ValueError("Dependency cycle")', "return")),
            "blocked-sorted": ("summary", wrapped('''
                def summarize(tasks):
                    result = _compute(tasks)
                    if "blocked_ids" in result:
                        result["blocked_ids"].sort()
                    return result
            ''')),
            "only-first-blocked-estimate": ("summary", wrapped('''
                def summarize(tasks):
                    result = _compute(tasks)
                    if result.get("blocked_ids"):
                        first = result["blocked_ids"][0]
                        result["blocked_estimate"] = next(t["estimate"] for t in tasks if t["id"] == first)
                    return result
            ''')),
            "float-scheduling-estimate": ("summary", wrapped('''
                def summarize(tasks):
                    result = _compute(tasks)
                    if "ready_estimate" in result:
                        result["ready_estimate"] = float(result["ready_estimate"])
                    return result
            ''')),
            "input-reference-list-sorted": ("summary", wrapped('''
                def summarize(tasks):
                    for task in tasks:
                        if "depends_on" in task:
                            task["depends_on"].sort()
                    return _compute(tasks)
            ''')),
            "shared-ready-list": ("summary", wrapped('''
                shared = []
                def summarize(tasks):
                    result = _compute(tasks)
                    if "ready_ids" in result:
                        shared[:] = result["ready_ids"]
                        result["ready_ids"] = shared
                    return result
            ''')),
        }
        for name, (target, source) in mutants.items():
            with self.subTest(mutant=name):
                self.assertNotEqual(source, KNOWN_SOLUTIONS[f"task_report/{target}.py"])
                patches = {**KNOWN_SOLUTIONS, f"task_report/{target}.py": source}
                result, receipt = self.check_fixture(patches, target)
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertFalse(receipt["successful"])
                self.assertGreater(receipt["failures"] + receipt["errors"], 0)


if __name__ == "__main__":
    unittest.main()
