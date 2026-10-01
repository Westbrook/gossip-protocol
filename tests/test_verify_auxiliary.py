"""External report selection and browser receipts stay explicit and read-only."""
from __future__ import annotations

import copy
import json
from pathlib import Path
import tempfile
import unittest

from devtools.verify_auxiliary import BROWSER_FILES, BROWSER_PAGES, fingerprint, inventory, validate_browser_receipt


def fixture(directory: Path) -> tuple[Path, Path]:
    root = directory / "repository"
    workspace = directory / "independent-report"
    root.mkdir()
    workspace.mkdir()
    (root / ".progress-report").mkdir()
    (workspace / "data").mkdir()
    (workspace / "data" / "project.json").write_text('{"feedback": ["canonical"]}\n')
    (workspace / "report.py").write_text("raise RuntimeError('inventory must not import me')\n")
    (workspace / "index.html").write_text("<html>Independent report</html>\n")
    (workspace / "test_report.py").write_text(
        "import unittest\nraise RuntimeError('inventory must not execute tests')\n"
        "class ReportContractTests(unittest.TestCase):\n"
        "    def test_scope(self): pass\n"
        "    def test_feedback(self): pass\n"
    )
    (root / ".progress-report" / "project.json").write_text(json.dumps({
        "schemaVersion": 1, "projectId": "fixture-report", "reportWorkspace": str(workspace),
        "stateLocation": str(workspace / "data" / "project.json"), "reportUrl": "http://127.0.0.1:4178/",
    }))
    browser = root / "devtools" / "browser"
    browser.mkdir(parents=True)
    for name in BROWSER_FILES:
        (browser / name).write_text("{}\n" if name.endswith("json") else "fixture source\n")
    (root / "research.html").write_text("<html>Research</html>\n")
    return root, workspace


def browser_receipt() -> dict:
    return {
        "schemaVersion": 1, "passed": True, "physicalExecution": True, "browserLaunches": 1,
        "playwright": "1.62.1", "chromiumRevision": "1234", "browserVersion": "fixture-version",
        "pages": list(BROWSER_PAGES), "results": [{"page": name, "passed": True} for name in BROWSER_PAGES],
        "canonicalReviewPreserved": True, "borrowedServerStopped": False,
        "cleanup": {"browser": "complete", "api": "complete", "ownedServer": "complete"},
        "sourceBindings": {"fixture.html": "0" * 64}, "verificationBindings": {"run.cjs": "1" * 64},
        "serverIdentity": {"projectId": "fixture-report"},
    }


class AuxiliaryVerificationTests(unittest.TestCase):
    def test_offline_selection_never_needs_external_workspace(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.assertEqual(inventory(root, ["fast", "git"]), [])
            self.assertEqual(fingerprint(root, ["fixtures"]), {})

    def test_report_inventory_uses_external_source_without_importing(self):
        with tempfile.TemporaryDirectory() as temporary:
            root, workspace = fixture(Path(temporary))
            before = (workspace / "data" / "project.json").read_bytes()
            records = inventory(root, ["report"])
            self.assertEqual([record["method"] for record in records], ["test_feedback", "test_scope"])
            self.assertEqual({record["class"] for record in records}, {"report/test_report.py::ReportContractTests"})
            self.assertTrue(all(record["root"] == str(workspace.resolve()) for record in records))
            self.assertTrue(all(record["path"] == "test_report.py" and record["lane"] == "report" for record in records))
            self.assertEqual(before, (workspace / "data" / "project.json").read_bytes())

    def test_browser_inventory_is_one_explicit_batch_without_installation(self):
        with tempfile.TemporaryDirectory() as temporary:
            root, _ = fixture(Path(temporary))
            records = inventory(root, ["browser"])
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0]["command"], ["node", str(root.resolve() / "devtools/browser/run.cjs"), "--pages", "all"])
            self.assertFalse(records[0]["reuse"])
            self.assertEqual(records[0]["pages"], list(BROWSER_PAGES))

    def test_selected_missing_locator_and_source_fail_clearly(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            with self.assertRaisesRegex(ValueError, "locator"):
                inventory(directory, ["report"])
            root, workspace = fixture(directory)
            (workspace / "report.py").unlink()
            with self.assertRaisesRegex(ValueError, "report.py"):
                inventory(root, ["browser"])

    def test_dynamic_test_discovery_fails_instead_of_omitting_tests(self):
        with tempfile.TemporaryDirectory() as temporary:
            root, workspace = fixture(Path(temporary))
            path = workspace / "test_report.py"
            for extension in ("\ndef load_tests(loader, tests, pattern): return tests\n", "\nclass Derived(ReportContractTests):\n    pass\n"):
                with self.subTest(extension=extension):
                    original = path.read_text()
                    path.write_text(original + extension)
                    with self.assertRaisesRegex(ValueError, "explicit inventory"):
                        inventory(root, ["report"])
                    path.write_text(original)

    def test_fingerprint_binds_sources_but_not_user_feedback_or_progress(self):
        with tempfile.TemporaryDirectory() as temporary:
            root, workspace = fixture(Path(temporary))
            original = fingerprint(root, ["report", "browser"])
            (workspace / "data" / "project.json").write_text('{"feedback": ["new user input"]}\n')
            self.assertEqual(fingerprint(root, ["report", "browser"]), original)
            for path, key in ((workspace / "report.py", "report:report.py"),
                              (workspace / "test_report.py", "report:test_report.py"),
                              (root / "research.html", "page:research.html"),
                              (root / "devtools/browser/package.json", "browser:package.json")):
                with self.subTest(path=path):
                    content = path.read_text()
                    path.write_text(content + "\n")
                    self.assertNotEqual(fingerprint(root, ["report", "browser"])[key], original[key])
                    path.write_text(content)

    def test_browser_receipt_requires_full_physical_pinned_clean_observation(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "receipt.json"
            valid = browser_receipt()
            path.write_text(json.dumps(valid))
            self.assertEqual(validate_browser_receipt(path), valid)
            mutations = [
                {"passed": False}, {"physicalExecution": False}, {"browserLaunches": 2},
                {"playwright": "different"}, {"chromiumRevision": "different"},
                {"pages": ["report"]}, {"results": valid["results"][:-1]},
                {"results": [*valid["results"], None]}, {"canonicalReviewPreserved": False},
                {"borrowedServerStopped": True}, {"cleanup": {"browser": "unknown"}}, {"sourceBindings": {}},
            ]
            for mutation in mutations:
                with self.subTest(mutation=mutation):
                    path.write_text(json.dumps({**copy.deepcopy(valid), **mutation}))
                    with self.assertRaises(ValueError):
                        validate_browser_receipt(path)


if __name__ == "__main__":
    unittest.main()
