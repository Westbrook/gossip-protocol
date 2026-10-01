"""Artifact lifecycle contracts, without Git, Docker, or provider execution."""

from contextlib import redirect_stderr
import io
import json
from pathlib import Path
import tempfile
import unittest

from devtools.test_artifacts import ArtifactDirectory


class ArtifactDirectoryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.messages = io.StringIO()
        self.redirect = redirect_stderr(self.messages)
        self.redirect.__enter__()
        self.addCleanup(self.redirect.__exit__, None, None, None)

    def workspace(self, **kwargs):
        return ArtifactDirectory("ExampleTests", environ={}, temporary_root=self.root, **kwargs)

    def test_failed_context_preserves_source_receipts_and_traceback(self):
        with self.assertRaisesRegex(AssertionError, "regression evidence"):
            with self.workspace() as artifacts:
                artifacts.output.mkdir()
                (artifacts.output / "candidate.py").write_text("answer = 41\n")
                (artifacts.output / "receipt.json").write_text('{"passed": false}\n')
                raise AssertionError("regression evidence")
        self.assertEqual((artifacts.output / "candidate.py").read_text(), "answer = 41\n")
        self.assertFalse(json.loads((artifacts.output / "receipt.json").read_text())["passed"])
        self.assertIn("AssertionError: regression evidence", (artifacts.root / "failure.txt").read_text())
        self.assertEqual(json.loads((artifacts.root / "artifacts.json").read_text())["retention"], "failure")
        self.assertIn(str(artifacts.root), self.messages.getvalue())

    def test_small_success_cleans_only_its_workspace(self):
        sentinel = self.root / "preexisting.txt"
        sentinel.write_text("keep me")
        with self.workspace() as artifacts:
            artifacts.output.mkdir()
            (artifacts.output / "receipt.json").write_text("{}")
        self.assertFalse(artifacts.root.exists())
        self.assertEqual(sentinel.read_text(), "keep me")
        artifacts.close()  # Cleanup is idempotent after the directory is gone.

    def test_expensive_success_and_unknown_class_outcome_are_retained(self):
        with self.workspace(retain_success=True) as artifacts:
            artifacts.output.mkdir()
        self.assertTrue(artifacts.output.is_dir())
        self.assertEqual(json.loads((artifacts.root / "artifacts.json").read_text())["retention"], "expensive-fixture")
        unknown = self.workspace()
        unknown.close()
        self.assertTrue(unknown.root.is_dir())
        self.assertEqual(json.loads((unknown.root / "artifacts.json").read_text())["retention"], "unknown-outcome")

    def test_session_root_allocates_unique_fresh_outputs_and_keeps_success(self):
        environment = {"GOSSIP_TEST_ARTIFACTS": str(self.root / "session")}
        workspaces = []
        for _ in range(2):
            with ArtifactDirectory("ExampleTests", environ=environment) as artifacts:
                self.assertFalse(artifacts.output.exists())
                artifacts.output.mkdir()
                workspaces.append(artifacts)
        self.assertNotEqual(workspaces[0].root, workspaces[1].root)
        self.assertTrue(all(item.output.is_dir() for item in workspaces))
        self.assertTrue(all(item.root.parent == self.root / "session" for item in workspaces))

    def test_legacy_exact_output_is_retained_without_deleting_siblings(self):
        output = self.root / "specified-output"
        sibling = self.root / "keep.txt"
        sibling.write_text("prior evidence")
        with ArtifactDirectory("ExampleTests", output_env="LEGACY_OUTPUT",
                               environ={"LEGACY_OUTPUT": str(output)},
                               temporary_root=self.root) as artifacts:
            self.assertEqual(artifacts.output, output)
            artifacts.output.mkdir()
            (output / "trace.jsonl").write_text("{}\n")
        self.assertEqual((output / "trace.jsonl").read_text(), "{}\n")
        self.assertEqual(sibling.read_text(), "prior evidence")
        self.assertTrue(artifacts.root.is_dir())

    def test_existing_output_and_dangling_symlink_rejected_without_changes(self):
        existing = self.root / "existing"
        existing.mkdir()
        sentinel = existing / "sentinel.txt"
        sentinel.write_text("prior evidence")
        dangling = self.root / "dangling"
        dangling.symlink_to(self.root / "missing")
        before = set(self.root.iterdir())
        for output in (existing, sentinel, dangling):
            with self.subTest(output=output), self.assertRaises(FileExistsError):
                ArtifactDirectory("ExampleTests", output_env="LEGACY_OUTPUT",
                                  environ={"LEGACY_OUTPUT": str(output)},
                                  temporary_root=self.root)
            self.assertEqual(set(self.root.iterdir()), before)
        self.assertEqual(sentinel.read_text(), "prior evidence")
        self.assertTrue(dangling.is_symlink())

    def test_class_setup_failure_retains_fixture_through_unittest_cleanup(self):
        parent = self
        class BrokenFixture(unittest.TestCase):
            @classmethod
            def setUpClass(cls):
                cls.artifacts = parent.workspace()
                cls.addClassCleanup(cls.artifacts.close)
                cls.artifacts.output.mkdir()
                (cls.artifacts.output / "setup.log").write_text("setup reached Git preparation")
                raise RuntimeError("setup failed")

            def test_never_runs(self):
                self.fail("setup failed first")

        result = unittest.TestResult()
        unittest.defaultTestLoader.loadTestsFromTestCase(BrokenFixture).run(result)
        self.assertEqual(len(result.errors), 1)
        self.assertEqual(result.testsRun, 0)
        self.assertTrue(BrokenFixture.artifacts.closed)
        self.assertEqual((BrokenFixture.artifacts.output / "setup.log").read_text(),
                         "setup reached Git preparation")


if __name__ == "__main__":
    unittest.main()
