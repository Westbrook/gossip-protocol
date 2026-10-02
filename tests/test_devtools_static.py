"""Regression checks for fail-closed cheap gates, using disposable source trees."""
from __future__ import annotations

import json
import hashlib
from pathlib import Path
import tempfile
import threading
import tomllib
import unittest
from unittest.mock import patch

from devtools.static_checks import Check, ConfigurationError, FROZEN_TYPE_BASELINE_EXCEPTIONS, _read_type_baseline, _run_type_tool, check_syntax, run_checks, source_files


PROJECT = Path(__file__).resolve().parents[1]


def fixture(root: Path, source: str = "value: int = 3\n") -> None:
    document = tomllib.loads((PROJECT / "pyproject.toml").read_text())
    dependencies = document["dependency-groups"]["dev"]
    (root / "sample.py").write_text(source)
    (root / "pyproject.toml").write_text(
        "[dependency-groups]\n"
        f"dev = {json.dumps(dependencies)}\n"
        '[tool.devtools.static]\nroots = ["sample.py"]\ntimeout_seconds = 30\n'
        '[tool.ruff.lint]\nselect = ["E9", "F63", "F7", "F82"]\n'
        '[tool.mypy]\nfiles = ["sample.py"]\npython_version = "3.11"\n'
        'check_untyped_defs = true\nincremental = true\ncache_dir = ".cache/mypy"\n'
    )


class StaticGateTests(unittest.TestCase):
    def test_type_debt_matches_exact_diagnostics_and_source_bytes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "gossip_harness").mkdir()
            source = root / "gossip_harness" / "legacy.py"
            source.write_text('value: int = "old debt"\n')
            diagnostic = {"file": "gossip_harness/legacy.py", "line": 1, "column": 1,
                          "message": "Existing assignment", "code": "assignment", "severity": "error"}
            baseline_file = root / "baseline.json"
            baseline_file.write_text(json.dumps({"schema_version": 1, "mypy_version": "1.20.2", "files": [
                {"path": "gossip_harness/legacy.py", "sha256": hashlib.sha256(source.read_bytes()).hexdigest(), "diagnostics": [diagnostic]}
            ]}))
            baseline = _read_type_baseline(root, "baseline.json", "1.20.2")
            with patch("devtools.static_checks._run_tool", return_value=Check("types", "failed", 0, json.dumps(diagnostic), returncode=1)):
                accepted = _run_type_tool(("mypy",), root, 10, baseline)
            self.assertEqual(accepted.status, "passed")
            self.assertEqual(accepted.baseline_count, 1)
            changed = {**diagnostic, "message": "New assignment problem"}
            with patch("devtools.static_checks._run_tool", return_value=Check("types", "failed", 0, json.dumps(changed), returncode=1)):
                self.assertEqual(_run_type_tool(("mypy",), root, 10, baseline).status, "failed")
            source.write_text(source.read_text() + "new_code = True\n")
            with self.assertRaisesRegex(ConfigurationError, "source changed"):
                _read_type_baseline(root, "baseline.json", "1.20.2")

    def test_new_tool_and_new_study_errors_cannot_be_baselined(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name in ("devtools/new.py", "gossip_harness/verification_new.py",
                         "gossip_harness/continuation_controller.py", "gossip_harness/continuation_followup.py",
                         "gossip_harness/continuation_new.py", "gossip_harness/benchmark_new.py",
                         "gossip_harness/benchmark_warehouse.py", "gossip_harness/benchmark_job_queue.py",
                         "gossip_harness/sustained_experiment_v2.py", "analysis/new.py",
                         "analysis/run_sustained_review_probes_v2.py"):
                with self.subTest(name=name):
                    (root / "baseline.json").write_text(json.dumps({"schema_version": 1, "mypy_version": "1.20.2", "files": [
                        {"path": name, "sha256": "unused", "diagnostics": []}
                    ]}))
                    with self.assertRaisesRegex(ConfigurationError, "restricted"):
                        _read_type_baseline(root, "baseline.json", "1.20.2")

    def test_frozen_exception_requires_original_bytes_and_identity(self):
        for name, identity in FROZEN_TYPE_BASELINE_EXCEPTIONS.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                source = root / name
                source.parent.mkdir(parents=True)
                source.write_bytes((PROJECT / name).read_bytes())
                self.assertEqual(hashlib.sha256(source.read_bytes()).hexdigest(), identity)
                document = {"schema_version": 1, "mypy_version": "1.20.2", "files": [
                    {"path": name, "sha256": identity, "diagnostics": []}
                ]}
                baseline_file = root / "baseline.json"
                baseline_file.write_text(json.dumps(document))
                self.assertEqual(_read_type_baseline(root, "baseline.json", "1.20.2").source_sha256[name], identity)
                source.write_bytes(source.read_bytes() + b"\n# A changed scientific source\n")
                with self.assertRaisesRegex(ConfigurationError, "source changed"):
                    _read_type_baseline(root, "baseline.json", "1.20.2")
                # Updating a JSON hash cannot approve debt for a new revision.
                document["files"][0]["sha256"] = hashlib.sha256(source.read_bytes()).hexdigest()
                baseline_file.write_text(json.dumps(document))
                with self.assertRaisesRegex(ConfigurationError, "restricted"):
                    _read_type_baseline(root, "baseline.json", "1.20.2")

    def test_syntax_never_imports_candidate_and_excludes_generated_trees(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "owned").mkdir()
            marker = root / "executed"
            (root / "owned" / "candidate.py").write_text(f"open({str(marker)!r}, 'w').write('bad')\n")
            (root / "owned" / "results").mkdir()
            (root / "owned" / "results" / "frozen.py").write_text("this is invalid python ?\n")
            files = source_files(root, ("owned",))
            self.assertEqual([item.name for item in files], ["candidate.py"])
            self.assertEqual(check_syntax(root, files).status, "passed")
            self.assertFalse(marker.exists())

    def test_syntax_error_starts_zero_tool_processes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            # AST parsing accepts this; full compile must reject it.
            fixture(root, "return 1\n")
            with patch("devtools.static_checks._installed_versions") as versions, patch("devtools.static_checks._run_tool") as tool:
                receipt = run_checks(root)
            self.assertEqual(receipt["status"], "failed")
            self.assertEqual([check["name"] for check in receipt["checks"]], ["configuration", "syntax"])
            versions.assert_not_called()
            tool.assert_not_called()

    def test_disabled_configuration_starts_zero_checks(self):
        for before, after in [
            ('select = ["E9", "F63", "F7", "F82"]', 'select = []'),
            ('check_untyped_defs = true', 'check_untyped_defs = false'),
            ('incremental = true', 'incremental = false'),
            ('files = ["sample.py"]', 'files = []'),
        ]:
            with self.subTest(after=after), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                fixture(root)
                path = root / "pyproject.toml"
                path.write_text(path.read_text().replace(before, after))
                with patch("devtools.static_checks.check_syntax") as syntax, patch("devtools.static_checks._run_tool") as tool:
                    receipt = run_checks(root)
                self.assertEqual(receipt["status"], "error")
                syntax.assert_not_called()
                tool.assert_not_called()

    def test_source_roots_reject_history_missing_and_external_paths(self):
        with tempfile.TemporaryDirectory() as temporary, tempfile.TemporaryDirectory() as external:
            root = Path(temporary)
            (Path(external) / "source.py").write_text("value = 1\n")
            (root / "escape").symlink_to(external, target_is_directory=True)
            for name in ("results", "missing", "escape", ".", "../other"):
                with self.subTest(name=name), self.assertRaises(ConfigurationError):
                    source_files(root, (name,))

    def test_wrong_tool_versions_fail_with_bootstrap_action(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture(root)
            with patch("devtools.static_checks.metadata.version", return_value="0.0.0"), patch("devtools.static_checks._run_tool") as tool:
                receipt = run_checks(root)
            self.assertEqual(receipt["status"], "error")
            self.assertIn("scripts/bootstrap-dev", receipt["checks"][-1]["diagnostics"])
            tool.assert_not_called()

    def test_independent_tools_run_in_parallel_and_merge_in_stable_order(self):
        barrier = threading.Barrier(2, timeout=5)

        def run(name, command, root, timeout):
            barrier.wait()
            return Check(name, "passed", 0.0, command=command)

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture(root)
            with patch("devtools.static_checks._run_tool", side_effect=run):
                receipt = run_checks(root)
            self.assertEqual(receipt["status"], "passed")
            self.assertEqual([item["name"] for item in receipt["checks"]][-2:], ["lint", "types"])

    def test_real_type_error_is_a_negative_gate(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture(root, 'value: int = "wrong"\n')
            receipt = run_checks(root)
            checks = {item["name"]: item for item in receipt["checks"]}
            self.assertEqual(receipt["status"], "failed", receipt)
            self.assertEqual(checks["lint"]["status"], "passed")
            self.assertEqual(checks["types"]["status"], "failed")
            self.assertIn("assignment", checks["types"]["diagnostics"])

    def test_real_undefined_name_is_a_negative_lint_gate(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture(root, "value = missing_symbol\n")
            receipt = run_checks(root)
            checks = {item["name"]: item for item in receipt["checks"]}
            self.assertEqual(receipt["status"], "failed", receipt)
            self.assertEqual(checks["lint"]["status"], "failed")
            self.assertIn("F821", checks["lint"]["diagnostics"])


if __name__ == "__main__":
    unittest.main()
