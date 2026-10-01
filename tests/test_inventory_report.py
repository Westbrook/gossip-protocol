"""Inventory evidence stays source-bound, physical, and honest about missing history."""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from devtools.inventory_report import build_inventory, main, receipt_paths
from devtools.verify import digest, inputs, load_manifest


CLASS = "tests/test_case.py::ContractTests"
IDS = [f"{CLASS}.test_one", f"{CLASS}.test_two"]


def write(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


class InventoryReportTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        (self.root / "tests").mkdir()
        (self.root / "tests/test_case.py").write_text(
            "import unittest\nraise RuntimeError('inventory must not import tests')\n"
            "class ContractTests(unittest.TestCase):\n"
            " def test_one(self): pass\n def test_two(self): pass\n")
        write(self.root / "verification-manifest.json", {
            "schema_version": 1, "test_roots": ["tests"], "input_globs": ["tests/*.py", "verification-manifest.json"],
            "classes": {CLASS: {"lane": "fast", "weight": 1, "timeout_seconds": 30,
                                "invariant": "Atomic publication", "distinct_defect": "Partial output",
                                "boundary": "Filesystem", "disposition": "Keep", "overlap_reason": "Owns commit barrier"}},
        })

    def session(self, name="first", status="passed", timeout=False):
        directory = self.root / "receipts" / name
        identity = {"inputs": inputs(self.root, load_manifest(self.root)), "runtime": {"python": "fixture"}, "workers": 1}
        original = {"schema_version": 1, "physical": True, "class": CLASS, "lane": "fast", "status": status,
                    "cleanup_status": "completed", "duration_seconds": 5.0, "setup_seconds": 2.0,
                    "run_seconds": 2.5, "cleanup_seconds": .5,
                    "tests": [{"id": item, "status": status, "detail": "subprocess.TimeoutExpired: timed out" if timeout else ""} for item in IDS]}
        write(directory / "result.json", original)
        log = directory / "worker.log"
        log.write_text("physical execution\n")
        job = {**original, "returncode": 0 if status == "passed" else 1,
               "evidence_path": str(directory / "result.json"), "evidence_sha256": hashlib.sha256((directory / "result.json").read_bytes()).hexdigest(),
               "log": str(log), "log_sha256": hashlib.sha256(log.read_bytes()).hexdigest(), "wall_seconds": 5.1}
        summary = {"schema_version": 1, "fingerprint": digest(identity), "static": {"status": "passed"},
                   "status": status, "stale_inputs": False, "jobs": [job]}
        write(directory / "inputs.json", identity)
        write(directory / "summary.json", summary)
        return directory / "summary.json", summary, identity

    def test_default_inventory_is_import_free_and_has_no_implied_execution(self):
        nested = self.root / "runs/nested/repository/summary.json"
        write(nested, {"bad": "must not be read"})
        report = build_inventory(self.root)
        row = report["rows"][0]
        self.assertEqual(report["discovered_methods"], 2)
        self.assertEqual(row["invariant"], "Atomic publication")
        self.assertEqual(row["methods"], IDS)
        self.assertFalse(row["current_complete_pass"])
        self.assertEqual(row["cost"]["duration_seconds"]["samples"], 0)
        self.assertEqual(report["selected_sessions"], [])

    def test_reuse_uses_original_physical_duration_once_not_cached_zero(self):
        source, summary, identity = self.session()
        reused = self.root / "receipts/reused/summary.json"
        summary["jobs"][0].update(physical=False, reused_from=str(source), duration_seconds=0, wall_seconds=0,
                                   original_duration_seconds=999)
        write(reused, summary)
        write(reused.with_name("inputs.json"), identity)
        report = build_inventory(self.root, receipts=[source, reused], current_identity=identity)
        row = report["rows"][0]
        self.assertEqual(row["physical_observations"], 1)
        self.assertEqual(row["reuse_observations"], 1)
        self.assertEqual(len(row["cost_by_execution_identity"]), 1)
        self.assertEqual(row["cost_by_execution_identity"][0]["ordered_methods"], IDS)
        self.assertEqual(row["cost"]["duration_seconds"]["median"], 5)
        self.assertIsNone(row["cost"]["duration_seconds"]["p95"])
        self.assertTrue(row["current_complete_pass"])

    def test_source_match_alone_is_not_full_current_verification(self):
        source, _, identity = self.session()
        row = build_inventory(self.root, receipts=[source])["rows"][0]
        self.assertEqual(row["observations"][0]["source_status"], "exact")
        self.assertFalse(row["current_complete_pass"])
        changed_runtime = copy.deepcopy(identity)
        changed_runtime["runtime"]["python"] = "different"
        row = build_inventory(self.root, receipts=[source], current_identity=changed_runtime)["rows"][0]
        self.assertEqual(row["observations"][0]["execution_identity_status"], "different")
        self.assertFalse(row["current_complete_pass"])

    def test_changed_source_preserves_historical_failure_and_timeout_without_flaky_rate(self):
        source, _, identity = self.session(status="error", timeout=True)
        with (self.root / "tests/test_case.py").open("a") as handle:
            handle.write("\n# new source\n")
        row = build_inventory(self.root, receipts=[source])["rows"][0]
        self.assertEqual(row["observations"][0]["source_status"], "different")
        self.assertEqual(len(row["failure_history"]), 1)
        self.assertEqual(row["timeout_observations"], 1)
        self.assertIsNone(row["flaky_rate"])
        self.assertFalse(row["current_complete_pass"])
        with self.assertRaisesRegex(ValueError, "current authored"):
            build_inventory(self.root, current_identity=identity)

    def test_tampered_physical_log_and_input_bindings_are_never_accepted(self):
        source, summary, identity = self.session()
        Path(summary["jobs"][0]["log"]).write_text("tampered\n")
        row = build_inventory(self.root, receipts=[source], current_identity=identity)["rows"][0]
        self.assertEqual(row["observations"][0]["evidence"]["status"], "unavailable")
        self.assertFalse(row["current_complete_pass"])
        self.assertEqual(row["physical_observations"], 0)
        identity["runtime"]["python"] = "altered after summary"
        write(source.with_name("inputs.json"), identity)
        report = build_inventory(self.root, receipts=[source])
        self.assertEqual(report["selected_sessions"][0]["binding"]["status"], "unavailable")

    def test_missing_worker_result_keeps_controller_timeout_and_not_run_distinct(self):
        source, summary, _ = self.session(status="error")
        job = summary["jobs"][0]
        Path(job["evidence_path"]).unlink()
        job.update(reason="class deadline exceeded", cleanup_status="unknown after bounded termination")
        summary["jobs"].append({"class": "deleted.py::Old", "lane": "git", "status": "not_run", "physical": False, "tests": []})
        write(source, summary)
        report = build_inventory(self.root, receipts=[source])
        row = report["rows"][0]
        self.assertEqual(row["timeout_observations"], 1)
        self.assertEqual(row["failure_history"][0]["controller_wall_seconds"], 5.1)
        self.assertIsNone(row["failure_history"][0]["physical_cost"])
        self.assertEqual(report["unmatched_historical_jobs"][0]["status"], "not_run")

    def test_shallow_receipt_roots_do_not_traverse_generated_repositories(self):
        source, _, _ = self.session()
        write(self.root / "receipts/deeper/repository/summary.json", {"malformed": True})
        paths = receipt_paths([source, source.parent], [self.root / "receipts"])
        self.assertEqual(paths, [source.resolve()])
        with self.assertRaisesRegex(ValueError, "does not exist"):
            receipt_paths([], [self.root / "missing"])

    def test_cli_refuses_to_replace_existing_report(self):
        output = self.root / "rollup.json"
        args = ["--root", str(self.root), "--output", str(output)]
        self.assertEqual(main(args), 0)
        original = output.read_bytes()
        with self.assertRaises(FileExistsError):
            main(args)
        self.assertEqual(output.read_bytes(), original)


if __name__ == "__main__":
    unittest.main()
