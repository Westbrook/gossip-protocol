"""Fresh pinned sandbox checks of the combined authored M3 tree.

Independent inputs and expectations are retained host-side; the child observes
public APIs. These are finite reference checks, not candidate-study evidence.
"""
from __future__ import annotations

import hashlib
import json
import os
import unittest

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness.blackbox_validator import BlackboxValidator, SUPERVISOR_ADAPTER
from gossip_harness.library_m2_acceptance_cases_v1 import (
    CHILD_ADAPTER as M2_ADAPTER, acceptance_cases as m2_cases, registry_manifest as m2_registry,
)
from gossip_harness.library_m3_acceptance_cases_v1 import CHILD_ADAPTER, acceptance_cases, registry_manifest
from gossip_harness.library_m3_reference_v1 import m3_files
from gossip_harness.library_project_fixture_v1 import public_cases
from gossip_harness.pilot import DEFAULT_IMAGE
from gossip_harness.sandbox import DockerValidator


def _save(path, value):
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, sort_keys=True, ensure_ascii=True, indent=2)
        stream.write("\n")


@unittest.skipUnless(os.environ.get("GOSSIP_RUN_DOCKER_TESTS") == "1", "explicit real Docker lane")
class LibraryM3DockerReferenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ready, detail = BlackboxValidator(DEFAULT_IMAGE).preflight()
        if not ready:
            raise RuntimeError(detail)

    def evaluate(self, label, *, milestone="m3", defect=None):
        cases = public_cases("m1") if milestone == "m1" else m2_cases() if milestone == "m2" else acceptance_cases()
        files = m3_files()
        if defect == "restore_edit_token_not_advanced":
            cases = [case for case in cases if case["id"] == "m3-restore-removal-edit-and-job-aba"]
            path = "library/catalog/backup.py"
            original = "max(record['edit_version'],document_highwater.get(identifier,0)) + 1"
            self.assertEqual(files[path].count(original), 1)
            files[path] = files[path].replace(original, "max(record['edit_version'],document_highwater.get(identifier,0))")
        elif defect == "worker_ignores_enrollment":
            cases = [case for case in cases if case["id"] == "m3-worker-once-explicit-enrollment-order"]
            path = "library/catalog/worker.py"
            original = "WHERE c.enrolled=1 AND j.state IN"
            self.assertEqual(files[path].count(original), 1)
            files[path] = files[path].replace(original, "WHERE j.state IN")
        if defect:
            self.assertEqual(len(cases), 1)
        validator = BlackboxValidator(DEFAULT_IMAGE, timeout_seconds=240, case_timeout_seconds=25)
        if milestone != "m1":
            validator._sandbox = DockerValidator(DEFAULT_IMAGE,
                {"supervisor.py":SUPERVISOR_ADAPTER,"child.py":M2_ADAPTER if milestone == "m2" else CHILD_ADAPTER},
                command=("python","-I","/checks/supervisor.py"),timeout_seconds=240)
        with ArtifactDirectory(label, retain_success=True) as artifacts:
            _save(artifacts.root / "case-definitions.json", cases)
            _save(artifacts.root / "authored-source.json", files)
            _save(artifacts.root / "pre-execution-freeze.json", {
                "purpose":"authored_reference_qualification",
                "not_experimental_candidate_acceptance":True,
                "cases_registry":None if milestone == "m1" else m2_registry() if milestone == "m2" else registry_manifest(),
                "definitions_file_sha256":hashlib.sha256((artifacts.root / "case-definitions.json").read_bytes()).hexdigest(),
                "source_file_sha256":hashlib.sha256((artifacts.root / "authored-source.json").read_bytes()).hexdigest(),
                "image":DEFAULT_IMAGE,"adapter_sha256":validator._sandbox.checks_sha256,
                "timeout_seconds":240,"case_timeout_seconds":25,"deliberate_defect":defect})
            result = validator.evaluate(files, cases)
            _save(artifacts.root / "execution-receipt.json", result)
            self.assertTrue(result.get("cleanup_verified"), result.get("status"))
            return result

    def test_inherited_m1_histories_on_combined_m3(self):
        result = self.evaluate("m3-inherited-m1", milestone="m1")
        self.assertTrue(result["passed"], [(o.get("id"),o["status"]) for o in result["outcomes"]])
        self.assertEqual(len(result["outcomes"]), 8)

    def test_inherited_independent_m2_histories_on_combined_m3(self):
        result = self.evaluate("m3-inherited-m2", milestone="m2")
        self.assertTrue(result["passed"], [(o.get("id"),o["status"]) for o in result["outcomes"]])
        self.assertEqual(len(result["outcomes"]), 12)

    def test_independent_m3_histories_on_combined_m3(self):
        result = self.evaluate("m3-independent-reference")
        self.assertTrue(result["passed"], [(o.get("id"),o["status"]) for o in result["outcomes"]])
        self.assertEqual(len(result["outcomes"]), len(acceptance_cases()))

    def test_independent_restore_history_detects_unadvanced_token(self):
        result = self.evaluate("m3-restore-token-defect", defect="restore_edit_token_not_advanced")
        self.assertFalse(result["passed"])
        self.assertEqual([(o["id"],o["status"]) for o in result["outcomes"]],
                         [("m3-restore-removal-edit-and-job-aba","wrong_answer")])

    def test_independent_worker_history_detects_unenrolled_execution(self):
        result = self.evaluate("m3-worker-enrollment-defect", defect="worker_ignores_enrollment")
        self.assertFalse(result["passed"])
        self.assertEqual([(o["id"],o["status"]) for o in result["outcomes"]],
                         [("m3-worker-once-explicit-enrollment-order","wrong_answer")])
