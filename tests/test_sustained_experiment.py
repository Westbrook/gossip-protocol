from contextlib import redirect_stderr, redirect_stdout
from copy import deepcopy
import io
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import Mock

from gossip_harness.gitstore import GitStore
from gossip_harness.ledger import Ledger
from gossip_harness.sustained_experiment import (
    CORE, POLICIES, PROTOCOL, checked_evaluate, contract, credential, digest, finalize_case,
    known_result, promote_files, projects, record_stage,
    validate_rehearsal,
)


class SustainedContractTests(unittest.TestCase):
    def proof(self, expected):
        return {
            "experiment": PROTOCOL, "mode": "rehearsal", "status": "finished",
            "contract": deepcopy(expected), "unexecuted": [],
            "cases": [{"project_id": project, "policy": policy, "repetition": 0,
                       "accepted": True, "rehearsal_repair_verified": True,
                       "handoff": {"verified": True, "previous_pid": 101, "resumed_pid": 202}}
                      for project in expected["project_ids"] for policy in POLICIES],
        }

    def test_contract_binds_quality_metrics_sources_roster_and_handoff(self):
        value = contract("frozen-image-for-unit-test")
        self.assertEqual(value["protocol"], PROTOCOL)
        self.assertEqual(value["image"], "frozen-image-for-unit-test")
        self.assertEqual(value["project_ids"], [project["id"] for project in projects()])
        self.assertEqual(len(set(value["project_ids"])), 2)
        self.assertEqual(value["policies"], list(POLICIES))
        self.assertEqual(value["milestones"], 3)
        self.assertIs(value["hidden_feedback"], False)
        self.assertEqual(value["metrics_order"][:2], ["final requirement coverage", "whole-project completion"])
        self.assertEqual(value["metrics_order"][-2:], ["cost", "elapsed time"])
        self.assertIn("new PID", value["interruption"])
        self.assertEqual(set(value["sources"]), set(CORE))
        for sha in value["sources"].values():
            self.assertRegex(sha, r"^[a-f0-9]{64}$")
        self.assertRegex(value["fixture_sha256"], r"^[a-f0-9]{64}$")
        self.assertEqual(set(value["models"]), {"cheap", "strong"})

    def test_rehearsal_requires_exact_complete_roster_and_contract(self):
        expected = {"project_ids": ["workflow", "inventory"], "source": "frozen"}
        proof = self.proof(expected)
        validate_rehearsal(proof, expected)
        invalid = []
        for field, value in (("experiment", "other"), ("mode", "live"),
                             ("status", "interrupted"), ("unexecuted", [["remaining"]])):
            altered = deepcopy(proof)
            altered[field] = value
            invalid.append(altered)
        altered = deepcopy(proof)
        altered["contract"]["source"] = "changed"
        invalid.append(altered)
        altered = deepcopy(proof)
        altered["cases"].pop()
        invalid.append(altered)
        altered = deepcopy(proof)
        altered["cases"][-1] = deepcopy(altered["cases"][0])
        invalid.append(altered)
        altered = deepcopy(proof)
        altered["cases"][0]["repetition"] = 1
        invalid.append(altered)
        for altered in invalid:
            with self.subTest(altered=altered):
                with self.assertRaises(ValueError):
                    validate_rehearsal(altered, expected)

    def test_rehearsal_requires_real_handoff_repair_and_acceptance(self):
        expected = {"project_ids": ["workflow", "inventory"]}
        proof = self.proof(expected)
        for change in ({"handoff": {}}, {"handoff": {"verified": False}},
                       {"handoff": {"verified": True, "previous_pid": 101, "resumed_pid": 101}},
                       {"accepted": False}, {"rehearsal_repair_verified": False}):
            with self.subTest(change=change):
                altered = deepcopy(proof)
                altered["cases"][0].update(change)
                with self.assertRaises(ValueError):
                    validate_rehearsal(altered, expected)


class SustainedEvaluationTests(unittest.TestCase):
    cases = [{"input": {"case": 0}, "expected": 0}, {"input": {"case": 1}, "expected": 1}]

    def receipt(self, passed=True):
        return {"status": "passed" if passed else "failed", "passed": passed,
                "cleanup_verified": True,
                "source_sha256": digest({"solution.py": "pass"}), "suite_sha256": digest(self.cases),
                "outcomes": [{"index": 0, "passed": True}, {"index": 1, "passed": passed}]}

    def test_accepts_ordered_consistent_pass_and_test_failure(self):
        for passed in (True, False):
            receipt = self.receipt(passed)
            validator = Mock()
            validator.evaluate.return_value = receipt
            self.assertIs(checked_evaluate(validator, {"solution.py": "pass"}, self.cases), receipt)
            validator.evaluate.assert_called_once_with({"solution.py": "pass"}, self.cases)

    def test_malformed_or_inconsistent_receipts_fail_closed(self):
        invalid = []
        for field, value in (("status", "timeout"), ("cleanup_verified", False),
                             ("cleanup_verified", 1), ("passed", 1), ("passed", False),
                             ("status", "failed"), ("outcomes", []),
                             ("source_sha256", "0" * 64), ("suite_sha256", "0" * 64)):
            altered = self.receipt()
            altered[field] = value
            invalid.append(altered)
        for field, value in (("index", 1), ("index", False), ("passed", "true")):
            altered = self.receipt()
            altered["outcomes"][0][field] = value
            invalid.append(altered)
        for receipt in invalid:
            with self.subTest(receipt=receipt):
                validator = Mock()
                validator.evaluate.return_value = receipt
                with self.assertRaises(RuntimeError):
                    checked_evaluate(validator, {"solution.py": "pass"}, self.cases)


class SustainedKnownWorkerTests(unittest.TestCase):
    def test_rehearsal_builder_fault_is_limited_to_first_selected_backend(self):
        for project in projects():
            original = deepcopy(project)
            for stage in range(3):
                for candidate, round_number, broken in (("single", 1, True), ("c0", 1, True),
                                                         ("c1", 1, False), ("single", 2, False), ("c0", 2, False)):
                    with self.subTest(project=project["id"], stage=stage, candidate=candidate, round=round_number):
                        result = known_result(project, stage,
                                              {"role": "builder", "candidate_id": candidate, "round": round_number},
                                              project["initial_files"])
                        self.assertEqual(set(result.changes), {*project["allowed_paths"], "control.json"})
                        self.assertEqual(result.usage_units, 0)
                        self.assertEqual(result.metadata["api_calls"], 0)
                        control = json.loads(result.changes["control.json"])
                        self.assertEqual(control["action"], "complete")
                        self.assertEqual(control["remaining"], [])
                        self.assertTrue(control["notes"])
                        for path in project["allowed_paths"]:
                            expected = ('raise RuntimeError("deliberate rehearsal fault")\n'
                                        if broken and path == project["allowed_paths"][-1]
                                        else project["stages"][stage]["known_files"][path])
                            self.assertEqual(result.changes[path], expected)
            self.assertEqual(project, original)

    def test_rehearsal_reviewer_forces_repair_then_accepts_without_editing_source(self):
        for project in projects():
            for round_number in (1, 2):
                result = known_result(project, 1, {"role": "reviewer", "round": round_number}, {})
                self.assertEqual(set(result.changes), {"review.json"})
                review = json.loads(result.changes["review.json"])
                self.assertEqual(review["candidate"], "c0")
                self.assertEqual(review["action"], "repair" if round_number == 1 else "accept")
                self.assertEqual(review["remaining"], list(project["stages"][1]["requirements"]) if round_number == 1 else [])


class SustainedCredentialTests(unittest.TestCase):
    def test_synthetic_key_loading_and_errors_do_not_print_credentials(self):
        synthetic = "sk-synthetic-unit-test-not-a-real-credential"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "synthetic.env"
            stdout, stderr = io.StringIO(), io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                path.write_text(f"# Synthetic fixture\nexport OPENAI_API_KEY='{synthetic}'\n")
                self.assertEqual(credential(path), synthetic)
                path.write_text(f"OPENAI_API_KEY={synthetic}\nOPENAI_API_KEY={synthetic}\n")
                with self.assertRaises(ValueError) as error:
                    credential(path)
                self.assertNotIn(synthetic, str(error.exception))
                link = Path(directory) / "symlink.env"
                link.symlink_to(path)
                with self.assertRaises(ValueError):
                    credential(link)
                with self.assertRaises(ValueError):
                    credential(Path(directory) / "missing.env")
            self.assertEqual(stdout.getvalue(), "")
            self.assertEqual(stderr.getvalue(), "")


class SustainedPublicationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.shared = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.shared.cleanup)
        cls.initial = {"solution.py": "def solve(value): return None\n", "scaffold.txt": "trusted\n"}
        cls.base = GitStore.create(Path(cls.shared.name) / "base.git", cls.initial)
        cls.old_head = cls.base.head()
        cls.source = GitStore.fork(cls.base, Path(cls.shared.name) / "proposal.git")
        cls.files = {**cls.initial, "solution.py": "def solve(value): return value\n"}
        cls.tip = cls.source.propose({"solution.py": cls.files["solution.py"]})
        cls.binding = {"store_path": str(cls.source.path), "tip_sha": cls.tip}

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.ledger = Ledger(self.root / "ledger.sqlite", budget_units=10)
        self.ledger.add_task("stage-record")
        self.lease = self.ledger.claim("stage-record", "controller", now=time.time(), ttl=600)
        self.ledger.reserve("settled-call", self.lease, 8, now=time.time())
        self.ledger.settle("settled-call", 3)

    def test_exact_publication_preserves_shared_source_and_accounting(self):
        release = promote_files(self.base, self.binding, self.root / "release.git", self.files,
                                self.ledger, self.lease, allowed_paths=("solution.py",))
        self.assertEqual(release.head(), self.tip)
        self.assertEqual(release.read_files(), self.files)
        self.assertEqual(self.base.head(), self.old_head)
        self.assertEqual(self.ledger.task("stage-record")["accepted_commit"], self.tip)
        self.assertEqual(self.ledger.budget()["spent_or_reserved"], 3)
        self.assertEqual(self.ledger.pending_intents(), [])

    def test_mismatched_exact_files_never_accepts_the_project(self):
        different = {**self.files, "solution.py": "def solve(value): return 'untested'\n"}
        with self.assertRaises((ValueError, RuntimeError)):
            promote_files(self.base, self.binding, self.root / "rejected.git", different,
                          self.ledger, self.lease, allowed_paths=("solution.py",))
        self.assertEqual(GitStore(self.root / "rejected.git").head(), self.old_head)
        self.assertEqual(self.ledger.task("stage-record")["status"], "claimed")
        self.assertEqual(self.ledger.pending_intents(), [])

    def test_final_release_can_publish_an_already_accepted_checkpoint_tree(self):
        GitStore.fork(self.base, self.root / "initial.git")
        checkpoint = promote_files(self.base, self.binding, self.root / "checkpoint.git", self.files,
                                   self.ledger, self.lease, allowed_paths=("solution.py",))
        project = {"id": "synthetic-project", "allowed_paths": ["solution.py"], "stages": []}
        stage_results = []
        for index in range(3):
            case = {"input": index, "expected": index, "requirement": f"stage-{index}"}
            project["stages"].append({"visible_cases": [case], "hidden_cases": [case]})
            stage_results.append({"files": self.files, "selected_binding": self.binding,
                                  "metrics": {metric: 0 for metric in (
                                      "builder_calls", "reviewer_calls", "repairs", "premature_completion",
                                      "regressions", "stagnation_events")}})
        state = {"files": self.files, "store_path": str(checkpoint.path), "head": checkpoint.head(),
                 "completed_stages": [{"completed": True}] * 3, "stages": stage_results,
                 "invocations": [], "terminal": "visible_complete"}
        (self.root / "trajectory.json").write_text(json.dumps(state))
        validator = Mock()

        def evaluate(files, cases):
            return {"status": "passed", "passed": True, "cleanup_verified": True,
                    "source_sha256": digest(files), "suite_sha256": digest(cases),
                    "outcomes": [{"index": index, "passed": True} for index in range(len(cases))]}

        validator.evaluate.side_effect = evaluate
        result = finalize_case(self.root, project,
                               {"policy": "strong-single", "repetition": 0, "run_id": "synthetic", "live": False},
                               validator)
        self.assertIs(result["accepted"], True)
        self.assertEqual(result["release_head"], self.tip)
        self.assertEqual(result["exact_tested_sha"], self.tip)
        self.assertEqual(GitStore(self.root / "release.git").read_files(), self.files)
        self.assertEqual((self.root / "accepted" / "solution.py").read_text(), self.files["solution.py"])
        self.assertEqual(self.base.head(), self.old_head)

    def test_completed_accounting_record_does_not_certify_incomplete_project(self):
        result = {"completed": False, "status": "max_steps_incomplete", "selected_binding": self.binding}
        record_stage(self.root, result, self.ledger, self.lease)
        record = GitStore(self.root / "record.git")
        saved = json.loads(record.read_files()["record.json"])
        self.assertEqual(saved, result)
        self.assertIs(saved["completed"], False)
        self.assertEqual(self.ledger.task("stage-record")["status"], "complete")
        self.assertEqual(self.ledger.task("stage-record")["accepted_commit"], record.head())
        self.assertNotEqual(record.head(), self.tip)
        self.assertEqual(self.base.head(), self.old_head)
        self.assertFalse((self.root / "release.git").exists())
        self.assertEqual(self.ledger.budget()["spent_or_reserved"], 3)


if __name__ == "__main__":
    unittest.main()
