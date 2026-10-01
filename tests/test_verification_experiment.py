from copy import deepcopy
import json
from pathlib import Path
import signal
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from gossip_harness.blackbox_validator import BlackboxValidator
from gossip_harness.gitstore import GitStore
from gossip_harness.ledger import Ledger
from gossip_harness import verification_experiment as runner
from gossip_harness.verification_stage import run_verification_stage


def receipt(files, cases, passed=None):
    prepared_files, execution_cases, _ = BlackboxValidator._inputs(files, cases)
    values = [True] * len(cases) if passed is None else passed
    okay = all(values)
    return {"status": "passed" if okay else "failed", "passed": okay, "cleanup_verified": True,
            "source_sha256": runner.digest(prepared_files), "suite_sha256": runner.digest(execution_cases),
            "image_id": "unit-test-image", "case_timeout_seconds": runner.CASE_TIMEOUT,
            "timeout_seconds": runner.SUITE_TIMEOUT,
            "outcomes": [{"index": index, "passed": value} for index, value in enumerate(values)]}


class VerificationContractTests(unittest.TestCase):
    def proof(self):
        expected = {"project_ids": ["buildgraph", "calendar"], "source": "frozen"}
        proof = {"experiment": runner.PROTOCOL, "mode": "rehearsal", "status": "finished",
                 "contract": deepcopy(expected), "unexecuted": [], "cases": []}
        for project in expected["project_ids"]:
            for policy in runner.POLICIES:
                proof["cases"].append({"project_id": project, "policy": policy, "repetition": 0,
                    "accepted": True, "fault": {"previous_pid": 101, "returncode": -signal.SIGKILL},
                    "resume": {"verified": True, "previous_pid": 101, "resumed_pid": 202, "stage_index": 1},
                    "metrics": {"repairs": 4, "admitted_new_probes": 4, "rejected_probes": 8},
                    "integration": {"stale_probe_status": "stale"}})
        return expected, proof

    def test_contract_binds_four_stage_common_review_and_qualification(self):
        with patch.object(runner.OpenAIWorker, "run", side_effect=AssertionError("No provider invocation")):
            value = runner.contract("unit-test-image")
        self.assertEqual(value["protocol"], runner.PROTOCOL)
        self.assertEqual(value["project_ids"], [project["id"] for project in runner.projects()])
        self.assertEqual(value["milestones"], 4)
        self.assertEqual(value["policies"], list(runner.POLICIES))
        self.assertEqual(value["initial_candidates"], {"strong-reviewed": 1, "cheap-reviewed": 1, "portfolio-reviewed": 4})
        self.assertEqual((value["repair_calls"], value["reviewer_calls"], value["probes_per_review"], value["new_probes_per_stage"]),
                         (4, 6, 4, 8))
        self.assertIs(value["hidden_feedback"], False)
        self.assertIn("before settlement", value["fault"])
        self.assertEqual(set(value["sources"]), set(runner.CORE))
        for sha in [value["plan_sha256"], value["fixture_sha256"], *value["sources"].values()]:
            self.assertRegex(sha, r"^[a-f0-9]{64}$")

    def test_exact_rehearsal_roster_contract_and_status_are_required(self):
        expected, proof = self.proof()
        runner.validate_rehearsal(proof, expected)
        bad = []
        for field, value in (("experiment", "other"), ("mode", "live"), ("status", "running"),
                             ("unexecuted", [["remaining"]]), ("contract", {**expected, "source": "changed"})):
            altered = deepcopy(proof)
            altered[field] = value
            bad.append(altered)
        altered = deepcopy(proof)
        altered["cases"].pop()
        bad.append(altered)
        altered = deepcopy(proof)
        altered["cases"][-1] = deepcopy(altered["cases"][0])
        bad.append(altered)
        for altered in bad:
            with self.subTest(altered=altered):
                with self.assertRaises(ValueError):
                    runner.validate_rehearsal(altered, expected)

    def test_rehearsal_rejects_forged_or_incomplete_fault_repair_probe_evidence(self):
        expected, proof = self.proof()
        mutations = [("accepted", False), ("accepted", 1), ("repetition", False),
                     ("repetition", 1), ("fault.returncode", 0), ("fault.previous_pid", 999),
                     ("fault.previous_pid", True), ("resume.previous_pid", 999),
                     ("resume.resumed_pid", 101), ("resume.resumed_pid", 0),
                     ("resume.verified", 1), ("resume.stage_index", True), ("resume.stage_index", 2),
                     ("metrics.repairs", 3), ("metrics.admitted_new_probes", 3),
                     ("metrics.rejected_probes", 7), ("integration.stale_probe_status", "accepted")]
        for field, value in mutations:
            with self.subTest(field=field, value=value):
                altered = deepcopy(proof)
                target = altered["cases"][0]
                keys = field.split(".")
                for key in keys[:-1]:
                    target = target[key]
                target[keys[-1]] = value
                with self.assertRaises(ValueError):
                    runner.validate_rehearsal(altered, expected)


class VerificationStateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temp.cleanup)
        cls.files = {"solution.py": "raise AssertionError('candidate must not execute on host')\n"}
        cls.store = GitStore.create(Path(cls.temp.name) / "source.git", cls.files)
        cls.head = cls.store.head()

    def state(self):
        return {"contract_sha": "a" * 64, "next_stage": 1,
                "stages": [{"completed": True, "stage_index": 0}],
                "files": dict(self.files), "store_path": str(self.store.path), "head": self.head}

    def test_state_is_bound_to_exact_contract_completed_history_and_git_source(self):
        state = self.state()
        loaded = runner.validate_state(state, "a" * 64)
        self.assertEqual(loaded.path, self.store.path)
        self.assertEqual(loaded.head(), self.head)
        changes = [dict(contract_sha="b" * 64), dict(next_stage=True), dict(next_stage=5),
                   dict(next_stage=0), dict(stages=[{"completed": False, "stage_index": 0}]),
                   dict(stages=[{"completed": 1, "stage_index": 0}]),
                   dict(stages=[{"completed": True, "stage_index": 1}]),
                   dict(stages=[{"completed": True, "stage_index": False}]),
                   dict(head="0" * 40), dict(files={"solution.py": "Changed"})]
        for change in changes:
            with self.subTest(change=change):
                altered = self.state()
                altered.update(change)
                with self.assertRaises(ValueError):
                    runner.validate_state(altered, "a" * 64)

    def test_cached_receipt_is_bound_to_source_suite_image_and_execution_limits(self):
        cases = [{"input": {}, "expected": None}]
        good = receipt(self.files, cases)
        self.assertIs(runner.verify_receipt(good, self.files, cases, "unit-test-image"), good)
        mutations = [("source_sha256", "0" * 64), ("suite_sha256", "0" * 64),
                     ("image_id", "other-image"), ("timeout_seconds", runner.SUITE_TIMEOUT + 1),
                     ("case_timeout_seconds", runner.CASE_TIMEOUT + 1), ("cleanup_verified", False),
                     ("passed", 1), ("status", "failed")]
        for field, value in mutations:
            with self.subTest(field=field):
                changed = deepcopy(good)
                changed[field] = value
                with self.assertRaises((ValueError, RuntimeError)):
                    runner.verify_receipt(changed, self.files, cases, "unit-test-image")


class VerificationOracleGateTests(unittest.TestCase):
    def origin(self, stage):
        return {"stage_index": stage, "review_round": 1, "source": "unit-test"}

    def test_generated_probe_receipt_uses_real_blackbox_projection_and_binds_expected_values(self):
        module = runner.fixture("buildgraph")
        project = module.PROJECT
        payload = {"commands": [{"op": "__projection_probe__"}]}
        proposal = {"input": payload, "expected": module.reference(0, payload),
                    "requirement": runner.requirements(project, 0)[0]}
        gate = runner.oracle_gate(module, project)
        cases = gate({"mode": "new", "probes": [proposal], "existing_cases": [], "max_new": 8},
                     0, self.origin(0))["admitted_cases"]
        self.assertEqual(len(cases), 1)
        files = {"solution.py": "def solve(value): return None\n"}
        prepared_files, execution_cases, _ = BlackboxValidator._inputs(files, cases)
        self.assertEqual(set(execution_cases[0]), {"input", "expected", "id", "requirement"})
        self.assertNotEqual(runner.digest(cases), runner.digest(execution_cases))
        failed = receipt(files, cases, [False])
        self.assertEqual(failed["source_sha256"], runner.digest(prepared_files))
        self.assertEqual(failed["suite_sha256"], runner.digest(execution_cases))
        self.assertIs(runner.verify_receipt(failed, files, cases, "unit-test-image"), failed)
        self.assertIs(failed["passed"], False)
        changed = deepcopy(cases)
        changed[0]["expected"] = ["altered expected value"]
        with self.assertRaises(RuntimeError):
            runner.verify_receipt(failed, files, changed, "unit-test-image")

    def test_real_fixtures_admit_correct_data_reject_bad_labels_and_duplicates(self):
        for module in runner.fixture_modules():
            with self.subTest(project=module.PROJECT["id"]):
                project = module.PROJECT
                case = deepcopy(project["stages"][0]["visible_cases"][0])
                proposal = {key: case[key] for key in ("input", "expected", "requirement")}
                wrong = deepcopy(proposal)
                wrong["input"] = {"commands": [{"op": "__incorrect_label_probe__"}]}
                wrong["expected"] = ["Deliberately incorrect oracle label"]
                gate = runner.oracle_gate(module, project)
                result = gate({"mode": "new", "probes": [proposal, wrong, deepcopy(proposal)],
                               "existing_cases": [], "max_new": 8}, 0, self.origin(0))
                self.assertIs(result["oracle_assisted"], True)
                self.assertEqual(len(result["admitted_cases"]), 1)
                self.assertEqual(len(result["rejected"]), 2)
                self.assertEqual(result["admitted_cases"][0]["expected"], proposal["expected"])
                self.assertTrue(all("expected" not in rejected for rejected in result["rejected"]))

    def test_revalidation_uses_saved_probes_not_the_public_existing_case_pool(self):
        module = runner.fixture("calendar")
        project = module.PROJECT
        payload = {"commands": [module.book("long", start=0, end=20)]}
        expected = module.reference(0, payload)
        gate = runner.oracle_gate(module, project)
        admitted = gate({"mode": "new", "probes": [{"input": payload, "expected": expected,
                         "requirement": runner.requirements(project, 0)[0]}],
                         "existing_cases": [], "max_new": 8}, 0, self.origin(0))["admitted_cases"]
        self.assertEqual(len(admitted), 1)
        envelope = {"mode": "revalidate", "probes": admitted,
                    "existing_cases": list(project["stages"][1]["visible_cases"]), "max_new": 1}
        retained = gate(envelope, 1, self.origin(1))
        self.assertEqual(retained["admitted_cases"][0]["id"], admitted[0]["id"])
        self.assertEqual(retained["admitted_cases"][0]["validated_stage_index"], 1)
        envelope["probes"] = retained["admitted_cases"]
        retired = gate(envelope, 3, self.origin(3))
        self.assertEqual(retired["admitted_cases"], [])
        self.assertEqual([row["id"] for row in retired["retired"]], [admitted[0]["id"]])
        self.assertTrue(all("expected" not in row for row in retired["retired"]))
        self.assertNotEqual(module.reference(3, payload), expected)

    def test_oracle_admission_does_not_execute_candidate_or_known_source_text(self):
        with tempfile.TemporaryDirectory() as directory:
            sentinel = Path(directory) / "executed"
            poison = f"from pathlib import Path\nPath({str(sentinel)!r}).write_text('unsafe')\n"
            for module in runner.fixture_modules():
                project = deepcopy(module.PROJECT)
                project["initial_files"] = {name: poison for name in project["initial_files"]}
                for stage in project["stages"]:
                    stage["known_files"] = {name: poison for name in stage["known_files"]}
                gate = runner.oracle_gate(module, project)
                payload = {"commands": [{"op": "__data_only_probe__"}]}
                proposal = {"input": payload, "expected": module.reference(0, payload),
                            "requirement": runner.requirements(project, 0)[0]}
                with patch("subprocess.run", side_effect=AssertionError("Host execution forbidden")), \
                     patch("subprocess.Popen", side_effect=AssertionError("Host execution forbidden")):
                    result = gate({"mode": "new", "probes": [proposal], "existing_cases": [], "max_new": 8},
                                  0, self.origin(0))
                self.assertEqual(len(result["admitted_cases"]), 1)
            self.assertFalse(sentinel.exists())

    def test_stage_engine_carries_real_oracle_probes_across_repaired_milestones(self):
        module = runner.fixture("buildgraph")
        project = module.PROJECT
        previous = {}
        files = project["initial_files"]
        for stage in (0, 1):
            expected_files = {**files, **{name: project["stages"][stage]["known_files"][name]
                                         for name in project["allowed_paths"]}}

            def invoke(**request):
                return runner.known_result(project, module, stage, request["metadata"])

            def evaluate(candidate_files, cases, label):
                return receipt(candidate_files, cases, [candidate_files == expected_files] * len(cases))

            result = run_verification_stage(project, stage, "portfolio-reviewed", files, previous,
                invoke=invoke, evaluate=evaluate,
                retain=lambda candidate_files, label: {"tip_sha": runner.digest(candidate_files),
                                                       "files_sha256": runner.digest(candidate_files), "label": label},
                validate_probes=runner.oracle_gate(module, project), emit=lambda *args, **kwargs: None,
                candidate_seed=7)
            self.assertIs(result["completed"], True)
            self.assertEqual(result["metrics"]["repairs"], 1)
            self.assertEqual(result["metrics"]["admitted_new_probes"], 1)
            self.assertGreaterEqual(result["metrics"]["rejected_probes"], 2)
            self.assertEqual(len(result["probe_pool"]), stage + 1)
            self.assertEqual(result["files"], expected_files)
            previous, files = result, result["files"]


class VerificationChildReplayTests(unittest.TestCase):
    def test_changed_source_contract_stops_before_credentials_or_accounting(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = root / "config.json"
            runner.save(config, {"root": str(root), "image": "unit-test-image", "contract_sha": "changed"})
            with patch.object(runner, "contract", return_value={"source": "frozen"}), \
                 patch.object(runner, "credential", side_effect=AssertionError("No key access")) as key, \
                 patch.object(runner, "Ledger", side_effect=AssertionError("No accounting access")) as ledger:
                with self.assertRaisesRegex(ValueError, "preregistration"):
                    runner.child(config, resumed=True)
            key.assert_not_called()
            ledger.assert_not_called()

    def test_tampered_checkpoint_bytes_stop_replay_before_provider_or_candidate_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            contract = {"source": "frozen"}
            ledger = Ledger(root / "budget.sqlite", budget_units=0)
            state_path = root / "state.json"
            sha = runner.save(state_path, {"contract_sha": runner.digest(contract), "next_stage": 0, "stages": []})
            runner.save(root / "state-binding.json", {"sha256": sha})
            state_path.write_bytes(state_path.read_bytes() + b" ")
            config = root / "config.json"
            runner.save(config, {"root": str(root), "contract_sha": runner.digest(contract),
                                 "image": "unit-test-image", "project_id": "buildgraph",
                                 "budget_ledger": str(ledger.path), "live": True})
            with patch.object(runner, "contract", return_value=contract), \
                 patch.object(runner, "credential", side_effect=AssertionError("No key access")) as key, \
                 patch.object(runner, "BlackboxValidator") as validator:
                with self.assertRaisesRegex(ValueError, "Checkpoint bytes"):
                    runner.child(config, resumed=True)
            key.assert_not_called()
            validator.return_value.evaluate.assert_not_called()
            self.assertEqual(ledger.budget(), {"limit": 0, "spent_or_reserved": 0, "remaining": 0})


class VerificationFinalGateTests(unittest.TestCase):
    def test_final_generated_probe_failure_blocks_release_and_preserves_real_metrics(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            files = {"solution.py": "raise AssertionError('host execution forbidden')\n"}
            generated = {"id": "generated-blocker", "input": "probe", "expected": True, "requirement": "quality"}
            project = {"id": "synthetic", "allowed_paths": ["solution.py"], "stages": []}
            stages = []
            for index in range(4):
                visible = {"id": f"visible-{index}", "input": index, "expected": index, "requirement": "quality"}
                hidden = {**visible, "id": f"hidden-{index}"}
                project["stages"].append({"visible_cases": [visible], "hidden_cases": [hidden]})
                stages.append({"completed": True, "invocations": [], "probe_pool": [generated] if index == 3 else [],
                               "metrics": {"admitted_new_probes": 2, "generated_probe_attempts": 3,
                                           "rejected_probes": 1, "blocked_acceptances": 1}})
            runner.save(root / "trajectory.json", {"files": files, "stages": stages, "terminal": "visible_complete"})
            validator = Mock()
            validator.evaluate.side_effect = lambda source, cases: receipt(
                source, cases, [case["id"] != generated["id"] for case in cases])
            config = {"project_id": "synthetic", "run_id": "synthetic-run", "policy": "cheap-reviewed",
                      "repetition": 0, "image": "unit-test-image"}
            with patch.object(runner, "fixture", return_value=SimpleNamespace(PROJECT=project)), \
                 patch.object(runner, "local_promote", side_effect=AssertionError("Failed quality cannot release")):
                result = runner.finalize(root, config, validator)
            self.assertIs(result["accepted"], False)
            self.assertIsNone(result["release_head"])
            self.assertEqual(result["status"], "final_quality_failed")
            self.assertIs(result["final_hidden"]["passed"], True)
            self.assertIs(result["final_visible"]["passed"], False)
            self.assertEqual(result["metrics"]["admitted_new_probes"], 8)
            self.assertEqual(result["metrics"]["generated_probe_attempts"], 12)
            self.assertEqual(result["metrics"]["rejected_probes"], 4)
            self.assertEqual(result["metrics"]["blocked_acceptances"], 4)


if __name__ == "__main__":
    unittest.main()
