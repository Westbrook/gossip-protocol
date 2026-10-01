"""Eligibility-diagnostic guards with synthetic evidence and no candidate execution."""
from contextlib import ExitStack, contextmanager
from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from analysis import run_verification_eligibility_diagnostic as runner


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def receipt(files, cases, contract, failed=()):
    rows = [dict(index=index, id=case["id"], requirement=case["requirement"],
                 actual=None if case["id"] in failed else case["expected"],
                 passed=case["id"] not in failed,
                 status="wrong_answer" if case["id"] in failed else "passed")
            for index, case in enumerate(cases)]
    passed = all(row["passed"] for row in rows)
    return dict(schema_version=1, protocol="gossip-blackbox-v1", passed=passed,
                status="passed" if passed else "failed", cleanup_verified=True,
                source_sha256=runner.digest(files), suite_sha256=runner.digest(cases),
                image_id=contract["image"], adapter_sha256=runner.frozen.ADAPTER_SHA256,
                case_timeout_seconds=12, timeout_seconds=300, case_count=len(cases),
                exit_code=0, timed_out=False, output_truncated=False,
                input_delivery_failed=False, outcomes=rows)


class SyntheticStudy:
    """Real immutable JSON bindings; Git reads return inert strings only."""

    def __init__(self, directory):
        self.base = Path(directory).resolve()
        self.study = self.base / "primary"
        self.output = self.base / "diagnostic"
        self.repository = self.base / "repository"
        self.finding_path = self.base / "finding.json"
        self.root = self.study / runner.RUN_ID
        self.stage_root = self.root / "stage-2"
        self.git = {}
        self.runtime = {"synthetic_runtime": True}
        helper = b"# Inert trusted helper fixture; never imported.\n"
        for root in (self.repository, self.study / "source-snapshot"):
            path = root / "gossip_harness/blackbox_validator.py"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(helper)
        (self.study / "study-plan.json").write_bytes(b"{}")
        initial = {"solution.py": "raise AssertionError('Never execute fixture sources')\n",
                   "backend.py": "initial", "policy.json": "old"}
        stages = [dict(hidden_cases=[dict(id=f"h{index}-{case}", input={"case": case},
                                        expected=case + 1, requirement=f"R{index}")
                                   for case in range(12)],
                       trusted_updates={"policy.json": "new"} if index == 1 else {})
                  for index in range(4)]
        self.projects = [dict(id="buildgraph", initial_files=initial, allowed_paths=["backend.py"], stages=stages),
                         dict(id="calendar", initial_files=initial, allowed_paths=["backend.py"], stages=deepcopy(stages))]
        self.suite = [case for stage in stages[:3] for case in stage["hidden_cases"]]
        self.contract = dict(protocol="verification-quality-v1", policies=list(runner.frozen.POLICIES),
            milestones=4, project_ids=["buildgraph", "calendar"], initial_candidates={"portfolio-reviewed": 4},
            image="sha256:" + "a" * 64, case_timeout_seconds=12, suite_timeout_seconds=300,
            sources={"blackbox_validator.py": runner.sha(helper)}, controller_runtime=self.runtime,
            plan_sha256=runner.sha(b"{}"), fixture_sha256=runner.digest(self.projects))
        self.roster = [(project, policy, repeat) for project in self.contract["project_ids"]
                       for policy in self.contract["policies"] for repeat in range(3)]
        self.report = dict(experiment="verification-quality-v1", mode="live", status="finished", repetitions=3,
            unexecuted=[], censored=[], active_case=None, contract=self.contract,
            contract_sha=runner.digest(self.contract), cases=[])
        for project, policy, repeat in self.roster:
            identity = f"{project}-{policy}-{repeat}"
            self.report["cases"].append(dict(run_id=identity, project_id=project, policy=policy,
                repetition=repeat, root=str(self.study / identity), accepted=False, status="max_steps_incomplete"))
        self.primary = next(row for row in self.report["cases"] if row["run_id"] == runner.RUN_ID)
        records, history = {}, []
        for index, (label, alias, round_, valid, backend) in enumerate((
            ("candidate_B_prior_valid", "candidate-B", 3, True, "B"),
            (runner.LABELS[0], "candidate-B", 4, False, "B"),
            (runner.LABELS[1], "candidate-C", 1, True, "C"),
        )):
            files = {**initial, "backend.py": backend, "policy.json": "new"}
            path = self.stage_root / f"source-{index}.git"
            tip, tree = str(index + 1) * 40, runner.digest(files)[:40]
            binding = dict(store_path=str(path), tip_sha=tip, files_sha256=runner.digest(files))
            export = path.with_suffix(".json")
            save(export, dict(files=files, binding=binding))
            self.git[(str(path), tip)] = dict(files=files, tree=tree)
            records[label] = dict(candidate=alias, builder_round=round_, source_valid=valid,
                git_store=str(path), git_tip_sha=tip, git_tree_sha=tree, export_path=str(export),
                export_sha256=runner.sha(export.read_bytes()), source_sha256=runner.digest(files),
                file_sha256={name: runner.sha(value.encode()) for name, value in files.items()})
            history.append(dict(kind="builder", candidate=alias, round=round_, source_valid=valid, binding=binding))
        selected_files = self.git[(records[runner.LABELS[1]]["git_store"], records[runner.LABELS[1]]["git_tip_sha"])]["files"]
        self.stage = dict(stage_index=2, completed=False, selected="candidate-C", files=selected_files,
            selected_binding=history[2]["binding"], trajectory=history,
            candidates={"candidate-B": dict(binding=history[1]["binding"], source_valid=False)})
        self.trajectory = dict(contract_sha=self.report["contract_sha"], files=selected_files,
                               stages=[dict(stage_index=0), dict(stage_index=1), self.stage])
        self.primary["files_sha256"] = runner.digest(selected_files)
        save(self.study / "fixtures.json", {"projects": self.projects})
        self.write_primary()
        marker = self.study / "immutable.txt"
        marker.write_bytes(b"Original primary evidence")
        self.finding = dict(purpose="verification-retained-source-eligibility-finding-v1",
            study_root=str(self.study), study_contract_sha256=self.report["contract_sha"],
            inputs=[dict(path=str(marker), sha256=runner.sha(marker.read_bytes()), snapshot_only_mutable=False),
                    dict(path=str(self.study / "results.json"), sha256="historical report snapshot", snapshot_only_mutable=True)],
            fixture_binding=dict(projects_sha256=runner.digest(self.projects),
                                 file_sha256=runner.sha((self.study / "fixtures.json").read_bytes())),
            sources=records, optional_postfreeze_diagnostic=dict(purpose=runner.PROTOCOL,
                source_labels=list(runner.LABELS), maximum_fresh_validations=2, maximum_per_source=1,
                source_sha256=[records[label]["source_sha256"] for label in runner.LABELS],
                limitations=["Supplemental observation; no counterfactual acceptance."],
                evaluation=dict(project_id="buildgraph", cumulative_stage_indices=[0, 1, 2],
                    case_kind="hidden_cases", case_count=36, suite_sha256=runner.digest(self.suite),
                    image=self.contract["image"], case_timeout_seconds=12, suite_timeout_seconds=300,
                    validator_source_sha256=self.contract["sources"]["blackbox_validator.py"])))
        self.write_finding()

    def write_primary(self):
        save(self.root / "trajectory.json", self.trajectory)
        self.primary["trajectory_sha256"] = runner.sha((self.root / "trajectory.json").read_bytes())
        save(self.stage_root / "result.json", self.stage)
        save(self.root / "result.json", self.primary)
        save(self.study / "results.json", self.report)
        save(self.study / "preregistered.json", dict(contract=self.contract, roster=self.roster))

    def write_finding(self):
        save(self.finding_path, self.finding)
        self.finding_sha = runner.sha(self.finding_path.read_bytes())

    def snapshot(self):
        return {str(path): path.read_bytes() for path in self.study.rglob("*") if path.is_file()}

    def store(self, path):
        parent = self

        class FakeStore:
            def read_files(self, tip):
                return deepcopy(parent.git[(str(path), tip)]["files"])

            def _git(self, command, revision):
                if command != "rev-parse" or not revision.endswith("^{tree}"):
                    raise AssertionError("Unexpected Git command")
                return parent.git[(str(path), revision.removesuffix("^{tree}"))]["tree"]

        return FakeStore()

    @contextmanager
    def patches(self):
        with ExitStack() as stack:
            stack.enter_context(patch.object(runner, "REPOSITORY", self.repository))
            stack.enter_context(patch.object(runner, "FINDING_SHA256", self.finding_sha))
            stack.enter_context(patch.object(runner.frozen, "CORE", ["blackbox_validator.py"]))
            stack.enter_context(patch.object(runner.frozen, "runtime", return_value=self.runtime))
            stack.enter_context(patch.object(runner, "GitStore", side_effect=self.store))
            yield

    def prepare(self):
        return runner.prepare(self.study, self.output, self.finding_path)

    def run(self):
        return runner.run(self.study, self.output, self.finding_path)


class VerificationEligibilityDiagnosticTests(unittest.TestCase):
    def test_entire_study_must_finish_before_git_docker_or_output(self):
        mutations = [dict(status="running"), dict(unexecuted=["later"]), dict(censored=["failed"]),
                     dict(active_case={"run_id": "active"}), dict(repetitions=True), dict(mode="rehearsal")]
        for mutation in mutations:
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                fixture = SyntheticStudy(directory)
                fixture.report.update(mutation)
                fixture.write_primary()
                with fixture.patches(), patch.object(runner, "GitStore") as git, patch.object(runner, "BlackboxValidator") as validator:
                    with self.assertRaises(ValueError):
                        fixture.run()
                    git.assert_not_called()
                    validator.assert_not_called()
                self.assertFalse(fixture.output.exists())

    def test_finding_bytes_are_pinned_before_git_docker_or_output(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = SyntheticStudy(directory)
            fixture.finding_path.write_bytes(fixture.finding_path.read_bytes() + b" ")
            with fixture.patches(), patch.object(runner, "GitStore") as git, patch.object(runner, "BlackboxValidator") as validator:
                with self.assertRaisesRegex(ValueError, "finding changed"):
                    fixture.run()
                git.assert_not_called()
                validator.assert_not_called()
            self.assertFalse(fixture.output.exists())

    def test_prepare_binds_failed_primary_same_source_demotion_and_exact_suite(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = SyntheticStudy(directory)
            original = fixture.snapshot()
            with fixture.patches(), patch.object(runner.BlackboxValidator, "evaluate") as execute:
                plan = fixture.prepare()
                execute.assert_not_called()
            self.assertEqual(plan["suite"], fixture.suite)
            self.assertEqual(len(plan["suite"]), 36)
            self.assertEqual([source["label"] for source in plan["sources"]], list(runner.LABELS))
            self.assertEqual([source["source_valid"] for source in plan["sources"]], [False, True])
            self.assertEqual(fixture.snapshot(), original)
            self.assertFalse(fixture.output.exists())

    def test_bound_suite_source_tree_and_immutable_hash_changes_are_rejected(self):
        for change in ("suite_hash", "case_count", "source_roster", "git_bytes", "git_tree", "immutable", "export"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as directory:
                fixture = SyntheticStudy(directory)
                planned = fixture.finding["optional_postfreeze_diagnostic"]
                source = fixture.finding["sources"][runner.LABELS[0]]
                if change == "suite_hash":
                    planned["evaluation"]["suite_sha256"] = "wrong"
                elif change == "case_count":
                    planned["evaluation"]["case_count"] = 48
                elif change == "source_roster":
                    planned["source_sha256"].reverse()
                elif change == "git_bytes":
                    fixture.git[(source["git_store"], source["git_tip_sha"])]["files"]["backend.py"] = "changed"
                elif change == "git_tree":
                    fixture.git[(source["git_store"], source["git_tip_sha"])]["tree"] = "wrong"
                elif change == "immutable":
                    (fixture.study / "immutable.txt").write_text("changed")
                else:
                    path = Path(source["export_path"])
                    path.write_bytes(path.read_bytes() + b" ")
                fixture.write_finding()
                with fixture.patches(), patch.object(runner.BlackboxValidator, "evaluate") as execute:
                    with self.assertRaises(ValueError):
                        fixture.run()
                    execute.assert_not_called()
                self.assertFalse(fixture.output.exists())

    def test_two_sources_execute_once_sequentially_with_same_suite_and_primary_unchanged(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = SyntheticStudy(directory)
            original = fixture.snapshot()
            calls = []

            def evaluate(files, cases):
                saved = runner.decode((fixture.output / "results.json").read_bytes())
                index = len(calls)
                self.assertEqual(saved["active_source"], runner.LABELS[index])
                self.assertEqual(len(saved["sources"]), index)
                if index:
                    self.assertTrue(saved["executions"][0]["verified"])
                self.assertEqual(cases, fixture.suite)
                calls.append(files["backend.py"])
                return receipt(files, cases, fixture.contract, failed=[cases[0]["id"]] if index else [])

            with fixture.patches(), patch.object(runner.BlackboxValidator, "preflight", return_value=(True, "offline mock")), \
                    patch.object(runner.BlackboxValidator, "evaluate", side_effect=evaluate) as execute:
                result = fixture.run()
            self.assertEqual(calls, ["B", "C"])
            self.assertEqual(execute.call_count, 2)
            self.assertEqual(result["status"], "finished")
            self.assertEqual(result["unexecuted"], [])
            self.assertIsNone(result["active_source"])
            self.assertEqual([row["hidden_passed"] for row in result["sources"]], [True, False])
            self.assertEqual(result["sources"][1]["hidden_failed_case_ids"], ["h0-0"])
            self.assertFalse(result["primary_scores_modified"])
            self.assertEqual(result["model_calls"], 0)
            self.assertEqual(result["promotions"], 0)
            for execution in result["executions"]:
                self.assertTrue(execution["verified"])
                self.assertEqual(execution["receipt_sha256"], runner.sha((fixture.output / execution["receipt"]).read_bytes()))
            self.assertEqual(fixture.snapshot(), original)
            with fixture.patches(), patch.object(runner.BlackboxValidator, "evaluate") as retry:
                with self.assertRaisesRegex(ValueError, "fresh output"):
                    fixture.run()
                retry.assert_not_called()

    def test_host_rechecks_receipt_truth_and_retains_invalid_observation_without_retry(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = SyntheticStudy(directory)
            original = fixture.snapshot()

            def forged(files, cases):
                value = receipt(files, cases, fixture.contract)
                value["outcomes"][0]["actual"] = True  # Expected 1; bool is not an equal JSON number.
                return value

            with fixture.patches(), patch.object(runner.BlackboxValidator, "preflight", return_value=(True, "offline mock")), \
                    patch.object(runner.BlackboxValidator, "evaluate", side_effect=forged) as execute:
                with self.assertRaisesRegex(ValueError, "correctness flag"):
                    fixture.run()
                self.assertEqual(execute.call_count, 1)
            result = runner.decode((fixture.output / "results.json").read_bytes())
            self.assertEqual(result["status"], "interrupted")
            self.assertEqual(result["active_source"], runner.LABELS[0])
            self.assertEqual(result["unexecuted"], [runner.LABELS[1]])
            self.assertEqual(result["sources"], [])
            self.assertFalse(result["executions"][0]["verified"])
            retained = fixture.output / result["executions"][0]["receipt"]
            self.assertEqual(result["executions"][0]["receipt_sha256"], runner.sha(retained.read_bytes()))
            self.assertIs(runner.decode(retained.read_bytes())["outcomes"][0]["actual"], True)
            self.assertEqual(fixture.snapshot(), original)

    def test_second_evaluation_exception_keeps_first_receipt_and_never_retries(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = SyntheticStudy(directory)
            original = fixture.snapshot()
            calls = []

            def evaluate(files, cases):
                calls.append(files["backend.py"])
                if len(calls) == 2:
                    raise RuntimeError("synthetic interrupted observation")
                return receipt(files, cases, fixture.contract)

            with fixture.patches(), patch.object(runner.BlackboxValidator, "preflight", return_value=(True, "offline mock")), \
                    patch.object(runner.BlackboxValidator, "evaluate", side_effect=evaluate):
                with self.assertRaisesRegex(RuntimeError, "synthetic interrupted"):
                    fixture.run()
            result = runner.decode((fixture.output / "results.json").read_bytes())
            self.assertEqual(calls, ["B", "C"])
            self.assertEqual(result["status"], "interrupted")
            self.assertEqual(result["active_source"], runner.LABELS[1])
            self.assertEqual(result["unexecuted"], [])
            self.assertEqual(len(result["sources"]), 1)
            self.assertEqual([item["verified"] for item in result["executions"]], [True, False])
            self.assertEqual(result["executions"][1]["raised"], "RuntimeError")
            for execution in result["executions"]:
                self.assertEqual(execution["receipt_sha256"], runner.sha((fixture.output / execution["receipt"]).read_bytes()))
            self.assertEqual(fixture.snapshot(), original)


if __name__ == "__main__":
    unittest.main()
