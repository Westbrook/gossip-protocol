"""Frozen-pool bindings and analysis, using fake Git reads and validators only."""
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


PATH = Path(__file__).resolve().parents[1] / "analysis" / "run_verification_pool_diagnostic.py"
SPEC = importlib.util.spec_from_file_location("verification_pool_diagnostic", PATH)
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


def receipt(files, cases, contract, failures=()):
    rows = [dict(index=index, id=case["id"], requirement=case["requirement"],
        actual=None if case["id"] in failures else case["expected"],
        passed=case["id"] not in failures,
        status="wrong_answer" if case["id"] in failures else "passed") for index, case in enumerate(cases)]
    okay = all(row["passed"] for row in rows)
    return dict(schema_version=1, protocol="gossip-blackbox-v1", passed=okay,
        status="passed" if okay else "failed", cleanup_verified=True,
        source_sha256=runner.digest(files), suite_sha256=runner.digest(runner.execution_cases(cases)),
        image_id=contract["image"], adapter_sha256=runner.ADAPTER_SHA256,
        case_timeout_seconds=contract["case_timeout_seconds"], timeout_seconds=contract["suite_timeout_seconds"],
        case_count=len(cases), exit_code=0, timed_out=False, output_truncated=False,
        input_delivery_failed=False, outcomes=rows)


class FrozenStudy:
    """One reached final pool plus five explicit early stops; no executable code."""
    def __init__(self, directory):
        self.study = Path(directory).resolve() / "primary"
        self.output = Path(directory).resolve() / "diagnostic"
        self.study.mkdir()
        self.git = {}
        self.projects = []
        for project_id in ("buildgraph", "calendar"):
            initial = {"solution.py": "raise AssertionError('Never execute source on host')", "backend.py": "initial", "policy.json": "old"}
            stages = [dict(requirements=[f"R{i}"], trusted_updates={"policy.json": "new"} if i == 3 else {},
                hidden_cases=[dict(id=f"h{i}", input={"hidden": i}, expected=1, requirement=f"R{i}")],
                visible_cases=[dict(id=f"v{i}", input={"visible": i}, expected=1, requirement=f"R{i}")]) for i in range(4)]
            self.projects.append(dict(id=project_id, initial_files=initial, allowed_paths=["backend.py"], stages=stages))
        sources = {}
        for name in runner.CORE:
            raw = (runner.REPOSITORY / "gossip_harness" / name).read_bytes()
            destination = self.study / "source-snapshot" / "gossip_harness" / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(raw)
            sources[name] = runner.sha(raw)
        (self.study / "study-plan.json").write_text("{}")
        self.contract = dict(protocol="verification-quality-v1", policies=list(runner.POLICIES), milestones=4,
            project_ids=["buildgraph", "calendar"], initial_candidates={"portfolio-reviewed": 4},
            image="sha256:" + "a" * 64, case_timeout_seconds=12, suite_timeout_seconds=300,
            sources=sources, plan_sha256=runner.sha(b"{}"), fixture_sha256=runner.digest(self.projects),
            controller_runtime=runner.runtime())
        roster = [(project, policy, repetition) for project in self.contract["project_ids"]
                  for policy in runner.POLICIES for repetition in range(3)]
        self.report = dict(experiment="verification-quality-v1", mode="live", status="finished", repetitions=3,
            unexecuted=[], censored=[], active_case=None, contract=self.contract,
            contract_sha=runner.digest(self.contract), cases=[])
        self.roster = roster
        self.root = self.study / "buildgraph-portfolio-reviewed-0"
        self.stage = None
        for project_id, policy, repetition in roster:
            identity = f"{project_id}-{policy}-{repetition}"
            row = dict(run_id=identity, project_id=project_id, policy=policy, repetition=repetition,
                       root=str(self.study / identity), accepted=False, status="max_steps_incomplete")
            if policy == "portfolio-reviewed":
                state = dict(contract_sha=self.report["contract_sha"], stages=[{}], files={})
                if project_id == "buildgraph" and repetition == 0:
                    state, details = self.final_pool()
                    row.update(details)
                raw = json.dumps(state).encode()
                (self.study / identity).mkdir(exist_ok=True)
                (self.study / identity / "trajectory.json").write_bytes(raw)
                row["trajectory_sha256"] = runner.sha(raw)
                save(self.study / identity / "result.json", row)
            self.report["cases"].append(row)
        save(self.study / "fixtures.json", {"projects": self.projects})
        save(self.study / "preregistered.json", dict(contract=self.contract, roster=roster))
        self.write_report()

    def final_pool(self):
        project = self.projects[0]
        evidence = [case for stage in project["stages"] for case in stage["visible_cases"]]
        hidden = [case for stage in project["stages"] for case in stage["hidden_cases"]]
        candidates, history, all_matrix = {}, [], {}
        for alias, content in (("candidate-A", "selected"), ("candidate-B", "repaired"),
                               ("candidate-C", "repaired"), ("candidate-D", "visibly-bad")):
            files = {**project["initial_files"], "backend.py": content, "policy.json": "new"}
            path = self.root / "stage-3" / (alias + ".git")
            path.mkdir(parents=True)
            binding = dict(store_path=str(path), tip_sha=runner.digest(files)[:40], files_sha256=runner.digest(files))
            self.git[(str(path), binding["tip_sha"])] = files
            save(path.with_suffix(".json"), dict(binding=binding, files=files))
            validation = receipt(files, evidence, self.contract, failures=["v0"] if alias == "candidate-D" else [])
            matrix = {case["id"]: {key: outcome[key] for key in ("passed", "status", "actual")}
                      for case, outcome in zip(evidence, validation["outcomes"])}
            candidates[alias] = dict(binding=binding, source_valid=True, reviewer_remaining=[], matrix=matrix,
                validation_receipts=[dict(case_ids=[case["id"] for case in evidence], receipt=validation)])
            history.append(dict(kind="builder", candidate=alias, binding={"earlier": "initial-version"},
                                files_sha256="not-final", source_valid=True))
            history.append(dict(kind="builder", candidate=alias, binding=binding,
                                files_sha256=runner.digest(files), source_valid=True))
            all_matrix[alias] = matrix
        selected = self.git[(candidates["candidate-A"]["binding"]["store_path"], candidates["candidate-A"]["binding"]["tip_sha"])]
        final_hidden = receipt(selected, hidden, self.contract, failures=["h0"])
        final_visible = receipt(selected, evidence, self.contract)
        self.stage = dict(stage_index=3, completed=True, files=selected, selected="candidate-A",
            selected_binding=candidates["candidate-A"]["binding"], evidence_cases=evidence,
            evidence_sha256=runner.digest(evidence), probe_pool=[], candidates=candidates,
            candidate_order=list(candidates), matrix=all_matrix, trajectory=history)
        save(self.root / "stage-3" / "result.json", self.stage)
        save(self.root / "final-private-receipt.json", final_hidden)
        save(self.root / "final-visible-receipt.json", final_visible)
        state = dict(contract_sha=self.report["contract_sha"],
                     stages=[dict(stage_index=index, completed=True) for index in range(3)] + [self.stage], files=selected)
        return state, dict(status="final_quality_failed", milestones_completed=4, files_sha256=runner.digest(selected),
                           final_hidden=final_hidden, final_visible=final_visible)

    def write_report(self):
        save(self.study / "results.json", self.report)

    def update_stage(self):
        row = next(row for row in self.report["cases"] if row["root"] == str(self.root))
        state = dict(contract_sha=self.report["contract_sha"],
                     stages=[dict(stage_index=index, completed=True) for index in range(3)] + [self.stage], files=self.stage["files"])
        raw = json.dumps(state).encode()
        (self.root / "trajectory.json").write_bytes(raw)
        row["trajectory_sha256"] = runner.sha(raw)
        save(self.root / "stage-3" / "result.json", self.stage)
        save(self.root / "result.json", row)
        self.write_report()

    def store(self, path):
        parent = self

        class ReadOnlyStore:
            def read_files(self, tip):
                return deepcopy(parent.git[(str(path), tip)])

        return ReadOnlyStore()


class VerificationPoolDiagnosticTests(unittest.TestCase):
    def test_incomplete_global_study_stops_before_output_or_docker(self):
        with tempfile.TemporaryDirectory() as directory:
            study = FrozenStudy(directory)
            for mutation in (dict(status="running"), dict(censored=[{"run_id": "unfinished"}]),
                             dict(active_case={"run_id": "running"}), dict(repetitions=True)):
                report = deepcopy(study.report)
                report.update(mutation)
                save(study.study / "results.json", report)
                with patch.object(runner, "BlackboxValidator") as validator:
                    with self.assertRaises(ValueError):
                        runner.run(study.study, study.output)
                    validator.assert_not_called()
                self.assertFalse(study.output.exists())

    def test_prepare_binds_final_repaired_git_versions_and_explicit_early_stops(self):
        with tempfile.TemporaryDirectory() as directory:
            study = FrozenStudy(directory)
            with patch.object(runner, "GitStore", side_effect=study.store):
                plan = runner.prepare(study.study, study.output)
            self.assertEqual(plan["planned_fresh_validations"], 2)
            self.assertEqual(len(plan["skipped"]), 5)
            self.assertEqual(plan["pools"][0]["candidates"][1]["files"]["backend.py"], "repaired")
            study.stage["trajectory"][-5]["files_sha256"] = "changed"
            study.update_stage()
            with patch.object(runner, "GitStore", side_effect=study.store):
                with self.assertRaisesRegex(ValueError, "final retained version"):
                    runner.prepare(study.study, study.output)

    def test_receipt_guards_reject_wrong_suite_adapter_labels_actual_and_cleanup(self):
        contract = dict(image="sha256:" + "a" * 64, case_timeout_seconds=12, suite_timeout_seconds=300)
        files = {"solution.py": "not executable"}
        cases = [dict(id="one", input=1, expected=1, requirement="R0")]
        good = receipt(files, cases, contract)
        runner.receipt_rows(good, files, cases, contract)
        for mutation in (dict(source_sha256="wrong"), dict(suite_sha256="wrong"), dict(adapter_sha256="wrong"),
                         dict(cleanup_verified=False), dict(protocol="wrong"), dict(exit_code=False), dict(case_count=True),
                         dict(outcomes=[{**good["outcomes"][0], "id": "wrong"}]),
                         dict(outcomes=[{**good["outcomes"][0], "actual": True}])):
            with self.subTest(mutation=mutation):
                with self.assertRaises(ValueError):
                    runner.receipt_rows({**good, **mutation}, files, cases, contract)

    def test_selected_primary_and_identical_source_reuse_are_explicit(self):
        with tempfile.TemporaryDirectory() as directory:
            study = FrozenStudy(directory)
            calls = []

            class FakeValidator:
                def __init__(self, *args, **kwargs):
                    pass

                def preflight(self):
                    return True, "offline fake"

                def evaluate(self, files, cases):
                    calls.append(files["backend.py"])
                    return receipt(files, cases, study.contract)

            with patch.object(runner, "GitStore", side_effect=study.store), patch.object(runner, "BlackboxValidator", FakeValidator):
                result = runner.run(study.study, study.output)
            self.assertEqual(calls, ["repaired", "visibly-bad"])
            self.assertEqual(result["fresh_validation_count"], 2)
            pool = result["pools"][0]
            rows = {row["alias"]: row for row in pool["candidates"]}
            self.assertEqual(rows["candidate-A"]["measurement_origin"], "reused_selected_primary")
            self.assertEqual(rows["candidate-B"]["measurement_origin"], "fresh_diagnostic")
            self.assertEqual(rows["candidate-C"]["measurement_origin"], "reused_identical_diagnostic_source")
            self.assertEqual(rows["candidate-B"]["original_receipt"], rows["candidate-C"]["original_receipt"])
            self.assertFalse(rows["candidate-D"]["visible_eligible"])
            self.assertTrue(pool["selector_miss"])
            self.assertEqual(pool["best_single_coverage_gain"], 1)
            self.assertFalse(result["primary_scores_modified"])

    def test_hiddenpassing_visible_ineligible_source_is_not_a_selector_miss(self):
        contract = dict(image="sha256:" + "a" * 64, case_timeout_seconds=12, suite_timeout_seconds=300)
        cases = [dict(id="h0", input={}, expected=1, requirement="R0")]
        pool = dict(run_id="test", project_id="buildgraph", repetition=0, selected="A", hidden_cases=cases,
                    primary_accepted=False, primary_status="failed", stage_completed=True)
        rows = [dict(alias=alias, source_sha256=alias, visible_eligible=alias == "A", accept_gate_eligible=alias == "A",
                **runner.score(receipt({"solution.py": alias}, cases, contract, failures=["h0"] if alias == "A" else []), cases))
                for alias in ("A", "B")]
        result = runner.summarize_pool(pool, rows)
        self.assertTrue(result["pool_any_hidden_passed"])
        self.assertFalse(result["visible_selector_opportunity"])
        self.assertFalse(result["selector_miss"])
        rows[1].update(visible_eligible=True, accept_gate_eligible=False)
        result = runner.summarize_pool(pool, rows)
        self.assertTrue(result["visible_selector_opportunity"])
        self.assertFalse(result["selector_miss"])

    def test_bad_fresh_receipt_is_saved_before_rejection_without_retry(self):
        with tempfile.TemporaryDirectory() as directory:
            study = FrozenStudy(directory)
            calls = []

            class FakeValidator:
                def __init__(self, *args, **kwargs):
                    pass

                def preflight(self):
                    return True, "offline fake"

                def evaluate(self, files, cases):
                    calls.append(files)
                    return {**receipt(files, cases, study.contract), "cleanup_verified": False}

            with patch.object(runner, "GitStore", side_effect=study.store), patch.object(runner, "BlackboxValidator", FakeValidator):
                with self.assertRaisesRegex(ValueError, "infrastructure failure"):
                    runner.run(study.study, study.output)
            self.assertEqual(len(calls), 1)
            retained = list((study.output / "receipts").glob("*.json"))
            self.assertEqual(len(retained), 1)
            self.assertIs(json.loads(retained[0].read_text())["cleanup_verified"], False)
            progress = json.loads((study.output / "results.json").read_text())
            self.assertEqual(progress["status"], "interrupted")
            self.assertFalse(progress["executions"][0]["verified"])

    def test_same_source_pool_reuses_primary_without_constructing_validator(self):
        with tempfile.TemporaryDirectory() as directory:
            study = FrozenStudy(directory)
            with patch.object(runner, "GitStore", side_effect=study.store):
                plan = runner.prepare(study.study, study.output)
            selected = deepcopy(plan["pools"][0]["candidates"][0])
            plan["pools"][0]["candidates"] = [{**deepcopy(selected), "alias": f"candidate-{letter}"} for letter in "ABCD"]
            plan["planned_fresh_validations"] = 0
            with patch.object(runner, "prepare", return_value=plan), patch.object(runner, "BlackboxValidator") as validator:
                result = runner.run(study.study, study.output)
            validator.assert_not_called()
            self.assertEqual(result["fresh_validation_count"], 0)
            self.assertTrue(all(row["measurement_origin"] == "reused_selected_primary" for row in result["pools"][0]["candidates"]))

    def test_existing_or_nested_output_is_rejected_and_inputs_remain_bound(self):
        with tempfile.TemporaryDirectory() as directory:
            study = FrozenStudy(directory)
            for output in (study.study, study.study / "nested", Path(directory)):
                with self.assertRaises(ValueError):
                    runner.validate_output(study.study, output)
            dangling = Path(directory) / "output-link"
            dangling.symlink_to(Path(directory) / "not-created")
            with self.assertRaisesRegex(ValueError, "fresh output"):
                runner.run(study.study, dangling)
            self.assertFalse((Path(directory) / "not-created").exists())
            with patch.object(runner, "GitStore", side_effect=study.store):
                plan = runner.prepare(study.study, study.output)
            runner.unchanged(plan["inputs"])
            (study.study / "results.json").write_text("{}")
            with self.assertRaisesRegex(ValueError, "Frozen input changed"):
                runner.unchanged(plan["inputs"])


if __name__ == "__main__":
    unittest.main()
