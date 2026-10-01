"""Offline boundary checks for the benchmark runner; no Docker/provider execution."""
from copy import deepcopy
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from gossip_harness import benchmark_experiment as runner
from gossip_harness.blackbox_validator import BlackboxValidator
from gossip_harness.ledger import Ledger
from gossip_harness.worker import WorkerFailure, WorkerResult


def fake_fixture(project_id="graph-patch"):
    stages = []
    for stage in range(2):
        case = dict(id=f"public-{stage}", input={"value": stage}, expected={"value": stage},
                    requirement=f"R{stage}")
        stages.append(dict(requirements=[f"R{stage}"], specification=f"Implement stage {stage}",
            visible_cases=[case], hidden_cases=[dict(case, id=f"private-{stage}",
                input={"value": stage + 10}, expected={"value": stage + 10})],
            known_files={"solution.py": f"def solve(payload):\n    return payload  # stage {stage}\n"}))
    project = dict(id=project_id, title="Offline fixture", allowed_paths=["solution.py"], stages=stages,
        initial_files={"solution.py": "def solve(payload):\n    return None\n", "fixed.txt": "Trusted adapter"})
    def validate(stage, payload):
        if type(payload) is not dict or set(payload) != {"value"} or type(payload["value"]) is not int:
            raise ValueError("Invalid input")
    return SimpleNamespace(PROJECT=project, reference=lambda stage, payload: deepcopy(payload),
        validate_input=validate, rehearsal_probe=lambda stage, index: {"value": 100 + stage * 10 + index},
        fault_bank=lambda stage: [], correct_controls=lambda stage: [])


def expected_contract():
    plan = runner.study_plan()
    return dict(protocol=runner.PROTOCOL, roster=runner.roster(), policies=deepcopy(plan["policies"]),
        limits={key: plan["controller"][key] for key in
            ("max_reviews", "max_repairs", "max_new_probes", "max_escalations", "stagnation_reviews")},
        image="unit-test-image", budget=deepcopy(plan["budget"]), extra_sources={},
        fault_banks_sha256=runner.digest({"offline": True}))


class FakeValidator:
    counter = 0

    def __init__(self, image, **limits):
        self.image = image

    def preflight(self):
        return True, "Offline fake validator"

    def evaluate(self, files, cases):
        type(self).counter += 1
        prepared, execution_cases, _ = BlackboxValidator._inputs(files, cases)
        return dict(status="passed", passed=True, cleanup_verified=True,
            source_sha256=runner.digest(prepared), suite_sha256=runner.digest(execution_cases),
            image_id=self.image, case_timeout_seconds=runner.CASE_TIMEOUT,
            timeout_seconds=runner.SUITE_TIMEOUT, container_name=f"offline-{self.counter}",
            outcomes=[dict(index=index, passed=True) for index in range(len(cases))])


class BenchmarkRunnerContractTests(unittest.TestCase):
    def test_roster_exact_balanced_and_anchors_have_one_repetition(self):
        rows = runner.roster()
        self.assertEqual(len(rows), 10)
        self.assertEqual(len({tuple(row) for row in rows}), 10)
        for project in runner.PROJECT_MODULES:
            self.assertEqual(sorted((policy, repetition) for item, policy, repetition in rows if item == project),
                [("independent-four", 0), ("independent-four", 1), ("sequential-four", 0),
                 ("sequential-four", 1), ("strong-anchor", 0)])
        self.assertNotEqual(rows[0][1], rows[1][1])

    def test_roster_rejects_missing_duplicate_unknown_and_boolean_repetition(self):
        plan = runner.study_plan()
        good = runner.roster()
        variants = [good[:-1], [good[0]] * 10,
                    [["unknown", *good[0][1:]], *good[1:]],
                    [[good[0][0], good[0][1], False], *good[1:]]]
        for rows in variants:
            with self.subTest(rows=rows), patch.object(runner, "study_plan", return_value={**plan, "roster": rows}):
                with self.assertRaises(ValueError):
                    runner.roster()

    def test_plan_rejects_unknown_formation_and_impossible_budget(self):
        plan = runner.study_plan()
        altered = deepcopy(plan)
        altered["policies"]["independent-four"]["formation"] = "sequential"
        bad_budget = deepcopy(plan)
        bad_budget["budget"]["incremental_cap_micro_usd"] = bad_budget["budget"]["shared_cumulative_cap_micro_usd"]
        for value in (altered, bad_budget):
            with self.subTest(value=value), patch.object(runner, "read", return_value=value):
                with self.assertRaises(ValueError):
                    runner.study_plan()

    def test_known_noop_only_applies_to_first_repair(self):
        fixture = fake_fixture()
        for phase, number in (("initial", 1), ("initial", 2), ("repair", 3)):
            result = runner.known_result(fixture.PROJECT, fixture, 0,
                dict(role="builder", phase=phase, round=number))
            self.assertEqual(result.usage_units, 0)
        with self.assertRaises(WorkerFailure) as caught:
            runner.known_result(fixture.PROJECT, fixture, 0, dict(role="builder", phase="repair", round=2))
        self.assertEqual(caught.exception.usage_units, 0)
        self.assertTrue(caught.exception.metadata["rehearsal_noop"])

    def test_scout_decoder_preserves_proposals_but_rejects_entire_malformed_batches(self):
        proposal = dict(input={"value": 1}, expected={"value": 1}, requirement="R0")
        result = WorkerResult({"probes.json": json.dumps({"probes": [proposal]})}, "", 0, {})
        values, receipt = runner.decode_scout(result)
        self.assertEqual(values, [proposal])
        self.assertEqual(receipt, dict(status="parsed", proposed_count=1))
        for raw in ('{"probes":[],"probes":[]}', '{"probes":[NaN]}', '{"probes":[]',
                    '{"extra":0,"probes":[]}', '{"probes":[1,2,3,4,5]}', '[]'):
            with self.subTest(raw=raw):
                values, receipt = runner.decode_scout(WorkerResult({"probes.json": raw}, "", 0, {}))
                self.assertEqual(values, [])
                self.assertEqual(receipt["status"], "rejected_batch")
        self.assertEqual(runner.decode_scout(None)[1]["status"], "rejected_batch")

    def test_scout_dispatch_requires_matching_durable_initial_pool(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture, invoke = fake_fixture(), Mock()
            pool = {"candidates": {"candidate-A": {"files": {"secret": "candidate-only"}}}}
            runner.save(root / "initial-pool.json", dict(pool={}, pool_sha256=runner.digest({})))
            with self.assertRaises(ValueError):
                runner.scout_probes(fixture.PROJECT, 0, fixture.PROJECT["initial_files"], invoke,
                    root, runner.Spans(root / "timings.json"), "test", pool)
            invoke.assert_not_called()

    def test_scouts_share_only_stage_start_public_context_and_keep_parse_accounting(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = fake_fixture()
            pool = {"candidates": {"candidate-A": {"files": {"secret": "candidate-only"}}}}
            runner.save(root / "initial-pool.json", dict(pool=pool, pool_sha256=runner.digest(pool)))
            inputs = []
            def invoke(**request):
                inputs.append(deepcopy(request["files"]))
                text = '{"probes":[]}' if request["metadata"]["scout_index"] == 0 else '{bad'
                return WorkerResult({"probes.json": text}, "", 0, {})
            proposals, evidence = runner.scout_probes(fixture.PROJECT, 0,
                fixture.PROJECT["initial_files"], invoke, root, runner.Spans(root / "timings.json"), "test", pool)
            self.assertEqual(len(proposals), 2)
            self.assertEqual(inputs[0], inputs[1])
            text = json.dumps(inputs[0])
            self.assertNotIn("candidate-only", text)
            self.assertNotIn("private-", text)
            self.assertNotIn("Implement stage 1", text)
            self.assertEqual(evidence["rejected_batches"], 1)
            self.assertEqual(runner.read(root / "scouts.json"), evidence)

    def test_missing_qualification_or_existing_output_fails_before_docker_or_credentials(self):
        with tempfile.TemporaryDirectory() as temporary:
            with patch.object(runner, "contract", return_value=expected_contract()), \
                    patch.object(runner, "BlackboxValidator") as validator, patch.object(runner, "credential") as key:
                with self.assertRaisesRegex(ValueError, "qualification"):
                    runner._run_owned(Path(temporary) / "new", live=True)
                with self.assertRaisesRegex(ValueError, "fresh"):
                    runner._run_owned(Path(temporary))
                validator.assert_not_called()
                key.assert_not_called()


class BenchmarkRunnerTrajectoryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temporary.name)
        cls.expected = expected_contract()
        cls.fixture = fake_fixture()
        cls.config = dict(root=str(cls.root / "run"), run_id="graph-patch-sequential-four-0",
            project_id="graph-patch", policy="sequential-four", repetition=0, live=False,
            image="unit-test-image", namespace="offline/case")
        Path(cls.config["root"]).mkdir()
        cls.ledger = Ledger(cls.root / "budget.sqlite", budget_units=0)
        cls.budget = runner.StudyBudget(cls.ledger, "offline", 0, cls.root / "budget.lock")
        cls.spans = runner.Spans(cls.root / "timings.json")
        with patch.object(runner, "fixture", return_value=cls.fixture), \
                patch.object(runner, "contract", return_value=cls.expected), \
                patch.object(runner, "BlackboxValidator", FakeValidator):
            cls.state = runner.trajectory(cls.config, cls.expected, cls.ledger, cls.budget, {}, cls.spans)
            cls.artifacts = runner.collect_stage_artifacts(Path(cls.config["root"]), cls.state)

    @classmethod
    def tearDownClass(cls):
        cls.temporary.cleanup()

    def test_real_controller_mock_executor_completes_both_stages_with_all_exports(self):
        self.assertEqual(self.state["terminal"], "visible_complete")
        self.assertEqual(len(self.artifacts), 2)
        for stage, result in enumerate(self.state["stages"]):
            self.assertEqual(result["metrics"]["initial_builder_calls"], 4)
            self.assertEqual(result["metrics"]["repairs"], 2)
            self.assertGreaterEqual(result["metrics"]["retained_eligibility_preserved"], 1)
            self.assertEqual(result["metrics"]["escalations"], 1)
            self.assertEqual(result["scouts"]["admitted"], 2)
            self.assertEqual(result["scouts"]["rejected"], 4)
            self.assertEqual(len(self.artifacts[stage]["exports"]), 6)
            self.assertTrue(all(call["usage_units"] == 0 and not call["physical_dispatch"] for call in result["invocations"]))
        self.assertEqual(runner.unsettled(self.ledger.path), dict(reservations=0, promotions=0))

    def test_initial_freeze_precedes_scouts_and_no_initial_context_contains_current_scout_probes(self):
        spans = runner.read(self.root / "timings.json")["spans"]
        for stage, result in enumerate(self.state["stages"]):
            stage_root = Path(self.config["root"]) / f"stage-{stage}"
            initial = runner.read(stage_root / "initial-pool.json")
            for call in result["invocations"]:
                span = spans[call["physical_span_id"]]
                if call["role"] == "scout":
                    self.assertGreaterEqual(span["started_monotonic_ns"], initial["frozen_monotonic_ns"])
                elif call["controller_metadata"].get("phase") == "initial":
                    self.assertLessEqual(span["finished_monotonic_ns"], initial["frozen_monotonic_ns"])
                    request = runner.read(stage_root / "requests" / f"{call['call_id']}.request.json")
                    context = json.loads(request["files"]["verification-context.json"])
                    self.assertFalse(any(case.get("origin", {}).get("stage_index") == stage
                        and case.get("origin", {}).get("source") == "scout" for case in context["verified_cases"]))

    def test_nonselected_export_tampering_is_rejected(self):
        item = self.artifacts[0]["exports"][1]
        path = Path(item["path"])
        old = path.read_bytes()
        try:
            value = json.loads(old)
            value["files"]["solution.py"] += "# altered\n"
            runner.save(path, value)
            with patch.object(runner, "fixture", return_value=self.fixture):
                with self.assertRaisesRegex(ValueError, "export bytes"):
                    runner.collect_stage_artifacts(Path(self.config["root"]), self.state)
        finally:
            path.write_bytes(old)

    def test_deleted_repair_export_cannot_silently_leave_pool(self):
        path = Path(self.config["root"]) / "stage-0" / "candidate-exports.json"
        old = path.read_bytes()
        try:
            value = json.loads(old)
            value["exports"].pop()
            runner.save(path, value)
            with patch.object(runner, "fixture", return_value=self.fixture):
                with self.assertRaisesRegex(ValueError, "Every initial and repaired"):
                    runner.collect_stage_artifacts(Path(self.config["root"]), self.state)
        finally:
            path.write_bytes(old)


class BenchmarkRunnerBarrierTests(unittest.TestCase):
    def _run(self, root, *, fail=False):
        expected = expected_contract()
        qualification = root / "qualification.json"
        runner.save(qualification, {"offline": True})
        calls, finalizations = [], []
        def trajectory(config, *_):
            calls.append(config["run_id"])
            if fail:
                raise RuntimeError("Unknown outcome")
            state = dict(terminal="visible_complete")
            runner.save(Path(config["root"]) / "trajectory.json", state)
            return state
        def freeze(output, _):
            self.assertEqual(len(calls), 10)
            runner.save(Path(output) / "frozen-trajectories.json", {"all": calls})
        def finalize(output, config, *_):
            self.assertEqual(len(calls), 10)
            self.assertTrue((Path(output) / "frozen-trajectories.json").exists())
            finalizations.append(config["run_id"])
            return dict(accepted=True)
        mocks = dict(freeze=Mock(side_effect=freeze), finalize=Mock(side_effect=finalize))
        with patch.object(runner, "contract", return_value=expected), \
                patch.object(runner, "CORE", ()), \
                patch.object(runner, "fixture", side_effect=fake_fixture), \
                patch.object(runner, "fault_banks", return_value={"offline": True}), \
                patch.object(runner, "trajectory", side_effect=trajectory), \
                patch.object(runner, "BlackboxValidator", FakeValidator), \
                patch.object(runner, "freeze_trajectories", mocks["freeze"]), \
                patch.object(runner, "finalize", mocks["finalize"]), \
                patch.object(runner, "validate_rehearsal"), \
                patch("analysis.qualify_benchmark.validate_qualification"):
            if fail:
                with self.assertRaisesRegex(RuntimeError, "Unknown outcome"):
                    runner._run_owned(root / "study", qualification=qualification)
            else:
                runner._run_owned(root / "study", qualification=qualification)
        return runner.read(root / "study" / "results.json"), calls, finalizations, mocks

    def test_every_trajectory_finishes_before_any_private_finalization(self):
        with tempfile.TemporaryDirectory() as temporary:
            report, calls, finals, mocks = self._run(Path(temporary))
        self.assertEqual(len(calls), 10)
        self.assertEqual(finals, calls)
        self.assertEqual(report["status"], "finished")
        mocks["freeze"].assert_called_once()

    def test_unknown_outcome_retains_censor_and_never_crosses_private_barrier(self):
        with tempfile.TemporaryDirectory() as temporary:
            report, calls, finals, mocks = self._run(Path(temporary), fail=True)
        self.assertEqual(len(calls), 1)
        self.assertEqual(finals, [])
        self.assertEqual(report["status"], "interrupted")
        self.assertEqual(len(report["censored"]), 1)
        self.assertEqual(len(report["unexecuted"]), 9)
        mocks["freeze"].assert_not_called()
        mocks["finalize"].assert_not_called()

    def test_unfrozen_or_duplicate_roster_is_rejected_before_evaluation(self):
        expected = expected_contract()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            runner.save(root / "frozen-trajectories.json", dict(protocol=runner.PROTOCOL,
                contract_sha=runner.digest(expected), trajectories=[], private_evaluation_started=False))
            with self.assertRaisesRegex(ValueError, "ten-trajectory"):
                runner.validate_freeze(root, expected, runner.sha_file(root / "frozen-trajectories.json"))


class BenchmarkRunnerProofTests(unittest.TestCase):
    def timing_proof(self, root):
        expected = expected_contract()
        runner.save(root / "frozen-trajectories.json", dict(frozen_monotonic_ns=100))
        freeze_sha = runner.sha_file(root / "frozen-trajectories.json")
        spans = []
        def span(kind, start, end, **values):
            value = dict(span_id=len(spans), kind=kind, clock_id="offline-clock", status="finished",
                started_monotonic_ns=start, finished_monotonic_ns=end, **values)
            spans.append(value)
            return value
        for project, policy, repetition in expected["roster"]:
            run_id = f"{project}-{policy}-{repetition}"
            span("model_trajectory", 0, 90, run_id=run_id)
            for kind in ("visible", "private"):
                path = root / run_id / f"final-{kind}-receipt.json"
                runner.save(path, dict(container_name=f"container-{len(spans)}"))
                span("docker_validation", 101, 110, run_id=run_id, purpose="final_" + kind,
                    receipt_path=str(path), receipt_sha256=runner.sha_file(path),
                    freeze_manifest_sha256=freeze_sha)
        runner.save(root / "timings.json", dict(clock_id="offline-clock", spans=spans))
        return expected, freeze_sha, spans

    def test_complete_fresh_final_timing_proof_is_accepted(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            expected, freeze_sha, _ = self.timing_proof(root)
            self.assertEqual(len(runner.validate_timing_evidence(root, expected, freeze_sha)), 30)

    def test_final_before_freeze_or_provider_after_freeze_is_rejected(self):
        for changed_index, key, value in ((1, "started_monotonic_ns", 99), (0, "finished_monotonic_ns", 101)):
            with self.subTest(key=key), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                expected, freeze_sha, spans = self.timing_proof(root)
                spans[changed_index][key] = value
                runner.save(root / "timings.json", dict(clock_id="offline-clock", spans=spans))
                with self.assertRaises(ValueError):
                    runner.validate_timing_evidence(root, expected, freeze_sha)

    def test_reused_container_and_changed_final_receipt_fail(self):
        for rebind in (False, True):
            with self.subTest(rebind=rebind), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                expected, freeze_sha, spans = self.timing_proof(root)
                second = Path(spans[2]["receipt_path"])
                second.write_bytes(Path(spans[1]["receipt_path"]).read_bytes())
                if rebind:
                    spans[2]["receipt_sha256"] = runner.sha_file(second)
                    runner.save(root / "timings.json", dict(clock_id="offline-clock", spans=spans))
                with self.assertRaises(ValueError):
                    runner.validate_timing_evidence(root, expected, freeze_sha)

    def test_missing_final_or_foreign_clock_is_not_full_success(self):
        for change in ("missing", "clock", "running", "freeze"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                expected, freeze_sha, spans = self.timing_proof(root)
                if change == "missing":
                    spans[-1]["purpose"] = "stage_evidence"
                elif change == "clock":
                    spans[-1]["clock_id"] = "other-clock"
                elif change == "running":
                    spans[-1]["status"] = "running"
                else:
                    spans[-1]["freeze_manifest_sha256"] = "0" * 64
                runner.save(root / "timings.json", dict(clock_id="offline-clock", spans=spans))
                with self.assertRaises(ValueError):
                    runner.validate_timing_evidence(root, expected, freeze_sha)

    def proof(self):
        expected = expected_contract()
        rows = []
        for project, policy, repetition in expected["roster"]:
            rows.append(dict(project_id=project, policy=policy, repetition=repetition, accepted=True,
                milestones_completed=2, physical_provider_calls=0, usage_micro_usd=0,
                metrics=dict(retained_eligibility_preserved=2, escalations=2, stagnation_events=2,
                    repairs=4, initial_builder_calls=2 * expected["policies"][policy]["initial_builders"]),
                scouts=dict(calls=4, admitted=4, rejected=8, rejected_batches=0),
                invocations=[dict(usage_units=0, physical_dispatch=False,
                    metadata=dict(api_calls=0, rehearsal_noop=True)) for _ in range(2)]))
        proof = dict(experiment=runner.PROTOCOL, mode="rehearsal", contract=deepcopy(expected),
            contract_sha=runner.digest(expected), status="finished", unexecuted=[], censored=[],
            active_case=None, incremental_micro_usd=0, cases=rows)
        return expected, proof

    def test_rehearsal_requires_exact_roster_and_all_controls(self):
        expected, proof = self.proof()
        runner.validate_rehearsal(proof, expected)
        for field, value in (("cases", proof["cases"][:-1]), ("cases", list(reversed(proof["cases"]))),
                             ("incremental_micro_usd", 1), ("status", "interrupted"),
                             ("censored", [expected["roster"][0]])):
            with self.subTest(field=field):
                changed = deepcopy(proof)
                changed[field] = value
                with self.assertRaises(ValueError):
                    runner.validate_rehearsal(changed, expected)

    def test_rehearsal_rejects_missing_initial_opportunity_or_scout_negative_controls(self):
        expected, proof = self.proof()
        for section, field, value in (("metrics", "initial_builder_calls", 7),
                ("metrics", "repairs", 3), ("metrics", "retained_eligibility_preserved", 1),
                ("scouts", "admitted", 3), ("scouts", "rejected", 7), ("scouts", "rejected_batches", 1)):
            with self.subTest(field=field):
                changed = deepcopy(proof)
                changed["cases"][0][section][field] = value
                with self.assertRaises(ValueError):
                    runner.validate_rehearsal(changed, expected)


class BenchmarkRunnerIntegrityTests(unittest.TestCase):
    def test_fast_final_gate_still_rejects_changed_accepted_ref(self):
        expected = expected_contract()
        files = {"solution.py": "def solve(payload): return payload\n"}
        binding = dict(store_path="/offline/store.git", tip_sha="1" * 40,
                       accepted_head="2" * 40, files_sha256=runner.digest(files))
        state = dict(contract_sha=runner.digest(expected), project_id="graph-patch", policy="strong-anchor",
            repetition=0, terminal="bounded_incomplete", stages=[dict(stage_index=0, completed=False, files=files)],
            files=files, final_binding=binding)
        store = Mock()
        store.head.return_value = binding["accepted_head"]
        store.read_files.side_effect = AssertionError("Fast gate must not reread frozen immutable objects")
        with patch.object(runner, "GitStore", return_value=store):
            self.assertEqual(runner.validate_trajectory(state, expected,
                ["graph-patch", "strong-anchor", 0], verify_git_sources=False), state)
            store.read_files.assert_not_called()
            store.head.return_value = "3" * 40
            with self.assertRaisesRegex(ValueError, "Git/source"):
                runner.validate_trajectory(state, expected,
                    ["graph-patch", "strong-anchor", 0], verify_git_sources=False)

    def artifact_fixture(self, root):
        fixture = fake_fixture()
        files = deepcopy(fixture.PROJECT["initial_files"])
        binding = dict(store_path=str(root / "store.git"), tip_sha="1" * 40, files_sha256=runner.digest(files))
        label = "stage-0-build-slot-0-1"
        stage_root = root / "stage-0"
        export_path = stage_root / f"{label}.json"
        runner.save(export_path, dict(label=label, binding=binding, files=files))
        pool = dict(project_id="graph-patch", stage_index=0, policy="strong-anchor", candidate_order=["candidate-A"],
            candidates={"candidate-A": dict(binding=binding, files=files)})
        initial = dict(protocol=runner.PROTOCOL, run_id="graph-patch-strong-anchor-0", stage_index=0,
                       pool=pool, pool_sha256=runner.digest(pool))
        scouts = dict(calls=2)
        result = dict(stage_index=0, initial_pool=pool, initial_pool_sha256=runner.digest(pool),
            initial_freeze=dict(freeze_id=str(stage_root / "initial-pool.json"), pool_sha256=runner.digest(pool)),
            metrics=dict(initial_builder_calls=1, builder_calls=1),
            trajectory=[dict(kind="builder", slot="slot-0", round=1, binding=binding, files_sha256=runner.digest(files))],
            candidates={"candidate-A": dict(binding=binding)}, selected_binding=binding, files=files,
            scouts=scouts, invocations=[])
        export = dict(label=label, path=str(export_path), sha256=runner.sha_file(export_path),
                      binding=binding, files_sha256=runner.digest(files))
        runner.save(stage_root / "candidate-exports.json", dict(protocol=runner.PROTOCOL,
            run_id="graph-patch-strong-anchor-0", project_id="graph-patch", stage_index=0, exports=[export]))
        runner.save(stage_root / "initial-pool.json", initial)
        runner.save(stage_root / "scouts.json", scouts)
        runner.save(stage_root / "result.json", result)
        return fixture, dict(run_id="graph-patch-strong-anchor-0", project_id="graph-patch",
                             policy="strong-anchor", stages=[result]), export_path

    def test_fast_artifact_gate_rehashes_every_source_without_repeating_git_reads(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture, state, path = self.artifact_fixture(root)
            with patch.object(runner, "fixture", return_value=fixture), patch.object(runner, "GitStore") as store:
                rows = runner.collect_stage_artifacts(root, state, verify_git_sources=False)
                self.assertEqual(len(rows[0]["exports"]), 1)
                store.assert_not_called()
                data = runner.read(path)
                data["files"]["solution.py"] += "# tampered\n"
                runner.save(path, data)
                with self.assertRaisesRegex(ValueError, "export bytes"):
                    runner.collect_stage_artifacts(root, state, verify_git_sources=False)

    def test_full_freeze_still_checks_actual_retained_git_source(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture, state, _ = self.artifact_fixture(root)
            store = Mock()
            store.read_files.return_value = {"solution.py": "wrong"}
            with patch.object(runner, "fixture", return_value=fixture), patch.object(runner, "GitStore", return_value=store):
                with self.assertRaisesRegex(ValueError, "Git source"):
                    runner.collect_stage_artifacts(root, state)
                store.read_files.assert_called_once_with("1" * 40)


class BenchmarkRunnerSerializationTests(unittest.TestCase):
    """Actual fixture JSON crosses the complete freeze/final gate with fake I/O.

    This builds synthetic terminal artifact envelopes directly: it does not run
    the controller, candidate code, Docker, a provider, or a real Git process.
    """

    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temporary.name)
        cls.expected = expected_contract()
        cls.expected["fault_banks_sha256"] = runner.digest(runner.fault_banks())
        cls.projects = {key: runner.fixture(key).PROJECT for key in runner.PROJECT_MODULES}
        cls.stores = {}
        cls.counter = 0
        cls.configs = []
        runner.save(cls.root / "fixtures.json", {"projects": list(cls.projects.values())})
        runner.save(cls.root / "fault-banks.json", runner.fault_banks())
        for project_id, policy, repetition in cls.expected["roster"]:
            project = cls.projects[project_id]
            run_id = f"{project_id}-{policy}-{repetition}"
            run_root = cls.root / run_id
            config = dict(root=str(run_root), run_id=run_id, project_id=project_id,
                          policy=policy, repetition=repetition, image="unit-test-image")
            cls.configs.append(config)
            cls.binding(run_root / "initial.git", project["initial_files"])
            stages = []
            for stage in range(2):
                stage_root = run_root / f"stage-{stage}"
                count = cls.expected["policies"][policy]["initial_builders"]
                files = {**project["initial_files"], **{name: project["stages"][stage]["known_files"][name]
                         for name in project["allowed_paths"]}}
                candidates, exports, builds = {}, [], []
                for slot in range(count):
                    alias = f"candidate-{slot}"
                    label = f"stage-{stage}-build-slot-{slot}-1"
                    binding = cls.binding(stage_root / f"{label}.git", files)
                    export_path = stage_root / f"{label}.json"
                    runner.save(export_path, dict(label=label, binding=binding, files=files))
                    exports.append(dict(label=label, path=str(export_path), sha256=runner.sha_file(export_path),
                                        files_sha256=runner.digest(files), binding=binding))
                    candidates[alias] = dict(files=files, binding=binding, source_valid=True, matrix={})
                    builds.append(dict(kind="builder", phase="initial", slot=f"slot-{slot}", round=1,
                                       binding=binding, files_sha256=runner.digest(files)))
                evidence = runner.cases_for(project, stage, "visible")
                pool = dict(project_id=project_id, stage_index=stage, policy=policy, candidates=candidates,
                            candidate_order=list(candidates), evidence_cases=evidence,
                            evidence_sha256=runner.digest(evidence))
                freeze_path = stage_root / "initial-pool.json"
                runner.save(freeze_path, dict(protocol=runner.PROTOCOL, run_id=run_id, stage_index=stage,
                                             pool=pool, pool_sha256=runner.digest(pool)))
                scouts = dict(calls=2, admitted=0, rejected=0, rejected_batches=0)
                selected = candidates["candidate-0"]["binding"]
                result = dict(stage_index=stage, completed=True, files=files, selected_binding=selected,
                    initial_pool=pool, initial_pool_sha256=runner.digest(pool),
                    initial_freeze=dict(freeze_id=str(freeze_path), pool_sha256=runner.digest(pool)),
                    candidates={alias: {key: value for key, value in item.items() if key != "files"}
                                for alias, item in candidates.items()},
                    metrics=dict(initial_builder_calls=count, builder_calls=count), trajectory=builds,
                    invocations=[], scouts=scouts, probe_pool=[], evidence_cases=evidence)
                runner.save(stage_root / "candidate-exports.json", dict(protocol=runner.PROTOCOL,
                    run_id=run_id, project_id=project_id, stage_index=stage, exports=exports))
                runner.save(stage_root / "scouts.json", scouts)
                runner.save(stage_root / "result.json", result)
                stages.append(result)
            state = dict(contract_sha=runner.digest(cls.expected), run_id=run_id, project_id=project_id,
                policy=policy, repetition=repetition, terminal="visible_complete", stages=stages,
                files=stages[-1]["files"], final_binding={**selected, "accepted_head": selected["tip_sha"]})
            runner.save(run_root / "trajectory.json", state)
        with patch.object(runner, "GitStore", side_effect=cls.controlled_store):
            cls.freeze = runner.freeze_trajectories(cls.root, cls.expected)
        cls.freeze_sha = runner.sha_file(cls.root / "frozen-trajectories.json")

    @classmethod
    def tearDownClass(cls):
        cls.temporary.cleanup()

    @classmethod
    def binding(cls, path, files):
        cls.counter += 1
        sha = f"{cls.counter:040x}"
        cls.stores[str(path)] = dict(head=sha, files=deepcopy(files))
        return dict(store_path=str(path), tip_sha=sha, files_sha256=runner.digest(files))

    @classmethod
    def controlled_store(cls, path):
        record = cls.stores[str(path)]
        def read_files(sha=None):
            if sha is not None and sha != record["head"]:
                raise ValueError("Controlled Git source identity changed")
            return deepcopy(record["files"])
        return SimpleNamespace(head=lambda: record["head"], read_files=read_files)

    def test_actual_tuple_fixtures_cross_complete_freeze_and_both_project_final_gates(self):
        self.assertTrue(all(isinstance(project["allowed_paths"], tuple)
                            and isinstance(project["stages"], tuple) for project in self.projects.values()))
        self.assertNotEqual(runner.read(self.root / "fixtures.json"), {"projects": list(self.projects.values())})
        self.assertEqual(len(self.freeze["trajectories"]), 10)
        self.assertEqual(sum(len(row["stage_artifacts"]) for row in self.freeze["trajectories"]), 20)
        spans = runner.Spans(self.root / "fake-final-timings.json")
        promotions = []
        def promote(base, binding, destination, files, allowed):
            self.assertEqual(files, self.stores[binding["store_path"]]["files"])
            promotions.append(str(destination))
            return SimpleNamespace(head=lambda: binding["tip_sha"])
        with patch.object(runner, "GitStore", side_effect=self.controlled_store), \
                patch.object(runner, "BlackboxValidator", FakeValidator), \
                patch.object(runner, "local_promote", side_effect=promote):
            states = runner.validate_freeze(self.root, self.expected, self.freeze_sha)
            self.assertEqual(len(states), 10)
            for project_id in self.projects:
                config = next(row for row in self.configs if row["project_id"] == project_id)
                result = runner.finalize(self.root, config, self.expected, self.freeze_sha, spans)
                self.assertTrue(result["accepted"])
                self.assertEqual(result["milestones_completed"], 2)
                self.assertTrue(all(result["requirement_coverage"].values()))
        self.assertEqual(len(promotions), 2)
        validations = runner.read(self.root / "fake-final-timings.json")["spans"]
        self.assertEqual([row["purpose"] for row in validations],
                         ["final_visible", "final_private", "final_visible", "final_private"])

    def test_real_fixture_content_change_is_rejected_before_git_or_candidate_work(self):
        path = self.root / "fixtures.json"
        original = path.read_bytes()
        try:
            changed = json.loads(original)
            changed["projects"][0]["title"] += " changed"
            runner.save(path, changed)
            with patch.object(runner, "GitStore") as git, patch.object(runner, "BlackboxValidator") as validator:
                with self.assertRaisesRegex(ValueError, "Preregistered fixtures"):
                    runner.freeze_trajectories(self.root, self.expected)
                git.assert_not_called()
                validator.assert_not_called()
        finally:
            path.write_bytes(original)

    def test_semantically_identical_reformatted_snapshot_still_breaks_frozen_raw_binding(self):
        path = self.root / "fixtures.json"
        original = path.read_bytes()
        try:
            path.write_bytes(original + b"\n")
            self.assertEqual(runner.digest(runner.read(path)), runner.digest({"projects": list(self.projects.values())}))
            with patch.object(runner, "GitStore") as git, patch.object(runner, "BlackboxValidator") as validator:
                with self.assertRaisesRegex(ValueError, "snapshot changed"):
                    runner.validate_freeze(self.root, self.expected, self.freeze_sha, verify_git_sources=False)
                git.assert_not_called()
                validator.assert_not_called()
        finally:
            path.write_bytes(original)
