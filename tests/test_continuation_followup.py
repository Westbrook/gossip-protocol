"""Zero-provider, zero-Docker tests for immutable paired setup foundations."""
from copy import deepcopy
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
from typing import Any
import unittest
from unittest.mock import patch

from gossip_harness import continuation_followup as runner
from gossip_harness.blackbox_validator import BlackboxValidator
from gossip_harness.continuation_controller import run_continuation_stage
from gossip_harness.gitstore import GitStore
from gossip_harness.ledger import Ledger
from gossip_harness.worker import WorkerResult
from analysis import audit_continuation_followup as audit


def fake_fixture():
    stages = []
    for stage in range(2):
        case = dict(id=f"public-{stage}", input={"value": stage}, expected={"value": stage},
                    requirement=f"R{stage}")
        stages.append(dict(requirements=[f"R{stage}"], specification=f"Implement stage {stage}",
            visible_cases=[case], hidden_cases=[dict(case, id=f"private-{stage}")],
            known_files={"solution.py": f"def solve(payload):\n    return payload  # golden stage {stage}\n"}))
    project = dict(id="warehouse", title="Offline fixture", allowed_paths=["solution.py"], stages=stages,
        initial_files={"solution.py": "def solve(payload):\n    return None\n", "fixed.txt": "Trusted adapter"})
    def validate(stage, payload):
        if type(payload) is not dict or set(payload) != {"value"} or type(payload["value"]) is not int:
            raise ValueError("Invalid input")
    return SimpleNamespace(PROJECT=project, reference=lambda stage, payload: deepcopy(payload),
        validate_input=validate, rehearsal_probe=lambda stage, index: {"value": 100 + stage * 10 + index})


class FakeValidator:
    counter = 0

    def __init__(self, image, **limits):
        self.image = image

    def evaluate(self, files, cases):
        type(self).counter += 1
        prepared, execution_cases, _ = BlackboxValidator._inputs(files, cases)
        return dict(status="passed", passed=True, cleanup_verified=True,
            source_sha256=runner.digest(prepared), suite_sha256=runner.digest(execution_cases),
            image_id=self.image, case_timeout_seconds=runner.CASE_TIMEOUT,
            timeout_seconds=runner.SUITE_TIMEOUT, container_name=f"unit-test-{self.counter}",
            outcomes=[dict(index=index, passed=True) for index in range(len(cases))])


class FollowupPlanTests(unittest.TestCase):
    def test_exact_paired_roster_and_setup_order(self):
        plan = runner.study_plan()
        self.assertEqual(len(plan["roster"]), 12)
        for project, repetition in plan["block_order"]:
            self.assertEqual({policy for name, policy, rep in plan["roster"]
                              if name == project and rep == repetition}, set(runner.POLICIES))

    def test_rejects_missing_duplicate_and_boolean_roster(self):
        plan = runner.study_plan()
        variants = [plan["roster"][:-1], [plan["roster"][0]] * 12,
                    [["warehouse", "current-independent", False], *plan["roster"][1:]]]
        for roster in variants:
            with self.subTest(roster=roster), self.assertRaises(ValueError):
                runner.validate_plan({**plan, "roster": roster})

    def test_rejects_unmatched_limits_and_formation(self):
        for field, value in (("max_reviews", 5), ("max_repairs", True), ("output_tokens", 8192)):
            plan = runner.study_plan()
            plan["controller"][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                runner.validate_plan(plan)
        plan = runner.study_plan()
        plan["policies"]["current-independent"]["formation"] = "sequential"
        with self.assertRaises(ValueError):
            runner.validate_plan(plan)

    def test_rejects_changed_pairing_and_partial_setup_order(self):
        plan = runner.study_plan()
        for key, value in (("pairing", {"stage0": "none", "later": "arm-specific"}),
                           ("block_order", plan["block_order"][:-1]),
                           ("generation_order", ["independent"])):
            with self.subTest(key=key), self.assertRaises(ValueError):
                runner.validate_plan({**plan, key: value})
        plan["improved_controller_rules"]["uncertainty_focus_retries"] = True
        with self.assertRaises(ValueError):
            runner.validate_plan(plan)

    def test_remaining_budget_is_hard_bound_and_boolean_money_invalid(self):
        for cap in (True, 0, 14_456_375):
            plan = runner.study_plan()
            plan["budget"]["incremental_cap_micro_usd"] = cap
            with self.subTest(cap=cap), self.assertRaises(ValueError):
                runner.validate_plan(plan)

    def test_call_count_separates_shared_cash_from_attributed_opportunities(self):
        envelope = runner.call_envelope()
        self.assertEqual(envelope["maximum_physical_calls"], 256)
        self.assertEqual(sum(envelope["maximum_cost_allocation_by_model"].values()), 256)
        self.assertEqual(envelope["maximum_calls_by_model"]["cheap"], 160)
        self.assertEqual(envelope["maximum_calls_by_model"]["strong"], 120)
        self.assertEqual(envelope["shared_stage0"]["physical_builders"], 32)
        self.assertEqual(envelope["shared_stage0"]["attributed_initial_builder_opportunities"], 48)
        self.assertEqual(envelope["shared_stage0"]["physical_scouts"], 8)
        self.assertEqual(envelope["shared_stage0"]["attributed_scout_opportunities"], 24)
        self.assertEqual(envelope["conservative_full_cohort_micro_usd"], 711_497_856)

    def test_foundation_cannot_be_promoted_to_live_by_editing_readiness(self):
        plan = runner.study_plan()
        plan["readiness"] = {"live_authorized": True}
        result = runner.live_readiness(plan)
        self.assertFalse(result["ready"])
        self.assertFalse(result["paid_calls_enabled"])
        self.assertGreater(result["conservative_full_cohort_micro_usd"], result["authorized_headroom_micro_usd"])

    def test_physical_setup_requires_qualification_before_docker_or_output(self):
        with tempfile.TemporaryDirectory() as temporary, patch.object(runner, "BlackboxValidator") as validator:
            output = Path(temporary) / "never-created"
            with self.assertRaisesRegex(ValueError, "qualification"):
                runner.prepare_offline_setup(output, "warehouse", 0, qualification=None)
            validator.assert_not_called()
            self.assertFalse(output.exists())


class FollowupSharedSetupTests(unittest.TestCase):
    temporary: tempfile.TemporaryDirectory[str]
    root: Path
    fixture: SimpleNamespace
    expected: dict[str, Any]
    contract_sha: str
    ledger: Ledger
    budget: runner.StudyBudget
    spans: runner.Spans
    manifest: dict[str, Any]
    path: Path
    sha: str

    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temporary.name).resolve()
        cls.fixture = fake_fixture()
        cls.expected = dict(protocol=runner.PROTOCOL, image="unit-test-image", sources={}, extra_sources={},
            plan_sha256=runner.sha_file(runner.PLAN_PATH),
            models={role: {"reservation_units": 0} for role in ("cheap", "strong")})
        cls.contract_sha = runner.digest(cls.expected)
        cls.ledger = Ledger(cls.root / "ledger.sqlite", budget_units=0)
        cls.budget = runner.StudyBudget(cls.ledger, "offline-fixture", 0, cls.root / "budget.lock")
        cls.spans = runner.Spans(cls.root / "live-timings.json")
        with patch.object(runner, "fixture", return_value=cls.fixture), \
                patch.object(runner.OpenAIWorker, "run", side_effect=AssertionError("Provider call forbidden")):
            cls.manifest = runner.prepare_shared_block(cls.root / "block", "warehouse", 0,
                contract_sha256=cls.contract_sha, image="unit-test-image", ledger=cls.ledger,
                budget=cls.budget, spans=cls.spans, expected_contract=cls.expected,
                validator_factory=FakeValidator)
        cls.path = cls.root / "block" / "shared-setup.json"
        cls.sha = runner.sha_file(cls.path)

    @classmethod
    def tearDownClass(cls):
        cls.temporary.cleanup()

    def _import(self, policy="current-independent", **overrides):
        args = dict(project_id="warehouse", repetition=0,
            initial_files=self.fixture.PROJECT["initial_files"], contract_sha256=self.contract_sha,
            expected_manifest_sha256=self.sha)
        args.update(overrides)
        return runner.import_shared_initial(self.path, policy, **args)

    def test_actual_git_sources_and_ten_scripted_calls_retained(self):
        sessions = [runner.read(self.root / "block" / name / "session.json")
                    for name in ("independent", "sequential", "scouts")]
        self.assertEqual([len(row["invocations"]) for row in sessions], [4, 4, 2])
        self.assertEqual([len(row["validations"]) for row in sessions], [4, 4, 0])
        self.assertEqual(self.manifest["validator_origin"], "unit_test_control")
        self.assertTrue(all(call["physical_dispatch"] is False and call["usage_units"] == 0
                            and call["journal_replay"] is False for row in sessions for call in row["invocations"]))
        for row in sessions[:2]:
            for entry in row["exports"]:
                saved = runner.read(entry["path"])
                self.assertEqual(GitStore(saved["binding"]["store_path"]).read_files(saved["binding"]["tip_sha"]), saved["files"])

    def test_accounting_tasks_complete_without_project_acceptance(self):
        self.assertEqual(runner.unsettled(self.ledger.path), {"reservations": 0, "promotions": 0})
        for name in ("independent", "sequential", "scouts"):
            row = runner.read(self.root / "block" / name / "session.json")
            task = self.ledger.task(row["billing"])
            self.assertEqual(task["status"], "complete")
            self.assertEqual(task["accepted_commit"], row["accounting_record"]["accepted_head"])
            self.assertEqual(row["accounting_record"]["record"],
                             dict(completed=False, status="shared_initial_setup", selected_binding=None))
        self.assertEqual(self.ledger.budget()["spent_or_reserved"], 0)

    def test_both_pools_precede_candidate_blind_common_scouts(self):
        spans = runner.read(self.root / "block" / "timings.json")["spans"]
        freezes = [runner.read(self.root / "block" / name / "initial-pool.json") for name in ("independent", "sequential")]
        for row in runner.read(self.root / "block" / "scouts" / "session.json")["invocations"]:
            dispatch = spans[row["physical_span_id"]]
            self.assertGreaterEqual(dispatch["started_monotonic_ns"], max(value["frozen_monotonic_ns"] for value in freezes))
            request = runner.read(self.root / "block" / "scouts" / "requests" / (row["call_id"] + ".request.json"))
            files = dict(request["files"])
            context = json.loads(files.pop("scout-context.json"))
            self.assertEqual(files, self.fixture.PROJECT["initial_files"])
            self.assertEqual(context["stage_index"], 0)
            self.assertEqual(len(context["milestones"]), 1)
            self.assertNotIn("golden", json.dumps(request))
            self.assertNotIn("private-", json.dumps(context))

    def test_serial_source_uses_predecessor_while_independent_sources_match(self):
        for name in ("independent", "sequential"):
            inputs = [runner.read(self.root / "block" / name / "requests" / f"builder-slot-{slot}-1.request.json")
                      for slot in range(4)]
            self.assertEqual(inputs[0]["files"]["solution.py"], self.fixture.PROJECT["initial_files"]["solution.py"])
            for request in inputs[1:]:
                source = self.fixture.PROJECT["initial_files"]["solution.py"] if name == "independent" else self.fixture.PROJECT["stages"][0]["known_files"]["solution.py"]
                self.assertEqual(request["files"]["solution.py"], source)
                feedback = json.loads(request["feedback"])
                self.assertEqual("evidence" in feedback, name == "sequential")

    def test_a_b_import_exact_same_pool_and_c_shares_only_scouts(self):
        a = self._import("current-independent")
        b = self._import("improved-independent")
        c = self._import("improved-sequential")
        self.assertEqual(a["frozen_initial_pool"], b["frozen_initial_pool"])
        self.assertNotEqual(a["frozen_initial_pool"]["pool_sha256"], c["frozen_initial_pool"]["pool_sha256"])
        self.assertEqual(a["scouts"], b["scouts"])
        self.assertEqual(b["scouts"], c["scouts"])
        self.assertTrue(all(row["attribution"]["physical_provider_calls"] == 0 for row in (a, b, c)))
        self.assertTrue(all(row["attribution"]["charged_micro_usd"] == 0 for row in (a, b, c)))

    def test_import_mismatch_rejects_before_model_or_candidate_execution(self):
        variants: list[dict[str, Any]] = [dict(repetition=1), dict(project_id="job-queue"), dict(contract_sha256="a" * 64),
                    dict(expected_manifest_sha256="b" * 64), dict(initial_files={"other": "source"})]
        for variant in variants:
            with self.subTest(variant=variant), self.assertRaises(ValueError):
                self._import(**variant)

    def test_unselected_serial_pool_tampering_invalidates_a_b_import(self):
        path = self.root / "block" / "sequential" / "initial-pool.json"
        original = path.read_bytes()
        try:
            value = json.loads(original)
            value["pool"]["formation"] = "independent"
            runner.save(path, value)
            with self.assertRaisesRegex(ValueError, "artifact binding"):
                self._import()
        finally:
            path.write_bytes(original)

    def test_import_freshly_validates_every_arm_without_initial_provider_calls(self):
        for policy in runner.POLICIES:
            imported = self._import(policy)
            calls, evaluations, retained = [], [], []
            def invoke(**request):
                calls.append(request)
                self.assertEqual(request["role"], "reviewer")
                alias = request["metadata"]["alias_by_slot"]["slot-0"]
                return WorkerResult({"review.json": json.dumps(dict(candidate=alias, action="accept",
                    notes="Checked", remaining=[], probes=[]))}, "", 0, {"api_calls": 0})
            def evaluate(files, cases, label):
                evaluations.append(label)
                return FakeValidator("unit-test-image").evaluate(files, cases)
            def retain(files, label):
                retained.append(label)
                return dict(store_path="unit-test-only", tip_sha=label, files_sha256=runner.digest(files))
            def after_initial(pool):
                return dict(freeze_id="unit-test-only", pool_sha256=runner.digest(pool), scouts=imported["scouts"])
            result = run_continuation_stage(self.fixture.PROJECT, 0,
                runner.POLICIES[policy]["formation"] + "-four", self.fixture.PROJECT["initial_files"], {},
                invoke=invoke, evaluate=evaluate, retain=retain,
                validate_probes=runner.oracle_gate(self.fixture, self.fixture.PROJECT), emit=lambda *a, **kw: None,
                after_initial=after_initial, candidate_seed=runner.candidate_seed("warehouse", 0),
                baseline_approval=self.manifest["baseline_approval"], controller_mode=runner.POLICIES[policy]["controller"],
                frozen_initial_pool=imported["frozen_initial_pool"])
            self.assertTrue(result["completed"])
            self.assertEqual(result["metrics"]["initial_builder_calls"], 0)
            self.assertEqual(result["metrics"]["shared_initial_builder_opportunities"], 4)
            self.assertEqual(len(retained), 4)
            self.assertEqual(sum(label.endswith("-fresh-evidence") for label in evaluations), 4)
            self.assertEqual(len(calls), 1)

    def test_shared_timing_snapshot_stays_immutable_when_global_clock_grows(self):
        path = self.root / "block" / "timings.json"
        before = path.read_bytes()
        with self.spans.measure("unit-test-later-work"):
            pass
        self.assertEqual(path.read_bytes(), before)
        self.assertNotEqual(path.read_bytes(), self.spans.path.read_bytes())

    def test_reusing_setup_output_is_rejected_and_manifest_unchanged(self):
        with self.assertRaisesRegex(ValueError, "fresh"):
            runner.prepare_shared_block(self.root / "block", "warehouse", 0,
                contract_sha256=self.contract_sha, image="unit-test-image", ledger=self.ledger,
                budget=self.budget, spans=self.spans, expected_contract=self.expected,
                validator_factory=FakeValidator)
        self.assertEqual(runner.sha_file(self.path), self.sha)

    def test_independent_audit_refuses_unit_validator_and_full_cohort_claim(self):
        with self.assertRaisesRegex(ValueError, "physical-Docker"):
            audit.audit_shared_setup(self.root / "block")
        with self.assertRaisesRegex(ValueError, "unsupported"):
            audit.audit_run(self.root / "block")

    def _pool_audit_inputs(self, formation):
        directory = self.root / "block" / formation
        session = runner.read(directory / "session.json")
        pool = runner.read(directory / "initial-pool.json")["pool"]
        spans = {row["span_id"]: row for row in runner.read(self.root / "block" / "timings.json")["spans"]}
        # Isolated structural audit controls, paired with fake_receipt below.
        # The retained fixture on disk stays explicitly unit_test_control and
        # remains ineligible for a physical shared-setup certificate.
        for span in spans.values():
            if span["kind"] == "docker_validation":
                span["validator_origin"] = "physical_docker"
        calls, _ = audit.audit_calls(directory, session["invocations"], session["billing"],
            self.expected, "rehearsal", spans, session["session_id"], 0)
        return pool, dict(project=self.fixture.PROJECT, candidate_seed=runner.candidate_seed("warehouse", 0),
            baseline_approval=self.manifest["baseline_approval"], formation=formation,
            calls=calls, contract=self.expected, root=directory, spans=spans,
            run_id=session["session_id"], initial_result=session["result"])

    def test_independent_pool_reconstruction_checks_both_formations(self):
        # Only the low-level typed Docker result parser is mocked. This is a
        # controller/journal/source reconstruction unit test, never a physical
        # execution audit certificate or a qualification artifact.
        def fake_receipt(receipt, files, cases, contract):
            return {case["id"]: {"passed": True} for case in cases}
        for formation in ("independent", "sequential"):
            pool, arguments = self._pool_audit_inputs(formation)
            with self.subTest(formation=formation), patch.object(audit, "audit_receipt", side_effect=fake_receipt):
                result = audit.audit_initial_pool(pool, **arguments)
                self.assertEqual(result["initial_opportunities"], 4)
                self.assertEqual(result["public_executions"], 4)

    def test_independent_pool_reconstruction_rejects_source_or_opportunity_tampering(self):
        pool, arguments = self._pool_audit_inputs("independent")
        calls = deepcopy(arguments["calls"])
        del calls["builder-slot-3-1"]
        with self.assertRaisesRegex(ValueError, "four unique"):
            audit.audit_initial_pool(pool, **{**arguments, "calls": calls})
        calls = deepcopy(arguments["calls"])
        calls["builder-slot-0-1"][0]["files"]["fixed.txt"] = "altered trusted adapter"
        with self.assertRaisesRegex(ValueError, "builder source"):
            audit.audit_initial_pool(pool, **{**arguments, "calls": calls})

    def test_full_setup_audit_pipeline_with_explicit_temporary_unit_doubles(self):
        # Deliberately patch the validator class identity as well as the typed
        # receipt parser to exercise top-level auditor integration. Everything
        # lives in a temporary unit fixture. No physical execution certificate
        # is written or offered as qualification by this test.
        with tempfile.TemporaryDirectory() as temporary, \
                patch.object(runner, "fixture", return_value=self.fixture), \
                patch.object(runner, "BlackboxValidator", FakeValidator), \
                patch.object(runner.OpenAIWorker, "run", side_effect=AssertionError("Provider call forbidden")):
            root = Path(temporary).resolve()
            image = "sha256:" + "0" * 64
            expected = runner.contract(image)
            ledger = Ledger(root / "ledger.sqlite", budget_units=0)
            budget = runner.StudyBudget(ledger, "full-unit-fixture", 0, root / "budget.lock")
            runner.prepare_shared_block(root / "block", "warehouse", 0,
                contract_sha256=runner.digest(expected), image=image, ledger=ledger, budget=budget,
                spans=runner.Spans(root / "clock.json"), expected_contract=expected,
                validator_factory=FakeValidator)
            def fake_receipt(receipt, files, cases, contract):
                return {case["id"]: {"passed": True} for case in cases}
            with patch.object(audit, "audit_receipt", side_effect=fake_receipt):
                result = audit.audit_shared_setup(root / "block")
                self.assertEqual(result["scripted_calls"], 10)
                self.assertEqual(result["physical_provider_calls"], 0)
                self.assertTrue(result["scope"]["shared_initial_setup"])
                self.assertFalse(result["scope"]["primary_cohort"])
                self.assertFalse(result["scope"]["prospective_arm_execution"])
                manifest_path = root / "block" / "shared-setup.json"
                original_snapshot = root / "block" / "source-snapshot" / "gossip_harness" / "continuation_followup.py"
                snapshot_bytes = original_snapshot.read_bytes()
                original_snapshot.write_bytes(snapshot_bytes + b"\n# changed snapshot\n")
                with self.assertRaisesRegex(ValueError, "artifact binding"):
                    runner.import_shared_initial(manifest_path, "current-independent", project_id="warehouse",
                        repetition=0, initial_files=self.fixture.PROJECT["initial_files"],
                        contract_sha256=runner.digest(expected), expected_manifest_sha256=runner.sha_file(manifest_path))
                original_snapshot.write_bytes(snapshot_bytes)
                request_path = root / "block" / "scouts" / "requests" / "scout-scout-0-1.request.json"
                request = runner.read(request_path)
                request["files"]["solution.py"] = "candidate source leaked into scout"
                runner.save(request_path, request)
                with self.assertRaises(ValueError):
                    audit.audit_shared_setup(root / "block")
