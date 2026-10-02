"""Zero-provider, zero-Docker tests for immutable paired setup foundations."""
from copy import deepcopy
import json
import re
import threading
from pathlib import Path
import tempfile
from types import SimpleNamespace
from typing import Any
import unittest
from unittest.mock import Mock, patch

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
        budget = runner.study_plan()["budget"]
        available = budget["shared_cumulative_cap_micro_usd"] - budget["expected_starting_usage_micro_usd"]
        for cap in (True, 0, available + 1):
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

    def test_live_admission_requires_exact_numeric_capped_policy(self):
        plan = runner.study_plan()
        plan["readiness"]["live_authorized"] = True
        cap = plan["budget"]["incremental_cap_micro_usd"]
        plan["budget"]["funding"] = dict(status="approved_capped_cohort_admission",
            policy="bounded_budget_termination_no_incomplete_comparison", approved_incremental_micro_usd=cap)
        self.assertTrue(runner.live_readiness(plan)["plan_ready"])
        for amount in (True, cap + 1, cap - 1):
            plan["budget"]["funding"]["approved_incremental_micro_usd"] = amount
            self.assertFalse(runner.live_readiness(plan)["plan_ready"])

    def test_live_mode_rejects_falsey_worker_without_scripted_fallback(self):
        session = runner.ExecutionSession.__new__(runner.ExecutionSession)
        session.fatal, session.mode, session.models = threading.Event(), "live", {"cheap": None}
        with patch.object(runner, "rehearsal_result", side_effect=AssertionError("Scripted fallback forbidden")):
            with self.assertRaisesRegex(ValueError, "invalid live model"):
                session.invoke(role="builder", model="cheap", files={}, instructions="", allowed_paths=[],
                               feedback="", metadata={})
        self.assertTrue(session.fatal.is_set())

    def test_reservation_failure_sets_fatal_before_sibling_can_dispatch(self):
        with tempfile.TemporaryDirectory() as temporary:
            session = runner.ExecutionSession.__new__(runner.ExecutionSession)
            worker = runner.OpenAIWorker("unit-test-no-network", model=runner.MODEL)
            session.fatal, session.mode, session.models = threading.Event(), "live", {"cheap": worker}
            session.contract_check, session.billing, session.contract_sha256 = None, "unit", "unit"
            session.root = Path(temporary)
            session.base = Mock(head=lambda: "unit-head")
            session.lock, session.started_call_ids = threading.RLock(), set()
            session.journal = runner.RequestJournal(session.root / "journal")
            arguments = dict(role="builder", model="cheap", files={}, instructions="", allowed_paths=[],
                feedback="", metadata={"candidate_id": "slot-0", "round": 1})
            with patch.object(worker, "reservation_units", side_effect=ValueError("reservation invalid")), \
                    patch.object(worker, "run", side_effect=AssertionError("Provider forbidden")):
                with self.assertRaisesRegex(ValueError, "reservation invalid"):
                    session.invoke(**arguments)
                self.assertTrue(session.fatal.is_set())
                with self.assertRaisesRegex(RuntimeError, "earlier execution"):
                    session.invoke(**{**arguments, "metadata": {"candidate_id": "slot-1", "round": 1}})


    def test_retained_rehearsal_certificate_matches_all_current_audit_bindings(self):
        current = dict(passed=True, mode="rehearsal", scope={"primary_cohort": True}, protocol="test-audit-v1",
            results_sha256="r" * 64, timings_sha256="t" * 64, freeze_manifest_sha256="f" * 64,
            contract_sha256="c" * 64, auditor_sha256="a" * 64)
        arguments = dict(results_sha256=current["results_sha256"], contract_sha256=current["contract_sha256"])
        runner.validate_rehearsal_certificate(deepcopy(current), current, **arguments)
        for field in ("protocol", "results_sha256", "timings_sha256", "freeze_manifest_sha256",
                      "contract_sha256", "auditor_sha256", "mode"):
            for target in ("saved", "current"):
                saved, fresh = deepcopy(current), deepcopy(current)
                (saved if target == "saved" else fresh)[field] = "changed"
                with self.subTest(field=field, target=target), self.assertRaisesRegex(ValueError, "certificate"):
                    runner.validate_rehearsal_certificate(saved, fresh, **arguments)
        for key in ("passed", "scope"):
            broken = deepcopy(current)
            broken[key] = False if key == "passed" else {"primary_cohort": False}
            with self.subTest(key=key), self.assertRaises(ValueError):
                runner.validate_rehearsal_certificate(broken, current, **arguments)

    def test_live_reaudit_mismatch_or_replacement_stops_before_credentials(self):
        from analysis import qualify_continuation_followup as qualifier
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            proof_path, audit_path = root / "proof" / "results.json", root / "proof-audit.json"
            qualification = root / "qualification.json"
            for path in (proof_path, qualification):
                runner.save(path, {"unit_test_only": True})
            expected = {"image": "unit-test-image"}
            current = dict(passed=True, mode="rehearsal", scope={"primary_cohort": True}, protocol="test-audit-v1",
                results_sha256=runner.sha_file(proof_path), timings_sha256="t" * 64,
                freeze_manifest_sha256="f" * 64, contract_sha256=runner.digest(expected), auditor_sha256="a" * 64)
            for tamper in ("wrong-mode", "old-auditor", "replaced-during-audit"):
                saved = deepcopy(current)
                if tamper == "wrong-mode":
                    saved["mode"] = "live"
                elif tamper == "old-auditor":
                    saved["auditor_sha256"] = "old"
                runner.save(audit_path, saved)
                def reaudited(path):
                    if tamper == "replaced-during-audit":
                        runner.save(audit_path, {**current, "mode": "live"})
                    return deepcopy(current)
                with self.subTest(tamper=tamper), \
                        patch.object(runner, "contract", return_value=expected), \
                        patch.object(runner, "live_readiness", return_value={"plan_ready": True}), \
                        patch.object(qualifier, "validate_qualification", return_value={"runtime_identity": {}}), \
                        patch.object(qualifier, "runtime_identity", return_value={}), \
                        patch.object(qualifier, "baseline_proof", return_value={}), \
                        patch.object(runner, "validate_rehearsal"), \
                        patch.object(audit, "audit_run", side_effect=reaudited), \
                        patch.object(runner, "credential", side_effect=AssertionError("Credential access forbidden")) as credentials:
                    with self.assertRaisesRegex(ValueError, "certificate"):
                        runner._run_owned(root / "never-created", mode="live", qualification=qualification,
                            rehearsal=proof_path, rehearsal_audit=audit_path, image="unit-test-image")
                    credentials.assert_not_called()
                    self.assertFalse((root / "never-created").exists())



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
        with self.assertRaises(ValueError):
            audit.audit_shared_setup(self.root / "block")
        with self.assertRaises((ValueError, FileNotFoundError)):
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

    def test_source_snapshot_tampering_rejected_before_shared_import(self):
        # The cohort auditor owns full qualified-pipeline reconstruction. This
        # foundation test proves snapshot binding without inventing eligibility.
        path = self.root / "block" / "study-plan.json"
        original = path.read_bytes()
        try:
            path.write_bytes(original + b"\n")
            with self.assertRaisesRegex(ValueError, "artifact binding"):
                self._import()
        finally:
            path.write_bytes(original)


class FaultAwareValidator(FakeValidator):
    """Unit oracle for declared operation faults; never executes application code."""
    lock = threading.Lock()

    def preflight(self):
        return True, "Explicit unit control, not Docker qualification"

    def evaluate(self, files, cases):
        with self.lock:
            result = super().evaluate(files, cases)
        faults = re.findall(r"^REHEARSAL_FAULT_OP = '([^']+)'$", "\n".join(files.values()), re.MULTILINE)
        def affected(payload):
            if isinstance(payload, dict):
                return payload.get("op") in faults or any(affected(value) for value in payload.values())
            return isinstance(payload, list) and any(affected(value) for value in payload)
        result["outcomes"] = [dict(index=index, passed=not affected(case["input"]))
                              for index, case in enumerate(cases)]
        result["passed"] = all(item["passed"] for item in result["outcomes"])
        result["status"] = "passed" if result["passed"] else "failed"
        return result


class FollowupCohortRunnerTests(unittest.TestCase):
    temporary: tempfile.TemporaryDirectory[str]
    root: Path
    expected: dict[str, Any]
    report: dict[str, Any]

    @classmethod
    def setUpClass(cls):
        from analysis import qualify_continuation_followup as qualifier
        cls.temporary = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temporary.name).resolve() / "cohort"
        plan = runner.study_plan()
        # The scientific source checks have dedicated unit and independent
        # audit coverage. This fake-validator control runs real fixture inputs,
        # controller decisions, Git stores, journals and the complete barrier.
        cls.expected = dict(protocol=runner.PROTOCOL, image="unit-test-image", sources={}, extra_sources={},
            plan_sha256=runner.sha_file(runner.PLAN_PATH),
            fixtures={pid: dict(fixture_sha256=runner.digest(runner.fixture(pid).PROJECT)) for pid in runner.PROJECT_MODULES},
            models={role: {"reservation_units": 0} for role in ("cheap", "strong")},
            project_ids=list(runner.PROJECT_MODULES), roster=plan["roster"], block_order=plan["block_order"],
            limits=deepcopy(runner.LIMITS), budget=plan["budget"])
        proof_path = cls.root.parent / "unit-qualification.json"
        runner.save(proof_path, {"unit_test_only": True})
        runtime = {"unit_test_only": True}
        with patch.object(runner, "contract", return_value=cls.expected), \
                patch.object(qualifier, "validate_qualification", return_value={"runtime_identity": runtime}), \
                patch.object(qualifier, "runtime_identity", return_value=runtime), \
                patch.object(qualifier, "baseline_proof", side_effect=lambda report, pid, files:
                    dict(unit_test_only=True, project_id=pid, source_sha256=runner.digest(files))), \
                patch.object(runner.OpenAIWorker, "run", side_effect=AssertionError("Provider call forbidden")):
            cls.report = runner.run(cls.root, mode="rehearsal", qualification=proof_path,
                                    image="unit-test-image", validator_factory=FaultAwareValidator)

    @classmethod
    def tearDownClass(cls):
        cls.temporary.cleanup()

    def test_complete_roster_and_fault_controls_finish_without_provider(self):
        self.assertEqual(self.report["status"], "finished")
        self.assertEqual(len(self.report["cases"]), 12)
        self.assertEqual(sum(row["milestones_completed"] for row in self.report["cases"]), 24)
        self.assertTrue(all(row["accepted"] for row in self.report["cases"]))
        calls = [call for row in self.report["shared_setups"] + self.report["cases"] for call in row["invocations"]]
        self.assertEqual(len(calls), 228)
        self.assertTrue(all(not call["physical_dispatch"] and call["usage_units"] == 0 for call in calls))
        self.assertEqual(len({call["reservation"] for call in calls}), len(calls))
        self.assertEqual(self.report["unsettled"], dict(reservations=0, promotions=0))

    def test_full_barrier_precedes_all_independent_final_executions(self):
        frozen = runner.read(self.root / "frozen-trajectories.json")
        spans = runner.read(self.root / "timings.json")["spans"]
        work = [row for row in spans if row["kind"] in {"model_trajectory", "shared_setup_block"}]
        finals = [row for row in spans if row.get("purpose", "").startswith("final_")]
        self.assertEqual(len(work), 16)
        self.assertEqual(len(finals), 24)
        self.assertLessEqual(max(row["finished_monotonic_ns"] for row in work), frozen["frozen_monotonic_ns"])
        self.assertGreaterEqual(min(row["started_monotonic_ns"] for row in finals), frozen["frozen_monotonic_ns"])
        self.assertEqual(len({runner.read(row["receipt_path"])["container_name"] for row in finals}), 24)

    def test_first_pool_pairing_and_later_accepted_lineage(self):
        for pid, repetition in self.expected["block_order"]:
            initial = []
            for policy in runner.POLICIES:
                root = self.root / f"{pid}-{policy}-{repetition}"
                state = runner.read(root / "trajectory.json")
                initial.append(runner.read(root / "stage-0" / "initial-pool.json")["pool"])
                second = runner.read(root / "stage-1" / "session.json")
                self.assertEqual(second["initial_files_sha256"], runner.digest(state["stages"][0]["files"]))
                self.assertEqual(state["stages"][1]["metrics"]["initial_builder_calls"], 4)
                self.assertEqual(state["stages"][0]["metrics"]["initial_builder_calls"], 0)
                self.assertEqual(state["stages"][0]["metrics"]["shared_initial_builder_opportunities"], 4)
            self.assertEqual([initial[0]["candidates"][alias]["files"] for alias in initial[0]["candidate_order"]],
                             [initial[1]["candidates"][alias]["files"] for alias in initial[1]["candidate_order"]])
            self.assertEqual(initial[0]["candidate_order"], initial[1]["candidate_order"])

    def test_rehearsal_retains_regressions_noop_and_focused_review(self):
        for row in self.report["cases"]:
            if row["policy"] == "current-independent":
                continue
            stages = runner.read(self.root / row["run_id"] / "trajectory.json")["stages"]
            self.assertEqual(stages[0]["metrics"]["retained_repair_checkpoints"], 2)
            self.assertEqual(stages[0]["metrics"]["blocked_acceptances"], 2)
            if row["repetition"] == 0:
                self.assertGreaterEqual(stages[0]["metrics"]["repair_regressions"], 1)
            else:
                self.assertEqual(stages[1]["metrics"]["focused_review_requests"], 1)
                self.assertEqual(stages[1]["metrics"]["repairs"], 0)
                self.assertTrue(any(call.get("metadata", {}).get("rehearsal_noop") for call in stages[0]["invocations"]))

    def test_source_evidence_tampering_blocks_private_gate(self):
        frozen = runner.read(self.root / "frozen-trajectories.json")
        path = Path(frozen["trajectories"][0]["stage_artifacts"][0]["scouts"]["path"])
        original = path.read_bytes()
        try:
            path.write_bytes(original + b"\n")
            with self.assertRaisesRegex(ValueError, "Frozen evidence artifact changed"):
                runner.validate_freeze(self.root, self.expected, self.report["freeze_manifest_sha256"])
        finally:
            path.write_bytes(original)

    def test_private_material_absent_from_provider_request_context(self):
        private_ids = {case["id"] for pid in runner.PROJECT_MODULES
                       for stage in runner.fixture(pid).PROJECT["stages"] for case in stage["hidden_cases"]}
        for path in self.root.glob("**/requests/*.request.json"):
            request = runner.read(path)
            content = json.dumps(request)
            self.assertNotIn("known_files", content)
            self.assertNotIn("hidden_cases", content)
            self.assertFalse(any(identifier in content for identifier in private_ids))
