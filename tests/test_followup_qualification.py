"""Fake-evaluator qualification boundaries; no Docker, candidate or API execution."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
from typing import Any
import unittest
from unittest.mock import patch

from analysis import qualify_continuation_followup as qualifier
from gossip_harness.blackbox_validator import BlackboxValidator


IMAGE = "sha256:" + "a" * 64


REAL_PREPARE = qualifier.prepare
REAL_RUNTIME_IDENTITY = qualifier.runtime_identity


def fixture(project_id="warehouse"):
    def case(name):
        return dict(id=name, requirement="R1", input=dict(name=name), expected=dict(value=name))

    def files(marker):
        return {"solution.py": "# Fake evaluator data only: " + project_id + "-" + marker}

    stages = [dict(known_files=files("golden-" + str(index)),
                   visible_cases=[case("public-" + str(index))],
                   hidden_cases=[case("private-" + str(index)),
                       *[case("witness-" + str(number)) for number in range(5)]]) for index in range(2)]

    def faults(index):
        return [dict(id="fault-" + str(number), family="family-" + str(number),
                     files=files(f"mutant-{index}-{number}"),
                     witness_cases=[case("witness-" + str(number))]) for number in range(5)]

    def controls(index):
        return [dict(id="control-" + str(number), files=files(f"control-{index}-{number}"))
                for number in range(2)]

    return SimpleNamespace(PROJECT=dict(id=project_id, stages=stages),
                           fault_bank=faults, correct_controls=controls)


def raw_receipt(files, cases, *, fault=None, infrastructure=False, crash=False):
    outcomes = []
    marker = files["solution.py"]
    for index, case in enumerate(cases):
        fails = ("mutant-" in marker and case["input"]["name"] == "witness-" + marker.rsplit("-", 1)[-1])
        if fault is not None:
            fails = fault(files, case)
        actual = {"wrong": True} if fails else deepcopy(case["expected"])
        status = "error" if crash and fails else "wrong_answer" if fails else "passed"
        outcome = dict(index=index, passed=not fails, status=status,
                       **{key: case[key] for key in ("id", "requirement") if key in case})
        if status != "error":
            outcome["actual"] = actual
        outcomes.append(outcome)
    passed = all(row["passed"] for row in outcomes)
    return dict(protocol=qualifier.BLACKBOX_PROTOCOL, status="sandbox_error" if infrastructure
                else "passed" if passed else "failed", passed=passed,
        cleanup_verified=not infrastructure, source_sha256=qualifier.digest(files),
        suite_sha256=qualifier.digest(cases), image_id=IMAGE,
        case_timeout_seconds=qualifier.CASE_TIMEOUT, timeout_seconds=qualifier.SUITE_TIMEOUT,
        adapter_sha256=qualifier.ADAPTER_SHA256, case_count=len(cases), exit_code=0,
        timed_out=False, input_delivery_failed=False, output_truncated=False,
        container_name="gossip-blackbox-" + hashlib.sha256(marker.encode()).hexdigest(), outcomes=outcomes)


class FollowupQualificationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.module = fixture()
        self.queue = fixture("job-queue")
        self.source = self.root / "dependency.py"
        self.source.write_text("frozen dependency\n")
        contract = dict(image=IMAGE, case_timeout_seconds=qualifier.CASE_TIMEOUT,
                        suite_timeout_seconds=qualifier.SUITE_TIMEOUT)
        self.prepared = dict(contract=contract, contract_sha256=qualifier.digest(contract),
            matrix=qualifier.matrix_for([self.module, self.queue]),
            inputs={"dependency.py": self.source.read_bytes()},
            execution_environment=qualifier.execution_environment(),
            execution_environment_sha256=qualifier.digest(qualifier.execution_environment()))
        self.calls: list[dict[str, Any]] = []
        owner = self

        class FakeValidator:
            _inputs = staticmethod(BlackboxValidator._inputs)

            def __init__(self, image, **kwargs):
                self.image = image
                self.last_receipt = {}

            def preflight(self):
                return True, "Fake preflight; no Docker"

            def evaluate(self, files, cases):
                owner.calls.append(dict(files=deepcopy(files), cases=deepcopy(cases), validator=self))
                self.last_receipt = raw_receipt(files, cases)
                self.last_receipt["container_name"] += "-" + str(len(owner.calls))
                return self.last_receipt

        self.fake_validator = FakeValidator
        self.runtime = dict(server_sha256="1" * 64, daemon_id_sha256="2" * 64,
                            configuration_sha256="3" * 64, context_sha256="4" * 64,
                            image=dict(Id=IMAGE, Os="linux", Architecture="arm64"))
        for manager in (patch.object(qualifier, "REPOSITORY", self.root),
                        patch.object(qualifier, "prepare", side_effect=lambda image=IMAGE: deepcopy(self.prepared)),
                        patch.object(qualifier, "runtime_identity", side_effect=lambda image: deepcopy(self.runtime)),
                        patch.object(qualifier, "BlackboxValidator", FakeValidator)):
            manager.start()
            self.addCleanup(manager.stop)

    def run_qualification(self):
        self.output = self.root / "qualification"
        return qualifier.run(self.output, IMAGE)

    def validate(self):
        return qualifier.validate_qualification(self.output / "results.json", self.prepared["contract_sha256"])

    def rewrite(self, name, mutate):
        path = self.output / name
        value = json.loads(path.read_text())
        mutate(value)
        path.write_text(json.dumps(value))

    def test_success_is_fresh_full_matrix_and_validates_without_reexecution(self):
        result = self.run_qualification()
        self.assertEqual(result["status"], "qualified")
        self.assertEqual(len(self.calls), 32)
        self.assertEqual(len({id(row["validator"]) for row in self.calls}), 32)
        self.assertEqual([row["public_surviving_family_count"] for row in result["conclusion"]["stages"]], [5, 5, 5, 5])
        self.assertTrue(all(row["physically_executed"] and not row["reused"] for row in result["executions"]))
        self.assertEqual(self.validate(), result)
        self.assertEqual(len(self.calls), 32)

    def test_golden_and_controls_cover_private_and_all_fault_witnesses(self):
        matrix = self.prepared["matrix"]
        for row in matrix["rows"]:
            if row["kind"] != "mutant":
                self.assertEqual(len(row["groups"]["witness"]), 5)
                self.assertEqual(len(row["groups"]["private"]), row["stage_index"] + 6)
                self.assertEqual(len(row["groups"]["public"]), row["stage_index"] + 1)
            else:
                self.assertTrue(set(row["groups"]["witness"]) <= set(row["groups"]["private"]))
                self.assertEqual(len(row["groups"]["witness"]), 1)

    def test_correct_control_may_repeat_golden_but_runs_independently(self):
        previous = self.module.correct_controls
        self.module.correct_controls = lambda stage: [
            dict(id="correct", files=self.module.PROJECT["stages"][stage]["known_files"]),
            previous(stage)[1]]
        self.prepared["matrix"] = qualifier.matrix_for([self.module, self.queue])
        result = self.run_qualification()
        self.assertEqual(result["status"], "qualified")
        self.assertEqual(self.calls[0]["files"], self.calls[1]["files"])
        self.assertIsNot(self.calls[0]["validator"], self.calls[1]["validator"])
        self.assertNotEqual(result["executions"][0]["purpose"], result["executions"][1]["purpose"])
        self.validate()

    def test_two_correct_controls_without_distinct_source_variant_rejected(self):
        self.module.correct_controls = lambda stage: [
            dict(id="correct", files=self.module.PROJECT["stages"][stage]["known_files"]),
            dict(id="also-correct", files=self.module.PROJECT["stages"][stage]["known_files"])]
        with self.assertRaisesRegex(ValueError, "At least one semantics-preserving"):
            qualifier.matrix_for([self.module, self.queue])

    def test_conflicting_expected_labels_rejected_before_execution(self):
        first = dict(input={"a": 1}, expected=True)
        with self.assertRaisesRegex(ValueError, "Conflicting expectations"):
            qualifier.ordered_suite(dict(public=[first], witness=[dict(first, expected=1)]))
        self.assertEqual(self.calls, [])

    def test_exact_duplicate_inputs_preserve_first_order_and_group_membership(self):
        a, b = dict(input={"x": 1}, expected=2, id="a"), dict(input={"x": 2}, expected=3, id="b")
        cases, groups = qualifier.ordered_suite(dict(public=[a, b, a], witness=[dict(a, id="alias")]))
        self.assertEqual(cases, [a, b])
        self.assertEqual(groups, dict(public=[0, 1], witness=[0]))

    def test_mutant_runtime_crash_is_not_semantic_fault_detection(self):
        row = next(row for row in self.prepared["matrix"]["rows"] if row["kind"] == "mutant")
        receipt = raw_receipt(row["files"], row["cases"], crash=True)
        qualifier._verified(receipt, row, self.prepared["contract"])
        result = qualifier.classify(row, receipt)
        self.assertFalse(result["qualified"])
        self.assertFalse(result["runnable"])
        self.assertEqual(result["witness_wrong_answers"], [])

    def test_public_failure_is_classified_but_not_a_public_surviving_family(self):
        row = next(row for row in self.prepared["matrix"]["rows"] if row["kind"] == "mutant")
        receipt = raw_receipt(row["files"], row["cases"], fault=lambda files, case: True)
        result = qualifier.classify(row, receipt)
        self.assertFalse(result["qualified"])
        self.assertFalse(result["public_surviving"])

    def test_fewer_than_four_distinct_families_fails_even_with_many_killed_mutants(self):
        rows = [qualifier.classify(row, raw_receipt(row["files"], row["cases"]))
                for row in self.prepared["matrix"]["rows"]]
        for row in rows:
            if row["kind"] == "mutant":
                row["family"] = "same-family"
        result = qualifier.conclusions(self.prepared["matrix"], rows)
        self.assertTrue(result["complete"])
        self.assertFalse(result["qualified"])
        self.assertEqual(result["stages"][0]["public_surviving_family_count"], 1)

    def test_unkilled_named_mutant_fails_even_when_four_other_families_survive(self):
        rows = []
        for row in self.prepared["matrix"]["rows"]:
            kwargs = dict(fault=lambda files, case: False) if row["source_id"] == "fault-4" else {}
            rows.append(qualifier.classify(row, raw_receipt(row["files"], row["cases"], **kwargs)))
        result = qualifier.conclusions(self.prepared["matrix"], rows)
        self.assertEqual(result["stages"][0]["public_surviving_family_count"], 4)
        self.assertFalse(result["qualified"])

    def test_invalid_infrastructure_receipt_is_retained_and_never_retried(self):
        def failed(instance, files, cases):
            self.calls.append(dict(files=files, cases=cases))
            return raw_receipt(files, cases, infrastructure=True)

        with patch.object(self.fake_validator, "evaluate", failed):
            with self.assertRaisesRegex(RuntimeError, "Sandbox infrastructure"):
                self.run_qualification()
        self.assertEqual(len(self.calls), 1)
        self.assertTrue((self.output / "receipts/0000.json").is_file())
        result = json.loads((self.output / "results.json").read_text())
        self.assertEqual(result["status"], "qualification_failed")
        self.assertFalse(result["conclusion"]["complete"])

    def test_validator_exception_retains_partial_last_receipt(self):
        def fail(instance, files, cases):
            instance.last_receipt = {"status": "starting", "source_sha256": qualifier.digest(files)}
            raise RuntimeError("Simulated Docker launch error")

        with patch.object(self.fake_validator, "evaluate", fail):
            with self.assertRaisesRegex(RuntimeError, "Simulated Docker"):
                self.run_qualification()
        raw = json.loads((self.output / "receipts/0000.json").read_text())
        self.assertEqual(raw["last_receipt"]["status"], "starting")
        self.assertEqual(raw["error_type"], "RuntimeError")

    def test_refuses_existing_output_without_touching_receipts(self):
        self.run_qualification()
        before = (self.output / "results.json").read_bytes()
        with self.assertRaisesRegex(ValueError, "Output already exists"):
            qualifier.run(self.output, IMAGE)
        self.assertEqual((self.output / "results.json").read_bytes(), before)
        self.assertEqual(len(self.calls), 32)

    def test_tampered_live_dependency_rejected(self):
        self.run_qualification()
        self.source.write_text("changed dependency\n")
        with self.assertRaisesRegex(ValueError, "dependency changed"):
            self.validate()

    def test_tampered_retained_dependency_rejected(self):
        self.run_qualification()
        (self.output / "inputs/dependency.py").write_text("changed copy\n")
        with self.assertRaisesRegex(ValueError, "Retained qualification dependency"):
            self.validate()

    def test_tampered_raw_receipt_rejected(self):
        self.run_qualification()
        self.rewrite("receipts/0000.json", lambda value: value.update(passed=False))
        with self.assertRaisesRegex(ValueError, "Raw qualification receipt changed"):
            self.validate()

    def test_tampered_conclusion_cannot_qualify(self):
        self.run_qualification()
        self.rewrite("results.json", lambda value: value["conclusion"].update(stages=[]))
        with self.assertRaisesRegex(ValueError, "findings do not follow"):
            self.validate()

    def test_mismatched_contract_digest_rejected(self):
        self.run_qualification()
        with self.assertRaisesRegex(ValueError, "Qualification contract changed"):
            qualifier.validate_qualification(self.output / "results.json", "0" * 64)

    def test_reused_execution_and_missing_matrix_row_rejected(self):
        self.run_qualification()
        original = (self.output / "results.json").read_bytes()
        self.rewrite("results.json", lambda value: value["executions"][0].update(reused=True))
        with self.assertRaisesRegex(ValueError, "execution binding changed"):
            self.validate()
        (self.output / "results.json").write_bytes(original)
        self.rewrite("results.json", lambda value: value["executions"].pop())
        with self.assertRaisesRegex(ValueError, "execution matrix incomplete"):
            self.validate()

    def test_pass_flag_cannot_disagree_with_actual_output(self):
        row = self.prepared["matrix"]["rows"][0]
        receipt = raw_receipt(row["files"], row["cases"])
        receipt["outcomes"][0]["actual"] = {"incorrect": True}
        with self.assertRaisesRegex(ValueError, "disagrees with actual"):
            qualifier._verified(receipt, row, self.prepared["contract"])

    def test_wrong_answer_without_actual_output_is_not_a_witness(self):
        row = next(row for row in self.prepared["matrix"]["rows"] if row["kind"] == "mutant")
        receipt = raw_receipt(row["files"], row["cases"])
        wrong = next(outcome for outcome in receipt["outcomes"] if not outcome["passed"])
        del wrong["actual"]
        with self.assertRaisesRegex(ValueError, "requires actual JSON"):
            qualifier._verified(receipt, row, self.prepared["contract"])

    def test_relabelled_container_reuse_is_rejected(self):
        self.run_qualification()
        raw0 = json.loads((self.output / "receipts/0000.json").read_text())
        self.rewrite("receipts/0001.json", lambda value: value.update(container_name=raw0["container_name"]))
        raw1 = (self.output / "receipts/0001.json").read_bytes()
        self.rewrite("results.json", lambda value: value["executions"][1].update(receipt_sha256=qualifier.sha(raw1)))
        with self.assertRaisesRegex(ValueError, "own fresh container"):
            self.validate()

    def test_duplicate_control_sources_and_conflicting_witnesses_rejected(self):
        self.module.correct_controls = lambda stage: [dict(id="c1", files={"solution.py": "a"}),
                                                       dict(id="c2", files={"solution.py": "a"})]
        with self.assertRaisesRegex(ValueError, "distinct exact source"):
            qualifier.matrix_for([self.module, self.queue])
        self.module = fixture()
        self.queue = fixture("job-queue")
        faults = self.module.fault_bank(0)
        faults[1]["witness_cases"] = [dict(faults[0]["witness_cases"][0], expected={"contradiction": True})]
        self.module.fault_bank = lambda stage: faults
        with self.assertRaisesRegex(ValueError, "private acceptance case"):
            qualifier.matrix_for([self.module, self.queue])

    def test_fixture_roster_must_include_both_domains_in_contract_order(self):
        for modules in ([self.module], [self.queue, self.module], [self.module, self.module]):
            with self.subTest(projects=[module.PROJECT["id"] for module in modules]):
                with self.assertRaisesRegex(ValueError, "exact ordered"):
                    qualifier.matrix_for(modules)

    def test_wrong_or_unknown_study_and_incomplete_cohort_cannot_qualify(self):
        contract: dict[str, Any] = dict(protocol=qualifier.STUDY_PROTOCOL, project_ids=list(qualifier.PROJECT_IDS),
            milestones=2, policies={policy: {} for policy in qualifier.POLICIES}, image=IMAGE,
            case_timeout_seconds=qualifier.CASE_TIMEOUT, suite_timeout_seconds=qualifier.SUITE_TIMEOUT,
            roster=[[project, policy, repetition] for project in qualifier.PROJECT_IDS
                    for policy in qualifier.POLICIES for repetition in range(2)])
        qualifier.validate_study_contract(contract, IMAGE)
        changes: list[dict[str, Any]] = [dict(protocol="evidence-frontier-v1"), dict(milestones=1),
            dict(project_ids=["warehouse"]), dict(policies={"current-independent": {}}),
            dict(roster=contract["roster"][:-1]),
            dict(roster=[contract["roster"][0]] * 12),
            dict(roster=[[*row[:2], bool(row[2])] for row in contract["roster"]]),
            dict(image="sha256:" + "b" * 64), dict(case_timeout_seconds=1)]
        for change in changes:
            with self.subTest(change=change):
                with self.assertRaises(ValueError):
                    qualifier.validate_study_contract(dict(contract, **change), IMAGE)

    def test_nonprivate_semantic_witness_cannot_qualify_bank(self):
        original = self.module.fault_bank
        def faults(stage):
            rows = original(stage)
            rows[0]["witness_cases"] = [dict(input={"name": "outside-private"}, expected=True)]
            return rows
        self.module.fault_bank = faults
        with self.assertRaisesRegex(ValueError, "exact private acceptance case"):
            qualifier.matrix_for([self.module, self.queue])
        self.assertEqual(self.calls, [])

    def test_crash_outside_designated_witness_still_disqualifies_mutant(self):
        row = next(row for row in self.prepared["matrix"]["rows"] if row["kind"] == "mutant")
        receipt = raw_receipt(row["files"], row["cases"])
        other = next(index for index in row["groups"]["private"] if index not in row["groups"]["witness"])
        receipt["outcomes"][other].update(status="error", passed=False)
        del receipt["outcomes"][other]["actual"]
        qualifier._verified(receipt, row, self.prepared["contract"])
        classified = qualifier.classify(row, receipt)
        self.assertTrue(classified["witness_wrong_answers"])
        self.assertFalse(classified["runnable"])
        self.assertFalse(classified["qualified"])

    def test_stable_fixture_identity_and_purpose_bind_every_exact_source(self):
        self.run_qualification()
        ids = [row["fixture_id"] for row in self.prepared["matrix"]["rows"]]
        self.assertEqual(len(ids), len(set(ids)))
        for row in self.prepared["matrix"]["rows"]:
            self.assertEqual(row["purpose"], qualifier.PROTOCOL + "/" + row["fixture_id"])
        self.rewrite("results.json", lambda value: value["executions"][0].update(
            purpose="independent-final-acceptance"))
        with self.assertRaisesRegex(ValueError, "execution binding changed"):
            self.validate()

    def test_duplicate_classifications_cannot_fill_missing_roster_entries(self):
        rows = [qualifier.classify(row, raw_receipt(row["files"], row["cases"]))
                for row in self.prepared["matrix"]["rows"]]
        rows[-1] = deepcopy(rows[-2])
        conclusion = qualifier.conclusions(self.prepared["matrix"], rows)
        self.assertFalse(conclusion["complete"])
        self.assertFalse(conclusion["qualified"])

    def test_execution_environment_change_fails_readonly_receipt_reuse(self):
        self.run_qualification()
        changed = dict(self.prepared["execution_environment"], docker_cli_environment_sha256="0" * 64)
        with patch.object(qualifier, "execution_environment", return_value=changed):
            with self.assertRaisesRegex(ValueError, "execution environment changed"):
                self.validate()
        self.assertEqual(len(self.calls), 32)

    def test_changed_execution_image_limits_adapter_and_order_are_rejected(self):
        row = self.prepared["matrix"]["rows"][0]
        original = raw_receipt(row["files"], row["cases"])
        for change in (dict(image_id="sha256:" + "b" * 64), dict(case_timeout_seconds=1),
                dict(timeout_seconds=2), dict(adapter_sha256="0" * 64),
                dict(suite_sha256="0" * 64), dict(outcomes=list(reversed(original["outcomes"])))):
            with self.subTest(change=list(change)):
                with self.assertRaises((ValueError, RuntimeError)):
                    qualifier._verified(dict(original, **change), row, self.prepared["contract"])

    def test_manifest_tampering_cannot_relabel_execution_environment(self):
        self.run_qualification()
        self.rewrite("manifest.json", lambda value: value["execution_environment"].update(seed="different"))
        manifest_sha = qualifier.sha((self.output / "manifest.json").read_bytes())
        self.rewrite("results.json", lambda value: value.update(manifest_sha256=manifest_sha))
        with self.assertRaisesRegex(ValueError, "matrix or execution contract changed"):
            self.validate()

    def test_preflight_failure_is_retained_without_candidate_execution(self):
        with patch.object(self.fake_validator, "preflight", return_value=(False, "Unavailable fake Docker")):
            with self.assertRaisesRegex(ValueError, "Docker preflight failed"):
                self.run_qualification()
        self.assertEqual(self.calls, [])
        retained = json.loads((self.output / "results.json").read_text())
        self.assertEqual(retained["status"], "qualification_failed")
        self.assertFalse(retained["preflight"]["passed"])
        self.assertFalse(retained["conclusion"]["complete"])

    def test_prepare_binds_full_local_plan_fixture_evaluator_inventory(self):
        source_names = ["continuation_followup.py", "benchmark_warehouse.py", "benchmark_job_queue.py",
            "blackbox_validator.py", "sandbox.py", "verification_experiment.py",
            "sustained_experiment.py", "sustained_checkpoint.py"]
        paths = ["gossip_harness/" + name for name in source_names]
        own_path = "analysis/qualify_continuation_followup.py"
        plan_path = "continuation-followup-study-plan.json"
        for name in [*paths, own_path, plan_path]:
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("Fake dependency bytes: " + name)
        hashes = {name: qualifier.sha((self.root / name).read_bytes())
                  for name in [*paths, own_path, plan_path]}
        contract = dict(protocol=qualifier.STUDY_PROTOCOL, project_ids=list(qualifier.PROJECT_IDS),
            milestones=2, policies={policy: {} for policy in qualifier.POLICIES}, image=IMAGE,
            case_timeout_seconds=qualifier.CASE_TIMEOUT, suite_timeout_seconds=qualifier.SUITE_TIMEOUT,
            roster=[[project, policy, repetition] for project in qualifier.PROJECT_IDS
                    for policy in qualifier.POLICIES for repetition in range(2)],
            plan_sha256=hashes[plan_path], sources={name: hashes["gossip_harness/" + name]
                for name in source_names}, extra_sources={own_path: hashes[own_path]})
        modules = {"warehouse": self.module, "job-queue": self.queue}
        runner = SimpleNamespace(contract=lambda image: deepcopy(contract), fixture=modules.__getitem__)
        with patch.object(qualifier.importlib, "import_module", return_value=runner), \
                patch.object(qualifier, "__file__", str(self.root / own_path)):
            prepared = REAL_PREPARE(IMAGE)
            self.assertEqual(set(prepared["inputs"]), set(hashes))
            self.assertEqual(prepared["contract_sha256"], qualifier.digest(contract))
            self.assertEqual(len(prepared["matrix"]["rows"]), 32)
            self.assertEqual(prepared["execution_environment_sha256"],
                             qualifier.digest(qualifier.execution_environment()))
            (self.root / plan_path).write_text("changed study plan")
            with self.assertRaisesRegex(ValueError, "Contract source changed"):
                REAL_PREPARE(IMAGE)
            contract["sources"].pop("sandbox.py")
            with self.assertRaisesRegex(ValueError, "dependency inventory is incomplete"):
                REAL_PREPARE(IMAGE)

    def test_daemon_change_is_rejected_even_with_unchanged_cli_environment(self):
        self.run_qualification()
        self.runtime["daemon_id_sha256"] = "9" * 64
        with self.assertRaisesRegex(ValueError, "Docker runtime identity changed"):
            self.validate()
        self.assertEqual(len(self.calls), 32)

    def test_daemon_change_during_execution_retains_unverified_receipt(self):
        evaluate = self.fake_validator.evaluate
        def changed(instance, files, cases):
            receipt = evaluate(instance, files, cases)
            self.runtime["context_sha256"] = "9" * 64
            return receipt
        with patch.object(self.fake_validator, "evaluate", changed):
            with self.assertRaisesRegex(ValueError, "Docker runtime identity changed"):
                self.run_qualification()
        retained = json.loads((self.output / "results.json").read_text())
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(retained["status"], "qualification_failed")
        self.assertFalse(retained["executions"][0]["verified"])
        self.assertTrue((self.output / "receipts/0000.json").is_file())

    def test_runtime_collector_binds_context_daemon_and_image_without_execution(self):
        server = dict(Version="unit-engine", ApiVersion="unit-api", Os="linux", Arch="arm64")
        info = dict(ID="unit-daemon", Driver="overlay2", Containers=7, ServerVersion="unit-engine")
        context = [dict(Name="unit-context", Endpoints={"docker": {"Host": "unit-secret-endpoint"}})]
        inspected = dict(Id=IMAGE, Os="linux", Architecture="arm64")
        def collect(values):
            responses = [SimpleNamespace(returncode=0, stdout=json.dumps(value)) for value in values]
            with patch.object(qualifier.subprocess, "run", side_effect=responses) as query:
                identity = REAL_RUNTIME_IDENTITY(IMAGE)
            self.assertEqual([call.args[0][1] for call in query.call_args_list],
                             ["version", "info", "context", "image"])
            return identity
        original = collect([server, info, context, inspected])
        self.assertNotIn("unit-secret-endpoint", json.dumps(original))
        self.assertNotIn("unit-daemon", json.dumps(original))
        self.assertEqual(original, collect([server, dict(info, Containers=100), context, inspected]))
        self.assertNotEqual(original, collect([server, dict(info, ID="other-daemon"), context, inspected]))
        self.assertNotEqual(original, collect([dict(server, Version="new-engine"), info, context, inspected]))
        with self.assertRaisesRegex(ValueError, "pinned Docker image identity"):
            collect([server, info, context, dict(inspected, Id="sha256:" + "b" * 64)])


if __name__ == "__main__":
    unittest.main()
