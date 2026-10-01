"""Fake-evaluator qualification boundaries; no Docker, candidate or API execution."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from analysis import qualify_benchmark as qualifier
from gossip_harness.blackbox_validator import BlackboxValidator


IMAGE = "sha256:" + "a" * 64


def fixture():
    def case(name):
        return dict(id=name, requirement="R1", input=dict(name=name), expected=dict(value=name))

    def files(marker):
        return {"solution.py": "# Fake evaluator data only: " + marker}

    stages = [dict(known_files=files("golden-" + str(index)),
                   visible_cases=[case("public-" + str(index))],
                   hidden_cases=[case("private-" + str(index))]) for index in range(2)]

    def faults(index):
        return [dict(id="fault-" + str(number), family="family-" + str(number),
                     files=files(f"mutant-{index}-{number}"),
                     witness_cases=[case("witness-" + str(number))]) for number in range(5)]

    def controls(index):
        return [dict(id="control-" + str(number), files=files(f"control-{index}-{number}"))
                for number in range(2)]

    return SimpleNamespace(PROJECT=dict(id="unit-project", stages=stages),
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


class BenchmarkQualificationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.module = fixture()
        self.source = self.root / "dependency.py"
        self.source.write_text("frozen dependency\n")
        contract = dict(image=IMAGE, case_timeout_seconds=qualifier.CASE_TIMEOUT,
                        suite_timeout_seconds=qualifier.SUITE_TIMEOUT)
        self.prepared = dict(contract=contract, contract_sha256=qualifier.digest(contract),
            matrix=qualifier.matrix_for([self.module]), inputs={"dependency.py": self.source.read_bytes()})
        self.calls = []
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
        for manager in (patch.object(qualifier, "REPOSITORY", self.root),
                        patch.object(qualifier, "prepare", side_effect=lambda image=IMAGE: deepcopy(self.prepared)),
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
        self.assertEqual(len(self.calls), 16)
        self.assertEqual(len({id(row["validator"]) for row in self.calls}), 16)
        self.assertEqual([row["public_surviving_family_count"] for row in result["conclusion"]["stages"]], [5, 5])
        self.assertTrue(all(row["physically_executed"] and not row["reused"] for row in result["executions"]))
        self.assertEqual(self.validate(), result)
        self.assertEqual(len(self.calls), 16)

    def test_golden_and_controls_cover_private_and_all_fault_witnesses(self):
        matrix = self.prepared["matrix"]
        for row in matrix["rows"]:
            if row["kind"] != "mutant":
                self.assertEqual(len(row["groups"]["witness"]), 5)
                self.assertEqual(len(row["groups"]["private"]), row["stage_index"] + 1)
                self.assertEqual(len(row["groups"]["public"]), row["stage_index"] + 1)
            else:
                self.assertNotIn("private", row["groups"])
                self.assertEqual(len(row["groups"]["witness"]), 1)

    def test_correct_control_may_repeat_golden_but_runs_independently(self):
        previous = self.module.correct_controls
        self.module.correct_controls = lambda stage: [
            dict(id="correct", files=self.module.PROJECT["stages"][stage]["known_files"]),
            previous(stage)[1]]
        self.prepared["matrix"] = qualifier.matrix_for([self.module])
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
            qualifier.matrix_for([self.module])

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
        self.assertTrue(result["qualified"])
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
        self.assertEqual(len(self.calls), 16)

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
            qualifier.matrix_for([self.module])
        self.module = fixture()
        faults = self.module.fault_bank(0)
        faults[1]["witness_cases"] = [dict(faults[0]["witness_cases"][0], expected={"contradiction": True})]
        self.module.fault_bank = lambda stage: faults
        with self.assertRaisesRegex(ValueError, "Conflicting expectations"):
            qualifier.matrix_for([self.module])


if __name__ == "__main__":
    unittest.main()
