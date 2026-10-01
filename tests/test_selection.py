"""Scientific controls for oracle-assisted candidate selection (no API or code execution)."""

from copy import deepcopy
import json
import unittest

from gossip_harness.selection import (
    build_pool, case_id, evaluate_selection, json_equal, parse_proposals, select_candidate,
)


class Task:
    id = "synthetic-double"
    spec = "D1: Return twice the nonnegative integer input. D2: Zero stays zero."
    requirements = ("D1", "D2")

    @staticmethod
    def oracle(value):
        if type(value) is not int or value < 0:
            raise ValueError("not a nonnegative integer")
        return value * 2


def case(value, expected=None, requirement="D1", **extra):
    return {"input": value, "expected": value * 2 if expected is None else expected,
            "requirement": requirement, **extra}


def proposed(cases, origin="candidate-a"):
    return parse_proposals(json.dumps(cases), Task(), origin)


class ProposalValidationTests(unittest.TestCase):
    def test_oracle_validates_expectation_and_preserves_provenance(self):
        receipt = proposed([case(2), case(3, expected=5), case(0, requirement="D2")])
        self.assertTrue(receipt["oracle_assisted"])
        self.assertEqual([item["input"] for item in receipt["eligible"]], [2, 0])
        self.assertEqual(receipt["rejected"][0]["reason"], "wrong_expectation")
        accepted = receipt["eligible"][0]
        self.assertEqual(accepted["id"], case_id(case(2)))
        self.assertEqual(len(accepted["validation_sha256"]), 64)
        self.assertEqual(accepted["origin"], "candidate-a")
        self.assertEqual(accepted["claimed_expected_sha256"], accepted["oracle_expected_sha256"])
        self.assertEqual(build_pool(receipt)[0]["provenance"][0]["validation_policy"], "fixture-oracle-v1")

    def test_rejects_invalid_labels_schema_inputs_and_duplicate_cases(self):
        receipt = proposed([case(1, requirement="UNKNOWN"), {**case(1), "code": "exec('bad')"},
                            case(-1), case(2), case(2, requirement="D2"), case(True)])
        self.assertEqual(len(receipt["eligible"]), 1)
        self.assertEqual([item["reason"] for item in receipt["rejected"]], [
            "unsupported_requirement", "case_schema", "oracle_rejected_input", "duplicate_input",
            "oracle_rejected_input"])

    def test_invalid_batch_never_partially_accepts_or_truncates(self):
        bodies = ["{}", "[NaN]", '[{"input":1,"input":2,"expected":4,"requirement":"D1"}]',
                  json.dumps([case(i) for i in range(7)]), "x" * 65537,
                  "[" * 1200 + "0" + "]" * 1200, "\ud800"]
        for body in bodies:
            with self.subTest(body=body[:70]):
                receipt = parse_proposals(body, Task(), "blind")
                self.assertEqual(receipt["eligible"], [])
                self.assertEqual(len(receipt["rejected"]), 1)
        self.assertEqual(parse_proposals("[]", Task(), "blind")["rejected"], [])

    def test_recursive_json_bounds_and_numeric_types(self):
        deep = 0
        for _ in range(12):
            deep = [deep]
        values = [deep, [0] * 257, "s" * 4097, 2**64,
                  {str(i): 0 for i in range(129)}]
        for value in values:
            receipt = proposed([{"input": value, "expected": 0, "requirement": "D1"}])
            self.assertEqual(receipt["rejected"][0]["reason"], "case_exceeds_json_limits")
        self.assertFalse(json_equal(True, 1))
        self.assertFalse(json_equal({"n": [2, None]}, {"n": [2.0, None]}))
        self.assertTrue(json_equal({"n": [2, None]}, {"n": [2, None]}))
        receipt = proposed([case(2, expected=4.0)])
        self.assertEqual(receipt["eligible"], [])
        self.assertEqual(receipt["rejected"][0]["reason"], "wrong_expectation")

    def test_contract_origin_and_case_mutation_invalidate_binding(self):
        receipt = proposed([case(2)])
        for mutate in (
            lambda r: r["eligible"][0].update(expected=999),
            lambda r: r["eligible"][0].update(input=3),
            lambda r: r["eligible"][0].update(requirement="D2"),
            lambda r: r.update(origin="candidate-b"),
            lambda r: r.update(task_contract_sha256="0" * 64),
            lambda r: r.update(oracle_assisted=False),
        ):
            changed = deepcopy(receipt)
            mutate(changed)
            with self.assertRaisesRegex(ValueError, "binding"):
                build_pool(changed)

    def test_oracle_cannot_mutate_retained_input(self):
        class MutatingTask(Task):
            @staticmethod
            def oracle(value):
                value.append(2)
                return len(value)
        receipt = parse_proposals('[{"input":[1],"expected":2,"requirement":"D1"}]', MutatingTask(), "blind")
        self.assertEqual(receipt["eligible"][0]["input"], [1])


class PoolAndSelectionTests(unittest.TestCase):
    def setUp(self):
        self.public = [case(0, requirement="D2", origin="public")]
        self.pool = build_pool(self.public, proposed([case(1), case(2)]))
        self.ids = [item["id"] for item in self.pool]

    def select(self, matrix, order=None):
        return select_candidate(matrix, cases=self.pool, public_case_ids=self.ids[:1], candidate_order=order)

    def test_pool_deduplicates_behavior_and_aggregates_origins(self):
        duplicate = proposed([case(1), case(2)], origin="candidate-b")
        pool = build_pool(self.public, proposed([case(1), case(2)]), duplicate)
        self.assertEqual(len(pool), 3)
        self.assertEqual(pool[1]["origins"], ["candidate-a", "candidate-b"])
        self.assertEqual(len(pool[1]["provenance"]), 2)
        # Duplicating submitted tests cannot increase their selection weight.
        matrix = {"a": {self.ids[0]: True, self.ids[1]: True, self.ids[2]: False},
                  "b": {self.ids[0]: True, self.ids[1]: False, self.ids[2]: True}}
        selection = select_candidate(matrix, cases=pool, public_case_ids=self.ids[:1], candidate_order=["b", "a"])
        self.assertEqual(selection["selected"], "b")
        self.assertEqual(selection["ranking"][0]["total"], 3)
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            build_pool([case(1)], [case(1, expected=3)])

    def test_public_gate_beats_higher_nonpublic_score_and_abstains(self):
        matrix = {"a": dict(zip(self.ids, [True, False, False])),
                  "b": dict(zip(self.ids, [False, True, True]))}
        self.assertEqual(self.select(matrix)["selected"], "a")
        matrix["a"][self.ids[0]] = False
        result = self.select(matrix)
        self.assertTrue(result["abstained"])
        self.assertIsNone(result["selected"])

    def test_highest_unique_pass_count_then_preregistered_order(self):
        matrix = {"a": dict.fromkeys(self.ids, True), "b": dict.fromkeys(self.ids, True)}
        result = self.select(matrix, ["b", "a"])
        self.assertEqual(result["selected"], "b")
        matrix["b"][self.ids[2]] = False
        self.assertEqual(self.select(matrix, ["b", "a"])["selected"], "a")
        self.assertFalse(result["hidden_outcomes_used"])
        self.assertEqual(len(result["matrix_sha256"]), 64)

    def test_missing_outcomes_fail_extra_hidden_or_nonboolean_outcomes_reject(self):
        result = self.select({"a": {self.ids[0]: True}, "b": {}})
        self.assertEqual(result["selected"], "a")
        self.assertEqual(result["ranking"][0]["missing_outcomes"], 2)
        for matrix in ({"a": {"hidden-id": True}}, {"a": {self.ids[0]: 1}}):
            with self.assertRaises(ValueError):
                self.select(matrix)
        with self.assertRaises(ValueError):
            self.select({"a": {}}, ["a", "a"])
        with self.assertRaises(ValueError):
            select_candidate({"a": {}}, cases=self.pool + self.pool[:1], public_case_ids=self.ids[:1])
        with self.assertRaises(ValueError):
            select_candidate({"a": {}}, cases=self.pool, public_case_ids=[])

    def test_sealed_evaluation_reports_coverage_separately_from_selection(self):
        selected = self.select({"a": dict.fromkeys(self.ids, True), "b": dict.fromkeys(self.ids, True)}, ["a", "b"])
        verdicts = {"a": False, "b": True}
        evaluation = evaluate_selection(selected, verdicts)
        self.assertTrue(evaluation["pass_at_k"])
        self.assertTrue(evaluation["selector_regret"])
        self.assertFalse(evaluation["selected_correct"])
        self.assertEqual(selected["selected"], "a")
        abstained = self.select({"a": {}, "b": {}})
        self.assertTrue(evaluate_selection(abstained, verdicts)["selector_regret"])
        no_correct = evaluate_selection(selected, {"a": False, "b": False})
        self.assertFalse(no_correct["pass_at_k"])
        self.assertFalse(no_correct["selector_regret"])
        with self.assertRaises(ValueError):
            evaluate_selection(selected, {"a": True})


if __name__ == "__main__":
    unittest.main()
