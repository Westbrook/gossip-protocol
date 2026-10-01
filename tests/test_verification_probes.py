"""Offline controls for the data-only verification probe gate."""

from copy import deepcopy
import json
import unittest

from gossip_harness.verification_probes import parse_proposals, revalidate_cases


def validate_input(stage, value):
    if type(value) is not dict or set(value) != {"n"} or type(value["n"]) is not int:
        raise ValueError("outside domain")
    if not 0 <= value["n"] <= (5 if stage == 3 else 10):
        raise ValueError("outside domain")


def reference(stage, value):
    return {"answer": value["n"] * (3 if stage >= 2 else 2)}


def proposal(n, expected=None, requirement="R1"):
    return {"requirement": requirement, "input": {"n": n},
            "expected": {"answer": n * 2} if expected is None else expected}


def admit(probes, **overrides):
    config = {"stage_index": 0, "requirements": ["R1", "R2"],
              "reference": reference, "validate_input": validate_input,
              "origin": {"response_id": "response-1", "stage_index": 0, "round": 1},
              "namespace": "calendar-r0"}
    config.update(overrides)
    return parse_proposals(json.dumps({"probes": probes}), **config)


class VerificationProbeTests(unittest.TestCase):
    def test_valid_provenance_determinism_and_typed_expectations(self):
        probes = [proposal(2), proposal(3, {"answer": 6.0}),
                  proposal(1, {"answer": True}), proposal(4, {"answer": 9})]
        result = admit(probes)
        self.assertEqual(result, admit(probes))
        self.assertTrue(result["oracle_assisted"])
        self.assertEqual(result["proposed_count"], 4)
        self.assertEqual([item["reason"] for item in result["rejected"]],
                         ["wrong_expectation"] * 3)
        case = result["admitted_cases"][0]
        self.assertEqual(case["input"], {"n": 2})
        self.assertEqual(case["expected"], {"answer": 4})
        self.assertTrue(case["id"].startswith("calendar-r0:"))
        self.assertEqual(case["origin"]["response_id"], "response-1")
        self.assertEqual(len(case["validation_sha256"]), 64)
        self.assertNotIn("expected", result["rejected"][0])

    def test_batch_strictness_and_atomic_rejection(self):
        bad = [None, "[]", '{"probes":[],"extra":1}', '{"probes":{},"x":1}',
               '{"probes":[],"probes":[]}', '{"probes":[NaN]}',
               '{"probes":[{"input":{"n":1,"n":2}}]}',
               "{" * 1200, "x" * 65537, "\ud800",
               json.dumps({"probes": [proposal(i) for i in range(5)]})]
        for content in bad:
            with self.subTest(content=str(content)[:50]):
                result = parse_proposals(content, stage_index=0, requirements=["R1"],
                                         reference=reference, validate_input=validate_input,
                                         origin={"response_id": "x"})
                self.assertEqual(result["admitted_cases"], [])
                self.assertEqual(len(result["rejected"]), 1)
        self.assertEqual(admit([])["rejected"], [])

    def test_schema_and_domain_errors_do_not_discard_good_sibling(self):
        result = admit([{**proposal(1), "code": "raise Exception()"},
                        proposal(1, requirement="UNKNOWN"), proposal(-1), proposal(2)])
        self.assertEqual([item["reason"] for item in result["rejected"]],
                         ["case_schema", "unsupported_requirement", "oracle_rejected_input"])
        self.assertEqual(len(result["admitted_cases"]), 1)

    def test_bounds_reject_nested_arrays_strings_objects_and_numbers(self):
        deep = 0
        for _ in range(12):
            deep = [deep]
        values = [deep, [0] * 257, "s" * 4097, 2**64, 1e200,
                  {str(i): 0 for i in range(129)}, "\ud800", ["s" * 4096] * 5]
        for value in values:
            with self.subTest(value=str(value)[:40]):
                result = admit([{"requirement": "R1", "input": value, "expected": 0}])
                self.assertEqual(result["admitted_cases"], [])
                self.assertEqual(result["rejected"][0]["reason"], "case_exceeds_json_limits")

    def test_dedup_inputs_across_labels_rounds_and_trusted_public_cases(self):
        first = admit([proposal(1), proposal(1, requirement="R2")])
        self.assertEqual(first["rejected"][0]["reason"], "duplicate_input")
        prior = first["admitted_cases"] + [{"input": {"n": 2}, "expected": {"answer": 4}}]
        later = admit([proposal(1), proposal(2), proposal(3)], existing_cases=prior)
        self.assertEqual([item["reason"] for item in later["rejected"]],
                         ["duplicate_input", "duplicate_input"])
        self.assertEqual(later["admitted_cases"][0]["input"], {"n": 3})

    def test_canonical_dedup_preserves_exact_input_number_types(self):
        def identity(stage, value):
            return value
        probes = [{"requirement": "R1", "input": value, "expected": value}
                  for value in ({"a": 1, "b": 2}, {"b": 2, "a": 1}, 1, 1.0)]
        result = admit(probes, reference=identity, validate_input=lambda stage, value: None)
        self.assertEqual(len(result["admitted_cases"]), 3)
        self.assertEqual(result["rejected"][0]["reason"], "duplicate_input")

    def test_rejected_expectation_does_not_prevent_correct_later_proposal(self):
        result = admit([proposal(2, {"answer": 9}), proposal(2)])
        self.assertEqual(len(result["admitted_cases"]), 1)
        self.assertEqual(result["rejected"][0]["reason"], "wrong_expectation")

    def test_supplied_stage_capacity_counts_admissions_not_bad_proposals(self):
        calls = []
        def spy(stage, payload):
            calls.append(payload)
            return reference(stage, payload)
        result = admit([proposal(1, {"answer": 99}), proposal(2), proposal(3), proposal(4)],
                       max_new=1, reference=spy)
        self.assertEqual(len(result["admitted_cases"]), 1)
        self.assertEqual([item["reason"] for item in result["rejected"]],
                         ["wrong_expectation", "stage_cap", "stage_cap"])
        self.assertEqual(len(calls), 2)
        self.assertEqual(admit([proposal(1)], max_new=0)["rejected"][0]["reason"], "stage_cap")

    def test_callbacks_get_independent_copies_and_origin_is_not_aliased(self):
        origin = {"response_id": "a", "nested": [1]}
        def mutating_validation(stage, value):
            value["n"] = 999
        def mutating_reference(stage, value):
            result = value["n"] * 2
            value["n"] = 888
            return {"answer": result}
        result = admit([proposal(2)], origin=origin, validate_input=mutating_validation,
                       reference=mutating_reference)
        origin["nested"].append(2)
        self.assertEqual(result["admitted_cases"][0]["input"], {"n": 2})
        self.assertEqual(result["admitted_cases"][0]["origin"]["nested"], [1])

    def test_invalid_host_configuration_and_unexpected_oracle_errors_fail_closed(self):
        for overrides in ({"stage_index": True}, {"stage_index": 4}, {"max_new": 9},
                          {"max_new": True}, {"origin": {}}, {"requirements": []},
                          {"origin": {"stage_index": 1}}, {"namespace": "../private"}):
            with self.subTest(overrides=overrides), self.assertRaises(ValueError):
                admit([], **overrides)
        def broken(stage, payload):
            raise RuntimeError("reference bug")
        with self.assertRaisesRegex(RuntimeError, "reference bug"):
            admit([proposal(1)], reference=broken)
        with self.assertRaisesRegex(RuntimeError, "unbounded JSON"):
            admit([proposal(1)], reference=lambda stage, payload: object())

    def test_reference_value_error_is_not_a_domain_rejection_or_retirement(self):
        def broken_reference(stage, payload):
            raise ValueError("reference bug after valid input")
        with self.assertRaisesRegex(ValueError, "reference bug"):
            admit([proposal(1)], reference=broken_reference)
        cases = admit([proposal(1)])["admitted_cases"]
        with self.assertRaisesRegex(ValueError, "reference bug"):
            revalidate_cases(cases, stage_index=1, reference=broken_reference,
                             validate_input=validate_input)

    def test_revalidation_keeps_ids_and_origin_without_mutating_prior_pool(self):
        cases = admit([proposal(0), proposal(2)])["admitted_cases"]
        original = deepcopy(cases)
        result = revalidate_cases(cases, stage_index=1, reference=reference,
                                  validate_input=validate_input, origin={"stage_index": 1})
        self.assertEqual(cases, original)
        self.assertEqual(result["retired"], [])
        for before, after in zip(cases, result["admitted_cases"]):
            self.assertEqual(before["id"], after["id"])
            self.assertEqual(before["origin"], after["origin"])
            self.assertEqual(after["validated_stage_index"], 1)
            self.assertNotEqual(before["validation_sha256"], after["validation_sha256"])
        # The revised binding remains valid for the next stage.
        again = revalidate_cases(result["admitted_cases"], stage_index=2, reference=reference,
                                 validate_input=validate_input)
        self.assertEqual(len(again["admitted_cases"]), 1)
        self.assertEqual(len(again["retired"]), 1)

    def test_contract_changes_and_domain_tightening_retire_without_correction(self):
        cases = admit([proposal(0), proposal(2), proposal(8)])["admitted_cases"]
        result = revalidate_cases(cases, stage_index=3, reference=reference,
                                  validate_input=validate_input)
        self.assertEqual([item["input"] for item in result["admitted_cases"]], [{"n": 0}])
        self.assertEqual([item["reason"] for item in result["retired"]],
                         ["contract_changed", "contract_changed"])
        for item in result["retired"]:
            self.assertNotIn("expected", item)
            self.assertNotIn("actual", item)
        self.assertEqual(cases[1]["expected"], {"answer": 4})

    def test_tampering_duplicate_and_backward_revalidation_rejected_before_oracle(self):
        cases = admit([proposal(1)])["admitted_cases"]
        for key, value in (("input", {"n": 3}), ("expected", {"answer": 8}),
                           ("origin", {"response_id": "forged"}), ("id", "forged"),
                           ("requirement", "R2")):
            bad = deepcopy(cases)
            bad[0][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                revalidate_cases(bad, stage_index=1, reference=reference,
                                 validate_input=validate_input)
            with self.assertRaises(ValueError):
                admit([proposal(2)], existing_cases=bad)
        with self.assertRaises(ValueError):
            revalidate_cases(cases + cases, stage_index=1, reference=reference,
                             validate_input=validate_input)
        advanced = revalidate_cases(cases, stage_index=1, reference=reference,
                                    validate_input=validate_input)["admitted_cases"]
        with self.assertRaises(ValueError):
            revalidate_cases(advanced, stage_index=0, reference=reference,
                             validate_input=validate_input)


if __name__ == "__main__":
    unittest.main()
