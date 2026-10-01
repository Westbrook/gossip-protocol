"""Host-side qualification of author-written fixtures, never model candidates."""
from copy import deepcopy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from gossip_harness.blackbox_validator import json_equal
from gossip_harness import benchmark_calendar_exchange as fixture
from gossip_harness import verification_calendar as predecessor


class TrustedCalendar:
    def __init__(self, files):
        self.temp = tempfile.TemporaryDirectory(prefix="trusted-calendar-exchange-")
        self.root = Path(self.temp.name)
        for name, content in files.items():
            target = self.root / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content)

    def solve_many(self, payloads):
        script = "import sys,json; sys.path.insert(0,sys.argv[1]); from solution import solve; print(json.dumps([solve(p) for p in json.load(sys.stdin)],allow_nan=False))"
        result = subprocess.run([sys.executable, "-I", "-c", script, str(self.root)],
                                input=json.dumps(payloads), capture_output=True,
                                text=True, timeout=90)
        if result.returncode:
            raise AssertionError(result.stderr)
        return json.loads(result.stdout)

    def close(self):
        self.temp.cleanup()


class BenchmarkCalendarExchangeTests(unittest.TestCase):
    def test_mature_baseline_fixed_scope_and_versioned_stages(self):
        self.assertEqual(fixture.INITIAL, predecessor.STAGES[3]["known_files"])
        self.assertEqual(fixture.PROJECT["allowed_paths"], predecessor.ALLOWED)
        self.assertEqual(len(fixture.PROJECT["stages"]), 2)
        self.assertEqual(fixture.VERSION, "calendar-exchange-v1")
        for index in range(2):
            files = fixture.known_files(index)
            self.assertEqual(set(files), set(fixture.INITIAL))
            for name in set(files) - set(fixture.ALLOWED):
                self.assertEqual(files[name], fixture.INITIAL[name])
            for name, text in files.items():
                if name.endswith(".py"):
                    compile(text, name, "exec")

    def test_golden_matches_independent_oracle_all_cumulative_scenarios(self):
        cumulative = []
        for index, stage in enumerate(fixture.STAGES):
            cumulative += list(stage["visible_cases"]) + list(stage["hidden_cases"])
            trusted = TrustedCalendar(stage["known_files"])
            try:
                answers = trusted.solve_many([case["input"] for case in cumulative])
                for case, answer in zip(cumulative, answers, strict=True):
                    with self.subTest(stage=index, case=case["id"]):
                        self.assertTrue(json_equal(fixture.reference(index, case["input"]), case["expected"]))
                        self.assertTrue(json_equal(answer, case["expected"]), (case["id"], answer, case["expected"]))
            finally:
                trusted.close()

    def test_public_private_ids_requirements_and_immutable_reference(self):
        ids = set()
        public = set()
        private = set()
        for index, stage in enumerate(fixture.STAGES):
            self.assertEqual(set(stage["requirements"]), {case["requirement"] for case in stage["visible_cases"]})
            self.assertEqual(set(stage["requirements"]), {case["requirement"] for case in stage["hidden_cases"]})
            for group, sink in (("visible_cases", public), ("hidden_cases", private)):
                for case in stage[group]:
                    self.assertNotIn(case["id"], ids)
                    ids.add(case["id"])
                    sink.add(json.dumps(case["input"], sort_keys=True))
                    before = deepcopy(case["input"])
                    fixture.validate_input(index, case["input"])
                    self.assertTrue(json_equal(fixture.reference(index, before), case["expected"]))
                    self.assertEqual(before, case["input"])
        self.assertFalse(public & private)
        self.assertEqual(len(ids), 119)

    def test_final_state_swaps_and_policy_precedence_are_manual(self):
        scenario = fixture.scenario([
            fixture.book("a", start=0, end=3), fixture.book("b", rid="b", start=3, end=6),
            fixture.exchange("x", fixture.move("a", 3, 6), fixture.move("b", 0, 3)), fixture.SNAP,
        ])
        result = fixture.reference(0, scenario)
        self.assertEqual(result[2], {"ok": True})
        self.assertEqual([(b["rid"], b["start"], b["end"]) for b in result[-1]["bookings"]], [("a", 3, 6), ("b", 0, 3)])
        case = next(c for c in fixture.STAGES[0]["hidden_cases"] if c["id"].endswith("policy-before-conflict"))
        self.assertEqual(case["expected"][3], {"error": "policy"})

    def test_settlement_retries_are_frozen_and_failed_keys_are_reusable(self):
        cases = {c["id"].split("-h-")[-1]: c for c in fixture.STAGES[1]["hidden_cases"]}
        replay = cases["retry-after-enrollment"]["expected"]
        self.assertEqual(replay[1], {"ok": True, "admitted": ["q"]})
        self.assertEqual(replay[4], replay[1])
        self.assertEqual(replay[5], {"found": True, "op": "settle", "response": replay[1]})
        self.assertEqual(replay[6]["waiting"][-1]["status"], "waiting")
        rollback = cases["late-missing-cancel"]["expected"]
        self.assertEqual(rollback[2], {"error": "missing"})
        self.assertEqual(rollback[3]["bookings"][0]["status"], "booked")
        self.assertEqual(rollback[4], {"found": False})
        self.assertEqual(rollback[6], {"ok": True, "admitted": ["q"]})

    def test_boundaries_and_future_api_isolation(self):
        for index in (False, -1, 2):
            with self.assertRaisesRegex(ValueError, "outside_input_domain"):
                fixture.validate_input(index, {"commands": []})
        for payload in ({}, {"commands": [fixture.SNAP] * 25}, {"commands": [{"op": "x", "v": float("nan")}]}, {"commands": [], "other": 1}):
            with self.assertRaisesRegex(ValueError, "outside_input_domain"):
                fixture.validate_input(1, payload)
        for op in (fixture.settle(), fixture.receipt("x")):
            with self.assertRaisesRegex(ValueError, "outside_input_domain"):
                fixture.validate_input(0, fixture.scenario([op]))
            fixture.validate_input(1, fixture.scenario([op]))
        malformed = fixture.scenario([None, [], {"op": []}, {"op": {}}, fixture.command("exchange", "x", moves=False)])
        self.assertEqual(fixture.reference(0, malformed), [{"error": "invalid"}] * 5)

    def test_rehearsal_probes_distinct_and_outside_frozen_cases(self):
        frozen = {json.dumps(c["input"], sort_keys=True) for s in fixture.STAGES for group in ("visible_cases", "hidden_cases") for c in s[group]}
        for index in range(2):
            probes = [fixture.rehearsal_probe(index, n) for n in range(4)]
            self.assertEqual(len({json.dumps(p, sort_keys=True) for p in probes}), 4)
            for probe in probes:
                self.assertNotIn(json.dumps(probe, sort_keys=True), frozen)
                self.assertTrue(fixture.reference(index, probe))

    def test_fault_bank_is_versioned_scoped_and_witnesses_are_private(self):
        for index, count in ((0, 6), (1, 8)):
            faults = fixture.fault_bank(index)
            self.assertEqual(len(faults), count)
            self.assertEqual(len({f["id"] for f in faults}), count)
            self.assertEqual(len({f["family"] for f in faults}), count)
            public_ids = {c["id"] for stage in fixture.STAGES[:index + 1] for c in stage["visible_cases"]}
            for fault in faults:
                changed = {name for name in fault["files"] if fault["files"][name] != fixture.known_files(index)[name]}
                self.assertEqual(changed, {"calendar_app/service.py"})
                compile(fault["files"]["calendar_app/service.py"], fault["id"], "exec")
                self.assertTrue(fault["witness_cases"])
                for case in fault["witness_cases"]:
                    self.assertNotIn(case["id"], public_ids)
                    self.assertTrue(json_equal(fixture.reference(index, case["input"]), case["expected"]))

    def test_controls_are_nontrivial_equivalent_files(self):
        for index in range(2):
            controls = fixture.correct_controls(index)
            self.assertEqual([c["id"] for c in controls], ["correct", "equivalent-whitespace"])
            self.assertEqual(controls[0]["files"], fixture.known_files(index))
            self.assertNotEqual(controls[0]["files"], controls[1]["files"])
            for name in fixture.ALLOWED:
                self.assertEqual(controls[0]["files"][name], controls[1]["files"][name].lstrip("\n"))
            trusted = TrustedCalendar(controls[1]["files"])
            try:
                probe = fixture.rehearsal_probe(index)
                self.assertTrue(json_equal(trusted.solve_many([probe])[0], fixture.reference(index, probe)))
            finally:
                trusted.close()


if __name__ == "__main__":
    unittest.main()
