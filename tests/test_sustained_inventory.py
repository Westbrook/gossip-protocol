"""Execute trusted frozen fixtures on host; model code is never imported here."""
import importlib.util
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from gossip_harness.sustained_inventory import INITIAL, MUTANTS, PROJECT, STAGES


class InventoryFixturesTest(unittest.TestCase):
    def run_files(self, files, cases):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, content in files.items():
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content)
            spec = importlib.util.spec_from_file_location("trusted_inventory_fixture", root / "solution.py")
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return [module.solve(case["input"]) for case in cases]

    def test_interface_and_fixed_boundaries(self):
        self.assertEqual(PROJECT["id"], "inventory")
        self.assertEqual(len(PROJECT["allowed_paths"]), 3)
        self.assertEqual(len(STAGES), 3)
        all_ids = set()
        all_inputs = set()
        for stage in STAGES:
            self.assertEqual(len(stage["visible_cases"]), 8)
            self.assertEqual(len(stage["hidden_cases"]), 10)
            self.assertEqual(stage["known_files"]["solution.py"], INITIAL["solution.py"])
            self.assertEqual(stage["known_files"]["inventory_app/cli.py"], INITIAL["inventory_app/cli.py"])
            for case in stage["visible_cases"] + stage["hidden_cases"]:
                self.assertNotIn(case["id"], all_ids)
                all_ids.add(case["id"])
                encoded = json.dumps(case["input"], sort_keys=True)
                self.assertNotIn(encoded, all_inputs)
                all_inputs.add(encoded)
                self.assertLessEqual(len(case["input"]["commands"]), 12)
                self.assertIn(case["requirement"], stage["requirements"])
                self.assertEqual(len(case["input"]["commands"]), len(case["expected"]))

    def test_reference_implementations_preserve_all_previous_stages(self):
        cumulative = []
        for stage in STAGES:
            cumulative.extend(stage["visible_cases"] + stage["hidden_cases"])
            actual = self.run_files(stage["known_files"], cumulative)
            for case, answer in zip(cumulative, actual):
                with self.subTest(stage=stage["id"], case=case["id"]):
                    self.assertEqual(answer, case["expected"])

    def test_v0_is_a_working_basic_repository(self):
        cases = [STAGES[0]["visible_cases"][0]]
        self.assertEqual(self.run_files(INITIAL, cases), [case["expected"] for case in cases])
        advanced = [STAGES[0]["visible_cases"][1]]
        self.assertNotEqual(self.run_files(INITIAL, advanced), [case["expected"] for case in advanced])

    def test_realistic_mutants_pass_simple_but_fail_advanced(self):
        basic = STAGES[0]["visible_cases"][:3]
        advanced = STAGES[1]["visible_cases"] + STAGES[1]["hidden_cases"] + STAGES[2]["visible_cases"] + STAGES[2]["hidden_cases"]
        for name, files in MUTANTS.items():
            with self.subTest(mutant=name):
                self.assertEqual(self.run_files(files, basic), [case["expected"] for case in basic])
                answers = self.run_files(files, advanced)
                failures = [case["id"] for case, answer in zip(advanced, answers) if answer != case["expected"]]
                self.assertTrue(failures)
                self.assertTrue(any("-h-" in case_id for case_id in failures))

    def test_expected_state_invariants(self):
        for stage in STAGES:
            for case in stage["visible_cases"] + stage["hidden_cases"]:
                for answer in case["expected"]:
                    if type(answer) is not dict or "stock" not in answer:
                        continue
                    self.assertEqual(answer["stock"], sorted(answer["stock"], key=lambda row: row["sku"]))
                    self.assertEqual(answer["reservations"], sorted(answer["reservations"], key=lambda row: row["rid"]))
                    for stock in answer["stock"]:
                        held = sum(row["remaining"] for row in answer["reservations"] if row["sku"] == stock["sku"] and row["status"] == "active")
                        self.assertEqual(stock["reserved"], held)
                        self.assertGreaterEqual(stock["on_hand"], held)
                        self.assertEqual(stock["available"], stock["on_hand"] - held)
                    for row in answer["reservations"]:
                        self.assertGreater(row["remaining"], 0) if row["status"] == "active" else self.assertEqual(row["remaining"], 0)


if __name__ == "__main__":
    unittest.main()
