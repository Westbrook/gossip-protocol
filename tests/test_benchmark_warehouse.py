"""Warehouse trusted-model contract tests; app execution belongs to qualification."""
import ast
from copy import deepcopy
import json
import unittest

from gossip_harness import benchmark_warehouse as fixture


class BenchmarkWarehouseTests(unittest.TestCase):
    def run_commands(self, *commands, stage=1):
        return fixture.reference(stage, {"commands":list(commands)})

    def test_fixture_has_real_baseline_two_milestones_and_scoped_sources(self):
        self.assertEqual(fixture.PROJECT["id"], "warehouse")
        self.assertEqual(len(fixture.STAGES), 2)
        self.assertEqual(len(fixture.ALLOWED), 3)
        self.assertNotIn('command["op"] != "allocate"', fixture.INITIAL[fixture.ALLOWED[2]])
        self.assertNotIn('if op == "ship":', fixture.known_files(0)[fixture.ALLOWED[2]])
        self.assertIn('if op == "ship":', fixture.known_files(1)[fixture.ALLOWED[2]])
        for stage in (0,1):
            files = fixture.known_files(stage)
            self.assertEqual(set(files), set(fixture.INITIAL))
            changed = {path for path in files if files[path] != fixture.INITIAL[path]}
            self.assertTrue(changed <= set(fixture.ALLOWED))
            for path,source in files.items():
                ast.parse(source, path)

    def test_case_ids_groups_and_public_private_inputs_are_disjoint(self):
        cases = [case for stage in fixture.STAGES for kind in ("visible_cases","hidden_cases")
                 for case in stage[kind]]
        self.assertEqual(len(cases),len({case["id"] for case in cases}))
        requirements = set()
        for stage in fixture.STAGES:
            requirements.update(stage["requirements"])
            self.assertTrue(all(case["requirement"] in requirements
                                for kind in ("visible_cases","hidden_cases") for case in stage[kind]))
        identities = [{json.dumps(case["input"],sort_keys=True) for stage in fixture.STAGES
                       for case in stage[kind]} for kind in ("visible_cases","hidden_cases")]
        self.assertFalse(identities[0] & identities[1])

    def test_hand_calculated_baseline_conservation_and_release(self):
        answers = self.run_commands(fixture.receive("lot",qty=5),fixture.hold(),fixture.STOCK,
                                    fixture.release(),fixture.STOCK,fixture.ORDERS,fixture.AUDIT)
        self.assertEqual(answers[1],{"ok":"held","order":"order","allocations":[{"lot":"lot","qty":2}]})
        self.assertEqual(answers[2],[dict(lot="lot",sku="a",expires=10,available=3,reserved=2,shipped=0)])
        self.assertEqual(answers[4],[dict(lot="lot",sku="a",expires=10,available=5,reserved=0,shipped=0)])
        self.assertEqual(answers[5],[dict(order="order",status="released",allocations=[])])
        self.assertEqual([row["seq"] for row in answers[6]],[1,2,3])

    def test_manual_expired_hold_remains_supported(self):
        answers = self.run_commands(fixture.receive("lot",expires=0),fixture.hold(),
                                    fixture.allocate(order="new",now=0),fixture.STOCK)
        self.assertEqual(answers[1]["ok"],"held")
        self.assertEqual(answers[2],{"error":"insufficient"})
        self.assertEqual(answers[3][0]["reserved"],2)

    def test_fefo_uses_expiry_then_name_and_splits_lots(self):
        answers = self.run_commands(fixture.receive("z",qty=2,expires=5),
                                    fixture.receive("a",qty=3,expires=5),
                                    fixture.receive("early",qty=1,expires=3),
                                    fixture.allocate(lines=(("a",5),)),fixture.STOCK)
        self.assertEqual(answers[3]["allocations"],[dict(lot="a",qty=3),dict(lot="early",qty=1),dict(lot="z",qty=1)])
        self.assertEqual([row["available"] for row in answers[4]],[0,0,1])

    def test_expiry_equality_excluded_and_sku_not_substituted(self):
        answers = self.run_commands(fixture.receive("lot",expires=3),fixture.receive("other",sku="b"),
                                    fixture.allocate(now=3),fixture.STOCK,fixture.ORDERS)
        self.assertEqual(answers[2],{"error":"insufficient"})
        self.assertEqual(answers[-1],[])
        self.assertEqual([row["available"] for row in answers[-2]],[5,5])

    def test_multi_line_shortfall_rolls_back_and_preserves_key(self):
        failed = fixture.allocate(lines=(("a",1),("b",1)))
        answers = self.run_commands(fixture.receive("lot"),failed,fixture.STOCK,fixture.ORDERS,
                                    dict(op="receipt",key="allocate"),fixture.allocate(),fixture.AUDIT)
        self.assertEqual(answers[1],{"error":"insufficient"})
        self.assertEqual(answers[2][0]["available"],5)
        self.assertEqual(answers[3],[])
        self.assertEqual(answers[4],{"found":False})
        self.assertEqual(answers[5]["ok"],"allocated")
        self.assertEqual([row["seq"] for row in answers[6]],[1,2])

    def test_reservations_from_other_orders_are_unavailable(self):
        answers = self.run_commands(fixture.receive("lot",qty=3),fixture.hold(),fixture.allocate(order="other"),
                                    fixture.STOCK,fixture.ORDERS)
        self.assertEqual(answers[2],{"error":"insufficient"})
        self.assertEqual(answers[3][0]["reserved"],2)
        self.assertEqual(len(answers[4]),1)

    def test_line_order_normalization_and_original_response_replay(self):
        first = fixture.allocate(lines=(("b",1),("a",2)))
        answers = self.run_commands(fixture.receive("a"),fixture.receive("b",sku="b"),first,
                                    fixture.release(),fixture.allocate(lines=(("a",2),("b",1))),
                                    fixture.ORDERS,fixture.AUDIT)
        self.assertEqual(answers[2],answers[4])
        self.assertEqual(answers[5][0]["allocations"],[])
        self.assertEqual(len(answers[6]),4)

    def test_validation_precedes_receipt_and_state_with_exact_scalar_types(self):
        invalid = [None,[],{"op":[]},{"op":{}},dict(fixture.receive("x"),qty=True),
                   dict(fixture.allocate(),now=0.0),fixture.allocate(lines=(("a",1),("a",2))),
                   dict(fixture.returned(),lines=[{"lot":"a","qty":1,"extra":0}])]
        answers = self.run_commands(fixture.receive("lot"),fixture.allocate(),
                                    fixture.allocate(lines=(("a",False),)),*invalid,fixture.AUDIT)
        self.assertEqual(answers[2:-1],[{"error":"invalid"}]*(1+len(invalid)))
        self.assertEqual(len(answers[-1]),2)

    def test_manual_ids_and_shared_receipts_remain_historical(self):
        answers = self.run_commands(fixture.receive("lot"),fixture.hold(),fixture.release(),
                                    fixture.hold(key="new"),fixture.allocate(key="receive-lot"),
                                    dict(op="receipt",key="hold"),fixture.hold(),fixture.AUDIT)
        self.assertEqual(answers[3:5],[{"error":"exists"},{"error":"key_conflict"}])
        self.assertEqual(answers[5]["response"],answers[1])
        self.assertEqual(answers[6],answers[1])
        self.assertEqual(len(answers[7]),3)

    def test_unicode_scalar_names_accept_non_ascii_but_reject_surrogates(self):
        answers = self.run_commands(fixture.receive("é"),fixture.receive("\U0001f4e6"),
                                    fixture.receive("\ud800"),fixture.receive("\udfff"),fixture.STOCK)
        self.assertEqual(answers[2:4],[{"error":"invalid"},{"error":"invalid"}])
        self.assertEqual([row["lot"] for row in answers[4]],["é","\U0001f4e6"])

    def test_partial_shipments_consume_reserved_fefo_with_exact_provenance(self):
        answers = self.run_commands(fixture.receive("a",qty=3,expires=8),fixture.receive("z",qty=3,expires=2),
                                    fixture.allocate(lines=(("a",5),)),fixture.ship(lines=(("a",4),)),
                                    fixture.STOCK,fixture.ORDERS,fixture.SHIPMENTS)
        self.assertEqual(answers[3]["allocations"],[dict(lot="a",qty=1),dict(lot="z",qty=3)])
        self.assertEqual(answers[5],[dict(order="order",status="reserved",allocations=[dict(lot="a",qty=1)])])
        self.assertEqual(answers[6],[dict(shipment="shipment",order="order",allocations=[
            dict(lot="a",qty=1,returned=0),dict(lot="z",qty=3,returned=0)])])
        for row in answers[4]:
            self.assertEqual(row["available"]+row["reserved"]+row["shipped"],3)

    def test_partial_release_does_not_restore_shipped_stock(self):
        answers = self.run_commands(fixture.receive("lot"),fixture.allocate(lines=(("a",4),)),
                                    fixture.ship(),fixture.release(),fixture.STOCK,fixture.SHIPMENTS,
                                    fixture.returned(),fixture.STOCK,fixture.ORDERS)
        self.assertEqual(answers[4][0],dict(lot="lot",sku="a",expires=10,available=4,reserved=0,shipped=1))
        self.assertEqual(answers[5][0]["allocations"],[dict(lot="lot",qty=1,returned=0)])
        self.assertEqual(answers[7][0]["available"],5)
        self.assertEqual(answers[8][0]["status"],"released")

    def test_complete_ship_and_return_do_not_reopen_order(self):
        answers = self.run_commands(fixture.receive("lot"),fixture.allocate(lines=(("a",1),)),fixture.ship(),
                                    fixture.returned(),fixture.ship(shipment="new",key="new"),fixture.ORDERS)
        self.assertEqual(answers[4],{"error":"closed"})
        self.assertEqual(answers[5],[dict(order="order",status="shipped",allocations=[])])

    def test_shipping_never_consumes_another_orders_reservation(self):
        answers = self.run_commands(fixture.receive("lot"),
                                    fixture.allocate(order="a",lines=(("a",1),),key="a"),
                                    fixture.allocate(order="b",lines=(("a",2),),key="b"),
                                    fixture.ship(order="a",lines=(("a",2),),key="send"),
                                    fixture.STOCK,fixture.SHIPMENTS,
                                    fixture.ship(order="b",lines=(("a",2),),key="send"),
                                    fixture.ship(shipment="a",order="a",key="send-a"),
                                    fixture.STOCK,fixture.ORDERS,fixture.AUDIT)
        self.assertEqual(answers[3],{"error":"insufficient"})
        self.assertEqual(answers[4][0],dict(lot="lot",sku="a",expires=10,available=2,reserved=3,shipped=0))
        self.assertEqual(answers[5],[])
        self.assertEqual(answers[8][0],dict(lot="lot",sku="a",expires=10,available=2,reserved=0,shipped=3))
        self.assertEqual([row["status"] for row in answers[9]],["shipped","shipped"])
        self.assertEqual(len(answers[10]),5)

    def test_ship_and_return_late_failures_roll_back_every_line(self):
        answers = self.run_commands(fixture.receive("a"),fixture.receive("b",sku="b"),
                                    fixture.allocate(lines=(("a",2),("b",1))),
                                    fixture.ship(lines=(("a",1),("b",2))),fixture.SHIPMENTS,fixture.STOCK,
                                    fixture.ship(lines=(("a",1),("b",1))),
                                    fixture.returned(lines=(("a",1),("b",2))),fixture.SHIPMENTS,fixture.STOCK)
        self.assertEqual(answers[3],{"error":"insufficient"})
        self.assertEqual(answers[4],[])
        self.assertEqual([row["shipped"] for row in answers[5]],[0,0])
        self.assertEqual(answers[7],{"error":"excess"})
        self.assertEqual([row["returned"] for row in answers[8][0]["allocations"]],[0,0])
        self.assertEqual([row["shipped"] for row in answers[9]],[1,1])

    def test_cumulative_returns_reject_excess_and_foreign_lot(self):
        answers = self.run_commands(fixture.receive("lot"),fixture.receive("other"),fixture.allocate(),
                                    fixture.ship(lines=(("a",2),)),fixture.returned(),
                                    fixture.returned(key="second"),fixture.returned(key="third"),
                                    fixture.returned(lines=(("other",1),),key="foreign"),fixture.STOCK,fixture.SHIPMENTS)
        self.assertEqual(answers[6:8],[{"error":"excess"},{"error":"missing"}])
        self.assertEqual(answers[8][0]["available"],5)
        self.assertEqual(answers[9][0]["allocations"],[dict(lot="lot",qty=2,returned=2)])

    def test_return_restores_original_expired_lot_but_allocate_excludes_it(self):
        answers = self.run_commands(fixture.receive("lot",expires=1),fixture.hold(),fixture.ship(),fixture.returned(),
                                    fixture.allocate(order="new",now=1),fixture.STOCK)
        self.assertEqual(answers[4],{"error":"insufficient"})
        self.assertEqual(answers[5][0],dict(lot="lot",sku="a",expires=1,available=4,reserved=1,shipped=0))

    def test_ship_and_return_replay_after_changed_state(self):
        answers = self.run_commands(fixture.receive("lot"),fixture.allocate(),fixture.ship(lines=(("a",2),)),
                                    fixture.returned(),fixture.returned(key="second"),fixture.returned(),
                                    fixture.ship(lines=(("a",2),)),fixture.STOCK,fixture.AUDIT)
        self.assertEqual(answers[2],answers[6])
        self.assertEqual(answers[3],answers[5])
        self.assertEqual(answers[7][0]["available"],5)
        self.assertEqual(len(answers[8]),5)

    def test_mutants_are_distinct_scoped_syntax_valid_private_witnessed(self):
        for stage in (0,1):
            known = fixture.known_files(stage)
            faults = fixture.fault_bank(stage)
            self.assertGreaterEqual(len(faults),6)
            self.assertEqual(len({row["family"] for row in faults}),len(faults))
            self.assertEqual(len({json.dumps(row["files"],sort_keys=True) for row in faults}),len(faults))
            private = [case for item in fixture.STAGES[:stage+1] for case in item["hidden_cases"]]
            for row in faults:
                self.assertEqual(set(row),{"id","family","files","witness_cases"})
                changed = {path for path in known if known[path] != row["files"][path]}
                self.assertTrue(changed)
                self.assertTrue(changed <= set(fixture.ALLOWED))
                for path,source in row["files"].items():
                    ast.parse(source,path)
                for witness in row["witness_cases"]:
                    self.assertIn(witness,private)
                    self.assertEqual(fixture.reference(stage,witness["input"]),witness["expected"])

    def test_controls_are_distinct_bounded_and_keep_fixed_adapter(self):
        for stage in (0,1):
            controls = fixture.correct_controls(stage)
            self.assertEqual(len(controls),2)
            self.assertNotEqual(controls[0]["files"],controls[1]["files"])
            for row in controls:
                self.assertEqual(row["files"]["solution.py"],fixture.INITIAL["solution.py"])
                for path,source in row["files"].items():
                    ast.parse(source,path)

    def test_rehearsal_inputs_are_unique_nonmutating_and_stage_bounded(self):
        for stage in (0,1):
            probes = [fixture.rehearsal_probe(stage,slot) for slot in range(10)]
            self.assertEqual(len({json.dumps(probe,sort_keys=True) for probe in probes}),10)
            for probe in probes:
                before = deepcopy(probe)
                answers = fixture.reference(stage,probe)
                self.assertEqual(probe,before)
                self.assertTrue(all(not (type(answer) is dict and "error" in answer) for answer in answers))
        with self.assertRaises(ValueError):
            fixture.validate_input(0,{"commands":[fixture.ship()]})
        for stage in (True,-1,2):
            with self.assertRaises(ValueError):
                fixture.known_files(stage)
        for payload in ({"commands":[]},{"commands":[],"extra":0},{"commands":[float("nan")]},[]):
            with self.assertRaises(ValueError):
                fixture.validate_input(0,payload)
        nested = []
        for _ in range(20):
            nested = [nested]
        for command in (dict(fixture.hold(),lot=nested),{1:"non-json-key"},(1,2)):
            with self.assertRaises(ValueError):
                fixture.validate_input(0,{"commands":[command]})

    def test_inherited_cases_have_identical_stage_two_answers(self):
        for case in fixture.STAGES[0]["visible_cases"]+fixture.STAGES[0]["hidden_cases"]:
            self.assertEqual(fixture.reference(1,case["input"]),case["expected"])


if __name__ == "__main__":
    unittest.main()
