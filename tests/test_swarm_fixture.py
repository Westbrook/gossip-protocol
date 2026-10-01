"""Offline contract checks: only repository-owned solutions execute on the host."""
from copy import deepcopy
import json
import random
import unittest

from gossip_harness.swarm_fixture import TASKS, get_task, public_manifest, fixture_signature, _sample


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def trusted_function(source):
    # Only literal trusted oracle/mutant source from this repository reaches exec.
    namespace = {}
    exec(compile(source, "trusted_fixture_solution.py", "exec"), namespace)
    return namespace["solve"]


class SwarmFixtureTests(unittest.TestCase):
    def test_roster_and_disjoint_frozen_case_pools(self):
        self.assertEqual(len(TASKS), 8)
        self.assertEqual(len({task.id for task in TASKS}), 8)
        self.assertEqual({task.family for task in TASKS}, {"config", "interval", "ledger"})
        for task in TASKS:
            with self.subTest(task=task.id):
                self.assertEqual(get_task(task.id), task)
                self.assertTrue(3 <= len(task.public_cases) <= 5)
                self.assertGreaterEqual(len(task.hidden_cases), 15)
                self.assertGreaterEqual(len(task.fuzz_cases), 12)
                pools = [{canonical(c["input"]) for c in cases} for cases in
                         (task.public_cases, task.hidden_cases, task.fuzz_cases)]
                self.assertEqual(sum(map(len, pools)), sum(map(len, (task.public_cases, task.hidden_cases, task.fuzz_cases))))
                self.assertFalse(pools[0] & pools[1] or pools[0] & pools[2] or pools[1] & pools[2])
                for case in task.public_cases + task.hidden_cases + task.fuzz_cases:
                    self.assertEqual(set(case), {"input", "expected", "requirement"})
                    self.assertIn(case["requirement"], task.requirements)
                for requirement in task.requirements:
                    self.assertIn(requirement + ":", task.spec)
                self.assertEqual(set(task.initial_files), {"solution.py"})
                self.assertIn("NotImplementedError", task.initial_files["solution.py"])
        with self.assertRaises(ValueError):
            get_task("missing")

    def test_handcomputed_and_generated_cases_match_both_implementations(self):
        for task in TASKS:
            solve = trusted_function(task.known_solution)
            for index, case in enumerate(task.public_cases + task.hidden_cases + task.fuzz_cases):
                with self.subTest(task=task.id, case=index):
                    payload = deepcopy(case["input"])
                    before = canonical(payload)
                    self.assertEqual(canonical(task.oracle(payload)), canonical(case["expected"]))
                    self.assertEqual(canonical(solve(payload)), canonical(case["expected"]))
                    self.assertEqual(canonical(payload), before)
                    # A repeated unrelated call cannot contaminate the same result.
                    solve(deepcopy(task.public_cases[0]["input"]))
                    self.assertEqual(canonical(solve(deepcopy(case["input"]))), canonical(case["expected"]))

    def test_all_meaningful_mutants_rejected_by_final_holdout(self):
        for task in TASKS:
            self.assertGreaterEqual(len(task.mutants), 2)
            public_survivors = 0
            for label, source in task.mutants.items():
                with self.subTest(task=task.id, mutant=label):
                    self.assertNotEqual(source, task.known_solution)
                    solve = trusted_function(source)
                    if all(canonical(solve(deepcopy(case["input"]))) == canonical(case["expected"]) for case in task.public_cases):
                        public_survivors += 1
                    failures = []
                    for index, case in enumerate(task.hidden_cases):
                        try:
                            correct = canonical(solve(deepcopy(case["input"]))) == canonical(case["expected"])
                        except Exception:
                            correct = False
                        if not correct:
                            failures.append(index)
                    self.assertTrue(failures, "Mutant survived the separate final evaluation")
            self.assertGreaterEqual(public_survivors, 1, "Holdout must catch a public-surviving mutant")

    def test_worker_manifest_never_discloses_private_fields_and_is_independent_copy(self):
        manifests = public_manifest()
        self.assertEqual(len(manifests), 8)
        for task, manifest in zip(TASKS, manifests):
            self.assertEqual(set(manifest), {"id", "family", "spec", "requirements", "initial_files", "public_cases"})
            self.assertEqual(manifest, public_manifest(task.id))
            self.assertNotIn(task.known_solution, canonical(manifest))
            manifest["public_cases"][0]["expected"] = "corrupted copy"
            manifest["initial_files"]["solution.py"] = "leak"
            self.assertNotEqual(task.public_cases[0]["expected"], "corrupted copy")
            self.assertIn("NotImplementedError", task.initial_files["solution.py"])

    def test_signature_is_stable_and_binds_private_cases_and_known_source(self):
        initial = fixture_signature()
        self.assertRegex(initial, r"^[0-9a-f]{64}$")
        self.assertEqual(initial, fixture_signature())
        task = TASKS[0]
        old = deepcopy(task.hidden_cases[0]["expected"])
        try:
            task.hidden_cases[0]["expected"] = {"mutated": True}
            self.assertNotEqual(initial, fixture_signature())
        finally:
            task.hidden_cases[0]["expected"] = old
        self.assertEqual(initial, fixture_signature())
        old_source = task.mutants["null-stored-instead-of-deleted"]
        try:
            task.mutants["null-stored-instead-of-deleted"] += "\n# changed\n"
            self.assertNotEqual(initial, fixture_signature())
        finally:
            task.mutants["null-stored-instead-of-deleted"] = old_source

    def test_reference_rejects_invalid_or_unbounded_proposed_inputs(self):
        invalid = {
            "config-merge": [{"layers": [None]}, {"layers": {}, "other": 1}, {"layers": [{}] * 17}],
            "config-pointers": [
                {"document": {}, "operations": [{"op": "add", "path": "", "value": 1}]},
                {"document": [], "operations": [{"op": "add", "path": "/01", "value": 1}]},
                {"document": {}, "operations": [{"op": "remove", "path": "/missing"}]},
                {"document": {}, "operations": [{"op": "add", "path": "/~2", "value": 1}]},
                {"document": [], "operations": [{"op": "replace", "path": "/-", "value": 1}]},
            ],
            "config-expand": [{"values": {"bad": "x"}}, {"values": {"A": 1}}, {"values": {"A": "x" * 65}}],
            "interval-union": [{"intervals": [[1, 0]], "window": [0, 1]}, {"intervals": [[False, 1]], "window": [0, 1]}],
            "interval-load": [{"bookings": [[0, 1, 0]]}, {"bookings": [[-31, 1, 2]]}],
            "interval-slot": [{"window": [0, 1], "blocked": [], "duration": 0, "not_before": 0}],
            "ledger-transfers": [{"balances": {"a": -1}, "events": []}, {"balances": {"a": 1}, "events": [{"id": "e", "from": "a", "to": "b", "amount": 1}]}],
            "ledger-versions": [{"events": [{"id": "e", "key": "k", "version": True, "op": "delete"}]}, {"events": [{"id": "e", "key": "k", "version": 0, "op": "set"}]}],
        }
        for task in TASKS:
            for payload in invalid[task.id] + [None, [], {"unknown": 1}, {"float": 1.5}, {"huge": "x" * 257}]:
                with self.subTest(task=task.id, input=payload):
                    with self.assertRaises(ValueError):
                        task.oracle(payload)
        with self.assertRaises(ValueError):
            get_task("config-expand").oracle({"values": {"A": "x" * 64, "B": "${A}" * 16, "C": "${B}" * 16}})

    def test_serialized_bounds_fit_blackbox_transport(self):
        with self.assertRaisesRegex(ValueError, "Serialized input bound"):
            get_task("config-merge").oracle({"layers": [{str(n): "x" * 256 for n in range(40)}]})
        with self.assertRaisesRegex(ValueError, "Serialized output bound"):
            get_task("config-expand").oracle({"values": {"A": "\x00" * 64, "B": "${A}" * 16, "C": "${B}" * 4, "D": "${C}"}})
        for task in TASKS:
            for case in task.public_cases + task.hidden_cases + task.fuzz_cases:
                self.assertLessEqual(len(canonical(case["input"]).encode("ascii")), 8192)
                self.assertLessEqual(len(canonical(case["expected"]).encode("ascii")), 32768)

    def test_additional_seeded_differential_cases(self):
        for number, task in enumerate(TASKS):
            solve, rng = trusted_function(task.known_solution), random.Random(97000 + number)
            for index in range(100):
                payload = _sample(task.id, rng)
                with self.subTest(task=task.id, sample=index):
                    self.assertEqual(canonical(solve(deepcopy(payload))), canonical(task.oracle(payload)))

    def test_interval_conservation_and_translation_properties(self):
        for task in TASKS:
            if task.family != "interval":
                continue
            for case in task.fuzz_cases:
                payload, answer = case["input"], case["expected"]
                if task.id == "interval-union":
                    lo, hi = payload["window"]
                    self.assertEqual(answer["total"] + sum(b - a for a, b in answer["gaps"]), hi - lo)
                    self.assertEqual(answer["total"], sum(b - a for a, b in answer["intervals"]))
                    permuted = {**payload, "intervals": list(reversed(payload["intervals"])) + payload["intervals"]}
                    self.assertEqual(task.oracle(permuted), answer)
                elif task.id == "interval-load":
                    self.assertEqual(answer["work"], sum((b - a) * weight for a, b, weight in payload["bookings"]))
                    self.assertEqual(task.oracle({"bookings": list(reversed(payload["bookings"]))}), answer)
                else:
                    if answer["start"] is not None:
                        start = answer["start"]
                        self.assertGreaterEqual(start, payload["not_before"])
                        self.assertTrue(any(a <= start and start + payload["duration"] <= b for a, b in answer["free"]))
                        for earlier in range(max(payload["window"][0], payload["not_before"]), start):
                            self.assertFalse(any(a <= earlier and earlier + payload["duration"] <= b for a, b in answer["free"]))

    def test_ledger_conservation_and_event_accounting(self):
        for case in get_task("ledger-transfers").fuzz_cases:
            payload, answer = case["input"], case["expected"]
            self.assertEqual(sum(payload["balances"].values()), sum(answer["balances"].values()))
            self.assertTrue(all(value >= 0 for value in answer["balances"].values()))
            self.assertEqual(len(answer["accepted"]) + len(answer["rejected"]) + len(answer["duplicates"]), len(payload["events"]))
        task = get_task("ledger-versions")
        for case in task.fuzz_cases:
            payload, answer = case["input"], case["expected"]
            repeated = {"events": payload["events"] + payload["events"]}
            if len(repeated["events"]) <= 32:
                result = task.oracle(repeated)
                self.assertEqual(result["state"], answer["state"])
                self.assertEqual(result["versions"], answer["versions"])
                self.assertEqual(result["stale"], answer["stale"])
                self.assertEqual(result["duplicates"], answer["duplicates"] + [event["id"] for event in payload["events"]])


if __name__ == "__main__":
    unittest.main()
