"""Cross-topology acceptance contracts over real independent Git stores."""

import json
import unittest

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness.experiment import EXPECTED, run_matrix


class ExperimentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.artifacts = ArtifactDirectory(cls.__name__, output_env="GOSSIP_LAB_TEST_OUTPUT",
                                          retain_success=True)
        cls.addClassCleanup(cls.artifacts.close)
        cls.root = cls.artifacts.root
        cls.output = cls.artifacts.output
        cls.report = run_matrix(cls.output, seeds=(0,),
                                progress=lambda message: print(message, flush=True))
        cls.cases = cls.report["cases"]
        cls.trace = [json.loads(line) for line in
                     (cls.output / "trace.jsonl").read_text().splitlines()]

    def cases_for(self, scenario):
        return [case for case in self.cases if case["scenario"] == scenario]

    def test_all_four_variants_preserve_release_state_and_task_ancestry(self):
        accepted = {
            "disjoint": ["alpha-one", "beta-two"],
            "text_conflict": ["alpha-one"],
            "semantic_conflict": ["alpha-six"],
            "inherited_scope": [],
        }
        self.assertEqual(len(self.cases), 16)
        for scenario, state in EXPECTED.items():
            cases = self.cases_for(scenario)
            self.assertEqual(len(cases), 4)
            self.assertEqual(len({case["release_tree"] for case in cases}), 1)
            for case in cases:
                with self.subTest(scenario=scenario, topology=case["topology"],
                                  transport=case["transport"]):
                    self.assertEqual(case["release_state"], state)
                    self.assertEqual(case["accepted_task_ids"], accepted[scenario])
                    self.assertLessEqual(sum(state.values()), 10)

    def test_text_conflict_is_distinct_from_semantic_conflict(self):
        for scenario, expected in (("text_conflict", "text_conflict"),
                                   ("semantic_conflict", "validation_failed")):
            for case in self.cases_for(scenario):
                rejections = [op for op in case["operations"]
                              if op["status"] not in ("accepted", "noop")]
                self.assertEqual(len(rejections), 1)
                self.assertEqual(rejections[0]["status"], expected)
                self.assertEqual(rejections[0]["new_head"], rejections[0]["old_head"])
                if case["topology"] == "maintainers":
                    stage = "subsystem" if scenario == "text_conflict" else "integration"
                    self.assertEqual(rejections[0]["stage"], stage)
                if scenario == "semantic_conflict":
                    self.assertIn("alpha + beta = 12 > 10", rejections[0]["detail"])
                    if case["topology"] == "maintainers":
                        local = [v for v in case["verifications"] if v["stage"] == "subsystem"]
                        self.assertEqual(len(local), 2)
                        self.assertTrue(all(v["passed"] for v in local))

    def test_unpublished_dependency_cannot_hide_behind_beta_tip_diff(self):
        for case in self.cases_for("inherited_scope"):
            self.assertEqual(len(case["operations"]), 1)
            operation = case["operations"][0]
            self.assertEqual(operation["status"], "scope_rejected")
            self.assertEqual(operation["changed_paths"], ["alpha.json", "beta.json"])
            self.assertIn("alpha.json", operation["detail"])
            self.assertEqual(case["release_head"], case["base_sha"])
            proposal = case["proposals"][0]
            self.assertEqual(proposal["allowed_paths"], ["beta.json"])
            self.assertEqual(proposal["base_sha"], case["unpublished_prerequisite"])
            self.assertEqual(proposal["dependencies"], [case["unpublished_prerequisite"]])

    def test_all_integration_levels_are_counted(self):
        counts = {
            ("hub", "disjoint"): (2, 2),
            ("hub", "text_conflict"): (2, 1),
            ("hub", "semantic_conflict"): (2, 2),
            ("hub", "inherited_scope"): (1, 0),
            ("maintainers", "disjoint"): (5, 5),
            ("maintainers", "text_conflict"): (4, 3),
            ("maintainers", "semantic_conflict"): (5, 5),
            ("maintainers", "inherited_scope"): (1, 0),
        }
        for case in self.cases:
            expected = counts[(case["topology"], case["scenario"])]
            self.assertEqual((case["metrics"]["prepare_calls"],
                              case["metrics"]["verification_calls"]), expected)
            if case["topology"] == "maintainers" and case["accepted_task_ids"]:
                self.assertEqual(case["release_head"], case["integration_head"])

    def test_delivery_precedes_fetch_and_promotion_uses_exact_tested_sha(self):
        events = {}
        received = set()
        for row in self.trace:
            key = (row["scenario"], row["topology"], row["transport"], row["seed"])
            if row["kind"] == "notice_published":
                events[(key, row["event"]["event_id"])] = row["event"]
            elif row["kind"] == "notice_received":
                event = events[(key, row["event_id"])]
                received.add((key, row["recipient"], event["payload"]["tip_sha"]))
            elif row["kind"] == "prepare":
                if row["stage"] == "subsystem":
                    subsystem = row["label"].split("-")[0]
                    receiver = f"maintainer-{subsystem}"
                else:
                    receiver = "integrator" if row["stage"] == "integration" else "release"
                self.assertIn((key, receiver, row["offered_sha"]), received)
            elif row["kind"] == "promotion":
                self.assertEqual(row["status"], "accepted")
                self.assertEqual(row["tested_sha"], row["new_head"])

    def test_same_patch_dag_and_final_heads_across_transport_variants(self):
        self.assertEqual(len({case["base_sha"] for case in self.cases}), 1)
        for scenario in EXPECTED:
            cases = self.cases_for(scenario)
            manifests = {json.dumps(case["proposals"], sort_keys=True) for case in cases}
            self.assertEqual(len(manifests), 1)
            for topology in ("hub", "maintainers"):
                matching = [case for case in cases if case["topology"] == topology]
                self.assertEqual(len({case["release_head"] for case in matching}), 1)
        # The independently generated disjoint/text fixtures both include the
        # same alpha-one patch. Different paths/times must yield identical DAGs.
        self.assertEqual(self.cases_for("disjoint")[0]["proposals"][0]["tip_sha"],
                         self.cases_for("text_conflict")[0]["proposals"][0]["tip_sha"])

    def test_saved_outputs_and_no_overwrite(self):
        self.assertEqual(json.loads((self.output / "results.json").read_text()), self.report)
        self.assertTrue(self.trace)
        original = (self.output / "results.json").read_bytes()
        with self.assertRaises(FileExistsError):
            run_matrix(self.output, seeds=(0,))
        self.assertEqual((self.output / "results.json").read_bytes(), original)
        for invalid in ((), (0, 0), (True,)):
            with self.assertRaises(ValueError):
                run_matrix(self.root / "invalid", seeds=invalid)


if __name__ == "__main__":
    unittest.main()
