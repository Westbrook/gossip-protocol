"""Host-only checks of prospectively derived histories; no product execution."""
from __future__ import annotations

from copy import deepcopy
import json
import unittest

from gossip_harness import library_m2_acceptance_cases_v1 as m2
from gossip_harness import library_m3_acceptance_cases_v1 as m3
from gossip_harness import library_m4_acceptance_cases_v3 as m4
from gossip_harness.library_project_fixture_v1 import public_cases
from gossip_harness import library_v2_inherited_cases_v1 as inherited


class LibraryV2InheritedCasesTests(unittest.TestCase):
    def test_frozen_census_all_cases_preserved_with_exact_declared_delta(self):
        factories = {"m1": lambda: public_cases("m1"), "m2": m2.acceptance_cases,
                     "m3": m3.acceptance_cases, "m4": m4.acceptance_cases}
        for stage, count in (("m1", 8), ("m2", 12), ("m3", 10), ("m4", 18)):
            cases, old = inherited.acceptance_cases(stage), factories[stage]()
            self.assertEqual(len(cases), count)
            self.assertEqual([c["id"] for c in cases], [c["id"] for c in old])
            for case, prior in zip(cases, old):
                position = inherited.ADOPTION_POSITIONS.get(case["id"])
                if position is not None:
                    action = case["input"]["actions"].pop(position[0])
                    self.assertEqual(action["op"], "cli")
                    self.assertEqual(action["args"], ["backup-root-adopt", "--expect-unbound"])
                    self.assertEqual(case["expected"]["observations"].pop(position[1]), inherited.ADOPTION_EXPECTED)
                self.assertEqual(case, prior)
            self.assertEqual(inherited.registry_manifest(stage)["prior_cases_sha256"], inherited._sha(old))

    def test_every_root_using_case_has_one_declared_adoption_and_no_other_changes(self):
        found = set()
        for stage in ("m3", "m4"):
            for case in inherited.acceptance_cases(stage):
                actions = case["input"]["actions"]
                uses_root = any(a.get("method") in {"backup", "restore_backup", "list_backups"}
                    or a.get("op") == "backup_listing"
                    or a.get("op") == "cli" and a["args"][0] in {"backups", "backup", "restore-backup"}
                    for a in actions)
                adoption = [a for a in actions if a.get("op") == "cli" and a["args"][0] == "backup-root-adopt"]
                self.assertEqual(len(adoption), int(uses_root))
                if uses_root:
                    found.add(case["id"])
        self.assertEqual(found, set(inherited.ADOPTION_POSITIONS))
        self.assertEqual(len(found), 7)

    def test_inputs_bound_observers_unchanged_and_host_only_expectations(self):
        adapters = {"m2": m2.CHILD_ADAPTER, "m3": m3.CHILD_ADAPTER, "m4": m4.CHILD_ADAPTER}
        for stage in ("m1", "m2", "m3", "m4"):
            adapter = inherited.child_adapter(stage)
            compile(adapter, "inherited-observer", "exec")
            if stage in adapters:
                self.assertEqual(adapter, adapters[stage])
            self.assertNotIn('"expected"', adapter)
            self.assertNotIn("score_case", adapter)
            for case in inherited.acceptance_cases(stage):
                self.assertNotIn("expected", case["input"])
                self.assertLessEqual(len(json.dumps(case["input"], ensure_ascii=True).encode()), 65536)
                self.assertLessEqual(len(json.dumps(case["expected"], ensure_ascii=True).encode()), 65536)

    def test_scoring_rejects_boolean_tokens_and_modified_definitions(self):
        case = inherited.acceptance_cases("m3")[0]
        observed = deepcopy(case["expected"])
        self.assertTrue(inherited.score_case("m3", case, observed))
        observed["observations"][0]["generation"] = False
        self.assertFalse(inherited.score_case("m3", case, observed))
        changed = deepcopy(case)
        changed["expected"] = observed
        self.assertFalse(inherited.score_case("m3", changed, observed))
        self.assertFalse(inherited.score_case("m3", case, {**case["expected"], "extra": 0}))

    def test_m4_domain_is_shifted_only_by_explicit_adoption(self):
        case = next(c for c in inherited.acceptance_cases("m4")
                    if c["id"] == "m4-frozen-m2-receipt-bytes-through-backup-restore")
        observed = deepcopy(case["expected"])
        domain_index = 35
        self.assertEqual(observed["observations"][domain_index]["worker_generation"], {"$integer_at_least": 0})
        observed["observations"][domain_index]["worker_generation"] = 17
        self.assertTrue(inherited.score_case("m4", case, observed))
        for value in (-1, True, 1.0, "1"):
            changed = deepcopy(observed)
            changed["observations"][domain_index]["worker_generation"] = value
            self.assertFalse(inherited.score_case("m4", case, changed))
        for value in ({"exit": 0, "value": {"adopted": False, "registered": 0}, "other_stream_empty": True}, None):
            changed = deepcopy(observed)
            changed["observations"][1] = value
            self.assertFalse(inherited.score_case("m4", case, changed))
        self.assertFalse(inherited.score_case("m4", case, {"observations": observed["observations"][1:]}))

    def test_definitions_are_fresh_unknown_milestones_fail_closed(self):
        cases = inherited.acceptance_cases("m3")
        cases[0]["expected"].clear()
        self.assertTrue(inherited.acceptance_cases("m3")[0]["expected"])
        for function in (inherited.acceptance_cases, inherited.child_adapter, inherited.registry_manifest):
            with self.assertRaises(ValueError):
                function("unknown")
        self.assertFalse(inherited.score_case("m1", {"id": "unknown"}, {}))
