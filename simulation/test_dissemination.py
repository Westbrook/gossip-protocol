"""Check the experiment model rather than fitting any particular published claim."""

import random
import unittest

from dissemination import draw_contacts, effective_fanout, run_trial, spread_round, summarize


class DisseminationTests(unittest.TestCase):
    def test_single_node_is_complete_at_round_zero(self):
        for mode in ("push", "push_pull"):
            trial = run_trial(1, 3, 42, mode)
            self.assertEqual(trial["rounds_to_95_percent"], 0)
            self.assertEqual(trial["rounds_to_100_percent"], 0)
            self.assertEqual(trial["attempted_contacts_to_100_percent"], 0)
            self.assertEqual(trial["informed_at_end_of_round"], [1])

    def test_fanout_is_capped_and_has_no_self_or_duplicate_peers(self):
        for node_count, requested in ((1, 3), (2, 3), (10, 3), (10, 0), (10, 100)):
            contacts = draw_contacts(node_count, requested, random.Random(42))
            self.assertEqual(len(contacts), node_count)
            for caller, peers in enumerate(contacts):
                self.assertEqual(len(peers), min(requested, node_count - 1))
                self.assertEqual(len(peers), len(set(peers)))
                self.assertNotIn(caller, peers)
                self.assertTrue(all(0 <= peer < node_count for peer in peers))

    def test_seed_reproduces_contacts_and_trial(self):
        self.assertEqual(draw_contacts(20, 3, random.Random(9)), draw_contacts(20, 3, random.Random(9)))
        self.assertNotEqual(draw_contacts(20, 3, random.Random(9)), draw_contacts(20, 3, random.Random(10)))
        for mode in ("push", "push_pull"):
            self.assertEqual(run_trial(100, 3, 91, mode), run_trial(100, 3, 91, mode))

    def test_push_has_no_within_round_cascade(self):
        initial = {0}
        self.assertEqual(spread_round(initial, [[1], [2], [3], [0]], "push"), {0, 1})
        self.assertEqual(initial, {0})

    def test_push_pull_is_full_duplex_but_has_no_within_round_cascade(self):
        initial = {0}
        self.assertEqual(spread_round(initial, [[1], [2], [3], [0]], "push_pull"), {0, 1, 3})
        self.assertEqual(initial, {0})

    def test_full_fanout_finishes_in_one_round(self):
        for mode in ("push", "push_pull"):
            trial = run_trial(5, 99, 1, mode)
            self.assertEqual(trial["informed_at_end_of_round"], [1, 5])
            self.assertEqual(trial["rounds_to_100_percent"], 1)
            # All callers count, including four initially uninformed callers.
            self.assertEqual(trial["attempted_contacts_to_100_percent"], 20)

    def test_zero_fanout_does_not_silently_report_success(self):
        trial = run_trial(5, 0, 1, "push_pull", max_rounds=4)
        self.assertEqual(trial["informed_at_end_of_round"], [1, 1, 1, 1, 1])
        self.assertIsNone(trial["rounds_to_95_percent"])
        self.assertIsNone(trial["rounds_to_100_percent"])
        self.assertIsNone(trial["attempted_contacts_to_100_percent"])

    def test_knowledge_is_monotonic_and_contacts_count_all_nodes(self):
        for mode in ("push", "push_pull"):
            trial = run_trial(100, 3, 98, mode)
            history = trial["informed_at_end_of_round"]
            self.assertEqual(history, sorted(history))
            self.assertEqual(history[0], 1)
            self.assertEqual(history[-1], 100)
            self.assertEqual(trial["attempted_contacts_to_100_percent"], 300 * (len(history) - 1))
            r95 = trial["rounds_to_95_percent"]
            self.assertGreaterEqual(history[r95], 95)
            self.assertLess(history[r95 - 1], 95)

    def test_push_pull_dominates_push_under_identical_schedules(self):
        rng = random.Random(314)
        push, push_pull = {0}, {0}
        for _ in range(20):
            contacts = draw_contacts(50, 3, rng)
            push = spread_round(push, contacts, "push")
            push_pull = spread_round(push_pull, contacts, "push_pull")
            self.assertTrue(push <= push_pull)

    def test_summary_nearest_rank_percentiles_and_failures(self):
        summary = summarize(list(range(1, 101)) + [None])
        self.assertEqual(summary["p50"], 50)
        self.assertEqual(summary["p95"], 95)
        self.assertEqual(summary["not_reached"], 1)
        self.assertEqual(summary["completed"], 100)

    def test_invalid_inputs(self):
        with self.assertRaises(ValueError):
            effective_fanout(0, 3)
        with self.assertRaises(ValueError):
            effective_fanout(10, -1)
        with self.assertRaises(ValueError):
            spread_round({0}, [[1], [0]], "not_a_mode")


if __name__ == "__main__":
    unittest.main()
