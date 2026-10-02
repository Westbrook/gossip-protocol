"""Independent finite enumeration checks for prospective statistical helpers."""
import itertools
import math
import unittest

from analysis.confirmatory_statistics import (
    analyze_pairs, binomial_masses, binomial_tail, clopper_pearson,
    cluster_sign_flip, exact_mcnemar, historical_uncertainty,
    interval_decision_power, mcnemar_power, paired_interval,
)


def mass(n, k, p):
    return math.comb(n, k) * p ** k * (1 - p) ** (n - k)


def joint(n, w, l, pw, pl):
    return math.comb(n, w) * math.comb(n - w, l) * pw ** w * pl ** l * (1 - pw - pl) ** (n - w - l)


class ConfirmatoryStatisticsTests(unittest.TestCase):
    def test_binomial_tails_match_direct_enumeration(self):
        for n, p in itertools.product(range(26), (0, .01, .2, .5, .9, 1)):
            for k in range(-1, n + 2):
                expected = math.fsum(mass(n, j, p) for j in range(max(k, 0), n + 1))
                self.assertAlmostEqual(binomial_tail(n, k, p), expected, places=12)

    def test_mass_recurrence_matches_independent_formula(self):
        for n, p in itertools.product((0, 1, 8, 30), (0, .1, .5, .97, 1)):
            values = binomial_masses(n, p)
            self.assertAlmostEqual(math.fsum(values), 1)
            for k, value in enumerate(values):
                self.assertAlmostEqual(value, mass(n, k, p), places=12)

    def test_large_n_tail_symmetry_and_monotonicity(self):
        for p in (.001, .2, .5, .9, .999):
            values = [binomial_tail(2000, k, p) for k in range(0, 2002, 37)]
            self.assertEqual(values, sorted(values, reverse=True))
            for k in (1, 400, 1000, 1800, 2000):
                self.assertAlmostEqual(binomial_tail(2000, k, p) + binomial_tail(2000, 2001 - k, 1 - p), 1, places=9)

    def test_clopper_pearson_boundary_closed_form(self):
        self.assertEqual(clopper_pearson(0, 0), (0, 1))
        lower, upper = clopper_pearson(0, 10)
        self.assertEqual(lower, 0)
        self.assertAlmostEqual(upper, 1 - .025 ** .1, places=12)
        lower, upper = clopper_pearson(10, 10)
        self.assertAlmostEqual(lower, .025 ** .1, places=12)
        self.assertEqual(upper, 1)

    def test_binomial_interval_has_finite_sample_coverage(self):
        for n, p in itertools.product(range(1, 13), (.001, .05, .2, .5, .9, .999)):
            coverage = math.fsum(mass(n, k, p) for k in range(n + 1)
                                 if clopper_pearson(k, n)[0] <= p <= clopper_pearson(k, n)[1])
            self.assertGreaterEqual(coverage + 1e-12, .95)

    def test_paired_interval_covers_multinomial_difference(self):
        for n, pw, pl in itertools.product((1, 4, 9, 12), (.0, .1, .3), (.0, .1, .3)):
            coverage = 0
            for w in range(n + 1):
                for l in range(n - w + 1):
                    ci = paired_interval(w, l, n - w - l)
                    if ci['lower'] <= pw - pl <= ci['upper']:
                        coverage += joint(n, w, l, pw, pl)
            self.assertGreaterEqual(coverage + 1e-12, .95)

    def test_interval_reverse_and_all_ties(self):
        a, b = paired_interval(20, 3, 17), paired_interval(3, 20, 17)
        self.assertAlmostEqual(a['lower'], -b['upper'])
        self.assertAlmostEqual(a['upper'], -b['lower'])
        self.assertAlmostEqual(a['estimate'], -b['estimate'])
        empty = paired_interval(0, 0, 0)
        self.assertIsNone(empty['estimate'])
        self.assertEqual((empty['lower'], empty['upper']), (-1, 1))
        ties = paired_interval(0, 0, 100)
        self.assertLess(ties['lower'], 0)
        self.assertGreater(ties['upper'], 0)

    def test_exact_mcnemar_known_counts(self):
        self.assertEqual(exact_mcnemar(0, 0), dict(greater=1, less=1, two_sided=1))
        self.assertAlmostEqual(exact_mcnemar(3, 0)['two_sided'], .25)
        self.assertEqual(exact_mcnemar(1, 0)['two_sided'], 1)
        self.assertAlmostEqual(exact_mcnemar(8, 1)['two_sided'], 20 / 512)
        self.assertEqual(exact_mcnemar(5, 5)['two_sided'], 1)

    def test_mcnemar_power_equals_full_multinomial_enumeration(self):
        for n, pw, pl in itertools.product((0, 6, 10, 16), (.1, .3, .6), (.0, .1, .3)):
            expected = math.fsum(joint(n, w, l, pw, pl)
                for w in range(n + 1) for l in range(n - w + 1)
                if math.fsum(mass(w + l, k, .5) for k in range(w, w + l + 1)) <= .025)
            self.assertAlmostEqual(mcnemar_power(n, pw, pl), expected, places=12)

    def test_mcnemar_power_extremes_and_null_control(self):
        self.assertEqual(mcnemar_power(100, 0, 0), 0)
        self.assertEqual(mcnemar_power(5, 1, 0), 0)
        self.assertEqual(mcnemar_power(6, 1, 0), 1)
        for n in (10, 40, 100):
            self.assertLessEqual(mcnemar_power(n, .2, .2), .025 + 1e-12)

    def test_interval_decision_power_matches_full_enumeration(self):
        for n, pw, pl in itertools.product((0, 6, 12), (.0, .3, .7), (.0, .1, .2)):
            expected = dict(meaningful_benefit=0., rules_out_meaningful_benefit=0., inconclusive=0.)
            for w in range(n + 1):
                for l in range(n - w + 1):
                    ci = paired_interval(w, l, n - w - l, alpha=.2)
                    key = ('meaningful_benefit' if ci['lower'] > .1 else
                           'rules_out_meaningful_benefit' if ci['upper'] < .1 else 'inconclusive')
                    expected[key] += joint(n, w, l, pw, pl)
            observed = interval_decision_power(n, pw, pl, alpha=.2)
            for key in expected:
                self.assertAlmostEqual(observed[key], expected[key], places=12)

    def test_margin_decision_is_stricter_than_zero_test(self):
        result = paired_interval(20, 5, 75)
        self.assertLess(exact_mcnemar(20, 5)['two_sided'], .05)
        self.assertLess(result['lower'], .1)
        self.assertEqual(analyze_pairs([])['decision'], 'inconclusive')

    def test_analysis_uses_preregistered_nondefault_alpha(self):
        rows = [dict(cluster_id=str(i), treatment_accepted=i < 20, control_accepted=i >= 95)
                for i in range(100)]
        default = analyze_pairs(rows)
        adjusted = analyze_pairs(rows, alpha=.025)
        self.assertEqual(adjusted['confidence'], .975)
        self.assertEqual(adjusted['alpha'], .025)
        self.assertLess(adjusted['lower'], default['lower'])
        self.assertGreater(adjusted['upper'], default['upper'])
        with self.assertRaises(ValueError):
            analyze_pairs(rows, alpha=0)

    def test_unique_clusters_and_literal_binary_outcomes_required(self):
        row = dict(cluster_id='repo-family', treatment_accepted=True, control_accepted=False)
        with self.assertRaises(ValueError):
            analyze_pairs([row, row])
        for bad in (1, 0, 'true', None):
            with self.assertRaises(ValueError):
                analyze_pairs([dict(row, treatment_accepted=bad)])
        for bad in ('', None, 4):
            with self.assertRaises(ValueError):
                analyze_pairs([dict(row, cluster_id=bad)])

    def test_both_pass_and_both_fail_are_ties_not_independent_trials(self):
        rows = [dict(cluster_id=str(i), treatment_accepted=t, control_accepted=c)
                for i, (t, c) in enumerate(((True, True), (False, False), (True, False), (False, True)))]
        result = analyze_pairs(rows)
        self.assertEqual((result['pairs'], result['wins'], result['losses'], result['ties']), (4, 1, 1, 2))
        self.assertEqual(result['estimate'], 0)

    def test_cluster_sensitivity_cannot_manufacture_two_domain_significance(self):
        self.assertEqual(cluster_sign_flip([0, .5])['two_sided_sign_flip_p'], 1)
        self.assertEqual(cluster_sign_flip([0, .5])['minimum_attainable_two_sided_p'], 1)
        result = cluster_sign_flip([.2, .1])
        self.assertEqual(result['two_sided_sign_flip_p'], .5)
        self.assertEqual(result['minimum_attainable_two_sided_p'], .5)
        self.assertEqual(cluster_sign_flip([0, 0])['two_sided_sign_flip_p'], 1)

    def test_historical_repeats_stay_inside_domains(self):
        rows = [dict(project_id=p, repetition=r, left_accepted=False, right_accepted=p == 'one' and r == 0,
                     left_requirements_passed=5, right_requirements_passed=6, requirements_total=10)
                for p, r in itertools.product(('one', 'two'), (0, 1))]
        summary = dict(status='certified_complete', comparisons=dict(whole_project_acceptance=dict(left_policy='sequential-four', right_policy='independent-four', matches=rows)))
        result = historical_uncertainty(summary)
        self.assertEqual(result['matched_blocks'], 4)
        self.assertEqual(result['independent_application_identities'], 2)
        self.assertEqual(result['acceptance']['equal_cluster_mean'], .25)
        self.assertIsNone(result['population_confidence_interval'])
        comparison = summary['comparisons']['whole_project_acceptance']
        comparison['left_policy'] = 'independent-four'
        with self.assertRaises(ValueError):
            historical_uncertainty(summary)
        comparison['left_policy'] = 'sequential-four'
        rows.append(rows[0])
        with self.assertRaises(ValueError):
            historical_uncertainty(summary)
        with self.assertRaises(ValueError):
            historical_uncertainty(dict(summary, status='partial'))

    def test_invalid_counts_probabilities_and_resource_bounds_rejected(self):
        for bad in (-1, True, 1.5, '2'):
            with self.assertRaises(ValueError):
                binomial_tail(bad, 0, .5)
        for bad in (-.1, 1.1, math.nan, math.inf, True):
            with self.assertRaises(ValueError):
                binomial_tail(2, 1, bad)
        for call in (lambda: clopper_pearson(3, 2), lambda: clopper_pearson(1, 2, 0),
                     lambda: exact_mcnemar(1, -1), lambda: mcnemar_power(10, .8, .3),
                     lambda: interval_decision_power(10, .8, .3), lambda: cluster_sign_flip([]),
                     lambda: cluster_sign_flip([.1] * 21), lambda: cluster_sign_flip([math.nan])):
            with self.assertRaises(ValueError):
                call()


if __name__ == '__main__':
    unittest.main()
