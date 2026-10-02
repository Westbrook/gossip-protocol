"""Prospective paired-study planning; no candidate execution or study score edits.

The exact interval is intentionally conservative: two 97.5% Clopper-Pearson
intervals give at least 95% simultaneous coverage for discordant-cell risks.
Their difference bounds the paired acceptance risk difference. Independence
and the sampling frame are design obligations, not properties inferred here.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from functools import lru_cache
import hashlib
import itertools
import json
import math
from pathlib import Path
from typing import Any

VERSION = "paired-confirmatory-statistics-v1"


def _count(value: int, name: str) -> None:
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be a nonnegative integer")


def _probability(value: float, name: str, *, interior: bool = False) -> None:
    if type(value) not in (int, float) or not math.isfinite(value):
        raise ValueError(f"{name} must be a finite probability")
    if not (0 < value < 1 if interior else 0 <= value <= 1):
        raise ValueError(f"{name} outside probability range")


def binomial_tail(n: int, k: int, p: float) -> float:
    """P[X >= k], using the shorter tail without subtracting a small answer."""
    _count(n, "n")
    if type(k) is not int:
        raise ValueError("k must be an integer")
    _probability(p, "p")
    if k <= 0:
        return 1.0
    if k > n:
        return 0.0
    if p in (0, 1):
        return float(p)
    # Start outside the mode and walk toward the end of that tail.
    upper = k > n * p
    index = k if upper else k - 1
    term = math.exp(math.lgamma(n + 1) - math.lgamma(index + 1)
                    - math.lgamma(n - index + 1) + index * math.log(p)
                    + (n - index) * math.log1p(-p))
    terms = [term]
    while (index < n if upper else index > 0):
        if upper:
            term *= (n - index) / (index + 1) * p / (1 - p)
            index += 1
        else:
            term *= index / (n - index + 1) * (1 - p) / p
            index -= 1
        terms.append(term)
        if term < terms[0] * 1e-16:
            break
    total = math.fsum(terms)
    return min(1.0, max(0.0, total if upper else 1 - total))


def binomial_masses(n: int, p: float) -> tuple[float, ...]:
    """Stable normalized recurrence from the binomial mode."""
    _count(n, "n")
    _probability(p, "p")
    result = [0.0] * (n + 1)
    if p in (0, 1):
        result[n if p else 0] = 1.0
        return tuple(result)
    mode = min(n, int((n + 1) * p))
    result[mode] = 1.0
    for k in range(mode, n):
        result[k + 1] = result[k] * (n - k) / (k + 1) * p / (1 - p)
    for k in range(mode, 0, -1):
        result[k - 1] = result[k] * k / (n - k + 1) * (1 - p) / p
    total = math.fsum(result)
    return tuple(value / total for value in result)


def clopper_pearson(k: int, n: int, alpha: float = 0.05) -> tuple[float, float]:
    """Equal-tailed binomial interval; coverage is at least 1-alpha."""
    _count(n, "n")
    _count(k, "k")
    _probability(alpha, "alpha", interior=True)
    if k > n:
        raise ValueError("k exceeds n")
    if n == 0:
        return (0.0, 1.0)
    return (_cp_lower(k, n, alpha / 2), 1 - _cp_lower(n - k, n, alpha / 2))


@lru_cache(maxsize=65536)
def _cp_lower(k: int, n: int, tail: float) -> float:
    if k == 0:
        return 0.0
    lo, hi = 0.0, k / n
    for _ in range(55):
        mid = (lo + hi) / 2
        if binomial_tail(n, k, mid) < tail:
            lo = mid
        else:
            hi = mid
    # Outward rounding protects coverage from the final bisection width.
    return lo


def paired_interval(wins: int, losses: int, ties: int,
                    alpha: float = 0.05) -> dict[str, Any]:
    """Win means only treatment accepted; loss means only control accepted."""
    for name, value in (("wins", wins), ("losses", losses), ("ties", ties)):
        _count(value, name)
    _probability(alpha, "alpha", interior=True)
    n = wins + losses + ties
    win = clopper_pearson(wins, n, alpha / 2)
    loss = clopper_pearson(losses, n, alpha / 2)
    return dict(pairs=n, wins=wins, losses=losses, ties=ties,
                estimate=(wins - losses) / n if n else None,
                lower=max(-1.0, win[0] - loss[1]),
                upper=min(1.0, win[1] - loss[0]), confidence=1 - alpha,
                method="Bonferroni-Clopper-Pearson discordant-cell risk difference")


def exact_mcnemar(wins: int, losses: int) -> dict[str, float]:
    _count(wins, "wins")
    _count(losses, "losses")
    n = wins + losses
    greater = binomial_tail(n, wins, 0.5)
    less = binomial_tail(n, losses, 0.5)
    return dict(greater=greater, less=less, two_sided=min(1.0, 2 * min(greater, less)))


def _joint(wins_probability: float, losses_probability: float) -> float:
    _probability(wins_probability, "wins_probability")
    _probability(losses_probability, "losses_probability")
    q = wins_probability + losses_probability
    if q > 1:
        raise ValueError("Discordant probabilities sum above one")
    return q


@lru_cache(maxsize=32768)
def _mcnemar_critical(discordant: int, alpha: float) -> int:
    lo, hi = 0, discordant + 1
    while lo < hi:
        mid = (lo + hi) // 2
        if binomial_tail(discordant, mid, 0.5) <= alpha / 2:
            hi = mid
        else:
            lo = mid + 1
    return lo


def mcnemar_power(n: int, wins_probability: float, losses_probability: float,
                  alpha: float = 0.05) -> float:
    """Exact unconditional power of the beneficial two-sided rejection tail.

    D~Bin(n,p10+p01), W|D~Bin(D,p10/(p10+p01)). This is power to show
    a direction at zero, not power to establish a practical margin.
    """
    _count(n, "n")
    _probability(alpha, "alpha", interior=True)
    q = _joint(wins_probability, losses_probability)
    if q == 0:
        return 0.0
    return math.fsum(mass * binomial_tail(d, _mcnemar_critical(d, alpha), wins_probability / q)
                     for d, mass in enumerate(binomial_masses(n, q)) if mass)


def interval_decision_power(n: int, wins_probability: float, losses_probability: float,
                            margin: float = 0.1, alpha: float = 0.05) -> dict[str, float]:
    """Exact multinomial probability of each conservative-interval decision."""
    _count(n, "n")
    _probability(alpha, "alpha", interior=True)
    _probability(margin, "margin")
    q = _joint(wins_probability, losses_probability)
    if n == 0:
        return dict(meaningful_benefit=0.0, rules_out_meaningful_benefit=0.0, inconclusive=1.0)
    lower = [_cp_lower(k, n, alpha / 4) for k in range(n + 1)]
    upper = [1 - lower[n - k] for k in range(n + 1)]
    # Condition on the win cell. Remaining losses are binomial, allowing an
    # O(n) mixture with monotone cut points rather than n^2 outcome enumeration.
    win_masses = binomial_masses(n, wins_probability)
    loss_p = losses_probability / (1 - wins_probability) if wins_probability < 1 else 0.0
    benefit_terms, ruled_out_terms = [], []
    for w, mass in enumerate(win_masses):
        if not mass:
            continue
        m = n - w
        lo, hi = 0, m + 1
        while lo < hi:  # first loss count that fails lower > margin
            mid = (lo + hi) // 2
            if lower[w] - upper[mid] > margin:
                lo = mid + 1
            else:
                hi = mid
        benefit_terms.append(mass * (1 - binomial_tail(m, lo, loss_p)))
        lo, hi = 0, m + 1
        while lo < hi:  # first loss count that makes upper < margin
            mid = (lo + hi) // 2
            if upper[w] - lower[mid] < margin:
                hi = mid
            else:
                lo = mid + 1
        ruled_out_terms.append(mass * binomial_tail(m, lo, loss_p))
    benefit, ruled_out = math.fsum(benefit_terms), math.fsum(ruled_out_terms)
    return dict(meaningful_benefit=benefit, rules_out_meaningful_benefit=ruled_out,
                inconclusive=max(0.0, 1 - benefit - ruled_out))


def analyze_pairs(rows: list[dict[str, Any]], margin: float = 0.1,
                  alpha: float = 0.05) -> dict[str, Any]:
    """Accept only one binary pair per declared independent cluster.

    A unique ID cannot prove independence; the preregistered sampling frame
    must establish it. Repeat seeds require a separate clustered estimand.
    """
    _probability(margin, "margin")
    ids: set[str] = set()
    wins = losses = ties = 0
    for row in rows:
        cluster = row.get("cluster_id")
        if not isinstance(cluster, str) or not cluster or cluster in ids:
            raise ValueError("Each pair needs a unique nonempty cluster_id")
        ids.add(cluster)
        t, c = row.get("treatment_accepted"), row.get("control_accepted")
        if type(t) is not bool or type(c) is not bool:
            raise ValueError("Acceptance outcomes must be literal booleans")
        wins += int(t and not c)
        losses += int(c and not t)
        ties += int(t == c)
    result = paired_interval(wins, losses, ties, alpha=alpha)
    result["alpha"] = alpha
    result["exact_mcnemar"] = exact_mcnemar(wins, losses)
    result["practical_margin"] = margin
    result["decision"] = ("meaningful_benefit" if result["lower"] > margin else
                          "rules_out_meaningful_benefit" if result["upper"] < margin else "inconclusive")
    return result


def cluster_sign_flip(differences: list[float]) -> dict[str, Any]:
    """Exploratory exhaustive sensitivity under whole-cluster sign exchangeability.

    This does not make convenience-selected historical domains randomized.
    """
    if not differences or len(differences) > 20:
        raise ValueError("Exact enumeration supports 1..20 clusters")
    if any(type(d) not in (int, float) or not math.isfinite(d) or abs(d) > 1 for d in differences):
        raise ValueError("Differences must be finite and within [-1,1]")
    observed = abs(math.fsum(differences))
    scores = [abs(math.fsum(s * d for s, d in zip(signs, differences)))
              for signs in itertools.product((-1, 1), repeat=len(differences))]
    count = sum(score >= observed - 1e-12 for score in scores)
    max_score = max(scores)
    return dict(clusters=len(differences), equal_cluster_mean=math.fsum(differences) / len(differences),
                two_sided_sign_flip_p=count / len(scores), enumerated_sign_assignments=len(scores),
                minimum_attainable_two_sided_p=sum(score >= max_score - 1e-12 for score in scores) / len(scores),
                interpretation="Exploratory sensitivity only; historical domain-label exchangeability and population sampling are unestablished")


def historical_uncertainty(summary: dict[str, Any]) -> dict[str, Any]:
    if summary.get("status") != "certified_complete":
        raise ValueError("Historical analysis requires a certified complete comparison")
    comparison = summary["comparisons"]["whole_project_acceptance"]
    if (comparison.get("left_policy"), comparison.get("right_policy")) != ("sequential-four", "independent-four"):
        raise ValueError("Historical policy orientation does not match this analysis")
    matches = comparison["matches"]
    domains: dict[str, list[dict[str, Any]]] = defaultdict(list)
    seen: set[tuple[str, int]] = set()
    for row in matches:
        identity = (row["project_id"], row["repetition"])
        if identity in seen:
            raise ValueError("Duplicate historical block")
        seen.add(identity)
        if type(row["left_accepted"]) is not bool or type(row["right_accepted"]) is not bool:
            raise ValueError("Malformed historical acceptance outcome")
        domains[row["project_id"]].append(row)
    acceptance, coverage, domain_rows = [], [], []
    for domain, rows in sorted(domains.items()):
        a = math.fsum(int(r["right_accepted"]) - int(r["left_accepted"]) for r in rows) / len(rows)
        c = math.fsum((r["right_requirements_passed"] - r["left_requirements_passed"]) / r["requirements_total"] for r in rows) / len(rows)
        acceptance.append(a)
        coverage.append(c)
        domain_rows.append(dict(domain=domain, repetitions=len(rows), acceptance_difference=a, coverage_difference=c))
    return dict(direction="independent-four minus sequential-four", matched_blocks=len(matches),
                independent_application_identities=len(domains), domains=domain_rows,
                acceptance=cluster_sign_flip(acceptance), coverage=cluster_sign_flip(coverage),
                population_confidence_interval=None,
                limitation="Only two convenience-selected application identities, each repeated twice. No calibrated population interval, confirmatory p-value, Elo, or cross-cohort ranking is justified.")


def _read_bound(path: Path) -> tuple[dict[str, Any], dict[str, str]]:
    raw = path.read_bytes()
    return json.loads(raw), dict(path=str(path), sha256=hashlib.sha256(raw).hexdigest())


def planning_artifact(summary_path: Path, recommendation_path: Path,
                      accounting_path: Path) -> dict[str, Any]:
    summary, summary_binding = _read_bound(summary_path)
    _, recommendation_binding = _read_bound(recommendation_path)
    accounting_receipt, accounting_binding = _read_bound(accounting_path)
    if accounting_receipt.get("status") != "passed" or any(accounting_receipt["unsettled"].values()):
        raise ValueError("Accounting snapshot must be reconciled and settled")
    accounting = accounting_receipt["accounting"]
    if accounting["limit"] != accounting["remaining"] + accounting["spent_or_reserved"]:
        raise ValueError("Accounting reconciliation does not balance")
    blocks = len(summary["comparisons"]["whole_project_acceptance"]["matches"])
    if blocks == 0:
        raise ValueError("No paired history available for cost illustration")
    cost = (summary["policies"]["independent-four"]["cost_micro_usd"]
            + summary["policies"]["sequential-four"]["cost_micro_usd"]) / blocks / 1_000_000
    scenarios = [("10pp_gain_30pct_discordance", 0.20, 0.10),
                 ("20pp_gain_40pct_discordance", 0.30, 0.10),
                 ("no_gain_30pct_discordance", 0.15, 0.15)]
    grid = [50, 100, 200, 400, 800, 1200]
    planning = []
    for label, pw, pl in scenarios:
        for n, alpha in itertools.product(grid, (.05, .025)):
            planning.append(dict(alpha=alpha, scenario=label, pairs=n, independent_task_families=n,
                                 trajectories=2 * n, p_treatment_only=pw, p_control_only=pl,
                                 true_acceptance_difference=pw - pl, discordance=pw + pl,
                                 direction_test_power=mcnemar_power(n, pw, pl, alpha=alpha),
                                 practical_interval_decision=interval_decision_power(n, pw, pl, alpha=alpha),
                                 historical_linear_api_cost_usd=n * cost))
    sources = [
        dict(title="NIST exact binomial confidence intervals", url="https://www.itl.nist.gov/div898/handbook/prc/section2/prc241.htm", use="Clopper-Pearson tail inversion; finite-sample binomial coverage"),
        dict(title="NIST Bonferroni simultaneous intervals", url="https://itl.nist.gov/div898/handbook/prc/section4/prc473.htm", use="Union bound for the two discordant-cell intervals; no independence between cells assumed"),
        dict(title="exact2x2 official documentation", url="https://search.r-project.org/CRAN/refmans/exact2x2/html/exact2x2.html", use="Conditional exact McNemar tests discordant wins given total discordance"),
        dict(title="Newcombe 1998 paired proportion intervals", url="https://pubmed.ncbi.nlm.nih.gov/9839354/", use="Paired score/profile-likelihood intervals are established efficiency alternatives; not implemented or validated here"),
        dict(title="Exact official paired unconditional test documentation", url="https://search.r-project.org/CRAN/refmans/Exact/html/paired.exact.test.html", use="Paired unconditional tests/intervals can improve power but require nuisance-parameter maximization and validated numerical search"),
        dict(title="exact2x2 official paired difference interval documentation", url="https://search.r-project.org/CRAN/refmans/exact2x2/html/mcnemarExactDP.html", use="Exact melding interval is another candidate; numerical integration and its nonzero-margin behavior must be independently qualified before adoption"),
        dict(title="Demsar 2006: Statistical Comparisons of Classifiers over Multiple Data Sets", url="https://www.jmlr.org/papers/volume7/demsar06a/demsar06a.pdf", use="Distinguish independent task/data-set comparisons from repeated measurements within them"),
    ]
    return dict(schema=VERSION, status="Prospective design proposal and retrospective sensitivity; not a frozen executable preregistration or spending authorization",
        bindings=[summary_binding, recommendation_binding, accounting_binding], historical=historical_uncertainty(summary),
        hypothesis_families=dict(
            formation="Independent versus serial candidate formation with the same controller; mechanism-specific, never evidence that live gossip is superior.",
            live_protocol="Separately preregister peer-local decisions plus gossip versus durable failover orchestration plus replicated broker, with matched roles, safety services, offered resources and exogenous fault schedule. One independent family per paired block; mechanism bridge arms remain descriptive unless separately allocated error.",
            joint_error_control="If the research claim is that either of these two techniques has a meaningful advantage, allocate alpha=.025 to each interval (Bonferroni overall error <=.05). The planning grid includes this stricter level and standalone alpha=.05. Select the family and allocation before any confirmatory outcome; never choose the better study after results.",
            scope="Do not pool studies, endpoints, task variants, model-interface generations or fault strata after outcomes. In live topology studies sample one fault stratum per task family independently from a frozen target mixture; stratified claims require their own prespecified analysis.",
        ),
        primary_design=dict(
            contrast="Independent-four versus sequential-four under the identical improved continuation controller; separate from development pilot A/B",
            endpoint="Binary success requires an exact source to complete the policy against public evidence and be frozen by the common active deadline/call budget, then pass every independent final private acceptance gate during later adjudication. Private grading starts only after the complete cohort source-freeze barrier; any unmet milestone or failed final gate is failure.",
            timing="The active cutoff applies to policy completion and source freeze, not to delayed private grading or final promotion. Record active source-freeze time, cohort waiting, independent final validation and final promotion separately. A retrospectively successful source time is credited only to a cutoff-eligible source that later passes final adjudication. Early terminal policy failure remains no success through the common horizon, not ordinary noninformative right-censoring or a speed improvement.",
            estimand="Difference in acceptance probability across a preregistered target distribution of independent task families and fresh model randomness",
            unit="One matched pair, one run per arm, from each independently authored/selected task family. Shared baselines, variants, repeated seeds and milestones remain one cluster and cannot inflate this sample size.",
            sampling="Freeze the task-family registry, task-generation/selection rule, target mixture weights and exclusions before any confirmatory calls. Sample independently from that mixture; one family per pair. New repositories alone do not prove independence. If fixed strata or repeated families are required, replace this iid analysis with preregistered stratum/cluster-aware inference before execution.",
            practical_margin=0.1, margin_reason="Proposed engineering decision: at least ten additional independently accepted projects per hundred attempts. This is not inferred from the observed pilot.",
            interval="Bonferroni-Clopper-Pearson paired risk-difference interval at the chosen 1-alpha confidence (95% standalone, 97.5% for each of two jointly protected claims): simultaneous intervals for P(treatment-only acceptance) and P(control-only acceptance), then subtract bounds.",
            decisions=dict(meaningful_benefit="Lower interval endpoint > +0.10", rules_out_meaningful_benefit="Upper endpoint < +0.10, within this population/model/budget contract; does not prove exact equality or universal failure", otherwise="Inconclusive; no posthoc margin change or automatic sample extension"),
            secondary_direction_test="Two-sided exact McNemar with the preregistered alpha (.05 standalone or .025 per claim in the two-claim family), with direction reported. Rejecting zero is weaker than establishing +10pp; this does not replace the primary interval decision.",
            multiplicity="One primary contrast, one endpoint and one final analysis. Coverage, time, cost, repairs and subgroup results are descriptive. Any additional confirmatory contrast or outcome requires prospectively allocated error control.",
            stopping="Fixed sample selected before outcomes using planning assumptions; no efficacy peeking or optional extension. Early cost/safety/infrastructure stop yields an incomplete study and no confirmatory declaration. All failed or stopped trajectories remain visible; no outcome-based replacements.",
            randomization="Randomize and freeze within-pair execution order; balance host/time windows; pair tasks and offered resource caps. Fresh arm-specific initial generation; no shared candidates in this confirmatory contrast.",
            independence="Families are sampled independently and each pair has fresh random model draws; whole-pair outcomes may be correlated. Repeated development feedback makes new tasks development data, not held-out confirmation.",
            freeze_barrier="Freeze specification, fixtures, complete evaluator, models, controller, budgets, seeds/order, sample size and analysis source. Seal all final-evaluation material and defer every private result until all study trajectories are frozen.",
            infrastructure="Predeclare infrastructure classification without seeing private outcomes. No automatic reruns; primary policy-success denominator retains launched bounded failures, with explicit infrastructure sensitivity and missingness disclosure. An incomplete/unsettled study is not certified.",
            guardrails="Preserve source/sandbox/Git/receipt/final-evaluation invariants. Report inherited regressions separately and veto deployment if those gates fail; never trade baseline correctness for feature coverage."),
        sequence=[
            "Complete the new 12-trajectory, two-domain exploratory controller pilot; this can guide engineering and detect feasibility failures, not establish superiority.",
            "Freeze the chosen controller without looking at confirmatory task outcomes. Use development-only data to choose assumptions and exact fixed sample size, then preregister a new task-family registry and evaluation contract.",
            "Run one fresh complete offline qualification/rehearsal for the changed contract; reconcile exact source, environment, evaluator, limits and budget reservation before paid execution.",
            "Run the independently sampled confirmatory pair roster with one final analysis and retain failures. Interpret only the population and model/tool interface actually tested.",
            "If a meaningful advantage survives, replicate on maintained repositories and tool-equipped agents; treat live gossip transport as a separate randomized fault-condition factor, not a claim established by candidate formation.",
        ],
        power=dict(method="Exact finite binomial/multinomial summation, floating-point evaluation; no Monte Carlo or fitted pilot effect", alpha_levels=[.05, .025], margin=.1,
                   iid_requirement="Numbers assume independent identically sampled task pairs from the fixed target mixture; correlated task variants invalidate these n values.",
                   grid=planning, sample_size_status="A planning grid, not a selected/frozen sample size. Pick the row/assumptions and funding before confirmation.",
                   conservatism="The 800-pair row is a conservative proof-of-feasibility example, not a minimum or inevitable requirement. No minimum sample-size search has been performed. Interval conservatism can materially increase task and API requirements.",
                   alternatives="Before funding confirmation, independently qualify an established paired score/profile-likelihood or exact-unconditional/melded interval and recalculate power on the same assumptions. Score approximations need coverage checks, particularly sparse/boundary cells; unconditional exact inference requires reliable nuisance maximization and inversion; melding requires validated integration. No unverified alternative is substituted here, and the method cannot change after outcomes.",
                   funding_recommendation="Complete the controller pilot first. At alpha=.025, 100/200/400 pairs have about81%/99%/100% direction-test power under a true20pp gain, but only4%/16%/51% power to establish a gain greater than10pp using this conservative interval. Under no gain, those sizes rule out10pp only about6%/22%/63% of the time. Do not fund these as high-power practical-effect confirmation. The800-pair example reaches about92% benefit or97% exclusion power for the respective assumptions; validate efficient alternatives and the task-family sampling frame before fixing a final budget.",
                   boundary="A true gain exactly equal to the10pp decision margin should usually remain inconclusive, even at large n. Power to detect10pp versus zero is a different question from establishing a gain strictly greater than10pp."),
        budget=dict(cumulative_cap_usd=accounting["limit"] / 1_000_000, cumulative_used_usd=accounting["spent_or_reserved"] / 1_000_000, remaining_usd=accounting["remaining"] / 1_000_000,
                    snapshot="Historical settled accounting snapshot bound to the completed first benchmark; not a live balance or a new spending authorization",
                    historical_api_cost_per_pair_usd=cost,
                    basis="Eight historical matched-policy trajectories / four pairs, excluding the strong anchor; linear API-only illustration, not a forecast or authorized cap. New repair use, task sizes and model versions can materially change cost.",
                    limitation="Remaining authorized funds cannot support even the smallest planning-grid confirmatory study. It may support an exploratory pilot only after a concrete reservation ceiling is checked. Do not spend beyond the ledger cap; local compute and fixture/engineering effort excluded."),
        sources=sources)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--recommendation", type=Path, required=True)
    parser.add_argument("--accounting-reconciliation", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    value = planning_artifact(args.summary, args.recommendation, args.accounting_reconciliation)
    value["analysis_source_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    with args.output.open("x") as stream:
        json.dump(value, stream, indent=2, allow_nan=False)
        stream.write("\n")
    print(json.dumps(dict(output=str(args.output), historical=value["historical"], budget=value["budget"])))


if __name__ == "__main__":
    main()
