# Statistical limits and the next confirmatory comparison

**Status: reviewed design guidance only. There is no new evidence of statistical advantage.** This note preserves the independently reviewed [design receipt](../analysis/cumulative-confirmatory-design-check-v1.json), copied unchanged into tracked source. It adds no C01/C02 prerequisite and does not change implementation, execution authorization, funding, or the planned cohort.

## What the six trajectories can establish

The current development study retains four cumulative milestones and all three arms: **S4-G** (4 builders + 4 reviewers), **S16-G** (16 + 4), and **O16-G** (16 + 4). Each runs in a healthy and a compound-recovery block. These are **six trajectories within one application family**, not six independent project replications. Roles, scenarios, candidate branches, milestones, and repeated executions are nested observations.

After execution and audit, this cohort can describe completion, inherited correctness, unresolved work, regressions, and observed continuation after the specified faults. It cannot establish population superiority, stable Elo rankings, or general recovery reliability. All arms use gossip, so it cannot establish gossip's advantage over a broker. The separate placement-by-transport four-cell study remains open.

## Smallest comparison preserving the three-arm scope

Use **F genuinely held-out application families**, each contributing one complete matched S4-G/S16-G/O16-G block: **3F whole-project trajectories**. Draw one fault condition per family from a prospectively frozen target mixture and apply it to every arm in that block. This supports the mixture-level paired analysis; separate condition effects need their own design. Related task variants and repeated conditions do not increase family count.

Prefer **S16-G versus O16-G** as the single primary contrast, with scale descriptive. If both placement and S16-G versus S4-G are confirmatory, prospectively allocate alpha .025 to each; the retained single-primary option uses .05. A two-arm study would explicitly defer scale confirmation, not complete the existing scope.

Quality remains primary: success requires all four milestones within the common public-work horizon and offered budget, followed by every mandatory inherited/new-feature acceptance gate and valid source/promotion provenance after the complete-cohort freeze. Speed, cost, and partial coverage cannot compensate for failure. Early stopping is unsuccessful through the horizon, not fast completion.

## Decision rule and sample needs

The retained **+10 percentage point** worthwhile-effect margin is an engineering proposal, not user approval or a pilot-derived estimate. With the prospectively chosen paired acceptance-difference interval:

- Lower endpoint above +10 points supports worthwhile benefit within the tested contract.
- Upper endpoint below +10 points rules out that worthwhile benefit within the tested contract.
- Otherwise the result is inconclusive. Rejecting zero with McNemar's test is a weaker claim.

The following are copied retained planning examples using the conservative Bonferroni–Clopper–Pearson paired interval and alpha .025 per claim. **No power calculation was rerun.**

| Independent family pairs | Establish >10pp, assuming true +20pp gain | Exclude 10pp, assuming no gain |
| ---: | ---: | ---: |
| 100 | 4.0% | 5.6% |
| 400 | 51.3% | 62.6% |
| 800 | 92.5% | 96.8% |

The two columns assume 40% and 30% paired discordance respectively, independently sampled families, and fresh model draws. **800 is an illustrative coarse-grid row, not a minimum, selected sample size, price forecast, or funding request.** An 800-family three-arm study would contain 2,400 project trajectories. These assumptions are not established by the one-family cohort.

Before later confirmation, freeze the held-out registry, controller/models, fault mixture, endpoints, multiplicity, analysis method, and fixed complete sample. Qualifying a more efficient established paired interval may change sample needs; changing methods after outcomes is not permitted. This later design work does not delay current engineering gates.

## Evidence boundary

The receipt binds inspected sources at `08c9fb2e68a4c92db2a5d870908ab9acf8fb2ebd`, distinguishes dated checkpoints from current implementation status, and preserves all three-arm and separate transport obligations. Independent review verified **10 source hashes, 10 finding pointers, and eight copied power rows**. No new tests, candidate execution, provider calls, statistical recalculations, or comparative outcomes were produced.

The tracked JSON is byte-identical to the reviewed receipt, SHA256 `42ecfbf0baf92936b91192456e9730acec2efb4924f8c2d00a5f206877241fdb`. Its input paths identify the inspected local evidence; paths or hashes alone do not supply missing generated artifacts in a fresh clone.
