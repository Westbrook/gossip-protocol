# Gossip × Agents: coordination lab

A provider-neutral experiment in **distributed Git integration authority** and
**gossip evidence delivery**. The original matrix uses scripted workers and real
independent Git repositories. A separate practical pilot connects bounded coding
workers, validation, task ownership, cost reservations, and release publication.
Both are small experiments; neither establishes production throughput or general
superiority over orchestration.

The working design follows Linux-style delegated integration:

```text
worker repositories → subsystem maintainer repositories
                    → disposable combined integration tree → accepted release

proposal/evidence announcements travel separately over a broker star or gossip
```

Repository hosting and authority are separate choices. This workflow can be
hosted on GitHub, a private server, or local paths. Git preserves history and
supports selective integration; executable project checks still determine
whether a clean merge is correct.

## Development verification

Use the shared verification workflow before running experiment demonstrations:

```sh
scripts/bootstrap-dev
.venv/bin/python -m devtools.verify --list
.venv/bin/python -m devtools.verify
```

Bootstrap reuses a persistent Python environment with hash-locked development
tools. The verification command checks syntax, types and correctness lint first,
then runs the offline tests from `tests/`, `simulation/` and `analysis/` in
isolated, resource-bounded groups. Results, timings and failure artifacts are
retained under `runs/verification/`. It does not make model requests. Use
`--reuse` to reuse successful checks whose complete inputs still match; reused
checks are reported separately from fresh executions.

Docker and browser checks are explicit lanes. For example:

```sh
.venv/bin/python -m devtools.verify --lanes docker
.venv/bin/python -m devtools.verify --lanes report,browser --workers 8
```

See [VERIFICATION.md](VERIFICATION.md) for focused checks, resource limits,
browser setup, artifact retention and the versioned validation batch API.
[AGENTS.md](AGENTS.md) specifies the scheduling and evidence rules.


The additive `gossip_harness.sustained_experiment_v2` and
`gossip_harness.swarm_experiment_v2` entry points batch independent validations
within explicit CPU/memory limits. `analysis.run_sustained_review_probes_v2`
uses the same session for independent supplemental observations. Original study
runners and frozen evidence remain unchanged; v1 rehearsal results do not qualify
v2 contracts. Visible evidence reuse needs an explicit determinism declaration;
final and repeatability observations always execute physically.

Use `python -m devtools.inventory_report --help` to join the manifest with
explicitly selected receipts, and `python -m devtools.benchmark_validation --help`
for the small provider-free measurement workload. CI and portable source contents
are documented in [.github/README.md](.github/README.md). Optional external
integration dependencies are declared separately from offline coverage.

## Run the original experiment

Python 3.11+ and Git 2.28+ on macOS or Linux are required; the local ledger uses
POSIX file locks. The original matrix needs no third-party Python packages,
network, credentials, or model API calls. Run from this directory:

```sh
python3 -m gossip_harness run --output runs/my-first-matrix --seeds 0
python3 -m gossip_harness inspect runs/my-first-matrix/results.json
```

The matrix performs real local Git operations, so it can take several minutes.
Every output directory must be fresh. It retains independent bare repositories,
`results.json`, and a causal `trace.jsonl` for inspection. All experimental Git
commands use local file transport and disable inherited Git configuration,
hooks, and signing. Existing user repositories are never used as fixture stores.

## What the matrix varies

| Variable | Options |
| --- | --- |
| Integration authority | One integration gate; subsystem maintainers followed by a global gate |
| Evidence transport | Broker-star anti-entropy; bounded-fanout push-pull gossip |

All four combinations receive the same deterministic patch DAG, proposal order,
acceptance requirements, and repair policy. Scripted edits exercise:

1. Independent changes that should both land.
2. Same-line text conflicts that must preserve the accepted state.
3. Changes that merge cleanly but violate a global resource-allocation invariant.
4. A branch that inherits unaccepted changes outside its subsystem scope.

The global invariant is `alpha + beta <= 10`. In the semantic-conflict fixture,
each subsystem's allocation of six passes its local checks, while the combined
allocation of twelve fails. A passing subsystem tree therefore remains distinct
from an accepted project release.

The retained seed-0 run passed all 16 cases. All variants produced the same
accepted release tree for each scenario. The maintainer path ran five validation
calls for independent edits, versus two for the single gate. Gossip required
fewer modeled delivery rounds in the maintainer cases and more peer contacts;
this establishes no wall-clock or cost advantage. Inspect the saved results
without rerunning Git:

```sh
python3 -m gossip_harness inspect results/git-lab-initial/results.json
```

The [retained run](results/git-lab-initial/README.md) includes the machine-readable
results and ordered trace. Across component suites and focused regressions,
57 unique harness tests passed, including seven matrix assertions.

Report all prepare attempts and validation calls, including maintainer,
integration, and release stages. A smaller number of root pulls does not imply a
lower total cost. Elapsed times are diagnostics and are not a fair performance
benchmark: the host, filesystem, caches and parallel case execution influence
them. Transport contacts are modeled exchanges, not bytes, socket operations or
LLM tokens. Processing order waits for notices intentionally; this matrix does
not test delivery-driven task scheduling.

## Implementation boundaries

| Module | Responsibility |
| --- | --- |
| `gossip_harness/gitstore.py` | Independent bare stores, immutable proposals, ancestry scope checks, private candidate validation, compare-and-swap promotion of the exact tested commit |
| `gossip_harness/transport.py` | Immutable hashed events, synchronous broker-star/gossip delivery, bounded batches, drops, partitions and repair |
| `gossip_harness/ledger.py` | Transactional claims and fencing epochs, dependencies, reserved credits, durable promotion intents |
| `gossip_harness/promotion.py` | Journaled bridge between the task ledger and Git, including interrupted-promotion reconciliation |
| `gossip_harness/experiment.py` | Scripted fixtures, 2×2 experiment matrix, complete operation counts and traces |
| `simulation/dissemination.py` | Earlier standalone spread model at 1,000 and 10,000 peers |

The ledger/promotion coordinator is exercised separately from the original
fixed-order topology matrix and is connected end to end in the practical pilot.
Its tests cover ownership reassignment, expired workers,
concurrent claims, reserved-budget bounds, stale Git heads, and interruptions
before/after a Git update. The pilot uses a fixed task roster with parallel
workers; it does not implement autonomous task discovery or planning.

SQLite and Git do not share a transaction. Promotion first persists an intent
that freezes the named task attempts, performs a Git compare-and-swap, then
finalizes the ledger. Recovery inspects the accepted ancestry. A local OS lock
prevents recovery from canceling a live promotion; an interrupted process releases
that lock. An unpublished rejected intent can be retried as a fresh publication
generation. All accepted-ref writes must follow this protocol; out-of-band force
updates and multi-host failover are outside this milestone.

Generic integer credits demonstrate reservations; they are not a dollar pricing
model. Lease expiry never refunds an in-flight reservation automatically. A
trusted accountant settles actual usage, including usage incurred by failed
workers.

## Limits and next experiments

- All peers share one trust domain. Topics are metadata; no ACL or signatures are
  claimed. Events carry observations, not executable commands or authority.
- Gossip is an in-process transport model. The broker baseline is a star
  anti-entropy model, not a production event-bus performance implementation.
- Repository stores are independent on one host. This tests workflow isolation,
  not host/network independence or Byzantine resilience.
- The original scripted matrix leaves rejected changes rejected. The practical
  pilot adds bounded local repair; global integration repair, maintainer
  replacement, scheduling fairness and emergent planning remain future work.
- The practical pilot adds a common patch-worker runner. Broader trials still
  need strong single-agent and orchestrated baselines, multiple repositories,
  repeated runs and realistic feature work before comparing live-agent quality.
- Preserve the gossip-versus-broker ablation. If a simpler shared-state approach
  delivers equivalent accepted work per dollar, prefer the simpler approach.

## Practical coding pilot

The [first live trial](results/practical-live-1/README.md) completed all three
variants: each passed 23 acceptance checks after one repair. Eight model requests
totaled $0.035885 in conservative usage-based accounting against the $10 cap.
The single worker needed two requests; each two-worker variant needed three.
These results demonstrate the execution path, not an advantage from gossip.

The pilot builds a Python CSV task-report CLI. Two scoped tasks implement parsing
and summary logic; trusted checks also exercise the complete CLI. It compares a
single worker implementing both modules with two parallel workers whose proposals
pass through maintainer trees, using either broker or gossip announcements.
Maintainer decisions are deterministic validation gates, not additional LLMs.
There are 13 parser checks, six summary checks, and 23 complete-project checks.

Docker must be running. The default image is the immutable Python 3.12 image
verified on this host; use `--image sha256:...` for another locally available
immutable image and repeat the rehearsal. The adapter never pulls implicitly.
Containers have read-only source and check mounts, no network or model credential,
nonroot execution, resource limits, and forced cleanup on timeout.

First run the offline rehearsal. Its known solutions are excluded from live
worker inputs. Each output directory must be fresh:

```sh
python3 -m gossip_harness pilot --output runs/my-rehearsal
python3 -m gossip_harness inspect runs/my-rehearsal/results.json
```

Live mode requires a passing rehearsal for the exact fixture, acceptance checks,
and image. The approved local credential destination is `.env.local`, using
`OPENAI_API_KEY`; the loader never executes that file or displays the key.

```sh
python3 -m gossip_harness pilot --live --output runs/my-live-trial \
  --rehearsal runs/my-rehearsal/results.json \
  --budget-usd 10 --budget-ledger runs/first-live-budget.sqlite
```

The first trial's authorized cap is **$10 across all variants and requests**.
Keep the same budget-ledger path across further attempts; a fresh output directory
does not reset that ledger. Live caps above $10 are rejected. This controls this
harness's requests, not other applications using the same API account.

The worker is pinned to `gpt-5.4-mini-2026-03-17`, low reasoning, with an 8,192-token
output ceiling. At the standard rates verified on September 30, 2026, it reserves
$0.336864 before each call: a full 400,000-token input context plus maximum output.
Reported usage is settled using integer microUSD; cached input is conservatively
priced as uncached. Unknown usage keeps the whole reservation and stops further
variants. No HTTP retries or automatic model substitution occur. Rate assumptions
must be reviewed before later trials. See the
[official model policy](https://developers.openai.com/api/docs/models/gpt-5.4-mini).

Each worker gets at most two patch attempts, with local test feedback for repair.
Leases remain alive while peers and validators work. A task is complete only after
every required proposal is ready, the combined project passes all acceptance
checks, and the exact tested release is journaled with all task completions.
Global integration failures remain failures; this pilot does not repair them.

Outputs include independent Git stores, an ordered trace, task-ledger state,
container/check receipts, request identifiers and usage, and the exact accepted
release commit. Interrupted model requests are never automatically replayed.
This is a practical smoke test of two small code-generation tasks, not evidence
that gossip makes coding faster or cheaper. Model workers propose patches through
structured output; they do not yet have an interactive shell/tool loop.
Gossip currently carries proposal announcements to integration gates. Workers do
not yet consume shared semantic discoveries to revise their plans, so this pilot
cannot test that proposed benefit of gossip.

| Pilot module | Responsibility |
| --- | --- |
| `gossip_harness/worker.py` | Provider-neutral request/result contract and bounded Responses adapter |
| `gossip_harness/pilot.py` | Claims, lease heartbeat, shared reservations, repair attempts, scoped integration and atomic task publication |
| `gossip_harness/sandbox.py` | Docker validation and reproducible execution receipts |
| `gossip_harness/pilot_fixture.py` | Versioned small project, fixed task contract and external acceptance checks |

## Expanded experiments

Two follow-up studies separate evidence usefulness, communication, and release
correctness. Both evolve the saved accepted CSV project. They remain small
internal benchmarks, not evaluations across industrial repositories.

The [retained offline rehearsals](results/expanded-rehearsals-1/README.md) passed
all four discovery setups and all 24 recovery cases. The user explicitly approved
both live studies on September 30, 2026, with up to $50 additional budget if
needed. The [completed live studies](results/expanded-experiments-1/README.md)
used 29 additional model requests and $0.289383 in conservative token accounting.
Cumulative accounting is $0.325268, so the active $10 hard cap was sufficient.

Across three repetitions, the single worker passed once, the isolated team twice,
and the shared-note team three times. Gossip reproduced the shared team's three
successes using exact response replay. The shared-versus-isolated difference is
one paired outcome; this small result does not establish superiority. Single and
team setups also differ in decomposition and permitted total repair attempts.
Seven of nine initial parser implementations failed duration checks, illustrating
correlated errors across independent samples.

The live recovery model repaired the deliberately incompatible changes in one
request. Its frozen patch passed all 24 delivery/release cases. These are 24
protocol replays of one repair, not 24 independent model successes. The retained
record includes failures, accepted project exports, exact tested Git commits,
source snapshots, response caches, and reconciled usage accounting.

**Shared discoveries.** Workers add hour/minute estimates and dependency-aware
ready/blocked summaries. Each repetition generates two source-citing discovery
notes from the full repository and specification. The single worker gets both
notes; isolated team members get their own; the shared team gets both. The same
notes are reused across arms. Quotes bind notes to source bytes but do not prove
their interpretations correct. All workers already have the full requirements.

The broker and gossip arms wait for identical evidence before coding. Their
normalized prompts must match exactly, and gossip replays the broker's recorded
response, including known failures. Thus they are a transport control with equal
coding outcomes by construction. The live quality comparison is single versus
isolated team versus shared team, with three exploratory repetitions. Raw test
receipts are preserved; only unittest's elapsed-time line is normalized in repair
feedback to make exact response replay possible.

**Conflicting integration and recovery.** Scripted branches add unknown estimates
and averaging under divergent assumptions. They pass local checks and merge
cleanly, but the full project fails. One bounded model repair receives the actual
failing tree, current contract, and test feedback. Its passing patch is frozen
and replayed through broker/gossip delivery under healthy, broker-isolated,
partition-and-heal, and lossy schedules, over three seeds. Stale epochs and
wrong-base notices are rejected. These are deliberate fault injections and
protocol replays, not 24 independent model repairs.

Rehearse both studies before spending:

```sh
python3 -m gossip_harness discovery --output runs/discovery-rehearsal --repetitions 1
python3 -m gossip_harness recovery --output runs/recovery-rehearsal
```

The authorized live continuation retains the original shared ledger and **$10
cumulative hard cap**, including the first pilot. The additional $50 authorization
does not automatically increase the runner's cap; these studies will use the
existing lower limit unless more is needed.
Use fresh output directories; do not reset that ledger. The verified rehearsal
paths for this session are `runs/discovery-rehearsal-2/results.json` and
`runs/recovery-rehearsal-1/results.json`.

```sh
python3 -m gossip_harness discovery --live --output runs/discovery-live \
  --rehearsal runs/discovery-rehearsal/results.json --repetitions 3 \
  --budget-ledger runs/first-live-budget.sqlite --budget-usd 10
python3 -m gossip_harness recovery --live --output runs/recovery-live \
  --rehearsal runs/recovery-rehearsal/results.json \
  --budget-ledger runs/first-live-budget.sqlite --budget-usd 10
```

Actual accounting charges frozen discovery production once and response replay
zero times. Comparative arm estimates must allocate the discovery production and
original coding usage to each arm that logically uses them; replay's zero
incremental API charge is not an efficiency finding. Communication remains an
in-process model, with trusted single-host ledger authority.

## Candidate swarm pilot

The [swarm proposal](swarm.html) investigates four independent inexpensive
implementations, pooled cross-candidate tests, and a stronger reviewer proposing
discriminating checks. Compare fixed selection and reviewer-assisted selection
on the same frozen candidates and baseline evidence, alongside strong-single
and cheap-sequential baselines. Measure whether a correct candidate exists,
whether selection finds it, and the full cost of failed candidates and reviews.

The [completed live pilot](results/swarm-live-1/README.md) ran all 16 task-runs
with 128 API requests. Strong single passed 16/16 for $0.157078 in attributed
API-token estimates; the cheap public-feedback baseline passed 14/16 for
$0.049400; fixed-pool selection passed 16/16 for $0.370771; reviewer-assisted
selection passed 16/16 for $1.158638. Matched fuzz also passed 16/16. Physical
requests cost $1.365116 once, bringing cumulative conservative accounting to
$1.690384 under the unchanged $10 cap. Attributed policy costs reuse the same
candidate pool and must not be added together as physical spending.

The pool contained seven failing implementations out of 64, but each task-run
had a passing candidate. Reviewer extras distinguished candidates on 18 of 91
executed tests, exactly the count for matched fuzz, and changed no winners.
The host rejected 19 incorrect expected answers across initial and reviewer
test proposals, two invalid reviewer inputs, and nine malformed initial test
batches. One of 240 final task-run inputs also appeared in visible evidence.

A [post-hoc public-gate audit](results/swarm-live-diagnostics-1.json) found that
the frozen tie order plus public tests alone would have selected the identical
winner in all 16 cases. This was not a preregistered comparison arm. The pilot
therefore verifies the harness and reveals a task-difficulty ceiling; it does
not establish incremental value from pooled tests, a stronger reviewer, or
gossip. The strong single worker is the simpler observed cost/quality choice.
Next compare sequential repair with the same validation evidence, charging for
its production, before adding candidates; then use harder repository changes
and evaluate test validity without privileged reference answers.

The `swarm` command implements this pilot on eight bounded JSON-function tasks
across configuration, interval scheduling, and event-ledger families, with two
repetitions per task. These are synthetic tasks, not eight industrial projects.
Five policies compare a strong single worker (at most two public-feedback
attempts), a cheap sequential worker (at most four), and three selectors over the
same four independently generated cheap candidates. The selectors use fixed
pooled evidence, matched extra fuzz cases, or stronger-reviewer test proposals.
Baselines stop after passing public examples; final outcomes never enter repair.

Generated tests are data only. The host rejects incorrect expected labels using
a trusted reference oracle, without correcting the labels. Selection is therefore
explicitly **oracle-assisted**. Every candidate faces the same baseline evidence;
reviewer and extra-fuzz executions are capped to the same accepted test count.
Selection requires passing every public example, then ranks distinct input cases
passed, breaking ties with a frozen anonymous order. Similar inputs can still
overweight a behavior; this is not semantic test deduplication.

The new black-box adapter sends only inputs to a Docker container and compares
returned JSON with expected values on the host. Winner decisions are saved before
final hidden evaluation. Fresh Python processes isolate ordinary memory between
cases, but cases share a container's temporary filesystem; this is not a hardened
adversarial VM boundary. An accepted release binds the exact tested source hash
to its promoted Git commit. The earlier pilots retain their original cooperative
validation design.

Rehearse all eight tasks with scripted failures and repairs before live use:

```sh
python3 -m gossip_harness swarm --output runs/swarm-rehearsal --repetitions 1
python3 -m gossip_harness swarm --live --output runs/swarm-live \
  --rehearsal runs/swarm-rehearsal/results.json --repetitions 2 \
  --budget-ledger runs/first-live-budget.sqlite --budget-usd 10
```

The rehearsal must match the exact source, fixtures, image and model profiles.
Mini and strong profiles are pinned to `gpt-5.4-mini-2026-03-17` and
`gpt-5.4-2026-03-05`, both with low reasoning and 4,096 output tokens. Each policy
has a $6 reservation ceiling and a 900-second deadline for starting new requests;
in-flight calls can finish later. The shared cumulative ledger remains capped at
$10 for this run, within the user's additional $50 authorization. Unknown usage
halts the run and retains its reservation. No implicit retries or model changes
occur. The swarm CLI supports caps up to $50, but it cannot silently change an
existing ledger's cap.

Report candidate coverage, selected success, selector regret, invalid/duplicate
test proposals, visible/hidden input overlap, and full attributed policy cost.
The physical API charge counts reused candidates once; each comparison policy
bears their full generation cost. Runtime and container counts are diagnostics,
not an equal-latency benchmark. As a predeclared diagnostic, examine nested
candidate subsets of one, two and four using prefixes of each case's frozen
anonymous order; do not use final outcomes to choose those subsets. Giving the
strong baseline the pool's evidence is deferred from this first five-policy
pilot. These comparisons do not isolate gossip as a cause of coding quality.

The matched fuzz arm is a diagnostic control: its test count depends on the
reviewer's eligible output, while its attributed API cost excludes the reviewer
call. Do not interpret that figure as the price of an independently runnable
policy. API-token estimates also exclude reference-oracle development and local
compute. Pool comparisons change prompts, evidence and repair policy as well as
candidate count.

Retain and independently reconcile a finished study with:

```sh
python3 retain_swarm.py --run runs/swarm-live --output results/swarm-live \
  --accounting-ledger runs/first-live-budget.sqlite \
  --rehearsal runs/swarm-rehearsal/results.json
```

The retainer reproduces selections from visible matrices, checks that final
evaluation follows the winner freeze, verifies candidate and promoted source
hashes, and reconciles every request with the shared ledger. It exports failed
candidates alongside accepted ones, response records, source snapshots, and
per-policy cost and test-signal diagnostics. It does not copy credentials.

| Swarm module | Responsibility |
| --- | --- |
| `swarm_fixture.py` | Frozen task contracts, public/fuzz/final cases and reference oracles |
| `blackbox_validator.py` | Bounded input/output execution with host-side expected answers |
| `selection.py` | Proposal validation, provenance, input deduplication and deterministic selection |
| `swarm_experiment.py` | Candidate generation, baselines, evidence controls, winner freeze and publication |
| `retain_swarm.py` | Independent evidence, source and accounting audit plus durable exports |

Focused offline checks are reproducible without a model credential:

```sh
.venv/bin/python -m devtools.verify tests.test_swarm_fixture tests.test_selection \
  tests.test_model_profiles tests.test_swarm_control tests.test_retain_swarm
.venv/bin/python -m devtools.verify --lanes docker tests.test_blackbox_validator.BlackboxDockerTests
```

## Sustained quality study

The [registered study plan](sustained-study-plan.json) makes sustained project
quality the primary question: can a system finish the complete requirements,
preserve earlier behavior, and continue after failed attempts and a controller
handoff? The implementation compares `strong-single`, `cheap-sequential`, and
`reviewed-portfolio` on two persistent SQLite applications: a workflow queue and
an inventory/reservation service. Each has three cumulative milestones. Two live
repetitions produced **12 project trajectories and 36 milestones**.

The [frozen primary results](results/sustained-live-1/results.json) and
[descriptive analysis](analysis/sustained-live-final.json) report:

| Policy | Whole projects accepted | Final requirement instances passed |
| --- | ---: | ---: |
| Reviewed cheap-model portfolio | 4/4 | 54/54 |
| Strong single worker | 3/4 | 53/54 |
| Cheap sequential worker | 3/4 | 52/54 |

Requirement instances count each requirement separately within each project run;
they are not 54 independent projects. All 36 milestones passed their visible
checks, and all 12 trajectories completed verified process handoffs. Final hidden
checks rejected two workflow implementations despite that visible completion:

- `workflow-strong-single-1` returned `{"error":"invalid"}` for an invalid
  second inner batch command, omitting the required `"at":1` error position.
- `workflow-cheap-sequential-1` claimed a lower-priority pending job instead of
  reclaiming the expired higher-priority job. This independently fails the
  unambiguous lease-selection contract. It also recorded only one finish audit
  event per job, while the hidden expectation requires one per job/token. The
  written Q3 contract says “FIRST successful finish” without explicitly defining
  that scope; Q2's token-scoped receipts support the hidden interpretation, but
  the wording is ambiguous. The frozen score remains unchanged, and this audit
  mismatch should not be treated as an unambiguous implementation defect.

The portfolio's primary result is an encouraging quality signal within **two
small synthetic applications**. It combines candidate diversity, stronger review
and additional calls; this experiment cannot attribute its result to any one of
those components. **The portfolio's 4/4 primary score is not evidence of complete
robustness:** every portfolio output failed at least one separately reported,
source-informed probe below. Further trials should strengthen independent
verification and clarify the finish-audit scope before drawing broader quality
conclusions.

Each project has 24 visible and 30 distinct hidden scenarios. Fixed CLI adapters
start a fresh subprocess for each command while preserving the scenario database.
Builders receive the same cumulative visible checks and repair feedback. The
portfolio begins with four independent cheap-model candidates; a stronger
reviewer inspects their source and execution evidence, then requests repairs or
selects a complete candidate. Reviewers cannot change trusted tests, and this
study does not use oracle-validated reviewer test proposals. Hidden checks run
only after the trajectory's model work has ended and never become repair
feedback. Earlier hidden defects may be repaired at later milestones; historical
hidden scores are retrospective diagnostics.

Primary outcomes are final requirement coverage and whole-project completion:
all three milestones must complete and the exact release must pass every final
cumulative visible and hidden check. Requirement coverage means all hidden cases
assigned to a requirement pass. Additional measures count previously passing
checks broken by a revision, repairs, premature completion claims, and stagnation
(repeated unchanged source or failure sets). The controller records a worker's
premature completion claim separately from its own refusal to accept unfinished
work. Cost and elapsed time are secondary diagnostics and safety limits; eventual
improvement in either remains a hypothesis.

After milestone two, the controller settles requests, writes a durable Git-bound
checkpoint, exits, and resumes under a verified **new process ID**. This is a real
process handoff at a settled boundary, not recovery from a crash or an ambiguous
in-flight API request. Model workers remain bounded, stateless patch generators;
continuity belongs to the persistent controller and its saved notes, code and
requirements. This study does not isolate a causal benefit from gossip. The
portfolio also combines candidate diversity, a stronger reviewer and additional
calls, so those effects cannot be separated by this comparison alone.

Docker and a passing rehearsal for the exact source/fixture/image contract are
required. Use fresh output directories; rehearsals use known implementations and
make no API calls:

```sh
python3 -m gossip_harness.sustained_experiment run \
  --output results/sustained-rehearsal-example
python3 -m gossip_harness.sustained_experiment run --live \
  --output results/sustained-live-example \
  --rehearsal results/sustained-rehearsal-example/results.json \
  --budget-ledger runs/first-live-budget.sqlite \
  --budget-units 20000000 --repetitions 2
```

The [audited budget increase](runs/sustained-budget-increase.json) raises the
existing cumulative ledger ceiling to **$20**, within the user's prior approval.
It preserves the **$1.690384** already accounted for before this study; it does not
reset spending. Keep the same ledger path. Each run retains its own `results.json`,
request and validation receipts, source snapshot, checkpoints, and accepted
exports. The completed primary run made **118 API requests**, accounting for
**$6.562429** of new usage and **$8.252813 cumulative** under the $20 cap. These
are conservative usage-based estimates. The
[independent retained-evidence audit](results/sustained-live-1-audit.json) passed,
reconciled all 118 requests and $6.562429 of new usage, and found no unsettled
reservations or pending promotions. Evidence consistency does not prove the
implementations satisfy every written requirement.

The retained-evidence audit checks source and Git bindings, completion decisions,
feedback, process checkpoints, and ledger accounting without executing candidate
code. A separate descriptive analysis measures repair paths, reviewer decisions,
and selected-code regressions between milestones:

```sh
python3 -m gossip_harness.sustained_audit \
  --run results/sustained-live-example \
  --accounting-ledger runs/first-live-budget.sqlite \
  --output analysis/sustained-audit-example.json
python3 analysis/sustained_quality.py \
  --run results/sustained-live-example \
  --output analysis/sustained-quality-example.json
```

Two supplemental diagnostics run only after the primary study finishes. The
all-stage pool diagnostic applies each milestone's frozen cumulative hidden suite
to the latest retained version of all four portfolio slots. It separates whether
a passing implementation existed from whether selection found one; it is not a
new selection policy. Matching selected source reuses its retained hidden
receipt, while unselected source executes in Docker. Historical milestone-three
receipts can differ from final acceptance receipts if code is nondeterministic;
the primary result is never replaced.

The [completed all-stage pool diagnostic](results/sustained-pool-all-stages-1.json)
found an eligible candidate passing the milestone's hidden suite in **12/12 latest
pools**, and the selected candidate also passed in **12/12**, yielding zero
selection regret on that suite. These are three observations within each of four
portfolio trajectories, not 12 independent projects. The diagnostic used 36 fresh
Docker validations and 12 retained historical receipts, with zero API calls.
Unselected earlier candidates were not carried through later milestones, so
these results do not establish their counterfactual whole-project quality.

```sh
python3 -m gossip_harness.sustained_pool_diagnostic \
  --run results/sustained-live-example \
  --output analysis/sustained-pool-example --all-stages
```

The [workflow probes](analysis/sustained-posthoc-workflow-1/cases.json) and
[inventory probes](analysis/sustained-posthoc-inventory-1/cases.json) were chosen
after inspecting candidate source, then independently checked against the frozen
written contracts and trusted references. They contain three workflow cases and
two correlated inventory cases covering one inventory defect family. Apply the
same project-specific probes to every final attempt that reached milestone three,
including unaccepted attempts; disclose exclusions and denominators. These are
source-informed supplemental quality checks, not an unbiased estimate of policy
quality. Neither diagnostic feeds repairs, changes releases, or relabels frozen
primary acceptance.

The [completed supplemental run](results/sustained-review-probes-1/results.json)
applied five cases covering four defect families to all 12 final attempts:
**30 case executions, no exclusions**. These results stay separate from the
primary table:

| Policy | Workflow outputs passing all 3 probes | Inventory outputs passing both correlated probes |
| --- | ---: | ---: |
| Reviewed cheap-model portfolio | 0/2 | 0/2 |
| Strong single worker | 0/2 | 2/2 |
| Cheap sequential worker | 0/2 | 0/2 |

All six workflow implementations failed the specified typed-error check for
`finish` with `outcome: []`. Both cheap and both portfolio inventory outputs
returned `internal` for a non-string inner operation where the contract requires
`invalid`; both strong inventory outputs passed. Thus all four portfolio outputs,
all four cheap outputs and the two strong workflow outputs failed at least one
supplemental case. The inventory checks confirmed preserved state/audit and key
reuse despite the incorrect error response; they did not reveal data corruption.
The original static review included a strong inventory implementation as a
predicted passing control. Because the cases were selected after source
inspection, these counts demonstrate missed contract behavior, not an unbiased
replacement ranking of the policies. The frozen primary scores remain unchanged.

The probe runner verifies both exact case-file hashes and the complete study
roster before Docker execution. It makes no API calls and never imports candidate
code on the host. Output must be a fresh directory outside the retained study:

```sh
python3 analysis/run_sustained_review_probes.py \
  --run results/sustained-live-example \
  --output analysis/sustained-review-probes-example \
  --cases workflow e77dd4f91e797f0e63f59c35d447a8d2fd66128e9b0966a83cc1b8da4f6901bf \
    analysis/sustained-posthoc-workflow-1/cases.json \
  --cases inventory a5fe99cc4cb518cfb9e3f3da95f65b560b6a94b80ced6b1e31bfdf567fe24537 \
    analysis/sustained-posthoc-inventory-1/cases.json
```

## Adaptive verification study

The [registered plan](verification-study-plan.json) extends the quality question
to two synthetic maintenance applications, a build graph and a booking calendar,
with four cumulative milestones. `strong-reviewed`, `cheap-reviewed`, and
`portfolio-reviewed` share the same strong reviewer, repair limits, and acceptance
rule; the portfolio starts with four cheap-model implementations instead of one.
Three repetitions per project and policy plan **18 trajectories and up to 72
milestones**. The [current live run](results/verification-live-1/results.json) is
in progress; consult the [methods and results report](verification-pilot.html)
and [progress report](http://127.0.0.1:4178/) for its current status.

Reviewers propose bounded JSON checks. A trusted host oracle verifies their
expected answers without correcting rejected labels, then admitted checks run
against every candidate. Retained checks are revalidated at later milestones;
changed contracts can retire them. Completion requires all four milestones and
the exact final source passing every cumulative public, active generated, and
private check. Private results never enter repair feedback.

At milestone two, the supervisor sends a real SIGKILL after a reviewer response
is durably recorded and before its usage is settled. A new process replays that
saved response and reconciles settlement without another provider dispatch.
Unknown provider outcomes remain reserved and stop execution. Before milestone
four, an independent Git history supplies a trusted policy update and exercises
rejection of stale integration evidence. This covers a specific recovery boundary
and policy merge, not arbitrary failure recovery or autonomous conflict resolution.

Run the development gates above first. Use the pinned `.venv` runtime and the
locally available immutable Docker image throughout qualification and execution.
Every example output path below must be fresh; preserve earlier runs. The six-case
rehearsal and retained-evidence audit make **zero API calls**:

```sh
.venv/bin/python -m gossip_harness.verification_experiment run \
  --output results/verification-rehearsal-example
.venv/bin/python -m gossip_harness.verification_audit \
  --run results/verification-rehearsal-example \
  --output analysis/verification-rehearsal-audit-example.json
```

Live mode requires a complete passing rehearsal for the exact source, fixture,
plan, runtime, image, and model contract. It uses the approved `.env.local`
credential and the existing cumulative ledger. The following command incurs
API charges and documents this study's authorized workflow; it does not authorize
future paid runs:

```sh
.venv/bin/python -m gossip_harness.verification_experiment run --live \
  --output results/verification-live-example \
  --rehearsal results/verification-rehearsal-example/results.json \
  --repetitions 3 --budget-units 40000000 \
  --budget-ledger runs/first-live-budget.sqlite
```

The [audited cap change](runs/verification-budget-increase.json) sets this study's
shared cumulative ceiling to **$40**, preserving all prior charges. Earlier
sections retain their historical caps. Keep the same ledger path: fresh output
directories do not reset spending, and the constructor rejects a silently changed
cap. Usage figures are conservative token-based estimates, not provider invoices;
they exclude local compute, oracle development, and human work.

After the entire live study finishes, audit its retained evidence and generate a
descriptive summary. These commands do not execute candidate code or call models:

```sh
.venv/bin/python -m gossip_harness.verification_audit \
  --run results/verification-live-example \
  --accounting-ledger runs/first-live-budget.sqlite \
  --output analysis/verification-live-audit-example.json
.venv/bin/python analysis/summarize_verification.py \
  results/verification-live-example \
  --output analysis/verification-live-summary-example.json
```

For an in-progress run, `verification_audit --finalized-only` can inspect finalized
cases, but cannot certify the whole study. Partial summaries label evaluated
denominators and charges not yet attributed to evaluated trajectories. A retained
evidence audit checks consistency and accounting; it does not prove every written
requirement is satisfied.

The following supplementary diagnostics make zero API calls and keep primary
scores and releases unchanged. Both live-study diagnostics require the **entire
finished 18-trajectory roster**, with no active, censored, or unexecuted work, and
fresh output directories outside the frozen study. They verify frozen source,
runtime, fixture, and receipt bindings before using Docker:

```sh
.venv/bin/python analysis/run_verification_pool_diagnostic.py \
  --run results/verification-live-example \
  --output analysis/verification-final-pool-example
.venv/bin/python analysis/run_verification_persistence_diagnostic.py \
  --golden-rehearsal analysis/verification-persistence-golden-example
.venv/bin/python analysis/run_verification_persistence_diagnostic.py \
  --run results/verification-live-example \
  --qualification analysis/verification-persistence-golden-example \
  --output analysis/verification-persistence-example
```

The final-pool diagnostic tests the latest retained portfolio candidates against
the frozen private suite. Matching selected-source receipts are reused for this
descriptive purpose; other distinct sources execute in Docker. It distinguishes
candidate coverage from selection quality after unequal repairs, not four
independent end-to-end projects. The persistence diagnostic first requires a
passing, exactly matched golden qualification, which can run before live work
finishes. Its four stories carry the same database through all four selected code
versions and fresh CLI processes, including primary failures that retained all
four versions; earlier stops are explicitly excluded.

This study is **oracle-assisted and unequal in compute**: portfolio width changes
initial call count, reviewer context, and candidate-aware generated evidence.
Two synthetic application identities and three repetitions do not establish
production superiority, a best-available-model result, or a causal benefit from
gossip. Quality and completion are primary; cost and elapsed time remain secondary.

## Research and review

- `research.html`: initial evidence review and proposed harness architecture.
- `investigation.html`: distributed-Git follow-up and measured local findings.
- `pilot.html`: practical coding trial, results, and the next experiment.
- `experiments.html`: shared-discovery and integration-recovery follow-up studies.
- `swarm.html`: primary-source research and a proposed candidate-selection study.
- `swarm-pilot.html`: measured candidate-selection pilot and its limitations.
- [sustained-pilot.html](sustained-pilot.html): cumulative project quality,
  persistence, retained-evidence audit, and separate supplemental diagnostics.
- [verification-pilot.html](verification-pilot.html): adaptive verification,
  persisted-response recovery, and the current maintenance-study status.
- `.progress-report/project.json`: locator for the independent durable progress
  report, review checkpoints and continuation handoff.

Primary workflow references:

- [Git distributed workflows](https://git-scm.com/book/en/v2/Distributed-Git-Distributed-Workflows)
- [Linux development process and next trees](https://docs.kernel.org/process/2.Process.html)
- [Kernel rebasing and merging guidance](https://docs.kernel.org/maintainer/rebasing-and-merging.html)
- [Git workflow manual](https://git-scm.com/docs/gitworkflows)
