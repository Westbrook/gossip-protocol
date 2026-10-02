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
Three repetitions per project and policy produced **18 evaluated trajectories**,
with no censored or unexecuted cases. The [finished live run](results/verification-live-1/results.json)
completed **64 of 72 milestones** and accepted **3 of 18 projects**. The
[descriptive summary](results/verification-live-1/descriptive-summary.json)
reports the following primary results; the [methods and results report](verification-pilot.html)
provides the interpretation.

| Policy | Accepted projects | Completed milestones | Private requirement instances satisfied | Estimated API cost |
| --- | ---: | ---: | ---: | ---: |
| `strong-reviewed` | 3/6 | 24/24 | 86/93 | $4.292283 |
| `cheap-reviewed` | 0/6 | 24/24 | 77/93 | $4.562591 |
| `portfolio-reviewed` | 0/6 | 16/24 | 54/93 | $6.833285 |

Each requirement denominator covers the six evaluated trajectories, including
incomplete projects. Requirement coverage is partial credit; project acceptance
requires every final gate. All six cheap-reviewed projects completed their
milestones but failed private checks. The wider portfolio did not improve
completion or final acceptance in this sample. Builders used pinned GPT-5.4 or
GPT-5.4-mini snapshots; all model calls, including the shared GPT-5.4 reviewer,
used **low reasoning effort**.

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
The live run recorded 17 kills and successful resumptions among trajectories that
reached the fault, and 15 stale-evidence integration checks among those that
reached the policy merge.

Run the development gates above first. The primary study's
[frozen contract](results/verification-live-1/preregistered.json) records CPython
3.14.6 at `/opt/homebrew/opt/python@3.14/bin/python3.14`; its plain `python3`
command resolved to this executable, rather than the development `.venv`.
Use the same controller runtime and locally available immutable Docker image
throughout a rehearsal and its live execution. A different runtime requires its
own matching rehearsal. Every example output path below must be fresh; preserve
earlier runs. The six-case rehearsal and retained-evidence audit make **zero API
calls**:

```sh
STUDY_PYTHON=/opt/homebrew/opt/python@3.14/bin/python3.14
"$STUDY_PYTHON" -m gossip_harness.verification_experiment run \
  --output results/verification-rehearsal-example
"$STUDY_PYTHON" -m gossip_harness.verification_audit \
  --run results/verification-rehearsal-example \
  --output analysis/verification-rehearsal-audit-example.json
```

Live mode requires a complete passing rehearsal for the exact source, fixture,
plan, runtime, image, and model contract. It uses the approved `.env.local`
credential and the existing cumulative ledger. The following command incurs
API charges and documents this study's authorized workflow; it does not authorize
future paid runs:

```sh
"$STUDY_PYTHON" -m gossip_harness.verification_experiment run --live \
  --output results/verification-live-example \
  --rehearsal results/verification-rehearsal-example/results.json \
  --repetitions 3 --budget-units 40000000 \
  --budget-ledger runs/first-live-budget.sqlite
```

The [audited cap change](runs/verification-budget-increase.json) sets this study's
shared cumulative ceiling to **$40**, preserving all prior charges. This completed
study used **$15.688159** in estimated API usage across 289 provider invocations,
bringing cumulative usage to **$23.940972**. Earlier sections retain their
historical caps. Keep the same ledger path: fresh output directories do not reset
spending, and the constructor rejects a silently changed
cap. Usage figures are conservative token-based estimates, not provider invoices;
they exclude local compute, oracle development, and human work.

After the entire live study finishes, audit its retained evidence and generate a
descriptive summary. These commands do not execute candidate code or call models:

```sh
"$STUDY_PYTHON" -m gossip_harness.verification_audit \
  --run results/verification-live-example \
  --accounting-ledger runs/first-live-budget.sqlite \
  --output analysis/verification-live-audit-example.json
"$STUDY_PYTHON" analysis/summarize_verification.py \
  results/verification-live-example \
  --output analysis/verification-live-summary-example.json
```

The completed [independent audit](results/verification-live-1/independent-audit.json)
passed and certified the retained evidence for all 18 trajectories. It reconciled
289 requests across 67 billing tasks, with zero unsettled reservations or pending
promotions. This certification covers evidence consistency and accounting;
it does not prove that every written requirement is satisfied.

For an in-progress run, `verification_audit --finalized-only` can inspect finalized
cases, but cannot certify the whole study. Partial summaries label evaluated
denominators and charges not yet attributed to evaluated trajectories.

The following supplementary diagnostics make zero API calls and keep primary
scores and releases unchanged. The live-study diagnostics require the **entire
finished 18-trajectory roster**, with no active, censored, or unexecuted work, and
fresh output directories outside the frozen study. They verify frozen source,
runtime, fixture, and receipt bindings before using Docker:

```sh
"$STUDY_PYTHON" analysis/run_verification_pool_diagnostic.py \
  --run results/verification-live-example \
  --output analysis/verification-final-pool-example
"$STUDY_PYTHON" analysis/run_verification_persistence_diagnostic.py \
  --golden-rehearsal analysis/verification-persistence-golden-example
"$STUDY_PYTHON" analysis/run_verification_persistence_diagnostic.py \
  --run results/verification-live-example \
  --qualification analysis/verification-persistence-golden-example \
  --output analysis/verification-persistence-example
```

The eligibility diagnostic is specific to the retained finding in
`verification-live-1`; it is not a general utility for a new run. Its command
reads that actual study and still requires a fresh output directory:

```sh
"$STUDY_PYTHON" analysis/run_verification_eligibility_diagnostic.py \
  --run results/verification-live-1 \
  --output results/verification-eligibility-example
```

The final-pool diagnostic tests the latest retained portfolio candidates against
the frozen private suite. Matching selected-source receipts are reused for this
descriptive purpose; other distinct sources execute in Docker. It distinguishes
candidate coverage from selection quality after unequal repairs, not four
independent end-to-end projects. The persistence diagnostic first requires a
passing, exactly matched golden qualification, which can run before live work
finishes. Its two stories per application carry the same database through all
four selected code versions and fresh CLI processes, including primary failures
that retained all four versions; earlier stops are explicitly excluded.

The completed [final-pool diagnostic](results/verification-pool-diagnostic-2/results.json)
found no fully passing private-suite implementation among the 12 retained
candidate versions in the three portfolios that reached the final milestone.
It reused three selected-source receipts and physically evaluated nine other
sources. The other three portfolios stopped earlier and have no final pool.

After a passing [golden qualification](results/verification-persistence-golden-1/results.json),
the [persistence diagnostic](results/verification-persistence-diagnostic-2/results.json)
passed all 30 story executions across 15 eligible trajectories, including all
six stories for the three primary accepted projects. Three trajectories that
stopped early were excluded. This supports continuity of persisted data for the
four story designs; primary quality failures remain failures.

A separate [eligibility diagnostic](results/verification-eligibility-diagnostic-1/results.json)
exposed a harness confound in an earlier milestone: an unchanged repair proposal
incorrectly made retained candidate B ineligible, although that source passed
all 36 private checks through milestone three; retained candidate C passed 27/36.
The reviewer also never requested acceptance of B. A source passing that
milestone's private suite was therefore available, while the final-pool result
concerns different trajectories that reached milestone four. This does not
establish that fixing eligibility would make the reviewer accept B or complete the next
milestone. The measured portfolio acceptance remains 0/6.

This study is **oracle-assisted and unequal in compute**: portfolio width changes
initial call count, reviewer context, and candidate-aware generated evidence.
Two synthetic application identities and three repetitions do not establish
production superiority, a best-available-model result, or a causal benefit from
gossip. Quality and completion are primary; cost and elapsed time remain secondary.

## Comparing policies and continuing real-module maintenance

The [historical comparison](results/verification-comparison-2/summary.json)
derives quality ranks, within-application win/draw/loss counts, and observed
terminal durations from the certified 18-trajectory study. Acceptance and
requirement coverage have separate rankings. Six trajectories per policy and
two application identities do not justify a calibrated Elo rating; the 18
cross-run comparisons per policy pair are correlated derived observations.
Accepted-only timing always includes its denominator, and a fast incomplete
run is not treated as a speed victory.

The separate [continuation plan](continuation-study-plan.json) freezes this
repository's actual `transport.py` into isolated candidate repositories. Two
cumulative milestones add reproducible snapshots and strict, atomic restore.
Two repetitions compare a strong maintainer/reviewer with the same controller
plus two independent cheap test scouts. Scouts propose data-only probes whose
expected outputs must pass a trusted oracle gate. This measures an additional
evidence policy, not equal compute or the causal effect of gossip transport.

The new controller preserves valid retained source after rejected or unchanged
proposals and escalates stalled review within explicit call limits. Acceptance
still requires executed evidence and an explicit reviewer decision. All four
terminal sources freeze before any private evaluation. Candidate execution
uses the pinned Docker validator; provider requests retain journals and budget
reservations. Unknown outcomes stop without an automatic retry.

Use the same Python executable for rehearsal and live execution, with fresh
output paths. Live work also requires the registered, quiescent shared ledger
and matching authorized budget; these examples do not reset either:

```sh
.venv/bin/python -m gossip_harness.continuation_experiment run \
  --output results/continuation-rehearsal-example
.venv/bin/python -m analysis.audit_continuation \
  --run results/continuation-rehearsal-example \
  --output results/continuation-rehearsal-example/independent-audit.json
.venv/bin/python -m gossip_harness.continuation_experiment run --live \
  --output results/continuation-live-example \
  --rehearsal results/continuation-rehearsal-example/results.json \
  --budget-ledger runs/first-live-budget.sqlite
```

New timing spans distinguish physical provider requests, validation, active
trajectory work, and the wait for the whole-cohort private-test barrier.
Concurrent and nested durations must not be summed as elapsed wall time.

The [complete zero-API rehearsal](results/continuation-rehearsal-1/results.json)
passed all four trajectories and its independent audit. The subsequent
[live study](results/continuation-live-1/results.json) and
[independent audit](results/continuation-live-1/independent-audit.json) also
finished, with 24 physical provider requests and no unsettled reservations or
pending promotions. The [audited comparison](results/continuation-comparison-1/summary.json)
reports:

| Policy | Accepted | Requirement coverage | Median active time | API estimate, both runs |
| --- | --- | --- | --- | --- |
| Strong maintainer/reviewer | 2/2 | 100% | 3.80 minutes | $0.684577 |
| Same maintainer/reviewer plus cheap scouts | 2/2 | 100% | 4.58 minutes | $0.879140 |

Both policies completed all four assigned milestones. Active time includes model
work, Git/controller work, stage checks and final evaluation; deliberate
whole-cohort barrier and evaluation-queue waits are reported separately. These
times are not comparable to the older cohort's different task and timing
contract. Total incremental API usage was $1.563717, bringing the shared ledger
to $25.504689 of its unchanged $40 cap.

The scouts supplied 12 admitted case proposals. Their seven rejection entries
comprise three malformed response batches and four rejected parsed proposals;
they are not seven individual rejected tests. No admitted scout check failed
against the observed public-passing implementations, and no live stage needed
a repair or escalation. Because those tests were available before the first
build, this does not rule out a preventive benefit. It also does not demonstrate
a quality advantage: this one-module pilot reached a ceiling, with only two
runs per policy. The forced rehearsal controls establish continuation behavior;
the live runs did not exercise stalled-review recovery.

The separately reviewed [scout-signal memo](analysis/continuation-scout-signal.json)
binds 38 retained inputs and distinguishes 12 new admissions from 16 passing
probe-stage observations. It also identifies weak requirement labels: correct
expected answers do not establish that a probe meaningfully exercises its
claimed behavior. A next benchmark should pair harder, multi-module maintenance
with a preregistered fault bank to measure incremental defect detection.

## Evidence-frontier benchmark

The [registered benchmark](benchmark-study-plan.json) addresses the previous
pilot's quality ceiling with two maintained, three-module SQLite applications:
simultaneous build-graph rename/atomic edits, and booking exchanges/transactional
waitlist settlement. Both retain earlier behavior through two new milestones.

The main comparison gives cheap models four initial opportunities either as a
sequential revision chain or as independent branches. Both retain all four
checkpoints for the same stronger reviewer and bounded repair policy. Two
repetitions per application provide four paired comparisons. One stronger-model
anchor per application provides additional context; it is not compute-matched.
These are centrally instrumented candidate-generation policies, not a claim
that gossip itself improves model reasoning.

Initial candidates freeze before the two independent test scouts run. Their
current-stage probes cannot influence those initial implementations. All ten
trajectories, source histories, tests and selections then freeze before private
evaluation or cross-policy analysis. The separate diagnostic measures:

- Best available initial/final candidate quality versus the selected source.
- Public-only, scout, reviewer and combined-evidence selectors on the same pool.
- Own-history evidence versus pooled evidence available only after the study.
- Detection of preregistered semantic fault families beyond public checks.
- Completion, regression retention, active time, provider overlap and API usage.

Fault qualification requires golden and equivalent controls to pass, every
deliberate fault to produce a runnable wrong answer on a witness, and at least
four public-surviving fault families per application/milestone. Candidate code
always runs in the pinned Docker boundary. The analysis uses descriptive paired
win/draw/loss, with quality before timing; two applications do not justify Elo.

Use fresh output paths and the pinned Python runtime. The live entry point
checks the exact qualification, full rehearsal, scientific contract and shared
ledger balance; it never resets earlier charges or automatically retries an
unknown provider outcome. A later paid cohort needs a newly registered contract
and the corresponding budget decision, not an edited historical result.

```sh
.venv/bin/python -m analysis.qualify_benchmark \
  --output results/benchmark-qualification-example
.venv/bin/python -m gossip_harness.benchmark_experiment run \
  --output results/benchmark-rehearsal-example \
  --qualification results/benchmark-qualification-example/results.json
.venv/bin/python -m analysis.audit_benchmark \
  --run results/benchmark-rehearsal-example \
  --output results/benchmark-rehearsal-example/independent-audit.json
.venv/bin/python -m gossip_harness.benchmark_experiment run --live \
  --output results/benchmark-live-example \
  --qualification results/benchmark-qualification-example/results.json \
  --rehearsal results/benchmark-rehearsal-example/results.json \
  --budget-ledger runs/first-live-budget.sqlite
```

The separate `simulation.benchmark_transport` module compares gossip, a durable
single broker, and replicated brokers with client failover under equal attempted
contact budgets. Its synthetic rehearsal and later frozen-artifact replay are
deterministic dissemination experiments, not additional coding trials or
production availability estimates. It reports bytes separately because equal
contact counts do not imply equal traffic.

## Continuation-controller follow-up

The [follow-up plan](continuation-followup-study-plan.json) separates two
questions: whether better continuation control helps finish projects, and
whether independent candidates help under that controller. The three arms are
the current controller with four independent candidates, the improved
controller with four independent candidates, and the improved controller with
four serial revisions. Warehouse allocation/returns and durable job leasing/
fan-out each have two cumulative milestones and two repetitions: twelve
trajectories across **two domains**, not twelve independent project families.

For each first-milestone block, both candidate formations freeze before common
candidate-blind scouts. The two independent arms share the exact initial draw;
all three share those scout proposals. Each arm evaluates imported source
afresh. Later milestones follow each arm's actual accepted lineage. Shared
model work is charged once; attributed opportunities are reported separately.

The improved controller directs repairs using executed failures, retains
pre-repair checkpoints and records newly introduced failures even when the
total failure count falls. A passing source with unresolved review concerns
gets at most one focused retry within the existing review allowance. Explicit
acceptance and the final independent checks remain mandatory. These changes
form one controller package; they do not isolate a routing-only effect.

Use the following zero-API chain after the source contract is frozen and the
affected offline verification lanes pass. Every output directory must be new.
Docker must already have the pinned image; qualification never pulls it.

```sh
.venv/bin/python -m gossip_harness.continuation_followup describe
.venv/bin/python -m analysis.qualify_continuation_followup \
  --output results/followup-qualification-example
.venv/bin/python -m gossip_harness.continuation_followup run \
  --mode rehearsal --output results/followup-rehearsal-example \
  --qualification results/followup-qualification-example/results.json
.venv/bin/python -m analysis.audit_continuation_followup --scope cohort \
  --run results/followup-rehearsal-example \
  --output results/followup-rehearsal-example/independent-audit.json
.venv/bin/python -m analysis.continuation_followup_comparison \
  --run results/followup-rehearsal-example \
  --output results/followup-comparison-example/summary.json
```

Qualification covers the two exact starting applications plus the forty
milestone golden/control/mutant rows. The complete rehearsal uses scripted
repairs, regressions, no-op outcomes and uncertainty handling through the real
Git, ledger and Docker boundaries. Scripted successes qualify execution; they
are not model-quality observations. All terminal trajectories freeze before
any candidate's final private evaluation. A later live run must bind the exact
qualification, complete rehearsal and independent audit, and satisfy the
registered funding/readiness gates before accessing credentials or dispatching.
The comparison requires a complete independently audited cohort. It reports
acceptance first, then inherited and new requirement coverage, with arm work,
shared setup and final grading times separated. Rehearsal comparisons remain
explicitly labeled as scripted infrastructure checks. Budget termination keeps
its evidence but cannot produce a scored incomplete-cohort comparison.

The current plan leaves live execution disabled. Historical charges remain in
the cumulative ledger. Its reservation ceiling is a worst-case bound, not a
cost forecast or authorization. A small development pilot cannot establish
general superiority: the [confirmatory design](analysis/benchmark-confirmatory-design-v4.json)
requires independently held-out task families and a fixed analysis/stopping
rule. The [separate live-coordination design](analysis/benchmark-live-gossip-followup-design-v2.json)
tests real peer decision-making and dissemination; this controller pilot does
not establish a gossip-versus-orchestrator effect.

The [first physical checkpoint](analysis/continuation-followup-physical-checkpoint-v1.json)
records 42 qualified fixture executions and a complete zero-API rehearsal with
12 runner-reported accepted trajectories and 24 completed milestones. Independent
audit then exposed two integration defects: serialized promotion-lease field
names and fresh scout-response JSON ordering. Audit v2 corrects both, with real
Git/SQLite and scout producer regressions; 174 affected checks pass. A diagnostic
traversal of all retained evidence found no further audit failure, but is not a
certificate. Those original outputs remain retained and uncertified.
The [corrected physical checkpoint](analysis/continuation-followup-physical-checkpoint-v2.json)
records a new 42-row fixture qualification and complete rehearsal under the
corrected contract. Its independent audit passes without source-comparison
waivers: 12 accepted trajectories, 24 completed milestones, 384 fresh Docker
executions and zero provider calls. The scripted comparison separates active
work, shared setup and waiting time; it assigns no Elo or quality ranking.
This qualifies the current execution path, not live model quality or a
gossip advantage. Live dispatch remains disabled; a changed plan needs matching
physical qualification and rehearsal before dispatch.
The [request-size reconciliation](analysis/continuation-followup-budget-reconciliation-v1.json)
is likewise diagnostic and does not authorize additional spending.
The [certified-evidence budget update](analysis/continuation-followup-budget-reconciliation-v2.json)
binds the new rehearsal. It retains the conditional $10–13 expected pilot usage
and an unapproved proposal to raise the cumulative cap from $50 to $80 for
repair demand and conservative reservation headroom; neither is a guarantee.

## Real peer transport foundation

The [peer runtime](docs/peer-runtime-v1.md) adds independently running local
processes with private durable stores, bounded TCP gossip, and a two-broker
adapter with client fallback. Tests exercise actual partitions, process death,
lost acknowledgments, replay and recovery. Events carry evidence; this runtime
has no task, spending, candidate-execution or Git-release authority.

This is the transport foundation for the proposed live comparison. Equivalent
worker and relay capacity, durable telemetry across restarts, real agent
decisions and the full four-cell experiment remain outstanding. Broker
acknowledgments confirm a receiver-local commit with asynchronous replication;
they do not provide quorum durability.
The [foundation checkpoint](analysis/peer-runtime-foundation-checkpoint-v1.json)
binds independent source review and 33 passing merged-tree checks, including
nine actual-process checks. The earlier sandbox socket-denial failure and
corrected test-resource declaration remain separately disclosed.

The [peer-local work foundation](docs/peer-work-v1.md) now connects those stores
to autonomous build/review fixture decisions through an authenticated narrow
[authority](docs/peer-authority-v1.md). It adds durable exact-request recovery,
epoch fencing, conservative unknown-dispatch reservations, an idempotent result
outbox and persistent work RPC telemetry. The fixtures transform and review text;
they make no model calls or software-quality claims. Provider integration, Git
acceptance, full transport accounting and the four-cell comparison remain open.

The [payload transport](docs/peer-payload-v1.md) adds immutable binary objects,
verified chunk arrival and retained wire observations for coding requests and
Git bundles. The [bundle adapter](docs/peer-git-bundle-v1.md) imports received
bytes into a fresh quarantine before the existing merged-tree and CAS gates;
receivers do not fetch proposals from a sender filesystem path.
The [coding dispatch adapter](docs/peer-coding-dispatch-v1.md) reuses the bounded
worker and durable request journal with an injected offline Responses transport.
Live HTTPS remains disabled until a separately qualified cumulative financial
ledger adapter is connected. These foundations do not establish coding quality
or complete the planned placement-by-transport comparison.

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
  persisted-response recovery, and supplementary failure diagnostics.
- [continuation-comparison.html](continuation-comparison.html): observed quality
  ranks and timings, plus the separate real-module maintenance comparison.
- [benchmark-comparison.html](benchmark-comparison.html): stronger maintenance
  tasks, matched candidate formation, evidence effectiveness and separate
  transport stress results.
- [investigation-roadmap.html](investigation-roadmap.html): the reviewed
  investigation design, evidence limits and proposed confirmatory studies.
- `.progress-report/project.json`: locator for the independent durable progress
  report, review checkpoints and continuation handoff.

Primary workflow references:

- [Git distributed workflows](https://git-scm.com/book/en/v2/Distributed-Git-Distributed-Workflows)
- [Linux development process and next trees](https://docs.kernel.org/process/2.Process.html)
- [Kernel rebasing and merging guidance](https://docs.kernel.org/maintainer/rebasing-and-merging.html)
- [Git workflow manual](https://git-scm.com/docs/gitworkflows)
