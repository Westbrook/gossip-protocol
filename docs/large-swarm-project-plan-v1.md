# A complete software project with 20 agent roles

**Status: prospective development and scaling study; execution is not qualified.**
This plan proposes one useful application, four cumulative release milestones,
and a comparison of 8 versus 20 durable cognitive roles. It makes no claim that
20-role software completion has already run. The existing large dissemination
simulations and small candidate-pool studies answer different questions.

The accompanying [machine-readable plan](../large-swarm-project-plan-v1.json)
records the roles, contrasts, resource proposals, readiness gates and source
bindings. This scaling-plan deliverable changes only its two new plan files.
Root’s separate funding activation changes the registered pilot’s authorization
and budget fields while preserving its 39 execution sources; that work is outside
this deliverable. The separate four-cell decision-placement × transport study
and held-out confirmation remain open obligations.

## The product: Local Research Library

Build a local application that turns a folder of research material into a
searchable, annotated library. A user can import Markdown, UTF-8 text, HTML and
bounded ZIP/JSON bundles; find documents; edit notes and tags; refresh changed
sources; inspect ingestion jobs; export selected material; and restore a backup.
It provides a CLI and a small accessible browser UI over one HTTP API. The
proposed implementation uses Python, SQLite and browser-native HTML/CSS/JavaScript;
dependencies and runtime versions must be pinned during fixture construction.
External websites, accounts, remote model calls inside the product, PDF/OCR and
multi-user authentication are outside this first product scope.

Start from a **working v0**, not an empty repository: local text import, persistent
document IDs, basic search, list/show/export commands, a minimal browser view and
published compatibility tests. The evaluator owns a separate implementation of
the specified state transitions and independently authored end-to-end checks.
The seed, product specification and public tests are frozen before any study run.

The challenging requirements concern interactions, not the number of screens:

- A logical document ID differs from its content-blob hash. Identical bytes can
  belong to distinct sources; refreshing one source must preserve that document's
  notes and must not overwrite another document's provenance.
- Search, CLI, API, browser views and exports must agree about current revisions,
  deleted records, ordering and pagination.
- Cancellation or replacement of an ingestion job must fence a stale worker's
  eventual result. Retries and restarts must not duplicate documents or revisions.
- Deleting one document must not remove content still referenced by another
  document or a retained revision. Failure during import or restore must leave
  the existing catalog usable.
- Migration must preserve inherited IDs, annotations, job receipts and export
  semantics while introducing the final revision-aware API contract.

These resemble real persistence and coordination concerns already explored in
this repository. The application is therefore a **new development/scaling target,
not a statistically independent held-out family**. Untouched private scenarios
can test its implementation without changing that classification.

## One project, four releases

| Milestone | Integrated release obligation | Cross-module pressure |
| --- | --- | --- |
| M1: reliable ingestion | Directory and bounded archive import, durable progress, cancellation, duplicate policy and per-source provenance; inherited text import/search still work | Ingestion writes, blob identity, catalog transactions, job state, API responses and browser progress |
| M2: document lifecycle | Refresh and revisions, notes/tags/collections, deletion and restoration, deterministic search/pagination and equivalent CLI/browser workflows | Identity versus content reuse, concurrent edits, stale revision tokens, search visibility, shared-blob retention |
| M3: recovery and portability | Restart-safe jobs, incremental reindex, bounded export, atomic backup/restore and useful failure diagnostics | Durable receipts, canceled workers, schema snapshots, archive integrity, UI recovery and storage cleanup |
| M4: compatibility release | A preregistered revision-aware API/schema change, migration from frozen v0/M2 data, CLI compatibility and a complete operator handoff | Old clients with new storage, old receipts with new serialization, annotations across migration, full workflow preservation |

All four milestone briefs and the full-project horizon are preregistered. Detailed
new milestone requirements are released at the same declared milestone boundary
in each arm; their hashes are fixed beforehand. The M4 change is not improvised
after observing an implementation. Earlier requirements remain binding throughout.
The final deliverable includes the runnable integrated application, installation
and recovery instructions, a representative user dataset, versioned API/schema
documentation and a reproducible release command. Lines of code and ticket count
are not acceptance criteria.

Four work packages span every release: **catalog and storage**, **ingestion and
jobs**, **query and service contracts**, and **clients and release workflows**.
The proposed 16 builders begin as four candidate lanes in each package. They
produce alternative contributions to the *same application*, then integrate and
maintain it across releases. They do not build 16 unrelated applications.

The requirement/dependency graph must expose actual opportunities for parallel
work. Freeze the graph and expected ready frontier before running; do not invent
extra tickets to fill 16 slots. Report feature-work frontier separately from
alternative-candidate lanes. Four useful work packages with four competing
implementations are not 16 independent product features.

## The three arms

| Arm | Cognitive roles | Decision placement | Dissemination |
| --- | --- | --- | --- |
| S4-G | 4 builders + 4 reviewers = **8** | Peer-local work selection, review requests, source selection and repair routing | Same versioned gossip protocol |
| S16-G | 16 builders + 4 reviewers = **20** | Same peer-local policy | Same versioned gossip protocol |
| O16-G | 16 builders + 4 reviewers = **20** | Durable central scheduler applies the declared policy to its arrived shared view | Same versioned gossip protocol |

S4-G versus S16-G estimates the effect of the larger builder population and its
resulting allocation/diversity under the offered resources. It does not isolate
parallelism, diversity or allocation individually. S16-G versus O16-G is the
decision-placement contrast; the same transport removes a broker-versus-gossip
change from that comparison. Neither contrast establishes that gossip is better
than a broker. A conventional orchestrator/broker arm requires a separately
registered addition; it does not silently replace the existing full 2×2 study.

The central scheduler is deterministic software, with no extra model calls. It
is counted as infrastructure and shares the same compute envelope. It receives
the same kinds of model-authored proposals and review decisions, has durable
restart/failover behavior and uses the same integration gates. It must not be
artificially restricted to one provider call at a time. If a model coordinator is
later introduced, its role and entire token/call budget must be counted; the
comparison can no longer be described as an unchanged equal-20-role contract.

## What makes a role an agent

Each cognitive role has a persistent identity, isolated local event/payload store,
independent Git clone and branches, local model context, decision journal and
resumable state machine. A builder repeatedly selects eligible work from locally
arrived offers, proposes a plan/change/tests, requests review, responds to evidence,
rebases or integrates, and chooses its next useful action. A reviewer repeatedly
chooses which arrived candidate sets and uncertainty to investigate. Nodes may
change package affinity when local work is blocked; affinity is an initial bias,
not a permanent assignment from a hidden dispatcher.

Work-item identity is separate from candidate-lane identity. Competing candidates
have distinct leased task IDs under one requirement/acceptance identity. This
permits intentional alternatives without allowing two agents to execute the same
fenced attempt. The first alternatives are blind to sibling source until their
proposal boundary; later cooperation is explicit and logged. The policy must
specify that boundary and how newly discovered dependency/contract work becomes
an admissible task before a run starts.

Peer decisions must be based on **materialized, locally arrived evidence**, not
merely a list of event IDs next to a fixed seed prompt. Every request binds its
actual source excerpts, candidate bytes, test receipts, requirement revision and
causal event/payload hashes. Missing payloads cause an explicit wait or another
local choice. A common service cannot quietly provide peers a global summary,
select their task, choose a winning source or compose their repair plan.

Initial implementation should use bounded structured propose/test/review/repair
actions with a qualified sandboxed test/retrieval interface. This is a
policy-autonomous project loop. It is not yet an unrestricted shell/tool-loop
agent or a multi-host availability claim. Transport processing must continue
while a role is waiting for its model or evaluator.

## Strong reviewers select evidence from diverse implementations

The same four reviewer roles appear in every arm:

1. R1: inherited behavior and interface compatibility.
2. R2: data integrity, durability and failure recovery.
3. R3: cross-package integration, candidate disagreement and semantic conflicts.
4. R4: end-to-end user workflows, browser/CLI consistency and release readiness.

Builders use the same less-capable model profile; all reviewers use the same
stronger profile. Exact snapshots, prompts and profiles remain to be frozen after
the current controller pilot. There are **zero additional scouts** in this plan;
test proposals come from the declared builders and reviewers. Adding scouts
changes the role and resource contract.

Reviewers receive candidate implementations under rotated aliases, their exact
public-test receipts and proposed tests after those artifacts arrive locally.
They choose a bounded set of high-signal checks: tests that distinguish competing
implementations, preserve inherited behavior, cover a dependency boundary, or
challenge an untested recovery claim. The selected tests execute against every
eligible candidate and the actual merged tree as appropriate. A majority of
implementations agreeing is not a correctness oracle.

The test interface needs a separately qualified, typed assertion format. In
addition to exact scalar results, it should support contract-defined relationships
such as an ID remaining unchanged across refresh, a retry preserving an original
response, or two independently observed digests matching. This avoids requiring
a reviewer to invent an opaque hash to express a valid invariant. A trusted
reference/contract checker must validate admissible expectations; unsupported
claims remain review notes rather than correctness evidence. Its scope and budget
must be identical across arms. No private acceptance or concealed fault-bank
result may inform live test selection, source selection or repair.

Promotion requires source-bound evidence and two appropriate non-author reviewer
decisions for the relevant cross-package changes. Common services validate those
bindings and the exact merged source; they do not choose the candidate because
it arrived first. A stale Git head requires rebase/revalidation and renewed
source-bound approval. Review queue depth and review capacity are measured: four
reviewers may be the limiting resource at 16 builders, and that is a valid result.

## Offered resources and a complete cohort

The proposed initial cohort is **six complete project trajectories**: the three
arms in a healthy block and the same three arms in a predefined recovery block.
Use matched seed snapshots, requirement releases and exogenous fault inputs
within each block, fresh model draws, randomized execution order and independent
mutable repositories/databases. Both blocks use one application family. This is
an engineering comparison; neither the 20 roles, four releases, many tests nor
two blocks create independent project families.

Proposed per-trajectory hard limits are a six-hour active project horizon, 128
builder dispatches, 32 reviewer dispatches and the same funded spend ceiling.
These are planning values, **not frozen execution limits or a price estimate**.
Every planning, test-design, implementation, review and repair model call counts
in its role's allowance. Limits are aggregate, not equal per-agent quotas: S4 can
use its builder allowance over more turns per builder. No stage silently loses
remaining repair capacity because of an arbitrary small review-round counter.
All stop rules and any per-stage limits must be frozen in the runnable contract.

Match global and per-profile provider concurrency, evaluator/browser concurrency,
CPU/memory, public evidence opportunity, model context/output limits and active
horizon across arms. Select concrete capacities after offline capacity and
reservation-headroom qualification. Report configured role count, roles that
actually made cognitive decisions, distinct physical processes, simultaneous
provider calls, queued calls, idle/blocked time, duplicated work and useful
integration throughput separately. Twenty live roles do not imply 20 concurrent
provider requests. More agents do not receive extra aggregate calls by default.

The user's larger-swarm request authorizes pursuing this study **within the now
approved $80 cumulative API ceiling**. The funded registered controller pilot takes priority
under that same ceiling. After it settles, derive the remaining available and
reserved balance from the cumulative ledger and estimate the entire proposed
cohort, including worst-case overlapping request reservations. No allocation or
price is invented here. If a complete preregistered, qualified cohort fits existing
authorization, another permission question is unnecessary. If it does not fit,
present the concrete funding gap or a prospectively narrowed complete cohort;
do not launch a partial cohort or borrow an assumed future allowance.

## Faults, integration conflicts and the stopping barrier

The recovery block should contain two frozen, boundary-triggered interventions:
a builder process restart after a provider result is durable but before its
candidate is published, and a bounded peer-message partition affecting a declared
work-package group. Keep financial authority and evaluator availability unchanged
for this initial compound scenario. Choose exact identities, durations and
partition mapping before dispatch; record the role fraction and absolute count
affected in every arm. A milestone never reached has an untriggered fault, not a
successful recovery. The result concerns the compound scenario, not an isolated
estimate of each fault's effect. Unknown provider outcomes are not automatically
retried; they retain reservations and follow the common fail-closed policy.

The application work also includes preregistered integration challenges: clean
textual merges that disagree about document identity, cancellation epochs,
revision visibility or shared-blob ownership. Real Git branches and bundles must
carry these independently developed changes. A textual merge and a worker's own
passing checks do not certify their combined source. Preserve failed offers,
review decisions, conflict repairs and accepted ancestry throughout the project.

Local quiescence and "all my tasks passed" do not mean the project is complete.
Each role publishes a durable status including unresolved obligations and pending
actions. A common stop/barrier service may enforce the fixed horizon, spending
limits and accounting reconciliation, but may not supply a repair or choose an
implementation. At terminal status or deadline, stop new dispatch, resolve or
fence in-flight actions under the frozen policy and snapshot every trajectory's
exact integrated source, full candidate/history set, task/evidence journals and
unresolved work. **Freeze all six trajectories before any private evaluation.**
No private feedback is returned to an active model. Retain stopped, failed and
unfinished trajectories without replacement.

Final acceptance runs on each frozen integrated release in a fresh pinned
environment. It covers inherited workflows, every new requirement, cross-module
failure/rollback behavior, restart/migration, install/start and browser/CLI/API
journeys. Hidden datasets and operation histories are authored independently
before model work. Replay selected historical milestone snapshots only after
the same barrier, for descriptive regression curves. A model saying "done",
public promotion, partial coverage and a working home page cannot substitute for
independent whole-project acceptance.

## What the Progress Report should show

Lead with accepted whole projects, inherited requirement preservation, new-feature
coverage, critical data-loss/integrity failures, unfinished obligations and
verified continuation after faults. Whole-project acceptance requires every
mandatory inherited/new-feature gate and valid source/promotion provenance;
coverage or speed cannot compensate for an invalid release.

Then show requirement coverage and regressions across all four releases, useful
work retained versus discarded, repairs available/used, source-selection regret
measured only after freeze, reviewer test yield/uniqueness and cross-implementation
disagreement, queue/idle/duplicate work, and recovery latency with denominator and
fault-exposure disclosures. Separate active work to frozen source, waiting for
the cohort barrier, and private adjudication. API, evaluator and communication
work may overlap; their summed durations are not elapsed time.

For this one-family study, use a block-by-arm outcome table and explicitly
descriptive quality ordering. An Elo rating would falsely suggest a stable league
from two related blocks. Cost per accepted project is undefined for an arm with
zero accepted releases; report total cost and acceptance together instead. Do not
claim statistical confirmation or general superiority from this cohort.

## Readiness before implementation becomes a live study

Source review shows the present runtime is a foundation, not a qualified 20-role
project loop: the peer/authority limits currently stop at 16; coding dispatch
serializes an invocation; the synchronous RPC path can outlive its caller's short
deadline; the worker has one fixed seed/task and terminal record; local context
and replicated event/payload limits are small; promotion lacks whole-project
selection and final acceptance. The runtime also needs evidence materialization,
demand-driven payload transfer and durable cleanup after evaluator-parent loss.
These are versioned engineering changes, not configuration-only scaling.

Before paid dispatch: freeze the product and dependency/acceptance contracts;
implement the additive multi-action local policy and matched central scheduler;
qualify asynchronous admission/status with bounded real provider overlap; qualify
20 isolated role processes, capacity/backpressure and causal local evidence;
prove source selection, review, merged-tree validation, stale-head recovery and
container cleanup; then run one complete zero-provider rehearsal of the exact
six-trajectory contract, including negative controls and the global private
barrier. Independently audit that rehearsal and reconcile a fully funded live
cohort with the remaining ledger balance. The current pilot's receipts do not
qualify changed runtime or project contracts.

This document and JSON received a bounded source/design review only. No larger
runtime, product fixture, tests, provider calls or new study qualification were
performed to produce them.
