# Running twenty roles before measuring project quality

The larger project plan uses sixteen builders and four reviewers. This runtime
adds the processes, durable local state, asynchronous dispatch and private Git
publications needed to exercise that plan. Its offline plumbing fixture uses
scripted provider responses. Passing it demonstrates working mechanisms, not
twenty intelligent agents completing the Local Research Library or a quality
advantage over an orchestrator.

The first physical observation passed on October 2, 2026: twenty role processes,
sixteen independently verified candidate bundles, twenty exact terminal replays,
and four observations of gossip delivery during blocked worker calls. It took
165.15 seconds before cleanup; all owned role processes exited successfully.
There were no live API calls, candidate executions, scope approvals or protected
releases. The [retained checkpoint](../analysis/peer-role-runtime-v2-checkpoint.json)
binds this result to its sources and verification receipts.

## Responsibilities

| Component | What it owns | Boundary |
| --- | --- | --- |
| `peer_mesh_store_v2` / `peer_mesh_v2` | One bounded event/payload store and independently running TCP gossip service per node | Notices propagate; payload bytes move after explicit local demand. A role cannot resolve an event that has not arrived locally. |
| `peer_financial_rpc_v2` | Authenticated per-role RPC and durable claim/renew/submit identities | Requests carry references. Model work runs asynchronously outside handlers and ledger transactions. |
| `peer_mesh_finance_v2` | Exact request-arrival guard and private result publication index | A same-digest event is not an exact reference. Finance publishes results as itself, never as a builder. |
| `peer_role_loop_v2` | Local evidence materialization and the persisted action state machine | A lost reply preserves the original request and lease. Unknown admission cannot free the role's call allowance. |
| `peer_candidate_v2` | A completed action's scoped proposal, Git bundle and publication intent | Exact base files, output bytes and source scope must match. Candidate source is not executed by this component. |

The shared schema, financial authority, review recovery and release eligibility
gate are described in [the foundation contract](peer-project-foundations-v2.md).
The financial implementation still rejects live mode. No API key is needed for
these offline fixtures and they do not open the project's cumulative paid ledger.

Each role receives its own financial capability. The mesh uses a separate shared
membership key and optional observer key on loopback TCP. That is trusted-runtime
membership authentication, not Byzantine producer authentication, encryption,
host isolation or a multi-host security claim. The observer can bootstrap static
membership, inspect counts and impose partitions; it cannot publish evidence or
choose a role's work. Model output remains data for the trusted adapters.

Transport has explicit limits on notices, objects, bytes, frames, handlers and
subscriptions. Exhaustion waits or rejects without claiming missing evidence is
available. Same-database restarts preserve local command identity; reconstructing
a lost node database from peer history is unsupported. Observer polling is
included in the current wire-byte counters, which are measured per process boot.
These counters must not be presented as data-plane-only traffic.

Run the explicit process fixture only after the affected fast, fixture and Git
lanes pass through `devtools.verify`:

```sh
.venv/bin/python -m gossip_harness.peer_twenty_role_fixture_v2 \
  --output runs/a-new-twenty-role-observation
```

The output must not exist. The default scenario deadline is 240 seconds; cleanup
is recorded separately. The retained contract binds source and runtime identity,
and the receipt rejects source drift, partial results and cleanup failures.
An unsuccessful observation remains in place for diagnosis. It is not retried
automatically or repurposed as a passing receipt.

## What the offline process fixture establishes

The fixture starts twenty separate role processes, with seed and finance counted
as additional infrastructure. Sixteen builders materialize source and evidence
from their local meshes, complete one injected worker action, create separate Git
repositories and publish scoped proposals. Four reviewers exercise the same
dispatch and result machinery with explicit fixture artifacts. Those artifacts
are not release approvals.

Four bounded provider-fixture calls can overlap while their responses are held.
A new probe must arrive through gossip during that hold. Lost submit
acknowledgments and terminal replay must preserve the original admission and
total invocation count. The receiver checks builder bundles against retained
financial outcomes. Process identities, local views, calls, timings, Git
identities and owned-process cleanup belong in the retained receipt.

This scenario uses trusted local directives and a small source fixture. It does
not yet exercise autonomous task discovery, model-selected discriminating tests,
cross-package repair, eight authenticated review scopes, fenced release
promotion, browser acceptance or the full recovery schedule. Separate component
tests for restart and partition behavior do not establish those properties for
the combined twenty-role project.

## The practical project remains the quality test

The authored Local Research Library seed supplies a working CLI, SQLite catalog,
file importer, search, loopback HTTP API and browser client. Its separate public
state-transition oracle and M1 contract define reliable multi-file ingestion,
job state, cancellation and atomic catalog changes. The seed deliberately does
not implement those new requirements. Later lifecycle, recovery and migration
milestones remain in [the full project plan](large-swarm-project-plan-v1.md).

The next integration must join actual project proposals to sandboxed checks,
review-selected evidence, source selection, repair and protected Git promotion.
An intentionally clean textual merge with incorrect combined behavior must be
rejected, repaired and checked again at the exact final source. Only then can
the full three-arm, healthy/recovery cohort be rehearsed and funded as a unit.

Project completion, inherited behavior, retained progress after faults and final
independent acceptance are the primary outcomes. Wall time, API cost, wire bytes,
idle time and review queues explain those outcomes. A single project family is
development evidence; additional untouched families and matched comparisons are
needed before claiming a general advantage or publishing an Elo-style ranking.
