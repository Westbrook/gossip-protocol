# Durable financial completion for sequential study cohorts

The financial V4 adapter adds a durable end to each study cohort. Once its
completion record commits in SQLite, new work cannot be admitted, even when
publishing that record to the independent checkpoint chain is interrupted.
Exact completed requests remain readable without new spending or provider calls.

This is harness infrastructure. It contributes **zero new comparative quality
samples** and does not establish a gossip or larger-swarm advantage. Live V4
entry remains unavailable until the complete rehearsal validator is implemented
and the changed whole execution contract is qualified.

## Study and accounting boundaries

The prospective roster contains six ordered child cohorts, each with 8 or 20
roles and 96 distinct role identities in total. That is six sequential cohorts,
not 96 simultaneously connected principals. The existing 64-principal RPC cap
is unchanged. Every child, predecessor handoff and final barrier must bind the
same full execution contract as the prospective study roster.

The adapter uses the original cumulative ledger, its file identity, spending
history, cap and exclusive owner lock. Terminal configuration is enrolled in
the same transaction as the cohort. It does not create a wallet, clear a halt,
reset a reservation, expire a lease artificially, or increase the budget.

Admission and seal operations share a short gate. An active execution token
spans the existing worker/accounting path; provider work remains outside that
gate. Pending, queued and publication-pending work prevents sealing. An unknown
outcome retains its reservation and halt, can only support a stopped-failure
record, and prevents a later child from opening. Completion of bookkeeping is
never a software-correctness verdict.

## Original evidence and interrupted publication

The host records each registered role's stop assertion and the child's terminal
status and final source identity. Preparation authenticates those records,
checks original accounting evidence and captures an exact per-child census.
The seal validates the exact checkpoint boundary, then compares the same census
and closes admission in one SQL transaction. External publication is a separate
step.

A failure after SQL commit leaves admission closed and acceptance incomplete.
Explicit recovery may publish only against the exact original preseal boundary;
it cannot rebase onto an unrelated later checkpoint or implicitly retry an
uncertain publication. Reopening a sealed cohort does not advance its clock or
rewrite its accounting. Prior-child handoff authenticates the linked original
receipt, SQL snapshot and financial configuration before exempting its retained
claims from the next child's opening checks.

The final financial barrier checks all six original seals and 96 stop records
against one read-only SQLite transaction, with ledger identity checked around
the read and a final independent checkpoint check. These are retained host
assertions: current unit fixtures do not prove that 96 actual role processes
have exited. The production controller must supply that evidence.

The receipt parser checks schema, input linkage and context bounds. It is not
standalone proof of an arbitrary historical checkpoint's ancestry. The complete
operation also relies on the trusted original SQL producer, its exact preseal
boundary and independently authenticated publication. Identity checks at declared
boundaries do not claim continuous filesystem attestation or transient ABA-swap
immunity.

## Verification and retained findings

The corrected combined static gate and all **227 affected offline tests** passed
in 11 fresh isolated class workers: 43 new controls and 184 nearest inherited
regressions. The batch took 22.28 seconds, with 216 fast and
11 fixture checks, no reused classes or skips, and no source/runtime drift across
562 bound inputs. The new contract-substitution controls also fail against
the preserved original code. The [machine-readable checkpoint](../analysis/financial-terminal-integration-v1.json)
links source pins, both combined receipts, review findings and retained logs.

The first combined batch passed all 223 selected methods. A separate source
review still found missing roster-to-execution-contract binding and a final
barrier that reopened the ledger pathname for each child. Both findings required
source changes and targeted controls. The earlier passing receipt and the
contributor's initial failures remain retained; they are not relabeled as
qualification of the corrected implementation.

Configured static checking includes the three new modules. The unchanged 117
recorded legacy type diagnostics remain visible; passing the incremental gate
does not establish whole-repository type safety. Test workers use disposable
ledgers, injected provider responses and owned loopback services, with no real
provider calls or candidate Docker execution.

## Required work before the larger live comparison

The complete V4 live rehearsal evidence validator remains unfinished; its entry
point explicitly rejects live qualification. A collection of passed flags or an
older V3 receipt cannot enable it. Integrating real process-stop and final-source
evidence into the full four-milestone/six-trajectory controller also remains
required, alongside independent semantic scope authority and the remaining
product observations.

After the coherent combined contract passes the applicable cheaper gates, it
needs fresh physical qualification and a matching complete zero-provider
rehearsal. The larger live comparison, independent held-out confirmation and
separate placement-versus-transport comparison remain open. None of the fixture
results substitutes for those experimental outcomes.
