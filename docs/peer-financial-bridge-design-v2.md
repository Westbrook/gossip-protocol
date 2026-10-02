# Cumulative financial bridge: prospective design v2

This is a reviewed design for connecting peer coding dispatch to the existing
cumulative financial ledger. It is **not implemented, qualified, enabled, or an
approval to spend**. The current cycle preserves all v1 and frozen scientific
sources. The governing machine-readable companion is
[peer-financial-bridge-plan-v2.json](../peer-financial-bridge-plan-v2.json).

The supplied cycle-7 checkpoint records a **$50 cumulative cap**, **$35.543626
committed**, and **$14.456374 remaining**. The proposal to raise the cap to $80 is
unanswered. These are retained context, not a fresh ledger read or a new study
allocation. Recheck the actual ledger under exclusive ownership before any future
live registration. Neither document changes the cap, authorizes a new cohort, or
loads credentials.

## Recommended integration

Keep the existing cumulative SQLite database as the sole spending authority.
Use a new versioned authority/dispatch adapter with cohort-scoped storage and
reuse the unchanged `Ledger` mutations and `RequestJournal`. Do not connect two
independently spendable wallets, copy historical spending into a private ledger,
or bypass v1's unconditional live-mode rejection with an injected HTTPS wrapper.
A two-ledger saga adds partial-admission and settlement reconciliation; escrow
would strand an entire allocation during ambiguity and obscure per-request
accounting. Neither is needed for this single-host authority.

The proposed trusted startup API is:

```python
CumulativeAuthorityV2.open(
    existing_ledger_path,
    service_root,
    cohort_contract,
    incremental_cap_micro_usd,
    expected_global_cap,
    expected_opening_usage,
)
```

The existing authenticated claim, coding-dispatch and lookup shape remains the
peer-facing seam, with an explicit new protocol and financial/cohort bindings.
The service validates requested task/profile permissions; it does not choose
tasks, models, recipients or winners, expose global candidate context, or accept
peer-controlled settlement. Its common policy and available capacity must be the
same in all four placement-by-transport study cells. A central financial safety
service is not evidence of a centralized work-planning treatment.

## Existing ledger, ownership and adoption

The registered production path is the existing `runs/first-live-budget.sqlite`,
resolved against the trusted repository configuration. Require an existing file,
expected schema, exact registered path/identity and expected budget; reject missing
files, aliases, a newly created wallet, incompatible schema and silent adoption.
Record the observed file identity and a canonical historical-row snapshot before
adding any new cohort metadata. Do not treat the changing live database's raw
file hash as a permanent execution identity.

The **provider-owning service process** must acquire and retain the exact existing
lock:

```text
str(Path(existing_ledger_path).resolve()) + ".continuation-live.lock"
```

For the current path this is
`/Users/westbrook/Documents/repos/gossip-protocol/runs/first-live-budget.sqlite.continuation-live.lock`.
The lock is a nonblocking exclusive `flock`, held for the whole possible-dispatch
lifetime, including recovery and in-flight work. Acquire it before any additional
service ownership lock and use that order everywhere. A new peer-only lock does
not exclude historical runners. A supervisor must not hold the only lock while a
child can continue invoking after the supervisor dies.

`continuation_experiment`, `benchmark_experiment` and `continuation_followup`
cooperate with this lock. Older pilot/sustained/verification entry points do not
all do so and must remain quiescent. SQLite's global cap still limits reservations
from other callers, but it does not itself establish cohort exclusivity. This is
a same-host cooperative ownership contract, not multi-host consensus or hostile
process isolation.

Do not pass the cumulative database to the v1 Authority constructor: it creates
a private authority and correctly rejects adoption of existing work. Its singleton
authority config and request/action tables are not scoped for successive cohorts.
Use new versioned cohort-scoped tables and the same joined Ledger object for all
claims, reservations, settlements and authority transactions. Existing historical
rows and budgets remain intact; no mutation algorithm from `Ledger` is forked.

## Stable cohort identity and admission

Registration binds one immutable `cohort_id` to the contract, complete roster,
allowed principals/tasks/profiles, incremental cap, source/evaluator identities,
journal namespace and cumulative ledger identity. Output directories are evidence
locations, not allowance identities. Restarting with a new directory must not mint
a second allowance or a second owner of the same request journal.

Global task and reservation identifiers are service-derived from a registered
cohort and exact task/action identity. Persist their mapping to the cohort,
principal, logical task, role, source generation, request ID and profile. Never
let an arbitrary peer-supplied namespace establish membership. Bind stable
request/action IDs to immutable bytes and authenticated principal; reject changed
payloads before considering lease freshness. Historical receipt replay does not
grant a new dispatch permission.

Within one joined `BEGIN IMMEDIATE` transaction:

1. Return an exact historical replay or reject conflicting identity.
2. Fence halted or unresolved actions and validate current task ownership using
   the authority-owned clock and lease epoch. For a new admission, sample fresh
   authority time **after acquiring the joined `BEGIN IMMEDIATE` SQLite write
   transaction**, so time spent waiting for either a process/file lock or the
   SQLite write lock cannot leave a stale lease check. A timestamp sampled before
   either wait is not sufficient.
3. Sum `COALESCE(spent, amount)` over exact registered cohort reservation
   membership. Require that the proposed commitment fits the immutable cohort
   cap and any preregistered per-slot limits.
4. Call unchanged `Ledger.reserve`, which independently enforces the cumulative
   cap, using this transaction and the same Ledger instance.
5. Persist membership, action/dispatch intent and the authenticated request
   receipt before committing.

Do not compute cohort use by subtracting global balances, infer ownership through
loose string prefixes, or reserve independently in a second database. Do not call
`StudyBudget.reserve()` within this transaction: its separate read connection and
file lock introduce uncommitted-state visibility and lock-order hazards. Reuse its
commitment invariant, not that call pattern. No SQLite transaction spans a provider
request or a wait on a journal lock.

## Registration differs from recovery

Fresh registration checks the exact expected cap, opening commitments, historical
quiescence and absence of pending promotion intents, then installs the immutable
cohort binding. An existing cohort is resumed only through its recovery path;
recovery validates its registered identity and retained records rather than
requiring the old opening balance again. A changed cap, source, profile, journal
root, roster or protocol requires a prospective new contract and qualification,
not an in-place reinterpretation of old requests.

The journal binds request bytes, provider/profile settings, source scope and
stable reservation/call identity. Preserve the existing ordering: reserve before
invocation; persist the result before settlement; publish a terminal receipt only
with complete bound evidence. Retain authenticated receipts in peer journals.
An exact saved successful receipt is historical evidence, not a new admission;
replaying it does not require a fresh lease or authorize another invocation.
Recovery of a durable intent follows its retained state: an intent without a
recoverable result remains unknown, while an exact durable result can be settled
after lease expiry. Neither recovery path converts a pre-lock timestamp into
current permission.
The credential stays inside the trusted provider-owning service and is never
included in event payloads, candidate files, logs or sandbox mounts.

| Retained crash boundary | Required behavior |
| --- | --- |
| No committed admission | Same-ID retry may proceed if no durable action exists |
| Reservation committed but no recoverable result | Keep full reservation, halt and do not automatically invoke |
| Durable dispatch intent without result | Unknown outcome; keep full reservation and prohibit provider retry |
| Durable result, missing settlement | Replay the exact result and settle idempotently even after lease expiry |
| Settlement committed, missing receipt | Reconstruct the receipt without a second invocation or charge |
| Result publication interrupted | Recover publication of the durable result; do not invoke again |
| Unknown usage | Keep full reservation and persistent dispatch halt |
| Storage cannot record unknown/halt | Fence in memory; return nonterminal waiting on the same ID; recover retained work before serving new dispatch |

Known failures with known usage remain charged and replayable. A halt requested
by worker metadata is preserved even when usage is known. Claim expiry does not
transfer an unresolved paid action. Neither lease expiry, missing acknowledgments,
new peer processes nor a new output directory authorizes refunds or retries.
There is no peer operation for clearing the halt, increasing the budget, forcing
settlement or publishing a release. This provides local dispatch ownership and
durable replay, not provider-side exactly-once execution.

## Source, purpose and qualification gates

The companion plan pins the actual supporting source bytes and this document.
Those are design references, not a frozen implementation contract. A future
execution contract must additionally bind every new authority/runtime source,
canonical encoding, profile/pricing estimate, complete roster and fault schedule,
ledger/cohort identity, ordered suites, evaluator, runtime/image, limits, seed,
journal namespace and evaluation purpose. Preserve raw requests, results,
settlement proofs, unknowns and failed runs. Token-based microUSD accounting is a
conservative estimate, not an invoice or total engineering/compute cost.

Qualify the actual new adapter against a **disposable existing-format ledger
containing historical settled spending**. Never point qualification at the real
cumulative ledger. Required offline cases include:

- Missing/changed ledger identity and schema rejection; historical rows and cap
  preserved; a new output path cannot duplicate an allowance.
- Concurrent admissions at both cohort and global limits; exact prior-request
  replay and cross-principal conflicts; known failures charged and unknowns kept.
- Authenticated localhost service and peer process restarts at every admission,
  result, settlement, publication and ACK boundary; retained transport entry
  counts establish no second invocation.
- Exact shared-lock contention with the cooperating historical runner path and
  service-owner death; no surviving provider-capable orphan owns dispatch.
- Durable outcome reconciliation after lease expiry, changed contract rejection,
  corrupt/missing terminal accounting evidence and halted-state recovery.
- Actual owned request/result payload transfer through the common adapter and
  real `OpenAIWorker` request/parser/usage code with a closed offline transport.
  A permissive fake wallet or text-only substitute does not qualify the bridge.

After cheaper checks pass, one verification owner must qualify the combined
financial, peer/controller, Git-bundle, sandboxed merged-source and exact-CAS
path, then complete one full zero-API rehearsal of the frozen roster. Independent
final evaluation remains behind the whole-cohort source freeze. A local finance
pass, a prefix of the roster, or a successful coding proposal cannot replace this
rehearsal or project acceptance.

Future live execution additionally requires a registered complete study/cost
contract, adequate authorized funding, current ledger recheck under ownership,
credential provisioning through the approved route and matching readiness proofs.
The unanswered $80 proposal is not approval. Preserve incomplete or interrupted
cohorts honestly; no small affordable prefix completes the full four-cell study or
held-out statistical objective. Autonomous coding tool loops and multi-host work
also remain separately unfinished.
