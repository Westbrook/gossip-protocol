# B02 v2: reviewed schedule applicability

This version preserves the failed v1 qualification and every original expected
history. It changes the execution contract so a reviewed source that holds an
exclusive write transaction throughout `prepare` is not forced into an impossible
synchronous secondary-write schedule. It also qualifies the separately versioned
JSON reader repair against the extended, prospectively defined case provider.

## Exact scope

The planned physical census is 251 independent fresh histories:

| Source and purpose | Histories | Meaning |
| --- | ---: | --- |
| Corrected authored reference, ordinary histories | 236 | Public results and persisted state are compared with independent expectations. |
| Corrected reference, forced schedules | 5 | Applicability is unavailable; ordinary preceding observations remain checked. |
| Ordinary surgical mutants | 4 | Archive, symlink, stale-token and illegal-cancel sensitivity. |
| Separately authored instrumentable schedule control | 5 | Qualifies observation of the originally specified forced ordering. |
| Instrumentable control with false-success defect | 1 | Qualifies sensitivity to an incorrect returned token after interference. |

These are 251 containers and independently owned volumes, with three captures
per history: 753 captures if all histories complete. The five unavailable
reference histories earn no product-acceptance credit. Successful limited
schedule controls do not establish concurrency correctness of the repaired
reference. The runner's unittest count is distinct from this physical census.

The intake, direct Store and limited schedule classes use separate source maps,
artifacts and mutable state. Each class runs its histories sequentially; the root
verification runner controls outer concurrency. Source or definition changes
require new bindings and qualification. Failed and interrupted runs remain
retained; there is no retry-until-green behavior.

## Closed qualification reviews

The v2 driver contains a prospectively reviewed, closed registry. Each record
binds the complete candidate-source inventory, eight relevant source-component
hashes, exact definition digest, permitted case IDs, purpose, applicability
reason and review identity. The caller must supply the expected review digest as
well as the expected complete-source and definition digests. Any source
substitution, unknown source, incorrect review identity, changed definitions or
out-of-scope case is rejected before output creation or Docker dispatch.

The repaired reference is eligible for the full 241-case roster. Each ordinary
mutant is eligible only for its named surgical case. The instrumentable positive
control is eligible only for the five forced schedules; its mutant is eligible
only for the designated false-success case. A caller-provided boolean cannot
change applicability. Returned review metadata is a copy of the installed
record.

This registry is explicitly **harness qualification only**. Its hashes identify
reviewed development inputs; they do not authenticate production source review,
authorize a ScopePlan, register a novel model-generated candidate, or grant final
acceptance. A general candidate controller still needs its own externally owned
review and admission authority.

## Why the five schedules are unavailable

The frozen public histories demand that a second Store completes cancel/retry or
prepare while the first caller is stopped immediately before its required
`start_job` or `fail_job` call. In the repaired reference, `JobManager.prepare`
already holds the encompassing `BEGIN IMMEDIATE` transaction at that point.
Waiting for the second write before releasing the first creates a test-induced
lock cycle. A successful secondary write—the prerequisite for the stale-token
expectation—has not occurred.

For these exact reviewed sources, the host records the inapplicability decision
in intent, review and terminal artifacts. The adapter returns the explicit
`reviewed_enclosing_write_transaction` unavailable marker before creating a
secondary Store or invoking prepare for that schedule. It preserves setup
results, stops dependent operations, and still captures after/reopened storage.
Qualification checks the exact marker, expected masked phases, unchanged
before/after raw storage and conserved logical/auxiliary state. A focused fake-API
control verifies that the unavailable branch never calls the forbidden methods.

The scorer retains earlier independent failures. Only assertions depending on
the unobserved transition become unknown. A random SQL exception, timeout,
returned candidate flag or arbitrary failure never selects the reviewed
inapplicability route. Unsupported interposition remains a separate observation
limitation. Method returns and instrumentation observations are untrusted process
data; the host's source selection, source-bound adapter and captured storage are
separate evidence.

## Limited instrumentable control

The control is a separately named evaluator-authored source map. Its prepare
method uses the required public get/validate/fail/start sequence without an
outer transaction, making the tested forced ordering observable. The repaired
reference's transaction is unchanged. The control is intentionally qualified
only for these five schedules and is not claimed to satisfy the complete v2
product contract.

Its five positive histories must reproduce the unchanged expected tokens,
staleness and durable state. The false-success mutant must return the designated
wrong token without an unrelated exception, and the host assertions must reject
it. A future concurrency contract should run genuinely overlapping callers,
retain the observed ordering and allow both permitted linearizable orders. This
control is not a substitute for that work.

## Preserved execution boundary

The explicit v2 fork retains the v1 bounded data-only operation interpreter,
read-only source/input mounts, UID 65534 candidate execution, pinned offline
image, fresh 32 MiB storage volume, 16 MiB cumulative session bound, 32 MiB
captured-file/total-data bound and 40 MiB complete tar bound. It retains raw
commands, stream hashes, paused inspections, complete captures, container removal
and exact-name absence, volume ownership/removal/absence, and source fingerprints.
No frozen v1 driver, definition, scorer, observer or reference file is changed.

Product observations are saved before auxiliary scoring. If a later mapper
becomes unavailable or fails, the earlier product failures survive in separate
artifacts and the census. Status distinguishes ordinary observations, product
failures, unavailable facets, mixed failed/unavailable evidence, infrastructure
failures and cases not run. Reopen remains a Store-handle reopen, not process
crash or power-loss evidence. No provider calls or model-comparison samples are
part of this qualification.
