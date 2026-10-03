# B02 v3: partial observations for unspecified error codes

This version preserves the failed v1/v2 qualifications and all 241 ordered
recipes. An independent normative review found that wrong-kind filesystem inputs
must be rejected without admission or state changes, but the product contract
does not specify their exact error code. The v3 scorer retains the original
comparison as a diagnostic and makes that exact-code assertion explicitly unknown.
It continues to check supported rejection and every independent state assertion.
The corrected reference source and execution adapter remain unchanged.

## Exact scope

The planned physical census is 251 independent fresh histories:

| Source and purpose | Histories | Meaning |
| --- | ---: | --- |
| Corrected authored reference, ordinary histories | 233 | Public results and persisted state are compared with independent expectations. |
| Corrected reference, wrong-kind inputs | 3 | Supported rejection and conservation are observed; exact error code remains unspecified. |
| Corrected reference, forced schedules | 5 | Applicability is unavailable; ordinary preceding observations remain checked. |
| Ordinary surgical mutants | 4 | Archive, symlink, stale-token and illegal-cancel sensitivity. |
| Separately authored instrumentable schedule control | 5 | Qualifies observation of the originally specified forced ordering. |
| Instrumentable control with false-success defect | 1 | Qualifies sensitivity to an incorrect returned token after interference. |

These are 251 containers and independently owned volumes, with three captures
per history: 753 captures if all histories complete. The three partial and five
unavailable reference histories earn no complete product-acceptance credit. Successful limited
schedule controls do not establish concurrency correctness of the repaired
reference. The runner's unittest count is distinct from this physical census.

The intake, direct Store and limited schedule classes use separate source maps,
artifacts and mutable state. Each class runs its histories sequentially; the root
verification runner controls outer concurrency. Source or definition changes
require new bindings and qualification. Failed and interrupted runs remain
retained; there is no retry-until-green behavior.

## Three declared partial observations

The closed policy applies only to `intake-directory-wrong-kind`,
`intake-zip-wrong-kind` and `intake-json-wrong-kind`. Qualification requires the
exact declared null assertion `after.result.0.exact-error-code`, a qualified
native error frame, supported rejection, and every other public/storage/auxiliary
assertion to be true. The guard requires the complete prospective product key
census derived from the frozen expected history, and all 40 auxiliary keys
(the initial incarnation plus three phases, each with an exact table census,
eleven tables and files). Missing
keys cannot disappear from the conjunction. It does not use an error-code allowlist. The observed error
frame and code, frozen legacy comparison, policy digest and evaluation-contract
digest remain retained. `all_local_assertions_passed` stays false.

These histories receive `partial-observation` and
`declared-partial-observation-verified`, never the ordinary-observations category.
The census separates unspecified assertions from observation-unavailable
assertions and keeps known false assertions. An extra null, malformed frame,
unexpected exception, missing observation, accepted JOB or state mutation cannot
qualify. A later auxiliary error cannot erase an earlier product failure.

The native error frame is the evaluator normalizer's qualified observation scope,
not an invented universal product exception or return-value contract. An
unrecognized return presentation is unqualified evidence; independently observed
state failures remain false. The unspecified code is a limit on the old local
assertion's authority, not a new product requirement, a product defect or a
clarification required for future product acceptance. These rows grant no I28
exact-code credit. This partial category is distinct from the five source-bound
schedules that cannot be performed by this adapter.

## Closed qualification reviews

The v3 driver contains a prospectively reviewed, closed registry. Each record
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

The explicit v3 fork retains the v2 bounded data-only operation interpreter,
read-only source/input mounts, UID 65534 candidate execution, pinned offline
image, fresh 32 MiB storage volume, 16 MiB cumulative session bound, 32 MiB
captured-file/total-data bound and 40 MiB complete tar bound. It retains raw
commands, stream hashes, paused inspections, complete captures, container removal
and exact-name absence, volume ownership/removal/absence, and source fingerprints.
No frozen v1/v2 driver, definition, scorer, observer or reference file is changed.

Product observations are saved before auxiliary scoring. If a later mapper
becomes unavailable or fails, the earlier product failures survive in separate
artifacts and the census. Status distinguishes ordinary observations, policy-limited observations, product
failures, unspecified facets, unavailable facets, mixed evidence, infrastructure
failures and cases not run. Reopen remains a Store-handle reopen, not process
crash or power-loss evidence. No provider calls or model-comparison samples are
part of this qualification.

## Qualification ownership and limits

The four new unittest classes are `CandidateIntakeStoreDriverV3Tests` (offline),
`CandidateIntakeDriverV3DockerTests`, `CandidateDirectStoreDriverV3DockerTests`
and `CandidateScheduleControlV3DockerTests`. The three Docker classes retain
separate mutable fixtures and sequential inner execution. The root verification
owner runs the combined static gate and then the registered Docker lane after
source, policy and closed review identities are frozen. The intake/Store timeout
is 1,800 seconds each; the limited control timeout is 300 seconds; resource weight
is one per class. Roughly 7–10 minutes and 753 captures are prospective estimates,
not observations or a promise that every planned case has run.

This cycle adds no staged-source manifest or public calltrace mechanism. Existing
complete-source/component digests, staged-byte checks, raw public responses and
physical storage captures remain the evidence boundary. The exact definition
source map, including normative policy inputs, is retained before and after each
execution; any change is infrastructure failure, not revised authority. Unknown candidates still
need an independently authorized source review; this registry remains a closed
development qualification facility.
