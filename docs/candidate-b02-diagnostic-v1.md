# B02 first physical diagnostic

The first physical B02 qualification **failed**. The prospective definitions and
executed implementation remain frozen. This checkpoint records useful failures;
it does not establish completed requirement coverage or a ready project study.

## What ran

The combined static gate and all 69 selected offline tests passed. The seven
selected Docker test methods produced one pass, two failures and four not-run
outcomes. The combined run took 201.890 seconds; this is development validation
time, not a comparison of agent approaches.

| Physical workload | Planned | Executed | Outcome |
| --- | ---: | ---: | --- |
| Direct Store histories | 117 | 111 | 110 matched; one forced-schedule mismatch |
| Intake histories | 121 | 86 | 85 matched; one decoded-content boundary rejection |
| Deliberate defect controls | 5 | 1 | Archive-admission defect distinguished |

There were 198 distinct executions and 594 before/after/reopened captures.
Forty-one ordinary histories and four defect controls remain unrun. The runner
stopped each failing roster; those missing observations are not passes. An earlier
attempt failed on three type annotations before starting any worker. Both attempts
are retained, and no automatic retry followed the physical failures.

## JSON rejection is a reference defect

`intake-json-control-total-byte-limit` contains 16 entries of 32,768 decoded
UTF-8 bytes each: exactly 524,288 bytes. JSON escaping expands the file to
3,146,333 bytes. The reference calls `_read_path(bundle_path, ARCHIVE_BYTES)`,
applying the ZIP archive's 1,048,576-byte limit before JSON decoding. It returns
`too_large`, admits no job and leaves later lookups returning `not_found`.

Two independent normative reviews confirmed that the frozen requirements limit
decoded content separately from ZIP archive bytes. M1-I24 explicitly preserves
equivalent decoded-byte accounting for escaped and literal Unicode. There is no
separate raw JSON-file limit in the declared contract. The expected acceptance
remains unchanged; increasing the undocumented encoded cap would merely move the
same defect, since whitespace can also enlarge valid JSON.

The inherited source is
`gossip_harness/library_m1_ingestion_reference_v1.py:279`; its generated
`library/ingestion/jobs.py:276` is bound in the execution freeze. A future repair
must use a new source version and preserve the failed reference and observation.

## Forced cancellation is an observation limitation

`interfere-start_job-cancel` injects a synchronous write through a second Store
inside the first Store's `prepare` call. This reference already holds a SQLite
`BEGIN IMMEDIATE` transaction across prepare. The first call waits for the
injected cancellation, while that cancellation needs the first call's write lock.
The secondary action reports `OperationalError` and never cancels the job.

Raw state moves from queued epoch 2 to running epoch 2 and remains there after
reopen. Successful prepare at epoch 2 therefore does not contradict fencing:
there was no completed cancellation or epoch change to fence against. The retained
census's original `product-failed` label is preserved, but this source-reviewed
interpretation is **observation unavailable for the forced ordering**.

A new driver contract should bind interposition eligibility to the exact reviewed
source and transaction profile before dispatch. It should retain prior ordinary
assertions and mark only the unavailable operation and its dependents unknown.
It must never turn arbitrary database errors, timeouts or candidate claims into
unavailability. The current stale-epoch expectation remains required when the
intervening cancellation demonstrably completes first.

A separate instrumentable control can test detector sensitivity. A future real
parallel test should observe both calls starting and admit the contract-permitted
orderings: prepare finishes before cancellation; cancellation precedes prepare's
initial lookup (`job_state`); or cancellation follows the queued read but precedes
the fenced write (`stale_epoch`). It should verify returned tokens and final state
without inventing whole-prepare linearizability. Neither removing the reference's
transaction nor relabeling this induced lock conflict would validate concurrency.

## Continuation

Repair JSON handling in a separately versioned reference; add the source-bound
interposition applicability contract; then freeze and qualify that changed
execution contract after the cheaper checks pass. Preserve the original sources,
failed receipts and every unreached case. Of the six prospective JSON timing
alternatives, five were reached and matched a declared complete history; the
unencodable-text case remains unrun. Alternate branches are not additional
physical executions or independent evidence.

The separately reviewed 57 shared ownership assignments are design inputs, not
acceptance. Production scope registration, other client/process/release facets,
complete project rehearsal, all six four-milestone comparison trajectories and
later held-out families remain required. This cycle made no provider calls and
adds no comparative model samples or statistical superiority evidence.

Exact sources, retained attempts and the independent physical audit are linked
from `analysis/candidate-b02-diagnostic-checkpoint-v1.json`.
