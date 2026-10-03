# B02 v3: complete harness qualification, bounded product coverage

The v3 qualification passed all **53 selected test methods**: 45 offline and eight
Docker methods. It completed 251 fresh histories with 753 paused captures. These
counts describe different units; the physical histories are not 251 independent
model samples or whole-product acceptance decisions.

| Physical purpose | Histories | Outcome |
| --- | ---: | --- |
| Ordinary reference observations | 233 | All specified local observations matched. |
| Wrong-kind input with unspecified exact error code | 3 | Supported rejection and state checks matched; exact codes remained ungraded. |
| Reference forced schedules | 5 | Explicitly unavailable; no acceptance credit. |
| Separate instrumentable schedule controls | 5 | Intended schedules observed on the limited control source. |
| Deliberate defect controls | 5 | Intended defects distinguished from successful behavior. |

The central runner used six isolated class workers under a four-token resource
budget. Its wall time was **265.44 seconds**, with no reused classes. No methods
were skipped or left unrun. Static checks passed and execution input/runtime
fingerprints remained unchanged. Type coverage is explicit and incremental;
117 existing baseline diagnostics remain. This is not a whole-program type-safety
claim or a performance comparison between agent approaches.

## Why another version was necessary

The previous v2 run failed when a directory supplied as a ZIP path produced
`invalid_source` instead of the evaluator's expected `io_error`. A new normative
review, performed without the candidate implementation or prior results,
confirmed that all three wrong-kind inputs must be rejected without admission
or stored-state changes, but the specification does not prescribe their exact
error code or one universal native exception representation.

V3 preserves every recipe, invocation order, baseline expected history, and all
61 candidate files. It changes the evaluation policy prospectively for exactly
three rows. A recognized native error frame qualifies supported rejection, a
valid returned job proves forbidden success, and an unrecognized presentation
remains unknown. The actual code and historical exact comparison remain visible
as diagnostics. There is no allowlist chosen from observed codes.

Every independent result and storage assertion still applies. The qualification
requires the full selected product-assertion set and all 40 auxiliary checks;
a missing assertion cannot become a vacuous pass. The three partial results
keep `all_local_assertions_passed` false while separately reporting supported
checks. Their unspecified code receives no M1-I28 credit. That unsupported rule
is not a new product requirement or permanent barrier to future acceptance;
genuine I/O-error coverage is still required. Frozen v1/v2 failures are unchanged.

The three large JSON cases that v2 did not reach now executed: legal padding,
a syntax error after padding, and invalid UTF-8 after padding. Each fixture is
larger than six MiB. The previously unrun symlink-admission mutant also executed.
These results exercise the existing reference repair without altering it.

## Evidence and limits

The [qualification checkpoint](../analysis/candidate-b02-v3-qualification-checkpoint.json)
binds the prospective freeze, source identities, exact ordered selection,
central results, independent retained-evidence audit, and final reconciliation.
Raw captures and command records remain at the local `runs/` paths named there.
They are not included in this document or published as bulk runtime data.

The independent audit checks retained integrity and bounded outcome consistency;
it is not another execution or an independent semantic rescore of every passing
assertion. Mounted source staging was ephemeral, so historical mounted bytes
cannot be rehashed. The five unavailable schedules have explicit markers,
source-bound adapter evidence and unchanged captures, but no independent trace
of secondary-call absence. Captures follow adapter responses, and same-process
Store reopen does not establish process-death recovery.

The five instrumentable control histories use a separate, limited source. They
do not close the unavailable reference concurrency facets. Closed qualification reviews and
authored schema profiles are not production prerequisite authorities. This run
does not close all B02 requirements or establish whole-project acceptance.

## Next boundary

The next implementation slice is B03's real client observation and independent
process-completion protocol, followed by CLI, raw HTTP and browser coverage.
Its retained plan maps all 33 B03 targets into ten groups and six ordered slices.
Browser export needs a prospective semantic policy rather than assumptions
about a reference-specific button. Genuine B02 concurrent orderings, production
ScopePlan, complete assertion mapping and the remaining process/release gates
remain required.

The full study still covers four milestones and six matched healthy/recovery
trajectories: S4-G (4 builders plus 4 reviewers), S16-G (16 plus 4), and O16-G
(16 plus 4). Quality and sustained completion remain primary. Real restarts,
bounded partitions, generated distinguishing tests, a whole-cohort freeze before
fresh independent acceptance, and later held-out projects remain in scope.
This cycle made no provider calls, produced no comparative sample, and supports
no statistical superiority claim.
