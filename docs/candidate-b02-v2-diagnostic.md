# B02 v2 qualification: useful progress, unresolved evaluator error

The v2 qualification remains **failed**. Static checks and all 45 offline test
methods passed. The eight selected Docker methods produced six passes, one
failure and one method not run. No failed run or frozen expectation was edited
to turn this result green.

The physical batch completed 241 of 251 planned fresh histories, retaining 723
paused captures. Counts describe different units and must not be combined into
an inflated pass total:

| Purpose | Planned | Executed | Outcome |
| --- | ---: | ---: | --- |
| Repaired reference: direct Store | 117 | 117 | 112 ordinary observations matched; five forced schedules explicitly unavailable. |
| Repaired reference: intake | 124 | 115 | 114 ordinary observations matched; one exact-error mismatch. Nine histories not run. |
| Limited instrumentable schedule control | 5 | 5 | Five intended schedules matched. This source is not the product reference. |
| Deliberate defect controls | 5 | 4 | Four intended defects detected; symlink control not run. |
| Total | 251 | 241 | No whole-product acceptance or comparative sample. |

The central runner used seven isolated class workers under a four-token resource
budget, with no nested test pool or reused classes. Wall time was 244.38 seconds.
The retained input and runtime fingerprints were unchanged at completion. Type
checking covers explicit roots and their imports; 117 existing baseline
diagnostics remain. This is not a whole-program type-safety claim.

## What changed

A separately versioned reference reads a JSON bundle to EOF through the existing
confined descriptors, while preserving ZIP limits and decoded-content limits.
Only generated `library/ingestion/jobs.py` changes; the other 60 generated files
remain byte-identical. This fixes the earlier accidental application of the
one-MiB ZIP archive bound to encoded JSON. The previously failing escaped-content
boundary now matched its frozen expected history.

The v2 definitions retain all 238 original rows and add three JSON inputs larger
than six MiB: legal whitespace around a tiny valid bundle, an invalid syntax
suffix, and an invalid UTF-8 suffix. All three were prospectively frozen, but
**none executed** before this batch stopped. Their offline fixture checks do not
substitute for physical candidate execution.

The v2 driver binds an exact source review, definition, case roster and purpose
before execution. It identifies five forced reentrant schedules as unavailable
for the reviewed reference because its enclosing write transaction prevents the
required secondary precedence. It does not attempt a secondary transition or
infer unavailability from arbitrary SQL errors. Earlier ordinary failures remain
failures. The five schedules retain unknown product facets and receive no
acceptance credit. Separately scoped limited controls establish that those
schedules and an intended false-success defect can be observed when the fixture
is instrumentable.

## Why qualification stopped

`intake-zip-wrong-kind` supplies an existing directory as the ZIP bundle path.
The reference returned `invalid_source`; the evaluator expected `io_error`.
No job was admitted and the database plus both lock files were byte-identical
before, after and on reopen. Only the exact-error assertion differed.

Independent normative review corrected its earlier clearance: the frozen M1
contract maps actual I/O failures to `io_error`, but does not explicitly classify
an existing directory rejected before reading. Its nonregular ZIP-member rule
addresses entries inside an archive, not the top-level filesystem argument.
The inventory's proposed wrong-kind coverage gap cannot create a new normative
error rule. The original exact-error expectation is therefore overconstrained;
this observation does **not prove a product defect**.

The failed summary and its historical `product-failed` census label remain
unchanged. The diagnosis is recorded separately. The unexecuted JSON wrong-kind
case may share the implementation path, but no execution result is claimed for
it. A future evaluator version must resolve this ambiguity prospectively and
receive independent review before a new qualification run. Product behavior
must not be changed merely to satisfy an unsupported oracle expectation.

## Evidence and remaining work

The machine-readable [diagnostic checkpoint](../analysis/candidate-b02-v2-diagnostic-checkpoint.json)
binds the prospective freeze, central runner result, independent raw-evidence
audit, diagnosis and source hashes. Raw captures and execution logs are retained
locally under the referenced `runs/` paths; they are not published by this page.
The independent audit checks retained integrity and outcome consistency, not a
fresh execution or an independent rescore of every passing semantic assertion.

Paused captures occur after an adapter response. Same-process Store reopen is
not process-death recovery. Closed authored reviews are qualification fixtures,
not production prerequisite authorities. These histories do not close every B02
requirement, the production ScopePlan, or the remaining client, process and
release obligations.

The four-milestone, six-trajectory matched study remains required, including
independent final acceptance after the cohort barrier, real restart and bounded
partitions, generated distinguishing tests and later held-out project families.
No provider calls, new comparative samples or statistical superiority claims
were produced in this cycle. The next step is a reviewed, versioned disposition
of the evaluator ambiguity, followed by complete qualification and the remaining
production-controller work.
