# Literal project-test catalog: offline checkpoint

All **276 HTTP test histories** now have concrete declarations covering ten
families and eight target requirement IDs. They include literal request bytes,
input files, prescribed public state, setup dependencies, process epochs and
separate interaction labels. Independent source review and **81 fresh
offline tests in four classes passed** after the combined static gate.

- [Catalog implementation and limits](candidate-c03-http-cases-v1.md)
- [Fixed error relationships](candidate-c03-http-relations-v1.md)
- [Exact definitions, source identities and verification](../analysis/candidate-c03-http-cases-v1-checkpoint.json)
- [Independent source-review projection](../analysis/candidate-c03-http-cases-v1-source-review.json)

The catalog declares 8,203 steps and
7,602 HTTP requests across all histories. The
longest history has 87 steps;
4 cases exceed the old executor's
64-step bound and are preserved intact. These are planned counts, not executed
candidate tests or independent experimental samples.

The new error comparator checks exact job state separately from a fixed equality
between an error response and the affected job. A matching string cannot hide a
wrong epoch, missing job or altered unrelated job. Missing or ambiguous evidence
retains its uncertainty; no observed code is inserted into an expected answer.

The verification had no failures, skips, reuse or source/runtime drift. Static
coverage includes 437 sources and the configured type roots;
117 unchanged historical type diagnostics remain. This does not prove
whole-repository type safety. Existing physical observer evidence was preserved,
not rerun or upgraded to fresh acceptance.

**This is not yet an executable 276-case candidate study.** A qualified versioned
executor must implement root changes, confined data links, same-database CLI
handoffs and the declared larger histories. Trusted prospective registration,
authenticated observations, production acceptance mapping, the full integrated
rehearsal and the matched live study remain required. All P01-P10 gates remain
open; source review and declarations alone discharge none of them.

This checkpoint adds **zero live model-quality samples**. It preserves the full
quality-and-persistence study, its approval boundaries and all earlier results.
