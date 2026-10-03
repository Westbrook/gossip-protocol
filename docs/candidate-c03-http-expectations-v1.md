# Prospective HTTP definitions and setup dependencies

`gossip_harness/candidate_http_expectations_v1.py` gives the application catalog
an immutable format for literal requests, expected responses and their setup
dependencies. It prevents missing rows, missing probe expectations and changed
requests or expected values from retaining the same definition identity.

This is a definition layer, **not a completed production expectation registry**.
A digest identifies the supplied declaration; it cannot prove when that
declaration existed. A separately qualified controller must register the exact
serialized bytes before execution. A future acceptance bridge must verify that
registration, fresh raw observations, original source/runtime bindings, and the
whole-cohort final barrier. The historical observer is unchanged and cannot gain
prospective or acceptance authority through this module.

## Definition contents

A `SuitePlan` contains an ordered roster, ordered `CasePlan` objects, source pins,
and an `ExecutionContext`. Serialization is canonical ASCII JSON with literal
binary data encoded as base64 plus byte length and SHA256. Returned record
objects are fresh copies; the definitions retain exact frozen dataclasses,
tuples, strings and bytes.

Each case includes the current v2 `HttpRecipe`: server arguments, exact port and
DB/root paths, fixture bytes and directories, and every ordered start/probe/stop
step. Each probe binds both its canonical request definition and its emitted
HTTP bytes. All requests must fit the same declared wire policy when the suite
is constructed. Every probe needs exactly one expectation in recipe order.

The context binds candidate source SHA256 and exact Git commit/tree, requirements,
evaluator, runtime, pinned image, environment, complete lifecycle/wire policy
including seed and deadlines, purpose and repetition. These are **declared
identities**, not observations of actual files or runtime. Source pins are also
declared; no file reader or dispatch side effect is hidden in construction.

Cases must cover exactly the supplied roster. That catches an omitted or unknown
row relative to this declaration. It does not establish that the supplied roster
contains every product obligation or that a case actually tests its claimed row.
The full 276-row proposal, materialized histories and requirement mapping still
need independent qualification. Interactions are separately labeled, and the same ID cannot occupy both fields
within a row. Correct target/interaction classification still requires
independent review; callers can supply incorrect labels.

The module accepts names of future observation purposes so purpose changes alter
definition identity. This does not expand v2's execution permission: its executor
still permits only `harness_qualification`. `SuitePlan.acceptance_authority` and
`fresh_execution` are fixed false for every declared purpose.

## Dependencies and diagnostic comparisons

A `StepExpectation` holds a pure semantic expectation, source citations and
optional `ConditionalFacet` records. Each condition names one comparison facet
and one or more `Prerequisite(step_id, facet)` values. Prerequisites must refer
to existing facets of earlier probes. Self-dependencies, cycles, later steps,
unknown facets and missing probe expectations are rejected before serialization.

For example, a post-import document census can require the earlier setup census
to pass before comparing exact contents. Its HTTP status and JSON syntax may be
independent assertions. If setup is missing, failed or unspecified, only the
conditional content facet becomes unavailable. A wrong independently prescribed
status remains a failure. Complete transport or status200 alone is not an
implicit proof that the entire setup state is correct.

`diagnose` accepts explicitly supplied pure `ResponseFacts` values. It evaluates
in recipe order regardless of input ordering and propagates prerequisites by
facet. It does not accept or authenticate historical journal objects. A caller
can extract values from any source and compare them diagnostically, but the
result is permanently `supplied_values_only`, with no fresh-execution or
acceptance authority. There is no aggregate product verdict. Missing later facts
do not erase earlier failures, and observed state never becomes expected state.

## Qualification and remaining scope

The two new fast-lane test classes cover literal byte binding, identity changes,
immutable nested values, complete ordered probe/roster coverage, invalid
references, explicit purpose/runtime declarations and missing/failed/unspecified
setup evidence. They do not execute a candidate, Docker or a provider. Root owns
the combined static gate and independent review for this change set; the cycle
checkpoint records the exact passing receipts and source hashes.

Before fresh product dispatch, complete these retained obligations:

- Materialize every row and multi-step history in the 276-row proposal. Freeze
  independently authored fixture values and all exact resource counts.
- Add the source-supported relational predicates needed where an exact error
  code is unspecified. A byte/depth rejection may require failed state and
  equality of response code to persisted JOB.error without prescribing the code.
  The current full-value comparator cannot express that split; do not invent an
  expected code or learn a permitted value from a candidate response.
- Qualify new root/argv epochs, confined data symlinks and genuine same-DB
  HTTP→CLI→HTTP transitions. The v2 recipe's existing limits remain intact.
- Bind authoritative prerequisite observations to the declared conditions and
  install a complete production ScopePlan/Registry mapping. Caller-supplied pure
  facts and a roster coverage check cannot replace that work.
- Register before dispatch, verify fresh physical observations and cleanup,
  preserve the final independent-evaluation barrier, and run the complete larger
  project/recovery rehearsal before its live study.

P01–P10 and the full multi-milestone, matched-trajectory scientific design remain
open wherever their original evidence is still missing. Public HTTP state does
not prove hidden receipt/blob conservation, transaction atomicity, architecture,
browser behavior or whole-project acceptance. This work adds no comparative
model observations and no evidence of swarm superiority.
