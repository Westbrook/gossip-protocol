# Acceptance against a complete declared inventory

Status: prospective next-version component. No live study uses this registry yet.
The existing v4 pilot and its evidence remain unchanged.

The [registry](../gossip_harness/project_acceptance_registry_v1.py) separates the
coverage question from the result of the observed checks. Every mandatory
requirement must map to at least one declared gate; every declared gate must
supply its complete ordered case census. A known assertion failure rejects the
subject. Missing cases, skipped checks and infrastructure failures stay explicit,
including when another gate has already failed.

An assessment can return `accepted_against_registry` only when all declared
checks pass, the exact source has verified protected-promotion evidence, and the
complete declared cohort has irreversibly stopped model work. An independent
acceptance gate is mandatory. Its presence does not mean every individual
requirement was independently retested: the prospectively reviewed mapping may
use valid public evidence for some obligations and independent histories for
cross-module behavior.

## The trusted boundary

This is a pure aggregation component, not a receipt authenticator or evaluator.
Adapters must first verify the original observations, source and Git identities,
execution and verifier receipts, promotion, and chronology of the terminal cohort
barrier. Hash equality identifies bytes; it does not prove they were executed.
The adapter must verify actual case outcomes rather than normalize an arbitrary
`passed` field supplied by a candidate or an unchecked receipt.

The frozen study contract pins the registry design. That design includes the
complete inventory identity, requirement/gate mapping, ordered case IDs,
cohort/trajectory/milestone/requirements identity, and every gate's suite,
evaluator, runtime/image, environment, limits, seed, protocol and purpose.
The terminal source is unknown when the contract is frozen, and the containing
contract cannot include its own hash. Only those two future subject digests are
excluded from the prospective design fingerprint. Assessment separately requires
the trusted execution-contract digest; its final registry fingerprint includes
both values. Expected digests must come from the frozen contract, not be derived
from untrusted received data merely to make that data pass its own check.

The independently reviewed product inventory supplies the denominator. This
component cannot infer whether natural-language requirements were omitted from
that inventory. Product-brief review and the remaining physical adapter checks
are necessary before any whole-project acceptance claim.

## Execution, reuse and incomplete results

Each gate receives at most one normalized observation. Retries or repeatability
observations require separately declared gates and distinct original executions;
there is no latest-result or pass-wins rule. Outcomes in a different order, extra
cases, duplicated evidence, or a different source/execution binding are invalid
evidence rather than product failures.

Public correctness evidence may be reused only with the exact original physical
execution and verifier references, identical execution binding, complete ordered
case list and known correctness outcomes. The assessment retains which gates
were executed and which were reused. Infrastructure failures, skipped cases and
partial executions cannot be reused as correctness judgments. Independent
acceptance and repeatability always require fresh physical execution after the
exact complete cohort freeze.

Passing tests cannot substitute for promotion. A missing or unknown promotion
keeps the result incomplete; a verified rejection is retained as rejection.
Incorrect source or contract identities raise an evidence-integrity error and
must not be counted as a demonstrated product defect.

## Remaining work

The prospective [M1 inventory](../library-m1-acceptance-inventory-v1.json) records
existing evidence definitions and the missing coverage; it is not an execution
receipt. The [cumulative product contract](library-cumulative-product-v1.md)
defines the later milestones before future model work.

Next steps are to implement the missing public and independent acceptance
adapters, verify their observation normalization against original receipts,
qualify the reference and deliberately defective implementations, and integrate
the full six-trajectory freeze barrier. The product, peer-local policy, matched
orchestrator, generated-test interface and recovery comparison still require
implementation and qualification. This component alone establishes none of
those results and no advantage for a swarm.
