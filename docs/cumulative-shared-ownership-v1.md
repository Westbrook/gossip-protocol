# Shared normative ownership v1

[The ownership ledger](../analysis/cumulative-shared-ownership-v1.json) assigns
all **57** currently ownerless shared obligation IDs to explicit product
requirements. It binds the complete union of **51 v1** and **57 v2** shared
units: **108 exact source instances**, each with its original JSON pointer,
raw source hash and compiler-normalized value hash. This is a semantic design
input for the future complete ScopePlan. It is not executable acceptance.

The frozen denominator remains 123 product requirements, three separate
prerequisites and 99 logical gates, with 233 v1 and 312 v2 source obligations.
The historical coverage map remains unchanged. Resolving these 57 assignments
establishes **zero qualified obligations and zero comparative samples**.

## Ownership decisions

Each ledger row contains the responsible requirement IDs, an explanation,
source citations for those requirements in each applicable version, required
cross-feature interactions and explicit applicability conditions. Owners are
chosen for the behavior described by the whole shared unit. Interactions name
mandatory joins with other requirements; they do not transfer responsibility,
remove the other requirement's obligations or grant coverage credit.

| Shared section | Units | Assignment principle |
| --- | ---: | --- |
| Constants | 16 | Capacity, type and transport limits belong to the operations that define or enforce each bound. |
| Value types | 7 | Exact shapes belong to their defining domain and interface requirements; legacy receipts stay historical. |
| Interfaces | 6 | Whole tables include every introduced route, CLI form, Python entry point and accessible browser action. |
| Persistence | 9 | Portable snapshot layouts belong to the stated schema/fixture boundary; control, graph and publication duties retain their domain owners. |
| Compatibility | 9 | Explicit old/new behavior is preserved per milestone, including authorized health and v2 amendments. |
| Release ownership | 7 | Package/source ownership, immutable author inputs and generated output paths remain distinct. |
| Added v2 sections | 3 | Counter domains, worker liveness and portable hash vectors use their amendments' explicit owners. |

The six units absent from v1 are `counter_max`, `backup_root`, compatibility
row8, `counter_domains`, `worker_state_table` and `portable_m1_hash_vectors`.
Changed shared values keep separate v1/v2 source instances. Citation of a v2
amendment does not retroactively change v1 or the frozen M1 pilot.

Several decisions matter for a valid study:

- Capacity includes tombstones, distinguishes distinct retained blob bytes
  from revision counts, and preserves no-op/replay behavior at the boundary.
  Per-file, aggregate archive, POST, export and backup limits are different
  obligations; matching numbers do not make them interchangeable.
- The complete `schema3` unit includes worker/job/document fences, maintenance
  ownership, backup registry/root, search state and diagnostics. Ownership
  covers all these facets; selecting a worker check alone cannot close it.
- Portable table declarations apply at their declared migration boundaries.
  Permitted indexes, triggers and derived layout remain implementation choices.
  A reviewed reference storage profile is not a universal M1 database grammar.
- Compatibility row2 retains `V0-HTTP-04` alongside `M4-API-SCHEMA` and
  `M4-COMPATIBILITY`. A future plan must express the compiler's exact health
  override using `m1:V0-HTTP-04` and `M4-API-SCHEMA:clause:2`; assigning owners
  alone cannot remove the inherited gate.
- Compatibility row8 uses the union of all six amendment owners. Its broad
  assignment is justified by that specific source, not by assigning every
  requirement to every shared unit.
- `future_fixture_status` is a frozen pre-execution authoring duty. Its
  source-time “not yet authored” wording is neither evidence that fixtures are
  complete nor a permanent condition that they must remain absent. A separate
  author/freeze proof is still required.

## Validation and review

Construction checks enumerate both fresh compiler inventories, compare the
exact union against the historical pending-ID list, verify every pointer/value
hash and reject missing, duplicate or foreign requirement owners. Independent
normative review is retained in
[the review record](../analysis/cumulative-shared-ownership-review-v1.json),
including exact reviewed file hashes. Agent review is not human review or
production authority registration.

No candidate source, candidate output, provider call, Docker run or new
behavioral test contributes to these assignments. They come from the frozen
requirements. No generic N/A approval has been issued: each future assertion
class and lane must have its own justified mapping or authorized applicability
decision.

## Integration boundary

A later complete ScopePlan may consume these owner lists and ownership reasons
only after binding the reviewed document and exact selected source version.
It must still decompose every whole shared unit into concrete assertion
selectors across its required classes and lanes, include the named semantic
interactions, preserve exact inherited compatibility authority and satisfy all
prerequisite registration/proof requirements. The ledger provides no candidate
verdict, promotion, cohort barrier or independent acceptance observation.

All four cumulative milestones and six matched trajectories remain in scope,
including the larger swarm arms, recovery conditions, durable orchestrator
comparison, complete cohort barrier and later held-out families. The ownership
count is a measure of completed design work, not evidence for or against swarm
quality.
