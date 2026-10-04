# Explicit capacity for complete execution histories

`project-acceptance-history-capacity-v1` is an opt-in aggregation contract. It
raises the bounded count of declared execution gates and collected observations
to 4096. It does not shrink the inventory, shard a requirement into disconnected
registries or waive an execution. `MAX_ITEMS` remains 512.

The current public inventory already needs 590 separate history slots: 57 CLI,
276 HTTP, eight product-process and 249 storage histories. Fresh independent,
repeatability, browser and worker observations add separate slots. 4096 is an
explicit prospective per-declaration/per-registry limit with headroom, not an
assertion that every future design fits. Capacity beyond this bound needs another
versioned contract. A complete new source-bound rehearsal is required before the
expanded contract can support production acceptance.

## Activation and identity

The typed `Declaration` and `Registry` gain a defaulted `capacity_profile`.
Existing callers retain `project-acceptance-capacity-legacy-v1`, with a limit of
512. Hosts activate the new contract explicitly:

```python
source.assemble_declaration(
    catalog, cohort, complete_slices,
    review_sha256=review_sha256,
    capacity_profile=registry.HISTORY_CAPACITY_PROFILE,
)
```

There is no inference from collection size, result success or caller-supplied
numeric cap. Unknown profile names/types are rejected. Compilation validates the
profile and transfers it into Registry. The existing independently registered
declaration and registry-design hashes bind the chosen profile and its fixed
4096/512 capacity manifest. The scope authority adds an explicit capacity-contract
review target for the new profile. The final consumer does not accept an extra
unregistered override; its incremental and final assessments use the registered
Registry profile.

The compiler counts **all declared execution gates**, including prerequisites,
against the chosen capacity. Registry counts its product gates, and assessment
counts the collected observations against that same profile. One physical origin
still cannot satisfy two separately declared execution slots.

## Collection audit

| Collection | Bound and disposition |
| --- | --- |
| Declaration execution gates | Explicit profile: 512 or 4096, including prerequisites |
| Registry product gates / assessment observations | Explicit profile: 512 or 4096 |
| Declaration suites, plans, edges, purpose records; keyed qualification targets and suite compatibility | Existing `MAX_DECLARATIONS = 16384`; unchanged |
| Source obligations / product requirements / prerequisites | Frozen 312 / 123 / 3; unchanged |
| Per-history ordered cases and physical outcomes | Existing 512; unchanged |
| Requirement IDs, cohort members, control paths, qualification specs/outcomes and evaluator-source maps | Existing 512; unchanged |
| Scope assembly slices, review targets and normalized consumer observation list | Already accept complete tuples/lists; no hidden 512 aggregate guard; the compiler and Registry now enforce the explicit aggregate cap |
| Registered design hashes and final adapter lookup | Bind the exact prospective declaration; no size-based profile conversion |

The execution recipes are still per-history records. This extension grants no
permission to pool independent histories, change resource limits, reuse acceptance
observations, skip unknown results, or replace runtime/provenance checks. Larger
aggregate capacity is not a larger per-history candidate resource budget.

## Historical preservation

`declaration_record()` and `registry_record()` are canonical serializers. They
omit the new field for the legacy profile, preserving pre-change declaration,
Registry, design and Subject fingerprints. Nonlegacy fingerprints additionally
bind the fixed capacity manifest. `ScopeSubmission.request()` uses the canonical
declaration serializer, while an opted-in request exposes the new profile and
its capacity review target.

Raw `dataclasses.asdict()` includes the new default field. There is no blanket
claim of raw dataclass serialization or decoder compatibility. No historical
receipt is rewritten or promoted. Changed source fingerprints still invalidate
old matching-execution/review assumptions even when a legacy logical hash is
unchanged. New source-bound review and the complete changed-contract rehearsal
remain required.

The test vectors were captured from the actual unmodified files at commit
`6d3e71d6728a06b96cf6148f07f9a2f693de7e3f`, before this draft changed them.
The legacy v2 serialized declaration was 662220 bytes with SHA256
`e73d379e0641d22a856bf39747079645acdb0e3c4d27f84c2d009740cf7004a5`.
Tests compare these retained constants, not just two calls to the new hash code.

## Offline qualification scope

`AcceptanceHistoryCapacityV1Tests` constructs a complete synthetic declaration
against the real frozen denominator: 590 public history slots, one independent
slot, one repeatability slot and one prerequisite gate. This exercises 593
suites/gates, 592 product observations and 591 admission qualification targets
through compilation, independent registration test doubles, all incremental
consumer checks and final assessment. All 312 source units, 123 product
requirements and three prerequisites remain present.

Those structural fixtures use synthetic selectors and normalized passed records.
They prove aggregation capacity and retained barriers; they are not observations
of 590 executed application histories or a qualified production ScopePlan. Real
source-derived scope assembly and physical execution remain separate gates.

Focused negative controls cover missing gates, known failures alongside missing
observations, duplicate gates/cases/origins, extra observations, wrong source,
profile substitution, incomplete cohort freeze, prohibited independent/repeatability
reuse, legacy rejection at 513, and new-profile rejection above 4096. The exact
4096 gate/observation boundary is also accepted. Tests retain the smaller bounds
for unrelated collections and explicitly exercise profile forwarding and the
review-request target. No Docker, model/provider call, spend or product acceptance
execution is involved in these controls.
