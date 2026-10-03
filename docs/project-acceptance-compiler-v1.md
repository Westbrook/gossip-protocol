# Prospective project acceptance compiler v1

`gossip_harness.project_acceptance_compiler_v1` compiles a complete, externally
registered coverage declaration into the existing product `Registry`. It does
not run a candidate, authenticate a receipt, establish semantic test adequacy,
freeze a cohort, or grant acceptance. No production clause-to-test mapping or
qualified scope plan accompanies this module. Existing reference histories and
M1 developmental observations retain their original purpose and scope.

## Source denominator

`load_inventory(root, product_file=..., expected_product_sha256=...)` reads only
three repository JSON files. The loader recognizes their exact byte hashes:

| Source | SHA-256 |
| --- | --- |
| `library-m1-acceptance-inventory-v1.json` | `f5d808866d6f18f3fd095de5d447bb5b0e2a43e20771761fe91a38cf4f59ad4a` |
| `library-cumulative-product-v1.json` | `3353dbaa0c2474527cbffb0d87a4091b5f77ed3f98418fff2f216d90b6a8487c` |
| `library-cumulative-product-v2.json` | `2d88ce0775888f148b0ec3caf90b3d5c82d8fed71f53bec5f7e75f492ae998dc` |

A caller-supplied hash of a smaller or changed document is not registration.
Unknown revisions require a versioned loader change. V2 also checks its frozen
ancestor, semantic amendment map, unchanged requirement identities, ownership,
dependency graph and evidence lanes. Neither source version is promoted by
loading it.

Both versions preserve 106 inherited product requirement IDs plus 17 later
product groups. The three separate prerequisite IDs are `M1-INTEGRATION-02`,
`M1-SCOPE-01` and `M1-SCOPE-02`. There are 99 logical gates: 96 product gates and
three qualification gates. The admission prerequisite applies additionally to
`V0-ADAPTER-01`, `V0-ADAPTER-02` and `M1-ADAPTER-03`; it does not become a product
requirement.

| Derived inventory | V1 | V2 |
| --- | ---: | ---: |
| Product requirement IDs | 123 | 123 |
| Separate prerequisite IDs | 3 | 3 |
| Later requirement clauses | 65 | 75 |
| Source obligation units | 233 | 312 |
| Qualification authority units | 22 | 22 |

Obligation units include every M1 row, all seven M1 clarification resolutions,
the admission scope, each later clause, shared normative section units, and v2
amendment decisions, clauses and required observations. Dense shared tables are
kept as source-bound units; they require an explicit reviewed owner assignment.
The 22 qualification authority units preserve all eight `acceptance_boundary`
entries and all 14 inherited gate definitions. They describe harness and
acceptance duties, not new product functionality.

The 188 inherited gap notes remain design inputs with their original type and
scope. An explicit disposition must retain each note's requirement owner and
relevant assertion class. Their presence is not a claim that 188 current
failures or gaps remain. The compiler reports declaration omissions separately
from candidate outcomes.

## Public API and trust boundary

All declaration records are frozen dataclasses. The main interface is:

```python
inventory = load_inventory(root, product_file=V2_FILE,
                           expected_product_sha256=PRODUCT_V2_SHA256)
result = compile_design(inventory, declaration, final_m4_subject,
                        scope_plan=reviewed_scope,
                        expected_scope_sha256=registered_scope_sha256)
```

`Declaration` contains obligation plans, suite definitions, physical execution
gates, case/observation edges, prospective purpose assignments, compatibility
interpretations and the complete cohort design. `ScopePlan` is a separately
registered decomposition containing:

- The exact inventory and reviewed declaration fingerprints.
- `ObligationApplicability` for every source obligation, with explicit
  `ApplicabilityCell(kind, logical_gate_id)` requirements and reasoned
  `NotApplicable` entries for every other applicable owner/class/lane cell.
- Dispositions for all inherited gap notes and acknowledgement of all source
  qualification authority units.
- Exact approved purpose assignments, compatibility authority/predecessor/
  successor mappings, and any `SuiteCompatibility` for a recognized prior
  contract's exact suite definition.

The compiler does not create the real scope plan. The host must obtain an
independently reviewed decomposition and register its digest independently of
the candidate or declaration submission. The digest binds identity; it does not
prove that review happened or that a selector observes the intended behavior.
The consumer must authenticate scope qualification for this exact inventory,
scope, declaration and evaluator lineage before accepting product evidence.

`declaration_fingerprint(declaration)` provides canonical identity for that
registration. Changing a suite, selector, purpose or interpretation invalidates
the registered declaration. Supplying no scope plan or no separate registration
returns `missing_scope_plan` or `missing_scope_registration` blockers and no
product Registry. A matching digest of an unreviewed mapping is insufficient
for scientific acceptance, even when its structure compiles.

## Mechanical coverage checks

Each actual assertion must have a declared case and observation selector for
every required logical gate. Every clause's required kind/lane cells are checked
individually; coverage of another clause in the same group cannot replace them.
All mandatory product groups retain positive, boundary, negative and history
representatives. Scope review decides which individual clause cells are
irrelevant and records reasons; the compiler does not infer them from prose.
Interaction and defect-control adequacy also remain scope-review duties.

Each suite declares its ordered cases, definition identity, original purpose,
prospective execution purpose, source contract, evaluator identity,
capabilities, and candidate portability/suitability. Reference-only definitions
cannot become candidate gates by changing a purpose label. Original M1
independent gates and all independent-integration, migration, process-restart
and release-install lanes require independent-acceptance execution. An
unrelated independent gate cannot make public observations satisfy those lanes.
Receipt reuse and execution authentication remain the existing registry and
consumer's responsibility.

Compatibility interpretations retain both inherited and successor clause
references with an explicit reason. The `/health` interpretation specifically
retains `m1:V0-HTTP-04` and `M4-API-SCHEMA:clause:2`; changing schema 0 to schema 4
does not declare the old expectation passed. Exact semantic eligibility of
other authority mappings is reviewed and bound by the external scope plan.
An arbitrary old contract hash plus a review hash cannot authorize a suite.

Logical gates may share one physical execution gate, and one case may observe
multiple clauses. One physical slot cannot be assigned to multiple gates.
The resulting Registry contains product gates only. Its inventory identity
binds the source census, complete declaration and registered scope plan.

`PrerequisitePlan` separately exposes the three prerequisite IDs, qualification
execution gates, product admission dependencies, the scope digest and all
qualification authorities. Admission gates declare `QualificationBinding`
records for each dependent physical product gate. Ordered suite, evaluator,
runtime/image, environment, limits and protocol must match that product gate.
The admission execution itself retains its separate harness subject. Its
qualification receipt must establish the declared target lineage; these fields
alone do not authenticate the receipt.

Every declared cohort contains all S4-G, S16-G and O16-G healthy and compound
recovery trajectories, all four milestones, the declared builder/reviewer
identities, matched block seeds/fault schedules, and the peer-local versus
durable-central decision placement contrast. Common metadata binds transport,
policy, resource/model contracts, generated distinguishing tests, barrier and
held-out plan. The actual process restart, bounded message partition, peer
policy, generated tests and held-out executions remain to be implemented and
physically verified. Cohort metadata is not evidence they occurred.

## Results and remaining work

`CompilationResult.registry` is absent when coverage has blockers.
`CoverageBlocker` describes incomplete design inputs; malformed or unknown
inputs raise `CompilerError`. `declaration_complete` means only that the
registered declaration passed these structural rules. The result has no
product outcome, physical-execution flag, receipt or acceptance claim.

The next adapter must provide the real source-bound scope mapping, candidate
portable suite definitions and selectors, qualifying harness observations,
and a consumer that authenticates prerequisites before invoking the product
Registry. It must then collect all mandatory physical independent observations
under the cohort barrier. A few reference histories or a sparse release gate
cannot supply that complete mapping.

`AcceptanceCompilerTests` uses deliberately synthetic selectors, ownership and
registration to exercise the structural boundary. Its passing fixtures do not
supply semantic scope qualification or candidate acceptance. Tests cover the
authentic source census and rejection of missing clauses, lanes, assertion
classes, applicability decisions, prerequisites, purpose and compatibility
mappings, partial cohort designs, unrecognized revisions and physical-slot
splits. Scoped checks do not replace the root-owned combined verification gate.
