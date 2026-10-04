# Product process observation v1

This versioned observer executes eight declared CLI/HTTP product histories against an admitted candidate source in isolated containers, retains the original captures, and projects only explicitly asserted facets into acceptance evidence. It extends coverage beyond request parsing to restart persistence, export bytes, backup/restore and seeded migration. This implementation and its offline tests are not evidence that those container histories have already run.

The eight histories declare 150 actions: 84 HTTP calls, 32 finite CLI invocations, 17 server starts and 17 stops. They cover worker-once enrollment/retry, reindex followed by restart, exact export bytes, backup destination handling, root adoption/restore, and schema-0/schema-2 to schema-4 seeded migration. The trusted setup creates the declared backup and alternate-root directories and copies the frozen SQLite seed before candidate execution.

The modules separate closed case definitions, prospective profile/recipe construction, execution ownership, authenticated original reading, and semantic projection:

- `candidate_product_process_cases_v1` and `candidate_product_process_core_v1` define the histories and exact setup.
- `candidate_product_process_execution_v1` binds candidate Git source, evaluator sources, purpose and admission before dispatch; it retains execution and cleanup under the compact checkpoint.
- `candidate_product_process_reader_v1` reads authenticated originals without candidate dispatch. It still needs an Engine runtime refresh, so it is not an Engine-free reader.
- `candidate_product_process_observation_v1` maps concrete observations to the closed selector catalog and verifies the loaded evaluator around projection.

The evaluator closure uses canonical `gossip_harness/` source names understood by the admission loader. A retained pre-integration failure exposed an unsupported `product/` namespace. The correction was checked with the real loader and a negative control that replaces an in-memory comparator while preserving its disk source map. The shared admission implementation was unchanged.

The catalog has 934 selectors, including 33 explicitly unspecified facets. Auxiliary CLI diagnostics, duplicate values in permitted diagnostic output and undefined import-wrapper shape do not become invented failures. Unsupported wire encoding or unavailable observations stay distinct from product rejection. Parsed JSON does not establish exact export bytes, command invocation does not establish its effects, and an error response alone does not prove that no filesystem operation occurred.

This version does not complete daemon crash/recovery, concurrent owner contention, all migration receipt replay, source inspection, browser behavior or the full product contract. The eight histories form a declared public product profile; they are not relabeled as independent held-out acceptance. Every independent or repeatability purpose requires its own prospective binding and execution.

The [scope registration layer](cumulative-scope-source-registration-v1.md) preserves those limits. Its census across CLI, HTTP and product-process definitions counts 341 declared histories for one public purpose, with narrow assertions touching 37 of 312 source units and 23 of 123 product requirement groups. None of those counts means 37 fully covered units or 341 successful physical executions. All remaining applicability and adequacy decisions remain explicit.

The [combined integration checkpoint](../analysis/cumulative-controller-product-scope-integration-v1.json) records offline qualification. Twelve separate physical controls have been authored outside this batch: eight positive histories and four deliberately defective candidates. They still require installation and actual Docker-lane execution; their declarations grant no physical pass.
