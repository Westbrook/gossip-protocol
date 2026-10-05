# Frozen probe and storage component review

Reviewer: `current_frozen_component_review`, independent read-only stream authorized by the user on 2026-10-05. Initial repository base: `main` at `917039432f8aa1b09abd3c7e52e2224169a28470`; all 117 frozen owner pins revalidated unchanged against current `main` at `663ab278beacdd50e6987aa9c5a76c4fba576c5a` when sealing. Exact byte identities and independently checked original inventories are in `evidence.json` beside this report. This is a bounded engineering readiness review, not an executable layout approval or a physical qualification result.

The prepared packets do **not** need blanket regeneration: all 117 generated-probe owner source pins and all 60 plan/evaluator pins match both current `main` and the working source. Subsequent context, ranking, corpus and accounting modules are outside these closures. The four amended storage profiles also match current source. There is useful independent work available now, but completing those reviews alone will not make the next live study runnable.

## Material findings

### F1 — The declared enclosing deadline is not propagated into source capture

**Priority: high for the advertised whole-child wall bound; source-level finding, not a measured runtime failure.**

`gossip_harness/cumulative_generated_probe_state_v1.py:190–204` checks the active window before and after `plans.verify_current_source`. That verifier (`cumulative_generated_probe_plan_v1.py:207–216`) calls `capture_registered_source` without an enclosing deadline. `candidate_source_capture_policy_v2.py:84–92` starts the retained capture with a fresh fixed 60-second timeout. Thus a boundary entered with only a small amount of time remaining can still spend up to the capture allowance plus cleanup before the post-check rejects it. The pipe's control-step deadline is likewise checked around the callback, not supplied to this nested source operation (`cumulative_generated_probe_pipe_v1.py:250–270`).

This is fail-closed concerning subsequent progress: the post-check refuses a late continuation. It does **not** establish the stronger claim that all source/setup work stops within the declared absolute window. There is no demonstrated false positive observation in this review. Synchronous journal operations also have byte bounds rather than hard interruption at that absolute deadline.

**Action:** integrate one enclosing deadline through the versioned source/control path, preserving fresh capture, kill/reap and cleanup proofs; declare the cleanup allowance separately. Root's whole-child/shared-executor work owns this change. Reconcile resulting source closure and qualification invalidation explicitly rather than reusing old timing evidence. Do not relax the control bound merely to turn a physical run green.

### F2 — Twelve prepared controls are not an implemented complete physical qualification suite

**Priority: high readiness gap, not a newly discovered behavior regression.**

`tests/generated_probe_physical_fixture_v1.py:153–218` writes inert source, schema, helper and request packets. Its eight `H01`–`H08` schedules are explicitly descriptions. `tests/test_generated_probe_physical_fixture_v1.py:15–86` checks preparation, source mutations, declared rosters and requests; it does not dispatch the candidate. The owner tests inspected here cover offline identity/construction/refusal seams. A source-bound physical runner, actual original reconstruction and retained terminal evidence are still required for the complete packet.

The missing host schedules are: admission revocation before intent; interrupted host and reopen/no redispatch; lost create acknowledgment; incorrect created sandbox settings; changed runtime/exec incarnation; malformed paused capture; interrupted/tampered retention; failed/uncertain normal cleanup. In particular, a same-process cleanup channel is not proof of cleanup after host process death. H02 needs a surviving owner/recovery strategy and explicit absence or uncertainty evidence.

**Action:** implement these under root's single verification owner and bounded resource allocation. Physical candidate runs and shared-run integration remain root-owned. Preparation tests must not be counted as these schedules or as independent model-quality samples.

## Source and control review

The semantic read concentrated on the complete probe plan/review/state/pipe/physical-owner/reader modules, fixed adapter, capture query, and selected source paths reached by the closed recipes. All candidate files were read for byte inventory and Git identity; that is **not** a claim to have semantically reviewed every line of all 61 candidate files.

The positive/negative design is coherent on the inspected call paths:

- P01/D01: actual `Service.refresh_document` delegates to the final Store; the mutant changes the returned document identity after ordinary refresh. The host checks identity continuity.
- P02/D02: equal content reaches the `unchanged` branch in `library/catalog/m4_store.py:318–343`; the mutant substitutes `refreshed`, giving a direct expected-value discrepancy.
- P03/D03: completed `commit_job` returns the stored serialized receipt (`library/catalog/m4_legacy.py:232–241`). The mutant reconstructs receipt documents from the refreshed current Store, making the replay differ from the original receipt.
- P04/D04: the submitted job's captured `content_hashes` are inspected through an immutable read-only SQLite connection and the independently declared schema/path census. The mutant persists an all-zero digest. This is a source-specific SQLite representation, not a universal mapper.
- N01 hangs before a required refresh value; N02 supplies the incorrect no-op status and then hangs during close; N03 floods output; N04 exits during refresh. The reader intentionally retains an already authenticated semantic failure while refusing a positive result after missing later mechanics. These are prospective discrimination expectations; none was physically executed by this review.

The fixed adapter keeps expected answers outside candidate request bytes, but calls and emits values in the candidate's Python process. Outputs remain untrusted. The execution owner verifies container/exec identity, user, command, prestart state, runtime and staging. The cold reader derives values and cleanup from anchored original bytes and demands ordinary removal/absence for positive completion. Review reports are exact-plan-bound and chronologically authenticated; their truth and reviewer identity remain trusted-controller duties rather than consequences of hashing.

No additional demonstrated false-acceptance defect was found within this bounded read. That statement is not proof of sandbox security, complete hostile-candidate resistance, or physical correctness.

## Exact generated-probe review requirements

Packet: `/Users/westbrook/.codex/worktrees/cumulative-reader-integration/gossip-protocol/runs/generated-probe-physical-review-packet-2`.

Each exact P01, D01, P02, D02, P03, D03, P04, D04, N01, N02, N03 and N04 request still needs independently delivered report and delivery originals. No such originals were added here. The three ordered duties in `cumulative_generated_probe_plan_v1.py:29–31` are:

1. `complete_source_and_public_invocation`;
2. `capture_representation_or_not_applicable`;
3. `bounds_release_and_remaining_scope`.

The report must bind protocol `cumulative-generated-probe-review-v1`, purpose `independent-generated-probe-source-layout-invocation-v1`, exact reviewer and request digest; each ordered decision needs rationale and actual source references; remaining obligations must be nonempty. Delivery binds that report and request with `independently_delivered_host_review`. Request/report/delivery originals are enrolled in that order under an independent ExternalHead. The closed fixture author labels are not approval evidence. P04/D04 additionally require exact schema and all three capture paths. The other controls must explain why that capture representation is not applicable.

These packets are usable **for those outstanding exact source reviews**, given the checked source pins. They are not ready-to-run approvals. Any F1 fix that changes a pinned owner/plan source requires explicit reconciliation; this result is pinned to the current bytes only.

## Storage review status

Amendment request: `/Users/westbrook/.codex/worktrees/cumulative-reader-integration/gossip-protocol/runs/workflow-shared-review-amendment-1/review-request.json`.

The six fresh requests and their source/schema originals are available under `runs/workflow-shared-review-prerequisite-1/fresh` in the retained integration worktree. P01, D01, P02 and D02 need **four amended original reports and deliveries**. Their request changes are the profile digest, `candidate_storage_product_profile_v1.py` source pin, and newly included `cumulative_finite_mapping_v1.py` pin. All remain `public_release`, with no `mapping_profile` selected. The inspected profile factory only enables the finite B02 mapping for explicit `independent_acceptance` requests (`candidate_storage_product_profile_v1.py:416–425`). Adding the pin alone does not activate those mapping semantics.

P03 and D03 fresh request bytes exactly match the original approved requests. Original request/report/delivery bytes were independently hash-checked against the retained six-control index. Preserve these two reports byte-for-byte with their original reviewer identity and provenance; do not relabel them as reviews performed by this stream. This review does not independently establish the real-world identity behind those older reports.

The four amended storage duties are complete source/persisted state; exact capture/schema/auxiliary representation; public API recipe/final-M4 inheritance; limitations/remaining obligations. The unchanged M2 P03/D03 additionally have the direct-API instrumentation/untrusted-output duty. This bounded review does not supply complete-source semantic approvals for the four amended requests, and must not be wrapped as if it did.

After receiving the four actual originals, assemble one exact six-control index in P01,D01,P02,D02,P03,D03 order, including the unchanged pair with provenance. Pin `GOSSIP_STORAGE_M2_REVIEW_PACKAGE` and `GOSSIP_STORAGE_M2_REVIEW_SHA256` in a new environment declaration. That declaration is changed input, not identical to the earlier run. Qualify the relevant changed context, use fresh physical histories/output, and retain the setup failure. The corresponding document is `docs/candidate-storage-m2-physical-qualification-v1.md`.

## What can proceed in parallel

Exact storage source/layout amendment review is the cleanest remaining independent review task: its request changes are narrow and source-valid. The 12 probe source/layout reviews can share one explicitly identified unchanged reference-tree analysis plus per-mutant analysis, while still delivering separate exact-request decisions; no candidate execution should be shared as a substitute for independent observation. Root can simultaneously finish whole-child deadline and executor integration. If that integration changes probe pins, finalize/rebind only the affected probe review artifacts afterward.

No tests, provider calls, Docker/Engine operations, candidate imports, repository edits, commits or report-state edits were made by this reviewer. No physical qualification, complete source-layout enrollment, product acceptance, statistical proof or new model-quality sample is claimed.
