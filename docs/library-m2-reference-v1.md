# Cumulative M2 reference qualification

This cycle implements the second milestone of the Local Research Library as a
trusted authored reference. It supplies an executable target for qualifying the
longer swarm experiment. It is not a model-produced project, a comparative result,
or evidence that more agents improve quality.

The [cumulative product contract](library-cumulative-product-v1.md) remains the
normative M2–M4 specification. The frozen M1 sources and live-pilot checkout are
unchanged. The new reference is assembled by
`gossip_harness.library_m2_reference_v1.m2_files()` from additive catalog,
Service/CLI/HTTP and browser overlays. It must never become a candidate seed,
model prompt, or public answer supplied to experimental builders.

## Implemented product behavior

- Atomic schema-0-to-2 migration, immutable content revisions, document edit
  tokens, and one catalog-generation increment per effective transaction.
- Explicit content refresh, normalized annotations, collection membership,
  soft deletion/restoration, and generation-bound filtering and pagination.
- Cumulative ingestion: admission still sees tombstones for conflicts and
  capacity; completed jobs replay their original receipts after later edits.
- A shared durable Store behind Python, command-line, HTTP and accessible browser
  controls. Stale edits fail without automatic retry or loss of the unsaved draft.

M3 backup/restore, worker recovery, reindexing and bounded export, and M4 migration
and release behavior remain unfinished. The M2 browser preserves legacy export;
the new bounded export controls belong to the separately declared M3 endpoint.

## Evidence boundaries

The composition check verifies that authored package overlays preserve immutable
inputs. Cumulative smoke runs the frozen M1 public histories against the combined
M2 application, not merely the old standalone M1 source. Catalog and client
checks exercise actual SQLite state, subprocess CLI and HTTP transport. Physical
browser checks use pinned Playwright/Chromium, isolated contexts and an owned
loopback service containing only the checked-in authored reference.

M2 acceptance scenarios and expected values are authored separately from the
implementation. Their case-definition hash is frozen before running the new
reference. These observations qualify the scenario machinery and reference; they
do not constitute post-freeze acceptance of an experimental candidate. The
whole-cohort barrier, trusted receipt adapters, complete requirement coverage,
and explicit promotion evidence are still required for the later study.

The [machine checkpoint](../analysis/library-m2-reference-checkpoint-v1.json) records
34 fresh passing checks across seven classes, with no skips or reused results.
The sandbox passed 12 independent histories and eight inherited public histories;
a deliberate missing edit-token increment was rejected. Real browser workflows
and desktop/mobile viewport checks passed. Retained earlier failures and exact
source/configuration bindings remain linked from the checkpoint.
A passing subset must not be interpreted as full M1, M2 or four-milestone project
acceptance. There are no new model calls or API charges in this cycle.

## Remaining experiment work

Finish the M1 acceptance adapters and M2 public development fixtures, then the
M3–M4 reference and independent histories. Qualify the actual peer-local planning
policy and matched central control, generated distinguishing tests, real process
restart/partition recovery, and the all-trajectory acceptance barrier. Preserve
all six trajectories and the independent held-out confirmation; this reference
implementation does not reduce that scope.
