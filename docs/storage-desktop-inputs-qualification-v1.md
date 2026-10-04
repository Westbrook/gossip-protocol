# Docker Desktop mount correction: qualification

The storage/M2 evaluator now accepts the one observed Docker Desktop representation
of a staged read-only `/inputs` bind: `/host_mnt` followed by the complete canonical
host path. This applies only to the explicitly qualified runtime and authenticated
original Docker responses. Other mount and sandbox checks remain strict. The
[protocol description](candidate-storage-desktop-inputs-v1.md) defines its limits.

The combined check passed **196 offline methods in 22 classes**, followed by
**six fresh physical Docker controls**. Both static gates passed. All 677 source
inputs and runtime identities remained stable; all retained test/log hashes were
reconciled. No execution receipt was reused. Existing layout reviews were reused
only because all six requests remained byte-identical for their original purpose.

The physical controls captured six candidate histories and 20 phases. Three
positive controls passed, and the three deliberately defective counterparts
produced their four expected failing observations. An additional pre-dispatch
refusal made seven attempts in total but added no candidate history. Independent
raw-evidence review found no discrepancy and verified removal of every owned
container and volume. See the [qualification checkpoint](../analysis/storage-desktop-inputs-qualification-v1.json)
for exact source hashes, raw-audit references and execution receipts.

The original run remains retained: one passed, one failed before startup on the
mount mismatch, and four did not run. The correction used a fresh execution
protocol; it did not erase that failure or reuse its passing observation. No
candidate or cleanup deadline was widened.

This qualifies these sandbox/observer controls. Twelve product-process batch
controls, the browser and controller successors, complete project acceptance,
and the matching full rehearsal remain unfinished. All 107 mapped-unit semantic
reviews are reconciled, but mapping corrections and 205 unmapped units remain.

**This cycle adds zero model-quality samples.** The [scientific evidence status](../analysis/scientific-evidence-status-20261004.json)
reports the actual live comparisons separately; the larger 20-role comparison
and a direct gossip-versus-orchestrator result remain unavailable.
