# Compact journals in candidate execution

The current CLI and HTTP product-observation owners now use the compact journal
and an independently owned durable head. Their execution protocol identifiers
change to `candidate-client-execution-v5-compact-v1` and
`candidate-http-execution-v4-compact-v1`. Historical finite-V4 and HTTP-V3 sources
and their physical observations remain unchanged.

This closes an integration gap between the compact storage foundation and actual
executor code. It does not supply the full M4 acceptance contract or execute a
new agent cohort. The [machine-readable checkpoint](../analysis/compact-executor-integration-v1.json) records verification and the
remaining qualifications without treating component checks as product evidence.

## Evidence consumption

Callers must supply a durable external head, separate raw/delta/cleanup roots,
and an independently held prefix when reopening. Physical owners require the
durable host implementation. A reopened journal authenticates the complete
prefix before its configuration can be parsed. Large declarations remain fully
retained while small, independently recomputed identities bind the journal
context; the context limit is not increased to accommodate a large recipe.

Appending a file retains original bytes once and advances one acknowledged
delta. Every consumed raw record is authenticated. Full inventory checks occur
at explicit execution and observation boundaries; membership in an acknowledged
prefix alone is not proof that no foreign file exists on disk. Source, runtime,
prospective admission, purpose and cohort checks remain additional requirements.

Host-authored CLI and HTTP verifier records are retained in the same chain. Their
stable identity uses the journal context and immutable configuration/terminal
records, avoiding a self-referential final checkpoint. Consumers rederive the
observations from authenticated originals and check the independently held
post-verifier prefix. A serialized claim about an earlier checkpoint is not an
ancestry or acceptance capability.

## Failure cleanup

An uncertain acknowledgement stops normal retention, dispatch and acceptance.
A separate bounded cleanup channel uses previously acknowledged resource
claims, exact source/runtime/run identities and fresh resource inspections.
Container deletion addresses verified full IDs. Volumes are considered only
after container cleanup, with their original full identity preserved. Unknown
creation or removal results remain explicit; cleanup does not retry a normal
removal whose outcome is unresolved or revise the main observation into a pass.

Confirmed normal removals retain exact removal and absence evidence, so a later
failure does not make every previously cleared resource appear unresolved.
Emergency diagnostics remain outside the poisoned main journal. Completing
emergency cleanup grants no product acceptance or permission to resume execution.

A narrow retention wrapper also closes a frozen-process teardown hole. It
preserves the V4 wire format and blocks every subsequent request after a
retention failure. Only the final attachment capture callback may return without
retention, at the pinned location where the next action closes the host socket.
The original exception is always re-raised; no observation is returned. This
provides retention-triggered teardown under ordinary socket behavior, not
immunity to repeated asynchronous interruption or arbitrary OS close failure.

## Offline verification

The central static gate and all **262 affected checks** passed in 16 fresh,
isolated class workers, with no reuse, skips or source/runtime drift. The run
bound 551 inputs and took 100.70 seconds. The configured type coverage retains
117 known legacy diagnostics; no new baseline debt was introduced.

Coverage includes reopen-before-parse, lost acknowledgements, raw substitutions,
prospective admission revocation, same-chain verifier publication, large complete
history configuration, cleanup ownership and protected reserves, keeper lifetime,
and attachment teardown after retention failures. Six prospective CLI/HTTP
physical controls were adapted but were not executed in this offline round.

## Qualification boundaries

The original twenty-method HTTP mechanics run is now fully independently
audited; see [its separate checkpoint](candidate-http-v3-physical-audit.md).
It does not physically qualify this changed execution contract. Likewise, the
earlier 99.82% metadata reduction is a fixed-corpus storage measurement, not a
measurement of these executors or a whole-study speedup.

The remaining work includes fresh complete physical controls for this contract,
M4 applicability/comparator integration with independently reviewed semantic
authority, the durable financial cohort seal, the complete four-milestone and
six-trajectory controller/acceptance path, and its matching rehearsal. The live
comparison and held-out confirmation remain outstanding. No new model-quality
sample, statistical ranking, calibrated Elo or general superiority claim is
created by this integration.
