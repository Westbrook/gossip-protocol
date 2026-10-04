# Explicit batch source capture in the prospective storage owner

This change uses the reviewed four-command Git capture through actual storage
registration, owner construction, every repeated `current()`/effect boundary,
observer entry/final validation and reopen. It adds a closed choice to the
prospective B01/B02 owner and cumulative-v2 source factory. It does not replace
frozen release, product-process, V5, study-source or terminal-original capture
functions. Those independent boundaries remain fresh legacy captures.

The caller opts in by creating `BatchCapturePolicy()` and using
`execution.capture_source(store, commit_oid, policy=capture_policy)` when it
constructs registration inputs. It then passes the same exact policy to
`execution.binding_for(..., capture_policy=capture_policy)`. The resulting
binding protocol is
`candidate-storage-product-execution-v1-ascii-json-v1-git-source-batch-v1`.
The ordinary `CandidateStorageExecution` constructor derives its closed capture
choice from that registered protocol; no arbitrary function, subclass or callback
can choose another implementation. No owner is copied, globally patched or
loaded dynamically.

The shared `candidate_source_capture_policy_v1` module exposes:

- `BatchCapturePolicy(timeout_seconds=60, cleanup_reap_seconds=5)`, accepting only
  those exact integer values. `record()` returns the explicit helper protocol,
  whole-capture deadline contract, fixed bounds and both exact source hashes. Loaded helper constants must also match this
  fixed contract; changing a deadline or bound in memory cannot select another
  policy under the same registration.
- `capture_registered_source(store, commit_oid, *, policy=None)`, invoking fresh
  release-v2 capture for `None` or fresh batch capture for the exact policy type.
- `evaluator_sources()`, binding both the policy and unchanged reviewed batch
  helper, with loaded-file hash checks.
- `SourceCaptureUnavailable`, which distinguishes unavailable/late/failed batch
  capture from a candidate product assertion. Cleanup interruptions still escape
  unchanged rather than becoming product results.

The batch deadline is a whole-capture monotonic 60 seconds, including parsing,
with a separate five-second cleanup reap allowance. It is not an extra deadline
per Git command. The helper retains all source-path, object-type, size, complete
tree, exact byte and Git hash checks. Each invocation launches four fresh Git
commands; no result or metadata survives as a substitute for the next capture.
An OS call that fails to return still cannot be preempted by a Python deadline.

## Bound identity and unchanged defaults

For opted-in storage, the policy record is included in `limits_sha256` and the
original authenticated `config.json`; the execution protocol propagates into
its gate, context, intent, application record, terminal and observed result.
The evaluator closure includes the policy and helper source bytes. The observer
uses the owner's actual registered execution protocol. The cumulative factory
includes the complete policy record beside the registration in its immutable
factory input, so source/scope review and subsequent reconstruction see the same
contract. Missing or changed records fail reconstruction. The layout reviewer
still authenticates the exact source and original layout request independently;
selecting batch capture supplies no layout or semantic review authority.

Default policy selection remains `None`. It keeps the legacy capture function,
legacy per-command deadlines, existing execution protocol, legacy limits object,
and existing config/factory-input shapes without a `source_capture` field.
Default config **bytes do change** because the evaluator source closure and
source fingerprints change. Prior source-bound receipts and review inputs are
invalidated; no historical receipt is relabeled as qualification of this tree.

Public signature changes are limited to adding optional `capture_policy=None`
to `binding_for(...)` and `selector_catalog(...)`. New `capture_source(...)` and
`capture_policy_for(protocol)` expose the closed selection. Existing
`StoragePolicy`, `StorageBinding` field layout, `StorageRegistration` fields,
owner constructor, `current()`, effect/action methods, layout-review authority,
observation-source constructor, and cumulative slice factory signatures remain
unchanged. `StorageBinding.protocol` now admits exactly two enumerated strings.
The cumulative `factory_input_json` has the additional policy record only for
batch bindings; its verifier reconstructs the exact original shape and bytes.

## Concrete dispatch prerequisite correction

An inert M2 staging control exposed an existing storage composition defect:
`DockerValidator(image, {})` rejects empty trusted checks before dispatch. This
successor now passes the same exact adapter/recipe/application text subsequently
staged under `/checks`. The real sandbox constructor still validates safe paths
and nonempty trusted text; no sandbox requirement is relaxed. This also repairs
the legacy-default dispatch path and is explicitly additional to preserving its
source-capture defaults. A no-Docker control enters both B01 and B02 staging,
checks the real constructor and exact staged helpers, and stops at cleanup-channel
creation before any Engine or command effect. This does not qualify the physical
run; all sandbox/start/cleanup checks remain required there.

## Verification scope

The isolated real-Git regression uses actual object reads, real checkpoint/head
journals and synthetic layout-review originals. It exercises default dispatch,
batch registration, repeated before/after boundary calls, observer review/final
checks, reopen, complete protocol/limits/config/source binding, altered scope
input, mixed legacy/batch bindings, tampered retained config and missing source
objects. It rejects attempts to widen the deadline or pass a caller implementation.
Candidate source deliberately raises if imported on the host; it is only read as
Git bytes. Fixture owners stay barred from physical dispatch and Registry
acceptance. Calling each effect boundary in a fixture is not evidence that a
physical candidate history ran.

These checks establish opt-in integration mechanics. They are not Docker
qualification, acceptance results, runtime benchmarks or measured speedups. Root
owns combined tests and source fingerprints after installation, followed by new
matching physical qualification and complete rehearsal for the changed execution
contract. Full cohort size, all history/purpose slots, sandbox checks, admission,
ordering, source checks, independent review, original bytes and final barriers
remain required.
