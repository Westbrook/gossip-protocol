# Candidate release execution v1

Status: development boundary qualified by 95 targeted checks, including two
fresh physical package controls. This is a new execution boundary for the
cumulative comparison. Existing M4 reference receipts do not
become candidate acceptance receipts, and this document does not authorize a
live study or declare whole-project completion.

## The evidence boundary

The controller registers an exact Git commit/tree, candidate source inventory,
ordered gate, purpose and declared repetition before invocation. Binary assets
are included. Git object IDs, hashes of raw file bytes, the source-manifest hash,
and any legacy text-JSON source hash are distinct identities.

A controller-owned journal retains admission and an immutable intent before any
candidate execution. A physical slot cannot be made fresh by changing a request
label. An interrupted intent without complete terminal evidence remains unknown;
reopening does not silently rerun it. Recovery of an existing original is not an
additional physical observation. The controller must durably retain the journal's
external checkpoint and prevent allocating a second root for the same registered
slot. The optional `checkpoint_sink` receives the controller checkpoint after
each retained write. A crash between that write and external publication leaves
an unverifiable suffix; reopening rejects it rather than treating it as evidence.
Local hashes do not provide a global slot registry.

The authority comes from the trusted controller owning dispatch and journal
storage. Hashes make substitution detectable relative to the retained identities;
they do not prove that arbitrary JSON was produced by a real execution. Candidate
programs cannot supply success flags or replace the registered evaluator. The
host/operator, Docker daemon and evaluator installation remain trusted. Start
the controller in a fresh process against a frozen installation; hot module
replacement is not a supported study runtime.

## Observe the package that was delivered

The physical path invokes the candidate's actual release command in a
pinned, isolated build container. Its output lives in a dedicated, bounded
Docker-managed tmpfs volume, with exact per-execution ownership and mount options.
The controller pauses every process in that container, verifies the frozen state
and expected mount, captures a bounded archive, rejects unsafe release entries,
and removes owned build resources. The captured capsule has exact file names,
lengths and byte hashes.

A separate fresh container receives those captured package files read-only.
Candidate CLI calls run as an unprivileged user; the host retains original
stdout/stderr, exit status, effective limits, runtime identity and cleanup
observations. No candidate module is imported into the host evaluator. Expected
values stay on the host. A release whose source implementation works but whose
emitted package is defective must fail the installed-package observation.

The host verdict is separate from the original execution artifacts. Parsing,
framing, ordered observations and complete execution provenance must qualify
before product assertions can be judged. Missing output, interrupted execution
and failed cleanup remain infrastructure/unknown outcomes. An observed product
assertion mismatch is a correctness failure. Neither can be retried away.

## What this does and does not qualify

The first physical gate has three ordered assertions: release build output,
manifest/file-byte completeness, and an installed CLI import/list/show/export
roundtrip through four separate processes. Its delivery profile requires the
registered source tree to contain the public deliverable only, with every
admitted file included in the package. A repository with source-only development
files needs a separately registered delivery profile before this gate applies.
It is not a universal product packaging policy.

Repeat-build determinism, no-clobber publication, inherited behavioral histories,
HTTP/browser behavior, migration/crash checks and release documentation coverage
remain separate required gates. The partial association with
`M4-RELEASE-HANDOFF` must not be mistaken for complete coverage of that group.

This version treats nonzero Docker-exec status conservatively as infrastructure
unknown: the CLI does not independently prove whether a product process ran or
whether exec transport failed. It retains the raw status and output without
retrying. Error-path product assertions that require a known nonzero product
exit need a later observer with authenticated exec completion. This limitation
must remain visible in any study using this protocol.

Independent acceptance and repeatability require a trusted complete-cohort
freeze capability at admission and completion. A unit-test callback can qualify
that interface, but cannot prove the future six-trajectory controller has stopped
all model actions. No concealed result may return to an active repair loop.

The full registry still requires the complete product inventory, separately
qualified harness/scope/oracle prerequisites, exact promotion evidence and all
required independent gates. A public gate alone cannot establish whole-project
acceptance. The prospective product-contract v2 has separate semantics and needs
its own implementation and qualification; the v1 reference is only a declared
control for this new execution mechanism.

## Full comparison still required

The experiment remains four cumulative milestones across six matched
trajectories: 4-builder gossip, 16-builder gossip and a matched 16-builder central
controller, each under healthy and compound-fault conditions. Peer-local policy,
transport and placement remain separate factors. Required evidence includes
generated distinguishing tests, actual restart/partition faults, source-bound
integration and promotion, the whole-cohort barrier and independent final
confirmation. Quality and sustained completion are the primary outcomes.

A working release executor is infrastructure for those observations. It is not
statistical evidence that larger swarms improve software quality.

## Controller API

Construct `CandidateReleaseExecution` with a controller-owned canonical root,
`GitStore`, exact `Registration` and pinned `ReleasePolicy`. Registration binds
the product gate, commit/tree, declared repetition and complete six-trajectory
roster. Supply the trusted cohort-freeze capability for independent or
repeatability purposes. `execute_once()` dispatches at most once for that slot;
`verified_execution()` validates the original retained artifacts. To reopen,
supply the exact independently retained `ControllerCheckpoint`. Fixture mode
cannot authenticate a physical candidate execution.

## First integration failure retained

The original container attempt built all 55 reference files, paused the build
container and verified its state. However, `docker cp` returned a 1,536-byte tar
containing only the underlying empty `tmp` directory. Docker documents that its
[copy command does not capture native tmpfs mounts](https://docs.docker.com/reference/cli/docker/container/cp/).
The candidate consumer never ran; this is a harness capture failure, not a
candidate correctness result. The run is retained under
`runs/cumulative-acceptance-qualification-1/20261003T090756-06510766`:
92 offline checks passed, one physical control failed, and the second was not run.

The correction uses a dedicated Docker-managed local tmpfs volume, a supported
[volume driver configuration](https://docs.docker.com/reference/cli/docker/volume/create/).
The volume has an explicit 32 MiB bound and registered ownership/options. The
builder stays paused during capture. The fresh correction run established actual capture,
installed-package behavior, journal identity and owned-resource cleanup.


## Qualified development checkpoint

The combined run `runs/cumulative-acceptance-qualification-2/20261003T091844-9c3af9fd`
passed all 95 selected checks: 81 fast, 12 Git/journal and two real Docker
controls, with no skips, reused classes or stale inputs/runtime. Static gates
passed with the unchanged declared legacy type baseline; this does not claim
all legacy code is fully typed. The retained first failure is not erased.

The valid package passed build, manifest and installed CLI assertions. A builder
that corrupted its emitted `library/__main__.py` after packaging passed build
but failed both manifest and CLI assertions. Both controls verified removal of
their two containers and owned volume. Reopening the exact external checkpoint
returned the original execution without a new dispatch.

The [machine-readable checkpoint](../analysis/cumulative-acceptance-boundary-checkpoint-v1.json)
binds sources, original observations, source reviews and remaining limits.
These authored controls use the frozen v1 reference and a synthetic public-gate
registration. They do not establish the prospective v2 product implementation,
a complete production ScopePlan, independent six-trajectory acceptance or a
comparative quality result. No provider calls or API spend were incurred.
