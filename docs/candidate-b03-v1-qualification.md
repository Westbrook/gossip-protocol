# Finite CLI qualification: first physical boundary result

The B03 finite CLI harness is implemented, but **physical qualification failed before any candidate command ran**. Both attempted histories stopped when Docker changed `HostConfig.OomKillDisable` from `false` in the created container to `null` in the running container. The exact identity comparison treated that representation change as a changed configuration.

This is a measurement-harness compatibility failure. It supplies no candidate-quality or swarm-versus-orchestrator result. The [checkpoint](../analysis/candidate-b03-v1-qualification-checkpoint.json), [terminal verification summary](../analysis/candidate-b03-v1-controls-result.json), and [independent retained-evidence audit](../analysis/candidate-b03-v1-physical-audit.json) preserve the finding.

## What was implemented

The authored provider contains 57 histories in 19 families and 305 separate CLI invocations. It defines 900 assertion cells: 868 supported by the frozen requirements and 32 explicitly unspecified. Unclear import/jobs wrappers and error presentation do not become invented acceptance criteria. Expected answers stay on the host.

Each invocation is designed to run as the main process of a fresh container. A separate bounded trusted holder preserves the isolated tmpfs database between commands. Source, fixtures, command, runtime, limits, durable intent, completion, raw streams, and owned-resource cleanup are bound independently. The provider no longer needs ignored local receipts to work in a fresh checkout.

Independent source review resolved missing cleanup accounting, insufficient initial source/mount binding, a pre-start output-stream race, and a failure-summary path that could lose earlier observations. Details are in the [execution contract](candidate-client-execution-v1.md), [process protocol](candidate-client-process-v1.md), [observation policy](candidate-client-observation-v1.md), and [prospective qualification plan](candidate-client-qualification-plan-v1.md).

## Executed evidence

| Check | Result |
| --- | --- |
| Combined static gate | Passed; 117 existing type diagnostics remain explicitly baselined |
| New offline tests | 94 passed |
| First physical methods | 2 failed at the same keeper boundary |
| Remaining phase-one methods | 6 not run |
| Bulk physical methods | 3 not launched |
| Candidate containers / processes / captures | 0 / 0 / 0 |
| Trusted keepers / owned volumes | 2 / 2; both pairs removed with retained absence checks |

The terminal central run took 96.759 seconds with four resource tokens and no reused classes. This is engineering verification time, not project completion speed or an agent comparison. Two earlier pre-dispatch failures remain retained: a combined type check exposed an optional endpoint, then the explicit endpoint guard exposed a missing fake endpoint in one mocked cleanup test. The focused correction passed before the final combined run.

The independent audit reconciled 332 checks, 452 runner inputs, 18 frozen source files, five normative source identities, and 41 reference-source identities. It verified 94 incremental external checkpoints, 94 journal files, 28 control records, 56 raw control streams, and 34,441 raw bytes. The sole difference in the compared keeper configurations was the field above. No retrospective normalization or passing verdict was applied.

Raw execution directories and three pre-execution source freezes remain locally retained under `runs/`; the linked committed audit and summary identify them by hash. The audit establishes consistency of retained trusted-controller evidence, not an external attestation or a fresh daemon census. Transient staging bytes were not independently retained; Git blobs and source/fixture manifests were reconciled.

## Next boundary

Preserve this v1 baseline. The [prospective compatibility plan](../analysis/candidate-b03-runtime-compatibility-plan-v1.json) binds the observed runtime to matching official Docker source, whose startup validation clears this unsupported cgroup-v2 option. Define a new, narrowly versioned runtime compatibility profile for that exact transition, with regression coverage for rejecting `true` and unrelated configuration changes. Full raw records, source identity, confinement, and exact running-holder continuity must remain checked. A new source freeze and complete controls/persistence qualification precede the remaining 56 reference histories; their 296 commands remain unrun.

This finite slice does not close B03, all CLI requirements, production acceptance authority, or the complete four-milestone/six-trajectory study. HTTP, browser/export, process recovery, release, inherited requirements, production integration, complete-cohort freeze, and fresh independent acceptance remain required.

The [statistical design review](cumulative-confirmatory-design-check-v1.md) also preserves the distinction between six trajectories in one development family and independent held-out project families. **No statistical advantage is established.** No provider calls, spending, or public-site refresh occurred in this cycle.
