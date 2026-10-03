# Finite CLI harness v2: retained failed qualification

The v2 harness passed its static gate and all 117 offline checks. Its first
physical qualification failed: one synthetic positive control completed, while
the reference persistence history and domain-error control stopped before their
first candidate process started. The remaining control methods and the bulk
reference roster were not executed. This is a harness diagnostic, not product
acceptance or evidence for either coordination approach.

## What changed prospectively

The prior v1 failure occurred when Docker changed the optional
`HostConfig.OomKillDisable` representation from false to null during startup.
V2 adds a typed, runtime-bound exception for that single transition, preserving
the full raw inspections and explicit policy/comparison digests. All other
fields and subsequent keeper identity remain exact. It also fixes a latent
evidence-path collision by retaining controller and process intent records
under distinct names. Both durable before-action barriers remain present.
Initial sandbox values use strict JSON type comparisons.

The shared provider is unchanged: 57 histories, 305 finite invocations,
900 assertion cells and 32 explicitly unspecified cells. All 11 synthetic
control sources, arguments and expected outcomes remain unchanged. Frozen v1
source and failed evidence are preserved. V2 creates no production acceptance
or registry authority.

## Executed gate

The root-owned run used four resource tokens, separate class workers and no
reused class observations. Static checks completed before the offline suite,
which completed before Docker execution.

| Observation | Result |
| --- | --- |
| Static gate | Passed; 117 existing baselined diagnostic debts unchanged |
| Offline methods | 117 passed |
| Docker methods selected | 2 failed, 6 not run |
| Additional bulk Docker methods | 3 never launched |
| Total gate elapsed time | 101.9858465 seconds |
| Source/runtime drift | None detected by the runner |
| Provider calls / comparative samples | 0 / 0 |

This elapsed time measures development qualification, not an agent-approach
benchmark. Type checks cover their explicit roots; they do not establish that
every legacy dynamic function is type-safe.

The physical controls class entered `semantic-success` and `domain-error`;
its other nine control histories were not run. The persistence class entered
`cli-db-root-isolation`, but its first invocation did not start. All 56 bulk
reference histories, containing 296 commands, remain unexecuted.

## Failure boundary

Both stopped histories retained `ProcessError: Container changed before start`.
The original CLI-created inspection and the transport's Engine prestart
inspection contain the same complete mount records in different array order.
V2 compares the entire array positionally and therefore rejects those snapshots.
The positive control happened to receive matching array order and completed
with natural exit zero and complete, independently separated streams.

This diagnosis does not reinterpret either failed run as a pass. The saved
startup compatibility comparisons remain separate from the new prestart
comparison failure. Any future mount-inventory policy must be versioned and
qualified before use, retain the original order and raw records, reject
duplicate or missing destinations, and compare every complete mount record.
It must not normalize arbitrary arrays or weaken source, mount permission,
sandbox, lifecycle, journal, cleanup or final acceptance checks.

Independent raw-evidence audit found no retained-evidence discrepancies.
Three candidate containers were created and removed; only the synthetic
positive control was started and completed naturally. The two failed histories
were still in Docker's created state when their abort paths attempted a kill;
Docker returned 409 because those processes had never started. Three trusted
keepers and three state volumes were created and removed with retained absence
checks. The audit reconciled 283 journal files with 283 external checkpoints,
66 CLI control records with 132 retained streams, and 20 raw HTTP pairs.

The [prospective inventory plan](../analysis/candidate-b03-mount-inventory-plan-v1.json)
is independently reviewed and source-grounded. At the pinned Moby commit,
`GetMountPoints` appends values from a map without sorting, so array order is
not a stable identity. The plan requires raw inspection decoders to reject
duplicate JSON keys before constructing a unique-destination inventory. It
then compares complete rows with strict types. This correction is **not yet
implemented or qualified**. The next version must pass one persistence history
plus all 11 controls before the remaining 56 reference histories run. Passed
persistence observations will not be repeated merely to assemble that total.

No v2 retry was launched, and no failed observation was reclassified.

## Retained evidence

- [Qualification checkpoint](../analysis/candidate-b03-v2-qualification-checkpoint.json)
- [Independent physical audit](../analysis/candidate-b03-v2-physical-audit.json)
- [Terminal runner result](../analysis/candidate-b03-v2-controls-result.json)
- [Prior v1 qualification](candidate-b03-v1-qualification.md)
- [Prospective startup compatibility plan](../analysis/candidate-b03-runtime-compatibility-plan-v1.json)
- [V2 process contract](candidate-client-process-v2.md)
- [V2 execution contract](candidate-client-execution-v2.md)
- [V2 observation contract](candidate-client-observation-v2.md)
- [V2 physical qualification plan](candidate-client-qualification-plan-v2.md)

The exact source/definition/runtime freeze, raw local Engine records,
checkpoints and focused-check logs are retained under the corresponding
`runs/candidate-b03-v2-*` directories. Raw runs remain outside ordinary test
discovery.

## Remaining research scope

C01/C02 qualification remains incomplete. The other B01–B05 coverage,
production scope and prerequisite authorities, whole-controller rehearsal,
four cumulative milestones and six matched development trajectories remain
required. Quality and sustained project completion remain primary. Independent
held-out project families and a prespecified analysis are still needed for
claims about general swarm advantage; harness controls are not model samples.
