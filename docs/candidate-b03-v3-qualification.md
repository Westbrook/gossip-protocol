# Finite CLI harness v3: partial qualification and retained startup error

The v3 harness passed its static gate, all 139 offline checks, the nine-command
reference persistence history and ten synthetic transport controls. The final
missing-executable control stopped before startup because the sandbox validator
rejected Docker's explicit `Config.Cmd: null` representation for a command with
no arguments. The qualification remains failed; the other 56 reference
histories were not launched. These are harness observations, not comparative
software-project samples or acceptance of the complete product.

## Prospective implementation

V3 compares Docker's top-level `Mounts` as a destination-keyed inventory of
complete, strictly typed rows. It preserves raw row order and full inspections,
rejects duplicate/missing/noncanonical destinations, and leaves all other arrays
ordered. The earlier runtime-bound OOM-default startup exception remains a
separate subrule. Durable prestart comparison evidence precedes attach/start.

The ordinary controller and process transport reject duplicate raw JSON keys
before conversion. A later audit found that the exceptional raw startup-control
helper still has four permissive `json.loads` callsites. The original source
review's claim that every raw CLI path used strict decoding was too broad; its
[supplement](../analysis/candidate-b03-v3-source-review-supplement.json) explicitly
corrects that claim. No duplicate keys were found in the
actual retained responses at those sites. That source-level gap is separate
from the observed argument-validation error and remains a required correction.

A follow-on source review also found that the missing-executable control accepts
`start_error` without independently requiring a complete daemon rejection
response. The transport can use the same status for a lost or malformed start
response. This latent false-positive path was not exercised by v3, which stopped
earlier. The next version must distinguish an evidenced daemon rejection from
transport uncertainty; a status label alone cannot qualify the control.

The shared definitions remain 57 histories, 305 commands, 900 assertion cells
and 32 explicitly unspecified cells. The corrected-v2 reference bytes and all
11 controls' sources, literal arguments, expected outcomes and limits are
unchanged. Earlier frozen versions and their failed observations are preserved.

## Executed gate

| Observation | Result |
| --- | --- |
| Static gate | Passed; 402 source files, 119 explicit type roots |
| Existing type-check diagnostic debt | 117, unchanged |
| Offline methods | 139 passed |
| Selected Docker methods | 7 passed, 1 error |
| Combined result | 146 passed, 1 error; failed qualification |
| Total gate elapsed | 206.41068275 seconds |
| Source/runtime drift | None detected by the runner |
| Reused class observations | 0 |
| Bulk reference methods | 3 never launched: 56 histories, 296 commands |
| New provider calls / comparative samples | 0 / 0 |

The root-owned runner used four resource tokens. Static checks preceded offline
checks, which preceded Docker execution. Explicit type roots and their imports
do not establish that every legacy dynamic function is type-safe. This elapsed
time measures harness development qualification, not agent approach performance.

The passing controls cover semantic JSON and opposite-stream diagnostics,
domain/usage exits, arbitrary binary stream separation, capture overflow,
wrong exit/channel/value, forged completion followed by a hang, and ambiguous
mixed-stderr framing. The authored transport-control verdict remains separate
from each synthetic source's registered product-case verdict. A transport
positive does not turn an incompatible product-case result into a pass.

## Independent evidence audit

The retained evidence reconciles without integrity discrepancies. The audit
reconstructed 86 identity comparisons, including 13 actual mount-order changes,
and matched 1,547 journal files to their external checkpoints. These are audit
checks, not additional test executions or independent project samples.

Twenty candidate containers were created and removed; nineteen processes
started and eighteen completed naturally. Seventeen had complete captured
streams. The output-limit control exited naturally but retained truncated
output and no passing product observation. The hanging control required a
verified kill. The missing-executable container never reached transport.
All eleven trusted keepers and twelve history volumes have retained removal
and absence evidence.

The reference persistence history has 25 supported assertions true and two
explicitly unspecified assertions. No reference assertion is unavailable or
failed in that history. The remaining 56 histories contain 873 unexecuted
assertion cells, including 30 unspecified cells. They remain outside the
qualified scope.

This is an audit of local trusted-controller retention and raw Engine traffic,
not external attestation or a fresh daemon census. Historical environment
values and socket wall-clock scheduling were not independently reconstructed.

## Startup failure boundary

The failed control declared one argv element, the executable, with no additional
arguments:
`/workspace/absent-control-executable`. The created container retained:

```json
{
  "Path": "/workspace/absent-control-executable",
  "Args": [],
  "Config": {
    "Entrypoint": ["/workspace/absent-control-executable"],
    "Cmd": null
  }
}
```

The controller's recipe/identity check matched the declared executable and
empty argument list. `validate_sandbox` then required `Config.Cmd` to be a list
and raised `Candidate argv differs`. The container stayed in `created` state
with PID zero. No transport start request or natural completion was recorded;
container and volume cleanup completed. This did not observe Docker's behavior
when starting a missing executable.

The prospective correction must validate this exact no-argument representation
without rewriting raw JSON or treating `null` and `[]` as equivalent identity
values. Missing keys, wrong types, nonempty arguments, unexpected entrypoints
and recipe drift must remain rejected. It must also strict-decode all four raw
startup-helper paths before use and independently prove the daemon's start
rejection from complete retained response evidence. The control's literal executable and arguments
must stay unchanged. The correction is not implemented in this checkpoint, and
the failed v3 result receives no retrospective pass credit.

The reviewed successor design also requires a distinct durable start-response
completion receipt before the transport cleanup block, with the final transport
result binding that receipt after that block. Owned-container removal follows
the retained result. A separate qualification-only rejected-start
comparison preserves raw inspections and checks the source-supported OOM default
transition without inventing a natural completion or product pass.

## Retained evidence

- [Qualification checkpoint](../analysis/candidate-b03-v3-qualification-checkpoint.json)
- [Terminal runner result](../analysis/candidate-b03-v3-controls-result.json)
- [Independent physical audit](../analysis/candidate-b03-v3-physical-audit.json)
- [Prospective empty-command correction](../analysis/candidate-b03-empty-command-plan-v3.json)
- [V3 process contract](candidate-client-process-v3.md)
- [V3 execution contract](candidate-client-execution-v3.md)
- [V3 observation contract](candidate-client-observation-v3.md)
- [V3 qualification plan](candidate-client-qualification-plan-v3.md)
- [Previous v2 failure](candidate-b03-v2-qualification.md)

The checkpoint binds the exact definition/source/runtime freeze, contributor
logs, original and supplemental source reviews, runner results and independent
raw-evidence audit. Local raw runs and checkpoints remain retained under
`runs/candidate-b03-v3-*`, outside ordinary discovery.

## Remaining research scope

C01/C02 qualification remains incomplete. B01 and B02 still have unclosed
facets; B03 HTTP/browser/export, B04 process/wire and B05 delivery remain. The
production scope declaration, requirement-to-assertion mapping, prerequisite
authorities, integrated controller and complete rehearsal remain necessary.

The study still requires four cumulative milestones across six matched
trajectories: 4-builder swarm, 16-builder swarm and 16-builder orchestrator,
each with four reviewers and healthy/recovery conditions. Quality and sustained
whole-project completion remain primary. Independent held-out project families
and a prespecified statistical analysis are required before general claims
about swarm advantage. This qualification adds no such statistical evidence.
