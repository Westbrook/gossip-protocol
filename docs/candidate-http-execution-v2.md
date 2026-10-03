# Source-bound HTTP execution v2

This is C03 observation infrastructure for M1 harness qualification. It supplies
source-bound wire records and explicit observation limits; it assigns no product
predicate, semantic score, acceptance credit or comparative outcome. Frozen
finite CLI v4 remains unchanged.

`HttpRecipe` registers literal server argv, explicit `/inputs` root, a database
path beneath `/tmp`, immutable fixture bytes/directories, one fixed unprivileged
port, and an ordered sequence of `HttpStep` values (`start`, `probe`, `stop`).
Probe requests are canonical closed JSON recipes from
`candidate_http_transport_v1`. Every server epoch must be explicitly stopped
before another starts. `HttpPolicy` freezes image, wire/capture/deadline limits,
keeper lifetime and seed. `binding_for` and `HttpRegistration` bind the complete
Git blob census, commit/tree, recipe order, fixture, helper bytes, loaded evaluator
sources, exact runtime, sanitized Docker environment, bounds and repetition.
`HttpBinding` additionally pins `role_policy_sha256` (the canonical v2 role policy
definition) and `role_policy_source_sha256` (this execution module). The same
policy identity is explicit in configuration, durable intent, comparisons,
probe process results and terminal results. `role_policy_definition()` and
`role_policy_identity()` expose these frozen definitions and source hashes.
Only `harness_qualification`, M1, and the already registered requirements identity
are admitted. No candidate code is imported or run on the host.

## Distinct container roles

All three roles use the pinned image, unprivileged user, read-only root,
capability drop, no-new-privileges, bounded CPU/memory/PIDs/files, disabled health
checks/log storage/restarts, private PID/IPC/cgroup namespaces and explicit empty
proxy variables.

| Role | Network | Mounts | Lifecycle |
| --- | --- | --- | --- |
| Server | `none` | exact read-only source `/workspace`, fixture `/inputs`, owned writable tmpfs volume `/tmp` | create, prestart, start, running continuity, explicit stop, removal |
| Keeper | `none` | only the same volume `/tmp`, read-only | holds the bounded volume across server absence |
| Trusted probe | `container:<full current server ID>` | only fixed read-only helper/input directory `/probe`; separate private `/tmp` tmpfs | attach before start, start acknowledgement, wait, final inspection, complete stream EOF, remove |

`RoleSpec`, `create_argv`, `validate_role`, `validate_state` and
`role_identity_comparison` are independently usable by mechanical qualification
controls. A wrong-network probe must be rejected before start. They do not relax
or relabel frozen v4 phases. Complete inspect rows remain retained; comparisons
permit destination-keyed permutation of complete mount rows, the existing exact
pinned-runtime false-to-null OOM default transition, and one new donor-bound
probe startup hostname mapping. Running continuity requires exact StartedAt,
PID, restart count and configuration. No v1 definition or failed result changes.

## Independently bound startup donor

The [prospective correction plan](../analysis/candidate-c03-http-probe-hostname-plan-v2.json)
and pinned [Moby startup implementation](https://github.com/moby/moby/blob/6bc6209b88a7a834c91f77d848e025c79e0227a1/daemon/container_operations.go#L411-L424)
explain the real transition: the created helper starts with its own default short
ID hostname; joining the current server's network namespace copies that server's
hostname and domain. V1 correctly rejected this otherwise complete observation.
The new version admits only `probe` / `created-to-exited`, under the exact
qualified Linux Engine runtime, API, commit and cgroup capability gate.

Before helper creation, the controller independently inspects the **current
running source-bound server**, validates its complete role/profile and exact
continuity against the initial running inspection, and binds its full ID, epoch,
source/fixture/Git registration, full runtime, explicit hostname and domain.
`bind_probe_donor(baseline=..., current=..., server_spec=..., registration=...,
runtime=..., epoch=...)` creates `ProbeDonorEvidence` with canonical immutable
bytes of the complete baseline, current inspection and runtime; each access
returns a fresh value. A supplied digest or helper-produced hostname cannot
replace this evidence. Full role validation and provenance checks are repeated
when the evidence is used.

The controller retains `step-NNN-donor-inspection.json` and
`step-NNN-donor.json` before the helper create intent, alongside the original raw
Engine inspection response. The donor record includes the complete baseline and
current inspections, source binding, exact server role spec, runtime and their
hashes. Source, fixture, registered Git, loaded evaluator and runtime are checked
again immediately before creation. The probe's network target must be
`container:<full donor ID>` and its execution/source/fixture/epoch/helper labels
must match that donor. Independent server and keeper continuity checks still run
after the helper finishes.

`role_identity_comparison(..., donor=...)` accepts donor authority only for that
probe terminal phase. The created hostname must be the probe's own full ID's
first twelve characters, and the terminal hostname must equal the independently
bound donor hostname. Both requirements apply even if the strings happen to be
equal. `Config.Domainname` must exist as an explicit empty string in the donor,
created helper and terminal helper. Missing, null, non-string and nonempty
values fail; no domain normalization is performed. Server, keeper, prestart and
running hostname comparisons remain exact. Each comparison records the actual
hostname mapping in `changed_fields`, the complete raw before/after hashes and
the donor record hash. No field is dropped from the existing immutable
projection to admit this transition.

Docker also inherits its managed hostname, hosts and resolver file paths when
joining the donor network namespace; see the pinned
[network initialization](https://github.com/moby/moby/blob/6bc6209b88a7a834c91f77d848e025c79e0227a1/daemon/container_operations_unix.go#L543-L547).
These runtime-managed top-level inspection paths are retained as raw evidence;
they were already outside the immutable container projection. They do not add
candidate source, fixture or database mounts. Complete `Mounts` rows, private
PID/filesystem restrictions and every other existing namespace restriction
remain independently checked.

The probe runs exactly `python -I /probe/helper.py /probe/request.json`. It shares
no source, input, database, PID namespace, host credential directory or Docker
socket. Source and exact request bytes are pinned before creation. Its stdout
budget is derived from wire bytes, base64 expansion and four bounded kernel
listener tables; the finite CLI's smaller output bound is not reused. Failed
helper completion cannot authenticate a transcript. A successfully completed
helper may report an incomplete or limited HTTP exchange without inventing a
product failure.

The wire helper's counted connection establishment retries occur only before any
request byte is sent, within its fixed total deadline and I/O budget. It makes no
hidden readiness request and never redispatches a sent request. Kernel IPv4/IPv6
listener snapshots are taken in the shared network namespace after connection,
before request send, and after observation. Loopback reachability is not proof of
loopback-only binding. Listener process ownership and future binding behavior
remain outside this observation.

## Journal and result

`CandidateHttpExecution(root, store, registration, recipe, policy, endpoint=...)`
claims an exclusive, canonical, previously absent journal. `execute_once()`
retains durable intent before any resource mutation. Reopening requires an exact
external `ControllerCheckpoint`; rollback, tampering and foreign suffixes fail.
An intent with no authenticated terminal raises `ExecutionUnknown` and is never
redispatched. Existing terminal evidence is revalidated, not executed again.
Checkpoint sinks can persist authority outside the journal after each durable
write. Fixture mode provides inert journal controls and cannot dispatch or
claim physical evidence.

Each authenticated result observation includes source/fixture/Git/binding,
server epoch/ID/database volume/path, helper ID and hash, finite process records,
server and keeper continuity, exact retained stdout/stderr descriptors, and a
host-decoded `wire` object. Source, fixtures, helper staging and evaluator/runtime
identity are checked after the probe before authentication. An unavailable row
keeps its raw evidence and error; earlier authenticated rows remain independently
available. A later cleanup failure does not erase an already authenticated wire
record. Result `status=completed` denotes completed infrastructure observation,
not successful HTTP semantics; consult `wire.exchange_complete` and the separate
response/listener limitations.

`terminal.json` retains all completed lifecycle steps, server epochs, probe rows,
owned cleanup outcomes and source fingerprints. It distinguishes planned
requests, observation rows, authenticated complete sends, unknown request
outcomes, create intents, confirmed returned IDs, uncertain creates and raw
start-response records. A diagnostic row or failed create is not counted as an
actual successfully sent request or confirmed creation.

Server stop has no natural-exit-zero obligation. Its explicit controller action,
complete stop response and exited inspection are retained. The prior server must
be removed before the next epoch starts, while the keeper remains continuously
running. This can support a later normal-restart persistence observation; it does
not prove abrupt-crash recovery, database conservation or internal delegation.
Cleanup rechecks full owned name/ID/image/labels before mutation and verifies
absence. Uncertain cleanup is retained, not automatically retried until green;
a dependent keeper/volume is preserved if another resource cannot be removed.

## Verification boundary

The offline tests use real isolated Git stores and durable local journals, with
inert Docker control and socket fixtures. They exercise exact role restrictions,
raw helper attachment/completion ordering, restart orchestration, provenance
loss, cleanup uncertainty and no-redispatch. V2 adds explicit hostname/domain
fields and distinct helper/server short IDs to the inert Engine: startup really
mutates the configured helper hostname to the observed donor value. Composed
tests retain the real controller, finite probe runner and wire decoder while
substituting only low-level control/socket I/O. They verify that a spoofed
hostname or changed domain withholds response attribution even when raw helper
output exists.

`fixtures/candidate-http-probe-hostname-v1/inspection-records.json` retains the
complete decoded JSON values from the original failed v1 inspection pair,
server donor observations, create intents and configuration. It is a portable
reserialized derivative with original paths/lengths/hashes and decoded body
hashes; no values are redacted. The regression adapts only the standalone test
registration to the prospective v2 protocol/policy and checks the original
inspection values against that rule. The frozen v1 comparator still rejects the
same pair. This test neither replays a container nor requalifies the old failure.
Negative controls cover missing or wrong donor evidence, source/fixture/epoch,
network target, hostname/domain types, unsupported phase/runtime/role and other
complete Config, HostConfig or mount-row drift. They make no real Engine/provider
calls or key reads and cannot substitute for separately registered physical
mechanics qualification. The global verification owner registers new test classes
and runs the combined static/offline gate before any Docker lane.
