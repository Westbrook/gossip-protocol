# Source-bound HTTP execution v1

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
permit only destination-keyed permutation of complete mount rows and the exact
pinned-runtime false-to-null OOM default transition during startup. Running
continuity requires exact StartedAt, PID, restart count and configuration.

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
loss, cleanup uncertainty and no-redispatch. They make no real Engine/provider
calls or key reads and cannot substitute for separately registered physical
mechanics qualification. The global verification owner registers new test classes
and runs the combined static/offline gate before any Docker lane.
