# Finite client execution v4: C01/C02 qualification

This controller binds finite CLI observations to complete immutable Git source,
a closed authored recipe, and the process transport. It supports only
`harness_qualification` at M1. It creates no Registry acceptance, production
prerequisite, promotion, or six-trajectory barrier authority. C03–C06 still own
HTTP wire tests, listener proof, browser actions/export, and the authenticated
observation bridge. A successful finite history is not a whole requirement pass.

## Public interfaces

- `ClientPolicy(image_id, command_timeout_seconds=30,
  stream_limit_bytes=4194304, frame_limit_bytes=4194304,
  transport_timeout_seconds=15, seed=0)` freezes resource observations.
- `capture_git_source(store, commit_oid)` reuses the unchanged bounded Git blob
  reader from `candidate_release_execution_v2`. The controller supplies its own
  source digest domain with `source_sha256(files)`; do not compare that digest
  with another driver's source digest without comparing the complete file map.
- `runtime_identity(endpoint, image_id)` delegates to the new Unix Engine
  transport's stable daemon/image projection. The host Docker control commands
  explicitly select that same Unix socket, independently of later context
  selection. No registry pull, remote endpoint fallback, or candidate socket
  mount is permitted. Constructor and before/after checks pass the declared
  `transport_timeout_seconds`; standalone metadata callers retain a 15 second
  default through the optional `timeout_seconds` keyword.
- `binding_for(files, case_id, policy, runtime,
  requirements_sha256=..., milestone='M1', purpose='harness_qualification')`
  creates a `ClientBinding`. The requirements digest must match the
  pinned cumulative-v2 product contract declared by the provider; arbitrary
  caller labels are rejected. Its `definition_sha256` binds the selected case,
  recipe, fixture identity, complete provider semantic digest, and normative
  source map. `ordered_suite_sha256` binds the full provider order. This is an
  intended suite identity; one history does not prove the whole suite ran.
- `ClientRegistration(binding, commit_oid, tree_oid, repetition_id)` binds the
  immutable Git subject and explicit development repetition.
- `CandidateClientExecution(root, store, registration, policy, endpoint=None,
  mode='physical', expected_checkpoint=None, checkpoint_sink=None)` owns one
  local journal. Use the context manager to release its exclusive owner lock.
  `execute_once()` and `verified_execution()` return `ClientHistoryResult`.
- `ClientHistoryResult` retains the execution/case IDs, history status,
  authenticated ordered `ProcessObservation` tuple, missing step IDs, cleanup,
  infrastructure details, terminal digest, and external checkpoint inventory.
  The observer's `observe_cli_step(expected, observation)` remains a separate,
  host-only scoring call. The controller does not hide later unavailable steps
  or erase earlier independently observed discrepancies.

The provider API is `definitions()`, `case_definition(case_id)`,
`execution_recipe(case_id)`, `definition_sources()`, and `definition_sha256()`.
A recipe has exactly `case_id`, `fixtures` (relative path to canonical base64),
`directories` (including empty directories), and ordered `steps`. Each step has
exactly `step_id` and literal `argv`. There is no shell or output-derived code,
mutation language, candidate-selected adapter, or caller-supplied expected data
inside the container. The fixture provider currently declares 57 histories and
305 finite processes; separate synthetic physical controls retain their own
qualification identity and never earn additional product coverage.

## Process and persistence boundary

Every finite invocation is the main process of a fresh pinned-image container.
The controller removes `--init` and `--rm` from the established sandbox profile;
there is no init process whose exit could substitute for the candidate process.
Source is mounted read-only at `/workspace`; immutable input roots are mounted
read-only at `/inputs`. Database paths such as `/tmp/db-a.sqlite` and
`/tmp/db-b.sqlite` share a fresh owned 32 MiB local-driver tmpfs volume within
that history. Candidate processes run as UID/GID 65534 with read-only root,
network none, dropped capabilities, no new privileges, 64 PIDs, 256 MiB memory,
one CPU, 256 descriptors, no healthcheck/restart policy, and logging disabled.
Credential-bearing proxy environment variables are explicitly cleared. Only
trusted host code receives Docker access, recipes, expected values, and scoring.

A local-driver tmpfs may disappear when its final active consumer releases its
mount. Therefore a distinct trusted keeper holds the volume throughout the
history. It mounts the volume **read-only**, has no source, fixture, or checks
mount, and executes only the fixed pinned-image command:

```
python -I -c 'import time;time.sleep(DECLARED_SECONDS)'
```

Its lifetime is prospectively bounded by
`120 + steps * (command_timeout_seconds + 24 * transport_timeout_seconds + 30)`.
This observation envelope accommodates per-step process/control operations and
cleanup; it is not a newly invented product deadline. Keeper argv, lifetime,
role, name, ID, image, full configuration, mount, `StartedAt`, and restart count
are retained. The same running identity with zero restarts is checked before and
after every finite invocation. These checks establish the declared local
persistence precondition; a changed, stopped, expired, or restarted keeper makes
that history unavailable. A separate physical persistence control must qualify
this volume lifetime before the full reference roster runs.

The keeper is counted separately from the candidate CLI containers/processes.
All owned finite containers must be removed before the keeper is removed; keeper
absence must be proven before the volume is removed. Cleanup never removes a
pre-existing name collision. Even uncertain creation requires an inspection that
matches the durable invocation's exact name, image, labels, and known ID before
removal. A failed cleanup leaves its owned resources/evidence visible for later
investigation and does not authorize redispatch.

The v4 process transport opens non-TTY Engine attach before start, independently
collects wait/final-inspect completion, and separates raw stdout/stderr from HTTP
control traffic. Its module documents framing, signal/start errors, deadlines,
and resource caps. Candidate output or a done marker is never completion proof.


## Versioned inspection comparison

Frozen v1 and v2 sources, failed qualification records, and provider definitions
remain unchanged. V2 stopped two histories before candidate start because the
Engine serialized identical top-level mount rows in a different array order.
The inventory implementation introduced in v3 uses an explicit execution/source domain and matching observer/process versions.
It changes neither the 57 recipes and 305 invocations nor product assertions.

Both `analysis/candidate-b03-runtime-compatibility-plan-v1.json` and
`analysis/candidate-b03-mount-inventory-plan-v1.json` are tracked evaluator
dependencies. `startup_policy_binding()` and `identity_policy_binding()` retain
the shared helper definitions, IDs, and digests in configuration and limits.
The inventory rule is `docker-inspect-mount-inventory-v1`; the unchanged startup
rule is `docker-hostconfig-oom-kill-default-v1`. Their implementation sources and
both plans are bound by the evaluator digest. Runtime identity binds the pinned
Linux Engine 29.2.1/Moby commit, API 1.47, daemon/socket/image, OS, cgroup
version/driver and OOM-disable capability.

All raw Docker CLI inspection JSON passes the shared bounded strict decoder
before conversion into mappings, including created candidates, keeper reads,
volume ownership, and cleanup inspections. Duplicate object keys at any depth,
invalid UTF-8, nonfinite numbers, and excessive depth/node/byte counts are
rejected. Raw bytes remain retained. Own canonical journal records have their
separate larger record bound and canonical-byte integrity check.

The shared `identity_comparison(before, after, runtime, phase,
before_full_inspection=..., after_full_inspection=...)` compares only the
top-level `Mounts` inventory without order. Every row must have a unique exact
canonical absolute destination; the comparison-only view maps each destination
to its complete original row. No field is discarded and no other array is
sorted. Typed changes to a row, unknown fields, nested lists, argv, environment,
HostConfig mounts or security settings remain mismatches. Candidate roles keep
exactly `/workspace`, `/inputs`, `/tmp`; the keeper keeps only `/tmp`.

The first keeper comparison uses phase `keeper-created-to-running`, omitting
only the separately validated changing `StartedAt` from its invariant
projection. Both full raw decoded CLI inspections are supplied and separately
hashed. The helper permits the existing strictly typed `OomKillDisable: false
-> null` startup transition only under the pinned capability profile. It
retains `keeper-startup-comparison.json` beside raw `keeper-created` and
`keeper-running` control artifacts, without rewriting either raw view.

Every subsequent boundary uses phase `keeper-running-to-running`, compares the
first-running baseline with the entire observed projection, and retains
`step-NNN-keeper-before-comparison.json` or
`step-NNN-keeper-after-comparison.json`. These records also bind both full raw
inspections. They include `StartedAt` and strict restart count; no OOM
normalization applies during the history. Loss of this persistence precondition
makes the history unavailable. The single keeper mount remains fully exact.

Transport separately retains `step-NNN-prestart-comparison.json` before attach
or start, and its final startup comparison after wait/inspect. Prestart permits
mount ordering only; the existing OOM transition is permitted only during the
created-to-exited phase. Each record exposes raw full/projection hashes,
original destination order, per-row hashes, inventory and comparison hashes,
phase/role/policy/runtime bindings, transformations, reasons, and a self-digest.
No record supplies product acceptance or upgrades earlier failed evidence.

## Independent command registration and complete start responses

V4 preserves frozen v1/v2/v3 sources and their failed qualification records.
`analysis/candidate-b03-empty-command-plan-v3.json` is an additional tracked
evaluator dependency. The controller imports only the v4 process and observer
for this protocol; product definitions, fixture bytes, ordered histories,
requirements digest, resource limits, purpose and grading are unchanged.

`command_policy_binding()` retains `docker-explicit-entrypoint-empty-command-v1`
with its full definition and digest. `start_response_policy_binding()` retains
the `docker-start-http-observation-v1` protocol, definition and policy digest.
Both are present in configuration and the limits digest alongside the existing
startup and mount inventory policies. The evaluator binds all three tracked
plans and the process/controller/observer source bytes.

The registered step supplies the independent `expected_argv` list. The
controller's initial configuration validation calls
`validate_sandbox(value, policy, expected_argv=step['argv'], runtime=self.runtime)`;
`run_process` receives that same declared list separately from the full initial
inspection and `expected_runtime`. It must not derive its expected command from
that inspection. The process layer freezes the declared command in its intent,
checks the initial view, refreshes runtime, and validates the independent
prestart view against the same command before attach/start.

Under the pinned runtime, an explicit present `Config.Cmd: null` is permitted
only for a registered singleton argv with that exact singleton Entrypoint,
matching Path and exact empty top-level Args. A list-valued Cmd must match the
registered argv tail and Args exactly. Missing Cmd, malformed types, inherited
image commands, or mismatched arguments remain unavailable. Null and empty
array remain different raw identities: no command field is normalized during
any inspection comparison. The controller does not insert a dummy argument or
special-case a control ID; the existing closed provider recipes remain intact.

The generic transport adds `start_response` and `start_response_receipt`.
They remain null until full bounded HTTP framing, network EOF, successful raw
retention, socket closure and a separate durable start-response completion
receipt have succeeded. The controller's existing exclusive fsynced retention
and external checkpoint callback apply to this receipt. `step-NNN-result.json`
is retained after transport cleanup and preserves both metadata fields exactly.
HTTP/control bytes never become candidate stdout, stderr, exit, or natural
completion evidence merely because a response is retained. These metadata are
not an independently qualified missing-executable result.

The more specific rejected-start diagnostic proof and its strict HTTP400,
message, state and cleanup predicates belong to the separately reviewed
qualification helper. This controller creates no rejected-start comparison
phase, product assertion, Registry authority or acceptance bridge. Root owns
physical qualification under an exact new freeze after offline checks.

## Journal and raw observation schema

The journal holds an exclusive process lock. Registration retains canonical
configuration, full source/fixture manifests, recipe, runtime, and loaded
source identities. Candidate initial inspection must match the exact registered
argv, source/fixture labels, staged bind paths, and owned volume before the
transport accepts its baseline. The keeper receives the same before-start
configuration check, including resource/security/environment restrictions.
An overall intent is fsynced before resource creation; a
keeper intent and each finite invocation controller intent
(`step-NNN-controller-intent.json`) are fsynced before their own
creation/start. The transport independently retains `step-NNN-intent.json`;
the distinct namespaces preserve both records without exclusive-write collision.
Every raw/control artifact uses exclusive creation and parent
fsync. The external checkpoint sink receives the exact current inventory after
each retained write. Opening an existing journal requires that independently
retained inventory; rollback, changes, deletion, or appended suffixes fail.
An interrupted intent remains unknown and is never dispatched again.

Raw per-step observer binding fields are exactly:

```
protocol, execution_id, source_sha256, commit_oid, tree_oid,
requirements_sha256, milestone, purpose, definition_sha256,
ordered_suite_sha256, case_id, step_id, step_index, ordered_step_ids,
argv, fixture_sha256, runtime_sha256, environment_sha256,
limits_sha256, evaluator_sha256
```

The transport result carries raw stream artifact descriptors, independent
completion evidence, identity/capture state, status, and raw control references.
The controller adds `history_state_verified` after checking the keeper around
that invocation. False preserves the raw exit comparison but prevents semantic
state/output grading; the overall history is infrastructure/unavailable and
must not be diagnosed as a candidate defect. Lost source/staging/runtime binding
also prevents authenticated results. Whole-output comparisons require actual
natural completion and complete identified streams. A natural wrong exit can
remain an observed mismatch when a sibling stream is incomplete; that local
comparison is not proof that a product caused a failure under lost state
preconditions.

Expected values and scoring remain host-only. Fixture limits are 1,024 files,
8 MiB total decoded bytes, 64 steps, and 64 KiB argument bytes per step. Default
stream/frame limits are 4 MiB, and the journal has explicit file/byte bounds.
These limits describe what this evaluator can observe. Timeout, truncation,
limit events, transport errors, or missing provenance earn no product pass and
are not proof of unstated latency or raw-output requirements. In particular,
there is no blanket empty-other-stream requirement, and usage errors are not
forced through the domain-error JSON parser.

Source, normative inputs, staged bytes/directories, runtime, and complete
registration are checked before and after execution. Retained checkpoints are
local trusted-controller evidence, not signatures or an external attestation.
Fixture mode is strictly for offline admission/journal tests and cannot execute
or authenticate candidate evidence. Physical qualification and its evidence
census belong to the central verification owner after the exact source freeze.
