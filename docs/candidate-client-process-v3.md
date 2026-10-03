# Finite candidate process observation, version 3

`candidate_client_process_v3.py` implements the B03 finite-process transport. It
starts an already-created, caller-owned container once and observes the real
process through the Docker Engine API. It neither evaluates product semantics nor
creates a reusable acceptance receipt.

The controller creates and inspects the container, verifies staged source and
fixture bytes before and after execution, verifies the bounded volume options,
and removes the container and volume with independent absence checks. Candidate
code never executes on the host. The transport cannot establish source content
from a Docker label alone: its source/fixture labels bind the controller's staged
identities and must be reconciled with the controller's retained byte census.

## Interface and authority

- `EngineEndpoint.from_environment()` resolves the explicitly selected Docker
  context/host. An explicit context takes precedence over `DOCKER_HOST`. Only a
  local Unix socket is accepted; there is no remote/default fallback after an
  invalid selection. The canonical socket path, device and inode are bound and
  checked for every connection.
- `runtime_identity(endpoint, image_id, retain=..., label=...)` reads version,
  daemon and local image inspection through API 1.47. It returns stable daemon,
  engine, architecture, kernel, endpoint and exact image identities, together with
  operating system, Engine Git commit, cgroup version/driver and the typed
  `OomKillDisable` capability; transient
  container counts do not enter the identity. The optional `timeout_seconds`
  defaults to 15; finite execution passes its declared transport timeout. It
  never pulls an image.
- `ProcessPolicy(image_id, timeout_seconds=30, stream_limit_bytes=4194304,
  frame_limit_bytes=4194304, transport_timeout_seconds=15)` is immutable.
- `run_process(endpoint, container_id=..., expected=..., policy=..., retain=...,
  label=..., expected_runtime=...)` accepts the full initial Docker inspection.
  The callback retains fresh raw evidence names and bytes. It returns JSON data.
  The caller must supply a fresh full container ID and a unique output prefix.

The transport checks the same container ID, creation time, name, image, process
path/arguments, complete configuration, host configuration and mounts before and
after execution, subject only to the explicit mount-inventory and startup rules below. It independently requires UID/GID 65534, no network, a read-only
root, all capabilities dropped, no-new-privileges, no init process or automatic
removal/restart, disabled logging/health checks, one CPU, 256 MiB RAM with equal
swap ceiling, 64 PIDs and 256 file descriptors. The only application mounts are
read-only `/workspace` and `/inputs` plus the caller's local `/tmp` volume. Proxy
variables are explicitly empty; the fixed Python/HOME settings are checked.

## Narrow startup compatibility rule

Versions 1 and 2 remain frozen, including their failed Docker qualifications.
The v3 transport preserves the prospective rule in
[`analysis/candidate-b03-runtime-compatibility-plan-v1.json`](../analysis/candidate-b03-runtime-compatibility-plan-v1.json),
bound to SHA-256
`7b8d89a9003594551c2677a9975d715b4b4e3584746874cc446601722f411b2d`.
No failed v1 or v2 observation is promoted to a pass or reused as candidate evidence.

`startup_policy()` returns a fresh JSON policy and `startup_policy_sha256()`
returns its canonical digest. Its identifier is
`docker-hostconfig-oom-kill-default-v1`. The only permitted transformation is
explicit JSON `false` in `HostConfig.OomKillDisable` before startup becoming
explicit JSON `null` afterward. The comparison copy changes that single `null`
back to `false`; retained raw inspections remain unmodified. Explicit `false`
and `null` may also remain unchanged. Missing fields, enabled `true`, numeric
zero, strings, reverse transitions and every other changed key or JSON type
are rejected. Initial sandbox booleans and declared restrictions are validated
with exact JSON types before a baseline is accepted.

The rule is available only to `keeper-created-to-running` and
`candidate-created-to-exited`. Both phases require the bound local Linux runtime
profile: Engine 29.2.1, selected API 1.47, cgroup v2, explicitly unsupported
`OomKillDisable`, and the exact Moby commit `6bc6209b88a7a834c91f77d848e025c79e0227a1`
(or its daemon-reported seven-character form `6bc6209`). Endpoint, daemon, image,
architecture, kernel and cgroup driver identities remain bound. Missing or
changed runtime data does not broaden the exception. Runtime identity is read
again before each candidate start.

`startup_identity_comparison(before_projection, after_projection, runtime, phase)`
is a compatibility wrapper restricted to these two startup phases. It delegates
to the full identity comparison described below and returns a JSON record containing `matches`, policy ID/digest, phase, raw
projection and runtime digests, comparison-copy digests, explicit
`transformations`, failure `reasons`, and a `comparison_sha256` over the record
without that final field. Malformed or unqualified comparisons return
`matches: false`. Inputs are bounded strict JSON; neither input is mutated.
The helper compares all supplied keys, including unfamiliar future fields.
It validates comparison structure; the controller and transport remain
responsible for authenticating source, lifecycle and sandbox evidence.

Candidate creation-to-prestart identity permits only top-level mount ordering as
defined below; `OomKillDisable` stays raw exact. After independent wait
and final inspection, the transport compares complete `immutable_inspection`
projections, which retain `Id`, `Name`, `Created`, `Image`, `Path`, `Args`, full
`Config`, full `HostConfig`, and full `Mounts`. It retains
`<label>-startup-comparison.json`, also returned as `startup_comparison` in process
metadata. That field is null if final inspection was never reached; a later
completion failure can coexist with a successful configuration comparison.
The process intent records the complete startup policy and its digest before
attachment. The controller uses the same helper for the keeper's first
transition, excluding only its changing `StartedAt`; every subsequent keeper
boundary compares the full running projection under the mount-inventory rule,
with every other field, including `OomKillDisable` and `StartedAt`, exact. Neither comparison
waives OOM, forced kill, output completeness, exit authentication, source or
fixture checks, or cleanup.

This narrow behavior follows immutable primary Moby sources at the bound commit:
[`daemon/start.go`](https://github.com/moby/moby/blob/6bc6209b88a7a834c91f77d848e025c79e0227a1/daemon/start.go)
revalidates host configuration at start,
[`daemon/daemon_unix.go`](https://github.com/moby/moby/blob/6bc6209b88a7a834c91f77d848e025c79e0227a1/daemon/daemon_unix.go)
clears the optional OOM-killer-disable pointer when unsupported, and
[`daemon/info_unix.go`](https://github.com/moby/moby/blob/6bc6209b88a7a834c91f77d848e025c79e0227a1/daemon/info_unix.go)
reports that capability. It is not a general compatibility rule for other
Docker releases or platforms. A different profile requires another prospective
version and qualification.

## Mount inventory identity and raw decoding

Version 3 adds only the mount inventory rule in
[`analysis/candidate-b03-mount-inventory-plan-v1.json`](../analysis/candidate-b03-mount-inventory-plan-v1.json),
SHA-256 `f574060cfa8192c7e67da8a000309da5a7d2f01e4fe69edff4d2ea011c35a521`.
The rule ID is `docker-inspect-mount-inventory-v1`; `identity_policy()` returns
its complete definition, including the independently scoped OOM startup rule,
and `identity_policy_sha256()` binds that definition. Both tracked plans are
part of the evaluator source census.

The pinned Moby daemon declares mount points as a map in
[`container.go`](https://github.com/moby/moby/blob/6bc6209b88a7a834c91f77d848e025c79e0227a1/daemon/container/container.go).
Its [`GetMountPoints` implementation](https://github.com/moby/moby/blob/6bc6209b88a7a834c91f77d848e025c79e0227a1/daemon/container/container_unix.go#L419)
appends map values to the inspection array without sorting; the
[Go specification](https://go.dev/ref/spec#For_range) does not guarantee map
iteration order. V2 correctly rejected two raw array inequalities before start,
but those arrays contained identical complete mount records. Their failed
qualification remains failed.

The comparison-only representation replaces **only top-level `Mounts`** with a
map from exact `Destination` to the complete unchanged row. It rejects duplicate
destinations before constructing the map, including duplicate identical rows.
Destinations must be canonical absolute POSIX paths without NUL; spelling is
never normalized. Candidate inventories must contain exactly `/workspace`,
`/inputs` and `/tmp`; keepers exactly `/tmp`. Every row field, unknown future
field, JSON type and nested array order remains significant. There is no generic
array sorting; `Args`, `Config.Cmd`, `Config.Env`, `HostConfig.Mounts`, options
and limits retain their original ordering requirements. Existing independent
sandbox and mount-source checks still apply.

`strict_json_loads(raw: bytes | str)` decodes UTF-8 JSON with duplicate object-key
rejection at every nesting level **before dictionary conversion**. Both Engine
responses and the controller's raw CLI inspections use this boundary. Decoded
dictionaries cannot reveal keys previously discarded by another parser. The
shared decoder rejects nonfinite values, excessive depth/node counts and bodies
larger than 1 MiB. Its structural limits are depth 32 and 100,000 values; raw
HTTP framing and body limits remain independently enforced.

The full API is `identity_comparison(before_projection, after_projection,
runtime, phase, *, before_full_inspection=None, after_full_inspection=None)`.
Allowed phases are `candidate-created-to-prestart`,
`candidate-created-to-exited`, `keeper-created-to-running` and
`keeper-running-to-running`. All require the pinned runtime profile above. Only
the two startup phases may compose the existing typed OOM transition.

The record includes `matches`, role/phase, full policy and OOM subrule IDs and
digests, runtime digest, raw projection digests, canonical comparison digests,
`transformations`, failure `reasons`, and `comparison_sha256` over all preceding
fields. `mounts_before` and `mounts_after` each contain the original `order`,
`row_sha256` keyed by destination, and `inventory_sha256`. The full inspection
arguments additionally bind canonical JSON hashes of those complete raw views,
after verifying that each projection field matches its full inspection. They
are optional for pure comparison utilities; the physical controller and process
provide both. Full-view JSON hashes are distinct from raw wire/file hashes,
which remain independently retained.

The process intent retains the complete identity policy/digest and full initial
inspection digest. After the independent prestart inspection,
`<label>-prestart-comparison.json` is durably retained **before attach and start**.
A mismatch or failed retention prevents dispatch. Metadata exposes this proof
as `prestart_comparison`, separately from final `startup_comparison` retained
at `<label>-startup-comparison.json`. An identity match never establishes process
completion by itself. The controller also retains distinct keeper startup and
per-step running-boundary comparison records while preserving continuous
keeper lifetime checks.

Offline tests cover all 36 before/after permutations of three mounts, strict
negative cases, raw duplicate-key decoding, full-view binding, and durable
comparison ordering. Portable diagnostic mount fixtures embedded in the tests
come from the failed v2 records and retain their provenance; tests do not read
ignored `runs/` files or create completion receipts from those fixtures.
Physical qualification remains separate: after coherent offline checks and
source freeze, run one persistence history and all 11 controls, then the remaining
56 histories only if that phase passes. Do not repeat the passed persistence
history.

## Completion and capture

Before sending start, the transport establishes an upgraded, non-TTY attach
connection with stdout and stderr enabled and stdin/log replay disabled. A
nonblocking socket probe rejects a connection already closed before start. It then
starts exactly once, waits through a separate Engine request, and obtains a final
inspection. Candidate-written completion markers have no authority.

Successful start confirmation is recorded immediately after the complete 204
response passes framing checks, before durable response retention can block on
filesystem work. Output EOF first observed before that confirmation remains
unqualified. A very fast valid process may therefore produce unknown
capture even when its natural exit is independently proven later. This
conservative ordering policy prevents an attachment that closes while the start
request is pending from qualifying as complete empty output. Raw bytes and
`attach_eof_after_start_confirmation` retain the observation; this is not a
product speed requirement or a reason to erase an independently proven exit.

A natural completion requires a successful start, matching wait and inspection
exit codes, stable identity, valid ordered lifecycle timestamps, terminal exited
state, PID zero, no restart, no OOM, no state/wait error and no controller kill.
Signal-shaped exits (128 and above) are conservatively unqualified because this
interface alone cannot distinguish a signal from an explicit large exit value.
This is observation policy, not a newly imposed product exit convention.

The attach wire uses Docker's eight-byte stream headers. Only stdout and stderr
frames are accepted; frame sizes/counts, total raw wire and each output stream
are bounded. Full EOF at a frame boundary is required for complete output.
Candidate bytes are never interpreted as Engine errors, and Engine HTTP bodies
never become candidate stderr. Raw request/response traffic, framed attach bytes,
demultiplexed bytes and completion metadata are retained separately.

A stream fault does not erase independently proven process completion. For
example, an authenticated exit 2 can remain known while truncated output is
unavailable. Each stream descriptor has `complete`, `truncated`, retained byte
count/hash and observed byte count. `capture_complete` requires complete framing;
`completion.natural` and `exit_code` describe the independent exit facet.

An incomplete run triggers a bounded kill against the owned container ID and
retains a post-kill inspection when available. The caller still owns removal and
absence verification. There is no automatic retry or re-dispatch.

## Prospective observation limits

The default output and individual frame bounds are 4 MiB, with an explicitly
configured maximum of 16 MiB. The fixed HTTP header bound is 32 KiB; the decoded
control body bound is 1 MiB; frame/chunk count is at most 65,536. HTTP response
headers, content-length, chunked framing, trailers, trailing bytes, partial
bodies and EOF are checked. Control response wire storage additionally allows
bounded chunk framing overhead. Raw attach storage allows two stream bounds
plus frame/header overhead. Socket reads have a fixed 65,536-byte maximum and
wire overflow observes at most one sentinel byte beyond the retained cap.

The command deadline spans attach capture, start and wait. The separate 15-second
transport deadline applies to setup/final inspection and cleanup; it does not
silently shorten a 30-second command to 15 seconds. Deadline, overflow, OOM,
start errors, transport errors, lost framing and forced kill are unavailable
observations, not invented product latency failures. These bounds do not change
the frozen product's semantics or impose a new export-size requirement.

Offline tests use synthetic bytes, fake sockets and mocked Engine responses;
they never open a Docker socket or execute candidate Python. Root-owned physical
qualification must separately verify actual Engine behavior after source freeze.

## Protocol sources

The implementation follows the official [Docker Engine API 1.47
reference](https://docs.docker.com/reference/api/engine/version/v1.47/), including
attach upgrade/framing, start, inspect and wait. The corresponding immutable
[Moby v27.5.1 API definition](https://github.com/moby/moby/blob/v27.5.1/docs/api/v1.47.yaml)
provides the endpoint and stream specifications. The selected API version is
checked against the daemon's minimum/maximum API versions; unsupported versions
remain infrastructure unavailability.
