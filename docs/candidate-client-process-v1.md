# Finite candidate process observation, version 1

`candidate_client_process_v1.py` implements the B03 finite-process transport. It
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
  engine, architecture, kernel, endpoint and exact image identities; transient
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
after execution. It independently requires UID/GID 65534, no network, a read-only
root, all capabilities dropped, no-new-privileges, no init process or automatic
removal/restart, disabled logging/health checks, one CPU, 256 MiB RAM with equal
swap ceiling, 64 PIDs and 256 file descriptors. The only application mounts are
read-only `/workspace` and `/inputs` plus the caller's local `/tmp` volume. Proxy
variables are explicitly empty; the fixed Python/HOME settings are checked.

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
