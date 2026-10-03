# Candidate client finite-process transport v4

This new process transport implements the independently reviewed
[empty-command plan v3](../analysis/candidate-b03-empty-command-plan-v3.json),
SHA256 `6c99fe8badb01281f0adb82265e5260d1022a818cf4edc18cf97da41912981d3`.
Frozen v1, v2 and v3 source and failed qualifications remain unchanged. Source
readiness and offline checks do not establish physical Docker qualification.
The root verification owner must freeze the combined evaluator and run the
prospective phase-one barrier before any remaining histories.

## Responsibility and version boundary

`candidate_client_process_v4.py` starts one already-created finite container at
most once through a bound local Unix Docker Engine endpoint. The controller
owns staging, immutable source and fixture bindings, owned temporary volume,
container creation, keeper lifecycle and final removal/absence. The transport
never invokes candidate source on the host, mounts the Docker socket, removes
containers, retries dispatch or grades product assertions.

The production protocol is `candidate-client-process-v4`. V4 adds two separately
hashed policy definitions: `command_policy()` with policy ID
`docker-explicit-entrypoint-empty-command-v1`, and `start_response_policy()` with
ID `docker-start-http-observation-v1`. Both definitions and digests, the complete
source map and the tracked plan digest are retained in process intent before
attachment. Runtime identity and controller limits/evaluator bindings cover
the new version. The generic transport does not select a required HTTP error
status, parse an expected error message or add a rejected-start comparison phase.
Those assertions belong to the independently versioned qualification helper.

## Independently declared invocation

The public validation API is:

```python
validate_sandbox(value, policy, *, expected_argv, runtime)
run_process(endpoint, *, container_id, expected, policy, retain, label,
            expected_runtime, expected_argv)
```

The caller supplies `expected_argv` from the registered recipe or raw-control
intent, independently of container inspection. It must be a nonempty actual
list of strings, with a nonempty executable and no NUL. Empty argument strings
remain real arguments. The transport copies the declared list and runtime
before retention callbacks can mutate the caller's objects. The intent retains
the declared list and its canonical SHA256. No expected value is derived from
inspected `Config.Cmd`, `Path` or `Args`.

Validation requires explicit `Config.Entrypoint`, `Config.Cmd`, `Path` and `Args`.
Entrypoint is an exact singleton containing the declared executable; Path is
that same string. Args is an actual list of strings equal to the declared tail
in type, order and value. Cmd is either the same exact argument list or explicit
null when the declared invocation is a singleton and Args is exactly `[]`.
Missing Cmd, null Args, arbitrary nulls, numbers, booleans, malformed arrays,
additional/inherited arguments and changed order are rejected.

This supported-schema admission is gated by the explicit pinned Linux
Engine29.2.1/commit6bc6209b88a7a834c91f77d848e025c79e0227a1/API1.47/cgroup2
runtime and capability profile, plus image/daemon/local endpoint bindings.
Installed CLI version is not inferred. Initial validation uses the caller's
bound runtime; the transport then observes fresh runtime identity, requires exact
agreement, independently inspects the still-created container, durably saves
the prestart identity comparison and validates again with the fresh runtime
before attachment or start. A changed runtime or declared invocation prevents
dispatch. Sandbox restrictions remain the frozen network-none, read-only,
UID/GID65534, capability/no-new-privileges, CPU/memory/PID/FD, volume and logging
restrictions. No new environment, mount or candidate completion marker is added.

Null and empty Cmd remain distinct raw identities. V4 does not add a Cmd
normalization. Created-to-prestart and created-to-final comparisons reject a
null/empty representation change even when each endpoint separately describes
no arguments. The prior complete Mounts inventory rule and narrow pinned
OomKillDisable startup rule are unchanged. Every other raw field/type/array
order remains exact in the immutable projection. Strict bounded JSON decoding
rejects duplicate keys before dictionary conversion and rejects nonfinite
numbers, malformed input and oversized/deep values.

## A complete HTTP observation is retained explicitly

`_control` still retains raw request and response separately, including partial
response bytes on errors. Such a raw file does not prove framing completion.
The new `start_response` and `start_response_receipt` result fields begin null.
Only a normal `_control` return after complete bounded framing, witnessed
socket EOF, no trailing data, successful raw-response retention and socket
close permits construction of a completion receipt. Every complete HTTP status
is recorded; a complete500 is still a valid HTTP observation and unavailable
candidate process evidence. A lost or malformed response can still carry the
diagnostic status `start_error`, but it has no complete-response receipt.

The separately retained `<label>-start-response-completion.json` contains:

| Field | Bound value |
| --- | --- |
| protocol | `docker-start-http-observation-v1` |
| method, request_path | POST and `/v1.47/containers/<full-ID>/start` |
| container_id | Full owned64-hex ID |
| endpoint_sha256 | Canonical bound Unix endpoint path/device/inode digest |
| runtime_sha256 | Canonical fresh runtime digest |
| request, response | Exact retained `{path, bytes, sha256}` descriptors |
| status | Parsed integer HTTP status |
| body_bytes, body_sha256 | Decoded HTTP body length and SHA256 |
| framing_complete, eof_observed, durably_retained | Exact true |

Canonical JSON uses sorted keys, compact separators, ASCII escaping and rejects
nonfinite values. The completion receipt has no self-hash. The final result's
`start_response` equals its object and `start_response_receipt` gives its exact
path/bytes/SHA256. Both remain null if receipt retention/checkpointing fails,
even when bytes were partially or fully written. Raw evidence is still retained
where possible and bounded owned-container cleanup still runs.

The actual sequence is raw start request, complete raw response, separate
durable completion receipt, non204 rejection branch if applicable, existing
`finally` cleanup, and final transport result. The controller later retains its
result and owned removal/absence terminal evidence. This ordering is covered by
inert socket/retention regressions. A qualification consumer must independently
authenticate the raw request/response, ordered external checkpoints and receipt
metadata; a caller-supplied boolean or filename is insufficient.

The existing early callback for fully parsed204 remains before potentially slow
raw-response retention. It records causal start acknowledgement for conservative
attach EOF handling, but does not grant durable completion-receipt authority.
EOF first observed before that successful acknowledgement remains incomplete.
A completion-receipt retention failure after acknowledged204 still triggers
cleanup and cannot establish natural candidate completion.

The wire bounds remain32KiB headers, at most127 nonempty unique header lines,
1MiB decoded control body,65,536 chunks and2,392,064 raw control bytes. Framing
rejects conflicting Content-Length/Transfer-Encoding, duplicate/malformed
headers, unsupported encodings, truncated/invalid chunks, trailers, extra bytes
or second messages. All framing forms require witnessed EOF. Candidate raw
multiplex streams have their separately declared prospective bounds and channel
identities. Engine error bodies never become candidate stdout or stderr.

## Completion and qualification stay separate

A candidate exit requires the existing independent wait and final inspection to
agree, natural lifecycle timestamps/state, verified identity, no controller kill
or OOM/error and the conservative exit range. Output authority additionally
requires valid complete raw framing/EOF. A stream failure can leave independently
proven natural exit evidence intact. Timeouts, limits, truncation, start failures
and ambiguous signals stay unknown product observations, not latency failures.

On a completed non204 rejection, no wait, inspect-final or final startup
comparison is expected. The transport still requests bounded defensive cleanup
on its owned ID and retains cleanup inspection when available. A kill request
is not proof that a process ran or was killed. Created ExitCode0 and daemon
startup bookkeeping127 are not candidate exits. There is no automatic retry.

The missing-executable qualification helper separately requires a complete
source-grounded400 response, independently decoded error/state proof and the
new explicitly named rejected-start diagnostic identity policy from the plan.
This transport does not make that control-specific judgment. That diagnostic
phase cannot supply a product pass or natural-completion evidence. The control
source and exact singleton argv remain unchanged.

## Offline and physical verification boundary

`tests/test_candidate_client_process_v4.py` copies the frozen v3 protocol,
lifecycle, compatibility and mount-inventory regressions into v4, preserving
portable historical fixture bytes without reading ignored runtime artifacts.
New command and start-receipt classes exercise null/list distinctions, missing
keys, ordered typed arguments, runtime/endpoint substitution, fresh validation,
caller mutation, all three HTTP framing forms, generic statuses, lost/absent/
malformed/ambiguous replies, EOF uncertainty, retention/checkpoint failures,
cleanup ordering and original no-argument control dispatch through inert sockets.
No test in this file opens an actual Docker endpoint or executes a candidate.

The root owner registers these classes and runs the combined static/offline gate.
Only after independent source review and freeze may the root execute exactly
one persistence history and all11 unchanged controls under v4. If that phase
passes, the remaining56 histories follow under the same frozen contract without
repeating the passed persistence case. Frozen v3 evidence remains failed; no
older version's passes substitute for this changed qualification contract.
