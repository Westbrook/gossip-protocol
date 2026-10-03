# C03 physical HTTP mechanics v1

This is a prospective qualification of HTTP observation machinery. It gives no
product acceptance credit, route-catalog coverage, browser qualification, or
whole-requirement verdict. The eight B03 HTTP target IDs and their source-grounded
dispositions remain unchanged. The independent C03 boundary does not replace the
three B03 v4 CLI additions that still await write approval.

The physical tests are in `tests/test_candidate_http_docker_v1.py`. Their small
evaluator-authored fixture is committed to an isolated Git store as inert source
bytes and runs as a long-lived TCP server in a container. It does not call a
reference application's `Service.request`, import candidate code on the host,
or receive expected answers. A separate, fixed trusted helper shares only the
server's private network namespace. Source, input and DB mounts do not enter the
helper. The server retains `NetworkMode=none`; there are no published ports.

## Closed control roster

The final prospective roster is nine histories and fourteen declared request
rows. The earlier independent eight-history/thirteen-request proposal was
expanded before qualification to add M09. Readiness traffic, kernel inventory,
helper starts, server starts and cleanup are separately counted; they do not
silently increase or replace the fourteen declared rows.

| History | Requests | Actual fixture behavior | Required mechanics observation |
| --- | ---: | --- | --- |
| M01 | 4 | Content-Length JSON while socket remains open; terminal chunked JSON while socket remains open; close-delimited JSON with EOF; complete 409 JSON error | Complete lawful frames without requiring EOF for framed responses. JSON whitespace, key order, escaping, numeric representation, header case and reason phrase do not create a discrepancy. The complete error stays a response, not a transport failure. |
| M02 | 2 | Complete deliberately wrong marker, then a 409 response whose declared Content-Length exceeds a valid-looking JSON prefix | The authored synthetic marker comparison is false. The second response is unavailable. Preserve both facts separately; a later incomplete sibling cannot erase the known mismatch. |
| M03 | 1 | Valid-looking close-delimited JSON, but no EOF before the declared deadline | Unavailable because framing completion is missing. The deadline is an observation bound, not a product latency requirement. |
| M04 | 2 | Complete raw response exactly at the declared 4096-byte wire budget, then a 4097-byte response | Exact-budget capture completes; budget+1 is bounded and unavailable. The equation includes status, headers, delimiters and body and is fixed before dispatch. There is no invented product response-size limit. |
| M05 | 1 | Server sends a response prefix and exits with code 17 | Retain partial wire and the broken server lifetime. No complete response or domain rejection is inferred. |
| M06 | 1 | Server binds IPv4 wildcard and serves an ordinary loopback request | HTTP is reachable and complete, while independent kernel listener evidence shows wildcard binding. Reachability alone cannot establish loopback-only binding. |
| M07 | 2 | POST a fixed SQLite marker, stop/remove server 1, keep the same DB-volume keeper alive, start source-identical server 2, GET the marker | Distinct server IDs/epochs, exact same source and DB mount, server 1 absent before server 2 starts, continuous keeper identity, persisted marker. This is normal process replacement, not crash recovery. |
| M08 | 0 | Start a source-bound owned server and DB keeper through the normal role profile; create a trusted-probe container with deliberately wrong network mode and validate it against an independently declared correct role | Reject before any helper start or request. Retain real created-state inspection, zero helper-start evidence, source/helper/input manifests and owned-resource cleanup. A prepared JSON artifact or mock cannot satisfy this physical control. |
| M09 | 1 | Emit a raw response with unequal duplicate Content-Length values, a finished header block and body, then close | Preserve the actual captured prefix and conflicting headers, and classify ambiguous framing as unavailable. The helper may stop at detected ambiguity before EOF; neither EOF nor the entire body is presumed observed. |

M01's JSON comparisons and M02's marker check are synthetic mechanics assertions,
not new HTTP product predicates. Complete raw bodies may otherwise be arbitrary
bytes; product JSON/error semantics belong to the separately authored observer.
M01's two held-open framed requests explicitly use `Connection: keep-alive`;
the other requests use `Connection: close`. The exact literal bytes are retained.

## Evidence boundary

Every physical history must bind the ordered control/request roster, fixture
source commit/tree/file hashes, executor and probe source identities, pinned
container image and observed Engine runtime, request bytes and limits, server
epoch, DB/root/volume, purpose and observation protocol before dispatch. The
trusted helper must report actual sent bytes and raw received bytes with bounded
completion metadata; the host decoder rechecks framing independently.

The socket deadline is fixed at 2 seconds, below the fixture's 8-second held-open
interval. The ordinary raw response budget is 65536 bytes; M04 alone uses 4096
bytes. Request/header/listener and helper completion bounds are recorded in each
prospective registration. These constants are fixed before execution and are
not adjusted in response to an observed result.

Retain full created/running/after-request server and probe inspections rather
than only precomputed `matches` flags. Check the server container, StartedAt,
restart count and process identity across each request interval. Listener
evidence reads both IPv4 and IPv6 kernel tables in the same private network
namespace, separately from application responses. Missing or untrusted listener
evidence remains unavailable.

Each run owns a fresh, never-overwritten output directory. Retain raw Engine,
Docker command, helper and wire evidence, prospective registrations, checkpoints,
per-history outcomes and final census, including failed runs. Separate created
containers, successful starts, actual requests, completed observations, unknown
observations, cleanup requests and verified absence. Stop requests alone do not
prove exit or deletion. Never stop a borrowed service or remove earlier evidence.
Probe records, authenticated fully sent requests, diagnostic-only bytes and
unknown request outcomes have separate counters. In M08, a separately labeled
host-only single-field contrast isolates the wrong network field after the real
inspection has already been rejected; it does not supply physical evidence.

## Verification and claim limits

The root agent owns manifest registration, the combined static gate, offline
qualification, physical Docker dispatch and final source reconciliation. The
physical test class uses the existing `GOSSIP_RUN_DOCKER_TESTS=1` convention and
must be selected through the central Docker lane after cheaper required gates.
This document and an unexecuted test are not physical evidence. Missing controls,
unexpected skips, infrastructure failures and unknown observations are reported
separately, never converted into success by retrying until green.

Reuse is allowed only for exact source/configuration, ordered suite, evaluator,
runtime/image, environment, limits, seed, protocol and purpose bindings where
reuse is authorized. A changed execution contract requires a fresh declared
mechanics run. No provider calls, paid experiment, publication, or product
acceptance authority is granted by these tests.
