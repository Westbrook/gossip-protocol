# C03 physical HTTP mechanics v2

This is a prospective qualification of HTTP observation machinery. It gives no
product acceptance credit, route-catalog coverage, browser qualification, or
whole-requirement verdict. The eight B03 HTTP target IDs and their source-grounded
dispositions remain unchanged. The independent C03 boundary does not replace the
three B03 v4 CLI additions that still await write approval.

The physical tests are in `tests/test_candidate_http_docker_v2.py`. Their small
evaluator-authored fixture is committed to an isolated Git store as inert source
bytes and runs as a long-lived TCP server in a container. It does not call a
reference application's `Service.request`, import candidate code on the host,
or receive expected answers. A separate, fixed trusted helper joins the
server's private network namespace. Docker also supplies the donor's managed
hostname, hosts and resolver files. Source, input and DB mounts do not enter the
helper; those Engine-managed files are not candidate mounts. The server retains `NetworkMode=none`; there are no published ports.

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


## Versioned donor-bound identity correction

Version 1 failed during the first M01 helper despite exit status zero and complete captured streams: Docker
changed the helper's initially assigned short-ID hostname to its network donor's
hostname. The frozen v1 comparison correctly rejected a change outside its
contract. That failed run and all v1 sources remain unchanged. Version 2 applies
`analysis/candidate-c03-http-probe-hostname-plan-v2.json`; it does not reinterpret
v1 diagnostic bytes as a qualified observation.

The v2 controller freezes the current running source-bound server's full ID,
epoch, baseline and current inspections, hostname, explicit empty domain,
source/tree/helper/fixture binding and runtime before creating a probe. A probe
may change from its own default short-ID hostname to that independently bound
donor hostname only in the qualified created-to-exited phase. Prestart, server,
keeper and running comparisons retain their prior exact rules. The inherited
hostname/domain/path facts do not permit additional namespace or candidate,
input, database or Docker-socket mounts.

Physical assertions independently decode untouched retained Engine responses
and check checkpoint order. They prove donor evidence precedes helper creation;
match the donor to the current source, epoch and server creation intent; require
an own-short-ID created/prestart helper hostname and the exact donor hostname at
exit; and compare all other Config values, all other HostConfig values, every
remaining immutable field and complete mount rows. The already-qualified
runtime-specific OOM false-to-null startup default remains a separate explicit
exception. Domains must be present strings and remain empty in the donor and both helper states.
`HostnamePath`, `HostsPath` and `ResolvConfPath` are retained and checked separately
against that same actual donor; they are not discarded or described as candidate
filesystem isolation.

These physical checks do not accept a production `matches` flag as their sole
oracle or feed host-modified inspections back into the positive identity check.
The M08 post-rejection single-field contrast remains explicitly host-only. M07
also requires each helper to name its own server epoch and distinct hostname,
while the original shared source, keeper and SQLite volume checks remain.
M05 checks the finite helper's donor transition separately from the intentionally
broken server continuity: diagnostic bytes still earn no product attribution.

The nine histories, fourteen declared request rows, exact 5039-byte server
fixture (`4a0f8e7b5885a908d8a060127c621d17bb6d8fbd19e1b9b29a0ca883550ea73d`),
request literals, 2-second observation bound, 8-second fixture delay and 4096-byte
M04 response cap are preserved from v1. A fresh complete physical run is required;
the v1 failed attempt and offline fakes supply no replacement correctness credit.

M08 writes its physical census inside cleanup before an earlier assertion can
propagate. It distinguishes creation intents, confirmed resources, unconfirmed
creates, start command intentions/results, validated running roles and verified
never-started helper state. Missing proof remains unknown; a failed control keeps
its partial census and cleanup errors without receiving qualification credit.
