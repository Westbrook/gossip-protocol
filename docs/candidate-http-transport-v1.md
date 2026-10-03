# Trusted candidate HTTP transport v1

This is a bounded observation boundary for C03 harness qualification. It has no
product verdict, acceptance authority, Docker dispatch, candidate import or
comparative-study result. The execution controller owns source/image identity,
server epoch and continuity, isolated helper configuration, process completion,
raw stdout/stderr retention and receipt authentication.

## Public interface

`gossip_harness.candidate_http_transport_v1` provides:

| API | Result |
| --- | --- |
| `WireLimits(...)` | Frozen validated observation resource policy |
| `request_bytes(recipe, port, limits=None)` | Exact declared request bytes |
| `build_probe_input(recipe, port, limits=None)` | Canonical, closed helper input JSON |
| `helper_source()` / `helper_sha256()` | Exact standalone stdlib helper source / digest |
| `max_probe_output_bytes(limits=None)` | Conservative stdout allocation bound including newline |
| `decode_probe_output(raw, recipe, port, limits=None)` | Immutable, independently reparsed `WireObservation` |
| `parse_response(raw, method, limits=None, eof=False)` | Framing observation without JSON interpretation |
| `decode_listener_snapshot(value, port, limits=None)` | Bounded kernel-table inventory for the declared port |

`WireObservation` retains exact `request`, successful-send prefix `sent`, and
captured `received` bytes; `sent_complete`, `socket_eof`, `termination`,
`exchange_complete`, and `limitations`; a `Response`; and listener snapshots
before and after the request. `Response` keeps final status/code, raw final
header block, ordered header pairs, informational blocks, ordered trailers,
transfer-decoded body bytes, framing kind, separate header/body/framing
completion, consumed-byte count and parser limitation. Header values are
Latin-1 strings; exact spelling, whitespace and bytes remain in `raw_headers`.

No product JSON is decoded here. Duplicate object keys, nonfinite JSON, invalid
UTF-8 or trailing JSON junk remain body bytes for the separately source-bound
semantic observer. A parser refusal alone does not establish a product defect.
A complete final status can remain observable when later framing is unusable.

## Closed input and isolated execution

The recipe is exactly `{method,target,headers,body_b64}`. Methods are literal
`GET`, `POST`, `HEAD`, `PUT`, `DELETE`, `PATCH`, or `OPTIONS`; targets are bounded
ASCII origin-form paths/queries. Header pairs are ordered two-string JSON lists.
The serializer neither adds nor sorts fields. The caller declares exactly one
`Host: 127.0.0.1:<port>`, one `Connection: close` or `Connection: keep-alive`, and a canonical decimal
`Content-Length` equal to the decoded body length. An empty non-POST request may
omit its length. Body base64 must round-trip canonically. Unknown fields,
control injection, absolute URLs, fragments, request transfer coding,
`Expect`, `Trailer`, and upgrades are outside this closed request profile.
These evaluator constraints do not assert that other lawful request dialects
violate the product contract. The default envelope admits both 65536-byte and
65537-byte POST bodies for the product's declared boundary controls.

The fixed CLI is `python -I /probe/helper.py /probe/request.json`. It reads a
bounded regular control file selected by that exact argument, not a response
path or shell string. The controller must stage and pin both files read-only,
authenticate every mount and exact argv, and run the helper in a separate
pinned container sharing only the intended server's network namespace. It
must have no candidate-source, input, DB or Docker-socket mount and no candidate
PID namespace. This module does not grant or verify those runtime privileges.

The helper opens only an IPv4 socket to `127.0.0.1` and the declared integer
port. It performs no DNS, redirect, compression negotiation or response-driven
operation. A new socket retries `ECONNREFUSED` establishment at fixed 10 ms
intervals within the original deadline and operation budget. Attempts and
refused-attempt counts are retained; the fixed policy supplies the refusal
reason. No retry occurs after connection establishment or any request send.
Each successful `send()` result contributes to the exact sent prefix; failure
cannot be represented as complete `sendall()` success.

The helper emits one bounded JSON transcript, with protocol/input/request
bindings, base64/count/SHA-256 records for the actual sent prefix and received
bytes, socket flags, termination, errno, elapsed time, operation/connection
counts and both kernel snapshots. Invalid evaluator input produces a fixed
diagnostic on stderr and nonzero exit. A natural helper exit says only that its
observation finished; it cannot turn an incomplete HTTP exchange into success.

## Completion and framing policy

The parser follows the message-length priority and chunk syntax described in
[RFC 9112 §6.3](https://www.rfc-editor.org/rfc/rfc9112.html#section-6.3) and
[§7.1](https://www.rfc-editor.org/rfc/rfc9112.html#section-7.1), within explicit
observation limits:

- HTTP/1.0 and HTTP/1.1 status lines, ordinary repeated headers, header casing,
  reason text, field whitespace, legal chunk extensions and harmless trailers
  do not require one output-tuned serialization.
- An exact Content-Length body or terminal chunk plus complete trailers ends
  observation immediately. Peer EOF is not required and is not claimed. A
  server that keeps the connection open can still supply a complete response.
- Without length or transfer coding, the body is close-delimited and requires
  observed peer EOF. Socket timeout/reset and helper exit do not substitute.
- HEAD and 204/304 use the no-body rules. Informational responses can precede
  a final response; 101 upgrade and unsupported transfer codings are recorded
  as limitations. 204 and informational responses with prohibited framing
  fields are not given body-completion credit.
- Identical duplicate/list Content-Length values normalize to one length;
  leading zeroes are legal. Conflicting values, TE plus CL, malformed field
  syntax, obsolete folding and forbidden trailer fields yield limitations.
- Decisive framing contradictions stop promptly. Captured prefix bytes are
  retained, but no drain, socket EOF or full-body claim is invented. Extra
  bytes already captured after a framed response invalidate its boundary.
  No claim is made about bytes a peer might send after the probe closes.

All header blocks, informational responses, chunk-size/extensions and trailers
share a cumulative metadata byte budget. Counts additionally bound fields,
informational responses and chunks. Decimal/hex length normalization avoids
unbounded integer conversion. No fixed product latency/response-size rule is
inferred from these evaluator bounds.

## Limits

| Limit | Default | Role |
| --- | ---: | --- |
| Request bytes | 131072 | Complete serialized request |
| Response bytes | 4194304 | Captured raw response, plus one overflow sentinel |
| Header/metadata bytes | 65536 | Cumulative response framing metadata; request header bound |
| Header count | 256 | Initial/informational/trailer fields; request fields |
| Chunk count | 65536 | Includes final zero chunk |
| Socket operations | 65536 | Connect/send/receive budget, plus failing-limit attempt |
| Each listener table | 262144 | Raw bytes, plus one overflow sentinel |
| Socket deadline | 10 seconds | Shared establishment/send/receive deadline |

Control input has a 2 MiB hard allocation cap. Output has a 32 MiB hard cap and
a tighter computed bound: 8192 metadata bytes plus base64 expansion of maximum
request, response-plus-sentinel and four listener-table-plus-sentinel records.
The default fits a 16 MiB process stream policy. The controller must reject a
policy whose computed bound exceeds its separately admitted stream cap before
dispatch. No parsed response body is duplicated in the helper transcript.

The response cap is observed by a read of at most the remaining capacity plus
one byte. Count and hash cover every captured byte including that sentinel;
the probe stops immediately without draining. A cap, operation bound, timeout,
unsupported coding or incomplete framing is an observation limitation, never
by itself a product failure.

## Listener evidence

The helper retains bounded raw `/proc/net/tcp` and `/proc/net/tcp6` tables after
successful connection establishment but before the first request byte, and
after socket closure. Failed establishment still emits diagnostic snapshots;
`listeners_before_stage=connection_failed_diagnostic` and `complete=False`
explicitly withhold established pre-request evidence. Successful establishment
uses `listeners_before_stage=connected_pre_request`. Each table declares `ok`,
`limit`, or `unavailable`, raw count/hash and errno. Missing IPv6 evidence is not
silently treated as an empty table.

Host decoding checks the table structure, address family and recorded native
byte order and extracts all LISTEN entries for the declared port. A complete
empty tuple means no listener observed; an incomplete table remains unavailable.
`127.0.0.1`, IPv4 wildcard, IPv6 loopback and IPv6 wildcard remain distinct
addresses. Snapshot absence/error does not erase independently complete HTTP
facets. Successful HTTP access alone does not establish loopback-only binding;
an observed interval does not prove future bind behavior or process ownership.

## Verification scope

Five offline test classes cover closed request validation, framing, the real
standalone helper over fake sockets, control-transcript integrity and kernel
listener parsing. They exercise every split boundary for representative length
and chunked messages, lawful variants, injection, contradictory framing,
truncation, caps, partial sends, startup refusal, timeout/reset and exact snapshot
ordering. They dispatch no real sockets or Docker operations. Physical isolated
helper/server mechanics, authenticated lifecycle continuity, restart persistence
and semantic product qualification remain separately owned verification lanes.
