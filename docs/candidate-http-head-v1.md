# Bounded HTTP response-head facts v1

`candidate_http_head_v1.extract_response_head(raw, limits)` is a pure host parser
for independently eligible response metadata. It leaves the frozen wire-v1
helper and parser unchanged. Its returned `HeadFacts` carries no physical,
source, checkpoint, request-completion, or acceptance authority. An authenticated
journal adapter must establish those separately before using the facts.

A complete strict final status line remains observable when later headers are
truncated, malformed, or exceed a metadata bound. Complete syntactically valid
header fields remain observable with conflicting Content-Length values,
Transfer-Encoding plus Content-Length, unsupported transfer codings, or missing
body bytes. No partial field collection is exposed: that could misrepresent an
unseen field as absent. This extractor never declares a body or exchange complete;
wire-v1 still supplies the independent, unambiguous complete-body predicate.

The syntax profile matches frozen wire-v1: HTTP/1.0 or HTTP/1.1, a three-digit
100–599 status with the required spaces, CRLF line endings, token field names,
no obsolete folded fields, and Latin-1 field values without forbidden controls.
Field name case, duplicate order, and values are retained, with only surrounding
SP/HTAB stripped as in wire-v1. An unsupported profile is an unavailable
observation, not a new product requirement or inferred product failure.

Traversal starts at byte zero and never resynchronizes. Informational responses
are skipped only after their entire head is valid and within the cumulative
limits; Content-Length or Transfer-Encoding on an informational head blocks
traversal, as does status 101. A later string resembling a final status cannot
escape an invalid or incomplete informational prefix. Informational statuses
are never exposed as final status facts.

The exact `WireLimits` instance is type-checked and revalidated. Cumulative head
bytes include informational heads, final status, fields and terminating CRLF.
Field count is cumulative, and informational count is independently bounded by
the same wire-v1 header-count setting. Capture may have the wire-v1 single byte
response-limit sentinel, but parsing never consumes that sentinel. Inputs above
that capture contract or with wrong types raise `ValueError`; syntactically
unavailable facts use immutable `semantics.Missing` with a reason. A limit reached
after available metadata does not erase that metadata.

Every range uses zero-based half-open offsets into the exact `raw` input:

- `status_range` includes the final status line's CRLF.
- `headers_range` starts after that line and includes the ending empty CRLF;
  an empty header set has a two-byte range.
- `header_ranges` names each original field line, including its CRLF.
- `informational_ranges` names each complete admitted informational head.

`raw_sha256` and `raw_length` describe all supplied bytes, including a possible
sentinel or body. `examined_bytes` is the bounded parser-visible prefix length
(minimum of input, response cap and header cap), not a count of interpreted body
bytes. All output containers are frozen dataclasses and tuples, with runtime
checks on types, limits, field syntax and range structure. These value checks
are not authentication, and hashes do not establish provenance.

Offline tests cover independent status/header facts, informational barriers,
exact raw offsets, duplicate fields, frozen-profile grammar, cumulative limits,
sentinel boundaries, immutable typed values, and separation from wire body
framing and semantic judgments. No helper, socket, Engine, or candidate is run.
