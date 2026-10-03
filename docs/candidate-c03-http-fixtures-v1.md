# Independent prospective HTTP fixtures, version 1

`gossip_harness/candidate_http_fixtures_v1.py` supplies immutable fixture bytes and
expected public state for the prospective M1 HTTP catalog. It imports standard
library modules only. It does not import the candidate, reference implementation,
previous model outputs, observation adapter, or execution controller. It reads no
files and performs no requests, database work, admission, or acceptance decisions.
Both execution and acceptance authority are explicitly false.

The inputs are the frozen V0/M1 requirement prose in
`gossip_harness/library_project_fixture_v1.py:38–204` and the reviewed policy in
`analysis/candidate-b03-c03-policy-review-v1.json`. Their exact source hashes are
recorded in the immutable `SOURCE_PINS` tuple. The subsequent
`docs/candidate-c03-http-catalog-norms-v1.md` explains unresolved scalar coercion,
entry-field closure, error-code, and response-wrapper facets. This module does not
turn those unknowns into new requirements. Source pins are provenance metadata;
this pure module cannot authenticate the files currently on disk.

## Values and state derivation

`Entry`, `Document`, `Job`, `Token`, `Receipt`, `JobRecord`, `ExpectedState`, and
`Transition` are frozen dataclasses. Nested fixture collections are tuples. JSON
projection methods return fresh dictionaries/lists. Mutable observations cannot
be substituted for the fixture state. Constructor failures raise `FixtureError`,
an authoring error, not a product failure or HTTP classification. In particular,
positive nonboolean integer token/epoch arguments keep these fixtures unambiguous;
they do not assert that arbitrary HTTP bool/float epochs have a prescribed error.

`document(source, text)` implements the independently specified hash identities:
source and document IDs have distinct NUL-terminated prefixes and include exact
UTF-8 source bytes; blob identity hashes exact UTF-8 content. Titles use the last
source segment. No root, clock, platform newline conversion, or observed ID is an
input. Document JSON has exactly six fields. A source/text tuple does not prove
that a real importer read the corresponding file.

`ExpectedState.listing` uses literal casefolded substring search over
`source + '\n' + text`, source/ID ordering, and total before pagination.
`export` projects valid all/selected documents in that same order. Invalid fixture
selections fail authoring without assigning unknown product codes.
`census_offsets` selects pages from the prospective expected document count; it
never consults a candidate total to shrink coverage. Every actual mutation still
needs authenticated before/after jobs, documents, and export checks.

`submit` canonicalizes shape-valid entries by `(source, text)` and retains their
exact values. Reversed order replays; changed content conflicts in every state.
Semantic errors are deferred. `apply_action` derives the 20 cells in the five-state,
four-action matrix. It preserves stale-epoch-before-state ordering, running
prepare replay, cancelled cancel replay, completed receipt replay, and literal
cancel/retry epoch increments. Running prepare returns its existing token without
revalidation. Commit rechecks expected conflicts and updates all expected batch
documents together. Failed and cancelled jobs always have completed zero.

Validation covers isolated source-assigned traversal, unsupported suffix,
member/count/byte, duplicate source, catalog capacity and changed-source cases.
Unknown source-key lexical/byte/depth classifications raise `UnspecifiedFixture`.
Direct V0 import into a full catalog also raises it: rejection remains required,
but the M1 coordinator capacity code is not transferred to that route.
Multiple distinct faults also raise it; the fixture layer does not invent an
unspecified validation precedence. Raw invalid UTF-8 is represented in byte
fixtures, not coerced into a Python text entry. This is a bounded authoring model,
not a universal replacement oracle for every malformed request.

A retry retains the original manifest: a failed traversal manifest queues at the
next epoch and fails again on prepare. A prepared epoch-one token remains stale
after cancel/retry/reprepare and after successor completion or conflict failure.
A direct import between prepare and commit changes the expected catalog before
the atomic failure check. Matching completed commits reuse the original receipt,
which includes batch documents and excludes unrelated earlier/later documents.

Method `Job`/`Token`/`Receipt` values are available for separately qualified
method-level assertions. They must not become exact HTTP success-wrapper checks
for import, submit, single-job, or actions. The HTTP policy requires success,
complete JSON and independently corroborated supported public effects for those
routes. Public state cannot prove hidden blobs/manifests/receipts, transaction
atomicity, internal delegation, or that a method was actually invoked.
`reopen` preserves expected values only; it performs and proves no actual restart.

## Named immutable data

The named corpus retains `Alpha.txt` and `alpha.md` with identical
`Straße [a.b]\n`, `nested/é.md` with `CAFÉ\r\n`, and empty `z.txt`.
`CANONICAL_BATCH` deliberately begins in unsorted order: `ns/z.txt`,
`ns/nested/a.md`, and `ns/a.html`, carrying empty, CRLF Unicode, and literal markup
content. `VALID_MANIFEST` is `[a.txt=a, b.txt=b]`; `FAILED_MANIFEST` keeps
`../bad.txt=bad` plus `a.txt=a`. `guarded_state()` includes `guard.txt=guard\n`
and an empty queued `sentinel` job. `expected_state(name)` gives the five exact
subject states, with guard state unless explicitly omitted.

`stored_zip` writes ZIP32 STORED bytes directly. Member order and duplicates are
preserved. Each local/central record uses UTF-8 flag `0x0800`, compression method
zero, DOS date 1980-01-01 and time zero, Unix creator/version 20, extraction version
20, explicit mode (regular `0100644` by default), no extra fields, comments,
encryption, data descriptor, or ZIP64. CRC32, exact sizes and local offsets are
embedded. A directory entry gets the DOS directory bit; controlled symlink/FIFO
members have explicit Unix modes. This describes archive metadata, not a real
filesystem symlink. Serialization uses no compressor, clock, filesystem mode,
archive extraction, or candidate code. Tests decode the bytes independently with
Python's ZIP reader and inspect metadata and record offsets.

`json_bundle` uses compact unescaped UTF-8 JSON, retaining supplied entry order;
canonical expected manifest order is a separate operation. `intake_files()`
returns immutable path/byte/SHA256 descriptors for nonempty/empty canonical
ZIP/JSON, directory members, malformed ZIP/JSON/schema, bad UTF-8, duplicate ZIP
members, symlink/FIFO ZIP modes, deferred traversal/unsupported entries, and
64/65-member directory/ZIP/JSON representations. `EMPTY_DIRECTORIES` explicitly
records the empty directory. These descriptors do not create host paths.

`KEY_BOUNDARIES` fixes the 256/257 combined UTF-8-byte cases using a 128-byte Unicode
leaf and namespace lengths 127/128; each segment remains within 128 bytes. It also
fixes combined 16/17-segment cases. `DIRECT_SOURCE_BOUNDARIES` preserves the
entries/JSON variants and `INVALID_NAMESPACES` includes all nine lexical/segment-byte
representatives from the proposal. No error code is inferred merely from these
strings.

`request_body_boundary(65536|65537)` contains one legal 8192-byte Unicode member,
compact JSON and ASCII-space padding. Exact hashes are respectively
`0eb37848fedad2a718f470eef4acdf95ab05e677208a5c7bbe826cd7dd219560` and
`ba1bb39465a8c8dd744116805238e3e4e3f1c9bfba1ed6101b62364110d78ca9`.
The over-limit body adds exactly one trailing space. Character count differs from
byte count; the member is below its separate byte bound. The eventual request
provider must supply exact matching Content-Length and independently sufficient
wire bounds.

## Verification and remaining authority

Portable classes are `HttpFixtureDocumentTests`, `HttpFixtureStateTests`, and
`HttpFixtureBytesTests` in `tests/test_candidate_http_fixtures_v1.py`. They contain
literal ID and request-hash controls, independently enumerated 20-cell outcomes,
all-state stale priority/replay, immutable retry/fencing/conflict histories, ZIP
metadata/offset checks, public-state projections and boundary descriptors. Every
state/action cell checks status, exact successful method value/type and replay
flag, or error with no successful value/replay. These method-value controls do
not add exact HTTP success-wrapper authority. Aggregate-byte controls isolate
524288/524289 bytes with a fixed 17-member count and each member within32768 bytes.
M1 capacity controls distinguish a 256-source union from257 (including a full
64-member batch against192/193 existing sources), retain existing
identical sources in receipts without double-counting them, and recheck capacity
after a direct import between prepare and commit. Failure retains the complete
prior expected catalog and manifest with completed zero. They
exercise authored Python only and are not product or comparative model trials.
Root owns class/config registration and the combined verification receipt.

These constructors are usable inputs to prospective expectation registration.
They do not constitute the full 276-row provider, authenticate setup, bind a live
case to source/runtime, prove applicability, or supply production acceptance.
Key-bound rows with unspecified CODE still need a prospectively declared relation
between response CODE and persisted JOB.error, plus exact remaining public state.
They must not be omitted because this exact-value model declines to fabricate a
code.

The catalog still needs literal per-case fixtures/requests/censuses, every family
and target mapping, independently authenticated prerequisites, root switching,
confined direct/ancestor data links, genuine same-DB HTTP/CLI handoff, fresh raw
observation, and the complete qualified rehearsal. Wrong-kind, nested-root, link,
and two-root layouts need their qualified staging recipe; file descriptors do not
replace it. Hidden storage/transaction/browser/delivery obligations and the full
matched cumulative study remain unchanged. No full-ID credit or statistical
sample follows from this module or its unit tests.
