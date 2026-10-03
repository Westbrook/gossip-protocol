# Original HTTP journal observations v1

`gossip_harness/candidate_http_observation_v1.py` reads the original HTTP executor
v2 evidence without instantiating a controller, executing candidate source,
calling Git, dispatching a request, opening an Engine connection or spending
provider budget. Its output is a historical qualification reanalysis or a
synthetic fixture observation. It is never a new physical execution, a semantic
acceptance receipt, or a comparative project sample.

## Authority and public API

`admit_historical_qualification("M01")` selects one of nine explicitly registered
origins from `analysis/candidate-c03-http-qualification-origin-v1.json`. The exact
manifest SHA is compiled into the adapter. The manifest independently anchors
original audit/freeze/supplement/session/publication evidence, original source
and commit bytes, frozen dependency sources, eight complete journal inventories
and their external checkpoint sequences, and the manual zero-request M08
control. A caller cannot supply a trust hash, arbitrary manifest, authentication
boolean, purpose or acceptance option. Unknown origin IDs are rejected.

A normal clone can run the portable offline tests without any ignored `runs/`
artifacts. Historical admission additionally requires the exact retained raw
journals, audit anchors and checkpoint files, or the supported bit-identical
copies; it fails closed when they are absent. The published manifest does not
ship those raw artifacts or create fresh-execution authority.

The optional `evidence_root=Path(...)` reads a bit-identical relocated inventory;
it does not create a new origin. Original checkpoints and authority anchors
remain independently selected by the fixed manifest. M08's registration is read
from the hash-bound `prospective-registration.json` within the selected evidence
root, so private original role/bind metadata need not appear in the public
manifest. Those path strings are validated as metadata; their old staging
locations are never opened.

`admit_fixture(root, definition, checkpoints)` accepts authored offline fixtures
through a separate API and always returns `physical_origin=False`, even when a
fixture copies the producer's `mode=physical` field. `observe_history(bound)`
requires the exact object issued by one of the admission functions and its
unchanged content fingerprint. A `dataclasses.replace` clone retaining private
fields is insufficient. This is an application boundary within trusted Python
code, not a security boundary against an attacker already executing arbitrary
code inside the evaluator process.

`HistoryObservation` keeps acceptance, freshness, protocol and comparison labels
fixed. Its `comparison_authority` is `not_registered`. No API retrospectively
registers semantic expectations for the old mechanics responses. `ResponseFacts`
and semantic `compare()` remain pure values/comparisons; constructing them does
not establish original execution provenance or product acceptance.

## What is reconstructed

Admission uses bounded regular-file reads with no-follow path traversal and
nonblocking final opens. It checks exact inventories, forbids symlinks/nonregular
files, and checks original external checkpoints: one durable addition per event,
no changed earlier hashes, no duplicates, no rollback, and no foreign suffix.
Equal hashes at different filenames are legal. The original final checkpoint
must contain the terminal. Missing original intent/terminal produces
`ObservationUnknown`; the adapter has no retry/dispatch path.

The adapter reconstructs regular-file Git blob/tree IDs and the commit ID from
manifest-bound immutable bytes, without a Git process or importing candidate
source. It recomputes recipe, fixture, source, evaluator, helper, runtime profile,
limits, seed and role-policy bindings. Historical environment hashes remain
opaque authority from the independently audited original supplement; the
adapter does not read or reconstruct them from today's host environment.
Candidate `Config.Env`, argv, labels, mounts and restrictions are independently
validated from raw role inspections.

Frozen Engine parser methods operate over an inert retained-byte buffer. This
preserves the producer's exact bounded status/header/chunk grammar, including
its prohibition of duplicate headers and unsupported trailers/extensions in
Engine control evidence. Those restrictions describe this frozen control
protocol, not general application HTTP conformance. Raw requests bind the exact
verb/path/full container ID; decoded replies are compared with saved snapshots.
The adapter reconstructs role creation/start/stop, server and keeper continuity,
donor evidence recorded before helper creation, the closed v2 probe hostname
transition, wait/final natural exit0, and Docker multiplex streams. Separate
stdout/stderr descriptors must agree with the raw attachment. Confirmed creates,
start records, observation census, retirement before replacement, cleanup
ownership, removal and empty absence proofs are checked separately.

Original raw bytes cannot by themselves prove when the controller peeked at a
socket or observed EOF, or re-run successful checks on deleted temporary source
trees. Those events explicitly retain the authority of the original trusted,
source-bound controller and independent checkpoint sink. The adapter requires
that original capture/EOF event, then replays parser completeness checks; it
never infers an actual EOF merely because a saved file ended. Synthetic fixtures
exercise this logic but do not acquire the original temporal authority.

The new adapter, head extractor and semantic observer capture their import-time
source identities. Admission and observation reject pre- or post-admission
source drift. Frozen executor source pins are checked against its import-time
closure. To account for dependencies that were already imported before the
adapter, their source-declared functions and methods are compared with code
objects compiled from the pinned evaluator source **without executing it**.
Generated dataclass methods are not source declarations. This comparison uses
the same running Python interpreter; evaluator source compilation is not
candidate execution. New observer identities belong to reanalysis provenance,
not to claims that those observers existed during the original run.

## Independent facts and diagnostic rows

An eligible request can have a known wrong final status, readable headers,
unavailable body and independently readable listener evidence simultaneously.
The adapter uses `candidate_http_head_v1.extract_response_head()` for bounded
independent status/header facts and the unchanged wire-v1 decoder for body
framing. Head extraction never authorizes body completeness. Complete CL/chunk
framing does not require peer EOF; close-delimited bodies do. Truncation,
timeouts, capture limits and ambiguity leave affected evidence unavailable.

Each eligible `StepObservation` includes source/request/server/epoch/checkpoint
identities plus immutable `FieldEvidence`: original helper-output artifact path,
SHA and byte length; JSON pointer to the captured field; decoded byte SHA/length;
snapshot stage; and status/header byte ranges when available. Body provenance
references the original wire bytes; the body itself is supplied only after
complete transfer decoding. Listener table provenance retains IPv4 and IPv6
separately. A wildcard address remains a discrepancy despite incomplete sibling
table coverage; only-loopback evidence with incomplete coverage is unavailable.
A connection-failed diagnostic snapshot is not promoted into a request's
before/after listener facet.

M05's server lifetime breaks during its request. Its retained raw process data
remain diagnostic, its semantic facts are absent, and its unknown outcome is
counted. Earlier eligible rows survive a later unknown row or cleanup failure.
`missing_step_ids` is the uncompleted **step-result suffix**, not a count of
unattempted requests. In the original M05 it contains `request-1` and `stop-1`
even though `request-1` has a diagnostic observation. These are complementary
records, not a contradiction or permission to retry.

M08 validates the raw never-started helper and original wrong-network rejection.
A diagnostic copy changing only the network field must satisfy the intended
role, so unrelated defects cannot explain the rejection. Original server/keeper
continuity and raw owned-resource cleanup are reconstructed. This control has
zero requests and produces zero semantic rows.

## Retained 37-case regression contract

The original design matrix remains in full. The table distinguishes authored
adapter controls, composed component coverage and boundaries that remain future
work. It does not assert 37 independently executed tests or full mutation
coverage. Concrete method counts and execution receipts are supplied by the
verification owner at the held source fingerprints.

| # | Matrix case | Present coverage / remaining boundary |
| --- | --- | --- |
| 1 | Original audited journal with external checkpoints | Root-owned read-only historical reanalysis after offline gate; fixtures cannot prove physical origin. |
| 2 | Caller authentication flags/value objects | Authored rejection controls; exact issued-object and fixed result labels. |
| 3 | Journal and local hashes recomputed after mutation | Authored rehashed proof mutations; physical origin still needs fixed independent manifest. |
| 4 | Missing/duplicate/reordered/changed checkpoints or suffix | Authored checkpoint/inventory rejection controls. |
| 5 | Traversal/symlink/nonregular/oversized evidence | Authored path, size, symlink and nonblocking FIFO controls. |
| 6 | Intent without terminal or incomplete step suffix | Authored no-redispatch and unknown-suffix controls. |
| 7 | Repeated original reads | Authored repeated observation under forbidden execution/IO guards. |
| 8 | Duplicate observations/resources/epochs/order | Authored duplicate observation and shared-epoch positive; second-epoch raw reconstruction is source-reviewed and awaits root's planned original M07 reanalysis. |
| 9 | Source/commit/tree/fixture/request/runtime/environment mismatch | Authored representative commit/source/environment/request mutants; every field recomputed/bound, not every field independently mutated. |
| 10 | Changed loaded/disk observer/evaluator or mixed sources | Import-time pins and nonexecuting loaded-definition comparison; focused source-drift controls. |
| 11 | Wrong raw Engine ID/path/verb | Authored request mutations. |
| 12 | Complete chunked control versus malformed/trailing/truncated | Authored CL/chunk positives and malformed/duplicate/truncated negatives. |
| 13 | Forged role/donor/comparison flags | Pure raw reconstruction; authored hostname/domain/raw-frame/order mutations, not every comparison field independently mutated. |
| 14 | Wrong donor ID/epoch/network or donor after helper create | Authored ordering and role mutants plus planned root original raw reanalysis. |
| 15 | Exact closed v2 hostname transition | Authored valid distinct identities and wrong hostname/domain controls; frozen role profile tests supply broader mount/runtime negatives. |
| 16 | Bad stderr/wait/final/capture/EOF | Raw predicates reconstruct each; authored false-EOF control; full process variant coverage also resides in frozen executor tests. |
| 17 | Multiplex bytes differ from saved stdout | Authored rehashed raw-frame/descriptor contradiction. |
| 18 | Complete CL/chunk response without EOF | Authored facet controls. |
| 19 | Wrong final status with truncated JSON-looking body | Authored composed status-failure/body-unavailable control. |
| 20 | Deadline/cap/ambiguity after readable head | Authored composed facet controls; no product latency/output-size rule. |
| 21 | Final status line with truncated headers | Authored adapter control plus head component tests. |
| 22 | Valid fields beside conflicting CL/TE | Authored adapter control plus head component tests; body unavailable. |
| 23 | Informational response with incomplete final | Authored adapter control plus head component tests. |
| 24 | Complete/partial send and connection refusal | Authored partial-send/failed-connect controls; counted pre-send retry rules remain wire-v1 component coverage. |
| 25 | Wildcard with missing tcp6 versus loopback with same gap | Authored composed facet controls. |
| 26 | Failed-connect wildcard diagnostic | Authored wrong-stage diagnostic control. |
| 27 | Before/after/empty listener coverage independently missing | Semantic/wire component controls; adapter maps both independently; not every sibling absence separately authored here. |
| 28 | M05 server exit during response | Authored unknown-last/single-unknown controls plus planned root original historical reanalysis. |
| 29 | M08 rejection before request | Authored manual zero-row/non-network-defect controls plus planned root original historical reanalysis. |
| 30 | Earlier valid row survives later failure | Authored later-unknown and cleanup-failure controls. |
| 31 | Legal alternative JSON versus wrong typed sibling | Semantic component tests; original mechanics expectations are not retroactively registered. |
| 32 | Unsupported content coding with known wrong status | Semantic component tests; adapter supplies independent head/body facts. |
| 33 | Supported versus unspecified outer wrappers | Semantic component tests; full route catalog/state effects remain future work. |
| 34 | Assigned error versus unassigned code/404 override | Semantic component tests; source-grounded expectation registry remains future work. |
| 35 | Candidate-derived or post-observation expectation | No adapter comparison/expectation registration API; original output fixed to not_registered; prospective expectation authority is unfinished. |
| 36 | Public state agreement without atomicity/conservation | No whole-requirement verdict API; those separate product observations remain unfinished. |
| 37 | Qualification reused as independent acceptance/cohort | Authored purpose and fixed-authority rejection controls; fresh independent acceptance contract remains unsupported. |

## Remaining integration

The full ten-family HTTP catalog, source-grounded predispatch semantic
expectations, independently derived expected document/job state and state-effect
histories, versioned link fixtures, HTTP/CLI same-DB handoff, and the production
M1-GATE-HTTP prerequisite/registry bridge remain required. The eight C03 IDs and
M1-A08 transport interaction do not establish whole M1 or the rest of M1-A08.
Public state alone cannot establish blob/receipt conservation or transaction
atomicity. Fresh private independent acceptance needs a prospective versioned
execution/observer/catalog contract and cohort freeze. No historical
qualification receipt is relabeled to satisfy that requirement.

The early author controls exposed a fixture canonical-path mismatch, then an
adapter case-sensitive Engine-header lookup, then permissive acceptance of a
truncated chunk terminator. The fixture path was corrected and the adapter now
uses the frozen Engine grammar. Independent source review additionally exposed
replace-clone authority escalation, permissive result-authority fields,
nonregular-file blocking, and source relabeling after imports. Those issues
motivated the explicit issued-object binding, fixed labels, bounded reads and
loaded-code checks above. Root's combined gate and historical reanalysis must
qualify the final combined source; earlier component passes are not substituted
for that check.
