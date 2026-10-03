# Candidate storage observation v1

This is an evaluator-owned physical observation seam for eight public B01
histories. It is not full M1/M4 acceptance, a qualified ScopePlan, a promotion
receipt, an independent held-out score, or evidence about swarm effectiveness.
The original 23 B01 target IDs remain a design target; these finite cases do
not establish their complete acceptance matrices.

`candidate_storage_cases_v1.py` declares expected identities, content digests,
job state, immutable manifests, stored receipts and public action results before
candidate execution. `candidate_storage_observer_v1.py` maps captured bytes on
the controller side and compares exact JSON types. A candidate-supplied `passed`
flag has no authority. Case definitions and the source that implements them are
separate identities; both must be retained when the suite is frozen.

| Case | Required action interval | Selected requirement facets |
| --- | --- | --- |
| rollback | Prepared running batch; injected failure; no provisional blobs/documents/receipt survive | A01, A07 storage conservation |
| capacity | Prepare at 254 documents; separate Store inserts document 255; commit two new sources | A01, A05, T07 commit-time capacity |
| invalid-admission | Submit member containing an extra field; immediate invalid_request; no job | T04 structural rejection |
| deferred-semantic | Admit mixed valid/path-invalid manifest; prepare persists failed/invalid_source | J06 deferred error and conservation |
| empty-batch | Prepare and commit zero entries; completed job and stored empty receipt | A04 empty receipt |
| canonical-replay | Replay reordered equal manifest on queued job; preserve immutable stored representation | J04, T04 selected admission replay |
| completed-replay | Recommit completed job with matching epoch; exact stored receipt unchanged | A03 selected terminal replay |
| value-mutation | Mutate selected returned JOB, manifest and nested receipt; same-instance public rereads and persisted state remain correct | T03 selected fresh-value behavior |

All requirement prefixes in the table are `M1-`. The assertion names identify
before, after and reopened state, public result, exact stored manifest/hash and
receipt conservation, and auxiliary table conservation. These are prospective
facets, not registered compiler applicability cells or complete requirement
verdicts. Registry authentication and independent scope review remain separate.

## Storage representations and authority

A registration pins the complete candidate source identity, mapper layout,
exact storage paths, full SQLite schema identity, and an external source-review
identity. Constructing this data object or calculating a matching digest does
not authenticate the review. The controller must verify that reviewed source
establishes a complete mapping of all persisted content, references, jobs,
immutable manifests and stored receipts; it must independently capture and
census the registered storage paths. It may not drop unregistered WAL, journal
or auxiliary files before asking this observer to judge the remaining bytes.

The initial SQLite mapper reads the reviewed `blobs`, `documents` and `jobs`
representation. It also captures every other physical table, including later
milestone revisions, document state and control records. No auxiliary table
mutation is expected during these eight action intervals: successful import
and epoch changes occur during setup, before the baseline capture. For the reviewed pinned reference profile, the empty
batch creates no document and changes no generation or epoch. All auxiliary
rows in this profile must remain exactly equal before/after/reopen. This is a bounded assertion
for these histories, not a rule forbidding legitimate mutations elsewhere. A source review must establish
that all extra tables in this profile are protected catalog/control state; a
different implementation with a mutable private telemetry/cache table needs a
separately qualified mapping and conservation projection.

The separately named v2 SQLite profile additionally registers exactly the two
zero-byte `catalog.sqlite.maintenance/maintenance.lock` and
`catalog.sqlite.maintenance/worker.lock` synchronization files established during
initial Store construction. Their identities remain in every capture. Missing,
nonempty or additional files are unqualified; there is no general lock-file
exemption. Before/after/reopened auxiliary-file conservation remains explicit
for the pinned reference after its setup interval. A valid implementation that
initializes another coordination file during an empty action needs a separately
reviewed mapper and allowed-transition profile; it is unqualified by this
profile rather than generically product-rejected.

These table names are an explicit mapper profile, not an undocumented product
requirement. Different valid storage representations require a separately
reviewed source-bound mapper. Unknown or incomplete mappings return observation
unavailable, never a passing product verdict or a product failure.

A second authored control physically writes content, catalog and jobs into
three separate JSON files, with base64 content values, and maps those bytes to
the same logical states. It qualifies independence from one physical layout.
It is **not** a second compliant M4 product implementation and does not relax
M4 portable SQLite compatibility or its schema requirements. Candidate-produced
JSON audit output is not a substitute for these controller-captured files.

The observer inventories every blob, including unreferenced rows. It retains
actual stored manifest/hash/receipt strings as well as parsed expected values.
Thus equal public document lists cannot conceal orphan blobs, and semantically
equal JSON rewriting cannot conceal a mutation of immutable stored fields.
Permitted JSON spellings are established from each captured baseline, never
forced to use the authored reference's formatting.

## Capture and parser limits

The driver is an executable Docker boundary accepting an exact registered
source map. Candidate Store/JobManager code runs in a fresh isolated container;
no candidate module is imported into the host observer. The persistent child
holds its Store through before and after phases. Host-owned pause/inspection and
bounded raw `/tmp` capture happen before the controller permits the next phase.
Public results remain untrusted process data. Reopen checks close and reopen a
Store in that child; they are not a process-death or process-restart experiment.

The boundary captures the completed public call interval, not an atomic CPU
instruction or an adversarial concurrent operation. It does not itself prove
that the injected failure hook ran after provisional writes. A07 additionally
requires exact-source review or separately qualified write-boundary evidence
of hook placement. Neither a claimed `injected_failure` result nor conserved
bytes alone proves that position.

SQLite is opened immutable/read-only with extension loading disabled,
`trusted_schema` off, query-only, a SELECT/read authorizer, and a progress budget.
Queries are fixed table scans; views and virtual tables are refused. Ordinary
write triggers are retained in the exact schema identity but never executed by
these SELECTs. The observer binds the whole schema; a different trigger requires
a different reviewed registration. It accepts only a complete main-database
capture, rejecting sidecars until a coherent capture protocol for those layouts
is qualified. A candidate using otherwise valid WAL storage is unqualified by
this profile, not declared incorrect.

Limits are eight files/eight MiB of captured bytes, 128 schema entries, 1,024
rows per table, two MiB per field, eight MiB of serialized row observations, and
200,000 virtual-machine instructions per SQLite inspection. Truncation, malformed
records, duplicate JSON keys, unknown schema/layout and incomplete census fail
closed as observation unavailable. Infrastructure/capture failure is separate
from an observed product mismatch. Malformed persisted records under an otherwise
registered mapper currently remain observation unavailable as well; this
conservative classification is not a reusable product correctness judgment.
Bounded mapped defects such as orphan rows, changed raw strings, wrong numeric
types and corrupted auxiliary rows are explicit assertion mismatches. These are evaluation-resource limits; they
do not alter the product's public size limits.

## Remaining work

The suite covers one queued canonical replay, one completed receipt replay,
one deferred path-error class, one post-prepare capacity interference, one
injected rollback, one structural admission error, an empty batch, and selected
returned-value mutations. It does not cover all-state replay, conflicting
manifests, all original/input/returned aliases, v0 import replay, the archive and
intake boundary matrix, actual concurrent overlap, browser/HTTP/CLI behavior,
process death/recovery, release acceptance or all four milestones.

Authored good/defect controls qualify mapper sensitivity separately from actual
candidate histories. The controller must still authenticate the scope and
review lineage, bind exact source/evaluator/runtime/limits/purpose, satisfy the
cohort barrier, and execute independently required acceptance observations.
