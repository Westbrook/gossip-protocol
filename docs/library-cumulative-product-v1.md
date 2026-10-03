# Local Research Library: cumulative product contract v1

**Status: prospective-unqualified.** This specifies M2–M4 for the existing authorized development/scaling study. It does not implement or accept those milestones, freeze missing migration fixtures, qualify a run, authorize provider work, or change the frozen M1 pilot. No private cases or model responses are included.

The [closed machine registry](../library-cumulative-product-v1.json) is normative together with this readable rendering. Requirement IDs remain stable across later acceptance records. The original [v0/M1 public contract](../gossip_harness/library_project_fixture_v1.py) and [large-swarm plan](large-swarm-project-plan-v1.md) remain binding. The exact source hashes are in the registry. The independent acceptance machinery must bind the final contract hash before future model work; detailed requirements are revealed only at their common preregistered milestone boundaries.

## Shared bounds and values

All objects described below are closed: extra keys are errors. Integer means a JSON/Python integer excluding booleans; strings must encode as strict UTF-8 unless explicitly retained as escaped original job-serialization strings. New list/history/export results have no timestamps. Unicode ordering means Python code-point order, with no locale sorting.

| Bound | Value |
| --- | ---: |
| `max_documents` | 256 |
| `max_file_bytes` | 32768 |
| `max_revisions_per_document` | 16 |
| `max_retained_revision_blob_bytes` | 16777216 |
| `max_note_bytes` | 16384 |
| `max_tags_per_document` | 32 |
| `max_collections_per_document` | 16 |
| `max_collections` | 64 |
| `max_name_bytes` | 64 |
| `max_query_characters` | 256 |
| `max_page_size` | 100 |
| `max_post_bytes` | 65536 |
| `max_reindex_step` | 64 |
| `max_export_bytes` | 16777216 |
| `max_backup_bytes` | 67108864 |

The inherited per-source identities, 32768-byte file limit, 256-record capacity and original archive/resource limits remain unchanged. New revision-retention limits cannot reject an otherwise legal pre-refresh v0/M1 catalog. Notes and names are bounded independently. An oversized backup does not invalidate a legal database.

### `DOCUMENT0`

Exactly these fields:

- `document_id`: document identity string.
- `source_id`: source identity string.
- `source`: canonical inherited source key.
- `blob_id`: current blob identity string.
- `title`: last source segment.
- `text`: exact decoded current UTF-8.

### `RECORD2`

Exactly these fields:

- `document`: DOCUMENT0.
- `revision`: integer 1..16.
- `edit_version`: integer >=1.
- `deleted`: boolean.
- `notes`: bounded string.
- `tags`: sorted normalized string array.
- `collections`: sorted normalized existing-name array.

### `REVISION2`

Exactly these fields:

- `revision`: integer 1..16.
- `blob_id`: blob identity string.
- `text`: exact decoded UTF-8.

### `RECORD4`

Exactly these fields:

- `document_id`: unchanged logical ID.
- `source_id`: unchanged source ID.
- `source`: unchanged source key.
- `title`: unchanged title.
- `current_revision`: REVISION4.
- `edit_version`: integer >=1.
- `deleted`: boolean.
- `notes`: bounded string.
- `tags`: sorted normalized string array.
- `collections`: sorted normalized existing-name array.

### `REVISION4`

Exactly these fields:

- `revision_id`: deterministic rev- SHA256 identity.
- `revision`: integer 1..16.
- `blob_id`: unchanged blob identity.
- `text`: exact decoded UTF-8.

### `BACKUP3`

Exactly these fields:

- `format`: literal local-research-library-backup-v3.
- `payload`: closed object of the arrays/fields in persistence.backup_rows.
- `payload_sha256`: 64 lowercase hex characters.

### `DIAGNOSTICS3`

Exactly these fields:

- `schema`: integer 3 or4.
- `generation`: nonnegative integer.
- `documents`: {active:nonnegative integer,deleted:nonnegative integer}.
- `revisions`: nonnegative integer.
- `blobs`: {count:nonnegative integer,bytes:nonnegative integer}.
- `jobs`: {queued:integer,running:integer,completed:integer,cancelled:integer,failed:integer}; all >=0.
- `worker_generation`: nonnegative integer.
- `worker_state`: idle|running|stopped.
- `index_state`: absent|stale|building|current.
- `last_error`: null or {operation,code}.
- `recovery_action`: none|restart_worker|retry_reindex|choose_new_backup|validate_backup|run_migration.

## Milestone requirements

### M2-IDENTITY-REVISIONS: Stable identity, retained revisions and edit tokens

Packages: catalog. Dependencies: inherited v0/M1 release.

Keep the inherited source_id, document_id and blob_id formulas, exact source spelling, title and six-field legacy projection. Logical identity never derives from content or an absolute root. Every genuinely new imported document starts with revision=1, edit_version=1, deleted=false, notes="", tags=[], collections=[]. Existing v0/M1 records receive these defaults atomically when first opened by M2.

A revision is immutable {revision,blob_id,text}; revision is a per-document integer starting at 1. Changed refresh appends current revision+1 even if its bytes equal an older revision; it reuses an existing blob hash. An unchanged refresh appends nothing. All revisions, tombstones and annotation state persist across reopen.

Each effective document change (refresh, annotation replacement, delete, restore) increments edit_version exactly once in the same transaction. No-op writes leave it unchanged. The expected_version request integer is the sole optimistic document edit token; it covers annotations and deletion as well as content, so content ABA never revives an old token.

The 256-document bound includes deleted records. Retain at most 16 revisions per document; an otherwise valid changed refresh beyond this bound fails revision_capacity without pruning. Across retained revision blobs, distinct raw UTF-8 content may occupy at most 16777216 bytes; any catalog write exceeding that bound fails capacity. Existing legal v0/M1 catalogs fit this bound. Blob references also include every original completed-job receipt; no deletion, refresh, annotation change, reindex or automatic cleanup prunes a retained revision or a blob referenced by a document, revision or original receipt. Explicit backup restore replaces the selected logical state as specified in M3.

All reads return fresh JSON-compatible values. All document writes, revision/annotation changes, generation changes and source/capacity rechecks commit together. Failures publish no partial state. Concurrent effective writes with one expected_version have one winner and stale_version for the loser.

Required evidence lanes: public-contract, independent-integration, durability.

### M2-REFRESH: Explicit bounded refresh preserving provenance

Packages: ingestion, catalog. Dependencies: M2-IDENTITY-REVISIONS.

Refresh addresses an existing document_id and accepts exactly one input: text (a JSON string) or path (a confined regular .txt/.md/.html file relative to the configured input root). The text is strict UTF-8, at most 32768 encoded bytes; preserve decoded bytes/newlines, including empty text and literal HTML. A path may differ from source; it supplies replacement bytes and never changes source/title/IDs. No original archive or directory location is inferred from the logical source key.

Path confinement checks the configured root, every ancestor and leaf for symlinks; reject absolute paths, backslashes, NUL, empty/dot/dot-dot segments, more than 16 segments and inherited byte limits. Source/path syntax fails invalid_source; other file types unsupported_type; non-regular/missing/unreadable files io_error; invalid UTF-8 invalid_utf8; byte overflow too_large.

After transport shape/type validation and input decoding, acquire the write transaction and check document existence, expected_version, then deleted state, then capacity. Missing document is not_found; stale token is stale_version (including on a tombstone); a matching token on a tombstone is document_deleted. Return {status:"refreshed"|"unchanged",record:RECORD2}. An equal current blob is unchanged; annotations and source identity are retained in either case.

Original import and M1 batch intake still reject changed current bytes as source_changed. Identical bytes for an existing tombstone return/include the six-field current projection without restoring it. A completed M1 job replays its originally persisted receipt, including the original document text, even after refresh, deletion or restoration.

Required evidence lanes: public-contract, cli, http, independent-integration.

### M2-ANNOTATIONS: Bounded notes and normalized tags

Packages: catalog. Dependencies: M2-IDENTITY-REVISIONS.

Annotations is an atomic complete replacement {expected_version,notes,tags,collections}. Notes is a strict UTF-8 string of at most 16384 bytes with no normalization; empty notes and literal markup are valid. Tags and collections are arrays; duplicates after normalization are invalid_request. There are at most 32 tags and 16 collections per document.

Normalize each tag and collection name by NFC, Unicode strip(), Unicode casefold(), then NFC. Require a nonempty result of at most 64 UTF-8 bytes, containing neither U+0000..U+001F nor U+007F. Names are stored and returned normalized and sorted by Python Unicode code-point order. Invalid name/type/count/duplicate is invalid_request; note-byte overflow is too_large; malformed Unicode text is invalid_utf8.

Check existence, expected_version and deleted state in the write transaction before membership existence. Every collection must already exist or the write fails collection_not_found. Equal normalized values return {status:"unchanged",record:RECORD2}; otherwise replace all three fields and return status:"updated" with edit_version incremented once. The content revision does not change. Deleted records must be restored before annotations can be edited.

Required evidence lanes: public-contract, cli, http, browser, independent-integration.

### M2-COLLECTIONS: Collection registry and membership consistency

Packages: catalog, query. Dependencies: M2-ANNOTATIONS.

Collections are a flat normalized-name registry, at most 64 names. No hierarchy or implicit collection creation exists. GET lists {collections:[{name,total}],generation}; names are sorted, total counts all members including tombstones. Membership is replaced through annotations under each document edit token.

Create/remove use {name,expected_generation}; generation is the nonnegative catalog generation. After shape/name validation, compare generation inside the transaction before lookup: a mismatch is stale_generation. Create of an existing name returns unchanged; new creation returns created and increments generation once; exceeding 64 names fails capacity. Remove of an absent name is not_found; a collection with any member, including a tombstone, fails collection_not_empty; successful removal returns removed and increments generation once.

Collection mutation response is {status:"created"|"removed"|"unchanged",name,generation}. A no-op does not change generation. There is no rename operation, cascade deletion or collection write that silently edits a document token.

Required evidence lanes: public-contract, http, browser, independent-integration.

### M2-DELETE-RESTORE: Soft deletion and restoration without loss

Packages: catalog. Dependencies: M2-IDENTITY-REVISIONS.

Delete and restore each accept {expected_version}. Check request shape, document existence and token in that order. Matching delete sets deleted=true; matching restore sets deleted=false. Each effective transition increments edit_version and catalog generation once and returns {status:"deleted"|"restored",record:RECORD2}. Repeating the same transition with the current token returns status:"unchanged"; repeating with the old token is stale_version.

Neither operation creates a content revision, changes annotations/memberships, releases document capacity nor deletes a blob. Active legacy list/search/show/export omit tombstones; an explicitly selected tombstone is not_found. A lifecycle show or revision-history read addresses tombstones as well as active records. Only explicit restore reactivates a document; import, receipt replay and query never do.

Required evidence lanes: public-contract, cli, http, browser, durability, independent-integration.

### M2-QUERY: Deterministic current-state query and pagination

Packages: query. Dependencies: M2-REFRESH, M2-COLLECTIONS, M2-DELETE-RESTORE.

Legacy list/search and six-field exports retain the original casefolded literal substring search over source+"\n"+current text only, source then document_id Unicode ordering, exact field sets, total-before-pagination and offset/limit validation. Notes/tags/history never enter the legacy search haystack.

Lifecycle listing parameters are q (default "", at most 256 characters), tag (optional single normalized exact match), collection (optional single normalized exact match), deleted (active|deleted|all, default active), offset (default 0), limit (default 100), and generation (optional nonnegative integer). Filters combine with AND; an absent valid tag or collection matches zero records. Text search is the inherited haystack. Output is {records:[RECORD2...],total,generation}.

One read transaction selects the catalog generation, filters, total and ordered page. If requested generation differs, fail stale_generation without a page. Mutation between default offset pages can change results; clients requiring a stable pagination series must echo generation and restart on stale_generation. No long-lived cursor or snapshot retention is promised.

Catalog generation starts at 0 for an empty new database and increments exactly once per effective catalog transaction, including a nonempty batch adding one or more new documents; unchanged imports/commits/edits and job-only transitions do not increment it. A legacy migration starts generation=0 for the complete migrated snapshot. Browser pagination carries generation and resets to the first page after a local mutation or stale_generation.

Required evidence lanes: public-contract, cli, http, browser, independent-integration.

### M2-INTERFACES: One durable lifecycle contract through API, CLI and browser

Packages: query, clients. Dependencies: M2-QUERY.

Add the exact routes/commands in interfaces.m2. Preserve all v0/M1 routes and commands. New routes use the inherited JSON/content-type/65536-byte POST bound; missing/extra fields, duplicate/unknown query keys and booleans where integers are expected are invalid_request. JSON objects are closed; unknown route/method is not_found. All successful data commands/routes return their specified single JSON value with status/exit 200/0; domain errors retain {error:CODE}, stderr/exit 2.

New HTTP conflict codes stale_version, stale_generation, document_deleted, revision_capacity and collection_not_empty are 409; not_found is 404; collection_not_found and other validation/capacity/I/O errors are 400. Existing M1 status mappings remain unchanged. Decimal query/CLI token syntax is ASCII digits without sign; JSON tokens require actual integers, not booleans.

Add accessible lifecycle controls as enumerated in interfaces.browser. They operate through the same Service and persistent Store as CLI. Display current document text/notes/source/tags/history literally; never execute imported HTML. Every mutation refreshes record, jobs if relevant, total/page and generation, and replaces previous status with the current literal error code or success. A stale edit never auto-retries or overwrites the new server state; preserve the unsaved draft visibly until reload or explicit resubmission with a fresh token.

Client selection stores document IDs, never row indexes. Display exact sorted record sets without ghost rows; all selected export IDs are validated before producing a result. Reopening the browser or CLI reflects durable state, not a separate client database.

Required evidence lanes: public-contract, cli, http, browser, independent-integration.

### M3-WORKER-RECOVERY: Opt-in durable worker recovery and stale-worker fencing

Packages: ingestion, catalog. Dependencies: M2-INTERFACES.

M1 submit/prepare/commit remain explicit manual operations. Opening Store, normal CLI invocations and serve alone do not run jobs or change their epochs. A new worker-enqueue operation enrolls a queued/running job for automatic execution; terminal enrollment is job_state, duplicate enrollment is unchanged. Cancelled/failed/completed jobs never automatically retry. Retrying a previously enrolled job retains enrollment but requires the explicit inherited retry transition.

worker --once takes the exclusive worker-owner lock and processes at most one enrolled queued/running job, selected by job_id Unicode order; worker without --once repeats until stopped and polls once per second when idle. A second live owner returns worker_busy without changing any job. Each successful owner start records a new durable worker generation; worker claims bind installation incarnation, generation, job_id and the inherited epoch. A resumed running job uses its persisted immutable manifest and existing public epoch.

Before every worker state/catalog write, validate current owner generation/incarnation and job epoch in the same transaction as the write. A stale worker claim fails stale_worker and has no effect; a current worker with an old job epoch fails stale_epoch. Manual inherited commit(token) still has the original two-field token semantics. Cancellation/retry and restore invalidate worker claims. Enrolling a job does not bypass M1 validation or commit-time conflict/capacity checks.

An abrupt exit before commit leaves queued/running durable state with completed=0 and no provisional documents/blobs/receipt. A fresh process resumes the enrolled job; an exit after SQLite commit but before response leaves exactly one completed receipt and no duplicate document/revision on recovery. No filesystem source reread is allowed for an admitted manifest. Domain validation failures retain the specified failed job; I/O/worker failures leave a retryable durable queued/running state and diagnostics, without claiming completion.

Owned staging is confined beneath a db-specific maintenance directory or the configured backup directory, with operation IDs and exclusive locks. Backup stages use hidden unique .partial basenames in the backup directory so final publication is on the same filesystem. At owner startup and after each maintenance operation, cleanup must remove abandoned owned backup/restore staging after proving no live owner holds it, and delete superseded shadow/published search generations after the atomic publication switch. Cleanup may visit at most64 recorded artifact entries per pass and must resume from a persisted cursor until all eligible owned artifacts are handled; idle worker cycles perform the next pass. It never removes user inputs, valid published backups or blobs/revisions/receipts still referenced by the retained catalog. All artifacts have durable ownership records before creation; unknown/unowned filesystem entries are left untouched. No broad directory wipe or age-only deletion is allowed. Actual fresh worker processes and abrupt termination at before-commit/after-commit boundaries are required independent evidence; graceful HTTP restart or modeled reopen is insufficient.

Required evidence lanes: public-contract, process-restart, durability, independent-integration.

### M3-REINDEX: Bounded restartable index generations

Packages: catalog, query. Dependencies: M2-QUERY.

Index generation is derived data, never the catalog authority. reindex-step accepts {limit} with integer 1..64. It visits at most limit document IDs in source/document_id order from a single captured catalog generation, retaining a durable cursor and shadow index; it indexes only active current source/text. It returns {state:"running"|"completed",generation,target_generation,processed,total,cursor}, where generation is the published generation (or null), processed is cumulative work for target_generation, total is target active-document count, and cursor is the last processed document_id or null.

Before each step and publication compare the captured generation with the catalog generation in the same transaction. A changed generation discards only shadow work and restarts at the new generation; the current call still handles at most limit documents. Publish a complete shadow index by an atomic pointer switch. An empty catalog completes immediately with processed=total=0 and cursor=null.

While an index is absent/stale/building, all list/search/query operations answer from the authoritative catalog in one read snapshot; they never return stale or partial index results. Restart resumes the persisted cursor if its target generation still matches, otherwise rebuilds. A crash at shadow-write/publication boundaries leaves either old published generation or new complete generation, with catalog, revisions, annotations and original job receipts unchanged.

Required evidence lanes: public-contract, process-restart, durability, independent-integration.

### M3-EXPORT: Bounded explicit portable selection

Packages: query, clients. Dependencies: M2-QUERY.

Legacy /api/export and export CLI retain exactly local-research-library-v0 and their six-field projection. The new export-bundle operation is separate, with exact body {ids,include_deleted,include_history,max_bytes}. ids is null for all matching records or a distinct array of at most 256 document IDs (empty means none); booleans are actual booleans; max_bytes is integer 1..16777216. Validate the entire selection before emitting any bytes; missing or disallowed deleted IDs are not_found.

Capture all selected current records and optional immutable histories in one read transaction; order documents by source/document_id and revisions by ascending revision number. The exact payload is {format:"local-research-library-export-v2",generation,documents:[{record:RECORD2,revisions:[REVISION2...]}...]}; revisions is [] when include_history=false. Canonical encoding is UTF-8 JSON with ensure_ascii=false, sort_keys=true and separators=(",",":"); no BOM, trailing newline or timestamps. Encode valid Unicode only. If encoded bytes exceed max_bytes, return too_large and emit no partial result.

HTTP returns that JSON body; CLI prints one JSON value; browser offers one download named research-library-v2.json with exactly the canonical payload bytes after successful complete validation. A failed export has no download. Browser evidence must trigger its actual Export selection control and inspect the resulting bytes through a bounded, declared download observation; a direct HTTP call alone is not browser evidence. Export is not a restorable backup.

Required evidence lanes: public-contract, cli, http, browser, independent-integration.

### M3-BACKUP-RESTORE: Validated bounded backup and atomic activation

Packages: catalog, clients. Dependencies: M3-WORKER-RECOVERY, M3-REINDEX, M3-EXPORT.

Backup requires a fresh destination basename under the configured backup directory (1..64 ASCII alphanumeric/underscore/hyphen plus .json); no absolute path, nested path, symlink or existing destination may be used. The configured directory is checked for ancestor symlinks. Existing destination is already_exists; invalid path is invalid_source; I/O is io_error. A backup snapshot includes all documents/tombstones, revisions, notes/tags/collections, blobs, schema metadata, canonical job manifests/content hashes and original receipt text, plus enrolled-job state; derived search index and transient claims are excluded.

The exact closed backup envelope and row types are value_types.BACKUP3 and persistence.backup_rows. Its payload is canonical UTF-8 JSON as for export; payload_sha256 hashes only canonical payload bytes. Envelope size is at most 67108864 bytes. This bounds the backup operation, not the legal number of M1 jobs: a larger valid database gets too_large and stays usable. Write an exclusive staging file, flush/fsync it, then publish without replacing an existing destination and fsync the directory. After publication, persist the completed backup name/size/digest/generation in a durable registry. Recovery registers a complete owned publication if a crash occurred before registration. Listing reads bounded metadata pages, not arbitrary directory contents or every backup payload; partial staging is not a backup. Restore may validate a manually supplied complete basename whether or not this installation created it.

Restore accepts an existing basename and expected_generation. Read at most 67108865 bytes without following symlinks. Validate envelope exact keys/version/size/digest; schema, row shapes and ordering; uniqueness; all ID/content hashes; revision continuity/head references; normalized annotation bounds; collection memberships; complete job state/manifest/content-hash/receipt invariants; blob references; and resource limits before acquiring activation authority. A malformed or corrupt backup is invalid_backup; over-bound input is too_large; a well-formed unsupported format/schema is unsupported_schema. Semantic invalid entries in queued/running/failed/cancelled M1 manifests remain legal if they satisfy the inherited admission shape and recorded deferred-error semantics.

Acquire exclusive maintenance authority, fence workers, then compare expected_generation to the live catalog generation. An active owner that cannot be quiesced gives maintenance_busy; a generation mismatch is stale_generation. Validation or lock failure leaves the live catalog unchanged. Activate validated data in one SQLite write transaction, materializing the backup logical schema3 into the current runtime physical schema3 or4 and setting a new catalog generation equal to previous live generation+1. Schema4 restore derives revision IDs deterministically and never downgrades metadata to3. Readers see the entire old or entire new snapshot; existing client handles detect the new incarnation before further writes. Rebuild derived indexes lazily. No copy-over of an open SQLite/WAL file is an atomic restore.

Restore preserves the selected snapshot logical IDs, every retained content revision, annotations and original completed-job receipt bytes. It intentionally restores the selected snapshot logical document/job set, including tombstones; records created after the snapshot are removed from the logical catalog. Durable per-document edit high-water marks and absent-document fencing tombstones remain target-side. Every incoming record receives edit_version=max(snapshot edit_version,target high-water)+1, and later reimport of a removed document ID starts above its retained high-water; a truly new ID still starts at1. This invalidates every pre-restore document edit token while preserving content revision numbers and data. It also retains target-installation per-job epoch high-water marks outside the replaced payload, including fencing tombstones for jobs absent from the backup. Incoming noncompleted jobs receive epoch=max(incoming epoch,target high-water)+1; completed jobs keep their snapshot epoch/receipt and can only replay. Later creation of a previously removed job ID uses target high-water+1, while genuinely new IDs start at 1. This prevents an old two-field job token from regaining write authority after restore. Every new worker generation/incarnation is fenced independently. These rules apply only after the new explicit restore event; all histories using only v0/M1 retain their original results.

A failure or abrupt exit before activation commit leaves the old catalog complete; after commit leaves the new one complete, with fencing committed consistently. Startup resolves abandoned owned staging without replacing either catalog. Restore returns {restored:true,generation,documents,jobs}; counts include tombstones and all restored jobs. Repeat restore is a new explicit replacement requiring the current expected_generation; it cannot silently replay with a stale generation.

Required evidence lanes: public-contract, cli, http, process-restart, durability, independent-integration.

### M3-DIAGNOSTICS: Durable bounded truthful recovery diagnostics

Packages: catalog, query, clients. Dependencies: M3-BACKUP-RESTORE.

Diagnostics returns the exact DIAGNOSTICS3 value from one durable snapshot: physical schema, catalog generation, active/deleted counts, revision/blob counts and bytes, job counts by all five states, current worker generation/state, index state, last maintenance error and recovery action. It contains no document text, annotations, input-root absolute paths, stack traces, provider keys or fabricated successful state.

worker_state is idle|running|stopped; an owner is running only while its live lock is held. index_state is absent|stale|building|current by actual generation and cursor. last_error is null or {operation:worker|reindex|backup|restore|migrate,code:declared domain code}; recovery_action is none|restart_worker|retry_reindex|choose_new_backup|validate_backup|run_migration. Persist the last error and replace/clear it only when that operation next fails/succeeds; diagnostics reads do not mutate state.

Public diagnostics exposes content graph counts as an observation contract, so independent checks can distinguish no provisional writes from hidden orphan blobs. Browser and CLI display current error code and recovery action literally. A failed action followed by success replaces stale error text; diagnostics itself cannot clear a failed job or silently retry an operation.

Required evidence lanes: public-contract, cli, http, browser, independent-integration.

### M3-INTERFACES: Recovery and portability through real clients

Packages: query, clients. Dependencies: M3-DIAGNOSTICS.

Add exact interfaces.m3 endpoints/CLI forms and accessible maintenance controls. Path-taking operations accept only a basename under an explicitly configured local backup directory; raw backup bytes are never posted through the inherited 65536-byte body limit. worker_busy, stale_worker, maintenance_busy, already_exists use HTTP409; unsupported_schema/invalid_backup/too_large use400. No shell command, arbitrary filesystem destination or private failure-injection field is exposed over HTTP.

CLI and HTTP share durable Store/JobManager/Service operations. Browser can enqueue, run a bounded reindex step, export selected documents, create/restore a named backup and inspect diagnostics; restore requires a deliberate button action tied to the displayed generation, with no auto-retry after stale_generation. User-visible state follows committed results and survives browser reload.

Public hooks used to pause/terminate a real worker or maintenance operation at declared boundaries must be separately frozen as evaluator-only interfaces before execution. They are never user request fields and never contain hidden acceptance data or expected answers. Independent end-to-end recovery uses fresh application processes; study builder-agent restart is a different event.

Required evidence lanes: public-contract, cli, http, browser, process-restart, independent-integration.

### M4-API-SCHEMA: Predeclared revision-aware API and storage transition

Packages: catalog, query. Dependencies: M3-INTERFACES.

Physical schema versions are 0 for frozen v0/M1, 2 for M2, 3 for M3 and 4 for the compatibility release. M1 jobs can exist in schema0; absence/presence of that table is validated, never guessed from wire health. M2 adds the exact portable table layout in persistence.schema2; M3 adds maintenance/index tables without changing its document/receipt layout; M4 replaces lifecycle/revisions with persistence.schema4 normalized revision-ID references.

M4 revision_id is "rev-"+sha256(b"revision\0"+document_id.encode("ascii")+b"\0"+str(revision).encode("ascii")+b"\0"+blob_id.encode("ascii")).hexdigest(). It depends on logical document, historical revision number and immutable blob identity; restoring an older snapshot and creating different content at the same number cannot reuse the old revision ID. M4 adds /api/v1 routes returning RECORD4/REVISION4 and versioned export format local-research-library-export-v4. Conversion preserves source/document/blob IDs, text, annotations, deletion and edit_version. Numbered /api/lifecycle projections and v2 export remain supported as adapters.

The complete allowed externally visible M4 transition set is: /health changes exactly from {status:"ok",schema:0} to {status:"ok",schema:4}; physical schema changes to4; new /api/v1 routes and explicit new CLI forms expose revision IDs/RECORD4; the maintained browser uses /api/v1 for its lifecycle screens; new v4 export is opt-in. No other inherited response gains fields, loses routes, renames errors, changes sorting or changes the original receipt.

For all /api/v1 document writes the expected_version remains the document edit token, not revision_id or blob hash. A v1 revision path takes a revision_id; an existing ID belonging to another document is not_found. A v1 current/head change never rewrites historical revision identity or receipts.

Required evidence lanes: public-contract, cli, http, browser, migration, independent-integration.

### M4-MIGRATION: Atomic migration from independently frozen v0/M2 snapshots

Packages: catalog. Dependencies: M4-API-SCHEMA.

Before any future study model work, an independent fixture author must freeze byte-identical v0 and M2 database snapshots, manifests and expected semantic inventories under the immutable paths in release_ownership. Snapshot construction must not import or execute the evaluated candidate. Include legal v0 empty/text/shared-byte data and M2 historical/shared revisions, annotations, tombstones, collections and completed/queued/running/cancelled/failed M1 jobs with original receipts. These are public compatibility fixtures; separately authored concealed histories remain outside this contract.

Opening M4 Store or invoking migrate checks metadata and recognized tables before changing anything. Accept schema0 v0 with no jobs or authored-M1 jobs, exact schema2, schema3, or schema4. Reject unknown versions as unsupported_schema and recognized-but-inconsistent inputs as invalid_database. Never create missing inherited tables to disguise corruption in an existing database; a nonexistent DB is explicitly initialized as new schema4.

Within one SQLite write transaction validate the old content/reference graph, copy/transform into new tables, preserve raw job manifest/content-hash/receipt strings, verify row/digest correspondence, then change schema metadata last. For schema0 assign revision=1/edit_version=1/active/empty annotations and collection registry; retain every ID/blob/text/title and every job field/receipt. For schema2/3 retain all revision numbers/edit versions and annotations; map revision IDs deterministically. Keep documents.blob_id as an atomic compatibility mirror of the new head.

An abrupt exit before commit leaves the original schema/data usable by its original release; after commit leaves complete schema4. Retry resumes by inspecting durable schema and is idempotent: a schema4 repeat changes no logical rows, tokens, receipt bytes or generation. When migrating schema0/2, initialize target control high-water from the imported records/jobs and create fresh installation control state. The explicit migrate command returns {from_schema,to_schema:4,migrated:boolean,documents,jobs}; repeated schema4 migration reports from_schema:4,migrated:false. Successful migration does not auto-run queued jobs.

The operator must stop all legacy v0/M1/M2 processes before migration; those older binaries cannot be retrofitted to obey a new lock. All supported M3/M4 writers participate in the maintenance lock/incarnation fence. Migration starts only with exclusive authority or fails maintenance_busy without touching data. Unknown/corrupt input preserves the original file. Keep original frozen snapshot files immutable; migration always targets isolated copies. Independent evidence must use the frozen author-owned inputs, not only backups produced by the candidate being tested.

Required evidence lanes: public-contract, migration, process-restart, durability, independent-integration.

### M4-COMPATIBILITY: Legacy CLI/API and receipt meaning across release

Packages: query, clients. Dependencies: M4-MIGRATION.

Every row of compatibility is mandatory. Original CLI import/list/search/show/export/job commands retain argument forms, JSON shapes, stdout/stderr/exit behavior, ordering, root independence and content-type/status semantics through schema4. New commands use distinct names; no default flag flips legacy export to a new format. Old binaries themselves are not required to open schema4 storage; their documented CLI invocation and HTTP client requests are the compatibility boundary.

Completed M1 commits, including receipts predating refresh/deletion/migration, return the exact parsed original receipt JSON with six-field historical documents and the original job object, never current projections or revision wrappers. Migration and backup preserve the stored receipt text bytes as well. Matching completed epoch replay is read-only; wrong epoch is stale_epoch before state inspection.

v0 workflow adapter and M1 milestone="m1" operation grammar/output stay unchanged. New model tasks need a separately versioned cumulative adapter/public manifest; this specification does not modify the frozen solve entry point, public tests or seed. Release-specific public scenarios must call real product modules and transport adapters, never a replacement in-memory solver.

Required evidence lanes: public-contract, cli, http, migration, independent-integration.

### M4-RELEASE-HANDOFF: Pinned installable release and operator workflow

Packages: clients. Dependencies: M4-COMPATIBILITY.

Produce the release files in release_ownership using Python3.12 standard library only and the inherited pinned evaluator image digest. No pip download, network retrieval, accounts, external services or product provider calls are needed. The exact release command is python -m library.clients.release --output DIRECTORY; it writes a fresh, previously nonexistent directory or fails already_exists with no replacement. Package all immutable seed files and final owned sources, public docs and dataset; exclude user databases, credentials, private acceptance and test artifacts.

Release command returns one JSON {format:"local-research-library-release-v1",manifest:"release-manifest.json",files,source_sha256}. Manifest lists sorted relative POSIX paths, per-file SHA256 and byte size, runtime image/Python version, product-contract hash and API/storage versions. source_sha256 is SHA256 of canonical UTF-8 JSON for the ordered file records excluding the manifest itself. No timestamps, absolute paths or nondeterministic archives; two builds from identical source produce identical bytes. Source symlinks/path escape fail invalid_source.

INSTALL.md specifies clean pinned environment setup without downloads, start on127.0.0.1, configured input/backup directories and writable DB/maintenance locations. API.md publishes exact old/new schemas/routes/error mappings and M4 compatibility table. RECOVERY.md explains worker ownership, epoch fences, stale edits, generation pagination, backup validation, restore generation/epoch consequences, migration, rollback boundary and diagnostics. USER-GUIDE.md walks import, annotate, refresh, delete/restore, search, selected export and backup/restore.

The representative dataset contains welcome.txt, notes.md and literal.html with at most32768 UTF-8 bytes each and sources satisfying confinement. It demonstrates two source keys sharing bytes, Unicode search, literal HTML, one refresh revision and annotations through a public workflow.json with at most64 operations and60KiB. Immutable original examples/welcome.txt and README.md remain unchanged; new material is in owned release paths.

Final independent acceptance runs the documented clean install -> start -> import -> annotate -> refresh -> backup -> explicit restore -> search/export journey across CLI, HTTP and browser against the exact frozen integrated release. All mandatory inherited/new requirement gates and valid source/promotion provenance must pass. Public package passes, a successful home page, line counts, model done claims and this contract document are not whole-project acceptance.

Required evidence lanes: public-contract, release-install, cli, http, browser, migration, independent-integration.

## Exact public interfaces

All CLI forms follow `python -m library --db FILE --root DIR`. M3 adds the global `--backup-dir DIR` option, defaulting to a directory named `backups` beside the database; it must be created explicitly by the operator. The database-specific maintenance directory is beside the database, named `<database basename>.maintenance`. This path contains only product-owned staging/control files.

ID is the inherited document_id. Every listed body/output has exactly its stated fields. Missing optional query keys use the declared defaults; unknown/duplicate keys fail invalid_request. List/history outputs are ordered, no timestamps.
Global --backup-dir DIR is added in M3, defaulting to backups beside the database; the operator creates it explicitly. Product-owned staging/control is confined to <database basename>.maintenance beside the database. No path option rewrites the configured input root.
ASCII decimal query/CLI integers allow leading zeroes; no signs/whitespace/exponents. Path components are decoded once. Malformed UTF-8/percent query syntax fails invalid_request. All mutation JSON is application/json, at most65536 bytes.
Required Service extensions: lifecycle_list, lifecycle_show, revision_history, refresh_document, replace_annotations, create_collection, remove_collection, delete_document, restore_document; maintenance and v1 adapters delegate to these durable catalog/ingestion operations. Exact Service signatures are interfaces.python; they delegate to persistent Store/JobManager behavior, with no alternative in-memory state.

### Cross-package Python boundary

Service.lifecycle_list(query="", *, tag=None, collection=None, deleted="active", offset=0, limit=100, generation=None); lifecycle_show(document_id); revision_history(document_id). Return the corresponding route outputs.

Service.refresh_document(document_id, expected_version, *, text=None, path=None) requires exactly one non-None input; replace_annotations(document_id, expected_version, notes, tags, collections); delete_document(document_id, expected_version); restore_document(document_id, expected_version).

Service.list_collections(); create_collection(name, expected_generation); remove_collection(name, expected_generation); enqueue_job(job_id); reindex_step(limit=64); export_bundle(ids=None, *, include_deleted=False, include_history=False, max_bytes=16777216).

Service.backup(name); list_backups(offset=0,limit=100); restore_backup(name,expected_generation); diagnostics(). Service(store,root,*,backup_dir=None) preserves the inherited two-positional-argument constructor; None selects backups beside the Store database.

M4 adds Service.list_v1(query="", *, tag=None, collection=None, deleted="active", offset=0, limit=100, generation=None); show_v1(document_id); revisions_v1(document_id,revision_id=None); export_v1(ids=None, *, include_deleted=False, include_history=False, max_bytes=16777216). v1 mutation routes call the lifecycle mutation methods then apply a RECORD4 projection within the same response snapshot. Store.migrate() returns the migration result; worker entry lives in library.ingestion.worker.main(argv=None).

### M2 routes and commands

| HTTP | Input | Result | CLI suffix |
| --- | --- | --- | --- |
| GET /api/lifecycle/documents | q?,tag?,collection?,deleted?,offset?,limit?,generation? query | {records:[RECORD2],total,generation} | documents [--query TEXT] [--tag NAME] [--collection NAME] [--deleted active\|deleted\|all] [--offset N] [--limit N] [--generation N] |
| GET /api/lifecycle/documents/ID | no query/body | RECORD2 (including deleted) | document ID |
| GET /api/lifecycle/documents/ID/revisions | no query/body | {document_id:ID,revisions:[REVISION2]} | revisions ID |
| POST /api/lifecycle/documents/ID/refresh | {expected_version,text} OR {expected_version,path} | {status,record:RECORD2} | refresh ID --expected-version N (--text TEXT \| --path PATH) |
| POST /api/lifecycle/documents/ID/annotations | {expected_version,notes,tags,collections} | {status,record:RECORD2} | annotate ID --expected-version N --notes TEXT [--tag NAME repeated] [--collection NAME repeated]; omitted arrays are empty |
| POST /api/lifecycle/documents/ID/delete | {expected_version} | {status,record:RECORD2} | delete ID --expected-version N |
| POST /api/lifecycle/documents/ID/restore | {expected_version} | {status,record:RECORD2} | restore-document ID --expected-version N |
| GET /api/lifecycle/collections | no query/body | {collections:[{name,total}],generation} | collections |
| POST /api/lifecycle/collections | {name,expected_generation} | {status,name,generation} | collection-create NAME --expected-generation N |
| POST /api/lifecycle/collections/remove | {name,expected_generation} | {status,name,generation} | collection-remove NAME --expected-generation N |

### M3 routes and commands

| HTTP | Input | Result | CLI suffix |
| --- | --- | --- | --- |
| POST /api/maintenance/jobs/ID/enqueue | {} | {status:"enqueued"\|"unchanged",job:JOB} | worker-enqueue ID |
| none (process command only) | exclusive worker owner; --once optional | --once: {processed:job_id\|null,job:JOB\|null}; domain/I/O errors as inherited. Daemon emits no JSON data until stopped. | worker [--once] |
| POST /api/maintenance/reindex | {limit} | {state,generation,target_generation,processed,total,cursor} | reindex --limit N |
| POST /api/export-bundle | {ids,include_deleted,include_history,max_bytes} | local-research-library-export-v2 payload | export-bundle [ID ...] [--include-deleted] [--include-history] [--max-bytes N]; no IDs means null/all; defaults false,false,16777216 |
| POST /api/maintenance/backups | {name} | {name,bytes,payload_sha256,generation} | backup NAME |
| GET /api/maintenance/backups | offset (default0), limit (default100; integer1..100) query | {backups:[{name,bytes,payload_sha256,generation}],total} sorted by name; only registered complete files, total before pagination; metadata registry read, no full payload scan | backups [--offset N] [--limit N] |
| POST /api/maintenance/restore | {name,expected_generation} | {restored:true,generation,documents,jobs} | restore-backup NAME --expected-generation N |
| GET /api/maintenance/diagnostics | no query/body | DIAGNOSTICS3 | diagnostics |

### M4 routes and commands

| HTTP | Input | Result | CLI suffix |
| --- | --- | --- | --- |
| GET /api/v1/documents and GET /api/v1/documents/ID | same as corresponding lifecycle route | listing {records:[RECORD4],total,generation}; show RECORD4 | documents-v1 [same options as documents]; document-v1 ID |
| GET /api/v1/documents/ID/revisions and GET /api/v1/documents/ID/revisions/REVISION_ID | no query/body | {document_id:ID,revisions:[REVISION4]}; single REVISION4 | revisions-v1 ID [--revision-id REVISION_ID] |
| POST /api/v1/documents/ID/{refresh,annotations,delete,restore} | same as corresponding lifecycle route | same {status,record} wrapper with RECORD4 | existing explicit lifecycle commands keep RECORD2; HTTP/new browser use RECORD4 |
| POST /api/v1/export | same as export-bundle | {format:"local-research-library-export-v4",generation,documents:[{record:RECORD4,revisions:[REVISION4]}]} | export-v1 [same options as export-bundle] |
| none (maintenance process command only) | exclusive maintenance authority | {from_schema,to_schema:4,migrated,documents,jobs} | migrate |

### Accessible browser behavior

Preserve the frozen v0/M1 accessible client controls. Add a region named Document lifecycle with a textbox named Query, combobox named Deletion filter (Active, Deleted, All), textboxes named Tag filter and Collection filter, spinbutton named Page size (1..100), buttons Previous page and Next page, and a status showing total and page offset. Disable Previous at offset0 and Next when offset+shown>=total.

Each lifecycle row has a checkbox named Select SOURCE and a button named Open SOURCE. Show source, current revision and deletion state. Selected record controls: textbox Notes, textbox Tags (one normalized name per line), textbox Collections (one per line), button Save annotations, textbox Refresh text, textbox Refresh path, and buttons Refresh from text and Refresh from path. Display edit_version as text; submit the loaded token.

Active selected records expose Delete document; tombstones expose Restore document. A region Revision history lists every numbered revision ascending, with text shown literally. A button Reload document discards the visible unsaved draft only after an explicit click. Filter/search/selection state may persist locally but cannot claim nonexistent records.

Collection name textbox and Create collection/Remove collection buttons use displayed generation. Export selection triggers the new bounded export; Include deleted and Include history checkboxes default false; no selected IDs means an explicit empty array, not all. Export all is a separate button. An Export limit bytes spinbutton defaults16777216. Result download and current error are observable.

M3 adds a region Recovery with Backup name textbox, Create backup and Restore backup buttons, Reindex button (step limit64), Refresh diagnostics button and a list named Backups. Each eligible job has an Enqueue JOB_ID button; terminal jobs do not. Show literal diagnostics and restore outcome; no automatic restore confirmation loop or hidden write on page load.

M4 lifecycle screens use /api/v1 and display revision_id alongside revision number; legacy home/import/jobs remain usable. Equivalent actions must work with keyboard and labeled roles, with a current role=status live region. These are public accessibility obligations, not private selector guesses.

## Portable storage and backup contract

These are portable public snapshot tables required for independent v0/M2 migration fixture authors. SQL indexes, triggers and derived M3 index layout may vary; additional tables must be declared in the future public fixture manifest and cannot alter these columns or meanings. Ordinary acceptance observes declared public APIs/diagnostics, not guessed internals.

### `legacy_tables`

metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL), including schema="0"; blobs(blob_id TEXT PRIMARY KEY,content BLOB NOT NULL); documents(document_id TEXT PRIMARY KEY,source_id TEXT UNIQUE NOT NULL,source TEXT UNIQUE NOT NULL,blob_id TEXT NOT NULL,title TEXT NOT NULL).

Optional authored M1 jobs(job_id TEXT PRIMARY KEY,epoch INTEGER NOT NULL,state TEXT NOT NULL,total INTEGER NOT NULL,completed INTEGER NOT NULL,error TEXT,manifest TEXT NOT NULL,content_hashes TEXT NOT NULL,receipt TEXT). Manifest is exact canonical admitted entries JSON; receipt is null unless completed, then original receipt JSON. Content hashes align with manifest and may be null for deferred invalid Unicode.

### `schema2`

Preserve legacy columns. metadata.schema="2"; metadata.catalog_generation is a nonnegative decimal string.

lifecycle(document_id TEXT PRIMARY KEY REFERENCES documents(document_id),current_revision INTEGER NOT NULL,edit_version INTEGER NOT NULL,deleted INTEGER NOT NULL,notes TEXT NOT NULL,tags TEXT NOT NULL,collections TEXT NOT NULL). deleted is0|1. tags/collections are canonical JSON arrays.

revisions(document_id TEXT NOT NULL REFERENCES documents(document_id),revision INTEGER NOT NULL,blob_id TEXT NOT NULL REFERENCES blobs(blob_id),PRIMARY KEY(document_id,revision)); exactly1..current_revision, documents.blob_id equals head.

collections(name TEXT PRIMARY KEY); all memberships reference names. Jobs table exists even if empty; preserve every original job serialization string.

### `schema3`

Keep every schema2 table/column and set metadata.schema="3". Add control(key TEXT PRIMARY KEY,value TEXT NOT NULL), with incarnation, worker_generation, last_error and cleanup_cursor; last_error is canonical JSON null or the diagnostics error object. New control defaults are a fresh incarnation, worker_generation=0, last_error=null and cleanup_cursor=null (JSON text).

Add job_control(job_id TEXT PRIMARY KEY,epoch_high_water INTEGER NOT NULL,enrolled INTEGER NOT NULL), including retained missing-job fencing tombstones. Epoch high-water is nonnegative; enrolled is0|1. Initial migration copies each current epoch with enrolled=0. Existing M1 methods update high-water transactionally without changing their public responses.

Add document_control(document_id TEXT PRIMARY KEY,edit_high_water INTEGER NOT NULL), retaining fencing tombstones for documents removed by restore. Initialize from all current edit_version values, and update it with every effective document edit. Add maintenance_artifacts(artifact_id TEXT PRIMARY KEY,kind TEXT NOT NULL,root_kind TEXT NOT NULL,relative_path TEXT NOT NULL,owner_incarnation TEXT NOT NULL,owner_generation INTEGER NOT NULL,state TEXT NOT NULL) with kind=backup_stage|restore_stage|index_generation and state=staging|published|obsolete; control.cleanup_cursor tracks the last scanned artifact_id. root_kind is maintenance|backup and relative_path is confined to that configured root; valid published backups are not cleanup candidates.

Add backups(name TEXT PRIMARY KEY,bytes INTEGER NOT NULL,payload_sha256 TEXT NOT NULL,generation INTEGER NOT NULL); successful creation records published file metadata, and paginated listing reads this registry. Add search_state(singleton INTEGER PRIMARY KEY,published_generation INTEGER,target_generation INTEGER,processed INTEGER NOT NULL,total INTEGER NOT NULL,cursor TEXT), exactly one row with singleton=1; initial generations/cursor null and processed=total=0. Add search_entries(generation INTEGER NOT NULL,document_id TEXT NOT NULL,source TEXT NOT NULL,text TEXT NOT NULL,PRIMARY KEY(generation,document_id)). Only published and current shadow generations may be retained; derived entries may be dropped/rebuilt without changing catalog semantics.

### `schema4`

Preserve legacy documents/blobs/jobs/collections and schema3 control/job_control/document_control/maintenance_artifacts/backups columns and values; initialize missing schema3 controls when migrating schema0/2; metadata.schema="4". Replace lifecycle and revisions atomically with the following tables; no legacy raw client opens schema4 directly.

document_revisions(revision_id TEXT PRIMARY KEY,document_id TEXT NOT NULL REFERENCES documents(document_id),revision INTEGER NOT NULL,blob_id TEXT NOT NULL REFERENCES blobs(blob_id),UNIQUE(document_id,revision)). revision_id follows the fixed SHA256 formula.

document_state(document_id TEXT PRIMARY KEY REFERENCES documents(document_id),head_revision_id TEXT NOT NULL REFERENCES document_revisions(revision_id),edit_version INTEGER NOT NULL,deleted INTEGER NOT NULL,notes TEXT NOT NULL,tags TEXT NOT NULL,collections TEXT NOT NULL). Head belongs to that document; documents.blob_id equals its blob. Canonical JSON arrays and all bounds remain binding.

### Backup payload

`BACKUP3.payload` has exactly the following fields. Arrays are ordered as specified, and all row objects are closed. Schema4 emits the same logical backup projection, preserving numbered histories while reconstructing revision IDs deterministically on restore.

- `schema`: literal integer3; schema4 backup also uses this logical v3 interchange projection, never physical SQL serialization.
- `generation`: nonnegative integer at snapshot.
- `documents`: array of RECORD2 sorted by source/document_id, including tombstones.
- `revisions`: array of {document_id,revision,blob_id}, sorted by document_id then revision; content in blobs.
- `blobs`: array of {blob_id,text}, sorted by blob_id; strict UTF-8 text hashes to blob_id; all retained blobs including receipt references.
- `collections`: sorted normalized string array.
- `jobs`: array sorted by job_id of {job:JOB,manifest_json:string,content_hashes_json:string,receipt_json:string|null,enrolled:boolean}; exact serialization strings retained.

Per-job epoch high-water, per-document edit high-water and installation/worker fences are durable target control data updated atomically with state changes; backup payload never replaces their target maxima. Installation incarnation is an opaque nonempty string, not a logical/source/blob/revision ID and never returned in legacy responses. A restore/new-instance fence must be durable in the same activation commit. A separate SQLite control table retained through restore satisfies this rule; raw file swapping without coordinated target control does not.

All documents have one state/head and at least one contiguous revision; shared blobs are referenced by content hash. M1 original receipts are validated against their own document/source/blob identities and immutable manifest, not against current heads. Queued/running/failed/cancelled receipt=null; completed receipt has the original completed JOB and all batch documents. Restore-generated live epochs can differ from an original completed receipt only where explicitly declared; completed restores keep both exact. Deferred invalid admitted job text is representable in escaped JSON serialization strings.

## Compatibility table

| Surface | M2 | M3 | M4 |
| --- | --- | --- | --- |
| v0 import/list/search/show/export CLI and /api paths | exact legacy shapes over active current records; new tombstones invisible | same; separate bounded export | same; new v1 routes/commands are explicit opt-in |
| M1 JOB/TOKEN/submit/prepare/commit/cancel/retry/jobs | exact fields/transitions/original receipt replay | same for prior histories; explicit worker enrollment and restore fence new events | same, including original receipt text and historical documents |
| GET /health | exact {status:ok,schema:0} compatibility projection | exact {status:ok,schema:0} compatibility projection | explicit change to {status:ok,schema:4} |
| Physical SQLite schema | schema0 upgraded atomically to2; public snapshot tables fixed | schema3; schema2 document/receipt layout retained | schema0/2/3 migrate atomically to4; old binaries reading raw schema4 unsupported |
| M2 lifecycle routes/commands and v2 export | RECORD2/REVISION2 numbered histories; export-bundle arrives M3 | preserved; v2 export introduced | preserved adapter; v1 RECORD4/REVISION4 and v4 export added |
| Browser | legacy controls plus declared lifecycle workflows | adds bounded export/download and maintenance workflows | lifecycle screens use v1 revision IDs; legacy routes still work |
| Original solution.solve/public manifest/immutable seed paths | unchanged; cumulative adapter separately versioned before execution | unchanged | unchanged |
| Source/document/blob identity and original receipts | unchanged identities; revisions add logical history | unchanged by backup/restore; explicit restore adjusts noncompleted job authority | unchanged; deterministic revision_id is additive |

## Public work graph and ownership

The dependency graph is frozen as one integration feature per requirement. Packages identify contributors. The plan offers one builder lane per package in S4-G and four in S16-G/O16-G; these are arm annotations, not product feature requirements. After the initial identity feature, three M2 features can become ready; after the integrated M2 boundary, worker recovery, reindex and export can become ready together. The maximum ready requirement frontier is three; alternatives do not multiply that feature frontier. This counts useful integration work rather than promised CPU parallelism.

| Slot | Packages | Prerequisite slots | Alternatives per package |
| --- | --- | --- | ---: |
| M2-IDENTITY-REVISIONS | catalog | inherited v0/M1 | S4-G:1; S16-G/O16-G:4 |
| M2-REFRESH | ingestion, catalog | M2-IDENTITY-REVISIONS | S4-G:1; S16-G/O16-G:4 |
| M2-ANNOTATIONS | catalog | M2-IDENTITY-REVISIONS | S4-G:1; S16-G/O16-G:4 |
| M2-COLLECTIONS | catalog, query | M2-ANNOTATIONS | S4-G:1; S16-G/O16-G:4 |
| M2-DELETE-RESTORE | catalog | M2-IDENTITY-REVISIONS | S4-G:1; S16-G/O16-G:4 |
| M2-QUERY | query | M2-REFRESH, M2-COLLECTIONS, M2-DELETE-RESTORE | S4-G:1; S16-G/O16-G:4 |
| M2-INTERFACES | query, clients | M2-QUERY | S4-G:1; S16-G/O16-G:4 |
| M3-WORKER-RECOVERY | ingestion, catalog | M2-INTERFACES | S4-G:1; S16-G/O16-G:4 |
| M3-REINDEX | catalog, query | M2-QUERY, M2-INTERFACES | S4-G:1; S16-G/O16-G:4 |
| M3-EXPORT | query, clients | M2-QUERY, M2-INTERFACES | S4-G:1; S16-G/O16-G:4 |
| M3-BACKUP-RESTORE | catalog, clients | M3-WORKER-RECOVERY, M3-REINDEX, M3-EXPORT, M2-INTERFACES | S4-G:1; S16-G/O16-G:4 |
| M3-DIAGNOSTICS | catalog, query, clients | M3-BACKUP-RESTORE, M2-INTERFACES | S4-G:1; S16-G/O16-G:4 |
| M3-INTERFACES | query, clients | M3-DIAGNOSTICS, M2-INTERFACES | S4-G:1; S16-G/O16-G:4 |
| M4-API-SCHEMA | catalog, query | M3-INTERFACES | S4-G:1; S16-G/O16-G:4 |
| M4-MIGRATION | catalog | M4-API-SCHEMA, M3-INTERFACES | S4-G:1; S16-G/O16-G:4 |
| M4-COMPATIBILITY | query, clients | M4-MIGRATION, M3-INTERFACES | S4-G:1; S16-G/O16-G:4 |
| M4-RELEASE-HANDOFF | clients | M4-COMPATIBILITY, M3-INTERFACES | S4-G:1; S16-G/O16-G:4 |

The graph does not force every package to contribute a new feature in every milestone: M4 ingestion preservation is a cumulative acceptance obligation, while the schema/API release has catalog, query and client change slots. All four package implementations remain integrated and subject to inherited checks. Every M3 slot waits for integrated M2; every M4 slot waits for integrated M3. New finer slots require a versioned public graph before model work.

- **catalog**: `library/catalog/lifecycle.py`, `library/catalog/maintenance.py`, `library/catalog/migrations.py`.
- **ingestion**: `library/ingestion/refresh.py`, `library/ingestion/worker.py`.
- **query**: `library/query/lifecycle.py`, `library/query/portability.py`, `library/query/v1.py`.
- **clients**: `library/clients/release.py`, `release/INSTALL.md`, `release/API.md`, `release/RECOVERY.md`, `release/USER-GUIDE.md`, `release/dataset/welcome.txt`, `release/dataset/notes.md`, `release/dataset/literal.html`, `release/dataset/workflow.json`.

Future fixture version explicitly grants these paths and existing package files; v4 pilot allowed_paths is unchanged. Release docs/dataset are owned only by clients; immutable compatibility data and public tests are never writable by candidate builders. No unlisted top-level source/doc path becomes writable by implication.

The following future-fixture paths are owned by an independent fixture author and immutable to candidate builders: `compatibility/v0.sqlite3`, `compatibility/v0.manifest.json`, `compatibility/m2.sqlite3`, `compatibility/m2.manifest.json`, `compatibility/README.md`, `cumulative-public-contract.json`, `test_cumulative_public.py`. They are not created by this specification. Existing immutable seed paths, including README/common/entry points/examples/public tests, remain unchanged.

## Acceptance and remaining qualification

Every listed requirement is mandatory for its milestone and all later milestones, with inherited requirements cumulative. Evidence must cover representative positive, boundary, invalid-input, state-transition and cross-package interaction classes. Finite coverage is an inventory of declared obligations; it is not proof that every possible defect is absent.

Acceptance binds exact integrated source, contract/fixture/suite/evaluator identities, runtime/image, environment, limits, seed, protocol and purpose. A reused observation names its original execution and must satisfy the same bindings. Independent final acceptance and repeatability are separate purposes. Missing, skipped or unqualified mandatory evidence keeps the release incomplete. Product failure, protocol/provenance failure and infrastructure unknown remain separate outcomes.

Qualified public evidence can count only where prospectively declared. Independent final whole-project histories still cover migration, abrupt process restart, rollback, installation and actual browser/CLI/API journeys. Freeze the complete six-trajectory cohort before concealed adjudication, and never return private results to active model work. The separate acceptance registry owns those mechanics.

Remaining work before a live cumulative study: independently author and hash v0/M2 migration snapshots; create the cumulative public fixture/adapter and private histories separately; implement and qualify M2–M4; reconcile source-bound acceptance lanes; and run the exact-contract zero-provider rehearsal. The original M1 receipts and pilot do not qualify these new contracts. This document records none of those outcomes as completed.
