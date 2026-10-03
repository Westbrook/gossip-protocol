# Public migration compatibility snapshots, version 1

These are immutable public inputs for `M4-MIGRATION` in
`library-cumulative-product-v1.json` (SHA-256
`3353dbaa0c2474527cbffb0d87a4091b5f77ed3f98418fff2f216d90b6a8487c`).
They are available to every study builder before implementation. They are not
concealed acceptance histories and do not establish statistical superiority.

Copy each `.sqlite3` file byte for byte to a fresh, isolated destination before
opening the application. Never run an application, migration, SQL mutation,
checkpoint, `VACUUM`, or writable SQLite connection against these originals.
Each manifest records the original SHA-256 and length, exact table layout,
independently authored history, expected semantic inventory and expected first
and repeated M4 migration responses. The inventory's `records2` and `records4`
include tombstones; `active_documents` is the inherited six-field projection.
Job wrappers preserve the literal `manifest_json`, `content_hashes_json` and
`receipt_json` strings. Compare these strings without parsing and reserializing
them. A changed document head is not a reason to alter its completed receipt.

## Inputs and provenance

- `v0.sqlite3`: schema 0 with five documents, no jobs table, one empty document,
  exact composed/decomposed Unicode, non-ASCII source keys and shared bytes under
  two independent source identities. Literal markup is stored in a valid `.md`
  source. M4 migration supplies initial lifecycle state without changing bytes.
- `m2.sqlite3`: schema 2, generation 11, five documents and eight revisions. It
  contains shared historical blobs, content ABA, an annotated tombstone,
  normalized names and collection membership. Seven persisted jobs cover every
  state, an empty completed batch and a queued deferred-invalid entry. The
  completed multi-document job has epoch 3 and its original receipt, including a
  document whose head later changed and was deleted. Pending jobs must not run
  as a side effect of migration.

`build_snapshots.py` is an independent authoring recipe, not application code.
It imports only Python standard-library modules and constructs explicit SQL
rows from authored semantic records. It never imports, runs or uses output
from an evaluated candidate or any M4 implementation. The manifests bind the
constructor source SHA-256 and Python/SQLite authoring runtime. There are no
additional snapshot tables. SQLite internal indexes are not product tables.

The committed SQLite bytes are authoritative. The recipe requires an existing,
empty output directory and refuses to overwrite any file. Rebuilding is useful
for auditing author intent; a reconstruction under another SQLite version is
not a replacement for the frozen input and need not have identical file bytes.
A new fixture version must be frozen before a study uses it. Runtime fixture
loading verifies hashes and never regenerates files.

## Bounded corruption derivatives

The harness exposes `corruption_recipes()` alongside these files. For each
recipe, start from a new copy of the named input, retain its initial SHA-256,
execute only the listed one or two SQL statements with SQLite foreign-key
checking disabled for intentional corruption, then close SQLite and record the
resulting SHA-256. Invoke migration on that copy. Unknown versions must produce
`unsupported_schema`; recognized but inconsistent inputs must produce
`invalid_database`. Record the complete bytes before and after the attempted
migration to prove that rejection preserved the corrupt input. Never mutate an
original, rerun a recipe against an already used copy, or silently replace a
failed artifact.

Recipes exercise an unknown schema version, missing inherited table, corrupt
blob digest, missing revision, inconsistent head mirror, invalid edit token,
non-normalized tags, unknown collection membership, and a completed job missing
its original receipt. They are public diagnostics, not comprehensive concealed
acceptance. Independent fixtures and real process interruption are additional
requirements. The schema-0 fixture intentionally omits the optional jobs table;
authoring the permitted M1 table on a separate copy is a distinct public history,
not permission to add arbitrary tables to a snapshot.

## Release paths

The public application workspace receives these exact five immutable paths:
`compatibility/v0.sqlite3`, `compatibility/v0.manifest.json`,
`compatibility/m2.sqlite3`, `compatibility/m2.manifest.json`, and
`compatibility/README.md`. Binary files must be injected as bytes, never decoded
as UTF-8 source. The cumulative public contract and public test runner are owned
and frozen separately; neither is part of these five files.
