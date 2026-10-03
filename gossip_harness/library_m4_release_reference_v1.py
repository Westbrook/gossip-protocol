"""Trusted authored deterministic M4 release and operator documentation.

This is a release implementation, never candidate evidence or private acceptance.
The allowlist contains the full supplied application tree and independent public
fixture assets. Frozen solve/public seed files are copied without modification.
"""
from __future__ import annotations

from collections.abc import Sequence
from pathlib import PurePosixPath

from .library_project_fixture_v1 import IMMUTABLE_PATHS, RUNTIME_IMAGE

PRODUCT_CONTRACT_SHA256 = "3353dbaa0c2474527cbffb0d87a4091b5f77ed3f98418fff2f216d90b6a8487c"
PUBLIC_FIXTURE_PATHS = (
    "compatibility/v0.sqlite3", "compatibility/v0.manifest.json",
    "compatibility/m2.sqlite3", "compatibility/m2.manifest.json",
    "compatibility/README.md", "cumulative-public-contract.json",
    "test_cumulative_public.py",
)

_RELEASE = r'''"""Deterministic, allowlisted, no-clobber local release builder."""
import argparse
import ctypes
import errno
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import secrets
import shutil
import stat
import sys
from library.common import LibraryError

SOURCE_PATHS = __SOURCE_PATHS__
RUNTIME_IMAGE = __RUNTIME_IMAGE__
PRODUCT_CONTRACT_SHA256 = __PRODUCT_CONTRACT_SHA256__


def canonical_bytes(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), allow_nan=False).encode('utf-8')


def _absolute(path):
    # abspath/resolve erase a symlink followed by '..' before it can be checked.
    # Joining cwd preserves those lexical components for descriptor traversal.
    path = Path(os.fspath(path))
    return path if path.is_absolute() else Path.cwd() / path


def _directory(path):
    """Open each lexical ancestor without following a symlink, including root."""
    path = _absolute(path)
    handle = os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for part in path.parts[1:]:
            following = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                dir_fd=handle)
            os.close(handle)
            handle = following
        return handle
    except OSError as error:
        os.close(handle)
        raise LibraryError('invalid_source') from error


def _read(root_fd, relative):
    parts = PurePosixPath(relative).parts
    if (not parts or str(PurePosixPath(relative)) != relative
            or any(part in ('.', '..') for part in parts) or relative.startswith('/')
            or '\\' in relative or '\0' in relative):
        raise LibraryError('invalid_source')
    handle = os.dup(root_fd)
    try:
        for part in parts[:-1]:
            following = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                dir_fd=handle)
            os.close(handle)
            handle = following
        source = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                         dir_fd=handle)
        try:
            before = os.fstat(source)
            if not stat.S_ISREG(before.st_mode):
                raise LibraryError('invalid_source')
            chunks = []
            while True:
                chunk = os.read(source, 1048576)
                if not chunk:
                    break
                chunks.append(chunk)
            after = os.fstat(source)
            if (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
                    after.st_size, after.st_mtime_ns, after.st_ctime_ns):
                raise LibraryError('invalid_source')
            return b''.join(chunks)
        finally:
            os.close(source)
    except OSError as error:
        raise LibraryError('invalid_source') from error
    finally:
        os.close(handle)


def _write(stage_fd, relative, raw):
    parts = PurePosixPath(relative).parts
    handle = os.dup(stage_fd)
    try:
        for part in parts[:-1]:
            try:
                os.mkdir(part, 0o755, dir_fd=handle)
            except FileExistsError:
                pass
            following = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                dir_fd=handle)
            os.close(handle)
            handle = following
        output = os.open(parts[-1], os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                         0o644, dir_fd=handle)
        try:
            remaining = memoryview(raw)
            while remaining:
                remaining = remaining[os.write(output, remaining):]
            os.fsync(output)
        finally:
            os.close(output)
        os.fsync(handle)
    finally:
        os.close(handle)


def _publish(parent_fd, stage_name, final_name):
    """Atomic no-replacement directory rename on pinned Linux and host macOS.

    Plain os.rename can replace an empty destination. The standard-library
    ctypes binding uses the OS's exclusive primitive instead; no unsafe fallback.
    """
    library = ctypes.CDLL(None, use_errno=True)
    if sys.platform == 'linux':
        rename = getattr(library, 'renameat2', None)
        flag = 1  # RENAME_NOREPLACE
    elif sys.platform == 'darwin':
        rename = getattr(library, 'renameatx_np', None)
        flag = 4  # RENAME_EXCL
    else:
        rename = None
        flag = 0
    if rename is None:
        raise LibraryError('io_error')
    rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int,
                       ctypes.c_char_p, ctypes.c_uint]
    rename.restype = ctypes.c_int
    if rename(parent_fd, os.fsencode(stage_name), parent_fd,
              os.fsencode(final_name), flag):
        code = ctypes.get_errno()
        if code in (errno.EEXIST, errno.ENOTEMPTY):
            raise LibraryError('already_exists')
        raise OSError(code, os.strerror(code))


def build_release(output):
    destination = _absolute(output)
    if not destination.name or destination.name in ('.', '..'):
        raise LibraryError('invalid_source')
    parent_fd = _directory(destination.parent)
    stage_name = None
    try:
        try:
            os.stat(destination.name, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            raise LibraryError('already_exists')
        # Read only declared inputs. Unlisted user data is never enumerated.
        source_fd = _directory(_absolute(__file__).parents[2])
        try:
            files = [(name, _read(source_fd, name)) for name in SOURCE_PATHS]
        finally:
            os.close(source_fd)
        records = [{'path': name, 'sha256': hashlib.sha256(raw).hexdigest(),
                    'bytes': len(raw)} for name, raw in files]
        source_sha256 = hashlib.sha256(canonical_bytes(records)).hexdigest()
        manifest = {
            'format': 'local-research-library-release-manifest-v1',
            'files': records, 'source_sha256': source_sha256,
            'runtime': {'image': RUNTIME_IMAGE, 'python': '3.12'},
            'product_contract_sha256': PRODUCT_CONTRACT_SHA256,
            'api_versions': ['v0', 'lifecycle-v2', 'maintenance-v3', 'v1'],
            'storage_version': 4,
        }
        while True:
            candidate = '.library-release-' + secrets.token_hex(16)
            try:
                os.mkdir(candidate, 0o700, dir_fd=parent_fd)
            except FileExistsError:
                continue
            stage_name = candidate
            break
        stage_fd = os.open(stage_name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                           dir_fd=parent_fd)
        try:
            for relative, raw in files:
                _write(stage_fd, relative, raw)
            _write(stage_fd, 'release-manifest.json', canonical_bytes(manifest))
            os.fsync(stage_fd)
        finally:
            os.close(stage_fd)
        _publish(parent_fd, stage_name, destination.name)
        stage_name = None
        os.fsync(parent_fd)
        return {'format': 'local-research-library-release-v1',
                'manifest': 'release-manifest.json', 'files': len(records),
                'source_sha256': source_sha256}
    finally:
        if stage_name is not None:
            shutil.rmtree(stage_name, dir_fd=parent_fd)
        os.close(parent_fd)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True)
    args = parser.parse_args(argv)
    try:
        result = build_release(args.output)
    except (LibraryError, OSError, ValueError) as error:
        print(json.dumps({'error': getattr(error, 'code', 'io_error')}), file=sys.stderr)
        return 2
    print(canonical_bytes(result).decode('utf-8'))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
'''

_ASSETS = {
    'release/INSTALL.md': r'''# Install and run the local research library

This release needs Python 3.12 and its standard library. No package installation,
network retrieval, provider account, or API key is needed. The release manifest
pins the qualification image to:

`sha256:f77ac9e44ae96ef2c90b8053ea08c31f8be030f824196b0ae4db6d462c84e51f`

First check that this exact image is already installed; do not substitute a tag
or download another image. Build from the source release root with:

```sh
python -m library.clients.release --output ../library-release
```

The destination must not exist. Existing files, directories and symlinks fail
`already_exists` without replacement. Source symlinks and missing inputs fail
`invalid_source`. The builder reads a compiled explicit allowlist, not the current
working directory. It includes public compatibility snapshots, public regression
fixtures and release data; it excludes user databases, credentials and private
acceptance artifacts. Two builds of unchanged inputs have identical file bytes
and manifest bytes. Verify the sorted per-file SHA256/byte records against
`release-manifest.json`; its source digest hashes canonical JSON of those records.

A clean pinned check, with the exact image already present, is:

```sh
docker image inspect sha256:f77ac9e44ae96ef2c90b8053ea08c31f8be030f824196b0ae4db6d462c84e51f
cd ../library-release
docker run --pull=never --rm --network none --read-only --tmpfs /tmp:rw,nosuid,nodev,size=128m -v "$PWD:/app:ro" -w /app sha256:f77ac9e44ae96ef2c90b8053ea08c31f8be030f824196b0ae4db6d462c84e51f python test_cumulative_public.py
```

The public checks exercise real product modules on isolated copies of the frozen
compatibility databases and the versioned release workflow; they never migrate
the originals in `compatibility/`. They are public regression evidence, not
independent held-out experiment acceptance.

For an interactive local install, use the already installed Python 3.12, remain
in this release root, and create writable state outside the release:

```sh
mkdir -p ../library-state/inputs ../library-state/backups
cp release/dataset/welcome.txt release/dataset/notes.md release/dataset/literal.html ../library-state/inputs/
python -m library --db ../library-state/library.sqlite3 --root ../library-state/inputs --backup-dir ../library-state/backups serve --port 8765
```

Open `http://127.0.0.1:8765/`. The server binds only `127.0.0.1`. The explicitly
configured input root is independent of the database and backup directory.
The database parent must also permit creating `library.sqlite3.maintenance` and
SQLite journal/WAL files. Create the backup directory yourself; the product does
not turn a misspelled backup path into a new directory. Inputs are confined regular
UTF-8 files. Never use a user home directory as the input root.

On a Linux host, the same already-installed pinned image can serve the interactive
journey with `--network host`, a read-only `/app` mount of this release, and a
writable `/state` mount of `../library-state`; run the same Python command with
`--db /state/library.sqlite3 --root /state/inputs --backup-dir /state/backups`.
Host networking keeps the server's loopback binding visible on the Linux host.
The offline public check above uses no network. On other hosts, use installed
Python 3.12 for local browsing and retain the pinned container public check as a
separate qualification lane.

Stop legacy v0/M1/M2 processes before migration. Read RECOVERY.md before pointing
this release at an existing database. USER-GUIDE.md gives the practical workflow;
API.md lists inherited and versioned interfaces and compatibility boundaries.
''',
    'release/API.md': r'''# API and compatibility contract

All objects below are closed unless explicitly stated.

## Value schemas

```json
{
  "BACKUP3": {
    "format": "literal local-research-library-backup-v3",
    "payload": "closed object of the arrays/fields in persistence.backup_rows",
    "payload_sha256": "64 lowercase hex characters"
  },
  "DIAGNOSTICS3": {
    "blobs": "{count:nonnegative integer,bytes:nonnegative integer}",
    "documents": "{active:nonnegative integer,deleted:nonnegative integer}",
    "generation": "nonnegative integer",
    "index_state": "absent|stale|building|current",
    "jobs": "{queued:integer,running:integer,completed:integer,cancelled:integer,failed:integer}; all >=0",
    "last_error": "null or {operation,code}",
    "recovery_action": "none|restart_worker|retry_reindex|choose_new_backup|validate_backup|run_migration",
    "revisions": "nonnegative integer",
    "schema": "integer 3 or4",
    "worker_generation": "nonnegative integer",
    "worker_state": "idle|running|stopped"
  },
  "DOCUMENT0": {
    "blob_id": "current blob identity string",
    "document_id": "document identity string",
    "source": "canonical inherited source key",
    "source_id": "source identity string",
    "text": "exact decoded current UTF-8",
    "title": "last source segment"
  },
  "RECORD2": {
    "collections": "sorted normalized existing-name array",
    "deleted": "boolean",
    "document": "DOCUMENT0",
    "edit_version": "integer >=1",
    "notes": "bounded string",
    "revision": "integer 1..16",
    "tags": "sorted normalized string array"
  },
  "RECORD4": {
    "collections": "sorted normalized existing-name array",
    "current_revision": "REVISION4",
    "deleted": "boolean",
    "document_id": "unchanged logical ID",
    "edit_version": "integer >=1",
    "notes": "bounded string",
    "source": "unchanged source key",
    "source_id": "unchanged source ID",
    "tags": "sorted normalized string array",
    "title": "unchanged title"
  },
  "REVISION2": {
    "blob_id": "blob identity string",
    "revision": "integer 1..16",
    "text": "exact decoded UTF-8"
  },
  "REVISION4": {
    "blob_id": "unchanged blob identity",
    "revision": "integer 1..16",
    "revision_id": "deterministic rev- SHA256 identity",
    "text": "exact decoded UTF-8"
  }
}
```

## Frozen v0 contract

Local Research Library v0 compatibility contract
Runtime: Python 3.12 standard library only, pinned evaluator image below. No
network retrieval, accounts, PDF/OCR, external providers or product model calls.
One SQLite database and an explicitly configured local input root. All paths are
relative POSIX source keys: 1..256 UTF-8 bytes, at most 128 UTF-8 bytes per segment,
no backslash, NUL, empty, '.' or '..' segment. Absolute paths and symlinks
(including parent symlinks) are rejected.
V0 imports regular .txt and .md files, at most 32768 raw bytes each, strict UTF-8,
preserving exact decoded text and newlines. Empty text is valid. At most 256
documents may be stored. Errors leave the catalog and blobs unchanged.

Source ID = 'src-' + sha256(b'source\0' + source.encode('utf-8')).hexdigest().
Document ID = 'doc-' + sha256(b'document\0' + source.encode('utf-8')).hexdigest().
Blob ID = 'blob-' + sha256(raw UTF-8 bytes).hexdigest(). IDs persist across process
restart and never depend on the root's absolute path. Different source keys
produce different documents even when bytes match; their blob ID then matches.
Reimporting the same source and bytes returns the original document, status
'unchanged', without adding rows. Same source with changed bytes is rejected as
'source_changed'; refresh belongs to M2. Title is the final source path segment.
Each document has exactly document_id, source_id, source, blob_id, title, text.

List and literal casefolded substring search over source + '\n' + text are ordered
by source (Python Unicode code-point order), then document_id. Query is at most
256 characters. Offset is a nonnegative integer and limit is an integer 1..100;
booleans are invalid integers. List/search return {'documents': [...], 'total':
matching count before pagination}. Show returns one complete document or
'not_found'. Export validates every selected ID first, rejects duplicate IDs,
and returns {'format':'local-research-library-v0','documents':[...]} in the same
source order. Omitted selection exports all. There are no timestamps in output.

CLI: python -m library --db FILE --root DIR import SOURCE | list [--offset N]
[--limit N] | search QUERY [--offset N] [--limit N] | show DOCUMENT_ID | export
[DOCUMENT_ID ...] | serve [--port N]. Every successful data command prints one
JSON value and exits 0. A domain/I/O error prints {'error':CODE} to stderr and
exits 2. Argparse usage errors also exit 2. Each invocation reopens persisted DB.
serve binds 127.0.0.1; GET / serves the accessible local browser client.

HTTP: GET /api/documents?q=TEXT&offset=N&limit=N; GET /api/documents/ID;
GET /api/export (all); POST /api/export with {'ids':[ID,...]}; POST /api/import
with {'source':SOURCE}. Successful responses are 200 JSON. Errors are
{'error':CODE}, status 404 for not_found, 409 for source_changed, otherwise 400.
Unsupported routes return 404. POST requires application/json and at most 65536
bytes. Missing required fields, extra POST fields, and malformed query values
are invalid_request. GET /health returns {'status':'ok','schema':0}. Browser
list/search/import/show/export use those routes and render text as text, not HTML.

Workflow adapter: solution.solve({'operations':[OP,...]}) creates an isolated
temporary input root/database, invokes the actual application modules, and
returns {'results':[...], 'documents':[all final documents]}. At most 64 ops and
60KiB JSON input/output are admitted by the public oracle. Ops: import(source,
text), list(offset=0,limit=100), search(query,offset=0,limit=100), show(source),
export(sources optional), reopen(). Import returns {'status':'imported' or
'unchanged','document':DOCUMENT}; domain errors yield {'error':CODE} and processing
continues. Reopen returns {'reopened':true}. Source references in show/export are
converted to document IDs. Adapter imports write supplied UTF-8 text beneath the
temporary root, then call the real file importer; it is not a separate solver.


## M1 ingestion extension

M1: reliable ingestion (v1; all v0 behavior remains binding)
This is milestone one of four, not a substitute for M2 lifecycle, M3 recovery or
M4 migration. Preserve the six-trajectory/full-project plan and final barrier.

Add bounded directory, ZIP and JSON-bundle intake. Directory discovery recurses
regular files without following symlinks and sorts source keys; .txt/.md are
inherited and .html is accepted as literal UTF-8 source text in M1 (no execution,
network loading or rich rendering). Unsupported members fail the whole batch.
JSON bundle is exactly {'entries':[{'source':KEY,'text':TEXT},...]}. ZIP sources
are member POSIX paths beneath a caller-supplied relative namespace, prefixed
NAMESPACE + '/'; directory sources are similarly prefixed relative file paths.
No extraction is permitted. Reject symlink/encrypted ZIP entries, path traversal,
duplicate canonical source keys, nonregular members, malformed UTF-8, >64 files,
>32768 decoded member bytes, >524288 total uncompressed bytes, >1048576 archive
bytes, >100 expansion ratio (uncompressed/max(1,compressed), per member and
aggregate), and >16 path segments.
ZIP directory entries are ignored but validated for traversal; all resource
bounds count before committing. Empty batches are allowed. JSON text is encoded
as UTF-8 for byte limits. Each admission is immutable and persists its canonical
manifest: ordered entries sorted by source, including text and content hashes.

Durable job state has exactly job_id, epoch, state, total, completed, error.
job_id is 1..64 ASCII alphanumeric/'_'/'-' characters. States queued, running,
completed, cancelled, failed; initial epoch=1, completed=0, error=null. total is
the manifest size. Failed/cancelled jobs expose completed=0. Atomic M1 exposes
completed=total only at commit (no fake partially visible catalog progress).
submit(job_id, entries) replays the current job if the canonical manifest matches,
otherwise job_conflict. prepare(job_id) queues->running and returns token
{'job_id':ID,'epoch':E}; repeated prepare while running returns that token.
Prepare of a terminal job fails job_state. The coordinator validates every entry
before catalog mutation; bad entries set failed/error=CODE with no document/blob
changes. Exact codes: invalid_source, unsupported_type, invalid_utf8, too_large,
invalid_batch, source_changed, capacity. Cancellation queued/running increments epoch and sets
cancelled, completed=0, error=null; repeat cancellation of cancelled is unchanged;
cancelling completed/failed is job_state. retry is allowed only for failed or
cancelled, increments epoch, sets queued/completed=0/error=null, and retains the
original manifest. Job lookup and all state transitions survive reopening DB.

commit(token) checks epoch and state inside the SAME SQLite write transaction as
all document/blob writes and terminal job receipt. Wrong epoch is stale_epoch
even if the job has become terminal. Matching running token commits all entries
or none; validation/conflict failure sets failed/error=CODE, leaves the catalog
unchanged and returns {'error':CODE}. Matching completed token replays the same
receipt. Tokens for queued/cancelled/failed fail job_state. Commit receipt is
{'job':JOB,'documents':[batch documents sorted by source]}. Existing identical
sources are included once in that receipt, shared blobs remain shared, and any
one conflicting source rolls back the whole batch. Cancellation/retry fences all
older prepared tokens. A direct v0 import between prepare and commit must be
rechecked transactionally. An injected failure immediately before commit leaves
all old catalog state usable and no new blobs/documents; job remains running and
the operation returns injected_failure. Advanced automatic
crash resumption, cleanup and backup/restore remain M3 obligations.

Four independently reviewable interfaces (new modules within package scopes are
allowed; immutable common seed is unchanged): library.catalog.store.Store owns
durable job, manifest, receipt and atomic/fenced batch transactions; import
JobManager from library.ingestion.jobs. JobManager owns
submit/prepare/commit/cancel/retry/get, directory/ZIP/JSON discovery and validation;
library.query.service.Service exposes GET /api/jobs returning {'jobs':[JOB,...]}
sorted by job_id, /api/jobs POST and /api/jobs/ID GET plus POST
/api/jobs/ID/{prepare,commit,cancel,retry}, with commit body {'epoch':E}; clients
provides equivalent CLI job commands, visible browser job state/cancel/retry and
workflow operations. JobManager(store) methods take the arguments named above;
get takes job_id. submit takes job_id and entries; commit takes a token dictionary
and keyword-only fail_before_commit=False. Service must call JobManager, which
must call Store transaction APIs: no independent in-memory client job DB.

Required Store methods, all returning fresh JSON-compatible values:
create_job(job_id, entries)->JOB; get_job(job_id)->JOB; list_jobs()->[JOB,...];
job_manifest(job_id)->ENTRIES; start_job(job_id, epoch)->TOKEN;
fail_job(job_id, epoch, code)->JOB; cancel_job(job_id)->JOB;
retry_job(job_id)->JOB; commit_job(job_id, epoch, *, fail_before_commit=False)
->RECEIPT. create_job canonicalizes manifest order by (source,text), persists
exact source/text entries and applies submit replay/conflict. Shape/job-id errors
are invalid_request; semantic member errors are deferred until prepare/commit.
start_job atomically fences epoch, transitions queued->running or replays running,
and rejects terminal state. fail_job atomically fences epoch, only accepts
queued/running, and records failed/error=code/completed=0. commit_job reads the
persisted manifest itself; it repeats all member, conflict, capacity and identity
checks inside its transaction before writes. All stale epochs fail stale_epoch.
All terminal-state misuse fails job_state; missing jobs fail not_found.
JobManager.prepare returns the existing token immediately for running jobs;
otherwise it validates the queued manifest, calls fail_job on validation failure,
or start_job on success. Concurrent state changes must be fenced at either write.
The optional fail_before_commit flag raises LibraryError('injected_failure')
after provisional writes but before SQLite commit; rollback includes job state
and receipt. It is an offline test hook, never an HTTP/browser request field.

JobManager.submit_directory(job_id, root, namespace), submit_zip(job_id,
archive_path, namespace), and submit_json(job_id, bundle_path) read only explicit
local paths and return JOB via submit. Invalid discovery/archive/JSON performs no
admission: job and catalog remain unchanged. ZIP/JSON syntax errors are
invalid_archive/invalid_json, I/O errors io_error; traversal/symlinks invalid_source,
bad UTF-8 invalid_utf8, file-count/duplicate-key errors invalid_batch, expansion
or byte bounds too_large. JSON schema errors are invalid_json; semantic source
errors in an otherwise valid entries bundle are deferred to prepare. ZIP
directory entries strip the final '/' for source validation then are ignored;
nonregular Unix modes other than symlinks and encrypted entries fail
invalid_archive; symlinks fail invalid_source. Missing Unix
filetype bits and explicit regular files are accepted. No symlink may be followed.

Route submit body is exactly one of {'job_id':ID,'entries':ENTRIES},
{'job_id':ID,'directory':PATH,'namespace':KEY}, {'job_id':ID,'zip':PATH,
'namespace':KEY}, {'job_id':ID,'json':PATH}. PATH is relative to Service.root and
uses inherited path confinement, except directory is a directory and bundle
extensions are .zip/.json. Namespace must be a valid key; all prefixed member
keys retain the 256-byte/16-segment limit. Actions other than commit accept {}.
CLI adds job-submit ID --kind directory|zip|json PATH [--namespace KEY] (namespace
required for directory/zip, forbidden for json), job-show ID, job-prepare ID,
job-commit ID EPOCH, job-cancel ID, job-retry ID, and jobs. Every CLI command shares
the inherited database/root options, JSON output and exit conventions. Errors job_conflict,
job_state, stale_epoch use HTTP409; not_found uses404; others400. Success200.

M1 adapter operations extend v0: submit(job_id,entries), prepare(job_id),
commit(job_id,epoch,fail_before_commit optional boolean), cancel(job_id),
retry(job_id), job(job_id), reopen(). For
M1 histories set payload['milestone']='m1'; output additionally includes jobs
sorted by job_id. Each op result is the exact JobManager result above: submit,
cancel,retry,get return JOB; prepare returns token; commit returns receipt.
Invalid-entry prepare returns {'error':CODE} after persisting failed state.
Public operation histories exercise M1 without encoding implementation source.
public_intake_probes() separately publishes portable directory/ZIP/JSON recipes
for evaluator execution against these real intake APIs; the dictionary workflow
oracle covers transitions, not archive parser correctness or physical durability.

Required integration evidence: source-bound package checks, inherited v0 cases,
actual merged-tree M1 cases, real CLI/API/browser workflows, durable reopen, and
an explicit clean textual merge conflict/repair. Freeze disjoint changes where
catalog accepts stale epochs while ingestion assumes catalog fences them, or
catalog deduplicates by blob while query assumes per-source documents. Their
combined wrong behavior must fail a public contract probe before repair and pass
after repair, with Git ancestry, both reviewer decisions and source-bound receipts.
Do not call a merge alone proof. This public development contract contains no
private scenarios and makes no held-out statistical-evidence claim.


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


## Error mapping and command process behavior

HTTP success is 200 application/json. Errors are {"error":CODE}. not_found is404; source_changed, job_conflict, job_state, stale_epoch, stale_version, stale_generation, document_deleted, revision_capacity, collection_not_empty, worker_busy, stale_worker, maintenance_busy and already_exists are409. Other domain errors, including invalid_request, invalid_source, invalid_utf8, unsupported_type, too_large, capacity, invalid_backup, invalid_database, unsupported_schema and io_error, are400. Unsupported routes are404. CLI data success writes exactly one JSON value to stdout and exits0; domain/I/O errors write one error JSON to stderr and exit2; argparse usage errors exit2. Canonical export bytes have no BOM or trailing newline. Worker daemon writes no JSON until stopped.
''',
    'release/RECOVERY.md': r'''# Recovery and migration

Stop older v0/M1/M2 binaries before opening their database with this release.
Those binaries predate the maintenance lock and cannot safely share a database
with a migration. Preserve an untouched offline copy first. `migrate` or opening
Store validates recognized schema 0, 2, 3 or 4, its tables and content graph, then
converts atomically to schema4. Unknown versions fail `unsupported_schema`;
inconsistent recognized data fails `invalid_database`, without repairing it by
silently creating inherited tables. A repeated schema4 migration is read-only
for logical data, generation, edit tokens and exact stored job receipt strings.

```sh
python -m library --db ../library-state/library.sqlite3 --root ../library-state/inputs --backup-dir ../library-state/backups migrate
```

An abrupt exit before migration commit leaves the old complete database; after
commit it leaves the complete new database. Reopen and inspect durable schema.
Old binaries cannot open physical schema4. Rollback means stopping every writer
and restoring your separately retained pre-migration database for the old release;
do not copy a database over an open connection or swap SQLite/WAL files live.
M4 compatibility concerns old command forms and HTTP requests through the new
release, not running old executables against new storage.

Jobs do not execute merely because Store, the server or an ordinary CLI opens.
Use `worker-enqueue JOB_ID`, then `worker --once` or `worker` for a polling daemon.
Only queued/running jobs can be enrolled; terminal jobs require explicit retry.
Duplicate enrollment is unchanged. One worker owns a lifetime exclusive lock;
a second fails `worker_busy`. Each owner gets a new generation, and each write
checks installation/worker generation before job epoch (`stale_worker`, then
`stale_epoch`). Persisted manifests are resumed without rereading input files.
A crash before commit leaves no provisional catalog rows; after commit, replay
returns the exact original receipt. Completed jobs never silently rerun.

Edits carry `expected_version`, independent of revision ID. On `stale_version`,
reload and reconcile the other edit yourself. The browser preserves an unsaved
draft until explicit Reload document. Pagination carries catalog `generation`;
on `stale_generation`, reload a coherent page instead of combining snapshots.

Create an unused basename such as `checkpoint.json` with `backup NAME` or Create
backup. Publication does not replace an existing file. List backups through the
registry; do not infer completeness from a filename. Backups contain a canonical
logical v3 envelope, SHA256 payload digest, all tombstones and numbered histories,
collections, persisted job manifests and original receipt strings. Schema4 uses
that same portable logical projection. Derived indexes and live authority tokens
are not a raw filesystem snapshot. A manually supplied complete envelope can be
validated by restore; never edit a digest to conceal a broken reference graph.

Before restore, read `diagnostics` and use its current generation explicitly:

```sh
python -m library --db ../library-state/library.sqlite3 --root ../library-state/inputs --backup-dir ../library-state/backups diagnostics
python -m library --db ../library-state/library.sqlite3 --root ../library-state/inputs --backup-dir ../library-state/backups restore-backup checkpoint.json --expected-generation GENERATION
```

The product validates the full bounded backup before taking exclusive maintenance
authority. Contention fails `maintenance_busy`; a changed generation fails
`stale_generation`. Restore replaces the logical record set in one transaction,
rotates installation authority, and advances the live generation by one. Restored
document edit tokens and noncompleted job epochs exceed both snapshot values and
retained target high-water marks. Removed IDs retain fencing tombstones, so their
later recreation cannot make stale edits or jobs valid again. Completed receipts
and their original epochs remain immutable replay results. Existing handles must
detect the changed incarnation; reopen them before new work. A precommit crash
leaves the old state and a postcommit crash leaves the complete restored state.

`reindex --limit 64` performs at most64 records per step, resumes a durable cursor,
and atomically publishes a completed index. Catalog changes invalidate the shadow
and restart bounded work. Authoritative search stays available with an absent,
building or stale index. Cleanup visits only registered owned artifacts, bounded
per pass and protected against live owners; it never broadly deletes user input
or valid backup files.

`diagnostics` is a read-only snapshot of schema/generation, document/revision/blob
and job counts, real worker lock state, index state, last maintenance error and
suggested recovery action. It contains no content, paths or secrets and does not
retry or clear errors. Follow `restart_worker`, `retry_reindex`, `choose_new_backup`,
`validate_backup` or `run_migration` as appropriate. Repeated automatic retries are
not a repair strategy. See API.md for exact shapes and status/error mappings.
''',
    'release/USER-GUIDE.md': r'''# Practical library workflow

Complete INSTALL.md first. In every CLI command below, prepend:

`python -m library --db ../library-state/library.sqlite3 --root ../library-state/inputs --backup-dir ../library-state/backups`

1. Run `import welcome.txt` and `import notes.md`. These distinct source keys have
   identical initial UTF-8 bytes and therefore share a blob, while retaining
   distinct document/source IDs. Search `café` or `雪` to observe literal Unicode
   matching. Copy the returned welcome `document_id` as ID below.
2. Run `annotate ID --expected-version 1 --notes 'Reviewed café / 雪; literal <b>note</b>.' --tag demo`.
   In the browser, Open welcome.txt, type the same Notes and Tags, and Save
   annotations instead if using an independently fresh dataset. The loaded edit
   token advances to2; stale editors are rejected rather than overwritten.
3. Run `refresh ID --expected-version 2 --text 'Café research — 雪: second revision'`.
   The original revision remains in history, annotations survive, and edit token
   becomes3. Browser Refresh from text performs the same operation with its
   displayed token; v1 history displays both revision number and revision_id.
4. Run `delete ID --expected-version 3`; the legacy listing hides the tombstone.
   Show Deleted or All in the browser, then Restore document, or run
   `restore-document ID --expected-version 4`. The restored edit token is5.
5. Run `search café` and `document-v1 ID`. Select a record in the browser and use
   Export selection. No selection exports an empty array; Export all is a
   separate action. Include history/deleted are explicit and default false.
   `export-bundle ID --include-history` produces canonical v2 bytes;
   `export-v1 ID --include-history` explicitly requests v4 revision IDs.
   An export above Export limit bytes fails without a partial download.
6. Run `backup checkpoint.json`, or enter the name in Recovery and Create backup.
   Make a deliberate annotation edit, refresh diagnostics, and record the current
   generation. Use `restore-backup checkpoint.json --expected-generation N`, or
   Restore backup using the displayed generation. The later annotation disappears;
   restored edit tokens advance and older client drafts must be reconciled.
   Search again, inspect history, and export to confirm the restored content.

For literal HTML, use M1 batch intake. The legacy single-file `import` command
accepts only .txt/.md. For a fresh demonstration root, the browser Intake type
Directory with Local path pointing to a subdirectory containing literal.html
and a Namespace imports it through an explicit job: submit, prepare, then commit.
Or use `job-submit demo --kind directory SUBDIRECTORY --namespace demo`, then
`job-prepare demo` and `job-commit demo EPOCH` with the returned epoch. The `<script>`
and `<b>` strings are displayed as text; no markup executes. Do not submit the
entire input root if it contains unrelated files.

`release/dataset/workflow.json` is a separately versioned public scenario with
seven operations. It imports welcome/notes from the release's confined assets,
submits/prepares/commits literal.html through M1, then annotates welcome at token1
and refreshes at token2. This is not input to frozen `solution.solve`: its v0/M1
grammar cannot express annotations or refresh and remains unchanged. The public
`test_cumulative_public.py` runner interprets this closed scenario by calling the
actual product modules on a fresh writable root/database. Run that script from
the release root; retain the workflow file unchanged. Independent browser/CLI/HTTP
qualification must additionally perform the operator journey above on the exact
integrated release. Merely finding this document or a passing home page is not
whole-project acceptance.
''',
    'release/dataset/welcome.txt': r'''Café research — 雪
Shared bytes across two source keys.
''',
    'release/dataset/notes.md': r'''Café research — 雪
Shared bytes across two source keys.
''',
    'release/dataset/literal.html': r'''<h1>Café / 雪</h1>
<script>window.releaseScriptExecuted = true</script>
<b>Literal research text</b>
''',
    'release/dataset/workflow.json': r'''{
  "format": "local-research-library-public-workflow-v1",
  "operations": [
    {
      "op": "import",
      "path": "release/dataset/welcome.txt",
      "source": "welcome.txt"
    },
    {
      "op": "import",
      "path": "release/dataset/notes.md",
      "source": "notes.md"
    },
    {
      "entries": [
        {
          "path": "release/dataset/literal.html",
          "source": "literal.html"
        }
      ],
      "job_id": "release-literal",
      "op": "submit"
    },
    {
      "job_id": "release-literal",
      "op": "prepare"
    },
    {
      "epoch": 1,
      "job_id": "release-literal",
      "op": "commit"
    },
    {
      "collections": [],
      "expected_version": 1,
      "notes": "Reviewed café / 雪; literal <b>note</b>.",
      "op": "annotate",
      "source": "welcome.txt",
      "tags": [
        "demo"
      ]
    },
    {
      "expected_version": 2,
      "op": "refresh",
      "source": "welcome.txt",
      "text": "Café research — 雪\nSecond revision; preserved annotations.\n"
    }
  ]
}
''',
}

def release_files(source_paths: Sequence[str]) -> dict[str, str]:
    """Return release sources with the exact final application/fixture allowlist.

    Caller supplies all final text and public binary fixture paths. This function
    adds only its declared release files; it never discovers files from cwd.
    """
    if isinstance(source_paths, (str, bytes)):
        raise ValueError("A sequence of explicit source paths is required")
    names = list(source_paths)
    if len(names) != len(set(names)):
        raise ValueError("Duplicate release source path")
    assets = dict(_ASSETS)
    all_paths = set(names) | set(assets) | {"library/clients/release.py"}
    allowed_fixed = set(IMMUTABLE_PATHS) | set(PUBLIC_FIXTURE_PATHS) | set(assets)
    for name in all_paths:
        if (not isinstance(name, str) or not name or "\\" in name or "\0" in name
                or str(PurePosixPath(name)) != name or name.startswith("/")
                or any(part.startswith(".") for part in PurePosixPath(name).parts)):
            raise ValueError("Invalid release source path")
        package_source = (name.startswith(("library/catalog/", "library/ingestion/",
                          "library/query/", "library/clients/"))
                          and PurePosixPath(name).suffix in (".py", ".html"))
        if name not in allowed_fixed and not package_source:
            raise ValueError("Unowned or private release source path")
    if not (set(IMMUTABLE_PATHS) | set(PUBLIC_FIXTURE_PATHS)).issubset(all_paths):
        raise ValueError("Release requires immutable seed and public fixture paths")
    assets["library/clients/release.py"] = (_RELEASE
        .replace("__SOURCE_PATHS__", repr(tuple(sorted(all_paths))))
        .replace("__RUNTIME_IMAGE__", repr(RUNTIME_IMAGE))
        .replace("__PRODUCT_CONTRACT_SHA256__", repr(PRODUCT_CONTRACT_SHA256)))
    return assets
