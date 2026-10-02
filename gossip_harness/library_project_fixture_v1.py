"""Authored Local Research Library v0 and the public M1 development contract.

This is a development/discovery family, not independent held-out evidence. The
seed is a working, deliberately small application. It does not implement M1.
Only ``seed_files`` and ``public_manifest`` belong in a builder workspace. The
dictionary oracle below is separately authored specification code; it never
imports or executes candidate files. Private acceptance must be independently
authored and withheld until the complete cohort's frozen-source barrier.
"""

from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import PurePosixPath
from textwrap import dedent
from typing import Any


FIXTURE_VERSION = "local-research-library-v1"
FAMILY = "local-research-library-development"
RUNTIME_IMAGE = "sha256:f77ac9e44ae96ef2c90b8053ea08c31f8be030f824196b0ae4db6d462c84e51f"
PACKAGE_SCOPES = {
    "catalog": ("library/catalog/",),
    "ingestion": ("library/ingestion/",),
    "query": ("library/query/",),
    "clients": ("library/clients/",),
}
IMMUTABLE_PATHS = (
    "README.md", "solution.py", "library/__init__.py", "library/__main__.py",
    "library/common.py", "examples/welcome.txt", "public_cases.json", "test_public.py",
)

V0_REQUIREMENTS = """Local Research Library v0 compatibility contract
Runtime: Python 3.12 standard library only, pinned evaluator image below. No
network retrieval, accounts, PDF/OCR, external providers or product model calls.
One SQLite database and an explicitly configured local input root. All paths are
relative POSIX source keys: 1..256 UTF-8 bytes, at most 128 UTF-8 bytes per segment,
no backslash, NUL, empty, '.' or '..' segment. Absolute paths and symlinks
(including parent symlinks) are rejected.
V0 imports regular .txt and .md files, at most 32768 raw bytes each, strict UTF-8,
preserving exact decoded text and newlines. Empty text is valid. At most 256
documents may be stored. Errors leave the catalog and blobs unchanged.

Source ID = 'src-' + sha256(b'source\\0' + source.encode('utf-8')).hexdigest().
Document ID = 'doc-' + sha256(b'document\\0' + source.encode('utf-8')).hexdigest().
Blob ID = 'blob-' + sha256(raw UTF-8 bytes).hexdigest(). IDs persist across process
restart and never depend on the root's absolute path. Different source keys
produce different documents even when bytes match; their blob ID then matches.
Reimporting the same source and bytes returns the original document, status
'unchanged', without adding rows. Same source with changed bytes is rejected as
'source_changed'; refresh belongs to M2. Title is the final source path segment.
Each document has exactly document_id, source_id, source, blob_id, title, text.

List and literal casefolded substring search over source + '\\n' + text are ordered
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
"""

M1_REQUIREMENTS = """M1: reliable ingestion (v1; all v0 behavior remains binding)
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
"""


def _source(text: str) -> str:
    return dedent(text).lstrip("\n").rstrip() + "\n"


_COMMON = _source(r'''
    """Immutable shared v0 identity, limits and domain errors."""
    import hashlib

    MAX_FILE_BYTES = 32768
    MAX_DOCUMENTS = 256

    class LibraryError(ValueError):
        def __init__(self, code):
            super().__init__(code)
            self.code = code

    def source_key(source):
        if (type(source) is not str or not source or '\\' in source or '\x00' in source
                or any(p in ('', '.', '..') for p in source.split('/'))):
            raise LibraryError('invalid_source')
        try:
            raw = source.encode('utf-8')
        except UnicodeError as error:
            raise LibraryError('invalid_source') from error
        if len(raw) > 256 or any(len(part.encode('utf-8')) > 128 for part in source.split('/')):
            raise LibraryError('invalid_source')
        return source

    def identity(kind, source):
        source_key(source)
        prefix = {'source': 'src-', 'document': 'doc-'}[kind]
        return prefix + hashlib.sha256(kind.encode() + b'\0' + source.encode('utf-8')).hexdigest()

    def blob_id(raw):
        return 'blob-' + hashlib.sha256(raw).hexdigest()

    def page(offset, limit):
        if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 100:
            raise LibraryError('invalid_request')
        return offset, limit
''')

_STORE = _source(r'''
    """SQLite ownership of documents and immutable content blobs."""
    import sqlite3
    from library.common import LibraryError, MAX_DOCUMENTS, blob_id, identity

    class Store:
        def __init__(self, path):
            self.db = sqlite3.connect(str(path), timeout=5)
            self.db.row_factory = sqlite3.Row
            self.db.execute('PRAGMA foreign_keys=ON')
            self.db.executescript(''' + '"""' + r'''
                CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                INSERT OR IGNORE INTO metadata VALUES ('schema', '0');
                CREATE TABLE IF NOT EXISTS blobs (blob_id TEXT PRIMARY KEY, content BLOB NOT NULL);
                CREATE TABLE IF NOT EXISTS documents (
                    document_id TEXT PRIMARY KEY, source_id TEXT UNIQUE NOT NULL,
                    source TEXT UNIQUE NOT NULL, blob_id TEXT NOT NULL REFERENCES blobs(blob_id),
                    title TEXT NOT NULL);
            ''' + '"""' + r''')
            self.db.commit()

        def close(self):
            self.db.close()

        def _document(self, row):
            return {key: row[key] for key in ('document_id', 'source_id', 'source', 'blob_id', 'title')} | {
                'text': bytes(row['content']).decode('utf-8')}

        def documents(self):
            rows = self.db.execute('SELECT d.*, b.content FROM documents d JOIN blobs b USING(blob_id)')
            return sorted((self._document(row) for row in rows), key=lambda d: (d['source'], d['document_id']))

        def show(self, document_id):
            row = self.db.execute('SELECT d.*, b.content FROM documents d JOIN blobs b USING(blob_id) '
                                  'WHERE document_id=?', (document_id,)).fetchone()
            if row is None:
                raise LibraryError('not_found')
            return self._document(row)

        def insert(self, source, raw):
            did, sid, bid = identity('document', source), identity('source', source), blob_id(raw)
            try:
                self.db.execute('BEGIN IMMEDIATE')
                previous = self.db.execute('SELECT blob_id FROM documents WHERE source=?', (source,)).fetchone()
                if previous:
                    if previous['blob_id'] != bid:
                        raise LibraryError('source_changed')
                    result = {'status': 'unchanged', 'document': self.show(did)}
                else:
                    if self.db.execute('SELECT count(*) FROM documents').fetchone()[0] >= MAX_DOCUMENTS:
                        raise LibraryError('capacity')
                    self.db.execute('INSERT OR IGNORE INTO blobs VALUES (?,?)', (bid, raw))
                    self.db.execute('INSERT INTO documents VALUES (?,?,?,?,?)',
                                    (did, sid, source, bid, source.rsplit('/', 1)[-1]))
                    result = {'status': 'imported', 'document': self.show(did)}
                self.db.commit()
                return result
            except BaseException:
                self.db.rollback()
                raise
''')

_INGEST = _source(r'''
    """Synchronous v0 single-file import; durable batch jobs arrive in M1."""
    from pathlib import Path
    from library.common import LibraryError, MAX_FILE_BYTES, source_key

    def checked_source(source):
        source_key(source)
        if Path(source).suffix.lower() not in ('.txt', '.md'):
            raise LibraryError('unsupported_type')
        return source

    def import_file(store, root, source):
        checked_source(source)
        base = Path(root).resolve()
        target = base
        for part in source.split('/'):
            target = target / part
            if target.is_symlink():
                raise LibraryError('invalid_source')
        try:
            target.resolve().relative_to(base)
            if not target.is_file():
                raise LibraryError('io_error')
            with target.open('rb') as handle:
                raw = handle.read(MAX_FILE_BYTES + 1)
        except (OSError, ValueError) as error:
            if isinstance(error, LibraryError):
                raise
            raise LibraryError('io_error') from error
        if len(raw) > MAX_FILE_BYTES:
            raise LibraryError('too_large')
        try:
            raw.decode('utf-8')
        except UnicodeError as error:
            raise LibraryError('invalid_utf8') from error
        return store.insert(source, raw)
''')

_SERVICE = _source(r'''
    """Shared query and HTTP contract, consumed by CLI and browser server."""
    import sqlite3
    from urllib.parse import parse_qs, unquote, urlsplit
    from library.common import LibraryError, page
    from library.ingestion.local import import_file

    class Service:
        def __init__(self, store, root):
            self.store, self.root = store, root

        def listing(self, query='', offset=0, limit=100):
            page(offset, limit)
            if type(query) is not str or len(query) > 256:
                raise LibraryError('invalid_request')
            needle = query.casefold()
            rows = [doc for doc in self.store.documents()
                    if needle in (doc['source'] + '\n' + doc['text']).casefold()]
            return {'documents': rows[offset:offset + limit], 'total': len(rows)}

        def export(self, ids=None):
            rows = self.store.documents()
            if ids is not None:
                if type(ids) is not list or any(type(x) is not str for x in ids) or len(set(ids)) != len(ids):
                    raise LibraryError('invalid_request')
                selected = set(ids)
                if selected - {d['document_id'] for d in rows}:
                    raise LibraryError('not_found')
                rows = [d for d in rows if d['document_id'] in selected]
            return {'format': 'local-research-library-v0', 'documents': rows}

        def request(self, method, target, body=None):
            try:
                url = urlsplit(target)
                query = parse_qs(url.query, keep_blank_values=True, strict_parsing=True)
                path = unquote(url.path)
                if method == 'GET' and path == '/health' and not query:
                    return 200, {'status': 'ok', 'schema': 0}
                if method == 'GET' and path == '/api/documents':
                    if set(query) - {'q', 'offset', 'limit'} or any(len(v) != 1 for v in query.values()):
                        raise LibraryError('invalid_request')
                    values = {k: query.get(k, [v])[0] for k, v in (('q', ''), ('offset', '0'), ('limit', '100'))}
                    if any(not values[k].isascii() or not values[k].isdigit() for k in ('offset', 'limit')):
                        raise LibraryError('invalid_request')
                    return 200, self.listing(values['q'], int(values['offset']), int(values['limit']))
                if method == 'GET' and path.startswith('/api/documents/') and not query:
                    return 200, self.store.show(path[len('/api/documents/'):])
                if method == 'GET' and path == '/api/export' and not query:
                    return 200, self.export()
                if method == 'POST' and path == '/api/export' and not query:
                    if type(body) is not dict or set(body) != {'ids'}:
                        raise LibraryError('invalid_request')
                    return 200, self.export(body['ids'])
                if method == 'POST' and path == '/api/import' and not query:
                    if type(body) is not dict or set(body) != {'source'}:
                        raise LibraryError('invalid_request')
                    return 200, import_file(self.store, self.root, body['source'])
                return 404, {'error': 'not_found'}
            except (ValueError, TypeError, UnicodeError, OSError, sqlite3.Error) as error:
                code = error.code if isinstance(error, LibraryError) else (
                    'io_error' if isinstance(error, (OSError, sqlite3.Error)) else 'invalid_request')
                return {'not_found': 404, 'source_changed': 409}.get(code, 400), {'error': code}
''')

_CLI = _source(r'''
    """Local Research Library command line."""
    import argparse
    import json
    import sqlite3
    import sys
    from library.catalog.store import Store
    from library.common import LibraryError
    from library.ingestion.local import import_file
    from library.query.service import Service

    def main(argv=None):
        parser = argparse.ArgumentParser(description='Local Research Library')
        parser.add_argument('--db', default='library.sqlite3')
        parser.add_argument('--root', default='.')
        commands = parser.add_subparsers(dest='command', required=True)
        commands.add_parser('import').add_argument('source')
        for name in ('list', 'search'):
            p = commands.add_parser(name)
            if name == 'search':
                p.add_argument('query')
            p.add_argument('--offset', type=int, default=0)
            p.add_argument('--limit', type=int, default=100)
        commands.add_parser('show').add_argument('document_id')
        commands.add_parser('export').add_argument('document_ids', nargs='*')
        commands.add_parser('serve').add_argument('--port', type=int, default=8765)
        args = parser.parse_args(argv)
        store = None
        try:
            store = Store(args.db)
            service = Service(store, args.root)
            if args.command == 'serve':
                from library.clients.http import serve
                serve(service, args.port)
                return 0
            if args.command == 'import':
                value = import_file(store, args.root, args.source)
            elif args.command in ('list', 'search'):
                value = service.listing(getattr(args, 'query', ''), args.offset, args.limit)
            elif args.command == 'show':
                value = store.show(args.document_id)
            else:
                value = service.export(args.document_ids or None)
            print(json.dumps(value, ensure_ascii=True, sort_keys=True))
            return 0
        except (LibraryError, OSError, sqlite3.Error) as error:
            print(json.dumps({'error': getattr(error, 'code', 'io_error')}), file=sys.stderr)
            return 2
        finally:
            if store is not None:
                store.close()
''')

_HTTP = _source(r'''
    """A loopback-only standard HTTP server; no third-party server dependencies."""
    from http.server import BaseHTTPRequestHandler, HTTPServer
    import json
    from pathlib import Path

    def handler_for(service):
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def respond(self, status, value, content_type='application/json; charset=utf-8'):
                raw = value if isinstance(value, bytes) else json.dumps(value, ensure_ascii=True).encode()
                self.send_response(status)
                self.send_header('Content-Type', content_type)
                self.send_header('Content-Length', str(len(raw)))
                self.send_header('Cache-Control', 'no-store')
                self.send_header('X-Content-Type-Options', 'nosniff')
                self.send_header('Content-Security-Policy', "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'")
                self.end_headers()
                self.wfile.write(raw)

            def valid_host(self):
                port = self.server.server_address[1]
                return self.headers.get('Host') in (f'127.0.0.1:{port}', f'localhost:{port}')

            def do_GET(self):
                if not self.valid_host():
                    self.respond(400, {'error': 'invalid_request'})
                elif self.path.split('?', 1)[0] == '/':
                    self.respond(200, Path(__file__).with_name('index.html').read_bytes(), 'text/html; charset=utf-8')
                else:
                    self.respond(*service.request('GET', self.path))

            def do_POST(self):
                try:
                    length = int(self.headers.get('Content-Length', '-1'))
                    if (not self.valid_host() or not 0 <= length <= 65536
                            or self.headers.get_content_type() != 'application/json'):
                        raise ValueError('bad request')
                    body = json.loads(self.rfile.read(length).decode('utf-8'))
                except (ValueError, UnicodeError):
                    self.respond(400, {'error': 'invalid_request'})
                    return
                self.respond(*service.request('POST', self.path, body))
        return Handler

    def serve(service, port=8765):
        with HTTPServer(('127.0.0.1', port), handler_for(service)) as server:
            print(f'Local Research Library: http://127.0.0.1:{server.server_port}/', flush=True)
            server.serve_forever()
''')

_BROWSER = _source(r'''
    <!doctype html>
    <html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
    <title>Local Research Library</title><style>
    body{font:17px/1.5 system-ui;margin:2rem auto;padding:0 1rem;max-width:60rem;color:#182b35;background:#f6f4eb}
    h1{font-family:Georgia,serif}form{margin:1rem 0;display:flex;gap:.6rem;flex-wrap:wrap}input,button{font:inherit;padding:.45rem}
    input{min-width:16rem}button{cursor:pointer}li{margin:.5rem 0}pre{white-space:pre-wrap;overflow-wrap:anywhere;background:white;padding:1rem;border:1px solid #ccd2ce}
    :focus-visible{outline:3px solid #16645a;outline-offset:2px}.muted{color:#526168}
    </style></head><body><header><h1>Local Research Library</h1>
    <p class="muted">Your local text and Markdown collection. Files stay on this computer.</p></header><main>
    <form id="import-form"><label>Source path <input id="source" required placeholder="examples/welcome.txt"></label><button>Import local file</button></form>
    <form id="search-form"><label>Search <input id="query" type="search" placeholder="Search text and source paths"></label><button>Search</button></form>
    <p id="status" role="status" aria-live="polite"></p><ul id="documents" aria-label="Documents"></ul>
    <button id="previous">Previous page</button> <button id="next">Next page</button> <button id="export">Export all as JSON</button>
    <section aria-label="Document details"><h2 id="title">Document details</h2><pre id="content">Choose a document to read it.</pre></section>
    </main><script>
    const byId = id => document.getElementById(id);
    let offset = 0, total = 0;
    async function request(url, body) {
      const response = await fetch(url, body === undefined ? {} : {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
      const value = await response.json(); if (!response.ok) throw new Error(value.error); return value;
    }
    async function list() {
      try {
        const value = await request('/api/documents?q='+encodeURIComponent(byId('query').value)+'&offset='+offset+'&limit=20');
        total=value.total; byId('documents').replaceChildren();
        for (const doc of value.documents) {
          const li=document.createElement('li'), button=document.createElement('button'); button.textContent=doc.source;
          button.onclick=async()=>{try {const d=await request('/api/documents/'+doc.document_id);byId('title').textContent=d.source;byId('content').textContent=d.text;}catch(e){byId('status').textContent=e.message;}};
          li.append(button);byId('documents').append(li);
        }
        byId('status').textContent=total+' documents'+(total ? ', showing '+(offset+1)+'–'+(offset+value.documents.length) : '');
        byId('previous').disabled=offset===0;byId('next').disabled=offset+20>=total;
      } catch(e) {byId('status').textContent=e.message;}
    }
    byId('search-form').onsubmit=e=>{e.preventDefault();offset=0;list();};
    byId('import-form').onsubmit=async e=>{e.preventDefault();try {await request('/api/import',{source:byId('source').value});offset=0;await list();}catch(error){byId('status').textContent=error.message;}};
    byId('previous').onclick=()=>{offset=Math.max(0,offset-20);list();};byId('next').onclick=()=>{offset+=20;list();};
    byId('export').onclick=async()=>{try {const value=await request('/api/export');const url=URL.createObjectURL(new Blob([JSON.stringify(value,null,2)],{type:'application/json'}));const a=document.createElement('a');a.href=url;a.download='library.json';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);}catch(e){byId('status').textContent=e.message;}};
    list();
    </script></body></html>
''')

_WORKFLOW = _source(r'''
    """Blackbox transport over actual v0 file, ingestion, query and SQLite APIs."""
    from pathlib import Path
    import tempfile
    from library.catalog.store import Store
    from library.common import LibraryError, identity, source_key
    from library.ingestion.local import checked_source, import_file
    from library.query.service import Service

    def solve(payload):
        with tempfile.TemporaryDirectory(prefix='research-library-') as directory:
            root = Path(directory) / 'input'
            root.mkdir()
            path = Path(directory) / 'library.sqlite3'
            store = Store(path)
            results = []
            try:
                for index, operation in enumerate(payload['operations']):
                    service = Service(store, root)
                    try:
                        op = operation['op']
                        if op == 'import':
                            source = checked_source(operation['source'])
                            staging = root / str(index)
                            target = staging / source
                            target.parent.mkdir(parents=True, exist_ok=True)
                            try:
                                target.write_bytes(operation['text'].encode('utf-8'))
                            except UnicodeError as error:
                                raise LibraryError('invalid_utf8') from error
                            result = import_file(store, staging, source)
                        elif op in ('list', 'search'):
                            result = service.listing(operation.get('query', '') if op == 'search' else '',
                                                     operation.get('offset', 0), operation.get('limit', 100))
                        elif op == 'show':
                            result = store.show(identity('document', operation['source']))
                        elif op == 'export':
                            ids = [identity('document', s) for s in operation['sources']] if 'sources' in operation else None
                            result = service.export(ids)
                        elif op == 'reopen':
                            store.close()
                            store = Store(path)
                            result = {'reopened': True}
                        else:
                            raise LibraryError('unsupported_operation')
                    except LibraryError as error:
                        result = {'error': error.code}
                    results.append(result)
                return {'results': results, 'documents': store.documents()}
            finally:
                store.close()
''')


def seed_files() -> dict[str, str]:
    """Return fresh UTF-8 source bytes-as-text for the real, unsolved v0 app."""
    files = {
        "README.md": "# Local Research Library\n\nA runnable v0; M1 is not implemented.\n\n"
        "Run `python -m library --root . import examples/welcome.txt`, then\n"
        "`python -m library --root . serve --port 8765`. Open the printed loopback URL.\n"
        "Use `python test_public.py` for public inherited workflow checks.\n"
        "No installation or dependencies required. Python 3.12.\n\n"
        f"Pinned evaluation image: `{RUNTIME_IMAGE}`.\n\n" + V0_REQUIREMENTS + "\n\n" + M1_REQUIREMENTS,
        "solution.py": "from library.clients.workflow import solve\n",
        "library/__init__.py": '"""Local Research Library v0."""\n',
        "library/__main__.py": "from library.clients.cli import main\nraise SystemExit(main())\n",
        "library/common.py": _COMMON,
        "library/catalog/__init__.py": '"""Catalog and storage package."""\n',
        "library/catalog/store.py": _STORE,
        "library/ingestion/__init__.py": '"""Ingestion and jobs package."""\n',
        "library/ingestion/local.py": _INGEST,
        "library/query/__init__.py": '"""Query and service contract package."""\n',
        "library/query/service.py": _SERVICE,
        "library/clients/__init__.py": '"""CLI, HTTP, browser and release workflows."""\n',
        "library/clients/cli.py": _CLI,
        "library/clients/http.py": _HTTP,
        "library/clients/workflow.py": _WORKFLOW,
        "library/clients/index.html": _BROWSER,
        "examples/welcome.txt": "A local library\nImport, search, read and export your own research.\n",
        "public_cases.json": json.dumps(public_cases("v0"), ensure_ascii=True, indent=2) + "\n",
        "test_public.py": _source('''
            """Published v0 compatibility checks; execute candidate only in its sandbox."""
            import json
            from pathlib import Path
            import unittest
            from solution import solve

            class PublicCompatibilityTests(unittest.TestCase):
                def test_public_histories(self):
                    cases = json.loads(Path(__file__).with_name('public_cases.json').read_text())
                    for case in cases:
                        with self.subTest(case=case['id']):
                            self.assertEqual(solve(case['input']), case['expected'])

            if __name__ == '__main__':
                unittest.main()
        '''),
    }
    return files


class _DomainError(ValueError):
    pass


def _source_valid(source: Any, *, m1: bool = False) -> str:
    if (type(source) is not str or not source or "\\" in source or "\x00" in source
            or any(part in ("", ".", "..") for part in source.split("/"))):
        raise _DomainError("invalid_source")
    try:
        size = len(source.encode("utf-8"))
    except UnicodeError as error:
        raise _DomainError("invalid_source") from error
    if (size > 256 or any(len(part.encode("utf-8")) > 128 for part in source.split("/"))
            or (m1 and len(source.split("/")) > 16)):
        raise _DomainError("invalid_source")
    return source


def _document(source: str, text: str, *, m1: bool = False) -> dict[str, str]:
    """Independent dictionary specification; no candidate import or SQLite use."""
    _source_valid(source, m1=m1)
    if PurePosixPath(source).suffix.lower() not in ((".txt", ".md", ".html") if m1 else (".txt", ".md")):
        raise _DomainError("unsupported_type")
    if type(text) is not str:
        raise _DomainError("invalid_batch")
    try:
        raw = text.encode("utf-8")
    except UnicodeError as error:
        raise _DomainError("invalid_utf8") from error
    if len(raw) > 32768:
        raise _DomainError("too_large")
    return {
        "document_id": "doc-" + hashlib.sha256(b"document\0" + source.encode()).hexdigest(),
        "source_id": "src-" + hashlib.sha256(b"source\0" + source.encode()).hexdigest(),
        "source": source,
        "blob_id": "blob-" + hashlib.sha256(raw).hexdigest(),
        "title": source.rsplit("/", 1)[-1],
        "text": text,
    }


def oracle(payload: Any) -> dict[str, Any]:
    """Admit bounded public workflows and compute specified v0/M1 transitions.

    This assists development test selection, never private acceptance. Invalid
    operation shapes are inadmissible ValueErrors; admitted domain failures are
    observable error results. Reopen models persistence, not process testing.
    """
    if (type(payload) is not dict or set(payload) - {"operations", "milestone"}
            or payload.get("milestone", "v0") not in ("v0", "m1")
            or type(payload.get("operations")) is not list or len(payload["operations"]) > 64):
        raise ValueError("Invalid workflow")
    if len(json.dumps(payload, ensure_ascii=True, allow_nan=False).encode()) > 61440:
        raise ValueError("Workflow input too large")
    milestone = payload.get("milestone", "v0")
    documents: dict[str, dict[str, str]] = {}
    jobs: dict[str, dict[str, Any]] = {}
    manifests: dict[str, list[dict[str, str]]] = {}
    receipts: dict[str, dict[str, Any]] = {}
    results: list[Any] = []
    result: Any

    def rows() -> list[dict[str, str]]:
        return [deepcopy(documents[source]) for source in sorted(documents)]

    def batch(entries: list[dict[str, str]]) -> list[dict[str, str]]:
        if len(entries) > 64 or len({entry["source"] for entry in entries}) != len(entries):
            raise _DomainError("invalid_batch")
        prepared = [_document(entry["source"], entry["text"], m1=True) for entry in entries]
        if sum(len(item["text"].encode()) for item in prepared) > 524288:
            raise _DomainError("too_large")
        for doc in prepared:
            old = documents.get(doc["source"])
            if old is not None and old["blob_id"] != doc["blob_id"]:
                raise _DomainError("source_changed")
        if len(set(documents) | {doc["source"] for doc in prepared}) > 256:
            raise _DomainError("capacity")
        return prepared

    shapes = {"import": ({"source", "text"}, set()), "list": (set(), {"offset", "limit"}),
              "search": ({"query"}, {"offset", "limit"}), "show": ({"source"}, set()),
              "export": (set(), {"sources"}), "reopen": (set(), set()),
              "submit": ({"job_id", "entries"}, set()), "prepare": ({"job_id"}, set()),
              "commit": ({"job_id", "epoch"}, {"fail_before_commit"}), "cancel": ({"job_id"}, set()),
              "retry": ({"job_id"}, set()), "job": ({"job_id"}, set())}
    for operation in payload["operations"]:
        if type(operation) is not dict or type(operation.get("op")) is not str or operation["op"] not in shapes:
            raise ValueError("Invalid operation")
        op = operation["op"]
        required, optional = shapes[op]
        if not required <= set(operation) or set(operation) - required - optional - {"op"}:
            raise ValueError("Invalid operation fields")
        if op == "import" and (type(operation["source"]) is not str or type(operation["text"]) is not str):
            raise ValueError("Import strings required")
        if op == "export" and "sources" in operation and (type(operation["sources"]) is not list
                or any(type(source) is not str for source in operation["sources"])):
            raise ValueError("Export source list required")
        if op in ("show",) and type(operation["source"]) is not str:
            raise ValueError("Source string required")
        try:
            if op == "import":
                doc = _document(operation["source"], operation["text"])
                old = documents.get(doc["source"])
                if old is not None and old["blob_id"] != doc["blob_id"]:
                    raise _DomainError("source_changed")
                if old is None and len(documents) >= 256:
                    raise _DomainError("capacity")
                documents[doc["source"]] = doc
                result = {"status": "unchanged" if old else "imported", "document": deepcopy(doc)}
            elif op in ("list", "search"):
                offset, limit = operation.get("offset", 0), operation.get("limit", 100)
                query = operation.get("query", "")
                if (type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 100
                        or type(query) is not str or len(query) > 256):
                    raise _DomainError("invalid_request")
                selected = [doc for doc in rows() if query.casefold() in (doc["source"] + "\n" + doc["text"]).casefold()]
                result = {"documents": selected[offset:offset + limit], "total": len(selected)}
            elif op == "show":
                source = _source_valid(operation["source"])
                if source not in documents:
                    raise _DomainError("not_found")
                result = deepcopy(documents[source])
            elif op == "export":
                sources = operation.get("sources")
                if sources is not None:
                    for source in sources:
                        _source_valid(source)
                    if len(set(sources)) != len(sources):
                        raise _DomainError("invalid_request")
                    if set(sources) - set(documents):
                        raise _DomainError("not_found")
                result = {"format": "local-research-library-v0", "documents": [
                    doc for doc in rows() if sources is None or doc["source"] in sources]}
            elif op == "reopen":
                result = {"reopened": True}
            elif milestone != "m1":
                raise _DomainError("unsupported_operation")
            else:
                jid = operation["job_id"]
                if (type(jid) is not str or not 1 <= len(jid) <= 64 or not jid.isascii()
                        or any(not (c.isalnum() or c in "_-") for c in jid)):
                    raise ValueError("Invalid job ID")
                if op == "submit":
                    entries = operation["entries"]
                    if (type(entries) is not list or len(entries) > 128 or any(type(entry) is not dict
                            or set(entry) != {"source", "text"} or any(type(v) is not str for v in entry.values())
                            for entry in entries)):
                        raise ValueError("Invalid manifest shape")
                    entries = sorted(deepcopy(entries), key=lambda item: (item["source"], item["text"]))
                    if jid in jobs:
                        if manifests[jid] != entries:
                            raise _DomainError("job_conflict")
                    else:
                        jobs[jid] = {"job_id": jid, "epoch": 1, "state": "queued", "total": len(entries),
                                     "completed": 0, "error": None}
                        manifests[jid] = entries
                    result = deepcopy(jobs[jid])
                else:
                    if jid not in jobs:
                        raise _DomainError("not_found")
                    job = jobs[jid]
                    if op == "job":
                        result = deepcopy(job)
                    elif op == "prepare":
                        if job["state"] not in ("queued", "running"):
                            raise _DomainError("job_state")
                        if job["state"] == "queued":
                            try:
                                batch(manifests[jid])
                            except _DomainError as error:
                                job.update(state="failed", error=str(error))
                                raise
                            job["state"] = "running"
                        result = {"job_id": jid, "epoch": job["epoch"]}
                    elif op == "cancel":
                        if job["state"] == "cancelled":
                            result = deepcopy(job)
                        else:
                            if job["state"] not in ("queued", "running"):
                                raise _DomainError("job_state")
                            job.update(state="cancelled", epoch=job["epoch"] + 1, completed=0, error=None)
                            result = deepcopy(job)
                    elif op == "retry":
                        if job["state"] not in ("cancelled", "failed"):
                            raise _DomainError("job_state")
                        job.update(state="queued", epoch=job["epoch"] + 1, completed=0, error=None)
                        result = deepcopy(job)
                    else:
                        if type(operation["epoch"]) is not int or operation["epoch"] < 1:
                            raise ValueError("Invalid epoch")
                        if type(operation.get("fail_before_commit", False)) is not bool:
                            raise ValueError("Invalid fault hook")
                        if job["epoch"] != operation["epoch"]:
                            raise _DomainError("stale_epoch")
                        if job["state"] == "completed":
                            result = deepcopy(receipts[jid])
                        elif job["state"] != "running":
                            raise _DomainError("job_state")
                        else:
                            try:
                                prepared = batch(manifests[jid])
                            except _DomainError as error:
                                job.update(state="failed", error=str(error))
                                raise
                            if operation.get("fail_before_commit", False):
                                raise _DomainError("injected_failure")
                            documents.update({doc["source"]: doc for doc in prepared})
                            job.update(state="completed", completed=job["total"], error=None)
                            result = {"job": deepcopy(job), "documents": deepcopy(prepared)}
                            receipts[jid] = deepcopy(result)
        except _DomainError as error:
            result = {"error": str(error)}
        results.append(result)
    answer: dict[str, Any] = {"results": results, "documents": rows()}
    if milestone == "m1":
        answer["jobs"] = [deepcopy(jobs[jid]) for jid in sorted(jobs)]
    if len(json.dumps(answer, ensure_ascii=True).encode()) > 61440:
        raise ValueError("Workflow output too large")
    return answer


def public_cases(milestone: str = "v0") -> list[dict[str, Any]]:
    """Published development histories, never a private acceptance bank."""
    v0: list[tuple[str, list[dict[str, Any]]]] = [
        ("inherited-text-persistence", [{"op": "import", "source": "notes/a.txt", "text": "Café\r\nResearch"},
         {"op": "reopen"}, {"op": "search", "query": "CAFÉ"}, {"op": "show", "source": "notes/a.txt"},
         {"op": "export"}]),
        ("identity-and-replay", [{"op": "import", "source": "b.md", "text": "Shared"},
         {"op": "import", "source": "a.txt", "text": "Shared"}, {"op": "import", "source": "b.md", "text": "Shared"},
         {"op": "import", "source": "b.md", "text": "Changed"}, {"op": "list", "limit": 1, "offset": 1},
         {"op": "export", "sources": ["b.md", "a.txt"]}]),
        ("errors-preserve-catalog", [{"op": "import", "source": "empty.txt", "text": ""},
         {"op": "import", "source": "../escape.txt", "text": "bad"},
         {"op": "import", "source": "book.pdf", "text": "bad"}, {"op": "show", "source": "absent.txt"},
         {"op": "export", "sources": ["empty.txt", "empty.txt"]}, {"op": "list", "limit": True}]),
    ]
    m1: list[tuple[str, list[dict[str, Any]]]] = [
        ("m1-cancel-fences-old-token", [{"op": "submit", "job_id": "batch", "entries": [{"source": "a.txt", "text": "A"}]},
         {"op": "prepare", "job_id": "batch"}, {"op": "cancel", "job_id": "batch"},
         {"op": "reopen"}, {"op": "commit", "job_id": "batch", "epoch": 1},
         {"op": "retry", "job_id": "batch"}, {"op": "prepare", "job_id": "batch"},
         {"op": "commit", "job_id": "batch", "epoch": 3}, {"op": "commit", "job_id": "batch", "epoch": 3}]),
        ("m1-atomic-conflict-after-prepare", [{"op": "submit", "job_id": "batch", "entries": [
         {"source": "a.txt", "text": "A"}, {"source": "z.txt", "text": "Z"}]},
         {"op": "prepare", "job_id": "batch"}, {"op": "import", "source": "z.txt", "text": "Other"},
         {"op": "commit", "job_id": "batch", "epoch": 1}, {"op": "job", "job_id": "batch"}, {"op": "list"}]),
        ("m1-per-source-provenance", [{"op": "submit", "job_id": "shared", "entries": [
         {"source": "z.html", "text": "same"}, {"source": "a.md", "text": "same"}]},
         {"op": "prepare", "job_id": "shared"}, {"op": "commit", "job_id": "shared", "epoch": 1},
         {"op": "reopen"}, {"op": "export"}]),
        ("m1-invalid-member-rolls-back", [{"op": "submit", "job_id": "bad", "entries": [
         {"source": "good.txt", "text": "A"}, {"source": "../bad.txt", "text": "B"}]},
         {"op": "prepare", "job_id": "bad"}, {"op": "job", "job_id": "bad"}, {"op": "list"}]),
        ("m1-injected-transaction-failure", [{"op": "import", "source": "keep.txt", "text": "keep"},
         {"op": "submit", "job_id": "fault", "entries": [{"source": "new.txt", "text": "new"}]},
         {"op": "prepare", "job_id": "fault"},
         {"op": "commit", "job_id": "fault", "epoch": 1, "fail_before_commit": True},
         {"op": "list"}, {"op": "reopen"}, {"op": "job", "job_id": "fault"},
         {"op": "commit", "job_id": "fault", "epoch": 1}]),
    ]
    if milestone not in ("v0", "m1"):
        raise ValueError("Unknown milestone")
    records = []
    for case_id, operations in (v0 if milestone == "v0" else v0 + m1):
        payload: dict[str, Any] = {"operations": operations}
        if milestone == "m1":
            payload["milestone"] = "m1"
        records.append({"id": case_id, "input": payload, "expected": oracle(payload)})
    return records


def public_manifest() -> dict[str, Any]:
    files = seed_files()
    return {
        "version": FIXTURE_VERSION, "family": FAMILY, "milestone": "m1",
        "classification": "development/discovery; not held-out statistical evidence",
        "runtime_image": RUNTIME_IMAGE, "v0_requirements": V0_REQUIREMENTS,
        "m1_requirements": M1_REQUIREMENTS, "package_scopes": deepcopy(PACKAGE_SCOPES),
        "immutable_paths": list(IMMUTABLE_PATHS), "public_cases": public_cases("m1"),
        "public_intake_probes": public_intake_probes(),
        "seed_sha256": hashlib.sha256(json.dumps(files, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
        "seed_file_count": len(files), "seed_bytes": sum(len(value.encode()) for value in files.values()),
        "remaining_milestones": ["M2 document lifecycle", "M3 recovery and portability", "M4 compatibility release"],
        "full_cohort_trajectories": 6,
    }


def public_intake_probes() -> list[dict[str, Any]]:
    """Portable public intake recipes, for a separate sandboxed M1 evaluator.

    Each probe starts with a fresh Store containing preexisting ``keep.txt`` /
    ``keep`` and invokes the indicated real JobManager.submit_* API. The runner
    writes fixture bytes in its sandbox; ZIP construction uses zipfile without
    extraction. These are requirements/data, not claimed execution receipts.
    """
    return [
        {"id": "zip-identical-bytes-distinct-sources", "kind": "zip", "namespace": "papers",
         "members": [{"name": "a.txt", "text": "same"}, {"name": "b.md", "text": "same"}],
         "then": ["prepare", "commit", "reopen", "list"],
         "expect_sources": ["keep.txt", "papers/a.txt", "papers/b.md"],
         "assertions": ["two distinct document IDs", "one shared imported blob ID", "keep.txt unchanged"]},
        {"id": "zip-traversal-admits-nothing", "kind": "zip", "namespace": "papers",
         "members": [{"name": "good.txt", "text": "A"}, {"name": "../bad.txt", "text": "B"}],
         "expect_error": "invalid_source", "expect_jobs": [], "expect_sources": ["keep.txt"]},
        {"id": "zip-bad-utf8-admits-nothing", "kind": "zip", "namespace": "papers",
         "members": [{"name": "good.txt", "text": "A"}, {"name": "bad.txt", "hex_bytes": "ff"}],
         "expect_error": "invalid_utf8", "expect_jobs": [], "expect_sources": ["keep.txt"]},
        {"id": "zip-symlink-is-not-extracted", "kind": "zip", "namespace": "papers",
         "members": [{"name": "link.txt", "text": "../keep.txt", "unix_mode": 41471}],
         "expect_error": "invalid_source", "expect_jobs": [], "expect_sources": ["keep.txt"]},
        {"id": "directory-sorted-import", "kind": "directory", "namespace": "notes",
         "members": [{"name": "z.txt", "text": "Z"}, {"name": "sub/a.md", "text": "A"}],
         "then": ["prepare", "commit", "reopen", "list"],
         "expect_sources": ["keep.txt", "notes/sub/a.md", "notes/z.txt"]},
        {"id": "json-schema-rejects-extras", "kind": "json",
         "raw_text": '{"entries":[],"extra":true}', "expect_error": "invalid_json",
         "expect_jobs": [], "expect_sources": ["keep.txt"]},
    ]
