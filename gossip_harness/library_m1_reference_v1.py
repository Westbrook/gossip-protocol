"""Trusted authored M1 integration fixtures; never a live builder input.

These source maps exercise the existing, frozen public development contract.
They are deliberately separate from seed_files/public_manifest and from all
private acceptance evidence. The defective variant is a controlled semantic
integration fault, not a claim about what a model would write.
"""

from __future__ import annotations

from textwrap import dedent

from gossip_harness.library_project_fixture_v1 import PACKAGE_SCOPES, seed_files


def _source(value: str) -> str:
    return dedent(value).lstrip("\n").rstrip() + "\n"


_VALIDATION = _source(r'''
    """M1 semantic checks, repeated inside the catalog write transaction."""
    from pathlib import PurePosixPath
    from library.common import LibraryError, MAX_DOCUMENTS, MAX_FILE_BYTES, blob_id, source_key

    def check_job_id(job_id):
        if (type(job_id) is not str or not 1 <= len(job_id) <= 64 or not job_id.isascii()
                or any(not (char.isalnum() or char in '_-') for char in job_id)):
            raise LibraryError('invalid_request')
        return job_id

    def check_epoch(epoch):
        if type(epoch) is not int or epoch < 1:
            raise LibraryError('invalid_request')
        return epoch

    def canonical_manifest(entries):
        if (type(entries) is not list or any(type(entry) is not dict
                or set(entry) != {'source', 'text'}
                or type(entry['source']) is not str or type(entry['text']) is not str
                for entry in entries)):
            raise LibraryError('invalid_request')
        return sorted((dict(entry) for entry in entries), key=lambda item: (item['source'], item['text']))

    def validate_entries(entries, existing_documents=()):
        entries = canonical_manifest(entries)
        if len(entries) > 64 or len({item['source'] for item in entries}) != len(entries):
            raise LibraryError('invalid_batch')
        total = 0
        prepared = []
        for entry in entries:
            source = source_key(entry['source'])
            if len(source.split('/')) > 16:
                raise LibraryError('invalid_source')
            if PurePosixPath(source).suffix.lower() not in ('.txt', '.md', '.html'):
                raise LibraryError('unsupported_type')
            try:
                raw = entry['text'].encode('utf-8')
            except UnicodeError as error:
                raise LibraryError('invalid_utf8') from error
            if len(raw) > MAX_FILE_BYTES:
                raise LibraryError('too_large')
            total += len(raw)
            prepared.append((source, blob_id(raw)))
        if total > 524288:
            raise LibraryError('too_large')
        existing = {doc['source']: doc['blob_id'] for doc in existing_documents}
        for source, bid in prepared:
            if source in existing and existing[source] != bid:
                raise LibraryError('source_changed')
        if len(set(existing) | {source for source, bid in prepared}) > MAX_DOCUMENTS:
            raise LibraryError('capacity')
        return entries
''')


_STORE = _source(r'''
    """Durable catalog with one transaction for job fencing and batch writes."""
    from contextlib import contextmanager
    import hashlib
    import json
    import sqlite3
    from library.common import LibraryError, MAX_DOCUMENTS, blob_id, identity
    from library.catalog.validation import canonical_manifest, check_epoch, check_job_id, validate_entries

    class Store:
        def __init__(self, path):
            self.db = sqlite3.connect(str(path), timeout=5, isolation_level=None)
            self.db.row_factory = sqlite3.Row
            self.db.execute('PRAGMA foreign_keys=ON')
            self.db.executescript("""
                CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                INSERT OR IGNORE INTO metadata VALUES ('schema', '0');
                CREATE TABLE IF NOT EXISTS blobs (blob_id TEXT PRIMARY KEY, content BLOB NOT NULL);
                CREATE TABLE IF NOT EXISTS documents (
                    document_id TEXT PRIMARY KEY, source_id TEXT UNIQUE NOT NULL,
                    source TEXT UNIQUE NOT NULL, blob_id TEXT NOT NULL REFERENCES blobs(blob_id),
                    title TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY, epoch INTEGER NOT NULL, state TEXT NOT NULL,
                    total INTEGER NOT NULL, completed INTEGER NOT NULL, error TEXT,
                    manifest TEXT NOT NULL, content_hashes TEXT NOT NULL, receipt TEXT);
            """)

        @contextmanager
        def _write(self):
            self.db.execute('BEGIN IMMEDIATE')
            try:
                yield
                self.db.commit()
            except BaseException:
                self.db.rollback()
                raise

        def close(self):
            self.db.close()

        def _document(self, row):
            return {key: row[key] for key in ('document_id', 'source_id', 'source', 'blob_id', 'title')} | {
                'text': bytes(row['content']).decode('utf-8')}

        def documents(self):
            rows = self.db.execute('SELECT d.*, b.content FROM documents d JOIN blobs b USING(blob_id)')
            return sorted((self._document(row) for row in rows), key=lambda doc: (doc['source'], doc['document_id']))

        def show(self, document_id):
            row = self.db.execute('SELECT d.*, b.content FROM documents d JOIN blobs b USING(blob_id) '
                                  'WHERE document_id=?', (document_id,)).fetchone()
            if row is None:
                raise LibraryError('not_found')
            return self._document(row)

        def _insert_document(self, source, raw):
            did, sid, bid = identity('document', source), identity('source', source), blob_id(raw)
            previous = self.db.execute('SELECT blob_id FROM documents WHERE source=?', (source,)).fetchone()
            if previous:
                if previous['blob_id'] != bid:
                    raise LibraryError('source_changed')
                return {'status': 'unchanged', 'document': self.show(did)}
            if self.db.execute('SELECT count(*) FROM documents').fetchone()[0] >= MAX_DOCUMENTS:
                raise LibraryError('capacity')
            self.db.execute('INSERT OR IGNORE INTO blobs VALUES (?,?)', (bid, raw))
            self.db.execute('INSERT INTO documents VALUES (?,?,?,?,?)',
                            (did, sid, source, bid, source.rsplit('/', 1)[-1]))
            return {'status': 'imported', 'document': self.show(did)}

        def insert(self, source, raw):
            with self._write():
                return self._insert_document(source, raw)

        def _insert_batch_document(self, source, raw):
            return self._insert_document(source, raw)

        def _job_row(self, job_id):
            check_job_id(job_id)
            row = self.db.execute('SELECT * FROM jobs WHERE job_id=?', (job_id,)).fetchone()
            if row is None:
                raise LibraryError('not_found')
            return row

        def _job(self, row):
            return {key: row[key] for key in ('job_id', 'epoch', 'state', 'total', 'completed', 'error')}

        def _fenced(self, job_id, epoch):
            check_epoch(epoch)
            row = self._job_row(job_id)
            if row['epoch'] != epoch:
                raise LibraryError('stale_epoch')
            return row

        def create_job(self, job_id, entries):
            check_job_id(job_id)
            entries = canonical_manifest(entries)
            manifest = json.dumps(entries, ensure_ascii=True, sort_keys=True, separators=(',', ':'))
            hashes = []
            for item in entries:
                try:
                    hashes.append(hashlib.sha256(item['text'].encode('utf-8')).hexdigest())
                except UnicodeError:
                    hashes.append(None)  # Invalid UTF-8 is a deferred semantic error.
            with self._write():
                previous = self.db.execute('SELECT * FROM jobs WHERE job_id=?', (job_id,)).fetchone()
                if previous is not None:
                    if previous['manifest'] != manifest:
                        raise LibraryError('job_conflict')
                    return self._job(previous)
                self.db.execute('INSERT INTO jobs VALUES (?,1,\'queued\',?,0,NULL,?,?,NULL)',
                                (job_id, len(entries), manifest, json.dumps(hashes)))
                return self.get_job(job_id)

        def get_job(self, job_id):
            return self._job(self._job_row(job_id))

        def list_jobs(self):
            return [self._job(row) for row in self.db.execute('SELECT * FROM jobs ORDER BY job_id')]

        def job_manifest(self, job_id):
            return json.loads(self._job_row(job_id)['manifest'])

        def start_job(self, job_id, epoch):
            with self._write():
                row = self._fenced(job_id, epoch)
                if row['state'] not in ('queued', 'running'):
                    raise LibraryError('job_state')
                self.db.execute("UPDATE jobs SET state='running' WHERE job_id=?", (job_id,))
                return {'job_id': job_id, 'epoch': epoch}

        def fail_job(self, job_id, epoch, code):
            if code not in ('invalid_source', 'unsupported_type', 'invalid_utf8', 'too_large',
                            'invalid_batch', 'source_changed', 'capacity'):
                raise LibraryError('invalid_request')
            with self._write():
                row = self._fenced(job_id, epoch)
                if row['state'] not in ('queued', 'running'):
                    raise LibraryError('job_state')
                self.db.execute("UPDATE jobs SET state='failed',completed=0,error=?,receipt=NULL WHERE job_id=?",
                                (code, job_id))
                return self.get_job(job_id)

        def cancel_job(self, job_id):
            with self._write():
                row = self._job_row(job_id)
                if row['state'] == 'cancelled':
                    return self._job(row)
                if row['state'] not in ('queued', 'running'):
                    raise LibraryError('job_state')
                self.db.execute("UPDATE jobs SET epoch=epoch+1,state='cancelled',completed=0,error=NULL,receipt=NULL "
                                'WHERE job_id=?', (job_id,))
                return self.get_job(job_id)

        def retry_job(self, job_id):
            with self._write():
                row = self._job_row(job_id)
                if row['state'] not in ('failed', 'cancelled'):
                    raise LibraryError('job_state')
                self.db.execute("UPDATE jobs SET epoch=epoch+1,state='queued',completed=0,error=NULL,receipt=NULL "
                                'WHERE job_id=?', (job_id,))
                return self.get_job(job_id)

        def commit_job(self, job_id, epoch, *, fail_before_commit=False):
            if type(fail_before_commit) is not bool:
                raise LibraryError('invalid_request')
            with self._write():
                row = self._fenced(job_id, epoch)
                if row['state'] == 'completed':
                    return json.loads(row['receipt'])
                if row['state'] != 'running':
                    raise LibraryError('job_state')
                try:
                    entries = validate_entries(json.loads(row['manifest']), self.documents())
                except LibraryError as error:
                    self.db.execute("UPDATE jobs SET state='failed',completed=0,error=?,receipt=NULL WHERE job_id=?",
                                    (error.code, job_id))
                    return {'error': error.code}
                documents = [self._insert_batch_document(entry['source'], entry['text'].encode('utf-8'))['document']
                             for entry in entries]
                self.db.execute("UPDATE jobs SET state='completed',completed=total,error=NULL WHERE job_id=?", (job_id,))
                receipt = {'job': self.get_job(job_id), 'documents': documents}
                self.db.execute('UPDATE jobs SET receipt=? WHERE job_id=?',
                                (json.dumps(receipt, ensure_ascii=True, sort_keys=True), job_id))
                if fail_before_commit:
                    raise LibraryError('injected_failure')
                return receipt
''')


def catalog_files(*, defective_blob_dedup: bool = False) -> dict[str, str]:
    """Catalog patch, optionally injecting wrong document-by-blob identity."""
    store = _STORE
    if defective_blob_dedup:
        needle = "    def _insert_batch_document(self, source, raw):\n"
        injected = (
            "        duplicate = self.db.execute('SELECT document_id FROM documents WHERE blob_id=?', (blob_id(raw),)).fetchone()\n"
            "        if duplicate is not None:\n"
            "            return {'status': 'unchanged', 'document': self.show(duplicate['document_id'])}\n"
        )
        if store.count(needle) != 1:
            raise AssertionError("Defect insertion point changed")
        store = store.replace(needle, needle + injected)
    return {"library/catalog/store.py": store, "library/catalog/validation.py": _VALIDATION}


def m1_files(*, defective_blob_dedup: bool = False) -> dict[str, str]:
    """Return the complete authored M1 app, excluding any private evaluator."""
    from gossip_harness.library_m1_clients_reference_v1 import clients_files
    from gossip_harness.library_m1_ingestion_reference_v1 import ingestion_files

    return seed_files() | catalog_files(defective_blob_dedup=defective_blob_dedup) | ingestion_files() | clients_files()


def package_changes(package: str, *, defective_blob_dedup: bool = False) -> dict[str, str]:
    """Only effective changes in one registered package relative to v0."""
    if package not in PACKAGE_SCOPES:
        raise ValueError("Unknown package")
    seed = seed_files()
    return {path: source for path, source in m1_files(defective_blob_dedup=defective_blob_dedup).items()
            if path.startswith(PACKAGE_SCOPES[package]) and seed.get(path) != source}


def catalog_repair_changes() -> dict[str, str]:
    """Exact repair to the controlled wrong catalog proposal."""
    return {"library/catalog/store.py": _STORE}
