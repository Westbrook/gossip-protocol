"""Trusted authored M2 catalog overlay, never private acceptance or model input.

The inherited M1 implementation is retained byte-for-byte in a generated legacy
module. New durable product behavior is implemented in the generated Store.
"""
from __future__ import annotations

from textwrap import dedent

from gossip_harness.library_m1_reference_v1 import catalog_files as m1_catalog_files


_STORE = dedent(r'''
    """Atomic lifecycle catalog layered over the original durable M1 job store."""
    from contextlib import contextmanager
    import json
    import sqlite3
    import unicodedata

    from library.catalog.legacy_m1 import Store as LegacyStore
    from library.catalog.validation import validate_entries
    from library.common import LibraryError, MAX_DOCUMENTS, MAX_FILE_BYTES, blob_id, identity, page, source_key

    def _integer(value, minimum=0):
        if type(value) is not int or value < minimum:
            raise LibraryError('invalid_request')
        return value

    def _utf8(value, limit):
        if type(value) is not str:
            raise LibraryError('invalid_request')
        try:
            raw = value.encode('utf-8')
        except UnicodeError as error:
            raise LibraryError('invalid_utf8') from error
        if len(raw) > limit:
            raise LibraryError('too_large')
        return raw

    def _name(value):
        if type(value) is not str:
            raise LibraryError('invalid_request')
        _utf8(value, 1 << 30)
        value = unicodedata.normalize('NFC', unicodedata.normalize('NFC', value).strip().casefold())
        if (not value or len(value.encode('utf-8')) > 64
                or any(ord(char) < 32 or ord(char) == 127 for char in value)):
            raise LibraryError('invalid_request')
        return value

    def _names(values, limit):
        if type(values) is not list or len(values) > limit:
            raise LibraryError('invalid_request')
        names = [_name(value) for value in values]
        if len(set(names)) != len(names):
            raise LibraryError('invalid_request')
        return sorted(names)

    def _json(value):
        return json.dumps(value, ensure_ascii=True, separators=(',', ':'))

    class Store(LegacyStore):
        def __init__(self, path):
            self.path = str(path)
            self.db = sqlite3.connect(self.path, timeout=5, isolation_level=None)
            self.db.row_factory = sqlite3.Row
            self.db.execute('PRAGMA foreign_keys=ON')
            try:
                with self._write():
                    # execute(), not executescript(): the latter commits an open
                    # transaction and would expose a partially migrated catalog.
                    for statement in (
                        'CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY,value TEXT NOT NULL)',
                        "INSERT OR IGNORE INTO metadata VALUES ('schema','0')",
                        'CREATE TABLE IF NOT EXISTS blobs (blob_id TEXT PRIMARY KEY,content BLOB NOT NULL)',
                        'CREATE TABLE IF NOT EXISTS documents (document_id TEXT PRIMARY KEY,'
                        'source_id TEXT UNIQUE NOT NULL,source TEXT UNIQUE NOT NULL,'
                        'blob_id TEXT NOT NULL REFERENCES blobs(blob_id),title TEXT NOT NULL)',
                        'CREATE TABLE IF NOT EXISTS jobs (job_id TEXT PRIMARY KEY,epoch INTEGER NOT NULL,'
                        'state TEXT NOT NULL,total INTEGER NOT NULL,completed INTEGER NOT NULL,error TEXT,'
                        'manifest TEXT NOT NULL,content_hashes TEXT NOT NULL,receipt TEXT)',
                    ):
                        self.db.execute(statement)
                    schema = self.db.execute("SELECT value FROM metadata WHERE key='schema'").fetchone()[0]
                    if schema not in ('0', '2'):
                        raise LibraryError('invalid_schema')
                    if schema == '0':
                        self.db.execute('CREATE TABLE lifecycle ('
                            'document_id TEXT PRIMARY KEY REFERENCES documents(document_id),'
                            'current_revision INTEGER NOT NULL,edit_version INTEGER NOT NULL,'
                            'deleted INTEGER NOT NULL,notes TEXT NOT NULL,tags TEXT NOT NULL,collections TEXT NOT NULL)')
                        self.db.execute('CREATE TABLE revisions (document_id TEXT NOT NULL REFERENCES documents(document_id),'
                            'revision INTEGER NOT NULL,blob_id TEXT NOT NULL REFERENCES blobs(blob_id),'
                            'PRIMARY KEY(document_id,revision))')
                        self.db.execute('CREATE TABLE collections (name TEXT PRIMARY KEY)')
                        self.db.execute("INSERT INTO lifecycle SELECT document_id,1,1,0,'','[]','[]' FROM documents")
                        self.db.execute('INSERT INTO revisions SELECT document_id,1,blob_id FROM documents')
                        self.db.execute("INSERT OR REPLACE INTO metadata VALUES ('catalog_generation','0')")
                        self.db.execute("UPDATE metadata SET value='2' WHERE key='schema'")
            except BaseException:
                self.db.close()
                raise

        @contextmanager
        def _write(self):
            self.db.execute('BEGIN IMMEDIATE')
            self._catalog_changed = False
            try:
                yield
                if self._catalog_changed:
                    self._capacity()
                    self.db.execute("UPDATE metadata SET value=? WHERE key='catalog_generation'",
                                    (str(self._generation() + 1),))
                self.db.commit()
            except BaseException:
                self.db.rollback()
                raise
            finally:
                self._catalog_changed = False

        @contextmanager
        def _read(self):
            own = not self.db.in_transaction
            if own:
                self.db.execute('BEGIN')
            try:
                yield
                if own:
                    self.db.commit()
            except BaseException:
                if own:
                    self.db.rollback()
                raise

        def _generation(self):
            return int(self.db.execute("SELECT value FROM metadata WHERE key='catalog_generation'").fetchone()[0])

        def _capacity(self):
            size = self.db.execute('SELECT COALESCE(SUM(length(content)),0) FROM blobs WHERE blob_id IN '
                                   '(SELECT blob_id FROM revisions)').fetchone()[0]
            if size > 16777216:
                raise LibraryError('capacity')

        def all_documents(self):
            # Admission must include tombstones; only the public legacy listing
            # hides them. Each returned dictionary is freshly decoded.
            return super().documents()

        def documents(self):
            rows = self.db.execute('SELECT d.*,b.content FROM documents d JOIN blobs b USING(blob_id) '
                                   'JOIN lifecycle l USING(document_id) WHERE l.deleted=0')
            return sorted((self._document(row) for row in rows), key=lambda doc: (doc['source'],doc['document_id']))

        def show(self, document_id):
            with self._read():
                record = self._record(document_id)
                if record['deleted']:
                    raise LibraryError('not_found')
                return record['document']

        def _record(self, document_id):
            if type(document_id) is not str:
                raise LibraryError('invalid_request')
            _utf8(document_id, 1 << 30)
            row = self.db.execute('SELECT d.*,b.content,l.current_revision,l.edit_version,l.deleted,l.notes,l.tags,l.collections '
                                  'FROM documents d JOIN blobs b USING(blob_id) JOIN lifecycle l USING(document_id) '
                                  'WHERE document_id=?',(document_id,)).fetchone()
            if row is None:
                raise LibraryError('not_found')
            return {'document':self._document(row),'revision':row['current_revision'],
                    'edit_version':row['edit_version'],'deleted':bool(row['deleted']),'notes':row['notes'],
                    'tags':json.loads(row['tags']),'collections':json.loads(row['collections'])}

        def lifecycle_show(self, document_id):
            with self._read():
                return self._record(document_id)

        def revision_history(self, document_id):
            with self._read():
                self._record(document_id)
                rows = self.db.execute('SELECT r.revision,r.blob_id,b.content FROM revisions r JOIN blobs b USING(blob_id) '
                                       'WHERE document_id=? ORDER BY r.revision',(document_id,))
                return {'document_id':document_id,'revisions':[
                    {'revision':row['revision'],'blob_id':row['blob_id'],'text':bytes(row['content']).decode('utf-8')}
                    for row in rows]}

        def lifecycle_list(self, query='', *, tag=None, collection=None, deleted='active',offset=0,limit=100,generation=None):
            page(offset,limit)
            if type(query) is not str or len(query) > 256 or type(deleted) is not str or deleted not in ('active','deleted','all'):
                raise LibraryError('invalid_request')
            _utf8(query, 1 << 30)
            tag = None if tag is None else _name(tag)
            collection = None if collection is None else _name(collection)
            if generation is not None:
                _integer(generation)
            with self._read():
                current_generation = self._generation()
                if generation is not None and generation != current_generation:
                    raise LibraryError('stale_generation')
                records = []
                for doc in self.all_documents():
                    record = self._record(doc['document_id'])
                    if deleted != 'all' and record['deleted'] != (deleted == 'deleted'):
                        continue
                    if tag is not None and tag not in record['tags']:
                        continue
                    if collection is not None and collection not in record['collections']:
                        continue
                    if query.casefold() not in (doc['source']+'\n'+doc['text']).casefold():
                        continue
                    records.append(record)
                return {'records':records[offset:offset+limit],'total':len(records),'generation':current_generation}

        def _editable(self, document_id, expected_version, *, allow_deleted=False):
            record = self._record(document_id)
            if record['edit_version'] != expected_version:
                raise LibraryError('stale_version')
            if record['deleted'] and not allow_deleted:
                raise LibraryError('document_deleted')
            return record

        def _insert_document(self, source, raw):
            source_key(source)
            if type(raw) is not bytes:
                raise LibraryError('invalid_request')
            if len(raw) > MAX_FILE_BYTES:
                raise LibraryError('too_large')
            try:
                raw.decode('utf-8')
            except UnicodeError as error:
                raise LibraryError('invalid_utf8') from error
            did,sid,bid = identity('document',source),identity('source',source),blob_id(raw)
            previous = self.db.execute('SELECT blob_id FROM documents WHERE source=?',(source,)).fetchone()
            if previous:
                if previous['blob_id'] != bid:
                    raise LibraryError('source_changed')
                return {'status':'unchanged','document':self._record(did)['document']}
            if self.db.execute('SELECT count(*) FROM documents').fetchone()[0] >= MAX_DOCUMENTS:
                raise LibraryError('capacity')
            self.db.execute('INSERT OR IGNORE INTO blobs VALUES (?,?)',(bid,raw))
            self.db.execute('INSERT INTO documents VALUES (?,?,?,?,?)',(did,sid,source,bid,source.rsplit('/',1)[-1]))
            self.db.execute("INSERT INTO lifecycle VALUES (?,1,1,0,'','[]','[]')",(did,))
            self.db.execute('INSERT INTO revisions VALUES (?,1,?)',(did,bid))
            self._catalog_changed = True
            return {'status':'imported','document':self._record(did)['document']}

        def refresh_document(self, document_id, expected_version, *, text):
            _integer(expected_version,1)
            raw = _utf8(text,MAX_FILE_BYTES)
            with self._write():
                record = self._editable(document_id,expected_version)
                bid = blob_id(raw)
                if record['document']['blob_id'] == bid:
                    status = 'unchanged'
                else:
                    if record['revision'] >= 16:
                        raise LibraryError('revision_capacity')
                    self.db.execute('INSERT OR IGNORE INTO blobs VALUES (?,?)',(bid,raw))
                    self.db.execute('INSERT INTO revisions VALUES (?,?,?)',(document_id,record['revision']+1,bid))
                    self.db.execute('UPDATE documents SET blob_id=? WHERE document_id=?',(bid,document_id))
                    self.db.execute('UPDATE lifecycle SET current_revision=current_revision+1,edit_version=edit_version+1 '
                                    'WHERE document_id=?',(document_id,))
                    self._catalog_changed = True
                    status = 'refreshed'
                return {'status':status,'record':self._record(document_id)}

        def replace_annotations(self, document_id, expected_version, notes, tags, collections):
            _integer(expected_version,1)
            _utf8(notes,16384)
            tags,collections = _names(tags,32),_names(collections,16)
            with self._write():
                record = self._editable(document_id,expected_version)
                available = {row[0] for row in self.db.execute('SELECT name FROM collections')}
                if set(collections)-available:
                    raise LibraryError('collection_not_found')
                if (notes,tags,collections) == (record['notes'],record['tags'],record['collections']):
                    status = 'unchanged'
                else:
                    self.db.execute('UPDATE lifecycle SET notes=?,tags=?,collections=?,edit_version=edit_version+1 '
                                    'WHERE document_id=?',(notes,_json(tags),_json(collections),document_id))
                    self._catalog_changed = True
                    status = 'updated'
                return {'status':status,'record':self._record(document_id)}

        def _transition(self, document_id, expected_version, deleted):
            _integer(expected_version,1)
            with self._write():
                record = self._editable(document_id,expected_version,allow_deleted=True)
                status = 'unchanged'
                if record['deleted'] != deleted:
                    self.db.execute('UPDATE lifecycle SET deleted=?,edit_version=edit_version+1 WHERE document_id=?',
                                    (int(deleted),document_id))
                    self._catalog_changed = True
                    status = 'deleted' if deleted else 'restored'
                return {'status':status,'record':self._record(document_id)}

        def delete_document(self, document_id, expected_version):
            return self._transition(document_id,expected_version,True)

        def restore_document(self, document_id, expected_version):
            return self._transition(document_id,expected_version,False)

        def list_collections(self):
            with self._read():
                totals = {row[0]:0 for row in self.db.execute('SELECT name FROM collections')}
                for row in self.db.execute('SELECT collections FROM lifecycle'):
                    for name in json.loads(row[0]):
                        totals[name] += 1
                return {'collections':[{'name':name,'total':totals[name]} for name in sorted(totals)],
                        'generation':self._generation()}

        def _collection(self, name, expected_generation, *, remove):
            name = _name(name)
            _integer(expected_generation)
            with self._write():
                generation = self._generation()
                if generation != expected_generation:
                    raise LibraryError('stale_generation')
                exists = self.db.execute('SELECT 1 FROM collections WHERE name=?',(name,)).fetchone() is not None
                if remove:
                    if not exists:
                        raise LibraryError('not_found')
                    if any(name in json.loads(row[0]) for row in self.db.execute('SELECT collections FROM lifecycle')):
                        raise LibraryError('collection_not_empty')
                    self.db.execute('DELETE FROM collections WHERE name=?',(name,))
                    status = 'removed'
                elif exists:
                    return {'status':'unchanged','name':name,'generation':generation}
                else:
                    if self.db.execute('SELECT count(*) FROM collections').fetchone()[0] >= 64:
                        raise LibraryError('capacity')
                    self.db.execute('INSERT INTO collections VALUES (?)',(name,))
                    status = 'created'
                self._catalog_changed = True
                return {'status':status,'name':name,'generation':generation+1}

        def create_collection(self, name, expected_generation):
            return self._collection(name,expected_generation,remove=False)

        def remove_collection(self, name, expected_generation):
            return self._collection(name,expected_generation,remove=True)

        def commit_job(self, job_id, epoch, *, fail_before_commit=False):
            if type(fail_before_commit) is not bool:
                raise LibraryError('invalid_request')
            with self._write():
                row = self._fenced(job_id,epoch)
                if row['state'] == 'completed':
                    return json.loads(row['receipt'])
                if row['state'] != 'running':
                    raise LibraryError('job_state')
                try:
                    entries = validate_entries(json.loads(row['manifest']),self.all_documents())
                    # Savepoint makes capacity failures a terminal job error
                    # while rolling back every tentative document/blob insert.
                    self.db.execute('SAVEPOINT batch_documents')
                    changed_before = self._catalog_changed
                    documents = [self._insert_batch_document(entry['source'],entry['text'].encode('utf-8'))['document']
                                 for entry in entries]
                    self._capacity()
                    self.db.execute('RELEASE batch_documents')
                except LibraryError as error:
                    if 'changed_before' in locals():
                        self.db.execute('ROLLBACK TO batch_documents')
                        self.db.execute('RELEASE batch_documents')
                        self._catalog_changed = changed_before
                    self.db.execute("UPDATE jobs SET state='failed',completed=0,error=?,receipt=NULL WHERE job_id=?",
                                    (error.code,job_id))
                    return {'error':error.code}
                self.db.execute("UPDATE jobs SET state='completed',completed=total,error=NULL WHERE job_id=?",(job_id,))
                receipt = {'job':self.get_job(job_id),'documents':documents}
                self.db.execute('UPDATE jobs SET receipt=? WHERE job_id=?',
                                (json.dumps(receipt,ensure_ascii=True,sort_keys=True),job_id))
                if fail_before_commit:
                    raise LibraryError('injected_failure')
                return receipt
''').lstrip('\n').rstrip() + '\n'


def catalog_files() -> dict[str, str]:
    """Return new M2 catalog sources while retaining original M1 code verbatim."""
    inherited = m1_catalog_files()
    return {
        "library/catalog/legacy_m1.py": inherited["library/catalog/store.py"],
        "library/catalog/store.py": _STORE,
    }
