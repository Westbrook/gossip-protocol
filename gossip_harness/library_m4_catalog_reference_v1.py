"""Authored normalized M4 catalog and atomic, validated migration.

Generated implementation is reference qualification, not independent acceptance
or evidence about agent coordination. Frozen M1--M3 sources stay unchanged.
"""
from __future__ import annotations

from textwrap import dedent

from .library_m2_catalog_reference_v1 import catalog_files as m2_catalog_files
from .library_m3_control_reference_v1 import control_files
from .library_m3_maintenance_reference_v1 import maintenance_files


_STORE = dedent(r'''
    """Normalized revision identity with compatible numbered projections."""
    import hashlib
    import json
    import os
    from pathlib import Path
    import sqlite3
    import stat
    import uuid

    from library.catalog.m4_control import CoreStore, _directory
    from library.catalog.legacy_m2 import _integer, _utf8
    from library.catalog.maintenance import canonical_export_bytes
    from library.catalog.m4_validation import inspect_database
    from library.common import LibraryError, MAX_DOCUMENTS, MAX_FILE_BYTES, blob_id, identity, source_key

    def revision_id(document_id, revision, blob_id):
        return 'rev-' + hashlib.sha256(b'revision\0' + document_id.encode('ascii') + b'\0' +
            str(revision).encode('ascii') + b'\0' + blob_id.encode('ascii')).hexdigest()

    def project_revision4(document_id, revision):
        return {'revision_id':revision_id(document_id, revision['revision'], revision['blob_id']), **revision}

    def project_record4(record):
        doc = record['document']
        return {key:doc[key] for key in ('document_id','source_id','source','title')} | {
            'current_revision':project_revision4(doc['document_id'],
                {'revision':record['revision'], 'blob_id':doc['blob_id'], 'text':doc['text']}),
            **{key:record[key] for key in ('edit_version','deleted','notes','tags','collections')}}

    _BASE = (
        'CREATE TABLE metadata (key TEXT PRIMARY KEY,value TEXT NOT NULL)',
        'CREATE TABLE blobs (blob_id TEXT PRIMARY KEY,content BLOB NOT NULL)',
        'CREATE TABLE documents (document_id TEXT PRIMARY KEY,source_id TEXT UNIQUE NOT NULL,'
        'source TEXT UNIQUE NOT NULL,blob_id TEXT NOT NULL REFERENCES blobs(blob_id),title TEXT NOT NULL)',
        'CREATE TABLE jobs (job_id TEXT PRIMARY KEY,epoch INTEGER NOT NULL,state TEXT NOT NULL,'
        'total INTEGER NOT NULL,completed INTEGER NOT NULL,error TEXT,manifest TEXT NOT NULL,'
        'content_hashes TEXT NOT NULL,receipt TEXT)',
        'CREATE TABLE collections (name TEXT PRIMARY KEY)',
    )
    _NORMALIZED = (
        'CREATE TABLE document_revisions (revision_id TEXT PRIMARY KEY,'
        'document_id TEXT NOT NULL REFERENCES documents(document_id),revision INTEGER NOT NULL,'
        'blob_id TEXT NOT NULL REFERENCES blobs(blob_id),UNIQUE(document_id,revision))',
        'CREATE TABLE document_state (document_id TEXT PRIMARY KEY REFERENCES documents(document_id),'
        'head_revision_id TEXT NOT NULL REFERENCES document_revisions(revision_id),'
        'edit_version INTEGER NOT NULL,deleted INTEGER NOT NULL,notes TEXT NOT NULL,tags TEXT NOT NULL,collections TEXT NOT NULL)',
    )

    def _fence_statements():
        statements = {}
        for table, field, target, key, high, extra in (
            ('jobs','epoch','job_control','job_id','epoch_high_water',',0'),
            ('document_state','edit_version','document_control','document_id','edit_high_water',''),
        ):
            for event in ('INSERT','UPDATE'):
                name = 'fence_'+table+'_'+event.lower()
                statements[name] = ('CREATE TRIGGER '+name+' AFTER '+event+' ON '+table+
                    ' BEGIN INSERT INTO '+target+' VALUES (NEW.'+key+',NEW.'+field+extra+') '
                    'ON CONFLICT('+key+') DO UPDATE SET '+high+'=MAX('+high+',excluded.'+high+'); END')
        return statements

    class NormalizedStore(CoreStore):
        def __init__(self, path, *, backup_dir=None):
            self.path = os.path.abspath(path)
            _directory(Path(self.path).parent)
            try:
                mode = os.lstat(self.path).st_mode
            except FileNotFoundError:
                existed = False
            else:
                existed = True
                if not stat.S_ISREG(mode):
                    raise LibraryError('invalid_source')
            # Read-only preflight cannot repair a corrupt input, change journal
            # mode, or leave a partially initialized table in an existing file.
            initial_schema = None
            if existed:
                connection = None
                try:
                    uri = Path(self.path).as_uri() + '?mode=ro'
                    connection = sqlite3.connect(uri, uri=True, isolation_level=None)
                    connection.row_factory = sqlite3.Row
                    connection.execute('BEGIN')
                    initial_schema, _ = inspect_database(connection)
                    connection.rollback()
                except LibraryError as error:
                    cause = error.__cause__
                    if not (isinstance(cause,sqlite3.Error) and
                            getattr(cause,'sqlite_errorcode',None) == sqlite3.SQLITE_READONLY_ROLLBACK):
                        raise
                    # A genuine hot rollback journal must be recovered before
                    # any reader can see committed data. Defer this one SQLite
                    # recovery condition to the exclusive maintenance path;
                    # no schema mutation occurs before its graph validation.
                except sqlite3.Error as error:
                    raise LibraryError('invalid_database') from error
                finally:
                    if connection is not None:
                        connection.close()
            self.maintenance_root = Path(self.path + '.maintenance')
            try:
                self.maintenance_root.mkdir(mode=0o700, exist_ok=True)
            except OSError as error:
                raise LibraryError('io_error') from error
            _directory(self.maintenance_root)
            self.backup_dir = (Path(self.path).parent / 'backups' if backup_dir is None else _directory(backup_dir))
            self._bootstrapping = True
            self._maintenance_depth = 0
            self._maintenance_exclusive = False
            self._worker_claim = None
            self._savepoint_counter = 0
            self._incarnation = None
            self._catalog_changed = False
            self._pending_migration = None
            self.db = sqlite3.connect(self.path, timeout=5, isolation_level=None)
            self.db.row_factory = sqlite3.Row
            self.db.execute('PRAGMA foreign_keys=ON')
            try:
                if initial_schema == 4:
                    # A normal open is a reader, so it can coexist with a live
                    # worker. Explicit migration still requires exclusivity.
                    with self._maintenance_shared(), self._read():
                        schema, payload = inspect_database(self.db)
                        if schema != 4:
                            raise LibraryError('invalid_database')
                        self._pending_migration = self._migration_result(schema, False, payload)
                        guards_current = (self._fences_current() and self.db.execute(
                            "SELECT 1 FROM control WHERE key='backup_root'").fetchone() is not None)
                    if not guards_current:
                        # Triggers are not part of portable schema admission.
                        # Install our write guarantees only after validating a
                        # legal input and acquiring exclusive writer authority.
                        with self.maintenance_authority():
                            self.db.execute('BEGIN IMMEDIATE')
                            try:
                                inspect_database(self.db)
                                self._install_fences()
                                self._initialize_backup_binding()
                                self.db.commit()
                            except BaseException:
                                self.db.rollback()
                                raise
                else:
                    with self.maintenance_authority():
                        self._pending_migration = self._migrate_storage(existed)
                self._bootstrapping = False
                self.adopt_incarnation()
                # Opening/migrating a copied database must preserve its backup
                # registry and ownership binding. Selected filesystem roots
                # are configuration, not permission to erase durable rows.
            except BaseException:
                self.db.close()
                raise

        def _fences_current(self):
            existing = {row[0]:row[1] for row in self.db.execute(
                "SELECT name,sql FROM sqlite_master WHERE type='trigger'")}
            return all(existing.get(name) == statement for name,statement in _fence_statements().items())

        def _install_fences(self):
            if not self.db.in_transaction:
                raise LibraryError('invalid_request')
            existing = {row[0]:row[1] for row in self.db.execute(
                "SELECT name,sql FROM sqlite_master WHERE type='trigger'")}
            for name,statement in _fence_statements().items():
                if existing.get(name) != statement:
                    self.db.execute('DROP TRIGGER IF EXISTS '+name)
                    self.db.execute(statement)

        def _initialize_backup_binding(self):
            if not self.db.in_transaction:
                raise LibraryError('invalid_request')
            if self.db.execute("SELECT 1 FROM control WHERE key='backup_root'").fetchone() is None:
                inherited = (self.db.execute("SELECT 1 FROM maintenance_artifacts WHERE root_kind='backup' LIMIT 1").fetchone()
                             or self.db.execute('SELECT 1 FROM backups LIMIT 1').fetchone())
                self._set_control('backup_root','' if inherited else str(self.backup_dir))

        def configure_backup_dir(self, path):
            if path is None:
                # The public default is still beside this database, including
                # a relocated copy. If that differs from its durable ownership
                # root, actual backup I/O fails closed until an operator makes
                # an explicit root choice; open/list/migrate remain read-only.
                self.backup_dir = Path(self.path).parent / 'backups'
                return
            selected = _directory(path)
            if self._control('backup_root') == '':
                # Portable schema3 has no required absolute backup-root key.
                # Only an explicit operator directory choice can bind inherited
                # relative ownership; adoption preserves all registered rows.
                with self._lock('backup-root.lock'), self._write():
                    if self._control('backup_root') == '':
                        self._set_control('backup_root',str(selected))
                        self.backup_dir = selected
                        return
            return super().configure_backup_dir(path)

        def _migration_checkpoint(self, phase):
            # Authored process-crash qualification overrides this seam. Normal
            # operation has no environment-driven or user-controlled injector.
            pass

        def _migration_result(self, schema, changed, payload):
            return {'from_schema':schema, 'to_schema':4, 'migrated':changed,
                    'documents':len(payload['documents']), 'jobs':len(payload['jobs'])}

        def _migrate_storage(self, existed):
            self.db.execute('BEGIN IMMEDIATE')
            try:
                if existed:
                    schema, payload = inspect_database(self.db)
                else:
                    # A concurrent creator must not be mistaken for an empty
                    # new database. Re-inspect if it has initialized anything.
                    if self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' LIMIT 1").fetchone():
                        schema, payload = inspect_database(self.db)
                        existed = True
                    else:
                        schema = 4
                        payload = {'documents':[], 'jobs':[], 'revisions':[], 'generation':0}
                if existed and schema == 4:
                    # Hot-journal recovery and concurrent initialization can
                    # enter here without the ordinary schema4-open branch.
                    self._install_fences()
                    self._initialize_backup_binding()
                    self.db.commit()
                    return self._migration_result(4, False, payload)
                if not existed:
                    for statement in _BASE:
                        self.db.execute(statement)
                    self.db.execute("INSERT INTO metadata VALUES ('catalog_generation','0')")
                elif schema == 0:
                    tables = {row[0] for row in self.db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                    if 'jobs' not in tables:
                        self.db.execute(_BASE[3])
                    self.db.execute(_BASE[4])
                    self.db.execute("INSERT INTO metadata VALUES ('catalog_generation','0')")
                for statement in _NORMALIZED:
                    self.db.execute(statement)
                for row in payload['revisions']:
                    self.db.execute('INSERT INTO document_revisions VALUES (?,?,?,?)',
                        (revision_id(row['document_id'],row['revision'],row['blob_id']),
                         row['document_id'],row['revision'],row['blob_id']))
                for record in payload['documents']:
                    did, bid = record['document']['document_id'], record['document']['blob_id']
                    if schema == 0:
                        tags = collections = '[]'
                    else:
                        old = self.db.execute('SELECT tags,collections FROM lifecycle WHERE document_id=?',(did,)).fetchone()
                        tags, collections = old['tags'], old['collections']
                    self.db.execute('INSERT INTO document_state VALUES (?,?,?,?,?,?,?)',
                        (did,revision_id(did,record['revision'],bid),record['edit_version'],
                         int(record['deleted']),record['notes'],tags,collections))
                if existed and schema in (2,3):
                    self.db.execute('DROP TABLE lifecycle')
                    self.db.execute('DROP TABLE revisions')
                self._initialize_control()
                self._install_fences()
                self._initialize_backup_binding()
                if existed and schema == 3:
                    # A held M3 Store checks this before its first legacy SQL
                    # write, so it cannot write the removed physical layout.
                    self._set_control('incarnation',uuid.uuid4().hex)
                # Validate transformed graph before the schema marker becomes
                # visible. Explicit override only informs our read-only verifier.
                _, after = inspect_database(self.db, schema_override=4)
                if after != payload and existed:
                    raise LibraryError('invalid_database')
                self.db.execute("INSERT INTO metadata VALUES ('schema','4') ON CONFLICT(key) DO UPDATE SET value='4'")
                self._migration_checkpoint('before_commit')
                self.db.commit()
            except BaseException:
                self.db.rollback()
                raise
            self._migration_checkpoint('after_commit')
            return self._migration_result(schema, existed, after)

        def migrate(self):
            with self.maintenance_authority(), self._read():
                schema, payload = inspect_database(self.db)
                if schema != 4:
                    raise LibraryError('invalid_database')
                result = self._pending_migration
                self._pending_migration = None
                return dict(result) if result is not None else self._migration_result(4,False,payload)

        def _record(self, document_id):
            if type(document_id) is not str:
                raise LibraryError('invalid_request')
            _utf8(document_id,1 << 30)
            row = self.db.execute('SELECT d.*,b.content,r.revision,s.edit_version,s.deleted,s.notes,s.tags,s.collections '
                'FROM documents d JOIN blobs b USING(blob_id) JOIN document_state s USING(document_id) '
                'JOIN document_revisions r ON r.revision_id=s.head_revision_id '
                'WHERE d.document_id=?',(document_id,)).fetchone()
            if row is None:
                raise LibraryError('not_found')
            return {'document':self._document(row),'revision':row['revision'],
                'edit_version':row['edit_version'],'deleted':bool(row['deleted']),'notes':row['notes'],
                'tags':json.loads(row['tags']),'collections':json.loads(row['collections'])}

        def revision_history(self, document_id):
            with self._read():
                self._record(document_id)
                rows = self.db.execute('SELECT r.revision,r.blob_id,b.content FROM document_revisions r '
                    'JOIN blobs b USING(blob_id) WHERE document_id=? ORDER BY r.revision',(document_id,))
                return {'document_id':document_id,'revisions':[
                    {'revision':row['revision'],'blob_id':row['blob_id'],'text':bytes(row['content']).decode('utf-8')}
                    for row in rows]}

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
            high = self.db.execute('SELECT edit_high_water FROM document_control WHERE document_id=?',(did,)).fetchone()
            edit = high[0]+1 if high is not None else 1
            rid = revision_id(did,1,bid)
            self.db.execute('INSERT OR IGNORE INTO blobs VALUES (?,?)',(bid,raw))
            self.db.execute('INSERT INTO documents VALUES (?,?,?,?,?)',(did,sid,source,bid,source.rsplit('/',1)[-1]))
            self.db.execute('INSERT INTO document_revisions VALUES (?,?,1,?)',(rid,did,bid))
            self.db.execute("INSERT INTO document_state VALUES (?,?,?,0,'','[]','[]')",(did,rid,edit))
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
                    number = record['revision']+1
                    rid = revision_id(document_id,number,bid)
                    self.db.execute('INSERT OR IGNORE INTO blobs VALUES (?,?)',(bid,raw))
                    self.db.execute('INSERT INTO document_revisions VALUES (?,?,?,?)',(rid,document_id,number,bid))
                    self.db.execute('UPDATE documents SET blob_id=? WHERE document_id=?',(bid,document_id))
                    self.db.execute('UPDATE document_state SET head_revision_id=?,edit_version=edit_version+1 '
                        'WHERE document_id=?',(rid,document_id))
                    self._catalog_changed = True
                    status = 'refreshed'
                return {'status':status,'record':self._record(document_id)}

        def list_v1(self, query='', *, tag=None, collection=None, deleted='active',offset=0,limit=100,generation=None):
            result = self.lifecycle_list(query,tag=tag,collection=collection,deleted=deleted,
                offset=offset,limit=limit,generation=generation)
            return {**result,'records':[project_record4(record) for record in result['records']]}

        def show_v1(self, document_id):
            return project_record4(self.lifecycle_show(document_id))

        def revisions_v1(self, document_id, revision_id=None):
            if revision_id is not None:
                if type(revision_id) is not str:
                    raise LibraryError('invalid_request')
                _utf8(revision_id,1 << 30)
            history = self.revision_history(document_id)
            revisions = [project_revision4(document_id,row) for row in history['revisions']]
            if revision_id is not None:
                for row in revisions:
                    if row['revision_id'] == revision_id:
                        return row
                raise LibraryError('not_found')
            return {'document_id':document_id,'revisions':revisions}

        def export_v1(self, ids=None, *, include_deleted=False,include_history=False,max_bytes=16777216):
            # Let inherited export perform all argument/selection checks in the
            # same snapshot. Its content has a lower bound on v4 output bytes.
            with self._read():
                old = self.export_bundle(ids,include_deleted=include_deleted,
                    include_history=include_history,max_bytes=max_bytes)
                result = {'format':'local-research-library-export-v4','generation':old['generation'],'documents':[]}
                size = len(canonical_export_bytes(result))
                if size > max_bytes:
                    raise LibraryError('too_large')
                for row in old['documents']:
                    did = row['record']['document']['document_id']
                    item = {'record':project_record4(row['record']),
                        'revisions':[project_revision4(did,revision) for revision in row['revisions']]}
                    size += len(canonical_export_bytes(item)) + bool(result['documents'])
                    if size > max_bytes:
                        raise LibraryError('too_large')
                    result['documents'].append(item)
                return result
''').lstrip('\n').rstrip() + '\n'


# Filled by the separately delegated storage-validation implementation. This
# helper is authored reference code, distinct from independent frozen fixtures.
_VALIDATION = dedent(r'''
    """Read-only validation of the recognized physical release layouts.

    The caller supplies a SQLite connection inside its migration transaction. No
    Store is opened, no row is repaired, and no filesystem side effect is performed.
    Logical validation deliberately has no serialized-backup byte ceiling.
    """
    import hashlib
    import json
    from pathlib import PurePosixPath
    import re
    import sqlite3

    from library.catalog.backup_format import _validate_payload
    from library.common import LibraryError


    def _bad():
        raise LibraryError('invalid_database')


    def _require(condition):
        if not condition:
            _bad()


    def _integer(value, minimum=0, maximum=None):
        _require(type(value) is int and value >= minimum
                 and (maximum is None or value <= maximum))
        return value


    def _text(value, nonempty=False):
        _require(type(value) is str and (not nonempty or bool(value)))
        value.encode('utf-8')
        return value


    def _decimal(value):
        _text(value)
        _require(re.fullmatch(r'[0-9]+', value) is not None)
        return int(value)


    def _pairs(items):
        result = {}
        for key, value in items:
            _require(key not in result)
            result[key] = value
        return result


    def _constant(value):
        _bad()


    def _json(value):
        _text(value)
        return json.loads(value, object_pairs_hook=_pairs, parse_constant=_constant)


    def _canonical(value):
        return json.dumps(value, ensure_ascii=True, allow_nan=False,
                          sort_keys=True, separators=(',', ':'))


    def _canonical_forms(value):
        # The portable storage contract permits JSON Unicode escapes or literal
        # UTF-8; both forms must remain compact, ordered and semantically valid.
        return (_canonical(value), json.dumps(value, ensure_ascii=False,
                allow_nan=False, sort_keys=True, separators=(',', ':')))


    def revision_identity(document_id, revision, blob_id):
        return 'rev-' + hashlib.sha256(
            b'revision\0' + document_id.encode('ascii') + b'\0'
            + str(revision).encode('ascii') + b'\0'
            + blob_id.encode('ascii')).hexdigest()


    # (column name, declared SQLite type, NOT NULL flag, primary-key ordinal)
    _LAYOUTS = {
        'metadata': [('key','TEXT',0,1), ('value','TEXT',1,0)],
        'blobs': [('blob_id','TEXT',0,1), ('content','BLOB',1,0)],
        'documents': [('document_id','TEXT',0,1), ('source_id','TEXT',1,0),
                      ('source','TEXT',1,0), ('blob_id','TEXT',1,0), ('title','TEXT',1,0)],
        'jobs': [('job_id','TEXT',0,1), ('epoch','INTEGER',1,0), ('state','TEXT',1,0),
                 ('total','INTEGER',1,0), ('completed','INTEGER',1,0), ('error','TEXT',0,0),
                 ('manifest','TEXT',1,0), ('content_hashes','TEXT',1,0), ('receipt','TEXT',0,0)],
        'lifecycle': [('document_id','TEXT',0,1), ('current_revision','INTEGER',1,0),
                      ('edit_version','INTEGER',1,0), ('deleted','INTEGER',1,0),
                      ('notes','TEXT',1,0), ('tags','TEXT',1,0), ('collections','TEXT',1,0)],
        'revisions': [('document_id','TEXT',1,1), ('revision','INTEGER',1,2), ('blob_id','TEXT',1,0)],
        'collections': [('name','TEXT',0,1)],
        'control': [('key','TEXT',0,1), ('value','TEXT',1,0)],
        'job_control': [('job_id','TEXT',0,1), ('epoch_high_water','INTEGER',1,0), ('enrolled','INTEGER',1,0)],
        'document_control': [('document_id','TEXT',0,1), ('edit_high_water','INTEGER',1,0)],
        'maintenance_artifacts': [('artifact_id','TEXT',0,1), ('kind','TEXT',1,0),
                                 ('root_kind','TEXT',1,0), ('relative_path','TEXT',1,0),
                                 ('owner_incarnation','TEXT',1,0), ('owner_generation','INTEGER',1,0),
                                 ('state','TEXT',1,0)],
        'backups': [('name','TEXT',0,1), ('bytes','INTEGER',1,0),
                    ('payload_sha256','TEXT',1,0), ('generation','INTEGER',1,0)],
        'search_state': [('singleton','INTEGER',0,1), ('published_generation','INTEGER',0,0),
                         ('target_generation','INTEGER',0,0), ('processed','INTEGER',1,0),
                         ('total','INTEGER',1,0), ('cursor','TEXT',0,0)],
        'search_entries': [('generation','INTEGER',1,1), ('document_id','TEXT',1,2),
                           ('source','TEXT',1,0), ('text','TEXT',1,0)],
        'document_revisions': [('revision_id','TEXT',0,1), ('document_id','TEXT',1,0),
                               ('revision','INTEGER',1,0), ('blob_id','TEXT',1,0)],
        'document_state': [('document_id','TEXT',0,1), ('head_revision_id','TEXT',1,0),
                           ('edit_version','INTEGER',1,0), ('deleted','INTEGER',1,0),
                           ('notes','TEXT',1,0), ('tags','TEXT',1,0), ('collections','TEXT',1,0)],
    }
    _BASE = {'metadata','blobs','documents'}
    _M2 = {'jobs','lifecycle','revisions','collections'}
    _M3 = {'control','job_control','document_control','maintenance_artifacts',
           'backups','search_state','search_entries'}
    _ERRORS = frozenset(('already_exists','capacity','collection_not_empty','collection_not_found',
        'document_deleted','invalid_archive','invalid_backup','invalid_batch','invalid_database',
        'invalid_json','invalid_request','invalid_schema','invalid_source','invalid_utf8',
        'io_error','job_conflict','job_state','maintenance_busy','not_found','revision_capacity',
        'source_changed','stale_epoch','stale_generation','stale_version','stale_worker',
        'too_large','unsupported_operation','unsupported_schema','unsupported_type','worker_busy'))


    def _rows(db, table):
        # All identifiers originate in the closed layouts above.
        cursor = db.execute('SELECT * FROM "' + table + '"')
        names = [entry[0] for entry in cursor.description]
        return [dict(zip(names, row)) for row in cursor]


    def _mapping(rows, key):
        result = {}
        for row in rows:
            _text(row[key], nonempty=True)
            _require(row[key] not in result)
            result[row[key]] = row
        return result


    def _unique(db, table, columns):
        for index in db.execute('PRAGMA index_list("' + table + '")'):
            # A partial unique index cannot enforce the whole logical table.
            if index[2] and not index[4]:
                escaped = index[1].replace('"','""')
                indexed = tuple(row[2] for row in db.execute('PRAGMA index_info("' + escaped + '")'))
                if indexed == tuple(columns):
                    return
        _bad()


    def _layout(db, table):
        rows = list(db.execute('PRAGMA table_xinfo("' + table + '")'))
        _require([(row[1],row[2].upper(),row[3],row[5]) for row in rows] == _LAYOUTS[table])
        # Hidden/generated columns and defaults are absent in each declared layout.
        _require(all(row[4] is None and row[6] == 0 for row in rows))


    def _controls(db, payload):
        controls = _mapping(_rows(db,'control'), 'key')
        for row in controls.values():
            _text(row['value'])
        _require({'incarnation','worker_generation','last_error','cleanup_cursor'} <= controls.keys())
        _text(controls['incarnation']['value'], nonempty=True)
        generation = _decimal(controls['worker_generation']['value'])
        error_raw = controls['last_error']['value']
        error = _json(error_raw)
        _require(error_raw in _canonical_forms(error))
        if error is not None:
            _require(type(error) is dict and set(error) == {'operation','code'})
            _require(error['operation'] in ('worker','reindex','backup','restore','migrate'))
            _require(type(error['code']) is str and error['code'] in _ERRORS)
        cleanup_raw = controls['cleanup_cursor']['value']
        cleanup = _json(cleanup_raw)
        _require(cleanup_raw in _canonical_forms(cleanup))
        if cleanup is not None:
            _text(cleanup, nonempty=True)
        docs = {row['document']['document_id']:row for row in payload['documents']}
        doc_controls = _mapping(_rows(db,'document_control'),'document_id')
        _require(set(docs) <= doc_controls.keys())
        for did, row in doc_controls.items():
            _require(re.fullmatch(r'doc-[0-9a-f]{64}',did) is not None)
            high = _integer(row['edit_high_water'])
            if did in docs:
                _require(high >= docs[did]['edit_version'])
        jobs = {row['job']['job_id']:row for row in payload['jobs']}
        job_controls = _mapping(_rows(db,'job_control'),'job_id')
        _require(set(jobs) <= job_controls.keys())
        for jid, row in job_controls.items():
            _require(re.fullmatch(r'[A-Za-z0-9_-]{1,64}',jid) is not None)
            high = _integer(row['epoch_high_water'])
            _integer(row['enrolled'],0,1)
            if jid in jobs:
                _require(high >= jobs[jid]['job']['epoch'])
        for row in _mapping(_rows(db,'maintenance_artifacts'),'artifact_id').values():
            _require(row['kind'] in ('backup_stage','restore_stage','index_generation'))
            _require(row['root_kind'] in ('maintenance','backup'))
            _require(row['state'] in ('staging','published','obsolete'))
            _text(row['owner_incarnation'],nonempty=True)
            _integer(row['owner_generation'])
            if row['owner_incarnation'] == controls['incarnation']['value']:
                _require(row['owner_generation'] <= generation)
            path = _text(row['relative_path'],nonempty=True)
            _require('\\' not in path and '\0' not in path and not PurePosixPath(path).is_absolute()
                     and str(PurePosixPath(path)) == path
                     and all(part not in ('','.','..') for part in path.split('/')))
        for row in _mapping(_rows(db,'backups'),'name').values():
            _require(re.fullmatch(r'[A-Za-z0-9_-]{1,64}\.json',row['name']) is not None)
            _integer(row['bytes'],1,67108864)
            _text(row['payload_sha256'])
            _require(re.fullmatch(r'[0-9a-f]{64}',row['payload_sha256']) is not None)
            _integer(row['generation'])
        _search(db,payload)


    def _search(db, payload):
        states = _rows(db,'search_state')
        _require(len(states) == 1)
        state = states[0]
        _require(type(state['singleton']) is int and state['singleton'] == 1)
        published,target = state['published_generation'],state['target_generation']
        allowed = set()
        for number in (published,target):
            if number is not None:
                allowed.add(_integer(number,0,payload['generation']))
        if published is not None and target is not None:
            _require(published <= target)
        processed,total = _integer(state['processed'],0,256),_integer(state['total'],0,256)
        _require(processed <= total)
        cursor = state['cursor']
        if cursor is not None:
            _text(cursor,nonempty=True)
        if target is None:
            _require(processed == total == 0 and cursor is None)
        docs = {row['document']['document_id']:row for row in payload['documents']}
        blobs = {row['blob_id']:row['text'] for row in payload['blobs']}
        history = {}
        for row in payload['revisions']:
            history.setdefault(row['document_id'],set()).add(blobs[row['blob_id']])
        groups = {number:[] for number in allowed}
        seen = set()
        for row in _rows(db,'search_entries'):
            number = _integer(row['generation'])
            did = _text(row['document_id'])
            _require(number in allowed and (number,did) not in seen and did in docs)
            seen.add((number,did))
            _text(row['source'])
            _text(row['text'])
            _require(row['source'] == docs[did]['document']['source'] and row['text'] in history[did])
            groups[number].append(row)
        for group in groups.values():
            group.sort(key=lambda row:(row['source'],row['document_id']))
        if target is not None:
            group = groups[target]
            _require(len(group) == processed)
            _require(cursor == (group[-1]['document_id'] if group else None))
            if processed == total:
                _require(published == target)
            if target == payload['generation']:
                expected = [{'generation':target,'document_id':row['document']['document_id'],
                             'source':row['document']['source'],'text':row['document']['text']}
                            for row in payload['documents'] if not row['deleted']]
                _require(total == len(expected) and group == expected[:processed])
        if published == payload['generation']:
            expected = [{'generation':published,'document_id':row['document']['document_id'],
                         'source':row['document']['source'],'text':row['document']['text']}
                        for row in payload['documents'] if not row['deleted']]
            _require(groups[published] == expected)


    def _inspect_database(db, schema_override=None):
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
        _require('metadata' in tables)
        # Detect an explicitly unknown version before rejecting its other layout.
        if schema_override is None:
            schema_rows = list(db.execute("SELECT value FROM metadata WHERE key='schema'"))
            _require(len(schema_rows) == 1 and type(schema_rows[0][0]) is str)
            raw_schema = schema_rows[0][0]
            if raw_schema not in ('0','2','3','4'):
                raise LibraryError('unsupported_schema')
            schema = int(raw_schema)
        else:
            # Internal migration validates the complete target before the final
            # metadata marker write; this is not an external admission bypass.
            _require(type(schema_override) is int and schema_override == 4)
            schema = schema_override
        expected = _BASE | (set() if schema == 0 else _M2) | (_M3 if schema >= 3 else set())
        if schema == 0 and 'jobs' in tables:
            expected.add('jobs')
        if schema == 4:
            expected = expected - {'lifecycle','revisions'} | {'document_state','document_revisions'}
        _require(tables == expected)
        for table in sorted(expected):
            _layout(db,table)
        _unique(db,'documents',('source_id',))
        _unique(db,'documents',('source',))
        if schema == 4:
            _unique(db,'document_revisions',('document_id','revision'))
        _require(all(row[0] == 'ok' for row in db.execute('PRAGMA quick_check')))
        _require(not list(db.execute('PRAGMA foreign_key_check')))
        metadata = _mapping(_rows(db,'metadata'),'key')
        for row in metadata.values():
            _text(row['value'])
        generation = 0 if schema == 0 else _decimal(metadata['catalog_generation']['value'])
        payload = {'schema':3,'generation':generation,'documents':[],'revisions':[],
                   'blobs':[],'collections':[],'jobs':[]}
        blob_map = {}
        for row in _mapping(_rows(db,'blobs'),'blob_id').values():
            _require(type(row['content']) is bytes)
            text = row['content'].decode('utf-8')
            blob_map[row['blob_id']] = text
            payload['blobs'].append({'blob_id':row['blob_id'],'text':text})
        docs = _mapping(_rows(db,'documents'),'document_id')
        if schema == 0:
            states = {did:{'current_revision':1,'edit_version':1,'deleted':0,
                           'notes':'','tags':'[]','collections':'[]'} for did in docs}
            payload['revisions'] = [{'document_id':did,'revision':1,'blob_id':row['blob_id']}
                                    for did,row in docs.items()]
        else:
            payload['collections'] = [row['name'] for row in _rows(db,'collections')]
            state_table = 'document_state' if schema == 4 else 'lifecycle'
            states = _mapping(_rows(db,state_table),'document_id')
            _require(set(states) == set(docs))
            revision_table = 'document_revisions' if schema == 4 else 'revisions'
            revisions = _rows(db,revision_table)
            if schema == 4:
                by_id = _mapping(revisions,'revision_id')
                for row in revisions:
                    _require(row['revision_id'] == revision_identity(row['document_id'],row['revision'],row['blob_id']))
                for did,row in states.items():
                    head = by_id.get(row['head_revision_id'])
                    _require(head is not None and head['document_id'] == did)
                    row['current_revision'] = head['revision']
            payload['revisions'] = [{key:row[key] for key in ('document_id','revision','blob_id')} for row in revisions]
        for did, doc in docs.items():
            _require(doc['blob_id'] in blob_map)
            state = states[did]
            _integer(state['deleted'],0,1)
            tags,collections = _json(state['tags']),_json(state['collections'])
            _require(state['tags'] in _canonical_forms(tags) and state['collections'] in _canonical_forms(collections))
            payload['documents'].append({'document':{**doc,'text':blob_map[doc['blob_id']]},
                'revision':state['current_revision'],'edit_version':state['edit_version'],
                'deleted':bool(state['deleted']),'notes':state['notes'],'tags':tags,'collections':collections})
        job_controls = _mapping(_rows(db,'job_control'),'job_id') if schema >= 3 else {}
        if 'jobs' in tables:
            for row in _rows(db,'jobs'):
                enrolled = job_controls[row['job_id']]['enrolled'] if schema >= 3 else 0
                _integer(enrolled,0,1)
                payload['jobs'].append({'job':{key:row[key] for key in ('job_id','epoch','state','total','completed','error')},
                    'manifest_json':row['manifest'],'content_hashes_json':row['content_hashes'],
                    'receipt_json':row['receipt'],'enrolled':bool(enrolled)})
        payload['documents'].sort(key=lambda row:(row['document']['source'],row['document']['document_id']))
        payload['revisions'].sort(key=lambda row:(row['document_id'],row['revision']))
        payload['blobs'].sort(key=lambda row:row['blob_id'])
        payload['collections'].sort()
        payload['jobs'].sort(key=lambda row:row['job']['job_id'])
        _validate_payload(payload)
        if schema >= 3:
            _controls(db,payload)
        return schema,payload


    def inspect_database(db, schema_override=None):
        """Validate recognized durable state and return its logical schema3 view."""
        try:
            return _inspect_database(db, schema_override=schema_override)
        except LibraryError as error:
            if error.code == 'unsupported_schema':
                raise
            raise LibraryError('invalid_database') from error
        except (sqlite3.Error, KeyError, IndexError, TypeError, ValueError,
                UnicodeError, OverflowError, RecursionError) as error:
            raise LibraryError('invalid_database') from error
''').lstrip('\n').rstrip() + '\n'


def _remove_method(source: str, name: str) -> str:
    marker = '    def ' + name + '('
    start = source.find(marker)
    if start < 0 or source.find(marker, start + len(marker)) >= 0:
        raise ValueError('Frozen method seam changed: ' + name)
    # Include a preceding contextmanager decorator when present.
    decorator = '    @contextmanager\n'
    if source[:start].endswith(decorator):
        start -= len(decorator)
    following = source.find('\n    def ', start + 1)
    if following < 0:
        return source[:start].rstrip() + '\n'
    end = following + 1
    if source[:end].endswith(decorator):
        end -= len(decorator)
    return source[:start] + source[end:]


def catalog_files() -> dict[str, str]:
    """Normalize inherited SQL with checked seams; no shadow legacy tables."""
    legacy = m2_catalog_files()['library/catalog/store.py']
    for name in ('__init__','_record','revision_history','_insert_document','refresh_document'):
        legacy = _remove_method(legacy,name)
    legacy = legacy.replace('lifecycle','document_state').replace('FROM revisions','FROM document_revisions')
    # Public lifecycle method names remain stable; only SQL table names change.
    legacy = legacy.replace('def document_state_show(', 'def lifecycle_show(')
    legacy = legacy.replace('def document_state_list(', 'def lifecycle_list(')
    core = control_files()['library/catalog/control.py']
    for name in ('__init__','_insert_document'):
        core = _remove_method(core,name)
    core = core.replace('from library.catalog.legacy_m2 import Store as LegacyStore',
                        'from library.catalog.m4_legacy import Store as LegacyStore')
    core = core.replace('lifecycle','document_state')
    binding = """            if self.db.execute("SELECT 1 FROM maintenance_artifacts WHERE root_kind='backup' LIMIT 1").fetchone():
                # An older unbound row cannot prove which directory owned
                # its relative name. Never guess and erase its witness.
                raise LibraryError('maintenance_busy')
            self._set_control('backup_root',str(self.backup_dir))
"""
    replacement = """            inherited = (self.db.execute("SELECT 1 FROM maintenance_artifacts WHERE root_kind='backup' LIMIT 1").fetchone()
                         or self.db.execute('SELECT 1 FROM backups LIMIT 1').fetchone())
            # Migration preserves portable relative ownership while refusing
            # to guess the originating root. An explicit later configuration
            # adopts an unbound root without discarding registry or artifacts.
            self._set_control('backup_root','' if inherited else str(self.backup_dir))
"""
    if core.count(binding) != 1:
        raise ValueError('Frozen control backup binding seam changed')
    core = core.replace(binding,replacement)
    marker = '        self.db.execute("UPDATE metadata SET value=\'3\' WHERE key=\'schema\'")\n'
    if core.count(marker) != 1:
        raise ValueError('Frozen control schema marker changed')
    core = core.replace(marker,'')
    maintenance = maintenance_files()['library/catalog/maintenance.py']
    if maintenance.count('FROM lifecycle') != 2 or maintenance.count('JOIN lifecycle') != 1 or maintenance.count('FROM revisions') != 1:
        raise ValueError('Frozen maintenance SQL seams changed')
    maintenance = maintenance.replace('FROM lifecycle','FROM document_state').replace(
        'JOIN lifecycle','JOIN document_state').replace('FROM revisions','FROM document_revisions')
    return {'library/catalog/m4_legacy.py':legacy, 'library/catalog/m4_control.py':core,
            'library/catalog/m4_store.py':_STORE, 'library/catalog/m4_validation.py':_VALIDATION,
            'library/catalog/maintenance.py':maintenance}
