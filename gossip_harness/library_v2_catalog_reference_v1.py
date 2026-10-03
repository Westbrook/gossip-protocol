"""Prospective v2 catalog derivative. Frozen M1--M4 generators stay unchanged.

Checked source seams produce ordinary modules; no runtime monkeypatching occurs.
The predecessor helper is an explicit v2 schema3 implementation for migration
qualification, not a retrofit of already running frozen-v1 processes.
"""
from __future__ import annotations

import ast
from textwrap import dedent


def _source(value: str) -> str:
    return dedent(value).strip() + '\n'


def _replace(source: str, old: str, new: str, count: int = 1) -> str:
    if source.count(old) != count:
        raise ValueError('Frozen v2 catalog seam changed: ' + old[:80])
    return source.replace(old, new)


def _method(source: str, name: str, replacement: str | None) -> str:
    nodes = [node for cls in ast.parse(source).body if isinstance(cls, ast.ClassDef)
             for node in cls.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name]
    if len(nodes) != 1:
        raise ValueError('Frozen method seam changed: ' + name)
    node = nodes[0]
    start = min([node.lineno, *(item.lineno for item in node.decorator_list)]) - 1
    lines = source.splitlines(keepends=True)
    assert node.end_lineno is not None
    rendered = '' if replacement is None else ''.join('    ' + line if line.strip() else line
        for line in _source(replacement).splitlines(keepends=True))
    return ''.join(lines[:start]) + rendered + ''.join(lines[node.end_lineno:])


_COUNTERS = _source('''
    """Exact public signed64 optimistic tokens and transition allocation."""
    from library.common import LibraryError
    COUNTER_MAX = 9223372036854775807

    def counter(value, minimum=0):
        if type(value) is not int or not minimum <= value <= COUNTER_MAX:
            raise LibraryError('invalid_request')
        return value

    def increment(value):
        value = counter(value)
        if value == COUNTER_MAX:
            raise LibraryError('counter_exhausted')
        return value + 1
''')

_CREATE = _source('''
    def create_job(self, job_id, entries):
        check_job_id(job_id)
        entries = canonical_manifest(entries)
        manifest = json.dumps(entries, ensure_ascii=True, sort_keys=True, separators=(',', ':'), allow_nan=False)
        hashes = []
        for entry in entries:
            try:
                hashes.append(hashlib.sha256(entry['text'].encode('utf-8')).hexdigest())
            except UnicodeError:
                hashes.append(None)
        with self._write():
            previous = self.db.execute('SELECT * FROM jobs WHERE job_id=?',(job_id,)).fetchone()
            if previous is not None:
                if entries != canonical_manifest(json.loads(previous['manifest'])):
                    raise LibraryError('job_conflict')
                return self._job(previous)
            high = self.db.execute('SELECT epoch_high_water FROM job_control WHERE job_id=?',(job_id,)).fetchone()
            epoch = increment(high[0]) if high is not None else 1
            self.db.execute("INSERT INTO jobs VALUES (?,?,'queued',?,0,NULL,?,?,NULL)",
                (job_id,epoch,len(entries),manifest,json.dumps(hashes,ensure_ascii=True,separators=(',', ':'),allow_nan=False)))
            return self.get_job(job_id)
''')

_EXTRA = _source('''
    def _validate_insert(self, source, raw):
        source_key(source)
        if type(raw) is not bytes:
            raise LibraryError('invalid_request')
        from library.common import MAX_FILE_BYTES
        if len(raw) > MAX_FILE_BYTES:
            raise LibraryError('too_large')
        try:
            raw.decode('utf-8')
        except UnicodeError as error:
            raise LibraryError('invalid_utf8') from error

    def insert(self, source, raw):
        self._validate_insert(source,raw)
        return super().insert(source,raw)

    def _preflight_catalog(self, edit_version=None):
        increment(self._generation())
        if edit_version is not None:
            increment(edit_version)

    def _preflight_batch(self, entries):
        changed = False
        for entry in entries:
            did = identity('document', entry['source'])
            if self.db.execute('SELECT 1 FROM documents WHERE document_id=?',(did,)).fetchone() is None:
                high = self.db.execute('SELECT edit_high_water FROM document_control WHERE document_id=?',(did,)).fetchone()
                if high is not None:
                    increment(high[0])
                changed = True
        if changed:
            self._preflight_catalog()

    def preflight_job_commit(self, job_id, epoch):
        check_job_id(job_id)
        counter(epoch,1)
        with self._write():
            row = self._fenced(job_id,epoch)
            if row['state'] == 'completed':
                return []
            if row['state'] not in ('queued','running'):
                raise LibraryError('job_state')
            from library.catalog.validation import validate_entries
            entries = validate_entries(json.loads(row['manifest']),self.all_documents())
            self._preflight_batch(entries)
            return entries

    def _fenced(self, job_id, epoch):
        counter(epoch,1)
        return super()._fenced(job_id,epoch)

    def start_job(self, job_id, epoch):
        check_job_id(job_id)
        counter(epoch,1)
        return super().start_job(job_id,epoch)

    def fail_job(self, job_id, epoch, code):
        check_job_id(job_id)
        counter(epoch,1)
        return super().fail_job(job_id,epoch,code)

    def cancel_job(self, job_id):
        check_job_id(job_id)
        with self._write():
            row = self._job_row(job_id)
            if row['state'] == 'cancelled':
                return self._job(row)
            if row['state'] not in ('queued','running'):
                raise LibraryError('job_state')
            epoch = increment(row['epoch'])
            self.db.execute("UPDATE jobs SET epoch=?,state='cancelled',completed=0,error=NULL,receipt=NULL WHERE job_id=?",(epoch,job_id))
            return self.get_job(job_id)

    def retry_job(self, job_id):
        check_job_id(job_id)
        with self._write():
            row = self._job_row(job_id)
            if row['state'] not in ('failed','cancelled'):
                raise LibraryError('job_state')
            epoch = increment(row['epoch'])
            self.db.execute("UPDATE jobs SET epoch=?,state='queued',completed=0,error=NULL,receipt=NULL WHERE job_id=?",(epoch,job_id))
            return self.get_job(job_id)
''')


def _legacy(source: str) -> str:
    source = _replace(source, 'from contextlib import contextmanager\n',
                      'from contextlib import contextmanager\nfrom library.counters import counter, increment\n')
    source = _replace(source, "if type(value) is not int or value < minimum:",
                      "if type(value) is not int or not minimum <= value <= 9223372036854775807:")
    source = _replace(source, '        _integer(expected_version,1)\n',
                      "        if type(document_id) is not str:\n            raise LibraryError('invalid_request')\n        _utf8(document_id,1 << 30)\n        _integer(expected_version,1)\n", source.count('        _integer(expected_version,1)\n'))
    source = _replace(source, "                self.db.execute('UPDATE document_state SET notes=", 
                      "                self._preflight_catalog(record['edit_version'])\n                self.db.execute('UPDATE document_state SET notes=")
    source = _replace(source, "                self.db.execute('UPDATE document_state SET deleted=",
                      "                self._preflight_catalog(record['edit_version'])\n                self.db.execute('UPDATE document_state SET deleted=")
    for sql in ("DELETE FROM collections WHERE name=?", "INSERT INTO collections VALUES (?)"):
        source = _replace(source, "                self.db.execute('" + sql,
                          "                self._preflight_catalog()\n                self.db.execute('" + sql)
    source = _replace(source, "        if type(fail_before_commit) is not bool:\n",
                      "        check_job_id(job_id)\n        counter(epoch,1)\n        if type(fail_before_commit) is not bool:\n")
    source = _replace(source, 'from library.catalog.validation import validate_entries',
                      'from library.catalog.validation import validate_entries, check_job_id')
    source = _replace(source, "                # Savepoint makes capacity failures", "                self._preflight_batch(entries)\n                # Savepoint makes capacity failures")
    source = _replace(source, "            except LibraryError as error:\n                if 'changed_before' in locals():",
                      "            except LibraryError as error:\n                if error.code == 'counter_exhausted':\n                    raise\n                if 'changed_before' in locals():")
    return source


def _core(source: str) -> str:
    source = _replace(source, 'import errno\n', 'import errno\nimport hashlib\nfrom library.counters import counter, increment\n')
    source = _replace(source, "raise LibraryError('stale_worker')\n                    yield", "raise LibraryError('stale_instance')\n                    yield")
    source = _replace(source, "raise LibraryError('stale_worker')\n                yield", "raise LibraryError('stale_instance')\n                yield")
    source = _replace(source, 'str(self._generation()+1)', 'str(increment(self._generation()))')
    # An absent portable root stays absent. Explicit adoption is a separate operation.
    start = source.index('        if self.db.execute("SELECT 1 FROM control WHERE key=\'backup_root\'").fetchone() is None:')
    end = source.index('        # Triggers update', start)
    source = source[:start] + source[end:]
    source = _method(source, 'create_job', _CREATE)
    source = _method(source, 'configure_backup_dir', '''
        def configure_backup_dir(self, path):
            self.backup_dir = Path(self.path).parent / 'backups' if path is None else Path(path)
    ''')
    source += '\n' + ''.join('    ' + line if line.strip() else line for line in _EXTRA.splitlines(keepends=True))
    return source


def _format(source: str) -> str:
    source = _replace(source, 'from library.common import LibraryError, blob_id, identity, source_key',
                      'from library.common import LibraryError, blob_id, identity, source_key\nfrom library.counters import COUNTER_MAX')
    source = _replace(source, 'def _integer(value, minimum=0, maximum=None):',
                      'def _integer(value, minimum=0, maximum=COUNTER_MAX):')
    return source


def _jobs(source: str) -> str:
    source = _replace(source, 'from library.catalog.validation import validate_entries',
                      'from library.catalog.validation import validate_entries, check_job_id\nfrom library.counters import counter')
    source = _method(source, 'prepare', """
        def prepare(self, job_id):
            check_job_id(job_id)
            failure = None
            with self.store._write():
                job = self.store.get_job(job_id)
                if job['state'] == 'running':
                    return {'job_id':job_id,'epoch':job['epoch']}
                if job['state'] != 'queued':
                    raise LibraryError('job_state')
                try:
                    validate_entries(self.store.job_manifest(job_id),self.store.documents())
                except LibraryError as error:
                    self.store.fail_job(job_id,job['epoch'],error.code)
                    failure = error
                else:
                    token = self.store.start_job(job_id,job['epoch'])
            if failure is not None:
                raise failure
            return token
    """)
    return _replace(source, "        return self.store.commit_job(token['job_id'], token['epoch'],",
                    "        check_job_id(token['job_id'])\n        counter(token['epoch'],1)\n        return self.store.commit_job(token['job_id'], token['epoch'],")


def _validation(validation: str) -> str:
    validation = _replace(validation, 'from library.common import LibraryError',
                          'from library.common import LibraryError\nfrom library.counters import COUNTER_MAX')
    validation = _replace(validation, 'def _integer(value, minimum=0, maximum=None):',
                          'def _integer(value, minimum=0, maximum=COUNTER_MAX):')
    validation = _replace(validation, '    return int(value)\n', '    return _integer(int(value))\n')
    validation = _replace(validation, "'too_large','unsupported_operation'", "'stale_instance','counter_exhausted','backup_root_unbound','backup_root_mismatch','too_large','unsupported_operation'")
    validation = _replace(validation, "    generation = _decimal(controls['worker_generation']['value'])", '''    if 'backup_root' in controls:
        root = _json(controls['backup_root']['value'])
        if root is not None:
            _text(root,nonempty=True)
            _require(root.startswith('/') and '\\0' not in root and str(PurePosixPath(root)) == root
                     and '..' not in root.split('/') and '.' not in root.split('/'))
    generation = _decimal(controls['worker_generation']['value'])''')
    return validation


def catalog_files(base: dict[str, str]) -> dict[str, str]:
    """Return only the catalog-owned complete generated replacements."""
    legacy = _legacy(base['library/catalog/m4_legacy.py'])
    core = _core(base['library/catalog/m4_control.py'])
    store = base['library/catalog/m4_store.py']
    store = _replace(store, 'from library.catalog.legacy_m2 import _integer, _utf8',
                     'from library.catalog.m4_legacy import _integer, _utf8\nfrom library.counters import increment')
    store = _replace(store, "self.backup_dir = (Path(self.path).parent / 'backups' if backup_dir is None else _directory(backup_dir))",
                     "self.backup_dir = (Path(self.path).parent / 'backups' if backup_dir is None else Path(backup_dir))\n        self.configure_backup_dir(backup_dir)")
    store = _replace(store, "guards_current = (self._fences_current() and self.db.execute(\n                        \"SELECT 1 FROM control WHERE key='backup_root'\").fetchone() is not None)",
                     'guards_current = self._fences_current()')
    store = _replace(store, '                            self._initialize_backup_binding()\n', '')
    store = _replace(store, '                self._initialize_backup_binding()\n', '')
    store = _replace(store, '            self._initialize_backup_binding()\n', '')
    store = _method(store, '_initialize_backup_binding', None)
    store = _method(store, 'configure_backup_dir', '''
        def configure_backup_dir(self, path):
            self.backup_dir = Path(self.path).parent / 'backups' if path is None else Path(path)
    ''')
    store = _replace(store, "edit = high[0]+1 if high is not None else 1", "edit = increment(high[0]) if high is not None else 1\n        self._preflight_catalog()")
    store = _replace(store, '        _integer(expected_version,1)\n',
                     "        if type(document_id) is not str:\n            raise LibraryError('invalid_request')\n        _utf8(document_id,1 << 30)\n        _integer(expected_version,1)\n")
    store = _replace(store, "                number = record['revision']+1",  "                self._preflight_catalog(record['edit_version'])\n                number = record['revision']+1")
    store = _replace(store, "            # Validate transformed graph before", "            if existed and schema in (0,2,3):\n                self._clear_migrate_error()\n            # Validate transformed graph before")
    store = _method(store, 'migrate', '''
        def migrate(self):
            with self.maintenance_authority(), self._write():
                schema, payload = inspect_database(self.db)
                if schema != 4:
                    raise LibraryError('invalid_database')
                self._clear_migrate_error()
                # Construction may already have activated an older schema.
                # This call describes its own current-state operation; only
                # the CLI command that opened it may report initial activation.
                self._pending_migration = None
                return self._migration_result(4,False,payload)
    ''')
    store += '\n' + ''.join('    ' + line if line.strip() else line for line in _source('''
        def _clear_migrate_error(self):
            error = json.loads(self._control('last_error'))
            if error is not None and error['operation'] == 'migrate':
                self._set_control('last_error','null')
    ''').splitlines(keepends=True))
    validation = _validation(base['library/catalog/m4_validation.py'])
    return {'library/catalog/m4_legacy.py':legacy, 'library/catalog/m4_control.py':core,
            'library/catalog/m4_store.py':store, 'library/catalog/m4_validation.py':validation,
            'library/catalog/backup_format.py':_format(base['library/catalog/backup_format.py']),
            'library/counters.py':_COUNTERS,
            'library/ingestion/jobs.py':_jobs(base['library/ingestion/jobs.py'])}


def schema3_catalog_files(base: dict[str, str]) -> dict[str, str]:
    """Explicit v2 schema3 predecessor; compose the v2 maintenance mixin first."""
    legacy = base['library/catalog/legacy_m2.py'].replace('lifecycle','document_state')
    legacy = _legacy(legacy)
    legacy = _replace(legacy, "                self.db.execute('INSERT OR IGNORE INTO blobs VALUES (?,?)',(bid,raw))",
                      "                self._preflight_catalog(record['edit_version'])\n                self.db.execute('INSERT OR IGNORE INTO blobs VALUES (?,?)',(bid,raw))")
    legacy = legacy.replace('document_state','lifecycle')
    core = _core(base['library/catalog/control.py'])
    # Preserve the pre-normalization layout; the shared helper only reads IDs.
    core = _replace(core, "else _directory(backup_dir))", "else Path(backup_dir))")
    core = _replace(core, '        result = super()._insert_document(source,raw)',
                    '''        # Validate before allocation, then preflight every increment before
        # entering the inherited insert that can create blobs/documents.
        self._validate_insert(source,raw)
        if previous is None:
            self._preflight_catalog()
            if high is not None:
                increment(high[0])
        result = super()._insert_document(source,raw)''')
    core = _replace(core, '(high[0]+1,did)', '(increment(high[0]),did)')
    core = _replace(core, 'from library.counters import counter, increment',
                    'from library.counters import counter, increment\nfrom library.catalog.m4_validation import inspect_database')
    core = _replace(core, '        self.path = os.path.abspath(path)', '''        self.path = os.path.abspath(path)
        self._schema3_admission_required = Path(self.path).exists()
        if self._schema3_admission_required:
            connection = None
            try:
                connection = sqlite3.connect(Path(self.path).as_uri()+'?mode=ro',uri=True,isolation_level=None)
                connection.row_factory = sqlite3.Row
                connection.execute('BEGIN')
                schema,_ = inspect_database(connection)
                if schema not in (0,2,3):
                    raise LibraryError('invalid_database')
                connection.rollback()
            except sqlite3.Error as error:
                raise LibraryError('invalid_database') from error
            finally:
                if connection is not None:
                    connection.close()''')
    core = _replace(core, "\n                if not self._bootstrapping and self._control('incarnation') != self._incarnation:",
                    '''
                if self._bootstrapping and getattr(self,'_schema3_admission_required',False):
                    schema,_ = inspect_database(self.db)
                    if schema not in (0,2,3):
                        raise LibraryError('invalid_database')
                    self._schema3_admission_required = False
                if not self._bootstrapping and self._control('incarnation') != self._incarnation:''')
    from .library_m4_catalog_reference_v1 import catalog_files as frozen_m4_catalog_files
    validation = _validation(frozen_m4_catalog_files()['library/catalog/m4_validation.py'])
    return {'library/catalog/legacy_m2.py':legacy, 'library/catalog/control.py':core,
            'library/catalog/m4_validation.py':validation,
            'library/catalog/backup_format.py':_format(base['library/catalog/backup_format.py']),
            'library/counters.py':_COUNTERS,
            'library/ingestion/jobs.py':_jobs(base['library/ingestion/jobs.py'])}
