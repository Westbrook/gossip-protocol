"""Trusted authored M3 control plane; never a candidate input or evaluator.

The generated M2 derivative changes only its migration admission/hook. Frozen
M2/M1 reference modules remain unchanged. Ordinary stale handles fail closed
with stale_worker; that error choice is implementation policy, not a new public
acceptance requirement for every compliant implementation.

Authored backup-root policy: the public default remains dbparent/backups. A
durable control.backup_root binds relative ownership records outside snapshots.
Reassignment clears only the old root-relative listing metadata, preserves
files, and fails maintenance_busy while old-root artifacts or live operations
remain. An explicit custom directory is therefore required to reopen pending
custom-root work; ordinary Store construction never runs recovery.
"""
from __future__ import annotations

from textwrap import dedent

from .library_m2_catalog_reference_v1 import catalog_files


_CONTROL = dedent(r'''
    """Durable fences, process locks, and bounded ownership-based recovery."""
    from contextlib import contextmanager
    import errno
    import fcntl
    import json
    import os
    from pathlib import Path, PurePosixPath
    import re
    import sqlite3
    import stat
    import uuid

    from library.catalog.legacy_m2 import Store as LegacyStore
    from library.catalog.validation import canonical_manifest, check_job_id
    from library.common import LibraryError, identity, source_key

    def _directory(path):
        path = Path(os.path.abspath(path))
        for component in reversed((path, *path.parents)):
            try:
                mode = component.lstat().st_mode
            except OSError as error:
                raise LibraryError('io_error') from error
            if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
                raise LibraryError('invalid_source')
        return path

    class CoreStore(LegacyStore):
        def __init__(self, path, *, backup_dir=None):
            self.path = os.path.abspath(path)
            _directory(Path(self.path).parent)
            self.maintenance_root = Path(self.path + '.maintenance')
            try:
                self.maintenance_root.mkdir(mode=0o700, exist_ok=True)
            except OSError as error:
                raise LibraryError('io_error') from error
            _directory(self.maintenance_root)
            self.backup_dir = (Path(self.path).parent / 'backups' if backup_dir is None
                               else _directory(backup_dir))
            self._bootstrapping = True
            self._maintenance_depth = 0
            self._maintenance_exclusive = False
            self._worker_claim = None
            self._savepoint_counter = 0
            self._incarnation = None
            super().__init__(self.path)
            self._bootstrapping = False
            try:
                self.adopt_incarnation()
                self.configure_backup_dir(backup_dir)
            except BaseException:
                self.db.close()
                raise

        def _initialize_control(self):
            for statement in (
                'CREATE TABLE IF NOT EXISTS control (key TEXT PRIMARY KEY,value TEXT NOT NULL)',
                'CREATE TABLE IF NOT EXISTS job_control (job_id TEXT PRIMARY KEY,epoch_high_water INTEGER NOT NULL,enrolled INTEGER NOT NULL)',
                'CREATE TABLE IF NOT EXISTS document_control (document_id TEXT PRIMARY KEY,edit_high_water INTEGER NOT NULL)',
                'CREATE TABLE IF NOT EXISTS maintenance_artifacts (artifact_id TEXT PRIMARY KEY,kind TEXT NOT NULL,'
                'root_kind TEXT NOT NULL,relative_path TEXT NOT NULL,owner_incarnation TEXT NOT NULL,'
                'owner_generation INTEGER NOT NULL,state TEXT NOT NULL)',
                'CREATE TABLE IF NOT EXISTS backups (name TEXT PRIMARY KEY,bytes INTEGER NOT NULL,payload_sha256 TEXT NOT NULL,generation INTEGER NOT NULL)',
                'CREATE TABLE IF NOT EXISTS search_state (singleton INTEGER PRIMARY KEY,published_generation INTEGER,'
                'target_generation INTEGER,processed INTEGER NOT NULL,total INTEGER NOT NULL,cursor TEXT)',
                'CREATE TABLE IF NOT EXISTS search_entries (generation INTEGER NOT NULL,document_id TEXT NOT NULL,'
                'source TEXT NOT NULL,text TEXT NOT NULL,PRIMARY KEY(generation,document_id))',
                'INSERT OR IGNORE INTO search_state VALUES (1,NULL,NULL,0,0,NULL)',
                'INSERT OR IGNORE INTO job_control SELECT job_id,epoch,0 FROM jobs',
                'INSERT OR IGNORE INTO document_control SELECT document_id,edit_version FROM lifecycle',
            ):
                self.db.execute(statement)
            for key, value in (('incarnation',uuid.uuid4().hex),('worker_generation','0'),
                               ('last_error','null'),('cleanup_cursor','null')):
                self.db.execute('INSERT OR IGNORE INTO control VALUES (?,?)',(key,value))
            if self.db.execute("SELECT 1 FROM control WHERE key='backup_root'").fetchone() is None:
                if self.db.execute("SELECT 1 FROM maintenance_artifacts WHERE root_kind='backup' LIMIT 1").fetchone():
                    # An older unbound row cannot prove which directory owned
                    # its relative name. Never guess and erase its witness.
                    raise LibraryError('maintenance_busy')
                self._set_control('backup_root',str(self.backup_dir))
            # Triggers update only the touched ID, so legal large job histories
            # do not cause a full historical scan on every ordinary write.
            for table, field, target, key, high, extra in (
                ('jobs','epoch','job_control','job_id','epoch_high_water',',0'),
                ('lifecycle','edit_version','document_control','document_id','edit_high_water',''),
            ):
                for event in ('INSERT','UPDATE'):
                    self.db.execute('CREATE TRIGGER IF NOT EXISTS fence_'+table+'_'+event.lower()+' AFTER '+event+
                        ' ON '+table+' BEGIN INSERT INTO '+target+' VALUES (NEW.'+key+',NEW.'+field+extra+') '
                        'ON CONFLICT('+key+') DO UPDATE SET '+high+'=MAX('+high+',excluded.'+high+'); END')
            self.db.execute("UPDATE metadata SET value='3' WHERE key='schema'")

        def configure_backup_dir(self, path):
            # None is the public default even if a previous process selected a
            # custom directory. Its existence is required only when used.
            selected = Path(self.path).parent / 'backups' if path is None else _directory(path)
            with self._lock('backup-root.lock',exclusive=False):
                if self._control('backup_root') == str(selected):
                    self.backup_dir = selected
                    return
            with self._lock('backup-root.lock'):
                with self._write():
                    if self._control('backup_root') != str(selected):
                        if self.db.execute("SELECT 1 FROM maintenance_artifacts WHERE root_kind='backup' LIMIT 1").fetchone():
                            raise LibraryError('maintenance_busy')
                        self._set_control('backup_root',str(selected))
                        # These rows describe files under the old configured
                        # root. Files themselves remain completely untouched.
                        self.db.execute('DELETE FROM backups')
                self.backup_dir = selected

        @contextmanager
        def backup_directory_authority(self):
            # Hold this through filesystem publication and ownership/registry
            # commits, including the interval before artifact enrollment.
            with self._lock('backup-root.lock',exclusive=False):
                if self._control('backup_root') != str(self.backup_dir):
                    raise LibraryError('maintenance_busy')
                yield self.backup_dir

        def _control(self, key):
            row = self.db.execute('SELECT value FROM control WHERE key=?',(key,)).fetchone()
            if row is None:
                raise LibraryError('invalid_schema')
            return row[0]

        def _set_control(self, key, value):
            if type(key) is not str or type(value) is not str or not self.db.in_transaction:
                raise LibraryError('invalid_request')
            self.db.execute('INSERT INTO control VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                            (key,value))

        def adopt_incarnation(self):
            self._incarnation = self._control('incarnation')

        @contextmanager
        def _lock(self, name, *, exclusive=True, blocking=False, busy='maintenance_busy'):
            _directory(self.maintenance_root)
            descriptor = None
            try:
                descriptor = os.open(self.maintenance_root / name,
                    os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
                if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                    raise LibraryError('invalid_source')
                try:
                    fcntl.flock(descriptor, (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
                                | (0 if blocking else fcntl.LOCK_NB))
                except OSError as error:
                    if error.errno in (errno.EACCES,errno.EAGAIN):
                        raise LibraryError(busy) from error
                    raise
                yield
            except OSError as error:
                raise LibraryError('io_error') from error
            finally:
                if descriptor is not None:
                    os.close(descriptor)

        @contextmanager
        def _maintenance_shared(self):
            if self._maintenance_depth:
                yield
                return
            with self._lock('maintenance.lock',exclusive=False):
                self._maintenance_depth += 1
                try:
                    yield
                finally:
                    self._maintenance_depth -= 1

        @contextmanager
        def maintenance_authority(self):
            if self._maintenance_exclusive:
                yield
                return
            if self._maintenance_depth or self._worker_claim is not None:
                raise LibraryError('maintenance_busy')
            with self._lock('maintenance.lock'), self._lock('worker.lock'):
                self._maintenance_depth += 1
                self._maintenance_exclusive = True
                try:
                    yield
                finally:
                    self._maintenance_exclusive = False
                    self._maintenance_depth -= 1

        @contextmanager
        def _write(self):
            with self._maintenance_shared():
                if self.db.in_transaction:
                    self._savepoint_counter += 1
                    name = 'nested_'+str(self._savepoint_counter)
                    changed = self._catalog_changed
                    self.db.execute('SAVEPOINT '+name)
                    try:
                        if not self._bootstrapping and self._control('incarnation') != self._incarnation:
                            raise LibraryError('stale_worker')
                        yield
                        self.db.execute('RELEASE '+name)
                    except BaseException:
                        self.db.execute('ROLLBACK TO '+name)
                        self.db.execute('RELEASE '+name)
                        self._catalog_changed = changed
                        raise
                    return
                self.db.execute('BEGIN IMMEDIATE')
                self._catalog_changed = False
                try:
                    if not self._bootstrapping and self._control('incarnation') != self._incarnation:
                        raise LibraryError('stale_worker')
                    yield
                    if self._catalog_changed:
                        self._capacity()
                        self.db.execute("UPDATE metadata SET value=? WHERE key='catalog_generation'",
                                        (str(self._generation()+1),))
                    self.db.commit()
                except BaseException:
                    self.db.rollback()
                    raise
                finally:
                    self._catalog_changed = False

        def create_job(self, job_id, entries):
            check_job_id(job_id)
            entries = canonical_manifest(entries)
            with self._write():
                previous = self.db.execute('SELECT * FROM jobs WHERE job_id=?',(job_id,)).fetchone()
                if previous is not None:
                    # Portable backups preserve serialization bytes. Whitespace
                    # and Unicode escapes never change admitted entry identity.
                    if entries != canonical_manifest(json.loads(previous['manifest'])):
                        raise LibraryError('job_conflict')
                    return self._job(previous)
                high = self.db.execute('SELECT epoch_high_water FROM job_control WHERE job_id=?',(job_id,)).fetchone()
                result = super().create_job(job_id,entries)
                if high is not None:
                    self.db.execute('UPDATE jobs SET epoch=? WHERE job_id=?',(high[0]+1,job_id))
                    result = self.get_job(job_id)
                return result

        def _insert_document(self, source, raw):
            source_key(source)
            previous = None
            high = None
            if type(source) is str:
                did = identity('document',source)
                previous = self.db.execute('SELECT 1 FROM documents WHERE document_id=?',(did,)).fetchone()
                high = self.db.execute('SELECT edit_high_water FROM document_control WHERE document_id=?',(did,)).fetchone()
            result = super()._insert_document(source,raw)
            if previous is None and high is not None:
                self.db.execute('UPDATE lifecycle SET edit_version=? WHERE document_id=?',(high[0]+1,did))
            return result

        @contextmanager
        def worker_owner(self):
            if self._worker_claim is not None:
                raise LibraryError('worker_busy')
            with self._lock('worker.lock',busy='worker_busy'), self._lock('worker-live.lock',busy='worker_busy',blocking=True):
                with self._write():
                    generation = int(self._control('worker_generation'))+1
                    self._set_control('worker_generation',str(generation))
                    owner = {'incarnation':self._control('incarnation'),'generation':generation}
                self._worker_claim = owner
                try:
                    try:
                        with self.worker_guard(owner):
                            self.cleanup_artifacts()
                    except (OSError,sqlite3.Error) as error:
                        try:
                            with self.worker_guard(owner):
                                self.record_error('worker','io_error')
                        except (LibraryError,OSError,sqlite3.Error):
                            pass
                        raise LibraryError('io_error') from error
                    except LibraryError as error:
                        if error.code not in ('stale_worker','stale_epoch'):
                            try:
                                with self.worker_guard(owner):
                                    self.record_error('worker',error.code)
                            except (LibraryError,OSError,sqlite3.Error):
                                pass
                        raise
                    yield dict(owner)
                finally:
                    self._worker_claim = None

        @contextmanager
        def worker_guard(self, claim):
            with self._write():
                if (type(claim) is not dict or set(claim) not in ({'incarnation','generation'},{'incarnation','generation','job_id','epoch'})
                    or type(claim.get('generation')) is not int
                    or self._worker_claim is None
                    or claim.get('incarnation') != self._control('incarnation')
                    or claim.get('generation') != int(self._control('worker_generation'))
                    or any(claim.get(key) != value for key,value in self._worker_claim.items())):
                    raise LibraryError('stale_worker')
                row = self._fenced(claim['job_id'],claim['epoch']) if 'job_id' in claim else None
                yield row

        def worker_state(self):
            if self._worker_claim is not None:
                return 'running'
            descriptor = None
            try:
                # Observation never creates a lock file, and maintenance's
                # worker reservation is distinct from actual worker liveness.
                descriptor = os.open(self.maintenance_root / 'worker-live.lock',os.O_RDONLY|os.O_NOFOLLOW)
                if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                    raise LibraryError('invalid_source')
                try:
                    fcntl.flock(descriptor,fcntl.LOCK_SH|fcntl.LOCK_NB)
                except OSError as error:
                    if error.errno in (errno.EACCES,errno.EAGAIN):
                        return 'running'
                    raise
            except FileNotFoundError:
                pass
            except OSError as error:
                raise LibraryError('io_error') from error
            finally:
                if descriptor is not None:
                    os.close(descriptor)
            pending = self.db.execute("SELECT 1 FROM jobs JOIN job_control USING(job_id) "
                "WHERE enrolled=1 AND state IN ('queued','running') LIMIT 1").fetchone()
            return 'idle' if pending else 'stopped'

        def record_error(self, operation, code):
            if operation not in ('worker','reindex','backup','restore','migrate') or (code is not None and type(code) is not str):
                raise LibraryError('invalid_request')
            with self._write():
                previous = json.loads(self._control('last_error'))
                if code is not None:
                    value = {'operation':operation,'code':code}
                elif previous is not None and previous['operation'] == operation:
                    value = None
                else:
                    return
                self._set_control('last_error',json.dumps(value,sort_keys=True,separators=(',',':')))

        @contextmanager
        def artifact_lock(self, artifact_id, blocking=False):
            if type(artifact_id) is not str or re.fullmatch(r'[A-Za-z0-9_-]{1,128}',artifact_id) is None:
                raise LibraryError('invalid_request')
            with self._lock('artifact-'+artifact_id+'.lock',blocking=blocking):
                yield

        def register_artifact(self, kind, root_kind, relative_path, artifact_id=None, state='staging'):
            if (kind not in ('backup_stage','restore_stage','index_generation')
                or root_kind not in ('maintenance','backup') or state not in ('staging','published','obsolete')
                or type(relative_path) is not str or not relative_path
                or '\\' in relative_path or '\x00' in relative_path):
                raise LibraryError('invalid_request')
            parts = PurePosixPath(relative_path).parts
            if (PurePosixPath(relative_path).is_absolute() or any(part in ('.','..') for part in relative_path.split('/'))
                or len(parts)>16 or str(PurePosixPath(relative_path)) != relative_path):
                raise LibraryError('invalid_request')
            artifact_id = uuid.uuid4().hex if artifact_id is None else artifact_id
            if type(artifact_id) is not str or re.fullmatch(r'[A-Za-z0-9_-]{1,128}',artifact_id) is None:
                raise LibraryError('invalid_request')
            with self._write():
                if root_kind == 'backup' and self._control('backup_root') != str(self.backup_dir):
                    raise LibraryError('maintenance_busy')
                self.db.execute('INSERT INTO maintenance_artifacts VALUES (?,?,?,?,?,?,?)',
                    (artifact_id,kind,root_kind,relative_path,self._control('incarnation'),
                     int(self._control('worker_generation')),state))
            return artifact_id

        def _remove_owned_file(self, row):
            root = self.maintenance_root if row['root_kind']=='maintenance' else self.backup_dir
            root = _directory(root)
            parts = PurePosixPath(row['relative_path']).parts
            descriptor = os.open(root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
            try:
                for part in parts[:-1]:
                    next_descriptor = os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=descriptor)
                    os.close(descriptor)
                    descriptor = next_descriptor
                try:
                    info = os.stat(parts[-1],dir_fd=descriptor,follow_symlinks=False)
                except FileNotFoundError:
                    return True
                if not stat.S_ISREG(info.st_mode):
                    return False
                os.unlink(parts[-1],dir_fd=descriptor)
                os.fsync(descriptor)
                return True
            finally:
                os.close(descriptor)

        def cleanup_artifacts(self, limit=64):
            if type(limit) is not int or not 1 <= limit <= 64:
                raise LibraryError('invalid_request')
            scanned = removed = 0
            with self._write():
                cursor = json.loads(self._control('cleanup_cursor'))
                rows = list(self.db.execute('SELECT * FROM maintenance_artifacts WHERE artifact_id>? '
                    'ORDER BY artifact_id LIMIT ?',(cursor or '',limit)))
                if not rows and cursor is not None:
                    rows = list(self.db.execute('SELECT * FROM maintenance_artifacts ORDER BY artifact_id LIMIT ?',(limit,)))
                for item in rows:
                    row = dict(item)
                    scanned += 1
                    cursor = row['artifact_id']
                    try:
                        with self.artifact_lock(row['artifact_id']):
                            if row['root_kind']=='backup' and self._control('backup_root') != str(self.backup_dir):
                                raise LibraryError('maintenance_busy')
                            remove = False
                            if row['kind']=='index_generation':
                                if not row['relative_path'].isdigit():
                                    continue
                                generation = int(row['relative_path'])
                                live = self.db.execute('SELECT published_generation,target_generation FROM search_state WHERE singleton=1').fetchone()
                                if generation not in tuple(live):
                                    self.db.execute('DELETE FROM search_entries WHERE generation=?',(generation,))
                                    remove = True
                            elif row['kind']=='backup_stage' and hasattr(self,'_recover_backup_artifact'):
                                remove = self._recover_backup_artifact(row)
                            elif row['state']!='published':
                                remove = self._remove_owned_file(row)
                            if remove:
                                self.db.execute('DELETE FROM maintenance_artifacts WHERE artifact_id=?',(row['artifact_id'],))
                                removed += 1
                    except LibraryError as error:
                        if error.code!='maintenance_busy':
                            raise
                    except OSError as error:
                        raise LibraryError('io_error') from error
                if not rows:
                    cursor = None
                self._set_control('cleanup_cursor',json.dumps(cursor,separators=(',',':')))
            return {'scanned':scanned,'removed':removed,'cursor':cursor}
''').lstrip('\n').rstrip() + '\n'


def control_files() -> dict[str, str]:
    """Produce an explicitly bounded migration derivative plus M3 control code."""
    source = catalog_files()["library/catalog/store.py"]
    admission = "if schema not in ('0', '2'):"
    hook = "        except BaseException:\n            self.db.close()"
    if source.count(admission) != 1 or source.count(hook) != 1:
        raise ValueError("Frozen M2 migration seam changed")
    source = source.replace(admission, "if schema not in ('0', '2', '3'):")
    source = source.replace(hook, "                self._initialize_control()\n" + hook)
    return {"library/catalog/legacy_m2.py": source, "library/catalog/control.py": _CONTROL}
