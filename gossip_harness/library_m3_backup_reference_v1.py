"""Authored M3 backup/restore implementation; never candidate acceptance evidence."""
from __future__ import annotations

from textwrap import dedent


_BACKUP = dedent(r'''
    """Logical backups with owned publication recovery and transactional restore."""
    from contextlib import contextmanager
    import errno
    import hashlib
    import json
    import os
    import re
    import sqlite3
    import stat
    import uuid

    from library.common import LibraryError, page
    from library.catalog.backup_format import canonical_bytes, validate_backup

    MAX_BACKUP_BYTES = 67108864
    _BASENAME = re.compile(r'[A-Za-z0-9_-]{1,64}\.json\Z', re.ASCII)
    _STAGE = re.compile(r'\.backup-([0-9a-f]{32})--([A-Za-z0-9_-]{1,64}\.json)\.partial\Z', re.ASCII)

    def _basename(name):
        if type(name) is not str or not _BASENAME.fullmatch(name):
            raise LibraryError('invalid_source')
        return name

    def _io(error):
        return LibraryError('invalid_source' if error.errno in (errno.ELOOP, errno.ENOTDIR) else 'io_error')

    class BackupMixin:
        @contextmanager
        def _backup_directory(self):
            # Binding cannot change between opening a root and registering its
            # first owned artifact, including the snapshot preparation window.
            with self.backup_directory_authority():
                with self._open_backup_directory() as descriptor:
                    yield descriptor

        @contextmanager
        def _open_backup_directory(self):
            # Walk every ancestor using directory descriptors. A symlink swap
            # after open cannot redirect a later relative operation elsewhere.
            path = os.path.abspath(os.fspath(self.backup_dir))
            descriptor = None
            try:
                descriptor = os.open('/', os.O_RDONLY | os.O_DIRECTORY)
                for part in path.split('/'):
                    if not part:
                        continue
                    child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                    dir_fd=descriptor)
                    os.close(descriptor)
                    descriptor = child
                yield descriptor
            except OSError as error:
                raise _io(error) from error
            finally:
                if descriptor is not None:
                    os.close(descriptor)

        def _backup_checkpoint(self, phase):
            # Reference qualification subclasses override this for real process
            # exits; normal code has no environment-driven fault injection.
            pass

        def _bounded_backup_read(self, directory, name):
            descriptor = None
            try:
                descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                                     dir_fd=directory)
                info = os.fstat(descriptor)
                if not stat.S_ISREG(info.st_mode):
                    raise LibraryError('invalid_source')
                if info.st_size > MAX_BACKUP_BYTES:
                    raise LibraryError('too_large')
                with os.fdopen(descriptor, 'rb', closefd=True) as stream:
                    descriptor = None
                    raw = stream.read(MAX_BACKUP_BYTES + 1)
                if len(raw) > MAX_BACKUP_BYTES:
                    raise LibraryError('too_large')
                return raw, info
            except OSError as error:
                raise _io(error) from error
            finally:
                if descriptor is not None:
                    os.close(descriptor)

        def _backup_payload(self):
            payload = {'schema':3, 'generation':self._generation(), 'documents':[],
                       'revisions':[], 'blobs':[], 'collections':[], 'jobs':[]}
            encoded_size = len(canonical_bytes(payload))
            def append(key, value):
                nonlocal encoded_size
                encoded_size += len(canonical_bytes(value)) + bool(payload[key])
                if encoded_size > MAX_BACKUP_BYTES:
                    raise LibraryError('too_large')
                payload[key].append(value)
            for row in self.db.execute('SELECT document_id FROM documents ORDER BY source,document_id'):
                append('documents', self._record(row[0]))
            for row in self.db.execute('SELECT document_id,revision,blob_id FROM revisions ORDER BY document_id,revision'):
                append('revisions', dict(row))
            for row in self.db.execute('SELECT blob_id,content FROM blobs ORDER BY blob_id'):
                append('blobs', {'blob_id':row['blob_id'], 'text':bytes(row['content']).decode('utf-8')})
            for row in self.db.execute('SELECT name FROM collections ORDER BY name'):
                append('collections', row[0])
            for row in self.db.execute('SELECT j.*,c.enrolled FROM jobs j JOIN job_control c USING(job_id) ORDER BY job_id'):
                append('jobs', {'job':self._job(row), 'manifest_json':row['manifest'],
                               'content_hashes_json':row['content_hashes'],
                               'receipt_json':row['receipt'], 'enrolled':bool(row['enrolled'])})
            return payload

        def _register_backup(self, name, raw, payload):
            metadata = {'name':name, 'bytes':len(raw),
                        'payload_sha256':hashlib.sha256(canonical_bytes(payload)).hexdigest(),
                        'generation':payload['generation']}
            with self._write():
                existing = self.db.execute('SELECT * FROM backups WHERE name=?', (name,)).fetchone()
                if existing is not None and dict(existing) != metadata:
                    raise LibraryError('already_exists')
                self.db.execute('INSERT OR IGNORE INTO backups VALUES (?,?,?,?)',
                    tuple(metadata[key] for key in ('name','bytes','payload_sha256','generation')))
                self.record_error('backup', None)
            return metadata

        def backup(self, name):
            try:
                return self._create_backup(_basename(name))
            except (LibraryError, sqlite3.Error) as error:
                failure = self._backup_failure('backup', error)
                if failure is error:
                    raise
                raise failure from error

        def _create_backup(self, name):
            with self._backup_directory() as directory:
                self.cleanup_artifacts()
                try:
                    existing_info = os.stat(name, dir_fd=directory, follow_symlinks=False)
                except FileNotFoundError:
                    pass
                else:
                    if not stat.S_ISREG(existing_info.st_mode):
                        raise LibraryError('invalid_source')
                    raise LibraryError('already_exists')
                with self._read():
                    payload = self._backup_payload()
                raw = canonical_bytes({'format':'local-research-library-backup-v3',
                    'payload':payload, 'payload_sha256':hashlib.sha256(canonical_bytes(payload)).hexdigest()})
                if len(raw) > MAX_BACKUP_BYTES:
                    raise LibraryError('too_large')
                validate_backup(raw)
                artifact_id = uuid.uuid4().hex
                stage = '.backup-' + artifact_id + '--' + name + '.partial'
                # Hold the future artifact lock before enrollment so another
                # cleaner cannot mistake this live operation for abandonment.
                with self.artifact_lock(artifact_id, blocking=True):
                    self.register_artifact('backup_stage', 'backup', stage, artifact_id=artifact_id)
                    published = False
                    try:
                        descriptor = os.open(stage, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                                             0o600, dir_fd=directory)
                        with os.fdopen(descriptor, 'wb') as stream:
                            stream.write(raw)
                            stream.flush()
                            os.fsync(stream.fileno())
                        self._backup_checkpoint('before_publish')
                        try:
                            os.link(stage, name, src_dir_fd=directory, dst_dir_fd=directory,
                                    follow_symlinks=False)
                        except FileExistsError as error:
                            raise LibraryError('already_exists') from error
                        published = True
                        os.fsync(directory)
                        self._backup_checkpoint('after_publish')
                        metadata = self._register_backup(name, raw, payload)
                        self._backup_checkpoint('after_registry')
                        os.unlink(stage, dir_fd=directory)
                        os.fsync(directory)
                        with self._write():
                            self.db.execute('DELETE FROM maintenance_artifacts WHERE artifact_id=?', (artifact_id,))
                        self.cleanup_artifacts()
                        return metadata
                    except BaseException:
                        # Publication may be complete even though its registry
                        # commit failed. Preserve the linked owned stage for
                        # startup to authenticate and complete registration.
                        if not published:
                            try:
                                os.unlink(stage, dir_fd=directory)
                                os.fsync(directory)
                            except FileNotFoundError:
                                pass
                            with self._write():
                                self.db.execute('DELETE FROM maintenance_artifacts WHERE artifact_id=?', (artifact_id,))
                        raise

        def list_backups(self, offset=0, limit=100):
            page(offset, limit)
            with self.backup_directory_authority(), self._read():
                total = self.db.execute('SELECT count(*) FROM backups').fetchone()[0]
                rows = self.db.execute('SELECT * FROM backups ORDER BY name LIMIT ? OFFSET ?', (limit,offset))
                return {'backups':[dict(row) for row in rows], 'total':total}

        def _recover_backup_artifact(self, row):
            # Core calls while holding this artifact's exclusive lock. The row
            # owns only the staging path. A complete final file is ours only
            # when it is the same inode as that tracked stage.
            match = _STAGE.fullmatch(row['relative_path'])
            if (row['root_kind'] != 'backup' or match is None
                    or match.group(1) != row['artifact_id']):
                return False
            stage, name = row['relative_path'], match.group(2)
            try:
                with self._backup_directory() as directory:
                    try:
                        stage_info = os.stat(stage, dir_fd=directory, follow_symlinks=False)
                    except FileNotFoundError:
                        return True
                    if not stat.S_ISREG(stage_info.st_mode):
                        return False
                    try:
                        final_info = os.stat(name, dir_fd=directory, follow_symlinks=False)
                    except FileNotFoundError:
                        os.unlink(stage, dir_fd=directory)
                        os.fsync(directory)
                        return True
                    if (not stat.S_ISREG(final_info.st_mode)
                            or (stage_info.st_dev,stage_info.st_ino) != (final_info.st_dev,final_info.st_ino)):
                        # A conflicting operator file is never ours to remove
                        # or register. The tracked abandoned stage still is.
                        os.unlink(stage, dir_fd=directory)
                        os.fsync(directory)
                        return True
                    raw, opened_info = self._bounded_backup_read(directory, name)
                    if (opened_info.st_dev,opened_info.st_ino) != (stage_info.st_dev,stage_info.st_ino):
                        return False
                    payload = validate_backup(raw)
                    os.fsync(directory)
                    existing = self.db.execute('SELECT * FROM backups WHERE name=?', (name,)).fetchone()
                    self._register_backup(name, raw, payload)
                    if existing is None:
                        self._backup_checkpoint('recovery_registered')
                        # Core recovery is inside its cleanup transaction. Keep
                        # the ownership witness until that OUTER commit; the
                        # next bounded sweep can remove the stage safely.
                        return False
                    os.unlink(stage, dir_fd=directory)
                    os.fsync(directory)
                    return True
            except (LibraryError, OSError):
                return False

        def restore_backup(self, name, expected_generation):
            try:
                name = _basename(name)
                if type(expected_generation) is not int or expected_generation < 0:
                    raise LibraryError('invalid_request')
                with self._backup_directory() as directory:
                    raw, _ = self._bounded_backup_read(directory, name)
                # Entire validation precedes lock acquisition and any mutation.
                payload = validate_backup(raw)
                with self.maintenance_authority():
                    self.cleanup_artifacts()
                    with self._write():
                        live_generation = self._generation()
                        if expected_generation != live_generation:
                            raise LibraryError('stale_generation')
                        self._activate_backup(payload)
                        self.db.execute("UPDATE metadata SET value=? WHERE key='catalog_generation'",
                                        (str(live_generation + 1),))
                        self._catalog_changed = False
                        self._set_control('incarnation', uuid.uuid4().hex)
                        self._set_control('worker_generation', str(int(self._control('worker_generation')) + 1))
                        last_error = json.loads(self._control('last_error'))
                        if last_error is not None and last_error['operation'] == 'restore':
                            self._set_control('last_error', 'null')
                        self._backup_checkpoint('before_activate_commit')
                    self.adopt_incarnation()
                    self._backup_checkpoint('after_activate_commit')
                    self.cleanup_artifacts()
                return {'restored':True,'generation':live_generation+1,
                        'documents':len(payload['documents']),'jobs':len(payload['jobs'])}
            except (LibraryError, sqlite3.Error, OverflowError) as error:
                # Defensive rollback for unrepresentable SQLite counters. The
                # public counter-range contract remains an explicit open gap;
                # this does not privately narrow backup format acceptance.
                failure = self._backup_failure('restore', error)
                if failure is error:
                    raise
                raise failure from error

        def _backup_failure(self, operation, error):
            failure = error if isinstance(error, LibraryError) else LibraryError('io_error')
            try:
                self.record_error(operation, failure.code)
            except (LibraryError, sqlite3.Error):
                # A locked, stale, or damaged database may also prevent the
                # diagnostics write; preserve the original public failure.
                pass
            return failure

        def _activate_backup(self, payload):
            # Physical schema4 will override this method in the M4 reference.
            # Never downgrade or claim support for a physical schema not built.
            schema = self.db.execute("SELECT value FROM metadata WHERE key='schema'").fetchone()[0]
            if schema != '3':
                raise LibraryError('unsupported_schema')
            document_highwater = {row[0]:row[1] for row in self.db.execute(
                'SELECT document_id,edit_high_water FROM document_control')}
            job_highwater = {row[0]:row[1] for row in self.db.execute(
                'SELECT job_id,epoch_high_water FROM job_control')}
            # Also absorb the live rows in the activation transaction; target
            # maxima remain durable even if a past reference lacked tracking.
            for row in self.db.execute('SELECT document_id,edit_version FROM lifecycle'):
                document_highwater[row[0]] = max(document_highwater.get(row[0],0),row[1])
            for row in self.db.execute('SELECT job_id,epoch FROM jobs'):
                job_highwater[row[0]] = max(job_highwater.get(row[0],0),row[1])
            for identifier, highwater in document_highwater.items():
                self.db.execute('INSERT INTO document_control VALUES (?,?) ON CONFLICT(document_id) '
                    'DO UPDATE SET edit_high_water=max(edit_high_water,excluded.edit_high_water)', (identifier,highwater))
            for identifier, highwater in job_highwater.items():
                self.db.execute('INSERT INTO job_control VALUES (?,?,0) ON CONFLICT(job_id) '
                    'DO UPDATE SET epoch_high_water=max(epoch_high_water,excluded.epoch_high_water),enrolled=0',
                    (identifier,highwater))
            for table in ('search_entries','revisions','lifecycle','documents','blobs','collections','jobs'):
                self.db.execute('DELETE FROM ' + table)
            self.db.execute('UPDATE search_state SET published_generation=NULL,target_generation=NULL,'
                            'processed=0,total=0,cursor=NULL WHERE singleton=1')
            for blob in payload['blobs']:
                self.db.execute('INSERT INTO blobs VALUES (?,?)', (blob['blob_id'],blob['text'].encode('utf-8')))
            for name in payload['collections']:
                self.db.execute('INSERT INTO collections VALUES (?)', (name,))
            for record in payload['documents']:
                document = record['document']
                identifier = document['document_id']
                edit_version = max(record['edit_version'],document_highwater.get(identifier,0)) + 1
                self.db.execute('INSERT INTO documents VALUES (?,?,?,?,?)',
                    tuple(document[key] for key in ('document_id','source_id','source','blob_id','title')))
                self.db.execute('INSERT INTO lifecycle VALUES (?,?,?,?,?,?,?)',
                    (identifier,record['revision'],edit_version,int(record['deleted']),record['notes'],
                     json.dumps(record['tags'],ensure_ascii=True,separators=(',',':')),
                     json.dumps(record['collections'],ensure_ascii=True,separators=(',',':'))))
                self.db.execute('INSERT INTO document_control VALUES (?,?) ON CONFLICT(document_id) '
                    'DO UPDATE SET edit_high_water=max(edit_high_water,excluded.edit_high_water)', (identifier,edit_version))
            for revision in payload['revisions']:
                self.db.execute('INSERT INTO revisions VALUES (?,?,?)',
                    tuple(revision[key] for key in ('document_id','revision','blob_id')))
            for item in payload['jobs']:
                job = item['job']
                identifier = job['job_id']
                epoch = (job['epoch'] if job['state'] == 'completed'
                         else max(job['epoch'],job_highwater.get(identifier,0)) + 1)
                self.db.execute('INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?,?)',
                    (identifier,epoch,job['state'],job['total'],job['completed'],job['error'],
                     item['manifest_json'],item['content_hashes_json'],item['receipt_json']))
                self.db.execute('INSERT INTO job_control VALUES (?,?,?) ON CONFLICT(job_id) '
                    'DO UPDATE SET epoch_high_water=max(epoch_high_water,excluded.epoch_high_water),'
                    'enrolled=excluded.enrolled',
                    (identifier,max(epoch,job_highwater.get(identifier,0)),int(item['enrolled'])))
''')


def backup_files() -> dict[str, str]:
    """Return the owned generated mixin; composition supplies the control store."""
    return {"library/catalog/backup.py": _BACKUP}
