"""Prospective cumulative-v2 maintenance overlay; authored development control only."""
from __future__ import annotations

import ast
from textwrap import dedent, indent


_CONTROL = dedent(r'''
    """Explicit root ownership, synchronized live worker claims and checked restore."""
    from contextlib import contextmanager, ExitStack
    import errno
    import fcntl
    import hashlib
    import json
    import os
    from pathlib import Path
    import sqlite3
    import stat
    import uuid

    from library.common import LibraryError
    from pathlib import PurePosixPath
    import re
    from library.counters import counter, increment
    from library.catalog.backup import _basename, MAX_BACKUP_BYTES
    from library.catalog.backup_format import canonical_bytes, validate_backup

    def _identity(info):
        return (info.st_dev, info.st_ino, info.st_mode, info.st_size,
                info.st_mtime_ns, info.st_ctime_ns)

    @contextmanager
    def lexical_directory(path):
        # Do not normalize away an ancestor before proving it is not a symlink.
        raw = os.fspath(path)
        if not isinstance(raw, str) or not raw or '\x00' in raw:
            raise LibraryError('invalid_source')
        if not os.path.isabs(raw):
            raw = os.getcwd() + '/' + raw
        descriptor = None
        try:
            descriptor = os.open('/', os.O_RDONLY | os.O_DIRECTORY)
            for part in raw.split('/'):
                if not part:
                    continue
                info = os.stat(part, dir_fd=descriptor, follow_symlinks=False)
                if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
                    raise LibraryError('invalid_source')
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                dir_fd=descriptor)
                if (os.fstat(child).st_dev, os.fstat(child).st_ino) != (info.st_dev, info.st_ino):
                    os.close(child)
                    raise LibraryError('io_error')
                os.close(descriptor)
                descriptor = child
            yield descriptor, os.path.abspath(raw)
        except OSError as error:
            code = 'invalid_source' if error.errno in (errno.ELOOP, errno.ENOTDIR) else 'io_error'
            raise LibraryError(code) from error
        finally:
            if descriptor is not None:
                os.close(descriptor)

    class V2MaintenanceMixin:
        def configure_backup_dir(self, path):
            selected = str(Path(self.path).parent / 'backups') if path is None else os.fspath(path)
            if not isinstance(selected, str) or not selected or '\x00' in selected:
                raise LibraryError('invalid_source')
            self._backup_lexical = selected
            self.backup_dir = Path(os.path.abspath(selected))

        def _bound_backup_root(self):
            row = self.db.execute("SELECT value FROM control WHERE key='backup_root'").fetchone()
            if row is None:
                return None
            try:
                bound = json.loads(row[0])
            except (TypeError, ValueError) as error:
                raise LibraryError('invalid_database') from error
            if bound is not None and (type(bound) is not str or not os.path.isabs(bound)
                    or os.path.abspath(bound) != bound):
                raise LibraryError('invalid_database')
            return bound

        def _assert_backup_binding(self):
            bound = self._bound_backup_root()
            if bound is None:
                raise LibraryError('backup_root_unbound')
            if bound != str(self.backup_dir):
                raise LibraryError('backup_root_mismatch')
            return bound

        @contextmanager
        def backup_directory_authority(self):
            with self._maintenance_shared(), self._lock('backup-root.lock', exclusive=False):
                with self._read():
                    self._assert_backup_binding()
                yield self.backup_dir

        @contextmanager
        def _open_backup_directory(self):
            self._assert_backup_binding()
            with lexical_directory(getattr(self, '_backup_lexical', str(self.backup_dir))) as (descriptor, selected):
                if selected != str(self.backup_dir):
                    raise LibraryError('backup_root_mismatch')
                yield descriptor

        def _pending_adoption_artifacts(self):
            # A completed active search descriptor is the sole published row
            # which needs no recovery/finalization. An unfinished shadow is pending.
            return self.db.execute("SELECT 1 FROM maintenance_artifacts AS a WHERE NOT ("
                "a.kind='index_generation' AND a.root_kind='maintenance' AND a.state='published' "
                "AND EXISTS (SELECT 1 FROM search_state AS s WHERE s.singleton=1 "
                "AND s.published_generation IS NOT NULL AND s.processed=s.total "
                "AND s.published_generation=s.target_generation "
                "AND a.relative_path=CAST(s.published_generation AS TEXT))) LIMIT 1").fetchone() is not None

        def _adoption_archive_pass(self, directory, *, validate):
            digest = hashlib.sha256()
            count = 0
            after = ''
            while True:
                rows = self.db.execute('SELECT * FROM backups WHERE name>? ORDER BY name LIMIT 100',
                                       (after,)).fetchall()
                if not rows:
                    break
                for row in rows:
                    name = _basename(row['name'])
                    if validate:
                        raw, info = self._bounded_backup_read(directory, name)
                        payload = validate_backup(raw)
                        if (type(row['bytes']) is not int or row['bytes'] != len(raw)
                                or row['payload_sha256'] != hashlib.sha256(canonical_bytes(payload)).hexdigest()
                                or type(row['generation']) is not int or row['generation'] != payload['generation']):
                            raise LibraryError('invalid_backup')
                        # The descriptor's observed identity must still name the
                        # same regular file after the bounded read/validation.
                        current = os.stat(name, dir_fd=directory, follow_symlinks=False)
                        if not stat.S_ISREG(current.st_mode):
                            raise LibraryError('invalid_source')
                        if _identity(info) != _identity(current):
                            raise LibraryError('io_error')
                    else:
                        info = os.stat(name, dir_fd=directory, follow_symlinks=False)
                        if not stat.S_ISREG(info.st_mode):
                            raise LibraryError('invalid_source')
                    digest.update(json.dumps([name, _identity(info)], separators=(',', ':')).encode('utf-8'))
                    count += 1
                    after = name
            return count, digest.digest()

        def adopt_backup_root(self, expected_root):
            if expected_root is not None and (type(expected_root) is not str
                    or not os.path.isabs(expected_root) or os.path.abspath(expected_root) != expected_root
                    or '\x00' in expected_root):
                raise LibraryError('invalid_request')
            try:
                with self._write():
                    pass
                with self.maintenance_authority(), self._lock('backup-root.lock'), self._write():
                    previous = self._bound_backup_root()
                    if previous != expected_root:
                        raise LibraryError('backup_root_mismatch')
                    if self._pending_adoption_artifacts():
                        raise LibraryError('maintenance_busy')
                    lexical = getattr(self, '_backup_lexical', str(self.backup_dir))
                    with lexical_directory(lexical) as (directory, selected):
                        identity = os.fstat(directory)
                        registered, identities = self._adoption_archive_pass(directory, validate=True)
                        self._backup_checkpoint('adoption_validated')
                        # Stream the identity census again instead of retaining
                        # an unbounded list of stat records or open descriptors.
                        if (registered, identities) != self._adoption_archive_pass(directory, validate=False):
                            raise LibraryError('io_error')
                        with lexical_directory(lexical) as (reopened, current):
                            other = os.fstat(reopened)
                            if current != selected or (identity.st_dev, identity.st_ino) != (other.st_dev, other.st_ino):
                                raise LibraryError('io_error')
                        if previous != selected:
                            self._set_control('backup_root', json.dumps(selected, ensure_ascii=True, separators=(',', ':')))
                    return {'adopted': previous != selected, 'registered': registered}
            except OSError as error:
                raise LibraryError('io_error') from error

        @contextmanager
        def _worker_observation(self, *, exclusive=False):
            if getattr(self, '_worker_observation_depth', 0):
                yield
                return
            with self._lock('worker-observe.lock', exclusive=exclusive, blocking=True):
                self._worker_observation_depth = 1
                try:
                    yield
                finally:
                    self._worker_observation_depth = 0

        def _write_live_claim(self, claim):
            # These process-only bytes never establish liveness by themselves.
            # The owner holds worker-live.lock and changes them under the same
            # observation authority used before diagnostics opens its DB snapshot.
            owner = self._worker_claim or {key: claim[key] for key in ('incarnation', 'generation')}
            state = {'owner': owner, 'claim': claim if 'job_id' in claim else None}
            raw = json.dumps(state, sort_keys=True, separators=(',', ':')).encode('utf-8')
            path = self.maintenance_root / 'worker-live.json'
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
            try:
                if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                    raise LibraryError('invalid_source')
                with os.fdopen(descriptor, 'wb', closefd=False) as stream:
                    stream.write(raw)
                    stream.flush()
                    os.fsync(descriptor)
            finally:
                os.close(descriptor)

        @contextmanager
        def _worker_reservation(self):
            # Existing owner authority is checked before allocation. If no
            # lock file exists yet, reject exhaustion before creating one.
            descriptor = None
            try:
                path = self.maintenance_root / 'worker.lock'
                try:
                    descriptor = os.open(path, os.O_RDWR | os.O_NOFOLLOW)
                except FileNotFoundError:
                    with self._write():
                        increment(int(self._control('worker_generation')))
                    descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
                if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                    raise LibraryError('invalid_source')
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except OSError as error:
                    if error.errno in (errno.EACCES, errno.EAGAIN):
                        raise LibraryError('worker_busy') from error
                    raise
                yield
            except OSError as error:
                raise LibraryError('io_error') from error
            finally:
                if descriptor is not None:
                    os.close(descriptor)

        @contextmanager
        def worker_owner(self):
            with self._write():
                pass
            if self._worker_claim is not None:
                raise LibraryError('worker_busy')
            locks = ExitStack()
            owner = None
            try:
                locks.enter_context(self._worker_reservation())
                with self._write():
                    increment(int(self._control('worker_generation')))
                with self._worker_observation(exclusive=True):
                    with self._write():
                        generation = increment(int(self._control('worker_generation')))
                        locks.enter_context(self._lock('worker-live.lock', busy='worker_busy'))
                        owner = {'incarnation': self._control('incarnation'), 'generation': generation}
                        self._set_control('worker_generation', str(generation))
                        self._write_live_claim(owner)
                    self._worker_claim = owner
                try:
                    with self.worker_guard(owner):
                        self.cleanup_artifacts()
                except (LibraryError, OSError, sqlite3.Error) as error:
                    code = error.code if isinstance(error, LibraryError) else 'io_error'
                    if code not in ('counter_exhausted', 'stale_instance', 'stale_worker', 'stale_epoch'):
                        try:
                            self._worker_error(owner, code)
                        except (LibraryError, OSError, sqlite3.Error):
                            pass
                    if isinstance(error, LibraryError):
                        raise
                    raise LibraryError('io_error') from error
                yield dict(owner)
            finally:
                if owner is None:
                    locks.close()
                else:
                    with self._worker_observation(exclusive=True):
                        self._worker_claim = None
                        locks.close()

        @contextmanager
        def active_worker_claim(self, claim, *, preflight=False):
            with self._worker_observation(exclusive=True), self.worker_guard(claim):
                if preflight:
                    from library.catalog.worker import VALIDATION_ERRORS
                    try:
                        self.preflight_job_commit(claim['job_id'], claim['epoch'])
                    except LibraryError as error:
                        if error.code not in VALIDATION_ERRORS:
                            raise
                self._write_live_claim(claim)
            try:
                yield
            finally:
                with self._worker_observation(exclusive=True):
                    # A fenced old process never overwrites a new owner's claim.
                    with self._read():
                        if (self._worker_claim is not None
                                and self._control('incarnation') == self._worker_claim['incarnation']
                                and int(self._control('worker_generation')) == self._worker_claim['generation']):
                            self._write_live_claim(self._worker_claim)

        @contextmanager
        def worker_guard(self, claim):
            if (type(claim) is not dict or set(claim) not in (
                    {'incarnation', 'generation'}, {'incarnation', 'generation', 'job_id', 'epoch'})
                    or type(claim.get('incarnation')) is not str):
                raise LibraryError('invalid_request')
            counter(claim.get('generation'))
            if 'job_id' in claim:
                from library.catalog.validation import check_job_id
                check_job_id(claim['job_id'])
                counter(claim.get('epoch'), minimum=1)
            with self._write():
                if claim['incarnation'] != self._control('incarnation'):
                    raise LibraryError('stale_instance')
                if (self._worker_claim is None
                        or claim['generation'] != int(self._control('worker_generation'))
                        or any(claim.get(key) != value for key, value in self._worker_claim.items())):
                    raise LibraryError('stale_worker')
                row = self._fenced(claim['job_id'], claim['epoch']) if 'job_id' in claim else None
                yield row

        def worker_state(self):
            with self._worker_observation(), self._read():
                descriptor = None
                try:
                    descriptor = os.open(self.maintenance_root / 'worker-live.lock', os.O_RDONLY | os.O_NOFOLLOW)
                    if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                        raise LibraryError('invalid_source')
                    try:
                        fcntl.flock(descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
                    except OSError as error:
                        if error.errno not in (errno.EACCES, errno.EAGAIN):
                            raise
                    else:
                        return 'stopped'
                    claim_fd = os.open(self.maintenance_root / 'worker-live.json', os.O_RDONLY | os.O_NOFOLLOW)
                    with os.fdopen(claim_fd, 'rb') as stream:
                        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                            raise LibraryError('invalid_source')
                        raw = stream.read(4097)
                    if len(raw) > 4096:
                        return 'stopped'
                    try:
                        state = json.loads(raw)
                    except (ValueError, UnicodeError):
                        return 'stopped'
                    if type(state) is not dict or set(state) != {'owner', 'claim'}:
                        return 'stopped'
                    owner, claim = state['owner'], state['claim']
                    if (type(owner) is not dict or set(owner) != {'incarnation', 'generation'}
                            or owner['incarnation'] != self._control('incarnation')
                            or type(owner['generation']) is not int
                            or owner['generation'] != int(self._control('worker_generation'))):
                        return 'stopped'
                    if (type(claim) is not dict
                            or set(claim) != {'incarnation', 'generation', 'job_id', 'epoch'}
                            or any(claim.get(key) != value for key, value in owner.items())
                            or type(claim['generation']) is not int or type(claim['job_id']) is not str):
                        return 'idle'
                    row = self.db.execute('SELECT j.epoch,j.state,c.enrolled FROM jobs j '
                                          'JOIN job_control c USING(job_id) WHERE j.job_id=?',
                                          (claim['job_id'],)).fetchone()
                    return ('running' if row is not None and row['enrolled'] == 1
                            and type(claim['epoch']) is int and row['epoch'] == claim['epoch'] else 'idle')
                except FileNotFoundError:
                    return 'stopped'
                except OSError as error:
                    raise LibraryError('io_error') from error
                finally:
                    if descriptor is not None:
                        os.close(descriptor)

        def diagnostics(self):
            with self._worker_observation():
                return super().diagnostics()

        def _restore_allocation(self, payload):
            generation = increment(self._generation())
            worker_generation = increment(int(self._control('worker_generation')))
            documents = {}
            jobs = {}
            for record in payload['documents']:
                identifier = record['document']['document_id']
                high = self.db.execute('SELECT edit_high_water FROM document_control WHERE document_id=?',
                                       (identifier,)).fetchone()
                documents[identifier] = increment(max(record['edit_version'], 0 if high is None else high[0]))
            for item in payload['jobs']:
                job = item['job']
                identifier = job['job_id']
                high = self.db.execute('SELECT epoch_high_water FROM job_control WHERE job_id=?',
                                       (identifier,)).fetchone()
                maximum = max(job['epoch'], 0 if high is None else high[0])
                jobs[identifier] = (job['epoch'] if job['state'] == 'completed' else increment(maximum))
            return generation, worker_generation, documents, jobs

        def backup(self, name):
            name = _basename(name)
            # Hold shared maintenance authority from incarnation validation
            # through publication, so a migration/restore cannot intervene.
            with self._maintenance_shared():
                with self._write():
                    pass
                return super().backup(name)

        def restore_backup(self, name, expected_generation):
            name = _basename(name)
            counter(expected_generation)
            try:
                with self._maintenance_shared():
                    with self._write():
                        pass
                    with self._backup_directory() as directory:
                        raw, _ = self._bounded_backup_read(directory, name)
                    payload = validate_backup(raw)
                with self.maintenance_authority(), self._lock('backup-root.lock', exclusive=False):
                    with self._write():
                        self._assert_backup_binding()
                        if expected_generation != self._generation():
                            raise LibraryError('stale_generation')
                        generation, worker, _, _ = self._restore_allocation(payload)
                        self._activate_backup(payload)
                        self.db.execute("UPDATE metadata SET value=? WHERE key='catalog_generation'", (str(generation),))
                        self._catalog_changed = False
                        self._set_control('incarnation', uuid.uuid4().hex)
                        self._set_control('worker_generation', str(worker))
                        previous = json.loads(self._control('last_error'))
                        if previous is not None and previous['operation'] == 'restore':
                            self._set_control('last_error', 'null')
                        self._backup_checkpoint('before_activate_commit')
                    self.adopt_incarnation()
                    self._backup_checkpoint('after_activate_commit')
                    self.cleanup_artifacts()
                return {'restored': True, 'generation': generation,
                        'documents': len(payload['documents']), 'jobs': len(payload['jobs'])}
            except (LibraryError, sqlite3.Error) as error:
                failure = self._backup_failure('restore', error)
                if failure is error:
                    raise
                raise failure from error

        def _backup_failure(self, operation, error):
            if isinstance(error, LibraryError) and error.code in (
                    'counter_exhausted', 'stale_instance', 'invalid_request',
                    'backup_root_unbound', 'backup_root_mismatch'):
                return error
            return super()._backup_failure(operation, error)
''').lstrip()


def _replace(source: str, old: str, new: str) -> str:
    if source.count(old) != 1:
        raise ValueError("Frozen maintenance seam changed: " + old[:80])
    return source.replace(old, new)


def maintenance_files(base: dict[str, str]) -> dict[str, str]:
    """Return only explicit generated maintenance replacements; never mutate base."""
    worker = base["library/catalog/worker.py"]
    worker = _replace(worker,
        "from library.catalog.validation import validate_entries",
        "from library.catalog.validation import validate_entries, check_job_id")
    worker = _replace(worker,
        "    def enqueue_job(self, job_id):\n        with self._write():",
        "    def enqueue_job(self, job_id):\n        check_job_id(job_id)\n        with self._write():")
    worker = _replace(worker,
        "        try:\n            failure = None",
        "        with self.active_worker_claim(claim, preflight=True):\n            return self._worker_claim_once(claim)\n\n    def _worker_claim_once(self, claim):\n        try:\n            failure = None")
    worker = _replace(worker,
        "if error.code not in ('stale_worker', 'stale_epoch', 'job_state'):",
        "if error.code not in ('stale_instance', 'stale_worker', 'stale_epoch', 'job_state', 'counter_exhausted'):")
    runner = _replace(base["library/ingestion/worker.py"],
        "            except LibraryError as error:\n                _cleanup(store, owner)",
        "            except LibraryError as error:\n                if error.code in ('counter_exhausted', 'stale_instance', 'stale_worker', 'stale_epoch'):\n                    raise\n                _cleanup(store, owner)")
    runner = _replace(runner,
        "if code in ('stale_worker', 'stale_epoch'):",
        "if code in ('counter_exhausted', 'stale_instance', 'stale_worker', 'stale_epoch'):")
    maintenance = _replace(base["library/catalog/maintenance.py"],
        "        except LibraryError as error:\n            self.record_error('reindex', error.code)",
        "        except LibraryError as error:\n            if error.code not in ('counter_exhausted', 'stale_instance', 'invalid_request'):\n                self.record_error('reindex', error.code)")
    maintenance = _replace(maintenance,
        "    def reindex_step(self, limit=64):\n        try:",
        "    def reindex_step(self, limit=64):\n        if type(limit) is not int or not 1 <= limit <= 64:\n            raise LibraryError('invalid_request')\n        try:")
    backup = _replace(base["library/catalog/backup.py"],
        "from library.common import LibraryError, page",
        "from library.common import LibraryError, page\nfrom library.counters import increment")
    backup = backup.replace("max(record['edit_version'],document_highwater.get(identifier,0)) + 1",
                            "increment(max(record['edit_version'],document_highwater.get(identifier,0)))")
    backup = backup.replace("max(job['epoch'],job_highwater.get(identifier,0)) + 1",
                            "increment(max(job['epoch'],job_highwater.get(identifier,0)))")
    result = {"library/catalog/worker.py": worker,
              "library/ingestion/worker.py": runner,
              "library/catalog/maintenance.py": maintenance,
              "library/catalog/backup.py": backup}
    if "library/catalog/m4_backup.py" in base:
        m4backup = _replace(base["library/catalog/m4_backup.py"],
            "from library.common import LibraryError", "from library.common import LibraryError\nfrom library.counters import increment")
        m4backup = _replace(m4backup, "max(record['edit_version'],document_highwater.get(identifier,0)) + 1",
                            "increment(max(record['edit_version'],document_highwater.get(identifier,0)))")
        m4backup = _replace(m4backup, "max(job['epoch'],job_highwater.get(identifier,0)) + 1",
                            "increment(max(job['epoch'],job_highwater.get(identifier,0)))")
        result["library/catalog/m4_backup.py"] = m4backup
    # Copy only three named control methods into the first-MRO v2 mixin. The
    # frozen originals stay immutable; source seams are checked, never guessed.
    control = base.get("library/catalog/m4_control.py", base["library/catalog/control.py"])
    tree = ast.parse(control)
    core = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "CoreStore")
    methods = {node.name: dedent("\n".join(control.splitlines()[node.lineno - 1:node.end_lineno])) + "\n"
               for node in core.body if isinstance(node, ast.FunctionDef)}
    register = _replace(methods["register_artifact"],
        "        if root_kind == 'backup' and self._control('backup_root') != str(self.backup_dir):\n            raise LibraryError('maintenance_busy')",
        "        if root_kind == 'backup':\n            self._assert_backup_binding()")
    cleanup = _replace(methods["cleanup_artifacts"],
        "        cursor = json.loads(self._control('cleanup_cursor'))",
        "        try:\n            self._assert_backup_binding()\n            scope = ''\n        except LibraryError as error:\n            if error.code not in ('backup_root_unbound', 'backup_root_mismatch'):\n                raise\n            scope = \"root_kind='maintenance' AND \"\n        cursor = json.loads(self._control('cleanup_cursor'))")
    cleanup = _replace(cleanup,
        "'SELECT * FROM maintenance_artifacts WHERE artifact_id>? '",
        "'SELECT * FROM maintenance_artifacts WHERE ' + scope + 'artifact_id>? '")
    cleanup = _replace(cleanup,
        "'SELECT * FROM maintenance_artifacts ORDER BY artifact_id LIMIT ?'",
        "'SELECT * FROM maintenance_artifacts WHERE ' + scope + '1=1 ORDER BY artifact_id LIMIT ?'")
    cleanup = _replace(cleanup,
        "                    if row['root_kind']=='backup' and self._control('backup_root') != str(self.backup_dir):\n                        raise LibraryError('maintenance_busy')",
        "                    if row['root_kind']=='backup':\n                        self._assert_backup_binding()")
    remove = dedent(r'''
        def _remove_owned_file(self, row):
            root = self.maintenance_root
            if row['root_kind'] == 'backup':
                self._assert_backup_binding()
                root = getattr(self, '_backup_lexical', str(self.backup_dir))
            parts = PurePosixPath(row['relative_path']).parts
            with lexical_directory(root) as (directory, _):
                descriptor = os.dup(directory)
                try:
                    for part in parts[:-1]:
                        child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                        dir_fd=descriptor)
                        os.close(descriptor)
                        descriptor = child
                    try:
                        info = os.stat(parts[-1], dir_fd=descriptor, follow_symlinks=False)
                    except FileNotFoundError:
                        return True
                    if not stat.S_ISREG(info.st_mode):
                        return False
                    os.unlink(parts[-1], dir_fd=descriptor)
                    os.fsync(descriptor)
                    return True
                finally:
                    os.close(descriptor)
    ''').lstrip()
    result["library/catalog/v2_maintenance.py"] = _CONTROL + "\n" + indent(register + "\n" + remove + "\n" + cleanup, "    ")
    return result
