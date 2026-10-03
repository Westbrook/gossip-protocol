"""Trusted authored ingestion package for the M1 integration development fixture.

These source strings are infrastructure qualification input, not model output or
private acceptance evidence. The v0 importer retains its format semantics while
using the M1 descriptor reader to reject root and parent symlinks race-safely.
"""

from __future__ import annotations

from textwrap import dedent


_JOBS = dedent(r'''
    """Durable ingestion coordination and bounded local intake without extraction."""
    import errno
    import io
    import json
    import os
    from pathlib import PurePosixPath
    import stat
    import zipfile
    import zlib

    from library.common import LibraryError, source_key
    from library.catalog.validation import validate_entries

    MEMBER_BYTES = 32768
    TOTAL_BYTES = 524288
    ARCHIVE_BYTES = 1048576
    FILE_COUNT = 64

    def _key(value):
        source_key(value)
        if len(value.split('/')) > 16:
            raise LibraryError('invalid_source')
        return value

    def _member_key(value):
        _key(value)
        if PurePosixPath(value).suffix.lower() not in ('.txt', '.md', '.html'):
            raise LibraryError('unsupported_type')
        return value

    def _decode(raw):
        if len(raw) > MEMBER_BYTES:
            raise LibraryError('too_large')
        try:
            return raw.decode('utf-8')
        except UnicodeError as error:
            raise LibraryError('invalid_utf8') from error

    def _io_error(error):
        if getattr(error, 'errno', None) == errno.ELOOP:
            return LibraryError('invalid_source')
        return LibraryError('io_error')

    def _child_fd(parent_fd, name, directory=False):
        """Check for a symlink, then open race-safely without following it."""
        info = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if stat.S_ISLNK(info.st_mode):
            raise LibraryError('invalid_source')
        if directory and not stat.S_ISDIR(info.st_mode):
            raise LibraryError('io_error')
        if not directory and not stat.S_ISREG(info.st_mode):
            raise LibraryError('invalid_source')
        flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
        if directory:
            flags |= os.O_DIRECTORY
        fd = os.open(name, flags, dir_fd=parent_fd)
        mode = os.fstat(fd).st_mode
        if not (stat.S_ISDIR(mode) if directory else stat.S_ISREG(mode)):
            os.close(fd)
            raise LibraryError('invalid_source')
        return fd

    def _path_fd(path, directory=False):
        """Walk every ancestor from /; neither resolve() nor following open()."""
        try:
            raw = os.fspath(path)
            if type(raw) is not str or not raw or '\x00' in raw:
                raise LibraryError('invalid_source')
            if '..' in raw.split('/'):
                raise LibraryError('invalid_source')
            absolute = os.path.abspath(raw)
            parts = [part for part in absolute.split('/') if part]
            current = os.open('/', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                for index, part in enumerate(parts):
                    child = _child_fd(current, part, directory or index < len(parts) - 1)
                    os.close(current)
                    current = child
                if not parts and not directory:
                    raise LibraryError('invalid_source')
                result, current = current, None
                return result
            finally:
                if current is not None:
                    os.close(current)
        except (OSError, TypeError, ValueError) as error:
            if isinstance(error, LibraryError):
                raise
            raise _io_error(error) from error

    def _read_fd(fd, limit):
        pieces, remaining = [], limit + 1
        while remaining:
            piece = os.read(fd, min(65536, remaining))
            if not piece:
                break
            pieces.append(piece)
            remaining -= len(piece)
        raw = b''.join(pieces)
        if len(raw) > limit:
            raise LibraryError('too_large')
        return raw

    def _read_path(path, limit):
        fd = _path_fd(path)
        try:
            return _read_fd(fd, limit)
        except OSError as error:
            raise _io_error(error) from error
        finally:
            os.close(fd)

    def _append(entries, seen, source, text, total):
        if len(entries) >= FILE_COUNT or source in seen:
            raise LibraryError('invalid_batch')
        try:
            count = len(text.encode('utf-8'))
        except UnicodeError as error:
            raise LibraryError('invalid_utf8') from error
        if count > MEMBER_BYTES or total + count > TOTAL_BYTES:
            raise LibraryError('too_large')
        seen.add(source)
        entries.append({'source': source, 'text': text})
        return total + count

    def _json_object(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise LibraryError('invalid_json')
            result[key] = value
        return result

    def _json_constant(value):
        raise LibraryError('invalid_json')

    class JobManager:
        def __init__(self, store):
            self.store = store

        def submit(self, job_id, entries):
            return self.store.create_job(job_id, entries)

        def get(self, job_id):
            return self.store.get_job(job_id)

        def prepare(self, job_id):
            job = self.store.get_job(job_id)
            if job['state'] == 'running':
                return {'job_id': job_id, 'epoch': job['epoch']}
            if job['state'] != 'queued':
                raise LibraryError('job_state')
            try:
                validate_entries(self.store.job_manifest(job_id), self.store.documents())
            except LibraryError as error:
                self.store.fail_job(job_id, job['epoch'], error.code)
                raise
            return self.store.start_job(job_id, job['epoch'])

        def commit(self, token, *, fail_before_commit=False):
            if (type(token) is not dict or set(token) != {'job_id', 'epoch'}
                    or type(fail_before_commit) is not bool):
                raise LibraryError('invalid_request')
            return self.store.commit_job(token['job_id'], token['epoch'],
                                         fail_before_commit=fail_before_commit)

        def cancel(self, job_id):
            return self.store.cancel_job(job_id)

        def retry(self, job_id):
            return self.store.retry_job(job_id)

        def submit_directory(self, job_id, root, namespace):
            _key(namespace)
            entries, seen, total = [], set(), 0

            def walk(fd, relative):
                nonlocal total
                with os.scandir(fd) as children:
                    for child in children:
                        member = relative + [child.name]
                        source = _key(namespace + '/' + '/'.join(member))
                        info = child.stat(follow_symlinks=False)
                        if stat.S_ISLNK(info.st_mode):
                            raise LibraryError('invalid_source')
                        is_dir = stat.S_ISDIR(info.st_mode)
                        if not is_dir:
                            _member_key(source)
                        opened = _child_fd(fd, child.name, is_dir)
                        try:
                            if is_dir:
                                walk(opened, member)
                            else:
                                text = _decode(_read_fd(opened, MEMBER_BYTES))
                                total = _append(entries, seen, source, text, total)
                        finally:
                            os.close(opened)

            fd = _path_fd(root, directory=True)
            try:
                walk(fd, [])
            except OSError as error:
                raise _io_error(error) from error
            finally:
                os.close(fd)
            return self.submit(job_id, sorted(entries, key=lambda item: item['source']))

        def submit_zip(self, job_id, archive_path, namespace):
            _key(namespace)
            raw = _read_path(archive_path, ARCHIVE_BYTES)
            entries, seen, total = [], set(), 0
            declared_total, compressed_total = 0, 0
            try:
                with zipfile.ZipFile(io.BytesIO(raw)) as archive:
                    for member in archive.infolist():
                        name = member.filename
                        if name != member.orig_filename:
                            raise LibraryError('invalid_source')
                        directory = name.endswith('/')
                        member_key = _key(name[:-1] if directory else name)
                        source = _key(namespace + '/' + member_key)
                        filetype = stat.S_IFMT(member.external_attr >> 16)
                        if filetype == stat.S_IFLNK:
                            raise LibraryError('invalid_source')
                        if member.flag_bits & 1:
                            raise LibraryError('invalid_archive')
                        if filetype not in (0, stat.S_IFREG, stat.S_IFDIR):
                            raise LibraryError('invalid_archive')
                        declared_total += member.file_size
                        compressed_total += member.compress_size
                        if (member.file_size > MEMBER_BYTES or declared_total > TOTAL_BYTES
                                or member.file_size > 100 * max(1, member.compress_size)
                                or declared_total > 100 * max(1, compressed_total)):
                            raise LibraryError('too_large')
                        if directory:
                            if filetype not in (0, stat.S_IFDIR):
                                raise LibraryError('invalid_archive')
                            # Ignoring a directory as a document must not skip
                            # validation of its local ZIP header and payload CRC.
                            with archive.open(member) as handle:
                                body = handle.read(MEMBER_BYTES + 1)
                            if len(body) != member.file_size:
                                raise LibraryError('invalid_archive')
                            continue
                        if filetype == stat.S_IFDIR:
                            raise LibraryError('invalid_archive')
                        _member_key(source)
                        if len(entries) >= FILE_COUNT or source in seen:
                            raise LibraryError('invalid_batch')
                        with archive.open(member) as handle:
                            body = handle.read(MEMBER_BYTES + 1)
                        if len(body) != member.file_size:
                            raise LibraryError('invalid_archive')
                        text = _decode(body)
                        total = _append(entries, seen, source, text, total)
            except UnicodeError as error:
                raise LibraryError('invalid_utf8') from error
            except (zipfile.BadZipFile, zipfile.LargeZipFile, RuntimeError,
                    NotImplementedError, ValueError, EOFError, OSError, zlib.error) as error:
                if isinstance(error, LibraryError):
                    raise
                raise LibraryError('invalid_archive') from error
            return self.submit(job_id, entries)

        def submit_json(self, job_id, bundle_path):
            raw = _read_path(bundle_path, ARCHIVE_BYTES)
            try:
                text = raw.decode('utf-8')
            except UnicodeError as error:
                raise LibraryError('invalid_utf8') from error
            try:
                value = json.loads(text, object_pairs_hook=_json_object,
                                   parse_constant=_json_constant)
            except (ValueError, RecursionError) as error:
                if isinstance(error, LibraryError):
                    raise
                raise LibraryError('invalid_json') from error
            if (type(value) is not dict or set(value) != {'entries'}
                    or type(value['entries']) is not list
                    or any(type(entry) is not dict or set(entry) != {'source', 'text'}
                           or any(type(item) is not str for item in entry.values())
                           for entry in value['entries'])):
                raise LibraryError('invalid_json')
            entries, seen, total = [], set(), 0
            for entry in value['entries']:
                # Only parser resource checks here; source semantics are deferred.
                total = _append(entries, seen, entry['source'], entry['text'], total)
            return self.submit(job_id, entries)
''').lstrip('\n')


_LOCAL = dedent(r'''
    """V0 single-file semantics with descriptor-based path confinement."""
    import os
    from pathlib import PurePosixPath
    from library.common import LibraryError, source_key
    from library.ingestion.jobs import _child_fd, _decode, _io_error, _path_fd, _read_fd, MEMBER_BYTES

    def checked_source(source):
        source_key(source)
        if PurePosixPath(source).suffix.lower() not in ('.txt', '.md'):
            raise LibraryError('unsupported_type')
        return source

    def import_file(store, root, source):
        checked_source(source)
        fd = _path_fd(root, directory=True)
        try:
            parts = source.split('/')
            for index, part in enumerate(parts):
                child = _child_fd(fd, part, directory=index < len(parts) - 1)
                os.close(fd)
                fd = child
            raw = _read_fd(fd, MEMBER_BYTES)
            _decode(raw)
            return store.insert(source, raw)
        except OSError as error:
            raise _io_error(error) from error
        finally:
            os.close(fd)
''').lstrip('\n')


def ingestion_files() -> dict[str, str]:
    """Return fresh authored source mappings within the ingestion package scope."""
    return {"library/ingestion/jobs.py": _JOBS, "library/ingestion/local.py": _LOCAL}
