"""Trusted authored M3 Service/HTTP/CLI overlays; not candidate evidence.

The inherited sources are retained as generated siblings. Maintenance clients
share the same durable Store and expose no evaluator hooks or backup uploads.
"""
from __future__ import annotations

from textwrap import dedent

from .library_m2_clients_reference_v1 import clients_files as m2_clients_files


def _source(value: str) -> str:
    return dedent(value).lstrip("\n").rstrip() + "\n"


_SERVICE = _source(r'''
    """Maintenance and portable export over the durable library catalog."""
    import json
    import sqlite3
    from library.common import LibraryError
    from library.query.legacy_m2_service import Service as LegacyService, _body, _target, decimal

    _CONFLICTS = frozenset(('worker_busy', 'stale_worker', 'maintenance_busy', 'already_exists',
                          'stale_version', 'stale_generation', 'document_deleted',
                          'revision_capacity', 'collection_not_empty', 'source_changed',
                          'job_conflict', 'job_state', 'stale_epoch'))

    def canonical_export_bytes(value):
        return json.dumps(value, ensure_ascii=False, sort_keys=True,
                          separators=(',', ':'), allow_nan=False).encode('utf-8')

    def _error(error):
        code = error.code if isinstance(error, LibraryError) else (
            'io_error' if isinstance(error, (OSError, sqlite3.Error)) else 'invalid_request')
        return (404 if code == 'not_found' else 409 if code in _CONFLICTS else 400), {'error': code}

    class Service(LegacyService):
        def __init__(self, store, root, *, backup_dir=None):
            super().__init__(store, root)
            # None explicitly selects backups beside the database, with path
            # existence checked only when used. Durable ownership guards reject
            # switching away from a root with pending owned artifacts.
            self.store.configure_backup_dir(backup_dir)

        def enqueue_job(self, job_id):
            return self.store.enqueue_job(job_id)

        def reindex_step(self, limit=64):
            return self.store.reindex_step(limit)

        def export_bundle(self, ids=None, *, include_deleted=False, include_history=False,
                          max_bytes=16777216):
            return self.store.export_bundle(ids, include_deleted=include_deleted,
                include_history=include_history, max_bytes=max_bytes)

        def backup(self, name):
            return self.store.backup(name)

        def list_backups(self, offset=0, limit=100):
            return self.store.list_backups(offset=offset, limit=limit)

        def restore_backup(self, name, expected_generation):
            return self.store.restore_backup(name, expected_generation)

        def diagnostics(self):
            return self.store.diagnostics()

        def request(self, method, target, body=None):
            try:
                pieces, query = _target(target)
                export_route = pieces == ['', 'api', 'export-bundle']
                maintenance = pieces[:3] == ['', 'api', 'maintenance']
                if not export_route and not maintenance:
                    status, value = super().request(method, target, body)
                    if type(value) is dict and set(value) == {'error'} and value['error'] in _CONFLICTS:
                        status = 409
                    return status, value
                route = pieces[3:] if maintenance else []
                supported = (export_route and method == 'POST') or (maintenance and (
                    (method == 'GET' and route in (['backups'], ['diagnostics'])) or
                    (method == 'POST' and (route in (['reindex'], ['backups'], ['restore']) or
                        (len(route) == 3 and route[0] == 'jobs' and bool(route[1]) and route[2] == 'enqueue')))))
                if not supported:
                    return 404, {'error': 'not_found'}
                if method == 'GET' and body is not None:
                    raise LibraryError('invalid_request')
                if maintenance and route == ['backups'] and method == 'GET':
                    if set(query) - {'offset', 'limit'}:
                        raise LibraryError('invalid_request')
                    values = {key: decimal(query[key]) for key in ('offset', 'limit') if key in query}
                    return 200, self.list_backups(**values)
                if query:
                    raise LibraryError('invalid_request')
                if export_route:
                    value = _body(body, ('ids', 'include_deleted', 'include_history', 'max_bytes'))
                    # Store validates and bounds the whole snapshot before any
                    # bytes reach the transport. The HTTP handler preserves bytes.
                    return 200, canonical_export_bytes(self.export_bundle(**value))
                if route == ['diagnostics']:
                    return 200, self.diagnostics()
                if route == ['reindex']:
                    value = _body(body, ('limit',))
                    return 200, self.reindex_step(value['limit'])
                if route == ['backups']:
                    value = _body(body, ('name',))
                    return 200, self.backup(value['name'])
                if route == ['restore']:
                    value = _body(body, ('name', 'expected_generation'))
                    return 200, self.restore_backup(value['name'], value['expected_generation'])
                _body(body, ())
                return 200, self.enqueue_job(route[1])
            except (ValueError, TypeError, UnicodeError, OSError, sqlite3.Error) as error:
                return _error(error)
''')

_CLI_ADDITIONS = _source(r'''
    command = commands.add_parser('worker-enqueue')
    command.add_argument('job_id')
    commands.add_parser('worker').add_argument('--once', action='store_true')
    commands.add_parser('reindex').add_argument('--limit', type=decimal, required=True)
    command = commands.add_parser('export-bundle')
    command.add_argument('document_ids', nargs='*')
    command.add_argument('--include-deleted', action='store_true')
    command.add_argument('--include-history', action='store_true')
    command.add_argument('--max-bytes', type=decimal, default=16777216)
    commands.add_parser('backup').add_argument('name')
    command = commands.add_parser('backups')
    command.add_argument('--offset', type=decimal, default=0)
    command.add_argument('--limit', type=decimal, default=100)
    command = commands.add_parser('restore-backup')
    command.add_argument('name')
    command.add_argument('--expected-generation', type=decimal, required=True)
    commands.add_parser('diagnostics')
''')

_CLI_DISPATCH = _source(r'''
    if args.command == 'worker':
        from library.ingestion.worker import run_worker
        try:
            value = run_worker(store, once=args.once)
        except KeyboardInterrupt:
            return 0
        if not args.once:
            return 0
    elif args.command == 'worker-enqueue':
        value = service.enqueue_job(args.job_id)
    elif args.command == 'reindex':
        value = service.reindex_step(args.limit)
    elif args.command == 'export-bundle':
        value = service.export_bundle(args.document_ids or None, include_deleted=args.include_deleted,
            include_history=args.include_history, max_bytes=args.max_bytes)
        sys.stdout.buffer.write(canonical_export_bytes(value))
        sys.stdout.buffer.flush()
        return 0
    elif args.command == 'backup':
        value = service.backup(args.name)
    elif args.command == 'backups':
        value = service.list_backups(args.offset, args.limit)
    elif args.command == 'restore-backup':
        value = service.restore_backup(args.name, args.expected_generation)
    elif args.command == 'diagnostics':
        value = service.diagnostics()
''')


def _replace_once(source: str, old: str, new: str) -> str:
    if source.count(old) != 1:
        raise ValueError(f'M3 client composition marker changed: {old!r}')
    return source.replace(old, new, 1)


def clients_files() -> dict[str, str]:
    """Return fresh M3 overlays with the inherited HTTP transport unchanged."""
    legacy = m2_clients_files()
    cli = legacy['library/clients/cli.py']
    cli = _replace_once(cli, 'from library.query.service import Service, decimal',
        'from library.query.service import Service, decimal, canonical_export_bytes')
    cli = _replace_once(cli, "parser.add_argument('--root', default='.')",
        "parser.add_argument('--root', default='.')\n    parser.add_argument('--backup-dir')")
    cli = _replace_once(cli, "token in ('--db', '--root')", "token in ('--db', '--root', '--backup-dir')")
    cli = _replace_once(cli, "token.startswith(('--db=', '--root='))",
        "token.startswith(('--db=', '--root=', '--backup-dir='))")
    cli = _replace_once(cli, '    raw_args = list(sys.argv[1:] if argv is None else argv)',
        ''.join('    ' + line + '\n' for line in _CLI_ADDITIONS.splitlines()) +
        '    raw_args = list(sys.argv[1:] if argv is None else argv)')
    cli = _replace_once(cli, "new_commands = {'documents'",
        "new_commands = {'worker-enqueue','worker','reindex','export-bundle',\n"
        "                    'backup','backups','restore-backup','diagnostics','documents'")
    cli = _replace_once(cli, 'store = Store(args.db)',
        'store = Store(args.db, backup_dir=args.backup_dir)')
    cli = _replace_once(cli, 'service = Service(store, args.root)',
        'service = Service(store, args.root, backup_dir=args.backup_dir)')
    cli = _replace_once(cli, "        if args.command == 'documents':",
        ''.join('        ' + line + '\n' for line in _CLI_DISPATCH.splitlines()) +
        "        elif args.command == 'documents':")
    return {
        'library/query/legacy_m2_service.py': legacy['library/query/service.py'],
        'library/query/service.py': _SERVICE,
        'library/clients/legacy_m2_cli.py': legacy['library/clients/cli.py'],
        'library/clients/cli.py': cli,
        'library/clients/http.py': legacy['library/clients/http.py'],
    }
