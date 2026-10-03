"""Trusted authored M4 additive clients; inherited wire shapes stay unchanged."""
from __future__ import annotations

from textwrap import dedent

from .library_m3_clients_reference_v1 import clients_files as m3_clients_files


def _source(value: str) -> str:
    return dedent(value).lstrip('\n').rstrip() + '\n'


_SERVICE = _source(r'''
    """Opt-in revision-aware routes over the same durable catalog."""
    import sqlite3
    from library.common import LibraryError
    from library.catalog.m4_store import project_record4
    from library.query.legacy_m3_service import (
        Service as LegacyService, _body, _target, _error, decimal, canonical_export_bytes)

    class Service(LegacyService):
        def list_v1(self, query='', *, tag=None, collection=None,
                    deleted='active', offset=0, limit=100, generation=None):
            return self.store.list_v1(query, tag=tag, collection=collection,
                deleted=deleted, offset=offset, limit=limit, generation=generation)

        def show_v1(self, document_id):
            return self.store.show_v1(document_id)

        def revisions_v1(self, document_id, revision_id=None):
            return self.store.revisions_v1(document_id, revision_id=revision_id)

        def export_v1(self, ids=None, *, include_deleted=False, include_history=False,
                      max_bytes=16777216):
            return self.store.export_v1(ids, include_deleted=include_deleted,
                include_history=include_history, max_bytes=max_bytes)

        def request(self, method, target, body=None):
            try:
                pieces, query = _target(target)
                if pieces[:3] != ['', 'api', 'v1']:
                    status, value = super().request(method, target, body)
                    if (method == 'GET' and pieces == ['', 'health'] and status == 200
                            and value == {'status': 'ok', 'schema': 0}):
                        return status, {'status': 'ok', 'schema': 4}
                    return status, value
                route = pieces[3:]
                supported = (
                    method == 'GET' and (route == ['documents'] or
                        (len(route) == 2 and route[0] == 'documents' and bool(route[1])) or
                        (len(route) in (3, 4) and route[0] == 'documents' and bool(route[1])
                         and route[2] == 'revisions' and (len(route) == 3 or bool(route[3])))) or
                    method == 'POST' and (route == ['export'] or
                        (len(route) == 3 and route[0] == 'documents' and bool(route[1])
                         and route[2] in ('refresh', 'annotations', 'delete', 'restore'))))
                if not supported:
                    return 404, {'error': 'not_found'}
                if method == 'GET' and body is not None:
                    raise LibraryError('invalid_request')
                if route == ['documents'] and method == 'GET':
                    if set(query) - {'q', 'tag', 'collection', 'deleted', 'offset', 'limit', 'generation'}:
                        raise LibraryError('invalid_request')
                    numbers = {key: decimal(query[key]) for key in ('offset', 'limit', 'generation')
                               if key in query}
                    return 200, self.list_v1(query.get('q', ''), tag=query.get('tag'),
                        collection=query.get('collection'), deleted=query.get('deleted', 'active'), **numbers)
                if query:
                    raise LibraryError('invalid_request')
                if route == ['export']:
                    value = _body(body, ('ids', 'include_deleted', 'include_history', 'max_bytes'))
                    return 200, canonical_export_bytes(self.export_v1(**value))
                document_id = route[1]
                if method == 'GET':
                    if len(route) == 2:
                        return 200, self.show_v1(document_id)
                    return 200, self.revisions_v1(document_id,
                        revision_id=route[3] if len(route) == 4 else None)
                action = route[2]
                if action == 'refresh':
                    if type(body) is not dict or set(body) not in (
                            {'expected_version', 'text'}, {'expected_version', 'path'}):
                        raise LibraryError('invalid_request')
                    if ('text' in body and type(body['text']) is not str) or (
                            'path' in body and type(body['path']) is not str):
                        raise LibraryError('invalid_request')
                    value = self.refresh_document(document_id, **body)
                elif action == 'annotations':
                    values = _body(body, ('expected_version', 'notes', 'tags', 'collections'))
                    value = self.replace_annotations(document_id, **values)
                else:
                    values = _body(body, ('expected_version',))
                    value = getattr(self, action + '_document')(document_id, values['expected_version'])
                # Project the returned committed snapshot. A second Store read
                # could mix this operation's status with a concurrent later edit.
                return 200, {'status': value['status'], 'record': project_record4(value['record'])}
            except (ValueError, TypeError, UnicodeError, OSError, sqlite3.Error) as error:
                return _error(error)
''')

_CLI_ADDITIONS = _source(r'''
    command = commands.add_parser('documents-v1')
    command.add_argument('--query', default='')
    command.add_argument('--tag')
    command.add_argument('--collection')
    command.add_argument('--deleted', choices=('active', 'deleted', 'all'), default='active')
    command.add_argument('--offset', type=decimal, default=0)
    command.add_argument('--limit', type=decimal, default=100)
    command.add_argument('--generation', type=decimal)
    commands.add_parser('document-v1').add_argument('document_id')
    command = commands.add_parser('revisions-v1')
    command.add_argument('document_id')
    command.add_argument('--revision-id')
    command = commands.add_parser('export-v1')
    command.add_argument('document_ids', nargs='*')
    command.add_argument('--include-deleted', action='store_true')
    command.add_argument('--include-history', action='store_true')
    command.add_argument('--max-bytes', type=decimal, default=16777216)
    commands.add_parser('migrate')
''')

_CLI_DISPATCH = _source(r'''
    if args.command == 'documents-v1':
        value = service.list_v1(args.query, tag=args.tag, collection=args.collection,
            deleted=args.deleted, offset=args.offset, limit=args.limit, generation=args.generation)
    elif args.command == 'document-v1':
        value = service.show_v1(args.document_id)
    elif args.command == 'revisions-v1':
        value = service.revisions_v1(args.document_id, revision_id=args.revision_id)
    elif args.command == 'export-v1':
        value = service.export_v1(args.document_ids or None, include_deleted=args.include_deleted,
            include_history=args.include_history, max_bytes=args.max_bytes)
        sys.stdout.buffer.write(canonical_export_bytes(value))
        sys.stdout.buffer.flush()
        return 0
''')


def _replace_once(source: str, old: str, new: str) -> str:
    if source.count(old) != 1:
        raise ValueError(f'M4 client composition marker changed: {old!r}')
    return source.replace(old, new, 1)


def clients_files() -> dict[str, str]:
    """Return M4 overlays retaining byte-identical M3 transport and siblings."""
    legacy = m3_clients_files()
    cli = legacy['library/clients/cli.py']
    cli = _replace_once(cli, '    raw_args = list(sys.argv[1:] if argv is None else argv)',
        ''.join('    ' + line + '\n' for line in _CLI_ADDITIONS.splitlines()) +
        '    raw_args = list(sys.argv[1:] if argv is None else argv)')
    cli = _replace_once(cli, "new_commands = {'worker-enqueue'",
        "new_commands = {'documents-v1','document-v1','revisions-v1','export-v1','migrate',\n"
        "                    'worker-enqueue'")
    cli = _replace_once(cli, '        service = Service(store, args.root, backup_dir=args.backup_dir)',
        "        if args.command == 'migrate':\n"
        "            print(json.dumps(store.migrate(), ensure_ascii=True, sort_keys=True))\n"
        "            return 0\n"
        '        service = Service(store, args.root, backup_dir=args.backup_dir)')
    cli = _replace_once(cli, "        if args.command == 'worker':",
        ''.join('        ' + line + '\n' for line in _CLI_DISPATCH.splitlines()) +
        "        elif args.command == 'worker':")
    return {
        'library/query/legacy_m3_service.py': legacy['library/query/service.py'],
        'library/query/service.py': _SERVICE,
        'library/clients/legacy_m3_cli.py': legacy['library/clients/cli.py'],
        'library/clients/cli.py': cli,
        'library/clients/http.py': legacy['library/clients/http.py'],
    }
