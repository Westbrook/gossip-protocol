"""Trusted authored M2 Service/HTTP/CLI overlays; not candidate evidence.

Legacy Service and CLI sources remain byte-identical in generated sibling
modules. New lifecycle clients share the durable catalog rather than keeping an
independent client cache or mutation model.
"""
from __future__ import annotations

from textwrap import dedent

from .library_m1_clients_reference_v1 import clients_files as m1_clients_files


def _source(value: str) -> str:
    return dedent(value).lstrip("\n").rstrip() + "\n"


_SERVICE = _source(r'''
    """Lifecycle routes and bounded refresh over the shared durable Store."""
    import errno
    import os
    from pathlib import Path
    import re
    import sqlite3
    import stat
    from urllib.parse import parse_qsl, unquote, urlsplit
    from library.common import LibraryError, MAX_FILE_BYTES, source_key
    from library.query.legacy_m1_service import Service as LegacyService

    def decimal(value):
        if type(value) is not str or not re.fullmatch(r'[0-9]+', value):
            raise LibraryError('invalid_request')
        try:
            return int(value)
        except ValueError as error:
            raise LibraryError('invalid_request') from error

    def _version(value):
        if type(value) is not int or value < 1:
            raise LibraryError('invalid_request')

    def _text(value):
        if type(value) is not str:
            raise LibraryError('invalid_request')
        try:
            raw = value.encode('utf-8')
        except UnicodeError as error:
            raise LibraryError('invalid_utf8') from error
        if len(raw) > MAX_FILE_BYTES:
            raise LibraryError('too_large')
        return value

    def _refresh_text(root, key):
        if type(key) is not str:
            raise LibraryError('invalid_request')
        source_key(key)
        if len(key.split('/')) > 16 or Path(key).is_absolute():
            raise LibraryError('invalid_source')
        if Path(key).suffix.lower() not in ('.txt', '.md', '.html'):
            raise LibraryError('unsupported_type')
        base = Path(root)
        if not base.is_absolute():
            base = Path.cwd() / base
        target = base / key
        descriptor = None
        try:
            descriptor = os.open(target.anchor, os.O_RDONLY | os.O_DIRECTORY)
            parts = target.parts[1:]
            for index, part in enumerate(parts):
                mode = os.stat(part, dir_fd=descriptor, follow_symlinks=False).st_mode
                if stat.S_ISLNK(mode):
                    raise LibraryError('invalid_source')
                last = index == len(parts) - 1
                if not (stat.S_ISREG(mode) if last else stat.S_ISDIR(mode)):
                    raise LibraryError('io_error')
                flags = os.O_RDONLY | os.O_NOFOLLOW | (os.O_NONBLOCK if last else os.O_DIRECTORY)
                next_descriptor = os.open(part, flags, dir_fd=descriptor)
                os.close(descriptor)
                descriptor = next_descriptor
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise LibraryError('io_error')
            with os.fdopen(descriptor, 'rb') as handle:
                descriptor = None
                raw = handle.read(MAX_FILE_BYTES + 1)
        except OSError as error:
            raise LibraryError('invalid_source' if error.errno == errno.ELOOP else 'io_error') from error
        finally:
            if descriptor is not None:
                os.close(descriptor)
        if len(raw) > MAX_FILE_BYTES:
            raise LibraryError('too_large')
        try:
            return raw.decode('utf-8')
        except UnicodeError as error:
            raise LibraryError('invalid_utf8') from error

    def _target(target):
        # Validate percent escapes before decoding. Decode each path component
        # once, so an escaped slash cannot introduce a second route segment.
        if type(target) is not str or re.search(r'%(?![0-9A-Fa-f]{2})', target):
            raise LibraryError('invalid_request')
        target.encode('utf-8')
        if any(ord(char) < 32 or ord(char) == 127 for char in target):
            raise LibraryError('invalid_request')
        url = urlsplit(target)
        if url.scheme or url.netloc or url.fragment:
            raise LibraryError('invalid_request')
        pairs = parse_qsl(url.query, keep_blank_values=True, strict_parsing=True,
                          encoding='utf-8', errors='strict', max_num_fields=32)
        query = {}
        for key, value in pairs:
            if key in query:
                raise LibraryError('invalid_request')
            query[key] = value
        pieces = [unquote(piece, encoding='utf-8', errors='strict')
                  for piece in url.path.split('/')]
        return pieces, query

    def _body(body, fields):
        if type(body) is not dict or set(body) != set(fields):
            raise LibraryError('invalid_request')
        return body

    class Service(LegacyService):
        def lifecycle_list(self, query='', *, tag=None, collection=None,
                           deleted='active', offset=0, limit=100, generation=None):
            return self.store.lifecycle_list(query, tag=tag, collection=collection,
                                            deleted=deleted, offset=offset, limit=limit,
                                            generation=generation)

        def lifecycle_show(self, document_id):
            return self.store.lifecycle_show(document_id)

        def revision_history(self, document_id):
            return self.store.revision_history(document_id)

        def refresh_document(self, document_id, expected_version, *, text=None, path=None):
            _version(expected_version)
            if (text is None) == (path is None):
                raise LibraryError('invalid_request')
            value = _text(text) if path is None else _refresh_text(self.root, path)
            return self.store.refresh_document(document_id, expected_version, text=value)

        def replace_annotations(self, document_id, expected_version, notes, tags, collections):
            return self.store.replace_annotations(document_id, expected_version, notes, tags, collections)

        def list_collections(self):
            return self.store.list_collections()

        def create_collection(self, name, expected_generation):
            return self.store.create_collection(name, expected_generation)

        def remove_collection(self, name, expected_generation):
            return self.store.remove_collection(name, expected_generation)

        def delete_document(self, document_id, expected_version):
            return self.store.delete_document(document_id, expected_version)

        def restore_document(self, document_id, expected_version):
            return self.store.restore_document(document_id, expected_version)

        def request(self, method, target, body=None):
            try:
                pieces, query = _target(target)
                if pieces[:3] != ['', 'api', 'lifecycle']:
                    return super().request(method, target, body)
                route = pieces[3:]
                if method not in ('GET', 'POST'):
                    return 404, {'error': 'not_found'}
                supported = (
                    method == 'GET' and (route in (['documents'], ['collections']) or
                        (len(route) == 2 and route[0] == 'documents' and bool(route[1])) or
                        (len(route) == 3 and route[0] == 'documents' and bool(route[1]) and route[2] == 'revisions')) or
                    method == 'POST' and (route in (['collections'], ['collections', 'remove']) or
                        (len(route) == 3 and route[0] == 'documents' and bool(route[1]) and
                         route[2] in ('refresh', 'annotations', 'delete', 'restore'))))
                if not supported:
                    return 404, {'error': 'not_found'}
                if method == 'GET' and body is not None:
                    raise LibraryError('invalid_request')
                if route == ['documents'] and method == 'GET':
                    if set(query) - {'q', 'tag', 'collection', 'deleted', 'offset', 'limit', 'generation'}:
                        raise LibraryError('invalid_request')
                    numbers = {key: decimal(query[key]) for key in ('offset', 'limit', 'generation')
                               if key in query}
                    return 200, self.lifecycle_list(query.get('q', ''), tag=query.get('tag'),
                        collection=query.get('collection'), deleted=query.get('deleted', 'active'), **numbers)
                if query:
                    # Every other listed lifecycle route has a closed empty query.
                    raise LibraryError('invalid_request')
                if route == ['collections']:
                    if method == 'GET':
                        return 200, self.list_collections()
                    value = _body(body, ('name', 'expected_generation'))
                    return 200, self.create_collection(value['name'], value['expected_generation'])
                if route == ['collections', 'remove'] and method == 'POST':
                    value = _body(body, ('name', 'expected_generation'))
                    return 200, self.remove_collection(value['name'], value['expected_generation'])
                if len(route) in (2, 3) and route[0] == 'documents' and route[1]:
                    document_id = route[1]
                    if len(route) == 2 and method == 'GET':
                        return 200, self.lifecycle_show(document_id)
                    if len(route) == 3 and route[2] == 'revisions' and method == 'GET':
                        return 200, self.revision_history(document_id)
                    if len(route) == 3 and method == 'POST':
                        action = route[2]
                        if action == 'refresh':
                            if type(body) is not dict or set(body) not in (
                                    {'expected_version', 'text'}, {'expected_version', 'path'}):
                                raise LibraryError('invalid_request')
                            if 'text' in body and type(body['text']) is not str:
                                raise LibraryError('invalid_request')
                            if 'path' in body and type(body['path']) is not str:
                                raise LibraryError('invalid_request')
                            return 200, self.refresh_document(document_id, **body)
                        if action == 'annotations':
                            value = _body(body, ('expected_version', 'notes', 'tags', 'collections'))
                            return 200, self.replace_annotations(document_id, **value)
                        if action in ('delete', 'restore'):
                            value = _body(body, ('expected_version',))
                            return 200, getattr(self, action + '_document')(document_id, value['expected_version'])
                return 404, {'error': 'not_found'}
            except (ValueError, TypeError, UnicodeError, OSError, sqlite3.Error) as error:
                code = error.code if isinstance(error, LibraryError) else (
                    'io_error' if isinstance(error, (OSError, sqlite3.Error)) else 'invalid_request')
                status = 404 if code == 'not_found' else 409 if code in (
                    'stale_version', 'stale_generation', 'document_deleted',
                    'revision_capacity', 'collection_not_empty') else 400
                return status, {'error': code}
''')

_HTTP = _source(r'''
    """Loopback HTTP transport with strict bounded JSON and explicit errors."""
    from http.server import BaseHTTPRequestHandler, HTTPServer
    import json
    import math
    from pathlib import Path
    import re

    def _pairs(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError('duplicate JSON key')
            value[key] = item
        return value

    def _constant(value):
        raise ValueError('nonfinite JSON number')

    def _float(value):
        result = float(value)
        if not math.isfinite(result):
            raise ValueError('nonfinite JSON number')
        return result

    def handler_for(service):
        class Handler(BaseHTTPRequestHandler):
            def setup(self):
                super().setup()
                self.connection.settimeout(5)

            def log_message(self, *_args):
                pass

            def respond(self, status, value, content_type='application/json; charset=utf-8'):
                raw = value if isinstance(value, bytes) else json.dumps(value, ensure_ascii=True).encode()
                self.send_response(status)
                self.send_header('Content-Type', content_type)
                self.send_header('Content-Length', str(len(raw)))
                self.send_header('Cache-Control', 'no-store')
                self.send_header('X-Content-Type-Options', 'nosniff')
                self.send_header('Content-Security-Policy', "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'")
                self.end_headers()
                if self.command != 'HEAD':
                    self.wfile.write(raw)

            def valid_host(self):
                port = self.server.server_address[1]
                return (len(self.headers.get_all('Host', [])) == 1 and
                        self.headers.get('Host') in (f'127.0.0.1:{port}', f'localhost:{port}'))

            def do_GET(self):
                lengths = self.headers.get_all('Content-Length', [])
                if (not self.valid_host() or self.headers.get('Transfer-Encoding') is not None
                        or len(lengths) > 1 or (lengths and lengths != ['0'])):
                    self.respond(400, {'error': 'invalid_request'})
                elif self.path == '/':
                    self.respond(200, Path(__file__).with_name('index.html').read_bytes(), 'text/html; charset=utf-8')
                else:
                    self.respond(*service.request('GET', self.path))

            def do_POST(self):
                try:
                    lengths = self.headers.get_all('Content-Length', [])
                    types = self.headers.get_all('Content-Type', [])
                    if (not self.valid_host() or len(lengths) != 1 or len(types) != 1
                            or not re.fullmatch(r'[0-9]+', lengths[0])
                            or self.headers.get('Transfer-Encoding') is not None
                            or self.headers.get_content_type() != 'application/json'):
                        raise ValueError('invalid request headers')
                    length = int(lengths[0])
                    if not 0 <= length <= 65536:
                        raise ValueError('invalid request length')
                    raw = self.rfile.read(length)
                    if len(raw) != length:
                        raise ValueError('incomplete request')
                    body = json.loads(raw.decode('utf-8'), object_pairs_hook=_pairs,
                                      parse_constant=_constant, parse_float=_float)
                except (ValueError, UnicodeError, OSError, RecursionError):
                    self.respond(400, {'error': 'invalid_request'})
                    return
                self.respond(*service.request('POST', self.path, body))

            def do_HEAD(self):
                self.respond(404, {'error': 'not_found'})

            do_PUT = do_DELETE = do_PATCH = do_OPTIONS = do_TRACE = do_CONNECT = do_HEAD

            def send_error(self, code, message=None, explain=None):
                # BaseHTTPRequestHandler's unknown-method default is an HTML501.
                self.respond(404 if code == 501 else 400, {'error': 'not_found' if code == 501 else 'invalid_request'})
        return Handler

    def serve(service, port=8765):
        with HTTPServer(('127.0.0.1', port), handler_for(service)) as server:
            print(f'Local Research Library: http://127.0.0.1:{server.server_port}/', flush=True)
            server.serve_forever()
''')

_CLI_ADDITIONS = _source(r'''
    for name in ('documents', 'document', 'revisions', 'refresh', 'annotate', 'delete',
                 'restore-document', 'collections', 'collection-create', 'collection-remove'):
        command = commands.add_parser(name)
        if name == 'documents':
            command.add_argument('--query', default='')
            command.add_argument('--tag')
            command.add_argument('--collection')
            command.add_argument('--deleted', choices=('active', 'deleted', 'all'), default='active')
            command.add_argument('--offset', type=decimal, default=0)
            command.add_argument('--limit', type=decimal, default=100)
            command.add_argument('--generation', type=decimal)
        elif name in ('collection-create', 'collection-remove'):
            command.add_argument('name')
            command.add_argument('--expected-generation', type=decimal, required=True)
        elif name != 'collections':
            command.add_argument('document_id')
            if name in ('refresh', 'annotate', 'delete', 'restore-document'):
                command.add_argument('--expected-version', type=decimal, required=True)
            if name == 'refresh':
                inputs = command.add_mutually_exclusive_group(required=True)
                inputs.add_argument('--text')
                inputs.add_argument('--path')
            if name == 'annotate':
                command.add_argument('--notes', required=True)
                command.add_argument('--tag', action='append', default=[])
                command.add_argument('--collection', action='append', default=[])
''')

_CLI_DISPATCH = _source(r'''
    if args.command == 'documents':
        value = service.lifecycle_list(args.query, tag=args.tag, collection=args.collection,
                                      deleted=args.deleted, offset=args.offset, limit=args.limit,
                                      generation=args.generation)
    elif args.command == 'document':
        value = service.lifecycle_show(args.document_id)
    elif args.command == 'revisions':
        value = service.revision_history(args.document_id)
    elif args.command == 'refresh':
        value = service.refresh_document(args.document_id, args.expected_version, text=args.text, path=args.path)
    elif args.command == 'annotate':
        value = service.replace_annotations(args.document_id, args.expected_version,
                                            args.notes, args.tag, args.collection)
    elif args.command in ('delete', 'restore-document'):
        method = service.delete_document if args.command == 'delete' else service.restore_document
        value = method(args.document_id, args.expected_version)
    elif args.command == 'collections':
        value = service.list_collections()
    elif args.command in ('collection-create', 'collection-remove'):
        method = service.create_collection if args.command == 'collection-create' else service.remove_collection
        value = method(args.name, args.expected_generation)
''')


def clients_files() -> dict[str, str]:
    """Return fresh overlays, preserving inherited command dispatch verbatim."""
    legacy = m1_clients_files()
    cli = legacy['library/clients/cli.py']
    cli = cli.replace('from library.query.service import Service',
                      'from library.query.service import Service, decimal')
    # Only parsing new lifecycle commands has JSON syntax errors; inherited CLI
    # parser behavior and data/error dispatch remain unchanged.
    cli = cli.replace('def main(argv=None):', '''class LifecycleParser(argparse.ArgumentParser):
    def error(self, message):
        if getattr(self, 'lifecycle', False):
            raise LibraryError('invalid_request')
        super().error(message)

def _selected_command(argv):
    index = 0
    while index < len(argv):
        token = argv[index]
        if token in ('--db', '--root'):
            index += 2
        elif token.startswith(('--db=', '--root=')):
            index += 1
        else:
            return token
    return None

def _main(argv=None):''')
    cli = cli.replace("parser = argparse.ArgumentParser(description='Local Research Library')",
                      "parser = LifecycleParser(description='Local Research Library')")
    cli = cli.replace("    args = parser.parse_args(argv)",
        ''.join('    ' + line + '\n' for line in _CLI_ADDITIONS.splitlines()) +
        "    raw_args = list(sys.argv[1:] if argv is None else argv)\n"
        "    new_commands = {'documents','document','revisions','refresh','annotate','delete',\n"
        "                    'restore-document','collections','collection-create','collection-remove'}\n"
        "    parser.lifecycle = _selected_command(raw_args) in new_commands\n"
        "    if parser.lifecycle:\n        parser.allow_abbrev = False\n"
        "    for child in commands.choices.values():\n"
        "        child.lifecycle = parser.lifecycle\n"
        "        if parser.lifecycle:\n            child.allow_abbrev = False\n"
        "    args = parser.parse_args(raw_args)")
    marker = "        if args.command == 'import':"
    assert cli.count(marker) == 1
    cli = cli.replace(marker,
        ''.join('        ' + line + '\n' for line in _CLI_DISPATCH.splitlines()) +
        "        elif args.command == 'import':")
    cli += "\ndef main(argv=None):\n    try:\n        return _main(argv)\n    except LibraryError as error:\n        print(json.dumps({'error': error.code}), file=sys.stderr)\n        return 2\n"
    return {
        'library/query/legacy_m1_service.py': legacy['library/query/service.py'],
        'library/query/service.py': _SERVICE,
        'library/clients/cli.py': cli,
        'library/clients/http.py': _HTTP,
    }
