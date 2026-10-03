"""Trusted authored M1 query/client sources, never independent acceptance evidence.

These sources implement the public development contract with the real Store and
JobManager interfaces. They are supplied only to offline reference fixtures; they
must not be treated as model-produced project work or hidden test answers.
"""

from __future__ import annotations

from textwrap import dedent

from .library_project_fixture_v1 import seed_files


def _source(value: str) -> str:
    return dedent(value).lstrip("\n").rstrip() + "\n"


_SERVICE = _source(r'''
    """Shared v0/M1 query and HTTP contract over persistent application APIs."""
    import os
    from pathlib import Path
    import sqlite3
    import stat
    from urllib.parse import parse_qs, unquote, urlsplit
    from library.common import LibraryError, page, source_key
    from library.ingestion.jobs import JobManager
    from library.ingestion.local import import_file

    class Service:
        def __init__(self, store, root):
            self.store, self.root = store, root
            self.jobs = JobManager(store)

        def listing(self, query='', offset=0, limit=100):
            page(offset, limit)
            if type(query) is not str or len(query) > 256:
                raise LibraryError('invalid_request')
            needle = query.casefold()
            rows = [doc for doc in self.store.documents()
                    if needle in (doc['source'] + '\n' + doc['text']).casefold()]
            return {'documents': rows[offset:offset + limit], 'total': len(rows)}

        def export(self, ids=None):
            rows = self.store.documents()
            if ids is not None:
                if type(ids) is not list or any(type(x) is not str for x in ids) or len(set(ids)) != len(ids):
                    raise LibraryError('invalid_request')
                selected = set(ids)
                if selected - {d['document_id'] for d in rows}:
                    raise LibraryError('not_found')
                rows = [d for d in rows if d['document_id'] in selected]
            return {'format': 'local-research-library-v0', 'documents': rows}

        def intake_path(self, key, kind):
            source_key(key)
            if len(key.split('/')) > 16:
                raise LibraryError('invalid_source')
            if kind in ('zip', 'json') and Path(key).suffix.lower() != '.' + kind:
                raise LibraryError('unsupported_type')
            # abspath does not resolve symlinks. Inspect every existing ancestor,
            # including the configured root, before delegating discovery.
            base = Path(os.path.abspath(self.root))
            target = base / key
            cursor = Path(target.anchor)
            try:
                for part in target.parts[1:]:
                    cursor = cursor / part
                    mode = cursor.lstat().st_mode
                    if stat.S_ISLNK(mode):
                        raise LibraryError('invalid_source')
                    if cursor != target and not stat.S_ISDIR(mode):
                        raise LibraryError('io_error')
                wanted = stat.S_ISDIR(mode) if kind == 'directory' else stat.S_ISREG(mode)
                if not wanted:
                    raise LibraryError('io_error')
            except OSError as error:
                raise LibraryError('io_error') from error
            return target

        def submit_job(self, body):
            if type(body) is not dict:
                raise LibraryError('invalid_request')
            if set(body) == {'job_id', 'entries'}:
                return self.jobs.submit(body['job_id'], body['entries'])
            kind = next((name for name in ('directory', 'zip', 'json') if name in body), None)
            wanted = {'job_id', kind} if kind == 'json' else {'job_id', kind, 'namespace'}
            if kind is None or set(body) != wanted:
                raise LibraryError('invalid_request')
            if kind != 'json':
                source_key(body['namespace'])
                if len(body['namespace'].split('/')) > 16:
                    raise LibraryError('invalid_source')
            path = self.intake_path(body[kind], kind)
            if kind == 'directory':
                return self.jobs.submit_directory(body['job_id'], path, body['namespace'])
            if kind == 'zip':
                return self.jobs.submit_zip(body['job_id'], path, body['namespace'])
            return self.jobs.submit_json(body['job_id'], path)

        def commit_job(self, job_id, epoch):
            result = self.jobs.commit({'job_id': job_id, 'epoch': epoch})
            if type(result) is dict and set(result) == {'error'}:
                raise LibraryError(result['error'])
            return result

        def request(self, method, target, body=None):
            try:
                url = urlsplit(target)
                query = parse_qs(url.query, keep_blank_values=True, strict_parsing=True)
                path = unquote(url.path)
                if method == 'GET' and path == '/health' and not query:
                    return 200, {'status': 'ok', 'schema': 0}
                if method == 'GET' and path == '/api/documents':
                    if set(query) - {'q', 'offset', 'limit'} or any(len(v) != 1 for v in query.values()):
                        raise LibraryError('invalid_request')
                    values = {k: query.get(k, [v])[0] for k, v in (('q', ''), ('offset', '0'), ('limit', '100'))}
                    if any(not values[k].isascii() or not values[k].isdigit() for k in ('offset', 'limit')):
                        raise LibraryError('invalid_request')
                    return 200, self.listing(values['q'], int(values['offset']), int(values['limit']))
                if method == 'GET' and path.startswith('/api/documents/') and not query:
                    return 200, self.store.show(path[len('/api/documents/'):])
                if method == 'GET' and path == '/api/export' and not query:
                    return 200, self.export()
                if method == 'POST' and path == '/api/export' and not query:
                    if type(body) is not dict or set(body) != {'ids'}:
                        raise LibraryError('invalid_request')
                    return 200, self.export(body['ids'])
                if method == 'POST' and path == '/api/import' and not query:
                    if type(body) is not dict or set(body) != {'source'}:
                        raise LibraryError('invalid_request')
                    return 200, import_file(self.store, self.root, body['source'])
                if path == '/api/jobs' and not query:
                    if method == 'GET':
                        return 200, {'jobs': self.store.list_jobs()}
                    if method == 'POST':
                        return 200, self.submit_job(body)
                if path.startswith('/api/jobs/') and not query:
                    pieces = path[len('/api/jobs/'):].split('/')
                    if method == 'GET' and len(pieces) == 1:
                        return 200, self.jobs.get(pieces[0])
                    if method == 'POST' and len(pieces) == 2:
                        job_id, action = pieces
                        if action not in ('prepare', 'commit', 'cancel', 'retry'):
                            return 404, {'error': 'not_found'}
                        fields = {'epoch'} if action == 'commit' else set()
                        if type(body) is not dict or set(body) != fields:
                            raise LibraryError('invalid_request')
                        if action == 'commit':
                            if type(body['epoch']) is not int or body['epoch'] < 1:
                                raise LibraryError('invalid_request')
                            return 200, self.commit_job(job_id, body['epoch'])
                        return 200, getattr(self.jobs, action)(job_id)
                return 404, {'error': 'not_found'}
            except (ValueError, TypeError, UnicodeError, OSError, sqlite3.Error) as error:
                code = error.code if isinstance(error, LibraryError) else (
                    'io_error' if isinstance(error, (OSError, sqlite3.Error)) else 'invalid_request')
                return {'not_found': 404, 'source_changed': 409, 'job_conflict': 409,
                        'job_state': 409, 'stale_epoch': 409}.get(code, 400), {'error': code}
''')

_CLI = _source(r'''
    """Local Research Library command line, including persistent ingestion jobs."""
    import argparse
    import json
    import sqlite3
    import sys
    from library.catalog.store import Store
    from library.common import LibraryError
    from library.ingestion.local import import_file
    from library.query.service import Service

    def main(argv=None):
        parser = argparse.ArgumentParser(description='Local Research Library')
        parser.add_argument('--db', default='library.sqlite3')
        parser.add_argument('--root', default='.')
        commands = parser.add_subparsers(dest='command', required=True)
        commands.add_parser('import').add_argument('source')
        for name in ('list', 'search'):
            command = commands.add_parser(name)
            if name == 'search':
                command.add_argument('query')
            command.add_argument('--offset', type=int, default=0)
            command.add_argument('--limit', type=int, default=100)
        commands.add_parser('show').add_argument('document_id')
        commands.add_parser('export').add_argument('document_ids', nargs='*')
        commands.add_parser('serve').add_argument('--port', type=int, default=8765)
        command = commands.add_parser('job-submit')
        command.add_argument('job_id')
        command.add_argument('--kind', required=True, choices=('directory', 'zip', 'json'))
        command.add_argument('path')
        command.add_argument('--namespace')
        for name in ('job-show', 'job-prepare', 'job-commit', 'job-cancel', 'job-retry'):
            command = commands.add_parser(name)
            command.add_argument('job_id')
            if name == 'job-commit':
                command.add_argument('epoch', type=int)
        commands.add_parser('jobs')
        args = parser.parse_args(argv)
        if args.command == 'job-submit':
            if args.kind in ('directory', 'zip') and args.namespace is None:
                parser.error('--namespace is required for directory and zip intake')
            if args.kind == 'json' and args.namespace is not None:
                parser.error('--namespace is forbidden for json intake')
        store = None
        try:
            store = Store(args.db)
            service = Service(store, args.root)
            if args.command == 'serve':
                from library.clients.http import serve
                serve(service, args.port)
                return 0
            if args.command == 'import':
                value = import_file(store, args.root, args.source)
            elif args.command in ('list', 'search'):
                value = service.listing(getattr(args, 'query', ''), args.offset, args.limit)
            elif args.command == 'show':
                value = store.show(args.document_id)
            elif args.command == 'export':
                value = service.export(args.document_ids or None)
            elif args.command == 'jobs':
                value = {'jobs': store.list_jobs()}
            elif args.command == 'job-submit':
                body = {'job_id': args.job_id, args.kind: args.path}
                if args.namespace is not None:
                    body['namespace'] = args.namespace
                value = service.submit_job(body)
            elif args.command == 'job-show':
                value = service.jobs.get(args.job_id)
            elif args.command == 'job-commit':
                value = service.commit_job(args.job_id, args.epoch)
            else:
                value = getattr(service.jobs, args.command.removeprefix('job-'))(args.job_id)
            print(json.dumps(value, ensure_ascii=True, sort_keys=True))
            return 0
        except (LibraryError, OSError, sqlite3.Error) as error:
            print(json.dumps({'error': getattr(error, 'code', 'io_error')}), file=sys.stderr)
            return 2
        finally:
            if store is not None:
                store.close()
''')

_WORKFLOW = _source(r'''
    """Blackbox transport through the real file, job, query and SQLite APIs."""
    from pathlib import Path
    import tempfile
    from library.catalog.store import Store
    from library.common import LibraryError, identity
    from library.ingestion.jobs import JobManager
    from library.ingestion.local import checked_source, import_file
    from library.query.service import Service

    def solve(payload):
        with tempfile.TemporaryDirectory(prefix='research-library-') as directory:
            # This directory is created by the adapter, not a supplied intake path.
            base = Path(directory).resolve()
            root = base / 'input'
            root.mkdir()
            path = base / 'library.sqlite3'
            store = Store(path)
            results = []
            try:
                for index, operation in enumerate(payload['operations']):
                    service = Service(store, root)
                    jobs = JobManager(store)
                    try:
                        op = operation['op']
                        if op == 'import':
                            source = checked_source(operation['source'])
                            staging = root / str(index)
                            target = staging / source
                            target.parent.mkdir(parents=True, exist_ok=True)
                            try:
                                target.write_bytes(operation['text'].encode('utf-8'))
                            except UnicodeError as error:
                                raise LibraryError('invalid_utf8') from error
                            result = import_file(store, staging, source)
                        elif op in ('list', 'search'):
                            result = service.listing(operation.get('query', '') if op == 'search' else '',
                                                     operation.get('offset', 0), operation.get('limit', 100))
                        elif op == 'show':
                            result = store.show(identity('document', operation['source']))
                        elif op == 'export':
                            ids = [identity('document', s) for s in operation['sources']] if 'sources' in operation else None
                            result = service.export(ids)
                        elif op == 'reopen':
                            store.close()
                            store = Store(path)
                            result = {'reopened': True}
                        elif payload.get('milestone') == 'm1' and op == 'submit':
                            result = jobs.submit(operation['job_id'], operation['entries'])
                        elif payload.get('milestone') == 'm1' and op in ('prepare', 'cancel', 'retry', 'job'):
                            method = jobs.get if op == 'job' else getattr(jobs, op)
                            result = method(operation['job_id'])
                        elif payload.get('milestone') == 'm1' and op == 'commit':
                            fail = operation.get('fail_before_commit', False)
                            if type(fail) is not bool:
                                raise LibraryError('invalid_request')
                            result = jobs.commit({'job_id': operation['job_id'], 'epoch': operation['epoch']},
                                                 fail_before_commit=fail)
                        else:
                            raise LibraryError('unsupported_operation')
                    except LibraryError as error:
                        result = {'error': error.code}
                    results.append(result)
                value = {'results': results, 'documents': store.documents()}
                if payload.get('milestone') == 'm1':
                    value['jobs'] = store.list_jobs()
                return value
            finally:
                store.close()
''')

_JOB_HTML = _source(r'''
    <section aria-labelledby="jobs-heading"><h2 id="jobs-heading">Ingestion jobs</h2>
    <p>Submit a local directory, ZIP archive, or JSON bundle. Prepare checks the batch; commit imports it atomically.</p>
    <form id="job-form"><label>Job ID <input id="job-id" required pattern="[A-Za-z0-9_-]{1,64}"></label>
    <label>Intake type <select id="job-kind"><option value="directory">Directory</option><option value="zip">ZIP archive</option><option value="json">JSON bundle</option></select></label>
    <label>Local path <input id="job-path" required></label><label>Namespace <input id="job-namespace" required></label>
    <button>Submit job</button></form><button id="jobs-refresh" type="button">Refresh jobs</button>
    <p id="jobs-status" role="status" aria-live="polite"></p><ul id="jobs" aria-label="Ingestion jobs"></ul></section>
''')

_JOB_SCRIPT = _source(r'''
    async function showJobs() {
      try {
        const value = await request('/api/jobs');
        byId('jobs').replaceChildren();
        for (const job of value.jobs) {
          const li = document.createElement('li');
          const description = document.createElement('span');
          description.textContent = job.job_id + ': ' + job.state + ', epoch ' + job.epoch + ', ' + job.completed + '/' + job.total + (job.error ? ', ' + job.error : '');
          li.append(description);
          const actions = job.state === 'queued' ? ['prepare', 'cancel'] : job.state === 'running' ? ['commit', 'cancel'] : ['failed', 'cancelled'].includes(job.state) ? ['retry'] : [];
          for (const action of actions) {
            const button = document.createElement('button');
            button.type = 'button'; button.textContent = action[0].toUpperCase() + action.slice(1);
            button.setAttribute('aria-label', action + ' ' + job.job_id);
            button.onclick = async () => {
              button.disabled = true;
              try {
                await request('/api/jobs/' + encodeURIComponent(job.job_id) + '/' + action, action === 'commit' ? {epoch: job.epoch} : {});
                byId('jobs-status').textContent = job.job_id + ': ' + action + ' succeeded';
              } catch (error) { byId('jobs-status').textContent = job.job_id + ': ' + error.message; }
              finally { await showJobs(); await list(); }
            };
            li.append(' ', button);
          }
          byId('jobs').append(li);
        }
      } catch (error) { byId('jobs-status').textContent = error.message; }
    }
    byId('job-kind').onchange = () => {
      const needsNamespace = byId('job-kind').value !== 'json';
      byId('job-namespace').disabled = !needsNamespace;
      byId('job-namespace').required = needsNamespace;
    };
    byId('job-form').onsubmit = async event => {
      event.preventDefault();
      const kind = byId('job-kind').value;
      const body = {job_id: byId('job-id').value, [kind]: byId('job-path').value};
      if (kind !== 'json') body.namespace = byId('job-namespace').value;
      try {
        const job = await request('/api/jobs', body);
        byId('jobs-status').textContent = job.job_id + ': ' + job.state;
      } catch (error) { byId('jobs-status').textContent = error.message; }
      await showJobs();
    };
    byId('jobs-refresh').onclick = showJobs;
    showJobs();
''')


def clients_files() -> dict[str, str]:
    """Return fresh authored replacements within the query and clients scopes."""
    browser = seed_files()["library/clients/index.html"]
    browser = browser.replace("</main><script>", _JOB_HTML + "</main><script>")
    browser = browser.replace("</script></body></html>", _JOB_SCRIPT + "</script></body></html>")
    browser = browser.replace("input,button{", "input,button,select{")
    return {
        "library/query/service.py": _SERVICE,
        "library/clients/cli.py": _CLI,
        "library/clients/workflow.py": _WORKFLOW,
        "library/clients/index.html": browser,
    }
