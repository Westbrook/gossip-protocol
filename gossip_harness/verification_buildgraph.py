"""Fresh build-graph maintenance benchmark with a host-only pure reference.

Only fixed transport and basic v0 code are public initially. Reference source,
known solutions and held-out cases must never be supplied to a model worker.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
import math
from pathlib import Path
import re
from textwrap import dedent

POLICY_V1 = '{"api":1,"schema":1,"max_import":4}\n'
POLICY_V2 = '{"api":2,"schema":2,"max_import":6}\n'
ALLOWED = ('buildgraph_app/storage.py', 'buildgraph_app/domain.py', 'buildgraph_app/service.py')
ADAPTER = dedent('''\
    import json
    from pathlib import Path
    import subprocess
    import sys
    import tempfile
    def solve(payload):
        with tempfile.TemporaryDirectory(prefix="buildgraph-", dir="/tmp") as directory:
            database = str(Path(directory) / "graph.sqlite")
            answers = []
            for command in payload["commands"]:
                completed = subprocess.run([sys.executable, "-I", str(Path(__file__).parent / "buildgraph_app/cli.py"), database],
                                           input=json.dumps(command), text=True, capture_output=True, timeout=3)
                if completed.returncode:
                    raise RuntimeError("buildgraph CLI failed")
                answers.append(json.loads(completed.stdout))
            return answers
''')
CLI = dedent('''\
    import json
    from pathlib import Path
    import sys
    root = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(root))
    from buildgraph_app import storage, service
    from buildgraph_app.domain import CommandError
    connection = storage.connect(sys.argv[1])
    try:
        command = json.load(sys.stdin)
        policy = json.loads((root / "policy.json").read_text())
        try:
            result = service.execute(connection, command, policy)
        except CommandError as error:
            result = {"error": error.code}
        print(json.dumps(result, ensure_ascii=True, allow_nan=False))
    finally:
        connection.close()
''')
STORAGE = dedent('''\
    import json
    import sqlite3
    def connect(path):
        db = sqlite3.connect(path, timeout=2)
        db.row_factory = sqlite3.Row
        with db:
            db.execute("CREATE TABLE IF NOT EXISTS targets (id TEXT PRIMARY KEY, source TEXT NOT NULL, priority INTEGER NOT NULL, deps TEXT NOT NULL, digest TEXT)")
            db.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            db.execute("INSERT OR IGNORE INTO meta VALUES ('version','0')")
            db.execute("CREATE TABLE IF NOT EXISTS receipts (key TEXT PRIMARY KEY, request TEXT NOT NULL, response TEXT NOT NULL)")
            db.execute("CREATE TABLE IF NOT EXISTS events (version INTEGER PRIMARY KEY, op TEXT NOT NULL, ids TEXT NOT NULL)")
        return db
    def load(db):
        graph = {r['id']:dict(source=r['source'], priority=r['priority'], deps=json.loads(r['deps']), digest=r['digest']) for r in db.execute('SELECT * FROM targets')}
        return graph, int(db.execute("SELECT value FROM meta WHERE key='version'").fetchone()[0])
    def save(db, graph, version, op, ids, audit):
        db.execute('DELETE FROM targets')
        db.executemany('INSERT INTO targets VALUES (?,?,?,?,?)', [(name, row['source'], row['priority'], json.dumps(row['deps']), row['digest']) for name,row in graph.items()])
        db.execute("UPDATE meta SET value=? WHERE key='version'", (str(version),))
        if audit:
            db.execute('INSERT INTO events VALUES (?,?,?)', (version, op, json.dumps(sorted(ids))))
    def receipt(db, key):
        row = db.execute('SELECT request,response FROM receipts WHERE key=?', (key,)).fetchone()
        return (row['request'], json.loads(row['response'])) if row else None
    def remember(db, key, request, response):
        db.execute('INSERT INTO receipts VALUES (?,?,?)', (key, request, json.dumps(response)))
    def audit(db):
        return [dict(version=r['version'], op=r['op'], ids=json.loads(r['ids'])) for r in db.execute('SELECT * FROM events ORDER BY version')]
''')
DOMAIN = dedent('''\
    import hashlib
    import json
    import re
    class CommandError(Exception):
        def __init__(self, code):
            super().__init__(code)
            self.code = code
    def fail(code):
        raise CommandError(code)
    def keys(value, required, optional=()):
        if type(value) is not dict or not set(required) <= value.keys() or set(value) - set(required) - set(optional):
            fail('invalid')
    def name(value):
        if type(value) is not str or re.fullmatch(r'[a-z][a-z0-9-]{0,15}', value) is None:
            fail('invalid')
        return value
    def names(value, low=0, high=6):
        if type(value) is not list or not low <= len(value) <= high:
            fail('invalid')
        for item in value:
            name(item)
        if len(set(value)) != len(value):
            fail('invalid')
        return sorted(value)
    def integer(value, low, high):
        if type(value) is not int or not low <= value <= high:
            fail('invalid')
        return value
    def record(value, legacy=False):
        keys(value, ('name','body','needs') if legacy else ('id','source','priority','deps'))
        identifier = name(value['name'] if legacy else value['id'])
        source = value['body'] if legacy else value['source']
        if type(source) is not str or len(source) > 64 or any(not 32 <= ord(c) <= 126 for c in source):
            fail('invalid')
        priority = 0 if legacy else integer(value['priority'], -3, 3)
        dependencies = names(value['needs'] if legacy else value['deps'])
        return dict(id=identifier, source=source, priority=priority, deps=dependencies)
    def validate(command, stage):
        if type(command) is not dict or type(command.get('op')) is not str:
            fail('invalid')
        op = command['op']
        if op in ('list','ready') or (stage >= 3 and op == 'audit') or (stage >= 4 and op in ('policy','export')):
            keys(command, ('op',))
        elif op == 'put':
            keys(command, ('op','id','source'), ('priority','deps'))
            record(dict(id=command['id'], source=command['source'], priority=command.get('priority',0), deps=command.get('deps',[])))
        elif op == 'remove' or (stage >= 2 and op in ('build','invalidate')):
            keys(command, ('op','id'))
            name(command['id'])
        elif stage >= 3 and op in ('plan','commit'):
            keys(command, ('op','targets') if op == 'plan' else ('op','targets','key','version'))
            names(command['targets'], 1, 8)
            if op == 'commit':
                name(command['key'])
                integer(command['version'], 0, 1000000)
        elif stage >= 4 and op == 'import':
            keys(command, ('op','key','manifest'))
            name(command['key'])
            manifest = command['manifest']
            keys(manifest, ('schema','targets'))
            if type(manifest['schema']) is not int or manifest['schema'] not in (1,2) or type(manifest['targets']) is not list or len(manifest['targets']) > 24:
                fail('invalid')
            records = [record(item, manifest['schema'] == 1) for item in manifest['targets']]
            if len({item['id'] for item in records}) != len(records):
                fail('invalid')
        else:
            fail('invalid')
        return op
    def canonical(value):
        return json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(',',':'))
    def fingerprint(graph, identifier):
        row = graph[identifier]
        value = dict(source=row['source'], deps=[[dep, graph[dep]['digest']] for dep in sorted(row['deps'])])
        return hashlib.sha256(canonical(value).encode()).hexdigest()
    def graph_valid(graph):
        if any(name in row['deps'] for name,row in graph.items()):
            fail('cycle')
        if any(dep not in graph for row in graph.values() for dep in row['deps']):
            fail('missing')
        visiting, done = set(), set()
        def visit(name):
            if name in visiting:
                fail('cycle')
            if name not in done:
                visiting.add(name)
                for dependency in graph[name]['deps']:
                    visit(dependency)
                visiting.remove(name)
                done.add(name)
        for name in graph:
            visit(name)
    def closure(graph, identifiers, reverse=False):
        reached, pending = set(), list(identifiers)
        while pending:
            name = pending.pop()
            if name not in reached:
                reached.add(name)
                pending.extend([other for other,row in graph.items() if name in row['deps']] if reverse else graph[name]['deps'])
        return reached
    def order(graph, identifiers):
        if any(name not in graph for name in identifiers):
            fail('missing')
        dirty = {name for name in closure(graph, identifiers) if graph[name]['digest'] is None}
        result = []
        while dirty:
            ready = [name for name in dirty if not set(graph[name]['deps']) & dirty]
            selected = min(ready, key=lambda name:(-graph[name]['priority'], name))
            dirty.remove(selected)
            result.append(selected)
        return result
''')
SERVICE = dedent('''\
    from . import storage
    from .domain import fail, validate, record, names, canonical, fingerprint, graph_valid, closure, order
    STAGE = __STAGE__
    def execute(db, command, policy):
        op = validate(command, STAGE)
        with db:
            graph, version = storage.load(db)
            if op == 'list':
                return [dict(id=name, **graph[name]) for name in sorted(graph)]
            if op == 'ready':
                return sorted([name for name,row in graph.items() if row['digest'] is None and all(graph[d]['digest'] is not None for d in row['deps'])], key=lambda name:(-graph[name]['priority'],name))
            if op == 'audit':
                return storage.audit(db)
            if op == 'policy':
                return policy
            if op == 'export':
                return dict(schema=policy['schema'], targets=[dict(id=name, source=graph[name]['source'], priority=graph[name]['priority'], deps=graph[name]['deps']) for name in sorted(graph)])
            if op == 'plan':
                return dict(order=order(graph, command['targets']), version=version)
            request = None
            if op in ('commit','import'):
                if op == 'commit':
                    request = canonical(dict(op=op, version=command['version'], targets=sorted(command['targets'])))
                else:
                    records = sorted([record(r, command['manifest']['schema'] == 1) for r in command['manifest']['targets']], key=lambda r:r['id'])
                    request = canonical(dict(op=op, targets=records))
                old = storage.receipt(db, command['key'])
                if old:
                    if old[0] != request:
                        fail('conflict')
                    return old[1]
            changed, identifiers = False, []
            if op == 'put':
                name = command['id']
                row = record(dict(id=name, source=command['source'], priority=command.get('priority',0), deps=command.get('deps',[])))
                row.pop('id')
                prior = graph.get(name)
                changed = prior is None or any(prior[k] != row[k] for k in ('source','priority','deps'))
                content_changed = prior is None or prior['source'] != row['source'] or prior['deps'] != row['deps']
                row['digest'] = prior['digest'] if prior else None
                graph[name] = row
                graph_valid(graph)
                if content_changed:
                    for affected in closure(graph, [name], reverse=True):
                        graph[affected]['digest'] = None
                identifiers, answer = [name], dict(ok='put', id=name, changed=changed)
            elif op == 'remove':
                name = command['id']
                if name not in graph:
                    fail('missing')
                if any(name in row['deps'] for row in graph.values()):
                    fail('in_use')
                del graph[name]
                changed, identifiers, answer = True, [name], dict(ok='removed', id=name)
            elif op == 'build':
                name = command['id']
                if name not in graph:
                    fail('missing')
                if any(graph[d]['digest'] is None for d in graph[name]['deps']):
                    fail('blocked')
                changed = graph[name]['digest'] is None
                if changed:
                    graph[name]['digest'] = fingerprint(graph, name)
                identifiers, answer = [name], dict(id=name, digest=graph[name]['digest'], cached=not changed)
            elif op == 'invalidate':
                name = command['id']
                if name not in graph:
                    fail('missing')
                identifiers = sorted(closure(graph, [name], reverse=True))
                changed = any(graph[n]['digest'] is not None for n in identifiers)
                for affected in identifiers:
                    graph[affected]['digest'] = None
                answer = dict(invalidated=identifiers)
            elif op == 'commit':
                if command['version'] != version:
                    fail('stale')
                identifiers = order(graph, command['targets'])
                for name in identifiers:
                    graph[name]['digest'] = fingerprint(graph, name)
                changed = True
                answer = dict(key=command['key'], built=identifiers, version=version+1,
                              digests={name:graph[name]['digest'] for name in sorted(command['targets'])})
            elif op == 'import':
                if len(records) > policy['max_import']:
                    fail('quota')
                graph = {r['id']:dict(source=r['source'], priority=r['priority'], deps=r['deps'], digest=None) for r in records}
                graph_valid(graph)
                changed, identifiers = True, sorted(graph)
                answer = dict(ok='imported', key=command['key'], count=len(graph), version=version+1)
            else:
                fail('invalid')
            if changed:
                storage.save(db, graph, version+1, op, identifiers, STAGE >= 3)
            if request is not None:
                storage.remember(db, command['key'], request, answer)
            return answer
''')
BASE_DOMAIN = dedent('''\
    import re
    class CommandError(Exception):
        def __init__(self, code):
            self.code = code
    def fail(code):
        raise CommandError(code)
    def validate(command):
        if type(command) is not dict:
            fail('invalid')
        if command.get('op') == 'list' and set(command) == {'op'}:
            return 'list'
        if command.get('op') != 'put' or not {'op','id','source'} <= command.keys() or set(command)-{'op','id','source','priority'}:
            fail('invalid')
        if type(command['id']) is not str or re.fullmatch(r'[a-z][a-z0-9-]{0,15}',command['id']) is None:
            fail('invalid')
        if type(command['source']) is not str or len(command['source']) > 64 or any(not 32 <= ord(c) <= 126 for c in command['source']):
            fail('invalid')
        priority=command.get('priority',0)
        if type(priority) is not int or not -3 <= priority <= 3:
            fail('invalid')
        return 'put'
''')
BASE_SERVICE = dedent('''\
    from . import storage
    from .domain import validate
    def execute(db, command, policy):
        op = validate(command)
        with db:
            graph, version = storage.load(db)
            if op == 'list':
                return [dict(id=name, **graph[name]) for name in sorted(graph)]
            name = command['id']
            row = dict(source=command['source'], priority=command.get('priority',0), deps=[], digest=None)
            changed = graph.get(name) != row
            graph[name] = row
            if changed:
                storage.save(db,graph,version+1,'put',[name],False)
            return dict(ok='put',id=name,changed=changed)
''')


# The initial application persists targets/version only. Retry receipts and audit
# storage are maintenance work for later stages, not preimplemented public helpers.
BASE_STORAGE = STORAGE.replace(
    '        db.execute("CREATE TABLE IF NOT EXISTS receipts (key TEXT PRIMARY KEY, request TEXT NOT NULL, response TEXT NOT NULL)")\n', ''
).replace(
    '        db.execute("CREATE TABLE IF NOT EXISTS events (version INTEGER PRIMARY KEY, op TEXT NOT NULL, ids TEXT NOT NULL)")\n', ''
).replace(
    "    if audit:\n        db.execute('INSERT INTO events VALUES (?,?,?)', (version, op, json.dumps(sorted(ids))))\n", ''
).split('def receipt(db, key):')[0]

def _files(stage):
    return {'solution.py':ADAPTER, 'buildgraph_app/cli.py':CLI, 'buildgraph_app/__init__.py':'',
            'policy.json':POLICY_V2 if stage == 4 else POLICY_V1,
            ALLOWED[0]:STORAGE if stage else BASE_STORAGE, ALLOWED[1]:DOMAIN if stage else BASE_DOMAIN,
            ALLOWED[2]:SERVICE.replace('__STAGE__',str(stage)) if stage else BASE_SERVICE}

COMMON = """Maintain the existing Python3.12 standard-library SQLite build graph. Only
buildgraph_app/storage.py, domain.py and service.py are editable. solution.py,
CLI, package marker and policy.json are fixed. The bridge calls storage.connect(path)
and service.execute(connection, command, policy), catching domain.CommandError.code.
Each command runs in a NEW CLI process using the SAME scenario database. Source
strings represent build inputs; no source code is executed. No network or packages.
Payload is {"commands":[command,...]}, maximum24 commands; return one JSON result
per command. Preserve every earlier milestone. Input envelopes are valid and bounded;
command field/type/shape errors ARE tested. Reject malformed/extra/missing command
fields and unknown op values with {"error":"invalid"}, never uncaught exceptions.
Identifiers and retry keys match [a-z][a-z0-9-]{0,15}. Source strings are0..64 printable
ASCII characters. Priority is integer -3..3; booleans/floats are not integers.
Dependency lists have0..6 unique identifiers. Syntax validation precedes all state
checks. Every error changes nothing. Commands not introduced yet are outside that
milestone's input domain. JSON object order does not matter; list order/types do.
Input-domain bounds: nesting depth<=10; <=3000 JSON value nodes (object keys not
counted); arrays/objects<=32 entries; strings<=128 characters, object keys<=64;
numbers finite and magnitude<=1000000; compact ensure_ascii JSON<=24000 bytes.
These are scenario bounds, not command validity: malformed field values inside
these bounds must produce invalid. The commands array may be empty.
The public adapter permits at most3s per CLI invocation. Persist all state in SQLite.
"""
SPEC1 = COMMON + """
B1-put: {op:'put',id,source,priority?:0,deps?:[]} upserts the full target definition.
Omitted optional fields reset to defaults. Validate the complete command syntax,
then the proposed graph: any self-dependency => cycle; otherwise any missing
prerequisite => missing; otherwise any transitive cycle => cycle, in that order.
Successful put returns {ok:'put',id,changed:boolean}, where changed compares the
source, priority and sorted dependency list to the prior definition (new=true).
Identical puts, including dependency-order permutations, are no-ops. Failed edits
preserve the previous target and all its edges.
B1-remove: {op:'remove',id} returns missing if absent, else in_use if any other
target directly depends on it; otherwise deletes it and returns {ok:'removed',id}.
B1-order: {op:'list'} returns targets by increasing id, exactly
{id,source,priority,deps:[sorted IDs],digest:null}. All initial caches are dirty(null).
{op:'ready'} returns dirty target IDs whose prerequisites have non-null digests,
ordered decreasing priority then increasing id. At this milestone no builds occur,
so only targets without dependencies are ready. Empty results are [].
B1-persist: Every successful update/removal survives fresh CLI invocations.
"""
SPEC2 = COMMON + """
All milestone1 graph behavior continues; list.digest can now also be a64-character
lowercase SHA256 hex string. B2-build: {op:'build',id}: missing if absent, blocked
if any direct prerequisite has null digest, else {id,digest,cached:boolean}.
A dirty target builds and cached=false; an already-clean target returns its existing
digest and cached=true without change. Its digest is SHA256 of UTF-8 canonical JSON
{"source":SOURCE,"deps":[[dependencyID,dependencyDigest],... sorted by ID]}, using
json.dumps(...,sort_keys=True,ensure_ascii=True,separators=(',',':')). IDs, priority
and the target's own name are absent from its digest; direct dependency IDs included.
B2-invalidate: A put changing source or dependency set clears that target's digest
AND every transitive reverse-dependent digest. Priority-only edits preserve caches.
{op:'invalidate',id} returns missing if absent, otherwise clears the same transitive
closure and returns {invalidated:[all affected IDs sorted]}, even if already dirty.
B2-durable: Cache state/digests persist across processes. Failed puts/builds/removals
must not invalidate valid caches; rebuilding unchanged inputs yields the same digest.
B2-ready: ready excludes clean nodes, reports only dirty nodes with all prerequisites
clean, and keeps the priority/id ordering. Shared ancestors do not create duplicates.
"""
SPEC3 = COMMON + """
B3-plan: Global version starts0 and increases by1 for each effective put, successful
remove, newly-built target, or invalidate that cleared at least one non-null digest.
No-op puts, cached builds, ineffective invalidations, reads and errors do not advance it.
{op:'plan',targets:[1..8 unique IDs]} returns {order:[IDs],version}. Missing any target
=> missing. Include all their ancestors, omit already-clean nodes, then repeatedly
choose an unplanned dirty node with no unplanned dependencies, by decreasing priority
then increasing id. This dependency-first ordering is deterministic across roots.
B3-commit: {op:'commit',key,version,targets} atomically builds all nodes in that plan,
then increases global version ONCE even for an empty plan. version is integer0..1000000.
Return {key,built:[plan order],version:newVersion,digests:{requestedID:digest,...}}.
First validate the whole syntax; then a stored key replays/conflicts (see below);
then version mismatch => stale; then missing target => missing. No partial builds,
key consumption, version or audit changes on failure. Cache each built node before
its dependents. Existing clean caches are preserved.
B3-retry: Persist successful commit receipts keyed by key. Same normalized request
(op,version,sorted targets) returns the exact original response even after later
edits/removals. A different request using that key => conflict. Invalid syntax always
precedes replay/conflict. Failed requests do not consume keys. Do not reapply replay.
B3-audit: {op:'audit'} returns [{version,op,ids},...] increasing by version. Exactly
one event per effective put/remove/build/invalidate and each FIRST successful commit;
no events for reads, no-ops, errors or key replays. ids is sorted: put/remove/build
name, entire affected closure for effective invalidate, built nodes for commit.
A commit with no builds still has an event with ids:[]. Records survive restarts.
"""
SPEC4 = COMMON + """
All earlier semantics continue. An upstream trusted integration changes policy.json
from API1/schema1/max_import4 to API2/schema2/max_import6. Read policy passed by the
fixed CLI; do not edit the policy file or hard-code the old contract.
B4-policy: {op:'policy'} returns that policy object. {op:'export'} returns
{schema:policy.schema,targets:[{id,source,priority,deps},...sorted by id]}, omitting
all cached digests. It does not change version/audit or consume a retry key.
B4-import: {op:'import',key,manifest:{schema,targets}} atomically REPLACES the graph.
schema2 records have exactly id/source/priority/deps; schema1 legacy records have
exactly name/body/needs and normalize to id/source/deps with priority0. Types/ranges
match put; duplicate target IDs or dependency IDs are invalid. Up to24 manifest
records are syntactically allowed. After full syntax validation, normalize record
and dependency ordering, then replay/conflict by shared retry key, then if record
count > policy.max_import return quota, then graph checks self-cycle before missing
before transitive cycle. Every failure preserves graph/caches/version/receipts/audit.
B4-migrate: First successful import, even an identical or empty graph, clears ALL
imported caches, preserves old receipts, increments version once, and returns
{ok:'imported',key,count,version}. Record one import event with all new IDs sorted.
Import and commit share the key namespace; normalized request includes op, so using
a commit key for import conflicts. Equivalent legacy/current manifests replay one
import receipt, even after later edits. Replays do not replace the graph again.
B4-integrate: Export/import schemas, policy quotas, caches and retry/audit persistence
must work together through fresh CLI processes, with earlier command behavior intact.
"""

# Independent pure reference: dictionary snapshots and fixed-point graph walks.
class _Failure(Exception):
    pass

def _bad(code='invalid'):
    raise _Failure(code)

def _ident(value):
    if type(value) is not str or re.fullmatch(r'[a-z][a-z0-9-]{0,15}',value) is None:
        _bad()

def _fields(value, required, optional=()):
    if type(value) is not dict or set(value)-set(required)-set(optional) or set(required)-set(value):
        _bad()

def _num(value, low, high):
    if type(value) is not int or value < low or value > high:
        _bad()

def _ids(value, minimum=0, maximum=6):
    if type(value) is not list or not minimum <= len(value) <= maximum:
        _bad()
    for name in value:
        _ident(name)
    if len(set(value)) != len(value):
        _bad()
    return sorted(value)

def _record(value, schema=2):
    _fields(value, ('name','body','needs') if schema == 1 else ('id','source','priority','deps'))
    name, source = (value['name'],value['body']) if schema == 1 else (value['id'],value['source'])
    _ident(name)
    if type(source) is not str or len(source)>64 or any(ord(c)<32 or ord(c)>126 for c in source):
        _bad()
    priority=0 if schema==1 else value['priority']
    _num(priority,-3,3)
    deps=_ids(value['needs'] if schema==1 else value['deps'])
    return dict(id=name,source=source,priority=priority,deps=deps)

def _canonical(value):
    return json.dumps(value,sort_keys=True,ensure_ascii=True,allow_nan=False,separators=(',',':'))

def _reference_syntax(command,stage):
    if type(command) is not dict or type(command.get('op')) is not str:
        _bad()
    op=command['op']
    if op in ('list','ready') or stage>=2 and op=='audit' or stage>=3 and op in ('policy','export'):
        _fields(command,('op',))
    elif op=='put':
        _fields(command,('op','id','source'),('priority','deps'))
        _record(dict(id=command['id'],source=command['source'],priority=command.get('priority',0),deps=command.get('deps',[])))
    elif op=='remove' or stage>=1 and op in ('build','invalidate'):
        _fields(command,('op','id'))
        _ident(command['id'])
    elif stage>=2 and op in ('plan','commit'):
        _fields(command,('op','targets') if op=='plan' else ('op','targets','key','version'))
        _ids(command['targets'],1,8)
        if op=='commit':
            _ident(command['key'])
            _num(command['version'],0,1000000)
    elif stage>=3 and op=='import':
        _fields(command,('op','key','manifest'))
        _ident(command['key'])
        manifest=command['manifest']
        _fields(manifest,('schema','targets'))
        if type(manifest['schema']) is not int or manifest['schema'] not in (1,2) or type(manifest['targets']) is not list or len(manifest['targets'])>24:
            _bad()
        normalized=[_record(item,manifest['schema']) for item in manifest['targets']]
        if len({r['id'] for r in normalized})!=len(normalized):
            _bad()
    else:
        _bad()
    return op

def _closure(graph,names,reverse=False):
    reached=set(names)
    while True:
        expanded=reached|({name for name,r in graph.items() if reached.intersection(r['deps'])} if reverse else {dep for name in reached for dep in graph[name]['deps']})
        if expanded==reached:
            return reached
        reached=expanded

def _valid_graph(graph):
    if any(name in r['deps'] for name,r in graph.items()):
        _bad('cycle')
    if any(dep not in graph for r in graph.values() for dep in r['deps']):
        _bad('missing')
    remaining=set(graph)
    while remaining:
        removable={name for name in remaining if not remaining.intersection(graph[name]['deps'])}
        if not removable:
            _bad('cycle')
        remaining-=removable

def _plan(graph,targets):
    if any(name not in graph for name in targets):
        _bad('missing')
    todo=_closure(graph,targets)
    remaining={name for name in todo if graph[name]['digest'] is None}
    result=[]
    for _ in range(len(remaining)):
        eligible=sorted((name for name in remaining if all(dep not in remaining for dep in graph[name]['deps'])),key=lambda n:(-graph[n]['priority'],n))
        chosen=eligible[0]
        result.append(chosen)
        remaining.remove(chosen)
    return result

def _digest(graph,name):
    item=graph[name]
    text=_canonical({'deps':[[d,graph[d]['digest']] for d in sorted(item['deps'])],'source':item['source']})
    return hashlib.sha256(text.encode('utf-8')).hexdigest()

def _apply(state,command,stage):
    op=_reference_syntax(command,stage)
    graph,version=state['graph'],state['version']
    if op=='list':
        return [dict(id=name,**deepcopy(graph[name])) for name in sorted(graph)]
    if op=='ready':
        return sorted([name for name,r in graph.items() if r['digest'] is None and all(graph[d]['digest'] is not None for d in r['deps'])],key=lambda n:(-graph[n]['priority'],n))
    if op=='audit':
        return deepcopy(state['audit'])
    if op=='plan':
        return {'order':_plan(graph,command['targets']),'version':version}
    if op=='policy':
        return json.loads(POLICY_V2)
    if op=='export':
        return dict(schema=2,targets=[dict(id=name,**{k:deepcopy(graph[name][k]) for k in ('source','priority','deps')}) for name in sorted(graph)])
    request=None
    if op in ('commit','import'):
        if op=='commit':
            request={'op':op,'version':command['version'],'targets':sorted(command['targets'])}
        else:
            records=sorted([_record(r,command['manifest']['schema']) for r in command['manifest']['targets']],key=lambda r:r['id'])
            request={'op':op,'targets':records}
        if command['key'] in state['receipts']:
            old_request,old_response=state['receipts'][command['key']]
            if request!=old_request:
                _bad('conflict')
            return deepcopy(old_response)
    changed,ids=False,[]
    if op=='put':
        name=command['id']
        row=_record(dict(id=name,source=command['source'],priority=command.get('priority',0),deps=command.get('deps',[])))
        row.pop('id')
        previous=graph.get(name)
        changed=previous is None or {k:previous[k] for k in row}!=row
        content=previous is None or any(previous[k]!=row[k] for k in ('source','deps'))
        graph[name]=dict(row,digest=previous['digest'] if previous else None)
        _valid_graph(graph)
        if content:
            for name_affected in _closure(graph,[name],True):
                graph[name_affected]['digest']=None
        ids,answer=[name],dict(ok='put',id=name,changed=changed)
    elif op=='remove':
        name=command['id']
        if name not in graph:
            _bad('missing')
        if any(name in r['deps'] for r in graph.values()):
            _bad('in_use')
        del graph[name]
        changed,ids,answer=True,[name],dict(ok='removed',id=name)
    elif op=='build':
        name=command['id']
        if name not in graph:
            _bad('missing')
        if any(graph[d]['digest'] is None for d in graph[name]['deps']):
            _bad('blocked')
        changed=graph[name]['digest'] is None
        if changed:
            graph[name]['digest']=_digest(graph,name)
        ids,answer=[name],dict(id=name,digest=graph[name]['digest'],cached=not changed)
    elif op=='invalidate':
        name=command['id']
        if name not in graph:
            _bad('missing')
        ids=sorted(_closure(graph,[name],True))
        changed=any(graph[n]['digest'] is not None for n in ids)
        for affected in ids:
            graph[affected]['digest']=None
        answer=dict(invalidated=ids)
    elif op=='commit':
        if command['version']!=version:
            _bad('stale')
        ids=_plan(graph,command['targets'])
        for name in ids:
            graph[name]['digest']=_digest(graph,name)
        changed=True
        answer=dict(key=command['key'],built=ids,version=version+1,digests={n:graph[n]['digest'] for n in sorted(command['targets'])})
    elif op=='import':
        if len(records)>6:
            _bad('quota')
        graph={r['id']:dict(source=r['source'],priority=r['priority'],deps=r['deps'],digest=None) for r in records}
        _valid_graph(graph)
        state['graph']=graph
        changed,ids=True,sorted(graph)
        answer=dict(ok='imported',key=command['key'],count=len(graph),version=version+1)
    else:
        _bad()
    if changed:
        state['version']=version+1
        if stage>=2:
            state['audit'].append(dict(version=version+1,op=op,ids=sorted(ids)))
    if request is not None:
        state['receipts'][command['key']]=(deepcopy(request),deepcopy(answer))
    return answer

INTRODUCED = {'put':0,'remove':0,'list':0,'ready':0,'build':1,'invalidate':1,'plan':2,'commit':2,'audit':2,'policy':3,'export':3,'import':3}

def validate_input(stage_index, payload):
    """Validate scenario bounds; malformed command fields deliberately remain in-domain."""
    def outside():
        raise ValueError('outside_input_domain')
    if type(stage_index) is not int or not 0<=stage_index<=3 or type(payload) is not dict or set(payload)!={'commands'} or type(payload['commands']) is not list or len(payload['commands'])>24:
        outside()
    budget=[3000]
    def visit(value,depth=0):
        budget[0]-=1
        if budget[0]<0 or depth>10:
            outside()
        if value is None or type(value) is bool:
            return
        if type(value) in (int,float):
            if abs(value)>1000000 or not math.isfinite(value):
                outside()
            return
        if type(value) is str:
            if len(value)>128:
                outside()
            return
        if type(value) is list:
            if len(value)>32:
                outside()
            for item in value: visit(item,depth+1)
            return
        if type(value) is dict:
            if len(value)>32 or any(type(k) is not str or len(k)>64 for k in value):
                outside()
            for item in value.values(): visit(item,depth+1)
            return
        outside()
    visit(payload)
    if len(_canonical(payload).encode('ascii'))>24000:
        outside()
    for command in payload['commands']:
        if type(command) is dict and type(command.get('op')) is str and INTRODUCED.get(command['op'],-1)>stage_index:
            outside()


def reference(stage_index,payload):
    validate_input(stage_index,payload)
    state={'graph':{},'version':0,'receipts':{},'audit':[]}
    answers=[]
    for command in payload['commands']:
        trial=deepcopy(state)
        try:
            answer=_apply(trial,command,stage_index)
        except _Failure as error:
            answer={'error':str(error)}
        else:
            state=trial
        answers.append(answer)
    return answers


def put(name, source='x', priority=0, deps=None):
    return dict(op='put',id=name,source=source,priority=priority,deps=[] if deps is None else deps)

def command(op,**values):
    return dict(op=op,**values)

def commit(key,version,*targets):
    return command('commit',key=key,version=version,targets=list(targets))

def manifest(*records, schema=2):
    return dict(schema=schema,targets=list(records))

def target(name, source='x', priority=0, deps=()):
    return dict(id=name,source=source,priority=priority,deps=list(deps))

def imported(key,*records,schema=2):
    return command('import',key=key,manifest=manifest(*records,schema=schema))

LIST,READY,AUDIT,EXPORT,POLICY=[command(op) for op in ('list','ready','audit','export','policy')]
def build(name): return command('build',id=name)
def invalidate(name): return command('invalidate',id=name)
def remove(name): return command('remove',id=name)
def plan(*names): return command('plan',targets=list(names))

def _case(stage,name,requirement,commands):
    payload={'commands':commands}
    return dict(id='buildgraph-'+name,requirement=requirement,input=payload,expected=reference(stage,payload))

def _cases(stage,items):
    return tuple(_case(stage,*item) for item in items)

V1=_cases(0,(
 ('visible-upsert','B1-put',[put('a','one'),put('a','one'),put('a','two',2),LIST]),
 ('visible-priority','B1-order',[put('low','l',-2),put('z','z',3),put('a','a',3),READY,LIST]),
 ('visible-cycle','B1-put',[put('root'),put('leaf',deps=['root']),put('root',deps=['leaf']),LIST]),
 ('visible-remove','B1-remove',[put('base'),put('child',deps=['base']),remove('base'),remove('child'),remove('base'),LIST]),
 ('visible-invalid-types','B1-put',[put('bad',priority=True),put('a',deps=['a','a']),command('put',id='UPPER',source='x'),LIST]),
 ('visible-persistence','B1-persist',[put('saved','first'),LIST,put('saved','second'),LIST,READY]),
))
H1=_cases(0,(
 ('hidden-self-missing-precedence','B1-put',[put('target',deps=['absent','target']),LIST]),
 ('hidden-transitive-edit','B1-put',[put('top'),put('mid',deps=['top']),put('bottom',deps=['mid']),put('top',deps=['bottom']),READY,LIST]),
 ('hidden-dependency-order','B1-put',[put('z'),put('a'),put('join',deps=['z','a']),put('join',deps=['a','z']),LIST]),
 ('hidden-priority-default-reset','B1-order',[put('reset','v',3),command('put',id='reset',source='v'),put('other','v',1),READY,LIST]),
 ('hidden-missing-remove','B1-remove',[remove('ghost'),put('present'),remove('ghost'),LIST]),
 ('hidden-guard-existing-edit','B1-persist',[put('preserve','good',-3),put('preserve','bad',2,['unknown']),LIST]),
 ('hidden-type-product','B1-put',[put('typed',source=value) for value in (None,True,12,1.0,[],{})]+[LIST]),
 ('hidden-unhashable-operation','B1-put',[{'op':[]},{'op':{}},None,[],True,LIST]),
 ('hidden-dependency-types','B1-put',[put('typed',deps=value) for value in (None,'a',[[]],[{}],[1],[False])]+[LIST]),
 ('hidden-priority-types','B1-order',[put('typed',priority=value) for value in (None,False,0.0,[],{},4,-4)]+[READY]),
 ('hidden-fields-source-boundary','B1-put',[{'op':'put','id':'ok'},dict(put('ok'),extra=1),put('ok','\n'),put('ok','x'*65),put('ok',''),LIST]),
 ('hidden-diamond-persistence','B1-persist',[put('seed'),put('left',deps=['seed']),put('right',deps=['seed']),put('end',deps=['right','left']),remove('right'),LIST,READY]),
))
V2=_cases(1,(
 ('visible-digest-cache','B2-build',[put('a','hello'),build('a'),build('a'),LIST,READY]),
 ('visible-build-dependency','B2-ready',[put('root','r'),put('leaf','l',deps=['root']),build('leaf'),build('root'),READY,build('leaf'),READY]),
 ('visible-transitive-invalidation','B2-invalidate',[put('a'),put('b',deps=['a']),put('c',deps=['b']),build('a'),build('b'),build('c'),put('a','changed'),LIST,READY]),
 ('visible-priority-keeps-cache','B2-invalidate',[put('a','v'),build('a'),put('a','v',3),build('a'),LIST]),
 ('visible-explicit-invalidation','B2-durable',[put('a','v'),build('a'),invalidate('a'),invalidate('a'),build('a'),LIST]),
 ('visible-failure-preserves-cache','B2-durable',[put('a','stable'),build('a'),put('a','new',deps=['none']),build('a'),build('none'),LIST]),
))
H2=_cases(1,(
 ('hidden-diamond-cache','B2-invalidate',[put('s','S'),put('l','L',deps=['s']),put('r','R',deps=['s']),put('j','J',deps=['l','r']),build('s'),build('l'),build('r'),build('j'),invalidate('s'),LIST,READY]),
 ('hidden-dependency-rewire','B2-invalidate',[put('old','O'),put('new','N'),put('user','U',deps=['old']),build('old'),build('new'),build('user'),put('user','U',deps=['new']),build('user'),LIST]),
 ('hidden-source-revert','B2-durable',[put('v','original'),build('v'),put('v','temporary'),build('v'),put('v','original'),build('v')]),
 ('hidden-alias-content','B2-build',[put('one','same',-1),put('two','same',2),build('one'),build('two'),LIST]),
 ('hidden-dependency-name-in-digest','B2-build',[put('a','same'),put('b','same'),put('one','x',deps=['a']),put('two','x',deps=['b']),build('a'),build('b'),build('one'),build('two')]),
 ('hidden-cache-noop-reordered-deps','B2-durable',[put('a'),put('z'),put('join','J',deps=['z','a']),build('a'),build('z'),build('join'),put('join','J',deps=['a','z']),build('join'),put('join','J',-3,['a','z']),build('join')]),
 ('hidden-remove-blocked-cache','B2-durable',[put('p'),put('q',deps=['p']),build('p'),build('q'),remove('p'),build('q'),LIST]),
 ('hidden-dirty-high-priority','B2-ready',[put('slow','s',-3),put('fast','f',3, ['slow']),put('other','o',1),READY,build('slow'),READY,build('fast'),READY]),
 ('hidden-invalidate-leaf-only','B2-invalidate',[put('a'),put('b',deps=['a']),put('c',deps=['a']),build('a'),build('b'),build('c'),invalidate('b'),build('a'),build('c'),LIST]),
 ('hidden-build-field-types','B2-build',[command('build',id=value) for value in (None,True,1,[],{})]+[command('invalidate',id=[]),LIST]),
 ('hidden-build-extra-fields','B2-build',[put('keep'),build('keep'),dict(build('keep'),extra=1),dict(invalidate('keep'),force=True),build('keep')]),
 ('hidden-empty-source-digest','B2-build',[put('empty',''),put('quote','"\\'),build('empty'),build('quote'),invalidate('absent'),LIST]),
))
V3=_cases(2,(
 ('visible-plan-order','B3-plan',[put('base','b',-3),put('z','z',3,['base']),put('a','a',1),plan('z','a'),AUDIT]),
 ('visible-atomic-commit','B3-commit',[put('base'),put('end',deps=['base']),commit('k',2,'end'),LIST,plan('end'),AUDIT]),
 ('visible-stale-retry','B3-retry',[put('a'),commit('key',0,'a'),commit('key',1,'a'),commit('key',1,'a'),AUDIT]),
 ('visible-replay-after-edit','B3-retry',[put('a','old'),commit('key',1,'a'),put('a','new'),commit('key',1,'a'),LIST,AUDIT]),
 ('visible-noop-audit','B3-audit',[put('a'),put('a'),build('a'),build('a'),invalidate('a'),invalidate('a'),AUDIT,plan('a')]),
 ('visible-failed-atomic-plan','B3-commit',[put('a'),commit('unused',1,'a','ghost'),LIST,commit('unused',1,'a'),AUDIT]),
))
H3=_cases(2,(
 ('hidden-multi-root-priority','B3-plan',[put('low','l',-3),put('hi','h',3,['low']),put('mid','m',1),put('tail','t',0,['hi','mid']),plan('tail','mid'),commit('all',4,'tail','mid'),AUDIT]),
 ('hidden-shared-ancestor-single-build','B3-commit',[put('root'),put('a',deps=['root']),put('b',deps=['root']),commit('both',3,'b','a'),LIST,AUDIT]),
 ('hidden-empty-plan-version','B3-audit',[put('a'),build('a'),plan('a'),commit('empty',2,'a'),commit('empty',2,'a'),plan('a'),AUDIT]),
 ('hidden-reordered-replay','B3-retry',[put('a'),put('b'),commit('key',2,'b','a'),commit('key',2,'a','b'),AUDIT]),
 ('hidden-replay-after-removal','B3-retry',[put('a'),commit('key',1,'a'),remove('a'),commit('key',1,'a'),LIST,AUDIT]),
 ('hidden-conflict-before-stale','B3-retry',[put('a'),commit('key',1,'a'),commit('key',0,'absent'),AUDIT]),
 ('hidden-invalid-before-replay','B3-retry',[put('a'),commit('key',1,'a'),commit('key',True,'a'),dict(commit('key',1,'a'),extra=0),AUDIT]),
 ('hidden-stale-before-missing','B3-commit',[put('a'),commit('k',0,'missing'),commit('k',1,'missing'),commit('k',1,'a'),AUDIT]),
 ('hidden-plan-bad-target-types','B3-plan',[command('plan',targets=value) for value in ([],None,'a',[[]],[{}],[False],['a','a'])]+[LIST]),
 ('hidden-commit-bad-types','B3-commit',[command('commit',key=value,version=0,targets=['a']) for value in (None,True,[],{})]+[commit('key',0.0,'a'),command('commit',key='key',version=0,targets={}),AUDIT]),
 ('hidden-audit-invalidation-closure','B3-audit',[put('a'),put('b',deps=['a']),commit('ab',2,'b'),invalidate('a'),invalidate('a'),put('a','x',2),AUDIT,plan('b')]),
 ('hidden-noop-put-plan-stability','B3-plan',[put('a'),put('b'),put('join',deps=['b','a']),put('join',deps=['a','b']),plan('join'),commit('once',3,'join'),AUDIT]),
))
V4=_cases(3,(
 ('visible-policy-export','B4-policy',[POLICY,put('a','code',2),EXPORT,AUDIT]),
 ('visible-current-import','B4-import',[imported('new',target('a','A'),target('b','B',deps=['a'])),LIST,plan('b'),commit('build',1,'b'),EXPORT]),
 ('visible-legacy-normalization','B4-migrate',[imported('old',{'name':'legacy','body':'v','needs':[]},schema=1),EXPORT,build('legacy'),AUDIT]),
 ('visible-import-atomic','B4-import',[put('keep'),build('keep'),imported('bad',target('orphan',deps=['missing'])),LIST,AUDIT]),
 ('visible-import-retry','B4-migrate',[imported('key',target('a')),put('a','changed'),imported('key',target('a')),LIST,AUDIT]),
 ('visible-shared-key-namespace','B4-integrate',[put('a'),commit('shared',1,'a'),imported('shared',target('a')),EXPORT,AUDIT]),
))
H4=_cases(3,(
 ('hidden-six-policy-quota','B4-policy',[imported('six',*[target('n'+str(i)) for i in range(6)]),EXPORT,POLICY]),
 ('hidden-quota-before-graph','B4-import',[put('keep','v'),build('keep'),imported('too-many',*[target('n'+str(i),deps=['absent']) for i in range(7)]),LIST,AUDIT]),
 ('hidden-legacy-current-replay','B4-migrate',[imported('k',{'name':'old','body':'O','needs':[]},schema=1),build('old'),imported('k',target('old','O')),LIST,AUDIT]),
 ('hidden-import-reordered-graph','B4-migrate',[imported('k',target('z'),target('a'),target('j',deps=['z','a'])),build('a'),imported('k',target('j',deps=['a','z']),target('a'),target('z')),LIST,AUDIT]),
 ('hidden-import-transitive-cycle','B4-import',[put('safe'),imported('cycle',target('a',deps=['c']),target('b',deps=['a']),target('c',deps=['b'])),LIST,AUDIT]),
 ('hidden-import-self-before-missing','B4-import',[imported('cycle',target('a',deps=['absent','a'])),LIST,AUDIT]),
 ('hidden-empty-import-retains-receipts','B4-integrate',[put('gone'),commit('receipt',1,'gone'),imported('empty'),commit('receipt',1,'gone'),LIST,AUDIT]),
 ('hidden-import-clears-caches','B4-integrate',[put('a'),build('a'),imported('replace',target('a')),LIST,build('a'),plan('a'),AUDIT]),
 ('hidden-manifest-type-product','B4-import',[command('import',key='k',manifest=value) for value in (None,[],{},True)]+[imported('k',target('a'),schema=True),command('import',key='k',manifest={'schema':2,'targets':[[]]}),LIST]),
 ('hidden-manifest-duplicate-invalid','B4-import',[imported('k',target('a'),target('a')),imported('k',target('a',deps=['a','a'])),imported('k',dict(target('a'),extra=1)),imported('k',target('a')),AUDIT]),
 ('hidden-import-receipt-conflict','B4-migrate',[imported('k',target('a')),imported('k',target('b',deps=['missing'])),imported('k',target('a',priority=True)),EXPORT,AUDIT]),
 ('hidden-policy-read-shapes','B4-policy',[dict(POLICY,extra=1),dict(EXPORT,extra=1),{'op':[]},{'op':{}},POLICY,EXPORT,AUDIT]),
))

PROJECT = dict(id='buildgraph',title='Build graph and artifact cache maintenance',initial_files=_files(0),allowed_paths=ALLOWED,
    stages=tuple(dict(id=f'buildgraph-{i+1}',title=title,specification=spec,requirements=tuple(requirements),
                      visible_cases=visible,hidden_cases=hidden,known_files=_files(i+1),
                      **({'trusted_updates':{'policy.json':POLICY_V2}} if i==3 else {}))
                 for i,(title,spec,requirements,visible,hidden) in enumerate((
                     ('Graph maintenance',SPEC1,('B1-put','B1-remove','B1-order','B1-persist'),V1,H1),
                     ('Digest cache and invalidation',SPEC2,('B2-build','B2-invalidate','B2-durable','B2-ready'),V2,H2),
                     ('Atomic build plans and retries',SPEC3,('B3-plan','B3-commit','B3-retry','B3-audit'),V3,H3),
                     ('Upstream policy and portable manifests',SPEC4,('B4-policy','B4-import','B4-migrate','B4-integrate'),V4,H4)) )))

MUTANTS={}
for name,old,new in (
    ('direct-only-invalidation',"pending.extend([other for other,row in graph.items() if name in row['deps']] if reverse else graph[name]['deps'])", "pending.extend([other for other,row in graph.items() if name in row['deps']] if reverse and name in identifiers else ([] if reverse else graph[name]['deps']))"),
    ('priority-discards-cache',"content_changed = prior is None or prior['source'] != row['source'] or prior['deps'] != row['deps']", "content_changed = changed"),
    ('commit-replay-rechecks-state',"return old[1]", "request = None"),
    ('old-policy-quota',"len(records) > policy['max_import']", "len(records) > 4"),
):
    files=deepcopy(_files(4))
    path=ALLOWED[1] if name=='direct-only-invalidation' else ALLOWED[2]
    assert old in files[path],name
    files[path]=files[path].replace(old,new)
    MUTANTS[name]=files
PROJECT['mutants']=MUTANTS
