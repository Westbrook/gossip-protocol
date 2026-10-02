"""Fresh durable work-queue benchmark; oracle and solutions are host-only data.

Only initial_files, specifications and public cases may enter model requests.
The reference uses an independent in-memory transition model. Authored SQLite
implementations and semantic mutants require separate physical qualification.
"""
from __future__ import annotations

from copy import deepcopy
import json
import math
import re
from textwrap import dedent

CONTRACT = 'benchmark-job-queue-v1'
ALLOWED = ('queue_app/storage.py', 'queue_app/domain.py', 'queue_app/service.py')

ADAPTER = dedent('''\
    import json
    from pathlib import Path
    import subprocess
    import sys
    import tempfile
    def solve(payload):
        with tempfile.TemporaryDirectory(prefix='queue-', dir='/tmp') as directory:
            database = str(Path(directory) / 'queue.sqlite')
            answers = []
            for command in payload['commands']:
                result = subprocess.run([sys.executable, '-I', str(Path(__file__).parent / 'queue_app/cli.py'), database],
                                        input=json.dumps(command), text=True, capture_output=True, timeout=3)
                if result.returncode:
                    raise RuntimeError('queue CLI failed')
                answers.append(json.loads(result.stdout))
            return answers
''')
CLI = dedent('''\
    import json
    from pathlib import Path
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from queue_app import storage, service
    from queue_app.domain import CommandError
    connection = storage.connect(sys.argv[1])
    try:
        try:
            result = service.execute(connection, json.load(sys.stdin))
        except CommandError as error:
            result = {'error': error.code}
        print(json.dumps(result, allow_nan=False))
    finally:
        connection.close()
''')
STORAGE = dedent('''\
    import json
    import sqlite3
    COLUMNS = ('id','payload','priority','seq','status','attempts','token','worker','available_at','lease_until','max_attempts','retry_delay')
    def connect(path):
        db = sqlite3.connect(path, timeout=2)
        db.row_factory = sqlite3.Row
        with db:
            db.execute('CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, payload TEXT NOT NULL, priority INTEGER NOT NULL, seq INTEGER NOT NULL, status TEXT NOT NULL, attempts INTEGER NOT NULL, token INTEGER NOT NULL, worker TEXT, available_at INTEGER NOT NULL, lease_until INTEGER, max_attempts INTEGER NOT NULL, retry_delay INTEGER NOT NULL)')
            db.execute('CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL)')
            db.executemany('INSERT OR IGNORE INTO meta VALUES (?,?)', [('now',0),('sequence',0)])
            db.execute('CREATE TABLE IF NOT EXISTS receipts (key TEXT PRIMARY KEY, request TEXT NOT NULL, response TEXT NOT NULL)')
        return db
    def load(db):
        return ({row['id']: dict(row) for row in db.execute('SELECT * FROM jobs')},
                {row['key']: row['value'] for row in db.execute('SELECT * FROM meta')})
    def save(db, jobs, meta):
        db.execute('DELETE FROM jobs')
        db.executemany('INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
                       [tuple(row[key] for key in COLUMNS) for row in jobs.values()])
        db.executemany('UPDATE meta SET value=? WHERE key=?', [(value,key) for key,value in meta.items()])
    def receipt(db, key):
        row = db.execute('SELECT request,response FROM receipts WHERE key=?', (key,)).fetchone()
        return None if row is None else (row['request'], json.loads(row['response']))
    def remember(db, key, request, response):
        db.execute('INSERT INTO receipts VALUES (?,?,?)', (key,request,json.dumps(response,sort_keys=True)))
''')
DOMAIN = dedent('''\
    import json
    import re
    class CommandError(Exception):
        def __init__(self, code):
            super().__init__(code)
            self.code = code
    def fail(code):
        raise CommandError(code)
    def fields(value, required, optional=()):
        if type(value) is not dict or not set(required) <= value.keys() or set(value) - set(required) - set(optional):
            fail('invalid')
    def name(value):
        if type(value) is not str or re.fullmatch(r'[a-z][a-z0-9-]{0,15}', value) is None:
            fail('invalid')
        return value
    def number(value, low, high):
        if type(value) is not int or not low <= value <= high:
            fail('invalid')
        return value
    def record(value):
        fields(value, ('id','payload'), __RECORD_OPTIONS__)
        name(value['id'])
        if type(value['payload']) is not str or len(value['payload']) > 64 or any(not 32 <= ord(c) <= 126 for c in value['payload']):
            fail('invalid')
        return dict(id=value['id'], payload=value['payload'], priority=number(value.get('priority',0),-3,3),
                    delay=__DELAY__, max_attempts=__MAX_ATTEMPTS__, retry_delay=__RETRY_DELAY__)
    def parse(command):
        if type(command) is not dict or type(command.get('op')) is not str:
            fail('invalid')
        op = command['op']
        if op == 'snapshot':
            fields(command, ('op',))
            return {'op':op}
        if op == 'enqueue':
            value = record({key:value for key,value in command.items() if key != 'op'})
            return dict(value, op=op)
        if op == 'tick':
            fields(command, ('op','delta'))
            return dict(op=op, delta=number(command['delta'],0,1000))
        if op == 'claim':
            fields(command, ('op','worker'), __CLAIM_OPTIONS__)
            return dict(op=op, worker=name(command['worker']), lease=__LEASE__)
        if op == 'ack':
            fields(command, ('op','id','token'))
            return dict(op=op, id=name(command['id']), token=number(command['token'],1,1000000))
        __EXTRA_PARSE__
        fail('invalid')
    def canonical(value):
        return json.dumps(value, sort_keys=True, separators=(',',':'), ensure_ascii=True)
''')
LEASE_PARSE = '''if op in ('fail','renew'):
        fields(command, ('op','id','token','extend') if op == 'renew' else ('op','id','token'))
        parsed = dict(op=op, id=name(command['id']), token=number(command['token'],1,1000000))
        if op == 'renew':
            parsed['extend'] = number(command['extend'],1,100)
        return parsed'''
FINISH_PARSE = '''
    if op == 'finish':
        fields(command, ('op','key','id','token','successors'))
        name(command['key'])
        name(command['id'])
        number(command['token'],1,1000000)
        rows = command['successors']
        if type(rows) is not list or len(rows) > 4:
            fail('invalid')
        normalized = [record(row) for row in rows]
        if len({row['id'] for row in normalized}) != len(normalized):
            fail('invalid')
        return dict(op=op, key=command['key'], id=command['id'], token=command['token'], successors=normalized)
    if op == 'receipt':
        fields(command, ('op','key'))
        return dict(op=op, key=name(command['key']))'''
SERVICE = dedent('''\
    from . import storage
    from .domain import parse, fail, canonical
    def add(jobs, meta, value):
        meta['sequence'] += 1
        row = dict(id=value['id'],payload=value['payload'],priority=value['priority'],seq=meta['sequence'],status='ready',
                   attempts=0,token=0,worker=None,available_at=meta['now']+value['delay'],lease_until=None,
                   max_attempts=value['max_attempts'],retry_delay=value['retry_delay'])
        jobs[row['id']] = row
        return row
    def active(jobs, command):
        row = jobs.get(command['id'])
        if row is None:
            fail('missing')
        if row['status'] != 'running' or row['token'] != command['token']:
            fail('stale')
        return row
    __LEASE_HELPER__
    def execute(db, command):
        value = parse(command)
        op = value['op']
        with db:
            jobs, meta = storage.load(db)
            __PRE_REPLAY__
            if op == 'snapshot':
                return dict(now=meta['now'], jobs=[jobs[key] for key in sorted(jobs)])
            if op == 'enqueue':
                if value['id'] in jobs:
                    fail('exists')
                add(jobs,meta,value)
                answer = dict(ok='enqueued',id=value['id'])
            elif op == 'tick':
                meta['now'] += value['delta']
                expired = []
                __TICK_SWEEP__
                answer = dict(now=meta['now'],expired=sorted(expired))
            elif op == 'claim':
                ready = [row for row in jobs.values() if row['status'] == 'ready' and row['available_at'] <= meta['now']]
                ready.sort(key=lambda row: (-row['priority'],row['seq']))
                if not ready:
                    return {'job':None}
                row = ready[0]
                row['attempts'] += 1
                row['token'] += 1
                row['status'],row['worker'] = 'running',value['worker']
                row['lease_until'] = __LEASE_UNTIL__
                answer = {'job':dict(row)}
            elif op == 'ack':
                row = jobs.get(value['id'])
                if row is None:
                    fail('missing')
                if row['status'] == 'done' and row['token'] == value['token']:
                    return dict(ok='acked',id=row['id'])
                row = active(jobs,value)
                row['status'],row['worker'],row['lease_until'] = 'done',None,None
                answer = dict(ok='acked',id=row['id'])
            __EXTRA_EXECUTE__
            else:
                fail('invalid')
            storage.save(db,jobs,meta)
            __POST_RECEIPT__
            return answer
''')
LEASE_HELPER = '''def release(row, when):
    row['status'] = 'dead' if row['attempts'] >= row['max_attempts'] else 'ready'
    row['worker'],row['lease_until'] = None,None
    row['available_at'] = when + row['retry_delay']'''
TICK_SWEEP = '''for row in jobs.values():
                if row['status'] == 'running' and row['lease_until'] is not None and row['lease_until'] <= meta['now']:
                    expired.append(row['id'])
                    release(row,row['lease_until'])'''
LEASE_EXECUTE = '''elif op == 'fail':
            row = active(jobs,value)
            release(row,meta['now'])
            answer = dict(ok='failed',id=row['id'],status=row['status'],available_at=row['available_at'])
        elif op == 'renew':
            row = active(jobs,value)
            if row['lease_until'] is None:
                fail('lease')
            row['lease_until'] += value['extend']
            answer = dict(ok='renewed',id=row['id'],lease_until=row['lease_until'])'''
PRE_REPLAY = '''if op == 'receipt':
            previous = storage.receipt(db,value['key'])
            return {'found':False} if previous is None else dict(found=True,response=previous[1])
        if op == 'finish':
            request = canonical({key:item for key,item in value.items() if key != 'key'})
            previous = storage.receipt(db,value['key'])
            if previous is not None:
                if previous[0] != request:
                    fail('conflict')
                return previous[1]'''
FINISH_EXECUTE = '''
        elif op == 'finish':
            row = active(jobs,value)
            if any(child['id'] in jobs for child in value['successors']):
                fail('exists')
            row['status'],row['worker'],row['lease_until'] = 'done',None,None
            children = []
            for child in value['successors']:
                children.append(add(jobs,meta,child)['id'])
            answer = dict(ok='finished',id=row['id'],successors=children)'''
POST_RECEIPT = '''if op == 'finish':
            storage.remember(db,value['key'],request,answer)'''


def _files(stage):
    advanced = stage >= 0
    domain = DOMAIN.replace('__RECORD_OPTIONS__', "('priority','delay','max_attempts','retry_delay')" if advanced else "('priority',)")
    for marker, replacement in {
        '__DELAY__': "number(value.get('delay',0),0,100)" if advanced else '0',
        '__MAX_ATTEMPTS__': "number(value.get('max_attempts',3),1,5)" if advanced else '3',
        '__RETRY_DELAY__': "number(value.get('retry_delay',0),0,100)" if advanced else '0',
        '__CLAIM_OPTIONS__': "('lease',)" if advanced else '()',
        '__LEASE__': "number(command['lease'],1,100) if 'lease' in command else None" if advanced else 'None',
        '__EXTRA_PARSE__': (LEASE_PARSE + (FINISH_PARSE if stage == 1 else '')) if advanced else '',
    }.items():
        domain = domain.replace(marker, replacement)
    service = SERVICE
    for marker, replacement in {
        '__LEASE_HELPER__': LEASE_HELPER if advanced else '',
        '__PRE_REPLAY__': PRE_REPLAY if stage == 1 else '',
        '__TICK_SWEEP__': TICK_SWEEP if advanced else '',
        '__LEASE_UNTIL__': "None if value['lease'] is None else meta['now'] + value['lease']" if advanced else 'None',
        '__EXTRA_EXECUTE__': (LEASE_EXECUTE + (FINISH_EXECUTE if stage == 1 else '')) if advanced else '',
        '__POST_RECEIPT__': POST_RECEIPT if stage == 1 else '',
    }.items():
        service = service.replace(marker, replacement)
    storage = STORAGE
    if stage < 1:
        storage = storage.split('def receipt(db, key):', 1)[0]
        storage = storage.replace("        db.execute('CREATE TABLE IF NOT EXISTS receipts (key TEXT PRIMARY KEY, request TEXT NOT NULL, response TEXT NOT NULL)')\n", '')
    return {'solution.py': ADAPTER, 'queue_app/__init__.py': '', 'queue_app/cli.py': CLI,
            ALLOWED[0]: storage, ALLOWED[1]: domain, ALLOWED[2]: service}


BASE_SPEC = '''
You maintain a working durable SQLite job queue in three editable modules:
queue_app/storage.py, domain.py and service.py. solution.py and cli.py are fixed.
Every command starts a FRESH CLI process against the SAME scenario database.
No in-memory globals may substitute for persisted jobs, clock or receipts.
Preserve the complete already-working baseline below while adding each milestone.
The scenario envelope is exactly {commands:[command,...]}, with 1..24 sequential
commands. Inputs are finite JSON, nesting depth at most12 (envelope at depth0),
and at most24000 characters after Python json.dumps with ensure_ascii=True and
default separators. Future finish/receipt commands are outside the Q1 input
domain. Malformed individual commands otherwise produce error outputs. These are
sequential durable histories with process restarts, not concurrent-client tests.

Each command is an exact-field JSON object. Names (id/worker/key) match
[a-z][a-z0-9-]{0,15}. payload is ASCII printable string of length0..64. Integers
exclude bool; no coercion. Unknown fields/operations or malformed types -> invalid.
Validate complete syntax before any state lookup. Errors change nothing.
{op:"enqueue",id,payload,priority?} adds a unique durable job; priority int -3..3
defaults0. Any already-used id, including done/dead, -> exists. It assigns the
next global seq starting1. {op:"claim",worker} chooses a ready eligible job by
highest priority, then smallest ORIGINAL seq. None -> {job:null}. A successful
claim increments its attempts AND token once, sets running/worker, and returns
{job:fullRow}. Baseline claims have no expiration. {op:"ack",id,token} uses integer
token1..1000000: missing id -> missing; otherwise non-running or mismatched token
-> stale. A done job with the same token replays {ok:"acked",id}. Success sets
done,worker:null,lease_until:null, returning {ok:"acked",id}. No job is deleted.
{op:"tick",delta} advances a persistent logical clock by int0..1000, initially0;
return {now,expired:[]}. No real clock is consulted. {op:"snapshot"} returns
{now,jobs:[full rows sorted by id]}. Baseline enqueue returns {ok:"enqueued",id}.
Rows have exactly id,payload,priority,seq,status,attempts,token,worker,available_at,
lease_until,max_attempts,retry_delay. Newly enqueued rows: status ready, attempts0,
token0,worker null,available_at now,lease_until null,max_attempts3,retry_delay0.
Those settings are stored even in the baseline; ready eligibility is available_at
<= now. Snapshots never mutate state. Successful empty claims allocate no token
or seq. The baseline deliberately has no fail/renew/finish/receipt operations.
'''
SPEC_LEASE = '''
Milestone Q1: durable finite leases and retry scheduling, retaining all baseline
commands and outputs. enqueue additionally permits delay int0..100 default0,
max_attempts int1..5 default3, retry_delay int0..100 default0. New available_at is
now+delay. claim optionally takes lease int1..100: deadline now+lease; omitted
lease keeps the baseline permanent claim. Explicit null is invalid.
tick expires ALL running finite leases with deadline <= new now, in one atomic
command; expired IDs sorted in its response. Expiry clears worker/lease_until,
sets dead if attempts >= max_attempts, otherwise ready, and sets available_at to
OLD deadline+retry_delay (even for dead rows). A large jump uses the original
deadline, NOT the new now. It never claims a job automatically. Only tick advances
time or expires leases; delta0 is valid. Jobs retain their ORIGINAL sequence and
token when released. Each later successful claim increments attempts and token,
so a stale worker cannot ack/fail/renew the new lease even with the same name.
{op:"fail",id,token} validates active ownership exactly like ack (but done is stale),
then releases with available_at=now+retry_delay and the same max-attempts rule;
return {ok:"failed",id,status,available_at}. {op:"renew",id,token,extend} requires
extend int1..100. Missing precedes stale; active permanent lease -> lease error.
It adds extend to the EXISTING deadline, not now; returns
{ok:"renewed",id,lease_until}. Renewal never changes attempts/token/sequence.
All errors and wrong-token operations preserve every row and counter.
'''
SPEC_FINISH = '''
Milestone Q2: atomic durable completion and successor fan-out, preserving Q1 and
the baseline. {op:"finish",key,id,token,successors:[record,...]} has exact fields;
successors length0..4, each record uses enqueue fields WITHOUT op, with the same
optional defaults/limits. Duplicate successor ids -> invalid. Validate ALL fields
and all successors before receipts or job state. Normalize omitted defaults;
successor ARRAY ORDER is meaningful and is preserved in request identity.

All successful finish commands share a durable key namespace. After validation,
look up key BEFORE any parent/job/token/clock checks. Matching normalized request
(everything except key) returns the EXACT original response forever. Any other
valid request with an existing key -> conflict, even if id is now missing/stale.
For a fresh key: parent missing -> missing, then inactive/wrong token -> stale,
then ANY successor id already used anywhere (including parent/done/dead) -> exists.
Success atomically marks parent done and clears worker/lease_until, then enqueues
successors in ARRAY ORDER, giving each the next seq. Children have independent
attempts0/token0 and availability CURRENT now+their delay. Store receipt atomically
with every state/counter change. Return {ok:"finished",id,successors:[IDs in order]}.
An empty successor list is valid and still finishes/stores receipt. Any error
rolls back parent, children, seq AND receipt; a failed key can be used later.
Replay never allocates jobs/sequence, changes parent, recalculates delay, or
replaces a historical response when children are later claimed/completed.
{op:"receipt",key} returns {found:false} or {found:true,response:originalResponse}.
Ordinary ack remains usable on finish-completed parent with its original token.
'''


class _Failure(Exception):
    pass


def _bad(code='invalid'):
    raise _Failure(code)


def _fields(value, required, optional=()):
    if type(value) is not dict or not set(required).issubset(value) or set(value).difference(required).difference(optional):
        _bad()


def _name(value):
    if type(value) is not str or re.fullmatch(r'[a-z][a-z0-9-]{0,15}', value) is None:
        _bad()
    return value


def _number(value, low, high):
    if type(value) is not int or value < low or value > high:
        _bad()
    return value


def _record(value, stage):
    _fields(value, ('id', 'payload'), ('priority', 'delay', 'max_attempts', 'retry_delay') if stage >= 0 else ('priority',))
    _name(value['id'])
    text = value['payload']
    if type(text) is not str or len(text) > 64 or any(ord(char) < 32 or ord(char) > 126 for char in text):
        _bad()
    parsed = {'id': value['id'], 'payload': text}
    for field, low, high, default in (('priority', -3, 3, 0), ('delay', 0, 100, 0),
                                      ('max_attempts', 1, 5, 3), ('retry_delay', 0, 100, 0)):
        parsed[field] = _number(value.get(field, default), low, high)
    return parsed


def _parse(value, stage):
    if type(value) is not dict or type(value.get('op')) is not str:
        _bad()
    op = value['op']
    if op == 'enqueue':
        return dict(_record({key: item for key, item in value.items() if key != 'op'}, stage), op=op)
    allowed = {'snapshot': ('op',), 'tick': ('op', 'delta'), 'claim': ('op', 'worker'), 'ack': ('op', 'id', 'token')}
    if stage >= 0:
        allowed.update(fail=('op', 'id', 'token'), renew=('op', 'id', 'token', 'extend'))
    if stage == 1:
        allowed.update(finish=('op', 'key', 'id', 'token', 'successors'), receipt=('op', 'key'))
    if op not in allowed:
        _bad()
    _fields(value, allowed[op], ('lease',) if op == 'claim' and stage >= 0 else ())
    parsed = deepcopy(value)
    for field in ('id', 'worker', 'key'):
        if field in parsed:
            _name(parsed[field])
    for field, low, high in (('delta', 0, 1000), ('token', 1, 1000000), ('extend', 1, 100), ('lease', 1, 100)):
        if field in parsed:
            _number(parsed[field], low, high)
    if op == 'claim':
        parsed.setdefault('lease', None)
    if op == 'finish':
        rows = parsed['successors']
        if type(rows) is not list or len(rows) > 4:
            _bad()
        parsed['successors'] = [_record(row, stage) for row in rows]
        identifiers = [row['id'] for row in parsed['successors']]
        if len(identifiers) != len(set(identifiers)):
            _bad()
    return parsed


def _apply(state, command, stage):
    value = _parse(command, stage)
    op = value['op']
    records = state['records']
    by_id = {row['id']: row for row in records}
    if op == 'snapshot':
        return {'now': state['clock'], 'jobs': deepcopy(sorted(records, key=lambda row: row['id']))}
    if op == 'receipt':
        saved = state['history'].get(value['key'])
        return {'found': False} if saved is None else {'found': True, 'response': deepcopy(saved['answer'])}
    if op == 'finish' and value['key'] in state['history']:
        saved = state['history'][value['key']]
        identity = {key: item for key, item in value.items() if key != 'key'}
        if identity != saved['request']:
            _bad('conflict')
        return deepcopy(saved['answer'])

    def create(item):
        state['allocated'] += 1
        created = dict(id=item['id'], payload=item['payload'], priority=item['priority'], seq=state['allocated'],
                       status='ready', attempts=0, token=0, worker=None, available_at=state['clock'] + item['delay'],
                       lease_until=None, max_attempts=item['max_attempts'], retry_delay=item['retry_delay'])
        records.append(created)

    def owner():
        if value['id'] not in by_id:
            _bad('missing')
        item = by_id[value['id']]
        if item['status'] != 'running' or item['token'] != value['token']:
            _bad('stale')
        return item

    if op == 'enqueue':
        if value['id'] in by_id:
            _bad('exists')
        create(value)
        return {'ok': 'enqueued', 'id': value['id']}
    if op == 'tick':
        state['clock'] += value['delta']
        expired = []
        for item in records:
            deadline = item['lease_until']
            if item['status'] == 'running' and deadline is not None and state['clock'] >= deadline:
                expired.append(item['id'])
                item.update(status='ready' if item['attempts'] < item['max_attempts'] else 'dead',
                            worker=None, lease_until=None, available_at=deadline + item['retry_delay'])
        return {'now': state['clock'], 'expired': sorted(expired)}
    if op == 'claim':
        options = [row for row in records if row['status'] == 'ready' and state['clock'] >= row['available_at']]
        if not options:
            return {'job': None}
        best = min(options, key=lambda row: (-row['priority'], row['seq']))
        best['attempts'] += 1
        best['token'] += 1
        best.update(status='running', worker=value['worker'],
                    lease_until=None if value['lease'] is None else state['clock'] + value['lease'])
        return {'job': deepcopy(best)}
    if op == 'ack' and value['id'] in by_id:
        item = by_id[value['id']]
        if item['status'] == 'done' and item['token'] == value['token']:
            return {'ok': 'acked', 'id': item['id']}
    item = owner()
    if op == 'ack':
        item.update(status='done', worker=None, lease_until=None)
        return {'ok': 'acked', 'id': item['id']}
    if op == 'fail':
        item.update(status='ready' if item['attempts'] < item['max_attempts'] else 'dead',
                    worker=None, lease_until=None, available_at=state['clock'] + item['retry_delay'])
        return {'ok': 'failed', 'id': item['id'], 'status': item['status'], 'available_at': item['available_at']}
    if op == 'renew':
        if item['lease_until'] is None:
            _bad('lease')
        item['lease_until'] += value['extend']
        return {'ok': 'renewed', 'id': item['id'], 'lease_until': item['lease_until']}
    if op == 'finish':
        if any(child['id'] in by_id for child in value['successors']):
            _bad('exists')
        item.update(status='done', worker=None, lease_until=None)
        for child in value['successors']:
            create(child)
        answer = {'ok': 'finished', 'id': item['id'], 'successors': [child['id'] for child in value['successors']]}
        state['history'][value['key']] = {'request': deepcopy({key: value for key, value in value.items() if key != 'key'}),
                                          'answer': deepcopy(answer)}
        return answer
    _bad()


def _stage(stage):
    if type(stage) is not int or stage not in (0, 1):
        raise ValueError('outside_input_domain: stage must be 0 or 1')


def validate_input(stage, payload):
    _stage(stage)
    if type(payload) is not dict or set(payload) != {'commands'} or type(payload['commands']) is not list or not 1 <= len(payload['commands']) <= 24:
        raise ValueError('outside_input_domain: commands must have length 1..24')
    def valid_json(value, depth=0):
        if depth > 12:
            return False
        if value is None or type(value) in (str, bool, int):
            return True
        if type(value) is float:
            return math.isfinite(value)
        if type(value) is list:
            return all(valid_json(item, depth + 1) for item in value)
        if type(value) is dict:
            return all(type(key) is str and valid_json(item, depth + 1) for key, item in value.items())
        return False
    if not valid_json(payload) or len(json.dumps(payload, ensure_ascii=True)) > 24000:
        raise ValueError('outside_input_domain: bounded JSON required')
    if stage == 0 and any(type(command) is dict and command.get('op') in ('finish', 'receipt') for command in payload['commands']):
        raise ValueError('outside_input_domain: future operation')


def reference(stage, payload):
    validate_input(stage, payload)
    state = dict(records=[], clock=0, allocated=0, history={})
    answers = []
    for command in payload['commands']:
        trial = deepcopy(state)
        try:
            answer = _apply(trial, command, stage)
        except _Failure as error:
            answer = {'error': str(error)}
        else:
            state = trial
        answers.append(answer)
    return answers


def known_files(stage):
    _stage(stage)
    return _files(stage)


def enqueue(identifier, payload='work', **options):
    return dict(op='enqueue', id=identifier, payload=payload, **options)


def claim(worker='worker', **options):
    return dict(op='claim', worker=worker, **options)


def ack(identifier='a', token=1):
    return dict(op='ack', id=identifier, token=token)


def tick(delta):
    return dict(op='tick', delta=delta)


def fail(identifier='a', token=1):
    return dict(op='fail', id=identifier, token=token)


def renew(identifier='a', token=1, extend=1):
    return dict(op='renew', id=identifier, token=token, extend=extend)


def child(identifier, payload='work', **options):
    return dict(id=identifier, payload=payload, **options)


def finish(key='finish', identifier='a', token=1, *successors):
    return dict(op='finish', key=key, id=identifier, token=token, successors=list(successors))


def receipt(key='finish'):
    return dict(op='receipt', key=key)


SNAP = {'op': 'snapshot'}


def _case(stage, identifier, requirement, commands):
    payload = {'commands': commands}
    return dict(id=f'queue-s{stage + 1}-{identifier}', requirement=requirement, input=payload, expected=reference(stage, payload))


BASE_PUBLIC_ROWS = (
    ('b-basic', 'Q0-lifecycle', [enqueue('a'), claim(), ack(), SNAP]),
    ('b-order', 'Q0-ordering', [enqueue('a', priority=-1), enqueue('b', priority=1), claim(), SNAP]),
    ('b-error', 'Q0-validation', [enqueue('a', payload=False), {'op': 'claim', 'worker': []}, ack('missing'), SNAP]),
    ('b-clock', 'Q0-persistence', [tick(2), enqueue('a'), tick(3), claim(), SNAP]),
)
BASE_PRIVATE_ROWS = (
    ('h-b-replay', 'Q0-lifecycle', [enqueue('a'), claim(), ack(), ack(), enqueue('a'), claim(), SNAP]),
    ('h-b-fifo', 'Q0-ordering', [enqueue('z'), enqueue('a'), enqueue('m', priority=1), claim(), claim(), claim(), SNAP]),
    ('h-b-types', 'Q0-validation', [None, [], {'op': []}, {'op': {}}, enqueue('a', priority=True), tick(True), {'op': 'snapshot', 'x': 1}, ack(token=False), SNAP]),
    ('h-b-counter', 'Q0-persistence', [claim(), enqueue('a'), enqueue('a'), enqueue('b'), ack('a'), claim(), SNAP]),
    ('h-b-wrong-token', 'Q0-lifecycle', [enqueue('a'), claim(), ack(token=2), SNAP, ack(), SNAP]),
    ('h-b-bounds', 'Q0-validation', [enqueue('a', payload=''), enqueue('b', payload='x' * 64, priority=3), enqueue('c', payload='x' * 65), enqueue('d', payload='\n'), enqueue('x' * 17), SNAP]),
)
LEASE_PUBLIC_ROWS = (
    ('lease-basic', 'Q1-expiration', [enqueue('a'), claim(lease=2), tick(3), SNAP]),
    ('lease-delayed', 'Q1-scheduling', [enqueue('a', delay=2), claim(), tick(3), claim(), SNAP]),
    ('lease-fence', 'Q1-fencing', [enqueue('a'), claim(lease=2), ack(token=9), SNAP]),
    ('lease-fail', 'Q1-retries', [enqueue('a'), claim(lease=4), fail(), SNAP]),
    ('lease-renew', 'Q1-renewal', [enqueue('a'), claim(lease=4), renew(extend=2), SNAP]),
    ('lease-type', 'Q1-validation', [enqueue('a', max_attempts=0), claim(lease=None), renew(extend=False), SNAP]),
)
LEASE_PRIVATE_ROWS = (
    ('deadline-equality', 'Q1-expiration', [enqueue('a'), claim(lease=3), tick(3), SNAP]),
    ('large-jump', 'Q1-scheduling', [enqueue('a', retry_delay=4), claim(lease=2), tick(10), claim(), SNAP]),
    ('delayed-priority-head', 'Q1-scheduling', [enqueue('urgent', priority=3, delay=5), enqueue('ready', priority=-1), claim(), SNAP]),
    ('reclaim-fence', 'Q1-fencing', [enqueue('a'), claim(lease=2), tick(3), claim(lease=5), ack(token=1), fail(token=1), renew(token=1), SNAP]),
    ('attempt-exhaustion', 'Q1-retries', [enqueue('a', max_attempts=1), claim(lease=2), tick(4), claim(), SNAP]),
    ('renew-from-deadline', 'Q1-renewal', [enqueue('a'), claim(lease=8), tick(3), renew(extend=4), tick(6), SNAP]),
    ('strict-booleans', 'Q1-validation', [enqueue('a', retry_delay=True), enqueue('a', delay=False), claim(lease=True), enqueue('a'), claim(lease=3), renew(token=True), SNAP]),
    ('retry-fifo', 'Q1-scheduling', [enqueue('z'), enqueue('a'), claim(lease=2), tick(4), claim(), SNAP]),
    ('exact-due', 'Q1-scheduling', [enqueue('a', delay=3), tick(3), claim(), SNAP]),
    ('delay-after-clock', 'Q1-scheduling', [tick(5), enqueue('a', delay=3), claim(), tick(3), claim(), SNAP]),
    ('fail-dead', 'Q1-retries', [enqueue('a', max_attempts=2, retry_delay=1), claim(), fail(), tick(1), claim(), fail(token=2), tick(100), claim(), SNAP]),
    ('dead-id-retained', 'Q1-retries', [enqueue('a', max_attempts=1), claim(lease=1), tick(1), enqueue('a', payload='new'), SNAP]),
    ('multi-expire', 'Q1-expiration', [enqueue('z'), enqueue('a'), claim(lease=3), claim(lease=2), tick(4), SNAP]),
    ('permanent-lease', 'Q1-renewal', [enqueue('a'), claim(), renew(), tick(1000), ack(), SNAP]),
    ('wrong-owner-rollback', 'Q1-fencing', [enqueue('a'), claim(lease=4), fail(token=2), renew(token=2), tick(2), SNAP]),
)
FINISH_PUBLIC_ROWS = (
    ('finish-basic', 'Q2-fanout', [enqueue('a'), claim(lease=5), finish('f', 'a', 1, child('b')), receipt('f'), SNAP]),
    ('finish-collision', 'Q2-atomicity', [enqueue('a'), enqueue('b'), claim(), finish('f', 'a', 1, child('b')), SNAP]),
    ('finish-types', 'Q2-validation', [finish('f', 'a', True), finish('f', 'a', 1, child('b'), child('b')), SNAP]),
    ('finish-missing', 'Q2-ownership', [finish('f', 'absent', 1), receipt('f'), SNAP]),
    ('finish-defaults', 'Q2-identity', [enqueue('a'), claim(), finish('f', 'a', 1, child('b', priority=0)), SNAP]),
    ('finish-history', 'Q2-history', [enqueue('a'), claim(), finish('f'), receipt('f'), ack(), SNAP]),
)
FINISH_PRIVATE_ROWS = (
    ('replay-after-change', 'Q2-history', [enqueue('a'), claim(lease=2), finish('f', 'a', 1, child('b', delay=2)), tick(4), claim(), ack('b'), finish('f', 'a', 1, child('b', delay=2)), receipt('f'), SNAP]),
    ('defaults-replay', 'Q2-identity', [enqueue('a'), claim(), finish('f', 'a', 1, child('b')), finish('f', 'a', 1, child('b', priority=0, delay=0, max_attempts=3, retry_delay=0)), SNAP]),
    ('array-order-conflict', 'Q2-identity', [enqueue('a'), claim(), finish('f', 'a', 1, child('z'), child('b')), finish('f', 'a', 1, child('b'), child('z')), SNAP]),
    ('fanout-fifo', 'Q2-fanout', [enqueue('a'), claim(), finish('f', 'a', 1, child('z'), child('b')), claim(), claim(), SNAP]),
    ('relative-delay', 'Q2-fanout', [enqueue('a'), claim(lease=10), tick(3), finish('f', 'a', 1, child('b', delay=2)), claim(), tick(2), claim(), SNAP]),
    ('late-collision', 'Q2-atomicity', [enqueue('a'), enqueue('used'), claim(), finish('f', 'a', 1, child('fresh'), child('used')), receipt('f'), SNAP, finish('f', 'a', 1, child('fresh')), SNAP]),
    ('completed-id-collision', 'Q2-atomicity', [enqueue('old'), claim(), ack('old'), enqueue('a'), claim(), finish('f', 'a', 1, child('old')), SNAP]),
    ('expired-owner', 'Q2-ownership', [enqueue('a'), claim(lease=2), tick(2), claim(lease=3), finish('f', 'a', 1, child('b')), SNAP, finish('f', 'a', 2, child('b')), SNAP]),
    ('validate-before-replay', 'Q2-validation', [enqueue('a'), claim(), finish('f'), dict(finish('f'), successors=[child('bad', delay=True)]), receipt('f'), SNAP]),
    ('conflict-before-state', 'Q2-identity', [enqueue('a'), claim(), finish('f'), finish('f', 'missing', 1), SNAP]),
    ('multiple-receipts', 'Q2-history', [enqueue('a'), claim(), finish('f', 'a', 1, child('b')), claim(), finish('g', 'b', 1), receipt('f'), receipt('g'), SNAP]),
    ('empty-finish-replay', 'Q2-history', [enqueue('a'), claim(lease=2), finish('f'), tick(3), finish('f'), SNAP]),
    ('whole-request-validation', 'Q2-validation', [finish('f', 'missing', 1, child('b'), child('c', max_attempts=False)), SNAP]),
    ('parent-collision', 'Q2-atomicity', [enqueue('a'), claim(), finish('f', 'a', 1, child('a')), SNAP]),
    ('read-before-use', 'Q2-history', [receipt('f'), enqueue('a'), claim(), finish('f'), receipt('f'), SNAP]),
)


def _cases(stage, rows):
    return tuple(_case(stage, *row) for row in rows)


BASE_PUBLIC = _cases(0, BASE_PUBLIC_ROWS)
BASE_PRIVATE = _cases(0, BASE_PRIVATE_ROWS)
STAGES = (
    dict(id='queue-lease', title='Leased retries and stale-worker fencing', specification=BASE_SPEC + SPEC_LEASE,
         requirements=tuple(f'Q0-{value}' for value in ('lifecycle', 'ordering', 'validation', 'persistence')) +
                      tuple(f'Q1-{value}' for value in ('expiration', 'scheduling', 'fencing', 'retries', 'renewal', 'validation')),
         visible_cases=BASE_PUBLIC + _cases(0, LEASE_PUBLIC_ROWS), hidden_cases=BASE_PRIVATE + _cases(0, LEASE_PRIVATE_ROWS), known_files=known_files(0)),
    dict(id='queue-fanout', title='Atomic completion and durable successor fan-out', specification=SPEC_FINISH,
         requirements=tuple(f'Q2-{value}' for value in ('fanout', 'atomicity', 'validation', 'ownership', 'identity', 'history')),
         visible_cases=_cases(1, FINISH_PUBLIC_ROWS), hidden_cases=_cases(1, FINISH_PRIVATE_ROWS), known_files=known_files(1)),
)
INITIAL = _files(-1)
PROJECT = dict(id='job-queue', title='Durable Worker Queue', contract=CONTRACT,
               initial_files=INITIAL, allowed_paths=ALLOWED, stages=STAGES)


def control_files(stage):
    correct = known_files(stage)
    alternative = deepcopy(correct)
    alternative[ALLOWED[2]] = alternative[ALLOWED[2]].replace("ready.sort(key=lambda row: (-row['priority'],row['seq']))",
                                                           "ready = sorted(ready, key=lambda row: (-row['priority'],row['seq']))")
    return {'correct': correct, 'equivalent-sort': alternative}


def correct_controls(stage):
    return [dict(id=identity, files=files) for identity, files in control_files(stage).items()]


def rehearsal_probe(stage, slot_index=0):
    _stage(stage)
    if type(slot_index) is not int or not 0 <= slot_index <= 9:
        raise ValueError('invalid probe slot')
    identifier = f'probe-{slot_index}'
    commands = [enqueue(identifier, retry_delay=2), claim(lease=3), tick(4), tick(1), claim(lease=5)]
    commands += ([renew(identifier, 2, 3), SNAP] if stage == 0 else
                 [finish(f'key-{slot_index}', identifier, 2, child(f'child-{slot_index}')), receipt(f'key-{slot_index}'), SNAP])
    return {'commands': commands}


def _variant(stage, path, before, after):
    files = known_files(stage)
    if files[path].count(before) != 1:
        raise AssertionError(f'Fault mutation needs one source site: {before!r}')
    files[path] = files[path].replace(before, after)
    return files


def fault_bank(stage):
    """Intended public-surviving faults; physical qualification is separate."""
    _stage(stage)
    service, domain = ALLOWED[2], ALLOWED[1]
    if stage == 0:
        faults = [
            ('late-expiry', 'lease-boundary', service, "row['lease_until'] <= meta['now']", "row['lease_until'] < meta['now']", 'deadline-equality'),
            ('delayed-head', 'ready-priority-filter', service, "ready = [row for row in jobs.values() if row['status'] == 'ready' and row['available_at'] <= meta['now']]", "ready = sorted([row for row in jobs.values() if row['status'] == 'ready'],key=lambda row:(-row['priority'],row['seq']))\n            ready = ready[:1] if ready and ready[0]['available_at'] <= meta['now'] else []", 'delayed-priority-head'),
            ('reset-fence', 'reclaim-token-fencing', service, "row['token'] += 1", "row['token'] = 1", 'reclaim-fence'),
            ('extra-attempt', 'attempt-exhaustion', service, "row['attempts'] >= row['max_attempts']", "row['attempts'] > row['max_attempts']", 'attempt-exhaustion'),
            ('boolean-integers', 'strict-numeric-types', domain, "type(value) is not int", "not isinstance(value,int)", 'strict-booleans'),
            ('id-tiebreak', 'retry-fifo-order', service, "(-row['priority'],row['seq'])", "(-row['priority'],row['id'])", 'retry-fifo'),
        ]
    else:
        faults = [
            ('state-before-replay', 'durable-replay-precedence', service, "            previous = storage.receipt(db,value['key'])\n            if previous is not None:", "            active(jobs,value)\n            previous = storage.receipt(db,value['key'])\n            if previous is not None:", 'replay-after-change'),
            ('partial-fanout-commit', 'atomic-fanout-rollback', service,
             "            if any(child['id'] in jobs for child in value['successors']):\n                fail('exists')\n            row['status'],row['worker'],row['lease_until'] = 'done',None,None\n            children = []\n            for child in value['successors']:\n                children.append(add(jobs,meta,child)['id'])",
             "            children = []\n            for child in value['successors']:\n                if child['id'] in jobs:\n                    fail('exists')\n                children.append(add(jobs,meta,child)['id'])\n                storage.save(db,jobs,meta)\n                db.commit()\n            row['status'],row['worker'],row['lease_until'] = 'done',None,None", 'late-collision'),
            ('unordered-key', 'successor-array-identity', service, "request = canonical({key:item for key,item in value.items() if key != 'key'})", "request = canonical(dict({key:item for key,item in value.items() if key != 'key'}, successors=sorted(value['successors'],key=lambda row:row['id'])))", 'array-order-conflict'),
            ('sorted-fanout', 'successor-sequence-order', service, "for child in value['successors']:\n                children.append", "for child in sorted(value['successors'],key=lambda row:row['id']):\n                children.append", 'fanout-fifo'),
            ('absolute-delay', 'successor-clock-origin', service, "children.append(add(jobs,meta,child)['id'])", "created = add(jobs,meta,child)\n                created['available_at'] = child['delay']\n                children.append(created['id'])", 'relative-delay'),
            ('discard-receipts', 'historical-receipt-retention', service, "storage.remember(db,value['key'],request,answer)", "db.execute('DELETE FROM receipts')\n            storage.remember(db,value['key'],request,answer)", 'multiple-receipts'),
        ]
    private = {case['id'].split(f'queue-s{stage + 1}-', 1)[-1]: case for case in STAGES[stage]['hidden_cases']}
    return [dict(id=f'queue-s{stage + 1}-{identifier}', family=family,
                 files=_variant(stage, path, before, after), witness_cases=[deepcopy(private[witness])])
            for identifier, family, path, before, after, witness in faults]
