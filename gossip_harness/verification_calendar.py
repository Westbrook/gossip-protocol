"""Frozen maintenance-project fixture: durable resource-booking calendar.

Only initial_files, current cumulative specifications, and visible cases belong
in model requests. reference() and known_files are trusted host-only fixtures.
Stage indices are zero based. Generated probe inputs require validate_input().
"""
from __future__ import annotations
from copy import deepcopy
import json
import math
from textwrap import dedent


def src(text):
    return dedent(text).lstrip()


INITIAL_POLICY = '{"max_duration":50,"max_active_per_user":8}\n'
FINAL_POLICY = '{"max_duration":12,"max_active_per_user":2}\n'
ADAPTER = src('''
    import contextlib
    import json
    from pathlib import Path
    import sqlite3
    import subprocess
    import sys
    import tempfile

    def solve(payload):
        with tempfile.TemporaryDirectory(prefix="calendar-") as directory:
            database = str(Path(directory) / "calendar.sqlite")
            if "legacy" in payload:
                with contextlib.closing(sqlite3.connect(database)) as db:
                    db.execute("CREATE TABLE bookings(rid TEXT PRIMARY KEY,resource TEXT NOT NULL,user TEXT NOT NULL,start INTEGER NOT NULL,end INTEGER NOT NULL,status TEXT NOT NULL)")
                    for item in payload["legacy"]:
                        db.execute("INSERT INTO bookings VALUES (?,?,?,?,?,'booked')", tuple(item[k] for k in ("rid","resource","user","start","end")))
                    db.execute("PRAGMA user_version=1")
                    db.commit()
            answers = []
            bridge = Path(__file__).parent / "calendar_app" / "cli.py"
            for command in payload["commands"]:
                result = subprocess.run([sys.executable, str(bridge), database, json.dumps(command)], capture_output=True, text=True, timeout=4)
                if result.returncode:
                    raise RuntimeError("Calendar CLI exited unsuccessfully")
                answers.append(json.loads(result.stdout))
            return answers
''')
CLI = src('''
    import json
    import sys
    from service import execute
    if __name__ == "__main__":
        try:
            answer = execute(sys.argv[1], json.loads(sys.argv[2]))
        except Exception:
            answer = {"error":"internal"}
        print(json.dumps(answer,sort_keys=True,separators=(",",":")))
''')
STORAGE = src('''
    """SQLite schema, durable rows, and resource queries; no business decisions."""
    import sqlite3

    def connect(path):
        db = sqlite3.connect(path)
        db.row_factory = sqlite3.Row
        db.execute("BEGIN IMMEDIATE")
        db.execute("CREATE TABLE IF NOT EXISTS bookings(rid TEXT PRIMARY KEY,resource TEXT NOT NULL,user TEXT NOT NULL,start INTEGER NOT NULL,end INTEGER NOT NULL,status TEXT NOT NULL)")
        columns = {row[1] for row in db.execute("PRAGMA table_info(bookings)")}
        if "token" not in columns:
            db.execute("ALTER TABLE bookings ADD COLUMN token INTEGER NOT NULL DEFAULT 0")
        if "expires" not in columns:
            db.execute("ALTER TABLE bookings ADD COLUMN expires INTEGER")
        db.execute("CREATE TABLE IF NOT EXISTS requests(key TEXT PRIMARY KEY,body TEXT NOT NULL,response TEXT NOT NULL)")
        db.execute("CREATE TABLE IF NOT EXISTS events(seq INTEGER PRIMARY KEY AUTOINCREMENT,key TEXT NOT NULL,op TEXT NOT NULL)")
        db.execute("CREATE TABLE IF NOT EXISTS waiting(seq INTEGER PRIMARY KEY AUTOINCREMENT,rid TEXT UNIQUE NOT NULL,resource TEXT NOT NULL,user TEXT NOT NULL,start INTEGER NOT NULL,end INTEGER NOT NULL,status TEXT NOT NULL)")
        db.execute("PRAGMA user_version=2")
        db.commit()
        return db

    def booking(db, rid):
        row = db.execute("SELECT * FROM bookings WHERE rid=?", (rid,)).fetchone()
        return dict(row) if row else None

    def queued(db, rid):
        row = db.execute("SELECT * FROM waiting WHERE rid=?", (rid,)).fetchone()
        return dict(row) if row else None

    def taken(db, resource, start, end, exclude=None):
        rows = db.execute("SELECT rid,start,end FROM bookings WHERE resource=? AND status IN ('booked','held')", (resource,))
        return any(row["rid"] != exclude and start < row["end"] and row["start"] < end for row in rows)

    def active_count(db, user, exclude=None):
        return sum(row[0] != exclude for row in db.execute("SELECT rid FROM bookings WHERE user=? AND status IN ('booked','held')", (user,)))

    def insert_booking(db, item, status="booked", token=0, expires=None):
        db.execute("INSERT INTO bookings VALUES (?,?,?,?,?,?,?,?)", (item["rid"],item["resource"],item["user"],item["start"],item["end"],status,token,expires))

    def snapshot(db):
        return {
            "version": db.execute("PRAGMA user_version").fetchone()[0],
            "bookings": [dict(row) for row in db.execute("SELECT rid,resource,user,start,end,status,token,expires FROM bookings ORDER BY rid")],
            "waiting": [dict(row) for row in db.execute("SELECT seq,rid,resource,user,start,end,status FROM waiting ORDER BY seq")],
        }

    def audit(db):
        return [dict(row) for row in db.execute("SELECT seq,key,op FROM events ORDER BY seq")]
''')
DOMAIN = src('''
    """Exact command shapes and scalar types, independent of database state."""
    class DomainError(Exception):
        pass

    def require(condition, code="invalid"):
        if not condition:
            raise DomainError(code)

    def name(value):
        require(type(value) is str and 1 <= len(value) <= 32)

    def integer(value, minimum=0):
        require(type(value) is int and minimum <= value <= 1000)

    ITEM = {"rid", "resource", "user", "start", "end"}
    FIELDS = {
        "book": ITEM, "cancel": {"rid"},
        "move": {"rid", "start", "end"}, "bundle": {"items"},
        "hold": ITEM | {"now", "ttl"}, "renew": {"rid", "token", "now", "ttl"},
        "confirm": {"rid", "token", "now"}, "expire": {"now"}, "batch": {"commands"},
        "enqueue": ITEM, "admit": set(),
    }
    LEVEL = {"book":0,"cancel":0,"move":1,"bundle":1,"hold":2,"renew":2,"confirm":2,"expire":2,"batch":2,"enqueue":3,"admit":3}

    def validate(command, stage, inner=False):
        require(type(command) is dict and type(command.get("op")) is str)
        op = command["op"]
        require(op in FIELDS and LEVEL[op] <= stage)
        require(set(command) == FIELDS[op] | {"op"} | (set() if inner else {"key"}))
        if not inner:
            name(command["key"])
        for field in ("rid", "resource", "user"):
            if field in command:
                name(command[field])
        for field in ("start", "end", "now"):
            if field in command:
                integer(command[field])
        if "start" in command:
            require(command["start"] < command["end"])
        if "token" in command:
            integer(command["token"], 1)
        if "ttl" in command:
            integer(command["ttl"], 1)
            require(command["now"] + command["ttl"] <= 1000)
        if op == "bundle":
            require(type(command["items"]) is list and 1 <= len(command["items"]) <= 4)
            for item in command["items"]:
                require(type(item) is dict and set(item) == ITEM)
                validate(dict(item, op="book"), stage, True)
        if op == "batch":
            require(type(command["commands"]) is list and 1 <= len(command["commands"]) <= 4)
            for item in command["commands"]:
                require(type(item) is dict and item.get("op") != "batch")
                validate(item, stage, True)
''')
SERVICE = src('''
    """Atomic calendar mutations and idempotent command execution."""
    import json
    from pathlib import Path
    from domain import DomainError, require, validate
    from storage import connect, booking, queued, taken, active_count, insert_booking, snapshot, audit

    STAGE = 3

    def policy():
        return json.loads((Path(__file__).parent.parent / "policy.json").read_text())

    def allowed_by_policy(db, item, exclude=None):
        if STAGE < 3:
            return True
        limits = policy()
        return (item["end"] - item["start"] <= limits["max_duration"]
                and active_count(db, item["user"], exclude) < limits["max_active_per_user"])

    def new_booking(db, item, status="booked", token=0, expires=None, from_waiting=False):
        require(booking(db, item["rid"]) is None and (from_waiting or queued(db, item["rid"]) is None), "duplicate")
        require(allowed_by_policy(db, item), "policy")
        require(not taken(db, item["resource"], item["start"], item["end"]), "conflict")
        insert_booking(db, item, status, token, expires)

    def active(db, rid):
        row = booking(db, rid)
        require(row is not None, "missing")
        require(row["status"] in ("booked", "held"), "closed")
        return row

    def fenced_hold(db, command):
        row = booking(db, command["rid"])
        require(row is not None, "missing")
        require(row["token"] == command["token"], "stale")
        require(row["status"] == "held", "closed")
        require(command["now"] < row["expires"], "expired")
        return row

    def apply(db, command):
        op = command["op"]
        if op == "book":
            new_booking(db, command)
        elif op == "cancel":
            row = booking(db, command["rid"])
            if row is not None:
                active(db, command["rid"])
                db.execute("UPDATE bookings SET status='cancelled' WHERE rid=?", (command["rid"],))
            else:
                wait = queued(db, command["rid"])
                require(STAGE >= 3 and wait is not None, "missing")
                require(wait["status"] == "waiting", "closed")
                db.execute("UPDATE waiting SET status='cancelled' WHERE rid=?", (command["rid"],))
        elif op == "move":
            row = active(db, command["rid"])
            require(row["status"] == "booked", "held")
            revised = dict(row, start=command["start"], end=command["end"])
            require(allowed_by_policy(db, revised, row["rid"]), "policy")
            require(not taken(db, row["resource"], revised["start"], revised["end"], row["rid"]), "conflict")
            db.execute("UPDATE bookings SET start=?,end=? WHERE rid=?", (revised["start"],revised["end"],row["rid"]))
        elif op == "bundle":
            for item in command["items"]:
                new_booking(db, item)
        elif op == "hold":
            new_booking(db, command, "held", 1, command["now"] + command["ttl"])
            return {"ok": True, "token": 1}
        elif op == "renew":
            row = fenced_hold(db, command)
            token = row["token"] + 1
            db.execute("UPDATE bookings SET token=?,expires=? WHERE rid=?", (token,command["now"]+command["ttl"],row["rid"]))
            return {"ok": True, "token": token}
        elif op == "confirm":
            row = fenced_hold(db, command)
            db.execute("UPDATE bookings SET status='booked',expires=NULL WHERE rid=?", (row["rid"],))
        elif op == "expire":
            db.execute("UPDATE bookings SET status='expired' WHERE status='held' AND expires<=?", (command["now"],))
        elif op == "batch":
            answers = [apply(db, item) for item in command["commands"]]
            return {"ok": True, "results": answers}
        elif op == "enqueue":
            require(booking(db,command["rid"]) is None and queued(db,command["rid"]) is None, "duplicate")
            require(command["end"] - command["start"] <= policy()["max_duration"], "policy")
            db.execute("INSERT INTO waiting(rid,resource,user,start,end,status) VALUES (?,?,?,?,?,'waiting')", tuple(command[k] for k in ("rid","resource","user","start","end")))
        elif op == "admit":
            for row in db.execute("SELECT * FROM waiting WHERE status='waiting' ORDER BY seq").fetchall():
                item = dict(row)
                if allowed_by_policy(db,item) and not taken(db,item["resource"],item["start"],item["end"]):
                    new_booking(db,item,from_waiting=True)
                    db.execute("UPDATE waiting SET status='admitted' WHERE rid=?", (item["rid"],))
                    return {"ok": True, "admitted": item["rid"]}
            return {"ok": True, "admitted": None}
        return {"ok": True}

    def execute(path, command):
        db = connect(path)
        try:
            if command == {"op":"snapshot"}:
                return snapshot(db)
            if STAGE >= 2 and command == {"op":"audit"}:
                return audit(db)
            validate(command, STAGE)
            key = command["key"]
            body = json.dumps({k:v for k,v in command.items() if k != "key"},sort_keys=True,separators=(",",":"))
            with db:
                previous = db.execute("SELECT body,response FROM requests WHERE key=?",(key,)).fetchone()
                if previous:
                    require(previous["body"] == body, "key_conflict")
                    return json.loads(previous["response"])
                response = apply(db,command)
                db.execute("INSERT INTO requests VALUES (?,?,?)",(key,body,json.dumps(response)))
                db.execute("INSERT INTO events(key,op) VALUES (?,?)",(key,command["op"]))
                return response
        except DomainError as error:
            return {"error":str(error)}
        finally:
            db.close()
''')

V0_STORAGE = src('''
    import sqlite3
    def connect(path):
        db=sqlite3.connect(path)
        db.row_factory=sqlite3.Row
        db.execute("CREATE TABLE IF NOT EXISTS bookings(rid TEXT PRIMARY KEY,resource TEXT NOT NULL,user TEXT NOT NULL,start INTEGER NOT NULL,end INTEGER NOT NULL,status TEXT NOT NULL,token INTEGER NOT NULL DEFAULT 0,expires INTEGER)")
        db.execute("CREATE TABLE IF NOT EXISTS requests(key TEXT PRIMARY KEY,body TEXT NOT NULL,response TEXT NOT NULL)")
        db.execute("PRAGMA user_version=2")
        db.commit()
        return db
    def snapshot(db):
        return {"version":2,"bookings":[dict(row) for row in db.execute("SELECT * FROM bookings ORDER BY rid")],"waiting":[]}
''')
V0_DOMAIN = src('''
    class DomainError(Exception):
        pass
    def require(condition,code="invalid"):
        if not condition:
            raise DomainError(code)
    def validate(c):
        require(type(c) is dict and c.get("op") in ("book","cancel"))
        fields={"op","key","rid"} | ({"resource","user","start","end"} if c["op"]=="book" else set())
        require(set(c)==fields)
        for key in ("key","rid","resource","user"):
            if key in c:
                require(type(c[key]) is str and 1<=len(c[key])<=32)
        if c["op"]=="book":
            require(type(c["start"]) is int and type(c["end"]) is int and 0<=c["start"]<c["end"]<=1000)
''')
V0_SERVICE = src('''
    import json
    from domain import DomainError,require,validate
    from storage import connect,snapshot
    def execute(path,c):
        db=connect(path)
        try:
            if c=={"op":"snapshot"}:
                return snapshot(db)
            validate(c)
            with db:
                old=db.execute("SELECT response FROM requests WHERE key=?",(c["key"],)).fetchone()
                if old:
                    return json.loads(old[0])
                row=db.execute("SELECT * FROM bookings WHERE rid=?",(c["rid"],)).fetchone()
                if c["op"]=="book":
                    require(row is None,"duplicate")
                    # Existing defect: closed interval comparison rejects adjacency.
                    conflict=db.execute("SELECT 1 FROM bookings WHERE resource=? AND status='booked' AND start<=? AND end>=?",(c["resource"],c["end"],c["start"])).fetchone()
                    require(conflict is None,"conflict")
                    db.execute("INSERT INTO bookings VALUES (?,?,?,?,?,'booked',0,NULL)",tuple(c[k] for k in ("rid","resource","user","start","end")))
                else:
                    require(row is not None,"missing")
                    require(row["status"]=="booked","closed")
                    db.execute("UPDATE bookings SET status='cancelled' WHERE rid=?",(c["rid"],))
                db.execute("INSERT INTO requests VALUES (?,?,?)",(c["key"],"",json.dumps({"ok":True})))
                return {"ok":True}
        except DomainError as e:
            return {"error":str(e)}
        finally:
            db.close()
''')
ALLOWED = ("calendar_app/storage.py", "calendar_app/domain.py", "calendar_app/service.py")
INITIAL = {"solution.py":ADAPTER,"calendar_app/cli.py":CLI,"policy.json":INITIAL_POLICY,
           ALLOWED[0]:V0_STORAGE,ALLOWED[1]:V0_DOMAIN,ALLOWED[2]:V0_SERVICE}

COMMON = '''Maintain this working SQLite calendar repository. Python standard library only.
Edit only calendar_app/storage.py (schema/persistence/queries), domain.py (typed
validation), service.py (transactions and behavior). solution.py and CLI are fixed;
every command launches a new CLI process against the same scenario DB.
policy.json is dormant until milestone4; do not enforce its limits earlier.
Input domain: payload has exactly commands and optionally legacy (only milestone
4). commands is a list of 0..24 commands. JSON nesting <=10, <=2000 nodes including object keys, object
or list size <=64, every string <=128 Unicode scalar characters (no surrogate code points),
integers within -10000..10000;
floats/booleans/null and malformed command values are permitted for typed-error
testing. Serialized compact ASCII JSON <=32768 bytes. Nonfinite numbers are out
of domain. Inputs whose correct compact ASCII JSON output exceeds32768 bytes
are also outside the domain. A string-valued op field naming a future-milestone operation in any nested
command object is outside this milestone's domain; validate_input rejects them without naming future ops.
Ordinary malformed/unknown operations are within domain and must return invalid.
Command objects have EXACT specified fields. All names key/rid/resource/user are
strings length 1..32. Logical times are integers 0..1000 excluding bool/float.
Intervals require start<end. Unknown fields, wrong types, missing fields or bad
bounds return {error:'invalid'} without mutation. Validate ALL shapes before
state checks. No wall clock or implicit expiry; only explicit expire mutates
expired holds. Capacity is one per resource, with HALF-OPEN intervals [start,end).
Overlaps exist exactly when a.start<b.end AND b.start<a.end. Active booked/held
records occupy capacity; closed records do not. Each top-level mutation has key.
The first successful key/body is durable; body is entire JSON command except key
(object order ignored, array order retained). Exact retries return original
response without mutation, even after later state changes. Different valid body
under an existing key -> key_conflict. Failed requests consume no key or audit
sequence. All failures are atomic. Keep every closed booking; IDs never reuse.
{op:'snapshot'} returns {version:2,bookings:[...],waiting:[...]}. Bookings sorted
lexically by rid: {rid,resource,user,start,end,status,token,expires}. Initial
booked token=0,expires=null. Cancelled/expired rows retain token/expiry values.
Waiting rows sorted by seq: {seq,rid,resource,user,start,end,status}; initially [].
State errors below use {error:code}. Unless stated, successful mutations return
{ok:true}. Sequential restart is tested; simultaneous clients/crash faults are not.
'''
PARTS = (
'''Milestone 1: Repair existing adjacency and idempotency bugs while retaining basic behavior.
book {op:'book',key,rid,resource,user,start,end}: existing ID -> duplicate;
otherwise overlapping active booking -> conflict; else insert booked row.
cancel {op:'cancel',key,rid}: absent -> missing; inactive -> closed; otherwise
set status=cancelled. Unknown operations are invalid. Snapshot has no side effects.
''',
'''Milestone 2: Add edits and atomic multi-resource bundles, preserving milestone 1.
move {op:'move',key,rid,start,end}: absent -> missing; inactive -> closed;
exclude itself from conflict checks and retain resource/user/token/expiry.
A conflict returns conflict with old interval unchanged; otherwise update times.
bundle {op:'bundle',key,items:[{rid,resource,user,start,end},...]}, 1..4 items.
Validate all item shapes first, then book in array order atomically. The first
state error aborts every item; same-resource or duplicate-ID conflicts within the
bundle count. Successful bundle records one request and one outer event.
''',
'''Milestone 3: Add holds, fencing, explicit expiry, atomic batches and complete audit.
hold {op:'hold',key,rid,resource,user,start,end,now,ttl}, ttl integer1..1000 and
now+ttl<=1000. Same duplicate/conflict precedence as book. Create held token=1,
expires=now+ttl; return {ok:true,token:1}. Holds occupy capacity even when their
logical expiry has passed until expire is called.
renew {op:'renew',key,rid,token,now,ttl}: require existing row, matching token,
held status, now<expires, in that order (errors missing,stale,closed,expired).
Increment token by one and set expires=now+ttl; return {ok:true,token:newToken}.
confirm {op:'confirm',key,rid,token,now}: same ordered checks as renew; set
status=booked,expires=null and preserve current token. token inputs are integers
1..1000. move on held -> held, after existence/active checks, before conflicts.
expire {op:'expire',key,now}: all held rows with expires<=now become expired.
No-op expire succeeds; no implicit ordering constraint between logical times.
batch {op:'batch',key,commands:[inner,...]}, 1..4 inner mutations supported by the
CURRENT milestone, WITHOUT keys. Nested batch and read operations invalid. Validate all
inner shapes before any state checks. Then execute in order atomically; first
state error aborts whole batch and returns {error:code}, without an index. Success
returns {ok:true,results:[innerResponses...]}. Inner calls consume no keys/events.
{op:'audit'} returns [{seq,key,op},...] in seq order starting at1, with no gaps:
exactly one event per successful top-level mutation, including no-ops; none for
retries, failures, reads or migration. Audit every earlier mutation type too.
''',
'''Milestone 4: Integrate the trusted upstream policy.json change, migration, and FIFO admission.
policy.json now sets max_duration=12 and max_active_per_user=2. Read the file;
do not hard-code values. New book/hold/admitted rows must respect duration and
current active booked+held count for their user. Existing rows are grandfathered;
reads/cancel/confirm/renew/expire do not revalidate policy. A move revalidates both
limits but excludes its own active row from the user count. State precedence for
book/hold: duplicate, policy, conflict. For move: missing,closed,held,policy,conflict.
Policy violation -> policy. Bundle and batch apply policy to intermediate state.
enqueue {op:'enqueue',key,rid,resource,user,start,end}: ID in bookings OR waiting
(including closed entries) -> duplicate; overlong duration -> policy; otherwise
append waiting row status=waiting with contiguous enrollment seq starting at1.
Do NOT reserve capacity or enforce active-user cap during enqueue. Queue entries
are retained. cancel can cancel a waiting entry; admitted IDs cancel their booking
and leave queue status=admitted. A cancelled waiting entry returns closed later.
admit {op:'admit',key}: choose earliest seq among CURRENTLY FEASIBLE waiting
entries (skip blocked older rows), considering resource conflicts and both policy
limits. Admit at most one by inserting booked token0/expiry null and changing
queue status=admitted. Return {ok:true,admitted:rid}; if none, admitted:null.
No automatic admission after cancel/expire; explicit admit is needed. Admission
can appear inside batch; its queue/booking changes roll back on later failure.
Optional payload legacy:[{rid,resource,user,start,end},...] seeds version-1
bookings table (rid,resource,user,start,end,status), status=booked, no tokens,
expiry, keys, queue or audit. Up to8 valid unique IDs; initial rows may overlap or
exceed NEW policy (grandfathered). Migrate transactionally to version2 with
token0/expiry null, preserving every row and producing no audit. Subsequent CLI
processes must preserve migrated state. Legacy input has no malformed rows.
''')
SPECS = tuple(COMMON+"\n".join(PARTS[:i+1]) for i in range(4))
LEVELS = {"book":0,"cancel":0,"move":1,"bundle":1,"hold":2,"renew":2,"confirm":2,"expire":2,"batch":2,"audit":2,"enqueue":3,"admit":3}


def validate_input(stage_index, payload):
    """Validate probe envelope/domain only, not ordinary command validity."""
    def reject():
        raise ValueError("outside_input_domain")
    if type(stage_index) is not int or not 0 <= stage_index < 4:
        reject()
    budget = [2000]
    def visit(value, depth=0, command_area=False):
        budget[0] -= 1
        if budget[0] < 0 or depth > 10:
            reject()
        if value is None or type(value) is bool:
            return
        if type(value) is int:
            if not -10000 <= value <= 10000:
                reject()
            return
        if type(value) is float:
            if not math.isfinite(value):
                reject()
            return
        if type(value) is str:
            if len(value)>128 or any(0xD800 <= ord(char) <= 0xDFFF for char in value):
                reject()
            return
        if type(value) is list and len(value)<=64:
            for item in value:
                visit(item,depth+1,command_area)
            return
        if type(value) is dict and len(value)<=64 and all(type(k) is str and len(k)<=128 for k in value):
            operation=value.get("op")
            if command_area and type(operation) is str and LEVELS.get(operation,stage_index)>stage_index:
                reject()
            for key,item in value.items():
                visit(key,depth+1,command_area)
                visit(item,depth+1,command_area)
            return
        reject()
    if type(payload) is not dict or not {"commands"} <= set(payload) <= {"commands","legacy"}:
        reject()
    if type(payload["commands"]) is not list or len(payload["commands"])>24:
        reject()
    visit(payload)
    budget[0] = 2000
    visit(payload["commands"],command_area=True)
    if len(json.dumps(payload,sort_keys=True,ensure_ascii=True,separators=(",",":"),allow_nan=False).encode())>32768:
        reject()
    if "legacy" in payload:
        if stage_index!=3 or type(payload["legacy"]) is not list or len(payload["legacy"])>8:
            reject()
        ids=set()
        for row in payload["legacy"]:
            if (type(row) is not dict or set(row)!={"rid","resource","user","start","end"}
                or any(type(row[k]) is not str or not 1<=len(row[k])<=32 for k in ("rid","resource","user"))
                or type(row["start"]) is not int or type(row["end"]) is not int
                or not 0<=row["start"]<row["end"]<=1000 or row["rid"] in ids):
                reject()
            ids.add(row["rid"])
    if len(json.dumps(_reference(stage_index,payload),sort_keys=True,ensure_ascii=True,separators=(",",":"),allow_nan=False).encode())>32768:
        reject()


def _reference(stage_index, payload):
    """Independent copy-on-write in-memory reference; no SQLite/reference-code eval."""
    state={"bookings":{},"waiting":[],"keys":{},"events":[]}
    for row in payload.get("legacy",[]):
        state["bookings"][row["rid"]]={**deepcopy(row),"status":"booked","token":0,"expires":None}
    class Error(Exception):
        pass
    def need(ok,error="invalid"):
        if not ok:
            raise Error(error)
    item_fields={"rid","resource","user","start","end"}
    fields={"book":item_fields,"cancel":{"rid"},"move":{"rid","start","end"},"bundle":{"items"},"hold":item_fields|{"now","ttl"},"renew":{"rid","token","now","ttl"},"confirm":{"rid","token","now"},"expire":{"now"},"batch":{"commands"},"enqueue":item_fields,"admit":set()}
    def validate(command,inner=False):
        need(type(command) is dict and type(command.get("op")) is str)
        op=command["op"]
        need(op in fields and LEVELS[op]<=stage_index)
        need(set(command)==fields[op]|{"op"}|(set() if inner else {"key"}))
        for k in ("key","rid","resource","user"):
            if k in command:
                need(type(command[k]) is str and 1<=len(command[k])<=32)
        for k,minimum in (("start",0),("end",0),("now",0),("ttl",1),("token",1)):
            if k in command:
                need(type(command[k]) is int and minimum<=command[k]<=1000)
        if "start" in command:
            need(command["start"]<command["end"])
        if "ttl" in command:
            need(command["now"]+command["ttl"]<=1000)
        if op=="bundle":
            need(type(command["items"]) is list and 1<=len(command["items"])<=4)
            for row in command["items"]:
                need(type(row) is dict and set(row)==item_fields)
                validate({"op":"book",**row},True)
        if op=="batch":
            need(type(command["commands"]) is list and 1<=len(command["commands"])<=4)
            for row in command["commands"]:
                need(type(row) is dict and row.get("op")!="batch")
                validate(row,True)
    def live(row):
        return row["status"] in ("held","booked")
    def policy_ok(s,row,exclude=None):
        return stage_index<3 or (row["end"]-row["start"]<=12 and sum(live(b) and b["user"]==row["user"] and rid!=exclude for rid,b in s["bookings"].items())<2)
    def conflict(s,row,exclude=None):
        # Integer cell sets intentionally differ from SQL interval predicates.
        wanted=set(range(row["start"],row["end"]))
        return any(rid!=exclude and live(b) and b["resource"]==row["resource"] and wanted.intersection(range(b["start"],b["end"])) for rid,b in s["bookings"].items())
    def exists(s,rid):
        return rid in s["bookings"] or any(w["rid"]==rid for w in s["waiting"])
    def add(s,c,status="booked",token=0,expires=None,from_queue=False):
        need(c["rid"] not in s["bookings"] and (from_queue or not exists(s,c["rid"])),"duplicate")
        need(policy_ok(s,c),"policy")
        need(not conflict(s,c),"conflict")
        s["bookings"][c["rid"]]={**{k:c[k] for k in item_fields},"status":status,"token":token,"expires":expires}
    def get_active(s,rid):
        need(rid in s["bookings"],"missing")
        row=s["bookings"][rid]
        need(live(row),"closed")
        return row
    def apply(s,c):
        op=c["op"]
        if op=="book":
            add(s,c)
        elif op=="cancel":
            if c["rid"] in s["bookings"]:
                get_active(s,c["rid"])["status"]="cancelled"
            else:
                rows=[w for w in s["waiting"] if w["rid"]==c["rid"]]
                need(stage_index>=3 and bool(rows),"missing")
                need(rows[0]["status"]=="waiting","closed")
                rows[0]["status"]="cancelled"
        elif op=="move":
            row=get_active(s,c["rid"])
            need(row["status"]!="held","held")
            changed={**row,"start":c["start"],"end":c["end"]}
            need(policy_ok(s,changed,c["rid"]),"policy")
            need(not conflict(s,changed,c["rid"]),"conflict")
            row.update(start=c["start"],end=c["end"])
        elif op=="bundle":
            for row in c["items"]:
                add(s,row)
        elif op=="hold":
            add(s,c,"held",1,c["now"]+c["ttl"])
            return {"ok":True,"token":1}
        elif op in ("renew","confirm"):
            need(c["rid"] in s["bookings"],"missing")
            row=s["bookings"][c["rid"]]
            need(row["token"]==c["token"],"stale")
            need(row["status"]=="held","closed")
            need(c["now"]<row["expires"],"expired")
            if op=="renew":
                row.update(token=row["token"]+1,expires=c["now"]+c["ttl"])
                return {"ok":True,"token":row["token"]}
            row.update(status="booked",expires=None)
        elif op=="expire":
            for row in s["bookings"].values():
                if row["status"]=="held" and row["expires"]<=c["now"]:
                    row["status"]="expired"
        elif op=="batch":
            return {"ok":True,"results":[apply(s,row) for row in c["commands"]]}
        elif op=="enqueue":
            need(not exists(s,c["rid"]),"duplicate")
            need(c["end"]-c["start"]<=12,"policy")
            s["waiting"].append({"seq":len(s["waiting"])+1,**{k:c[k] for k in item_fields},"status":"waiting"})
        elif op=="admit":
            for row in s["waiting"]:
                if row["status"]=="waiting" and policy_ok(s,row) and not conflict(s,row):
                    add(s,row,from_queue=True)
                    row["status"]="admitted"
                    return {"ok":True,"admitted":row["rid"]}
            return {"ok":True,"admitted":None}
        return {"ok":True}
    answers=[]
    for command in payload["commands"]:
        if command=={"op":"snapshot"}:
            answers.append({"version":2,"bookings":[deepcopy(v) for k,v in sorted(state["bookings"].items())],"waiting":deepcopy(state["waiting"])})
            continue
        if stage_index>=2 and command=={"op":"audit"}:
            answers.append(deepcopy(state["events"]))
            continue
        try:
            validate(command)
            key=command["key"]
            body=json.dumps({k:v for k,v in command.items() if k!="key"},sort_keys=True)
            if key in state["keys"]:
                old_body,answer=state["keys"][key]
                need(body==old_body,"key_conflict")
                answers.append(deepcopy(answer))
                continue
            revised=deepcopy(state)
            answer=apply(revised,command)
            revised["keys"][key]=(body,deepcopy(answer))
            revised["events"].append({"seq":len(revised["events"])+1,"key":key,"op":command["op"]})
            state=revised
            answers.append(answer)
        except Error as error:
            answers.append({"error":str(error)})
    return answers


def reference(stage_index,payload):
    """Validate the bounded domain, then compute the expected JSON response."""
    validate_input(stage_index,payload)
    return _reference(stage_index,payload)


def command(op,key=None,**fields):
    return {"op":op,**({"key":key} if key is not None else {}),**fields}

def item(rid="a",resource="room",user=None,start=2,end=6):
    return {"rid":rid,"resource":resource,"user":user or "user-"+rid,"start":start,"end":end}

def book(key="k",**fields):
    return command("book",key,**item(**fields))

def hold(key="h",now=0,ttl=5,**fields):
    return command("hold",key,**item(**fields),now=now,ttl=ttl)

def enqueue(key="q",**fields):
    return command("enqueue",key,**item(**fields))

SNAP={"op":"snapshot"}
AUDIT={"op":"audit"}
# Six public and twelve private scenarios per milestone; each scenario input is
# fresh and frozen before evaluation. Data-only probes can use the same contract.
SCENARIOS=(
 (
  ("basic","booking",[book(),SNAP]),
  ("adjacent","half-open",[book(),book("b",rid="b",start=6,end=9),SNAP]),
  ("overlap","half-open",[book(),book("b",rid="b",start=5,end=7),SNAP]),
  ("cancel","cancellation",[book(),command("cancel","c",rid="a"),book("b",rid="b"),SNAP]),
  ("retry","idempotency",[book(),book(),book("k",rid="b"),SNAP]),
  ("typed","typed-errors",[book(start=True),book("b",start=2.0),SNAP]),
  ("left-adjacency","half-open",[book("a",start=6,end=10),book("b",rid="b",start=1,end=6),SNAP]),
  ("containment","half-open",[book("a",start=4,end=8),book("b",rid="b",start=3,end=9),SNAP]),
  ("contained","half-open",[book("a",start=1,end=10),book("b",rid="b",start=4,end=5),SNAP]),
  ("resources","booking",[book("z",rid="z",resource="z"),book("a",rid="a",resource="a"),SNAP]),
  ("failed-key","idempotency",[book("a"),book("b",rid="b"),command("cancel","c",rid="a"),book("b",rid="b"),SNAP]),
  ("cancel-retry","idempotency",[book("a"),command("cancel","c",rid="a"),book("b",rid="b"),command("cancel","c",rid="a"),SNAP]),
  ("closed-id","cancellation",[book("a"),command("cancel","b",rid="a"),book("c"),command("cancel","d",rid="a"),SNAP]),
  ("lookup","cancellation",[command("cancel","x",rid="missing"),SNAP]),
  ("shape-crossproduct","typed-errors",[None,[],{"op":[]},{"op":{}},dict(book(),extra=1),{"op":"snapshot","key":"x"},SNAP]),
  ("bounds","typed-errors",[book("a",start=5,end=5),book("b",start=-1,end=5),book("c",start=0,end=1001),book("d",user=""),SNAP]),
  ("malformed-state-precedence","typed-errors",[book("a"),dict(book("b"),start="2"),command("cancel","c",rid=0),SNAP]),
  ("replay-after-cancel","restart",[book("a"),command("cancel","b",rid="a"),book("a"),SNAP]),
 ),
 (
  ("move","reschedule",[book(),command("move","m",rid="a",start=7,end=10),SNAP]),
  ("move-conflict","reschedule",[book(),book("b",rid="b",start=7,end=10),command("move","m",rid="a",start=6,end=9),SNAP]),
  ("bundle","bundle-atomicity",[command("bundle","b",items=[item("a"),item("b",resource="other")]),SNAP]),
  ("bundle-conflict","bundle-atomicity",[command("bundle","b",items=[item("a"),item("b")]),SNAP]),
  ("move-self","reschedule",[book(),command("move","m",rid="a",start=3,end=7),SNAP]),
  ("bundle-shape","typed-errors",[command("bundle","b",items=[item("a"),dict(item("b"),end=False)]),SNAP]),
  ("move-adjacency","reschedule",[book("a",start=1,end=4),book("b",rid="b",start=8,end=10),command("move","m",rid="a",start=4,end=8),SNAP]),
  ("move-old-freed","restart",[book("a"),command("move","m",rid="a",start=10,end=12),book("b",rid="b"),SNAP]),
  ("move-retry","idempotency",[book("a"),command("move","m",rid="a",start=8,end=10),command("move","n",rid="a",start=12,end=14),command("move","m",rid="a",start=8,end=10),SNAP]),
  ("move-closed","reschedule",[book("a"),command("cancel","c",rid="a"),command("move","m",rid="a",start=3,end=7),command("move","n",rid="none",start=3,end=7),SNAP]),
  ("bundle-duplicate","bundle-atomicity",[command("bundle","b",items=[item("a"),item("a",resource="other")]),SNAP]),
  ("bundle-rollback-existing","bundle-atomicity",[book("a"),command("bundle","b",items=[item("b",resource="other"),item("c")]),SNAP]),
  ("bundle-adjacency","bundle-atomicity",[command("bundle","b",items=[item("a",start=0,end=4),item("b",start=4,end=8)]),SNAP]),
  ("bundle-failed-key","idempotency",[command("bundle","b",items=[item("a"),item("b")]),command("bundle","b",items=[item("a"),item("b",resource="other")]),SNAP]),
  ("validation-first","typed-errors",[book("a"),command("bundle","b",items=[item("a"),dict(item("b"),start=[])]),SNAP]),
  ("bundle-container-types","typed-errors",[command("bundle","a",items={}),command("bundle","b",items=[]),command("bundle","c",items=[None]),SNAP]),
  ("move-int-types","typed-errors",[book("a"),command("move","b",rid="a",start=False,end=5),command("move","c",rid="a",start=1,end=8.0),SNAP]),
  ("bundle-order-key","idempotency",[command("bundle","k",items=[item("a"),item("b",resource="other")]),command("bundle","k",items=[item("b",resource="other"),item("a")]),SNAP]),
 ),
 (
  ("hold-confirm","fencing",[hold(),command("confirm","c",rid="a",token=1,now=1),SNAP,AUDIT]),
  ("renew-fence","fencing",[hold(),command("renew","r",rid="a",token=1,now=1,ttl=5),command("confirm","c",rid="a",token=1,now=2),command("confirm","d",rid="a",token=2,now=2),SNAP]),
  ("expire","expiration",[hold(),command("expire","e",now=5),book("b",rid="b"),SNAP]),
  ("batch","batch-atomicity",[command("batch","b",commands=[command("book",**item()),command("cancel",rid="a")]),SNAP,AUDIT]),
  ("batch-rollback","batch-atomicity",[command("batch","b",commands=[command("book",**item()),command("book",**item("b"))]),SNAP,AUDIT]),
  ("audit-retry","audit",[book("a"),book("a"),command("cancel","c",rid="a"),command("cancel","d",rid="a"),AUDIT]),
  ("boundary-no-implicit","expiration",[hold("a",ttl=3),command("confirm","c",rid="a",token=1,now=3),book("b",rid="b"),command("expire","e",now=3),book("b",rid="b"),SNAP,AUDIT]),
  ("renew-replay","restart",[hold("a",ttl=4),command("renew","r",rid="a",token=1,now=1,ttl=6),command("renew","r",rid="a",token=1,now=1,ttl=6),command("renew","s",rid="a",token=1,now=2,ttl=5),SNAP]),
  ("fence-error-precedence","fencing",[hold("a"),command("cancel","c",rid="a"),command("confirm","x",rid="a",token=2,now=9),command("confirm","y",rid="a",token=1,now=9),command("confirm","z",rid="missing",token=1,now=1),SNAP]),
  ("expired-renew","expiration",[hold("a",ttl=2),command("renew","r",rid="a",token=1,now=2,ttl=3),command("expire","e",now=2),command("renew","s",rid="a",token=1,now=1,ttl=3),SNAP]),
  ("hold-blocks-move","reschedule",[hold("a"),command("move","m",rid="a",start=8,end=10),book("b",rid="b",start=1,end=3),SNAP]),
  ("batch-shape-first","typed-errors",[book("a"),command("batch","b",commands=[command("book",**item()),{"op":[]}]),SNAP,AUDIT]),
  ("nested-and-reads","typed-errors",[command("batch","a",commands=[{"op":"batch","commands":[]}]),command("batch","b",commands=[{"op":"snapshot"}]),command("batch","c",commands=[{"op":"audit"}]),AUDIT]),
  ("batch-token-rollback","batch-atomicity",[hold("a"),command("batch","b",commands=[command("renew",rid="a",token=1,now=1,ttl=7),command("cancel",rid="missing")]),command("confirm","c",rid="a",token=1,now=2),SNAP,AUDIT]),
  ("batch-response","batch-atomicity",[command("batch","b",commands=[command("hold",**item(),now=0,ttl=3),command("renew",rid="a",token=1,now=1,ttl=5)]),SNAP,AUDIT]),
  ("expiry-selectivity","audit",[hold("a",ttl=3),hold("b",rid="b",resource="other",ttl=4),command("expire","e",now=3),command("expire","f",now=3),SNAP,AUDIT]),
  ("token-and-ttl-types","typed-errors",[hold("a"),command("confirm","c",rid="a",token=True,now=1),command("renew","r",rid="a",token=1,now=1,ttl=1.0),hold("b",rid="b",resource="other",now=999,ttl=2),SNAP]),
  ("expire-key-replay","idempotency",[hold("a",ttl=2),command("expire","e",now=2),hold("b",rid="b",ttl=2),command("expire","e",now=2),SNAP,AUDIT]),
 ),
 (
  ("duration-policy","policy",[book("a",start=0,end=13),book("b",rid="b",start=0,end=12),SNAP,AUDIT]),
  ("user-limit","policy",[book("a",resource="a",user="u"),book("b",rid="b",resource="b",user="u"),book("c",rid="c",resource="c",user="u"),SNAP]),
  ("queue-admit","waitlist",[enqueue(),command("admit","a"),SNAP,AUDIT]),
  ("queue-skip","waitlist",[book("block"),enqueue("q",rid="q"),enqueue("r",rid="r",resource="other"),command("admit","a"),SNAP]),
  ("migration","migration",[SNAP,command("cancel","c",rid="legacy"),SNAP,AUDIT],[item("legacy",start=0,end=30)]),
  ("queue-cancel","waitlist",[enqueue("q"),command("cancel","c",rid="a"),command("admit","a"),SNAP,AUDIT]),
  ("queue-fifo","waitlist",[enqueue("z",rid="z"),enqueue("a",rid="a"),command("admit","one"),command("cancel","c",rid="z"),command("admit","two"),SNAP,AUDIT]),
  ("user-limit-skip","waitlist",[book("a",resource="a",user="u"),book("b",rid="b",resource="b",user="u"),enqueue("q",rid="q",resource="q",user="u"),enqueue("r",rid="r",resource="r",user="v"),command("admit","one"),command("cancel","c",rid="a"),command("admit","two"),SNAP]),
  ("move-excludes-self","policy",[hold("a",resource="a",user="u",ttl=4),book("b",rid="b",resource="b",user="u"),command("renew","r",rid="a",token=1,now=1,ttl=6),command("confirm","c",rid="a",token=2,now=2),command("move","m",rid="a",start=10,end=20),SNAP,AUDIT]),
  ("bundle-policy-rollback","policy",[book("a",resource="a",user="u"),command("bundle","b",items=[item("b",resource="b",user="u"),item("c",resource="c",user="u")]),SNAP,AUDIT]),
  ("queue-batch-rollback","batch-atomicity",[enqueue("q"),command("batch","b",commands=[command("admit"),command("cancel",rid="missing")]),SNAP,command("admit","a"),SNAP,AUDIT]),
  ("admission-key-replay","restart",[enqueue("q",rid="q"),enqueue("r",rid="r",resource="other"),command("admit","a"),command("admit","a"),SNAP,AUDIT]),
  ("idle-key-replay","idempotency",[command("admit","a"),enqueue("q"),command("admit","a"),command("admit","b"),SNAP,AUDIT]),
  ("grandfather-and-move","migration",[SNAP,command("move","m",rid="legacy",start=1,end=21),book("n",rid="new",resource="new",user="u"),command("cancel","c",rid="other"),book("n",rid="new",resource="new",user="u"),SNAP,AUDIT],[item("legacy",user="u",start=0,end=20),item("other",resource="other",user="u")]),
  ("typed-policy","typed-errors",[enqueue("q",start=False),command("admit","a",extra=True),command("batch","b",commands=[{"op":{}}]),SNAP,AUDIT]),
  ("queue-id-retained","waitlist",[enqueue("q"),command("cancel","c",rid="a"),book("b"),enqueue("r"),command("cancel","d",rid="a"),SNAP,AUDIT]),
  ("expire-then-admit","expiration",[hold("h",ttl=3),enqueue("q",rid="q"),command("admit","a"),command("expire","e",now=3),SNAP,command("admit","b"),SNAP,AUDIT]),
  ("policy-priority","policy",[book("a",user="u"),book("b",rid="b",resource="b",user="u"),book("c",rid="c",user="u"),book("d",rid="a",start=0,end=20),SNAP,AUDIT]),
 ),
)
REQUIREMENTS=(
 ("booking","half-open","cancellation","idempotency","typed-errors","restart"),
 ("reschedule","bundle-atomicity","idempotency","typed-errors","restart"),
 ("fencing","expiration","batch-atomicity","audit","reschedule","idempotency","typed-errors","restart"),
 ("policy","waitlist","migration","batch-atomicity","idempotency","typed-errors","expiration","restart"),
)
TITLES=("Repair interval and request semantics","Atomic edits across resources","Expiring holds and fenced confirmation","Integrate policy and durable admission")

def _cases(index,rows,private):
    result=[]
    for name,requirement,commands,*legacy in rows:
        payload={"commands":deepcopy(commands)}
        if legacy:
            payload["legacy"]=deepcopy(legacy[0])
        result.append({"id":f"calendar-s{index+1}-{'h' if private else 'v'}-{name}","requirement":requirement,"input":payload,"expected":reference(index,payload)})
    return tuple(result)

STAGES=tuple({
    "id":f"calendar-stage-{i+1}","title":TITLES[i],"specification":SPECS[i],"requirements":REQUIREMENTS[i],
    "visible_cases":_cases(i,SCENARIOS[i][:6],False),"hidden_cases":_cases(i,SCENARIOS[i][6:],True),
    "known_files":{**INITIAL,ALLOWED[0]:STORAGE,ALLOWED[1]:DOMAIN,ALLOWED[2]:SERVICE.replace("STAGE = 3",f"STAGE = {i}"),"policy.json":FINAL_POLICY if i==3 else INITIAL_POLICY},
    **({"trusted_updates":{"policy.json":FINAL_POLICY}} if i==3 else {}),
} for i in range(4))
PROJECT={"id":"calendar","title":"Resource Booking Calendar Maintenance","initial_files":INITIAL,"allowed_paths":ALLOWED,"stages":STAGES}
MUTANTS={
 "closed_interval":{**STAGES[-1]["known_files"],ALLOWED[0]:STORAGE.replace('start < row["end"] and row["start"] < end','start <= row["end"] and row["start"] <= end')},
 "autocommit":{**STAGES[-1]["known_files"],ALLOWED[0]:STORAGE.replace('db.commit()\n    return db','db.commit()\n    db.isolation_level = None\n    return db')},
 "stale_fence":{**STAGES[-1]["known_files"],ALLOWED[2]:SERVICE.replace('require(row["token"] == command["token"], "stale")','require(True, "stale")')},
 "waitlist_head_blocking":{**STAGES[-1]["known_files"],ALLOWED[2]:SERVICE.replace('ORDER BY seq").fetchall()', 'ORDER BY seq LIMIT 1").fetchall()')},
}
