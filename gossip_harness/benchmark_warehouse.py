"""Fresh maintained warehouse fixture for the exploratory continuation pilot.

The shipped SQLite implementation and pure host reference are separately written.
Only initial_files, stage specifications and visible cases enter model requests.
Golden sources and semantic faults require physical Docker qualification; their
presence here is not evidence that that qualification has happened.
"""
from __future__ import annotations

from copy import deepcopy
import json
from textwrap import dedent
from typing import Any

CONTRACT = "warehouse-fulfillment-v1"
ALLOWED = ("warehouse_app/storage.py", "warehouse_app/domain.py", "warehouse_app/service.py")


def src(value):
    return dedent(value).lstrip()


ADAPTER = src('''
    import json
    from pathlib import Path
    import subprocess
    import sys
    import tempfile

    def solve(payload):
        answers = []
        with tempfile.TemporaryDirectory(prefix="warehouse-") as directory:
            database = str(Path(directory) / "warehouse.sqlite")
            bridge = Path(__file__).parent / "warehouse_app" / "cli.py"
            for command in payload["commands"]:
                result = subprocess.run([sys.executable, str(bridge), database, json.dumps(command)],
                                        capture_output=True, text=True, timeout=4)
                if result.returncode:
                    raise RuntimeError("Warehouse CLI exited unsuccessfully")
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
        print(json.dumps(answer, sort_keys=True, separators=(",", ":")))
''')
STORAGE = src('''
    """Persistent warehouse rows; transactions are owned by the service."""
    import sqlite3

    def connect(path):
        db = sqlite3.connect(path)
        db.row_factory = sqlite3.Row
        db.execute("CREATE TABLE IF NOT EXISTS lots(lot TEXT PRIMARY KEY,sku TEXT NOT NULL,expires INTEGER NOT NULL,available INTEGER NOT NULL,reserved INTEGER NOT NULL,shipped INTEGER NOT NULL)")
        db.execute("CREATE TABLE IF NOT EXISTS orders(oid TEXT PRIMARY KEY,status TEXT NOT NULL)")
        db.execute("CREATE TABLE IF NOT EXISTS allocations(oid TEXT NOT NULL,lot TEXT NOT NULL,qty INTEGER NOT NULL,PRIMARY KEY(oid,lot))")
        db.execute("CREATE TABLE IF NOT EXISTS requests(key TEXT PRIMARY KEY,body TEXT NOT NULL,response TEXT NOT NULL)")
        db.execute("CREATE TABLE IF NOT EXISTS events(seq INTEGER PRIMARY KEY,key TEXT NOT NULL,op TEXT NOT NULL)")
        # Extension schemas belong here.
        db.commit()
        return db

    def rows(db, sql, params=()):
        return [dict(row) for row in db.execute(sql, params)]

    def allocations(db, order):
        return rows(db, "SELECT lot,qty FROM allocations WHERE oid=? AND qty>0 ORDER BY lot", (order,))

    def stock(db):
        return rows(db, "SELECT * FROM lots ORDER BY lot")

    def orders(db):
        return [{"order":row["oid"],"status":row["status"],"allocations":allocations(db,row["oid"])}
                for row in db.execute("SELECT * FROM orders ORDER BY oid")]
''')
SHIP_SCHEMA = '''        db.execute("CREATE TABLE IF NOT EXISTS shipments(sid TEXT PRIMARY KEY,oid TEXT NOT NULL)")
        db.execute("CREATE TABLE IF NOT EXISTS shipment_parts(sid TEXT NOT NULL,lot TEXT NOT NULL,qty INTEGER NOT NULL,returned INTEGER NOT NULL,PRIMARY KEY(sid,lot))")'''
DOMAIN = src('''
    """Exact syntax validation, prior to receipt lookup or mutable state."""
    class Invalid(Exception):
        pass

    def require(condition, error="invalid"):
        if not condition:
            raise Invalid(error)

    def name(value):
        require(type(value) is str and 1 <= len(value) <= 24)
        require(all(not 0xD800 <= ord(char) <= 0xDFFF for char in value))

    def integer(value, low=0):
        require(type(value) is int and low <= value <= 1000)

    def shape(command, fields):
        require(type(command) is dict and set(command) == set(fields))

    def validate(command):
        require(type(command) is dict and type(command.get("op")) is str)
        op = command["op"]
        fields = {"receive":{"lot","sku","qty","expires"},
                  "hold":{"order","lot","qty"},"release":{"order"},
                  "stock":set(),"orders":set(),"audit":set(),"receipt":{"key"}}
        require(op in fields)
        reads = {"stock","orders","audit","receipt"}
        shape(command, fields[op] | {"op"} | (set() if op in reads else {"key"}))
        for field in ("key","lot","sku","order"):
            if field in command:
                name(command[field])
        if "qty" in command:
            integer(command["qty"], 1)
        if "expires" in command:
            integer(command["expires"])
        return dict(command)
''')
ALLOCATE_DOMAIN = src('''
    _baseline_validate = validate

    def lines(value, identifier):
        require(type(value) is list and 1 <= len(value) <= 4)
        seen = set()
        for item in value:
            shape(item, (identifier,"qty"))
            name(item[identifier])
            integer(item["qty"], 1)
            require(item[identifier] not in seen)
            seen.add(item[identifier])
        return sorted((dict(item) for item in value), key=lambda item:item[identifier])

    def validate(command):
        if type(command) is not dict or command.get("op") != "allocate":
            return _baseline_validate(command)
        shape(command, ("op","key","order","now","lines"))
        name(command["key"])
        name(command["order"])
        integer(command["now"])
        return dict(command, lines=lines(command["lines"],"sku"))
''')
SHIP_DOMAIN = src('''
    _allocation_validate = validate

    def validate(command):
        op = command.get("op") if type(command) is dict else None
        if op not in ("ship","return","shipments"):
            return _allocation_validate(command)
        if op == "shipments":
            shape(command, ("op",))
            return dict(command)
        shape(command, ("op","key","shipment","order","lines") if op == "ship"
              else ("op","key","shipment","lines"))
        for field in ("key","shipment"):
            name(command[field])
        if op == "ship":
            name(command["order"])
        return dict(command, lines=lines(command["lines"], "sku" if op == "ship" else "lot"))
''')
SERVICE = src('''
    """Warehouse command service; every mutation and receipt is one transaction."""
    import json
    from domain import Invalid, require, validate
    from storage import connect, rows, allocations, stock, orders

    READS = {"stock","orders","audit","receipt"}

    def apply(db, command):
        op = command["op"]
        if op == "stock":
            return stock(db)
        if op == "orders":
            return orders(db)
        if op == "audit":
            return rows(db, "SELECT * FROM events ORDER BY seq")
        if op == "receipt":
            saved = db.execute("SELECT body,response FROM requests WHERE key=?", (command["key"],)).fetchone()
            return {"found":False} if saved is None else {"found":True,"op":json.loads(saved["body"])["op"],"response":json.loads(saved["response"])}
        if op == "receive":
            require(db.execute("SELECT 1 FROM lots WHERE lot=?", (command["lot"],)).fetchone() is None, "exists")
            db.execute("INSERT INTO lots VALUES (?,?,?,?,0,0)", (command["lot"],command["sku"],command["expires"],command["qty"]))
            return {"ok":"received","lot":command["lot"]}
        if op == "hold":
            require(db.execute("SELECT 1 FROM orders WHERE oid=?", (command["order"],)).fetchone() is None, "exists")
            row = db.execute("SELECT * FROM lots WHERE lot=?", (command["lot"],)).fetchone()
            require(row is not None, "missing")
            require(row["available"] >= command["qty"], "insufficient")
            db.execute("INSERT INTO orders VALUES (?, 'reserved')", (command["order"],))
            db.execute("INSERT INTO allocations VALUES (?,?,?)", (command["order"],command["lot"],command["qty"]))
            db.execute("UPDATE lots SET available=available-?,reserved=reserved+? WHERE lot=?", (command["qty"],command["qty"],command["lot"]))
            return {"ok":"held","order":command["order"],"allocations":allocations(db,command["order"])}
        row = db.execute("SELECT status FROM orders WHERE oid=?", (command["order"],)).fetchone()
        require(row is not None, "missing")
        require(row["status"] == "reserved", "closed")
        for item in allocations(db, command["order"]):
            db.execute("UPDATE lots SET available=available+?,reserved=reserved-? WHERE lot=?", (item["qty"],item["qty"],item["lot"]))
        db.execute("DELETE FROM allocations WHERE oid=?", (command["order"],))
        db.execute("UPDATE orders SET status='released' WHERE oid=?", (command["order"],))
        return {"ok":"released","order":command["order"]}

    def execute(path, command):
        db = connect(path)
        try:
            normalized = validate(command)
            db.execute("BEGIN IMMEDIATE")
            if normalized["op"] in READS:
                answer = apply(db, normalized)
            else:
                body = json.dumps({key:value for key,value in normalized.items() if key != "key"}, sort_keys=True,separators=(",",":"))
                saved = db.execute("SELECT body,response FROM requests WHERE key=?", (normalized["key"],)).fetchone()
                if saved is not None:
                    require(saved["body"] == body, "key_conflict")
                    answer = json.loads(saved["response"])
                else:
                    answer = apply(db, normalized)
                    db.execute("INSERT INTO requests VALUES (?,?,?)", (normalized["key"],body,json.dumps(answer,sort_keys=True)))
                    db.execute("INSERT INTO events(key,op) VALUES (?,?)", (normalized["key"],normalized["op"]))
            db.commit()
            return answer
        except Invalid as error:
            db.rollback()
            return {"error":str(error)}
        finally:
            db.close()
''')
ALLOCATE_SERVICE = src('''
    _baseline_apply = apply

    def apply(db, command):
        if command["op"] != "allocate":
            return _baseline_apply(db, command)
        order = command["order"]
        require(db.execute("SELECT 1 FROM orders WHERE oid=?", (order,)).fetchone() is None, "exists")
        db.execute("INSERT INTO orders VALUES (?, 'reserved')", (order,))
        for line in command["lines"]:
            remaining = line["qty"]
            eligible = rows(db, "SELECT * FROM lots WHERE sku=? AND expires>? AND available>0 ORDER BY expires,lot", (line["sku"],command["now"]))
            for lot in eligible:
                amount = min(remaining, lot["available"])
                if amount:
                    db.execute("INSERT INTO allocations VALUES (?,?,?)", (order,lot["lot"],amount))
                    db.execute("UPDATE lots SET available=available-?,reserved=reserved+? WHERE lot=?", (amount,amount,lot["lot"]))
                    remaining -= amount
            require(remaining == 0, "insufficient")
        return {"ok":"allocated","order":order,"allocations":allocations(db,order)}
''')
SHIP_SERVICE = src('''
    READS.add("shipments")
    _allocation_apply = apply

    def apply(db, command):
        op = command["op"]
        if op == "shipments":
            return [{"shipment":row["sid"],"order":row["oid"],"allocations":rows(db,"SELECT lot,qty,returned FROM shipment_parts WHERE sid=? ORDER BY lot",(row["sid"],))}
                    for row in db.execute("SELECT * FROM shipments ORDER BY sid")]
        if op == "ship":
            require(db.execute("SELECT 1 FROM shipments WHERE sid=?", (command["shipment"],)).fetchone() is None, "exists")
            order = db.execute("SELECT status FROM orders WHERE oid=?", (command["order"],)).fetchone()
            require(order is not None, "missing")
            require(order["status"] == "reserved", "closed")
            db.execute("INSERT INTO shipments VALUES (?,?)", (command["shipment"],command["order"]))
            for line in command["lines"]:
                remaining = line["qty"]
                reserved = rows(db,"SELECT a.lot,a.qty FROM allocations a JOIN lots l ON l.lot=a.lot WHERE a.oid=? AND l.sku=? AND a.qty>0 ORDER BY l.expires,l.lot", (command["order"],line["sku"]))
                for lot in reserved:
                    amount = min(remaining, lot["qty"])
                    if amount:
                        db.execute("UPDATE allocations SET qty=qty-? WHERE oid=? AND lot=?", (amount,command["order"],lot["lot"]))
                        db.execute("UPDATE lots SET reserved=reserved-?,shipped=shipped+? WHERE lot=?", (amount,amount,lot["lot"]))
                        db.execute("INSERT INTO shipment_parts VALUES (?,?,?,0)", (command["shipment"],lot["lot"],amount))
                        remaining -= amount
                require(remaining == 0, "insufficient")
            if not allocations(db, command["order"]):
                db.execute("UPDATE orders SET status='shipped' WHERE oid=?", (command["order"],))
            answer = rows(db,"SELECT lot,qty FROM shipment_parts WHERE sid=? ORDER BY lot",(command["shipment"],))
            return {"ok":"shipped","shipment":command["shipment"],"allocations":answer}
        if op == "return":
            require(db.execute("SELECT 1 FROM shipments WHERE sid=?", (command["shipment"],)).fetchone() is not None, "missing")
            for item in command["lines"]:
                part = db.execute("SELECT qty,returned FROM shipment_parts WHERE sid=? AND lot=?", (command["shipment"],item["lot"])).fetchone()
                require(part is not None, "missing")
                require(item["qty"] <= part["qty"] - part["returned"], "excess")
                db.execute("UPDATE shipment_parts SET returned=returned+? WHERE sid=? AND lot=?", (item["qty"],command["shipment"],item["lot"]))
                db.execute("UPDATE lots SET shipped=shipped-?,available=available+? WHERE lot=?", (item["qty"],item["qty"],item["lot"]))
            return {"ok":"returned","shipment":command["shipment"],"allocations":command["lines"]}
        return _allocation_apply(db, command)
''')


def _files(stage):
    storage, domain, service = STORAGE, DOMAIN, SERVICE
    if stage >= 0:
        domain += "\n" + ALLOCATE_DOMAIN
        service += "\n" + ALLOCATE_SERVICE
    if stage >= 1:
        storage = storage.replace("    # Extension schemas belong here.",
                                  "\n".join("    "+line.strip() for line in SHIP_SCHEMA.splitlines()))
        domain += "\n" + SHIP_DOMAIN
        service += "\n" + SHIP_SERVICE
    return {"solution.py":ADAPTER,"warehouse_app/cli.py":CLI,
            ALLOWED[0]:storage,ALLOWED[1]:domain,ALLOWED[2]:service}


def _stage(stage):
    if type(stage) is not int or stage not in (0, 1):
        raise ValueError("stage must be 0 or 1")


def known_files(stage_index):
    _stage(stage_index)
    return _files(stage_index)


INITIAL = _files(-1)
BASE_SPEC = '''Maintain this working SQLite warehouse application, not a blank project.
Only warehouse_app/storage.py, domain.py and service.py may change; standard
library only. The fixed adapter starts a fresh CLI process for EVERY command.
All state, request receipts and audit records survive those process restarts.
Each scenario input is {commands:[command,...]}, 1..40 commands. The scenario
envelope is finite JSON with maximum nesting depth16 (outer object depth0) and
2000 total values, counting containers and scalars but not object keys;
its default Python json.dumps representation has at most16000 characters.
Milestone W1 probe inputs cannot contain ship, return or shipments commands.
These envelope bounds limit probes; malformed command shapes within the bounds
are valid test inputs and must return the documented invalid error. Every command
has EXACT documented fields. Names are strings length1..24 of Unicode scalar
values (surrogate code points are invalid). Quantities are exact
integers1..1000; expiry/now exact integers0..1000; bool/float are never integers.
Malformed types, unknown ops, missing/extra fields or duplicate line identifiers
return {error:"invalid"}. Validate the entire command before any state lookup.
Successful mutations share one durable key namespace. Identity excludes key;
object order is irrelevant. Exact retries return the ORIGINAL response before
examining current state. Different valid body under a used key -> key_conflict.
Errors roll back every effect and consume neither key nor audit sequence. Every
first successful mutation adds ONE contiguous audit event {seq,key,op}; retries
and reads do not. IDs for lots and orders are never reusable, even when closed.

Baseline commands already work and must be preserved:
receive {op:"receive",key,lot,sku,qty,expires}: create one stock lot; existing lot
-> exists. Return {ok:"received",lot}. Expiry is metadata for manual operations.
hold {op:"hold",key,order,lot,qty}: create a manual single-lot reservation.
Existing order -> exists before missing lot -> missing before available qty
shortfall -> insufficient. Manual holds deliberately allow any expiry. Return
{ok:"held",order,allocations:[{lot,qty}]}. Move available units to reserved.
release {op:"release",key,order}: missing order -> missing; nonreserved -> closed.
Release only its current outstanding allocations, mark released, and return
{ok:"released",order}. Never delete historical IDs.
stock {op:"stock"}: sorted-by-lot list of {lot,sku,expires,available,reserved,shipped}.
Counters track current available, outstanding reserved, and net shipped units.
orders {op:"orders"}: sorted-by-order list of {order,status,allocations}; allocations
are positive outstanding {lot,qty} sorted by lot. Initial status is reserved;
release clears allocations and sets released. Baseline shipped is always zero.
audit {op:"audit"}: ordered audit list. receipt {op:"receipt",key}: {found:false}
for an unused/failed key, else {found:true,op:<original op>,response:<original
response>}. Read keys never become consumed mutation keys.
'''
PARTS = (
'''Milestone W1: Add allocate {op:"allocate",key,order,now,lines:[{sku,qty},...]}.
There are1..4 lines with unique sku. Normalize lines by sku before request identity;
line-order permutations are the SAME request. Validate every line before replay.
An existing order -> exists. For each requested SKU, choose available stock whose
expires>now (equality is expired), in ascending (expires,lot) order (FEFO, lexical
tie break). Split quantities across as many eligible lots as needed. A shortfall
in ANY line -> insufficient, rolling back all lots and the order. Success is one
atomic order with status reserved and response {ok:"allocated",order,allocations:
[{lot,qty},...]} sorted by lot. Later receipts retain these original quantities
after releases or shipments. Never replenish expired stock or silently substitute
another SKU. Manual baseline holds retain their original expiry-independent rule.
''',
'''Milestone W2: Preserve all prior behavior; add partial shipment and return.
ship {op:"ship",key,shipment,order,lines:[{sku,qty},...]} uses1..4 unique SKU lines,
normalized by sku for identity. Check existing shipment -> exists before missing
order -> missing before nonreserved order -> closed. Consume only this order's
outstanding allocations, by ascending (lot expiry,lot), regardless of current
expiry. Any shortfall -> insufficient and rolls back ALL changes and shipment ID.
Transfer consumed quantities from reserved to shipped. Positive remaining order
allocations keep reserved status; exhausting them sets shipped. Return
{ok:"shipped",shipment,allocations:[{lot,qty},...]} sorted by lot, recording exact
lot provenance forever. Partial shipments and baseline release compose: release
returns only unshipped outstanding units, and never modifies past shipments.
return {op:"return",key,shipment,lines:[{lot,qty},...]} uses1..4 unique lot lines,
normalized by lot. Missing shipment -> missing. Process normalized lines: a lot
absent from that shipment -> missing; qty greater than original shipped quantity
minus all earlier returns of that lot -> excess. The whole return is atomic.
Restore returned quantities to AVAILABLE inventory of the ORIGINAL lot, even if
expired. Reduce net shipped. Never reopen the order or add reserved quantities.
Return {ok:"returned",shipment,allocations:<normalized lines>}. Repeated partial
returns under distinct keys may total at most the originally shipped quantities;
matching key retries never apply twice and still return the original response.
shipments {op:"shipments"}: read sorted-by-shipment list of {shipment,order,
allocations:[{lot,qty,returned},...]}, sorted by lot including fully returned rows.
Inventory conservation for each lot: available+reserved+shipped=received qty.
Every new mutation still has exactly one durable receipt/audit event, including
multi-line operations. No new command permits editing a past shipment or receipt.
''')


class _Failure(Exception):
    pass


def _assert(ok, error="invalid"):
    if not ok:
        raise _Failure(error)


def _name(value):
    _assert(type(value) is str and 1 <= len(value) <= 24)
    _assert(not any(0xD800 <= ord(char) <= 0xDFFF for char in value))


def _number(value, low=0):
    _assert(type(value) is int and low <= value <= 1000)


def _parse(command, stage):
    _assert(type(command) is dict and type(command.get("op")) is str)
    op = command["op"]
    shapes = {"receive":("key","lot","sku","qty","expires"),
              "hold":("key","order","lot","qty"),"release":("key","order"),
              "stock":(),"orders":(),"audit":(),"receipt":("key",)}
    if stage >= 0:
        shapes["allocate"] = ("key","order","now","lines")
    if stage >= 1:
        shapes.update(ship=("key","shipment","order","lines"),
                      **{"return":("key","shipment","lines"),"shipments":()})
    _assert(op in shapes and set(command) == {"op", *shapes[op]})
    result = deepcopy(command)
    for field in ("key","lot","sku","order","shipment"):
        if field in command:
            _name(command[field])
    for field in ("qty","expires","now"):
        if field in command:
            _number(command[field], 1 if field == "qty" else 0)
    if "lines" in command:
        values = command["lines"]
        _assert(type(values) is list and 1 <= len(values) <= 4)
        identifier = "lot" if op == "return" else "sku"
        identities = []
        for item in values:
            _assert(type(item) is dict and set(item) == {identifier,"qty"})
            _name(item[identifier])
            _number(item["qty"], 1)
            identities.append(item[identifier])
        _assert(len(set(identities)) == len(identities))
        result["lines"] = sorted(values, key=lambda item:item[identifier])
    return result


def validate_input(stage_index, payload):
    _stage(stage_index)
    if type(payload) is not dict or set(payload) != {"commands"}:
        raise ValueError("outside_input_domain")
    commands = payload["commands"]
    if type(commands) is not list or not 1 <= len(commands) <= 40:
        raise ValueError("outside_input_domain")
    pending = [(payload,0)]
    visited = 0
    while pending:
        value, depth = pending.pop()
        visited += 1
        if visited > 2000 or depth > 16:
            raise ValueError("outside_input_domain")
        if type(value) is dict:
            if any(type(key) is not str for key in value):
                raise ValueError("outside_input_domain")
            pending.extend((item,depth+1) for item in value.values())
        elif type(value) is list:
            pending.extend((item,depth+1) for item in value)
        elif type(value) not in (str,int,float,bool,type(None)):
            raise ValueError("outside_input_domain")
    try:
        encoded = json.dumps(payload, allow_nan=False)
    except (TypeError, ValueError, RecursionError) as error:
        raise ValueError("outside_input_domain") from error
    if len(encoded) > 16000:
        raise ValueError("outside_input_domain")
    if stage_index == 0 and any(type(c) is dict and c.get("op") in ("ship","return","shipments") for c in commands):
        raise ValueError("second_milestone_operation")


def _allocation_rows(allocations):
    return [{"lot":lot,"qty":quantity} for lot,quantity in sorted(allocations.items()) if quantity]


def _stock(state):
    result = []
    for lot, item in sorted(state["lots"].items()):
        reserved = sum(order["allocations"].get(lot,0) for order in state["orders"].values())
        shipped = sum(parts.get(lot, (0,0))[0] - parts.get(lot, (0,0))[1]
                      for _,parts in state["shipments"].values())
        result.append(dict(lot=lot,sku=item["sku"],expires=item["expires"],
                           available=item["qty"]-reserved-shipped,reserved=reserved,shipped=shipped))
    return result


def _apply(state, command):
    op = command["op"]
    if op == "stock":
        return _stock(state)
    if op == "orders":
        return [dict(order=oid,status=order["status"],allocations=_allocation_rows(order["allocations"]))
                for oid,order in sorted(state["orders"].items())]
    if op == "audit":
        return deepcopy(state["audit"])
    if op == "receipt":
        saved = state["receipts"].get(command["key"])
        return {"found":False} if saved is None else dict(found=True,op=saved[0]["op"],response=deepcopy(saved[1]))
    if op == "shipments":
        return [dict(shipment=sid,order=oid,allocations=[dict(lot=lot,qty=qty,returned=returned)
                for lot,(qty,returned) in sorted(parts.items())]) for sid,(oid,parts) in sorted(state["shipments"].items())]
    body = {k:deepcopy(v) for k,v in command.items() if k != "key"}
    saved = state["receipts"].get(command["key"])
    if saved is not None:
        _assert(saved[0] == body, "key_conflict")
        return deepcopy(saved[1])
    if op == "receive":
        _assert(command["lot"] not in state["lots"], "exists")
        state["lots"][command["lot"]] = {k:command[k] for k in ("sku","qty","expires")}
        answer = dict(ok="received",lot=command["lot"])
    elif op in ("hold","allocate"):
        oid = command["order"]
        _assert(oid not in state["orders"], "exists")
        available = {row["lot"]:row for row in _stock(state)}
        selected = {}
        if op == "hold":
            _assert(command["lot"] in available, "missing")
            _assert(available[command["lot"]]["available"] >= command["qty"], "insufficient")
            selected[command["lot"]] = command["qty"]
        else:
            for line in command["lines"]:
                options = sorted((row for row in available.values()
                                  if row["sku"] == line["sku"] and row["expires"] > command["now"]),
                                 key=lambda row:(row["expires"],row["lot"]))
                _assert(sum(row["available"] for row in options) >= line["qty"], "insufficient")
                needed = line["qty"]
                for row in options:
                    amount = min(needed,row["available"])
                    if amount:
                        selected[row["lot"]] = amount
                        needed -= amount
        state["orders"][oid] = dict(status="reserved",allocations=selected)
        answer = dict(ok="held" if op == "hold" else "allocated",order=oid,allocations=_allocation_rows(selected))
    elif op == "release":
        _assert(command["order"] in state["orders"], "missing")
        order = state["orders"][command["order"]]
        _assert(order["status"] == "reserved", "closed")
        order.update(status="released",allocations={})
        answer = dict(ok="released",order=command["order"])
    elif op == "ship":
        _assert(command["shipment"] not in state["shipments"], "exists")
        _assert(command["order"] in state["orders"], "missing")
        order = state["orders"][command["order"]]
        _assert(order["status"] == "reserved", "closed")
        selected = {}
        for line in command["lines"]:
            eligible = sorted((lot for lot in order["allocations"] if state["lots"][lot]["sku"] == line["sku"]),
                              key=lambda lot:(state["lots"][lot]["expires"],lot))
            _assert(sum(order["allocations"][lot] for lot in eligible) >= line["qty"], "insufficient")
            needed = line["qty"]
            for lot in eligible:
                amount = min(needed,order["allocations"][lot])
                if amount:
                    selected[lot] = amount
                    order["allocations"][lot] -= amount
                    needed -= amount
        if not any(order["allocations"].values()):
            order["status"] = "shipped"
        state["shipments"][command["shipment"]] = (command["order"], {lot:(qty,0) for lot,qty in selected.items()})
        answer = dict(ok="shipped",shipment=command["shipment"],allocations=_allocation_rows(selected))
    else:
        _assert(command["shipment"] in state["shipments"], "missing")
        _,parts = state["shipments"][command["shipment"]]
        for item in command["lines"]:
            _assert(item["lot"] in parts, "missing")
            qty,returned = parts[item["lot"]]
            _assert(item["qty"] <= qty-returned, "excess")
            parts[item["lot"]] = (qty,returned+item["qty"])
        answer = dict(ok="returned",shipment=command["shipment"],allocations=deepcopy(command["lines"]))
    state["receipts"][command["key"]] = (body,deepcopy(answer))
    state["audit"].append(dict(seq=len(state["audit"])+1,key=command["key"],op=op))
    return answer


def reference(stage_index, payload):
    validate_input(stage_index,payload)
    state: dict[str, Any] = dict(lots={},orders={},shipments={},receipts={},audit=[])
    answers = []
    for command in payload["commands"]:
        trial = deepcopy(state)
        try:
            result = _apply(trial,_parse(command,stage_index))
        except _Failure as error:
            result = {"error":str(error)}
        else:
            state = trial
        answers.append(result)
    return answers


def receive(lot, sku="a", qty=5, expires=10, key=None):
    return dict(op="receive",key=key or "receive-"+lot,lot=lot,sku=sku,qty=qty,expires=expires)


def hold(order="order", lot="lot", qty=2, key="hold"):
    return dict(op="hold",key=key,order=order,lot=lot,qty=qty)


def allocate(order="order", now=0, lines=(("a",2),), key="allocate"):
    return dict(op="allocate",key=key,order=order,now=now,lines=[dict(sku=sku,qty=qty) for sku,qty in lines])


def ship(shipment="shipment", order="order", lines=(("a",1),), key="ship"):
    return dict(op="ship",key=key,shipment=shipment,order=order,lines=[dict(sku=sku,qty=qty) for sku,qty in lines])


def returned(shipment="shipment", lines=(("lot",1),), key="return"):
    return dict(op="return",key=key,shipment=shipment,lines=[dict(lot=lot,qty=qty) for lot,qty in lines])


def release(order="order", key="release"):
    return dict(op="release",key=key,order=order)


STOCK, ORDERS, AUDIT, SHIPMENTS = ({"op":name} for name in ("stock","orders","audit","shipments"))


def _case(stage, identity, requirement, commands):
    payload = dict(commands=commands)
    return dict(id="warehouse-"+identity,requirement=requirement,input=payload,expected=reference(stage,payload))


BASE_PUBLIC_ROWS = (
    ("base-manual", "W0-stock", [receive("lot"),hold(),STOCK,ORDERS,release(),STOCK,AUDIT]),
    ("base-shortfall", "W0-rollback", [receive("lot",qty=1),hold(),STOCK,ORDERS,AUDIT]),
    ("base-retry", "W0-retry", [receive("lot"),hold(),hold(),release(),AUDIT]),
    ("base-shape", "W0-validation", [None,{"op":"receive","key":"x"},{"op":"stock","extra":1},AUDIT]),
)
BASE_PRIVATE_ROWS = (
    ("base-types", "W0-validation", [{"op":[]},{"op":{}},dict(receive("lot"),qty=True),dict(hold(),qty=2.0),receive("\ud800"),AUDIT]),
    ("base-closed-id", "W0-history", [receive("lot"),hold(),release(),hold(key="new"),release(key="again"),ORDERS]),
    ("base-precedence", "W0-validation", [receive("lot"),hold(),hold(lot="missing",key="other"),dict(hold(),qty=False),AUDIT]),
    ("base-manual-expired", "W0-stock", [receive("lot",expires=0),hold(),STOCK,release(),STOCK]),
    ("base-key-conflict", "W0-retry", [receive("lot"),hold(),dict(hold(),qty=1),dict(receive("b"),key="hold"),AUDIT]),
    ("base-receipt", "W0-history", [dict(op="receipt",key="hold"),receive("lot"),hold(),release(),dict(op="receipt",key="hold"),hold(),AUDIT]),
    ("base-failure-key", "W0-rollback", [hold(),dict(op="receipt",key="hold"),receive("lot"),hold(),STOCK,AUDIT]),
    ("base-sorting", "W0-stock", [receive("z"),receive("a",sku="b"),hold("z","z",1,"h1"),hold("a","a",3,"h2"),STOCK,ORDERS]),
)
FIRST_PUBLIC_ROWS = (
    ("allocate-basic", "W1-fefo", [receive("lot"),allocate(),STOCK,ORDERS,AUDIT]),
    ("allocate-shortfall", "W1-atomic", [receive("lot",qty=1),allocate(),STOCK,ORDERS,AUDIT]),
    ("allocate-shape", "W1-validation", [allocate(lines=(("a",1),("a",1))),allocate(now=True),AUDIT]),
    ("allocate-expired", "W1-expiry", [receive("lot",expires=1),allocate(now=2),STOCK,AUDIT]),
)
FIRST_PRIVATE_ROWS = (
    ("fefo-priority", "W1-fefo", [receive("a",expires=8),receive("z",expires=3),allocate(lines=(("a",6),)),STOCK,ORDERS]),
    ("fefo-tie", "W1-fefo", [receive("z"),receive("a"),allocate(),STOCK]),
    ("expiry-equality", "W1-expiry", [receive("lot",expires=5),allocate(now=5),STOCK,AUDIT]),
    ("multi-rollback", "W1-atomic", [receive("lot"),allocate(lines=(("a",1),("b",1))),STOCK,ORDERS,AUDIT,allocate(),AUDIT]),
    ("held-unavailable", "W1-capacity", [receive("lot",qty=3),hold(),allocate(order="other"),STOCK,ORDERS]),
    ("line-order-replay", "W1-retry", [receive("a"),receive("b",sku="b"),allocate(lines=(("b",1),("a",2))),allocate(lines=(("a",2),("b",1))),AUDIT]),
    ("historical-replay", "W1-retry", [receive("lot"),allocate(),release(),allocate(),ORDERS,STOCK,AUDIT]),
    ("different-order", "W1-history", [receive("lot"),allocate(),allocate(order="new",key="second"),STOCK,ORDERS]),
    ("shared-namespace", "W1-history", [receive("lot"),allocate(key="receive-lot"),STOCK,AUDIT]),
    ("whole-validation", "W1-validation", [receive("lot"),allocate(),allocate(lines=(("a",1),("b",False))),AUDIT]),
    ("sku-isolation", "W1-capacity", [receive("lot",sku="b"),allocate(),STOCK,ORDERS]),
    ("zero-available-skip", "W1-capacity", [receive("a",qty=1),receive("b",qty=2),hold(lot="a",qty=1),allocate(order="new"),STOCK]),
)
SECOND_PUBLIC_ROWS = (
    ("ship-basic", "W2-provenance", [receive("lot"),allocate(),ship(),STOCK,ORDERS,SHIPMENTS,AUDIT]),
    ("return-basic", "W2-return", [receive("lot"),allocate(),ship(lines=(("a",2),)),returned(),STOCK,SHIPMENTS]),
    ("ship-shape", "W2-validation", [dict(ship(),lines=[]),returned(lines=(("lot",True),)),AUDIT]),
    ("ship-shortfall", "W2-atomic", [receive("lot"),allocate(),ship(lines=(("a",3),)),STOCK,SHIPMENTS,AUDIT]),
)
SECOND_PRIVATE_ROWS = (
    ("ship-fefo", "W2-provenance", [receive("a",qty=3,expires=8),receive("z",qty=3,expires=4),allocate(lines=(("a",5),)),ship(lines=(("a",4),)),STOCK,ORDERS,SHIPMENTS]),
    ("partial-release", "W2-inherited", [receive("lot"),allocate(lines=(("a",4),)),ship(),release(),returned(),STOCK,ORDERS,SHIPMENTS]),
    ("ship-multi-rollback", "W2-atomic", [receive("lot"),allocate(),ship(lines=(("a",1),("b",1))),STOCK,ORDERS,SHIPMENTS,ship(),AUDIT]),
    ("cumulative-return", "W2-return", [receive("lot"),allocate(),ship(lines=(("a",2),)),returned(),returned(key="second"),returned(key="excess"),STOCK,SHIPMENTS,AUDIT]),
    ("return-rollback", "W2-atomic", [receive("a"),receive("b",sku="b"),allocate(lines=(("a",1),("b",1))),ship(lines=(("a",1),("b",1))),returned(lines=(("a",1),("b",2))),STOCK,SHIPMENTS,returned(lines=(("a",1),)),AUDIT]),
    ("return-original-lot", "W2-provenance", [receive("z",expires=2),receive("a",expires=8),allocate(),ship(),returned(lines=(("z",1),)),STOCK,SHIPMENTS]),
    ("return-no-reopen", "W2-inherited", [receive("lot"),allocate(lines=(("a",1),)),ship(),returned(),ship(shipment="new",key="new"),ORDERS,STOCK]),
    ("return-expired", "W2-inherited", [receive("lot",expires=1),hold(),ship(),returned(),allocate(order="new",now=1),STOCK]),
    ("ship-retry", "W2-retry", [receive("lot"),allocate(),ship(lines=(("a",2),)),returned(),ship(lines=(("a",2),)),STOCK,SHIPMENTS,AUDIT]),
    ("return-retry", "W2-retry", [receive("lot"),allocate(),ship(lines=(("a",2),)),returned(),returned(),returned(key="second"),returned(),STOCK,SHIPMENTS,AUDIT]),
    ("foreign-lot", "W2-provenance", [receive("lot"),receive("other"),allocate(),ship(),returned(lines=(("other",1),)),STOCK,SHIPMENTS]),
    ("new-api-validation", "W2-validation", [receive("lot"),allocate(),ship(),dict(returned(),lines=[{"lot":"lot","qty":1},{"lot":"lot","qty":1}]),dict(ship(),order=[]),{"op":[]},AUDIT]),
    ("shipment-id-precedence", "W2-history", [receive("lot"),allocate(),ship(),ship(order="absent",key="new"),dict(op="receipt",key="ship"),AUDIT]),
    ("ship-line-order", "W2-retry", [receive("a"),receive("b",sku="b"),allocate(lines=(("a",1),("b",1))),ship(lines=(("b",1),("a",1))),ship(lines=(("a",1),("b",1))),returned(lines=(("b",1),("a",1))),returned(lines=(("a",1),("b",1))),AUDIT]),
    ("cross-order-isolation", "W2-provenance", [receive("lot"),allocate(order="a",lines=(("a",1),),key="alloc-a"),allocate(order="b",lines=(("a",2),),key="alloc-b"),ship(order="a",lines=(("a",2),),key="send"),STOCK,ORDERS,SHIPMENTS,ship(order="b",lines=(("a",2),),key="send"),ship(shipment="a",order="a",key="send-a"),STOCK,ORDERS,SHIPMENTS,AUDIT]),
)


BASE_PUBLIC = tuple(_case(0,*row) for row in BASE_PUBLIC_ROWS)
BASE_PRIVATE = tuple(_case(0,*row) for row in BASE_PRIVATE_ROWS)
STAGES = tuple(dict(
    id=f"warehouse-{index+1}",title=("Atomic FEFO order allocation","Provenance-safe fulfillment and returns")[index],
    specification=BASE_SPEC+"\n"+"\n".join(PARTS[:index+1]),
    requirements=tuple(dict.fromkeys(row[1] for row in
        ((BASE_PUBLIC_ROWS+BASE_PRIVATE_ROWS+FIRST_PUBLIC_ROWS+FIRST_PRIVATE_ROWS) if index==0
         else SECOND_PUBLIC_ROWS+SECOND_PRIVATE_ROWS))),
    visible_cases=(BASE_PUBLIC if index==0 else ())+tuple(_case(index,*row) for row in (FIRST_PUBLIC_ROWS,SECOND_PUBLIC_ROWS)[index]),
    hidden_cases=(BASE_PRIVATE if index==0 else ())+tuple(_case(index,*row) for row in (FIRST_PRIVATE_ROWS,SECOND_PRIVATE_ROWS)[index]),
    known_files=known_files(index),
) for index in range(2))
PROJECT = dict(id="warehouse",title="Lot-aware Warehouse Fulfillment",contract=CONTRACT,
               initial_files=INITIAL,allowed_paths=ALLOWED,stages=STAGES)


def control_files(stage_index):
    correct = known_files(stage_index)
    alternate = deepcopy(correct)
    alternate[ALLOWED[2]] += "\n# Equivalent formatting control.\n"
    return {"correct":correct,"equivalent-comment":alternate}


def correct_controls(stage_index):
    return [dict(id=identity,files=files) for identity,files in control_files(stage_index).items()]


def rehearsal_probe(stage_index, slot_index):
    _stage(stage_index)
    if type(slot_index) is not int or not 0 <= slot_index <= 9:
        raise ValueError("invalid probe slot")
    lot = "probe-"+str(slot_index)
    commands = [receive(lot,qty=slot_index+2),allocate(lines=(("a",1),))]
    if stage_index:
        commands += [ship(),returned(lines=((lot,1),)),SHIPMENTS]
    return dict(commands=commands+[STOCK,ORDERS,AUDIT])


def fault_bank(stage_index):
    """Authored semantic mutants, pending source-bound Docker qualification."""
    _stage(stage_index)
    mutations = [
        ("expiry-boundary", ALLOWED[2], "expires>?", "expires>=?", "expiry-equality"),
        ("fefo-direction", ALLOWED[2], "ORDER BY expires,lot", "ORDER BY expires DESC,lot", "fefo-priority"),
        ("fefo-tie", ALLOWED[2], "ORDER BY expires,lot", "ORDER BY expires,lot DESC", "fefo-tie"),
        ("line-identity", ALLOWED[1], 'return sorted((dict(item) for item in value), key=lambda item:item[identifier])',
         'return [dict(item) for item in value]', "line-order-replay"),
        ("held-capacity", ALLOWED[2], 'min(remaining, lot["available"])', 'min(remaining, lot["available"]+lot["reserved"])', "held-unavailable"),
        ("partial-rollback", ALLOWED[2],
         'require(remaining == 0, "insufficient")\n    return {"ok":"allocated"',
         'require(remaining == 0, "insufficient")\n        db.commit()\n    return {"ok":"allocated"', "multi-rollback"),
    ]
    if stage_index:
        mutations += [
            ("ship-provenance",ALLOWED[2],"ORDER BY l.expires,l.lot","ORDER BY l.expires DESC,l.lot","ship-fefo"),
            ("cumulative-return",ALLOWED[2],'part["qty"] - part["returned"]','part["qty"]',"cumulative-return"),
            ("return-reopens",ALLOWED[2],'return {"ok":"returned","shipment":command["shipment"],"allocations":command["lines"]}',
             'db.execute("UPDATE orders SET status=\'reserved\' WHERE oid=(SELECT oid FROM shipments WHERE sid=?)", (command["shipment"],))\n        return {"ok":"returned","shipment":command["shipment"],"allocations":command["lines"]}',"return-no-reopen"),
            ("cross-order-reservation",ALLOWED[2],
             'SELECT a.lot,a.qty FROM allocations a JOIN lots l ON l.lot=a.lot WHERE a.oid=? AND l.sku=? AND a.qty>0 ORDER BY l.expires,l.lot',
             'SELECT a.lot,SUM(a.qty) AS qty FROM allocations a JOIN lots l ON l.lot=a.lot WHERE ? IS NOT NULL AND l.sku=? AND a.qty>0 GROUP BY a.lot ORDER BY l.expires,l.lot',
             "cross-order-isolation"),
        ]
    cases = {case["id"].removeprefix("warehouse-"):case for stage in STAGES[:stage_index+1] for case in stage["hidden_cases"]}
    result = []
    for family,path,before,after,witness in mutations:
        files = known_files(stage_index)
        if files[path].count(before) != 1:
            raise AssertionError("mutation needs one exact source site: "+family)
        files[path] = files[path].replace(before,after)
        result.append(dict(id=f"warehouse-s{stage_index+1}-{family}",family=family,files=files,witness_cases=[deepcopy(cases[witness])]))
    return result
