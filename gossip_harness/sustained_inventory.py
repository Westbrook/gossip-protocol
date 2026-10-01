"""Frozen, staged SQLite inventory repository for quality and continuity studies.

Only initial_files, stage specs and visible cases are worker material. Known
implementations, the independent in-memory reference and hidden cases are host
fixtures. The fixed adapter launches one CLI process for every command; process
restarts are therefore part of every scenario rather than a special mock.
"""
from __future__ import annotations

from copy import deepcopy
from textwrap import dedent
import json


def source(text):
    return dedent(text).lstrip()


ADAPTER = source('''
    import json
    from pathlib import Path
    import sqlite3
    import subprocess
    import sys
    import tempfile

    def solve(payload):
        with tempfile.TemporaryDirectory(prefix="inventory-") as directory:
            db = str(Path(directory) / "inventory.sqlite")
            if "legacy" in payload:
                with sqlite3.connect(db) as connection:
                    connection.execute("CREATE TABLE stock(sku TEXT PRIMARY KEY, qty INTEGER NOT NULL)")
                    connection.executemany("INSERT INTO stock VALUES (?,?)", [(r["sku"], r["qty"]) for r in payload["legacy"]])
                    connection.execute("PRAGMA user_version=1")
            cli = Path(__file__).parent / "inventory_app" / "cli.py"
            results = []
            for command in payload["commands"]:
                run = subprocess.run([sys.executable, str(cli), db, json.dumps(command)], capture_output=True, text=True, timeout=3)
                if run.returncode:
                    raise RuntimeError("Inventory CLI failed")
                results.append(json.loads(run.stdout))
            return results
''')

CLI = source('''
    import json
    import sys
    from service import execute

    if __name__ == "__main__":
        try:
            answer = execute(sys.argv[1], json.loads(sys.argv[2]))
        except Exception:
            answer = {"error": "internal"}
        print(json.dumps(answer, sort_keys=True, separators=(",", ":")))
''')

STORAGE = source('''
    import sqlite3

    def connect(path):
        db = sqlite3.connect(path)
        db.row_factory = sqlite3.Row
        db.execute("BEGIN IMMEDIATE")
        if db.execute("PRAGMA user_version").fetchone()[0] == 1:
            db.execute("ALTER TABLE stock RENAME TO old_stock")
            db.execute("CREATE TABLE stock(sku TEXT PRIMARY KEY, on_hand INTEGER NOT NULL)")
            db.execute("INSERT INTO stock SELECT sku, qty FROM old_stock")
            db.execute("DROP TABLE old_stock")
        db.execute("CREATE TABLE IF NOT EXISTS stock(sku TEXT PRIMARY KEY, on_hand INTEGER NOT NULL)")
        db.execute("CREATE TABLE IF NOT EXISTS reservations(rid TEXT PRIMARY KEY, sku TEXT NOT NULL, remaining INTEGER NOT NULL, status TEXT NOT NULL, expires INTEGER NOT NULL)")
        db.execute("CREATE TABLE IF NOT EXISTS requests(key TEXT PRIMARY KEY, body TEXT NOT NULL, response TEXT NOT NULL)")
        db.execute("CREATE TABLE IF NOT EXISTS events(seq INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT NOT NULL, op TEXT NOT NULL)")
        db.execute("PRAGMA user_version=2")
        db.commit()
        return db

    def reserved(db, sku):
        return db.execute("SELECT COALESCE(SUM(remaining),0) FROM reservations WHERE sku=? AND status='active'", (sku,)).fetchone()[0]

    def stock(db, sku):
        row = db.execute("SELECT on_hand FROM stock WHERE sku=?", (sku,)).fetchone()
        return row[0] if row else 0

    def put_stock(db, sku, quantity):
        db.execute("INSERT INTO stock VALUES (?,?) ON CONFLICT(sku) DO UPDATE SET on_hand=excluded.on_hand", (sku, quantity))

    def snapshot(db):
        goods = []
        for row in db.execute("SELECT sku,on_hand FROM stock ORDER BY sku"):
            held = reserved(db, row["sku"])
            goods.append({"sku": row["sku"], "on_hand": row["on_hand"], "reserved": held, "available": row["on_hand"] - held})
        reservations = [dict(row) for row in db.execute("SELECT rid,sku,remaining,status,expires FROM reservations ORDER BY rid")]
        return {"version": db.execute("PRAGMA user_version").fetchone()[0], "stock": goods, "reservations": reservations}
''')

DOMAIN = source('''
    class DomainError(Exception):
        pass

    FIELDS = {
        "adjust": {"sku", "delta"}, "reserve": {"rid", "sku", "qty", "expires"},
        "cancel": {"rid"}, "capture": {"rid", "qty"}, "release": {"rid", "qty"},
        "expire": {"now"}, "bundle": {"items"}, "reconcile": {"counts"}, "batch": {"commands"},
    }

    def require(condition, error="invalid"):
        if not condition:
            raise DomainError(error)

    def name(value):
        require(type(value) is str and 1 <= len(value) <= 40)

    def integer(value, minimum=0):
        require(type(value) is int and minimum <= value <= 1000)

    def validate(command, keyed=True, stage=3):
        require(type(command) is dict and type(command.get("op")) is str)
        op = command["op"]
        permitted = {"adjust", "reserve", "cancel"}
        if stage >= 2:
            permitted |= {"capture", "release", "expire", "bundle"}
        if stage >= 3:
            permitted |= {"reconcile", "batch"}
        require(op in permitted)
        require(set(command) == FIELDS[op] | {"op"} | ({"key"} if keyed else set()))
        if keyed:
            name(command["key"])
        for field in ("sku", "rid"):
            if field in command:
                name(command[field])
        if op == "adjust":
            integer(command["delta"], -1000)
        if op in {"reserve", "capture", "release"}:
            integer(command["qty"], 1)
        if op == "reserve":
            integer(command["expires"])
        if op == "expire":
            integer(command["now"])
        if op == "bundle":
            require(type(command["items"]) is list and 1 <= len(command["items"]) <= 5)
            for item in command["items"]:
                require(type(item) is dict)
                validate(dict(item, op="reserve"), False, stage)
                require(set(item) == FIELDS["reserve"])
        if op == "reconcile":
            require(type(command["counts"]) is list and 1 <= len(command["counts"]) <= 5)
            names = set()
            for item in command["counts"]:
                require(type(item) is dict and set(item) == {"sku", "on_hand"})
                name(item["sku"])
                integer(item["on_hand"])
                require(item["sku"] not in names)
                names.add(item["sku"])
        if op == "batch":
            require(type(command["commands"]) is list and 1 <= len(command["commands"]) <= 5)
            for item in command["commands"]:
                require(type(item) is dict and item.get("op") != "batch")
                validate(item, False, stage)
''')

SERVICE = source('''
    import json
    from domain import DomainError, require, validate
    from storage import connect, put_stock, reserved, snapshot, stock

    STAGE = 3

    def apply(db, command):
        op = command["op"]
        if op == "adjust":
            sku = command["sku"]
            quantity = stock(db, sku) + command["delta"]
            require(quantity >= reserved(db, sku), "insufficient")
            put_stock(db, sku, quantity)
        elif op == "reserve":
            require(db.execute("SELECT 1 FROM reservations WHERE rid=?", (command["rid"],)).fetchone() is None, "duplicate")
            require(stock(db, command["sku"]) - reserved(db, command["sku"]) >= command["qty"], "insufficient")
            db.execute("INSERT INTO reservations VALUES (?,?,?,'active',?)", (command["rid"], command["sku"], command["qty"], command["expires"]))
        elif op in {"cancel", "capture", "release"}:
            row = db.execute("SELECT * FROM reservations WHERE rid=?", (command["rid"],)).fetchone()
            require(row is not None, "missing")
            require(row["status"] == "active", "closed")
            amount = row["remaining"] if op == "cancel" else command["qty"]
            require(amount <= row["remaining"], "insufficient")
            left = row["remaining"] - amount
            if op == "capture":
                put_stock(db, row["sku"], stock(db, row["sku"]) - amount)
            status = "active" if left else {"cancel": "cancelled", "capture": "captured", "release": "released"}[op]
            db.execute("UPDATE reservations SET remaining=?,status=? WHERE rid=?", (left, status, command["rid"]))
        elif op == "expire":
            db.execute("UPDATE reservations SET remaining=0,status='expired' WHERE status='active' AND expires<=?", (command["now"],))
        elif op == "bundle":
            for item in command["items"]:
                apply(db, dict(item, op="reserve"))
        elif op == "reconcile":
            for item in command["counts"]:
                require(item["on_hand"] >= reserved(db, item["sku"]), "insufficient")
                put_stock(db, item["sku"], item["on_hand"])
        elif op == "batch":
            for item in command["commands"]:
                apply(db, item)

    def execute(path, command):
        db = connect(path)
        try:
            if command == {"op": "snapshot"}:
                return snapshot(db)
            if STAGE >= 3 and command == {"op": "audit"}:
                return [dict(row) for row in db.execute("SELECT seq,key,op FROM events ORDER BY seq")]
            validate(command, stage=STAGE)
            key = command["key"]
            body = json.dumps({k: v for k, v in command.items() if k != "key"}, sort_keys=True, separators=(",", ":"))
            with db:
                prior = db.execute("SELECT body,response FROM requests WHERE key=?", (key,)).fetchone()
                if prior:
                    require(prior["body"] == body, "conflict")
                    return json.loads(prior["response"])
                apply(db, command)
                response = {"ok": True}
                db.execute("INSERT INTO requests VALUES (?,?,?)", (key, body, json.dumps(response)))
                db.execute("INSERT INTO events(key,op) VALUES (?,?)", (key, command["op"]))
                return response
        except DomainError as error:
            return {"error": str(error)}
        finally:
            db.close()
''')

V0_SERVICE = source('''
    from domain import DomainError, require, validate
    from storage import connect, put_stock, snapshot, stock

    def execute(path, command):
        db = connect(path)
        try:
            if command == {"op": "snapshot"}:
                return snapshot(db)
            validate(command, stage=1)
            require(command["op"] == "adjust")
            quantity = stock(db, command["sku"]) + command["delta"]
            require(quantity >= 0, "insufficient")
            with db:
                put_stock(db, command["sku"], quantity)
            return {"ok": True}
        except DomainError as error:
            return {"error": str(error)}
        finally:
            db.close()
''')

V0_STORAGE = source('''
    import sqlite3

    def connect(path):
        db = sqlite3.connect(path)
        db.execute("CREATE TABLE IF NOT EXISTS stock(sku TEXT PRIMARY KEY, on_hand INTEGER NOT NULL)")
        db.execute("PRAGMA user_version=2")
        db.commit()
        return db

    def stock(db, sku):
        row = db.execute("SELECT on_hand FROM stock WHERE sku=?", (sku,)).fetchone()
        return row[0] if row else 0

    def put_stock(db, sku, quantity):
        db.execute("INSERT INTO stock VALUES (?,?) ON CONFLICT(sku) DO UPDATE SET on_hand=excluded.on_hand", (sku, quantity))

    def snapshot(db):
        return {"version": 2, "stock": [{"sku": sku, "on_hand": qty, "reserved": 0, "available": qty} for sku, qty in db.execute("SELECT sku,on_hand FROM stock ORDER BY sku")], "reservations": []}
''')

V0_DOMAIN = source('''
    class DomainError(Exception):
        pass

    def require(condition, error="invalid"):
        if not condition:
            raise DomainError(error)

    def validate(command, stage=1):
        require(type(command) is dict and set(command) == {"op", "key", "sku", "delta"})
        require(command["op"] == "adjust")
        for field in ("key", "sku"):
            require(type(command[field]) is str and 1 <= len(command[field]) <= 40)
        require(type(command["delta"]) is int and -1000 <= command["delta"] <= 1000)
''')

ALLOWED = ("inventory_app/storage.py", "inventory_app/domain.py", "inventory_app/service.py")
INITIAL = {"solution.py": ADAPTER, "inventory_app/cli.py": CLI,
           ALLOWED[0]: V0_STORAGE, ALLOWED[1]: V0_DOMAIN, ALLOWED[2]: V0_SERVICE}

COMMON = '''Maintain the existing SQLite inventory repository using Python standard library only.
Modify only inventory_app/storage.py, domain.py and service.py. Keep module roles:
storage owns connection/schema/query helpers, domain owns typed validation, and
service owns transactional command execution. Fixed solution.py calls the fixed
CLI in a fresh subprocess for every command; same scenario DB persists across
commands. execute(db_path, command) returns JSON; no other output. Inputs are
{"commands":[command,...]} with at most 12 commands. A new DB starts version 2.
Commands are objects with EXACT specified fields. All names (sku,rid,key) are
strings length 1..40. Integer fields EXCLUDE booleans/floats and range 0..1000,
except delta -1000..1000 and qty 1..1000. Unknown operations/fields, missing
fields and incorrect types return {"error":"invalid"}, changing nothing.
Only these command-shape/type errors are invalid; state errors are specified
below. Validate complete shapes before state changes. Successful mutations
return {"ok":true}; failures are atomic and do not consume their idempotency key.
Every top-level mutation has key. Compare the whole command except key as JSON
(with object key order ignored, array order retained). Repeating a successful
key/body returns its original success without further mutation, including after
state changes. A valid, different body under an existing key returns
{"error":"conflict"}. Failed keys can be retried with corrected bodies.
{"op":"snapshot"} returns {"version":2,"stock":[...],"reservations":[...]}.
Stock entries sorted by sku are {sku,on_hand,reserved,available}; reserved is the
sum of remaining across active reservations; available=on_hand-reserved.
Reservation entries sorted by rid are {rid,sku,remaining,status,expires}.
Retain all stock and closed-reservation records. No implicit wall-clock expiry;
expires is a logical integer. Unknown SKUs have stock zero until adjusted.
Ensure on_hand>=reserved>=0 after every successful transaction. Sequential
process restart is tested; concurrent clients/crashes during commit are not.
'''

SPECS = (
    COMMON + '''Stage 1: Preserve adjust/snapshot and add durable reservations and idempotency.
- adjust: {op:"adjust",key,sku,delta}; create/update stock, including delta 0.
  If resulting on_hand is below reserved (or negative), error "insufficient".
- reserve: {op:"reserve",key,rid,sku,qty,expires}; existing rid gives "duplicate"
  even if closed; otherwise insufficient available gives "insufficient".
  Create active reservation of qty, without subtracting on_hand.
- cancel: {op:"cancel",key,rid}; unknown gives "missing", closed gives "closed";
  otherwise remaining=0,status="cancelled", without changing on_hand.
Only these mutations and snapshot are supported at this stage.
''',
    COMMON + '''Stage 2: Preserve stage 1 and add the following mutations.
- capture/release: {op:"capture" or "release",key,rid,qty}; unknown rid ->
  "missing", closed -> "closed", qty>remaining -> "insufficient". Both subtract
  qty from remaining; capture also subtracts qty from on_hand. Partial records
  stay active; final statuses respectively "captured" or "released".
- expire: {op:"expire",key,now}; close every active reservation with expires<=now
  as remaining=0,status="expired". No stock subtraction. Even a no-op succeeds.
- bundle: {op:"bundle",key,items:[{rid,sku,qty,expires},...]}, 1..5 items.
  Behave as reservations in list order, but atomically: any duplicate or shortage
  rolls back all items. Validate every item's complete shape before applying.
''',
    COMMON + '''Stage 3: Preserve stages 1 and 2; add reconciliation, batch, audit and migration.
- reconcile: {op:"reconcile",key,counts:[{sku,on_hand},...]}, 1..5 entries,
  unique sku values (duplicates -> "invalid"). Set absolute physical counts,
  create unknown SKUs. Any count below reserved -> "insufficient" and roll back
  all entries. Unlisted stock is unchanged.
- batch: {op:"batch",key,commands:[inner,...]}, 1..5 mutations from stages 1..3
  except batch. Inner mutations have NO key. No snapshot/audit/nested batch.
  Validate all inner shapes first, then execute in order in ONE transaction;
  any state failure rolls back all writes. The batch has one outer key.
- {op:"audit"} returns entries {seq,key,op}, sorted seq, starting at 1 with no
  gaps; exactly one entry per successful top-level mutation. Retries, failures,
  reads and migration add none. A bundle or batch adds just its outer event.
  Existing successful stage 1/2 mutation types are also audited.
- Optional scenario input legacy:[{sku,qty},...] seeds an existing version-1
  SQLite DB with table stock(sku TEXT PRIMARY KEY,qty INTEGER NOT NULL).
  On first command migrate transactionally to version 2, preserving every
  physical count; no reservations, keys or audit events exist in this legacy
  DB. Legacy rows are valid, unique, nonnegative integers. Migration must remain
  correct over subsequent fresh CLI processes. New DBs still work.
''',
)

_stage_parts = [part.removeprefix(COMMON) for part in SPECS]
SPECS = tuple(COMMON + "\n".join(
    part.replace("Only these mutations and snapshot are supported at this stage.\n", "")
    for part in _stage_parts[:index+1]
) for index in range(3))

# This intentionally independent reference uses copied Python state, not SQL,
# production service source or fixture implementation execution.
def _expected(payload, stage):
    state = {"stock": {row["sku"]: row["qty"] for row in payload.get("legacy", [])}, "reservations": {}, "keys": {}, "events": []}
    # The reference validator below is independent from the shipped DOMAIN.
    fields = {"adjust": {"sku", "delta"}, "reserve": {"rid", "sku", "qty", "expires"}, "cancel": {"rid"}, "capture": {"rid", "qty"}, "release": {"rid", "qty"}, "expire": {"now"}, "bundle": {"items"}, "reconcile": {"counts"}, "batch": {"commands"}}
    permitted = list(fields)[:3 if stage == 1 else 7 if stage == 2 else 9]
    class Error(Exception):
        pass
    def check(condition, code="invalid"):
        if not condition:
            raise Error(code)
    def valid(c, inner=False):
        check(type(c) is dict and type(c.get("op")) is str and c["op"] in permitted)
        op = c["op"]
        check(set(c) == {"op"} | fields[op] | (set() if inner else {"key"}))
        for key in ("key", "sku", "rid"):
            if key in c:
                check(type(c[key]) is str and 0 < len(c[key]) <= 40)
        for key, lower in (("delta", -1000), ("qty", 1), ("expires", 0), ("now", 0)):
            if key in c:
                check(type(c[key]) is int and lower <= c[key] <= 1000)
        if op == "bundle":
            check(type(c["items"]) is list and 1 <= len(c["items"]) <= 5)
            for item in c["items"]:
                check(type(item) is dict and set(item) == fields["reserve"])
                valid({"op": "reserve", **item}, True)
        if op == "reconcile":
            check(type(c["counts"]) is list and 1 <= len(c["counts"]) <= 5)
            seen = set()
            for item in c["counts"]:
                check(type(item) is dict and set(item) == {"sku", "on_hand"})
                check(type(item["sku"]) is str and 0 < len(item["sku"]) <= 40)
                check(type(item["on_hand"]) is int and 0 <= item["on_hand"] <= 1000)
                check(item["sku"] not in seen)
                seen.add(item["sku"])
        if op == "batch":
            check(type(c["commands"]) is list and 1 <= len(c["commands"]) <= 5)
            for child in c["commands"]:
                check(type(child) is dict and child.get("op") != "batch")
                valid(child, True)
    def held(s, sku):
        return sum(r["remaining"] for r in s["reservations"].values() if r["sku"] == sku and r["status"] == "active")
    def apply(s, c):
        op = c["op"]
        if op == "adjust":
            quantity = s["stock"].get(c["sku"], 0) + c["delta"]
            check(quantity >= held(s, c["sku"]), "insufficient")
            s["stock"][c["sku"]] = quantity
        elif op == "reserve":
            check(c["rid"] not in s["reservations"], "duplicate")
            check(s["stock"].get(c["sku"], 0) - held(s, c["sku"]) >= c["qty"], "insufficient")
            s["reservations"][c["rid"]] = {"rid": c["rid"], "sku": c["sku"], "remaining": c["qty"], "status": "active", "expires": c["expires"]}
        elif op in ("cancel", "release", "capture"):
            check(c["rid"] in s["reservations"], "missing")
            row = s["reservations"][c["rid"]]
            check(row["status"] == "active", "closed")
            qty = row["remaining"] if op == "cancel" else c["qty"]
            check(qty <= row["remaining"], "insufficient")
            row["remaining"] -= qty
            if op == "capture":
                s["stock"][row["sku"]] -= qty
            if row["remaining"] == 0:
                row["status"] = {"cancel": "cancelled", "release": "released", "capture": "captured"}[op]
        elif op == "expire":
            for row in s["reservations"].values():
                if row["status"] == "active" and row["expires"] <= c["now"]:
                    row.update(remaining=0, status="expired")
        elif op == "bundle":
            for item in c["items"]:
                apply(s, {"op": "reserve", **item})
        elif op == "reconcile":
            for item in c["counts"]:
                check(item["on_hand"] >= held(s, item["sku"]), "insufficient")
                s["stock"][item["sku"]] = item["on_hand"]
        elif op == "batch":
            for child in c["commands"]:
                apply(s, child)
    results = []
    for command in payload["commands"]:
        if command == {"op": "snapshot"}:
            results.append({"version": 2, "stock": [{"sku": sku, "on_hand": n, "reserved": held(state, sku), "available": n-held(state, sku)} for sku, n in sorted(state["stock"].items())], "reservations": [deepcopy(row) for _, row in sorted(state["reservations"].items())]})
            continue
        if stage == 3 and command == {"op": "audit"}:
            results.append(deepcopy(state["events"]))
            continue
        try:
            valid(command)
            key = command["key"]
            body = json.dumps({k: v for k, v in command.items() if k != "key"}, sort_keys=True)
            if key in state["keys"]:
                check(state["keys"][key] == body, "conflict")
            else:
                revised = deepcopy(state)
                apply(revised, command)
                revised["keys"][key] = body
                revised["events"].append({"seq": len(revised["events"])+1, "key": key, "op": command["op"]})
                state = revised
            results.append({"ok": True})
        except Error as error:
            results.append({"error": str(error)})
    return results


def c(op, key=None, **kw):
    return {"op": op, **({"key": key} if key is not None else {}), **kw}

def item(rid, sku, qty, expires=10):
    return {"rid": rid, "sku": sku, "qty": qty, "expires": expires}

def adj(key, sku="a", delta=10):
    return c("adjust", key, sku=sku, delta=delta)

def res(key, rid="r", sku="a", qty=3, expires=10):
    return c("reserve", key, **item(rid, sku, qty, expires))

S = c("snapshot")
A = c("audit")

# Each stage has eight public and ten held-out scenarios, with distinct inputs.
SCENARIOS = (
    (
        ("stock", [adj("a"), adj("b", delta=-2), S]),
        ("reserve", [adj("a"), res("b"), S]),
        ("cancel", [adj("a"), res("b"), c("cancel", "c", rid="r"), S]),
        ("retry", [adj("a"), adj("a"), S]),
        ("conflict", [adj("a"), adj("a", delta=9), S]),
        ("availability", [adj("a", delta=4), res("b"), res("c", rid="s", qty=2), S]),
        ("failure", [res("a"), adj("b"), res("a"), S]),
        ("types", [adj("a", delta=True), res("b", qty=0), S]),
        ("reserved-floor", [adj("a", delta=8), res("b", qty=6), adj("c", delta=-3), S]),
        ("cancel-retry", [adj("a", delta=9), res("b", qty=5), c("cancel", "c", rid="r"), res("d", rid="s", qty=9), c("cancel", "c", rid="r"), S]),
        ("closed-id", [adj("a", delta=7), res("b", qty=2), c("cancel", "c", rid="r"), res("d", qty=1), S]),
        ("sort", [adj("a", "z", 3), adj("b", "b", 5), res("c", "z", "z", 2), res("d", "a", "b", 1), S]),
        ("zero", [adj("a", "zero", 0), adj("b", "absent", -1), S]),
        ("missing-closed", [c("cancel", "a", rid="none"), adj("b", delta=3), res("c", qty=3), c("cancel", "d", rid="r"), c("cancel", "e", rid="r"), S]),
        ("key-cross-op", [adj("a", delta=6), res("a", qty=2), S]),
        ("full-shape", [dict(adj("a"), unexpected=1), {"op":"adjust","sku":"a","delta":1}, {"op":"snapshot","key":"x"}, S]),
        ("bad-types", [adj("a", delta=1.0), res("b", expires=False), adj("c", sku="", delta=1), S]),
        ("reserve-retry-after-cancel", [adj("a", delta=6), res("b", qty=4), c("cancel", "c", rid="r"), res("b", qty=4), S]),
    ),
    (
        ("partial-capture", [adj("a"), res("b", qty=6), c("capture", "c", rid="r", qty=2), S]),
        ("partial-release", [adj("a"), res("b", qty=6), c("release", "c", rid="r", qty=2), S]),
        ("capture-close", [adj("a", delta=5), res("b", qty=5), c("capture", "c", rid="r", qty=5), S]),
        ("expiry-boundary", [adj("a"), res("b", expires=4), c("expire", "c", now=4), S]),
        ("bundle", [adj("a"), c("bundle", "b", items=[item("r","a",3),item("s","a",4)]), S]),
        ("bundle-atomic", [adj("a", delta=5), c("bundle", "b", items=[item("r","a",3),item("s","a",3)]), S]),
        ("overcapture", [adj("a"), res("b"), c("capture", "c", rid="r", qty=4), S]),
        ("release-close", [adj("a"), res("b"), c("release", "c", rid="r", qty=3), S]),
        ("capture-idempotency", [adj("a", delta=12), res("b", qty=8), c("capture","c",rid="r",qty=3), c("capture","c",rid="r",qty=3), S]),
        ("expire-partial", [adj("a",delta=11), res("b",qty=7,expires=6), c("capture","c",rid="r",qty=2), c("expire","d",now=6), c("capture","e",rid="r",qty=1), S]),
        ("expiry-selectivity", [adj("a",delta=15), res("b","r",qty=3,expires=5), res("c","s",qty=4,expires=6), c("expire","d",now=5), S]),
        ("duplicate-bundle", [adj("a",delta=20), c("bundle","b",items=[item("r","a",2),item("r","a",1)]), S]),
        ("cross-sku-rollback", [adj("a","a",9), adj("b","b",1), c("bundle","c",items=[item("r","a",4),item("s","b",2)]), S]),
        ("bundle-retry", [adj("a",delta=9), c("bundle","b",items=[item("r","a",2),item("s","a",3)]), c("cancel","c",rid="r"), c("bundle","b",items=[item("r","a",2),item("s","a",3)]), S]),
        ("retry-expiry", [adj("a",delta=9), res("b",expires=3), c("expire","c",now=3), res("d","s",expires=3), c("expire","c",now=3), S]),
        ("release-then-capture", [adj("a",delta=8), res("b",qty=7), c("release","c",rid="r",qty=2), c("capture","d",rid="r",qty=5), S]),
        ("validate-whole-bundle", [adj("a",delta=14), c("bundle","b",items=[item("r","a",4),item("s","a",True)]), c("capture","c",rid="none",qty=1), S]),
        ("closed-ops", [adj("a",delta=4), res("b",qty=2), c("release","c",rid="r",qty=2), c("release","d",rid="r",qty=1), c("capture","e",rid="r",qty=1), S]),
    ),
    (
        ("reconcile", [adj("a"), c("reconcile","b",counts=[{"sku":"a","on_hand":7},{"sku":"b","on_hand":2}]), S,A]),
        ("reconcile-floor", [adj("a"),res("b",qty=5),c("reconcile","c",counts=[{"sku":"a","on_hand":4}]),S,A]),
        ("batch", [c("batch","a",commands=[c("adjust",sku="a",delta=7),c("reserve",**item("r","a",3))]),S,A]),
        ("batch-rollback", [c("batch","a",commands=[c("adjust",sku="a",delta=2),c("reserve",**item("r","a",3))]),S,A]),
        ("audit-retry", [adj("a"),res("b"),res("b"),c("cancel","c",rid="r"),A,S]),
        ("migration", [S,adj("a","legacy",2),S,A], [{"sku":"legacy","qty":5}]),
        ("batch-key", [c("batch","a",commands=[c("adjust",sku="a",delta=4)]),c("batch","a",commands=[c("adjust",sku="a",delta=4)]),S,A]),
        ("invalid-batch", [c("batch","a",commands=[adj("inner")]),S,A]),
        ("reconcile-cross-rollback", [adj("a","a",8),adj("b","b",6),res("c","r","b",4),c("reconcile","d",counts=[{"sku":"a","on_hand":1},{"sku":"b","on_hand":3}]),S,A]),
        ("failed-key-recovery", [c("batch","x",commands=[c("adjust",sku="a",delta=4),c("reserve",**item("r","a",5))]),c("batch","x",commands=[c("adjust",sku="a",delta=6),c("reserve",**item("r","a",5))]),S,A]),
        ("batch-mixed", [adj("a",delta=15),res("b",qty=8),c("batch","c",commands=[c("capture",rid="r",qty=2),c("release",rid="r",qty=1),c("reconcile",counts=[{"sku":"a","on_hand":6}])]),S,A]),
        ("batch-late-rollback", [adj("a",delta=12),res("b",qty=9),c("batch","c",commands=[c("capture",rid="r",qty=4),c("adjust",sku="a",delta=-5)]),S,A]),
        ("audit-no-gaps", [adj("a",delta=3),res("b",qty=4),res("c",qty=2),adj("a",delta=1),c("expire","d",now=10),A,S]),
        ("legacy-reserve", [res("a","r","old",4),c("capture","b",rid="r",qty=2),c("reconcile","c",counts=[{"sku":"old","on_hand":3}]),S,A], [{"sku":"old","qty":7},{"sku":"zero","qty":0}]),
        ("legacy-restart", [S,S,c("batch","k",commands=[c("adjust",sku="z",delta=-2),c("reserve",**item("r","b",1))]),S,A], [{"sku":"z","qty":5},{"sku":"b","qty":2}]),
        ("duplicate-counts", [adj("a",delta=13),c("reconcile","b",counts=[{"sku":"a","on_hand":5},{"sku":"a","on_hand":6}]),S,A]),
        ("nested-batch", [c("batch","a",commands=[c("adjust",sku="a",delta=3),c("batch",commands=[c("adjust",sku="b",delta=2)])]),S,A]),
        ("audit-outer-only", [adj("a",delta=16),c("batch","b",commands=[c("bundle",items=[item("r","a",4),item("s","a",5)]),c("expire",now=10)]),c("batch","b",commands=[c("bundle",items=[item("r","a",4),item("s","a",5)]),c("expire",now=10)]),S,A]),
    ),
)

REQUIREMENTS = (
    ("stock", "reserve", "cancel", "idempotency", "atomicity", "typed-validation", "restart"),
    ("partial-capture", "partial-release", "expiry", "bundle-atomicity", "restart"),
    ("reconcile-atomicity", "batch-atomicity", "audit", "migration", "restart"),
)

CASE_REQUIREMENTS = (
    ("stock", "reserve", "cancel", "idempotency", "idempotency", "reserve", "atomicity", "typed-validation",
     "atomicity", "idempotency", "reserve", "restart", "stock", "cancel", "idempotency", "typed-validation", "typed-validation", "idempotency"),
    ("partial-capture", "partial-release", "partial-capture", "expiry", "bundle-atomicity", "bundle-atomicity", "partial-capture", "partial-release",
     "restart", "expiry", "expiry", "bundle-atomicity", "bundle-atomicity", "bundle-atomicity", "expiry", "partial-capture", "bundle-atomicity", "partial-release"),
    ("reconcile-atomicity", "reconcile-atomicity", "batch-atomicity", "batch-atomicity", "audit", "migration", "batch-atomicity", "batch-atomicity",
     "reconcile-atomicity", "batch-atomicity", "batch-atomicity", "batch-atomicity", "audit", "migration", "restart", "reconcile-atomicity", "batch-atomicity", "audit"),
)

def _cases(stage, rows, hidden):
    cases = []
    for index, row in enumerate(rows):
        name, commands, *legacy = row
        payload = {"commands": deepcopy(commands)}
        if legacy:
            payload["legacy"] = deepcopy(legacy[0])
        cases.append({"id": f"inventory-s{stage}-{'h' if hidden else 'v'}-{name}", "input": payload, "expected": _expected(payload, stage), "requirement": CASE_REQUIREMENTS[stage-1][index + (8 if hidden else 0)]})
    return tuple(cases)

STAGES = tuple({
    "id": f"inventory-stage-{stage}", "spec": SPECS[stage-1], "requirements": REQUIREMENTS[stage-1],
    "visible_cases": _cases(stage, SCENARIOS[stage-1][:8], False),
    "hidden_cases": _cases(stage, SCENARIOS[stage-1][8:], True),
    "known_files": {**INITIAL, ALLOWED[0]: STORAGE, ALLOWED[1]: DOMAIN, ALLOWED[2]: SERVICE.replace("STAGE = 3", f"STAGE = {stage}")},
} for stage in (1,2,3))

PROJECT = {"id": "inventory", "title": "Persistent Inventory and Reservations", "initial_files": INITIAL,
           "allowed_paths": ALLOWED, "stages": STAGES}

# Realistic shortcuts: failing to wrap writes in one transaction; strict '<'
# expiry boundary. Both satisfy basic adjust/reserve checks, fail later scenarios.
MUTANTS = {
    "autocommit_breaks_atomicity": {**STAGES[2]["known_files"], ALLOWED[0]: STORAGE.replace("db.commit()\n    return db", "db.commit()\n    db.isolation_level = None\n    return db")},
    "exclusive_expiry_boundary": {**STAGES[2]["known_files"], ALLOWED[2]: SERVICE.replace("expires<=?", "expires<?")},
}
