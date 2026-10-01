"""Versioned multi-module calendar benchmark with final-state atomic exchanges.

This fixture starts from the mature, trusted calendar implementation. The oracle
is an independent copy-on-write state machine, never an import of candidate code.
The frozen predecessor module supplies immutable source and regression cases.
"""
from __future__ import annotations

from copy import deepcopy
import json
from textwrap import dedent

from . import verification_calendar as predecessor

VERSION = "calendar-exchange-v1"
ALLOWED = predecessor.ALLOWED
INITIAL = deepcopy(predecessor.STAGES[3]["known_files"])

EXTRA_VALIDATION = dedent('''\
    def exchange_shape(moves):
        require(type(moves) is list and len(moves) <= 4)
        seen = set()
        for move in moves:
            require(type(move) is dict and set(move) == {"rid", "start", "end"})
            name(move["rid"])
            integer(move["start"])
            integer(move["end"])
            require(move["start"] < move["end"] and move["rid"] not in seen)
            seen.add(move["rid"])
        return seen

    old_validate = validate
    def validate(command, stage, inner=False):
        if type(command) is not dict or command.get("op") not in ("exchange", "settle"):
            return old_validate(command, stage, inner)
        op = command["op"]
        require(op != "settle" or EXTENSION >= 1)
        fields = {"op", "moves"} | (set() if inner else {"key"})
        if op == "settle":
            fields |= {"cancels", "limit"}
        require(set(command) == fields)
        if not inner:
            name(command["key"])
        moved = exchange_shape(command["moves"])
        if op == "exchange":
            require(bool(moved))
        else:
            require(type(command["cancels"]) is list and len(command["cancels"]) <= 4)
            cancelled = set()
            for rid in command["cancels"]:
                name(rid)
                require(rid not in cancelled and rid not in moved)
                cancelled.add(rid)
            integer(command["limit"])
            require(command["limit"] <= 4)
''')

EXTRA_SERVICE = dedent('''\
    def exchange(db, moves):
        # Read and check every target before evaluating the simultaneous result.
        revised = []
        for move in moves:
            row = active(db, move["rid"])
            require(row["status"] == "booked", "held")
            revised.append(dict(row, start=move["start"], end=move["end"]))
        for row in revised:
            require(allowed_by_policy(db, row, row["rid"]), "policy")
        targets = {row["rid"] for row in revised}
        final = [dict(row) for row in db.execute("SELECT * FROM bookings WHERE status IN ('booked','held')") if row["rid"] not in targets] + revised
        for row in revised:
            require(not any(other["rid"] != row["rid"] and other["resource"] == row["resource"] and row["start"] < other["end"] and other["start"] < row["end"] for other in final), "conflict")
        for row in revised:
            db.execute("UPDATE bookings SET start=?,end=? WHERE rid=?", (row["start"], row["end"], row["rid"]))
        return {"ok": True}

    old_apply = apply
    def apply(db, command):
        if command["op"] == "exchange":
            return exchange(db, command["moves"])
        if command["op"] == "settle":
            for rid in command["cancels"]:
                old_apply(db, {"op":"cancel", "rid":rid})
            exchange(db, command["moves"])
            admitted = []
            for _ in range(command["limit"]):
                rid = old_apply(db, {"op":"admit"})["admitted"]
                if rid is None:
                    break
                admitted.append(rid)
            return {"ok": True, "admitted": admitted}
        return old_apply(db, command)
''')


def known_files(stage_index):
    """Author-written SQLite implementation used for fixture qualification."""
    if type(stage_index) is not int or stage_index not in (0, 1):
        raise ValueError("outside_input_domain")
    domain = predecessor.DOMAIN + f"\nEXTENSION = {stage_index}\n" + EXTRA_VALIDATION
    service = predecessor.SERVICE + "\n" + EXTRA_SERVICE
    if stage_index == 1:
        service = service.replace(
            '        if command == {"op":"snapshot"}:',
            '        if type(command) is dict and command.get("op") == "receipt":\n'
            '            from domain import name\n'
            '            require(set(command) == {"op", "key"})\n'
            '            name(command["key"])\n'
            '            row = db.execute("SELECT body,response FROM requests WHERE key=?", (command["key"],)).fetchone()\n'
            '            return {"found":False} if row is None else {"found":True,"op":json.loads(row["body"])["op"],"response":json.loads(row["response"])}\n'
            '        if command == {"op":"snapshot"}:',
        )
    return {**INITIAL, ALLOWED[1]: domain, ALLOWED[2]: service}


COMMON = '''Maintain the supplied mature SQLite resource-booking repository. This is
an extension of the fully working predecessor, not a rewrite. Python standard
library only. Only calendar_app/storage.py, calendar_app/domain.py and
calendar_app/service.py may change. The fixed adapter starts a new CLI process
for EACH command using one scenario database: receipts/state must survive restarts.
All predecessor operations, half-open intervals, explicit expiry, fencing,
policy.json limits, legacy migration, typed validation, atomic batch behavior,
FIFO feasible admission, audit, and original-response idempotency still apply.
Existing policy is max_duration=12, max_active_per_user=2; READ policy.json.
Commands have exactly the fields stated. Names are strings length1..32; times
are exact integers0..1000 (no bool/float); start<end. Validate the complete command
shape before any state checks. Invalid commands return {error:'invalid'}.
Every successful top-level mutation records exactly one durable request and one
contiguous audit event. Body identity excludes key; object order ignored, array
order significant. Exact retries return the ORIGINAL response and do not execute
again; different valid body under a successful key returns key_conflict.
Failures roll back ALL changes and consume neither key nor audit sequence.
Read commands snapshot/audit have no side effects. Existing closed IDs are retained.
Input envelope, bounds, legacy format and full predecessor behavior follow below.
'''

PARTS = (
'''Milestone1: Add final-state atomic exchanges.
exchange {op:'exchange',key,moves:[{rid,start,end},...]}, 1..4 moves, unique rid.
The operation changes times of already BOOKED records simultaneously: legal swaps
and 3/4-way cycles must succeed even when sequential moves would conflict.
Validate all move shapes/duplicates first. Check target states in array order:
missing row->missing, inactive row->closed, held row->held. Then check ALL revised
rows against duration and active-user limits (exclude that row itself from count);
policy errors precede every overlap check. User/resource/status/token/expiry stay
unchanged. Finally check each moved row against the entire final active state,
including other moved rows and unchanged BOOKED/HELD rows. Overlap->conflict;
adjacent endpoints are legal. Existing grandfathered overlaps not involving any
moved row are left alone. Holds do not expire implicitly. Success {ok:true}.
exchange is legal inside existing batch (without key); no partial updates survive
any later batch failure. Existing move retains its original sequential semantics.
''',
'''Milestone2: Add settlement with bounded, atomic waitlist draining and receipt reads.
settle {op:'settle',key,cancels:[rid,...],moves:[{rid,start,end},...],limit}.
cancels/moves each 0..4 entries. Every rid is unique across BOTH lists; duplicates
are invalid. limit is exact integer0..4. Empty cancels/moves are legal, including
limit0. Validate ALL shapes first. Apply cancellations in listed order using
existing cancel semantics (including waiting IDs); then apply one simultaneous
exchange of moves using milestone1 semantics. Finally repeat existing admit up
to limit times: on each iteration choose earliest CURRENTLY FEASIBLE waiting row,
skipping blocked older rows, rechecking policy and capacity after each admission.
Stop when no feasible row remains. Return {ok:true,admitted:[rid,...]} in admission
order. Cancellation, exchange, admissions, request and one outer audit event are
ONE transaction. No automatic admission is added to ordinary cancel/move/expire.
settle is allowed inside batch without key. On any later failure, everything
rolls back, including cancelled/changed rows and waitlist states. Exact retries
return the original admitted list even after intervening cancel/enqueue/settle.
receipt {op:'receipt',key} is a NEW READ command; key follows ordinary name rules.
For a successful mutation key return {found:true,op:<original operation>,response:
<original response>}; an unused/failed key returns {found:false}. Reads never
consume keys, audit events, or change state; a receipt key may later be used for
a mutation. Receipt is invalid inside batch and extra/missing fields are invalid.
''',
)
# Preserve the predecessor's full behavior description, but not its progression.
BASE_SPEC = predecessor.SPECS[3].replace(
    "policy.json is dormant until milestone4; do not enforce its limits earlier.",
    "All four predecessor milestones below are already implemented and active.",
).replace("only milestone\n4", "both extension milestones")
SPECS = tuple(COMMON + "\n".join(PARTS[:i + 1]) + "\nPREDECESSOR CONTRACT:\n" + BASE_SPEC for i in range(2))


def validate_input(stage_index, payload):
    """The predecessor envelope accepts malformed commands; future APIs are excluded."""
    if type(stage_index) is not int or stage_index not in (0, 1):
        raise ValueError("outside_input_domain")
    predecessor.validate_input(3, payload)
    if stage_index == 0:
        def walk(value):
            if type(value) is dict:
                if type(value.get("op")) is str and value["op"] in ("settle", "receipt"):
                    raise ValueError("outside_input_domain")
                for child in value.values():
                    walk(child)
            elif type(value) is list:
                for child in value:
                    walk(child)
        walk(payload["commands"])
    if len(json.dumps(_reference(stage_index, payload), sort_keys=True, ensure_ascii=True, separators=(",", ":"), allow_nan=False).encode()) > 32768:
        raise ValueError("outside_input_domain")


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
    fields={"book":item_fields,"cancel":{"rid"},"move":{"rid","start","end"},"bundle":{"items"},"hold":item_fields|{"now","ttl"},"renew":{"rid","token","now","ttl"},"confirm":{"rid","token","now"},"expire":{"now"},"batch":{"commands"},"enqueue":item_fields,"admit":set(),"exchange":{"moves"},"settle":{"cancels","moves","limit"}}
    def validate(command,inner=False):
        need(type(command) is dict and type(command.get("op")) is str)
        op=command["op"]
        need(op in fields and (op != "settle" or stage_index >= 1))
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
        if op in ("exchange", "settle"):
            need(type(command["moves"]) is list and len(command["moves"]) <= 4)
            ids = set()
            for move in command["moves"]:
                need(type(move) is dict and set(move) == {"rid", "start", "end"})
                need(type(move["rid"]) is str and 1 <= len(move["rid"]) <= 32)
                need(type(move["start"]) is int and type(move["end"]) is int and 0 <= move["start"] < move["end"] <= 1000)
                need(move["rid"] not in ids)
                ids.add(move["rid"])
            if op == "exchange":
                need(bool(ids))
            else:
                need(type(command["cancels"]) is list and len(command["cancels"]) <= 4)
                for rid in command["cancels"]:
                    need(type(rid) is str and 1 <= len(rid) <= 32 and rid not in ids)
                    ids.add(rid)
                need(type(command["limit"]) is int and 0 <= command["limit"] <= 4)
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
        return False or (row["end"]-row["start"]<=12 and sum(live(b) and b["user"]==row["user"] and rid!=exclude for rid,b in s["bookings"].items())<2)
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
    def exchange(s, moves):
        targets = []
        for move in moves:
            original = get_active(s, move["rid"])
            need(original["status"] == "booked", "held")
            targets.append({**original, "start":move["start"], "end":move["end"]})
        for row in targets:
            need(policy_ok(s, row, row["rid"]), "policy")
        simulated = deepcopy(s)
        for row in targets:
            simulated["bookings"][row["rid"]] = row
        for row in targets:
            need(not conflict(simulated, row, row["rid"]), "conflict")
        for row in targets:
            s["bookings"][row["rid"]] = row
        return {"ok":True}
    def apply(s,c):
        op=c["op"]
        if op == "exchange":
            return exchange(s, c["moves"])
        if op == "settle":
            for rid in c["cancels"]:
                apply(s, {"op":"cancel", "rid":rid})
            exchange(s, c["moves"])
            admitted = []
            for _ in range(c["limit"]):
                rid = apply(s, {"op":"admit"})["admitted"]
                if rid is None:
                    break
                admitted.append(rid)
            return {"ok":True, "admitted":admitted}
        if op=="book":
            add(s,c)
        elif op=="cancel":
            if c["rid"] in s["bookings"]:
                get_active(s,c["rid"])["status"]="cancelled"
            else:
                rows=[w for w in s["waiting"] if w["rid"]==c["rid"]]
                need(bool(rows),"missing")
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
        if command=={"op":"audit"}:
            answers.append(deepcopy(state["events"]))
            continue
        try:
            if stage_index >= 1 and type(command) is dict and command.get("op") == "receipt":
                need(set(command) == {"op", "key"})
                need(type(command["key"]) is str and 1 <= len(command["key"]) <= 32)
                record = state["keys"].get(command["key"])
                answers.append({"found":False} if record is None else {"found":True,"op":json.loads(record[0])["op"],"response":deepcopy(record[1])})
                continue
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



def reference(stage_index, payload):
    validate_input(stage_index, payload)
    return _reference(stage_index, payload)

command = predecessor.command
item = predecessor.item
book = predecessor.book
hold = predecessor.hold
enqueue = predecessor.enqueue
SNAP = {"op": "snapshot"}
AUDIT = {"op": "audit"}


def move(rid, start, end):
    return {"rid": rid, "start": start, "end": end}


def exchange(key="x", *moves):
    return command("exchange", key, moves=list(moves))


def settle(key="s", *, cancels=(), moves=(), limit=4):
    return command("settle", key, cancels=list(cancels), moves=list(moves), limit=limit)


def receipt(key):
    return command("receipt", key)


def scenario(commands, legacy=None):
    result = {"commands": deepcopy(commands)}
    if legacy is not None:
        result["legacy"] = deepcopy(legacy)
    return result


FIRST = (
    ("single", "exchange-final-state", [book(), exchange("x", move("a", 8, 12)), SNAP, AUDIT]),
    ("two-way-swap", "exchange-final-state", [book("a", start=0, end=4), book("b", rid="b", start=4, end=8), exchange("x", move("a", 4, 8), move("b", 0, 4)), SNAP]),
    ("adjacency", "exchange-capacity", [book("a", start=0, end=3), book("b", rid="b", start=6, end=9), exchange("x", move("a", 3, 6)), SNAP]),
    ("external-conflict", "exchange-capacity", [book("a", start=0, end=3), book("b", rid="b", start=6, end=9), exchange("x", move("a", 5, 8)), SNAP, AUDIT]),
    ("held-target", "exchange-fencing", [hold(), exchange("x", move("a", 8, 12)), command("confirm", "c", rid="a", token=1, now=1), SNAP]),
    ("duplicate-target", "exchange-validation", [book(), exchange("x", move("a", 8, 12), move("a", 14, 18)), SNAP]),
    ("batch-success", "exchange-atomicity", [book(), command("batch", "b", commands=[command("exchange", moves=[move("a", 8, 12)]), command("book", **item("b"))]), SNAP, AUDIT]),
    ("retry", "exchange-receipts", [book(), exchange("x", move("a", 8, 12)), exchange("y", move("a", 14, 18)), exchange("x", move("a", 8, 12)), SNAP, AUDIT]),
    ("single-policy", "exchange-policy", [book(), exchange("x", move("a", 0, 13)), SNAP, AUDIT]),
    ("three-cycle", "exchange-final-state", [book("a", start=0, end=3), book("b", rid="b", start=3, end=6), book("c", rid="c", start=6, end=9), exchange("x", move("a", 3, 6), move("b", 6, 9), move("c", 0, 3)), SNAP, AUDIT]),
    ("four-cycle", "exchange-final-state", [book("a", start=0, end=2), book("b", rid="b", start=2, end=4), book("c", rid="c", start=4, end=6), book("d", rid="d", start=6, end=8), exchange("x", move("d", 0, 2), move("a", 2, 4), move("b", 4, 6), move("c", 6, 8)), SNAP]),
    ("internal-collision", "exchange-capacity", [book("a", start=0, end=3), book("b", rid="b", start=3, end=6), exchange("x", move("a", 10, 14), move("b", 12, 16)), SNAP, AUDIT]),
    ("late-policy", "exchange-policy", [book("a", resource="r1"), book("b", rid="b", resource="r2"), exchange("x", move("a", 10, 14), move("b", 0, 13)), SNAP, AUDIT]),
    ("policy-before-conflict", "exchange-policy", [book("a", start=0, end=3), book("b", rid="b", resource="r2"), book("c", rid="c", start=8, end=11), exchange("x", move("a", 8, 11), move("b", 0, 13)), SNAP, AUDIT]),
    ("fencing-history", "exchange-fencing", [hold(), command("renew", "r", rid="a", token=1, now=1, ttl=7), command("confirm", "c", rid="a", token=2, now=2), exchange("x", move("a", 8, 12)), SNAP, command("renew", "z", rid="a", token=1, now=3, ttl=4)]),
    ("late-batch-failure", "exchange-atomicity", [book(), command("batch", "b", commands=[command("exchange", moves=[move("a", 8, 12)]), command("cancel", rid="missing")]), SNAP, AUDIT, exchange("b", move("a", 14, 18)), SNAP]),
    ("grandfathered-user-cap", "exchange-policy", [exchange("x", move("a", 10, 12)), SNAP, AUDIT], [item("a", resource="r1", user="u"), item("b", resource="r2", user="u"), item("c", resource="r3", user="u")]),
    ("unrelated-grandfather-overlap", "exchange-final-state", [exchange("x", move("a", 10, 12)), SNAP], [item("a", resource="other"), item("b"), item("c")]),
    ("closed-before-missing", "exchange-validation", [book(), command("cancel", "c", rid="a"), exchange("x", move("a", 8, 12), move("missing", 14, 18)), SNAP, AUDIT]),
    ("shape-before-state", "exchange-validation", [exchange("x", move("missing", 0, 3), move("a", True, 4)), exchange("y", move("missing", 0, 3), move("missing", 4, 8)), SNAP, AUDIT]),
    ("unexpired-capacity", "exchange-fencing", [hold("h", ttl=1, start=8, end=12), book("b", rid="b", start=0, end=4), exchange("x", move("b", 8, 12)), command("expire", "e", now=1), exchange("x", move("b", 8, 12)), SNAP, AUDIT]),
    ("array-body-identity", "exchange-receipts", [book("a", resource="r1"), book("b", rid="b", resource="r2"), exchange("x", move("a", 8, 12), move("b", 14, 18)), exchange("x", move("b", 14, 18), move("a", 8, 12)), SNAP, AUDIT]),
    ("empty-and-malformed", "exchange-validation", [exchange("x"), command("exchange", "x", moves={}), command("exchange", "x", moves=[{"rid": "a", "start": 1, "end": 4, "extra": 1}]), exchange("x", move("a", 1, 4.0)), SNAP, AUDIT]),
)
SECOND = (
    ("cancel-admit", "settlement-transaction", [book(), enqueue("q", rid="q"), settle(cancels=["a"]), SNAP, AUDIT]),
    ("move-admit", "settlement-transaction", [book(), enqueue("q", rid="q"), settle(moves=[move("a", 10, 14)]), SNAP]),
    ("drain-limit", "settlement-drain", [enqueue("q", rid="q", resource="q"), enqueue("r", rid="r", resource="r"), settle(limit=1), SNAP, settle("t", limit=1), SNAP]),
    ("empty-receipt", "settlement-receipts", [receipt("s"), settle(limit=0), receipt("s"), receipt("missing"), AUDIT]),
    ("receipt-retry", "settlement-receipts", [enqueue("q", rid="q"), settle(), receipt("s"), settle(), SNAP, AUDIT]),
    ("invalid-overlap", "settlement-validation", [book(), settle(cancels=["a"], moves=[move("a", 8, 12)]), SNAP, receipt("s")]),
    ("batch-settle", "settlement-atomicity", [book(), enqueue("q", rid="q"), command("batch", "b", commands=[command("settle", cancels=["a"], moves=[], limit=1), command("cancel", rid="q")]), SNAP, receipt("b"), AUDIT]),
    ("no-implicit-drain", "settlement-transaction", [book(), enqueue("q", rid="q"), command("cancel", "c", rid="a"), SNAP, settle(limit=0), SNAP, settle("t", limit=1), SNAP]),
    ("skip-blocked-head", "settlement-drain", [book(), enqueue("q", rid="q"), enqueue("r", rid="r", resource="other"), settle(), SNAP, AUDIT]),
    ("retry-after-enrollment", "settlement-receipts", [enqueue("q", rid="q"), settle(), command("cancel", "c", rid="q"), enqueue("r", rid="r"), settle(), receipt("s"), SNAP, AUDIT]),
    ("dynamic-user-feasibility", "settlement-drain", [book("b", rid="b", resource="b", user="u"), enqueue("q", rid="q", resource="q", user="u"), enqueue("r", rid="r", resource="r", user="u"), enqueue("t", rid="t", resource="t", user="v"), settle(), SNAP]),
    ("dynamic-resource-feasibility", "settlement-drain", [enqueue("q", rid="q", start=0, end=4), enqueue("r", rid="r", start=2, end=6), enqueue("t", rid="t", start=4, end=8), settle(), SNAP, AUDIT]),
    ("late-missing-cancel", "settlement-atomicity", [book(), enqueue("q", rid="q"), settle(cancels=["a", "missing"]), SNAP, receipt("s"), AUDIT, settle(cancels=["a"]), receipt("s")]),
    ("cancel-then-policy-failure", "settlement-atomicity", [book("a", resource="a"), book("b", rid="b", resource="b"), enqueue("q", rid="q", resource="a"), settle(cancels=["a"], moves=[move("b", 0, 13)]), SNAP, receipt("s"), AUDIT]),
    ("outer-rollback-admissions", "settlement-atomicity", [book(), enqueue("q", rid="q"), command("batch", "b", commands=[command("settle", cancels=["a"], moves=[], limit=4), command("cancel", rid="missing")]), SNAP, receipt("b"), AUDIT, settle(cancels=["a"]), SNAP]),
    ("queue-cancellation", "settlement-transaction", [enqueue("q", rid="q"), enqueue("r", rid="r"), settle(cancels=["q"]), SNAP, AUDIT]),
    ("read-is-not-mutation", "settlement-receipts", [receipt("a"), book("a"), receipt("a"), command("batch", "b", commands=[receipt("a")]), SNAP, AUDIT]),
    ("receipt-types", "settlement-validation", [command("receipt", "s", extra=True), {"op": "receipt"}, command("receipt", True), settle(limit=True), settle(limit=5), settle(cancels=["x", "x"]), receipt("s"), AUDIT]),
    ("drain-empty-retry", "settlement-receipts", [settle(), enqueue("q", rid="q"), settle(), receipt("s"), settle("t"), SNAP, AUDIT]),
    ("move-swap-and-vacancy", "settlement-transaction", [book("a", start=0, end=3), book("b", rid="b", start=3, end=6), enqueue("q", rid="q", start=0, end=3), settle(moves=[move("a", 3, 6), move("b", 6, 9)]), SNAP, receipt("s")]),
    ("key-conflict-keeps-receipt", "settlement-receipts", [settle(limit=0), settle(limit=1), receipt("s"), SNAP, AUDIT]),
    ("shape-before-cancellation", "settlement-validation", [book(), settle(cancels=["a"], moves=[move("missing", False, 3)]), SNAP, receipt("s"), AUDIT]),
    ("fence-survives-settlement", "settlement-transaction", [hold("h", resource="held", ttl=3), book("b", rid="b"), enqueue("q", rid="q"), settle(cancels=["b"]), command("renew", "r", rid="a", token=1, now=1, ttl=6), SNAP, AUDIT]),
    ("cancel-frees-policy-cap", "settlement-drain", [book("a", resource="a", user="u"), book("b", rid="b", resource="b", user="u"), enqueue("q", rid="q", resource="q", user="u"), enqueue("r", rid="r", resource="r", user="u"), settle(cancels=["b"]), SNAP, AUDIT]),
)


def _cases(stage_index, rows, private):
    result = []
    for name, requirement, commands, *legacy in rows:
        payload = scenario(commands, legacy[0] if legacy else None)
        result.append({"id": f"calx-s{stage_index + 1}-{'h' if private else 'v'}-{name}",
                       "requirement": requirement, "input": payload,
                       "expected": reference(stage_index, payload)})
    return tuple(result)


# Retain every predecessor scenario at the mature policy level.
# Recompute expectations at the mature policy level, rather than copying outputs
# from earlier releases that intentionally did not yet enforce the policy.
_BASE_PUBLIC_ROWS = tuple(row for stage in predecessor.SCENARIOS for row in stage[:6])
_BASE_PRIVATE_ROWS = tuple(row for stage in predecessor.SCENARIOS for row in stage[6:])
BASE_PUBLIC = tuple({**case, "id": case["id"].replace("calx-s1-v", "calx-base-v"),
                     "requirement": "baseline-regressions"}
                    for case in _cases(0, _BASE_PUBLIC_ROWS, False))
BASE_PRIVATE = tuple({**case, "id": case["id"].replace("calx-s1-h", "calx-base-h"),
                      "requirement": "baseline-regressions"}
                     for case in _cases(0, _BASE_PRIVATE_ROWS, True))
STAGES = (
    {"id": "calendar-exchange-stage-1", "title": "Simultaneous reservation exchanges",
     "specification": SPECS[0],
     "requirements": ("baseline-regressions", "exchange-final-state", "exchange-capacity", "exchange-fencing", "exchange-validation", "exchange-atomicity", "exchange-policy", "exchange-receipts"),
     "visible_cases": BASE_PUBLIC + _cases(0, FIRST[:9], False),
     "hidden_cases": BASE_PRIVATE + _cases(0, FIRST[9:], True), "known_files": known_files(0)},
    {"id": "calendar-exchange-stage-2", "title": "Atomic settlement, admission and durable receipts",
     "specification": SPECS[1],
     "requirements": ("settlement-transaction", "settlement-drain", "settlement-receipts", "settlement-validation", "settlement-atomicity"),
     "visible_cases": _cases(1, SECOND[:8], False),
     "hidden_cases": _cases(1, SECOND[8:], True), "known_files": known_files(1)},
)
PROJECT = {"id": "calendar-exchange", "title": "Transactional Calendar Exchange",
           "initial_files": INITIAL, "allowed_paths": ALLOWED, "stages": STAGES}


def rehearsal_probe(stage_index, index=0):
    """Distinct, bounded inputs; no private case is used to construct probes."""
    if type(index) is not int or not 0 <= index <= 30:
        raise ValueError("probe index outside supported range")
    start = 30 + 3 * index
    commands = [book("p", rid="probe", start=start, end=start + 2)]
    if stage_index == 0:
        commands += [exchange("px", move("probe", start + 3, start + 5)), SNAP, AUDIT]
    elif stage_index == 1:
        commands += [enqueue("pq", rid="probe-wait", start=start, end=start + 2), settle("ps", cancels=["probe"], limit=1), receipt("ps"), SNAP, AUDIT]
    else:
        raise ValueError("outside_input_domain")
    return scenario(commands)


def control_files(stage_index):
    """Negative controls: same semantics with exact and formatting-only source."""
    correct = known_files(stage_index)
    equivalent = {name: ("\n" + text if name in ALLOWED else text) for name, text in correct.items()}
    return {"correct": correct, "equivalent-whitespace": equivalent}


def fault_bank(stage_index):
    """Predeclared semantic defects; witness cases remain host-only evidence.

    Public survival must be measured by the qualifying runner, not assumed from
    family labels. The first six are designed around unshown interactions; the
    final two target settlement-only behavior. No fault source reaches a model.
    """
    files = known_files(stage_index)
    service = files[ALLOWED[2]]
    cases = {case["id"].split("-h-", 1)[-1]: case
             for stage in STAGES[:stage_index + 1] for case in stage["hidden_cases"]}
    specifications = [
        ("three-way-omission", "exchange-cycle", "    revised = []\n", "    if len(moves) == 3:\n        moves = moves[:2]\n    revised = []\n", "three-cycle"),
        ("internal-overlap", "final-state-capacity", 'other["rid"] != row["rid"] and other["resource"]', 'other["rid"] not in targets and other["resource"]', "internal-collision"),
        ("last-policy-skipped", "final-state-policy", '    for row in revised:\n        require(allowed_by_policy', '    for row in revised[:1]:\n        require(allowed_by_policy', "late-policy"),
        ("fence-reset", "fencing-preservation", 'UPDATE bookings SET start=?,end=? WHERE rid=?", (row["start"], row["end"], row["rid"])', 'UPDATE bookings SET start=?,end=?,token=0 WHERE rid=?", (row["start"], row["end"], row["rid"])', "fencing-history"),
        ("exchange-commits-early", "nested-transaction", '    return {"ok": True}\n\nold_apply = apply', '    db.commit()\n    return {"ok": True}\n\nold_apply = apply', "late-batch-failure"),
        ("grandfather-cap-ignored", "policy-revalidation", 'require(allowed_by_policy(db, row, row["rid"]), "policy")', 'require(row["end"] - row["start"] <= policy()["max_duration"], "policy")', "grandfathered-user-cap"),
    ]
    if stage_index == 1:
        specifications += [
            ('queue-head-blocking', 'feasible-fifo', '        for _ in range(command["limit"]):\n            rid = old_apply', '        for _ in range(command["limit"]):\n            head = db.execute("SELECT * FROM waiting WHERE status=\'waiting\' ORDER BY seq LIMIT 1").fetchone()\n            if head is not None and (not allowed_by_policy(db, head) or taken(db, head["resource"], head["start"], head["end"])):\n                break\n            rid = old_apply', 'skip-blocked-head'),
            ('settlement-reexecutes', 'durable-retry', '                return json.loads(previous["response"])', '                if command["op"] == "settle" and db.execute("SELECT 1 FROM waiting WHERE status=\'waiting\'").fetchone():\n                    return apply(db, command)\n                return json.loads(previous["response"])', 'retry-after-enrollment'),
        ]
    result = []
    for identifier, family, old, new, witness in specifications:
        if service.count(old) != 1:
            raise AssertionError(f"fault mutation target is not unique: {identifier}")
        result.append({"id": f"calx-{identifier}", "family": family,
                       "files": {**files, ALLOWED[2]: service.replace(old, new, 1)},
                       "witness_cases": [deepcopy(cases[witness])]})
    return result


def correct_controls(stage_index):
    return [{"id": name, "files": files} for name, files in control_files(stage_index).items()]
