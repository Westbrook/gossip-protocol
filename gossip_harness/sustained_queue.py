"""Frozen workflow repository fixture; reference implementations stay host-side.

The public adapter and CLI contain transport and legacy database setup only.
Every command executes a fresh CLI process against the scenario's SQLite file.
Expected values below are independently specified, never computed by candidates.
"""
from __future__ import annotations

from copy import deepcopy
from textwrap import dedent

ADAPTER = dedent('''\
    import json
    from pathlib import Path
    import sqlite3
    import subprocess
    import sys
    import tempfile

    def solve(payload):
        with tempfile.TemporaryDirectory(prefix="workflow-", dir="/tmp") as directory:
            database = str(Path(directory) / "queue.sqlite")
            if "legacy" in payload:
                with sqlite3.connect(database) as connection:
                    connection.execute("CREATE TABLE jobs (id TEXT PRIMARY KEY, priority INTEGER NOT NULL, status TEXT NOT NULL)")
                    connection.executemany("INSERT INTO jobs VALUES (?,?,?)", [(r["id"], r["priority"], r["status"]) for r in payload["legacy"]])
            results = []
            for command in payload["commands"]:
                result = subprocess.run([sys.executable, "-I", str(Path(__file__).parent / "workflow" / "cli.py"), database],
                                        input=json.dumps(command), text=True, capture_output=True, timeout=3)
                if result.returncode != 0:
                    raise RuntimeError("CLI command failed")
                results.append(json.loads(result.stdout))
            return results
''')
CLI = dedent('''\
    import json
    from pathlib import Path
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from workflow import storage, service
    from workflow.domain import CommandError

    connection = storage.connect(sys.argv[1])
    try:
        command = json.load(sys.stdin)
        try:
            answer = service.execute(connection, command)
        except CommandError as error:
            answer = {"error": error.code}
            if error.at is not None:
                answer["at"] = error.at
        sys.stdout.write(json.dumps(answer, ensure_ascii=True, allow_nan=False))
        sys.stdout.write("\\n")
    finally:
        connection.close()
''')
DOMAIN = dedent('''\
    import re

    class CommandError(Exception):
        def __init__(self, code, at=None):
            super().__init__(code)
            self.code, self.at = code, at

    def fail(code):
        raise CommandError(code)

    def identifier(value):
        if type(value) is not str or re.fullmatch(r"[a-z][a-z0-9-]{0,19}", value) is None:
            fail("invalid")
        return value

    def integer(value, low, high):
        if type(value) is not int or not low <= value <= high:
            fail("invalid")
        return value

    def fields(command, required, optional=()):
        if type(command) is not dict or not set(required) <= command.keys() or set(command) - set(required) - set(optional):
            fail("invalid")

    def validate(command, stage):
        if type(command) is not dict:
            fail("invalid")
        op = command.get("op")
        if op == "add":
            fields(command, ("op", "id"), ("priority", "deps"))
            identifier(command["id"])
            integer(command.get("priority", 0), -9, 9)
            deps = command.get("deps", [])
            if type(deps) is not list or len(deps) > 20:
                fail("invalid")
            for value in deps:
                identifier(value)
            if len(set(deps)) != len(deps):
                fail("invalid")
        elif op == "link" and stage >= 1:
            fields(command, ("op", "id", "dependency"))
            identifier(command["id"])
            identifier(command["dependency"])
        elif op in ("list", "ready"):
            fields(command, ("op",))
        elif op == "claim" and stage >= 2:
            fields(command, ("op", "worker", "now", "ttl"))
            identifier(command["worker"])
            integer(command["now"], 0, 100000)
            integer(command["ttl"], 1, 1000)
        elif op == "finish" and stage >= 2:
            fields(command, ("op", "id", "worker", "token", "now", "outcome"))
            identifier(command["id"])
            identifier(command["worker"])
            integer(command["token"], 1, 100000)
            integer(command["now"], 0, 100000)
            if command["outcome"] not in ("done", "retry", "failed"):
                fail("invalid")
        elif op == "audit" and stage >= 3:
            fields(command, ("op",))
        elif op == "batch" and stage >= 3:
            fields(command, ("op", "commands"))
            items = command["commands"]
            if type(items) is not list or not 1 <= len(items) <= 8:
                fail("invalid")
        else:
            fail("invalid")
        return op

    def cyclic(connection, job, dependency):
        pending, visited = [dependency], set()
        while pending:
            current = pending.pop()
            if current == job:
                return True
            if current not in visited:
                visited.add(current)
                pending.extend(row[0] for row in connection.execute("SELECT dependency FROM deps WHERE job=?", (current,)))
        return False
''')
STORAGE = dedent('''\
    import sqlite3
    STAGE = __STAGE__

    def connect(path):
        connection = sqlite3.connect(path, timeout=2)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        with connection:
            connection.execute("CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, priority INTEGER NOT NULL, status TEXT NOT NULL)")
            connection.execute("CREATE TABLE IF NOT EXISTS deps (job TEXT REFERENCES jobs(id), dependency TEXT REFERENCES jobs(id), PRIMARY KEY(job, dependency))")
            if STAGE >= 2:
                existing = {row[1] for row in connection.execute("PRAGMA table_info(jobs)")}
                for name, declaration in (("attempt", "INTEGER NOT NULL DEFAULT 0"), ("token", "INTEGER NOT NULL DEFAULT 0"), ("owner", "TEXT"), ("until", "INTEGER")):
                    if name not in existing:
                        connection.execute("ALTER TABLE jobs ADD COLUMN " + name + " " + declaration)
                connection.execute("CREATE TABLE IF NOT EXISTS receipts (job TEXT, token INTEGER, worker TEXT, outcome TEXT, status TEXT, PRIMARY KEY(job,token))")
            if STAGE >= 3:
                connection.execute("CREATE TABLE IF NOT EXISTS audit (seq INTEGER PRIMARY KEY, op TEXT NOT NULL, id TEXT NOT NULL)")
        return connection
''')
SERVICE = dedent('''\
    from .domain import CommandError, fail, validate, cyclic
    STAGE = __STAGE__

    def job(connection, name):
        row = connection.execute("SELECT * FROM jobs WHERE id=?", (name,)).fetchone()
        if row is None:
            fail("missing")
        return row

    def satisfied(connection, name):
        return connection.execute("SELECT 1 FROM deps JOIN jobs ON jobs.id=deps.dependency WHERE deps.job=? AND jobs.status!='done' LIMIT 1", (name,)).fetchone() is None

    def record(connection, op, name):
        if STAGE >= 3:
            connection.execute("INSERT INTO audit(op,id) VALUES (?,?)", (op, name))

    def apply(connection, command):
        op = validate(command, STAGE)
        if op == "list":
            return [{"id": row["id"], "priority": row["priority"], "status": row["status"], "deps": [r[0] for r in connection.execute("SELECT dependency FROM deps WHERE job=? ORDER BY dependency", (row["id"],))]} for row in connection.execute("SELECT * FROM jobs ORDER BY id")]
        if op == "ready":
            return [row["id"] for row in connection.execute("SELECT * FROM jobs WHERE status='pending' ORDER BY priority DESC,id") if satisfied(connection, row["id"])]
        if op == "add":
            name, dependencies = command["id"], command.get("deps", [])
            if connection.execute("SELECT 1 FROM jobs WHERE id=?", (name,)).fetchone():
                fail("duplicate")
            if name in dependencies:
                fail("cycle")
            for dependency in dependencies:
                job(connection, dependency)
            connection.execute("INSERT INTO jobs(id,priority,status) VALUES (?,?,'pending')", (name, command.get("priority", 0)))
            connection.executemany("INSERT INTO deps VALUES (?,?)", [(name, dep) for dep in dependencies])
            record(connection, op, name)
            return {"ok": "added", "id": name}
        if op == "link":
            name, dependency = command["id"], command["dependency"]
            row = job(connection, name)
            job(connection, dependency)
            if row["status"] != "pending":
                fail("busy")
            if cyclic(connection, name, dependency):
                fail("cycle")
            changed = connection.execute("INSERT OR IGNORE INTO deps VALUES (?,?)", (name, dependency)).rowcount
            if changed:
                record(connection, op, name)
            return {"ok": "linked", "id": name, "dependency": dependency}
        if op == "claim":
            now = command["now"]
            choices = connection.execute("SELECT * FROM jobs WHERE status='pending' OR (status='running' AND until<=?) ORDER BY priority DESC,id", (now,)).fetchall()
            row = next((row for row in choices if satisfied(connection, row["id"])), None)
            if row is None:
                return None
            attempt, token, until = row["attempt"] + 1, row["token"] + 1, now + command["ttl"]
            connection.execute("UPDATE jobs SET status='running',attempt=?,token=?,owner=?,until=? WHERE id=?", (attempt, token, command["worker"], until, row["id"]))
            record(connection, op, row["id"])
            return {"id": row["id"], "attempt": attempt, "token": token, "until": until}
        if op == "finish":
            name = command["id"]
            row = job(connection, name)
            receipt = connection.execute("SELECT * FROM receipts WHERE job=? AND token=?", (name, command["token"])).fetchone()
            if receipt is not None:
                if receipt["worker"] != command["worker"] or receipt["outcome"] != command["outcome"]:
                    fail("stale")
                return {"ok": "finished", "id": name, "status": receipt["status"]}
            if row["status"] != "running" or row["token"] != command["token"] or row["owner"] != command["worker"] or command["now"] >= row["until"]:
                fail("stale")
            status = "pending" if command["outcome"] == "retry" else command["outcome"]
            connection.execute("UPDATE jobs SET status=?,owner=NULL,until=NULL WHERE id=?", (status, name))
            connection.execute("INSERT INTO receipts VALUES (?,?,?,?,?)", (name, command["token"], command["worker"], command["outcome"], status))
            record(connection, op, name)
            return {"ok": "finished", "id": name, "status": status}
        if op == "audit":
            return [dict(row) for row in connection.execute("SELECT seq,op,id FROM audit ORDER BY seq")]
        fail("invalid")

    def execute(connection, command):
        with connection:
            op = validate(command, STAGE)
            if op == "batch":
                results = []
                for index, item in enumerate(command["commands"]):
                    try:
                        if type(item) is dict and item.get("op") in ("batch", "audit"):
                            fail("invalid")
                        results.append(apply(connection, item))
                    except CommandError as error:
                        raise CommandError(error.code, index) from error
                return {"ok": "batch", "results": results}
            return apply(connection, command)
''')
BASE_SERVICE = dedent('''\
    from .domain import fail, validate
    def execute(connection, command):
        op = validate(command, 0)
        with connection:
            if op == "add":
                if connection.execute("SELECT 1 FROM jobs WHERE id=?", (command["id"],)).fetchone():
                    fail("duplicate")
                connection.execute("INSERT INTO jobs VALUES (?,?, 'pending')", (command["id"], command.get("priority", 0)))
                return {"ok": "added", "id": command["id"]}
            if op == "list":
                return [{"id": row["id"], "priority": row["priority"], "status": row["status"], "deps": []} for row in connection.execute("SELECT * FROM jobs ORDER BY id")]
            fail("invalid")
''')

BASE_DOMAIN = dedent('''\
    import re
    class CommandError(Exception):
        def __init__(self, code, at=None):
            super().__init__(code)
            self.code, self.at = code, at
    def fail(code):
        raise CommandError(code)
    def validate(command, stage):
        if type(command) is not dict:
            fail("invalid")
        op = command.get("op")
        if op == "list" and set(command) == {"op"}:
            return op
        if op != "add" or not {"op", "id"} <= command.keys() or set(command) - {"op", "id", "priority"}:
            fail("invalid")
        name, priority = command["id"], command.get("priority", 0)
        if type(name) is not str or re.fullmatch(r"[a-z][a-z0-9-]{0,19}", name) is None:
            fail("invalid")
        if type(priority) is not int or not -9 <= priority <= 9:
            fail("invalid")
        return op
''')

BASE_STORAGE = dedent('''\
    import sqlite3
    def connect(path):
        connection = sqlite3.connect(path, timeout=2)
        connection.row_factory = sqlite3.Row
        with connection:
            connection.execute("CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, priority INTEGER NOT NULL, status TEXT NOT NULL)")
        return connection
''')

def _files(stage):
    return {"solution.py": ADAPTER, "workflow/__init__.py": "", "workflow/cli.py": CLI,
            "workflow/domain.py": BASE_DOMAIN if stage == 0 else DOMAIN,
            "workflow/storage.py": BASE_STORAGE if stage == 0 else STORAGE.replace("__STAGE__", str(stage)),
            "workflow/service.py": (BASE_SERVICE if stage == 0 else SERVICE.replace("__STAGE__", str(stage)))}

COMMON = """Extend the existing standard-library Python 3.12 SQLite workflow queue.
Only workflow/storage.py, workflow/domain.py and workflow/service.py are editable.
solution.py and workflow/cli.py are fixed transport. solve receives {"commands":
[command,...]} and optional "legacy" rows, returning one JSON value per command.
Each command is executed by the real CLI in a FRESH process against the same SQLite
file; in-memory state cannot implement persistence. Every scenario gets a fresh DB.
Inputs have <=12 commands, <=20 jobs, <=20 dependency IDs per add. Identifiers
(job and worker names) match [a-z][a-z0-9-]{0,19}. Integers exclude booleans.
Commands reject missing, extra, or malformed fields with {"error":"invalid"};
unknown operations also return invalid. Every error leaves persistent state unchanged.
No network, third-party packages, subprocesses outside the fixed transport, or
external state. Keep command output strict JSON. Preserve all earlier milestones.
The bridge calls storage.connect(path), then service.execute(connection, command),
and catches domain.CommandError whose .code and optional .at describe errors.
Validation occurs before DB existence/state checks. Fields below are exact except
explicit defaults. Basic v0 supports add/list; extend it without changing their shape.
"""
SPEC1 = COMMON + """
Q1-add: {op:'add',id,priority?:0,deps?:[]} creates a pending job. Priority is -9..9.
Dependencies are unique valid IDs of existing jobs. Duplicate IDs in deps are invalid.
Existing job ID => duplicate; self dependency => cycle; nonexistent dependency =>
missing, in that precedence after syntax validation. Successful output is
{ok:'added',id}. Unsuccessful adds create no partial job/dependency records.
Q1-link: {op:'link',id,dependency} adds an edge from job to prerequisite; both jobs
must exist (missing), source must be pending (busy), graph must remain acyclic
(cycle), in that order. Existing edge is a successful no-op. Output
{ok:'linked',id,dependency}. A self edge is a cycle. Check transitive cycles too.
Q1-order: {op:'list'} returns all jobs sorted by id, each exactly
{id,priority,status,deps:[sorted dependency IDs]}. {op:'ready'} returns pending job
IDs whose dependencies are all done, sorted by decreasing priority then increasing
id. Empty queries return []. Jobs with pending prerequisites are not ready.
Q1-durable: Every successful change survives subsequent CLI invocations.
"""
SPEC2 = COMMON + """
All milestone 1 behavior remains required.
Q2-lease: {op:'claim',worker,now,ttl} selects one dependency-satisfied pending job
OR running job whose lease expired (now >= until), using the ready priority/id
order. now is an integer 0..100000; ttl is 1..1000. Return null if no eligible job.
Otherwise persist status running, increase that job's attempt and fencing token
from initial 0 by one each, owner=worker, until=now+ttl; return exactly
{id,attempt,token,until}. Time is explicitly supplied; do not use wall clock.
ready still reports only pending jobs, not running/expired jobs. Logical times in
normal scenarios are nondecreasing. A job can be reclaimed exactly at expiration.
Q2-fence: {op:'finish',id,worker,token,now,outcome} requires an existing job,
matching running owner+token, and now < until. token is integer 1..100000;
outcome is done, retry, or failed. Missing job => missing; unmatched/expired lease
=> stale. Retry sets pending, done/failed set their names. Return
{ok:'finished',id,status}. Clear active owner/expiration; preserve attempt/token.
A failed prerequisite never satisfies a dependency; done does.
Q2-idempotent: Persist each successful finish receipt by (job,token). Repeating
that receipt's worker+outcome returns the original response even after expiration
or a later claim, and MUST NOT mutate current job state. Same token with a different
worker or outcome returns stale. Syntactic validation and job existence come first,
then prior receipts, then current lease checks. All finish outcomes are idempotent.
Q2-recovery: Claims, fences, receipts and retries survive every CLI process restart.
"""
SPEC3 = COMMON + """
All earlier milestone behavior remains required.
Q3-atomic: {op:'batch',commands:[...]} executes 1..8 ordinary commands sequentially
inside ONE transaction. Inner list/ready queries are allowed. Nested batch and audit
are invalid inner commands. Success returns {ok:'batch',results:[responses...]};
first failed inner command returns {error:code,at:zeroBasedIndex}, rolls back ALL
earlier writes/receipts/audit events in that batch, and skips later commands. Invalid
batch envelope returns {error:'invalid'} without at. No partial commits.
Q3-audit: {op:'audit'} returns persistent events [{seq,op,id},...], ordered by seq,
starting at 1 and contiguous after rollback. Record add, newly-created link, claim
of a job, and FIRST successful finish. Successful no-op links, duplicate finish
receipts, null claims, reads, errors, and rolled-back mutations add NO events.
Batch itself adds no event; successful inner mutations use their actual op.
Q3-migrate: The optional legacy payload initializes a v0 SQLite jobs table containing
ONLY id TEXT PRIMARY KEY, priority INTEGER NOT NULL, status TEXT NOT NULL. Rows
are valid unique IDs, priorities -9..9, statuses pending/done/failed. Open and upgrade
that database without losing/reordering jobs or changing their states/priorities.
Legacy jobs have no dependencies; attempt/token start at 0. Migration creates no
audit events and is idempotent on every subsequent process restart.
Q3-restart: Database migrations, atomic results, audit sequence and fencing receipts
remain correct across fresh CLI invocations, including after a failed batch.
"""

# These expected outputs are hand-specified with small declarative helpers.
def add(name, priority=0, deps=None):
    value = {"op": "add", "id": name, "priority": priority}
    if deps is not None:
        value["deps"] = deps
    return value

def added(name):
    return {"ok": "added", "id": name}

def row(name, priority=0, status="pending", deps=()):
    return {"id": name, "priority": priority, "status": status, "deps": list(deps)}

def claim(worker="worker", now=0, ttl=10):
    return {"op": "claim", "worker": worker, "now": now, "ttl": ttl}

def claimed(name, attempt=1, token=1, until=10):
    return {"id": name, "attempt": attempt, "token": token, "until": until}

def finish(name, token=1, now=1, outcome="done", worker="worker"):
    return {"op": "finish", "id": name, "worker": worker, "token": token, "now": now, "outcome": outcome}

def finished(name, status="done"):
    return {"ok": "finished", "id": name, "status": status}

def error(code, at=None):
    result = {"error": code}
    if at is not None:
        result["at"] = at
    return result

def link(name, dependency):
    return {"op": "link", "id": name, "dependency": dependency}

def linked(name, dependency):
    return {"ok": "linked", "id": name, "dependency": dependency}

def events(*items):
    return [{"seq": index + 1, "op": op, "id": name} for index, (op, name) in enumerate(items)]

def case(name, requirement, commands, expected, legacy=None):
    payload = {"commands": commands}
    if legacy is not None:
        payload["legacy"] = legacy
    return {"id": name, "requirement": requirement, "input": payload, "expected": expected}

LIST, READY, AUDIT = {"op": "list"}, {"op": "ready"}, {"op": "audit"}
V1 = (
    case("empty", "Q1-order", [LIST, READY], [[], []]),
    case("priority", "Q1-order", [add("b", 2), add("a", 2), add("c", -1), READY, LIST], [added("b"), added("a"), added("c"), ["a", "b", "c"], [row("a", 2), row("b", 2), row("c", -1)]]),
    case("dependencies", "Q1-add", [add("base"), add("child", 9, ["base"]), READY, LIST], [added("base"), added("child"), ["base"], [row("base"), row("child", 9, deps=["base"])]]),
    case("unknown-atomic", "Q1-add", [add("child", deps=["absent"]), LIST], [error("missing"), []]),
    case("cycle", "Q1-link", [add("a"), add("b", deps=["a"]), link("a", "b"), READY], [added("a"), added("b"), error("cycle"), ["a"]]),
    case("duplicate-edge", "Q1-link", [add("root"), add("leaf"), link("leaf", "root"), link("leaf", "root"), LIST], [added("root"), added("leaf"), linked("leaf", "root"), linked("leaf", "root"), [row("leaf", deps=["root"]), row("root")]]),
    case("validation", "Q1-add", [add("bad", True), add("bad", deps=["x", "x"]), {"op": "add", "id": "UPPER"}, {"op": "ready", "extra": 1}, LIST], [error("invalid")] * 4 + [[]]),
    case("restart", "Q1-durable", [add("persist", -9), LIST, add("persist", 9), LIST], [added("persist"), [row("persist", -9)], error("duplicate"), [row("persist", -9)]]),
)
H1 = (
    case("self-dependency", "Q1-add", [add("solo", deps=["solo"]), LIST], [error("cycle"), []]),
    case("multi-level-cycle", "Q1-link", [add("first"), add("second", deps=["first"]), add("third", deps=["second"]), link("first", "third"), READY], [added("first"), added("second"), added("third"), error("cycle"), ["first"]]),
    case("missing-link", "Q1-link", [add("present"), link("gone", "present"), link("present", "gone"), READY], [added("present"), error("missing"), error("missing"), ["present"]]),
    case("self-link", "Q1-link", [add("unit"), link("unit", "unit"), LIST], [added("unit"), error("cycle"), [row("unit")]]),
    case("deps-sort", "Q1-order", [add("zeta"), add("alpha"), add("middle", deps=["zeta", "alpha"]), LIST], [added("zeta"), added("alpha"), added("middle"), [row("alpha"), row("middle", deps=["alpha", "zeta"]), row("zeta")]]),
    case("negative-order", "Q1-order", [add("zz", -4), add("aa", -4), add("mm", -9), READY], [added("zz"), added("aa"), added("mm"), ["aa", "zz", "mm"]]),
    case("bad-priority", "Q1-add", [add("x", 10), add("y", -10), add("z", 1.0), LIST], [error("invalid")] * 3 + [[]]),
    case("malformed", "Q1-add", [{"op": "add"}, {"op": "add", "id": "good", "deps": "x"}, {"op": "link", "id": "good", "dependency": 7}, {"op": "wat"}], [error("invalid")] * 4),
    case("partial-deps", "Q1-durable", [add("yes"), add("no", deps=["yes", "missing"]), add("no"), LIST], [added("yes"), error("missing"), added("no"), [row("no"), row("yes")]]),
    case("diamond", "Q1-link", [add("top"), add("left", deps=["top"]), add("right", deps=["top"]), add("bottom", deps=["left", "right"]), link("right", "left"), READY], [added("top"), added("left"), added("right"), added("bottom"), linked("right", "left"), ["top"]]),
)
V2 = (
    case("claim-complete", "Q2-lease", [add("a"), claim(), READY, finish("a"), LIST], [added("a"), claimed("a"), [], finished("a"), [row("a", status="done")]]),
    case("expiry-fence", "Q2-fence", [add("a"), claim(ttl=2), finish("a", now=2), claim("other", 2, 4), finish("a", now=3), finish("a", 2, 3, worker="other")], [added("a"), claimed("a", until=2), error("stale"), claimed("a", 2, 2, 6), error("stale"), finished("a")]),
    case("release-dependency", "Q2-lease", [add("base"), add("child", 9, ["base"]), claim(), finish("base"), claim(now=2)], [added("base"), added("child"), claimed("base"), finished("base"), claimed("child", until=12)]),
    case("retry-receipt", "Q2-idempotent", [add("a"), claim(), finish("a", outcome="retry"), claim(now=2), finish("a", now=3, outcome="retry"), LIST], [added("a"), claimed("a"), finished("a", "pending"), claimed("a", 2, 2, 12), finished("a", "pending"), [row("a", status="running")]]),
    case("failed-block", "Q2-fence", [add("root"), add("leaf", deps=["root"]), claim(), finish("root", outcome="failed"), READY, claim(now=2)], [added("root"), added("leaf"), claimed("root"), finished("root", "failed"), [], None]),
    case("repeat-done", "Q2-idempotent", [add("a"), claim(), finish("a"), finish("a", now=99), finish("a", now=99, outcome="failed")], [added("a"), claimed("a"), finished("a"), finished("a"), error("stale")]),
    case("lease-validation", "Q2-lease", [claim(ttl=0), claim(now=True), finish("gone", token=0), finish("gone"), claim()], [error("invalid"), error("invalid"), error("invalid"), error("missing"), None]),
    case("link-busy", "Q2-recovery", [add("a"), add("b"), claim(), link("a", "b"), LIST], [added("a"), added("b"), claimed("a"), error("busy"), [row("a", status="running"), row("b")]]),
)
H2 = (
    case("expired-not-ready", "Q2-lease", [add("job"), claim(ttl=1), READY, finish("job", now=1), claim("next", 1, 1), LIST], [added("job"), claimed("job", until=1), [], error("stale"), claimed("job", 2, 2, 2), [row("job", status="running")]]),
    case("no-premature-reclaim", "Q2-lease", [add("job"), claim(ttl=9), claim("next", 8, 1), finish("job", now=8)], [added("job"), claimed("job", until=9), None, finished("job")]),
    case("owner-mismatch", "Q2-fence", [add("job"), claim("alice"), finish("job", worker="bob"), finish("job", worker="alice"), finish("job", worker="bob")], [added("job"), claimed("job"), error("stale"), finished("job"), error("stale")]),
    case("wrong-token", "Q2-fence", [add("job"), claim(), finish("job", token=2), finish("job")], [added("job"), claimed("job"), error("stale"), finished("job")]),
    case("repeated-failed", "Q2-idempotent", [add("job"), claim(), finish("job", outcome="failed"), finish("job", now=11, outcome="failed"), claim(now=12)], [added("job"), claimed("job"), finished("job", "failed"), finished("job", "failed"), None]),
    case("attempts-persist", "Q2-recovery", [add("item"), claim(ttl=1), claim(now=1, ttl=1), finish("item", 2, 1, "retry"), claim(now=2, ttl=3), finish("item", 3, 3), LIST], [added("item"), claimed("item", until=1), claimed("item", 2, 2, 2), finished("item", "pending"), claimed("item", 3, 3, 5), finished("item"), [row("item", status="done")]]),
    case("claim-priority", "Q2-lease", [add("z", 4), add("b", 9), add("a", 9), claim(ttl=2), claim("second", 1, 10), claim("third", 2, 10)], [added("z"), added("b"), added("a"), claimed("a", until=2), claimed("b", until=11), claimed("a", 2, 2, 12)]),
    case("all-deps-done", "Q2-lease", [add("a"), add("b"), add("c", 9, ["a", "b"]), claim(), finish("a"), READY, claim(now=2), finish("b", now=3), READY], [added("a"), added("b"), added("c"), claimed("a"), finished("a"), ["b"], claimed("b", until=12), finished("b"), ["c"]]),
    case("finish-before-claim", "Q2-fence", [add("wait"), finish("wait"), LIST], [added("wait"), error("stale"), [row("wait")]]),
    case("bad-lease-fields", "Q2-lease", [claim(ttl=True), claim(now=-1), claim("BAD"), finish("abc", outcome="unknown"), {"op": "claim", "worker": "x", "now": 0}], [error("invalid")] * 5),
)

def batch(*commands):
    return {"op": "batch", "commands": list(commands)}

def batched(*results):
    return {"ok": "batch", "results": list(results)}

V3 = (
    case("atomic-success", "Q3-atomic", [batch(add("a"), add("b", deps=["a"]), READY), LIST], [batched(added("a"), added("b"), ["a"]), [row("a"), row("b", deps=["a"])]]),
    case("atomic-rollback", "Q3-atomic", [batch(add("a"), add("a"), add("later")), LIST, AUDIT], [error("duplicate", 1), [], []]),
    case("audit-noops", "Q3-audit", [add("a"), add("b"), link("b", "a"), link("b", "a"), claim(), finish("a"), finish("a"), AUDIT], [added("a"), added("b"), linked("b", "a"), linked("b", "a"), claimed("a"), finished("a"), finished("a"), events(("add", "a"), ("add", "b"), ("link", "b"), ("claim", "a"), ("finish", "a"))]),
    case("legacy-read", "Q3-migrate", [LIST, READY, AUDIT], [[row("done", 7, "done"), row("todo", -2)], ["todo"], []], legacy=[{"id": "todo", "priority": -2, "status": "pending"}, {"id": "done", "priority": 7, "status": "done"}]),
    case("legacy-claim", "Q3-migrate", [claim(), finish("old"), LIST, AUDIT], [claimed("old"), finished("old"), [row("old", 3, "done")], events(("claim", "old"), ("finish", "old"))], legacy=[{"id": "old", "priority": 3, "status": "pending"}]),
    case("nested-rejected", "Q3-atomic", [batch(add("a"), batch(add("b"))), LIST, batch(AUDIT)], [error("invalid", 1), [], error("invalid", 0)]),
    case("receipt-rollback", "Q3-restart", [add("a"), claim(), batch(finish("a"), add("a")), finish("a", now=2), AUDIT], [added("a"), claimed("a"), error("duplicate", 1), finished("a"), events(("add", "a"), ("claim", "a"), ("finish", "a"))]),
    case("audit-after-error", "Q3-audit", [add("a"), batch(add("b"), link("a", "missing")), add("c"), AUDIT], [added("a"), error("missing", 1), added("c"), events(("add", "a"), ("add", "c"))]),
)
H3 = (
    case("batch-cycle-rollback", "Q3-atomic", [add("root"), batch(add("child", deps=["root"]), link("root", "child")), LIST, AUDIT], [added("root"), error("cycle", 1), [row("root")], events(("add", "root"))]),
    case("claim-rollback-token", "Q3-restart", [add("job"), batch(claim(), add("job")), claim("owner", 1, 3), AUDIT], [added("job"), error("duplicate", 1), claimed("job", until=4), events(("add", "job"), ("claim", "job"))]),
    case("retry-rollback", "Q3-restart", [add("task"), claim(), batch(finish("task", outcome="retry"), claim(now=2), add("task")), finish("task", now=3), AUDIT], [added("task"), claimed("task"), error("duplicate", 2), finished("task"), events(("add", "task"), ("claim", "task"), ("finish", "task"))]),
    case("batch-full-life", "Q3-atomic", [batch(add("work"), claim(), finish("work"), LIST), AUDIT], [batched(added("work"), claimed("work"), finished("work"), [row("work", status="done")]), events(("add", "work"), ("claim", "work"), ("finish", "work"))]),
    case("empty-bad-batch", "Q3-atomic", [batch(), {"op": "batch", "commands": {}}, batch(add("good"), {"op": "wat"}), LIST], [error("invalid"), error("invalid"), error("invalid", 1), []]),
    case("null-read-audit", "Q3-audit", [claim(), READY, LIST, AUDIT, add("only"), claim(), claim(now=1), AUDIT], [None, [], [], [], added("only"), claimed("only"), None, events(("add", "only"), ("claim", "only"))]),
    case("legacy-failed-dependency", "Q3-migrate", [add("dependent", 9, ["failed"]), READY, claim(), LIST], [added("dependent"), [], None, [row("dependent", 9, deps=["failed"]), row("failed", 2, "failed")]], legacy=[{"id": "failed", "priority": 2, "status": "failed"}]),
    case("legacy-done-dependency", "Q3-migrate", [add("new", deps=["old"]), claim(now=8, ttl=2), finish("new", now=9), AUDIT], [added("new"), claimed("new", until=10), finished("new"), events(("add", "new"), ("claim", "new"), ("finish", "new"))], legacy=[{"id": "old", "priority": 0, "status": "done"}]),
    case("legacy-restart-retry", "Q3-restart", [claim(ttl=2), finish("legacy", outcome="retry"), claim("next", 2, 2), finish("legacy", now=3, outcome="retry"), finish("legacy", 2, 3, worker="next"), AUDIT], [claimed("legacy", until=2), finished("legacy", "pending"), claimed("legacy", 2, 2, 4), finished("legacy", "pending"), finished("legacy"), events(("claim", "legacy"), ("finish", "legacy"), ("claim", "legacy"), ("finish", "legacy"))], legacy=[{"id": "legacy", "priority": 0, "status": "pending"}]),
    case("batch-receipt-noop", "Q3-audit", [add("j"), claim(), finish("j"), batch(finish("j", now=20), READY), AUDIT], [added("j"), claimed("j"), finished("j"), batched(finished("j"), []), events(("add", "j"), ("claim", "j"), ("finish", "j"))]),
)

PROJECT = {
    "id": "workflow", "title": "Durable dependency workflow queue",
    "initial_files": _files(0),
    "allowed_paths": ("workflow/storage.py", "workflow/domain.py", "workflow/service.py"),
    "stages": tuple({"id": f"workflow-{number}", "spec": spec,
                     "requirements": tuple(requirements), "visible_cases": visible,
                     "hidden_cases": hidden, "known_files": _files(number)}
                    for number, spec, requirements, visible, hidden in (
                        (1, SPEC1, ("Q1-add", "Q1-link", "Q1-order", "Q1-durable"), V1, H1),
                        (2, SPEC2, ("Q2-lease", "Q2-fence", "Q2-idempotent", "Q2-recovery"), V2, H2),
                        (3, SPEC3, ("Q3-atomic", "Q3-audit", "Q3-migrate", "Q3-restart"), V3, H3))),
}
MUTANTS = {}
for _name, _old, _new in (
    ("inclusive-expiry", 'command["now"] >= row["until"]', 'command["now"] > row["until"]'),
    ("partial-batch-commit", 'results.append(apply(connection, item))', 'results.append(apply(connection, item))\n                    connection.commit()'),
    ("receipt-reapplied", 'return {"ok": "finished", "id": name, "status": receipt["status"]}', 'connection.execute("UPDATE jobs SET status=? WHERE id=?", (receipt["status"], name))\n            return {"ok": "finished", "id": name, "status": receipt["status"]}'),
):
    _fileset = deepcopy(PROJECT["stages"][-1]["known_files"])
    assert _old in _fileset["workflow/service.py"]
    _fileset["workflow/service.py"] = _fileset["workflow/service.py"].replace(_old, _new)
    MUTANTS[_name] = _fileset
PROJECT["mutants"] = MUTANTS
