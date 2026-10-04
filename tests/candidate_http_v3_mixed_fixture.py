"""Independent inert source for physical HTTP/CLI namespace qualification.

MIXED_SOURCE is data. Never import or execute it on the host. The physical
runner commits exactly library.py and mounts only source plus catalog inputs.
The fixture records transport/argv facts in SQLite; it does not implement the
product, interpret catalog expectations, or receive an expected-state oracle.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

from gossip_harness import candidate_http_cases_v1 as catalog
from gossip_harness import candidate_http_execution_v3 as execution
from gossip_harness import candidate_http_transport_v1 as wire

MIXED_ROW_ID = "HTTP-PERSIST-LISTENER/http-cli-http-same-db"
MIXED_DEFINITION_SHA256 = "4e571dabbe0f51ed959ed27a59634d3b99d4cdb723e5054b1511a4b795897e4a"
OBSERVATION_PROTOCOL = "inert-http-cli-v3-state-facts-v1"
EMPTY_CHAIN = "0" * 64

MIXED_SOURCE = r'''import argparse
import hashlib
import json
import socketserver
import sqlite3
import sys

parser = argparse.ArgumentParser(allow_abbrev=False)
parser.add_argument("--db", required=True)
parser.add_argument("--root", required=True)
args, command = parser.parse_known_args()
if not command:
    parser.error("fixture command is required")
argv = ["python", "-m", "library", *sys.argv[1:]]

def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")

def connect():
    db = sqlite3.connect(args.db)
    db.execute("CREATE TABLE IF NOT EXISTS fixture_state (id INTEGER PRIMARY KEY, starts INTEGER NOT NULL, events INTEGER NOT NULL, http INTEGER NOT NULL, cli INTEGER NOT NULL, chain TEXT NOT NULL, last_event TEXT)")
    db.execute("INSERT OR IGNORE INTO fixture_state VALUES (1, 0, 0, 0, 0, ?, NULL)", ("0" * 64,))
    db.commit()
    return db

def snapshot(db):
    row = db.execute("SELECT starts, events, http, cli, chain, last_event FROM fixture_state WHERE id = 1").fetchone()
    return {"starts": row[0], "events": row[1], "http": row[2], "cli": row[3],
            "chain": row[4], "last_event": None if row[5] is None else json.loads(row[5])}

def observe(event):
    db = connect()
    db.execute("BEGIN IMMEDIATE")
    before = snapshot(db)
    chain = hashlib.sha256(bytes.fromhex(before["chain"]) + canonical(event)).hexdigest()
    db.execute("UPDATE fixture_state SET events = events + 1, http = http + ?, cli = cli + ?, chain = ?, last_event = ? WHERE id = 1",
               (int(event["interface"] == "http"), int(event["interface"] == "cli"), chain, canonical(event).decode("utf-8")))
    db.commit()
    # Read from SQLite after committing rather than echoing a predicted state.
    after = snapshot(db)
    db.close()
    return {"protocol": "inert-http-cli-v3-state-facts-v1", "database": args.db,
            "root": args.root, "event": event, "before": before, "after": after}

class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        self.request.settimeout(5)
        head = bytearray()
        while not head.endswith(b"\r\n\r\n"):
            part = self.request.recv(1)
            if not part:
                return
            head.extend(part)
            if len(head) > 32768:
                return
        lines = bytes(head[:-4]).split(b"\r\n")
        method, target, version = lines[0].split(b" ")
        if version != b"HTTP/1.1":
            return
        lengths = []
        for line in lines[1:]:
            name, value = line.split(b":", 1)
            if name.lower() == b"content-length":
                lengths.append(int(value.strip()))
            if name.lower() == b"transfer-encoding":
                return
        if len(lengths) > 1 or (lengths and not 0 <= lengths[0] <= 131072):
            return
        size = lengths[0] if lengths else 0
        body = bytearray()
        while len(body) < size:
            part = self.request.recv(min(8192, size - len(body)))
            if not part:
                return
            body.extend(part)
        raw = bytes(head) + bytes(body)
        event = {"interface": "http", "argv": argv, "method": method.decode("ascii"),
                 "target": target.decode("ascii"), "request_bytes": len(raw),
                 "request_sha256": hashlib.sha256(raw).hexdigest()}
        data = canonical(observe(event))
        self.request.sendall(b"HTTP/1.1 200 Fixture state\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: "
                             + str(len(data)).encode("ascii") + b"\r\n\r\n" + data)

class Server(socketserver.TCPServer):
    allow_reuse_address = True

if command[0] == "serve":
    serve = argparse.ArgumentParser(allow_abbrev=False)
    serve.add_argument("--port", type=int, required=True)
    port = serve.parse_args(command[1:]).port
    db = connect()
    db.execute("UPDATE fixture_state SET starts = starts + 1 WHERE id = 1")
    db.commit()
    db.close()
    with Server(("127.0.0.1", port), Handler) as server:
        server.serve_forever(poll_interval=0.05)
else:
    # Every finite command is deliberately just a new state observation. It
    # does not evaluate the command's product meaning or catalog expectations.
    sys.stdout.buffer.write(canonical(observe({"interface": "cli", "argv": argv})) + b"\n")
'''


def source_files() -> dict[str, bytes]:
    return {"library.py": MIXED_SOURCE.encode("utf-8")}


def fixed_mixed_recipe() -> execution.HttpRecipe:
    rows = tuple(row for row in catalog.definitions() if row.row_id == MIXED_ROW_ID)
    if len(rows) != 1 or rows[0].definition_sha256 != MIXED_DEFINITION_SHA256:
        raise ValueError("Fixed mixed qualification row declaration changed")
    return execution.recipe_from_case(rows[0])


def expected_facts(recipe: execution.HttpRecipe) -> tuple[dict[str, Any], ...]:
    """Prospective inert state facts, never derived from observed output.

    Also usable for separately registered compact composition-fault controls.
    This function confers neither recipe registration nor product authority.
    Actual fixture source execution is always separately owned by the runner.
    """
    state: dict[str, Any] = {"starts": 0, "events": 0, "http": 0, "cli": 0,
                             "chain": EMPTY_CHAIN, "last_event": None}
    rows = []
    server_argv: list[str] | None = None
    for index, step in enumerate(recipe.steps):
        if step.kind == "start":
            state = {**state, "starts": state["starts"] + 1}
            server_argv = list(step.argv)
            continue
        if step.kind == "stop":
            server_argv = None
            continue
        if step.kind == "probe":
            if server_argv is None:
                raise ValueError("Fixture HTTP event requires an active declared server")
            request = json.loads(step.request_json)
            raw = wire.request_bytes(request, recipe.port)
            event = {"interface": "http", "argv": list(server_argv),
                     "method": request["method"], "target": request["target"],
                     "request_bytes": len(raw), "request_sha256": hashlib.sha256(raw).hexdigest()}
        elif step.kind == "cli":
            if server_argv is not None:
                raise ValueError("Fixture CLI event requires a stopped declared server")
            event = {"interface": "cli", "argv": list(step.argv)}
        else:
            raise ValueError("Unknown mixed fixture step kind")
        before = json.loads(execution.encoded(state))
        state = {**state, "events": state["events"] + 1,
                 "http": state["http"] + int(step.kind == "probe"),
                 "cli": state["cli"] + int(step.kind == "cli"),
                 "chain": hashlib.sha256(bytes.fromhex(state["chain"]) + execution.encoded(event)).hexdigest(),
                 "last_event": event}
        rows.append({"step_id": step.step_id, "step_index": index, "kind": step.kind,
                     "status": 200 if step.kind == "probe" else None,
                     "exit_code": 0 if step.kind == "cli" else None,
                     "json": {"protocol": OBSERVATION_PROTOCOL, "database": recipe.database_path,
                              "root": step.root_path, "event": event, "before": before,
                              "after": json.loads(execution.encoded(state))}})
    return tuple(rows)


def expected_observations(recipe: execution.HttpRecipe) -> tuple[dict[str, Any], ...]:
    if execution.encoded(recipe.record()) != execution.encoded(fixed_mixed_recipe().record()):
        raise ValueError("Unknown or changed full mixed qualification declaration")
    return expected_facts(recipe)
