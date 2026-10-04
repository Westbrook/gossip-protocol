"""Inert HTTP-only v3 mechanics fixtures; never execute SERVER_SOURCE on the host.

The fixed long row retains its complete semantic declaration, but these fixture
responses establish only request transport/state-namespace mechanics. They do
not satisfy the row's product expectations. The full 87-step/29-CLI history is
still required separately and is neither replaced nor qualified by this slice.
"""
from __future__ import annotations

import base64
import hashlib
import json
from typing import Any

from gossip_harness import candidate_http_cases_v1 as catalog
from gossip_harness import candidate_http_cases_core_v1 as core
from gossip_harness import candidate_http_execution_v3 as execution

LONG_ROW_ID = "HTTP-ACTION-STATE/old-token-after-successor-failed"
LONG_DEFINITION_SHA256 = "dc486b5dcfad72782813ddefda8a4a9201e03f2247e3686cc9b97b2acb1e3d97"
OBSERVATION_PROTOCOL = "inert-http-v3-transport-facts-v1"
PORT = core.PORT
DATABASE = core.DATABASE
ROOT_MARKERS = {"north": b"north-root-v3\n", "south": b"south-root-v3\n"}
OUTER_MARKER = b"outer-root-decoy-v3\n"
SHARED_PAYLOAD = b"shared-outside-selected-root-inside-inputs-v3\n"

# Independent deliberately non-product TCP server. It echoes literal transport
# facts for EVERY route, never imports a candidate/reference or serves product
# answers. This text is committed as library.py to an isolated Git store by the
# physical runner. Host tests compile/inspect it; they must not import/exec it.
SERVER_SOURCE = r'''import argparse
import hashlib
import json
import os
import socket
import socketserver
import sqlite3
import stat

parser = argparse.ArgumentParser()
parser.add_argument("--db", required=True)
parser.add_argument("--root", required=True)
parser.add_argument("command", choices=("serve",))
parser.add_argument("--port", required=True, type=int)
args = parser.parse_args()

connection = sqlite3.connect(args.db)
connection.execute("CREATE TABLE IF NOT EXISTS counters (id INTEGER PRIMARY KEY, starts INTEGER NOT NULL, requests INTEGER NOT NULL)")
connection.execute("INSERT OR IGNORE INTO counters VALUES (1, 0, 0)")
connection.execute("UPDATE counters SET starts = starts + 1 WHERE id = 1")
connection.commit()
startup_index = connection.execute("SELECT starts FROM counters WHERE id = 1").fetchone()[0]
connection.close()

def file_fact(path):
    with open(path, "rb") as stream:
        raw = stream.read()
    return {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}

def root_facts():
    direct = os.path.join(args.root, "direct.txt")
    ancestor = os.path.join(args.root, "ancestor")
    return {
        "marker": file_fact(os.path.join(args.root, "marker.txt")),
        "outer_marker": file_fact("/inputs/marker.txt"),
        "direct": {"is_symlink": stat.S_ISLNK(os.lstat(direct).st_mode),
                   "target": os.readlink(direct), "resolved": os.path.realpath(direct),
                   "file": file_fact(direct)},
        "ancestor": {"is_symlink": stat.S_ISLNK(os.lstat(ancestor).st_mode),
                     "target": os.readlink(ancestor), "resolved": os.path.realpath(ancestor),
                     "file": file_fact(os.path.join(ancestor, "payload.txt"))}}

def frame(value, status):
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    return (b"HTTP/1.1 " + str(status).encode("ascii") + b" Fixture facts\r\n"
            + b"Content-Type: application/json\r\nConnection: close\r\nContent-Length: "
            + str(len(raw)).encode("ascii") + b"\r\n\r\n" + raw)

class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        self.request.settimeout(5)
        head = bytearray()
        # Read exactly the head: the early-close control deliberately reads no
        # body bytes. This does NOT imply the peer failed to send its body.
        while not head.endswith(b"\r\n\r\n"):
            piece = self.request.recv(1)
            if not piece:
                return
            head.extend(piece)
            if len(head) > 32768:
                return
        lines = bytes(head[:-4]).split(b"\r\n")
        method, target, version = lines[0].split(b" ")
        lengths = []
        for line in lines[1:]:
            key, value = line.split(b":", 1)
            if key.lower() == b"content-length":
                lengths.append(int(value.strip()))
        if len(lengths) > 1 or (lengths and not 0 <= lengths[0] <= 131072):
            return
        length = lengths[0] if lengths else 0
        early = target == b"/fixture/early-close"
        body = bytearray()
        if not early:
            while len(body) < length:
                piece = self.request.recv(min(8192, length - len(body)))
                if not piece:
                    return
                body.extend(piece)
        connection = sqlite3.connect(args.db)
        connection.execute("UPDATE counters SET requests = requests + 1 WHERE id = 1")
        request_index = connection.execute("SELECT requests FROM counters WHERE id = 1").fetchone()[0]
        connection.commit()
        connection.close()
        value = {"protocol": "inert-http-v3-transport-facts-v1", "root": args.root,
                 "database": args.db, "startup_index": startup_index,
                 "request_index": request_index, "method": method.decode("ascii"),
                 "target": target.decode("ascii"), "declared_length": length,
                 "received_length": len(body), "body_sha256": hashlib.sha256(body).hexdigest(),
                 "root_view": root_facts() if target == b"/fixture/root-links" else None}
        self.request.sendall(frame(value, 413 if early else 200))
        if early:
            # No draining, retry or observation-dependent timing. The kernel may
            # already have buffered the full body; response capture may be lost.
            self.request.shutdown(socket.SHUT_RDWR)

class Server(socketserver.TCPServer):
    allow_reuse_address = True

with Server(("127.0.0.1", args.port), Handler) as server:
    server.serve_forever(poll_interval=0.05)
'''


def source_files() -> dict[str, bytes]:
    """Source data for a separately owned Git store, not an execution helper."""
    return {"library.py": SERVER_SOURCE.encode("utf-8")}


def fixed_long_recipe() -> execution.HttpRecipe:
    matches = tuple(row for row in catalog.definitions() if row.row_id == LONG_ROW_ID)
    if len(matches) != 1 or matches[0].definition_sha256 != LONG_DEFINITION_SHA256:
        raise ValueError("Fixed qualification row declaration changed")
    return execution.recipe_from_case(matches[0])


def _argv(root: str) -> tuple[str, ...]:
    return ("python", "-m", "library", "--db", DATABASE, "--root", root,
            "serve", "--port", str(PORT))


def _request(target: str, body: bytes | None = None) -> bytes:
    raw = b"" if body is None else body
    headers = [["Host", f"127.0.0.1:{PORT}"], ["Connection", "close"]]
    if body is not None:
        headers.extend([["Content-Type", "application/octet-stream"], ["Content-Length", str(len(raw))]])
    return execution.encoded({"method": "GET" if body is None else "POST", "target": target,
        "headers": headers, "body_b64": base64.b64encode(raw).decode("ascii")})


def root_link_recipe() -> execution.HttpRecipe:
    entries = [core.Fixture("shared", "directory"), core.Fixture("shared/payload.txt", "file", SHARED_PAYLOAD),
               core.Fixture("marker.txt", "file", OUTER_MARKER)]
    steps = []
    for epoch, name in enumerate(("north", "south"), 1):
        entries.extend((core.Fixture(name, "directory"),
            core.Fixture(name + "/marker.txt", "file", ROOT_MARKERS[name]),
            core.Fixture(name + "/direct.txt", "symlink", target="../shared/payload.txt"),
            core.Fixture(name + "/ancestor", "symlink", target="../shared")))
        root = "/inputs/" + name
        steps.append(execution.HttpStep(f"start-{epoch}", "start", epoch=epoch, root_path=root, argv=_argv(root)))
        for index in range(1, 3):
            steps.append(execution.HttpStep(f"root-{epoch}-{index}", "probe", _request("/fixture/root-links"),
                                           epoch=epoch, root_path=root))
        steps.append(execution.HttpStep(f"stop-{epoch}", "stop", epoch=epoch, root_path=root))
    return execution.HttpRecipe("mechanics-v3-root-links", _argv("/inputs/north"),
        tuple((x.path, x.data) for x in entries if x.kind == "file"),
        tuple(x.path for x in entries if x.kind == "directory"), tuple(steps),
        port=PORT, database_path=DATABASE, root_path="/inputs/north", input_entries=tuple(entries))


def boundary_body(size: int) -> bytes:
    if type(size) is not int or size not in (65536, 65537):
        raise ValueError("Only the two preregistered body sizes are fixtures")
    return bytes(range(256)) * 256 + (b"\xa5" if size == 65537 else b"")


def body_recipe(size: int, *, early_close: bool = False) -> execution.HttpRecipe:
    if type(early_close) is not bool or (early_close and size != 65537):
        raise ValueError("Early close has only the fixed 65537-byte declaration")
    target = "/fixture/early-close" if early_close else "/fixture/body"
    name = "early-close" if early_close else "body-" + str(size)
    steps = (execution.HttpStep("start-1", "start"),
             execution.HttpStep("request-1", "probe", _request(target, boundary_body(size))),
             execution.HttpStep("stop-1", "stop"))
    return execution.HttpRecipe("mechanics-v3-" + name, _argv("/inputs"), (), (), steps,
                                port=PORT, database_path=DATABASE)


def _file_fact(raw: bytes) -> dict[str, Any]:
    return {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}


def expected_observations(recipe: execution.HttpRecipe) -> tuple[dict[str, Any], ...]:
    """Prospective fixture facts, computed only from fixed declarations.

    Never takes observations. Not a product oracle, attribution adapter or
    verdict. Early-close response facts are conditional on actual retained wire
    bytes: sending and capturing that response are not guaranteed. A complete
    helper send does not mean that this source consumed any request body.
    """
    allowed = (fixed_long_recipe(), root_link_recipe(), body_recipe(65536), body_recipe(65537),
               body_recipe(65537, early_close=True))
    if not any(execution.encoded(recipe.record()) == execution.encoded(x.record()) for x in allowed):
        raise ValueError("Unknown or changed qualification declaration")
    rows = []
    request_index = 0
    for step in recipe.steps:
        if step.kind != "probe":
            continue
        request_index += 1
        request = json.loads(step.request_json)
        raw = base64.b64decode(request["body_b64"], validate=True)
        early = request["target"] == "/fixture/early-close"
        consumed = b"" if early else raw
        root_view = None
        if request["target"] == "/fixture/root-links":
            name = step.root_path.removeprefix("/inputs/")
            root_view = {"marker": _file_fact(ROOT_MARKERS[name]), "outer_marker": _file_fact(OUTER_MARKER),
                "direct": {"is_symlink": True, "target": "../shared/payload.txt",
                           "resolved": "/inputs/shared/payload.txt", "file": _file_fact(SHARED_PAYLOAD)},
                "ancestor": {"is_symlink": True, "target": "../shared",
                             "resolved": "/inputs/shared", "file": _file_fact(SHARED_PAYLOAD)}}
        rows.append({"step_id": step.step_id, "status": 413 if early else 200,
            "conditional_response": early, "guaranteed_partial_send": False,
            "json": {"protocol": OBSERVATION_PROTOCOL, "root": step.root_path,
                "database": recipe.database_path, "startup_index": step.epoch,
                "request_index": request_index, "method": request["method"], "target": request["target"],
                "declared_length": len(raw), "received_length": len(consumed),
                "body_sha256": hashlib.sha256(consumed).hexdigest(), "root_view": root_view}})
    return tuple(rows)
