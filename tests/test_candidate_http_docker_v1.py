"""Physical C03 observation mechanics; no product acceptance authority.

The server fixture below is committed as inert bytes to an isolated Git store.
It executes only as a real container main process; host tests never import it.
"""
from __future__ import annotations

import base64
from dataclasses import asdict
import hashlib
import http.client
import io
import json
import os
from pathlib import Path
import subprocess
import threading
import unittest
import uuid

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness import candidate_cli_cases_v1 as cli_cases
from gossip_harness import candidate_client_process_v4 as engine
from gossip_harness import candidate_http_execution_v1 as execution
from gossip_harness import candidate_http_transport_v1 as wire
from gossip_harness.gitstore import GitStore
from gossip_harness.library_project_fixture_v1 import RUNTIME_IMAGE
from gossip_harness.sandbox import DockerValidator


PURPOSE = "harness_qualification"
WIRE_CAP = 4096
ORDINARY_WIRE_CAP = 65536
SOCKET_TIMEOUT_SECONDS = 2.0
PORT = 8765
SEMANTIC_VALUE = {"a": 1, "b": [True, None], "text": "\u00e9"}
MARKER = "c03-fixed-sqlite-marker-v1"

# A deliberately small actual TCP server. This does not import any reference or
# candidate application, implement product routes, or receive host expectations.
SERVER_SOURCE = r'''import argparse
import json
import os
import socket
import socketserver
import sqlite3
import time

p = argparse.ArgumentParser()
p.add_argument("--bind", default="127.0.0.1")
p.add_argument("--port", type=int, required=True)
p.add_argument("--db", required=True)
p.add_argument("--root", required=True)
args = p.parse_args()

def frame(body, status=b"200 Different reason", headers=()):
    return (b"HTTP/1.1 " + status + b"\r\n" +
            b"content-type: application/json\r\n" +
            b"Content-Length: " + str(len(body)).encode() + b"\r\n" +
            b"".join(k + b": " + v + b"\r\n" for k, v in headers) +
            b"\r\n" + body)

def exact_wire(size):
    # This is a prospective byte equation, never fitted to observed output.
    for count in range(size):
        raw = frame(b"x" * count)
        if len(raw) == size:
            return raw
    raise ValueError("No exact fixture size")

class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        self.request.settimeout(5)
        data = b""
        while b"\r\n\r\n" not in data:
            piece = self.request.recv(4096)
            if not piece:
                return
            data += piece
            if len(data) > 131072:
                return
        head, body = data.split(b"\r\n\r\n", 1)
        method, target, _ = head.split(b"\r\n", 1)[0].split(b" ")
        length = 0
        for line in head.split(b"\r\n")[1:]:
            name, value = line.split(b":", 1)
            if name.lower() == b"content-length":
                length = int(value)
        while len(body) < length:
            piece = self.request.recv(min(4096, length - len(body)))
            if not piece:
                return
            body += piece
        if target == b"/ready":
            self.request.sendall(frame(b'{"ready":true}'))
        elif target == b"/cl-keepalive":
            self.request.sendall(frame(b' { "text":"\\u00e9", "b":[true,null], "a":1.0 } \n',
                                       headers=((b"Connection", b"keep-alive"),)))
            time.sleep(8)
        elif target == b"/chunked":
            value = '{"a":1,"text":"\u00e9","b":[true,null]}'.encode()
            chunks = [value[:9], value[9:]]
            raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Type: application/json\r\n\r\n"
            for chunk in chunks:
                raw += format(len(chunk), "x").encode() + b";fixture=yes\r\n" + chunk + b"\r\n"
            self.request.sendall(raw + b"0\r\nX-Fixture-Trailer: done\r\n\r\n")
            time.sleep(8)
        elif target == b"/close":
            self.request.sendall(b'HTTP/1.0 200 OK\r\nContent-Type: application/json\r\n\r\n'
                                 b'{"b":[true,null],"a":1,"text":"\\u00e9"}')
        elif target == b"/error":
            self.request.sendall(frame(b' { "error": "job_conflict" } ', b"409 Fixture conflict"))
        elif target == b"/wrong-marker":
            self.request.sendall(frame(b'{"marker":"deliberately-wrong"}'))
        elif target == b"/truncated":
            body = b'{"error":"job_conflict"}'
            self.request.sendall(b"HTTP/1.1 409 Conflict\r\nContent-Type: application/json\r\nContent-Length: "
                                 + str(len(body) + 11).encode() + b"\r\n\r\n" + body)
        elif target == b"/no-eof":
            self.request.sendall(b'HTTP/1.0 200 OK\r\nContent-Type: application/json\r\n\r\n{"ready":true}')
            time.sleep(8)
        elif target == b"/cap-exact":
            self.request.sendall(exact_wire(4096))
        elif target == b"/cap-overflow":
            self.request.sendall(exact_wire(4097))
        elif target == b"/exit-during-response":
            self.request.sendall(b'HTTP/1.1 200 OK\r\nContent-Length: 999\r\n\r\n{"ready":')
            os._exit(17)
        elif target == b"/contradictory-length":
            self.request.sendall(b'HTTP/1.1 200 OK\r\nContent-Length: 2\r\nContent-Length: 3\r\n\r\n{}\n')
        elif target == b"/marker":
            db = sqlite3.connect(args.db)
            try:
                db.execute("CREATE TABLE IF NOT EXISTS marker (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
                if method == b"POST":
                    value = json.loads(body)["marker"]
                    db.execute("INSERT OR REPLACE INTO marker (id,value) VALUES (1,?)", (value,))
                    db.commit()
                row = db.execute("SELECT value FROM marker WHERE id=1").fetchone()
                payload = json.dumps({"marker": None if row is None else row[0]}).encode()
                self.request.sendall(frame(payload))
            finally:
                db.close()
        else:
            self.request.sendall(frame(b'{"error":"fixture_route"}', b"404 Not Found"))

class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

with Server((args.bind, args.port), Handler) as server:
    print("fixture-listening", flush=True)
    server.serve_forever(poll_interval=0.05)
'''


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True, allow_nan=False).encode("ascii")


def sha256(value):
    return hashlib.sha256(value).hexdigest()


def write_new(path, value):
    with path.open("xb") as stream:
        stream.write(encoded(value))
        stream.flush()
        os.fsync(stream.fileno())


def retained_docker_command(root, label, endpoint, arguments):
    """Bound and retain real test-owned controls used only by physical M08.

    This is deliberately not a fake controller response or an executor override.
    The command has literal argv, a pinned local endpoint, no stdin or shell,
    independent capped streams, and fresh intent/output paths.
    """
    endpoint.validate()
    argv = ["docker", "--host", "unix://" + endpoint.socket_path, *arguments]
    cap, timeout = 1024 * 1024, 15
    environment = {key: value for key, value in DockerValidator._environment().items()
                   if key not in ("DOCKER_HOST", "DOCKER_CONTEXT")}
    write_new(root / (label + "-intent.json"), {
        "argv": argv, "stream_limit_bytes": cap, "timeout_seconds": timeout,
        "purpose": PURPOSE, "environment_sha256": sha256(encoded(environment)),
    })
    child = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, env=environment)
    streams, counts, errors = [bytearray(), bytearray()], [0, 0], []

    def drain(index):
        pipe = child.stdout if index == 0 else child.stderr
        assert pipe is not None
        try:
            while block := pipe.read(4096):
                counts[index] += len(block)
                remaining = cap - len(streams[index])
                streams[index].extend(block[:max(0, remaining)])
                if len(block) > remaining:
                    errors.append("stream_limit")
                    child.kill()
                    break
        except OSError as error:
            errors.append(type(error).__name__)
        finally:
            pipe.close()

    readers = [threading.Thread(target=drain, args=(index,), daemon=True) for index in range(2)]
    for reader in readers:
        reader.start()
    timed_out = False
    try:
        child.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        timed_out = True
        child.kill()
        child.wait(timeout=5)
    for reader in readers:
        reader.join(timeout=5)
    record = {"argv": argv, "exit_code": child.returncode, "timed_out": timed_out,
              "capture_complete": not errors and all(not reader.is_alive() for reader in readers)}
    for index, channel in enumerate(("stdout", "stderr")):
        raw = bytes(streams[index])
        name = label + "-" + channel + ".bin"
        with (root / name).open("xb") as stream:
            stream.write(raw)
        record[channel] = {"path": name, "bytes": len(raw), "sha256": sha256(raw),
                           "observed_bytes": counts[index], "truncated": counts[index] != len(raw)}
    write_new(root / (label + ".json"), record)
    return record


def retained_bytes(test, root, descriptor):
    name = descriptor["path"]
    test.assertEqual(Path(name).name, name)
    raw = (root / name).read_bytes()
    test.assertEqual(len(raw), descriptor["bytes"])
    test.assertEqual(sha256(raw), descriptor["sha256"])
    return raw


def strict_control_json(raw):
    def pairs(items):
        result = {}
        for name, value in items:
            if name in result:
                raise ValueError("Duplicate control JSON key")
            result[name] = value
        return result

    def constant(value):
        raise ValueError("Nonfinite control JSON constant")

    return json.loads(raw, object_pairs_hook=pairs, parse_constant=constant)


def retained_engine_json(test, root, label):
    """Read retained Engine response bytes only; never opens a socket."""
    raw = (root / (label + "-response.bin")).read_bytes()

    class RetainedBytes(io.BytesIO):
        def close(self):
            pass

    stream = RetainedBytes(raw)

    class RetainedSocket:
        def makefile(self, *args, **kwargs):
            return stream

    response = http.client.HTTPResponse(RetainedSocket())
    response.begin()
    test.assertEqual(response.status, 200)
    body = response.read(1024 * 1024 + 1)
    test.assertLessEqual(len(body), 1024 * 1024)
    test.assertEqual(stream.tell(), len(raw))
    value = strict_control_json(body)
    test.assertIs(type(value), dict)
    return value


def role_spec(record):
    return execution.RoleSpec(record["role"], record["name"], tuple(record["argv"]),
        tuple(tuple(pair) for pair in record["labels"]), tuple(tuple(pair) for pair in record["binds"]),
        volume=record["volume"], server_id=record["server_id"])


def control_definitions():
    """Ordered prospective mechanics roster; startup traffic is separate."""
    rows = (
        ("M01", "lawful-framing", ["/cl-keepalive", "/chunked", "/close", "/error"]),
        ("M02", "mismatch-then-incomplete", ["/wrong-marker", "/truncated"]),
        ("M03", "close-without-eof", ["/no-eof"]),
        ("M04", "wire-cap-boundary", ["/cap-exact", "/cap-overflow"]),
        ("M05", "server-exits", ["/exit-during-response"]),
        ("M06", "wildcard-listener", ["/ready"]),
        ("M07", "normal-same-db-restart", ["/marker", "/marker"]),
        ("M08", "wrong-probe-namespace-prestart", []),
        ("M09", "ambiguous-framing", ["/contradictory-length"]),
    )
    return tuple({"control_id": control_id, "name": name, "targets": targets,
                  "purpose": PURPOSE, "acceptance_credit": False}
                 for control_id, name, targets in rows)


def request_recipe(target, *, method="GET", body=b""):
    connection = "keep-alive" if target in ("/cl-keepalive", "/chunked") else "close"
    headers = [["Host", "127.0.0.1:" + str(PORT)], ["Connection", connection]]
    if method == "POST" or body:
        headers.extend([["Content-Type", "application/json"], ["Content-Length", str(len(body))]])
    return {"method": method, "target": target, "headers": headers,
            "body_b64": base64.b64encode(body).decode("ascii")}


def server_argv(bind="127.0.0.1"):
    return ("python", "/workspace/fixture_server.py", "--bind", bind, "--port", str(PORT),
            "--db", "/tmp/library.sqlite", "--root", "/inputs")


def policy_for(control_id):
    return execution.HttpPolicy(RUNTIME_IMAGE, wire_limits=wire.WireLimits(
        response_limit_bytes=WIRE_CAP if control_id == "M04" else ORDINARY_WIRE_CAP,
        timeout_seconds=SOCKET_TIMEOUT_SECONDS))


def recipe_for(row):
    steps = [execution.HttpStep("start-1", "start")]
    for index, target in enumerate(row["targets"], 1):
        if row["control_id"] == "M07" and index == 2:
            steps.extend([execution.HttpStep("stop-1", "stop"), execution.HttpStep("start-2", "start")])
        post = row["control_id"] == "M07" and index == 1
        request = request_recipe(target, method="POST" if post else "GET",
                                 body=encoded({"marker": MARKER}) if post else b"")
        steps.append(execution.HttpStep("request-" + str(index), "probe", execution.encoded(request)))
    steps.append(execution.HttpStep("stop-2" if row["control_id"] == "M07" else "stop-1", "stop"))
    return execution.HttpRecipe("mechanics-" + row["control_id"].lower(),
        server_argv("0.0.0.0" if row["control_id"] == "M06" else "127.0.0.1"), (), (), tuple(steps), port=PORT)


@unittest.skipUnless(os.environ.get("GOSSIP_RUN_DOCKER_TESTS") == "1", "explicit real Docker lane")
class CandidateHttpMechanicsV1DockerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.artifacts = ArtifactDirectory("candidate-c03-http-mechanics-v1", retain_success=True)
        cls.addClassCleanup(cls.artifacts.close)
        cls.controls = {row["control_id"]: row for row in control_definitions()}
        cls.outcomes = {key: {"control_id": key, "status": "not-run", "acceptance_credit": False}
                        for key in cls.controls}
        cls.addClassCleanup(cls.save_census)
        cls.endpoint = engine.EngineEndpoint.from_environment()

        def retain_runtime(name, raw):
            with (cls.artifacts.root / name).open("xb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())

        cls.runtime = engine.runtime_identity(cls.endpoint, RUNTIME_IMAGE, retain=retain_runtime, label="class-runtime")
        cls.files = {"fixture_server.py": SERVER_SOURCE.encode("utf-8")}
        cls.store = GitStore.create(cls.artifacts.root / "fixture.git", {"fixture_server.py": SERVER_SOURCE})
        cls.commit = cls.store.head()
        cls.tree, captured = execution.capture_git_source(cls.store, cls.commit)
        if captured != cls.files:
            raise AssertionError("Committed fixture differs from authored bytes")
        cls.requirements_sha256 = cli_cases.NORMATIVE_SHA256["library-cumulative-product-v2.json"]
        cls.roster_sha256 = sha256(encoded(list(cls.controls.values())))
        write_new(cls.artifacts.root / "prospective-controls.json", {
            "controls": list(cls.controls.values()), "ordered_roster_sha256": cls.roster_sha256,
            "history_count": 9, "declared_request_count": 14, "purpose": PURPOSE,
            "source_commit": cls.commit, "source_tree": cls.tree,
            "source_files": {name: sha256(raw) for name, raw in cls.files.items()},
            "source_sha256": execution.source_sha256(cls.files), "runtime": cls.runtime,
            "helper_sha256": wire.helper_sha256(), "evaluator_sources": execution.evaluator_sources(),
            "definition_sources": {str(Path(__file__).name): sha256(Path(__file__).read_bytes()),
                "docs/candidate-http-mechanics-v1.md": sha256((Path(__file__).resolve().parents[1]
                    / "docs/candidate-http-mechanics-v1.md").read_bytes())},
            "policies": {key: asdict(policy_for(key)) for key in cls.controls},
            "acceptance_credit": False, "full_requirement_verdict": None,
        })

    @classmethod
    def save_census(cls):
        write_new(cls.artifacts.root / "completion-census.json", {
            "planned": list(cls.controls), "planned_history_count": 9, "planned_request_count": 14,
            "outcomes": list(cls.outcomes.values()),
            "not_run": [key for key, value in cls.outcomes.items() if value["status"] == "not-run"],
            "not_qualified": [key for key, value in cls.outcomes.items() if value["status"] != "qualified"],
            "acceptance_credit": False, "production_acceptance_authority": False,
            "full_requirement_verdict": None,
        })

    def begin_control(self, control_id):
        self.assertEqual(self.outcomes[control_id]["status"], "not-run", "Never redispatch a control")
        self.outcomes[control_id]["status"] = "started-not-qualified"
        root = self.artifacts.root / control_id.lower()
        root.mkdir()
        return root

    def finish_control(self, control_id, assertions):
        self.outcomes[control_id].update({"status": "qualified", "mechanics_assertions": assertions,
            "qualification_verified": True, "acceptance_credit": False,
            "production_acceptance_authority": False})
        write_new(self.artifacts.root / control_id.lower() / "mechanics-outcome.json", self.outcomes[control_id])

    def checked_command(self, root, label, arguments):
        record = retained_docker_command(root, label, self.endpoint, arguments)
        self.assertEqual(record["exit_code"], 0, record)
        self.assertIs(record["timed_out"], False)
        self.assertIs(record["capture_complete"], True)
        self.assertFalse(record["stdout"]["truncated"] or record["stderr"]["truncated"])
        return record

    def inspected(self, root, label, container_id):
        record = self.checked_command(root, label, ["inspect", "--format", "{{json .}}", container_id])
        value = strict_control_json(retained_bytes(self, root, record["stdout"]))
        self.assertIs(type(value), dict)
        self.assertEqual(value["Id"], container_id)
        return value

    def assert_created_id(self, root, record):
        value = retained_bytes(self, root, record["stdout"]).strip().decode("ascii")
        self.assertRegex(value, r"^[0-9a-f]{64}$")
        return value

    def run_history(self, control_id):
        root = self.begin_control(control_id)
        row = self.controls[control_id]
        recipe, policy = recipe_for(row), policy_for(control_id)
        binding = execution.binding_for(self.files, recipe, policy, self.runtime,
                                       requirements_sha256=self.requirements_sha256)
        registration = execution.HttpRegistration(binding, self.commit, self.tree, "physical-mechanics-v1-1")
        write_new(root / "prospective-registration.json", {"registration": asdict(registration),
            "recipe": recipe.record(), "ordered_roster_sha256": self.roster_sha256, "policy": asdict(policy)})
        checkpoints = []

        def checkpoint_sink(checkpoint):
            write_new(root / ("external-checkpoint-" + str(len(checkpoints)).zfill(5) + ".json"), asdict(checkpoint))
            checkpoints.append(checkpoint)

        journal = root / "execution"
        with execution.CandidateHttpExecution(journal, self.store, registration, recipe, policy,
                endpoint=self.endpoint, checkpoint_sink=checkpoint_sink) as controller:
            result = controller.execute_once()
            self.assertEqual(result.checkpoint, controller.checkpoint())
        terminal = strict_control_json((journal / "terminal.json").read_bytes())
        self.assertEqual(sha256((journal / "terminal.json").read_bytes()), result.terminal_sha256)
        self.outcomes[control_id].update({"execution_status": result.status,
            "observed_probe_records": len(result.observations), "planned_declared_requests": len(row["targets"]),
            "authenticated_sent_complete_requests": terminal["observed_requests"],
            "unknown_request_outcomes": terminal["unknown_request_outcomes"],
            "missing_step_ids": list(result.missing_step_ids), "infrastructure": list(result.infrastructure),
            "helper_creates": terminal["created_helpers"], "server_creates": terminal["created_servers"],
            "keeper_creates": terminal["created_keepers"], "cleanup_verified": result.cleanup_verified,
            "create_intents": terminal["create_intents"], "unconfirmed_creates": terminal["unconfirmed_creates"],
            "helper_starts": sum(item.get("process", {}).get("completion", {}).get("started") is True
                                 for item in result.observations),
            "server_running_epochs": len(terminal["epochs"]),
            "keeper_starts": None,
            "keeper_running_record_present": (journal / "keeper-running.json").is_file(),
            "terminal_sha256": result.terminal_sha256,
            "authenticated_requests": sum(item["authenticated"] is True for item in result.observations),
            "complete_exchanges": sum(item["wire"] is not None and item["wire"].exchange_complete
                                      for item in result.observations)})
        write_new(root / "physical-census.json", self.outcomes[control_id])
        keeper_running = strict_control_json((journal / "keeper-running.json").read_bytes())
        execution.validate_state(keeper_running, "running")
        self.outcomes[control_id]["keeper_starts"] = 1
        self.assertTrue(checkpoints)
        self.assertEqual(terminal["planned_requests"], len(row["targets"]))
        self.assertEqual(terminal["observed_probe_records"], len(row["targets"]))
        self.assertEqual(terminal["observed_requests"], len(row["targets"]) - (control_id == "M05"))
        self.assertEqual(terminal["unknown_request_outcomes"], 1 if control_id == "M05" else 0)
        self.assertEqual(terminal["unconfirmed_creates"], 0)
        self.assertEqual(terminal["create_intents"], len(row["targets"]) + (2 if control_id == "M07" else 1) + 1)
        self.assertEqual(terminal["created_helpers"], len(row["targets"]))
        self.assertEqual(terminal["created_servers"], 2 if control_id == "M07" else 1)
        self.assertEqual(terminal["created_keepers"], 1)
        self.assertEqual(self.outcomes[control_id]["helper_starts"], len(row["targets"]))
        self.assertIs(result.cleanup_verified, True)
        self.assertTrue(terminal["cleanup"])
        self.assertTrue(all(value is True for value in terminal["cleanup"].values()))
        self.assertIs(terminal["volume_cleanup"], True)
        self.assertEqual(len(result.observations), len(row["targets"]))
        self.assertEqual([item["step_id"] for item in result.observations],
                         [step.step_id for step in recipe.steps if step.kind == "probe"])
        self.assertEqual(len({item["probe_id"] for item in result.observations}), len(row["targets"]))
        self.assertTrue(all(item["probe_removed"] is True for item in result.observations))
        if control_id != "M05":
            self.assertEqual(result.status, "completed", result)
            self.assertEqual(result.missing_step_ids, ())
            self.assertEqual(result.infrastructure, ())
        self.assertEqual(execution.capture_git_source(self.store, self.commit), (self.tree, self.files))
        return root, journal, recipe, policy, result, terminal, checkpoints

    def assert_running_interval(self, before, after, comparison):
        """Reconstruct identity/epoch evidence rather than accepting a matches flag."""
        for value in (before, after):
            execution.validate_state(value, "running")
        self.assertEqual(before["Id"], after["Id"])
        self.assertEqual(before["State"]["StartedAt"], after["State"]["StartedAt"])
        self.assertEqual(before["State"]["Pid"], after["State"]["Pid"])
        self.assertEqual(before["RestartCount"], after["RestartCount"])
        self.assertEqual(before["HostConfig"]["NetworkMode"], "none")
        self.assertEqual(after["HostConfig"]["NetworkMode"], "none")
        left, right = (engine.immutable_inspection(value) for value in (before, after))
        for projection in (left, right):
            mounts = projection["Mounts"]
            self.assertEqual(len(mounts), len({item["Destination"] for item in mounts}))
            projection["Mounts"] = {item["Destination"]: item for item in mounts}
        self.assertEqual(execution.encoded(left), execution.encoded(right))
        self.assertEqual(comparison["before_sha256"], execution.digest(before))
        self.assertEqual(comparison["after_sha256"], execution.digest(after))
        self.assertEqual(comparison["runtime_sha256"], execution.digest(self.runtime))
        self.assertIs(comparison["matches"], True)

    def authenticated_wire(self, journal, recipe, policy, observation):
        self.assertIs(observation["authenticated"], True)
        self.assertIsNotNone(observation["wire"])
        self.assertEqual(observation["commit_oid"], self.commit)
        self.assertEqual(observation["tree_oid"], self.tree)
        self.assertEqual(observation["source_sha256"], execution.source_sha256(self.files))
        self.assertEqual(observation["helper_sha256"], wire.helper_sha256())
        self.assertEqual(observation["binding"]["purpose"], PURPOSE)
        self.assertEqual(observation["binding"]["recipe_sha256"], execution.digest(recipe.record()))
        self.assertEqual(observation["binding"]["limits_sha256"], execution.digest(asdict(policy)))
        label = observation["label"]
        process = observation["process"]
        self.assertEqual(process["status"], "completed")
        self.assertIs(process["completion"]["natural"], True)
        self.assertEqual(process["completion"]["inspect_exit_code"], 0)
        self.assertIs(process["capture_complete"], True)
        self.assertIs(process["attach_eof_after_start_confirmation"], True)
        self.assertEqual(retained_bytes(self, journal, process["stderr"]), b"")
        raw = retained_bytes(self, journal, process["stdout"])
        step = recipe.steps[observation["step_index"]]
        request = strict_control_json(step.request_json)
        observed = wire.decode_probe_output(raw, request, PORT, policy.wire_limits)
        self.assertEqual(observation["wire"], observed)
        self.assertIs(observed.sent_complete, True)
        self.assertIs(observed.connected, True)
        self.assertEqual(observed.listeners_before_stage, "connected_pre_request")
        self.assertEqual(observed.sent, wire.request_bytes(request, PORT, policy.wire_limits))
        self.assertEqual(observed.request, observed.sent)
        self.assertEqual(strict_control_json(raw)["connect_attempts"] >= 1, True)
        probe_created = retained_engine_json(self, journal, label + "-probe-created")
        probe_final = retained_engine_json(self, journal, label + "-probe-final")
        declared = strict_control_json((journal / (label + "-probe-create-intent.json")).read_bytes())
        spec = role_spec(declared["spec"])
        for value in (probe_created, probe_final):
            execution.validate_role(value, spec, RUNTIME_IMAGE, self.runtime)
            self.assertEqual(value["Id"], observation["probe_id"])
            self.assertEqual(value["HostConfig"]["NetworkMode"], "container:" + observation["server_id"])
            self.assertFalse(value["HostConfig"].get("PidMode"))
            self.assertFalse(value["HostConfig"].get("PortBindings"))
            self.assertEqual({item["Destination"] for item in value["Mounts"]}, {"/probe"})
        execution.validate_state(probe_created, "created")
        execution.validate_state(probe_final, "exited")
        self.assertEqual(probe_final["State"]["ExitCode"], 0)
        start_step = next(item for item in strict_control_json((journal / "terminal.json").read_bytes())["steps"]
                          if item["kind"] == "start" and item["server_id"] == observation["server_id"])
        baseline = strict_control_json((journal / (start_step["label"] + "-running.json")).read_bytes())
        before = retained_engine_json(self, journal, label + "-server-before")
        after = retained_engine_json(self, journal, label + "-server-after")
        self.assert_running_interval(baseline, before, observation["continuity_before"])
        self.assert_running_interval(baseline, after, observation["continuity_after"])
        keeper = strict_control_json((journal / "keeper-running.json").read_bytes())
        keeper_after = retained_engine_json(self, journal, label + "-probe-keeper-after")
        self.assert_running_interval(keeper, keeper_after, observation["keeper_continuity"])
        self.assertEqual(keeper["Id"], observation["keeper_id"])
        self.assertNotIn(observation["probe_id"], (observation["server_id"], observation["keeper_id"]))
        self.assertEqual(observation["database_path"], "/tmp/library.sqlite")
        for value in (baseline, before, after):
            mounts = {item["Destination"]: item for item in value["Mounts"]}
            self.assertEqual(set(mounts), {"/workspace", "/inputs", "/tmp"})
            self.assertEqual(mounts["/tmp"]["Name"], observation["database_volume"])
            self.assertFalse(mounts["/workspace"]["RW"] or mounts["/inputs"]["RW"])
        return observed

    def assert_listener(self, observed, address="127.0.0.1"):
        for snapshot in (observed.listeners_before, observed.listeners_after):
            self.assertIs(snapshot.complete, True, snapshot)
            self.assertEqual(snapshot.limitations, ())
            self.assertTrue(snapshot.tcp and snapshot.tcp6)
            self.assertEqual(snapshot.listeners, (("tcp", address, PORT),))

    def test_m01_complete_lawful_frames_and_error(self):
        root, journal, recipe, policy, result, _, _ = self.run_history("M01")
        observed = [self.authenticated_wire(journal, recipe, policy, item) for item in result.observations]
        for item in observed:
            self.assertIs(item.exchange_complete, True)
            self.assertTrue(item.response.framing_complete and item.response.body_complete)
            self.assertEqual(item.limitations, ())
            self.assert_listener(item)
        self.assertEqual([item.response.framing for item in observed],
                         ["content-length", "chunked", "close-delimited", "content-length"])
        for item in observed[:3]:
            self.assertEqual(item.response.status_code, 200)
            self.assertEqual(json.loads(item.response.body), SEMANTIC_VALUE)
        self.assertFalse(observed[0].socket_eof or observed[1].socket_eof)
        self.assertEqual([item.termination for item in observed[:2]], ["message_complete", "message_complete"])
        self.assertIs(observed[2].socket_eof, True)
        self.assertEqual(observed[3].response.status_code, 409)
        self.assertEqual(json.loads(observed[3].response.body), {"error": "job_conflict"})
        self.finish_control("M01", {"lawful_framing": True, "synthetic_json_equivalence": True,
            "framed_completion_without_eof": True, "complete_error_is_response": True})

    def test_m02_known_synthetic_mismatch_survives_incomplete_sibling(self):
        root, journal, recipe, policy, result, _, _ = self.run_history("M02")
        first, second = [self.authenticated_wire(journal, recipe, policy, item) for item in result.observations]
        self.assertIs(first.exchange_complete, True)
        self.assertIs(second.exchange_complete, False)
        self.assertEqual(second.response.limitation, "incomplete_content_length")
        self.assertEqual(second.termination, "eof")
        self.assertFalse(second.response.body_complete)
        self.assertIn(b'{"error":"job_conflict"}', second.received)
        facets = {"synthetic_marker_matches": json.loads(first.response.body) == {"marker": MARKER},
                  "later_complete_error": None}
        self.assertIs(facets["synthetic_marker_matches"], False)
        self.assertIsNone(facets["later_complete_error"])
        write_new(root / "separate-facets.json", {"facets": facets, "known_mismatch": ["synthetic_marker_matches"],
            "unavailable": ["later_complete_error"], "product_assertions": [], "acceptance_credit": False})
        self.finish_control("M02", {"known_synthetic_mismatch_retained": True,
                                   "incomplete_valid_json_prefix_unavailable": True})

    def test_m03_close_delimited_without_eof_is_unavailable(self):
        _, journal, recipe, policy, result, _, _ = self.run_history("M03")
        observed = self.authenticated_wire(journal, recipe, policy, result.observations[0])
        self.assertFalse(observed.exchange_complete or observed.socket_eof or observed.response.body_complete)
        self.assertEqual(observed.termination, "timeout")
        self.assertEqual(observed.response.limitation, "awaiting_close")
        self.assertIn(b'{"ready":true}', observed.received)
        self.finish_control("M03", {"deadline_left_eof_unobserved": True, "product_latency_failure_claimed": False})

    def test_m04_exact_wire_cap_and_overflow(self):
        _, journal, recipe, policy, result, _, _ = self.run_history("M04")
        exact, overflow = [self.authenticated_wire(journal, recipe, policy, item) for item in result.observations]
        self.assertEqual(len(exact.received), WIRE_CAP)
        self.assertEqual(len(exact.response.body), 4009)
        self.assertTrue(exact.exchange_complete and exact.response.body_complete)
        self.assertEqual(len(overflow.received), WIRE_CAP + 1)
        self.assertFalse(overflow.exchange_complete)
        self.assertEqual(overflow.termination, "response_limit")
        self.assertIn("response_limit", overflow.limitations)
        self.finish_control("M04", {"exact_declared_cap_complete": True, "one_byte_overflow_unavailable": True,
                                   "product_response_size_limit_claimed": False})

    def test_m05_server_exit_preserves_raw_but_breaks_attribution(self):
        _, journal, recipe, policy, result, _, _ = self.run_history("M05")
        observation = result.observations[0]
        self.assertEqual(result.status, "observation_unavailable")
        self.assertTrue(result.infrastructure)
        self.assertIs(observation["authenticated"], False)
        self.assertIsNone(observation["wire"])
        self.assertIs(observation["continuity_before"]["matches"], True)
        self.assertIn("observation_error", observation)
        final = retained_engine_json(self, journal, observation["label"] + "-server-after")
        self.assertEqual(final["Id"], observation["server_id"])
        execution.validate_state(final, "exited")
        self.assertEqual(final["State"]["ExitCode"], 17)
        process = observation["process"]
        self.assertEqual(process["status"], "completed")
        raw = retained_bytes(self, journal, process["stdout"])
        # Retained helper bytes can be decoded diagnostically. Broken server
        # continuity means they remain outside authenticated product evidence.
        diagnostic = wire.decode_probe_output(raw, strict_control_json(recipe.steps[1].request_json), PORT, policy.wire_limits)
        self.assertIs(diagnostic.sent_complete, True)
        self.assertIn(b'{"ready":', diagnostic.received)
        self.assertFalse(diagnostic.exchange_complete or diagnostic.response.body_complete)
        self.outcomes["M05"]["diagnostic_only_sent_complete_requests"] = 1
        self.finish_control("M05", {"real_server_exit_observed": True, "raw_prefix_retained": True,
                                   "server_attribution_unavailable": True, "domain_rejection_claimed": False})

    def test_m06_loopback_reachability_does_not_hide_wildcard_listener(self):
        root, journal, recipe, policy, result, _, _ = self.run_history("M06")
        observed = self.authenticated_wire(journal, recipe, policy, result.observations[0])
        self.assertTrue(observed.exchange_complete)
        self.assertEqual(json.loads(observed.response.body), {"ready": True})
        self.assert_listener(observed, "0.0.0.0")
        write_new(root / "independent-listener-facet.json", {"loopback_request_completed": True,
            "loopback_only_listener": False, "before": observed.listeners_before.listeners,
            "after": observed.listeners_after.listeners, "product_acceptance_credit": False})
        self.finish_control("M06", {"loopback_wire_complete": True, "wildcard_listener_detected_independently": True})

    def test_m07_explicit_stop_restart_reopens_same_sqlite_db(self):
        root, journal, recipe, policy, result, terminal, checkpoints = self.run_history("M07")
        observed = [self.authenticated_wire(journal, recipe, policy, item) for item in result.observations]
        for item in observed:
            self.assertTrue(item.exchange_complete)
            self.assertEqual(json.loads(item.response.body), {"marker": MARKER})
            self.assert_listener(item)
        first, second = terminal["epochs"]
        self.assertEqual([first["epoch"], second["epoch"]], [1, 2])
        self.assertNotEqual(first["server_id"], second["server_id"])
        self.assertNotEqual(first["started_at"], second["started_at"])
        for key in ("database_volume", "database_path", "source_sha256", "fixture_sha256", "keeper_id", "keeper_started_at"):
            self.assertEqual(first[key], second[key])
        stop = terminal["steps"][2]
        self.assertEqual(stop["kind"], "stop")
        self.assertIs(stop["removed"], True)
        self.assertEqual(stop["stop"]["container_id"], first["server_id"])
        self.assertEqual(stop["stop"]["status"], 204)
        self.assertIs(stop["stop"]["natural_exit_zero_required"], False)
        first_seen = {}
        for index, checkpoint in enumerate(checkpoints):
            for name, digest in checkpoint.files:
                self.assertEqual(sha256((journal / name).read_bytes()), digest)
                first_seen.setdefault(name, index)
        absence = strict_control_json((journal / "step-002-retire-absence.json").read_bytes())
        self.assertEqual(absence["exit_code"], 0)
        self.assertEqual(retained_bytes(self, journal, absence["stdout"]), b"")
        self.assertLess(first_seen["step-002-retire-absence.json"], first_seen["step-003-start-intent.json"])
        keeper_before = retained_engine_json(self, journal, "step-002-keeper-after")
        keeper_after = retained_engine_json(self, journal, "step-003-keeper-before")
        for key in ("Id", "RestartCount"):
            self.assertEqual(keeper_before[key], keeper_after[key])
        for key in ("Pid", "StartedAt"):
            self.assertEqual(keeper_before["State"][key], keeper_after["State"][key])
        write_new(root / "restart-boundary.json", {"epochs": terminal["epochs"],
            "old_server_absence_checkpoint": first_seen["step-002-retire-absence.json"],
            "new_server_start_intent_checkpoint": first_seen["step-003-start-intent.json"],
            "same_held_database": True, "crash_recovery_claimed": False})
        self.finish_control("M07", {"explicit_replacement_with_old_server_absent": True,
                                   "same_keeper_db_and_source": True, "sqlite_marker_reopened": True})

    def test_m09_conflicting_content_lengths_are_unavailable(self):
        _, journal, recipe, policy, result, _, _ = self.run_history("M09")
        observed = self.authenticated_wire(journal, recipe, policy, result.observations[0])
        self.assertFalse(observed.exchange_complete or observed.response.framing_complete or observed.response.body_complete)
        self.assertEqual(observed.termination, "parse_stopped")
        self.assertEqual(observed.response.limitation, "conflicting_content_length")
        self.assertIn(b"Content-Length: 2\r\nContent-Length: 3\r\n", observed.received)
        self.assertIn(b"Content-Length: 2\r\nContent-Length: 3\r\n", observed.response.raw_headers)
        self.finish_control("M09", {"actual_conflicting_headers_captured": True,
                                   "ambiguous_framing_unavailable": True, "eof_or_full_body_inferred": False})

    def test_m08_wrong_probe_network_rejected_before_start(self):
        root = self.begin_control("M08")
        nonce = "gossip-http-m08-" + uuid.uuid4().hex[:16]
        volume = nonce + "-db"
        directories = {name: root / name for name in ("source", "inputs", "probe")}
        for directory in directories.values():
            directory.mkdir(mode=0o755)
        staged = {"source": self.files, "inputs": {}, "probe": {
            "helper.py": wire.helper_source(), "request.json": wire.build_probe_input(
                request_recipe("/ready"), PORT, policy_for("M08").wire_limits)}}
        for role, files in staged.items():
            for name, raw in files.items():
                path = directories[role] / name
                with path.open("xb") as stream:
                    stream.write(raw)
                    stream.flush()
                    os.fsync(stream.fileno())
                path.chmod(0o444)

        def verify_staging(label):
            manifest = {}
            for role, files in staged.items():
                directory = directories[role]
                self.assertEqual({path.name for path in directory.iterdir()}, set(files))
                for name, raw in files.items():
                    path = directory / name
                    self.assertFalse(path.is_symlink())
                    self.assertTrue(path.is_file())
                    self.assertEqual(path.read_bytes(), raw)
                manifest[role] = {name: {"bytes": len(raw), "sha256": sha256(raw)} for name, raw in files.items()}
            self.assertEqual(execution.capture_git_source(self.store, self.commit), (self.tree, self.files))
            write_new(root / (label + ".json"), {"manifest": manifest,
                "source_commit": self.commit, "source_tree": self.tree, "helper_sha256": wire.helper_sha256()})
            return manifest

        staged_before = verify_staging("stage-before-dispatch")
        labels = (("gossip.execution", nonce), ("gossip.source", execution.source_sha256(self.files)),
                  ("gossip.purpose", PURPOSE), ("gossip.control", "M08"))
        keeper_spec = execution.RoleSpec("keeper", nonce + "-keeper",
            ("python", "-c", "import time;time.sleep(120)"), labels,
            (), volume=volume)
        server_spec = execution.RoleSpec("server", nonce + "-server", server_argv(), labels,
            (("/workspace", str(directories["source"].resolve())),
             ("/inputs", str(directories["inputs"].resolve()))), volume=volume)
        write_new(root / "prospective-registration.json", {
            "control": self.controls["M08"], "ordered_roster_sha256": self.roster_sha256,
            "source_commit": self.commit, "source_tree": self.tree,
            "source_sha256": execution.source_sha256(self.files), "runtime": self.runtime,
            "helper_sha256": wire.helper_sha256(), "policy": asdict(policy_for("M08")),
            "keeper": asdict(keeper_spec), "server": asdict(server_spec),
            "volume": volume, "volume_options": execution.VOLUME_OPTIONS,
            "declared_requests": 0, "declared_helper_starts": 0, "purpose": PURPOSE,
        })
        created = []
        claimed = []
        cleanup = []
        cleanup_errors = []
        volume_created = False
        volume_claimed = False
        volume_absent = False
        try:
            before = self.checked_command(root, "volume-before", ["volume", "ls", "--filter",
                "name=^" + volume + "$", "--format", "{{.Name}}"])
            self.assertEqual(retained_bytes(self, root, before["stdout"]), b"")
            arguments = ["volume", "create", "--driver", "local", "--label", "gossip.execution=" + nonce]
            for key, value in execution.VOLUME_OPTIONS.items():
                arguments.extend(["--opt", key + "=" + value])
            arguments.append(volume)
            volume_claimed = True
            self.checked_command(root, "volume-create", arguments)
            volume_created = True
            volume_record = self.checked_command(root, "volume-created", ["volume", "inspect", "--format", "{{json .}}", volume])
            volume_value = strict_control_json(retained_bytes(self, root, volume_record["stdout"]))
            self.assertEqual(volume_value["Name"], volume)
            self.assertEqual(volume_value["Options"], execution.VOLUME_OPTIONS)
            self.assertEqual(volume_value["Driver"], "local")
            running = {}
            for spec in (keeper_spec, server_spec):
                before = self.checked_command(root, spec.role + "-before", ["container", "ls", "--all",
                    "--filter", "name=^/" + spec.name + "$", "--format", "{{.ID}}"])
                self.assertEqual(retained_bytes(self, root, before["stdout"]), b"")
                claimed.append(spec)
                record = self.checked_command(root, spec.role + "-create", execution.create_argv(spec, RUNTIME_IMAGE)[1:])
                container_id = self.assert_created_id(root, record)
                created.append((spec.role, container_id))
                before = self.inspected(root, spec.role + "-created", container_id)
                execution.validate_role(before, spec, RUNTIME_IMAGE, self.runtime)
                execution.validate_state(before, "created")
                self.checked_command(root, spec.role + "-start", ["start", container_id])
                after = self.inspected(root, spec.role + "-running", container_id)
                execution.validate_role(after, spec, RUNTIME_IMAGE, self.runtime)
                execution.validate_state(after, "running")
                comparison = execution.role_identity_comparison(before, after, spec, self.runtime, "created-to-running")
                self.assertIs(comparison["matches"], True)
                write_new(root / (spec.role + "-continuity.json"), comparison)
                running[spec.role] = after
            probe_spec = execution.RoleSpec("probe", nonce + "-probe", execution.PROBE_ARGV, labels,
                (("/probe", str(directories["probe"].resolve())),), server_id=running["server"]["Id"])
            correct_argv = execution.create_argv(probe_spec, RUNTIME_IMAGE)
            wrong_argv = list(correct_argv)
            index = wrong_argv.index("--network=container:" + running["server"]["Id"])
            wrong_argv[index] = "--network=none"
            self.assertEqual([index], [i for i, (left, right) in enumerate(zip(correct_argv, wrong_argv)) if left != right])
            write_new(root / "probe-prospective-role.json", {
                "expected_role": asdict(probe_spec), "correct_argv": correct_argv,
                "actual_create_argv": wrong_argv, "single_defect": "HostConfig.NetworkMode",
            })
            before = self.checked_command(root, "probe-before", ["container", "ls", "--all",
                "--filter", "name=^/" + probe_spec.name + "$", "--format", "{{.ID}}"])
            self.assertEqual(retained_bytes(self, root, before["stdout"]), b"")
            claimed.append(probe_spec)
            record = self.checked_command(root, "probe-create", wrong_argv[1:])
            probe_id = self.assert_created_id(root, record)
            created.append(("probe", probe_id))
            wrong = self.inspected(root, "probe-created", probe_id)
            execution.validate_state(wrong, "created")
            self.assertEqual(wrong["HostConfig"]["NetworkMode"], "none")
            with self.assertRaisesRegex(execution.ExecutionError, "namespace/resource"):
                execution.validate_role(wrong, probe_spec, RUNTIME_IMAGE, self.runtime)
            # Physical rejection above uses the actual untouched inspection.
            # This separate host-only contrast checks that no second profile
            # defect explains the rejection; it is not a physical observation.
            network_only = strict_control_json(encoded(wrong))
            network_only["HostConfig"]["NetworkMode"] = "container:" + probe_spec.server_id
            execution.validate_role(network_only, probe_spec, RUNTIME_IMAGE, self.runtime)
            after_rejection = self.inspected(root, "probe-after-rejection", probe_id)
            execution.validate_state(after_rejection, "created")
            self.assertEqual(encoded(wrong), encoded(after_rejection))
            for spec in (server_spec, keeper_spec):
                after = self.inspected(root, spec.role + "-after-rejection", running[spec.role]["Id"])
                execution.validate_state(after, "running")
                comparison = execution.role_identity_comparison(running[spec.role], after, spec,
                                                                self.runtime, "running-to-running")
                self.assertIs(comparison["matches"], True)
                write_new(root / (spec.role + "-after-rejection-continuity.json"), comparison)
            write_new(root / "observed-rejection.json", {
                "validator": "candidate_http_execution_v1.validate_role",
                "actual_inspection_sha256": sha256(encoded(wrong)), "expected_role": asdict(probe_spec),
                "probe_state_before": wrong["State"], "probe_state_after": after_rejection["State"],
                "declared_request_count": 0, "helper_start_count": 0,
                "server_start_count": 1, "keeper_start_count": 1, "acceptance_credit": False,
            })
            self.assertEqual(staged_before, verify_staging("stage-after-observation"))
        finally:
            # A durable creation intent may precede an incomplete response.
            # Reconcile only our previously absent name and independent labels;
            # never invent a successful creation or omit an uncertain resource.
            for spec in claimed:
                if any(role == spec.role for role, _ in created):
                    continue
                try:
                    record = retained_docker_command(root, spec.role + "-uncertain-inspect", self.endpoint,
                                                    ["inspect", "--format", "{{json .}}", spec.name])
                    if record["exit_code"] == 0 and record["capture_complete"] and not record["timed_out"]:
                        value = strict_control_json(retained_bytes(self, root, record["stdout"]))
                        self.assertEqual(value["Name"], "/" + spec.name)
                        self.assertEqual(value["Image"], RUNTIME_IMAGE)
                        self.assertEqual(value["Config"]["Labels"], dict(spec.labels))
                        self.assertRegex(value["Id"], r"^[0-9a-f]{64}$")
                        created.append((spec.role, value["Id"]))
                    else:
                        absent = self.checked_command(root, spec.role + "-uncertain-absence", ["container", "ls", "--all",
                            "--filter", "name=^/" + spec.name + "$", "--format", "{{.ID}}"])
                        self.assertEqual(retained_bytes(self, root, absent["stdout"]), b"")
                        cleanup.append({"role": spec.role, "container_id": None, "creation_unproven": True,
                                        "absence_verified": True})
                except (OSError, ValueError, AssertionError, subprocess.SubprocessError) as error:
                    cleanup_errors.append({"role": spec.role, "operation": "uncertain-create", "error": str(error)[:512]})
            for role, container_id in sorted(created, key=lambda pair: {"probe": 0, "server": 1, "keeper": 2}[pair[0]]):
                spec = next(item for item in claimed if item.role == role)
                try:
                    owned = self.inspected(root, role + "-cleanup-owned", container_id)
                    self.assertEqual(owned["Name"], "/" + spec.name)
                    self.assertEqual(owned["Image"], RUNTIME_IMAGE)
                    self.assertEqual(owned["Config"]["Labels"], dict(spec.labels))
                except (OSError, ValueError, AssertionError, subprocess.SubprocessError) as error:
                    cleanup_errors.append({"role": role, "operation": "ownership", "error": str(error)[:512]})
                    cleanup.append({"role": role, "container_id": container_id, "absence_verified": False,
                                    "removal_refused": "ownership unavailable"})
                    continue
                stopped_proven = role == "probe"
                if role != "probe":
                    try:
                        self.checked_command(root, role + "-stop", ["stop", "--time", "5", container_id])
                        stopped = self.inspected(root, role + "-stopped", container_id)
                        execution.validate_state(stopped, "exited")
                        stopped_proven = True
                    except (OSError, ValueError, AssertionError, subprocess.SubprocessError) as error:
                        cleanup_errors.append({"role": role, "operation": "stop", "error": str(error)[:512]})
                absent_proven = False
                try:
                    self.checked_command(root, role + "-remove", ["rm", *([] if stopped_proven else ["--force"]), container_id])
                    absent = self.checked_command(root, role + "-absent", ["container", "ls", "--all", "--no-trunc",
                        "--filter", "id=" + container_id, "--format", "{{.ID}}"])
                    self.assertEqual(retained_bytes(self, root, absent["stdout"]), b"")
                    absent_proven = True
                except (OSError, ValueError, AssertionError, subprocess.SubprocessError) as error:
                    cleanup_errors.append({"role": role, "operation": "remove", "error": str(error)[:512]})
                cleanup.append({"role": role, "container_id": container_id,
                    "stop_proven_or_never_started": stopped_proven,
                    "forced_remove_requested": not stopped_proven, "absence_verified": absent_proven})
            if volume_claimed:
                try:
                    record = retained_docker_command(root, "volume-cleanup-inspect", self.endpoint,
                        ["volume", "inspect", "--format", "{{json .}}", volume])
                    if record["exit_code"] == 0 and record["capture_complete"] and not record["timed_out"]:
                        value = strict_control_json(retained_bytes(self, root, record["stdout"]))
                        self.assertEqual(value["Name"], volume)
                        self.assertEqual(value["Labels"], {"gossip.execution": nonce})
                        self.assertEqual(value["Options"], execution.VOLUME_OPTIONS)
                        self.checked_command(root, "volume-remove", ["volume", "rm", volume])
                    absent = self.checked_command(root, "volume-absent", ["volume", "ls", "--filter",
                        "name=^" + volume + "$", "--format", "{{.Name}}"])
                    self.assertEqual(retained_bytes(self, root, absent["stdout"]), b"")
                    volume_absent = True
                except (OSError, ValueError, AssertionError, subprocess.SubprocessError) as error:
                    cleanup_errors.append({"role": "volume", "operation": "remove", "error": str(error)[:512]})
            write_new(root / "cleanup.json", {"containers": cleanup, "volume_created": volume_created,
                "volume_claimed": volume_claimed,
                "volume_absence_verified": volume_absent, "errors": cleanup_errors, "acceptance_credit": False})
        self.assertEqual(cleanup_errors, [])
        self.assertTrue(volume_absent)
        commands = [strict_control_json(path.read_bytes())["argv"] for path in sorted(root.glob("*-intent.json"))]
        start_commands = [argv for argv in commands if argv[3] == "start"]
        self.assertEqual([argv[-1] for argv in start_commands], [running["keeper"]["Id"], running["server"]["Id"]])
        self.assertNotIn(probe_id, [argv[-1] for argv in start_commands])
        self.outcomes["M08"].update({"planned_declared_requests": 0, "observed_probe_records": 0,
            "authenticated_sent_complete_requests": 0, "unknown_request_outcomes": 0,
            "helper_creates": 1, "helper_starts": 0, "server_creates": 1, "server_running_epochs": 1,
            "keeper_creates": 1, "keeper_starts": 1, "create_intents": len(claimed),
            "unconfirmed_creates": len(claimed) - len(created), "cleanup": cleanup})
        self.finish_control("M08", {"real_wrong_role_rejected_before_start": True,
            "probe_stayed_created": True, "no_probe_start_or_http_request": True,
            "running_server_and_keeper_continuity": True, "owned_cleanup_verified": True})
