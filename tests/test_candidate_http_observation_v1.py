"""Portable authored journal controls; no candidate, Git, Engine or network runs.

The fixture is generated from literal bytes and pure profile predicates. Rehashing
a mutated fixture tests reconstruction, never independent physical authority.
"""
from __future__ import annotations

import base64
from copy import deepcopy
from dataclasses import asdict, FrozenInstanceError, replace
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from gossip_harness import candidate_http_execution_v2 as execution
from gossip_harness import candidate_http_observation_v1 as observer
from gossip_harness import candidate_http_semantics_v1 as semantics
from gossip_harness import candidate_http_transport_v1 as wire
from tests.test_candidate_http_execution_v2 import IMAGE, REQUIREMENTS, RUNTIME, inspection


ENVIRONMENT = {"PATH": "/synthetic/bin", "HOME": "/synthetic/home"}
EXTERNAL_EVENTS = "original-source-bound-controller-and-external-checkpoint-sink"
TCP_HEADER = b"  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode\n"
HEALTH_BODY = b'{"status":"ok","schema":0}'
HEALTH_RESPONSE = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: " + str(len(HEALTH_BODY)).encode() + b"\r\n\r\n" + HEALTH_BODY


def b64(raw):
    return base64.b64encode(raw).decode("ascii")


def object_oid(kind, raw):
    return hashlib.sha1(kind.encode() + b" " + str(len(raw)).encode() + b"\0" + raw).hexdigest()


def request():
    return {"method": "GET", "target": "/health", "headers": [["Host", "127.0.0.1:8765"],
        ["Connection", "keep-alive"], ["Content-Length", "0"]], "body_b64": ""}


def snapshot(*, wildcard=False, missing_tcp6=False):
    address = b"00000000" if wildcard else b"0100007F"
    tcp = TCP_HEADER + b"  0: " + address + b":223D 00000000:0000 0A 00000000:00000000 00:00000000 00000000 65534 0 42\n"
    return {"byteorder": "little", "tables": {
        "tcp": {"status": "ok", "raw": wire._record(tcp), "errno": None},
        "tcp6": {"status": "unavailable", "raw": wire._record(b""), "errno": 2} if missing_tcp6 else
            {"status": "ok", "raw": wire._record(TCP_HEADER), "errno": None}}}


def transcript(response=HEALTH_RESPONSE, *, termination="message_complete", eof=False,
               wildcard=False, missing_tcp6=False, connected=True, sent=None, limits=None):
    limits = limits or wire.WireLimits()
    sent = wire.request_bytes(request(), 8765, limits) if sent is None else sent
    snap = snapshot(wildcard=wildcard, missing_tcp6=missing_tcp6)
    return wire.encoded({"protocol": wire.PROTOCOL,
        "input_sha256": execution.sha256(wire.build_probe_input(request(), 8765, limits)),
        "request_sha256": execution.sha256(wire.request_bytes(request(), 8765, limits)),
        "sent": wire._record(sent), "received": wire._record(response), "connected": connected,
        "socket_eof": eof, "termination": termination, "errno": None, "elapsed_seconds": 0.01,
        "io_operations": 3, "connect_attempts": 1, "connect_refused_attempts": 0,
        "listeners_before_stage": "connected_pre_request" if connected else "connection_failed_diagnostic",
        "listeners_before": snap, "listeners_after": snap})


class SyntheticJournal:
    """A complete authored lifecycle trace, explicitly incapable of physical proof."""

    def __init__(self, root, *, payloads=None, chunked=False, cleanup=True, limits=None, unknown_last=False):
        self.root = root
        self.root.mkdir()
        self.files = {}
        self.order = []
        self.chunked = chunked
        self.unknown_last = unknown_last
        self.policy = execution.HttpPolicy(IMAGE, wire_limits=limits or wire.WireLimits())
        self.sources = {"server.py": b"raise RuntimeError('inert fixture must not execute')\n"}
        payloads = payloads or [transcript()]
        steps = [execution.HttpStep("start-1", "start")]
        steps += [execution.HttpStep("request-" + str(i + 1), "probe", execution.encoded(request()))
                  for i in range(len(payloads))]
        steps.append(execution.HttpStep("stop-1", "stop"))
        self.recipe = execution.HttpRecipe("synthetic-http", ("python", "/workspace/server.py", "--db",
            "/tmp/library.sqlite", "--root", "/inputs", "--port", "8765"), (), (), tuple(steps))
        tree = b"100644 server.py\0" + bytes.fromhex(object_oid("blob", self.sources["server.py"]))
        tree_oid = object_oid("tree", tree)
        commit = ("tree " + tree_oid + "\nauthor Fixture <fixture@example.invalid> 0 +0000\n"
                  "committer Fixture <fixture@example.invalid> 0 +0000\n\nAuthored inert fixture\n").encode()
        with patch.object(execution.DockerValidator, "_environment", return_value=ENVIRONMENT):
            binding = execution.binding_for(self.sources, self.recipe, self.policy, RUNTIME,
                                           requirements_sha256=REQUIREMENTS)
        self.registration = execution.HttpRegistration(binding, object_oid("commit", commit), tree_oid,
                                                       "synthetic-repetition")
        self.definition = {"kind": "journal", "control_id": "synthetic",
            "registration": {"registration": asdict(self.registration), "recipe": self.recipe.record(),
                "policy": asdict(self.policy), "ordered_roster_sha256": execution.digest([s.record() for s in steps])},
            "source_files": {name: b64(raw) for name, raw in self.sources.items()}, "fixture_files": {},
            "git_commit_payload_b64": b64(commit), "environment_sha256": binding.environment_sha256,
            "docker_command_environment_sha256": execution.digest(ENVIRONMENT),
            "temporal_authority": EXTERNAL_EVENTS}
        self.config = {"protocol": execution.PROTOCOL, "mode": "physical", "root": str(root),
            "repository": "/synthetic/repository.git", "registration": asdict(self.registration),
            "policy": asdict(self.policy), "endpoint": RUNTIME["endpoint"], "runtime": RUNTIME,
            "source_manifest": execution.source_manifest(self.sources),
            "evaluator_sources": execution.evaluator_sources(), "role_policy": execution.role_policy_identity(),
            "recipe": self.recipe.record(), "snapshot_protocol": execution.SNAPSHOT_PROTOCOL,
            "helper_sha256": wire.helper_sha256(), "helper_stdout_envelope": wire.max_probe_output_bytes(self.policy.wire_limits)}
        self.put("config.json", self.config)
        self.intent = {"protocol": execution.PROTOCOL, "execution_id": "http-synthetic",
            "role_policy": execution.role_policy_identity(), "config_sha256": execution.digest(self.config),
            "registration_sha256": execution.digest(asdict(self.registration)),
            "volume": "gossip-http-synthetic-volume", "keeper": "gossip-http-synthetic-keeper",
            "containers": ["gossip-http-synthetic-" + str(i) for i in range(len(steps))],
            "planned_steps": len(steps), "planned_requests": len(payloads), "planned_servers": 1, "planned_keepers": 1}
        self.put("intent.json", self.intent)
        self.stage = {"workspace": "/synthetic/stage/workspace", "inputs": "/synthetic/stage/inputs",
            "source_manifest": execution.source_manifest(self.sources), "fixture_manifest": []}
        self.put("staging.json", self.stage)
        volume = self.intent["volume"]
        self.command("volume-before", ["docker", "volume", "ls", "--quiet", "--filter", "name=^" + volume + "$"], b"")
        volume_argv = ["docker", "volume", "create", "--driver", "local", "--label", "gossip.execution=http-synthetic",
                       "--label", "gossip.snapshot=" + execution.SNAPSHOT_PROTOCOL]
        for key, value in execution.VOLUME_OPTIONS.items():
            volume_argv += ["--opt", key + "=" + value]
        volume_argv.append(volume)
        self.put("volume-intent.json", {"argv": volume_argv, "volume": volume})
        self.command("volume-create", volume_argv, volume.encode() + b"\n")
        self.volume = {"Name": volume, "Driver": "local", "Options": execution.VOLUME_OPTIONS,
            "Labels": {"gossip.execution": "http-synthetic", "gossip.snapshot": execution.SNAPSHOT_PROTOCOL}}
        self.command("volume-created", ["docker", "volume", "inspect", "--format", "{{json .}}", volume], execution.encoded(self.volume))
        self.keeper_spec = self.spec("keeper", self.intent["keeper"], "state-lifetime", 0)
        self.keeper = self.create_start("keeper", self.keeper_spec, "1" * 64)
        self.boundary("step-000-keeper-before", self.keeper, self.keeper_spec)
        self.server_spec = self.spec("server", self.intent["containers"][0], "start-1", 1)
        self.server = self.create_start("step-000", self.server_spec, "2" * 64)
        self.epoch = {"epoch": 1, "server_id": self.server["Id"], "name": self.server_spec.name,
            "started_at": self.server["State"]["StartedAt"], "pid": self.server["State"]["Pid"],
            "database_volume": volume, "database_path": self.recipe.database_path,
            "source_sha256": binding.source_sha256, "fixture_sha256": binding.fixture_sha256,
            "running_inspection_sha256": execution.digest(self.server), "keeper_id": self.keeper["Id"],
            "keeper_started_at": self.keeper["State"]["StartedAt"]}
        start_row = {"step_id": "start-1", "step_index": 0, "kind": "start", "label": "step-000", **self.epoch}
        self.boundary("step-000-keeper-after", self.keeper, self.keeper_spec)
        self.put("step-000-result.json", start_row)
        rows, observations, removed = [start_row], [], {}
        for i, payload in enumerate(payloads, 1):
            row, spec = self.probe(i, payload, unknown=unknown_last and i == len(payloads))
            observations.append(row)
            if row["authenticated"]:
                rows.append(row)
            removed[spec.name] = True
        if not unknown_last:
            i = len(steps) - 1
            label = "step-" + str(i).zfill(3)
            self.boundary(label + "-keeper-before", self.keeper, self.keeper_spec)
            stop = self.stop(label + "-stop", self.server, self.server_spec)
            self.remove(label + "-retire", self.server, self.server_spec, force=False)
            self.boundary(label + "-keeper-after", self.keeper, self.keeper_spec)
            stop_row = {"step_id": "stop-1", "step_index": i, "kind": "stop", "label": label,
                        "epoch": 1, "server_id": self.server["Id"], "stop": stop, "removed": True}
            rows.append(stop_row)
            self.put(label + "-result.json", stop_row)
        else:
            stopped = inspection(self.server_spec, self.server["Id"], "exited")
            stopped["State"]["ExitCode"] = 17
            self.remove("cleanup-" + str(len(payloads)).zfill(3), stopped, self.server_spec, force=True)
        removed[self.server_spec.name] = True
        self.stop("keeper-stop", self.keeper, self.keeper_spec)
        self.remove("cleanup-" + str(len(payloads) + 1).zfill(3), self.keeper, self.keeper_spec, force=True)
        removed[self.keeper_spec.name] = True
        self.command("volume-cleanup-inspect", ["docker", "volume", "inspect", "--format", "{{json .}}", volume], execution.encoded(self.volume))
        self.command("volume-remove", ["docker", "volume", "rm", volume], volume.encode() + b"\n")
        self.command("volume-absence", ["docker", "volume", "ls", "--quiet", "--filter", "name=^" + volume + "$"], b"" if cleanup else volume.encode())
        self.terminal = {"protocol": execution.PROTOCOL, "mode": "physical", "role_policy": execution.role_policy_identity(),
            "intent_sha256": execution.sha256(self.files["intent.json"]), "steps": rows, "observations": observations,
            "epochs": [self.epoch], "cleanup": removed, "volume_cleanup": cleanup,
            "infrastructure": (["authored cleanup failure"] if not cleanup else []) +
                (["ValueError:Probe observation/provenance unavailable"] if unknown_last else []), "planned_requests": len(payloads),
            "observed_probe_records": len(payloads),
            "observed_requests": sum(row["authenticated"] and row.get("sent_complete") is True for row in observations),
            "unknown_request_outcomes": int(unknown_last), "create_intents": len(payloads) + 2, "unconfirmed_creates": 0,
            "created_helpers": len(payloads), "created_servers": 1, "created_keepers": 1,
            "start_response_records": [{"path": name, "sha256": execution.sha256(raw), "status": json.loads(raw)["status"]}
                for name, raw in sorted(self.files.items()) if name.endswith("-start-completion.json")],
            "evaluator_sources_after": execution.evaluator_sources()}
        self.put("terminal.json", self.terminal)
        self.rehash()

    def put(self, name, value):
        raw = value if type(value) is bytes else execution.encoded(value)
        assert name not in self.files
        self.files[name] = raw
        self.order.append(name)
        (self.root / name).write_bytes(raw)
        return {"path": name, "bytes": len(raw), "sha256": execution.sha256(raw)}

    def replace(self, name, value, *, rehash=True):
        raw = value if type(value) is bytes else execution.encoded(value)
        self.files[name] = raw
        (self.root / name).write_bytes(raw)
        if rehash:
            self.rehash()

    def read(self, name):
        return json.loads(self.files[name])

    def rebind_artifact(self, name, raw):
        """Update local fixture descriptors too; never changes physical authority."""
        self.replace(name, raw, rehash=False)
        def changed(value):
            if type(value) is dict:
                result = {key: changed(item) for key, item in value.items()}
                if result.get("path") == name:
                    result["sha256"] = execution.sha256(raw)
                    if "bytes" in result:
                        result["bytes"] = len(raw)
                    if "observed_bytes" in result:
                        result["observed_bytes"] = len(raw)
                return result
            if type(value) is list:
                return [changed(item) for item in value]
            return value
        for path, data in list(self.files.items()):
            if path.endswith(".json"):
                self.replace(path, changed(json.loads(data)), rehash=False)
        self.rehash()

    def rehash(self):
        inventory = {}
        checkpoints = []
        for name in self.order:
            inventory[name] = execution.sha256(self.files[name])
            checkpoints.append({"files": [list(pair) for pair in sorted(inventory.items())]})
        self.checkpoints = tuple(checkpoints)
        self.definition["files"] = dict(inventory)

    def command(self, label, argv, stdout):
        command = {"argv": ["docker", "--host", "unix://" + RUNTIME["endpoint"]["socket_path"], *argv[1:]],
                   "exit_code": 0, "timed_out": False, "capture_complete": True}
        for kind, raw in (("stdout", stdout), ("stderr", b"")):
            command[kind] = {**self.put(label + "-" + kind + ".bin", raw), "observed_bytes": len(raw), "truncated": False}
        self.put(label + ".json", command)
        return command

    def control(self, label, cid, operation, body, *, method="GET", code=200):
        path = "/containers/" + cid + "/" + operation
        self.put(label + "-request.bin", execution.engine._request(method, path))
        if type(body) is not bytes:
            body = execution.encoded(body)
        if self.chunked and body:
            raw = b"HTTP/1.1 " + str(code).encode() + b" OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n" + hex(len(body))[2:].encode() + b"\r\n" + body + b"\r\n0\r\n\r\n"
        else:
            raw = b"HTTP/1.1 " + str(code).encode() + b" OK\r\nContent-Length: " + str(len(body)).encode() + b"\r\nConnection: close\r\n\r\n" + body
        self.put(label + "-response.bin", raw)

    def spec(self, kind, name, step, epoch, *, probe_path=""):
        binding = self.registration.binding
        labels = tuple(sorted({"gossip.execution": "http-synthetic", "gossip.role": kind,
            "gossip.source": binding.source_sha256, "gossip.fixture": binding.fixture_sha256,
            "gossip.epoch": str(epoch), "gossip.step": step, "gossip.helper": binding.helper_sha256}.items()))
        argv = execution.PROBE_ARGV if kind == "probe" else self.recipe.server_argv if kind == "server" else (
            "python", "-I", "-c", "import time;time.sleep(" + str(self.policy.lifetime_seconds) + ")")
        binds = (("/probe", probe_path),) if kind == "probe" else (
            ("/workspace", self.stage["workspace"]), ("/inputs", self.stage["inputs"])) if kind == "server" else ()
        return execution.RoleSpec(kind, name, argv, labels, binds,
            "" if kind == "probe" else self.intent["volume"], self.server["Id"] if kind == "probe" else "")

    def create(self, label, spec, cid):
        self.command(label + "-before", ["docker", "container", "ls", "--all", "--quiet", "--filter", "name=^/" + spec.name + "$"], b"")
        argv = execution.create_argv(spec, IMAGE)
        self.put(label + "-create-intent.json", {"spec": asdict(spec), "create_argv": argv})
        self.command(label + "-create", argv, cid.encode() + b"\n")
        created = inspection(spec, cid)
        self.control(label + "-created", cid, "json", created)
        return created

    def compare(self, label, before, after, spec, phase, *, donor=None):
        value = execution.role_identity_comparison(before, after, spec, RUNTIME, phase, donor=donor)
        self.put(label + ".json", value)
        return value

    def create_start(self, label, spec, cid):
        created = self.create(label, spec, cid)
        self.control(label + "-prestart", cid, "json", created)
        self.compare(label + "-prestart-comparison", created, created, spec, "created-to-prestart")
        self.put(label + "-start-intent.json", {"container_id": cid, "spec": asdict(spec),
            "prestart_sha256": execution.digest(created), "controller_action": "start-long-lived-role"})
        self.control(label + "-start", cid, "start", b"", method="POST", code=204)
        self.put(label + "-start-completion.json", {"status": 204, "body_sha256": execution.sha256(b""),
            "framing_complete": True, "eof_observed": True, "container_id": cid})
        running = inspection(spec, cid, "running")
        self.control(label + "-running", cid, "json", running)
        self.compare(label + "-startup-comparison", created, running, spec, "created-to-running")
        self.put(label + "-running.json", running)
        return running

    def boundary(self, label, value, spec):
        self.control(label, value["Id"], "json", value)
        return self.compare(label + "-continuity", value, value, spec, "running-to-running")

    def stop(self, label, value, spec):
        self.boundary(label + "-before", value, spec)
        cid = value["Id"]
        self.put(label + "-intent.json", {"controller_action": "explicit-stop", "container_id": cid,
            "running_sha256": execution.digest(value), "timeout_seconds": self.policy.stop_timeout_seconds,
            "natural_exit_zero_required": False})
        self.control(label, cid, "stop?t=" + str(self.policy.stop_timeout_seconds), b"", method="POST", code=204)
        final = inspection(spec, cid, "exited")
        self.control(label + "-final", cid, "json", final)
        comparison = self.compare(label + "-comparison", value, final, spec, "running-to-exited")
        result = {"controller_action": "explicit-stop", "status": 204, "response_body_sha256": execution.sha256(b""),
            "container_id": cid, "exit_code": 0, "natural_exit_zero_required": False,
            "comparison": comparison, "final_inspection_sha256": execution.digest(final)}
        self.put(label + "-result.json", result)
        return result

    def remove(self, label, value, spec, *, force):
        cid = value["Id"]
        self.command(label + "-inspect", ["docker", "inspect", "--format", "{{json .}}", spec.name], execution.encoded(value))
        self.put(label + "-intent.json", {"container_id": cid, "spec": asdict(spec), "force": force,
            "inspection_sha256": execution.digest(value)})
        self.command(label + "-remove", ["docker", "rm", *(["--force"] if force else []), cid], cid.encode() + b"\n")
        self.command(label + "-absence", ["docker", "container", "ls", "--all", "--quiet", "--filter", "name=^/" + spec.name + "$"], b"")

    def probe(self, index, payload, *, unknown=False):
        label = "step-" + str(index).zfill(3)
        step = self.recipe.steps[index]
        self.boundary(label + "-keeper-before", self.keeper, self.keeper_spec)
        before = self.boundary(label + "-server-before", self.server, self.server_spec)
        donor = execution.bind_probe_donor(baseline=self.server, current=self.server, server_spec=self.server_spec,
            registration=self.registration, runtime=RUNTIME, epoch=1)
        self.put(label + "-donor-inspection.json", donor.inspection_json)
        self.put(label + "-donor.json", donor.record())
        probe_input = wire.build_probe_input(request(), 8765, self.policy.wire_limits)
        self.put(label + "-probe-input.json", probe_input)
        self.put(label + "-probe-source.json", {"helper_sha256": wire.helper_sha256(),
            "request_input_sha256": execution.sha256(probe_input),
            "request_sha256": execution.sha256(wire.request_bytes(request(), 8765)),
            "argv": execution.PROBE_ARGV, "server_epoch": 1, "server_id": self.server["Id"]})
        spec = self.spec("probe", self.intent["containers"][index], step.step_id, 1,
                         probe_path="/synthetic/stage/" + label)
        cid = format(index + 2, "x") * 64
        probe_label = label + "-probe"
        created = self.create(probe_label, spec, cid)
        evidence = {}
        limit = wire.max_probe_output_bytes(self.policy.wire_limits)
        def save(name, value):
            evidence[name] = self.put(probe_label + "-" + name, value)
            return evidence[name]
        save("intent.json", {"protocol": execution.PROTOCOL, "role": asdict(spec),
            "expected": execution.digest(created), "runtime": RUNTIME, "policy": asdict(self.policy),
            "role_policy": execution.role_policy_identity(), "donor": donor.record(),
            "helper_stdout_envelope": limit, "helper_sha256": wire.helper_sha256()})
        def control(name, operation, body, *, method="GET", code=200):
            self.control(probe_label + "-" + name, cid, operation, body, method=method, code=code)
            for suffix in ("request.bin", "response.bin"):
                path = probe_label + "-" + name + "-" + suffix
                raw = self.files[path]
                evidence[name + "-" + suffix] = {"path": path, "bytes": len(raw), "sha256": execution.sha256(raw)}
        control("prestart", "json", created)
        save("prestart-comparison.json", execution.role_identity_comparison(created, created, spec, RUNTIME, "created-to-prestart"))
        save("attach-request.bin", execution.engine._request("POST", "/containers/" + cid + "/attach?stream=1&stdout=1&stderr=1&stdin=0&logs=0", upgrade=True))
        control("start", "start", b"", method="POST", code=204)
        start = {"status": 204, "body_bytes": 0, "body_sha256": execution.sha256(b""),
            "framing_complete": True, "eof_observed": True, "request": evidence["start-request.bin"],
            "response": evidence["start-response.bin"]}
        save("start-completion.json", start)
        waited = {"StatusCode": 0, "Error": None}
        control("wait", "wait?condition=not-running", waited, method="POST")
        final = inspection(spec, cid, "exited")
        final["Config"]["Hostname"] = self.server["Config"]["Hostname"]
        control("final", "json", final)
        comparison = execution.role_identity_comparison(created, final, spec, RUNTIME, "created-to-exited", donor=donor)
        save("identity-comparison.json", comparison)
        frames = b"\x01\x00\x00\x00" + len(payload).to_bytes(4, "big") + payload
        save("attach-response.bin", b"HTTP/1.1 101 UPGRADED\r\nConnection: Upgrade\r\nUpgrade: tcp\r\nContent-Type: application/vnd.docker.raw-stream\r\n\r\n" + frames)
        result = {"protocol": execution.PROTOCOL, "role": "probe", "status": "completed", "error": None,
            "container_id": cid, "start_response": start, "role_policy": execution.role_policy_identity(),
            "donor": donor.record(), "completion": execution.engine.completion_evidence(waited, final,
                started=True, killed=False, identity_ok=True), "identity_comparison": comparison,
            "attach_eof_after_start_confirmation": True, "capture_complete": True, "helper_stdout_envelope": limit}
        for kind, raw in (("stdout", payload), ("stderr", b"")):
            result[kind] = {**save(kind + ".bin", raw), "observed_bytes": len(raw), "truncated": False, "complete": True}
        result["evidence"] = dict(evidence)
        save("process.json", result)
        if not unknown:
            after = self.boundary(label + "-server-after", self.server, self.server_spec)
            keeper_after = self.boundary(label + "-probe-keeper-after", self.keeper, self.keeper_spec)
        else:
            exited = inspection(self.server_spec, self.server["Id"], "exited")
            exited["State"]["ExitCode"] = 17
            self.control(label + "-server-after", self.server["Id"], "json", exited)
        observation = wire.decode_probe_output(payload, request(), 8765, self.policy.wire_limits)
        self.remove(label + "-probe-retire", final, spec, force=True)
        row = {"step_id": step.step_id, "step_index": index, "kind": "probe", "label": label,
            "epoch": 1, "server_id": self.server["Id"], "database_volume": self.intent["volume"],
            "database_path": self.recipe.database_path, "source_sha256": self.registration.binding.source_sha256,
            "fixture_sha256": self.registration.binding.fixture_sha256, "commit_oid": self.registration.commit_oid,
            "tree_oid": self.registration.tree_oid, "helper_sha256": wire.helper_sha256(),
            "binding": asdict(self.registration.binding), "keeper_id": self.keeper["Id"], "authenticated": not unknown,
            "continuity_before": before, "donor": donor.record(), "probe_id": cid, "process": result,
            "probe_removed": True}
        if not unknown:
            row.update(continuity_after=after, keeper_continuity=keeper_after,
                       exchange_complete=observation.exchange_complete, sent_complete=observation.sent_complete)
        else:
            row["observation_error"] = "ValueError:Role state is not running"
        self.put(label + "-observation.json", row)
        if not unknown:
            self.boundary(label + "-keeper-after", self.keeper, self.keeper_spec)
            self.put(label + "-result.json", row)
        return row, spec

    def admit(self):
        return observer.admit_fixture(self.root, self.definition, self.checkpoints)


class SyntheticManualControl:
    """Authored wrong-network rejection; zero helper starts or semantic rows."""

    put = SyntheticJournal.put
    read = SyntheticJournal.read
    replace = SyntheticJournal.replace
    rebind_artifact = SyntheticJournal.rebind_artifact
    command = SyntheticJournal.command

    def __init__(self, root):
        self.root = root
        self.root.mkdir()
        (root / "source").mkdir()
        (root / "inputs").mkdir()
        (root / "probe").mkdir()
        self.files, self.order = {}, []
        source = b"raise RuntimeError('manual authored fixture must not execute')\n"
        tree = b"100644 server.py\0" + bytes.fromhex(object_oid("blob", source))
        tree_oid = object_oid("tree", tree)
        commit = ("tree " + tree_oid + "\nauthor Fixture <fixture@example.invalid> 0 +0000\n"
                  "committer Fixture <fixture@example.invalid> 0 +0000\n\nAuthored manual fixture\n").encode()
        source_hash = execution.source_sha256({"server.py": source})
        labels = (("gossip.execution", "manual-fixture"), ("gossip.source", source_hash),
                  ("gossip.purpose", "harness_qualification"), ("gossip.control", "M08"))
        volume = "manual-fixture-volume"
        server = execution.RoleSpec("server", "manual-server", ("python", "/workspace/server.py", "--db",
            "/tmp/library.sqlite", "--root", "/inputs", "--port", "8765"), labels,
            (("/workspace", str(root / "source")), ("/inputs", str(root / "inputs"))), volume)
        keeper = execution.RoleSpec("keeper", "manual-keeper", ("python", "-c", "import time;time.sleep(120)"),
                                    labels, (), volume)
        probe = execution.RoleSpec("probe", "manual-probe", execution.PROBE_ARGV, labels,
            (("/probe", str(root / "probe")),), server_id="2" * 64)
        registration = {"control": {"control_id": "M08", "targets": []},
            "ordered_roster_sha256": execution.digest(["synthetic-M08"]),
            "source_commit": object_oid("commit", commit), "source_tree": tree_oid,
            "source_sha256": source_hash, "runtime": RUNTIME, "helper_sha256": wire.helper_sha256(),
            "policy": asdict(execution.HttpPolicy(IMAGE)), "keeper": asdict(keeper), "server": asdict(server),
            "volume": volume, "volume_options": execution.VOLUME_OPTIONS, "declared_requests": 0,
            "declared_helper_starts": 0, "purpose": "harness_qualification"}
        self.definition = {"kind": "manual_zero_request", "control_id": "synthetic-M08",
            "registration": registration, "source_files": {"server.py": b64(source)}, "fixture_files": {},
            "git_commit_payload_b64": b64(commit), "temporal_authority": EXTERNAL_EVENTS}
        self.put("prospective-registration.json", registration)
        self.put("source/server.py", source)
        self.put("probe/helper.py", wire.helper_source())
        self.put("probe/request.json", wire.build_probe_input(request(), 8765))
        correct = execution.create_argv(probe, IMAGE)
        actual = ["--network=none" if token == "--network=container:" + probe.server_id else token for token in correct]
        self.put("probe-prospective-role.json", {"expected_role": asdict(probe), "correct_argv": correct,
            "actual_create_argv": actual, "single_defect": "HostConfig.NetworkMode"})
        self.wrong = inspection(probe, "3" * 64)
        self.wrong["HostConfig"]["NetworkMode"] = "none"
        for label in ("probe-created", "probe-after-rejection"):
            self.command(label, ["docker", "inspect", "--format", "{{json .}}", "3" * 64], execution.encoded(self.wrong))
        for spec, cid in ((server, "2" * 64), (keeper, "1" * 64)):
            for suffix in ("running", "after-rejection"):
                self.command(spec.role + "-" + suffix, ["docker", "inspect", "--format", "{{json .}}", cid],
                             execution.encoded(inspection(spec, cid, "running")))
        cleanup = []
        for spec, cid in ((probe, "3" * 64), (server, "2" * 64), (keeper, "1" * 64)):
            value = self.wrong if spec.role == "probe" else inspection(spec, cid, "running")
            self.command(spec.role + "-cleanup-owned", ["docker", "inspect", "--format", "{{json .}}", cid],
                         execution.encoded(value))
            if spec.role != "probe":
                self.command(spec.role + "-stop", ["docker", "stop", "--time", "5", cid], cid.encode())
                self.command(spec.role + "-stopped", ["docker", "inspect", "--format", "{{json .}}", cid],
                             execution.encoded(inspection(spec, cid, "exited")))
            self.command(spec.role + "-remove", ["docker", "rm", cid], cid.encode())
            self.command(spec.role + "-absent", ["docker", "container", "ls", "--all", "--no-trunc",
                         "--filter", "id=" + cid, "--format", "{{.ID}}"], b"")
            cleanup.append({"role": spec.role, "container_id": cid, "absence_verified": True,
                            "stop_proven_or_never_started": True, "forced_remove_requested": False})
        self.command("volume-cleanup-inspect", ["docker", "volume", "inspect", "--format", "{{json .}}", volume],
            execution.encoded({"Name": volume, "Options": execution.VOLUME_OPTIONS,
                               "Labels": {"gossip.execution": "manual-fixture"}}))
        self.command("volume-remove", ["docker", "volume", "rm", volume], volume.encode())
        self.command("volume-absent", ["docker", "volume", "ls", "--filter", "name=^" + volume + "$",
                                      "--format", "{{.Name}}"], b"")
        self.put("cleanup.json", {"containers": cleanup, "volume_created": True, "volume_claimed": True,
            "volume_absence_verified": True, "errors": [], "acceptance_credit": False})
        self.put("physical-census.json", {"planned_declared_requests": 0, "observed_probe_records": 0,
            "authenticated_sent_complete_requests": 0, "helper_starts": 0, "cleanup_errors": [],
            "cleanup": cleanup, "volume_cleanup_verified": True})
        self.put("observed-rejection.json", {"actual_inspection_sha256": execution.digest(self.wrong),
            "declared_request_count": 0, "helper_start_count": 0})
        self.rehash()

    def rehash(self):
        SyntheticJournal.rehash(self)
        self.checkpoints = ()

    def admit(self):
        return observer.admit_fixture(self.root, self.definition, ())


class JournalTestFixture:
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="http-observation-offline-")
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name).resolve()

    def fixture(self, **kwargs):
        return SyntheticJournal(self.base / ("journal-" + str(len(list(self.base.iterdir())))), **kwargs)

    def manual(self):
        return SyntheticManualControl(self.base / ("manual-" + str(len(list(self.base.iterdir())))))


class HttpObservationAdmissionTests(JournalTestFixture, unittest.TestCase):
    def test_complete_generated_journal_has_no_physical_or_acceptance_authority(self):
        journal = self.fixture()
        history = observer.observe_history(journal.admit())
        self.assertFalse(history.physical_origin)
        self.assertFalse(history.acceptance_authority)
        self.assertEqual(history.comparison_authority, "not_registered")
        self.assertEqual(history.missing_step_ids, ())
        self.assertTrue(history.cleanup_verified)
        self.assertEqual(len(history.observations), 1)
        row = history.observations[0]
        self.assertEqual(row.step_id, "request-1")
        self.assertFalse(row.physical_origin)
        self.assertTrue(row.eligible)
        self.assertFalse(row.diagnostic)
        self.assertEqual(row.facts.status, 200)
        self.assertEqual(row.facts.body, HEALTH_BODY)
        self.assertIsInstance(row.provenance, tuple)
        with self.assertRaises(FrozenInstanceError):
            row.eligible = False

    def test_chunked_engine_control_replies_decode_without_engine(self):
        history = observer.observe_history(self.fixture(chunked=True).admit())
        self.assertEqual(history.observations[0].facts.body, HEALTH_BODY)

    def test_multiple_probes_share_one_epoch_but_keep_unique_observations(self):
        history = observer.observe_history(self.fixture(payloads=[transcript(), transcript()]).admit())
        self.assertEqual(tuple(row.step_id for row in history.observations), ("request-1", "request-2"))
        self.assertTrue(all(row.eligible for row in history.observations))

    def test_checksum_tamper_is_rejected_against_unchanged_fixture_checkpoint(self):
        journal = self.fixture()
        journal.replace("step-001-probe-stdout.bin", b"{}", rehash=False)
        with self.assertRaises(ValueError):
            journal.admit()

    def test_checkpoint_missing_duplicate_reorder_and_prior_rewrite_rejected(self):
        for mutation in ("missing", "duplicate", "reorder", "rewrite"):
            with self.subTest(mutation=mutation):
                journal = self.fixture()
                checkpoints = list(deepcopy(journal.checkpoints))
                if mutation == "missing":
                    checkpoints.pop(4)
                elif mutation == "duplicate":
                    checkpoints.insert(4, deepcopy(checkpoints[3]))
                elif mutation == "reorder":
                    checkpoints[4], checkpoints[5] = checkpoints[5], checkpoints[4]
                else:
                    checkpoints[4]["files"][0][1] = "0" * 64
                with self.assertRaises(ValueError):
                    observer.admit_fixture(journal.root, journal.definition, tuple(checkpoints))

    def test_foreign_suffix_and_symlink_are_rejected(self):
        for kind in ("foreign", "symlink", "directory"):
            with self.subTest(kind=kind):
                journal = self.fixture()
                if kind == "foreign":
                    (journal.root / "foreign.json").write_text("{}")
                elif kind == "symlink":
                    target = journal.root / "step-001-probe-stdout.bin"
                    target.unlink()
                    target.symlink_to(journal.root / "step-001-probe-stderr.bin")
                else:
                    target = journal.root / "step-001-probe-stdout.bin"
                    target.unlink()
                    target.mkdir()
                with self.assertRaises(ValueError):
                    observer.observe_history(journal.admit())

    def test_rehashed_wrong_source_commit_and_environment_cannot_admit(self):
        for field in ("source", "commit", "environment"):
            with self.subTest(field=field):
                journal = self.fixture()
                if field == "source":
                    journal.definition["source_files"]["server.py"] = b64(b"different")
                elif field == "commit":
                    journal.definition["git_commit_payload_b64"] = b64(b"tree " + b"0" * 40 + b"\n\nforeign\n")
                else:
                    journal.definition["environment_sha256"] = "0" * 64
                with self.assertRaises(ValueError):
                    observer.observe_history(journal.admit())

    def test_raw_request_owned_id_verb_and_path_are_reconstructed(self):
        for raw in (execution.engine._request("GET", "/containers/" + "9" * 64 + "/json"),
                    execution.engine._request("POST", "/containers/" + "3" * 64 + "/json"),
                    execution.engine._request("GET", "/containers/" + "3" * 64 + "/stats")):
            with self.subTest(raw=raw):
                journal = self.fixture()
                journal.rebind_artifact("step-001-probe-prestart-request.bin", raw)
                with self.assertRaises(ValueError):
                    observer.observe_history(journal.admit())

    def test_malformed_trailing_and_truncated_engine_control_proof_rejected(self):
        for mutation in ("trailing", "truncated", "bad_chunk", "duplicate_nonframing_header"):
            with self.subTest(mutation=mutation):
                journal = self.fixture(chunked=True)
                name = "step-000-running-response.bin"
                raw = journal.files[name]
                if mutation == "duplicate_nonframing_header":
                    changed = raw.replace(b"Connection: close\r\n", b"Connection: close\r\nX-Trace: one\r\nx-trace: two\r\n")
                else:
                    changed = raw + b"junk" if mutation == "trailing" else raw[:-3] if mutation == "truncated" else raw.replace(b"\r\n0\r\n\r\n", b"\r\nZ\r\n\r\n")
                journal.replace(name, changed)
                with self.assertRaises(ValueError):
                    observer.observe_history(journal.admit())

    def test_donor_checkpoint_after_helper_creation_rejected_despite_rehash(self):
        journal = self.fixture()
        name = "step-001-donor.json"
        journal.order.remove(name)
        journal.order.insert(journal.order.index("step-001-probe-create-intent.json") + 1, name)
        journal.rehash()
        with self.assertRaises(ValueError):
            observer.observe_history(journal.admit())

    def test_raw_multiplex_bytes_disagreeing_with_stdout_rejected(self):
        journal = self.fixture()
        name = "step-001-probe-attach-response.bin"
        raw = journal.files[name].replace(b'"elapsed_seconds":0.01', b'"elapsed_seconds":0.02')
        self.assertNotEqual(raw, journal.files[name])
        journal.rebind_artifact(name, raw)
        with self.assertRaises(ValueError):
            observer.observe_history(journal.admit())

    def test_duplicate_observation_census_rejected_even_when_terminal_rehashed(self):
        journal = self.fixture()
        terminal = journal.read("terminal.json")
        terminal["observations"].append(deepcopy(terminal["observations"][0]))
        terminal["observed_probe_records"] = 2
        journal.replace("terminal.json", terminal)
        with self.assertRaises(ValueError):
            observer.observe_history(journal.admit())

    def test_retained_success_survives_later_cleanup_failure(self):
        history = observer.observe_history(self.fixture(cleanup=False).admit())
        self.assertFalse(history.cleanup_verified)
        self.assertTrue(history.observations[0].eligible)
        self.assertEqual(history.observations[0].facts.body, HEALTH_BODY)

    def test_repeated_snapshot_reads_execute_no_runtime_or_source_commands(self):
        bound = self.fixture().admit()
        with ExitStack() as stack:
            for target in ("subprocess.Popen", "subprocess.run", "socket.socket"):
                stack.enter_context(patch(target, side_effect=AssertionError("offline reader attempted execution")))
            first = observer.observe_history(bound)
            second = observer.observe_history(bound)
        self.assertEqual(first, second)
        self.assertFalse(first.fresh_execution)
        self.assertFalse(first.acceptance_authority)

    def test_replaced_bound_objects_cannot_promote_or_change_admission(self):
        bound = self.fixture().admit()
        changes = ({"physical_origin": True}, {"files": ()}, {"definition_json": b"{}"},
            {"origin_manifest_sha256": "0" * 64}, {"origin_id": "caller-asserted-physical"},
            {"reanalysis_source_pins": (("observer.py", "0" * 64),)})
        for changed in changes:
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                observer.observe_history(replace(bound, **changed))

    def test_value_objects_and_authenticated_flags_cannot_admit_physical_origin(self):
        facts = semantics.ResponseFacts(200, (), b"{}")
        for fake in (facts, {"authenticated": True}, {"physical_origin": True}, object()):
            with self.subTest(fake=type(fake).__name__), self.assertRaises(ValueError):
                observer.observe_history(fake)

    def test_missing_original_terminal_is_unknown_and_never_redispatched(self):
        journal = self.fixture()
        journal.order.remove("terminal.json")
        del journal.files["terminal.json"]
        (journal.root / "terminal.json").unlink()
        journal.rehash()
        with patch("subprocess.Popen", side_effect=AssertionError("redispatch")):
            with self.assertRaises(observer.ObservationUnknown):
                observer.observe_history(journal.admit())

    def test_qualification_cannot_be_relabelled_as_acceptance(self):
        journal = self.fixture()
        journal.definition["registration"]["registration"]["binding"]["purpose"] = "independent_acceptance"
        with self.assertRaises(ValueError):
            observer.observe_history(journal.admit())

    def test_raw_donor_domain_and_probe_hostname_must_match_closed_transition(self):
        for name, key, value in (("step-001-server-before-response.bin", "Domainname", "foreign"),
                                 ("step-001-probe-final-response.bin", "Hostname", "spoofed")):
            with self.subTest(name=name):
                journal = self.fixture()
                _, body = journal.files[name].split(b"\r\n\r\n", 1)
                record = json.loads(body)
                record["Config"][key] = value
                body = execution.encoded(record)
                raw = b"HTTP/1.1 200 OK\r\nContent-Length: " + str(len(body)).encode() + b"\r\nConnection: close\r\n\r\n" + body
                journal.rebind_artifact(name, raw)
                with self.assertRaises(ValueError):
                    observer.observe_history(journal.admit())

    def test_rehashed_false_eof_event_cannot_be_inferred_from_file_end(self):
        journal = self.fixture()
        process_name = "step-001-probe-process.json"
        process = journal.read(process_name)
        process["attach_eof_after_start_confirmation"] = False
        journal.replace(process_name, process)
        for name in ("step-001-observation.json", "step-001-result.json"):
            row = journal.read(name)
            row["process"] = deepcopy(process)
            journal.replace(name, row)
        terminal = journal.read("terminal.json")
        terminal["steps"][1]["process"] = deepcopy(process)
        terminal["observations"][0]["process"] = deepcopy(process)
        journal.replace("terminal.json", terminal)
        with self.assertRaisesRegex(ValueError, "EOF|capture"):
            observer.observe_history(journal.admit())

    def test_history_cannot_override_fixed_authority_or_freshness_fields(self):
        history = observer.observe_history(self.fixture().admit())
        for changed in ({"acceptance_authority": True}, {"fresh_execution": True},
                        {"comparison_authority": "accepted"}, {"protocol": "caller-protocol"}):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                replace(history, **changed)

    def test_read_bounds_and_traversal_reject_before_consuming_unsafe_artifact(self):
        path = self.base / "small.bin"
        path.write_bytes(b"0123456789")
        self.assertEqual(observer._read(self.base, "small.bin", 10), b"0123456789")
        for name, limit in (("small.bin", 9), ("../small.bin", 10), (str(path), 10)):
            with self.subTest(name=name, limit=limit), self.assertRaises(ValueError):
                observer._read(self.base, name, limit)

    def test_fifo_artifact_is_rejected_without_a_blocking_open(self):
        path = self.base / "fifo.bin"
        os.mkfifo(path)
        real_open = os.open
        def guarded_open(name, flags, *args, **kwargs):
            if name == "fifo.bin":
                self.assertTrue(flags & os.O_NONBLOCK, "FIFO read must not block the offline verifier")
            return real_open(name, flags, *args, **kwargs)
        with patch.object(observer.os, "open", side_effect=guarded_open):
            with self.assertRaises(ValueError):
                observer._read(self.base, "fifo.bin")

    def test_earlier_eligible_row_survives_later_unattributed_server_exit(self):
        history = observer.observe_history(self.fixture(payloads=[transcript(), transcript()], unknown_last=True).admit())
        first, last = history.observations
        self.assertTrue(first.eligible)
        self.assertEqual(first.facts.body, HEALTH_BODY)
        self.assertFalse(last.eligible)
        self.assertTrue(last.diagnostic)
        self.assertIsNone(last.facts)
        self.assertEqual(history.missing_step_ids, ("request-2", "stop-1"))
        self.assertEqual(dict(history.census)["unknown_request_outcomes"], 1)
        self.assertEqual(dict(history.census)["authenticated_sent_complete"], 1)

    def test_server_exit_during_response_exposes_diagnostic_only(self):
        history = observer.observe_history(self.fixture(unknown_last=True).admit())
        self.assertEqual(len(history.observations), 1)
        self.assertIsNone(history.observations[0].facts)
        self.assertTrue(history.observations[0].diagnostic)
        self.assertEqual(dict(history.census)["complete_exchanges"], 0)

    def test_manual_wrong_network_rejection_produces_zero_semantic_rows(self):
        history = observer.observe_history(self.manual().admit())
        self.assertFalse(history.physical_origin)
        self.assertFalse(history.acceptance_authority)
        self.assertFalse(history.fresh_execution)
        self.assertEqual(history.observations, ())
        self.assertEqual(history.missing_step_ids, ())
        self.assertTrue(all(value == 0 for _, value in history.census))

    def test_manual_network_only_rejection_cannot_hide_another_role_defect(self):
        journal = self.manual()
        wrong = deepcopy(journal.wrong)
        wrong["Config"]["User"] = "0:0"
        for name in ("probe-created-stdout.bin", "probe-after-rejection-stdout.bin"):
            journal.rebind_artifact(name, execution.encoded(wrong))
        rejection = journal.read("observed-rejection.json")
        rejection["actual_inspection_sha256"] = execution.digest(wrong)
        journal.replace("observed-rejection.json", rejection)
        with self.assertRaises(ValueError):
            observer.observe_history(journal.admit())

    def test_manual_cleanup_claim_requires_raw_absence(self):
        journal = self.manual()
        journal.rebind_artifact("probe-absent-stdout.bin", b"3333333333333333333333333333333333333333333333333333333333333333\n")
        with self.assertRaisesRegex(ValueError, "still present"):
            observer.observe_history(journal.admit())

    def test_previously_loaded_sources_and_evaluator_code_must_still_match(self):
        journal = self.fixture()
        pins = tuple((name, "0" * 64 if name == "candidate_http_observation_v1.py" else fingerprint)
                     for name, fingerprint in observer.reanalysis_sources())
        for target, name, value in ((observer.head, "LOADED_SOURCE_SHA256", "0" * 64),
                                    (observer, "reanalysis_sources", lambda: pins),
                                    (execution.engine, "_request", lambda *args, **kwargs: b"changed")):
            with self.subTest(name=name), patch.object(target, name, value):
                with self.assertRaisesRegex(ValueError, "[Ll]oaded|[Pp]reviously imported"):
                    observer.observe_history(journal.admit())


class HttpObservationFacetTests(JournalTestFixture, unittest.TestCase):
    def test_complete_cl_and_chunked_without_socket_eof_have_body(self):
        responses = [HEALTH_RESPONSE, b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n" + hex(len(HEALTH_BODY))[2:].encode() + b"\r\n" + HEALTH_BODY + b"\r\n0\r\n\r\n"]
        for raw in responses:
            with self.subTest(raw=raw):
                history = observer.observe_history(self.fixture(payloads=[transcript(raw)]).admit())
                self.assertEqual(history.observations[0].facts.body, HEALTH_BODY)

    def test_wrong_status_is_retained_when_body_truncated(self):
        raw = b"HTTP/1.1 409 Conflict\r\nContent-Type: application/json\r\nContent-Length: 1000\r\n\r\n{}"
        history = observer.observe_history(self.fixture(payloads=[transcript(raw, termination="eof", eof=True)]).admit())
        facts = history.observations[0].facts
        self.assertEqual(facts.status, 409)
        self.assertIsInstance(facts.body, semantics.Missing)

    def test_final_status_survives_truncated_headers(self):
        raw = b"HTTP/1.1 409 Conflict\r\nContent-Type: applic"
        history = observer.observe_history(self.fixture(payloads=[transcript(raw, termination="eof", eof=True)]).admit())
        facts = history.observations[0].facts
        self.assertEqual(facts.status, 409)
        self.assertIsInstance(facts.headers, semantics.Missing)
        self.assertIsInstance(facts.body, semantics.Missing)

    def test_conflicting_framing_keeps_independent_headers(self):
        raw = b"HTTP/1.1 409 Conflict\r\nContent-Type: application/json\r\nContent-Length: 2\r\nContent-Length: 3\r\n\r\n{}"
        history = observer.observe_history(self.fixture(payloads=[transcript(raw, termination="parse_stopped")]).admit())
        facts = history.observations[0].facts
        self.assertEqual(facts.status, 409)
        self.assertIn(("Content-Type", "application/json"), facts.headers)
        self.assertIsInstance(facts.body, semantics.Missing)

    def test_informational_status_is_not_promoted_to_final(self):
        raw = b"HTTP/1.1 103 Early Hints\r\nLink: </x>\r\n\r\nHTTP/1.1 20"
        history = observer.observe_history(self.fixture(payloads=[transcript(raw, termination="eof", eof=True)]).admit())
        self.assertIsInstance(history.observations[0].facts.status, semantics.Missing)

    def test_partial_send_cannot_supply_application_facts(self):
        history = observer.observe_history(self.fixture(payloads=[transcript(b"", termination="send_error", sent=b"GET")]).admit())
        facts = history.observations[0].facts
        self.assertIsInstance(facts.status, semantics.Missing)
        self.assertIsInstance(facts.body, semantics.Missing)

    def test_listener_incomplete_coverage_preserves_wildcard_and_limitation(self):
        for wildcard in (False, True):
            history = observer.observe_history(self.fixture(payloads=[transcript(wildcard=wildcard, missing_tcp6=True)]).admit())
            listener = history.observations[0].facts.listener_before
            self.assertIsInstance(listener, semantics.ListenerFacts)
            self.assertIn("0.0.0.0" if wildcard else "127.0.0.1", listener.addresses)
            self.assertIsNotNone(listener.limitation)

    def test_failed_connect_wildcard_snapshot_remains_diagnostic(self):
        payload = transcript(b"", connected=False, sent=b"", termination="connect_error", wildcard=True)
        history = observer.observe_history(self.fixture(payloads=[payload]).admit())
        facts = history.observations[0].facts
        self.assertIsInstance(facts.status, semantics.Missing)
        self.assertIsInstance(facts.listener_before, semantics.Missing)
        self.assertIsInstance(facts.listener_after, semantics.Missing)
        self.assertFalse(history.observations[0].sent_complete)

    def test_timeout_and_response_cap_preserve_status_but_not_body(self):
        limits = wire.WireLimits(response_limit_bytes=4096)
        prefix = b"HTTP/1.1 409 Conflict\r\nContent-Type: application/json\r\n\r\n"
        for termination, raw in (("timeout", prefix + b"{}"),
                                 ("response_limit", prefix + b"x" * (4097 - len(prefix)))):
            with self.subTest(termination=termination):
                payload = transcript(raw, termination=termination, limits=limits)
                row = observer.observe_history(self.fixture(payloads=[payload], limits=limits).admit()).observations[0]
                self.assertEqual(row.facts.status, 409)
                self.assertIn(("Content-Type", "application/json"), row.facts.headers)
                self.assertIsInstance(row.facts.body, semantics.Missing)
                self.assertIn(termination, row.limitations)
