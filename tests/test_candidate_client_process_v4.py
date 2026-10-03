"""Offline bounded Engine protocol controls. No Docker socket or candidate runs."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict
import hashlib
from itertools import permutations
import json
from pathlib import Path
import socket
import time
import threading
import unittest
from unittest.mock import patch

from gossip_harness import candidate_client_process_v4 as process

IMAGE = "sha256:" + "a" * 64
ARGV = ["python", "-m", "library", "list"]
IDENTITY = "b" * 64
ENDPOINT = process.EngineEndpoint("/not-opened-offline.sock", 1, 2)


def runtime_fixture():
    return {"protocol": process.PROTOCOL, "endpoint": asdict(ENDPOINT), "api_version": "1.47",
        "daemon_id": "offline-daemon", "engine_version": "29.2.1", "os": "linux",
        "engine_git_commit": "6bc6209", "architecture": "arm64", "kernel_version": "offline-kernel",
        "image_id": IMAGE, "image_inspect_sha256": "e" * 64, "cgroup_version": "2",
        "cgroup_driver": "cgroupfs", "oom_kill_disable_supported": False}


def frame(kind, raw):
    return bytes([kind, 0, 0, 0]) + len(raw).to_bytes(4, "big") + raw


def fixture_inspection():
    environment = ["HOME=/tmp", "PYTHONDONTWRITEBYTECODE=1", "PYTHONNOUSERSITE=1"]
    environment.extend(key + "=" for key in ("HTTP_PROXY", "HTTPS_PROXY", "FTP_PROXY", "NO_PROXY", "ALL_PROXY",
        "http_proxy", "https_proxy", "ftp_proxy", "no_proxy", "all_proxy"))
    return {"Id": IDENTITY, "Name": "/gossip-client-offline", "Created": "2026-10-03T00:00:00.001Z",
        "Image": IMAGE, "Path": "python", "Args": ["-m", "library", "list"], "RestartCount": 0,
        "Config": {"Image": IMAGE, "User": "65534:65534", "WorkingDir": "/workspace", "Tty": False,
            "OpenStdin": False, "StdinOnce": False, "AttachStdout": True, "AttachStderr": True,
            "Entrypoint": ["python"], "Cmd": ["-m", "library", "list"], "Healthcheck": {"Test": ["NONE"]},
            "Labels": {"gossip.execution": "offline", "gossip.source": "c" * 64,
                       "gossip.fixture": "d" * 64, "gossip.step": "step-1"}, "Env": environment},
        "HostConfig": {"NetworkMode": "none", "ReadonlyRootfs": True, "Privileged": False,
            "OomKillDisable": False, "AutoRemove": False, "Memory": 256 * 1024 * 1024, "MemorySwap": 256 * 1024 * 1024,
            "NanoCpus": 1000000000, "PidsLimit": 64, "CapDrop": ["ALL"], "Init": False,
            "SecurityOpt": ["no-new-privileges:true"], "LogConfig": {"Type": "none", "Config": {}},
            "RestartPolicy": {"Name": "no", "MaximumRetryCount": 0},
            "Ulimits": [{"Name": "nofile", "Hard": 256, "Soft": 256}], "IpcMode": "private",
            "CgroupnsMode": "private"},
        "Mounts": [{"Destination": target, "Type": "bind", "Source": str(Path(source).resolve()),
                    "RW": False, "Propagation": "rprivate"}
                   for target, source in (("/workspace", "/offline-source"), ("/inputs", "/offline-inputs"))]
                  + [{"Destination": "/tmp", "Type": "volume", "Name": "gossip-volume-offline",
                      "Driver": "local", "RW": True}],
        "State": {"Status": "created", "Running": False, "Paused": False, "Restarting": False,
            "OOMKilled": False, "Dead": False, "Pid": 0, "ExitCode": 0, "Error": "",
            "StartedAt": "0001-01-01T00:00:00Z", "FinishedAt": "0001-01-01T00:00:00Z"}}


def finished(exit_code=0):
    result = fixture_inspection()
    result["State"].update(Status="exited", ExitCode=exit_code,
        StartedAt="2026-10-03T00:00:01.000000001Z", FinishedAt="2026-10-03T00:00:01.000000002Z")
    return result


class FakeSocket:
    def __init__(self, response, chunks=7, gate=None, eof_error=None):
        self.eof_error = eof_error
        self.gate = gate
        self.header_remaining = response.find(b"\r\n\r\n") + 4
        self.response = bytearray(response)
        self.chunk = chunks
        self.sent = []
        self.closed = False
    def settimeout(self, value):
        self.timeout = value
    def connect(self, path):
        self.path = path
    def sendall(self, raw):
        self.sent.append(raw)
    def recv(self, size, flags=0):
        if not self.response and self.eof_error is not None:
            raise self.eof_error
        if self.gate is not None and self.header_remaining == 0 and not self.gate.is_set():
            if self.timeout == 0:
                raise BlockingIOError()
            self.gate.wait(timeout=2)
        size = min(size, self.chunk)
        if self.gate is not None and self.header_remaining:
            size = min(size, self.header_remaining)
        result = bytes(self.response[:size])
        if not flags & socket.MSG_PEEK:
            del self.response[:size]
            self.header_remaining = max(0, self.header_remaining - len(result))
        return result
    def shutdown(self, how):
        pass
    def close(self):
        self.closed = True


class CandidateClientProcessV4ProtocolTests(unittest.TestCase):
    def test_multiplex_preserves_arbitrary_binary_and_empty_frames(self):
        decoder = process.MultiplexDecoder(process.ProcessPolicy(IMAGE))
        raw = frame(1, b"\xff\0JSON\r\n") + frame(2, b"warning\xfe") + frame(1, b"")
        for byte in raw:
            decoder.feed(bytes([byte]))
        decoder.eof()
        self.assertEqual(decoder.streams["stdout"], b"\xff\0JSON\r\n")
        self.assertEqual(decoder.streams["stderr"], b"warning\xfe")
        self.assertEqual(decoder.frames, 3)
        self.assertTrue(decoder.complete)
        with self.assertRaises(process.ProcessError):
            decoder.feed(b"more")

    def test_partial_header_and_partial_payload_never_complete(self):
        for raw in (b"\x01\0", frame(1, b"payload")[:-1]):
            decoder = process.MultiplexDecoder(process.ProcessPolicy(IMAGE))
            decoder.feed(raw)
            with self.assertRaises(process.ProcessError):
                decoder.eof()
            self.assertFalse(decoder.complete)

    def test_control_or_stdin_frames_cannot_be_candidate_stderr(self):
        for raw in (frame(0, b"x"), frame(3, b"engine fault"), b"\x01\x01\0\0" + b"\0" * 4):
            decoder = process.MultiplexDecoder(process.ProcessPolicy(IMAGE))
            with self.assertRaises(process.ProcessError):
                decoder.feed(raw)
            self.assertFalse(decoder.streams["stderr"])

    def test_stream_frame_and_frame_count_bounds(self):
        for policy, raw in ((process.ProcessPolicy(IMAGE, stream_limit_bytes=3), frame(1, b"abcd")),
                            (process.ProcessPolicy(IMAGE, frame_limit_bytes=3), frame(1, b"abcd"))):
            decoder = process.MultiplexDecoder(policy)
            with self.assertRaises(process.OutputLimit):
                decoder.feed(raw)
            self.assertLessEqual(len(decoder.streams["stdout"]), 3)
        decoder = process.MultiplexDecoder(process.ProcessPolicy(IMAGE))
        with patch.object(process, "FRAME_COUNT_LIMIT", 2), self.assertRaises(process.OutputLimit):
            decoder.feed(frame(1, b"") * 3)

    def read_control(self, response):
        fake = FakeSocket(response)
        with patch.object(process.EngineEndpoint, "validate"), patch.object(process.socket, "socket", return_value=fake):
            wire = process._Wire(ENDPOINT, time.monotonic() + 3, 3 * process.CONTROL_LIMIT)
            try:
                status, headers = wire.headers()
                return status, wire.body(status, headers)
            finally:
                wire.close()

    def test_http_content_length_chunked_and_close_framing(self):
        for response in (b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\n{}\n",
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\n{}\r\n1\r\n\n\r\n0\r\n\r\n",
            b"HTTP/1.0 200 OK\r\n\r\n{}\n"):
            self.assertEqual(self.read_control(response), (200, b"{}\n"))
        self.assertEqual(self.read_control(b"HTTP/1.1 204 No Content\r\n\r\n"), (204, b""))

    def test_http_rejects_truncation_ambiguity_trailing_and_header_injection(self):
        responses = [b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\n{}",
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}X",
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nContent-Length: 2\r\n\r\n{}",
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nTransfer-Encoding: chunked\r\n\r\n{}",
            b"HTTP/1.1 200 OK\r\n bad: header\r\n\r\n",
            b"HTTP/1.1 204 No Content\r\nContent-Length: 2\r\n\r\n{}",
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\n{}XX0\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\nX-Trailer: no\r\n\r\n"]
        for response in responses:
            with self.subTest(response=response), self.assertRaises(process.ProcessError):
                self.read_control(response)

    def test_http_body_and_wire_bounds_are_not_unbounded_allocations(self):
        for response in (b"HTTP/1.1 200 OK\r\nContent-Length: 1048577\r\n\r\n",
                         b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n100001\r\n"):
            with self.assertRaises(process.OutputLimit):
                self.read_control(response)
        fake = FakeSocket(b"abcdef", chunks=100)
        with patch.object(process.EngineEndpoint, "validate"), patch.object(process.socket, "socket", return_value=fake):
            wire = process._Wire(ENDPOINT, time.monotonic() + 3, 3)
            with self.assertRaises(process.OutputLimit):
                wire.receive()
            self.assertEqual(wire.raw, b"abc")
            wire.close()

    def test_http_header_byte_and_count_bounds(self):
        with self.assertRaises(process.ProcessError):
            self.read_control(b"HTTP/1.1 200 OK\r\nX:" + b"x" * process.HEADER_LIMIT + b"\r\n\r\n")
        with self.assertRaises(process.ProcessError):
            self.read_control(b"HTTP/1.1 200 OK\r\n" + b"".join(f"X-{i}: v\r\n".encode() for i in range(129)) + b"\r\n")

    def test_endpoint_rejects_remote_or_ambiguous_selection_without_fallback(self):
        for value in ("tcp://localhost:2375", "ssh://root@example", "http://localhost", "unix://relative", "unix:///tmp/x?socket=1"):
            with self.assertRaises(process.ProcessError):
                process.EngineEndpoint.from_url(value)
        with patch.object(process.DockerValidator, "_environment", return_value={"DOCKER_HOST": "tcp://example:2375"}), \
                patch.object(process.subprocess, "Popen") as invoked, self.assertRaises(process.ProcessError):
            process.EngineEndpoint.from_environment()
        invoked.assert_not_called()

    def test_policy_rejects_invalid_deadlines_limits_and_unpinned_image(self):
        for kwargs in ({"timeout_seconds": True}, {"timeout_seconds": float("inf")}, {"timeout_seconds": 0},
                       {"stream_limit_bytes": True}, {"frame_limit_bytes": 0}, {"transport_timeout_seconds": 121}):
            with self.assertRaises(process.ProcessError):
                process.ProcessPolicy(IMAGE, **kwargs)
        with self.assertRaises(process.ProcessError):
            process.ProcessPolicy("python:latest")
        self.assertEqual(asdict(process.ProcessPolicy(IMAGE))["image_id"], IMAGE)


class CandidateClientProcessV4LifecycleTests(unittest.TestCase):
    def test_sandbox_and_exact_configuration_projection(self):
        value = fixture_inspection()
        process.validate_sandbox(value, process.ProcessPolicy(IMAGE), expected_argv=ARGV, runtime=runtime_fixture())
        changed = deepcopy(value)
        changed["Config"]["Cmd"] += ["--other"]
        self.assertNotEqual(process.immutable_inspection(value), process.immutable_inspection(changed))
        for section, key, bad in (("Config", "User", "0:0"), ("Config", "Tty", True),
                ("Config", "OpenStdin", True), ("HostConfig", "Init", True),
                ("HostConfig", "NetworkMode", "host"), ("HostConfig", "AutoRemove", True),
                ("HostConfig", "CapAdd", ["SYS_ADMIN"]), ("HostConfig", "Memory", 0),
                ("HostConfig", "PidsLimit", -1), ("HostConfig", "IpcMode", "host"),
                ("HostConfig", "LogConfig", {"Type": "json-file", "Config": {}})):
            changed = deepcopy(value)
            changed[section][key] = bad
            with self.subTest(key=key), self.assertRaises(process.ProcessError):
                process.validate_sandbox(changed, process.ProcessPolicy(IMAGE), expected_argv=ARGV, runtime=runtime_fixture())
        changed = deepcopy(value)
        changed["Mounts"].append({"Destination": "/var/run/docker.sock", "Type": "bind", "RW": True})
        with self.assertRaises(process.ProcessError):
            process.validate_sandbox(changed, process.ProcessPolicy(IMAGE), expected_argv=ARGV, runtime=runtime_fixture())

    def test_initial_sandbox_rejects_boolean_numeric_and_nested_type_substitutions(self):
        substitutions = [("ReadonlyRootfs", 1), ("Privileged", 0), ("AutoRemove", 0),
            ("Init", 0), ("OomKillDisable", 0), ("Memory", 268435456.0),
            ("NanoCpus", 1000000000.0), ("PidsLimit", 64.0),
            ("RestartPolicy", {"Name": "no", "MaximumRetryCount": False}),
            ("Ulimits", [{"Name": "nofile", "Hard": 256.0, "Soft": 256}])]
        for key, bad in substitutions:
            value = fixture_inspection()
            value["HostConfig"][key] = bad
            with self.subTest(key=key), self.assertRaises(process.ProcessError):
                process.validate_sandbox(value, process.ProcessPolicy(IMAGE), expected_argv=ARGV, runtime=runtime_fixture())
        for key in ("ReadonlyRootfs", "Privileged", "AutoRemove", "Memory", "OomKillDisable"):
            value = fixture_inspection()
            del value["HostConfig"][key]
            with self.subTest(missing=key), self.assertRaises(process.ProcessError):
                process.validate_sandbox(value, process.ProcessPolicy(IMAGE), expected_argv=ARGV, runtime=runtime_fixture())

    def test_valid_exit_zero_and_two_independently_proven(self):
        for code in (0, 2):
            proof = process.completion_evidence({"StatusCode": code}, finished(code),
                                                started=True, killed=False, identity_ok=True)
            self.assertTrue(proof["natural"])
            self.assertEqual(proof["inspect_exit_code"], code)

    def test_disagreement_oom_kill_restart_and_missing_timestamps_unknown(self):
        alternatives = []
        for key, bad in (("ExitCode", 2), ("OOMKilled", True), ("Running", True), ("Error", "start failed"),
                ("StartedAt", "0001-01-01T00:00:00Z"), ("FinishedAt", "2026-10-03T00:00:00Z"),
                ("FinishedAt", "2026-13-99T00:00:00Z"), ("Dead", True), ("Pid", 42)):
            value = finished()
            value["State"][key] = bad
            alternatives.append(value)
        value = finished()
        value["RestartCount"] = 1
        alternatives.append(value)
        for value in alternatives:
            with self.subTest(value=value):
                self.assertFalse(process.completion_evidence({"StatusCode": 0}, value,
                    started=True, killed=False, identity_ok=True)["natural"])
        for options in ({"started": False}, {"killed": True}, {"identity_ok": False}):
            kwargs = {"started": True, "killed": False, "identity_ok": True, **options}
            self.assertFalse(process.completion_evidence({"StatusCode": 0}, finished(), **kwargs)["natural"])
        self.assertFalse(process.completion_evidence({"StatusCode": 137}, finished(137),
            started=True, killed=False, identity_ok=True)["natural"])

    def fake_run(self, raw=b"", *, exit_code=0, start=204, wait_error=None, final_edit=None, policy=None, premature_eof=False, eof_during_start=False, eof_during_retention=False, before_edit=None, reject_retention=None, raw_start_response=None, start_eof_error=None, start_exception=None, initial_edit=None, declared_argv=None, retain_mutation=None, reject_after_write=None):
        saved, events = {}, []
        real_thread, readers = threading.Thread, []
        real_control = process._control
        def capture_thread(*args, **kwargs):
            reader = real_thread(*args, **kwargs)
            readers.append(reader)
            return reader
        expected = fixture_inspection()
        final = finished(exit_code)
        if initial_edit:
            initial_edit(expected)
            initial_edit(final)
        if final_edit:
            final_edit(final)
        response = b"HTTP/1.1 101 UPGRADED\r\nConnection: Upgrade\r\nUpgrade: tcp\r\nContent-Type: application/vnd.docker.multiplexed-stream\r\n\r\n" + raw
        gate = threading.Event()
        fake = FakeSocket(response, gate=None if premature_eof else gate)
        sockets = [fake, FakeSocket(raw_start_response if raw_start_response is not None else b"HTTP/1.1 204 No Content\r\n\r\n", eof_error=start_eof_error)]
        def retain(name, value):
            if name == reject_retention:
                raise OSError("offline durable retention failed")
            self.assertNotIn(name, saved)
            saved[name] = value
            events.append(name)
            if retain_mutation:
                retain_mutation(name)
            if name == reject_after_write:
                raise OSError("offline checkpoint failed after write")
            if eof_during_retention and name == "p-start-response.bin":
                gate.set()
                for reader in readers:
                    reader.join(timeout=2)
                    self.assertFalse(reader.is_alive())
        def json_control(endpoint, path, **kwargs):
            events.append(kwargs["label"])
            if path.endswith("/wait?condition=not-running"):
                gate.set()
                if wait_error:
                    raise wait_error
                return {"StatusCode": exit_code}
            observed = deepcopy(expected if kwargs["label"] == "inspect-before" else final)
            if kwargs["label"] == "inspect-before" and before_edit:
                before_edit(observed)
            return observed
        def control(endpoint, method, path, **kwargs):
            events.append(kwargs["label"])
            if kwargs["label"] == "start":
                self.assertIn("p-attach-request.bin", saved)
                self.assertTrue(fake.sent)
                if eof_during_retention or raw_start_response is not None:
                    try:
                        return real_control(endpoint, method, path, **kwargs)
                    finally:
                        gate.set()
                if start_exception is not None:
                    kwargs["retain"]("start-request.bin", process._request(method, path))
                    gate.set()
                    raise start_exception
                if start != 204 or eof_during_start:
                    gate.set()
                if eof_during_start:
                    for reader in readers:
                        reader.join(timeout=2)
                        self.assertFalse(reader.is_alive())
                body = b"engine start failure" if start != 204 else b""
                kwargs["retain"]("start-request.bin", process._request(method, path))
                kwargs["on_response"](start)
                raw_response = (f"HTTP/1.1 {start} test\r\nContent-Length: {len(body)}\r\n\r\n".encode() + body)
                kwargs["retain"]("start-response.bin", raw_response)
                return start, body
            return 204, b""
        with patch.object(process, "runtime_identity", return_value=runtime_fixture()) as runtime, \
                patch.object(process.threading, "Thread", side_effect=capture_thread), \
                patch.object(process, "_json_control", side_effect=json_control), \
                patch.object(process, "_control", side_effect=control), \
                patch.object(process.EngineEndpoint, "validate"), \
                patch.object(process.socket, "socket", side_effect=lambda *_a, **_k: sockets.pop(0)):
            result = process.run_process(ENDPOINT, container_id=IDENTITY, expected=expected,
                policy=policy or process.ProcessPolicy(IMAGE, timeout_seconds=1), retain=retain,
                label="p", expected_runtime=runtime_fixture(), expected_argv=ARGV if declared_argv is None else declared_argv)
        if fake.sent:
            self.assertTrue(fake.closed)
        else:
            self.assertIn(fake, sockets)  # A rejected prestart identity never attaches.
        self.assertEqual(runtime.call_args.kwargs["timeout_seconds"],
                         (policy or process.ProcessPolicy(IMAGE)).transport_timeout_seconds)
        return result, saved, events

    def test_completed_binary_streams_use_attach_before_start_then_wait(self):
        result, saved, events = self.fake_run(frame(1, b"\0\xffout") + frame(2, b"err\xfe"), exit_code=2)
        self.assertEqual(result["status"], "completed")
        self.assertTrue(result["capture_complete"])
        self.assertEqual(result["exit_code"], 2)
        self.assertEqual(saved[result["stdout"]["path"]], b"\0\xffout")
        self.assertEqual(saved[result["stderr"]["path"]], b"err\xfe")
        self.assertLess(events.index("p-attach-request.bin"), events.index("start"))
        self.assertLess(events.index("start"), events.index("wait"))
        self.assertNotIn("kill", events)
        self.assertEqual(hashlib.sha256(saved[result["stdout"]["path"]]).hexdigest(), result["stdout"]["sha256"])

    def test_truncated_output_preserves_known_natural_exit(self):
        result, _, events = self.fake_run(frame(1, b"abc")[:-1], exit_code=2)
        self.assertEqual(result["status"], "transport_error")
        self.assertFalse(result["capture_complete"])
        self.assertTrue(result["completion"]["natural"])
        self.assertEqual(result["exit_code"], 2)
        self.assertFalse(result["stdout"]["complete"])
        self.assertNotIn("kill", events)

    def test_output_limit_is_unknown_output_not_product_latency_failure(self):
        result, _, _ = self.fake_run(frame(1, b"abcd"), policy=process.ProcessPolicy(IMAGE, stream_limit_bytes=3))
        self.assertEqual(result["status"], "output_limit")
        self.assertTrue(result["stdout"]["truncated"])
        self.assertEqual(result["stdout"]["bytes"], 3)
        self.assertFalse(result["capture_complete"])
        self.assertEqual(result["exit_code"], 0)

    def test_timeout_kills_owned_container_and_leaves_exit_unknown(self):
        result, _, events = self.fake_run(frame(1, b"{\"done\":true}"), wait_error=TimeoutError())
        self.assertEqual(result["status"], "timeout")
        self.assertIsNone(result["exit_code"])
        self.assertFalse(result["completion"]["natural"])
        self.assertIn("kill", events)
        self.assertIn("inspect-after-kill", events)

    def test_start_error_never_becomes_candidate_stderr_or_exit(self):
        result, saved, events = self.fake_run(start=500)
        self.assertEqual(result["status"], "start_error")
        self.assertIsNone(result["exit_code"])
        self.assertEqual(saved[result["stderr"]["path"]], b"")
        self.assertNotIn("wait", events)
        self.assertIn("kill", events)

    def test_final_identity_substitution_cannot_prove_completion(self):
        result, _, _ = self.fake_run(final_edit=lambda value: value.update(Id="e" * 64))
        self.assertEqual(result["status"], "completion_unproven")
        self.assertIsNone(result["exit_code"])
        self.assertFalse(result["completion"]["identity_verified"])

    def test_runtime_mismatch_prevents_start(self):
        with patch.object(process, "runtime_identity", return_value={"other": True}), \
                patch.object(process, "_control", return_value=(204, b"")) as control, \
                patch.object(process, "_json_control", return_value={}):
            result = process.run_process(ENDPOINT, container_id=IDENTITY, expected=fixture_inspection(),
                policy=process.ProcessPolicy(IMAGE), retain=lambda _name, _raw: None,
                label="p", expected_runtime=runtime_fixture(), expected_argv=ARGV)
        self.assertEqual(result["status"], "transport_error")
        self.assertTrue(all("/start" not in call.args[2] for call in control.call_args_list))

    def test_terminal_container_cannot_be_redispatched(self):
        with patch.object(process, "runtime_identity") as runtime, self.assertRaises(process.ProcessError):
            process.run_process(ENDPOINT, container_id=IDENTITY, expected=finished(),
                policy=process.ProcessPolicy(IMAGE), retain=lambda _name, _raw: None,
                label="p", expected_runtime={}, expected_argv=ARGV)
        runtime.assert_not_called()

    def test_closed_attach_before_start_is_not_empty_output_completion(self):
        result, _, events = self.fake_run(premature_eof=True)
        self.assertEqual(result["status"], "transport_error")
        self.assertNotIn("start", events)
        self.assertIsNone(result["exit_code"])
        self.assertFalse(result["capture_complete"])

    def test_eof_during_start_response_is_unknown_capture_but_exit_survives(self):
        result, saved, events = self.fake_run(frame(1, b"{}"), exit_code=2, eof_during_start=True)
        self.assertEqual(result["status"], "transport_error")
        self.assertFalse(result["capture_complete"])
        self.assertIs(result["attach_eof_after_start_confirmation"], False)
        self.assertEqual(result["exit_code"], 2)
        self.assertTrue(result["completion"]["natural"])
        self.assertEqual(saved[result["stdout"]["path"]], b"{}")
        self.assertNotIn("kill", events)

    def test_runtime_preflight_uses_declared_transport_budget(self):
        version = {"MinAPIVersion": "1.24", "ApiVersion": "1.47", "Os": "linux", "GitCommit": "6bc6209"}
        info = {"ID": "daemon", "OSType": "linux", "CgroupVersion": "2", "CgroupDriver": "cgroupfs", "OomKillDisable": False}
        image = {"Id": IMAGE, "Os": "linux"}
        with patch.object(process.time, "monotonic", return_value=100), \
                patch.object(process, "_json_control", side_effect=[version, info, image]) as requests:
            process.runtime_identity(ENDPOINT, IMAGE, timeout_seconds=7)
        self.assertEqual([call.kwargs["deadline"] for call in requests.call_args_list], [107, 107, 107])
        result, _, _ = self.fake_run(policy=process.ProcessPolicy(IMAGE, transport_timeout_seconds=7))
        self.assertEqual(result["status"], "completed")

    def test_confirmed_start_eof_during_slow_retention_is_complete(self):
        result, saved, _events = self.fake_run(frame(1, b"{}"), eof_during_retention=True)
        self.assertEqual(result["status"], "completed")
        self.assertTrue(result["capture_complete"])
        self.assertIs(result["attach_eof_after_start_confirmation"], True)
        self.assertEqual(result["exit_code"], 0)
        self.assertEqual(saved["p-start-response.bin"], b"HTTP/1.1 204 No Content\r\n\r\n")

    def test_response_callback_requires_complete_framing_before_retention(self):
        for raw, valid in ((b"HTTP/1.1 204 No Content\r\n\r\n", True),
                (b"HTTP/1.1 204 No Content\r\n\r\ntrailing", False)):
            events = []
            fake = FakeSocket(raw)
            with patch.object(process.EngineEndpoint, "validate"), \
                    patch.object(process.socket, "socket", return_value=fake):
                def call():
                    return process._control(ENDPOINT, "POST", "/containers/x/start", deadline=time.monotonic() + 3,
                        retain=lambda name, _raw: events.append(name), label="start",
                        on_response=lambda code: events.append(code))
                if valid:
                    self.assertEqual(call(), (204, b""))
                    self.assertLess(events.index(204), events.index("start-response.bin"))
                else:
                    with self.assertRaises(process.ProcessError):
                        call()
                    self.assertNotIn(204, events)
                self.assertEqual(events.count("start-response.bin"), 1)
                self.assertTrue(fake.closed)

    def test_mount_permutations_are_durably_compared_before_attach(self):
        def prestart(value):
            value["Mounts"] = value["Mounts"][1:] + value["Mounts"][:1]
        def final(value):
            value["Mounts"].reverse()
            value["HostConfig"]["OomKillDisable"] = None
        result, saved, events = self.fake_run(before_edit=prestart, final_edit=final)
        self.assertEqual(result["status"], "completed")
        self.assertTrue(result["prestart_comparison"]["matches"])
        self.assertTrue(result["startup_comparison"]["matches"])
        self.assertEqual(json.loads(saved["p-prestart-comparison.json"]), result["prestart_comparison"])
        self.assertLess(events.index("p-prestart-comparison.json"), events.index("p-attach-request.bin"))
        self.assertLess(events.index("p-prestart-comparison.json"), events.index("start"))
        for key in ("prestart_comparison", "startup_comparison"):
            proof = result[key]
            self.assertIsNotNone(proof["before_full_inspection_sha256"])
            self.assertIsNotNone(proof["after_full_inspection_sha256"])
            self.assertNotEqual(proof["before_sha256"], proof["after_sha256"])
        intent = json.loads(saved["p-intent.json"])
        self.assertEqual(intent["identity_policy"], process.identity_policy())
        self.assertEqual(intent["identity_policy_sha256"], process.identity_policy_sha256())
        self.assertEqual(intent["expected_full_inspection_sha256"], hashlib.sha256(
            process._encoded(fixture_inspection())).hexdigest())

    def test_actual_mount_change_before_start_stays_unavailable(self):
        result, saved, events = self.fake_run(before_edit=lambda value: value["Mounts"][0].update(RW=True))
        self.assertFalse(result["prestart_comparison"]["matches"])
        self.assertIn("p-prestart-comparison.json", saved)
        self.assertNotIn("p-attach-request.bin", saved)
        self.assertNotIn("start", events)
        self.assertIsNone(result["exit_code"])

    def test_failed_prestart_comparison_retention_prevents_attach(self):
        result, saved, events = self.fake_run(reject_retention="p-prestart-comparison.json")
        self.assertEqual(result["status"], "transport_error")
        self.assertNotIn("p-attach-request.bin", saved)
        self.assertNotIn("start", events)
        self.assertFalse(result["completion"]["natural"])

    def test_candidate_accepted_startup_transition_retains_exact_comparison(self):
        result, saved, _ = self.fake_run(final_edit=lambda value: value["HostConfig"].update(OomKillDisable=None))
        self.assertEqual(result["status"], "completed")
        comparison = result["startup_comparison"]
        self.assertTrue(comparison["matches"])
        self.assertEqual(len(comparison["transformations"]), 1)
        self.assertEqual(json.loads(saved["p-startup-comparison.json"]), comparison)
        self.assertEqual(result["exit_code"], 0)

    def test_candidate_prestart_transition_remains_raw_exact(self):
        result, _, events = self.fake_run(before_edit=lambda value: value["HostConfig"].update(OomKillDisable=None))
        self.assertEqual(result["status"], "transport_error")
        self.assertIsNone(result["startup_comparison"])
        self.assertNotIn("start", events)
        self.assertIsNone(result["exit_code"])

    def test_allowed_configuration_transition_does_not_override_oom(self):
        def changed(value):
            value["HostConfig"]["OomKillDisable"] = None
            value["State"]["OOMKilled"] = True
        result, _, events = self.fake_run(final_edit=changed)
        self.assertTrue(result["startup_comparison"]["matches"])
        self.assertEqual(result["status"], "completion_unproven")
        self.assertIsNone(result["exit_code"])
        self.assertIn("kill", events)


class CandidateClientProcessV4CompatibilityTests(unittest.TestCase):
    def pair(self):
        before = process.immutable_inspection(fixture_inspection())
        after = deepcopy(before)
        after["HostConfig"]["OomKillDisable"] = None
        return before, after

    def compare(self, before, after, runtime=None, phase="candidate-created-to-exited"):
        return process.startup_identity_comparison(before, after, runtime or runtime_fixture(), phase)

    def test_policy_is_fresh_and_bound_to_tracked_design(self):
        first = process.startup_policy()
        self.assertEqual(first["policy_id"], process.STARTUP_POLICY_ID)
        self.assertEqual(first["compatibility_plan_sha256"], hashlib.sha256(
            Path("analysis/candidate-b03-runtime-compatibility-plan-v1.json").read_bytes()).hexdigest())
        first["allowed_phases"].append("arbitrary")
        self.assertNotEqual(first, process.startup_policy())
        self.assertEqual(process.startup_policy_sha256(), hashlib.sha256(process._encoded(process.startup_policy())).hexdigest())

    def test_allowed_transition_both_phases_binds_raw_inputs_without_mutating(self):
        before, after = self.pair()
        raw_before, raw_after = process._encoded(before), process._encoded(after)
        for phase in ("keeper-created-to-running", "candidate-created-to-exited"):
            left, right = deepcopy(before), deepcopy(after)
            if phase.startswith("keeper-"):
                left["Mounts"] = [row for row in left["Mounts"] if row["Destination"] == "/tmp"]
                right["Mounts"] = deepcopy(left["Mounts"])
            value = self.compare(left, right, phase=phase)
            self.assertTrue(value["matches"])
            self.assertEqual(value["before_sha256"], hashlib.sha256(process._encoded(left)).hexdigest())
            self.assertEqual(value["after_sha256"], hashlib.sha256(process._encoded(right)).hexdigest())
            self.assertNotEqual(value["before_sha256"], value["after_sha256"])
            self.assertEqual(value["comparison_before_sha256"], value["comparison_after_sha256"])
            self.assertEqual(value["transformations"], [{"path": "HostConfig.OomKillDisable", "before": False,
                "after": None, "comparison_after": False, "phase": phase,
                "runtime_sha256": hashlib.sha256(process._encoded(runtime_fixture())).hexdigest()}])
            self.assertEqual(value["comparison_sha256"], hashlib.sha256(process._encoded(
                {k: v for k, v in value.items() if k != "comparison_sha256"})).hexdigest())
        self.assertEqual(process._encoded(before), raw_before)
        self.assertEqual(process._encoded(after), raw_after)

    def test_unchanged_pairs_have_no_transformation(self):
        before, _ = self.pair()
        for flag in (False, None):
            before["HostConfig"]["OomKillDisable"] = flag
            value = self.compare(before, deepcopy(before))
            self.assertTrue(value["matches"])
            self.assertFalse(value["transformations"])
            self.assertEqual(value["before_sha256"], value["after_sha256"])

    def test_enabled_missing_numeric_string_collection_and_reverse_rejected(self):
        for bad in (True, 0, 1, 0.0, "false", "null", [], {}):
            for side in (0, 1):
                values = list(self.pair())
                values[side]["HostConfig"]["OomKillDisable"] = bad
                self.assertFalse(self.compare(*values)["matches"], (side, bad))
                with self.assertRaises(process.ProcessError):
                    process.validate_oom_kill_default(values[side]["HostConfig"])
        for side in (0, 1):
            values = list(self.pair())
            del values[side]["HostConfig"]["OomKillDisable"]
            self.assertFalse(self.compare(*values)["matches"])
        before, after = self.pair()
        self.assertFalse(self.compare(after, before)["matches"])

    def test_unqualified_phases_and_runtime_profile_rejected(self):
        before, after = self.pair()
        for phase in ("", "keeper-running-to-running", "candidate-created-to-created", None, True):
            self.assertFalse(self.compare(before, after, phase=phase)["matches"])
        for key in runtime_fixture():
            runtime = runtime_fixture()
            del runtime[key]
            self.assertFalse(process.startup_identity_comparison(before, after, runtime,
                "candidate-created-to-exited")["matches"], key)
        changes = {"os": "windows", "engine_version": "29.3.0", "engine_git_commit": "6bc620",
            "api_version": "1.53", "cgroup_version": "1", "oom_kill_disable_supported": True,
            "image_id": "sha256:" + "f" * 64, "cgroup_driver": "", "endpoint": {"socket_path": "tcp://bad"}}
        for key, bad in changes.items():
            runtime = {**runtime_fixture(), key: bad}
            self.assertFalse(self.compare(before, after, runtime)["matches"], key)
        runtime = {**runtime_fixture(), "oom_kill_disable_supported": 0}
        self.assertFalse(self.compare(before, after, runtime)["matches"])
        runtime = {**runtime_fixture(), "engine_git_commit": "6bc6209b88a7a834c91f77d848e025c79e0227a1"}
        self.assertTrue(self.compare(before, after, runtime)["matches"])

    def test_every_other_hostconfig_field_and_unknown_field_change_rejected(self):
        before, template = self.pair()
        for key, value in before["HostConfig"].items():
            if key == "OomKillDisable":
                continue
            after = deepcopy(template)
            after["HostConfig"][key] = {"changed": value}
            self.assertFalse(self.compare(before, after)["matches"], key)
        after = deepcopy(template)
        after["HostConfig"]["FutureUnlistedCapability"] = False
        self.assertFalse(self.compare(before, after)["matches"])
        before["HostConfig"]["FutureUnlistedCapability"] = False
        self.assertTrue(self.compare(before, after)["matches"])
        after["HostConfig"]["FutureUnlistedCapability"] = 0
        self.assertFalse(self.compare(before, after)["matches"])
        after = deepcopy(template)
        del after["HostConfig"]["Ulimits"]
        self.assertFalse(self.compare(before, after)["matches"])

    def test_other_projection_fields_and_nested_type_changes_rejected(self):
        before, template = self.pair()
        for key, value in before.items():
            if key == "HostConfig":
                continue
            after = deepcopy(template)
            after[key] = {"changed": value}
            self.assertFalse(self.compare(before, after)["matches"], key)
        after = deepcopy(template)
        after["HostConfig"]["Privileged"] = 0
        self.assertFalse(self.compare(before, after)["matches"])
        after = deepcopy(template)
        after["Config"]["Labels"]["gossip.source"] = "f" * 64
        self.assertFalse(self.compare(before, after)["matches"])
        after = deepcopy(template)
        after["Mounts"][0]["RW"] = True
        self.assertFalse(self.compare(before, after)["matches"])
        after = deepcopy(template)
        after["extra"] = []
        self.assertFalse(self.compare(before, after)["matches"])

    def test_malformed_nonjson_or_incomplete_projection_is_unknown(self):
        before, after = self.pair()
        for malformed in (None, [], {"HostConfig": {"OomKillDisable": False}},
                          {**before, "Args": ()}, {**before, "Config": False},
                          {**before, "HostConfig": []}, {**before, 1: "numeric-key"},
                          {**before, "extra": float("nan")}, {**before, "extra": b"bytes"}):
            self.assertFalse(self.compare(malformed, after)["matches"])
        nested = []
        for _ in range(34):
            nested = [nested]
        self.assertFalse(self.compare({**before, "extra": nested}, after)["matches"])

    def test_runtime_capabilities_are_required_and_stably_projected(self):
        version = {"MinAPIVersion": "1.44", "ApiVersion": "1.53", "Os": "linux", "GitCommit": "6bc6209",
                   "Version": "29.2.1", "Arch": "arm64", "KernelVersion": "kernel"}
        info = {"ID": "daemon", "OSType": "linux", "CgroupVersion": "2", "CgroupDriver": "cgroupfs", "OomKillDisable": False}
        image = {"Id": IMAGE, "Os": "linux"}
        with patch.object(process, "_json_control", side_effect=[version, info, image]):
            first = process.runtime_identity(ENDPOINT, IMAGE)
        self.assertEqual(first["engine_git_commit"], "6bc6209")
        self.assertIs(first["oom_kill_disable_supported"], False)
        with patch.object(process, "_json_control", side_effect=[version, {**info, "Containers": 42}, image]):
            self.assertEqual(process.runtime_identity(ENDPOINT, IMAGE), first)
        for key, bad in (("OomKillDisable", 0), ("CgroupVersion", 2), ("CgroupDriver", None)):
            changed = {**info, key: bad}
            with patch.object(process, "_json_control", side_effect=[version, changed, image]), self.assertRaises(process.ProcessError):
                process.runtime_identity(ENDPOINT, IMAGE)
        changed = dict(info)
        del changed["OomKillDisable"]
        with patch.object(process, "_json_control", side_effect=[version, changed, image]), self.assertRaises(process.ProcessError):
            process.runtime_identity(ENDPOINT, IMAGE)


# Diagnostic representations from failed v2 controls; not candidate success receipts.
RETAINED_DIAGNOSTIC_MOUNTS = [{'name': 'persistence', 'original_before_projection_sha256': '87c1992ed28719b32e6d5a788dd672ac89ca3274f99d4d848ce070355040186b', 'original_prestart_projection_sha256': '91aea0dbe7d0095c44112c5570ea2c7a350b1a1c0416a0ade0008caba97e2be4', 'before_order': ['/tmp', '/workspace', '/inputs'], 'after_order': ['/workspace', '/inputs', '/tmp'], 'rows': {'/inputs': {'Destination': '/inputs', 'Mode': '', 'Propagation': 'rprivate', 'RW': False, 'Source': '/private/var/folders/7p/smv_b8qx5f3c_x31g78958nw0000gn/T/gossip-client-stage-adn9d54w/inputs', 'Type': 'bind'}, '/tmp': {'Destination': '/tmp', 'Driver': 'local', 'Mode': 'z', 'Name': 'gossip-client-886202ccd20d4579a2e6ddae40185bee-volume', 'Propagation': '', 'RW': True, 'Source': '/var/lib/docker/volumes/gossip-client-886202ccd20d4579a2e6ddae40185bee-volume/_data', 'Type': 'volume'}, '/workspace': {'Destination': '/workspace', 'Mode': '', 'Propagation': 'rprivate', 'RW': False, 'Source': '/private/var/folders/7p/smv_b8qx5f3c_x31g78958nw0000gn/T/gossip-client-stage-adn9d54w/workspace', 'Type': 'bind'}}, 'inventory_sha256': '7c4b91df058359d1241b21ffafbbdc3c66d95623ddcb68534ace07b957216519', 'raw_evidence': [{'bytes': 6001, 'path': 'runs/candidate-b03-v2-controls-qualification-1/20261003T160224-d190033f/classes/2afb7b40d9bf8e79/artifacts/gossip-candidate-b03-v2-cli-persistence-nu0hjvc8/cli-db-root-isolation/execution/step-000-created-stdout.bin', 'sha256': 'ed55887ee7ba6de2d582736cad4cec06389dbaaf30e89add2c134f4b470f9318'}, {'bytes': 5352, 'path': 'runs/candidate-b03-v2-controls-qualification-1/20261003T160224-d190033f/classes/2afb7b40d9bf8e79/artifacts/gossip-candidate-b03-v2-cli-persistence-nu0hjvc8/cli-db-root-isolation/execution/step-000-inspect-before-response.bin', 'sha256': '695c677001acbd441a6df6ea5969320c44f7cd05c1776d092e9b76eb42a88e26'}]}, {'name': 'domain-error', 'original_before_projection_sha256': '73b2ab4b49aa293b617adba949ed5480fe278b655cdba767d1b5baf7ca9f43b9', 'original_prestart_projection_sha256': 'd15bd8d51d6d9ce898d59961cf078362bb66037b7eaa0d9a0757f53734246670', 'before_order': ['/inputs', '/tmp', '/workspace'], 'after_order': ['/workspace', '/inputs', '/tmp'], 'rows': {'/inputs': {'Destination': '/inputs', 'Mode': '', 'Propagation': 'rprivate', 'RW': False, 'Source': '/private/var/folders/7p/smv_b8qx5f3c_x31g78958nw0000gn/T/gossip-client-stage-3uw_4tk9/inputs', 'Type': 'bind'}, '/tmp': {'Destination': '/tmp', 'Driver': 'local', 'Mode': 'z', 'Name': 'gossip-client-2ce90f2babdc426daa977c303ef17eb5-volume', 'Propagation': '', 'RW': True, 'Source': '/var/lib/docker/volumes/gossip-client-2ce90f2babdc426daa977c303ef17eb5-volume/_data', 'Type': 'volume'}, '/workspace': {'Destination': '/workspace', 'Mode': '', 'Propagation': 'rprivate', 'RW': False, 'Source': '/private/var/folders/7p/smv_b8qx5f3c_x31g78958nw0000gn/T/gossip-client-stage-3uw_4tk9/workspace', 'Type': 'bind'}}, 'inventory_sha256': 'af0904746a55c5f390d57df095cd4d331b31a104f5a3ccd168977b549f60ff1c', 'raw_evidence': [{'bytes': 5961, 'path': 'runs/candidate-b03-v2-controls-qualification-1/20261003T160224-d190033f/classes/043a1624de399d6a/artifacts/gossip-candidate-b03-v2-process-controls-wk9zs7br/domain-error/execution/step-000-created-stdout.bin', 'sha256': '7f469193511b13cd1194276684585160c6c686af12eb573b1a84963015ff1ee9'}, {'bytes': 5312, 'path': 'runs/candidate-b03-v2-controls-qualification-1/20261003T160224-d190033f/classes/043a1624de399d6a/artifacts/gossip-candidate-b03-v2-process-controls-wk9zs7br/domain-error/execution/step-000-inspect-before-response.bin', 'sha256': '343d7fbd8137000cd3f3124a7333a992295caad9919c2fa741335e8925f65513'}]}]


class CandidateClientProcessV4MountInventoryTests(unittest.TestCase):
    def compare(self, before, after, phase="candidate-created-to-prestart", **kwargs):
        return process.identity_comparison(before, after, runtime_fixture(), phase, **kwargs)

    def test_policy_pins_full_rules_and_both_plans(self):
        value = process.identity_policy()
        self.assertEqual(value["policy_id"], process.IDENTITY_POLICY_ID)
        self.assertEqual(value["mount_plan_sha256"], hashlib.sha256(Path(
            "analysis/candidate-b03-mount-inventory-plan-v1.json").read_bytes()).hexdigest())
        self.assertEqual(value["startup_policy_sha256"], process.startup_policy_sha256())
        self.assertEqual(process.identity_policy_sha256(), hashlib.sha256(process._encoded(value)).hexdigest())
        value["candidate_destinations"].append("/bad")
        self.assertNotEqual(value, process.identity_policy())

    def test_every_mount_permutation_uses_complete_destination_inventory(self):
        base = process.immutable_inspection(fixture_inspection())
        for first in permutations(base["Mounts"]):
            for second in permutations(base["Mounts"]):
                before, after = deepcopy(base), deepcopy(base)
                before["Mounts"], after["Mounts"] = list(first), list(second)
                snapshot = process._encoded([before, after])
                for phase in ("candidate-created-to-prestart", "candidate-created-to-exited"):
                    proof = self.compare(before, after, phase)
                    self.assertTrue(proof["matches"], proof)
                    self.assertEqual(proof["mounts_before"]["inventory_sha256"], proof["mounts_after"]["inventory_sha256"])
                    inventory = {row["Destination"]: row for row in first}
                    self.assertEqual(proof["mounts_before"]["inventory_sha256"], hashlib.sha256(process._encoded(inventory)).hexdigest())
                    self.assertEqual(proof["comparison_before_sha256"], hashlib.sha256(process._encoded({**before, "Mounts": inventory})).hexdigest())
                    self.assertEqual(len(proof["transformations"]), int(first != second))
                self.assertEqual(process._encoded([before, after]), snapshot)

    def test_duplicate_destination_even_identical_rows_is_rejected(self):
        before = process.immutable_inspection(fixture_inspection())
        for rows in (before["Mounts"] + [deepcopy(before["Mounts"][0])],
                     [before["Mounts"][0], deepcopy(before["Mounts"][0]), before["Mounts"][2]]):
            malformed = {**before, "Mounts": rows}
            self.assertFalse(self.compare(malformed, malformed)["matches"])
            self.assertFalse(self.compare(before, malformed)["matches"])

    def test_noncanonical_or_malformed_destinations_and_mount_arrays_rejected(self):
        base = process.immutable_inspection(fixture_inspection())
        for destination in (None, False, 0, "", "workspace", "/workspace/", "//workspace", "/./workspace", "/x/../workspace", "/work\x00space"):
            malformed = deepcopy(base)
            malformed["Mounts"][0]["Destination"] = destination
            self.assertFalse(self.compare(malformed, malformed)["matches"], repr(destination))
        for rows in (None, {}, (), [], [None], [0], [{"Source": "/x"}]):
            malformed = {**base, "Mounts": rows}
            self.assertFalse(self.compare(malformed, malformed)["matches"], rows)
        for rows in (base["Mounts"][:2], base["Mounts"] + [{"Destination": "/extra"}]):
            self.assertFalse(self.compare(base, {**base, "Mounts": rows})["matches"])

    def test_every_mount_field_and_unknown_nested_field_remains_exact(self):
        before = process.immutable_inspection(fixture_inspection())
        for index, row in enumerate(before["Mounts"]):
            for key in row:
                for operation in ("change", "remove"):
                    after = deepcopy(before)
                    if operation == "change":
                        after["Mounts"][index][key] = {"changed": row[key]}
                    else:
                        del after["Mounts"][index][key]
                    self.assertFalse(self.compare(before, after)["matches"], (index, key, operation))
        after = deepcopy(before)
        after["Mounts"][0]["RW"] = 0
        self.assertFalse(self.compare(before, after)["matches"])
        after = deepcopy(before)
        after["Mounts"][0]["FutureOptions"] = {"enabled": False, "sequence": [1, 2]}
        self.assertFalse(self.compare(before, after)["matches"])
        before = deepcopy(after)
        self.assertTrue(self.compare(before, after)["matches"])
        after["Mounts"][0]["FutureOptions"]["enabled"] = 0
        self.assertFalse(self.compare(before, after)["matches"])
        after = deepcopy(before)
        after["Mounts"][0]["FutureOptions"]["sequence"].reverse()
        self.assertFalse(self.compare(before, after)["matches"])

    def test_other_arrays_preserve_order_without_recursive_sorting(self):
        base = process.immutable_inspection(fixture_inspection())
        for path in (("Args",), ("Config", "Cmd"), ("Config", "Env"), ("HostConfig", "Mounts"),
                     ("HostConfig", "SecurityOpt"), ("HostConfig", "Ulimits")):
            before = deepcopy(base)
            target = before
            for key in path[:-1]:
                target = target[key]
            target[path[-1]] = ["first", "second"]
            after = deepcopy(before)
            target = after
            for key in path[:-1]:
                target = target[key]
            target[path[-1]].reverse()
            self.assertFalse(self.compare(before, after)["matches"], path)

    def test_only_startup_phases_allow_oom_transition(self):
        base = process.immutable_inspection(fixture_inspection())
        for phase in ("candidate-created-to-prestart", "candidate-created-to-exited", "keeper-created-to-running", "keeper-running-to-running"):
            before = deepcopy(base)
            if phase.startswith("keeper-"):
                before["Mounts"] = [before["Mounts"][-1]]
            after = deepcopy(before)
            after["HostConfig"]["OomKillDisable"] = None
            proof = self.compare(before, after, phase)
            self.assertEqual(proof["matches"], phase in ("candidate-created-to-exited", "keeper-created-to-running"))
        self.assertFalse(self.compare(base, base, "keeper-created-to-running")["matches"])
        self.assertFalse(self.compare(base, base, "arbitrary")["matches"])

    def test_full_raw_view_and_projection_hashes_are_distinct_and_bound(self):
        before, after = fixture_inspection(), finished()
        after["Mounts"].reverse()
        proof = self.compare(process.immutable_inspection(before), process.immutable_inspection(after),
            "candidate-created-to-exited", before_full_inspection=before, after_full_inspection=after)
        self.assertTrue(proof["matches"])
        self.assertEqual(proof["before_full_inspection_sha256"], hashlib.sha256(process._encoded(before)).hexdigest())
        self.assertNotEqual(proof["before_full_inspection_sha256"], proof["before_sha256"])
        substituted = deepcopy(before)
        substituted["Config"]["User"] = "0"
        bad = self.compare(process.immutable_inspection(before), process.immutable_inspection(after),
            "candidate-created-to-exited", before_full_inspection=substituted, after_full_inspection=after)
        self.assertFalse(bad["matches"])
        self.assertEqual(proof["comparison_sha256"], hashlib.sha256(process._encoded(
            {k: v for k, v in proof.items() if k != "comparison_sha256"})).hexdigest())

    def test_portable_retained_failure_mount_fixtures_are_diagnostic_only(self):
        for fixture in RETAINED_DIAGNOSTIC_MOUNTS:
            before = process.immutable_inspection(fixture_inspection())
            after = deepcopy(before)
            before["Mounts"] = [fixture["rows"][d] for d in fixture["before_order"]]
            after["Mounts"] = [fixture["rows"][d] for d in fixture["after_order"]]
            self.assertNotEqual(process._encoded(before), process._encoded(after))
            proof = self.compare(before, after)
            self.assertTrue(proof["matches"])
            self.assertEqual(proof["mounts_before"]["inventory_sha256"], fixture["inventory_sha256"])
            self.assertEqual(proof["mounts_after"]["inventory_sha256"], fixture["inventory_sha256"])
            self.assertEqual(fixture["name"] in ("persistence", "domain-error"), True)
            # No execution/completion record is synthesized or asserted from this identity-only comparison.

    def test_strict_raw_decoder_rejects_duplicate_keys_at_every_depth(self):
        for raw in (b'{"Id":"a","Id":"b"}', b'{"Mounts":[{"Destination":"/tmp","Destination":"/workspace"}]}',
                    b'{"Mounts":[{"Destination":"/tmp","Future":{"flag":false,"flag":true}}]}',
                    rb'{"x":1,"\u0078":2}'):
            with self.subTest(raw=raw), self.assertRaises(process.ProcessError):
                process.strict_json_loads(raw)
        valid = {"Mounts": [{"Destination": "/tmp", "RW": False}], "label": "caf\u00e9"}
        self.assertEqual(process.strict_json_loads(json.dumps(valid).encode("utf-8")), valid)
        self.assertEqual(process.strict_json_loads(json.dumps(valid)), valid)

    def test_raw_decoder_bounds_types_utf8_and_nonfinite_numbers(self):
        for raw in (b'"\xff"', b'{"x":NaN}', b'{"x":Infinity}', b'{"x":1e999}', b'{} trailing',
                    b'[' * 34 + b'0' + b']' * 34, b'"' + b'x' * process.CONTROL_LIMIT + b'"', {}, None):
            with self.subTest(raw_type=type(raw)), self.assertRaises(process.ProcessError):
                process.strict_json_loads(raw)

    def test_engine_raw_decoder_rejects_duplicate_keys_before_object_conversion(self):
        for raw in (b'{"Id":"a","Id":"b"}', b'{"Mounts":[{"Destination":"/tmp","Destination":"/tmp"}]}'):
            with patch.object(process, "_control", return_value=(200, raw)), self.assertRaises(process.ProcessError):
                process._json_control(ENDPOINT, "/containers/id/json", deadline=time.monotonic() + 1,
                    retain=lambda _name, _raw: None, label="inspect")


class CandidateClientProcessV4CommandTests(unittest.TestCase):
    def validate(self, value, argv=None, runtime=None):
        process.validate_sandbox(value, process.ProcessPolicy(IMAGE),
            expected_argv=ARGV if argv is None else argv,
            runtime=runtime_fixture() if runtime is None else runtime)

    def singleton(self, cmd=None):
        value = fixture_inspection()
        value.update(Path="/workspace/absent-control-executable", Args=[])
        value["Config"].update(Entrypoint=[value["Path"]], Cmd=cmd)
        return value

    def test_explicit_null_and_empty_cmd_admit_exact_singleton_only(self):
        for cmd in (None, []):
            value = self.singleton(cmd)
            self.validate(value, [value["Path"]])
            self.assertIs(value["Config"]["Cmd"], cmd)
        self.validate(fixture_inspection())
        value = self.singleton([""])
        value["Args"] = [""]
        self.validate(value, [value["Path"], ""])

    def test_missing_command_or_top_level_args_never_means_null(self):
        for key, parent in (("Cmd", "Config"), ("Entrypoint", "Config"), ("Args", None), ("Path", None)):
            value = self.singleton()
            del (value[parent] if parent else value)[key]
            with self.subTest(key=key), self.assertRaises(process.ProcessError):
                self.validate(value, ["/workspace/absent-control-executable"])

    def test_null_command_rejects_unregistered_or_malformed_args(self):
        for args in (None, "", {}, False, 0, [""], ["extra"], [False], [0]):
            value = self.singleton()
            value["Args"] = args
            with self.subTest(args=args), self.assertRaises(process.ProcessError):
                self.validate(value, [value["Path"]])
        for cmd in (False, 0, 1, "", {}, [False], [0], [None], ["\x00"]):
            value = self.singleton(cmd)
            with self.subTest(cmd=cmd), self.assertRaises(process.ProcessError):
                self.validate(value, [value["Path"]])
        value = self.singleton()
        with self.assertRaises(process.ProcessError):
            self.validate(value, [value["Path"], ""])

    def test_entrypoint_and_path_are_exact_registered_singleton(self):
        for entrypoint in (None, [], [""], [False], [0], ["other"], ["python", "extra"], "python"):
            value = self.singleton()
            value["Config"]["Entrypoint"] = entrypoint
            with self.subTest(entrypoint=entrypoint), self.assertRaises(process.ProcessError):
                self.validate(value, [value["Path"]])
        for path in (None, False, 0, "", "other"):
            value = self.singleton()
            value["Path"] = path
            with self.subTest(path=path), self.assertRaises(process.ProcessError):
                self.validate(value, ["/workspace/absent-control-executable"])

    def test_nonempty_arrays_and_independent_argv_preserve_type_order_value(self):
        for argv in (None, (), [], [""], [False], [0], ["python", "\x00"],
                ["python", "list", "library", "-m"], ARGV + [""], ARGV[:-1]):
            with self.subTest(argv=argv), self.assertRaises(process.ProcessError):
                process.validate_sandbox(fixture_inspection(), process.ProcessPolicy(IMAGE),
                    expected_argv=argv, runtime=runtime_fixture())
        for key in ("Args", "Cmd"):
            for args in (ARGV[1:][::-1], ARGV[1:] + [""], ARGV[1:-1], [False], [1], "-m"):
                value = fixture_inspection()
                (value["Config"] if key == "Cmd" else value)[key] = args
                with self.subTest(key=key, args=args), self.assertRaises(process.ProcessError):
                    self.validate(value)

    def test_invocation_requires_pinned_runtime_and_explicit_policy(self):
        value = self.singleton()
        for key in runtime_fixture():
            runtime = runtime_fixture()
            del runtime[key]
            with self.subTest(missing=key), self.assertRaises(process.ProcessError):
                self.validate(value, [value["Path"]], runtime)
        for key, bad in (("engine_version", "29.2.2"), ("api_version", "1.48"),
                ("engine_git_commit", "abcdef0"), ("image_id", "sha256:" + "0" * 64),
                ("oom_kill_disable_supported", 0), ("cgroup_version", "1")):
            with self.subTest(key=key), self.assertRaises(process.ProcessError):
                self.validate(value, [value["Path"]], {**runtime_fixture(), key: bad})
        policy = process.command_policy()
        self.assertEqual(policy["policy_id"], process.COMMAND_POLICY_ID)
        self.assertEqual(process.command_policy_sha256(), hashlib.sha256(process._encoded(policy)).hexdigest())
        self.assertEqual(policy["empty_command_plan_sha256"], hashlib.sha256(
            Path("analysis/candidate-b03-empty-command-plan-v3.json").read_bytes()).hexdigest())
        policy["runtime_gate"]["os"] = "other"
        self.assertEqual(process.command_policy()["runtime_gate"]["os"], "linux")

    def test_null_empty_identity_changes_still_fail_prestart_and_final(self):
        before = self.singleton()
        after = self.singleton([])
        for phase in ("candidate-created-to-prestart", "candidate-created-to-exited"):
            for old, new in ((before, after), (after, before)):
                with self.subTest(phase=phase, cmd=old["Config"]["Cmd"]):
                    result = process.identity_comparison(process.immutable_inspection(old),
                        process.immutable_inspection(new), runtime_fixture(), phase,
                        before_full_inspection=old, after_full_inspection=new)
                    self.assertFalse(result["matches"])

    def test_declared_argv_is_copied_and_fresh_validation_runs_before_attach(self):
        helper = CandidateClientProcessV4LifecycleTests()
        argv = list(ARGV)
        seen = []
        real_validate = process.validate_sandbox
        def validate(value, policy, **kwargs):
            seen.append((deepcopy(kwargs), value["State"]["Status"]))
            return real_validate(value, policy, **kwargs)
        def mutate(name):
            if name == "p-intent.json":
                argv[:] = ["changed"]
        with patch.object(process, "validate_sandbox", side_effect=validate):
            result, saved, events = helper.fake_run(declared_argv=argv, retain_mutation=mutate)
        self.assertEqual(result["status"], "completed")
        self.assertEqual(len(seen), 2)
        self.assertEqual([row[0]["expected_argv"] for row in seen], [ARGV, ARGV])
        intent = process.strict_json_loads(saved["p-intent.json"])
        self.assertEqual(intent["expected_argv"], ARGV)
        self.assertEqual(intent["expected_argv_sha256"], hashlib.sha256(process._encoded(ARGV)).hexdigest())
        self.assertLess(events.index("p-prestart-comparison.json"), events.index("p-attach-request.bin"))

    def test_wrong_declared_endpoint_or_argv_cannot_reach_runtime_or_attach(self):
        for runtime, argv in (({**runtime_fixture(), "endpoint": {**asdict(ENDPOINT), "inode": 3}}, ARGV),
                              (runtime_fixture(), ["other"]), ({}, ARGV)):
            with patch.object(process, "runtime_identity") as runtime_call, \
                    patch.object(process.socket, "socket") as socket_call, self.assertRaises(process.ProcessError):
                process.run_process(ENDPOINT, container_id=IDENTITY, expected=fixture_inspection(),
                    policy=process.ProcessPolicy(IMAGE), retain=lambda *_args: None, label="p",
                    expected_runtime=runtime, expected_argv=argv)
            runtime_call.assert_not_called()
            socket_call.assert_not_called()


class CandidateClientProcessV4StartReceiptTests(unittest.TestCase):
    def run_fake(self, **kwargs):
        return CandidateClientProcessV4LifecycleTests().fake_run(**kwargs)

    @staticmethod
    def response(status, body=b'{"message":"offline daemon rejection"}', framing="length"):
        head = f"HTTP/1.1 {status} test\r\n".encode()
        if framing == "length":
            return head + f"Content-Length: {len(body)}\r\n\r\n".encode() + body
        if framing == "chunked":
            return head + b"Transfer-Encoding: chunked\r\n\r\n" + f"{len(body):x}\r\n".encode() + body + b"\r\n0\r\n\r\n"
        return head + b"\r\n" + body

    def assert_receipt(self, result, saved, events, wire, status, body):
        receipt = result["start_response_receipt"]
        value = result["start_response"]
        self.assertEqual(receipt, {"path": "p-start-response-completion.json",
            "bytes": len(saved[receipt["path"]]), "sha256": hashlib.sha256(saved[receipt["path"]]).hexdigest()})
        self.assertEqual(value, process.strict_json_loads(saved[receipt["path"]]))
        self.assertEqual(set(value), {"protocol", "method", "request_path", "container_id", "endpoint_sha256",
            "runtime_sha256", "request", "response", "status", "body_bytes", "body_sha256",
            "framing_complete", "eof_observed", "durably_retained"})
        self.assertEqual(value["protocol"], process.START_RESPONSE_PROTOCOL)
        self.assertEqual(value["method"], "POST")
        self.assertEqual(value["request_path"], "/v1.47/containers/" + IDENTITY + "/start")
        self.assertEqual(value["container_id"], IDENTITY)
        self.assertEqual(value["endpoint_sha256"], hashlib.sha256(process._encoded(asdict(ENDPOINT))).hexdigest())
        self.assertEqual(value["runtime_sha256"], hashlib.sha256(process._encoded(runtime_fixture())).hexdigest())
        self.assertEqual(value["status"], status)
        self.assertEqual((value["body_bytes"], value["body_sha256"]), (len(body), hashlib.sha256(body).hexdigest()))
        for field, suffix in (("request", "start-request.bin"), ("response", "start-response.bin")):
            raw = saved["p-" + suffix]
            self.assertEqual(value[field], {"path": "p-" + suffix, "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()})
            self.assertIn(value[field]["path"], result["evidence"])
        self.assertEqual(saved[value["response"]["path"]], wire)
        self.assertTrue(all(value[key] is True for key in ("framing_complete", "eof_observed", "durably_retained")))
        self.assertLess(events.index("p-start-request.bin"), events.index("p-start-response.bin"))
        self.assertLess(events.index("p-start-response.bin"), events.index(receipt["path"]))
        self.assertLess(events.index(receipt["path"]), events.index("p-process.json"))
        self.assertEqual(process.strict_json_loads(saved["p-process.json"])["start_response"], value)

    def test_complete_responses_bind_raw_bytes_for_all_framing_modes(self):
        body = b'{"message":"offline daemon rejection"}'
        for framing in ("length", "chunked", "close"):
            wire = self.response(400, body, framing)
            with self.subTest(framing=framing):
                result, saved, events = self.run_fake(raw_start_response=wire)
                self.assert_receipt(result, saved, events, wire, 400, body)
                self.assertLess(events.index("p-start-response-completion.json"), events.index("kill"))
                self.assertLess(events.index("p-cleanup-state.json"), events.index("p-process.json"))
                self.assertIsNone(result["exit_code"])
                self.assertIsNone(result["startup_comparison"])
                self.assertNotIn("wait", events)
                self.assertNotIn("inspect-final", events)

    def test_generic_transport_records_statuses_without_qualification_predicate(self):
        for status in (204, 304, 400, 404, 409, 500):
            body = b"" if status in (204, 304) else b"not JSON; generic transport does not grade it"
            wire = self.response(status, body)
            with self.subTest(status=status):
                result, saved, events = self.run_fake(raw_start_response=wire)
                self.assert_receipt(result, saved, events, wire, status, body)
                if status == 204:
                    self.assertEqual(result["status"], "completed")
                else:
                    self.assertEqual(result["status"], "start_error")
                    self.assertFalse(result["completion"]["started"])
                    self.assertFalse(result["completion"]["natural"])
                    self.assertIsNone(result["exit_code"])
                    self.assertEqual(saved[result["stderr"]["path"]], b"")

    def test_lost_and_malformed_responses_never_gain_completion_receipt(self):
        bodies = [b"", b"HTTP/1.1 400 test\r\n", b"HTTP/1.1 400 test\r\nContent-Length: 9\r\n\r\n{}",
            b"HTTP/1.1 400 test\r\nContent-Length: 2\r\ncontent-length: 2\r\n\r\n{}",
            b"HTTP/1.1 400 test\r\nContent-Length: 2\r\nTransfer-Encoding: chunked\r\n\r\n{}",
            b"HTTP/1.1 400 test\r\nTransfer-Encoding: chunked\r\n\r\n2\r\n{}\r\n",
            self.response(400) + b"extra", self.response(400) + self.response(400),
            b"HTTP/1.1 400 test\r\nContent-Length: 1048577\r\n\r\n"]
        for wire in bodies:
            with self.subTest(wire=wire):
                result, saved, events = self.run_fake(raw_start_response=wire)
                self.assertIsNone(result["start_response"])
                self.assertIsNone(result["start_response_receipt"])
                self.assertNotIn("p-start-response-completion.json", saved)
                self.assertFalse(result["completion"]["natural"])
                self.assertIsNone(result["exit_code"])
                self.assertEqual(events.count("start"), 1)
                self.assertIn("kill", events)
                self.assertNotIn("wait", events)

    def test_file_appears_complete_but_socket_eof_was_not_observed(self):
        for framing in ("length", "chunked", "close"):
            wire = self.response(400, framing=framing)
            result, saved, events = self.run_fake(raw_start_response=wire, start_eof_error=TimeoutError())
            self.assertEqual(saved["p-start-response.bin"], wire)
            self.assertEqual(result["status"], "timeout")
            self.assertIsNone(result["start_response"])
            self.assertNotIn("p-start-response-completion.json", saved)
            self.assertIn("kill", events)

    def test_absent_or_lost_start_transport_keeps_metadata_null(self):
        for error in (OSError("offline reset"), process.ProcessError("offline malformed reply"), ValueError("offline parse")):
            with self.subTest(error=type(error).__name__):
                result, saved, events = self.run_fake(start_exception=error)
                self.assertEqual(result["status"], "start_error")
                self.assertIsNone(result["start_response"])
                self.assertIsNone(result["start_response_receipt"])
                self.assertNotIn("p-start-response-completion.json", saved)
                self.assertEqual(events.count("start"), 1)
                self.assertIn("kill", events)

    def test_failed_raw_or_completion_retention_keeps_null_metadata_and_cleans(self):
        for status in (400, 204):
            wire = self.response(status, body=b"" if status == 204 else b'{"message":"offline rejection"}')
            for artifact in ("p-start-request.bin", "p-start-response.bin", "p-start-response-completion.json"):
                for after_write in (False, True):
                    options = {"reject_after_write" if after_write else "reject_retention": artifact}
                    with self.subTest(status=status, artifact=artifact, after_write=after_write):
                        result, saved, events = self.run_fake(raw_start_response=wire, **options)
                        self.assertIsNone(result["start_response"])
                        self.assertIsNone(result["start_response_receipt"])
                        self.assertIs(result["completion"]["started"],
                                      status == 204 and artifact != "p-start-request.bin")
                        self.assertFalse(result["completion"]["natural"])
                        self.assertIsNone(result["exit_code"])
                        self.assertIsNone(result["startup_comparison"])
                        self.assertIn("kill", events)
                        self.assertIn("p-cleanup-state.json", saved)
                        self.assertNotIn("wait", events)
                        self.assertNotIn("inspect-final", events)
                        self.assertEqual(events.count("start"), 1)

    def test_null_command_original_control_reaches_real_parser_once(self):
        def singleton(value):
            value.update(Path="/workspace/absent-control-executable", Args=[])
            value["Config"].update(Entrypoint=[value["Path"]], Cmd=None)
        wire = self.response(400)
        result, saved, events = self.run_fake(initial_edit=singleton,
            declared_argv=["/workspace/absent-control-executable"], raw_start_response=wire)
        self.assertEqual(events.count("start"), 1)
        self.assertEqual(result["status"], "start_error")
        self.assertIsNotNone(result["start_response_receipt"])
        self.assertIsNone(result["exit_code"])
        self.assertFalse(result["completion"]["natural"])
        intent = process.strict_json_loads(saved["p-intent.json"])
        self.assertEqual(intent["expected_argv"], ["/workspace/absent-control-executable"])
        self.assertIn("kill", events)

    def test_policies_and_plan_are_bound_before_dispatch(self):
        result, saved, _events = self.run_fake(raw_start_response=self.response(400))
        intent = process.strict_json_loads(saved["p-intent.json"])
        for kind in ("command", "start_response"):
            policy = getattr(process, kind + "_policy")()
            digest = getattr(process, kind + "_policy_sha256")()
            self.assertEqual(intent[kind + "_policy"], policy)
            self.assertEqual(intent[kind + "_policy_sha256"], digest)
            self.assertEqual(digest, hashlib.sha256(process._encoded(policy)).hexdigest())
        self.assertEqual(intent["sources"]["analysis/candidate-b03-empty-command-plan-v3.json"],
            process.EMPTY_COMMAND_PLAN_SHA256)
        self.assertIsNotNone(result["prestart_comparison"])
