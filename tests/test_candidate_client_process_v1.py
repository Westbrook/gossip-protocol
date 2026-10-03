"""Offline bounded Engine protocol controls. No Docker socket or candidate runs."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import socket
import time
import threading
import unittest
from unittest.mock import patch

from gossip_harness import candidate_client_process_v1 as process

IMAGE = "sha256:" + "a" * 64
IDENTITY = "b" * 64
ENDPOINT = process.EngineEndpoint("/not-opened-offline.sock", 1, 2)


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
            "AutoRemove": False, "Memory": 256 * 1024 * 1024, "MemorySwap": 256 * 1024 * 1024,
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
    def __init__(self, response, chunks=7, gate=None):
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


class CandidateClientProcessProtocolTests(unittest.TestCase):
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


class CandidateClientProcessLifecycleTests(unittest.TestCase):
    def test_sandbox_and_exact_configuration_projection(self):
        value = fixture_inspection()
        process.validate_sandbox(value, process.ProcessPolicy(IMAGE))
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
                process.validate_sandbox(changed, process.ProcessPolicy(IMAGE))
        changed = deepcopy(value)
        changed["Mounts"].append({"Destination": "/var/run/docker.sock", "Type": "bind", "RW": True})
        with self.assertRaises(process.ProcessError):
            process.validate_sandbox(changed, process.ProcessPolicy(IMAGE))

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

    def fake_run(self, raw=b"", *, exit_code=0, start=204, wait_error=None, final_edit=None, policy=None, premature_eof=False, eof_during_start=False, eof_during_retention=False):
        saved, events = {}, []
        real_thread, readers = threading.Thread, []
        real_control = process._control
        def capture_thread(*args, **kwargs):
            reader = real_thread(*args, **kwargs)
            readers.append(reader)
            return reader
        expected = fixture_inspection()
        final = finished(exit_code)
        if final_edit:
            final_edit(final)
        response = b"HTTP/1.1 101 UPGRADED\r\nConnection: Upgrade\r\nUpgrade: tcp\r\nContent-Type: application/vnd.docker.multiplexed-stream\r\n\r\n" + raw
        gate = threading.Event()
        fake = FakeSocket(response, gate=None if premature_eof else gate)
        sockets = [fake, FakeSocket(b"HTTP/1.1 204 No Content\r\n\r\n")]
        def retain(name, value):
            self.assertNotIn(name, saved)
            saved[name] = value
            events.append(name)
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
            return deepcopy(expected if kwargs["label"] == "inspect-before" else final)
        def control(endpoint, method, path, **kwargs):
            events.append(kwargs["label"])
            if kwargs["label"] == "start":
                self.assertIn("p-attach-request.bin", saved)
                self.assertTrue(fake.sent)
                if eof_during_retention:
                    return real_control(endpoint, method, path, **kwargs)
                if start != 204 or eof_during_start:
                    gate.set()
                if eof_during_start:
                    for reader in readers:
                        reader.join(timeout=2)
                        self.assertFalse(reader.is_alive())
                kwargs["on_response"](start)
                return start, b"engine start failure" if start != 204 else b""
            return 204, b""
        with patch.object(process, "runtime_identity", return_value={"bound": True}) as runtime, \
                patch.object(process.threading, "Thread", side_effect=capture_thread), \
                patch.object(process, "_json_control", side_effect=json_control), \
                patch.object(process, "_control", side_effect=control), \
                patch.object(process.EngineEndpoint, "validate"), \
                patch.object(process.socket, "socket", side_effect=lambda *_a, **_k: sockets.pop(0)):
            result = process.run_process(ENDPOINT, container_id=IDENTITY, expected=expected,
                policy=policy or process.ProcessPolicy(IMAGE, timeout_seconds=1), retain=retain,
                label="p", expected_runtime={"bound": True})
        self.assertTrue(fake.closed)
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
                label="p", expected_runtime={"bound": True})
        self.assertEqual(result["status"], "transport_error")
        self.assertTrue(all("/start" not in call.args[2] for call in control.call_args_list))

    def test_terminal_container_cannot_be_redispatched(self):
        with patch.object(process, "runtime_identity") as runtime, self.assertRaises(process.ProcessError):
            process.run_process(ENDPOINT, container_id=IDENTITY, expected=finished(),
                policy=process.ProcessPolicy(IMAGE), retain=lambda _name, _raw: None,
                label="p", expected_runtime={})
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
        version = {"MinAPIVersion": "1.24", "ApiVersion": "1.47", "Os": "linux"}
        info = {"ID": "daemon", "OSType": "linux"}
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
