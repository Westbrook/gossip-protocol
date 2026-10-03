"""Offline controls execute the actual standalone helper over fake sockets only."""
from __future__ import annotations

import base64
from dataclasses import asdict, replace
import errno
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import socket
import sys
import types
import unittest
from unittest.mock import patch

from gossip_harness import candidate_http_transport_v1 as wire


PORT = 8765
PROC_HEADER = b"  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode\n"


def recipe(body: bytes = b"", method: str = "GET") -> dict:
    return {"method": method, "target": "/health", "headers": [
        ["Host", "127.0.0.1:8765"], ["Connection", "close"], ["Content-Length", str(len(body))]],
        "body_b64": base64.b64encode(body).decode("ascii")}


def snapshot(limits: wire.WireLimits) -> dict:
    return {"byteorder": "little", "tables": {name: {
        "status": "ok", "raw": wire._record(PROC_HEADER), "errno": None} for name in ("tcp", "tcp6")}}


class FakeSocket:
    def __init__(self, parts: list | None = None, *, send_size: int = 7,
                 send_error_after: int | None = None, connect_error: OSError | None = None) -> None:
        self.parts = list(parts or [])
        self.sent = bytearray()
        self.send_size = send_size
        self.send_error_after = send_error_after
        self.connect_error = connect_error
        self.address = None
        self.timeouts = []
        self.closed = False
        self.recv_calls = 0

    def settimeout(self, value: float) -> None:
        self.timeouts.append(value)

    def connect(self, address: tuple[str, int]) -> None:
        self.address = address
        if self.connect_error is not None:
            raise self.connect_error

    def send(self, raw: bytes) -> int:
        if self.send_error_after is not None and len(self.sent) >= self.send_error_after:
            raise OSError(errno.EPIPE, "send failure")
        count = min(len(raw), self.send_size)
        self.sent.extend(raw[:count])
        return count

    def recv(self, size: int) -> bytes:
        self.recv_calls += 1
        value = self.parts.pop(0) if self.parts else b""
        if isinstance(value, Exception):
            raise value
        if len(value) > size:
            self.parts.insert(0, value[size:])
        return value[:size]

    def close(self) -> None:
        self.closed = True


class Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, amount: float) -> None:
        self.now += amount


def probe(parts: list, *, limits: wire.WireLimits | None = None, request: dict | None = None,
          sock: FakeSocket | None = None) -> tuple[wire.WireObservation, dict, FakeSocket]:
    limits = limits or wire.WireLimits()
    request = request or recipe()
    sock = sock or FakeSocket(parts)
    payload = wire._run_probe(wire.build_probe_input(request, PORT, limits),
                              socket_factory=lambda *args: sock, snapshot=snapshot)
    return wire.decode_probe_output(wire.encoded(payload), request, PORT, limits), payload, sock


class HttpWireRequestTests(unittest.TestCase):
    def test_literal_order_and_body_are_exact(self) -> None:
        value = recipe(b' {"x":1}\n', "POST")
        value["headers"].append(["cOnTeNt-TyPe", "application/json"])
        raw = wire.request_bytes(value, PORT)
        self.assertEqual(raw, b'POST /health HTTP/1.1\r\nHost: 127.0.0.1:8765\r\nConnection: close\r\nContent-Length: 9\r\ncOnTeNt-TyPe: application/json\r\n\r\n {"x":1}\n')

    def test_product_oversize_request_is_not_transport_capped(self) -> None:
        for size in (65536, 65537):
            self.assertTrue(wire.request_bytes(recipe(b" " * size, "POST"), PORT).endswith(b" " * size))

    def test_closed_recipe_method_target_and_control_injection(self) -> None:
        variants = [{**recipe(), "unknown": 1}]
        for method in ("get", "CONNECT", "TRACE", "GET\r\nX", True, None):
            variants.append({**recipe(), "method": method})
        for target in ("http://x/", "//evil/", "/a#b", "/a b", "/a\r\nX: y", "/a\x00", "/a\\b", "/é"):
            variants.append({**recipe(), "target": target})
        for value in variants:
            with self.subTest(value=value), self.assertRaises(wire.WireError):
                wire.request_bytes(value, PORT)

    def test_header_injection_and_framing_override_rejected(self) -> None:
        for pair in (["Bad Name", "x"], ["X", "a\r\nb"], ["X", "a\x00b"], ["X", "a\x7fb"],
                     ["Transfer-Encoding", "chunked"], ["Content-Length", "0"], ["Host", "evil"],
                     ["Expect", "100-continue"], ["Upgrade", "websocket"], ["Trailer", "X"]):
            value = recipe()
            value["headers"].append(pair)
            with self.subTest(pair=pair), self.assertRaises(wire.WireError):
                wire.request_bytes(value, PORT)
        for pair in (("X", "y"), ["X"], ["X", 2]):
            value = recipe()
            value["headers"].append(pair)
            with self.assertRaises(wire.WireError):
                wire.request_bytes(value, PORT)

    def test_host_connection_and_content_length_are_bound(self) -> None:
        for index, value in ((0, "localhost:8765"), (0, "127.0.0.1:8766"), (1, "keep-alive, close"),
                             (2, "00"), (2, "+0"), (2, "1")):
            request = recipe()
            request["headers"][index][1] = value
            with self.assertRaises(wire.WireError):
                wire.request_bytes(request, PORT)
        request = recipe()
        request["headers"].pop()
        wire.request_bytes(request, PORT)
        request["method"] = "POST"
        with self.assertRaises(wire.WireError):
            wire.request_bytes(request, PORT)

    def test_canonical_base64_and_limits_types(self) -> None:
        for body in ("/x==", "eA==\n", "eA", "!!!!", True):
            with self.subTest(body=body), self.assertRaises(wire.WireError):
                wire.request_bytes({**recipe(), "body_b64": body}, PORT)
        for port in (True, 0, -1, 65536, "8765"):
            with self.assertRaises(wire.WireError):
                wire.request_bytes(recipe(), port)
        for values in ({"timeout_seconds": float("inf")}, {"timeout_seconds": float("nan")},
                       {"timeout_seconds": True}, {"response_limit_bytes": True},
                       {"request_limit_bytes": 0}, {"listener_limit_bytes": 1048577}):
            with self.assertRaises(wire.WireError):
                wire.WireLimits(**values)

    def test_explicit_keep_alive_is_preserved_and_framed_without_eof(self) -> None:
        request = recipe()
        request["headers"][1][1] = "KeEp-AlIvE"
        raw = wire.request_bytes(request, PORT)
        self.assertIn(b"Connection: KeEp-AlIvE\r\n", raw)
        value, _, sock = probe([b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}", TimeoutError()], request=request)
        self.assertTrue(value.exchange_complete)
        self.assertFalse(value.socket_eof)
        self.assertEqual(bytes(sock.sent), raw)

    def test_request_header_and_body_caps_are_independent(self) -> None:
        for limits in (wire.WireLimits(header_limit_bytes=40), wire.WireLimits(request_limit_bytes=80),
                       wire.WireLimits(header_count_limit=2)):
            with self.assertRaises(wire.WireError):
                wire.request_bytes(recipe(), PORT, limits)


class HttpWireFramingTests(unittest.TestCase):
    def test_length_keepopen_has_complete_body_without_eof(self) -> None:
        value, _, sock = probe([b"HTTP/1.1 200 Anything\r\nContent-Length: 2\r\n\r\n{}", TimeoutError()])
        self.assertTrue(value.exchange_complete)
        self.assertTrue(value.response.body_complete)
        self.assertFalse(value.socket_eof)
        self.assertEqual(value.termination, "message_complete")
        self.assertEqual(sock.recv_calls, 1)

    def test_chunk_extensions_trailers_repeated_headers(self) -> None:
        raw = (b"HTTP/1.1 200 \r\nTransfer-Encoding: Chunked\r\nSet-Cookie: a=1\r\nSet-Cookie: b=2\r\n\r\n"
               b'01 ; foo = "a\\\"b";bare\r\n{\r\n1;v=token\r\n}\r\n0;end=yes\r\nX-Trailer: good\r\n\r\n')
        value, _, _ = probe([raw])
        self.assertTrue(value.exchange_complete, value.limitations)
        self.assertEqual(value.response.body, b"{}")
        self.assertEqual(value.response.trailers, (("X-Trailer", "good"),))
        self.assertEqual(value.response.headers[-2:], (("Set-Cookie", "a=1"), ("Set-Cookie", "b=2")))

    def test_close_delimited_requires_observed_eof(self) -> None:
        raw = b"HTTP/1.0 200 OK\r\nContent-Type: application/json\r\n\r\n{}"
        complete, _, _ = probe([raw, b""])
        incomplete, _, _ = probe([raw, TimeoutError()])
        self.assertTrue(complete.exchange_complete)
        self.assertTrue(complete.socket_eof)
        self.assertFalse(incomplete.response.body_complete)
        self.assertEqual(incomplete.response.body, b"{}")
        self.assertIn("awaiting_close", incomplete.limitations)

    def test_informational_then_final_and_all_byte_boundaries(self) -> None:
        raw = (b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 103 Early Hints\r\nLink: </a>\r\n\r\n"
               b"HTTP/1.1 409 Alternative\r\ncOnTeNt-LeNgTh: \t2 \t\r\nX: \xff\r\n\r\n{}")
        for split in range(1, len(raw)):
            value, _, _ = probe([raw[:split], raw[split:]])
            self.assertTrue(value.exchange_complete, (split, value.limitations))
            self.assertEqual(value.response.status_code, 409)
            self.assertEqual(len(value.response.informational), 2)

    def test_chunked_all_byte_boundaries(self) -> None:
        raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\n{}\r\n0\r\nX: y\r\n\r\n"
        for split in range(1, len(raw)):
            value, _, _ = probe([raw[:split], raw[split:]])
            self.assertTrue(value.exchange_complete, (split, value.limitations))
            self.assertEqual(value.response.body, b"{}")

    def test_equal_duplicate_list_lengths_are_not_arbitrary_failures(self) -> None:
        for headers in (b"Content-Length: 2\r\nContent-Length: 02", b"Content-Length: 002, 2, 02"):
            value, _, _ = probe([b"HTTP/1.1 200 OK\r\n" + headers + b"\r\n\r\n{}"])
            self.assertTrue(value.exchange_complete, value.limitations)

    def test_conflicting_invalid_and_unsupported_framing_is_limited(self) -> None:
        for headers in (b"Content-Length: 2\r\nContent-Length: 3", b"Content-Length: 2,3",
                        b"Content-Length: +2", b"Content-Length: 2x", b"Content-Length: ",
                        b"Transfer-Encoding: chunked\r\nContent-Length: 2",
                        b"Transfer-Encoding: gzip, chunked", b"Transfer-Encoding: chunked, chunked",
                        b"Transfer-Encoding: chunked,gzip", b"Transfer-Encoding: chunked;foo=x"):
            value, _, _ = probe([b"HTTP/1.1 409 Error\r\n" + headers + b"\r\n\r\n{}"])
            self.assertFalse(value.exchange_complete)
            self.assertFalse(value.response.body_complete)
            self.assertEqual(value.response.status_code, 409)

    def test_bodyless_rules_and_upgrade_are_separate(self) -> None:
        for method, status, headers in (("HEAD", 200, b"Content-Length: 999\r\n"),
                                        ("GET", 304, b"Content-Length: 99\r\n"), ("GET", 204, b"")):
            value, _, _ = probe([b"HTTP/1.1 " + str(status).encode() + b" OK\r\n" + headers + b"\r\n"],
                                 request=recipe(method=method))
            self.assertTrue(value.exchange_complete)
            self.assertEqual(value.response.framing, "no-body")
        for raw in (b"HTTP/1.1 204 OK\r\nContent-Length: 0\r\n\r\n",
                    b"HTTP/1.1 100 OK\r\nContent-Length: 0\r\n\r\n",
                    b"HTTP/1.1 101 Switching\r\nUpgrade: x\r\n\r\n"):
            value, _, _ = probe([raw])
            self.assertFalse(value.exchange_complete)

    def test_control_header_status_line_and_lone_lf_rejection(self) -> None:
        for raw in (b"HTTP/1.1 200 OK\nContent-Length: 2\n\n{}",
                    b"HTTP/1.1 200 O\x00K\r\nContent-Length: 2\r\n\r\n{}",
                    b"HTTP/1.1 200\r\nContent-Length: 2\r\n\r\n{}",
                    b"HTTP/1.1 200 OK\r\n Content-Length: 2\r\n\r\n{}",
                    b"HTTP/1.1 200 OK\r\nContent-Length : 2\r\n\r\n{}",
                    b"HTTP/1.1 200 OK\r\nX: a\x7fb\r\n\r\n{}"):
            value, _, _ = probe([raw])
            self.assertFalse(value.exchange_complete)

    def test_valid_looking_json_prefix_is_not_complete_response(self) -> None:
        raw = b'HTTP/1.1 409 Error\r\nContent-Length: 100\r\n\r\n{"error":"job_conflict"}'
        value, _, _ = probe([raw, b""])
        self.assertTrue(value.socket_eof)
        self.assertEqual(value.response.status_code, 409)
        self.assertFalse(value.response.body_complete)
        self.assertIn("incomplete_content_length", value.limitations)

    def test_truncated_chunk_components_and_malformed_extensions(self) -> None:
        prefix = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
        for body in (b"2", b"2\r\n{", b"2\r\n{}\r", b"2\r\n{}xx0\r\n\r\n", b"0\r\n",
                     b"0\r\nX: y\r\n", b"+2\r\n{}\r\n0\r\n\r\n", b"0x2\r\n{}\r\n0\r\n\r\n",
                     b'2;foo="unterminated\r\n{}\r\n0\r\n\r\n', b"2;foo=\r\n{}\r\n0\r\n\r\n",
                     b"0\r\nContent-Length: 0\r\n\r\n", b"0\r\nContent-Type: x\r\n\r\n"):
            value, _, _ = probe([prefix + body])
            self.assertFalse(value.response.body_complete, body)

    def test_extra_captured_bytes_never_grant_complete_prefix(self) -> None:
        for raw in (b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}junk",
                    b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\nHTTP/1.1 200 OK\r\n\r\n",
                    b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\nx",
                    b"HTTP/1.1 204 OK\r\n\r\nx"):
            value, _, _ = probe([raw])
            self.assertFalse(value.response.framing_complete)
            self.assertEqual(value.response.limitation, "excess_response_bytes")

    def test_json_is_raw_even_when_ambiguous_or_invalid(self) -> None:
        for body in (b'{"x":1,"x":2}', b'{"x":NaN}', b"{}junk", b"\xff"):
            raw = b"HTTP/1.1 200 OK\r\nContent-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body
            value, _, _ = probe([raw])
            self.assertTrue(value.exchange_complete)
            self.assertEqual(value.response.body, body)

    def test_header_chunk_count_and_integer_bounds(self) -> None:
        cases = [(b"HTTP/1.1 200 OK\r\nX: " + b"x" * 100 + b"\r\n\r\n", wire.WireLimits(header_limit_bytes=96)),
                 (b"HTTP/1.1 200 OK\r\nX: 1\r\nX: 2\r\nX: 3\r\nX: 4\r\n\r\n", wire.WireLimits(header_count_limit=3)),
                 (b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nx\r\n0\r\n\r\n", wire.WireLimits(chunk_count_limit=1)),
                 (b"HTTP/1.1 200 OK\r\nContent-Length: " + b"9" * 5000 + b"\r\n\r\n", wire.WireLimits()),
                 (b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n" + b"f" * 5000 + b"\r\n", wire.WireLimits())]
        for raw, limits in cases:
            value, _, _ = probe([raw], limits=limits)
            self.assertFalse(value.exchange_complete)
            self.assertIn("limit", value.response.limitation or "")


class HttpWireHelperTests(unittest.TestCase):
    def test_partial_sends_are_counted_and_fail_without_receiving(self) -> None:
        sock = FakeSocket([], send_error_after=14)
        value, control, _ = probe([], sock=sock)
        self.assertEqual(len(value.sent), 14)
        self.assertEqual(value.sent, wire.request_bytes(recipe(), PORT)[:14])
        self.assertFalse(value.sent_complete)
        self.assertEqual(value.termination, "send_error")
        self.assertEqual(sock.recv_calls, 0)
        self.assertEqual(control["errno"], errno.EPIPE)
        self.assertTrue(sock.closed)

    def test_loopback_only_exact_socket_and_deadline(self) -> None:
        made = []
        sock = FakeSocket([b"HTTP/1.1 204 OK\r\n\r\n"])
        def factory(*args: object) -> FakeSocket:
            made.append(args)
            return sock
        control = wire._run_probe(wire.build_probe_input(recipe(), PORT), socket_factory=factory, snapshot=snapshot)
        self.assertEqual(made, [(socket.AF_INET, socket.SOCK_STREAM)])
        self.assertEqual(sock.address, ("127.0.0.1", PORT))
        self.assertTrue(all(0 < t <= 10 for t in sock.timeouts))
        self.assertEqual(control["connect_attempts"], 1)

    def test_refused_connect_retries_before_request_once(self) -> None:
        clock = Clock()
        sockets = [FakeSocket(connect_error=ConnectionRefusedError(errno.ECONNREFUSED, "refused")),
                   FakeSocket([b"HTTP/1.1 204 OK\r\n\r\n"])]
        unused = list(sockets)
        control = wire._run_probe(wire.build_probe_input(recipe(), PORT), socket_factory=lambda *args: unused.pop(0),
                                  clock=clock, sleep=clock.sleep, snapshot=snapshot)
        value = wire.decode_probe_output(wire.encoded(control), recipe(), PORT)
        self.assertTrue(value.exchange_complete)
        self.assertEqual(control["connect_attempts"], 2)
        self.assertEqual(control["connect_refused_attempts"], 1)
        self.assertEqual(sockets[0].sent, b"")
        self.assertEqual(sockets[1].sent, value.request)
        self.assertTrue(all(sock.closed for sock in sockets))

    def test_refusal_readiness_is_total_deadline_bounded(self) -> None:
        clock = Clock()
        limits = wire.WireLimits(timeout_seconds=0.025)
        control = wire._run_probe(wire.build_probe_input(recipe(), PORT, limits),
                                  socket_factory=lambda *args: FakeSocket(connect_error=ConnectionRefusedError(errno.ECONNREFUSED, "no")),
                                  clock=clock, sleep=clock.sleep, snapshot=snapshot)
        value = wire.decode_probe_output(wire.encoded(control), recipe(), PORT, limits)
        self.assertEqual(value.termination, "timeout")
        self.assertEqual(control["connect_attempts"], 3)
        self.assertEqual(control["connect_refused_attempts"], 3)
        self.assertEqual(value.sent, b"")
        self.assertFalse(value.listeners_before.complete)
        self.assertEqual(value.listeners_before_stage, "connection_failed_diagnostic")

    def test_nonrefused_connect_error_is_not_retried(self) -> None:
        value, control, _ = probe([], sock=FakeSocket(connect_error=OSError(errno.EACCES, "denied")))
        self.assertEqual(value.termination, "connect_error")
        self.assertEqual(control["connect_attempts"], 1)

    def test_listener_snapshots_bracket_send_after_connected(self) -> None:
        events = []
        class OrderedSocket(FakeSocket):
            def connect(self, address: tuple[str, int]) -> None:
                events.append("connect")
                super().connect(address)

            def send(self, raw: bytes) -> int:
                events.append("send")
                return super().send(raw)

            def close(self) -> None:
                events.append("close")
                super().close()

        def observed(limits: wire.WireLimits) -> dict:
            events.append("snapshot")
            return snapshot(limits)

        control = wire._run_probe(wire.build_probe_input(recipe(), PORT),
                                  socket_factory=lambda *args: OrderedSocket([b"HTTP/1.1 204 OK\r\n\r\n"]), snapshot=observed)
        self.assertEqual(events[:3], ["connect", "snapshot", "send"])
        self.assertEqual(events[-2:], ["close", "snapshot"])
        self.assertTrue(wire.decode_probe_output(wire.encoded(control), recipe(), PORT).exchange_complete)

    def test_timeout_receive_reset_and_byte_cap_are_limitations(self) -> None:
        for parts, expected in (([TimeoutError()], "timeout"), ([OSError(errno.ECONNRESET, "reset")], "receive_error")):
            value, _, _ = probe(parts)
            self.assertEqual(value.termination, expected)
            self.assertTrue(value.sent_complete)
            self.assertFalse(value.exchange_complete)
        limits = wire.WireLimits(response_limit_bytes=100)
        value, control, _ = probe([b"HTTP/1.1 200 OK\r\n\r\n" + b"x" * 1000], limits=limits)
        self.assertEqual(value.termination, "response_limit")
        self.assertEqual(len(value.received), 101)
        self.assertEqual(control["received"]["sha256"], hashlib.sha256(value.received).hexdigest())

    def test_operation_cap_bounds_small_reads_and_sends(self) -> None:
        limits = wire.WireLimits(io_operation_limit=3)
        value, control, sock = probe([], limits=limits)
        self.assertEqual(value.termination, "io_operation_limit")
        self.assertEqual(control["io_operations"], 4)
        self.assertEqual(len(sock.sent), 14)

    def test_strict_probe_input_rejects_duplicate_unknown_and_noncanonical_control(self) -> None:
        raw = wire.build_probe_input(recipe(), PORT)
        variants = [raw.replace(b'"port":8765', b'"port":8765,"port":8765'),
                    raw.replace(b'"method":"GET"', b'"method":"GET","method":"POST"'),
                    raw.replace(b'"port":8765', b'"port":NaN'), raw + b" ",
                    wire.encoded({**json.loads(raw), "extra": True})]
        for value in variants:
            with self.subTest(value=value[:100]), self.assertRaises(wire.WireError):
                wire._run_probe(value, socket_factory=lambda *a: self.fail("socket reached"), snapshot=snapshot)

    def test_standalone_helper_source_executes_without_package_imports(self) -> None:
        module = types.ModuleType("trusted_c03_helper_test")
        sys.modules[module.__name__] = module
        try:
            exec(compile(wire.helper_source(), "/probe/helper.py", "exec"), module.__dict__)
            sock = FakeSocket([b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}"])
            control = module._run_probe(wire.build_probe_input(recipe(), PORT),
                                        socket_factory=lambda *args: sock, snapshot=snapshot)
            value = wire.decode_probe_output(wire.encoded(control), recipe(), PORT)
            self.assertTrue(value.exchange_complete)
            self.assertEqual(wire.helper_sha256(), hashlib.sha256(wire.helper_source()).hexdigest())
        finally:
            del sys.modules[module.__name__]

    def test_main_fixed_file_path_and_safe_failure_stdout(self) -> None:
        fake_out = types.SimpleNamespace(buffer=io.BytesIO())
        raw = wire.build_probe_input(recipe(), PORT)
        control = wire._run_probe(raw, socket_factory=lambda *args: FakeSocket([b"HTTP/1.1 204 OK\r\n\r\n"]), snapshot=snapshot)
        with patch.object(wire.sys, "argv", ["/probe/helper.py", "/probe/request.json"]), \
                patch("builtins.open", return_value=io.BytesIO(raw)) as opened, \
                patch.object(wire, "_run_probe", return_value=control), patch.object(wire.sys, "stdout", fake_out):
            self.assertEqual(wire._main(), 0)
            opened.assert_called_once_with("/probe/request.json", "rb")
        self.assertTrue(wire.decode_probe_output(fake_out.buffer.getvalue(), recipe(), PORT).exchange_complete)
        for args in (["/probe/helper.py"], ["/probe/helper.py", "/tmp/elsewhere"], ["/probe/helper.py", "/probe/request.json", "extra"]):
            with patch.object(wire.sys, "argv", args), patch.object(wire.sys, "stderr", io.StringIO()), \
                    patch.object(wire.sys, "stdout", types.SimpleNamespace(buffer=io.BytesIO())), \
                    patch("builtins.open", side_effect=AssertionError("unexpected input read")):
                self.assertEqual(wire._main(), 2)

    def test_output_envelope_bound_includes_four_listener_captures(self) -> None:
        limits = wire.WireLimits(response_limit_bytes=100, listener_limit_bytes=128)
        _, control, _ = probe([b"HTTP/1.1 200 OK\r\n\r\n" + b"x" * 100], limits=limits)
        for key in ("listeners_before", "listeners_after"):
            control[key] = {"byteorder": "little", "tables": {name: {
                "status": "limit", "raw": wire._record(b"x" * 129), "errno": None} for name in ("tcp", "tcp6")}}
        self.assertLess(len(wire.encoded(control)), wire.max_probe_output_bytes(limits))
        self.assertLess(wire.max_probe_output_bytes(), 16 * 1024 * 1024)


class HttpWireTranscriptTests(unittest.TestCase):
    def setUp(self) -> None:
        _, self.control, _ = probe([b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}"])

    def test_tampered_hash_count_bytes_and_request_binding_rejected(self) -> None:
        modifications = [("request_sha256", "0" * 64), ("input_sha256", "0" * 64),
                         ("sent", wire._record(b"GET /other")), ("received", {**self.control["received"], "bytes": True}),
                         ("received", {**self.control["received"], "sha256": "0" * 64}),
                         ("received", {**self.control["received"], "extra": 0})]
        for field, value in modifications:
            with self.subTest(field=field), self.assertRaises(wire.WireError):
                wire.decode_probe_output(wire.encoded({**self.control, field: value}), recipe(), PORT)

    def test_closed_control_shape_duplicates_and_nonfinite_rejected(self) -> None:
        raw = wire.encoded(self.control)
        for value in (raw.replace(b'"connected":true', b'"connected":true,"connected":true'),
                      raw.replace(b'"connected":true', b'"connected":NaN'),
                      wire.encoded({**self.control, "pass": True}), raw + b"{}"):
            with self.assertRaises(wire.WireError):
                wire.decode_probe_output(value, recipe(), PORT)

    def test_socket_flags_and_termination_cannot_grant_completion(self) -> None:
        for field, value in (("socket_eof", True), ("connected", False), ("connected", 1), ("termination", "eof"),
                             ("termination", "timeout"), ("termination", "response_limit"), ("termination", "parse_stopped"),
                             ("termination", "connect_error"), ("errno", 5), ("connect_attempts", True),
                             ("connect_attempts", 0), ("connect_refused_attempts", 1),
                             ("listeners_before_stage", "connection_failed_diagnostic"),
                             ("io_operations", True), ("elapsed_seconds", -1)):
            with self.subTest(field=field, value=value), self.assertRaises(wire.WireError):
                wire.decode_probe_output(wire.encoded({**self.control, field: value}), recipe(), PORT)

    def test_changed_limits_or_recipe_never_rebind_same_transcript(self) -> None:
        with self.assertRaises(wire.WireError):
            wire.decode_probe_output(wire.encoded(self.control), recipe(), PORT, wire.WireLimits(timeout_seconds=11))
        with self.assertRaises(wire.WireError):
            wire.decode_probe_output(wire.encoded(self.control), {**recipe(), "target": "/different"}, PORT)


class HttpWireListenerTests(unittest.TestCase):
    def test_actual_snapshot_reads_only_bounded_fixed_kernel_paths(self) -> None:
        opened = []
        def opening(path: str, mode: str) -> io.BytesIO:
            opened.append((path, mode))
            return io.BytesIO(PROC_HEADER)
        with patch("builtins.open", side_effect=opening):
            value = wire._snapshot(wire.WireLimits())
        self.assertEqual(opened, [("/proc/net/tcp", "rb"), ("/proc/net/tcp6", "rb")])
        self.assertTrue(wire.decode_listener_snapshot(value, PORT).complete)

    def test_loopback_wildcard_ipv6_and_other_port_inventory(self) -> None:
        value = snapshot(wire.WireLimits())
        rows = (b"0: 0100007F:223D 00000000:0000 0A 0:0 0:0 0 65534 0 100\n"
                b"1: 00000000:223D 00000000:0000 0A 0:0 0:0 0 65534 0 101\n"
                b"2: 00000000:9999 00000000:0000 0A 0:0 0:0 0 65534 0 102\n")
        rows6 = (b"0: 00000000000000000000000001000000:223D 00000000000000000000000000000000:0000 0A 0:0 0:0 0 65534 0 103\n"
                 b"1: 00000000000000000000000000000000:223D 00000000000000000000000000000000:0000 0A 0:0 0:0 0 65534 0 104\n")
        value["tables"]["tcp"]["raw"] = wire._record(PROC_HEADER + rows)
        value["tables"]["tcp6"]["raw"] = wire._record(PROC_HEADER + rows6)
        result = wire.decode_listener_snapshot(value, PORT)
        self.assertTrue(result.complete)
        self.assertEqual(result.listeners, (("tcp", "127.0.0.1", PORT), ("tcp", "0.0.0.0", PORT),
                                            ("tcp6", "::1", PORT), ("tcp6", "::", PORT)))

    def test_absent_listener_distinct_from_failed_table_capture(self) -> None:
        absent = wire.decode_listener_snapshot(snapshot(wire.WireLimits()), PORT)
        self.assertEqual(absent.listeners, ())
        self.assertTrue(absent.complete)
        with patch("builtins.open", side_effect=OSError(errno.EACCES, "denied")):
            failed = wire.decode_listener_snapshot(wire._snapshot(wire.WireLimits()), PORT)
        self.assertFalse(failed.complete)
        self.assertEqual(failed.limitations, ("tcp:unavailable", "tcp6:unavailable"))

    def test_listener_cap_and_malformed_table_stay_unavailable(self) -> None:
        limits = wire.WireLimits(listener_limit_bytes=10)
        with patch("builtins.open", side_effect=lambda *args: io.BytesIO(PROC_HEADER)):
            result = wire.decode_listener_snapshot(wire._snapshot(limits), PORT, limits)
        self.assertFalse(result.complete)
        self.assertEqual(len(result.tcp), 11)
        for raw in (b"", b"not kernel table\n", PROC_HEADER + b"broken row\n"):
            value = snapshot(wire.WireLimits())
            value["tables"]["tcp"]["raw"] = wire._record(raw)
            self.assertFalse(wire.decode_listener_snapshot(value, PORT).complete)

    def test_listener_control_integrity_and_bound_flags_rejected(self) -> None:
        for field, payload in (("status", "limit"), ("status", True), ("errno", True),
                               ("raw", {"b64": "", "bytes": 1, "sha256": "0" * 64})):
            value = snapshot(wire.WireLimits())
            value["tables"]["tcp"][field] = payload
            with self.assertRaises(wire.WireError):
                wire.decode_listener_snapshot(value, PORT)


if __name__ == "__main__":
    unittest.main()
