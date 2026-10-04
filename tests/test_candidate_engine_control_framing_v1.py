"""Supplied-byte regressions for independent retained Engine-control assertions."""
from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from tests import candidate_engine_control_framing_v1 as framing
from tests import test_candidate_http_docker_v3 as raw_checks


class CandidateEngineControlFramingTests(unittest.TestCase):
    @staticmethod
    def length(body=b"{}", *, declared=None, extra_headers=b""):
        size = len(body) if declared is None else declared
        return (b"HTTP/1.1 200 OK\r\nContent-Length: " + str(size).encode()
                + b"\r\n" + extra_headers + b"\r\n" + body)

    @staticmethod
    def chunks(body=b"{}"):
        return (b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
                + format(len(body), "x").encode() + b"\r\n" + body + b"\r\n0\r\n\r\n")

    def test_framed_length_and_chunks_do_not_invent_socket_eof(self):
        for raw, kind in ((self.length(), "content-length"), (self.chunks(), "chunked")):
            for eof in (None, False, True):
                with self.subTest(kind=kind, eof=eof):
                    observed = framing.decode_control_frame(raw, socket_eof_observed=eof)
                    self.assertEqual((observed["status"], observed["body"], observed["framing"]), (200, b"{}", kind))
                    self.assertIs(observed["framing_complete"], True)
                    self.assertIs(observed["socket_eof_observed"], eof)

    def test_valid_json_prefix_cannot_complete_a_longer_declared_body(self):
        for body, declared in ((b"{}", 3), (b'{"State":{"Running":false}}', 99), (b"", 1)):
            with self.subTest(body=body, declared=declared), self.assertRaisesRegex(framing.ControlFrameError, "Incomplete Content-Length"):
                framing.decode_control_frame(self.length(body, declared=declared))

    def test_duplicate_headers_and_conflicting_framing_are_rejected(self):
        headers = (b"content-length: 2\r\n", b"Content-Length: 9\r\n",
                   b"Transfer-Encoding: chunked\r\n", b"X-Fact: one\r\nx-fact: two\r\n")
        for extra in headers:
            with self.subTest(extra=extra), self.assertRaises(framing.ControlFrameError):
                framing.decode_control_frame(self.length(extra_headers=extra))
        raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\ntransfer-encoding: chunked\r\n\r\n2\r\n{}\r\n0\r\n\r\n"
        with self.assertRaisesRegex(framing.ControlFrameError, "Duplicate"):
            framing.decode_control_frame(raw)

    def test_missing_chunk_suffix_bad_size_and_trailers_are_rejected(self):
        head = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
        for suffix in (b"2\r\n{}\r\n", b"3\r\n{}\r\n0\r\n\r\n", b"2\r\n{}xx0\r\n\r\n",
                       b"2;ext=x\r\n{}\r\n0\r\n\r\n", b"2\r\n{}\r\n0\r\nX-Fact: y\r\n\r\n",
                       b"2\r\n{}\r\n0\r\n", b"g\r\n{}\r\n0\r\n\r\n"):
            with self.subTest(suffix=suffix), self.assertRaises(framing.ControlFrameError):
                framing.decode_control_frame(head + suffix)

    def test_extra_suffix_second_message_and_no_body_mismatch_are_rejected(self):
        for raw in (self.length() + b"x", self.length() + self.length(), self.chunks() + b"\r\n",
                    b"HTTP/1.1 204 Empty\r\n\r\nx", b"HTTP/1.1 204 Empty\r\nContent-Length: 2\r\n\r\n{}",
                    b"HTTP/1.1 304 Same\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n"):
            with self.subTest(raw=raw), self.assertRaises(framing.ControlFrameError):
                framing.decode_control_frame(raw)
        for status in (204, 304):
            observed = framing.decode_control_frame(f"HTTP/1.1 {status} Empty\r\nContent-Length: 0\r\n\r\n".encode())
            self.assertEqual((observed["body"], observed["framing"]), (b"", "no-body"))
            self.assertIs(observed["framing_complete"], True)
            self.assertIsNone(observed["socket_eof_observed"])

    def test_close_delimited_capture_needs_separate_explicit_eof_evidence(self):
        raw = b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n{}"
        for eof in (None, False, True):
            observed = framing.decode_control_frame(raw, socket_eof_observed=eof)
            self.assertEqual((observed["body"], observed["framing"]), (b"{}", "connection-close"))
            self.assertIs(observed["framing_complete"], eof is True)
            self.assertIs(observed["socket_eof_observed"], eof)
        for invalid in (0, 1, "true", [], {}):
            with self.subTest(invalid=invalid), self.assertRaises(framing.ControlFrameError):
                framing.decode_control_frame(raw, socket_eof_observed=invalid)

    def test_header_grammar_count_size_and_final_status_bounds(self):
        # Status plus 127 nonempty headers and a terminating blank line fits the
        # frozen loop. Its 128th nonempty header must not be accepted.
        extras = b"".join(b"X-" + str(i).encode() + b": v\r\n" for i in range(126))
        self.assertTrue(framing.decode_control_frame(self.length(extra_headers=extras))["framing_complete"])
        malformed = (self.length(extra_headers=extras + b"X-Last: v\r\n"),
            self.length(extra_headers=b"X-Large: " + b"a" * 32768 + b"\r\n"),
            self.length(extra_headers=b" Folded: x\r\n"), self.length(extra_headers=b"X-Bad: \x00\r\n"),
            self.length(extra_headers=b"X Bad: v\r\n"), self.length().replace(b"200 OK", b"101 UPGRADED"),
            self.length().replace(b"200 OK", b"100 Continue"), self.length().replace(b"200 OK", b"999 Invalid"),
            self.length().replace(b"Content-Length: 2", b"Content-Length: +2"))
        for raw in malformed:
            with self.subTest(prefix=raw[:80]), self.assertRaises(framing.ControlFrameError):
                framing.decode_control_frame(raw)

    def test_declared_body_wire_and_chunk_count_bounds(self):
        body = b"x" * framing.BODY_LIMIT
        self.assertEqual(len(framing.decode_control_frame(self.length(body))["body"]), framing.BODY_LIMIT)
        for raw in (self.length(body + b"x"), b"x" * (framing.WIRE_LIMIT + 1)):
            with self.subTest(size=len(raw)), self.assertRaises(framing.ControlFrameError):
                framing.decode_control_frame(raw)
        head = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
        exact = head + b"1\r\nx\r\n" * (framing.CHUNK_COUNT_LIMIT - 1) + b"0\r\n\r\n"
        self.assertEqual(len(framing.decode_control_frame(exact)["body"]), framing.CHUNK_COUNT_LIMIT - 1)
        with self.assertRaisesRegex(framing.ControlFrameError, "chunk count"):
            framing.decode_control_frame(head + b"1\r\nx\r\n" * framing.CHUNK_COUNT_LIMIT + b"0\r\n\r\n")

    def test_shared_base_reader_rejects_prefix_and_eof_inference(self):
        with tempfile.TemporaryDirectory(prefix="retained-engine-frame-") as directory:
            root = Path(directory).resolve()
            target = root / "sample-response.bin"
            target.write_bytes(self.length())
            self.assertEqual(raw_checks.retained_engine_json(self, root, "sample"), {})
            for raw in (self.length(declared=3), self.length(extra_headers=b"Content-Length: 2\r\n"),
                        self.length(extra_headers=b"Transfer-Encoding: chunked\r\n")):
                target.write_bytes(raw)
                with self.assertRaises(framing.ControlFrameError):
                    raw_checks.retained_engine_json(self, root, "sample")
            target.write_bytes(b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n{}")
            with self.assertRaisesRegex(AssertionError, "completeness unavailable"):
                raw_checks.retained_engine_json(self, root, "sample")
