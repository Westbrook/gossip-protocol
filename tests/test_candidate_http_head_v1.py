"""Offline bounded head controls; no execution, sockets, or evidence admission."""
from __future__ import annotations

from dataclasses import FrozenInstanceError, fields, replace
import hashlib
import unittest

from gossip_harness import candidate_http_head_v1 as head
from gossip_harness import candidate_http_semantics_v1 as semantics
from gossip_harness import candidate_http_transport_v1 as wire


FINAL = b"HTTP/1.1 409 Conflict\r\n"
EMPTY = b"HTTP/1.1 200 OK\r\n\r\n"
INFO = b"HTTP/1.1 100 Continue\r\n\r\n"
HINT = b"HTTP/1.1 103 Early Hints\r\nLink: </a>\r\n\r\n"


def parse(raw: bytes, **limits: object) -> head.HeadFacts:
    return head.extract_response_head(raw, wire.WireLimits(**limits))


class HttpHeadFactsTests(unittest.TestCase):
    def test_final_status_survives_every_truncated_header_prefix(self) -> None:
        suffix = b"Content-Type: application/json\r\nX-Another: value\r\n\r\n"
        for end in range(len(suffix)):
            with self.subTest(end=end):
                raw = FINAL + suffix[:end]
                value = parse(raw)
                self.assertEqual(value.status, 409)
                self.assertEqual(raw[value.status_range.start:value.status_range.end], FINAL)
                self.assertIsInstance(value.headers, semantics.Missing)
                self.assertEqual(value.header_ranges, ())
        self.assertIsInstance(parse(FINAL + suffix).headers, tuple)

    def test_partial_final_status_never_becomes_a_fact(self) -> None:
        for end in range(len(FINAL)):
            with self.subTest(end=end):
                value = parse(FINAL[:end])
                self.assertIsInstance(value.status, semantics.Missing)
                self.assertIsNone(value.status_range)

    def test_complete_metadata_survives_framing_ambiguities(self) -> None:
        for raw_fields in (b"Content-Length: 2\r\nContent-Length: 3\r\n",
                           b"Content-Length: 2\r\nTransfer-Encoding: chunked\r\n",
                           b"Content-Length: +2\r\n", b"Transfer-Encoding: gzip\r\n"):
            with self.subTest(fields=raw_fields):
                raw = FINAL + raw_fields + b"\r\n{}"
                facts = parse(raw)
                framing = wire.parse_response(raw, "GET")
                self.assertEqual(facts.status, 409)
                self.assertIsInstance(facts.headers, tuple)
                self.assertEqual(facts.limitations, ())
                self.assertFalse(framing.body_complete)
                if b"gzip" not in raw_fields:
                    self.assertFalse(framing.headers_complete)

    def test_metadata_does_not_depend_on_body_or_eof(self) -> None:
        prefix = FINAL + b"Content-Length: 100\r\nContent-Type: application/json\r\n\r\n"
        known = parse(prefix)
        for suffix in (b"", b"{}", b"HTTP/1.1 200 OK\r\n\r\n", b"\x00\xff"):
            with self.subTest(suffix=suffix):
                value = parse(prefix + suffix)
                self.assertEqual(value.status, known.status)
                self.assertEqual(value.headers, known.headers)
                self.assertEqual(value.status_range, known.status_range)
                self.assertEqual(value.header_ranges, known.header_ranges)
                self.assertEqual(value.headers_range, known.headers_range)
                self.assertFalse(wire.parse_response(prefix + suffix, "GET").body_complete)
        self.assertNotIn("body", {field.name for field in fields(head.HeadFacts)})
        self.assertNotIn("authenticated", {field.name for field in fields(head.HeadFacts)})

    def test_wrong_status_survives_semantic_body_unavailability(self) -> None:
        value = parse(FINAL + b"Content-Type: application/json\r\nContent-Length: 100\r\n\r\n{}")
        response = semantics.ResponseFacts(value.status, value.headers, semantics.Missing("incomplete_body"))
        comparison = semantics.compare(response, semantics.success("health"))
        self.assertEqual(comparison.facet("status").disposition, "fail")
        self.assertEqual(comparison.facet("body_shape_value").disposition, "unavailable")

    def test_exact_offsets_after_informational_prefixes(self) -> None:
        raw_fields = b"X-One: a\r\nx-two:\t\xff \t\r\n"
        raw = INFO + HINT + FINAL + raw_fields + b"\r\nbody"
        value = parse(raw)
        self.assertEqual(value.informational_ranges,
                         (head.ByteRange(0, len(INFO)), head.ByteRange(len(INFO), len(INFO + HINT))))
        self.assertEqual(tuple(raw[x.start:x.end] for x in value.informational_ranges), (INFO, HINT))
        start = len(INFO + HINT)
        self.assertEqual(value.status_range, head.ByteRange(start, start + len(FINAL)))
        self.assertEqual(raw[value.status_range.start:value.status_range.end], FINAL)
        self.assertEqual(raw[value.headers_range.start:value.headers_range.end], raw_fields + b"\r\n")
        self.assertEqual(tuple(raw[x.start:x.end] for x in value.header_ranges),
                         (b"X-One: a\r\n", b"x-two:\t\xff \t\r\n"))
        self.assertEqual(value.headers, (("X-One", "a"), ("x-two", "ÿ")))
        self.assertEqual(value.raw_sha256, hashlib.sha256(raw).hexdigest())
        self.assertEqual(value.raw_length, len(raw))

    def test_duplicates_case_and_ows_are_preserved_without_collapsing(self) -> None:
        value = parse(FINAL + b"Set-Cookie: a=1\r\nset-cookie:\tb=2 \t\r\nX: \r\nX:\xff\r\n\r\n")
        self.assertEqual(value.headers,
                         (("Set-Cookie", "a=1"), ("set-cookie", "b=2"), ("X", ""), ("X", "ÿ")))
        self.assertEqual(len(value.header_ranges), 4)

    def test_empty_header_set_is_complete_and_distinct_from_missing(self) -> None:
        value = parse(FINAL + b"\r\n")
        self.assertEqual(value.headers, ())
        self.assertEqual(value.headers_range, head.ByteRange(len(FINAL), len(FINAL) + 2))
        self.assertEqual(value.header_ranges, ())
        self.assertIsInstance(parse(FINAL).headers, semantics.Missing)

    def test_malformed_final_field_preserves_status_but_no_partial_fields(self) -> None:
        for invalid in (b"No-Colon", b" leading: value", b"X : value", b": empty",
                        b"X:\x00", b"X:\x7f", b"X:a\rb", b"X:a\nb", b"\xff: value"):
            with self.subTest(invalid=invalid):
                value = parse(FINAL + b"Good: value\r\n" + invalid + b"\r\n\r\n")
                self.assertEqual(value.status, 409)
                self.assertEqual(value.headers, semantics.Missing("invalid_header"))
                self.assertEqual(value.header_ranges, ())


class HttpHeadInformationalTests(unittest.TestCase):
    def test_valid_info_chain_without_final_is_not_a_final_status(self) -> None:
        for suffix in (b"", b"HTTP/1.1 20", b"HTTP/1.1 200 OK\r"):
            with self.subTest(suffix=suffix):
                value = parse(INFO + HINT + suffix)
                self.assertIsInstance(value.status, semantics.Missing)
                self.assertEqual(len(value.informational_ranges), 2)
                self.assertIsNone(value.status_range)

    def test_info_chain_with_complete_final_line_preserves_final_status(self) -> None:
        value = parse(INFO + HINT + FINAL + b"Partial:")
        self.assertEqual(value.status, 409)
        self.assertEqual(value.headers, semantics.Missing("incomplete_headers"))
        self.assertEqual(len(value.informational_ranges), 2)

    def test_informational_fields_cannot_contain_framing(self) -> None:
        for field in (b"Content-Length: 0", b"Content-Length: invalid",
                      b"Transfer-Encoding: chunked", b"Transfer-Encoding:"):
            with self.subTest(field=field):
                raw = INFO + b"HTTP/1.1 103 Early Hints\r\n" + field + b"\r\n\r\n" + EMPTY
                value = parse(raw)
                self.assertEqual(value.status, semantics.Missing("informational_framing"))
                self.assertEqual(value.informational_ranges, (head.ByteRange(0, len(INFO)),))

    def test_upgrade_is_a_traversal_barrier(self) -> None:
        raw = b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: example\r\n\r\n" + EMPTY
        value = parse(raw)
        self.assertEqual(value.status, semantics.Missing("unsupported_upgrade"))
        self.assertEqual(value.informational_ranges, ())

    def test_malformed_informational_prefix_never_resynchronizes(self) -> None:
        for prefix in (b"HTTP/1.1 100 Continue\r\nBroken\r\n\r\n",
                       b"HTTP/1.1 103 Early Hints\r\n Folded: x\r\n\r\n",
                       b"HTTP/1.1 100 Continue\n\n", b"HTTP/1.1 10 Continue\r\n\r\n"):
            with self.subTest(prefix=prefix):
                value = parse(INFO + prefix + EMPTY)
                self.assertIsInstance(value.status, semantics.Missing)
                self.assertIsInstance(value.headers, semantics.Missing)
                self.assertEqual(value.informational_ranges, (head.ByteRange(0, len(INFO)),))

    def test_status_like_suffix_absorbed_in_interim_value_is_not_final(self) -> None:
        prefix = b"HTTP/1.1 100 Continue\r\nX: partial"
        value = parse(INFO + prefix + EMPTY)
        # Appended bytes finish X's value and its head, not a final response.
        self.assertEqual(value.status, semantics.Missing("incomplete_status_line"))
        self.assertEqual(len(value.informational_ranges), 2)
        self.assertIsInstance(value.headers, semantics.Missing)

    def test_invalid_status_grammar_and_leading_junk(self) -> None:
        for line in (b"HTTP/2 200 OK", b"HTTP/1.2 200 OK", b"HTTP/1.1 20 OK",
                     b"HTTP/1.1 2000 OK", b"HTTP/1.1 099 Nope", b"HTTP/1.1 600 Nope",
                     b"HTTP/1.1 200", b"HTTP/1.1  200 OK", b"HTTP/1.1 200\tOK",
                     b"HTTP/1.1 200 O\x00K", b"HTTP/1.1 200 O\x7fK",
                     b"HTTP/1.1 200 O\nK", b"HTTP/1.1 200 O\rK", b"", b"junk"):
            with self.subTest(line=line):
                value = parse(line + b"\r\n\r\n" + EMPTY)
                self.assertEqual(value.status, semantics.Missing("invalid_status_line"))
                self.assertIsNone(value.status_range)

    def test_frozen_profile_allows_empty_and_latin1_reason(self) -> None:
        for version in (b"HTTP/1.0", b"HTTP/1.1"):
            for reason in (b"", b"\t\xff reason"):
                with self.subTest(version=version, reason=reason):
                    value = parse(version + b" 599 " + reason + b"\r\n\r\n")
                    self.assertEqual(value.status, 599)
                    self.assertEqual(value.headers, ())

    def test_interim_fields_never_mix_into_final_fields(self) -> None:
        value = parse(HINT + FINAL + b"Content-Type: application/json\r\n\r\n")
        self.assertEqual(value.headers, (("Content-Type", "application/json"),))
        self.assertEqual(len(value.header_ranges), 1)


class HttpHeadBoundsTests(unittest.TestCase):
    def test_header_cap_around_status_line(self) -> None:
        for bound, known in ((len(FINAL) - 1, False), (len(FINAL), True), (len(FINAL) + 1, True)):
            with self.subTest(bound=bound):
                value = parse(FINAL + b"X: value\r\n\r\n", header_limit_bytes=bound)
                self.assertEqual(type(value.status) is int, known)
                self.assertEqual(value.headers, semantics.Missing("header_limit"))
                self.assertEqual(value.examined_bytes, bound)

    def test_complete_head_exact_header_cap_and_one_byte_short(self) -> None:
        raw = FINAL + b"X: value\r\n\r\n"
        good = parse(raw, header_limit_bytes=len(raw))
        capped = parse(raw, header_limit_bytes=len(raw) - 1)
        self.assertEqual(good.headers, (("X", "value"),))
        self.assertEqual(capped.status, 409)
        self.assertEqual(capped.headers, semantics.Missing("header_limit"))
        # A body byte beyond the metadata cap cannot erase a complete head.
        self.assertEqual(parse(raw + b"a", header_limit_bytes=len(raw)).headers, good.headers)

    def test_header_byte_cap_is_cumulative_across_interims(self) -> None:
        raw = INFO + HINT + FINAL + b"\r\n"
        exact = parse(raw, header_limit_bytes=len(raw))
        short = parse(raw, header_limit_bytes=len(raw) - 1)
        status_short = parse(raw, header_limit_bytes=len(INFO + HINT + FINAL) - 1)
        self.assertEqual(exact.status, 409)
        self.assertEqual(exact.headers, ())
        self.assertEqual(short.status, 409)
        self.assertIsInstance(short.headers, semantics.Missing)
        self.assertIsInstance(status_short.status, semantics.Missing)
        self.assertEqual(len(status_short.informational_ranges), 2)

    def test_field_count_is_cumulative_and_status_independent(self) -> None:
        prefix = HINT + FINAL
        raw = prefix + b"One: x\r\nTwo: y\r\n\r\n"
        self.assertEqual(len(parse(raw, header_count_limit=3).headers), 2)
        capped = parse(raw, header_count_limit=2)
        self.assertEqual(capped.status, 409)
        self.assertEqual(capped.headers, semantics.Missing("header_count_limit"))
        self.assertEqual(capped.header_ranges, ())

    def test_exceeded_informational_count_blocks_later_final(self) -> None:
        self.assertEqual(parse(INFO * 2 + EMPTY, header_count_limit=2).status, 200)
        value = parse(INFO * 3 + EMPTY, header_count_limit=2)
        self.assertEqual(value.status, semantics.Missing("informational_limit"))
        self.assertEqual(len(value.informational_ranges), 2)

    def test_exceeded_interim_field_count_blocks_later_final(self) -> None:
        value = parse(HINT * 2 + EMPTY, header_count_limit=1)
        self.assertEqual(value.status, semantics.Missing("header_count_limit"))
        self.assertEqual(len(value.informational_ranges), 1)

    def test_response_sentinel_never_completes_status_or_headers(self) -> None:
        missing_status = parse(FINAL, response_limit_bytes=len(FINAL) - 1)
        self.assertEqual(missing_status.status, semantics.Missing("response_limit"))
        raw = FINAL + b"\r\n"
        missing_headers = parse(raw, response_limit_bytes=len(raw) - 1)
        self.assertEqual(missing_headers.status, 409)
        self.assertEqual(missing_headers.headers, semantics.Missing("response_limit"))
        exact = parse(raw, response_limit_bytes=len(raw))
        sentinel = parse(raw + b"x", response_limit_bytes=len(raw))
        self.assertEqual(exact.headers, ())
        self.assertEqual(sentinel.headers, ())
        self.assertEqual(sentinel.limitations, ("response_limit",))

    def test_response_capture_above_one_sentinel_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            parse(EMPTY + b"xx", response_limit_bytes=len(EMPTY))

    def test_empty_or_exact_cap_incomplete_input_does_not_invent_overrun(self) -> None:
        self.assertEqual(parse(b"").status, semantics.Missing("incomplete_status_line"))
        raw = FINAL + b"X: partial"
        value = parse(raw, header_limit_bytes=len(raw), response_limit_bytes=len(raw))
        self.assertEqual(value.headers, semantics.Missing("incomplete_headers"))
        self.assertEqual(value.limitations, ("incomplete_headers",))

    def test_raw_and_limits_types_are_strict(self) -> None:
        class Raw(bytes):
            pass
        class Limits(wire.WireLimits):
            pass
        for raw in ("", bytearray(), memoryview(b""), None, False, Raw(b"")):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                head.extract_response_head(raw, wire.WireLimits())
        for limits in (None, {}, False, Limits()):
            with self.subTest(limits=limits), self.assertRaises(ValueError):
                head.extract_response_head(b"", limits)
        corrupted = wire.WireLimits()
        object.__setattr__(corrupted, "header_limit_bytes", True)
        with self.assertRaises(ValueError):
            head.extract_response_head(b"", corrupted)

    def test_ranges_reject_wrong_types_or_impossible_bounds(self) -> None:
        for start, end in ((False, 1), (0, True), (-1, 1), (1, 1), (2, 1), (0, "2")):
            with self.subTest(start=start, end=end), self.assertRaises(ValueError):
                head.ByteRange(start, end)

    def test_output_is_immutable_and_constructor_checks_typed_structure(self) -> None:
        value = parse(EMPTY)
        with self.assertRaises(FrozenInstanceError):
            value.status = 409
        with self.assertRaises(FrozenInstanceError):
            value.status_range.start = 2
        changes = ({"status": True}, {"status": 199}, {"status": "200"}, {"headers": []},
                   {"headers": (("X", []),)}, {"headers": (), "header_ranges": []},
                   {"informational_ranges": []}, {"limitations": ["x"]}, {"limitations": ("x", "x")},
                   {"raw_length": True}, {"examined_bytes": len(EMPTY) + 1}, {"raw_sha256": "bad"},
                   {"status_range": None}, {"headers_range": None},
                   {"status_range": head.ByteRange(1, len(EMPTY) - 2)},
                   {"headers_range": head.ByteRange(len(EMPTY) - 3, len(EMPTY))})
        for change in changes:
            with self.subTest(change=change), self.assertRaises(ValueError):
                replace(value, **change)
