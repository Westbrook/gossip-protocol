"""Bounded pure response-head facts, with no journal or physical authority.

This version preserves status and complete header syntax independently of body
framing. It does not repair wire-v1, change helper stopping, or parse a body.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
from pathlib import Path
import re

from gossip_harness import candidate_http_semantics_v1 as semantics
from gossip_harness import candidate_http_transport_v1 as wire

PROTOCOL = "candidate-http-head-v1"
# Capture once: a later on-disk edit must not relabel this imported implementation.
# This detects source drift; it supplies no execution or acceptance authority.
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
_TOKEN = re.compile(rb"[!#$%&'*+.^_`|~0-9A-Za-z-]+")
_STATUS = re.compile(rb"HTTP/1\.[01] ([0-9]{3}) ([\t\x20-\x7e\x80-\xff]*)")
_SHA = re.compile(r"[0-9a-f]{64}")


def _require(condition: bool, reason: str) -> None:
    if not condition:
        raise ValueError(reason)


def _missing(value: object) -> bool:
    return type(value) is semantics.Missing and type(value.reason) is str and bool(value.reason)


@dataclass(frozen=True)
class ByteRange:
    """Zero-based, half-open offsets into the exact supplied raw byte string."""

    start: int
    end: int

    def __post_init__(self) -> None:
        _require(type(self.start) is int and type(self.end) is int
                 and 0 <= self.start < self.end, "nonempty integer byte range required")


@dataclass(frozen=True)
class HeadFacts:
    """Pure syntax observations, never authenticated evidence by themselves.

    Status/header field line ranges include their CRLF. ``headers_range`` starts
    after the status line and includes the final empty CRLF; an empty field set
    therefore has a two-byte range. No partial field set is exposed.
    ``examined_bytes`` is the length of the bounded parser-visible prefix, not
    a body-consumption count or proof that all of that prefix was interpreted.
    """

    status: int | semantics.Missing
    headers: tuple[tuple[str, str], ...] | semantics.Missing
    status_range: ByteRange | None
    headers_range: ByteRange | None
    header_ranges: tuple[ByteRange, ...]
    informational_ranges: tuple[ByteRange, ...]
    raw_sha256: str
    raw_length: int
    examined_bytes: int
    limitations: tuple[str, ...]

    def __post_init__(self) -> None:
        _require(type(self.raw_sha256) is str and _SHA.fullmatch(self.raw_sha256) is not None,
                 "raw SHA256 required")
        _require(type(self.raw_length) is int and type(self.examined_bytes) is int
                 and 0 <= self.examined_bytes <= self.raw_length, "bounded raw lengths required")
        _require(type(self.limitations) is tuple
                 and all(type(x) is str and x for x in self.limitations)
                 and len(set(self.limitations)) == len(self.limitations), "immutable unique limitations required")
        _require(type(self.header_ranges) is tuple and type(self.informational_ranges) is tuple,
                 "immutable range tuples required")
        ranges = self.header_ranges + self.informational_ranges
        ranges += tuple(x for x in (self.status_range, self.headers_range) if x is not None)
        _require(all(type(x) is ByteRange and 0 <= x.start < x.end <= self.examined_bytes
                     for x in ranges), "ranges must name supplied bounded bytes")
        known_status = type(self.status) is int and 200 <= self.status <= 599
        _require(known_status or _missing(self.status), "final status or Missing required")
        _require(known_status == (self.status_range is not None), "status range must match availability")
        known_headers = type(self.headers) is tuple
        _require(known_headers or _missing(self.headers), "immutable header pairs or Missing required")
        if known_headers:
            _require(known_status and self.headers_range is not None, "headers require final status and range")
            _require(type(self.headers) is tuple and len(self.headers) == len(self.header_ranges),
                     "one range per field required")
            assert isinstance(self.headers, tuple)
            for pair in self.headers:
                _require(type(pair) is tuple and len(pair) == 2
                         and all(type(x) is str for x in pair), "immutable string field pairs required")
                name, value = pair
                _require(name.isascii() and _TOKEN.fullmatch(name.encode("ascii")) is not None
                         and all(c == "\t" or 32 <= ord(c) <= 255 and ord(c) != 127 for c in value)
                         and value == value.strip(" \t"), "normalized strict field syntax required")
        else:
            _require(self.headers_range is None and not self.header_ranges, "missing fields cannot carry ranges")
        cursor = 0
        for span in self.informational_ranges:
            _require(span.start == cursor, "informational spans must be a contiguous prefix")
            cursor = span.end
        if self.status_range is not None:
            _require(self.status_range.start == cursor, "final status must follow admitted informational prefix")
        if self.headers_range is not None:
            _require(self.status_range is not None and self.headers_range.start == self.status_range.end,
                     "field section must follow final status")
            cursor = self.headers_range.start
            for span in self.header_ranges:
                _require(span.start == cursor, "field spans must be contiguous")
                cursor = span.end
            _require(cursor + 2 == self.headers_range.end, "field section must include exactly its empty CRLF")


def extract_response_head(raw: bytes, limits: wire.WireLimits) -> HeadFacts:
    """Extract strict final metadata without inferring framing or authenticity.

    Raw capture may contain wire-v1's one response-limit sentinel byte. The
    sentinel never supplies parsing syntax. Header byte/count and informational
    response limits are cumulative across the contiguous response-head prefix.
    A malformed/incomplete informational response stops traversal permanently.
    """
    _require(type(raw) is bytes, "immutable raw bytes required")
    _require(type(limits) is wire.WireLimits, "exact WireLimits required")
    # Revalidate even an externally corrupted frozen dataclass; no duck types.
    wire.WireLimits(**asdict(limits))
    _require(len(raw) <= limits.response_limit_bytes + 1, "raw response capture bound exceeded")
    visible_end = min(len(raw), limits.response_limit_bytes, limits.header_limit_bytes)
    visible = raw[:visible_end]
    raw_sha256 = hashlib.sha256(raw).hexdigest()
    informational: list[ByteRange] = []
    status: int | semantics.Missing = semantics.Missing("incomplete_status_line")
    status_range: ByteRange | None = None
    field_count = 0
    position = 0

    def unavailable_reason(default: str) -> str:
        if len(raw) > visible_end:
            return ("response_limit" if limits.response_limit_bytes <= limits.header_limit_bytes
                    else "header_limit")
        return default

    def result(reason: str | None, fields: tuple[tuple[str, str], ...] | None = None,
               field_spans: tuple[ByteRange, ...] = (), section: ByteRange | None = None) -> HeadFacts:
        limitations: list[str] = [] if reason is None else [reason]
        if len(raw) > limits.response_limit_bytes and "response_limit" not in limitations:
            limitations.append("response_limit")
        final_status = status if type(status) is int else semantics.Missing(reason or "incomplete_status_line")
        headers = semantics.Missing(reason or "incomplete_headers") if fields is None else fields
        return HeadFacts(final_status, headers, status_range, section, field_spans,
                         tuple(informational), raw_sha256, len(raw), visible_end, tuple(limitations))

    while True:
        head_start = position
        line_end = visible.find(b"\r\n", position)
        if line_end < 0:
            return result(unavailable_reason("incomplete_status_line"))
        match = _STATUS.fullmatch(visible[position:line_end])
        if match is None or not 100 <= int(match[1]) <= 599:
            return result("invalid_status_line")
        code = int(match[1])
        position = line_end + 2
        if code >= 200:
            status = code
            status_range = ByteRange(head_start, position)
        section_start = position
        fields: list[tuple[str, str]] = []
        field_spans: list[ByteRange] = []
        while True:
            line_end = visible.find(b"\r\n", position)
            if line_end < 0:
                return result(unavailable_reason("incomplete_headers"))
            line = visible[position:line_end]
            if not line:
                position = line_end + 2
                break
            if b":" not in line:
                return result("invalid_header")
            name, value = line.split(b":", 1)
            if (_TOKEN.fullmatch(name) is None
                    or any(c != 9 and (c < 32 or c == 127) for c in value)):
                return result("invalid_header")
            field_count += 1
            if field_count > limits.header_count_limit:
                return result("header_count_limit")
            fields.append((name.decode("ascii"), value.decode("latin1").strip(" \t")))
            field_spans.append(ByteRange(position, line_end + 2))
            position = line_end + 2
        if code >= 200:
            # Metadata grammar is independent of CL/TE/body framing semantics.
            return result(None, tuple(fields), tuple(field_spans), ByteRange(section_start, position))
        if code == 101:
            return result("unsupported_upgrade")
        if any(name.lower() in {"content-length", "transfer-encoding"} for name, _ in fields):
            return result("informational_framing")
        if len(informational) >= limits.header_count_limit:
            return result("informational_limit")
        informational.append(ByteRange(head_start, position))
