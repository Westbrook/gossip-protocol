"""Independent retained Engine-control framing checks for physical test assertions.

This helper performs no I/O and imports no production transport decoder. It
recognizes the frozen bounded Engine control dialect, not arbitrary HTTP or
candidate responses. Bytes ending in a file never establish actual socket EOF.
A framed message can be complete while that separate observation is unknown.
"""
from __future__ import annotations

import re

HEADER_LIMIT = 32768
BODY_LIMIT = 1048576
CHUNK_COUNT_LIMIT = 65536
WIRE_LIMIT = HEADER_LIMIT + BODY_LIMIT + CHUNK_COUNT_LIMIT * 20


class ControlFrameError(ValueError):
    """Malformed or incomplete retained control frame; never a product verdict."""


def _require(condition, message):
    if not condition:
        raise ControlFrameError(message)


def decode_control_frame(raw: bytes, *, socket_eof_observed: bool | None = None) -> dict:
    """Decode one bounded control frame without inventing stream-completion facts.

    ``socket_eof_observed`` is supplied only from independently retained producer
    evidence. None means unavailable; False means it was not observed. Neither
    the framing parser nor captured-byte exhaustion changes that value. A
    close-delimited body therefore remains incomplete without explicit True.
    Framed-message completeness alone is not transport-operation completion.
    """
    _require(type(raw) is bytes and len(raw) <= WIRE_LIMIT, "Control wire type or bound differs")
    _require(socket_eof_observed is None or type(socket_eof_observed) is bool,
             "Socket EOF observation must be explicit boolean or unavailable")
    offset = 0

    def line(limit):
        nonlocal offset
        end = raw.find(b"\r\n", offset)
        _require(end >= offset and end + 2 - offset <= limit, "Incomplete or excessive control line")
        result = raw[offset:end]
        offset = end + 2
        return result

    first = line(HEADER_LIMIT)
    match = re.fullmatch(rb"HTTP/1\.[01] ([0-9]{3}) [\x20-\x7e\x80-\xff]*", first)
    _require(match is not None, "Malformed control status line")
    status = int(match[1])
    _require(200 <= status <= 599, "Interim or upgraded response is not a final control frame")
    headers = {}
    # The terminating blank line counts in the frozen 128-line loop.
    for _ in range(128):
        item = line(HEADER_LIMIT - offset)
        if not item:
            break
        _require(b":" in item and item[:1] not in (b" ", b"\t"), "Malformed control header")
        name, value = item.split(b":", 1)
        _require(re.fullmatch(rb"[!#$%&'*+.^_`|~0-9A-Za-z-]+", name) is not None,
                 "Malformed control header name")
        name = name.decode("ascii").lower()
        _require(name not in headers, "Duplicate control header")
        _require(all((byte >= 32 and byte != 127) or byte == 9 for byte in value),
                 "Control character in header value")
        headers[name] = value.decode("latin1").strip(" \t")
    else:
        raise ControlFrameError("Control header count exceeds bound")
    length, transfer = headers.get("content-length"), headers.get("transfer-encoding")
    _require(length is None or transfer is None, "Ambiguous Content-Length and Transfer-Encoding")
    complete = True
    if status in (204, 304):
        _require(transfer is None and length in (None, "0"), "Invalid no-body control framing")
        body, framing = b"", "no-body"
    elif transfer is not None:
        _require(transfer.lower() == "chunked", "Unsupported control transfer encoding")
        body = bytearray()
        for _ in range(CHUNK_COUNT_LIMIT):
            chunk = line(128)
            _require(re.fullmatch(rb"[0-9a-fA-F]{1,16}", chunk) is not None,
                     "Malformed control chunk size")
            size = int(chunk, 16)
            if size == 0:
                _require(line(2) == b"", "Control trailers are unsupported")
                break
            _require(len(body) + size <= BODY_LIMIT, "Control body exceeds bound")
            _require(offset + size + 2 <= len(raw), "Incomplete control chunk")
            body.extend(raw[offset:offset + size])
            offset += size
            _require(raw[offset:offset + 2] == b"\r\n", "Malformed control chunk terminator")
            offset += 2
        else:
            raise ControlFrameError("Control chunk count exceeds bound")
        body, framing = bytes(body), "chunked"
    elif length is not None:
        _require(re.fullmatch(r"[0-9]{1,12}", length) is not None, "Malformed control Content-Length")
        size = int(length)
        _require(size <= BODY_LIMIT, "Control body exceeds bound")
        _require(offset + size <= len(raw), "Incomplete Content-Length control body")
        body, framing = raw[offset:offset + size], "content-length"
        offset += size
    else:
        body, framing = raw[offset:], "connection-close"
        offset = len(raw)
        complete = socket_eof_observed is True
    _require(offset == len(raw), "Trailing bytes after control frame")
    _require(len(body) <= BODY_LIMIT, "Control body exceeds bound")
    return {"status": status, "headers": headers, "body": body, "framing": framing,
            "framing_complete": complete, "socket_eof_observed": socket_eof_observed}
