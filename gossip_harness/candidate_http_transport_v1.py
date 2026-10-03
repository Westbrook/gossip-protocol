"""Trusted, standalone HTTP socket observation; no product scoring or Docker calls.

The controller copies this exact file into its isolated pinned probe container.
Only that controller can authenticate a transcript. Parsing a dictionary here
does not establish source, server epoch, process completion or sandbox identity.
"""
from __future__ import annotations

import base64
from dataclasses import asdict, dataclass, replace
import errno as errno_module
import hashlib
import json
import math
from pathlib import Path
import re
import socket
import sys
import time
from typing import Any, Callable, Protocol

PROTOCOL = "candidate-http-transport-v1"
INPUT_PROTOCOL = "candidate-http-probe-input-v1"
INPUT_PATH = "/probe/request.json"
MAX_INPUT_BYTES = 2 * 1024 * 1024
MAX_OUTPUT_BYTES = 32 * 1024 * 1024
_TOKEN = rb"[!#$%&'*+.^_`|~0-9A-Za-z-]+"
_METHODS = frozenset({"GET", "POST", "HEAD", "PUT", "DELETE", "PATCH", "OPTIONS"})


class WireError(ValueError):
    """Invalid evaluator input or unauthenticated/malformed transport evidence."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise WireError(message)


def encoded(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
                      allow_nan=False).encode("ascii")


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _b64(raw: bytes) -> str:
    return base64.b64encode(raw).decode("ascii")


def _unb64(value: Any, bound: int) -> bytes:
    _require(type(value) is str and len(value) <= 4 * ((bound + 2) // 3), "base64 bound/type")
    try:
        raw = base64.b64decode(value, validate=True)
    except (ValueError, UnicodeError) as error:
        raise WireError("invalid base64") from error
    _require(len(raw) <= bound and _b64(raw) == value, "noncanonical/bounded base64 required")
    return raw


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for name, value in pairs:
        _require(name not in result, "duplicate control JSON key")
        result[name] = value
    return result


def _constant(value: str) -> Any:
    raise WireError("nonfinite control JSON")


def _json(raw: bytes, bound: int) -> Any:
    _require(type(raw) is bytes and len(raw) <= bound, "control JSON byte bound/type")
    try:
        return json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs, parse_constant=_constant)
    except (ValueError, UnicodeError, RecursionError) as error:
        raise WireError("invalid strict control JSON") from error


@dataclass(frozen=True)
class WireLimits:
    request_limit_bytes: int = 131072
    response_limit_bytes: int = 4194304
    header_limit_bytes: int = 65536
    header_count_limit: int = 256
    chunk_count_limit: int = 65536
    io_operation_limit: int = 65536
    listener_limit_bytes: int = 262144
    timeout_seconds: float = 10.0

    def __post_init__(self) -> None:
        for name, upper in (("request_limit_bytes", 1048576), ("response_limit_bytes", 16777216),
                            ("header_limit_bytes", 1048576), ("header_count_limit", 65536),
                            ("chunk_count_limit", 1048576), ("io_operation_limit", 1048576),
                            ("listener_limit_bytes", 1048576)):
            value = getattr(self, name)
            _require(type(value) is int and 1 <= value <= upper, "invalid " + name)
        _require(type(self.timeout_seconds) in (int, float)
                 and math.isfinite(self.timeout_seconds) and 0 < self.timeout_seconds <= 120,
                 "invalid total socket deadline")


def _limits(value: Any) -> WireLimits:
    _require(type(value) is dict and set(value) == set(asdict(WireLimits())), "limits fields")
    return WireLimits(**value)


def _port(port: Any) -> int:
    _require(type(port) is int and 1 <= port <= 65535, "invalid loopback port")
    return port


def request_bytes(recipe: Any, port: int, limits: WireLimits | None = None) -> bytes:
    """Serialize literal HTTP/1.1 bytes without adding, sorting or rewriting fields.

    The closed core profile requires explicit Host and Connection, and
    exact Content-Length for POST/nonempty bodies. This is a request recipe
    constraint, not a claim that other legal HTTP request dialects are defective.
    """
    limits = limits or WireLimits()
    _port(port)
    _require(type(recipe) is dict and set(recipe) == {"method", "target", "headers", "body_b64"},
             "request recipe fields")
    method, target, headers = recipe["method"], recipe["target"], recipe["headers"]
    _require(type(method) is str and method in _METHODS, "unsupported literal method")
    _require(type(target) is str and target.startswith("/") and not target.startswith("//")
             and len(target) <= limits.request_limit_bytes
             and all(33 <= ord(c) <= 126 and c not in "#\\" for c in target),
             "ASCII origin-form target required")
    _require(type(headers) is list and len(headers) <= limits.header_count_limit,
             "ordered header-pair list required")
    lines: list[bytes] = []
    normalized: dict[str, list[str]] = {}
    for pair in headers:
        _require(type(pair) is list and len(pair) == 2 and all(type(x) is str for x in pair),
                 "literal header pair required")
        name, value = pair
        _require(len(name) + len(value) <= limits.header_limit_bytes, "request header bound")
        _require(re.fullmatch(_TOKEN.decode("ascii"), name) is not None
                 and all(c == "\t" or 32 <= ord(c) <= 126 for c in value), "unsafe request header")
        normalized.setdefault(name.lower(), []).append(value.strip(" \t"))
        lines.append((name + ": " + value).encode("ascii"))
    _require(normalized.get("host") == ["127.0.0.1:" + str(port)], "exact loopback Host required")
    _require(len(normalized.get("connection", [])) == 1
             and normalized["connection"][0].lower() in {"close", "keep-alive"},
             "one explicit Connection: close or keep-alive required")
    _require(not any(name in normalized for name in ("transfer-encoding", "trailer", "upgrade", "expect")),
             "request transfer/upgrade/expect profile unsupported")
    body = _unb64(recipe["body_b64"], limits.request_limit_bytes)
    length = normalized.get("content-length", [])
    _require(length == [str(len(body))] or (not length and not body and method != "POST"),
             "exact single request Content-Length required")
    head = method.encode("ascii") + b" " + target.encode("ascii") + b" HTTP/1.1\r\n" + b"\r\n".join(lines) + b"\r\n\r\n"
    _require(len(head) <= limits.header_limit_bytes, "request header bound")
    raw = head + body
    _require(len(raw) <= limits.request_limit_bytes, "request byte bound")
    return raw


def build_probe_input(recipe: Any, port: int, limits: WireLimits | None = None) -> bytes:
    limits = limits or WireLimits()
    raw = request_bytes(recipe, port, limits)
    result = encoded({"protocol": INPUT_PROTOCOL, "port": port, "recipe": recipe,
                      "limits": asdict(limits), "request_sha256": _sha(raw)})
    _require(len(result) <= MAX_INPUT_BYTES, "probe input bound")
    return result


def helper_source() -> bytes:
    """Copy exact standalone source; no candidate or repository imports occur."""
    return Path(__file__).read_bytes()


def helper_sha256() -> str:
    return _sha(helper_source())


def max_probe_output_bytes(limits: WireLimits | None = None) -> int:
    """Bound the JSON envelope, including four raw kernel tables and one sentinel.

    Counts/hashes/flags are fixed-size metadata; no parsed response body is
    duplicated in helper output. Executors must admit this bound before launch.
    """
    limits = limits or WireLimits()
    return (8192 + 4 * ((limits.request_limit_bytes + 2) // 3)
            + 4 * ((limits.response_limit_bytes + 3) // 3)
            + 16 * ((limits.listener_limit_bytes + 3) // 3))


@dataclass(frozen=True)
class Response:
    status_code: int | None = None
    status_line: bytes = b""
    raw_headers: bytes = b""
    headers: tuple[tuple[str, str], ...] = ()
    informational: tuple[bytes, ...] = ()
    trailers: tuple[tuple[str, str], ...] = ()
    body: bytes = b""
    framing: str | None = None
    headers_complete: bool = False
    framing_complete: bool = False
    body_complete: bool = False
    consumed_bytes: int = 0
    limitation: str | None = None


class _Stop(Exception):
    pass


def _fields(lines: list[bytes]) -> tuple[tuple[str, str], ...]:
    fields: list[tuple[str, str]] = []
    for line in lines:
        if b":" not in line:
            raise _Stop("invalid_header")
        name, value = line.split(b":", 1)
        if (re.fullmatch(_TOKEN, name) is None
                or any(c != 9 and (c < 32 or c == 127) for c in value)):
            raise _Stop("invalid_header")
        fields.append((name.decode("ascii"), value.decode("latin1").strip(" \t")))
    return tuple(fields)


def _values(fields: tuple[tuple[str, str], ...], name: str) -> list[str]:
    return [value for key, value in fields if key.lower() == name]


def _content_length(fields: tuple[tuple[str, str], ...]) -> int | None:
    tokens = [part.strip(" \t") for value in _values(fields, "content-length") for part in value.split(",")]
    if not tokens:
        return None
    if any(re.fullmatch(r"[0-9]+", token) is None for token in tokens):
        raise _Stop("invalid_content_length")
    # RFC 9112 6.3 permits identical repeated/list values. Compare normalized
    # decimals without converting arbitrarily large adversarial integers.
    canonical = [token.lstrip("0") or "0" for token in tokens]
    if len(set(canonical)) != 1:
        raise _Stop("conflicting_content_length")
    if len(canonical[0]) > 20:
        raise _Stop("declared_body_limit")
    return int(canonical[0])


_EXTENSION = re.compile(rb"(?:[ \t]*;[ \t]*" + _TOKEN + rb"(?:[ \t]*=[ \t]*(?:" + _TOKEN
                        + rb'|"(?:[\t !#-\[\]-~\x80-\xff]|\\[\t -~\x80-\xff])*"))?)*[ \t]*\Z')


def parse_response(raw: bytes, method: str, limits: WireLimits | None = None, *, eof: bool = False) -> Response:
    """Parse bounded HTTP framing only; product JSON remains uninterpreted bytes.

    Complete length/chunk/no-body framing does not require peer EOF. Excess
    bytes already captured invalidate the boundary. This finite observation
    makes no claim about bytes a peer might send after the probe closes.
    """
    limits = limits or WireLimits()
    _require(type(raw) is bytes and len(raw) <= limits.response_limit_bytes + 1,
             "raw response capture bound/type")
    _require(type(method) is str and method in _METHODS and type(eof) is bool, "parse context")
    values: dict[str, Any] = {}
    position = 0
    header_bytes = 0
    header_count = 0
    informational: list[bytes] = []
    try:
        while True:
            end = raw.find(b"\r\n\r\n", position)
            if end < 0:
                if header_bytes + len(raw) - position > limits.header_limit_bytes:
                    raise _Stop("header_limit")
                raise _Stop("incomplete_headers")
            end += 4
            block = raw[position:end]
            header_bytes += len(block)
            if header_bytes > limits.header_limit_bytes:
                raise _Stop("header_limit")
            lines = block[:-4].split(b"\r\n")
            match = re.fullmatch(rb"HTTP/1\.[01] ([0-9]{3}) ([\t\x20-\x7e\x80-\xff]*)", lines[0])
            if match is None or not 100 <= int(match[1]) <= 599:
                raise _Stop("invalid_status_line")
            status = int(match[1])
            if not 100 <= status < 200:
                # A syntactically complete final status is observable even if
                # unrelated headers/framing later prove unusable.
                values.update(status_code=status, status_line=lines[0], raw_headers=block)
            fields = _fields(lines[1:])
            header_count += len(fields)
            if header_count > limits.header_count_limit:
                raise _Stop("header_count_limit")
            length = _content_length(fields)
            transfer = _values(fields, "transfer-encoding")
            if transfer and length is not None:
                raise _Stop("conflicting_transfer_length")
            if 100 <= status < 200:
                if status == 101:
                    raise _Stop("unsupported_upgrade")
                if transfer or length is not None:
                    raise _Stop("informational_framing")
                informational.append(block)
                if len(informational) > limits.header_count_limit:
                    raise _Stop("informational_limit")
                values["informational"] = tuple(informational)
                position = end
                continue
            values.update(headers=fields, headers_complete=True)
            position = end
            break
        if method == "HEAD" or status in (204, 304):
            if status == 204 and (transfer or length is not None):
                raise _Stop("bodyless_framing")
            values.update(framing="no-body", body=b"", consumed_bytes=position,
                          framing_complete=True, body_complete=True)
        elif transfer:
            codings = [p.strip(" \t").lower() for value in transfer for p in value.split(",")]
            if codings != ["chunked"]:
                raise _Stop("unsupported_or_ambiguous_transfer_coding")
            values["framing"] = "chunked"
            parts: list[bytes] = []
            for _ in range(limits.chunk_count_limit):
                line_end = raw.find(b"\r\n", position)
                if line_end < 0:
                    if header_bytes + len(raw) - position > limits.header_limit_bytes:
                        raise _Stop("chunk_metadata_limit")
                    raise _Stop("incomplete_chunk_size")
                line = raw[position:line_end]
                header_bytes += len(line) + 2
                if header_bytes > limits.header_limit_bytes:
                    raise _Stop("chunk_metadata_limit")
                size_match = re.match(rb"[0-9A-Fa-f]+", line)
                if size_match is None or _EXTENSION.fullmatch(line[size_match.end():]) is None:
                    raise _Stop("invalid_chunk_size_or_extension")
                digits = size_match[0].lstrip(b"0") or b"0"
                if len(digits) > 16:
                    raise _Stop("declared_body_limit")
                size = int(digits, 16)
                if size > limits.response_limit_bytes:
                    raise _Stop("declared_body_limit")
                position = line_end + 2
                if size == 0:
                    trailer_lines: list[bytes] = []
                    while True:
                        trailer_end = raw.find(b"\r\n", position)
                        if trailer_end < 0:
                            if header_bytes + len(raw) - position > limits.header_limit_bytes:
                                raise _Stop("trailer_limit")
                            raise _Stop("incomplete_trailers")
                        trailer = raw[position:trailer_end]
                        header_bytes += len(trailer) + 2
                        if header_bytes > limits.header_limit_bytes:
                            raise _Stop("trailer_limit")
                        position = trailer_end + 2
                        if not trailer:
                            break
                        trailer_lines.append(trailer)
                        if header_count + len(trailer_lines) > limits.header_count_limit:
                            raise _Stop("header_count_limit")
                    trailers = _fields(trailer_lines)
                    if any(name.lower() in {"content-length", "transfer-encoding", "host", "connection",
                                            "trailer", "content-type", "content-encoding", "content-range"}
                           for name, _ in trailers):
                        raise _Stop("prohibited_trailer")
                    values.update(trailers=trailers, body=b"".join(parts), consumed_bytes=position,
                                  framing_complete=True, body_complete=True)
                    break
                available = raw[position:position + size]
                parts.append(available)
                if len(available) != size or len(raw) < position + size + 2:
                    values["body"] = b"".join(parts)
                    raise _Stop("incomplete_chunk_data")
                if raw[position + size:position + size + 2] != b"\r\n":
                    values["body"] = b"".join(parts)
                    raise _Stop("invalid_chunk_terminator")
                position += size + 2
            else:
                raise _Stop("chunk_count_limit")
        elif length is not None:
            values.update(framing="content-length", body=raw[position:position + length])
            if length > limits.response_limit_bytes:
                raise _Stop("declared_body_limit")
            if len(raw) - position < length:
                raise _Stop("incomplete_content_length")
            position += length
            values.update(consumed_bytes=position, framing_complete=True, body_complete=True)
        else:
            values.update(framing="close-delimited", body=raw[position:])
            if not eof:
                raise _Stop("awaiting_close")
            position = len(raw)
            values.update(consumed_bytes=position, framing_complete=True, body_complete=True)
        if values["consumed_bytes"] != len(raw):
            values.update(framing_complete=False, body_complete=False)
            raise _Stop("excess_response_bytes")
        if len(raw) > limits.response_limit_bytes:
            values.update(framing_complete=False, body_complete=False)
            raise _Stop("response_limit")
    except _Stop as error:
        values["limitation"] = str(error)
    return Response(**values)


def _record(raw: bytes) -> dict[str, Any]:
    return {"b64": _b64(raw), "bytes": len(raw), "sha256": _sha(raw)}


def _record_bytes(value: Any, bound: int) -> bytes:
    _require(type(value) is dict and set(value) == {"b64", "bytes", "sha256"}, "byte record fields")
    raw = _unb64(value["b64"], bound)
    _require(type(value["bytes"]) is int and value["bytes"] == len(raw)
             and type(value["sha256"]) is str and value["sha256"] == _sha(raw), "byte record integrity")
    return raw


def _snapshot(limits: WireLimits) -> dict[str, Any]:
    tables: dict[str, Any] = {}
    for name in ("tcp", "tcp6"):
        try:
            with open("/proc/net/" + name, "rb") as stream:
                raw = stream.read(limits.listener_limit_bytes + 1)
            tables[name] = {"status": "limit" if len(raw) > limits.listener_limit_bytes else "ok",
                            "raw": _record(raw), "errno": None}
        except OSError as error:
            tables[name] = {"status": "unavailable", "raw": _record(b""), "errno": error.errno}
    return {"byteorder": sys.byteorder, "tables": tables}


@dataclass(frozen=True)
class ListenerSnapshot:
    tcp: bytes
    tcp6: bytes
    listeners: tuple[tuple[str, str, int], ...]
    complete: bool
    limitations: tuple[str, ...]


def decode_listener_snapshot(value: Any, port: int, limits: WireLimits | None = None) -> ListenerSnapshot:
    limits = limits or WireLimits()
    _port(port)
    _require(type(value) is dict and set(value) == {"byteorder", "tables"}
             and value["byteorder"] in ("little", "big") and type(value["tables"]) is dict
             and set(value["tables"]) == {"tcp", "tcp6"}, "listener snapshot fields")
    raws: list[bytes] = []
    listeners: list[tuple[str, str, int]] = []
    limitations: list[str] = []
    for name in ("tcp", "tcp6"):
        table = value["tables"][name]
        _require(type(table) is dict and set(table) == {"status", "raw", "errno"}, "listener table fields")
        raw = _record_bytes(table["raw"], limits.listener_limit_bytes + 1)
        raws.append(raw)
        state = table["status"]
        _require(state in ("ok", "limit", "unavailable") and (table["errno"] is None
                 or type(table["errno"]) is int), "listener status")
        _require((state == "ok" and len(raw) <= limits.listener_limit_bytes and table["errno"] is None)
                 or (state == "limit" and len(raw) == limits.listener_limit_bytes + 1 and table["errno"] is None)
                 or (state == "unavailable" and raw == b""), "listener state contradiction")
        if state != "ok":
            limitations.append(name + ":" + state)
            continue
        try:
            lines = raw.decode("ascii").splitlines()
            if not lines or not all(token in lines[0].split() for token in ("sl", "local_address", "st", "inode")):
                raise ValueError("kernel header")
            for line in lines[1:]:
                fields = line.split()
                if len(fields) < 10 or re.fullmatch(r"[0-9]+:", fields[0]) is None:
                    raise ValueError("kernel row")
                local = re.fullmatch(r"([0-9A-Fa-f]{8}|[0-9A-Fa-f]{32}):([0-9A-Fa-f]{4})", fields[1])
                if local is None or re.fullmatch(r"[0-9A-Fa-f]{2}", fields[3]) is None:
                    raise ValueError("kernel address/state")
                expected = 8 if name == "tcp" else 32
                if len(local[1]) != expected:
                    raise ValueError("kernel address family")
                if fields[3].upper() == "0A" and int(local[2], 16) == port:
                    address = b"".join(int(local[1][i:i + 8], 16).to_bytes(4, value["byteorder"])
                                       for i in range(0, expected, 8))
                    family = socket.AF_INET if name == "tcp" else socket.AF_INET6
                    listeners.append((name, socket.inet_ntop(family, address), port))
        except (ValueError, UnicodeError, OSError):
            limitations.append(name + ":invalid_kernel_table")
    return ListenerSnapshot(raws[0], raws[1], tuple(listeners), not limitations, tuple(limitations))


class _Socket(Protocol):
    def settimeout(self, value: float) -> None: ...
    def connect(self, address: tuple[str, int]) -> None: ...
    def send(self, data: bytes) -> int: ...
    def recv(self, size: int) -> bytes: ...
    def close(self) -> None: ...


def _run_probe(input_raw: bytes, *, socket_factory: Callable[..., _Socket] = socket.socket,
               clock: Callable[[], float] = time.monotonic,
               sleep: Callable[[float], None] = time.sleep,
               snapshot: Callable[[WireLimits], dict[str, Any]] = _snapshot) -> dict[str, Any]:
    """Real helper path. Injection points exist only for offline fake-I/O checks."""
    payload = _json(input_raw, MAX_INPUT_BYTES)
    _require(type(payload) is dict and set(payload) == {"protocol", "port", "recipe", "limits", "request_sha256"}
             and payload["protocol"] == INPUT_PROTOCOL, "probe input fields")
    limits = _limits(payload["limits"])
    port = _port(payload["port"])
    request = request_bytes(payload["recipe"], port, limits)
    _require(payload["request_sha256"] == _sha(request), "request binding")
    _require(input_raw == build_probe_input(payload["recipe"], port, limits), "canonical probe input required")
    before: dict[str, Any] | None = None
    sent = 0
    response = bytearray()
    connected = eof = False
    termination = "connect_error"
    errno: int | None = None
    started = clock()
    deadline = started + limits.timeout_seconds
    operations = 0
    connect_attempts = 0
    connect_refused_attempts = 0
    connection: _Socket | None = None

    def timeout() -> None:
        nonlocal operations
        operations += 1
        if operations > limits.io_operation_limit:
            raise _Stop("io_operation_limit")
        remaining = deadline - clock()
        if remaining <= 0:
            raise TimeoutError()
        assert connection is not None
        connection.settimeout(remaining)

    try:
        while True:
            connection = socket_factory(socket.AF_INET, socket.SOCK_STREAM)
            timeout()
            connect_attempts += 1
            try:
                connection.connect(("127.0.0.1", port))
                break
            except ConnectionRefusedError as error:
                if error.errno != errno_module.ECONNREFUSED:
                    raise
                connect_refused_attempts += 1
                connection.close()
                connection = None
                remaining = deadline - clock()
                if remaining <= 0:
                    raise TimeoutError()
                # Readiness retries occur only before connection and before any
                # request bytes. The product operation itself is never retried.
                sleep(min(0.01, remaining))
        connected = True
        before = snapshot(limits)
        termination = "send_error"
        while sent < len(request):
            timeout()
            count = connection.send(request[sent:])
            if type(count) is not int or not 0 < count <= len(request) - sent:
                raise OSError("invalid socket send result")
            sent += count
        termination = "receive_error"
        while True:
            timeout()
            part = connection.recv(min(65536, limits.response_limit_bytes + 1 - len(response)))
            if not part:
                eof = True
                termination = "eof"
                break
            response.extend(part)
            if len(response) > limits.response_limit_bytes:
                termination = "response_limit"
                break
            parsed = parse_response(bytes(response), payload["recipe"]["method"], limits)
            if parsed.framing_complete:
                termination = "message_complete"
                break
            if parsed.limitation not in {"incomplete_headers", "incomplete_chunk_size", "incomplete_chunk_data",
                                         "incomplete_trailers", "incomplete_content_length", "awaiting_close"}:
                termination = "parse_stopped"
                break
    except TimeoutError:
        termination = "timeout"
    except _Stop as error:
        termination = str(error)
    except OSError as error:
        errno = error.errno
    finally:
        if connection is not None:
            connection.close()
    elapsed = max(0.0, clock() - started)
    return {"protocol": PROTOCOL, "input_sha256": _sha(input_raw), "request_sha256": _sha(request),
            "sent": _record(request[:sent]), "received": _record(bytes(response)),
            "connected": connected, "socket_eof": eof, "termination": termination,
            "errno": errno, "elapsed_seconds": elapsed, "io_operations": operations,
            "connect_attempts": connect_attempts,
            "connect_refused_attempts": connect_refused_attempts,
            "listeners_before_stage": "connected_pre_request" if before is not None else "connection_failed_diagnostic",
            "listeners_before": before if before is not None else snapshot(limits),
            "listeners_after": snapshot(limits)}


@dataclass(frozen=True)
class WireObservation:
    request: bytes
    sent: bytes
    received: bytes
    response: Response
    sent_complete: bool
    socket_eof: bool
    termination: str
    exchange_complete: bool
    limitations: tuple[str, ...]
    connected: bool
    listeners_before_stage: str
    listeners_before: ListenerSnapshot
    listeners_after: ListenerSnapshot


def decode_probe_output(raw: bytes, recipe: Any, port: int, limits: WireLimits | None = None) -> WireObservation:
    """Authenticate internal consistency only; caller authenticates helper execution."""
    limits = limits or WireLimits()
    control = _json(raw, min(MAX_OUTPUT_BYTES, max_probe_output_bytes(limits)))
    fields = {"protocol", "input_sha256", "request_sha256", "sent", "received", "connected", "socket_eof",
              "termination", "errno", "elapsed_seconds", "io_operations", "connect_attempts",
              "connect_refused_attempts", "listeners_before_stage", "listeners_before", "listeners_after"}
    _require(type(control) is dict and set(control) == fields and control["protocol"] == PROTOCOL,
             "probe output fields")
    request = request_bytes(recipe, port, limits)
    _require(control["input_sha256"] == _sha(build_probe_input(recipe, port, limits))
             and control["request_sha256"] == _sha(request), "probe request/input binding")
    sent = _record_bytes(control["sent"], len(request))
    received = _record_bytes(control["received"], limits.response_limit_bytes + 1)
    _require(request.startswith(sent), "undeclared sent bytes")
    _require(type(control["connected"]) is bool and type(control["socket_eof"]) is bool
             and (control["errno"] is None or type(control["errno"]) is int), "socket flags/types")
    elapsed, operations = control["elapsed_seconds"], control["io_operations"]
    _require(type(elapsed) in (int, float) and math.isfinite(elapsed) and elapsed >= 0
             and type(operations) is int and 0 <= operations <= limits.io_operation_limit + 1,
             "probe runtime counters")
    attempts = control["connect_attempts"]
    refused = control["connect_refused_attempts"]
    _require(type(attempts) is int and 0 <= attempts <= operations
             and (not control["connected"] or attempts >= 1), "connect attempt count")
    _require(type(refused) is int and 0 <= refused <= attempts <= refused + 1
             and (not control["connected"] or attempts == refused + 1), "connect refusal census")
    termination = control["termination"]
    _require(type(termination) is str and termination in {"connect_error", "send_error", "receive_error",
             "timeout", "io_operation_limit", "response_limit", "parse_stopped", "eof", "message_complete"},
             "probe termination")
    eof = control["socket_eof"]
    sent_complete = sent == request
    _require((not sent and not received and not eof) or control["connected"], "unconnected socket bytes")
    _require(not received or sent_complete, "response before complete request")
    _require(eof == (termination == "eof") and (not eof or sent_complete), "EOF contradiction")
    _require(termination != "connect_error" or (not control["connected"] and not sent and not received),
             "connect failure contradiction")
    _require(termination != "send_error" or (control["connected"] and not sent_complete and not received),
             "send failure contradiction")
    if termination in {"receive_error", "response_limit", "parse_stopped", "message_complete"}:
        _require(control["connected"] and sent_complete, "receive phase contradiction")
    if termination not in {"connect_error", "send_error", "receive_error"}:
        _require(control["errno"] is None, "unexpected socket errno")
    _require((len(received) > limits.response_limit_bytes) == (termination == "response_limit"),
             "capture limit contradiction")
    _require((operations > limits.io_operation_limit) == (termination == "io_operation_limit"),
             "operation limit contradiction")
    parsed = parse_response(received, recipe["method"], limits, eof=eof)
    _require((termination == "message_complete") == (parsed.framing_complete and not eof),
             "framing termination contradiction")
    if termination == "parse_stopped":
        _require(parsed.limitation is not None and parsed.limitation not in {
            "incomplete_headers", "incomplete_chunk_size", "incomplete_chunk_data", "incomplete_trailers",
            "incomplete_content_length", "awaiting_close"}, "parser termination contradiction")
    stage = "connected_pre_request" if control["connected"] else "connection_failed_diagnostic"
    _require(control["listeners_before_stage"] == stage, "listener snapshot stage mismatch")
    before = decode_listener_snapshot(control["listeners_before"], port, limits)
    if not control["connected"]:
        before = replace(before, complete=False, limitations=before.limitations + ("connection_not_established",))
    limitations = []
    if not sent_complete:
        limitations.append("request_incomplete")
    if parsed.limitation:
        limitations.append(parsed.limitation)
    if termination not in {"eof", "message_complete"}:
        limitations.append(termination)
    return WireObservation(request, sent, received, parsed, sent_complete, eof, termination,
                           sent_complete and parsed.framing_complete and not limitations, tuple(limitations),
                           control["connected"], stage, before,
                           decode_listener_snapshot(control["listeners_after"], port, limits))


def _main() -> int:
    try:
        _require(sys.argv[1:] == [INPUT_PATH], "fixed probe input path required")
        with open(INPUT_PATH, "rb") as stream:
            raw = stream.read(MAX_INPUT_BYTES + 1)
        result = encoded(_run_probe(raw))
        limits = _limits(_json(raw, MAX_INPUT_BYTES)["limits"])
        _require(len(result) + 1 <= min(MAX_OUTPUT_BYTES, max_probe_output_bytes(limits)), "helper output bound")
        sys.stdout.buffer.write(result + b"\n")
        return 0
    except (WireError, OSError):
        sys.stderr.write("trusted HTTP probe input/execution unavailable\n")
        return 2


if __name__ == "__main__":
    raise SystemExit(_main())
