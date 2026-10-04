"""Bounded trusted-journal bytes and JSON, without execution/provenance authority.

HTTP journals can exceed the separately frozen one-MiB Engine control dialect.
This helper does not parse candidate responses or relax any Engine decoder. Its
syntax, size and stable-file checks do not authenticate a retained checkpoint.
"""
from __future__ import annotations

from contextlib import ExitStack
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
from typing import Any

PROTOCOL = "candidate-http-journal-v3"
MAX_RECORD_BYTES = 32 * 1024 * 1024
MAX_DEPTH = 64
MAX_NODES = 1_000_000
_READ_CHUNK_BYTES = 64 * 1024
_STRING_SPECIAL = re.compile(r'["\\\x00-\x1f]')
_SURROGATE = re.compile(r'[\ud800-\udfff]')
_NUMBER = re.compile(r'-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?')


class JournalError(ValueError):
    """Unsafe, changed, oversized or malformed journal observation."""


def evaluator_sources() -> dict[str, str]:
    """Current source identity for the controller's separate authenticated bind."""
    return {"gossip_harness/candidate_http_journal_v3.py":
            hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}


def _bound(max_bytes: int) -> None:
    if type(max_bytes) is not int or not 0 < max_bytes <= MAX_RECORD_BYTES:
        raise JournalError("Invalid journal byte limit")


def _file_identity(value: os.stat_result) -> tuple[int, ...]:
    return (value.st_dev, value.st_ino, value.st_mode, value.st_nlink,
            value.st_size, value.st_mtime_ns, value.st_ctime_ns)


def _directory_identity(value: os.stat_result) -> tuple[int, int, int]:
    return value.st_dev, value.st_ino, value.st_mode


def read(path: Path | str, *, max_bytes: int = MAX_RECORD_BYTES) -> bytes:
    """Read one stable regular file without following any path-component link.

    Opens directories through anchored descriptors and checks their path entries
    again after reading. Size and file identity/timestamps must agree before
    open, on the descriptor, after reading and at the final directory entry.
    These are bounded observation guards, not an adversarial filesystem snapshot
    or proof of who authored the bytes. The owner must authenticate their digest.
    """
    _bound(max_bytes)
    if not isinstance(path, (str, Path)):
        raise JournalError("Journal path must be a string or Path")
    candidate = Path(path)
    if ".." in candidate.parts or "\0" in str(candidate):
        raise JournalError("Unsafe journal path")
    candidate = candidate.absolute()
    parts = candidate.parts[1:]
    if not parts or candidate.anchor != "/":
        raise JournalError("Journal path must name a local POSIX file")
    if not all(hasattr(os, flag) for flag in ("O_NOFOLLOW", "O_DIRECTORY", "O_NONBLOCK")):
        raise JournalError("Required no-follow descriptor support unavailable")
    flags = os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    try:
        with ExitStack() as stack:
            parent = os.open("/", flags | os.O_DIRECTORY)
            stack.callback(os.close, parent)
            ancestors: list[tuple[int, str, tuple[int, int, int]]] = []
            for name in parts[:-1]:
                entry = os.stat(name, dir_fd=parent, follow_symlinks=False)
                if not stat.S_ISDIR(entry.st_mode):
                    raise JournalError("Non-directory or linked journal ancestor")
                child = os.open(name, flags | os.O_DIRECTORY, dir_fd=parent)
                stack.callback(os.close, child)
                opened = os.fstat(child)
                directory_identity = _directory_identity(entry)
                if not stat.S_ISDIR(opened.st_mode) or _directory_identity(opened) != directory_identity:
                    raise JournalError("Journal ancestor changed during open")
                ancestors.append((parent, name, directory_identity))
                parent = child
            name = parts[-1]
            entry = os.stat(name, dir_fd=parent, follow_symlinks=False)
            if not stat.S_ISREG(entry.st_mode) or not 0 <= entry.st_size <= max_bytes:
                raise JournalError("Journal is not a bounded regular file")
            descriptor = os.open(name, flags | os.O_NONBLOCK, dir_fd=parent)
            stack.callback(os.close, descriptor)
            opened = os.fstat(descriptor)
            identity = _file_identity(entry)
            if not stat.S_ISREG(opened.st_mode) or _file_identity(opened) != identity:
                raise JournalError("Journal changed during open")
            chunks: list[bytes] = []
            count = 0
            while count <= opened.st_size:
                chunk = os.read(descriptor, min(_READ_CHUNK_BYTES, opened.st_size + 1 - count))
                if not chunk:
                    break
                chunks.append(chunk)
                count += len(chunk)
            if count != opened.st_size or _file_identity(os.fstat(descriptor)) != identity:
                raise JournalError("Journal content/size changed during read")
            current = os.stat(name, dir_fd=parent, follow_symlinks=False)
            if not stat.S_ISREG(current.st_mode) or _file_identity(current) != identity:
                raise JournalError("Journal path changed during read")
            for directory, component, expected in ancestors:
                current = os.stat(component, dir_fd=directory, follow_symlinks=False)
                if not stat.S_ISDIR(current.st_mode) or _directory_identity(current) != expected:
                    raise JournalError("Journal ancestor changed during read")
            return b"".join(chunks)
    except (OSError, ValueError, OverflowError) as error:
        if isinstance(error, JournalError):
            raise
        raise JournalError("Unable to read a stable no-follow journal") from error


def _budget(text: str) -> None:
    """Count structural depth and lexical nodes before allocating decoded JSON.

    Every container, object member name and scalar value counts once. JSON's
    decoder still owns grammar; this scan never relaxes malformed input. String
    contents are skipped as one node, respecting escapes rather than counting
    embedded braces, commas or apparent JSON values.
    """
    position = 0
    nodes = 0
    closing: list[str] = []
    while position < len(text):
        token = text[position]
        if token in " \t\r\n:,":
            position += 1
            continue
        if token in "}]":
            if not closing or closing.pop() != token:
                raise JournalError("Unbalanced journal JSON containers")
            position += 1
            continue
        nodes += 1
        if nodes > MAX_NODES:
            raise JournalError("Journal JSON node bound")
        if token in "[{":
            closing.append("]" if token == "[" else "}")
            if len(closing) > MAX_DEPTH:
                raise JournalError("Journal JSON nesting bound")
            position += 1
        elif token == '"':
            position += 1
            while True:
                found = _STRING_SPECIAL.search(text, position)
                if found is None:
                    raise JournalError("Unterminated journal JSON string")
                position = found.end()
                if found.group() == '"':
                    break
                if found.group() == "\\":
                    position += 1
                else:
                    raise JournalError("Control character in journal JSON string")
        elif token in "-0123456789":
            found = _NUMBER.match(text, position)
            if found is None:
                raise JournalError("Invalid journal JSON number")
            position = found.end()
        else:
            for literal in ("true", "false", "null"):
                if text.startswith(literal, position):
                    position += len(literal)
                    break
            else:
                raise JournalError("Invalid journal JSON token")
    if closing:
        raise JournalError("Unclosed journal JSON container")


def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise JournalError("Duplicate journal JSON key")
        result[key] = value
    return result


def _constant(value: str) -> Any:
    raise JournalError("Nonfinite journal JSON number")


def _float(value: str) -> float:
    result = float(value)
    if not math.isfinite(result):
        raise JournalError("Nonfinite journal JSON number")
    return result


def _unicode_scalars(value: Any) -> None:
    pending = [value]
    while pending:
        item = pending.pop()
        if type(item) is str and _SURROGATE.search(item):
            raise JournalError("Non-scalar Unicode in journal JSON")
        if type(item) is list:
            pending.extend(item)
        elif type(item) is dict:
            pending.extend(item.keys())
            pending.extend(item.values())


def decode(raw: bytes, *, max_bytes: int = MAX_RECORD_BYTES) -> Any:
    """Decode strict UTF-8 journal JSON; legal whitespace is allowed here.

    This trusted-journal dialect rejects duplicate keys, nonfinite/overflowing
    numbers, BOMs, UTF-16/32 and unpaired surrogate escapes. It is not the parser
    or semantic policy for candidate product JSON. The caller owns provenance.
    """
    _bound(max_bytes)
    if type(raw) is not bytes or len(raw) > max_bytes:
        raise JournalError("Journal JSON requires bounded immutable bytes")
    try:
        text = raw.decode("utf-8", errors="strict")
        _budget(text)
        value = json.loads(text, object_pairs_hook=_object, parse_constant=_constant, parse_float=_float)
        _unicode_scalars(value)
        return value
    except (ValueError, UnicodeError, RecursionError, OverflowError) as error:
        if isinstance(error, JournalError):
            raise
        raise JournalError("Invalid bounded strict journal JSON") from error


def read_json(path: Path | str, *, max_bytes: int = MAX_RECORD_BYTES) -> Any:
    """Read the exact canonical UTF-8 dialect emitted by finite.encoded.

    Sorted object keys, compact separators, unescaped Unicode and finite numbers
    are framing rules for authored journals. Matching them is not authentication.
    """
    raw = read(path, max_bytes=max_bytes)
    value = decode(raw, max_bytes=max_bytes)
    try:
        canonical = json.dumps(value, sort_keys=True, separators=(",", ":"),
                               ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError, UnicodeError, RecursionError, OverflowError) as error:
        raise JournalError("Journal JSON is not canonically encodable") from error
    if canonical != raw:
        raise JournalError("Journal JSON is not canonical")
    return value
