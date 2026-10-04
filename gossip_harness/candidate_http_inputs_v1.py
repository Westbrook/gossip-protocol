"""Confined host staging for literal HTTP data fixtures; no candidate execution.

Every path is traversed through real directory descriptors without following
links. Only the explicit data fixture leaves may be symlinks. Verification is a
bounded snapshot of trusted owned staging, not an atomic hostile-host proof.
"""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import os
from pathlib import Path
import stat
from typing import Any, Iterator

from .candidate_http_cases_core_v1 import Fixture

PROTOCOL = "candidate-http-inputs-v1"
MAX_ENTRIES = 1024
MAX_BYTES = 8 * 1024 * 1024
FILE_MODE = 0o444
DIRECTORY_MODE = 0o755
_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
_FILE_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK


class InputError(ValueError):
    """Invalid declaration or changed staging; never a candidate verdict."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise InputError(message)


def validate(entries: tuple[Fixture, ...]) -> None:
    """Validate data-only confinement, independently of a process recipe."""
    _require(type(entries) is tuple and len(entries) <= MAX_ENTRIES
             and all(type(entry) is Fixture for entry in entries), "Bounded immutable fixture tuple required")
    try:
        for entry in entries:
            # Revalidate exact frozen values without relying on constructor history.
            Fixture(entry.path, entry.kind, entry.data, entry.target)
    except (AttributeError, TypeError, ValueError, UnicodeError) as error:
        raise InputError("Invalid fixture value") from error
    _require(sum(len(entry.data) for entry in entries) <= MAX_BYTES, "Fixture byte budget exceeded")
    paths = {entry.path: entry for entry in entries}
    _require(len(paths) == len(entries), "Repeated fixture path")
    for entry in entries:
        parts = entry.path.split("/")
        for end in range(1, len(parts)):
            parent = "/".join(parts[:end])
            _require(parent in paths and paths[parent].kind == "directory", "Explicit real parent required")
        if entry.kind != "symlink":
            continue
        resolved = parts[:-1]
        target_parts = entry.target.split("/")
        for index, part in enumerate(target_parts):
            if part in ("", "."):
                continue
            if part == "..":
                _require(bool(resolved), "Link escapes owned input root")
                resolved.pop()
                continue
            resolved.append(part)
            current = "/".join(resolved)
            _require(current in paths and paths[current].kind != "symlink", "Link traverses missing or linked fixture")
            if index < len(target_parts) - 1:
                _require(paths[current].kind == "directory", "Link ancestor is not a real directory")
        target = "/".join(resolved)
        _require(target in paths and paths[target].kind in ("file", "directory"), "Link target must be a real owned fixture")


def manifest(entries: tuple[Fixture, ...]) -> dict[str, Any]:
    """Ordered declaration identity, omitting payload bytes and host paths."""
    validate(entries)
    return {"protocol": PROTOCOL, "entries": [
        {"path": entry.path, "kind": entry.kind,
         "bytes": len(entry.data) if entry.kind == "file" else None,
         "sha256": hashlib.sha256(entry.data).hexdigest() if entry.kind == "file" else None,
         "target": entry.target if entry.kind == "symlink" else None} for entry in entries],
        "entry_count": len(entries), "file_bytes": sum(len(entry.data) for entry in entries)}


@contextmanager
def _directory(parent: int, parts: tuple[str, ...]) -> Iterator[int]:
    descriptor = os.dup(parent)
    try:
        for part in parts:
            child = os.open(part, _DIRECTORY_FLAGS, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        yield descriptor
    finally:
        os.close(descriptor)


def _open_root(root: Path) -> int:
    _require(isinstance(root, Path) and root.is_absolute() and root.anchor == "/"
             and all(part not in ("", ".", "..") for part in root.parts[1:]),
             "Canonical absolute owned root required")
    descriptor = os.open("/", _DIRECTORY_FLAGS)
    try:
        for part in root.parts[1:]:
            child = os.open(part, _DIRECTORY_FLAGS, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


@contextmanager
def _root(root: Path) -> Iterator[int]:
    descriptor = -1
    try:
        descriptor = _open_root(root)
        before = os.fstat(descriptor)
        yield descriptor
        current = _open_root(root)
        try:
            after = os.fstat(current)
            _require((before.st_dev, before.st_ino) == (after.st_dev, after.st_ino), "Owned root identity changed")
        finally:
            os.close(current)
    except OSError as error:
        raise InputError("Missing, unsafe or inaccessible staging path: " + type(error).__name__) from error
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _identity(value: os.stat_result) -> tuple[int, ...]:
    return (value.st_dev, value.st_ino, value.st_mode, value.st_nlink, value.st_size,
            value.st_mtime_ns, value.st_ctime_ns)


def _names(descriptor: int, limit: int) -> set[str]:
    names: set[str] = set()
    with os.scandir(descriptor) as rows:
        for row in rows:
            names.add(row.name)
            _require(len(names) <= limit, "Unregistered staging entry")
    return names


def stage(root: Path, entries: tuple[Fixture, ...]) -> None:
    """Populate an existing empty real root; retain partial output on failure."""
    validate(entries)
    with _root(root) as descriptor:
        _require(not _names(descriptor, 0), "Staging root must be empty")
        directories = sorted((entry for entry in entries if entry.kind == "directory"),
                             key=lambda entry: (entry.path.count("/"), entry.path))
        for entry in directories:
            parts = entry.path.split("/")
            with _directory(descriptor, tuple(parts[:-1])) as parent:
                os.mkdir(parts[-1], DIRECTORY_MODE, dir_fd=parent)
                with _directory(parent, (parts[-1],)) as created:
                    os.fchmod(created, DIRECTORY_MODE)
        for entry in entries:
            if entry.kind != "file":
                continue
            parts = entry.path.split("/")
            with _directory(descriptor, tuple(parts[:-1])) as parent:
                output = os.open(parts[-1], os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                                 0o600, dir_fd=parent)
                try:
                    offset = 0
                    while offset < len(entry.data):
                        written = os.write(output, entry.data[offset:offset + 65536])
                        _require(written > 0, "Staging write made no progress")
                        offset += written
                    os.fchmod(output, FILE_MODE)
                finally:
                    os.close(output)
        for entry in entries:
            if entry.kind != "symlink":
                continue
            parts = entry.path.split("/")
            with _directory(descriptor, tuple(parts[:-1])) as parent:
                os.symlink(entry.target, parts[-1], dir_fd=parent)
    verify(root, entries)


def _file(parent: int, name: str, entry: Fixture, before: os.stat_result) -> None:
    _require(stat.S_ISREG(before.st_mode) and stat.S_IMODE(before.st_mode) == FILE_MODE
             and before.st_nlink == 1 and before.st_size == len(entry.data), "Regular file metadata differs")
    descriptor = os.open(name, _FILE_FLAGS, dir_fd=parent)
    try:
        _require(_identity(os.fstat(descriptor)) == _identity(before), "File changed before read")
        chunks: list[bytes] = []
        remaining = len(entry.data) + 1
        while remaining:
            chunk = os.read(descriptor, min(65536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        after = os.fstat(descriptor)
        named = os.stat(name, dir_fd=parent, follow_symlinks=False)
        _require(_identity(before) == _identity(after) == _identity(named), "File identity changed during read")
        _require(b"".join(chunks) == entry.data, "Staged file bytes differ")
    finally:
        os.close(descriptor)


def verify(root: Path, entries: tuple[Fixture, ...]) -> None:
    """Check exact no-follow tree snapshots against the immutable declaration."""
    validate(entries)
    children: dict[str, dict[str, Fixture]] = {"": {}}
    for entry in entries:
        parent_path, _, name = entry.path.rpartition("/")
        children.setdefault(parent_path, {})[name] = entry
        if entry.kind == "directory":
            children.setdefault(entry.path, {})
    with _root(root) as descriptor:
        for relative, expected in sorted(children.items()):
            parts = tuple(relative.split("/")) if relative else ()
            with _directory(descriptor, parts) as parent:
                before_directory = os.fstat(parent)
                if relative:
                    _require(stat.S_IMODE(before_directory.st_mode) == DIRECTORY_MODE, "Directory mode differs")
                _require(_names(parent, len(expected)) == set(expected), "Staged inventory differs")
                for name, entry in expected.items():
                    before = os.stat(name, dir_fd=parent, follow_symlinks=False)
                    if entry.kind == "directory":
                        _require(stat.S_ISDIR(before.st_mode) and stat.S_IMODE(before.st_mode) == DIRECTORY_MODE,
                                 "Declared directory changed type or mode")
                    elif entry.kind == "symlink":
                        _require(stat.S_ISLNK(before.st_mode), "Declared link changed type")
                        _require(os.readlink(name, dir_fd=parent) == entry.target, "Literal link target changed")
                        _require(_identity(os.stat(name, dir_fd=parent, follow_symlinks=False)) == _identity(before),
                                 "Link changed during verification")
                    else:
                        _file(parent, name, entry, before)
                _require(_identity(os.fstat(parent)) == _identity(before_directory), "Directory changed during verification")
