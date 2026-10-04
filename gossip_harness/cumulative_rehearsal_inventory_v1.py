"""Streaming verification of stopped child originals from an anchored manifest.

The shared cumulative ledger must not be byte-pinned per child: later registered
children append to it. It is authenticated separately by original inode and the
immutable per-child financial census. This module grants no acceptance authority.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import os
from pathlib import Path
import stat

from .peer_financial_terminal_v1 import require

MAX_FILES = 20_000
MAX_FILE_BYTES = 1_073_741_824
MAX_TOTAL_BYTES = 4_294_967_296
SUBDIRECTORIES = ('provider-journals', 'financial-payloads', 'seed-mesh', 'finance-mesh',
                  'roles', 'processes', 'private-git', 'protected.git')


@dataclass(frozen=True)
class InventoryAudit:
    root: str
    file_count: int
    total_bytes: int
    live_qualification: bool = field(default=False, init=False)


def _identity(info: os.stat_result) -> tuple[int, int, int, int, int, int]:
    return info.st_dev, info.st_ino, info.st_mode, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def _consume(path: Path, size: int, expected: str) -> None:
    before = path.lstat()
    require(stat.S_ISREG(before.st_mode) and before.st_size == size, 'Original is not expected regular file')
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        opened = os.fstat(descriptor)
        require(_identity(opened) == _identity(before), 'Original changed at open')
        h = hashlib.sha256()
        count = 0
        while True:
            chunk = os.read(descriptor, min(1_048_576, size - count + 1))
            if not chunk:
                break
            count += len(chunk)
            require(count <= size, 'Original exceeded declared bytes')
            h.update(chunk)
        require(count == size and h.hexdigest() == expected
                and _identity(os.fstat(descriptor)) == _identity(opened)
                and _identity(path.lstat()) == _identity(opened), 'Original content or identity changed')
    finally:
        os.close(descriptor)


def audit_child_inventory(root: Path, files: list[dict]) -> InventoryAudit:
    """Read exact canonical files; reject missing, unknown or indirect suffixes.

    All roots are host-owned and all writers must already be stopped. Stable
    no-follow consumption detects persistent replacement, not malicious-host ABA.
    """
    root = Path(root)
    require(root.is_absolute() and root.resolve() == root and root.is_dir(), 'Canonical original child root required')
    require(type(files) is list and 0 < len(files) <= MAX_FILES, 'Bounded complete original inventory required')
    expected: dict[str, dict] = {}
    total = 0
    for item in files:
        require(type(item) is dict and set(item) == {'path', 'sha256', 'size'}, 'Closed original file identity required')
        path = Path(item['path'])
        require(path.is_absolute() and path.resolve() == path and path.is_relative_to(root)
                and path.relative_to(root).parts[0] in SUBDIRECTORIES, 'Original path is outside registered roots')
        require(str(path) not in expected and type(item['size']) is int and 0 <= item['size'] <= MAX_FILE_BYTES
                and type(item['sha256']) is str and len(item['sha256']) == 64
                and all(c in '0123456789abcdef' for c in item['sha256']), 'Invalid or repeated original identity')
        total += item['size']
        require(total <= MAX_TOTAL_BYTES, 'Original total byte limit exceeded')
        expected[str(path)] = item
    seen: set[str] = set()
    directories = 0
    def visit(directory: Path) -> None:
        nonlocal directories
        directories += 1
        require(directories <= MAX_FILES, 'Original directory limit exceeded')
        info = directory.lstat()
        require(stat.S_ISDIR(info.st_mode) and directory.resolve() == directory, 'Indirect original directory')
        with os.scandir(directory) as entries:
            for entry in entries:
                path = directory / entry.name
                if entry.is_dir(follow_symlinks=False):
                    visit(path)
                    continue
                require(entry.is_file(follow_symlinks=False) and str(path) in expected and len(seen) < MAX_FILES,
                        'Unexpected or indirect original artifact')
                item = expected[str(path)]
                _consume(path, item['size'], item['sha256'])
                seen.add(str(path))
        after = directory.lstat()
        require((info.st_dev, info.st_ino, info.st_mtime_ns, info.st_ctime_ns)
                == (after.st_dev, after.st_ino, after.st_mtime_ns, after.st_ctime_ns),
                'Original directory changed during census')
    for name in SUBDIRECTORIES:
        path = root / name
        if path.exists() or path.is_symlink():
            visit(path)
    require(seen == set(expected), 'Original inventory contains missing files')
    return InventoryAudit(str(root), len(seen), total)
