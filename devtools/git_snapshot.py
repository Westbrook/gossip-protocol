"""Read an exact Git text tree with two subprocesses and verified blob framing.

This development/export optimization deliberately leaves the scientific
GitStore implementation unchanged. It reads object bytes only: no checkout,
filters, candidate imports, hooks, or candidate execution.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import re
import subprocess
from typing import Sequence

from gossip_harness.gitstore import GitError


_SHA = re.compile(r'(?:[0-9a-f]{40}|[0-9a-f]{64})\Z')
_HEADER = re.compile(rb'([0-9a-f]{40}|[0-9a-f]{64}) blob (0|[1-9][0-9]*)\Z')


def _git_bytes(path: Path, args: Sequence[str], *, input_data: bytes | None = None) -> bytes:
    # Match the scientific reader's local, config-isolated trust boundary.
    # cat-file reads raw immutable objects; it does not apply working-tree
    # filters, attributes, or textconv without explicit enabling options.
    env = {key: value for key, value in os.environ.items() if not key.startswith('GIT_')}
    env.update(GIT_CONFIG_NOSYSTEM='1', GIT_CONFIG_GLOBAL=os.devnull,
               GIT_TERMINAL_PROMPT='0', GIT_ALLOW_PROTOCOL='file',
               GIT_NO_REPLACE_OBJECTS='1', GIT_OPTIONAL_LOCKS='0', LC_ALL='C')
    command = ['git', '--literal-pathspecs', '-c', f'core.hooksPath={os.devnull}',
               '-c', 'core.fsmonitor=false', '-c', 'protocol.allow=never',
               '-c', 'protocol.file.allow=always', '-C', str(path), *args]
    result = subprocess.run(command, input=input_data, env=env, capture_output=True, timeout=60)
    if result.returncode:
        detail = result.stderr.decode('utf-8', errors='replace').strip()
        raise GitError(f'git {args[0]} failed ({result.returncode}): {detail}')
    return result.stdout


def _tree_entries(raw: bytes, hash_length: int) -> dict[str, str]:
    if raw and not raw.endswith(b'\x00'):
        raise ValueError('Truncated Git tree record')
    result: dict[str, str] = {}
    for record in raw.split(b'\x00')[:-1]:
        metadata, separator, filename = record.partition(b'\t')
        fields = metadata.split(b' ')
        if not separator or len(fields) != 3 or not filename:
            raise ValueError('Malformed Git tree record')
        mode, kind, raw_sha = fields
        sha = raw_sha.decode('ascii')
        if kind != b'blob' or mode not in (b'100644', b'100755', b'120000'):
            raise ValueError('Only regular, executable, and symlink blobs can be exported')
        if _SHA.fullmatch(sha) is None or len(sha) != hash_length:
            raise ValueError('Invalid Git tree object hash')
        name = filename.decode('utf-8')
        # Paths are data here, and may include tabs/newlines. NUL framing and
        # the first tab separate metadata safely; export path policy is applied
        # later by the transactional writer for the target filesystem.
        if name in result or any(part in ('', '.', '..') for part in name.split('/')):
            raise ValueError('Invalid or duplicate Git tree path')
        result[name] = sha
    return result


def _blob_batch(raw: bytes, object_ids: Sequence[str]) -> dict[str, bytes]:
    cursor = 0
    blobs: dict[str, bytes] = {}
    for expected in object_ids:
        newline = raw.find(b'\n', cursor)
        if newline < 0:
            raise ValueError('Truncated Git batch header')
        match = _HEADER.fullmatch(raw[cursor:newline])
        if match is None or match[1].decode('ascii') != expected:
            raise ValueError('Git batch object hash or type differs from request')
        size = int(match[2])
        start, stop = newline + 1, newline + 1 + size
        if stop >= len(raw) or raw[stop:stop + 1] != b'\n':
            raise ValueError('Truncated Git batch payload or invalid declared size')
        contents = raw[start:stop]
        algorithm = 'sha1' if len(expected) == 40 else 'sha256'
        actual = hashlib.new(algorithm, b'blob ' + str(size).encode('ascii') + b'\x00' + contents).hexdigest()
        if actual != expected:
            raise ValueError('Git blob bytes differ from their object hash')
        blobs[expected] = contents
        cursor = stop + 1
    if cursor != len(raw):
        raise ValueError('Unexpected trailing Git batch data')
    return blobs


def read_text_tree(path: Path, commit: str) -> dict[str, str]:
    """Read regular/executable/symlink text blobs at an exact full commit SHA.

    Symlinks retain their target text, matching GitStore.read_files; they are
    never followed. Gitlinks and non-UTF-8 contents remain unsupported. Unique
    blob IDs are fetched once, using size-delimited payloads rather than lines.
    """
    if _SHA.fullmatch(commit) is None:
        raise ValueError('An exact full Git commit SHA is required')
    entries = _tree_entries(_git_bytes(Path(path), ('ls-tree', '-r', '-z', f'{commit}^{{commit}}')), len(commit))
    object_ids = sorted(set(entries.values()))
    requests = b''.join(sha.encode('ascii') + b'\n' for sha in object_ids)
    blobs = _blob_batch(_git_bytes(Path(path), ('cat-file', '--batch'), input_data=requests), object_ids)
    return {name: blobs[entries[name]].decode('utf-8') for name in sorted(entries)}
