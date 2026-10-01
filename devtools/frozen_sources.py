"""Materialize verified archived core sources as data, without executing them.

This is a preparation boundary for historical audits, not permission to run old
candidate code. Run trusted, current audit tooling separately. Only files named
by the recorded contract are copied; candidate repositories and large run trees
are deliberately excluded.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import tempfile
from typing import Any, Sequence

from retain_experiments import InputSnapshot, _component, _publish_directory, _validate_export_paths, _write_export


def _digest(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, ensure_ascii=True, allow_nan=False,
                         separators=(',', ':')).encode('utf-8')
    return hashlib.sha256(encoded).hexdigest()


def materialize(run: Path, output: Path) -> dict[str, Any]:
    """Verify the recorded core source bytes and publish only those bytes."""
    run, output = Path(run), Path(output).absolute()
    if run.is_symlink() or not run.is_dir():
        raise ValueError('Archived run must be a real directory')
    run = run.resolve()
    if output.exists() or output.is_symlink():
        raise FileExistsError('Frozen-source output must be fresh')
    output = output.parent.resolve() / output.name
    if output.is_relative_to(run):
        raise ValueError('Frozen-source output must be outside the archived run')
    snapshot = InputSnapshot()
    result = json.loads(snapshot.read(run / 'results.json', run))
    contract = result.get('contract')
    if not isinstance(contract, dict) or not isinstance(contract.get('sources'), dict) or not contract['sources']:
        raise ValueError('Run has no recorded core-source contract')
    contract_sha = _digest(contract)
    if 'contract_sha' in result and result['contract_sha'] != contract_sha:
        raise ValueError('Recorded contract digest does not match its contents')
    for name in ('manifest.json', 'preregistered.json'):
        path = run / name
        if path.exists() or path.is_symlink():
            document = json.loads(snapshot.read(path, run))
            if 'contract' in document:
                if document['contract'] != contract:
                    raise ValueError('Archived manifest and results contracts differ')
            elif name == 'manifest.json' and document.get('sources') != contract['sources']:
                raise ValueError('Archived manifest and results source contracts differ')
    files: dict[str, bytes] = {}
    hashes: dict[str, str] = {}
    for name, expected in sorted(contract['sources'].items()):
        if not isinstance(name, str) or _component(name) != name or not name.endswith('.py'):
            raise ValueError('Core-source contract requires Python file names without directories')
        if not isinstance(expected, str) or re.fullmatch(r'[0-9a-f]{64}', expected) is None:
            raise ValueError('Core-source contract has an invalid SHA-256 digest')
        relative = f'gossip_harness/{name}'
        contents = snapshot.read(run / 'source-snapshot' / relative, run)
        if hashlib.sha256(contents).hexdigest() != expected:
            raise ValueError(f'Archived core source differs from its contract: {name}')
        files[relative] = contents
        hashes[relative] = expected
    manifest = dict(schema_version=1, purpose='verified-frozen-core-sources',
                    contract=contract, contract_sha256=contract_sha, source_sha256=hashes,
                    executed=False, complete_runtime=False,
                    protocol=contract.get('protocol', result.get('experiment')),
                    input_sha256={path.relative_to(run).as_posix(): hashlib.sha256(contents).hexdigest()
                                  for path, (_, contents) in snapshot.files.items()},
                    note='Prepare-only: these are the exact recorded core bytes, not a complete runtime or execution approval. Missing __init__.py, audit scripts, fixtures, and dependencies are not supplied. Use current trusted audit tooling separately and preserve the bound original protocol for any historical audit.')
    files['replay-manifest.json'] = (json.dumps(manifest, indent=2, sort_keys=True) + '\n').encode('utf-8')
    _validate_export_paths(files)
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f'.{output.name}.staging-', dir=output.parent))
    try:
        _write_export(staging, files)
        snapshot.verify()
        _publish_directory(staging, output)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    return manifest


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    print(json.dumps(materialize(args.run, args.output), indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
