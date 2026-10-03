"""Physical authored-v2 host release/install controls, never candidate evidence."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness.library_m4_reference_v1 import m4_files
from gossip_harness.library_project_fixture_v1 import IMMUTABLE_PATHS, RUNTIME_IMAGE
from gossip_harness.library_v2_reference_v1 import v2_binary_files, v2_files, write_v2_project
from gossip_harness.library_v2_release_reference_v1 import (
    API_SOURCE_SHA256, PRODUCT_CONTRACT_SHA256, PUBLIC_FIXTURE_PATHS, release_files,
)


def _run(root, arguments, *, expected=0):
    environment = {key: value for key, value in os.environ.items()
                   if key not in ('PYTHONPATH', 'PYTHONHOME', 'OPENAI_API_KEY')}
    process = subprocess.run([sys.executable, '-B', *arguments], cwd=root,
                             env=environment, capture_output=True, timeout=40, check=False)
    if process.returncode != expected or len(process.stdout) + len(process.stderr) > 1048576:
        raise AssertionError(f'Authored v2 subprocess failed ({process.returncode}): '
                             + process.stderr.decode('utf-8', 'replace')[:6000])
    return json.loads(process.stdout if expected == 0 else process.stderr)


def _release(root, output, *, expected=0):
    return _run(root, ['-m', 'library.clients.release', '--output', str(output)], expected=expected)


def _manifest(root):
    raw = (root / 'release-manifest.json').read_bytes()
    manifest = json.loads(raw)
    def canonical(value):
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
    if raw != canonical(manifest):
        raise AssertionError('Noncanonical release manifest')
    rows = manifest['files']
    if [row['path'] for row in rows] != sorted({row['path'] for row in rows}):
        raise AssertionError('Unsorted or duplicate release entries')
    if hashlib.sha256(canonical(rows)).hexdigest() != manifest['source_sha256']:
        raise AssertionError('Wrong source inventory digest')
    for row in rows:
        target = root / row['path']
        data = target.read_bytes()
        if (set(row) != {'path', 'bytes', 'sha256'} or target.is_symlink()
                or len(data) != row['bytes'] or hashlib.sha256(data).hexdigest() != row['sha256']):
            raise AssertionError('Changed emitted package member: ' + row['path'])
    return manifest


class LibraryV2ReleaseReferenceTests(unittest.TestCase):
    def test_explicit_public_allowlist_and_current_normative_operator_docs(self):
        paths = sorted(set(m4_files()) | set(v2_binary_files()) | {'library/counters.py'})
        release = release_files(paths)
        compile(release['library/clients/release.py'], 'release.py', 'exec')
        self.assertEqual(hashlib.sha256(release['release/CUMULATIVE-CONTRACT.md'].encode()).hexdigest(), API_SOURCE_SHA256)
        self.assertEqual(hashlib.sha256(release['library-cumulative-product-v2.json'].encode()).hexdigest(), PRODUCT_CONTRACT_SHA256)
        for path in ('release/INSTALL.md', 'release/RECOVERY.md', 'release/USER-GUIDE.md'):
            self.assertIn('backup-root-adopt', release[path])
        self.assertIn("GET /health returns {'status':'ok','schema':4}", release['release/API.md'])
        self.assertIn('/api/jobs/ID/{prepare,commit,cancel,retry}', release['release/API.md'])
        self.assertIn('backup_root_unbound and backup_root_mismatch are409', release['release/API.md'])
        self.assertIn('counter_exhausted', release['release/RECOVERY.md'])
        self.assertIn('pending owned artifact', release['release/RECOVERY.md'])
        for name in ('library/not-declared.py', 'secret.json', '.env.local', 'release-manifest.json',
                     '../secret.py', 'library/catalog/../secret.py', 'private/tests.py'):
            with self.subTest(name=name), self.assertRaises(ValueError):
                release_files(paths + [name])
        with self.assertRaises(ValueError):
            release_files([path for path in paths if path != 'library/counters.py'])
        with self.assertRaises(ValueError):
            release_files(paths + [paths[0]])

    def test_deterministic_relocated_self_build_and_target_source_boundaries(self):
        with ArtifactDirectory('v2-release-determinism') as artifacts:
            base = artifacts.root.resolve()
            source = base / 'source'
            write_v2_project(source)
            (source / '.env.local').write_text('private sentinel')
            (source / 'user.sqlite3').write_text('private database sentinel')
            first, second, third = (base / name for name in ('first', 'second', 'third'))
            result = _release(source, first)
            self.assertEqual(_release(source, second), result)
            self.assertEqual(_release(first, third), result)
            manifest = _manifest(first)
            self.assertEqual(_manifest(second), manifest)
            self.assertEqual(_manifest(third), manifest)
            self.assertEqual(manifest['product_contract_sha256'], PRODUCT_CONTRACT_SHA256)
            self.assertEqual(manifest['runtime'], {'image': RUNTIME_IMAGE, 'python': '3.12'})
            self.assertEqual(manifest['storage_version'], 4)
            names = {row['path'] for row in manifest['files']}
            self.assertTrue(set(IMMUTABLE_PATHS) | set(PUBLIC_FIXTURE_PATHS) <= names)
            self.assertEqual(names, set(v2_files()) | set(v2_binary_files()))
            self.assertFalse(names & {'.env.local', 'user.sqlite3'})
            for name in names | {'release-manifest.json'}:
                self.assertEqual((first/name).read_bytes(), (second/name).read_bytes())
                self.assertEqual((first/name).read_bytes(), (third/name).read_bytes())
            before = (first / 'release-manifest.json').read_bytes()
            self.assertEqual(_release(source, first, expected=2), {'error': 'already_exists'})
            self.assertEqual((first / 'release-manifest.json').read_bytes(), before)
            asset = source / 'library/counters.py'
            content = asset.read_bytes()
            asset.unlink()
            self.assertEqual(_release(source, base/'missing', expected=2), {'error': 'invalid_source'})
            outside = base / 'counter-outside.py'
            outside.write_bytes(content)
            asset.symlink_to(outside)
            self.assertEqual(_release(source, base/'symlinked', expected=2), {'error': 'invalid_source'})
            self.assertFalse((base/'missing').exists())
            self.assertFalse((base/'symlinked').exists())
            self.assertEqual(list(base.glob('.library-release-*')), [])
            (base/'receipt.json').write_text(json.dumps({'purpose': 'authored_v2_release_controls',
                'executed': True, 'result': result, 'manifest': manifest, 'identical_builds': 3,
                'whole_product_acceptance': False}, indent=2) + '\n')

    def test_emitted_package_runs_public_migrations_and_real_workflow(self):
        with ArtifactDirectory('v2-release-public-checks') as artifacts:
            base = artifacts.root.resolve()
            source, installed = base/'source', base/'installed'
            write_v2_project(source)
            release = _release(source, installed)
            code = ('import importlib.util,json,sys,unittest\n'
                    'sys.path.insert(0,sys.argv[1])\n'
                    'spec=importlib.util.spec_from_file_location("v2_public",sys.argv[1]+"/test_cumulative_public.py")\n'
                    'module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)\n'
                    'result=unittest.TextTestRunner().run(unittest.defaultTestLoader.loadTestsFromModule(module))\n'
                    'print(json.dumps({"run":result.testsRun,"failures":len(result.failures),"errors":len(result.errors),"skipped":len(result.skipped)}))\n'
                    'raise SystemExit(0 if result.wasSuccessful() else 1)\n')
            result = _run(installed, ['-I', '-c', code, str(installed)])
            self.assertEqual(result, {'run': 3, 'failures': 0, 'errors': 0, 'skipped': 0})
            (base/'receipt.json').write_text(json.dumps({'purpose': 'authored_v2_installed_public_regression',
                'release': release, 'result': result, 'whole_product_acceptance': False}, indent=2) + '\n')

    def test_installed_cli_requires_adoption_then_preserves_backup_restore_workflow(self):
        with ArtifactDirectory('v2-release-installed-cli') as artifacts:
            base = artifacts.root.resolve()
            source, installed = base/'source', base/'installed'
            write_v2_project(source)
            release = _release(source, installed)
            inputs, backups = base/'inputs', base/'backups'
            inputs.mkdir(); backups.mkdir()
            (inputs/'welcome.txt').write_bytes((installed/'release/dataset/welcome.txt').read_bytes())
            prefix = ['-m', 'library', '--db', str(base/'library.sqlite3'), '--root', str(inputs),
                      '--backup-dir', str(backups)]
            commands = []
            def cli(*args, expected=0):
                value = _run(installed, prefix + list(args), expected=expected)
                commands.append({'arguments': args, 'exit': expected, 'result': value})
                return value
            identifier = cli('import', 'welcome.txt')['document']['document_id']
            self.assertEqual(cli('backups', expected=2), {'error': 'backup_root_unbound'})
            self.assertEqual(list(backups.iterdir()), [])
            self.assertEqual(cli('backup-root-adopt', '--expect-unbound'), {'adopted': True, 'registered': 0})
            self.assertEqual(cli('backup-root-adopt', '--expect-root', str(backups)), {'adopted': False, 'registered': 0})
            cli('annotate', identifier, '--expected-version', '1', '--notes', 'reviewed 雪', '--tag', 'demo')
            cli('refresh', identifier, '--expected-version', '2', '--text', 'Second café / 雪 revision')
            cli('backup', 'checkpoint.json')
            cli('delete', identifier, '--expected-version', '3')
            generation = cli('diagnostics')['generation']
            restored = cli('restore-backup', 'checkpoint.json', '--expected-generation', str(generation))
            self.assertEqual(restored['generation'], generation+1)
            document = cli('document-v1', identifier)
            self.assertFalse(document['deleted'])
            self.assertGreater(document['edit_version'], 4)
            self.assertEqual(document['current_revision']['revision'], 2)
            self.assertEqual(cli('search', '雪')['total'], 1)
            self.assertEqual(cli('migrate')['migrated'], False)
            self.assertEqual(cli('diagnostics')['worker_state'], 'stopped')
            (base/'receipt.json').write_text(json.dumps({'purpose': 'authored_v2_installed_cli_regression',
                'release': release, 'commands': commands, 'whole_product_acceptance': False}, indent=2, ensure_ascii=False)+'\n')
