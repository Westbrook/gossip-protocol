"""Physical release/install checks of fixed authored sources, not candidates."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness.library_m3_reference_v1 import m3_files
from gossip_harness.library_m4_release_reference_v1 import (
    PRODUCT_CONTRACT_SHA256, PUBLIC_FIXTURE_PATHS, release_files,
)
from gossip_harness.library_project_fixture_v1 import IMMUTABLE_PATHS, RUNTIME_IMAGE


def _environment():
    return {key: value for key, value in os.environ.items()
            if key not in ('PYTHONPATH', 'PYTHONHOME', 'OPENAI_API_KEY')}


def _run(root, arguments, *, expected=0):
    result = subprocess.run([sys.executable, '-B', *arguments], cwd=root,
        env=_environment(), capture_output=True, timeout=30, check=False)
    if result.returncode != expected or len(result.stdout) + len(result.stderr) > 1048576:
        raise AssertionError(f'Authored release subprocess failed ({result.returncode}): '
                             + result.stderr.decode('utf-8', 'replace')[:6000])
    return json.loads(result.stdout if expected == 0 else result.stderr)


def _project(parent):
    from gossip_harness.library_m4_reference_v1 import write_m4_project
    root = parent / 'source'
    write_m4_project(root)
    return root


def _release(root, output, *, expected=0):
    return _run(root, ['-m', 'library.clients.release', '--output', str(output)], expected=expected)


def _manifest(root):
    raw = (root / 'release-manifest.json').read_bytes()
    manifest = json.loads(raw)
    canonical = lambda value: json.dumps(value, ensure_ascii=False, sort_keys=True,
                                         separators=(',', ':')).encode()
    assert raw == canonical(manifest)
    records = manifest['files']
    assert [row['path'] for row in records] == sorted(row['path'] for row in records)
    assert len({row['path'] for row in records}) == len(records)
    assert hashlib.sha256(canonical(records)).hexdigest() == manifest['source_sha256']
    for row in records:
        assert set(row) == {'path', 'bytes', 'sha256'}
        source = root / row['path']
        assert source.is_file() and not source.is_symlink()
        contents = source.read_bytes()
        assert len(contents) == row['bytes']
        assert hashlib.sha256(contents).hexdigest() == row['sha256']
    return manifest


class LibraryM4ReleaseReferenceTests(unittest.TestCase):
    def test_allowlist_workflow_and_generated_source_are_explicit(self):
        paths = sorted(set(m3_files()) | set(PUBLIC_FIXTURE_PATHS))
        files = release_files(paths)
        compile(files['library/clients/release.py'], 'library/clients/release.py', 'exec')
        self.assertEqual(len(files), 9)
        self.assertFalse(set(files).intersection(IMMUTABLE_PATHS))
        self.assertEqual(files['release/dataset/welcome.txt'], files['release/dataset/notes.md'])
        for name in ('welcome.txt', 'notes.md', 'literal.html'):
            self.assertLessEqual(len(files['release/dataset/' + name].encode()), 32768)
        workflow = json.loads(files['release/dataset/workflow.json'])
        self.assertEqual(workflow['format'], 'local-research-library-public-workflow-v1')
        self.assertEqual([op['op'] for op in workflow['operations']],
                         ['import', 'import', 'submit', 'prepare', 'commit', 'annotate', 'refresh'])
        self.assertLessEqual(len(workflow['operations']), 64)
        self.assertLessEqual(len(files['release/dataset/workflow.json'].encode()), 60 * 1024)
        for name in ('../secret.py', '/absolute.py', '.env.local', 'release-manifest.json',
                     'private-tests/test_private.py', 'library/catalog/../keys.py',
                     'library/catalog/.env.py', 'library/catalog/user.sqlite3'):
            with self.subTest(name=name), self.assertRaises(ValueError):
                release_files(paths + [name])
        with self.assertRaises(ValueError):
            release_files(paths + [paths[0]])
        with self.assertRaises(ValueError):
            release_files(list(m3_files()))

    def test_two_fresh_builds_and_relocated_self_build_are_byte_identical(self):
        with ArtifactDirectory('m4-release-determinism') as artifacts:
            base = artifacts.root.resolve()
            source = _project(base)
            for name, value in {'.env.local': 'secret sentinel', 'user.sqlite3': 'user data',
                                'private/test_hidden.py': 'hidden sentinel'}.items():
                target = source / name
                target.parent.mkdir(exist_ok=True)
                target.write_text(value)
            first, second, third = (base / name for name in ('first', 'second', 'third'))
            result = _release(source, first)
            self.assertEqual(_release(source, second), result)
            self.assertEqual(_release(first, third), result)
            manifest = _manifest(first)
            self.assertEqual(_manifest(second), manifest)
            self.assertEqual(_manifest(third), manifest)
            self.assertEqual(set(result), {'format', 'manifest', 'files', 'source_sha256'})
            self.assertEqual(result['format'], 'local-research-library-release-v1')
            self.assertEqual(result['manifest'], 'release-manifest.json')
            self.assertEqual(result['files'], len(manifest['files']))
            self.assertEqual(manifest['runtime'], {'image': RUNTIME_IMAGE, 'python': '3.12'})
            self.assertEqual(manifest['product_contract_sha256'], PRODUCT_CONTRACT_SHA256)
            self.assertEqual(manifest['storage_version'], 4)
            names = {row['path'] for row in manifest['files']}
            self.assertTrue(set(IMMUTABLE_PATHS).issubset(names))
            self.assertTrue(set(PUBLIC_FIXTURE_PATHS).issubset(names))
            for name in names | {'release-manifest.json'}:
                self.assertEqual((first / name).read_bytes(), (second / name).read_bytes())
                self.assertEqual((first / name).read_bytes(), (third / name).read_bytes())
            self.assertFalse(names & {'.env.local', 'user.sqlite3', 'private/test_hidden.py'})
            self.assertNotIn(str(base), json.dumps(manifest))
            (base / 'receipt.json').write_text(json.dumps({
                'purpose': 'authored_reference_release_qualification', 'executed': True,
                'python': sys.version, 'result': result, 'manifest': manifest,
                'identical_builds': 3, 'whole_project_acceptance': False,
            }, indent=2) + '\n')

    def test_existing_targets_missing_inputs_and_symlinks_preserve_state(self):
        with ArtifactDirectory('m4-release-boundaries') as artifacts:
            base = artifacts.root.resolve()
            source = _project(base)
            for name, kind in (('existing-file', 'file'), ('existing-dir', 'dir'),
                               ('existing-link', 'link')):
                target = base / name
                if kind == 'dir':
                    target.mkdir()
                elif kind == 'file':
                    target.write_text('preserve')
                else:
                    target.symlink_to(base / 'missing-target')
                self.assertEqual(_release(source, target, expected=2), {'error': 'already_exists'})
                self.assertTrue(target.exists() or target.is_symlink())
            self.assertEqual((base / 'existing-file').read_text(), 'preserve')
            asset = source / 'release/dataset/welcome.txt'
            raw = asset.read_bytes()
            asset.unlink()
            self.assertEqual(_release(source, base / 'missing-input', expected=2),
                             {'error': 'invalid_source'})
            outside = base / 'outside.txt'
            outside.write_bytes(raw)
            asset.symlink_to(outside)
            self.assertEqual(_release(source, base / 'symlink-input', expected=2),
                             {'error': 'invalid_source'})
            asset.unlink()
            asset.write_bytes(raw)
            original = source / 'release/dataset'
            moved = base / 'moved-dataset'
            original.rename(moved)
            original.symlink_to(moved, target_is_directory=True)
            self.assertEqual(_release(source, base / 'ancestor-input', expected=2),
                             {'error': 'invalid_source'})
            original.unlink()
            moved.rename(original)
            linked = base / 'linked-source'
            linked.symlink_to(source, target_is_directory=True)
            # Keep the lexical import path: subprocess cwd would resolve the link.
            result = _run(base, ['-c', 'import runpy,sys;sys.path.insert(0,sys.argv[1]);'
                'sys.argv=["release","--output",sys.argv[2]];'
                'runpy.run_module("library.clients.release",run_name="__main__")',
                str(linked), str(base / 'root-link-output')], expected=2)
            self.assertEqual(result, {'error': 'invalid_source'})
            for name in ('missing-input', 'symlink-input', 'ancestor-input', 'root-link-output'):
                self.assertFalse((base / name).exists())
            self.assertEqual(list(base.glob('.library-release-*')), [])

    def test_real_module_loader_preserves_and_rejects_symlink_before_parent_segment(self):
        with ArtifactDirectory('m4-release-lexical-ancestors') as artifacts:
            base = artifacts.root.resolve()
            source = _project(base)
            (base / 'real-child').mkdir()
            (base / 'link').symlink_to(base / 'real-child', target_is_directory=True)
            lexical = str(base / 'link' / '..' / 'source')
            script = "import json,sys\nsys.path.insert(0,sys.argv[1])\nfrom library.clients import release\nvalue={'module_file':release.__file__}\ntry: value['result']=release.build_release(sys.argv[2])\nexcept release.LibraryError as error: value['error']=error.code\nprint(json.dumps(value))\n"
            value = _run(base, ['-I', '-c', script, lexical, str(base / 'bad-source')])
            self.assertIn('/link/../source/', value['module_file'])
            self.assertEqual(value['error'], 'invalid_source')
            self.assertNotIn('result', value)
            self.assertFalse((base / 'bad-source').exists())
            output = str(base / 'link' / '..' / 'bad-destination')
            self.assertEqual(_release(source, output, expected=2), {'error': 'invalid_source'})
            self.assertFalse((base / 'bad-destination').exists())
            # Ordinary parent-relative output remains supported (INSTALL.md).
            result = _release(source, '../valid-relative')
            self.assertEqual(result['format'], 'local-research-library-release-v1')
            self.assertTrue((base / 'valid-relative/release-manifest.json').is_file())
            (base / 'proof.json').write_text(json.dumps(value, indent=2) + '\n')

    def test_concurrent_publish_has_one_winner_and_no_partial_replacement(self):
        with ArtifactDirectory('m4-release-concurrency') as artifacts:
            base = artifacts.root.resolve()
            source = _project(base)
            output = base / 'winner'
            command = [sys.executable, '-B', '-m', 'library.clients.release', '--output', str(output)]
            processes = [subprocess.Popen(command, cwd=source, env=_environment(),
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE) for _ in range(2)]
            try:
                values = []
                for process in processes:
                    stdout, stderr = process.communicate(timeout=30)
                    values.append((process.returncode, json.loads(stdout or stderr)))
                self.assertEqual(sorted(code for code, _ in values), [0, 2])
                self.assertIn((2, {'error': 'already_exists'}), values)
                manifest = _manifest(output)
                self.assertEqual(next(value for code, value in values if code == 0)['source_sha256'],
                                 manifest['source_sha256'])
                self.assertEqual(list(base.glob('.library-release-*')), [])
            finally:
                for process in processes:
                    if process.poll() is None:
                        process.kill()
                        process.wait(timeout=3)

    def test_publication_failure_retains_competing_empty_directory(self):
        with ArtifactDirectory('m4-release-no-clobber') as artifacts:
            base = artifacts.root.resolve()
            source = _project(base)
            output = base / 'competing'
            # Declared authored interposition creates a competing empty directory
            # after staging, precisely where plain os.rename would replace it.
            script = '''import json,os,sys
from library.clients import release
original=release._publish
observed={}
def competing(parent, stage, target):
 os.mkdir(target,dir_fd=parent)
 observed['inode']=os.stat(target,dir_fd=parent).st_ino
 original(parent,stage,target)
release._publish=competing
try: release.build_release(sys.argv[1])
except release.LibraryError as error: observed['error']=error.code
observed['after']=os.stat(sys.argv[1]).st_ino
observed['contents']=os.listdir(sys.argv[1])
print(json.dumps(observed))
'''
            value = _run(source, ['-c', script, str(output)])
            self.assertEqual(value['error'], 'already_exists')
            self.assertEqual(value['inode'], value['after'])
            self.assertEqual(value['contents'], [])
            self.assertEqual(list(base.glob('.library-release-*')), [])

    def test_fresh_installed_release_runs_real_cli_lifecycle_recovery_journey(self):
        with ArtifactDirectory('m4-release-installed-cli') as artifacts:
            base = artifacts.root.resolve()
            source = _project(base)
            installed = base / 'installed'
            result = _release(source, installed)
            inputs = base / 'inputs'; inputs.mkdir()
            backups = base / 'backups'; backups.mkdir()
            for name in ('welcome.txt', 'notes.md', 'literal.html'):
                (inputs / name).write_bytes((installed / 'release/dataset' / name).read_bytes())
            prefix = ['-m', 'library', '--db', str(base / 'library.sqlite3'),
                      '--root', str(inputs), '--backup-dir', str(backups)]
            commands = []
            def cli(*args):
                value = _run(installed, prefix + list(args))
                commands.append({'arguments': list(args), 'result': value})
                return value
            welcome = cli('import', 'welcome.txt')['document']
            notes = cli('import', 'notes.md')['document']
            self.assertNotEqual(welcome['document_id'], notes['document_id'])
            self.assertEqual(welcome['blob_id'], notes['blob_id'])
            identifier = welcome['document_id']
            cli('annotate', identifier, '--expected-version', '1', '--notes', 'reviewed 雪', '--tag', 'demo')
            cli('refresh', identifier, '--expected-version', '2', '--text', 'Café research — 雪: second revision')
            document = cli('document-v1', identifier)
            self.assertEqual(document['current_revision']['revision'], 2)
            self.assertTrue(document['current_revision']['revision_id'].startswith('rev-'))
            cli('backup', 'checkpoint.json')
            cli('delete', identifier, '--expected-version', '3')
            generation = cli('diagnostics')['generation']
            restored = cli('restore-backup', 'checkpoint.json', '--expected-generation', str(generation))
            self.assertEqual(restored['generation'], generation + 1)
            self.assertEqual(cli('search', '雪')['total'], 2)
            restored_document = cli('document-v1', identifier)
            self.assertFalse(restored_document['deleted'])
            self.assertGreater(restored_document['edit_version'], 4)
            exported = cli('export-v1', identifier, '--include-history')
            self.assertEqual(exported['format'], 'local-research-library-export-v4')
            self.assertEqual(len(exported['documents'][0]['revisions']), 2)
            self.assertEqual(cli('migrate')['migrated'], False)
            (base / 'receipt.json').write_text(json.dumps({
                'purpose': 'authored_reference_release_install_qualification',
                'python': sys.version, 'release': result, 'commands': commands,
                'whole_project_acceptance': False,
            }, indent=2, ensure_ascii=False) + '\n')
