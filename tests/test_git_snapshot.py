"""Batch Git object framing and real-tree equivalence without candidate execution."""
import hashlib
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from devtools import git_snapshot
from gossip_harness.gitstore import GitStore, _run


def blob_hash(content, algorithm='sha1'):
    return hashlib.new(algorithm, b'blob ' + str(len(content)).encode() + b'\x00' + content).hexdigest()


def framed(content, *, algorithm='sha1'):
    sha = blob_hash(content, algorithm)
    return sha, sha.encode() + b' blob ' + str(len(content)).encode() + b'\n' + content + b'\n'


class GitBatchFramingTests(unittest.TestCase):
    def test_binary_payloads_are_size_framed_and_both_hash_formats_verified(self):
        content = b'\x00\xff\n' + b'a' * 40 + b' blob 0\n\n'
        for algorithm in ('sha1', 'sha256'):
            with self.subTest(algorithm=algorithm):
                sha, raw = framed(content, algorithm=algorithm)
                self.assertEqual(git_snapshot._blob_batch(raw, [sha]), {sha: content})

    def test_truncated_corrupt_mistyped_and_extra_frames_are_rejected(self):
        content = b'original\x00payload\n'
        sha, raw = framed(content)
        malformed = [raw[:-1], raw[:-4], raw.split(b'\n', 1)[0], raw + b'extra',
                     raw.replace(b' blob ', b' tree ', 1),
                     raw.replace(sha.encode(), b'0' * 40, 1),
                     raw.replace(b'original', b'changed!'),
                     raw.replace(b' blob 17\n', b' blob 18\n', 1),
                     sha.encode() + b' missing\n']
        for payload in malformed:
            with self.subTest(payload=payload):
                with self.assertRaises(ValueError):
                    git_snapshot._blob_batch(payload, [sha])

    def test_tree_framing_preserves_tabs_newlines_and_symlink_target_records(self):
        sha = blob_hash(b'contents')
        records = (f'100755 blob {sha}\tscript\nwith\ttabs.py\0'
                   f'120000 blob {sha}\tsymlink\0').encode()
        self.assertEqual(git_snapshot._tree_entries(records, 40),
                         {'script\nwith\ttabs.py': sha, 'symlink': sha})
        for invalid in (records[:-1], records + records, records.replace(b'100755 blob', b'160000 commit'),
                        records.replace(sha.encode(), b'badsha'), records.replace(b'symlink\x00', b'../escape\x00')):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    git_snapshot._tree_entries(invalid, 40)

    def test_symbolic_revision_rejected_before_starting_git(self):
        with patch.object(git_snapshot, '_git_bytes') as git:
            with self.assertRaisesRegex(ValueError, 'exact full Git commit SHA'):
                git_snapshot.read_text_tree(Path('unused'), 'HEAD')
        git.assert_not_called()


class GitSnapshotTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix='test-batched-git-')
        cls.addClassCleanup(cls.temp.cleanup)
        cls.root = Path(cls.temp.name)
        cls.files = {'empty.txt': '', 'unicode.py': 'VALUE = "caf\u00e9"\r\n',
                     'dir/file.py': 'print("fixture only, never executed")\n',
                     'duplicate.txt': 'print("fixture only, never executed")\n',
                     'tab\tand\nnewline.txt': 'nul\x00and\nnewline\n'}
        cls.store = GitStore.create(cls.root / 'fixture.git', cls.files)
        cls.commit = cls.store.head()

    def test_real_tree_matches_serial_reader_with_exactly_two_processes(self):
        expected = self.store.read_files(self.commit)
        with patch.object(subprocess, 'run', wraps=subprocess.run) as run:
            actual = git_snapshot.read_text_tree(self.store.path, self.commit)
        self.assertEqual(actual, expected)
        self.assertEqual(actual, self.files)
        self.assertEqual(run.call_count, 2)
        self.assertIn('ls-tree', run.call_args_list[0].args[0])
        self.assertIn('--batch', run.call_args_list[1].args[0])
        requested = run.call_args_list[1].kwargs['input'].splitlines()
        self.assertEqual(len(requested), len(set(requested)))
        self.assertLess(len(requested), len(self.files))

    def test_old_commit_stays_pinned_after_new_proposal(self):
        newer = self.store.propose({'unicode.py': 'CHANGED = True\n'}, base_sha=self.commit)
        self.assertNotEqual(newer, self.commit)
        self.assertEqual(git_snapshot.read_text_tree(self.store.path, self.commit), self.files)
        self.assertEqual(git_snapshot.read_text_tree(self.store.path, newer), self.store.read_files(newer))

    def test_symlink_blob_matches_serial_target_text_without_following_it(self):
        target = b'/outside/never-follow-this-path'
        blob = git_snapshot._git_bytes(self.store.path, ('hash-object', '-w', '--stdin'), input_data=target).decode().strip()
        tree = git_snapshot._git_bytes(self.store.path, ('mktree', '-z'),
                                      input_data=f'120000 blob {blob}\tlink\0'.encode()).decode().strip()
        commit = _run(self.store.path, 'commit-tree', tree, '-m', 'Trusted symlink fixture').stdout.decode().strip()
        expected = self.store.read_files(commit)
        self.assertEqual(git_snapshot.read_text_tree(self.store.path, commit), expected)
        self.assertEqual(expected, {'link': target.decode()})

    def test_corrupted_batch_payload_from_git_is_rejected(self):
        original = git_snapshot._git_bytes
        def corrupt(path, args, *, input_data=None):
            result = original(path, args, input_data=input_data)
            if args[0] == 'cat-file':
                result = result.replace(b'fixture only', b'fixture fake', 1)
            return result
        with patch.object(git_snapshot, '_git_bytes', side_effect=corrupt):
            with self.assertRaisesRegex(ValueError, 'bytes differ from their object hash'):
                git_snapshot.read_text_tree(self.store.path, self.commit)


if __name__ == '__main__':
    unittest.main()
