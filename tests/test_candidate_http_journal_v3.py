"""Disposable host-only boundary fixtures; no Engine or candidate execution."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from gossip_harness import candidate_http_journal_v3 as journal
from gossip_harness import candidate_client_process_v4 as engine


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


class CandidateHttpJournalV3DecodeTests(unittest.TestCase):
    def test_strict_values_whitespace_and_unicode(self):
        for value in ({"café": [True, False, None, 1, -2, 0.25, "雪"]}, [1, 2], "text", None):
            with self.subTest(value=value):
                self.assertEqual(journal.decode(b' \n' + canonical(value) + b'\r\n'), value)
        self.assertEqual(journal.decode(b'"\\ud83d\\ude00"'), "😀")
        self.assertEqual(journal.decode(b'"braces [{]} and \\\"quotes\\\""'), 'braces [{]} and "quotes"')

    def test_large_journal_is_separate_from_frozen_engine_control_limit(self):
        raw = canonical({"retained": "x" * (engine.CONTROL_LIMIT + 1)})
        self.assertGreater(len(raw), engine.CONTROL_LIMIT)
        self.assertEqual(len(journal.decode(raw)["retained"]), engine.CONTROL_LIMIT + 1)
        with self.assertRaises(engine.ProcessError):
            engine.strict_json_loads(raw)
        self.assertEqual(engine.CONTROL_LIMIT, 1024 * 1024)

    def test_global_byte_boundary_and_per_call_smaller_bound(self):
        raw = b'"' + b'x' * (journal.MAX_RECORD_BYTES - 2) + b'"'
        self.assertEqual(len(journal.decode(raw)), journal.MAX_RECORD_BYTES - 2)
        with self.assertRaises(journal.JournalError):
            journal.decode(raw + b' ')
        self.assertEqual(journal.decode(b'null', max_bytes=4), None)
        with self.assertRaises(journal.JournalError):
            journal.decode(b'null ', max_bytes=4)
        for bound in (0, -1, True, 1.0, journal.MAX_RECORD_BYTES + 1):
            with self.subTest(bound=bound), self.assertRaises(journal.JournalError):
                journal.decode(b'0', max_bytes=bound)

    def test_duplicate_keys_rejected_after_escape_decoding(self):
        for raw in (b'{"a":1,"a":2}', b'{"a":{"x":1,"x":2}}', b'{"x":1,"\\u0078":2}'):
            with self.subTest(raw=raw), self.assertRaises(journal.JournalError):
                journal.decode(raw)

    def test_nonfinite_and_overflow_numbers_are_rejected(self):
        for raw in (b'NaN', b'Infinity', b'-Infinity', b'[NaN]', b'{"x":1e9999}', b'-1e9999'):
            with self.subTest(raw=raw), self.assertRaises(journal.JournalError):
                journal.decode(raw)

    def test_only_utf8_scalar_text_dialect_is_admitted(self):
        for raw in (b'\xef\xbb\xbf{}', '{}'.encode('utf-16'), '{}'.encode('utf-32'),
                    b'"\xff"', b'"\\ud800"', b'{"\\udfff":0}'):
            with self.subTest(raw=raw), self.assertRaises(journal.JournalError):
                journal.decode(raw)
        for raw in ('{}', bytearray(b'{}'), memoryview(b'{}')):
            with self.subTest(kind=type(raw)), self.assertRaises(journal.JournalError):
                journal.decode(raw)

    def test_depth_boundary_precedes_json_materialization(self):
        raw = b'[' * journal.MAX_DEPTH + b'0' + b']' * journal.MAX_DEPTH
        value = journal.decode(raw)
        for _ in range(journal.MAX_DEPTH):
            value = value[0]
        self.assertEqual(value, 0)
        with patch.object(journal.json, 'loads', side_effect=AssertionError('decoder must not run')):
            with self.assertRaisesRegex(journal.JournalError, 'nesting bound'):
                journal.decode(b'[' + raw + b']')

    def test_node_boundary_precedes_json_materialization(self):
        # One array container plus MAX_NODES-1 scalar values.
        raw = b'[' + b'0,' * (journal.MAX_NODES - 2) + b'0]'
        self.assertEqual(len(journal.decode(raw)), journal.MAX_NODES - 1)
        with patch.object(journal.json, 'loads', side_effect=AssertionError('decoder must not run')):
            with self.assertRaisesRegex(journal.JournalError, 'node bound'):
                journal.decode(raw[:-1] + b',0]')

    def test_object_keys_count_toward_budget_but_string_contents_do_not(self):
        # A bounded focused guard for the declared node-count definition; the
        # actual million-node boundary is exercised separately above.
        with patch.object(journal, 'MAX_NODES', 5):
            self.assertEqual(journal.decode(b'{"a":1,"b":2}'), {"a": 1, "b": 2})
            with self.assertRaises(journal.JournalError):
                journal.decode(b'{"a":1,"b":[2]}')
            self.assertEqual(journal.decode(b'"a,b,[{true false null}]"'), 'a,b,[{true false null}]')

    def test_malformed_or_multiple_values_never_pass_budget_scan_as_json(self):
        for raw in (b'', b'{}{}', b'[}', b'{', b'"unfinished', b'"bad\\q"', b'"bad\nstring"',
                    b'1.', b'01', b'truefalse', b'{"a":1,}', b'[,]'):
            with self.subTest(raw=raw), self.assertRaises(journal.JournalError):
                journal.decode(raw)


class CandidateHttpJournalV3FileTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='http-journal-v3-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.path = self.root / 'record.json'
        self.path.write_bytes(canonical({"z": 1, "café": "雪"}))

    def test_stable_canonical_file_and_source_map(self):
        raw = self.path.read_bytes()
        self.assertEqual(journal.read(self.path), raw)
        self.assertEqual(journal.read_json(str(self.path)), {"z": 1, "café": "雪"})
        sources = journal.evaluator_sources()
        self.assertEqual(sources, {'gossip_harness/candidate_http_journal_v3.py':
            hashlib.sha256(Path(journal.__file__).read_bytes()).hexdigest()})

    def test_read_json_preserves_authored_canonical_dialect(self):
        for raw in (b' {"a":1}', b'{"z":1,"a":2}', b'{"caf\\u00e9":"snow"}', b'1e0'):
            self.path.write_bytes(raw)
            self.assertIsNotNone(journal.decode(raw))
            with self.subTest(raw=raw), self.assertRaisesRegex(journal.JournalError, 'canonical'):
                journal.read_json(self.path)
        self.path.write_bytes(b'{"a":1,"a":2}')
        with self.assertRaises(journal.JournalError):
            journal.read_json(self.path)

    def test_file_size_boundary_empty_raw_and_oversized_sparse_file(self):
        self.path.write_bytes(b'1234')
        self.assertEqual(journal.read(self.path, max_bytes=4), b'1234')
        with self.assertRaises(journal.JournalError):
            journal.read(self.path, max_bytes=3)
        self.path.write_bytes(b'')
        self.assertEqual(journal.read(self.path), b'')
        with self.path.open('wb') as handle:
            handle.truncate(journal.MAX_RECORD_BYTES + 1)
        with self.assertRaises(journal.JournalError):
            journal.read(self.path)

    def test_final_and_ancestor_symlinks_are_rejected(self):
        link = self.root / 'link.json'
        link.symlink_to(self.path.name)
        with self.assertRaises(journal.JournalError):
            journal.read(link)
        real = self.root / 'real'
        real.mkdir()
        (real / 'record.json').write_bytes(b'{}')
        (self.root / 'linked-directory').symlink_to('real', target_is_directory=True)
        with self.assertRaises(journal.JournalError):
            journal.read(self.root / 'linked-directory' / 'record.json')

    def test_directory_fifo_missing_and_traversal_are_rejected(self):
        fifo = self.root / 'fifo'
        os.mkfifo(fifo)
        for path in (self.root, fifo, self.root / 'missing', self.root / '..' / self.root.name / 'record.json'):
            with self.subTest(path=path), self.assertRaises(journal.JournalError):
                journal.read(path)

    def test_growth_during_read_is_rejected(self):
        actual = journal.os.read
        changed = False
        def growing(descriptor, count):
            nonlocal changed
            if not changed:
                changed = True
                with self.path.open('ab') as handle:
                    handle.write(b'!')
            return actual(descriptor, count)
        with patch.object(journal.os, 'read', side_effect=growing):
            with self.assertRaises(journal.JournalError):
                journal.read(self.path)

    def test_same_size_rewrite_and_truncation_are_rejected(self):
        actual = journal.os.read
        for replacement in (b'changed bytes', b''):
            self.path.write_bytes(b'original data')
            changed = False
            def rewriting(descriptor, count):
                nonlocal changed
                result = actual(descriptor, count)
                if not changed:
                    changed = True
                    self.path.write_bytes(replacement)
                    before = self.path.stat().st_mtime_ns
                    os.utime(self.path, ns=(before, before + 1_000_000))
                return result
            with self.subTest(replacement=replacement), patch.object(journal.os, 'read', side_effect=rewriting):
                with self.assertRaises(journal.JournalError):
                    journal.read(self.path)

    def test_replacement_between_stat_and_open_is_rejected(self):
        actual = journal.os.open
        changed = False
        def replacing(path, flags, *args, **kwargs):
            nonlocal changed
            if path == self.path.name and not changed:
                changed = True
                self.path.rename(self.root / 'original.json')
                self.path.write_bytes(b'{}')
            return actual(path, flags, *args, **kwargs)
        with patch.object(journal.os, 'open', side_effect=replacing):
            with self.assertRaises(journal.JournalError):
                journal.read(self.path)

    def test_replacement_after_read_preserves_fd_but_invalidates_path(self):
        actual = journal.os.read
        changed = False
        def replacing(descriptor, count):
            nonlocal changed
            result = actual(descriptor, count)
            if not changed:
                changed = True
                self.path.rename(self.root / 'original.json')
                self.path.symlink_to('original.json')
            return result
        with patch.object(journal.os, 'read', side_effect=replacing):
            with self.assertRaises(journal.JournalError):
                journal.read(self.path)

    def test_ancestor_replacement_is_detected_after_read(self):
        parent = self.root / 'parent'
        parent.mkdir()
        path = parent / 'record.json'
        path.write_bytes(b'{}')
        actual = journal.os.read
        changed = False
        def replacing(descriptor, count):
            nonlocal changed
            result = actual(descriptor, count)
            if not changed:
                changed = True
                parent.rename(self.root / 'moved')
                parent.mkdir()
                (parent / 'record.json').write_bytes(b'{}')
            return result
        with patch.object(journal.os, 'read', side_effect=replacing):
            with self.assertRaises(journal.JournalError):
                journal.read(path)


if __name__ == '__main__':
    unittest.main()
