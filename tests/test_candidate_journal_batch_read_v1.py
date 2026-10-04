"""Actual host-filesystem and anchored-journal controls; no Engine or browser."""
from __future__ import annotations

from collections import Counter
from dataclasses import asdict
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from typing import Any
import unittest
from unittest.mock import patch

from gossip_harness import candidate_checkpoint_chain_v1 as chain
from gossip_harness import candidate_checkpoint_head_v1 as head
from gossip_harness import candidate_execution_journal_v1 as journal
from gossip_harness import candidate_http_journal_v3 as stable
from gossip_harness import candidate_journal_batch_read_v1 as batch


def _directory_identity(path: Path) -> tuple[int, int, int]:
    value = path.stat(follow_symlinks=False)
    return value.st_dev, value.st_ino, value.st_mode


class CandidateJournalBatchReadV1Tests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory(prefix="gossip-checkpoint-reader-")
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name).resolve()
        self.ancestor = self.base / "ancestry"
        self.ancestor.mkdir()
        self.roots = (self.ancestor / "raw", self.ancestor / "delta")
        for root in self.roots:
            root.mkdir()
        self.identities = tuple(_directory_identity(root) for root in self.roots)
        self.policy = batch.BatchReadPolicy()

    def reader(self) -> batch.CheckpointReader:
        reader = batch.CheckpointReader(self.roots, self.identities, policy=self.policy)
        self.addCleanup(reader.close)
        return reader

    def test_policy_is_closed_and_root_identity_is_independently_supplied(self) -> None:
        record = self.policy.record()
        self.assertEqual(record["protocol"], batch.PROTOCOL)
        self.assertEqual(record["ancestry_checks"], "before-and-after-every-file")
        self.assertFalse(record["cross_checkpoint_cache"])

        class DerivedPolicy(batch.BatchReadPolicy):
            pass

        with self.assertRaises(stable.JournalError):
            DerivedPolicy()
        with self.assertRaises(stable.JournalError):
            batch.CheckpointReader(self.roots, self.identities, policy=record)  # type: ignore[arg-type]
        wrong = (self.identities[1], self.identities[0])
        with self.assertRaises(stable.JournalError):
            batch.CheckpointReader(self.roots, wrong, policy=self.policy)

    def test_actual_copied_helper_and_leaf_reader_source_drift_are_rejected(self) -> None:
        script = r"""
import pathlib, sys
sys.path.insert(0, sys.argv[1])
from gossip_harness import candidate_journal_batch_read_v1 as batch
from gossip_harness import candidate_http_journal_v3 as stable
policy = batch.BatchReadPolicy()
policy.record()
path = pathlib.Path(sys.argv[1])/'gossip_harness'/sys.argv[2]
path.write_bytes(path.read_bytes()+b'\n# authored source drift\n')
try:
    policy.record()
except stable.JournalError:
    print('source-drift-rejected')
else:
    raise AssertionError('changed loaded helper bytes accepted')
"""
        for name in ('candidate_journal_batch_read_v1.py', 'candidate_http_journal_v3.py'):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                package = root/'gossip_harness'
                package.mkdir()
                (package/'__init__.py').write_bytes(b'')
                for module in (batch, stable):
                    shutil.copy2(Path(module.__file__), package/Path(module.__file__).name)
                result = subprocess.run([sys.executable, '-I', '-c', script, str(root), name],
                                        capture_output=True, timeout=15, check=False)
                self.assertEqual(result.returncode, 0, result.stderr.decode())
                self.assertEqual(result.stdout, b'source-drift-rejected\n')

    def test_fresh_reads_accept_empty_and_stable_hardlinked_files_with_bounds(self) -> None:
        empty = self.roots[0] / "empty.bin"
        empty.write_bytes(b"")
        path = self.roots[0] / "fact.bin"
        path.write_bytes(b"one")
        os.link(path, self.base / "external-link.bin")
        with self.reader() as reader:
            self.assertEqual(reader.read(empty, max_bytes=1), b"")
            self.assertEqual(reader.read(path, max_bytes=3), b"one")
            path.write_bytes(b"two")
            self.assertEqual(reader.read(path, max_bytes=3), b"two")
            with self.assertRaises(stable.JournalError):
                reader.read(path, max_bytes=2)
        with self.assertRaises(stable.JournalError):
            reader.read(path)

    def test_symlink_leaf_and_intermediate_ancestor_are_rejected(self) -> None:
        target = self.base / "outside.bin"
        target.write_bytes(b"fact")
        linked = self.roots[0] / "fact.bin"
        linked.symlink_to(target)
        with self.reader() as reader:
            with self.assertRaises(stable.JournalError):
                reader.read(linked)
            with self.assertRaises(stable.JournalError):
                reader.read(self.roots[0] / "owner.lock")
            with self.assertRaises(stable.JournalError):
                reader.read(target)
        alias = self.base / "alias"
        alias.symlink_to(self.ancestor, target_is_directory=True)
        with self.assertRaises(stable.JournalError):
            batch.CheckpointReader((alias / "raw", alias / "delta"), self.identities,
                                   policy=self.policy)

    def test_leaf_replacement_with_identical_bytes_during_read_is_rejected(self) -> None:
        path = self.roots[0] / "fact.bin"
        path.write_bytes(b"fact")
        replacement = self.base / "replacement.bin"
        replacement.write_bytes(b"fact")
        original_read = os.read
        changed = False

        def replace_after_read(descriptor: int, count: int) -> bytes:
            nonlocal changed
            raw = original_read(descriptor, count)
            if not changed:
                changed = True
                os.replace(replacement, path)
            return raw

        with self.reader() as reader:
            with patch.object(batch.os, "read", side_effect=replace_after_read):
                with self.assertRaises(stable.JournalError):
                    reader.read(path)
        self.assertTrue(changed)
        self.assertEqual(path.read_bytes(), b"fact")

    def test_nlink_change_during_read_is_rejected(self) -> None:
        path = self.roots[0] / "fact.bin"
        path.write_bytes(b"fact")
        original_read = os.read
        changed = False

        def link_after_read(descriptor: int, count: int) -> bytes:
            nonlocal changed
            raw = original_read(descriptor, count)
            if not changed:
                changed = True
                os.link(path, self.base / "new-link.bin")
            return raw

        with self.reader() as reader:
            with patch.object(batch.os, "read", side_effect=link_after_read):
                with self.assertRaises(stable.JournalError):
                    reader.read(path)
        self.assertTrue(changed)
        self.assertEqual(path.stat().st_nlink, 2)

    def test_ancestor_replacement_during_read_is_rejected_before_return(self) -> None:
        path = self.roots[0] / "fact.bin"
        path.write_bytes(b"fact")
        moved = self.base / "moved"
        original_read = os.read
        changed = False

        def move_after_read(descriptor: int, count: int) -> bytes:
            nonlocal changed
            raw = original_read(descriptor, count)
            if not changed:
                changed = True
                self.ancestor.rename(moved)
                self.ancestor.mkdir()
            return raw

        reader = self.reader()
        try:
            with patch.object(batch.os, "read", side_effect=move_after_read):
                with self.assertRaises(stable.JournalError):
                    reader.read(path)
            self.assertTrue(changed)
        finally:
            if changed:
                self.ancestor.rmdir()
                moved.rename(self.ancestor)

    def test_held_ancestor_descriptor_change_is_rejected_with_entry_unchanged(self) -> None:
        path = self.roots[0] / "fact.bin"
        path.write_bytes(b"fact")
        original_open = os.open
        ancestor_descriptors: list[int] = []

        def capture_open(name: Any, flags: int, *args: Any, **kwargs: Any) -> int:
            descriptor = original_open(name, flags, *args, **kwargs)
            if name == "ancestry" and kwargs.get("dir_fd") is not None:
                ancestor_descriptors.append(descriptor)
            return descriptor

        with patch.object(batch.os, "open", side_effect=capture_open):
            reader = self.reader()
        self.assertTrue(ancestor_descriptors)
        entry_identity = _directory_identity(self.ancestor)
        other = self.base / "other"
        other.mkdir()
        descriptor = original_open(other, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.dup2(descriptor, ancestor_descriptors[0])
        finally:
            os.close(descriptor)
        self.assertEqual(_directory_identity(self.ancestor), entry_identity)
        with self.assertRaises(stable.JournalError):
            reader.read(path)

    def test_ancestor_entry_is_checked_again_before_the_next_file(self) -> None:
        first, second = self.roots[0] / "first.bin", self.roots[0] / "second.bin"
        first.write_bytes(b"first")
        second.write_bytes(b"second")
        reader = self.reader()
        self.assertEqual(reader.read(first), b"first")
        moved = self.base / "moved"
        self.ancestor.rename(moved)
        self.ancestor.mkdir()
        try:
            with self.assertRaises(stable.JournalError):
                reader.read(second)
        finally:
            self.ancestor.rmdir()
            moved.rename(self.ancestor)

    def test_leaf_growth_same_size_rewrite_and_truncation_are_rejected(self) -> None:
        for changed in (b"longer", b"evil", b"x"):
            with self.subTest(changed=changed):
                path = self.roots[0] / "fact.bin"
                path.write_bytes(b"fact")
                original_read = os.read
                changed_once = False

                def rewrite_after_read(descriptor: int, count: int) -> bytes:
                    nonlocal changed_once
                    raw = original_read(descriptor, count)
                    if not changed_once:
                        changed_once = True
                        path.write_bytes(changed)
                    return raw

                with self.reader() as reader:
                    with patch.object(batch.os, "read", side_effect=rewrite_after_read):
                        with self.assertRaises(stable.JournalError):
                            reader.read(path)
                self.assertTrue(changed_once)

    def test_interrupted_read_releases_owned_descriptors_and_preserves_borrowed(self) -> None:
        path = self.roots[0] / "fact.bin"
        path.write_bytes(b"fact")
        original_open = os.open
        borrowed = original_open(self.roots[0], os.O_RDONLY | os.O_DIRECTORY)
        opened: list[int] = []

        def capture_open(name: Any, flags: int, *args: Any, **kwargs: Any) -> int:
            descriptor = original_open(name, flags, *args, **kwargs)
            opened.append(descriptor)
            return descriptor

        try:
            with patch.object(batch.os, "open", side_effect=capture_open):
                with self.assertRaises(KeyboardInterrupt):
                    with self.reader() as reader:
                        with patch.object(batch.os, "read", side_effect=KeyboardInterrupt):
                            reader.read(path)
            os.fstat(borrowed)
            for descriptor in opened:
                with self.assertRaises(OSError):
                    os.fstat(descriptor)
        finally:
            os.close(borrowed)

    def test_unobserved_rename_restore_is_not_claimed_as_atomic_snapshot(self) -> None:
        path = self.roots[0] / "fact.bin"
        path.write_bytes(b"fact")
        moved = self.base / "moved"
        with self.reader() as reader:
            self.assertEqual(reader.read(path), b"fact")
            self.ancestor.rename(moved)
            moved.rename(self.ancestor)
            self.assertEqual(reader.read(path), b"fact")

    def test_partial_setup_preserves_primary_error_and_attempts_all_closes(self) -> None:
        original_open, original_close = os.open, os.close
        opened: list[int] = []
        closed: list[int] = []

        def fail_open(name: Any, flags: int, *args: Any, **kwargs: Any) -> int:
            if len(opened) == 3:
                raise OSError("fixture partial setup")
            descriptor = original_open(name, flags, *args, **kwargs)
            opened.append(descriptor)
            return descriptor

        def fail_first_close(descriptor: int) -> None:
            closed.append(descriptor)
            original_close(descriptor)
            if len(closed) == 1:
                raise OSError("fixture cleanup acknowledgement")

        with patch.object(batch.os, "open", side_effect=fail_open):
            with patch.object(batch.os, "close", side_effect=fail_first_close):
                with self.assertRaisesRegex(OSError, "fixture partial setup") as caught:
                    batch.CheckpointReader(self.roots, self.identities, policy=self.policy)
        self.assertEqual(closed, list(reversed(opened)))
        self.assertIn("fixture cleanup acknowledgement", " ".join(caught.exception.__notes__))
        for descriptor in opened:
            with self.assertRaises(OSError):
                os.fstat(descriptor)

    def test_close_attempts_all_descriptors_once_and_preserves_failures(self) -> None:
        original_open, original_close = os.open, os.close
        opened: list[int] = []
        closed: list[int] = []

        def capture_open(name: Any, flags: int, *args: Any, **kwargs: Any) -> int:
            descriptor = original_open(name, flags, *args, **kwargs)
            opened.append(descriptor)
            return descriptor

        with patch.object(batch.os, "open", side_effect=capture_open):
            reader = self.reader()

        def fail_closes(descriptor: int) -> None:
            closed.append(descriptor)
            original_close(descriptor)
            if len(closed) <= 2:
                raise OSError("fixture close " + str(len(closed)))

        with patch.object(batch.os, "close", side_effect=fail_closes):
            with self.assertRaisesRegex(OSError, "fixture close 1") as caught:
                reader.close()
            reader.close()
        self.assertEqual(closed, list(reversed(opened)))
        self.assertIn("fixture close 2", " ".join(caught.exception.__notes__))
        with self.assertRaises(stable.JournalError):
            reader.validate()

    def test_leaf_close_and_final_ancestry_failure_cannot_report_success(self) -> None:
        path = self.roots[0] / "fact.bin"
        path.write_bytes(b"fact")
        reader = self.reader()
        original_close = os.close

        def close_then_fail(descriptor: int) -> None:
            original_close(descriptor)
            raise OSError("fixture leaf close acknowledgement")

        with patch.object(batch.os, "close", side_effect=close_then_fail):
            with self.assertRaisesRegex(OSError, "fixture leaf close acknowledgement"):
                reader.read(path)
        reader.validate()
        moved = self.base / "moved"
        try:
            with self.assertRaises(stable.JournalError):
                with reader:
                    self.assertEqual(reader.read(path), b"fact")
                    self.ancestor.rename(moved)
                    self.ancestor.mkdir()
        finally:
            if moved.exists():
                self.ancestor.rmdir()
                moved.rename(self.ancestor)
        with self.assertRaises(stable.JournalError):
            reader.validate()


class CandidateJournalBatchOwnerV1Tests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory(prefix="gossip-batch-owner-")
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name).resolve()
        self.raw, self.delta, self.anchor = (self.base / name for name in ("raw", "delta", "anchor"))
        self.context = {"purpose": "harness_qualification", "steps": ("first", "second")}
        self.authority = head.ExternalHead.create(self.anchor, journal_roots=(self.raw, self.delta))
        self.addCleanup(self.authority.close)
        self.policy = batch.BatchReadPolicy()

    def owner(self, *, read_policy: batch.BatchReadPolicy | None = None,
              expected: chain.PrefixCommitment | None = None) -> journal.OwnerJournal:
        owner = journal.OwnerJournal(self.raw, self.delta, context=self.context,
                                     authority=self.authority, read_policy=read_policy, expected=expected)
        self.addCleanup(owner.close)
        return owner

    def test_default_genesis_bytes_and_reader_path_remain_legacy(self) -> None:
        with patch.object(batch, "CheckpointReader", side_effect=AssertionError("default selected batch")):
            owner = self.owner()
            owner.retain("fact.bin", b"fact")
            expected = owner.checkpoint()
            genesis = {"protocol": chain.PROTOCOL, "kind": "genesis",
                       "context": {"owner_journal_protocol": journal.PROTOCOL,
                                   "execution_context": {"purpose": "harness_qualification",
                                                         "steps": ["first", "second"]}},
                       "raw_root": str(self.raw), "delta_root": str(self.delta),
                       "limits": asdict(chain.Limits())}
            self.assertEqual((self.delta / "genesis.json").read_bytes(),
                             json.dumps(genesis, sort_keys=True, separators=(",", ":")).encode())
            owner.close()
            reopened = self.owner(expected=expected)
            self.assertEqual(reopened.read("fact.bin"), b"fact")
            self.assertEqual(reopened.checkpoint(), expected)
            reopened.close()
        before = (self.anchor / "head.json").read_bytes()
        with self.assertRaises(chain.ChainError):
            self.owner(expected=expected, read_policy=self.policy)
        self.assertEqual((self.anchor / "head.json").read_bytes(), before)

    def test_batch_policy_is_bound_and_reopen_requires_independent_matching_policy(self) -> None:
        owner = self.owner(read_policy=self.policy)
        owner.retain("fact.bin", b"fact")
        expected = owner.checkpoint()
        genesis = json.loads((self.delta / "genesis.json").read_bytes())
        self.assertEqual(genesis["journal_read"], self.policy.record())
        self.assertEqual(genesis["context"]["journal_read"], self.policy.record())
        owner.close()
        before = (self.anchor / "head.json").read_bytes()
        with self.assertRaises(chain.ChainError):
            self.owner(expected=expected)
        self.assertEqual((self.anchor / "head.json").read_bytes(), before)
        self.authority.close()
        self.authority = head.ExternalHead.reopen(self.anchor, journal_roots=(self.raw, self.delta),
                                                  expected=expected)
        self.addCleanup(self.authority.close)
        reopened = self.owner(expected=expected, read_policy=batch.BatchReadPolicy())
        self.assertEqual(reopened.checkpoint(), expected)
        self.assertEqual(reopened.read("fact.bin"), b"fact")

    def test_direct_chain_binds_policy_without_owner_context_wrapper(self) -> None:
        context = {"purpose": "harness_qualification"}
        item = chain.CheckpointChain.create(self.raw, self.delta, context=context,
                                           authority=self.authority, read_policy=self.policy)
        self.addCleanup(item.close)
        item.retain("fact.bin", b"fact")
        expected = item.validate_boundary().commitment
        genesis = json.loads((self.delta / "genesis.json").read_bytes())
        self.assertEqual(genesis["context"], context)
        self.assertEqual(genesis["journal_read"], self.policy.record())
        item.close()
        with self.assertRaises(chain.ChainError):
            chain.CheckpointChain.reopen(self.raw, self.delta, context=context,
                                         authority=self.authority, expected=expected)
        reopened = chain.CheckpointChain.reopen(self.raw, self.delta, context=context,
                                                authority=self.authority, expected=expected,
                                                read_policy=self.policy)
        self.addCleanup(reopened.close)
        self.assertEqual(reopened.validate_boundary().commitment, expected)

    def test_each_checkpoint_freshly_reads_every_genesis_delta_and_raw_file(self) -> None:
        owner = self.owner(read_policy=self.policy)
        owner.retain("first.bin", b"one")
        owner.retain("second.bin", b"two")
        expected = owner.commitment
        original_read = batch.CheckpointReader.read
        reads: list[tuple[batch.CheckpointReader, Path]] = []

        def record_read(reader: batch.CheckpointReader, path: Path | str, *,
                        max_bytes: int = stable.MAX_RECORD_BYTES) -> bytes:
            reads.append((reader, Path(path)))
            return original_read(reader, path, max_bytes=max_bytes)

        with patch.object(batch.CheckpointReader, "read", new=record_read):
            with patch.object(self.authority, "read", wraps=self.authority.read) as head_reads:
                self.assertEqual(owner.checkpoint(), expected)
                self.assertEqual(owner.checkpoint(), expected)
        expected_files = {self.delta / "genesis.json", self.delta / "delta-00000001.json",
                          self.delta / "delta-00000002.json", self.raw / "first.bin", self.raw / "second.bin"}
        self.assertEqual(Counter(path for _, path in reads), Counter({path: 2 for path in expected_files}))
        readers = {reader for reader, _ in reads}
        self.assertEqual(len(readers), 2)
        for reader in readers:
            with self.assertRaises(stable.JournalError):
                reader.validate()
        self.assertGreaterEqual(head_reads.call_count, 4)

    def test_persistent_raw_tamper_latches_unknown_and_preserves_prior_head(self) -> None:
        owner = self.owner(read_policy=self.policy)
        owner.retain("first.bin", b"one")
        owner.retain("second.bin", b"two")
        expected = owner.checkpoint()
        path = self.raw / "first.bin"
        before = path.stat()
        path.write_bytes(b"bad")
        os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
        with self.assertRaises(chain.ChainUnknown):
            owner.checkpoint()
        self.assertTrue(owner.uncertain)
        self.assertEqual(owner.commitment, expected)
        self.assertEqual(self.authority.read(), expected)
        prior = owner.read_prior("second.bin")
        self.assertEqual(prior.raw, b"two")
        self.assertTrue(prior.prior_only)
        self.assertFalse(prior.acceptance_authority)
        with self.assertRaises(chain.ChainUnknown):
            owner.retain("cleanup.json", b"{}", cleanup=True)

    def test_live_policy_cannot_be_downgraded_after_genesis(self) -> None:
        owner = self.owner(read_policy=self.policy)
        owner.retain("fact.bin", b"fact")
        expected = owner.checkpoint()
        owner._chain.read_policy = None
        with self.assertRaises(chain.ChainUnknown):
            owner.checkpoint()
        self.assertTrue(owner.uncertain)
        self.assertEqual(self.authority.read(), expected)

    def test_changed_delta_is_reconstructed_again_on_next_checkpoint(self) -> None:
        owner = self.owner(read_policy=self.policy)
        owner.retain("first.bin", b"one")
        expected = owner.checkpoint()
        delta = self.delta / "delta-00000001.json"
        original = delta.read_bytes()
        changed = original.replace(b'"name":"first.bin"', b'"name":"third.bin"')
        self.assertNotEqual(changed, original)
        self.assertEqual(len(changed), len(original))
        delta.write_bytes(changed)
        with self.assertRaises(chain.ChainUnknown):
            owner.checkpoint()
        self.assertTrue(owner.uncertain)
        self.assertEqual(self.authority.read(), expected)

    def test_restored_unobserved_tamper_passes_but_consumed_transient_bytes_fail(self) -> None:
        owner = self.owner(read_policy=self.policy)
        owner.retain("first.bin", b"one")
        expected = owner.checkpoint()
        path = self.raw / "first.bin"
        path.write_bytes(b"bad")
        path.write_bytes(b"one")
        self.assertEqual(owner.checkpoint(), expected)
        original_read = batch.CheckpointReader.read
        consumed = False

        def tamper_during_read(reader: batch.CheckpointReader, target: Path | str, *,
                               max_bytes: int = stable.MAX_RECORD_BYTES) -> bytes:
            nonlocal consumed
            if Path(target) != path:
                return original_read(reader, target, max_bytes=max_bytes)
            consumed = True
            path.write_bytes(b"bad")
            try:
                return original_read(reader, target, max_bytes=max_bytes)
            finally:
                path.write_bytes(b"one")

        with patch.object(batch.CheckpointReader, "read", new=tamper_during_read):
            with self.assertRaises(chain.ChainUnknown):
                owner.checkpoint()
        self.assertTrue(consumed)
        self.assertEqual(path.read_bytes(), b"one")
        self.assertTrue(owner.uncertain)
        self.assertEqual(self.authority.read(), expected)

    def test_checkpoint_close_failure_latches_unknown_without_closing_external_head(self) -> None:
        owner = self.owner(read_policy=self.policy)
        owner.retain("fact.bin", b"fact")
        expected = owner.checkpoint()
        original_close = batch.CheckpointReader.close

        def close_then_fail(reader: batch.CheckpointReader) -> None:
            original_close(reader)
            raise OSError("fixture checkpoint close acknowledgement")

        with patch.object(batch.CheckpointReader, "close", new=close_then_fail):
            with self.assertRaises(chain.ChainUnknown):
                owner.checkpoint()
        self.assertTrue(owner.uncertain)
        self.assertEqual(self.authority.read(), expected)
        self.assertEqual(owner.read_prior("fact.bin").raw, b"fact")
        owner.close()
        self.assertEqual(self.authority.read(), expected)
