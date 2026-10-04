"""Isolated host-only point-position checks; no candidate, Engine or provider calls."""
from __future__ import annotations

from dataclasses import replace
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from gossip_harness import candidate_checkpoint_chain_v1 as chain


class MemoryAuthority:
    def __init__(self):
        self.head = None
        self.commits = 0
        self.lose_ack = False

    def read(self):
        return self.head

    def compare_and_set(self, expected, proposed):
        self.commits += 1
        if self.head != expected:
            return False
        self.head = proposed
        if self.lose_ack:
            raise OSError("fixture lost acknowledgement")
        return True


class CandidateCheckpointPositionV1Tests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="gossip-checkpoint-position-v1-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.raw = self.root / "raw"
        self.delta = self.root / "external"
        self.authority = MemoryAuthority()
        self.context = {"execution_id": "position-fixture", "purpose": "harness_qualification"}
        self.item = chain.CheckpointChain.create(self.raw, self.delta, context=self.context,
                                                authority=self.authority)
        self.addCleanup(self.item.close)

    def test_append_order_is_one_based_and_independent_of_filename_sort(self):
        self.item.retain("z-stop.json", b"stopped")
        self.item.retain("a-seal.json", b"sealed")
        self.item.retain("m-private.json", b"private")
        self.assertEqual(self.item.position("z-stop.json"), 1)
        self.assertEqual(self.item.position("a-seal.json"), 2)
        self.assertEqual(self.item.position("m-private.json"), 3)

    def test_reopen_preserves_order_and_later_append_extends_it(self):
        self.item.retain("z-stop.json", b"stopped")
        self.item.retain("a-seal.json", b"sealed")
        expected = self.item.commitment
        self.item.close()
        reopened = chain.CheckpointChain.reopen(self.raw, self.delta, context=self.context,
                                               authority=self.authority, expected=expected)
        self.addCleanup(reopened.close)
        self.assertEqual(reopened.position("z-stop.json"), 1)
        self.assertEqual(reopened.position("a-seal.json"), 2)
        reopened.retain("b-private.json", b"private")
        self.assertEqual(reopened.position("b-private.json"), 3)

    def test_point_lookup_authenticates_exact_two_files_without_writes_or_full_scan(self):
        self.item.retain("z-stop.json", b"stopped")
        self.item.retain("a-seal.json", b"sealed")
        before = {str(path): path.read_bytes() for root in (self.raw, self.delta)
                  for path in root.iterdir()}
        commits = self.authority.commits
        prefix = self.item.commitment
        with patch.object(chain.stable, "read", wraps=chain.stable.read) as read:
            with patch.object(self.item, "_verify_all", side_effect=AssertionError("full scan")):
                self.assertEqual(self.item.position("a-seal.json"), 2)
        self.assertEqual([call.args[0] for call in read.call_args_list],
                         [self.raw / "a-seal.json", self.delta / "delta-00000002.json"])
        self.assertEqual({str(path): path.read_bytes() for root in (self.raw, self.delta)
                          for path in root.iterdir()}, before)
        self.assertEqual(self.item.commitment, prefix)
        self.assertEqual(self.authority.commits, commits)

    def test_unknown_record_is_rejected(self):
        with self.assertRaises(chain.ChainUnknown):
            self.item.position("missing.json")
        self.assertTrue(self.item.uncertain)

    def test_unacknowledged_disk_suffix_has_no_position(self):
        self.item.retain("first.json", b"first")
        (self.raw / "suffix.json").write_bytes(b"unacknowledged")
        with self.assertRaises(chain.ChainUnknown):
            self.item.position("suffix.json")
        self.assertEqual(self.item.commitment.sequence, 1)

    def test_raw_tamper_with_same_length_and_restored_mtime_is_rejected(self):
        self.item.retain("first.json", b"first")
        path = self.raw / "first.json"
        before = path.stat()
        path.write_bytes(b"other")
        os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
        with self.assertRaises(chain.ChainUnknown):
            self.item.position("first.json")
        self.assertTrue(self.item.uncertain)

    def test_substituted_delta_is_rejected_even_when_raw_is_unchanged(self):
        self.item.retain("first.json", b"same")
        self.item.retain("second.json", b"same")
        (self.delta / "delta-00000001.json").write_bytes(
            (self.delta / "delta-00000002.json").read_bytes())
        with self.assertRaises(chain.ChainUnknown):
            self.item.position("first.json")
        self.assertTrue(self.item.uncertain)

    def test_missing_delta_is_rejected(self):
        self.item.retain("first.json", b"first")
        (self.delta / "delta-00000001.json").unlink()
        with self.assertRaises(chain.ChainUnknown):
            self.item.position("first.json")

    def test_lost_ack_blocks_prior_and_attempted_positions_until_explicit_exact_reopen(self):
        self.item.retain("first.json", b"first")
        prior = self.item.commitment
        self.authority.lose_ack = True
        with self.assertRaises(chain.ChainUnknown):
            self.item.retain("second.json", b"second")
        committed_but_unacknowledged = self.authority.head
        self.assertEqual(self.item.commitment, prior)
        for name in ("first.json", "second.json"):
            with self.assertRaises(chain.ChainUnknown):
                self.item.position(name)
        self.item.close()
        with self.assertRaises(chain.ChainError):
            chain.CheckpointChain.reopen(self.raw, self.delta, context=self.context,
                                         authority=self.authority, expected=prior)
        reopened = chain.CheckpointChain.reopen(self.raw, self.delta, context=self.context,
                                               authority=self.authority,
                                               expected=committed_but_unacknowledged)
        self.addCleanup(reopened.close)
        self.assertEqual(reopened.position("first.json"), 1)
        self.assertEqual(reopened.position("second.json"), 2)

    def test_changed_external_head_rejects_before_any_raw_read(self):
        self.item.retain("first.json", b"first")
        self.authority.head = replace(self.item.commitment, head_sha256="f" * 64)
        with patch.object(chain.stable, "read", side_effect=AssertionError("must not read")):
            with self.assertRaises(chain.ChainUnknown):
                self.item.position("first.json")
        self.assertTrue(self.item.uncertain)

    def test_external_head_changed_during_delta_read_is_rejected(self):
        self.item.retain("first.json", b"first")
        original_read = chain.stable.read

        def change_after_read(path, *, max_bytes):
            raw = original_read(path, max_bytes=max_bytes)
            if path == self.delta / "delta-00000001.json":
                self.authority.head = replace(self.item.commitment, head_sha256="f" * 64)
            return raw

        with patch.object(chain.stable, "read", side_effect=change_after_read):
            with self.assertRaises(chain.ChainUnknown):
                self.item.position("first.json")
        self.assertTrue(self.item.uncertain)

    def test_actual_closed_instance_rejects_position(self):
        self.item.retain("first.json", b"first")
        self.item.close()
        with self.assertRaises(chain.ChainError):
            self.item.position("first.json")

    def test_foreign_thread_and_process_identity_reject_without_poisoning_owner(self):
        self.item.retain("first.json", b"first")
        errors = []

        def foreign_thread():
            try:
                self.item.position("first.json")
            except BaseException as error:
                errors.append(error)

        worker = threading.Thread(target=foreign_thread)
        worker.start()
        worker.join(timeout=5)
        self.assertFalse(worker.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], chain.ChainError)
        current_pid = os.getpid()
        with patch.object(chain.os, "getpid", return_value=current_pid + 1):
            with self.assertRaises(chain.ChainError):
                self.item.position("first.json")
        self.assertFalse(self.item.uncertain)
        self.assertEqual(self.item.position("first.json"), 1)


if __name__ == "__main__":
    unittest.main()
