from __future__ import annotations

from dataclasses import asdict, replace
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

from gossip_harness import candidate_checkpoint_chain_v1 as chain
from gossip_harness import candidate_checkpoint_head_v1 as head


class CandidateCheckpointHeadV1Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="gossip-external-head-")
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name).resolve()
        self.root = self.base / "anchor"
        self.roots = (self.base / "raw", self.base / "deltas")
        self.genesis = chain.PrefixCommitment("a" * 64, 0, "b" * 64, 0, 0, 100)
        self.next = chain.PrefixCommitment("a" * 64, 1, "c" * 64, 1, 7, 400)

    def owner(self):
        owner = head.ExternalHead.create(self.root, journal_roots=self.roots)
        self.addCleanup(owner.close)
        return owner

    def initialized(self):
        owner = self.owner()
        self.assertIs(owner.compare_and_set(None, self.genesis), True)
        return owner

    def test_exact_cas_and_mismatch_without_write(self):
        owner = self.initialized()
        before = (self.root / "head.json").read_bytes()
        self.assertIs(owner.compare_and_set(None, self.next), False)
        self.assertEqual((self.root / "head.json").read_bytes(), before)
        self.assertFalse(owner.uncertain)
        self.assertIs(owner.compare_and_set(self.genesis, self.next), True)
        self.assertEqual(owner.read(), self.next)
        self.assertEqual(set(p.name for p in self.root.iterdir()), {"head.lock", "head.json"})

    def test_real_process_reopen_uses_independent_expected_prefix(self):
        owner = self.initialized()
        owner.compare_and_set(self.genesis, self.next)
        owner.close()
        code = """import json,sys
from pathlib import Path
from gossip_harness.candidate_checkpoint_chain_v1 import PrefixCommitment
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
base=Path(sys.argv[1]); expected=PrefixCommitment(**json.loads(sys.argv[2]))
owner=ExternalHead.reopen(base/'anchor',journal_roots=(base/'raw',base/'deltas'),expected=expected)
assert owner.read()==expected
owner.close()
"""
        result = subprocess.run([sys.executable, "-c", code, str(self.base), json.dumps(asdict(self.next))],
                                capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_wrong_expected_head_fails_without_rewriting_original(self):
        owner = self.initialized()
        owner.compare_and_set(self.genesis, self.next)
        owner.close()
        before = (self.root / "head.json").read_bytes()
        with self.assertRaises(head.HeadUnknown):
            head.ExternalHead.reopen(self.root, journal_roots=self.roots, expected=self.genesis)
        self.assertEqual((self.root / "head.json").read_bytes(), before)
        reopened = head.ExternalHead.reopen(self.root, journal_roots=self.roots, expected=self.next)
        reopened.close()

    def test_lifetime_exclusion_in_same_and_other_processes(self):
        owner = self.initialized()
        with self.assertRaises(BlockingIOError):
            head.ExternalHead.reopen(self.root, journal_roots=self.roots, expected=self.genesis)
        code = """import json,sys
from pathlib import Path
from gossip_harness.candidate_checkpoint_chain_v1 import PrefixCommitment
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
base=Path(sys.argv[1])
try: ExternalHead.reopen(base/'anchor',journal_roots=(base/'raw',base/'deltas'),expected=PrefixCommitment(**json.loads(sys.argv[2])))
except BlockingIOError: pass
else: raise AssertionError('concurrent owner admitted')
"""
        result = subprocess.run([sys.executable, "-c", code, str(self.base), json.dumps(asdict(self.genesis))],
                                capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(owner.read(), self.genesis)

    def test_thread_and_fork_cannot_use_or_release_parent_owner(self):
        owner = self.initialized()
        errors = []
        def other_thread():
            for action in (owner.read, owner.close):
                try: action()
                except head.HeadError: errors.append(True)
        thread = threading.Thread(target=other_thread)
        thread.start(); thread.join(5)
        self.assertEqual(errors, [True, True])
        pid = os.fork()
        if pid == 0:
            try:
                owner.read()
            except head.HeadError:
                try: owner.close()
                except head.HeadError: os._exit(0)
            os._exit(1)
        _, status = os.waitpid(pid, 0)
        self.assertEqual(os.waitstatus_to_exitcode(status), 0)
        self.assertEqual(owner.read(), self.genesis)

    def test_foreign_context_skips_and_counter_reversal_rejected_before_write(self):
        owner = self.initialized()
        values = [replace(self.next, context_sha256="d" * 64),
                  replace(self.next, sequence=2, raw_file_count=2),
                  replace(self.next, external_bytes=100),
                  replace(self.next, head_sha256=self.genesis.head_sha256)]
        for value in values:
            with self.subTest(value=value), self.assertRaises(head.HeadError):
                owner.compare_and_set(self.genesis, value)
        self.assertFalse(owner.uncertain)
        self.assertEqual(owner.read(), self.genesis)

    def test_invalid_genesis_and_corrupted_dataclass_cannot_enter_store(self):
        owner = self.owner()
        with self.assertRaises(head.HeadError): owner.compare_and_set(None, self.next)
        broken = replace(self.genesis)
        object.__setattr__(broken, "sequence", False)
        with self.assertRaises(head.HeadError): owner.compare_and_set(None, broken)
        self.assertIsNone(owner.read())

    def test_overlap_and_link_aliases_cannot_hide_anchor_in_journal(self):
        for root in (self.roots[0], self.roots[0] / "anchor", self.base):
            with self.subTest(root=root), self.assertRaises(head.HeadError):
                head.ExternalHead.create(root, journal_roots=self.roots)
        (self.base / "alias").symlink_to(self.base, target_is_directory=True)
        with self.assertRaises(head.HeadError):
            head.ExternalHead.create(self.root, journal_roots=(self.base / "alias" / "raw", self.roots[1]))

    def test_create_never_overwrites_and_reopen_requires_private_directory(self):
        owner = self.initialized()
        before = (self.root / "head.json").read_bytes()
        with self.assertRaises(FileExistsError): self.owner()
        owner.close()
        self.root.chmod(0o755)
        with self.assertRaises(head.HeadError):
            head.ExternalHead.reopen(self.root, journal_roots=self.roots, expected=self.genesis)
        self.assertEqual((self.root / "head.json").read_bytes(), before)

    def test_observed_head_rollback_latches_uncertainty(self):
        owner = self.initialized()
        old = (self.root / "head.json").read_bytes()
        owner.compare_and_set(self.genesis, self.next)
        (self.root / "head.json").write_bytes(old)
        with self.assertRaises(head.HeadUnknown): owner.read()
        self.assertTrue(owner.uncertain)
        with self.assertRaises(head.HeadUnknown): owner.compare_and_set(self.genesis, self.next)

    def test_noncanonical_unknown_or_duplicate_fields_are_rejected(self):
        owner = self.initialized()
        original = (self.root / "head.json").read_bytes()
        owner.close()
        value = json.loads(original)
        variants = [json.dumps(value).encode(), original[:-1] + b',"protocol":"duplicate"}',
                    head._encoded({**value, "extra": 1}),
                    head._encoded({**value, "commitment": {**value["commitment"], "extra": 1}})]
        for raw in variants:
            (self.root / "head.json").write_bytes(raw)
            with self.subTest(raw=raw), self.assertRaises(head.HeadUnknown):
                head.ExternalHead.reopen(self.root, journal_roots=self.roots, expected=self.genesis)

    def test_pending_suffix_is_preserved_and_never_retried(self):
        owner = self.initialized()
        pending = self.root / "pending.json"
        pending.write_bytes(b"interrupted")
        with self.assertRaises(head.HeadUnknown): owner.read()
        self.assertEqual(pending.read_bytes(), b"interrupted")
        owner.close()
        with self.assertRaises(head.HeadUnknown):
            head.ExternalHead.reopen(self.root, journal_roots=self.roots, expected=self.genesis)

    def test_head_symlink_and_hardlink_are_rejected(self):
        owner = self.initialized()
        path = self.root / "head.json"
        saved = self.base / "saved.json"
        path.rename(saved)
        path.symlink_to(saved)
        with self.assertRaises(head.HeadUnknown): owner.read()
        owner.close()
        path.unlink(); os.link(saved, path)
        with self.assertRaises(head.HeadUnknown):
            head.ExternalHead.reopen(self.root, journal_roots=self.roots, expected=self.genesis)

    def test_renamed_ancestor_cannot_redirect_live_owner(self):
        owner = self.initialized()
        moved = self.base / "moved"
        self.root.rename(moved)
        self.root.mkdir(mode=0o700)
        with self.assertRaises(head.HeadUnknown): owner.read()
        self.assertTrue((moved / "head.json").is_file())

    def test_failed_write_preserves_partial_artifact_and_poisoned_owner(self):
        owner = self.initialized()
        original = (self.root / "head.json").read_bytes()
        with patch.object(head.os, "write", side_effect=OSError("injected storage failure")):
            with self.assertRaises(head.HeadUnknown): owner.compare_and_set(self.genesis, self.next)
        self.assertTrue(owner.uncertain)
        self.assertTrue((self.root / "pending.json").is_file())
        self.assertEqual((self.root / "head.json").read_bytes(), original)
        with self.assertRaises(head.HeadUnknown): owner.compare_and_set(self.genesis, self.next)

    def test_lost_directory_sync_ack_never_reports_success_or_retries(self):
        owner = self.initialized()
        original_sync = head.os.fsync
        def fail_directory(fd):
            if fd == owner._dir: raise OSError("lost directory fsync acknowledgement")
            return original_sync(fd)
        with patch.object(head.os, "fsync", side_effect=fail_directory):
            with self.assertRaises(head.HeadUnknown): owner.compare_and_set(self.genesis, self.next)
        self.assertTrue(owner.uncertain)
        self.assertEqual(json.loads((self.root / "head.json").read_bytes())["commitment"], asdict(self.next))
        with self.assertRaises(head.HeadUnknown): owner.read()
        owner.close()
        # Caller supplies the exact independently retained proposed commitment;
        # the old owner did not recover itself or authorize any candidate retry.
        reopened = head.ExternalHead.reopen(self.root, journal_roots=self.roots, expected=self.next)
        self.assertEqual(reopened.read(), self.next)
        reopened.close()

    def test_interrupt_after_write_attempt_latches_uncertainty(self):
        owner = self.initialized()
        with patch.object(owner, "_publish", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt): owner.compare_and_set(self.genesis, self.next)
        self.assertTrue(owner.uncertain)
        with self.assertRaises(head.HeadUnknown): owner.read()

    def test_reopen_sync_failure_retains_bytes_and_releases_ownership(self):
        owner = self.initialized()
        owner.close()
        before = (self.root / "head.json").read_bytes()
        with patch.object(head.os, "fsync", side_effect=OSError("recovery durability unavailable")):
            with self.assertRaises(head.HeadUnknown):
                head.ExternalHead.reopen(self.root, journal_roots=self.roots, expected=self.genesis)
        self.assertEqual((self.root / "head.json").read_bytes(), before)
        reopened = head.ExternalHead.reopen(self.root, journal_roots=self.roots, expected=self.genesis)
        reopened.close()

    def test_reentrant_cas_read_and_close_cannot_interleave_with_publication(self):
        owner = self.initialized()
        original_publish = owner._publish
        nested = replace(self.next, head_sha256="d" * 64)
        rejected = []
        def attempt_reentrancy(value):
            for action in (lambda: owner.compare_and_set(self.genesis, nested), owner.read, owner.close):
                with self.assertRaises(head.HeadError): action()
                rejected.append(True)
            original_publish(value)
        with patch.object(owner, "_publish", side_effect=attempt_reentrancy):
            self.assertIs(owner.compare_and_set(self.genesis, self.next), True)
        self.assertEqual(rejected, [True, True, True])
        self.assertEqual(owner.read(), self.next)

    def test_fork_during_constructor_cleanup_never_unlocks_parent(self):
        original_read = head.ExternalHead.read
        child = False
        def fork_before_final_read(owner):
            nonlocal child
            pid = os.fork()
            if pid == 0:
                child = True
                return original_read(owner)  # Raises; _open closes inherited FDs.
            _, status = os.waitpid(pid, 0)
            self.assertEqual(os.waitstatus_to_exitcode(status), 0)
            return original_read(owner)
        try:
            with patch.object(head.ExternalHead, "read", fork_before_final_read):
                owner = self.owner()
        except BaseException as error:
            if child: os._exit(0 if isinstance(error, head.HeadError) else 2)
            raise
        if child: os._exit(3)
        owner.compare_and_set(None, self.genesis)
        with self.assertRaises(BlockingIOError):
            head.ExternalHead.reopen(self.root, journal_roots=self.roots, expected=self.genesis)
        self.assertEqual(owner.read(), self.genesis)

    def test_real_chain_round_trip_and_raw_authentication(self):
        authority = self.owner()
        context = {"purpose": "harness_qualification", "source_sha256": "d" * 64}
        instance = chain.CheckpointChain.create(*self.roots, context=context, authority=authority)
        self.addCleanup(instance.close)
        instance.retain("intent.json", b'{"action":"observe"}')
        expected = instance.retain("response.raw", b"original bytes")
        self.assertEqual(instance.validate_boundary().commitment, expected)
        instance.close(); authority.close()
        reopened_authority = head.ExternalHead.reopen(self.root, journal_roots=self.roots, expected=expected)
        self.addCleanup(reopened_authority.close)
        reopened = chain.CheckpointChain.reopen(*self.roots, context=context, authority=reopened_authority,
                                                expected=expected)
        self.addCleanup(reopened.close)
        self.assertEqual(reopened.read("response.raw"), b"original bytes")
        (self.roots[0] / "response.raw").write_bytes(b"changed bytes!")
        with self.assertRaises(chain.ChainError): reopened.read("response.raw")

    def test_chain_prior_facts_survive_lost_anchor_ack_without_current_authority(self):
        authority = self.owner()
        instance = chain.CheckpointChain.create(*self.roots, context={"purpose": "control"}, authority=authority)
        self.addCleanup(instance.close)
        prior = instance.retain("first.raw", b"known failure")
        publish = authority._publish
        def lose_ack(proposed):
            publish(proposed)
            raise OSError("acknowledgement unavailable")
        with patch.object(authority, "_publish", side_effect=lose_ack):
            with self.assertRaises(chain.ChainError): instance.retain("second.raw", b"uncertain suffix")
        self.assertTrue(instance.uncertain)
        fact = instance.read_prior("first.raw")
        self.assertEqual(fact.raw, b"known failure")
        self.assertEqual(fact.commitment, prior)
        self.assertTrue(fact.prior_only)
        self.assertFalse(fact.acceptance_authority)
        with self.assertRaises(chain.ChainError): instance.read_prior("second.raw")
        with self.assertRaises(chain.ChainError): instance.validate_boundary()
        with self.assertRaises(chain.ChainError): instance.retain("retry.raw", b"must not happen")
        self.assertTrue((self.roots[0] / "second.raw").is_file())

    def test_external_storage_grows_by_delta_not_full_prefix_copies(self):
        authority = self.owner()
        instance = chain.CheckpointChain.create(*self.roots, context={"purpose": "bounded-storage"}, authority=authority)
        self.addCleanup(instance.close)
        baseline = instance.commitment.external_bytes
        for index in range(40): instance.retain(f"record-{index:04d}.raw", b"x" * 256)
        middle = instance.commitment.external_bytes
        for index in range(40, 80): instance.retain(f"record-{index:04d}.raw", b"x" * 256)
        final = instance.validate_boundary().commitment
        self.assertLessEqual(final.external_bytes - middle, (middle - baseline) * 1.02)
        self.assertLess((self.root / "head.json").stat().st_size, head.MAX_HEAD_BYTES)
        raw_bytes = sum(p.stat().st_size for p in self.roots[0].iterdir())
        delta_bytes = sum(p.stat().st_size for p in self.roots[1].iterdir())
        self.assertEqual(raw_bytes, final.raw_bytes)
        self.assertEqual(delta_bytes, final.external_bytes)
        self.assertEqual(final.raw_file_count, 80)


if __name__ == "__main__":
    unittest.main()
