"""Host-only immutable-prefix failure fixtures; no Engine/provider/candidate work."""
from __future__ import annotations

from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from gossip_harness import candidate_checkpoint_chain_v1 as chain


class MemoryAuthority:
    """Explicit fixture authority, deliberately not a production default."""
    def __init__(self):
        self.head = None
        self.commits = 0
        self.mode = "ok"

    def read(self):
        return self.head

    def compare_and_set(self, expected, proposed):
        self.commits += 1
        if self.head != expected or self.mode == "reject":
            return False
        if self.mode == "raise-before":
            raise OSError("fixture anchor unavailable")
        self.head = proposed
        if self.mode == "raise-after":
            raise OSError("fixture lost acknowledgement")
        return True


class CandidateCheckpointChainV1Tests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="gossip-checkpoint-chain-v1-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.raw = self.root / "raw"
        self.delta = self.root / "external"
        self.authority = MemoryAuthority()
        self.context = {"execution_id": "fixture-a", "purpose": "harness_qualification",
                        "source_sha256": "1" * 64, "requirements_sha256": "2" * 64}
        self.opened = []
        self.addCleanup(self.close_all)

    def close_all(self):
        for item in self.opened:
            item.close()

    def create(self, **kwargs):
        item = chain.CheckpointChain.create(self.raw, self.delta, context=self.context,
                                           authority=self.authority, **kwargs)
        self.opened.append(item)
        return item

    def reopen(self, expected, **kwargs):
        item = chain.CheckpointChain.reopen(self.raw, self.delta, context=self.context,
                                           authority=self.authority, expected=expected, **kwargs)
        self.opened.append(item)
        return item

    def test_genesis_and_delta_are_domain_separated_and_exactly_bound(self):
        item = self.create()
        genesis = (self.delta / "genesis.json").read_bytes()
        prefix = item.commitment
        self.assertEqual(prefix.context_sha256,
                         hashlib.sha256((chain.PROTOCOL + "/genesis\0").encode() + genesis).hexdigest())
        self.assertEqual(prefix.head_sha256, prefix.context_sha256)
        value = json.loads(genesis)
        self.assertEqual(value["context"], self.context)
        self.assertEqual(value["raw_root"], str(self.raw))
        self.assertEqual(value["delta_root"], str(self.delta))
        self.assertEqual(prefix.sequence, 0)
        new = item.retain("intent.json", b'{"intent":true}')
        raw = (self.delta / "delta-00000001.json").read_bytes()
        delta = json.loads(raw)
        self.assertEqual(delta["previous_head_sha256"], prefix.head_sha256)
        self.assertEqual(delta["context_sha256"], prefix.context_sha256)
        self.assertEqual(delta["sequence"], 1)
        self.assertEqual(delta["bytes"], 15)
        self.assertEqual(new.head_sha256, hashlib.sha256((chain.PROTOCOL + "/delta\0").encode() + raw).hexdigest())
        self.assertNotEqual(new.head_sha256, hashlib.sha256(raw).hexdigest())
        self.assertEqual(new.external_bytes, len(genesis) + len(raw))
        self.assertEqual(self.authority.head, new)

    def test_reopen_requires_external_expected_and_all_exact_bytes(self):
        item = self.create()
        item.retain("first.bin", b"one")
        item.retain("empty.bin", b"")
        expected = item.commitment
        boundary = item.validate_boundary()
        item.close()
        reopened = self.reopen(expected)
        self.assertEqual(reopened.read("first.bin"), b"one")
        self.assertEqual(reopened.validate_boundary(), boundary)
        self.assertFalse(boundary.acceptance_authority)
        self.assertFalse(reopened.uncertain)

    def test_prefix_commitment_is_not_full_byte_validation(self):
        item = self.create()
        item.retain("first.bin", b"one")
        (self.raw / "first.bin").write_bytes(b"bad")
        prefix = item.retain("second.bin", b"two")
        self.assertEqual(prefix.sequence, 2)
        with self.assertRaises(chain.ChainUnknown):
            item.validate_boundary()
        self.assertTrue(item.uncertain)

    def test_consumed_tamper_with_restored_mtime_is_rejected(self):
        item = self.create()
        item.retain("first.bin", b"one")
        path = self.raw / "first.bin"
        before = path.stat()
        path.write_bytes(b"bad")
        os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
        with self.assertRaises(chain.ChainUnknown):
            item.read("first.bin")
        self.assertTrue(item.uncertain)

    def test_transient_unconsumed_tamper_is_explicitly_outside_boundary_guarantee(self):
        item = self.create()
        item.retain("first.bin", b"one")
        path = self.raw / "first.bin"
        path.write_bytes(b"bad")
        path.write_bytes(b"one")
        self.assertEqual(item.validate_boundary().commitment, item.commitment)

    def test_raw_failure_keeps_prior_fact_and_unknown_suffix_without_retry(self):
        item = self.create()
        item.retain("first.bin", b"one")
        prior = item.commitment
        original = item._write_new
        calls = []
        def fail_after_raw(index, name, raw):
            calls.append((index, name))
            original(index, name, raw)
            if index == 0:
                raise OSError("fixture raw fsync reply lost")
        with patch.object(item, "_write_new", side_effect=fail_after_raw):
            with self.assertRaises(chain.ChainUnknown):
                item.retain("unknown.bin", b"uncertain")
        self.assertEqual(calls, [(0, "unknown.bin")])
        self.assertEqual(item.commitment, prior)
        self.assertEqual(self.authority.head, prior)
        self.assertEqual((self.raw / "unknown.bin").read_bytes(), b"uncertain")
        fact = item.read_prior("first.bin")
        self.assertTrue(fact.prior_only)
        self.assertEqual(fact.raw, b"one")
        self.assertEqual(fact.commitment, prior)
        with self.assertRaises(chain.ChainUnknown):
            item.read_prior("unknown.bin")
        for operation in (lambda: item.retain("cleanup.json", b"{}", cleanup=True),
                          item.validate_boundary, lambda: item.read("first.bin"), item.require_current):
            with self.assertRaises(chain.ChainUnknown):
                operation()
        item.close()
        with self.assertRaises(chain.ChainError):
            self.reopen(prior)
        self.assertTrue((self.raw / "unknown.bin").exists())

    def test_delta_failure_does_not_publish_or_promote_attempted_file(self):
        item = self.create()
        item.retain("first.bin", b"one")
        prior = item.commitment
        original = item._write_new
        def fail_after_delta(index, name, raw):
            original(index, name, raw)
            if index == 1:
                raise OSError("fixture delta fsync reply lost")
        with patch.object(item, "_write_new", side_effect=fail_after_delta):
            with self.assertRaises(chain.ChainUnknown):
                item.retain("second.bin", b"two")
        self.assertEqual(self.authority.head, prior)
        self.assertTrue((self.delta / "delta-00000002.json").exists())
        self.assertEqual(item.read_prior("first.bin").raw, b"one")
        with self.assertRaises(chain.ChainUnknown):
            item.read_prior("second.bin")

    def test_anchor_committed_then_reply_lost_requires_explicit_exact_reopen(self):
        item = self.create()
        item.retain("first.bin", b"one")
        prior = item.commitment
        self.authority.mode = "raise-after"
        with self.assertRaises(chain.ChainUnknown):
            item.retain("second.bin", b"two")
        observed = self.authority.read()
        self.assertEqual(observed.sequence, 2)
        self.assertEqual(item.commitment, prior)
        self.assertEqual(item.read_prior("first.bin").raw, b"one")
        with self.assertRaises(chain.ChainUnknown):
            item.read_prior("second.bin")
        item.close()
        with self.assertRaises(chain.ChainError):
            self.reopen(prior)
        self.authority.mode = "ok"
        reopened = self.reopen(observed)
        self.assertEqual(reopened.read("second.bin"), b"two")
        self.assertEqual(reopened.validate_boundary().commitment, observed)
        # Reopening authenticates retained bytes; it executes nothing.
        self.assertEqual(self.authority.commits, 3)

    def test_anchor_rejection_and_raise_before_preserve_unknown_artifacts(self):
        for mode in ("reject", "raise-before"):
            with self.subTest(mode=mode):
                with tempfile.TemporaryDirectory(dir=self.root) as temp:
                    root = Path(temp)
                    authority = MemoryAuthority()
                    item = chain.CheckpointChain.create(root / "r", root / "d", context=self.context, authority=authority)
                    self.opened.append(item)
                    before = item.commitment
                    authority.mode = mode
                    with self.assertRaises(chain.ChainUnknown):
                        item.retain("one.bin", b"one")
                    self.assertEqual(authority.head, before)
                    self.assertEqual(item.commitment, before)
                    self.assertTrue((root / "r/one.bin").exists())
                    self.assertTrue((root / "d/delta-00000001.json").exists())
                    item.close()

    def test_raw_file_fsync_failure_never_relabels_success(self):
        item = self.create()
        item.retain("first.bin", b"one")
        prior = item.commitment
        with patch.object(chain.os, "fsync", side_effect=OSError("fixture fsync unavailable")):
            with self.assertRaises(chain.ChainUnknown):
                item.retain("second.bin", b"two")
        self.assertEqual(item.commitment, prior)
        self.assertEqual(self.authority.head, prior)
        self.assertTrue((self.raw / "second.bin").exists())
        self.assertEqual(item.read_prior("first.bin").raw, b"one")

    def test_foreign_current_head_blocks_read_append_and_boundary(self):
        item = self.create()
        item.retain("first.bin", b"one")
        prior = item.commitment
        self.authority.head = replace(prior, head_sha256="f" * 64)
        with self.assertRaises(chain.ChainUnknown):
            item.read("first.bin")
        self.assertEqual(item.read_prior("first.bin").raw, b"one")
        self.assertTrue(item.uncertain)

    def test_external_head_change_during_consumed_read_is_rejected(self):
        item = self.create()
        item.retain("first.bin", b"one")
        original = chain.stable.read
        def changing_read(path, **kwargs):
            raw = original(path, **kwargs)
            self.authority.head = replace(item.commitment, head_sha256="a" * 64)
            return raw
        with patch.object(chain.stable, "read", side_effect=changing_read):
            with self.assertRaises(chain.ChainUnknown):
                item.read("first.bin")

    def test_raw_and_delta_suffix_gap_and_tamper_fail_full_boundary(self):
        mutations = ("raw-extra", "raw-delete", "delta-extra", "delta-delete", "delta-edit", "genesis-edit")
        for mutation in mutations:
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory(dir=self.root) as temp:
                root = Path(temp); authority = MemoryAuthority()
                item = chain.CheckpointChain.create(root / "r", root / "d", context=self.context, authority=authority)
                self.opened.append(item)
                item.retain("one.bin", b"one")
                if mutation == "raw-extra": (root / "r/foreign.bin").write_bytes(b"foreign")
                if mutation == "raw-delete": (root / "r/one.bin").unlink()
                if mutation == "delta-extra": (root / "d/delta-00000002.json").write_bytes(b"{}")
                if mutation == "delta-delete": (root / "d/delta-00000001.json").unlink()
                if mutation == "delta-edit": (root / "d/delta-00000001.json").write_bytes(b"{}")
                if mutation == "genesis-edit": (root / "d/genesis.json").write_bytes(b"{}")
                with self.assertRaises(chain.ChainUnknown): item.validate_boundary()
                item.close()

    def test_local_chain_cannot_establish_freshness_after_external_rollback(self):
        item = self.create()
        old = item.retain("one.bin", b"one")
        current = item.retain("two.bin", b"two")
        item.close()
        self.authority.head = old
        with self.assertRaises(chain.ChainError): self.reopen(current)
        with self.assertRaises(chain.ChainError): self.reopen(old)
        self.assertTrue((self.raw / "two.bin").exists())

    def test_delta_reordering_and_cross_context_splicing_are_rejected(self):
        item = self.create()
        item.retain("one.bin", b"one")
        item.retain("two.bin", b"two")
        a = self.delta / "delta-00000001.json"; b = self.delta / "delta-00000002.json"
        ar, br = a.read_bytes(), b.read_bytes()
        a.write_bytes(br); b.write_bytes(ar)
        with self.assertRaises(chain.ChainUnknown): item.validate_boundary()
        item.close()
        a.write_bytes(ar); b.write_bytes(br)
        self.context = {**self.context, "execution_id": "another-execution"}
        with self.assertRaises(chain.ChainError): self.reopen(self.authority.head)

    def test_filename_and_duplicate_admission_do_not_write(self):
        item = self.create()
        item.retain("one.bin", b"one")
        before = sorted(self.raw.iterdir())
        for name in ("one.bin", "owner.lock", "../escape", "bad/name", "UPPER", "", "x" * 182):
            with self.subTest(name=name), self.assertRaises(chain.ChainError): item.retain(name, b"x")
        self.assertEqual(sorted(self.raw.iterdir()), before)
        self.assertFalse(item.uncertain)
        item.validate_boundary()

    def test_file_and_raw_reserves_allow_only_healthy_cleanup_then_fail_closed(self):
        limits = chain.Limits(max_files=3, cleanup_files=1, max_raw_bytes=8,
                              max_raw_file_bytes=4, cleanup_raw_bytes=2,
                              max_external_bytes=4096, cleanup_external_bytes=512)
        item = self.create(limits=limits)
        item.retain("one.bin", b"123")
        item.retain("two.bin", b"456")
        before = item.commitment
        with self.assertRaises(chain.BoundsExceeded): item.retain("three.bin", b"7")
        self.assertEqual(item.commitment, before)
        self.assertFalse(item.uncertain)
        item.retain("cleanup.bin", b"78", cleanup=True)
        with self.assertRaises(chain.BoundsExceeded): item.retain("four.bin", b"", cleanup=True)
        self.assertEqual(item.validate_boundary().commitment.raw_bytes, 8)

    def test_external_delta_reserve_is_enforced_before_raw_write(self):
        limits = chain.Limits(max_files=10, cleanup_files=1, max_raw_bytes=100,
                              max_raw_file_bytes=10, cleanup_raw_bytes=10,
                              max_external_bytes=2000, cleanup_external_bytes=600)
        item = self.create(limits=limits)
        while True:
            name = "ordinary-" + str(item.commitment.sequence) + ".bin"
            try: item.retain(name, b"a")
            except chain.BoundsExceeded: break
        self.assertFalse((self.raw / name).exists())
        prior = item.commitment
        item.retain("cleanup.bin", b"a", cleanup=True)
        self.assertGreater(item.commitment.external_bytes, prior.external_bytes)
        item.validate_boundary()

    def test_owner_lock_and_foreign_thread_are_exclusive(self):
        item = self.create()
        item.retain("one.bin", b"one")
        with self.assertRaises(chain.ChainError): self.reopen(item.commitment)
        errors = []
        def other_thread():
            for operation in (item.require_current, item.close, lambda: item.read_prior("one.bin")):
                try: operation()
                except chain.ChainError as error: errors.append(str(error))
        thread = threading.Thread(target=other_thread); thread.start(); thread.join()
        self.assertEqual(len(errors), 3)
        self.assertEqual(item.read("one.bin"), b"one")
        item.close(); item.close()
        with self.assertRaises(chain.ChainError): item.read("one.bin")

    def test_symlink_leaf_and_owner_lock_replacement_are_rejected(self):
        item = self.create()
        item.retain("one.bin", b"one")
        elsewhere = self.root / "elsewhere"; elsewhere.write_bytes(b"one")
        (self.raw / "one.bin").unlink(); (self.raw / "one.bin").symlink_to(elsewhere)
        with self.assertRaises(chain.ChainUnknown): item.read_prior("one.bin")
        item.close()
        (self.raw / "one.bin").unlink(); (self.raw / "one.bin").write_bytes(b"one")
        item = self.reopen(self.authority.head)
        (self.raw / "owner.lock").unlink(); (self.raw / "owner.lock").write_bytes(b"")
        with self.assertRaises(chain.ChainUnknown): item.require_current()

    def test_canonical_disjoint_roots_and_context_limits_are_required(self):
        link = self.root / "link"; link.symlink_to(self.root, target_is_directory=True)
        for raw, delta in ((link / "r", self.delta), (self.raw, self.raw / "child"), (self.raw, self.raw)):
            with self.subTest(raw=str(raw), delta=str(delta)), self.assertRaises(chain.ChainError):
                chain.CheckpointChain.create(raw, delta, context=self.context, authority=self.authority)
        with self.assertRaises(chain.ChainError):
            chain.CheckpointChain.create(self.raw, self.delta, context={"huge": "x" * 40000}, authority=self.authority)
        self.assertFalse(self.raw.exists())

    def test_invalid_limits_and_coerced_commitment_counters_are_rejected(self):
        for kwargs in ({"max_files": True}, {"cleanup_files": -1}, {"max_files": 0},
                       {"max_raw_file_bytes": 33 * 1024 * 1024}, {"max_context_bytes": 0}):
            with self.subTest(kwargs=kwargs), self.assertRaises(chain.ChainError): chain.Limits(**kwargs)
        item = self.create()
        for invalid in ({"sequence": True}, {"raw_bytes": -1}, {"raw_file_count": 1}, {"head_sha256": "x"}):
            with self.subTest(invalid=invalid), self.assertRaises(chain.ChainError): replace(item.commitment, **invalid)

    def test_empty_and_binary_raw_bytes_survive_exactly(self):
        item = self.create()
        item.retain("empty.bin", b"")
        raw = bytes(range(256))
        item.retain("binary.bin", raw)
        self.assertEqual(item.read("empty.bin"), b"")
        self.assertEqual(item.read("binary.bin"), raw)
        self.assertEqual(item.validate_boundary().commitment.raw_bytes, 256)

    def test_append_and_point_read_do_not_hide_full_inventory_scans(self):
        item = self.create()
        with patch.object(item, "_verify_all", side_effect=AssertionError("no full scan during append")):
            for n in range(12): item.retain(f"record-{n:02d}.bin", bytes([n]))
            self.assertEqual(item.read("record-11.bin"), b"\x0b")
        checkpoints = list(self.delta.glob("delta-*.json"))
        self.assertEqual(len(checkpoints), 12)
        self.assertTrue(all(p.stat().st_size <= chain._DELTA_LIMIT for p in checkpoints))
        with patch.object(item, "_verify_all", wraps=item._verify_all) as verify:
            item.validate_boundary()
            self.assertEqual(verify.call_count, 1)
        self.assertEqual(len(item._files), 12)
        self.assertEqual(len(item._deltas), 12)

    def test_prior_fact_does_not_accept_modified_committed_bytes(self):
        item = self.create()
        item.retain("one.bin", b"one")
        self.authority.mode = "reject"
        with self.assertRaises(chain.ChainUnknown): item.retain("two.bin", b"two")
        (self.raw / "one.bin").write_bytes(b"bad")
        with self.assertRaises(chain.ChainUnknown): item.read_prior("one.bin")

    def test_genesis_anchor_failure_preserves_roots_for_explicit_recovery(self):
        self.authority.mode = "raise-after"
        with self.assertRaises(chain.ChainError): self.create()
        observed = self.authority.read()
        self.assertEqual(observed.sequence, 0)
        self.assertTrue((self.delta / "genesis.json").exists())
        self.authority.mode = "ok"
        item = self.reopen(observed)
        self.assertEqual(item.validate_boundary().commitment, observed)

    def test_actual_raw_during_read_mutation_is_rejected_by_stable_reader(self):
        item = self.create()
        item.retain("one.bin", b"one")
        original = chain.stable.os.read
        touched = False
        def mutate(fd, count):
            nonlocal touched
            raw = original(fd, count)
            if raw and not touched:
                touched = True
                (self.raw / "one.bin").write_bytes(b"bad")
            return raw
        with patch.object(chain.stable.os, "read", side_effect=mutate):
            with self.assertRaises(chain.ChainUnknown): item.read("one.bin")
        self.assertTrue(touched)

    def test_inherited_fork_identity_cannot_use_parent_chain_or_release_owner(self):
        item = self.create()
        item.retain("one.bin", b"one")
        with patch.object(chain.os, "getpid", return_value=os.getpid() + 1):
            for operation in (item.require_current, item.close, item.validate_boundary,
                              lambda: item.retain("two.bin", b"two"),
                              lambda: item.read("one.bin"), lambda: item.read_prior("one.bin")):
                with self.assertRaises(chain.ChainError): operation()
        self.assertFalse(item.uncertain)
        self.assertEqual(item.read("one.bin"), b"one")

    def test_create_never_implicitly_creates_unflushed_ancestors(self):
        missing = self.root / "missing"
        with self.assertRaisesRegex(chain.ChainError, "parent"):
            chain.CheckpointChain.create(missing / "raw", self.delta, context=self.context,
                                         authority=self.authority)
        self.assertFalse(missing.exists())
        self.assertFalse(self.delta.exists())
        self.assertIsNone(self.authority.read())

    def test_raw_directory_fsync_failure_preserves_unanchored_suffix(self):
        item = self.create()
        prior = item.commitment
        original = chain.os.fsync
        def fail_directory(fd):
            if fd == item._root_fds[0]:
                raise OSError("fixture directory fsync unavailable")
            original(fd)
        with patch.object(chain.os, "fsync", side_effect=fail_directory):
            with self.assertRaises(chain.ChainUnknown): item.retain("one.bin", b"one")
        self.assertEqual((self.raw / "one.bin").read_bytes(), b"one")
        self.assertEqual(item.commitment, prior)
        self.assertEqual(self.authority.head, prior)
        self.assertFalse((self.delta / "delta-00000001.json").exists())

    def test_partial_raw_bytes_remain_unknown_and_are_not_deleted_or_retried(self):
        item = self.create()
        prior = item.commitment
        def partial_write(index, name, raw):
            self.assertEqual(index, 0)
            with (self.raw / name).open("xb") as stream:
                stream.write(raw[:1])
                stream.flush()
            raise OSError("fixture interrupted raw write")
        with patch.object(item, "_write_new", side_effect=partial_write):
            with self.assertRaises(chain.ChainUnknown): item.retain("one.bin", b"one")
        self.assertEqual((self.raw / "one.bin").read_bytes(), b"o")
        self.assertEqual(item.commitment, prior)
        self.assertEqual(self.authority.head, prior)
        with self.assertRaises(chain.ChainUnknown): item.read_prior("one.bin")
        item.close()
        with self.assertRaises(chain.ChainError): self.reopen(prior)

    def test_anchor_readback_failure_does_not_promote_attempted_suffix(self):
        item = self.create()
        item.retain("one.bin", b"one")
        prior = item.commitment
        original = self.authority.read
        def unavailable_new_head():
            value = original()
            assert value is not None
            if value.sequence == 2:
                raise OSError("fixture anchor readback unavailable")
            return value
        with patch.object(self.authority, "read", side_effect=unavailable_new_head):
            with self.assertRaises(chain.ChainUnknown): item.retain("two.bin", b"two")
        observed = self.authority.read()
        assert observed is not None
        self.assertEqual(observed.sequence, 2)
        self.assertEqual(item.commitment, prior)
        self.assertEqual(item.read_prior("one.bin").raw, b"one")
        with self.assertRaises(chain.ChainUnknown): item.read_prior("two.bin")

    def test_mutated_frozen_limits_rejected_and_valid_caller_limits_detached(self):
        invalid = chain.Limits()
        object.__setattr__(invalid, "max_files", True)
        with self.assertRaises(chain.ChainError): self.create(limits=invalid)
        self.assertFalse(self.raw.exists())
        self.assertFalse(self.delta.exists())
        self.assertIsNone(self.authority.head)
        valid = chain.Limits()
        item = self.create(limits=valid)
        object.__setattr__(valid, "max_files", 0)
        self.assertEqual(item.limits.max_files, 16384)
        item.retain("one.bin", b"one")
        item.validate_boundary()

    def test_result_data_cannot_constructor_claim_acceptance_or_nonprior_fact(self):
        item = self.create()
        item.retain("one.bin", b"one")
        boundary = item.validate_boundary()
        fact = item.read_prior("one.bin")
        with self.assertRaises(TypeError):
            chain.BoundaryValidation(item.commitment, boundary.inventory_sha256, acceptance_authority=True)  # type: ignore[call-arg]
        with self.assertRaises(TypeError):
            chain.PriorFact("one.bin", b"one", fact.sha256, item.commitment, prior_only=False)  # type: ignore[call-arg]
        with self.assertRaises(TypeError):
            chain.PriorFact("one.bin", b"one", fact.sha256, item.commitment, acceptance_authority=True)  # type: ignore[call-arg]
        self.assertFalse(boundary.acceptance_authority)
        self.assertFalse(fact.acceptance_authority)
        self.assertTrue(fact.prior_only)

    def test_has_is_healthy_prefix_membership_without_full_inventory(self):
        item = self.create()
        item.retain("one.bin", b"one")
        with patch.object(item, "_verify_all", side_effect=AssertionError("membership is not full scan")):
            self.assertTrue(item.has("one.bin"))
            self.assertFalse(item.has("absent.bin"))
            (self.raw / "foreign.bin").write_bytes(b"foreign")
            self.assertFalse(item.has("foreign.bin"))
        with self.assertRaises(chain.ChainUnknown): item.validate_boundary()
        with self.assertRaises(chain.ChainUnknown): item.has("one.bin")

    def test_has_rejects_invalid_foreign_process_and_head_without_authority(self):
        item = self.create()
        item.retain("one.bin", b"one")
        for invalid in ("../x", "owner.lock", "UPPER", ""):
            with self.assertRaises(chain.ChainError): item.has(invalid)
        self.assertFalse(item.uncertain)
        with patch.object(chain.os, "getpid", return_value=os.getpid() + 1):
            with self.assertRaises(chain.ChainError): item.has("one.bin")
        self.assertFalse(item.uncertain)
        self.authority.head = replace(item.commitment, head_sha256="f" * 64)
        with self.assertRaises(chain.ChainUnknown): item.has("one.bin")
        self.assertTrue(item.uncertain)

    def test_has_is_unavailable_after_consumed_read_failure(self):
        item = self.create()
        item.retain("one.bin", b"one")
        (self.raw / "one.bin").write_bytes(b"bad")
        with self.assertRaises(chain.ChainUnknown): item.read("one.bin")
        with self.assertRaises(chain.ChainUnknown): item.has("one.bin")
