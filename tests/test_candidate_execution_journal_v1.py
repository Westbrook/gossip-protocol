"""Actual durable-head/adapter host fixtures; no candidate or Docker execution."""
from __future__ import annotations

from dataclasses import dataclass, replace
import json
from pathlib import Path
import tempfile
import unittest
from typing import Any
from unittest.mock import patch

from gossip_harness import candidate_checkpoint_chain_v1 as chain
from gossip_harness import candidate_checkpoint_head_v1 as head
from gossip_harness import candidate_execution_journal_v1 as journal


@dataclass(frozen=True)
class Declaration:
    purpose: str
    steps: tuple[tuple[str, int], ...]


class CandidateExecutionJournalV1Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="gossip-owner-journal-")
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name).resolve()
        self.raw, self.delta, self.anchor = (self.base / name for name in ("raw", "delta", "anchor"))
        self.context = {"registration": Declaration("harness_qualification", (("first", 1),)),
                        "source": "a" * 64, "runtime": {"image": "fixed", "argv": ("python", "module")}}
        self.authority = head.ExternalHead.create(self.anchor, journal_roots=(self.raw, self.delta))
        self.addCleanup(self.authority.close)
        self.owners = []
        self.addCleanup(self.close_owners)

    def close_owners(self):
        for owner in self.owners: owner.close()

    def owner(self, **kwargs):
        owner = journal.OwnerJournal(self.raw, self.delta, context=self.context,
                                     authority=self.authority, **kwargs)
        self.owners.append(owner)
        return owner

    def reopen(self, expected):
        self.close_owners()
        self.authority.close()
        self.authority = head.ExternalHead.reopen(self.anchor, journal_roots=(self.raw, self.delta), expected=expected)
        self.addCleanup(self.authority.close)
        return self.owner(expected=expected)

    def test_actual_durable_head_roundtrip_and_normalized_context(self):
        owner = self.owner()
        owner.retain("config.json", b'{"value":1}')
        owner.retain("stdout.bin", b"raw\0bytes")
        expected = owner.checkpoint()
        self.assertEqual(self.authority.read(), expected)
        genesis = json.loads((self.delta / "genesis.json").read_bytes())
        self.assertEqual(genesis["context"], {"owner_journal_protocol": journal.PROTOCOL,
            "execution_context": {"registration": {"purpose": "harness_qualification", "steps": [["first", 1]]},
                "source": "a" * 64, "runtime": {"image": "fixed", "argv": ["python", "module"]}}})
        self.assertEqual(genesis["raw_root"], str(self.raw))
        self.assertEqual(genesis["delta_root"], str(self.delta))
        reopened = self.reopen(expected)
        self.assertEqual(reopened.json("config.json"), {"value": 1})
        self.assertEqual(reopened.read("stdout.bin"), b"raw\0bytes")
        self.assertEqual(reopened.checkpoint(), expected)
        self.assertIs(journal.ControllerCheckpoint, chain.PrefixCommitment)

    def test_reopen_rejects_tampered_config_before_adapter_parse(self):
        owner = self.owner(); owner.retain("config.json", b'{"value":1}')
        expected = owner.checkpoint(); owner.close()
        (self.raw / "config.json").write_bytes(b'{"value":2}')
        with patch.object(journal.OwnerJournal, "json", side_effect=AssertionError("must not parse config")):
            with self.assertRaises(journal.ChainError): self.owner(expected=expected)
        self.assertEqual(self.authority.read(), expected)
        self.assertEqual((self.raw / "config.json").read_bytes(), b'{"value":2}')

    def test_reopen_foreign_suffix_is_not_adopted(self):
        owner = self.owner(); owner.retain("config.json", b"{}")
        expected = owner.checkpoint(); owner.close()
        (self.raw / "unanchored.bin").write_bytes(b"suffix")
        with self.assertRaises(journal.ChainError): self.owner(expected=expected)
        self.assertEqual(self.authority.read(), expected)
        self.assertTrue((self.raw / "unanchored.bin").exists())

    def test_missing_authority_has_no_local_fallback(self):
        with self.assertRaises(TypeError):
            journal.OwnerJournal(self.raw, self.delta, context=self.context)  # type: ignore[call-arg]
        with self.assertRaises(journal.ChainError):
            journal.OwnerJournal(self.raw, self.delta, context=self.context, authority=None)  # type: ignore[arg-type]
        self.assertFalse(self.raw.exists())
        self.assertFalse(self.delta.exists())
        self.assertIsNone(self.authority.read())

    def test_lost_anchor_ack_preserves_prior_facts_and_requires_explicit_reopen(self):
        owner = self.owner(); owner.retain("prior.json", b'{"known":false}')
        prior = owner.checkpoint(); real_cas = self.authority.compare_and_set
        def lost_ack(expected, proposed):
            self.assertTrue(real_cas(expected, proposed))
            raise OSError("fixture lost acknowledgement after durable CAS")
        with patch.object(self.authority, "compare_and_set", side_effect=lost_ack):
            with self.assertRaises(journal.ChainUnknown): owner.retain("pending.bin", b"pending")
        current = self.authority.read()
        assert current is not None
        self.assertEqual(current.sequence, prior.sequence + 1)
        self.assertTrue(owner.uncertain)
        self.assertEqual(owner.commitment, prior)
        fact = owner.read_prior("prior.json")
        self.assertTrue(fact.prior_only)
        self.assertFalse(fact.acceptance_authority)
        self.assertEqual(fact.raw, b'{"known":false}')
        self.assertEqual(fact.commitment, prior)
        for operation in (owner.checkpoint, lambda: owner.read("prior.json"),
                          lambda: owner.json("prior.json"), lambda: owner.read_prior("pending.bin"),
                          lambda: owner.retain("cleanup.json", b"{}", cleanup=True)):
            with self.assertRaises(journal.ChainUnknown): operation()
        reopened = self.reopen(current)
        self.assertEqual(reopened.read("pending.bin"), b"pending")
        self.assertEqual(reopened.checkpoint(), current)

    def test_checkpoint_full_validation_detects_tamper_without_per_file_scan(self):
        owner = self.owner(); owner.retain("one.bin", b"one")
        (self.raw / "one.bin").write_bytes(b"bad")
        with patch.object(chain.CheckpointChain, "validate_boundary", side_effect=AssertionError("no sink")):
            prefix = owner.retain("two.bin", b"two")
        self.assertEqual(prefix.sequence, 2)
        self.assertEqual(owner.commitment, prefix)
        with self.assertRaises(journal.ChainUnknown): owner.checkpoint()
        self.assertTrue(owner.uncertain)

    def test_point_read_authenticates_before_json_decoder(self):
        owner = self.owner(); owner.retain("config.json", b'{"value":1}')
        (self.raw / "config.json").write_bytes(b'{"value":2}')
        with patch.object(journal.strict, "decode", side_effect=AssertionError("no unauthenticated parse")):
            with self.assertRaises(journal.ChainUnknown): owner.json("config.json")

    def test_noncanonical_or_duplicate_json_is_rejected_after_authenticated_read(self):
        owner = self.owner()
        for index, raw in enumerate((b'{ "x":1}', b'{"x":1,"x":2}', b'{"x":NaN}', b'"\\ud800"')):
            name = f"bad-{index}.json"; owner.retain(name, raw)
            with self.assertRaises(journal.ChainError): owner.json(name)
            self.assertEqual(owner.read(name), raw)
        self.assertFalse(owner.uncertain)
        owner.checkpoint()

    def test_context_cycles_unsupported_keys_and_nonfinite_values_precede_writes(self):
        loop: list[Any] = []; loop.append(loop)
        invalid_contexts: tuple[Any, ...] = ({"loop": loop}, {1: "coerced"}, {"bad": float("nan")},
                        {"bad": object()}, {"bad": Declaration}, {"bad": "\ud800"}, [])
        for context in invalid_contexts:
            with self.subTest(kind=type(context).__name__), self.assertRaises(journal.ChainError):
                journal.OwnerJournal(self.raw, self.delta, context=context, authority=self.authority)
            self.assertFalse(self.raw.exists()); self.assertFalse(self.delta.exists())
        self.assertIsNone(self.authority.read())

    def test_context_depth_and_byte_bounds_precede_writes(self):
        nested: dict[str, Any] = {}
        for _ in range(journal.strict.MAX_DEPTH + 1): nested = {"next": nested}
        for context in (nested, {"too_large": "x" * 32769}):
            with self.assertRaises(journal.ChainError):
                journal.OwnerJournal(self.raw, self.delta, context=context, authority=self.authority)
            self.assertFalse(self.raw.exists())
        self.assertIsNone(self.authority.read())

    def test_context_is_detached_and_tuple_list_reopen_is_equivalent(self):
        owner = self.owner(); owner.retain("config.json", b"{}")
        expected = owner.checkpoint(); owner.close()
        self.context = {"registration": {"purpose": "harness_qualification", "steps": [["first", 1]]},
                        "source": "a" * 64, "runtime": {"image": "fixed", "argv": ["python", "module"]}}
        reopened = self.owner(expected=expected)
        self.context["source"] = "b" * 64
        self.assertEqual(reopened.checkpoint(), expected)
        reopened.close()
        with self.assertRaises(journal.ChainError): self.owner(expected=expected)

    def test_cleanup_reserve_remains_chain_admission(self):
        limits = journal.Limits(max_files=2, cleanup_files=1, max_raw_bytes=10,
                                max_raw_file_bytes=8, cleanup_raw_bytes=2)
        owner = self.owner(limits=limits)
        owner.retain("first.bin", b"12345678")
        with self.assertRaises(journal.BoundsExceeded): owner.retain("ordinary.bin", b"x")
        self.assertFalse(owner.uncertain)
        owner.retain("cleanup.bin", b"ok", cleanup=True)
        self.assertEqual(owner.checkpoint().raw_bytes, 10)

    def test_close_leaves_caller_owned_authority_available(self):
        owner = self.owner(); owner.retain("config.json", b"{}")
        expected = owner.checkpoint(); owner.close()
        self.assertEqual(self.authority.read(), expected)
        with self.assertRaises(journal.ChainError): owner.checkpoint()
        reopened = self.owner(expected=expected)
        self.assertEqual(reopened.json("config.json"), {})

    def test_wrong_external_expected_prefix_is_rejected_without_rewrite(self):
        owner = self.owner(); owner.retain("config.json", b"{}")
        expected = owner.checkpoint(); owner.close()
        before = (self.anchor / "head.json").read_bytes()
        with self.assertRaises(journal.ChainError):
            self.owner(expected=replace(expected, head_sha256="f" * 64))
        self.assertEqual((self.anchor / "head.json").read_bytes(), before)
        self.assertEqual(self.authority.read(), expected)

    def test_stable_point_read_rejects_symlink_even_with_identical_content(self):
        owner = self.owner(); owner.retain("config.json", b"{}")
        other = self.base / "other"; other.write_bytes(b"{}")
        (self.raw / "config.json").unlink(); (self.raw / "config.json").symlink_to(other)
        with self.assertRaises(journal.ChainUnknown): owner.read("config.json")
        self.assertTrue(owner.uncertain)

    def test_membership_and_roots_expose_only_the_acknowledged_prefix(self):
        owner = self.owner(); owner.retain("config.json", b"{}")
        self.assertEqual(owner.raw_root, self.raw)
        self.assertEqual(owner.delta_root, self.delta)
        self.assertTrue(owner.has("config.json"))
        self.assertFalse(owner.has("missing.json"))
        (self.raw / "foreign.json").write_bytes(b"{}")
        self.assertFalse(owner.has("foreign.json"))
        with self.assertRaises(journal.ChainUnknown): owner.checkpoint()
        with self.assertRaises(journal.ChainUnknown): owner.has("config.json")
        self.assertTrue(owner.read_prior("config.json").prior_only)
