"""Real private Git stores with trusted evidence fakes; no candidate execution."""
from __future__ import annotations

from dataclasses import asdict, replace
import hashlib
from pathlib import Path
import sqlite3
import tempfile
import unittest

from gossip_harness.gitstore import GitStore
from gossip_harness.peer_candidate_v2 import named_sources
from gossip_harness.peer_coding_dispatch_v1 import source_digest
from gossip_harness.peer_project_contract_v2 import (
    PACKAGES, ContractError, SelectedOffer, identity, to_dict,
)
from gossip_harness.peer_project_promotion_v3 import ProjectPromotion, PromotionError, PromotionResult, StagedRelease
from gossip_harness.peer_review_release_v2 import AdmittedRefutation, RequiredCheck, ReviewGateReceipt, suite_digest
from tests.test_peer_review_release_v2 import Fixture, evidence, sha


class Crash(RuntimeError):
    pass


class PeerProjectPromotionV3Tests(unittest.TestCase):
    """Actual merge/CAS/crash histories; execution and provider proofs are fakes."""

    @classmethod
    def setUpClass(cls):
        cls.fixture_tmp = tempfile.TemporaryDirectory(prefix="project-promotion-v3-immutable-")
        cls.fixture_root = Path(cls.fixture_tmp.name)
        seed = {"seed.py": "seed bytes", **{package + ".py": "before-" + package for package in PACKAGES}}
        cls.fixture_store = GitStore.create(cls.fixture_root / "seed.git", seed)
        cls.fixture_proposals = {}
        for package in PACKAGES:
            store = GitStore.fork(cls.fixture_store, cls.fixture_root / (package + ".git"))
            commit = store.propose({package + ".py": "source-" + package})
            cls.fixture_proposals[package] = (store, commit)
        # Immutable shared Git input, built through actual scope-checked merges.
        # Journal-only controls below explicitly seed synthetic trusted history;
        # production stage behavior has separate fresh real-Git tests.
        cls.fixture_combined = GitStore.fork(cls.fixture_store, cls.fixture_root / "combined.git")
        for package in PACKAGES:
            store, commit = cls.fixture_proposals[package]
            candidate = cls.fixture_combined.prepare(store, commit, cls.fixture_combined.head(),
                lambda path: (True, "Trusted authored fixture; no execution"), allowed_paths=(package + ".py",))
            if candidate.status not in ("prepared", "noop"):
                raise AssertionError(candidate)
            cls.fixture_combined.accept(candidate)

    @classmethod
    def tearDownClass(cls):
        cls.fixture_tmp.cleanup()

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="project-promotion-v3-")
        self.root = Path(self.tmp.name)
        self.fx = Fixture()
        self.seed = {"seed.py": "seed bytes", **{package + ".py": "before-" + package for package in PACKAGES}}
        self.store = GitStore.fork(self.fixture_store, self.root / "protected.git")
        self.base = self.store.head()
        self.stores = {}
        offers = []
        members = {}
        for old in self.fx.offers:
            package = old.dispatch.action.work.package_id
            store, commit = self.fixture_proposals[package]
            offer = replace(old, commit_oid=commit, source_sha256=source_digest(store.read_files(commit)))
            offers.append(offer)
            self.stores[identity(offer)] = store
            members[identity(offer)] = named_sources({package + ".py": "source-" + package})
        self.fx.offers, self.fx.members = tuple(offers), members
        self.fx.selection = replace(self.fx.selection,
            eligible_offer_sha256s=tuple(identity(item) for item in offers),
            selected=tuple(SelectedOffer(item.dispatch.action.work.package_id, identity(item)) for item in offers))
        self.promotion = self.open()

    def tearDown(self):
        if hasattr(self, "promotion"):
            self.promotion.close()
        self.tmp.cleanup()

    def open(self, **changes):
        arguments = dict(root=self.root / "history", protected_store=self.store,
            repository_id="repository", baseline_sha=self.base, policy=self.fx.policy,
            initial_suite=self.fx.suite, package_scopes={package: (package + ".py",) for package in PACKAGES},
            verified_selection=lambda selection: self.fx.selection,
            verified_contribution=lambda offer: self.fx.members[identity(offer)],
            verified_terminal=self.fx.terminal,
            verified_view=lambda binding: self.fx.views[identity(binding)],
            verified_execution=lambda target, suite: self.fx.execution)
        return ProjectPromotion(**(arguments | changes))

    def stage(self, **changes):
        arguments = dict(generation=0, package_generations=self.fx.policy.package_generations)
        return self.promotion.stage(self.fx.selection, self.fx.offers, self.stores, **(arguments | changes))

    def trusted_stage_history(self):
        """Synthetic trusted journal fixture; fresh real Git pin and CAS state.

        This is not a production stage execution or a signed-history restore.
        Only controls downstream of staging use this fixture; staging tests call
        the actual public stage API on fresh private stores separately.
        """
        commit = self.fixture_combined.head()
        candidate = self.store.prepare(self.fixture_combined, commit, self.base,
            lambda path: (True, "Trusted shared immutable stage fixture"),
            allowed_paths=tuple(package + ".py" for package in PACKAGES))
        self.assertEqual(candidate.candidate_sha, commit)
        stage_id = sha("synthetic-trusted-stage-history")
        sources = named_sources(self.store.read_files(commit))
        self.promotion._append("stage_intent", {"stage_id": stage_id, "generation": 0,
            "selection": to_dict(self.fx.selection), "offers": [to_dict(item) for item in self.fx.offers],
            "expected_head": self.base, "sources": [to_dict(item) for item in sources],
            "suite_sha256": suite_digest(self.fx.suite),
            "package_generations": self.fx.policy.package_generations, "integration_order": PACKAGES})
        staged = StagedRelease(stage_id, 0, self.base, commit,
            self.store._git("rev-parse", commit + "^{tree}"), sources, identity(self.fx.selection),
            suite_digest(self.fx.suite), PACKAGES)
        self.promotion._append("staged", asdict(staged))
        return staged

    def target(self):
        staged = self.trusted_stage_history()
        target = self.promotion.bind_target(staged, evaluator_sha256=sha("evaluator"),
            execution_receipt_refs=(evidence("execution", "public-execution"),))
        self.fx.target = target
        self.fx.execution = self.fx.execution_for(target, self.promotion.suites[-1])
        return target

    def reviews(self, target, *, edit=None, revision=0):
        original = self.fx.binding
        if revision:
            def revised(*args, **kwargs):
                binding = original(*args, **kwargs)
                return replace(binding, call_id=binding.call_id + "-" + str(revision),
                    reservation_id=binding.reservation_id + "-" + str(revision),
                    action=replace(binding.action, action_id=binding.action.action_id + "-" + str(revision),
                                   request_id=binding.action.request_id + "-" + str(revision)))
            self.fx.binding = revised
        try:
            verdicts = self.fx.reviews(target=target, edit=edit)
        finally:
            self.fx.binding = original
        dispatches = tuple(dict.fromkeys(item.review_dispatch for item in verdicts))
        self.promotion.begin_review_round(target, round_number=revision,
            requests=tuple((binding.action.actor, binding.action.request_id) for binding in dispatches))
        self.promotion.record_reviews(target, dispatches)
        return dispatches

    def test_four_actual_git_histories_are_staged_without_advancing_protected(self):
        staged = self.stage(integration_order=tuple(reversed(PACKAGES)))
        self.assertEqual(self.store.head(), self.base)
        self.assertEqual(staged.integration_order, tuple(reversed(PACKAGES)))
        self.assertEqual(self.store.read_files(staged.commit_oid),
                         {"seed.py": "seed bytes", **{package + ".py": "source-" + package for package in PACKAGES}})
        for offer in self.fx.offers:
            self.assertTrue(self.store.is_ancestor(offer.commit_oid, staged.commit_oid))
        self.assertTrue(self.store.is_ancestor(self.base, staged.commit_oid))
        checkpoint = self.promotion.checkpoint()
        self.assertEqual(self.stage(integration_order=tuple(reversed(PACKAGES))), staged)
        self.assertEqual(self.promotion.checkpoint(), checkpoint)

    def test_exact_combined_execution_and_eight_scopes_authorize_one_cas(self):
        target = self.target()
        self.reviews(target)
        result = self.promotion.promote(target)
        self.assertIsInstance(result, PromotionResult)
        self.assertEqual(self.store.head(), target.commit_oid)
        checkpoint = self.promotion.checkpoint()
        self.assertEqual(self.promotion.promote(target), result)
        self.assertEqual(self.promotion.checkpoint(), checkpoint)
        self.assertFalse(result.recovered)

    def test_execution_failure_blocks_otherwise_unanimous_review(self):
        target = self.target()
        self.reviews(target)
        self.fx.execution = replace(self.fx.execution,
            check_results=tuple((check, "failed" if index == 2 else status)
                                for index, (check, status) in enumerate(self.fx.execution.check_results)))
        receipt = self.promotion.promote(target)
        self.assertIsInstance(receipt, ReviewGateReceipt)
        self.assertFalse(receipt.eligible)
        self.assertIn("required_execution_not_passed", receipt.blockers)
        self.assertEqual(self.store.head(), self.base)

    def test_missing_reviews_never_promote(self):
        target = self.target()
        receipt = self.promotion.promote(target)
        self.assertFalse(receipt.eligible)
        self.assertEqual(len(receipt.missing_slots), 8)
        self.assertEqual(self.store.head(), self.base)

    def test_latest_complete_response_replaces_both_scopes_and_old_replay_cannot_restore(self):
        target = self.target()
        original = self.reviews(target)
        self.reviews(target, revision=1, edit=lambda actor, body:
            body["verdicts"].pop() if actor == "R1" else None)
        self.promotion.record_reviews(target, original)
        receipt = self.promotion.promote(target)
        self.assertFalse(receipt.eligible)
        self.assertEqual(receipt.missing_slots, (("R1", "clients"),))
        self.assertEqual(self.store.head(), self.base)

    def test_older_unseen_approval_cannot_replace_registered_newer_rejection(self):
        target = self.target()
        old_verdicts = self.fx.reviews(target=target)
        old_dispatches = tuple(dict.fromkeys(item.review_dispatch for item in old_verdicts))
        self.promotion.begin_review_round(target, round_number=0,
            requests=tuple((item.action.actor, item.action.request_id) for item in old_dispatches))
        self.reviews(target, revision=1, edit=lambda actor, body:
            body["verdicts"][0].update(verdict="request_changes") if actor == "R1" else None)
        self.promotion.record_reviews(target, old_dispatches)
        receipt = self.promotion.promote(target)
        self.assertFalse(receipt.eligible)
        self.assertIn(("R1", "catalog"), receipt.rejected_slots)
        self.assertEqual(sum(kind == "late_review" for kind, _, _ in self.promotion.events), 4)
        checkpoint = self.promotion.checkpoint()
        self.promotion.record_reviews(target, old_dispatches)
        self.assertEqual(self.promotion.checkpoint(), checkpoint)
        self.assertEqual(self.store.head(), self.base)

    def test_admitted_refutation_is_retained_across_restart(self):
        target = self.target()
        self.reviews(target)
        refutation = AdmittedRefutation(identity(target), ("catalog",), evidence("counterexample", "refutation"))
        self.promotion.admit_refutation(refutation)
        checkpoint = self.promotion.checkpoint()
        self.promotion.close()
        self.promotion = self.open(expected_checkpoint=checkpoint)
        self.promotion.admit_refutation(refutation)
        self.assertEqual(self.promotion.checkpoint(), checkpoint)
        receipt = self.promotion.promote(target)
        self.assertFalse(receipt.eligible)
        self.assertEqual(len(receipt.rejected_slots), 2)
        self.assertEqual(self.promotion.refutations, [refutation])

    def test_new_required_check_invalidates_old_target(self):
        target = self.target()
        self.reviews(target)
        suite = self.promotion.record_suite((RequiredCheck("new-counterexample", sha("assertion-new")),))
        self.assertEqual(suite.checks[:len(self.fx.suite.checks)], self.fx.suite.checks)
        with self.assertRaisesRegex(ContractError, "stale"):
            self.promotion.promote(target)
        with self.assertRaises(ContractError):
            self.promotion.record_suite((RequiredCheck("public", sha("weakened")),))
        self.assertEqual(len(self.promotion.suites), 2)

    def test_crash_after_git_cas_recovers_without_second_cas_or_review(self):
        target = self.target()
        self.reviews(target)
        def crash(point):
            if point == "after_git_cas":
                raise Crash(point)
        self.promotion.crash_hook = crash
        with self.assertRaises(Crash):
            self.promotion.promote(target)
        self.assertEqual(self.store.head(), target.commit_oid)
        checkpoint = self.promotion.checkpoint()
        self.promotion.close()
        self.promotion = self.open(expected_checkpoint=checkpoint,
            verified_terminal=lambda binding: self.fail("Recovery reinvoked review verification"))
        original_accept = self.store.accept
        self.store.accept = lambda candidate: self.fail("Recovery attempted duplicate Git CAS")
        try:
            result = self.promotion.promote(target)
        finally:
            self.store.accept = original_accept
        self.assertTrue(result.recovered)
        self.assertEqual(self.store.head(), target.commit_oid)

    def test_crash_before_git_cas_replays_exact_intent(self):
        target = self.target()
        self.reviews(target)
        def crash(point):
            if point == "after_cas_intent":
                raise Crash(point)
        self.promotion.crash_hook = crash
        with self.assertRaises(Crash):
            self.promotion.promote(target)
        self.assertEqual(self.store.head(), self.base)
        with self.assertRaisesRegex(ContractError, "Recover"):
            self.promotion.record_suite((RequiredCheck("late", sha("late")),))
        self.promotion.close()
        self.promotion = self.open()
        result = self.promotion.promote(target)
        self.assertTrue(result.recovered)
        self.assertEqual(self.store.head(), target.commit_oid)

    def test_stale_external_head_cannot_be_overwritten(self):
        target = self.target()
        self.reviews(target)
        other = self.store.propose({"catalog.py": "external concurrent change"})
        candidate = self.store.prepare(self.store, other, self.base, lambda path: (True, "trusted fixture"))
        self.store.accept(candidate)
        with self.assertRaisesRegex(ContractError, "head changed"):
            self.promotion.promote(target)
        self.assertEqual(self.store.head(), other)

    def test_recovery_rejects_head_neither_side_of_cas(self):
        target = self.target()
        self.reviews(target)
        def crash(point):
            if point == "after_cas_intent":
                raise Crash(point)
        self.promotion.crash_hook = crash
        with self.assertRaises(Crash):
            self.promotion.promote(target)
        other = self.store.propose({"catalog.py": "other tree"})
        self.store.accept(self.store.prepare(self.store, other, self.base, lambda path: (True, "fixture")))
        with self.assertRaisesRegex(PromotionError, "both sides"):
            self.promotion.promote(target)
        self.assertEqual(self.store.head(), other)

    def test_unknown_selection_and_mismatched_candidate_bytes_are_rejected(self):
        wrong = replace(self.fx.selection, eligibility_policy_sha256=sha("another-policy"))
        with self.assertRaisesRegex(ContractError, "provenance"):
            self.promotion.stage(wrong, self.fx.offers, self.stores, generation=0,
                                 package_generations=self.fx.policy.package_generations)
        old = self.fx.offers[0]
        self.fx.members[identity(old)] = named_sources({"catalog.py": "not actual Git bytes"})
        with self.assertRaisesRegex(ContractError, "omits or changes"):
            self.stage()
        self.assertEqual(self.promotion.checkpoint()[0], 0)

    def test_introduced_out_of_scope_history_is_rejected_even_if_reverted(self):
        old = self.fx.offers[0]
        store = self.stores.pop(identity(old))
        bad = store.propose({"seed.py": "unauthorized"})
        reverted = store.propose({"seed.py": "seed bytes", "catalog.py": "source-catalog"}, base_sha=bad)
        offer = replace(old, commit_oid=reverted, source_sha256=source_digest(store.read_files(reverted)))
        self.stores[identity(offer)] = store
        self.fx.members[identity(offer)] = self.fx.members.pop(identity(old))
        self.fx.offers = (offer, *self.fx.offers[1:])
        self.fx.selection = replace(self.fx.selection,
            eligible_offer_sha256s=tuple(identity(item) for item in self.fx.offers),
            selected=tuple(SelectedOffer(item.dispatch.action.work.package_id, identity(item)) for item in self.fx.offers))
        with self.assertRaisesRegex(PromotionError, "scope_rejected"):
            self.stage()
        self.assertEqual(self.store.head(), self.base)
        self.assertEqual([kind for kind, _, _ in self.promotion.events], ["stage_intent", "stage_failed"])

    def test_mid_stage_restart_reuses_prefix_without_changing_result(self):
        def crash(point):
            if point == "after_stage_package:ingestion":
                raise Crash(point)
        self.promotion.crash_hook = crash
        with self.assertRaises(Crash):
            self.stage()
        self.promotion.close()
        self.promotion = self.open()
        staged = self.stage()
        self.assertEqual(self.store.head(), self.base)
        self.assertEqual(len(staged.sources), 5)
        self.assertEqual(sum(kind == "stage_intent" for kind, _, _ in self.promotion.events), 1)

    def test_policy_and_root_cannot_change_on_restart(self):
        self.promotion.close()
        with self.assertRaisesRegex(ContractError, "configuration"):
            self.open(repository_id="different-repository")
        self.promotion = self.open()
        with self.assertRaisesRegex(PromotionError, "owns"):
            self.open()

    def test_external_checkpoint_detects_journal_rollback(self):
        self.promotion.record_suite((RequiredCheck("extra", sha("extra")),))
        checkpoint = self.promotion.checkpoint()
        self.promotion.close()
        database = sqlite3.connect(self.root / "history" / "history.sqlite3")
        database.execute("DELETE FROM events")
        database.commit()
        database.close()
        with self.assertRaisesRegex(ContractError, "rolled back"):
            self.open(expected_checkpoint=checkpoint)

    def test_hash_chain_detects_changed_history(self):
        self.promotion.record_suite((RequiredCheck("extra", sha("extra")),))
        self.promotion.close()
        database = sqlite3.connect(self.root / "history" / "history.sqlite3")
        database.execute("UPDATE events SET digest=?", (hashlib.sha256(b"wrong").hexdigest(),))
        database.commit()
        database.close()
        with self.assertRaisesRegex(ContractError, "content changed"):
            self.open()

    def test_target_cannot_change_receipt_identity_after_binding(self):
        target = self.target()
        staged = next(iter(self.promotion.stages.values()))
        with self.assertRaisesRegex(ContractError, "rebound"):
            self.promotion.bind_target(staged, evaluator_sha256=target.evaluator_sha256,
                                      execution_receipt_refs=(evidence("other-execution", "public-execution"),))


if __name__ == "__main__":
    unittest.main()
