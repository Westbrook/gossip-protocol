"""Actual topic Git histories; trusted finance/review/execution adapters are fakes."""
from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import tempfile
import unittest
import json

from gossip_harness.gitstore import GitStore
from gossip_harness.peer_candidate_v2 import named_sources
from gossip_harness.peer_coding_dispatch_v1 import source_digest
from gossip_harness.peer_project_contract_v2 import (
    PACKAGES, ContractError, ReleaseTarget, SelectedOffer, canonical_bytes, identity,
)
from gossip_harness.peer_project_promotion_v3 import ProjectPromotion, PromotionResult
from gossip_harness.peer_project_repair_v4 import (
    UnchangedTopic, assert_current_plan, plan_topic_repairs, publish_repair_directive, repair_frontier,
    verify_repair_directive,
)
from gossip_harness.peer_role_loop_v2 import financial_task_id
from gossip_harness.peer_project_views_v3 import materialize_project
from gossip_harness.peer_review_release_v2 import ReviewGateReceipt, policy_digest, suite_digest
from tests.test_peer_review_release_v2 import Fixture, sha
from tests.test_peer_role_loop_v2 import FakeMesh


class PeerProjectRepairV4Tests(unittest.TestCase):
    """Real Git source/scope controls; no provider, Docker or candidate execution."""

    @classmethod
    def setUpClass(cls):
        cls.shared_tmp = tempfile.TemporaryDirectory(prefix="topic-repair-v4-shared-")
        cls.shared_root = Path(cls.shared_tmp.name)
        cls.seed = {"seed.py": "seed bytes", **{p + ".py": "before-" + p for p in PACKAGES}}
        cls.seed_store = GitStore.create(cls.shared_root / "seed.git", cls.seed)
        cls.topics = {}
        cls.combined = GitStore.fork(cls.seed_store, cls.shared_root / "combined.git")
        for package in PACKAGES:
            store = GitStore.fork(cls.seed_store, cls.shared_root / (package + ".git"))
            commit = store.propose({package + ".py": "source-" + package})
            cls.topics[package] = store, commit
            prepared = cls.combined.prepare(store, commit, cls.combined.head(),
                lambda _path: (True, "Authored structural fixture only"), allowed_paths=(package + ".py",))
            if cls.combined.accept(prepared).status != "accepted":
                raise AssertionError(prepared)

    @classmethod
    def tearDownClass(cls):
        cls.shared_tmp.cleanup()

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="topic-repair-v4-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.protected = GitStore.fork(self.seed_store, self.root / "protected.git")
        self.baseline = self.protected.head()
        # Import immutable fixture history without accepting it. The integration
        # test below separately performs all actual ProjectPromotion.stage calls.
        combined = self.combined.head()
        self.protected._git("fetch", "--no-tags", str(self.combined.path), combined)
        self.fx = Fixture()
        self.scopes = {p: (p + ".py",) for p in PACKAGES}
        self.stores, offers, self.members = {}, [], {}
        for old in self.fx.offers:
            package = old.dispatch.action.work.package_id
            store, commit = self.topics[package]
            offer = replace(old, commit_oid=commit, source_sha256=source_digest(store.read_files(commit)))
            offers.append(offer)
            self.stores[identity(offer)] = store
            self.members[identity(offer)] = named_sources({package + ".py": "source-" + package})
        self.fx.offers, self.fx.members = tuple(offers), self.members
        self.set_selection(self.fx.offers, 0)
        self.mesh = FakeMesh("registry")
        receipt_ref = self.mesh.arrive("executor", "execution-receipt", b'{"public":"failed"}')
        self.target = ReleaseTarget(self.fx.context, 0, "repository", "refs/heads/accepted", self.baseline,
            "sha1", combined, self.protected._git("rev-parse", combined + "^{tree}"),
            named_sources(self.protected.read_files(combined)), identity(self.fx.selection),
            suite_digest(self.fx.suite), sha("evaluator"), (receipt_ref,))
        self.failure = ReviewGateReceipt(identity(self.target), policy_digest(self.fx.policy),
            suite_digest(self.fx.suite), (), (), (), (), ("required_execution_not_passed",), False)
        self.current = self.target, self.failure

    def set_selection(self, offers, generation):
        old = self.fx.selection.selecting_dispatch
        dispatch = replace(old, action=replace(old.action,
            work=replace(old.action.work, generation=generation),
            action_id="selection-action-" + str(generation), request_id="selection-request-" + str(generation)))
        self.fx.selection = replace(self.fx.selection, selecting_dispatch=dispatch,
            eligible_offer_sha256s=tuple(identity(offer) for offer in offers),
            selected=tuple(SelectedOffer(offer.dispatch.action.work.package_id, identity(offer)) for offer in offers))

    def plan(self, **changes):
        args = dict(target_store=self.protected, baseline_sha=self.baseline, selection=self.fx.selection,
            offers=self.fx.offers, stores_by_offer=self.stores, package_scopes=self.scopes,
            repair_packages=("catalog", "ingestion"), feedback_refs=self.current[0].execution_receipt_refs,
            current_failure=lambda: self.current, verified_selection=lambda _selection: self.fx.selection,
            verified_contribution=lambda offer: self.members[identity(offer)],
            verified_feedback=lambda _target, _gate: self.current[0].execution_receipt_refs)
        return plan_topic_repairs(**(args | changes))

    def directive(self, plan, package="catalog", **changes):
        topic = plan.topic(package)
        args = dict(work=replace(topic.offer.dispatch.action.work, generation=topic.new_generation,
                                 slot_id="repair-" + package),
                    profile_id=topic.offer.dispatch.action.profile_id,
                    instructions="Repair the public failure in your package.", current_failure=lambda: self.current)
        return publish_repair_directive(plan, package, self.mesh, **(args | changes))

    def replacement(self, topic, value):
        store = self.stores[identity(topic.offer)]
        commit = store.propose({topic.package_id + ".py": value}, topic.base_sha)
        old = topic.offer.dispatch
        dispatch = replace(old, action=replace(old.action, kind="repair",
            work=replace(old.action.work, generation=topic.new_generation, slot_id="repair-" + topic.package_id),
            request_id=old.action.request_id + "-" + str(topic.new_generation),
            action_id=old.action.action_id + "-" + str(topic.new_generation)),
            call_id=old.call_id + "-" + str(topic.new_generation),
            reservation_id=old.reservation_id + "-" + str(topic.new_generation))
        offer = replace(topic.offer, dispatch=dispatch, commit_oid=commit,
                        source_sha256=source_digest(store.read_files(commit)))
        self.stores[identity(offer)] = store
        self.members[identity(offer)] = named_sources({topic.package_id + ".py": value})
        return offer

    def unchanged(self, plan, package):
        """Trusted failure fixture; real finance authentication is controller-owned."""
        topic = plan.topic(package)
        old = topic.offer.dispatch
        work = replace(old.action.work, generation=topic.new_generation, slot_id="repair-" + package)
        task = financial_task_id(plan.failed_target.context, work)
        binding = replace(old, action=replace(old.action, kind="repair", work=work,
            action_id="action-" + sha(task), request_id="request-" + sha(task)),
            call_id="call-" + sha(task), reservation_id="reservation-" + sha(task))
        return UnchangedTopic(package, plan.sha256, identity(topic.offer), binding, sha("known-no-patch-" + task))

    def test_worker_patch_base_is_topic_while_complete_combined_context_is_preserved(self):
        plan = self.plan()
        directive = self.directive(plan)
        request, _view = materialize_project(directive, self.mesh, sha("policy"))
        topic = plan.topic("catalog")
        self.assertEqual(request.base_sha, topic.offer.commit_oid)
        self.assertEqual(request.files, dict(topic.base_files))
        self.assertEqual(request.files["ingestion.py"], "before-ingestion")
        self.assertEqual(dict(plan.failed_files)["ingestion.py"], "source-ingestion")
        self.assertIn("source-ingestion", request.feedback)
        self.assertEqual(verify_repair_directive(plan, directive, self.mesh).base_sha, topic.base_sha)

    def test_whole_failed_source_cannot_be_substituted_as_patch_base(self):
        plan = self.plan()
        directive = self.directive(plan)
        source = self.mesh.arrive("registry", "project-source", canonical_bytes({
            "files": dict(plan.failed_files), "base_sha": plan.failed_target.commit_oid}))
        with self.assertRaisesRegex(ContractError, "base or failed combined"):
            verify_repair_directive(plan, replace(directive, source_ref=source), self.mesh)

    def test_unselected_topic_or_mismatched_context_cannot_replace_registered_evidence(self):
        plan = self.plan()
        directive = self.directive(plan)
        bad_source = self.mesh.arrive("registry", "project-source", canonical_bytes({
            "files": dict(plan.topic("ingestion").base_files), "base_sha": plan.topic("ingestion").base_sha}))
        wrong_context = self.mesh.arrive("registry", "project-repair-context", b'{"other":"failure"}')
        for altered in (replace(directive, source_ref=bad_source),
                        replace(directive, evidence_refs=(wrong_context, *plan.feedback_refs))):
            with self.subTest(altered=altered.source_ref.payload_sha256), self.assertRaises(ContractError):
                verify_repair_directive(plan, altered, self.mesh)

    def test_stale_failure_blocks_new_dispatch_and_frontier_but_not_immutable_view_proof(self):
        plan = self.plan()
        directive = self.directive(plan)
        self.current = self.target, replace(self.failure, blockers=("changed-public-failure",))
        with self.assertRaisesRegex(ContractError, "stale"):
            self.directive(plan)
        with self.assertRaisesRegex(ContractError, "stale"):
            repair_frontier(plan, (), current_failure=lambda: self.current)
        # Revalidating already-accounted repair evidence after a successor exists
        # must not invalidate that successor's own contribution proofs.
        self.assertEqual(verify_repair_directive(plan, directive, self.mesh).base_sha,
                         plan.topic("catalog").base_sha)

    def test_accepted_or_foreign_failure_cannot_admit_repairs(self):
        for failure in (replace(self.failure, eligible=True),
                        replace(self.failure, target_sha256=sha("another-target")),
                        replace(self.failure, independent_final_project_acceptance=True)):
            with self.subTest(failure=failure), self.assertRaises(ContractError):
                self.plan(current_failure=lambda: (self.target, failure))

    def test_required_execution_refs_and_public_feedback_cannot_be_dropped_or_private(self):
        private = self.mesh.arrive("acceptance", "private-acceptance", b"must not enter prompts")
        for refs in ((), (private,), (*self.target.execution_receipt_refs, private)):
            with self.subTest(refs=refs), self.assertRaises(ContractError):
                self.plan(feedback_refs=refs)

    def test_reviewer_request_changes_can_trigger_repair_after_all_public_tests_pass(self):
        self.current = self.target, replace(self.failure, blockers=(), rejected_slots=(("R1", "catalog"),))
        with self.assertRaisesRegex(ContractError, "Rejected reviews require"):
            self.plan(repair_packages=("catalog",))
        # Fake authenticated public review packets, visibly carrying the reason.
        # The callback stands in for retained finance/role-result verification.
        role = self.mesh.arrive("R1", "role-result", b'{"call_id":"review-1"}')
        result = self.mesh.arrive("finance", "financial-result", b'{"review":"Preserve source provenance"}')
        refs = (*self.target.execution_receipt_refs, role, result)
        plan = self.plan(repair_packages=("catalog",), feedback_refs=refs,
                         verified_feedback=lambda target, gate: refs if (target, gate) == self.current else ())
        self.assertEqual(plan.package_generations, (("catalog", 1), ("ingestion", 0), ("query", 0), ("clients", 0)))
        directive = self.directive(plan)
        request, _view = materialize_project(directive, self.mesh, sha("policy"))
        self.assertIn("Preserve source provenance", request.feedback)
        self.assertEqual(json.loads(plan.context_payload())["plan"]["failure_receipt"]["rejected_slots"], [["R1", "catalog"]])

    def test_feedback_must_match_complete_authenticated_current_gate_artifacts(self):
        unrelated = self.mesh.arrive("finance", "financial-result", b'{"another":"review"}')
        with self.assertRaisesRegex(ContractError, "authenticated complete"):
            self.plan(feedback_refs=(*self.target.execution_receipt_refs, unrelated))

    def test_unrelated_introduced_history_is_rejected_even_after_revert(self):
        topic = self.fx.offers[0]
        store = self.stores[identity(topic)]
        unrelated = store.propose({"ingestion.py": "temporary unrelated edit"}, topic.commit_oid)
        restored = store.propose({"ingestion.py": "before-ingestion"}, unrelated)
        imposter = replace(topic, commit_oid=restored,
                           source_sha256=source_digest(store.read_files(restored)))
        old = self.fx.offers
        self.fx.offers = (imposter, *old[1:])
        self.stores[identity(imposter)] = store
        self.members[identity(imposter)] = self.members[identity(topic)]
        self.set_selection(self.fx.offers, 0)
        altered_target = replace(self.target, selection_sha256=identity(self.fx.selection))
        self.current = altered_target, replace(self.failure, target_sha256=identity(altered_target))
        with self.assertRaisesRegex(ContractError, "unrelated package history"):
            self.plan()

    def test_frontier_retains_exact_unchanged_offers_and_rejects_extra_or_stale_inputs(self):
        plan = self.plan()
        replacements = tuple(self.replacement(plan.topic(p), "repaired-" + p) for p in ("catalog", "ingestion"))
        frozen = repair_frontier(plan, replacements, current_failure=lambda: self.current)
        self.assertEqual(frozen[2:], self.fx.offers[2:])
        for late in ((*replacements, replacements[0]), (*replacements, self.fx.offers[2]), replacements[:1]):
            with self.subTest(count=len(late)), self.assertRaises(ContractError):
                repair_frontier(plan, late, current_failure=lambda: self.current)
        # This helper is stateless. Actual once-only frontier registration is a
        # controller obligation, separately tested by its integration controls.
        self.current = self.target, replace(self.failure, blockers=("current-gate-changed",))
        with self.assertRaisesRegex(ContractError, "stale"):
            repair_frontier(plan, replacements, current_failure=lambda: self.current)
        self.assertEqual(frozen[2:], self.fx.offers[2:])

    def test_replacement_generation_actor_profile_and_package_are_fenced(self):
        plan = self.plan(repair_packages=("catalog",))
        offer = self.replacement(plan.topic("catalog"), "repaired")
        variants = (replace(offer, bundle_ref=replace(offer.bundle_ref, producer="another-builder"),
                        dispatch=replace(offer.dispatch, lease=replace(offer.dispatch.lease, worker_id="another-builder"),
                        action=replace(offer.dispatch.action, actor="another-builder",
                            worker_payload_ref=replace(offer.dispatch.action.worker_payload_ref, producer="another-builder")))),
                    replace(offer, dispatch=replace(offer.dispatch, profile_sha256=sha("different-profile"))),
                    replace(offer, dispatch=replace(offer.dispatch, action=replace(offer.dispatch.action,
                        work=replace(offer.dispatch.action.work, generation=2)))))
        for variant in variants:
            with self.subTest(variant=identity(variant)), self.assertRaises(ContractError):
                repair_frontier(plan, (variant,), current_failure=lambda: self.current)

    def test_known_no_patch_requires_exact_proof_and_preserves_old_offer_source_generation(self):
        plan = self.plan(repair_packages=("ingestion",))
        disposition = self.unchanged(plan, "ingestion")
        with self.assertRaisesRegex(ContractError, "trusted failure verifier"):
            repair_frontier(plan, (), current_failure=lambda: self.current, unchanged=(disposition,))
        with self.assertRaisesRegex(ContractError, "exact authenticated known-failure"):
            repair_frontier(plan, (), current_failure=lambda: self.current, unchanged=(disposition,),
                verified_unchanged=lambda item: replace(item, result_sha256=sha("different-result")))
        reads = []
        def verify(item):
            reads.append(item)
            return disposition
        frontier = repair_frontier(plan, (), current_failure=lambda: self.current,
                                   unchanged=(disposition,), verified_unchanged=verify)
        self.assertEqual(reads, [disposition])
        self.assertEqual(frontier, self.fx.offers)
        self.assertIs(frontier[1], plan.topic("ingestion").offer)
        self.assertEqual(frontier[1].dispatch.action.work.generation, 0)
        self.assertEqual(self.stores[identity(frontier[1])].read_files(frontier[1].commit_oid),
                         dict(plan.topic("ingestion").base_files))
        self.assertEqual(self.protected.head(), self.baseline)
        self.assertEqual(disposition.body()["result_sha256"], disposition.result_sha256)
        for replacements, unchanged in (((), (disposition, disposition)),
                ((self.replacement(plan.topic("ingestion"), "overlap"),), (disposition,))):
            with self.subTest(replacements=len(replacements)), self.assertRaisesRegex(ContractError, "exactly"):
                repair_frontier(plan, replacements, current_failure=lambda: self.current,
                                unchanged=unchanged, verified_unchanged=verify)

    def test_unchanged_disposition_fences_plan_offer_actor_profile_round_and_missing_slot(self):
        plan = self.plan(repair_packages=("ingestion",))
        disposition = self.unchanged(plan, "ingestion")
        binding = disposition.dispatch
        bad_actor = replace(binding, lease=replace(binding.lease, worker_id="foreign"),
            action=replace(binding.action, actor="foreign",
                worker_payload_ref=replace(binding.action.worker_payload_ref, producer="foreign")))
        variants = (replace(disposition, plan_sha256=sha("stale-plan")),
            replace(disposition, original_offer_sha256=identity(self.fx.offers[0])),
            replace(disposition, dispatch=bad_actor),
            replace(disposition, dispatch=replace(binding, profile_sha256=sha("foreign-profile"))),
            replace(disposition, dispatch=replace(binding, action=replace(binding.action,
                work=replace(binding.action.work, generation=0)))),
            replace(disposition, dispatch=replace(binding, call_id=plan.topic("ingestion").offer.dispatch.call_id)))
        for variant in variants:
            with self.subTest(variant=variant.body()), self.assertRaisesRegex(ContractError, "registered repair"):
                repair_frontier(plan, (), current_failure=lambda: self.current, unchanged=(variant,),
                                verified_unchanged=lambda item: item)
        with self.assertRaisesRegex(ContractError, "exactly"):
            repair_frontier(plan, (), current_failure=lambda: self.current,
                            unchanged=(replace(disposition, package_id="query"),), verified_unchanged=lambda item: item)

    def test_no_patch_round1_then_real_round2_is_distinct_and_can_retain_previous_repair(self):
        plan = self.plan()
        catalog = self.replacement(plan.topic("catalog"), "repaired-catalog-round1")
        empty_ingestion = self.unchanged(plan, "ingestion")
        first = repair_frontier(plan, (catalog,), current_failure=lambda: self.current,
                                unchanged=(empty_ingestion,), verified_unchanged=lambda item: item)
        self.assertEqual(tuple(offer.dispatch.action.work.generation for offer in first), (1, 0, 0, 0))
        self.fx.offers = first
        self.set_selection(first, 1)
        stage = GitStore.fork(self.seed_store, self.root / "round1.git")
        for offer in first:
            package = offer.dispatch.action.work.package_id
            candidate = stage.prepare(self.stores[identity(offer)], offer.commit_oid, stage.head(),
                lambda _path: (True, "Authored scope-only fixture; no acceptance"), allowed_paths=self.scopes[package])
            self.assertEqual(stage.accept(candidate).status, "accepted")
        self.protected._git("fetch", "--no-tags", str(stage.path), stage.head())
        target = replace(self.target, generation=1, commit_oid=stage.head(),
            tree_oid=stage._git("rev-parse", stage.head() + "^{tree}"), sources=named_sources(stage.read_files()),
            selection_sha256=identity(self.fx.selection))
        self.current = target, replace(self.failure, target_sha256=identity(target))
        plan2 = self.plan()
        self.assertEqual(plan2.topic("ingestion").offer, first[1])
        self.assertEqual(plan2.topic("ingestion").new_generation, 2)
        next_empty = self.unchanged(plan2, "ingestion")
        self.assertNotEqual(next_empty.dispatch.call_id, empty_ingestion.dispatch.call_id)
        self.assertNotEqual(financial_task_id(target.context, next_empty.dispatch.action.work),
                            financial_task_id(target.context, empty_ingestion.dispatch.action.work))
        # The next actual ingestion candidate uses the distinct round2 work key,
        # while catalog's new known empty response retains its round1 candidate.
        ingestion = self.replacement(plan2.topic("ingestion"), "repaired-ingestion-round2")
        empty_catalog = self.unchanged(plan2, "catalog")
        second = repair_frontier(plan2, (ingestion,), current_failure=lambda: self.current,
                                 unchanged=(empty_catalog,), verified_unchanged=lambda item: item)
        self.assertEqual(second[0], catalog)
        self.assertEqual(tuple(offer.dispatch.action.work.generation for offer in second), (1, 2, 0, 0))
        self.assertNotEqual(ingestion.dispatch.call_id, empty_ingestion.dispatch.call_id)
        self.assertEqual(ingestion.dispatch.action.work.generation, 2)
        stage2 = GitStore.fork(self.seed_store, self.root / "round2.git")
        for offer in second:
            package = offer.dispatch.action.work.package_id
            candidate = stage2.prepare(self.stores[identity(offer)], offer.commit_oid, stage2.head(),
                lambda _path: (True, "Authored scope-only fixture; no acceptance"), allowed_paths=self.scopes[package])
            self.assertEqual(stage2.accept(candidate).status, "accepted")
        self.assertEqual(stage2.read_files(), {"seed.py": "seed bytes", "catalog.py": "repaired-catalog-round1",
            "ingestion.py": "repaired-ingestion-round2", "query.py": "source-query", "clients.py": "source-clients"})
        self.assertEqual(self.protected.head(), self.baseline)

    def test_two_parallel_topics_then_second_round_restage_with_fresh_execution_and_reviews(self):
        promotion = ProjectPromotion(self.root / "promotion", self.protected, repository_id="repository",
            baseline_sha=self.baseline, policy=self.fx.policy, initial_suite=self.fx.suite,
            package_scopes=self.scopes, verified_selection=lambda _selection: self.fx.selection,
            verified_contribution=lambda offer: self.members[identity(offer)], verified_terminal=self.fx.terminal,
            verified_view=lambda binding: self.fx.views[identity(binding)],
            verified_execution=lambda _target, _suite: self.fx.execution)
        self.addCleanup(promotion.close)

        def stage(generation, generations):
            staged = promotion.stage(self.fx.selection, self.fx.offers, self.stores,
                                     generation=generation, package_generations=generations)
            ref = self.mesh.arrive("executor", "execution-receipt", canonical_bytes({"generation": generation}))
            target = promotion.bind_target(staged, evaluator_sha256=sha("evaluator"), execution_receipt_refs=(ref,))
            self.fx.target = target
            self.fx.execution = self.fx.execution_for(target, promotion.suites[-1])
            return target

        first = stage(0, self.fx.policy.package_generations)
        # Genuine gate refusal over the physically merged source; evaluator and
        # financial evidence are explicitly trusted fakes in this offline test.
        first_reviews = self.fx.reviews(target=first)
        old_bindings = tuple(dict.fromkeys(v.review_dispatch for v in first_reviews))
        promotion.begin_review_round(first, round_number=0,
            requests=tuple((b.action.actor, b.action.request_id) for b in old_bindings))
        promotion.record_reviews(first, old_bindings)
        self.fx.execution = replace(self.fx.execution,
            check_results=tuple((check, "failed") for check, _ in self.fx.execution.check_results))
        first_failure = promotion.promote(first)
        self.assertIsInstance(first_failure, ReviewGateReceipt)
        self.current = first, first_failure
        plan = self.plan()
        directive = self.directive(plan)
        replacements = tuple(self.replacement(plan.topic(p), "round-1-" + p) for p in ("catalog", "ingestion"))
        self.fx.offers = repair_frontier(plan, replacements, current_failure=lambda: self.current)
        self.set_selection(self.fx.offers, 1)
        self.fx.policy = replace(self.fx.policy, package_generations=plan.package_generations)
        second = stage(1, plan.package_generations)
        merged = self.protected.read_files(second.commit_oid)
        self.assertEqual((merged["catalog.py"], merged["ingestion.py"]), ("round-1-catalog", "round-1-ingestion"))
        self.assertEqual(merged["query.py"], "source-query")
        self.assertEqual(self.protected.head(), self.baseline)
        self.assertEqual(verify_repair_directive(plan, directive, self.mesh).base_sha, plan.topic("catalog").base_sha)
        with self.assertRaises(ContractError):
            promotion.record_reviews(second, old_bindings)
        missing = promotion.promote(second)
        self.assertFalse(missing.eligible)
        self.assertEqual(len(missing.missing_slots), 8)
        # Repair a previously untouched gen0 package plus catalog again. Package
        # generations become catalog2, ingestion1, query2, clients0, not all2.
        self.fx.execution = replace(self.fx.execution,
            check_results=tuple((check, "failed") for check, _ in self.fx.execution.check_results))
        self.current = second, promotion.promote(second)
        plan2 = self.plan(repair_packages=("catalog", "query"))
        self.assertEqual(plan2.topic("catalog").base_sha, replacements[0].commit_oid)
        more = tuple(self.replacement(plan2.topic(p), "round-2-" + p) for p in ("catalog", "query"))
        self.fx.offers = repair_frontier(plan2, more, current_failure=lambda: self.current)
        self.set_selection(self.fx.offers, 2)
        self.fx.policy = replace(self.fx.policy, package_generations=plan2.package_generations)
        third = stage(2, plan2.package_generations)
        self.assertEqual(plan2.package_generations, (("catalog", 2), ("ingestion", 1), ("query", 2), ("clients", 0)))
        self.assertEqual(self.protected.read_files(third.commit_oid), {"seed.py": "seed bytes", "catalog.py": "round-2-catalog",
            "ingestion.py": "round-1-ingestion", "query.py": "round-2-query", "clients.py": "source-clients"})
        # Fresh receipt must name this final combined tree, not the prior target.
        self.fx.execution = self.fx.execution_for(second, promotion.suites[-1])
        with self.assertRaisesRegex(ContractError, "provenance"):
            promotion.promote(third)
        self.fx.execution = self.fx.execution_for(third, promotion.suites[-1])
        fresh = self.fx.reviews(target=third)
        bindings = tuple(dict.fromkeys(v.review_dispatch for v in fresh))
        # Fixture builder generates stable reviewer request names; adapt the
        # trusted fake before dispatch to model distinct round registrations.
        rewritten = []
        for binding in bindings:
            new = replace(binding, call_id=binding.call_id + "-fresh", reservation_id=binding.reservation_id + "-fresh",
                action=replace(binding.action, action_id=binding.action.action_id + "-fresh",
                               request_id=binding.action.request_id + "-fresh"))
            proof = self.fx.proofs[identity(binding)]
            self.fx.put_result(new, proof["worker_request"], proof["result"])
            self.fx.views[identity(new)] = self.fx.views[identity(binding)]
            rewritten.append(new)
        promotion.begin_review_round(third, round_number=0,
            requests=tuple((b.action.actor, b.action.request_id) for b in rewritten))
        promotion.record_reviews(third, tuple(rewritten))
        result = promotion.promote(third)
        self.assertIsInstance(result, PromotionResult)
        self.assertEqual(self.protected.head(), third.commit_oid)


if __name__ == "__main__":
    unittest.main()
