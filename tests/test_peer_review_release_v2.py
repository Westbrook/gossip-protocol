"""Pure gate controls and injected provider parsing; no Git, Docker or API calls."""
from __future__ import annotations

from dataclasses import asdict, replace
import hashlib
import json
import unittest

from gossip_harness.ledger import Lease
from gossip_harness.peer_project_contract_v2 import (
    ActionRequest, CandidateOffer, Context, ContractError, DispatchBinding, DispatchReply,
    EvidenceRef, LocalViewManifest, NamedSource, ReleaseTarget, SelectedOffer,
    SelectionManifest, WorkKey, canonical_bytes, encode, identity, to_dict, worker_request_digest,
)
from gossip_harness.peer_review_release_v2 import (
    PROTOCOL, SLOTS, AdmittedRefutation, PublicExecutionEvidence, RequiredCheck,
    RequiredSuite, ReviewerProfile, ReviewPolicy, ScopeRule, evaluate_release,
    extend_required_suite, materialize_verdicts, policy_digest, receipt_digest,
    suite_digest,
)
from gossip_harness.worker import HTTPResponse, MODEL, OpenAIWorker, WorkerRequest, WorkerResult


def sha(value):
    return hashlib.sha256(value.encode()).hexdigest()


def evidence(name, kind, payload=None, producer="service"):
    return EvidenceRef(sha("event-" + name), producer, kind,
                       hashlib.sha256(payload).hexdigest() if payload is not None else sha(name))


class Fixture:
    """Trusted callback fakes, deliberately separate from submitted peer verdicts."""

    def __init__(self):
        self.context = Context(sha("contract"), "cohort", "trajectory", 1, sha("requirements"))
        self.suite = RequiredSuite(self.context, (RequiredCheck("public", sha("assertion-1")),
            RequiredCheck("inherited", sha("assertion-2")), RequiredCheck("diagonal", sha("assertion-3"))))
        requirements = ("catalog", "ingestion", "query", "clients", "storage-query", "ingestion-client")
        coverage = (("catalog", "storage-query"), ("clients",), ("catalog",),
                    ("ingestion", "ingestion-client"), ("ingestion",),
                    ("query", "storage-query"), ("query",), ("clients", "ingestion-client"))
        self.policy = ReviewPolicy(self.context,
            tuple(ScopeRule(*slot, covered) for slot, covered in zip(SLOTS, coverage)),
            tuple(ReviewerProfile(actor, "reviewer", sha("profile")) for actor in ("R1", "R2", "R3", "R4")),
            requirements, ("storage-query",), ("ingestion-client",), sha("eligibility"),
            suite_digest(self.suite), sha("execution-environment"), sha("authority"),
            tuple((package, 0) for package in ("catalog", "ingestion", "query", "clients")),
            (NamedSource("seed.py", sha("seed bytes")),))
        self.members = {}
        offers = []
        for package in ("catalog", "ingestion", "query", "clients"):
            binding = self.binding("B-" + package, "build", package)
            offer = CandidateOffer(binding, sha(package)[:40], sha("full-" + package),
                sha("result-" + package), sha("bundle-" + package),
                evidence(package, "candidate-bundle", producer=binding.action.actor))
            offers.append(offer)
            self.members[identity(offer)] = (NamedSource(package + ".py", sha("source-" + package)),)
        self.offers = tuple(offers)
        selecting = self.binding("B-selector", "select_source", "catalog")
        self.selection = SelectionManifest(self.context, self.policy.eligibility_policy_sha256,
            selecting, selecting.action.view_manifest_sha256, tuple(identity(offer) for offer in offers), (),
            tuple(SelectedOffer(offer.dispatch.action.work.package_id, identity(offer)) for offer in offers))
        sources = tuple(sorted((*self.policy.inherited_sources,
            *(source for sources in self.members.values() for source in sources)), key=lambda source: source.path))
        self.target = ReleaseTarget(self.context, 0, "repository", "refs/heads/protected", "a" * 40,
            "sha1", "b" * 40, "c" * 40, sources, identity(self.selection), suite_digest(self.suite),
            sha("evaluator"), (evidence("execution", "public-execution"),))
        self.history = (self.suite,)
        self.proofs, self.views, self.reads = {}, {}, []
        self.execution = self.execution_for(self.target, self.suite)
        self.verdicts = self.reviews()
        self.reads.clear()

    def binding(self, actor, kind, package, *, view=None, request=None, target=None):
        request = request or WorkerRequest("task-" + actor, "Return bounded artifact.", ("review.json",),
                                           {}, "a" * 40, 1)
        target = target or getattr(self, "target", None)
        action = ActionRequest(self.context, "action-" + actor, "request-" + actor, actor, kind,
            WorkKey(package, "requirement", "slot-" + actor, target.generation if target else 0),
            "reviewer" if kind == "review" else "builder",
            evidence(actor, "worker-request", producer=actor),
            identity(view) if view else sha("view-" + actor))
        return DispatchBinding(action, Lease("task-" + actor, actor, 1, 100.0),
            worker_request_digest(request), sha("profile"), sha("authority"),
            "call-" + actor, "reservation-" + actor, 100000)

    def execution_for(self, target, suite):
        return PublicExecutionEvidence(target.context, target.commit_oid, target.tree_oid, target.sources,
            suite_digest(suite), target.evaluator_sha256, self.policy.execution_provenance_sha256,
            target.execution_receipt_refs, tuple((check.check_id, "passed") for check in suite.checks))

    def terminal(self, binding):
        self.reads.append(binding.call_id)
        return self.proofs[identity(binding)]

    def put_result(self, binding, request, result):
        payload = {"kind": "result", "payload": asdict(result)}
        digest = hashlib.sha256(canonical_bytes(payload)).hexdigest()
        reply = DispatchReply(binding.action.request_id, identity(binding.action), "completed", "accounted",
                              binding, digest, result.usage_units)
        self.proofs[identity(binding)] = {"binding": binding, "action": binding.action, "reply": reply,
            "result": result, "result_payload": payload, "worker_request": request}

    def reviews(self, *, edit=None, target=None):
        target = target or self.target
        verdicts = []
        for actor in ("R1", "R2", "R3", "R4"):
            target_ref = evidence("target", "release-target", encode(target))
            selection_ref = evidence("selection", "selection-manifest", encode(self.selection))
            refs = (target_ref, selection_ref, *target.execution_receipt_refs)
            view = LocalViewManifest(self.context, policy_digest(self.policy), target_ref,
                refs[1:], tuple(ref.event_id for ref in refs), (), sha("materialized-" + actor))
            request = WorkerRequest("task-" + actor, "Write review.json for the exact supplied target.",
                ("review.json",), {"context/release.json": encode(target).decode()}, target.expected_head, 1)
            binding = self.binding(actor, "review", "catalog" if actor in ("R1", "R2") else "query",
                                   view=view, request=request, target=target)
            decisions = [{"scope_id": rule.scope_id, "covered_requirement_ids": list(rule.required_requirement_ids),
                "evidence_refs": [to_dict(ref) for ref in refs], "verdict": "approve", "rationale": "Reviewed."}
                for rule in self.policy.slots if rule.reviewer == actor]
            body = {"protocol": PROTOCOL, "target_sha256": identity(target), "verdicts": decisions}
            if edit:
                edit(actor, body)
            self.put_result(binding, request, WorkerResult({"review.json": json.dumps(body)}, "Review recorded.", 4, {}))
            self.views[identity(binding)] = view
            verdicts.extend(materialize_verdicts(binding, target, self.terminal))
        return tuple(verdicts)

    def gate(self, **changes):
        arguments = dict(target=self.target, selection=self.selection, offers=self.offers, policy=self.policy,
            suite_history=self.history, verdicts=self.verdicts, refutations=(), verified_terminal=self.terminal,
            verified_view=lambda binding: self.views[identity(binding)],
            verified_contribution=lambda offer: self.members[identity(offer)],
            verified_execution=lambda target, suite: self.execution,
            verified_selection=lambda selection: self.selection)
        return evaluate_release(**(arguments | changes))


class ReviewReleaseV2Tests(unittest.TestCase):
    def setUp(self):
        self.fx = Fixture()

    def test_eight_scopes_come_from_four_charged_calls_and_stable_receipt(self):
        receipt = self.fx.gate()
        self.assertTrue(receipt.eligible)
        self.assertEqual(len(receipt.verdict_sha256s), 8)
        self.assertEqual(len(receipt.call_ids), 4)
        self.assertEqual(len(self.fx.reads), 4)
        self.assertEqual(receipt.purpose, "public_release")
        self.assertFalse(receipt.independent_final_project_acceptance)
        self.assertEqual(receipt_digest(receipt), receipt_digest(self.fx.gate(offers=tuple(reversed(self.fx.offers)))))

    def test_missing_second_scope_remains_missing(self):
        self.fx.verdicts = self.fx.reviews(edit=lambda actor, body:
            body["verdicts"].pop() if actor == "R1" else None)
        result = self.fx.gate()
        self.assertFalse(result.eligible)
        self.assertEqual(result.missing_slots, (("R1", "clients"),))

    def test_request_changes_and_insufficient_evidence_do_not_pass(self):
        for value in ("request_changes", "insufficient_evidence"):
            with self.subTest(value=value):
                self.fx.verdicts = self.fx.reviews(edit=lambda actor, body:
                    body["verdicts"][0].update(verdict=value) if actor == "R2" else None)
                result = self.fx.gate()
                self.assertFalse(result.eligible)
                self.assertEqual(result.rejected_slots, (("R2", "catalog"),))

    def test_peer_forged_verdict_cannot_replace_actual_response(self):
        for changes in ({"rationale": "Forged"}, {"result_payload_sha256": sha("forged")},
                        {"verdict": "request_changes"}):
            with self.subTest(changes=changes), self.assertRaisesRegex(ContractError, "actual worker"):
                self.fx.gate(verdicts=(replace(self.fx.verdicts[0], **changes), *self.fx.verdicts[1:]))

    def test_failed_unknown_and_unsettled_terminal_states_fail_closed(self):
        binding = self.fx.verdicts[0].review_dispatch
        original = self.fx.proofs[identity(binding)]["reply"]
        for state, usage in (("failed", original.usage_units), ("unknown", None),
                             ("publication_pending", None)):
            self.fx.proofs[identity(binding)]["reply"] = replace(original, state=state, usage_units=usage)
            with self.subTest(state=state), self.assertRaisesRegex(ContractError, "successful"):
                self.fx.gate()

    def test_worker_payload_and_accounting_tampering_is_rejected(self):
        binding = self.fx.verdicts[0].review_dispatch
        original = self.fx.proofs[identity(binding)]
        for change in ({"result_payload": {"kind": "result", "payload": {}}},
                       {"result": replace(original["result"], usage_units=3)},
                       {"result": replace(original["result"], metadata={"halt": True})},
                       {"action": replace(binding.action, request_id="another-request")}):
            self.fx.proofs[identity(binding)] = original | change
            with self.subTest(change=change), self.assertRaises(ContractError):
                self.fx.gate()

    def test_wrong_request_path_or_normalized_request_digest_is_rejected(self):
        binding = self.fx.verdicts[0].review_dispatch
        proof = self.fx.proofs[identity(binding)]
        original = proof["worker_request"]
        for request in (replace(original, allowed_paths=("source.py",)),
                        replace(original, instructions="Changed after dispatch")):
            proof["worker_request"] = request
            with self.subTest(request=request), self.assertRaisesRegex(ContractError, "exact bound"):
                self.fx.gate()

    def test_review_artifact_schema_duplicate_keys_and_extra_output_fail_closed(self):
        binding = self.fx.verdicts[0].review_dispatch
        proof = self.fx.proofs[identity(binding)]
        request, result = proof["worker_request"], proof["result"]
        bodies = ["{\"protocol\":\"a\",\"protocol\":\"b\"}",
                  json.dumps({"protocol": PROTOCOL, "target_sha256": identity(self.fx.target), "verdicts": []})]
        for body in bodies:
            self.fx.put_result(binding, request, replace(result, changes={"review.json": body}))
            with self.subTest(body=body), self.assertRaises(ContractError):
                materialize_verdicts(binding, self.fx.target, self.fx.terminal)
        self.fx.put_result(binding, request, replace(result, changes=result.changes | {"catalog.py": "edited"}))
        with self.assertRaisesRegex(ContractError, "exactly"):
            materialize_verdicts(binding, self.fx.target, self.fx.terminal)

    def test_wrong_profile_authority_and_repeated_scope_are_rejected(self):
        first = self.fx.verdicts[0]
        for binding in (replace(first.review_dispatch, profile_sha256=sha("wrong-profile")),
                        replace(first.review_dispatch, authority_config_sha256=sha("wrong-authority"))):
            with self.subTest(binding=binding), self.assertRaisesRegex(ContractError, "provenance"):
                self.fx.gate(verdicts=(replace(first, review_dispatch=binding), *self.fx.verdicts[1:]))
        with self.assertRaises(ContractError):
            self.fx.gate(verdicts=(*self.fx.verdicts, first))

    def test_all_target_identity_changes_invalidate_prior_approvals(self):
        mutations = ({"expected_head": "d" * 40}, {"commit_oid": "d" * 40},
                     {"tree_oid": "d" * 40}, {"generation": 1},
                     {"execution_receipt_refs": (evidence("new", "public-execution"),)},
                     {"evaluator_sha256": sha("new-evaluator")})
        for mutation in mutations:
            target = replace(self.fx.target, **mutation)
            with self.subTest(mutation=mutation), self.assertRaises(ContractError):
                self.fx.gate(target=target, verified_execution=lambda t, s: self.fx.execution_for(t, s))

    def test_unrelated_working_branch_movement_has_no_gate_input(self):
        before = self.fx.gate()
        unrelated_heads = {"working": "d" * 40}
        unrelated_heads["working"] = "e" * 40
        self.assertEqual(before, self.fx.gate())

    def test_named_sources_must_equal_all_four_contributions_plus_frozen_seed(self):
        for sources in (self.fx.target.sources[:-1],
                        (replace(self.fx.target.sources[0], sha256=sha("changed")), *self.fx.target.sources[1:])):
            with self.subTest(sources=sources), self.assertRaisesRegex(ContractError, "whole-four-package"):
                self.fx.gate(target=replace(self.fx.target, sources=sources))
        duplicate = self.fx.policy.inherited_sources
        with self.assertRaisesRegex(ContractError, "overlap"):
            self.fx.gate(verified_contribution=lambda offer: duplicate)

    def test_selection_cannot_omit_package_or_rebind_eligible_source(self):
        incomplete = replace(self.fx.selection, selected=self.fx.selection.selected[:-1])
        with self.assertRaisesRegex(ContractError, "four packages"):
            self.fx.gate(selection=incomplete, target=replace(self.fx.target, selection_sha256=identity(incomplete)),
                         verified_selection=lambda selection: selection)
        with self.assertRaisesRegex(ContractError, "membership"):
            self.fx.gate(offers=self.fx.offers[:-1])
        with self.assertRaisesRegex(ContractError, "eligibility"):
            self.fx.gate(policy=replace(self.fx.policy, eligibility_policy_sha256=sha("another")))

    def test_production_author_cannot_supply_release_approval(self):
        offer = self.fx.offers[0]
        binding = offer.dispatch
        action = replace(binding.action, actor="R1", worker_payload_ref=replace(binding.action.worker_payload_ref, producer="R1"))
        binding = replace(binding, action=action, lease=replace(binding.lease, worker_id="R1"))
        changed = replace(offer, dispatch=binding, bundle_ref=replace(offer.bundle_ref, producer="R1"))
        self.fx.members[identity(changed)] = self.fx.members[identity(offer)]
        offers = (changed, *self.fx.offers[1:])
        selected = (replace(self.fx.selection.selected[0], offer_sha256=identity(changed)), *self.fx.selection.selected[1:])
        selection = replace(self.fx.selection, eligible_offer_sha256s=tuple(identity(item) for item in offers), selected=selected)
        target = replace(self.fx.target, selection_sha256=identity(selection))
        with self.assertRaisesRegex(ContractError, "author"):
            self.fx.gate(target=target, selection=selection, offers=offers,
                         verified_selection=lambda selection: selection,
                         verified_execution=lambda t, s: self.fx.execution_for(t, s))

    def test_forged_selection_and_wrong_candidate_generation_are_rejected(self):
        forged = replace(self.fx.selection, missing_slots=("unverified-absence",))
        with self.assertRaisesRegex(ContractError, "selector provenance"):
            self.fx.gate(selection=forged, target=replace(self.fx.target, selection_sha256=identity(forged)))
        generations = (("catalog", 1), *self.fx.policy.package_generations[1:])
        with self.assertRaisesRegex(ContractError, "generation"):
            self.fx.gate(policy=replace(self.fx.policy, package_generations=generations))

    def test_genuine_selection_from_another_authority_cannot_cross_registration(self):
        dispatch = replace(self.fx.selection.selecting_dispatch, authority_config_sha256=sha("other-authority"))
        selection = replace(self.fx.selection, selecting_dispatch=dispatch)
        target = replace(self.fx.target, selection_sha256=identity(selection))
        with self.assertRaisesRegex(ContractError, "Selector authority"):
            self.fx.gate(target=target, selection=selection, verified_selection=lambda value: value)
        original = self.fx.offers[0]
        offer = replace(original, dispatch=replace(original.dispatch, authority_config_sha256=sha("other-authority")))
        offers = (offer, *self.fx.offers[1:])
        selection = replace(self.fx.selection, eligible_offer_sha256s=tuple(identity(item) for item in offers),
            selected=(replace(self.fx.selection.selected[0], offer_sha256=identity(offer)), *self.fx.selection.selected[1:]))
        target = replace(self.fx.target, selection_sha256=identity(selection))
        with self.assertRaisesRegex(ContractError, "candidate authority"):
            self.fx.gate(target=target, selection=selection, offers=offers, verified_selection=lambda value: value)

    def test_diagonal_requirements_need_named_reviewers_and_exact_scope_coverage(self):
        slots = list(self.fx.policy.slots)
        slots[0] = replace(slots[0], required_requirement_ids=("catalog",))
        with self.assertRaises(ContractError):
            replace(self.fx.policy, slots=tuple(slots))
        self.fx.verdicts = self.fx.reviews(edit=lambda actor, body:
            body["verdicts"][0].update(covered_requirement_ids=["catalog"]) if actor == "R1" else None)
        with self.assertRaisesRegex(ContractError, "diagonal"):
            self.fx.gate()

    def test_diagonal_coverage_cannot_be_moved_to_unrelated_package_scopes(self):
        for requirement, remove_indices, add_indices in (
                ("storage-query", (0, 5), (1, 4)), ("ingestion-client", (3, 7), (2, 6))):
            slots = list(self.fx.policy.slots)
            for index in remove_indices:
                slots[index] = replace(slots[index], required_requirement_ids=tuple(
                    value for value in slots[index].required_requirement_ids if value != requirement))
            for index in add_indices:
                slots[index] = replace(slots[index], required_requirement_ids=(
                    *slots[index].required_requirement_ids, requirement))
            with self.subTest(requirement=requirement), self.assertRaisesRegex(ContractError, "endpoint"):
                replace(self.fx.policy, slots=tuple(slots))

    def test_materialized_view_and_actual_evidence_provenance_must_match(self):
        binding = self.fx.verdicts[0].review_dispatch
        original = self.fx.views[identity(binding)]
        self.fx.views[identity(binding)] = replace(original, materialized_content_sha256=sha("another view"))
        with self.assertRaisesRegex(ContractError, "materialized"):
            self.fx.gate()
        self.fx.views[identity(binding)] = original
        self.fx.verdicts = self.fx.reviews(edit=lambda actor, body:
            body["verdicts"][0]["evidence_refs"].pop() if actor == "R1" else None)
        with self.assertRaisesRegex(ContractError, "omits"):
            self.fx.gate()

    def test_admitted_required_checks_cannot_be_dropped_changed_or_reordered(self):
        previous = self.fx.suite
        invalid = (RequiredSuite(previous.context, previous.checks[:-1]),
            RequiredSuite(previous.context, tuple(reversed(previous.checks))),
            RequiredSuite(previous.context, (*previous.checks[:-1], replace(previous.checks[-1], assertion_sha256=sha("weakened")))))
        for after in invalid:
            with self.subTest(after=after), self.assertRaisesRegex(ContractError, "monotonically"):
                self.fx.gate(suite_history=(previous, after))
        with self.assertRaisesRegex(ContractError, "Repeated check"):
            extend_required_suite(previous, (replace(previous.checks[0], assertion_sha256=sha("weakened")),))

    def test_grown_suite_requires_new_execution_and_fresh_approvals(self):
        after = extend_required_suite(self.fx.suite, (RequiredCheck("admitted-failure", sha("assertion-4")),))
        self.fx.history = (self.fx.suite, after)
        with self.assertRaisesRegex(ContractError, "latest required suite"):
            self.fx.gate()
        self.fx.target = replace(self.fx.target, required_suite_sha256=suite_digest(after))
        self.fx.execution = self.fx.execution_for(self.fx.target, after)
        with self.assertRaisesRegex(ContractError, "another target"):
            self.fx.gate()
        self.fx.verdicts = self.fx.reviews()
        self.assertTrue(self.fx.gate().eligible)

    def test_required_failure_unknown_and_wrong_execution_provenance_block_release(self):
        original = self.fx.execution
        for status in ("failed", "unknown", "infrastructure_failure"):
            self.fx.execution = replace(original, check_results=(("public", status), *original.check_results[1:]))
            with self.subTest(status=status):
                self.assertFalse(self.fx.gate().eligible)
        self.fx.execution = replace(original, provenance_sha256=sha("another-runtime"))
        with self.assertRaisesRegex(ContractError, "provenance"):
            self.fx.gate()
        self.fx.execution = replace(original, check_results=original.check_results[:-1])
        with self.assertRaisesRegex(ContractError, "omits"):
            self.fx.gate()

    def test_admitted_refutation_invalidates_only_affected_exact_target_scopes(self):
        refutation = AdmittedRefutation(identity(self.fx.target), ("catalog",), evidence("refute", "refutation"))
        result = self.fx.gate(refutations=(refutation,))
        self.assertFalse(result.eligible)
        self.assertEqual(result.rejected_slots, (("R1", "catalog"), ("R2", "catalog")))
        self.assertTrue(self.fx.gate(refutations=(replace(refutation, target_sha256=sha("other-target")),)).eligible)

    def test_independent_final_acceptance_cannot_be_substituted(self):
        with self.assertRaises(ContractError):
            replace(self.fx.target, purpose="independent_final_acceptance")
        with self.assertRaises(ContractError):
            replace(self.fx.execution, purpose="independent_final_acceptance")

    def test_real_worker_parser_emits_dedicated_review_artifact_from_injected_http(self):
        binding = self.fx.verdicts[0].review_dispatch
        proof = self.fx.proofs[identity(binding)]
        data = proof["result"].changes["review.json"]
        proposal = {"changes": [{"path": "review.json", "content": data}], "summary": "Scoped review complete."}
        response = {"id": "offline-review", "model": MODEL, "status": "completed", "service_tier": "default",
            "usage": {"input_tokens": 30, "output_tokens": 30}, "output": [{"type": "message", "role": "assistant",
                "status": "completed", "content": [{"type": "output_text", "text": json.dumps(proposal)}]}]}
        calls = []

        def transport(request, timeout, limit):
            calls.append(json.loads(request.data))
            return HTTPResponse(200, {}, json.dumps(response).encode())

        result = OpenAIWorker("offline-test-only", transport=transport).run(proof["worker_request"])
        self.fx.put_result(binding, proof["worker_request"], result)
        verdicts = materialize_verdicts(binding, self.fx.target, self.fx.terminal)
        self.assertEqual(len(verdicts), 2)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["text"]["format"]["schema"]["properties"]["changes"]["items"]["properties"]["path"]["enum"],
                         ["review.json"])
        self.assertIn("context/release.json", json.loads(calls[0]["input"])["files"])


if __name__ == "__main__":
    unittest.main()
