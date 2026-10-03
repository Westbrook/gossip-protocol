"""Exact local cognitive views; fake finance, no API, Docker or candidate execution."""
from __future__ import annotations

from dataclasses import replace
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from gossip_harness.ledger import Lease
from gossip_harness.peer_project_contract_v2 import (
    ActionRequest, CandidateOffer, Context, DispatchBinding, EvidenceRef, NamedSource,
    ReleaseTarget, SelectedOffer, SelectionManifest, WorkKey, canonical_bytes,
    encode, identity, to_dict, worker_request_digest,
)
from gossip_harness.peer_project_views_v3 import (
    CONTRIBUTION_PROTOCOL, FRONTIER_PROTOCOL, PROTOCOL, ProjectRoleLoopV3,
    effective_runtime_policy_sha256, materialize_project, verify_materialized_view,
)
from gossip_harness.peer_role_loop_v2 import RoleError, RoleLoop, WorkDirective, directive_id
from tests.test_peer_role_loop_v2 import FakeFinance, FakeMesh


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


class PeerProjectViewsV3Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.mesh = FakeMesh("B01")
        self.finance = FakeFinance(self.mesh, lambda: 100.0)
        self.context = Context("a" * 64, "cohort", "trajectory", 1, "b" * 64)
        self.policy = "c" * 64
        self.files = {f"library/{name}/api.py": f"{name} source\n"
                      for name in ("catalog", "ingestion", "query", "clients")}
        self.files["library/common.py"] = "inherited\n"
        self.source = self.mesh.arrive("registry", "project-source", canonical_bytes(
            {"files": self.files, "base_sha": "a" * 40}))

    def directive(self, **changes):
        fields = dict(context=self.context, work=WorkKey("catalog", "feature", "slot-1", 0),
            profile_id="builder", kind="build", source_ref=self.source, evidence_refs=(),
            allowed_paths=("library/catalog/api.py",), instructions="Implement the requirement.",
            feedback="Existing exact feedback")
        fields.update(changes)
        return WorkDirective(**fields)

    def dispatch(self, directive, request, view, actor="B01"):
        key = directive_id(directive)
        request_ref = self.mesh.arrive(actor, "worker-request", b"request")
        action = ActionRequest(self.context, "action-" + key, "dispatch-" + key, actor,
            directive.kind, directive.work, directive.profile_id, request_ref, identity(view))
        return DispatchBinding(action, Lease(request.task_id, actor, 1, 200.0),
            worker_request_digest(request), "d" * 64, "e" * 64, "call-" + key, "reservation", 10)

    def candidates(self):
        records, refs, contributions, entries = [], [], [], []
        for index, package in enumerate(("catalog", "clients", "ingestion", "query"), 1):
            actor = f"B{index:02d}"
            work = WorkKey(package, "feature", "slot-1", 0)
            action = ActionRequest(self.context, f"action-{index}", f"dispatch-{index}", actor,
                "build", work, "builder", self.mesh.arrive(actor, "worker-request", b"request"), "f" * 64)
            dispatch = DispatchBinding(action, Lease(f"task-{index}", actor, 1, 200.0),
                "d" * 64, "e" * 64, "f" * 64, f"call-{index}", f"reservation-{index}", 10)
            offer = CandidateOffer(dispatch, str(index) * 40, "a" * 64, "b" * 64, "c" * 64,
                self.mesh.arrive(actor, "candidate-bundle", b"bundle"))
            ref = self.mesh.arrive("registry", "candidate-offer", encode(offer))
            contribution = self.mesh.arrive("registry", "candidate-contribution", canonical_bytes({
                "protocol": CONTRIBUTION_PROTOCOL, "offer_sha256": identity(offer),
                "commit_oid": offer.commit_oid, "files": {
                    f"library/{package}/api.py": self.files[f"library/{package}/api.py"]}}))
            records.append(offer)
            refs.append(ref)
            contributions.append(contribution)
            entries.append({"slot": to_dict(work), "actor": actor, "offer_sha256": identity(offer),
                            "offer_ref": to_dict(ref), "contribution_ref": to_dict(contribution)})
        frontier = self.mesh.arrive("registry", "selection-frontier", canonical_bytes({
            "protocol": FRONTIER_PROTOCOL, "context": to_dict(self.context),
            "eligibility_policy_sha256": self.policy, "offers": entries, "missing_slots": []}))
        return tuple(records), tuple(refs), tuple(contributions), frontier

    def review(self):
        offers, refs, contributions, frontier = self.candidates()
        selection_directive = self.directive(kind="select_source", allowed_paths=("selection.json",),
            evidence_refs=(frontier, *refs, *contributions))
        request, view = materialize_project(selection_directive, self.mesh, self.policy)
        selection = SelectionManifest(self.context, self.policy,
            self.dispatch(selection_directive, request, view, "R1"), identity(view),
            tuple(identity(offer) for offer in offers), (), tuple(SelectedOffer(package,
                identity(next(offer for offer in offers if offer.dispatch.action.work.package_id == package)))
                for package in ("catalog", "ingestion", "query", "clients")))
        selection_ref = self.mesh.arrive("registry", "selection-manifest", encode(selection))
        receipt = self.mesh.arrive("validator", "execution-receipt", b'{"actual":"execution"}')
        target = ReleaseTarget(self.context, 0, "project", "refs/heads/accepted", "b" * 40,
            "sha1", "a" * 40, "c" * 40, tuple(NamedSource(path, digest(body.encode()))
                for path, body in sorted(self.files.items())), identity(selection), "d" * 64,
            "e" * 64, (receipt,))
        target_ref = self.mesh.arrive("registry", "release-target", encode(target))
        directive = self.directive(kind="review", source_ref=target_ref, profile_id="reviewer",
            evidence_refs=(self.source, selection_ref, receipt, *refs, *contributions),
            allowed_paths=("review.json",))
        return directive, target

    def test_complete_builder_recipe_binds_every_byte_and_reconstructs(self):
        evidence = self.mesh.arrive("requirements", "requirements", b'quoted "data"\ncomplete')
        directive = self.directive(evidence_refs=(evidence,))
        request, view = materialize_project(directive, self.mesh, self.policy)
        recipe = json.loads(request.feedback)
        self.assertEqual(recipe["protocol"], PROTOCOL)
        self.assertEqual(recipe["directive"]["feedback"], directive.feedback)
        self.assertEqual(recipe["materials"][1]["utf8"], 'quoted "data"\ncomplete')
        binding = self.dispatch(directive, request, view)
        self.assertEqual(verify_materialized_view(binding, request, view, self.mesh,
                         policy_sha256=self.policy), view)

    def test_missing_notice_or_payload_waits_without_partial_view(self):
        evidence = self.mesh.arrive("requirements", "requirements", b"required")
        directive = self.directive(evidence_refs=(evidence,))
        self.mesh.refs.remove(evidence)
        self.assertIsNone(materialize_project(directive, self.mesh, self.policy))
        self.mesh.refs.append(evidence)
        self.mesh.blobs.pop(evidence.event_id)
        self.assertIsNone(materialize_project(directive, self.mesh, self.policy))
        self.assertEqual(self.mesh.wants, [evidence, evidence])

    def test_hash_changed_payload_and_fabricated_notice_rejected(self):
        self.mesh.blobs[self.source.event_id] = b"changed"
        with self.assertRaises(ValueError):
            materialize_project(self.directive(), self.mesh, self.policy)
        fabricated = replace(self.source, event_id="9" * 64)
        self.assertIsNone(materialize_project(self.directive(source_ref=fabricated), self.mesh, self.policy))

    def test_binary_evidence_fails_instead_of_being_omitted(self):
        evidence = self.mesh.arrive("requirements", "requirements", b"\xff")
        with self.assertRaisesRegex(RoleError, "UTF8"):
            materialize_project(self.directive(evidence_refs=(evidence,)), self.mesh, self.policy)

    def test_source_packet_exact_fields_and_commit_required(self):
        for packet in ({"files": self.files, "base_sha": "main"},
                       {"files": self.files, "base_sha": "a" * 40, "ignored": "x"}):
            ref = self.mesh.arrive("registry", "project-source", canonical_bytes(packet))
            with self.assertRaises(RoleError):
                materialize_project(self.directive(source_ref=ref), self.mesh, self.policy)

    def test_review_target_full_source_and_selected_bytes_materialized(self):
        directive, target = self.review()
        request, view = materialize_project(directive, self.mesh, self.policy)
        self.assertEqual(request.files, self.files)
        self.assertEqual(request.base_sha, target.commit_oid)
        self.assertEqual(view.source_ref.kind, "release-target")
        self.assertEqual(len(json.loads(request.feedback)["materials"]), len(directive.required_refs))
        self.assertEqual(verify_materialized_view(self.dispatch(directive, request, view, "R1"),
            request, view, self.mesh, policy_sha256=self.policy), view)

    def test_review_wrong_commit_or_source_bytes_cannot_claim_target(self):
        directive, _ = self.review()
        for packet in ({"files": self.files, "base_sha": "f" * 40},
                       {"files": {**self.files, "library/common.py": "wrong"}, "base_sha": "a" * 40}):
            ref = self.mesh.arrive("registry", "project-source", canonical_bytes(packet))
            altered = replace(directive, evidence_refs=(ref, *directive.evidence_refs[1:]))
            with self.assertRaisesRegex(RoleError, "differs from target"):
                materialize_project(altered, self.mesh, self.policy)

    def test_review_missing_execution_or_candidate_evidence_rejected(self):
        directive, target = self.review()
        for kind in ("execution-receipt", "candidate-offer", "candidate-contribution", "selection-manifest"):
            altered = replace(directive, evidence_refs=tuple(ref for ref in directive.evidence_refs if ref.kind != kind))
            with self.subTest(kind=kind), self.assertRaises(RoleError):
                materialize_project(altered, self.mesh, self.policy)
        with self.assertRaises(RoleError):
            materialize_project(replace(directive, work=replace(directive.work, generation=1)),
                                self.mesh, self.policy)

    def test_review_writable_scope_cannot_include_source(self):
        directive, _ = self.review()
        with self.assertRaises(RoleError):
            materialize_project(replace(directive, allowed_paths=("review.json", "library/common.py")),
                                self.mesh, self.policy)

    def test_selector_materializes_full_frontier_and_owned_source_bytes(self):
        _, refs, contributions, frontier = self.candidates()
        directive = self.directive(kind="select_source", allowed_paths=("selection.json",),
            evidence_refs=(frontier, *refs, *contributions))
        request, _ = materialize_project(directive, self.mesh, self.policy)
        materials = json.loads(request.feedback)["materials"]
        self.assertEqual({entry["ref"]["event_id"] for entry in materials},
                         {ref.event_id for ref in directive.required_refs})
        self.assertIn("query source", request.feedback)

    def test_selector_missing_frontier_member_or_substituted_ref_rejected(self):
        _, refs, contributions, frontier = self.candidates()
        directive = self.directive(kind="select_source", allowed_paths=("selection.json",),
            evidence_refs=(frontier, *refs, *contributions))
        with self.assertRaises(RoleError):
            materialize_project(replace(directive, evidence_refs=(frontier, *refs[1:], *contributions[1:])),
                                self.mesh, self.policy)
        new_ref = self.mesh.arrive("registry", "candidate-offer", self.mesh.resolve(refs[0]), token="other")
        with self.assertRaisesRegex(RoleError, "reference differs"):
            materialize_project(replace(directive, evidence_refs=(frontier, new_ref, *refs[1:], *contributions)),
                                self.mesh, self.policy)

    def test_selector_incomplete_frontier_is_not_silently_eligible(self):
        _, refs, contributions, frontier = self.candidates()
        raw = json.loads(self.mesh.resolve(frontier))
        raw["missing_slots"] = ["missing"]
        bad = self.mesh.arrive("registry", "selection-frontier", canonical_bytes(raw))
        with self.assertRaisesRegex(RoleError, "complete bounded"):
            materialize_project(self.directive(kind="select_source", allowed_paths=("selection.json",),
                evidence_refs=(bad, *refs, *contributions)), self.mesh, self.policy)

    def test_selector_frontier_must_match_registered_eligibility_policy(self):
        _, refs, contributions, frontier = self.candidates()
        directive = self.directive(kind="select_source", allowed_paths=("selection.json",),
            evidence_refs=(frontier, *refs, *contributions))
        with self.assertRaisesRegex(RoleError, "eligibility policy"):
            materialize_project(directive, self.mesh, "f" * 64)

    def test_verifier_rejects_self_reported_view_or_changed_actual_request(self):
        directive = self.directive()
        request, view = materialize_project(directive, self.mesh, self.policy)
        binding = self.dispatch(directive, request, view)
        with self.assertRaises(RoleError):
            verify_materialized_view(binding, replace(request, instructions="other"), view,
                                     self.mesh, policy_sha256=self.policy)
        with self.assertRaises(RoleError):
            verify_materialized_view(binding, request, replace(view, policy_sha256="f" * 64),
                                     self.mesh, policy_sha256=self.policy)
        with self.assertRaises(RoleError):
            verify_materialized_view(binding, request, view, self.mesh, policy_sha256="f" * 64)

    def test_verifier_rejects_invented_recipe_even_when_new_request_hash_matches(self):
        directive = self.directive()
        request, view = materialize_project(directive, self.mesh, self.policy)
        recipe = json.loads(request.feedback)
        recipe["materials"][0]["utf8"] = "invented source"
        changed = replace(request, feedback=canonical_bytes(recipe).decode())
        forged_view = replace(view, materialized_content_sha256=worker_request_digest(changed))
        binding = self.dispatch(directive, changed, forged_view)
        with self.assertRaisesRegex(RoleError, "exact materialized"):
            verify_materialized_view(binding, changed, forged_view, self.mesh, policy_sha256=self.policy)

    def test_verifier_binds_registered_template_and_attempt_allowance(self):
        directive = self.directive(attempt_limit=2)
        request, view = materialize_project(directive, self.mesh, self.policy)
        binding = self.dispatch(directive, request, view)
        expected = digest(directive.instructions.encode())
        self.assertEqual(verify_materialized_view(binding, request, view, self.mesh,
            policy_sha256=self.policy, expected_instructions_sha256=expected, expected_attempt_limit=2), view)
        for instruction_hash, allowance in (("f" * 64, 2), (expected, 1), (expected, None), (None, 2)):
            with self.subTest(allowance=allowance), self.assertRaises(RoleError):
                verify_materialized_view(binding, request, view, self.mesh, policy_sha256=self.policy,
                    expected_instructions_sha256=instruction_hash, expected_attempt_limit=allowance)

    def test_verifier_requires_its_own_arrived_material(self):
        directive = self.directive()
        request, view = materialize_project(directive, self.mesh, self.policy)
        binding = self.dispatch(directive, request, view)
        with self.assertRaisesRegex(RoleError, "not arrived"):
            verify_materialized_view(binding, request, view, FakeMesh("receiver"), policy_sha256=self.policy)

    def test_role_uses_v3_hook_and_journal_binds_implementation(self):
        loop = ProjectRoleLoopV3(self.root, "B01", self.mesh, self.finance, call_limit=1,
                                policy_sha256=self.policy, result_producer="finance", clock=lambda: 100.0)
        loop.enqueue(self.directive())
        snapshot = loop.tick()
        self.assertEqual(snapshot.state, "materialized")
        self.assertEqual(snapshot.view.policy_sha256, self.policy)
        self.assertNotEqual(loop.policy_sha256, self.policy)
        loop.close()
        with patch("gossip_harness.peer_project_views_v3.effective_runtime_policy_sha256", return_value="f" * 64):
            with self.assertRaisesRegex(RoleError, "configuration"):
                ProjectRoleLoopV3(self.root, "B01", self.mesh, self.finance, call_limit=1,
                                 policy_sha256=self.policy, result_producer="finance", clock=lambda: 100.0)

    def test_registered_generation_policies_share_journal_and_are_immutable(self):
        policies = {("build", 0): self.policy, ("build", 1): "d" * 64}
        loop = ProjectRoleLoopV3(self.root, "B01", self.mesh, self.finance, call_limit=1,
            policy_sha256="f" * 64, view_policies=policies, result_producer="finance", clock=lambda: 100.0)
        self.addCleanup(loop.close)
        policies[("build", 1)] = "e" * 64
        directive = self.directive(work=WorkKey("catalog", "feature", "slot-1", 1))
        loop.enqueue(directive)
        self.assertEqual(loop.tick().view.policy_sha256, "d" * 64)
        with self.assertRaises(TypeError):
            loop.view_policies[("build", 1)] = "e" * 64
        with self.assertRaises(RoleError):
            loop._materialize(self.directive(work=WorkKey("catalog", "feature", "slot-1", 2)))

    def test_policy_registration_changes_cannot_reopen_same_journal(self):
        args = dict(call_limit=2, policy_sha256=self.policy, result_producer="finance", clock=lambda: 100.0)
        loop = ProjectRoleLoopV3(self.root, "B01", self.mesh, self.finance,
            view_policies={("build", 0): self.policy}, **args)
        loop.close()
        with self.assertRaisesRegex(RoleError, "configuration"):
            ProjectRoleLoopV3(self.root, "B01", self.mesh, self.finance,
                view_policies={("build", 0): self.policy, ("build", 1): self.policy}, **args)

    def test_policy_generation_does_not_reset_aggregate_call_limit(self):
        policies = {("build", 0): self.policy, ("build", 1): "d" * 64}
        args = dict(call_limit=1, policy_sha256="f" * 64, view_policies=policies,
                    result_producer="finance", clock=lambda: 100.0)
        loop = ProjectRoleLoopV3(self.root, "B01", self.mesh, self.finance, **args)
        loop.enqueue(self.directive())
        first = None
        for _ in range(20):
            first = loop.tick()
            if first and first.state == "pending":
                self.finance.finish(first.action.request_id)
            if first and first.state == "published":
                break
        self.assertEqual(first.state, "published")
        loop.close()
        loop = ProjectRoleLoopV3(self.root, "B01", self.mesh, self.finance, **args)
        self.addCleanup(loop.close)
        loop.enqueue(self.directive(work=WorkKey("catalog", "feature", "slot-1", 1)))
        snapshot = loop.tick()
        self.assertEqual((snapshot.state, snapshot.reason), ("stopped", "role_call_limit"))
        self.assertEqual(self.finance.invocations, 1)

    def test_default_v2_hook_semantics_remain_original(self):
        # This test concerns only the tiny protected-hook edit, not a claim that
        # an old whole-runtime receipt automatically covers the changed source.
        loop = RoleLoop(self.root, "B01", self.mesh, self.finance, call_limit=1,
                        policy_sha256=self.policy, result_producer="finance", clock=lambda: 100.0)
        self.addCleanup(loop.close)
        key = loop.enqueue(self.directive())
        loop.tick()
        self.assertEqual(loop.worker_request(key).feedback, "Existing exact feedback")

    def test_oversized_recipe_fails_without_truncation(self):
        evidence = self.mesh.arrive("requirements", "requirements", b"x" * 510_000)
        with self.assertRaises(ValueError):
            materialize_project(self.directive(evidence_refs=(evidence,)), self.mesh, self.policy)

    def test_impossible_total_evidence_stops_resolving_later_objects(self):
        first = self.mesh.arrive("requirements", "requirements", b"x" * 300_000)
        second = self.mesh.arrive("requirements", "requirements", b"y" * 300_000)
        later = self.mesh.arrive("requirements", "requirements", b"z" * 300_000)
        original, calls = self.mesh.resolve, []

        def tracked(ref):
            calls.append(ref)
            return original(ref)

        self.mesh.resolve = tracked
        with self.assertRaisesRegex(RoleError, "record byte limit"):
            materialize_project(self.directive(evidence_refs=(first, second, later)), self.mesh, self.policy)
        self.assertEqual(calls, [self.source, first, second])

    def test_runtime_digest_includes_entire_registered_policy_map(self):
        a = effective_runtime_policy_sha256(self.policy, view_policies={("review", 0): "d" * 64})
        b = effective_runtime_policy_sha256(self.policy,
            view_policies={("review", 0): "d" * 64, ("review", 1): "e" * 64})
        self.assertNotEqual(a, b)


if __name__ == "__main__":
    unittest.main()
