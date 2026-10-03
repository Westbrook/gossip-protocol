"""Actual local evidence, Git quarantine, and selector provenance; no source execution."""
from __future__ import annotations

from dataclasses import asdict, replace
import hashlib
from pathlib import Path
import unittest
from unittest.mock import patch

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness.gitstore import GitStore
from gossip_harness.ledger import Lease
from gossip_harness.peer_candidate_v2 import CandidatePublisher, named_sources, result_payload_bytes
from gossip_harness.peer_project_contract_v2 import (
    PACKAGES, ActionRequest, Context, DispatchBinding, DispatchReply, EvidenceRef,
    from_dict, identity, to_dict, worker_request_digest,
)
from gossip_harness.peer_project_contract_v2 import WorkKey
from gossip_harness.peer_project_evidence_v3 import (
    SELECTION_PROTOCOL, ContributionSlot, EvidenceError, EvidenceRegistry, EvidenceUnavailable,
)
from gossip_harness.peer_project_views_v3 import materialize_project, verify_materialized_view
from gossip_harness.peer_role_loop_v2 import WorkDirective, directive_id
from gossip_harness.peer_store_v1 import canonical_bytes, strict_loads
from gossip_harness.worker import WorkerResult

CONTEXT = Context("c" * 64, "cohort", "trajectory", 1, "e" * 64)
INSTRUCTIONS = "Implement the registered project task."
AUTHORITY, PROFILE, POLICY, ELIGIBILITY = "a" * 64, "f" * 64, "d" * 64, "b" * 64
SCOPES = {package: ("library/" + package,) for package in PACKAGES}
FILES = {"README.md": "inherited source\n", **{
    path: value for package in PACKAGES for path, value in (
        (f"library/{package}/main.py", f"value = '{package}'\n"),
        (f"library/{package}/stable.py", "untouched = '✓'\n"))}}


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


class MemoryMesh:
    """Explicit local fixture inbox; omitting a notice removes provenance."""
    def __init__(self, actor="receiver", payloads=None):
        self.node_id, self.payloads = actor, dict(payloads or {})
        self.commands, self.wanted, self.hidden = {}, [], set()

    def arrived(self):
        return tuple(ref for ref in self.payloads if ref not in self.hidden)

    def resolve(self, ref):
        return self.payloads.get(ref) if ref not in self.hidden else None

    def want(self, ref):
        self.wanted.append(ref)
        return True

    def publish(self, kind, payload, command_id):
        command = (self.node_id, command_id)
        if command in self.commands:
            previous, ref = self.commands[command]
            if previous != (kind, payload):
                raise ValueError("Command mutation")
            return ref
        ref = EvidenceRef(sha((self.node_id + ":" + command_id).encode()), self.node_id, kind, sha(payload))
        self.commands[command], self.payloads[ref] = ((kind, payload), ref), payload
        return ref

    def send(self, actor, kind, body, command):
        prior = self.node_id
        self.node_id = actor
        try:
            return self.publish(kind, body, command)
        finally:
            self.node_id = prior


def terminal(mesh, slot, source, *, kind, evidence=(), changes=None, suffix=""):
    directive = WorkDirective(CONTEXT, slot.work, slot.profile_id, kind, source, tuple(evidence),
                              slot.allowed_paths, INSTRUCTIONS, feedback=suffix)
    request, view = materialize_project(directive, mesh, ELIGIBILITY if kind == "select_source" else POLICY)
    body = asdict(request)
    body["allowed_paths"] = list(request.allowed_paths)
    request_ref = mesh.send(slot.actor, "worker-request", canonical_bytes({"worker_request": body,
        "view_manifest_sha256": identity(view)}), "request-" + directive_id(directive))
    action = ActionRequest(CONTEXT, "action-" + directive_id(directive), "dispatch-" + directive_id(directive),
                           slot.actor, kind, slot.work, slot.profile_id, request_ref, identity(view))
    binding = DispatchBinding(action, Lease(request.task_id, slot.actor, 1, 100.0),
        worker_request_digest(request), slot.profile_sha256, AUTHORITY,
        "call-" + directive_id(directive), "reservation-" + directive_id(directive), 10)
    result = WorkerResult(changes, "Known fixture outcome", 3, {})
    result_ref = mesh.send("finance", "financial-result", result_payload_bytes(result), "result-" + identity(binding))
    reply = DispatchReply(action.request_id, identity(action), "completed", "", binding, result_ref.payload_sha256, 3)
    role_ref = mesh.send(slot.actor, "role-result", canonical_bytes({"protocol": "peer-role-loop-v2", "actor": slot.actor,
        "action": to_dict(action), "reply": to_dict(reply), "view_manifest": to_dict(view),
        "result_ref": to_dict(result_ref)}), "role-" + identity(binding))
    proof = {"binding": binding, "action": action, "reply": reply, "worker_request": request,
             "result": result, "result_payload": {"kind": "result", "payload": asdict(result)}}
    return binding, role_ref, proof


class ProjectEvidenceV3Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.artifacts = ArtifactDirectory("project-evidence-v3", retain_success=True)
        cls.addClassCleanup(cls.artifacts.close)
        cls.fixture_root = cls.artifacts.root.resolve()
        cls.seed = GitStore.create(cls.fixture_root / "seed", FILES)
        cls.base = cls.seed.head()
        cls.mesh = MemoryMesh()
        cls.source = cls.mesh.send("seed", "project-source", canonical_bytes({"files": FILES, "base_sha": cls.base}), "source")
        cls.slots = tuple(ContributionSlot("B" + str(index), WorkKey(package, package + "-api", "alternative-1", 0),
            "fixture", PROFILE, (f"library/{package}/main.py",), cls.base, sha(INSTRUCTIONS.encode()))
            for index, package in enumerate(PACKAGES, 1))
        cls.selector = ContributionSlot("R1", WorkKey("catalog", "choose", "selection", 0),
                                       "fixture", PROFILE, ("selection.json",), cls.base, sha(INSTRUCTIONS.encode()))
        cls.proofs, cls.publications, cls.role_refs = {}, [], []
        for slot in cls.slots:
            binding, role_ref, proof = terminal(cls.mesh, slot, cls.source, kind="build",
                changes={slot.allowed_paths[0]: "value = 'changed'\n"})
            cls.proofs[identity(binding)] = proof
            store = GitStore.fork(cls.seed, cls.fixture_root / slot.actor)
            cls.mesh.node_id = slot.actor
            publisher = CandidatePublisher(cls.fixture_root / (slot.actor + "-publisher"), cls.mesh, store,
                actor=slot.actor, baseline_sha=cls.base, package_scopes=SCOPES,
                execution_contract_sha256=CONTEXT.execution_contract_sha256,
                completed_proof=lambda dispatch, request, result, digest: cls.proofs[identity(dispatch)]["reply"])
            cls.publications.append(publisher.publish(binding, proof["worker_request"], proof["result"],
                                                       proof["reply"].result_payload_sha256))
            cls.role_refs.append(role_ref)
        cls.mesh.node_id = "receiver"

    def setUp(self):
        self.mesh = MemoryMesh(payloads=self.__class__.mesh.payloads)
        self.proofs = dict(self.__class__.proofs)
        self.root = self.fixture_root / self._testMethodName
        self.registry = self.open_registry()
        self.addCleanup(self.registry.close)

    def open_registry(self, **changes):
        args = dict(context=CONTEXT, authority_config_sha256=AUTHORITY, package_scopes=SCOPES,
            slots=self.slots, selector=self.selector, bases={self.base: FILES},
            eligibility_policy_sha256=ELIGIBILITY, view_policies={("build", 0): POLICY, ("select_source", 0): ELIGIBILITY},
            verified_terminal=lambda binding: self.proofs[identity(binding)],
            verified_view=lambda binding, request, view: verify_materialized_view(binding, request, view, self.mesh,
                                                                                  policy_sha256=ELIGIBILITY if binding.action.kind == "select_source" else POLICY))
        args.update(changes)
        return EvidenceRegistry(self.root, self.mesh, **args)

    def register(self, count=4):
        return tuple(self.registry.register_offer(publication.offer_ref, role)
                     for publication, role in zip(self.publications[:count], self.role_refs[:count], strict=True))

    def select(self, change=None, suffix=""):
        frontier_ref = self.registry.frontier()
        frontier = strict_loads(self.mesh.resolve(frontier_ref))
        refs = [frontier_ref]
        refs += [from_dict(EvidenceRef, entry[key]) for entry in frontier["offers"]
                 for key in ("offer_ref", "contribution_ref")]
        artifact = {"protocol": SELECTION_PROTOCOL, "eligibility_policy_sha256": ELIGIBILITY,
            "frontier_sha256": frontier_ref.payload_sha256,
            "eligible_offer_sha256s": [entry["offer_sha256"] for entry in frontier["offers"]],
            "selected": [{"package_id": package, "offer_sha256": next(entry["offer_sha256"]
                         for entry in frontier["offers"] if entry["slot"]["package_id"] == package)} for package in PACKAGES]}
        if change:
            change(artifact)
        binding, role_ref, proof = terminal(self.mesh, self.selector, self.source, kind="select_source", evidence=refs,
            changes={"selection.json": canonical_bytes(artifact).decode()}, suffix=suffix)
        self.proofs[identity(binding)] = proof
        return binding, role_ref

    def test_real_import_returns_all_owned_sources_including_unchanged(self):
        offer = self.register(1)[0]
        owned = self.registry.candidate_files(offer)
        self.assertEqual(set(owned), {"library/catalog/main.py", "library/catalog/stable.py"})
        self.assertEqual(self.registry.verified_contribution(offer), named_sources(owned))
        self.assertEqual(owned["library/catalog/stable.py"], "untouched = '✓'\n")
        self.assertFalse((self.root / ("quarantine-" + identity(offer)) / "quarantine.git" / "refs/heads/accepted").exists())
        self.assertEqual(self.registry.register_offer(self.publications[0].offer_ref, self.role_refs[0]), offer)

    def test_absent_exact_offer_notice_cannot_use_same_payload_digest(self):
        ref = self.publications[0].offer_ref
        fabricated = replace(ref, event_id="0" * 64)
        with self.assertRaises(EvidenceUnavailable):
            self.registry.register_offer(fabricated, self.role_refs[0])
        self.assertIn(fabricated, self.mesh.wanted)

    def test_missing_role_request_finance_result_and_source_each_block(self):
        proof = self.proofs[identity(self.publications[0].offer.dispatch)]
        role_body = strict_loads(self.mesh.resolve(self.role_refs[0]))
        refs = [self.role_refs[0], proof["action"].worker_payload_ref,
                from_dict(EvidenceRef, role_body["result_ref"]), self.source]
        for ref in refs:
            with self.subTest(kind=ref.kind):
                self.mesh.hidden.add(ref)
                with self.assertRaises((EvidenceUnavailable, ValueError)):
                    self.registry.register_offer(self.publications[0].offer_ref, self.role_refs[0])
                self.mesh.hidden.remove(ref)
        self.assertFalse(any(self.root.glob("quarantine-*")))

    def test_completed_reply_and_usage_must_match_internal_proof(self):
        offer = self.publications[0].offer
        original = self.proofs[identity(offer.dispatch)]
        for reply in (replace(original["reply"], state="failed"), replace(original["reply"], usage_units=4)):
            with self.subTest(state=reply.state, usage=reply.usage_units):
                self.proofs[identity(offer.dispatch)] = {**original, "reply": reply}
                with self.assertRaises(EvidenceError):
                    self.registry.register_offer(self.publications[0].offer_ref, self.role_refs[0])
        self.proofs[identity(offer.dispatch)] = original

    def test_wrong_role_result_and_finance_producer_rejected(self):
        original = self.role_refs[0]
        body = strict_loads(self.mesh.resolve(original))
        bad_ref = self.mesh.send("attacker", "role-result", self.mesh.resolve(original), "forged")
        with self.assertRaises(EvidenceError):
            self.registry.register_offer(self.publications[0].offer_ref, bad_ref)
        # A second registry preserves the failed registration rather than overwriting it.
        other = EvidenceRegistry(self.root / "other", self.mesh, context=CONTEXT,
            authority_config_sha256=AUTHORITY, package_scopes=SCOPES, slots=self.slots, selector=self.selector,
            bases={self.base: FILES}, eligibility_policy_sha256=ELIGIBILITY, view_policies={("build", 0): POLICY, ("select_source", 0): ELIGIBILITY},
            verified_terminal=lambda binding: self.proofs[identity(binding)], verified_view=self.registry.view_verifier)
        self.addCleanup(other.close)
        body["result_ref"]["producer"] = "attacker"
        bad_result = self.mesh.send(original.producer, "role-result", canonical_bytes(body), "forged-finance")
        with self.assertRaises(EvidenceError):
            other.register_offer(self.publications[0].offer_ref, bad_result)

    def test_failed_quarantine_is_retained_and_not_retried(self):
        offer = self.publications[0].offer
        quarantine = self.root / ("quarantine-" + identity(offer))
        quarantine.mkdir()
        (quarantine / "failure.json").write_text('{"status":"rejected"}')
        with patch("gossip_harness.peer_project_evidence_v3.import_bundle") as importer:
            with self.assertRaisesRegex(EvidenceError, "refusing retry"):
                self.registry.register_offer(self.publications[0].offer_ref, self.role_refs[0])
            importer.assert_not_called()
        self.assertTrue((quarantine / "failure.json").exists())

    def test_import_failure_before_quarantine_creation_is_not_retried(self):
        with patch("gossip_harness.peer_project_evidence_v3.import_bundle", side_effect=ValueError("preimport rejection")) as importer:
            with self.assertRaisesRegex(ValueError, "preimport"):
                self.registry.register_offer(self.publications[0].offer_ref, self.role_refs[0])
            with self.assertRaisesRegex(EvidenceError, "refusing retry"):
                self.registry.register_offer(self.publications[0].offer_ref, self.role_refs[0])
            self.assertEqual(importer.call_count, 1)

    def test_registry_lock_allows_nested_trusted_callback(self):
        with self.registry._locked():
            with self.registry._locked():
                self.assertEqual(self.registry._records(), {})

    def test_frontier_requires_every_registered_slot(self):
        self.register(1)
        with self.assertRaisesRegex(EvidenceError, "missing registered slots"):
            self.registry.frontier()

    def test_full_selector_result_derives_and_authenticates_manifest(self):
        offers = self.register()
        binding, role_ref = self.select()
        selection = self.registry.materialize_selection(binding, role_ref)
        self.assertEqual(self.registry.verified_selection(selection), selection)
        self.assertEqual(set(selection.eligible_offer_sha256s), {identity(offer) for offer in offers})
        self.assertEqual(tuple(item.package_id for item in selection.selected), PACKAGES)
        self.assertEqual(self.registry.selection_ref(selection).kind, "selection-manifest")
        self.assertEqual(selection.missing_slots, ())
        binding_key = identity(binding)
        proof = self.proofs[binding_key]
        self.proofs[binding_key] = {**proof, "reply": replace(proof["reply"], state="failed")}
        with self.assertRaises(EvidenceError):
            self.registry.verified_selection(selection)

    def test_selector_rejects_frontier_subsets_cross_scope_and_unknown_fields(self):
        self.register()
        changes = [lambda value: value["eligible_offer_sha256s"].pop(),
                   lambda value: value.update(frontier_sha256="0" * 64),
                   lambda value: value["selected"][0].update(offer_sha256=value["selected"][1]["offer_sha256"]),
                   lambda value: value.update(model_approval=True)]
        for index, change in enumerate(changes):
            with self.subTest(index=index):
                binding, role_ref = self.select(change, str(index))
                with self.assertRaises(EvidenceError):
                    self.registry.materialize_selection(binding, role_ref)

    def test_registry_detects_record_deletion_and_configuration_rebinding(self):
        self.register(1)
        self.registry.db.execute("DELETE FROM records WHERE ordinal=(SELECT MAX(ordinal) FROM records)")
        self.registry.db.commit()
        with self.assertRaisesRegex(EvidenceError, "membership"):
            self.registry.verified_contribution(self.publications[0].offer)
        with self.assertRaises(EvidenceError):
            self.open_registry(view_policies={("build", 0): "0" * 64})

    def test_registered_package_scope_and_exact_base_are_required(self):
        invalid = replace(self.slots[0], allowed_paths=("README.md",))
        with self.assertRaises(EvidenceError):
            self.open_registry(slots=(invalid, *self.slots[1:]))
        with self.assertRaises(EvidenceError):
            self.open_registry(bases={self.base: {**FILES, "README.md": "substituted"}})

    def test_review_candidate_labels_require_exact_full_owned_source_publication(self):
        self.register()
        binding, role_ref = self.select()
        self.registry.materialize_selection(binding, role_ref)
        body = strict_loads(self.mesh.resolve(role_ref))
        from gossip_harness.peer_project_contract_v2 import LocalViewManifest
        view = from_dict(LocalViewManifest, body["view_manifest"])
        self.registry.verified_candidate_materials(view)
        original = next(ref for ref in view.evidence_refs if ref.kind == "candidate-contribution")
        payload = strict_loads(self.mesh.resolve(original))
        payload["files"] = {"README.md": FILES["README.md"]}
        forged = self.mesh.send("attacker", "candidate-contribution", canonical_bytes(payload), "truncated-label")
        changed = replace(view, evidence_refs=tuple(forged if ref == original else ref for ref in view.evidence_refs),
                          required_evidence_ids=tuple(forged.event_id if value == original.event_id else value
                                                      for value in view.required_evidence_ids))
        with self.assertRaisesRegex(EvidenceError, "unauthenticated"):
            self.registry.verified_candidate_materials(changed)

    def test_instruction_template_and_attempt_policy_are_prospectively_bound(self):
        original = self.proofs[identity(self.publications[0].offer.dispatch)]
        request = original["worker_request"]
        # Rebinding the registered template itself is rejected before any import.
        invalid = replace(self.slots[0], instructions_sha256=sha(b"Different instructions"))
        other = EvidenceRegistry(self.root / "template", self.mesh, context=CONTEXT,
            authority_config_sha256=AUTHORITY, package_scopes=SCOPES, slots=(invalid, *self.slots[1:]),
            selector=self.selector, bases={self.base: FILES}, eligibility_policy_sha256=ELIGIBILITY,
            view_policies={("build", 0): POLICY, ("select_source", 0): ELIGIBILITY},
            verified_terminal=lambda binding: self.proofs[identity(binding)], verified_view=self.registry.view_verifier)
        self.addCleanup(other.close)
        with self.assertRaisesRegex(EvidenceError, "template"):
            other.register_offer(self.publications[0].offer_ref, self.role_refs[0])
        self.assertEqual(request.instructions, INSTRUCTIONS)

    def test_raw_file_hashes_are_distinct_from_full_source_digest(self):
        offer = self.register(1)[0]
        sources = self.registry.verified_contribution(offer)
        stable = next(source for source in sources if source.path.endswith("stable.py"))
        self.assertEqual(stable.sha256, sha("untouched = '✓'\n".encode()))
        self.assertNotEqual(stable.sha256, offer.source_sha256)
