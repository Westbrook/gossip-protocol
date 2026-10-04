"""Offline product-purpose admission controls; no Docker or candidate execution."""
from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import unittest
from unittest.mock import patch

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness import candidate_cli_acceptance_profile_v1 as profile
from gossip_harness import candidate_client_execution_v5 as execution
from gossip_harness import candidate_client_execution_v4 as old_execution
from gossip_harness import candidate_client_observation_source_v1 as bridge
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import project_acceptance_registry_v1 as registry
from gossip_harness.gitstore import GitStore

COHORT = tuple("trajectory-" + str(index) for index in range(6))


class CandidateClientExecutionV5Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.artifacts = ArtifactDirectory("candidate-client-execution-v5-offline", retain_success=True)
        cls.addClassCleanup(cls.artifacts.close)
        cls.store = GitStore.create(cls.artifacts.root / "store.git", {
            "library/__init__.py": "raise RuntimeError('no host execution')\n",
            "library/__main__.py": "raise RuntimeError('no host execution')\n",
        })
        cls.commit = cls.store.head()
        cls.tree, cls.files = execution.capture_git_source(cls.store, cls.commit)
        cls.policy = execution.ClientPolicy("sha256:" + "a" * 64)
        cls.runtime = {"kind": "fixture-no-Docker"}
        cls.requirements = profile.NORMATIVE_SHA256["library-cumulative-product-v2.json"]

    def setUp(self):
        self.root = self.artifacts.root.resolve() / self.id().rsplit(".", 1)[-1]
        self.owners = []
        self.addCleanup(lambda: [owner.close() for owner in self.owners])
        self.make_registration()

    def make_registration(self, purpose="public_release", case_id="cli-empty"):
        self.binding = execution.binding_for(self.files, case_id, self.policy, self.runtime,
            requirements_sha256=self.requirements, purpose=purpose)
        self.subject = registry.Subject("cohort-1", COHORT[0], "M1", "e" * 64,
                                        self.requirements, self.binding.source_sha256)
        self.gate = execution.gate_for(self.subject, self.binding, gate_id="cli-profile")
        self.registration = execution.ClientRegistration(self.binding, self.commit, self.tree,
                                                          "fresh-1", self.gate, COHORT)
        registered = execution.observation_registration(self.registration)
        self.current = {"registration": registered, "freeze": None}
        self.admission = admission.ObservationAdmission(registered,
            verify_registration=lambda: self.current["registration"],
            verify_cohort=lambda: self.current["freeze"])

    def owner(self, *, root=None, checkpoint=None, registration=None, authority=None):
        owner = execution.CandidateClientExecution(root or self.root, self.store,
            registration or self.registration, self.policy, mode="fixture",
            expected_checkpoint=checkpoint, admission_authority=authority or self.admission)
        self.owners.append(owner)
        return owner

    def test_complete_source_and_original_profile_purpose_are_bound(self):
        owner = self.owner()
        self.assertEqual(owner.files, self.files)
        self.assertEqual(owner.binding.source_sha256, admission.source_sha256(self.files))
        self.assertNotEqual(owner.binding.source_sha256, old_execution.source_sha256(self.files))
        self.assertEqual(owner.observation_registration.original_definition_purpose, "harness_qualification")
        self.assertEqual(owner.observation_registration.gate, self.gate)
        self.assertEqual(owner.observation_registration.original_binding_sha256,
                         execution.digest(execution.asdict(self.binding)))

    def test_qualification_and_wrong_milestone_or_requirement_are_rejected(self):
        for changes in ({"purpose": "harness_qualification"}, {"milestone": "M4"},
                        {"requirements_sha256": "f" * 64}, {"protocol": old_execution.PROTOCOL}):
            with self.subTest(changes=changes), self.assertRaises(execution.ExecutionError):
                replace(self.binding, **changes)

    def test_gate_order_subset_subject_and_purpose_cannot_be_substituted(self):
        altered = (replace(self.gate, ordered_case_ids=self.gate.ordered_case_ids[:-1]),
                   replace(self.gate, ordered_case_ids=tuple(reversed(self.gate.ordered_case_ids))),
                   replace(self.gate, binding=replace(self.gate.binding, purpose="repeatability")),
                   replace(self.gate, binding=replace(self.gate.binding,
                       subject=replace(self.subject, source_sha256="f" * 64))))
        for gate in altered:
            with self.subTest(gate=gate), self.assertRaises(execution.ExecutionError):
                replace(self.registration, gate=gate)

    def test_changed_original_binding_repetition_and_git_require_new_admission(self):
        candidates = (replace(self.registration, repetition_id="fresh-2"),
                      replace(self.registration, tree_oid="f" * 40),
                      replace(self.registration, commit_oid="f" * 40))
        for index, registration in enumerate(candidates):
            with self.subTest(index=index), self.assertRaises((ValueError, RuntimeError)):
                self.owner(root=self.root.with_name(self.root.name + str(index)), registration=registration)

    def test_revoked_registration_prevents_intent(self):
        owner = self.owner()
        self.current["registration"] = None
        with self.assertRaises(admission.AdmissionUnavailable):
            owner.execute_once()
        self.assertFalse((self.root / "intent.json").exists())

    def test_independent_purpose_without_full_cohort_freeze_never_claims_intent(self):
        self.make_registration("independent_acceptance")
        owner = self.owner()
        with self.assertRaises(admission.AdmissionUnavailable):
            owner.execute_once()
        self.assertFalse((self.root / "intent.json").exists())
        self.current["freeze"] = registry.CohortFreeze((self.subject,), "a" * 64, "b" * 64, True)
        with self.assertRaises(admission.AdmissionError):
            owner.execute_once()
        self.assertFalse((self.root / "intent.json").exists())

    def test_full_freeze_is_retained_before_fixture_dispatch_is_refused(self):
        self.make_registration("repeatability")
        subjects = tuple(replace(self.subject, trajectory_id=item,
            source_sha256=self.subject.source_sha256 if index == 0 else str(index) * 64)
            for index, item in enumerate(COHORT))
        freeze = registry.CohortFreeze(subjects, "a" * 64, "b" * 64, True)
        self.current["freeze"] = freeze
        owner = self.owner()
        with self.assertRaises(execution.ExecutionError):
            owner.execute_once()
        self.assertEqual(owner.retained_freeze(), freeze)
        with self.assertRaises(execution.ExecutionUnknown):
            owner.execute_once()

    def test_interrupted_intent_is_never_redispatched_after_reopen(self):
        owner = self.owner()
        with self.assertRaises(execution.ExecutionError):
            owner.execute_once()
        checkpoint = owner.checkpoint()
        owner.close()
        reopened = self.owner(checkpoint=checkpoint)
        with patch.object(reopened, "_dispatch", side_effect=AssertionError("redispatched")):
            with self.assertRaises(execution.ExecutionUnknown):
                reopened.execute_once()

    def test_external_checkpoint_rejects_unretained_suffix_and_rollback(self):
        owner = self.owner()
        checkpoint = owner.checkpoint()
        owner._retain("retained-control.json", execution.encoded({"control": True}))
        newest = owner.checkpoint()
        owner.close()
        with self.assertRaises(execution.ExecutionError):
            self.owner(checkpoint=checkpoint)
        reopened = self.owner(checkpoint=newest)
        (self.root / "retained-control.json").unlink()
        with self.assertRaises(execution.ExecutionError):
            reopened.checkpoint()

    def test_fixture_cannot_publish_verifier_or_become_observation_source(self):
        owner = self.owner()
        with self.assertRaises(bridge.ObservationError):
            bridge.publish_verifier(owner)
        with self.assertRaises(bridge.ObservationError):
            bridge.ClientObservationSource(owner, owner.checkpoint())
        with self.assertRaises(bridge.ObservationError):
            bridge.ClientObservationSource({"passed": True}, owner.checkpoint())

    def test_checkpoint_does_not_authorize_old_journal_owner(self):
        old_binding = old_execution.binding_for(self.files, "cli-empty",
            old_execution.ClientPolicy(self.policy.image_id), self.runtime,
            requirements_sha256=self.requirements)
        old_registration = old_execution.ClientRegistration(old_binding, self.commit, self.tree, "qualification-1")
        with old_execution.CandidateClientExecution(self.root, self.store, old_registration,
                old_execution.ClientPolicy(self.policy.image_id), mode="fixture") as owner:
            with self.assertRaises(bridge.ObservationError):
                bridge.ClientObservationSource(owner, owner.checkpoint())
