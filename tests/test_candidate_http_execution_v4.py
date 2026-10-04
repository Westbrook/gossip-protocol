"""Offline product-purpose checks use real Git and inert controller journals."""
from dataclasses import asdict, replace
from pathlib import Path
import unittest
from unittest.mock import patch

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness import candidate_http_execution_v4 as execution
from gossip_harness import candidate_http_cases_core_v1 as core
from gossip_harness import candidate_http_semantics_v1 as semantics
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import project_acceptance_registry_v1 as registry
from gossip_harness.gitstore import GitStore

IMAGE = "sha256:" + "a" * 64
REQUIREMENTS = next(iter(execution.finite.SUPPORTED_REQUIREMENTS_SHA256))
COHORT = tuple("trajectory-" + str(i) for i in range(6))


def profile():
    builder = core.Builder("HTTP-EMPTY-HEALTH/c06-purpose", ("V0-HTTP-01",))
    builder.start()
    builder.request("GET", "/health", semantics.success("health"), check=False)
    builder.stop()
    return execution.HttpProductProfile(builder.finish(), "harness_qualification")


class HttpProductAdmissionV4Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.artifacts = ArtifactDirectory("http-product-admission-v4", retain_success=True)
        cls.addClassCleanup(cls.artifacts.close)
        cls.store = GitStore.create(cls.artifacts.root / "store.git", {"library/__init__.py": "raise RuntimeError('never import candidate')\n"})
        cls.commit = cls.store.head()
        cls.tree, cls.files = execution.capture_git_source(cls.store, cls.commit)

    def setUp(self):
        self.root = self.artifacts.root.resolve() / self.id().split(".")[-1]
        self.profile = profile()
        self.recipe = execution.recipe_from_case(self.profile.case)
        self.policy = execution.HttpPolicy(IMAGE)
        self.set_purpose("public_release")
        self.controllers = []
        self.addCleanup(lambda: [owner.close() for owner in self.controllers])

    def set_purpose(self, purpose):
        self.binding = execution.binding_for(self.files, self.recipe, self.policy, {"kind": "fixture-no-Docker"},
            requirements_sha256=REQUIREMENTS, profile=self.profile, purpose=purpose)
        subject = registry.Subject("cohort", COHORT[0], "M1", "c" * 64, REQUIREMENTS, execution.source_sha256(self.files))
        self.prospective = execution.observation_registration_for(self.binding, self.profile, self.policy,
            subject=subject, gate_id="http-full-case", commit_oid=self.commit, tree_oid=self.tree,
            repetition_id="repetition-1", cohort_trajectory_ids=COHORT)
        self.registration = execution.HttpRegistration(self.binding, self.commit, self.tree, "repetition-1", self.prospective)
        self.current = self.prospective
        self.freeze = registry.CohortFreeze(tuple(replace(subject, trajectory_id=key,
            source_sha256=subject.source_sha256 if index == 0 else str(index) * 64)
            for index, key in enumerate(COHORT)), "d" * 64, "e" * 64, True)
        self.admission = admission.ObservationAdmission(self.prospective,
            verify_registration=lambda: self.current, verify_cohort=lambda: self.freeze)

    def controller(self, **kwargs):
        owner = execution.CandidateHttpExecution(self.root, self.store, self.registration, self.recipe, self.policy,
            profile=self.profile, observation_admission=self.admission, mode="fixture", **kwargs)
        self.controllers.append(owner)
        return owner

    def test_public_original_profile_and_canonical_source_bound_before_intent(self):
        with patch.object(execution.engine, "runtime_identity", side_effect=AssertionError("Engine forbidden")):
            owner = self.controller()
        self.assertEqual(owner.binding.source_sha256, admission.source_sha256(self.files))
        self.assertEqual(owner.config["observation_registration"], asdict(self.prospective))
        self.assertEqual(owner.config["product_profile"]["original_definition_purpose"], "harness_qualification")
        self.assertFalse(owner.config["product_profile"]["held_out_claim"])
        self.assertFalse((self.root / "intent.json").exists())

    def test_qualification_purpose_cannot_be_relabelled(self):
        with self.assertRaises(ValueError):
            replace(self.binding, purpose="harness_qualification")
        altered = replace(self.prospective, original_binding_sha256="f" * 64)
        self.registration = replace(self.registration, observation=altered)
        with self.assertRaises(ValueError):
            self.controller()
        self.assertFalse((self.root / "intent.json").exists())

    def test_wrong_source_recipe_profile_git_or_repetition_never_writes_intent(self):
        changes = ({"commit_oid": "f" * 40}, {"tree_oid": "f" * 40}, {"repetition_id": "other"},
                   {"binding": replace(self.binding, source_sha256="f" * 64)})
        for index, change in enumerate(changes):
            with self.subTest(change=change):
                root = self.root / str(index)
                with self.assertRaises((ValueError, RuntimeError)):
                    execution.CandidateHttpExecution(root, self.store, replace(self.registration, **change),
                        self.recipe, self.policy, profile=self.profile, observation_admission=self.admission, mode="fixture")
                self.assertFalse((root / "intent.json").exists())

    def test_registration_revocation_is_checked_before_dispatch(self):
        owner = self.controller()
        self.current = replace(self.prospective, repetition_id="revoked")
        with patch.object(owner, "_dispatch", side_effect=AssertionError("dispatch forbidden")):
            with self.assertRaises(admission.AdmissionError):
                owner.execute_once()
        self.assertFalse((self.root / "intent.json").exists())

    def test_independent_needs_exact_six_trajectory_freeze_and_detects_sibling_change(self):
        self.set_purpose("independent_acceptance")
        owner = self.controller()
        self.assertEqual(owner.retained_freeze, self.freeze)
        self.freeze = replace(self.freeze, subjects=(*self.freeze.subjects[:-1],
            replace(self.freeze.subjects[-1], source_sha256="f" * 64)))
        with self.assertRaises(admission.AdmissionError):
            owner.execute_once()
        self.assertFalse((self.root / "intent.json").exists())

    def test_missing_or_partial_freeze_has_no_original_intent(self):
        self.set_purpose("repeatability")
        self.freeze = replace(self.freeze, subjects=self.freeze.subjects[:1])
        with self.assertRaises(admission.AdmissionError):
            self.controller()
        self.assertFalse((self.root / "intent.json").exists())

    def test_fixture_intent_is_at_most_once_and_never_physical_evidence(self):
        owner = self.controller()
        with patch.object(owner, "_dispatch", side_effect=AssertionError("dispatch forbidden")):
            with self.assertRaises(execution.ExecutionError):
                owner.execute_once()
            with self.assertRaises(execution.ExecutionUnknown):
                owner.execute_once()
        with self.assertRaises(execution.ExecutionError):
            owner.verified_execution()

    def test_reopen_requires_exact_external_checkpoint_and_preserves_original_intent(self):
        owner = self.controller()
        with self.assertRaises(execution.ExecutionError):
            owner.execute_once()
        checkpoint = owner.checkpoint()
        owner.close()
        reopened = self.controller(expected_checkpoint=checkpoint)
        with self.assertRaises(execution.ExecutionUnknown):
            reopened.execute_once()
        reopened.close()
        (self.root / "foreign.json").write_text("{}")
        with self.assertRaises(execution.ExecutionError):
            self.controller(expected_checkpoint=checkpoint)

    def test_full_case_and_semantic_definition_changes_are_binding_changes(self):
        other = replace(self.profile, original_definition_purpose="public_release")
        changed = execution.binding_for(self.files, self.recipe, self.policy, {"kind": "fixture-no-Docker"},
            requirements_sha256=REQUIREMENTS, profile=other, purpose="public_release")
        self.assertNotEqual(changed.profile_sha256, self.binding.profile_sha256)
        self.assertEqual(len(self.prospective.gate.ordered_case_ids), len(self.recipe.steps))

    def test_concrete_bridge_translates_invalid_and_missing_authority(self):
        from gossip_harness.candidate_http_observation_source_v1 import HttpObservationSource
        from gossip_harness.candidate_scope_consumer_v1 import AuthorityError, AuthorityUnavailable
        owner = self.controller()
        bridge = HttpObservationSource(owner, owner.checkpoint(), receipt_path=self.root.parent / (self.root.name + ".receipt.json"))
        with self.assertRaises(AuthorityError):
            bridge.observation(replace(self.prospective.gate, gate_id="wrong"), None)
        self.current = None
        with self.assertRaises(AuthorityUnavailable):
            bridge.observation(self.prospective.gate, None)
        self.assertFalse(bridge.receipt_path.exists())

    def test_bridge_rejects_loaded_semantic_source_drift_before_retaining_receipt(self):
        from gossip_harness import candidate_http_observation_source_v1 as source
        from gossip_harness import candidate_http_observation_v2 as reader
        from gossip_harness.candidate_scope_consumer_v1 import AuthorityError
        from tests.test_candidate_http_observation_source_v1 import history
        owner = self.controller()
        checkpoint = owner.checkpoint()
        bridge = source.HttpObservationSource(owner, checkpoint, receipt_path=self.root.parent / (self.root.name + ".receipt.json"))
        supplied = replace(history(self.profile), original_binding_sha256=self.prospective.original_binding_sha256,
                           checkpoint_sha256=execution.digest(asdict(checkpoint)))
        with patch.object(reader, "observe_execution", return_value=supplied), patch.object(execution, "evaluator_sources", return_value={}):
            with self.assertRaises(AuthorityError):
                bridge.observation(self.prospective.gate, None)
        self.assertFalse(bridge.receipt_path.exists())
