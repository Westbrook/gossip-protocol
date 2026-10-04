"""Offline product-purpose checks use real Git and inert controller journals."""
from dataclasses import asdict, replace
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness import candidate_http_execution_v4 as execution
from gossip_harness import candidate_http_cases_core_v1 as core
from gossip_harness import candidate_http_semantics_v1 as semantics
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import project_acceptance_registry_v1 as registry
from gossip_harness import candidate_checkpoint_head_v1 as head
from gossip_harness import candidate_checkpoint_chain_v1 as compact
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
        self.delta_root = self.root.with_name(self.root.name + "-deltas")
        self.cleanup_root = self.root.with_name(self.root.name + "-cleanup")
        self.checkpoint_authority = head.ExternalHead.create(self.root.with_name(self.root.name + "-head"),
            journal_roots=(self.root, self.delta_root))
        self.addCleanup(self.checkpoint_authority.close)
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
            profile=self.profile, observation_admission=self.admission, mode="fixture",
            checkpoint_authority=self.checkpoint_authority, delta_root=self.delta_root,
            cleanup_root=self.cleanup_root, **kwargs)
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
                self.root.mkdir(exist_ok=True)
                delta = self.root / (str(index) + "-deltas")
                authority = head.ExternalHead.create(self.root / (str(index) + "-head"), journal_roots=(root, delta))
                self.addCleanup(authority.close)
                with self.assertRaises((ValueError, RuntimeError)):
                    execution.CandidateHttpExecution(root, self.store, replace(self.registration, **change),
                        self.recipe, self.policy, profile=self.profile, observation_admission=self.admission, mode="fixture",
                        checkpoint_authority=authority, delta_root=delta, cleanup_root=self.root / (str(index) + "-cleanup"))
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
        with self.assertRaises(ValueError):
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
        bridge = HttpObservationSource(owner, owner.checkpoint(), receipt_path=self.root / "semantic-verifier.json")
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
        bridge = source.HttpObservationSource(owner, checkpoint, receipt_path=self.root / "semantic-verifier.json")
        supplied = replace(history(self.profile), original_binding_sha256=self.prospective.original_binding_sha256,
                           checkpoint_sha256=execution.digest(asdict(checkpoint)))
        with patch.object(reader, "observe_execution", return_value=supplied), patch.object(execution, "evaluator_sources", return_value={}):
            with self.assertRaises(AuthorityError):
                bridge.observation(self.prospective.gate, None)
        self.assertFalse(bridge.receipt_path.exists())

    def test_full_e01_catalog_history_binds_bounded_context_without_narrowing(self):
        from gossip_harness import candidate_http_cases_v1 as catalog
        case = next(item for item in catalog.definitions()
                    if item.row_id == "HTTP-ACTION-STATE/old-token-after-successor-failed")
        self.profile = execution.HttpProductProfile(case, "harness_qualification")
        self.recipe = execution.recipe_from_case(case)
        self.set_purpose("public_release")
        owner = self.controller()
        config_raw = owner.read_authenticated("config.json")
        self.assertGreater(len(config_raw), 300000)
        genesis = json.loads((self.delta_root / "genesis.json").read_bytes())
        context_raw = json.dumps(genesis["context"], sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
        self.assertLessEqual(len(context_raw), execution.compact_limits().max_context_bytes)
        self.assertEqual(sum(step.kind == "probe" for step in owner.recipe.steps), 70)
        self.assertEqual(owner.recipe.record(), execution.recipe_from_case(case).record())
        self.assertEqual(genesis["limits"], asdict(execution.compact_limits()))
        checkpoint = owner.checkpoint()
        owner.close()
        reopened = self.controller(expected_checkpoint=checkpoint)
        self.assertEqual(reopened.read_authenticated("config.json"), config_raw)

    def test_reopen_authenticates_complete_prefix_before_config_parser(self):
        owner = self.controller()
        checkpoint = owner.checkpoint()
        owner.close()
        (self.root / "config.json").write_bytes(b'{"tampered":true}')
        with patch.object(execution.CandidateHttpExecution, "json_authenticated",
                          side_effect=AssertionError("must reject before parsing")):
            with self.assertRaises(compact.ChainUnknown):
                self.controller(expected_checkpoint=checkpoint)

    def test_lost_anchor_preserves_prior_failure_but_blocks_dispatch_and_observation(self):
        from gossip_harness.candidate_http_observation_source_v1 import HttpObservationSource
        from gossip_harness.candidate_scope_consumer_v1 import AuthorityUnavailable
        owner = self.controller()
        failure = b'{"known_diagnostic":"failed"}'
        owner._retain("known-failure.json", failure)
        prior = owner.checkpoint()
        bridge = HttpObservationSource(owner, prior, receipt_path=self.root / "semantic-verifier.json")
        original = self.checkpoint_authority.compare_and_set
        def lost_reply(expected, proposed):
            original(expected, proposed)
            raise OSError("lost durable head acknowledgement")
        with patch.object(self.checkpoint_authority, "compare_and_set", side_effect=lost_reply):
            with self.assertRaises(compact.ChainUnknown): owner._retain("unknown.bin", b"unknown")
        fact = owner.read_prior("known-failure.json")
        self.assertEqual(fact.raw, failure)
        self.assertTrue(fact.prior_only)
        self.assertFalse(fact.acceptance_authority)
        with self.assertRaises(compact.ChainUnknown): owner.read_prior("unknown.bin")
        with patch.object(owner, "_dispatch", side_effect=AssertionError("redispatch forbidden")):
            with self.assertRaises(compact.ChainUnknown): owner.execute_once()
        with self.assertRaises(AuthorityUnavailable): bridge.observation(self.prospective.gate, None)
        self.assertFalse((self.root / "terminal.json").exists())

    def test_foreign_head_blocks_control_launch_and_matching_raw_descriptor(self):
        owner = self.controller()
        owner._retain("raw.bin", b"raw")
        descriptor = owner._descriptor("raw.bin")
        with patch.object(self.checkpoint_authority, "read", return_value=replace(owner.checkpoint(), head_sha256="f" * 64)), \
             patch.object(execution.subprocess, "Popen", side_effect=AssertionError("no effect")):
            with self.assertRaises(compact.ChainUnknown): owner._command("forbidden", ["docker", "ps"])
        with self.assertRaises(compact.ChainUnknown): owner._raw({"stdout": descriptor})

    def test_verifier_is_same_chain_and_requires_new_exact_external_prefix(self):
        owner = self.controller()
        owner._cleanup_mode = True
        owner._retain("terminal.json", b"{}")
        original = owner.checkpoint()
        verified = owner.retain_verifier("semantic-verifier.json", b'{"diagnostic":"fixture-only"}')
        self.assertEqual(verified.sequence, original.sequence + 1)
        self.assertEqual(verified.context_sha256, original.context_sha256)
        self.assertEqual(verified, self.checkpoint_authority.read())
        self.assertEqual(owner.read_authenticated("semantic-verifier.json"), b'{"diagnostic":"fixture-only"}')
        owner.close()
        with self.assertRaises(compact.ChainUnknown): self.controller(expected_checkpoint=original)
        reopened = self.controller(expected_checkpoint=verified)
        self.assertEqual(reopened.read_authenticated("semantic-verifier.json"), b'{"diagnostic":"fixture-only"}')

    def test_emergency_channel_failure_cannot_replace_original_exception(self):
        owner = self.controller()
        from types import SimpleNamespace
        calls = []
        def interrupted(**kwargs):
            calls.append(kwargs)
            raise KeyboardInterrupt("cleanup interrupted")
        owner.emergency_cleanup = SimpleNamespace(run=interrupted, close=lambda: None)
        original = compact.ChainUnknown("original unavailable")
        owner._emergency_after_failure(original)
        owner._emergency_after_failure(original)
        self.assertEqual(len(calls), 1)
        self.assertIn("KeyboardInterrupt", owner.emergency_result["cleanup_unavailable"])
