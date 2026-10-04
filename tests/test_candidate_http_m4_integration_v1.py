"""M4 declaration/owner mechanics and pure supplied diagnostics; no physical run."""
from collections import Counter
from dataclasses import asdict, replace
import json
import unittest
from unittest.mock import patch

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness import candidate_checkpoint_head_v1 as head
from gossip_harness import candidate_http_cases_v1 as catalog
from gossip_harness import candidate_http_execution_v4 as execution
from gossip_harness import candidate_http_observation_source_v1 as source
from gossip_harness import candidate_http_observation_v2 as reader
from gossip_harness import candidate_http_semantics_v1 as semantics
from gossip_harness import candidate_http_transport_v1 as wire
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import cumulative_observation_profile_v1 as cumulative
from gossip_harness import project_acceptance_registry_v1 as registry
from gossip_harness.candidate_scope_consumer_v1 import AuthorityError
from gossip_harness.gitstore import GitStore

HEALTH = "HTTP-EMPTY-HEALTH/health"
RAW = "HTTP-PERSIST-LISTENER/root-page-reachable"
IMAGE = "sha256:" + "a" * 64
COHORT = tuple("trajectory-" + str(i) for i in range(6))


def selected(row_id=HEALTH, purpose="public_release"):
    original = next(case for case in catalog.definitions() if case.row_id == row_id)
    return execution.HttpProductProfile(original, "harness_qualification",
        cumulative.http_profile(row_id, purpose=purpose))


def facts(body=b'{"status":"ok","schema":4}', *, listener="127.0.0.1", status=200):
    return semantics.ResponseFacts(status, (("Content-Type", "application/json"),), body,
        semantics.ListenerFacts((listener,)), semantics.ListenerFacts(("127.0.0.1",)))


def supplied_history(profile):
    """Supplied values only. Exact original-owner authority is deliberately absent."""
    snapshot = wire.ListenerSnapshot(b"", b"", (("tcp", "127.0.0.1", 8765),), True, ())
    observed = wire.WireObservation(b"request", b"request", b"response",
        wire.Response(headers_complete=True, framing_complete=True, body_complete=True),
        True, False, "message_complete", True, (), True, "connected_pre_request", snapshot, snapshot)
    rows = []
    for index, step in enumerate(profile.case.steps):
        body = b"{}"
        if step.expectation is not None and step.expectation.semantic is not None:
            body = step.expectation.semantic.expected_body or body
        if profile.cumulative_profile is not None and any(
                item.step_index == index for item in profile.cumulative_profile.health_successors):
            body = cumulative.HEALTH_AFTER
        rows.append(reader.StepObservation(step.step_id, index,
            "probe" if step.kind == "request" else step.kind, "authenticated",
            facts(body) if step.kind == "request" else None,
            b"{}" if step.kind == "cli" else None, b"" if step.kind == "cli" else None,
            0 if step.kind == "cli" else None, observed if step.kind == "request" else None, (), ()))
    return reader.HistoryObservation("supplied-not-physical", "a" * 64, "b" * 64, "c" * 64,
        tuple(rows), (), True, ())


def change_step(history, index, **changes):
    rows = list(history.steps)
    rows[index] = replace(rows[index], **changes)
    return replace(history, steps=tuple(rows))


class HttpM4ProfileBindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.artifacts = ArtifactDirectory("http-m4-profile-binding", retain_success=True)
        cls.addClassCleanup(cls.artifacts.close)
        cls.store = GitStore.create(cls.artifacts.root / "store.git",
            {"library/__init__.py": "raise RuntimeError('never import candidate')\n"})
        cls.commit = cls.store.head()
        cls.tree, cls.files = execution.capture_git_source(cls.store, cls.commit)

    def setUp(self):
        self.profile = selected()
        self.recipe = execution.recipe_from_case(self.profile.case)
        self.policy = execution.HttpPolicy(IMAGE)
        self.root = self.artifacts.root.resolve() / self.id().split(".")[-1]
        self.delta = self.root.with_name(self.root.name + "-deltas")
        self.cleanup = self.root.with_name(self.root.name + "-cleanup")
        self.authority = head.ExternalHead.create(self.root.with_name(self.root.name + "-head"),
            journal_roots=(self.root, self.delta))
        self.addCleanup(self.authority.close)
        self.owners = []
        self.addCleanup(lambda: [owner.close() for owner in self.owners])
        self.configure()

    def configure(self, purpose="public_release", row_id=HEALTH):
        self.profile = selected(row_id, purpose=purpose)
        self.recipe = execution.recipe_from_case(self.profile.case)
        self.binding = execution.binding_for(self.files, self.recipe, self.policy, {"kind": "fixture-no-Docker"},
            requirements_sha256=cumulative.TARGET_CONTRACT_SHA256, profile=self.profile, purpose=purpose)
        self.subject = registry.Subject("cohort", COHORT[0], "M4", "c" * 64,
            cumulative.TARGET_CONTRACT_SHA256, execution.source_sha256(self.files))
        self.prospective = self.registration_for()
        self.registration = execution.HttpRegistration(self.binding, self.commit, self.tree, "repetition-1", self.prospective)
        self.current = self.prospective
        self.freeze = registry.CohortFreeze(tuple(replace(self.subject, trajectory_id=name,
            source_sha256=self.subject.source_sha256 if i == 0 else str(i) * 64) for i, name in enumerate(COHORT)),
            "d" * 64, "e" * 64, True)
        self.admission = admission.ObservationAdmission(self.prospective,
            verify_registration=lambda: self.current, verify_cohort=lambda: self.freeze)

    def registration_for(self, subject=None, binding=None):
        return execution.observation_registration_for(binding or self.binding, self.profile, self.policy,
            subject=subject or self.subject, gate_id="http-m4-whole-history", commit_oid=self.commit,
            tree_oid=self.tree, repetition_id="repetition-1", cohort_trajectory_ids=COHORT)

    def owner(self, **kwargs):
        owner = execution.CandidateHttpExecution(self.root, self.store, self.registration, self.recipe, self.policy,
            profile=self.profile, observation_admission=self.admission, mode="fixture", checkpoint_authority=self.authority,
            delta_root=self.delta, cleanup_root=self.cleanup, **kwargs)
        self.owners.append(owner)
        return owner

    def test_complete_catalog_retains_all_original_mechanics_and_diagnostics(self):
        counts, successors = Counter(), Counter()
        for case in catalog.definitions():
            profile = execution.HttpProductProfile(case, "harness_qualification",
                cumulative.http_profile(case.row_id, purpose="public_release"))
            original = execution.HttpProductProfile(case, "harness_qualification")
            recipe = execution.recipe_from_case(profile.case)
            self.assertEqual(recipe.record(), execution.recipe_from_case(original.case).record())
            self.assertEqual(profile.diagnostic_case_ids, original.ordered_case_ids)
            self.assertEqual(profile.diagnostic_case_ids, profile.cumulative_profile.diagnostic_case_ids)
            self.assertEqual(profile.ordered_case_ids,
                (*profile.cumulative_profile.decisive_case_ids, profile.mechanics_case_id))
            self.assertLessEqual(len(profile.ordered_case_ids), 128)
            self.assertEqual(tuple(row.step_id for row in recipe.steps), tuple(row.step_id for row in case.steps))
            counts.update(histories=1, steps=len(recipe.steps), requests=sum(row.kind == "probe" for row in recipe.steps))
            if profile.cumulative_profile.health_successors:
                successors[case.row_id] = len(profile.cumulative_profile.health_successors)
        self.assertEqual(counts, {"histories": 276, "steps": 8203, "requests": 7602})
        self.assertEqual(successors, {HEALTH: 1, "HTTP-PERSIST-LISTENER/listener-all-epochs": 2})

    def test_exact_m4_binding_and_disclosed_guard_no_broad_coverage(self):
        self.assertEqual((self.binding.milestone, self.binding.protocol), ("M4", execution.M4_PROTOCOL))
        self.assertEqual(self.binding.cumulative_profile_sha256, self.profile.cumulative_profile.sha256)
        self.assertEqual(self.binding.target_definition_sha256,
            cumulative.digest(self.profile.cumulative_profile.target_definition()))
        gate = self.prospective.gate
        self.assertEqual(gate.binding.subject, self.subject)
        self.assertEqual(gate.binding.execution_protocol, execution.M4_PROTOCOL)
        self.assertEqual(gate.ordered_case_ids, self.profile.ordered_case_ids)
        self.assertEqual(gate.requirement_ids, (*self.profile.case.requirement_ids, *self.profile.case.interaction_ids))
        record = self.profile.record()
        self.assertFalse(record["mechanics_guard"]["semantic_credit"])
        self.assertFalse(record["whole_project_acceptance"])
        self.assertEqual(record["remaining_coverage"], list(cumulative.REMAINING_COVERAGE))

    def test_target_requirements_and_purpose_cannot_be_relabelled(self):
        for requirements, purpose in ((cumulative.ORIGINAL_CONTRACT_SHA256, "public_release"),
                                      (cumulative.TARGET_CONTRACT_SHA256, "independent_acceptance")):
            with self.subTest(purpose=purpose), self.assertRaises(ValueError):
                execution.binding_for(self.files, self.recipe, self.policy, {"kind": "fixture-no-Docker"},
                    requirements_sha256=requirements, profile=self.profile, purpose=purpose)
        for changes in ({"milestone": "M1"}, {"protocol": execution.PROTOCOL},
                        {"cumulative_profile_sha256": None}, {"target_definition_sha256": None}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                replace(self.binding, **changes)

    def test_registration_rejects_wrong_subject_or_target_hash(self):
        for changes in ({"milestone": "M1"}, {"requirements_sha256": cumulative.ORIGINAL_CONTRACT_SHA256},
                        {"source_sha256": "f" * 64}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.registration_for(subject=replace(self.subject, **changes))
        with self.assertRaises(ValueError):
            self.registration_for(binding=replace(self.binding, target_definition_sha256="f" * 64))

    def test_owner_config_and_fixture_intent_bind_m4_and_reopen_without_dispatch(self):
        with patch.object(execution.engine, "runtime_identity", side_effect=AssertionError("Engine forbidden")):
            owner = self.owner()
            config = owner.read_authenticated("config.json")
            self.assertEqual(json.loads(config)["protocol"], execution.M4_PROTOCOL)
            self.assertEqual(owner.config["product_profile"], self.profile.record())
            with self.assertRaises(execution.ExecutionError):
                owner.execute_once()
            self.assertEqual(owner.json_authenticated("intent.json")["protocol"], execution.M4_PROTOCOL)
            checkpoint = owner.checkpoint()
            owner.close()
            reopened = self.owner(expected_checkpoint=checkpoint)
            self.assertEqual(reopened.read_authenticated("config.json"), config)
            with self.assertRaises(execution.ExecutionUnknown):
                reopened.execute_once()
            with self.assertRaises(ValueError):
                reader.observe_execution(reopened, reopened.checkpoint())

    def test_independent_m4_requires_original_six_trajectory_freeze(self):
        self.configure("independent_acceptance")
        owner = self.owner()
        self.assertEqual(owner.retained_freeze, self.freeze)
        self.assertTrue(all(subject.milestone == "M4" for subject in owner.retained_freeze.subjects))
        self.freeze = replace(self.freeze, subjects=(*self.freeze.subjects[:-1],
            replace(self.freeze.subjects[-1], source_sha256="f" * 64)))
        with self.assertRaises(admission.AdmissionError):
            owner.execute_once()
        self.assertFalse((self.root / "intent.json").exists())

    def test_full_e01_m4_config_reopens_with_original_seventy_requests(self):
        self.configure(row_id="HTTP-ACTION-STATE/old-token-after-successor-failed")
        owner = self.owner()
        config_raw = owner.read_authenticated("config.json")
        self.assertGreater(len(config_raw), 300000)
        self.assertEqual(sum(step.kind == "probe" for step in owner.recipe.steps), 70)
        genesis = json.loads((self.delta / "genesis.json").read_bytes())
        context_raw = json.dumps(genesis["context"], sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
        self.assertLessEqual(len(context_raw), execution.compact_limits().max_context_bytes)
        self.assertEqual(genesis["limits"], asdict(execution.compact_limits()))
        checkpoint = owner.checkpoint()
        owner.close()
        reopened = self.owner(expected_checkpoint=checkpoint)
        self.assertEqual(reopened.read_authenticated("config.json"), config_raw)
        self.assertEqual(reopened.recipe.record(), execution.recipe_from_case(self.profile.case).record())

    def test_owner_rechecks_current_profile_before_intent_and_bridge_rejects_fixture(self):
        owner = self.owner()
        bridge = source.HttpObservationSource(owner, owner.checkpoint(), receipt_path=self.root / "semantic-verifier.json")
        with self.assertRaises(AuthorityError):
            bridge.observation(self.prospective.gate, None)
        target = self.profile.cumulative_profile
        object.__setattr__(target, "purpose", "repeatability")
        with self.assertRaises(ValueError):
            owner.execute_once()
        self.assertFalse((self.root / "intent.json").exists())

    def test_changed_original_definition_or_original_purpose_rejected(self):
        with self.assertRaises(ValueError):
            replace(self.profile, original_definition_purpose="public_release")
        with self.assertRaises(ValueError):
            replace(self.profile, case=replace(self.profile.case, row_id="HTTP-EMPTY-HEALTH/other"))
        other = cumulative.http_profile("HTTP-EMPTY-HEALTH/documents", purpose="public_release")
        with self.assertRaises(ValueError):
            replace(self.profile, cumulative_profile=other)

    def test_m1_default_keeps_original_profile_gate_and_health_expectation(self):
        original = execution.HttpProductProfile(self.profile.case, "harness_qualification")
        binding = execution.binding_for(self.files, self.recipe, self.policy, {"kind": "fixture-no-Docker"},
            requirements_sha256=next(iter(execution.finite.SUPPORTED_REQUIREMENTS_SHA256)),
            profile=original, purpose="public_release")
        self.assertEqual((binding.milestone, binding.protocol), ("M1", execution.PROTOCOL))
        self.assertIsNone(binding.cumulative_profile_sha256)
        self.assertEqual(original.ordered_case_ids, original.diagnostic_case_ids)
        self.assertNotIn("mechanics_guard", original.record())
        index = self.profile.cumulative_profile.health_successors[0].step_index
        self.assertEqual(original.case.steps[index].expectation.semantic.expected_body, cumulative.HEALTH_BEFORE)


class HttpM4DiagnosticProjectionTests(unittest.TestCase):
    def test_health_successor_and_all_original_diagnostics_with_explicit_guard(self):
        profile = selected()
        projected = source.project_outcomes(profile, supplied_history(profile))
        self.assertEqual(tuple(item.case_id for item in projected.diagnostics), profile.diagnostic_case_ids)
        self.assertEqual(tuple(item.case_id for item in projected.decisive_outcomes), profile.ordered_case_ids)
        self.assertTrue(all(item.status == "passed" for item in projected.decisive_outcomes))
        index = profile.cumulative_profile.health_successors[0].step_index
        old = change_step(supplied_history(profile), index, facts=facts(cumulative.HEALTH_BEFORE))
        observed = source.project_outcomes(profile, old)
        self.assertEqual(observed.diagnostics[index].status, "failed")
        self.assertEqual(observed.mechanics_guard.status, "passed")

    def test_raw_only_body_status_never_gains_semantic_credit(self):
        profile = selected(RAW)
        index = next(i for i, row in enumerate(profile.case.steps) if row.expectation and row.expectation.raw_facts_only)
        original = change_step(supplied_history(profile), index, facts=facts(b"not JSON", status=599))
        projected = source.project_outcomes(profile, original)
        self.assertEqual(len(projected.diagnostics), len(profile.case.steps))
        self.assertNotIn(profile.diagnostic_case_ids[index], tuple(item.case_id for item in projected.decisive_outcomes))
        raw = projected.diagnostics[index]
        self.assertNotIn("status", tuple(item.name for item in raw.facets))
        self.assertNotIn("body_shape_value", tuple(item.name for item in raw.facets))
        self.assertEqual(next(item.disposition for item in raw.facets if item.name == "raw_facts"), "unspecified")
        self.assertEqual(projected.mechanics_guard.status, "passed")

    def test_raw_only_listener_failure_dominates_missing_step_capture_and_cleanup(self):
        profile = selected(RAW)
        index = next(i for i, row in enumerate(profile.case.steps) if row.expectation and row.expectation.raw_facts_only)
        original = supplied_history(profile)
        partial = replace(original.steps[index].wire_observation, exchange_complete=False)
        original = change_step(original, index, facts=facts(listener="0.0.0.0"), wire_observation=partial)
        original = change_step(original, len(profile.case.steps) - 1, state="unentered")
        original = replace(original, missing_step_ids=(profile.case.steps[-1].step_id,),
            cleanup_verified=False, infrastructure=("retirement unavailable",))
        projected = source.project_outcomes(profile, original)
        self.assertEqual(projected.diagnostics[index].status, "failed")
        self.assertEqual(projected.mechanics_guard.status, "failed")
        self.assertEqual(projected.decisive_outcomes[-1].status, "failed")
        self.assertTrue(any(row.disposition == "unavailable" for row in projected.mechanics_guard.facets))
        self.assertTrue(any(row.name.endswith(":listener_before") and row.disposition == "fail"
                            for row in projected.mechanics_guard.facets))

    def test_missing_capture_lineage_or_cleanup_cannot_project_to_pass(self):
        profile = selected()
        original = supplied_history(profile)
        controls = (change_step(original, 1, wire_observation=None),
            change_step(original, 1, state="unavailable"), change_step(original, 2, state="unentered"),
            change_step(original, 1, limitations=("body capture exhausted",)),
            replace(original, cleanup_verified=False), replace(original, infrastructure=("lost original",)),
            replace(original, missing_step_ids=(profile.case.steps[-1].step_id,)))
        for value in controls:
            with self.subTest(value=value):
                self.assertEqual(source.project_outcomes(profile, value).mechanics_guard.status, "infrastructure_error")

    def test_unavailable_raw_only_facts_cannot_forge_a_known_listener_failure(self):
        profile = selected(RAW)
        index = next(i for i, row in enumerate(profile.case.steps) if row.expectation and row.expectation.raw_facts_only)
        original = change_step(supplied_history(profile), index, state="unavailable", facts=facts(listener="0.0.0.0"))
        projected = source.project_outcomes(profile, original)
        self.assertEqual(projected.mechanics_guard.status, "infrastructure_error")
        self.assertFalse(any(item.disposition == "fail" for item in projected.diagnostics[index].facets))

    def test_known_semantic_failure_survives_incomplete_mechanics(self):
        profile = selected()
        original = change_step(supplied_history(profile), 1, facts=facts(status=500), wire_observation=None)
        projected = source.project_outcomes(profile, replace(original, cleanup_verified=False))
        self.assertEqual(projected.diagnostics[1].status, "failed")
        self.assertEqual(projected.mechanics_guard.status, "infrastructure_error")
        self.assertIn("failed", tuple(item.status for item in projected.decisive_outcomes))

    def test_complete_ordered_diagnostic_roster_is_required(self):
        profile = selected()
        original = supplied_history(profile)
        for steps in (original.steps[:-1], tuple(reversed(original.steps)), (original.steps[0],) * len(original.steps)):
            with self.subTest(steps=steps), self.assertRaises(ValueError):
                source.project_outcomes(profile, replace(original, steps=steps))

    def test_m1_semantics_and_raw_only_diagnostic_behavior_unchanged(self):
        profile = selected()
        original_profile = execution.HttpProductProfile(profile.case, "harness_qualification")
        original = change_step(supplied_history(original_profile), 1, wire_observation=None)
        projected = source.project_outcomes(original_profile, original)
        self.assertIsNone(projected.mechanics_guard)
        self.assertTrue(all(item.status == "passed" for item in projected.decisive_outcomes))
        self.assertFalse(any(facet.name.startswith("mechanics_") for row in projected.diagnostics for facet in row.facets))
        raw = execution.HttpProductProfile(selected(RAW).case, "harness_qualification")
        raw_rows = source.project_outcomes(raw, supplied_history(raw)).diagnostics
        index = next(i for i, row in enumerate(raw.case.steps) if row.expectation and row.expectation.raw_facts_only)
        self.assertEqual(raw_rows[index].status, "skipped")

    def test_supplied_projection_never_is_original_owner_authority(self):
        profile = selected()
        projected = source.project_outcomes(profile, supplied_history(profile))
        self.assertEqual(len(projected.decisive_outcomes), len(profile.case.steps) + 1)
        with self.assertRaises(ValueError):
            reader.observe_execution(projected, execution.ControllerCheckpoint("a" * 64, 0, "b" * 64, 0, 0, 1))
