"""Offline final-M4 consumption controls; no physical or semantic-review authority."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from gossip_harness import candidate_cli_acceptance_profile_v1 as legacy
from gossip_harness import candidate_client_execution_v5 as execution
from gossip_harness import candidate_client_observation_source_v1 as bridge
from gossip_harness import candidate_client_observer_v5 as observer
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import cumulative_observation_profile_v1 as cumulative
from gossip_harness import project_acceptance_registry_v1 as registry
from gossip_harness.gitstore import GitStore
from tests.test_candidate_client_observation_source_v1 import synthetic

COHORT = tuple("cumulative-trajectory-" + str(index) for index in range(6))


def m4_observation(value, index=0, **kwargs):
    """Synthetic raw observation for comparator checks, never Registry authority."""
    old = synthetic(value.case_id, index, **kwargs)
    binding = json.loads(old.binding_json)
    binding.update(protocol=execution.M4_PROTOCOL, milestone="M4", purpose=value.purpose,
        requirements_sha256=cumulative.TARGET_CONTRACT_SHA256, profile_sha256=value.sha256,
        target_definition_sha256=execution.digest(value.target_definition()))
    return observer.process_observation(binding, json.loads(old.transport_json), old.stdout, old.stderr)


class CandidateClientCumulativeProfileV1Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.definitions = legacy.definitions()
        cls.profiles = tuple(cumulative.cli_profile(row["case_id"], purpose="public_release")
                             for row in cls.definitions)

    def test_complete_history_recipe_and_diagnostic_census_is_consumed(self):
        steps = diagnostics = decisive = skipped = 0
        for original, value in zip(self.definitions, self.profiles, strict=True):
            self.assertEqual(value.target_definition()["definition"]["recipe"], original["recipe"])
            cells = bridge._mapped_cells(value.case_id, (), cumulative_profile=value)
            outcomes = bridge._projected_outcomes(cells, value)
            self.assertEqual(tuple(row["case_id"] for row in cells), value.diagnostic_case_ids)
            self.assertEqual(tuple(row.case_id for row in outcomes), value.decisive_case_ids)
            self.assertTrue(all(row.status == "infrastructure_error" for row in outcomes))
            steps += len(original["recipe"]["steps"])
            diagnostics += len(cells)
            decisive += len(outcomes)
            skipped += sum(row["status"] == "skipped" for row in cells)
        self.assertEqual((len(self.profiles), steps, diagnostics, decisive, skipped), (57, 305, 900, 868, 32))

    def test_known_failure_and_unknown_tail_survive_projection(self):
        value = cumulative.cli_profile("cli-empty", purpose="repeatability")
        observed = m4_observation(value, exit_code=2, unavailable_stdout=True)
        cells = bridge._mapped_cells(value.case_id, (observed,), cumulative_profile=value)
        outcomes = bridge._projected_outcomes(cells, value)
        self.assertEqual(outcomes[0].status, "failed")
        self.assertTrue(all(row.status == "infrastructure_error" for row in outcomes[1:]))
        self.assertEqual(len(cells), 12)
        self.assertTrue(all(not row["step_entered"] for row in cells[3:]))

    def test_unspecified_cells_remain_in_diagnostics_without_acceptance_credit(self):
        value = cumulative.cli_profile("cli-grammar-unknown-command", purpose="public_release")
        cells = bridge._mapped_cells(value.case_id, (m4_observation(value, exit_code=2),), cumulative_profile=value)
        self.assertEqual([row["status"] for row in cells], ["passed", "skipped"])
        self.assertEqual([row.status for row in bridge._projected_outcomes(cells, value)], ["passed"])
        changed = deepcopy(cells)
        changed[1]["status"] = "passed"
        with self.assertRaises(cumulative.ProfileError): bridge._projected_outcomes(changed, value)

    def test_old_m1_observation_cannot_be_relabelled_by_an_m4_profile(self):
        value = cumulative.cli_profile("cli-empty", purpose="public_release")
        with self.assertRaises(bridge.ObservationError):
            bridge._mapped_cells(value.case_id, (synthetic(value.case_id, 0),), cumulative_profile=value)
        with self.assertRaises(bridge.ObservationError):
            bridge._mapped_cells(value.case_id, (m4_observation(value),))

    def test_wrong_purpose_profile_target_and_requirement_are_rejected(self):
        value = cumulative.cli_profile("cli-empty", purpose="independent_acceptance")
        original = m4_observation(value)
        for change in ({"purpose": "public_release"}, {"profile_sha256": "f" * 64},
                       {"target_definition_sha256": "f" * 64}, {"requirements_sha256": "f" * 64}):
            changed = json.loads(original.binding_json) | change
            observed = observer.process_observation(changed, json.loads(original.transport_json),
                                                    original.stdout, original.stderr)
            with self.subTest(change=change), self.assertRaises(bridge.ObservationError):
                bridge._mapped_cells(value.case_id, (observed,), cumulative_profile=value)

    def test_m4_observer_requires_its_closed_fields_and_milestone(self):
        value = cumulative.cli_profile("cli-empty", purpose="public_release")
        original = m4_observation(value)
        for change in ({"milestone": "M1"}, {"protocol": observer.BINDING_PROTOCOL},
                       {"target_definition_sha256": None}):
            with self.subTest(change=change), self.assertRaises(observer.ObservationUnavailable):
                observer.process_observation(json.loads(original.binding_json) | change,
                    json.loads(original.transport_json), original.stdout, original.stderr)

    def test_changed_profile_definition_is_rejected_before_mapping(self):
        value = deepcopy(self.profiles[0])
        changed = value.record()
        changed["target_contract_sha256"] = "f" * 64
        object.__setattr__(value, "_record_json", cumulative.encoded(changed))
        with self.assertRaises(cumulative.ProfileError):
            bridge._mapped_cells(value.case_id, (), cumulative_profile=value)


class CandidateClientCumulativeAdmissionV1Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix="gossip-cli-cumulative-offline-")
        cls.addClassCleanup(cls.temp.cleanup)
        cls.base = Path(cls.temp.name).resolve()
        cls.store = GitStore.create(cls.base / "source.git", {
            "library/__init__.py": "raise RuntimeError('never run on host')\n",
            "library/__main__.py": "raise RuntimeError('never run on host')\n"})
        cls.commit = cls.store.head()
        cls.tree, cls.files = execution.capture_git_source(cls.store, cls.commit)
        cls.policy = execution.ClientPolicy("sha256:" + "a" * 64)
        cls.runtime = {"kind": "fixture-no-Docker"}

    def setUp(self):
        self.root = self.base / self.id().rsplit(".", 1)[-1]
        self.root.mkdir()
        self.make_registration()

    def make_registration(self, *, purpose="public_release", case_id="cli-empty"):
        self.profile = cumulative.cli_profile(case_id, purpose=purpose)
        self.binding = execution.binding_for(self.files, case_id, self.policy, self.runtime,
            requirements_sha256=cumulative.TARGET_CONTRACT_SHA256, milestone="M4", purpose=purpose,
            cumulative_profile=self.profile)
        self.subject = registry.Subject("cumulative-offline", COHORT[0], "M4", "e" * 64,
                                         cumulative.TARGET_CONTRACT_SHA256, self.binding.source_sha256)
        self.gate = execution.gate_for(self.subject, self.binding, gate_id="legacy-cli-at-m4")
        self.registration = execution.ClientRegistration(self.binding, self.commit, self.tree, "fresh-m4", self.gate, COHORT)
        registered = execution.observation_registration(self.registration)
        self.current = {"registration": registered, "freeze": None}
        self.admission = admission.ObservationAdmission(registered,
            verify_registration=lambda: self.current["registration"], verify_cohort=lambda: self.current["freeze"])
        self.head = None

    def owner(self, *, profile=None, registration=None, expected=None):
        raw, delta = self.root / "raw", self.root / "delta"
        if self.head is None:
            self.head = execution.checkpoint_head.ExternalHead.create(self.root / "head", journal_roots=(raw, delta))
            self.addCleanup(self.head.close)
        owner = execution.CandidateClientExecution(raw, self.store, registration or self.registration, self.policy,
            mode="fixture", checkpoint_authority=self.head, delta_root=delta, cleanup_root=self.root / "cleanup",
            admission_authority=self.admission, expected_checkpoint=expected,
            cumulative_profile=self.profile if profile is None else profile)
        self.addCleanup(owner.close)
        return owner

    def test_owner_binds_complete_profile_target_and_separate_review_limitation(self):
        owner = self.owner()
        self.assertEqual(owner.protocol, execution.M4_PROTOCOL)
        self.assertEqual(owner.config["cumulative_profile"], self.profile.record())
        self.assertEqual(owner.config["target_definition"], self.profile.target_definition())
        self.assertEqual(owner.recipe, self.profile.target_definition()["definition"]["recipe"])
        self.assertEqual(owner.observation_registration.profile_sha256, self.profile.sha256)
        self.assertEqual(owner.binding.target_definition_sha256, execution.digest(self.profile.target_definition()))
        self.assertFalse(owner.config["cumulative_profile"]["acceptance_authority"])
        self.assertTrue(owner.config["cumulative_profile"]["review_required"])
        self.assertEqual(self.gate.requirement_ids, legacy.REQUIREMENT_IDS)

    def test_gate_selects_decisive_ids_without_narrowing_executed_recipe(self):
        self.make_registration(case_id="cli-grammar-unknown-command")
        owner = self.owner()
        self.assertEqual(self.gate.ordered_case_ids, self.profile.decisive_case_ids)
        self.assertEqual(len(self.profile.diagnostic_case_ids), 2)
        self.assertEqual(len(self.gate.ordered_case_ids), 1)
        self.assertEqual(owner.recipe, legacy.execution_recipe(self.profile.case_id))

    def test_binding_requires_explicit_profile_and_matching_target_purpose(self):
        for kwargs in ({"milestone": "M4"},
                       {"milestone": "M1", "cumulative_profile": self.profile},
                       {"milestone": "M4", "purpose": "repeatability", "cumulative_profile": self.profile}):
            with self.subTest(kwargs=kwargs), self.assertRaises(execution.ExecutionError):
                execution.binding_for(self.files, "cli-empty", self.policy, self.runtime,
                    requirements_sha256=cumulative.TARGET_CONTRACT_SHA256, **kwargs)
        for changes in ({"profile_sha256": "f" * 64}, {"target_definition_sha256": "f" * 64}):
            with self.subTest(changes=changes), self.assertRaises(execution.ExecutionError):
                execution.gate_for(self.subject, replace(self.binding, **changes), gate_id="substitution")

    def test_external_profile_revocation_prevents_intent(self):
        owner = self.owner()
        self.current["registration"] = replace(self.current["registration"], profile_sha256="f" * 64)
        with self.assertRaises(admission.AdmissionError): owner.execute_once()
        self.assertFalse(owner.has_retained("intent.json"))

    def test_old_m1_binding_cannot_be_relabelled_and_owner_requires_explicit_profile(self):
        old = execution.binding_for(self.files, "cli-empty", self.policy, self.runtime,
                                   requirements_sha256=cumulative.TARGET_CONTRACT_SHA256)
        relabelled = replace(old, protocol=execution.M4_PROTOCOL, milestone="M4",
            profile_sha256=self.profile.sha256, target_definition_sha256=execution.digest(self.profile.target_definition()))
        gate = execution.gate_for(self.subject, relabelled, gate_id=self.gate.gate_id)
        registration = replace(self.registration, binding=relabelled, gate=gate)
        with self.assertRaises(execution.ExecutionError): self.owner(registration=registration)
        self.assertFalse((self.root / "raw").exists())
        with self.assertRaises(execution.ExecutionError):
            execution.CandidateClientExecution(self.root / "raw", self.store, self.registration, self.policy,
                mode="fixture", checkpoint_authority=self.head, delta_root=self.root / "delta",
                cleanup_root=self.root / "cleanup", admission_authority=self.admission)
        self.assertFalse((self.root / "raw").exists())

    def test_wrong_profile_purpose_rejected_before_journal_creation(self):
        wrong = cumulative.cli_profile("cli-empty", purpose="repeatability")
        with self.assertRaises(execution.ExecutionError): self.owner(profile=wrong)
        self.assertFalse((self.root / "raw").exists())

    def test_independent_m4_freeze_and_reopen_preserve_original_profile(self):
        self.make_registration(purpose="independent_acceptance")
        owner = self.owner()
        with self.assertRaises(admission.AdmissionUnavailable): owner.execute_once()
        frozen = registry.CohortFreeze(tuple(replace(self.subject, trajectory_id=item) for item in COHORT),
                                        "a" * 64, "b" * 64, True)
        self.current["freeze"] = frozen
        with self.assertRaises(execution.ExecutionError): owner.execute_once()  # Fixture never dispatches.
        self.assertEqual(owner.retained_freeze(), frozen)
        checkpoint = owner.checkpoint()
        owner.close()
        reopened = self.owner(expected=checkpoint)
        with self.assertRaises(execution.ExecutionUnknown): reopened.execute_once()
        self.assertEqual(reopened.config["cumulative_profile"], self.profile.record())

    def test_projected_semantic_pass_cannot_hide_cleanup_failure(self):
        # Semantic execution is mocked only to isolate the real verifier/projection
        # path. Real compact journal/admission checks remain; no physical claim.
        self.make_registration(case_id="cli-grammar-unknown-command")
        owner = self.owner()
        owner._retain("intent.json", execution.encoded({"cohort_freeze": None}))
        terminal = execution.encoded({"offline_mocked_execution": True})
        owner._retain("terminal.json", terminal)
        owner.mode = "physical"
        def original():
            return execution.ClientHistoryResult("offline-m4", self.profile.case_id, "observation_unavailable",
                (m4_observation(self.profile, exit_code=2),), (), False, ("cleanup unavailable",),
                execution.sha256(terminal), owner.checkpoint())
        with patch.object(owner, "verified_execution", side_effect=original):
            checkpoint = bridge.publish_verifier(owner)
            self.assertEqual(bridge.publish_verifier(owner), checkpoint)
            result = bridge.ClientObservationSource(owner, checkpoint).observation(self.gate, None)
        verifier = owner.json_authenticated(bridge.VERIFIER_FILE)
        self.assertEqual(len(verifier["cells"]), 2)
        self.assertEqual(verifier["cells"][1]["status"], "skipped")
        self.assertEqual(result.execution.outcomes, (registry.CaseResult(self.profile.decisive_case_ids[0], "passed"),))
        self.assertEqual(result.execution.terminal_status, "infrastructure_error")
        self.assertFalse(verifier["cleanup_verified"])
        self.assertFalse(verifier["independent_semantic_review_supplied"])
        self.assertEqual(verifier["remaining_coverage"], list(cumulative.REMAINING_COVERAGE))

    def test_m4_step_binding_carries_current_profile_and_target(self):
        owner = self.owner()
        binding = owner._step_binding({"execution_id": "offline-step"}, 0)
        self.assertEqual(binding["protocol"], execution.M4_PROTOCOL)
        self.assertEqual(binding["profile_sha256"], self.profile.sha256)
        self.assertEqual(binding["target_definition_sha256"], execution.digest(self.profile.target_definition()))
        self.assertEqual(binding["milestone"], "M4")
        self.assertEqual(binding["ordered_step_ids"], [step["step_id"] for step in owner.recipe["steps"]])
