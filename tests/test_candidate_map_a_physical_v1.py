"""Fresh MAP-A opt-in qualification; four authored fixtures, zero study samples.

The exact existing histories, inputs and expectations are unchanged. Each class
owns one real source/registration/journal/container execution. Public-purpose
fixture registration is deliberately not production semantic scope authority.
Same-source verifier rereads below reconstruct one original; they are neither
new executions nor independent acceptance/repeatability observations.
"""
from __future__ import annotations

import ast
from collections import Counter
from contextlib import closing
from dataclasses import asdict, replace
import json
import os
from pathlib import Path
import tempfile
import time
import unittest

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness import candidate_checkpoint_head_v1 as head
from gossip_harness import candidate_http_cases_v1 as http_cases
from gossip_harness import candidate_http_execution_v4 as http
from gossip_harness import candidate_http_observation_source_v1 as http_source
from gossip_harness import candidate_http_observation_v2 as http_reader
from gossip_harness import candidate_http_semantics_v1 as semantics
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import candidate_product_process_core_v1 as product_cases
from gossip_harness import candidate_product_process_execution_v1 as product
from gossip_harness import candidate_product_process_observation_v1 as product_source
from gossip_harness import candidate_product_process_reader_v1 as product_reader
from gossip_harness import cumulative_observation_profile_v1 as cumulative
from gossip_harness import cumulative_scope_source_v1 as mapping
from gossip_harness import cumulative_scope_source_v3 as scope
from gossip_harness import project_acceptance_registry_v1 as registry
from gossip_harness.candidate_scope_consumer_v1 import AuthorityError
from gossip_harness.library_project_fixture_v1 import RUNTIME_IMAGE
from tests import test_candidate_product_process_batch_docker_v1 as original

PROTOCOL = "candidate-map-a-physical-qualification-v1"
PURPOSE = "public_release"
HEALTH = "HTTP-EMPTY-HEALTH/health"
RESTORE = "process-backup-restore-fences-edits-and-removed-id"
COHORT = original.COHORT
CONTROLS = (
    ("MH-P", "http", HEALTH, "reference"),
    ("MH-D", "http", HEALTH, "health-schema-zero"),
    ("MP-P", "product", RESTORE, "reference"),
    ("MP-D", "product", RESTORE, "reimport-loses-high-water"),
)
HEALTH_MUTATION = ("library/query/legacy_v1_service.py",
    "return status, {'status': 'ok', 'schema': 4}",
    "return status, {'status': 'ok', 'schema': 0}")


def profile_for(row):
    if row[1] == "http":
        case = next(case for case in http_cases.definitions() if case.row_id == row[2])
        return http.HttpProductProfile(case, cumulative.ORIGINAL_DEFINITION_PURPOSE,
            cumulative.http_profile(row[2], purpose=PURPOSE), mapping_profile=mapping.MAP_A_MAPPING)
    if row[1] == "product":
        return product.HttpProductProfile(product_cases.case_definition(row[2]),
            capture_policy=product.source_capture.BatchCapturePolicy(), mapping_profile=mapping.MAP_A_MAPPING)
    raise ValueError("Closed MAP-A control family required")


def candidate_for(row):
    files, evidence = original.candidate_files("reference" if row[3] == "health-schema-zero" else row[3])
    if row[3] != "health-schema-zero":
        return files, evidence
    path, before, after = HEALTH_MUTATION
    source = files[path].decode("utf-8")
    if source.count(before) != 1:
        raise ValueError("Unique prospective health mutation seam changed")
    files = dict(files)
    files[path] = source.replace(before, after, 1).encode("utf-8")
    ast.parse(files[path], filename=path)  # Inert syntax only; never import candidate code.
    return files, {**evidence, "variant": row[3],
        "candidate_source_manifest": product.source_manifest(files),
        "mutation": {"path": path, "before_literal": before, "after_literal": after,
            "original_sha256": product.sha256(source.encode("utf-8")),
            "mutated_sha256": product.sha256(files[path])}}


def recipe_and_policy(row, profile):
    module = http if row[1] == "http" else product
    return module.recipe_from_case(profile.case), module.HttpPolicy(RUNTIME_IMAGE,
        lifetime_seconds=900 if row[1] == "http" else 1800)


def selected_slice(row, registration, profile, policy):
    factory = scope.http_slice if row[1] == "http" else scope.product_process_slice
    return factory(registration, profile, policy, mapping_profile=mapping.MAP_A_MAPPING)


def defect_witness(raw, defect_value):
    """Exact value/serialization helper; caller must first authenticate raw origin."""
    if type(raw) is not bytes or semantics.semantic_compare(
            semantics.parse_json(raw), semantics.parse_json(product.encoded(defect_value))) != "pass":
        raise AssertionError("Original raw bytes do not prove the exact declared defect value")
    return {"original_raw_sha256": product.sha256(raw), "original_raw_hex": raw.hex(),
        "semantic_match": "pass", "proven_semantic_value": defect_value}


def planned_control(row):
    profile = profile_for(row)
    recipe, policy = recipe_and_policy(row, profile)
    return {"control_id": row[0], "family": row[1], "history_id": row[2], "candidate_variant": row[3],
        "profile": profile.record(), "recipe": recipe.record(), "policy": asdict(policy),
        "planned_steps": len(recipe.steps), "planned_by_kind": dict(Counter(step.kind for step in recipe.steps)),
        "purpose": PURPOSE, "original_definition_purpose": profile.original_definition_purpose,
        "expected_discrepancy": None if row[3] == "reference" else
            {"http": "health body_shape_value; original decoded schema0 rather than required schema4",
             "product": "s018-cli/json_exact; original reimport edit_version1 rather than required2"}[row[1]],
        "fixture_registration_authority": True, "semantic_scope_approval": False,
        "whole_project_acceptance": False, "scientific_samples": 0,
        "full_study_histories_preserved": 602, "full_source_units_preserved": 312,
        "independent_acceptance_qualified": False, "repeatability_qualified": False,
        "physical_execution_reuse": False, "adaptive_deadlines": False}


class MapAPhysicalDefinitionV1Tests(unittest.TestCase):
    def test_four_closed_controls_preserve_existing_whole_histories(self):
        self.assertEqual(tuple(row[0] for row in CONTROLS), ("MH-P", "MH-D", "MP-P", "MP-D"))
        for row in CONTROLS:
            profile = profile_for(row)
            legacy = replace(profile, mapping_profile=None)
            self.assertEqual(profile.case, legacy.case)
            self.assertEqual(profile.ordered_case_ids, legacy.ordered_case_ids)
            self.assertEqual(profile.record()["definition"], legacy.record()["definition"])
            self.assertNotEqual(profile.sha256, legacy.sha256)
            self.assertEqual(profile.milestone, "M4")
            self.assertEqual(profile.mapping_record()["protocol"], mapping.MAP_A_MAPPING)
            self.assertFalse(profile.mapping_record()["semantic_approval"])

    def test_unique_mutations_change_exactly_one_candidate_file(self):
        for row in CONTROLS:
            actual, evidence = candidate_for(row)
            baseline, _ = candidate_for((row[0], row[1], row[2], "reference"))
            changed = {name for name in baseline if baseline[name] != actual[name]}
            self.assertEqual(set(actual), set(baseline))
            self.assertEqual(changed, set() if row[3] == "reference" else {evidence["mutation"]["path"]})
            if row[3] != "reference":
                self.assertEqual(product.sha256(actual[evidence["mutation"]["path"]]), evidence["mutation"]["mutated_sha256"])

    def test_health_witness_retains_original_value_and_explicit_m4_successor(self):
        profile = profile_for(CONTROLS[1])
        successors = profile.cumulative_profile.health_successors
        self.assertEqual(len(successors), 1)
        self.assertEqual(successors[0].target_body, cumulative.HEALTH_AFTER)
        self.assertEqual(profile.case.steps[successors[0].step_index].expectation.semantic.shape, "health")
        self.assertIn("M4-API-SCHEMA", profile.requirement_ids)
        self.assertEqual(recipe_and_policy(CONTROLS[1], profile)[1].lifetime_seconds, 900)
        # Same value/serialization path as physical witnesses, with authored raw
        # bytes only: no claim of an original candidate observation here.
        examples = ((b'{"schema":0,"status":"ok"}', {"status": "ok", "schema": 0}),
                    (b'{"edit_version":1,"notes":""}', {"edit_version": 1, "notes": ""}))
        with tempfile.TemporaryDirectory() as directory:
            for index, (raw, expected) in enumerate(examples):
                record = defect_witness(raw, expected)
                path = Path(directory) / (str(index) + ".json")
                written = original.write_new(path, record)
                self.assertEqual(path.read_bytes(), written)
                self.assertEqual(json.loads(written)["proven_semantic_value"], expected)
                self.assertEqual(bytes.fromhex(record["original_raw_hex"]), raw)
        for raw in (b'{"schema":4,"status":"ok"}', b'{"schema":false,"status":"ok"}',
                    b'{"schema":0,"schema":0,"status":"ok"}'):
            with self.assertRaises(AssertionError):
                defect_witness(raw, {"status": "ok", "schema": 0})

    def test_high_water_witness_is_existing_d03_exact_json_observation(self):
        profile = profile_for(CONTROLS[3])
        step = profile.case.steps[18]
        self.assertEqual(step.step_id, "s018-cli")
        self.assertEqual(step.expectation.record()["json"]["edit_version"], 2)
        self.assertEqual(original.DEFECTS[2][2:], ("reimport-loses-high-water", (("s018-cli", "json_exact"),)))
        self.assertIn("M3-DIAGNOSTICS", profile.requirement_ids)
        self.assertEqual(recipe_and_policy(CONTROLS[3], profile)[1].lifetime_seconds, 1800)

    def test_fixture_purpose_is_explicit_and_cannot_claim_independent_scope(self):
        for row in CONTROLS:
            value = planned_control(row)
            self.assertEqual(value["purpose"], "public_release")
            self.assertFalse(value["semantic_scope_approval"] or value["whole_project_acceptance"])
            self.assertFalse(value["independent_acceptance_qualified"] or value["repeatability_qualified"])
            self.assertEqual(value["scientific_samples"], 0)
            self.assertEqual((value["full_study_histories_preserved"], value["full_source_units_preserved"]), (602, 312))


class _MapAControl(original._PhysicalProductControl):
    """Reuse retained-raw product setup/cleanup assertions, never its dispatcher."""
    CONTROL_ID = ""

    @classmethod
    def setUpClass(cls):
        cls.row = next(row for row in CONTROLS if row[0] == cls.CONTROL_ID)
        cls.artifacts = ArtifactDirectory("map-a-physical-v1-" + cls.CONTROL_ID.lower(), retain_success=True)
        cls.addClassCleanup(cls.artifacts.close)
        cls.outcome = {"control_id": cls.CONTROL_ID, "status": "not-run", "qualification_verified": False,
            "whole_project_acceptance": False, "scientific_samples": 0, "purpose": PURPOSE}
        cls.addClassCleanup(cls.save_census)
        original.write_new(cls.artifacts.root / "prospective-control.json", planned_control(cls.row))
        cls.endpoint = product.engine.EngineEndpoint.from_environment()
        def retain_runtime(name, raw):
            with (cls.artifacts.root / name).open("xb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
        cls.runtime = product.engine.runtime_identity(cls.endpoint, RUNTIME_IMAGE,
            retain=retain_runtime, label="class-runtime")

    @classmethod
    def save_census(cls):
        original.write_new(cls.artifacts.root / "completion-census.json", {"protocol": PROTOCOL,
            "planned_controls": [row[0] for row in CONTROLS], "this_class": cls.outcome,
            "other_controls": "separate independent class receipts; not inferred from this result",
            "scientific_samples": 0, "whole_project_acceptance": False})

    def run_map_a_control(self):
        root = self.artifacts.root.resolve()
        row, started = self.row, time.monotonic()
        self.outcome["status"] = "started-not-qualified"
        try:
            module, observer, reader = ((http, http_source, http_reader) if row[1] == "http" else
                (product, product_source, product_reader))
            profile = profile_for(row)
            recipe, policy = recipe_and_policy(row, profile)
            files, candidate = candidate_for(row)
            store = original.make_store(root / "candidate.git", files)
            commit = store.head()
            def capture():
                return (http.capture_git_source(store, commit) if row[1] == "http" else
                        product.capture_source(store, commit, policy=profile.capture_policy))
            tree, captured = capture()
            self.assertEqual(captured, files)
            binding = module.binding_for(files, recipe, policy, self.runtime,
                requirements_sha256=cumulative.TARGET_CONTRACT_SHA256, profile=profile, purpose=PURPOSE)
            subject = registry.Subject(PROTOCOL, COHORT[0], "M4", module.digest(planned_control(row)),
                cumulative.TARGET_CONTRACT_SHA256, binding.source_sha256)
            common = dict(subject=subject, gate_id="map-a-" + row[0].lower(), commit_oid=commit,
                tree_oid=tree, repetition_id="map-a-physical-v1-1", cohort_trajectory_ids=COHORT)
            prospective = module.observation_registration_for(binding, profile, policy, **common)
            registration = module.HttpRegistration(binding, commit, tree, common["repetition_id"], prospective)
            component = selected_slice(row, registration, profile, policy)
            scope.verify_slice(component)
            self.assertEqual(component.gate, prospective.gate)
            self.assertEqual(tuple(prospective.gate.requirement_ids), profile.requirement_ids)
            legacy = replace(profile, mapping_profile=None)
            legacy_binding = module.binding_for(files, recipe, policy, self.runtime,
                requirements_sha256=cumulative.TARGET_CONTRACT_SHA256, profile=legacy, purpose=PURPOSE)
            legacy_registration = module.observation_registration_for(legacy_binding, legacy, policy, **common)
            self.assertNotEqual(legacy_binding.profile_sha256, binding.profile_sha256)
            self.assertNotEqual(legacy_registration.gate.binding, prospective.gate.binding)
            with self.assertRaises(ValueError):
                module.observation_registration_for(legacy_binding, profile, policy, **common)
            retained = original.write_new(root / "prospective-registration.json", {
                "protocol": PROTOCOL, "registration": asdict(prospective), "original": asdict(registration),
                "profile": profile.record(), "mapping": profile.mapping_record(), "recipe": recipe.record(),
                "slice_sha256": component.sha256, "candidate": candidate, "policy": asdict(policy),
                "runtime": self.runtime, "evaluator_sources": module.evaluator_sources(),
                "fixture_registration_authority": True, "semantic_scope_approval": False,
                "whole_project_acceptance": False, "scientific_samples": 0})
            def authenticate_registration():
                admission.require((root / "prospective-registration.json").read_bytes() == retained,
                    "Exact prospective fixture registration changed")
                return prospective
            authority = admission.ObservationAdmission(prospective,
                verify_registration=authenticate_registration, verify_cohort=lambda: None)
            journal, deltas = root / "journal", root / "deltas"
            with closing(head.ExternalHead.create(root / "external-head", journal_roots=(journal, deltas))) as anchor, \
                 module.CandidateHttpExecution(journal, store, registration, recipe, policy, profile=profile,
                    observation_admission=authority, checkpoint_authority=anchor, delta_root=deltas,
                    cleanup_root=root / "cleanup", endpoint=self.endpoint) as owner:
                original_result = owner.execute_once()
                terminal = owner.json_authenticated("terminal.json")
                self.outcome.update(execution_status=original_result.status,
                    execution_id=original_result.execution_id, terminal_sha256=original_result.terminal_sha256,
                    cleanup_verified=original_result.cleanup_verified, infrastructure=list(original_result.infrastructure),
                    missing_step_ids=list(original_result.missing_step_ids))
                original.write_new(root / "physical-census.json", {"outcome": self.outcome, "terminal": terminal})
                self.assertEqual(original_result.status, "completed")
                self.assertTrue(original_result.cleanup_verified)
                self.assertEqual((original_result.infrastructure, original_result.missing_step_ids), ((), ()))
                checkpoint = owner.checkpoint()
                self.assertEqual(anchor.read(), checkpoint)
                original.write_new(root / "original-checkpoint.json", asdict(checkpoint))
                history = reader.observe_execution(owner, checkpoint)
                projection = observer.project_outcomes(profile, history)
                original.write_new(root / "observed-diagnostics.json", asdict(projection))
                self.assertEqual(len(history.steps), len(recipe.steps))
                self.assertTrue(all(item.state == "authenticated" and not item.limitations for item in history.steps))
                self.assertEqual(terminal["entered_step_indices"], list(range(len(recipe.steps))))
                self.assertEqual(terminal["unentered_step_ids"], [])
                self.assertEqual(owner.actual_registration, prospective)
                self.assertEqual(projection.mechanics_guard.status, "passed")
                if row[1] == "product":
                    self.assert_complete_census(owner, original_result, terminal, history)
                    self.outcome["setup_proof"] = self.assert_setup(owner)
                    self.outcome["cleanup_proof"] = self.assert_cleanup_and_order(owner, terminal)
                bridge = observer.HttpObservationSource(owner, checkpoint, receipt_path=journal / "semantic-verifier.json")
                # A real original cannot be consumed through the default-profile gate.
                with self.assertRaises(AuthorityError):
                    bridge.observation(legacy_registration.gate, None)
                self.assertEqual(owner.checkpoint(), checkpoint)
                observed = bridge.observation(prospective.gate, None)
                self.assertEqual(anchor.read(), bridge.checkpoint)
                self.assertGreater(bridge.checkpoint.sequence, checkpoint.sequence)
                self.assertEqual(observed.execution.outcomes, projection.decisive_outcomes)
                self.assertEqual(observed.execution.binding, prospective.gate.binding)
                self.assertEqual(observed.execution.receipt_sha256, original_result.terminal_sha256)
                self.assertEqual(observed.execution.terminal_status, "completed")
                self.assertEqual(observed.mode, "physical")
                verifier = owner.json_authenticated("semantic-verifier.json")
                expected_protocol = (http_source.M4_PROTOCOL if row[1] == "http" else
                    product_source.PROTOCOL + "-git-source-batch-v1") + "-map-a-v1"
                self.assertEqual(verifier["protocol"], expected_protocol)
                self.assertEqual(verifier["mapping_profile"], profile.mapping_record())
                self.assertEqual(verifier["product_profile"], profile.record())
                self.assertEqual(verifier["original_registration"], json.loads(module.encoded(asdict(prospective))))
                self.assertEqual(verifier["original_binding_sha256"], prospective.original_binding_sha256)
                self.assertEqual(verifier["original_terminal_sha256"], original_result.terminal_sha256)
                self.assertEqual(verifier["evaluator_sources"], module.evaluator_sources())
                self.assertIsNone(verifier["cohort_freeze"])
                self.assertFalse(verifier["held_out_claim"] or verifier["whole_project_acceptance"] or verifier["physical_execution_reused"])
                self.assertEqual(observed.execution.verifier_receipt_sha256,
                    module.sha256(owner.read_authenticated("semantic-verifier.json")))
                failed = {(item.step_id, facet.name) for item in projection.diagnostics for facet in item.facets
                    if facet.name in item.required_facets and facet.disposition == "fail"}
                unavailable = {(item.step_id, facet.name) for item in projection.diagnostics for facet in item.facets
                    if facet.name in item.required_facets and facet.disposition in ("unavailable", "unspecified")}
                self.assertEqual(unavailable, set())
                expected_failed = set()
                if row[3] == "health-schema-zero":
                    successor = profile.cumulative_profile.health_successors[0]
                    expected_failed = {(profile.case.steps[successor.step_index].step_id, "body_shape_value")}
                    raw = history.steps[successor.step_index].facts.body
                    self.assertIs(type(raw), bytes)
                    defect_value = {"status": "ok", "schema": 0}
                    original.write_new(root / "declared-defect-witness.json", {
                        **defect_witness(raw, defect_value), "step_index": successor.step_index,
                        "required": {"status": "ok", "schema": 4}, "facet": "body_shape_value"})
                elif row[3] == "reimport-loses-high-water":
                    expected_failed = {("s018-cli", "json_exact")}
                    raw = history.steps[18].stdout
                    self.assertIs(type(raw), bytes)
                    expected = profile.case.steps[18].expectation.record()["json"]
                    defect_value = {**expected, "edit_version": 1}
                    original.write_new(root / "declared-defect-witness.json", {
                        **defect_witness(raw, defect_value), "step_id": "s018-cli",
                        "required": expected, "facet": "json_exact"})
                self.assertEqual(failed, expected_failed)
                expected_failed_steps = {step for step, _ in expected_failed}
                self.assertEqual({item.step_id for item in projection.diagnostics if item.status == "failed"}, expected_failed_steps)
                expected_failed_cases = {item.case_id for item in projection.diagnostics if item.step_id in expected_failed_steps}
                self.assertEqual({item.case_id for item in observed.execution.outcomes if item.status == "failed"}, expected_failed_cases)
                self.assertTrue(all(item.status in ("passed", "failed") for item in observed.execution.outcomes))
                post = bridge.checkpoint
                # Exact same-purpose original reconstruction; no extra sample/reuse record.
                self.assertEqual(bridge.observation(prospective.gate, None), observed)
                self.assertEqual(owner.checkpoint(), post)
                original.write_new(root / "verified-checkpoint.json", asdict(post))
                original.write_new(root / "registry-observation.json", asdict(observed))
                self.assertEqual(capture(), (tree, files))
                self.outcome.update(failed_required_facets=sorted(failed),
                    verifier_sha256=observed.execution.verifier_receipt_sha256)
            self.outcome.update(status="qualified", qualification_verified=True)
        except BaseException as error:
            self.outcome.update(status="failed-not-qualified", failure_type=type(error).__name__, failure=str(error)[:2048])
            raise
        finally:
            self.outcome["elapsed_seconds"] = time.monotonic() - started
            original.write_new(root / "control-outcome.json", self.outcome)


_DOCKER = unittest.skipUnless(os.environ.get("GOSSIP_RUN_DOCKER_TESTS") == "1", "explicit real Docker lane")


@_DOCKER
class MapAHttpPositiveV1DockerTests(_MapAControl, unittest.TestCase):
    CONTROL_ID = "MH-P"
    def test_original_health_m4_map_a_positive(self):
        self.run_map_a_control()


@_DOCKER
class MapAHttpSchemaDefectV1DockerTests(_MapAControl, unittest.TestCase):
    CONTROL_ID = "MH-D"
    def test_original_health_schema_defect_is_exactly_detected(self):
        self.run_map_a_control()


@_DOCKER
class MapAProductPositiveV1DockerTests(_MapAControl, unittest.TestCase):
    CONTROL_ID = "MP-P"
    def test_original_restore_map_a_positive(self):
        self.run_map_a_control()


@_DOCKER
class MapAProductHighWaterDefectV1DockerTests(_MapAControl, unittest.TestCase):
    CONTROL_ID = "MP-D"
    def test_original_reimport_high_water_defect_is_exactly_detected(self):
        self.run_map_a_control()
