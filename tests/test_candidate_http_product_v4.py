"""Prospective C06 physical controls; synthetic cohort authority is not a study.

Fixtures run only as isolated container main processes. The three controls use
new product-purpose journals; no old qualification evidence changes purpose.
"""
from dataclasses import asdict, replace
import os
from pathlib import Path
import unittest

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness import candidate_http_execution_v4 as execution
from gossip_harness import candidate_http_observation_source_v1 as source
from gossip_harness import candidate_http_cases_core_v1 as core
from gossip_harness import candidate_http_semantics_v1 as sem
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import project_acceptance_registry_v1 as registry
from gossip_harness import candidate_cli_cases_v1 as cli_cases
from gossip_harness.gitstore import GitStore
from gossip_harness.library_project_fixture_v1 import RUNTIME_IMAGE

SERVER_SOURCE = r'''import argparse
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
p = argparse.ArgumentParser()
p.add_argument("--db", required=True)
p.add_argument("--root", required=True)
p.add_argument("command")
p.add_argument("--port", type=int, default=18765)
args = p.parse_args()
if args.command != "serve":
    print(json.dumps({"ok": True}))
else:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            body = HEALTH_BODY
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        def log_message(self, *args):
            pass
    HTTPServer(("127.0.0.1", args.port), Handler).serve_forever()
'''
COHORT = tuple("trajectory-" + str(i) for i in range(6))
CONTROLS = (("H01", "public_release", False, False),
            ("H02", "independent_acceptance", True, False),
            ("H03", "repeatability", False, True))


def profile_for(control):
    control_id, _, _, mixed = control
    builder = core.Builder("HTTP-EMPTY-HEALTH/c06-" + control_id.lower(), ("V0-HTTP-01",),
                           ("V0-CLI-03",) if mixed else ())
    builder.start()
    builder.request("GET", "/health", sem.success("health"), check=False)
    builder.stop()
    if mixed:
        builder.cli(("jobs",))
        builder.start()
        builder.request("GET", "/health", sem.success("health"), check=False)
        builder.stop()
    return execution.HttpProductProfile(builder.finish(), "harness_qualification")


def files_for(defect):
    body = b'{"status":"wrong","schema":0}' if defect else b'{"status":"ok","schema":0}'
    return {"library/__init__.py": "", "library/__main__.py": SERVER_SOURCE.replace("HEALTH_BODY", repr(body))}


def write_new(path, value):
    raw = execution.encoded(value)
    with path.open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    return execution.sha256(raw)


class HttpProductV4PhysicalDefinitionTests(unittest.TestCase):
    def test_complete_prospective_controls_bind_all_purposes_and_mixed_history(self):
        self.assertEqual(tuple(row[1] for row in CONTROLS), registry.PURPOSES)
        profiles = tuple(profile_for(row) for row in CONTROLS)
        self.assertEqual(tuple(len(item.case.steps) for item in profiles), (3, 3, 7))
        self.assertEqual(sum(item.case.counts["http_requests"] for item in profiles), 4)
        self.assertEqual(sum(item.case.counts["finite_cli_processes"] for item in profiles), 1)
        self.assertTrue(all(item.original_definition_purpose == "harness_qualification" for item in profiles))
        self.assertNotEqual(files_for(True), files_for(False))
        self.assertTrue(all(len(execution.recipe_from_case(item.case).steps) == len(item.ordered_case_ids) for item in profiles))


@unittest.skipUnless(os.environ.get("GOSSIP_RUN_DOCKER_TESTS") == "1", "explicit real Docker lane")
class HttpProductV4DockerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.artifacts = ArtifactDirectory("candidate-http-product-v4", retain_success=True)
        cls.addClassCleanup(cls.artifacts.close)
        cls.outcomes = {row[0]: {"control_id": row[0], "status": "not-run", "product_credit": False} for row in CONTROLS}
        cls.addClassCleanup(cls.save_census)
        cls.endpoint = execution.engine.EngineEndpoint.from_environment()
        def retain_runtime(name, raw):
            with (cls.artifacts.root / name).open("xb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
        cls.runtime = execution.engine.runtime_identity(cls.endpoint, RUNTIME_IMAGE,
            retain=retain_runtime, label="class-runtime")
        write_new(cls.artifacts.root / "prospective-controls.json", {"controls": [
            {"control_id": row[0], "purpose": row[1], "defect": row[2], "mixed": row[3],
             "profile": profile_for(row).record(), "files": files_for(row[2])} for row in CONTROLS],
            "planned_histories": 3, "planned_steps": 13, "planned_requests": 4, "planned_cli": 1,
            "authority": "synthetic-retained-six-subject-freeze-tests-admission-mechanics-only",
            "whole_project_acceptance": False, "statistical_samples": 0})

    @classmethod
    def save_census(cls):
        write_new(cls.artifacts.root / "completion-census.json", {"outcomes": list(cls.outcomes.values()),
            "not_run": [key for key, value in cls.outcomes.items() if value["status"] == "not-run"],
            "product_credit": False, "statistical_samples": 0})

    def run_control(self, control_id):
        row = next(row for row in CONTROLS if row[0] == control_id)
        root = self.artifacts.root.resolve() / control_id
        root.mkdir()
        self.outcomes[control_id]["status"] = "started"
        profile = profile_for(row)
        recipe = execution.recipe_from_case(profile.case)
        policy = execution.HttpPolicy(RUNTIME_IMAGE, lifetime_seconds=900)
        store = GitStore.create(root / "candidate.git", files_for(row[2]))
        commit = store.head()
        tree, files = execution.capture_git_source(store, commit)
        requirements = cli_cases.NORMATIVE_SHA256["library-cumulative-product-v2.json"]
        binding = execution.binding_for(files, recipe, policy, self.runtime, requirements_sha256=requirements,
                                        profile=profile, purpose=row[1])
        subject = registry.Subject("c06-http-controls", COHORT[0], "M1", "c" * 64, requirements, execution.source_sha256(files))
        prospective = execution.observation_registration_for(binding, profile, policy, subject=subject,
            gate_id="http-" + control_id, commit_oid=commit, tree_oid=tree, repetition_id="physical-v4-1",
            cohort_trajectory_ids=COHORT)
        registration = execution.HttpRegistration(binding, commit, tree, "physical-v4-1", prospective)
        freeze = None if row[1] == "public_release" else registry.CohortFreeze(tuple(replace(subject,
            trajectory_id=key) for key in COHORT), "d" * 64, "e" * 64, True)
        registration_sha = write_new(root / "prospective-registration.json", asdict(prospective))
        freeze_sha = write_new(root / "prospective-freeze.json", None if freeze is None else asdict(freeze))
        def verify_registration():
            self.assertEqual(execution.sha256((root / "prospective-registration.json").read_bytes()), registration_sha)
            return prospective
        def verify_freeze():
            self.assertEqual(execution.sha256((root / "prospective-freeze.json").read_bytes()), freeze_sha)
            return freeze
        authority = admission.ObservationAdmission(prospective, verify_registration=verify_registration,
                                                    verify_cohort=verify_freeze)
        checkpoints = []
        def sink(value):
            write_new(root / ("checkpoint-" + str(len(checkpoints)).zfill(5) + ".json"), asdict(value))
            checkpoints.append(value)
        with execution.CandidateHttpExecution(root / "journal", store, registration, recipe, policy,
                profile=profile, observation_admission=authority, endpoint=self.endpoint, checkpoint_sink=sink) as owner:
            original = owner.execute_once()
            self.assertEqual(original.status, "completed")
            self.assertTrue(original.cleanup_verified)
            self.assertEqual(original.infrastructure, ())
            self.assertEqual(original.missing_step_ids, ())
            self.assertEqual(checkpoints[-1], owner.checkpoint())
            bridge = source.HttpObservationSource(owner, checkpoints[-1], receipt_path=root / "semantic-verifier.json")
            observed = bridge.observation(prospective.gate, freeze)
            self.assertEqual(observed.execution.binding.purpose, row[1])
            self.assertEqual(observed.execution.receipt_sha256, original.terminal_sha256)
            statuses = tuple(item.status for item in observed.execution.outcomes)
            self.assertEqual(statuses, ("passed", "failed", "passed") if row[2] else ("passed",) * len(recipe.steps))
            self.assertEqual(observed.mode, "physical")
            write_new(root / "registry-observation.json", asdict(observed))
            self.outcomes[control_id].update(status="qualified", original_execution_id=original.execution_id,
                terminal_sha256=original.terminal_sha256, verifier_sha256=observed.execution.verifier_receipt_sha256,
                registry_statuses=statuses, purpose=row[1], cleanup_verified=original.cleanup_verified)
        return observed

    def test_H01_public_health_positive(self):
        self.run_control("H01")

    def test_H02_independent_surgical_health_defect(self):
        self.run_control("H02")

    def test_H03_repeatability_mixed_cli_restart_positive(self):
        self.run_control("H03")
