"""Authored physical C06 CLI mechanics qualification; root runs after freeze.

These controls use real disposable candidate containers and original journals.
The six-subject registration/freeze capability is an explicitly synthetic host
fixture, not the future study controller or a comparative acceptance result.
No qualification-era observation is relabelled as product evidence.
"""
from __future__ import annotations

from dataclasses import asdict, replace
import hashlib
import json
import os
from pathlib import Path
import unittest

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness import candidate_cli_acceptance_profile_v1 as profile
from gossip_harness import candidate_client_execution_v5 as execution
from gossip_harness import candidate_client_observation_source_v1 as bridge
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import project_acceptance_registry_v1 as registry
from gossip_harness.library_project_fixture_v1 import RUNTIME_IMAGE
from gossip_harness.library_v2_json_reference_v2 import corrected_v2_binary_files, corrected_v2_files
from tests.test_candidate_clients_docker_v4 import make_store

COHORT = tuple("qualification-trajectory-" + str(index) for index in range(6))


def _write_new(path, value):
    with path.open("xb") as stream:
        stream.write(execution.encoded(value))


@unittest.skipUnless(os.environ.get("GOSSIP_RUN_DOCKER_TESTS") == "1", "explicit real Docker lane")
class CandidateClientAcceptanceV5DockerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.artifacts = ArtifactDirectory("candidate-client-acceptance-v5-physical", retain_success=True)
        cls.addClassCleanup(cls.artifacts.close)

    def _run_original(self, name, files, case_id, purpose, *, timeout_seconds=30):
        root = self.artifacts.root.resolve() / name
        root.mkdir()
        store = make_store(root / "source.git", files)
        commit = store.head()
        tree, captured = execution.capture_git_source(store, commit)
        self.assertEqual(captured, files)
        endpoint = execution.process_transport.EngineEndpoint.from_environment()
        policy = execution.ClientPolicy(RUNTIME_IMAGE, command_timeout_seconds=timeout_seconds)
        runtime = execution.runtime_identity(endpoint, policy.image_id,
                                              timeout_seconds=policy.transport_timeout_seconds)
        binding = execution.binding_for(captured, case_id, policy, runtime,
            requirements_sha256=profile.NORMATIVE_SHA256["library-cumulative-product-v2.json"], purpose=purpose)
        subject = registry.Subject("c06-cli-mechanics-fixture", COHORT[0], "M1",
            execution.digest({"protocol": "synthetic-c06-cli-mechanics-registration-v1"}),
            binding.requirements_sha256, binding.source_sha256)
        gate = execution.gate_for(subject, binding, gate_id="cli-" + name)
        registered = execution.ClientRegistration(binding, commit, tree, name + "-fresh-1", gate, COHORT)
        prospective = execution.observation_registration(registered)
        frozen = None if purpose == "public_release" else registry.CohortFreeze(
            tuple(replace(subject, trajectory_id=identifier) for identifier in COHORT),
            execution.digest({"synthetic_barrier": name}), execution.digest({"synthetic_barrier_verifier": name}), True)
        records = {"registration": asdict(prospective), "freeze": None if frozen is None else asdict(frozen),
            "authority_fixture": "synthetic-only; no model actors or real study trajectories",
            "acceptance_credit": False, "production_scope_authority": False}
        _write_new(root / "prospective.json", records)
        expected = (root / "prospective.json").read_bytes()
        def authenticate_registration():
            admission.require((root / "prospective.json").read_bytes() == expected,
                              "Retained prospective fixture changed")
            return prospective
        def authenticate_freeze():
            authenticate_registration()
            return frozen
        authority = admission.ObservationAdmission(prospective,
            verify_registration=authenticate_registration, verify_cohort=authenticate_freeze)
        checkpoints = []
        def retain_checkpoint(checkpoint):
            path = root / ("checkpoint-" + str(len(checkpoints)).zfill(4) + ".json")
            _write_new(path, asdict(checkpoint))
            checkpoints.append(checkpoint)
        journal = root / "journal"
        with execution.CandidateClientExecution(journal, store, registered, policy, endpoint=endpoint,
                admission_authority=authority, checkpoint_sink=retain_checkpoint) as owner:
            original = owner.execute_once()
            before_publication = owner.checkpoint()
            with self.assertRaises(bridge.ObservationError):
                bridge.ClientObservationSource(owner, before_publication)
            checkpoint = bridge.publish_verifier(owner)
            self.assertEqual(checkpoint, checkpoints[-1])
            source = bridge.ClientObservationSource(owner, checkpoint)
            observed = source.observation(gate, frozen)
            self.assertEqual(bridge.publish_verifier(owner), checkpoint, "Verifier publication must be idempotent")
            self.assertEqual(source.observation(gate, frozen), observed)
            with self.assertRaises(bridge.ObservationError):
                source.observation(replace(gate, gate_id="foreign-gate"), frozen)
            if frozen is not None:
                with self.assertRaises(bridge.ObservationError):
                    source.observation(gate, replace(frozen, receipt_sha256="f" * 64))
            self.assertEqual(owner.checkpoint(), checkpoint)
        # A fresh owner must use the exact separately retained checkpoint and
        # original purpose/freeze. Reading does not dispatch candidate code again.
        with execution.CandidateClientExecution(journal, store, registered, policy, endpoint=endpoint,
                admission_authority=authority, expected_checkpoint=checkpoint) as reopened:
            self.assertEqual(bridge.ClientObservationSource(reopened, checkpoint).observation(gate, frozen), observed)
            self.assertEqual(reopened.checkpoint(), checkpoint)
        _write_new(root / "qualification-observation.json", {
            "scope": "physical C06 product-purpose mechanics only", "acceptance_credit": False,
            "source_files": execution.source_manifest(captured),
            "physical_observation": asdict(observed), "original_status": original.status,
            "original_missing_step_ids": original.missing_step_ids,
            "checkpoint": asdict(checkpoint), "terminal_sha256": original.terminal_sha256})
        return observed, original, root

    def test_public_empty_positive_original_verifier_and_reopen(self):
        files = {"library/__init__.py": b"", "library/__main__.py": (
            "import json,sys\n"
            "value={'format':'local-research-library-v0','documents':[]} if 'export' in sys.argv else {'documents':[],'total':0}\n"
            "print(json.dumps(value))\n").encode()}
        observed, original, _ = self._run_original("public-empty", files, "cli-empty", "public_release")
        self.assertEqual(original.status, "completed")
        self.assertTrue(original.cleanup_verified)
        self.assertEqual(len(observed.execution.outcomes), 12)
        self.assertTrue(all(item.status == "passed" for item in observed.execution.outcomes))
        self.assertIsNone(observed.execution.cohort_freeze_sha256)

    def test_independent_persistence_keeps_unspecified_cells_and_exact_freeze(self):
        observed, original, _ = self._run_original("independent-persistence",
            {name: text.encode("utf-8") for name, text in corrected_v2_files().items()} | corrected_v2_binary_files(),
                                                  "cli-db-root-isolation", "independent_acceptance")
        self.assertEqual(original.status, "completed")
        self.assertTrue(original.cleanup_verified)
        self.assertEqual(len(observed.execution.outcomes), len(profile.ordered_assertion_ids("cli-db-root-isolation")))
        statuses = [item.status for item in observed.execution.outcomes]
        self.assertEqual(statuses.count("skipped"), 2)
        self.assertEqual(statuses.count("failed"), 0)
        self.assertEqual(statuses.count("infrastructure_error"), 0)
        self.assertIsNotNone(observed.execution.cohort_freeze_sha256)

    def test_repeatability_known_failure_survives_hang_and_missing_tail(self):
        files = {"library/__init__.py": b"", "library/__main__.py": (
            "from pathlib import Path\nimport time\n"
            "counter=Path('/tmp/c06-control-count')\n"
            "n=int(counter.read_text())+1 if counter.exists() else 1\n"
            "counter.write_text(str(n))\n"
            "if n==1:\n print('{\"documents\":[],\"total\":0}',flush=True)\n raise SystemExit(2)\n"
            "print('{\"completed\":true}',flush=True)\ntime.sleep(120)\n").encode()}
        observed, original, _ = self._run_original("repeatability-defect", files, "cli-empty", "repeatability", timeout_seconds=3)
        self.assertEqual(original.status, "observation_unavailable")
        self.assertTrue(original.cleanup_verified)
        self.assertEqual(original.missing_step_ids, ("s03", "s04"))
        self.assertEqual(observed.execution.terminal_status, "infrastructure_error")
        self.assertEqual([item.status for item in observed.execution.outcomes[:3]], ["failed", "passed", "passed"])
        self.assertEqual([item.status for item in observed.execution.outcomes[3:]], ["infrastructure_error"] * 9)
        self.assertIsNotNone(observed.execution.cohort_freeze_sha256)
