"""Prospective real worker qualification; no physical execution occurred at authoring.

Each class owns a fresh committed authored application, journal, keeper and DB
volume. These are public machinery controls, not scientific samples or evidence
that every recovery clause is covered. Root must register these classes before
running the explicit Docker lane. No implicit engine preflight occurs on import.
"""
from __future__ import annotations

from contextlib import closing
from dataclasses import asdict
import os
from pathlib import Path
import unittest

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness import candidate_checkpoint_head_v1 as head
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import candidate_product_worker_cases_v1 as cases
from gossip_harness import candidate_product_worker_execution_v1 as execution
from gossip_harness import candidate_product_worker_fixture_v1 as fixture
from gossip_harness import candidate_product_worker_observation_v1 as observer
from gossip_harness import project_acceptance_registry_v1 as registry
from gossip_harness.library_project_fixture_v1 import RUNTIME_IMAGE
from tests.test_candidate_clients_docker_v4 import make_store

PROTOCOL = "candidate-product-worker-physical-qualification-v1"
COHORT = ("worker-fixture-original", *("worker-fixture-reserved-" + str(i) for i in range(1, 6)))
_DOCKER = unittest.skipUnless(os.environ.get("GOSSIP_RUN_DOCKER_TESTS") == "1", "explicit real Docker lane")


def write_new(path: Path, value):
    raw = cases.encoded(value)
    with path.open("xb") as output:
        output.write(raw)
        output.flush()
        os.fsync(output.fileno())
    return raw


class _WorkerPhysical:
    CASE_INDEX = 0
    VARIANT = "instrumented"

    def run_history(self):
        artifacts = ArtifactDirectory("worker-lifecycle-v1-" + type(self).__name__.lower(), retain_success=True)
        self.addCleanup(artifacts.close)
        root = artifacts.root.resolve()
        profile = execution.WorkerProfile(cases.cases()[self.CASE_INDEX])
        policy = execution.WorkerPolicy(RUNTIME_IMAGE)
        files = fixture.instrumented_files()
        if self.VARIANT == "missing_hook":
            files["library/_evaluator_worker_v1.py"] = b"def boundary(store, phase, claim=None):\n    return\n"
        elif self.VARIANT == "early_exit":
            files["library/_evaluator_worker_v1.py"] = (b"def boundary(store, phase, claim=None):\n"
                b"    if phase == 'owner_acquired':\n        import os\n        os._exit(23)\n")
        elif self.VARIANT != "instrumented":
            raise ValueError("Closed physical variant")
        planned = {"protocol": PROTOCOL, "variant": self.VARIANT, "profile": profile.record(),
            "policy": asdict(policy), "fixture_sources": fixture.source_inputs(),
            "source_manifest": execution.source_manifest(files), "evaluator_sources": execution.evaluator_sources(),
            "scientific_samples": 0, "whole_product_acceptance": False, "scope_credit": False,
            "expected_unavailable_facets": [facet["selector"] for facet in profile.case.record["facets"]
                if facet["selector"] in ("precommit-abrupt-no-visible-graph", "old-claim-epoch-fence")]}
        write_new(root / "prospective-history.json", planned)
        store = make_store(root / "candidate.git", files)
        commit = store.head()
        tree, captured = execution.capture_git_source(store, commit)
        self.assertEqual(captured, files)
        endpoint = execution.engine.EngineEndpoint.from_environment()

        def retain_runtime(name, raw):
            with (root / name).open("xb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())

        runtime = execution.engine.runtime_identity(endpoint, RUNTIME_IMAGE, retain=retain_runtime, label="runtime")
        binding = execution.binding_for(files, profile, policy, runtime)
        subject = registry.Subject("worker-physical-qualification-v1", COHORT[0], "M4",
            execution.digest(planned), cases.CONTRACT_SHA256, binding.source_sha256)
        prospective = execution.observation_registration_for(binding, profile, policy, subject=subject,
            gate_id="worker-qualification-" + str(self.CASE_INDEX) + "-" + self.VARIANT,
            commit_oid=commit, tree_oid=tree, repetition_id="physical-v1-1", cohort_trajectory_ids=COHORT)
        registration = execution.WorkerRegistration(binding, commit, tree, "physical-v1-1", prospective)
        retained = write_new(root / "prospective-registration.json", {"registration": asdict(prospective),
            "source_registration": asdict(registration), "fixture_authority_only": True,
            "independent_purpose_credit": False})

        def authenticate_registration():
            admission.require((root / "prospective-registration.json").read_bytes() == retained,
                              "Original prospective physical fixture registration changed")
            return prospective

        authority = admission.ObservationAdmission(prospective, verify_registration=authenticate_registration,
            verify_cohort=lambda: None)
        journal, deltas = root / "journal", root / "deltas"
        outcome = {"status": "started-not-qualified", "cleanup_verified": None,
                   "scientific_samples": 0, "whole_product_acceptance": False}
        try:
            with closing(head.ExternalHead.create(root / "external-head", journal_roots=(journal, deltas))) as anchor, \
                 execution.WorkerExecution(journal, store, registration, profile, policy,
                    observation_admission=authority, checkpoint_authority=anchor, delta_root=deltas,
                    cleanup_root=root / "cleanup", endpoint=endpoint) as owner:
                checkpoint = owner.execute_once()
                terminal = execution.engine.strict_json_loads(owner.read_authenticated("terminal.json"))
                outcome.update(planned_actions=terminal["planned_actions"],
                    observed_action_records=terminal["observed_action_records"],
                    cleanup_verified=terminal["cleanup_verified"], infrastructure=terminal["infrastructure"])
                # Retain even partial census before assertions or semantic reads.
                write_new(root / "physical-census.json", outcome)
                write_new(root / "original-checkpoint.json", asdict(checkpoint))
                self.assertEqual(anchor.read(), checkpoint)
                observed = observer.read_original(owner, checkpoint)
                write_new(root / "worker-observation.json", asdict(observed))
                self.assertTrue(observed.cleanup_verified)
                self.assertFalse(observed.acceptance_authority)
                if self.VARIANT == "instrumented":
                    self.assertFalse(observed.known_discrepancies, observed.known_discrepancies)
                    self.assertEqual(terminal["observed_action_records"], terminal["planned_actions"])
                    self.assertFalse(terminal["infrastructure"], terminal["infrastructure"])
                    for facet in observed.facets:
                        expected = "unavailable" if facet.selector in planned["expected_unavailable_facets"] else "passed"
                        self.assertEqual(facet.state, expected, (facet, observed.unavailable))
                else:
                    # Real missing/early-dead worker evidence must remain
                    # unavailable. No fabricated process row is supplied.
                    self.assertTrue(observed.unavailable)
                    self.assertTrue(terminal["infrastructure"])
                    self.assertLess(terminal["observed_action_records"], terminal["planned_actions"])
                    self.assertFalse(observed.known_discrepancies)
                    self.assertTrue(any(facet.state == "unavailable" for facet in observed.facets))
                self.assertEqual(execution.capture_git_source(store, commit), (tree, files))
                outcome.update(status="qualified-with-declared-limits", facets=[asdict(facet) for facet in observed.facets])
        except BaseException as error:
            outcome.update(status="failed-not-qualified", failure_type=type(error).__name__, failure=str(error)[:1024])
            raise
        finally:
            write_new(root / "control-outcome.json", outcome)


@_DOCKER
class WorkerDaemonContentionV1DockerTests(_WorkerPhysical, unittest.TestCase):
    CASE_INDEX = 0
    def test_real_daemon_competitor_sigkill_and_new_owner(self):
        self.run_history()


@_DOCKER
class WorkerBeforeCommitCrashV1DockerTests(_WorkerPhysical, unittest.TestCase):
    CASE_INDEX = 1
    def test_real_beforecommit_rendezvous_kill_and_resume_with_explicit_phase_limit(self):
        self.run_history()


@_DOCKER
class WorkerAfterCommitReceiptV1DockerTests(_WorkerPhysical, unittest.TestCase):
    CASE_INDEX = 2
    def test_real_durable_receipt_beforeoutput_kill_and_historical_replay(self):
        self.run_history()


@_DOCKER
class WorkerOldClaimEpochV1DockerTests(_WorkerPhysical, unittest.TestCase):
    CASE_INDEX = 3
    def test_real_paused_worker_cancel_retry_and_stale_epoch_with_explicit_claim_limit(self):
        self.run_history()


@_DOCKER
class WorkerMissingHookV1DockerTests(_WorkerPhysical, unittest.TestCase):
    VARIANT = "missing_hook"
    def test_actual_live_worker_without_hook_is_unavailable_and_cleaned(self):
        self.run_history()


@_DOCKER
class WorkerEarlyExitV1DockerTests(_WorkerPhysical, unittest.TestCase):
    VARIANT = "early_exit"
    def test_actual_early_worker_exit_is_unavailable_and_cleaned(self):
        self.run_history()
