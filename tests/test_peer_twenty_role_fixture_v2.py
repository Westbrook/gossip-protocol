"""Cheap qualification contract tests; no TCP listeners or child processes."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict, replace
import hashlib
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from gossip_harness.peer_twenty_role_fixture_v2 import (
    BUILDERS, REVIEWERS, ROLES, ROSTER, FixtureConfig, FixtureError,
    execution_contract, fixture_seed, package_for, role_path, run_fixture,
    validate_observations, verify_candidate_source, attest_execution, qualify_before_deadline, _wait, _write,
)

from gossip_harness.ledger import Lease
from gossip_harness.peer_coding_dispatch_v1 import source_digest
from gossip_harness.peer_financial_authority_v2 import canonical_payload
from gossip_harness.peer_project_contract_v2 import (
    ActionRequest, CandidateOffer, Context, DispatchBinding, DispatchReply, EvidenceRef,
    WorkKey, identity, worker_request_digest,
)
from gossip_harness.worker import WorkerRequest, WorkerResult


def candidate_fixture():
    request = WorkerRequest("task", "Fixture patch", ("catalog/part.py",), fixture_seed(), "1" * 40, 1)
    result = WorkerResult({"catalog/part.py": "VALUE = 'B01'\n"}, "Fixture", 1, {})
    result_sha = hashlib.sha256(canonical_payload({"kind": "result", "payload": asdict(result)})).hexdigest()
    action = ActionRequest(Context("a" * 64, "cohort", "trajectory", 0, "b" * 64), "action", "request", "B01",
                           "build", WorkKey("catalog", "fixture", "B01", 0), "mini",
                           EvidenceRef("c" * 64, "B01", "worker-request", "d" * 64), "e" * 64)
    binding = DispatchBinding(action, Lease("task", "B01", 1, 100), worker_request_digest(request),
                              "f" * 64, "1" * 64, "call", "reservation", 10)
    reply = DispatchReply("request", identity(action), "completed", "", binding, result_sha, 1)
    files = {**request.files, **result.changes}
    offer = CandidateOffer(binding, "2" * 40, source_digest(files), result_sha, "3" * 64,
                           EvidenceRef("4" * 64, "B01", "candidate-bundle", "5" * 64))
    return offer, request, result, reply, files


def observations():
    roles = []
    calls = []
    for index, actor in enumerate(ROLES):
        held = index < 4
        roles.append({"actor": actor, "pid": 100 + index, "root": "/fixture/" + actor,
                      "mesh_root": "/fixture/mesh/" + actor, "journal_root": "/fixture/journal/" + actor,
                      "git_path": "/fixture/git/" + actor if actor in BUILDERS else None,
                      "source_materialized": True, "readonly_evidence_materialized": True,
                      "dropped_acknowledgment": True, "exact_terminal_replay": True,
                      "reply": {"state": "completed"}, "view": {"omitted_required_ids": []},
                      "mesh_summary": {"fatal_error": None},
                      "probe": {"arrived_at": 2, "mesh_ticks": 10, "role_state": "pending"} if held else None})
        calls.append({"actor": actor, "task_id": "task-" + actor, "materialized": True,
                      "held": held, "started_at": 1, "finished_at": 3})
    candidates = [{"actor": actor, "verified": True} for actor in BUILDERS]
    return roles, {"calls": calls, "peak": 4, "active": 0}, candidates


class PeerTwentyRoleFixtureV2Tests(unittest.TestCase):
    def test_frozen_roster_and_disjoint_exact_package_paths(self):
        self.assertEqual((len(BUILDERS), len(REVIEWERS), len(ROSTER)), (16, 4, 22))
        self.assertEqual(len(set(ROSTER)), 22)
        for package in ("catalog", "ingestion", "query", "clients"):
            self.assertEqual(sum(package_for(actor) == package for actor in BUILDERS), 4)
        self.assertEqual({role_path(actor) for actor in BUILDERS}, set(fixture_seed()) - {"README.txt"})
        self.assertTrue(all(role_path(actor) == "review.json" for actor in REVIEWERS))
        with self.assertRaises(FixtureError):
            package_for("observer")

    def test_explicit_resource_bounds_reject_invalid_values(self):
        for options in ({"deadline_seconds": float("nan")}, {"deadline_seconds": float("inf")},
                        {"deadline_seconds": 1}, {"deadline_seconds": True}, {"executor_slots": True},
                        {"executor_slots": 20}, {"interval": 0.01}):
            with self.subTest(options=options), self.assertRaises(FixtureError):
                FixtureConfig(**options)

    def test_contract_binds_actual_sources_and_excludes_quality_claims(self):
        contract = execution_contract(FixtureConfig())
        self.assertEqual(contract["roles"], list(ROLES))
        self.assertIn("peer_twenty_role_fixture_v2.py", contract["sources"])
        self.assertIn("peer_mesh_finance_v2.py", contract["sources"])
        self.assertTrue(all(len(value) == 64 for value in contract["sources"].values()))
        self.assertIn("statistical-superiority", contract["claims_excluded"])
        self.assertIn("scope-approval", contract["claims_excluded"])
        self.assertFalse(contract["candidate_execution"])

    def test_passing_observations_report_only_plumbing_counts(self):
        result = validate_observations(*observations())
        self.assertEqual(result["role_processes"], 20)
        self.assertEqual(result["verified_candidate_bundles"], 16)
        self.assertEqual(result["gossip_during_wait_observations"], 4)
        self.assertEqual(result["api_calls"], 0)
        self.assertEqual(result["protected_releases"], 0)

    def test_duplicate_pid_or_shared_state_cannot_prove_twenty_independent_roles(self):
        for field in ("pid", "root", "mesh_root", "journal_root", "git_path"):
            roles, calls, candidates = observations()
            roles[1][field] = roles[0][field]
            with self.subTest(field=field), self.assertRaises(FixtureError):
                validate_observations(roles, calls, candidates)

    def test_missing_materialization_replay_or_completion_rejects_receipt(self):
        for field in ("source_materialized", "readonly_evidence_materialized",
                      "dropped_acknowledgment", "exact_terminal_replay"):
            roles, calls, candidates = observations()
            roles[0][field] = False
            with self.subTest(field=field), self.assertRaises(FixtureError):
                validate_observations(roles, calls, candidates)
        for change in ({"reply": {"state": "unknown"}}, {"view": {"omitted_required_ids": ["missing"]}},
                       {"mesh_summary": {"fatal_error": "lost store"}}):
            roles, calls, candidates = observations()
            roles[0].update(change)
            with self.subTest(change=change), self.assertRaises(FixtureError):
                validate_observations(roles, calls, candidates)

    def test_extra_or_duplicate_calls_reject_no_reinvocation_claim(self):
        for duplicate in (False, True):
            roles, calls, candidates = observations()
            if duplicate:
                calls["calls"][1] = deepcopy(calls["calls"][0])
            else:
                calls["calls"].append(deepcopy(calls["calls"][0]))
            with self.subTest(duplicate=duplicate), self.assertRaises(FixtureError):
                validate_observations(roles, calls, candidates)

    def test_peak_without_bounded_overlap_does_not_pass(self):
        for peak in (1, 5):
            roles, calls, candidates = observations()
            calls["peak"] = peak
            with self.subTest(peak=peak), self.assertRaises(FixtureError):
                validate_observations(roles, calls, candidates)

    def test_probe_must_physically_arrive_during_wait_with_transport_ticks(self):
        for change in ({"arrived_at": 0}, {"arrived_at": 4}, {"mesh_ticks": 0}, {"role_state": "published"}):
            roles, calls, candidates = observations()
            roles[0]["probe"].update(change)
            with self.subTest(change=change), self.assertRaises(FixtureError):
                validate_observations(roles, calls, candidates)

    def test_missing_bundle_verification_rejects_publication_claim(self):
        roles, calls, candidates = observations()
        candidates[0]["verified"] = False
        with self.assertRaises(FixtureError):
            validate_observations(roles, calls, candidates)

    def test_existing_output_is_never_reused_or_deleted(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory).resolve() / "retained-failure"
            output.mkdir()
            marker = output / "failure.json"
            marker.write_text("retained")
            with patch("gossip_harness.peer_twenty_role_fixture_v2.MeshNode") as node:
                with self.assertRaises(FixtureError):
                    run_fixture(output)
                node.assert_not_called()
            self.assertEqual(marker.read_text(), "retained")

    def test_receiver_binds_imported_bytes_and_result_to_financial_completion(self):
        offer, request, result, reply, files = candidate_fixture()
        verify_candidate_source(offer, request, result, reply, files, request.files, request.base_sha, True)
        cases = [
            (replace(offer, result_payload_sha256="9" * 64), files, request.files, request.base_sha, True),
            (replace(offer, source_sha256=source_digest(request.files)), request.files,
             request.files, request.base_sha, True),
            (offer, files, {**request.files, "readonly.txt": "substitution"}, request.base_sha, True),
            (offer, files, request.files, "9" * 40, True),
            (offer, files, request.files, request.base_sha, False),
        ]
        for altered_offer, imported, base_files, base_sha, ancestor in cases:
            with self.subTest(base_sha=base_sha, ancestor=ancestor), self.assertRaises(FixtureError):
                verify_candidate_source(altered_offer, request, result, reply, imported, base_files, base_sha, ancestor)

    def test_checkpoint_publication_is_atomic_and_never_overwrites(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ready.json"
            original_link = os.link
            observed = []
            def link(source, target):
                self.assertFalse(path.exists())
                self.assertEqual(Path(source).read_bytes(), b'{"ready":true}')
                observed.append(True)
                return original_link(source, target)
            with patch("gossip_harness.peer_twenty_role_fixture_v2.os.link", side_effect=link):
                _write(path, {"ready": True})
            self.assertEqual(observed, [True])
            with self.assertRaises(FileExistsError):
                _write(path, {"ready": False})
            self.assertEqual(path.read_bytes(), b'{"ready":true}')
            self.assertEqual(len(list(path.parent.glob("ready.json.pending-*"))), 1)

    def test_execution_attestation_rejects_source_and_runtime_drift(self):
        with patch("gossip_harness.peer_twenty_role_fixture_v2.source_fingerprints", return_value={"module": "a"}), \
                patch("gossip_harness.peer_twenty_role_fixture_v2.runtime_identity", return_value={"python": "pinned"}):
            self.assertEqual(attest_execution({"module": "a"}, {"python": "pinned"})["sources"], {"module": "a"})
            with self.assertRaises(FixtureError):
                attest_execution({"module": "changed"}, {"python": "pinned"})
            with self.assertRaises(FixtureError):
                attest_execution({"module": "a"}, {"python": "changed"})

    def test_wait_deadline_applies_even_when_predicate_is_already_true(self):
        with patch("gossip_harness.peer_twenty_role_fixture_v2.time.monotonic", return_value=2):
            with self.assertRaises(FixtureError):
                _wait(lambda: True, 1, {})
        with patch("gossip_harness.peer_twenty_role_fixture_v2.time.monotonic", side_effect=[0, 2]):
            with self.assertRaises(FixtureError):
                _wait(lambda: True, 1, {})

    def test_final_qualification_checks_clock_after_all_observation_assertions(self):
        with patch("gossip_harness.peer_twenty_role_fixture_v2.time.monotonic", return_value=10):
            self.assertEqual(qualify_before_deadline(*observations(), deadline=11)["role_processes"], 20)
            for deadline in (9, 10):
                with self.subTest(deadline=deadline), self.assertRaises(FixtureError):
                    qualify_before_deadline(*observations(), deadline=deadline)
        ordered = []
        def validate(*args):
            ordered.append("assertions")
            return {"role_processes": 20}
        def clock():
            self.assertEqual(ordered, ["assertions"])
            return 11
        with patch("gossip_harness.peer_twenty_role_fixture_v2.validate_observations", side_effect=validate), \
                patch("gossip_harness.peer_twenty_role_fixture_v2.time.monotonic", side_effect=clock):
            with self.assertRaises(FixtureError):
                qualify_before_deadline(*observations(), deadline=10)
