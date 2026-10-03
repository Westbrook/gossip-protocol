"""Cheap controller contracts; no Docker, provider, or candidate execution."""
from collections import Counter
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from gossip_harness.ledger import Lease
from gossip_harness.library_project_fixture_v1 import IMMUTABLE_PATHS, seed_files
from gossip_harness.library_m1_reference_v1 import m1_files, package_changes
from gossip_harness.peer_library_project_v3 import (
    BUILDERS, PACKAGES, ROLES, ProjectConfig, RehearsalError, ScriptedTransport, action_plan,
    call_limit, canonical_payload, exact_scopes, instructions, package_for, selector_artifact,
    textual_conflict_probe, validate_accounting, work_key,
)
from gossip_harness.peer_project_contract_v2 import (
    ActionRequest, CandidateOffer, Context, DispatchBinding, EvidenceRef,
    MAX_RECORD_BYTES, encode, identity, to_dict,
)
from gossip_harness.peer_project_views_v3 import materialize_project
from gossip_harness.peer_role_loop_v2 import WorkDirective
from gossip_harness.worker import OpenAIWorker


class MemoryMesh:
    """Synthetic transport for shape/size checks only, never release evidence."""
    def __init__(self):
        self.refs = []
        self.payloads = {}

    def put(self, producer, kind, raw):
        ref = EvidenceRef(hashlib.sha256(str(len(self.refs)).encode()).hexdigest(), producer, kind, hashlib.sha256(raw).hexdigest())
        self.refs.append(ref)
        self.payloads[ref] = raw
        return ref

    def arrived(self):
        return tuple(self.refs)

    def want(self, ref):
        return ref in self.refs

    def resolve(self, ref):
        return self.payloads.get(ref)


def synthetic_frontier():
    """Real complete package text with explicitly synthetic dispatch metadata."""
    mesh = MemoryMesh()
    context = Context("a" * 64, "fixture", "size-only", 1, "b" * 64)
    policy = "c" * 64
    seed = seed_files()
    complete = m1_files()
    scopes = exact_scopes()
    source = mesh.put("seed", "project-source", canonical_payload({"files": seed, "base_sha": "a" * 40}))
    entries, material_refs, offers = [], [], []
    for index, actor in enumerate(BUILDERS, 1):
        package = package_for(actor)
        work = work_key(actor, "build", 0)
        action = ActionRequest(context, "action-" + actor, "dispatch-" + actor, actor, "build", work, "mini",
            mesh.put(actor, "worker-request", b"synthetic-request"), "d" * 64)
        dispatch = DispatchBinding(action, Lease("task-" + actor, actor, 1, 1000), "e" * 64,
            "f" * 64, "a" * 64, "call-" + actor, "reservation-" + actor, 1)
        offer = CandidateOffer(dispatch, f"{index:040x}", "b" * 64, "c" * 64, "d" * 64,
            mesh.put(actor, "candidate-bundle", b"synthetic-bundle"))
        ref = mesh.put("seed", "candidate-offer", encode(offer))
        contribution = mesh.put("seed", "candidate-contribution", canonical_payload({
            "protocol": "peer-project-contribution-v3", "offer_sha256": identity(offer),
            "commit_oid": offer.commit_oid, "files": {path: complete[path] for path in scopes[package]}}))
        offers.append(offer)
        material_refs.extend((ref, contribution))
        entries.append({"slot": to_dict(work), "actor": actor, "offer_sha256": identity(offer),
                        "offer_ref": to_dict(ref), "contribution_ref": to_dict(contribution)})
    entries.sort(key=lambda entry: (entry["slot"]["package_id"], entry["slot"]["slot_id"], entry["actor"]))
    body = {"protocol": "peer-project-frontier-v3", "context": to_dict(context),
        "eligibility_policy_sha256": policy, "offers": entries, "missing_slots": []}
    frontier = mesh.put("seed", "selection-frontier", canonical_payload(body))
    directive = WorkDirective(context, work_key("R1", "select_source", 0), "strong", "select_source", source,
        (frontier, *material_refs), ("selection.json",), instructions("R1", "select_source", 0))
    return mesh, policy, directive, frontier, body, offers


class PeerLibraryProjectV3Tests(unittest.TestCase):
    def test_bounded_config_rejects_nonfinite_and_wrong_resource_slots(self):
        for value in (True, 0, 239, 1801, float("nan"), float("inf")):
            with self.subTest(value=value), self.assertRaises(RehearsalError):
                ProjectConfig(deadline_seconds=value)
        with self.assertRaises(RehearsalError):
            ProjectConfig(executor_slots=20)
        self.assertEqual(ProjectConfig().deadline_seconds, 1800)

    def test_twenty_roles_have_one_aggregate_twenty_seven_call_plan(self):
        self.assertEqual(len(ROLES), 20)
        self.assertEqual(len(action_plan()), 27)
        self.assertEqual(Counter(actor for actor, _, _ in action_plan()), Counter({a: call_limit(a) for a in ROLES}))
        self.assertEqual(sum(kind == "select_source" for _, kind, _ in action_plan()), 2)
        self.assertEqual(len({work_key(a, k, g) for a, k, g in action_plan()}), 27)

    def test_scope_roots_include_unchanged_owned_files_without_inherited_overlap(self):
        scopes = exact_scopes()
        owned = [path for package in PACKAGES for path in scopes[package]]
        self.assertEqual(len(owned), len(set(owned)))
        self.assertIn("library/catalog/validation.py", scopes["catalog"])
        self.assertIn("library/catalog/__init__.py", scopes["catalog"])
        self.assertIn("library/ingestion/jobs.py", scopes["ingestion"])
        self.assertEqual(set(owned) | set(IMMUTABLE_PATHS), set(m1_files()))
        self.assertFalse(set(owned) & set(IMMUTABLE_PATHS))

    def test_complete_sixteen_offer_request_and_envelope_fit_without_truncation(self):
        mesh, policy, directive, _frontier, _body, _offers = synthetic_frontier()
        request, view = materialize_project(directive, mesh, policy)
        self.assertEqual(request.files, seed_files())
        self.assertEqual(len(view.evidence_refs), 33)
        request_raw = canonical_payload(asdict(request))
        envelope = canonical_payload({"worker_request": asdict(request), "view_manifest_sha256": identity(view)})
        self.assertLess(len(request_raw), MAX_RECORD_BYTES)
        self.assertLess(len(envelope), MAX_RECORD_BYTES)
        recipe = json.loads(request.feedback)
        self.assertEqual(len(recipe["materials"]), 34)
        self.assertEqual(recipe["directive"], directive.to_dict())
        self.assertEqual(sum(item["ref"]["kind"] == "candidate-contribution" for item in recipe["materials"]), 16)
        # Reservation estimation exercises the actual worker input encoder and
        # model context bound without issuing a completion or reading a key.
        worker = OpenAIWorker("offline-size-only", max_output_tokens=16384)
        self.assertGreater(worker.reservation_units(request), 0)

    def test_scripted_selector_picks_named_actor_and_preserves_complete_frontier(self):
        _mesh, policy, _directive, frontier, body, offers = synthetic_frontier()
        selected = selector_artifact(body, frontier.payload_sha256)
        self.assertEqual(selected["eligibility_policy_sha256"], policy)
        self.assertEqual(len(selected["eligible_offer_sha256s"]), 16)
        self.assertEqual(tuple(item["package_id"] for item in selected["selected"]), PACKAGES)
        selected_ids = {item["offer_sha256"] for item in selected["selected"]}
        self.assertEqual({offer.dispatch.action.actor for offer in offers if identity(offer) in selected_ids},
                         {"B01", "B05", "B09", "B13"})

    def test_selector_rejects_missing_package_instead_of_inventing_offer(self):
        _mesh, _policy, _directive, frontier, body, _offers = synthetic_frontier()
        body["offers"] = [item for item in body["offers"] if item["slot"]["package_id"] != "clients"]
        with self.assertRaises(RehearsalError):
            selector_artifact(body, frontier.payload_sha256)

    def test_final_accounting_rejects_count_only_success(self):
        calls = [{"actor": a, "kind": k, "generation": g, "finished_at": 1} for a, k, g in action_plan()]
        with self.assertRaises(RehearsalError):
            validate_accounting({"calls": calls, "active": 0}, {}, [])

    def test_actual_worker_encoder_and_parser_use_only_scripted_transport(self):
        mesh = MemoryMesh()
        context = Context("a" * 64, "fixture", "worker-shape", 1, "b" * 64)
        source = mesh.put("seed", "project-source", canonical_payload({"files": seed_files(), "base_sha": "a" * 40}))
        directive = WorkDirective(context, work_key("B01", "build", 0), "mini", "build", source, (),
            exact_scopes()["catalog"], instructions("B01", "build", 0))
        request, _ = materialize_project(directive, mesh, "c" * 64)
        changes = package_changes("catalog", defective_blob_dedup=True)
        transport = ScriptedTransport()
        transport.register(directive, changes, "B01")
        worker = OpenAIWorker("offline-scripted-test", max_output_tokens=16384, transport=transport)
        with patch("gossip_harness.peer_library_project_v3.time.sleep"):
            result = worker.run(request)
        self.assertEqual(result.changes, changes)
        self.assertEqual(len(transport.snapshot()["calls"]), 1)
        self.assertEqual(transport.snapshot()["active"], 0)


class PeerLibraryProjectV3GitTests(unittest.TestCase):
    def test_actual_textual_conflict_preserves_accepted_head(self):
        with tempfile.TemporaryDirectory() as temporary:
            result = textual_conflict_probe(Path(temporary))
        self.assertEqual(result["candidate"]["status"], "text_conflict")
        self.assertTrue(result["head_unchanged"])
        self.assertFalse(result["candidate_execution"])


if __name__ == "__main__":
    unittest.main()
