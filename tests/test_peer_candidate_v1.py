"""Offline Git boundary tests; candidate Python is never executed."""
from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from gossip_harness.gitstore import GitStore
from gossip_harness.peer_candidate_v1 import CandidateError, CandidatePublisher, _execution_identity
from gossip_harness.peer_coding_dispatch_v1 import canonical_payload, source_digest
from gossip_harness.peer_coding_runtime_v1 import CodingPeer
from gossip_harness.peer_payload_runtime_v1 import PayloadPeer
from gossip_harness.peer_store_v1 import canonical_bytes


TOKEN = "candidate-mesh-offline-credential-0001"
KEY = "candidate-principal-offline-key-00001"


def digest(value):
    return hashlib.sha256(canonical_payload(value)).hexdigest()


class PeerCandidateTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.peers = []
        self.case_number = 0

    def tearDown(self):
        for peer in self.peers:
            peer.owner.close()
        self.directory.cleanup()

    def case(self, *, seed_files=None, changes=None):
        self.case_number += 1
        root = self.root / str(self.case_number)
        root.mkdir()
        files = {"solution.py": "def answer():\n    return 'initial'\n", "README.md": "Keep this file.\n"}
        store = GitStore.create(root / "worker" / "repository.git", files)
        seed = {"task_id": "candidate-task", "instructions": "Change the answer.", "allowed_paths": ["solution.py"],
                "files": files if seed_files is None else seed_files, "base_sha": store.head(), "attempt": 1, "feedback": ""}
        config = {"run_id": "candidate-offline", "task_id": "candidate-task", "profile_id": "fixture-mini",
                  "seed_producer": "origin", "seed_sha256": digest(seed), "service_producer": "service",
                  "authority_sha256": "a" * 64, "dispatch_config_sha256": "b" * 64}
        result = {"kind": "result", "payload": {"changes": {"solution.py": "def answer():\n    return 'café'\n"}
                                                     if changes is None else changes,
                                                 "summary": "Offline fixture", "usage_units": 1, "metadata": {}}}
        case = SimpleNamespace(root=root, store=store, seed=seed, config=config, result=result, calls=[])

        def call(port, principal, key, request_id, operation, payload, **kwargs):
            case.calls.append(operation)
            if operation == "claim":
                return {"run_id": config["run_id"], "config_sha256": config["authority_sha256"], "status": "ok",
                        "authority_time": 1.0, "lease": {"task_id": config["task_id"], "worker_id": "worker",
                                                          "epoch": 1, "expires_at": 31.0}}
            intent = case.worker.record["intent"]
            return {"run_id": config["run_id"], "config_sha256": config["authority_sha256"],
                    "dispatch_config_sha256": config["dispatch_config_sha256"], "status": "completed",
                    "dispatch_protocol": "peer-coding-dispatch-v1", "principal": "worker", "request_id": request_id,
                    "task_id": config["task_id"], "profile_id": config["profile_id"], "action_id": intent["action_id"],
                    "epoch": 1, "payload_sha256": intent["request_sha256"],
                    "worker_request_sha256": intent["worker_request_sha256"], "profile_sha256": "c" * 64,
                    "reserved_units": 100, "usage_units": 1, "reservation_id": "reserved", "call_id": "call",
                    "result_sha256": digest(result)}

        case.call = call
        case.worker = self.worker(case)
        for node in ("origin", "service"):
            peer = PayloadPeer(root / node, node, "gossip", TOKEN)
            self.peers.append(peer)
            setattr(case, node, peer)
        return case

    def worker(self, case):
        worker = CodingPeer(case.root / "worker", "worker", "gossip", TOKEN, coding=case.config,
                            authority={"port": 12345, "principal": "worker", "key": KEY}, authority_call=case.call)
        self.peers.append(worker)
        return worker

    def restart(self, case):
        case.worker.owner.close()
        self.peers.remove(case.worker)
        case.worker = self.worker(case)

    def publisher(self, case, **kwargs):
        return CandidatePublisher(case.worker.root / "candidate", case.worker, case.store,
                                  baseline_sha=case.seed["base_sha"], allowed_paths=("solution.py",), **kwargs)

    @staticmethod
    def transfer(sender, receiver, sha, *, chunks=True):
        receiver.store.merge(sender.store.export([], limit=8))
        receiver.arrived_payloads()
        if chunks:
            for index in receiver.payloads.missing(sha):
                receiver.payloads.accept_chunk(sha, index, sender.payloads.read_chunk(sha, index))

    def complete(self, case):
        case.origin.publish_payload(canonical_payload(case.seed), "application/json", "seed")
        self.transfer(case.origin, case.worker, digest(case.seed))
        case.worker.coding_tick()
        case.service.publish_payload(canonical_payload(case.result), "application/json", "result")
        self.transfer(case.service, case.worker, digest(case.result))
        case.worker.coding_tick()
        self.assertEqual(case.worker.record["phase"], "published")

    def test_requires_locally_arrived_result_notice_and_complete_bytes(self):
        case = self.case()
        publisher = self.publisher(case)
        self.assertIsNone(publisher.tick())
        case.origin.publish_payload(canonical_payload(case.seed), "application/json", "seed")
        self.transfer(case.origin, case.worker, digest(case.seed))
        case.worker.coding_tick()
        case.service.publish_payload(canonical_payload(case.result), "application/json", "result")
        self.transfer(case.service, case.worker, digest(case.result), chunks=False)
        case.worker.coding_tick()
        self.assertIsNone(publisher.tick())
        self.assertEqual(case.store._git("for-each-ref", "refs/harness/proposals"), "")
        self.transfer(case.service, case.worker, digest(case.result))
        case.worker.coding_tick()
        self.assertEqual(publisher.tick()["phase"], "published")
        self.assertEqual(case.calls, ["claim", "coding_dispatch"])

    def test_full_request_tree_must_match_base_including_unchanged_files(self):
        case = self.case(seed_files={"solution.py": "def answer():\n    return 'initial'\n"})
        self.complete(case)
        publisher = self.publisher(case)
        with self.assertRaisesRegex(CandidateError, "exact Git base tree"):
            publisher.tick()
        self.assertIsNone(publisher.state())
        self.assertFalse(publisher.inputs_path.exists())
        self.assertEqual(case.store.head(), case.seed["base_sha"])

    def test_out_of_scope_result_cannot_create_proposal(self):
        case = self.case(changes={"README.md": "Overwritten outside the task scope."})
        self.complete(case)
        publisher = self.publisher(case)
        with self.assertRaisesRegex(CandidateError, "task-owned paths"):
            publisher.tick()
        self.assertEqual(case.store._git("for-each-ref", "refs/harness/proposals"), "")
        self.assertFalse(publisher.inputs_path.exists())

    def test_exact_bundle_offer_and_unicode_digest_leave_accepted_unchanged(self):
        case = self.case()
        self.complete(case)
        publisher = self.publisher(case)
        state = publisher.tick()
        expected = {**case.seed["files"], **case.result["payload"]["changes"]}
        self.assertEqual(case.store.read_files(state["offered_sha"]), expected)
        self.assertEqual(case.store.head(), case.seed["base_sha"])
        self.assertTrue(case.store.is_ancestor(case.seed["base_sha"], state["offered_sha"]))
        bundle_record = json.loads(publisher.bundle_path.read_bytes())
        bundle = base64.b64decode(bundle_record["payload_base64"])
        manifest = state["bundle_manifest"]
        self.assertEqual(hashlib.sha256(bundle).hexdigest(), manifest["bundle_sha256"])
        self.assertEqual(case.worker.payloads.read(manifest["bundle_sha256"]), bundle)
        events = {e["event_id"]: e for e in case.worker.store.state()["events"]}
        event = events[state["offer_event_id"]]
        offer = case.worker.store.artifact(event["artifact_sha256"])
        self.assertEqual(event["producer"], "worker")
        self.assertEqual(offer["bundle_manifest"], manifest)
        self.assertEqual(offer["request_id"], case.worker.record["response"]["request_id"])
        self.assertEqual(offer["generation"], 0)
        self.assertNotIn("repository", offer)
        self.assertEqual(state["intent"]["peer_candidate_source_sha256"], source_digest(expected))
        blackbox_canonical = json.dumps(expected, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
        self.assertNotEqual(source_digest(expected), hashlib.sha256(blackbox_canonical).hexdigest())
        snapshot = publisher.state()
        snapshot["intent"]["epoch"] = 500
        self.assertEqual(publisher.state()["intent"]["epoch"], 1)

    def test_restart_each_durable_boundary_preserves_one_proposal_and_offer(self):
        for boundary in ("after_intent", "after_proposal", "after_bundle", "after_publish"):
            with self.subTest(boundary=boundary):
                case = self.case()
                self.complete(case)

                def crash(point):
                    if point == boundary:
                        raise RuntimeError("injected boundary stop")

                publisher = self.publisher(case, crash_hook=crash)
                with self.assertRaisesRegex(RuntimeError, "injected"):
                    publisher.tick()
                before = publisher.state()
                self.restart(case)
                resumed = self.publisher(case)
                if boundary in {"after_bundle", "after_publish"}:
                    with patch("gossip_harness.peer_candidate_v1.export_bundle", side_effect=AssertionError("must reuse bytes")):
                        after = resumed.tick()
                else:
                    after = resumed.tick()
                self.assertEqual(after["intent"], before["intent"])
                self.assertEqual(after["phase"], "published")
                self.assertEqual(len(case.store._git("for-each-ref", "--format=%(objectname)", "refs/harness/proposals").splitlines()), 1)
                offers = [e for e in case.worker.store.state()["events"] if e["kind"] == "candidate_offer"]
                self.assertEqual([e["event_id"] for e in offers], [after["offer_event_id"]])
                self.assertEqual(case.calls, ["claim", "coding_dispatch"])
                self.assertEqual(case.store.head(), case.seed["base_sha"])

    def test_git_and_bundle_side_effects_before_state_write_are_recoverable(self):
        for interrupted_phase in ("proposal", "bundle"):
            with self.subTest(phase=interrupted_phase):
                case = self.case()
                self.complete(case)
                publisher = self.publisher(case)
                persist = publisher._persist

                def stop(record):
                    if record is not None and record["phase"] == interrupted_phase:
                        raise OSError("injected state write failure")
                    persist(record)

                with patch.object(publisher, "_persist", side_effect=stop):
                    with self.assertRaisesRegex(OSError, "state write"):
                        publisher.tick()
                self.restart(case)
                resumed = self.publisher(case)
                if interrupted_phase == "bundle":
                    retained = publisher.bundle_path.read_bytes()
                    with patch("gossip_harness.peer_candidate_v1.export_bundle", side_effect=AssertionError("must reuse bytes")):
                        resumed.tick()
                    self.assertEqual(resumed.bundle_path.read_bytes(), retained)
                else:
                    resumed.tick()
                self.assertEqual(len(case.store._git("for-each-ref", "--format=%(objectname)", "refs/harness/proposals").splitlines()), 1)

    def test_reopen_rejects_corrupt_inputs_bundle_and_self_consistent_extra_intent(self):
        for corruption in ("input", "bundle", "intent", "other_proposal", "other_parent"):
            with self.subTest(corruption=corruption):
                case = self.case()
                self.complete(case)

                def stop(point):
                    if point == "after_proposal":
                        raise RuntimeError("injected proposal boundary")

                publisher = self.publisher(case, crash_hook=stop if corruption.startswith("other_") else None)
                if corruption.startswith("other_"):
                    with self.assertRaisesRegex(RuntimeError, "proposal boundary"):
                        publisher.tick()
                else:
                    publisher.tick()
                if corruption == "input":
                    value = json.loads(publisher.inputs_path.read_bytes())
                    value["candidate_files"]["solution.py"] = "unrecorded source"
                    publisher.inputs_path.write_bytes(canonical_bytes(value))
                elif corruption == "bundle":
                    value = json.loads(publisher.bundle_path.read_bytes())
                    value["payload_base64"] = base64.b64encode(b"different bundle").decode()
                    publisher.bundle_path.write_bytes(canonical_bytes(value))
                elif corruption == "intent":
                    value = json.loads(publisher.state_path.read_bytes())
                    intent = value["data"]["record"]["intent"]
                    intent["extra_authority"] = True
                    intent["proposal_id"] = digest({k: v for k, v in intent.items() if k != "proposal_id"})
                    value["sha256"] = digest(value["data"])
                    publisher.state_path.write_bytes(canonical_bytes(value))
                else:
                    value = json.loads(publisher.state_path.read_bytes())
                    record = value["data"]["record"]
                    if corruption == "other_parent":
                        # Same visible source and message, but another direct parent.
                        base = record["offered_sha"]
                        changes = {}
                    else:
                        base = case.seed["base_sha"]
                        changes = {"solution.py": "Different source under the original action."}
                    record["offered_sha"] = case.store.propose(changes, base_sha=base,
                                                               message="Peer candidate " + record["intent"]["proposal_id"])
                    value["sha256"] = digest(value["data"])
                    publisher.state_path.write_bytes(canonical_bytes(value))
                self.restart(case)
                with self.assertRaises(ValueError):
                    self.publisher(case)

    def test_repeated_valid_notices_do_not_replace_bound_prerequisites(self):
        case = self.case()
        self.complete(case)
        seed_id = case.worker.record["intent"]["seed_event_id"]
        case.origin.publish_payload(canonical_payload(case.seed), "application/json", "same-seed-again")
        case.service.publish_payload(canonical_payload(case.result), "application/json", "same-result-again")
        self.transfer(case.origin, case.worker, digest(case.seed))
        self.transfer(case.service, case.worker, digest(case.result))
        publisher = self.publisher(case)
        state = publisher.tick()
        self.assertEqual(state["phase"], "published")
        self.assertIn(seed_id, state["intent"]["visible_event_ids"])

    def test_exclusive_adapter_and_private_journal_identity(self):
        case = self.case()
        with self.assertRaisesRegex(CandidateError, "exclusively owned"):
            CandidatePublisher(case.root / "outside-peer", case.worker, case.store,
                               baseline_sha=case.seed["base_sha"], allowed_paths=("solution.py",))
        publisher = self.publisher(case)
        with self.assertRaisesRegex(CandidateError, "already owns"):
            self.publisher(case)
        self.complete(case)
        publisher.tick()
        self.restart(case)
        with self.assertRaisesRegex(CandidateError, "identity changed"):
            CandidatePublisher(case.worker.root / "candidate", case.worker, case.store,
                               baseline_sha=case.seed["base_sha"], allowed_paths=("solution.py", "README.md"))
        for changed in ("sources", "git", "python", "git_selectors_sha256"):
            with self.subTest(execution=changed):
                identity = _execution_identity()
                if changed == "sources":
                    identity["sources"]["peer_candidate_v1.py"] = "0" * 64
                elif changed == "git_selectors_sha256":
                    identity[changed] = "0" * 64
                else:
                    identity[changed]["sha256"] = "0" * 64
                with patch("gossip_harness.peer_candidate_v1._execution_identity", return_value=identity):
                    with self.assertRaisesRegex(CandidateError, "identity changed"):
                        self.publisher(case)


if __name__ == "__main__":
    unittest.main()
