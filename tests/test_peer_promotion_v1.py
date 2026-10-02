"""Received candidate tests: offline direct Git controls and opted-in Docker.

Direct fixtures explicitly copy producer events and chunks, without networking;
the separate project-runtime process tests qualify actual network delivery.
"""
from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import threading
import unittest
from unittest.mock import patch
import uuid

from gossip_harness import peer_promotion_v1 as module
from gossip_harness.blackbox_validator import BlackboxValidator
from gossip_harness.gitstore import GitStore
from gossip_harness.peer_authority_v1 import Authority, _signed
from gossip_harness.peer_coding_dispatch_v1 import CodingDispatch, canonical_payload, source_digest
from gossip_harness.peer_coding_runtime_v1 import MeshPayloads
from gossip_harness.peer_git_bundle_v1 import PROFILE, export_bundle
from gossip_harness.peer_payload_runtime_v1 import PayloadPeer
from gossip_harness.peer_promotion_v1 import ReceivedPromotion, PromotionError, validate_offer
from gossip_harness.pilot import DEFAULT_IMAGE
from gossip_harness.promotion import SimulatedCrash
from gossip_harness.worker import HTTPResponse, MODEL, OpenAIWorker

CODE = "# café\r\ndef solve(payload):\r\n    return payload['x']\r\n"
CASES = [{"id": "unicode-case", "input": {"x": "café"}, "expected": "café"}]
IMAGE = DEFAULT_IMAGE


def metadata_offer():
    manifest = {"protocol": "peer-git-bundle-v1", "profile": PROFILE, "object_format": "sha1",
                "base_sha": "a" * 40, "offered_sha": "b" * 40,
                "offered_ref": "refs/harness/proposals/" + "b" * 40,
                "bundle_sha256": "c" * 64, "bundle_bytes": 100, "object_count": 6,
                "commit_count": 2, "expanded_bytes": 300, "objects_sha256": "d" * 64,
                "tree_sha": "e" * 40, "files_sha256": "f" * 64, "file_count": 1, "source_bytes": 10}
    return {"protocol": "peer-candidate-v1", "generation": 0, "run_id": "run", "task_id": "build",
            "principal": "alpha", "epoch": 1, "action_id": "action", "request_id": "request",
            "profile_id": "mini", "request_sha256": "1" * 64, "result_sha256": "2" * 64,
            "dispatch_config_sha256": "3" * 64, "bundle_sha256": "c" * 64, "bundle_manifest": manifest}


class PromotionMetadataTests(unittest.TestCase):
    def test_producer_and_generation_are_exact(self):
        offer = metadata_offer()
        self.assertEqual(validate_offer(offer, "alpha"), offer)
        with self.assertRaises(PromotionError):
            validate_offer(offer, "beta")
        for generation in (True, 1):
            with self.assertRaises(PromotionError):
                validate_offer({**offer, "generation": generation}, "alpha")

    def test_peer_pass_receipt_is_not_accepted_as_offer_authority(self):
        with self.assertRaises(PromotionError):
            validate_offer({**metadata_offer(), "passed": True}, "alpha")

    def test_bundle_identity_cannot_be_substituted(self):
        with self.assertRaises(PromotionError):
            validate_offer({**metadata_offer(), "bundle_sha256": "9" * 64}, "alpha")

    def test_unicode_evaluator_digest_is_independent_of_peer_wire_digest(self):
        value = {"solution.py": CODE}
        expected = hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=True,
                                             separators=(",", ":")).encode()).hexdigest()
        self.assertEqual(module._evaluator_digest(value), expected)
        self.assertNotEqual(module._digest(value), expected)

    def test_execution_identity_binds_binary_bytes_and_hides_selector_values(self):
        root = Path("runs/peer-promotion-fixtures") / uuid.uuid4().hex
        root.mkdir(parents=True)
        executable = root / "fixture-executable"
        executable.write_bytes(b"first immutable binary")
        with patch.object(module.shutil, "which", return_value=str(executable)), \
             patch.dict(os.environ, {"DOCKER_CONTEXT": "private-selector-sentinel"}):
            first = module._execution_identity()
            executable.write_bytes(b"replacement binary")
            second = module._execution_identity()
            with patch.dict(os.environ, {"DOCKER_CONTEXT": "changed-selector-sentinel"}):
                third = module._execution_identity()
        self.assertNotEqual(first["executables"]["docker"]["sha256"], second["executables"]["docker"]["sha256"])
        self.assertNotEqual(second["environment_sha256"], third["environment_sha256"])
        self.assertNotIn("private-selector-sentinel", json.dumps(first))
        self.assertNotIn("changed-selector-sentinel", json.dumps(third))

    def test_record_integrity_rejects_unacknowledged_edits(self):
        root = Path("runs/peer-promotion-fixtures") / uuid.uuid4().hex
        root.mkdir(parents=True)
        path = root / "receipt.json"
        module._save(path, {"status": "rejected"})
        value = json.loads(path.read_bytes())
        value["record"]["status"] = "accepted"
        path.write_text(json.dumps(value))
        with self.assertRaises(PromotionError):
            module._read(path)


class PromotionFixture:
    def setUp(self):
        self.root = Path("runs/peer-promotion-fixtures").resolve() / uuid.uuid4().hex
        self.root.mkdir(parents=True)
        self.clock_value = 100.0
        self.bridge = None
        self.peer = PayloadPeer(self.root / "service", "service", "gossip", "p" * 32)
        self.sender = PayloadPeer(self.root / "sender", "alpha", "gossip", "p" * 32)
        self.authority = None
        self.addCleanup(self.cleanup_fixture)
        files = {"solution.py": "def solve(payload):\n    return None\n", "context.txt": "baseline\n"}
        self.origin = GitStore.create(self.root / "origin.git", files)
        self.base = self.origin.head()
        self.target = GitStore.fork(self.origin, self.root / "target.git")
        other = self.target.propose({"context.txt": "independent receiver work\n"})
        prepared = self.target.prepare(self.target, other, self.base, lambda path: (True, "trusted fixture setup"))
        self.target.accept(prepared)
        self.before = self.target.head()
        config = {"protocol": "peer-authority-v1", "run_id": "promotion-test", "budget_units": 1_000_000,
                  "lease_seconds": 300, "principals": {"alpha": {"key": "a" * 32, "tasks": ["build"]}},
                  "tasks": {"build": {"fixture": "text_build", "reservation_units": 1}}}
        self.authority = Authority(self.root / "authority", config, clock=lambda: self.clock_value)
        self.provider_calls = 0

        def offline(request, timeout, maximum):
            self.provider_calls += 1
            proposal = {"changes": [{"path": "solution.py", "content": CODE}], "summary": "Return requested value."}
            response = {"id": "resp_offline", "model": MODEL, "status": "completed", "service_tier": "default",
                        "usage": {"input_tokens": 101, "output_tokens": 20, "total_tokens": 121},
                        "output": [{"type": "message", "role": "assistant", "status": "completed",
                                    "content": [{"type": "output_text", "text": json.dumps(proposal)}]}]}
            return HTTPResponse(200, {"X-Request-Id": "req_offline"}, json.dumps(response).encode())

        worker = OpenAIWorker("offline-fixture-key", max_output_tokens=256, timeout=2, transport=offline)
        self.coding = CodingDispatch(self.authority, MeshPayloads(self.peer), {"mini": worker},
                                     {"build": {"allowed_paths": ["solution.py"], "profiles": ["mini"]}},
                                     transport_identity="promotion-offline-response-v1")
        claim = {"protocol": "peer-authority-v1", "run_id": config["run_id"], "principal": "alpha",
                 "request_id": "claim", "operation": "claim", "payload": {"task_id": "build"}}
        claim_receipt = self.authority.handle(_signed(claim, "a" * 32))["body"]["receipt"]
        self.assertEqual(claim_receipt["status"], "ok")
        self.request = {"worker_request": {"task_id": "build", "instructions": "Implement solve returning x.",
                                          "allowed_paths": ["solution.py"], "files": files, "base_sha": self.base,
                                          "attempt": 1, "feedback": ""},
                        "context": {"source_sha256": source_digest(files), "event_ids": []}}
        data = canonical_payload(self.request)
        request_sha = hashlib.sha256(data).hexdigest()
        self.sender.publish_payload(data, "application/json", "request")
        self.deliver()
        dispatch = {**claim, "request_id": "coding", "operation": "coding_dispatch",
                    "payload": {"task_id": "build", "epoch": claim_receipt["lease"]["epoch"],
                                "action_id": "action", "profile_id": "mini", "request_sha256": request_sha}}
        receipt = self.coding.execute(dispatch)
        self.assertEqual(receipt["status"], "completed")
        self.offered = self.origin.propose({"solution.py": CODE}, self.base)
        self.bundle, manifest = export_bundle(self.origin, self.offered, self.base)
        self.offer = {**metadata_offer(), "run_id": config["run_id"], "request_id": "coding",
                      "request_sha256": request_sha, "result_sha256": receipt["result_sha256"],
                      "dispatch_config_sha256": self.coding.config_sha256,
                      "bundle_sha256": manifest["bundle_sha256"], "bundle_manifest": manifest}
        self.sender.publish_payload(self.bundle, "application/x-git-bundle", "bundle")
        self.event = self.sender.store.publish("offer", "candidate_offer", self.offer)
        self.evaluations = []

    def cleanup_fixture(self):
        if self.bridge is not None:
            self.bridge.close()
        if self.authority is not None:
            self.authority.close()
        self.sender.owner.close()
        self.peer.owner.close()

    def deliver(self, *, chunks=True):
        self.peer.store.merge(self.sender.store.export(self.peer.store.inventory()))
        self.peer.arrived_payloads()
        if chunks:
            for sha, descriptor in self.sender.arrived_payloads().items():
                self.peer.payloads.register(descriptor)
                for index in self.peer.payloads.missing(sha):
                    self.peer.payloads.accept_chunk(sha, index, self.sender.payloads.read_chunk(sha, index))

    def open_bridge(self, **kwargs):
        if self.bridge is not None:
            self.bridge.close()
        self.bridge = ReceivedPromotion(self.root / "promotion", peer=self.peer, coding=self.coding,
                                        target=self.target, cases=CASES, image=IMAGE, **kwargs)
        return self.bridge

    def mock_evaluate(self, files, cases, *, passed=True, status=None):
        self.evaluations.append(deepcopy(files))
        return {"schema_version": 1, "protocol": "gossip-blackbox-v1", "passed": passed,
                "status": status or ("passed" if passed else "failed"), "image_id": IMAGE,
                "adapter_sha256": self.bridge.validator._sandbox.checks_sha256,
                "source_sha256": module._evaluator_digest(files), "suite_sha256": module._evaluator_digest(cases),
                "timeout_seconds": self.bridge.validator.timeout_seconds,
                "case_timeout_seconds": self.bridge.validator.case_timeout_seconds,
                "cleanup_verified": True, "container_name": "explicitly-mocked-no-container", "offline_test_stub": True,
                "outcomes": [{"index": i, "passed": passed} for i in range(len(cases))]}

    def perform(self, **kwargs):
        with patch.object(self.bridge.validator, "evaluate", side_effect=self.mock_evaluate):
            return self.bridge.process(self.event["event_id"], **kwargs)


class ReceivedPromotionGitTests(PromotionFixture, unittest.TestCase):
    def test_only_arrived_offer_and_complete_bundle_admit_validation(self):
        self.open_bridge()
        self.assertEqual(self.bridge.process(self.event["event_id"])["reason"], "offer_not_arrived")
        self.deliver(chunks=False)
        self.assertEqual(self.bridge.process(self.event["event_id"])["reason"], "bundle_not_arrived")
        self.assertEqual(self.bridge.state()["attempts"], [])
        self.deliver()
        self.assertEqual(self.perform()["status"], "accepted")

    def test_divergent_merged_tree_preserves_exact_unicode_crlf_and_fences_task(self):
        self.deliver()
        self.open_bridge()
        outcome = self.perform()
        self.assertEqual(outcome["status"], "accepted")
        self.assertEqual(len(self.evaluations), 1)
        self.assertEqual(self.evaluations[0], {"solution.py": CODE, "context.txt": "independent receiver work\n"})
        self.assertNotEqual(self.target.head(), self.offered)
        self.assertEqual(self.target.head(), outcome["candidate"]["candidate_sha"])
        self.assertEqual(self.authority.ledger.task("build")["status"], "complete")
        self.assertFalse(outcome["independent_final_project_acceptance"])
        self.assertEqual(self.bridge.process(self.event["event_id"]), outcome)
        self.assertEqual(self.provider_calls, 1)

    def test_wrong_answer_is_terminal_and_never_retried(self):
        self.deliver()
        self.open_bridge()
        with patch.object(self.bridge.validator, "evaluate", side_effect=lambda f, c: self.mock_evaluate(f, c, passed=False)):
            outcome = self.bridge.process(self.event["event_id"])
        self.assertEqual(outcome["status"], "rejected")
        self.open_bridge()
        with patch.object(self.bridge.validator, "evaluate", side_effect=AssertionError("must not repeat")):
            self.assertEqual(self.bridge.process(self.event["event_id"]), outcome)
        self.assertEqual(self.target.head(), self.before)

    def test_infrastructure_failure_retained_without_retry(self):
        self.deliver()
        self.open_bridge()
        with patch.object(self.bridge.validator, "evaluate", side_effect=lambda f, c: self.mock_evaluate(f, c, passed=False, status="sandbox_error")):
            outcome = self.bridge.process(self.event["event_id"])
        self.assertEqual(outcome["status"], "infrastructure_failed")
        self.assertEqual(outcome["validation"]["receipt"]["status"], "sandbox_error")
        self.assertEqual(self.bridge.process(self.event["event_id"]), outcome)
        self.assertEqual(self.target.head(), self.before)

    def test_authoritative_patch_mismatch_never_reaches_validator(self):
        wrong = self.origin.propose({"solution.py": "def solve(payload): return 'wrong'\n"}, self.base)
        bundle, manifest = export_bundle(self.origin, wrong, self.base)
        self.sender.publish_payload(bundle, "application/x-git-bundle", "wrong-bundle")
        offer = {**self.offer, "bundle_sha256": manifest["bundle_sha256"], "bundle_manifest": manifest}
        event = self.sender.store.publish("wrong-offer", "candidate_offer", offer)
        self.deliver()
        self.open_bridge()
        with patch.object(self.bridge.validator, "evaluate", side_effect=AssertionError("unbound source")):
            result = self.bridge.process(event["event_id"])
        self.assertEqual(result["status"], "rejected")
        self.assertIn("authoritative worker patch", result["detail"])
        self.assertEqual(self.target.head(), self.before)

    def test_saved_validation_can_resume_without_second_evaluation(self):
        self.deliver()
        self.open_bridge()
        with self.assertRaises(SimulatedCrash):
            self.perform(crash_at="after_validation")
        self.assertEqual(self.target.head(), self.before)
        self.open_bridge()
        with patch.object(self.bridge.validator, "evaluate", side_effect=AssertionError("must not re-evaluate")):
            outcome = self.bridge.process(self.event["event_id"])
        self.assertEqual(outcome["status"], "accepted")
        self.assertEqual(len(self.evaluations), 1)

    def test_git_cas_crash_recovers_same_intent_without_second_evaluation(self):
        self.deliver()
        self.open_bridge()
        with self.assertRaises(SimulatedCrash):
            self.perform(crash_at="after_git")
        accepted = self.target.head()
        self.assertNotEqual(accepted, self.before)
        self.assertEqual(self.authority.ledger.task("build")["status"], "submitting")
        self.open_bridge()
        with patch.object(self.bridge.validator, "evaluate", side_effect=AssertionError("must not re-evaluate")):
            outcome = self.bridge.process(self.event["event_id"])
        self.assertEqual(outcome["status"], "accepted")
        self.assertEqual(outcome["head"], accepted)
        self.assertEqual(self.authority.ledger.task("build")["status"], "complete")
        self.assertEqual(len(self.evaluations), 1)

    def test_unpublished_intent_and_ambiguous_validation_are_not_retried(self):
        self.deliver()
        self.open_bridge()
        with self.assertRaises(SimulatedCrash):
            self.perform(crash_at="after_intent")
        self.open_bridge()
        self.assertEqual(self.bridge.process(self.event["event_id"])["status"], "rejected")
        self.assertEqual(self.target.head(), self.before)
        self.assertEqual(len(self.evaluations), 1)

    def test_clock_is_sampled_after_promotion_lock_wait(self):
        self.deliver()
        self.open_bridge()
        original = self.bridge.coordinator._exclusive
        entered = threading.Event()
        release = threading.Event()

        @contextmanager
        def delayed():
            entered.set()
            if not release.wait(5):
                raise AssertionError("lock wait fixture timeout")
            with original():
                yield

        results = []
        errors = []

        def run():
            try:
                results.append(self.perform())
            except BaseException as error:
                errors.append(error)

        with patch.object(self.bridge.coordinator, "_exclusive", delayed):
            thread = threading.Thread(target=run)
            thread.start()
            try:
                self.assertTrue(entered.wait(20))
                self.clock_value = 500.0
            finally:
                release.set()
                thread.join(20)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(results[0]["status"], "rejected")
        self.assertEqual(self.target.head(), self.before)

    def test_malformed_offer_is_retained_without_blocking_valid_work(self):
        invalid = self.sender.store.publish("malformed-offer", "candidate_offer", {"passed": True})
        self.deliver()
        self.open_bridge()
        with patch.object(self.bridge.validator, "evaluate", side_effect=self.mock_evaluate):
            outcomes = self.bridge.tick()
        self.assertEqual(sorted(row["status"] for row in outcomes), ["accepted", "rejected"])
        self.assertEqual(self.bridge.state()["rejected_offers"][0]["offer_event_id"], invalid["event_id"])
        self.assertEqual(len(self.evaluations), 1)

    def test_admission_crash_does_not_restart_validation(self):
        self.deliver()
        self.open_bridge()
        with self.assertRaises(SimulatedCrash):
            self.perform(crash_at="after_admission")
        self.open_bridge()
        with patch.object(self.bridge.validator, "evaluate", side_effect=AssertionError("must not execute")):
            result = self.bridge.process(self.event["event_id"])
        self.assertEqual(result["status"], "interrupted")
        self.assertEqual(self.evaluations, [])
        self.assertEqual(self.target.head(), self.before)

    def test_intent_uses_same_write_transaction_as_fresh_clock_after_database_wait(self):
        self.deliver()
        self.open_bridge()
        original_exclusive = self.bridge.coordinator._exclusive
        original_atomic = self.bridge.ledger.atomic
        original_begin = self.bridge.ledger.begin_intent
        entering_promotion = threading.Event()
        waiting_database = threading.Event()
        release_database = threading.Event()
        admissions = []
        delayed = False

        @contextmanager
        def exclusive():
            with original_exclusive():
                entering_promotion.set()
                yield

        @contextmanager
        def atomic():
            nonlocal delayed
            if entering_promotion.is_set() and not delayed:
                delayed = True
                waiting_database.set()
                if not release_database.wait(5):
                    raise AssertionError("database admission fixture timeout")
            with original_atomic() as db:
                yield db

        def begin(*args, **kwargs):
            # Actual first intent admission must share the live write connection;
            # coordinator's later idempotent lookup is a separate replay.
            admissions.append((getattr(self.bridge.ledger.context, "db", None) is not None, kwargs["now"]))
            return original_begin(*args, **kwargs)

        results = []
        errors = []
        def run():
            try:
                results.append(self.perform())
            except BaseException as error:
                errors.append(error)

        with patch.object(self.bridge.coordinator, "_exclusive", exclusive), \
             patch.object(self.bridge.ledger, "atomic", atomic), \
             patch.object(self.bridge.ledger, "begin_intent", begin):
            thread = threading.Thread(target=run)
            thread.start()
            try:
                self.assertTrue(waiting_database.wait(20))
                self.clock_value = 200.0
            finally:
                release_database.set()
                thread.join(20)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(results[0]["status"], "accepted")
        self.assertEqual(admissions[0], (True, 200.0))
        self.assertEqual(admissions[1], (False, 200.0))

    def test_relabelled_accepted_receipt_requires_actual_published_intent(self):
        self.deliver()
        self.open_bridge()
        with self.assertRaises(SimulatedCrash):
            self.perform(crash_at="after_validation")
        record = deepcopy(self.bridge.state()["attempts"][0])
        record.update(status="accepted", intent_id="a" * 64, head=record["candidate"]["candidate_sha"])
        module._save(self.bridge.root / ("attempt-" + record["attempt_id"] + ".json"), record)
        with self.assertRaises(PromotionError):
            self.open_bridge()
        self.assertEqual(self.target.head(), self.before)
        self.assertEqual(self.authority.ledger.task("build")["status"], "claimed")

    def test_saved_candidate_substitution_does_not_reuse_another_pass(self):
        self.deliver()
        self.open_bridge()
        with self.assertRaises(SimulatedCrash):
            self.perform(crash_at="after_validation")
        record = deepcopy(self.bridge.state()["attempts"][0])
        unrelated = self.target.propose({"solution.py": "def solve(payload): return 'other'\n"})
        replacement = self.target.prepare(self.target, unrelated, self.before, lambda path: (True, "trusted tamper fixture"))
        record["candidate"] = {**asdict(replacement), "changed_paths": list(replacement.changed_paths),
                               "offered_sha": record["offer"]["bundle_manifest"]["offered_sha"]}
        record["validation"]["candidate_sha"] = replacement.candidate_sha
        module._save(self.bridge.root / ("attempt-" + record["attempt_id"] + ".json"), record)
        with self.assertRaises(PromotionError):
            self.open_bridge()
        self.assertEqual(self.target.head(), self.before)


@unittest.skipUnless(os.environ.get("GOSSIP_RUN_DOCKER_TESTS") == "1", "explicit Docker lane only")
class ReceivedPromotionDockerTests(PromotionFixture, unittest.TestCase):
    def test_received_authoritative_bundle_executes_only_in_docker_then_promotes_merged_tree(self):
        self.deliver()
        self.open_bridge()
        ok, detail = self.bridge.validator.preflight()
        self.assertTrue(ok, detail)
        outcome = self.bridge.process(self.event["event_id"])
        self.assertEqual(outcome["status"], "accepted")
        receipt = outcome["validation"]["receipt"]
        self.assertEqual(receipt["status"], "passed")
        self.assertTrue(receipt["cleanup_verified"])
        self.assertEqual(receipt["case_count"], 1)
        self.assertEqual(self.target.read_files()["solution.py"], CODE)
        self.assertEqual(self.target.read_files()["context.txt"], "independent receiver work\n")
        self.assertNotEqual(self.target.head(), self.offered)
        self.assertFalse(outcome["independent_final_project_acceptance"])
