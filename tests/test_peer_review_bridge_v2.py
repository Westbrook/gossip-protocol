"""Offline finance -> retained review artifact -> bounded correction checks.

ReleaseTarget and evidence are pure fixture records, not authenticated Git,
release eligibility, a 20-role runtime, or independent project acceptance.
Only a disposable existing-format ledger and an injected transport are used.
The resource envelope is one test process plus one asynchronous worker thread.
"""
from __future__ import annotations

from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
import unittest

from gossip_harness.ledger import Ledger
from gossip_harness.peer_financial_authority_v2 import (
    CumulativeAuthorityV2, FinancialError, canonical_payload, ledger_identity,
)
from gossip_harness.peer_project_contract_v2 import (
    ActionRequest, Context, ContractError, EvidenceRef, NamedSource,
    ReleaseTarget, WorkKey, identity, to_dict,
)
from gossip_harness.peer_review_recovery_v2 import (
    ProviderOutcome, RecoveryError, ReviewBinding, SchemaValidator,
    ValidationOutcome, new_recovery, record_response, reserve_correction,
    reserve_fresh_review,
)
from gossip_harness import peer_review_release_v2 as release
from gossip_harness.worker import HTTPResponse, MODEL, OpenAIWorker


def digest(text):
    return hashlib.sha256(text.encode()).hexdigest()


class BridgePayloads:
    def __init__(self):
        self.values = {}
        self.lock = threading.Lock()

    def read_owned(self, actor, sha):
        with self.lock:
            return self.values[actor, sha]

    def put_owned(self, actor, data):
        sha = hashlib.sha256(data).hexdigest()
        with self.lock:
            self.values[actor, sha] = bytes(data)
        return sha


class BridgeTransport:
    """A bounded in-process transport; no network or real credential access."""

    def __init__(self):
        self.responses = []
        self.calls = []
        self.omit_usage = False
        self.entered = threading.Event()
        self.release = threading.Event()
        self.release.set()
        self.lock = threading.Lock()

    def __call__(self, request, timeout, maximum):
        with self.lock:
            index = len(self.calls)
            self.calls.append(json.loads(request.data))
            review = self.responses[index]
        self.entered.set()
        if not self.release.wait(5):
            raise RuntimeError("Offline bridge barrier expired")
        proposal = {"changes": [{"path": "review.json", "content": review}],
                    "summary": "Scoped offline review fixture"}
        envelope = {
            "id": f"resp_bridge_{index}", "model": MODEL,
            "status": "completed", "service_tier": "default",
            "output": [{"type": "message", "role": "assistant", "status": "completed",
                        "content": [{"type": "output_text", "text": json.dumps(proposal)}]}],
        }
        if not self.omit_usage:
            envelope["usage"] = {"input_tokens": 101, "output_tokens": 20, "total_tokens": 121}
        return HTTPResponse(200, {"X-Request-Id": f"req_bridge_{index}"},
                            json.dumps(envelope).encode())


class PeerReviewBridgeV2Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.path = self.root / "existing-format.sqlite"
        self.ledger = Ledger(self.path, 1_000_000)
        self.ledger.add_task("historical")
        historical_lease = self.ledger.claim("historical", "fixture-owner", now=1, ttl=1)
        self.ledger.reserve("historical-charge", historical_lease, 2000, now=1)
        self.ledger.settle("historical-charge", 1000)
        self.payloads, self.transport = BridgePayloads(), BridgeTransport()
        self.authority = None
        self.addCleanup(self.cleanup)

        self.context = Context(digest("bridge-contract"), "bridge-cohort", "bridge-trajectory",
                               0, digest("bridge-requirements"))
        self.work = WorkKey("catalog", "catalog-read", "R1-review", 0)
        self.source_files = {"catalog/catalog.py": "def lookup(key):\n    return key\n"}
        receipt_text = json.dumps({"fixture": "public-check", "result": "passed"})
        self.receipt = EvidenceRef(digest("fixture-check-event"), "fixture-validator",
                                   "execution-receipt", digest(receipt_text))
        self.target = ReleaseTarget(
            self.context, 0, "fixture-repository", "refs/heads/public", "a" * 40,
            "sha1", "b" * 40, "c" * 40,
            tuple(NamedSource(path, digest(text)) for path, text in self.source_files.items()),
            digest("fixture-selection"), digest("ordered-fixture-suite"),
            digest("fixture-evaluator"), (self.receipt,),
        )
        self.readonly_files = {**self.source_files, "evidence/public-check.json": receipt_text,
                               "context/release-target.json": json.dumps(to_dict(self.target))}
        self.review_binding = ReviewBinding(
            hashlib.sha256(canonical_payload(self.source_files)).hexdigest(), identity(self.target),
            hashlib.sha256(canonical_payload(self.readonly_files)).hexdigest(),
            self.target.required_suite_sha256,
            hashlib.sha256(Path(release.__file__).read_bytes()).hexdigest(),
        )
        contract = {
            "cohort_id": self.context.cohort_id,
            "execution_contract_sha256": self.context.execution_contract_sha256,
            "ledger_identity": ledger_identity(self.path),
            "journal_root": str(self.root / "journal"),
            "transport_identity": "closed-offline-review-bridge",
            "task_specs": [{"context": to_dict(self.context), "work": to_dict(self.work),
                            "actors": ["R1"], "kinds": ["review"], "profiles": ["reviewer"],
                            "allowed_paths": ["review.json"], "max_reserved_units": 900_000}],
        }
        worker = OpenAIWorker("offline-fixture-not-a-real-key", max_output_tokens=64,
                              timeout=2, transport=self.transport)
        self.authority = CumulativeAuthorityV2.open(
            self.path, self.root / "service", contract, 900_000, 1_000_000, 1000,
            payloads=self.payloads, workers={"reviewer": worker}, max_workers=1,
            mode="offline", clock=lambda: 100.0,
        )
        self.lease = self.authority.claim("R1", self.context, self.work)
        self.validator_inputs = []

    def cleanup(self):
        self.transport.release.set()
        if self.authority is not None:
            self.authority.close()
        self.temp.cleanup()

    def review_text(self, *, malformed=False):
        return json.dumps({
            "protocol": release.PROTOCOL, "target_sha256": identity(self.target),
            "verdicts": [
                {"scope_id": scope, "covered_requirement_ids": [requirement],
                 "evidence_refs": [to_dict(self.receipt)], "verdict": "approve",
                 "rationale": ["Wrong type despite a valid first scope"] if malformed and scope == "clients"
                 else "Fixture evidence covers this exact target."}
                for scope, requirement in (("catalog", "catalog-read"), ("clients", "client-read"))
            ],
        })

    def fresh(self):
        state = new_recovery(recovery_id="R1-review-chain", binding=self.review_binding,
                             total_call_limit=2, correction_limit=1)
        return reserve_fresh_review(state, self.review_binding)

    def action(self, request_id, *, attempt=1, correction=None):
        request = {
            "task_id": self.lease.task_id,
            "instructions": "Review catalog and clients against the immutable supplied context; write review.json.",
            "allowed_paths": ["review.json"], "files": dict(self.readonly_files),
            "base_sha": self.target.commit_oid, "attempt": attempt,
            "feedback": json.dumps(asdict(correction)) if correction is not None else "",
        }
        raw = canonical_payload({"worker_request": request,
                                 "view_manifest_sha256": self.review_binding.view_sha256})
        sha = self.payloads.put_owned("R1", raw)
        ref = EvidenceRef(digest(request_id), "R1", "worker-request", sha)
        return ActionRequest(self.context, "review-" + request_id, request_id, "R1", "review",
                             self.work, "reviewer", ref, self.review_binding.view_sha256)

    def terminal(self, action):
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            reply = self.authority.lookup("R1", action.request_id)
            if reply is not None and reply.state not in {"pending", "waiting"}:
                return reply
            time.sleep(0.005)
        self.fail("Offline review did not reach a retained outcome")

    def submit_when_available(self, action):
        # A visible terminal receipt can precede the previous worker's finally
        # block releasing its slot. Retry only this unadmitted capacity wait.
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            reply = self.authority.submit("R1", action, self.lease)
            if (reply.state, reply.reason) != ("waiting", "executor_capacity"):
                return reply
            time.sleep(0.005)
        self.fail("Offline executor did not release its slot")

    def reservation_rows(self):
        with sqlite3.connect(self.path) as db:
            return db.execute("SELECT id,amount,spent,state FROM reservations ORDER BY id").fetchall()

    def validator(self, binding):
        def validate(text):
            self.validator_inputs.append(text)
            proof = self.authority.verified_terminal(binding)
            if text != proof["result"].changes["review.json"]:
                return ValidationOutcome(False, ("Response differs from the accounted review artifact",))
            try:
                # Equality above binds the actual input to the complete artifact
                # parsed here; the callback never ignores or patches its input.
                release.materialize_verdicts(binding, self.target, self.authority.verified_terminal)
            except ContractError:
                return ValidationOutcome(False, ("Complete scoped review artifact is malformed",))
            return ValidationOutcome(True)
        return SchemaValidator(self.review_binding.schema_sha256, validate)

    def record_complete(self, reservation, reply):
        proof = self.authority.verified_terminal(reply.binding)
        return record_response(
            reservation.state, call_id=reservation.request.call_id, current_binding=self.review_binding,
            outcome=ProviderOutcome("complete", proof["result"].changes["review.json"]),
            validator=self.validator(reply.binding),
        )

    def test_malformed_scoped_response_is_charged_before_one_exact_binding_correction(self):
        malformed, corrected = self.review_text(malformed=True), self.review_text()
        self.transport.responses = [malformed, corrected]
        first = self.fresh()
        action = self.action(first.request.call_id)
        self.assertEqual((first.state.calls_used, first.state.corrections_used), (1, 0))
        self.assertEqual(self.authority.submit("R1", action, self.lease).state, "pending")
        reply = self.terminal(action)
        self.assertEqual((reply.state, reply.usage_units), ("completed", 166))
        self.assertEqual(self.ledger.budget()["spent_or_reserved"], 1166)
        with self.assertRaises(ContractError):
            release.materialize_verdicts(reply.binding, self.target, self.authority.verified_terminal)
        invalid = self.record_complete(first, reply)
        self.assertEqual(invalid.state.phase, "schema_invalid")
        self.assertIsNone(invalid.validated_response)
        self.assertEqual(invalid.state.calls_used, 1)
        # Even valid substitute bytes cannot launder this charged malformed call.
        self.assertFalse(self.validator(reply.binding).validate(corrected).valid)

        correction = reserve_correction(invalid.state, self.review_binding)
        self.assertEqual(correction.request.kind, "correction")
        self.assertEqual(correction.request.binding, first.request.binding)
        self.assertEqual(correction.request.correction.binding, first.request.binding)
        self.assertEqual(correction.request.correction.previous_response_sha256, invalid.state.response_sha256)
        self.assertEqual((correction.state.calls_used, correction.state.corrections_used), (2, 1))
        next_action = self.action(correction.request.call_id, attempt=2,
                                  correction=correction.request.correction)
        self.assertEqual(self.submit_when_available(next_action).state, "pending")
        next_reply = self.terminal(next_action)
        valid = self.record_complete(correction, next_reply)
        self.assertEqual((valid.state.phase, valid.validated_response), ("validated", corrected))
        verdicts = release.materialize_verdicts(next_reply.binding, self.target,
                                               self.authority.verified_terminal)
        self.assertEqual(tuple(verdict.scope_id for verdict in verdicts), ("catalog", "clients"))
        self.assertTrue(all(verdict.review_dispatch == next_reply.binding for verdict in verdicts))
        self.assertNotEqual(reply.binding.call_id, next_reply.binding.call_id)
        first_request = self.authority.verified_terminal(reply.binding)["worker_request"]
        next_request = self.authority.verified_terminal(next_reply.binding)["worker_request"]
        self.assertEqual(first_request.files, next_request.files)
        self.assertEqual(first_request.files, self.readonly_files)
        self.assertEqual(next_request.attempt, 2)
        self.assertEqual(next_request.feedback, json.dumps(asdict(correction.request.correction)))
        self.assertEqual((first_request.base_sha, first_request.instructions, first_request.task_id),
                         (next_request.base_sha, next_request.instructions, next_request.task_id))
        self.assertEqual((first_request.allowed_paths, next_request.allowed_paths),
                         (("review.json",), ("review.json",)))
        self.assertEqual(len(self.transport.calls), 2)
        self.assertEqual(self.ledger.budget()["spent_or_reserved"], 1332)
        self.assertEqual(sorted(row[2] for row in self.reservation_rows()), [166, 166, 1000])
        self.assertTrue(all(row[3] == "settled" for row in self.reservation_rows()))
        with self.assertRaises(RecoveryError):
            reserve_correction(valid.state, self.review_binding)

    def test_unknown_provider_outcome_cannot_correct_or_reinvoke_and_keeps_reservation(self):
        self.transport.responses = [self.review_text()]
        self.transport.omit_usage = True
        first = self.fresh()
        action = self.action(first.request.call_id)
        pending = self.authority.submit("R1", action, self.lease)
        unknown = self.terminal(action)
        self.assertEqual((unknown.state, unknown.usage_units), ("unknown", None))
        with self.assertRaises(FinancialError):
            release.materialize_verdicts(unknown.binding, self.target, self.authority.verified_terminal)
        resolved = record_response(
            first.state, call_id=first.request.call_id, current_binding=self.review_binding,
            outcome=ProviderOutcome("provider_unknown", detail="Retained financial outcome is unknown"),
            validator=self.validator(unknown.binding),
        )
        self.assertEqual((resolved.state.phase, resolved.state.calls_used,
                          resolved.state.corrections_used), ("provider_unknown", 1, 0))
        self.assertIsNone(resolved.validated_response)
        self.assertEqual(self.validator_inputs, [])
        for reserve in (reserve_correction, reserve_fresh_review):
            with self.assertRaises(RecoveryError):
                reserve(resolved.state, self.review_binding)
        retained_rows = self.reservation_rows()
        row = next(row for row in retained_rows if row[0] == pending.binding.reservation_id)
        self.assertEqual(row[1:], (pending.binding.reserved_units, None, "reserved"))
        self.assertEqual(self.ledger.budget()["spent_or_reserved"], 1000 + pending.binding.reserved_units)
        replayed = self.authority.submit("R1", action, self.lease)
        self.assertEqual(replayed, unknown)
        blocked = self.submit_when_available(self.action("unpermitted-new-attempt", attempt=2))
        self.assertEqual((blocked.state, blocked.reason, blocked.binding), ("waiting", "cohort_halted", None))
        self.assertEqual(self.reservation_rows(), retained_rows)
        self.assertEqual(len(self.transport.calls), 1)

    def test_exact_request_replays_once_before_and_after_validated_artifact(self):
        text = self.review_text()
        self.transport.responses = [text]
        self.transport.release.clear()
        first = self.fresh()
        action = self.action(first.request.call_id)
        pending = self.authority.submit("R1", action, self.lease)
        self.assertTrue(self.transport.entered.wait(2))
        self.assertEqual(self.authority.submit("R1", action, self.lease), pending)
        self.assertEqual(len(self.transport.calls), 1)
        self.transport.release.set()
        reply = self.terminal(action)
        valid = self.record_complete(first, reply)
        self.assertEqual((valid.state.phase, valid.state.calls_used), ("validated", 1))
        self.assertEqual(valid.validated_response, text)
        self.assertEqual(len(release.materialize_verdicts(reply.binding, self.target,
                                                          self.authority.verified_terminal)), 2)
        retained_rows = self.reservation_rows()
        self.assertEqual(self.authority.submit("R1", action, self.lease), reply)
        self.assertEqual(self.reservation_rows(), retained_rows)
        self.assertEqual(self.ledger.budget()["spent_or_reserved"], 1166)
        self.assertEqual(len(self.transport.calls), 1)
        with self.assertRaises(RecoveryError):
            record_response(valid.state, call_id=first.request.call_id,
                            current_binding=self.review_binding,
                            outcome=ProviderOutcome("complete", text),
                            validator=self.validator(reply.binding))


if __name__ == "__main__":
    unittest.main()
