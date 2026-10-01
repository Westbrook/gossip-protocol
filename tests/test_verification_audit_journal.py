"""Offline audit controls using real journal/ledger writes and dummy outcomes.

The synthetic ``live`` receipts exercise accounting; no provider, candidate,
fixture implementation, Git operation, or container is invoked here.
"""

from copy import deepcopy
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from gossip_harness.ledger import Ledger
from gossip_harness.verification_audit import audit_accounting, audit_journal, digest
from gossip_harness.verification_journal import RequestJournal
from gossip_harness.worker import WorkerFailure, WorkerRequest, WorkerResult


def write(path, value):
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n")


class Chain:
    """One synthetic stage; RequestJournal supplies authentic receipt framing."""

    def __init__(self, root, mode="live"):
        self.root, self.mode = root, mode
        self.stage = root / "stage-0"
        (self.stage / "requests").mkdir(parents=True)
        self.journal = RequestJournal(self.stage / "journal")
        self.namespace = "verification-quality-v1/test_under_score/project"
        self.billing = self.namespace + "/stage-0"
        self.head = "b" * 40
        self.ledger = Ledger(root / "budget.sqlite", budget_units=20_000_000)
        self.before = self.ledger.budget()
        self.ledger.add_task(self.billing)
        self.lease = self.ledger.claim(self.billing, "first-process", now=0, ttl=10)
        self.contract = {"output_tokens": 50, "models": {
            "cheap": {"model": "synthetic-pinned-mini", "context_tokens": 400_000,
                      "reservation_units": 300_225,
                      "input_micro_usd_per_million": 750_000,
                      "output_micro_usd_per_million": 4_500_000},
            "strong": {"model": "synthetic-pinned-full", "context_tokens": 1_050_000,
                       "reservation_units": 5_251_125,
                       "input_micro_usd_per_million": 2_500_000,
                       "output_micro_usd_per_million": 15_000_000,
                       "long_context_above_input_tokens": 272_000,
                       "long_input_micro_usd_per_million": 5_000_000,
                       "long_output_micro_usd_per_million": 22_500_000},
        }}
        self.calls, self.dispatches, self.entries = [], {}, {}
        self.dispatch_count = 0

    def add(self, call_id="builder-slot-0-1", *, model="cheap", tokens=(1, 1),
            usage=6, failure=False, response_id=None):
        reservation = self.billing + "/" + call_id
        request = WorkerRequest("task-" + digest(reservation)[:24], "Return data only",
                                ("artifact.txt",), {"artifact.txt": "original"}, "a" * 40, 1)
        role = "reviewer" if call_id.startswith("reviewer") else "builder"
        profile = self.contract["models"][model]
        metadata = ({"api_calls": 0} if self.mode == "rehearsal" else {
            "model": profile["model"],
            "usage": {"input_tokens": tokens[0], "output_tokens": tokens[1]},
            "response_id": response_id or "response-" + call_id,
        })
        if self.mode == "rehearsal":
            usage = 0
        outcome = (WorkerFailure("Sanitized failure", usage, metadata) if failure else
                   WorkerResult({"artifact.txt": "replacement"}, "Saved", usage, metadata))
        amount = profile["reservation_units"] if self.mode == "live" else 0

        def invoke():
            self.dispatch_count += 1
            self.dispatches[call_id] = dict(call_id=call_id, reservation=reservation,
                request_sha256=digest(asdict(request)), role=role, model=model)
            if failure:
                raise outcome
            return outcome

        write(self.stage / "requests" / f"{call_id}.request.json", asdict(request))
        arguments = dict(call_id=call_id, request=request, reservation_id=reservation,
                         invoke=invoke,
                         reserve=lambda: self.ledger.reserve(reservation, self.lease, amount, now=1),
                         settle=lambda cost: self.ledger.settle(reservation, cost))
        try:
            self.journal.execute(**arguments)
        except WorkerFailure:
            if not failure:
                raise
        payload = (dict(failure=str(outcome), usage_units=usage, metadata=metadata) if failure
                   else asdict(outcome))
        row = dict(payload, call_id=call_id, reservation=reservation, role=role, model=model)
        write(self.stage / "requests" / f"{call_id}.result.json", row)
        self.calls.append(row)
        self.entries[call_id] = arguments
        return row

    def journal_audit(self, calls=None, dispatches=None):
        return audit_journal(self.stage, self.calls if calls is None else calls, self.billing,
                             self.contract, self.mode,
                             self.dispatches if dispatches is None else dispatches)

    def finish(self):
        intent = self.ledger.begin_intent([self.lease], self.root / "accounting-record.git",
                                          "a" * 40, self.head, now=2)
        self.ledger.finish_intent(intent["id"], True, "Synthetic accounting record")

    def report(self):
        usage = sum(row["usage_units"] for row in self.calls)
        return dict(incremental_micro_usd=usage, budget_before=self.before,
                    budget=self.ledger.budget())

    def accounting_audit(self, bindings=None, report=None):
        if bindings is None:
            _, bindings = self.journal_audit()
        return audit_accounting(self.ledger.path, bindings, [(self.billing, self.head)],
                                [self.namespace], self.report() if report is None else report)


class VerificationAuditJournalTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.chain = Chain(Path(self.temp.name))

    def test_real_receipts_replay_once_and_charge_once_after_new_lease_epoch(self):
        chain = self.chain
        call = chain.add()
        before = chain.ledger.budget()
        chain.journal.execute(**chain.entries[call["call_id"]])
        self.assertEqual(chain.dispatch_count, 1)
        self.assertEqual(chain.ledger.budget(), before)
        chain.lease = chain.ledger.claim(chain.billing, "resumed-process", now=11, ttl=10)
        chain.finish()
        loaded, bindings = chain.journal_audit()
        self.assertEqual(set(loaded), {call["call_id"]})
        self.assertEqual(bindings[0]["usage"], 6)
        self.assertEqual(chain.accounting_audit()["study_usage_micro_usd"], 6)

    def test_known_failure_is_paid_but_unknown_usage_cannot_pass(self):
        self.chain.add(failure=True)
        self.chain.finish()
        self.assertEqual(self.chain.accounting_audit()["study_usage_micro_usd"], 6)
        with tempfile.TemporaryDirectory() as directory:
            unknown = Chain(Path(directory))
            unknown.add(failure=True, usage=None)
            with self.assertRaisesRegex(ValueError, "unknown provider usage"):
                unknown.journal_audit()
            with sqlite3.connect(unknown.ledger.path) as db:
                self.assertEqual(db.execute("SELECT spent,state FROM reservations").fetchone(),
                                 (None, "reserved"))

    def test_unknown_and_missing_dispatches_are_rejected(self):
        self.chain.add()
        with self.assertRaisesRegex(ValueError, "Missing provider dispatch"):
            self.chain.journal_audit(dispatches={})
        dispatches = dict(self.chain.dispatches, orphan={"reservation": "unexpected"})
        with self.assertRaisesRegex(ValueError, "Unlinked provider dispatch"):
            self.chain.journal_audit(dispatches=dispatches)

    def test_orphan_request_and_journal_receipts_are_rejected(self):
        self.chain.add()
        for path, message in (
            (self.chain.stage / "requests" / "orphan.request.json", "roster"),
            (self.chain.stage / "journal" / "orphan.settled.json", "orphan journal"),
        ):
            with self.subTest(path=path.name):
                write(path, {})
                try:
                    with self.assertRaisesRegex(ValueError, message):
                        self.chain.journal_audit()
                finally:
                    path.unlink()

    def test_changed_payload_or_hash_cannot_match_a_saved_response(self):
        call = self.chain.add()
        path = self.chain.journal.paths(call["call_id"])["result"]
        original = path.read_bytes()
        for field, value in (("payload_sha256", "0" * 64), ("payload", {"summary": "Changed"})):
            with self.subTest(field=field):
                changed = json.loads(original)
                changed[field] = value
                write(path, changed)
                with self.assertRaisesRegex(ValueError, "payload mismatch"):
                    self.chain.journal_audit()
                path.write_bytes(original)

    def test_json_boolean_cannot_impersonate_integer_usage_in_journal(self):
        call = self.chain.add(tokens=(1, 0), usage=1)
        paths = self.chain.journal.paths(call["call_id"])
        result = json.loads(paths["result"].read_bytes())
        result["payload"]["usage_units"] = True
        # Keep the original payload checksum: equality must not collapse true/1.
        write(paths["result"], result)
        settled = json.loads(paths["settled"].read_bytes())
        settled["result_sha256"] = hashlib.sha256(paths["result"].read_bytes()).hexdigest()
        write(paths["settled"], settled)
        with self.assertRaises(ValueError):
            self.chain.journal_audit()

    def test_settlement_binds_exact_persisted_result_bytes(self):
        call = self.chain.add()
        result = self.chain.journal.paths(call["call_id"])["result"]
        # Same JSON, different bytes; the stored settlement must not still pass.
        write(result, json.loads(result.read_bytes()))
        with self.assertRaisesRegex(ValueError, "settlement mismatch"):
            self.chain.journal_audit()

    def test_duplicate_invocation_and_duplicate_charge_are_rejected(self):
        call = self.chain.add()
        with self.assertRaisesRegex(ValueError, "Duplicate stage invocation"):
            self.chain.journal_audit(calls=[call, deepcopy(call)])
        self.chain.finish()
        _, bindings = self.chain.journal_audit()
        with self.assertRaisesRegex(ValueError, "Reservation reused"):
            self.chain.accounting_audit(bindings=bindings * 2)

    def test_distinct_requests_cannot_reuse_provider_response_identity(self):
        self.chain.add(response_id="same-response")
        self.chain.add("builder-slot-1-1", response_id="same-response")
        self.chain.finish()
        with self.assertRaisesRegex(ValueError, "response ID reused"):
            self.chain.accounting_audit()

    def test_exact_rounding_and_long_context_threshold(self):
        # Literal independently calculated microUSD totals at/above the boundary.
        self.chain.add(tokens=(1, 1), usage=6)
        self.chain.add("reviewer-review-1", model="strong", tokens=(272_000, 1), usage=680_015)
        self.chain.add("reviewer-review-2", model="strong", tokens=(272_001, 1), usage=1_360_028)
        self.chain.finish()
        self.assertEqual(self.chain.accounting_audit()["study_usage_micro_usd"], 2_040_049)

    def test_self_consistent_receipts_do_not_excuse_wrong_pricing_or_token_bounds(self):
        for tokens, usage, message in (((1, 1), 5, "frozen pricing"),
                                      ((1, 51), 231, "reserved bounds")):
            with self.subTest(tokens=tokens), tempfile.TemporaryDirectory() as directory:
                chain = Chain(Path(directory))
                chain.add(tokens=tokens, usage=usage)
                with self.assertRaisesRegex(ValueError, message):
                    chain.journal_audit()

    def test_unlinked_settled_charge_inside_namespace_is_not_hidden_in_totals(self):
        self.chain.add()
        orphan = self.chain.billing + "/orphan"
        self.chain.ledger.reserve(orphan, self.chain.lease, 10, now=1)
        self.chain.ledger.settle(orphan, 1)
        self.chain.finish()
        with self.assertRaisesRegex(ValueError, "orphan study reservation"):
            self.chain.accounting_audit()

    def test_changed_ledger_usage_or_unsettled_charge_fails_closed(self):
        self.chain.add()
        self.chain.finish()
        for spent, state in ((5, "settled"), (None, "reserved")):
            with self.subTest(spent=spent):
                with sqlite3.connect(self.chain.ledger.path) as db:
                    db.execute("UPDATE reservations SET spent=?,state=?", (spent, state))
                with self.assertRaisesRegex(ValueError, "settled reservation"):
                    self.chain.accounting_audit()

    def test_budget_delta_and_stage_promotion_must_match(self):
        self.chain.add()
        with self.assertRaisesRegex(ValueError, "promotion is incomplete"):
            self.chain.accounting_audit()
        self.chain.finish()
        report = self.chain.report()
        report["incremental_micro_usd"] += 1
        with self.assertRaisesRegex(ValueError, "budget delta"):
            self.chain.accounting_audit(report=report)

    def test_later_unrelated_activity_is_disclosed_not_charged_to_study(self):
        self.chain.add()
        self.chain.finish()
        frozen_report = self.chain.report()
        unrelated = self.chain.namespace + "-other/stage-0"
        self.chain.ledger.add_task(unrelated)
        lease = self.chain.ledger.claim(unrelated, "unrelated", now=0, ttl=10)
        self.chain.ledger.reserve(unrelated + "/call", lease, 10, now=1)
        audited = self.chain.accounting_audit(report=frozen_report)
        self.assertEqual(audited["study_usage_micro_usd"], 6)
        self.assertEqual(audited["current_spent_or_reserved"], 16)
        self.assertEqual(audited["unrelated_or_current_unsettled_reservations"], 1)

    def test_complete_study_scope_rejects_zero_charge_unknown_case(self):
        chain = self.chain
        chain.add()
        chain.finish()
        report = chain.report()
        _, bindings = chain.journal_audit()
        study = chain.namespace.rsplit("/", 1)[0]

        def audit():
            return audit_accounting(chain.ledger.path, bindings, [(chain.billing, chain.head)],
                                    [chain.namespace], report, study_namespace=study)

        # A later, similarly prefixed study must remain outside this roster.
        unrelated = study + "-later/project/stage-0"
        chain.ledger.add_task(unrelated)
        lease = chain.ledger.claim(unrelated, "later-worker", now=0, ttl=10)
        chain.ledger.reserve(unrelated + "/call", lease, 10, now=1)
        accepted = audit()
        self.assertEqual(accepted["study_usage_micro_usd"], 6)
        self.assertEqual(accepted["current_spent_or_reserved"], 16)
        self.assertEqual(accepted["unrelated_or_current_unsettled_reservations"], 1)

        # This hidden extra case has no budget effect, so a sum alone misses it.
        orphan = study + "/unrecorded-case/stage-0"
        chain.ledger.add_task(orphan)
        lease = chain.ledger.claim(orphan, "orphan-worker", now=0, ttl=10)
        chain.ledger.reserve(orphan + "/call", lease, 0, now=1)
        chain.ledger.settle(orphan + "/call", 0)
        self.assertEqual(chain.ledger.budget()["spent_or_reserved"], 16)
        with self.assertRaisesRegex(ValueError, "complete study namespace"):
            audit()

    def test_rehearsal_evidence_requires_zero_usage_and_no_provider_calls(self):
        with tempfile.TemporaryDirectory() as directory:
            chain = Chain(Path(directory), mode="rehearsal")
            chain.add()
            chain.finish()
            self.assertEqual(chain.accounting_audit()["study_usage_micro_usd"], 0)
            chain.mode = "live"
            with self.assertRaisesRegex(ValueError, "pinned profile"):
                chain.journal_audit()


if __name__ == "__main__":
    unittest.main()
