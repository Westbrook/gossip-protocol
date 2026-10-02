"""Zero-provider checks for prospective bounded reviewer-response correction."""
from dataclasses import asdict, replace
import json
import unittest

from gossip_harness.peer_review_recovery_v2 import (
    PROTOCOL, SIMPLE_REVIEW_SCHEMA_SHA256, SIMPLE_REVIEW_VALIDATOR,
    ProviderOutcome, RecoveryError, RecoveryState, ReviewBinding, SchemaValidator,
    ValidationOutcome, new_recovery, record_response, reserve_correction,
    reserve_fresh_review, validate_simple_review,
)


class PeerReviewRecoveryV2Tests(unittest.TestCase):
    def binding(self):
        return ReviewBinding("a" * 64, "b" * 64, "c" * 64, "d" * 64, SIMPLE_REVIEW_SCHEMA_SHA256)

    def state(self, total=4, corrections=1, used=0):
        return new_recovery(recovery_id="reviewer-7-milestone-1", binding=self.binding(),
                            total_call_limit=total, correction_limit=corrections, calls_already_used=used)

    def pending(self, **kwargs):
        reservation = reserve_fresh_review(self.state(**kwargs), self.binding())
        self.assertIsNotNone(reservation.request)
        return reservation.state

    def response(self, state, *, notes="Exact evidence inspected", decision="approve", remaining=None,
                 binding=None, text=None, validator=SIMPLE_REVIEW_VALIDATOR):
        return record_response(state, call_id=state.pending_call_id,
            current_binding=binding or state.binding,
            outcome=ProviderOutcome("complete", text if text is not None else json.dumps({
                "decision": decision, "notes": notes, "remaining": remaining or []})),
            validator=validator)

    def restore(self, state):
        return RecoveryState.from_json(state.to_json(), expected_sha256=state.checkpoint_sha256)

    def test_malformed_notes_has_no_partial_approval_and_one_counted_correction(self):
        initial = self.pending()
        malformed = self.response(initial, notes=["All evidence passes"])
        self.assertEqual(malformed.state.phase, "schema_invalid")
        self.assertIsNone(malformed.validated_response)
        self.assertEqual(malformed.state.calls_used, 1)
        correction = reserve_correction(malformed.state, self.binding())
        self.assertEqual(correction.state.calls_used, 2)
        self.assertEqual(correction.state.corrections_used, 1)
        self.assertEqual(correction.request.kind, "correction")
        prompt = correction.request.correction
        self.assertEqual(prompt.binding, self.binding())
        self.assertEqual(prompt.previous_response_sha256, malformed.state.response_sha256)
        self.assertIn("notes must be a string", prompt.validation_errors[0])
        valid = self.response(correction.state)
        self.assertEqual(valid.state.phase, "validated")
        self.assertEqual(json.loads(valid.validated_response)["decision"], "approve")
        with self.assertRaises(RecoveryError):
            reserve_correction(valid.state, self.binding())

    def test_semantic_rejection_and_uncertainty_are_valid_not_malformed(self):
        for decision in ("request_changes", "insufficient_evidence"):
            with self.subTest(decision=decision):
                result = self.response(self.pending(), decision=decision, remaining=["R7"])
                self.assertEqual(result.state.phase, "validated")
                self.assertIsNotNone(result.validated_response)
                with self.assertRaises(RecoveryError):
                    reserve_correction(result.state, self.binding())
                with self.assertRaises(RecoveryError):
                    reserve_fresh_review(result.state, self.binding())

    def test_all_immutable_binding_components_invalidate_correction(self):
        malformed = self.response(self.pending(), notes=[]).state
        for field in asdict(self.binding()):
            with self.subTest(field=field):
                changed = replace(self.binding(), **{field: "e" * 64})
                invalidated = reserve_correction(malformed, changed)
                self.assertIsNone(invalidated.request)
                self.assertEqual(invalidated.state.phase, "context_changed")
                self.assertEqual(invalidated.state.calls_used, 1)
                fresh = reserve_fresh_review(invalidated.state, changed)
                self.assertEqual(fresh.request.kind, "fresh")
                self.assertIsNone(fresh.request.correction)
                self.assertEqual(fresh.state.calls_used, 2)
                self.assertEqual(fresh.state.corrections_used, 0)
                self.assertEqual(fresh.state.binding, changed)

    def test_stale_complete_response_cannot_authorize_any_decision(self):
        state = self.pending()
        result = self.response(state, binding=replace(state.binding, target_sha256="e" * 64))
        self.assertEqual(result.state.phase, "context_changed")
        self.assertIsNone(result.validated_response)
        self.assertEqual(result.state.calls_used, 1)

    def test_correction_cap_stops_with_unused_reviewer_allowance(self):
        malformed = self.response(self.pending(total=9, corrections=1), notes=[]).state
        correction = reserve_correction(malformed, self.binding())
        malformed_again = self.response(correction.state, notes=[]).state
        exhausted = reserve_correction(malformed_again, self.binding())
        self.assertIsNone(exhausted.request)
        self.assertEqual(exhausted.state.phase, "exhausted")
        self.assertEqual(exhausted.state.calls_used, 2)
        self.assertEqual(exhausted.state.corrections_used, 1)
        self.assertIsNone(reserve_correction(exhausted.state, self.binding()).request)
        with self.assertRaises(RecoveryError):
            reserve_fresh_review(exhausted.state, self.binding())

    def test_total_call_limit_includes_preexisting_and_correction_calls(self):
        malformed = self.response(self.pending(total=4, corrections=3, used=3), notes=[]).state
        exhausted = reserve_correction(malformed, self.binding())
        self.assertIsNone(exhausted.request)
        self.assertEqual(exhausted.state.calls_used, 4)
        self.assertEqual(exhausted.state.corrections_used, 0)
        initial = self.state(total=4, used=4)
        self.assertIsNone(reserve_fresh_review(initial, self.binding()).request)

    def test_zero_correction_allowance_never_emits_a_retry(self):
        malformed = self.response(self.pending(corrections=0), notes=[]).state
        self.assertIsNone(reserve_correction(malformed, self.binding()).request)

    def test_restart_keeps_limits_consumption_and_pending_call_without_dispatch(self):
        state = self.pending(total=6, corrections=1, used=2)
        restored = self.restore(state)
        self.assertEqual(restored, state)
        for reserve in (reserve_correction, reserve_fresh_review):
            with self.assertRaises(RecoveryError):
                reserve(restored, self.binding())
        malformed = self.restore(self.response(restored, notes=[]).state)
        pending = self.restore(reserve_correction(malformed, self.binding()).state)
        final_invalid = self.restore(self.response(pending, notes=[]).state)
        exhausted = self.restore(reserve_correction(final_invalid, self.binding()).state)
        self.assertEqual((exhausted.calls_used, exhausted.corrections_used), (4, 1))
        self.assertIsNone(reserve_correction(exhausted, self.binding()).request)

    def test_restart_rejects_tampering_and_old_checkpoint_under_current_digest(self):
        original = self.state()
        pending = reserve_fresh_review(original, self.binding()).state
        with self.assertRaises(RecoveryError):
            RecoveryState.from_json(original.to_json(), expected_sha256=pending.checkpoint_sha256)
        for field, value in (("calls_used", 0), ("correction_limit", 4), ("total_call_limit", 99),
                             ("phase", "ready"), ("protocol", "old")):
            with self.subTest(field=field):
                document = json.loads(pending.to_json())
                document["state"][field] = value
                with self.assertRaises(RecoveryError):
                    RecoveryState.from_json(json.dumps(document), expected_sha256=pending.checkpoint_sha256)
        duplicate = pending.to_json().replace('"protocol":', '"protocol":"duplicate","protocol":')
        with self.assertRaises(RecoveryError):
            RecoveryState.from_json(duplicate, expected_sha256=pending.checkpoint_sha256)

    def test_unknown_and_infrastructure_failures_never_invoke_validator_or_retry(self):
        def forbidden(_text):
            self.fail("Provider failure must not be parsed as a reviewer response")
        validator = SchemaValidator(SIMPLE_REVIEW_SCHEMA_SHA256, forbidden)
        for status in ("provider_unknown", "infrastructure_failure"):
            with self.subTest(status=status):
                state = self.pending()
                result = record_response(state, call_id=state.pending_call_id, current_binding=self.binding(),
                    outcome=ProviderOutcome(status, detail="Provider completion is unavailable"), validator=validator)
                restored = self.restore(result.state)
                self.assertEqual(restored.phase, status)
                self.assertEqual(restored.calls_used, 1)
                self.assertIsNone(result.validated_response)
                for reserve in (reserve_fresh_review, reserve_correction):
                    with self.assertRaises(RecoveryError):
                        reserve(restored, replace(self.binding(), target_sha256="e" * 64))

    def test_stale_or_duplicate_response_cannot_consume_another_slot(self):
        first = self.pending()
        malformed = self.response(first, notes=[]).state
        correction = reserve_correction(malformed, self.binding()).state
        for target in (malformed, correction):
            with self.assertRaises(RecoveryError):
                record_response(target, call_id=first.pending_call_id, current_binding=self.binding(),
                    outcome=ProviderOutcome("complete", '{"decision":"approve","notes":"ok","remaining":[]}'),
                    validator=SIMPLE_REVIEW_VALIDATOR)
        valid = self.response(correction).state
        with self.assertRaises(RecoveryError):
            self.response(valid)

    def test_changed_schema_validator_rejected_and_validator_crash_not_retried(self):
        pending = self.pending()
        wrong = SchemaValidator("e" * 64, lambda _: ValidationOutcome(True))
        with self.assertRaises(RecoveryError):
            self.response(pending, validator=wrong)
        def broken(_):
            raise RuntimeError("fixture validation fault")
        result = self.response(pending, validator=SchemaValidator(SIMPLE_REVIEW_SCHEMA_SHA256, broken))
        self.assertEqual(result.state.phase, "infrastructure_failure")
        self.assertIsNone(result.validated_response)
        with self.assertRaises(RecoveryError):
            reserve_correction(result.state, self.binding())

    def test_policy_accepts_other_fully_validated_schemas_without_reading_decisions(self):
        schema = "f" * 64
        binding = replace(self.binding(), schema_sha256=schema)
        state = replace(self.state(), binding=binding)
        pending = reserve_fresh_review(state, binding).state
        text = '{"scoped_verdicts":[{"scope":"S0","verdict":"request_changes"}]}'
        validator = SchemaValidator(schema, lambda value: ValidationOutcome(value == text,
            () if value == text else ("Expected entire fixture response",)))
        result = self.response(pending, text=text, validator=validator)
        self.assertEqual(result.state.phase, "validated")
        self.assertEqual(result.validated_response, text)

    def test_fixture_full_schema_rejects_duplicate_unknown_truncated_and_wrong_types(self):
        malformed = [
            '{"decision":"approve","notes":[],"remaining":[]}',
            '{"decision":"approve","notes":"ok","remaining":[],"extra":true}',
            '{"decision":"approve","notes":"ok","notes":"again","remaining":[]}',
            '{"decision":"approve","notes":"ok","remaining":[]',
            '{"decision":"approve","notes":"ok","remaining":["R1","R1"]}',
            '{"decision":true,"notes":"ok","remaining":[]}',
            '{"decision":"approve","notes":"ok","remaining":[{}]}',
            '{"decision":"approve","notes":NaN,"remaining":[]}',
            '```json\n{"decision":"approve","notes":"ok","remaining":[]}\n```',
        ]
        for text in malformed:
            with self.subTest(text=text):
                self.assertFalse(validate_simple_review(text).valid)
                result = self.response(self.pending(), text=text)
                self.assertEqual(result.state.phase, "schema_invalid")
                self.assertIsNone(result.validated_response)

    def test_budget_values_and_impossible_states_are_rejected(self):
        for value in (-1, True, 1.5, float("inf")):
            with self.subTest(value=value), self.assertRaises(RecoveryError):
                self.state(corrections=value)
        with self.assertRaises(RecoveryError):
            self.state(total=2, corrections=3)
        with self.assertRaises(RecoveryError):
            replace(self.pending(), corrections_used=1)
        with self.assertRaises(RecoveryError):
            replace(self.pending(), phase="ready", pending_call_id=None)
        with self.assertRaises(RecoveryError):
            replace(self.pending(), response_sha256="f" * 64)
        with self.assertRaises(RecoveryError):
            replace(self.pending(), validation_errors=("Already failed",))
        for phase in ("provider_unknown", "infrastructure_failure"):
            with self.subTest(phase=phase), self.assertRaises(RecoveryError):
                replace(self.pending(), phase=phase, pending_call_id=None)
        with self.assertRaises(RecoveryError):
            replace(self.pending(), phase="exhausted", pending_call_id=None)
        with self.assertRaises(RecoveryError):
            ValidationOutcome(True, ("Cannot approve a partly invalid response",))
        self.assertEqual(PROTOCOL, "peer-review-recovery-v2")

    def test_fresh_context_does_not_reset_consumed_correction_allowance(self):
        malformed = self.response(self.pending(), notes=[]).state
        correction = reserve_correction(malformed, self.binding()).state
        valid = self.response(correction, decision="request_changes").state
        changed = replace(self.binding(), source_sha256="e" * 64)
        fresh = reserve_fresh_review(self.restore(valid), changed).state
        self.assertEqual((fresh.calls_used, fresh.corrections_used), (3, 1))
        malformed_fresh = self.response(fresh, notes=[]).state
        exhausted = reserve_correction(malformed_fresh, changed)
        self.assertIsNone(exhausted.request)
        self.assertEqual(exhausted.state.calls_used, 3)

    def test_changed_context_can_start_fresh_after_correction_exhaustion(self):
        malformed = self.response(self.pending(total=9), notes=[]).state
        correction = reserve_correction(malformed, self.binding()).state
        malformed_again = self.response(correction, notes=[]).state
        exhausted = reserve_correction(malformed_again, self.binding()).state
        changed = replace(self.binding(), source_sha256="e" * 64)
        fresh = reserve_fresh_review(self.restore(exhausted), changed)
        self.assertEqual(fresh.request.kind, "fresh")
        self.assertEqual((fresh.state.calls_used, fresh.state.corrections_used), (3, 1))
        still_invalid = self.response(fresh.state, notes=[]).state
        self.assertIsNone(reserve_correction(still_invalid, changed).request)

    def test_bounded_validation_errors_always_fit_durable_checkpoint(self):
        errors = tuple("\\x00" * 60 for _ in range(16))
        with self.assertRaises(RecoveryError):
            ValidationOutcome(False, errors)
        bounded = tuple("😀" * 20 for _ in range(16))
        validator = SchemaValidator(SIMPLE_REVIEW_SCHEMA_SHA256, lambda _: ValidationOutcome(False, bounded))
        invalid = self.response(self.pending(), validator=validator).state
        self.assertEqual(self.restore(invalid), invalid)

    def test_missing_or_partial_provider_outcome_is_not_a_complete_response(self):
        with self.assertRaises(RecoveryError):
            ProviderOutcome("complete")
        with self.assertRaises(RecoveryError):
            ProviderOutcome("provider_unknown", response='{"decision":"approve"', detail="Timed out")
        pending = self.pending()
        with self.assertRaises(RecoveryError):
            record_response(pending, call_id=pending.pending_call_id, current_binding=self.binding(),
                            outcome=None, validator=SIMPLE_REVIEW_VALIDATOR)
