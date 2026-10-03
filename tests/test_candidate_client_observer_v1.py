"""Offline synthetic controls for the host-only finite CLI observer."""
from copy import deepcopy
from dataclasses import FrozenInstanceError
import hashlib
import json
import unittest

from gossip_harness import candidate_client_observer_v1 as observer


def _binding():
    result = {key: "a" * 64 for key in observer._DIGEST_FIELDS}
    result.update(protocol=observer.BINDING_PROTOCOL, execution_id="execution-1",
                  commit_oid="b" * 40, tree_oid="c" * 40, milestone="m1",
                  purpose="harness_qualification", case_id="cli-list", step_id="list",
                  step_index=0, ordered_step_ids=["list"], argv=["python", "-m", "library", "list"])
    return result


def _transport(stdout, stderr, exit_code=0):
    def stream(raw, path):
        return {"path": path, "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw),
                "observed_bytes": len(raw), "truncated": False, "complete": True}
    return {"status": "completed", "capture_complete": True, "exit_code": exit_code,
            "stdout": stream(stdout, "stdout.bin"), "stderr": stream(stderr, "stderr.bin"),
            "completion": {"container_id": "d" * 64, "started": True, "natural": True,
                "wait_status_code": exit_code, "inspect_exit_code": exit_code,
                "finished_at": "2026-10-03T12:00:00Z", "oom_killed": False, "state_error": "",
                "identity_verified": True, "killed_by_controller": False, "wait_error": None,
                "signal_exit_ambiguous": False}}


def _observation(stdout=b'{}', stderr=b'', exit_code=0, edit=None):
    transport = _transport(stdout, stderr, exit_code)
    if edit:
        edit(transport)
    return observer.process_observation(_binding(), transport, stdout, stderr)


def _expected(kind="success", value=None, supported=True, code="not_found"):
    ids = ["process.exit"]
    unspecified = []
    if kind == "success":
        ids.append("stdout.json")
        if supported:
            ids.append("stdout.value")
        else:
            unspecified.append("stdout.wrapper")
    elif kind == "domain_error":
        ids.extend(["stderr.error", "stderr.error_code"])
        if code is None:
            unspecified.append("stderr.error_code")
    else:
        unspecified.append("usage.presentation")
    return {"kind": kind, "exit_code": 0 if kind == "success" else 2, "value": value,
            "semantic_value_supported": supported, "error_code": code,
            "assertion_ids": ids + [key for key in unspecified if key not in ids],
            "unspecified_assertion_ids": unspecified}


class CandidateClientObservationBindingTests(unittest.TestCase):
    def test_bytes_and_metadata_are_immutable(self):
        binding = _binding()
        transport = _transport(b'{}', b'')
        observation = observer.process_observation(binding, transport, b'{}', b'')
        before = observer.observation_sha256(observation)
        binding["case_id"] = "later"
        transport["completion"]["natural"] = False
        self.assertEqual(observer.observation_sha256(observation), before)
        with self.assertRaises(FrozenInstanceError):
            observation.stdout = b'[]'

    def test_binding_exact_fields_identity_order_and_types(self):
        alterations = [{"source_sha256": "z" * 64}, {"tree_oid": "x"}, {"step_index": True},
            {"ordered_step_ids": ["list", "list"]}, {"step_id": "wrong"}, {"argv": ["x\0y"]},
            {"purpose": "independent_acceptance"}, {"extra": "field"}]
        for changes in alterations:
            with self.subTest(changes=changes), self.assertRaises(observer.ObservationUnavailable):
                observer.process_observation(_binding() | changes, _transport(b'{}', b''), b'{}', b'')
        binding = _binding()
        del binding["ordered_suite_sha256"]
        with self.assertRaises(observer.ObservationUnavailable):
            observer.process_observation(binding, _transport(b'{}', b''), b'{}', b'')

    def test_changed_raw_hash_or_length_is_unavailable(self):
        for key, value in (("sha256", "0" * 64), ("bytes", 3), ("observed_bytes", 4), ("truncated", 1)):
            with self.subTest(key=key), self.assertRaises(observer.ObservationUnavailable):
                _observation(edit=lambda t: t["stdout"].update({key: value}))

    def test_forged_candidate_supervisor_marker_does_not_prove_completion(self):
        raw = b'{"exit_code":0,"completed":true,"natural":true}'
        def incomplete(t):
            t.update(status="completion_unproven", exit_code=None)
            t["completion"].update(natural=False, wait_status_code=None, inspect_exit_code=None)
        result = observer.observe_cli_step(_expected(value=json.loads(raw)), _observation(raw, edit=incomplete))
        self.assertIsNone(result["assertions"]["process.exit"])
        self.assertIsNone(result["assertions"]["stdout.value"])
        self.assertEqual(result["status"], "observation-unavailable")

    def test_completed_status_requires_real_agreed_natural_exit(self):
        for changes in ({"natural": False}, {"wait_status_code": 2}, {"oom_killed": True},
                        {"state_error": "failure"}, {"inspect_exit_code": True}, {"finished_at": ""},
                        {"identity_verified": False}, {"killed_by_controller": True},
                        {"wait_error": {"Message": "bad"}}, {"signal_exit_ambiguous": True}):
            with self.subTest(changes=changes), self.assertRaises(observer.ObservationUnavailable):
                _observation(edit=lambda t: t["completion"].update(changes))

    def test_noncanonical_or_mutable_metadata_cannot_bypass_factory(self):
        obs = _observation()
        altered = observer.ProcessObservation(json.dumps(_binding()).encode(), obs.transport_json, obs.stdout, obs.stderr)
        with self.assertRaises(observer.ObservationUnavailable):
            observer.observation_sha256(altered)
        altered = observer.ProcessObservation(bytearray(obs.binding_json), obs.transport_json, obs.stdout, obs.stderr)
        with self.assertRaises(observer.ObservationUnavailable):
            observer.observation_sha256(altered)

    def test_digest_binds_streams_binding_and_provenance(self):
        original = _observation()
        changed = _observation(stderr=b'diagnostic')
        self.assertNotEqual(observer.observation_sha256(original), observer.observation_sha256(changed))
        binding = _binding() | {"limits_sha256": "f" * 64}
        changed = observer.process_observation(binding, _transport(b'{}', b''), b'{}', b'')
        self.assertNotEqual(observer.observation_sha256(original), observer.observation_sha256(changed))

    def test_partial_stream_metadata_cannot_claim_complete(self):
        def partial(t):
            t["stdout"].update(truncated=True)
        with self.assertRaises(observer.ObservationUnavailable):
            _observation(edit=partial)


class CandidateClientObservationScoringTests(unittest.TestCase):
    def test_success_accepts_semantic_whitespace_order_escape_and_diagnostics(self):
        raw = b' \n {"b": [1.00, "caf\\u00e9"], "a": true}\r\n'
        result = observer.observe_cli_step(_expected(value={"a": True, "b": [1, "caf\u00e9"]}),
                                          _observation(raw, b'optional diagnostic\n'))
        self.assertTrue(result["all_local_assertions_passed"])
        self.assertEqual(set(result["assertions"]), {"process.exit", "stdout.json", "stdout.value"})

    def test_true_and_false_never_equal_json_numbers(self):
        for raw, value in ((b'true', 1), (b'false', 0), (b'1', True), (b'0', False),
                           (b'{"epoch":true}', {"epoch": 1})):
            with self.subTest(raw=raw):
                result = observer.observe_cli_step(_expected(value=value), _observation(raw))
                self.assertFalse(result["assertions"]["stdout.value"])
                self.assertTrue(result["assertions"]["stdout.json"])

    def test_null_is_a_supported_semantic_value(self):
        result = observer.observe_cli_step(_expected(value=None), _observation(b'null'))
        self.assertTrue(result["all_local_assertions_passed"])

    def test_extra_value_prose_empty_invalid_utf8_nonfinite_are_failures(self):
        for raw in (b'{} {}', b'prefix {}', b'{} suffix', b'', b' \n', b'\xff', b'NaN', b'Infinity'):
            with self.subTest(raw=raw):
                result = observer.observe_cli_step(_expected(value={}), _observation(raw))
                self.assertFalse(result["assertions"]["stdout.json"])
                self.assertFalse(result["assertions"]["stdout.value"])

    def test_duplicate_members_and_depth_limits_are_unavailable(self):
        for raw in (b'{"x":0,"x":1}', b'[' * 300 + b'0' + b']' * 300):
            with self.subTest(length=len(raw)):
                result = observer.observe_cli_step(_expected(value={"x": 1}), _observation(raw))
                self.assertIsNone(result["assertions"]["stdout.json"])
                self.assertIsNone(result["assertions"]["stdout.value"])
                self.assertFalse(result["all_local_assertions_passed"])

    def test_large_lawful_numbers_not_confused_with_parser_integer_limit(self):
        result = observer.observe_cli_step(_expected(supported=False), _observation(b'1' * 5000))
        self.assertTrue(result["assertions"]["stdout.json"])
        self.assertEqual(result["status"], "partial-observation")

    def test_unspecified_success_wrapper_is_not_inferred(self):
        result = observer.observe_cli_step(_expected(supported=False), _observation(b'{"unfamiliar":"wrapper"}'))
        self.assertTrue(result["supported_assertions_passed"])
        self.assertIsNone(result["assertions"]["stdout.wrapper"])
        self.assertFalse(result["all_local_assertions_passed"])
        self.assertEqual(result["status"], "partial-observation")

    def test_domain_error_allows_auxiliary_stdout(self):
        result = observer.observe_cli_step(_expected("domain_error"),
            _observation(b'diagnostic on other channel', b' {"error":"not_found"}\n', 2))
        self.assertTrue(result["all_local_assertions_passed"])
        self.assertNotIn("stdout.empty", result["assertions"])

    def test_pure_stderr_shape_and_code_are_separate(self):
        result = observer.observe_cli_step(_expected("domain_error"),
            _observation(b'', b'{"error":"not_found","extra":true}', 2))
        self.assertFalse(result["assertions"]["stderr.error"])
        self.assertTrue(result["assertions"]["stderr.error_code"])
        for raw in (b'{}', b'[]', b'{"error":14}', b'{"error":""}'):
            with self.subTest(raw=raw):
                result = observer.observe_cli_step(_expected("domain_error"), _observation(b'', raw, 2))
                self.assertFalse(result["assertions"]["stderr.error"])
                self.assertFalse(result["assertions"]["stderr.error_code"])

    def test_supported_wrong_code_and_wrong_channel_fail(self):
        result = observer.observe_cli_step(_expected("domain_error"),
            _observation(b'', b'{"error":"wrong"}', 2))
        self.assertTrue(result["assertions"]["stderr.error"])
        self.assertFalse(result["assertions"]["stderr.error_code"])
        result = observer.observe_cli_step(_expected("domain_error"),
            _observation(b'{"error":"not_found"}', b'', 2))
        self.assertFalse(result["assertions"]["stderr.error"])
        self.assertFalse(result["assertions"]["stderr.error_code"])

    def test_mixed_domain_error_framing_is_ungraded_without_substring_extraction(self):
        for raw in (b'log\n{"error":"not_found"}\n', b'{"error":"not_found"}\nlog',
                    b'{"error":"x","error":"not_found"}'):
            with self.subTest(raw=raw):
                result = observer.observe_cli_step(_expected("domain_error"), _observation(b'', raw, 2))
                self.assertIsNone(result["assertions"]["stderr.error"])
                self.assertIsNone(result["assertions"]["stderr.error_code"])
                self.assertEqual(result["status"], "observation-unavailable")

    def test_complete_prose_with_no_object_token_proves_required_message_absent(self):
        for raw in (b'traceback only', b'not_found', b'Error: no document exists'):
            with self.subTest(raw=raw):
                result = observer.observe_cli_step(_expected("domain_error"), _observation(b'', raw, 2))
                self.assertFalse(result["assertions"]["stderr.error"])
                self.assertFalse(result["assertions"]["stderr.error_code"])
        # An apparent record/diagnostic containing a brace remains ambiguous.
        result = observer.observe_cli_step(_expected("domain_error"),
            _observation(b'', b'diagnostic mentioning {not_found}', 2))
        self.assertIsNone(result["assertions"]["stderr.error"])

    def test_wrong_exit_survives_unavailable_error_framing(self):
        result = observer.observe_cli_step(_expected("domain_error"),
            _observation(b'', b'diagnostic\n{"error":"not_found"}', 0))
        self.assertFalse(result["assertions"]["process.exit"])
        self.assertIsNone(result["assertions"]["stderr.error"])
        self.assertEqual(result["status"], "product-failed")

    def test_unspecified_error_code_not_learned_from_candidate(self):
        result = observer.observe_cli_step(_expected("domain_error", code=None),
            _observation(b'', b'{"error":"new_code"}', 2))
        self.assertTrue(result["assertions"]["stderr.error"])
        self.assertIsNone(result["assertions"]["stderr.error_code"])
        self.assertEqual(result["assertion_dispositions"]["stderr.error_code"], "unspecified")

    def test_usage_only_grades_actual_exit(self):
        result = observer.observe_cli_step(_expected("usage_or_rejection"),
            _observation(b'arbitrary help text', b'this need not be JSON', 2))
        self.assertTrue(result["supported_assertions_passed"])
        self.assertEqual(set(result["assertions"]), {"process.exit", "usage.presentation"})

    def test_stream_limit_does_not_hide_known_wrong_exit(self):
        def truncated(t):
            t.update(status="output_limit", capture_complete=False)
            t["stdout"].update(complete=False, truncated=True, observed_bytes=100)
        result = observer.observe_cli_step(_expected(value={}), _observation(b'{', exit_code=7, edit=truncated))
        self.assertFalse(result["assertions"]["process.exit"])
        self.assertIsNone(result["assertions"]["stdout.json"])
        self.assertEqual(result["status"], "product-failed")

    def test_timeout_has_no_invented_latency_or_forced_exit_failure(self):
        def timeout(t):
            t.update(status="timeout", capture_complete=False, exit_code=None)
            t["completion"].update(natural=False, wait_status_code=137, inspect_exit_code=137)
            t["stdout"].update(complete=False)
            t["stderr"].update(complete=False)
        result = observer.observe_cli_step(_expected(value={}), _observation(b'{}', edit=timeout))
        self.assertTrue(all(value is None for value in result["assertions"].values()))
        self.assertEqual(result["status"], "observation-unavailable")

    def test_full_capture_after_kill_cannot_grade_expected_prefix_or_wrong_value(self):
        def killed(t):
            t.update(status="timeout", exit_code=None)
            t["completion"].update(natural=False, killed_by_controller=True,
                                   wait_status_code=137, inspect_exit_code=137)
        for raw in (b'{}', b'{"wrong":true}', b'not JSON'):
            with self.subTest(raw=raw):
                result = observer.observe_cli_step(_expected(value={}), _observation(raw, edit=killed))
                self.assertTrue(all(value is None for value in result["assertions"].values()))
                self.assertEqual(result["status"], "observation-unavailable")

    def test_identity_unproven_full_capture_cannot_establish_product_verdict(self):
        def wrong_identity(t):
            t.update(status="completion_unproven", exit_code=None)
            t["completion"].update(natural=False, identity_verified=False)
        result = observer.observe_cli_step(_expected(value={}),
                                           _observation(b'not JSON', exit_code=9, edit=wrong_identity))
        self.assertTrue(all(value is None for value in result["assertions"].values()))
        self.assertFalse(result["failed_assertion_ids"])

    def test_other_stream_limit_does_not_hide_complete_supported_stdout(self):
        def incomplete_stderr(t):
            t.update(status="transport_error", capture_complete=False)
            t["stderr"].update(complete=False)
        result = observer.observe_cli_step(_expected(value={}), _observation(edit=incomplete_stderr))
        self.assertTrue(result["all_local_assertions_passed"])
        self.assertFalse(result["production_acceptance_authority"])

    def test_lost_history_state_blocks_semantics_without_erasing_proven_exit(self):
        for code in (0, 9):
            with self.subTest(code=code):
                result = observer.observe_cli_step(_expected(value={}),
                    _observation(b'{"unexpected":true}', exit_code=code,
                                 edit=lambda t: t.update(history_state_verified=False)))
                self.assertEqual(result["assertions"]["process.exit"], code == 0)
                self.assertIsNone(result["assertions"]["stdout.json"])
                self.assertIsNone(result["assertions"]["stdout.value"])
        with self.assertRaises(observer.ObservationUnavailable):
            _observation(edit=lambda t: t.update(history_state_verified="yes"))

    def test_missing_step_preserves_exact_roster_unknown_and_unspecified(self):
        expected = _expected(supported=False)
        result = observer.observe_cli_step(expected, None)
        self.assertEqual(set(result["assertions"]), set(expected["assertion_ids"]))
        self.assertEqual(result["unavailable_assertion_ids"], ["process.exit", "stdout.json"])
        self.assertEqual(result["unspecified_assertion_ids"], ["stdout.wrapper"])
        self.assertFalse(result["supported_assertions_passed"])

    def test_expectation_census_missing_extra_duplicate_or_empty_fails_closed(self):
        for keys in ([], ["process.exit"], ["process.exit", "stdout.json", "stdout.value", "invented"],
                     ["process.exit", "stdout.json", "stdout.value", "process.exit"]):
            expected = _expected(value={})
            expected["assertion_ids"] = keys
            with self.subTest(keys=keys), self.assertRaises(observer.InvalidExpectation):
                observer.observe_cli_step(expected, None)
        expected = _expected("domain_error", code=None)
        expected["unspecified_assertion_ids"] = []
        with self.assertRaises(observer.InvalidExpectation):
            observer.observe_cli_step(expected, None)


if __name__ == "__main__":
    unittest.main()
