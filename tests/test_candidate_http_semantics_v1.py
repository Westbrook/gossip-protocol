"""Synthetic pure-value controls only; no source/runtime/acceptance authority."""

from dataclasses import fields
import json
import unittest

from gossip_harness.candidate_http_semantics_v1 import (
    Comparison, Expectation, Facet, JsonNumber, JsonObject, ListenerFacts, MAX_BODY_BYTES, Missing,
    ResponseFacts, classified_error, compare, parse_json, semantic_compare,
    success, unclassified_error,
)


def facts(body: bytes | Missing, status: int = 200, **kwargs: object) -> ResponseFacts:
    return ResponseFacts(status, (("Content-Type", "application/json"),), body, **kwargs)


def document(source: str, text: str) -> dict[str, str]:
    # Synthetic expected values: these are not claimed to prove ID derivation.
    return dict(document_id="doc-" + source, source_id="src-" + source,
                source=source, blob_id="blob-" + source, title=source, text=text)


def encoded(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False).encode("utf-8")


class HttpSemanticFacetsV1Tests(unittest.TestCase):
    def test_known_wrong_status_survives_unavailable_body(self) -> None:
        result = compare(facts(Missing("framing incomplete"), 500), success("health"))
        self.assertEqual(result.facet("status").disposition, "fail")
        self.assertEqual(result.facet("json_syntax").disposition, "unavailable")
        self.assertEqual(result.facet("body_shape_value").disposition, "unavailable")

    def test_alternative_serialization_is_semantically_equal(self) -> None:
        body = b' \r\n{ "schema" : 0e0, "status": "\\u006f\\u006b" } \t\n'
        result = compare(facts(body), success("health"))
        self.assertEqual(result.facet("body_shape_value").disposition, "pass")
        self.assertEqual(result.facet("json_syntax").disposition, "pass")

    def test_boolean_is_not_schema_zero(self) -> None:
        result = compare(facts(b'{"status":"ok","schema":false}'), success("health"))
        self.assertEqual(result.facet("body_shape_value").disposition, "fail")

    def test_extra_or_missing_field_fails_supported_closed_shape(self) -> None:
        for body in (b'{"status":"ok"}', b'{"status":"ok","schema":0,"other":1}'):
            with self.subTest(body=body):
                result = compare(facts(body), success("health"))
                self.assertEqual(result.facet("body_shape_value").disposition, "fail")

    def test_wrong_exact_value_fails(self) -> None:
        result = compare(facts(b'{"status":"bad","schema":0}'), success("health"))
        self.assertEqual(result.facet("body_shape_value").disposition, "fail")

    def test_duplicate_key_is_not_last_value_wins(self) -> None:
        body = b'{"schema":9,"schema":0,"status":"ok"}'
        result = compare(facts(body), success("health"))
        self.assertEqual(result.facet("body_shape_value").disposition, "unspecified")

    def test_duplicate_ambiguity_does_not_hide_wrong_sibling(self) -> None:
        body = b'{"schema":9,"schema":0,"status":"wrong"}'
        result = compare(facts(body), success("health"))
        self.assertEqual(result.facet("body_shape_value").disposition, "fail")

    def test_nested_wrapper_cannot_satisfy_exact_shape(self) -> None:
        result = compare(facts(b'{"data":{"status":"ok","schema":0}}'), success("health"))
        self.assertEqual(result.facet("body_shape_value").disposition, "fail")

    def test_unspecified_action_wrapper_is_not_graded_by_example(self) -> None:
        for body in (b'{"job":{"job_id":"x"}}', b'{"result":{"job_id":"x"}}', b'[]'):
            with self.subTest(body=body):
                result = compare(facts(body), success("unspecified"))
                self.assertEqual(result.facet("body_shape_value").disposition, "unspecified")
                self.assertEqual(result.facet("json_syntax").disposition, "pass")

    def test_unspecified_wrapper_does_not_hide_malformed_json(self) -> None:
        result = compare(facts(b'{"result":'), success("unspecified"))
        self.assertEqual(result.facet("json_syntax").disposition, "fail")
        self.assertEqual(result.facet("body_shape_value").disposition, "unspecified")

    def test_success_prefix_with_trailing_garbage_is_not_json(self) -> None:
        result = compare(facts(b'{"status":"ok","schema":0} garbage'), success("health"))
        self.assertEqual(result.facet("json_syntax").disposition, "fail")
        self.assertEqual(result.facet("body_shape_value").disposition, "unavailable")

    def test_nonfinite_constants_are_not_json(self) -> None:
        for constant in (b'NaN', b'Infinity', b'-Infinity'):
            with self.subTest(constant=constant):
                result = compare(facts(b'{"schema":' + constant + b'}'), success("health"))
                self.assertEqual(result.facet("json_syntax").disposition, "fail")

    def test_capture_allocation_limit_is_not_product_failure(self) -> None:
        result = compare(facts(b' ' * (MAX_BODY_BYTES + 1), 404), unclassified_error(404))
        self.assertEqual(result.facet("status").disposition, "pass")
        self.assertEqual(result.facet("json_syntax").disposition, "unavailable")
        self.assertEqual(result.facet("error_code").disposition, "unspecified")

    def test_unsupported_json_nesting_is_unavailable(self) -> None:
        result = compare(facts(b'[' * 2000 + b'0' + b']' * 2000), success("health"))
        self.assertEqual(result.facet("json_syntax").disposition, "unavailable")

    def test_list_array_order_and_multiplicity_are_preserved(self) -> None:
        docs = [document("a.txt", "a"), document("b.txt", "b")]
        expected = success("documents", encoded(dict(documents=docs, total=2)))
        for actual in (docs[::-1], [docs[0], docs[0]], docs[:1]):
            with self.subTest(actual=actual):
                result = compare(facts(encoded(dict(documents=actual, total=2))), expected)
                self.assertEqual(result.facet("body_shape_value").disposition, "fail")

    def test_document_preserves_exact_unicode_and_newlines(self) -> None:
        doc = document("a.txt", "\u00e9\r\n")
        expected = success("document", encoded(doc))
        for wrong in ("e\u0301\r\n", "\u00e9\n"):
            result = compare(facts(encoded(doc | {"text": wrong})), expected)
            self.assertEqual(result.facet("body_shape_value").disposition, "fail")
        alternative = json.dumps(doc, ensure_ascii=True, sort_keys=True, indent=3).encode()
        self.assertEqual(compare(facts(alternative), expected).facet("body_shape_value").disposition, "pass")

    def test_export_exact_format_and_no_extra_timestamps(self) -> None:
        value = dict(format="local-research-library-v0", documents=[])
        expected = success("export", encoded(value))
        for wrong in (value | {"format": "other"}, value | {"time": 0}):
            self.assertEqual(compare(facts(encoded(wrong)), expected).facet("body_shape_value").disposition, "fail")

    def test_jobs_order_fields_and_boolean_number_distinction(self) -> None:
        a = dict(job_id="a", epoch=1, state="queued", total=0, completed=0, error=None)
        b = a | {"job_id": "b"}
        expected = success("jobs", encoded(dict(jobs=[a, b])))
        for jobs in ([b, a], [a | {"epoch": True}, b], [a | {"extra": 1}, b]):
            result = compare(facts(encoded(dict(jobs=jobs))), expected)
            self.assertEqual(result.facet("body_shape_value").disposition, "fail")

    def test_named_error_code_and_status_are_independent(self) -> None:
        expected = classified_error("not_found")
        result = compare(facts(b'{"error":"invalid_request"}', 404), expected)
        self.assertEqual(result.facet("status").disposition, "pass")
        self.assertEqual(result.facet("body_shape_value").disposition, "pass")
        self.assertEqual(result.facet("error_code").disposition, "fail")
        self.assertEqual(result.facet("reported_code_status_relation").disposition, "fail")

    def test_inherited_and_added_conflict_statuses(self) -> None:
        for code in ("source_changed", "job_conflict", "job_state", "stale_epoch"):
            with self.subTest(code=code):
                result = compare(facts(encoded({"error": code}), 409), classified_error(code))
                for name in ("status", "error_code", "reported_code_status_relation"):
                    self.assertEqual(result.facet(name).disposition, "pass")

    def test_unknown_route_code_unspecified_does_not_hide_wrong_404(self) -> None:
        result = compare(facts(b'{"error":"no_such_route"}', 200), unclassified_error(404))
        self.assertEqual(result.facet("status").disposition, "fail")
        self.assertEqual(result.facet("error_code").disposition, "unspecified")

    def test_route_status_does_not_indirectly_assign_unspecified_code(self) -> None:
        result = compare(facts(b'{"error":"no_such_route"}', 404), unclassified_error(404))
        self.assertEqual(result.facet("status").disposition, "pass")
        self.assertEqual(result.facet("error_code").disposition, "unspecified")
        self.assertEqual(result.facet("reported_code_status_relation").disposition, "unspecified")

    def test_unknown_error_code_has_no_observed_allowlist(self) -> None:
        result = compare(facts(b'{"error":"new_validity_label"}', 400), unclassified_error(400))
        self.assertEqual(result.facet("body_shape_value").disposition, "pass")
        self.assertEqual(result.facet("error_code").disposition, "unspecified")
        self.assertEqual(result.facet("reported_code_status_relation").disposition, "pass")

    def test_unresolved_fixed_status_can_still_fail_code_status_relation(self) -> None:
        result = compare(facts(b'{"error":"not_found"}', 400), unclassified_error())
        self.assertEqual(result.facet("status").disposition, "unspecified")
        self.assertEqual(result.facet("reported_code_status_relation").disposition, "fail")

    def test_error_envelope_shape_does_not_recursively_extract(self) -> None:
        for body in (b'{"data":{"error":"not_found"}}', b'{"error":1}', b'{"error":"not_found","extra":0}'):
            with self.subTest(body=body):
                result = compare(facts(body, 404), classified_error("not_found"))
                self.assertEqual(result.facet("body_shape_value").disposition, "fail")

    def test_wrong_envelope_does_not_hide_readable_root_code(self) -> None:
        result = compare(facts(b'{"error":"invalid_request","extra":0}', 404), classified_error("not_found"))
        self.assertEqual(result.facet("body_shape_value").disposition, "fail")
        self.assertEqual(result.facet("error_code").disposition, "fail")
        self.assertEqual(result.facet("reported_code_status_relation").disposition, "fail")

    def test_duplicate_error_code_remains_ambiguous(self) -> None:
        result = compare(facts(b'{"error":"bad","error":"not_found"}', 404), classified_error("not_found"))
        self.assertEqual(result.facet("body_shape_value").disposition, "unspecified")
        self.assertEqual(result.facet("error_code").disposition, "unavailable")

    def test_response_headers_are_not_new_product_norms(self) -> None:
        for headers in ((), (("content-type", "text/plain"),), (("CONTENT-TYPE", "Application/JSON; charset=utf-8"),)):
            result = compare(ResponseFacts(200, headers, b'{"status":"ok","schema":0}'), success("health"))
            self.assertEqual(result.facet("response_media_type").disposition, "unspecified")
            self.assertEqual(result.facet("body_shape_value").disposition, "pass")

    def test_content_encoding_refusal_is_an_observation_limit(self) -> None:
        result = compare(ResponseFacts(500, (("Content-Encoding", "gzip"),), b'\x1f\x8b...'), success("health"))
        self.assertEqual(result.facet("status").disposition, "fail")
        self.assertEqual(result.facet("json_syntax").disposition, "unavailable")
        self.assertEqual(result.facet("body_shape_value").disposition, "unavailable")

    def test_missing_headers_do_not_turn_unparsed_body_into_syntax_failure(self) -> None:
        result = compare(ResponseFacts(200, Missing("header capture incomplete"), b'\x1f\x8b...'), success("health"))
        self.assertEqual(result.facet("response_media_type").disposition, "unavailable")
        self.assertEqual(result.facet("json_syntax").disposition, "unavailable")

    def test_explicit_identity_content_coding_is_supported(self) -> None:
        result = compare(ResponseFacts(200, (("Content-Encoding", "Identity"),), b'{"status":"ok","schema":0}'), success("health"))
        self.assertEqual(result.facet("body_shape_value").disposition, "pass")

    def test_listener_fail_survives_partial_table_and_missing_body(self) -> None:
        result = compare(facts(Missing("timeout"), listener_before=ListenerFacts(("0.0.0.0",), "IPv6 unavailable")), success("health"))
        self.assertEqual(result.facet("listener_before").disposition, "fail")
        self.assertEqual(result.facet("json_syntax").disposition, "unavailable")

    def test_listener_reachability_or_incomplete_legal_snapshot_not_enough(self) -> None:
        result = compare(facts(b'{"status":"ok","schema":0}', listener_before=ListenerFacts(("127.0.0.1",), "IPv6 unavailable")), success("health"))
        self.assertEqual(result.facet("listener_before").disposition, "unavailable")
        self.assertEqual(result.facet("listener_after").disposition, "unavailable")

    def test_listener_snapshots_separate_and_ipv6_not_ipv4_loopback(self) -> None:
        result = compare(facts(b'{}', listener_before=ListenerFacts(("127.0.0.1",)), listener_after=ListenerFacts(("::1",))), success("health"))
        self.assertEqual(result.facet("listener_before").disposition, "pass")
        self.assertEqual(result.facet("listener_after").disposition, "fail")
        self.assertEqual(result.facet("body_shape_value").disposition, "fail")

    def test_numeric_lexemes_retained_without_backporting_future_policy(self) -> None:
        number = parse_json(b'1.0e0')
        self.assertIsInstance(number, JsonNumber)
        assert isinstance(number, JsonNumber)
        self.assertEqual(number.text, "1.0e0")
        self.assertFalse(number.integer_token)
        self.assertEqual(semantic_compare(number, parse_json(b'1')), "pass")
        self.assertEqual(semantic_compare(parse_json(b'true'), parse_json(b'1')), "fail")

    def test_input_expectations_cannot_assign_unsupported_wrappers_or_codes(self) -> None:
        with self.assertRaises(ValueError):
            success("unspecified", b'{}')
        with self.assertRaises(ValueError):
            success("document", b'{"document":{"text":"x"}}')
        with self.assertRaises(ValueError):
            classified_error("arbitrary_observed_code")
        with self.assertRaises(ValueError):
            semantic_compare(parse_json(b'{"x":1}'), parse_json(b'{"x":1,"x":2}'))

    def test_direct_expectation_constructor_cannot_bypass_source_guards(self) -> None:
        for args in (("health", 200, b'{"invented":true}'),
                     ("error", 418, None, "arbitrary_observed_code"),
                     ("error", 400, None, "not_found"),
                     ("unspecified", 200, b'{}'),
                     ("jobs", 200, b'{"data":[]}')):
            with self.subTest(args=args), self.assertRaises(ValueError):
                Expectation(*args)

    def test_direct_health_expectation_rejects_mutable_body(self) -> None:
        with self.assertRaises(ValueError):
            Expectation("health", 200, bytearray(b'{"status":"ok","schema":0}'))

    def test_value_facts_are_never_acceptance_authority(self) -> None:
        result = compare(facts(b'{"status":"ok","schema":0}'), success("health"))
        self.assertIsInstance(result, Comparison)
        self.assertEqual(result.product_verdict, "not_evaluated")
        self.assertEqual(result.authority, "pure_value_comparison_only")
        self.assertNotIn("authenticated", {field.name for field in fields(ResponseFacts)})
        self.assertIsInstance(parse_json(b'{"x":1,"x":2}'), JsonObject)


    def test_missing_reason_rejects_mutable_or_nonstring_values(self) -> None:
        for reason in ([], ["unknown"], 1, True, ""):
            with self.subTest(reason=reason), self.assertRaises(ValueError):
                Missing(reason)

    def test_final_status_rejects_informational_bool_and_float(self) -> None:
        for status in (100, 101, 103, 199, True, 200.0, "200", 600):
            with self.subTest(status=status), self.assertRaises(ValueError):
                ResponseFacts(status, (), b"{}")
        for status in (200, 404, 599, Missing("no final response")):
            self.assertEqual(ResponseFacts(status, (), b"{}").status, status)

    def test_header_facts_reject_outer_or_nested_mutability(self) -> None:
        for headers in ([], [("x", "y")], (["x", "y"],), (("x", []),), (("x",),), ((1, "y"),)):
            with self.subTest(headers=headers), self.assertRaises(TypeError):
                ResponseFacts(200, headers, b"{}")
        self.assertEqual(ResponseFacts(200, (("x", "y"),), b"{}").headers, (("x", "y"),))

    def test_body_facts_reject_mutable_or_nonbyte_values(self) -> None:
        for body in (bytearray(b"{}"), memoryview(b"{}"), "{}", {}):
            with self.subTest(body=body), self.assertRaises(TypeError):
                ResponseFacts(200, (), body)

    def test_listener_facts_require_immutable_typed_fields(self) -> None:
        for addresses in (["127.0.0.1"], (["127.0.0.1"],), (1,), "127.0.0.1"):
            with self.subTest(addresses=addresses), self.assertRaises(TypeError):
                ListenerFacts(addresses)
        for limitation in ([], ["missing"], False, 0, ""):
            with self.subTest(limitation=limitation), self.assertRaises(ValueError):
                ListenerFacts(("127.0.0.1",), limitation)

    def test_response_listener_slots_reject_untyped_payloads(self) -> None:
        for value in ([], {}, ("127.0.0.1",), True):
            for slot in ("listener_before", "listener_after"):
                with self.subTest(value=value, slot=slot), self.assertRaises(TypeError):
                    ResponseFacts(200, (), b"{}", **{slot: value})

    def test_comparison_constructor_cannot_override_authority_labels(self) -> None:
        for option, value in (("protocol", "other"), ("authority", "authenticated"),
                              ("product_verdict", "pass")):
            with self.subTest(option=option), self.assertRaises(TypeError):
                Comparison((), **{option: value})
        result = Comparison(())
        self.assertEqual(result.protocol, "candidate-http-semantics-v1")
        self.assertEqual(result.authority, "pure_value_comparison_only")
        self.assertEqual(result.product_verdict, "not_evaluated")

    def test_comparison_and_facet_records_reject_nested_mutability(self) -> None:
        for citations in (["V0-HTTP"], (["V0-HTTP"],), (1,)):
            with self.subTest(citations=citations), self.assertRaises(TypeError):
                Facet("status", "pass", "reason", citations)
        row = Facet("status", "pass", "reason", ("V0-HTTP",))
        for rows in ([row], ({"name": "status"},)):
            with self.subTest(rows=rows), self.assertRaises(TypeError):
                Comparison(rows)
        with self.assertRaises(ValueError):
            Comparison((row, row))

    def test_expectation_constructor_rejects_untyped_status_and_code(self) -> None:
        with self.assertRaises(TypeError):
            Expectation("health", 200.0, b'{"status":"ok","schema":0}')
        with self.assertRaises(TypeError):
            Expectation("error", 400, None, ["invalid_request"])
        with self.assertRaises(ValueError):
            Expectation(["health"], 200, b'{"status":"ok","schema":0}')


if __name__ == "__main__":
    unittest.main()
