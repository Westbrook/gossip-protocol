"""Adversarial pure-value controls for fixed prospective HTTP error relations.

Synthetic values only: no candidate/reference imports or execution evidence.
Every required envelope, literal census and equality facet remains separate.
"""
from __future__ import annotations

from dataclasses import FrozenInstanceError, fields, replace
import hashlib
import json
import unittest

from gossip_harness import candidate_http_relations_v1 as relation
from gossip_harness import candidate_http_semantics_v1 as sem


def encoded(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def observed(step: str, body: bytes | sem.Missing, status: int | sem.Missing = 200,
             headers: tuple[tuple[str, str], ...] | sem.Missing = ()) -> relation.ObservedResponse:
    return relation.ObservedResponse(step, sem.ResponseFacts(status, headers, body))


def declared() -> relation.JobsExpectation:
    return relation.JobsExpectation("census-after", (
        relation.JobRule("sentinel", 1, "queued", 0, 0, None),
        relation.JobRule("subject", 2, "failed", 2, 0, relation.ErrorReference("prepare-rejection")),
        relation.JobRule("untouched", 3, "failed", 1, 0, "prior_failure"),
    ))


def jobs(code: object = "key_bound") -> list[dict[str, object]]:
    return [
        {"job_id": "sentinel", "epoch": 1, "state": "queued", "total": 0, "completed": 0, "error": None},
        {"job_id": "subject", "epoch": 2, "state": "failed", "total": 2, "completed": 0, "error": code},
        {"job_id": "untouched", "epoch": 3, "state": "failed", "total": 1, "completed": 0, "error": "prior_failure"},
    ]


def compare_bodies(census: bytes | sem.Missing, error: bytes | sem.Missing = b'{"error":"key_bound"}',
                   *, census_status: int | sem.Missing = 200, error_status: int | sem.Missing = 400,
                   census_headers: tuple[tuple[str, str], ...] | sem.Missing = (),
                   error_headers: tuple[tuple[str, str], ...] | sem.Missing = ()) -> relation.RelationDiagnostic:
    return relation.compare(declared(), (
        observed("census-after", census, census_status, census_headers),
        observed("prepare-rejection", error, error_status, error_headers),
    ))


def disposition(result: relation.RelationDiagnostic, name: str = "body_shape_value") -> str:
    return result.jobs.facet(name).disposition


def equality(result: relation.RelationDiagnostic) -> str:
    return result.relations[0].facet.disposition


class HttpRelationDeclarationsV1Tests(unittest.TestCase):
    def test_declaration_record_contains_only_literal_fields_and_fixed_relation(self) -> None:
        expected = declared()
        manual = {"protocol": "candidate-http-relations-v1", "kind": "jobs-with-related-errors",
                  "jobs_step_id": "census-after", "jobs": [
            {"job_id": "sentinel", "epoch": 1, "state": "queued", "total": 0, "completed": 0, "error": None},
            {"job_id": "subject", "epoch": 2, "state": "failed", "total": 2, "completed": 0,
             "error": {"from_error_step": "prepare-rejection", "expected_status": 400}},
            {"job_id": "untouched", "epoch": 3, "state": "failed", "total": 1, "completed": 0, "error": "prior_failure"},
        ]}
        self.assertEqual(expected.record(), manual)
        canonical = json.dumps(manual, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")
        self.assertEqual(expected.sha256, hashlib.sha256(canonical).hexdigest())
        changed = expected.record()
        changed["jobs"][1]["error"]["from_error_step"] = "changed"
        self.assertEqual(expected.record(), manual)

    def test_hash_binds_source_destination_subject_status_and_literal_state(self) -> None:
        expected = declared()
        subject = expected.jobs[1]
        alternatives = (
            replace(expected, step_id="another-census"),
            replace(expected, jobs=(expected.jobs[0], replace(subject, error=relation.ErrorReference("another-error")), expected.jobs[2])),
            replace(expected, jobs=(expected.jobs[0], replace(subject, error=relation.ErrorReference("prepare-rejection", None)), expected.jobs[2])),
            replace(expected, jobs=(expected.jobs[0], replace(subject, job_id="subject2"), expected.jobs[2])),
            replace(expected, jobs=(expected.jobs[0], replace(subject, epoch=3), expected.jobs[2])),
        )
        self.assertEqual(len({expected.sha256, *(item.sha256 for item in alternatives)}), 6)
        before = expected.sha256
        for code in ("brand_new_code", "another_code", ""):
            result = relation.compare(expected, (observed("census-after", encoded({"jobs": jobs(code)})),
                observed("prepare-rejection", encoded({"error": code}), 400)))
            self.assertEqual(equality(result), "pass")
            self.assertEqual(expected.sha256, before)
            self.assertEqual(expected.jobs[1].error, relation.ErrorReference("prepare-rejection"))

    def test_immutable_objects_and_diagnostic_authority_are_fixed(self) -> None:
        expected = declared()
        result = compare_bodies(encoded({"jobs": jobs()}))
        for value, field, replacement in ((expected, "step_id", "other"),
                                         (expected.jobs[1], "error", "observed"),
                                         (result, "acceptance_authority", True),
                                         (result.relations[0], "job_id", "different")):
            with self.subTest(field=field), self.assertRaises(FrozenInstanceError):
                setattr(value, field, replacement)
        self.assertEqual(result.protocol, "candidate-http-relations-v1")
        with self.assertRaises(ValueError):
            replace(result, protocol="different-protocol")
        self.assertEqual(result.authority, "supplied_values_only")
        for field in ("acceptance_authority", "fresh_execution", "chronology_verified"):
            self.assertIs(getattr(result, field), False)
            with self.assertRaises(ValueError):
                replace(result, **{field: True})
        with self.assertRaises(ValueError):
            replace(result, authority="trusted")
        self.assertEqual(result.jobs.product_verdict, "not_evaluated")
        self.assertEqual(result.sources[0].comparison.product_verdict, "not_evaluated")
        self.assertNotIn("accepted", {field.name for field in fields(result)})
        self.assertIs(type(result.sources), tuple)
        self.assertIs(type(result.relations), tuple)

    def test_declaration_rejects_unsorted_duplicate_empty_or_mutable_jobs(self) -> None:
        rules = declared().jobs
        for invalid in ((), list(rules), tuple(reversed(rules)), (rules[1], rules[1])):
            with self.subTest(kind=type(invalid), length=len(invalid)), self.assertRaises(relation.RelationError):
                relation.JobsExpectation("census-after", invalid)
        with self.assertRaises(relation.RelationError):
            relation.JobsExpectation("census-after", (rules[0],))

    def test_evaluator_declaration_count_bound_has_no_other_invalidity(self) -> None:
        # This is an authoring allocation bound, not a product limit on persisted jobs.
        rules = (relation.JobRule("job0000", 1, "failed", 0, 0, relation.ErrorReference("source")),) + tuple(
            relation.JobRule(f"job{i:04d}", 1, "queued", 0, 0, None) for i in range(1, relation.MAX_JOBS + 1))
        ids = tuple(rule.job_id for rule in rules)
        self.assertEqual(ids, tuple(sorted(set(ids))))
        self.assertEqual(len(rules), relation.MAX_JOBS + 1)
        within = relation.JobsExpectation("census", rules[:-1])
        self.assertEqual(len(within.jobs), relation.MAX_JOBS)
        with self.assertRaises(relation.RelationError):
            relation.JobsExpectation("census", rules)

    def test_references_reject_self_alias_conflicting_status_and_bad_identifiers(self) -> None:
        subject = declared().jobs[1]
        with self.assertRaises(relation.RelationError):
            relation.JobsExpectation("census-after", (replace(subject, error=relation.ErrorReference("census-after")),))
        with self.assertRaises(relation.RelationError):
            relation.JobsExpectation("census-after", (
                replace(subject, job_id="a", error=relation.ErrorReference("same", 400)),
                replace(subject, job_id="b", error=relation.ErrorReference("same", 404))))
        for invalid in ("", "has space", "x" * 257, "../x", True):
            with self.subTest(step=invalid), self.assertRaises(relation.RelationError):
                relation.ErrorReference(invalid)
        for invalid_status in (True, "400", 409, 500):
            with self.subTest(status=invalid_status), self.assertRaises(relation.RelationError):
                relation.ErrorReference("source", invalid_status)
        for status in (None, 400, 404):
            self.assertEqual(relation.ErrorReference("source", status).expected_status, status)

    def test_rule_checks_are_authoring_constraints_not_response_numeric_dialect(self) -> None:
        subject = declared().jobs[1]
        for changed in ({"epoch": True}, {"epoch": 0}, {"total": False}, {"completed": True},
                        {"completed": 1}, {"error": None}, {"job_id": "é"}, {"state": "unknown"}):
            with self.subTest(changed=changed), self.assertRaises(relation.RelationError):
                replace(subject, **changed)
        with self.assertRaises(relation.RelationError):
            relation.JobRule("queued", 1, "queued", 0, 0, relation.ErrorReference("source"))
        self.assertEqual(replace(subject, error="").error, "")
        self.assertEqual(relation.JobRule("done", 3, "completed", 2, 2, None).completed, 2)

    def test_supplied_steps_must_be_unique_declared_and_immutable(self) -> None:
        census = observed("census-after", encoded({"jobs": jobs()}))
        source = observed("prepare-rejection", b'{"error":"key_bound"}', 400)
        for invalid in ([census, source], (census, source, census),
                        (census, source, observed("unknown", b"{}")), (object(),)):
            with self.subTest(kind=type(invalid), size=len(invalid)), self.assertRaises(relation.RelationError):
                relation.compare(declared(), invalid)
        with self.assertRaises(relation.RelationError):
            relation.ObservedResponse("source", {"status": 400})
        self.assertEqual(relation.compare(declared(), (source, census)), relation.compare(declared(), (census, source)))

    def test_diagnostic_subject_and_source_identities_cannot_be_contradictory(self) -> None:
        result = compare_bodies(encoded({"jobs": jobs()}))
        with self.assertRaises(relation.RelationError):
            replace(result, sources=(result.sources[0], result.sources[0]))
        with self.assertRaises(relation.RelationError):
            replace(result, relations=(result.relations[0], result.relations[0]))
        with self.assertRaises(relation.RelationError):
            replace(result, jobs_step_id="unrelated-census")
        with self.assertRaises(relation.RelationError):
            replace(result.relations[0], source_step_id="census-after")
        with self.assertRaises(relation.RelationError):
            replace(result.relations[0], facet=sem.Facet("not_equality", "pass", "", ()))


class HttpRelationComparisonsV1Tests(unittest.TestCase):
    def test_exact_jobs_source_and_equality_have_separate_success_facets(self) -> None:
        result = compare_bodies(encoded({"jobs": jobs()}))
        self.assertEqual((result.jobs_step_id, len(result.sources), len(result.relations)), ("census-after", 1, 1))
        for name in ("status", "json_syntax", "body_shape_value"):
            self.assertEqual(disposition(result, name), "pass")
            self.assertEqual(result.sources[0].comparison.facet(name).disposition, "pass")
        self.assertEqual(result.sources[0].comparison.facet("error_code").disposition, "unspecified")
        self.assertEqual(result.sources[0].comparison.facet("reported_code_status_relation").disposition, "pass")
        self.assertEqual((result.relations[0].source_step_id, result.relations[0].jobs_step_id,
                          result.relations[0].job_id, equality(result)),
                         ("prepare-rejection", "census-after", "subject", "pass"))

    def test_any_string_code_including_empty_is_eligible_without_allowlist(self) -> None:
        for code in ("", "previously_unseen", "é\\n☃", "punctuation []{}", "embedded\0nul"):
            with self.subTest(code=code):
                result = compare_bodies(encoded({"jobs": jobs(code)}), encoded({"error": code}))
                self.assertEqual(disposition(result), "pass")
                self.assertEqual(result.sources[0].comparison.facet("body_shape_value").disposition, "pass")
                self.assertEqual(result.sources[0].comparison.facet("error_code").disposition, "unspecified")
                self.assertEqual(equality(result), "pass")

    def test_mismatch_fails_relation_without_changing_literal_census_or_source_code_facet(self) -> None:
        before = declared().record()
        result = compare_bodies(encoded({"jobs": jobs("different")}))
        self.assertEqual(disposition(result), "pass")
        self.assertEqual(equality(result), "fail")
        self.assertEqual(result.sources[0].comparison.facet("error_code").disposition, "unspecified")
        self.assertEqual(declared().record(), before)
        self.assertEqual(equality(compare_bodies(encoded({"jobs": jobs()}), b'{"error":"different"}')), "fail")

    def test_every_literal_subject_field_remains_exact(self) -> None:
        for field, wrong in (("job_id", "other"), ("epoch", 3), ("state", "running"), ("total", 3), ("completed", 1)):
            with self.subTest(field=field):
                census = jobs()
                census[1][field] = wrong
                result = compare_bodies(encoded({"jobs": census}))
                self.assertEqual(disposition(result), "fail")
                self.assertEqual(equality(result), "unavailable" if field == "job_id" else "pass")

    def test_all_unaffected_jobs_and_literal_error_slots_remain_exact(self) -> None:
        for index, field, wrong in ((0, "epoch", 2), (0, "state", "running"), (0, "total", 1),
                                    (0, "completed", 1), (0, "error", "invented"),
                                    (2, "error", "changed"), (2, "epoch", 4), (2, "job_id", "wrong")):
            with self.subTest(index=index, field=field):
                census = jobs()
                census[index][field] = wrong
                result = compare_bodies(encoded({"jobs": census}))
                self.assertEqual(disposition(result), "fail")
                self.assertEqual(equality(result), "pass")

    def test_job_order_multiplicity_count_and_closed_fields_are_independent(self) -> None:
        base = jobs()
        cases = (base[::-1], base[:-1], base + [base[0]], [base[0], base[1], base[1]],
                 [base[0], base[1] | {"extra": 0}, base[2]],
                 [base[0], {k: v for k, v in base[1].items() if k != "total"}, base[2]])
        for census in cases:
            with self.subTest(census=census):
                self.assertEqual(disposition(compare_bodies(encoded({"jobs": census}))), "fail")
        self.assertEqual(disposition(compare_bodies(encoded({"jobs": base, "extra": 0}))), "fail")
        for raw in (b"[]", b"null", b'{"jobs":{}}', b'{"other":[]}'):
            with self.subTest(raw=raw):
                result = compare_bodies(raw)
                self.assertEqual(disposition(result), "fail")
                self.assertEqual(equality(result), "unavailable")

    def test_legal_numeric_spelling_key_order_and_escaping_are_equal(self) -> None:
        raw = encoded({"jobs": jobs("é")}).replace(b'"epoch":2', b'"epoch":2e0').replace(b'"total":2', b'"total":2.0')
        raw = raw.replace(b'"completed":0', b'"completed":-0.0').replace("é".encode(), b"\\u00e9")
        result = compare_bodies(b" \r\n" + raw + b"\t", b'{ "error" : "\\u00e9" }')
        self.assertEqual(disposition(result), "pass")
        self.assertEqual(equality(result), "pass")
        reordered = [{key: job[key] for key in reversed(tuple(job))} for job in jobs()]
        self.assertEqual(disposition(compare_bodies(encoded({"jobs": reordered}))), "pass")

    def test_boolean_fields_are_not_numbers_and_related_errors_are_strings(self) -> None:
        for field, wrong in (("epoch", True), ("total", True), ("completed", False),
                              ("error", None), ("error", 0), ("error", False), ("error", {})):
            with self.subTest(field=field, wrong=wrong):
                census = jobs()
                census[1][field] = wrong
                result = compare_bodies(encoded({"jobs": census}))
                self.assertEqual(disposition(result), "fail")
                self.assertEqual(equality(result), "unavailable" if field == "error" else "pass")

    def test_wrong_source_status_and_envelope_cannot_be_hidden_by_equality(self) -> None:
        result = compare_bodies(encoded({"jobs": jobs()}), b'{"error":"key_bound","extra":0}', error_status=500)
        self.assertEqual(equality(result), "pass")
        source = result.sources[0].comparison
        self.assertEqual(source.facet("status").disposition, "fail")
        self.assertEqual(source.facet("body_shape_value").disposition, "fail")
        self.assertEqual(source.facet("reported_code_status_relation").disposition, "fail")
        self.assertEqual(disposition(result), "pass")
        result = compare_bodies(encoded({"jobs": jobs()}), census_status=500)
        self.assertEqual(disposition(result, "status"), "fail")
        self.assertEqual(disposition(result), "pass")
        self.assertEqual(equality(result), "pass")

    def test_generic_code_status_relation_does_not_assign_the_error_code(self) -> None:
        for code, actual_status in (("not_found", 404), ("stale_epoch", 409), ("unknown", 400)):
            with self.subTest(code=code):
                expected = relation.JobsExpectation("census-after", (
                    relation.JobRule("subject", 2, "failed", 2, 0, relation.ErrorReference("prepare-rejection", None)),))
                result = relation.compare(expected, (
                    observed("census-after", encoded({"jobs": [jobs(code)[1]]})),
                    observed("prepare-rejection", encoded({"error": code}), actual_status)))
                source = result.sources[0].comparison
                self.assertEqual(source.facet("status").disposition, "unspecified")
                self.assertEqual(source.facet("error_code").disposition, "unspecified")
                self.assertEqual(source.facet("reported_code_status_relation").disposition, "pass")
                self.assertEqual(equality(result), "pass")
        result = compare_bodies(encoded({"jobs": jobs("stale_epoch")}), b'{"error":"stale_epoch"}')
        self.assertEqual(result.sources[0].comparison.facet("status").disposition, "pass")
        self.assertEqual(result.sources[0].comparison.facet("reported_code_status_relation").disposition, "fail")
        self.assertEqual(equality(result), "pass")

    def test_fixed404_source_retains_explicit_route_override(self) -> None:
        expected = relation.JobsExpectation("census-after", (
            relation.JobRule("subject", 2, "failed", 2, 0, relation.ErrorReference("route-error", 404)),))
        result = relation.compare(expected, (observed("census-after", encoded({"jobs": [jobs("unknown_route")[1]]})),
                                           observed("route-error", b'{"error":"unknown_route"}', 404)))
        self.assertEqual(result.sources[0].comparison.facet("status").disposition, "pass")
        self.assertEqual(result.sources[0].comparison.facet("reported_code_status_relation").disposition, "unspecified")
        self.assertEqual(equality(result), "pass")

    def test_missing_steps_are_unavailable_without_erasing_other_facts(self) -> None:
        result = relation.compare(declared(), ())
        self.assertEqual(disposition(result), "unavailable")
        self.assertEqual(disposition(result, "status"), "unavailable")
        self.assertEqual(equality(result), "unavailable")
        only_census = relation.compare(declared(), (observed("census-after", encoded({"jobs": jobs()})),))
        self.assertEqual(disposition(only_census), "pass")
        self.assertEqual(equality(only_census), "unavailable")
        only_source = relation.compare(declared(), (observed("prepare-rejection", b'{"error":"key_bound"}', 500),))
        self.assertEqual(only_source.sources[0].comparison.facet("status").disposition, "fail")
        self.assertEqual(equality(only_source), "unavailable")

    def test_missing_status_does_not_erase_independently_readable_code_equality(self) -> None:
        result = compare_bodies(encoded({"jobs": jobs()}), census_status=sem.Missing("status capture missing"),
                                error_status=sem.Missing("status capture missing"))
        self.assertEqual(disposition(result, "status"), "unavailable")
        self.assertEqual(disposition(result), "pass")
        source = result.sources[0].comparison
        self.assertEqual(source.facet("status").disposition, "unavailable")
        self.assertEqual(source.facet("body_shape_value").disposition, "pass")
        self.assertEqual(source.facet("reported_code_status_relation").disposition, "unavailable")
        self.assertEqual(equality(result), "pass")

    def test_complete_malformed_json_is_failed_syntax_but_not_fabricated_equality(self) -> None:
        for raw in (b'{"jobs":', b'{"jobs":[]} trailing', b"NaN"):
            with self.subTest(census=raw):
                result = compare_bodies(raw, census_status=500)
                self.assertEqual(disposition(result, "status"), "fail")
                self.assertEqual(disposition(result, "json_syntax"), "fail")
                self.assertEqual(disposition(result), "unavailable")
                self.assertEqual(equality(result), "unavailable")
        for raw in (b'{"error":', b'{"error":"key_bound"} trailing', b"Infinity"):
            with self.subTest(source=raw):
                result = compare_bodies(encoded({"jobs": jobs()}), raw)
                self.assertEqual(result.sources[0].comparison.facet("json_syntax").disposition, "fail")
                self.assertEqual(disposition(result), "pass")
                self.assertEqual(equality(result), "unavailable")

    def test_incomplete_bodies_keep_wrong_status_without_product_syntax_failure(self) -> None:
        missing = sem.Missing("incomplete content length")
        result = compare_bodies(missing, error_status=500, census_status=500)
        self.assertEqual(disposition(result, "status"), "fail")
        self.assertEqual(disposition(result, "json_syntax"), "unavailable")
        self.assertEqual(equality(result), "unavailable")
        result = compare_bodies(encoded({"jobs": jobs()}), missing, error_status=500)
        self.assertEqual(result.sources[0].comparison.facet("status").disposition, "fail")
        self.assertEqual(result.sources[0].comparison.facet("json_syntax").disposition, "unavailable")
        self.assertEqual(disposition(result), "pass")
        self.assertEqual(equality(result), "unavailable")

    def test_missing_or_unsupported_coding_metadata_blocks_only_affected_decoding(self) -> None:
        for headers in (sem.Missing("headers incomplete"), (("Content-Encoding", "gzip"),),
                        (("Content-Encoding", "identity, br"),)):
            with self.subTest(headers=headers):
                source_limited = compare_bodies(encoded({"jobs": jobs()}), error_headers=headers, error_status=500)
                self.assertEqual(source_limited.sources[0].comparison.facet("status").disposition, "fail")
                self.assertEqual(source_limited.sources[0].comparison.facet("json_syntax").disposition, "unavailable")
                self.assertEqual(disposition(source_limited), "pass")
                self.assertEqual(equality(source_limited), "unavailable")
                census_limited = compare_bodies(encoded({"jobs": jobs()}), census_headers=headers, census_status=500)
                self.assertEqual(disposition(census_limited, "status"), "fail")
                self.assertEqual(disposition(census_limited), "unavailable")
                self.assertEqual(equality(census_limited), "unavailable")
        supported = compare_bodies(encoded({"jobs": jobs()}), census_headers=(("Content-Encoding", "Identity"),),
                                   error_headers=(("content-encoding", "identity"),))
        self.assertEqual((disposition(supported), equality(supported)), ("pass", "pass"))

    def test_response_content_type_is_not_an_invented_constraint(self) -> None:
        result = compare_bodies(encoded({"jobs": jobs()}), census_headers=(("Content-Type", "text/plain"),),
                                error_headers=(("Content-Type", "application/octet-stream"),))
        self.assertEqual(disposition(result, "response_media_type"), "unspecified")
        self.assertEqual((disposition(result), equality(result)), ("pass", "pass"))

    def test_body_allocation_and_utf8_limits_leave_relation_unavailable(self) -> None:
        for raw in (b" " * (sem.MAX_BODY_BYTES + 1), b"\xff", b"[" * 2000 + b"0" + b"]" * 2000):
            with self.subTest(length=len(raw)):
                result = compare_bodies(encoded({"jobs": jobs()}), raw, error_status=500)
                self.assertEqual(result.sources[0].comparison.facet("status").disposition, "fail")
                self.assertEqual(result.sources[0].comparison.facet("json_syntax").disposition, "unavailable")
                self.assertEqual(equality(result), "unavailable")

    def test_duplicate_source_error_keys_are_not_last_key_wins(self) -> None:
        for raw in (b'{"error":"wrong","error":"key_bound"}', b'{"error":"key_bound","error":"wrong"}',
                    b'{"error":"key_bound","error":"key_bound"}'):
            with self.subTest(raw=raw):
                result = compare_bodies(encoded({"jobs": jobs()}), raw, error_status=500)
                self.assertEqual(result.sources[0].comparison.facet("body_shape_value").disposition, "unspecified")
                self.assertEqual(result.sources[0].comparison.facet("status").disposition, "fail")
                self.assertEqual(equality(result), "unavailable")

    def test_duplicate_subject_error_keys_preserve_ambiguity_and_wrong_siblings(self) -> None:
        raw = encoded({"jobs": jobs()}).replace(b'"error":"key_bound"', b'"error":"wrong","error":"key_bound"')
        result = compare_bodies(raw)
        self.assertEqual((disposition(result), equality(result)), ("unspecified", "unavailable"))
        result = compare_bodies(raw.replace(b'"epoch":2', b'"epoch":7'))
        self.assertEqual((disposition(result), equality(result)), ("fail", "unavailable"))
        result = compare_bodies(raw.replace(b'"prior_failure"', b'"wrong"'))
        self.assertEqual((disposition(result), equality(result)), ("fail", "unavailable"))

    def test_duplicate_subject_id_and_duplicate_literal_field_are_distinct(self) -> None:
        raw = encoded({"jobs": jobs()})
        ambiguous_id = raw.replace(b'"job_id":"subject"', b'"job_id":"wrong","job_id":"subject"')
        result = compare_bodies(ambiguous_id)
        self.assertEqual((disposition(result), equality(result)), ("unspecified", "unavailable"))
        ambiguous_epoch = raw.replace(b'"epoch":2', b'"epoch":3,"epoch":2')
        result = compare_bodies(ambiguous_epoch)
        self.assertEqual((disposition(result), equality(result)), ("unspecified", "pass"))
        result = compare_bodies(ambiguous_epoch.replace(b'"total":2', b'"total":9'))
        self.assertEqual((disposition(result), equality(result)), ("fail", "pass"))

    def test_duplicate_root_jobs_cannot_select_one_census(self) -> None:
        array = encoded(jobs())
        for raw in (b'{"jobs":' + array + b',"jobs":[]}', b'{"jobs":[],"jobs":' + array + b'}'):
            with self.subTest(raw=raw):
                result = compare_bodies(raw)
                self.assertEqual((disposition(result), equality(result)), ("unspecified", "unavailable"))
        raw = b'{"jobs":' + array + b',"jobs":[],"extra":0}'
        self.assertEqual((disposition(compare_bodies(raw)), equality(compare_bodies(raw))), ("fail", "unavailable"))

    def test_clean_target_plus_ambiguous_second_target_is_not_unique(self) -> None:
        raw = encoded({"jobs": jobs()})
        ambiguous = encoded(jobs()[1]).replace(b'"job_id":"subject"', b'"job_id":"other","job_id":"subject"')
        result = compare_bodies(raw[:-2] + b"," + ambiguous + b"]}")
        self.assertEqual(disposition(result), "fail")  # Extra row independently contradicts the exact census.
        self.assertEqual(equality(result), "unavailable")
        duplicated = jobs() + [jobs()[1]]
        result = compare_bodies(encoded({"jobs": duplicated}))
        self.assertEqual((disposition(result), equality(result)), ("fail", "unavailable"))

    def test_nested_error_wrappers_and_nonstring_source_codes_are_not_extracted(self) -> None:
        for raw in (b'{"data":{"error":"key_bound"}}', b'{"error":null}', b'{"error":0}',
                    b'{"error":false}', b'{"error":{"value":"key_bound"}}', b"[]"):
            with self.subTest(raw=raw):
                result = compare_bodies(encoded({"jobs": jobs()}), raw)
                self.assertEqual(result.sources[0].comparison.facet("body_shape_value").disposition, "fail")
                self.assertEqual(equality(result), "unavailable")
        result = compare_bodies(encoded({"data": {"jobs": jobs()}}))
        self.assertEqual((disposition(result), equality(result)), ("fail", "unavailable"))

    def test_multiple_relations_with_large_unrelated_census_preserve_every_job(self) -> None:
        unrelated_rules = tuple(relation.JobRule(f"unrelated-{i:03d}", 1, "queued", 0, 0, None) for i in range(512))
        expected = relation.JobsExpectation("large-census", (
            relation.JobRule("related-a", 2, "failed", 1, 0, relation.ErrorReference("source-a")),
            relation.JobRule("related-b", 3, "failed", 2, 0, relation.ErrorReference("source-b")),
        ) + unrelated_rules)
        census = [
            {"job_id": "related-a", "epoch": 2, "state": "failed", "total": 1, "completed": 0, "error": "alpha"},
            {"job_id": "related-b", "epoch": 3, "state": "failed", "total": 2, "completed": 0, "error": "beta"},
        ] + [{"job_id": f"unrelated-{i:03d}", "epoch": 1, "state": "queued", "total": 0, "completed": 0, "error": None}
             for i in range(512)]
        sources = (observed("source-a", b'{"error":"alpha"}', 400), observed("source-b", b'{"error":"beta"}', 400))
        result = relation.compare(expected, (observed("large-census", encoded({"jobs": census})),) + sources)
        self.assertEqual((disposition(result), tuple(item.facet.disposition for item in result.relations)),
                         ("pass", ("pass", "pass")))
        census[199]["epoch"] = 7
        census[1]["error"] = "alpha"
        changed = relation.compare(expected, (observed("large-census", encoded({"jobs": census})),) + sources)
        self.assertEqual(disposition(changed), "fail")
        self.assertEqual(tuple((item.job_id, item.facet.disposition) for item in changed.relations),
                         (("related-a", "pass"), ("related-b", "fail")))
        self.assertEqual(len(expected.jobs), 514)
        # No timing threshold or implementation-call counter: this is a semantic census control.

    def test_multiple_explicit_relations_are_ordered_independent_and_shared_only_when_declared(self) -> None:
        expected = relation.JobsExpectation("readback", (
            relation.JobRule("a", 1, "failed", 1, 0, relation.ErrorReference("first")),
            relation.JobRule("b", 2, "failed", 2, 0, relation.ErrorReference("second")),
            relation.JobRule("c", 3, "failed", 3, 0, relation.ErrorReference("first")),
        ))
        census = [
            {"job_id": "a", "epoch": 1, "state": "failed", "total": 1, "completed": 0, "error": "one"},
            {"job_id": "b", "epoch": 2, "state": "failed", "total": 2, "completed": 0, "error": "wrong"},
            {"job_id": "c", "epoch": 3, "state": "failed", "total": 3, "completed": 0, "error": "one"},
        ]
        supplied = (observed("second", b'{"error":"two"}', 400), observed("readback", encoded({"jobs": census})),
                    observed("first", b'{"error":"one"}', 400))
        result = relation.compare(expected, supplied)
        self.assertEqual(disposition(result), "pass")
        self.assertEqual(tuple(item.step_id for item in result.sources), ("first", "second"))
        self.assertEqual(tuple((item.job_id, item.source_step_id, item.facet.disposition) for item in result.relations),
                         (("a", "first", "pass"), ("b", "second", "fail"), ("c", "first", "pass")))
        self.assertEqual(relation.compare(expected, tuple(reversed(supplied))), result)
        missing_second = relation.compare(expected, supplied[1:])
        self.assertEqual(tuple(item.facet.disposition for item in missing_second.relations), ("pass", "unavailable", "pass"))
        census[1]["error"] = "two"
        passed = relation.compare(expected, (supplied[0], observed("readback", encoded({"jobs": census})), supplied[2]))
        self.assertEqual(tuple(item.facet.disposition for item in passed.relations), ("pass", "pass", "pass"))


if __name__ == "__main__":
    unittest.main()
