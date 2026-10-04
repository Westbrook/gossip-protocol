"""Synthetic M4 value/binding controls; no dispatch or acceptance evidence."""
from copy import deepcopy
from dataclasses import FrozenInstanceError, replace
from functools import lru_cache
import unittest
from unittest.mock import patch

from gossip_harness import candidate_http_cases_v1 as catalog
from gossip_harness import candidate_http_m4_semantics_v1 as m4
from gossip_harness import candidate_http_semantics_v1 as semantics
from gossip_harness import cumulative_observation_profile_v1 as profiles


HEALTH_ROWS = ("HTTP-EMPTY-HEALTH/health", "HTTP-PERSIST-LISTENER/listener-all-epochs")
LISTENER = semantics.ListenerFacts(("127.0.0.1",))


def facts(body=profiles.HEALTH_AFTER, status=200, headers=()):
    return semantics.ResponseFacts(status, headers, body, LISTENER, LISTENER)


@lru_cache(maxsize=None)
def binding(row_id):
    return m4.HttpM4Comparator(profiles.http_profile(row_id, purpose="public_release"))


def health():
    comparator = binding(HEALTH_ROWS[0])
    return comparator, comparator.profile.health_successors[0].step_index


class HttpM4SemanticComparatorTests(unittest.TestCase):
    def test_exact_three_successors_pass_schema_four_and_fail_schema_zero(self):
        expected_rows = {HEALTH_ROWS[0]: 1, HEALTH_ROWS[1]: 2}
        actual_rows = {}
        for case in catalog.definitions():
            indexes = tuple(index for index, step in enumerate(case.steps)
                            if step.expectation and step.expectation.semantic
                            and step.expectation.semantic.shape == "health")
            if not indexes:
                continue
            comparator = binding(case.row_id)
            self.assertEqual(indexes, tuple(item.step_index for item in comparator.profile.health_successors))
            actual_rows[case.row_id] = len(indexes)
            for index in indexes:
                with self.subTest(row=case.row_id, index=index):
                    self.assertEqual(comparator.compare(index, facts()).facet("body_shape_value").disposition, "pass")
                    self.assertEqual(comparator.compare(index, facts(profiles.HEALTH_BEFORE)).facet(
                        "body_shape_value").disposition, "fail")
                    self.assertEqual(case.steps[index].expectation.semantic.expected_body, profiles.HEALTH_BEFORE)
        self.assertEqual(actual_rows, expected_rows)

    def test_m1_expectations_and_comparator_remain_unchanged(self):
        expected = semantics.success("health")
        self.assertEqual(semantics.compare(facts(profiles.HEALTH_BEFORE), expected).facet(
            "body_shape_value").disposition, "pass")
        self.assertEqual(semantics.compare(facts(), expected).facet("body_shape_value").disposition, "fail")
        with self.assertRaises(ValueError):
            semantics.Expectation("health", 200, profiles.HEALTH_AFTER)

    def test_equivalent_serialization_preserves_frozen_numeric_value_policy(self):
        comparator, index = health()
        for raw in (b'{"schema":4.0,"status":"ok"}', b'{"status":"ok","schema":4e0}',
                    b' \r\n{ "schema" : 4, "status": "\\u006f\\u006b" } \t\n'):
            with self.subTest(body=raw):
                self.assertEqual(comparator.compare(index, facts(raw)).facet("body_shape_value").disposition, "pass")

    def test_closed_shape_exact_values_and_boolean_number_distinction(self):
        comparator, index = health()
        bodies = (b'{"status":"ok","schema":true}', b'{"status":"ok","schema":false}',
                  b'{"status":"ok","schema":"4"}', b'{"status":"ok","schema":5}',
                  b'{"status":"bad","schema":4}', b'{"status":"ok"}',
                  b'{"status":"ok","schema":4,"extra":1}',
                  b'{"data":{"status":"ok","schema":4}}', b'[]')
        for raw in bodies:
            with self.subTest(body=raw):
                self.assertEqual(comparator.compare(index, facts(raw)).facet("body_shape_value").disposition, "fail")

    def test_duplicate_ambiguity_and_independent_mismatch_are_preserved(self):
        comparator, index = health()
        for raw, expected in (
            (b'{"schema":0,"schema":4,"status":"ok"}', "unspecified"),
            (b'{"schema":4,"schema":0,"status":"ok"}', "unspecified"),
            (b'{"schema":4,"schema":4,"status":"ok"}', "unspecified"),
            (b'{"schema":4,"schema":4,"status":"wrong"}', "fail"),
            (b'{"schema":5,"status":"ok","status":"ok"}', "fail"),
        ):
            with self.subTest(body=raw):
                self.assertEqual(comparator.compare(index, facts(raw)).facet("body_shape_value").disposition, expected)

    def test_wrong_status_and_listener_failures_survive_successor_body_pass(self):
        comparator, index = health()
        supplied = replace(facts(status=500),
            listener_before=semantics.ListenerFacts(("0.0.0.0",)),
            listener_after=semantics.Missing("post-request listener absent"))
        original = semantics.compare(supplied, comparator.case.steps[index].expectation.semantic)
        result = comparator.compare(index, supplied)
        self.assertEqual(result.facet("body_shape_value").disposition, "pass")
        self.assertEqual(result.facet("status").disposition, "fail")
        self.assertEqual(result.facet("listener_before").disposition, "fail")
        self.assertEqual(result.facet("listener_after").disposition, "unavailable")
        self.assertEqual(tuple(facet.name for facet in result.facets), tuple(facet.name for facet in original.facets))
        for facet in original.facets:
            if facet.name != "body_shape_value":
                self.assertEqual(result.facet(facet.name), facet)

    def test_unavailable_and_invalid_representations_are_exact_frozen_results(self):
        comparator, index = health()
        cases = (
            facts(semantics.Missing("body incomplete"), status=500),
            facts(headers=semantics.Missing("headers incomplete")),
            facts(headers=(("Content-Encoding", "gzip"),)),
            facts(headers=(("Content-Encoding", "identity, br"),)),
            facts(b" " * (semantics.MAX_BODY_BYTES + 1)),
            facts(b"\xff"), facts(b"[" * 2000 + b"0" + b"]" * 2000),
            facts(b'{"status":"ok","schema":4} trailing'),
            facts(b'{"status":"ok","schema":NaN}'),
        )
        for supplied in cases:
            with self.subTest(body_type=type(supplied.body).__name__, headers=supplied.headers):
                original = semantics.compare(supplied, comparator.case.steps[index].expectation.semantic)
                self.assertNotEqual(original.facet("json_syntax").disposition, "pass")
                self.assertEqual(comparator.compare(index, supplied), original)

    def test_unknown_status_and_media_type_are_not_promoted(self):
        comparator, index = health()
        supplied = facts(status=semantics.Missing("status incomplete"),
                         headers=(("Content-Type", "not-json"),))
        result = comparator.compare(index, supplied)
        self.assertEqual(result.facet("body_shape_value").disposition, "pass")
        self.assertEqual(result.facet("status").disposition, "unavailable")
        self.assertEqual(result.facet("response_media_type").disposition, "unspecified")

    def test_non_successor_success_error_and_unspecified_comparisons_are_unchanged(self):
        for shape in ("documents", "error", "unspecified"):
            case = next(case for case in catalog.definitions() if any(
                step.expectation and step.expectation.semantic and step.expectation.semantic.shape == shape
                for step in case.steps))
            comparator = binding(case.row_id)
            for index, step in enumerate(case.steps):
                if not step.expectation or not step.expectation.semantic:
                    continue
                if any(item.step_index == index for item in comparator.profile.health_successors):
                    continue
                for supplied in (facts(), facts(b'{"error":"not_found"}', status=400),
                                 facts(semantics.Missing("capture incomplete"), status=500)):
                    self.assertEqual(comparator.compare(index, supplied),
                                     semantics.compare(supplied, step.expectation.semantic))

    def test_step_comparison_does_not_rebuild_profiles_or_catalog(self):
        comparator, index = health()
        with patch.object(profiles, "assert_profile_current", side_effect=AssertionError("per-step rebuild")), \
             patch.object(catalog, "definitions", side_effect=AssertionError("per-step catalog")):
            self.assertEqual(comparator.compare(index, facts()).facet("body_shape_value").disposition, "pass")

    def test_composition_retains_pure_comparison_authority(self):
        comparator, index = health()
        result = comparator.compare(index, facts())
        self.assertEqual(comparator.protocol, "candidate-http-m4-semantics-v1")
        self.assertFalse(comparator.acceptance_authority)
        self.assertEqual(result.authority, "pure_value_comparison_only")
        self.assertEqual(result.product_verdict, "not_evaluated")
        with self.assertRaises(FrozenInstanceError):
            comparator.case = binding(HEALTH_ROWS[1]).case


class HttpM4SemanticBindingTests(unittest.TestCase):
    def test_requires_exact_http_profile_and_product_purpose(self):
        comparator, _ = health()
        class ProfileSubclass(profiles.CumulativeProfile):
            pass
        subclass = object.__new__(ProfileSubclass)
        for name in ("family", "case_id", "purpose", "_record_json", "diagnostic_cells", "health_successors"):
            object.__setattr__(subclass, name, getattr(comparator.profile, name))
        for invalid in (None, comparator.profile.record(), subclass,
                        profiles.cli_profile("cli-empty", purpose="public_release")):
            with self.subTest(kind=type(invalid).__name__), self.assertRaises(m4.ComparatorError):
                m4.HttpM4Comparator(invalid)
        for purpose in ("harness_qualification", "repeatability"):
            changed = deepcopy(comparator.profile)
            object.__setattr__(changed, "purpose", purpose)
            with self.subTest(purpose=purpose), self.assertRaises(profiles.ProfileError):
                m4.HttpM4Comparator(changed)

    def test_all_three_valid_purposes_bind_their_original_profile(self):
        for purpose in ("public_release", "independent_acceptance", "repeatability"):
            value = profiles.http_profile(HEALTH_ROWS[0], purpose=purpose)
            comparator = m4.HttpM4Comparator(value)
            self.assertIs(comparator.profile, value)
            comparator.check_current()

    def test_profile_record_and_diagnostic_corruption_are_rejected(self):
        comparator, _ = health()
        for key in ("target_milestone", "original_definition_sha256", "original_definition_purpose"):
            changed = deepcopy(comparator.profile)
            record = changed.record()
            record[key] = "wrong"
            object.__setattr__(changed, "_record_json", profiles.encoded(record))
            with self.subTest(key=key), self.assertRaises(profiles.ProfileError):
                m4.HttpM4Comparator(changed)
        changed = deepcopy(comparator.profile)
        object.__setattr__(changed, "diagnostic_cells", changed.diagnostic_cells[:-1])
        with self.assertRaises(profiles.ProfileError):
            m4.HttpM4Comparator(changed)

    def test_successor_index_identity_count_and_target_corruption_are_rejected(self):
        comparator, _ = health()
        for field, value in (("step_index", 0), ("step_id", "other"),
                             ("target_body", b'{"status":"ok","schema":5}')):
            changed = deepcopy(comparator.profile)
            object.__setattr__(changed.health_successors[0], field, value)
            with self.subTest(field=field), self.assertRaises(profiles.ProfileError):
                m4.HttpM4Comparator(changed)
        changed = deepcopy(comparator.profile)
        object.__setattr__(changed, "health_successors", ())
        with self.assertRaises(profiles.ProfileError):
            m4.HttpM4Comparator(changed)

    def test_detached_record_edits_cannot_change_bound_profile(self):
        comparator, index = health()
        record = comparator.profile.record()
        record["expectation_successors"].clear()
        record["original_definition"]["steps"].clear()
        comparator.check_current()
        self.assertEqual(comparator.compare(index, facts()).facet("body_shape_value").disposition, "pass")

    def test_original_full_record_must_match_after_profile_validation(self):
        comparator, _ = health()
        original_catalog = catalog.definitions()
        changed = replace(comparator.case, notes=comparator.case.notes + ("changed full history",))
        altered_catalog = tuple(changed if case.row_id == changed.row_id else case for case in original_catalog)
        with patch.object(catalog, "definitions", side_effect=(original_catalog, altered_catalog)):
            with self.assertRaisesRegex(m4.ComparatorError, "complete profile"):
                m4.HttpM4Comparator(comparator.profile)

    def test_current_definition_source_binding_is_required(self):
        comparator, _ = health()
        changed = profiles.definition_sources()
        changed["gossip_harness/candidate_http_semantics_v1.py"] = "0" * 64
        with patch.object(profiles, "definition_sources", return_value=changed):
            with self.assertRaises(profiles.ProfileError):
                m4.HttpM4Comparator(comparator.profile)
            with self.assertRaises(profiles.ProfileError):
                comparator.check_current()

    def test_stale_loaded_semantics_and_comparator_cannot_relabel_current_disk(self):
        comparator, _ = health()
        for module in (semantics, m4):
            with self.subTest(module=module.PROTOCOL), patch.object(module, "LOADED_SOURCE_SHA256", "0" * 64):
                with self.assertRaises(m4.ComparatorError):
                    m4.HttpM4Comparator(comparator.profile)
                with self.assertRaises(m4.ComparatorError):
                    comparator.check_current()

    def test_invalid_step_indices_or_untyped_facts_are_rejected(self):
        comparator, index = health()
        for invalid in (True, -1, len(comparator.case.steps), 1.0, "1"):
            with self.subTest(index=invalid), self.assertRaises(m4.ComparatorError):
                comparator.compare(invalid, facts())
        with self.assertRaises(m4.ComparatorError):
            comparator.compare(index, {"body": profiles.HEALTH_AFTER})
        with self.assertRaises(m4.ComparatorError):
            comparator.compare(0, facts())

    def test_raw_and_relational_steps_need_their_existing_owner_adapters(self):
        for selector in ("raw_facts_only", "relation_json"):
            case, index = next((case, index) for case in catalog.definitions()
                               for index, step in enumerate(case.steps)
                               if step.expectation and getattr(step.expectation, selector))
            with self.subTest(selector=selector), self.assertRaises(m4.ComparatorError):
                binding(case.row_id).compare(index, facts())
