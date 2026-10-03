from __future__ import annotations

import base64
from dataclasses import FrozenInstanceError, replace
import hashlib
import json
import unittest

from gossip_harness import candidate_http_execution_v2 as execution
from gossip_harness import candidate_http_expectations_v1 as plans
from gossip_harness import candidate_http_semantics_v1 as semantics
from gossip_harness import candidate_http_transport_v1 as wire

H = "a" * 64
IMAGE = "sha256:" + H


def request(target="/api/health", body=b"", method="GET"):
    headers = [["Host", "127.0.0.1:8765"], ["Connection", "close"]]
    if body or method == "POST":
        headers.append(["Content-Length", str(len(body))])
    return wire.encoded({"method": method, "target": target, "headers": headers,
                         "body_b64": base64.b64encode(body).decode("ascii")})


def recipe(probes=1):
    return execution.HttpRecipe("literal-case", ("python", "-m", "library", "serve", "--db",
        "/tmp/library.sqlite", "--root", "/inputs", "--port", "8765"),
        (("Alpha.txt", "Straße [a.b]\n".encode()),), ("empty",),
        (execution.HttpStep("start", "start"),)
        + tuple(execution.HttpStep("read-" + str(i), "probe", request()) for i in range(probes))
        + (execution.HttpStep("stop", "stop"),))


def health(step="read-0", conditions=()):
    return plans.StepExpectation(step, semantics.success("health"), ("V0-HTTP",), conditions)


def case(probes=1):
    return plans.CasePlan("health-case", ("HTTP-EMPTY-01",), recipe(probes),
                          tuple(health("read-" + str(i)) for i in range(probes)))


def context():
    return plans.ExecutionContext("a" * 40, "b" * 40, H, H, H, H, IMAGE, H,
                                  execution.HttpPolicy(IMAGE), "harness_qualification", "repetition-0")


def suite():
    return plans.SuitePlan("test-suite", (plans.CatalogRow("HTTP-EMPTY-01", "HTTP-EMPTY-HEALTH",
        ("V0-HTTP-01",), ("V0-CLI-03",)),), (case(),),
        (plans.SourcePin("requirements.txt", H),), context())


def facts(body=b'{"status":"ok","schema":0}', status=200):
    return semantics.ResponseFacts(status, (), body, semantics.ListenerFacts(("127.0.0.1",)),
                                   semantics.ListenerFacts(("127.0.0.1",)))


class HttpExpectationDefinitionTests(unittest.TestCase):
    def test_serialization_binds_literal_requests_and_fixture_bytes(self):
        value = suite()
        record = json.loads(value.serialize())
        probe = record["cases"][0]["probes"][0]
        raw = b"GET /api/health HTTP/1.1\r\nHost: 127.0.0.1:8765\r\nConnection: close\r\n\r\n"
        self.assertEqual(base64.b64decode(probe["wire"]["base64"]), raw)
        self.assertEqual(probe["wire"]["sha256"], hashlib.sha256(raw).hexdigest())
        self.assertEqual(base64.b64decode(record["cases"][0]["fixtures"][0]["content"]["base64"]),
                         "Straße [a.b]\n".encode())
        self.assertEqual(value.sha256, hashlib.sha256(value.serialize()).hexdigest())

    def test_plan_and_nested_values_are_immutable(self):
        value = suite()
        before = value.serialize()
        record = value.record()
        record["cases"][0]["recipe"]["steps"].clear()
        record["context"]["policy"]["wire_limits"]["timeout_seconds"] = 1
        self.assertEqual(before, value.serialize())
        with self.assertRaises(FrozenInstanceError):
            value.cases[0].case_id = "changed"
        with self.assertRaises(plans.DefinitionError):
            replace(value, cases=list(value.cases))

    def test_all_context_dimensions_change_identity(self):
        value = suite()
        changes = {"commit_oid": "c" * 40, "tree_oid": "c" * 40,
            "source_sha256": "c" * 64, "requirements_sha256": "c" * 64,
            "evaluator_sha256": "c" * 64, "runtime_sha256": "c" * 64,
            "environment_sha256": "c" * 64, "purpose": "independent_acceptance",
            "repetition_id": "repetition-1", "policy": replace(value.context.policy, seed=1)}
        for field, changed in changes.items():
            with self.subTest(field=field):
                self.assertNotEqual(value.sha256, replace(value, context=replace(value.context, **{field: changed})).sha256)
        changed_image = "sha256:" + "c" * 64
        changed_context = replace(value.context, image_id=changed_image,
                                  policy=replace(value.context.policy, image_id=changed_image))
        self.assertNotEqual(value.sha256, replace(value, context=changed_context).sha256)

    def test_limits_and_lifetime_are_bound(self):
        value = suite()
        for policy in (replace(value.context.policy, lifetime_seconds=800),
                       replace(value.context.policy, wire_limits=replace(wire.WireLimits(), response_limit_bytes=1000))):
            self.assertNotEqual(value.sha256, replace(value, context=replace(value.context, policy=policy)).sha256)

    def test_fixture_and_request_spelling_are_bound(self):
        value = suite()
        original = value.cases[0]
        recipes = (replace(original.recipe, fixtures=(("Alpha.txt", b"different"),)),
                   replace(original.recipe, directories=("another-empty",)),
                   replace(original.recipe, steps=(original.recipe.steps[0],
                       execution.HttpStep("read-0", "probe", request("/api/health?literal=1")), original.recipe.steps[-1])))
        for changed in recipes:
            self.assertNotEqual(value.sha256, replace(value, cases=(replace(original, recipe=changed),)).sha256)

    def test_expected_values_and_citations_are_bound(self):
        value = suite()
        empty = semantics.success("documents", b'{"documents":[],"total":0}')
        for expected in (replace(health(), expectation=empty), replace(health(), citations=("M1-HTTP",))):
            changed = replace(value.cases[0], expectations=(expected,))
            self.assertNotEqual(value.sha256, replace(value, cases=(changed,)).sha256)

    def test_ordered_cases_and_roster_and_pins_are_bound(self):
        value = suite()
        second = replace(case(), case_id="second")
        two = replace(value, cases=(case(), second))
        self.assertNotEqual(two.sha256, replace(two, cases=(second, case())).sha256)
        self.assertNotEqual(value.sha256, replace(value, sources=(plans.SourcePin("requirements.txt", "b" * 64),)).sha256)
        self.assertNotEqual(value.sha256, replace(value, rows=(replace(value.rows[0], requirement_ids=("V0-HTTP-02",)),)).sha256)

    def test_omitted_or_added_roster_row_rejected(self):
        value = suite()
        extra = plans.CatalogRow("HTTP-EMPTY-02", "HTTP-EMPTY-HEALTH", ("V0-HTTP-01",))
        with self.assertRaises(plans.DefinitionError):
            replace(value, rows=value.rows + (extra,))
        with self.assertRaises(plans.DefinitionError):
            replace(value, cases=(replace(case(), row_ids=("HTTP-EMPTY-02",)),))

    def test_duplicate_ids_pins_and_overlapping_labels_rejected(self):
        value = suite()
        for field in ("rows", "cases", "sources"):
            with self.subTest(field=field), self.assertRaises(plans.DefinitionError):
                replace(value, **{field: getattr(value, field) * 2})
        with self.assertRaises(plans.DefinitionError):
            replace(value.rows[0], interaction_ids=value.rows[0].requirement_ids)

    def test_each_probe_needs_one_expectation_in_exact_order(self):
        value = case(2)
        for expectations in (value.expectations[:1], value.expectations[::-1], value.expectations * 2):
            with self.subTest(expectations=expectations), self.assertRaises(plans.DefinitionError):
                replace(value, expectations=expectations)

    def test_cannot_depend_on_same_later_or_nonexistent_step(self):
        value = case(2)
        for step in ("read-0", "read-1", "not-here", "start"):
            condition = plans.ConditionalFacet("body_shape_value", (plans.Prerequisite(step, "status"),))
            with self.subTest(step=step), self.assertRaises(plans.DefinitionError):
                replace(value, expectations=(health(conditions=(condition,)), value.expectations[1]))

    def test_cannot_depend_on_facet_absent_from_earlier_expectation(self):
        value = case(2)
        condition = plans.ConditionalFacet("body_shape_value", (plans.Prerequisite("read-0", "error_code"),))
        with self.assertRaises(plans.DefinitionError):
            replace(value, expectations=(health(), health("read-1", (condition,))))
        with self.assertRaises(plans.DefinitionError):
            health(conditions=(plans.ConditionalFacet("error_code", (plans.Prerequisite("read-0", "status"),)),))

    def test_dependencies_change_identity(self):
        value = replace(suite(), cases=(case(2),))
        condition = plans.ConditionalFacet("body_shape_value", (plans.Prerequisite("read-0", "body_shape_value"),))
        changed = replace(value.cases[0], expectations=(health(), health("read-1", (condition,))))
        self.assertNotEqual(value.sha256, replace(value, cases=(changed,)).sha256)

    def test_policy_must_admit_every_request_before_plan_serialization(self):
        value = suite()
        limits = replace(wire.WireLimits(), request_limit_bytes=10)
        with self.assertRaises(wire.WireError):
            replace(value, context=replace(value.context, policy=replace(value.context.policy, wire_limits=limits)))

    def test_source_paths_and_digests_are_closed(self):
        for path in ("/tmp/spec", "../spec", "a/../spec", "a//b", "a\\b", "a\n"):
            with self.subTest(path=path), self.assertRaises(plans.DefinitionError):
                plans.SourcePin(path, H)
        with self.assertRaises(plans.DefinitionError):
            plans.SourcePin("spec", "not-a-hash")
        with self.assertRaises(plans.DefinitionError):
            replace(context(), commit_oid="main")
        with self.assertRaises(plans.DefinitionError):
            replace(context(), image_id="python:latest")
        with self.assertRaises(plans.DefinitionError):
            replace(context(), image_id="sha256:" + "b" * 64)

    def test_authority_cannot_be_selected_by_constructor(self):
        value = suite()
        for field, forged in (("fresh_execution", True), ("acceptance_authority", True), ("authority", "physical")):
            with self.subTest(field=field), self.assertRaises(ValueError):
                replace(value, **{field: forged})
        independent = replace(value, context=replace(value.context, purpose="independent_acceptance"))
        self.assertFalse(independent.acceptance_authority)
        self.assertFalse(independent.fresh_execution)
        self.assertEqual(independent.authority, "author_declarations_only")


class HttpExpectationPrerequisiteTests(unittest.TestCase):
    def dependent(self, required="body_shape_value"):
        value = case(2)
        dependency = plans.ConditionalFacet("body_shape_value", (plans.Prerequisite("read-0", required),))
        return replace(value, expectations=(health(), health("read-1", (dependency,))))

    def test_missing_setup_blocks_dependent_value_but_keeps_independent_failure(self):
        result = plans.diagnose(self.dependent(), (plans.SuppliedFacts("read-1", facts(b'{"wrong":1}', 409)),))
        self.assertEqual(result.steps[1].comparison.facet("body_shape_value").disposition, "unavailable")
        self.assertEqual(result.steps[1].comparison.facet("status").disposition, "fail")
        self.assertEqual(result.steps[1].comparison.facet("json_syntax").disposition, "pass")

    def test_failed_setup_is_not_candidate_learned_expected_state(self):
        bad = facts(b'{"status":"ok","schema":99}')
        result = plans.diagnose(self.dependent(), (plans.SuppliedFacts("read-0", bad), plans.SuppliedFacts("read-1", bad)))
        self.assertEqual(result.steps[0].comparison.facet("body_shape_value").disposition, "fail")
        self.assertEqual(result.steps[1].comparison.facet("body_shape_value").disposition, "unavailable")

    def test_unspecified_prerequisite_does_not_pass(self):
        result = plans.diagnose(self.dependent("response_media_type"),
            (plans.SuppliedFacts("read-0", facts()), plans.SuppliedFacts("read-1", facts())))
        self.assertEqual(result.steps[0].comparison.facet("response_media_type").disposition, "unspecified")
        self.assertEqual(result.steps[1].comparison.facet("body_shape_value").disposition, "unavailable")

    def test_passing_setup_allows_comparison_and_order_is_recipe_order(self):
        result = plans.diagnose(self.dependent(),
            (plans.SuppliedFacts("read-1", facts()), plans.SuppliedFacts("read-0", facts())))
        self.assertEqual(tuple(row.step_id for row in result.steps), ("read-0", "read-1"))
        self.assertEqual(result.steps[1].comparison.facet("body_shape_value").disposition, "pass")
        self.assertFalse(result.acceptance_authority)
        self.assertFalse(result.fresh_execution)
        self.assertEqual(result.authority, "supplied_values_only")

    def test_missing_later_facts_do_not_erase_earlier_failure(self):
        result = plans.diagnose(self.dependent(), (plans.SuppliedFacts("read-0", facts(status=500)),))
        self.assertEqual(result.steps[0].comparison.facet("status").disposition, "fail")
        self.assertEqual(result.steps[1].comparison.facet("status").disposition, "unavailable")

    def test_prerequisites_are_transitive_per_facet(self):
        value = case(3)
        def depend(step, earlier):
            return health(step, (plans.ConditionalFacet("body_shape_value", (plans.Prerequisite(earlier, "body_shape_value"),)),))
        value = replace(value, expectations=(health(), depend("read-1", "read-0"), depend("read-2", "read-1")))
        result = plans.diagnose(value, (plans.SuppliedFacts("read-1", facts()), plans.SuppliedFacts("read-2", facts())))
        self.assertEqual(result.steps[2].comparison.facet("body_shape_value").disposition, "unavailable")
        self.assertEqual(result.steps[2].comparison.facet("status").disposition, "pass")

    def test_unknown_duplicate_and_untyped_observation_rejected(self):
        for observations in ((plans.SuppliedFacts("missing", facts()),),
                             (plans.SuppliedFacts("read-0", facts()),) * 2,
                             ({"step_id": "read-0", "facts": facts()},)):
            with self.subTest(observations=observations), self.assertRaises(plans.DefinitionError):
                plans.diagnose(case(), observations)

    def test_diagnostic_records_reject_mutable_or_untyped_payloads(self):
        result = plans.diagnose(case(), (plans.SuppliedFacts("read-0", facts()),))
        for steps in (list(result.steps), result.steps * 2, ("not-a-step",)):
            with self.subTest(steps=steps), self.assertRaises(plans.DefinitionError):
                plans.CaseDiagnostic(steps)
        with self.assertRaises(plans.DefinitionError):
            plans.StepDiagnostic("read-0", {"status": "pass"})

    def test_all_required_facets_must_pass(self):
        value = case(2)
        condition = plans.ConditionalFacet("body_shape_value", (plans.Prerequisite("read-0", "status"),
                                                                 plans.Prerequisite("read-0", "body_shape_value")))
        value = replace(value, expectations=(health(), health("read-1", (condition,))))
        result = plans.diagnose(value, (plans.SuppliedFacts("read-0", facts(status=409)), plans.SuppliedFacts("read-1", facts())))
        self.assertEqual(result.steps[1].comparison.facet("body_shape_value").disposition, "unavailable")


if __name__ == "__main__":
    unittest.main()
