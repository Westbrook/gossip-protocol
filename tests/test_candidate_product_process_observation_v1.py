"""Cheap host checks only; synthetic values never prove physical qualification."""
from __future__ import annotations

from dataclasses import replace
import hashlib
import json
import unittest
from unittest.mock import patch

from gossip_harness import candidate_product_process_core_v1 as core
from gossip_harness import candidate_product_process_execution_v1 as execution
from gossip_harness import candidate_product_process_reader_v1 as reader
from gossip_harness import candidate_product_process_observation_v1 as observer
from gossip_harness import candidate_http_semantics_v1 as semantics
from gossip_harness import candidate_http_transport_v1 as wire
from gossip_harness import candidate_observation_admission_v1 as admission


class CandidateProductProcessDeclarationTests(unittest.TestCase):
    def test_real_registered_source_loader_accepts_installed_product_closure(self):
        # Run from the canonical installed tree: scaffold symlinks cannot prove
        # source-path and loaded-definition agreement for a new executor.
        admission.verify_loaded_sources(execution.evaluator_sources())

    def test_real_registered_source_loader_rejects_loaded_observer_replacement(self):
        sources = execution.evaluator_sources()
        with patch.object(observer, 'compare_step', lambda expected, actual: ()):
            with self.assertRaises(admission.AdmissionError):
                admission.verify_loaded_sources(sources)

    def test_closed_full_roster_binds_fresh_process_recipes_and_setup(self):
        self.assertEqual(execution.quota_policy()["semantic_limits"], observer.SEMANTIC_LIMITS)
        cases = core.definitions()
        self.assertEqual(len(cases), 8)
        self.assertEqual(sum(len(c.steps) for c in cases), 150)
        for case in cases:
            profile = execution.HttpProductProfile(case)
            recipe = execution.recipe_from_case(case)
            self.assertEqual(profile.milestone, "M4")
            self.assertEqual(len(profile.ordered_case_ids), len(case.steps) + 1)
            if case.seed_database_fixture:
                self.assertEqual(recipe.steps[0].epoch, 0)
                self.assertEqual(recipe.steps[0].kind, "cli")
            self.assertEqual(next(step.kind for step in reversed(recipe.steps) if step.kind != "cli"), "stop")
            self.assertEqual(recipe.record()["definition"], case.record())
            setup = execution.setup_definition(recipe)
            self.assertFalse(setup["candidate_code_executed"])
            self.assertEqual(setup["directories"], ["/tmp/backups", "/tmp/next"])
            argv = execution.setup_argv(recipe, "a" * 64)
            self.assertIn(execution.SETUP_SOURCE, argv)
            self.assertTrue(all(len(arg.encode()) < 131072 for arg in argv))
            if case.seed_database_fixture:
                seed = next(f.data for f in case.fixtures if f.path == "initial.sqlite")
                self.assertEqual(setup["seed"]["sha256"], hashlib.sha256(seed).hexdigest())
                self.assertEqual(setup["seed"]["bytes"], len(seed))

    def test_typed_case_cannot_rewrite_source_expected_values_or_argv(self):
        case = core.definitions()[0]
        position = next(i for i, step in enumerate(case.steps) if step.kind == "cli")
        first = case.steps[position]
        expected = first.expectation.record()
        expected["json"] = {"fabricated": True}
        changed = replace(case, steps=(*case.steps[:position], replace(first, expectation=core.Expectation(core.encode(expected))), *case.steps[position+1:]))
        with self.assertRaises(core.DefinitionError):
            execution.HttpProductProfile(changed)
        changed = replace(case, steps=(*case.steps[:position], replace(first, argv=(*first.argv[:-1], "different.json")), *case.steps[position+1:]))
        with self.assertRaises(core.DefinitionError):
            execution.HttpProductProfile(changed)

    def test_reference_purpose_or_wrong_contract_cannot_be_relabeled(self):
        case = core.definitions()[0]
        with self.assertRaises(execution.ExecutionError):
            execution.HttpProductProfile(case, "authored_reference_qualification")
        profile = execution.HttpProductProfile(case)
        with self.assertRaises(execution.ExecutionError):
            profile.check_current(purpose="authored_reference_qualification")
        with self.assertRaises(execution.ExecutionError):
            profile.check_current(requirements_sha256="0" * 64)

    def test_selector_roster_is_source_derived_and_claims_no_qualification(self):
        case = core.definitions()[0]
        catalog = observer.selector_catalog(case.row_id, purpose="independent_acceptance")
        profile = execution.HttpProductProfile(case)
        self.assertEqual(catalog["ordered_case_ids"], list(profile.ordered_case_ids))
        self.assertFalse(catalog["scope_review_supplied"])
        self.assertFalse(catalog["acceptance_authority"])
        self.assertEqual(catalog["original_definition_purpose"], "public_product_definition")
        self.assertEqual(catalog["execution_purpose"], "independent_acceptance")
        for row in catalog["selectors"]:
            self.assertFalse(row["semantic_adequacy_reviewed"])
            self.assertFalse(row["physically_qualified"])
            self.assertIn(row["case_id"], profile.ordered_case_ids)
        self.assertEqual(catalog["selectors"][-1]["observation_pointer"], "/mechanics_guard/status")

    def test_original_modes_do_not_grant_physical_bridge_authority(self):
        with self.assertRaises(execution.ExecutionError):
            observer.HttpObservationSource(object(), object(), receipt_path="/tmp/not-authority.json")


class CandidateProductProcessComparisonTests(unittest.TestCase):
    def cli(self, raw=b'{"n":9223372036854775807}', *, exit=0, stderr=b"", state="authenticated", proof=()):
        return reader.StepObservation("step", 0, "cli", state, None, raw, stderr, exit, None, (), proof)

    def http(self, raw=b'{"n":7}', *, status=200, coding=None):
        headers = (("Content-Length", str(len(raw))),) + (() if coding is None else (("Content-Encoding", coding),))
        packet = ("HTTP/1.1 " + str(status) + " OK\r\nContent-Length: " + str(len(raw)) + "\r\n\r\n").encode() + raw
        parsed = wire.parse_response(packet, "GET")
        observation = wire.WireObservation(b"request", b"request", packet, parsed, True, False, "complete", True,
                                          (), True, "connected_pre_request", None, None)
        listener = semantics.ListenerFacts(("127.0.0.1",))
        facts = semantics.ResponseFacts(status, headers, raw, listener, listener)
        return reader.StepObservation("step", 0, "probe", "authenticated", facts, b"", b"", 0, observation, (), ())

    def dispositions(self, expected, actual):
        return {x.name: x.disposition for x in observer.compare_step(expected, actual) if not x.name.endswith("_unspecified")}

    def test_signed64_json_never_rounds_or_accepts_bool_float_tokens(self):
        expected = {"kind": "cli", "exit": 0, "json": {"n": 9223372036854775807}}
        self.assertEqual(set(self.dispositions(expected, self.cli()).values()), {"pass"})
        for value in (True, "9223372036854775807", 9223372036854775806, 9.223372036854776e18):
            verdict = self.dispositions(expected, self.cli(json.dumps({"n": value}).encode()))
            self.assertEqual(verdict["json_exact"], "fail")

    def test_syntax_duplicate_nonfinite_and_trailing_values_fail(self):
        expected = {"kind": "cli", "exit": 0, "json": {"n": 1}}
        for raw in (b'{"n":NaN}', b'{} {}', b'"\xff"'):
            with self.subTest(raw=raw):
                value = self.dispositions(expected, self.cli(raw))
                self.assertEqual(value["json_syntax"], "fail")
                self.assertEqual(value["json_exact"], "unavailable")

    def test_valid_overdepth_json_is_observer_unavailable(self):
        expected = {"kind": "cli", "exit": 0, "json": []}
        raw = b"[" * 65 + b"0" + b"]" * 65
        value = self.dispositions(expected, self.cli(raw))
        self.assertEqual(value["json_syntax"], "unavailable")
        self.assertEqual(value["json_exact"], "unavailable")
        self.assertEqual(observer._strict_json(b'{"text":"[[[[\\\"}"}'), {"text": '[[[["}'})

    def test_nonzero_candidate_exit_is_decisive_despite_unavailable_streams(self):
        expected = {"kind": "cli", "exit": 0, "json": {}}
        actual = self.cli(None, exit=2, state="unavailable", proof=(("finite_exit_proof_sha256", "a" * 64),))
        facets = observer.compare_step(expected, actual)
        self.assertEqual(observer._status(facets, observer.declared_facets(expected), actual.state), "failed")
        actual = replace(actual, provenance=())
        facets = observer.compare_step(expected, actual)
        self.assertEqual(observer._status(facets, observer.declared_facets(expected), actual.state), "infrastructure_error")

    def test_expected_error_uses_stderr_and_cannot_choose_actual_exit_stream(self):
        expected = {"kind": "cli", "exit": 2, "json": {"error": "counter_exhausted"}}
        actual = self.cli(b"", exit=2, stderr=b'{"error":"counter_exhausted"}')
        self.assertEqual(set(self.dispositions(expected, actual).values()), {"pass"})
        wrong = self.cli(b'{"error":"counter_exhausted"}', exit=0)
        verdict = self.dispositions(expected, wrong)
        self.assertEqual(verdict["exit_status"], "fail")
        self.assertEqual(verdict["json_syntax"], "fail")
        self.assertNotIn("other_stream_empty", verdict)

    def test_cli_ambiguities_never_become_false_product_failures(self):
        success = {"kind": "cli", "exit": 0, "json": {"n": 1}}
        self.assertEqual(self.dispositions(success, self.cli(b'{"n":1}', stderr=b'diagnostic'))["json_exact"], "pass")
        duplicate = self.dispositions(success, self.cli(b'{"n":1,"n":1}'))
        self.assertEqual(duplicate["json_syntax"], "unavailable")
        overflow = self.dispositions(success, self.cli(b'{"n":1e999}'))
        self.assertEqual(overflow["json_syntax"], "unavailable")
        error = {"kind": "cli", "exit": 2, "json": {"error": "missing"}}
        for raw in (b'diagnostic\n{"error":"missing"}', b'{"error":"missing"}\ndiagnostic', b'{bad', b'\xff'):
            verdict = self.dispositions(error, self.cli(exit=2, stderr=raw))
            self.assertEqual(verdict["json_syntax"], "unavailable")
            self.assertEqual(verdict["json_exact"], "unavailable")
        for raw in (b'', b'diagnostic without object', b'[]', b'{"error":"wrong"}'):
            verdict = self.dispositions(error, self.cli(exit=2, stderr=raw))
            self.assertIn("fail", verdict.values())
        grammar_only = {"kind": "cli", "exit": 0, "json_value_only": True}
        for raw in (b'null', b'{"alternative":"wrapper"}', b'[]'):
            self.assertEqual(set(self.dispositions(grammar_only, self.cli(raw, stderr=b'diagnostic')).values()), {"pass"})
        facets = observer.compare_step(grammar_only, self.cli(b'null'))
        self.assertEqual({x.name for x in facets if x.disposition == "unspecified"},
                         {"auxiliary_stream_unspecified", "json_wrapper_unspecified"})

    def test_http_compares_real_entity_bytes_separately_from_json(self):
        expected = {"kind": "http", "status": 200, "json": {"n": 7}, "body_utf8": '{"n":7}'}
        self.assertEqual(set(self.dispositions(expected, self.http()).values()), {"pass"})
        changed = self.dispositions(expected, self.http(b'{"n": 7}\n'))
        self.assertEqual(changed["json_exact"], "pass")
        self.assertEqual(changed["canonical_wire_bytes"], "fail")
        changed = self.dispositions(expected, self.http(status=409))
        self.assertEqual(changed["status"], "fail")

    def test_http_unsupported_encoding_is_unavailable_not_false_product_error(self):
        expected = {"kind": "http", "status": 200, "json": {"n": 7}}
        changed = self.dispositions(expected, self.http(coding="gzip"))
        self.assertEqual(changed["status"], "pass")
        self.assertEqual(changed["json_syntax"], "unavailable")

    def test_subset_outer_census_and_integer_ranges_are_distinct(self):
        expected = {"kind": "http", "status": 200, "json_subset": {"name": "saved.json"},
                    "required_keys": ["name", "bytes"], "integer_ranges": {"/bytes": [1, 67108864]}}
        value = self.dispositions(expected, self.http(b'{"name":"saved.json","bytes":21}'))
        self.assertEqual(set(value.values()), {"pass"})
        for data in (b'{"name":"saved.json","bytes":true}', b'{"name":"saved.json","bytes":0}',
                     b'{"name":"saved.json","bytes":67108865}'):
            self.assertEqual(self.dispositions(expected, self.http(data))["integer_range:/bytes"], "fail")
        value = self.dispositions(expected, self.http(b'{"name":"saved.json","bytes":21,"extra":0}'))
        self.assertEqual(value["json_outer_keys"], "fail")
        self.assertEqual(value["json_field:name"], "pass")

    def test_unentered_matching_bytes_never_receive_correctness_credit(self):
        expected = {"kind": "cli", "exit": 0, "json": {"n": 9223372036854775807}}
        actual = self.cli(state="unentered")
        facets = observer.compare_step(expected, actual)
        self.assertTrue(all(x.disposition == "unavailable" for x in facets if not x.name.endswith("_unspecified")))
        self.assertEqual(observer._status(facets, observer.declared_facets(expected), actual.state), "infrastructure_error")


if __name__ == "__main__":
    unittest.main()
