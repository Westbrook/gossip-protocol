"""Pure diagnosis fixtures do not authenticate product executions."""
from dataclasses import replace
from pathlib import Path
import unittest

from gossip_harness import candidate_http_execution_v4 as execution
from gossip_harness import candidate_http_observation_source_v1 as source
from gossip_harness import candidate_http_observation_v2 as reader
from gossip_harness import candidate_http_semantics_v1 as sem
from gossip_harness import candidate_http_cases_core_v1 as core
from tests.test_candidate_http_execution_v4 import profile


def history(selected_profile, *, facts=None, states=None):
    facts, states = facts or {}, states or {}
    steps = tuple(reader.StepObservation(step.step_id, index, "probe" if step.kind == "request" else step.kind, states.get(index, "authenticated"),
        facts.get(index, sem.ResponseFacts(200, (("Content-Type", "application/json"),), b'{"status":"ok","schema":0}',
            sem.ListenerFacts(("127.0.0.1",)), sem.ListenerFacts(("127.0.0.1",)))) if step.kind == "request" else None,
        b"{}" if step.kind == "cli" else None, b"" if step.kind == "cli" else None,
        0 if step.kind == "cli" else None, None, (), ()) for index, step in enumerate(selected_profile.case.steps))
    return reader.HistoryObservation("http-synthetic", "a" * 64, "b" * 64, "c" * 64, steps, (), True, ())


class HttpProductSemanticSourceV1Tests(unittest.TestCase):
    def test_full_roster_known_failure_survives_unentered_suffix(self):
        selected = profile()
        bad = sem.ResponseFacts(500, (("Content-Type", "application/json"),), sem.Missing("body lost"))
        report = source.diagnose(selected, history(selected, facts={1: bad}, states={2: "unentered"}))
        self.assertEqual(tuple(row.status for row in report), ("passed", "failed", "infrastructure_error"))
        self.assertEqual(tuple(row.case_id for row in report), selected.ordered_case_ids)

    def test_semantic_health_and_lifecycle_positive(self):
        selected = profile()
        self.assertTrue(all(row.status == "passed" for row in source.diagnose(selected, history(selected))))

    def test_unspecified_error_code_is_retained_without_inventing_exact_code(self):
        selected = profile()
        request = replace(selected.case.steps[1], expectation=core.HttpExpectation(semantic=sem.unclassified_error(400)))
        selected = replace(selected, case=replace(selected.case, steps=(selected.case.steps[0], request, selected.case.steps[2])))
        actual = sem.ResponseFacts(400, (("Content-Type", "application/json"),), b'{"error":"custom"}',
            sem.ListenerFacts(("127.0.0.1",)), sem.ListenerFacts(("127.0.0.1",)))
        report = source.diagnose(selected, history(selected, facts={1: actual}))[1]
        self.assertEqual(report.status, "passed")
        self.assertEqual(next(row.disposition for row in report.facets if row.name == "error_code"), "unspecified")
        self.assertNotIn("error_code", report.required_facets)

    def test_raw_facts_request_has_no_invented_product_pass(self):
        selected = profile()
        request = replace(selected.case.steps[1], expectation=core.HttpExpectation(raw_facts_only=True))
        selected = replace(selected, case=replace(selected.case, steps=(selected.case.steps[0], request, selected.case.steps[2])))
        self.assertEqual(source.diagnose(selected, history(selected))[1].status, "skipped")
        self.assertEqual(source.diagnose(selected, history(selected, states={1: "unentered"}))[1].status, "infrastructure_error")

    def test_complete_step_roster_required(self):
        selected = profile()
        original = history(selected)
        for steps in (original.steps[:-1], tuple(reversed(original.steps)), (original.steps[0],) * len(original.steps)):
            with self.subTest(steps=steps), self.assertRaises(ValueError):
                source.diagnose(selected, replace(original, steps=steps))

    def test_unavailable_step_cannot_smuggle_synthetic_failure_or_pass(self):
        selected = profile()
        bad = sem.ResponseFacts(500, (), b"invalid")
        report = source.diagnose(selected, history(selected, facts={1: bad}, states={1: "unavailable"}))
        self.assertEqual(report[1].status, "infrastructure_error")
        self.assertFalse(any(row.disposition == "fail" for row in report[1].facets))

    def test_content_prerequisite_suppresses_only_dependent_content(self):
        selected = profile()
        first = selected.case.steps[1]
        dependent = replace(first, step_id="second", expectation=core.HttpExpectation(semantic=sem.success("health"),
            content_requires=(core.Dependency(first.step_id),)))
        selected = replace(selected, case=replace(selected.case, steps=(selected.case.steps[0], first, dependent, selected.case.steps[-1])))
        bad = sem.ResponseFacts(500, (("Content-Type", "application/json"),), b'{"wrong":true}',
            sem.ListenerFacts(("127.0.0.1",)), sem.ListenerFacts(("127.0.0.1",)))
        report = source.diagnose(selected, history(selected, facts={1: bad, 2: bad}))
        self.assertEqual(report[1].status, "failed")
        self.assertEqual(report[2].status, "failed")
        self.assertEqual(next(row.disposition for row in report[2].facets if row.name == "body_shape_value"), "unavailable")
        self.assertEqual(next(row.disposition for row in report[2].facets if row.name == "status"), "fail")

    def test_caller_dictionaries_and_synthetic_history_cannot_be_observation_authority(self):
        with self.assertRaises(ValueError):
            source.HttpObservationSource({}, {}, receipt_path=Path("/private/tmp/not-issued.json"))
        selected = profile()
        with self.assertRaises(ValueError):
            source.HttpObservationSource(history(selected), execution.ControllerCheckpoint("a" * 64, 0, "a" * 64, 0, 0, 1),
                receipt_path=Path("/private/tmp/not-issued.json"))

    def test_independent_cli_exit_failure_survives_unavailable_state_lineage(self):
        selected = profile()
        # Use the authoring helper's source-derived state schema.
        builder = core.Builder("HTTP-PERSIST-LISTENER/cli-exit", ("V0-HTTP-01",), ("V0-CLI-03",))
        builder.start()
        builder.request("GET", "/health", sem.success("health"), check=False)
        builder.stop()
        builder.cli(("jobs",))
        selected = execution.HttpProductProfile(builder.finish(), "harness_qualification")
        original = history(selected)
        cli_index = next(i for i, step in enumerate(selected.case.steps) if step.kind == "cli")
        steps = list(original.steps)
        steps[cli_index] = replace(steps[cli_index], state="unavailable", stdout=None, stderr=None, exit_code=7,
            provenance=(("finite_exit_proof_sha256", "a" * 64),))
        self.assertEqual(source.diagnose(selected, replace(original, steps=tuple(steps)))[cli_index].status, "failed")
        steps[cli_index] = replace(steps[cli_index], exit_code=0)
        self.assertEqual(source.diagnose(selected, replace(original, steps=tuple(steps)))[cli_index].status, "infrastructure_error")
        steps[cli_index] = replace(steps[cli_index], exit_code=7, provenance=())
        self.assertEqual(source.diagnose(selected, replace(original, steps=tuple(steps)))[cli_index].status, "infrastructure_error")

    def test_related_job_error_uses_exact_literal_step_and_job_and_all_source_facets(self):
        from gossip_harness import candidate_http_relations_v1 as relations
        selected = profile()
        first = replace(selected.case.steps[1], step_id="error-source",
            expectation=core.HttpExpectation(semantic=sem.unclassified_error(400)))
        rules = relations.JobsExpectation("jobs-census", (relations.JobRule("job-a", 1, "failed", 2, 0,
            relations.ErrorReference("error-source", 400)),))
        census = replace(first, step_id="jobs-census", expectation=core.HttpExpectation(relation_json=core.encode(rules.record())))
        selected = replace(selected, case=replace(selected.case, steps=(selected.case.steps[0], first, census, selected.case.steps[-1])))
        listeners = sem.ListenerFacts(("127.0.0.1",))
        error = sem.ResponseFacts(400, (), b'{"error":"opaque"}', listeners, listeners)
        jobs = sem.ResponseFacts(200, (), b'{"jobs":[{"job_id":"job-a","epoch":1,"state":"failed","total":2,"completed":0,"error":"opaque"}]}', listeners, listeners)
        report = source.diagnose(selected, history(selected, facts={1: error, 2: jobs}))
        self.assertEqual(report[2].status, "passed")
        changed = replace(jobs, body=jobs.body.replace(b'"opaque"', b'"different"'))
        report = source.diagnose(selected, history(selected, facts={1: error, 2: changed}))
        self.assertEqual(report[2].status, "failed")
        report = source.diagnose(selected, history(selected, facts={1: replace(error, status=500), 2: jobs}))
        self.assertEqual(report[2].status, "failed")
        self.assertTrue(any(row.name == "source:error-source:status" and row.disposition == "fail" for row in report[2].facets))

    def test_git_and_transport_errors_are_consumer_unavailable(self):
        import subprocess
        from unittest.mock import patch
        from gossip_harness.candidate_scope_consumer_v1 import AuthorityUnavailable
        from gossip_harness.gitstore import GitError
        bridge = object.__new__(source.HttpObservationSource)
        for error in (GitError("unavailable Git original"), subprocess.TimeoutExpired(["git"], 1)):
            with self.subTest(error=error), patch.object(bridge, "_observation", side_effect=error):
                with self.assertRaises(AuthorityUnavailable):
                    bridge.observation(None, None)
