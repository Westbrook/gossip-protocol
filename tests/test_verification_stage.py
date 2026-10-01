from copy import deepcopy
import json
import threading
import unittest

from gossip_harness.verification_probes import parse_proposals, revalidate_cases
from gossip_harness.verification_stage import run_verification_stage, digest
from gossip_harness.worker import WorkerResult


def project():
    return dict(id="evolving", title="Evolving repository",
        allowed_paths=("engine.py", "helper.py"),
        initial_files={"engine.py": "{}", "helper.py": "stable", "README.md": "trusted"},
        stages=tuple(dict(specification=f"Implement milestone {index}", requirements=[f"R{index}"],
            visible_cases=[dict(id=f"public-{index}", input=f"public{index}", expected=1, requirement=f"R{index}")],
            hidden_cases=[dict(input="PRIVATE_HIDDEN_SENTINEL", expected="PRIVATE_ANSWER_SENTINEL")],
            known_files={"engine.py": "PRIVATE_ORACLE_SENTINEL"}) for index in range(4)))


def code(**extra):
    return {**{f"public{i}": 1 for i in range(4)}, **extra}


def builder(values, notes=None, extra=None):
    changes = {"engine.py": json.dumps(values)}
    if notes is not None:
        changes["notes.json"] = json.dumps(dict(notes=notes, remaining=[]))
    changes.update(extra or {})
    return WorkerResult(changes, "source proposal", 0, {})


def review(request, action="accept", slot="slot-0", probes=None, notes="Inspect the implementation", remaining=None):
    value = dict(candidate=request["metadata"]["alias_by_slot"][slot], action=action,
                 notes=notes, remaining=remaining or [], probes=probes or [])
    return WorkerResult({"review.json": json.dumps(value)}, "review decision", 0, {})


def probe(value="probe", expected=2, requirement="R0"):
    return dict(input=value, expected=expected, requirement=requirement)


def reference(stage, value):
    if value == "stageful":
        return stage + 2
    return 1 if value.startswith("public") else 2


def validate_input(stage, value):
    if not isinstance(value, str) or len(value) > 40 or value.startswith("outside"):
        raise ValueError("Outside the fixture domain")


class Callbacks:
    def __init__(self, answer, stage=0):
        self.project = project()
        self.answer = answer
        self.stage = stage
        self.requests = []
        self.events = []
        self.retained = {}
        self.evaluations = []
        self.gates = []
        self.lock = threading.Lock()

    def invoke(self, **request):
        with self.lock:
            self.requests.append(deepcopy(request))
        return self.answer(request)

    def retain(self, files, label):
        self.retained[label] = deepcopy(files)
        sha = digest(files)
        return dict(files_sha256=sha, tip_sha=sha[:40], store_path="/host-private-store/" + label + ".git")

    def evaluate(self, files, cases, label):
        self.evaluations.append((deepcopy(files), deepcopy(cases), label))
        assert any(retained == files for retained in self.retained.values())
        actual = json.loads(files["engine.py"])
        outcomes = [dict(index=index, passed=digest(actual.get(case["input"])) == digest(case["expected"]),
                         actual=actual.get(case["input"]), status="executed") for index, case in enumerate(cases)]
        passed = all(row["passed"] for row in outcomes)
        # BlackboxValidator drops provenance from the execution-suite binding;
        # the stage keeps its separate hash of the full retained evidence.
        execution_cases = [{key: case[key] for key in ("input", "expected", "id", "requirement") if key in case}
                           for case in cases]
        return dict(passed=passed, status="passed" if passed else "failed", outcomes=outcomes,
                    cleanup_verified=True, source_sha256=digest(files), suite_sha256=digest(execution_cases))

    def validate_probes(self, envelope, stage, origin):
        self.gates.append(deepcopy(envelope))
        if envelope["mode"] == "revalidate":
            return revalidate_cases(envelope["probes"], stage_index=stage, reference=reference,
                                    validate_input=validate_input, origin=origin)
        requirements = [requirement for item in self.project["stages"][:stage + 1] for requirement in item["requirements"]]
        return parse_proposals(json.dumps({"probes": envelope["probes"]}), stage_index=stage,
             requirements=requirements, reference=reference, validate_input=validate_input, origin=origin,
             existing_cases=envelope["existing_cases"], max_new=envelope["max_new"])

    def emit(self, event, **data):
        self.events.append(dict(event=event, **deepcopy(data)))

    def run(self, policy="cheap-reviewed", prior=None, initial=None, seed=17):
        return run_verification_stage(self.project, self.stage, policy,
            initial or self.project["initial_files"], prior or {},
            invoke=self.invoke, evaluate=self.evaluate, retain=self.retain,
            validate_probes=self.validate_probes, emit=self.emit, candidate_seed=seed)


class VerificationStageTests(unittest.TestCase):
    def test_same_completion_gate_all_policies_builder_notes_are_not_gating(self):
        for policy, count, model in (("strong-reviewed", 1, "strong"),
                                      ("cheap-reviewed", 1, "cheap"),
                                      ("portfolio-reviewed", 4, "cheap")):
            with self.subTest(policy=policy):
                callbacks = Callbacks(lambda request: review(request) if request["role"] == "reviewer" else builder(code()))
                result = callbacks.run(policy)
                self.assertTrue(result["completed"])
                self.assertEqual(result["metrics"]["builder_calls"], count)
                self.assertEqual(result["metrics"]["invalid_builder_notes"], count)
                self.assertEqual(result["metrics"]["reviewer_calls"], 1)
                self.assertEqual({r["model"] for r in callbacks.requests if r["role"] == "builder"}, {model})
                self.assertEqual(result["metrics"]["acceptances_without_new_probes"], 1)

    def test_new_probe_blocks_acceptance_cross_checks_all_and_allows_reselection(self):
        def answer(request):
            meta = request["metadata"]
            if request["role"] == "builder":
                return builder(code(probe=0 if meta["candidate_id"] == "slot-0" else 2))
            return review(request, slot="slot-0" if meta["round"] == 1 else "slot-1",
                          probes=[probe()] if meta["round"] == 1 else [])

        callbacks = Callbacks(answer)
        result = callbacks.run("portfolio-reviewed")
        self.assertTrue(result["completed"])
        self.assertEqual(result["selected"], result["alias_map"]["slot-1"])
        self.assertEqual(result["metrics"]["blocked_acceptances"], 1)
        self.assertEqual(result["metrics"]["repairs"], 0)
        self.assertEqual(result["metrics"]["probe_discriminating_cases"], 1)
        self.assertEqual(len([x for x in callbacks.evaluations if x[2].endswith("new-probes")]), 4)
        review_rows = [row for row in result["trajectory"] if row["kind"] == "reviewer"]
        self.assertEqual([row["accepted"] for row in review_rows], [False, True])

    def test_failed_provisional_acceptance_never_automatically_repairs(self):
        callbacks = Callbacks(lambda request: review(request, probes=[probe()]) if request["role"] == "reviewer" else builder(code()))
        result = callbacks.run()
        self.assertFalse(result["completed"])
        self.assertEqual(result["metrics"]["builder_calls"], 1)
        self.assertEqual(result["metrics"]["reviewer_calls"], 6)
        self.assertEqual(result["metrics"]["admitted_new_probes"], 1)
        self.assertEqual(result["metrics"]["blocked_acceptances"], 6)

    def test_repair_requires_subsequent_review_and_has_a_four_call_limit(self):
        callbacks = Callbacks(lambda request: review(request, "repair", remaining=["R0"])
                              if request["role"] == "reviewer" else builder({}))
        result = callbacks.run("portfolio-reviewed")
        self.assertFalse(result["completed"])
        self.assertEqual(result["metrics"]["builder_calls"], 8)
        self.assertEqual(result["metrics"]["repairs"], 4)
        self.assertEqual(result["metrics"]["reviewer_calls"], 6)
        self.assertTrue(result["trajectory"][-1]["errors"])

    def test_repair_restores_checks_then_review_can_accept(self):
        def answer(request):
            if request["role"] == "reviewer":
                return review(request, "repair" if request["metadata"]["round"] == 1 else "accept")
            return builder({} if request["metadata"]["round"] == 1 else code())

        result = Callbacks(answer).run()
        self.assertTrue(result["completed"])
        self.assertEqual(result["metrics"]["repairs"], 1)
        self.assertEqual(result["metrics"]["reviewer_calls"], 2)

    def test_probe_cap_is_eight_new_unique_inputs_per_stage(self):
        def answer(request):
            if request["role"] == "builder":
                return builder(code(**{f"p{index}": 2 for index in range(12)}))
            round_number = request["metadata"]["round"]
            probes = [probe(f"p{4 * (round_number - 1) + index}") for index in range(4)] if round_number < 4 else []
            return review(request, "inspect" if round_number < 4 else "accept", probes=probes)

        result = Callbacks(answer).run()
        self.assertTrue(result["completed"])
        self.assertEqual(len(result["probe_pool"]), 8)
        self.assertEqual(result["metrics"]["admitted_new_probes"], 8)
        self.assertEqual(result["metrics"]["rejected_probes"], 4)

    def test_bad_expectations_and_outside_inputs_do_not_imply_source_failure(self):
        callbacks = Callbacks(lambda request: review(request, probes=[probe(expected=999), probe("outside-domain")])
                              if request["role"] == "reviewer" else builder(code()))
        result = callbacks.run()
        self.assertTrue(result["completed"])
        self.assertEqual(result["metrics"]["admitted_new_probes"], 0)
        self.assertEqual(result["metrics"]["rejected_probes"], 2)
        self.assertFalse(result["probe_pool"])

    def test_gate_cannot_silently_correct_or_inject_expectations(self):
        callbacks = Callbacks(lambda request: review(request, probes=[probe(expected=999)])
                              if request["role"] == "reviewer" else builder(code(probe=2)))
        callbacks.validate_probes = lambda envelope, stage, origin: dict(oracle_assisted=True,
            admitted_cases=[dict(id="injected", input="probe", expected=2, requirement="R0")], rejected=[])
        with self.assertRaisesRegex(RuntimeError, "changed an expectation"):
            callbacks.run()

    def test_execution_suite_hash_excludes_retained_probe_provenance(self):
        callbacks = Callbacks(lambda request: review(request, probes=[probe()])
                              if request["role"] == "reviewer" else builder(code(probe=2)))
        result = callbacks.run()
        self.assertTrue(result["completed"])
        self.assertIn("validation_sha256", result["probe_pool"][0])
        receipt = result["candidates"][result["selected"]]["validation_receipts"][-1]["receipt"]
        execution_cases = [{key: case[key] for key in ("input", "expected", "id", "requirement")}
                           for case in result["probe_pool"]]
        self.assertEqual(receipt["suite_sha256"], digest(execution_cases))
        self.assertNotEqual(receipt["suite_sha256"], digest(result["probe_pool"]))
        self.assertEqual(result["evidence_sha256"], digest(result["evidence_cases"]))

    def test_old_probes_revalidated_and_changed_contract_retired_explicitly(self):
        first = Callbacks(lambda request: review(request, probes=[probe("stageful")])
                          if request["role"] == "reviewer" else builder(code(stageful=2))).run()
        callbacks = Callbacks(lambda request: review(request) if request["role"] == "reviewer" else builder(code(stageful=3)), stage=1)
        result = callbacks.run(prior=first, initial=first["files"])
        self.assertTrue(result["completed"])
        self.assertEqual(result["metrics"]["retired_probes"], 1)
        self.assertFalse(result["probe_pool"])
        self.assertEqual(result["retired_probes"][0]["case"]["expected"], 2)
        self.assertEqual(result["probe_receipts"][0]["receipt"]["retired"][0]["reason"], "contract_changed")

    def test_tampered_retained_provenance_fails_before_requests(self):
        first = Callbacks(lambda request: review(request, probes=[probe()])
                          if request["role"] == "reviewer" else builder(code(probe=2))).run()
        first["probe_pool"][0]["expected"] = 999
        callbacks = Callbacks(lambda request: builder(code()), stage=1)
        with self.assertRaises(ValueError):
            callbacks.run(prior=first, initial=first["files"])
        self.assertFalse(callbacks.requests)

    def test_aliases_are_stable_and_old_notes_private_fields_and_stores_are_absent(self):
        prior = dict(notes="candidate-A has a hidden advantage", check_history=[dict(stage_index=0,
                      candidate="c0", notes="old alias model cheap", outcomes=[])])
        callbacks = Callbacks(lambda request: review(request) if request["role"] == "reviewer" else builder(code(), notes="candidate-B old context"))
        result = callbacks.run("portfolio-reviewed", prior=prior)
        reviewer_request = next(request for request in callbacks.requests if request["role"] == "reviewer")
        serialized = json.dumps({key: value for key, value in reviewer_request.items() if key not in {"metadata", "model"}})
        for forbidden in ("PRIVATE_HIDDEN_SENTINEL", "PRIVATE_ANSWER_SENTINEL", "PRIVATE_ORACLE_SENTINEL",
                          "/host-private-store/", "portfolio-reviewed", "old alias model cheap", "hidden advantage"):
            self.assertNotIn(forbidden, serialized)
        self.assertEqual(result["notes"], "")
        other = Callbacks(lambda request: review(request) if request["role"] == "reviewer" else builder(code())).run()
        self.assertEqual(result["alias_map"]["slot-0"], other["alias_map"]["slot-0"])

    def test_deterministic_replay_including_initial_concurrent_calls(self):
        def answer(request):
            if request["role"] == "builder":
                return builder(code(probe=2), notes="Preserve the old behavior")
            return review(request, probes=[probe()])

        first, second = Callbacks(answer), Callbacks(answer)
        self.assertEqual(first.run("portfolio-reviewed"), second.run("portfolio-reviewed"))
        self.assertEqual(first.events, second.events)
        normalize = lambda requests: sorted((digest(request["metadata"]), digest(request)) for request in requests)
        self.assertEqual(normalize(first.requests), normalize(second.requests))

    def test_invalid_reviews_and_inspect_are_bounded_without_success(self):
        for action in ("invalid", "inspect"):
            def answer(request):
                if request["role"] == "builder":
                    return builder(code())
                if action == "inspect":
                    return review(request, "inspect")
                return WorkerResult({"review.json": "{}"}, "bad", 0, {})

            callbacks = Callbacks(answer)
            result = callbacks.run()
            self.assertFalse(result["completed"])
            self.assertEqual(result["metrics"]["builder_calls"], 1)
            self.assertEqual(result["metrics"]["reviewer_calls"], 6)

    def test_deeply_nested_or_overflowed_model_probes_are_bounded_invalid_reviews(self):
        for value in ("[" * 1500 + "0" + "]" * 1500, "1e309"):
            with self.subTest(value=value[:20]):
                def answer(request):
                    if request["role"] == "builder":
                        return builder(code())
                    content = json.dumps(dict(candidate=request["metadata"]["alias_by_slot"]["slot-0"],
                        action="accept", notes="Assess", remaining=[], probes=[]))
                    content = content.replace('"probes": []',
                        '"probes": [{"input":' + value + ',"expected":2,"requirement":"R0"}]')
                    return WorkerResult({"review.json": content}, "invalid probe", 0, {})

                callbacks = Callbacks(answer)
                result = callbacks.run()
                self.assertFalse(result["completed"])
                self.assertEqual(result["metrics"]["invalid_reviews"], 6)
                self.assertFalse(callbacks.gates)

    def test_incomplete_checkpoint_keeps_reviewer_remaining_with_passing_checks(self):
        callbacks = Callbacks(lambda request: review(request, "repair", remaining=["R0"])
                              if request["role"] == "reviewer" else builder(code()))
        result = callbacks.run()
        self.assertFalse(result["completed"])
        self.assertEqual(result["remaining"], ["R0"])
        self.assertTrue(all(row["passed"] for row in result["matrix"][result["selected"]].values()))
        resumed = Callbacks(lambda request: review(request) if request["role"] == "reviewer" else builder(code()), stage=1)
        completed = resumed.run(prior=result, initial=result["files"])
        self.assertEqual(json.loads(resumed.requests[0]["files"]["verification-context.json"])["prior_remaining"], ["R0"])
        self.assertTrue(completed["completed"])
        self.assertEqual(completed["remaining"], [])

    def test_retained_probe_evidence_is_reexecuted_after_a_source_repair(self):
        first = Callbacks(lambda request: review(request, probes=[probe()])
                          if request["role"] == "reviewer" else builder(code(probe=2))).run()

        def answer(request):
            if request["role"] == "reviewer":
                return review(request, "repair" if request["metadata"]["round"] == 1 else "accept")
            return builder(code(probe=2 if request["metadata"]["round"] == 1 else 0))

        callbacks = Callbacks(answer, stage=1)
        result = callbacks.run(prior=first, initial=first["files"])
        self.assertFalse(result["completed"])
        self.assertEqual(len(result["probe_pool"]), 1)
        self.assertEqual(result["probe_pool"][0]["validated_stage_index"], 1)
        self.assertEqual(result["metrics"]["retired_probes"], 0)
        self.assertEqual(result["metrics"]["blocked_acceptances"], 5)
        self.assertFalse(result["matrix"][result["selected"]][first["probe_pool"][0]["id"]]["passed"])

    def test_provider_and_validation_exceptions_propagate_without_retry(self):
        callbacks = Callbacks(lambda request: (_ for _ in ()).throw(RuntimeError("Unknown usage")))
        with self.assertRaisesRegex(RuntimeError, "Unknown usage"):
            callbacks.run()
        self.assertEqual(len(callbacks.requests), 1)
        callbacks = Callbacks(lambda request: builder(code()))
        callbacks.evaluate = lambda *args: dict(status="timeout", passed=False)
        with self.assertRaises(RuntimeError):
            callbacks.run()
        self.assertEqual(len(callbacks.requests), 1)

    def test_unauthorized_source_edit_cannot_be_accepted_even_if_existing_code_passes(self):
        callbacks = Callbacks(lambda request: review(request) if request["role"] == "reviewer"
                              else builder(code(), extra={"README.md": "tampered"}))
        result = callbacks.run(initial={**project()["initial_files"], "engine.py": json.dumps(code())})
        self.assertFalse(result["completed"])
        self.assertEqual(result["files"]["README.md"], "trusted")
        self.assertEqual(result["metrics"]["invalid_source_proposals"], 1)


if __name__ == "__main__":
    unittest.main()
