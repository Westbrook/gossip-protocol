from copy import deepcopy
import json
import threading
import unittest

from gossip_harness.verification_probes import parse_proposals, revalidate_cases
from gossip_harness.benchmark_stage import run_benchmark_stage, digest, PROTOCOL
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
        self.frozen = []
        self.scouts = []

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

    def after_initial(self, pool):
        self.frozen.append(deepcopy(pool))
        return dict(freeze_id="offline-freeze", pool_sha256=digest(pool), scouts=deepcopy(self.scouts))

    def run(self, policy="strong-anchor", prior=None, initial=None, seed=17, **options):
        return run_benchmark_stage(self.project, self.stage, policy,
            initial or self.project["initial_files"], prior or {},
            invoke=self.invoke, evaluate=self.evaluate, retain=self.retain,
            validate_probes=self.validate_probes, emit=self.emit, after_initial=self.after_initial, candidate_seed=seed, **options)


class BenchmarkStageTests(unittest.TestCase):
    def test_serial_and_independent_initial_calls_have_matched_models_and_distinct_dependencies(self):
        for policy in ("sequential-four", "independent-four"):
            seen = []
            def answer(r):
                if r["role"] == "reviewer":
                    return review(r)
                previous = json.loads(r["files"]["engine.py"])
                seen.append((r["metadata"]["candidate_id"], previous, json.loads(r["feedback"]), len(cb.evaluations)))
                return builder(code(revision=previous.get("revision", 0) + 1))
            cb = Callbacks(answer)
            result = cb.run(policy)
            self.assertTrue(result["completed"])
            self.assertEqual(result["metrics"]["initial_builder_calls"], 4)
            requests = [r for r in cb.requests if r["role"] == "builder"]
            self.assertEqual({r["model"] for r in requests}, {"cheap"})
            self.assertEqual({r["metadata"]["phase"] for r in requests}, {"initial"})
            snapshots = result["initial_pool"]["candidates"]
            revisions = [json.loads(snapshots[alias]["files"]["engine.py"])["revision"] for alias in result["candidate_order"]]
            if policy == "sequential-four":
                self.assertEqual(revisions, [1, 2, 3, 4])
                self.assertEqual([row[3] for row in seen], [0, 1, 2, 3])
                for row in seen[1:]:
                    self.assertTrue(row[2]["evidence"][0]["outcome"]["passed"])
                    self.assertEqual(row[2]["evidence"][0]["case"]["id"], "public-0")
            else:
                self.assertEqual(revisions, [1, 1, 1, 1])
                self.assertTrue(all(row[1] == {} and row[3] == 0 and "evidence" not in row[2] for row in seen))

    def test_serial_failed_and_no_op_proposals_inherit_exact_previous_provenance(self):
        def answer(r):
            if r["role"] == "reviewer":
                return review(r, slot="slot-3")
            slot = r["metadata"]["candidate_id"]
            return builder(code()) if slot in {"slot-0", "slot-2"} else None
        result = Callbacks(answer).run("sequential-four")
        self.assertTrue(result["completed"])
        pool = result["initial_pool"]["candidates"]
        hashes = {digest(c["files"]) for c in pool.values()}
        self.assertEqual(len(hashes), 1)
        self.assertTrue(all(c["source_valid"] for c in pool.values()))
        self.assertEqual({c["eligibility_origin"]["label"] for c in pool.values()}, {"stage-0-build-slot-0-1"})
        rows = [r for r in result["trajectory"] if r["kind"] == "builder"]
        self.assertEqual([r["attempt_status"] for r in rows], ["effective_source_proposal",
                         "rejected_source_proposal", "no_effective_source_change", "rejected_source_proposal"])
        self.assertEqual([r["inherited_from"] for r in rows], [None, *result["candidate_order"][:3]])

    def test_serial_invalid_source_never_becomes_eligible_from_a_no_op(self):
        initial = {**project()["initial_files"], "engine.py": json.dumps(code())}
        cb = Callbacks(lambda r: review(r, slot="slot-3") if r["role"] == "reviewer"
                       else builder(code(), extra={"README.md": "tampered"}) if r["metadata"]["candidate_id"] == "slot-0" else builder(code()))
        result = cb.run("sequential-four", initial=initial, limits={"max_reviews": 1})
        self.assertFalse(result["completed"])
        self.assertTrue(all(not c["source_valid"] for c in result["initial_pool"]["candidates"].values()))
        self.assertEqual(result["files"]["README.md"], "trusted")

    def test_approved_baseline_scope_is_preserved_across_serial_rejected_attempts(self):
        initial = {**project()["initial_files"], "engine.py": json.dumps(code())}
        approval = dict(files_sha256=digest(initial), source_valid=True, approval_id="trusted-retained-source")
        result = Callbacks(lambda r: review(r, slot="slot-3") if r["role"] == "reviewer" else None).run(
            "sequential-four", initial=initial, baseline_approval=approval)
        self.assertTrue(result["completed"])
        self.assertTrue(all(c["eligibility_origin"]["kind"] == "approved_baseline"
                            for c in result["initial_pool"]["candidates"].values()))

    def test_scout_barrier_follows_all_retention_and_precedes_every_review(self):
        def answer(r):
            if r["role"] == "builder":
                context = json.loads(r["files"]["verification-context.json"])
                self.assertEqual([c["input"] for c in context["verified_cases"]], ["public0"])
                return builder(code(probe=2))
            self.assertEqual(len(cb.frozen), 1)
            context = json.loads(r["files"]["verification-context.json"])
            self.assertEqual(len(context["verified_cases"]), 2)
            self.assertTrue(all(len(matrix) == 2 for matrix in context["matrix"].values()))
            return review(r)
        cb = Callbacks(answer)
        original = cb.after_initial
        def after_initial(pool):
            self.assertEqual(len(cb.retained), 4)
            self.assertEqual(len(cb.evaluations), 4)
            self.assertFalse(any(r["role"] == "reviewer" for r in cb.requests))
            self.assertTrue(all(len(c["matrix"]) == 1 for c in pool["candidates"].values()))
            return original(pool)
        cb.after_initial = after_initial
        cb.scouts = [dict(scout_id="scout-0", probes=[probe()]), dict(scout_id="scout-1", probes=[])]
        result = cb.run("independent-four")
        self.assertTrue(result["completed"])
        self.assertEqual(result["initial_pool_sha256"], digest(result["initial_pool"]))
        self.assertEqual(len(result["initial_pool"]["evidence_cases"]), 1)
        self.assertEqual(len([e for e in cb.evaluations if "scout-probes" in e[2]]), 4)

    def test_callback_cannot_mutate_frozen_or_live_sources(self):
        cb = Callbacks(lambda r: review(r) if r["role"] == "reviewer" else builder(code()))
        def after_initial(pool):
            bound = digest(pool)
            for candidate in pool["candidates"].values():
                candidate["files"]["engine.py"] = "tampered"
                candidate["source_valid"] = False
                candidate["matrix"].clear()
            pool["candidate_order"].reverse()
            return dict(freeze_id="bound-before-mutation", pool_sha256=bound, scouts=[])
        cb.after_initial = after_initial
        result = cb.run("independent-four")
        self.assertTrue(result["completed"])
        self.assertTrue(all(c["files"]["engine.py"] == json.dumps(code())
                            for c in result["initial_pool"]["candidates"].values()))

    def test_bad_pool_binding_stops_before_scout_admission_or_review(self):
        cb = Callbacks(lambda r: builder(code()))
        cb.after_initial = lambda pool: dict(freeze_id="wrong", pool_sha256="wrong", scouts=[dict(scout_id="scout-0", probes=[probe()])])
        with self.assertRaisesRegex(RuntimeError, "freeze callback"):
            cb.run("independent-four")
        self.assertFalse(cb.gates)
        self.assertEqual(len(cb.requests), 4)

    def test_scout_envelope_caps_and_unique_safe_ids_fail_closed(self):
        bad_scouts = ([dict(scout_id="same", probes=[])] * 2,
                      [dict(scout_id="../unsafe", probes=[])],
                      [dict(scout_id="scout-0", probes=[probe()] * 5)],
                      [dict(scout_id=f"scout-{i}", probes=[]) for i in range(3)],
                      [dict(scout_id="scout-0", probes=[], extra=True)])
        for scouts in bad_scouts:
            with self.subTest(scouts=scouts):
                cb = Callbacks(lambda r: builder(code()))
                cb.scouts = scouts
                with self.assertRaises(RuntimeError):
                    cb.run()
                self.assertFalse(cb.gates)

    def test_scout_and_reviewer_probe_budgets_are_separate_and_cross_applied(self):
        def answer(r):
            if r["role"] == "builder":
                return builder(code(**{f"p{i}": 2 for i in range(16)}))
            round_number = r["metadata"]["round"]
            context = json.loads(r["files"]["verification-context.json"])
            self.assertEqual(context["new_probe_capacity_remaining"], max(0, 12 - round_number * 4))
            return review(r, "inspect" if round_number < 3 else "accept",
                          probes=[probe(f"p{i}") for i in range(4 + round_number * 4, 8 + round_number * 4)] if round_number < 3 else [])
        cb = Callbacks(answer)
        cb.scouts = [dict(scout_id=f"scout-{s}", probes=[probe(f"p{i}") for i in range(s * 4, s * 4 + 4)]) for s in range(2)]
        result = cb.run("independent-four")
        self.assertTrue(result["completed"])
        self.assertEqual(result["metrics"]["admitted_scout_probes"], 8)
        self.assertEqual(result["metrics"]["admitted_new_probes"], 8)
        self.assertEqual(len(result["probe_pool"]), 16)
        self.assertTrue(all(len(matrix) == 17 for matrix in result["matrix"].values()))

    def test_scout_failure_is_visible_to_first_reviewer_and_blocks_acceptance(self):
        cb = Callbacks(lambda r: review(r) if r["role"] == "reviewer" else builder(code(probe=0)))
        cb.scouts = [dict(scout_id="scout-0", probes=[probe()])]
        result = cb.run("independent-four", limits={"max_reviews": 1})
        self.assertFalse(result["completed"])
        self.assertEqual(result["reason_code"], "active_checks_failed")
        self.assertEqual(result["metrics"]["scout_probe_failures_found"], 4)
        context = json.loads(next(r for r in cb.requests if r["role"] == "reviewer")["files"]["verification-context.json"])
        self.assertTrue(all(any(not value["passed"] for value in m.values()) for m in context["matrix"].values()))

    def test_scout_discrimination_enables_reselection_without_changing_initial_pool(self):
        cb = Callbacks(lambda r: review(r, slot="slot-1") if r["role"] == "reviewer" else
                       builder(code(probe=0 if r["metadata"]["candidate_id"] == "slot-0" else 2)))
        cb.scouts = [dict(scout_id="scout-0", probes=[probe()])]
        result = cb.run("independent-four")
        self.assertTrue(result["completed"])
        self.assertEqual(result["metrics"]["scout_probe_discriminating_cases"], 1)
        self.assertEqual(result["metrics"]["scout_probe_failures_found"], 1)
        self.assertTrue(all(len(c["validation_receipts"]) == 1 for c in result["initial_pool"]["candidates"].values()))

    def test_wrong_scout_expectations_rejected_and_duplicates_not_double_counted(self):
        cb = Callbacks(lambda r: review(r) if r["role"] == "reviewer" else builder(code(probe=2)))
        cb.scouts = [dict(scout_id="scout-0", probes=[probe(), probe("wrong", expected=999)]),
                     dict(scout_id="scout-1", probes=[probe()])]
        result = cb.run()
        self.assertTrue(result["completed"])
        self.assertEqual(result["metrics"]["admitted_scout_probes"], 1)
        self.assertEqual(result["metrics"]["rejected_scout_probes"], 2)
        self.assertEqual([r["origin"]["source"] for r in result["probe_receipts"][:2]], ["scout", "scout"])

    def test_repair_and_escalation_models_and_caps_match_between_four_builder_policies(self):
        for policy in ("sequential-four", "independent-four", "strong-anchor"):
            def answer(r):
                if r["role"] == "builder":
                    return None if r["metadata"]["phase"] == "repair" and r["metadata"]["round"] == 2 else builder(code())
                return review(r, {1: "repair", 2: "inspect", 3: "inspect", 4: "accept"}[r["metadata"]["round"]])
            cb = Callbacks(answer)
            result = cb.run(policy)
            self.assertTrue(result["completed"])
            repairs = [r for r in cb.requests if r["role"] == "builder" and r["metadata"]["phase"] == "repair"]
            self.assertEqual([r["model"] for r in repairs], ["strong" if policy == "strong-anchor" else "cheap", "strong"])
            self.assertEqual(result["metrics"]["repairs"], 2)
            self.assertEqual(result["metrics"]["reviewer_calls"], 4)
            self.assertEqual(result["metrics"]["escalations"], 1)
            self.assertGreaterEqual(result["metrics"]["retained_eligibility_preserved"], 1)

    def test_repair_reexecutes_scout_and_public_cases_and_keeps_initial_snapshot(self):
        def answer(r):
            if r["role"] == "reviewer":
                return review(r, "repair" if r["metadata"]["round"] == 1 else "accept")
            return builder(code(probe=0 if r["metadata"]["phase"] == "initial" else 2))
        cb = Callbacks(answer)
        cb.scouts = [dict(scout_id="scout-0", probes=[probe()])]
        result = cb.run()
        self.assertTrue(result["completed"])
        initial = result["initial_pool"]["candidates"][result["selected"]]
        self.assertEqual(json.loads(initial["files"]["engine.py"])["probe"], 0)
        self.assertEqual(json.loads(result["files"]["engine.py"])["probe"], 2)
        repaired = [row for row in cb.evaluations if "-2-all-evidence" in row[2]]
        self.assertEqual(len(repaired), 1)
        self.assertEqual(len(repaired[0][1]), 2)

    def test_prior_stage_probes_remain_available_to_initial_builders(self):
        cb = Callbacks(lambda r: review(r) if r["role"] == "reviewer" else builder(code(probe=2)))
        cb.scouts = [dict(scout_id="scout-0", probes=[probe()])]
        first = cb.run()
        next_cb = Callbacks(lambda r: review(r) if r["role"] == "reviewer" else builder(code(probe=2, revision=2)), stage=1)
        result = next_cb.run("independent-four", prior=first, initial=first["files"])
        self.assertTrue(result["completed"])
        for request in next_cb.requests:
            if request["role"] == "builder":
                cases = json.loads(request["files"]["verification-context.json"])["verified_cases"]
                self.assertIn("probe", [c["input"] for c in cases])

    def test_receipt_source_suite_and_cleanup_bindings_are_mandatory(self):
        for field in ("source_sha256", "suite_sha256", "cleanup_verified"):
            cb = Callbacks(lambda r: builder(code()))
            original = cb.evaluate
            def evaluate(*args):
                receipt = original(*args)
                receipt.pop(field)
                return receipt
            cb.evaluate = evaluate
            with self.assertRaises(RuntimeError):
                cb.run()
            self.assertFalse(cb.frozen)

    def test_provider_failure_during_serial_formation_propagates_without_retry_or_scouts(self):
        def answer(r):
            if r["metadata"]["candidate_id"] == "slot-1":
                raise RuntimeError("unknown usage")
            return builder(code())
        cb = Callbacks(answer)
        with self.assertRaisesRegex(RuntimeError, "unknown usage"):
            cb.run("sequential-four")
        self.assertEqual(len(cb.requests), 2)
        self.assertEqual(len(cb.retained), 1)
        self.assertFalse(cb.frozen)

    def test_exhausted_review_capacity_never_repairs_or_auto_accepts(self):
        cb = Callbacks(lambda r: review(r, "repair", remaining=["R0"]) if r["role"] == "reviewer" else builder(code()))
        result = cb.run(limits={"max_reviews": 1})
        self.assertFalse(result["completed"])
        self.assertEqual(result["metrics"]["repairs"], 0)
        self.assertEqual(result["remaining"], ["R0"])

    def test_private_cases_and_policy_identity_never_enter_review_contents(self):
        cb = Callbacks(lambda r: review(r) if r["role"] == "reviewer" else builder(code()))
        cb.run("sequential-four")
        for request in cb.requests:
            text = json.dumps({k: v for k, v in request.items() if k not in {"metadata", "model"}})
            for secret in ("PRIVATE_HIDDEN_SENTINEL", "PRIVATE_ANSWER_SENTINEL", "PRIVATE_ORACLE_SENTINEL",
                           "/host-private-store/", "sequential-four"):
                self.assertNotIn(secret, text)

    def test_replay_is_deterministic_with_serial_and_independent_pools(self):
        for policy in ("sequential-four", "independent-four"):
            answer = lambda r: review(r) if r["role"] == "reviewer" else builder(code(probe=2))
            first, second = Callbacks(answer), Callbacks(answer)
            first.scouts = second.scouts = [dict(scout_id="scout-0", probes=[probe()])]
            self.assertEqual(first.run(policy), second.run(policy))
            self.assertEqual(first.events, second.events)
            normalize = lambda requests: sorted((digest(r["metadata"]), digest(r)) for r in requests)
            self.assertEqual(normalize(first.requests), normalize(second.requests))


if __name__ == "__main__":
    unittest.main()

