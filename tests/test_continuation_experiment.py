"""Offline safety boundaries for the study controller; no provider or Docker calls."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from gossip_harness import continuation_experiment as runner
from gossip_harness import continuation_transport as transport
from gossip_harness.blackbox_validator import BlackboxValidator
from gossip_harness.continuation_stage import run_continuation_stage
from gossip_harness.gitstore import GitStore
from gossip_harness.ledger import BudgetExceeded, Ledger
from gossip_harness.worker import WorkerFailure


def execution_receipt(files, cases, *, passed=False):
    prepared, execution_cases, _ = BlackboxValidator._inputs(files, cases)
    return dict(status="passed" if passed else "failed", passed=passed, cleanup_verified=True,
        source_sha256=runner.digest(prepared), suite_sha256=runner.digest(execution_cases),
        image_id="unit-test-image", case_timeout_seconds=runner.CASE_TIMEOUT,
        timeout_seconds=runner.SUITE_TIMEOUT,
        outcomes=[dict(index=index, passed=passed) for index in range(len(cases))])


class ContinuationBudgetTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.ledger = Ledger(self.root / "budget.sqlite", budget_units=1000)
        self.namespace = "continuation/study"

    def lease(self, task):
        self.ledger.add_task(task)
        return self.ledger.claim(task, "test", now=time.time(), ttl=300)

    def budget(self, cap):
        return runner.StudyBudget(self.ledger, self.namespace, cap, self.root / "admission.lock")

    def test_concurrent_distinct_controllers_reserve_full_amount_under_one_cap(self):
        count = 8
        leases = [self.lease(f"{self.namespace}/case-{index}") for index in range(count)]
        start = threading.Barrier(count)

        def attempt(index):
            budget = self.budget(100)
            start.wait(timeout=10)
            try:
                budget.reserve(leases[index].task_id + "/request", leases[index], 30)
                return True
            except BudgetExceeded:
                return False

        with ThreadPoolExecutor(max_workers=count) as pool:
            admitted = list(pool.map(attempt, range(count)))
        self.assertEqual(sum(admitted), 3)
        self.assertEqual(self.ledger.budget()["spent_or_reserved"], 90)
        with sqlite3.connect(self.ledger.path) as db:
            rows = db.execute("SELECT amount,spent,state FROM reservations").fetchall()
        self.assertEqual(rows, [(30, None, "reserved")] * 3)

    def test_idempotent_reservation_keeps_amount_and_settlement_releases_only_unused_units(self):
        lease = self.lease(self.namespace + "/case")
        budget = self.budget(40)
        first, second = lease.task_id + "/first", lease.task_id + "/second"
        budget.reserve(first, lease, 40)
        budget.reserve(first, lease, 40)
        with self.assertRaises(BudgetExceeded):
            budget.reserve(second, lease, 1)
        self.ledger.settle(first, 7)
        budget.reserve(first, lease, 40)
        with self.assertRaisesRegex(ValueError, "different parameters"):
            budget.reserve(first, lease, 39)
        budget.reserve(second, lease, 33)
        self.assertEqual(self.ledger.budget()["spent_or_reserved"], 40)
        with self.assertRaises(BudgetExceeded):
            budget.reserve(lease.task_id + "/third", lease, 1)

    def test_exact_namespace_excludes_lookalikes_and_rejects_cross_task_reservations(self):
        outside = self.lease(self.namespace + "-other/case")
        self.ledger.reserve(outside.task_id + "/request", outside, 70, now=time.time())
        lease = self.lease(self.namespace + "/case")
        budget = self.budget(30)
        budget.reserve(lease.task_id + "/request", lease, 30)
        self.assertEqual(self.ledger.budget()["spent_or_reserved"], 100)
        for reservation, owner in ((outside.task_id + "/new", outside),
                                   (self.namespace + "/different/request", lease),
                                   (lease.task_id + "-other/request", lease)):
            with self.subTest(reservation=reservation), self.assertRaises(ValueError):
                budget.reserve(reservation, owner, 0)
        for units in (True, -1, 0.5):
            with self.subTest(units=units), self.assertRaises(ValueError):
                budget.reserve(lease.task_id + "/bad", lease, units)

    def test_shared_ledger_limit_still_applies_when_study_has_room(self):
        outside = self.lease("another-study/case")
        self.ledger.reserve("another-study/case/request", outside, 980, now=time.time())
        lease = self.lease(self.namespace + "/case")
        budget = self.budget(100)
        with self.assertRaises(BudgetExceeded):
            budget.reserve(lease.task_id + "/too-large", lease, 21)
        budget.reserve(lease.task_id + "/fits", lease, 20)
        self.assertEqual(self.ledger.budget()["remaining"], 0)

    def test_live_ownership_excludes_second_run_and_releases_on_failure(self):
        with self.assertRaisesRegex(RuntimeError, "owner stopped"):
            with runner.live_ownership(self.ledger.path):
                with self.assertRaisesRegex(RuntimeError, "Another continuation run"):
                    with runner.live_ownership(self.ledger.path):
                        self.fail("A second controller acquired the same live budget")
                raise RuntimeError("owner stopped")
        with runner.live_ownership(self.ledger.path):
            self.assertTrue(self.ledger.path.is_file())
        with self.assertRaisesRegex(ValueError, "existing shared ledger"):
            with runner.live_ownership(self.root / "missing.sqlite"):
                self.fail("Live ownership created a new ledger")
        self.assertFalse((self.root / "missing.sqlite").exists())

    def test_public_live_entrypoint_holds_ownership_before_any_run_work(self):
        with runner.live_ownership(self.ledger.path):
            with patch.object(runner, "_run_owned") as owned:
                with self.assertRaisesRegex(RuntimeError, "Another continuation run"):
                    runner.run(self.root / "second-output", live=True, budget_ledger=self.ledger.path)
            owned.assert_not_called()


class ContinuationTimingTests(unittest.TestCase):
    def test_concurrent_spans_persist_starts_and_close_success_and_base_exception(self):
        class Stopped(BaseException):
            pass

        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "timings.json"
            spans = runner.Spans(path)
            count = 6
            active = threading.Barrier(count)

            def operation(index):
                try:
                    with spans.measure("request", index=index) as row:
                        persisted = runner.read(path)["spans"][row["span_id"]]
                        self.assertEqual(persisted["status"], "running")
                        self.assertNotIn("finished_monotonic_ns", persisted)
                        spans.annotate(row, durable_request_id=f"request-{index}")
                        active.wait(timeout=10)
                        if index == 0:
                            raise Stopped("known interruption")
                    return "finished"
                except Stopped:
                    return "failed"

            with spans.measure("study"):
                with ThreadPoolExecutor(max_workers=count) as pool:
                    outcomes = list(pool.map(operation, range(count)))
            persisted = runner.read(path)
            self.assertEqual(outcomes.count("failed"), 1)
            self.assertIn("overlap", persisted["interpretation"])
            rows = persisted["spans"]
            self.assertEqual(len(rows), count + 1)
            self.assertEqual({row["span_id"] for row in rows}, set(range(count + 1)))
            for row in rows:
                self.assertEqual(row["clock_id"], persisted["clock_id"])
                self.assertGreaterEqual(row["finished_monotonic_ns"], row["started_monotonic_ns"])
                self.assertGreaterEqual(row["elapsed_seconds"], 0)
                self.assertIn("+00:00", row["started_utc"])
                self.assertIn("+00:00", row["finished_utc"])
                self.assertIn(row["status"], ("finished", "failed"))
            failed = [row for row in rows if row["status"] == "failed"]
            self.assertEqual(failed[0]["error_type"], "Stopped")
            requests = [row for row in rows if row["kind"] == "request"]
            self.assertEqual({row["durable_request_id"] for row in requests},
                             {f"request-{index}" for index in range(count)})
            self.assertLess(max(row["started_monotonic_ns"] for row in requests),
                            min(row["finished_monotonic_ns"] for row in requests))


class ContinuationRehearsalTests(unittest.TestCase):
    def proof(self):
        expected = dict(roster=[["transport", policy, repetition]
            for repetition in range(2) for policy in runner.POLICIES], image="unit-test-image")
        rows = []
        for project, policy, repetition in expected["roster"]:
            rows.append(dict(project_id=project, policy=policy, repetition=repetition,
                accepted=True, milestones_completed=2, physical_provider_calls=0, usage_micro_usd=0,
                metrics=dict(retained_eligibility_preserved=2, escalations=2, stagnation_events=2, repairs=4),
                scouts=dict(calls=4, admitted=4, rejected=8) if policy == "scout-assisted"
                    else dict(calls=0, admitted=0, rejected=0),
                invocations=[dict(usage_units=0, physical_dispatch=False,
                    metadata=dict(api_calls=0, rehearsal_noop=True)) for _ in range(2)]))
        return expected, dict(experiment=runner.PROTOCOL, mode="rehearsal", contract=deepcopy(expected),
            contract_sha=runner.digest(expected), status="finished", unexecuted=[], censored=[],
            active_case=None, incremental_micro_usd=0, cases=rows)

    def test_exact_ordered_roster_and_finished_zero_charge_contract_are_required(self):
        expected, proof = self.proof()
        runner.validate_rehearsal(proof, expected)
        mutations = (("experiment", "other"), ("mode", "live"), ("status", "running"),
            ("contract", {**expected, "image": "other-image"}), ("contract_sha", "0" * 64),
            ("unexecuted", [expected["roster"][0]]), ("censored", [expected["roster"][0]]),
            ("active_case", expected["roster"][0]), ("incremental_micro_usd", 1),
            ("cases", proof["cases"][:-1]), ("cases", list(reversed(proof["cases"]))),
            ("cases", [proof["cases"][0]] * 4))
        for field, value in mutations:
            with self.subTest(field=field, value=value):
                altered = deepcopy(proof)
                altered[field] = value
                with self.assertRaises(ValueError):
                    runner.validate_rehearsal(altered, expected)

    def test_rehearsal_requires_noop_eligibility_stagnation_escalation_and_offline_calls(self):
        expected, proof = self.proof()
        mutations = (("accepted", False), ("accepted", 1), ("repetition", False),
            ("milestones_completed", 1), ("physical_provider_calls", 1), ("usage_micro_usd", 1),
            ("metrics.retained_eligibility_preserved", 1), ("metrics.escalations", 1),
            ("metrics.stagnation_events", 1), ("metrics.repairs", 3), ("invocations", []),
            ("scouts.calls", 1))
        for field, value in mutations:
            with self.subTest(field=field, value=value):
                altered = deepcopy(proof)
                target = altered["cases"][0]
                keys = field.split(".")
                for key in keys[:-1]:
                    target = target[key]
                target[keys[-1]] = value
                with self.assertRaises(ValueError):
                    runner.validate_rehearsal(altered, expected)
        for field, value in (("usage_units", 1), ("physical_dispatch", True),
                             ("metadata", dict(api_calls=1, rehearsal_noop=True)),
                             ("metadata", dict(api_calls=0))):
            with self.subTest(call_field=field):
                altered = deepcopy(proof)
                altered["cases"][0]["invocations"][0][field] = value
                with self.assertRaises(ValueError):
                    runner.validate_rehearsal(altered, expected)

    def test_scout_policy_requires_both_independent_and_negative_controls(self):
        expected, proof = self.proof()
        for field, value in (("calls", 3), ("admitted", 3), ("rejected", 7)):
            with self.subTest(field=field):
                altered = deepcopy(proof)
                altered["cases"][1]["scouts"][field] = value
                with self.assertRaises(ValueError):
                    runner.validate_rehearsal(altered, expected)

    def test_real_stage_scripted_controls_fit_caps_and_preserve_noop_eligibility(self):
        initial, prior = deepcopy(transport.PROJECT["initial_files"]), {}
        for stage in (0, 1):
            requests, failures, retained = [], [], {}

            def invoke(**request):
                requests.append(deepcopy(request))
                try:
                    return runner.known_result(transport.PROJECT, transport, stage, request["metadata"])
                except WorkerFailure as failure:
                    self.assertEqual(failure.usage_units, 0)
                    self.assertEqual(failure.metadata, {"api_calls": 0, "rehearsal_noop": True})
                    failures.append(failure)
                    return None

            def retain(files, label):
                retained[label] = deepcopy(files)
                sha = runner.digest(files)
                return dict(store_path="/unexecuted-test-store/" + label,
                            tip_sha=sha[:40], files_sha256=sha)

            def evaluate(files, cases, label):
                self.assertIn(files, retained.values())
                return execution_receipt(files, cases, passed=True)

            result = run_continuation_stage(transport.PROJECT, stage, "strong-reviewed", initial, prior,
                invoke=invoke, retain=retain, evaluate=evaluate,
                validate_probes=runner.oracle_gate(transport, transport.PROJECT), emit=lambda *args, **kwargs: None,
                limits=dict(max_reviews=6, max_repairs=4, max_new_probes=8,
                            max_escalations=2, stagnation_reviews=2),
                baseline_approval=dict(files_sha256=runner.digest(initial), source_valid=True,
                                       approval_id="trusted-test-source"))
            with self.subTest(stage=stage):
                self.assertTrue(result["completed"])
                self.assertEqual(result["files"], transport.known_files(stage))
                self.assertEqual(len(failures), 1)
                self.assertGreaterEqual(result["metrics"]["retained_eligibility_preserved"], 1)
                self.assertGreaterEqual(result["metrics"]["escalations"], 1)
                self.assertGreaterEqual(result["metrics"]["stagnation_events"], 1)
                self.assertGreaterEqual(result["metrics"]["repairs"], 2)
                self.assertLessEqual(result["metrics"]["repairs"], 4)
                self.assertLessEqual(sum(row["role"] == "reviewer" for row in requests), 6)
                self.assertEqual([row["metadata"]["round"] for row in requests if row["role"] == "reviewer"],
                                 [1, 2, 3, 4])
            initial, prior = result["files"], result


class ContinuationBarrierTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        temporary = tempfile.TemporaryDirectory()
        cls.addClassCleanup(temporary.cleanup)
        cls.files = {"solution.py": "raise AssertionError('candidate must never execute on host')\n"}
        cls.store = GitStore.create(Path(temporary.name) / "frozen.git", cls.files)
        cls.head = cls.store.head()

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.expected = dict(roster=[["transport", policy, repetition]
            for repetition in range(2) for policy in runner.POLICIES], image="unit-test-image")
        self.paths = []
        for project, policy, repetition in self.expected["roster"]:
            run_id = f"{project}-{policy}-{repetition}"
            path = self.root / run_id / "trajectory.json"
            state = dict(contract_sha=runner.digest(self.expected), run_id=run_id,
                project_id=project, policy=policy, repetition=repetition, terminal="visible_complete",
                files=deepcopy(self.files), final_binding=dict(store_path=str(self.store.path),
                    accepted_head=self.head, tip_sha=self.head, files_sha256=runner.digest(self.files)),
                stages=[dict(stage_index=index, completed=True, files=deepcopy(self.files),
                    evidence_cases=[], probe_pool=[], invocations=[], metrics={},
                    scouts=dict(calls=0, admitted=0, rejected=0)) for index in range(2)])
            runner.save(path, state)
            self.paths.append(path)
        self.config = dict(root=str(self.paths[0].parent), run_id=self.paths[0].parent.name,
            project_id="transport", policy="strong-maintainer", repetition=0, image="unit-test-image")

    def freeze(self):
        manifest = runner.freeze_trajectories(self.root, self.expected)
        return manifest, runner.sha_file(self.root / "frozen-trajectories.json")

    def test_missing_fourth_trajectory_cannot_create_freeze_or_start_evaluation(self):
        self.paths[-1].unlink()
        with self.assertRaises(FileNotFoundError):
            self.freeze()
        self.assertFalse((self.root / "frozen-trajectories.json").exists())
        with patch.object(runner, "BlackboxValidator") as validator:
            with self.assertRaises(FileNotFoundError):
                runner.finalize(self.root, self.config, self.expected, "0" * 64,
                                runner.Spans(self.root / "timings.json"))
        validator.assert_not_called()

    def test_complete_freeze_binds_all_four_exact_trajectories_and_real_git_sources(self):
        manifest, sha = self.freeze()
        states = runner.validate_freeze(self.root, self.expected, sha)
        self.assertEqual(len(states), 4)
        self.assertEqual([[row[key] for key in ("project_id", "policy", "repetition")]
                          for row in manifest["trajectories"]], self.expected["roster"])
        self.assertIs(manifest["private_evaluation_started"], False)
        self.assertEqual({state["final_binding"]["tip_sha"] for state in states.values()}, {self.head})

    def test_trajectory_tampering_blocks_other_case_finalization_before_validator(self):
        _, sha = self.freeze()
        state = runner.read(self.paths[-1])
        state["stages"][-1]["evidence_cases"] = [dict(input="tampered")]
        runner.save(self.paths[-1], state)
        with patch.object(runner, "BlackboxValidator") as validator:
            with self.assertRaisesRegex(ValueError, "Frozen trajectory changed"):
                runner.finalize(self.root, self.config, self.expected, sha,
                                runner.Spans(self.root / "timings.json"))
        validator.assert_not_called()

    def test_recomputed_source_digest_cannot_hide_git_mismatch_before_freeze(self):
        state = runner.read(self.paths[-1])
        state["files"]["solution.py"] = "tampered source\n"
        state["stages"][-1]["files"] = deepcopy(state["files"])
        state["final_binding"]["files_sha256"] = runner.digest(state["files"])
        runner.save(self.paths[-1], state)
        with self.assertRaisesRegex(ValueError, "Git/source binding changed"):
            self.freeze()
        self.assertFalse((self.root / "frozen-trajectories.json").exists())

    def test_terminal_and_milestone_history_cannot_claim_unattempted_completion(self):
        original = runner.read(self.paths[0])
        variants = []
        omitted = deepcopy(original)
        omitted["stages"].pop()
        variants.append(omitted)
        reordered = deepcopy(original)
        reordered["stages"].reverse()
        variants.append(reordered)
        incomplete = deepcopy(original)
        incomplete["stages"][0]["completed"] = False
        variants.append(incomplete)
        forged_head = deepcopy(original)
        forged_head["final_binding"]["accepted_head"] = "0" * 40
        variants.append(forged_head)
        for index, state in enumerate(variants):
            with self.subTest(variant=index), self.assertRaises(ValueError):
                runner.validate_trajectory(state, self.expected, self.expected["roster"][0])

    def test_changed_manifest_and_rehashed_incomplete_or_duplicate_rosters_are_rejected(self):
        manifest, sha = self.freeze()
        path = self.root / "frozen-trajectories.json"
        modified = deepcopy(manifest)
        modified["frozen_utc"] = "changed"
        runner.save(path, modified)
        with self.assertRaisesRegex(ValueError, "manifest changed"):
            runner.validate_freeze(self.root, self.expected, sha)
        for rows in (manifest["trajectories"][:-1], manifest["trajectories"][:1] * 4,
                     list(reversed(manifest["trajectories"]))):
            with self.subTest(rows=[row["run_id"] for row in rows]):
                runner.save(path, {**manifest, "trajectories": rows})
                with self.assertRaises(ValueError):
                    runner.validate_freeze(self.root, self.expected, runner.sha_file(path))

    def test_all_four_are_durable_before_first_validation_and_private_is_last(self):
        _, sha = self.freeze()
        observed = []

        def evaluate(files, cases):
            states = runner.validate_freeze(self.root, self.expected, sha)
            self.assertEqual(len(states), 4)
            self.assertTrue(all(path.is_file() for path in self.paths))
            observed.append([case["id"] for case in cases])
            return execution_receipt(files, cases)

        with patch.object(runner, "BlackboxValidator") as validator:
            validator.return_value.evaluate.side_effect = evaluate
            result = runner.finalize(self.root, self.config, self.expected, sha,
                                     runner.Spans(self.root / "timings.json"))
        self.assertEqual(observed, [[case["id"] for case in runner.cases_for(transport.PROJECT, 1, kind)]
                                    for kind in ("visible", "hidden")])
        self.assertEqual(result["status"], "final_quality_failed")
        self.assertFalse(result["accepted"])
        self.assertIsNone(result["release_head"])
        self.assertEqual(runner.read(self.paths[0].parent / "result.json"), result)

    def test_mutation_during_visible_evaluation_prevents_private_dispatch(self):
        _, sha = self.freeze()

        def evaluate(files, cases):
            state = runner.read(self.paths[-1])
            state["unexpected_mutation"] = True
            runner.save(self.paths[-1], state)
            return execution_receipt(files, cases)

        with patch.object(runner, "BlackboxValidator") as validator:
            validator.return_value.evaluate.side_effect = evaluate
            with self.assertRaisesRegex(ValueError, "Frozen trajectory changed"):
                runner.finalize(self.root, self.config, self.expected, sha,
                                runner.Spans(self.root / "timings.json"))
            self.assertEqual(validator.return_value.evaluate.call_count, 1)
        self.assertTrue((self.paths[0].parent / "final-visible-receipt.json").exists())
        self.assertFalse((self.paths[0].parent / "final-private-receipt.json").exists())


class ContinuationScoutTests(unittest.TestCase):
    def test_concurrent_scouts_get_independent_public_inputs_and_oracle_rejects_bad_labels(self):
        for stage in (0, 1):
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                initial = deepcopy(transport.PROJECT["initial_files"])
                prior = dict(probe_pool=[], retained_marker={"value": "unchanged"})
                originals = deepcopy((initial, prior))
                arrived = threading.Barrier(2)
                requests = {}
                lock = threading.Lock()

                def invoke(**request):
                    index = request["metadata"]["scout_index"]
                    with lock:
                        requests[index] = deepcopy(request)
                    request["files"][f"peer-{index}-private.txt"] = "must not be shared"
                    arrived.wait(timeout=10)
                    self.assertNotIn(f"peer-{1 - index}-private.txt", request["files"])
                    return runner.known_result(transport.PROJECT, transport, stage, request["metadata"])

                updated, evidence = runner.scout_probes(transport.PROJECT, transport, stage,
                    initial, prior, invoke, root, runner.Spans(root / "timings.json"), "test-run")
                self.assertEqual((initial, prior), originals)
                self.assertEqual(set(requests), {0, 1})
                self.assertEqual(requests[0]["files"], requests[1]["files"])
                self.assertEqual(evidence["shared_input_sha256"], runner.digest(requests[0]["files"]))
                context = json.loads(requests[0]["files"]["scout-context.json"])
                self.assertEqual(set(context), {"project_id", "stage_index", "milestones", "public_cases"})
                self.assertEqual(len(context["milestones"]), stage + 1)
                self.assertEqual(context["public_cases"], json.loads(json.dumps(
                    runner.cases_for(transport.PROJECT, stage, "visible"))))
                for request in requests.values():
                    self.assertEqual(request["allowed_paths"], ("probes.json",))
                    self.assertEqual(request["model"], "cheap")
                    self.assertEqual(request["feedback"], "")
                    self.assertNotIn("hidden_cases", request["files"]["scout-context.json"])
                    self.assertNotIn("known_files", request["files"]["scout-context.json"])
                self.assertEqual((evidence["calls"], evidence["admitted"], evidence["rejected"]), (2, 2, 4))
                self.assertEqual(len(updated["probe_pool"]), 2)
                for index, case in enumerate(updated["probe_pool"]):
                    self.assertEqual(case["origin"]["scout_index"], index)
                    self.assertEqual(case["expected"], transport.reference(stage, case["input"]))
                for receipt in evidence["receipts"]:
                    self.assertEqual({row["reason"] for row in receipt["rejected"]},
                                     {"wrong_expectation", "duplicate_input"})
                    self.assertTrue(all("expected" not in row for row in receipt["rejected"]))
                self.assertEqual(runner.read(root / "scouts.json"), evidence)


if __name__ == "__main__":
    unittest.main()
