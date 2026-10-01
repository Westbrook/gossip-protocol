from copy import deepcopy
import hashlib
import json
import threading
import unittest

from gossip_harness.sustained_stage import run_stage
from gossip_harness.worker import WorkerResult


def builder(code, action="complete", remaining=None, notes="checkpoint", **extra):
    changes = {"engine.py": json.dumps(code), "control.json": json.dumps({
        "action": action, "notes": notes, "remaining": remaining or []})}
    changes.update(extra)
    return WorkerResult(changes, "proposal", 1, {})


def reviewer(candidate="c0", action="accept", remaining=None):
    return WorkerResult({"review.json": json.dumps({
        "candidate": candidate, "action": action,
        "notes": "Inspect and preserve the old operation.", "remaining": remaining or []})},
        "review", 1, {})


def project():
    return {
        "id": "test-project", "title": "A small evolving repository",
        "initial_files": {"engine.py": "{}", "helpers.py": "stable", "README.md": "trusted"},
        "allowed_paths": ("engine.py", "helpers.py"),
        "stages": (
            {"spec": "Return old=1.", "requirements": ["R-old"],
             "visible_cases": [{"id": "old-case", "input": "old", "expected": 1,
                                "requirement": "R-old"}],
             "hidden_cases": [{"input": "PRIVATE-HIDDEN-SENTINEL"}],
             "known_files": {"engine.py": "PRIVATE-REFERENCE-SENTINEL"}},
            {"spec": "Preserve old and return new=2.", "requirements": ["R-new"],
             "visible_cases": [{"id": "new-case", "input": "new", "expected": 2,
                                "requirement": "R-new"}],
             "hidden_cases": [{"input": "PRIVATE-FUTURE-SENTINEL"}],
             "known_files": {"engine.py": "PRIVATE-REFERENCE-SENTINEL"}},
        ),
    }


class Callbacks:
    def __init__(self, project, stage_index, answer):
        self.project = project
        self.stage_index = stage_index
        self.answer = answer
        self.requests = []
        self.events = []
        self.order = []
        self.retained = {}
        self.lock = threading.Lock()

    def invoke(self, **kwargs):
        with self.lock:
            self.requests.append(deepcopy(kwargs))
        return self.answer(kwargs)

    def retain(self, files, label):
        self.order.append(("retain", label))
        self.retained[label] = deepcopy(files)
        sha = hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest()
        return {"tip_sha": sha, "files_sha256": sha, "store_path": label}

    def evaluate(self, files, label):
        self.order.append(("evaluate", label))
        assert self.retained[label] == files
        cases = [case for stage in self.project["stages"][:self.stage_index + 1]
                 for case in stage["visible_cases"]]
        values = json.loads(files["engine.py"])
        outcomes = [{"index": index, "actual": values.get(case["input"]),
                     "passed": values.get(case["input"]) == case["expected"],
                     "status": "ok"} for index, case in enumerate(cases)]
        passed = all(outcome["passed"] for outcome in outcomes)
        return {"passed": passed, "status": "passed" if passed else "failed", "outcomes": outcomes}

    def emit(self, kind, **data):
        self.events.append({"kind": kind, **deepcopy(data)})

    def run(self, policy="cheap-sequential", initial=None, prior=None):
        return run_stage(self.project, self.stage_index, policy,
                         initial or self.project["initial_files"], prior,
                         invoke=self.invoke, retain=self.retain,
                         evaluate=self.evaluate, emit=self.emit)


class SustainedStageTests(unittest.TestCase):
    def test_false_completion_is_repaired_with_full_machine_feedback(self):
        callbacks = Callbacks(project(), 0, lambda request: builder(
            {} if request["metadata"]["round"] == 1 else {"old": 1}))
        result = callbacks.run()
        self.assertTrue(result["completed"])
        self.assertEqual(result["attempts"], 2)
        self.assertEqual(result["metrics"]["premature_completion"], 1)
        feedback = json.loads(callbacks.requests[1]["feedback"])
        failed = feedback["visible_validation"]["outcomes"][0]
        self.assertEqual((failed["input"], failed["expected"], failed["actual"]), ("old", 1, None))
        self.assertIn("Completion denied", feedback["control_errors"][0])
        self.assertEqual(callbacks.order, [(operation, label)
                         for label in callbacks.retained for operation in ("retain", "evaluate")])

    def test_cumulative_specs_and_checkpoint_survive_milestone_and_regression(self):
        fixture = project()
        first = Callbacks(fixture, 0, lambda request: builder({"old": 1}, notes="Remember old=1")).run()
        callbacks = Callbacks(fixture, 1, lambda request: builder(
            {"new": 2} if request["metadata"]["round"] == 1 else {"old": 1, "new": 2}))
        result = callbacks.run(initial=first["files"], prior=first)
        self.assertTrue(result["completed"])
        self.assertEqual(result["metrics"]["regressions"], 1)
        self.assertEqual(result["trajectory"][0]["regressions"], ["old-case"])
        context = json.loads(callbacks.requests[0]["files"]["__stage_context__.json"])
        self.assertEqual(len(context["milestones"]), 2)
        self.assertEqual(context["prior_agent_notes"], "Remember old=1")
        self.assertEqual(len(context["prior_machine_history"]), 1)
        serialized = json.dumps(callbacks.requests)
        for secret in ("PRIVATE-HIDDEN-SENTINEL", "PRIVATE-REFERENCE-SENTINEL", "PRIVATE-FUTURE-SENTINEL"):
            self.assertNotIn(secret, serialized)

    def test_strong_and_cheap_have_identical_visible_evidence_and_limits(self):
        outputs = []
        for policy in ("strong-single", "cheap-sequential"):
            callbacks = Callbacks(project(), 0, lambda request: builder({}, remaining=["R-old"]))
            result = callbacks.run(policy)
            self.assertFalse(result["completed"])
            self.assertEqual(result["attempts"], 8)
            self.assertEqual(result["metrics"]["abandoned_remaining"], ["R-old"])
            self.assertGreaterEqual(result["metrics"]["stagnation_events"], 1)
            self.assertEqual({request["model"] for request in callbacks.requests},
                             {"strong" if policy == "strong-single" else "cheap"})
            outputs.append(json.loads(callbacks.requests[1]["feedback"])["visible_validation"])
        self.assertEqual(outputs[0], outputs[1])

    def test_passing_sources_do_not_override_continue_or_invalid_control(self):
        for artifact in (json.dumps({"action": "continue", "notes": "still checking", "remaining": []}),
                         '{"action":"complete","action":"complete","notes":"","remaining":[]}',
                         json.dumps({"action": "complete", "notes": "", "remaining": ["unknown"]}),
                         '[' * 2000 + '0' + ']' * 2000):
            with self.subTest(artifact=artifact):
                callbacks = Callbacks(project(), 0, lambda request: builder({"old": 1}, **{"control.json": artifact}))
                result = callbacks.run()
                self.assertFalse(result["completed"])
                self.assertTrue(result["visible_receipt"]["passed"])
                self.assertEqual(result["attempts"], 8)

    def test_source_deletion_or_trusted_edit_rejected_atomically(self):
        for extra in ({"README.md": "tampered"}, {"helpers.py": None}, {"unapproved.py": "new"}):
            with self.subTest(extra=extra):
                callbacks = Callbacks(project(), 0, lambda request: builder({"old": 1}, **extra))
                result = callbacks.run()
                self.assertEqual(result["files"], project()["initial_files"])
                self.assertFalse(result["completed"])
                self.assertEqual(result["metrics"]["invalid_proposals"], 8)

    def test_unknown_provider_exception_propagates_without_retry(self):
        count = 0

        def fail(request):
            nonlocal count
            count += 1
            raise RuntimeError("unknown usage, keep reservation")

        callbacks = Callbacks(project(), 0, fail)
        with self.assertRaisesRegex(RuntimeError, "unknown usage"):
            callbacks.run()
        self.assertEqual(count, 1)
        self.assertFalse(callbacks.retained)

    def test_infrastructure_receipt_does_not_trigger_model_repair(self):
        for bad in ({"status": "timeout", "passed": False},
                    {"status": "passed", "passed": True, "outcomes": []},
                    {"status": "passed", "passed": True, "cleanup_verified": False, "outcomes": []},
                    {"status": "failed", "passed": True, "outcomes": [{"passed": True}]}):
            callbacks = Callbacks(project(), 0, lambda request: builder({"old": 1}))
            callbacks.evaluate = lambda files, label: bad
            with self.assertRaises(RuntimeError):
                callbacks.run()
            self.assertEqual(len(callbacks.requests), 1)
            self.assertEqual(len(callbacks.retained), 1)

    def test_portfolio_initial_independence_and_serial_retention(self):
        barrier = threading.Barrier(4)

        def answer(request):
            if request["role"] == "reviewer":
                return reviewer("c2")
            barrier.wait(timeout=5)
            return builder({"old": 1})

        callbacks = Callbacks(project(), 0, answer)
        result = callbacks.run("reviewed-portfolio")
        self.assertTrue(result["completed"])
        self.assertEqual(result["selected"], "c2")
        self.assertEqual(result["metrics"]["builder_calls"], 4)
        self.assertEqual(result["metrics"]["reviewer_calls"], 1)
        initial_requests = [request for request in callbacks.requests if request["role"] == "builder"]
        self.assertEqual(len({_json["files"]["__stage_context__.json"] for _json in initial_requests}), 1)
        self.assertTrue(all(request["files"]["engine.py"] == "{}" for request in initial_requests))
        self.assertEqual(len(callbacks.retained), 4)
        review_request = next(request for request in callbacks.requests if request["role"] == "reviewer")
        self.assertEqual(review_request["allowed_paths"], ("review.json",))
        self.assertIn("candidates/c3/helpers.py", review_request["files"])

    def test_reviewer_cannot_accept_failure_then_repairs_selected_slot(self):
        def answer(request):
            if request["role"] == "reviewer":
                return reviewer("c3")
            return builder({"old": 1} if request["metadata"]["round"] > 1 else {})

        callbacks = Callbacks(project(), 0, answer)
        result = callbacks.run("reviewed-portfolio")
        self.assertTrue(result["completed"])
        self.assertEqual(result["selected"], "c3")
        self.assertEqual(result["metrics"]["reviewer_calls"], 2)
        self.assertEqual(result["metrics"]["builder_calls"], 5)
        repairs = [request for request in callbacks.requests if request["role"] == "builder"
                   and request["metadata"]["round"] > 1]
        self.assertEqual(repairs[0]["metadata"]["candidate_id"], "c3")
        self.assertTrue(json.loads(repairs[0]["feedback"])["review"]["errors"])

    def test_invalid_reviews_are_bounded_and_passing_fallback_is_incomplete(self):
        def answer(request):
            if request["role"] == "reviewer":
                return WorkerResult({"review.json": "{}", "engine.py": "unauthorized"}, "bad", 1, {})
            return builder({"old": 1})

        callbacks = Callbacks(project(), 0, answer)
        result = callbacks.run("reviewed-portfolio")
        self.assertFalse(result["completed"])
        self.assertTrue(result["visible_receipt"]["passed"])
        self.assertEqual(result["selected"], "c0")
        self.assertEqual(result["metrics"]["reviewer_calls"], 5)
        self.assertEqual(result["metrics"]["builder_calls"], 8)
        self.assertEqual(result["metrics"]["invalid_controls"], 5)

    def test_review_remaining_is_retained_when_work_stops(self):
        def answer(request):
            if request["role"] == "reviewer":
                return reviewer("c0", "repair", ["R-old"])
            return builder({"old": 1})

        result = Callbacks(project(), 0, answer).run("reviewed-portfolio")
        self.assertFalse(result["completed"])
        self.assertEqual(result["metrics"]["abandoned_remaining"], ["R-old"])

    def test_portfolio_repairs_receive_the_selected_candidates_stagnation_warning(self):
        def answer(request):
            if request["role"] == "reviewer":
                return reviewer("c0", "repair", ["R-old"])
            return builder({})

        callbacks = Callbacks(project(), 0, answer)
        result = callbacks.run("reviewed-portfolio")
        self.assertFalse(result["completed"])
        repairs = [request for request in callbacks.requests if request["role"] == "builder"
                   and request["metadata"]["round"] > 1]
        self.assertTrue(json.loads(repairs[-1]["feedback"])["stagnation_warning"])

    def test_incomplete_fallback_notes_describe_its_own_implementation(self):
        def answer(request):
            if request["role"] == "reviewer":
                return WorkerResult({"review.json": json.dumps({
                    "candidate": "c3", "action": "repair", "notes": "c3-only implementation advice",
                    "remaining": ["R-old"]})}, "review", 1, {})
            return builder({"old": 1}, notes=request["metadata"]["candidate_id"] + " own checkpoint")

        result = Callbacks(project(), 0, answer).run("reviewed-portfolio")
        self.assertFalse(result["completed"])
        self.assertEqual(result["selected"], "c0")
        self.assertEqual(result["notes"], "c0 own checkpoint")


if __name__ == "__main__":
    unittest.main()
