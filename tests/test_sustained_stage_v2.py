"""Version-two controller boundaries; no candidate, Git, Docker, or provider I/O."""

from copy import deepcopy
import hashlib
import json
import threading
import unittest
from unittest.mock import patch

from gossip_harness.sustained_stage_v2 import PROTOCOL, run_stage
from gossip_harness.worker import WorkerResult


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=True,
        allow_nan=False, separators=(",", ":")).encode()).hexdigest()


def project():
    return {
        "id": "batch-test", "title": "A cumulative repository",
        "initial_files": {"engine.py": "{}", "README.md": "trusted"},
        "allowed_paths": ("engine.py",),
        "stages": [
            {"spec": "Return old=1", "requirements": ["old"],
             "visible_cases": [{"id": "old", "input": "old", "expected": 1, "requirement": "old"}],
             "hidden_cases": [{"input": "PRIVATE-HIDDEN"}],
             "known_files": {"engine.py": "PRIVATE-REFERENCE"}},
            {"spec": "Also return new=2", "requirements": ["new"],
             "visible_cases": [{"id": "new", "input": "new", "expected": 2, "requirement": "new"}],
             "hidden_cases": [{"input": "PRIVATE-FUTURE"}],
             "known_files": {"engine.py": "PRIVATE-REFERENCE"}},
        ],
    }


def builder(values=None, *, control=None, **extra):
    return WorkerResult({"engine.py": json.dumps({"old": 1} if values is None else values),
        "control.json": json.dumps({"action": "complete", "notes": "checkpoint", "remaining": []})
            if control is None else control, **extra}, "scripted", 0, {"api_calls": 0})


def reviewer(candidate="c0", action="accept", remaining=None):
    return WorkerResult({"review.json": json.dumps({"candidate": candidate, "action": action,
        "notes": "Review checkpoint", "remaining": remaining or []})}, "scripted", 0, {"api_calls": 0})


class Callbacks:
    def __init__(self, answer=None, *, stage=0):
        self.project, self.stage = project(), stage
        self.answer = answer or (lambda request: reviewer() if request["role"] == "reviewer" else builder())
        self.requests, self.batches, self.order, self.events = [], [], [], []
        self.retained, self.proofs = {}, {}
        self.response_filter = lambda values: values
        self.physical = 0
        self.lock = threading.Lock()
        self.controller = threading.get_ident()

    def invoke(self, **request):
        with self.lock:
            self.requests.append(deepcopy(request))
        return self.answer(request)

    def retain(self, files, label):
        assert threading.get_ident() == self.controller
        self.order.append(("retain", label))
        self.retained[label] = deepcopy(files)
        return {"store_path": "HOST-BINDING-SENTINEL/" + label, "tip_sha": digest(files),
                "files_sha256": digest(files), "private_metadata": "HOST-RETENTION-SENTINEL"}

    def evaluate_many(self, batch):
        assert threading.get_ident() == self.controller
        self.order.append(("batch", len(batch)))
        self.batches.append(deepcopy(batch))
        cases = [case for stage in self.project["stages"][:self.stage + 1] for case in stage["visible_cases"]]
        envelopes = []
        for item in batch:
            assert item["files"] == self.retained[item["label"]]
            assert item["source_sha256"] == digest(item["files"])
            key = (item["source_sha256"], digest(cases))
            reused = key in self.proofs
            if not reused:
                self.physical += 1
                values = json.loads(item["files"]["engine.py"])
                outcomes = [dict(index=index, actual=values.get(case["input"]),
                    passed=values.get(case["input"]) == case["expected"], status="scripted")
                    for index, case in enumerate(cases)]
                passed = all(row["passed"] for row in outcomes)
                receipt = dict(passed=passed, status="passed" if passed else "failed",
                    cleanup_verified=True, source_sha256=key[0], suite_sha256=key[1], outcomes=outcomes)
                self.proofs[key] = (receipt, f"execution-{self.physical}")
            receipt, execution_id = self.proofs[key]
            validation = dict(source_sha256=key[0], suite_sha256=key[1], label=item["label"],
                purpose="visible", physical=not reused, execution_id=execution_id,
                reuse=dict(execution_id=execution_id, origin_session_id="fixture", artifact_path="retained/origin")
                    if reused else None,
                private_runtime_detail="HOST-ONLY-SENTINEL")
            envelopes.append(dict(receipt=deepcopy(receipt), validation=validation))
        return self.response_filter(envelopes)

    def emit(self, kind, **data):
        self.events.append(dict(kind=kind, **deepcopy(data)))

    def run(self, policy="cheap-sequential", *, initial=None, prior=None):
        return run_stage(self.project, self.stage, policy,
            self.project["initial_files"] if initial is None else initial, prior,
            invoke=self.invoke, retain=self.retain, evaluate_many=self.evaluate_many, emit=self.emit)


class SustainedStageV2Tests(unittest.TestCase):
    def test_portfolio_retains_serially_then_batches_and_reduces_in_order(self):
        barrier = threading.Barrier(4)
        def answer(request):
            if request["role"] == "reviewer":
                return reviewer("c2")
            barrier.wait(timeout=5)
            return builder()
        callbacks = Callbacks(answer)
        result = callbacks.run("reviewed-portfolio")
        self.assertTrue(result["completed"])
        self.assertEqual((result["selected"], result["stage_protocol"]), ("c2", PROTOCOL))
        self.assertEqual([len(batch) for batch in callbacks.batches], [4])
        self.assertEqual([kind for kind, _ in callbacks.order], ["retain"] * 4 + ["batch"])
        self.assertEqual([row["candidate"] for row in result["trajectory"] if row["kind"] == "builder"],
                         ["c0", "c1", "c2", "c3"])
        self.assertEqual(callbacks.physical, 1)
        self.assertFalse(result["visible_validation"]["physical"])
        self.assertEqual(result["visible_validation"]["reuse"]["execution_id"], "execution-1")

    def test_all_source_identity_is_prepared_before_first_retention(self):
        responses = {}
        def answer(request):
            if request["role"] == "reviewer":
                return reviewer("c3")
            value = builder()
            responses[request["metadata"]["candidate_id"]] = value
            return value
        callbacks = Callbacks(answer)
        original = callbacks.retain
        def retain(files, label):
            responses["c3"].changes["engine.py"] = json.dumps({"old": 99})
            return original(files, label)
        with patch.object(callbacks, "retain", retain):
            result = callbacks.run("reviewed-portfolio")
        self.assertTrue(result["completed"])
        self.assertEqual(json.loads(result["files"]["engine.py"]), {"old": 1})
        self.assertTrue(all(item["source_sha256"] == digest(item["files"])
                            for item in callbacks.batches[0]))

    def test_repair_waits_for_reviewer_and_uses_singleton_batch(self):
        def answer(request):
            if request["role"] == "reviewer":
                return reviewer("c3")
            return builder({} if request["metadata"]["round"] == 1 else {"old": 1})
        callbacks = Callbacks(answer)
        result = callbacks.run("reviewed-portfolio")
        self.assertTrue(result["completed"])
        self.assertEqual([len(batch) for batch in callbacks.batches], [4, 1])
        self.assertEqual(result["metrics"]["reviewer_calls"], 2)
        repair = next(request for request in callbacks.requests
                      if request["role"] == "builder" and request["metadata"]["round"] == 2)
        self.assertEqual(repair["metadata"]["candidate_id"], "c3")
        self.assertTrue(json.loads(repair["feedback"])["review"]["errors"])

    def test_reused_pass_does_not_validate_malformed_completion_control(self):
        callbacks = Callbacks(lambda request: builder(control='{"action":"complete"}'))
        result = callbacks.run()
        self.assertFalse(result["completed"])
        self.assertTrue(result["visible_receipt"]["passed"])
        self.assertEqual(result["attempts"], 8)
        self.assertEqual(result["metrics"]["invalid_controls"], 8)
        self.assertEqual(callbacks.physical, 1)
        self.assertFalse(result["visible_validation"]["physical"])
        self.assertTrue(all(row["control_error"] for row in result["trajectory"]))

    def test_reused_pass_does_not_validate_unauthorized_proposal(self):
        callbacks = Callbacks(lambda request: builder(**{"README.md": "tampered"}))
        initial = {**project()["initial_files"], "engine.py": json.dumps({"old": 1})}
        result = callbacks.run(initial=initial)
        self.assertFalse(result["completed"])
        self.assertEqual(result["files"], initial)
        self.assertTrue(result["visible_receipt"]["passed"])
        self.assertEqual(result["metrics"]["invalid_proposals"], 8)
        self.assertEqual(callbacks.physical, 1)

    def test_reused_pass_after_empty_result_still_needs_valid_proposal(self):
        callbacks = Callbacks(lambda request: None)
        initial = {**project()["initial_files"], "engine.py": json.dumps({"old": 1})}
        result = callbacks.run(initial=initial)
        self.assertFalse(result["completed"])
        self.assertEqual(result["metrics"]["provider_empty_results"], 8)
        self.assertEqual(callbacks.physical, 1)

    def test_cumulative_regression_and_machine_history_survive_batch_boundary(self):
        first = Callbacks().run()
        callbacks = Callbacks(lambda request: builder({"new": 2} if request["metadata"]["round"] == 1
                              else {"old": 1, "new": 2}), stage=1)
        result = callbacks.run(initial=first["files"], prior=first)
        self.assertTrue(result["completed"])
        self.assertEqual(result["metrics"]["regressions"], 1)
        self.assertEqual(result["trajectory"][0]["regressions"], ["old"])
        context = json.loads(callbacks.requests[0]["files"]["__stage_context__.json"])
        self.assertEqual(len(context["prior_machine_history"]), 1)
        self.assertEqual(len(context["milestones"]), 2)

    def test_hidden_and_host_provenance_never_enter_model_context(self):
        callbacks = Callbacks()
        callbacks.run("reviewed-portfolio")
        serialized = json.dumps(callbacks.requests)
        for sentinel in ("PRIVATE-HIDDEN", "PRIVATE-FUTURE", "PRIVATE-REFERENCE", "HOST-ONLY-SENTINEL",
                         "HOST-BINDING-SENTINEL", "HOST-RETENTION-SENTINEL"):
            self.assertNotIn(sentinel, serialized)
        contexts = [request["files"]["__stage_context__.json"] for request in callbacks.requests
                    if request["role"] == "builder"]
        self.assertEqual(len(set(contexts)), 1)

    def test_misordered_or_incomplete_batch_cannot_publish_partial_state(self):
        for change in (lambda rows: rows[:-1], lambda rows: list(reversed(rows))):
            callbacks = Callbacks()
            callbacks.response_filter = change
            with self.assertRaises(RuntimeError):
                callbacks.run("reviewed-portfolio")
            self.assertEqual(len(callbacks.retained), 4)
            self.assertFalse(callbacks.events)
            self.assertEqual(len(callbacks.requests), 4)

    def test_receipt_identity_cleanup_and_provenance_fail_closed(self):
        mutations = [
            lambda row: row["receipt"].update(source_sha256="wrong"),
            lambda row: row["receipt"].update(suite_sha256="wrong"),
            lambda row: row["receipt"].update(cleanup_verified=False),
            lambda row: row["validation"].update(source_sha256="wrong"),
            lambda row: row["validation"].update(purpose="final"),
            lambda row: row["validation"].update(physical=False, reuse=None),
            lambda row: row["validation"].update(physical=1),
            lambda row: row["validation"].update(execution_id=""),
            lambda row: row["receipt"].update(outcomes=[]),
        ]
        for mutate in mutations:
            with self.subTest(mutate=mutate):
                callbacks = Callbacks()
                def corrupt(rows):
                    mutate(rows[-1])
                    return rows
                callbacks.response_filter = corrupt
                with self.assertRaises(RuntimeError):
                    callbacks.run("reviewed-portfolio")
                self.assertFalse(callbacks.events)
                self.assertEqual(len(callbacks.requests), 4)

    def test_batch_input_mutation_is_rejected_without_repair(self):
        callbacks = Callbacks()
        original = callbacks.evaluate_many
        def mutate(batch):
            rows = original(batch)
            batch[0]["files"]["engine.py"] = "changed after validation"
            return rows
        with patch.object(callbacks, "evaluate_many", mutate):
            with self.assertRaisesRegex(RuntimeError, "immutable input"):
                callbacks.run()
        self.assertEqual(len(callbacks.requests), 1)
        self.assertFalse(callbacks.events)

    def test_infrastructure_failure_propagates_without_provider_retry(self):
        callbacks = Callbacks()
        def fail(batch):
            raise RuntimeError("sandbox cleanup failed")
        with patch.object(callbacks, "evaluate_many", fail):
            with self.assertRaisesRegex(RuntimeError, "cleanup failed"):
                callbacks.run()
        self.assertEqual(len(callbacks.requests), 1)
        self.assertEqual(len(callbacks.retained), 1)
        self.assertFalse(callbacks.events)

    def test_continuing_pass_and_invalid_reviewer_remain_bounded_incomplete(self):
        for policy in ("strong-single", "cheap-sequential", "reviewed-portfolio"):
            def answer(request):
                if request["role"] == "reviewer":
                    return WorkerResult({"review.json": "{}"}, "invalid", 0, {})
                return builder(control=json.dumps({"action": "continue", "notes": "keep working", "remaining": []}))
            callbacks = Callbacks(answer)
            result = callbacks.run(policy)
            self.assertFalse(result["completed"])
            self.assertEqual(result["attempts"], 8)
            self.assertTrue(result["visible_receipt"]["passed"])
            self.assertEqual(callbacks.physical, 1)


if __name__ == "__main__":
    unittest.main()
