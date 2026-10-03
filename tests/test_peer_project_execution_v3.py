"""Host-adapter controls with explicit mocks; no Docker or candidate execution.

The physical-path tests mock Docker observations and BlackboxValidator.evaluate
inside the test process. They prove binding/retention only and are not retained
as physical study evidence. The fixture path itself cannot authorize release.
"""
from __future__ import annotations

from dataclasses import replace
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from gossip_harness.blackbox_validator import BlackboxValidator, PROTOCOL as BLACKBOX_PROTOCOL
from gossip_harness.gitstore import GitStore, _run
from gossip_harness.peer_coding_dispatch_v1 import source_digest
from gossip_harness.peer_project_contract_v2 import Context, EvidenceRef, NamedSource, ReleaseTarget
from gossip_harness.peer_project_execution_v3 import (
    ExecutionError, ExecutionPolicy, ExecutionSubject, ExecutionUnknown, ProjectExecution,
    _blackbox_json, _json, _sha, assertion_digest, required_suite,
)
from gossip_harness.peer_review_release_v2 import RequiredCheck, RequiredSuite, suite_digest


class MemoryMesh:
    node_id = "execution"

    def __init__(self):
        self.commands = {}
        self.payloads = {}
        self.calls = 0
        self.fail_after_publish = False

    def publish(self, kind, payload, command_id):
        self.calls += 1
        ref = EvidenceRef(_sha(command_id.encode()), self.node_id, kind, _sha(payload))
        if command_id in self.commands and self.commands[command_id] != ref:
            raise ValueError("Changed publication")
        self.commands[command_id] = ref
        self.payloads[ref] = payload
        if self.fail_after_publish:
            self.fail_after_publish = False
            raise OSError("Lost publication acknowledgment")
        return ref

    def resolve(self, ref):
        return self.payloads.get(ref)


class PeerProjectExecutionV3Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture_root = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.fixture_root.cleanup)
        cls.fixture_files = {"solution.py": "# café\ndef solve(value):\n    return value\n", "library/data.txt": "résumé\n"}
        cls.fixture_store = GitStore.create(Path(cls.fixture_root.name).resolve() / "repo", cls.fixture_files)

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.files = dict(self.fixture_files)
        self.store = self.fixture_store
        self.mesh = MemoryMesh()
        self.context = Context(_sha(b"contract"), "cohort", "trajectory", 1, _sha(b"requirements"))
        self.cases = [{"id": "echo", "requirement": "catalog", "input": {"n": 1}, "expected": {"n": 1}},
                      {"id": "unicode", "input": "café", "expected": "café"}]
        self.suite = required_suite(self.context, self.cases)
        self.policy = ExecutionPolicy("sha256:" + "a" * 64)
        self.calls = 0
        self.mutation = None
        self.runtime = {"server": {"Version": "fixture-runtime"}, "image": {"Id": self.policy.image_id}}
        self.adapters = []
        self.addCleanup(self.close_all)

    def close_all(self):
        for adapter in self.adapters:
            adapter.close()

    def receipt(self, files, cases):
        self.calls += 1
        validator = BlackboxValidator(self.policy.image_id, timeout_seconds=self.policy.timeout_seconds,
                                     case_timeout_seconds=self.policy.case_timeout_seconds)
        receipt = {"schema_version": 1, "protocol": BLACKBOX_PROTOCOL, "passed": True, "status": "passed",
            "container_name": "gossip-blackbox-" + f"{self.calls:032x}", "image_id": self.policy.image_id,
            "adapter_sha256": validator._sandbox.checks_sha256, "source_sha256": _sha(_blackbox_json(files)),
            "suite_sha256": _sha(_blackbox_json(cases)),
            "staging": {"files": len(files), "bytes": sum(len(text.encode()) for text in files.values())},
            "case_count": len(cases), "exit_code": 0, "timed_out": False,
            "timeout_seconds": self.policy.timeout_seconds, "case_timeout_seconds": self.policy.case_timeout_seconds,
            "cleanup_verified": True, "output_truncated": False, "input_delivery_failed": False,
            "output_bytes": 123, "output_sha256": _sha(b"mock-observed-output"), "runtime_seconds": 0.1,
            "outcomes": [{"index": i, "passed": True, "status": "passed", "actual": case["expected"],
                          "id": case["id"], **({"requirement": case["requirement"]} if "requirement" in case else {})}
                         for i, case in enumerate(cases)]}
        if self.mutation:
            self.mutation(receipt)
        return receipt

    def adapter(self, *, mode="fixture", path=None, crash_hook=None):
        # Physical-path mocks are test fixtures, never genuine execution proof.
        if mode == "physical":
            with patch.object(ProjectExecution, "_runtime", return_value=self.runtime):
                result = ProjectExecution(path or self.root / "execution", self.mesh, self.store,
                    producer="execution", execution_contract_sha256=self.context.execution_contract_sha256,
                    cohort_id="cohort", policy=self.policy, mode=mode, crash_hook=crash_hook)
        else:
            result = ProjectExecution(path or self.root / "execution", self.mesh, self.store,
                producer="execution", execution_contract_sha256=self.context.execution_contract_sha256,
                cohort_id="cohort", policy=self.policy, mode=mode, fixture_executor=self.receipt,
                crash_hook=crash_hook)
        self.adapters.append(result)
        return result

    def execute(self, adapter, request_id="request", *, subject=None, cases=None, suite=None):
        suite = suite or self.suite
        subject = subject or adapter.subject(self.context, self.store.head(), suite)
        with patch.object(adapter, "_runtime", return_value=self.runtime), \
                patch.object(adapter.validator, "evaluate", side_effect=self.receipt):
            return adapter.execute(request_id, subject, suite, self.cases if cases is None else cases)

    def target(self, publication):
        subject = publication.subject
        return ReleaseTarget(subject.context, 0, "repo", "refs/heads/protected", subject.commit_oid, "sha1",
            subject.commit_oid, subject.tree_oid, subject.sources, _sha(b"selection"), subject.suite_sha256,
            subject.evaluator_sha256, (publication.receipt_ref,))

    def directory(self, request_id="request"):
        return self.root / "execution" / "executions" / _sha(request_id.encode())

    def test_whole_assertions_include_inputs_expectations_labels_and_order(self):
        self.assertEqual(self.suite.checks[0].assertion_sha256, assertion_digest(self.cases[0]))
        for key, value in (("input", 9), ("expected", False), ("requirement", "query"), ("id", "changed")):
            changed = dict(self.cases[0], **{key: value})
            self.assertNotEqual(assertion_digest(changed), self.suite.checks[0].assertion_sha256)
        self.assertNotEqual(required_suite(self.context, self.cases[::-1]), self.suite)

    def test_case_extras_duplicate_ids_and_nonfinite_values_fail_before_intent(self):
        for cases in ([dict(self.cases[0], passed=True)], [self.cases[0], self.cases[0]],
                      [dict(self.cases[0], input=float("nan"))]):
            with self.subTest(cases=str(cases)[:80]), self.assertRaises(ValueError):
                required_suite(self.context, cases)
        self.assertEqual(self.calls, 0)

    def test_ascii_validator_digest_is_not_candidate_utf8_digest(self):
        adapter = self.adapter()
        publication = self.execute(adapter)
        intent = json.loads((self.directory() / "intent.json").read_bytes())
        self.assertEqual(intent["candidate_source_sha256"], source_digest(self.files))
        self.assertEqual(intent["blackbox_source_sha256"], _sha(_blackbox_json(self.files)))
        self.assertNotEqual(intent["candidate_source_sha256"], intent["blackbox_source_sha256"])
        self.assertEqual(publication.subject.sources[0].sha256, _sha(self.files["library/data.txt"].encode()))

    def test_fixture_evidence_is_published_but_cannot_authorize_release(self):
        adapter = self.adapter()
        publication = self.execute(adapter)
        self.assertEqual(publication.status, "passed")
        self.assertFalse(publication.physically_executed)
        with self.assertRaisesRegex(ExecutionError, "Fixture/independent"):
            adapter.verified_execution(self.target(publication), self.suite)

    def test_mocked_physical_path_binds_exact_immutable_target(self):
        adapter = self.adapter(mode="physical")
        publication = self.execute(adapter)
        evidence = adapter.verified_execution(self.target(publication), self.suite)
        self.assertEqual(evidence.check_results, (("echo", "passed"), ("unicode", "passed")))
        self.assertEqual(evidence.receipt_refs, (publication.receipt_ref,))
        self.assertEqual(evidence.provenance_sha256, adapter.provenance_sha256)
        self.assertTrue(publication.physically_executed)
        self.assertFalse(publication.replayed)

    def test_exact_replay_and_restart_do_not_evaluate_again(self):
        adapter = self.adapter()
        first = self.execute(adapter)
        replay = self.execute(adapter)
        self.assertEqual(first.receipt_ref, replay.receipt_ref)
        self.assertTrue(replay.replayed)
        self.assertFalse(replay.physically_executed)
        adapter.close()
        restored = self.adapter()
        again = self.execute(restored)
        self.assertEqual(first.receipt_ref, again.receipt_ref)
        self.assertEqual(self.calls, 1)
        self.assertEqual(len(self.mesh.commands), 1)

    def test_new_independent_request_ids_physically_reexecute_same_source(self):
        self.policy = replace(self.policy, purpose="repeatability")
        adapter = self.adapter(mode="physical")
        first = self.execute(adapter, "observation-one")
        second = self.execute(adapter, "observation-two")
        self.assertEqual(first.subject, second.subject)
        self.assertNotEqual(first.receipt_ref, second.receipt_ref)
        self.assertEqual(self.calls, 2)
        replay = self.execute(adapter, "observation-one")
        self.assertTrue(replay.replayed)
        self.assertFalse(replay.physically_executed)
        self.assertEqual(self.calls, 2)
        with self.assertRaisesRegex(ExecutionError, "Fixture/independent"):
            adapter.verified_execution(self.target(first), self.suite)

    def test_same_request_with_changed_case_or_source_is_not_reused(self):
        adapter = self.adapter()
        self.execute(adapter)
        changed = [dict(self.cases[0], expected="other"), self.cases[1]]
        suite = required_suite(self.context, changed)
        with self.assertRaisesRegex(ExecutionError, "replay changed"):
            self.execute(adapter, suite=suite, cases=changed)
        self.assertEqual(self.calls, 1)

    def test_suite_reordering_is_rejected_before_evaluation(self):
        adapter = self.adapter()
        with self.assertRaisesRegex(ExecutionError, "Ordered public assertions"):
            self.execute(adapter, cases=self.cases[::-1])
        self.assertEqual(self.calls, 0)

    def test_named_path_hash_omission_or_stale_tree_cannot_execute(self):
        adapter = self.adapter()
        subject = adapter.subject(self.context, self.store.head(), self.suite)
        for changed in (replace(subject, tree_oid="f" * 40), replace(subject, sources=subject.sources[1:]),
                        replace(subject, sources=(NamedSource(subject.sources[0].path, "f" * 64), *subject.sources[1:]))):
            with self.subTest(subject=changed), self.assertRaisesRegex(ExecutionError, "exact Git source"):
                self.execute(adapter, subject=changed)
        self.assertEqual(self.calls, 0)

    def test_cross_cohort_or_contract_context_is_rejected(self):
        adapter = self.adapter()
        for context in (replace(self.context, cohort_id="another"),
                        replace(self.context, execution_contract_sha256="b" * 64)):
            with self.assertRaisesRegex(ExecutionError, "context differs"):
                adapter.subject(context, self.store.head(), required_suite(context, self.cases))

    def test_request_bound_evaluator_provenance_and_purpose(self):
        adapter = self.adapter()
        subject = adapter.subject(self.context, self.store.head(), self.suite)
        for changed in (replace(subject, evaluator_sha256="b" * 64),
                        replace(subject, provenance_sha256="b" * 64), replace(subject, purpose="repeatability")):
            with self.assertRaisesRegex(ExecutionError, "frozen policy"):
                self.execute(adapter, subject=changed)
        self.assertEqual(self.calls, 0)

    def test_single_owner_and_configuration_rebinding_fail_closed(self):
        adapter = self.adapter()
        with self.assertRaisesRegex(ExecutionError, "already has an owner"):
            self.adapter()
        adapter.close()
        self.policy = replace(self.policy, seed=1)
        with self.assertRaisesRegex(ExecutionError, "artifact changed"):
            self.adapter()

    def test_intent_crash_is_unknown_and_never_automatically_reinvoked(self):
        def crash(point):
            if point == "after_intent":
                raise RuntimeError("intent crash")
        adapter = self.adapter(crash_hook=crash)
        with self.assertRaises(RuntimeError):
            self.execute(adapter)
        adapter.close()
        restored = self.adapter()
        with self.assertRaises(ExecutionUnknown):
            self.execute(restored)
        self.assertEqual(self.calls, 0)

    def test_evaluation_crash_is_unknown_and_never_automatically_reinvoked(self):
        def crash(point):
            if point == "after_evaluation":
                raise RuntimeError("result persistence crash")
        adapter = self.adapter(crash_hook=crash)
        with self.assertRaises(RuntimeError):
            self.execute(adapter)
        adapter.close()
        restored = self.adapter()
        with self.assertRaises(ExecutionUnknown):
            self.execute(restored)
        self.assertEqual(self.calls, 1)

    def test_persisted_receipt_recovers_publication_without_reevaluation(self):
        def crash(point):
            if point == "after_receipt":
                raise RuntimeError("receipt persisted")
        adapter = self.adapter(crash_hook=crash)
        with self.assertRaises(RuntimeError):
            self.execute(adapter)
        adapter.close()
        restored = self.adapter()
        publication = self.execute(restored)
        self.assertTrue(publication.replayed)
        self.assertEqual(self.calls, 1)
        self.assertEqual(len(self.mesh.commands), 1)

    def test_lost_mesh_ack_reuses_same_command_and_original_execution(self):
        adapter = self.adapter()
        self.mesh.fail_after_publish = True
        with self.assertRaises(OSError):
            self.execute(adapter)
        publication = self.execute(adapter)
        self.assertTrue(publication.replayed)
        self.assertEqual(self.calls, 1)
        self.assertEqual(self.mesh.calls, 2)
        self.assertEqual(len(self.mesh.commands), 1)

    def test_crash_after_mesh_publish_before_ref_persistence_is_exact_replay(self):
        def crash(point):
            if point == "after_publish":
                raise RuntimeError("reference persistence crash")
        adapter = self.adapter(crash_hook=crash)
        with self.assertRaises(RuntimeError):
            self.execute(adapter)
        adapter.close()
        restored = self.adapter()
        self.execute(restored)
        self.assertEqual(self.calls, 1)
        self.assertEqual(len(self.mesh.commands), 1)

    def test_failed_semantic_check_remains_false_in_release_evidence(self):
        def mutation(receipt):
            receipt["passed"], receipt["status"] = False, "failed"
            receipt["outcomes"][0].update(passed=False, status="wrong_answer", actual={"n": 9})
        self.mutation = mutation
        adapter = self.adapter(mode="physical")
        publication = self.execute(adapter)
        evidence = adapter.verified_execution(self.target(publication), self.suite)
        self.assertEqual(evidence.check_results, (("echo", "failed"), ("unicode", "passed")))
        self.assertEqual(publication.status, "failed")
        self.assertTrue((self.directory() / "receipt.json").exists())

    def test_infrastructure_observation_is_retained_but_cannot_authorize(self):
        def mutation(receipt):
            receipt["passed"], receipt["status"], receipt["exit_code"] = False, "sandbox_error", 125
            for outcome in receipt["outcomes"]:
                outcome.pop("actual")
                outcome.update(passed=False, status="sandbox_error")
        self.mutation = mutation
        adapter = self.adapter(mode="physical")
        publication = self.execute(adapter)
        self.assertTrue(all(status == "infrastructure_failure" for _, status in publication.check_results))
        with self.assertRaisesRegex(ExecutionError, "Infrastructure observations"):
            adapter.verified_execution(self.target(publication), self.suite)
        replay = self.execute(adapter)
        self.assertTrue(replay.replayed)
        self.assertEqual(self.calls, 1)

    def test_false_peer_pass_flags_are_recomputed_against_actual_json_types(self):
        def mutation(receipt):
            receipt["outcomes"][0]["actual"] = {"n": True}
        self.mutation = mutation
        adapter = self.adapter()
        with self.assertRaisesRegex(ExecutionError, "host oracle"):
            self.execute(adapter)
        self.assertTrue((self.directory() / "receipt.json").exists())
        with self.assertRaises(ExecutionError):
            self.execute(adapter)
        self.assertEqual(self.calls, 1)

    def test_reordered_original_outcomes_are_not_accepted(self):
        self.mutation = lambda receipt: receipt["outcomes"].reverse()
        adapter = self.adapter()
        with self.assertRaisesRegex(ExecutionError, "check fields|order or labels"):
            self.execute(adapter)

    def test_wrong_original_source_suite_image_or_limits_are_retained_and_rejected(self):
        changes = (("source_sha256", "b" * 64), ("suite_sha256", "b" * 64),
                   ("adapter_sha256", "b" * 64), ("image_id", "sha256:" + "b" * 64),
                   ("timeout_seconds", 1), ("case_timeout_seconds", 99))
        for index, (key, value) in enumerate(changes):
            with self.subTest(key=key):
                self.mutation = lambda receipt, key=key, value=value: receipt.update({key: value})
                adapter = self.adapter(path=self.root / f"bad-{index}")
                with self.assertRaises(ExecutionError):
                    self.execute(adapter)
                self.assertTrue((adapter.jobs / _sha(b"request") / "receipt.json").exists())

    def test_unclean_pass_or_missing_outcome_cannot_authorize(self):
        changes = (("cleanup_verified", False), ("exit_code", 1), ("timed_out", True),
                   ("output_truncated", True), ("input_delivery_failed", True), ("outcomes", []))
        for index, (key, value) in enumerate(changes):
            with self.subTest(key=key):
                self.mutation = lambda receipt, key=key, value=value: receipt.update({key: value})
                adapter = self.adapter(path=self.root / f"unclean-{index}")
                with self.assertRaises(ExecutionError):
                    self.execute(adapter)

    def test_tampered_retained_receipt_or_publication_does_not_validate(self):
        adapter = self.adapter(mode="physical")
        publication = self.execute(adapter)
        path = self.directory() / "receipt.json"
        value = json.loads(path.read_bytes())
        value["original_receipt"]["runtime_seconds"] = 999
        path.write_bytes(_json(value))
        with self.assertRaisesRegex(ExecutionError, "binding differs"):
            adapter.verified_execution(self.target(publication), self.suite)

    def test_missing_exact_mesh_payload_fails_verification(self):
        adapter = self.adapter(mode="physical")
        publication = self.execute(adapter)
        self.mesh.payloads.pop(publication.receipt_ref)
        with self.assertRaisesRegex(ExecutionError, "absent or altered"):
            adapter.verified_execution(self.target(publication), self.suite)

    def test_fabricated_ref_with_correct_payload_digest_is_not_local_proof(self):
        adapter = self.adapter(mode="physical")
        publication = self.execute(adapter)
        fabricated = replace(publication.receipt_ref, event_id="b" * 64)
        target = replace(self.target(publication), execution_receipt_refs=(fabricated,))
        with self.assertRaisesRegex(ExecutionError, "unique host-owned"):
            adapter.verified_execution(target, self.suite)

    def test_changed_release_tree_pathhash_evaluator_or_suite_is_rejected(self):
        adapter = self.adapter(mode="physical")
        publication = self.execute(adapter)
        target = self.target(publication)
        for changed in (replace(target, tree_oid="f" * 40), replace(target, evaluator_sha256="f" * 64),
                        replace(target, required_suite_sha256="f" * 64), replace(target, sources=target.sources[1:])):
            with self.subTest(target=changed), self.assertRaisesRegex(ExecutionError, "physically executed subject"):
                adapter.verified_execution(changed, self.suite)

    def test_foreign_root_and_symlink_outputs_are_not_overwritten(self):
        foreign = self.root / "foreign"
        foreign.mkdir()
        (foreign / "unrelated").write_text("preserve")
        with self.assertRaisesRegex(ExecutionError, "Foreign or partial"):
            self.adapter(path=foreign)
        self.assertEqual((foreign / "unrelated").read_text(), "preserve")
        linked = self.root / "linked"
        linked.symlink_to(foreign, target_is_directory=True)
        with self.assertRaisesRegex(ExecutionError, "canonical"):
            self.adapter(path=linked)

    def test_changed_actual_runtime_is_rejected_before_new_intent(self):
        adapter = self.adapter(mode="physical")
        subject = adapter.subject(self.context, self.store.head(), self.suite)
        with patch.object(adapter, "_runtime", return_value={"server": {"Version": "changed"}}), \
                self.assertRaisesRegex(ExecutionError, "runtime/environment changed"):
            adapter.execute("request", subject, self.suite, self.cases)
        self.assertFalse(self.directory().exists())
        self.assertEqual(self.calls, 0)

    def test_independent_purpose_cannot_rebind_existing_registration(self):
        adapter = self.adapter()
        self.execute(adapter)
        adapter.close()
        self.policy = replace(self.policy, purpose="independent_acceptance")
        with self.assertRaisesRegex(ExecutionError, "artifact changed"):
            self.adapter()

    def test_parent_directory_is_synced_before_evaluation_starts(self):
        from gossip_harness import peer_project_execution_v3 as module
        observed = []
        original_sync = module._sync_directory

        def sync(path):
            observed.append(path)
            original_sync(path)

        def executor(files, cases):
            self.assertIn(adapter.jobs, observed)
            self.assertIn(adapter.jobs / _sha(b"request"), observed)
            return self.receipt(files, cases)

        with patch.object(module, "_sync_directory", side_effect=sync):
            adapter = self.adapter()
            self.assertIn(adapter.root, observed)
            adapter.fixture_executor = executor
            self.execute(adapter)

    def test_valid_depth_64_input_and_expected_survive_retained_envelopes(self):
        nested = "leaf"
        for _ in range(64):
            nested = [nested]
        cases = [{"id": "deep", "input": nested, "expected": nested}]
        suite = required_suite(self.context, cases)
        adapter = self.adapter()
        publication = self.execute(adapter, cases=cases, suite=suite)
        self.assertEqual(publication.check_results, (("deep", "passed"),))
        replay = self.execute(adapter, cases=cases, suite=suite)
        self.assertTrue(replay.replayed)
        self.assertEqual(self.calls, 1)

    def test_valid_depth_64_wrong_answer_is_retained_as_failure(self):
        nested = "leaf"
        for _ in range(64):
            nested = [nested]

        def mutation(receipt):
            receipt["passed"], receipt["status"] = False, "failed"
            receipt["outcomes"][0].update(passed=False, status="wrong_answer", actual=nested)

        self.mutation = mutation
        adapter = self.adapter(mode="physical")
        publication = self.execute(adapter)
        evidence = adapter.verified_execution(self.target(publication), self.suite)
        self.assertEqual(evidence.check_results[0], ("echo", "failed"))
        self.assertTrue((self.directory() / "receipt.json").exists())

    def test_each_clean_per_case_execution_failure_remains_failed(self):
        adapter = self.adapter(mode="physical")
        for status in ("error", "timeout", "output_error", "output_limit", "invalid_output"):
            def mutation(receipt, status=status):
                receipt["passed"], receipt["status"] = False, "failed"
                receipt["outcomes"][0].pop("actual")
                receipt["outcomes"][0].update(passed=False, status=status)
            self.mutation = mutation
            publication = self.execute(adapter, request_id="failure-" + status)
            evidence = adapter.verified_execution(self.target(publication), self.suite)
            self.assertEqual(evidence.check_results[0], ("echo", "failed"))
        self.assertEqual(self.calls, 5)

    def test_changed_commit_with_identical_bytes_is_not_same_request(self):
        adapter = self.adapter()
        original = self.execute(adapter)
        commit = self.store._git("commit-tree", original.subject.tree_oid, "-p", original.subject.commit_oid,
                                 "-m", "A distinct immutable history point")
        self.assertNotEqual(commit, original.subject.commit_oid)
        subject = adapter.subject(self.context, commit, self.suite)
        self.assertEqual(subject.sources, original.subject.sources)
        with self.assertRaisesRegex(ExecutionError, "replay changed"):
            self.execute(adapter, subject=subject)
        self.assertEqual(self.calls, 1)

    def test_closed_adapter_refuses_publication_and_subject_changes(self):
        adapter = self.adapter()
        adapter.close()
        with self.assertRaisesRegex(ExecutionError, "closed"):
            adapter.subject(self.context, self.store.head(), self.suite)


if __name__ == "__main__":
    unittest.main()
