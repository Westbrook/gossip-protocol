"""Offline scheduling/provenance tests; candidate text is never imported here."""

from dataclasses import replace
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from devtools.validate_batch import load_manifest, main
from devtools.validation_session import (
    BLACKBOX_PROTOCOL, ResourceBudget, SESSION_PROTOCOL, ValidationJob,
    ValidationSession, _ADAPTER_DIGEST, digest, support_digests,
)


IMAGE = "sha256:" + "a" * 64
FILES = {"solution.py": "This is intentionally not importable Python."}
CASES = [{"id": "a", "input": 1, "expected": 2}]


def job(name="visible", **options):
    return ValidationJob(name, dict(FILES), list(CASES), deterministic=True, **options)


class FakeFactory:
    def __init__(self, *, fail_first=False, behavioral_failure=False,
                 preflight=True, entered=None, release=None):
        self.lock = threading.Lock()
        self.calls = []
        self.preflights = 0
        self.active = 0
        self.peak = 0
        self.fail_first = fail_first
        self.behavioral_failure = behavioral_failure
        self.ready = preflight
        self.entered, self.release = entered, release

    def __call__(self, image, *, timeout_seconds, case_timeout_seconds):
        owner = self

        class Validator:
            last_receipt = {}

            def preflight(self):
                owner.preflights += 1
                return owner.ready, "fake readiness"

            def evaluate(self, files, cases):
                with owner.lock:
                    owner.calls.append(self)
                    number = len(owner.calls)
                    owner.active += 1
                    owner.peak = max(owner.peak, owner.active)
                try:
                    if owner.entered:
                        owner.entered.set()
                        if not owner.release.wait(timeout=5):
                            raise RuntimeError("Test release deadline expired")
                    else:
                        # Force the two-worker test to expose a real overlap.
                        time.sleep(0.003)
                    outcomes = []
                    for index, case in enumerate(cases):
                        passed = not owner.behavioral_failure
                        outcomes.append(dict(index=index, passed=passed,
                            status="passed" if passed else "wrong_answer",
                            actual=case["expected"] if passed else "deliberately-wrong",
                            **{label: case[label] for label in ("id", "requirement") if label in case}))
                    self.last_receipt = dict(schema_version=1, protocol=BLACKBOX_PROTOCOL,
                        passed=not owner.behavioral_failure,
                        status="failed" if owner.behavioral_failure else "passed",
                        image_id=image, adapter_sha256=_ADAPTER_DIGEST,
                        source_sha256=digest(files), suite_sha256=digest(cases),
                        timeout_seconds=timeout_seconds, case_timeout_seconds=case_timeout_seconds,
                        case_count=len(cases), exit_code=0, timed_out=False,
                        output_truncated=False, input_delivery_failed=False,
                        cleanup_verified=True, outcomes=outcomes)
                    if owner.fail_first and number == 1:
                        self.last_receipt["status"] = "sandbox_error"
                    return self.last_receipt
                finally:
                    with owner.lock:
                        owner.active -= 1

        return Validator()


class ValidationSessionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.counter = 0

    def session(self, factory=None, **options):
        self.counter += 1
        return ValidationSession(self.root / str(self.counter), image=IMAGE,
            validator_factory=factory or FakeFactory(), runtime_identity=lambda: "fake-daemon-v1",
            **options)

    def test_identical_visible_jobs_singleflight_and_reuse_with_one_preflight(self):
        factory = FakeFactory()
        session = self.session(factory)
        first = session.run([job("first"), job("second"), job("third")])
        self.assertEqual(first["counts"]["logical_jobs"], 3)
        self.assertEqual(first["counts"]["physical_executions"], 1)
        self.assertEqual(first["counts"]["reused_singleflight"], 2)
        second = session.run([job("fourth")])
        self.assertEqual(second["counts"]["physical_executions"], 1)
        self.assertEqual(second["counts"]["reused_in_session"], 1)
        self.assertEqual(factory.preflights, 1)
        self.assertEqual(len(factory.calls), 1)
        ids = {result["execution_id"] for result in second["results"]}
        self.assertEqual(len(ids), 1)
        for result in second["results"]:
            retained = json.loads((Path(result["artifact_path"]) / "result.json").read_text())
            self.assertEqual(retained, result)
            self.assertNotIn("completed", result)
        # Caller mutation cannot alter the session's immutable reusable proof.
        first["results"][0]["receipt"]["outcomes"][0]["actual"] = "corruption"
        last = session.run([job("fifth")])["results"][-1]
        self.assertEqual(last["receipt"]["outcomes"][0]["actual"], 2)

    def test_final_repeatability_and_undeclared_determinism_always_execute(self):
        factory = FakeFactory()
        session = self.session(factory)
        jobs = [job("visible")]
        for purpose in ("final", "repeatability"):
            jobs += [job(purpose + str(index), purpose=purpose, reason="Independent observation")
                     for index in range(2)]
        jobs += [replace(job("nondeterministic" + str(index)), deterministic=False)
                 for index in range(2)]
        result = session.run(jobs)
        self.assertEqual(result["counts"]["physical_executions"], 7)
        self.assertEqual(len({id(validator) for validator in factory.calls}), 7)
        self.assertTrue(all(not result["reuse"] for result in result["results"]))

    def test_resource_partition_and_parallel_merge_keep_stable_order(self):
        jobs = [replace(job(str(index)), seed=index) for index in range(5)]
        rows = []
        for workers in (1, 2):
            factory = FakeFactory()
            result = self.session(factory, budget=ResourceBudget(workers=workers)).run(jobs)
            self.assertEqual(factory.peak, workers)
            self.assertEqual([row["name"] for row in result["results"]], [str(i) for i in range(5)])
            rows.append([(row["name"], row["status"], row["receipt"]["source_sha256"],
                          row["receipt"]["suite_sha256"]) for row in result["results"]])
        self.assertEqual(rows[0], rows[1])
        self.assertEqual(ResourceBudget(workers=4, cpus=4, memory_mib=1024,
                                        outer_parallelism=2).capacity(), 2)
        with self.assertRaises(ValueError):
            ResourceBudget(outer_parallelism=3).capacity()

    def test_failed_infrastructure_is_retained_but_not_reused(self):
        factory = FakeFactory(fail_first=True)
        session = self.session(factory)
        result = session.run([job("first"), job("second")])
        self.assertEqual(result["counts"]["physical_executions"], 2)
        self.assertEqual(result["counts"]["infrastructure_failed"], 1)
        self.assertEqual(result["counts"]["passed"], 1)
        self.assertEqual(result["results"][0]["receipt"]["status"], "sandbox_error")
        self.assertEqual(factory.preflights, 2)
        self.assertFalse(result["passed"])
        self.assertIsNone(result["results"][1]["reuse"])

    def test_deterministic_behavioral_failure_is_eligible_evidence(self):
        factory = FakeFactory(behavioral_failure=True)
        result = self.session(factory).run([job("first"), job("second")])
        self.assertEqual(result["counts"]["physical_executions"], 1)
        self.assertEqual(result["counts"]["failed"], 2)
        self.assertFalse(result["passed"])

    def test_persisted_proof_validates_exact_contract_before_reuse(self):
        cache = self.root / "cache"
        first = self.session(cache=cache).run([job()])
        factory = FakeFactory()
        second = self.session(factory, cache=cache).run([job()])
        self.assertEqual(len(factory.calls), 0)
        self.assertEqual(second["counts"]["reused_persisted"], 1)
        self.assertEqual(first["results"][0]["execution_id"], second["results"][0]["execution_id"])
        self.assertEqual(factory.preflights, 1)

    def test_every_key_component_rejects_an_old_proof_even_if_misfiled(self):
        cache = self.root / "cache"
        session = self.session(cache=cache)
        result = session.run([job()])["results"][0]
        original = result["binding"]
        proof = (cache / (digest(original) + ".json")).read_text()
        # Covers source, ordered suite, checker/support, adapter, image, resource,
        # environment, runtime, seed, protocol, purpose and determinism fields.
        for key in original:
            with self.subTest(component=key):
                changed = dict(original, **{key: {"changed": original[key]}})
                (cache / (digest(changed) + ".json")).write_text(proof)
                value, kind, rejected = session._read_proof(job().snapshot(), changed, session.output)
                self.assertIsNone(value)
                self.assertIsNone(kind)
                self.assertTrue(rejected)

    def test_source_suite_order_and_observation_contract_changes_execute(self):
        session = self.session()
        base = replace(job("base"), cases=CASES + [{"input": 3, "expected": 4}])
        variants = [base, replace(base, name="source", files={"solution.py": "different text"}),
                    replace(base, name="order", cases=list(reversed(base.cases))),
                    replace(base, name="seed", seed=1),
                    replace(base, name="protocol", protocol="new-protocol")]
        result = session.run(variants)
        self.assertEqual(result["counts"]["physical_executions"], 5)
        self.assertEqual(len({row["key_sha256"] for row in result["results"]}), 5)

    def test_corrupt_receipt_is_preserved_rejected_and_physically_rechecked(self):
        for mutation in ("digest", "actual", "cleanup", "provenance"):
            with self.subTest(mutation=mutation):
                cache = self.root / ("cache-" + mutation)
                self.session(cache=cache).run([job()])
                path = next(cache.glob("*.json"))
                proof = json.loads(path.read_text())
                if mutation == "digest":
                    proof["receipt_sha256"] = "bad"
                elif mutation == "actual":
                    proof["receipt"]["outcomes"][0]["actual"] = 99
                    proof["receipt_sha256"] = digest(proof["receipt"])
                elif mutation == "cleanup":
                    proof["receipt"]["cleanup_verified"] = False
                    proof["receipt_sha256"] = digest(proof["receipt"])
                else:
                    del proof["origin_session_id"]
                path.write_text(json.dumps(proof))
                factory = FakeFactory()
                result = self.session(factory, cache=cache).run([job()])
                self.assertEqual(result["counts"]["cache_rejections"], 1)
                self.assertEqual(result["counts"]["physical_executions"], 1)
                self.assertTrue(result["passed"])
                self.assertTrue((Path(result["results"][0]["artifact_path"]) / "rejected-cache.txt").is_file())

    def test_cancel_drains_started_job_and_retains_every_queued_observation(self):
        entered, release = threading.Event(), threading.Event()
        factory = FakeFactory(entered=entered, release=release)
        session = self.session(factory, budget=ResourceBudget(workers=1))
        holder = []
        runner = threading.Thread(target=lambda: holder.append(session.run([
            replace(job(str(index)), seed=index) for index in range(4)])))
        runner.start()
        self.assertTrue(entered.wait(timeout=5))
        session.cancel()
        release.set()
        runner.join(timeout=5)
        self.assertFalse(runner.is_alive())
        self.assertEqual(factory.active, 0)
        self.assertEqual(holder[0]["counts"]["physical_executions"], 1)
        self.assertEqual(holder[0]["counts"]["logical_jobs"], 4)
        self.assertEqual(holder[0]["counts"]["cancelled"], 3)

    def test_unsafe_inputs_and_support_changes_stop_before_preflight(self):
        for invalid in (replace(job(), files={"../solution.py": "bad"}),
                        replace(job(), purpose="final"),
                        replace(job(), cases=[dict(CASES[0], unknown=True)])):
            factory = FakeFactory()
            session = self.session(factory)
            with self.assertRaises(ValueError):
                session.run([invalid])
            self.assertEqual(factory.preflights, 0)
            self.assertEqual(factory.calls, [])
        factory = FakeFactory()
        session = self.session(factory)
        with patch("devtools.validation_session.support_digests", return_value={}):
            with self.assertRaises(RuntimeError):
                session.run([job()])
        self.assertEqual(factory.preflights, 0)

    def test_unavailable_preflight_retains_all_inputs_without_physical_execution(self):
        factory = FakeFactory(preflight=False)
        result = self.session(factory).run([job("first"), job("second")])
        self.assertEqual(result["counts"]["infrastructure_failed"], 2)
        self.assertEqual(result["counts"]["physical_executions"], 0)
        self.assertEqual(factory.preflights, 1)
        self.assertTrue(all((Path(row["artifact_path"]) / "input.json").is_file()
                            for row in result["results"]))

    def test_mid_batch_support_change_invalidates_results_before_publishing_proof(self):
        cache = self.root / "cache"
        session = self.session(cache=cache)
        original = session.base_binding["support_sha256"]
        with patch("devtools.validation_session.support_digests", side_effect=[original, {}]):
            result = session.run([job("first"), job("second")])
        self.assertFalse(result["passed"])
        self.assertEqual(result["counts"]["physical_executions"], 1)
        self.assertEqual(result["counts"]["infrastructure_failed"], 2)
        self.assertEqual(list(cache.glob("*.json")), [])
        self.assertEqual(session._proofs, {})
        for row in result["results"]:
            self.assertEqual(row["error"], "session_inputs_changed")
            self.assertEqual(row["observed_status"], "passed")

    def test_support_closure_tracks_helpers_but_ignores_reverse_dependencies(self):
        root = self.root / "source"
        for relative, source in {
                "devtools/__init__.py": "",
                "devtools/validation_session.py": "from gossip_harness.blackbox_validator import Validator\n",
                "devtools/validate_batch.py": "from .validation_session import Session\n",
                "gossip_harness/__init__.py": "",
                "gossip_harness/blackbox_validator.py": "from . import helper\n",
                "gossip_harness/sandbox.py": "",
                "gossip_harness/sustained_experiment.py": "",
                "gossip_harness/helper.py": "VALUE = 1\n",
                "gossip_harness/verification_unrelated.py": "from .blackbox_validator import Validator\n",
        }.items():
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(source)
        original = support_digests(root)
        self.assertIn("gossip_harness/helper.py", original)
        self.assertNotIn("gossip_harness/verification_unrelated.py", original)
        (root / "gossip_harness/verification_unrelated.py").write_text("changed unrelated source\n")
        (root / "gossip_harness/new_unrelated.py").write_text("more unrelated source\n")
        self.assertEqual(support_digests(root), original)
        (root / "gossip_harness/helper.py").write_text("VALUE = 2\n")
        self.assertNotEqual(support_digests(root), original)
        (root / "gossip_harness/helper.py").write_text("from .new_helper import VALUE\n")
        (root / "gossip_harness/new_helper.py").write_text("VALUE = 3\n")
        self.assertIn("gossip_harness/new_helper.py", support_digests(root))
        (root / "gossip_harness/helper.py").write_text("__import__('gossip_harness.new_helper')\n")
        with self.assertRaisesRegex(ValueError, "Dynamic imports"):
            support_digests(root)

    def test_new_daemon_identity_between_batches_rechecks_health_and_rejects_old_proof(self):
        factory = FakeFactory()
        session = self.session(factory)
        identity = ["daemon-one"]
        session.runtime_identity = lambda: identity[0]
        first = session.run([job("first")])
        second = session.run([job("same-daemon")])
        self.assertEqual(factory.preflights, 1)
        self.assertEqual(second["counts"]["physical_executions"], 1)
        identity[0] = "daemon-two"
        third = session.run([job("new-daemon")])
        self.assertEqual(factory.preflights, 2)
        self.assertEqual(third["counts"]["physical_executions"], 2)
        self.assertNotEqual(first["results"][0]["key_sha256"], third["results"][-1]["key_sha256"])
        self.assertIsNone(third["results"][-1]["reuse"])

    def test_mid_batch_daemon_change_prevents_proof_publication(self):
        cache = self.root / "cache"
        session = self.session(cache=cache)
        identities = iter(["daemon-one", "daemon-two"])
        session.runtime_identity = lambda: next(identities)
        result = session.run([job()])
        self.assertFalse(result["passed"])
        self.assertEqual(result["results"][0]["error"], "session_inputs_changed")
        self.assertEqual(list(cache.glob("*.json")), [])

    def test_external_contract_is_bound_and_midrun_guard_blocks_publication(self):
        cache = self.root / "guard-cache"
        checks = iter([True, False])
        session = self.session(cache=cache, external_contract={"study_source": "a" * 64},
                               finalization_guard=lambda: next(checks))
        summary = session.run([job()])
        self.assertEqual(summary["results"][0]["binding"]["external_contract"],
                         {"study_source": "a" * 64})
        self.assertEqual(summary["results"][0]["status"], "infrastructure_failed")
        self.assertEqual(summary["results"][0]["error"], "session_inputs_changed")
        self.assertEqual(list(cache.glob("*.json")), [])
        retained = json.loads((Path(summary["results"][0]["artifact_path"]) / "result.json").read_text())
        self.assertEqual(retained, summary["results"][0])
        self.assertEqual(session._proofs, {})

    def test_changed_external_contract_cannot_reuse_same_protocol_proof(self):
        cache = self.root / "external-cache"
        first = self.session(cache=cache, external_contract={"study_source": "first"},
                             finalization_guard=lambda: True).run([job()])
        factory = FakeFactory()
        second = self.session(factory, cache=cache, external_contract={"study_source": "second"},
                              finalization_guard=lambda: True).run([job()])
        self.assertEqual(len(factory.calls), 1)
        self.assertNotEqual(first["results"][0]["key_sha256"], second["results"][0]["key_sha256"])
        self.assertIsNone(second["results"][0]["reuse"])

    def test_external_guard_rejects_before_preflight_and_requires_bound_identity(self):
        factory = FakeFactory()
        session = self.session(factory, external_contract={"study": "identity"},
                               finalization_guard=lambda: False)
        with self.assertRaisesRegex(RuntimeError, "External validation contract changed"):
            session.run([job()])
        self.assertEqual(factory.preflights, 0)
        self.assertEqual(factory.calls, [])
        for options in (dict(external_contract={"study": "identity"}),
                        dict(finalization_guard=lambda: True)):
            with self.assertRaisesRegex(ValueError, "required together"):
                self.session(**options)

    def test_cli_shape_validation_and_manifest_example(self):
        example = Path(__file__).resolve().parents[1] / "devtools/examples/validation-batch.json"
        options, jobs = load_manifest(example)
        self.assertEqual(len(jobs), 3)
        self.assertEqual(options["budget"].capacity(), 2)
        invalid = self.root / "invalid.json"
        invalid.write_text('{"schema":"validation-session-v1","schema":"duplicate"}')
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            load_manifest(invalid)
        with patch("devtools.validate_batch.ValidationSession") as constructor:
            with self.assertRaises(SystemExit):
                main([str(invalid), "--output", str(self.root / "not-created")])
            constructor.assert_not_called()


if __name__ == "__main__":
    unittest.main()
