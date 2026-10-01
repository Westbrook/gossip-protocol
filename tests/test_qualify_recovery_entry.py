"""Composition reuses exact recovery observations without reexecuting them."""
from contextlib import ExitStack
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
from typing import Any
import unittest
from unittest.mock import patch

from devtools import qualify_recovery_entry as composition


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


class RecoveryCompositionTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.original = composition.guarded.original
        self.signature: dict[str, Any] = dict(
            spec_version=self.original.SPEC_VERSION,
            image_id=self.original.DEFAULT_IMAGE,
            source_commit=self.original.SOURCE_COMMIT,
            fixture_sha256="a" * 64,
            injected_sha256="b" * 64,
            checks_sha256=self.original._hash(self.original.CHECKS),
            contract_sha256="c" * 64,
            implementation_sha256=sha256(Path(self.original.__file__)),
            seeds=[0, 1, 2], fault_profiles=list(self.original.FAULT_PROFILES),
            attempts=2, replay_concurrency=2,
        )
        self.sources = composition.guarded.execution_sources()
        cases = []
        self.events: list[dict[str, Any]] = []
        for transport in ("bus", "gossip"):
            for fault in self.signature["fault_profiles"]:
                for seed in self.signature["seeds"]:
                    case = f"{transport}-{fault}-{seed}"
                    cases.append(dict(case=case, transport=transport, fault=fault, seed=seed,
                        status="accepted", project_accepted=True, task_status="complete",
                        release_head="d" * 40, exact_tested_sha="d" * 40))
                    completion = dict(spec_version=self.original.SPEC_VERSION, target="all",
                        tests_run=self.original.EXPECTED_TEST_COUNTS["all"], failures=0,
                        errors=0, successful=True)
                    self.events.append(dict(kind="validation", case=case, stage="final-repair",
                        passed=True, target="all", receipt=dict(schema_version=1, status="passed",
                        container_name="gossip-check-" + case,
                        image_id=self.signature["image_id"],
                        command=["python", "-I", "/checks/run_checks.py", "all"],
                        checks_sha256=self.signature["checks_sha256"], exit_code=0,
                        timeout_seconds=30, timed_out=False, cleanup_verified=True,
                        output=json.dumps(completion), output_truncated=False)))
        self.report: dict[str, Any] = dict(schema_version=1, experiment="integration-recovery-study",
            status="accepted", mode="rehearsal", signature=self.signature, cases=cases,
            repair=dict(status="accepted", task_status="complete", release_head="e" * 40,
                        exact_tested_sha="e" * 40), unexecuted=[])
        self.archive = self.root / "original"
        self.archive.mkdir()
        self.proof = self.archive / "results.json"
        self.manifest = self.archive / "manifest.json"
        self.trace = self.archive / "trace.jsonl"
        self.write(self.proof, self.report)
        self.write(self.manifest, dict(experiment=self.report["experiment"],
            signature=self.signature, mode="rehearsal", status="finished"))
        self.write_trace()
        self.baseline = self.root / "execution-baseline.json"
        self.write(self.baseline, dict(sources=self.sources))
        self.regression = self.root / "regression.json"
        self.regression_data: dict[str, Any] = dict(returncode=0, static_passed=True, unchanged=True, tests=20,
            source_sha256={name: sha256(composition.ROOT / name) for name in (
                "gossip_harness/recovery_experiment_v2.py", "tests/test_recovery_experiment_v2.py")})
        self.write(self.regression, self.regression_data)
        self.output = self.root / "composed"

    @staticmethod
    def write(path, document):
        path.write_text(json.dumps(document, indent=2) + "\n")

    def write_trace(self):
        self.trace.write_text("".join(json.dumps(event) + "\n" for event in self.events))

    def qualify(self, *, probes=None, **overrides):
        arguments = dict(original_proof=self.proof, trusted_proof_sha256=sha256(self.proof),
            execution_baseline=self.baseline, trusted_baseline_sha256=sha256(self.baseline),
            regression_receipt=self.regression, trusted_regression_sha256=sha256(self.regression),
            output=self.output)
        arguments.update(overrides)
        with ExitStack() as stack:
            stack.enter_context(patch.object(composition, "current_signature", return_value=deepcopy(self.signature)))
            stack.enter_context(patch.object(composition.guarded, "execution_sources", return_value=dict(self.sources)))
            probe = stack.enter_context(patch.object(composition, "physical_guard_probes",
                side_effect=probes, return_value=dict(passed=True, count=3, physical=True)))
            original_run = stack.enter_context(patch.object(self.original, "run_recovery_experiment",
                side_effect=AssertionError("Composition must not rerun the original study")))
            try:
                result = composition.qualify(**arguments)
            finally:
                original_run.assert_not_called()
            self.assertEqual(probe.call_count, 1)
            return result

    def test_complete_roster_is_reused_byte_for_byte_without_candidate_execution(self):
        original_bytes = self.proof.read_bytes()
        self.assertTrue(self.original._valid_rehearsal(self.report, self.signature))
        result = self.qualify()
        self.assertTrue(result["passed"])
        self.assertEqual(result["qualification_mode"], "composed-entry-qualification")
        self.assertEqual(result["original_executions"], "reused")
        self.assertEqual((self.output / "results.json").read_bytes(), original_bytes)
        self.assertEqual(self.proof.read_bytes(), original_bytes)
        self.assertEqual(result["result_sha256"], sha256(self.proof))
        self.assertEqual(result["counts"]["reused_original_cases"], 24)
        for field in ("new_candidate_executions", "new_container_executions", "provider_requests"):
            self.assertEqual(result["counts"][field], 0)
        self.assertEqual(result["counts"]["physical_guard_probes"], 3)
        for key, path in (("original_proof", self.proof), ("original_manifest", self.manifest),
                          ("original_trace", self.trace), ("execution_baseline", self.baseline),
                          ("regression_receipt", self.regression)):
            self.assertEqual(result["references"][key], dict(path=str(path), sha256=sha256(path)))
        self.assertTrue(result["guard_probes"]["physical"])

    def test_independently_pinned_input_digests_are_required(self):
        for name in ("trusted_proof_sha256", "trusted_baseline_sha256", "trusted_regression_sha256"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                self.qualify(**{name: "0" * 64})
            self.assertFalse(self.output.exists())

    def test_execution_source_drift_rejects_before_output(self):
        self.write(self.baseline, dict(sources={key: "0" * 64 for key in self.sources}))
        with self.assertRaises(ValueError):
            self.qualify()
        self.assertFalse(self.output.exists())

    def test_incomplete_roster_and_original_acceptance_fail_closed(self):
        for mutation in (lambda report: report["cases"].pop(),
                         lambda report: report["cases"].append(deepcopy(report["cases"][0])),
                         lambda report: report["repair"].update(exact_tested_sha="f" * 40)):
            report = deepcopy(self.report)
            mutation(report)
            self.write(self.proof, report)
            with self.subTest(report=report["repair"]), self.assertRaises(ValueError):
                self.qualify()
            self.assertFalse(self.output.exists())
        for parameters in (dict(seeds=[True]), dict(seeds=[0, 0]), dict(fault_profiles=["unknown"]),
                           dict(attempts=True), dict(image_id="mutable:latest")):
            signature = dict(self.signature, **parameters)
            with self.subTest(parameters=parameters), patch.object(self.original, "fixture_files") as fixture:
                with self.assertRaises(ValueError):
                    composition.current_signature(signature)
                fixture.assert_not_called()

    def test_manifest_signature_must_match_original_results(self):
        document = json.loads(self.manifest.read_text())
        document["signature"]["replay_concurrency"] = 4
        self.write(self.manifest, document)
        with self.assertRaises(ValueError):
            self.qualify()
        self.assertFalse(self.output.exists())

    def test_final_repair_receipts_require_actual_complete_docker_validation(self):
        saved = deepcopy(self.events)
        for mutation in (dict(test_only_host_validator=True), dict(image_id="sha256:" + "0" * 64),
                         dict(cleanup_verified=False), dict(output="forged completion"),
                         dict(checks_sha256="0" * 64)):
            self.events = deepcopy(saved)
            self.events[0]["receipt"].update(mutation)
            self.write_trace()
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                self.qualify()
            self.assertFalse(self.output.exists())
        for events in (saved[:-1], saved + [saved[0]]):
            self.events = events
            self.write_trace()
            with self.assertRaises(ValueError):
                self.qualify()
            self.assertFalse(self.output.exists())

    def test_focused_receipt_requires_success_and_current_guard_sources(self):
        for mutation in (dict(returncode=1), dict(static_passed=False), dict(unchanged=False),
                         dict(source_sha256={name: "0" * 64 for name in self.regression_data["source_sha256"]})):
            document = deepcopy(self.regression_data)
            document.update(mutation)
            self.write(self.regression, document)
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                self.qualify()
            self.assertFalse(self.output.exists())

    def test_mutation_of_original_reference_during_probe_is_not_certified(self):
        def mutate_reference(_output):
            self.proof.write_bytes(self.proof.read_bytes() + b" ")
            return dict(passed=True, count=3, physical=True)
        with self.assertRaises(ValueError):
            self.qualify(probes=mutate_reference)
        if (self.output / "entry-guard.json").exists():
            receipt = json.loads((self.output / "entry-guard.json").read_text())
            self.assertIsNot(receipt.get("passed"), True)
