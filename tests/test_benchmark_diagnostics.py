"""Synthetic, non-executing checks for benchmark diagnostic trust boundaries."""
from copy import deepcopy
import hashlib
import json
import math
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch

from gossip_harness import benchmark_diagnostics as diagnostics


MISSING = object()


def case(case_id="public-1", *, expected=MISSING, input_value=MISSING):
    return {
        "id": case_id,
        "input": {"command": "read"} if input_value is MISSING else input_value,
        "expected": {"version": 1} if expected is MISSING else expected,
        "requirement": "R1",
    }


def receipt(files, cases, contract):
    return {
        "protocol": "gossip-blackbox-v1",
        "schema_version": 1,
        "source_sha256": diagnostics.digest(files),
        "suite_sha256": diagnostics.digest(diagnostics.execution_cases(cases)),
        "image_id": contract["image"],
        "adapter_sha256": diagnostics.ADAPTER_SHA256,
        "case_timeout_seconds": contract["case_timeout_seconds"],
        "timeout_seconds": contract["suite_timeout_seconds"],
        "status": "passed",
        "passed": True,
        "cleanup_verified": True,
        "exit_code": 0,
        "timed_out": False,
        "output_truncated": False,
        "input_delivery_failed": False,
        "case_count": len(cases),
        "outcomes": [
            {
                "index": index,
                "id": item["id"],
                "requirement": item["requirement"],
                "status": "passed",
                "passed": True,
                "actual": deepcopy(item["expected"]),
            }
            for index, item in enumerate(cases)
        ],
    }


def pool_fixture():
    pool = {
        "candidate_order": ["candidate-C", "candidate-A", "candidate-B"],
        "candidates": {
            alias: {"source_id": source, "source_valid": True}
            for alias, source in (
                ("candidate-A", "source-a"),
                ("candidate-B", "source-b"),
                ("candidate-C", "source-c"),
            )
        },
    }
    matrix = {
        source: {
            case_id: {"passed": True, "status": "passed"}
            for case_id in ("public-1", "public-2", "extra-1", "extra-2")
        }
        for source in ("source-a", "source-b", "source-c")
    }
    return pool, matrix


def cohort_fixture():
    roster = [
        [project, policy, repetition]
        for project in ("graph-patch", "calendar-exchange")
        for policy, count in (("sequential-four", 2), ("independent-four", 2), ("strong-anchor", 1))
        for repetition in range(count)
    ]
    contract = {"protocol": diagnostics.STUDY_PROTOCOL, "milestones": 2, "roster": roster}
    cases = [{"project_id": project, "policy": policy, "repetition": repetition} for project, policy, repetition in roster]
    report = {
        "experiment": diagnostics.STUDY_PROTOCOL, "status": "finished", "phase": "finished",
        "mode": "rehearsal", "active_case": None, "unexecuted": [], "censored": [],
        "contract": contract, "contract_sha": diagnostics.digest(contract), "cases": cases,
    }
    prereg = {"contract": deepcopy(contract), "roster": deepcopy(roster)}
    frozen = {
        "protocol": diagnostics.STUDY_PROTOCOL, "contract_sha": diagnostics.digest(contract),
        "private_evaluation_started": False, "trajectories": deepcopy(cases),
    }
    return report, prereg, frozen


def execution_plan(directory, source_count=1):
    contract = {"image": "sha256:" + "a" * 64, "case_timeout_seconds": 12, "suite_timeout_seconds": 300}
    cases = [case("public:one"), case("private:one")]
    sources = {}
    for index in range(source_count):
        files = {"solution.py": f"raise AssertionError('Never execute synthetic candidate {index}')"}
        source_id = diagnostics.digest(files)
        sources[source_id] = {"files": files, "source_sha256": source_id, "occurrences": [{"kind": "candidate", "label": str(index)}]}
    group = {
        "project_id": "graph-patch", "stage_index": 0, "sources": sources,
        "cases": cases, "suite_sha256": diagnostics.digest(diagnostics.execution_cases(cases)),
        "public_ids": ["public:one"], "private_ids": ["private:one"], "probes": [],
        "pools": [], "faults": [], "controls": [{"id": "trusted-golden", "source_id": next(iter(sources))}],
    }
    return {
        "protocol": diagnostics.PROTOCOL, "purpose": diagnostics.PURPOSE, "mode": "rehearsal",
        "primary_run": str(Path(directory) / "primary"), "contract": contract, "input_hashes": {},
        "freeze_monotonic_ns": 0, "frozen_utc": "1970-01-01T00:00:00+00:00",
        "global_freeze_sha256": "0" * 64, "runtime": {"fixture": "synthetic"}, "groups": [group],
        "adapter_sha256": diagnostics.ADAPTER_SHA256,
        "frozen_diagnostic_source_sha256": hashlib.sha256(Path(diagnostics.__file__).read_bytes()).hexdigest(),
        "invocation_count": 0, "input_primary_results_sha256": "1" * 64,
        "primary_contract_sha256": "2" * 64, "input_timings_sha256": "3" * 64,
    }


class BenchmarkDiagnosticHelperTests(unittest.TestCase):
    def test_digest_is_canonical_but_preserves_json_value_types_and_order(self):
        self.assertEqual(
            diagnostics.digest({"a": [1, False], "b": "é"}),
            diagnostics.digest({"b": "é", "a": [1, False]}),
        )
        for left, right in ((1, True), (1, 1.0), ([1, 2], [2, 1]), (None, "null")):
            with self.subTest(left=left, right=right):
                self.assertNotEqual(diagnostics.digest(left), diagnostics.digest(right))
        for value in (math.nan, math.inf, -math.inf):
            with self.subTest(value=value), self.assertRaises(ValueError):
                diagnostics.digest({"value": value})

    def test_execution_projection_drops_provenance_and_preserves_suite_order(self):
        cases = [case("second"), case("first", expected=[True])]
        expected = deepcopy(cases)
        cases[0].update(origin={"agent": "scout"}, origins=[{"run": "r"}], rationale="ignored")
        self.assertEqual(diagnostics.execution_cases(cases), expected)
        self.assertIn("origin", cases[0])

    def test_probe_dedup_joins_three_way_overlap_without_losing_origins(self):
        origins = [{"run": "one", "role": "scout"}, {"role": "scout", "run": "two"}, {"run": "three", "role": "reviewer"}]
        base = case("local-id", input_value={"b": 2, "a": 1})
        probes = [dict(base, id=f"local-{index}", origin=origin) for index, origin in enumerate(origins)]
        probes.append(dict(base, origins=[origins[0], origins[2]]))
        before = deepcopy(probes)
        merged = diagnostics.deduplicate_probes(probes)
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["id"], "probe-" + diagnostics.digest(base["input"]))
        self.assertEqual(merged[0]["input"], base["input"])
        self.assertEqual(merged[0]["expected"], base["expected"])
        self.assertEqual({diagnostics.digest(value) for value in merged[0]["origins"]}, {diagnostics.digest(value) for value in origins})
        self.assertEqual(len(merged[0]["origins"]), 3)
        self.assertEqual(probes, before)

    def test_probe_dedup_retains_first_requirement_and_distinguishes_typed_inputs(self):
        first = case("one", expected=True, input_value={"value": 1})
        second = dict(first, id="two", requirement="alternative-label", origin={"run": "two"})
        third = case("three", expected=True, input_value={"value": True})
        merged = diagnostics.deduplicate_probes([first, second, third])
        self.assertEqual(len(merged), 2)
        by_id = {item["id"]: item for item in merged}
        self.assertEqual(by_id["probe-" + diagnostics.digest(first["input"])]["requirement"], "R1")

    def test_probe_dedup_rejects_conflicting_exact_typed_labels(self):
        for expected in (1, 1.0, False, {"nested": 1}):
            first = case(expected={"nested": True} if isinstance(expected, dict) else True)
            second = dict(first, expected=expected)
            with self.subTest(expected=expected), self.assertRaises(ValueError):
                diagnostics.deduplicate_probes([first, second])

    def test_probe_dedup_rejects_missing_expected_labels(self):
        missing = case()
        del missing["expected"]
        with self.assertRaises((ValueError, KeyError)):
            diagnostics.deduplicate_probes([missing])

    def test_selector_uses_frozen_alias_order_for_public_only_ties(self):
        pool, matrix = pool_fixture()
        selected = diagnostics.select_source(pool, matrix, [], public_ids=["public-1", "public-2"])
        self.assertEqual(selected, {"candidate": "candidate-C", "source_id": "source-c", "extra_passed": 0})

    def test_selector_ranks_only_extra_passes_after_public_gate(self):
        pool, matrix = pool_fixture()
        matrix["source-c"]["extra-1"] = {"passed": False, "status": "wrong_answer"}
        matrix["source-b"]["public-1"] = {"passed": False, "status": "wrong_answer"}
        selected = diagnostics.select_source(pool, matrix, ["extra-1", "extra-2"], public_ids=["public-1", "public-2"])
        self.assertEqual(selected, {"candidate": "candidate-A", "source_id": "source-a", "extra_passed": 2})

    def test_selector_abstains_when_sources_invalid_or_public_evidence_incomplete(self):
        pool, matrix = pool_fixture()
        pool["candidates"]["candidate-C"]["source_valid"] = False
        del matrix["source-a"]["public-2"]
        matrix["source-b"]["public-1"] = {"passed": False, "status": "wrong_answer"}
        self.assertIsNone(diagnostics.select_source(pool, matrix, [], public_ids=["public-1", "public-2"]))

    def test_selector_does_not_treat_truthy_values_as_pass_or_eligibility(self):
        pool, matrix = pool_fixture()
        pool["candidates"]["candidate-C"]["source_valid"] = "yes"
        matrix["source-a"]["public-1"]["passed"] = 1
        matrix["source-b"]["public-1"]["passed"] = "true"
        self.assertIsNone(diagnostics.select_source(pool, matrix, [], public_ids=["public-1"]))

    def test_selector_missing_extra_evidence_is_an_integrity_failure(self):
        pool, matrix = pool_fixture()
        del matrix["source-a"]["extra-1"]
        with self.assertRaises(ValueError):
            diagnostics.select_source(pool, matrix, ["extra-1"], public_ids=["public-1"])

    def test_selector_can_skip_ineligible_source_missing_diagnostic_rows(self):
        pool, matrix = pool_fixture()
        pool["candidates"]["candidate-C"]["source_valid"] = False
        del matrix["source-c"]
        selected = diagnostics.select_source(pool, matrix, ["extra-1"], public_ids=["public-1"])
        self.assertEqual(selected["candidate"], "candidate-A")

    def test_selector_rejects_corrupted_tie_order_or_duplicate_extra_weight(self):
        for mutation in ("duplicate-alias", "missing-alias", "duplicate-extra"):
            pool, matrix = pool_fixture()
            extras = ["extra-1"]
            if mutation == "duplicate-alias":
                pool["candidate_order"][-1] = pool["candidate_order"][0]
            elif mutation == "missing-alias":
                pool["candidate_order"].pop()
            else:
                extras.append("extra-1")
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                diagnostics.select_source(pool, matrix, extras, public_ids=["public-1"])


class BenchmarkDiagnosticReceiptTests(unittest.TestCase):
    def setUp(self):
        self.files = {"solution.py": "raise AssertionError('Synthetic source must never execute')"}
        self.cases = [case(), case("public-2", expected={"nested": [True, 1, 1.0]})]
        self.contract = {"image": "sha256:" + "a" * 64, "case_timeout_seconds": 12, "suite_timeout_seconds": 300}
        self.valid = receipt(self.files, self.cases, self.contract)

    def check(self, value):
        return diagnostics.receipt_rows(value, self.files, self.cases, self.contract)

    def test_receipt_returns_verified_ordered_outcomes(self):
        self.assertEqual(self.check(self.valid), self.valid["outcomes"])

    def test_receipt_accepts_explicit_json_null_but_rejects_missing_actual(self):
        cases = [case(expected=None, input_value=None)]
        value = receipt(self.files, cases, self.contract)
        self.assertIsNone(diagnostics.receipt_rows(value, self.files, cases, self.contract)[0]["actual"])
        del value["outcomes"][0]["actual"]
        with self.assertRaises(ValueError):
            diagnostics.receipt_rows(value, self.files, cases, self.contract)

    def test_receipt_rejects_source_suite_adapter_image_and_limit_drift(self):
        for key, replacement in (
            ("protocol", "other"), ("schema_version", True), ("source_sha256", "b" * 64),
            ("suite_sha256", "b" * 64), ("adapter_sha256", "b" * 64),
            ("image_id", "sha256:" + "b" * 64), ("case_timeout_seconds", 12.0),
            ("timeout_seconds", 299),
        ):
            value = deepcopy(self.valid)
            value[key] = replacement
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.check(value)

    def test_receipt_rejects_infrastructure_failures_and_truthy_flags(self):
        for key, replacement in (
            ("cleanup_verified", False), ("cleanup_verified", 1), ("exit_code", 1),
            ("exit_code", False), ("timed_out", True), ("timed_out", 0),
            ("output_truncated", True), ("input_delivery_failed", True), ("status", "error"),
        ):
            value = deepcopy(self.valid)
            value[key] = replacement
            with self.subTest(key=key, replacement=replacement), self.assertRaises(ValueError):
                self.check(value)

    def test_receipt_rejects_missing_duplicate_reordered_or_mislabeled_outcomes(self):
        variants = []
        value = deepcopy(self.valid)
        value["outcomes"].pop()
        variants.append(value)
        value = deepcopy(self.valid)
        value["outcomes"][1] = deepcopy(value["outcomes"][0])
        variants.append(value)
        value = deepcopy(self.valid)
        value["outcomes"].reverse()
        variants.append(value)
        for malformed in (None, [], 7):
            value = deepcopy(self.valid)
            value["outcomes"][1] = malformed
            variants.append(value)
        for field, replacement in (("index", True), ("id", "unknown"), ("requirement", "other"), ("passed", 1)):
            value = deepcopy(self.valid)
            value["outcomes"][1][field] = replacement
            variants.append(value)
        for index, value in enumerate(variants):
            with self.subTest(index=index), self.assertRaises(ValueError):
                self.check(value)

    def test_receipt_recomputes_exact_nested_json_correctness(self):
        value = deepcopy(self.valid)
        value["outcomes"][1]["actual"] = {"nested": [1, 1, 1.0]}
        with self.assertRaises(ValueError):
            self.check(value)
        value["outcomes"][1].update(passed=False, status="wrong_answer")
        value.update(passed=False, status="failed")
        self.assertFalse(self.check(value)[1]["passed"])

    def test_receipt_preserves_candidate_failures_without_reusing_infrastructure_failure(self):
        for status in ("error", "timeout", "output_error", "output_limit", "invalid_output"):
            value = deepcopy(self.valid)
            value["outcomes"][1].update(status=status, passed=False)
            del value["outcomes"][1]["actual"]
            value.update(passed=False, status="failed")
            with self.subTest(status=status):
                self.assertEqual(self.check(value)[1]["status"], status)
                value["outcomes"][1]["actual"] = None
                with self.assertRaises(ValueError):
                    self.check(value)

    def test_receipt_rejects_missing_actual_unknown_status_and_aggregate_tampering(self):
        value = deepcopy(self.valid)
        del value["outcomes"][0]["actual"]
        with self.assertRaises(ValueError):
            self.check(value)
        for mutation in ({"passed": False}, {"passed": 1}, {"status": "failed"}, {"case_count": True}, {"case_count": 3}):
            value = deepcopy(self.valid)
            value.update(mutation)
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                self.check(value)
        value = deepcopy(self.valid)
        value["outcomes"][0].update(status="skipped", passed=False)
        del value["outcomes"][0]["actual"]
        value.update(passed=False, status="failed")
        with self.assertRaises(ValueError):
            self.check(value)


class BenchmarkDiagnosticGateTests(unittest.TestCase):
    def test_completion_gate_accepts_exact_finished_live_and_rehearsal_cohorts(self):
        for mode in ("live", "rehearsal"):
            report, prereg, frozen = cohort_fixture()
            report["mode"] = mode
            with self.subTest(mode=mode):
                self.assertEqual(diagnostics.completion_gate(report, prereg, frozen), report["contract"])

    def test_completion_gate_rejects_unfinished_or_partial_provider_cohorts(self):
        for mutation in (
            {"status": "running"}, {"phase": "private_evaluation"}, {"mode": "offline"},
            {"active_case": {"run_id": "active"}}, {"unexecuted": ["last-run"]},
            {"censored": [{"reason": "budget"}]}, {"experiment": "other-study"},
        ):
            report, prereg, frozen = cohort_fixture()
            report.update(mutation)
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                diagnostics.completion_gate(report, prereg, frozen)

    def test_completion_gate_rejects_missing_duplicate_reordered_and_boolean_roster_rows(self):
        for mutation in ("missing", "duplicate", "reordered", "boolean"):
            report, prereg, frozen = cohort_fixture()
            if mutation == "missing":
                report["cases"].pop()
            elif mutation == "duplicate":
                report["cases"][-1] = deepcopy(report["cases"][0])
            elif mutation == "reordered":
                report["cases"].reverse()
            else:
                report["cases"][0]["repetition"] = False
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                diagnostics.completion_gate(report, prereg, frozen)

    def test_completion_gate_rejects_contract_or_preregistered_roster_changes(self):
        for mutation in ("digest", "contract", "preregistered-roster"):
            report, prereg, frozen = cohort_fixture()
            if mutation == "digest":
                report["contract_sha"] = "0" * 64
            elif mutation == "contract":
                prereg["contract"]["milestones"] = 3
            else:
                prereg["roster"].pop()
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                diagnostics.completion_gate(report, prereg, frozen)

    def test_completion_gate_rejects_unfrozen_changed_or_boolean_typed_trajectory_identity(self):
        for mutation in ("protocol", "contract", "private-started", "private-boolean", "missing", "boolean-repetition"):
            report, prereg, frozen = cohort_fixture()
            if mutation == "protocol":
                frozen["protocol"] = "other"
            elif mutation == "contract":
                frozen["contract_sha"] = "0" * 64
            elif mutation == "private-started":
                frozen["private_evaluation_started"] = True
            elif mutation == "private-boolean":
                frozen["private_evaluation_started"] = 0
            elif mutation == "missing":
                frozen["trajectories"].pop()
            else:
                frozen["trajectories"][0]["repetition"] = False
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                diagnostics.completion_gate(report, prereg, frozen)

    def test_prepare_rejects_incomplete_cohort_before_reading_execution_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report, prereg, frozen = cohort_fixture()
            report["status"] = "running"
            for name, document in (("results.json", report), ("preregistered.json", prereg), ("frozen-trajectories.json", frozen)):
                (root / name).write_text(json.dumps(document))
            with self.assertRaisesRegex(ValueError, "Full terminal"):
                diagnostics.prepare(root)

    def test_bound_inputs_detects_changes_after_reading_frozen_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence = root / "results.json"
            evidence.write_text('{"status":"finished"}')
            bound = diagnostics.BoundInputs(root)
            self.assertEqual(bound.read("results.json"), {"status": "finished"})
            self.assertTrue(bound.unchanged())
            self.assertIn("results.json", bound.hashes)
            evidence.write_text('{"status":"running"}')
            self.assertFalse(bound.unchanged())

    def test_repeated_read_cannot_rebase_an_existing_fingerprint(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence = root / "results.json"
            evidence.write_text('{"status":"finished"}')
            bound = diagnostics.BoundInputs(root)
            bound.read("results.json")
            original = deepcopy(bound.hashes)
            evidence.write_text('{"status":"running"}')
            with self.assertRaises(ValueError):
                bound.read("results.json")
            self.assertEqual(bound.hashes, original)

    def test_bound_json_rejects_duplicate_members_and_nonfinite_values(self):
        values = ('{"value":1,"value":2}', '{"nested":{"a":1,"a":2}}', '{"value":NaN}', '{"value":Infinity}', '{"value":-Infinity}')
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for index, raw in enumerate(values):
                path = root / f"invalid-{index}.json"
                path.write_text(raw)
                with self.subTest(raw=raw), self.assertRaises(ValueError):
                    diagnostics.BoundInputs(root).read(path)

    def test_bound_inputs_rejects_external_paths_and_symlinked_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "frozen"
            root.mkdir()
            outside = Path(directory) / "outside.json"
            outside.write_text("{}")
            linked = root / "linked.json"
            linked.symlink_to(outside)
            bound = diagnostics.BoundInputs(root)
            for path in (outside, "../outside.json", "linked.json"):
                with self.subTest(path=path), self.assertRaises(ValueError):
                    bound.read(path)
            self.assertEqual(bound.hashes, {})

    def test_new_artifact_write_preserves_existing_result(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "diagnostic" / "results.json"
            diagnostics.save_new(output, {"status": "failed", "reason": "retained"})
            original = output.read_bytes()
            with self.assertRaises(FileExistsError):
                diagnostics.save_new(output, {"status": "passed"})
            self.assertEqual(output.read_bytes(), original)


class BenchmarkDiagnosticRunTests(unittest.TestCase):
    def test_failed_cohort_gate_precedes_output_creation_and_validator_factory(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "diagnostic"
            factory = Mock()
            with patch.object(diagnostics, "prepare", side_effect=ValueError("Incomplete cohort")):
                with self.assertRaisesRegex(ValueError, "Incomplete cohort"):
                    diagnostics.run(Path(directory) / "primary", output, validator_factory=factory)
            factory.assert_not_called()
            self.assertFalse(output.exists())

    def test_invalid_parallelism_or_output_paths_fail_before_preparation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            study = root / "primary"
            study.mkdir()
            existing = root / "existing"
            existing.mkdir()
            for output, workers in ((root / "new", True), (root / "new", 3), (existing, 1), (study / "inside", 1), (root, 1)):
                with self.subTest(output=output, workers=workers), patch.object(diagnostics, "prepare") as prepare:
                    with self.assertRaises(ValueError):
                        diagnostics.run(study, output, max_workers=workers, validator_factory=Mock())
                    prepare.assert_not_called()

    def test_changed_input_stops_before_validator_or_new_output(self):
        with tempfile.TemporaryDirectory() as directory:
            plan = execution_plan(directory)
            output = Path(directory) / "diagnostic"
            factory = Mock()
            with patch.object(diagnostics, "prepare", return_value=plan), patch.object(diagnostics, "_unchanged", return_value=False):
                with self.assertRaisesRegex(ValueError, "Primary evidence changed"):
                    diagnostics.run(plan["primary_run"], output, validator_factory=factory)
            factory.assert_not_called()
            self.assertFalse(output.exists())

    def test_bounded_parallel_execution_uses_fresh_validators_and_deterministic_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            plan = execution_plan(directory, source_count=4)
            output = Path(directory) / "diagnostic"
            validators, active = [], {"current": 0, "maximum": 0}
            lock, barrier = threading.Lock(), threading.Barrier(2)

            def evaluate(files, cases):
                with lock:
                    active["current"] += 1
                    active["maximum"] = max(active["maximum"], active["current"])
                try:
                    barrier.wait(timeout=10)
                    return receipt(files, cases, plan["contract"])
                finally:
                    with lock:
                        active["current"] -= 1

            def factory():
                validator = Mock()
                validator.evaluate.side_effect = evaluate
                with lock:
                    validators.append(validator)
                return validator

            with patch.object(diagnostics, "prepare", return_value=plan), patch.object(diagnostics, "_unchanged", return_value=True):
                result = diagnostics.run(plan["primary_run"], output, max_workers=2, validator_factory=factory)
            self.assertEqual(result["status"], "finished")
            self.assertEqual(active["maximum"], 2)
            self.assertEqual(len(validators), 4)
            self.assertTrue(all(validator.evaluate.call_count == 1 for validator in validators))
            index = json.loads((output / "receipt-index.json").read_text())
            self.assertEqual([row["source_id"] for row in index["rows"]], sorted(plan["groups"][0]["sources"]))
            self.assertEqual(result["invocation_count"], 0)
            self.assertIs(result["primary_scores_changed"], False)
            self.assertTrue(all(row["evaluation_attempted"] for row in index["rows"]))

    def test_candidate_boundary_exception_is_retained_without_retry(self):
        with tempfile.TemporaryDirectory() as directory:
            plan = execution_plan(directory)
            output = Path(directory) / "diagnostic"
            files = next(iter(plan["groups"][0]["sources"].values()))["files"]
            last = receipt(files, plan["groups"][0]["cases"], plan["contract"])
            last.update(status="error", cleanup_verified=False)
            validator = Mock(last_receipt=last)
            validator.evaluate.side_effect = RuntimeError("Synthetic boundary failed")
            factory = Mock(return_value=validator)
            with patch.object(diagnostics, "prepare", return_value=plan), patch.object(diagnostics, "_unchanged", return_value=True):
                with self.assertRaisesRegex(ValueError, "never automatic retry"):
                    diagnostics.run(plan["primary_run"], output, validator_factory=factory)
            factory.assert_called_once()
            validator.evaluate.assert_called_once()
            result = json.loads((output / "results.json").read_text())
            index = json.loads((output / "receipt-index.json").read_text())
            self.assertEqual(result["status"], "execution_failed")
            self.assertEqual(index["rows"][0]["error"]["type"], "RuntimeError")
            self.assertIs(index["rows"][0]["evaluation_attempted"], True)
            saved = output / index["rows"][0]["receipt_path"]
            self.assertEqual(json.loads(saved.read_text()), last)

    def test_validator_constructor_failure_retains_nonattempted_job_without_retry(self):
        with tempfile.TemporaryDirectory() as directory:
            plan = execution_plan(directory)
            output = Path(directory) / "diagnostic"
            factory = Mock(side_effect=RuntimeError("Synthetic constructor failed"))
            with patch.object(diagnostics, "prepare", return_value=plan), patch.object(diagnostics, "_unchanged", return_value=True):
                with self.assertRaisesRegex(ValueError, "never automatic retry"):
                    diagnostics.run(plan["primary_run"], output, validator_factory=factory)
            factory.assert_called_once()
            index = json.loads((output / "receipt-index.json").read_text())
            self.assertIs(index["rows"][0]["evaluation_attempted"], False)
            self.assertIs(index["rows"][0]["physical_execution"], False)
            self.assertIsNone(index["rows"][0]["receipt_path"])

    def test_changed_inputs_after_execution_prevent_finished_results(self):
        with tempfile.TemporaryDirectory() as directory:
            plan = execution_plan(directory)
            output = Path(directory) / "diagnostic"
            validator = Mock()
            validator.evaluate.side_effect = lambda files, cases: receipt(files, cases, plan["contract"])
            with patch.object(diagnostics, "prepare", return_value=plan), patch.object(diagnostics, "_unchanged", side_effect=[True, False]):
                with self.assertRaisesRegex(ValueError, "changed during diagnostic"):
                    diagnostics.run(plan["primary_run"], output, validator_factory=lambda: validator)
            validator.evaluate.assert_called_once()
            self.assertTrue((output / "receipt-index.json").exists())
            self.assertFalse((output / "results.json").exists())


class BenchmarkDiagnosticAnalysisTests(unittest.TestCase):
    def fixture(self):
        own = {**case("probe-own", input_value={"probe": "own"}), "origins": [{"source": "scout", "run_id": "run-1"}]}
        peer = {**case("probe-peer", input_value={"probe": "peer"}), "origins": [{"source": "reviewer", "run_id": "run-2"}]}
        cases = [case("public:one"), case("private:r1-a"), case("private:r1-b"),
                 {**case("private:r2"), "requirement": "R2"}, own, peer,
                 case("witness:f1"), case("witness:f2")]
        group = {
            "project_id": "graph-patch", "stage_index": 0, "cases": cases,
            "public_ids": ["public:one"], "private_ids": ["private:r1-a", "private:r1-b", "private:r2"],
            "sources": {source: {"occurrences": [{"kind": kind}]} for source, kind in
                        (("a", "candidate"), ("b", "candidate"), ("f1", "fault"), ("f2", "fault"), ("gold", "correct_control"))},
            "probes": [own, peer],
            "faults": [{"id": source, "family": "one-semantic-family", "source_id": source, "witness_ids": [f"witness:{source}"]}
                       for source in ("f1", "f2")],
            "controls": [{"id": "golden", "source_id": "gold"}],
            "pools": [{"run_id": "run-1", "policy": "independent-four", "repetition": 0, "phase": "initial",
                       "candidate_order": ["candidate-A", "candidate-B"],
                       "candidates": {alias: {"source_id": source, "source_valid": True}
                                      for alias, source in (("candidate-A", "a"), ("candidate-B", "b"))},
                       "selected_alias": "candidate-A", "selected_final_source_id": "a",
                       "own_probe_inputs": [diagnostics.digest(own["input"])]}],
        }
        matrix = {source: {item["id"]: {"passed": True, "status": "passed"} for item in cases} for source in group["sources"]}
        for source, ids in (("a", ["private:r1-a", "probe-own"]), ("b", ["probe-peer"]),
                            ("f1", ["witness:f1", "probe-own", "probe-peer"]), ("f2", ["witness:f2", "probe-own"])):
            for case_id in ids:
                matrix[source][case_id] = {"passed": False, "status": "wrong_answer"}
        return group, matrix

    def test_quality_requires_every_case_in_requirement_and_never_selects_on_private_scores(self):
        group, matrix = self.fixture()
        result = diagnostics.analyze_group(group, matrix)
        quality = result["source_qualities"]["a"]
        self.assertEqual((quality["private_passed"], quality["private_total"]), (2, 3))
        self.assertEqual((quality["requirements_passed"], quality["requirements_total"]), (1, 2))
        self.assertEqual(quality["requirement_coverage"], {"R1": False, "R2": True})
        pool = result["pools"][0]
        own = pool["counterfactuals"]["own_history"]
        self.assertEqual(own["public_only"]["selection"]["source_id"], "a")
        self.assertEqual(own["public_only"]["requirement_selection_regret"], 1)
        self.assertIs(own["public_only"]["correct_available_but_not_selected"], True)
        self.assertEqual(own["scout"]["selection"]["source_id"], "b")
        self.assertEqual(own["scout"]["requirement_selection_regret"], 0)
        self.assertEqual(own["reviewer"]["evidence_ids"], [])
        self.assertEqual(pool["counterfactuals"]["pooled_same_project_stage"]["reviewer"]["evidence_ids"], ["probe-peer"])

    def test_fault_utility_counts_families_once_and_separates_probe_discrimination(self):
        group, matrix = self.fixture()
        result = diagnostics.analyze_group(group, matrix)
        self.assertIs(result["qualified"], True)
        utility = result["pooled_probe_utility"]
        self.assertEqual(utility["public_surviving_family_count"], 1)
        self.assertEqual(utility["public_surviving_families_killed"], ["one-semantic-family"])
        self.assertEqual(utility["candidate_discriminating_probes"], 2)
        self.assertTrue(all(not probe["unique_family_contribution_within_pooled_probes"] for probe in result["generated_probes"]))
        own = next(probe for probe in result["generated_probes"] if probe["id"] == "probe-own")
        self.assertEqual(own["killed_public_surviving_fault_ids"], ["f1", "f2"])

    def test_ineligible_private_perfect_source_is_not_an_achievable_selection_miss(self):
        group, matrix = self.fixture()
        group["pools"][0]["candidates"]["candidate-B"]["source_valid"] = False
        pool = diagnostics.analyze_group(group, matrix)["pools"][0]
        self.assertEqual(pool["best_any_requirements_passed"], 2)
        self.assertEqual(pool["best_eligible_public_passing_requirements_passed"], 1)
        self.assertIs(pool["correct_eligible_public_passing_source_available"], False)
        self.assertIs(pool["actual_same_pool_correct_selection_miss"], False)

    def test_repaired_final_selection_outside_initial_pool_has_no_same_pool_regret(self):
        group, matrix = self.fixture()
        group["sources"]["repaired"] = {"occurrences": [{"kind": "candidate"}]}
        matrix["repaired"] = deepcopy(matrix["b"])
        group["pools"][0]["selected_final_source_id"] = "repaired"
        pool = diagnostics.analyze_group(group, matrix)["pools"][0]
        self.assertIs(pool["actual_final_selection_in_this_pool"], False)
        self.assertIsNone(pool["actual_same_pool_requirement_regret"])
        self.assertIsNone(pool["actual_same_pool_correct_selection_miss"])

    def test_controls_witnesses_and_complete_matrix_are_required(self):
        for mutation in ("bad-control", "unkilled-fault", "missing-source", "missing-case"):
            group, matrix = self.fixture()
            if mutation == "bad-control":
                matrix["gold"]["probe-own"] = {"passed": False, "status": "wrong_answer"}
            elif mutation == "unkilled-fault":
                matrix["f1"]["witness:f1"] = {"passed": True, "status": "passed"}
            elif mutation == "missing-source":
                del matrix["a"]
            else:
                del matrix["a"]["private:r2"]
            with self.subTest(mutation=mutation):
                if mutation in {"missing-source", "missing-case"}:
                    with self.assertRaises(ValueError):
                        diagnostics.analyze_group(group, matrix)
                else:
                    self.assertIs(diagnostics.analyze_group(group, matrix)["qualified"], False)


class BenchmarkDiagnosticAuditTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.plan = execution_plan(self.directory.name, source_count=2)
        primary = Path(self.plan["primary_run"])
        primary.mkdir()
        self.primary_evidence = primary / "bound-evidence.json"
        self.primary_evidence.write_text('{"cohort":"frozen"}')
        self.plan["input_hashes"] = {"bound-evidence.json": hashlib.sha256(self.primary_evidence.read_bytes()).hexdigest()}
        self.output = self.root / "diagnostic"

        def factory():
            validator = Mock()
            validator.evaluate.side_effect = lambda files, cases: receipt(files, cases, self.plan["contract"])
            return validator

        with patch.object(diagnostics, "prepare", return_value=deepcopy(self.plan)):
            diagnostics.run(self.plan["primary_run"], self.output, validator_factory=factory)

    def load(self, name):
        return json.loads((self.output / name).read_text())

    def write(self, name, value):
        (self.output / name).write_text(json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n")

    def audit(self):
        with patch.object(diagnostics, "prepare", return_value=deepcopy(self.plan)) as prepare, patch.object(diagnostics, "BlackboxValidator") as validator:
            result = diagnostics.audit(self.output)
            validator.assert_not_called()
            prepare.assert_called_once_with(self.plan["primary_run"], require_current_evaluator=False)
            return result

    def rewrite_index(self, index):
        self.write("receipt-index.json", index)
        for row in index["rows"]:
            self.write("rows/" + row["job_id"] + ".json", row)

    def rewrite_plan_bindings(self, plan):
        """Keep mutable diagnostic links consistent to exercise the frozen anchor."""
        self.write("plan.json", plan)
        plan_sha = hashlib.sha256((self.output / "plan.json").read_bytes()).hexdigest()
        index, result = self.load("receipt-index.json"), self.load("results.json")
        index["plan_sha256"] = result["plan_sha256"] = plan_sha
        for row in index["rows"]:
            row["plan_sha256"] = plan_sha
        self.rewrite_index(index)
        self.write("results.json", result)

    def test_audit_reconciles_frozen_sources_receipts_and_scores_without_execution(self):
        result = self.audit()
        self.assertIs(result["passed"], True)
        self.assertEqual(result["verified_matrix_rows"], 2)
        self.assertEqual(result["audited_groups"], 1)
        self.assertEqual(result["invocation_count"], 0)
        self.assertIs(result["inputs_unchanged"], True)

    def test_audit_rejects_changed_row_sidecar(self):
        row = self.load("receipt-index.json")["rows"][0]
        row["image"] = "sha256:" + "b" * 64
        self.write("rows/" + row["job_id"] + ".json", row)
        with self.assertRaisesRegex(ValueError, "Row sidecar"):
            self.audit()

    def test_audit_rejects_changed_raw_receipt(self):
        row = self.load("receipt-index.json")["rows"][0]
        value = self.load(row["receipt_path"])
        value["outcomes"][0]["actual"] = {"tampered": True}
        self.write(row["receipt_path"], value)
        with self.assertRaisesRegex(ValueError, "Raw typed receipt changed"):
            self.audit()

    def test_audit_recomputes_receipt_correctness_after_mutable_hashes_are_rebound(self):
        index = self.load("receipt-index.json")
        row = index["rows"][0]
        value = self.load(row["receipt_path"])
        value["outcomes"][0]["actual"] = {"tampered": True}
        self.write(row["receipt_path"], value)
        row["receipt_sha256"] = hashlib.sha256((self.output / row["receipt_path"]).read_bytes()).hexdigest()
        self.rewrite_index(index)
        with self.assertRaisesRegex(ValueError, "exact typed actual"):
            self.audit()

    def test_audit_rejects_missing_or_duplicate_execution_rows(self):
        original = self.load("receipt-index.json")
        for mutation in ("missing", "duplicate"):
            index = deepcopy(original)
            if mutation == "missing":
                index["rows"].pop()
            else:
                index["rows"][1] = deepcopy(index["rows"][0])
            self.write("receipt-index.json", index)
            with self.subTest(mutation=mutation), self.assertRaisesRegex(ValueError, "Missing, duplicate"):
                self.audit()

    def test_audit_rejects_orphan_rows_and_receipts(self):
        for directory in ("rows", "receipts"):
            extra = self.output / directory / "orphan.json"
            extra.write_text("{}")
            with self.subTest(directory=directory), self.assertRaisesRegex(ValueError, "Orphan"):
                self.audit()
            extra.unlink()

    def test_audit_rejects_mutated_evaluator_copy(self):
        path = self.output / "evaluator.py"
        path.write_bytes(path.read_bytes() + b"\n# Changed retained evaluator\n")
        with self.assertRaises(ValueError):
            self.audit()

    def test_audit_frozen_evaluator_hash_cannot_be_replaced_with_consistent_mutable_claims(self):
        path = self.output / "evaluator.py"
        path.write_bytes(path.read_bytes() + b"\n# Different evaluator\n")
        changed_sha = hashlib.sha256(path.read_bytes()).hexdigest()
        plan = self.load("plan.json")
        plan["diagnostic_source_sha256"] = plan["frozen_diagnostic_source_sha256"] = changed_sha
        self.rewrite_plan_bindings(plan)
        with self.assertRaises(ValueError):
            self.audit()

    def test_audit_rejects_boolean_concurrency_even_with_updated_plan_links(self):
        plan = self.load("plan.json")
        plan["max_workers"] = True
        self.rewrite_plan_bindings(plan)
        with self.assertRaises(ValueError):
            self.audit()

    def test_audit_rejects_observed_overlap_above_frozen_worker_limit(self):
        index = self.load("receipt-index.json")
        for row in index["rows"]:
            row.update(started_monotonic_ns=100, finished_monotonic_ns=200, elapsed_seconds=100 / 1e9)
        self.rewrite_index(index)
        with self.assertRaises(ValueError):
            self.audit()

    def test_audit_rejects_changed_derived_scores(self):
        result = self.load("results.json")
        result["groups"][0]["unique_sources"] += 1
        self.write("results.json", result)
        with self.assertRaisesRegex(ValueError, "derived selection/fault scores"):
            self.audit()

    def test_audit_rejects_changed_primary_bound_input(self):
        self.primary_evidence.write_text('{"cohort":"changed"}')
        with self.assertRaisesRegex(ValueError, "Primary inputs changed"):
            self.audit()


if __name__ == "__main__":
    unittest.main()
