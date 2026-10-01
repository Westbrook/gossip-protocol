"""No-Docker checks of benchmark accounting, provenance, and early failure."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from devtools.benchmark_validation import fixture_jobs, run_benchmark, run_seeded_failure
from devtools.validation_session import digest


class FakeSessions:
    def __init__(self, fail_at=None, reuse_final=False):
        self.calls = []
        self.fail_at = fail_at
        self.reuse_final = reuse_final
        self.cache = {}

    def __call__(self, output, **kwargs):
        owner = self
        number = len(self.calls)
        self.calls.append((output, kwargs))
        output.mkdir()
        cache = str(kwargs["cache"])
        warm = cache in self.cache
        visible = self.cache.setdefault(cache, f"visible-{number}")

        class Session:
            def run(self, jobs):
                passed = number != owner.fail_at
                if not passed:
                    kwargs["on_negative"]()
                rows = []
                for job in jobs:
                    final = job.purpose == "final"
                    rows.append(dict(name=job.name, purpose=job.purpose, status="passed" if passed else "failed",
                                     binding=dict(source_sha256=digest(job.files), suite_sha256=digest(job.cases),
                                                  runtime_identity="fake-daemon-v1"),
                                     execution_id=("reused-final" if owner.reuse_final else f"final-{number}") if final else visible,
                                     physical=final or (not warm and job.name == "visible-first"),
                                     reuse=None if final else {"kind": "persisted"} if warm else None))
                result = dict(passed=passed, runtime_contract={"runtime": "fake unchanged host"}, results=rows,
                    counts=dict(logical_jobs=3, physical_executions=1 if warm else 2, preflights=1,
                                reused_persisted=2 if warm else 0, reused_singleflight=0 if warm else 1,
                                failed=0 if passed else 3, infrastructure_failed=0, cancelled=0),
                    resources={"scope": "fake host"}, io_accounting={"scope": "fake I/O"})
                (output / "session.json").write_text(json.dumps(result))
                return result

            def cancel(self):
                pass

        return Session()


class BenchmarkValidationTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.output = Path(self.folder.name) / "benchmark"

    def test_fixture_preserves_original_source_oracle_and_final_independence(self):
        jobs = fixture_jobs()
        self.assertEqual(digest(jobs[0].files), "04b9e4bef26a2fc1cbebf73d0ae465eba6e07b6677c7fe8b8474f628afdfab73")
        self.assertEqual(digest(jobs[0].cases), "b9d3e6fd1e9193305d51447f883591e393595b3d8683c83e4e2ff96d8726a7b0")
        self.assertEqual([job.purpose for job in jobs], ["visible", "visible", "final"])
        self.assertTrue(jobs[2].reason)

    def test_alternates_three_cold_samples_then_reuses_only_last_matching_cache(self):
        fake = FakeSessions()
        with patch("devtools.benchmark_validation.source_binding", return_value={"source": "fixed"}):
            result = run_benchmark(self.output, session_factory=fake)
        self.assertTrue(result["passed"], result)
        self.assertEqual([call[1]["budget"].capacity() for call in fake.calls], [1, 2, 1, 2, 1, 2, 2])
        self.assertEqual(len({call[1]["cache"] for call in fake.calls[:6]}), 6)
        self.assertEqual(fake.calls[5][1]["cache"], fake.calls[6][1]["cache"])
        for statistics in result["statistics"].values():
            self.assertEqual(statistics["count"], 3)
            self.assertEqual(statistics["p50"]["status"], "measured")
            self.assertEqual(statistics["p95"]["status"], "unavailable")
        self.assertIsNone(result["first_negative_seconds"])

    def test_failed_batch_retains_evidence_and_never_retries_or_starts_warm_batch(self):
        fake = FakeSessions(fail_at=1)
        with patch("devtools.benchmark_validation.source_binding", return_value={"source": "fixed"}):
            result = run_benchmark(self.output, session_factory=fake)
        self.assertFalse(result["passed"])
        self.assertEqual(len(fake.calls), 2)
        self.assertIsNone(result["warm_run"])
        self.assertIsNotNone(result["first_negative_seconds"])
        self.assertTrue(Path(result["cold_runs"][-1]["receipt"]).is_file())

    def test_reused_independent_final_and_changed_source_fail_closed(self):
        with patch("devtools.benchmark_validation.source_binding", return_value={"source": "fixed"}):
            result = run_benchmark(self.output, session_factory=FakeSessions(reuse_final=True))
        self.assertFalse(result["passed"])
        self.assertIn("Independent final", result["error"])
        fake = FakeSessions()
        with patch("devtools.benchmark_validation.source_binding", side_effect=[{"source": "before"}, {"source": "after"}]):
            result = run_benchmark(Path(self.folder.name) / "changed", session_factory=fake)
        self.assertFalse(result["passed"])
        self.assertEqual(fake.calls, [])

    def test_existing_destination_cannot_be_overwritten(self):
        self.output.mkdir()
        marker = self.output / "historical"
        marker.write_text("keep")
        with self.assertRaises(FileExistsError):
            run_benchmark(self.output, session_factory=FakeSessions())
        self.assertEqual(marker.read_text(), "keep")

    def test_seeded_syntax_diagnostic_fails_before_tools_without_candidate_execution(self):
        result = run_seeded_failure(self.output, "syntax")
        self.assertTrue(result["passed"], result)
        self.assertEqual(result["gate_status"], "failed")
        self.assertEqual(result["failing_checks"], ["syntax"])
        self.assertEqual(result["candidate_executions"], 0)
        self.assertEqual(result["direct_subprocess_starts"], {})
        self.assertTrue((self.output / "workspace/sample.py").is_file())


if __name__ == "__main__":
    unittest.main()
