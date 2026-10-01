"""Isolated behavioral contracts for the developer verification controller."""
import json
import os
import signal
import subprocess
import time
from pathlib import Path
import sys
import tempfile
import textwrap
import unittest
import uuid
from unittest.mock import patch

from devtools.verify import discover, inputs, load_manifest, run, select


class VerificationRunnerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="verify-contract-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        for name in ("tests", "simulation", "analysis"):
            (self.root / name).mkdir()
        self.manifest = {"schema_version": 1, "test_roots": ["tests", "simulation", "analysis"],
                         "input_globs": ["*.py", "tests/**/*.py", "simulation/**/*.py", "analysis/**/*.py", "verification-manifest.json"],
                         "retained_fixtures": [], "classes": {}}

    def module(self, body, name="tests/test_sample.py", classes=None):
        (self.root / name).write_text("import unittest\nimport os\nfrom pathlib import Path\n" + textwrap.dedent(body))
        for cls, config in (classes or {"Sample": {"lane": "fast", "weight": 1}}).items():
            self.manifest["classes"][f"{name}::{cls}"] = config
        self.save()

    def save(self):
        (self.root / "verification-manifest.json").write_text(json.dumps(self.manifest))

    def execute(self, **kwargs):
        return run(self.root, workers=2, output=self.root / "runs", static_command=[sys.executable, "-c", "pass"], **kwargs)

    def test_all_owned_roots_and_method_docker_split_without_imports(self):
        for folder in ("tests", "simulation", "analysis"):
            self.module('''
                raise RuntimeError("must not import during discovery")
                class Sample(unittest.TestCase):
                    def test_unit(self): pass
                    def test_docker(self): pass
            ''', name=f"{folder}/test_sample.py", classes={"Sample": {"lane": "fast", "methods": {"test_docker": {"lane": "docker"}}}})
        inventory = discover(self.root, load_manifest(self.root))
        self.assertEqual(len(inventory), 6)
        self.assertEqual(len(select(inventory, ["fast"], [])), 3)
        self.assertEqual(len(select(inventory, ["docker"], [])), 3)
        self.assertEqual(select(inventory, ["fast"], ["analysis.test_sample.Sample.test_unit"])[0]["method"], "test_unit")

    def test_unmapped_classes_and_empty_selection_fail_before_launch(self):
        self.module("class Sample(unittest.TestCase):\n    def test_one(self): pass\n")
        self.assertIn("no tests", self.execute(selectors=["absent"])["error"])
        self.manifest["classes"] = {}
        self.save()
        result = self.execute()
        self.assertIn("unmapped test class", result["error"])
        self.assertEqual(result["workers_started"], 0)

    def test_syntax_and_static_failures_launch_zero_workers(self):
        self.module('''
            Path("imported").write_text("unexpected")
            class Sample(unittest.TestCase):
                def test_one(self): pass
        ''')
        (self.root / "broken.py").write_text("def broken(: pass")
        syntax = self.execute()
        self.assertIn("SyntaxError", syntax["error"])
        self.assertEqual(syntax["workers_started"], 0)
        (self.root / "broken.py").unlink()
        gate = run(self.root, output=self.root / "runs", static_command=[sys.executable, "-c", "raise SystemExit(1)"])
        self.assertEqual(gate["outcomes"], {"not_run": 1})
        self.assertEqual(gate["workers_started"], 0)
        self.assertFalse((self.root / "imported").exists())

    def test_setup_once_timing_and_retained_failure_logs(self):
        self.module('''
            class Sample(unittest.TestCase):
                @classmethod
                def setUpClass(cls):
                    Path("setups").write_text(Path("setups").read_text() + "x" if Path("setups").exists() else "x")
                def test_first(self): self.assertTrue(True)
                def test_second(self): self.fail("retained diagnosis")
        ''')
        result = self.execute()
        self.assertEqual((self.root / "setups").read_text(), "x")
        self.assertEqual(result["outcomes"], {"passed": 1, "failed": 1})
        job = result["jobs"][0]
        self.assertIn("retained diagnosis", Path(job["log"]).read_text())
        self.assertGreaterEqual(job["setup_seconds"], 0)
        self.assertIn("cleanup_seconds", job["tests"][0])
        self.assertIsNotNone(result["first_negative_seconds"])

    def test_reuse_requires_exact_source_environment_and_retained_fixture(self):
        self.module('''
            class Sample(unittest.TestCase):
                def test_one(self):
                    Path("executions").write_text(Path("executions").read_text() + "x" if Path("executions").exists() else "x")
        ''')
        (self.root / "fixture.txt").write_text("one")
        self.manifest["retained_fixtures"] = ["fixture.txt"]
        self.save()
        # Keep cache generations ordered even when they finish in one second.
        with patch("devtools.verify.uuid.uuid4", side_effect=[uuid.UUID(int=i << 96) for i in range(1, 4)]):
            physical = self.execute()
            self.assertEqual(physical["workers_started"], 1)
            reused = self.execute(reuse=True)
            second_warm = self.execute(reuse=True)
        evidence = json.loads(Path(physical["jobs"][0]["evidence_path"]).read_text())
        self.assertGreater(evidence["duration_seconds"], 0)
        for warm in (reused, second_warm):
            self.assertEqual(warm["status"], "passed")
            self.assertEqual(warm["workers_started"], 0)
            self.assertIn("reused_from", warm["jobs"][0])
            self.assertEqual(warm["jobs"][0]["original_duration_seconds"], evidence["duration_seconds"])
        (self.root / "fixture.txt").write_text("two")
        self.assertEqual(self.execute(reuse=True)["workers_started"], 1)
        (self.root / "dependency.py").write_text("value = 1\n")
        self.assertEqual(self.execute(reuse=True)["workers_started"], 1)
        with patch.dict(os.environ, {"VERIFY_RELEVANT_TEST_INPUT": "changed"}):
            self.assertEqual(self.execute(reuse=True)["workers_started"], 1)
        self.assertEqual((self.root / "executions").read_text(), "xxxx")

    def test_offline_unsets_inherited_docker_and_explicit_lane_selects_only_docker(self):
        self.module('''
            class Sample(unittest.TestCase):
                def test_unit(self): self.assertNotIn("GOSSIP_RUN_DOCKER_TESTS", os.environ)
                def test_docker(self): self.assertEqual(os.environ.get("GOSSIP_RUN_DOCKER_TESTS"), "1")
        ''', classes={"Sample": {"lane": "fast", "methods": {"test_docker": {"lane": "docker"}}}})
        with patch.dict(os.environ, {"GOSSIP_RUN_DOCKER_TESTS": "1"}):
            offline = self.execute()
        docker = self.execute(lanes=["docker"], reuse=True)
        self.assertEqual(offline["outcomes"], {"passed": 1})
        self.assertEqual(docker["outcomes"], {"passed": 1})
        self.assertEqual(docker["selected_total"], 1)
        self.assertEqual(docker["coverage"], "targeted")
        self.assertEqual(self.execute(lanes=["docker"], reuse=True)["workers_started"], 1)

    def test_unknown_skip_and_expected_failure_are_not_green(self):
        self.module('''
            class Sample(unittest.TestCase):
                @unittest.skip("unexpected missing infrastructure")
                def test_skip(self): pass
                @unittest.expectedFailure
                def test_known(self): self.fail("requires declared disposition")
        ''')
        result = self.execute()
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["outcomes"], {"expected_failure": 1, "skipped": 1})

    def test_failure_cancels_queue_and_drains_active_class(self):
        self.module('''
            import time
            class A(unittest.TestCase):
                def test_fail(self):
                    deadline = time.monotonic() + 5
                    while not Path("b-started").exists() and time.monotonic() < deadline: time.sleep(.01)
                    Path("a-failed").touch()
                    self.fail("stop queued classes")
            class B(unittest.TestCase):
                def test_drain(self):
                    Path("b-started").touch()
                    deadline = time.monotonic() + 5
                    while not Path("a-failed").exists() and time.monotonic() < deadline: time.sleep(.01)
                    time.sleep(.2)
                    Path("b-cleaned").touch()
            class C(unittest.TestCase):
                def test_never(self): Path("c-started").touch()
        ''', classes={name: {"lane": "fast", "weight": 1} for name in ("A", "B", "C")})
        result = self.execute()
        self.assertEqual(result["workers_started"], 2)
        self.assertEqual(result["outcomes"], {"failed": 1, "passed": 1, "not_run": 1})
        self.assertTrue((self.root / "b-cleaned").exists())
        self.assertFalse((self.root / "c-started").exists())

    def test_nested_resource_weight_blocks_underprovisioned_run(self):
        self.module("class Sample(unittest.TestCase):\n    def test_one(self): pass\n", classes={"Sample": {"lane": "git", "weight": 4}})
        result = self.execute()
        self.assertEqual(result["workers_started"], 0)
        self.assertIn("use --workers 4", result["error"])

    def test_inputs_changing_during_execution_cannot_be_reused(self):
        self.module('''
            class Sample(unittest.TestCase):
                def test_change(self): Path("dependency.py").write_text("value = 2\\n")
        ''')
        (self.root / "dependency.py").write_text("value = 1\n")
        result = self.execute()
        self.assertEqual(result["status"], "failed")
        self.assertTrue(result["stale_inputs"])
        self.assertEqual(result["outcomes"], {"passed": 1})

    def test_runtime_drift_invalidates_passes_and_prevents_reuse(self):
        self.module('''
            class Sample(unittest.TestCase):
                def test_one(self):
                    Path("executions").write_text(Path("executions").read_text() + "x" if Path("executions").exists() else "x")
        ''')
        initial = {"tools": {"git": {"sha256": "original"}}, "environment_sha256": "unchanged"}
        changed = {"tools": {"git": {"sha256": "replaced"}}, "environment_sha256": "unchanged"}
        with patch("devtools.verify.runtime_fingerprint", side_effect=[initial, changed]):
            stale = self.execute()
        self.assertEqual(stale["outcomes"], {"passed": 1})
        self.assertEqual(stale["status"], "failed")
        self.assertTrue(stale["stale_runtime"])
        self.assertTrue(stale["stale_inputs"])
        self.assertFalse(stale["runtime_reconciliation"]["matches"])
        self.assertIn("runtime changed", stale["error"])
        self.assertEqual(json.loads((Path(stale["session"]) / "runtime-final.json").read_text()), changed)
        with patch("devtools.verify.runtime_fingerprint", return_value=initial):
            replacement = self.execute(reuse=True)
        self.assertEqual(replacement["status"], "passed")
        self.assertEqual(replacement["workers_started"], 1)
        self.assertFalse(replacement["stale_runtime"])
        self.assertEqual((self.root / "executions").read_text(), "xx")

    def test_unavailable_final_runtime_identity_invalidates_evidence(self):
        self.module("class Sample(unittest.TestCase):\n    def test_one(self): pass\n")
        with patch("devtools.verify.runtime_fingerprint", side_effect=[{"tools": {}}, OSError("git disappeared")]):
            result = self.execute()
        self.assertEqual(result["status"], "failed")
        self.assertTrue(result["stale_runtime"])
        self.assertTrue(result["stale_inputs"])
        self.assertIn("could not be reconciled", result["error"])
        self.assertEqual(result["outcomes"], {"passed": 1})

    def test_generated_history_excluded_but_declared_fixture_is_hashed(self):
        self.module("class Sample(unittest.TestCase):\n    def test_one(self): pass\n")
        for directory in ("runs", "results", ".venv"):
            (self.root / directory).mkdir()
            (self.root / directory / "test_bad.py").write_text("invalid syntax ???")
        self.manifest["input_globs"].extend(["**/*.py", "*.json"])
        plan = self.root / "study-plan.json"
        plan.write_text("{\"version\": 1}")
        first = inputs(self.root, self.manifest)
        plan.write_text("{\"version\": 2}")
        self.assertNotEqual(first["study-plan.json"], inputs(self.root, self.manifest)["study-plan.json"])
        self.assertFalse(any(name.startswith(("runs/", "results/", ".venv/")) for name in first))
        self.manifest["retained_fixtures"] = ["results/test_bad.py"]
        self.assertIn("results/test_bad.py", inputs(self.root, self.manifest))

    def test_cached_evidence_tamper_requires_physical_rerun(self):
        self.module("class Sample(unittest.TestCase):\n    def test_one(self): pass\n")
        original = self.execute()
        evidence = Path(original["jobs"][0]["evidence_path"])
        changed = json.loads(evidence.read_text())
        changed["tests"][0]["status"] = "failed"
        evidence.write_text(json.dumps(changed))
        replacement = self.execute(reuse=True)
        self.assertEqual(replacement["workers_started"], 1)
        self.assertEqual(replacement["status"], "passed")
        summary_path = Path(replacement["session"]) / "summary.json"
        saved = json.loads(summary_path.read_text())
        saved["jobs"][0]["tests"] = []
        summary_path.write_text(json.dumps(saved))
        self.assertEqual(self.execute(reuse=True)["workers_started"], 1)

    def test_costly_lane_waits_for_fast_prerequisites(self):
        self.module('''
            import time
            class A(unittest.TestCase):
                def test_fast(self):
                    time.sleep(.1)
                    Path("fast-complete").touch()
            class B(unittest.TestCase):
                def test_fixture(self): self.assertTrue(Path("fast-complete").exists())
        ''', classes={"A": {"lane": "fast"}, "B": {"lane": "fixtures"}})
        result = self.execute()
        self.assertEqual(result["status"], "passed", json.dumps(result, indent=2))

    def test_class_timeout_is_bounded_and_never_claims_cleanup(self):
        self.module('''
            import time
            class Sample(unittest.TestCase):
                def test_hung(self): time.sleep(30)
        ''', classes={"Sample": {"lane": "fast", "timeout_seconds": 1.0}})
        result = self.execute()
        self.assertEqual(result["status"], "failed")
        self.assertLess(result["duration_seconds"], 10)
        self.assertIn("unknown", result["jobs"][0]["cleanup_status"])
        self.assertEqual(result["outcomes"], {"interrupted": 1})

    def test_unsupported_inheritance_dynamic_loading_and_invalid_timeout_fail_closed(self):
        self.module('''
            class Sample(unittest.TestCase):
                def test_one(self): pass
            class Derived(Sample): pass
        ''')
        self.assertIn("inherited", self.execute()["error"])
        self.module('''
            def load_tests(*args): pass
            class Sample(unittest.TestCase):
                def test_one(self): pass
        ''')
        self.assertIn("load_tests", self.execute()["error"])
        self.module("class Sample(unittest.TestCase):\n    def test_one(self): pass\n", classes={"Sample": {"lane": "fast", "timeout_seconds": float("nan")}})
        self.assertIn("invalid timeout", self.execute()["error"])

    def test_failed_batch_reuses_only_intact_passed_classes(self):
        self.module('''
            import time
            class A(unittest.TestCase):
                def test_pass(self):
                    Path("passed-executions").write_text(Path("passed-executions").read_text() + "x" if Path("passed-executions").exists() else "x")
            class B(unittest.TestCase):
                def test_fail(self):
                    deadline = time.monotonic() + 5
                    while not Path("passed-executions").exists() and time.monotonic() < deadline: time.sleep(.01)
                    self.fail("record this failure without discarding A")
        ''', classes={name: {"lane": "fast", "weight": 1} for name in ("A", "B")})
        first = self.execute()
        self.assertEqual(first["status"], "failed", json.dumps(first, indent=2))
        self.assertEqual(first["outcomes"], {"passed": 1, "failed": 1})
        self.assertEqual(first["workers_started"], 2)
        passed_only = self.execute(selectors=["tests.test_sample.A"], reuse=True)
        self.assertEqual(passed_only["status"], "passed", json.dumps(passed_only, indent=2))
        self.assertEqual(passed_only["workers_started"], 0)
        retry = self.execute(reuse=True)
        self.assertEqual(retry["status"], "failed", json.dumps(retry, indent=2))
        self.assertEqual(retry["workers_started"], 1)
        self.assertEqual(retry["reused_classes"], 1)
        self.assertFalse(retry["jobs"][0]["physical"])
        self.assertTrue(retry["jobs"][1]["physical"])
        self.assertEqual((self.root / "passed-executions").read_text(), "x")

    def test_exclusive_classes_reserve_full_larger_worker_budget(self):
        self.module('''
            import time
            def check_exclusive(test):
                descriptor = os.open("exclusive.lock", os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                try:
                    time.sleep(.1)
                finally:
                    os.close(descriptor)
                    Path("exclusive.lock").unlink()
            class A(unittest.TestCase):
                def test_first(self): check_exclusive(self)
            class B(unittest.TestCase):
                def test_second(self): check_exclusive(self)
        ''', classes={"A": {"lane": "fixtures", "weight": 1, "exclusive": True},
                      "B": {"lane": "fixtures", "weight": 1, "methods": {"test_second": {"exclusive": True}}}})
        result = run(self.root, workers=4, output=self.root / "runs", static_command=[sys.executable, "-c", "pass"])
        self.assertEqual(result["status"], "passed", json.dumps(result, indent=2))
        self.assertEqual(result["workers_started"], 2)
        self.assertEqual(result["peak_resource_tokens"], 4)
        self.assertEqual([job["tokens"] for job in result["jobs"]], [4, 4])
        self.manifest["classes"]["tests/test_sample.py::A"]["exclusive"] = "true"
        self.save()
        invalid = self.execute()
        self.assertEqual(invalid["workers_started"], 0)
        self.assertIn("exclusive must be boolean", invalid["error"])

    def test_ready_fit_bypasses_large_job_without_crossing_barriers(self):
        self.module('''
            import time
            def wait_for(path):
                deadline = time.monotonic() + 10
                while not Path(path).exists() and time.monotonic() < deadline: time.sleep(.01)
                if not Path(path).exists(): raise AssertionError("fitting job was not scheduled: " + path)
            class A(unittest.TestCase):
                def test_wait_for_fitting_job(self):
                    wait_for("c-started")
                    Path("a-done").touch()
            class B(unittest.TestCase):
                def test_large(self):
                    self.assertTrue(Path("a-done").exists())
                    self.assertTrue(Path("c-done").exists())
            class C(unittest.TestCase):
                def test_fit(self):
                    Path("c-started").touch()
                    wait_for("a-done")
                    time.sleep(.1)
                    Path("c-done").touch()
            class D(unittest.TestCase):
                def test_exclusive(self): pass
            class E(unittest.TestCase):
                def test_after_exclusive(self): pass
            class F(unittest.TestCase):
                def test_next_lane(self): pass
        ''', classes={"A": {"lane": "fast", "weight": 1}, "B": {"lane": "fast", "weight": 2},
                      "C": {"lane": "fast", "weight": 1}, "D": {"lane": "fast", "weight": 1, "exclusive": True},
                      "E": {"lane": "fast", "weight": 1}, "F": {"lane": "git", "weight": 1}})
        result = self.execute()
        self.assertEqual(result["status"], "passed", json.dumps(result, indent=2))
        self.assertEqual(result["workers_started"], 6)
        self.assertEqual(result["peak_resource_tokens"], 2)
        a, b, c, d, e, f = result["jobs"]
        self.assertLess(c["queue_seconds"], b["queue_seconds"])
        self.assertGreaterEqual(d["queue_seconds"], max(job["queue_seconds"] + job["wall_seconds"] for job in (a, b, c)))
        self.assertGreaterEqual(e["queue_seconds"], d["queue_seconds"] + d["wall_seconds"])
        self.assertGreaterEqual(f["queue_seconds"], max(job["queue_seconds"] + job["wall_seconds"] for job in (a, b, c, d, e)))

    def test_browser_cancellation_allows_owned_detached_child_cleanup(self):
        script = self.root / "fake_browser.py"
        script.write_text(textwrap.dedent('''
            import os, signal, subprocess, sys, time
            from pathlib import Path
            child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"], start_new_session=True)
            Path("browser-child.pid").write_text(str(child.pid))
            def cleanup(signum, frame):
                child.terminate()
                child.wait(timeout=5)
                Path("browser-cleaned").touch()
                raise SystemExit(0)
            signal.signal(signal.SIGTERM, cleanup)
            Path("browser-ready").touch()
            while True: time.sleep(.05)
        '''))
        test_id = "browser/fake::Browser.test_cleanup"
        request = {"root": str(self.root), "result": str(self.root / "result.json"),
                   "job": {"class": "browser/fake::Browser", "lane": "browser", "path": "fake_browser.py",
                           "command": [sys.executable, str(script)], "tests": [{"id": test_id}]}}
        request_path = self.root / "request.json"
        request_path.write_text(json.dumps(request))
        runner = Path(__file__).resolve().parents[1] / "devtools/verify.py"
        process = subprocess.Popen([sys.executable, str(runner), "--worker", str(request_path)],
                                   cwd=self.root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                   text=True, start_new_session=True)
        try:
            deadline = time.monotonic() + 10
            while not (self.root / "browser-ready").exists() and time.monotonic() < deadline:
                if process.poll() is not None: break
                time.sleep(.02)
            self.assertTrue((self.root / "browser-ready").exists())
            process.send_signal(signal.SIGTERM)
            output, _ = process.communicate(timeout=20)
            self.assertEqual(process.returncode, 1, output)
            self.assertTrue((self.root / "browser-cleaned").exists(), output)
            result = json.loads((self.root / "result.json").read_text())
            self.assertEqual(result["status"], "error")
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=5)
            child_pid = self.root / "browser-child.pid"
            if child_pid.exists():
                try: os.kill(int(child_pid.read_text()), signal.SIGKILL)
                except ProcessLookupError: pass


if __name__ == "__main__":
    unittest.main()
