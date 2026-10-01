"""Measurement boundaries, failure paths, and units for verification telemetry."""
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch

from devtools.telemetry import IOAccounting, ProcessTelemetry, _Usage, distribution


class TelemetryTests(unittest.TestCase):
    def test_cpu_deltas_and_lifetime_rss_units_are_explicit(self):
        before = (_Usage(10, 2, 100), _Usage(20, 3, 500))
        after = (_Usage(12, 3, 200), _Usage(25, 4, 900))
        for platform, rss in (("darwin", 200), ("linux", 204800)):
            with self.subTest(platform=platform), patch("devtools.telemetry.sys.platform", platform), \
                    patch("devtools.telemetry._usage", side_effect=[before, after]), \
                    patch("devtools.telemetry.time.monotonic", side_effect=[1.0, 3.0]):
                result = ProcessTelemetry("isolated worker").snapshot()
            self.assertEqual(result["self_cpu_seconds"]["value"], 3)
            self.assertEqual(result["waited_children_cpu_seconds"]["value"], 6)
            self.assertEqual(result["self_average_cpu_cores"]["value"], 1.5)
            self.assertEqual(result["self_lifetime_peak_rss_bytes"]["value"], rss)
            self.assertIn("lifetime", result["self_lifetime_peak_rss_bytes"]["scope"])
            self.assertIsNone(result["peak_cpu_cores"]["value"])
            self.assertIsNone(result["process_tree_peak_rss_bytes"]["value"])

    def test_unsupported_counters_do_not_become_zero_resource_usage(self):
        with patch("devtools.telemetry._usage", return_value=None):
            result = ProcessTelemetry("unsupported host").snapshot()
        for key in ("self_cpu_seconds", "waited_children_cpu_seconds", "self_lifetime_peak_rss_bytes"):
            self.assertEqual(result[key]["status"], "unavailable")
            self.assertIsNone(result[key]["value"])
        usage = (_Usage(2, 2, 1), _Usage(0, 0, 0))
        with patch("devtools.telemetry.sys.platform", "unknown-unix"), \
                patch("devtools.telemetry._usage", return_value=usage):
            result = ProcessTelemetry("unknown units").snapshot()
        self.assertEqual(result["self_lifetime_peak_rss_bytes"]["status"], "unavailable")
        self.assertEqual(result["self_cpu_seconds"]["value"], 0)

    def test_decreased_resource_counters_fail_closed(self):
        with patch("devtools.telemetry._usage", side_effect=[
                (_Usage(5, 2, 1), _Usage(3, 1, 1)),
                (_Usage(4, 2, 1), _Usage(3, 1, 1))]):
            result = ProcessTelemetry("reset process").snapshot()
        self.assertEqual(result["self_cpu_seconds"]["status"], "unavailable")
        self.assertEqual(result["self_average_cpu_cores"]["status"], "unavailable")

    def test_byte_counts_measure_completed_payload_operations_not_file_sizes(self):
        meter = IOAccounting("export staging")
        self.assertIsNone(meter.snapshot()["read_bytes"]["value"])
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "payload"
            meter.write_bytes(path, "héllo".encode("utf-8"), "evidence")
            self.assertEqual(meter.read_bytes(path), meter.read_bytes(path))
            with self.assertRaises(FileNotFoundError):
                meter.read_bytes(Path(folder) / "missing")
        report = meter.snapshot()
        self.assertEqual(report["staged_bytes"]["value"], 6)
        self.assertEqual(report["read_bytes"]["value"], 12)
        self.assertEqual(report["read_bytes"]["channels"]["files"]["operations"], 2)
        for invalid in (-1, True, 1.5):
            with self.assertRaises(ValueError):
                meter.record_read(invalid)
        self.assertEqual(meter.snapshot()["read_bytes"]["value"], 12)

    def test_parallel_accounting_snapshots_are_detached(self):
        meter = IOAccounting("two validation threads")
        def record():
            for _ in range(250):
                meter.record_staged(3, "source")
        workers = [threading.Thread(target=record) for _ in range(2)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()
        first = meter.snapshot()
        self.assertEqual(first["staged_bytes"]["value"], 1500)
        first["staged_bytes"]["channels"]["source"]["bytes"] = -1
        self.assertEqual(meter.snapshot()["staged_bytes"]["value"], 1500)

    def test_lock_wait_excludes_body_and_releases_on_exception(self):
        meter = IOAccounting("controller admission")
        lock = threading.Lock()
        with patch("devtools.telemetry.time.monotonic", side_effect=[1.0, 1.25]):
            with self.assertRaisesRegex(RuntimeError, "body failed"):
                with meter.acquire(lock, "run") as acquired:
                    self.assertTrue(acquired)
                    self.assertTrue(lock.locked())
                    raise RuntimeError("body failed")
        self.assertFalse(lock.locked())
        report = meter.snapshot()["lock_wait_seconds"]
        self.assertEqual(report["value"], 0.25)
        self.assertEqual(report["locks"]["run"], {"wait_seconds": .25, "attempts": 1, "acquired": 1, "failed": 0})

    def test_failed_lock_acquisitions_are_recorded_without_releasing_borrowed_lock(self):
        meter = IOAccounting("failed admission")
        lock = threading.Lock()
        lock.acquire()
        with meter.acquire(lock, "run", blocking=False) as acquired:
            self.assertFalse(acquired)
        self.assertTrue(lock.locked())
        lock.release()
        broken = Mock()
        broken.acquire.side_effect = OSError("unavailable")
        with self.assertRaises(OSError):
            with meter.acquire(broken, "run"):
                self.fail("acquire error must propagate")
        broken.release.assert_not_called()
        self.assertEqual(meter.snapshot()["lock_wait_seconds"]["locks"]["run"]["failed"], 2)

    def test_percentiles_require_explicit_sample_support(self):
        self.assertIsNone(distribution([])["p50"]["value"])
        short = distribution([3, 1, 2])
        self.assertEqual(short["p50"]["value"], 2)
        self.assertIsNone(short["p95"]["value"])
        enough = distribution(list(range(1, 21)))
        self.assertEqual(enough["p50"]["value"], 10.5)
        self.assertEqual(enough["p95"]["value"], 19)
        for invalid in ([float("nan")], [float("inf")], [-1], [True]):
            with self.assertRaises(ValueError):
                distribution(invalid)

    def test_real_host_receipt_is_json_serializable_and_has_scope(self):
        meter = ProcessTelemetry("actual host smoke")
        # Real API smoke needs no sleep, subprocess, browser, or Docker daemon.
        result = json.loads(json.dumps(meter.snapshot()))
        self.assertEqual(result["scope"], "actual host smoke")
        self.assertGreaterEqual(result["wall_seconds"]["value"], 0)
        if result["self_cpu_seconds"]["status"] == "measured":
            self.assertGreaterEqual(result["self_cpu_seconds"]["value"], 0)


if __name__ == "__main__":
    unittest.main()
