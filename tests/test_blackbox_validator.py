"""Host-oracle boundary tests, plus explicitly opted-in real Docker checks."""

import io
import json
import os
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch

from gossip_harness.blackbox_validator import (
    BlackboxValidator, CHILD_ADAPTER, MAX_OUTPUT_BYTES, PROTOCOL,
    SUPERVISOR_ADAPTER, json_equal,
)
from gossip_harness.pilot import DEFAULT_IMAGE


IMAGE = "sha256:" + "a" * 64
FILES = {"solution.py": "def solve(payload):\n    return payload\n"}
CASES = [{"id": "opaque-case", "input": 4, "expected": 4,
          "requirement": "host-only-requirement-sentinel"}]


class CapturedInput(io.BytesIO):
    def close(self):
        self.retained = self.getvalue()
        super().close()


class FakeProcess:
    def __init__(self, output, code=0, timeout=False):
        self.stdin = CapturedInput()
        self.stdout = io.BytesIO(output)
        self.returncode = code
        self.timeout = timeout
        self.killed = False

    def wait(self, timeout):
        if self.timeout and not self.killed:
            raise subprocess.TimeoutExpired("docker", timeout)
        return self.returncode

    def kill(self):
        self.killed = True
        self.returncode = -9

    def poll(self):
        return self.returncode


def envelope(outputs):
    return json.dumps({"protocol": PROTOCOL,
                       "results": [{"index": index, "status": "ok", "output": output}
                                   for index, output in enumerate(outputs)]}).encode()


def completed(code=0, stdout=b""):
    return subprocess.CompletedProcess([], code, stdout, b"")


class BlackboxValidatorTests(unittest.TestCase):
    def evaluate(self, raw=None, *, cases=CASES, process=None, cleanup=True, **kwargs):
        process = process or FakeProcess(envelope([4]) if raw is None else raw)
        validator = BlackboxValidator(IMAGE, **kwargs)
        with patch("gossip_harness.blackbox_validator.subprocess.Popen", return_value=process), \
             patch.object(validator._sandbox, "_remove", return_value=cleanup):
            receipt = validator.evaluate(FILES, cases)
        return receipt, process

    def test_only_inputs_and_generic_adapter_enter_container(self):
        expected_secret = "host-only-expected-sentinel"
        cases = [{**CASES[0], "expected": expected_secret}]
        invocation = []
        process = FakeProcess(envelope([expected_secret]))

        def start(arguments, **kwargs):
            invocation.append(arguments)
            staging = Path(kwargs["cwd"])
            self.assertEqual(sorted(path.name for path in staging.iterdir()), ["checks", "workspace"])
            self.assertEqual((staging / "workspace/solution.py").read_text(), FILES["solution.py"])
            self.assertEqual((staging / "workspace/solution.py").stat().st_mode & 0o777, 0o444)
            self.assertEqual((staging / "checks/supervisor.py").read_text(), SUPERVISOR_ADAPTER)
            self.assertEqual((staging / "checks/child.py").read_text(), CHILD_ADAPTER)
            for path in staging.rglob("*"):
                if path.is_file():
                    self.assertNotIn(expected_secret, path.read_text())
                    self.assertNotIn(CASES[0]["requirement"], path.read_text())
            self.assertNotIn("OPENAI_API_KEY", kwargs["env"])
            self.assertEqual(kwargs["stdin"], subprocess.PIPE)
            return process

        validator = BlackboxValidator(IMAGE)
        with patch.dict(os.environ, {"OPENAI_API_KEY": "secret-sentinel"}), \
             patch("gossip_harness.blackbox_validator.subprocess.Popen", side_effect=start), \
             patch.object(validator._sandbox, "_remove", return_value=True):
            receipt = validator.evaluate(FILES, cases)
        self.assertTrue(receipt["passed"])
        request = json.loads(process.stdin.retained)
        self.assertEqual(request["cases"], [{"index": 0, "input": 4}])
        self.assertNotIn(expected_secret.encode(), process.stdin.retained)
        self.assertNotIn(CASES[0]["requirement"].encode(), process.stdin.retained)
        self.assertEqual(receipt["outcomes"][0]["id"], CASES[0]["id"])
        arguments = invocation[0]
        for option in ("--interactive", "--pull=never", "--network=none", "--read-only",
                       "--cap-drop=ALL", "--security-opt=no-new-privileges:true",
                       "--user=65534:65534", "--pids-limit=64", "--memory=256m",
                       "--memory-swap=256m", "--cpus=1"):
            self.assertIn(option, arguments)
        mounts = [arguments[index + 1] for index, value in enumerate(arguments) if value == "--mount"]
        self.assertEqual(len(mounts), 2)
        self.assertTrue(all(",readonly," in mount for mount in mounts))
        self.assertFalse(any("docker.sock" in value or "secret-sentinel" in value
                             for value in arguments))
        self.assertEqual(arguments[-3:], [IMAGE, "-I", "/checks/supervisor.py"])

    def test_host_comparison_not_candidate_status_decides_pass(self):
        receipt, _ = self.evaluate(envelope([5]))
        self.assertFalse(receipt["passed"])
        self.assertEqual(receipt["outcomes"][0]["status"], "wrong_answer")
        self.assertEqual(receipt["outcomes"][0]["actual"], 5)
        for forged in ({"passed": True}, {"status": "passed"}, 4.1, True, "4"):
            with self.subTest(forged=forged):
                self.assertFalse(self.evaluate(envelope([forged]))[0]["passed"])

    def test_json_equality_is_type_aware(self):
        self.assertFalse(json_equal(True, 1))
        self.assertFalse(json_equal({"a": False}, {"a": 0}))
        self.assertFalse(json_equal(2.0, 2))
        self.assertFalse(json_equal([1, 2], [2, 1]))
        self.assertFalse(json_equal({"a": 1, "b": 2}, {"a": 1}))
        self.assertTrue(json_equal({"a": [1.0, None]}, {"a": [1.0, None]}))

    def test_valid_nested_values_account_for_transport_envelope_depth(self):
        nested = None
        for _ in range(64):
            nested = [nested]
        cases = [{"input": nested, "expected": nested}]
        self.assertTrue(self.evaluate(envelope([nested]), cases=cases)[0]["passed"])
        self.assertEqual(self.evaluate(envelope([[nested]]))[0]["status"], "invalid_output")

    def test_strict_framing_missing_duplicate_noise_and_unknown_fields_fail_closed(self):
        good = envelope([4])
        malformed = [b"", b"log\n" + good, good + good, b"\xff", b"NaN",
                     b'{"protocol":"gossip-blackbox-v1","results":[],"passed":true}',
                     envelope([]), envelope([4, 4]),
                     good.replace(b'"index": 0', b'"index": true'),
                     good.replace(b'"index": 0', b'"index": 1'),
                     good.replace(b'"status": "ok"', b'"status": "passed"'),
                     good.replace(b'"status": "ok"', b'"status": []'),
                     good.replace(b'"output": 4', b'"output": NaN'),
                     good.replace(b'"output": 4', b'"output": 4, "output": 5'),
                     good.replace(b'"output": 4', b'"output": 4, "passed": true')]
        for raw in malformed:
            with self.subTest(raw=raw):
                receipt, _ = self.evaluate(raw)
                self.assertFalse(receipt["passed"])
                self.assertEqual(receipt["status"], "invalid_output")
                self.assertEqual(len(receipt["outcomes"]), 1)

    def test_child_error_states_fail_the_case(self):
        for status in ("error", "timeout", "output_error", "output_limit", "invalid_output"):
            with self.subTest(status=status):
                raw = json.dumps({"protocol": PROTOCOL, "results": [{"index": 0,
                                  "status": status}]}).encode()
                receipt, _ = self.evaluate(raw)
                self.assertFalse(receipt["passed"])
                self.assertEqual(receipt["outcomes"][0]["status"], status)

    def test_timeout_kills_and_cleanup_is_verified(self):
        process = FakeProcess(envelope([4]), timeout=True)
        receipt, process = self.evaluate(process=process, timeout_seconds=0.01)
        self.assertFalse(receipt["passed"])
        self.assertTrue(receipt["timed_out"])
        self.assertTrue(process.killed)
        self.assertTrue(receipt["cleanup_verified"])
        self.assertEqual(receipt["status"], "timeout")

    def test_cleanup_failure_nonzero_exit_and_excess_output_fail_closed(self):
        self.assertEqual(self.evaluate(cleanup=False)[0]["status"], "cleanup_failed")
        receipt, _ = self.evaluate(process=FakeProcess(envelope([4]), code=1))
        self.assertFalse(receipt["passed"])
        self.assertEqual(receipt["status"], "sandbox_error")
        receipt, _ = self.evaluate(b" " * (MAX_OUTPUT_BYTES + 1) + envelope([4]))
        self.assertFalse(receipt["passed"])
        self.assertTrue(receipt["output_truncated"])
        self.assertEqual(receipt["status"], "output_limit")
        self.assertNotIn("output", receipt)

    def test_popen_errors_do_not_leak_sensitive_exception_text(self):
        validator = BlackboxValidator(IMAGE)
        with patch("gossip_harness.blackbox_validator.subprocess.Popen",
                   side_effect=OSError("secret-sentinel")):
            receipt = validator.evaluate(FILES, CASES)
        self.assertEqual(receipt["status"], "sandbox_error")
        self.assertNotIn("secret-sentinel", json.dumps(receipt))

    def test_unsafe_files_non_json_and_unbounded_inputs_rejected_before_docker(self):
        for files, cases in [({}, CASES), ({"../solution.py": "x"}, CASES),
                             ({**FILES, ".env.local": "secret"}, CASES),
                             ({**FILES, "a//b": "x"}, CASES),
                             ({**FILES, "a\\b": "x"}, CASES),
                             ({**FILES, "/outside": "x"}, CASES),
                             ({"solution.py": "x" * 1_048_577}, CASES),
                             (FILES, []), (FILES, CASES * 513),
                             (FILES, [{"input": float("nan"), "expected": None}]),
                             (FILES, [{"input": {1: 1}, "expected": None}]),
                             (FILES, [{"input": (1,), "expected": None}]),
                             (FILES, [{"input": "x" * 65_537, "expected": None}])]:
            with self.subTest(files=list(files), cases_count=len(cases)), \
                 patch("gossip_harness.blackbox_validator.subprocess.Popen") as started, \
                 self.assertRaises(ValueError):
                BlackboxValidator(IMAGE).evaluate(files, cases)
            started.assert_not_called()

    def test_image_timeout_and_preflight_reuse_pinned_sandbox_contract(self):
        for timeout in (0, True, float("nan"), float("inf")):
            with self.subTest(timeout=timeout), self.assertRaises(ValueError):
                BlackboxValidator(IMAGE, case_timeout_seconds=timeout)
        with self.assertRaises(ValueError):
            BlackboxValidator("python:latest")
        validator = BlackboxValidator(IMAGE)
        with patch.object(validator._sandbox, "preflight", return_value=(True, "available")):
            self.assertEqual(validator.preflight(), (True, "available"))


@unittest.skipUnless(os.environ.get("GOSSIP_RUN_DOCKER_TESTS") == "1", "real Docker opt-in")
class BlackboxDockerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ready, detail = BlackboxValidator(DEFAULT_IMAGE).preflight()
        if not ready:
            raise RuntimeError(detail)

    def run_source(self, source, cases=CASES, **kwargs):
        result = BlackboxValidator(DEFAULT_IMAGE, **kwargs).evaluate({"solution.py": source}, cases)
        self.assertTrue(result["cleanup_verified"], result)
        return result

    def test_good_candidate_and_fresh_case_processes(self):
        result = self.run_source(
            "counter = 0\ndef solve(payload):\n    global counter\n    counter += 1\n    return counter\n",
            [{"input": None, "expected": 1}, {"input": None, "expected": 1}])
        self.assertTrue(result["passed"], result)

    def test_wrong_answer_and_forged_candidate_pass_receipt_are_rejected(self):
        result = self.run_source("def solve(payload):\n    return {'passed': True, 'status': 'passed'}\n")
        self.assertFalse(result["passed"])
        self.assertEqual(result["outcomes"][0]["status"], "wrong_answer")

    def test_import_printing_cannot_forge_transport_frame(self):
        forged = envelope([4]).decode()
        result = self.run_source(f"print({forged!r})\ndef solve(payload):\n    return 4\n")
        self.assertFalse(result["passed"])
        self.assertEqual(result["outcomes"][0]["status"], "invalid_output")

    def test_only_generic_adapters_and_inputs_are_available(self):
        source = '''import os
from pathlib import Path
def solve(payload):
    return {"checks": sorted(path.name for path in Path("/checks").iterdir()),
            "workspace": sorted(path.name for path in Path("/workspace").iterdir()),
            "api_key": os.environ.get("OPENAI_API_KEY"),
            "payload": payload}
'''
        expected = {"checks": ["child.py", "supervisor.py"], "workspace": ["solution.py"],
                    "api_key": None, "payload": {"value": 7}}
        result = self.run_source(source, [{"input": {"value": 7}, "expected": expected,
                                          "requirement": "host-only-secret-requirement"}])
        self.assertTrue(result["passed"], result)

    def test_hanging_case_is_bounded_and_later_case_still_runs(self):
        source = "def solve(payload):\n    if payload:\n        while True: pass\n    return 4\n"
        result = self.run_source(source, [{"input": True, "expected": 4},
                                         {"input": False, "expected": 4}],
                                 case_timeout_seconds=0.3)
        self.assertFalse(result["passed"])
        self.assertEqual(result["outcomes"][0]["status"], "timeout")
        self.assertTrue(result["outcomes"][1]["passed"])

    def test_noisy_candidate_is_bounded(self):
        source = "def solve(payload):\n    print('x' * 200000)\n    return 4\n"
        result = self.run_source(source)
        self.assertFalse(result["passed"])
        self.assertEqual(result["outcomes"][0]["status"], "output_limit")


if __name__ == "__main__":
    unittest.main()
