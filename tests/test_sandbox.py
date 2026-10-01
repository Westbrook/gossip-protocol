"""Contract and failure-path tests; Docker itself is probed separately."""

import io
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from gossip_harness.sandbox import DockerValidator


IMAGE = "sha256:" + "a" * 64
CHECKS = {"run_checks.py": "print('trusted check')\n"}


class FakeProcess:
    def __init__(self, output=b"ok\n", code=0, timeout=False):
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


def completed(code=0, stdout=b""):
    return subprocess.CompletedProcess([], code, stdout, b"")


class DockerValidatorTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.checkout = Path(self.temporary.name) / "candidate"
        self.checkout.mkdir()
        (self.checkout / "module.py").write_text("answer = 42\n")

    def tearDown(self):
        self.temporary.cleanup()

    def validator(self, **kwargs):
        return DockerValidator(IMAGE, CHECKS, **kwargs)

    def test_rejects_mutable_images_and_unsafe_check_paths(self):
        for image in ("python:3.12", "sha256:abc", "repository@" + IMAGE):
            with self.subTest(image=image), self.assertRaises(ValueError):
                DockerValidator(image, CHECKS)
        for path in ("../escape", "/escape", "a/../escape", "a//b", "a\\b"):
            with self.subTest(path=path), self.assertRaises(ValueError):
                DockerValidator(IMAGE, {path: "code"})
        for timeout in (0, float("inf"), float("nan"), True):
            with self.subTest(timeout=timeout), self.assertRaises(ValueError):
                self.validator(timeout_seconds=timeout)

    def test_stages_only_candidate_and_checks_with_constrained_container(self):
        (self.checkout / ".git").mkdir()
        (self.checkout / ".git/config").write_text("private git configuration")
        (self.checkout / ".env.local").write_text("SECRET=never copied")
        invocations = []

        def popen(arguments, **kwargs):
            invocations.append(arguments)
            staged = Path(kwargs["cwd"])
            self.assertEqual((staged / "workspace/module.py").read_text(), "answer = 42\n")
            self.assertFalse((staged / "workspace/.git").exists())
            self.assertFalse((staged / "workspace/.env.local").exists())
            self.assertEqual((staged / "checks/run_checks.py").read_text(), CHECKS["run_checks.py"])
            self.assertEqual((staged / "workspace/module.py").stat().st_mode & 0o777, 0o444)
            self.assertNotIn("OPENAI_API_KEY", kwargs["env"])
            self.assertEqual(kwargs["stdin"], subprocess.DEVNULL)
            return FakeProcess()

        with patch.dict(os.environ, {"OPENAI_API_KEY": "secret-sentinel"}), \
             patch("gossip_harness.sandbox.subprocess.Popen", side_effect=popen), \
             patch("gossip_harness.sandbox.subprocess.run", return_value=completed()):
            validator = self.validator()
            accepted, _ = validator(self.checkout)
        self.assertTrue(accepted)
        arguments = invocations[0]
        for option in ("--pull=never", "--network=none", "--read-only", "--cap-drop=ALL",
                       "--security-opt=no-new-privileges:true", "--user=65534:65534",
                       "--pids-limit=64", "--memory=256m", "--memory-swap=256m", "--cpus=1"):
            self.assertIn(option, arguments)
        mounts = [arguments[index + 1] for index, item in enumerate(arguments) if item == "--mount"]
        self.assertEqual(len(mounts), 2)
        self.assertTrue(all(",readonly," in mount for mount in mounts))
        self.assertFalse(any("docker.sock" in part or "secret-sentinel" in part for part in arguments))
        self.assertEqual(arguments[-3:], [IMAGE, "-I", "/checks/run_checks.py"])
        self.assertTrue(validator.last_receipt["cleanup_verified"])
        self.assertEqual(validator.last_receipt["staging"]["excluded_paths"], [".env.local", ".git"])
        self.assertEqual(validator.last_receipt["exit_code"], 0)

    def test_symlink_and_special_file_rejected_before_docker(self):
        for mode in ("file-link", "directory-link", "fifo"):
            with self.subTest(mode=mode):
                target = self.checkout / "untrusted"
                if mode == "file-link":
                    target.symlink_to(self.checkout / "module.py")
                elif mode == "directory-link":
                    target.symlink_to(self.checkout, target_is_directory=True)
                else:
                    os.mkfifo(target)
                with patch("gossip_harness.sandbox.subprocess.Popen") as started:
                    accepted, _ = self.validator()(self.checkout)
                self.assertFalse(accepted)
                started.assert_not_called()
                target.unlink()

    def test_timeout_kills_cli_and_forcibly_removes_the_unique_container(self):
        process = FakeProcess(timeout=True)
        with patch("gossip_harness.sandbox.subprocess.Popen", return_value=process), \
             patch("gossip_harness.sandbox.subprocess.run", return_value=completed()) as run:
            validator = self.validator(timeout_seconds=0.01)
            accepted, detail = validator(self.checkout)
        self.assertFalse(accepted)
        self.assertTrue(process.killed)
        self.assertIn("timeout", detail)
        self.assertTrue(validator.last_receipt["timed_out"])
        self.assertTrue(validator.last_receipt["cleanup_verified"])
        self.assertEqual(run.call_args.args[0], ["docker", "rm", "--force", validator.last_receipt["container_name"]])

    def test_output_is_drained_but_retained_receipt_is_bounded(self):
        process = FakeProcess(output=b"x" * 100_000, code=1)
        with patch("gossip_harness.sandbox.subprocess.Popen", return_value=process), \
             patch("gossip_harness.sandbox.subprocess.run", return_value=completed()):
            validator = self.validator()
            accepted, _ = validator(self.checkout)
        self.assertFalse(accepted)
        self.assertEqual(validator.last_receipt["exit_code"], 1)
        self.assertEqual(len(validator.last_receipt["output"]), 16_384)
        self.assertTrue(validator.last_receipt["output_truncated"])

    def test_cleanup_failure_fails_closed_and_absence_is_checked(self):
        with patch("gossip_harness.sandbox.subprocess.Popen", return_value=FakeProcess()), \
             patch("gossip_harness.sandbox.subprocess.run", side_effect=[completed(1), completed(1)]):
            validator = self.validator()
            accepted, detail = validator(self.checkout)
        self.assertFalse(accepted)
        self.assertIn("cleanup_failed", detail)
        with patch("gossip_harness.sandbox.subprocess.Popen", return_value=FakeProcess()), \
             patch("gossip_harness.sandbox.subprocess.run", side_effect=[completed(1), completed()]):
            accepted, _ = self.validator()(self.checkout)
        self.assertTrue(accepted)

    def test_errors_never_include_raw_exception_text(self):
        with patch("gossip_harness.sandbox.subprocess.Popen", side_effect=OSError("secret-sentinel")):
            validator = self.validator()
            accepted, detail = validator(self.checkout)
        self.assertFalse(accepted)
        self.assertNotIn("secret-sentinel", detail)
        self.assertEqual(validator.last_receipt["status"], "sandbox_error")

    def test_preflight_requires_exact_local_image(self):
        version = subprocess.CompletedProcess([], 0, "29.2.1\n", "")
        image = subprocess.CompletedProcess([], 0, IMAGE + "\n", "")
        with patch("gossip_harness.sandbox.subprocess.run", side_effect=[version, image]) as run:
            self.assertTrue(self.validator().preflight()[0])
            self.assertEqual(run.call_args.args[0], ["docker", "image", "inspect", "--format", "{{.Id}}", IMAGE])
        with patch("gossip_harness.sandbox.subprocess.run", side_effect=[version, completed(1)]):
            self.assertFalse(self.validator().preflight()[0])

    def test_check_hash_is_stable_and_changes_with_trusted_content(self):
        first = DockerValidator(IMAGE, {"a.py": "a", "b.py": "b"})
        same = DockerValidator(IMAGE, {"b.py": "b", "a.py": "a"})
        changed = DockerValidator(IMAGE, {"a.py": "A", "b.py": "b"})
        self.assertEqual(first.checks_sha256, same.checks_sha256)
        self.assertNotEqual(first.checks_sha256, changed.checks_sha256)


if __name__ == "__main__":
    unittest.main()
