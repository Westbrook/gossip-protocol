"""Pinned-image Docker checks for untrusted candidate code.

Docker is a prerequisite, never installed or started here. The caller approves and
pulls a trusted image separately, then supplies its full local image ID. This is
a container boundary for a local pilot, not a claim of VM-grade isolation.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import stat
import subprocess
import tempfile
import threading
import uuid


_IMAGE_ID = re.compile(r"sha256:[0-9a-f]{64}\Z")
_OUTPUT_LIMIT = 16_384
_COPY_LIMIT = 128 * 1024 * 1024
_FILE_LIMIT = 10_000
_EXCLUDED = {".git", ".aws", ".ssh", ".docker", ".codex", ".config"}


class DockerValidator:
    """Validate one immutable checkout using caller-owned, separate test files.

    ``preflight()`` and calls return ``(passed, detail)``. The last call's receipt
    is JSON serializable. Tests, image selection, and the command are trusted
    harness inputs; candidate files are untrusted. Concurrent calls should use
    separate validator instances because ``last_receipt`` describes one call.
    """

    def __init__(self, image: str, tests: dict[str, str],
                 command: tuple[str, ...] = ("python", "-I", "/checks/run_checks.py"),
                 timeout_seconds: float = 30):
        if not isinstance(image, str) or not _IMAGE_ID.fullmatch(image):
            raise ValueError("A full local sha256 image ID is required")
        if not tests or not isinstance(tests, dict):
            raise ValueError("Trusted check files are required")
        for name, content in tests.items():
            if (not isinstance(name, str) or not name or "\x00" in name
                    or "\\" in name or PurePosixPath(name).is_absolute()
                    or any(part in {"", ".", ".."} for part in name.split("/"))
                    or not isinstance(content, str)):
                raise ValueError("Check files must have safe relative paths and text content")
        if (not isinstance(command, tuple) or not command
                or any(not isinstance(part, str) or not part or "\x00" in part
                       for part in command)):
            raise ValueError("Command must be a nonempty tuple of arguments")
        if (isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float))
                or not math.isfinite(timeout_seconds) or timeout_seconds <= 0):
            raise ValueError("Timeout must be finite and positive")
        self.image = image
        self.tests = dict(tests)
        self.command = command
        self.timeout_seconds = timeout_seconds
        self.checks_sha256 = hashlib.sha256(json.dumps(
            self.tests, sort_keys=True, ensure_ascii=True, separators=(",", ":")
        ).encode()).hexdigest()
        self.last_receipt: dict = {}

    @staticmethod
    def _environment() -> dict[str, str]:
        # Docker CLI needs its selected local context. None of these variables
        # is passed into the container, and application/API variables are absent.
        names = ("PATH", "HOME", "DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_CONFIG",
                 "XDG_RUNTIME_DIR")
        return {key: os.environ[key] for key in names if key in os.environ}

    def preflight(self) -> tuple[bool, str]:
        """Check the daemon and exact image without registry/network requests."""
        try:
            version = subprocess.run(
                ["docker", "version", "--format", "{{.Server.Version}}"],
                capture_output=True, text=True, timeout=15, env=self._environment(),
                check=False,
            )
            if version.returncode or not version.stdout.strip():
                return False, "Docker daemon is unavailable"
            inspected = subprocess.run(
                ["docker", "image", "inspect", "--format", "{{.Id}}", self.image],
                capture_output=True, text=True, timeout=15, env=self._environment(),
                check=False,
            )
            if inspected.returncode or inspected.stdout.strip() != self.image:
                return False, "The pinned image is not available locally"
            return True, "Docker and the pinned local image are available"
        except (OSError, subprocess.SubprocessError):
            return False, "Docker preflight could not complete"

    @staticmethod
    def _copy_checkout(checkout: Path, target: Path) -> dict:
        if checkout.is_symlink() or not checkout.is_dir():
            raise ValueError("Candidate must be a directory, not a symlink")
        target.mkdir(mode=0o755)
        count, total = 0, 0
        excluded = []
        for parent, directories, filenames in os.walk(checkout, followlinks=False):
            relative = Path(parent).relative_to(checkout)
            for name in list(directories):
                if name in _EXCLUDED or name == ".env" or name.startswith(".env."):
                    directories.remove(name)
                    excluded.append((relative / name).as_posix())
                    continue
                source = Path(parent) / name
                if source.is_symlink():
                    raise ValueError("Candidate symlinks are not supported")
                (target / relative / name).mkdir(mode=0o755)
            for name in filenames:
                if name in _EXCLUDED or name == ".env" or name.startswith(".env."):
                    excluded.append((relative / name).as_posix())
                    continue
                source = Path(parent) / name
                if not stat.S_ISREG(source.lstat().st_mode):
                    raise ValueError("Candidate symlinks and special files are not supported")
                count += 1
                if count > _FILE_LIMIT:
                    raise ValueError("Candidate exceeds the staging file limit")
                destination = target / relative / name
                # O_NOFOLLOW also rejects a file replaced by a symlink after lstat.
                descriptor = os.open(source, os.O_RDONLY | os.O_NOFOLLOW)
                with os.fdopen(descriptor, "rb") as incoming, destination.open("wb") as outgoing:
                    metadata = os.fstat(incoming.fileno())
                    if not stat.S_ISREG(metadata.st_mode):
                        raise ValueError("Candidate contains a special file")
                    while chunk := incoming.read(65_536):
                        total += len(chunk)
                        if total > _COPY_LIMIT:
                            raise ValueError("Candidate exceeds the staging byte limit")
                        outgoing.write(chunk)
                destination.chmod(0o555 if metadata.st_mode & 0o111 else 0o444)
        return {"files": count, "bytes": total, "excluded_paths": sorted(excluded)}

    def _arguments(self, name: str, workspace: Path, checks: Path) -> list[str]:
        # --mount is comma-delimited; reject rather than reinterpret odd paths.
        if any("," in str(path) for path in (workspace, checks)):
            raise ValueError("Staging paths cannot contain commas")
        return [
            "docker", "run", "--name", name, "--rm", "--pull=never", "--init",
            "--network=none", "--read-only", "--cap-drop=ALL",
            "--security-opt=no-new-privileges:true", "--user=65534:65534",
            "--pids-limit=64", "--memory=256m", "--memory-swap=256m", "--cpus=1",
            "--ulimit=nofile=256:256", "--no-healthcheck", "--restart=no",
            "--log-driver=none", "--hostname=gossip-validator",
            "--tmpfs=/tmp:rw,nosuid,nodev,noexec,size=32m,mode=1777",
            "--env=HOME=/tmp", "--env=PYTHONDONTWRITEBYTECODE=1",
            "--env=PYTHONNOUSERSITE=1", "--workdir=/workspace",
            # Docker config may automatically inject credential-bearing proxies.
            # Explicit empty values override that behavior in both common cases.
            *[f"--env={key}=" for key in (
                "HTTP_PROXY", "HTTPS_PROXY", "FTP_PROXY", "NO_PROXY", "ALL_PROXY",
                "http_proxy", "https_proxy", "ftp_proxy", "no_proxy", "all_proxy")],
            "--mount", f"type=bind,source={workspace},target=/workspace,readonly,bind-propagation=rprivate",
            "--mount", f"type=bind,source={checks},target=/checks,readonly,bind-propagation=rprivate",
            "--entrypoint", self.command[0], self.image, *self.command[1:],
        ]

    def _remove(self, name: str) -> bool:
        try:
            removed = subprocess.run(
                ["docker", "rm", "--force", name], capture_output=True, timeout=10,
                env=self._environment(), check=False,
            )
            if removed.returncode == 0:
                return True
            # --rm may already have removed it. Prove absence rather than relying
            # on localized stderr. A failed inspect does not prove daemon health.
            listed = subprocess.run(
                ["docker", "container", "ls", "--all", "--quiet", "--filter", f"name=^/{name}$"],
                capture_output=True, timeout=10, env=self._environment(), check=False,
            )
            return listed.returncode == 0 and not listed.stdout.strip()
        except (OSError, subprocess.SubprocessError):
            return False

    def __call__(self, checkout: Path) -> tuple[bool, str]:
        name = "gossip-check-" + uuid.uuid4().hex
        receipt = {
            "schema_version": 1, "status": "starting", "container_name": name,
            "image_id": self.image, "command": list(self.command),
            "checks_sha256": self.checks_sha256, "exit_code": None,
            "timeout_seconds": self.timeout_seconds, "timed_out": False,
            "cleanup_verified": None, "output": "", "output_truncated": False,
        }
        self.last_receipt = receipt
        process = None
        output = bytearray()
        output_count = [0]
        reader = None
        try:
            with tempfile.TemporaryDirectory(prefix="gossip-validator-") as temporary:
                staging = Path(temporary).resolve()
                workspace, checks = staging / "workspace", staging / "checks"
                receipt["staging"] = self._copy_checkout(Path(checkout), workspace)
                checks.mkdir(mode=0o755)
                for filename, content in self.tests.items():
                    file = checks / filename
                    file.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
                    file.write_text(content, encoding="utf-8")
                    file.chmod(0o444)
                arguments = self._arguments(name, workspace, checks)
                process = subprocess.Popen(
                    arguments, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    stdin=subprocess.DEVNULL, env=self._environment(), cwd=staging,
                )

                def drain():
                    while chunk := process.stdout.read(8192):
                        output_count[0] += len(chunk)
                        remaining = _OUTPUT_LIMIT - len(output)
                        if remaining > 0:
                            output.extend(chunk[:remaining])

                reader = threading.Thread(target=drain, daemon=True)
                reader.start()
                try:
                    receipt["exit_code"] = process.wait(timeout=self.timeout_seconds)
                    receipt["status"] = "passed" if process.returncode == 0 else "failed"
                except subprocess.TimeoutExpired:
                    receipt["timed_out"] = True
                    receipt["status"] = "timeout"
                    process.kill()
                    process.wait(timeout=5)
                finally:
                    receipt["cleanup_verified"] = self._remove(name)
                    reader.join(timeout=5)
                    if reader.is_alive():
                        receipt["status"] = "output_error"
                    else:
                        process.stdout.close()
        except ValueError as error:
            # Only our fixed validation messages are included, never raw OS errors.
            receipt["status"] = "invalid_checkout"
            receipt["detail"] = str(error)
        except (OSError, subprocess.SubprocessError):
            receipt["status"] = "sandbox_error"
            receipt["detail"] = "Docker validation could not complete"
            if process is not None:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)
                receipt["cleanup_verified"] = self._remove(name)
        receipt["output"] = output.decode("utf-8", errors="replace")
        receipt["output_truncated"] = output_count[0] > _OUTPUT_LIMIT
        if process is not None and not receipt["cleanup_verified"]:
            receipt["status"] = "cleanup_failed"
        passed = receipt["status"] == "passed"
        detail = f"Docker validation {receipt['status']}"
        if receipt.get("detail"):
            detail += ": " + receipt["detail"]
        if receipt["output"]:
            detail += "\n" + receipt["output"]
        return passed, detail
