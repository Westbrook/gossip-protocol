"""Owned, collision-safe workspaces for expensive unittest evidence.

The verification runner sets GOSSIP_TEST_ARTIFACTS to a session-owned shard
directory. Direct unittest invocations keep failures in the system temporary
directory and print their location. Existing experiment-specific output variables
remain exact output paths; they must be fresh and are never cleaned here.
"""

from __future__ import annotations

from collections.abc import Mapping
import json
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile
import traceback
from types import TracebackType


class ArtifactDirectory:
    """Keep expensive evidence and failed contexts; clean only owned successes.

    ``root`` exists immediately, while ``output`` is a fresh path for experiment
    runners that require a nonexistent destination. For class-scoped fixtures,
    register ``close`` with ``addClassCleanup`` immediately after construction.
    Without a known outcome, close conservatively retains the workspace.
    """

    def __init__(
        self,
        label: str,
        *,
        output_env: str | None = None,
        retain_success: bool = False,
        environ: Mapping[str, str] | None = None,
        temporary_root: str | Path | None = None,
    ) -> None:
        environment = os.environ if environ is None else environ
        self.label = label
        self.retain_success = retain_success
        self.closed = False
        self.requested_root = environment.get("GOSSIP_TEST_ARTIFACTS")
        self.output_env = output_env
        requested_output = environment.get(output_env) if output_env else None
        self.explicit_output = bool(requested_output)
        output = Path(requested_output).expanduser().absolute() if requested_output else None
        # Refuse before allocating or writing anything. In particular, do not
        # resolve a symlink and accidentally treat its missing target as fresh.
        if output is not None and (output.exists() or output.is_symlink()):
            raise FileExistsError(f"Test output must be fresh: {output}")
        base = Path(self.requested_root).expanduser().absolute() if self.requested_root else temporary_root
        if base is not None:
            Path(base).mkdir(parents=True, exist_ok=True)
        safe_label = re.sub(r"[^A-Za-z0-9_.-]+", "-", label).strip(".-") or "test"
        self.root = Path(tempfile.mkdtemp(prefix=f"gossip-{safe_label[:80]}-", dir=base))
        self.output = output if output is not None else self.root / "run"
        self._write_manifest("running")
        # A killed process may never execute cleanup. Announce the owned path
        # before expensive execution as well as when retaining completed work.
        print(f"Test artifacts ({self.label}): {self.root}; output: {self.output}", file=sys.stderr, flush=True)

    def _write_manifest(self, retention: str) -> None:
        (self.root / "artifacts.json").write_text(json.dumps({
            "schema": "gossip-test-artifacts-v1",
            "label": self.label,
            "output": str(self.output),
            "retention": retention,
            "explicit_output_environment": self.output_env if self.explicit_output else None,
        }, indent=2) + "\n")

    def close(self, *, failed: bool | None = None) -> None:
        """Finish once; an unknown class-fixture outcome is retained safely."""
        if self.closed:
            return
        if failed:
            reason = "failure"
        elif self.requested_root or self.explicit_output:
            reason = "requested"
        elif self.retain_success:
            reason = "expensive-fixture"
        elif failed is None:
            reason = "unknown-outcome"
        else:
            reason = "clean-success"
        if reason == "clean-success":
            # root is exclusively ours. Never remove a caller's requested output,
            # the parent directory, or artifacts from another test invocation.
            shutil.rmtree(self.root)
        else:
            self._write_manifest(reason)
            print(f"Retained test artifacts ({reason}): {self.root}; output: {self.output}",
                  file=sys.stderr, flush=True)
        self.closed = True

    def __enter__(self) -> ArtifactDirectory:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if exc is not None:
            (self.root / "failure.txt").write_text("".join(traceback.format_exception(exc_type, exc, tb)))
        self.close(failed=exc_type is not None)
