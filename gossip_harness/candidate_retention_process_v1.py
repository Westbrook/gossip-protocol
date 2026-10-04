"""Retention-failure teardown around the frozen finite-process implementation.

The V4 wire/observation format is unchanged. This wrapper has a distinct owner
contract: any failed retention irrevocably forbids returning an observation.
All subsequent request retentions fail before dispatch. Only the final attach
capture callback is allowed to return without retention, so V4 can close its
socket after shutting down and joining the reader. No effect follows that
callback in the pinned V4 implementation. The owner separately cleans up known
candidate resources through its emergency channel; this module cannot heal a
journal or establish cleanup/acceptance authority.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from . import candidate_client_process_v4 as frozen

PROTOCOL = "candidate-retention-process-v1"
FROZEN_V4_SOURCE_SHA256 = "5fe7901bc0b79aca276e725bdca77de60c88466f804523740988982056c1b48b"


class _RetentionStopped(frozen.ProcessError):
    """Internal unwind accepted by the frozen process's cleanup handlers."""


def run_process(endpoint: frozen.EngineEndpoint, *, container_id: str,
                expected: dict[str, Any], policy: frozen.ProcessPolicy,
                retain: frozen.Retain, label: str, expected_runtime: dict[str, Any],
                expected_argv: list[str]) -> dict[str, Any]:
    """Run once, preserving the first retention exception after host teardown."""
    if hashlib.sha256(Path(frozen.__file__).read_bytes()).hexdigest() != FROZEN_V4_SOURCE_SHA256:
        raise frozen.ProcessError("Frozen process teardown ordering changed")
    first_failure: BaseException | None = None
    final_capture_name = label + "-attach-response.bin"

    def save(name: str, raw: bytes) -> None:
        nonlocal first_failure
        if first_failure is None:
            try:
                retain(name, raw)
                return
            except BaseException as error:
                first_failure = error
        # In the pinned source this exact callback occurs after shutdown/join
        # and immediately before wire.close(). No subsequent Engine effect is
        # possible. Discarding this untrusted capture does not issue a receipt.
        if name == final_capture_name:
            return
        raise _RetentionStopped("Retention failed; no further process effect is authorized") from first_failure

    try:
        return frozen.run_process(endpoint, container_id=container_id, expected=expected,
            policy=policy, retain=save, label=label, expected_runtime=expected_runtime,
            expected_argv=expected_argv)
    finally:
        # Covers both a normally returned diagnostic and an exception after
        # teardown. Neither may replace the original journal/anchor failure.
        if first_failure is not None:
            raise first_failure
