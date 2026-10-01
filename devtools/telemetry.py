"""Low-overhead, explicitly scoped verification measurements.

These measurements describe the observing host process, never Docker daemon
work or a sampled process tree. Counters are opt-in at I/O and lock call sites;
they describe logical payload bytes, not physical disk traffic. Do not sum
process-lifetime RSS high-water marks or add parent/child CPU to worker CPU:
those scopes overlap. Concurrent threads share one process meter.
"""
from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
import math
from pathlib import Path
import statistics
import sys
import threading
import time
from typing import Any, Protocol


@dataclass(frozen=True)
class _Usage:
    user_seconds: float
    system_seconds: float
    max_rss: float


def _usage() -> tuple[_Usage, _Usage] | None:
    try:
        import resource
        own = resource.getrusage(resource.RUSAGE_SELF)
        child = resource.getrusage(resource.RUSAGE_CHILDREN)
        return (_Usage(own.ru_utime, own.ru_stime, own.ru_maxrss),
                _Usage(child.ru_utime, child.ru_stime, child.ru_maxrss))
    except (ImportError, AttributeError, OSError):
        return None


def _unavailable(reason: str, unit: str, scope: str) -> dict[str, Any]:
    return dict(status="unavailable", value=None, unit=unit, scope=scope, reason=reason)


def _measurement(value: float | int, unit: str, scope: str) -> dict[str, Any]:
    return dict(status="measured", value=value, unit=unit, scope=scope)


class ProcessTelemetry:
    """Measure CPU deltas and lifetime RSS with portable Unix resource APIs.

    Construct once per isolated worker or controller. ``snapshot`` is cumulative
    since construction and does not reset the meter. RUSAGE_CHILDREN includes
    only terminated children accounted for by the OS, excludes live children,
    and can overlap child workers' own CPU. RSS is a lifetime high-water mark,
    not the maximum attributable to this particular interval.
    """

    def __init__(self, scope: str) -> None:
        if not scope.strip():
            raise ValueError("A measurement scope is required")
        self.scope = scope
        self._started = time.monotonic()
        self._initial = _usage()

    def snapshot(self) -> dict[str, Any]:
        wall = time.monotonic() - self._started
        result: dict[str, Any] = {
            "schema": "verification-process-telemetry-v1", "scope": self.scope,
            "wall_seconds": _measurement(wall, "seconds", "observer interval"),
            "peak_cpu_cores": _unavailable("No CPU sampling; an interval average is not a peak",
                                           "cores", "observer process"),
            "process_tree_peak_rss_bytes": _unavailable(
                "No simultaneous process-tree sampling; individual RSS peaks are not additive",
                "bytes", "observer process tree"),
        }
        current = _usage()
        if self._initial is None or current is None:
            for name, unit, scope in (
                ("self_cpu_seconds", "seconds", "observer process during interval"),
                ("waited_children_cpu_seconds", "seconds", "OS-accounted waited children during interval"),
                ("self_average_cpu_cores", "cores", "observer process CPU divided by interval wall time"),
                ("self_lifetime_peak_rss_bytes", "bytes", "observer process lifetime"),
            ):
                result[name] = _unavailable("Unix getrusage unavailable", unit, scope)
            return result
        for name, before, after, scope in (
            ("self_cpu_seconds", self._initial[0], current[0], "observer process during interval"),
            ("waited_children_cpu_seconds", self._initial[1], current[1],
             "OS-accounted waited children during interval; excludes live children and remote Docker work"),
        ):
            user = after.user_seconds - before.user_seconds
            system = after.system_seconds - before.system_seconds
            if min(user, system) < 0:
                result[name] = _unavailable("Resource counters decreased", "seconds", scope)
            else:
                result[name] = {**_measurement(user + system, "seconds", scope),
                                "user_seconds": user, "system_seconds": system}
        own_cpu = result["self_cpu_seconds"]
        average_scope = "observer process CPU divided by interval wall time; excludes child CPU"
        result["self_average_cpu_cores"] = (
            _measurement(own_cpu["value"] / wall, "cores", average_scope)
            if wall > 0 and own_cpu["status"] == "measured" else
            _unavailable("No valid positive wall/CPU interval", "cores", average_scope))
        rss = current[0].max_rss
        multiplier = 1 if sys.platform == "darwin" else 1024 if sys.platform.startswith("linux") else None
        result["self_lifetime_peak_rss_bytes"] = (
            _measurement(int(rss * multiplier), "bytes", "observer process lifetime; not an interval delta")
            if multiplier is not None and math.isfinite(rss) and rss >= 0 else
            _unavailable("Unrecognized platform RSS units or invalid reading", "bytes", "observer process lifetime"))
        return result


class Lock(Protocol):
    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool: ...
    def release(self) -> None: ...


class IOAccounting:
    """Thread-safe counters for named, explicitly instrumented operations only.

    Repeated reads count repeatedly, even when the OS serves its page cache.
    Reused in-memory bytes must not call ``record_read`` again. Staged bytes mean
    successful payload writes, not directory metadata, allocation, or fsync.
    Counter snapshots are cumulative and separate from process resource meters.
    """

    def __init__(self, scope: str) -> None:
        if not scope.strip():
            raise ValueError("An accounting scope is required")
        self.scope = scope
        self._guard = threading.Lock()
        self._bytes: dict[str, dict[str, dict[str, int]]] = {"read": {}, "staged": {}}
        self._locks: dict[str, dict[str, float | int]] = {}

    def _record_bytes(self, kind: str, count: int, channel: str) -> None:
        if type(count) is not int or count < 0 or not channel.strip():
            raise ValueError("Byte count must be a nonnegative integer with a channel")
        with self._guard:
            counter = self._bytes[kind].setdefault(channel, {"bytes": 0, "operations": 0})
            counter["bytes"] += count
            counter["operations"] += 1

    def record_read(self, count: int, channel: str = "files") -> None:
        self._record_bytes("read", count, channel)

    def record_staged(self, count: int, channel: str = "files") -> None:
        self._record_bytes("staged", count, channel)

    def read_bytes(self, path: Path, channel: str = "files") -> bytes:
        contents = path.read_bytes()
        self.record_read(len(contents), channel)
        return contents

    def write_bytes(self, path: Path, contents: bytes, channel: str = "files") -> int:
        written = path.write_bytes(contents)
        self.record_staged(written, channel)
        return written

    @contextmanager
    def acquire(self, lock: Lock, label: str, *, blocking: bool = True,
                timeout: float = -1) -> Iterator[bool]:
        """Measure only acquire latency; release an acquired lock on every exit.

        The yielded boolean preserves nonblocking/timeout semantics. Failed
        attempts and acquire exceptions count as failed acquisitions. Critical
        section duration is deliberately excluded from lock wait.
        """
        if not label.strip():
            raise ValueError("A lock label is required")
        began = time.monotonic()
        acquired = False
        try:
            try:
                acquired = lock.acquire(blocking, timeout)
            finally:
                elapsed = time.monotonic() - began
                with self._guard:
                    counter = self._locks.setdefault(label, {"wait_seconds": 0.0, "attempts": 0,
                                                            "acquired": 0, "failed": 0})
                    counter["wait_seconds"] += elapsed
                    counter["attempts"] += 1
                    counter["acquired" if acquired else "failed"] += 1
            yield acquired
        finally:
            if acquired:
                lock.release()

    def snapshot(self) -> dict[str, Any]:
        with self._guard:
            result: dict[str, Any] = {"schema": "verification-io-telemetry-v1", "scope": self.scope}
            for kind, channels in self._bytes.items():
                scope = "instrumented logical payload operations; excludes uninstrumented and physical disk I/O"
                value = sum(channel["bytes"] for channel in channels.values())
                result[kind + "_bytes"] = (
                    {**_measurement(value, "bytes", scope),
                     "channels": {key: dict(counter) for key, counter in sorted(channels.items())}}
                    if channels else _unavailable("No operations instrumented", "bytes", scope))
            scope = "sum of instrumented acquire latencies; concurrent waits may overlap in wall time"
            result["lock_wait_seconds"] = (
                {**_measurement(sum(counter["wait_seconds"] for counter in self._locks.values()), "seconds", scope),
                 "locks": {key: dict(counter) for key, counter in sorted(self._locks.items())}}
                if self._locks else _unavailable("No lock acquisitions instrumented", "seconds", scope))
            return result


def distribution(samples: Sequence[float]) -> dict[str, Any]:
    """Describe same-workload samples without inventing unsupported percentiles.

    p50 requires three observations; nearest-rank p95 requires at least twenty.
    These are descriptive sample statistics, not confidence intervals or a
    performance guarantee. The caller must bind identical workload/hardware.
    """
    if any(isinstance(value, bool) or not math.isfinite(value) or value < 0 for value in samples):
        raise ValueError("Timing samples must be finite nonnegative numbers")
    ordered = sorted(samples)
    scope = "provided same-workload observations; descriptive only, no confidence claim"
    result: dict[str, Any] = {"count": len(ordered), "unit": "seconds", "scope": scope,
                              "samples_seconds": list(samples)}
    for name, minimum in (("p50", 3), ("p95", 20)):
        if len(ordered) < minimum:
            result[name] = _unavailable(f"Requires at least {minimum} same-workload observations", "seconds", scope)
        else:
            value = statistics.median(ordered) if name == "p50" else ordered[math.ceil(0.95 * len(ordered)) - 1]
            result[name] = {**_measurement(value, "seconds", scope),
                            "method": "median" if name == "p50" else "nearest-rank"}
    return result
