"""Bounded generated-probe exchange over an already-owned binary subprocess.

This module never starts a process or authenticates its source/runtime. The
physical owner must establish those facts before supplying the child, bind this
source and policy, and provide authenticated retention and effect boundaries.
Callbacks in isolation are not that authority. Local pipe teardown cannot prove
container or volume removal. No result here is a product acceptance judgment.
"""
from __future__ import annotations

from collections import deque
from dataclasses import asdict, dataclass
import hashlib
import os
from pathlib import Path
import selectors
import subprocess
import threading
import time
from typing import Any, Callable

from . import cumulative_generated_probe_values_v2 as values
from . import cumulative_generated_probe_wire_v1 as wire

PROTOCOL = 'cumulative-generated-probe-pipe-v1'
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
READ_BYTES = 65536
FRAME_QUEUE = 8
CLEANUP_SECONDS = 5


def require(ok: bool, message: str) -> None:
    if not ok:
        raise ValueError(message)


@dataclass(frozen=True, slots=True)
class PipePolicy:
    wire: wire.WireLimits
    stderr_bytes: int
    control_seconds: int

    def __post_init__(self) -> None:
        require(type(self.wire) is wire.WireLimits, 'exact_wire_limits_required')
        wire.WireLimits(**asdict(self.wire))
        require(type(self.stderr_bytes) is int and 0 < self.stderr_bytes <= 65536, 'bounded_stderr_required')
        require(type(self.control_seconds) is int and 0 < self.control_seconds <= 30, 'bounded_control_window_required')
        require(self.wire.frame_bytes <= 131072 and self.wire.stream_bytes <= 524288
                and self.wire.json_nodes <= 65536 and self.wire.json_depth <= 64, 'registered_wire_envelope_exceeded')


def definition(policy: PipePolicy) -> dict[str, Any]:
    require(type(policy) is PipePolicy, 'exact_pipe_policy_required')
    return {'protocol': PROTOCOL, 'policy': asdict(policy), 'read_bytes': READ_BYTES,
        'queued_frames': FRAME_QUEUE, 'cleanup_seconds': CLEANUP_SECONDS,
        'clock': 'absolute monotonic nanoseconds; each exchange step also has one control window',
        'io': 'POSIX nonblocking pipes; stdout and stderr drained together during reads and writes',
        'retention': 'raw frames, continuation intents/completions, bounded raw streams and local terminal',
        'prefetch': 'observed future or partial frame before acknowledgement refused; not proof of child-side causality',
        'authority': 'none; physical owner supplies source/runtime/admission/capture/retention proofs'}


class ProbePipe:
    """One exchange, owner-affine, no spawn or transparent retry.

    Ownership transfers after validated arguments, before file descriptors are
    configured. Initialization failure tears down the transferred local child.
    Call close() even if exchange() was never reached; close never sends input.
    """
    def __init__(self, child: subprocess.Popen[bytes], policy: PipePolicy, *, deadline_ns: int):
        require(isinstance(child, subprocess.Popen) and not child.universal_newlines, 'binary_owned_process_required')
        require(type(policy) is PipePolicy and type(deadline_ns) is int and 0 < deadline_ns <= 2**63-1,
                'exact_policy_and_absolute_deadline_required')
        require(all(getattr(child, name) is not None for name in ('stdin', 'stdout', 'stderr')),
                'three_owned_binary_pipes_required')
        self.child, self.policy, self.deadline_ns = child, policy, deadline_ns
        self._pid, self._thread = os.getpid(), threading.get_ident()
        self.closed, self.exchanged = False, False
        self.selector = selectors.DefaultSelector()
        self.streams = {'stdout': bytearray(), 'stderr': bytearray()}
        self.counts = {'stdout': 0, 'stderr': 0}
        self.errors: set[str] = set()
        self.eof: set[str] = set()
        self.frames: deque[bytes] = deque()
        self.pending = bytearray()
        self.discarding = False
        self.acks: list[dict[str, Any]] = []
        self.cleanup_errors: list[str] = []
        self.retention_failed = False
        self.natural_exit_seen = False
        self._written = 0
        try:
            for name in ('stdin', 'stdout', 'stderr'):
                pipe = getattr(child, name)
                os.set_blocking(pipe.fileno(), False)
                if name != 'stdin':
                    self.selector.register(pipe, selectors.EVENT_READ, name)
        except BaseException:
            self.close()
            raise

    def _owner(self) -> None:
        require(not self.closed and self._pid == os.getpid() and self._thread == threading.get_ident(),
                'pipe_closed_or_wrong_owner')

    def _remaining(self, deadline: int) -> float:
        remaining = min(deadline, self.deadline_ns) - time.monotonic_ns()
        if remaining <= 0:
            raise TimeoutError('probe_absolute_or_control_deadline')
        return remaining / 1_000_000_000

    def _step_deadline(self) -> int:
        self._remaining(self.deadline_ns)
        return min(self.deadline_ns, time.monotonic_ns() + self.policy.control_seconds * 1_000_000_000)

    def _consume(self, kind: str, raw: bytes) -> None:
        limit = self.policy.wire.stream_bytes if kind == 'stdout' else self.policy.stderr_bytes
        self.counts[kind] += len(raw)
        self.streams[kind].extend(raw[:max(0, limit-len(self.streams[kind]))])
        if self.counts[kind] > limit:
            self.errors.add(kind + '_limit')
        if kind != 'stdout':
            return
        parts = raw.split(b'\n')
        for index, part in enumerate(parts):
            complete = index < len(parts)-1
            if not self.discarding:
                size = len(self.pending) + len(part) + int(complete)
                if size > self.policy.wire.frame_bytes:
                    self.discarding = True
                    self.pending.clear()
                    self.errors.add('frame_limit')
                else:
                    self.pending.extend(part)
                    if complete:
                        self.pending.extend(b'\n')
            if complete:
                if not self.discarding:
                    if len(self.frames) < FRAME_QUEUE:
                        self.frames.append(bytes(self.pending))
                    else:
                        self.errors.add('frame_queue_limit')
                self.pending.clear()
                self.discarding = False

    def _pump(self, deadline: int) -> bool:
        """Drain each ready output once; bounded work before rechecking time."""
        events = self.selector.select(self._remaining(deadline))
        self._remaining(deadline)
        writable = False
        for key, _mask in events:
            self._remaining(deadline)
            if key.data == 'stdin':
                writable = True
                continue
            kind = key.data
            try:
                raw = os.read(key.fd, READ_BYTES)
            except BlockingIOError:
                continue
            if raw:
                self._consume(kind, raw)
            else:
                self.eof.add(kind)
                self.selector.unregister(key.fileobj)
                if kind == 'stdout' and (self.pending or self.discarding):
                    self.errors.add('truncated_frame')
        self._remaining(deadline)
        return writable

    def _line(self, deadline: int) -> bytes:
        while not self.frames:
            self._remaining(deadline)
            require('stdout' not in self.eof, 'probe_stdout_closed_before_expected_frame')
            require(not self.errors, 'probe_stream_integrity_unavailable')
            self._pump(deadline)
        self._remaining(deadline)
        return self.frames.popleft()

    def _write(self, raw: bytes, deadline: int) -> None:
        pipe = self.child.stdin
        assert pipe is not None
        self._written = 0
        self.selector.register(pipe, selectors.EVENT_WRITE, 'stdin')
        try:
            while self._written < len(raw):
                if not self._pump(deadline):
                    continue
                require(not self.errors, 'probe_stream_integrity_unavailable')
                require(not self.frames and not self.pending, 'probe_unacknowledged_future_output')
                self._remaining(deadline)
                try:
                    count = os.write(pipe.fileno(), raw[self._written:])
                except BlockingIOError:
                    continue
                require(count > 0, 'probe_stdin_closed')
                self._written += count
            self._remaining(deadline)
        finally:
            self.selector.unregister(pipe)

    def _finish(self, deadline: int) -> bool:
        assert self.child.stdin is not None
        self.child.stdin.close()
        while self.eof != {'stdout', 'stderr'}:
            self._pump(deadline)
        self.child.wait(timeout=self._remaining(deadline))
        self._remaining(deadline)
        self.natural_exit_seen = True
        require(not self.frames and not self.pending and not self.errors, 'probe_extra_or_invalid_output')
        require(self.child.returncode == 0, 'probe_nonzero_exit')
        return True

    def exchange(self, admitted: dict[str, Any], *, released_requirements: tuple[str, ...],
                 before_effect: Callable[[], None], retain: Callable[[str, bytes], None],
                 capture_job: Callable[[], Any] | None = None) -> dict[str, Any]:
        """Boundary callbacks are obligations of the external physical owner.

        A returned value verdict remains separate from stream/process mechanics.
        Known bad values survive later timeout or framing failure; missing values
        never become invented observations. There is no dispatch/acceptance flag.
        """
        self._owner()
        require(not self.exchanged, 'probe_pipe_exchange_is_one_shot')
        self.exchanged = True
        try:
            require(callable(before_effect) and callable(retain), 'owner_boundaries_required')
            transcript = wire.ValueTranscript(admitted, released_requirements=released_requirements, limits=self.policy.wire)
        except BaseException:
            self.close()
            raise
        infrastructure: list[str] = []
        natural = False
        primary: BaseException | None = None

        def keep(name: str, raw: bytes) -> None:
            try:
                retain(name, raw)
            except BaseException:
                self.retention_failed = True
                raise

        def boundary(deadline: int) -> None:
            self._remaining(deadline)
            before_effect()
            self._remaining(deadline)

        try:
            while (slot := transcript.next_slot) is not None:
                deadline = self._step_deadline()
                boundary(deadline)
                raw = self._line(deadline)
                keep('probe-frame-' + slot + '.bin', raw)
                self._remaining(deadline)
                event = transcript.append(raw)
                boundary(deadline)
                require(not self.errors, 'probe_stream_integrity_unavailable')
                if slot == 'capture_job':
                    require(callable(capture_job), 'reviewed_capture_callback_required')
                    assert capture_job is not None
                    captured = capture_job()
                    boundary(deadline)
                    transcript.supply_captured_value(captured)
                require(not self.frames and not self.pending, 'probe_unacknowledged_future_output')
                ack = wire.continuation_bytes(slot)
                keep('probe-continue-' + slot + '-intent.bin', ack)
                boundary(deadline)
                self.acks.append({'slot': slot, 'requested_bytes': len(ack), 'written_bytes': 0})
                try:
                    self._write(ack, deadline)
                finally:
                    self.acks[-1]['written_bytes'] = self._written
                keep('probe-continue-' + slot + '-written.json', values.canonical(self.acks[-1]))
                boundary(deadline)
            natural = self._finish(self._step_deadline())
            boundary(self.deadline_ns)
        except BaseException as error:
            primary = error
            infrastructure.append(type(error).__name__ + ':' + str(error)[:300])
        finally:
            self.close()
        infrastructure.extend(sorted(self.errors))
        infrastructure.extend(self.cleanup_errors)
        result = {'protocol': PROTOCOL, 'value': transcript.result(), 'natural_exit': self.natural_exit_seen,
            'mechanics_complete': natural and not infrastructure and transcript.result()['wire_complete'],
            'exit_code': self.child.returncode, 'local_pipes_closed': all(getattr(self.child, name).closed for name in ('stdin', 'stdout', 'stderr')),
            'infrastructure': infrastructure, 'acks': self.acks,
            'execution_authority': False, 'acceptance_authority': False,
            'container_cleanup_proven': False, 'runtime_identity_proven': False,
            'streams': {kind: {'bytes': len(raw), 'observed_bytes': self.counts[kind],
                'sha256': hashlib.sha256(raw).hexdigest(), 'truncated': self.counts[kind] != len(raw)}
                for kind, raw in self.streams.items()}}
        if not self.retention_failed:
            try:
                for kind, raw in self.streams.items():
                    keep('probe-' + kind + '.bin', bytes(raw))
                keep('probe-pipe-terminal.json', values.canonical(result))
            except BaseException as error:
                if primary is None:
                    primary = error
        if primary is not None and (self.retention_failed or not isinstance(primary, Exception)):
            raise primary
        return result

    def close(self) -> None:
        if self.closed:
            return
        require(self._pid == os.getpid() and self._thread == threading.get_ident(), 'wrong_pipe_close_owner')
        try:
            if self.child.poll() is None:
                self.child.kill()
            self.child.wait(timeout=CLEANUP_SECONDS)
        except BaseException as error:
            self.cleanup_errors.append('local_reap:' + type(error).__name__)
        finally:
            self.selector.close()
            for name in ('stdin', 'stdout', 'stderr'):
                pipe = getattr(self.child, name)
                if pipe is not None:
                    try:
                        pipe.close()
                    except OSError as error:
                        self.cleanup_errors.append('local_pipe_close:' + type(error).__name__)
            self.closed = True

    def __enter__(self) -> ProbePipe:
        self._owner()
        return self

    def __exit__(self, *_args: Any) -> None:
        self.close()
