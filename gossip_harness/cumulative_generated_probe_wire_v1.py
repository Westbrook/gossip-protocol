"""Closed generated-probe messages and transcript values, without IO authority.

The execution owner must retain raw bytes and independently verify source,
runtime, deadlines and captures before using these helpers. Neither a parsed
frame nor continuation bytes authenticate a candidate or authorize dispatch.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any

from . import cumulative_generated_probe_values_v2 as values

PROTOCOL = 'cumulative-generated-probe-wire-v1'
DATABASE_PATH = '/tmp/m2/library.sqlite'
_SLOTS = {
    'refresh-identity-v1': ('imported', 'before', 'refresh', 'finished'),
    'refresh-noop-v1': ('imported', 'before', 'refresh', 'finished'),
    'completed-receipt-replay-v1': ('submitted', 'prepared', 'original_receipt', 'before', 'refresh', 'replay', 'finished'),
    'manifest-content-hash-v1': ('submitted', 'capture_job', 'finished'),
}


@dataclass(frozen=True, slots=True)
class WireLimits:
    """Caller-frozen numeric quotas, not new study allowances or deadlines."""
    frame_bytes: int
    stream_bytes: int
    json_nodes: int
    json_depth: int

    def __post_init__(self) -> None:
        if any(type(n) is not int or n < 1 for n in
               (self.frame_bytes, self.stream_bytes, self.json_nodes, self.json_depth)):
            raise ValueError('positive_exact_wire_limits_required')
        if self.frame_bytes > self.stream_bytes:
            raise ValueError('frame_exceeds_stream_limit')


def checked_probe(admitted: dict[str, Any], *, released_requirements: tuple[str, ...]) -> dict[str, Any]:
    """Reconstruct pure admission; this is not source or observation review."""
    if type(admitted) is not dict:
        raise ValueError('admitted_probe_required')
    keys = ('template_id', 'requirement_id', 'parameters', 'expectation')
    if not all(key in admitted for key in keys):
        raise ValueError('admitted_probe_required')
    expected = values.admit({key: admitted[key] for key in keys},
        released_requirements=released_requirements, contract_sha256=values.PRODUCT_SHA256)
    if not values.exact(admitted, expected):
        raise ValueError('admitted_probe_identity_differs')
    return expected


def request_bytes(admitted: dict[str, Any], *, released_requirements: tuple[str, ...]) -> bytes:
    probe = checked_probe(admitted, released_requirements=released_requirements)
    return values.canonical({'protocol': PROTOCOL, 'template_id': probe['template_id'],
                             'parameters': probe['parameters']})


def decode_frame(raw: bytes, *, expected_slot: str, limits: WireLimits) -> dict[str, Any]:
    if type(limits) is not WireLimits:
        raise ValueError('exact_wire_limits_required')
    if expected_slot not in {slot for slots in _SLOTS.values() for slot in slots}:
        raise ValueError('unknown_slot')
    if (type(raw) is not bytes or not raw.endswith(b'\n') or raw.count(b'\n') != 1
            or len(raw) > limits.frame_bytes):
        raise ValueError('invalid_bounded_frame')
    try:
        event = json.loads(raw.decode('utf-8'), object_pairs_hook=values._pairs,
                           parse_constant=values._constant)
        values._bounded(event, max_nodes=limits.json_nodes, max_depth=limits.json_depth)
    except (UnicodeError, RecursionError) as error:
        raise ValueError('invalid_frame_json') from error
    if (type(event) is not dict or set(event) != {'protocol', 'slot', 'value'}
            or event['protocol'] != PROTOCOL or event['slot'] != expected_slot):
        raise ValueError('frame_identity_differs')
    if expected_slot == 'capture_job' and not values.exact(event['value'],
            {'job_id': values.JOB, 'database': DATABASE_PATH}):
        raise ValueError('capture_boundary_differs')
    if expected_slot == 'finished' and not values.exact(event['value'], {'closed': True}):
        raise ValueError('finish_boundary_differs')
    return event


def continuation_bytes(slot: str) -> bytes:
    """Wire formatting only; caller owes durable admission before writing it."""
    if slot not in {item for slots in _SLOTS.values() for item in slots}:
        raise ValueError('unknown_slot')
    return ('continue:' + slot + '\n').encode('ascii')


class ValueTranscript:
    """Ordered finite value transcript, not an execution or evidence receipt.

    A capture marker is kept separate from a supplied capture value. The future
    owner/cold reader must establish that value's origin from real SQLite bytes.
    The final closed marker still needs actual exit and owned cleanup outside
    this class. No method performs IO or issues an acceptance judgment.
    """
    def __init__(self, admitted: dict[str, Any], *, released_requirements: tuple[str, ...],
                 limits: WireLimits) -> None:
        self._probe = checked_probe(admitted, released_requirements=released_requirements)
        if type(limits) is not WireLimits:
            raise ValueError('exact_wire_limits_required')
        self._limits = limits
        self._slots = _SLOTS[self._probe['template_id']]
        self._next = 0
        self._bytes = 0
        self._values: dict[str, Any] = {}
        self._invalid = False
        self._capture_seen = False

    @property
    def next_slot(self) -> str | None:
        return None if self._invalid or self._next == len(self._slots) else self._slots[self._next]

    def append(self, raw: bytes) -> dict[str, Any]:
        try:
            slot = self.next_slot
            if slot is None:
                raise ValueError('transcript_closed_or_invalid')
            if type(raw) is not bytes or self._bytes + len(raw) > self._limits.stream_bytes:
                raise ValueError('stream_limit')
            event = decode_frame(raw, expected_slot=slot, limits=self._limits)
            if slot == 'finished' and self._capture_seen and 'captured_job' not in self._values:
                raise ValueError('capture_value_missing_before_continuation')
            self._bytes += len(raw)
            self._next += 1
            if slot == 'capture_job':
                self._capture_seen = True
            elif slot != 'finished':
                # Do not expose the mutable dictionary owned by this transcript.
                self._values[slot] = json.loads(values.canonical(event['value']))
            return event
        except ValueError:
            self._invalid = True
            raise

    def supply_captured_value(self, value: Any) -> None:
        """Untrusted values only; an external owner must authenticate originals."""
        if (self._invalid or not self._capture_seen or 'captured_job' in self._values
                or self.next_slot != 'finished'):
            self._invalid = True
            raise ValueError('capture_value_out_of_order')
        try:
            raw = values.canonical(value)
            if len(raw) > self._limits.frame_bytes:
                raise ValueError('capture_value_limit')
            values._bounded(value, max_nodes=self._limits.json_nodes, max_depth=self._limits.json_depth)
            self._values['captured_job'] = json.loads(raw)
        except (ValueError, TypeError, RecursionError) as error:
            self._invalid = True
            raise ValueError('invalid_capture_value') from error

    def result(self) -> dict[str, Any]:
        result = values.evaluate_values(self._probe, self._values)
        if result['disposition'] != 'fail' and (self._invalid or self._next != len(self._slots)):
            result = {**result, 'disposition': 'unavailable', 'reason': 'incomplete_or_invalid_transcript'}
        return {**result, 'protocol': PROTOCOL, 'wire_complete': not self._invalid and self._next == len(self._slots),
                'received_frames': self._next, 'received_bytes': self._bytes,
                'execution_authority': False, 'dispatch_authority': False}
