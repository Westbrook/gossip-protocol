"""Deterministic, in-process evidence dissemination demonstrator.

This is neither a real network nor an LLM/software-quality benchmark. Peers
share one trust domain; producer checks are not cryptographic authentication.
Topics are metadata, not routing or ACLs. A delivered proposal or refutation
does not become true, approved, or authoritative by being widely replicated.

Rounds use snapshots so delivery cannot cascade within a round. ``contacts``
counts attempted directed peer selections, including dropped/partitioned ones;
each contact exchanges records in both directions. It is not a wire-message
or byte count. ``converge`` uses an external observer, not distributed termination
detection. Both transports retain every record and retry via anti-entropy.

The bus is a star anti-entropy model of a reliable broker: each leaf contacts
only the broker once per round. This models connectivity and dissemination,
not production pub/sub throughput, broker replication, or processing capacity.
A leaf-origin record needs an ingress round and a later egress round to reach
another leaf. Gossip uses bounded random peer fanout without a broker.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
import random
from typing import Any, Iterable, Literal


def _canonical(value: Any) -> str:
    """Encode JSON without accepting coercions such as integer object keys."""
    def check(item: Any) -> None:
        if item is None or type(item) in (bool, int, str):
            return
        if type(item) is float and math.isfinite(item):
            return
        if type(item) is list:
            for child in item:
                check(child)
            return
        if type(item) is dict and all(type(key) is str for key in item):
            for child in item.values():
                check(child)
            return
        raise ValueError("Event content must contain only finite JSON values")

    check(value)
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True, allow_nan=False)


def _identity(producer: str, sequence: int, kind: str, payload: dict,
              topic: str) -> dict:
    for label, value in (("producer", producer), ("kind", kind), ("topic", topic)):
        if not isinstance(value, str) or not value:
            raise ValueError(f"{label} must be a nonempty string")
    if type(sequence) is not int or sequence < 0:
        raise ValueError("sequence must be a nonnegative integer")
    if type(payload) is not dict:
        raise ValueError("payload must be a JSON object")
    return {"producer": producer, "sequence": sequence, "kind": kind,
            "payload": payload, "topic": topic}


def _digest(body: dict) -> str:
    return hashlib.sha256(_canonical(body).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class Event:
    """A content-addressed record; payload reads return independent JSON copies."""

    producer: str
    sequence: int
    kind: str
    topic: str
    _payload_json: str
    event_id: str

    @classmethod
    def create(cls, producer: str, sequence: int, kind: str, payload: dict,
               topic: str = "project") -> Event:
        body = _identity(producer, sequence, kind, payload, topic)
        encoded = _canonical(body)
        return cls(producer, sequence, kind, topic, _canonical(payload),
                   hashlib.sha256(encoded.encode("utf-8")).hexdigest())

    @property
    def payload(self) -> dict:
        return json.loads(self._payload_json)

    def to_dict(self) -> dict:
        return {"event_id": self.event_id, "producer": self.producer,
                "sequence": self.sequence, "kind": self.kind,
                "topic": self.topic, "payload": self.payload}

    def verify(self) -> None:
        """Reject malformed records and content hashes that no longer match."""
        try:
            body = _identity(self.producer, self.sequence, self.kind,
                             self.payload, self.topic)
            expected = _digest(body)
        except (TypeError, ValueError) as error:
            raise ValueError("Malformed event content") from error
        if self.event_id != expected:
            raise ValueError("Event content hash mismatch")


class Mesh:
    """Compare broker-star anti-entropy with bounded-fanout push-pull gossip."""

    def __init__(self, peers: Iterable[str], mode: Literal["bus", "gossip"],
                 seed: int = 0, fanout: int = 3, batch_size: int = 8,
                 broker: str | None = None) -> None:
        peer_list = list(peers)
        if not peer_list or any(not isinstance(p, str) or not p for p in peer_list):
            raise ValueError("peers must contain nonempty names")
        if len(set(peer_list)) != len(peer_list):
            raise ValueError("peer names must be unique")
        if mode not in ("bus", "gossip"):
            raise ValueError("mode must be 'bus' or 'gossip'")
        for name, value in (("fanout", fanout), ("batch_size", batch_size)):
            if type(value) is not int or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        self.peers = tuple(sorted(peer_list))
        self.broker = self.peers[0] if broker is None else broker
        if self.broker not in self.peers:
            raise ValueError("broker must be a known peer")
        self.mode = mode
        self.fanout = fanout
        self.batch_size = batch_size
        self._rng = random.Random(seed)
        self._records: dict[str, dict[str, Event]] = {p: {} for p in self.peers}
        self._stats = {"rounds": 0, "contacts": 0, "event_deliveries": 0,
                       "duplicate_deliveries": 0}

    def _check_peer(self, peer: str) -> None:
        if peer not in self._records:
            raise ValueError(f"Unknown peer: {peer}")

    def publish(self, peer: str, event: Event) -> None:
        self._check_peer(peer)
        if not isinstance(event, Event):
            raise ValueError("Only Event records can be published")
        event.verify()
        if event.producer != peer:
            raise ValueError("Publishing peer must be the event's producer")
        self._records[peer][event.event_id] = event

    def events(self, peer: str) -> tuple[Event, ...]:
        self._check_peer(peer)
        return tuple(self._records[peer][key] for key in sorted(self._records[peer]))

    def has(self, peer: str, event_id: str) -> bool:
        self._check_peer(peer)
        return event_id in self._records[peer]

    @property
    def stats(self) -> dict[str, int]:
        return self._stats.copy()

    def _partition_map(self, partitions: list[set[str]] | None) -> dict[str, int]:
        if partitions is None:
            return dict.fromkeys(self.peers, 0)
        groups: dict[str, int] = {}
        for index, group in enumerate(partitions):
            if not group:
                raise ValueError("Partition groups must be nonempty")
            for peer in group:
                self._check_peer(peer)
                if peer in groups:
                    raise ValueError("Each peer must belong to exactly one partition")
                groups[peer] = index
        if set(groups) != set(self.peers):
            raise ValueError("Partitions must include every peer exactly once")
        return groups

    def step(self, partitions: list[set[str]] | None = None,
             drop_rate: float = 0.0) -> int:
        """Run one round; return the number of newly inserted remote records.

        ``drop_rate`` independently drops whole contacts (both exchange directions).
        ``partitions`` must list every peer exactly once. Selection still attempts
        unreachable peers, so faults reduce useful work rather than hiding cost.
        Batches contain the first sorted IDs missing at the destination snapshot.
        For finite input, delivered IDs leave subsequent batches, avoiding starvation.
        """
        if not isinstance(drop_rate, (int, float)) or not 0 <= drop_rate <= 1:
            raise ValueError("drop_rate must be between zero and one")
        groups = self._partition_map(partitions)
        snapshots = {peer: records.copy() for peer, records in self._records.items()}
        pending: list[tuple[str, Event]] = []
        self._stats["rounds"] += 1

        def offer(source: str, target: str) -> None:
            missing = sorted(snapshots[source].keys() - snapshots[target].keys())
            pending.extend((target, snapshots[source][key])
                           for key in missing[:self.batch_size])

        for source in self.peers:
            if self.mode == "bus":
                targets = [] if source == self.broker else [self.broker]
            else:
                candidates = [peer for peer in self.peers if peer != source]
                targets = self._rng.sample(candidates, min(self.fanout, len(candidates)))
            for target in targets:
                self._stats["contacts"] += 1
                if groups[source] != groups[target]:
                    continue
                if drop_rate and self._rng.random() < drop_rate:
                    continue
                offer(source, target)
                offer(target, source)

        delivered = 0
        for target, event in pending:
            if event.event_id in self._records[target]:
                self._stats["duplicate_deliveries"] += 1
            else:
                self._records[target][event.event_id] = event
                delivered += 1
        self._stats["event_deliveries"] += delivered
        return delivered

    def converge(self, max_rounds: int = 100, **faults: Any) -> bool:
        """Use an external observer to check identical event-ID sets, with a cap."""
        if type(max_rounds) is not int or max_rounds < 0:
            raise ValueError("max_rounds must be a nonnegative integer")

        def same_records() -> bool:
            reference = self._records[self.peers[0]].keys()
            return all(records.keys() == reference for records in self._records.values())

        for _ in range(max_rounds):
            if same_records():
                return True
            self.step(**faults)
        return same_records()
