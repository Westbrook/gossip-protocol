"""Frozen-evidence dissemination simulation; no model, network or Git execution.

Every arm is charged N scheduled full-duplex contact opportunities per round,
including offline callers that cannot physically transmit. Contacts
exchange at most ``batch_size`` missing records per direction from round-start
snapshots. Equal contacts do not imply equal bytes, capacity, or production cost.
The replicated arm is two durable anti-entropy hubs with *client* timeout routing;
it is not consensus, leader election, or linearizable promotion. All records and
reviewer decisions are durable. Restarts round-trip persisted records through JSON
and reset volatile routing state, without deleting evidence or doing real I/O.

Reviewers consume only their local log. The externally supplied epoch floor is a
trusted work assignment, not discovered by observing the network. An authority's
received contract binds exact source bytes and exact pre-frozen receipt IDs. Both
positive checks and an explicit challenge-complete receipt must arrive before any
decision. Receipt absence never implies clearance. This intentionally models
*dissemination of already frozen evidence*, not dynamic testing or agent quality.
"""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
import math
from pathlib import Path
import random
import statistics
import sys
from typing import Any

from gossip_harness.transport import Event

PROTOCOL = "frozen-evidence-transport-v1"
PEERS = ("primary", "standby", "builder-a", "builder-b", "checker-a", "checker-b", "reviewer-a", "reviewer-b")
REVIEWERS = ("reviewer-a", "reviewer-b")
CHECKERS = ("checker-a", "checker-b")
ARMS = ("gossip", "durable-broker", "replicated-broker-client-failover")
SCENARIOS = ("healthy", "primary-isolation", "standby-isolation", "worker-isolation", "partition-heal", "loss-reorder", "persisted-restart")
DEFAULT_ROUNDS = 64


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def event_from_dict(value: dict[str, Any]) -> Event:
    if set(value) != {"producer", "sequence", "kind", "payload", "topic", "event_id"}:
        raise ValueError("Malformed serialized event")
    event = Event.create(value["producer"], value["sequence"], value["kind"], value["payload"], value["topic"])
    if event.event_id != value["event_id"]:
        raise ValueError("Event content hash mismatch")
    return event


def _artifact(producer: str, sequence: int, content: str) -> Event:
    return Event.create(producer, sequence, "artifact", {"content_utf8": content, "sha256": hashlib.sha256(content.encode()).hexdigest()})


def synthetic_corpus() -> dict[str, Any]:
    """A rehearsal corpus with accepted/rejected offers and stale/forged distractors."""
    schedule: list[dict[str, Any]] = []
    expectations: dict[str, str] = {}
    sequence: Counter[str] = Counter()

    def make(peer: str, kind: str, payload: dict[str, Any], when: int) -> Event:
        sequence[peer] += 1
        event = Event.create(peer, sequence[peer], kind, payload)
        schedule.append({"round": when, "event": event.to_dict()})
        return event

    def artifact(peer: str, content: str, when: int) -> Event:
        sequence[peer] += 1
        event = _artifact(peer, sequence[peer], content)
        schedule.append({"round": when, "event": event.to_dict()})
        return event

    for index, offer_id in enumerate(("good", "refuted")):
        producer = f"builder-{'a' if index == 0 else 'b'}"
        source = artifact(producer, f"synthetic frozen source for {offer_id}\n", 0)
        source_hash = source.payload["sha256"]
        base = {"offer_id": offer_id, "epoch": 2, "source_sha256": source_hash}
        required = []
        for checker, check_id, polarity, verdict, when in (
            ("checker-a", "regression", "positive", "pass", 6),
            ("checker-b", "independent-challenge", "refutation", "clear" if index == 0 else "refuted", 10 + 4 * index),
        ):
            evidence = artifact(checker, canonical({**base, "check_id": check_id, "verdict": verdict}), when)
            receipt = make(checker, "receipt", {**base, "check_id": check_id, "polarity": polarity, "verdict": verdict, "evidence_sha256": evidence.payload["sha256"]}, when)
            required.append({"event_id": receipt.event_id, "producer": checker, "check_id": check_id, "polarity": polarity})
        make("builder-a", "contract", {**base, "builder": producer, "required_receipts": required}, 4)
        make(producer, "offer", base, 4)
        make(producer, "offer", {**base, "epoch": 1}, 1)
        make("checker-b", "receipt", {**base, "epoch": 1, "check_id": "independent-challenge", "polarity": "refutation", "verdict": "clear", "evidence_sha256": source_hash}, 5)
        make(producer, "receipt", {**base, "check_id": "independent-challenge", "polarity": "refutation", "verdict": "clear", "evidence_sha256": source_hash}, 5)
        expectations[offer_id] = "accept" if index == 0 else "reject"
    # Exact event replay is intentional. It must not create a second record/job.
    schedule.append({"round": 8, "event": schedule[0]["event"]})
    return {"schema_version": 1, "purpose": "synthetic-rehearsal", "label": "Two frozen offers; one independently refuted", "authority": "builder-a", "epoch_floors": {"good": 2, "refuted": 2}, "expected_decisions": expectations, "schedule": sorted(schedule, key=lambda item: (item["round"], item["event"]["event_id"]))}


def frozen_artifact_corpus(offers: list[dict[str, Any]], *, label: str, provenance: dict[str, Any]) -> dict[str, Any]:
    """Embed exact completed-study bytes without executing code or grading them.

    Each offer must contain ``offer_id``, ``source_utf8``,
    ``positive_evidence_utf8``, ``challenge_evidence_utf8``, and Boolean
    ``positive_passed`` / ``challenge_clear``. Callers obtain strings with
    ``Path(...).read_bytes().decode('utf-8')`` to preserve exact newlines. A
    multi-file source can be the exact retained source-bundle JSON text.

    The verdicts must come from independently audited completed-study receipts.
    This helper binds and distributes those verdicts; it does not certify the
    receipts' execution or correctness. ``provenance`` should name and SHA256
    bind the study, audit, frozen source bundles, and original receipt files.
    No inferred quality gain follows from successful distribution of their data.
    """
    if not offers or not label or not provenance:
        raise ValueError("Offers, label, and explicit study provenance are required")
    schedule: list[dict[str, Any]] = []
    expectations: dict[str, str] = {}
    sequence: Counter[str] = Counter()

    def make(peer: str, kind: str, payload: dict[str, Any], when: int) -> Event:
        sequence[peer] += 1
        event = Event.create(peer, sequence[peer], kind, payload)
        schedule.append({"round": when, "event": event.to_dict()})
        return event

    def artifact(peer: str, content: str, when: int) -> Event:
        sequence[peer] += 1
        event = _artifact(peer, sequence[peer], content)
        schedule.append({"round": when, "event": event.to_dict()})
        return event

    for index, item in enumerate(offers):
        offer_id = item.get("offer_id")
        if type(offer_id) is not str or not offer_id or offer_id in expectations:
            raise ValueError("Offer IDs must be unique nonempty strings")
        if any(type(item.get(key)) is not str for key in ("source_utf8", "positive_evidence_utf8", "challenge_evidence_utf8")) or any(type(item.get(key)) is not bool for key in ("positive_passed", "challenge_clear")):
            raise ValueError("Exact UTF-8 artifact text and Boolean audited verdicts required")
        producer = "builder-a" if index % 2 == 0 else "builder-b"
        source = artifact(producer, item["source_utf8"], 0)
        binding = {"offer_id": offer_id, "epoch": 2, "source_sha256": source.payload["sha256"]}
        requirements = []
        for peer, check_id, polarity, field, verdict, when in (
            ("checker-a", "retained-positive-suite", "positive", "positive_evidence_utf8", "pass" if item["positive_passed"] else "fail", 6),
            ("checker-b", "independent-acceptance", "refutation", "challenge_evidence_utf8", "clear" if item["challenge_clear"] else "refuted", 10),
        ):
            evidence = artifact(peer, item[field], when)
            receipt = make(peer, "receipt", {**binding, "check_id": check_id, "polarity": polarity, "verdict": verdict, "evidence_sha256": evidence.payload["sha256"]}, when)
            requirements.append({"event_id": receipt.event_id, "producer": peer, "check_id": check_id, "polarity": polarity})
        make("builder-a", "contract", {**binding, "builder": producer, "required_receipts": requirements}, 4)
        make(producer, "offer", binding, 4)
        expectations[offer_id] = "accept" if item["positive_passed"] and item["challenge_clear"] else "reject"
    corpus = {"schema_version": 1, "purpose": "frozen-artifact-replay", "label": label, "provenance": provenance, "authority": "builder-a", "epoch_floors": dict.fromkeys(expectations, 2), "expected_decisions": expectations, "schedule": sorted(schedule, key=lambda item: (item["round"], item["event"]["event_id"]))}
    validate_corpus(corpus, DEFAULT_ROUNDS)
    return corpus


def _artifact_available(records: dict[str, Event], sha256: str | None, producer: str | None = None) -> bool:
    if type(sha256) is not str:
        return False
    for event in records.values():
        payload = event.payload
        if event.kind == "artifact" and (producer is None or event.producer == producer) and payload.get("sha256") == sha256:
            content = payload.get("content_utf8")
            if type(content) is str and hashlib.sha256(content.encode()).hexdigest() == sha256:
                return True
    return False


def local_decision(records: dict[str, Event], offer_id: str, epoch_floor: int, authority: str) -> str | None:
    """Decide solely from visible records and the explicit trusted assignment."""
    contracts = [event.payload for event in records.values() if event.kind == "contract" and event.producer == authority and event.payload.get("offer_id") == offer_id and event.payload.get("epoch") == epoch_floor]
    if len({canonical(item) for item in contracts}) != 1:
        return None
    contract = contracts[0]
    source = contract.get("source_sha256")
    if not _artifact_available(records, source, contract.get("builder")):
        return None
    binding = {key: contract.get(key) for key in ("offer_id", "epoch", "source_sha256")}
    if not any(event.kind == "offer" and event.producer == contract.get("builder") and event.payload == binding for event in records.values()):
        return None
    requirements = contract.get("required_receipts")
    if type(requirements) is not list or not requirements or {item.get("polarity") for item in requirements} != {"positive", "refutation"}:
        return None
    if len({item.get("event_id") for item in requirements}) != len(requirements):
        return None
    if any(item.get("producer") not in CHECKERS or item.get("producer") == contract.get("builder") for item in requirements):
        return None
    positive_producers = {item["producer"] for item in requirements if item["polarity"] == "positive"}
    challenge_producers = {item["producer"] for item in requirements if item["polarity"] == "refutation"}
    if positive_producers & challenge_producers:
        return None
    verdicts = []
    for required in requirements:
        event = records.get(required.get("event_id"))
        if event is None or event.kind != "receipt" or event.producer != required.get("producer"):
            return None
        payload = event.payload
        if any(payload.get(key) != value for key, value in binding.items()) or any(payload.get(key) != required.get(key) for key in ("check_id", "polarity")):
            return None
        if not _artifact_available(records, payload.get("evidence_sha256"), event.producer):
            return None
        allowed = ("pass", "fail") if required["polarity"] == "positive" else ("clear", "refuted")
        if payload.get("verdict") not in allowed:
            return None
        verdicts.append(payload["verdict"])
    return "reject" if any(verdict in ("fail", "refuted") for verdict in verdicts) else "accept"


def validate_corpus(corpus: dict[str, Any], rounds: int) -> None:
    if corpus.get("schema_version") != 1 or corpus.get("purpose") not in ("synthetic-rehearsal", "frozen-artifact-replay"):
        raise ValueError("Unsupported corpus schema or purpose")
    if corpus.get("authority") not in PEERS:
        raise ValueError("Unknown authority")
    floors = corpus.get("epoch_floors")
    expected = corpus.get("expected_decisions")
    if type(floors) is not dict or not floors or type(expected) is not dict or set(floors) != set(expected):
        raise ValueError("Explicit assignments and external expected outcomes are required")
    if any(type(value) is not int or value < 1 for value in floors.values()) or any(value not in ("accept", "reject") for value in expected.values()):
        raise ValueError("Invalid epoch floor or expected outcome")
    records = {}
    for item in corpus["schedule"]:
        if type(item["round"]) is not int or not 0 <= item["round"] < rounds:
            raise ValueError("Publication outside the run")
        event = event_from_dict(item["event"])
        if event.producer not in PEERS:
            raise ValueError("Unknown producer")
        if event.kind == "artifact":
            payload = event.payload
            if type(payload.get("content_utf8")) is not str or hashlib.sha256(payload["content_utf8"].encode()).hexdigest() != payload.get("sha256"):
                raise ValueError("Artifact byte hash mismatch")
        records[event.event_id] = event
    for offer_id, floor in floors.items():
        if local_decision(records, offer_id, floor, corpus["authority"]) != expected[offer_id]:
            raise ValueError("Complete corpus does not establish its external expected outcome")


def _field(seed: int, round_index: int, source: str, target: str, ordinal: int, label: str) -> float:
    """Common environmental field, independent from topology random draws."""
    value = digest([PROTOCOL, seed, round_index, source, target, ordinal, label])
    return int(value[:13], 16) / 16**13


def _offline(peer: str, scenario: str, round_index: int) -> bool:
    return scenario == "persisted-restart" and ((peer in ("primary", "checker-a") and 8 <= round_index <= 11) or (peer == "reviewer-a" and round_index == 12))


def _reachable(source: str, target: str, scenario: str, round_index: int) -> bool:
    if _offline(source, scenario, round_index) or _offline(target, scenario, round_index):
        return False
    if not 4 <= round_index <= 20:
        return True
    isolates = {"primary-isolation": "primary", "standby-isolation": "standby", "worker-isolation": "checker-b"}
    if scenario in isolates:
        isolated = isolates[scenario]
        return source != isolated and target != isolated
    if scenario == "partition-heal":
        side = {"primary", "builder-a", "checker-a", "reviewer-a"}
        return (source in side) == (target in side)
    return True


def run_trial(corpus: dict[str, Any], arm: str, scenario: str, seed: int, *, rounds: int = DEFAULT_ROUNDS, batch_size: int = 4, retain_trace: bool = False) -> dict[str, Any]:
    if arm not in ARMS or scenario not in SCENARIOS or type(rounds) is not int or rounds < 24 or type(batch_size) is not int or batch_size < 1:
        raise ValueError("Invalid trial configuration")
    validate_corpus(corpus, rounds)
    rng = random.Random(seed)
    records: dict[str, dict[str, Event]] = {peer: {} for peer in PEERS}
    routes = dict.fromkeys(PEERS[2:], "primary")
    failures = dict.fromkeys(PEERS[2:], 0)
    pending: list[tuple[int, str, str, Event]] = []
    publications: dict[int, list[Event]] = {}
    for item in corpus["schedule"]:
        publications.setdefault(item["round"], []).append(event_from_dict(item["event"]))
    decisions: dict[str, dict[str, dict[str, Any]]] = {peer: {} for peer in REVIEWERS}
    consumed: dict[str, set[str]] = {peer: set() for peer in REVIEWERS}
    ingestion_counts: Counter[tuple[str, str]] = Counter()
    counters: Counter[str] = Counter()
    trace = []
    restarts = []

    def insert(peer: str, event: Event, *, delivered: bool) -> None:
        event.verify()
        if event.event_id in records[peer]:
            counters["duplicate_record_deliveries" if delivered else "duplicate_origin_publications"] += 1
        else:
            records[peer][event.event_id] = event
            counters["record_deliveries" if delivered else "origin_publications"] += 1

    def observe(round_index: int) -> None:
        for peer in REVIEWERS:
            if _offline(peer, scenario, round_index):
                counters["reviewer_offline_rounds"] += 1
                counters["pending_reviewer_offer_rounds"] += len(corpus["epoch_floors"]) - len(decisions[peer])
                continue
            new_receipts = {key for key, event in records[peer].items() if event.kind == "receipt"} - consumed[peer]
            counters["unique_receipt_consumptions"] += len(new_receipts)
            consumed[peer].update(new_receipts)
            for event_id in new_receipts:
                ingestion_counts[(peer, event_id)] += 1
            for offer_id, floor in corpus["epoch_floors"].items():
                if offer_id in decisions[peer]:
                    continue
                outcome = local_decision(records[peer], offer_id, floor, corpus["authority"])
                if outcome is None:
                    counters["pending_reviewer_offer_rounds"] += 1
                    continue
                decisions[peer][offer_id] = {"decision": outcome, "round": round_index, "attempted_contacts": counters["attempted_contacts"], "visible_event_ids": sorted(records[peer])}
                # Expectations are accessed only here by the external scorer.
                expected = corpus["expected_decisions"][offer_id]
                counters["false_accepts"] += int(outcome == "accept" and expected != "accept")
                counters["false_rejects"] += int(outcome == "reject" and expected != "reject")

    for event in publications.get(0, []):
        insert(event.producer, event, delivered=False)
    observe(0)
    for round_index in range(1, rounds + 1):
        for peer in PEERS:
            if _offline(peer, scenario, round_index - 1) and not _offline(peer, scenario, round_index):
                encoded = canonical([event.to_dict() for event in records[peer].values()])
                records[peer] = {event.event_id: event for item in json.loads(encoded) for event in [event_from_dict(item)]}
                if peer in routes:
                    routes[peer] = "primary"
                    failures[peer] = 0
                restarts.append({"peer": peer, "round": round_index, "persisted_event_ids": sorted(records[peer])})
        for event in publications.get(round_index, []):
            # The corpus models durable pre-frozen production, including offline
            # producers: publication enters their durable log but cannot transmit.
            insert(event.producer, event, delivered=False)
        ready, pending = [item for item in pending if item[0] <= round_index], [item for item in pending if item[0] > round_index]
        for _, source, target, event in ready:
            if _reachable(source, target, scenario, round_index):
                insert(target, event, delivered=True)
            else:
                counters["delayed_records_dropped_at_delivery"] += 1
        snapshots = {peer: dict(items) for peer, items in records.items()}
        if arm == "gossip":
            contacts = [(peer, rng.choice([other for other in PEERS if other != peer])) for peer in PEERS]
        elif arm == "durable-broker":
            leaves = PEERS[1:]
            contacts = [(peer, "primary") for peer in leaves] + [(leaves[(round_index - 1) % len(leaves)], "primary")]
        else:
            workers = PEERS[2:]
            contacts = [(peer, routes[peer]) for peer in workers] + [("standby", "primary"), (workers[(round_index - 1) % len(workers)], routes[workers[(round_index - 1) % len(workers)]])]
        immediate = []
        contact_occurrences: Counter[tuple[str, str]] = Counter()
        round_trace = []
        for source, target in contacts:
            ordinal = contact_occurrences[(source, target)]
            contact_occurrences[(source, target)] += 1
            counters["attempted_contacts"] += 1
            reachable = _reachable(source, target, scenario, round_index)
            lost = scenario == "loss-reorder" and 4 <= round_index <= 35 and _field(seed, round_index, source, target, ordinal, "loss") < 0.30
            success = reachable and not lost
            if arm == "replicated-broker-client-failover" and source in routes and not _offline(source, scenario, round_index):
                failures[source] = 0 if success else failures[source] + 1
                if failures[source] >= 3:
                    routes[source] = "standby" if routes[source] == "primary" else "primary"
                    failures[source] = 0
                    counters["client_route_switches"] += 1
            delay = int(_field(seed, round_index, source, target, ordinal, "delay") * 4) if scenario == "loss-reorder" and 4 <= round_index <= 35 else 0
            round_trace.append([PEERS.index(source), PEERS.index(target), "ok" if success else "unreachable" if not reachable else "loss", delay])
            if not success:
                counters["failed_contacts"] += 1
                continue
            counters["successful_contacts"] += 1
            for sender, receiver in ((source, target), (target, source)):
                missing = sorted(snapshots[sender].keys() - snapshots[receiver].keys())[:batch_size]
                for key in missing:
                    event = snapshots[sender][key]
                    counters["attempted_record_transfers"] += 1
                    counters["serialized_record_bytes"] += len(canonical(event.to_dict()).encode())
                    if delay:
                        pending.append((round_index + delay, sender, receiver, event))
                    else:
                        immediate.append((receiver, event))
        for target, event in immediate:
            insert(target, event, delivered=True)
        trace.append(round_trace)
        observe(round_index)
    all_events = {item["event"]["event_id"] for item in corpus["schedule"]}
    union = set().union(*(set(items) for items in records.values()))
    decision_rows = [item for offers in decisions.values() for item in offers.values()]
    target_count = len(REVIEWERS) * len(corpus["epoch_floors"])
    complete = len(decision_rows) == target_count
    metric_names = ("attempted_contacts", "successful_contacts", "failed_contacts", "record_deliveries", "attempted_record_transfers", "serialized_record_bytes", "duplicate_record_deliveries", "duplicate_origin_publications", "unique_receipt_consumptions", "pending_reviewer_offer_rounds", "reviewer_offline_rounds", "false_accepts", "false_rejects", "client_route_switches", "delayed_records_dropped_at_delivery")
    result = {"arm": arm, "scenario": scenario, "seed": seed, "rounds_executed": rounds, "contacts_per_round": len(PEERS), "decisions": decisions, "actionable_decisions": len(decision_rows), "required_decisions": target_count, "correct_complete": complete and counters["false_accepts"] == counters["false_rejects"] == 0, "rounds_to_all_actionable": max(item["round"] for item in decision_rows) if complete else None, "contacts_to_all_actionable": max(item["attempted_contacts"] for item in decision_rows) if complete else None, "lost_facts": len(all_events - union), "undelivered_reviewer_facts": sum(len(all_events - set(records[peer])) for peer in REVIEWERS), "duplicate_receipt_consumptions": sum(max(0, count - 1) for count in ingestion_counts.values()), "metrics": {name: counters[name] for name in metric_names}, "restarts": restarts, "contact_schedule_sha256": digest(trace)}
    if retain_trace:
        result["contact_trace"] = trace
    return result


def _summarize(values: list[int | None]) -> dict[str, Any]:
    available = sorted(value for value in values if value is not None)
    return {"completed": len(available), "censored": len(values) - len(available), "median": statistics.median(available) if available else None, "p95_nearest_rank": available[math.ceil(len(available) * 0.95) - 1] if available else None}


def run_experiment(corpus: dict[str, Any], *, seeds: int = 20, base_seed: int = 20261002, rounds: int = DEFAULT_ROUNDS, batch_size: int = 4, retain_trace: bool = False, corpus_bytes_sha256: str | None = None) -> dict[str, Any]:
    if type(seeds) is not int or seeds < 20:
        raise ValueError("The benchmark requires at least 20 deterministic seeds")
    validate_corpus(corpus, rounds)
    trials = [run_trial(corpus, arm, scenario, seed, rounds=rounds, batch_size=batch_size, retain_trace=retain_trace) for scenario in SCENARIOS for seed in range(base_seed, base_seed + seeds) for arm in ARMS]
    summaries = []
    paired = []
    for scenario in SCENARIOS:
        for arm in ARMS:
            selected = [item for item in trials if item["scenario"] == scenario and item["arm"] == arm]
            summaries.append({"scenario": scenario, "arm": arm, "trials": len(selected), "correct_complete": sum(item["correct_complete"] for item in selected), "false_accepts": sum(item["metrics"]["false_accepts"] for item in selected), "false_rejects": sum(item["metrics"]["false_rejects"] for item in selected), "lost_facts": sum(item["lost_facts"] for item in selected), "rounds_to_all_actionable": _summarize([item["rounds_to_all_actionable"] for item in selected]), "contacts_to_all_actionable": _summarize([item["contacts_to_all_actionable"] for item in selected]), "pending_reviewer_offer_rounds": _summarize([item["metrics"]["pending_reviewer_offer_rounds"] for item in selected]), "serialized_record_bytes": _summarize([item["metrics"]["serialized_record_bytes"] for item in selected]), "duplicate_record_deliveries": _summarize([item["metrics"]["duplicate_record_deliveries"] for item in selected])})
        for opponent in ARMS[1:]:
            counts: Counter[str] = Counter()
            for seed in range(base_seed, base_seed + seeds):
                left = next(item for item in trials if item["scenario"] == scenario and item["seed"] == seed and item["arm"] == "gossip")
                right = next(item for item in trials if item["scenario"] == scenario and item["seed"] == seed and item["arm"] == opponent)
                def score(item: dict[str, Any]) -> tuple[int, int, int]:
                    return (-item["metrics"]["false_accepts"], int(item["correct_complete"]), -item["rounds_to_all_actionable"] if item["correct_complete"] else -rounds - 1)
                counts["win" if score(left) > score(right) else "loss" if score(left) < score(right) else "draw"] += 1
            paired.append({"scenario": scenario, "left": "gossip", "right": opponent, **{key: counts[key] for key in ("win", "draw", "loss")}})
    return {"schema_version": 1, "protocol": PROTOCOL, "purpose": corpus["purpose"], "label": corpus["label"], "source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), "corpus_canonical_sha256": digest(corpus), "corpus_input_bytes_sha256": corpus_bytes_sha256, "python_version": sys.version.split()[0], "configuration": {"seeds": seeds, "base_seed": base_seed, "rounds": rounds, "batch_size": batch_size, "peers": list(PEERS), "contacts_per_round": len(PEERS), "contact_unit": "Charged scheduled full-duplex contact opportunity, including offline callers; not physical packets or bytes", "arms": list(ARMS), "scenarios": list(SCENARIOS)}, "scope": "Deterministic frozen-evidence simulation only; no network, model reasoning, Git promotion, dynamic test execution, or production availability claim.", "ranking": "Per scenario, matched seed: fewer false accepts, then correct complete, then earlier all-actionable round. No speed credit for incomplete runs; no Elo.", "fault_contract": {"isolation_and_partition_rounds": [4, 20], "worker_isolated": "checker-b", "partition_left": ["primary", "builder-a", "checker-a", "reviewer-a"], "loss_window": [4, 35], "contact_drop_probability": 0.30, "delay_rounds": [0, 3], "paired_field": "SHA256(protocol,seed,round,source,target,per-pair ordinal,label); independent of topology RNG", "failover": "Per-client switch after three failed attempted contacts; replication uses one charged contact per round", "restarts": "primary/checker-a offline rounds8..11; reviewer-a offline round12; durable logs and decision/receipt-consumption state retained; volatile client route/timeouts reset"}, "limitations": ["Equal contacts and batch caps do not imply equal bytes or node capacity; bytes are reported separately.", "Faults are specified stress cases, not a sample of real failure incidence.", "Epoch floors and a single trusted contract producer are explicit inputs; authentication/consensus are not modeled.", "Reviewer requirements name exact frozen receipt IDs; this is closed evidence dissemination, not discovering whether future refutations exist.", "All producer logs are durable; zero globally lost facts is a storage assumption/control, not proof of fault tolerance.", "Receipt consumption means idempotent ingest of a visible receipt ID. Local decision predicates may rescan cached records; duplicate coding or inference work is not modeled.", "Replicated anti-entropy hubs with client failover provide no exclusive-leader or linearizable-promotion guarantee.", "Seeds vary gossip choices and the common loss field; broker schedules repeat identically outside loss. These are deterministic transport cases, not independent software projects or model trials."], "summary": summaries, "paired_win_draw_loss": paired, "trials": trials}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", type=Path, help="Frozen corpus JSON with exact source/receipt artifact UTF-8 bytes embedded")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--write-synthetic-corpus", type=Path)
    parser.add_argument("--seeds", type=int, default=20)
    parser.add_argument("--base-seed", type=int, default=20261002)
    parser.add_argument("--rounds", type=int, default=DEFAULT_ROUNDS)
    parser.add_argument("--batch-size", type=int, default=4)
    parser.add_argument("--retain-traces", action="store_true")
    args = parser.parse_args()
    if args.output.exists() or args.write_synthetic_corpus and args.write_synthetic_corpus.exists():
        raise SystemExit("Refusing to overwrite a retained artifact")
    raw = args.corpus.read_bytes() if args.corpus else None
    corpus = json.loads(raw) if raw is not None else synthetic_corpus()
    if args.write_synthetic_corpus:
        if raw is not None:
            raise SystemExit("Synthetic corpus export cannot accompany --corpus")
        args.write_synthetic_corpus.parent.mkdir(parents=True, exist_ok=True)
        with args.write_synthetic_corpus.open("x") as stream:
            json.dump(corpus, stream, indent=2)
            stream.write("\n")
    result = run_experiment(corpus, seeds=args.seeds, base_seed=args.base_seed, rounds=args.rounds, batch_size=args.batch_size, retain_trace=args.retain_traces, corpus_bytes_sha256=hashlib.sha256(raw).hexdigest() if raw is not None else None)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")
    print(canonical({"output": str(args.output), "protocol": PROTOCOL, "purpose": corpus["purpose"], "trials": len(result["trials"]), "unsafe_accepts": sum(item["metrics"]["false_accepts"] for item in result["trials"])}))


if __name__ == "__main__":
    main()
