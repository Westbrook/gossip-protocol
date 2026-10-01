#!/usr/bin/env python3
"""Reproducible dissemination experiment, NOT an LLM or software-quality benchmark.

Run from the repository root:
    python3 -m unittest discover -s simulation -p 'test_*.py' -v
    python3 simulation/dissemination.py --output simulation/results.json

The experiment distributes one immutable fact, initially known by node zero.
Every node chooses up to `fanout` distinct uniformly random OTHER nodes each
round. Choices are independent between senders and rounds. All communication
uses the START-OF-ROUND snapshot: knowledge cannot cascade inside one round.
In push mode only the caller's snapshot propagates; in push-pull either endpoint
having the fact makes both endpoints informed at the end of the round.

Attempted contacts count EVERY caller/peer pair, including calls from uninformed
nodes, duplicate destination choices, and reciprocal choices. A push-pull contact
is a full-duplex exchange, not a claim that request and reply cost one message.
We do not estimate wire bytes, request/reply counts, inference, or context cost.

Assumptions are deliberately optimistic: complete connectivity, fixed population,
no latency variation, no bandwidth/concurrency limits, no loss, no failures, no
membership discovery overhead, and no conflicting or superseded information.
The ideal star baseline is one simultaneous delivery per other node, N-1 total,
in one network hop, assuming the initial fact is already at the hub. It has no
hub throughput limit. It does NOT model the latency or cost of an LLM orchestrator.
"""

from __future__ import annotations

import argparse
import json
import math
import random
import statistics
import sys
from pathlib import Path
from typing import Iterable, Sequence


MODES = ("push", "push_pull")


def effective_fanout(node_count: int, requested_fanout: int) -> int:
    if node_count < 1:
        raise ValueError("node_count must be at least one")
    if requested_fanout < 0:
        raise ValueError("requested_fanout must be nonnegative")
    return min(requested_fanout, node_count - 1)


def draw_contacts(node_count: int, fanout: int, rng: random.Random) -> list[list[int]]:
    """Draw uniform peers without materializing an N-element list for each node."""
    k = effective_fanout(node_count, fanout)
    contacts: list[list[int]] = []
    for caller in range(node_count):
        # Sampling 0..N-2 then skipping caller is a bijection onto all other nodes.
        sampled = rng.sample(range(node_count - 1), k)
        contacts.append([peer if peer < caller else peer + 1 for peer in sampled])
    return contacts


def spread_round(
    informed: set[int], contacts: Sequence[Iterable[int]], mode: str
) -> set[int]:
    """Apply a supplied valid contact schedule using only the incoming snapshot."""
    if mode not in MODES:
        raise ValueError(f"unknown mode: {mode}")
    next_informed = set(informed)
    for caller, peers in enumerate(contacts):
        caller_informed = caller in informed
        for peer in peers:
            if caller_informed:
                next_informed.add(peer)
            if mode == "push_pull" and peer in informed:
                next_informed.add(caller)
    return next_informed


def run_trial(
    node_count: int,
    fanout: int,
    seed: int,
    mode: str,
    max_rounds: int = 1000,
) -> dict:
    if mode not in MODES:
        raise ValueError(f"unknown mode: {mode}")
    k = effective_fanout(node_count, fanout)
    rng = random.Random(seed)
    informed = {0}
    history = [1]
    threshold_95 = math.ceil(0.95 * node_count)
    rounds_95 = 0 if len(informed) >= threshold_95 else None
    for round_index in range(1, max_rounds + 1):
        if len(informed) == node_count:
            break
        informed = spread_round(informed, draw_contacts(node_count, k, rng), mode)
        history.append(len(informed))
        if rounds_95 is None and len(informed) >= threshold_95:
            rounds_95 = round_index
    rounds_100 = len(history) - 1 if len(informed) == node_count else None
    return {
        "seed": seed,
        "rounds_to_95_percent": rounds_95,
        "rounds_to_100_percent": rounds_100,
        "attempted_contacts_to_95_percent": rounds_95 * node_count * k
        if rounds_95 is not None else None,
        "attempted_contacts_to_100_percent": rounds_100 * node_count * k
        if rounds_100 is not None else None,
        "informed_at_end_of_round": history,
    }


def summarize(values: list[int | None]) -> dict:
    """Nearest-rank percentiles; count null failures separately, never hide them."""
    completed = sorted(value for value in values if value is not None)
    if not completed:
        return {"completed": 0, "not_reached": len(values), "p50": None, "p95": None}
    return {
        "completed": len(completed),
        "not_reached": len(values) - len(completed),
        "min": completed[0],
        "p50": completed[math.ceil(0.50 * len(completed)) - 1],
        "p95": completed[math.ceil(0.95 * len(completed)) - 1],
        "max": completed[-1],
        "mean": statistics.mean(completed),
    }


def run_experiment(fanout: int = 3, base_seed: int = 20260929) -> dict:
    result = {
        "experiment": "Synchronous dissemination of one immutable fact",
        "schema_version": 1,
        "python_version": sys.version.split()[0],
        "requested_fanout": fanout,
        "base_seed": base_seed,
        "percentile_method": "nearest rank; failures counted separately",
        "contact_unit": "one directed caller-peer contact; push-pull is full-duplex",
        "model": {
            "initial_informed_node": 0,
            "topology": "complete graph; peers uniformly sampled without replacement per caller",
            "scheduling": "all nodes contact fanout peers every round, including uninformed nodes",
            "round_semantics": "start-of-round snapshots; no within-round cascades",
            "pairing": "identical seed/contact schedules for push and push-pull until either terminates",
            "attempted_contacts_include": [
                "uninformed callers", "repeated destinations across callers/rounds", "reciprocal contacts"
            ],
            "assumptions": [
                "fixed population", "no failures or packet loss", "no network partitions",
                "unlimited per-round concurrency and bandwidth", "no membership discovery overhead",
                "no conflicting or superseded facts", "no stopping-detection overhead"
            ],
            "not_measured": [
                "wire messages or bytes", "wall-clock latency", "LLM tokens or cost",
                "reasoning or software correctness", "task allocation or agreement"
            ],
        },
        "configurations": [],
    }
    for node_count, trial_count in ((1000, 100), (10000, 20)):
        config = {
            "node_count": node_count,
            "trial_count": trial_count,
            "effective_fanout": effective_fanout(node_count, fanout),
            "ideal_star_pubsub": {
                "deliveries_to_100_percent": node_count - 1,
                "network_hops_to_100_percent": 1 if node_count > 1 else 0,
                "assumptions": "fact initially at hub; simultaneous deliveries; no hub throughput limit",
                "limitation": "delivery count is not a comparison of LLM inference or context cost",
            },
            "modes": {},
        }
        seeds = [base_seed + node_count * 1000 + index for index in range(trial_count)]
        for mode in MODES:
            trials = [run_trial(node_count, fanout, seed, mode) for seed in seeds]
            config["modes"][mode] = {
                "summary": {
                    field: summarize([trial[field] for trial in trials])
                    for field in (
                        "rounds_to_95_percent", "rounds_to_100_percent",
                        "attempted_contacts_to_95_percent", "attempted_contacts_to_100_percent",
                    )
                },
                "trials": trials,
            }
        result["configurations"].append(config)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path(__file__).with_name("results.json"))
    parser.add_argument("--fanout", type=int, default=3)
    parser.add_argument("--seed", type=int, default=20260929)
    args = parser.parse_args()
    result = run_experiment(fanout=args.fanout, base_seed=args.seed)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    for config in result["configurations"]:
        for mode, data in config["modes"].items():
            summary = data["summary"]
            r95, r100 = summary["rounds_to_95_percent"], summary["rounds_to_100_percent"]
            c100 = summary["attempted_contacts_to_100_percent"]
            print(
                f"N={config['node_count']} trials={config['trial_count']} {mode}: "
                f"95% rounds p50/p95={r95['p50']}/{r95['p95']}; "
                f"100% rounds p50/p95={r100['p50']}/{r100['p95']}; "
                f"100% contacts p50/p95={c100['p50']}/{c100['p95']}"
            )
    print(f"Raw trial results: {args.output}")


if __name__ == "__main__":
    main()
