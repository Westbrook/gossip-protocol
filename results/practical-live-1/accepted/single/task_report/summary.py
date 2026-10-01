"""Aggregate parser-valid tasks without modifying them."""

from __future__ import annotations


def summarize(tasks: list[dict]) -> dict:
    counts = {"todo": 0, "doing": 0, "done": 0}
    total_estimate = 0
    remaining_estimate = 0

    for task in tasks:
        status = task["status"]
        estimate = task["estimate"]
        counts[status] += 1
        total_estimate += estimate
        if status in ("todo", "doing"):
            remaining_estimate += estimate

    return {
        "counts": counts,
        "total_estimate": total_estimate,
        "remaining_estimate": remaining_estimate,
    }
