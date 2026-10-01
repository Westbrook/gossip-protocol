"""Data-only, post-freeze calendar database handoff scenarios.

Use the actual selected source version for each step and ONE database per case.
The calendar CLI receives database path and JSON command as positional arguments.
Do not call solution.solve: that adapter intentionally creates a fresh database.
These scenarios are supplemental diagnostics, never primary-score replacements.
"""
from copy import deepcopy

PROJECT_ID = "calendar"
CLASSIFICATION = "Post-freeze supplementary same-database source-version handoff diagnostic"
CLI = {"relative_path": "calendar_app/cli.py", "arguments": ["database_path", "json_command"], "stdin": "unused"}


def _step(stage_index, command):
    return {"stage_index": stage_index, "command": command}


def _row(rid, resource, user, start, end, *, status="booked", token=0, expires=None):
    return {"rid": rid, "resource": resource, "user": user, "start": start,
            "end": end, "status": status, "token": token, "expires": expires}


def _snapshot(*rows, waiting=()):
    return {"version": 2, "bookings": sorted(deepcopy(list(rows)), key=lambda row: row["rid"]),
            "waiting": deepcopy(list(waiting))}


_OK = {"ok": True}
_OLD_COMMAND = {"op": "book", "key": "book-old", "rid": "old", "resource": "room",
                "user": "owner", "start": 0, "end": 20}
_NEW_COMMAND = {"op": "book", "key": "book-new", "rid": "new", "resource": "free",
                "user": "owner", "start": 2, "end": 4}
_OLD = _row("old", "room", "owner", 0, 20)
_MOVED = _row("old", "room", "owner", 5, 25)
_SIDE = _row("side", "aux", "owner", 0, 4)
_THIRD = _row("third", "annex", "owner", 2, 5)
_NEW = _row("new", "free", "owner", 2, 4)

_GRANDFATHERED = {
    "id": "calendar-handoff-grandfathered-booking-and-keys",
    "requirements": ["restart", "idempotency", "reschedule", "policy"],
    "classification": CLASSIFICATION,
    "contract_basis": [
        "COMMON: requests and booking state are durable across CLI processes; exact successful-key retries return the original response despite later state changes.",
        "Milestones 1 and 2 preserve working behavior and add moves; policy.json is explicitly dormant before milestone 4, so a 20-unit interval and three active same-user bookings are valid when created.",
        "Milestone 4 explicitly grandfathers existing rows, revalidates moves, and limits only new active bookings. Reads and cancellation must preserve or deliberately close earlier rows without deleting history.",
        "Failure does not consume a request key: a new booking refused at the user cap can succeed with the identical key/body after cancellations free capacity.",
        "This extends the fixed study's per-command persistence to actual candidate source-version handoff. It does not imply every implementation needs a schema migration; stable compatible schemas can pass unchanged.",
    ],
    "steps": [
        _step(0, deepcopy(_OLD_COMMAND)),
        _step(0, {"op": "snapshot"}),
        _step(1, {"op": "move", "key": "move-old", "rid": "old", "start": 5, "end": 25}),
        _step(1, {"op": "book", "key": "book-side", "rid": "side", "resource": "aux", "user": "owner", "start": 0, "end": 4}),
        _step(1, {"op": "snapshot"}),
        _step(2, {"op": "book", "key": "book-third", "rid": "third", "resource": "annex", "user": "owner", "start": 2, "end": 5}),
        _step(2, {"op": "snapshot"}),
        _step(3, {"op": "snapshot"}),
        _step(3, deepcopy(_OLD_COMMAND)),
        _step(3, {"op": "move", "key": "move-policy", "rid": "old", "start": 5, "end": 25}),
        _step(3, deepcopy(_NEW_COMMAND)),
        _step(3, {"op": "cancel", "key": "cancel-side", "rid": "side"}),
        _step(3, {"op": "cancel", "key": "cancel-third", "rid": "third"}),
        _step(3, deepcopy(_NEW_COMMAND)),
        _step(3, {"op": "snapshot"}),
    ],
    "expected_outputs": [
        deepcopy(_OK), _snapshot(_OLD), deepcopy(_OK), deepcopy(_OK), _snapshot(_MOVED, _SIDE),
        deepcopy(_OK), _snapshot(_MOVED, _SIDE, _THIRD), _snapshot(_MOVED, _SIDE, _THIRD),
        deepcopy(_OK), {"error": "policy"}, {"error": "policy"}, deepcopy(_OK), deepcopy(_OK),
        deepcopy(_OK), _snapshot(_MOVED, _NEW, dict(_SIDE, status="cancelled"), dict(_THIRD, status="cancelled")),
    ],
}

_ANCHOR = _row("anchor", "anchor-room", "owner", 0, 4, status="cancelled")
_A = _row("book-a", "room", "owner", 2, 6, status="cancelled")
_B = _row("book-b", "other", "visitor", 2, 6)
_HELD = _row("held", "room", "owner", 1, 21, status="held", token=2, expires=6)
_RENEW_COMMAND = {"op": "renew", "key": "renew-held", "rid": "held", "token": 1, "now": 1, "ttl": 5}
_QUEUED = _row("queued", "room", "visitor", 1, 6)
_WAITING = {"seq": 1, "rid": "queued", "resource": "room", "user": "visitor", "start": 1, "end": 6, "status": "admitted"}

_FENCING = {
    "id": "calendar-handoff-live-hold-fencing-and-admission",
    "requirements": ["restart", "bundle-atomicity", "fencing", "idempotency", "policy", "waitlist"],
    "classification": CLASSIFICATION,
    "contract_basis": [
        "Milestone 2 bundles and cancellation preserve earlier rows. This diagnostic additionally assumes that each cumulative source version can operate on the preceding version's actual SQLite file, as disclosed under extra_assumptions.",
        "Milestone 3 renewal increments the fencing token and stores its exact response under the successful request key. An exact retry must replay token 2 rather than renew again or reject the old token.",
        "Milestone 4 explicitly grandfathers existing held rows and says confirm/renew do not revalidate policy. A pre-policy 20-unit live hold can therefore be confirmed after the duration limit becomes 12.",
        "The old token must fail stale; the current token confirms the same booking while clearing expiry. Enqueue reserves no capacity, admission skips conflicts, and cancellation frees the grandfathered interval for an explicit later admit.",
        "No audit command is asserted: early milestones did not unambiguously require reconstructable audit records before the audit feature was introduced.",
    ],
    "steps": [
        _step(0, {"op": "book", "key": "book-anchor", "rid": "anchor", "resource": "anchor-room", "user": "owner", "start": 0, "end": 4}),
        _step(0, {"op": "cancel", "key": "cancel-anchor", "rid": "anchor"}),
        _step(1, {"op": "bundle", "key": "bundle-books", "items": [
            {"rid": "book-a", "resource": "room", "user": "owner", "start": 2, "end": 6},
            {"rid": "book-b", "resource": "other", "user": "visitor", "start": 2, "end": 6},
        ]}),
        _step(1, {"op": "cancel", "key": "cancel-book-a", "rid": "book-a"}),
        _step(2, {"op": "hold", "key": "hold-book", "rid": "held", "resource": "room", "user": "owner", "start": 1, "end": 21, "now": 0, "ttl": 5}),
        _step(2, deepcopy(_RENEW_COMMAND)),
        _step(2, {"op": "snapshot"}),
        _step(3, {"op": "snapshot"}),
        _step(3, deepcopy(_RENEW_COMMAND)),
        _step(3, {"op": "confirm", "key": "confirm-old", "rid": "held", "token": 1, "now": 2}),
        _step(3, {"op": "confirm", "key": "confirm-current", "rid": "held", "token": 2, "now": 2}),
        _step(3, {"op": "enqueue", "key": "queue-next", "rid": "queued", "resource": "room", "user": "visitor", "start": 1, "end": 6}),
        _step(3, {"op": "admit", "key": "admit-blocked"}),
        _step(3, {"op": "cancel", "key": "cancel-held", "rid": "held"}),
        _step(3, {"op": "admit", "key": "admit-now"}),
        _step(3, {"op": "snapshot"}),
    ],
    "expected_outputs": [
        deepcopy(_OK), deepcopy(_OK), deepcopy(_OK), deepcopy(_OK), {"ok": True, "token": 1},
        {"ok": True, "token": 2}, _snapshot(_ANCHOR, _A, _B, _HELD), _snapshot(_ANCHOR, _A, _B, _HELD),
        {"ok": True, "token": 2}, {"error": "stale"}, deepcopy(_OK), deepcopy(_OK),
        {"ok": True, "admitted": None}, deepcopy(_OK), {"ok": True, "admitted": "queued"},
        _snapshot(_ANCHOR, _A, _B, dict(_HELD, status="cancelled", expires=None), _QUEUED, waiting=[_WAITING]),
    ],
}

_ASSUMPTIONS = {
    "required_contract": [
        "Successful request receipts and booking state are durable across fresh CLI processes; exact retries and typed state errors follow the written contracts.",
        "Milestone-four policy explicitly grandfathers existing booked and held rows, while moves and new bookings must satisfy the new limits.",
        "Each later milestone preserves earlier specified behavior; closed rows and current hold tokens remain observable through snapshots and responses.",
    ],
    "extra_assumptions": [
        "Accepted milestone versions are treated as sequential releases of one persistent application and are expected to consume the preceding version's actual database representation. The primary study reset each scenario database and did not explicitly require arbitrary candidate-created schema compatibility.",
        "The handoff occurs after a command has exited successfully, with automatic startup migration if needed and no undocumented separate migration command. This is an additional operational diagnostic boundary, not a retroactive primary acceptance rule.",
    ],
    "failure_interpretation": "A failure establishes same-database handoff incompatibility under these assumptions. Inspect the failing step and source before classifying it as a direct written-contract bug; do not relabel primary results.",
}
for _case in (_GRANDFATHERED, _FENCING):
    _case["required_contract_vs_extra_assumptions"] = deepcopy(_ASSUMPTIONS)

CASES = (deepcopy(_GRANDFATHERED), deepcopy(_FENCING))
LIMITATIONS = (
    "These are two targeted post-freeze stories, not additional primary acceptance cases or an unbiased estimate of policy quality.",
    "Every story starts with an empty database created by stage-zero code; no golden legacy seed, schema inspection, SQL rewrite, or database reset is permitted during the story.",
    "One fresh CLI subprocess is used per step, with the indicated immutable source directory and its preserved policy.json; all steps in one story share a database.",
    "A passing result establishes compatibility for these exercised paths; it does not prove arbitrary migrations, concurrent access, crash recovery, or all historical database shapes.",
    "Earlier audit-history reconstruction is not assessed because that cross-version requirement is ambiguous.",
)
