"""Frozen, synthetic JSON function tasks for the many-candidate pilot.

Public examples are worker inputs. Hidden cases are final evaluation only. Fuzz
cases and the host-side oracle are selection aids: any experiment using either
must be described as oracle-assisted. No candidate code is executed here.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
import hashlib
import json
from pathlib import Path
import random
from textwrap import dedent
from typing import Any, Callable

FIXTURE_VERSION = "swarm-json-v1"
STUB = 'def solve(payload):\n    raise NotImplementedError("Implement the supplied contract")\n'
COMMON_SPEC = """Implement solution.py with solve(payload), returning the specified JSON value.
Use Python 3.12 and its standard library only. Do not read files, use a network,
print output, or depend on external state. All evaluated inputs obey the bounds
below; invalid-input behavior is not evaluated. JSON integers exclude booleans.
Object key order is irrelevant; array order and JSON value types are significant.
Inputs contain only objects, arrays, strings, integers, booleans and null (no
floats). Maximum nesting is 8, total JSON nodes 500, string length 256, array or
object size 64, and absolute integer value 10000 unless a tighter bound follows.
Compact ASCII-escaped JSON (no spaces between tokens) is at most 8192 bytes for
the input and 32768 bytes for its required output. These are input-domain bounds;
inputs whose correct expansion would exceed the output bound are not evaluated.
"""


def _json(value: Any, depth: int = 0, budget: list[int] | None = None) -> None:
    budget = [500] if budget is None else budget
    budget[0] -= 1
    if budget[0] < 0 or depth > 8:
        raise ValueError("JSON size bound")
    if value is None or type(value) is bool:
        return
    if type(value) is int and abs(value) <= 10000:
        return
    if type(value) is str and len(value) <= 256:
        return
    if type(value) is list and len(value) <= 64:
        for item in value:
            _json(item, depth + 1, budget)
        return
    if type(value) is dict and len(value) <= 64 and all(type(k) is str and len(k) <= 256 for k in value):
        for item in value.values():
            _json(item, depth + 1, budget)
        return
    raise ValueError("Unsupported JSON value or bound")


def _keys(value: Any, keys: set[str]) -> None:
    if type(value) is not dict or set(value) != keys:
        raise ValueError("Incorrect object fields")


def _integer(value: Any, low: int, high: int) -> None:
    if type(value) is not int or not low <= value <= high:
        raise ValueError("Integer bound")


def _intervals(value: Any) -> None:
    if type(value) is not list or len(value) > 32:
        raise ValueError("Intervals must be a bounded list")
    for pair in value:
        if type(pair) is not list or len(pair) != 2:
            raise ValueError("Interval shape")
        _integer(pair[0], -30, 30)
        _integer(pair[1], -30, 30)
        if pair[0] > pair[1]:
            raise ValueError("Reversed interval")


def _runs(cells: dict[int, int], *, weighted: bool = False) -> list[list[int]]:
    result: list[list[int]] = []
    for point in sorted(cells):
        value = cells[point]
        if result and result[-1][1] == point and (not weighted or result[-1][2] == value):
            result[-1][1] = point + 1
        else:
            result.append([point, point + 1, value] if weighted else [point, point + 1])
    return result


@dataclass(frozen=True)
class SwarmTask:
    id: str
    family: str
    spec: str
    requirements: tuple[str, ...]
    initial_files: dict[str, str]
    known_solution: str
    public_cases: tuple[dict[str, Any], ...]
    hidden_cases: tuple[dict[str, Any], ...]
    fuzz_cases: tuple[dict[str, Any], ...]
    _oracle: Callable[[Any], Any] = field(repr=False, compare=False)
    mutants: dict[str, str] = field(default_factory=dict, repr=False, compare=False)

    def oracle(self, payload: Any) -> Any:
        """Validate a proposed input, then compute a trusted expected output."""
        _json(payload)
        if len(_canonical(payload).encode("ascii")) > 8192:
            raise ValueError("Serialized input bound")
        try:
            answer = self._oracle(deepcopy(payload))
            if len(_canonical(answer).encode("ascii")) > 32768:
                raise ValueError("Serialized output bound")
            return answer
        except (KeyError, IndexError, TypeError, AttributeError, OverflowError) as exc:
            raise ValueError("Invalid task input") from exc

    def public_manifest(self) -> dict[str, Any]:
        return deepcopy({"id": self.id, "family": self.family, "spec": self.spec,
                         "requirements": list(self.requirements), "initial_files": self.initial_files,
                         "public_cases": list(self.public_cases)})


def _merge(payload):
    _keys(payload, {"layers"})
    if type(payload["layers"]) is not list or len(payload["layers"]) > 16 or any(type(x) is not dict for x in payload["layers"]):
        raise ValueError("Layers must be objects")
    answer = {}
    for layer in payload["layers"]:
        pending = [(answer, layer)]
        while pending:
            target, source = pending.pop()
            for key, value in source.items():
                if value is None:
                    target.pop(key, None)
                elif type(value) is dict:
                    if type(target.get(key)) is not dict:
                        target[key] = {}
                    pending.append((target[key], value))
                else:
                    target[key] = deepcopy(value)
    return answer


MERGE_SOLUTION = dedent('''\
    from copy import deepcopy
    def solve(payload):
        def merge(old, new):
            out = deepcopy(old) if isinstance(old, dict) else {}
            for key, value in new.items():
                if value is None:
                    out.pop(key, None)
                elif isinstance(value, dict):
                    out[key] = merge(out.get(key), value)
                else:
                    out[key] = deepcopy(value)
            return out
        result = {}
        for layer in payload["layers"]:
            result = merge(result, layer)
        return result
''')
MERGE_SPEC = """Input: {"layers": [object, ...]}, at most 16 layers. Return the merged object.
C1: Apply layers left to right. A non-object value replaces the prior value.
C2: Object values merge recursively; an object replacing a non-object starts empty.
C3: Null at an object key deletes that key, including while creating new nested
objects. Deleting an absent key is harmless. Retain empty objects after deletion.
C4: Arrays replace wholesale, not elementwise. Their contents (including nulls
and objects containing nulls) are ordinary data and are not recursively merged.
"""


def _pointer_tokens(path):
    if type(path) is not str or not path.startswith("/"):
        raise ValueError("Nonempty JSON pointer required")
    parts = path[1:].split("/")
    result = []
    for part in parts:
        cursor, text = 0, ""
        while cursor < len(part):
            if part[cursor] == "~":
                if cursor + 1 == len(part) or part[cursor + 1] not in "01":
                    raise ValueError("Invalid pointer escape")
                text += "~" if part[cursor + 1] == "0" else "/"
                cursor += 2
            else:
                text += part[cursor]
                cursor += 1
        result.append(text)
    return result


def _index(token, size, append=False):
    if token == "-" and append:
        return size
    if not token or any(c not in "0123456789" for c in token) or (len(token) > 1 and token[0] == "0"):
        raise ValueError("Invalid array index")
    index = int(token)
    if not 0 <= index < size + int(append):
        raise ValueError("Array index out of range")
    return index


def _pointers(payload):
    _keys(payload, {"document", "operations"})
    if type(payload["document"]) not in (dict, list) or type(payload["operations"]) is not list or len(payload["operations"]) > 20:
        raise ValueError("Pointer input shape")
    document = deepcopy(payload["document"])
    for operation in payload["operations"]:
        if type(operation) is not dict or operation.get("op") not in ("add", "replace", "remove"):
            raise ValueError("Unknown operation")
        op = operation["op"]
        _keys(operation, {"op", "path"} if op == "remove" else {"op", "path", "value"})
        parts = _pointer_tokens(operation["path"])
        target = document
        for key in parts[:-1]:
            if type(target) is list:
                target = target[_index(key, len(target))]
            elif type(target) is dict and key in target:
                target = target[key]
            else:
                raise ValueError("Missing parent")
        key = parts[-1]
        if type(target) is list:
            offset = _index(key, len(target), op == "add")
            if op == "add":
                target[offset:offset] = [deepcopy(operation["value"])]
            elif op == "remove":
                del target[offset]
            else:
                target[offset] = deepcopy(operation["value"])
        elif type(target) is dict:
            if op != "add" and key not in target:
                raise ValueError("Missing object target")
            if op == "remove":
                del target[key]
            else:
                target[key] = deepcopy(operation["value"])
        else:
            raise ValueError("Non-container parent")
        _json(document)
    return document


POINTER_SOLUTION = dedent('''\
    from copy import deepcopy
    def solve(payload):
        doc = deepcopy(payload["document"])
        for change in payload["operations"]:
            path = [x.replace("~1", "/").replace("~0", "~") for x in change["path"][1:].split("/")]
            node = doc
            for key in path[:-1]:
                node = node[int(key)] if isinstance(node, list) else node[key]
            key, op = path[-1], change["op"]
            if isinstance(node, list):
                index = len(node) if key == "-" else int(key)
                if op == "add":
                    node.insert(index, deepcopy(change["value"]))
                elif op == "replace":
                    node[index] = deepcopy(change["value"])
                else:
                    node.pop(index)
            elif op == "remove":
                del node[key]
            else:
                node[key] = deepcopy(change["value"])
        return doc
''')
POINTER_SPEC = """Input: {"document": object-or-array, "operations": [operation, ...]}, at most 20 operations.
Return the resulting document. Inputs guarantee each operation is valid when applied.
P1: Operations run in order. Each has op (add|replace|remove) and path; add and
replace also have value. Paths are nonempty JSON pointers starting with '/';
empty path/root replacement is excluded. Split at '/', decode '~1' to '/' and
'~0' to '~' once, without reinterpreting the decoded text. Empty object keys work.
P2: Every parent already exists. On objects add inserts or overwrites; replace
and remove require the target key to exist. Object keys remain strings.
P3: On arrays, add inserts before index 0..len, shifting later items; '-' appends.
Replace/remove use indices 0..len-1. Array indices are canonical ASCII decimal
strings (0 or no leading zero). '-' is allowed only for array add.
P4: Values are copied as complete JSON values, including null. No other fields
occur. The input and every intermediate document obey the common JSON bounds.
"""


def _tokens(text):
    tokens, literal, i = [], "", 0
    while i < len(text):
        if text.startswith("$$", i):
            literal += "$"
            i += 2
        elif text.startswith("${", i):
            end = text.find("}", i + 2)
            name = text[i + 2:end] if end >= 0 else ""
            if name and all(c in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_" for c in name) and name[0] in "ABCDEFGHIJKLMNOPQRSTUVWXYZ_":
                if literal:
                    tokens.append((False, literal))
                    literal = ""
                tokens.append((True, name))
                i = end + 1
            else:
                literal += text[i]
                i += 1
        else:
            literal += text[i]
            i += 1
    if literal:
        tokens.append((False, literal))
    return tokens


def _expand(payload):
    _keys(payload, {"values"})
    values = payload["values"]
    if type(values) is not dict or len(values) > 12:
        raise ValueError("Values object bound")
    for name, value in values.items():
        if not name or name[0] not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ_" or any(c not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_" for c in name) or type(value) is not str or len(value) > 64:
            raise ValueError("Name/string format")
    parsed = {name: _tokens(value) for name, value in values.items()}
    answer = {}
    while True:
        changed = False
        for name, tokens in parsed.items():
            if name not in answer and all(not reference or part in answer for reference, part in tokens):
                answer[name] = "".join(answer[part] if reference else part for reference, part in tokens)
                if len(answer[name]) > 4096:
                    raise ValueError("Expanded value bound")
                changed = True
        if not changed:
            break
    return {"resolved": answer, "unresolved": sorted(set(values) - set(answer))}


EXPAND_SOLUTION = dedent('''\
    import re
    def solve(payload):
        values, done, bad, active = payload["values"], {}, set(), set()
        pattern = re.compile(r"\\$\\$|\\$\\{([A-Z_][A-Z0-9_]*)\\}")
        def resolve(name):
            if name in done:
                return done[name]
            if name not in values or name in active or name in bad:
                raise ValueError("unresolved")
            active.add(name)
            try:
                # Matches are chosen from the original string only. Inserted values
                # and dollar-escaped text are never scanned for references again.
                result = pattern.sub(lambda match: "$" if match.group(0) == "$$" else resolve(match.group(1)), values[name])
            except ValueError:
                bad.add(name)
                raise
            finally:
                active.remove(name)
            done[name] = result
            return result
        for name in values:
            try:
                resolve(name)
            except ValueError:
                pass
        return {"resolved": done, "unresolved": sorted(set(values) - set(done))}
''')
EXPAND_SPEC = """Input: {"values": {NAME: string, ...}}, at most 12 names, source strings at most
64 characters. Names match ASCII [A-Z_][A-Z0-9_]*. Return exactly
{"resolved": {name: expanded-string, ...}, "unresolved": [name, ...]}.
E1: Scan each original source left to right. '$$' emits one literal '$' and
consumes both characters. Otherwise '${NAME}' expands the referenced name.
If neither token matches at the current position, emit that one character
literally and advance one character; malformed outer text does not hide later
valid tokens inside it.
E2: Referenced values are recursively expanded, but inserted strings and escaped
text are not scanned again. Thus '$${A}' produces literal '${A}'. Empty strings
are resolved values. Successfully expanded strings are at most 4096 characters.
E3: A name is unresolved if it depends directly or transitively on a missing name
or a cycle, including a self-cycle. Omit the entire unresolved value; include all
other names. Cyclic names remain unresolved even if their literal parts are empty.
E4: unresolved lists defined unresolved names only, in lexicographic order;
missing external names are not additional output entries. Object order is irrelevant.
"""


def _union(payload):
    _keys(payload, {"intervals", "window"})
    _intervals(payload["intervals"])
    _intervals([payload["window"]])
    lo, hi = payload["window"]
    covered = {x: 1 for x in range(lo, hi) if any(a <= x < b for a, b in payload["intervals"])}
    return {"intervals": _runs(covered), "gaps": _runs({x: 1 for x in range(lo, hi) if x not in covered}), "total": len(covered)}


UNION_SOLUTION = dedent('''\
    def solve(payload):
        lo, hi = payload["window"]
        merged = []
        for a, b in sorted((max(a, lo), min(b, hi)) for a, b in payload["intervals"]):
            if a >= b:
                continue
            if merged and a <= merged[-1][1]:
                merged[-1][1] = max(merged[-1][1], b)
            else:
                merged.append([a, b])
        cursor, gaps = lo, []
        for a, b in merged:
            if cursor < a:
                gaps.append([cursor, a])
            cursor = b
        if cursor < hi:
            gaps.append([cursor, hi])
        return {"intervals": merged, "gaps": gaps, "total": sum(b-a for a,b in merged)}
''')
UNION_SPEC = """Input: {"intervals": [[start,end],...], "window": [lo,hi]}. Every endpoint
is an integer in [-30,30], start<=end, lo<=hi, and there are at most 32 intervals.
U1: All intervals are half-open [start,end). Clip them to the window and discard
empty intersections. Duplicates and input order do not change the covered set.
U2: Return intervals as sorted maximal covered intervals; merge overlap AND
adjacency. Never emit empty intervals.
U3: Return gaps as sorted maximal uncovered intervals inside the window, using
the same half-open convention. An empty window has no intervals or gaps.
U4: Return exactly {"intervals": [...], "gaps": [...], "total": integer}, where
total is the union's covered length, counting overlap only once.
"""


def _load(payload):
    _keys(payload, {"bookings"})
    bookings = payload["bookings"]
    if type(bookings) is not list or len(bookings) > 32:
        raise ValueError("Bookings bound")
    for row in bookings:
        if type(row) is not list or len(row) != 3:
            raise ValueError("Booking shape")
        _intervals([row[:2]])
        _integer(row[2], 1, 9)
    cells = {x: sum(w for a, b, w in bookings if a <= x < b) for x in range(-30, 30)}
    positive = {x: value for x, value in cells.items() if value}
    return {"segments": _runs(positive, weighted=True), "peak": max(cells.values(), default=0), "work": sum(cells.values())}


LOAD_SOLUTION = dedent('''\
    def solve(payload):
        changes = {}
        for a, b, weight in payload["bookings"]:
            if a < b:
                changes[a] = changes.get(a, 0) + weight
                changes[b] = changes.get(b, 0) - weight
        points, active, segments = sorted(changes), 0, []
        for index, point in enumerate(points[:-1]):
            active += changes[point]
            end = points[index+1]
            if active:
                if segments and segments[-1][1] == point and segments[-1][2] == active:
                    segments[-1][1] = end
                else:
                    segments.append([point, end, active])
        return {"segments": segments, "peak": max((x[2] for x in segments), default=0), "work": sum((b-a)*w for a,b,w in segments)}
''')
LOAD_SPEC = """Input: {"bookings": [[start,end,weight], ...]}, at most 32 bookings. Endpoints
are integers in [-30,30], start<=end, weight is an integer in [1,9].
L1: Bookings occupy half-open intervals. Add weights for simultaneous bookings;
zero-length bookings contribute nothing, and duplicates each contribute.
L2: Return segments as sorted nonempty [start,end,load] triples with positive
constant total load. Coalesce adjacent segments when their total load is equal,
even if the set of contributing bookings changes. Omit zero-load gaps.
L3: Return peak as the largest simultaneous load, or integer zero if none.
L4: Return exactly {"segments": [...], "peak": integer, "work": integer}; work
is the sum of duration*weight of every booking, equivalently area under load.
"""


def _slot(payload):
    _keys(payload, {"window", "blocked", "duration", "not_before"})
    _intervals([payload["window"]])
    _intervals(payload["blocked"])
    _integer(payload["duration"], 1, 20)
    _integer(payload["not_before"], -30, 30)
    lo, hi = payload["window"]
    free = {x: 1 for x in range(lo, hi) if not any(a <= x < b for a, b in payload["blocked"])}
    options = [x for x in range(max(lo, payload["not_before"]), hi + 1) if all(y in free for y in range(x, x + payload["duration"]))]
    return {"start": min(options) if options else None, "free": _runs(free)}


SLOT_SOLUTION = dedent('''\
    def solve(payload):
        lo, hi = payload["window"]
        cursor, free = lo, []
        for a, b in sorted(payload["blocked"]):
            a, b = max(lo, a), min(hi, b)
            if a >= b:
                continue
            if cursor < a:
                free.append([cursor, a])
            cursor = max(cursor, b)
        if cursor < hi:
            free.append([cursor, hi])
        start = None
        for a, b in free:
            candidate = max(a, payload["not_before"])
            if candidate + payload["duration"] <= b:
                start = candidate
                break
        return {"start": start, "free": free}
''')
SLOT_SPEC = """Input: {"window": [lo,hi], "blocked": [[start,end],...], "duration": integer,
"not_before": integer}. Endpoints and not_before are in [-30,30], each start<=end
and lo<=hi; at most 32 blocked intervals. duration is an integer in [1,20].
S1: Interpret intervals as half-open. Clip blocked intervals to the window;
discard empty ones and combine overlap/adjacency, regardless of input order.
S2: Return free as all sorted maximal nonempty unblocked intervals in the full
window. Do not trim this list at not_before and do not remove short free gaps.
S3: Return start as the earliest integer x>=not_before where [x,x+duration) fits
entirely in a free interval. Finishing exactly at its right endpoint is allowed.
S4: Return exactly {"start": integer-or-null, "free": [...]}. If no fit exists,
start is null. A zero-length window has no free intervals and no fit.
"""


def _transfers(payload):
    _keys(payload, {"balances", "events"})
    balances, events = payload["balances"], payload["events"]
    if type(balances) is not dict or not 1 <= len(balances) <= 12 or type(events) is not list or len(events) > 32:
        raise ValueError("Ledger shape")
    for name, balance in balances.items():
        if not name or len(name) > 16:
            raise ValueError("Account name")
        _integer(balance, 0, 100)
    accepted, rejected, duplicates, seen = [], [], [], set()
    for event in events:
        _keys(event, {"id", "from", "to", "amount"})
        if type(event["id"]) is not str or not 1 <= len(event["id"]) <= 16 or event["from"] not in balances or event["to"] not in balances:
            raise ValueError("Event identity/account")
        _integer(event["amount"], 0, 100)
        identity = event["id"]
        if identity in seen:
            duplicates.append(identity)
            continue
        seen.add(identity)
        if balances[event["from"]] < event["amount"]:
            rejected.append(identity)
        else:
            accepted.append(identity)
            balances[event["from"]] -= event["amount"]
            balances[event["to"]] += event["amount"]
    return {"balances": balances, "accepted": accepted, "rejected": rejected, "duplicates": duplicates}


TRANSFER_SOLUTION = dedent('''\
    def solve(payload):
        balances = dict(payload["balances"])
        seen, accepted, rejected, duplicates = set(), [], [], []
        for event in payload["events"]:
            identity, source, target, amount = event["id"], event["from"], event["to"], event["amount"]
            if identity in seen:
                duplicates.append(identity)
                continue
            seen.add(identity)
            if balances[source] < amount:
                rejected.append(identity)
                continue
            balances[source] = balances[source] - amount
            balances[target] = balances[target] + amount
            accepted.append(identity)
        return dict(balances=balances, accepted=accepted, rejected=rejected, duplicates=duplicates)
''')
TRANSFER_SPEC = """Input: {"balances": {account: integer,...}, "events": [event,...]}. There
are 1..12 nonempty account names of at most 16 characters, with initial balances
0..100, and at most 32 events. An event has exactly id, from, to, amount; id is a
nonempty string of at most 16 characters, from/to are existing accounts, amount
is an integer 0..100. All event objects are valid, including duplicate IDs.
T1: Process events in input order. The first occurrence of an ID consumes that
ID even if rejected; all later occurrences are duplicates regardless of payload.
T2: Reject a first event if the source has less than amount. Rejection changes
no balance. Otherwise atomically subtract amount from source and add to target.
T3: Zero amounts succeed. A self-transfer succeeds only with sufficient balance
and then leaves that balance unchanged. Total funds are conserved.
T4: Return exactly {"balances": final-account-object, "accepted": [ids],
"rejected": [ids], "duplicates": [ids]}. Each list preserves event occurrence
order; duplicates includes every repeated occurrence, not merely unique IDs.
"""


def _versions(payload):
    _keys(payload, {"events"})
    if type(payload["events"]) is not list or len(payload["events"]) > 32:
        raise ValueError("Events bound")
    first, seen, duplicate, stale, max_seen = [], set(), [], [], {}
    for index, event in enumerate(payload["events"]):
        if type(event) is not dict or event.get("op") not in ("set", "delete"):
            raise ValueError("Event operation")
        _keys(event, {"id", "key", "version", "op", "value"} if event["op"] == "set" else {"id", "key", "version", "op"})
        if any(type(event[name]) is not str or not 1 <= len(event[name]) <= 16 for name in ("id", "key")):
            raise ValueError("Event identity/key")
        _integer(event["version"], 0, 100)
        identity, key, version = event["id"], event["key"], event["version"]
        if identity in seen:
            duplicate.append(identity)
            continue
        seen.add(identity)
        first.append((index, event))
        if key in max_seen and version <= max_seen[key]:
            stale.append(identity)
        else:
            max_seen[key] = version
    winners = {}
    for index, event in sorted(first, key=lambda pair: (-pair[1]["version"], pair[0])):
        winners.setdefault(event["key"], event)
    return {"state": {key: deepcopy(event["value"]) for key, event in winners.items() if event["op"] == "set"},
            "versions": {key: event["version"] for key, event in winners.items()}, "stale": stale, "duplicates": duplicate}


VERSION_SOLUTION = dedent('''\
    from copy import deepcopy
    def solve(payload):
        seen, state, versions, stale, duplicates = set(), {}, {}, [], []
        for event in payload["events"]:
            identity, key, version = event["id"], event["key"], event["version"]
            if identity in seen:
                duplicates.append(identity)
                continue
            seen.add(identity)
            if key in versions and version <= versions[key]:
                stale.append(identity)
                continue
            versions[key] = version
            if event["op"] == "delete":
                state.pop(key, None)
            else:
                state[key] = deepcopy(event["value"])
        return dict(state=state, versions=versions, stale=stale, duplicates=duplicates)
''')
VERSION_SPEC = """Input: {"events": [event,...]}, at most 32 events. Each event has id, key,
version, op. id/key are nonempty strings of at most 16 characters; version is an
integer in [0,100]; op is set|delete. Set events additionally have value (any
bounded JSON value, including null); delete events have no value field.
V1: Process in input order, ignoring every repeated id after its first occurrence,
even if the first event was stale or the duplicate has a different key/version.
V2: Keep a current version per key. A first event applies only if its version is
strictly greater than that key's current version (or no version exists). Equal
versions are stale, so earlier input wins ties. Version zero is a valid first version.
V3: Applied set stores value; applied delete removes the key from state but keeps
its version as a tombstone. A later strictly newer set can recreate it. A null set
value is stored as null and is distinct from deleting. Delete of absent key applies.
V4: Return exactly {"state": live-value-object, "versions": version-object,
"stale": [ids], "duplicates": [ids]}. Lists preserve occurrence order; record
staleness when encountered, never retroactively when a newer event replaces it.
"""


def _case(payload, expected, requirement):
    return {"input": payload, "expected": expected, "requirement": requirement}


def _transfer(identity, source="a", target="b", amount=1):
    return {"id": identity, "from": source, "to": target, "amount": amount}


def _version(identity, key="a", version=0, value=0, delete=False):
    row = {"id": identity, "key": key, "version": version, "op": "delete" if delete else "set"}
    if not delete:
        row["value"] = value
    return row


# Hand-computed examples anchor the reference implementations. Seeded cases below
# expand coverage using separate seeds and disjoint serialized inputs.
DEFINITIONS = [
    ("config-merge", "config", "C", MERGE_SPEC, _merge, MERGE_SOLUTION,
     [_case({"layers": []}, {}, "C1"),
      _case({"layers": [{"x": 1}, {"x": 2, "y": 3}]}, {"x": 2, "y": 3}, "C1"),
      _case({"layers": [{"a": {"x": 1, "y": 2}}, {"a": {"x": None}}]}, {"a": {"y": 2}}, "C3")],
     [_case({"layers": [{"a": [1, 2]}, {"a": [None, {"x": None}]}]}, {"a": [None, {"x": None}]}, "C4"),
      _case({"layers": [{"a": 5}, {"a": {"x": None, "z": 0}}]}, {"a": {"z": 0}}, "C2"),
      _case({"layers": [{"a": {"x": 1}}, {"a": {"x": None}}]}, {"a": {}}, "C3"),
      _case({"layers": [{"a": False}, {"a": {}}, {"b": None}]}, {"a": {}}, "C2"),
      _case({"layers": [{"a": {"x": 1}}, {"a": None}, {"a": {"y": 2}}]}, {"a": {"y": 2}}, "C3")]),
    ("config-pointers", "config", "P", POINTER_SPEC, _pointers, POINTER_SOLUTION,
     [_case({"document": {"x": 1}, "operations": []}, {"x": 1}, "P1"),
      _case({"document": [1, 3], "operations": [{"op": "add", "path": "/1", "value": 2}]}, [1, 2, 3], "P3"),
      _case({"document": {"a/b": 1}, "operations": [{"op": "replace", "path": "/a~1b", "value": None}]}, {"a/b": None}, "P1")],
     [_case({"document": {"~1": 1, "/": 2}, "operations": [{"op": "remove", "path": "/~01"}]}, {"/": 2}, "P1"),
      _case({"document": {"": {"": 1}}, "operations": [{"op": "replace", "path": "//", "value": 4}]}, {"": {"": 4}}, "P1"),
      _case({"document": [1], "operations": [{"op": "add", "path": "/-", "value": [2]}, {"op": "add", "path": "/0", "value": 0}, {"op": "remove", "path": "/1"}]}, [0, [2]], "P3"),
      _case({"document": {"0": "old"}, "operations": [{"op": "add", "path": "/0", "value": {"a": None}}]}, {"0": {"a": None}}, "P2"),
      _case({"document": {"a": [1, 2]}, "operations": [{"op": "remove", "path": "/a/0"}, {"op": "replace", "path": "/a/0", "value": False}]}, {"a": [False]}, "P4")]),
    ("config-expand", "config", "E", EXPAND_SPEC, _expand, EXPAND_SOLUTION,
     [_case({"values": {}}, {"resolved": {}, "unresolved": []}, "E4"),
      _case({"values": {"A": "hi", "B": "${A}!"}}, {"resolved": {"A": "hi", "B": "hi!"}, "unresolved": []}, "E1"),
      _case({"values": {"A": "${MISSING}", "B": "ok"}}, {"resolved": {"B": "ok"}, "unresolved": ["A"]}, "E3")],
     [_case({"values": {"A": "$${MISSING}", "B": "${A}", "C": ""}}, {"resolved": {"A": "${MISSING}", "B": "${MISSING}", "C": ""}, "unresolved": []}, "E2"),
      _case({"values": {"Z": "${A}", "A": "${B}", "B": "${A}", "C": "yes"}}, {"resolved": {"C": "yes"}, "unresolved": ["A", "B", "Z"]}, "E3"),
      _case({"values": {"A": "${A}", "B": "$${A}"}}, {"resolved": {"B": "${A}"}, "unresolved": ["A"]}, "E3"),
      _case({"values": {"A": "", "B": "$$${A}$$", "C": "${bad}:${1A}:${}"}}, {"resolved": {"A": "", "B": "$$", "C": "${bad}:${1A}:${}"}, "unresolved": []}, "E1"),
      _case({"values": {"A": "${B}${B}", "B": "$", "C": "${A}{Z}"}}, {"resolved": {"A": "$$", "B": "$", "C": "$${Z}"}, "unresolved": []}, "E2")]),
    ("interval-union", "interval", "U", UNION_SPEC, _union, UNION_SOLUTION,
     [_case({"intervals": [], "window": [0, 5]}, {"intervals": [], "gaps": [[0, 5]], "total": 0}, "U3"),
      _case({"intervals": [[1, 3], [2, 4]], "window": [0, 5]}, {"intervals": [[1, 4]], "gaps": [[0, 1], [4, 5]], "total": 3}, "U4"),
      _case({"intervals": [[-2, 3]], "window": [0, 2]}, {"intervals": [[0, 2]], "gaps": [], "total": 2}, "U1")],
     [_case({"intervals": [[3, 5], [1, 3]], "window": [0, 7]}, {"intervals": [[1, 5]], "gaps": [[0, 1], [5, 7]], "total": 4}, "U2"),
      _case({"intervals": [[-2, 5], [1, 2], [1, 2]], "window": [1, 1]}, {"intervals": [], "gaps": [], "total": 0}, "U3"),
      _case({"intervals": [[0, 0], [2, 2], [4, 4]], "window": [0, 4]}, {"intervals": [], "gaps": [[0, 4]], "total": 0}, "U1"),
      _case({"intervals": [[-8, -5], [-4, -2], [-3, 0]], "window": [-6, -1]}, {"intervals": [[-6, -5], [-4, -1]], "gaps": [[-5, -4]], "total": 4}, "U4"),
      _case({"intervals": [[-5, -1], [4, 8]], "window": [0, 3]}, {"intervals": [], "gaps": [[0, 3]], "total": 0}, "U1")]),
    ("interval-load", "interval", "L", LOAD_SPEC, _load, LOAD_SOLUTION,
     [_case({"bookings": []}, {"segments": [], "peak": 0, "work": 0}, "L3"),
      _case({"bookings": [[0, 2, 2], [1, 3, 1]]}, {"segments": [[0, 1, 2], [1, 2, 3], [2, 3, 1]], "peak": 3, "work": 6}, "L1"),
      _case({"bookings": [[1, 1, 9]]}, {"segments": [], "peak": 0, "work": 0}, "L1")],
     [_case({"bookings": [[0, 2, 3], [2, 4, 3]]}, {"segments": [[0, 4, 3]], "peak": 3, "work": 12}, "L2"),
      _case({"bookings": [[0, 2, 2], [0, 2, 2]]}, {"segments": [[0, 2, 4]], "peak": 4, "work": 8}, "L1"),
      _case({"bookings": [[-5, -3, 1], [-1, 1, 2]]}, {"segments": [[-5, -3, 1], [-1, 1, 2]], "peak": 2, "work": 6}, "L2"),
      _case({"bookings": [[0, 5, 2], [1, 4, 3], [2, 2, 9]]}, {"segments": [[0, 1, 2], [1, 4, 5], [4, 5, 2]], "peak": 5, "work": 19}, "L4"),
      _case({"bookings": [[-30, 30, 9], [-30, 30, 9]]}, {"segments": [[-30, 30, 18]], "peak": 18, "work": 1080}, "L3")]),
    ("interval-slot", "interval", "S", SLOT_SPEC, _slot, SLOT_SOLUTION,
     [_case({"window": [0, 10], "blocked": [[2, 4]], "duration": 3, "not_before": 0}, {"start": 4, "free": [[0, 2], [4, 10]]}, "S3"),
      _case({"window": [0, 3], "blocked": [], "duration": 4, "not_before": 0}, {"start": None, "free": [[0, 3]]}, "S4"),
      _case({"window": [0, 10], "blocked": [], "duration": 2, "not_before": 3}, {"start": 3, "free": [[0, 10]]}, "S2")],
     [_case({"window": [0, 5], "blocked": [[2, 5]], "duration": 2, "not_before": 0}, {"start": 0, "free": [[0, 2]]}, "S3"),
      _case({"window": [-5, 5], "blocked": [[-8, -2], [1, 9]], "duration": 3, "not_before": -9}, {"start": -2, "free": [[-2, 1]]}, "S1"),
      _case({"window": [0, 8], "blocked": [[3, 5], [1, 3], [6, 6]], "duration": 3, "not_before": 2}, {"start": 5, "free": [[0, 1], [5, 8]]}, "S1"),
      _case({"window": [1, 1], "blocked": [], "duration": 1, "not_before": 0}, {"start": None, "free": []}, "S4"),
      _case({"window": [-4, 4], "blocked": [], "duration": 1, "not_before": 4}, {"start": None, "free": [[-4, 4]]}, "S4")]),
    ("ledger-transfers", "ledger", "T", TRANSFER_SPEC, _transfers, TRANSFER_SOLUTION,
     [_case({"balances": {"a": 5, "b": 0}, "events": [_transfer("x", amount=3)]}, {"balances": {"a": 2, "b": 3}, "accepted": ["x"], "rejected": [], "duplicates": []}, "T2"),
      _case({"balances": {"a": 0, "b": 0}, "events": [_transfer("x")]}, {"balances": {"a": 0, "b": 0}, "accepted": [], "rejected": ["x"], "duplicates": []}, "T2"),
      _case({"balances": {"a": 5, "b": 0}, "events": [_transfer("x"), _transfer("x")]}, {"balances": {"a": 4, "b": 1}, "accepted": ["x"], "rejected": [], "duplicates": ["x"]}, "T1")],
     [_case({"balances": {"a": 0, "b": 5}, "events": [_transfer("x"), _transfer("fund", "b", "a", 3), _transfer("x"), _transfer("x", amount=0)]}, {"balances": {"a": 3, "b": 2}, "accepted": ["fund"], "rejected": ["x"], "duplicates": ["x", "x"]}, "T1"),
      _case({"balances": {"a": 2}, "events": [_transfer("x", "a", "a", 3), _transfer("y", "a", "a", 2), _transfer("z", "a", "a", 0)]}, {"balances": {"a": 2}, "accepted": ["y", "z"], "rejected": ["x"], "duplicates": []}, "T3"),
      _case({"balances": {"a": 0, "b": 0}, "events": [_transfer("zero", amount=0)]}, {"balances": {"a": 0, "b": 0}, "accepted": ["zero"], "rejected": [], "duplicates": []}, "T3"),
      _case({"balances": {"a": 1, "b": 1}, "events": [_transfer("x"), _transfer("x", "b", "a", 2), _transfer("z", "b", "a", 2)]}, {"balances": {"a": 2, "b": 0}, "accepted": ["x", "z"], "rejected": [], "duplicates": ["x"]}, "T4"),
      _case({"balances": {"a": 9}, "events": []}, {"balances": {"a": 9}, "accepted": [], "rejected": [], "duplicates": []}, "T4")]),
    ("ledger-versions", "ledger", "V", VERSION_SPEC, _versions, VERSION_SOLUTION,
     [_case({"events": []}, {"state": {}, "versions": {}, "stale": [], "duplicates": []}, "V4"),
      _case({"events": [_version("x", version=1, value="old"), _version("y", version=2, value="new")]}, {"state": {"a": "new"}, "versions": {"a": 2}, "stale": [], "duplicates": []}, "V2"),
      _case({"events": [_version("x", version=2), _version("y", version=1)]}, {"state": {"a": 0}, "versions": {"a": 2}, "stale": ["y"], "duplicates": []}, "V2")],
     [_case({"events": [_version("x", version=0, value=None)]}, {"state": {"a": None}, "versions": {"a": 0}, "stale": [], "duplicates": []}, "V3"),
      _case({"events": [_version("d", version=3, delete=True), _version("s", version=2, value=8)]}, {"state": {}, "versions": {"a": 3}, "stale": ["s"], "duplicates": []}, "V3"),
      _case({"events": [_version("x", version=1, value=False), _version("y", version=1, value=2)]}, {"state": {"a": False}, "versions": {"a": 1}, "stale": ["y"], "duplicates": []}, "V2"),
      _case({"events": [_version("high", version=4), _version("x", version=1), _version("x", key="b", version=8), _version("x", version=9)]}, {"state": {"a": 0}, "versions": {"a": 4}, "stale": ["x"], "duplicates": ["x", "x"]}, "V1"),
      _case({"events": [_version("d", version=0, delete=True), _version("s", version=1, value=[None])]}, {"state": {"a": [None]}, "versions": {"a": 1}, "stale": [], "duplicates": []}, "V3")]),
]


def _sample(identity: str, rng: random.Random):
    if identity == "config-merge":
        values = [None, False, True, 0, 4, "", [None, {"k": None}], {"x": 1}, {"x": None, "z": [2]}, {}]
        return {"layers": [{key: deepcopy(rng.choice(values)) for key in rng.sample(["a", "b", "c", ""], rng.randrange(5))} for _ in range(rng.randrange(1, 7))]}
    if identity == "config-pointers":
        document = {"a/b": list(range(rng.randrange(5))), "~1": {"": rng.randrange(9)}, "0": None}
        size, operations = len(document["a/b"]), []
        for _ in range(rng.randrange(1, 9)):
            op = rng.choice(["add", "replace", "remove"]) if size else "add"
            index = rng.randrange(size + int(op == "add"))
            row = {"op": op, "path": "/a~1b/" + ("-" if op == "add" and index == size and rng.randrange(2) else str(index))}
            if op != "remove":
                row["value"] = rng.choice([None, False, rng.randrange(20), [0]])
            size += int(op == "add") - int(op == "remove")
            operations.append(row)
        operations.append({"op": "replace", "path": "/~01/", "value": rng.randrange(20)})
        return {"document": document, "operations": operations}
    if identity == "config-expand":
        names = ["A", "B", "C", "D", "E", "_X"][:rng.randrange(2, 7)]
        parts = ["", "hi", "$$", "${A}", "${B}", "${C}", "${MISSING}", "$${A}", "${bad}", "$", "!"]
        return {"values": {name: "".join(rng.choice(parts) for _ in range(rng.randrange(4))) for name in names}}
    if identity in ("interval-union", "interval-slot"):
        intervals = [sorted([rng.randrange(-12, 13), rng.randrange(-12, 13)]) for _ in range(rng.randrange(9))]
        window = sorted([rng.randrange(-10, 11), rng.randrange(-10, 11)])
        if identity == "interval-union":
            return {"intervals": intervals, "window": window}
        return {"window": window, "blocked": intervals, "duration": rng.randrange(1, 10), "not_before": rng.randrange(-12, 13)}
    if identity == "interval-load":
        return {"bookings": [sorted([rng.randrange(-12, 13), rng.randrange(-12, 13)]) + [rng.randrange(1, 10)] for _ in range(rng.randrange(1, 9))]}
    if identity == "ledger-transfers":
        accounts = ["a", "b", "c"]
        return {"balances": {name: rng.randrange(11) for name in accounts}, "events": [_transfer(str(rng.randrange(5)), rng.choice(accounts), rng.choice(accounts), rng.randrange(12)) for _ in range(rng.randrange(1, 16))]}
    if identity == "ledger-versions":
        return {"events": [_version(str(rng.randrange(7)), key=rng.choice(["a", "b", "c"]), version=rng.randrange(6), value=rng.choice([None, 0, False, "x", [1], {"a": 2}]), delete=rng.randrange(4) == 0) for _ in range(rng.randrange(1, 16))]}
    raise AssertionError(identity)


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _mutants(identity, source):
    substitutions = {
        "config-merge": [("null-stored-instead-of-deleted", "out.pop(key, None)", "out[key] = None"), ("arrays-concatenated", "out[key] = deepcopy(value)", "out[key] = deepcopy(out.get(key, []) + value) if isinstance(value, list) and isinstance(out.get(key), list) else deepcopy(value)")],
        "config-pointers": [("wrong-escape-order", '.replace("~1", "/").replace("~0", "~")', '.replace("~0", "~").replace("~1", "/")'), ("array-add-overwrites", 'node.insert(index, deepcopy(change["value"]))', 'node[index:index+1] = [deepcopy(change["value"])]')],
        "config-expand": [("escaped-dollars-expanded", 'r"\\$\\$|\\$\\{([A-Z_][A-Z0-9_]*)\\}"', 'r"\\$\\{([A-Z_][A-Z0-9_]*)\\}"'), ("unresolved-order-reversed", 'sorted(set(values) - set(done))', 'sorted(set(values) - set(done), reverse=True)')],
        "interval-union": [("adjacency-not-merged", 'a <= merged[-1][1]', 'a < merged[-1][1]'), ("overlap-double-counted", 'sum(b-a for a,b in merged)', 'sum(max(0, min(b, hi)-max(a, lo)) for a,b in payload["intervals"])')],
        "interval-load": [("equal-adjacent-load-not-coalesced", 'if segments and segments[-1][1] == point', 'if False and segments and segments[-1][1] == point'), ("duplicate-bookings-discarded", 'for a, b, weight in payload["bookings"]:', 'for a, b, weight in set(map(tuple, payload["bookings"])):')],
        "interval-slot": [("exact-fit-rejected", 'candidate + payload["duration"] <= b', 'candidate + payload["duration"] < b'), ("ignores-not-before", 'candidate = max(a, payload["not_before"])', 'candidate = a')],
        "ledger-transfers": [("rejected-id-not-consumed", 'rejected.append(identity)\n            continue', 'rejected.append(identity)\n            seen.remove(identity)\n            continue'), ("self-transfer-always-accepted", 'if balances[source] < amount:', 'if source != target and balances[source] < amount:')],
        "ledger-versions": [("equal-version-overwrites", 'version <= versions[key]', 'version < versions[key]'), ("tombstone-forgotten", 'state.pop(key, None)', 'state.pop(key, None)\n            versions.pop(key, None)')],
    }
    answer = {}
    for label, old, new in substitutions[identity]:
        if old not in source:
            raise AssertionError(f"Missing mutation site {identity}/{label}")
        answer[label] = source.replace(old, new, 1)
    return answer


def _build():
    tasks = []
    for index, (identity, family, prefix, spec, oracle, source, public, hidden) in enumerate(DEFINITIONS):
        seen = {_canonical(case["input"]) for case in public + hidden}
        def generated(count, seed):
            rng, result = random.Random(seed), []
            for _ in range(10000):
                if len(result) == count:
                    return result
                payload = _sample(identity, rng)
                key = _canonical(payload)
                if key in seen:
                    continue
                _json(payload)
                expected = oracle(deepcopy(payload))
                seen.add(key)
                result.append(_case(payload, expected, prefix + str(1 + len(result) % 4)))
            raise AssertionError("Insufficient unique fixture inputs")
        hidden = hidden + generated(15 - len(hidden), 17001 + index)
        fuzz = generated(16, 31001 + index)
        tasks.append(SwarmTask(identity, family, COMMON_SPEC + "\n" + spec,
                               tuple(prefix + str(n) for n in range(1, 5)), {"solution.py": STUB}, source,
                               tuple(public), tuple(hidden), tuple(fuzz), oracle, _mutants(identity, source)))
    return tuple(tasks)


TASKS = _build()


def get_task(identity: str) -> SwarmTask:
    for task in TASKS:
        if task.id == identity:
            return task
    raise ValueError(f"Unknown swarm task: {identity}")


def public_manifest(identity: str | None = None):
    """Return fresh worker-safe data, with no expected hidden/fuzz values."""
    if identity is not None:
        return get_task(identity).public_manifest()
    return [task.public_manifest() for task in TASKS]


def fixture_signature() -> str:
    """Bind every contract, case, trusted implementation, and helper source."""
    content = {"version": FIXTURE_VERSION, "source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
               "tasks": [{**task.public_manifest(), "known_solution": task.known_solution,
                          "hidden_cases": task.hidden_cases, "fuzz_cases": task.fuzz_cases,
                          "mutants": task.mutants} for task in TASKS]}
    return hashlib.sha256(_canonical(content).encode()).hexdigest()
