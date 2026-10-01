"""Versioned maintenance fixture for the real transport.py repository module.

Baseline and host-owned adapter are fixed; models edit only transport.py. The
reference below is a separate state machine and never imports candidate/golden
code. All held-out cases and expected values remain on the host.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
import math
import random
from textwrap import dedent

BASELINE_SHA256 = 'c8f68f90480b6090e9f4dc43cc6882b6fad5bde7c6c5378c35b0084691a771a0'
BASELINE = '"""Deterministic, in-process evidence dissemination demonstrator.\n\nThis is neither a real network nor an LLM/software-quality benchmark. Peers\nshare one trust domain; producer checks are not cryptographic authentication.\nTopics are metadata, not routing or ACLs. A delivered proposal or refutation\ndoes not become true, approved, or authoritative by being widely replicated.\n\nRounds use snapshots so delivery cannot cascade within a round. ``contacts``\ncounts attempted directed peer selections, including dropped/partitioned ones;\neach contact exchanges records in both directions. It is not a wire-message\nor byte count. ``converge`` uses an external observer, not distributed termination\ndetection. Both transports retain every record and retry via anti-entropy.\n\nThe bus is a star anti-entropy model of a reliable broker: each leaf contacts\nonly the broker once per round. This models connectivity and dissemination,\nnot production pub/sub throughput, broker replication, or processing capacity.\nA leaf-origin record needs an ingress round and a later egress round to reach\nanother leaf. Gossip uses bounded random peer fanout without a broker.\n"""\n\nfrom __future__ import annotations\n\nfrom dataclasses import dataclass\nimport hashlib\nimport json\nimport math\nimport random\nfrom typing import Any, Iterable, Literal\n\n\ndef _canonical(value: Any) -> str:\n    """Encode JSON without accepting coercions such as integer object keys."""\n    def check(item: Any) -> None:\n        if item is None or type(item) in (bool, int, str):\n            return\n        if type(item) is float and math.isfinite(item):\n            return\n        if type(item) is list:\n            for child in item:\n                check(child)\n            return\n        if type(item) is dict and all(type(key) is str for key in item):\n            for child in item.values():\n                check(child)\n            return\n        raise ValueError("Event content must contain only finite JSON values")\n\n    check(value)\n    return json.dumps(value, sort_keys=True, separators=(",", ":"),\n                      ensure_ascii=True, allow_nan=False)\n\n\ndef _identity(producer: str, sequence: int, kind: str, payload: dict,\n              topic: str) -> dict:\n    for label, value in (("producer", producer), ("kind", kind), ("topic", topic)):\n        if not isinstance(value, str) or not value:\n            raise ValueError(f"{label} must be a nonempty string")\n    if type(sequence) is not int or sequence < 0:\n        raise ValueError("sequence must be a nonnegative integer")\n    if type(payload) is not dict:\n        raise ValueError("payload must be a JSON object")\n    return {"producer": producer, "sequence": sequence, "kind": kind,\n            "payload": payload, "topic": topic}\n\n\ndef _digest(body: dict) -> str:\n    return hashlib.sha256(_canonical(body).encode("utf-8")).hexdigest()\n\n\n@dataclass(frozen=True)\nclass Event:\n    """A content-addressed record; payload reads return independent JSON copies."""\n\n    producer: str\n    sequence: int\n    kind: str\n    topic: str\n    _payload_json: str\n    event_id: str\n\n    @classmethod\n    def create(cls, producer: str, sequence: int, kind: str, payload: dict,\n               topic: str = "project") -> Event:\n        body = _identity(producer, sequence, kind, payload, topic)\n        encoded = _canonical(body)\n        return cls(producer, sequence, kind, topic, _canonical(payload),\n                   hashlib.sha256(encoded.encode("utf-8")).hexdigest())\n\n    @property\n    def payload(self) -> dict:\n        return json.loads(self._payload_json)\n\n    def to_dict(self) -> dict:\n        return {"event_id": self.event_id, "producer": self.producer,\n                "sequence": self.sequence, "kind": self.kind,\n                "topic": self.topic, "payload": self.payload}\n\n    def verify(self) -> None:\n        """Reject malformed records and content hashes that no longer match."""\n        try:\n            body = _identity(self.producer, self.sequence, self.kind,\n                             self.payload, self.topic)\n            expected = _digest(body)\n        except (TypeError, ValueError) as error:\n            raise ValueError("Malformed event content") from error\n        if self.event_id != expected:\n            raise ValueError("Event content hash mismatch")\n\n\nclass Mesh:\n    """Compare broker-star anti-entropy with bounded-fanout push-pull gossip."""\n\n    def __init__(self, peers: Iterable[str], mode: Literal["bus", "gossip"],\n                 seed: int = 0, fanout: int = 3, batch_size: int = 8,\n                 broker: str | None = None) -> None:\n        peer_list = list(peers)\n        if not peer_list or any(not isinstance(p, str) or not p for p in peer_list):\n            raise ValueError("peers must contain nonempty names")\n        if len(set(peer_list)) != len(peer_list):\n            raise ValueError("peer names must be unique")\n        if mode not in ("bus", "gossip"):\n            raise ValueError("mode must be \'bus\' or \'gossip\'")\n        for name, value in (("fanout", fanout), ("batch_size", batch_size)):\n            if type(value) is not int or value < 1:\n                raise ValueError(f"{name} must be a positive integer")\n        self.peers = tuple(sorted(peer_list))\n        self.broker = self.peers[0] if broker is None else broker\n        if self.broker not in self.peers:\n            raise ValueError("broker must be a known peer")\n        self.mode = mode\n        self.fanout = fanout\n        self.batch_size = batch_size\n        self._rng = random.Random(seed)\n        self._records: dict[str, dict[str, Event]] = {p: {} for p in self.peers}\n        self._stats = {"rounds": 0, "contacts": 0, "event_deliveries": 0,\n                       "duplicate_deliveries": 0}\n\n    def _check_peer(self, peer: str) -> None:\n        if peer not in self._records:\n            raise ValueError(f"Unknown peer: {peer}")\n\n    def publish(self, peer: str, event: Event) -> None:\n        self._check_peer(peer)\n        if not isinstance(event, Event):\n            raise ValueError("Only Event records can be published")\n        event.verify()\n        if event.producer != peer:\n            raise ValueError("Publishing peer must be the event\'s producer")\n        self._records[peer][event.event_id] = event\n\n    def events(self, peer: str) -> tuple[Event, ...]:\n        self._check_peer(peer)\n        return tuple(self._records[peer][key] for key in sorted(self._records[peer]))\n\n    def has(self, peer: str, event_id: str) -> bool:\n        self._check_peer(peer)\n        return event_id in self._records[peer]\n\n    @property\n    def stats(self) -> dict[str, int]:\n        return self._stats.copy()\n\n    def _partition_map(self, partitions: list[set[str]] | None) -> dict[str, int]:\n        if partitions is None:\n            return dict.fromkeys(self.peers, 0)\n        groups: dict[str, int] = {}\n        for index, group in enumerate(partitions):\n            if not group:\n                raise ValueError("Partition groups must be nonempty")\n            for peer in group:\n                self._check_peer(peer)\n                if peer in groups:\n                    raise ValueError("Each peer must belong to exactly one partition")\n                groups[peer] = index\n        if set(groups) != set(self.peers):\n            raise ValueError("Partitions must include every peer exactly once")\n        return groups\n\n    def step(self, partitions: list[set[str]] | None = None,\n             drop_rate: float = 0.0) -> int:\n        """Run one round; return the number of newly inserted remote records.\n\n        ``drop_rate`` independently drops whole contacts (both exchange directions).\n        ``partitions`` must list every peer exactly once. Selection still attempts\n        unreachable peers, so faults reduce useful work rather than hiding cost.\n        Batches contain the first sorted IDs missing at the destination snapshot.\n        For finite input, delivered IDs leave subsequent batches, avoiding starvation.\n        """\n        if not isinstance(drop_rate, (int, float)) or not 0 <= drop_rate <= 1:\n            raise ValueError("drop_rate must be between zero and one")\n        groups = self._partition_map(partitions)\n        snapshots = {peer: records.copy() for peer, records in self._records.items()}\n        pending: list[tuple[str, Event]] = []\n        self._stats["rounds"] += 1\n\n        def offer(source: str, target: str) -> None:\n            missing = sorted(snapshots[source].keys() - snapshots[target].keys())\n            pending.extend((target, snapshots[source][key])\n                           for key in missing[:self.batch_size])\n\n        for source in self.peers:\n            if self.mode == "bus":\n                targets = [] if source == self.broker else [self.broker]\n            else:\n                candidates = [peer for peer in self.peers if peer != source]\n                targets = self._rng.sample(candidates, min(self.fanout, len(candidates)))\n            for target in targets:\n                self._stats["contacts"] += 1\n                if groups[source] != groups[target]:\n                    continue\n                if drop_rate and self._rng.random() < drop_rate:\n                    continue\n                offer(source, target)\n                offer(target, source)\n\n        delivered = 0\n        for target, event in pending:\n            if event.event_id in self._records[target]:\n                self._stats["duplicate_deliveries"] += 1\n            else:\n                self._records[target][event.event_id] = event\n                delivered += 1\n        self._stats["event_deliveries"] += delivered\n        return delivered\n\n    def converge(self, max_rounds: int = 100, **faults: Any) -> bool:\n        """Use an external observer to check identical event-ID sets, with a cap."""\n        if type(max_rounds) is not int or max_rounds < 0:\n            raise ValueError("max_rounds must be a nonnegative integer")\n\n        def same_records() -> bool:\n            reference = self._records[self.peers[0]].keys()\n            return all(records.keys() == reference for records in self._records.values())\n\n        for _ in range(max_rounds):\n            if same_records():\n                return True\n            self.step(**faults)\n        return same_records()\n'

ALLOWED = ('gossip_harness/transport.py',)
ADAPTER = dedent('''\
    import hashlib
    import json
    import math
    from copy import deepcopy
    from gossip_harness.transport import Event, Mesh

    def digest(value):
        def ordinary(item):
            if item is None or type(item) in (str, bool, int):
                return
            if type(item) is float and math.isfinite(item):
                return
            if type(item) is list:
                for child in item:
                    ordinary(child)
                return
            if type(item) is dict and all(type(key) is str for key in item):
                for child in item.values():
                    ordinary(child)
                return
            raise ValueError('Checkpoint is not ordinary finite JSON')
        ordinary(value)
        return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False).encode()).hexdigest()

    def patched(value, edits):
        for edit in edits:
            if not edit['path']:
                value = deepcopy(edit['value'])
                continue
            parent = value
            for key in edit['path'][:-1]:
                parent = parent[key]
            key = edit['path'][-1]
            if edit['action'] == 'delete':
                del parent[key]
            elif edit['action'] == 'append':
                parent[key].append(deepcopy(edit['value']))
            else:
                parent[key] = deepcopy(edit['value'])
        return value

    def observed(mesh):
        return dict(peers=list(mesh.peers), mode=mesh.mode, broker=mesh.broker,
                    fanout=mesh.fanout, batch_size=mesh.batch_size, stats=mesh.stats,
                    records={p:[e.to_dict() for e in mesh.events(p)] for p in mesh.peers})

    def solve(payload):
        mesh = Mesh(**payload['config'])
        saved, answers = {}, []
        for command in payload['commands']:
            op = command['op']
            if op in ('publish', 'has'):
                event = Event.create(command['peer'], command['sequence'], command.get('kind', 'proposal'), command.get('payload', {}), command.get('topic', 'project'))
                if op == 'publish':
                    mesh.publish(command['peer'], event)
                    answer = True
                else:
                    answer = mesh.has(command['query_peer'], event.event_id)
            elif op in ('step', 'converge'):
                faults = dict(drop_rate=command.get('drop_rate', 0))
                if 'partitions' in command:
                    faults['partitions'] = [set(group) for group in command['partitions']]
                answer = mesh.step(**faults) if op == 'step' else mesh.converge(command.get('max_rounds', 100), **faults)
            elif op == 'inspect':
                answer = observed(mesh)
            elif op == 'save':
                saved[command['name']] = mesh.snapshot()
                answer = True
            elif op == 'compare':
                answer = digest(mesh.snapshot()) == digest(saved[command['name']])
            elif op == 'fingerprint':
                answer = digest(mesh.snapshot())
            elif op == 'advance':
                faults = dict(drop_rate=command.get('drop_rate', 0))
                if 'partitions' in command:
                    faults['partitions'] = [set(group) for group in command['partitions']]
                for _ in range(command.get('rounds', 1)):
                    mesh.step(**faults)
                answer = True
            elif op == 'saved':
                answer = digest(saved[command['name']])
            elif op == 'mutate':
                saved[command['name']] = patched(saved[command['name']], command['edits'])
                answer = True
            elif op == 'load':
                source = saved[command['name']]
                if command.get('edits'):
                    source = patched(deepcopy(source), command['edits'])
                try:
                    if command.get('method', 'from_snapshot') == 'restore':
                        result = mesh.restore(source)
                        if result is not None:
                            raise RuntimeError('restore must return None')
                    else:
                        mesh = Mesh.from_snapshot(source)
                    answer = True
                except ValueError:
                    answer = {'error': 'invalid_snapshot'}
            else:
                raise ValueError('Unknown adapter command')
            answers.append(deepcopy(answer))
        return answers
''')

# Trusted repair, assembled against the exact immutable baseline above. This is
# deliberately a separate implementation from the host reference state machine.
GOLDEN_METHODS = '''
    def snapshot(self) -> dict:
        state = self._rng.getstate()
        return dict(version=1, peers=list(self.peers), mode=self.mode,
                    broker=self.broker, fanout=self.fanout, batch_size=self.batch_size,
                    stats=self.stats, records={p:[e.to_dict() for e in self.events(p)] for p in self.peers},
                    rng_state=[state[0], list(state[1]), state[2]])

    @classmethod
    def from_snapshot(cls, data: dict) -> Mesh:
        return _read_mesh_snapshot(cls, data)
'''
GOLDEN_RESTORE = '''
    def restore(self, data: dict) -> None:
        replacement = type(self).from_snapshot(data)
        self.__dict__.clear()
        self.__dict__.update(replacement.__dict__)
'''
GOLDEN_HELPER = '''
def _read_mesh_snapshot(cls, data):
    def require(condition):
        if not condition:
            raise ValueError('Invalid snapshot')
    def keys(obj, required):
        require(type(obj) is dict and set(obj) == set(required))
    try:
        _canonical(data)
        keys(data, ('version','peers','mode','broker','fanout','batch_size','stats','records','rng_state'))
        require(type(data['version']) is int and data['version'] == 1)
        peers = data['peers']
        require(type(peers) is list and len(peers) > 0 and all(type(p) is str and p for p in peers))
        require(peers == sorted(set(peers)))
        require(type(data['mode']) is str and data['mode'] in ('bus','gossip'))
        require(type(data['broker']) is str and data['broker'] in peers)
        for field in ('fanout','batch_size'):
            require(type(data[field]) is int and data[field] > 0)
        keys(data['stats'], ('rounds','contacts','event_deliveries','duplicate_deliveries'))
        require(all(type(v) is int and v >= 0 for v in data['stats'].values()))
        rng = data['rng_state']
        require(type(rng) is list and len(rng) == 3 and type(rng[0]) is int and rng[0] == 3)
        require(type(rng[1]) is list and len(rng[1]) == 625)
        require(all(type(v) is int and 0 <= v < 2**32 for v in rng[1][:-1]))
        require(type(rng[1][-1]) is int and 0 <= rng[1][-1] <= 624)
        require(rng[2] is None or (type(rng[2]) in (int,float) and math.isfinite(rng[2])))
        keys(data['records'], peers)
        records = {}
        for peer in peers:
            rows = data['records'][peer]
            require(type(rows) is list)
            records[peer] = {}
            for row in rows:
                keys(row, ('event_id','producer','sequence','kind','payload','topic'))
                require(type(row['producer']) is str and row['producer'] in peers)
                event = Event.create(row['producer'], row['sequence'], row['kind'], row['payload'], row['topic'])
                require(type(row['event_id']) is str and row['event_id'] == event.event_id)
                require(event.event_id not in records[peer])
                records[peer][event.event_id] = event
            require(list(records[peer]) == sorted(records[peer]))
        for rows in records.values():
            for event in rows.values():
                require(event.event_id in records[event.producer])
        mesh = cls(peers, data['mode'], fanout=data['fanout'], batch_size=data['batch_size'], broker=data['broker'])
        mesh._rng.setstate((3, tuple(rng[1]), rng[2]))
        mesh._records = records
        mesh._stats = data['stats'].copy()
        return mesh
    except (KeyError, TypeError, OverflowError, IndexError) as error:
        raise ValueError('Invalid snapshot') from error
'''


def _files(stage):
    source = BASELINE if stage == 0 else BASELINE + GOLDEN_METHODS + (GOLDEN_RESTORE if stage == 2 else '') + GOLDEN_HELPER
    return {'solution.py': ADAPTER, 'gossip_harness/__init__.py': '', 'gossip_harness/transport.py': source}


def _encoded(value):
    def ordinary(item):
        if item is None or type(item) in (str, bool, int):
            return
        if type(item) is float and math.isfinite(item):
            return
        if type(item) is list:
            for child in item:
                ordinary(child)
            return
        if type(item) is dict and all(type(key) is str for key in item):
            for child in item.values():
                ordinary(child)
            return
        raise ValueError('invalid_snapshot')
    ordinary(value)
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False)


def _hash(value):
    return hashlib.sha256(_encoded(value).encode()).hexdigest()


def _event(command):
    body = dict(producer=command['peer'], sequence=command['sequence'],
                kind=command.get('kind', 'proposal'), topic=command.get('topic', 'project'),
                payload=deepcopy(command.get('payload', {})))
    return dict(event_id=_hash(body), **body)


class _Reference:
    """Independent event-dictionary simulator, never imports transport.py."""
    def __init__(self, config):
        self.peers = sorted(config['peers'])
        self.mode = config['mode']
        self.broker = config.get('broker') or self.peers[0]
        self.fanout = config.get('fanout', 3)
        self.batch_size = config.get('batch_size', 8)
        self.rng = random.Random(config.get('seed', 0))
        self.rows = {p:{} for p in self.peers}
        self.stats = dict(rounds=0, contacts=0, event_deliveries=0, duplicate_deliveries=0)

    def observed(self):
        return dict(peers=self.peers[:], mode=self.mode, broker=self.broker,
                    fanout=self.fanout, batch_size=self.batch_size, stats=self.stats.copy(),
                    records={p:[deepcopy(self.rows[p][k]) for k in sorted(self.rows[p])] for p in self.peers})

    def snapshot(self):
        state = self.rng.getstate()
        return dict(version=1, **self.observed(), rng_state=[3, list(state[1]), state[2]])

    def step(self, command):
        partitions = command.get('partitions', [self.peers])
        component = {peer:n for n, group in enumerate(partitions) for peer in group}
        previous = deepcopy(self.rows)
        packets = []
        self.stats['rounds'] += 1
        for peer in self.peers:
            choices = [p for p in self.peers if p != peer]
            if self.mode == 'bus':
                neighbors = [] if peer == self.broker else [self.broker]
            else:
                neighbors = self.rng.sample(choices, min(self.fanout, len(choices)))
            for neighbor in neighbors:
                self.stats['contacts'] += 1
                if component[peer] != component[neighbor]:
                    continue
                loss = command.get('drop_rate', 0)
                if loss and self.rng.random() < loss:
                    continue
                for origin, destination in ((peer, neighbor), (neighbor, peer)):
                    transfer = sorted(set(previous[origin]) - set(previous[destination]))[:self.batch_size]
                    packets.extend((destination, previous[origin][key]) for key in transfer)
        inserted = 0
        for destination, row in packets:
            key = row['event_id']
            if key in self.rows[destination]:
                self.stats['duplicate_deliveries'] += 1
            else:
                self.rows[destination][key] = deepcopy(row)
                inserted += 1
        self.stats['event_deliveries'] += inserted
        return inserted

    def converge(self, command):
        def done():
            return len({tuple(sorted(rows)) for rows in self.rows.values()}) == 1
        for _ in range(command.get('max_rounds', 100)):
            if done():
                break
            self.step(command)
        return done()


def _restore_reference(data):
    """Parse from public format into independent state, independently of golden."""
    def exact(value, fields):
        if type(value) is not dict or set(value) != set(fields):
            raise ValueError('invalid_snapshot')
    def integer(value, low, high=None):
        if type(value) is not int or value < low or (high is not None and value > high):
            raise ValueError('invalid_snapshot')
    _encoded(data)
    exact(data, ['version','peers','mode','broker','fanout','batch_size','stats','records','rng_state'])
    integer(data['version'], 1, 1)
    peers = data['peers']
    if type(peers) is not list or not peers or any(type(p) is not str or not p for p in peers):
        raise ValueError('invalid_snapshot')
    if len(set(peers)) != len(peers) or peers != sorted(peers):
        raise ValueError('invalid_snapshot')
    if type(data['mode']) is not str or data['mode'] not in ('gossip','bus') or type(data['broker']) is not str or data['broker'] not in peers:
        raise ValueError('invalid_snapshot')
    integer(data['fanout'], 1)
    integer(data['batch_size'], 1)
    exact(data['stats'], ['rounds','contacts','event_deliveries','duplicate_deliveries'])
    for number in data['stats'].values():
        integer(number, 0)
    state = data['rng_state']
    if type(state) is not list or len(state) != 3:
        raise ValueError('invalid_snapshot')
    integer(state[0], 3, 3)
    if type(state[1]) is not list or len(state[1]) != 625:
        raise ValueError('invalid_snapshot')
    for word in state[1][:624]:
        integer(word, 0, 2**32-1)
    integer(state[1][624], 0, 624)
    if state[2] is not None and (type(state[2]) not in (int,float) or not math.isfinite(state[2])):
        raise ValueError('invalid_snapshot')
    exact(data['records'], peers)
    collected = {}
    for peer, rows in data['records'].items():
        if type(rows) is not list:
            raise ValueError('invalid_snapshot')
        collected[peer] = {}
        for row in rows:
            exact(row, ['event_id','producer','sequence','kind','payload','topic'])
            if any(type(row[k]) is not str or not row[k] for k in ('event_id','producer','kind','topic')):
                raise ValueError('invalid_snapshot')
            if row['producer'] not in peers or type(row['payload']) is not dict:
                raise ValueError('invalid_snapshot')
            integer(row['sequence'], 0)
            body = {k:v for k,v in row.items() if k != 'event_id'}
            if _hash(body) != row['event_id'] or row['event_id'] in collected[peer]:
                raise ValueError('invalid_snapshot')
            collected[peer][row['event_id']] = deepcopy(row)
        if list(collected[peer]) != sorted(collected[peer]):
            raise ValueError('invalid_snapshot')
    for rows in collected.values():
        for key, row in rows.items():
            if key not in collected[row['producer']]:
                raise ValueError('invalid_snapshot')
    result = _Reference({k:data[k] for k in ('peers','mode','broker','fanout','batch_size')})
    result.stats = deepcopy(data['stats'])
    result.rows = collected
    result.rng.setstate((3, tuple(state[1]), state[2]))
    return result


def _patch(value, edits):
    for edit in edits:
        path = edit['path']
        if not path:
            value = deepcopy(edit['value'])
            continue
        node = value
        for key in path[:-1]:
            node = node[key]
        key = path[-1]
        if edit['action'] == 'delete':
            del node[key]
        elif edit['action'] == 'append':
            node[key].append(deepcopy(edit['value']))
        else:
            node[key] = deepcopy(edit['value'])
    return value


def validate_input(stage_index, payload):
    """Bound scenario commands; invalid snapshots themselves remain in domain."""
    def require(condition):
        if not condition:
            raise ValueError('outside_input_domain')
    require(type(stage_index) is int and stage_index in (0,1))
    require(type(payload) is dict and set(payload) == {'config','commands'})
    config, commands = payload['config'], payload['commands']
    require(type(config) is dict and {'peers','mode'} <= set(config) <= {'peers','mode','seed','fanout','batch_size','broker'})
    peers = config['peers']
    require(type(peers) is list and 1 <= len(peers) <= 6 and all(type(p) is str and 0 < len(p) <= 24 for p in peers))
    require(len(peers) == len(set(peers)) and config['mode'] in ('bus','gossip'))
    require(type(config.get('seed',0)) is int and abs(config.get('seed',0)) <= 100000)
    for field in ('fanout','batch_size'):
        require(type(config.get(field,1)) is int and 1 <= config.get(field,1) <= 12)
    require(config.get('broker', peers[0]) in peers)
    require(type(commands) is list and len(commands) <= 32)
    budget = [6000]
    def bounded(value, depth=0):
        budget[0] -= 1
        require(budget[0] >= 0 and depth <= 12)
        if value is None or type(value) is bool:
            return
        if type(value) in (int,float):
            require(abs(value) <= 2**40 and math.isfinite(value))
        elif type(value) is str:
            require(len(value) <= 256)
        elif type(value) is list:
            require(len(value) <= 64)
            for item in value:
                bounded(item, depth+1)
        elif type(value) is dict:
            require(len(value) <= 32 and all(type(k) is str and len(k) <= 64 for k in value))
            for item in value.values():
                bounded(item, depth+1)
        else:
            require(False)
    bounded(payload)
    saved = set()
    for item in commands:
        require(type(item) is dict and type(item.get('op')) is str)
        op = item['op']
        fields = {'publish':({'op','peer','sequence'},{'kind','topic','payload'}),
                  'has':({'op','peer','query_peer','sequence'},{'kind','topic','payload'}),
                  'step':({'op'},{'drop_rate','partitions'}),
                  'converge':({'op'},{'drop_rate','partitions','max_rounds'}),
                  'advance':({'op'},{'drop_rate','partitions','rounds'}),
                  'compare':({'op','name'},set()), 'fingerprint':({'op'},set()),
                  'inspect':({'op'},set()), 'save':({'op','name'},set()),
                  'saved':({'op','name'},set()), 'mutate':({'op','name','edits'},set()),
                  'load':({'op','name'},{'edits','method'})}
        require(op in fields)
        required, optional = fields[op]
        require(required <= set(item) <= required | optional)
        if op in ('publish','has'):
            require(item['peer'] in peers and type(item['sequence']) is int and 0 <= item['sequence'] <= 1000)
            require(type(item.get('payload',{})) is dict)
            if op == 'has':
                require(item['query_peer'] in peers)
            for label in ('kind','topic'):
                require(type(item.get(label,'x')) is str and bool(item.get(label,'x')))
        if op in ('step','converge','advance'):
            require(type(item.get('drop_rate',0)) in (int,float) and 0 <= item.get('drop_rate',0) <= 1)
            groups = item.get('partitions', [peers])
            require(type(groups) is list and all(type(g) is list and g for g in groups))
            flat = [p for group in groups for p in group]
            require(all(type(p) is str for p in flat) and sorted(flat) == sorted(peers))
            require(type(item.get('max_rounds',0)) is int and 0 <= item.get('max_rounds',0) <= 100)
            require(type(item.get('rounds',1)) is int and 0 <= item.get('rounds',1) <= 12)
        if 'name' in item:
            require(type(item['name']) is str and 0 < len(item['name']) <= 24)
            if op == 'save':
                saved.add(item['name'])
            else:
                require(item['name'] in saved)
        if op == 'load':
            require(item.get('method','from_snapshot') in ('from_snapshot','restore'))
            require(stage_index == 1 or item.get('method') != 'restore')
        if 'edits' in item:
            require(stage_index == 1 or op == 'mutate')
            edits = item['edits']
            require(type(edits) is list and len(edits) <= 12)
            for edit in edits:
                require(type(edit) is dict and set(edit) in ({'action','path'}, {'action','path','value'}))
                require(edit.get('action') in ('set','delete','append'))
                require(type(edit.get('path')) is list and len(edit['path']) <= 8)
                require(all(type(k) in (str,int) and type(k) is not bool for k in edit['path']))
                require(edit['action'] == 'delete' or 'value' in edit)
                require(bool(edit['path']) or edit['action'] == 'set')
    # Reject state-dependent malformed adapter paths and future stage contracts
    # here: probe admission catches only this validator's domain exceptions.
    answers = _simulate(stage_index, payload)
    # Mirror the admission gate's output bounds before it calls the oracle.
    count = [0]
    def bounded_output(value, depth=0):
        count[0] += 1
        require(count[0] <= 2048 and depth <= 10)
        if type(value) is list:
            require(len(value) <= 256)
            for item in value:
                bounded_output(item, depth+1)
        elif type(value) is dict:
            require(len(value) <= 128)
            for key, item in value.items():
                bounded_output(key, depth+1)
                bounded_output(item, depth+1)
    bounded_output(answers)
    require(len(_encoded(answers).encode()) <= 10000)


def _simulate(stage_index, payload):
    mesh = _Reference(payload['config'])
    saved, output = {}, []
    try:
        for item in payload['commands']:
            op = item['op']
            if op in ('publish', 'has'):
                row = _event(item)
                if op == 'publish':
                    mesh.rows[item['peer']][row['event_id']] = row
                    answer = True
                else:
                    answer = row['event_id'] in mesh.rows[item['query_peer']]
            elif op == 'step':
                answer = mesh.step(item)
            elif op == 'converge':
                answer = mesh.converge(item)
            elif op == 'inspect':
                answer = mesh.observed()
            elif op == 'save':
                saved[item['name']] = mesh.snapshot()
                answer = True
            elif op == 'compare':
                answer = _hash(mesh.snapshot()) == _hash(saved[item['name']])
            elif op == 'fingerprint':
                answer = _hash(mesh.snapshot())
            elif op == 'advance':
                for _ in range(item.get('rounds', 1)):
                    mesh.step(item)
                answer = True
            elif op == 'saved':
                answer = _hash(saved[item['name']])
            elif op == 'mutate':
                saved[item['name']] = _patch(saved[item['name']], item['edits'])
                answer = True
            else:
                incoming = saved[item['name']]
                if item.get('edits'):
                    incoming = _patch(deepcopy(incoming), item['edits'])
                try:
                    replacement = _restore_reference(incoming)
                except (ValueError, TypeError, KeyError, IndexError, OverflowError) as error:
                    if stage_index == 0:
                        raise ValueError('outside_input_domain') from error
                    answer = {'error':'invalid_snapshot'}
                else:
                    mesh = replacement
                    answer = True
            output.append(deepcopy(answer))
    except (KeyError, IndexError, TypeError, AttributeError) as error:
        raise ValueError('outside_input_domain') from error
    return output


def reference(stage_index, payload):
    validate_input(stage_index, payload)
    return _simulate(stage_index, payload)


def publish(peer='a', sequence=0, **values):
    return dict(op='publish', peer=peer, sequence=sequence, **values)


def cmd(op, **values):
    return dict(op=op, **values)


def edit(path, value=None, action='set'):
    return dict(action=action, path=path, **({} if action == 'delete' else {'value':value}))


def scenario(commands, **config):
    return dict(config=dict(peers=['a','b','c'], mode='gossip', seed=19, fanout=1, batch_size=1, **{}) | config,
                commands=commands)


SAVE = cmd('save', name='s')
LOAD = cmd('load', name='s')
INSPECT = cmd('inspect')
STEP = cmd('step')


def _case(stage, identifier, requirement, commands, **config):
    commands = [part for command in commands for part in ([command, cmd('fingerprint')] if command['op'] == 'save' else [command])]
    payload = scenario(commands, **config)
    return dict(id=f'transport-{identifier}', requirement=requirement,
                input=payload, expected=reference(stage, payload))


def _reject(identifier, requirement, edits, method='restore', **config):
    commands = [publish(payload={'nested':{'keep':[1,2]}}), publish('b',1), STEP, SAVE,
                publish('c',2), STEP,
                cmd('load', name='s', method=method, edits=edits),
                INSPECT, cmd('step', drop_rate=0.4), INSPECT, SAVE]
    return _case(1, identifier, requirement, commands, **config)


V1 = (
    _case(0, 'v1-format', 'T1-format', [publish(), SAVE, LOAD, INSPECT]),
    _case(0, 'v1-resume', 'T1-resume', [publish(), publish('b',1), STEP, SAVE,
           cmd('step',drop_rate=0.5), INSPECT, LOAD, cmd('step',drop_rate=0.5), INSPECT]),
    _case(0, 'v1-bus', 'T1-regression', [publish('c'), STEP, SAVE, LOAD, STEP, INSPECT], mode='bus', broker='b'),
    _case(0, 'v1-detached', 'T1-ownership', [publish(payload={'nested':{'x':[1]}}), SAVE,
           cmd('mutate',name='s',edits=[edit(['stats','rounds'], 99)]), INSPECT, SAVE]),
    _case(0, 'v1-duplicates', 'T1-regression', [publish(), publish(), publish(payload={'new':True}), STEP, SAVE, LOAD,
           cmd('converge',max_rounds=10), INSPECT], fanout=2),
    _case(0, 'v1-partition', 'T1-resume', [publish(), cmd('step',partitions=[['a'],['b','c']]), SAVE, LOAD,
           cmd('converge',max_rounds=12), INSPECT]),
)
H1 = (
    _case(0, 'h1-empty', 'T1-format', [SAVE, LOAD, INSPECT], peers=['solo']),
    _case(0, 'h1-rng-loss', 'T1-resume', [publish(), publish('d',4), cmd('step',drop_rate=0.3), SAVE,
           cmd('step',drop_rate=0.6), STEP, SAVE, LOAD, cmd('step',drop_rate=0.2), INSPECT], peers=['d','c','b','a'], seed=871, fanout=2),
    _case(0, 'h1-payload-copy', 'T1-ownership', [publish(payload={'nested':{'x':[1,2]}}), SAVE,
           cmd('mutate',name='s',edits=[edit(['records','a',0,'payload','nested','x',0], 77)]), INSPECT, SAVE]),
    _case(0, 'h1-import-copy', 'T1-ownership', [publish(payload={'nested':[{'x':1}]}), SAVE, LOAD,
           cmd('mutate',name='s',edits=[edit(['records','a',0,'payload','nested',0,'x'], 8), edit(['stats','rounds'], 88), edit(['rng_state',1,0],0)]), STEP, INSPECT, SAVE]),
    _case(0, 'h1-array-order', 'T1-format', [publish('z',9,kind='refutation',topic='é',payload={'s':'λ'}), publish('a',2), publish('z',3),
           cmd('converge',max_rounds=20), SAVE, LOAD, INSPECT], peers=['z','m','a']),
    _case(0, 'h1-no-cascade', 'T1-regression', [publish('c'), STEP, SAVE, LOAD, INSPECT, STEP, INSPECT], mode='bus', broker='a'),
    _case(0, 'h1-batch-resume', 'T1-resume', [publish(sequence=n) for n in range(5)] + [STEP, SAVE, LOAD, STEP, INSPECT,
           cmd('converge',max_rounds=30), SAVE], fanout=2),
    _case(0, 'h1-zero-cap', 'T1-regression', [publish(), cmd('converge',max_rounds=0), SAVE, LOAD,
           cmd('converge',max_rounds=0), INSPECT]),
    _case(0, 'h1-isolation-rounds', 'T1-resume', [publish('b',7), cmd('step',partitions=[['a'],['b'],['c']]), SAVE, LOAD,
           cmd('step',partitions=[['a'],['b'],['c']]), cmd('step',drop_rate=0.25), INSPECT]),
    _case(0, 'h1-snapshot-no-rng-consumption', 'T1-resume', [publish(), SAVE, SAVE, SAVE, STEP, INSPECT], seed=612),
    _case(0, 'h1-duplicate-accounting', 'T1-regression', [publish(), STEP, SAVE, LOAD, STEP, INSPECT], fanout=3, batch_size=8),
    _case(0, 'h1-source-not-mutated', 'T1-ownership', [publish(), SAVE, LOAD, STEP, cmd('saved',name='s'), INSPECT]),
)
V2 = (
    _reject('v2-root-key', 'T2-shape', [edit(['unexpected'], True)], method='from_snapshot'),
    _reject('v2-negative-stats', 'T2-shape', [edit(['stats','contacts'], -1)]),
    _reject('v2-event-hash', 'T2-events', [edit(['records','a',0,'event_id'], '0'*64)], method='from_snapshot'),
    _reject('v2-rng-index', 'T2-rng', [edit(['rng_state',1,624],625)]),
    _reject('v2-late-event-atomic', 'T2-atomic', [edit(['records','b',0,'payload'], {'bad':1})]),
    _case(1, 'v2-valid-restore', 'T2-atomic', [publish(), STEP, SAVE, publish('b',6), STEP,
           cmd('load',name='s',method='restore'), INSPECT, STEP, SAVE]),
)
H2 = (
    _reject('h2-missing-field', 'T2-shape', [edit(['batch_size'],action='delete')]),
    _reject('h2-counter-bool', 'T2-shape', [edit(['stats','rounds'],True)], method='from_snapshot'),
    _reject('h2-peer-record-membership', 'T2-events', [edit(['records','ghost'], [])]),
    _reject('h2-publisher-record-missing', 'T2-events', [edit(['records','a'], [])], method='from_snapshot', fanout=2),
    _reject('h2-event-extra', 'T2-events', [edit(['records','b',0,'trusted'],True)]),
    _reject('h2-rng-word-bool', 'T2-rng', [edit(['rng_state',1,7],True)], method='from_snapshot'),
    _reject('h2-rng-negative-word', 'T2-rng', [edit(['rng_state',1,11],-1)]),
    _reject('h2-rng-version', 'T2-rng', [edit(['rng_state',0],2)], method='from_snapshot'),
    _reject('h2-late-record-shape', 'T2-atomic', [edit(['records','c'],[None])]),
    _reject('h2-null-root', 'T2-shape', [edit([],None)], method='from_snapshot'),
    _case(1, 'h2-restore-input-ownership', 'T2-atomic', [publish(payload={'x':[1]}), SAVE,
           cmd('load',name='s',method='restore'), cmd('mutate',name='s',edits=[edit(['records','a',0,'payload','x'],[99]),edit(['stats','rounds'],4)]),
           STEP, INSPECT, SAVE]),
    _case(1, 'h2-failed-then-success', 'T2-atomic', [publish(), STEP, SAVE, publish('c',3),
           cmd('load',name='s',method='restore',edits=[edit(['rng_state',2],True)]), STEP,
           cmd('load',name='s',method='restore',edits=[edit(['rng_state',2],1.5),edit(['fanout'],2),
                edit(['rng_state',1,624],0),edit(['rng_state',1,0],4294967295),
                edit(['stats'],dict(rounds=0,contacts=999,event_deliveries=0,duplicate_deliveries=99))]), STEP, INSPECT, SAVE]),
)

SPEC1 = '''Maintain the existing gossip_harness/transport.py module, preserving Event and Mesh behavior.
Add Mesh.snapshot() -> dict and classmethod Mesh.from_snapshot(data) -> Mesh for a portable JSON checkpoint.
snapshot has EXACT keys version, peers, mode, broker, fanout, batch_size, stats, records, rng_state.
version is integer 1; peers is sorted unique nonempty string names; mode/broker/fanout/batch_size preserve existing configuration.
stats has EXACT rounds, contacts, event_deliveries, duplicate_deliveries counters. records maps EVERY peer to a list of Event.to_dict() records sorted by event_id, even empty lists.
rng_state is the JSON representation of random.Random.getstate(): [3, list_of_625_integers, gaussian_cache_or_null].
A checkpoint and its JSON roundtrip must restore all records, counters, settings and exact future seeded RNG behavior, including partitioned/lossy rounds and duplicate-delivery accounting. Output is ordinary finite JSON; no tuples or live Event objects. Dict-key insertion order is irrelevant; peer and record array order is canonical.
All exported/imported mutable content is detached: caller mutation of snapshot or its nested records/stats/RNG must not change a Mesh, and later Mesh work must not mutate imported snapshots. snapshot consumes no randomness. Preserve publish, events, has, step and converge contracts, including no within-round cascade, content-addressed event ordering, multiple contents at the same producer/sequence, and bounded batches.
Only gossip_harness/transport.py may change. solution.py is a fixed host-owned public API adapter; its bounded scenario operations are documented by that readable adapter. publish/save/load/mutate/advance return True on success (invalid loads return {error: invalid_snapshot}); compare(name) compares the current checkpoint with a named saved checkpoint, including exact RNG and all configuration/events/counters, returning bool. advance runs bounded rounds and returns True. These support predictable metamorphic probes: publish, save original, advance, save expected, load original, repeat the same advance, compare expected => all True. inspect/step/converge retain observed outputs; fingerprint/saved expose canonical SHA256 checkpoint digests for host-authored cases and need not be used in generated probes. The adapter is part of the public repository and may be read. Existing baseline SHA256 is ''' + BASELINE_SHA256 + '.'
SPEC2 = '''Add Mesh.restore(data) -> None, replacing the receiver atomically. Both from_snapshot and restore must reject every malformed snapshot with ValueError. If restore fails, configuration, peers, events, counters and RNG remain exactly unchanged, including after late nested-field failure.
The stage-one format is strict: exact keys at root/stats/event rows; no missing/extra keys. Root/version and RNG version are exact ints 1 and 3, never bool/float. Peers must be a nonempty sorted list of unique nonempty strings; mode exactly bus or gossip; broker is a known peer; fanout and batch_size exact positive ints. Stats are exact nonnegative ints; impose no other counter-reachability relationships.
records is an object with exactly the peer keys; each value is a list sorted strictly by unique event_id. Each event has exactly event_id, producer, sequence, kind, topic, payload. Existing Event validation and canonical SHA256 identity apply; event_id string must equal the content digest. Event producer must be a known peer; every replicated record must ALSO occur in that producer's own records. Different contents sharing producer/sequence remain legal.
rng_state is a list of length 3; its middle list has exactly 625 exact ints. The first 624 words are 0..4294967295 inclusive, the final index is 0..624 inclusive. The final gaussian cache is null or a finite int/float excluding bool. Version 2/coercions are forbidden. All values recursively must be ordinary finite JSON with string object keys.
On valid import, detach caller-owned content, retain exact RNG state and preserve all earlier behavior. restore returns None. Validation must finish before mutation of the receiver. No hidden or inferred invariant beyond this contract is required.'''

PROJECT = dict(
    id='transport', title='Real transport module: checkpoint and strict atomic restore',
    initial_files=_files(0), allowed_paths=ALLOWED,
    baseline_sha256=BASELINE_SHA256,
    stages=(
        dict(id='transport-1', title='Portable resumable mesh checkpoints', specification=SPEC1,
             requirements=('T1-format','T1-resume','T1-ownership','T1-regression'), visible_cases=V1,
             hidden_cases=H1, known_files=_files(1), trusted_updates={}),
        dict(id='transport-2', title='Strict atomic checkpoint import', specification=SPEC2,
             requirements=('T2-shape','T2-events','T2-rng','T2-atomic'), visible_cases=V2,
             hidden_cases=H2, known_files=_files(2), trusted_updates={}),
    ),
)


def golden_files(stage_index):
    if type(stage_index) is not int or stage_index not in (0,1):
        raise ValueError('invalid_stage')
    return _files(stage_index + 1)


def known_files(stage_index):
    return golden_files(stage_index)


def rehearsal_probe(stage_index, slot_index):
    if type(stage_index) is not int or stage_index not in (0,1) or type(slot_index) is not int or not 0 <= slot_index <= 9:
        raise ValueError('invalid_probe_slot')
    commands = [publish(payload={'probe':slot_index}), SAVE, cmd('advance',rounds=2,drop_rate=0.3),
                cmd('save',name='expected'), LOAD, cmd('advance',rounds=2,drop_rate=0.3), cmd('compare',name='expected')]
    if stage_index:
        commands += [cmd('load',name='s',method='restore',edits=[edit(['stats','rounds'],-1)]), cmd('compare',name='expected')]
    return scenario(commands, seed=12300 + stage_index*10 + slot_index)
