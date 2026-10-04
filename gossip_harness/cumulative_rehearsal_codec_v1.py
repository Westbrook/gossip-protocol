"""Closed, bounded transport of concrete rehearsal inputs, never verdicts.

The caller independently pins the containing capsule. Type tags select only
dataclasses from the source-defined table; they never import a supplied module
or unpickle code. Decoding constructs inputs, not an evidence authority.
"""
from __future__ import annotations

import base64
from dataclasses import fields, is_dataclass
import importlib
import json
import math
from pathlib import Path
from typing import Any

from .candidate_storage_product_profile_v1 import decode as bounded_decode
from .peer_financial_terminal_v1 import require, sha

PROTOCOL = 'cumulative-rehearsal-typed-input-v1'
MAX_BYTES = 32 * 1024 * 1024
MAX_DEPTH = 48
MAX_NODES = 1_000_000
MODULES = (
    'candidate_checkpoint_chain_v1', 'project_acceptance_registry_v1',
    'project_acceptance_compiler_v1', 'candidate_scope_consumer_v1',
    'cumulative_scope_source_v1', 'cumulative_scope_source_v2', 'cumulative_scope_source_v3',
    'cumulative_scope_authority_v1', 'cumulative_scope_authority_v2', 'cumulative_scope_authority_v3',
    'cumulative_study_controller_v2', 'cumulative_observation_profile_v1',
    'cumulative_prerequisite_review_v1', 'candidate_observation_admission_v1',
    'candidate_client_execution_v5', 'candidate_client_process_v4',
    'candidate_source_capture_policy_v1', 'candidate_http_semantics_v1', 'cumulative_cli_projection_v1',
    'candidate_http_execution_v4', 'candidate_http_transport_v1', 'candidate_http_cases_core_v1',
    'candidate_product_process_execution_v1', 'candidate_product_process_core_v1',
    'candidate_storage_product_execution_v1', 'candidate_storage_product_profile_v1',
    'candidate_storage_review_authority_v1', 'candidate_m2_product_execution_v1',
    'candidate_m2_product_profile_v1', 'candidate_m2_review_authority_v1',
)


def types() -> dict[str, type]:
    result = {}
    for name in MODULES:
        module = importlib.import_module('.' + name, __package__)
        for value in vars(module).values():
            if isinstance(value, type) and is_dataclass(value) and value.__module__ == module.__name__:
                result[module.__name__ + '.' + value.__qualname__] = value
    return result


def encoded(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(',', ':'), allow_nan=False).encode()


def pack(value: Any) -> dict:
    allowed = types()
    count = 0

    def visit(item: Any, depth: int) -> Any:
        nonlocal count
        count += 1
        require(depth <= MAX_DEPTH and count <= MAX_NODES, 'Typed rehearsal input exceeds structural bound')
        if item is None or type(item) in (str, bool, int):
            return item
        if type(item) is float:
            require(math.isfinite(item), 'Nonfinite rehearsal input')
            return item
        if type(item) is bytes:
            return {'tag': 'bytes', 'value': base64.b64encode(item).decode('ascii')}
        if isinstance(item, Path):
            require(item.is_absolute() and item.resolve() == item, 'Canonical absolute input path required')
            return {'tag': 'path', 'value': str(item)}
        if type(item) in (tuple, list):
            return {'tag': 'tuple' if type(item) is tuple else 'list',
                    'value': [visit(part, depth + 1) for part in item]}
        if type(item) is dict:
            require(all(type(key) is str for key in item), 'String-keyed rehearsal inputs required')
            return {'tag': 'dict', 'value': {key: visit(part, depth + 1) for key, part in item.items()}}
        name = type(item).__module__ + '.' + type(item).__qualname__
        require(name in allowed and type(item) is allowed[name], 'Unknown rehearsal input type')
        return {'tag': 'dataclass', 'type': name, 'value': {
            field.name: visit(getattr(item, field.name), depth + 1) for field in fields(item) if field.init}}

    out = {'protocol': PROTOCOL, 'input': visit(value, 0)}
    raw=encoded(out)
    require(len(raw) <= MAX_BYTES and bounded_decode(raw) == out,
            'Typed rehearsal input exceeds its exact reader bounds')
    return out


def unpack(record: dict) -> Any:
    require(type(record) is dict and set(record) == {'protocol', 'input'} and record['protocol'] == PROTOCOL,
            'Unknown typed rehearsal input protocol')
    raw = encoded(record)
    require(len(raw) <= MAX_BYTES and bounded_decode(raw) == record, 'Invalid typed rehearsal input bytes')
    allowed = types()
    count = 0

    def visit(item: Any, depth: int) -> Any:
        nonlocal count
        count += 1
        require(depth <= MAX_DEPTH and count <= MAX_NODES, 'Typed rehearsal input exceeds structural bound')
        if item is None or type(item) in (str, bool, int):
            return item
        if type(item) is float:
            require(math.isfinite(item), 'Nonfinite rehearsal input')
            return item
        require(type(item) is dict and type(item.get('tag')) is str, 'Malformed typed rehearsal input')
        tag = item['tag']
        require(set(item) == ({'tag', 'type', 'value'} if tag == 'dataclass' else {'tag', 'value'}),
                'Unknown typed input fields')
        value = item['value']
        if tag == 'bytes':
            require(type(value) is str, 'Byte input is not base64')
            result = base64.b64decode(value, validate=True)
            require(base64.b64encode(result).decode('ascii') == value, 'Noncanonical base64')
            return result
        if tag == 'path':
            require(type(value) is str, 'Input path is not text')
            path = Path(value)
            require(path.is_absolute() and str(path.resolve()) == value, 'Canonical input path required')
            return path
        if tag in ('tuple', 'list'):
            require(type(value) is list, 'Ordered input array required')
            parts = [visit(part, depth + 1) for part in value]
            return tuple(parts) if tag == 'tuple' else parts
        require(type(value) is dict and all(type(key) is str for key in value), 'Typed object fields required')
        if tag == 'dict':
            return {key: visit(part, depth + 1) for key, part in value.items()}
        require(tag == 'dataclass' and type(item['type']) is str and item['type'] in allowed,
                'Type tag is not in the closed source contract')
        cls = allowed[item['type']]
        require(set(value) == {field.name for field in fields(cls) if field.init}, 'Exact dataclass fields required')
        return cls(**{key: visit(part, depth + 1) for key, part in value.items()})

    result = visit(record['input'], 0)
    require(pack(result) == record, 'Typed input did not round-trip exactly')
    return result


def read(reference: dict) -> Any:
    from .candidate_http_journal_v3 import read as stable_read
    require(type(reference) is dict and set(reference) == {'path', 'sha256'}, 'Pinned typed input reference required')
    path = Path(reference['path'])
    require(path.is_absolute() and str(path.resolve()) == reference['path'], 'Canonical typed input reference required')
    raw = stable_read(path, max_bytes=MAX_BYTES)
    require(sha(raw) == reference['sha256'], 'Pinned typed rehearsal input changed')
    value = bounded_decode(raw)
    require(encoded(value) == raw, 'Canonical typed input bytes required')
    return unpack(value)
