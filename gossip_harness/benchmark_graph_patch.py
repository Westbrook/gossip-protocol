"""Versioned graph-maintenance extension and concealed semantic fault bank.

The mature SQLite seed is from the frozen verification fixture.  Only PROJECT's
initial_files, stage specifications, and visible cases belong in model requests.
Reference code, known files, fault variants and private witnesses remain host data.
"""
from __future__ import annotations

from copy import deepcopy
from textwrap import dedent

from . import verification_buildgraph as base

CONTRACT = "benchmark-graph-patch-v1"
ALLOWED = base.ALLOWED

EXTENSION_DOMAIN = dedent('''

def validate_extension(command, stage):
    op = command.get('op') if type(command) is dict else None
    keys(command, ('op', 'key', 'version', 'mapping') if op == 'rename'
         else ('op', 'key', 'version', 'edits'))
    if op not in ('rename', 'patch') or (op == 'patch' and stage < 1):
        fail('invalid')
    name(command['key'])
    integer(command['version'], 0, 1000000)
    def action(value):
        if type(value) is not dict or value.get('op') not in ('put', 'remove', 'rename'):
            fail('invalid')
        kind = value['op']
        if kind == 'put':
            keys(value, ('op', 'id', 'source'), ('priority', 'deps'))
            return dict(op=kind, **record(dict(id=value['id'], source=value['source'],
                        priority=value.get('priority', 0), deps=value.get('deps', []))))
        if kind == 'remove':
            keys(value, ('op', 'id'))
            return dict(op=kind, id=name(value['id']))
        keys(value, ('op', 'mapping'))
        rows = value['mapping']
        if type(rows) is not list or not 1 <= len(rows) <= 6:
            fail('invalid')
        pairs = []
        for row in rows:
            keys(row, ('from', 'to'))
            pairs.append({'from': name(row['from']), 'to': name(row['to'])})
        if len({row['from'] for row in pairs}) != len(pairs) or len({row['to'] for row in pairs}) != len(pairs):
            fail('invalid')
        pairs.sort(key=lambda row: row['from'])
        return dict(op=kind, mapping=pairs)
    if op == 'rename':
        result = action(dict(op=op, mapping=command['mapping']))
    else:
        edits = command['edits']
        if type(edits) is not list or not 1 <= len(edits) <= 6:
            fail('invalid')
        result = dict(op=op, edits=[action(item) for item in edits])
    return dict(result, key=command['key'], version=command['version'])
''')

EXTENSION_SERVICE = dedent('''

# New atomic graph operations share the legacy durable receipt namespace.
from copy import deepcopy
from .domain import validate_extension
EXTENSION_STAGE = __EXTENSION_STAGE__
_legacy_execute = execute

def _definition(row):
    return {key: row[key] for key in ('source', 'priority', 'deps')}

def _edit_graph(graph, lineage, edit):
    op = edit['op']
    if op == 'put':
        identifier = edit['id']
        if identifier not in graph:
            lineage[identifier] = None
        graph[identifier] = {key: deepcopy(edit[key]) for key in ('source', 'priority', 'deps')}
        graph[identifier]['digest'] = None
    elif op == 'remove':
        identifier = edit['id']
        if identifier not in graph:
            fail('missing')
        del graph[identifier]
        lineage.pop(identifier, None)
    else:
        mapping = {row['from']: row['to'] for row in edit['mapping']}
        if any(identifier not in graph for identifier in mapping):
            fail('missing')
        if any(target in graph and target not in mapping for target in mapping.values()):
            fail('collision')
        renamed, origins = {}, {}
        for identifier, row in graph.items():
            target = mapping.get(identifier, identifier)
            revised = deepcopy(row)
            revised['deps'] = sorted(mapping.get(dep, dep) for dep in row['deps'])
            renamed[target], origins[target] = revised, lineage[identifier]
        graph.clear()
        graph.update(renamed)
        lineage.clear()
        lineage.update(origins)

def _reconcile_cache(original, graph, lineage):
    dirty = set()
    for identifier, row in graph.items():
        prior = original.get(lineage[identifier])
        if (prior is None or any(prior[key] != row[key] for key in ('source', 'deps'))
                or any(lineage[dependency] != dependency for dependency in row['deps'])):
            dirty.add(identifier)
        row['digest'] = prior['digest'] if prior is not None else None
    affected = closure(graph, dirty, reverse=True)
    for identifier in affected:
        graph[identifier]['digest'] = None
    return sorted(affected)

def execute(db, command, policy):
    if type(command) is not dict or command.get('op') not in ('rename', 'patch'):
        return _legacy_execute(db, command, policy)
    normalized = validate_extension(command, EXTENSION_STAGE)
    request = canonical({key: value for key, value in normalized.items() if key != 'key'})
    with db:
        original, version = storage.load(db)
        previous = storage.receipt(db, normalized['key'])
        if previous:
            if previous[0] != request:
                fail('conflict')
            return previous[1]
        if normalized['version'] != version:
            fail('stale')
        graph = deepcopy(original)
        lineage = {identifier: identifier for identifier in graph}
        edits = [normalized] if normalized['op'] == 'rename' else normalized['edits']
        for edit in edits:
            _edit_graph(graph, lineage, edit)
        graph_valid(graph)
        invalidated = _reconcile_cache(original, graph, lineage)
        changed = sorted(identifier for identifier in set(original) | set(graph)
                         if identifier not in original or identifier not in graph
                         or _definition(original[identifier]) != _definition(graph[identifier]))
        answer = dict(ok='renamed' if normalized['op'] == 'rename' else 'patched',
                      key=normalized['key'], version=version + 1, invalidated=invalidated)
        if normalized['op'] == 'rename':
            answer['renamed'] = normalized['mapping']
        storage.save(db, graph, version + 1, normalized['op'], changed, True)
        storage.remember(db, normalized['key'], request, answer)
        return answer
''')

SPEC_RENAME = '''
New milestone G1: add atomic guarded simultaneous rename, preserving all legacy
behavior. {op:"rename",key,version,mapping:[{from,to},...]} has EXACT fields;
key/from/to use the existing target-name syntax; version is int0..1000000,
excluding bool. Mapping has 1..6 entries with unique from AND unique to names.
Self mappings are valid. Validate the WHOLE command first; canonicalize mapping
by from. Then replay/conflict the shared durable key namespace BEFORE examining
current version or target state. A matching request returns its exact original
response forever; different valid normalized request -> conflict. Invalid syntax
precedes replay. Otherwise wrong current version -> stale. Then any absent source
-> missing; then a destination occupied by a node not in the source set -> collision.
Renames are simultaneous: swaps and longer cycles are valid and lose no data.
Rewrite every dependency ID through the mapping, including dependencies of
unrenamed nodes, and sort dependencies. No transient intermediate graph is observable.

Track node provenance through renaming. A node's OWN name and priority are absent
from its digest. Preserve its previous digest iff source and literal sorted
dependency-ID list equal those of its original node AND every dependency name
still denotes its original node. Swapping root names beneath a fan-in can retain
the same dependency-ID set but change those bindings: invalidate that fan-in.
Invalidate every node whose source/dependency list or dependency bindings changed,
every new node, and their transitive reverse
dependents in the final graph. An unchanged renamed root may keep its own digest,
while its dependents become dirty because dependency IDs affect THEIR digests.
Existing dirty nodes remain dirty but are not automatically listed as newly
affected. Return {ok:"renamed",key,version:newVersion,renamed:[normalized mapping],
invalidated:[all affected IDs sorted]}, including affected nodes already dirty.
Each first successful rename, including all-self mappings, increases version ONCE
and adds ONE durable audit record {version,op:"rename",ids:[sorted changed IDs]}.
Changed IDs are the union of old/new names whose record is absent on one side or
whose source/priority/deps differs, ignoring digest and provenance. Therefore a
pure self-map has ids:[], but still increments version and stores its receipt.
Preserve all historical receipts verbatim, even when they name targets that were
renamed or removed. Every error preserves graph, caches, version, audit and keys.
Fresh CLI processes and old put/build/commit/import commands continue to work.
'''

SPEC_PATCH = '''
New milestone G2: preserve G1 and add {op:"patch",key,version,edits:[edit,...]}.
Outer fields are exact, key/version follow rename, and edits has 1..6 elements.
Each edit is a legacy put (op/id/source, optional priority/deps with the existing
defaults), a remove (exact op/id), or a rename (exact op/mapping, G1 mapping rules)
WITHOUT inner key/version. Validate ALL edit shapes/types before key replay or
state work. Normalize put defaults/dependency ordering and each rename mapping;
preserve EDIT ARRAY ORDER in the normalized request. Shares the legacy/G1 key
namespace. Replay/conflict precedes stale, which precedes applying state edits.

Apply edits in order to one tentative graph. Removing a missing ID -> missing;
removing a referenced ID is allowed if later edits fix the references. A rename
checks current tentative source existence and collision rules, then rewrites edges
simultaneously. A put can temporarily introduce missing dependencies or cycles.
Check final graph ONLY after all edits: self-cycle -> cycle before missing
dependency -> missing before transitive cycle -> cycle. Failure rolls back all
effects and consumes no key. This permits final-valid graph rewrites whose
intermediate states violate graph constraints.

Cache reconciliation compares FINAL definitions and dependency bindings to each node's pre-patch origin,
then invalidates the final reverse-dependent closure. Changing source and restoring
it within one patch preserves the cache; priority-only changes preserve it too.
Rename preserves provenance. Remove permanently discards provenance: recreating
the same ID is NEW and must be dirty, even with identical content. New node puts
preserve their new provenance until removed. No cache validity is inferred merely
from a coincidentally identical ID or from the intermediate digest field.
Return {ok:"patched",key,version:newVersion,invalidated:[affected IDs sorted]}.
Every first successful patch (even net no-op) increments version ONCE and has ONE
audit event op:"patch", ids computed from before/final definitions as for rename.
All earlier commands, cache rules, failures, durable retries and reads persist.
'''


def known_files(stage_index):
    _stage(stage_index)
    files = deepcopy(base._files(4))
    files[ALLOWED[1]] += EXTENSION_DOMAIN
    files[ALLOWED[2]] += EXTENSION_SERVICE.replace('__EXTENSION_STAGE__', str(stage_index))
    return files


def _stage(stage_index):
    if type(stage_index) is not int or stage_index not in (0, 1):
        raise ValueError('stage_index must be 0 or 1')


def validate_input(stage_index, payload):
    _stage(stage_index)
    base.validate_input(3, payload)
    if stage_index == 0 and any(type(command) is dict and command.get('op') == 'patch'
                                for command in payload['commands']):
        raise ValueError('patch belongs to the second milestone')


def _parse(command, stage_index):
    """Independent reference parser; no shipped candidate helper is invoked."""
    operation = command.get('op') if type(command) is dict else None
    base._fields(command, ('op', 'key', 'version', 'mapping') if operation == 'rename'
                 else ('op', 'key', 'version', 'edits'))
    if operation not in ('rename', 'patch') or (operation == 'patch' and stage_index == 0):
        base._bad()
    base._ident(command['key'])
    base._num(command['version'], 0, 1000000)

    def parse_edit(item):
        if type(item) is not dict or item.get('op') not in ('put', 'remove', 'rename'):
            base._bad()
        kind = item['op']
        if kind == 'put':
            base._fields(item, ('op', 'id', 'source'), ('priority', 'deps'))
            return dict(op=kind, **base._record(dict(id=item['id'], source=item['source'],
                        priority=item.get('priority', 0), deps=item.get('deps', []))))
        if kind == 'remove':
            base._fields(item, ('op', 'id'))
            base._ident(item['id'])
            return dict(op=kind, id=item['id'])
        base._fields(item, ('op', 'mapping'))
        rows = item['mapping']
        if type(rows) is not list or not 1 <= len(rows) <= 6:
            base._bad()
        parsed, sources, targets = [], set(), set()
        for row in rows:
            base._fields(row, ('from', 'to'))
            base._ident(row['from'])
            base._ident(row['to'])
            origin, target = row['from'], row['to']
            if origin in sources or target in targets:
                base._bad()
            sources.add(origin)
            targets.add(target)
            parsed.append({'from': origin, 'to': target})
        return dict(op=kind, mapping=sorted(parsed, key=lambda pair: pair['from']))

    if operation == 'rename':
        result = parse_edit(dict(op='rename', mapping=command['mapping']))
    else:
        if type(command['edits']) is not list or not 1 <= len(command['edits']) <= 6:
            base._bad()
        result = dict(op='patch', edits=[parse_edit(item) for item in command['edits']])
    return dict(result, key=command['key'], version=command['version'])


def _extension(state, command, stage_index):
    normalized = _parse(command, stage_index)
    request = {key: value for key, value in normalized.items() if key != 'key'}
    saved = state['receipts'].get(normalized['key'])
    if saved is not None:
        if saved[0] != request:
            base._bad('conflict')
        return deepcopy(saved[1])
    if normalized['version'] != state['version']:
        base._bad('stale')
    original = deepcopy(state['graph'])
    # Carry original rows by object value rather than a mutable identity table.
    graph = state['graph']
    origins = {identifier: dict(name=identifier, row=deepcopy(row)) for identifier, row in graph.items()}
    edits = [normalized] if normalized['op'] == 'rename' else normalized['edits']
    for edit in edits:
        if edit['op'] == 'put':
            identifier = edit['id']
            if identifier not in graph:
                origins[identifier] = None
            graph[identifier] = {key: deepcopy(edit[key]) for key in ('source', 'priority', 'deps')}
            graph[identifier]['digest'] = None
        elif edit['op'] == 'remove':
            if edit['id'] not in graph:
                base._bad('missing')
            graph.pop(edit['id'])
            origins.pop(edit['id'])
        else:
            translation = {pair['from']: pair['to'] for pair in edit['mapping']}
            if set(translation) - set(graph):
                base._bad('missing')
            if (set(translation.values()) & set(graph)) - set(translation):
                base._bad('collision')
            before, prior_origins = deepcopy(graph), deepcopy(origins)
            graph.clear()
            origins.clear()
            for identifier in before:
                target = translation.get(identifier, identifier)
                graph[target] = before[identifier]
                graph[target]['deps'] = sorted(translation.get(dep, dep) for dep in before[identifier]['deps'])
                origins[target] = prior_origins[identifier]
    base._valid_graph(graph)
    affected = set()
    for identifier, row in graph.items():
        origin = origins[identifier]
        old = None if origin is None else origin['row']
        row['digest'] = None if old is None else old['digest']
        rebound = any(origins[dependency] is None or origins[dependency]['name'] != dependency
                      for dependency in row['deps'])
        if old is None or old['source'] != row['source'] or old['deps'] != row['deps'] or rebound:
            affected.add(identifier)
    # Fixed point intentionally differs from candidate stack-based closure.
    while True:
        grown = affected | {name for name, row in graph.items() if set(row['deps']) & affected}
        if grown == affected:
            break
        affected = grown
    for identifier in affected:
        graph[identifier]['digest'] = None
    def definition(row):
        return None if row is None else tuple(row[key] for key in ('source', 'priority', 'deps'))
    changed = sorted(name for name in set(original) | set(graph)
                     if definition(original.get(name)) != definition(graph.get(name)))
    state['version'] += 1
    response = dict(ok='renamed' if normalized['op'] == 'rename' else 'patched',
                    key=normalized['key'], version=state['version'], invalidated=sorted(affected))
    if normalized['op'] == 'rename':
        response['renamed'] = deepcopy(normalized['mapping'])
    state['audit'].append(dict(version=state['version'], op=normalized['op'], ids=changed))
    state['receipts'][normalized['key']] = (deepcopy(request), deepcopy(response))
    return response


def reference(stage_index, payload):
    validate_input(stage_index, payload)
    state = dict(graph={}, version=0, receipts={}, audit=[])
    answers = []
    for command in payload['commands']:
        trial = deepcopy(state)
        try:
            if type(command) is dict and command.get('op') in ('rename', 'patch'):
                answer = _extension(trial, command, stage_index)
            else:
                answer = base._apply(trial, command, 3)
        except base._Failure as error:
            answer = {'error': str(error)}
        else:
            state = trial
        answers.append(answer)
    return answers


def rename(key, version, *pairs):
    return dict(op='rename', key=key, version=version,
                mapping=[{'from': origin, 'to': target} for origin, target in pairs])


def patch(key, version, *edits):
    return dict(op='patch', key=key, version=version, edits=list(edits))


def _inner_rename(*pairs):
    return {'op': 'rename', 'mapping': rename('unused', 0, *pairs)['mapping']}


def _case(stage, identifier, requirement, commands):
    payload = {'commands': commands}
    return dict(id=f'graph-extension-s{stage + 1}-{identifier}', requirement=requirement,
                input=payload, expected=reference(stage, payload))


P = base.put
L, A = base.LIST, base.AUDIT
B = base.build
C = base.commit
R = base.remove

NEW_PUBLIC = (
    (
        ('rename-basic', 'G1-simultaneous', [P('a'), P('b', deps=['a']), rename('change', 2, ('a', 'z')), L, A]),
        ('rename-collision', 'G1-atomicity', [P('a'), P('b'), rename('change', 2, ('a', 'b')), L, A]),
        ('rename-types', 'G1-validation', [P('a'), rename('change', True, ('a', 'z')), rename('change', 1, ('a', 'z'), ('a', 'x')), L]),
        ('rename-stale', 'G1-retry', [P('a'), rename('change', 0, ('a', 'z')), rename('change', 1, ('a', 'z')), L]),
    ),
    (
        ('patch-basic', 'G2-final-state', [P('a'), patch('edit', 1, P('a', 'new')), L, A]),
        ('patch-final-invalid', 'G2-atomicity', [P('a'), patch('edit', 1, P('b', deps=['absent'])), L, A]),
        ('patch-shape', 'G2-validation', [patch('edit', 0, {'op': 'remove', 'id': True}), patch('edit', 0), L, A]),
        ('patch-stale', 'G2-retry', [P('a'), patch('edit', 0, P('a', 'new')), L, A]),
    ),
)

NEW_PRIVATE = (
    (
        ('swap', 'G1-simultaneous', [P('a', 'A'), P('b', 'B'), rename('swap', 2, ('a', 'b'), ('b', 'a')), L, A]),
        ('rename-cycle', 'G1-simultaneous', [P('a', 'A'), P('b', 'B'), P('c', 'C'), rename('cycle', 3, ('a', 'b'), ('b', 'c'), ('c', 'a')), L]),
        ('deep-cache', 'G1-cache', [P('a'), P('b', deps=['a']), P('c', deps=['b']), P('d', deps=['c']), C('build', 4, 'd'), rename('rename', 5, ('a', 'z')), L, base.READY]),
        ('root-cache', 'G1-cache', [P('a', 'stable'), B('a'), rename('rename', 2, ('a', 'z')), B('z'), L]),
        ('mapping-order-replay', 'G1-retry', [P('a'), P('b'), rename('swap', 2, ('a', 'b'), ('b', 'a')), rename('swap', 2, ('b', 'a'), ('a', 'b')), L, A]),
        ('replay-after-edit', 'G1-retry', [P('a'), rename('rename', 1, ('a', 'z')), P('z', 'new'), rename('rename', 1, ('a', 'z')), L, A]),
        ('historical-receipt', 'G1-history', [P('a'), C('build', 1, 'a'), rename('rename', 2, ('a', 'z')), C('build', 1, 'a'), L, A]),
        ('identity-effect', 'G1-history', [P('a'), B('a'), rename('self', 2, ('a', 'a')), rename('self', 2, ('a', 'a')), base.plan('a'), A]),
        ('missing-before-collision', 'G1-validation', [P('a'), P('b'), rename('r', 2, ('ghost', 'a'), ('a', 'b')), L, A]),
        ('failure-key-reusable', 'G1-atomicity', [P('a'), P('b'), rename('r', 2, ('a', 'b')), rename('r', 2, ('a', 'c')), L, A]),
        ('shared-receipt-namespace', 'G1-history', [P('a'), C('shared', 1, 'a'), rename('shared', 2, ('a', 'z')), L, A]),
        ('diamond-cache', 'G1-cache', [P('a'), P('l', deps=['a']), P('r', deps=['a']), P('j', deps=['l', 'r']), C('all', 4, 'j'), rename('r', 5, ('a', 'z')), C('new', 6, 'j'), L, A]),
        ('swap-fanin-cache', 'G1-cache', [P('a', 'A'), P('b', 'B'), P('join', 'J', deps=['a', 'b']), C('all', 3, 'join'), rename('swap', 4, ('a', 'b'), ('b', 'a')), L, C('after', 5, 'join'), L, A]),
    ),
    (
        ('transient-missing', 'G2-final-state', [P('a'), P('b', deps=['a']), patch('rewrite', 2, R('a'), P('b', 'new', deps=[])), L, A]),
        ('transient-cycle', 'G2-final-state', [P('a'), P('b', deps=['a']), patch('rewrite', 2, P('a', deps=['b']), P('b')), L, A]),
        ('net-revert-cache', 'G2-cache', [P('a', 'stable'), B('a'), patch('revert', 2, P('a', 'temporary'), P('a', 'stable')), B('a'), L, A]),
        ('priority-cache', 'G2-cache', [P('a'), B('a'), patch('priority', 2, P('a', priority=3)), B('a'), L]),
        ('recreate-new-origin', 'G2-provenance', [P('a'), B('a'), patch('replace', 2, R('a'), P('a')), B('a'), L, A]),
        ('version-once', 'G2-history', [patch('many', 0, P('a'), P('b'), P('c')), base.plan('c'), A]),
        ('edit-order-conflict', 'G2-retry', [P('a'), patch('order', 1, P('a', 'A'), P('a', 'B')), patch('order', 1, P('a', 'B'), P('a', 'A')), L, A]),
        ('defaults-replay', 'G2-retry', [patch('new', 0, {'op': 'put', 'id': 'a', 'source': 'x'}), patch('new', 0, P('a')), A]),
        ('late-failure-rollback', 'G2-atomicity', [P('a'), B('a'), patch('bad', 2, P('a', 'new'), R('ghost')), L, A, patch('bad', 2, P('a', 'good')), L]),
        ('compose-rename', 'G2-provenance', [P('a', 'A'), P('b', 'B', deps=['a']), C('build', 2, 'b'), patch('edit', 3, _inner_rename(('a', 'z')), P('z', 'changed'), P('c', deps=['b'])), L, C('after', 4, 'c'), L, A]),
        ('rename-roundtrip', 'G2-provenance', [P('a'), P('b', deps=['a']), C('build', 2, 'b'), patch('roundtrip', 3, _inner_rename(('a', 'z')), _inner_rename(('z', 'a'))), L, A]),
        ('validate-before-state', 'G2-validation', [P('a'), patch('bad', 1, R('ghost'), {'op': 'put', 'id': 'b', 'source': True}), L, A]),
        ('old-import-receipt', 'G2-history', [base.imported('import', base.target('a')), patch('clear', 1, R('a')), base.imported('import', base.target('a')), L, A]),
        ('cycle-before-missing', 'G2-final-state', [P('safe'), patch('bad', 1, P('self', deps=['self', 'missing'])), L, A]),
    ),
)


def _cases(stage, rows):
    return tuple(_case(stage, *row) for row in rows)


_LEGACY_PUBLIC = tuple(case for stage in base.PROJECT['stages'] for case in stage['visible_cases'])
_LEGACY_PRIVATE = tuple(case for stage in base.PROJECT['stages'] for case in stage['hidden_cases'])


def _legacy_cases(rows):
    # The mature final contract, including audit/version, applies from the start.
    return tuple(dict(deepcopy(case), id='legacy-' + case['id'],
                      expected=reference(0, case['input'])) for case in rows)


STAGES = tuple(dict(
    id=f'graph-extension-{index + 1}',
    title=('Simultaneous target rename', 'Atomic graph refactoring')[index],
    specification=('\n'.join(stage['specification'] for stage in base.PROJECT['stages'])
                   + SPEC_RENAME if index == 0 else SPEC_PATCH),
    requirements=((tuple(dict.fromkeys(case['requirement'] for case in _LEGACY_PUBLIC + _LEGACY_PRIVATE))
                   + ('G1-simultaneous', 'G1-cache', 'G1-retry', 'G1-history', 'G1-atomicity', 'G1-validation')) if index == 0
                  else ('G2-final-state', 'G2-cache', 'G2-provenance', 'G2-history', 'G2-retry', 'G2-atomicity', 'G2-validation')),
    visible_cases=(_legacy_cases(_LEGACY_PUBLIC) if index == 0 else ()) + _cases(index, NEW_PUBLIC[index]),
    hidden_cases=(_legacy_cases(_LEGACY_PRIVATE) if index == 0 else ()) + _cases(index, NEW_PRIVATE[index]),
    known_files=known_files(index),
) for index in range(2))

PROJECT = dict(id='graph-patch', title='Durable Build Graph Refactoring', contract=CONTRACT,
               initial_files=base._files(4), allowed_paths=ALLOWED, stages=STAGES)


def control_files(stage_index):
    correct = known_files(stage_index)
    formatted = deepcopy(correct)
    formatted[ALLOWED[2]] += '\n# Semantics-preserving control: trailing comment only.\n'
    return {'correct': correct, 'equivalent-comment': formatted}


def correct_controls(stage_index):
    return [dict(id=identity, files=files) for identity, files in control_files(stage_index).items()]


def rehearsal_probe(stage_index, slot_index):
    _stage(stage_index)
    if type(slot_index) is not int or not 0 <= slot_index <= 9:
        raise ValueError('invalid probe slot')
    name, target = f'probe-{slot_index}', f'moved-{slot_index}'
    commands = [P(name, f'probe source {slot_index}'), B(name)]
    if stage_index == 0:
        commands += [rename('probe-rename', 2, (name, target)), B(target), L, A]
    else:
        commands += [patch('probe-patch', 2, _inner_rename((name, target)), P(target, f'new source {slot_index}')), B(target), L, A]
    return {'commands': commands}


def _variant(stage_index, path, replacements):
    files = known_files(stage_index)
    for before, after in replacements:
        if files[path].count(before) != 1:
            raise AssertionError('Fault mutation needs exactly one source site')
        files[path] = files[path].replace(before, after)
    return files


def fault_bank(stage_index):
    """Return concealed variants, not a claim of physical qualification.

    Each variant is intended to survive the complete public suite and be killed
    by its witness. Root-owned Docker qualification must verify those properties.
    """
    _stage(stage_index)
    service, domain = ALLOWED[2], ALLOWED[1]
    faults = [
        ('direct-invalidation', 'transitive-cache-invalidation', service,
         [("affected = closure(graph, dirty, reverse=True)", "affected = dirty | {name for name, row in graph.items() if set(row['deps']) & dirty}")], 'deep-cache'),
        ('rename-discards-cache', 'rename-cache-preservation', service,
         [("        row['digest'] = prior['digest'] if prior is not None else None", "        row['digest'] = prior['digest'] if prior is not None and lineage[identifier] == identifier else None")], 'root-cache'),
        ('mapping-order-key', 'normalized-retry-identity', domain,
         [("        pairs.sort(key=lambda row: row['from'])\n", "        # Fault: request identity retains mapping order.\n")], 'mapping-order-replay'),
        ('stale-before-replay', 'retry-precedence', service,
         [("        previous = storage.receipt(db, normalized['key'])", "        if normalized['version'] != version:\n            fail('stale')\n        previous = storage.receipt(db, normalized['key'])")], 'replay-after-edit'),
        ('discard-history', 'historical-receipt-retention', service,
         [("        storage.remember(db, normalized['key'], request, answer)", "        db.execute('DELETE FROM receipts')\n        storage.remember(db, normalized['key'], request, answer)")], 'historical-receipt'),
        ('identity-no-effect', 'successful-noop-accounting', service,
         [("        storage.save(db, graph, version + 1, normalized['op'], changed, True)", "        if normalized['op'] == 'rename' and all(row['from'] == row['to'] for row in normalized['mapping']):\n            return answer\n        storage.save(db, graph, version + 1, normalized['op'], changed, True)")], 'identity-effect'),
        ('swap-loses-origin', 'simultaneous-provenance', service,
         [("            renamed[target], origins[target] = revised, lineage[identifier]", "            renamed[target], origins[target] = revised, lineage.get(target, lineage[identifier])")], 'swap'),
        ('fanin-keeps-stale-cache', 'dependency-binding-invalidation', service,
         [("or any(lineage[dependency] != dependency for dependency in row['deps'])", "or False")], 'swap-fanin-cache'),
    ]
    if stage_index == 1:
        faults = [
            ('intermediate-validation', 'final-state-validation', service,
             [("            _edit_graph(graph, lineage, edit)\n", "            _edit_graph(graph, lineage, edit)\n            graph_valid(graph)\n")], 'transient-missing'),
            ('priority-dirties-cache', 'irrelevant-field-cache-invalidation', service,
             [("for key in ('source', 'deps'))", "for key in ('source', 'priority', 'deps'))")], 'priority-cache'),
            ('revert-loses-cache', 'final-state-cache-reconciliation', service,
             [("        if identifier not in graph:\n            lineage[identifier] = None", "        if identifier not in graph or graph[identifier]['source'] != edit['source']:\n            lineage[identifier] = None")], 'net-revert-cache'),
            ('recreate-inherits-origin', 'deleted-node-provenance', service,
             [("            lineage[identifier] = None", "            lineage.setdefault(identifier, None)"), ("        lineage.pop(identifier, None)\n", "        # Fault: removal retains origin by name.\n")], 'recreate-new-origin'),
            ('version-per-edit', 'transaction-accounting', service,
             [("        storage.save(db, graph, version + 1, normalized['op'], changed, True)", "        storage.save(db, graph, version + len(edits), normalized['op'], changed, True)")], 'version-once'),
            ('unordered-edit-key', 'ordered-request-identity', service,
             [("    request = canonical({key: value for key, value in normalized.items() if key != 'key'})", "    identity = {key: value for key, value in normalized.items() if key != 'key'}\n    if identity['op'] == 'patch':\n        identity = dict(identity, edits=sorted(identity['edits'], key=canonical))\n    request = canonical(identity)")], 'edit-order-conflict'),
            faults[0],
        ]
    all_cases = {case['id'].split(f'-s{index + 1}-', 1)[-1]: case
                 for index in range(stage_index + 1) for case in _cases(index, NEW_PRIVATE[index])}
    return [dict(id=f'graph-s{stage_index + 1}-{identifier}', family=family,
                 files=_variant(stage_index, path, replacements),
                 witness_cases=[deepcopy(all_cases[witness])])
            for identifier, family, path, replacements, witness in faults]
