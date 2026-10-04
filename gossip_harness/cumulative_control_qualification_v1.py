"""Original CONTROL qualification of one closed, trusted semantic-conflict fixture.

This is a real local Git/probe producer, not candidate execution, product scope
approval, or an arbitrary host-code runner. Both disjoint branches pass alone;
the clean merge fails the same probe and its descendant repair passes. Review
requests require separately anchored, protected independent reviewer originals.
An incomplete durable intent is unknown and is never dispatched again.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import os
from pathlib import Path
import selectors
import shutil
import signal
import subprocess
import sys
import time
from typing import Any

from . import candidate_checkpoint_chain_v1 as checkpoint
from .candidate_checkpoint_head_v1 import ExternalHead
from . import candidate_http_journal_v3 as stable
from . import candidate_git_source_batch_v1 as git_capture
from . import candidate_observation_admission_v1 as admission
from . import candidate_scope_consumer_v1 as consumer
from . import cumulative_prerequisite_review_v1 as review
from . import project_acceptance_registry_v1 as registry
from .gitstore import GitStore, _run

PROTOCOL = 'cumulative-control-qualification-v1'
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
LABELS = ('base', 'left', 'right', 'merged', 'repaired')
CASE_IDS = tuple('control-' + label for label in LABELS)
FACET_SELECTORS = tuple((name, '/facets/' + name) for name in
    ('semantic_conflict', 'ancestry', 'package_review', 'integration_review'))
OUTPUT_LIMIT = 65536
TIMEOUT_SECONDS = 5
SEED = 0
LIMITS = {'probe_timeout_seconds': 5, 'probe_output_per_stream_bytes': 65536, 'probe_cleanup_seconds': 2,
          'source_capture_per_revision_seconds': 60, 'source_capture_cleanup_seconds': 5, 'source_capture_revisions': 5}
BASE = {'producer.py': 'def produce():\n    return "12"\n',
        'consumer.py': 'def consume(value):\n    return int(value.split()[0])\n'}
LEFT = BASE | {'producer.py': 'def produce():\n    return "12 USD"\n'}
RIGHT = BASE | {'consumer.py': 'def consume(value):\n    return int(value)\n'}
MERGED = LEFT | {'consumer.py': RIGHT['consumer.py']}
REPAIRED = MERGED | {'consumer.py': 'def consume(value):\n    amount, unit = value.split()\n    if unit != "USD":\n        raise ValueError("unexpected unit")\n    return int(amount)\n'}
FIXTURES = dict(zip(LABELS, (BASE, LEFT, RIGHT, MERGED, REPAIRED), strict=True))
PROBE = '''import json,runpy,sys
from pathlib import Path
root=Path(sys.argv[1])
try:
    value=runpy.run_path(str(root/'consumer.py'))['consume'](runpy.run_path(str(root/'producer.py'))['produce']())
    result={'value':value}
    code=0 if type(value) is int and value==12 else 2
except ValueError:
    result={'error':'ValueError'}
    code=1
print(json.dumps(result,sort_keys=True,separators=(',',':')))
raise SystemExit(code)
'''
ENVIRONMENT = {'PATH': '/usr/bin:/bin', 'LC_ALL': 'C', 'LANG': 'C', 'PYTHONHASHSEED': '0'}
require = consumer.require
encoded, digest = review.encoded, review.digest


def implementation_sources() -> dict[str, str]:
    names = ('cumulative_control_qualification_v1.py', 'candidate_git_source_batch_v1.py', 'gitstore.py')
    sources = review.implementation_sources()
    sources.update({'gossip_harness/' + name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
                    for name in names})
    admission.verify_loaded_sources(sources)
    return sources


def runtime_identity() -> dict[str, Any]:
    python = Path(sys.executable).resolve()
    git = shutil.which('git')
    require(git is not None, 'Actual Git executable unavailable')
    assert git is not None
    git_path = Path(git).resolve()
    # GitStore uses its documented scrubbed Git environment; bind the inherited
    # host environment by digest without disclosing credential values in records.
    return {'python': str(python), 'python_sha256': hashlib.sha256(stable.read(python)).hexdigest(),
        'python_version': sys.version, 'git': str(git_path),
        'git_sha256': hashlib.sha256(stable.read(git_path)).hexdigest(),
        'git_host_environment_sha256': digest(dict(os.environ)),
        'probe_environment': ENVIRONMENT, 'probe_sha256': hashlib.sha256(PROBE.encode()).hexdigest(),
        'source_capture': {'protocol': git_capture.PROTOCOL, 'deadline_contract': git_capture.DEADLINE_CONTRACT,
            'per_revision_timeout_seconds': 60, 'revision_count': 5},
        'host_platform': sys.platform, 'isolation': 'closed-authored-fixture-local-process-not-candidate-sandbox'}


def _revision(value: dict[str, Any]) -> consumer.Revision:
    require(type(value) is dict and set(value) == {'commit_oid', 'tree_oid', 'source_sha256'},
            'Exact control revision required')
    return consumer.Revision(**value)


def _recipe(value: dict[str, Any]) -> consumer.ControlRecipe:
    require(type(value) is dict and set(value) == set(consumer.ControlRecipe.__dataclass_fields__),
            'Exact control recipe required')
    body = dict(value)
    for name in LABELS:
        body[name] = _revision(body[name])
    for name in ('left_paths', 'right_paths'):
        body[name] = tuple(body[name])
    body['reviews'] = tuple(consumer.ReviewTarget(**(row | {'source': _revision(row['source'])}))
                            for row in body['reviews'])
    result = consumer.ControlRecipe(**body)
    require(encoded(asdict(result)) == encoded(value), 'Control recipe changes under decoding')
    return result


def _read(raw: bytes) -> dict[str, Any]:
    value = stable.decode(raw, max_bytes=review.MAX_BYTES)
    require(type(value) is dict and encoded(value) == raw, 'Noncanonical control original')
    return value


def _sync(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _complete_capture(result: dict[str, Any], streams: dict[str, bytes]) -> bool:
    # Signals have no authored semantic meaning for this closed trusted probe.
    return bool(result['capture_complete'] and not result['timed_out'] and not result['overflow']
        and result['launch_error'] is None and type(result['exit_code']) is int and result['exit_code'] >= 0
        and result['observed_bytes'] == {kind: len(raw) for kind, raw in streams.items()})


def _semantic_status(probes: tuple[ProbeFact, ...]) -> str:
    if any(row.infrastructure_complete and not row.matches_expected for row in probes):
        return 'failed'
    return 'passed' if all(row.matches_expected for row in probes) else 'infrastructure_error'


@dataclass(frozen=True, slots=True)
class ControlDefinition:
    recipe: consumer.ControlRecipe
    source_revision: consumer.Revision
    product_lineages_sha256: str
    purpose: str
    definition_sha256: str
    runtime: dict[str, Any]
    environment_sha256: str
    limits_sha256: str
    seed_sha256: str
    ordered_case_ids: tuple[str, ...] = CASE_IDS
    facet_selectors: tuple[tuple[str, str], ...] = FACET_SELECTORS


@dataclass(frozen=True, slots=True)
class ProbeFact:
    label: str
    source: consumer.Revision
    intent_name: str
    terminal_name: str
    terminal_sha256: str
    stdout_name: str
    stdout_sha256: str
    stderr_name: str
    stderr_sha256: str
    exit_code: int | None
    stdout: bytes
    stderr: bytes
    infrastructure_complete: bool
    matches_expected: bool


@dataclass(frozen=True, slots=True)
class ControlOriginals:
    recipe: consumer.ControlRecipe
    source_revision: consumer.Revision
    product_lineages_sha256: str
    definition_sha256: str
    purpose: str
    ordered_case_ids: tuple[str, ...]
    facet_selectors: tuple[tuple[str, str], ...]
    probes: tuple[ProbeFact, ...]
    reviews: tuple[review.AuthenticatedReview, ...]
    outcomes: tuple[registry.CaseResult, ...]
    facets: tuple[registry.CaseResult, ...]
    terminal_status: str
    receipt_name: str
    receipt_sha256: str
    verifier_name: str
    verifier_receipt_sha256: str
    checkpoint: checkpoint.PrefixCommitment


class ControlQualification:
    """One immutable purpose/lineage on an externally anchored controller chain.

    The chain and independently retained expected head belong to the caller.
    Reopening authenticates the complete boundary before decoding config. The
    fixture root must initially be absent; interrupted roots are preserved and
    never adopted. Review originals must have a separate caller-owned chain.
    """
    @review.normalized
    def __init__(self, chain: checkpoint.CheckpointChain, expected: checkpoint.PrefixCommitment, *,
                 root: Path, product_lineages_sha256: str, reviewer_ids: tuple[str, str],
                 purpose: str = 'harness_qualification'):
        require(type(chain) is checkpoint.CheckpointChain and type(chain.authority) is ExternalHead
                and type(expected) is checkpoint.PrefixCommitment, 'Actual external control head required')
        chain.validate_boundary(expected=expected)
        assert isinstance(chain.authority, ExternalHead)
        registry.sha256(product_lineages_sha256)
        require(type(reviewer_ids) is tuple and len(reviewer_ids) == 2 and len(set(reviewer_ids)) == 2,
                'Two distinct prospectively enrolled reviewer identities required')
        for identity in reviewer_ids:
            registry.identifier(identity)
        require(purpose in ('harness_qualification', 'registry_qualification'), 'CONTROL cannot use candidate purpose')
        root = Path(root)
        require(root.is_absolute() and root.resolve() == root and root.parent.is_dir()
                and not root.is_symlink(), 'Canonical control root with existing parent required')
        require(all(not (root == path or root.is_relative_to(path) or path.is_relative_to(root))
                    for path in (chain.raw_root, chain.delta_root, chain.authority.root)),
                'Control fixture and all journal roots must be disjoint')
        self.chain, self.expected, self.root = chain, expected, root
        self.sources, self.runtime = implementation_sources(), runtime_identity()
        self.config: dict[str, Any] = {'protocol': PROTOCOL, 'root': str(root), 'product_lineages_sha256': product_lineages_sha256,
            'reviewer_ids': list(reviewer_ids), 'purpose': purpose, 'sources': self.sources, 'runtime': self.runtime,
            'fixture_sha256': digest(FIXTURES), 'case_ids': list(CASE_IDS), 'facets': [list(row) for row in FACET_SELECTORS],
            'limits': LIMITS, 'seed': SEED}
        if chain.has('control-config.json'):
            require(chain.read('control-config.json') == encoded(self.config), 'Control configuration changed')
        else:
            require(not root.exists(), 'A fresh control cannot adopt existing fixture files')
            self._put('control-config.json', self.config)
        self._current()

    def _current(self) -> None:
        self.chain.validate_boundary(expected=self.expected)
        require(implementation_sources() == self.sources and runtime_identity() == self.runtime,
                'Control implementation or execution runtime changed')
        require(self.chain.read('control-config.json') == encoded(self.config), 'Control original config changed')

    def _put(self, name: str, value: dict[str, Any]) -> None:
        raw = encoded(value)
        _read(raw)
        self.chain.retain(name, raw)
        self.expected = self.chain.commitment

    def _get(self, name: str) -> dict[str, Any]:
        return _read(self.chain.read(name))

    def _ref(self, name: str) -> dict[str, Any]:
        raw = self.chain.read(name)
        return {'name': name, 'sha256': hashlib.sha256(raw).hexdigest(), 'position': self.chain.position(name)}

    def _capture_many(self, store: GitStore, revisions: dict[str, str]) -> tuple[dict[str, consumer.Revision], dict[str, list[str]]]:
        require(set(revisions) <= set(LABELS) and bool(revisions), 'Unknown closed control source label')
        result = {}
        for label, oid in revisions.items():
            # New prospective CONTROL identity explicitly pins the reviewed
            # batch helper and its whole-capture60s + separate cleanup5s contract.
            tree, files = git_capture.capture_git_source_batch(store.path, oid, timeout_seconds=60)
            require(files == {name: raw.encode() for name, raw in FIXTURES[label].items()},
                    'Git control source is not the exact closed authored fixture')
            result[label] = consumer.Revision(oid, tree, admission.source_sha256(files))
        rows = store._git('rev-list', '--parents', '--no-walk=unsorted', *revisions.values()).splitlines()
        parsed = [row.split() for row in rows]
        require(len(parsed) == len(revisions) and all(row and row[0] in revisions.values() for row in parsed)
            and len({row[0] for row in parsed}) == len(revisions), 'Actual control commit-parent census differs')
        by_oid = {row[0]: row[1:] for row in parsed}
        return result, {label: by_oid[oid] for label, oid in revisions.items()}

    def _probe_contract(self, source: consumer.Revision, label: str) -> dict[str, Any]:
        return {'protocol': PROTOCOL, 'source': asdict(source), 'label': label,
            'purpose': self.config['purpose'], 'product_lineages_sha256': self.config['product_lineages_sha256'],
            'runtime': self.runtime, 'environment': ENVIRONMENT, 'limits': LIMITS,
            'seed': SEED, 'expected': {'exit_code': 1 if label == 'merged' else 0,
                'stdout': '{"error":"ValueError"}\n' if label == 'merged' else '{"value":12}\n', 'stderr': ''}}

    def _review_definition(self, role: str, source: consumer.Revision) -> dict[str, Any]:
        return {'protocol': PROTOCOL, 'purpose': 'control_review', 'role': role, 'source': asdict(source),
            'fixture_sha256': self.config['fixture_sha256'], 'product_lineages_sha256': self.config['product_lineages_sha256'],
            'qualification_purpose': self.config['purpose'], 'sources': self.sources,
            'reviewed_paths': ['consumer.py'] if role == 'package' else ['consumer.py', 'producer.py']}

    @review.normalized
    def prepare(self) -> ControlDefinition:
        self._current()
        if self.chain.has('control-prepared.json'):
            return self.definition()
        if self.chain.has('control-prepare-intent.json'):
            raise consumer.AuthorityUnavailable('Incomplete control preparation is unknown; never reconstruct or retry')
        require(not self.root.exists(), 'Foreign fixture root before preparation')
        self._put('control-prepare-intent.json', {'protocol': PROTOCOL, 'config_sha256': digest(self.config)})
        self.root.mkdir()
        _sync(self.root.parent)
        store = GitStore.create(self.root / 'fixture.git', BASE)
        base = store.head()
        left = store.propose({'producer.py': LEFT['producer.py']}, base, 'Control producer accepts units')
        right = store.propose({'consumer.py': RIGHT['consumer.py']}, base, 'Control consumer parses exact integer')
        # Actual Git clean merge: no synthetic tree construction or claimed merge flag.
        with store._checkout(left) as checkout:
            _run(checkout, 'fetch', '--no-tags', str(store.path), right)
            merged = _run(checkout, 'merge', '--no-ff', '--no-commit', '--no-edit', right, check=False)
            self.chain.retain('control-merge-stdout.bin', merged.stdout)
            self.expected = self.chain.commitment
            self.chain.retain('control-merge-stderr.bin', merged.stderr)
            self.expected = self.chain.commitment
            require(merged.returncode == 0, 'Authored control Git merge was not clean')
            require(not _run(checkout, 'diff', '--name-only', '--diff-filter=U', '-z').stdout,
                    'Control merge has unresolved paths')
            _run(checkout, 'commit', '-m', 'Control clean semantic merge')
            merge_oid = _run(checkout, 'rev-parse', 'HEAD').stdout.decode().strip()
            store._git('fetch', '--no-tags', str(checkout), merge_oid + ':refs/harness/control-merge')
        repaired = store.propose({'consumer.py': REPAIRED['consumer.py']}, merge_oid, 'Control repair unit-aware consumption')
        revisions, _ = self._capture_many(store, dict(zip(LABELS, (base, left, right, merge_oid, repaired), strict=True)))
        reviews = tuple(consumer.ReviewTarget(role, identity, revisions['repaired'], 'accept',
                    digest(self._review_definition(role, revisions['repaired'])))
            for role, identity in zip(('package', 'integration'), self.config['reviewer_ids'], strict=True))
        recipe = consumer.ControlRecipe(revisions['base'], revisions['left'], revisions['right'],
            revisions['merged'], revisions['repaired'], left_paths=('producer.py',), right_paths=('consumer.py',),
            before_probe_contract_sha256=digest(self._probe_contract(revisions['merged'], 'merged')),
            after_probe_contract_sha256=digest(self._probe_contract(revisions['repaired'], 'repaired')), reviews=reviews)
        st = store.path.stat()
        self._put('control-prepared.json', {'protocol': PROTOCOL, 'recipe': asdict(recipe),
            'repository': {'path': str(store.path), 'device': st.st_dev, 'inode': st.st_ino},
            'merge': {'exit_code': merged.returncode, 'stdout': self._ref('control-merge-stdout.bin'),
                      'stderr': self._ref('control-merge-stderr.bin')}})
        return self.definition()

    @review.normalized
    def definition(self) -> ControlDefinition:
        self._current()
        if not self.chain.has('control-prepared.json'):
            raise consumer.AuthorityUnavailable('Actual control Git preparation is missing')
        value = self._get('control-prepared.json')
        require(set(value) == {'protocol', 'recipe', 'repository', 'merge'} and value['protocol'] == PROTOCOL,
                'Exact original preparation schema required')
        recipe = _recipe(value['recipe'])
        repository = value['repository']
        path = self.root / 'fixture.git'
        require(path.resolve() == path and not path.is_symlink(), 'Control repository path changed')
        st = path.stat()
        require(repository == {'path': str(path), 'device': st.st_dev, 'inode': st.st_ino},
                'Original control repository was substituted')
        store = GitStore(path)
        revisions, parents = self._capture_many(store, {label: getattr(recipe, label).commit_oid for label in LABELS})
        require(all(revisions[label] == getattr(recipe, label) for label in LABELS), 'Actual control revision differs')
        require(store.head() == recipe.base.commit_oid, 'Control protected baseline moved')
        require(parents == {'base': [], 'left': [recipe.base.commit_oid], 'right': [recipe.base.commit_oid],
            'merged': [recipe.left.commit_oid, recipe.right.commit_oid], 'repaired': [recipe.merged.commit_oid]},
            'Control ancestry is not actual two disjoint branches, clean merge and descendant repair')
        # Every actual blob and exact tree entry was checked above, so derive the
        # branch diff from those authenticated fixed trees, not another Git flag.
        for label, expected in (('left', recipe.left_paths), ('right', recipe.right_paths)):
            changed = tuple(sorted(name for name in BASE if BASE[name] != FIXTURES[label][name]))
            require(changed == expected, 'Control branch scope differs')
        require(value['merge'] == {'exit_code': 0, 'stdout': self._ref('control-merge-stdout.bin'),
            'stderr': self._ref('control-merge-stderr.bin')}, 'Original clean merge output differs')
        require(self.chain.position('control-prepare-intent.json') < self.chain.position('control-prepared.json'),
                'Control source construction preceded its durable intent')
        require(recipe.before_probe_contract_sha256 == digest(self._probe_contract(recipe.merged, 'merged'))
            and recipe.after_probe_contract_sha256 == digest(self._probe_contract(recipe.repaired, 'repaired')),
            'Prospective probe contract differs')
        for role, identity, target in zip(('package', 'integration'), self.config['reviewer_ids'], recipe.reviews, strict=True):
            require(target == consumer.ReviewTarget(role, identity, recipe.repaired, 'accept',
                digest(self._review_definition(role, recipe.repaired))), 'Prospective review target differs')
        body = {'protocol': PROTOCOL, 'recipe': asdict(recipe), 'source_revision': asdict(recipe.base),
            'product_lineages_sha256': self.config['product_lineages_sha256'], 'purpose': self.config['purpose'],
            'runtime': self.runtime, 'sources': self.sources, 'ordered_case_ids': CASE_IDS,
            'facet_selectors': FACET_SELECTORS, 'limits': LIMITS, 'seed': SEED}
        self._current()
        return ControlDefinition(recipe, recipe.base, self.config['product_lineages_sha256'], self.config['purpose'],
            digest(body), self.runtime, digest(ENVIRONMENT), digest(body['limits']), digest({'seed': SEED}))

    def _run_probe(self, command: list[str], cwd: Path) -> dict[str, Any]:
        """Bound actual pipes, deadline and owned process-group cleanup separately."""
        process: subprocess.Popen[bytes] | None = None
        raw = {'stdout': bytearray(), 'stderr': bytearray()}
        counts = {'stdout': 0, 'stderr': 0}
        timed_out = overflow = False
        launch_error: str | None = None
        code: int | None = None
        complete = False
        try:
            try:
                process = subprocess.Popen(command, cwd=cwd, env=ENVIRONMENT, stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
            except OSError as error:
                launch_error = type(error).__name__
            if process is not None:
                deadline = time.monotonic() + TIMEOUT_SECONDS
                with selectors.DefaultSelector() as selector:
                    for kind in raw:
                        pipe = getattr(process, kind)
                        assert pipe is not None
                        os.set_blocking(pipe.fileno(), False)
                        selector.register(pipe, selectors.EVENT_READ, kind)
                    while selector.get_map():
                        if time.monotonic() >= deadline:
                            timed_out = True
                            break
                        for key, _ in selector.select(min(.05, max(0, deadline - time.monotonic()))):
                            chunk = os.read(key.fd, 65536)
                            if not chunk:
                                selector.unregister(key.fileobj)
                                continue
                            kind = key.data
                            counts[kind] += len(chunk)
                            raw[kind].extend(chunk[:max(0, OUTPUT_LIMIT - len(raw[kind]))])
                            if counts[kind] > OUTPUT_LIMIT:
                                overflow = True
                        if overflow:
                            break
                    if not timed_out and not overflow:
                        try:
                            code = process.wait(timeout=max(.001, deadline-time.monotonic()))
                            complete = True
                        except subprocess.TimeoutExpired:
                            timed_out = True
        finally:
            pending = sys.exc_info()[1]
            # Every cleanup action is attempted independently, even after a kill,
            # wait or close failure. No journal operation can prevent teardown.
            cleanup_errors = []
            if process is not None:
                try:
                    if process.poll() is None:
                        os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                except BaseException as error:
                    cleanup_errors.append(error)
                try:
                    code = process.wait(timeout=2)
                except BaseException as error:
                    cleanup_errors.append(error)
                for kind in ('stdin', 'stdout', 'stderr'):
                    pipe = getattr(process, kind)
                    if pipe is not None:
                        try:
                            pipe.close()
                        except BaseException as error:
                            cleanup_errors.append(error)
            if cleanup_errors:
                interruption = pending if isinstance(pending, (KeyboardInterrupt, SystemExit, GeneratorExit)) else next(
                    (error for error in cleanup_errors if isinstance(error, (KeyboardInterrupt, SystemExit, GeneratorExit))), None)
                if interruption is not None:
                    interruption.add_note('Control cleanup errors: ' + ','.join(type(error).__name__ for error in cleanup_errors))
                    raise interruption
                raise consumer.AuthorityUnavailable('Owned control process cleanup uncertain: ' +
                    ','.join(type(error).__name__ for error in cleanup_errors))
        return {'exit_code': code, 'stdout': bytes(raw['stdout']), 'stderr': bytes(raw['stderr']),
                'observed_bytes': counts, 'capture_complete': complete, 'timed_out': timed_out,
                'overflow': overflow, 'launch_error': launch_error}

    @review.normalized
    def execute_once(self) -> tuple[ProbeFact, ...]:
        definition = self.definition()
        if self.chain.has('control-execution.json'):
            return self.probes()
        if self.chain.has('control-execution-intent.json'):
            raise consumer.AuthorityUnavailable('Interrupted control probes are unknown; no replay or successor dispatch')
        self._put('control-execution-intent.json', {'protocol': PROTOCOL, 'definition_sha256': definition.definition_sha256})
        for label in LABELS:
            source = getattr(definition.recipe, label)
            contract = self._probe_contract(source, label)
            self._put('control-' + label + '-intent.json', {'protocol': PROTOCOL, 'contract': contract,
                'definition_sha256': definition.definition_sha256})
            workspace = self.root / ('probe-' + label)
            require(not workspace.exists(), 'Control probe cannot adopt a prior workspace')
            workspace.mkdir()
            for name, content in FIXTURES[label].items():
                with (workspace / name).open('xb') as stream:
                    stream.write(content.encode())
                    stream.flush()
                    os.fsync(stream.fileno())
            _sync(workspace)
            _sync(self.root)
            command = [self.runtime['python'], '-I', '-S', '-c', PROBE, str(workspace)]
            self._current()
            result = self._run_probe(command, workspace)
            for kind in ('stdout', 'stderr'):
                name = 'control-' + label + '-' + kind + '.bin'
                self.chain.retain(name, result.pop(kind))
                self.expected = self.chain.commitment
                result[kind] = self._ref(name)
            require({name: stable.read(workspace/name) for name in FIXTURES[label]}
                == {name: value.encode() for name, value in FIXTURES[label].items()}, 'Trusted probe source changed')
            self._put('control-' + label + '-terminal.json', {'protocol': PROTOCOL, 'command': command,
                'source': asdict(source), 'contract_sha256': digest(contract), 'result': result})
        self._put('control-execution.json', {'protocol': PROTOCOL, 'definition_sha256': definition.definition_sha256,
            'probes': [self._ref('control-' + label + '-terminal.json') for label in LABELS]})
        return self.probes()

    @review.normalized
    def probes(self) -> tuple[ProbeFact, ...]:
        return self._probe_facts(self.definition())

    def _probe_facts(self, definition: ControlDefinition) -> tuple[ProbeFact, ...]:
        if not self.chain.has('control-execution.json'):
            raise consumer.AuthorityUnavailable('Original full probe execution is incomplete')
        require(self._get('control-execution-intent.json') == {'protocol': PROTOCOL,
            'definition_sha256': definition.definition_sha256}, 'Prospective full probe definition changed')
        facts = []
        previous = self.chain.position('control-execution-intent.json')
        for label in LABELS:
            prefix = 'control-' + label
            source = getattr(definition.recipe, label)
            contract = self._probe_contract(source, label)
            require(self._get(prefix+'-intent.json') == {'protocol': PROTOCOL, 'contract': contract,
                'definition_sha256': definition.definition_sha256}, 'Probe intent changed')
            body = self._get(prefix+'-terminal.json')
            require(set(body) == {'protocol', 'command', 'source', 'contract_sha256', 'result'}
                and body['protocol'] == PROTOCOL and body['source'] == asdict(source)
                and body['contract_sha256'] == digest(contract)
                and body['command'] == [self.runtime['python'], '-I', '-S', '-c', PROBE, str(self.root/('probe-'+label))],
                'Original probe source/command/contract differs')
            result = body['result']
            require(type(result) is dict and set(result) == {'exit_code', 'stdout', 'stderr', 'observed_bytes',
                'capture_complete', 'timed_out', 'overflow', 'launch_error'}, 'Exact raw process terminal required')
            require((result['exit_code'] is None or type(result['exit_code']) is int)
                and all(type(result[key]) is bool for key in ('capture_complete', 'timed_out', 'overflow'))
                and (result['launch_error'] is None or type(result['launch_error']) is str), 'Typed process terminal required')
            streams = {}
            positions = [previous, self.chain.position(prefix+'-intent.json')]
            for kind in ('stdout', 'stderr'):
                name = prefix+'-'+kind+'.bin'
                require(result[kind] == self._ref(name), 'Original raw probe stream differs')
                streams[kind] = self.chain.read(name)
                count = result['observed_bytes'][kind]
                require(type(count) is int and len(streams[kind]) <= OUTPUT_LIMIT and count >= len(streams[kind]),
                        'Invalid bounded stream census')
                positions.append(self.chain.position(name))
            positions.append(self.chain.position(prefix+'-terminal.json'))
            require(positions == sorted(set(positions)), 'Actual probe/stream chronology differs')
            previous = positions[-1]
            complete = _complete_capture(result, streams)
            expected = contract['expected']
            matches = complete and result['exit_code'] == expected['exit_code'] and all(
                streams[kind] == expected[kind].encode() for kind in streams)
            facts.append(ProbeFact(label, source, prefix+'-intent.json', prefix+'-terminal.json',
                self._ref(prefix+'-terminal.json')['sha256'], prefix+'-stdout.bin', result['stdout']['sha256'],
                prefix+'-stderr.bin', result['stderr']['sha256'], result['exit_code'], streams['stdout'], streams['stderr'],
                complete, matches))
        require(self._get('control-execution.json') == {'protocol': PROTOCOL,
            'definition_sha256': definition.definition_sha256,
            'probes': [self._ref('control-'+label+'-terminal.json') for label in LABELS]}
            and previous < self.chain.position('control-execution.json'), 'Full probe census/chronology differs')
        self._current()
        return tuple(facts)

    def _review_context(self, definition: ControlDefinition, target: consumer.ReviewTarget) -> dict[str, Any]:
        assert isinstance(self.chain.authority, ExternalHead)
        return {'control_original_namespace': {'raw_root': str(self.chain.raw_root), 'delta_root': str(self.chain.delta_root),
                'anchor_root': str(self.chain.authority.root), 'context_sha256': self.expected.context_sha256},
            'definition': self._review_definition(target.role, target.source),
            'definition_sha256': target.definition_sha256, 'qualification_definition_sha256': definition.definition_sha256,
            'recipe': asdict(definition.recipe), 'original_execution': self._ref('control-execution.json'),
            'probe_originals': [self._ref('control-'+label+'-terminal.json') for label in LABELS],
            'raw_probe_originals': [self._ref('control-'+label+'-'+kind+'.bin') for label in LABELS for kind in ('stdout','stderr')]}

    @review.normalized
    def stage_reviews(self, authority: review.OriginalReviewAuthority) -> tuple[review.ReviewRequest, ...]:
        require(type(authority) is review.OriginalReviewAuthority and authority.chain is not self.chain,
                'Independent review requires its separately anchored original journal')
        definition = self.definition()
        self._probe_facts(definition)
        return tuple(authority.stage(purpose='control_review', role=target.role, source=target.source,
            context=self._review_context(definition, target)) for target in definition.recipe.reviews)

    def _reviews(self, definition: ControlDefinition, authority: review.OriginalReviewAuthority) -> tuple[tuple[review.AuthenticatedReview, ...], dict[str, str]]:
        require(type(authority) is review.OriginalReviewAuthority and authority.chain is not self.chain,
                'Separate independently retained review originals required')
        authority._current()  # Authority-wide integrity failure is never a missing review.
        rows = []
        statuses = {}
        for target in definition.recipe.reviews:
            try:
                request = authority.original(purpose='control_review', role=target.role, source=target.source,
                    context=self._review_context(definition, target))
                original = authority.authenticate(request)
            except consumer.AuthorityUnavailable:
                authority._current()
                statuses[target.role] = 'infrastructure_error'
                continue
            require(original.reviewer_id == target.reviewer_id, 'Another reviewer substituted for enrolled control reviewer')
            rows.append(original)
            statuses[target.role] = ('passed' if original.decision == 'accept' else
                                    'failed' if original.decision == 'reject' else 'infrastructure_error')
        require(len({row.reviewer_id for row in rows}) == len(rows), 'Control reviews require distinct actual enrolled identities')
        authority._current()
        return tuple(rows), statuses

    def _receipt(self, definition: ControlDefinition, probes: tuple[ProbeFact, ...],
                 originals: tuple[review.AuthenticatedReview, ...], statuses: dict[str, str],
                 authority: review.OriginalReviewAuthority) -> dict[str, Any]:
        return {'protocol': PROTOCOL, 'definition_sha256': definition.definition_sha256,
            'product_lineages_sha256': definition.product_lineages_sha256, 'purpose': definition.purpose,
            'execution': self._ref('control-execution.json'), 'reviews': [asdict(row) for row in originals],
            'review_checkpoint': asdict(authority.expected), 'review_context_sha256': authority.expected.context_sha256,
            'outcomes': [{'case_id': case, 'status': 'passed' if fact.matches_expected else
                'failed' if fact.infrastructure_complete else 'infrastructure_error'}
                for case, fact in zip(CASE_IDS, probes, strict=True)],
            'facets': {'semantic_conflict': _semantic_status(probes),
                'ancestry': 'passed', 'package_review': statuses['package'],
                'integration_review': statuses['integration']}}

    @review.normalized
    def complete(self, authority: review.OriginalReviewAuthority) -> ControlOriginals:
        definition = self.definition()
        probes = self._probe_facts(definition)
        originals, statuses = self._reviews(definition, authority)
        receipt = self._receipt(definition, probes, originals, statuses, authority)
        pin = digest(receipt)
        receipt_name, verifier_name = 'control-qualification-'+pin+'.json', 'control-verifier-'+pin+'.json'
        if self.chain.has(receipt_name):
            # A receipt with no verifier is interrupted publication, never repaired
            # implicitly. New review delivery makes a distinct explicit snapshot.
            return self.verify_current(authority)
        self._put(receipt_name, receipt)
        require(self.definition() == definition and self._probe_facts(definition) == probes
            and self._reviews(definition, authority) == (originals, statuses),
            'Control originals changed during qualification publication')
        self._put(verifier_name, {'protocol': PROTOCOL, 'sources': self.sources,
            'qualification': self._ref(receipt_name), 'definition_sha256': definition.definition_sha256})
        return self.verify_current(authority)

    @review.normalized
    def verify_current(self, authority: review.OriginalReviewAuthority) -> ControlOriginals:
        definition = self.definition()
        probes = self._probe_facts(definition)
        originals, statuses = self._reviews(definition, authority)
        receipt = self._receipt(definition, probes, originals, statuses, authority)
        pin = digest(receipt)
        receipt_name, verifier_name = 'control-qualification-'+pin+'.json', 'control-verifier-'+pin+'.json'
        if not self.chain.has(receipt_name) or not self.chain.has(verifier_name):
            raise consumer.AuthorityUnavailable('Current original CONTROL publication/verifier incomplete; never inferred')
        require(self._get(receipt_name) == receipt, 'Original CONTROL receipt differs')
        require(self._get(verifier_name) == {'protocol': PROTOCOL, 'sources': self.sources,
            'qualification': self._ref(receipt_name), 'definition_sha256': definition.definition_sha256},
            'Original CONTROL verifier differs')
        require(self.chain.position('control-execution.json') < self.chain.position(receipt_name)
            < self.chain.position(verifier_name), 'CONTROL final publication chronology differs')
        result = ControlOriginals(definition.recipe, definition.source_revision, definition.product_lineages_sha256,
            definition.definition_sha256, definition.purpose, CASE_IDS, FACET_SELECTORS, probes, originals,
            tuple(registry.CaseResult(**row) for row in receipt['outcomes']),
            tuple(registry.CaseResult(name, receipt['facets'][name]) for name, _ in FACET_SELECTORS),
            'completed' if all(row.infrastructure_complete for row in probes)
                and all(value != 'infrastructure_error' for value in statuses.values()) else 'infrastructure_error',
            receipt_name, self._ref(receipt_name)['sha256'], verifier_name, self._ref(verifier_name)['sha256'], self.expected)
        self._current()
        authority._current()
        return result
