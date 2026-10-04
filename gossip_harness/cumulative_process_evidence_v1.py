"""Host-owned role exit and protected Git evidence for the cumulative study.

Only this owner launches its role processes. A child final message is data;
the parent must reap the actual Popen handle and observe an empty process group.
Checkpoint records are originals under the trusted controller boundary, not
remote attestation. This producer supplies no product acceptance or live permit.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile
import threading
from typing import Any

from . import candidate_http_journal_v3 as stable
from .candidate_checkpoint_chain_v1 import CheckpointChain, PrefixCommitment
from .candidate_observation_admission_v1 import PROTOCOL as SOURCE_PROTOCOL, source_manifest, source_sha256
from .candidate_release_execution_v2 import capture_git_source
from .gitstore import GitStore
from .peer_financial_authority_v2 import canonical_payload
from .peer_financial_terminal_v1 import (
    ChildTerminalSealRequest, EvidenceReference, TerminalRoster, digest, sha,
)

PROTOCOL = 'cumulative-process-evidence-v1'
ROLE_PROTOCOL = 'cumulative-role-process-v1'
CONTEXT_ENV = 'GOSSIP_ROLE_EVIDENCE_CONTEXT'
DIRECTORY_ENV = 'GOSSIP_ROLE_EVIDENCE_DIR'
MAX_ROLE_RECORD = 2_100_000
MAX_GENERATIONS = 2
MAX_ACTION_IDS = 4096
SOURCES = ('cumulative_process_evidence_v1.py', 'candidate_checkpoint_chain_v1.py',
           'candidate_http_journal_v3.py', 'candidate_observation_admission_v1.py',
           'candidate_release_execution_v2.py', 'gitstore.py', 'peer_financial_terminal_v1.py',
           'peer_financial_authority_v2.py')


class ProcessEvidenceError(ValueError):
    """Missing, changed or uncertain process/source evidence; never acceptance."""


def require(ok: bool, message: str) -> None:
    if not ok:
        raise ProcessEvidenceError(message)


def source_fingerprints() -> dict[str, str]:
    root = Path(__file__).parent
    return {'gossip_harness/' + name: sha((root / name).read_bytes()) for name in SOURCES}


def protected_repository(store: GitStore) -> dict:
    require(type(store) is GitStore, 'Actual protected Git store required')
    root = store.path
    require(root.is_absolute() and root.resolve() == root and root.is_dir(), 'Canonical protected store required')
    stat = root.stat()
    marker = stable.read(root / 'gossip-harness-store', max_bytes=4096)
    return {'path': str(root), 'device': stat.st_dev, 'inode': stat.st_ino, 'marker_sha256': sha(marker)}


def _stem(cohort: str) -> str:
    return 'process-' + sha(cohort.encode())[:24]


def closure_name(cohort: str) -> str:
    return _stem(cohort) + '.closure.json'


def confirmation_name(cohort: str) -> str:
    return _stem(cohort) + '.confirmation.json'


def _action_ids(values: Any) -> list[str]:
    require(type(values) in (list, tuple) and len(values) <= MAX_ACTION_IDS, 'Bounded action roster required')
    require(all(type(item) is str and 1 <= len(item) <= 256 for item in values), 'Invalid action identity')
    require(len(set(values)) == len(values), 'Duplicate action identity')
    return list(values)


def _identity(value: Any) -> dict:
    require(type(value) is dict and set(value) == {'cohort', 'trajectory', 'execution_contract_sha256',
            'actor', 'generation', 'launch_id'}, 'Closed process identity required')
    require(all(type(value[key]) is str and value[key] for key in
                ('cohort', 'trajectory', 'actor', 'execution_contract_sha256', 'launch_id')),
            'Invalid process identity')
    require(type(value['generation']) is int and 0 <= value['generation'] < MAX_GENERATIONS,
            'Invalid process generation')
    return value


def publish_role_event(event: str, *, outcome: str | None = None,
                       completed_action_ids: tuple[str, ...] = (),
                       remaining_action_ids: tuple[str, ...] = ()) -> None:
    """Trusted role-runtime helper. Publish immutable bytes before parent wait."""
    require(event in ('ready', 'final'), 'Unknown role event')
    identity = _identity(json.loads(os.environ[CONTEXT_ENV]))
    root = Path(os.environ[DIRECTORY_ENV])
    require(root.is_absolute() and root.resolve() == root and root.is_dir(), 'Canonical evidence directory required')
    value = {'protocol': ROLE_PROTOCOL, 'event': event, 'identity': identity, 'pid': os.getpid()}
    if event == 'final':
        ready = stable.decode(stable.read(root / 'ready.json', max_bytes=MAX_ROLE_RECORD),
                              max_bytes=MAX_ROLE_RECORD)
        require(ready == {**value, 'event': 'ready'}, 'Final event requires the original ready identity')
        require(outcome in ('stopped', 'restart', 'failed'), 'Unknown final role outcome')
        completed, remaining = _action_ids(completed_action_ids), _action_ids(remaining_action_ids)
        require(not set(completed).intersection(remaining), 'Completed and remaining actions overlap')
        value.update(outcome=outcome, completed_action_ids=completed, remaining_action_ids=remaining)
    else:
        require(outcome is None and not completed_action_ids and not remaining_action_ids, 'Ready has no outcome')
        require(not (root / 'final.json').exists(), 'Ready cannot follow a final event')
    raw = canonical_payload(value)
    require(len(raw) <= MAX_ROLE_RECORD, 'Role evidence exceeds bound')
    temporary: str | None = None
    try:
        with tempfile.NamedTemporaryFile(dir=root, prefix='.process-event-', delete=False) as stream:
            temporary = stream.name
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, root / (event + '.json'))  # Atomic and exclusive; no overwrite.
        fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        if temporary is not None:
            os.unlink(temporary)


@dataclass
class _Incarnation:
    actor: str
    generation: int
    identity: dict
    directory: Path
    directory_identity: tuple[int, int]
    intent: EvidenceReference
    process: subprocess.Popen | None = None
    launch: EvidenceReference | None = None
    ready: EvidenceReference | None = None
    final: EvidenceReference | None = None
    wait: EvidenceReference | None = None
    launch_failed: EvidenceReference | None = None


class ProcessEvidence:
    def __init__(self, roster: TerminalRoster, cohort: str, chain: CheckpointChain,
                 expected_head: PrefixCommitment, *, protected_store: GitStore):
        require(type(roster) is TerminalRoster and type(chain) is CheckpointChain,
                'Exact prospective roster and caller-owned checkpoint required')
        TerminalRoster(roster.execution_contract_sha256, roster.children, roster.policy)
        chain.validate_boundary(expected=expected_head)
        matches = [child for child in roster.children if child.cohort == cohort]
        require(len(matches) == 1, 'Unregistered child cohort')
        self.roster, self.child, self.chain = roster, matches[0], chain
        self._pid, self._thread = os.getpid(), threading.get_ident()
        self._frozen = False
        self._incarnations: dict[tuple[str, int], _Incarnation] = {}
        self._sources = source_fingerprints()
        self._protected = protected_repository(protected_store)
        self._store = protected_store
        self._request: ChildTerminalSealRequest | None = None
        self.registration = self._retain(_stem(cohort) + '.registration.json', {
            'protocol': PROTOCOL, 'kind': 'registration', 'roster': roster.record(),
            'roster_sha256': roster.sha256, 'child': self.child.record(),
            'sources': self._sources, 'max_generations': MAX_GENERATIONS,
            'protected_repository': self._protected,
            'parent_pid': self._pid, 'parent_thread': self._thread})

    def _owner(self) -> None:
        require(os.getpid() == self._pid and threading.get_ident() == self._thread,
                'Foreign process or thread cannot use process evidence owner')

    def _current(self) -> None:
        self._owner()
        self.chain.require_current()
        require(source_fingerprints() == self._sources, 'Process evidence source changed')

    def _retain(self, name: str, value: dict, *, cleanup: bool = False) -> EvidenceReference:
        raw = canonical_payload(value)
        self.chain.retain(name, raw, cleanup=cleanup)
        return EvidenceReference(name, sha(raw))

    def _name(self, item: _Incarnation, kind: str) -> str:
        return _stem(self.child.cohort) + '-' + sha(item.actor.encode())[:24] + '-' + str(item.generation) + '.' + kind + '.json'

    def handles(self) -> tuple[tuple[str, int, subprocess.Popen], ...]:
        """Owned handles for explicit cleanup, even after uncertain retention."""
        self._owner()
        return tuple((item.actor, item.generation, item.process) for item in self._incarnations.values()
                     if item.process is not None)

    def launch(self, actor: str, argv: list[str], *, cwd: Path, env: dict[str, str],
               stdout_path: Path, generation: int = 0) -> subprocess.Popen:
        self._current()
        require(not self._frozen, 'Process launch admission is permanently closed')
        require(actor in self.child.actors and type(generation) is int and 0 <= generation < MAX_GENERATIONS,
                'Unregistered role or generation')
        require((actor, generation) not in self._incarnations, 'Generation already attempted; no implicit retry')
        if generation:
            prior = self._incarnations.get((actor, generation - 1))
            require(prior is not None and prior.wait is not None, 'Prior incarnation has no authenticated stop')
        require(type(argv) is list and 0 < len(argv) <= 128
                and all(type(x) is str and '\0' not in x and len(x) <= 32768 for x in argv), 'Invalid argument vector')
        require(type(env) is dict and all(type(k) is type(v) is str and '\0' not in k + v for k, v in env.items()),
                'Explicit string environment required')
        require(not {CONTEXT_ENV, DIRECTORY_ENV}.intersection(env), 'Reserved process evidence environment')
        cwd, stdout_path = Path(cwd), Path(stdout_path)
        directory = stdout_path.parent
        require(cwd.is_absolute() and cwd.resolve() == cwd and cwd.is_dir()
                and stdout_path.is_absolute() and directory.resolve() == directory and directory.is_dir(),
                'Canonical existing process paths required')
        require(not stdout_path.exists() and not stdout_path.is_symlink()
                and not any((directory / name).exists() or (directory / name).is_symlink()
                            for name in ('ready.json', 'final.json')), 'Fresh process evidence paths required')
        stat = directory.stat()
        require(all(item.directory != directory for item in self._incarnations.values()),
                'Incarnations require distinct evidence directories')
        identity = {'cohort': self.child.cohort, 'trajectory': self.child.trajectory,
            'execution_contract_sha256': self.roster.execution_contract_sha256,
            'actor': actor, 'generation': generation, 'launch_id': secrets.token_hex(24)}
        child_env = {**env, CONTEXT_ENV: canonical_payload(identity).decode(), DIRECTORY_ENV: str(directory)}
        intent_value = {'protocol': PROTOCOL, 'kind': 'launch-intent', 'identity': identity,
            'registration': asdict(self.registration), 'argv_sha256': digest(argv),
            'cwd': str(cwd), 'environment_sha256': digest(child_env), 'stdout_path': str(stdout_path),
            'directory': str(directory), 'directory_identity': [stat.st_dev, stat.st_ino],
            'start_new_session': True}
        self.chain.validate_boundary()
        intent = self._retain(_stem(self.child.cohort) + '-' + sha(actor.encode())[:24]
                              + '-' + str(generation) + '.intent.json', intent_value)
        item = _Incarnation(actor, generation, identity, directory, (stat.st_dev, stat.st_ino), intent)
        self._incarnations[actor, generation] = item
        try:
            fd = os.open(stdout_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, 'wb') as stream:
                item.process = subprocess.Popen(list(argv), cwd=cwd, env=child_env,
                    stdin=subprocess.DEVNULL, stdout=stream, stderr=subprocess.STDOUT,
                    start_new_session=True, close_fds=True)
        except OSError as error:
            # Popen reports exec/start failure after cleaning up its child.
            # Other exceptions remain uncertain and cannot justify closure.
            if item.process is None:
                item.launch_failed = self._retain(self._name(item, 'launch-failed'), {
                    'protocol': PROTOCOL, 'kind': 'launch-failed', 'identity': identity,
                    'intent': asdict(intent), 'error_type': type(error).__name__, 'errno': error.errno,
                    'parent_pid': self._pid}, cleanup=True)
            raise
        item.launch = self._retain(self._name(item, 'launch'), {'protocol': PROTOCOL, 'kind': 'launch',
            'identity': identity, 'intent': asdict(intent), 'pid': item.process.pid,
            'process_group': item.process.pid, 'parent_pid': self._pid})
        return item.process

    def _item(self, actor: str, generation: int) -> _Incarnation:
        self._current()
        require((actor, generation) in self._incarnations, 'No owned incarnation')
        return self._incarnations[actor, generation]

    def _record(self, item: _Incarnation, event: str, *, cleanup: bool = False) -> EvidenceReference:
        stat = item.directory.stat()
        require(item.directory.resolve() == item.directory
                and (stat.st_dev, stat.st_ino) == item.directory_identity, 'Process evidence directory changed')
        raw = stable.read(item.directory / (event + '.json'), max_bytes=MAX_ROLE_RECORD)
        value = stable.decode(raw, max_bytes=MAX_ROLE_RECORD)
        keys = {'protocol', 'event', 'identity', 'pid'}
        if event == 'final':
            keys |= {'outcome', 'completed_action_ids', 'remaining_action_ids'}
        require(type(value) is dict and set(value) == keys and canonical_payload(value) == raw,
                'Closed canonical process event required')
        _identity(value['identity'])
        require(item.process is not None and value['protocol'] == ROLE_PROTOCOL and value['event'] == event
                and value['identity'] == item.identity and type(value['pid']) is int
                and value['pid'] == item.process.pid, 'Process event identity differs from owned launch')
        if event == 'final':
            require(value['outcome'] in ('stopped', 'restart', 'failed'), 'Invalid final outcome')
            completed, remaining = _action_ids(value['completed_action_ids']), _action_ids(value['remaining_action_ids'])
            require(not set(completed).intersection(remaining), 'Final action sets overlap')
        name = self._name(item, event)
        reference = getattr(item, event)
        if reference is not None:
            require(reference.read(self.chain) == value, 'Process event changed after observation')
            return reference
        self.chain.retain(name, raw, cleanup=cleanup)
        return EvidenceReference(name, sha(raw))

    def observe_ready(self, actor: str, generation: int = 0) -> EvidenceReference:
        item = self._item(actor, generation)
        require(item.launch is not None, 'Launch publication is incomplete')
        item.ready = self._record(item, 'ready')
        return item.ready

    def observe_exit(self, actor: str, generation: int = 0, *, timeout: float = 0) -> EvidenceReference:
        item = self._item(actor, generation)
        if item.process is None or item.launch is None:
            raise ProcessEvidenceError('Actual acknowledged process launch required')
        if item.wait is not None:
            item.wait.read(self.chain)
            return item.wait
        returncode = item.process.wait(timeout=timeout)
        try:
            os.killpg(item.process.pid, 0)
        except ProcessLookupError:
            pass
        else:
            raise ProcessEvidenceError('Owned process group remains after leader exit')
        errors = []
        for kind in ('ready', 'final'):
            try:
                setattr(item, kind, self._record(item, kind, cleanup=True))
            except (FileNotFoundError, ProcessEvidenceError, stable.JournalError) as error:
                errors.append({'event': kind, 'error_type': type(error).__name__})
        item.wait = self._retain(self._name(item, 'wait'), {'protocol': PROTOCOL, 'kind': 'wait',
            'identity': item.identity, 'launch': asdict(item.launch),
            'ready': asdict(item.ready) if item.ready else None, 'final': asdict(item.final) if item.final else None,
            'pid': item.process.pid, 'returncode': returncode, 'reaped_with': 'Popen.wait',
            'process_group_empty': True, 'event_errors': errors, 'parent_pid': self._pid}, cleanup=True)
        return item.wait

    def finalize(self, protected_store: GitStore, terminal_status: str,
                 expected_head: PrefixCommitment) -> ChildTerminalSealRequest:
        self._current()
        require(not self._frozen, 'Closure already attempted; no implicit retry')
        require(terminal_status in ('completed', 'stopped_failure'), 'Unknown child terminal status')
        self.chain.validate_boundary(expected=expected_head)
        for item in self._incarnations.values():
            require((item.process is None and item.launch_failed is not None)
                    or (item.process is not None and item.wait is not None),
                    'Actual owned process stop or known launch failure is unproven')
        if terminal_status == 'completed':
            for actor in self.child.actors:
                items = [item for item in self._incarnations.values() if item.actor == actor]
                require(bool(items), 'Successful closure requires every registered role')
                last = max(items, key=lambda item: item.generation)
                if last.process is None or last.wait is None or last.ready is None or last.final is None:
                    raise ProcessEvidenceError('Successful role originals missing')
                value = last.final.read(self.chain)
                wait = last.wait.read(self.chain)
                require(last.process.returncode == 0 and value['outcome'] == 'stopped'
                        and not value['remaining_action_ids']
                        and not wait['event_errors']
                        and wait['ready'] == asdict(last.ready) and wait['final'] == asdict(last.final),
                        'Successful role stop unproven')
        self._frozen = True
        require(protected_repository(protected_store) == self._protected, 'Protected Git repository changed')
        cleanup = terminal_status == 'stopped_failure'
        commit = protected_store.head()
        tree, files = capture_git_source(protected_store, commit)
        source = self._retain(_stem(self.child.cohort) + '.source.json', {'protocol': PROTOCOL,
            'kind': 'final-source', 'registration': asdict(self.registration),
            'repository': str(protected_store.path.resolve()), 'commit_oid': commit, 'tree_oid': tree,
            'source_identity_protocol': SOURCE_PROTOCOL, 'source_sha256': source_sha256(files),
            'files': source_manifest(files)}, cleanup=cleanup)
        stops, records = [], []
        for actor in self.child.actors:
            items = sorted((item for item in self._incarnations.values() if item.actor == actor),
                           key=lambda item: item.generation)
            reference = self._retain(_stem(self.child.cohort) + '-' + sha(actor.encode())[:24] + '.stop.json', {
                'protocol': 'financial-role-stop-v1', 'roster_sha256': self.roster.sha256,
                'cohort': self.child.cohort, 'trajectory': self.child.trajectory, 'actor': actor, 'stopped': True},
                cleanup=cleanup)
            stops.append(reference)
            records.append({'actor': actor, 'stop': asdict(reference), 'incarnations': [
                {'generation': item.generation, 'intent': asdict(item.intent),
                 'launch': asdict(item.launch) if item.launch else None,
                 'ready': asdict(item.ready) if item.ready else None,
                 'final': asdict(item.final) if item.final else None,
                 'wait': asdict(item.wait) if item.wait else None,
                 'launch_failed': asdict(item.launch_failed) if item.launch_failed else None} for item in items],
                'never_launched': not any(item.process is not None for item in items)})
        terminal = self._retain(_stem(self.child.cohort) + '.terminal.json', {
            'protocol': 'financial-child-terminal-v1', 'roster_sha256': self.roster.sha256,
            'cohort': self.child.cohort, 'trajectory': self.child.trajectory,
            'status': terminal_status, 'final_source_sha256': source_sha256(files)}, cleanup=cleanup)
        request = ChildTerminalSealRequest(tuple(stops), terminal)
        require(protected_store.head() == commit, 'Protected Git head changed during closure')
        closure = self._retain(closure_name(self.child.cohort), {'protocol': PROTOCOL, 'kind': 'closure',
            'registration': asdict(self.registration), 'source': asdict(source),
            'roles': records, 'request': request.record(), 'launch_admission_closed': True,
            'acceptance_authority': False}, cleanup=cleanup)
        self.chain.validate_boundary()
        require(protected_repository(protected_store) == self._protected
                and protected_store.head() == commit, 'Protected Git head or repository changed after closure')
        self._retain(confirmation_name(self.child.cohort), {
            'protocol': PROTOCOL, 'kind': 'closure-confirmation', 'closure': asdict(closure),
            'source': asdict(source), 'request_sha256': digest(request.record()),
            'protected_repository': self._protected, 'commit_oid': commit}, cleanup=cleanup)
        self._request = request
        return request

    def verified_request(self, expected_head: PrefixCommitment) -> ChildTerminalSealRequest:
        """Owner-only handoff after successful finalize; no partial-closure recovery.

        This is a current observation before the separate financial transaction,
        not an atomic Git/checkpoint/SQL commit or a guarantee of future state.
        """
        self._current()
        if self._request is None:
            raise ProcessEvidenceError('No successfully finalized request')
        self.chain.validate_boundary(expected=expected_head)
        value = stable.decode(self.chain.read(confirmation_name(self.child.cohort)))
        require(value['request_sha256'] == digest(self._request.record()), 'Confirmed request changed')
        source = EvidenceReference(**value['source']).read(self.chain)
        closure = EvidenceReference(**value['closure']).read(self.chain)
        require(closure['request'] == self._request.record() and closure['source'] == value['source'],
                'Confirmed closure changed')
        require(protected_repository(self._store) == self._protected == value['protected_repository']
                and self._store.head() == source['commit_oid'] == value['commit_oid'],
                'Confirmed protected source is no longer current')
        return self._request
