"""Read-only authentication of the actual cumulative process producer's records.

A retained Popen.wait record is host evidence from the pinned producer, not a
remote attestation. Prefix order authenticates recording order, not wall time.
This component never grants product acceptance or live-entry authority.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .candidate_checkpoint_chain_v1 import CheckpointChain, PrefixCommitment
from .candidate_observation_admission_v1 import PROTOCOL as SOURCE_PROTOCOL, source_manifest, source_sha256
from .candidate_release_execution_v2 import capture_git_source
from .cumulative_process_evidence_v1 import PROTOCOL, ROLE_PROTOCOL, MAX_GENERATIONS, closure_name, confirmation_name
from .gitstore import GitStore
from .cumulative_study_controller_v1 import Records
from .peer_financial_authority_v2 import canonical_payload
from .peer_financial_terminal_v1 import EvidenceReference, TerminalRoster, require, digest, sha


@dataclass(frozen=True)
class ProcessAudit:
    cohort: str
    trajectory: str
    actors: tuple[str, ...]
    incarnation_count: int
    process_ids: tuple[tuple[str, tuple[int, ...]], ...]
    completed_action_ids: tuple[tuple[str, tuple[str, ...]], ...]
    final_source_sha256: str
    commit_oid: str
    closure_position: int
    confirmation_position: int
    live_qualification: bool = field(default=False, init=False)


def _closed(value: Any, keys: str) -> dict:
    require(type(value) is dict and set(value) == set(keys.split()), 'Closed original process fields differ')
    return value


def _ref(chain: CheckpointChain, value: Any) -> tuple[EvidenceReference, dict, int]:
    _closed(value, 'name sha256')
    ref = EvidenceReference(**value)
    return ref, ref.read(chain), chain.position(ref.name)


def _kind(value: dict, kind: str) -> None:
    require(value.get('protocol') == PROTOCOL and value.get('kind') == kind, 'Process original protocol/kind differs')


def _actions(value: Any) -> tuple[str, ...]:
    require(type(value) is list and len(value) <= 4096
            and all(type(item) is str and 0 < len(item) <= 256 for item in value)
            and len(set(value)) == len(value), 'Invalid original role action census')
    return tuple(value)


def audit_process_closure(chain: CheckpointChain, expected: PrefixCommitment, *, roster: TerminalRoster,
                          cohort: str, expected_sources: dict[str, str],
                          expected_source: dict, runtime_root: Path, execution_repository: Path,
                          python_executable: str) -> ProcessAudit:
    """Authenticate full successful role closure plus original protected Git tree.

    Completed rehearsal requires every expected role. Failure/never-launched
    records remain retainable but cannot become complete rehearsal evidence.
    Caller authenticates the manifest, runtime/source closure and final barrier.
    """
    require(type(chain) is CheckpointChain and type(roster) is TerminalRoster, 'Exact checkpoint and roster required')
    chain.validate_boundary(expected=expected)
    TerminalRoster(roster.execution_contract_sha256, roster.children, roster.policy)
    children = [child for child in roster.children if child.cohort == cohort]
    require(len(children) == 1, 'Unknown rehearsal child')
    child = children[0]
    closure_raw = chain.read(closure_name(cohort))
    from .peer_store_v1 import strict_loads
    closure = strict_loads(closure_raw, max_bytes=2_100_000)
    _closed(closure, 'protocol kind registration source roles request launch_admission_closed acceptance_authority')
    _kind(closure, 'closure')
    closure_position = chain.position(closure_name(cohort))
    require(closure['launch_admission_closed'] is True and closure['acceptance_authority'] is False,
            'Closure is not a completed owner boundary')
    registration_ref, registration, registration_position = _ref(chain, closure['registration'])
    _closed(registration, 'protocol kind roster roster_sha256 child sources max_generations parent_pid parent_thread protected_repository')
    _kind(registration, 'registration')
    require(registration['roster'] == roster.record() and registration['roster_sha256'] == roster.sha256
            and registration['child'] == child.record() and registration['sources'] == expected_sources
            and registration['max_generations'] == MAX_GENERATIONS
            and type(registration['parent_pid']) is int and registration['parent_pid'] > 0
            and type(registration['parent_thread']) is int, 'Original process registration binding differs')
    require(registration_position < closure_position, 'Closure predates registration')
    confirmation = strict_loads(chain.read(confirmation_name(cohort)), max_bytes=2_100_000)
    _closed(confirmation, 'protocol kind closure source request_sha256 protected_repository commit_oid')
    _kind(confirmation, 'closure-confirmation')
    confirmation_position = chain.position(confirmation_name(cohort))
    require(confirmation['closure'] == {'name': closure_name(cohort), 'sha256': sha(closure_raw)}
            and confirmation['source'] == closure['source']
            and confirmation['request_sha256'] == digest(closure['request'])
            and confirmation['protected_repository'] == registration['protected_repository']
            and closure_position < confirmation_position, 'Original closure lacks successful producer confirmation')
    source_ref, source, source_position = _ref(chain, closure['source'])
    _closed(source, 'protocol kind registration repository commit_oid tree_oid source_identity_protocol source_sha256 files')
    _kind(source, 'final-source')
    require(source['registration'] == closure['registration'] and source['source_identity_protocol'] == SOURCE_PROTOCOL,
            'Final source producer binding differs')
    require(source['source_sha256'] == expected_source['source_sha256']
            and source['commit_oid'] == expected_source['commit_oid'], 'Controller and process final source differ')
    require(registration_position < source_position < closure_position, 'Final source order differs')
    rows = closure['roles']
    require(type(rows) is list and tuple(row.get('actor') for row in rows) == child.actors,
            'Original role order or membership differs')
    request = _closed(closure['request'], 'role_stops terminal purpose')
    require(request['purpose'] == 'normal-financial-terminal-v1'
            and type(request['role_stops']) is list and len(request['role_stops']) == len(rows),
            'Original terminal request differs')
    latest_completed = []
    process_ids = []
    launch_ids: set[str] = set()
    count = 0
    intervals: list[tuple[int, int, int]] = []
    for row, stop_ref in zip(rows, request['role_stops'], strict=True):
        _closed(row, 'actor stop incarnations never_launched')
        require(row['never_launched'] is False and row['stop'] == stop_ref,
                'Never-launched role is incomplete rehearsal evidence')
        actor = row['actor']
        _, stop, stop_position = _ref(chain, stop_ref)
        require(stop == {'protocol': 'financial-role-stop-v1', 'roster_sha256': roster.sha256,
                'cohort': cohort, 'trajectory': child.trajectory, 'actor': actor, 'stopped': True},
                'Financial role stop is not bound to exact original role')
        incarnations = row['incarnations']
        require(type(incarnations) is list and 1 <= len(incarnations) <= MAX_GENERATIONS
                and [item.get('generation') for item in incarnations] == list(range(len(incarnations))),
                'Missing or changed incarnation order')
        previous_wait = registration_position
        actor_pids = []
        final_completed: tuple[str, ...] = ()
        for ordinal, item in enumerate(incarnations):
            _closed(item, 'generation intent launch ready final wait launch_failed')
            require(item['launch_failed'] is None and type(item['generation']) is int,
                    'Failed or invalid original incarnation cannot complete rehearsal')
            require(all(item[key] is not None for key in ('intent', 'launch', 'ready', 'final', 'wait')),
                    'Complete rehearsal requires every original incarnation record')
            _, intent, intent_position = _ref(chain, item['intent'])
            _closed(intent, 'protocol kind identity registration argv_sha256 cwd environment_sha256 stdout_path directory directory_identity start_new_session')
            _kind(intent, 'launch-intent')
            identity = _closed(intent['identity'], 'cohort trajectory execution_contract_sha256 actor generation launch_id')
            require({key: identity[key] for key in ('cohort', 'trajectory', 'execution_contract_sha256', 'actor', 'generation')}
                    == {'cohort': cohort, 'trajectory': child.trajectory,
                        'execution_contract_sha256': roster.execution_contract_sha256, 'actor': actor, 'generation': ordinal},
                    'Incarnation original context differs')
            require(type(identity['generation']) is int and type(identity['launch_id']) is str and len(identity['launch_id']) == 48
                    and identity['launch_id'] not in launch_ids, 'Launch identity is missing or reused')
            launch_ids.add(identity['launch_id'])
            launcher_key = 'runtime.' + child.trajectory + '.launcher.' + actor + '.' + str(ordinal)
            launcher = Records(chain).read(launcher_key)
            _closed(launcher, 'argv cwd environment actor generation')
            assert launcher is not None
            directory = runtime_root / 'processes' / actor / str(ordinal)
            environment = launcher['environment']
            require(type(environment) is dict and set(environment) <= {'PATH', 'HOME', 'LANG', 'LC_ALL', 'TMPDIR'}
                    and all(type(k) is type(v) is str for k, v in environment.items()), 'Unregistered role environment')
            argv = [python_executable, '-m', 'gossip_harness.cumulative_study_role_v1',
                    '--role-config', str(runtime_root / 'roles' / actor / 'config.json')]
            require(launcher['argv'] == argv and launcher['cwd'] == str(execution_repository)
                    and launcher['actor'] == actor and type(launcher['generation']) is int
                    and launcher['generation'] == ordinal and intent['argv_sha256'] == digest(argv)
                    and intent['cwd'] == launcher['cwd'] and intent['directory'] == str(directory)
                    and intent['stdout_path'] == str(directory / 'stdout.log')
                    and chain.position(Records.name(launcher_key)) < intent_position
                    and intent['environment_sha256'] == digest({**environment,
                        'GOSSIP_ROLE_EVIDENCE_CONTEXT': canonical_payload(identity).decode(),
                        'GOSSIP_ROLE_EVIDENCE_DIR': str(directory)}), 'Original launch preimage differs')
            info = directory.stat()
            require(directory.resolve() == directory and intent['directory_identity'] == [info.st_dev, info.st_ino],
                    'Original incarnation directory changed')
            require(intent['registration'] == closure['registration'] and intent['start_new_session'] is True,
                    'Intent is outside original process owner')
            _, launch, launch_position = _ref(chain, item['launch'])
            _closed(launch, 'protocol kind identity intent pid process_group parent_pid')
            _kind(launch, 'launch')
            require(launch['identity'] == identity and launch['intent'] == item['intent']
                    and type(launch['pid']) is int and launch['pid'] > 0
                    and launch['process_group'] == launch['pid']
                    and launch['parent_pid'] == registration['parent_pid'], 'Actual owned launch binding differs')
            events = []
            for event in ('ready', 'final'):
                _, value, position = _ref(chain, item[event])
                keys = 'protocol event identity pid'
                if event == 'final':
                    keys += ' outcome completed_action_ids remaining_action_ids'
                _closed(value, keys)
                require(value['protocol'] == ROLE_PROTOCOL and value['event'] == event
                        and value['identity'] == identity and value['pid'] == launch['pid'],
                        'Original role event differs from actual owned launch')
                from . import candidate_http_journal_v3 as stable
                event_path = Path(intent['directory']) / (event + '.json')
                require(event_path.is_absolute() and event_path.resolve() == event_path
                        and stable.read(event_path, max_bytes=2_100_000) == chain.read(item[event]['name']),
                        'Retained original child event file differs')
                events.append((value, position))
            final, final_position = events[1]
            final_completed = _actions(final['completed_action_ids'])
            remaining = _actions(final['remaining_action_ids'])
            require(not set(final_completed).intersection(remaining), 'Final action sets overlap')
            _, wait, wait_position = _ref(chain, item['wait'])
            _closed(wait, 'protocol kind identity launch ready final pid returncode reaped_with process_group_empty event_errors parent_pid')
            _kind(wait, 'wait')
            require(wait['identity'] == identity and all(wait[key] == item[key] for key in ('launch', 'ready', 'final'))
                    and wait['pid'] == launch['pid'] and wait['parent_pid'] == registration['parent_pid']
                    and wait['reaped_with'] == 'Popen.wait' and wait['process_group_empty'] is True
                    and wait['event_errors'] == [] and type(wait['returncode']) is int,
                    'Actual owned wait or child event originals differ')
            require(previous_wait < intent_position < launch_position < events[0][1] < final_position < wait_position
                    < source_position < stop_position < closure_position, 'Original process lifecycle order differs')
            if ordinal == len(incarnations) - 1:
                require(wait['returncode'] == 0 and final['outcome'] == 'stopped' and not remaining,
                        'Latest role incarnation did not stop completely')
            else:
                require(final['outcome'] == 'restart' and wait['returncode'] == 86,
                        'Unexpected original recovery incarnation outcome')
            actor_pids.append(launch['pid'])
            intervals.append((launch['pid'], launch_position, wait_position))
            previous_wait = wait_position
            count += 1
        latest_completed.append((actor, final_completed))
        process_ids.append((actor, tuple(actor_pids)))
    # OS PIDs can legitimately recur after a prior incarnation exited.
    for index, (pid, start, end) in enumerate(intervals):
        require(not any(pid == other and max(start, begin) < min(end, finish)
                        for other, begin, finish in intervals[index + 1:]), 'Concurrent original PID reuse')
    _, terminal, terminal_position = _ref(chain, request['terminal'])
    require(terminal == {'protocol': 'financial-child-terminal-v1', 'roster_sha256': roster.sha256,
            'cohort': cohort, 'trajectory': child.trajectory, 'status': 'completed',
            'final_source_sha256': source['source_sha256']}, 'Original completed financial terminal differs')
    require(all(chain.position(ref['name']) < terminal_position for ref in request['role_stops'])
            and terminal_position < closure_position, 'Terminal precedes original role stops')
    repository = Path(source['repository'])
    require(repository.is_absolute() and repository.resolve() == repository and repository.is_dir(),
            'Original protected Git repository missing or indirect')
    protected = _closed(registration['protected_repository'], 'path device inode marker_sha256')
    from . import candidate_http_journal_v3 as stable
    info = repository.stat()
    require(protected == {'path': str(repository), 'device': info.st_dev, 'inode': info.st_ino,
        'marker_sha256': sha(stable.read(repository / 'gossip-harness-store', max_bytes=4096))}
        and confirmation['commit_oid'] == source['commit_oid'], 'Original protected repository identity changed')
    store = GitStore(repository)  # Read-only constructor and commands only.
    tree, files = capture_git_source(store, source['commit_oid'])
    require(store.head() == source['commit_oid'] and tree == source['tree_oid']
            and source_manifest(files) == source['files'] and source_sha256(files) == source['source_sha256'],
            'Actual final protected Git source differs')
    chain.validate_boundary(expected=expected)
    return ProcessAudit(cohort, child.trajectory, child.actors, count, tuple(process_ids), tuple(latest_completed),
                        source['source_sha256'], source['commit_oid'], closure_position, confirmation_position)
