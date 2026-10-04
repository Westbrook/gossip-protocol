"""Original six-child cumulative rehearsal audit, with live entry kept closed.

The current controller has no complete independent final acceptance producer.
This reader can reconcile real mechanics originals but cannot turn that missing
step into qualification. Neither unit fixtures nor counts/booleans qualify it.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
import sys
import hashlib
import json
import re

from .gitstore import GitStore
from .candidate_release_execution_v2 import capture_git_source
from .candidate_observation_admission_v1 import source_sha256
from .candidate_checkpoint_chain_v1 import CheckpointChain, PrefixCommitment
from .cumulative_rehearsal_inventory_v1 import audit_child_inventory
from .cumulative_rehearsal_process_v1 import audit_process_closure, ProcessAudit
from .cumulative_study_controller_v1 import StudyController, StudyPlan, Release, Records, PublicResult, PACKAGES, MILESTONES, PUBLIC_PURPOSE, REVIEWERS, SOURCE_CLOSURE, TRANSPORT_CONTRACT, package_for, plain
from .financial_rehearsal_originals_v1 import FinancialOriginals, OriginalFile, audit_financial_originals
from .peer_financial_authority_v3 import _gate, _bound_file, _action_limits
from .peer_financial_authority_v4 import REQUIRED_SOURCES as FINANCIAL_SOURCES, REQUIRED_TEST_CLASSES
from .peer_financial_terminal_v1 import ChildTerminalSeal, verified_study_barrier, require, digest, sha
from .peer_mesh_v2 import MeshConfig
from .peer_mesh_store_v2 import MeshLimits
from .peer_role_loop_v2 import WorkDirective, directive_id
from .peer_project_contract_v2 import DispatchBinding, DispatchReply, EvidenceRef, Context, WorkKey, to_dict, from_dict, identity

PROTOCOL = 'cumulative-rehearsal-validator-v1'
REQUIRED_REHEARSAL_CLASSES = (*REQUIRED_TEST_CLASSES,
    'tests/test_financial_rehearsal_originals_v1.py::FinancialRehearsalOriginalsV1Tests',
    'tests/test_cumulative_role_originals_v1.py::CumulativeRoleOriginalsV1Tests',
    'tests/test_cumulative_rehearsal_process_v1.py::CumulativeRehearsalProcessV1Tests',
    'tests/test_cumulative_rehearsal_inventory_v1.py::CumulativeRehearsalInventoryV1Tests',
    'tests/test_cumulative_rehearsal_capsule_v1.py::CumulativeRehearsalCapsuleV1Tests',
    'tests/test_cumulative_rehearsal_validator_v1.py::CumulativeRehearsalValidatorV1Tests')
REQUIRED_READER_SOURCES = (*FINANCIAL_SOURCES, *SOURCE_CLOSURE,
    'gossip_harness/cumulative_rehearsal_capsule_v1.py',
    'gossip_harness/sandbox.py',
    *(name.split('::')[0] for name in REQUIRED_REHEARSAL_CLASSES),
    'gossip_harness/cumulative_study_controller_v1.py',
    'gossip_harness/cumulative_study_runtime_v1.py',
    'gossip_harness/cumulative_study_role_v1.py',
    'gossip_harness/cumulative_process_evidence_v1.py',
    'gossip_harness/cumulative_rehearsal_validator_v1.py',
    'gossip_harness/cumulative_rehearsal_process_v1.py',
    'gossip_harness/cumulative_rehearsal_inventory_v1.py',
    'gossip_harness/cumulative_role_originals_v1.py',
    'gossip_harness/financial_rehearsal_originals_v1.py',
    'gossip_harness/peer_role_loop_v2.py',
    'gossip_harness/peer_mesh_finance_v2.py',
    'gossip_harness/peer_mesh_v2.py',
    'gossip_harness/peer_mesh_store_v2.py')


@dataclass(frozen=True)
class RehearsalAudit:
    protocol: str
    execution_contract_sha256: str
    trajectories: int
    roles: int
    milestone_histories: int
    admitted_calls: int
    known_failed_calls: int
    spent_micro_usd: int
    final_checkpoint: dict
    missing_qualification: tuple[str, ...]
    accepted: bool = field(default=False, init=False)
    live_qualification: bool = field(default=False, init=False)


@dataclass(frozen=True)
class CohortOriginals:
    execution_contract_sha256: str
    child_terminals: tuple[dict, ...]
    process_audits: tuple
    financial_audits: tuple
    barrier: dict
    barrier_position: int
    checkpoint: PrefixCommitment
    accepted: bool = field(default=False, init=False)
    live_qualification: bool = field(default=False, init=False)


class _Originals:
    def __init__(self, chain: CheckpointChain):
        self.chain = chain
        self.records = Records(chain)

    def get(self, key: str) -> dict:
        value = self.records.read(key)
        require(type(value) is dict, 'Missing actual controller slot: ' + key)
        assert value is not None
        return value

    def position(self, key: str) -> int:
        self.get(key)
        return self.chain.position(Records.name(key))

    def before(self, *keys: str) -> None:
        positions = [self.position(key) for key in keys]
        require(all(left < right for left, right in zip(positions, positions[1:])),
                'Original controller record order differs')

    def results(self, key: str, actors: tuple[str, ...]) -> tuple[dict, ...]:
        group = self.get(key)
        require(set(group) == {'originals', 'count'} and type(group['count']) is int
                and group['count'] == len(actors) and type(group['originals']) is list
                and len(group['originals']) == len(actors), 'Original result group count differs')
        results = []
        for index, (ref, actor) in enumerate(zip(group['originals'], actors, strict=True)):
            slot = key + '.original.' + str(index)
            require(type(ref) is dict and set(ref) == {'slot', 'name', 'sha256'}
                    and ref['slot'] == slot and ref['name'] == Records.name(slot), 'Original result slot differs')
            value = self.get(slot)
            require(ref['sha256'] == digest(value) and value['actor'] == actor,
                    'Original result body/actor differs')
            self.before(slot, key)
            results.append(value)
        return tuple(results)


def _publication(originals: _Originals, key: str, kind: str, payload: dict) -> EvidenceRef:
    """Consume the actual original publication, not an unretained derived ref."""
    request = {'kind': kind, 'payload_sha256': digest(payload), 'command': key}
    value = originals.get('publish.' + key)
    ref = from_dict(EvidenceRef, value['ref'])
    require(digest(originals.get('publish.' + key + '.intent')) == digest(request)
            and digest(value) == digest({'ref': to_dict(ref), 'payload': payload, **request})
            and ref.producer == 'seed' and ref.kind == kind and ref.payload_sha256 == digest(payload),
            'Original source/feedback/frontier publication differs')
    originals.before('publish.' + key + '.intent', 'publish.' + key)
    return ref


def _changed(files: dict[str, bytes], changes: dict) -> dict[str, bytes]:
    require(type(changes) is dict and all(type(name) is str and (value is None or type(value) is str)
            for name, value in changes.items()), 'Exact scoped source changes required')
    result = dict(files)
    for name, value in changes.items():
        if value is None:
            result.pop(name, None)
        else:
            result[name] = value.encode()
    return result


def _candidate(store: GitStore, candidate: dict, *, old: str, offered: str,
               result: dict, files: dict[str, bytes], paths: tuple[str, ...] | None) -> str:
    """Read immutable Git trees/refs only; never prepare, accept, or replay CAS."""
    require(set(candidate) == {'status', 'old_head', 'candidate_sha', 'offered_sha', 'detail', 'changed_paths'}
            and candidate['status'] in ('prepared', 'noop') and candidate['old_head'] == old
            and candidate['offered_sha'] == offered and type(candidate['detail']) is str
            and type(candidate['changed_paths']) is list and result['commit_oid'] == candidate['candidate_sha'],
            'Original protected Git candidate/CAS binding differs')
    commit = candidate['candidate_sha']
    changed = store._introduced_paths(old, offered) if candidate['status'] == 'prepared' else ()
    require(candidate['changed_paths'] == list(changed)
            and (paths is None or all(any(name == path.rstrip('/') or name.startswith(path.rstrip('/') + '/')
                for path in paths) for name in changed))
            and (candidate['status'] != 'noop' or commit == old)
            and store.is_ancestor(old, commit) and store.is_ancestor(offered, commit)
            and store._git('rev-parse', store._candidate_ref(old, commit)) == commit,
            'Original prepared history/scope or retained candidate ref differs')
    _, captured = capture_git_source(store, commit)
    require(captured == files and result['source_sha256'] == source_sha256(files),
            'Protected candidate tree is not the exact selected cumulative changes')
    return commit


def _release(originals: _Originals, plan: StudyPlan, index: int, release: Release, store: GitStore,
             old: str, files: dict[str, bytes], previous: str) -> tuple[str, dict[str, bytes]]:
    child = plan.roster.children[index]
    key = 'runtime.' + child.trajectory + '.release.' + release.milestone
    result = originals.get(key)
    require(originals.get(key + '.proposal-intent') == {'old': old, 'changes_sha256': digest(release.files)}
            and result['release_sha256'] == release.sha256
            and all(name not in files or files[name] == value.encode() for name, value in release.files.items()),
            'Requirement release rewrites prior source or starts from a foreign head')
    intent = originals.get(key + '.git-intent')
    require(set(intent) == {'candidate', 'release_sha256'} and intent['release_sha256'] == release.sha256,
            'Original requirement release Git intent differs')
    offered = intent['candidate']['offered_sha']
    expected = _changed(files, release.files)
    require(capture_git_source(store, offered)[1] == expected and store.is_ancestor(old, offered),
            'Original release proposal differs from exact declared additions')
    commit = _candidate(store, intent['candidate'], old=old, offered=offered, result=result,
                        files=expected, paths=tuple(release.files) or None)
    slot = 'child.' + child.trajectory + '.' + release.milestone + '.release'
    require(digest(originals.get(slot)) == digest(result), 'Published release differs from original Git effect')
    originals.before(previous, key + '.proposal-intent', key + '.git-intent', key, slot)
    return commit, expected


def _integration(originals: _Originals, plan: StudyPlan, index: int, stage: str,
                 builds: tuple[dict, ...], reviews: tuple[dict, ...], store: GitStore,
                 old: str, files: dict[str, bytes]) -> tuple[str, dict[str, bytes]]:
    from .peer_project_contract_v2 import strict_loads
    selections, selected_builds = [], []
    all_changes: dict = {}
    rejected = None
    for package in PACKAGES:
        candidates = {item['actor']: item for item in builds if package_for(item['actor']) == package
                      and item['snapshot'].get('reply') is not None and item['snapshot']['reply']['state'] == 'completed'}
        scoped_reviews = [item for item in reviews if package_for(item['actor']) == package]
        require(len(scoped_reviews) == 1, 'Exactly one original scoped reviewer required')
        review = scoped_reviews[0]
        try:
            require(review['snapshot']['reply']['state'] == 'completed', 'Reviewer unavailable')
            decision = strict_loads(review['result_payload']['payload']['changes']['decision.json'].encode())
            require(set(decision) == {'stage_id', 'package', 'selected_actor', 'reasons', 'tests'}
                    and decision['stage_id'] == stage and decision['package'] == package
                    and type(decision['tests']) is list and type(decision['reasons']) is str,
                    'Reviewer selection format differs')
            selected = candidates[decision['selected_actor']]
            changes = selected['result_payload']['payload']['changes']
            require(type(changes) is dict and all(type(name) is str and any(name == path.rstrip('/')
                    or name.startswith(path.rstrip('/') + '/') for path in plan.package_paths[package])
                    for name in changes), 'Builder exceeded package ownership')
            require(not set(changes) & set(all_changes), 'Selected proposals overlap')
            all_changes.update(changes)
            selected_builds.append(selected)
            selections.append({'package': package, 'reviewer': review['actor'], 'selected_actor': selected['actor'],
                'proposal_ref': selected['original_ref'], 'review_ref': review['original_ref'], 'decision': decision})
        except (KeyError, TypeError, ValueError) as error:
            rejected = {'status': 'selection_rejected', 'package': package, 'reason': str(error),
                        'selections': selections, 'acceptance_authority': False}
            break
    integrated = originals.get(stage + '.integration')
    if rejected is not None:
        require(digest(integrated) == digest(rejected) and all(not originals.chain.has(Records.name(stage + '.merge.' + p + suffix))
                for p in PACKAGES for suffix in ('.proposal-intent', '.proposal', '.git-intent', '')),
                'Rejected selection cannot claim a merge or hide a Git effect')
        return old, files
    merges = []
    previous = stage + '.reviews'
    base, base_files = old, files
    for selected, package in zip(selected_builds, PACKAGES, strict=True):
        key, pin = stage + '.merge.' + package, digest(selected)
        request, changes = selected['worker_request'], selected['result_payload']['payload']['changes']
        require(request['base_sha'] == base and request['files'] == {name: raw.decode() for name, raw in base_files.items()},
                'Selected worker source differs from original generation source')
        private_path = store.path.parent / 'private-git' / hashlib.sha256((stage + selected['actor']).encode()).hexdigest()
        require(originals.get(key + '.proposal-intent') == {'proposal_sha256': pin, 'base_sha': base, 'git_path': str(private_path)},
                'Original private proposal identity/base/path differs')
        proposal = originals.get(key + '.proposal')
        require(set(proposal) == {'proposal_sha256', 'git_path', 'offered_sha', 'base_sha'}
                and proposal['proposal_sha256'] == pin and proposal['git_path'] == str(private_path) and proposal['base_sha'] == base,
                'Original private proposal result differs')
        private = GitStore(private_path)
        offered = proposal['offered_sha']
        require(capture_git_source(private, offered)[1] == _changed(base_files, changes)
                and private.is_ancestor(base, offered)
                and private._git('rev-parse', 'refs/harness/proposals/' + offered) == offered,
                'Private Git offer does not contain exact selected proposal')
        intent, result = originals.get(key + '.git-intent'), originals.get(key)
        require(set(intent) == {'proposal_sha256', 'candidate'} and intent['proposal_sha256'] == pin
                and set(result) == {'proposal_sha256', 'private_git', 'commit_oid', 'source_sha256'}
                and result['proposal_sha256'] == pin and digest(result['private_git']) == digest(proposal),
                'Original private merge identity differs')
        files = _changed(files, changes)
        old = _candidate(store, intent['candidate'], old=old, offered=offered, result=result,
                         files=files, paths=plan.package_paths[package])
        originals.before(previous, key + '.proposal-intent', key + '.proposal', key + '.git-intent', key)
        previous = key
        merges.append(result)
    require(digest(integrated) == digest({'status': 'integrated', 'commit_oid': old, 'source_sha256': source_sha256(files),
            'selections': selections, 'private_git_merges': merges, 'acceptance_authority': False}),
            'Integration summary differs from original selections and protected Git effects')
    originals.before(previous, stage + '.integration')
    return old, files


def _histories(originals: _Originals, plan: StudyPlan, index: int, protected_path: Path) -> tuple[dict[str, tuple[dict, ...]], dict]:
    trajectory = plan.cohort.trajectories[index]
    child = plan.roster.children[index]
    key = 'child.' + trajectory.id
    begin = originals.get(key + '.begin')
    require(begin['trajectory'] == plain(asdict(trajectory)) and begin['contract_sha256'] == plan.sha256
            and type(begin['started_at']) in (int, float) and type(begin['deadline']) in (int, float)
            and 0 < begin['deadline'] - begin['started_at'] <= plan.horizon_seconds + 1,
            'Actual trajectory registration/limits differ')
    terminal = originals.get(key + '.terminal')
    require(terminal['trajectory_id'] == trajectory.id and terminal['status'] == 'completed'
            and terminal['milestone_records'] == [key + '.' + m + '.terminal' for m in MILESTONES]
            and terminal['acceptance_authority'] is False, 'Full rehearsal requires completed original four-milestone child')
    store = GitStore(protected_path)
    require(store.head() == terminal['final_source']['commit_oid'], 'Final protected branch differs from child terminal')
    results: dict[str, list[dict]] = {actor: [] for actor in child.actors}
    init = 'runtime.' + child.trajectory + '.source-initialization'
    initial = originals.get(init + '.result')
    require(originals.get(init + '.intent') == {'path': str(protected_path), 'initial_files_sha256': digest(plan.initial_files)}
            and initial['files'] == plan.initial_files
            and set(initial) == {'commit_oid', 'source_sha256', 'files'}, 'Original initial source differs from plan')
    current_commit = initial['commit_oid']
    current_files = {name: body.encode() for name, body in plan.initial_files.items()}
    require(capture_git_source(store, current_commit)[1] == current_files
            and initial['source_sha256'] == source_sha256(current_files)
            and store.is_ancestor(current_commit, terminal['final_source']['commit_oid']), 'Initial Git source is foreign')
    originals.before(key + '.begin', init + '.intent', init + '.result')
    # Calling this pure method without its writing constructor cannot enroll or
    # dispatch work; the frozen producer remains the instruction authority.
    producer = object.__new__(StudyController)
    producer.plan = plan
    previous = init + '.result'
    for release in plan.releases:
        mkey = key + '.' + release.milestone
        current_commit, current_files = _release(originals, plan, index, release, store, current_commit, current_files, previous)
        current_source = source_sha256(current_files)
        milestone = originals.get(mkey + '.terminal')
        require(milestone['milestone'] == release.milestone and milestone['status'] == 'public_completed'
                and milestone['acceptance_authority'] is False, 'Missing completed original milestone')
        reached = False
        prior_generation = mkey + '.release'
        for generation in range(plan.source_generations):
            gkey = mkey + '.g' + str(generation)
            if reached:
                require(not originals.chain.has(Records.name(gkey + '.terminal')),
                        'Controller continued after successful source generation')
                continue
            value = originals.get(gkey + '.terminal')
            seeded = tuple(sorted(child.actors, key=lambda actor: digest({'seed': trajectory.block_seed_sha256, 'actor': actor})))
            build_actors = tuple(actor for actor in seeded if actor.rsplit('.', 1)[-1] not in REVIEWERS)
            review_actors = tuple(actor for actor in seeded if actor.rsplit('.', 1)[-1] in REVIEWERS)
            source_ref = _publication(originals, gkey + '.source', 'project-source',
                {'files': {name: raw.decode() for name, raw in current_files.items()}, 'base_sha': current_commit})
            originals.before(prior_generation, 'publish.' + gkey + '.source')
            evidence: tuple[EvidenceRef, ...] = ()
            if generation:
                feedback = originals.get(mkey + '.g' + str(generation - 1) + '.terminal')
                evidence = (_publication(originals, gkey + '.feedback', 'cumulative-feedback', feedback),)
                originals.before('publish.' + gkey + '.source', 'publish.' + gkey + '.feedback')
            groups: dict[str, tuple[dict, ...]] = {}
            for suffix, actors in (('builds', build_actors), ('reviews', review_actors)):
                group = originals.results(gkey + '.' + suffix, actors)
                expected_directives = dict(producer._directives(index, plan.releases.index(release) + 1,
                    generation, source_ref, evidence if suffix == 'builds' else (), reviews=suffix == 'reviews'))
                if suffix == 'reviews':
                    for actor, directive in expected_directives.items():
                        package = package_for(actor)
                        scoped = [{'actor': item['actor'], 'directive_id': item['directive_id'],
                            'original_ref': item['original_ref'], 'snapshot': item['snapshot'], 'proposal': item['result_payload']}
                            for item in groups['builds'] if package_for(item['actor']) == package]
                        frontier = _publication(originals, gkey + '.frontier.' + package, 'cumulative-frontier',
                            {'stage_id': gkey, 'package': package, 'builds': scoped, 'shared_source_ref': to_dict(source_ref)})
                        originals.before(gkey + '.builds', 'publish.' + gkey + '.frontier.' + package)
                        expected_directives[actor] = replace(directive, evidence_refs=(frontier,))
                offered = []
                for row in group:
                    dkey = 'runtime.' + child.trajectory + '.directive.' + row['directive_id']
                    registration = originals.get(dkey)
                    directive = WorkDirective.from_dict(registration['directive'])
                    require(digest(registration) == digest({'actor': row['actor'], 'directive': expected_directives[row['actor']].to_dict()})
                            and directive_id(directive) == row['directive_id'], 'Exact prospective directive/source/evidence differs')
                    prerequisite = ('publish.' + gkey + '.frontier.' + package_for(row['actor']) if suffix == 'reviews'
                        else 'publish.' + gkey + ('.feedback' if generation else '.source'))
                    originals.before(prerequisite, dkey, gkey + '.' + suffix)
                    scope = digest({'source': to_dict(directive.source_ref), 'evidence': [to_dict(ref) for ref in directive.evidence_refs]})
                    offered.append((row, scope))
                scopes = {scope for _, scope in offered}
                for row, scope in offered:
                    stage = gkey + ('.build' if suffix == 'builds' else '.review')
                    if len(scopes) > 1:
                        stage += '.' + scope[:12]
                    require(row['stage_id'] == stage, 'Controller result stage/scope differs')
                    results[row['actor']].append(row)
                groups[suffix] = group
            next_commit, next_files = _integration(originals, plan, index, gkey, groups['builds'], groups['reviews'],
                                                   store, current_commit, current_files)
            originals.before(prior_generation, gkey + '.builds', gkey + '.reviews', gkey + '.integration',
                             gkey + '.public-evaluation.intent', gkey + '.public-evaluation.result', gkey + '.terminal')
            evaluation = originals.get(gkey + '.public-evaluation.result')
            result = PublicResult(**{**evaluation, 'ordered_check_ids': tuple(evaluation['ordered_check_ids']),
                'outcomes': tuple(tuple(row) for row in evaluation['outcomes'])})
            result.validate(release, value['commit_oid'], value['source_after'])
            require(result.status == 'completed' and type(result.raw_receipt) is dict,
                    'Original public evaluation is unavailable')
            require(originals.get(gkey + '.public-evaluation.intent') == {
                'source_sha256': value['source_after'], 'commit_oid': value['commit_oid'],
                'release_sha256': release.sha256, 'purpose': PUBLIC_PURPOSE}, 'Evaluation intent binding differs')
            integrated = originals.get(gkey + '.integration')
            _, generation_files = capture_git_source(store, value['commit_oid'])
            require(value['commit_oid'] == next_commit and generation_files == next_files
                    and value['source_before'] == current_source
                    and source_sha256(generation_files) == value['source_after']
                    and store.is_ancestor(value['commit_oid'], terminal['final_source']['commit_oid']),
                    'Actual source-generation bytes or cumulative lineage differs')
            current_source = value['source_after']
            current_commit, current_files = next_commit, next_files
            require(value['milestone'] == release.milestone and type(value['generation']) is int
                    and value['generation'] == generation and value['public_result'] == evaluation
                    and value['integration'] == integrated and value['acceptance_authority'] is False
                    and value['public_passed'] is (result.passed and integrated['status'] == 'integrated'),
                    'Original generation completion differs')
            # Original sandbox output and exact ordered authored case census are
            # consumed, rather than trusting the controller's derived passed flag.
            receipt = result.raw_receipt['sandbox']
            require(receipt['schema_version'] == 1 and receipt['image_id'] == plan.runtime['image']
                    and receipt['command'] == list(release.command)
                    and receipt['checks_sha256'] == hashlib.sha256(json.dumps(release.checks, sort_keys=True,
                        ensure_ascii=True, separators=(',', ':')).encode()).hexdigest()
                    and receipt['timeout_seconds'] == plan.runtime['public_timeout_seconds']
                    and receipt['timed_out'] is False
                    and type(receipt['exit_code']) is int
                    and ((receipt['exit_code'] == 0) is result.passed)
                    and type(receipt['container_name']) is str
                    and re.fullmatch(r'gossip-check-[0-9a-f]{32}', receipt['container_name']) is not None
                    and receipt['staging'] == {'files': len(generation_files),
                        'bytes': sum(map(len, generation_files.values())), 'excluded_paths': ['.git']},
                    'Original public evaluator/source staging/runtime binding differs')
            from .peer_store_v1 import strict_loads
            parsed = strict_loads(receipt['output'].encode(), max_bytes=2_100_000)
            require(parsed == {'purpose': PUBLIC_PURPOSE, 'outcomes': [list(row) for row in result.outcomes]}
                    and receipt['cleanup_verified'] is True and receipt.get('output_truncated') is not True
                    and receipt['status'] == ('passed' if result.passed else 'failed'),
                    'Original public output/case census differs')
            reached = value['public_passed']
            prior_generation = gkey + '.terminal'
        require(reached, 'Bounded repair history never reached a complete public result')
        require(milestone['source']['source_sha256'] == current_source
                and milestone['source']['commit_oid'] == value['commit_oid']
                and {name: body.encode() for name, body in milestone['source']['files'].items()} == generation_files,
                'Milestone final source body differs from original Git')
        originals.before(prior_generation, mkey + '.terminal')
        previous = mkey + '.terminal'
    require(digest(terminal['final_source']) == digest({'commit_oid': current_commit,
        'source_sha256': source_sha256(current_files), 'files': {name: raw.decode() for name, raw in current_files.items()}}),
        'Terminal full source differs from final authenticated milestone')
    originals.before(previous, key + '.terminal')
    return {actor: tuple(values) for actor, values in results.items()}, terminal


def _fault_publication_barrier(originals: _Originals, plan: StudyPlan, index: int) -> None:
    trajectory, child = plan.cohort.trajectories[index], plan.roster.children[index]
    key = 'runtime.' + child.trajectory
    stage = 'child.' + child.trajectory + '.M1.g0.build'
    actors = tuple(sorted((actor for actor in child.actors if actor.rsplit('.', 1)[-1] not in REVIEWERS),
                   key=lambda actor: digest({'seed': trajectory.block_seed_sha256, 'actor': actor})))
    publications = [stage + '.work']
    if trajectory.decision_placement == 'durable_central_scheduler':
        for actor in actors:
            publications.extend((stage + '.decision.' + actor, stage + '.assignment.' + actor))
    def reference(slot: str) -> dict:
        name = Records.name(slot)
        return {'name': name, 'sha256': sha(originals.chain.read(name)), 'position': originals.position(slot)}
    frontier = originals.get(key + '.partition-frontier')
    require(frontier == {'stage': stage, 'partition_start': reference(key + '.partition-start.result'),
            'publication_keys': publications}, 'Original fault/publication frontier differs')
    originals.before(key + '.partition-start.result', key + '.partition-frontier')
    prior = key + '.partition-frontier'
    for publication in publications:
        slot = 'publish.' + publication
        originals.before(prior, slot + '.intent', slot)
        prior = slot
    first = originals.get(key + '.partition-first-publication')
    start = originals.get(key + '.partition-start.result')
    require(set(first) == {'boundary', 'publication_key', 'ref', 'observed_at'}
            and first['boundary'] == 'first-work-publication-durably-acknowledged'
            and first['publication_key'] == publications[0]
            and first['ref'] == originals.get('publish.' + publications[0])['ref']
            and type(first['observed_at']) in (int, float) and first['observed_at'] >= start['observed_at'],
            'Original first-publication observation differs')
    originals.before('publish.' + publications[0], key + '.partition-first-publication', key + '.partition-frontier-complete')
    complete = originals.get(key + '.partition-frontier-complete')
    require(complete == {'stage': stage, 'publications': [reference('publish.' + name) for name in publications]},
            'Original fault frontier completion differs')
    originals.before(prior, key + '.partition-frontier-complete', key + '.partition-end.intent')


def _faults(originals: _Originals, plan: StudyPlan, index: int, process: ProcessAudit,
            role_results: dict[str, tuple[dict, ...]], root: Path) -> None:
    trajectory, child = plan.cohort.trajectories[index], plan.roster.children[index]
    key = 'runtime.' + child.trajectory
    target = next(actor for actor in child.actors if actor.endswith('.B01'))
    compound = trajectory.block == 'compound_recovery'
    require(all(len(pids) == (2 if compound and actor == target else 1)
                for actor, pids in process.process_ids), 'Actual restart incarnation census differs from fault contract')
    if not compound:
        require(not any(originals.chain.has(Records.name(key + suffix)) for suffix in
                        ('.restart-original', '.restart-completed', '.partition-start.intent', '.partition-end.intent')),
                'Healthy trajectory contains an unregistered fault')
        return
    _fault_publication_barrier(originals, plan, index)
    marker, restarted = originals.get(key + '.restart-original'), originals.get(key + '.restart-completed')
    from . import candidate_http_journal_v3 as stable
    from .peer_financial_authority_v2 import canonical_payload
    require(stable.read(root / 'roles' / target / 'restart-fault.json', max_bytes=2_100_000) == canonical_payload(marker),
            'Original restart marker bytes differ')
    require(marker['actor'] == target and marker['pid'] == dict(process.process_ids)[target][0]
            and marker['config_sha256'] == digest(originals.get(key + '.role-config.' + target))
            and marker['fault'] == 'known_result_before_publication_builder_restart'
            and restarted == {'actor': target, 'generation': 1}, 'Actual restart boundary differs')
    matching = [value for value in role_results[target] if value['directive_id'] == marker['directive_id']]
    require(len(matching) == 1 and matching[0]['snapshot']['reply'] == marker['reply'],
            'Restart marker lacks the same original terminal financial reply')
    reply = from_dict(DispatchReply, marker['reply'])
    require(reply.state == 'completed' and reply.binding is not None
            and reply.binding.action.actor == target and reply.binding.action.kind == 'build'
            and reply.binding.action.context.milestone == 1 and reply.binding.action.work.generation == 0,
            'Restart was outside the prescribed known-result boundary')
    originals.before(key + '.restart-original', key + '.launcher.' + target + '.1', key + '.restart-completed')
    peers = child.actors + ('seed', 'finance')
    events = []
    for suffix, enabled in (('.partition-start', True), ('.partition-end', False)):
        intent, result = originals.get(key + suffix + '.intent'), originals.get(key + suffix + '.result')
        require(intent == {'enabled': enabled, 'target': target} and result['enabled'] is enabled
                and result['target'] == target and type(result['observed_at']) in (int, float)
                and type(result['receipts']) is list
                and tuple(row['actor'] for row in result['receipts']) == peers,
                'Original partition intent/response census differs')
        for row in result['receipts']:
            blocked = sorted(peer for peer in peers if peer != target) if row['actor'] == target else [target]
            require(row['response'] == {'blocked': blocked if enabled else []},
                    'Actual partition response did not apply the prescribed peer links')
        originals.before(key + suffix + '.intent', key + suffix + '.result')
        events.append(result)
    originals.before(key + '.partition-start.result', key + '.partition-end.intent', key + '.originals')
    require(events[1]['observed_at'] - events[0]['observed_at'] >= plan.partition_seconds,
            'Actual successful recovery partition ended before its registered duration')


def _checked_profiles(config: dict, permit: dict) -> None:
    """Bind V3's enriched financial profiles to the bare approved manifests."""
    from .peer_financial_authority_v3 import FIXTURE_TRANSPORT, LIVE_TRANSPORT
    require(config['mode'] in ('fixture', 'live') and permit['mode'] == config['mode'],
            'Financial/permit observation mode differs')
    observation = 'provider' if config['mode'] == 'live' else 'simulated'
    transport = LIVE_TRANSPORT if config['mode'] == 'live' else FIXTURE_TRANSPORT
    require(config['observation_kind'] == observation and config['contract']['transport_identity'] == transport
            and type(permit['profiles']) is dict and bool(permit['profiles'])
            and all(type(profile) is dict and set(profile) == {'manifest', 'timeout'} for profile in permit['profiles'].values()),
            'Original bare financial profile or transport differs')
    expected = {name: {**profile, 'transport_identity': transport, 'observation_kind': observation}
                for name, profile in permit['profiles'].items()}
    require(digest(config['profiles']) == digest(expected), 'Enriched financial profiles differ from approved bare manifests')


def checked_design_envelope(envelope: dict, plan: StudyPlan) -> tuple[dict, ...]:
    """Validate structure/linkage; caller must independently authenticate bytes."""
    require(type(envelope) is dict and set(envelope) == {'protocol', 'execution_contract_sha256',
        'terminal_roster', 'sources', 'runtime', 'profiles', 'children'}
        and envelope['protocol'] == 'cumulative-execution-design-envelope-v1'
        and envelope['execution_contract_sha256'] == plan.sha256
        and digest(envelope['terminal_roster']) == digest(plan.roster.record())
        and digest(envelope['sources']) == digest(plan.source_pins) and digest(envelope['runtime']) == digest(plan.runtime)
        and digest(envelope['profiles']) == plan.cohort.model_profiles_sha256
        and type(envelope['children']) is list and len(envelope['children']) == 6,
        'Independent complete execution-design envelope differs')
    designs = []
    for entry, child in zip(envelope['children'], plan.roster.children, strict=True):
        require(type(entry) is dict and set(entry) == {'cohort', 'trajectory', 'execution_design'}
                and entry['cohort'] == child.cohort and entry['trajectory'] == child.trajectory,
                'Ordered independently approved child design differs')
        design = entry['execution_design']
        require(type(design) is dict and digest(design.get('terminal_roster')) == digest(plan.roster.record())
                and digest(design.get('profiles')) == digest(envelope['profiles'])
                and type(design.get('max_workers')) is int and design['max_workers'] == plan.executor_slots,
                'Child design shared policy differs')
        limits = _action_limits(design.get('action_limits'))
        require(set(limits['by_actor']) == set(child.actors), 'Child design role-limit membership differs')
        designs.append(design)
    return tuple(designs)


def audit_cohort_originals(chain: CheckpointChain, expected: PrefixCommitment, *, plan: StudyPlan,
                             ledger_identity: dict, repository: Path, gate: dict,
                             ordered_test_classes: tuple[str, ...], expected_design_envelope: dict) -> CohortOriginals:
    """Audit actual complete mechanics; unavailable acceptance remains explicit.

    Caller owns the exact chain and independent expected head on this thread.
    Historical proof open/close must be serialized when invoked by finance
    workers. This method never opens a wallet, creates a role, or retries work.
    """
    require(type(chain) is CheckpointChain and type(plan) is StudyPlan, 'Exact owned proof and prospective plan required')
    plan.__post_init__()
    require(set(REQUIRED_READER_SOURCES) <= set(plan.source_pins), 'Full reader/controller/financial source closure missing')
    plan.verify_sources(repository)
    require(type(ordered_test_classes) is tuple and len(ordered_test_classes) == len(set(ordered_test_classes))
            and set(REQUIRED_REHEARSAL_CLASSES) <= set(ordered_test_classes), 'Exact ordered qualification suite missing')
    expected_designs = checked_design_envelope(expected_design_envelope, plan)
    _gate(gate, plan.source_pins, {'required_test_classes': list(ordered_test_classes)})
    summary, _ = _bound_file(gate['summary'])
    require(tuple(job['class'] for job in summary['jobs']) == ordered_test_classes,
            'Qualification gate class order or scope differs')
    chain.validate_boundary(expected=expected)
    originals = _Originals(chain)
    require(originals.get('contract') == plan.record() and originals.get('ledger') == ledger_identity,
            'Original plan or cumulative wallet identity differs')
    receipts = []
    terminals, process_audits, financial_audits = [], [], []
    prior_terminal: str | None = None
    next_opening: int | None = None
    global_cap: int | None = None
    for index, child in enumerate(plan.roster.children):
        ckey, rkey = 'child.' + child.trajectory, 'runtime.' + child.trajectory
        originals.before('contract', ckey + '.begin')
        if prior_terminal is not None:
            originals.before(prior_terminal, ckey + '.begin', rkey + '.financial-config')
        manifest = originals.get(rkey + '.originals')
        require(manifest['writer_lifetimes_closed'] is True and manifest['ledger_identity'] == ledger_identity
                and manifest['cohort'] == child.cohort, 'Original child lifetime/ledger identity differs')
        root = Path(manifest['original_paths']['runtime_root'])
        expected_paths = {'runtime_root': str(root), 'financial_service': str(root / 'financial-service'),
            'provider_journals': str(root / 'provider-journals'), 'financial_payloads': str(root / 'financial-payloads'),
            'finance_mesh': str(root / 'finance-mesh'), 'seed_mesh': str(root / 'seed-mesh'),
            'protected_git': str(root / 'protected.git'), 'roles': {actor: str(root / 'roles' / actor) for actor in child.actors},
            'processes': str(root / 'processes')}
        require(manifest['original_paths'] == expected_paths, 'Original child paths escaped the authenticated inventory')
        audit_child_inventory(root, manifest['complete_child_files'])
        role_results, terminal = _histories(originals, plan, index, Path(manifest['original_paths']['protected_git']))
        file_paths = {item['path'] for item in manifest['complete_child_files']}
        require(all(path in file_paths and value['path'] == path for path, value in manifest['database_identities'].items())
                and manifest['payload_index_identity'] == manifest['database_identities'][str(root / 'financial-payloads' / 'payload-index.sqlite')]
                and manifest['mesh_database_identity'] == manifest['database_identities'][str(root / 'finance-mesh' / 'mesh.sqlite')],
                'Original database identity is outside complete child inventory')
        config, permit, contract = (originals.get(rkey + suffix) for suffix in
                                    ('.financial-config', '.permit', '.financial-contract'))
        _checked_profiles(config, permit)
        require(manifest['config'] == config and manifest['config_sha256'] == digest(config)
                and config['operator_permit'] == permit and config['operator_permit_sha256'] == digest(permit)
                and config['contract'] == contract and permit['cohort_contract_sha256'] == digest(contract)
                and permit['mode'] == 'fixture' and permit['qualification'] is None
                and permit['sources'] == plan.source_pins
                and digest(permit['execution_design']) == digest(expected_designs[index])
                and permit['execution_design_sha256'] == digest(permit['execution_design'])
                and permit['execution_design']['terminal_roster'] == plan.roster.record()
                and digest(permit['profiles']) == plan.cohort.model_profiles_sha256
                and permit['max_workers'] == plan.executor_slots
                and contract['journal_root'] == str(root / 'provider-journals'),
                'Original permit/config/source/profile/contract binding differs')
        _action_limits(permit['execution_design']['action_limits'])
        task_specs = []
        for number, release in enumerate(plan.releases, 1):
            context = Context(plan.sha256, child.cohort, child.trajectory, number, release.sha256)
            for generation in range(plan.source_generations):
                for actor in child.actors:
                    role_name = actor.rsplit('.', 1)[-1]
                    review = role_name in REVIEWERS
                    task_specs.append({'context': to_dict(context),
                        'work': to_dict(WorkKey(package_for(actor), release.milestone, role_name, generation)),
                        'actors': [actor], 'kinds': ['review' if review else 'build' if generation == 0 else 'repair'],
                        'profiles': ['strong' if review else 'mini'],
                        'allowed_paths': ['decision.json'] if review else list(plan.package_paths[package_for(actor)]),
                        'max_reserved_units': plan.runtime['max_reserved_units']})
        require(contract['task_specs'] == task_specs and contract['execution_contract_sha256'] == plan.sha256,
                'Original financial admitted task universe differs from prospective four milestones')
        process = audit_process_closure(chain, expected, roster=plan.roster, cohort=child.cohort,
            expected_sources={key: plan.source_pins[key] for key in
                __import__('gossip_harness.cumulative_process_evidence_v1', fromlist=['source_fingerprints']).source_fingerprints()},
            expected_source=terminal['final_source'], runtime_root=Path(manifest['original_paths']['runtime_root']),
            execution_repository=repository, python_executable=sys.executable)
        from .cumulative_process_evidence_v1 import closure_name
        from .candidate_http_journal_v3 import decode
        closure = decode(chain.read(closure_name(child.cohort)))
        require(originals.position(rkey + '.source-initialization.result')
                < chain.position(closure['registration']['name']) < originals.position(rkey + '.financial-config'),
                'Initial source did not precede process registration and financial enrollment')
        from .cumulative_role_originals_v1 import audit_role_originals
        dispatches: list[DispatchBinding] = []
        role_replies: list[DispatchReply] = []
        transport = originals.get(rkey + '.transport')
        finance_port = originals.get(rkey + '.finance-port')['port']
        def mesh(actor: str, directory: Path) -> MeshConfig:
            return MeshConfig(directory, actor, child.cohort, plan.sha256, (*child.actors, 'seed', 'finance'),
                transport['transport_key'], observer_key=transport['observer_key'], interval=.05, fanout=2,
                limits=MeshLimits(max_payloads=TRANSPORT_CONTRACT['max_payloads'],
                    max_events=TRANSPORT_CONTRACT['max_events'],
                    max_reserved_bytes=TRANSPORT_CONTRACT['max_reserved_bytes']))
        require(manifest['mesh_identity'] == mesh('finance', root / 'finance-mesh').identity(),
                'Actual finance mesh differs from prospective transport contract')
        for actor, final_actions in process.completed_action_ids:
            role_config = originals.get(rkey + '.role-config.' + actor)
            require(role_config['mesh'] == plain({**asdict(mesh(actor, root / 'roles' / actor / 'mesh')),
                        'root': str(root / 'roles' / actor / 'mesh')})
                    and role_config['finance'] == {'port': finance_port, 'capability': transport['capabilities'][actor],
                        'contract_sha256': plan.sha256}
                    and role_config['deadline_unix'] == originals.get(ckey + '.begin')['deadline']
                    and role_config['placement'] == plan.cohort.trajectories[index].decision_placement
                    and role_config['call_limit'] == role_config['max_actions'] == 4 * plan.source_generations
                    and role_config['actor'] == actor and role_config['trajectory_id'] == child.trajectory
                    and role_config['policy_sha256'] == plan.cohort.shared_policy_sha256
                    and role_config['requirements_by_milestone'] == {str(i): release.sha256 for i, release in enumerate(plan.releases, 1)}
                    and role_config['finance']['contract_sha256'] == plan.sha256
                    and role_config['mesh']['cohort_id'] == child.cohort
                    and role_config['mesh']['execution_contract_sha256'] == plan.sha256
                    and role_config['mesh']['root'] == str(root / 'roles' / actor / 'mesh')
                    and role_config['crash_once'] is (plan.cohort.trajectories[index].block == 'compound_recovery' and actor.endswith('.B01')),
                    'Actual role limits or placement differ')
            role = audit_role_originals(actor=actor, role_config=role_config,
                role_root=Path(manifest['original_paths']['roles'][actor]),
                database_identities=manifest['database_identities'], controller_results=role_results[actor],
                expected_final_action_ids=final_actions)
            require(set(role.observed_process_ids) <= set(dict(process.process_ids)[actor]),
                    'Original role actions/history include a foreign process')
            dispatches.extend(role.dispatches)
            role_replies.extend(role.terminal_replies)
        _faults(originals, plan, index, process, role_results, root)
        declared = tuple(from_dict(DispatchBinding, value) for value in manifest['dispatches'])
        require(sorted(map(identity, declared)) == sorted(map(identity, dispatches)),
                'Original role admission and financial manifest membership differ')
        facts = audit_financial_originals(FinancialOriginals(ledger_identity, child.cohort, config, digest(config),
            manifest['payload_index_identity'], manifest['payload_config'], manifest['mesh_database_identity'],
            manifest['mesh_identity'], tuple(OriginalFile(**item) for item in manifest['journal_files']), tuple(dispatches), tuple(role_replies)))
        require((next_opening is None or facts.opening_micro_usd == next_opening)
                and facts.opening_micro_usd == permit['expected_opening_usage']
                and (global_cap is None or facts.global_cap_micro_usd == global_cap),
                'Original cumulative wallet opening/cap was reset or changed')
        next_opening = facts.opening_micro_usd + facts.spent_micro_usd
        global_cap = facts.global_cap_micro_usd
        terminals.append(terminal)
        process_audits.append(process)
        financial_audits.append(facts)
        seal = terminal['financial_seal']
        receipt = ChildTerminalSeal(seal['raw_utf8'].encode(), seal['publication_name'], PrefixCommitment(**seal['commitment']))
        require(process.confirmation_position < chain.position(receipt.publication_name)
                < originals.position(rkey + '.originals') < originals.position(ckey + '.terminal'),
                'Actual process closure, financial seal, writer close or child terminal order differs')
        receipts.append(receipt)
        audit_child_inventory(Path(manifest['original_paths']['runtime_root']), manifest['complete_child_files'])
        prior_terminal = ckey + '.terminal'
    barrier = verified_study_barrier(plan.roster, chain, expected, tuple(receipts),
        existing_ledger_path=Path(ledger_identity['path']), expected_ledger_identity=ledger_identity)
    retained = originals.get('complete-cohort-barrier')
    require({key:value for key,value in barrier.items() if key != 'checkpoint'} ==
            {key:value for key,value in retained.items() if key != 'checkpoint'}, 'Original complete-cohort barrier differs')
    assert prior_terminal is not None
    originals.before(prior_terminal, 'complete-cohort-barrier')
    chain.validate_boundary(expected=expected)
    plan.verify_sources(repository)
    return CohortOriginals(plan.sha256, tuple(terminals), tuple(process_audits), tuple(financial_audits),
                           retained, originals.position('complete-cohort-barrier'), expected)


def audit_original_rehearsal(chain: CheckpointChain, expected: PrefixCommitment, *, plan: StudyPlan,
                             ledger_identity: dict, repository: Path, gate: dict,
                             ordered_test_classes: tuple[str, ...], expected_design_envelope: dict) -> RehearsalAudit:
    facts = audit_cohort_originals(chain, expected, plan=plan, ledger_identity=ledger_identity,
                                  repository=repository, gate=gate, ordered_test_classes=ordered_test_classes,
                                  expected_design_envelope=expected_design_envelope)
    originals = _Originals(chain)
    originals.before('complete-cohort-barrier', 'final-acceptance.intent', 'final-acceptance.result')
    intent, result = originals.get('final-acceptance.intent'), originals.get('final-acceptance.result')
    require(intent['purpose'] == 'independent-acceptance' and intent['barrier_sha256'] == digest(facts.barrier)
            and intent['study_sha256'] == plan.sha256, 'Final independent evaluation did not follow exact global barrier')
    require(result.get('accepted') is False and result.get('status') == 'unavailable',
            'Unknown final acceptance producer: no accepted-boolean qualification path exists')
    chain.validate_boundary(expected=expected)
    plan.verify_sources(repository)
    return RehearsalAudit(PROTOCOL, plan.sha256, 6, 96, 24,
        sum(item.admitted for item in facts.financial_audits),
        sum(item.known_failures for item in facts.financial_audits),
        sum(item.spent_micro_usd for item in facts.financial_audits), asdict(expected),
        ('complete independent final acceptance producer and original receipt',
         'one complete physical rehearsal of the final matching execution contract'))
