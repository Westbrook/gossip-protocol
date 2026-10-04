"""Original V2 source promotion, requiring independent final integration review.

Package choices and mechanical CAS are structural evidence. They do not supply
an integration review, CONTROL qualification or independent product acceptance.
All readback uses stopped originals; no Git, role or financial owner is opened.
"""
from __future__ import annotations

from dataclasses import asdict, replace
from functools import wraps
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
from typing import Any, cast

from . import candidate_observation_admission_v1 as admission
from . import candidate_scope_consumer_v1 as consumer
from . import cumulative_terminal_originals_v2 as terminal
from . import financial_rehearsal_originals_v1 as financial
from . import project_acceptance_registry_v1 as registry
from .candidate_release_execution_v2 import capture_git_source
from .cumulative_rehearsal_inventory_v1 import audit_child_inventory
from .cumulative_rehearsal_process_v1 import audit_process_closure
from .cumulative_rehearsal_validator_v1 import (
    _Originals, _integration, _publication, _release, REQUIRED_READER_SOURCES,
)
from .cumulative_role_originals_v1 import audit_role_originals
from .cumulative_study_controller_v2 import (
    StudyController, StudyPlan, Records, PublicResult, PACKAGES, MILESTONES,
    PUBLIC_PURPOSE, REVIEWERS, TRANSPORT_CONTRACT, package_for, plain,
)
from .gitstore import GitStore
from .peer_financial_authority_v2 import canonical_payload
from .peer_financial_authority_v3 import FIXTURE_TRANSPORT, LIVE_TRANSPORT
from .peer_financial_authority_v5 import CumulativeAuthorityV5
from .peer_financial_terminal_v1 import census, digest, require, sha, seal_row
from .peer_financial_terminal_v2 import checked_financial_config
from .peer_mesh_v2 import MeshConfig
from .peer_mesh_store_v2 import MeshLimits
from .peer_project_contract_v2 import DispatchBinding, DispatchReply, EvidenceRef, encode, identity, from_dict, to_dict
from .peer_role_loop_v2 import WorkDirective, directive_id
from .peer_store_v1 import strict_loads

PROTOCOL = 'cumulative-source-promotion-v1'
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
POLICY = {
    'protocol': PROTOCOL,
    'history': 'all-four-public-milestones-exact-role-finance-private-git-cas',
    'integration_review': 'independent-original-exact-final-source-and-history-required',
    'missing': 'never-promoted',
    'control_qualification': False,
    'product_acceptance': False,
}


class _TrackedOriginals(_Originals):
    def __init__(self, chain):
        super().__init__(chain)
        self.references: dict[str, dict[str, Any]] = {}

    def get(self, key: str) -> dict:
        value = super().get(key)
        name = Records.name(key)
        self.references[key] = {'slot': key, 'name': name, 'sha256': sha(self.chain.read(name)),
                                'position': self.chain.position(name)}
        return value


class _FinancialReadV5(financial._FinancialRead, CumulativeAuthorityV5):
    """Read facade constructor plus concrete V5 canonical terminal verification.

    Only _action/_audit_membership/_verify_terminal are used. SQL is mode=ro;
    inherited public lifecycle, dispatch and recovery methods are never invoked.
    """


def _histories_v2(originals: _Originals, plan: StudyPlan, index: int, protected_path: Path) -> tuple[dict[str, tuple[dict, ...]], dict]:
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
            and initial['source_sha256'] == admission.source_sha256(current_files)
            and store.is_ancestor(current_commit, terminal['final_source']['commit_oid']), 'Initial Git source is foreign')
    originals.before(key + '.begin', init + '.intent', init + '.result')
    # Reconstruct exact directives with the concrete V2 producer, without its writing constructor.
    producer = object.__new__(StudyController)
    producer.plan = plan
    previous = init + '.result'
    for release in plan.releases:
        mkey = key + '.' + release.milestone
        # This source-pinned helper only reads common Git/record fields; no V1
        # controller, financial owner or V1 plan/receipt is constructed here.
        current_commit, current_files = cast(Any, _release)(originals, plan, index, release, store, current_commit, current_files, previous)
        current_source = admission.source_sha256(current_files)
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
            next_commit, next_files = cast(Any, _integration)(originals, plan, index, gkey, groups['builds'], groups['reviews'],
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
                    and admission.source_sha256(generation_files) == value['source_after']
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
        'source_sha256': admission.source_sha256(current_files), 'files': {name: raw.decode() for name, raw in current_files.items()}}),
        'Terminal full source differs from final authenticated milestone')
    originals.before(previous, key + '.terminal')
    return {actor: tuple(values) for actor, values in results.items()}, terminal


def _audit_finance_v5(originals: financial.FinancialOriginals) -> financial.FinancialAudit:
    """Reconcile actual immutable V5 SQL, request journals and mesh publications.

    The manifest's exact dispatch list must come from the authenticated original
    controller and role journals; this function proves its equality to financial
    admission. It does not prove controller execution or qualification by itself.
    """
    require(type(originals) is financial.FinancialOriginals, 'Typed financial originals required')
    config = originals.config
    checked_financial_config(config)
    mode = config.get('mode')
    require(mode in ('fixture', 'live') and config.get('observation_kind') ==
            ('provider' if mode == 'live' else 'simulated') and config['operator_permit']['mode'] == mode
            and digest(config) == originals.config_sha256, 'Exact V5 financial origin required')
    transport = LIVE_TRANSPORT if mode == 'live' else FIXTURE_TRANSPORT
    expected_profiles = {name: {**profile, 'transport_identity': transport,
        'observation_kind': config['observation_kind']} for name, profile in config['operator_permit']['profiles'].items()}
    require(config['contract']['transport_identity'] == transport and digest(config['profiles']) == digest(expected_profiles),
            'V5 profile/transport origin differs')
    contract = config['contract']
    require(contract['cohort_id'] == originals.cohort and contract['ledger_identity'] == originals.ledger_identity
            and contract['execution_contract_sha256'] == config['terminal_config']['roster']['execution_contract_sha256'],
            'Original financial cohort/study/ledger binding differs')
    require(type(originals.dispatches) is tuple and len(originals.dispatches) <= financial.MAX_CALLS
            and all(type(item) is DispatchBinding for item in originals.dispatches), 'Invalid bounded dispatch census')
    require(type(originals.terminal_replies) is tuple and len(originals.terminal_replies) == len(originals.dispatches)
            and all(type(reply) is DispatchReply and reply.binding is not None for reply in originals.terminal_replies),
            'Independent original role terminal reply census required')
    require(type(originals.journal_files) is tuple and len(originals.journal_files) <= 4 * financial.MAX_CALLS
            and all(type(item) is financial.OriginalFile for item in originals.journal_files), 'Invalid original journal inventory')
    files = {item.path: item for item in originals.journal_files}
    require(len(files) == len(originals.journal_files), 'Duplicate original journal path')
    for item in files.values():
        item.read()
    with financial.readonly_database(originals.ledger_identity) as ledger, \
            financial.readonly_database(originals.payload_index_identity) as index, \
            financial.readonly_database(originals.mesh_database_identity) as mesh:
        financial._financial_bounds(ledger, index, originals.cohort)
        cohort = ledger.execute('SELECT * FROM financial_cohorts_v2 WHERE cohort=?', (originals.cohort,)).fetchone()
        require(cohort is not None and cohort['config'] == canonical_payload(config)
                and cohort['config_sha'] == originals.config_sha256 and cohort['halted'] == 0,
                'Original cohort configuration is missing, changed or halted')
        terminal = seal_row(ledger, originals.cohort)
        require(terminal is not None and terminal['snapshot_sha'] == sha(terminal['snapshot'])
                and terminal['receipt_sha'] == sha(terminal['receipt'])
                and census(ledger, originals.cohort, originals.config_sha256) == terminal['snapshot'],
                'Original financial seal/census changed')
        assert terminal is not None
        seal = strict_loads(terminal['receipt'], max_bytes=2_100_000)
        require(seal['snapshot_sha256'] == terminal['snapshot_sha'] and seal['seal_id'] == terminal['seal_id']
                and seal['financial_config_sha256'] == originals.config_sha256
                and digest({key: value for key, value in seal.items() if key != 'seal_id'}) == seal['seal_id'],
                'Seal does not bind original financial snapshot')
        request_count = ledger.execute('SELECT COUNT(*) FROM financial_requests_v2 WHERE cohort=?',
                                       (originals.cohort,)).fetchone()[0]
        require(request_count <= financial.MAX_CALLS, 'Oversized original request census')
        payloads = financial._ReadPayloads(originals, index, mesh)
        view = _FinancialReadV5(originals, ledger, payloads)
        view._audit_membership(ledger)
        requested = {(item.action.actor, item.action.request_id): item for item in originals.dispatches}
        require(len(requested) == len(originals.dispatches), 'Duplicate controller dispatch identity')
        replies = {(reply.binding.action.actor, reply.request_id): reply for reply in originals.terminal_replies
                   if reply.binding is not None}
        require(len(replies) == len(originals.terminal_replies) and set(replies) == set(requested),
                'Independent role reply/dispatch membership differs')
        rows = list(ledger.execute('SELECT * FROM financial_actions_v2 WHERE cohort=? ORDER BY actor,request_id',
                                   (originals.cohort,)))
        require(len(rows) == request_count == len(requested)
                and {(row['actor'], row['request_id']) for row in rows} == set(requested),
                'Controller, financial requests and admitted actions differ')
        journal_paths: set[str] = set()
        journal_locks: set[str] = set()
        result_keys: set[tuple[str, str]] = set()
        spent = failed = 0
        for row in rows:
            require(row['state'] in {'completed', 'failed'}, 'Incomplete or unknown admitted work cannot qualify')
            _, binding, reply, request = view._action(row['actor'], row['request_id'])
            require(encode(binding) == encode(requested[(row['actor'], row['request_id'])]),
                    'Original controller dispatch binding differs')
            require(encode(reply) == encode(replies[(row['actor'], row['request_id'])]),
                    'Original role reply differs from financial/provider terminal outcome')
            paths = view.journal.paths(binding.call_id)
            paths['reservation'] = view.journal.root / (hashlib.sha256(binding.reservation_id.encode()).hexdigest() + '.reservation.json')
            journal_paths.update(str(path) for path in paths.values())
            journal_locks.update(str(paths[kind].with_suffix('.lock')) for kind in ('request', 'reservation'))
            require(all(str(path) in files for path in paths.values()), 'Missing original request journal file')
            payloads.request_guard(binding.action)
            worker_fields = asdict(request)
            worker_fields['allowed_paths'] = list(request.allowed_paths)
            expected_request = canonical_payload({'worker_request': worker_fields,
                'view_manifest_sha256': binding.action.view_manifest_sha256})
            require(payloads._resolve(binding.action.worker_payload_ref) == expected_request,
                    'Original owned request does not match admitted worker bytes')
            proof = view._verify_terminal(row['actor'], row['request_id'])
            require(proof['binding'] == binding and type(reply.usage_units) is int
                    and 0 <= reply.usage_units <= binding.reserved_units, 'Unknown or invalid original usage')
            assert isinstance(reply.usage_units, int) and reply.result_payload_sha256 is not None
            spent += reply.usage_units
            failed += int(reply.state == 'failed')
            result_keys.add((row['actor'], reply.result_payload_sha256))
        require(set(files) == journal_paths, 'Journal manifest contains missing or extra call evidence')
        journal_root = Path(contract['journal_root'])
        require(journal_root.is_absolute() and journal_root.resolve() == journal_root, 'Indirect original journal root')
        with os.scandir(journal_root) as entries:
            actual_names: set[str] = set()
            for entry in entries:
                require(len(actual_names) < financial.MAX_CALLS * 6 and entry.is_file(follow_symlinks=False)
                        and stat.S_ISREG(entry.stat(follow_symlinks=False).st_mode), 'Unexpected or oversized journal inventory')
                actual_names.add(str(journal_root / entry.name))
        require(actual_names == journal_paths | journal_locks, 'Original journal contains unregistered files or missing call locks')
        require({(row['principal'], row['sha']) for row in index.execute('SELECT * FROM results')} == result_keys,
                'Original result publication membership differs')
        require(not ledger.execute('''SELECT 1 FROM reservations r JOIN financial_tasks_v2 t
            ON t.task_id=r.task_id WHERE t.cohort=? AND r.spent IS NULL LIMIT 1''', (originals.cohort,)).fetchone(),
            'Unsettled original reservation cannot qualify')
        require(ledger.execute("SELECT value FROM settings WHERE key='budget'").fetchone()[0] == config['expected_global_cap'],
                'Original cumulative cap changed')
        for item in files.values():
            item.read()
        return financial.FinancialAudit(originals.cohort, originals.config_sha256, terminal['seal_id'], len(rows), failed, spent,
                              cohort['opening'], config['expected_global_cap'],
                              tuple(identity(requested[key]) for key in sorted(requested)))


def implementation_sources() -> dict[str, str]:
    from . import cumulative_prerequisite_review_v1 as review
    names = set(REQUIRED_READER_SOURCES) | set(terminal.TERMINAL_READER_SOURCES) | {
        'gossip_harness/cumulative_source_promotion_v1.py',
        'gossip_harness/cumulative_role_originals_v1.py',
        'gossip_harness/cumulative_final_acceptance_v1.py',
        'gossip_harness/cumulative_final_acceptance_v2.py',
        'gossip_harness/cumulative_final_acceptance_v3.py',
        'gossip_harness/candidate_scope_consumer_v1.py',
        'gossip_harness/project_acceptance_registry_v1.py',
    }
    repository = Path(__file__).resolve().parents[1]
    sources = {name: sha((repository / name).read_bytes()) for name in sorted(names)}
    for name, pin in review.implementation_sources().items():
        consumer.require(name not in sources or sources[name] == pin, 'Conflicting promotion source aliases')
        sources[name] = pin
    return sources


def _joined_originals(owner: Any, subject: registry.Subject) -> dict[str, Any]:
    """Read every generation, admitted role action, provider original and Git effect."""
    plan = owner.plan
    consumer.require(type(plan) is StudyPlan, 'Exact V2 source plan required')
    matches = [(i, slot) for i, slot in enumerate(owner.originals.slots) if slot.trajectory == subject.trajectory_id]
    consumer.require(len(matches) == 1, 'Unknown original promotion trajectory')
    index, slot = matches[0]
    if slot.outcome != 'completed' or slot.evidence_errors:
        raise consumer.AuthorityUnavailable('Promotion requires all four completed original milestones')
    child = plan.roster.children[index]
    originals = _TrackedOriginals(owner.study_chain)
    require(digest(originals.get('contract')) == digest(plan.record())
            and originals.get('ledger') == owner.ledger_identity, 'Original study/wallet binding differs')
    rkey, ckey = 'runtime.' + child.trajectory, 'child.' + child.trajectory
    manifest = originals.get(rkey + '.originals')
    root = Path(manifest['original_paths']['runtime_root'])
    require(root.is_absolute() and root.resolve() == root, 'Canonical original runtime root required')
    expected_paths = {'runtime_root': str(root), 'financial_service': str(root / 'financial-service'),
        'provider_journals': str(root / 'provider-journals'), 'financial_payloads': str(root / 'financial-payloads'),
        'finance_mesh': str(root / 'finance-mesh'), 'seed_mesh': str(root / 'seed-mesh'),
        'protected_git': str(root / 'protected.git'), 'roles': {actor: str(root / 'roles' / actor) for actor in child.actors},
        'processes': str(root / 'processes')}
    require(manifest['original_paths'] == expected_paths and manifest['writer_lifetimes_closed'] is True
            and manifest['cohort'] == child.cohort and manifest['ledger_identity'] == owner.ledger_identity,
            'Original closed child inventory identity differs')
    audit_child_inventory(root, manifest['complete_child_files'])
    role_results, child_terminal = _histories_v2(originals, plan, index, root / 'protected.git')
    require(child_terminal['final_source']['source_sha256'] == subject.source_sha256
            and slot.final_source['source_sha256'] == subject.source_sha256,
            'Promotion target differs from original final source')
    file_paths = {item['path'] for item in manifest['complete_child_files']}
    require(all(path in file_paths and value['path'] == path for path, value in manifest['database_identities'].items())
            and manifest['payload_index_identity'] == manifest['database_identities'][str(root / 'financial-payloads' / 'payload-index.sqlite')]
            and manifest['mesh_database_identity'] == manifest['database_identities'][str(root / 'finance-mesh' / 'mesh.sqlite')],
            'Original database identity escaped the anchored inventory')
    config, permit, contract = (originals.get(rkey + suffix) for suffix in
                                ('.financial-config', '.permit', '.financial-contract'))
    require(manifest['config'] == config and manifest['config_sha256'] == digest(config)
            and config['operator_permit'] == permit and config['operator_permit_sha256'] == digest(permit)
            and config['contract'] == contract and permit['cohort_contract_sha256'] == digest(contract)
            and permit['sources'] == plan.source_pins and digest(permit['profiles']) == plan.cohort.model_profiles_sha256
            and permit['max_workers'] == plan.executor_slots and contract['journal_root'] == str(root / 'provider-journals')
            and config['mode'] == plan.runtime['final_acceptance_financial_mode'],
            'Original financial source/profile/config binding differs')
    from .cumulative_process_evidence_v1 import source_fingerprints, closure_name, confirmation_name
    process = audit_process_closure(owner.study_chain, owner.study_expected, roster=plan.roster,
        cohort=child.cohort, expected_sources={name: plan.source_pins[name] for name in source_fingerprints()},
        expected_source=child_terminal['final_source'], runtime_root=root,
        execution_repository=owner.repository, python_executable=sys.executable)
    transport = originals.get(rkey + '.transport')
    finance_port = originals.get(rkey + '.finance-port')['port']
    def mesh(actor: str, directory: Path) -> MeshConfig:
        return MeshConfig(directory, actor, child.cohort, plan.sha256, (*child.actors, 'seed', 'finance'),
            transport['transport_key'], observer_key=transport['observer_key'], interval=.05, fanout=2,
            limits=MeshLimits(max_payloads=TRANSPORT_CONTRACT['max_payloads'], max_events=TRANSPORT_CONTRACT['max_events'],
                max_reserved_bytes=TRANSPORT_CONTRACT['max_reserved_bytes']))
    require(manifest['mesh_identity'] == mesh('finance', root / 'finance-mesh').identity(),
            'Original financial mesh domain differs')
    dispatches: list[DispatchBinding] = []
    replies: list[DispatchReply] = []
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
                and role_config['crash_once'] is (plan.cohort.trajectories[index].block == 'compound_recovery' and actor.endswith('.B01')),
                'Actual role source/limits/placement differ')
        role = audit_role_originals(actor=actor, role_config=role_config, role_root=root / 'roles' / actor,
            database_identities=manifest['database_identities'], controller_results=role_results[actor],
            expected_final_action_ids=final_actions)
        require(set(role.observed_process_ids) <= set(dict(process.process_ids)[actor]), 'Foreign original role process')
        dispatches.extend(role.dispatches)
        replies.extend(role.terminal_replies)
    declared = tuple(from_dict(DispatchBinding, value) for value in manifest['dispatches'])
    require(sorted(map(identity, declared)) == sorted(map(identity, dispatches)),
            'Role/controller and financial manifest membership differ')
    facts = _audit_finance_v5(financial.FinancialOriginals(owner.ledger_identity, child.cohort, config, digest(config),
        manifest['payload_index_identity'], manifest['payload_config'], manifest['mesh_database_identity'],
        manifest['mesh_identity'], tuple(financial.OriginalFile(**item) for item in manifest['journal_files']),
        tuple(dispatches), tuple(replies)))
    store = GitStore(root / 'protected.git')
    commit = child_terminal['final_source']['commit_oid']
    tree, files = capture_git_source(store, commit)
    require(store.head() == commit and commit == slot.final_source['commit_oid']
            and tree == slot.final_source['tree_oid'] and admission.source_sha256(files) == subject.source_sha256,
            'Final current Git revision differs from the frozen source')
    audit_child_inventory(root, manifest['complete_child_files'])
    owner._current_originals()
    process_refs = [{'name': name, 'sha256': sha(owner.study_chain.read(name)),
                     'position': owner.study_chain.position(name)}
                    for name in (closure_name(child.cohort), confirmation_name(child.cohort))]
    return {'protocol': PROTOCOL, 'policy': POLICY, 'subject': asdict(subject),
        'revision': {'commit_oid': commit, 'tree_oid': tree, 'source_sha256': subject.source_sha256},
        'repository': str(store.path), 'source_files': {name: raw.decode() for name, raw in files.items()},
        'study_checkpoint': asdict(owner.study_expected), 'freeze': asdict(owner.freeze),
        'history': sorted(originals.references.values(), key=lambda row: row['position']),
        'process': process_refs, 'financial': asdict(facts),
        'role_actors': list(child.actors),
        'package_reviewers': [actor for actor in child.actors if actor.rsplit('.', 1)[-1] in REVIEWERS],
        'acceptance_authority': False, 'control_qualification': False}


def _normalized(operation: Any) -> Any:
    @wraps(operation)
    def call(*args: Any, **kwargs: Any) -> Any:
        from .cumulative_final_acceptance_v1 import normalize_authority
        return normalize_authority(operation)(*args, **kwargs)
    return call


class SourcePromotion:
    """Host capability; stage once, authenticate delivered review, grade readonly."""
    @staticmethod
    def _owner_protocol(owner: Any) -> str:
        from .cumulative_final_acceptance_v2 import FinalAcceptanceV2, PROTOCOL as V2
        from .cumulative_final_acceptance_v3 import FinalAcceptanceV3, PROTOCOL as V3
        consumer.require(type(owner) in (FinalAcceptanceV2, FinalAcceptanceV3),
                         'Exact final V2 or V3 owner required')
        protocol = V2 if type(owner) is FinalAcceptanceV2 else V3
        consumer.require(owner.protocol == protocol, 'Final promotion owner protocol changed')
        return protocol

    @_normalized
    def __init__(self, owner: Any, *, reviews: Any):
        from .cumulative_prerequisite_review_v1 import OriginalReviewAuthority
        self.owner_protocol = self._owner_protocol(owner)
        consumer.require(type(reviews) is OriginalReviewAuthority,
                         'Exact original independent review authority required')
        self.owner, self.reviews = owner, reviews
        self.sources = implementation_sources()
        own_chains = (owner.chain, owner.study_chain, owner.scope_owner.chain)
        review_roots = (reviews.chain.raw_root, reviews.chain.delta_root, reviews.chain.authority.root)
        for chain in own_chains:
            consumer.require(chain is not reviews.chain and all(
                not a.is_relative_to(b) and not b.is_relative_to(a)
                for a in review_roots for b in (chain.raw_root, chain.delta_root, chain.authority.root)),
                'Independent review chain overlaps another original authority')
        self._current()

    def _current(self) -> None:
        from .cumulative_prerequisite_review_v1 import OriginalReviewAuthority
        consumer.require(self._owner_protocol(self.owner) == self.owner_protocol
                         and type(self.reviews) is OriginalReviewAuthority
                         and type(self.owner.plan) is StudyPlan, 'Promotion capability changed')
        self.owner._current()
        self.reviews._current()
        consumer.require(self.owner.plan.runtime.get('final_promotion_protocol') == PROTOCOL,
                         'Source promotion was not prospectively declared')
        consumer.require(implementation_sources() == self.sources
            and all(self.owner.plan.source_pins.get(name) == pin for name, pin in self.sources.items()),
            'Complete promotion reader/review closure was not prospectively pinned')
        admission.verify_loaded_sources(self.sources)

    @staticmethod
    def _key(subject: registry.Subject) -> str:
        consumer.require(type(subject) is registry.Subject, 'Exact final promotion subject required')
        return 'final.source-promotion.' + digest(asdict(subject))

    def _context(self, subject: registry.Subject) -> dict[str, Any]:
        self._current()
        self.owner._current_originals()
        freeze = self.owner.freeze
        if freeze is None or not self.owner.originals.freeze_eligible:
            raise consumer.AuthorityUnavailable('Complete original six-child freeze required for promotion')
        consumer.require(type(subject) is registry.Subject and subject in self.owner.subjects
            and tuple(freeze.subjects) == self.owner.subjects and len(freeze.subjects) == 6
            and freeze.no_further_model_actions is True, 'Promotion is outside the original final cohort')
        submission = self.owner.submissions.get(subject.trajectory_id)
        consumer.require(submission is not None and submission.subject == subject,
                         'Original final source scope registration required')
        context = _joined_originals(self.owner, subject)
        original_root = Path(context['repository']).parent
        consumer.require(all(not path.is_relative_to(original_root) and not original_root.is_relative_to(path)
            for path in (self.reviews.chain.raw_root, self.reviews.chain.delta_root, self.reviews.chain.authority.root)),
            'Review publication roots overlap original candidate artifacts')
        context['sources'] = self.sources
        context['final_acceptance_protocol'] = self.owner_protocol
        self._current()
        return context

    @staticmethod
    def _request_link(request: Any, reviews: Any) -> dict[str, Any]:
        return {'name': request.name, 'sha256': request.sha256, 'position': request.position,
                'context_sha256': reviews.expected.context_sha256}

    @_normalized
    def stage(self, subject: registry.Subject) -> dict[str, Any]:
        """Retain the complete rederived review target before any review report."""
        context = self._context(subject)
        key = self._key(subject)
        self.owner._put(key + '.target', context)
        request = self.reviews.stage(purpose='candidate_source_promotion', role='integration',
                                    source=consumer.Revision(**context['revision']), context=context)
        result = self.owner._put(key + '.request', self._request_link(request, self.reviews))
        self._current()
        return result

    def _reviewed(self, subject: registry.Subject) -> tuple[dict[str, Any], Any]:
        context = self._context(subject)
        key = self._key(subject)
        retained = self.owner.records.read(key + '.target')
        if retained is None:
            raise consumer.AuthorityUnavailable('Original promotion review target was never staged')
        consumer.require(digest(retained) == digest(context), 'Staged final source/history changed')
        request = self.reviews.original(purpose='candidate_source_promotion', role='integration',
            source=consumer.Revision(**context['revision']), context=context)
        consumer.require(self.owner.records.read(key + '.request') == self._request_link(request, self.reviews),
                         'Original cross-journal review request changed')
        review = self.reviews.authenticate(request)
        consumer.require(review.reviewer_id not in context['role_actors']
                         and review.reviewer_id not in context['package_reviewers'],
                         'Final integration reviewer must be independent of original project roles')
        if review.decision == 'unresolved':
            raise consumer.AuthorityUnavailable('Original integration review remains unresolved')
        consumer.require(review.decision in ('accept', 'reject'), 'Unknown original integration review decision')
        return context, review

    @_normalized
    def verify_and_retain(self, subject: registry.Subject) -> registry.Promotion:
        """Write authority/verifier receipts only from authenticated original decisions."""
        context, review = self._reviewed(subject)
        key = self._key(subject)
        promotion = {'protocol': PROTOCOL, 'subject': asdict(subject), 'policy': POLICY,
            'status': 'promoted' if review.decision == 'accept' else 'rejected',
            'target_sha256': digest(context), 'review': plain(asdict(review)), 'sources': self.sources,
            'acceptance_authority': False, 'control_qualification': False}
        self.owner._put(key + '.original', promotion)
        name = Records.name(key + '.original')
        self.owner._put(key + '.verifier', {'protocol': PROTOCOL, 'purpose': 'independent-source-promotion-verifier',
            'subject': asdict(subject), 'original_name': name, 'original_sha256': sha(self.owner.chain.read(name)),
            'original_position': self.owner.chain.position(name), 'study_checkpoint': asdict(self.owner.study_expected),
            'review_checkpoint': asdict(self.reviews.expected), 'target_sha256': digest(context), 'sources': self.sources})
        result = self.promotion(subject)
        consumer.require(result is not None, 'Retained promotion disappeared')
        assert result is not None
        return result

    @_normalized
    def promotion(self, subject: registry.Subject) -> registry.Promotion | None:
        """Authenticate originals and retained verifier without writing or refreshing."""
        self._current()
        key = self._key(subject)
        original = self.owner.records.read(key + '.original')
        verifier = self.owner.records.read(key + '.verifier')
        if original is None and verifier is None:
            return None
        if original is None or verifier is None:
            raise consumer.AuthorityUnavailable('Source promotion publication remains incomplete')
        context, review = self._reviewed(subject)
        expected = {'protocol': PROTOCOL, 'subject': asdict(subject), 'policy': POLICY,
            'status': 'promoted' if review.decision == 'accept' else 'rejected',
            'target_sha256': digest(context), 'review': plain(asdict(review)), 'sources': self.sources,
            'acceptance_authority': False, 'control_qualification': False}
        original_name, verifier_name = Records.name(key + '.original'), Records.name(key + '.verifier')
        original_hash = sha(self.owner.chain.read(original_name))
        expected_verifier = {'protocol': PROTOCOL, 'purpose': 'independent-source-promotion-verifier',
            'subject': asdict(subject), 'original_name': original_name, 'original_sha256': original_hash,
            'original_position': self.owner.chain.position(original_name),
            'study_checkpoint': asdict(self.owner.study_expected), 'review_checkpoint': asdict(self.reviews.expected),
            'target_sha256': digest(context), 'sources': self.sources}
        consumer.require(digest(original) == digest(expected) and digest(verifier) == digest(expected_verifier)
            and self.owner.chain.position(Records.name(key + '.target'))
            < self.owner.chain.position(Records.name(key + '.request'))
            < self.owner.chain.position(original_name) < self.owner.chain.position(verifier_name),
            'Retained original promotion/verifier source, review or chronology differs')
        result = registry.Promotion(subject, original['status'], original_hash,
                                    sha(self.owner.chain.read(verifier_name)))
        self.owner._current_originals()
        self._current()
        return result
