"""Read actual V2/V5 cumulative fixture mechanics; never grant final acceptance.

Every history, role, original provider journal, Git CAS, prescribed fault, process
exit and six-child financial barrier is authenticated. Fixture usage remains
simulated accounting, including known failed calls. Live qualification additionally
requires the separately versioned FinalV3 original reader and approved live mapping.
No wallet, runtime, role, provider or Git writer is created by this reader.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
import sys
from typing import Any, cast

from . import cumulative_terminal_originals_v2 as terminal_reader
from .candidate_checkpoint_chain_v1 import CheckpointChain, PrefixCommitment
from .candidate_http_journal_v3 import decode
from .cumulative_process_evidence_v1 import closure_name, source_fingerprints
from .cumulative_rehearsal_inventory_v1 import audit_child_inventory
from .cumulative_rehearsal_process_v1 import audit_process_closure, ProcessAudit
from .cumulative_rehearsal_validator_v1 import (
    _Originals, _checked_profiles, _faults,
    REQUIRED_READER_SOURCES as V1_READER_SOURCES,
    REQUIRED_REHEARSAL_CLASSES as V1_REHEARSAL_CLASSES,
)
from .cumulative_role_originals_v1 import audit_role_originals
from .cumulative_source_promotion_v1 import _histories_v2, _audit_finance_v5
from .cumulative_study_controller_v2 import StudyPlan, REVIEWERS, SOURCE_CLOSURE, TRANSPORT_CONTRACT, package_for, plain
from .financial_rehearsal_originals_v1 import FinancialOriginals, FinancialAudit, OriginalFile
from .peer_financial_authority_v3 import _gate, _bound_file, _action_limits, _PERMIT_FIELDS, FIXTURE_TRANSPORT
from .peer_financial_authority_v5 import REQUIRED_SOURCES as FINANCIAL_SOURCES, REQUIRED_TEST_CLASSES, PERMIT_PROTOCOL
from .peer_financial_terminal_v1 import ChildTerminalSeal, require, digest, sha
from .peer_financial_terminal_v2 import checked_financial_config, checked_policy, verified_study_barrier
from .peer_mesh_v2 import MeshConfig
from .peer_mesh_store_v2 import MeshLimits
from .peer_project_contract_v2 import DispatchBinding, DispatchReply, Context, WorkKey, to_dict, from_dict, identity

PROTOCOL = 'cumulative-rehearsal-validator-v2'
DESIGN_PROTOCOL = 'cumulative-execution-design-envelope-v2'
REQUIRED_REHEARSAL_CLASSES = tuple(dict.fromkeys((*V1_REHEARSAL_CLASSES, *REQUIRED_TEST_CLASSES,
    'tests/test_cumulative_terminal_originals_v2.py::CumulativeTerminalRepairedV2Tests',
    'tests/test_cumulative_study_repaired_runtime_v2.py::CumulativeStudyRepairedRuntimeV2Tests',
    'tests/test_cumulative_source_promotion_v1.py::CumulativeSourcePromotionFinanceV1Tests',
    'tests/test_cumulative_source_promotion_v1.py::CumulativeSourcePromotionGitV1Tests',
    'tests/test_cumulative_source_promotion_v1.py::CumulativeSourcePromotionHistoryV1Tests',
    'tests/test_cumulative_rehearsal_validator_v2.py::CumulativeRehearsalValidatorV2Tests')))
REQUIRED_READER_SOURCES = tuple(sorted(set(V1_READER_SOURCES) | set(SOURCE_CLOSURE) | set(FINANCIAL_SOURCES) | {
    'gossip_harness/cumulative_rehearsal_validator_v2.py',
    'gossip_harness/cumulative_source_promotion_v1.py',
    'gossip_harness/cumulative_terminal_originals_v2.py',
    *(name.split('::')[0] for name in REQUIRED_REHEARSAL_CLASSES)}))


def implementation_sources() -> dict[str, str]:
    from . import cumulative_source_promotion_v1 as promotion
    sources = promotion.implementation_sources()
    repository = Path(__file__).resolve().parents[1]
    for name in REQUIRED_READER_SOURCES:
        pin = sha((repository / name).read_bytes())
        require(name not in sources or sources[name] == pin, 'Conflicting V2 original-reader source pin')
        sources[name] = pin
    return sources


@dataclass(frozen=True)
class CohortOriginals:
    execution_contract_sha256: str
    child_terminals: tuple[dict, ...]
    process_audits: tuple[ProcessAudit, ...]
    financial_audits: tuple[FinancialAudit, ...]
    barrier: dict
    barrier_position: int
    checkpoint: PrefixCommitment
    financial_configs: tuple[dict, ...]
    operator_permits: tuple[dict, ...]
    execution_designs: tuple[dict, ...]
    protocol: str = field(default=PROTOCOL, init=False)
    accepted: bool = field(default=False, init=False)
    live_qualification: bool = field(default=False, init=False)


def checked_design_envelope(envelope: dict, plan: StudyPlan) -> tuple[dict, ...]:
    """Exact fixture design only; no fixture/live equivalence is inferred here."""
    require(type(plan) is StudyPlan, 'Exact V2 prospective plan required')
    plan.__post_init__()
    require(type(envelope) is dict and set(envelope) == {'protocol', 'execution_contract_sha256',
        'terminal_roster', 'sources', 'runtime', 'profiles', 'financial_closure_policy', 'children'}
        and envelope['protocol'] == DESIGN_PROTOCOL
        and envelope['execution_contract_sha256'] == plan.sha256
        and digest(envelope['terminal_roster']) == digest(plan.roster.record())
        and digest(envelope['sources']) == digest(plan.source_pins)
        and digest(envelope['runtime']) == digest(plan.runtime)
        and digest(envelope['profiles']) == plan.cohort.model_profiles_sha256
        and type(envelope['children']) is list and len(envelope['children']) == 6,
        'Independent complete V2 execution-design envelope differs')
    require(checked_policy(envelope['financial_closure_policy']) == plan.financial_closure_policy,
            'Independent execution-design closure policy differs')
    designs = []
    for entry, child in zip(envelope['children'], plan.roster.children, strict=True):
        require(type(entry) is dict and set(entry) == {'cohort', 'trajectory', 'execution_design'}
                and entry['cohort'] == child.cohort and entry['trajectory'] == child.trajectory,
                'Ordered independently approved child design differs')
        design = entry['execution_design']
        require(type(design) is dict and digest(design.get('terminal_roster')) == digest(plan.roster.record())
                and digest(design.get('profiles')) == digest(envelope['profiles'])
                and digest(design.get('runtime')) == digest(plan.runtime)
                and type(design.get('max_workers')) is int and design['max_workers'] == plan.executor_slots,
                'Child design shared policy differs')
        require(checked_policy(design.get('financial_closure_policy')) == plan.financial_closure_policy,
                'Child design V5 closure policy differs')
        limits = _action_limits(design.get('action_limits'))
        require(set(limits['by_actor']) == set(child.actors), 'Child design role-limit membership differs')
        designs.append(design)
    return tuple(designs)


def _checked_financial_origin(*, plan: StudyPlan, index: int, root: Path, ledger_identity: dict,
                              manifest: dict, config: dict, permit: dict, contract: dict,
                              expected_design: dict) -> None:
    child = plan.roster.children[index]
    require(type(permit) is dict and set(permit) == _PERMIT_FIELDS
            and type(permit['approval_ref']) is str and 1 <= len(permit['approval_ref']) <= 1024,
            'Exact originally approved closed V5 permit required')
    require(type(contract) is dict and set(contract) == {'cohort_id', 'execution_contract_sha256',
            'ledger_identity', 'journal_root', 'transport_identity', 'task_specs'},
            'Closed actual V2 financial contract required')
    checked_financial_config(config)
    _checked_profiles(config, permit)
    require(manifest['config'] == config and manifest['config_sha256'] == digest(config)
            and config['operator_permit'] == permit and config['operator_permit_sha256'] == digest(permit)
            and config['contract'] == contract and permit['cohort_contract_sha256'] == digest(contract)
            and config['mode'] == 'fixture' and config['observation_kind'] == 'simulated'
            and permit['protocol'] == PERMIT_PROTOCOL
            and permit['mode'] == 'fixture' and permit['qualification'] is None
            and config['sources'] == permit['sources']
            and config['execution_design_sha256'] == permit['execution_design_sha256']
            and config['expected_opening_usage'] == permit['expected_opening_usage']
            and config['expected_global_cap'] == permit['expected_global_cap']
            and config['incremental_cap_micro_usd'] == permit['incremental_cap_micro_usd']
            and permit['sources'] == plan.source_pins
            and digest(permit['execution_design']) == digest(expected_design)
            and permit['execution_design_sha256'] == digest(permit['execution_design'])
            and permit['execution_design']['terminal_roster'] == plan.roster.record()
            and digest(permit['profiles']) == plan.cohort.model_profiles_sha256
            and type(config['max_workers']) is int and config['max_workers'] == permit['max_workers']
            and type(permit['max_workers']) is int and permit['max_workers'] == plan.executor_slots
            and contract['journal_root'] == str(root / 'provider-journals')
            and contract['cohort_id'] == child.cohort
            and contract['ledger_identity'] == ledger_identity
            and contract['transport_identity'] == FIXTURE_TRANSPORT,
            'Original permit/config/source/profile/contract binding differs')
    require(all(type(permit[name]) is int for name in
        ('expected_opening_usage', 'expected_global_cap', 'incremental_cap_micro_usd'))
        and 0 <= permit['expected_opening_usage'] <= permit['expected_global_cap']
        and 0 < permit['incremental_cap_micro_usd'] <= permit['expected_global_cap'] - permit['expected_opening_usage'],
        'Original exact financial limits differ')
    limits = _action_limits(permit['execution_design']['action_limits'])
    require(set(limits['by_actor']) == set(child.actors), 'Original action-limit actor membership differs')
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
    require(all(kind in limits['by_kind']
            and str(spec['work']['generation']) in limits['by_kind_generation'].get(kind, {})
            for spec in task_specs for kind in spec['kinds']),
            'Original task universe exceeds approved kind/generation limits')


def audit_cohort_originals(chain: CheckpointChain, expected: PrefixCommitment, *, plan: StudyPlan,
                             ledger_identity: dict, repository: Path, gate: dict,
                             ordered_test_classes: tuple[str, ...], expected_design_envelope: dict) -> CohortOriginals:
    """Audit six actual fixture children; final acceptance is a separate authority.

    Caller owns the exact chain and independent expected head on this thread.
    Historical proof open/close must be serialized when invoked by finance
    workers. This method never opens a wallet, creates a role, or retries work.
    """
    require(type(chain) is CheckpointChain and type(plan) is StudyPlan, 'Exact owned proof and prospective plan required')
    plan.__post_init__()
    required_sources = implementation_sources()
    require(all(plan.source_pins.get(name) == pin for name, pin in required_sources.items()),
            'Full exact V2 reader/controller/V5 source closure missing')
    require(plan.runtime.get('final_acceptance_protocol') == 'cumulative-final-acceptance-v3'
            and plan.runtime.get('study_successor_protocol') == 'cumulative-study-successor-v3'
            and plan.runtime.get('final_acceptance_financial_mode') == 'fixture',
            'Exact prospective FinalV3 fixture successor required')
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
    terminal_facts = terminal_reader.audit_terminal_cohort(chain, expected, plan=plan,
        ledger_identity=ledger_identity, repository=repository)
    require(terminal_facts.freeze_eligible and len(terminal_facts.slots) == 6
            and all(slot.outcome == 'completed' for slot in terminal_facts.slots),
            'Full successful V5 cohort and current original wallet required')
    receipts = []
    terminals, process_audits, financial_audits = [], [], []
    configs, permits = [], []
    child_roots: set[Path] = set()
    prior_terminal: str | None = None
    next_opening: int | None = None
    global_cap: int | None = None
    for index, child in enumerate(plan.roster.children):
        ckey, rkey = 'child.' + child.trajectory, 'runtime.' + child.trajectory
        originals.before('contract', ckey + '.begin')
        if prior_terminal is not None:
            originals.before(prior_terminal, ckey + '.begin', rkey + '.financial-config')
        manifest = originals.get(rkey + '.originals')
        require(manifest['protocol'] == 'cumulative-study-runtime-v2-originals'
                and manifest['writer_lifetimes_closed'] is True and manifest['ledger_identity'] == ledger_identity
                and manifest['cohort'] == child.cohort, 'Original child lifetime/ledger identity differs')
        root = Path(manifest['original_paths']['runtime_root'])
        require(root.is_absolute() and root.resolve() == root and not root.is_symlink()
                and not any(root == other or root in other.parents or other in root.parents for other in child_roots),
                'Child roots must be canonical, distinct and disjoint')
        child_roots.add(root)
        expected_paths = {'runtime_root': str(root), 'financial_service': str(root / 'financial-service'),
            'provider_journals': str(root / 'provider-journals'), 'financial_payloads': str(root / 'financial-payloads'),
            'finance_mesh': str(root / 'finance-mesh'), 'seed_mesh': str(root / 'seed-mesh'),
            'protected_git': str(root / 'protected.git'), 'roles': {actor: str(root / 'roles' / actor) for actor in child.actors},
            'processes': str(root / 'processes')}
        require(manifest['original_paths'] == expected_paths, 'Original child paths escaped the authenticated inventory')
        audit_child_inventory(root, manifest['complete_child_files'])
        role_results, terminal = _histories_v2(originals, plan, index, Path(manifest['original_paths']['protected_git']))
        file_paths = {item['path'] for item in manifest['complete_child_files']}
        require(all(path in file_paths and value['path'] == path for path, value in manifest['database_identities'].items())
                and manifest['payload_index_identity'] == manifest['database_identities'][str(root / 'financial-payloads' / 'payload-index.sqlite')]
                and manifest['mesh_database_identity'] == manifest['database_identities'][str(root / 'finance-mesh' / 'mesh.sqlite')],
                'Original database identity is outside complete child inventory')
        config, permit, contract = (originals.get(rkey + suffix) for suffix in
                                    ('.financial-config', '.permit', '.financial-contract'))
        _checked_financial_origin(plan=plan, index=index, root=root, ledger_identity=ledger_identity,
            manifest=manifest, config=config, permit=permit, contract=contract, expected_design=expected_designs[index])
        process = audit_process_closure(chain, expected, roster=plan.roster, cohort=child.cohort,
            expected_sources={key: plan.source_pins[key] for key in source_fingerprints()},
            expected_source=terminal['final_source'], runtime_root=Path(manifest['original_paths']['runtime_root']),
            execution_repository=repository, python_executable=sys.executable)
        closure = decode(chain.read(closure_name(child.cohort)))
        require(originals.position(rkey + '.source-initialization.result')
                < chain.position(closure['registration']['name']) < originals.position(rkey + '.financial-config'),
                'Initial source did not precede process registration and financial enrollment')
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
        # The pinned V1 helper consumes unchanged original fault/process schemas.
        # It does not construct or accept a V1 plan or financial authority.
        cast(Any, _faults)(originals, plan, index, process, role_results, root)
        declared = tuple(from_dict(DispatchBinding, value) for value in manifest['dispatches'])
        require(sorted(map(identity, declared)) == sorted(map(identity, dispatches)),
                'Original role admission and financial manifest membership differ')
        facts = _audit_finance_v5(FinancialOriginals(ledger_identity, child.cohort, config, digest(config),
            manifest['payload_index_identity'], manifest['payload_config'], manifest['mesh_database_identity'],
            manifest['mesh_identity'], tuple(OriginalFile(**item) for item in manifest['journal_files']), tuple(dispatches), tuple(role_replies)))
        require((next_opening is None or facts.opening_micro_usd == next_opening)
                and facts.opening_micro_usd == permit['expected_opening_usage']
                and facts.global_cap_micro_usd == permit['expected_global_cap']
                and (global_cap is None or facts.global_cap_micro_usd == global_cap),
                'Original cumulative wallet opening/cap was reset or changed')
        next_opening = facts.opening_micro_usd + facts.spent_micro_usd
        global_cap = facts.global_cap_micro_usd
        configs.append(config)
        permits.append(permit)
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
    require(terminal_facts.barrier == retained, 'Terminal and strict-history barrier originals differ')
    require({key:value for key,value in barrier.items() if key != 'checkpoint'} ==
            {key:value for key,value in retained.items() if key != 'checkpoint'}, 'Original complete-cohort barrier differs')
    assert prior_terminal is not None
    originals.before(prior_terminal, 'complete-cohort-barrier')
    require(len(terminals) == 6 and sum(len(item.completed_action_ids) for item in process_audits) == 96,
            'Complete six-child, 96-role original census required')
    chain.validate_boundary(expected=expected)
    plan.verify_sources(repository)
    return CohortOriginals(plan.sha256, tuple(terminals), tuple(process_audits), tuple(financial_audits),
                           retained, originals.position('complete-cohort-barrier'), expected,
                           tuple(configs), tuple(permits), expected_designs)
