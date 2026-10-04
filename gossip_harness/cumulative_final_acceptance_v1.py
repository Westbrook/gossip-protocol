"""Post-barrier original-evidence composition, never a receipt-dictionary grader.

Dispatch is explicit and fresh. The scope owner authenticates complete semantic
registration; the terminal reader authenticates all planned outcomes. Missing
prerequisite and promotion producers remain missing in CandidateScopeConsumer.
This module does not upgrade the public v1 controller's unavailable result.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, is_dataclass
import base64
import importlib
from functools import wraps
import subprocess
import sqlite3
import hashlib
from pathlib import Path
from typing import Any, Callable, ParamSpec, TypeVar
from types import ModuleType

from . import candidate_checkpoint_chain_v1 as checkpoint
from .candidate_checkpoint_head_v1 import ExternalHead
from .candidate_source_capture_policy_v1 import SourceCaptureUnavailable
from . import candidate_client_execution_v5 as cli
from . import candidate_client_observation_source_v1 as cli_source
from . import candidate_http_execution_v4 as http
from . import candidate_http_observation_source_v1 as http_source
from . import candidate_product_process_execution_v1 as product
from . import candidate_product_process_observation_v1 as product_source
from . import candidate_storage_product_execution_v1 as storage
from . import candidate_storage_product_observation_v1 as storage_source
from . import cumulative_observation_recipe_factory_v1 as recipe_factory
from . import candidate_observation_admission_v1 as admission
from . import candidate_scope_consumer_v1 as consumer
from . import cumulative_scope_authority_v1 as scope_authority
from . import project_acceptance_compiler_v1 as compiler
from . import project_acceptance_registry_v1 as registry
from .cumulative_study_controller_v1 import StudyPlan, Records, plain, digest
from .gitstore import GitStore, GitError

PROTOCOL = 'cumulative-final-acceptance-v1'
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
require = consumer.require


def _canonical_sources(groups: tuple[dict[str, str], ...]) -> dict[str, str]:
    root = Path(__file__).resolve().parent.parent
    result: dict[str, str] = {}
    for group in groups:
        for name, pin in group.items():
            while name.startswith('finite/'):
                name = name[len('finite/'):]
            candidate = name if (root / name).is_file() else 'gossip_harness/' + Path(name).name
            path = root / candidate
            require(path.is_file() and path.resolve().is_relative_to(root)
                    and hashlib.sha256(path.read_bytes()).hexdigest() == pin, 'Unresolved original source: ' + name)
            require(candidate not in result or result[candidate] == pin, 'Conflicting original source inventory')
            result[candidate] = pin
    return dict(sorted(result.items()))


def implementation_sources() -> dict[str, str]:
    """Canonical complete evaluator, normative definition and scope closure."""
    root = Path(__file__).resolve().parent.parent
    original_catalog = scope_authority.source.load_catalog(root)
    return _canonical_sources((
        {'gossip_harness/cumulative_final_acceptance_v1.py': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
         'gossip_harness/cumulative_observation_recipe_factory_v1.py': recipe_factory.LOADED_SOURCE_SHA256,
         'gossip_harness/cumulative_study_controller_v1.py':
             hashlib.sha256((root / 'gossip_harness/cumulative_study_controller_v1.py').read_bytes()).hexdigest()},
        cli.evaluator_sources(), http.evaluator_sources(), product.evaluator_sources(),
        storage.evaluator_sources(), storage.profile.definition_sources(),
        cli.cases.definition_sources(), cli.cumulative.definition_sources(), product.core.definition_sources(),
        scope_authority.implementation_sources(), dict(original_catalog.source_pins)))


def terminal_implementation_sources() -> dict[str, str]:
    """Missing concrete reader rejects public launch, before any spend."""
    return _terminal_sources('cumulative_terminal_originals_v1')


def _terminal_sources(module_name: str) -> dict[str, str]:
    require(module_name in ('cumulative_terminal_originals_v1', 'cumulative_terminal_originals_v2'),
            'Unknown terminal-originals contract')
    try:
        terminal = importlib.import_module('.' + module_name, __package__)
    except ImportError as error:
        raise consumer.AuthorityUnavailable('Concrete terminal-originals reader unavailable') from error
    sources = _canonical_sources((terminal.terminal_reader_sources(),))
    admission.verify_loaded_sources(sources)
    return sources


_P = ParamSpec('_P')
_R = TypeVar('_R')


def normalize_authority(operation: Callable[_P, _R]) -> Callable[_P, _R]:
    """Keep operational uncertainty distinct from invalid provenance at APIs."""
    @wraps(operation)
    def guarded(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        try:
            return operation(*args, **kwargs)
        except (consumer.AuthorityError, consumer.AuthorityUnavailable):
            raise
        except (checkpoint.ChainUnknown, admission.AdmissionUnavailable, cli.ExecutionUnknown,
                http.ExecutionUnknown, product.ExecutionUnknown, storage.ExecutionUnknown, OSError, GitError,
                subprocess.SubprocessError, sqlite3.DatabaseError, SourceCaptureUnavailable) as error:
            raise consumer.AuthorityUnavailable(str(error)) from error
        except (ValueError, KeyError, TypeError) as error:
            raise consumer.AuthorityError(str(error)) from error
    return guarded


def _material(value: Any) -> Any:
    """Canonical proof material preserves original seal bytes without decoding."""
    if is_dataclass(value) and not isinstance(value, type):
        return _material(asdict(value))
    if isinstance(value, bytes):
        return {'encoding': 'base64', 'bytes': len(value), 'base64': base64.b64encode(value).decode('ascii')}
    if isinstance(value, (tuple, list)):
        return [_material(item) for item in value]
    if type(value) is dict:
        return {key: _material(item) for key, item in value.items()}
    return value


@dataclass(frozen=True)
class ObservationSpec:
    """Typed construction inputs; no execution factory or supplied observation."""
    kind: str
    root: Path
    delta_root: Path
    cleanup_root: Path
    checkpoint_authority: ExternalHead
    store: GitStore
    registration: Any
    policy: Any
    recipe: Any = None
    profile: Any = None
    cumulative_profile: Any = None
    endpoint: Any = None
    layout_plan: Any = None
    layout_authority: Any = None

    def observation_registration(self) -> admission.ObservationRegistration:
        if self.kind == 'm2':
            from . import candidate_m2_product_execution_v1 as m2
            require(type(self.registration) is m2.M2Registration and type(self.policy) is m2.M2Policy
                and type(self.profile) is m2.profile.M2Profile and type(self.layout_plan) is m2.review.LayoutPlan
                and type(self.layout_authority) is m2.review.M2ReviewAuthority
                and self.recipe is None and self.cumulative_profile is None, 'Exact M2 inputs required')
            return m2.observation_registration(self.registration)
        if self.kind == 'storage':
            require(type(self.registration) is storage.StorageRegistration and type(self.policy) is storage.StoragePolicy
                and type(self.profile) is storage.profile.StorageProductProfile
                and type(self.layout_plan) is storage.review.LayoutPlan
                and type(self.layout_authority) is storage.review.StorageReviewAuthority
                and self.recipe is None and self.cumulative_profile is None, 'Exact storage inputs required')
            return storage.observation_registration(self.registration)
        require(self.layout_plan is None and self.layout_authority is None,
                'Layout authority applies only to the explicit storage family')
        if self.kind == 'cli':
            require(type(self.registration) is cli.ClientRegistration and type(self.policy) is cli.ClientPolicy
                    and self.recipe is None and self.profile is None, 'Exact CLI inputs required')
            return cli.observation_registration(self.registration)
        module = http if self.kind == 'http' else product if self.kind == 'product' else None
        require(module is not None, 'Unknown physical observation owner')
        assert module is not None
        require(type(self.registration) is module.HttpRegistration and type(self.policy) is module.HttpPolicy
                and type(self.recipe) is module.HttpRecipe and type(self.profile) is module.HttpProductProfile
                and self.cumulative_profile is None, 'Exact HTTP/product inputs required')
        require(self.registration.binding.profile_sha256 == self.profile.sha256,
                'Original observer profile differs from registered binding')
        if self.profile.mapping_profile is not None:
            require(self.recipe == module.recipe_from_case(self.profile.case), 'MAP-A original whole recipe differs')
            original = self.registration.observation
            actual = module.observation_registration_for(self.registration.binding, self.profile, self.policy,
                subject=original.gate.binding.subject, gate_id=original.gate.gate_id,
                commit_oid=self.registration.commit_oid, tree_oid=self.registration.tree_oid,
                repetition_id=self.registration.repetition_id, cohort_trajectory_ids=original.cohort_trajectory_ids)
            require(actual == original, 'MAP-A profile/registration differs before final admission')
        return self.registration.observation


@dataclass
class _Enrollment:
    registration: admission.ObservationRegistration
    scope: scope_authority.RegistrationProvenance
    admission_record: dict[str, Any]
    phase: str = 'constructing'
    admission_owner: admission.ObservationAdmission | None = None
    owner: Any = None
    source: Any = None
    post_checkpoint: checkpoint.PrefixCommitment | None = None
    observation: registry.Observation | None = None
    history: dict[str, Any] | None = None


@dataclass(frozen=True)
class _KnownContract:
    protocol: str
    plan_type: type
    scope: ModuleType
    sources: Callable[[], dict[str, str]]
    terminal_module: str
    terminal_sources: Callable[[], dict[str, str]]
    families: tuple[str, ...]


def _known_contract(owner: FinalAcceptance) -> _KnownContract:
    """Closed version selection; never accept an injected evaluator/backend."""
    if type(owner) is FinalAcceptance:
        return _KnownContract(PROTOCOL, StudyPlan, scope_authority, implementation_sources,
            'cumulative_terminal_originals_v1', terminal_implementation_sources, ('cli', 'http', 'product'))
    from . import cumulative_final_acceptance_v2 as v2
    if type(owner) is v2.FinalAcceptanceV2:
        return _KnownContract(v2.PROTOCOL, v2.StudyPlan, v2.scope_authority, v2.implementation_sources,
            'cumulative_terminal_originals_v2', v2.terminal_implementation_sources,
            ('cli', 'http', 'product', 'storage'))
    from . import cumulative_final_acceptance_v3 as v3
    if type(owner) is v3.FinalAcceptanceV3:
        return _KnownContract(v3.PROTOCOL, v3.StudyPlan, v3.scope_authority, v3.implementation_sources,
            'cumulative_terminal_originals_v2', v3.terminal_implementation_sources,
            ('cli', 'http', 'product', 'storage', 'm2'))
    raise consumer.AuthorityError('Unknown final acceptance owner version')


class FinalAcceptance(consumer.EvidenceAuthority):
    """Own the new acceptance journal, borrow immutable study/scope authorities.

    The caller supplies independently current external heads. No observer or
    verdict can be enrolled after execution. Candidate roots and all controller
    proof roots must be disjoint. Owner construction may inspect Docker runtime;
    dispatch() performs real candidate work only when every admission holds.
    """
    def __init__(self, *, plan: Any, repository: Path, ledger_identity: dict[str, Any],
                 study_chain: checkpoint.CheckpointChain, study_expected: checkpoint.PrefixCommitment,
                 journal: checkpoint.CheckpointChain, expected: checkpoint.PrefixCommitment,
                 scope: Any, submissions: tuple[Any, ...]):
        contract = _known_contract(self)
        require(type(plan) is contract.plan_type and type(scope) is contract.scope.ScopeRegistrationController,
                'Actual prospective plan and semantic scope controller required')
        plan.__post_init__()
        require(all(type(chain) is checkpoint.CheckpointChain and type(chain.authority) is ExternalHead
                    for chain in (study_chain, journal)), 'Durable independently owned proof chains required')
        require(study_chain is not journal and scope.chain is not journal,
                'Acceptance append journal must be separate from frozen authorities')
        require(type(submissions) is tuple and all(type(row) is contract.scope.ScopeSubmission for row in submissions),
                'Exact typed scope submissions required')
        require(len({row.subject.trajectory_id for row in submissions}) == len(submissions), 'Duplicate subject submission')
        require(plan.runtime.get('final_acceptance_financial_mode') in ('live', 'fixture'),
                'Original contract must declare the financial origin for final interpretation')
        require(plan.runtime.get('final_acceptance_protocol') == contract.protocol,
                'Old public-only execution contract cannot adopt final acceptance')
        assert isinstance(journal.authority, ExternalHead)
        require(expected.sequence == expected.raw_file_count == 0,
                'Fresh acceptance journal required; original attempts cannot be reopened')
        require(plan.runtime.get('final_acceptance_roots') == {
            'raw': str(journal.raw_root), 'delta': str(journal.delta_root), 'head': str(journal.authority.root)},
            'Acceptance session roots differ from original prospective study contract')
        self.plan, self.repository, self.ledger_identity = plan, Path(repository), dict(ledger_identity)
        self.study_chain, self.study_expected = study_chain, study_expected
        self.chain, self.expected, self.records = journal, expected, Records(journal)
        snapshot = scope.open_snapshot()
        require(type(snapshot) is contract.scope.ScopeSnapshot, 'Exact original scope snapshot required')
        self.scope_owner, self.scope_snapshot = scope, snapshot
        self.submissions = {row.subject.trajectory_id: row for row in submissions}
        self.sources = contract.sources()
        require(all(plan.source_pins.get(name) == pin for name, pin in self.sources.items()),
                'Adapter and source authorities were not prospectively pinned')
        self.originals: Any = None
        self.freeze: registry.CohortFreeze | None = None
        self.subjects: tuple[registry.Subject, ...] = ()
        self.enrollments: dict[str, _Enrollment] = {}
        self.closed = False
        self.dispatch_halt: dict[str, Any] | None = None
        self._current()
        self._put('final.contract', {'protocol': self.protocol, 'study_sha256': plan.sha256,
            'study_checkpoint': asdict(study_expected), 'scope_checkpoint': self.scope_snapshot.checkpoint,
            'ledger_identity': self.ledger_identity, 'sources': self.sources,
            'scope_subjects': [asdict(row.subject) for row in submissions]})

    @property
    def protocol(self) -> str:
        return _known_contract(self).protocol

    def _current(self) -> None:
        contract = _known_contract(self)
        require(type(self.plan) is contract.plan_type and type(self.scope_owner) is contract.scope.ScopeRegistrationController,
                'Prospective contract version changed')
        self.plan.__post_init__()
        require(self.plan.runtime.get('final_acceptance_protocol') == contract.protocol, 'Final protocol changed')
        require(not self.closed, 'Closed acceptance owner')
        self.chain.validate_boundary(expected=self.expected)
        self.study_chain.validate_boundary(expected=self.study_expected)
        self.scope_snapshot.check_current(self.scope_snapshot.checkpoint)
        self.plan.verify_sources(self.repository)
        require(contract.sources() == self.sources, 'Acceptance implementation changed')
        admission.verify_loaded_sources(self.sources)

    def _put(self, key: str, value: dict[str, Any]) -> dict[str, Any]:
        self._current()
        out = self.records.put(key, value)
        self.expected = self.chain.commitment
        return out

    def _read_terminal(self) -> Any:
        contract = _known_contract(self)
        try:
            terminal = importlib.import_module('.' + contract.terminal_module, __package__)
        except ImportError as error:
            raise consumer.AuthorityUnavailable('Concrete terminal-originals reader unavailable') from error
        name = 'gossip_harness/' + contract.terminal_module + '.py'
        path = self.repository / name
        require(path.is_file() and self.plan.source_pins.get(name) == hashlib.sha256(path.read_bytes()).hexdigest(),
                'Original terminal reader was not prospectively pinned')
        terminal_sources = contract.terminal_sources()
        require(type(terminal_sources) is dict and all(self.plan.source_pins.get(name) == pin
                    for name, pin in terminal_sources.items()), 'Full terminal reader closure was not pinned')
        admission.verify_loaded_sources(terminal_sources)
        result = terminal.audit_terminal_cohort(self.study_chain, self.study_expected, plan=self.plan,
            ledger_identity=self.ledger_identity, repository=self.repository)
        require(type(result) is terminal.TerminalCohortOriginals and result.execution_contract_sha256 == self.plan.sha256
                and result.roster_sha256 == self.plan.roster.sha256 and result.checkpoint == self.study_expected,
                'Terminal audit differs from original study contract or external checkpoint')
        admission.verify_loaded_sources(terminal_sources)
        require(tuple((slot.cohort, slot.trajectory, slot.planned_actors) for slot in result.slots) ==
                tuple((child.cohort, child.trajectory, child.actors) for child in self.plan.roster.children),
                'Terminal audit omitted or changed a planned trajectory')
        return result

    @normalize_authority
    def prepare(self) -> registry.CohortFreeze | None:
        """Retain a fresh independent originals verifier before any observation."""
        self._current()
        originals = self._read_terminal()
        for slot in originals.slots:
            if slot.final_source is not None:
                origin = slot.financial_origin
                mode = self.plan.runtime['final_acceptance_financial_mode']
                require(type(origin) is dict and origin.get('mode') == mode
                        and origin.get('observation_kind') == ('provider' if mode == 'live' else 'simulated'),
                        'Final source financial origin differs from prospective interpretation')
        subjects = tuple(registry.Subject(self.plan.cohort.cohort_id, slot.trajectory, 'M4',
            self.plan.sha256, compiler.PRODUCT_V2_SHA256, slot.final_source['source_sha256'])
            for slot in originals.slots if slot.final_source is not None)
        for subject in subjects:
            submission = self.submissions.get(subject.trajectory_id)
            require(submission is None or submission.subject == subject,
                    'Scope submission substituted a final source or requirements subject')
        verifier = {'protocol': self.protocol, 'purpose': 'independent-terminal-originals-verifier',
            'study_checkpoint': asdict(self.study_expected), 'originals': _material(originals),
            'subjects': [asdict(subject) for subject in subjects], 'sources': self.sources,
            'acceptance_authority': False}
        self._put('final.terminal-verifier', verifier)
        self.originals, self.subjects = originals, subjects
        if originals.freeze_eligible:
            require(len(subjects) == 6 and originals.barrier_name is not None
                    and originals.barrier_sha256 is not None, 'Complete freeze has no original six-child barrier')
            raw = self.study_chain.read(originals.barrier_name)
            require(hashlib.sha256(raw).hexdigest() == originals.barrier_sha256
                    and self.study_chain.position(originals.barrier_name) == originals.barrier_position,
                    'Original barrier receipt or chronology differs')
            verified_raw = self.chain.read(Records.name('final.terminal-verifier'))
            self.freeze = registry.CohortFreeze(subjects, originals.barrier_sha256,
                                                hashlib.sha256(verified_raw).hexdigest(), True)
            self._put('final.freeze', {'protocol': self.protocol, 'freeze': asdict(self.freeze),
                'barrier_name': originals.barrier_name, 'barrier_position': originals.barrier_position,
                'original_context_sha256': self.study_expected.context_sha256,
                'verifier_name': Records.name('final.terminal-verifier')})
        self._current_originals()
        return self.freeze

    def _current_originals(self) -> None:
        self._current()
        require(self.originals is not None and self._read_terminal() == self.originals,
                'Original terminal evidence changed or was never verified')
        value = self.records.read('final.terminal-verifier')
        require(value is not None and value['originals'] == _material(self.originals), 'Retained original verifier changed')
        if self.freeze is not None:
            require(self.records.read('final.freeze')['freeze'] == plain(asdict(self.freeze)), 'Retained cohort freeze changed')  # type: ignore[index]
            require(hashlib.sha256(self.chain.read(Records.name('final.terminal-verifier'))).hexdigest() ==
                    self.freeze.verifier_receipt_sha256, 'Original freeze verifier changed')

    def _registered_gate(self, registration: admission.ObservationRegistration) -> scope_authority.RegistrationProvenance:
        self._current_originals()
        if self.freeze is None:
            raise consumer.AuthorityUnavailable('Six authentic terminal sources and original barrier required')
        gate, subject = registration.gate, registration.gate.binding.subject
        require(subject in self.subjects and registration.cohort_trajectory_ids ==
                tuple(child.trajectory for child in self.plan.roster.children), 'Final subject/complete roster differs')
        slot = next(row for row in self.originals.slots if row.trajectory == subject.trajectory_id)
        require(registration.commit_oid == slot.final_source['commit_oid']
                and registration.tree_oid == slot.final_source['tree_oid'], 'Exact final Git revision differs')
        submission = self.submissions.get(subject.trajectory_id)
        registered = self.scope_snapshot.registration(subject)
        provenance = self.scope_snapshot.registration_provenance(subject)
        if submission is None or registered is None or provenance is None:
            raise consumer.AuthorityUnavailable('Complete independently reviewed scope is not registered')
        require(submission.subject == subject and submission.declaration.cohort == self.plan.cohort,
                'Reviewed scope belongs to another study cohort')
        compiled = compiler.compile_design(submission.catalog.inventory, submission.declaration, subject,
            scope_plan=submission.scope, expected_scope_sha256=registered.scope_sha256)
        require(compiled.registry is not None and not compiled.blockers
                and gate in compiled.registry.gates
                and registry.design_fingerprint(compiled.registry) == registered.registry_design_sha256,
                'Gate is not in the complete independently registered design')
        return provenance

    def _callback(self, key: str) -> admission.ObservationRegistration:
        self._current_originals()
        row = self.enrollments[key]
        require(row.phase in ('constructing', 'bound', 'dispatching', 'observing', 'complete'),
                'Observation admission is no longer available')
        require(self._registered_gate(row.registration) == row.scope
                and self.records.read(key + '.admission') == row.admission_record,
                'Original prospective cross-journal admission changed')
        return row.registration

    def _storage_factory(self) -> ModuleType:
        return recipe_factory

    def _construct(self, spec: ObservationSpec, issued: admission.ObservationAdmission) -> Any:
        common = {'checkpoint_authority': spec.checkpoint_authority, 'delta_root': spec.delta_root,
                  'cleanup_root': spec.cleanup_root, 'endpoint': spec.endpoint, 'mode': 'physical'}
        if spec.kind == 'm2':
            from . import cumulative_m2_observation_recipe_v1 as m2_factory
            return m2_factory.construct_m2_owner(spec, issued, mode='physical')
        if spec.kind == 'storage':
            return self._storage_factory().construct_storage_owner(spec, issued, mode='physical')
        if spec.kind == 'cli':
            return cli.CandidateClientExecution(spec.root, spec.store, spec.registration, spec.policy,
                admission_authority=issued, cumulative_profile=spec.cumulative_profile, **common)
        module = http if spec.kind == 'http' else product
        return module.CandidateHttpExecution(spec.root, spec.store, spec.registration, spec.recipe, spec.policy,
            profile=spec.profile, observation_admission=issued, **common)

    def _protected_roots(self) -> list[Path]:
        protected: list[Path] = []
        for chain in (self.chain, self.study_chain, self.scope_owner.chain):
            authority = chain.authority
            require(type(authority) is ExternalHead, 'Proof head authority changed')
            assert isinstance(authority, ExternalHead)
            protected.extend(Path(path).resolve() for path in (chain.raw_root, chain.delta_root, authority.root))
        return protected

    @normalize_authority
    def dispatch(self, spec: ObservationSpec) -> registry.Observation:
        """Run one fresh actual owner; unknown intent cannot be resumed/retried."""
        require(type(spec) is ObservationSpec, 'Exact construction specification required')
        require(spec.kind in _known_contract(self).families, 'Observation family is not in this contract version')
        if self.dispatch_halt is not None:
            raise consumer.AuthorityUnavailable('Prior physical execution halted further dispatch')
        require(self.records.read('final.assessment') is None, 'Assessment already sealed the observation census')
        registration = spec.observation_registration()
        provenance = self._registered_gate(registration)
        key = 'final.observation.' + registry.fingerprint(registration.gate)
        require(self.records.read(key + '.admission') is None and key not in self.enrollments,
                'Observation slot already attempted; no reuse or redispatch')
        require(type(spec.checkpoint_authority) is ExternalHead and type(spec.store) is GitStore,
                'Actual external checkpoint and protected Git store required')
        slot = next(row for row in self.originals.slots if row.trajectory == registration.gate.binding.subject.trajectory_id)
        require(str(spec.store.path.resolve()) == slot.final_source['repository'], 'Another final Git store supplied')
        protected = self._protected_roots()
        if spec.kind in ('storage','m2'):
            review_chain = spec.layout_authority.journal
            require(type(review_chain.authority) is ExternalHead, 'Layout review head authority changed')
            protected.extend((review_chain.raw_root, review_chain.delta_root, review_chain.authority.root))
        candidate_roots = (spec.root, spec.delta_root, spec.cleanup_root, spec.checkpoint_authority.root)
        require(all(path.is_absolute() and path.resolve() == path for path in candidate_roots)
                and not spec.root.exists() and not spec.delta_root.exists() and not spec.cleanup_root.exists()
                and all(not a.is_relative_to(b) and not b.is_relative_to(a) for a in candidate_roots for b in protected),
                'Fresh execution roots must be outside all original proof roots')
        assert isinstance(self.freeze, registry.CohortFreeze)
        record = {'protocol': self.protocol, 'registration': asdict(registration), 'scope': asdict(provenance),
            'freeze': asdict(self.freeze), 'study_checkpoint': asdict(self.study_expected),
            'kind': spec.kind, 'root': str(spec.root), 'delta_root': str(spec.delta_root),
            'cleanup_root': str(spec.cleanup_root), 'head_root': str(spec.checkpoint_authority.root)}
        record = self._put(key + '.admission', record)
        row = _Enrollment(registration, provenance, record)
        self.enrollments[key] = row
        issued = admission.ObservationAdmission(registration, verify_registration=lambda:self._callback(key),
            verify_cohort=lambda:self._verified_freeze())
        row.admission_owner = issued
        try:
            owner = self._construct(spec, issued)
            row.owner = owner
            expected_type: type[Any]
            if spec.kind == 'm2':
                from . import candidate_m2_product_execution_v1 as m2
                expected_type = m2.CandidateM2Execution
            else:
                expected_type = storage.CandidateStorageExecution if spec.kind == 'storage' else (
                    cli.CandidateClientExecution if spec.kind == 'cli' else (
                    http.CandidateHttpExecution if spec.kind == 'http' else product.CandidateHttpExecution))
            require(type(owner) is expected_type, 'Factory did not construct the exact physical owner')
            actual = owner.observation_registration if spec.kind in ('cli', 'storage', 'm2') else owner.actual_registration
            before = owner.checkpoint()
            require(owner.admission is issued and owner.mode == 'physical' and actual == registration
                    and before.sequence == before.raw_file_count == 1,
                    'Owner is not the fresh config-only admitted execution')
            config = owner.read_authenticated('config.json')
            self._put(key + '.bound', {**record, 'owner_checkpoint': asdict(before),
                'config_sha256': hashlib.sha256(config).hexdigest()})
            require(owner.checkpoint() == before, 'Owner changed before dispatch')
            row.phase = 'bound'
            self._put(key + '.dispatch', {'protocol': self.protocol, 'registration_sha256': digest(asdict(registration)),
                                        'owner_checkpoint': asdict(before)})
            row.phase = 'dispatching'
            history = owner.execute_once()
            bridge: Any
            if spec.kind == 'm2':
                from . import candidate_m2_product_observation_v1 as m2_source
                from . import cumulative_m2_observation_recipe_v1 as m2_factory
                row.phase = 'observing'
                verified = m2_source.publish_verifier(owner)
                history = m2_factory.m2_history(owner, history)
                self._history(key, row, history)
                bridge = m2_source.M2ObservationSource(owner, verified)
            elif spec.kind == 'storage':
                row.phase = 'observing'
                verified = storage_source.publish_verifier(owner)
                history = self._storage_factory().storage_history(owner, history)
                self._history(key, row, history)
                bridge = storage_source.StorageObservationSource(owner, verified)
            else:
                history_type = cli.ClientHistoryResult if spec.kind == 'cli' else (
                    http.HttpHistoryResult if spec.kind == 'http' else product.HttpHistoryResult)
                require(type(history) is history_type and owner.checkpoint() == history.checkpoint,
                        'Actual executor history or checkpoint changed')
                self._history(key, row, history)
                row.phase = 'observing'
                if spec.kind == 'cli':
                    verified = cli_source.publish_verifier(owner)
                    bridge = cli_source.ClientObservationSource(owner, verified)
                else:
                    module = http_source if spec.kind == 'http' else product_source
                    bridge = module.HttpObservationSource(owner, owner.checkpoint(), receipt_path=owner.root / 'semantic-verifier.json')
            observation = bridge.observation(registration.gate,
                None if registration.gate.binding.purpose == 'public_release' else self.freeze)
            row.source, row.observation, row.post_checkpoint = bridge, observation, owner.checkpoint()
            self._put(key + '.verified', {'protocol': self.protocol, 'observation': asdict(observation),
                'post_checkpoint': asdict(row.post_checkpoint), 'history': row.history,
                'intent_sha256': hashlib.sha256(owner.read_authenticated('intent.json')).hexdigest(),
                'terminal_sha256': hashlib.sha256(owner.read_authenticated('terminal.json')).hexdigest()})
            row.phase = 'complete'
            self._current_originals()
            require(owner.checkpoint() == row.post_checkpoint, 'Original observation changed at publication')
            return observation
        except BaseException:
            row.phase = 'unavailable'
            if self.dispatch_halt is None:
                self.dispatch_halt = {'slot': key, 'reason': 'physical_execution_exception'}
            # The admission/dispatch prefix remains the original failed attempt.
            # Cleanup belongs to the concrete owner; no failed slot is rearmed.
            raise

    def _history(self, key: str, row: _Enrollment, history: Any) -> None:
        # Original raw observations remain in the authenticated owner journal;
        # retain its exact bounded summary without duplicating binary streams.
        row.history = {'execution_id': history.execution_id, 'status': history.status,
            'cleanup_verified': history.cleanup_verified, 'infrastructure': list(history.infrastructure),
            'missing_step_ids': list(history.missing_step_ids), 'terminal_sha256': history.terminal_sha256,
            'checkpoint': asdict(history.checkpoint)}
        if history.cleanup_verified is not True:
            self.dispatch_halt = {'slot': key, 'reason': 'original_cleanup_not_verified'}
            self._put('final.dispatch-halt', self.dispatch_halt)
        self._put(key + '.history', row.history)

    def _verified_freeze(self) -> registry.CohortFreeze:
        self._current_originals()
        if self.freeze is None:
            raise consumer.AuthorityUnavailable('No authentic complete cohort freeze')
        return self.freeze

    @normalize_authority
    def open_snapshot(self) -> consumer.AuthoritySnapshot:
        self._current_originals()
        require(all(row.phase in ('complete', 'unavailable') for row in self.enrollments.values()),
                'Cannot grade a changing physical observation')
        return _Snapshot(self)

    @normalize_authority
    def assess(self) -> dict[str, Any]:
        """All six planned slots remain visible, including unknown/unattempted."""
        self._current_originals()
        results = []
        for slot in self.originals.slots:
            submission = self.submissions.get(slot.trajectory)
            subject = next((value for value in self.subjects if value.trajectory_id == slot.trajectory), None)
            assessed = None
            if submission is not None and subject is not None:
                assessed = consumer.CandidateScopeConsumer(self).assess(submission.catalog.inventory,
                    submission.declaration, submission.scope, subject)
            results.append({'trajectory': slot.trajectory, 'terminal_outcome': slot.outcome,
                'process_counts': slot.process_counts, 'evidence_errors': slot.evidence_errors,
                'financial_origin': slot.financial_origin,
                'subject': None if subject is None else asdict(subject),
                'assessment': None if assessed is None else asdict(assessed),
                'physical_diagnostics': [asdict(row.observation) for row in self.enrollments.values()
                    if row.observation is not None and row.registration.gate.binding.subject.trajectory_id == slot.trajectory],
                'unavailable_observation_slots': [key for key,row in self.enrollments.items()
                    if row.phase == 'unavailable' and row.registration.gate.binding.subject.trajectory_id == slot.trajectory],
                'accepted_against_registry': assessed is not None and assessed.accepted_against_registry})
        self._current_originals()
        out = {'protocol': self.protocol, 'study_sha256': self.plan.sha256, 'planned_trajectories': 6,
            'freeze_available': self.freeze is not None, 'slots': results,
            'dispatch_halt': self.dispatch_halt,
            'financial_mode': self.plan.runtime['final_acceptance_financial_mode'],
            'live_financial_origin': self.freeze is not None and self.plan.runtime['final_acceptance_financial_mode'] == 'live',
            'accepted': self.dispatch_halt is None and self.freeze is not None and all(row['accepted_against_registry'] for row in results),
            'completed_and_accepted': self.dispatch_halt is None and self.freeze is not None and all(row['accepted_against_registry']
                and row['terminal_outcome'] == 'completed' for row in results),
            'missing_producers': self.missing_producers(), 'authority_diagnostics': self.authority_diagnostics()}
        self._put('final.assessment', out)
        self._current_originals()
        return out

    def authority_diagnostics(self) -> list[dict[str, Any]]:
        return []

    def missing_producers(self) -> list[str]:
        return ['original SCOPE/ADMISSION/CONTROL qualification', 'original promotion/CAS/review authority']

    def close(self) -> None:
        errors = []
        for row in self.enrollments.values():
            if row.owner is not None:
                try:
                    row.owner.close()
                except BaseException as error:
                    errors.append(type(error).__name__ + ': ' + str(error))
        self.closed = True
        if errors:
            raise consumer.AuthorityUnavailable('Observation owner cleanup unavailable: ' + '; '.join(errors))


class _Snapshot(consumer.AuthoritySnapshot):
    def __init__(self, owner: FinalAcceptance):
        self.owner, self.expected = owner, owner.expected
        self._checkpoint = digest({'protocol': owner.protocol, 'acceptance_context': self.expected.context_sha256,
            'sources': owner.sources, 'freeze': None if owner.freeze is None else asdict(owner.freeze),
            'study': asdict(owner.study_expected), 'scope': owner.scope_snapshot.checkpoint,
            'observations': {key:None if row.post_checkpoint is None else asdict(row.post_checkpoint)
                             for key,row in owner.enrollments.items()}})

    @property
    def checkpoint(self) -> str:
        return self._checkpoint

    @normalize_authority
    def check_current(self, checkpoint: str) -> None:
        require(checkpoint == self.checkpoint and self.owner.expected == self.expected, 'Acceptance snapshot changed')
        self.owner._current_originals()
        for row in self.owner.enrollments.values():
            if row.phase == 'complete':
                require(row.owner.checkpoint() == row.post_checkpoint, 'Original execution checkpoint changed')

    @normalize_authority
    def registration(self, subject: registry.Subject) -> consumer.RegisteredAcceptance | None:
        self.check_current(self.checkpoint)
        return self.owner.scope_snapshot.registration(subject)

    @normalize_authority
    def qualification(self, request: consumer.QualificationRequest) -> consumer.QualificationEvidence | None:
        self.check_current(self.checkpoint)
        return self.owner.scope_snapshot.qualification(request)

    @normalize_authority
    def freeze(self) -> registry.CohortFreeze | None:
        self.check_current(self.checkpoint)
        return self.owner.freeze

    @normalize_authority
    def observation(self, gate: registry.Gate, freeze: registry.CohortFreeze | None) -> registry.Observation | None:
        self.check_current(self.checkpoint)
        require(freeze == self.owner.freeze, 'Consumer requested another original freeze')
        row = self.owner.enrollments.get('final.observation.' + registry.fingerprint(gate))
        if row is None:
            return None
        if row.phase != 'complete':
            raise consumer.AuthorityUnavailable('Original dispatch did not yield an authenticated observation')
        observed = row.source.observation(gate, None if gate.binding.purpose == 'public_release' else freeze)
        require(observed == row.observation, 'Reopened reader differs from enrolled original')
        self.check_current(self.checkpoint)
        return observed

    @normalize_authority
    def promotion(self, subject: registry.Subject) -> registry.Promotion | None:
        self.check_current(self.checkpoint)
        return self.owner.scope_snapshot.promotion(subject)
