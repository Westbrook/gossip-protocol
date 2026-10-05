"""Original workflow source-plan review and fresh host product inspection.

The host enrolls independent reviewer deliveries outside candidate execution.
Checkpoint hashes authenticate retained originals, not reviewer identity or
semantic adequacy. Mechanism approval and product inspection are distinct:
rejected product duties remain failures. No actual review is shipped here.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import os
from pathlib import Path
import platform
import threading
from typing import Any, cast
import uuid

from . import candidate_checkpoint_chain_v1 as chain
from .candidate_checkpoint_head_v1 import ExternalHead
from . import candidate_execution_journal_v1 as journals
from . import candidate_observation_admission_v1 as admission
from . import candidate_source_capture_policy_v2 as capture
from .candidate_storage_product_profile_v1 import encoded, decode, digest
from .candidate_storage_review_authority_v1 import ReviewEnrollment
from . import project_acceptance_registry_v1 as registry
from .gitstore import GitStore

PROTOCOL = 'candidate-workflow-mechanism-review-v1'
PURPOSE = 'independent_final_m4_workflow_layout_and_invocation'
INSPECTION_PROTOCOL = 'candidate-workflow-product-inspection-v1'
INSPECTION_PURPOSE = 'independent_final_m4_workflow_product_inspection'
INSPECTION_FAMILY = 'workflow-source-inspection-v1'
ORIGINAL_DEFINITION_PURPOSE = 'source_bound_qualification'
VERIFIER_FILE = 'inspection-verifier.json'
SOURCE_CAPTURE_POLICY = capture.TwoProcessCapturePolicy()
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
require = admission.require
DUTIES = ('complete_source_and_all_persisted_state',
    'exact_capture_paths_schema_and_auxiliary_representation',
    'public_workflow_and_final_m4_inheritance',
    'boundary_instrumentation_is_read_only_and_source_bound',
    'exact_conservation_capture_roles_and_operation_order',
    'snapshot_limitations_and_remaining_obligations')
INSPECTION_DUTIES = ('immutable_solve_entry_and_real_workflow_delegation',
    'per_solve_isolated_real_input_root_and_database',
    'actual_public_importer_and_store_boundaries',
    'actual_m1_jobmanager_returns_and_final_jobs',
    'real_store_close_reopen_and_persistence',
    'actual_provisional_write_fault_hook_and_rollback_structure',
    'approved_v2_epoch_rule_and_unchanged_non_token_admission_behavior')


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _relative(value: str) -> None:
    require(type(value) is str and bool(value) and not value.startswith('/') and '\\' not in value
        and '\x00' not in value and all(p not in ('', '.', '..') for p in value.split('/')),
        'Confined relative source/capture path required')


def _oid(value: str) -> None:
    require(type(value) is str and len(value) == 40 and all(c in '0123456789abcdef' for c in value),
            'Complete Git identity required')


def source_sha256(files: dict[str, bytes]) -> str:
    return digest({'protocol': 'workflow-native-source-v1', 'files': admission.source_manifest(files)})


def capture_git_source(store: GitStore, commit_oid: str) -> tuple[str, dict[str, bytes]]:
    require(type(store) is GitStore, 'Actual final Git store required')
    return capture.capture_registered_source(store, commit_oid, policy=SOURCE_CAPTURE_POLICY)


@dataclass(frozen=True, slots=True)
class WorkflowBoundary:
    id: str
    source_path: str
    function: str
    line: int
    event: str
    root_local: str | None
    database_local: str | None
    meaning: str
    occurrences: int = 1

    def __post_init__(self) -> None:
        registry.identifier(self.id)
        _relative(self.source_path)
        require(self.source_path.endswith('.py') and type(self.function) is str
            and self.function.isidentifier(), 'Exact source function required')
        require(type(self.line) is int and self.line > 0 and self.event in ('line', 'return'),
                'Exact source event/line required')
        require(self.meaning in ('solve_enter', 'solve_exit', 'reopen', 'fault', 'post_fault', 'reopened'), 'Closed boundary meaning required')
        require(type(self.occurrences) is int and 1 <= self.occurrences <= 256, 'Bounded maximum occurrences per call required')
        for value in (self.root_local, self.database_local):
            require(value is None or (type(value) is str and value.isidentifier()), 'Local names only; never eval')


@dataclass(frozen=True, slots=True)
class WorkflowCapturePoint:
    """An enrolled source event selection, never a candidate-owned verdict.

    The exact request/report must approve what this event observes in the
    invoked source. Roles do not authenticate themselves or infer execution.
    """
    role: str
    call_index: int
    boundary_id: str
    occurrence: int

    def __post_init__(self) -> None:
        require(self.role in ('initial', 'post_fault', 'reopened', 'final'),
                'Closed workflow conservation role required')
        require(type(self.call_index) is int and 0 <= self.call_index <= 2,
                'Exact bounded workflow call index required')
        registry.identifier(self.boundary_id)
        require(type(self.occurrence) is int and 0 <= self.occurrence < 256,
                'Exact bounded source-event occurrence required')


@dataclass(frozen=True, slots=True)
class WorkflowSourcePlan:
    source_sha256: str
    native_source_sha256: str
    commit_oid: str
    tree_oid: str
    case_id: str
    profile_sha256: str
    layout: str
    storage_paths: tuple[str, ...]
    schema_sha256: str
    purpose: str
    boundaries: tuple[WorkflowBoundary, ...] = ()
    profile_kind: str = 'workflow'
    capture_points: tuple[WorkflowCapturePoint, ...] = ()

    def __post_init__(self) -> None:
        for value in (self.source_sha256, self.native_source_sha256, self.profile_sha256, self.schema_sha256):
            registry.sha256(value)
        _oid(self.commit_oid)
        _oid(self.tree_oid)
        registry.identifier(self.case_id)
        require(self.purpose in registry.PURPOSES and self.profile_kind in ('workflow', 'workflow_inspection'),
                'Exact workflow profile kind/purpose required')
        require(self.layout == 'reviewed-workflow-final-sqlite-v1', 'Closed source-reviewed SQLite mapping required')
        require(type(self.storage_paths) is tuple and bool(self.storage_paths)
            and len(set(self.storage_paths)) == len(self.storage_paths), 'Exact complete capture paths required')
        for path in self.storage_paths:
            _relative(path)
        require(type(self.boundaries) is tuple and all(type(row) is WorkflowBoundary for row in self.boundaries)
            and len({row.id for row in self.boundaries}) == len(self.boundaries), 'Exact distinct boundary roster required')
        if self.profile_kind == 'workflow':
            require(bool(self.boundaries), 'Actual source-bound workflow capture boundaries required')
        require(type(self.capture_points) is tuple
            and all(type(row) is WorkflowCapturePoint for row in self.capture_points)
            and len({(row.role, row.call_index) for row in self.capture_points}) == len(self.capture_points),
            'Exact unique conservation role/call selections required')
        boundaries = {row.id: row for row in self.boundaries}
        for point in self.capture_points:
            require(point.boundary_id in boundaries
                and point.occurrence < boundaries[point.boundary_id].occurrences,
                'Conservation point exceeds its enrolled source boundary')
        if self.profile_kind == 'workflow' and self.case_id == 'WF19-provisional-fault-boundary':
            require({(row.role, row.call_index) for row in self.capture_points} == {
                ('initial', 0), ('post_fault', 0), ('reopened', 0), ('final', 0)},
                'All four exact WF19 conservation points required')
        else:
            require(not self.capture_points, 'Conservation points are closed to the declared WF19 profile')

    def record(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def sha256(self) -> str:
        return digest(self.record())

    def request(self) -> dict[str, Any]:
        if self.profile_kind == 'workflow':
            from . import candidate_workflow_profile_v1 as profiles
            value: Any
            value, record = profiles.profile_record_for(self.case_id, self.purpose)
        else:
            value = WorkflowInspectionProfile(self.purpose)
            record = value.record()
        require(value.case_id == self.case_id and digest(record) == self.profile_sha256,
                'Reviewed workflow profile differs from exact source definition')
        return {'protocol': PROTOCOL, 'purpose': PURPOSE, 'plan': self.record(), 'profile': record,
            'duties': list(DUTIES), 'scope': 'named workflow source attribution only',
            'product_correctness_approved': False, 'whole_scope_approved': False}


def _authenticate(journal: chain.CheckpointChain, expected: chain.PrefixCommitment,
        enrollment: ReviewEnrollment, request: dict[str, Any], *, protocol: str, purpose: str,
        duties: tuple[str, ...], approval_only: bool) -> dict[str, Any]:
    require(type(journal) is chain.CheckpointChain and type(expected) is chain.PrefixCommitment
        and type(journal.authority) is ExternalHead and type(enrollment) is ReviewEnrollment,
        'Exact protected original review capability required')
    before = journal.validate_boundary(expected=expected).commitment
    records: list[Any] = []
    for name, sha in ((enrollment.request_name, enrollment.request_sha256),
        (enrollment.report_name, enrollment.report_sha256), (enrollment.delivery_name, enrollment.delivery_sha256)):
        raw = journal.read(name)
        require(_sha(raw) == sha, 'Retained review bytes differ from enrolled originals')
        record = decode(raw)
        require(encoded(record) == raw, 'Canonical original review records required')
        records.append(record)
    original, report, delivery = records
    require(encoded(original) == encoded(request), 'Original review request differs from complete source/profile')
    require(type(report) is dict and set(report) == {'protocol','purpose','reviewer_id','request_sha256',
        'decisions','remaining_obligations'} and report['protocol'] == protocol and report['purpose'] == purpose
        and report['reviewer_id'] == enrollment.reviewer_id and report['request_sha256'] == enrollment.request_sha256,
        'Wrong original review role/purpose/request')
    decisions = report['decisions']
    require(type(decisions) is list and len(decisions) == len(duties), 'Complete ordered review duties required')
    for duty, row in zip(duties, decisions, strict=True):
        require(type(row) is dict and set(row) == {'id','decision','rationale','source_references'}
            and row['id'] == duty and row['decision'] in ('approved','rejected','unresolved')
            and type(row['rationale']) is str and bool(row['rationale'].strip())
            and type(row['source_references']) is list and bool(row['source_references'])
            and all(type(ref) is str and bool(ref.strip()) for ref in row['source_references']),
            'Ungrounded or malformed review decision')
        if approval_only:
            require(row['decision'] == 'approved', 'Mechanism attribution review remains unresolved or rejected')
    require(type(report['remaining_obligations']) is list and bool(report['remaining_obligations'])
        and all(type(item) is str and bool(item.strip()) for item in report['remaining_obligations']),
        'A workflow review cannot waive remaining full-project obligations')
    require(delivery == {'protocol': protocol, 'purpose': purpose, 'reviewer_id': enrollment.reviewer_id,
        'request_sha256': enrollment.request_sha256, 'report_sha256': enrollment.report_sha256,
        'origin': 'independently_delivered_host_review'}, 'Protected independent delivery differs')
    positions = {key: journal.position(name) for key, name in (
        ('request', enrollment.request_name), ('report', enrollment.report_name), ('delivery', enrollment.delivery_name))}
    require(positions['request'] < positions['report'] < positions['delivery'], 'Original review chronology differs')
    require(journal.validate_boundary(expected=expected).commitment == before, 'Review prefix changed during read')
    return {'report': report, 'provenance': {'protocol': protocol, 'purpose': purpose,
        'expected_prefix': asdict(expected), 'enrollment': asdict(enrollment), 'positions': positions}}


class WorkflowReviewAuthority:
    def __init__(self, journal: chain.CheckpointChain, expected: chain.PrefixCommitment, enrollment: ReviewEnrollment):
        require(type(journal) is chain.CheckpointChain and type(journal.authority) is ExternalHead
            and type(expected) is chain.PrefixCommitment and type(enrollment) is ReviewEnrollment,
            'Exact independently anchored mechanism review required')
        self.journal, self.expected, self.enrollment = journal, expected, enrollment

    def _original(self, plan: WorkflowSourcePlan) -> dict[str, Any]:
        require(type(plan) is WorkflowSourcePlan, 'Exact workflow source plan required')
        return _authenticate(self.journal, self.expected, self.enrollment, plan.request(), protocol=PROTOCOL,
            purpose=PURPOSE, duties=DUTIES, approval_only=True)

    def authenticate(self, plan: WorkflowSourcePlan) -> str:
        self._original(plan)
        return self.enrollment.report_sha256

    def provenance(self, plan: WorkflowSourcePlan) -> dict[str, Any]:
        return cast(dict[str, Any], self._original(plan)['provenance'])

    def authenticate_with_provenance(self, plan: WorkflowSourcePlan) -> tuple[str, dict[str, Any]]:
        """Both identities from one fresh, before/after checked original read.

        No result survives this call. Derive the report identity from the
        authenticated canonical record, not mutable enrollment after the read.
        """
        original = self._original(plan)
        return digest(original['report']), cast(dict[str, Any], original['provenance'])


@dataclass(frozen=True, slots=True)
class WorkflowInspectionProfile:
    purpose: str = 'independent_acceptance'

    def __post_init__(self) -> None:
        require(self.purpose in ('independent_acceptance', 'repeatability'), 'Fresh independent host inspection required')

    @property
    def case_id(self) -> str:
        return 'workflow-source-inspection'

    @property
    def family(self) -> str:
        return INSPECTION_FAMILY

    @property
    def ordered_case_ids(self) -> tuple[str, ...]:
        return tuple(INSPECTION_FAMILY + ':' + value for value in INSPECTION_DUTIES)

    @property
    def requirement_ids(self) -> tuple[str, ...]:
        return ('V0-ADAPTER-01', 'V0-ADAPTER-02', 'M1-ADAPTER-03', 'M1-A07', 'M4-COMPATIBILITY')

    @property
    def sha256(self) -> str:
        return digest(self.record())

    def selectors(self) -> list[dict[str, Any]]:
        """Exact source-review duties, distinct from their required executions.

        M1-ADAPTER-03 has no ARCHITECTURE lane. Its runtime observations and all
        M4 compatibility cells remain separate; source labels cannot create an
        unsupported owner/lane association.
        """
        declarations = (
            (('m1:V0-ADAPTER-01', 'm1:V0-ADAPTER-02'),
             'Independent invoked-source inspection of the immutable solve entry and real workflow delegation; output equality supplies no delegation proof.'),
            (('m1:V0-ADAPTER-02',),
             'Independent source inspection of per-call real temporary input/database isolation; actual path and state observations remain separately required.'),
            (('m1:V0-ADAPTER-02',),
             'Independent source inspection of real importer/Store calls in the workflow; actual file and persisted effects remain separately required.'),
            (('m1:V0-ADAPTER-02',),
             'Independent source inspection of actual workflow JobManager delegation and job projection; this does not invent an ARCHITECTURE lane for M1-ADAPTER-03.'),
            (('m1:V0-ADAPTER-02',),
             'Independent source inspection of real Store close/reopen control flow; complete persisted/reopened observations remain separately required.'),
            (('m1:V0-ADAPTER-02',),
             'Independent exact invoked-source inspection of after-write/before-commit hook placement and rollback control flow; actual graph/job/receipt/reopen conservation remains separately required and this is not whole M1-A07 proof.'),
            (('CLARIFY-INPUT-DOMAIN', 'm1:V0-ADAPTER-02'),
             'Independent source inspection of approved token-before-lookup behavior and unchanged non-token admitted-domain handling; evaluator admission and actual result observations remain separate.'),
        )
        return [{'case_id': self.ordered_case_ids[index], 'pointer': '/outcomes/' + str(index) + '/status',
            'definition_pointer': '/duties/' + str(index), 'assertion_kind': 'inspection',
            'evidence_kind': 'source_inspection', 'value_domain': ['passed', 'failed', 'infrastructure_error'],
            'source_unit_ids': list(units), 'logical_gate_ids': ['M1-GATE-ARCHITECTURE'],
            'source_unit_facets': [{'source_unit_id': unit, 'logical_gate_ids': ['M1-GATE-ARCHITECTURE'],
                'rationale': rationale} for unit in units], 'rationale': rationale,
            'semantic_authority_supplied': False, 'physical_engine_evidence': False,
            'whole_source_unit_qualified': False} for index, (units, rationale) in enumerate(declarations)]

    def record(self) -> dict[str, Any]:
        from . import cumulative_workflow_exposure_v1 as exposure
        from . import cumulative_cli_projection_v1 as cli
        return {'protocol': INSPECTION_PROTOCOL, 'family': self.family, 'case_id': self.case_id,
            'purpose': self.purpose, 'original_definition_purpose': ORIGINAL_DEFINITION_PURPOSE,
            'duties': list(INSPECTION_DUTIES), 'selectors': self.selectors(),
            'ordered_case_ids': list(self.ordered_case_ids),
            'requirement_ids': list(self.requirement_ids), 'effective_requirements': cli.manifest(),
            'workflow_requirements': exposure.manifest(), 'combined_effective_requirements': exposure.combined_manifest(),
            'whole_scope_approved': False, 'physical_engine_evidence': False}


def inspection_evaluator_sources() -> dict[str, str]:
    from . import cumulative_workflow_exposure_v1 as exposure
    result = {**exposure.definition_sources(), **capture.evaluator_sources()}
    names = ('candidate_workflow_review_v1.py', 'candidate_checkpoint_chain_v1.py',
        'candidate_checkpoint_head_v1.py', 'candidate_execution_journal_v1.py',
        'candidate_observation_admission_v1.py', 'candidate_storage_review_authority_v1.py',
        'candidate_storage_product_profile_v1.py', 'candidate_http_journal_v3.py',
        'project_acceptance_registry_v1.py', 'gitstore.py')
    root = Path(__file__).resolve().parent
    result.update({'gossip_harness/' + name: _sha((root/name).read_bytes()) for name in names})
    admission.verify_loaded_sources(result)
    return dict(sorted(result.items()))


def _host_runtime() -> dict[str, Any]:
    return {'kind': 'fresh_host_source_inspection', 'python': [platform.python_implementation(), platform.python_version()],
            'platform': platform.system(), 'machine': platform.machine(), 'engine_used': False}


@dataclass(frozen=True, slots=True)
class WorkflowInspectionRegistration:
    gate: registry.Gate
    commit_oid: str
    tree_oid: str
    repetition_id: str
    cohort_trajectory_ids: tuple[str, ...]
    profile: WorkflowInspectionProfile
    source_plan_sha256: str
    review_sha256: str
    review_origin_sha256: str

    def __post_init__(self) -> None:
        require(type(self.gate) is registry.Gate and type(self.profile) is WorkflowInspectionProfile,
                'Exact original inspection gate/profile required')
        _oid(self.commit_oid)
        _oid(self.tree_oid)
        registry.identifier(self.repetition_id)
        registry.identifiers(self.cohort_trajectory_ids)
        require(len(self.cohort_trajectory_ids) == 6 and self.gate.binding.subject.trajectory_id in self.cohort_trajectory_ids,
                'All six exact subject trajectories required')
        for value in (self.source_plan_sha256, self.review_sha256, self.review_origin_sha256):
            registry.sha256(value)
        require(self.gate.binding.purpose == self.profile.purpose and self.gate.ordered_case_ids == self.profile.ordered_case_ids
            and self.gate.requirement_ids == self.profile.requirement_ids, 'Inspection profile/gate differs')


def inspection_registration_for(files: dict[str, bytes], subject: registry.Subject, *,
        value: WorkflowInspectionProfile, plan: WorkflowSourcePlan, review_authority: WorkflowReviewAuthority,
        commit_oid: str, tree_oid: str, repetition_id: str, gate_id: str,
        cohort_trajectory_ids: tuple[str, ...]) -> WorkflowInspectionRegistration:
    from . import cumulative_workflow_exposure_v1 as exposure
    require(type(value) is WorkflowInspectionProfile and type(plan) is WorkflowSourcePlan
        and type(review_authority) is WorkflowReviewAuthority and type(subject) is registry.Subject,
        'Exact host inspection definition/source authority required')
    exposure.validate_source(files)
    require(subject.milestone == 'M4' and subject.requirements_sha256 == exposure.BASE_SHA256
        and subject.source_sha256 == admission.source_sha256(files) == plan.source_sha256
        and plan.native_source_sha256 == source_sha256(files) and plan.commit_oid == commit_oid and plan.tree_oid == tree_oid
        and plan.profile_kind == 'workflow_inspection' and plan.profile_sha256 == value.sha256
        and plan.case_id == value.case_id and plan.purpose == value.purpose, 'Inspection source/profile plan differs')
    origin = review_authority.provenance(plan)
    binding = registry.Binding(subject, digest(value.record()), digest(inspection_evaluator_sources()),
        digest(_host_runtime()), digest({'environment': 'protected-host-review-no-candidate-process'}),
        digest({'journal': asdict(chain.Limits()), 'capture': SOURCE_CAPTURE_POLICY.record()}),
        digest({'seed': 'fixed-workflow-source-duties-v1'}), INSPECTION_PROTOCOL, value.purpose)
    gate = registry.Gate(gate_id, value.requirement_ids, value.ordered_case_ids, binding)
    return WorkflowInspectionRegistration(gate, commit_oid, tree_oid, repetition_id, cohort_trajectory_ids,
        value, plan.sha256, review_authority.enrollment.report_sha256, digest(origin))


def inspection_observation_registration(registration: WorkflowInspectionRegistration) -> admission.ObservationRegistration:
    require(type(registration) is WorkflowInspectionRegistration, 'Exact inspection registration required')
    registration.__post_init__()
    return admission.ObservationRegistration(registration.gate, registration.commit_oid, registration.tree_oid,
        registration.repetition_id, registration.cohort_trajectory_ids, digest(registration.profile.record()),
        registration.profile.sha256, ORIGINAL_DEFINITION_PURPOSE, digest(asdict(registration)))


class ProductInspectionDelivery:
    """Host-installed original delivery; this class does not assert reviewer identity."""
    def __init__(self, journal: chain.CheckpointChain, expected: chain.PrefixCommitment, enrollment: ReviewEnrollment):
        require(type(journal) is chain.CheckpointChain and type(journal.authority) is ExternalHead
            and type(expected) is chain.PrefixCommitment and type(enrollment) is ReviewEnrollment,
            'Exact protected product-inspection delivery required')
        self.journal, self.expected, self.enrollment = journal, expected, enrollment

    def authenticate(self, request: dict[str, Any]) -> dict[str, Any]:
        return _authenticate(self.journal, self.expected, self.enrollment, request, protocol=INSPECTION_PROTOCOL,
            purpose=INSPECTION_PURPOSE, duties=INSPECTION_DUTIES, approval_only=False)


class WorkflowInspectionExecution:
    """Asynchronous fresh host inspection; begin and independent delivery are separate.

    No Engine, reviewer callback or candidate execution is hidden in this owner.
    Admission capabilities and reviewer enrollments are supplied by the trusted
    controller. Cold reopen never dispatches or upgrades a historical report.
    """
    def __init__(self, root: Path, store: GitStore, registration: WorkflowInspectionRegistration, *,
            value: WorkflowInspectionProfile, plan: WorkflowSourcePlan, review_authority: WorkflowReviewAuthority,
            admission_authority: admission.ObservationAdmission, checkpoint_authority: ExternalHead,
            delta_root: Path, expected_checkpoint: chain.PrefixCommitment | None = None,
            mode: str = 'physical', product_delivery: ProductInspectionDelivery | None = None):
        require(type(store) is GitStore and type(registration) is WorkflowInspectionRegistration
            and type(value) is WorkflowInspectionProfile and type(plan) is WorkflowSourcePlan
            and type(review_authority) is WorkflowReviewAuthority and type(admission_authority) is admission.ObservationAdmission
            and type(checkpoint_authority) is ExternalHead and mode in ('physical','fixture'), 'Closed host inspection constructor required')
        self.pid, self.thread, self.closed = os.getpid(), threading.get_ident(), False
        self.root, self.delta_root, self.store = root, delta_root, store
        self.registration, self.profile, self.plan = registration, value, plan
        self.review_authority, self.admission, self.mode = review_authority, admission_authority, mode
        self.checkpoint_authority = checkpoint_authority
        self.product_delivery: ProductInspectionDelivery | None = None
        self.journal: journals.OwnerJournal | None = None
        self.sources = inspection_evaluator_sources()
        self.observation_registration = inspection_observation_registration(registration)
        require(admission_authority.registration == self.observation_registration and value == registration.profile,
                'Issued original inspection admission differs')
        roots = (root, delta_root, checkpoint_authority.root)
        require(all(type(path) is Path or isinstance(path, Path) for path in roots)
            and all(path.is_absolute() and path.resolve() == path for path in roots), 'Canonical owner roots required')
        mechanism_head = review_authority.journal.authority
        assert isinstance(mechanism_head, ExternalHead)
        protected = (review_authority.journal.raw_root, review_authority.journal.delta_root, mechanism_head.root)
        require(all(not a.is_relative_to(b) and not b.is_relative_to(a) for a in roots for b in protected),
                'Host inspection roots overlap mechanism proof')
        require(checkpoint_authority.journal_roots == (root, delta_root), 'External owner roots differ')
        config = {'protocol': INSPECTION_PROTOCOL, 'mode': mode, 'registration': asdict(registration),
            'profile': value.record(), 'source_plan': plan.record(), 'review': review_authority.provenance(plan),
            'sources': self.sources, 'runtime': _host_runtime(), 'source_capture': SOURCE_CAPTURE_POLICY.record()}
        self.config = config
        try:
            self._source_current()
            self.journal = journals.OwnerJournal(root, delta_root, context={'protocol': INSPECTION_PROTOCOL,
                'config_sha256': digest(config), 'registration_sha256': digest(asdict(registration))},
                authority=checkpoint_authority, expected=expected_checkpoint)
            if expected_checkpoint is None:
                self._retain('config.json', encoded(config))
            else:
                require(self.read_authenticated('config.json') == encoded(config), 'Reopened inspection config differs')
            self.current()
            if product_delivery is not None:
                self.attach_original_delivery(product_delivery)
        except BaseException:
            try:
                self.close()
            except BaseException:
                pass  # Preserve constructor failure; this grants no cleanup evidence.
            raise

    def _owner(self) -> None:
        require(not self.closed and os.getpid() == self.pid and threading.get_ident() == self.thread,
                'Closed or foreign host inspection owner')

    def _source_current(self) -> None:
        require(self.store.head() == self.registration.commit_oid, 'Final source head changed')
        tree, files = capture_git_source(self.store, self.registration.commit_oid)
        require(tree == self.registration.tree_oid, 'Final inspection tree changed')
        actual = inspection_registration_for(files, self.registration.gate.binding.subject, value=self.profile,
            plan=self.plan, review_authority=self.review_authority, commit_oid=self.registration.commit_oid,
            tree_oid=tree, repetition_id=self.registration.repetition_id, gate_id=self.registration.gate.gate_id,
            cohort_trajectory_ids=self.registration.cohort_trajectory_ids)
        require(actual == self.registration and self.sources == inspection_evaluator_sources(), 'Inspection original binding changed')

    def current(self) -> None:
        self._owner()
        self._source_current()
        require(self.observation_registration == inspection_observation_registration(self.registration)
            == self.admission.registration and self.profile == self.registration.profile,
            'Mutable owner identity differs from original issued admission')
        if self.journal is not None:
            self.journal.checkpoint()
            if self.journal.has('config.json'):
                actual_config = {'protocol': INSPECTION_PROTOCOL, 'mode': self.mode,
                    'registration': asdict(self.registration), 'profile': self.profile.record(),
                    'source_plan': self.plan.record(), 'review': self.review_authority.provenance(self.plan),
                    'sources': self.sources, 'runtime': _host_runtime(),
                    'source_capture': SOURCE_CAPTURE_POLICY.record()}
                require(self.read_authenticated('config.json') == encoded(actual_config),
                        'Live owner differs from original complete configuration')
        freeze = self.retained_freeze() if self.has_retained('intent.json') else self.admission.before_intent(self.observation_registration)
        self.admission.check_current(self.observation_registration, freeze)

    def _retain(self, name: str, raw: bytes) -> None:
        self._owner()
        require(self.journal is not None, 'Inspection journal unavailable')
        assert self.journal is not None
        self.journal.retain(name, raw)
        self.journal.checkpoint()

    def checkpoint(self) -> chain.PrefixCommitment:
        self._owner()
        require(self.journal is not None, 'Inspection journal unavailable')
        assert self.journal is not None
        return self.journal.checkpoint()

    def read_authenticated(self, name: str) -> bytes:
        self._owner()
        require(self.journal is not None, 'Inspection journal unavailable')
        assert self.journal is not None
        return self.journal.read(name)

    def authenticated_position(self, name: str) -> int:
        self._owner()
        require(self.journal is not None, 'Inspection journal unavailable')
        assert self.journal is not None
        return self.journal._chain.position(name)

    def has_retained(self, name: str) -> bool:
        self._owner()
        return self.journal is not None and self.journal.has(name)

    def retained_freeze(self) -> registry.CohortFreeze | None:
        value = decode(self.read_authenticated('intent.json'))
        require(type(value) is dict and set(value) == {'protocol','execution_id','registration','cohort_freeze'}
            and value['protocol'] == INSPECTION_PROTOCOL
            and encoded(value['registration']) == encoded(asdict(self.registration)), 'Original inspection intent differs')
        registry.identifier(value['execution_id'])
        return admission.freeze_from_record(value['cohort_freeze'])

    def begin_review(self) -> dict[str, Any]:
        require(self.mode == 'physical', 'Fixture inspection cannot dispatch or publish')
        self.current()
        require(not self.has_retained('intent.json'), 'Inspection request cannot be retried/reused')
        freeze = self.admission.before_intent(self.observation_registration)
        require(freeze is not None, 'Actual full cohort barrier required for host review')
        assert freeze is not None
        execution_id = 'workflow-inspection-' + uuid.uuid4().hex
        intent = {'protocol': INSPECTION_PROTOCOL, 'execution_id': execution_id,
            'registration': asdict(self.registration), 'cohort_freeze': asdict(freeze)}
        self._retain('intent.json', encoded(intent))
        self.current()  # Post-ack revocation prohibits even publishing the reviewer request.
        request = {'protocol': INSPECTION_PROTOCOL, 'purpose': INSPECTION_PURPOSE,
            'execution_id': execution_id, 'original_definition_purpose': ORIGINAL_DEFINITION_PURPOSE,
            'execution_purpose': self.profile.purpose, 'registration': asdict(self.registration),
            'profile': self.profile.record(), 'source_plan': self.plan.record(),
            'mechanism_review': self.review_authority.provenance(self.plan),
            'cohort_freeze': asdict(freeze), 'intent_sha256': _sha(self.read_authenticated('intent.json')),
            'intent_prefix': asdict(self.checkpoint()), 'duties': list(INSPECTION_DUTIES)}
        self._retain('review-request.json', encoded(request))
        self.current()
        return request

    def _delivery(self, delivery: ProductInspectionDelivery) -> dict[str, Any]:
        require(type(delivery) is ProductInspectionDelivery, 'Exact separately enrolled product delivery required')
        own_roots = (self.root, self.delta_root, self.checkpoint_authority.root)
        delivery_head = delivery.journal.authority
        require(type(delivery_head) is ExternalHead, 'Original delivery external authority changed')
        assert isinstance(delivery_head, ExternalHead)
        other_roots = (delivery.journal.raw_root, delivery.journal.delta_root, delivery_head.root)
        require(all(not a.is_relative_to(b) and not b.is_relative_to(a) for a in own_roots for b in other_roots),
                'Product review originals overlap execution')
        request = decode(self.read_authenticated('review-request.json'))
        intent = decode(self.read_authenticated('intent.json'))
        require(type(request) is dict and request.get('protocol') == INSPECTION_PROTOCOL
            and request.get('purpose') == INSPECTION_PURPOSE and request.get('execution_id') == intent['execution_id']
            and request.get('intent_sha256') == _sha(self.read_authenticated('intent.json'))
            and encoded(request.get('registration')) == encoded(asdict(self.registration))
            and encoded(request.get('profile')) == encoded(self.profile.record())
            and encoded(request.get('source_plan')) == encoded(self.plan.record())
            and encoded(request.get('cohort_freeze')) == encoded(intent['cohort_freeze'])
            and request.get('duties') == list(INSPECTION_DUTIES), 'Original host reviewer request differs')
        original = delivery.authenticate(request)
        self.current()
        return original

    def complete_review(self, delivery: ProductInspectionDelivery) -> dict[str, Any]:
        require(self.mode == 'physical', 'Fixture inspection cannot complete physical evidence')
        self.current()
        require(self.has_retained('review-request.json') and not self.has_retained('terminal.json'),
                'Fresh original request and no previous completion required')
        original = self._delivery(delivery)
        self.product_delivery = delivery
        self._retain('review-enrollment.json', encoded(original['provenance']))
        self.current()
        intent = decode(self.read_authenticated('intent.json'))
        terminal = {'protocol': INSPECTION_PROTOCOL, 'execution_id': intent['execution_id'],
            'case_id': self.profile.case_id, 'intent_sha256': _sha(self.read_authenticated('intent.json')),
            'request_sha256': _sha(self.read_authenticated('review-request.json')),
            'delivery_origin_sha256': digest(original['provenance']), 'status': 'completed',
            'infrastructure': [], 'engine_used': False}
        self._retain('terminal.json', encoded(terminal))
        self.current()
        return terminal

    def attach_original_delivery(self, delivery: ProductInspectionDelivery) -> None:
        self.current()
        require(self.has_retained('terminal.json'), 'Only completed original delivery can be attached read-only')
        original = self._delivery(delivery)
        require(self.read_authenticated('review-enrollment.json') == encoded(original['provenance']),
                'Cold independently enrolled delivery differs')
        self.product_delivery = delivery
        self.read_original()

    def read_original(self) -> dict[str, Any]:
        require(self.mode == 'physical', 'Fixture inspection has no product observation')
        self.current()
        before = self.checkpoint()
        require(self.product_delivery is not None, 'Independent original delivery unavailable')
        assert self.product_delivery is not None
        original = self._delivery(self.product_delivery)
        require(self.read_authenticated('review-enrollment.json') == encoded(original['provenance']),
                'Original product enrollment differs')
        terminal_raw = self.read_authenticated('terminal.json')
        terminal = decode(terminal_raw)
        intent = decode(self.read_authenticated('intent.json'))
        require(terminal['protocol'] == INSPECTION_PROTOCOL and terminal['execution_id'] == intent['execution_id']
            and terminal['intent_sha256'] == _sha(self.read_authenticated('intent.json'))
            and terminal['request_sha256'] == _sha(self.read_authenticated('review-request.json'))
            and terminal['delivery_origin_sha256'] == digest(original['provenance'])
            and terminal['status'] == 'completed' and terminal['engine_used'] is False,
            'Original host terminal differs from authenticated originals')
        require(self.authenticated_position('intent.json') < self.authenticated_position('review-request.json')
            < self.authenticated_position('review-enrollment.json') < self.authenticated_position('terminal.json'),
            'Original host request/completion order differs')
        rows = original['report']['decisions']
        freeze = self.retained_freeze()
        require(freeze is not None, 'Original host barrier is missing')
        assert freeze is not None
        statuses = {'approved': 'passed', 'rejected': 'failed', 'unresolved': 'infrastructure_error'}
        result = {'protocol': INSPECTION_PROTOCOL, 'family': INSPECTION_FAMILY, 'case_id': self.profile.case_id,
            'execution_id': terminal['execution_id'], 'registration': asdict(self.registration),
            'diagnostics': rows, 'outcomes': [{'case_id': case, 'status': statuses[row['decision']]}
                for case, row in zip(self.profile.ordered_case_ids, rows, strict=True)],
            'original_purpose': ORIGINAL_DEFINITION_PURPOSE, 'execution_purpose': self.profile.purpose,
            'terminal_sha256': _sha(terminal_raw), 'config_sha256': _sha(self.read_authenticated('config.json')),
            'journal_context_sha256': before.context_sha256, 'review_origin': original['provenance'],
            'cohort_freeze': asdict(freeze), 'engine_used': False,
            'mechanics': {'status': 'passed', 'cleanup_required': False, 'cleanup_verified': True,
                'unavailable': [row['id'] for row in rows if row['decision'] == 'unresolved']}}
        self.current()
        require(self.checkpoint() == before, 'Inspection changed during original reconstruction')
        return result

    def retain_verifier(self, raw: bytes) -> None:
        self.current()
        require(raw == encoded(self.read_original()), 'Verifier must exactly reconstruct original host evidence')
        if self.has_retained(VERIFIER_FILE):
            require(self.read_authenticated(VERIFIER_FILE) == raw, 'Original inspection verifier differs')
        else:
            self._retain(VERIFIER_FILE, raw)
        self.current()

    def close(self) -> None:
        if self.closed:
            return
        self._owner()
        try:
            if self.journal is not None:
                self.journal.close()
        finally:
            self.closed = True

    def __enter__(self) -> WorkflowInspectionExecution:
        self._owner()
        return self

    def __exit__(self, *_args: Any) -> None:
        self.close()
