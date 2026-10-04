"""Controller-owned durable registration of a fully reviewed acceptance design.

Protected host enrollment of an independently delivered reviewer original is the
provenance root. Checkpoint hashes authenticate bytes/currentness, not authorship
or semantic adequacy. Candidate submission cannot enroll a report. No production
complete review, ScopePlan, prerequisite execution, freeze or promotion is shipped.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from . import candidate_scope_consumer_v1 as consumer
from . import project_acceptance_compiler_v1 as compiler
from . import project_acceptance_registry_v1 as registry
from . import cumulative_scope_source_v1 as source
from .candidate_checkpoint_chain_v1 import CheckpointChain, PrefixCommitment, ChainUnknown
from .candidate_checkpoint_head_v1 import ExternalHead

PROTOCOL = 'cumulative-scope-authority-v1'
LOADED_SOURCE_SHA256 = source.sha(Path(__file__).read_bytes())
REVIEW_PURPOSE = 'independent_complete_scope_semantics'


def implementation_sources() -> dict[str, str]:
    """Prospective code identity; fresh-worker/source-freeze is still host-owned.

    Re-check loaded definitions and available import pins. This does not claim
    arbitrary interpreter or mutable-global integrity for legacy dependencies.
    """
    from . import candidate_observation_admission_v1 as admission
    names = ("cumulative_scope_source_v1", "cumulative_scope_authority_v1",
             "candidate_scope_consumer_v1", "project_acceptance_compiler_v1",
             "project_acceptance_registry_v1", "candidate_checkpoint_chain_v1",
             "candidate_checkpoint_head_v1", "candidate_http_journal_v3",
             "candidate_observation_admission_v1")
    root = Path(__file__).resolve().parent
    pins = {"gossip_harness/" + name + ".py": source.sha((root / (name + ".py")).read_bytes()) for name in names}
    admission.verify_loaded_sources(pins)
    return pins


class RegistrationMissing(consumer.AuthorityUnavailable):
    """Required real decomposition, reviewer original or executable surface absent."""


@dataclass(frozen=True, slots=True)
class ReviewEnrollment:
    """PROTECTED HOST configuration, not a candidate-supplied approval record.

    The host establishes reviewer provenance outside this module and installs the
    original artifact identity independently of submission. Fabricating enrollment
    in a fixture exercises the mechanism; it does not establish real review.
    """
    reviewer_id: str
    role: str
    report_name: str
    report_sha256: str
    provenance_name: str
    provenance_sha256: str

    def __post_init__(self) -> None:
        registry.identifier(self.reviewer_id)
        consumer.require(self.role == 'independent_scope_reviewer', 'Wrong reviewer authority scope')
        registry.sha256(self.report_sha256)
        registry.sha256(self.provenance_sha256)


@dataclass(frozen=True, slots=True)
class ScopeSubmission:
    catalog: source.SourceCatalog
    declaration: compiler.Declaration
    scope: compiler.ScopePlan
    subject: registry.Subject
    slices: tuple[source.ExecutableSlice, ...]
    qualification_specs: tuple[consumer.QualificationSpec, ...]

    def request(self) -> dict[str, Any]:
        """Actual source/mapping review payload; no default decisions or approvals."""
        catalog, declaration, scope = self.catalog, self.declaration, self.scope
        consumer.require(type(catalog) is source.SourceCatalog and type(declaration) is compiler.Declaration
            and type(scope) is compiler.ScopePlan and type(self.subject) is registry.Subject,
            'Typed complete review inputs required')
        consumer.require(type(self.slices) is tuple and all(type(item) is source.ExecutableSlice for item in self.slices),
                         'Exact executable catalogs required')
        for item in self.slices:
            source.verify_slice(item)
        # A review request contains all original values, cells, retained gap scope,
        # full declaration/scope and executable selectors. No observed output.
        units = [{**asdict(item), 'value_json': item.value_json.decode()} for item in catalog.units]
        plan = {row.obligation_id: row for row in declaration.obligations}
        applicability = {row.obligation_id: row for row in scope.applicability}
        targets = []
        def target(identifier: str, value: Any, duty: str) -> None:
            targets.append({'id': identifier, 'value': value, 'sha256': source.sha(source.encoded(value)), 'duty': duty})
        for unit in units:
            identifier = unit['obligation']['id']
            target('obligation:' + identifier, {'source': unit,
                'plan': None if identifier not in plan else asdict(plan[identifier]),
                'applicability': None if identifier not in applicability else asdict(applicability[identifier]),
                'edges': [asdict(row) for row in declaration.edges if row.obligation_id == identifier]},
                'Inspect full source unit and every subfacet, owner, kind/lane, concrete assertion selector, exclusions and named interactions. Missing implementation cannot be approved as covered.')
        for rule in catalog.inventory.qualification_rules:
            target('authority:' + rule.id, asdict(rule), 'Verify exact prerequisite/acceptance boundary is retained and separately qualified; no product credit.')
        for requirement, gap, kind, text in catalog.inventory.planning_notes:
            target('gap:' + gap, {'requirement': requirement, 'gap': gap, 'kind': kind, 'scope': text,
                'dispositions': [asdict(row) for row in scope.planning_dispositions if row.gap_id == gap]},
                'Inspect original owner/kind/scope and concrete disposition; this note is neither an observed failure nor automatic waiver.')
        for gate in catalog.inventory.logical_gates:
            target('purpose:' + gate.id, {'gate': asdict(gate), 'assignments': [asdict(row) for row in declaration.purposes if row.logical_gate_id == gate.id]},
                'Preserve original purpose; prospective fresh independence is not historical relabeling or held-out evidence.')
        for suite in declaration.suites:
            target('suite:' + suite.id, {'suite': asdict(suite),
                'gates': [asdict(row) for row in declaration.gates if row.suite_id == suite.id],
                'source_catalogs': [asdict(row) for row in self.slices if any(g.id == row.gate.gate_id and g.suite_id == suite.id for g in declaration.gates)]},
                'Check actual executable definition, ordered cases/selectors, evaluator lineage and lane suitability. Reference-only or unsupported semantics remain missing.')
        target('compatibility', {'declaration': [asdict(row) for row in declaration.compatibility],
                'scope': [asdict(row) for row in scope.compatibility],
                'suite_compatibility': [asdict(row) for row in scope.suite_compatibility]},
                'Verify exact predecessor/successor authorities and all inherited M4 meanings; no blanket milestone relabel.')
        target('cohort', asdict(declaration.cohort), 'Verify all four milestones and six S4-G/S16-G/O16-G healthy/compound-recovery trajectories, exact roles, matched faults, barrier and later held-out duties.')
        if declaration.capacity_profile != registry.LEGACY_CAPACITY_PROFILE:
            target('capacity-contract', registry.capacity_manifest(declaration.capacity_profile),
                'Review explicit aggregate history capacity; retain full source inventory, per-history bounds, distinct executions, purpose/freeze barriers and new source-bound rehearsal.')
        consumer.require(len({row['id'] for row in targets}) == len(targets), 'Duplicate review target')
        return {'protocol': PROTOCOL, 'kind': 'complete-semantic-review-request',
            'inventory_sha256': catalog.inventory.sha256, 'catalog_sha256': catalog.sha256,
            'declaration_sha256': compiler.declaration_fingerprint(declaration), 'scope_sha256': scope.sha256,
            'execution_contract_sha256': self.subject.execution_contract_sha256,
            'registry_design_subject': {key: value for key, value in asdict(self.subject).items() if key != 'source_sha256'},
            'declaration': compiler.declaration_record(declaration), 'scope': asdict(scope),
            'implementation_sources': implementation_sources(),
            'source_pins': dict(catalog.source_pins), 'qualification_specs': [asdict(row) for row in self.qualification_specs],
            'targets': targets, 'ownership_review_scope': '57 owners only; no applicability or selector approval',
            'candidate_observations': 'none', 'acceptance_authority': False}


@dataclass(frozen=True, slots=True)
class StagedDesign:
    request_name: str
    request_sha256: str
    request_position: int
    design: compiler.CompilationResult
    missing_executable_edges: tuple[str, ...]


def _position(chain: CheckpointChain, name: str) -> int:
    # Public readonly membership position is required; never reconstruct a prefix
    # by subtracting serialized file maps or trust a report's claimed sequence.
    return chain.position(name)


def _record(raw: bytes) -> dict[str, Any]:
    from .candidate_http_journal_v3 import decode
    value = decode(raw)
    consumer.require(type(value) is dict and source.encoded(value) == raw, 'Canonical original object required')
    return value


@dataclass(frozen=True, slots=True)
class RegistrationProvenance:
    """Authenticated positions in ONE chain; not cross-journal temporal proof."""
    context_sha256: str
    checkpoint: PrefixCommitment
    request_name: str
    request_sha256: str
    request_position: int
    report_name: str
    report_sha256: str
    report_position: int
    delivery_name: str
    delivery_sha256: str
    delivery_position: int
    registration_name: str
    registration_sha256: str
    registration_position: int


class ScopeRegistrationController(consumer.EvidenceAuthority):
    """Install only in trusted controller, outside candidate/role execution.

    Constructor enrollments are protected provenance configuration from actual
    independent delivery. They are not reconstructed from candidate JSON. The
    controller only writes prospective design/registration; it never dispatches.
    """
    def __init__(self, root: Path, chain: CheckpointChain, expected: PrefixCommitment,
                 *, reviewer_enrollments: tuple[ReviewEnrollment, ...] = ()):
        consumer.require(type(chain) is CheckpointChain and type(expected) is PrefixCommitment
                         and type(chain.authority) is ExternalHead,
                         'Exact controller-owned chain and external checkpoint required')
        consumer.require(type(reviewer_enrollments) is tuple and all(type(row) is ReviewEnrollment for row in reviewer_enrollments),
                         'Protected typed reviewer enrollments required')
        consumer.require(len({row.report_name for row in reviewer_enrollments}) == len(reviewer_enrollments), 'Duplicate review enrollment')
        self.root, self.chain, self.expected = Path(root), chain, expected
        self.enrollments = reviewer_enrollments
        self._installed: dict[str, tuple[ScopeSubmission, str, bytes, StagedDesign]] = {}
        chain.validate_boundary(expected=expected)

    def _current(self) -> None:
        consumer.require(type(self.chain.authority) is ExternalHead, 'Durable external host head changed')
        try:
            self.chain.validate_boundary(expected=self.expected)
        except (ChainUnknown, OSError) as error:
            raise consumer.AuthorityUnavailable(str(error)) from error

    def _check_submission(self, submission: ScopeSubmission) -> dict[str, Any]:
        try:
            consumer.require(type(submission) is ScopeSubmission and submission.catalog == source.load_catalog(self.root),
                             'Full original source/ownership catalog differs')
            known = {unit.obligation.id: unit.requirement_ids for unit in submission.catalog.units}
            plans = submission.declaration.obligations
            consumer.require(len({row.obligation_id for row in plans}) == len(plans)
                and all(row.obligation_id in known and row.requirement_ids == known[row.obligation_id] for row in plans),
                'Declaration changed exact source or independently reviewed shared owners')
            return submission.request()
        except OSError as error:
            raise consumer.AuthorityUnavailable(str(error)) from error
        except (ValueError, TypeError, LookupError) as error:
            raise consumer.AuthorityError(str(error)) from error

    def stage(self, submission: ScopeSubmission) -> StagedDesign:
        """Retain real concrete declaration for review; incomplete remains incomplete."""
        self._current()
        request = self._check_submission(submission)
        raw = source.encoded(request)
        name = 'scope-request-' + source.sha(raw) + '.json'
        if self.chain.has(name):
            consumer.require(self.chain.read(name) == raw, 'Existing request differs')
        else:
            self.chain.retain(name, raw)
        self.expected = self.chain.validate_boundary().commitment
        design = compiler.compile_design(submission.catalog.inventory, submission.declaration, submission.subject,
            scope_plan=submission.scope, expected_scope_sha256=submission.scope.sha256)
        selectors = {(row.gate.gate_id, item.case_id, item.observation_pointer)
                     for row in submission.slices for item in row.selectors
                     if item.disposition == 'normative' and item.evidence_kind == 'semantic'}
        product_gates = {row.id for row in submission.declaration.gates if row.role == 'product'}
        missing = tuple(sorted({edge.gate_id + ':' + edge.case_id + ':' + edge.assertion_selector
                    for edge in submission.declaration.edges if edge.gate_id in product_gates
                    and (edge.gate_id, edge.case_id, edge.assertion_selector) not in selectors}))
        return StagedDesign(name, source.sha(raw), _position(self.chain, name), design, missing)

    def authenticate_review(self, staged: StagedDesign, *, report_name: str) -> dict[str, Any]:
        """Read the enrolled original and its delivery provenance, never a passed flag."""
        self._current()
        matches = [row for row in self.enrollments if row.report_name == report_name]
        if len(matches) != 1:
            raise RegistrationMissing('No independently installed reviewer-original enrollment')
        enrollment = matches[0]
        raw = self.chain.read(report_name)
        provenance_raw = self.chain.read(enrollment.provenance_name)
        consumer.require(source.sha(raw) == enrollment.report_sha256
            and source.sha(provenance_raw) == enrollment.provenance_sha256, 'Enrolled original bytes differ')
        report, provenance = _record(raw), _record(provenance_raw)
        consumer.require(set(provenance) == {'protocol', 'reviewer_id', 'role', 'report_name', 'report_sha256', 'request_sha256', 'delivery_reference'}
            and provenance['protocol'] == 'scope-independent-review-delivery-v1'
            and provenance['reviewer_id'] == enrollment.reviewer_id and provenance['role'] == enrollment.role
            and provenance['report_name'] == report_name and provenance['report_sha256'] == enrollment.report_sha256
            and provenance['request_sha256'] == staged.request_sha256
            and type(provenance['delivery_reference']) is str and bool(provenance['delivery_reference'].strip()),
            'Original delivery provenance differs from protected host enrollment')
        consumer.require(set(report) == {'protocol', 'purpose', 'reviewer_id', 'reviewed_request_sha256', 'decisions', 'limitations'}
            and report['protocol'] == 'complete-scope-semantic-review-v1' and report['purpose'] == REVIEW_PURPOSE
            and report['reviewer_id'] == enrollment.reviewer_id
            and report['reviewed_request_sha256'] == staged.request_sha256, 'Wrong original review scope/purpose/request')
        request_raw = self.chain.read(staged.request_name)
        consumer.require(source.sha(request_raw) == staged.request_sha256
            and _position(self.chain, staged.request_name) == staged.request_position
            and staged.request_position < _position(self.chain, report_name)
            < _position(self.chain, enrollment.provenance_name), 'Review chronology or original request differs')
        request = _record(request_raw)
        targets = request['targets']
        decisions = report['decisions']
        consumer.require(type(decisions) is list and len(decisions) == len(targets)
            and type(report['limitations']) is list and all(type(row) is str for row in report['limitations']),
            'Incomplete semantic review decision census')
        unresolved = []
        for target, decision in zip(targets, decisions, strict=True):
            consumer.require(type(decision) is dict and set(decision) == {'target_id', 'target_sha256', 'decision', 'rationale', 'inspected_references'}
                and decision['target_id'] == target['id'] and decision['target_sha256'] == target['sha256']
                and decision['decision'] in ('approved', 'rejected', 'unresolved')
                and type(decision['rationale']) is str and bool(decision['rationale'].strip())
                and type(decision['inspected_references']) is list and bool(decision['inspected_references'])
                and all(type(ref) is str and bool(ref.strip()) for ref in decision['inspected_references']),
                'Substituted, vague or reordered review decision')
            if decision['decision'] != 'approved':
                unresolved.append(target['id'] + ':' + decision['decision'])
        self._current()
        if unresolved:
            raise RegistrationMissing('Semantic review remains unresolved/rejected: ' + ','.join(unresolved))
        return report

    def register(self, submission: ScopeSubmission, *, report_name: str) -> consumer.RegisteredAcceptance:
        """Explicit host installation after real full review and complete compilation."""
        staged = self.stage(submission)
        if staged.design.registry is None or staged.design.blockers or staged.missing_executable_edges:
            raise RegistrationMissing('Complete executable decomposition is unavailable: '
                + ','.join(row.code + ':' + row.target for row in staged.design.blockers)
                + ','.join(staged.missing_executable_edges))
        self.authenticate_review(staged, report_name=report_name)
        assert staged.design.registry is not None
        product = staged.design.registry
        # Closed factory's physical gate must equal compiled normalized scope.
        # A semantic review cannot silently change the owner's requirements or roster.
        for item in submission.slices:
            consumer.require(next((row for row in product.gates if row.gate_id == item.gate.gate_id), None) == item.gate,
                             'Compiled gate differs from actual executable original')
        registered = consumer.RegisteredAcceptance(submission.catalog.inventory.sha256, submission.scope.sha256,
            compiler.declaration_fingerprint(submission.declaration), registry.design_fingerprint(product),
            submission.subject.execution_contract_sha256, submission.qualification_specs)
        consumer.qualification_requests(registered, staged.design, submission.declaration)
        material = {'protocol': PROTOCOL, 'request_name': staged.request_name, 'request_sha256': staged.request_sha256,
            'report_name': report_name, 'report_sha256': source.sha(self.chain.read(report_name)),
            'subject': asdict(submission.subject), 'registered': asdict(registered)}
        raw = source.encoded(material)
        name = 'scope-registration-' + registry.fingerprint(submission.subject) + '.json'
        if self.chain.has(name):
            consumer.require(self.chain.read(name) == raw, 'Immutable registration replay differs')
        else:
            self.chain.retain(name, raw)
        self.expected = self.chain.validate_boundary().commitment
        enrollment = next(row for row in self.enrollments if row.report_name == report_name)
        self._registration_order(report_name, enrollment.provenance_name, name)
        self._installed[registry.fingerprint(submission.subject)] = (submission, name, raw, staged)
        return registered

    def _registration_order(self, report_name: str, provenance_name: str, registration_name: str) -> None:
        consumer.require(_position(self.chain, report_name) < _position(self.chain, provenance_name)
                         < _position(self.chain, registration_name), 'Registration predates independently retained delivery')

    def open_snapshot(self) -> consumer.AuthoritySnapshot:
        self._current()
        return ScopeSnapshot(self, self.expected)


class ScopeSnapshot(consumer.AuthoritySnapshot):
    """Registration capability only; absent producers remain explicit prerequisites."""
    def __init__(self, owner: ScopeRegistrationController, expected: PrefixCommitment):
        self.owner, self.expected = owner, expected

    @property
    def checkpoint(self) -> str:
        return source.sha(source.encoded(asdict(self.expected)))

    def check_current(self, checkpoint: str) -> None:
        consumer.require(checkpoint == self.checkpoint and self.owner.expected == self.expected,
                         'Registration snapshot changed')
        self.owner._current()

    def _original(self, subject: registry.Subject) -> tuple[ScopeSubmission, str, bytes, StagedDesign] | None:
        self.check_current(self.checkpoint)
        installed = self.owner._installed.get(registry.fingerprint(subject))
        if installed is None:
            return None
        submission, name, raw, staged = installed
        consumer.require(submission.subject == subject and self.owner.chain.read(name) == raw,
                         'Original independently installed registration differs')
        request = self.owner._check_submission(submission)
        consumer.require(source.encoded(request) == self.owner.chain.read(staged.request_name)
                         and staged.design.registry is not None and not staged.design.blockers
                         and not staged.missing_executable_edges, 'Current complete reviewed request differs')
        material = _record(raw)
        report_name = material['report_name']
        self.owner.authenticate_review(staged, report_name=report_name)
        enrollment = next(row for row in self.owner.enrollments if row.report_name == report_name)
        self.owner._registration_order(report_name, enrollment.provenance_name, name)
        self.check_current(self.checkpoint)
        return installed

    def registration_provenance(self, subject: registry.Subject) -> RegistrationProvenance | None:
        """Re-authenticate originals and expose comparable same-chain chronology.

        Candidate dispatch in another journal needs an original controller-owned
        pre-dispatch link. Sequence integers from different chains are unrelated.
        """
        installed = self._original(subject)
        if installed is None:
            return None
        _, name, raw, staged = installed
        material = _record(raw)
        enrollment = next(row for row in self.owner.enrollments if row.report_name == material['report_name'])
        result = RegistrationProvenance(self.expected.context_sha256, self.expected,
            staged.request_name, staged.request_sha256, _position(self.owner.chain, staged.request_name),
            enrollment.report_name, enrollment.report_sha256, _position(self.owner.chain, enrollment.report_name),
            enrollment.provenance_name, enrollment.provenance_sha256, _position(self.owner.chain, enrollment.provenance_name),
            name, source.sha(raw), _position(self.owner.chain, name))
        self.check_current(self.checkpoint)
        return result

    def registration(self, subject: registry.Subject) -> consumer.RegisteredAcceptance | None:
        installed = self._original(subject)
        if installed is None:
            return None
        submission, _, raw, _ = installed
        value = _record(raw)['registered']
        # Specs remain the original independently installed typed controller input.
        result = consumer.RegisteredAcceptance(value['inventory_sha256'], value['scope_sha256'],
            value['declaration_sha256'], value['registry_design_sha256'], value['execution_contract_sha256'],
            submission.qualification_specs)
        consumer.require(source.encoded(asdict(result)) == source.encoded(value), 'Original qualification specs differ')
        self.check_current(self.checkpoint)
        return result

    def qualification(self, request: consumer.QualificationRequest) -> consumer.QualificationEvidence | None:
        self.check_current(self.checkpoint)
        return None  # Real SCOPE/ADMISSION/CONTROL producer integration is still required.

    def freeze(self) -> registry.CohortFreeze | None:
        self.check_current(self.checkpoint)
        return None  # No count-only or declared stop assertions become actual lifecycle proof.

    def observation(self, gate: registry.Gate, freeze: registry.CohortFreeze | None) -> registry.Observation | None:
        self.check_current(self.checkpoint)
        return None  # Typed existing source bridges belong to the later composite controller.

    def promotion(self, subject: registry.Subject) -> registry.Promotion | None:
        self.check_current(self.checkpoint)
        return None
