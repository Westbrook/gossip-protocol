"""Original finite SCOPE and ADMISSION qualification, separate from product truth.

SCOPE reauthenticates the enrolled whole semantic review. ADMISSION executes
closed guard/parser controls over every exact prospective product registration
and requires an independently delivered review of the actual evaluator and
isolation contract. Its callbacks below are adversarial controls, not substitute
production authorities. No candidate program, provider or Docker effect runs.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, replace
import hashlib
from pathlib import Path
import platform
import sys
from typing import Any

from . import candidate_http_journal_v3 as parser
from . import candidate_observation_admission_v1 as admission
from . import candidate_scope_consumer_v1 as consumer
from . import cumulative_scope_source_v3 as source
from . import cumulative_prerequisite_review_v1 as review
from . import project_acceptance_compiler_v1 as compiler
from . import project_acceptance_registry_v1 as registry
from . import candidate_git_source_batch_v1 as capture
from .gitstore import GitStore
from .cumulative_final_acceptance_v1 import normalize_authority

PROTOCOL = 'cumulative-prerequisite-qualification-v1'
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
require, encoded, digest = consumer.require, review.encoded, review.digest
CASES = {
    'scope': ('complete-registered-design', 'original-whole-semantic-review', 'exact-six-final-source-barrier'),
    'admission': ('complete-prospective-product-lineages', 'exact-registration-guards', 'full-cohort-guards',
                  'bounded-malformed-input-controls', 'independent-evaluator-isolation-review'),
}
PURPOSES = {'scope': 'registry_qualification', 'admission': 'harness_qualification'}
LIMITS: dict[str, Any] = {'max_control_json_bytes': 4096, 'max_control_depth': parser.MAX_DEPTH, 'candidate_effects': 0,
          'source_capture_protocol': capture.PROTOCOL, 'source_capture_deadline': capture.DEADLINE_CONTRACT, 'replay': 'never', 'registration_census': 'every-exact-registered-product-gate'}
GUARDS = ('baseline', 'source', 'commit', 'tree', 'purpose', 'suite', 'ordered-cases', 'evaluator',
          'runtime', 'environment', 'limits', 'seed', 'execution-protocol', 'definition', 'profile',
          'binding', 'repetition', 'registration-unavailable', 'cohort-unavailable', 'cohort-incomplete',
          'cohort-changed', 'cohort-retained-missing')
MALFORMED = (('duplicate', b'{"x":1,"x":2}'), ('nonfinite', b'{"x":NaN}'),
             ('utf8', b'\xff'), ('depth', b'[' * (parser.MAX_DEPTH + 1) + b'0' + b']' * (parser.MAX_DEPTH + 1)),
             ('bytes', b' ' * 4097))


def implementation_sources() -> dict[str, str]:
    names = ('cumulative_finite_mapping_v1.py', 'cumulative_rehearsal_codec_v1.py',
        'cumulative_prerequisite_qualification_v1.py', 'cumulative_final_acceptance_v1.py',
        'cumulative_final_acceptance_v2.py', 'cumulative_final_acceptance_v3.py',
        'cumulative_scope_source_v1.py', 'cumulative_scope_source_v2.py', 'cumulative_scope_authority_v1.py',
        'cumulative_scope_authority_v2.py', 'cumulative_scope_source_v3.py', 'cumulative_scope_authority_v3.py', 'candidate_release_execution_v1.py',
        'candidate_release_execution_v2.py', 'candidate_git_source_batch_v1.py', 'gitstore.py')
    pins = {**review.implementation_sources(), **{'gossip_harness/' + name:
        hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest() for name in names}}
    admission.verify_loaded_sources(pins)
    return pins


def runtime_record() -> dict[str, Any]:
    executable = Path(sys.executable).resolve()
    return {'executable': str(executable), 'executable_sha256': hashlib.sha256(executable.read_bytes()).hexdigest(),
        'python': sys.version, 'platform': platform.platform(), 'execution': 'trusted-host-guards-only',
        'candidate_effects': False}


def harness_files() -> dict[str, str]:
    root = Path(__file__).resolve().parents[1]
    pins = implementation_sources()
    return {**{name: (root / name).read_text() for name in pins},
            'qualification-contract.json': encoded({'protocol': PROTOCOL, 'cases': CASES,
                'purposes': PURPOSES, 'limits': LIMITS, 'guards': GUARDS, 'sources': pins}).decode()}


@normalize_authority
def create_harness(path: Path) -> GitStore:
    """Create a separate immutable source fixture before scope registration.

    This helper only constructs trusted harness source. Callers must retain their
    own prospective construction intent; an existing path is never resumed.
    """
    require(not Path(path).exists(), 'Qualification source path already exists')
    return GitStore.create(path, harness_files())


@normalize_authority
def harness_revision(store: GitStore) -> consumer.Revision:
    require(type(store) is GitStore, 'Exact independent harness Git store required')
    commit = store.head()
    try:
        tree, files = capture.capture_git_source_batch(store.path, commit)
    except capture.CaptureError as error:
        raise consumer.AuthorityUnavailable(str(error)) from error
    require(files == {name: text.encode() for name, text in harness_files().items()},
            'Qualification Git source differs from current complete harness')
    require(store.head() == commit, 'Harness source moved during capture')
    return consumer.Revision(commit, tree, admission.source_sha256(files))


@dataclass(frozen=True, slots=True)
class Definition:
    kind: str
    source: consumer.Revision
    product_lineages_sha256: str
    record: dict[str, Any]
    suite: compiler.SuiteDefinition
    execution: compiler.ExecutionGate
    specification: consumer.QualificationSpec


@normalize_authority
def definition_for(kind: str, revision: consumer.Revision, product_gates: tuple[registry.Gate, ...], *,
                   gate_id: str, suite_id: str, physical_slot: str) -> Definition:
    require(kind in CASES and type(revision) is consumer.Revision and type(product_gates) is tuple
        and bool(product_gates) and all(type(gate) is registry.Gate for gate in product_gates),
        'Closed qualification kind, actual harness revision and full target gates required')
    require(len({gate.gate_id for gate in product_gates}) == len(product_gates), 'Duplicate product lineage')
    for name in (gate_id, suite_id, physical_slot): registry.identifier(name)
    sources, runtime = implementation_sources(), runtime_record()
    lineages = consumer._lineages(product_gates)
    record = {'protocol': PROTOCOL, 'kind': kind, 'source': asdict(revision),
        'product_lineages_sha256': lineages, 'ordered_cases': CASES[kind], 'purpose': PURPOSES[kind],
        'sources': sources, 'runtime': runtime, 'environment': {'ambient_candidate_input': False},
        'limits': LIMITS, 'seed': None, 'selectors': {kind: '/outcomes'},
        'interpretation': 'harness qualification only; independent review remains mandatory'}
    evaluator = digest(sources)
    suite = compiler.SuiteDefinition(suite_id, CASES[kind], digest(CASES[kind]), digest(record),
        PURPOSES[kind], PURPOSES[kind], compiler.M1_SHA256, evaluator, False, False, (compiler.M1_LANES[kind.upper()],))
    targets = tuple(compiler.QualificationBinding(g.gate_id, g.binding.ordered_suite_sha256,
        g.binding.evaluator_sha256, g.binding.runtime_image_sha256, g.binding.environment_sha256,
        g.binding.limits_sha256, g.binding.execution_protocol) for g in product_gates) if kind == 'admission' else ()
    execution = compiler.ExecutionGate(gate_id, physical_slot, suite_id, 'prerequisite',
        ('M1-GATE-' + kind.upper(),), digest(runtime), digest(record['environment']), digest(LIMITS),
        digest(None), PROTOCOL, targets)
    spec = consumer.QualificationSpec(gate_id, revision.source_sha256, suite.definition_sha256,
        ((kind, '/outcomes'),))
    return Definition(kind, revision, lineages, record, suite, execution, spec)


def _status(decision: str) -> str:
    return 'passed' if decision == 'accept' else 'failed' if decision == 'reject' else 'skipped'


def _control(registration: admission.ObservationRegistration, freeze: registry.CohortFreeze,
             current_registration: Any) -> tuple[dict[str, Any], ...]:
    """Actual guards, with explicitly adversarial callbacks on negative paths."""
    def supplied():
        current_registration()
        return registration
    owned = admission.ObservationAdmission(registration, verify_registration=supplied, verify_cohort=lambda: freeze)
    retained = owned.before_intent(registration)
    owned.check_current(registration, retained)
    rows: list[dict[str, Any]] = [{'case': 'baseline', 'expected': 'admitted', 'observed': 'admitted', 'passed': True}]
    subject, gate, binding = registration.gate.binding.subject, registration.gate, registration.gate.binding
    wrong = lambda value: ('0' if value[0] != '0' else '1') + value[1:]
    changed = {
        'source': replace(registration, gate=replace(gate, binding=replace(binding,
            subject=replace(subject, source_sha256=wrong(subject.source_sha256))))),
        'commit': replace(registration, commit_oid=wrong(registration.commit_oid)),
        'tree': replace(registration, tree_oid=wrong(registration.tree_oid)),
        'purpose': replace(registration, gate=replace(gate, binding=replace(binding,
            purpose='public_release' if binding.purpose != 'public_release' else 'independent_acceptance'))),
        'suite': replace(registration, gate=replace(gate, binding=replace(binding,
            ordered_suite_sha256=wrong(binding.ordered_suite_sha256)))),
        'ordered-cases': replace(registration, gate=replace(gate, ordered_case_ids=('control-substituted-case',))),
        **{label:replace(registration, gate=replace(gate,binding=replace(binding, **{field:wrong(getattr(binding,field))})))
            for label,field in (('evaluator','evaluator_sha256'),('runtime','runtime_image_sha256'),
                ('environment','environment_sha256'),('limits','limits_sha256'),('seed','seed_sha256'))},
        'execution-protocol': replace(registration,gate=replace(gate,binding=replace(binding,
            execution_protocol=('q' if binding.execution_protocol[0] != 'q' else 'r') + binding.execution_protocol[1:]))),
        'definition': replace(registration, definition_sha256=wrong(registration.definition_sha256)),
        'profile': replace(registration, profile_sha256=wrong(registration.profile_sha256)),
        'binding': replace(registration, original_binding_sha256=wrong(registration.original_binding_sha256)),
        'repetition': replace(registration, repetition_id=('q' if registration.repetition_id[0] != 'q' else 'r') + registration.repetition_id[1:]),
    }
    for label in GUARDS[1:]:
        if label.startswith('cohort-') and binding.purpose == 'public_release':
            rows.append({'case': label, 'expected': 'not-applicable-public-purpose',
                         'observed': 'not-applicable-public-purpose', 'passed': True})
            continue
        try:
            if label in changed: owned.before_intent(changed[label])
            elif label == 'registration-unavailable':
                admission.ObservationAdmission(registration, verify_registration=lambda: None,
                    verify_cohort=lambda: freeze).before_intent(registration)
            elif label == 'cohort-unavailable':
                admission.ObservationAdmission(registration, verify_registration=supplied,
                    verify_cohort=lambda: None).before_intent(registration)
            elif label == 'cohort-incomplete':
                admission.ObservationAdmission(registration, verify_registration=supplied,
                    verify_cohort=lambda:replace(freeze,subjects=freeze.subjects[:-1])).before_intent(registration)
            elif label == 'cohort-changed':
                sibling = next(row for row in freeze.subjects if row != subject)
                changed_freeze = replace(freeze, subjects=tuple(replace(row,
                    source_sha256=wrong(row.source_sha256)) if row == sibling else row for row in freeze.subjects))
                admission.ObservationAdmission(registration, verify_registration=supplied,
                    verify_cohort=lambda: changed_freeze).check_current(registration, retained)
            else: owned.check_current(registration, None)
        except admission.AdmissionError as error:
            rows.append({'case': label, 'expected': 'AdmissionError', 'observed': type(error).__name__,
                         'detail': str(error), 'passed': True})
        else:
            rows.append({'case': label, 'expected': 'AdmissionError', 'observed': 'admitted', 'passed': False})
    owned.check_current(registration, retained)
    return tuple(rows)


def malformed_controls() -> tuple[dict[str, Any], ...]:
    results = []
    for name, raw in MALFORMED:
        try: parser.decode(raw, max_bytes=LIMITS['max_control_json_bytes'])
        except (ValueError, UnicodeError) as error:
            results.append({'case': name, 'raw_hex': raw.hex(), 'expected': 'rejected',
                'observed': type(error).__name__, 'passed': True})
        else: results.append({'case': name, 'raw_hex': raw.hex(), 'expected': 'rejected', 'observed': 'accepted', 'passed': False})
    return tuple(results)


def validate_guard_facts(facts: tuple[dict[str, Any], ...]) -> None:
    """Validate retained diagnostics without inventing unexecuted siblings."""
    require(bool(facts), 'No original target guards')
    for fact in facts:
        require(tuple(row['case'] for row in fact['guards']) == GUARDS, 'Original guard census changed')
        for row in fact['guards']:
            expected, observed = row['expected'], row['observed']
            passed = observed in ('AdmissionError', 'AdmissionUnavailable') if expected == 'AdmissionError' else observed == expected
            require(type(row['passed']) is bool and row['passed'] == passed,
                    'Guard Boolean disagrees with retained observed outcome')
            require(expected == ('admitted' if row['case'] == 'baseline' else
                'not-applicable-public-purpose' if row['case'].startswith('cohort-') and
                fact['registration']['gate']['binding']['purpose'] == 'public_release' else 'AdmissionError'),
                'Guard expected outcome was relabeled')


def admission_outcomes(facts: tuple[dict[str, Any], ...], malformed: tuple[dict[str, Any], ...]) -> tuple[registry.CaseResult, ...]:
    """Derive aggregate verdicts from the complete original diagnostic census."""
    validate_guard_facts(facts)
    require(tuple(row['case'] for row in malformed) == tuple(name for name,_ in MALFORMED),
            'Malformed control census differs')
    for row,(_,raw) in zip(malformed,MALFORMED,strict=True):
        require(row['raw_hex'] == raw.hex() and row['expected'] == 'rejected'
            and row['observed'] in ('JournalError','accepted')
            and row['passed'] is (row['observed'] == 'JournalError'), 'Malformed raw control/result differs')
    return (registry.CaseResult(CASES['admission'][0], 'passed'),
        registry.CaseResult(CASES['admission'][1], 'passed' if all(row['passed'] for fact in facts for row in fact['guards']) else 'failed'),
        registry.CaseResult(CASES['admission'][2], 'passed' if all(row['passed'] for fact in facts for row in fact['guards'] if row['case'].startswith('cohort-')) else 'failed'),
        registry.CaseResult(CASES['admission'][3], 'passed' if all(row['passed'] for row in malformed) else 'failed'))


class PrerequisiteQualification:
    """One real prospective original producer; reads never append or redispatch."""
    @normalize_authority
    def __init__(self, owner: Any, *, store: GitStore, reviews: review.OriginalReviewAuthority):
        from .cumulative_final_acceptance_v3 import FinalAcceptanceV3
        require(type(owner) is FinalAcceptanceV3 and type(reviews) is review.OriginalReviewAuthority,
            'Exact V3 final owner and enrolled review capability required')
        self.owner, self.store, self.reviews = owner, store, reviews
        self.source = harness_revision(store)
        self.sources = implementation_sources()
        require(reviews.chain not in (owner.chain, owner.study_chain, owner.scope_owner.chain),
                'Independent original review journal required')
        self._current()

    def _current(self) -> None:
        self.owner._current_originals()
        self.reviews._current()
        require(self.owner.plan.runtime.get('prerequisite_qualification_protocol') == PROTOCOL
            and implementation_sources() == self.sources
            and all(self.owner.plan.source_pins.get(name) == pin for name,pin in self.sources.items()),
            'Qualification protocol/full source was not prospectively bound')
        require(harness_revision(self.store) == self.source, 'Original harness source changed')
        if self.owner.freeze is None: raise consumer.AuthorityUnavailable('Complete six-source barrier required')

    def _resolve(self, request: consumer.QualificationRequest) -> tuple[Any, registry.Registry, Definition, Any]:
        self._current()
        require(type(request) is consumer.QualificationRequest, 'Typed original qualification request required')
        matches = []
        for submission in self.owner.submissions.values():
            registered = self.owner.scope_snapshot.registration(submission.subject)
            if registered != request.registration: continue
            compiled = compiler.compile_design(submission.catalog.inventory, submission.declaration,
                submission.subject, scope_plan=submission.scope, expected_scope_sha256=registered.scope_sha256)
            require(compiled.registry is not None and not compiled.blockers, 'Full scope no longer compiles')
            assert compiled.registry is not None
            if request in consumer.qualification_requests(registered, compiled, submission.declaration):
                matches.append((submission, compiled.registry))
        require(len(matches) == 1, 'Request is not one exact complete registered subject')
        submission, product = matches[0]
        require(request.gate.logical_gate_ids in (('M1-GATE-SCOPE',), ('M1-GATE-ADMISSION',)),
                'This producer cannot replace CONTROL or combined qualification')
        kind = 'scope' if request.gate.logical_gate_ids == ('M1-GATE-SCOPE',) else 'admission'
        definition = definition_for(kind, self.source, product.gates, gate_id=request.gate.id,
            suite_id=request.suite.id, physical_slot=request.gate.physical_slot)
        require((request.gate, request.suite, request.specification, request.product_lineages_sha256) ==
            (definition.execution, definition.suite, definition.specification, definition.product_lineages_sha256),
            'Registered qualification differs from actual source-defined guards/purpose/full lineage')
        provenance = self.owner.scope_snapshot.registration_provenance(submission.subject)
        if provenance is None: raise consumer.AuthorityUnavailable('Original complete semantic review unavailable')
        require(submission.subject in self.owner.freeze.subjects, 'Subject outside frozen cohort')
        return submission, product, definition, provenance

    @staticmethod
    def _key(request: consumer.QualificationRequest) -> str:
        return 'final.qualification.' + request.sha256

    def _context(self, request: consumer.QualificationRequest, body: dict) -> dict:
        return {'protocol': PROTOCOL, 'request': asdict(request), 'original_controls': body,
                'limits': LIMITS, 'no_candidate_execution': True,
                'required_judgment': 'actual complete target evaluators, adapters, malformed-input policy and isolation adequacy'}

    @normalize_authority
    def execute_once(self, request: consumer.QualificationRequest, specifications: tuple[Any, ...] = ()) -> dict:
        from .cumulative_final_acceptance_v1 import ObservationSpec, plain
        submission, product, definition, provenance = self._resolve(request)
        key = self._key(request)
        require(self.owner.records.read(key + '.intent') is None, 'Qualification already attempted; no replay')
        require(type(specifications) is tuple and all(type(row) is ObservationSpec for row in specifications),
                'Exact prospective construction inputs required')
        specs = tuple(row for row in specifications if row.observation_registration().gate.binding.subject == submission.subject)
        if definition.kind == 'admission':
            actual = tuple(row.observation_registration().gate for row in specs)
            require(actual == product.gates, 'Admission requires every product gate in exact registered order')
        else: require(not specifications, 'SCOPE does not consume product execution specifications')
        intent = {'protocol': PROTOCOL, 'request': asdict(request), 'definition': asdict(definition),
            'scope_provenance': asdict(provenance), 'freeze': asdict(self.owner.freeze),
            'registrations': [asdict(row.observation_registration()) for row in specs],
            'original_purpose': definition.suite.execution_purpose, 'source': asdict(self.source)}
        self.owner._put(key + '.intent', plain(intent))
        # Intent is durable before executing even bounded, trusted guard code.
        if definition.kind == 'scope':
            body = {'scope_provenance': asdict(provenance), 'registered': asdict(request.registration),
                'inventory': submission.catalog.inventory.sha256, 'scope': submission.scope.sha256,
                'freeze': asdict(self.owner.freeze), 'candidate_effects': 0}
            outcomes = [registry.CaseResult(name, 'passed') for name in CASES['scope']]
        else:
            facts: list[dict[str, Any]] = []
            for spec in specs:
                registration = spec.observation_registration()
                slice_ = source.cli_slice(spec.registration) if spec.kind == 'cli' else (
                    source.m2_slice(spec.registration) if spec.kind == 'm2' else (
                    source.storage_slice(spec.registration) if spec.kind == 'storage' else (
                    source.http_slice(spec.registration, spec.profile, spec.policy, mapping_profile=spec.profile.mapping_profile) if spec.kind == 'http'
                    else source.product_process_slice(spec.registration, spec.profile, spec.policy, mapping_profile=spec.profile.mapping_profile))))
                require(slice_ in submission.slices, 'Actual prospective factory differs from semantic registration')
                provenance_now = self.owner._registered_gate(registration)
                require(provenance_now == provenance, 'Original registration changed during controls')
                guards = _control(registration, self.owner.freeze, self.owner._current)
                facts.append({'registration': asdict(registration), 'slice_sha256': slice_.sha256, 'guards': guards})
                self.owner._put(key + '.target.' + str(len(facts) - 1), plain(facts[-1]))
            malformed = malformed_controls()
            target_refs = [{'name': self.owner.records.name(key + '.target.' + str(i)),
                'sha256': hashlib.sha256(self.owner.chain.read(self.owner.records.name(key + '.target.' + str(i)))).hexdigest(),
                'gate_id': fact['registration']['gate']['gate_id']} for i,fact in enumerate(facts)]
            body = {'targets': target_refs, 'malformed': malformed, 'candidate_effects': 0,
                'interpretation': 'guard mechanics; runtime isolation requires independent original review'}
            outcomes = list(admission_outcomes(tuple(facts),malformed))
        self._resolve(request)
        result = {'protocol': PROTOCOL, 'request_sha256': request.sha256, 'source': asdict(self.source),
            'original_purpose': definition.suite.execution_purpose, 'terminal_status': 'completed',
            'body': plain(body), 'outcomes': [asdict(row) for row in outcomes]}
        self.owner._put(key + '.execution', result)
        self._resolve(request)
        return result

    @normalize_authority
    def stage_review(self, request: consumer.QualificationRequest) -> review.ReviewRequest:
        _, _, definition, _ = self._resolve(request)
        require(definition.kind == 'admission', 'SCOPE uses its separately enrolled full semantic review')
        original = self._execution(request)
        require(original['terminal_status'] == 'completed', 'Partial controls cannot solicit completed admission review')
        return self.reviews.stage(purpose='admission_fixture_review', role='admission', source=self.source,
            context=self._context(request, original))

    def _partial_execution(self, request: consumer.QualificationRequest) -> dict:
        """Read acknowledged partial controls; never resume or create a terminal."""
        submission, product, definition, provenance = self._resolve(request)
        key = self._key(request)
        intent = self.owner.records.read(key + '.intent')
        if intent is None or definition.kind != 'admission':
            raise consumer.AuthorityUnavailable('Original qualification execution missing; never retry')
        require(digest(intent['request']) == digest(asdict(request))
            and digest(intent['definition']) == digest(asdict(definition))
            and digest(intent['scope_provenance']) == digest(asdict(provenance))
            and digest(intent['freeze']) == digest(asdict(self.owner.freeze)), 'Partial qualification intent differs')
        registrations = intent['registrations']
        require(tuple(source.previous.previous._gate(row['gate']) for row in registrations) == product.gates,
                'Partial qualification target census differs')
        facts, refs, missing = [], [], []
        for i,registration in enumerate(registrations):
            slot = key + '.target.' + str(i)
            fact = self.owner.records.read(slot)
            if fact is None:
                missing.append(registration['gate']['gate_id'])
                continue
            require(not missing and fact['registration'] == registration,
                    'Original sequential target prefix has a gap/substitution')
            matching = tuple(row for row in submission.slices if row.gate.gate_id == registration['gate']['gate_id'])
            require(len(matching) == 1 and matching[0].sha256 == fact['slice_sha256'],
                    'Partial original source factory differs')
            source.verify_slice(matching[0])
            name = self.owner.records.name(slot)
            require(self.owner.chain.position(self.owner.records.name(key + '.intent')) < self.owner.chain.position(name),
                    'Partial controls preceded intent')
            validate_guard_facts((fact,))
            facts.append(fact)
            refs.append({'name':name,'sha256':hashlib.sha256(self.owner.chain.read(name)).hexdigest(),
                         'gate_id':registration['gate']['gate_id']})
        failed = any(row['passed'] is False for fact in facts for row in fact['guards'])
        failed_cohort = any(row['passed'] is False for fact in facts for row in fact['guards'] if row['case'].startswith('cohort-'))
        statuses = ('passed', 'failed' if failed else 'infrastructure_error',
                    'failed' if failed_cohort else 'infrastructure_error', 'infrastructure_error')
        return {'protocol':PROTOCOL,'request_sha256':request.sha256,'source':asdict(self.source),
            'original_purpose':definition.suite.execution_purpose,'terminal_status':'infrastructure_error',
            'body':{'derived_from_incomplete_originals':True,'intent_name':self.owner.records.name(key + '.intent'),
                'targets':refs,'unobserved_targets':missing,'missing_original_terminal':True,'candidate_effects':0},
            'outcomes':[asdict(registry.CaseResult(name,status)) for name,status in zip(CASES['admission'][:-1],statuses,strict=True)]}

    def _execution(self, request: consumer.QualificationRequest) -> dict:
        key = self._key(request)
        original = self.owner.records.read(key + '.execution')
        if original is None: return self._partial_execution(request)
        intent = self.owner.records.read(key + '.intent')
        require(intent is not None and digest(intent['request']) == digest(asdict(request)),
                'Qualification intent request changed')
        require(self.owner.chain.position(self.owner.records.name(key + '.intent')) <
                self.owner.chain.position(self.owner.records.name(key + '.execution')),
                'Qualification execution preceded prospective intent')
        require(original['request_sha256'] == request.sha256 and original['source'] == asdict(self.source),
                'Original qualification source/request differs')
        submission, product, definition, provenance = self._resolve(request)
        require(digest(intent['definition']) == digest(asdict(definition))
            and digest(intent['scope_provenance']) == digest(asdict(provenance))
            and digest(intent['freeze']) == digest(asdict(self.owner.freeze)),
            'Original qualification definition/scope/barrier changed')
        if definition.kind == 'scope':
            expected_body = {'scope_provenance': asdict(provenance), 'registered': asdict(request.registration),
                'inventory': submission.catalog.inventory.sha256, 'scope': submission.scope.sha256,
                'freeze': asdict(self.owner.freeze), 'candidate_effects': 0}
            require(digest(original['body']) == digest(expected_body), 'Original full scope evidence differs')
            expected_rows = tuple(registry.CaseResult(name,'passed') for name in CASES['scope'])
        else:
            registrations = intent['registrations']
            require(tuple(source.previous.previous._gate(row['gate']) for row in registrations) == product.gates,
                    'Original admission gate census changed')
            refs = original['body']['targets']
            require(len(refs) == len(registrations), 'Original target records omitted')
            facts = []
            for i,(ref,registration) in enumerate(zip(refs,registrations,strict=True)):
                slot = key + '.target.' + str(i)
                name = self.owner.records.name(slot)
                fact = self.owner.records.read(slot)
                require(fact is not None and fact['registration'] == registration
                    and ref == {'name':name,'sha256':hashlib.sha256(self.owner.chain.read(name)).hexdigest(),
                        'gate_id':registration['gate']['gate_id']}, 'Original per-target admission facts differ')
                require(self.owner.chain.position(self.owner.records.name(key + '.intent')) < self.owner.chain.position(name)
                    < self.owner.chain.position(self.owner.records.name(key + '.execution')),
                    'Target guard chronology differs')
                matching = tuple(row for row in submission.slices if row.gate.gate_id == ref['gate_id'])
                require(len(matching) == 1 and matching[0].sha256 == fact['slice_sha256'],
                        'Original guard source factory changed')
                source.verify_slice(matching[0])
                facts.append(fact)
            expected_rows = admission_outcomes(tuple(facts), tuple(original['body']['malformed']))
        require(original['outcomes'] == [asdict(row) for row in expected_rows],
                'Original qualification outcomes disagree with complete raw diagnostics')
        return original

    def _evidence(self, request: consumer.QualificationRequest) -> tuple[consumer.QualificationEvidence, dict]:
        _, _, definition, provenance = self._resolve(request)
        original = self._execution(request)
        rows = tuple(registry.CaseResult(**row) for row in original['outcomes'])
        expected = CASES[definition.kind] if definition.kind == 'scope' else CASES['admission'][:-1]
        require(tuple(row.case_id for row in rows) == expected and original['original_purpose'] == definition.suite.execution_purpose
            and original['terminal_status'] in ('completed','infrastructure_error'), 'Original control outcome census or purpose differs')
        reviewed = None
        review_missing = None
        if definition.kind == 'admission':
            try:
                if original['terminal_status'] != 'completed':
                    raise consumer.AuthorityUnavailable('Partial original controls cannot qualify evaluator isolation')
                requested = self.reviews.original(purpose='admission_fixture_review', role='admission', source=self.source,
                    context=self._context(request, original))
                reviewed = self.reviews.authenticate(requested)
            except consumer.AuthorityUnavailable as error:
                review_missing = str(error)
            rows += (registry.CaseResult(CASES['admission'][-1], 'skipped' if reviewed is None else _status(reviewed.decision)),)
        known = 'failed' if any(row.status == 'failed' for row in rows) else ('passed' if all(row.status == 'passed' for row in rows) else 'skipped')
        body = {'protocol': PROTOCOL, 'request_sha256': request.sha256, 'original': original,
            'scope_provenance': asdict(provenance), 'review': None if reviewed is None else asdict(reviewed),
            'review_unavailable': review_missing,
            'outcomes': [asdict(row) for row in rows], 'facets': [{'case_id': definition.kind, 'status': known}]}
        key = self._key(request)
        evidence = consumer.QualificationEvidence(request.sha256, self.source.source_sha256,
            'qualification-' + request.sha256[:24], hashlib.sha256(self.owner.chain.read(self.owner.records.name(key + ('.execution' if original['terminal_status'] == 'completed' else '.intent')))).hexdigest(),
            digest(body), definition.suite.execution_purpose, original['terminal_status'], rows,
            (registry.CaseResult(definition.kind, known),))
        return evidence, body

    @normalize_authority
    def verify_and_retain(self, request: consumer.QualificationRequest) -> consumer.QualificationEvidence:
        evidence, body = self._evidence(request)
        self.owner._put(self._key(request) + '.verifier.' + digest(body), body)
        return self.qualification(request)

    @normalize_authority
    def qualification(self, request: consumer.QualificationRequest) -> consumer.QualificationEvidence:
        evidence, body = self._evidence(request)
        saved = self.owner.records.read(self._key(request) + '.verifier.' + digest(body))
        if saved is None: raise consumer.AuthorityUnavailable('Original qualification verifier missing')
        require(digest(saved) == digest(body), 'Qualification originals/review changed after verification')
        self._resolve(request)
        return evidence
