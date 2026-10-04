"""Cold, read-only reconstruction of the concrete FinalAcceptanceV3 originals.

Every external head and enrollment is an independently pinned capsule input.
No stage/register/dispatch/publish method is called. The exact physical readers
reopen one completed owner at a time; fixture observations remain ineligible.
Opening proof locks performs fsync and must not race an active owner.
"""
from __future__ import annotations

from contextlib import ExitStack
from dataclasses import asdict
from pathlib import Path
from typing import Any

from . import candidate_checkpoint_chain_v1 as checkpoint
from . import candidate_http_journal_v3 as stable
from .candidate_checkpoint_head_v1 import ExternalHead
from . import candidate_observation_admission_v1 as admission
from . import candidate_scope_consumer_v1 as consumer
from . import cumulative_final_acceptance_v1 as shared
from . import cumulative_final_acceptance_v3 as final
from . import cumulative_scope_authority_v3 as scope
from . import cumulative_scope_authority_v1 as scope_base
from . import cumulative_scope_source_v3 as source
from . import cumulative_prerequisite_review_v1 as review
from . import cumulative_prerequisite_qualification_v1 as qualification
from . import cumulative_control_qualification_v1 as control
from . import cumulative_source_promotion_v1 as promotion
from . import cumulative_rehearsal_codec_v1 as codec
from . import project_acceptance_registry_v1 as registry
from . import project_acceptance_compiler_v1 as compiler
from .cumulative_study_controller_v2 import StudyPlan, plain, digest
from .cumulative_study_controller_v1 import Records
from .gitstore import GitStore
from .peer_financial_terminal_v1 import require, sha

PROTOCOL = 'cumulative-final-originals-v1'
MAX_OBSERVATIONS = 6 * 4096


def closed(value: Any, names: str) -> dict:
    require(type(value) is dict and set(value) == set(names.split()), 'Closed final proof fields differ')
    return value


def path(value: Any) -> Path:
    require(type(value) is str, 'Proof path must be text')
    result = Path(value)
    require(result.is_absolute() and str(result.resolve()) == value, 'Canonical proof path required')
    return result


class ProofPool:
    """Own original control journals on this thread, releasing independently."""
    def __init__(self, records: dict, stack: ExitStack):
        require(type(records) is dict and 1 <= len(records) <= MAX_OBSERVATIONS + 64,
                'Bounded protected proof roster required')
        self.records, self.stack = records, stack
        self.opened: dict[str, checkpoint.CheckpointChain] = {}

    def open(self, key: str) -> checkpoint.CheckpointChain:
        require(type(key) is str and key in self.records, 'Unknown protected proof identity')
        if key in self.opened:
            return self.opened[key]
        value = closed(self.records[key], 'raw delta head context limits expected')
        roots = tuple(path(value[name]) for name in ('raw', 'delta', 'head'))
        expected = checkpoint.PrefixCommitment(**closed(value['expected'],
            'context_sha256 sequence head_sha256 raw_file_count raw_bytes external_bytes'))
        limits = checkpoint.Limits(**value['limits'])
        authority = ExternalHead.reopen(roots[2], journal_roots=(roots[0], roots[1]), expected=expected)
        self.stack.callback(authority.close)
        chain = checkpoint.CheckpointChain.reopen(roots[0], roots[1], context=value['context'],
            authority=authority, expected=expected, limits=limits)
        self.stack.callback(chain.close)
        self.opened[key] = chain
        return chain

    def current(self) -> None:
        for key, chain in self.opened.items():
            chain.validate_boundary(expected=checkpoint.PrefixCommitment(**self.records[key]['expected']))


def _restore_scope(pool: ProofPool, value: dict, repository: Path) -> tuple[Any, tuple[Any, ...]]:
    closed(value, 'chain enrollments submissions')
    journal = pool.open(value['chain'])
    enrollments = codec.read(value['enrollments'])
    require(type(enrollments) is tuple and all(type(row) is scope_base.ReviewEnrollment for row in enrollments),
            'Exact independently enrolled whole-review originals required')
    require(type(value['submissions']) is list and len(value['submissions']) == 6,
            'All six typed complete scope submissions required')
    submissions = tuple(codec.read(ref) for ref in value['submissions'])
    require(all(type(row) is scope.ScopeSubmission for row in submissions), 'Exact scope V3 submissions required')
    owner = scope.ScopeRegistrationController(repository, journal, journal.commitment,
                                               reviewer_enrollments=enrollments)
    for submission in submissions:
        request = owner._check_submission(submission)
        raw = source.encoded(request)
        request_name = 'scope-request-' + source.sha(raw) + '.json'
        require(journal.read(request_name) == raw, 'Whole original scope request changed')
        design = compiler.compile_design(submission.catalog.inventory, submission.declaration, submission.subject,
            scope_plan=submission.scope, expected_scope_sha256=submission.scope.sha256)
        require(design.registry is not None and not design.blockers, 'Complete executable semantic scope unavailable')
        selectors = {(row.gate.gate_id, item.case_id, item.observation_pointer)
            for row in submission.slices for item in row.selectors
            if item.disposition == 'normative' and item.evidence_kind == 'semantic'}
        products = {row.id for row in submission.declaration.gates if row.role == 'product'}
        require(all((edge.gate_id, edge.case_id, edge.assertion_selector) in selectors
            for edge in submission.declaration.edges if edge.gate_id in products),
            'Complete scope contains an unimplemented semantic selector')
        assert design.registry is not None
        require(all(next((gate for gate in design.registry.gates if gate.gate_id == item.gate.gate_id), None)
                    == item.gate for item in submission.slices), 'Compiled original gate changed')
        staged = scope_base.StagedDesign(request_name, source.sha(raw), journal.position(request_name), design, ())
        name = 'scope-registration-' + registry.fingerprint(submission.subject) + '.json'
        original = journal.read(name)
        material = scope_base._record(original)
        registered = consumer.RegisteredAcceptance(submission.catalog.inventory.sha256, submission.scope.sha256,
            compiler.declaration_fingerprint(submission.declaration), registry.design_fingerprint(design.registry),
            submission.subject.execution_contract_sha256, submission.qualification_specs)
        expected = {'protocol': scope.PROTOCOL, 'request_name': request_name, 'request_sha256': source.sha(raw),
            'report_name': material['report_name'], 'report_sha256': source.sha(journal.read(material['report_name'])),
            'subject': asdict(submission.subject), 'registered': asdict(registered)}
        require(source.encoded(expected) == original, 'Original registration is not the complete derived design')
        consumer.qualification_requests(registered, design, submission.declaration)
        owner.authenticate_review(staged, report_name=material['report_name'])
        enrollment = next(row for row in enrollments if row.report_name == material['report_name'])
        owner._registration_order(material['report_name'], enrollment.provenance_name, name)
        owner._installed[registry.fingerprint(submission.subject)] = (submission, name, original, staged)
    return owner, submissions


def _review(pool: ProofPool, value: dict) -> review.OriginalReviewAuthority:
    closed(value, 'chain enrollments')
    chain = pool.open(value['chain'])
    return review.OriginalReviewAuthority(chain, chain.commitment, enrollments=codec.read(value['enrollments']))


def _prospective_sources(plan: StudyPlan) -> None:
    required={**final.implementation_sources(),**final.terminal_implementation_sources()}
    require(all(plan.source_pins.get(name) == pin for name,pin in required.items()),
            'Full actual final-reader/authority source was not prospectively pinned')
    admission.verify_loaded_sources(required)


def _prerequisite_prefix(owner: final.FinalAcceptanceV3, gate: registry.Gate, admission_position: int) -> None:
    """Authenticate the exact acknowledged prerequisite joins before dispatch."""
    submission=owner.submissions[gate.binding.subject.trajectory_id]
    registered=owner.scope_snapshot.registration(submission.subject)
    require(registered is not None,'Original complete scope registration missing')
    assert registered is not None
    design=compiler.compile_design(submission.catalog.inventory,submission.declaration,submission.subject,
        scope_plan=submission.scope,expected_scope_sha256=registered.scope_sha256)
    require(design.registry is not None and not design.blockers,'Original complete prerequisite design missing')
    requests=consumer.qualification_requests(registered,design,submission.declaration)
    key='final.prerequisites.'+registry.fingerprint(gate)
    require(owner.records.read(key) == plain({'protocol':final.PROTOCOL,'gate':asdict(gate),
        'qualification_requests':[request.sha256 for request in requests]}),
        'Original predispatch prerequisite record differs')
    bound=owner.chain.position(Records.name('final.original-authorities'))
    frontier=owner.chain.position(Records.name(key))
    require(bound < frontier < admission_position, 'Original capability/prerequisite admission chronology differs')
    for request in requests:
        evidence=owner.qualification(request)
        require(not consumer._qualification_issues(request,evidence),'Original prerequisite cannot authorize dispatch')
        if request.gate.logical_gate_ids == ('M1-GATE-CONTROL',):
            _,body=owner._control_join(request)
            name='final.control-join.'+request.sha256+'.'+review.digest(body)
        else:
            require(owner.prerequisite_owner is not None,'Original prerequisite owner missing')
            assert owner.prerequisite_owner is not None
            _,body=owner.prerequisite_owner._evidence(request)
            name=owner.prerequisite_owner._key(request)+'.verifier.'+qualification.digest(body)
        position=owner.chain.position(Records.name(name))
        require(position < frontier and (request.gate.logical_gate_ids != ('M1-GATE-CONTROL',) or bound < position),
                'Original qualified verifier was published after candidate admission')


def _restore_owner(packet: dict, pool: ProofPool, plan: StudyPlan) -> final.FinalAcceptanceV3:
    _prospective_sources(plan)
    scope_owner, submissions = _restore_scope(pool, packet['scope'], path(packet['repository']))
    require(tuple(row.subject.trajectory_id for row in submissions) ==
            tuple(row.trajectory for row in plan.roster.children), 'Ordered six scope subjects differ')
    owner = object.__new__(final.FinalAcceptanceV3)
    owner.plan, owner.repository, owner.ledger_identity = plan, path(packet['repository']), packet['ledger_identity']
    owner.study_chain, owner.chain = pool.open(packet['study']), pool.open(packet['final'])
    owner.study_expected, owner.expected = owner.study_chain.commitment, owner.chain.commitment
    owner.records = Records(owner.chain)
    owner.scope_owner, owner.scope_snapshot = scope_owner, scope_owner.open_snapshot()
    owner.submissions = {row.subject.trajectory_id: row for row in submissions}
    owner.sources = final.implementation_sources()
    owner.closed, owner.dispatch_halt, owner.enrollments = False, None, {}
    owner.original_authorities_record = None
    owner.prerequisite_owner, owner.promotion_owner, owner.control_owners = None, None, ()
    owner.originals = owner._read_terminal()
    require(owner.originals.freeze_eligible and all(row.outcome == 'completed' for row in owner.originals.slots),
            'Complete six successful original terminal closures required')
    owner.subjects = tuple(registry.Subject(plan.cohort.cohort_id, row.trajectory, 'M4', plan.sha256,
        compiler.PRODUCT_V2_SHA256, row.final_source['source_sha256']) for row in owner.originals.slots)
    original_freeze = owner.records.read('final.freeze')
    require(original_freeze is not None, 'Original six-source final freeze missing')
    assert original_freeze is not None
    owner.freeze = admission.freeze_from_record(original_freeze['freeze'])
    require(owner.freeze is not None and owner.freeze.subjects == owner.subjects,
            'Original final freeze substituted the complete subject census')
    contract = owner.records.read('final.contract')
    require(contract == plain({'protocol': final.PROTOCOL, 'study_sha256': plan.sha256,
        'study_checkpoint': asdict(owner.study_expected), 'scope_checkpoint': owner.scope_snapshot.checkpoint,
        'ledger_identity': owner.ledger_identity, 'sources': owner.sources,
        'scope_subjects': [asdict(row.subject) for row in submissions]}), 'Original final contract differs')
    require(plan.runtime['final_acceptance_roots'] == {key: packet['proofs'][packet['final']][key]
        for key in ('raw','delta','head')}, 'Final journal roots changed')
    verifier = {'protocol': final.PROTOCOL, 'purpose': 'independent-terminal-originals-verifier',
        'study_checkpoint': asdict(owner.study_expected), 'originals': shared._material(owner.originals),
        'subjects': [asdict(subject) for subject in owner.subjects], 'sources': owner.sources,
        'acceptance_authority': False}
    require(owner.records.read('final.terminal-verifier') == plain(verifier), 'Original terminal verifier differs')
    require(owner.originals.barrier_name is not None and owner.originals.barrier_sha256 is not None,
            'Exact original terminal barrier missing')
    expected_freeze = registry.CohortFreeze(owner.subjects, owner.originals.barrier_sha256,
        sha(owner.chain.read(Records.name('final.terminal-verifier'))), True)
    require(owner.freeze == expected_freeze and original_freeze == plain({
        'protocol': final.PROTOCOL, 'freeze': asdict(expected_freeze),
        'barrier_name': owner.originals.barrier_name, 'barrier_position': owner.originals.barrier_position,
        'original_context_sha256': owner.study_expected.context_sha256,
        'verifier_name': Records.name('final.terminal-verifier')}), 'Original barrier/freeze provenance differs')
    require(owner.chain.position(Records.name('final.contract')) <
        owner.chain.position(Records.name('final.terminal-verifier')) <
        owner.chain.position(Records.name('final.freeze')), 'Original final preparation chronology differs')
    owner._current_originals()
    admission_reviews, promotion_reviews = _review(pool, packet['admission_reviews']), _review(pool, packet['promotion_reviews'])
    controls = []
    for row in packet['controls']:
        closed(row, 'chain root reviews')
        chain = pool.open(row['chain'])
        require(chain.has('control-config.json'), 'Original control config absent; do not construct a new fixture')
        config = review._read(chain.read('control-config.json'))
        actual = control.ControlQualification(chain, chain.commitment, root=path(row['root']),
            product_lineages_sha256=config['product_lineages_sha256'], reviewer_ids=tuple(config['reviewer_ids']),
            purpose=config['purpose'])
        controls.append((actual, _review(pool, row['reviews'])))
    prerequisite = qualification.PrerequisiteQualification(owner, store=GitStore(path(packet['harness_repository'])),
                                                           reviews=admission_reviews)
    promoted = promotion.SourcePromotion(owner, reviews=promotion_reviews)
    require(len({item.config['product_lineages_sha256'] for item,_ in controls}) == len(controls),
            'Ambiguous original CONTROL lineage')
    chains=[owner.chain,owner.study_chain,owner.scope_owner.chain,admission_reviews.chain,promotion_reviews.chain]
    for item,reviews in controls: chains.extend((item.chain,reviews.chain))
    roots=[root for chain in dict.fromkeys(chains) for root in (chain.raw_root,chain.delta_root,chain.authority.root)]
    roots += [prerequisite.store.path.resolve()] + [item.root for item,_ in controls]
    require(all(not a.is_relative_to(b) and not b.is_relative_to(a)
        for index,a in enumerate(roots) for b in roots[index+1:]),
        'Original authority, harness and CONTROL roots overlap')
    require(all(item.chain not in (owner.chain,owner.study_chain,owner.scope_owner.chain,reviews.chain)
        for item,reviews in controls), 'Original CONTROL journal is not separately protected')
    owner.prerequisite_owner, owner.promotion_owner, owner.control_owners = prerequisite, promoted, tuple(controls)
    owner.original_authorities_record = owner.records.read('final.original-authorities')
    require(owner.original_authorities_record is not None, 'Original prerequisite capability binding absent')
    owner._current_originals()
    require(owner.records.read('final.dispatch-halt') is None, 'Original final dispatch halted')
    return owner


class _ColdAuthority(consumer.EvidenceAuthority):
    def __init__(self, owner: final.FinalAcceptanceV3, observations: dict[str, Any], pool: ProofPool):
        self.owner, self.observations, self.pool = owner, observations, pool

    def open_snapshot(self) -> consumer.AuthoritySnapshot:
        return _ColdSnapshot(self)


class _ColdSnapshot(consumer.AuthoritySnapshot):
    def __init__(self, authority: _ColdAuthority):
        self.authority = authority
        self._checkpoint = digest({'protocol': PROTOCOL, 'final': asdict(authority.owner.expected),
            'proofs': authority.pool.records, 'observations': authority.observations})

    @property
    def checkpoint(self) -> str:
        return self._checkpoint

    def check_current(self, checkpoint_value: str) -> None:
        require(checkpoint_value == self._checkpoint, 'Cold original snapshot changed')
        self.authority.owner._current_originals()
        self.authority.pool.current()

    def registration(self, subject: registry.Subject) -> consumer.RegisteredAcceptance | None:
        self.check_current(self.checkpoint)
        return self.authority.owner.scope_snapshot.registration(subject)

    def qualification(self, request: consumer.QualificationRequest) -> consumer.QualificationEvidence:
        self.check_current(self.checkpoint)
        return self.authority.owner.qualification(request)

    def freeze(self) -> registry.CohortFreeze | None:
        self.check_current(self.checkpoint)
        return self.authority.owner.freeze

    def observation(self, gate: registry.Gate, freeze: registry.CohortFreeze | None) -> registry.Observation | None:
        self.check_current(self.checkpoint)
        require(freeze == self.authority.owner.freeze, 'Consumer requested a different complete freeze')
        key = 'final.observation.' + registry.fingerprint(gate)
        value = self.authority.observations.get(key)
        if value is None:
            return None
        return _observation(self.authority.owner, key, value, self.authority.pool, gate,
                            None if gate.binding.purpose == 'public_release' else freeze)

    def promotion(self, subject: registry.Subject) -> registry.Promotion | None:
        self.check_current(self.checkpoint)
        require(self.authority.owner.promotion_owner is not None, 'Original final integration review unavailable')
        assert self.authority.owner.promotion_owner is not None
        return self.authority.owner.promotion_owner.promotion(subject)


def config_prefix(owner: Any) -> checkpoint.PrefixCommitment:
    """Reconstruct the original config-only prefix from the authenticated chain.

    Uses the pinned chain wire format. Whole-boundary validation before and
    after these two bounded reads authenticates their current ancestry; no
    caller-supplied historical prefix or private in-memory map is trusted.
    """
    current = owner.checkpoint()
    config = owner.read_authenticated('config.json')
    genesis = stable.read(owner.delta_root/'genesis.json',max_bytes=65536)
    delta = stable.read(owner.delta_root/'delta-00000001.json',max_bytes=4096)
    require(sha(checkpoint._GENESIS_DOMAIN + genesis) == current.context_sha256,
            'Original execution genesis differs')
    expected_delta = {'protocol':checkpoint.PROTOCOL,'kind':'raw-file',
        'context_sha256':current.context_sha256,'sequence':1,'previous_head_sha256':current.context_sha256,
        'name':'config.json','bytes':len(config),'sha256':sha(config)}
    require(delta == checkpoint._encoded(expected_delta) and owner.checkpoint() == current,
            'Original config is not the first committed execution record')
    return checkpoint.PrefixCommitment(current.context_sha256,1,sha(checkpoint._DELTA_DOMAIN+delta),
        1,len(config),len(genesis)+len(delta))


def _observation(owner: final.FinalAcceptanceV3, key: str, value: dict, pool: ProofPool,
                 gate: registry.Gate, freeze: registry.CohortFreeze | None) -> registry.Observation:
    """Authenticate completed original bytes without executing or publishing."""
    closed(value, 'inputs proof layout_review')
    inputs = codec.read(value['inputs'])
    closed(inputs, 'kind root delta_root cleanup_root store registration policy recipe profile cumulative_profile endpoint layout_plan')
    admitted = owner.records.read(key + '.admission')
    verified = owner.records.read(key + '.verified')
    require(admitted is not None and verified is not None, 'Complete original physical observation missing')
    assert admitted is not None and verified is not None
    proof = closed(value['proof'], 'raw delta head expected')
    roots = tuple(path(proof[name]) for name in ('raw','delta','head'))
    expected = checkpoint.PrefixCommitment(**proof['expected'])
    require(inputs['root'] == roots[0] and inputs['delta_root'] == roots[1]
        and verified['post_checkpoint'] == asdict(expected), 'Physical final checkpoint/root substitution')
    with ExitStack() as stack:
        head = ExternalHead.reopen(roots[2], journal_roots=(roots[0], roots[1]), expected=expected)
        stack.callback(head.close)
        layout = None
        if inputs['kind'] in ('storage','m2'):
            from . import candidate_storage_review_authority_v1 as storage_review
            from . import candidate_m2_review_authority_v1 as m2_review
            layout_spec = closed(value['layout_review'], 'chain enrollment')
            # Layout journals may be unique per case. Do not retain thousands
            # of their advisory descriptors for the whole audit lifetime.
            chain = (pool.opened[layout_spec['chain']] if layout_spec['chain'] in pool.opened
                else ProofPool(pool.records,stack).open(layout_spec['chain']))
            module: Any = storage_review if inputs['kind'] == 'storage' else m2_review
            cls: Any = module.StorageReviewAuthority if inputs['kind'] == 'storage' else module.M2ReviewAuthority
            layout = cls(chain, chain.commitment, codec.read(layout_spec['enrollment']))
        else:
            require(value['layout_review'] is None, 'Foreign layout authority')
        spec = shared.ObservationSpec(**{**inputs, 'store': GitStore(inputs['store']),
            'checkpoint_authority': head, 'layout_authority': layout})
        registration = spec.observation_registration()
        assert owner.freeze is not None
        require(registration.gate == gate and admitted['registration'] == plain(asdict(registration))
            and admitted['freeze'] == plain(asdict(owner.freeze))
            and (freeze is None if gate.binding.purpose == 'public_release' else freeze == owner.freeze),
            'Purpose/source/complete frozen cohort differs')
        provenance = owner._registered_gate(registration)
        require(admitted['scope'] == plain(asdict(provenance)) and admitted['kind'] == spec.kind
            and tuple(admitted[name] for name in ('root','delta_root','head_root')) == tuple(map(str,roots))
            and admitted['cleanup_root'] == str(spec.cleanup_root), 'Original cross-journal admission differs')
        slot = next(row for row in owner.originals.slots if row.trajectory == gate.binding.subject.trajectory_id)
        require(str(spec.store.path.resolve()) == slot.final_source['repository'], 'Original candidate repository changed')
        protected = owner._protected_roots()
        if layout is not None:
            protected.extend((layout.journal.raw_root,layout.journal.delta_root,layout.journal.authority.root))
        candidate_roots = (*roots,spec.cleanup_root)
        require(all(item.is_absolute() and item.resolve() == item for item in candidate_roots)
            and all(not a.is_relative_to(b) and not b.is_relative_to(a) for a in candidate_roots for b in protected),
            'Original physical paths overlap protected proof roots')
        require(admitted['protocol'] == final.PROTOCOL and admitted['study_checkpoint'] == asdict(owner.study_expected),
            'Original physical admission contract differs')
        _prerequisite_prefix(owner,gate,owner.chain.position(Records.name(key+'.admission')))
        row = shared._Enrollment(registration, provenance, admitted, phase='complete')
        owner.enrollments[key] = row
        issued = admission.ObservationAdmission(registration, verify_registration=lambda:owner._callback(key),
            verify_cohort=owner._verified_freeze)
        common = dict(checkpoint_authority=head, delta_root=spec.delta_root, cleanup_root=spec.cleanup_root,
            endpoint=spec.endpoint, mode='physical', expected_checkpoint=expected)
        actual: Any
        bridge: Any
        if spec.kind == 'cli':
            actual = shared.cli.CandidateClientExecution(spec.root,spec.store,spec.registration,spec.policy,
                admission_authority=issued,cumulative_profile=spec.cumulative_profile,**common)
        elif spec.kind in ('http','product'):
            module = shared.http if spec.kind == 'http' else shared.product
            actual = module.CandidateHttpExecution(spec.root,spec.store,spec.registration,spec.recipe,spec.policy,
                profile=spec.profile,observation_admission=issued,**common)
        else:
            from . import candidate_m2_product_execution_v1 as m2
            cls = shared.storage.CandidateStorageExecution if spec.kind == 'storage' else m2.CandidateM2Execution
            actual = cls(spec.root,spec.store,spec.registration,spec.policy,value=spec.profile,
                plan=spec.layout_plan,review_authority=spec.layout_authority,admission_authority=issued,**common)
        stack.callback(actual.close)
        # The HTTP readers can publish a missing verifier during their normal
        # producer path. Cold verification requires its acknowledged original
        # before calling them, so that branch is unreachable here.
        verifier_name = ('semantic-verifier.json' if spec.kind in ('http','product') else
            shared.cli_source.VERIFIER_FILE if spec.kind == 'cli' else
            shared.storage_source.VERIFIER_FILE if spec.kind == 'storage' else 'm2-product-verifier.json')
        require(actual.journal is not None and actual.journal.has(verifier_name), 'Original physical verifier missing')
        before = config_prefix(actual)
        bound = owner.records.read(key + '.bound')
        dispatch = owner.records.read(key + '.dispatch')
        require(bound == {**admitted, 'owner_checkpoint': asdict(before),
            'config_sha256': sha(actual.read_authenticated('config.json'))}
            and dispatch == {'protocol': final.PROTOCOL, 'registration_sha256': digest(asdict(registration)),
                'owner_checkpoint': asdict(before)}, 'Original config-only binding or dispatch differs')
        if spec.kind == 'cli':
            bridge = shared.cli_source.ClientObservationSource(actual,expected)
        elif spec.kind in ('http','product'):
            reader: Any = shared.http_source if spec.kind == 'http' else shared.product_source
            bridge = reader.HttpObservationSource(actual,expected,receipt_path=spec.root/verifier_name)
        elif spec.kind == 'storage':
            bridge = shared.storage_source.StorageObservationSource(actual,expected)
        else:
            from . import candidate_m2_product_observation_v1 as m2_reader
            bridge = m2_reader.M2ObservationSource(actual,expected)
        observed = bridge.observation(gate, freeze)
        require(plain(asdict(observed)) == verified['observation']
            and sha(actual.read_authenticated('intent.json')) == verified['intent_sha256']
            and sha(actual.read_authenticated('terminal.json')) == verified['terminal_sha256']
            and actual.checkpoint() == expected, 'Original physical observation or terminal changed')
        history = owner.records.read(key + '.history')
        require(history is not None and history == verified['history'] and history['cleanup_verified'] is True
            and not history['infrastructure'] and not history['missing_step_ids'], 'Original physical cleanup/completeness missing')
        positions = [owner.chain.position(Records.name(key+suffix)) for suffix in
                     ('.admission','.bound','.dispatch','.history','.verified')]
        require(all(a < b for a,b in zip(positions,positions[1:])), 'Original dispatch chronology differs')
        require(owner.chain.position(Records.name('final.freeze')) < positions[0]
            and positions[-1] < owner.chain.position(Records.name('final.assessment')),
            'Product observation did not follow complete freeze and precede assessment')
        row.observation, row.post_checkpoint, row.history = observed, expected, history
        owner._current_originals()
        return observed


def audit_final_originals(packet: dict, *, plan: StudyPlan) -> dict:
    closed(packet, 'protocol repository ledger_identity proofs study final scope admission_reviews promotion_reviews controls harness_repository observations')
    require(packet['protocol'] == PROTOCOL and type(plan) is StudyPlan
        and plan.runtime['final_acceptance_financial_mode'] == 'fixture', 'Exact fixture FinalV3 proof required')
    require(type(packet['observations']) is dict and 1 <= len(packet['observations']) <= MAX_OBSERVATIONS,
            'Bounded complete physical observation census required')
    with ExitStack() as stack:
        pool = ProofPool(packet['proofs'],stack)
        owner = _restore_owner(packet,pool,plan)
        authority = _ColdAuthority(owner,packet['observations'],pool)
        retained = owner.records.read('final.assessment')
        require(retained is not None and retained['protocol'] == final.PROTOCOL
            and retained['planned_trajectories'] == 6 and retained['dispatch_halt'] is None,
            'Original complete final assessment absent')
        assert retained is not None
        expected_keys: set[str] = set()
        results = []
        for index,subject in enumerate(owner.subjects):
            submission = owner.submissions[subject.trajectory_id]
            design = compiler.compile_design(submission.catalog.inventory,submission.declaration,subject,
                scope_plan=submission.scope,expected_scope_sha256=submission.scope.sha256)
            require(design.registry is not None and not design.blockers, 'Full registered scope unavailable')
            assert design.registry is not None
            expected_keys.update('final.observation.'+registry.fingerprint(gate) for gate in design.registry.gates)
            result = consumer.CandidateScopeConsumer(authority).assess(submission.catalog.inventory,
                submission.declaration,submission.scope,subject)
            require(result.accepted_against_registry, 'Original physical/prerequisite/promotion evidence does not satisfy full scope')
            original = retained['slots'][index]
            terminal = owner.originals.slots[index]
            require(original['process_counts'] == plain(terminal.process_counts)
                and original['evidence_errors'] == plain(terminal.evidence_errors)
                and original['financial_origin'] == plain(terminal.financial_origin)
                and original['accepted_against_registry'] is True, 'Original terminal slot interpretation differs')
            require(original['trajectory'] == subject.trajectory_id and original['subject'] == asdict(subject)
                and original['terminal_outcome'] == 'completed' and not original['unavailable_observation_slots'],
                'Original final slot/source/completion differs')
            # Cold verification has a new independently expected current head;
            # the old opaque snapshot token is not transplanted as fresh authority.
            old = dict(original['assessment']); current = plain(asdict(result))
            registry.sha256(old.pop('authority_checkpoint'))
            current.pop('authority_checkpoint')
            require(old == current, 'Original final assessment differs from current raw evidence')
            rows = [(key,row) for key,row in owner.enrollments.items()
                if row.registration.gate.binding.subject == subject]
            rows.sort(key=lambda item:owner.chain.position(Records.name(item[0]+'.admission')))
            require(original['physical_diagnostics'] == plain([asdict(row.observation) for _,row in rows
                if row.observation is not None]), 'Original physical diagnostic census differs')
            results.append({'subject':asdict(subject),'assessment':asdict(result)})
        require(set(packet['observations']) == expected_keys, 'Full registered physical census differs')
        require(retained['study_sha256'] == plan.sha256 and retained['financial_mode'] == 'fixture'
            and retained['live_financial_origin'] is False and retained['freeze_available'] is True
            and retained['missing_producers'] == owner.missing_producers()
            and retained['authority_diagnostics'] == plain(owner.authority_diagnostics()),
            'Original assessment financial or authority interpretation differs')
        require(retained['accepted'] is True and retained['completed_and_accepted'] is True,
                'Original final phase did not complete accepted')
        # Re-read every physical journal after the full consumer pass. Each cold
        # reader holds its own head while grading; no N-squared retained FD pool.
        for subject in owner.subjects:
            submission = owner.submissions[subject.trajectory_id]
            design = compiler.compile_design(submission.catalog.inventory,submission.declaration,subject,
                scope_plan=submission.scope,expected_scope_sha256=submission.scope.sha256)
            assert design.registry is not None
            for gate in design.registry.gates:
                key = 'final.observation.'+registry.fingerprint(gate)
                _observation(owner,key,packet['observations'][key],pool,gate,
                    None if gate.binding.purpose == 'public_release' else owner.freeze)
        pool.current(); owner._current_originals()
        assert owner.freeze is not None
        return {'protocol':PROTOCOL,'study_sha256':plan.sha256,'final_checkpoint':asdict(owner.expected),
            'assessment_name':Records.name('final.assessment'),
            'assessment_sha256':sha(owner.chain.read(Records.name('final.assessment'))),
            'freeze':asdict(owner.freeze),'subjects':results,'physical_observations':len(expected_keys),
            'interpretation':'Matching fixture harness rehearsal only; candidate acceptance is never transferred to live sources.'}
