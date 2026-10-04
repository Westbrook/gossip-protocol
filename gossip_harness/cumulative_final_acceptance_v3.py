"""Closed final V3 composition with real prerequisite and promotion originals.

The public V2/V5 study is unchanged. This prospective authority adds separate
source-defined qualification and original final integration review readers; none
can manufacture semantic scope, candidate observations, or the six-source stop.
"""
from __future__ import annotations

from dataclasses import asdict
import hashlib
from pathlib import Path
from typing import Any

from . import candidate_scope_consumer_v1 as consumer
from . import cumulative_final_acceptance_v1 as shared
from . import cumulative_final_acceptance_v2 as previous
from . import cumulative_prerequisite_review_v1 as review
from . import cumulative_prerequisite_qualification_v1 as qualification
from . import cumulative_control_qualification_v1 as control
from . import cumulative_source_promotion_v1 as promotion
from . import cumulative_scope_authority_v3 as scope_authority
from . import project_acceptance_compiler_v1 as compiler
from . import project_acceptance_registry_v1 as registry
from .cumulative_study_controller_v2 import StudyPlan
from .gitstore import GitStore
from types import ModuleType
from . import candidate_m2_product_execution_v1 as m2
from . import cumulative_m2_observation_recipe_v1 as m2_factory
from . import cumulative_observation_recipe_factory_v2 as storage_factory
from .candidate_checkpoint_head_v1 import ExternalHead

PROTOCOL = 'cumulative-final-acceptance-v3'
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
require = consumer.require


def implementation_sources() -> dict[str, str]:
    return shared._canonical_sources((previous.implementation_sources(), qualification.implementation_sources(),
        control.implementation_sources(), promotion.implementation_sources(), scope_authority.implementation_sources(),
        m2.evaluator_sources(), shared.workflow.definition_sources(),
        {'gossip_harness/cumulative_final_acceptance_v3.py': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
         'gossip_harness/cumulative_m2_observation_recipe_v1.py': hashlib.sha256(Path(m2_factory.__file__).read_bytes()).hexdigest(),
         'gossip_harness/cumulative_observation_recipe_factory_v2.py': hashlib.sha256(Path(storage_factory.__file__).read_bytes()).hexdigest()}))


terminal_implementation_sources = previous.terminal_implementation_sources


def control_records(definition: control.ControlDefinition, *, gate_id: str, suite_id: str,
                    physical_slot: str) -> tuple[compiler.SuiteDefinition, compiler.ExecutionGate, consumer.QualificationSpec]:
    """Register the actual prepared five-revision producer before its probe run."""
    require(type(definition) is control.ControlDefinition and definition.purpose == 'harness_qualification',
            'Actual harness-purpose CONTROL definition required')
    require(definition.source_revision == definition.recipe.base
        and definition.ordered_case_ids == control.CASE_IDS
        and definition.facet_selectors == control.FACET_SELECTORS,
        'Exact original five-revision CONTROL definition required')
    for value in (gate_id, suite_id, physical_slot): registry.identifier(value)
    suite = compiler.SuiteDefinition(suite_id, definition.ordered_case_ids, review.digest(definition.ordered_case_ids),
        definition.definition_sha256, definition.purpose, definition.purpose, compiler.M1_SHA256,
        review.digest(control.implementation_sources()), False, False, ('harness-control',))
    execution = compiler.ExecutionGate(gate_id, physical_slot, suite_id, 'prerequisite', ('M1-GATE-CONTROL',),
        review.digest(definition.runtime), definition.environment_sha256, definition.limits_sha256,
        definition.seed_sha256, control.PROTOCOL)
    spec = consumer.QualificationSpec(gate_id, definition.source_revision.source_sha256,
        definition.definition_sha256, definition.facet_selectors, definition.recipe)
    return suite, execution, spec


class FinalAcceptanceV3(previous.FinalAcceptanceV2):
    """Exact known version; capabilities are concrete original owners only."""
    prerequisite_owner: qualification.PrerequisiteQualification | None = None
    promotion_owner: promotion.SourcePromotion | None = None
    control_owners: tuple[tuple[control.ControlQualification, review.OriginalReviewAuthority], ...] = ()
    original_authorities_record: dict[str, Any] | None = None

    def _current(self) -> None:
        super()._current()
        if self.original_authorities_record is not None:
            require(self.records.read('final.original-authorities') == self.original_authorities_record
                and self.prerequisite_owner is not None and self.promotion_owner is not None,
                'Original authority binding is unavailable or changed')
            assert self.prerequisite_owner is not None and self.promotion_owner is not None
            require(asdict(self.prerequisite_owner.source) == self.original_authorities_record['harness_source']
                and self.prerequisite_owner.reviews.expected.context_sha256 == self.original_authorities_record['admission_review_context']
                and self.promotion_owner.reviews.expected.context_sha256 == self.original_authorities_record['promotion_review_context']
                and [{'context':owner.expected.context_sha256,'config':owner.config,
                    'review_context':reviews.expected.context_sha256} for owner,reviews in self.control_owners]
                    == self.original_authorities_record['controls'], 'Original authority context/configuration changed')
        else:
            require(self.prerequisite_owner is None and self.promotion_owner is None and not self.control_owners,
                    'Unpublished original authority capability')

    def _storage_factory(self) -> ModuleType:
        return storage_factory

    def _protected_roots(self) -> list[Path]:
        paths = super()._protected_roots()
        if self.prerequisite_owner is not None:
            paths.append(self.prerequisite_owner.store.path.resolve())
            reviews = [self.prerequisite_owner.reviews]
            if self.promotion_owner is not None: reviews.append(self.promotion_owner.reviews)
            for owner,reviewer in self.control_owners:
                require(type(owner.chain.authority) is ExternalHead, 'Original CONTROL head capability changed')
                assert isinstance(owner.chain.authority, ExternalHead)
                paths.extend((owner.root,owner.chain.raw_root,owner.chain.delta_root,owner.chain.authority.root))
                reviews.append(reviewer)
            for reviewer in reviews:
                require(type(reviewer.chain.authority) is ExternalHead, 'Original review head capability changed')
                assert isinstance(reviewer.chain.authority, ExternalHead)
                paths.extend((reviewer.chain.raw_root,reviewer.chain.delta_root,reviewer.chain.authority.root))
        return list(dict.fromkeys(paths))

    @shared.normalize_authority
    def bind_originals(self, *, store: GitStore, admission_reviews: review.OriginalReviewAuthority,
                      controls: tuple[tuple[control.ControlQualification, review.OriginalReviewAuthority], ...],
                      promotion_reviews: review.OriginalReviewAuthority) -> None:
        self._current_originals()
        require(type(store) is GitStore and type(admission_reviews) is review.OriginalReviewAuthority
            and type(promotion_reviews) is review.OriginalReviewAuthority,
            'Exact harness Git store and protected review capabilities required')
        require(not self.enrollments and self.prerequisite_owner is None and self.promotion_owner is None
            and self.records.read('final.original-authorities') is None,
                'Original prerequisite capabilities must be bound before any candidate dispatch')
        require(type(controls) is tuple and all(type(row) is tuple and len(row) == 2
            and type(row[0]) is control.ControlQualification and type(row[1]) is review.OriginalReviewAuthority
            for row in controls), 'Exact original CONTROL owners and review capabilities required')
        require(len({row[0].config['product_lineages_sha256'] for row in controls}) == len(controls),
                'Ambiguous original CONTROL lineage')
        chains = [self.chain,self.study_chain,self.scope_owner.chain,admission_reviews.chain,promotion_reviews.chain]
        for owner,reviewer in controls: chains.extend((owner.chain,reviewer.chain))
        chains = list(dict.fromkeys(chains))
        roots = [path for chain in chains for path in (chain.raw_root,chain.delta_root,chain.authority.root)]
        roots += [store.path.resolve()] + [owner.root for owner,_ in controls]
        require(all(not a.is_relative_to(b) and not b.is_relative_to(a)
            for i,a in enumerate(roots) for b in roots[i+1:]),
            'Original authority, harness and control roots must be pairwise disjoint')
        prerequisite_owner = qualification.PrerequisiteQualification(self, store=store, reviews=admission_reviews)
        promotion_owner = promotion.SourcePromotion(self, reviews=promotion_reviews)
        for owner, reviews in controls:
            require(owner.chain not in (self.chain, self.study_chain, self.scope_owner.chain, reviews.chain),
                    'Original CONTROL needs its own protected journal')
            owner._current(); reviews._current()
        retained = self._put('final.original-authorities', {'protocol': PROTOCOL,
            'harness_source': asdict(prerequisite_owner.source),
            'admission_review_context': admission_reviews.expected.context_sha256,
            'promotion_review_context': promotion_reviews.expected.context_sha256,
            'controls': [{'context': owner.expected.context_sha256, 'config': owner.config,
                'review_context': reviews.expected.context_sha256} for owner,reviews in controls]})
        # No capability is usable until the complete binding is acknowledged.
        self.original_authorities_record = retained
        self.prerequisite_owner, self.promotion_owner, self.control_owners = prerequisite_owner, promotion_owner, controls
        self._current()

    def _request(self, request: consumer.QualificationRequest) -> None:
        self._current_originals()
        require(type(request) is consumer.QualificationRequest, 'Exact registered qualification request required')
        matches = []
        for submission in self.submissions.values():
            registered = self.scope_snapshot.registration(submission.subject)
            if registered != request.registration: continue
            design = compiler.compile_design(submission.catalog.inventory, submission.declaration, submission.subject,
                scope_plan=submission.scope, expected_scope_sha256=registered.scope_sha256)
            if design.registry is not None and not design.blockers:
                matches += [item for item in consumer.qualification_requests(registered, design, submission.declaration)
                            if item == request]
        require(len(matches) == 1, 'Qualification request is not one complete registered original')

    def _control_original(self, request: consumer.QualificationRequest) -> tuple[control.ControlQualification, control.ControlOriginals]:
        self._request(request)
        require(request.gate.logical_gate_ids == ('M1-GATE-CONTROL',), 'Exact CONTROL request required')
        matches = [(owner,reviews) for owner,reviews in self.control_owners
                   if owner.config['product_lineages_sha256'] == request.product_lineages_sha256]
        if not matches: raise consumer.AuthorityUnavailable('Exact original CONTROL lineage unavailable')
        require(len(matches) == 1, 'Ambiguous original CONTROL authority')
        owner, reviews = matches[0]
        definition = owner.definition()
        suite, gate, specification = control_records(definition, gate_id=request.gate.id,
            suite_id=request.suite.id, physical_slot=request.gate.physical_slot)
        require((suite,gate,specification) == (request.suite,request.gate,request.specification),
                'Original CONTROL contract differs from independent registration')
        return owner, owner.verify_current(reviews)

    def _control_join(self, request: consumer.QualificationRequest) -> tuple[consumer.QualificationEvidence, dict]:
        owner, original = self._control_original(request)
        first_key = 'final.control-first-use.' + owner.expected.context_sha256
        first = self.records.read(first_key)
        if first is None: raise consumer.AuthorityUnavailable('Original CONTROL has not been admitted to this final phase')
        require(set(first) == {'protocol','request_sha256','control_context','product_lineages_sha256','original_purpose'}
            and first['protocol'] == PROTOCOL and first['control_context'] == owner.expected.context_sha256
            and first['product_lineages_sha256'] == request.product_lineages_sha256
            and first['original_purpose'] == original.purpose, 'Original CONTROL first-admission identity differs')
        registry.sha256(first['request_sha256'])
        mode = 'physical' if first['request_sha256'] == request.sha256 else 'reused'
        body = {'protocol':PROTOCOL,'purpose':'request-bound-control-original-verifier',
            'request':asdict(request),'control_checkpoint':asdict(owner.expected),
            'control_originals':shared._material(original),'mode':mode,
            'first_original_admission':first,'reuse_policy':'exact-successful-harness-qualification-only',
            'candidate_observation_reuse':False}
        name = 'final.control-join.' + request.sha256 + '.' + review.digest(body)
        # Its receipt is the acknowledged request-specific join, never a pasted
        # request SHA interpreted as another execution of the original probes.
        receipt = self.records.read(name)
        if receipt is None: raise consumer.AuthorityUnavailable('Request-bound original CONTROL verifier missing')
        require(review.digest(receipt) == review.digest(body), 'Original CONTROL join changed')
        pin = hashlib.sha256(self.chain.read(self.records.name(name))).hexdigest()
        evidence = consumer.QualificationEvidence(request.sha256, original.source_revision.source_sha256,
            'control-' + owner.expected.context_sha256[:24], original.receipt_sha256, pin,
            original.purpose, original.terminal_status, original.outcomes, original.facets, mode,
            pin if mode == 'reused' else None)
        return evidence, body

    @shared.normalize_authority
    def publish_control(self, request: consumer.QualificationRequest) -> consumer.QualificationEvidence:
        """Fresh original readback/admission; never reruns a CONTROL probe."""
        owner, original = self._control_original(request)
        key = 'final.control-first-use.' + owner.expected.context_sha256
        first = self.records.read(key)
        if first is None:
            first = self._put(key, {'protocol':PROTOCOL,'request_sha256':request.sha256,
                'control_context':owner.expected.context_sha256,'product_lineages_sha256':request.product_lineages_sha256,
                'original_purpose':original.purpose})
        mode = 'physical' if first['request_sha256'] == request.sha256 else 'reused'
        body = {'protocol':PROTOCOL,'purpose':'request-bound-control-original-verifier',
            'request':asdict(request),'control_checkpoint':asdict(owner.expected),
            'control_originals':shared._material(original),'mode':mode,
            'first_original_admission':first,'reuse_policy':'exact-successful-harness-qualification-only',
            'candidate_observation_reuse':False}
        self._put('final.control-join.' + request.sha256 + '.' + review.digest(body),body)
        return self.qualification(request)

    @shared.normalize_authority
    def qualification(self, request: consumer.QualificationRequest) -> consumer.QualificationEvidence:
        self._request(request)
        if request.gate.logical_gate_ids != ('M1-GATE-CONTROL',):
            if self.prerequisite_owner is None: raise consumer.AuthorityUnavailable('Original prerequisite owner unavailable')
            return self.prerequisite_owner.qualification(request)
        evidence, _ = self._control_join(request)
        if evidence.mode == 'reused' and (evidence.terminal_status != 'completed'
                or any(row.status != 'passed' for row in (*evidence.outcomes,*evidence.facets))):
            raise consumer.AuthorityUnavailable('Failed or infrastructure CONTROL originals cannot authorize reuse')
        self._request(request)
        return evidence

    @shared.normalize_authority
    def open_snapshot(self) -> consumer.AuthoritySnapshot:
        self._current_originals()
        require(all(row.phase in ('complete','unavailable') for row in self.enrollments.values()),
                'Cannot grade a changing physical observation')
        return _Snapshot(self)

    def _dispatch_prerequisites(self, spec: shared.ObservationSpec) -> None:
        shared.projection.validate_spec(self.plan, spec)
        shared.workflow.validate_spec(self.plan, spec)
        registration = spec.observation_registration()
        self._registered_gate(registration)
        submission = self.submissions[registration.gate.binding.subject.trajectory_id]
        registered = self.scope_snapshot.registration(submission.subject)
        design = compiler.compile_design(submission.catalog.inventory, submission.declaration, submission.subject,
            scope_plan=submission.scope, expected_scope_sha256=registered.scope_sha256)
        for request in consumer.qualification_requests(registered, design, submission.declaration):
            evidence = self.qualification(request)
            require(not consumer._qualification_issues(request,evidence),
                    'Original prerequisite did not qualify this candidate dispatch')
        self._put('final.prerequisites.' + registry.fingerprint(registration.gate), {'protocol': PROTOCOL,
            'gate': asdict(registration.gate), 'qualification_requests': [request.sha256 for request in
                consumer.qualification_requests(registered, design, submission.declaration)]})

    @shared.normalize_authority
    def dispatch(self, spec: shared.ObservationSpec) -> registry.Observation:
        require(spec.kind != 'workflow_inspection', 'Host inspection requires explicit begin/complete delivery')
        self._dispatch_prerequisites(spec)
        return super().dispatch(spec)

    @shared.normalize_authority
    def begin_workflow_inspection(self, spec: shared.ObservationSpec) -> dict[str, Any]:
        require(type(spec) is shared.ObservationSpec and spec.kind == 'workflow_inspection',
                'Exact prospective workflow host inspection specification required')
        self._dispatch_prerequisites(spec)
        return self._dispatch_original(spec)

    @shared.normalize_authority
    def complete_workflow_inspection(self, gate: registry.Gate, delivery: Any) -> registry.Observation:
        key = 'final.observation.' + registry.fingerprint(gate)
        row = self.enrollments.get(key)
        require(row is not None and row.spec is not None and row.registration.gate == gate,
                'Original host inspection admission required')
        assert row is not None and row.spec is not None
        self._dispatch_prerequisites(row.spec)  # Reauthenticate original qualifiers after independent delivery.
        return self._complete_workflow_inspection(gate, delivery)

    def authority_diagnostics(self) -> list[dict[str, Any]]:
        rows = []
        for submission in self.submissions.values():
            registered = self.scope_snapshot.registration(submission.subject)
            if registered is None: continue
            design = compiler.compile_design(submission.catalog.inventory, submission.declaration,submission.subject,
                scope_plan=submission.scope,expected_scope_sha256=registered.scope_sha256)
            if design.registry is None or design.blockers: continue
            for request in consumer.qualification_requests(registered,design,submission.declaration):
                try:
                    evidence = (self._control_join(request)[0] if request.gate.logical_gate_ids == ('M1-GATE-CONTROL',)
                        else self.qualification(request))
                except consumer.AuthorityUnavailable: continue
                rows.append({'trajectory':submission.subject.trajectory_id,'gate_id':request.gate.id,
                    'original_diagnostic':asdict(evidence),'candidate_product_judgment':False,
                    'reuse_authorized':evidence.mode == 'reused' and evidence.terminal_status == 'completed'
                        and all(row.status == 'passed' for row in (*evidence.outcomes,*evidence.facets))})
        return rows

    def missing_producers(self) -> list[str]:
        # Installed implementations do not imply available independent originals.
        return []


class _Snapshot(shared._Snapshot):
    def __init__(self, owner: FinalAcceptanceV3):
        super().__init__(owner)
        self.original_owner = owner
        self.authorities = (owner.prerequisite_owner, owner.promotion_owner, owner.control_owners)
        self.original_heads = self._heads()
        self._checkpoint = review.digest({'shared':self._checkpoint, 'protocol':PROTOCOL,
            'heads':self.original_heads})

    def _heads(self) -> dict:
        owner = self.original_owner
        result: dict[str, Any] = {}
        if owner.prerequisite_owner is not None:
            result['prerequisite_reviews'] = asdict(owner.prerequisite_owner.reviews.expected)
        if owner.promotion_owner is not None:
            result['promotion_reviews'] = asdict(owner.promotion_owner.reviews.expected)
        result['controls'] = [[asdict(control.expected),asdict(reviews.expected)] for control,reviews in owner.control_owners]
        return result

    @shared.normalize_authority
    def check_current(self, checkpoint: str) -> None:
        super().check_current(checkpoint)
        owner = self.original_owner
        require(self.authorities == (owner.prerequisite_owner,owner.promotion_owner,owner.control_owners)
            and self.original_heads == self._heads(), 'Original authority or independently expected head changed')
        if owner.prerequisite_owner is not None: owner.prerequisite_owner._current()
        if owner.promotion_owner is not None: owner.promotion_owner._current()
        for control,reviews in owner.control_owners:
            control._current(); reviews._current()

    @shared.normalize_authority
    def qualification(self, request: consumer.QualificationRequest) -> consumer.QualificationEvidence:
        self.check_current(self.checkpoint)
        evidence = self.original_owner.qualification(request)
        self.check_current(self.checkpoint)
        return evidence

    @shared.normalize_authority
    def promotion(self, subject: registry.Subject) -> registry.Promotion | None:
        self.check_current(self.checkpoint)
        owner = self.original_owner.promotion_owner
        if owner is None: raise consumer.AuthorityUnavailable('Original source promotion owner unavailable')
        result = owner.promotion(subject)
        self.check_current(self.checkpoint)
        return result
