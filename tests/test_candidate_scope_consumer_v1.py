"""Synthetic authority-contract controls, never production scope qualification.

A trusted test double models already authenticated controller originals. It is
explicitly not a receipt authenticator, independent reviewer or study authority.
The separate Git class exercises a real fixture-mode release journal fail closed.
"""
from dataclasses import replace
from pathlib import Path
import tempfile
import unittest

from gossip_harness import candidate_scope_consumer_v1 as s
from gossip_harness import project_acceptance_compiler_v1 as c
from gossip_harness import project_acceptance_registry_v1 as r
from tests.test_project_acceptance_compiler_v1 import inventory, sha, subject, synthetic_declaration, synthetic_scope


def revision(name):
    return s.Revision(sha(name + '-commit')[:40], sha(name + '-tree')[:40], sha(name + '-source'))


def recipe():
    base, left, right, merged, repaired = map(revision, ('base', 'left', 'right', 'merged', 'repaired'))
    return s.ControlRecipe(base, left, right, merged, repaired, ('catalog/store.py',), ('ingestion/jobs.py',),
        sha('before-public-probe-contract'), sha('after-public-probe-contract'),
        (s.ReviewTarget('package', 'package-reviewer', repaired, 'accept', sha('package-review-definition')),
         s.ReviewTarget('integration', 'integration-reviewer', repaired, 'accept', sha('integration-review-definition'))))


def registered(inv, declaration, scope, subj):
    design = c.compile_design(inv, declaration, subj, scope_plan=scope, expected_scope_sha256=scope.sha256)
    specs = tuple(s.QualificationSpec(gate.id, sha('qualification-source'), sha('qualification-definition'),
        tuple((facet, '$.original.' + facet) for key in gate.logical_gate_ids for facet in s.FACETS[key]),
        recipe() if 'M1-GATE-CONTROL' in gate.logical_gate_ids else None)
        for gate in design.prerequisites.execution_gates)
    reg = s.RegisteredAcceptance(inv.sha256, scope.sha256, c.declaration_fingerprint(declaration),
        r.design_fingerprint(design.registry), subj.execution_contract_sha256, specs)
    return reg, design


def duplicate_gate(declaration, role):
    """Two separately declared physical slots, preserving the full inventory."""
    original = next(gate for gate in declaration.gates if gate.role == role)
    other = replace(original, id=original.id + '-two', physical_slot=original.physical_slot + '-two')
    gates = (*declaration.gates, other)
    if role == 'product':
        gates = tuple(replace(gate, qualification_targets=(*gate.qualification_targets,
            replace(gate.qualification_targets[0], product_gate_id=other.id)))
            if gate.role == 'prerequisite' and gate.qualification_targets else gate for gate in gates)
    return replace(declaration, gates=gates, edges=(*declaration.edges,
        *(replace(edge, gate_id=other.id) for edge in declaration.edges if edge.gate_id == original.id)))


class FixtureSnapshot(s.AuthoritySnapshot):
    """Explicit simulation of a trusted host; no actual qualification occurs."""
    def __init__(self, inv, declaration, scope, subj):
        self.reg, self.design = registered(inv, declaration, scope, subj)
        self.requests = s.qualification_requests(self.reg, self.design, declaration)
        self.qualifications = {request.gate.id: s.QualificationEvidence(request.sha256,
            request.specification.qualification_source_sha256, 'qualification-' + request.gate.id,
            sha('qualification-receipt-' + request.gate.id), sha('verifier-' + request.gate.id),
            request.suite.execution_purpose, 'completed',
            tuple(r.CaseResult(key, 'passed') for key in request.suite.ordered_case_ids),
            tuple(r.CaseResult(key, 'passed') for key, _ in request.specification.facet_selectors))
            for request in self.requests}
        self.frozen = r.CohortFreeze(tuple(replace(subj, trajectory_id=t.id,
            source_sha256=subj.source_sha256 if t.id == subj.trajectory_id else sha(t.id))
            for t in declaration.cohort.trajectories), sha('barrier'), sha('barrier-verifier'), True)
        self.observations = {}
        for gate in self.design.registry.gates:
            original = r.PhysicalExecution(gate.binding, 'product-' + gate.gate_id, sha('product-' + gate.gate_id),
                sha('product-verifier-' + gate.gate_id), 'completed',
                tuple(r.CaseResult(key, 'passed') for key in gate.ordered_case_ids), self.frozen.receipt_sha256)
            self.observations[gate.gate_id] = r.Observation(gate.gate_id, gate.binding, original)
        self.promoted = r.Promotion(subj, 'promoted', sha('promotion'), sha('promotion-verifier'))
        self.calls = []
        self.revision = sha('test-controller-snapshot')
        self.change_after = None
        self.unavailable = None
        self.invalid = None

    @property
    def checkpoint(self):
        return self.revision

    def check_current(self, checkpoint):
        if self.change_after is not None and self.change_after in self.calls:
            self.revision = sha('changed snapshot')
        s.require(checkpoint == self.revision, 'External retained checkpoint changed')

    def called(self, label):
        self.calls.append(label)
        if self.unavailable == label:
            raise s.AuthorityUnavailable('Original authority unavailable')
        if self.invalid == label:
            raise s.AuthorityError('Original authority rejected provenance')

    def registration(self, subj):
        self.called('registration')
        return self.reg

    def qualification(self, request):
        self.called('qualification')
        return self.qualifications.get(request.gate.id)

    def freeze(self):
        self.called('freeze')
        return self.frozen

    def observation(self, gate, freeze):
        self.called('observation')
        return self.observations.get(gate.gate_id)

    def promotion(self, subj):
        self.called('promotion')
        return self.promoted


class FixtureAuthority(s.EvidenceAuthority):
    def __init__(self, snapshot):
        self.snapshot = snapshot

    def open_snapshot(self):
        return self.snapshot


class CandidateScopeConsumerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.inv = inventory(2)
        cls.declaration = synthetic_declaration(cls.inv)
        cls.scope = synthetic_scope(cls.inv, cls.declaration)
        cls.subj = subject(cls.inv)

    def setUp(self):
        self.snapshot = FixtureSnapshot(self.inv, self.declaration, self.scope, self.subj)

    def assess(self, **changes):
        values = dict(inventory=self.inv, declaration=self.declaration, scope=self.scope, subject=self.subj)
        values.update(changes)
        return s.CandidateScopeConsumer(FixtureAuthority(self.snapshot)).assess(**values)

    def qualification(self, **changes):
        key = self.snapshot.requests[0].gate.id
        self.snapshot.qualifications[key] = replace(self.snapshot.qualifications[key], **changes)

    def assert_no_product(self, result):
        self.assertFalse(result.accepted_against_registry)
        self.assertIsNone(result.product)
        self.assertNotIn('observation', self.snapshot.calls)
        self.assertNotIn('promotion', self.snapshot.calls)

    def test_complete_synthetic_capability_contract_preserves_full_denominator(self):
        result = self.assess()
        self.assertTrue(result.accepted_against_registry, result.issues)
        self.assertEqual(len(result.product.requirements), 123)
        self.assertEqual(self.snapshot.design.prerequisites.requirement_ids,
                         ('M1-INTEGRATION-02', 'M1-SCOPE-01', 'M1-SCOPE-02'))
        self.assertEqual(len(self.inv.obligations), 312)
        self.assertEqual(len(self.inv.qualification_rules), 22)
        self.assertEqual(len(self.inv.planning_notes), 188)
        self.assertEqual(len(result.qualifications[0].facets), 6)
        self.assertEqual(result.product.physical_gates, ('product',))
        self.assertEqual(result.authority_checkpoint, self.snapshot.checkpoint)

    def test_missing_independent_registration_never_derived_from_submission(self):
        self.snapshot.reg = None
        result = self.assess()
        self.assert_no_product(result)
        self.assertEqual(result.issues[0].status, 'missing')
        self.assertNotIn('qualification', self.snapshot.calls)

    def test_independent_registration_pins_all_design_identities(self):
        for name in ('inventory_sha256', 'scope_sha256', 'declaration_sha256',
                     'registry_design_sha256', 'execution_contract_sha256'):
            with self.subTest(name=name):
                self.setUp()
                self.snapshot.reg = replace(self.snapshot.reg, **{name: sha('substituted')})
                result = self.assess()
                self.assert_no_product(result)
                self.assertEqual(result.status, 'invalid_evidence')
                self.assertNotIn('qualification', self.snapshot.calls)

    def test_changed_selector_cannot_use_old_registration(self):
        edge = self.declaration.edges[0]
        declaration = replace(self.declaration, edges=(replace(edge, assertion_selector='$.fake'), *self.declaration.edges[1:]))
        result = self.assess(declaration=declaration)
        self.assert_no_product(result)
        self.assertEqual(result.status, 'invalid_evidence')

    def test_missing_qualification_keeps_product_unobserved(self):
        self.snapshot.qualifications.clear()
        result = self.assess()
        self.assert_no_product(result)
        self.assertEqual(result.issues[0].status, 'missing')

    def test_substituted_request_source_purpose_and_fixture_lineage_rejected(self):
        for changes in ({'request_sha256': sha('other-scope')},
                        {'qualification_source_sha256': self.subj.source_sha256},
                        {'original_purpose': 'public_release'}):
            with self.subTest(changes=changes):
                self.setUp()
                self.qualification(**changes)
                result = self.assess()
                self.assert_no_product(result)
                self.assertEqual(result.status, 'invalid_evidence')

    def test_facet_failure_is_not_product_failure_or_acceptance(self):
        evidence = next(iter(self.snapshot.qualifications.values()))
        for facet in evidence.facets:
            with self.subTest(facet=facet.case_id):
                self.setUp()
                facets = tuple(replace(row, status='failed') if row.case_id == facet.case_id else row for row in evidence.facets)
                self.qualification(facets=facets)
                result = self.assess()
                self.assert_no_product(result)
                self.assertEqual(result.status, 'prerequisites_failed')
                self.assertTrue(any(item.status == 'failed' for item in result.issues))

    def test_partial_qualification_cases_or_facets_stay_missing(self):
        evidence = next(iter(self.snapshot.qualifications.values()))
        for changes in ({'outcomes': ()}, {'facets': evidence.facets[:-1]}):
            with self.subTest(changes=changes):
                self.setUp()
                self.qualification(**changes)
                result = self.assess()
                self.assert_no_product(result)
                self.assertEqual(result.status, 'incomplete')
                self.assertTrue(any(item.status == 'missing' for item in result.issues))

    def test_failed_missing_and_infrastructure_remain_separate(self):
        evidence = next(iter(self.snapshot.qualifications.values()))
        self.qualification(outcomes=(), terminal_status='infrastructure_error',
            facets=(replace(evidence.facets[0], status='failed'), *evidence.facets[1:]))
        result = self.assess()
        self.assert_no_product(result)
        self.assertEqual(result.status, 'prerequisites_failed')
        self.assertEqual({row.status for row in result.issues}, {'failed', 'missing', 'infrastructure_error'})

    def test_skipped_qualification_never_qualifies(self):
        evidence = next(iter(self.snapshot.qualifications.values()))
        self.qualification(outcomes=tuple(replace(row, status='skipped') for row in evidence.outcomes))
        result = self.assess()
        self.assert_no_product(result)
        self.assertEqual(result.issues[0].status, 'skipped')

    def test_exact_successful_prerequisite_reuse_allowed_and_disclosed(self):
        self.qualification(mode='reused', reuse_receipt_sha256=sha('independently-authenticated-reuse'))
        result = self.assess()
        self.assertTrue(result.accepted_against_registry, result.issues)
        self.assertEqual(result.qualifications[0].mode, 'reused')
        self.assertEqual(result.product.reused_gates, ())

    def test_partial_failed_or_infrastructure_qualification_reuse_invalid(self):
        evidence = next(iter(self.snapshot.qualifications.values()))
        for changes in ({'outcomes': ()}, {'terminal_status': 'infrastructure_error'},
                        {'facets': tuple(replace(row, status='failed') for row in evidence.facets)}):
            with self.subTest(changes=changes):
                self.setUp()
                self.qualification(**changes, mode='reused', reuse_receipt_sha256=sha('reuse'))
                result = self.assess()
                self.assert_no_product(result)
                self.assertEqual(result.status, 'invalid_evidence')

    def test_independent_qualification_reuse_rejected(self):
        request = self.snapshot.requests[0]
        evidence = next(iter(self.snapshot.qualifications.values()))
        for purpose in ('independent_acceptance', 'repeatability'):
            with self.subTest(purpose=purpose):
                altered = replace(request, suite=replace(request.suite, execution_purpose=purpose))
                reused = replace(evidence, request_sha256=altered.sha256, original_purpose=purpose,
                                 mode='reused', reuse_receipt_sha256=sha('reuse'))
                with self.assertRaisesRegex(s.AuthorityError, 'Unsupported qualification purpose'):
                    s._qualification_issues(altered, reused)

    def test_qualification_case_and_facet_order_duplicates_extras_rejected(self):
        evidence = next(iter(self.snapshot.qualifications.values()))
        for facets in (tuple(reversed(evidence.facets)), evidence.facets + evidence.facets[:1],
                       evidence.facets + (r.CaseResult('invented', 'passed'),)):
            with self.subTest(facets=facets):
                self.setUp()
                self.qualification(facets=facets)
                result = self.assess()
                self.assert_no_product(result)
                self.assertEqual(result.status, 'invalid_evidence')

    def test_qualification_spec_cannot_omit_scope_admission_or_control_facets(self):
        spec = self.snapshot.reg.qualification_specs[0]
        for selector in spec.facet_selectors:
            with self.subTest(facet=selector[0]):
                self.setUp()
                omitted = replace(spec, facet_selectors=tuple(row for row in spec.facet_selectors if row != selector))
                self.snapshot.reg = replace(self.snapshot.reg, qualification_specs=(omitted,))
                result = self.assess()
                self.assert_no_product(result)
                self.assertEqual(result.status, 'invalid_evidence')

    def test_control_recipe_missing_changed_or_candidate_subject_cannot_qualify(self):
        spec = self.snapshot.reg.qualification_specs[0]
        for change in ({'control_recipe': None}, {'qualification_source_sha256': self.subj.source_sha256},
                       {'control_recipe': replace(spec.control_recipe, before_probe_contract_sha256=sha('different-probe'))}):
            with self.subTest(change=change):
                self.setUp()
                self.snapshot.reg = replace(self.snapshot.reg, qualification_specs=(replace(spec, **change),))
                result = self.assess()
                self.assert_no_product(result)
                self.assertEqual(result.status, 'invalid_evidence')

    def test_control_recipe_requires_disjoint_fail_then_pass_and_both_reviewers(self):
        original = recipe()
        for changes in ({'right_paths': original.left_paths}, {'before_expected': 'passed'},
                        {'reviews': original.reviews[:1]}, {'repaired': original.merged},
                        {'reviews': (original.reviews[0], original.reviews[0])}):
            with self.subTest(changes=changes), self.assertRaises(s.AuthorityError):
                replace(original, **changes)

    def test_defect_detection_is_qualification_pass_not_product_failed(self):
        result = self.assess()
        self.assertEqual(self.snapshot.requests[0].specification.control_recipe.before_expected, 'failed')
        self.assertTrue(result.accepted_against_registry)
        self.assertEqual(result.product.failed_gates, ())

    def test_missing_barrier_prevents_independent_observation_request(self):
        self.snapshot.frozen = None
        result = self.assess()
        self.assertFalse(result.accepted_against_registry)
        self.assertEqual(result.product.missing_gates, ('product',))
        self.assertNotIn('observation', self.snapshot.calls)

    def test_incomplete_or_substituted_cohort_barrier_rejected(self):
        freeze = self.snapshot.frozen
        for change in ({'subjects': freeze.subjects[:-1]}, {'no_further_model_actions': False},
                       {'subjects': tuple(replace(row, requirements_sha256=sha('other-product')) for row in freeze.subjects)},
                       {'subjects': tuple(replace(row, milestone='M1') for row in freeze.subjects)},
                       {'subjects': (replace(freeze.subjects[0], source_sha256=sha('changed-final')), *freeze.subjects[1:])}):
            with self.subTest(change=change):
                self.setUp()
                self.snapshot.frozen = replace(freeze, **change)
                result = self.assess()
                self.assertEqual(result.status, 'invalid_evidence')
                self.assertNotIn('observation', self.snapshot.calls)

    def test_missing_and_rejected_promotion_cannot_accept(self):
        self.snapshot.promoted = None
        result = self.assess()
        self.assertEqual(result.status, 'incomplete')
        self.assertEqual(result.product.promotion_status, 'missing')
        self.snapshot.promoted = r.Promotion(self.subj, 'rejected', sha('reject'), sha('reject-verifier'))
        result = self.assess()
        self.assertEqual(result.status, 'rejected')
        self.assertEqual(result.product.promotion_status, 'rejected')

    def test_product_failure_missing_and_infrastructure_remain_product_outcomes(self):
        original = self.snapshot.observations['product']
        for status, expected in (('failed', 'rejected'), ('infrastructure_error', 'incomplete'), ('skipped', 'incomplete')):
            with self.subTest(status=status):
                execution = replace(original.execution, outcomes=(r.CaseResult('case', status),))
                self.snapshot.observations['product'] = replace(original, execution=execution)
                result = self.assess()
                self.assertEqual(result.status, expected)
                self.assertEqual(result.issues, ())
        self.snapshot.observations.clear()
        result = self.assess()
        self.assertEqual(result.product.missing_gates, ('product',))

    def test_product_subject_evaluator_and_freeze_substitution_rejected(self):
        original = self.snapshot.observations['product']
        for binding in (replace(original.binding, evaluator_sha256=sha('wrong-evaluator')),
                        replace(original.binding, subject=replace(self.subj, source_sha256=sha('wrong-source')))):
            with self.subTest(binding=binding):
                self.setUp()
                execution = replace(original.execution, binding=binding)
                self.snapshot.observations['product'] = replace(original, binding=binding, execution=execution)
                self.assertEqual(self.assess().status, 'invalid_evidence')
        self.setUp()
        execution = replace(original.execution, cohort_freeze_sha256=sha('wrong-freeze'))
        self.snapshot.observations['product'] = replace(original, execution=execution)
        self.assertEqual(self.assess().status, 'invalid_evidence')

    def test_snapshot_revocation_before_or_after_product_prevents_acceptance(self):
        for phase in ('registration', 'qualification', 'observation', 'promotion'):
            with self.subTest(phase=phase):
                self.setUp()
                self.snapshot.change_after = phase
                result = self.assess()
                self.assertEqual(result.status, 'invalid_evidence')
                if result.product is not None:
                    self.assertFalse(result.product.accepted_against_registry)
                if phase in ('registration', 'qualification'):
                    self.assertNotIn('observation', self.snapshot.calls)

    def test_authority_unavailability_and_integrity_failure_are_distinct(self):
        for field, expected in (('unavailable', 'incomplete'), ('invalid', 'invalid_evidence')):
            for phase in ('registration', 'qualification', 'freeze', 'observation', 'promotion'):
                with self.subTest(field=field, phase=phase):
                    self.setUp()
                    setattr(self.snapshot, field, phase)
                    result = self.assess()
                    self.assertEqual(result.status, expected)
                    if phase in ('registration', 'qualification', 'freeze'):
                        self.assertIsNone(result.product)
                    else:
                        self.assertIsNotNone(result.product)
                        self.assertFalse(result.product.accepted_against_registry)

    def test_two_qualification_slots_cannot_alias_execution_or_receipt(self):
        declaration = duplicate_gate(self.declaration, 'prerequisite')
        scope = synthetic_scope(self.inv, declaration)
        for field in ('execution_id', 'receipt_sha256'):
            with self.subTest(field=field):
                self.snapshot = FixtureSnapshot(self.inv, declaration, scope, self.subj)
                first, second = self.snapshot.requests
                original = self.snapshot.qualifications[first.gate.id]
                self.snapshot.qualifications[second.gate.id] = replace(
                    self.snapshot.qualifications[second.gate.id], **{field: getattr(original, field)})
                result = self.assess(declaration=declaration, scope=scope)
                self.assert_no_product(result)
                self.assertEqual(result.status, 'invalid_evidence')

    def test_physical_independent_purpose_qualification_requires_new_barrier_adapter(self):
        evidence = next(iter(self.snapshot.qualifications.values()))
        for purpose in ('independent_acceptance', 'repeatability', 'public_release', 'authored_reference_qualification'):
            with self.subTest(purpose=purpose):
                request = self.snapshot.requests[0]
                request = replace(request, suite=replace(request.suite, execution_purpose=purpose))
                changed = replace(evidence, request_sha256=request.sha256, original_purpose=purpose)
                with self.assertRaisesRegex(s.AuthorityError, 'Unsupported qualification purpose'):
                    s._qualification_issues(request, changed)

    def test_prior_authenticated_product_failure_survives_unavailable_or_invalid_sibling(self):
        declaration = duplicate_gate(self.declaration, 'product')
        scope = synthetic_scope(self.inv, declaration)
        for error in (s.AuthorityUnavailable, s.AuthorityError):
            with self.subTest(error=error):
                self.snapshot = FixtureSnapshot(self.inv, declaration, scope, self.subj)
                first = self.snapshot.observations['product']
                self.snapshot.observations['product'] = replace(first, execution=replace(first.execution,
                    outcomes=(r.CaseResult('case', 'failed'),)))
                original = self.snapshot.observation
                def observe(gate, freeze):
                    if gate.gate_id == 'product-two':
                        raise error('Second original unavailable or invalid')
                    return original(gate, freeze)
                self.snapshot.observation = observe
                result = self.assess(declaration=declaration, scope=scope)
                self.assertFalse(result.accepted_against_registry)
                self.assertEqual(result.product.failed_gates, ('product',))
                self.assertEqual(result.product.missing_gates, ('product-two',))
                self.assertEqual(result.product.status, 'rejected')
                self.assertTrue(any(row.target == 'product-two' for row in result.issues))

    def test_prior_product_failure_survives_unavailable_promotion_or_final_checkpoint(self):
        first = self.snapshot.observations['product']
        failed = replace(first, execution=replace(first.execution, outcomes=(r.CaseResult('case', 'failed'),)))
        for change in ('promotion', 'checkpoint'):
            with self.subTest(change=change):
                self.setUp()
                self.snapshot.observations['product'] = failed
                if change == 'promotion':
                    self.snapshot.unavailable = 'promotion'
                else:
                    self.snapshot.change_after = 'promotion'
                result = self.assess()
                self.assertFalse(result.accepted_against_registry)
                self.assertEqual(result.product.failed_gates, ('product',))
                self.assertEqual(result.product.status, 'rejected')

    def test_product_origin_alias_preserves_first_failure_and_marks_second_invalid(self):
        declaration = duplicate_gate(self.declaration, 'product')
        scope = synthetic_scope(self.inv, declaration)
        for field in ('execution_id', 'receipt_sha256'):
            with self.subTest(field=field):
                self.snapshot = FixtureSnapshot(self.inv, declaration, scope, self.subj)
                first = self.snapshot.observations['product']
                self.snapshot.observations['product'] = replace(first, execution=replace(first.execution,
                    outcomes=(r.CaseResult('case', 'failed'),)))
                second = self.snapshot.observations['product-two']
                self.snapshot.observations['product-two'] = replace(second, execution=replace(second.execution,
                    **{field: getattr(first.execution, field)}))
                result = self.assess(declaration=declaration, scope=scope)
                self.assertEqual(result.status, 'invalid_evidence')
                self.assertEqual(result.product.failed_gates, ('product',))
                self.assertEqual(result.product.missing_gates, ('product-two',))

    def test_untyped_submission_cannot_reach_authority(self):
        for key in ('inventory', 'declaration', 'scope', 'subject'):
            with self.subTest(key=key):
                self.setUp()
                result = self.assess(**{key: {'passed': True}})
                self.assertEqual(result.status, 'invalid_evidence')
                self.assertEqual(self.snapshot.calls, [])

    def test_receipt_dict_or_bare_callback_is_not_a_controller_capability(self):
        for value in ({'passed': True}, lambda: self.snapshot):
            with self.subTest(value=type(value)), self.assertRaises(s.AuthorityError):
                s.CandidateScopeConsumer(value)


class CandidateScopeReleaseBridgeTests(unittest.TestCase):
    """Real Git/controller fixture journal, no Docker/product observation claimed."""
    def setUp(self):
        from tests.test_candidate_release_execution_v1 import make_store, registration
        from gossip_harness import candidate_release_execution_v1 as execution
        from gossip_harness import candidate_release_observer_v1 as observer
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        store = make_store(self.root / 'source.git', {'library/__init__.py': b'# fixture only\n'})
        policy = execution.ReleasePolicy(observer.RUNTIME_IMAGE)
        self.registration = registration(store, policy, {'kind': 'fixture-no-Docker'})
        self.owner = execution.CandidateReleaseExecution(self.root / 'journal', store, self.registration,
                                                        policy, mode='fixture')
        self.addCleanup(self.owner.close)
        self.checkpoint = self.owner.checkpoint()
        self.bridge = s.ReleaseObservationSource(self.owner, self.checkpoint)

    def test_real_fixture_journal_never_authenticates_product_execution(self):
        with self.assertRaisesRegex(s.AuthorityError, 'Fixture records'):
            self.bridge.observation(self.registration.gate, None)

    def test_substituted_gate_or_external_checkpoint_rejected(self):
        gate = replace(self.registration.gate, gate_id='other')
        with self.assertRaisesRegex(s.AuthorityError, 'gate changed'):
            self.bridge.observation(gate, None)
        from gossip_harness.candidate_release_execution_v1 import ControllerCheckpoint
        bad = ControllerCheckpoint((('config.json', sha('substituted')),))
        with self.assertRaisesRegex(s.AuthorityError, 'retained checkpoint'):
            s.ReleaseObservationSource(self.owner, bad).observation(self.registration.gate, None)

    def test_candidate_appended_success_cannot_enter_real_journal(self):
        (self.owner.root / 'terminal.json').write_text('{"passed":true}')
        with self.assertRaisesRegex(s.AuthorityError, 'controller-owned original writes'):
            self.bridge.observation(self.registration.gate, None)

    def test_v1_journal_cannot_be_recast_as_v2_bridge(self):
        with self.assertRaisesRegex(s.AuthorityError, 'Actual v2 release'):
            s.V2ReleaseObservationSource(self.owner, self.checkpoint)
