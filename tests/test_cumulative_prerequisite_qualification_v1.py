"""Actual guard/parser controls; no real complete semantic approval is invented."""
from copy import deepcopy
from dataclasses import asdict, replace
import unittest

from gossip_harness import cumulative_prerequisite_qualification_v1 as qualification
from gossip_harness import candidate_scope_consumer_v1 as consumer
from gossip_harness import project_acceptance_compiler_v1 as compiler
from tests.test_candidate_observation_admission_v1 import registration, frozen


class CumulativePrerequisiteQualificationV1Tests(unittest.TestCase):
    def fact(self,purpose='independent_acceptance'):
        registered=registration(purpose)
        return {'registration':asdict(registered),'guards':qualification._control(registered,frozen(registered),lambda:None)}

    def test_actual_closed_guards_and_raw_bounded_parser_controls_pass(self):
        fact=self.fact(); malformed=qualification.malformed_controls()
        self.assertEqual(tuple(row['case'] for row in fact['guards']),qualification.GUARDS)
        self.assertTrue(all(row['passed'] for row in fact['guards']))
        self.assertTrue(all(row.status=='passed' for row in qualification.admission_outcomes((fact,),malformed)))
        self.assertTrue(all(row['observed']=='JournalError' for row in malformed))

    def test_public_guard_control_discloses_absent_freeze_duty(self):
        fact=self.fact('public_release')
        self.assertTrue(all(row['expected']=='not-applicable-public-purpose' for row in fact['guards'] if row['case'].startswith('cohort-')))
        self.assertTrue(all(row.status=='passed' for row in qualification.admission_outcomes((fact,),qualification.malformed_controls())))

    def test_actual_authority_unavailability_is_not_a_synthetic_admission_pass(self):
        registered=registration('independent_acceptance')
        def unavailable(): raise consumer.AuthorityUnavailable('Original scope revoked')
        with self.assertRaisesRegex(consumer.AuthorityUnavailable,'revoked'):
            qualification._control(registered,frozen(registered),unavailable)

    def test_known_false_guard_survives_successful_siblings(self):
        fact=deepcopy(self.fact());fact['guards'][2].update(observed='admitted',passed=False)
        rows=qualification.admission_outcomes((fact,),qualification.malformed_controls())
        self.assertEqual(rows[1].status,'failed');self.assertEqual(rows[3].status,'passed')

    def test_claimed_pass_cannot_replace_original_false_guard(self):
        fact=deepcopy(self.fact());fact['guards'][2].update(observed='admitted',passed=True)
        with self.assertRaisesRegex(consumer.AuthorityError,'Boolean disagrees'):
            qualification.admission_outcomes((fact,),qualification.malformed_controls())

    def test_missing_guard_cannot_narrow_census(self):
        fact=self.fact();fact['guards']=fact['guards'][:-1]
        with self.assertRaisesRegex(consumer.AuthorityError,'census'):
            qualification.admission_outcomes((fact,),qualification.malformed_controls())

    def test_foreign_malformed_input_cannot_replace_authored_raw_bytes(self):
        malformed=deepcopy(qualification.malformed_controls());malformed[0]['raw_hex']=b'{}'.hex()
        with self.assertRaisesRegex(consumer.AuthorityError,'raw control'):
            qualification.admission_outcomes((self.fact(),),malformed)

    def test_missing_malformed_input_cannot_narrow_census(self):
        with self.assertRaisesRegex(consumer.AuthorityError,'census'):
            qualification.admission_outcomes((self.fact(),),qualification.malformed_controls()[:-1])

    def test_independent_freeze_cannot_be_declared_public_not_applicable(self):
        fact=deepcopy(self.fact());fact['guards'][-1].update(expected='not-applicable-public-purpose',observed='not-applicable-public-purpose')
        with self.assertRaisesRegex(consumer.AuthorityError,'relabeled'):
            qualification.admission_outcomes((fact,),qualification.malformed_controls())

    def test_rejected_or_unresolved_review_never_maps_to_pass(self):
        self.assertEqual(qualification._status('reject'),'failed')
        self.assertEqual(qualification._status('unresolved'),'skipped')

    def test_actual_definition_binds_every_target_and_separate_harness_source(self):
        first=registration().gate
        second=replace(first,gate_id='another-target',binding=replace(first.binding,evaluator_sha256='d'*64))
        revision=consumer.Revision('a'*40,'b'*40,'c'*64)
        definition=qualification.definition_for('admission',revision,(first,second),gate_id='admission',suite_id='admission-suite',physical_slot='admission-slot')
        self.assertEqual(tuple(row.product_gate_id for row in definition.execution.qualification_targets),(first.gate_id,second.gate_id))
        self.assertEqual(definition.specification.qualification_source_sha256,revision.source_sha256)
        self.assertEqual(definition.suite.source_contract_sha256,compiler.M1_SHA256)
        self.assertEqual(definition.suite.capabilities,('harness-admission',))
        self.assertNotEqual(definition.product_lineages_sha256,consumer._lineages((first,)))
        self.assertFalse(definition.suite.candidate_suitable)

    def test_scope_is_registry_qualification_and_does_not_claim_control(self):
        definition=qualification.definition_for('scope',consumer.Revision('a'*40,'b'*40,'c'*64),
            (registration().gate,),gate_id='scope',suite_id='scope-suite',physical_slot='scope-slot')
        self.assertEqual(definition.suite.execution_purpose,'registry_qualification')
        self.assertEqual(definition.specification.facet_selectors,(('scope','/outcomes'),))
        self.assertIsNone(definition.specification.control_recipe)

    def test_unknown_combined_qualification_kind_is_rejected(self):
        with self.assertRaises(consumer.AuthorityError):
            qualification.definition_for('all',consumer.Revision('a'*40,'b'*40,'c'*64),
                (registration().gate,),gate_id='all',suite_id='all-suite',physical_slot='all-slot')


class CumulativePrerequisitePartialOriginalV1Tests(unittest.TestCase):
    """Real acknowledged journal prefix with explicit semantic-authority stubs."""
    def setUp(self):
        from pathlib import Path
        from types import SimpleNamespace
        import tempfile
        from gossip_harness import candidate_checkpoint_chain_v1 as checkpoint
        from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
        from gossip_harness.cumulative_study_controller_v1 import Records
        from gossip_harness import candidate_client_execution_v5 as cli
        from tests.test_cumulative_scope_source_v1 import cli_registration
        temporary=tempfile.TemporaryDirectory(prefix='gossip-admission-partial-');self.addCleanup(temporary.cleanup)
        root=Path(temporary.name).resolve()
        head=ExternalHead.create(root/'head',journal_roots=(root/'raw',root/'delta'));self.addCleanup(head.close)
        self.chain=checkpoint.CheckpointChain.create(root/'raw',root/'delta',context={'synthetic':'partial-control-reader'},authority=head)
        self.addCleanup(self.chain.close);self.records=Records(self.chain)
        # Exact closed legacy CLI components keep the full source-factory schema.
        # These host-only declarations remain synthetic semantic-authority inputs,
        # not executions or independently approved scope.
        first=cli_registration(purpose='independent_acceptance')
        second=replace(first,gate=replace(first.gate,gate_id='second-declared-target'))
        self.first,self.second=(cli.observation_registration(value) for value in (first,second))
        self.slices=tuple(qualification.source.cli_slice(value) for value in (first,second))
        self.source=consumer.Revision('a'*40,'b'*40,'c'*64)
        self.definition=qualification.definition_for('admission',self.source,(self.first.gate,self.second.gate),
            gate_id='admission',suite_id='admission-suite',physical_slot='admission-slot')
        registered=consumer.RegisteredAcceptance(*(['a'*64]*5),(self.definition.specification,))
        self.request=consumer.QualificationRequest(registered,self.definition.execution,self.definition.suite,
            self.definition.specification,self.definition.product_lineages_sha256)
        self.provenance=self.source  # Explicit structural stand-in; not a scope review.
        self.freeze=replace(frozen(self.first),subjects=tuple(
            replace(self.first.gate.binding.subject,trajectory_id=key)
            for key in self.first.cohort_trajectory_ids))
        self.owner=object.__new__(qualification.PrerequisiteQualification)
        self.owner.source=self.source
        self.owner.owner=SimpleNamespace(records=self.records,chain=self.chain,freeze=self.freeze)
        self.owner._resolve=lambda request:(SimpleNamespace(slices=self.slices),
            SimpleNamespace(gates=(self.first.gate,self.second.gate)),self.definition,self.provenance)
        self.key=self.owner._key(self.request)
        self.records.put(self.key+'.intent',{'request':asdict(self.request),'definition':asdict(self.definition),
            'scope_provenance':asdict(self.provenance),'freeze':asdict(self.freeze),
            'registrations':[asdict(self.first),asdict(self.second)]})

    def retain_target(self,index,failed=False):
        registered=(self.first,self.second)[index]
        guards=deepcopy(qualification._control(registered,self.freeze,lambda:None))
        if failed:guards[1].update(observed='admitted',passed=False)
        self.records.put(self.key+'.target.'+str(index),{'registration':asdict(registered),
            'slice_sha256':self.slices[index].sha256,'guards':guards})

    def partial(self):
        return self.owner._partial_execution(self.request)

    def test_known_failed_acknowledged_prefix_survives_unknown_later_target(self):
        self.retain_target(0,failed=True);before=self.chain.commitment
        result=self.partial()
        self.assertEqual(result['terminal_status'],'infrastructure_error')
        self.assertEqual(result['outcomes'][1]['status'],'failed')
        self.assertEqual(result['outcomes'][3]['status'],'infrastructure_error')
        self.assertEqual(result['body']['unobserved_targets'],[self.second.gate.gate_id])
        self.assertTrue(result['body']['missing_original_terminal'])
        self.assertEqual(before,self.chain.commitment)
        self.assertIsNone(self.records.read(self.key+'.execution'))

    def test_empty_acknowledged_prefix_never_fills_unknown_controls_with_passes(self):
        result=self.partial()
        self.assertEqual([row['status'] for row in result['outcomes']],
            ['passed','infrastructure_error','infrastructure_error','infrastructure_error'])
        self.assertEqual(len(result['body']['unobserved_targets']),2)

    def test_noncontiguous_target_prefix_is_invalid_not_partial_success(self):
        self.retain_target(1)
        with self.assertRaisesRegex(consumer.AuthorityError,'gap/substitution'):self.partial()

    def test_claimed_guard_pass_cannot_override_original_observed_mismatch(self):
        guards=list(qualification._control(self.first,self.freeze,lambda:None))
        guards[1]=dict(guards[1],observed='admitted',passed=True)
        self.records.put(self.key+'.target.0',{'registration':asdict(self.first),'slice_sha256':self.slices[0].sha256,'guards':guards})
        with self.assertRaisesRegex(consumer.AuthorityError,'Boolean disagrees'):self.partial()
