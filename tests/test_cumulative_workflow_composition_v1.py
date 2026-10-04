"""Prospective workflow composition; synthetic reviews never grant full scope.

Actual journals, named inputs and host originals exercise mechanisms. The fixture
phase adapter explicitly isolates unavailable whole-scope/prerequisite authority;
no complete rehearsal, candidate acceptance or Engine execution is asserted.
"""
from contextlib import ExitStack
from dataclasses import asdict, replace
from functools import wraps
import hashlib
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from gossip_harness import cumulative_workflow_exposure_v1 as exposure
from gossip_harness import cumulative_cli_projection_v1 as cli
from gossip_harness import cumulative_final_acceptance_v1 as shared
from gossip_harness import cumulative_final_acceptance_v3 as final
from gossip_harness import cumulative_rehearsal_codec_v1 as codec
from gossip_harness import cumulative_rehearsal_export_v1 as exporter
from gossip_harness import cumulative_final_originals_v1 as cold
from gossip_harness import cumulative_prerequisite_qualification_v1 as prerequisite
from gossip_harness import candidate_client_execution_v5 as cli_execution
from gossip_harness import project_acceptance_compiler_v1 as compiler
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import cumulative_scope_authority_v3 as scope
from gossip_harness import cumulative_scope_source_v3 as source
from gossip_harness import cumulative_study_runtime_v2 as runtime
from gossip_harness import cumulative_study_successor_v3 as successor
from gossip_harness import candidate_checkpoint_chain_v1 as checkpoint
from gossip_harness import candidate_scope_consumer_v1 as consumer
from gossip_harness import candidate_workflow_review_v1 as review
from gossip_harness import candidate_workflow_observation_v1 as observer
from gossip_harness import project_acceptance_registry_v1 as registry
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from gossip_harness.cumulative_study_controller_v2 import StudyController, Records, plain
from gossip_harness.peer_role_loop_v2 import materialize
from gossip_harness.peer_project_contract_v2 import canonical_bytes
from tests import test_cumulative_cli_projection_v1 as cli_tests
from tests import test_candidate_workflow_review_v1 as review_tests
from tests.test_cumulative_terminal_originals_v2 import synthetic_plan
from tests.test_peer_role_loop_v2 import FakeMesh

ROOT = Path(__file__).resolve().parents[1]


class CumulativeWorkflowExposureV1Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base = synthetic_plan()
        cls.cli_plan = cli.expose_plan(cls.base)
        cls.plan = exposure.expose_plan(cls.cli_plan)

    def test_exact_three_identities_and_four_ordered_releases_preserve_existing_contract(self):
        self.assertEqual(hashlib.sha256(exposure.text().encode()).hexdigest(),exposure.TEXT_SHA256)
        self.assertEqual(cli.contract_fields(self.plan),cli.contract_fields(self.cli_plan))
        self.assertEqual(exposure.contract_fields(self.plan),{
            'workflow_requirements':exposure.manifest(),'combined_effective_requirements':exposure.combined_manifest()})
        self.assertFalse(set(cli.contract_fields(self.plan)) & set(exposure.contract_fields(self.plan)))
        for original,current in zip(self.base.releases,self.plan.releases,strict=True):
            self.assertEqual(current.instructions,original.instructions+'\n\n'+exposure.text()+'\n\n'+cli.text())
            self.assertEqual(current.files,original.files | {cli.RELEASE_PATH:cli.text(),exposure.RELEASE_PATH:exposure.text()})
            for field in ('checks','command','ordered_check_ids','requirement_ids','purpose','source_contract_sha256'):
                self.assertEqual(getattr(current,field),getattr(original,field))
        for field in ('horizon_seconds','source_generations','partition_seconds','executor_slots','financial_closure_policy'):
            self.assertEqual(getattr(self.plan,field),getattr(self.base,field))
        self.assertEqual(sum(len(row.actors) for row in self.plan.roster.children),96)
        self.assertEqual(len(self.plan.roster.children),6)
        self.assertIsNone(exposure.validate_plan(self.base,ROOT))
        self.assertIsNone(exposure.validate_plan(self.cli_plan,ROOT))
        self.assertEqual(exposure.validate_plan(self.plan,ROOT),exposure.combined_manifest())

    def test_actual_all_role_materialization_carries_both_fulltext_and_readonly_files(self):
        controller = object.__new__(StudyController);controller.plan=self.plan
        counts={'build':0,'repair':0,'review':0};actors=set()
        for milestone,release in enumerate(self.plan.releases,1):
            mesh=FakeMesh()
            ref=mesh.arrive('seed','source',canonical_bytes({'files':self.plan.initial_files | release.files,'base_sha':'a'*40}))
            for index in range(6):
                for generation,reviews in ((0,False),(1,False),(0,True),(1,True)):
                    for actor,directive in controller._directives(index,milestone,generation,ref,(),reviews=reviews):
                        request,view=materialize(directive,mesh,'d'*64)
                        actors.add(actor);counts[directive.kind]+=1
                        self.assertEqual(request.instructions.count(exposure.text()),1)
                        self.assertEqual(request.instructions.count(cli.text()),1)
                        self.assertLess(request.instructions.index(exposure.text()),request.instructions.index(cli.text()))
                        for path,text in ((exposure.RELEASE_PATH,exposure.text()),(cli.RELEASE_PATH,cli.text())):
                            self.assertEqual(request.files[path],text);self.assertNotIn(path,request.allowed_paths)
                        self.assertEqual(view.context,directive.context)
        self.assertEqual(len(actors),96);self.assertEqual(counts,{'build':288,'repair':288,'review':192})

    def test_drop_reorder_duplicate_file_and_manifest_refuse_before_entry_effect(self):
        variants=[]
        for index,release in enumerate(self.plan.releases):
            for changed in (replace(release,instructions=release.instructions.replace(exposure.text(),'')),
                replace(release,instructions=release.instructions+'\n'+exposure.text()),
                replace(release,instructions=release.instructions.replace(exposure.text()+'\n\n'+cli.text(),cli.text()+'\n\n'+exposure.text())),
                replace(release,files={key:value for key,value in release.files.items() if key != exposure.RELEASE_PATH}),
                replace(release,files={**release.files,cli.RELEASE_PATH:'changed'})):
                releases=list(self.plan.releases);releases[index]=changed
                variants.append(cli_tests.rebalance(self.plan,releases=tuple(releases)))
        for key in ('workflow_projection_protocol','workflow_projection_contract','cumulative_public_contract',
                    'cli_projection_protocol','cli_projection_contract'):
            variants.append(cli_tests.rebalance(self.plan,runtime={k:v for k,v in self.plan.runtime.items() if k != key}))
        for value in variants:
            with self.subTest(contract=value.sha256),patch.object(runtime,'ledger_identity') as wallet:
                with self.assertRaises(ValueError):
                    runtime.run_study(value,output=ROOT,repository=ROOT,checkpoint=None,expected_checkpoint=None,
                        existing_ledger_path=ROOT/'never-wallet',expected_ledger_identity={},workers={},mode='fixture',
                        permit_provider=None,evaluator=None)
                wallet.assert_not_called()

    def test_minimum_legacy_guard_and_workflow_readonly_path_cannot_be_dropped(self):
        for key in exposure.guard_sources():
            changed=replace(self.base,source_pins={**self.base.source_pins,key:'f'*64})
            with self.subTest(pin=key),self.assertRaises(ValueError):exposure.validate_plan(changed,ROOT)
        for path in (exposure.RELEASE_PATH,'requirements',exposure.RELEASE_PATH+'/child'):
            paths=dict(self.plan.package_paths);paths[next(iter(paths))]=(path,)
            with self.subTest(path=path),self.assertRaises(ValueError):exposure.validate_plan(replace(self.plan,package_paths=paths))

    def test_closed_typed_plan_and_both_original_profile_identity_records_survive_transport(self):
        from gossip_harness import candidate_workflow_profile_v1 as profiles
        value=profiles.profile_for(profiles.CASE_IDS[0],'independent_acceptance')
        host=review.WorkflowInspectionProfile()
        restored=codec.unpack(codec.pack({'plan':self.plan,'workflow':value,'host':host}))
        self.assertEqual(restored,{'plan':self.plan,'workflow':value,'host':host})
        for spec in (SimpleNamespace(kind='workflow',profile=restored['workflow']),
                     SimpleNamespace(kind='workflow_inspection',profile=restored['host'])):
            exposure.validate_spec(restored['plan'],spec)
            with self.assertRaises(ValueError):exposure.validate_spec(self.cli_plan,spec)
        wire=codec.pack(self.plan)
        wire['input']['value']['runtime']['value'].pop('cumulative_public_contract')
        with self.assertRaises(ValueError):exposure.validate_plan(codec.unpack(wire))


class CumulativeWorkflowStudyCompositionV1Tests(unittest.TestCase):
    def setUp(self):
        cli_tests.CumulativeCliProjectionCompositionV1Tests.setUp(self)
        self.plan=exposure.expose_plan(self.plan)

    chain=cli_tests.CumulativeCliProjectionCompositionV1Tests.chain

    def test_actual_final_constructor_records_all_identities_and_six_unavailable_slots(self):
        import sqlite3
        from gossip_harness.peer_financial_authority_v2 import ledger_identity
        study,_,_=self.chain('study');journal,_,_=self.chain('final');scope_chain,_,_=self.chain('scope')
        wallet=self.root/'unenrolled.sqlite'
        with sqlite3.connect(wallet) as db:db.execute('CREATE TABLE fixture_only (id INTEGER)')
        identity=ledger_identity(wallet)
        Records(study).put('contract',self.plan.record());Records(study).put('ledger',identity)
        authority=scope.ScopeRegistrationController(ROOT,scope_chain,scope_chain.commitment)
        owner=final.FinalAcceptanceV3(plan=self.plan,repository=ROOT,ledger_identity=identity,
            study_chain=study,study_expected=study.commitment,journal=journal,expected=journal.commitment,
            scope=authority,submissions=())
        self.addCleanup(owner.close)
        contract=owner.records.read('final.contract')
        for key,value in {**cli.contract_fields(self.plan),**exposure.contract_fields(self.plan)}.items():
            self.assertEqual(contract[key],value)
        result=successor.run_final_phase(owner,())
        self.assertFalse(result['accepted']);self.assertIsNone(owner.freeze)
        self.assertEqual(len(owner.originals.slots),6)
        self.assertEqual(sum(row.process_counts['planned'] for row in owner.originals.slots),96)
        self.assertTrue(all(row.outcome=='unattempted' for row in owner.originals.slots))

    def test_actual_successor_and_tick_reject_nested_workflow_revocation(self):
        from copy import deepcopy
        from unittest.mock import Mock
        changed=deepcopy(self.plan)
        changed.releases[0].files[exposure.RELEASE_PATH]='revoked'
        with patch.object(runtime,'run_study') as dispatched:
            with self.assertRaises(ValueError):successor.run_public_phase(changed,repository=ROOT,mode='fixture')
            dispatched.assert_not_called()
        chain,_,_=self.chain('unstarted-child');before=chain.commitment
        permit,evaluator,clock=Mock(),Mock(),Mock()
        output=self.root/'child-never-started'
        with patch.object(runtime,'ledger_identity') as wallet:
            with self.assertRaises(ValueError):
                runtime.GossipChildRuntime(changed,0,Records(chain),10,root=output,repository=ROOT,
                    existing_ledger_path=self.root/'absent-wallet',expected_ledger_identity={},workers={},
                    mode='fixture',permit_provider=permit,evaluator=evaluator,clock=clock)
            for effect in (wallet,permit,evaluator,clock):effect.assert_not_called()
        self.assertFalse(output.exists());self.assertEqual(chain.commitment,before)
        child=object.__new__(runtime.GossipChildRuntime)
        child.plan,child.repository,child.clock=changed,ROOT,Mock()
        with self.assertRaises(ValueError):child._tick()
        child.clock.assert_not_called()


class CumulativeWorkflowScopeCompositionV1Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Reuse a genuinely incomplete source declaration; it cannot register.
        cli_tests.CumulativeCliProjectionScopeV1Tests.setUpClass.__func__(cls)
        cls.plan=exposure.expose_plan(cls.plan)
        registration=cli_tests.input_registration(cls.value)
        subject=replace(registration.gate.binding.subject,cohort_id=cls.plan.cohort.cohort_id,
            trajectory_id=cls.plan.roster.children[0].trajectory,execution_contract_sha256=cls.plan.sha256)
        registration=replace(registration,gate=cli_execution.gate_for(subject,registration.binding,
            gate_id=registration.gate.gate_id),cohort_trajectory_ids=tuple(row.trajectory for row in cls.plan.roster.children))
        cls.component=source.cli_slice(registration)
        cls.declaration=source.assemble_declaration(cls.catalog,cls.plan.cohort,(cls.component,),
            review_sha256='d'*64,capacity_profile=registry.HISTORY_CAPACITY_PROFILE)
        cls.scope=source.scope_review_input(cls.catalog,cls.declaration)
        cls.submission=scope.ScopeSubmission(cls.catalog,cls.declaration,cls.scope,subject,(cls.component,),(),
            cli_projection_contract=cli.manifest(),workflow_projection_contract=exposure.manifest(),
            cumulative_public_contract=exposure.combined_manifest())

    setUp=cli_tests.CumulativeCliProjectionScopeV1Tests.setUp
    deliver=cli_tests.CumulativeCliProjectionScopeV1Tests.deliver

    def test_original_full_scope_target_adds_exact_workflow_family_and_inspection_without_waiver(self):
        request=self.submission.request()
        for prefix,count in (('obligation:',312),('authority:',22),('gap:',188)):
            self.assertEqual(sum(row['id'].startswith(prefix) for row in request['targets']),count)
        for key in (cli.REVIEW_TARGET,exposure.REVIEW_TARGET,exposure.FAMILY_REVIEW_TARGET,exposure.INSPECTION_REVIEW_TARGET):
            self.assertEqual(sum(row['id']==key for row in request['targets']),1)
        self.assertEqual(request['effective_requirements'],cli.manifest())
        self.assertEqual(request['workflow_requirements'],exposure.manifest())
        self.assertEqual(request['combined_effective_requirements'],exposure.combined_manifest())
        family=next(row for row in request['targets'] if row['id']==exposure.FAMILY_REVIEW_TARGET)
        self.assertEqual(len(family['value']['complete_original_and_supplemental_definitions']),20)
        staged=self.owner.stage(self.submission)
        self.assertTrue(staged.design.blockers);self.assertIsNone(staged.design.registry)
        with self.assertRaises(scope.RegistrationMissing):self.owner.register(self.submission,report_name='absent.json')

    def test_missing_unresolved_or_changed_workflow_review_cannot_be_digest_only_approval(self):
        staged=self.owner.stage(self.submission)
        self.deliver(staged,lambda rows:next(row for row in rows if row['target_id']==exposure.REVIEW_TARGET).update(decision='unresolved'))
        with self.assertRaises(scope.RegistrationMissing):self.owner.authenticate_review(staged,report_name='review.json')
        self.assertIsNone(self.owner.open_snapshot().registration(self.submission.subject))
        reopened=codec.unpack(codec.pack(self.submission))
        self.assertEqual(reopened.request(),self.submission.request())
        for field in ('workflow_projection_contract','cumulative_public_contract'):
            with self.subTest(field=field),self.assertRaises(ValueError):replace(reopened,**{field:None}).request()
        downgraded=replace(reopened,workflow_projection_contract=None,cumulative_public_contract=None)
        with self.assertRaises(ValueError):exposure.validate_submission(self.plan,(downgraded,))

    def test_actual_six_typed_requests_stop_at_missing_complete_scope_before_physical(self):
        submissions=[]
        for child in self.plan.roster.children:
            subject=replace(self.submission.subject,trajectory_id=child.trajectory)
            gate=replace(self.component.gate,binding=replace(self.component.gate.binding,subject=subject))
            registration=cli_tests.input_registration(self.value)
            registration=replace(registration,gate=gate,
                cohort_trajectory_ids=tuple(row.trajectory for row in self.plan.roster.children))
            component=source.cli_slice(registration)
            declaration=source.assemble_declaration(self.catalog,self.plan.cohort,(component,),
                review_sha256='d'*64,capacity_profile=registry.HISTORY_CAPACITY_PROFILE)
            submission=scope.ScopeSubmission(self.catalog,declaration,source.scope_review_input(self.catalog,declaration),
                subject,(component,),(),cli_projection_contract=cli.manifest(),
                workflow_projection_contract=exposure.manifest(),cumulative_public_contract=exposure.combined_manifest())
            self.owner.stage(submission);submissions.append(submission)
        exposure.validate_submission(self.plan,tuple(submissions))
        writer=exporter.InputWriter(self.root/'typed')
        descriptor=exporter.proof_descriptor(self.chain,
            {'protocol':cli.SCOPE_PROTOCOL,'purpose':'offline review mechanics only'})
        packet={'chain':'scope','enrollments':writer.put((),typed=True),
            'submissions':[writer.put(item,typed=True) for item in submissions]}
        self.chain.close();self.head.close()
        from gossip_harness.peer_financial_terminal_v1 import FinancialError
        constructed=[]
        original=review.WorkflowInspectionExecution.__init__
        @wraps(original)
        def forbidden(*args,**kwargs):
            constructed.append(True)
            raise AssertionError('No physical constructor is authorized by incomplete scope')
        with ExitStack() as stack,patch.object(review.WorkflowInspectionExecution,'__init__',forbidden):
            pool=cold.ProofPool({'scope':descriptor},stack)
            with self.assertRaisesRegex(FinancialError,'Complete executable semantic scope unavailable'):
                cold._restore_scope(pool,packet,ROOT)
            self.assertEqual(constructed,[])


class CumulativeWorkflowHostCompositionV1Tests(unittest.TestCase):
    """Real source and journals; only full semantic/prerequisite callbacks isolated.

    Passing these method controls is not an independently approved product or
    complete capsule. The separate real constructor/cold-scope tests above must
    still refuse missing authority. No candidate Python or Engine is executed.
    """
    setUpClass=classmethod(review_tests.CandidateWorkflowInspectionTests.setUpClass.__func__)
    deliver=review_tests.CandidateWorkflowInspectionTests.deliver

    def setUp(self):
        review_tests.CandidateWorkflowInspectionTests.setUp(self)
        self.source_plan=self.plan
        self.plan=exposure.expose_plan(cli.expose_plan(synthetic_plan()))
        self.spec=shared.ObservationSpec(kind='workflow_inspection',root=self.raw,delta_root=self.delta,
            cleanup_root=self.root/'cleanup',store=self.store,registration=self.registration,policy=None,
            checkpoint_authority=self.head,profile=self.value,layout_plan=self.source_plan,layout_authority=self.authority)
        self.assertEqual(self.spec.observation_registration(),review.inspection_observation_registration(self.registration))
        raw,delta=self.root/'final-raw',self.root/'final-delta'
        self.final_head=ExternalHead.create(self.root/'final-head',journal_roots=(raw,delta))
        self.addCleanup(self.final_head.close)
        self.final_chain=checkpoint.CheckpointChain.create(raw,delta,
            context={'synthetic_phase_boundary':True,'semantic_approval':False},authority=self.final_head)
        self.addCleanup(self.final_chain.close)
        owner=object.__new__(final.FinalAcceptanceV3)
        owner.plan,owner.repository,owner.chain=self.plan,ROOT,self.final_chain
        owner.expected,owner.study_expected=self.final_chain.commitment,self.final_chain.commitment
        owner.records,owner.enrollments,owner.dispatch_halt=Records(self.final_chain),{},None
        owner.closed=False;owner.freeze=self.state['freeze'];owner.subjects=owner.freeze.subjects
        owner.originals=SimpleNamespace(slots=(SimpleNamespace(trajectory=self.subject.trajectory_id,
            final_source={'repository':str(self.store.path.resolve())}),))
        owner.submissions={self.subject.trajectory_id:SimpleNamespace(subject=self.subject)}
        owner.scope_snapshot=SimpleNamespace(registration=lambda subject:None)
        provenance=consumer.Revision(self.commit,self.tree,self.subject.source_sha256)
        def current():
            exposure.validate_plan(owner.plan,ROOT)
            owner.chain.validate_boundary(expected=owner.expected)
            admission.require(self.state['registration']==self.spec.observation_registration(),'Synthetic authority revoked')
            admission.require(owner.freeze==self.state['freeze'],'Synthetic freeze revoked')
        def registered(registration):
            current()
            admission.require(registration==self.state['registration'],'Synthetic registration differs')
            return provenance
        owner._current=current;owner._current_originals=current
        owner._registered_gate=registered
        owner._dispatch_prerequisites=lambda spec:registered(spec.observation_registration())
        owner._protected_roots=lambda:[self.final_chain.raw_root,self.final_chain.delta_root,self.final_head.root]
        # These deliberately absent authorities are never serialized as granted.
        owner.prerequisite_owner=None;owner.promotion_owner=None;owner.control_owners=()
        self.final_owner=owner
        owner._put('final.freeze',{'synthetic_method_fixture_only':True})

    def begin(self):
        request=self.final_owner.begin_workflow_inspection(self.spec)
        self.key='final.observation.'+registry.fingerprint(self.registration.gate)
        row=self.final_owner.enrollments[self.key]
        self.addCleanup(row.owner.close)
        self.assertEqual(row.phase,'bound')
        self.assertEqual(row.owner.read_authenticated('review-request.json'),review.encoded(request))
        self.assertFalse(row.owner.has_retained('terminal.json'))
        return request

    def test_actual_two_phase_original_review_observer_and_readonly_reopen(self):
        request=self.begin()
        with self.assertRaises(consumer.AuthorityError):self.final_owner.open_snapshot()
        delivery=self.deliver(request)
        result=self.final_owner.complete_workflow_inspection(self.registration.gate,delivery)
        row=self.final_owner.enrollments[self.key]
        self.assertEqual(row.phase,'complete')
        self.assertEqual(tuple(item.case_id for item in result.execution.outcomes),self.value.ordered_case_ids)
        self.assertTrue(all(item.status=='passed' for item in result.execution.outcomes))
        raw=row.owner.read_original()
        self.assertFalse(raw['engine_used']);self.assertFalse(raw['mechanics']['cleanup_required'])
        expected=row.owner.checkpoint();row.owner.close()
        reopened=review.WorkflowInspectionExecution(self.raw,self.store,self.registration,value=self.value,
            plan=self.source_plan,review_authority=self.authority,admission_authority=row.admission_owner,
            checkpoint_authority=self.head,delta_root=self.delta,expected_checkpoint=expected,product_delivery=delivery)
        self.addCleanup(reopened.close)
        self.assertEqual(observer.WorkflowInspectionObservationSource(reopened,expected).observation(
            self.registration.gate,self.final_owner.freeze),result)
        self.assertEqual(reopened.checkpoint(),expected)
        with self.assertRaises(consumer.AuthorityError):
            self.final_owner.complete_workflow_inspection(self.registration.gate,delivery)
        self.assertIsNone(self.final_owner.records.read('final.assessment'))

    def test_product_rejection_survives_unknown_sibling_without_fabricated_acceptance(self):
        request=self.begin()
        decisions=[{'id':duty,'decision':'rejected' if index==0 else 'unresolved' if index==1 else 'approved',
            'rationale':'Explicit synthetic test decision, no semantic approval','source_references':['fixture:1']}
            for index,duty in enumerate(review.INSPECTION_DUTIES)]
        result=self.final_owner.complete_workflow_inspection(self.registration.gate,self.deliver(request,decisions=decisions))
        self.assertEqual([row.status for row in result.execution.outcomes][:2],['failed','infrastructure_error'])
        retained=self.final_owner.records.read(self.key+'.verified')
        self.assertEqual(retained['observation'],plain(asdict(result)))
        self.assertTrue(retained['history']['missing_step_ids'])
        self.assertIsNone(self.final_owner.records.read('final.assessment'))

    def test_synchronous_route_and_changed_freeze_refuse_before_completion_or_retry(self):
        with self.assertRaisesRegex(consumer.AuthorityError,'explicit begin/complete'):
            self.final_owner.dispatch(self.spec)
        self.assertFalse(self.raw.exists())
        request=self.begin();delivery=self.deliver(request)
        self.state['freeze']=replace(self.state['freeze'],receipt_sha256='9'*64)
        with self.assertRaises(consumer.AuthorityError):
            self.final_owner.complete_workflow_inspection(self.registration.gate,delivery)
        self.assertFalse(self.final_owner.enrollments[self.key].owner.has_retained('terminal.json'))
        with self.assertRaises(consumer.AuthorityError):self.final_owner.begin_workflow_inspection(self.spec)

    def test_original_product_delivery_drift_between_phases_cannot_complete(self):
        request=self.begin();delivery=self.deliver(request)
        delivery.journal.retain('revocation.json',b'{}')
        with self.assertRaises((consumer.AuthorityError,consumer.AuthorityUnavailable)):
            self.final_owner.complete_workflow_inspection(self.registration.gate,delivery)
        row=self.final_owner.enrollments[self.key]
        self.assertEqual(row.phase,'unavailable')
        self.assertIsNotNone(self.final_owner.dispatch_halt)
        self.assertFalse(row.owner.has_retained('terminal.json'))

    def test_actual_cold_host_reader_reconstructs_both_live_proof_owners_and_no_dispatch(self):
        request=self.begin();delivery=self.deliver(request)
        observed=self.final_owner.complete_workflow_inspection(self.registration.gate,delivery)
        row=self.final_owner.enrollments[self.key]
        # This chronology marker only permits isolated cold observation reading;
        # it is explicitly not an accepted assessment and cannot pass audit_final_originals.
        self.final_owner._put('final.assessment',{'synthetic_method_fixture_only':True,'accepted':False})
        writer=exporter.InputWriter(self.root/'cold-inputs')
        inputs={name:getattr(self.spec,name) for name in ('kind','root','delta_root','cleanup_root',
            'registration','policy','recipe','profile','cumulative_profile','endpoint','layout_plan')}
        inputs['store']=self.store.path.resolve()
        context={'fixture_only':True,'semantic_approval':False}
        proofs={'mechanism':exporter.proof_descriptor(self.authority.journal,context),
            'product':exporter.proof_descriptor(delivery.journal,context)}
        value={'inputs':writer.put(inputs,typed=True),'proof':{'raw':str(self.raw),'delta':str(self.delta),
            'head':str(self.head.root),'expected':asdict(row.post_checkpoint)},
            'layout_review':{'chain':'mechanism','enrollment':writer.put(self.authority.enrollment,typed=True)},
            'product_review':{'chain':'product','expected':asdict(delivery.expected),
                'enrollment':writer.put(delivery.enrollment,typed=True)}}
        row.owner.close();self.head.close()
        for authority in (self.authority,delivery):
            authority.journal.close();authority.journal.authority.close()
        # Exact missing-scope guard is checked first, with no physical constructor.
        from gossip_harness.peer_financial_terminal_v1 import FinancialError
        with ExitStack() as stack:
            pool=cold.ProofPool(proofs,stack)
            with self.assertRaisesRegex(FinancialError,'complete scope registration missing'):
                cold._observation(self.final_owner,self.key,value,pool,self.registration.gate,self.final_owner.freeze)
        # Explicit method isolation only. __wrapped__ preserves source introspection;
        # the real loaded-code and every original observer/checkpoint check still run.
        prefix=cold._prerequisite_prefix
        @wraps(prefix)
        def absent_semantic_boundary(*args):
            return None
        with ExitStack() as stack,patch.object(cold,'_prerequisite_prefix',absent_semantic_boundary):
            pool=cold.ProofPool(proofs,stack)
            actual=cold._observation(self.final_owner,self.key,value,pool,self.registration.gate,self.final_owner.freeze)
            self.assertEqual(actual,observed)
            pool.current()
        self.assertFalse(self.root.joinpath('cleanup').exists())
        self.assertFalse(self.final_owner.records.read('final.assessment')['accepted'])

    def test_closed_product_types_refuse_all_qualification_dataclasses_on_pack_and_unpack(self):
        from gossip_harness import candidate_workflow_execution_v1 as execution
        from gossip_harness.peer_financial_terminal_v1 import FinancialError
        for cls in (execution.WorkflowQualificationProfile,execution.WorkflowQualificationBinding,
                    execution.WorkflowQualificationRegistration):
            name=cls.__module__+'.'+cls.__qualname__
            self.assertNotIn(name,codec.types())
            with self.subTest(cls=name),self.assertRaisesRegex(FinancialError,'Unknown rehearsal input type'):
                codec.pack(object.__new__(cls))
            with self.subTest(tag=name),self.assertRaisesRegex(FinancialError,'closed source contract'):
                codec.unpack({'protocol':codec.PROTOCOL,'input':{'tag':'dataclass','type':name,'value':{}}})
        value={'registration':self.registration,'profile':self.value,'plan':self.source_plan}
        self.assertEqual(codec.unpack(codec.pack(value)),value)
        qualifier=execution.WorkflowQualificationProfile(execution.QUALIFICATION_IDS[0])
        foreign=object.__new__(execution.WorkflowQualificationRegistration)
        spec=replace(self.spec,kind='workflow',registration=foreign,policy=execution.WorkflowPolicy(),profile=qualifier)
        with self.assertRaises(consumer.AuthorityError):spec.observation_registration()
        with self.assertRaises(ValueError):source.workflow_slice(foreign)
        with self.assertRaises(consumer.AuthorityError):self.final_owner.dispatch(spec)
        self.assertFalse(self.raw.exists());self.assertFalse(self.final_owner.enrollments)

    def test_actual_gate_specific_purpose_conversions_remain_independently_required(self):
        from gossip_harness import candidate_workflow_profile_v1 as profiles
        from gossip_harness import candidate_workflow_execution_v1 as execution
        value=profiles.profile_for(profiles.ORIGINAL_CASE_IDS[0],'independent_acceptance')
        boundary=review.WorkflowBoundary('fixture-return','library/__init__.py','solve',1,'return',None,None,'solve_exit')
        source_plan=replace(self.source_plan,profile_kind='workflow',case_id=value.case_id,
            profile_sha256=value.sha256,boundaries=(boundary,))
        authority,journal,head=review_tests.synthetic_delivery(self.root/'runtime-mechanism',source_plan.request())
        self.addCleanup(head.close);self.addCleanup(journal.close)
        policy=execution.WorkflowPolicy()
        binding=execution.binding_for(self.files,value,policy,{'fixture_runtime_only':True},source_plan,review_authority=authority)
        registration=execution.WorkflowRegistration(binding,self.commit,self.tree,'fresh-runtime',
            execution.gate_for(self.subject,binding,gate_id='workflow-runtime'),review_tests.COHORT)
        component=source.workflow_slice(registration)
        catalog=source.load_catalog(ROOT)
        declaration=source.assemble_declaration(catalog,self.plan.cohort,(component,),review_sha256='d'*64,
            capacity_profile=registry.HISTORY_CAPACITY_PROFILE)
        suite,gate,_=component.compiler_records(suite_id='original-workflow',physical_slot='original-workflow')
        self.assertEqual(suite.definition_purpose,'public_development')
        self.assertIn('M4-COMPATIBILITY:clause:2',{row.obligation_id for row in component.assertions})
        self.assertIn('M4-COMPATIBILITY.public-contract',gate.logical_gate_ids)
        logical={row.id:row for row in catalog.inventory.logical_gates if row.id in gate.logical_gate_ids}
        public=next(row for row in logical.values() if row.original_purpose=='public_development')
        adapter=next(row for row in logical.values() if row.original_purpose=='public_contract_regression')
        # Use the matching prospective subject only for compiler lineage; no complete plan is fabricated.
        subject=replace(self.subject,cohort_id=self.plan.cohort.cohort_id,
            trajectory_id=self.plan.roster.children[0].trajectory,execution_contract_sha256=self.plan.sha256)
        compiled=compiler.compile_design(catalog.inventory,declaration,subject)
        missing={row.target for row in compiled.blockers if row.code=='missing_purpose_mapping'}
        self.assertIn(public.id,missing);self.assertIn(adapter.id,missing)
        wrong=compiler.PurposeAssignment(adapter.id,'public_development','independent_acceptance','d'*64)
        with self.assertRaisesRegex(compiler.CompilerError,'Purpose mapping mismatch'):
            compiler.compile_design(catalog.inventory,replace(declaration,purposes=(wrong,)),subject)
        exact=tuple(compiler.PurposeAssignment(row.id,row.original_purpose,'independent_acceptance','d'*64)
            for row in (public,adapter))
        for present,absent in ((exact[0],adapter),(exact[1],public)):
            partial=compiler.compile_design(catalog.inventory,replace(declaration,purposes=(present,)),subject)
            missing={row.target for row in partial.blockers if row.code=='missing_purpose_mapping'}
            self.assertIn(absent.id,missing);self.assertNotIn(present.logical_gate_id,missing)
        amended=compiler.compile_design(catalog.inventory,replace(declaration,purposes=exact),subject)
        self.assertTrue(amended.blockers);self.assertIsNone(amended.registry)
        self.assertFalse({public.id,adapter.id} & {row.target for row in amended.blockers if row.code=='missing_purpose_mapping'})

    def test_prerequisite_original_target_cannot_drop_any_of_three_identities(self):
        owner=object.__new__(prerequisite.PrerequisiteQualification);owner.owner=self.final_owner
        component=source.workflow_inspection_slice(self.registration)
        fields={**cli.contract_fields(self.plan),**exposure.contract_fields(self.plan)}
        owner._target_contract(component,fields)
        for key in fields:
            with self.subTest(key=key),self.assertRaises(consumer.AuthorityError):
                owner._target_contract(component,{name:value for name,value in fields.items() if name!=key})
        self.final_owner.plan=cli.expose_plan(synthetic_plan())
        with self.assertRaises(consumer.AuthorityError):owner._target_contract(component,fields)

    def test_exact_nested_capture_points_roundtrip_without_plain_dict_or_unknown_role(self):
        from gossip_harness import candidate_workflow_profile_v1 as profiles
        value=profiles.profile_for('WF19-provisional-fault-boundary','independent_acceptance')
        boundary=review.WorkflowBoundary('fixture-role-events','library/__init__.py','solve',1,
            'line',None,None,'post_fault',4)
        points=tuple(review.WorkflowCapturePoint(role,0,boundary.id,index)
            for index,role in enumerate(('initial','post_fault','reopened','final')))
        plan=replace(self.source_plan,profile_kind='workflow',case_id=value.case_id,profile_sha256=value.sha256,
            boundaries=(boundary,),capture_points=points)
        # Transport only: these positions are never offered as real source approval.
        encoded=codec.pack(plan);decoded=codec.unpack(encoded)
        self.assertEqual(decoded,plan)
        self.assertTrue(all(type(point) is review.WorkflowCapturePoint for point in decoded.capture_points))
        nested=encoded['input']['value']['capture_points']['value'][0]
        nested['tag']='dict';nested.pop('type')
        with self.assertRaises(ValueError):codec.unpack(encoded)
        with self.assertRaises(ValueError):replace(points[0],role='arbitrary-last-row')
        from gossip_harness import candidate_workflow_execution_v1 as execution
        authority,journal,head=review_tests.synthetic_delivery(self.root/'capture-mechanism',plan.request())
        self.addCleanup(head.close);self.addCleanup(journal.close)
        binding=execution.binding_for(self.files,value,execution.WorkflowPolicy(),{'fixture_runtime_only':True},
            plan,review_authority=authority)
        registration=execution.WorkflowRegistration(binding,self.commit,self.tree,'fresh-capture',
            execution.gate_for(self.subject,binding,gate_id='workflow-conservation'),review_tests.COHORT)
        component=source.workflow_slice(registration)
        source.verify_slice(component)
        capture_ids={row['case_id'] for row in profiles.capture_selectors(value.case_id)}
        self.assertEqual(len(capture_ids),16)
        selected=[row for row in component.selectors if row.case_id in capture_ids]
        self.assertEqual(len(selected),16)
        self.assertTrue(all(row.evidence_kind=='semantic' for row in selected))
        assertions=[row for row in component.assertions if row.case_id in capture_ids]
        self.assertEqual({row.obligation_id for row in assertions},{'m1:M1-ADAPTER-03'})
        self.assertTrue(all(row.assertion.kind=='history' for row in assertions))

    def recipe_fixture(self, kind, name):
        from tests.test_cumulative_workflow_recipe_v1 import recipe_fixture
        stack=ExitStack();self.addCleanup(stack.close)
        fixture=recipe_fixture(self.root/name,stack,kind,store=self.store)
        return fixture,stack

    def method_owner(self, fixture, stack):
        """Only missing scope/terminal/qualification callbacks are isolated."""
        context={'synthetic_phase_boundary':True,'semantic_approval':False}
        raw,delta=fixture.root/'final-raw',fixture.root/'final-delta'
        head=ExternalHead.create(fixture.root/'final-head',journal_roots=(raw,delta));stack.callback(head.close)
        journal=checkpoint.CheckpointChain.create(raw,delta,context=context,authority=head);stack.callback(journal.close)
        owner=object.__new__(final.FinalAcceptanceV3)
        owner.plan,owner.repository,owner.chain=self.plan,ROOT,journal
        owner.expected=journal.commitment;owner.study_expected=journal.commitment
        owner.records,owner.enrollments,owner.dispatch_halt=Records(journal),{},None
        owner.closed=False;owner.freeze=fixture.state['freeze'];owner.subjects=owner.freeze.subjects
        owner.originals=SimpleNamespace(slots=(SimpleNamespace(trajectory=fixture.subject.trajectory_id,
            final_source={'repository':str(fixture.store.path.resolve())}),))
        owner.submissions={fixture.subject.trajectory_id:SimpleNamespace(subject=fixture.subject)}
        owner.scope_snapshot=SimpleNamespace(registration=lambda subject:None)
        provenance=consumer.Revision(fixture.commit,fixture.tree,fixture.subject.source_sha256)
        def current():
            exposure.validate_plan(owner.plan,ROOT);journal.validate_boundary(expected=owner.expected)
            admission.require(fixture.state['registration']==fixture.spec.observation_registration(),'Synthetic registration revoked')
            admission.require(fixture.state['freeze']==owner.freeze,'Synthetic complete freeze revoked')
        def registered(value):
            current();admission.require(value==fixture.state['registration'],'Synthetic gate differs')
            return provenance
        owner._current=current;owner._current_originals=current;owner._registered_gate=registered
        owner._dispatch_prerequisites=lambda spec:registered(spec.observation_registration())
        owner._protected_roots=lambda:[journal.raw_root,journal.delta_root,head.root]
        owner.prerequisite_owner=None;owner.promotion_owner=None;owner.control_owners=()
        owner._put('final.freeze',{'synthetic_method_fixture_only':True})
        return owner,{journal.commitment.context_sha256:context}

    def export_packet(self, fixture, stack, owner, contexts, *, delivery=None):
        """Real exporter over unaccepted originals and genuinely incomplete scope.

        Empty actual review authorities and uninitialized prerequisite/promotion
        owners provide export descriptors only. No evidence/approval method on
        those owners is called; the real cold full-scope path must still refuse.
        """
        import sqlite3
        from gossip_harness import cumulative_prerequisite_review_v1 as reviews
        from gossip_harness import cumulative_source_promotion_v1 as promotion
        from gossip_harness import cumulative_rehearsal_capsule_v2 as capsule
        from gossip_harness.peer_financial_authority_v2 import ledger_identity
        def new_chain(name):
            root=fixture.root/name;root.mkdir(mode=0o700);raw,delta=root/'raw',root/'delta'
            context={'synthetic_export':name,'semantic_approval':False}
            head=ExternalHead.create(root/'head',journal_roots=(raw,delta));stack.callback(head.close)
            chain=checkpoint.CheckpointChain.create(raw,delta,context=context,authority=head);stack.callback(chain.close)
            contexts[chain.commitment.context_sha256]=context
            return chain
        owner.study_chain=new_chain('study')
        scope_chain=new_chain('scope')
        owner.scope_owner=scope.ScopeRegistrationController(ROOT,scope_chain,scope_chain.commitment)
        catalog=source.load_catalog(ROOT)
        cohort=replace(self.plan.cohort,cohort_id=fixture.subject.cohort_id,
            trajectories=tuple(replace(row,id=subject.trajectory_id) for row,subject in
                zip(self.plan.cohort.trajectories,owner.subjects,strict=True)))
        owner.submissions={}
        for subject in owner.subjects:
            original=fixture.spec.registration
            gate=replace(original.gate,binding=replace(original.gate.binding,subject=subject))
            registration=replace(original,gate=gate)
            component=(source.workflow_slice(registration) if fixture.spec.kind=='workflow'
                else source.workflow_inspection_slice(registration))
            declaration=source.assemble_declaration(catalog,cohort,(component,),review_sha256='d'*64,
                capacity_profile=registry.HISTORY_CAPACITY_PROFILE)
            submission=scope.ScopeSubmission(catalog,declaration,source.scope_review_input(catalog,declaration),
                subject,(component,),(),cli_projection_contract=cli.manifest(),
                workflow_projection_contract=exposure.manifest(),cumulative_public_contract=exposure.combined_manifest())
            staged=owner.scope_owner.stage(submission)
            self.assertTrue(staged.design.blockers);self.assertIsNone(staged.design.registry)
            owner.submissions[subject.trajectory_id]=submission
        owner.scope_snapshot=owner.scope_owner.open_snapshot()
        owner.prerequisite_owner=object.__new__(prerequisite.PrerequisiteQualification)
        owner.prerequisite_owner.store=fixture.store
        journal=new_chain('admission-reviews')
        owner.prerequisite_owner.reviews=reviews.OriginalReviewAuthority(journal,journal.commitment)
        owner.promotion_owner=object.__new__(promotion.SourcePromotion)
        journal=new_chain('promotion-reviews')
        owner.promotion_owner.reviews=reviews.OriginalReviewAuthority(journal,journal.commitment)
        wallet=fixture.root/'empty-wallet.sqlite'
        with sqlite3.connect(wallet) as db:db.execute('CREATE TABLE fixture_only(id INTEGER)')
        owner.ledger_identity=ledger_identity(wallet)
        contexts[fixture.mechanism_journal.commitment.context_sha256]=fixture.mechanism_context
        if delivery is not None:
            contexts[delivery.journal.commitment.context_sha256]=dict(fixture.mechanism_context)
        owner._put('final.assessment',{'synthetic_method_fixture_only':True,'accepted':False})
        from gossip_harness.peer_financial_terminal_v1 import FinancialError
        with self.assertRaisesRegex(FinancialError,'outside all candidate'):
            exporter.export_final_packet(owner,(fixture.spec,),destination=fixture.raw/'forbidden',contexts=contexts)
        with self.assertRaisesRegex(FinancialError,'slot census'):
            exporter.export_final_packet(owner,(),destination=fixture.root/'omitted-census',contexts=contexts)
        reference=exporter.export_final_packet(owner,(fixture.spec,),destination=fixture.root/'export',contexts=contexts)
        packet=capsule.bound(reference)
        self.assertEqual(set(packet['observations']),set(owner.enrollments))
        self.assertEqual(len(packet['scope']['submissions']),6)
        for name,value in {**cli.contract_fields(self.plan),**exposure.contract_fields(self.plan)}.items():
            self.assertEqual(packet[name],value)
            changed={**packet,name:{'substituted':True}}
            with self.assertRaisesRegex(FinancialError,'combined requirement identity'):
                cold.audit_final_originals(changed,plan=self.plan)
        key='final.observation.'+registry.fingerprint(fixture.spec.registration.gate)
        record=packet['observations'][key]
        self.assertEqual(codec.read(record['layout_review']['enrollment']),fixture.authority.enrollment)
        self.assertEqual(codec.read(record['inputs'])['endpoint'],owner.enrollments[key].owner.endpoint
            if fixture.spec.kind=='workflow' else None)
        if delivery is not None:
            self.assertEqual(record['product_review']['expected'],asdict(delivery.expected))
            self.assertEqual(codec.read(record['product_review']['enrollment']),delivery.enrollment)
        return packet,key,record

    def cold_packet(self, fixture, owner, packet, key, record, observed, *, delivery=None, runtime_patch=None):
        from gossip_harness.peer_financial_terminal_v1 import FinancialError
        row=owner.enrollments[key]
        before=row.owner.checkpoint()
        self.assertEqual(row.post_checkpoint,before)
        row.owner.close();fixture.head.close()
        fixture.mechanism_journal.close();fixture.mechanism_head.close()
        if delivery is not None:
            delivery.journal.close();delivery.journal.authority.close()
        # Actual missing full registration rejects before any physical owner.
        with ExitStack() as stack:
            pool=cold.ProofPool(packet['proofs'],stack)
            with self.assertRaisesRegex(FinancialError,'complete scope registration missing'):
                cold._observation(owner,key,record,pool,fixture.spec.registration.gate,owner.freeze)
        prefix=cold._prerequisite_prefix
        @wraps(prefix)
        def method_only_missing_authority(*args):
            return None
        with ExitStack() as stack,patch.object(cold,'_prerequisite_prefix',method_only_missing_authority):
            if runtime_patch is not None:stack.enter_context(runtime_patch())
            pool=cold.ProofPool(packet['proofs'],stack)
            actual=cold._observation(owner,key,record,pool,fixture.spec.registration.gate,owner.freeze)
            self.assertEqual(actual,observed);pool.current()
            self.assertEqual(owner.enrollments[key].post_checkpoint,before)
        # Borrowed capabilities cannot survive closure; no cached review pass.
        with self.assertRaises((consumer.AuthorityError,consumer.AuthorityUnavailable,ValueError)):
            fixture.authority.authenticate(fixture.plan)
        self.assertFalse(owner.records.read('final.assessment')['accepted'])

    def test_both_actual_recipes_capsule_bound_codec_normalization_and_identity_refusal(self):
        from gossip_harness import cumulative_rehearsal_capsule_v2 as capsule
        from gossip_harness.peer_financial_terminal_v1 import FinancialError
        from tests.test_cumulative_workflow_recipe_v1 import typed_inputs,restore_spec
        for kind in exposure.KINDS:
            fixture,_=self.recipe_fixture(kind,'capsule-'+kind)
            writer=exporter.InputWriter(fixture.root/'inputs')
            fields={**cli.contract_fields(self.plan),**exposure.contract_fields(self.plan)}
            reference=writer.put({**fields,'inputs':writer.put(typed_inputs(fixture),typed=True),
                'full_capsule_qualified':False})
            packet=capsule.bound(reference)
            self.assertEqual(Path(reference['path']).read_bytes(),codec.encoded(packet))
            spec=restore_spec(fixture,codec.read(packet['inputs']))
            self.assertEqual(spec.observation_registration(),fixture.state['registration'])
            exposure.validate_spec(self.plan,spec)
            with self.assertRaises(FinancialError):capsule.bound({**reference,'sha256':'0'*64})
            altered=Path(reference['path']).with_name('noncanonical.json')
            altered.write_bytes(b' '+Path(reference['path']).read_bytes())
            with self.assertRaisesRegex(FinancialError,'Canonical closed'):
                capsule.bound({'path':str(altered),'sha256':hashlib.sha256(altered.read_bytes()).hexdigest()})
            with self.assertRaises(consumer.AuthorityError):
                replace(spec,kind='workflow_inspection' if kind=='workflow' else 'workflow').observation_registration()
            with self.assertRaises(consumer.AuthorityError):
                replace(spec,profile=replace(spec.profile,purpose='repeatability')).observation_registration()
            self.assertFalse(fixture.raw.exists())

    def test_actual_host_export_and_cold_consume_exported_protected_original_descriptors(self):
        fixture,stack=self.recipe_fixture('workflow_inspection','export-host')
        owner,contexts=self.method_owner(fixture,stack)
        request=owner.begin_workflow_inspection(fixture.spec)
        key='final.observation.'+registry.fingerprint(fixture.spec.registration.gate)
        stack.callback(owner.enrollments[key].owner.close)
        delivery,journal,head=review_tests.synthetic_delivery(fixture.root/'product',request,product=True)
        stack.callback(head.close);stack.callback(journal.close)
        observed=owner.complete_workflow_inspection(fixture.spec.registration.gate,delivery)
        packet,key,record=self.export_packet(fixture,stack,owner,contexts,delivery=delivery)
        self.cold_packet(fixture,owner,packet,key,record,observed,delivery=delivery)

    def product_dispatch(self, fixture, owner, stack):
        from gossip_harness import candidate_workflow_execution_v1 as execution
        fixture.spec=replace(fixture.spec,endpoint=execution.process.EngineEndpoint('/inert-workflow.sock',1,2))
        original_runtime=execution.process.runtime_identity
        @wraps(original_runtime)
        def injected_runtime(*args,**kwargs):
            return {'kind':'fixture-no-Docker'}
        def runtime_patch():
            return patch.object(execution.process,'runtime_identity',injected_runtime)
        original_dispatch=execution.CandidateWorkflowExecution._dispatch
        @wraps(original_dispatch)
        def injected_dispatch(actual,intent):
            _InjectedWorkflowOriginals(actual).dispatch(intent)
        with runtime_patch(),patch.object(execution.CandidateWorkflowExecution,'_dispatch',injected_dispatch):
            observed=owner.dispatch(fixture.spec)
        key='final.observation.'+registry.fingerprint(fixture.spec.registration.gate)
        stack.callback(owner.enrollments[key].owner.close)
        self.assertEqual(owner.enrollments[key].phase,'complete')
        self.assertTrue(owner.enrollments[key].history['cleanup_verified'])
        self.assertEqual(owner.enrollments[key].history['infrastructure'],[])
        self.assertEqual(owner.enrollments[key].history['missing_step_ids'],[])
        self.assertTrue(any(row.status=='failed' for row in observed.execution.outcomes))
        self.assertEqual(observed.execution.outcomes[-1].status,'passed')
        return key,observed,runtime_patch

    def test_actual_product_export_cold_raw_observer_retains_failure_and_refuses_closed_source(self):
        fixture,stack=self.recipe_fixture('workflow','export-product')
        owner,contexts=self.method_owner(fixture,stack)
        key,observed,runtime_patch=self.product_dispatch(fixture,owner,stack)
        original=owner.enrollments[key].owner
        record=observer.reconstruct(original)
        self.assertEqual(record['mechanics']['status'],'passed')
        self.assertEqual(record['original_intent_sha256'],hashlib.sha256(original.read_authenticated('intent.json')).hexdigest())
        self.assertEqual(record['original_terminal_sha256'],hashlib.sha256(original.read_authenticated('terminal.json')).hexdigest())
        source_=observer.WorkflowObservationSource(original,original.checkpoint())
        packet,key,descriptor=self.export_packet(fixture,stack,owner,contexts)
        self.cold_packet(fixture,owner,packet,key,descriptor,observed,runtime_patch=runtime_patch)
        with self.assertRaises((consumer.AuthorityError,consumer.AuthorityUnavailable)):
            source_.observation(fixture.spec.registration.gate,owner.freeze)

    def test_product_original_missing_verifier_is_rejected_without_republication(self):
        from gossip_harness.peer_financial_terminal_v1 import FinancialError
        fixture,stack=self.recipe_fixture('workflow','missing-verifier')
        owner,contexts=self.method_owner(fixture,stack)
        key,observed,runtime_patch=self.product_dispatch(fixture,owner,stack)
        packet,key,record=self.export_packet(fixture,stack,owner,contexts)
        actual=owner.enrollments[key].owner
        actual.close();fixture.head.close();fixture.mechanism_journal.close();fixture.mechanism_head.close()
        # Method isolation hides only verifier membership, never edits originals.
        # The cold path must deny before reconstruct/publish; no receipt healing.
        from gossip_harness import candidate_execution_journal_v1 as journals
        journal_has=journals.OwnerJournal.has
        @wraps(journal_has)
        def hide_verifier(journal,name):
            return False if name==observer.VERIFIER_FILE else journal_has(journal,name)
        prefix=cold._prerequisite_prefix
        @wraps(prefix)
        def missing_authority(*args):return None
        with ExitStack() as read_stack, runtime_patch(),patch.object(cold,'_prerequisite_prefix',missing_authority),\
                patch.object(journals.OwnerJournal,'has',hide_verifier):
            pool=cold.ProofPool(packet['proofs'],read_stack)
            with self.assertRaisesRegex(FinancialError,'Original physical verifier missing'):
                cold._observation(owner,key,record,pool,fixture.spec.registration.gate,owner.freeze)
        self.assertTrue(any(row.status=='failed' for row in observed.execution.outcomes))

    def prerequisite_originals(self, kind, *, partial):
        from tests.test_cumulative_workflow_recipe_v1 import typed_inputs,restore_spec
        fixture,stack=self.recipe_fixture(kind,('partial-' if partial else 'complete-')+kind)
        owner,_=self.method_owner(fixture,stack)
        writer=exporter.InputWriter(fixture.root/'target-inputs')
        spec=restore_spec(fixture,codec.read(writer.put(typed_inputs(fixture),typed=True)))
        registration=spec.observation_registration();component=fixture.recipe.scope_slice()
        revision=consumer.Revision(fixture.commit,fixture.tree,fixture.subject.source_sha256)
        definition=prerequisite.definition_for('admission',revision,(registration.gate,),
            gate_id='fixture-admission',suite_id='fixture-admission-suite',physical_slot='fixture-admission-slot')
        registered=consumer.RegisteredAcceptance(*(['a'*64]*5),(definition.specification,))
        request=consumer.QualificationRequest(registered,definition.execution,definition.suite,
            definition.specification,definition.product_lineages_sha256)
        producer=object.__new__(prerequisite.PrerequisiteQualification)
        producer.owner,producer.source=owner,revision
        # Explicit method-only registration fixture. Actual source target,
        # guard execution and complete/partial original readers are unpatched.
        producer._resolve=lambda value:(SimpleNamespace(subject=fixture.subject,slices=(component,)),
            SimpleNamespace(gates=(registration.gate,)),definition,revision)
        key=producer._key(request)
        if partial:
            original_put=owner._put
            def stop_after_ack(name,value):
                result=original_put(name,value)
                if name==key+'.target.0':raise RuntimeError('Synthetic interruption after acknowledged target')
                return result
            owner._put=stop_after_ack
            with self.assertRaisesRegex(RuntimeError,'after acknowledged target'):
                producer.execute_once(request,(spec,))
            owner._put=original_put
            before=owner.chain.commitment
            result=producer._partial_execution(request)
            self.assertEqual(result,producer._execution(request))
            self.assertEqual(owner.chain.commitment,before)
            self.assertIsNone(owner.records.read(key+'.execution'))
            self.assertTrue(result['body']['missing_original_terminal'])
            self.assertTrue(any(row['status']=='infrastructure_error' for row in result['outcomes']))
        else:
            result=producer.execute_once(request,(spec,))
            self.assertEqual(producer._execution(request),result)
            self.assertEqual(result['terminal_status'],'completed')
        target=owner.records.read(key+'.target.0')
        self.assertEqual(target['registration'],plain(asdict(registration)))
        self.assertEqual(target['slice_sha256'],component.sha256)
        self.assertTrue(all(row['passed'] for row in target['guards']))
        for name,value in {**cli.contract_fields(self.plan),**exposure.contract_fields(self.plan)}.items():
            self.assertEqual(target[name],value)
        self.assertEqual(result['body']['candidate_effects'],0)
        self.assertFalse(fixture.raw.exists());self.assertIsNone(owner.records.read('final.assessment'))
        with self.assertRaises(consumer.AuthorityError):producer.execute_once(request,(spec,))

    def test_both_actual_prerequisite_factories_and_complete_original_readers(self):
        for kind in exposure.KINDS:
            with self.subTest(kind=kind):self.prerequisite_originals(kind,partial=False)

    def test_both_actual_prerequisite_factories_keep_partial_acknowledged_targets_without_replay(self):
        for kind in exposure.KINDS:
            with self.subTest(kind=kind):self.prerequisite_originals(kind,partial=True)


    def test_actual_workflow_catalog_lanes_preserve_dual_purposes_and_refuse_metadata_substitution(self):
        from copy import deepcopy
        from tests.test_cumulative_workflow_recipe_v1 import recipe_fixture
        catalog=source.load_catalog(ROOT)
        gates={row.id:row for row in catalog.inventory.logical_gates}
        self.assertEqual((gates['M1-GATE-PUBLIC'].lane,gates['M1-GATE-PUBLIC'].original_purpose),
            ('public-contract','public_development'))
        self.assertEqual((gates['M1-GATE-ADAPTER'].lane,gates['M1-GATE-ADAPTER'].original_purpose),
            ('workflow','public_contract_regression'))
        self.assertEqual(gates['M4-COMPATIBILITY.public-contract'].lane,'public-contract')
        self.assertEqual(source.WORKFLOW_LANES,('public-contract','workflow'))
        for case_id in ('inherited-text-persistence','WF19-provisional-fault-boundary'):
            with self.subTest(case=case_id):
                stack=ExitStack();self.addCleanup(stack.close)
                fixture=recipe_fixture(self.root/('lanes-'+case_id),stack,'workflow',store=self.store,case_id=case_id)
                component=fixture.recipe.scope_slice();source.verify_slice(component)
                suite,gate,_=component.compiler_records(suite_id='lane-suite',physical_slot='lane-slot')
                self.assertEqual(suite.capabilities,('public-contract','workflow'))
                self.assertIn('M1-GATE-ADAPTER',gate.logical_gate_ids)
                if case_id=='inherited-text-persistence':
                    self.assertIn('M1-GATE-PUBLIC',gate.logical_gate_ids)
                self.assertFalse({'cli','http','browser','source-inspection'} & set(suite.capabilities))
                record=observer.selector_catalog(case_id,purpose='independent_acceptance')
                self.assertEqual(tuple(record['capabilities']),suite.capabilities)
                for selector in record['selectors']:
                    if selector['evidence_kind']=='mechanics':continue
                    declared=tuple(dict.fromkeys(key for facet in selector['source_unit_facets']
                        for key in facet['logical_gate_ids']))
                    self.assertEqual(tuple(selector['logical_gate_ids']),declared)
                    self.assertEqual(set(selector['lanes']),{gates[key].lane for key in declared})
                original_catalog=observer.selector_catalog
                for alteration in ('missing-lane','foreign-lane','dropped-gate','foreign-capability'):
                    @wraps(original_catalog)
                    def changed_catalog(*args,**kwargs):
                        altered=deepcopy(original_catalog(*args,**kwargs))
                        semantic=next(row for row in altered['selectors'] if row['evidence_kind']!='mechanics')
                        if alteration=='missing-lane':semantic['lanes']=[]
                        elif alteration=='foreign-lane':semantic['lanes']=['http']
                        elif alteration=='dropped-gate':semantic['logical_gate_ids']=[]
                        else:altered['capabilities']=['public-contract','workflow','http']
                        return altered
                    with self.subTest(alteration=alteration),patch.object(observer,'selector_catalog',changed_catalog):
                        with self.assertRaisesRegex(ValueError,'metadata differs|closed family lanes'):
                            source.workflow_slice(fixture.recipe.registration)
        host,_=self.recipe_fixture('workflow_inspection','host-lanes')
        component=host.recipe.scope_slice();source.verify_slice(component)
        suite,_,_=component.compiler_records(suite_id='host-lane-suite',physical_slot='host-lane-slot')
        self.assertEqual(suite.capabilities,('source-inspection',))
        self.assertEqual(observer.inspection_selector_catalog('independent_acceptance')['capabilities'],['source-inspection'])


class _InjectedWorkflowOriginals:
    """Synthetic Engine frames over an actual admitted owner; never physical proof.

    This finite fixture replaces only the effectful _dispatch method. The actual
    constructor, execute_once intent, raw journal, verifier, final admission and
    cold reconstruction remain in the production path. Candidate code is inert.
    Every returned value is deliberately wrong, preserving a product failure.
    """
    def __init__(self, owner):
        self.owner = owner

    def command(self, label, argv, raw):
        from tests.test_candidate_workflow_observation_v1 import CandidateWorkflowOriginalReaderTests
        actual = self.owner.docker + argv[1:]
        return CandidateWorkflowOriginalReaderTests.command(self, label, actual, raw)

    def guard(self, label, side):
        from tests.test_candidate_workflow_observation_v1 import CandidateWorkflowOriginalReaderTests
        return CandidateWorkflowOriginalReaderTests.guard(self, label, side)

    def engine(self, label, *, first=False, pid=17):
        from tests.test_candidate_workflow_observation_v1 import CandidateWorkflowOriginalReaderTests
        return CandidateWorkflowOriginalReaderTests.engine(self, label, first=first, pid=pid)

    def capture(self, label):
        from tests.test_candidate_workflow_observation_v1 import CandidateWorkflowOriginalReaderTests
        return CandidateWorkflowOriginalReaderTests.capture(self, label)

    def retain(self, name, value):
        from gossip_harness import candidate_workflow_execution_v1 as execution
        self.owner._retain(name, execution.encoded(value))

    def dispatch(self, intent):
        from copy import deepcopy
        from gossip_harness import candidate_workflow_execution_v1 as execution
        from gossip_harness import candidate_workflow_profile_v1 as profiles
        from tests.test_candidate_storage_prestart_v1 import created_fixture
        from tests.test_candidate_workflow_observation_v1 import http_original
        from tests.test_candidate_workflow_execution_v1 import exec_value
        owner = self.owner
        self.created, expected = created_fixture(inputs=True)
        self.cid = expected['container_id']
        expected.update(name=intent['container'], volume=intent['volume'], image_id=owner.policy.image_id,
            labels={'gossip.execution':intent['execution_id'],'gossip.source':owner.binding.source_sha256,
                    'gossip.fixture':owner.binding.fixture_sha256})
        self.created.update(Name='/'+expected['name'], Image=expected['image_id'])
        self.created['Config'].update(Image=expected['image_id'],Labels=expected['labels'])
        self.created['Mounts'][-1].update(Name=expected['volume'],
            Source='/var/lib/docker/volumes/'+expected['volume']+'/_data')
        self.created['HostConfig']['Mounts'][-1]['Source']=expected['volume']
        self.stage={'source_manifest':admission.source_manifest(owner.files),
            'helper_manifest':admission.source_manifest(execution.adapter_files(owner.profile.case_id,owner.plan)),
            'fixtures_sha256':execution.digest(admission.source_manifest(profiles.input_files(owner.profile.case_id)))}
        stage={**{name:expected['mounts'][target] for name,target in
            (('workspace','/workspace'),('checks','/checks'),('inputs','/inputs'))},
            'source_manifest':self.stage['source_manifest'],'proof':self.stage}
        self.retain('staging.json',stage)
        sandbox=execution.DockerValidator(owner.policy.image_id,{'workflow_adapter.py':execution.ADAPTER},
            command=execution.prestart.COMMAND)
        argv=execution.b02._start_arguments(sandbox,expected['name'],Path(stage['workspace']),Path(stage['checks']),
            Path(stage['inputs']),expected['volume'])
        argv[1]='create';argv.remove('--detach');at=argv.index('--entrypoint')
        for key,value in expected['labels'].items():
            argv[at:at]=['--label',key+'='+value];at+=2
        self.command('volume-created',['docker','volume','inspect','--format','{{json .}}',expected['volume']],
            execution.encoded({'Name':expected['volume'],'Driver':'local','Scope':'local','Options':execution.b01.VOLUME_OPTIONS,
                'Labels':{'gossip.execution':intent['execution_id'],'gossip.snapshot':execution.b01.SNAPSHOT_PROTOCOL}}))
        self.command('container-create',argv,self.cid.encode())
        self.command('container-prestart',['docker','inspect','--format','{{json .}}',self.cid],execution.encoded(self.created))
        self.retain(execution.prestart.PROOF_FILE,execution.prestart.proof_for(execution.encoded(self.created),**expected))
        self.command('container-start',['docker','start',self.cid],self.cid.encode())
        self.retain('session-dispatch.json',{'argv':owner.docker+['exec','--interactive','--user','65534:65534',
            self.cid,'python','-I','-B','/checks/workflow_adapter.py']})
        self.running=deepcopy(self.created)
        self.running['State'].update(Status='running',Running=True,Paused=True,Pid=123,StartedAt='2026-10-04T00:00:01.000000001Z')
        frames=[execution._READY]
        owner._retain('session-ready.bin',execution._READY)
        self.engine('session-ready',first=True);self.capture('session-ready')
        self.retain('session-ready-ack.json',{'request':'ready\n'})
        for index,phase in enumerate(owner.profile.phases):
            self.guard(phase,'before');self.retain(phase+'-request.json',{'request':phase+'\n'})
            # The enrolled marker is synthetic source placement, not a real
            # execution of this candidate. Each actual reader boundary is kept.
            boundary=owner.plan.boundaries[0]
            event={'kind':'boundary','phase':phase,'ordinal':0,'boundary':boundary.id,'occurrence':0,
                'paths':{'root':None,'database':None},'path_origins':{'root':None,'database':None}}
            raw=execution.encoded(event)+b'\n';frames.append(raw)
            owner._retain(phase+'-frame-000.bin',raw)
            label=phase+'-boundary-000';self.retain(label+'-event.json',event)
            self.engine(label)
            path_raw=execution.encoded({'root':None,'database':None})
            self.command(label+'-paths',['docker','exec','--user','65534:65534',self.cid,
                'python','-I','-B','/checks/workflow_paths.py',execution.encoded(event['paths']).decode('ascii')],path_raw)
            owner._retain(label+'-path-facts.json',path_raw);self.capture(label)
            self.retain(phase+'-resume-000.json',{'request':'resume:'+phase+':0\n'})
            raw=execution.encoded({'kind':'result','phase':phase,'value':{'wrong':True}})+b'\n'
            frames.append(raw);owner._retain(phase+'-frame-001.bin',raw);owner._retain(phase+'-response.bin',raw)
            self.engine(phase+'-result');self.capture(phase+'-result')
            self.retain(phase+'-next.json',{'request':'next:'+phase+'\n'});self.guard(phase,'after')
        done=exec_value();done.update(ContainerID=self.cid,Running=False)
        owner._retain('session-final-exec-request.bin',execution.process._request('GET','/exec/'+'e'*64+'/json'))
        owner._retain('session-final-exec-response.bin',http_original(done))
        self.retain('session-final-exec-verified.json',{'exec_id':'e'*64,'pid':17,'completed':True,'value_sha256':execution.digest(done)})
        session={'exit_code':0,'natural_exit':True,'timed_out':False,'capture_complete':True,'errors':[],
            'requests':list(owner.profile.phases)}
        for kind,raw in (('stdout',b''.join(frames)),('stderr',b'')):
            name='session-'+kind+'.bin';owner._retain_blob(name,raw)
            session[kind]={'path':name,'bytes':len(raw),'observed_bytes':len(raw),'sha256':execution.sha(raw),'truncated':False}
        self.retain('session.json',session)
        self.command('container-remove',['docker','rm','--force',self.cid],self.cid.encode())
        self.command('container-after',['docker','container','ls','--all','--quiet','--filter','name=^/'+expected['name']+'$'],b'')
        self.command('volume-remove',['docker','volume','rm',expected['volume']],expected['volume'].encode())
        self.command('volume-after',['docker','volume','ls','--quiet','--filter','name=^'+expected['volume']+'$'],b'')
        self.retain('staging-final.json',self.stage)
        self.retain('terminal.json',{'protocol':owner.binding.protocol,
            'intent_sha256':execution.sha(owner.read_authenticated('intent.json')),'execution_id':intent['execution_id'],
            'case_id':owner.binding.case_id,'family':owner.binding.family,'source_sha256':owner.binding.source_sha256,
            'native_source_sha256':owner.binding.native_source_sha256,'infrastructure':[],
            'original_definition_purpose':owner.observation_registration.original_definition_purpose,
            'cleanup_verified':True,'container_cleanup':True,'volume_cleanup':True,'status':'completed','missing_step_ids':[]})
