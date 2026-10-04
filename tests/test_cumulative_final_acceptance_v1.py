"""Offline admission controls only; synthetic terminal slots never prove a run."""
from dataclasses import dataclass, replace
import hashlib
from pathlib import Path
import shutil
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from gossip_harness import candidate_checkpoint_chain_v1 as checkpoint
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from gossip_harness import candidate_scope_consumer_v1 as consumer
from gossip_harness import cumulative_final_acceptance_v1 as final
from gossip_harness import cumulative_scope_authority_v1 as scope
from gossip_harness import cumulative_study_successor_v1 as successor
from gossip_harness.cumulative_study_controller_v1 import digest, Records
from tests.test_cumulative_study_controller_v1 import plan as base_plan


@dataclass(frozen=True)
class SyntheticSlot:
    cohort: str
    trajectory: str
    planned_actors: tuple[str, ...]
    outcome: str = 'unattempted'
    final_source: dict | None = None
    financial_seal: object = None
    financial_origin: dict | None = None
    process_counts: tuple = ()
    evidence_errors: tuple[str, ...] = ()


@dataclass(frozen=True)
class SyntheticAudit:
    execution_contract_sha256: str
    roster_sha256: str
    slots: tuple[SyntheticSlot, ...]
    checkpoint: checkpoint.PrefixCommitment
    barrier: dict | None = None
    barrier_position: int | None = None
    barrier_name: str | None = None
    barrier_sha256: str | None = None
    freeze_eligible: bool = False


class CumulativeFinalAcceptanceV1Tests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='gossip-final-mechanics-')
        self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name).resolve()
        self.repository=self.root/'repository';self.repository.mkdir()
        original=base_plan(self.repository)
        sources={**final.implementation_sources(), **final.terminal_implementation_sources()}
        sources['gossip_harness/cumulative_study_successor_v1.py']=hashlib.sha256(Path(successor.__file__).read_bytes()).hexdigest()
        package=Path(final.__file__).parent.parent
        for name in sources:
            (self.repository/name).parent.mkdir(parents=True,exist_ok=True)
            shutil.copyfile(package/name,self.repository/name)
        runtime={**original.runtime,'final_acceptance_protocol':final.PROTOCOL,'study_successor_protocol':successor.PROTOCOL,'final_acceptance_financial_mode':'fixture',
            'final_acceptance_roots':{name:str(self.root/'acceptance'/folder) for name,folder in
                (('raw','raw'),('delta','delta'),('head','head'))}}
        limits={'horizon_seconds':original.horizon_seconds,'source_generations':original.source_generations,
            'partition_seconds':original.partition_seconds,'executor_slots':original.executor_slots,'runtime':runtime}
        self.plan=replace(original,runtime=runtime,source_pins={**original.source_pins,**sources},
                          cohort=replace(original.cohort,resource_contract_sha256=digest(limits)))
        self.study=self.make_chain('study')
        ledger=self.root/'ledger.sqlite'
        with sqlite3.connect(ledger) as db:
            db.execute('CREATE TABLE fixture_only (id INTEGER PRIMARY KEY)')
        stat=ledger.stat()
        self.ledger_identity={'path':str(ledger),'device':stat.st_dev,'inode':stat.st_ino}
        Records(self.study).put('contract',self.plan.record())
        Records(self.study).put('ledger',self.ledger_identity)
        self.journal=self.make_chain('acceptance')
        self.scope_chain=self.make_chain('scope')
        self.scope=scope.ScopeRegistrationController(self.repository,self.scope_chain,self.scope_chain.commitment)
        self.owner=final.FinalAcceptance(plan=self.plan,repository=self.repository,ledger_identity=self.ledger_identity,
            study_chain=self.study,study_expected=self.study.commitment,journal=self.journal,expected=self.journal.commitment,
            scope=self.scope,submissions=())
        self.addCleanup(self.owner.close)
        self.synthetic=SyntheticAudit(self.plan.sha256,self.plan.roster.sha256,
            tuple(SyntheticSlot(row.cohort,row.trajectory,row.actors) for row in self.plan.roster.children),self.study.commitment)
    def make_chain(self,name):
        root=self.root/name;root.mkdir()
        head=ExternalHead.create(root/'head',journal_roots=(root/'raw',root/'delta'))
        self.addCleanup(head.close)
        chain=checkpoint.CheckpointChain.create(root/'raw',root/'delta',context={'test':name},authority=head)
        self.addCleanup(chain.close)
        return chain
    def fixture_reader(self):
        # Deliberate isolated fixture boundary; never an actual terminal audit.
        self.owner._read_terminal=lambda:self.synthetic
    def test_missing_real_terminal_reader_never_creates_freeze(self):
        with patch.object(final.importlib,'import_module',side_effect=ImportError('not installed')):
            with self.assertRaises(consumer.AuthorityUnavailable):self.owner.prepare()
        self.assertIsNone(self.owner.freeze)
        self.assertIsNone(self.owner.records.read('final.freeze'))
    def test_incomplete_fixture_keeps_all_six_slots_without_sources_or_acceptance(self):
        self.fixture_reader()
        self.assertIsNone(self.owner.prepare())
        result=self.owner.assess()
        self.assertEqual(len(result['slots']),6)
        self.assertTrue(all(row['subject'] is None and row['terminal_outcome']=='unattempted' for row in result['slots']))
        self.assertFalse(result['accepted'])
        self.assertFalse(result['completed_and_accepted'])
        self.assertEqual(self.owner.assess(),result)
    def test_boolean_freeze_cannot_fill_missing_original_sources(self):
        self.synthetic=replace(self.synthetic,freeze_eligible=True)
        self.fixture_reader()
        with self.assertRaises(consumer.AuthorityError):self.owner.prepare()
        self.assertIsNone(self.owner.records.read('final.freeze'))
    def test_current_terminal_revocation_prevents_final_result(self):
        self.fixture_reader();self.owner.prepare()
        self.synthetic=replace(self.synthetic,slots=(replace(self.synthetic.slots[0],outcome='evidence_unknown',
            evidence_errors=('original vanished',)),*self.synthetic.slots[1:]))
        with self.assertRaises(consumer.AuthorityError):self.owner.assess()
        self.assertIsNone(self.owner.records.read('final.assessment'))
    def test_study_prefix_cannot_advance_after_freeze_input(self):
        self.fixture_reader();self.owner.prepare()
        self.study.retain('unrelated.json',b'{}')
        with self.assertRaises(consumer.AuthorityUnavailable):self.owner.assess()
    def test_old_public_only_plan_cannot_adopt_final_protocol(self):
        runtime={key:value for key,value in self.plan.runtime.items() if key!='final_acceptance_protocol'}
        limits={'horizon_seconds':self.plan.horizon_seconds,'source_generations':self.plan.source_generations,
            'partition_seconds':self.plan.partition_seconds,'executor_slots':self.plan.executor_slots,'runtime':runtime}
        old=replace(self.plan,runtime=runtime,cohort=replace(self.plan.cohort,resource_contract_sha256=digest(limits)))
        with self.assertRaises(consumer.AuthorityError):successor.validate_successor(old,self.repository)
    def test_forged_registration_dict_is_rejected_without_owner_construction(self):
        spec=final.ObservationSpec('cli',self.root/'candidate',self.root/'delta',self.root/'cleanup',None,None,{}, {})
        with patch.object(self.owner,'_construct',side_effect=AssertionError('must not construct')):
            with self.assertRaises(consumer.AuthorityError):self.owner.dispatch(spec)
    def test_byte_originals_are_preserved_losslessly_in_verifier_material(self):
        self.assertEqual(final._material({'seal':b'\xff\x00'}),{'seal':{'encoding':'base64','bytes':2,'base64':'/wA='}})
    def test_acceptance_source_drift_prevents_even_missing_assessment(self):
        self.fixture_reader();self.owner.prepare()
        path=self.repository/'gossip_harness/cumulative_final_acceptance_v1.py'
        path.write_text(path.read_text()+'\n# changed\n')
        with self.assertRaises(ValueError):self.owner.assess()

    def test_full_source_closure_includes_definition_and_scope_originals(self):
        sources=final.implementation_sources()
        for name in ('candidate_client_process_v4.py','candidate_http_observation_v2.py',
                     'candidate_product_process_core_v1.py','candidate_scope_consumer_v1.py'):
            self.assertIn('gossip_harness/'+name,sources)
        self.assertIn('library-cumulative-product-v2.json',sources)
        self.assertIn('analysis/cumulative-shared-ownership-review-v1.json',sources)
        self.assertEqual(set(final.terminal_implementation_sources()),
                         set(__import__('gossip_harness.cumulative_terminal_originals_v1',fromlist=['x']).TERMINAL_READER_SOURCES))

    def test_missing_terminal_reader_stops_public_entry_before_runtime(self):
        from gossip_harness import cumulative_study_runtime_v1 as runtime
        with patch.object(final.importlib,'import_module',side_effect=ImportError('reader missing')):
            with patch.object(runtime,'run_study') as launch:
                with self.assertRaises(consumer.AuthorityUnavailable):
                    successor.run_public_phase(self.plan,repository=self.repository,mode='fixture')
                launch.assert_not_called()

    def test_missing_terminal_reader_final_entry_retains_all_six_planned_unknowns(self):
        with patch.object(final.importlib,'import_module',side_effect=ImportError('reader missing')):
            result=successor.run_final_phase(self.owner,())
        self.assertFalse(result['accepted'])
        self.assertEqual(result['status'],'unavailable')
        self.assertEqual(len(result['planned_trajectories']),6)
        self.assertTrue(all(row['current_status']=='evidence_unknown' for row in result['planned_trajectories']))

    def test_fixture_origin_cannot_be_labeled_live_quality(self):
        self.synthetic=replace(self.synthetic,slots=(replace(self.synthetic.slots[0],
            final_source={'source_sha256':'a'*64},financial_origin={'mode':'live','observation_kind':'provider'}),
            *self.synthetic.slots[1:]))
        self.fixture_reader()
        with self.assertRaisesRegex(consumer.AuthorityError,'financial origin'):
            self.owner.prepare()
        self.assertIsNone(self.owner.records.read('final.terminal-verifier'))

    def test_original_financial_provenance_remains_explicit_without_a_freeze(self):
        self.synthetic=replace(self.synthetic,slots=(replace(self.synthetic.slots[0],
            final_source={'source_sha256':'a'*64},financial_origin={'mode':'fixture','observation_kind':'simulated'}),
            *self.synthetic.slots[1:]))
        self.fixture_reader();self.owner.prepare()
        result=self.owner.assess()
        self.assertEqual(result['slots'][0]['financial_origin'],{'mode':'fixture','observation_kind':'simulated'})
        self.assertFalse(result['live_financial_origin'])
        self.assertFalse(result['accepted'])

    def test_actual_scope_owner_missing_registration_prevents_construction(self):
        from types import SimpleNamespace
        from gossip_harness import project_acceptance_registry_v1 as registry
        subject=registry.Subject(self.plan.cohort.cohort_id,self.plan.roster.children[0].trajectory,
            'M4',self.plan.sha256,final.compiler.PRODUCT_V2_SHA256,'a'*64)
        self.owner.subjects=(subject,)
        self.owner.freeze=registry.CohortFreeze((subject,),'b'*64,'c'*64,True)
        self.owner.originals=replace(self.synthetic,slots=(replace(self.synthetic.slots[0],
            final_source={'commit_oid':'d'*40,'tree_oid':'e'*40}),*self.synthetic.slots[1:]))
        self.owner._current_originals=lambda:None
        registration=SimpleNamespace(gate=SimpleNamespace(binding=SimpleNamespace(subject=subject)),
            cohort_trajectory_ids=tuple(child.trajectory for child in self.plan.roster.children),
            commit_oid='d'*40,tree_oid='e'*40)
        with patch.object(self.owner,'_construct',side_effect=AssertionError('must not construct')):
            with self.assertRaisesRegex(consumer.AuthorityUnavailable,'scope is not registered'):
                self.owner._registered_gate(registration)
        self.assertFalse(self.owner.enrollments)

    def test_normalization_preserves_known_failure_with_unknown_or_invalid_sibling(self):
        import subprocess
        import sqlite3
        from gossip_harness.gitstore import GitError
        from gossip_harness import project_acceptance_registry_v1 as registry
        from tests.test_candidate_scope_consumer_v1 import FixtureAuthority,FixtureSnapshot,duplicate_gate
        from tests.test_project_acceptance_compiler_v1 import inventory,subject,synthetic_declaration,synthetic_scope
        inv=inventory(2)
        declaration=duplicate_gate(synthetic_declaration(inv),'product')
        scope_plan,subj=synthetic_scope(inv,declaration),subject(inv)
        errors=(OSError('lost storage'),GitError('tree absent'),subprocess.TimeoutExpired('git',1),
                sqlite3.DatabaseError('ledger unavailable'),checkpoint.ChainUnknown('lost ack'),ValueError('substitution'))
        for error in errors:
            with self.subTest(error=type(error).__name__):
                snapshot=FixtureSnapshot(inv,declaration,scope_plan,subj)
                first=snapshot.observations['product']
                snapshot.observations['product']=replace(first,execution=replace(first.execution,
                    outcomes=(registry.CaseResult('case','failed'),)))
                original=snapshot.observation
                def observed(gate,freeze):
                    if gate.gate_id=='product-two':raise error
                    return original(gate,freeze)
                snapshot.observation=final.normalize_authority(observed)
                result=consumer.CandidateScopeConsumer(FixtureAuthority(snapshot)).assess(
                    inventory=inv,declaration=declaration,scope=scope_plan,subject=subj)
                self.assertEqual(result.product.failed_gates,('product',))
                self.assertEqual(result.product.missing_gates,('product-two',))
                self.assertEqual(result.product.status,'rejected')

    def test_actual_terminal_reader_and_successor_keep_unattempted_census_without_acceptance(self):
        # Real source-pinned reader, SQLite originals and independent heads;
        # deliberately no roles/financial enrollment/seals, so no freeze or pass.
        result=successor.run_final_phase(self.owner,())
        self.assertFalse(result['accepted'])
        assessed=result['final_phase']
        self.assertFalse(assessed['freeze_available'])
        self.assertFalse(assessed['live_financial_origin'])
        self.assertEqual(len(assessed['slots']),6)
        self.assertEqual(sum(row['process_counts']['planned'] for row in assessed['slots']),96)
        self.assertTrue(all(row['terminal_outcome']=='unattempted' for row in assessed['slots']))
        self.assertIsNotNone(self.owner.records.read('final.terminal-verifier'))
        self.assertIsNotNone(self.owner.records.read('final.assessment'))
        self.assertIsNone(self.owner.records.read('final.freeze'))

    def test_successor_rejects_runtime_mode_substitution_before_public_call(self):
        # Omitting all other mandatory runtime inputs also proves run_study was
        # not entered; a leaked call would raise TypeError instead.
        with self.assertRaisesRegex(consumer.AuthorityError,'financial mode differs'):
            successor.run_public_phase(self.plan,repository=self.repository,mode='live')

    def test_unverified_cleanup_is_retained_and_latches_direct_dispatch(self):
        from types import SimpleNamespace
        history=final.cli.ClientHistoryResult('fixture-only','cli-empty','infrastructure_error',(),(),False,
            ('unverified-volume-removal',),'a'*64,self.journal.commitment)
        row=SimpleNamespace(history=None)
        self.owner._history('synthetic-cleanup-control',row,history)
        self.assertFalse(row.history['cleanup_verified'])
        self.assertEqual(row.history['infrastructure'],['unverified-volume-removal'])
        self.assertEqual(self.owner.records.read('final.dispatch-halt'),self.owner.dispatch_halt)
        spec=final.ObservationSpec('cli',self.root/'candidate',self.root/'delta',self.root/'cleanup',None,None,{}, {})
        with patch.object(self.owner,'_construct',side_effect=AssertionError('must not construct')):
            with self.assertRaisesRegex(consumer.AuthorityUnavailable,'halted further dispatch'):
                self.owner.dispatch(spec)
        self.fixture_reader();self.owner.prepare()
        assessed=self.owner.assess()
        self.assertEqual(assessed['dispatch_halt'],self.owner.dispatch_halt)
        self.assertFalse(assessed['accepted'])

    def test_same_session_cannot_reopen_and_reset_attempt_state(self):
        with self.assertRaisesRegex(consumer.AuthorityError,'cannot be reopened'):
            final.FinalAcceptance(plan=self.plan,repository=self.repository,ledger_identity=self.ledger_identity,
                study_chain=self.study,study_expected=self.study.commitment,journal=self.journal,
                expected=self.journal.commitment,scope=self.scope,submissions=())

    def test_fresh_substituted_session_cannot_bypass_original_root_binding(self):
        other=self.make_chain('another-acceptance')
        with self.assertRaisesRegex(consumer.AuthorityError,'session roots differ'):
            final.FinalAcceptance(plan=self.plan,repository=self.repository,ledger_identity=self.ledger_identity,
                study_chain=self.study,study_expected=self.study.commitment,journal=other,
                expected=other.commitment,scope=self.scope,submissions=())

    def test_successor_rejects_overlapping_acceptance_roots_before_public_call(self):
        original=self.plan.runtime['final_acceptance_roots']
        for changed in ({**original,'delta':original['raw']},
                        {**original,'head':original['raw']+'/nested'}):
            with self.subTest(roots=changed):
                runtime={**self.plan.runtime,'final_acceptance_roots':changed}
                limits={'horizon_seconds':self.plan.horizon_seconds,'source_generations':self.plan.source_generations,
                    'partition_seconds':self.plan.partition_seconds,'executor_slots':self.plan.executor_slots,'runtime':runtime}
                plan=replace(self.plan,runtime=runtime,cohort=replace(self.plan.cohort,resource_contract_sha256=digest(limits)))
                with self.assertRaisesRegex(consumer.AuthorityError,'pairwise disjoint'):
                    successor.run_public_phase(plan,repository=self.repository,mode='fixture')
