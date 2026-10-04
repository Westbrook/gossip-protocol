"""Actual V2 reader/owner composition; no complete study or semantic approval.

The original ledger is deliberately unenrolled. Positive controls establish the
real version/source/read path, while preserving all six unavailable outcomes.
Public-runtime routing reaches the untouched function signature only, without
supplying the mandatory effect inputs or executing candidate work.
"""
from copy import deepcopy
from dataclasses import asdict, replace
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
from gossip_harness import cumulative_final_acceptance_v1 as old_final
from gossip_harness import cumulative_final_acceptance_v2 as final
from gossip_harness import cumulative_scope_authority_v1 as old_scope
from gossip_harness import cumulative_scope_authority_v2 as scope
from gossip_harness import cumulative_study_controller_v2 as controller
from gossip_harness import cumulative_study_runtime_v2 as runtime
from gossip_harness import cumulative_study_successor_v1 as old_successor
from gossip_harness import cumulative_study_successor_v2 as successor
from gossip_harness import cumulative_terminal_originals_v1 as old_terminal
from gossip_harness import cumulative_terminal_originals_v2 as terminal
from gossip_harness.peer_financial_terminal_v2 import CLOSURE_POLICY
from tests.test_cumulative_study_controller_v1 import plan as old_plan


class CumulativeFinalAcceptanceV2Tests(unittest.TestCase):
    def setUp(self):
        temporary=tempfile.TemporaryDirectory(prefix='gossip-final-v2-')
        self.addCleanup(temporary.cleanup)
        self.root=Path(temporary.name).resolve()
        self.repository=self.root/'repository';self.repository.mkdir()
        original=old_plan(self.repository)
        self.old_plan=original
        package=Path(final.__file__).parents[1]
        sources={**final.implementation_sources(),**final.terminal_implementation_sources(),
            **{name:hashlib.sha256((package/name).read_bytes()).hexdigest() for name in controller.SOURCE_CLOSURE},
            **{'gossip_harness/'+Path(module.__file__).name:hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()
               for module in (old_successor,successor)}}
        for name in sources:
            (self.repository/name).parent.mkdir(parents=True,exist_ok=True)
            shutil.copyfile(package/name,self.repository/name)
        runtime_identity={**original.runtime,'protocol':runtime.PROTOCOL,
            'financial_protocol':'peer-financial-authority-v5','financial_rpc_protocol':'peer-financial-rpc-v5',
            'final_acceptance_protocol':final.PROTOCOL,'study_successor_protocol':successor.PROTOCOL,
            'final_acceptance_financial_mode':'fixture',
            'final_acceptance_roots':{key:str(self.root/'acceptance'/key) for key in ('raw','delta','head')}}
        limits={'horizon_seconds':original.horizon_seconds,'source_generations':original.source_generations,
            'partition_seconds':original.partition_seconds,'executor_slots':original.executor_slots,'runtime':runtime_identity}
        trajectories=tuple(replace(t,fault_schedule_sha256=controller.digest(controller.fault_schedule(t.block,.1)))
                           for t in original.cohort.trajectories)
        cohort=replace(original.cohort,trajectories=trajectories,
            shared_policy_sha256=controller.digest(controller.SHARED_POLICY),resource_contract_sha256=controller.digest(limits))
        self.plan=controller.StudyPlan(cohort,tuple(controller.Release(**asdict(r)) for r in original.releases),
            original.initial_files,original.package_paths,{**original.source_pins,**sources},
            financial_closure_policy=deepcopy(CLOSURE_POLICY),**limits)
        self.study=self.chain('study');self.journal=self.chain('acceptance');self.scope_chain=self.chain('scope')
        ledger=self.root/'ledger.sqlite'
        with sqlite3.connect(ledger) as db:db.execute('CREATE TABLE fixture_only (id INTEGER PRIMARY KEY)')
        stat=ledger.stat();self.identity={'path':str(ledger),'device':stat.st_dev,'inode':stat.st_ino}
        controller.Records(self.study).put('contract',self.plan.record())
        controller.Records(self.study).put('ledger',self.identity)
        self.scope=scope.ScopeRegistrationController(self.repository,self.scope_chain,self.scope_chain.commitment)
        self.owner=final.FinalAcceptanceV2(**self.arguments())
        self.addCleanup(self.owner.close)

    def chain(self,name):
        base=self.root/name;base.mkdir()
        head=ExternalHead.create(base/'head',journal_roots=(base/'raw',base/'delta'));self.addCleanup(head.close)
        chain=checkpoint.CheckpointChain.create(base/'raw',base/'delta',context={'test':name},authority=head)
        self.addCleanup(chain.close);return chain

    def arguments(self,**changes):
        return dict(plan=self.plan,repository=self.repository,ledger_identity=self.identity,
            study_chain=self.study,study_expected=self.study.commitment,journal=self.journal,
            expected=self.journal.commitment,scope=self.scope,submissions=())|changes

    def test_actual_v2_reader_and_successor_keep_all_six_missing_without_acceptance(self):
        result=successor.run_final_phase(self.owner,())
        self.assertEqual(result['protocol'],successor.PROTOCOL)
        self.assertEqual(result['final_phase']['protocol'],final.PROTOCOL)
        self.assertFalse(result['accepted']);self.assertFalse(result['completed_and_accepted'])
        self.assertIs(type(self.owner.originals),terminal.TerminalCohortOriginals)
        self.assertEqual(len(result['final_phase']['slots']),6)
        self.assertEqual(sum(row['process_counts']['planned'] for row in result['final_phase']['slots']),96)
        self.assertTrue(all(row['terminal_outcome']=='unattempted' for row in result['final_phase']['slots']))
        self.assertIsNone(self.owner.freeze)
        verifier=self.owner.records.read('final.terminal-verifier')
        self.assertEqual(verifier['protocol'],final.PROTOCOL)
        self.assertFalse(verifier['acceptance_authority'])
        self.assertEqual(self.owner.records.read('final.contract')['study_sha256'],self.plan.sha256)

    def test_actual_v2_terminal_sources_and_v5_policy_are_prospectively_bound(self):
        contract=successor.validate_successor(self.plan,self.repository)
        for name in ('peer_financial_authority_v5.py','peer_financial_rpc_v5.py','peer_financial_terminal_v2.py',
                     'cumulative_study_controller_v1.py','cumulative_study_controller_v2.py',
                     'cumulative_study_runtime_v2.py','cumulative_terminal_originals_v2.py',
                     'cumulative_final_acceptance_v1.py','cumulative_final_acceptance_v2.py',
                     'cumulative_scope_authority_v2.py','candidate_storage_product_execution_v1.py'):
            self.assertIn('gossip_harness/'+name,contract['sources'])
        self.assertEqual(contract['public_phase_protocol'],controller.PROTOCOL)
        self.assertEqual(self.plan.financial_closure_policy,CLOSURE_POLICY)
        self.assertEqual(self.owner.protocol,final.PROTOCOL)

    def test_v1_owner_and_entry_reject_v2_plan_and_owner_before_dispatch(self):
        with self.assertRaisesRegex(consumer.AuthorityError,'prospective plan'):
            old_final.FinalAcceptance(**self.arguments())
        with self.assertRaises(consumer.AuthorityError):old_successor.run_final_phase(self.owner,())
        with self.assertRaises(consumer.AuthorityError):old_successor.validate_successor(self.plan,self.repository)

    def test_v2_rejects_v1_plan_and_scope_before_journal_append(self):
        before=self.journal.commitment
        with self.assertRaisesRegex(consumer.AuthorityError,'prospective plan'):
            final.FinalAcceptanceV2(**self.arguments(plan=self.old_plan))
        old=old_scope.ScopeRegistrationController(self.repository,self.scope_chain,self.scope_chain.commitment)
        with self.assertRaisesRegex(consumer.AuthorityError,'prospective plan'):
            final.FinalAcceptanceV2(**self.arguments(scope=old))
        self.assertEqual(before,self.journal.commitment)

    def test_unknown_subclass_cannot_supply_another_acceptance_contract(self):
        class Other(final.FinalAcceptanceV2):pass
        with self.assertRaisesRegex(consumer.AuthorityError,'Unknown final acceptance owner'):
            Other(**self.arguments())

    def test_v5_policy_mutation_is_rejected_before_public_runtime_entry(self):
        self.plan.financial_closure_policy['acceptance_authority']=True
        with patch.object(runtime,'run_study') as call:
            with self.assertRaises(ValueError):successor.run_public_phase(self.plan,repository=self.repository,mode='fixture')
            call.assert_not_called()
        with self.assertRaises(ValueError):self.owner.prepare()

    def test_v2_mode_substitution_cannot_enter_public_runtime(self):
        # Missing remaining runtime arguments would produce TypeError if the
        # real endpoint were reached, without changing its loaded code.
        with self.assertRaisesRegex(consumer.AuthorityError,'financial mode differs'):
            successor.run_public_phase(self.plan,repository=self.repository,mode='live')

    def test_v2_public_entry_reaches_only_actual_runtime_after_contract_validation(self):
        # The untouched concrete endpoint rejects its missing mandatory args.
        # No patched result is interpreted as execution or quality evidence.
        contract=successor.validate_successor(self.plan,self.repository)
        self.assertEqual(contract['public_phase_protocol'],controller.PROTOCOL)
        with self.assertRaisesRegex(TypeError,'required keyword-only'):
            successor.run_public_phase(self.plan,repository=self.repository,mode='fixture')

    def test_old_terminal_audit_type_cannot_be_reused_for_v2_final_verifier(self):
        actual=self.owner._read_terminal()
        old=old_terminal.TerminalCohortOriginals(**{key:getattr(actual,key) for key,value in old_terminal.TerminalCohortOriginals.__dataclass_fields__.items() if value.init})
        with patch.object(terminal,'audit_terminal_cohort',return_value=old):
            # Loaded-code authentication rejects the patched producer, or the
            # exact terminal class check rejects it if a source gate is stubbed.
            with self.assertRaises(consumer.AuthorityError):self.owner.prepare()
        self.assertIsNone(self.owner.records.read('final.terminal-verifier'))

    def test_storage_remains_unavailable_without_six_original_sources(self):
        self.owner.prepare()
        from types import SimpleNamespace
        from gossip_harness import project_acceptance_registry_v1 as registry
        subject=registry.Subject(self.plan.cohort.cohort_id,self.plan.roster.children[0].trajectory,
            'M4',self.plan.sha256,controller.PRODUCT_V2_SHA256,'a'*64)
        registered=SimpleNamespace(gate=SimpleNamespace(binding=SimpleNamespace(subject=subject)))
        with patch.object(self.owner,'_construct',side_effect=AssertionError('No construction')):
            with self.assertRaisesRegex(consumer.AuthorityUnavailable,'Six authentic'):
                self.owner._registered_gate(registered)
        self.assertFalse(self.owner.enrollments)

    def test_missing_v2_terminal_source_blocks_public_entry(self):
        self.plan.source_pins.pop('gossip_harness/cumulative_terminal_originals_v2.py')
        with self.assertRaisesRegex(consumer.AuthorityError,'source-bound'):
            successor.run_public_phase(self.plan,repository=self.repository,mode='fixture')

    def test_missing_shared_v1_record_source_blocks_v2_public_entry(self):
        self.plan.source_pins.pop('gossip_harness/cumulative_study_controller_v1.py')
        with self.assertRaisesRegex(consumer.AuthorityError, 'source-bound'):
            successor.run_public_phase(self.plan, repository=self.repository, mode='fixture')

    def test_substituted_shared_v1_record_source_blocks_v2_public_entry(self):
        self.plan.source_pins['gossip_harness/cumulative_study_controller_v1.py'] = 'f' * 64
        with self.assertRaisesRegex(consumer.AuthorityError, 'source-bound'):
            successor.run_public_phase(self.plan, repository=self.repository, mode='fixture')
