"""Reader boundary controls; synthetic linked records are never a rehearsal."""
from dataclasses import asdict
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch, Mock

from gossip_harness.candidate_checkpoint_chain_v1 import CheckpointChain
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from gossip_harness.candidate_observation_admission_v1 import PROTOCOL as SOURCE_PROTOCOL, source_manifest, source_sha256
from gossip_harness.cumulative_process_evidence_v1 import PROTOCOL, ROLE_PROTOCOL, closure_name, confirmation_name
from gossip_harness.cumulative_terminal_originals_v1 import _process
from gossip_harness.peer_financial_authority_v2 import canonical_payload, FinancialError
from gossip_harness.cumulative_study_controller_v1 import Records, plain
from gossip_harness.peer_financial_terminal_v1 import ChildRegistration, TerminalRoster, sha, digest

from gossip_harness.cumulative_terminal_originals_v1 import (_sealed_child,_ledger_bounds,audit_terminal_cohort,terminal_reader_sources)
from gossip_harness.financial_rehearsal_originals_v1 import readonly_database
from gossip_harness.peer_financial_authority_v2 import ledger_identity
from gossip_harness.ledger import Ledger
from tests.financial_v4_fixture import Fixture
from tests.financial_rehearsal_fixture_v1 import synthetic_plan


class CumulativeTerminalProcessV1Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.head = ExternalHead.create(self.root/'anchor', journal_roots=(self.root/'raw',self.root/'delta'))
        self.chain = CheckpointChain.create(self.root/'raw', self.root/'delta', context={'fixture':'process-reader'}, authority=self.head)
        self.roster = TerminalRoster('a'*64, tuple(ChildRegistration('cohort'+str(i), 'trajectory'+str(i),
            tuple(f't{i}.r{j}' for j in range(8 if i<2 else 20))) for i in range(6)))
        self.child = self.roster.children[0]
        self.sources = {'gossip_harness/cumulative_process_evidence_v1.py':'b'*64}
        self.files = {'src/a.py':b'pass\n'}
        self.source = {'source_sha256':source_sha256(self.files), 'commit_oid':'c'*40, 'files':{'src/a.py':'pass\n'}}
        (self.root/'gossip-harness-store').write_bytes(b'fixture-marker')
        info=self.root.stat()
        self.protected={'path':str(self.root),'device':info.st_dev,'inode':info.st_ino,'marker_sha256':sha(b'fixture-marker')}
        self.counter = 0
        self.addCleanup(self.cleanup)

    def cleanup(self):
        self.chain.close()
        self.head.close()
        self.temp.cleanup()

    def retain(self, value):
        self.counter += 1
        name = f'record-{self.counter}.json'
        raw = canonical_payload(value)
        self.chain.retain(name, raw)
        return {'name':name, 'sha256':sha(raw)}

    def records(self, mutation=None, *, confirm=True, status="stopped_failure", missing_final=False, never=False, launch_failed=False, unknown_launch=False, missing_wait=False):
        reg = self.retain({'protocol':PROTOCOL,'kind':'registration','roster':self.roster.record(),
            'roster_sha256':self.roster.sha256,'child':self.child.record(),'sources':self.sources,
            'max_generations':2,'parent_pid':1,'parent_thread':1,'protected_repository':self.protected})
        rows=[]
        for ordinal,actor in enumerate(self.child.actors):
            if never and ordinal==0:
                rows.append({'actor':actor,'incarnations':[],'never_launched':True})
                continue
            identity={'cohort':self.child.cohort,'trajectory':self.child.trajectory,
                'execution_contract_sha256':self.roster.execution_contract_sha256,
                'actor':actor,'generation':0,'launch_id':f'{ordinal:048x}'}
            directory=self.root/'processes'/actor/'0'
            directory.mkdir(parents=True)
            st=directory.stat()
            argv=['/fixture/python','-m','gossip_harness.cumulative_study_role_v1','--role-config',str(self.root/'roles'/actor/'config.json')]
            Records(self.chain).put('runtime.'+self.child.trajectory+'.launcher.'+actor+'.0',
                {'argv':argv,'cwd':str(self.root),'environment':{},'actor':actor,'generation':0})
            intent=self.retain({'protocol':PROTOCOL,'kind':'launch-intent','identity':identity,'registration':reg,
                'argv_sha256':digest(argv),'cwd':str(self.root),'environment_sha256':digest({
                    'GOSSIP_ROLE_EVIDENCE_CONTEXT':canonical_payload(identity).decode(),'GOSSIP_ROLE_EVIDENCE_DIR':str(directory)}),
                'stdout_path':str(directory/'stdout.log'),'directory':str(directory),
                'directory_identity':[st.st_dev,st.st_ino],'start_new_session':True})
            if (launch_failed or unknown_launch) and ordinal==0:
                failure=self.retain({'protocol':PROTOCOL,'kind':'launch-failed','identity':identity,'intent':intent,
                    'error_type':'FileNotFoundError','errno':2,'parent_pid':1}) if launch_failed else None
                rows.append({'actor':actor,'incarnations':[{'generation':0,'intent':intent,'launch':None,
                    'ready':None,'final':None,'wait':None,'launch_failed':failure}],'never_launched':True})
                continue
            launch_value={'protocol':PROTOCOL,'kind':'launch','identity':identity,'intent':intent,
                'pid':100+ordinal,'process_group':100+ordinal,'parent_pid':1}
            if mutation:
                mutation('launch',actor,launch_value)
            launch=self.retain(launch_value)
            events={'ready':None,'final':None}
            for event in ('ready','final'):
                if event=='final' and missing_final and ordinal==0:
                    continue
                value={'protocol':ROLE_PROTOCOL,'event':event,'identity':identity,'pid':100+ordinal}
                if event=='final':
                    value.update(outcome='stopped',completed_action_ids=['action'],remaining_action_ids=[])
                if mutation:
                    mutation(event,actor,value)
                (directory/(event+'.json')).write_bytes(canonical_payload(value))
                events[event]=self.retain(value)
            wait_value={'protocol':PROTOCOL,'kind':'wait','identity':identity,'launch':launch,**events,
                'pid':100+ordinal,'returncode':0,'reaped_with':'Popen.wait','process_group_empty':True,
                'event_errors':[{'event':'final','error_type':'FileNotFoundError'}] if events['final'] is None else [],'parent_pid':1}
            if mutation:
                mutation('wait',actor,wait_value)
            wait=None if missing_wait and ordinal==0 else self.retain(wait_value)
            rows.append({'actor':actor,'incarnations':[{'generation':0,'intent':intent,'launch':launch,
                **events,'wait':wait,'launch_failed':None}], 'never_launched':False})
        source_ref=self.retain({'protocol':PROTOCOL,'kind':'final-source','registration':reg,
            'repository':str(self.root),'tree_oid':'f'*40,'source_identity_protocol':SOURCE_PROTOCOL,
            **{k:v for k,v in self.source.items() if k!='files'},'files':source_manifest(self.files)})
        stops=[]
        for row in rows:
            stop=self.retain({'protocol':'financial-role-stop-v1','roster_sha256':self.roster.sha256,
                'cohort':self.child.cohort,'trajectory':self.child.trajectory,'actor':row['actor'],'stopped':True})
            row['stop']=stop
            stops.append(stop)
        terminal=self.retain({'protocol':'financial-child-terminal-v1','roster_sha256':self.roster.sha256,
            'cohort':self.child.cohort,'trajectory':self.child.trajectory,'status':status,
            'final_source_sha256':self.source['source_sha256']})
        closure={'protocol':PROTOCOL,'kind':'closure','registration':reg,'source':source_ref,'roles':rows,
            'request':{'role_stops':stops,'terminal':terminal,'purpose':'normal-financial-terminal-v1'},
            'launch_admission_closed':True,'acceptance_authority':False}
        if mutation:
            mutation('closure','',closure)
        self.chain.retain(closure_name(self.child.cohort), canonical_payload(closure))
        if confirm:
            self.chain.retain(confirmation_name(self.child.cohort), canonical_payload({'protocol':PROTOCOL,
            'kind':'closure-confirmation','closure':{'name':closure_name(self.child.cohort),'sha256':sha(canonical_payload(closure))},
            'source':source_ref,'request_sha256':digest(closure['request']),'protected_repository':self.protected,'commit_oid':'c'*40}))
        return closure


    def audit(self,status='stopped_failure'):
        with patch('gossip_harness.cumulative_terminal_originals_v1.GitStore',return_value=Mock(head=lambda:'c'*40)), \
                patch('gossip_harness.cumulative_terminal_originals_v1.capture_git_source',return_value=('f'*40,self.files)):
            return _process(self.chain,self.chain.commitment,roster=self.roster,cohort=self.child.cohort,
                expected_sources=self.sources,expected_source=self.source,runtime_root=self.root,
                execution_repository=self.root,python_executable='/fixture/python',status=status)

    def test_completed_original_process_closure_remains_successful(self):
        self.records(status='completed')
        source,counts,_,_=self.audit('completed')
        self.assertEqual((counts['planned'],counts['launched'],counts['exited']),(8,8,8))
        self.assertEqual(source['files'],source_manifest(self.files))

    def test_stopped_failure_can_preserve_owned_exit_with_missing_child_final(self):
        self.records(missing_final=True)
        _,counts,_,_=self.audit()
        self.assertEqual((counts['launched'],counts['exited'],counts['missing_final']),(8,8,1))

    def test_never_launched_role_is_retained_separately_from_owned_stops(self):
        self.records(never=True)
        _,counts,_,_=self.audit()
        self.assertEqual((counts['planned'],counts['launched'],counts['exited'],counts['never_launched']),(8,7,7,1))

    def test_known_exec_failure_is_distinct_from_unknown_launch(self):
        self.records(launch_failed=True)
        _,counts,_,_=self.audit()
        self.assertEqual((counts['known_launch_failed'],counts['never_launched'],counts['exited']),(1,1,7))

    def test_unacknowledged_launch_without_original_failure_is_unknown(self):
        self.records(unknown_launch=True)
        with self.assertRaisesRegex(FinancialError,'Unknown launch'):
            self.audit()

    def test_failure_does_not_excuse_missing_owned_wait(self):
        self.records(missing_wait=True)
        with self.assertRaisesRegex(FinancialError,'owned wait'):
            self.audit()

    def test_missing_final_requires_original_owner_observation_error(self):
        self.records(lambda kind,actor,value:value.update(event_errors=[]) if kind=='wait' else None,missing_final=True)
        with self.assertRaisesRegex(FinancialError,'not explained'):
            self.audit()

    def test_stopped_failure_cannot_forge_wait_from_child_text(self):
        self.records(lambda kind,actor,value:value.update(reaped_with='child-said-so') if kind=='wait' else None)
        with self.assertRaisesRegex(FinancialError,'owned wait'):
            self.audit()

    def test_prepared_failure_closure_still_requires_final_confirmation(self):
        self.records(confirm=False)
        with self.assertRaises(ValueError):
            self.audit()

    def test_failed_closure_still_authenticates_full_controller_source_bytes(self):
        self.records()
        self.source={**self.source,'files':{'foreign.py':'foreign'}}
        with self.assertRaisesRegex(FinancialError,'full bytes'):
            self.audit()

    def test_failure_source_cannot_hide_missing_original_event_bytes(self):
        self.records()
        (self.root/'processes'/self.child.actors[0]/'0'/'ready.json').write_bytes(b'{}')
        with self.assertRaisesRegex(FinancialError,'event bytes'):
            self.audit()


class CumulativeTerminalFinancialV1Tests(Fixture,unittest.TestCase):
    def original(self,halt=False):
        self.open()
        if halt:
            with self.authority.ledger.atomic() as db:
                db.execute('UPDATE financial_cohorts_v2 SET halted=1 WHERE cohort=?',(self.child.cohort,))
        seal=self.seal('stopped_failure' if halt else 'completed')
        self.config=dict(self.authority.config)
        self.authority.close()
        self.authority=None
        self.terminal_record={'status':'stopped_failure' if halt else 'completed','financial_seal':{
            'raw_utf8':seal.raw.decode(),'publication_name':seal.publication_name,'commitment':asdict(seal.commitment)}}
        return seal

    def audit_financial(self):
        with readonly_database(ledger_identity(self.path)) as db:
            _ledger_bounds(db,self.child.cohort)
            return _sealed_child(db,self.chain,roster=self.roster,index=0,ledger_identity=ledger_identity(self.path),
                                  terminal=self.terminal_record,config=self.config)

    def test_original_sql_seal_gets_current_external_observation_without_owner(self):
        old=self.original()
        self.chain.retain('later-observation.json',b'{}')
        with patch('gossip_harness.peer_financial_authority_v4.CumulativeAuthorityV4.__init__',side_effect=AssertionError('owner')):
            observed,_,errors,opening,spent=self.audit_financial()
        self.assertEqual(observed.raw,old.raw)
        self.assertEqual(observed.commitment,self.chain.commitment)
        self.assertNotEqual(observed.commitment,old.commitment)
        self.assertEqual((errors,opening,spent),((),1000,0))

    def test_halted_original_financial_seal_is_diagnostic_never_freeze_eligible(self):
        self.original(halt=True)
        _,_,errors,_,_=self.audit_financial()
        self.assertEqual(errors,('financial_halt_or_unknown_action',))

    def test_sql_mutation_cannot_be_hidden_by_original_published_seal(self):
        self.original()
        import sqlite3
        with sqlite3.connect(self.path) as db:
            db.execute('UPDATE financial_cohorts_v2 SET halted=1 WHERE cohort=?',(self.child.cohort,))
        with self.assertRaisesRegex(FinancialError,'census'):
            self.audit_financial()

    def test_original_wallet_cap_change_is_not_accepted(self):
        self.original()
        import sqlite3
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE settings SET value=value+1 WHERE key='budget'")
        with self.assertRaisesRegex(FinancialError,'cap/opening'):
            self.audit_financial()

    def test_foreign_roster_cannot_adopt_same_child_seal(self):
        self.original()
        self.roster=TerminalRoster('f'*64,self.roster.children)
        with self.assertRaisesRegex(FinancialError,'outside this prospective'):
            self.audit_financial()

    def test_fixture_origin_cannot_be_relabelled_provider_without_original_config(self):
        self.original()
        self.config={**self.config,'observation_kind':'provider'}
        with self.assertRaisesRegex(FinancialError,'observation/permit mode'):
            self.audit_financial()

    def test_historical_wrapper_commitment_is_not_promoted_to_current_authority(self):
        self.original()
        self.terminal_record['financial_seal']['commitment']['head_sha256']='0'*64
        observed,_,_,_,_=self.audit_financial()
        self.assertEqual(observed.commitment,self.chain.commitment)


class CumulativeTerminalCohortV1Tests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.root=Path(self.temp.name).resolve()
        self.plan=synthetic_plan()
        self.ledger=Ledger(self.root/'wallet.sqlite',1000000)
        self.identity=ledger_identity(self.root/'wallet.sqlite')
        self.head=ExternalHead.create(self.root/'anchor',journal_roots=(self.root/'raw',self.root/'delta'))
        self.chain=CheckpointChain.create(self.root/'raw',self.root/'delta',context={'fixture':'terminal-outcomes'},authority=self.head)
        self.records=Records(self.chain)
        self.records.put('contract',self.plan.record())
        self.records.put('ledger',self.identity)
        self.addCleanup(self.cleanup)

    def cleanup(self):
        self.chain.close()
        self.head.close()
        self.temp.cleanup()

    def audit(self,expected=None):
        return audit_terminal_cohort(self.chain,expected or self.chain.commitment,plan=self.plan,
            ledger_identity=self.identity,repository=Path(__file__).resolve().parents[1])

    def test_fresh_original_wallet_preserves_all_six_unattempted_subjects(self):
        facts=self.audit()
        self.assertEqual(len(facts.slots),6)
        self.assertEqual(sum(len(slot.planned_actors) for slot in facts.slots),96)
        self.assertEqual({slot.outcome for slot in facts.slots},{'unattempted'})
        self.assertFalse(facts.freeze_eligible)
        self.assertIsNone(facts.barrier)

    def test_interrupted_first_child_does_not_erase_five_unattempted_slots(self):
        self.records.put('child.'+self.plan.roster.children[0].trajectory+'.begin',{'incomplete':True})
        facts=self.audit()
        self.assertEqual([slot.outcome for slot in facts.slots],['evidence_unknown']+['unattempted']*5)
        self.assertFalse(facts.freeze_eligible)

    def test_orphan_source_intent_is_unknown_rather_than_unattempted(self):
        self.records.put('runtime.'+self.plan.roster.children[0].trajectory+'.source-initialization.intent',{'orphan':True})
        facts=self.audit()
        self.assertEqual(facts.slots[0].outcome,'evidence_unknown')

    def test_self_described_barrier_cannot_manufacture_six_original_subjects(self):
        self.records.put('complete-cohort-barrier',{'self_described':True})
        facts=self.audit()
        self.assertFalse(facts.freeze_eligible)
        self.assertIsNone(facts.barrier_name)
        self.assertIn('retained_barrier_lacks_complete_current_originals',facts.evidence_errors)

    def test_stale_external_head_rejects_even_partial_outcome_audit(self):
        old=self.chain.commitment
        self.chain.retain('suffix.json',b'{}')
        with self.assertRaises(ValueError):
            self.audit(old)

    def test_original_wallet_identity_cannot_be_substituted(self):
        self.identity={**self.identity,'inode':self.identity['inode']+1}
        with self.assertRaises(FinancialError):
            self.audit()

    def test_terminal_reader_exposes_actual_complete_source_pins(self):
        pins=terminal_reader_sources()
        self.assertIn('gossip_harness/cumulative_terminal_originals_v1.py',pins)
        self.assertIn('gossip_harness/financial_rehearsal_originals_v1.py',pins)
        self.assertEqual(pins['gossip_harness/cumulative_terminal_originals_v1.py'],sha(
            (Path(__file__).resolve().parents[1]/'gossip_harness/cumulative_terminal_originals_v1.py').read_bytes()))

    def test_orphan_result_config_launcher_and_process_records_are_not_unattempted(self):
        children=self.plan.roster.children
        for index,suffix in enumerate(('.source-initialization.result','.financial-config','.launcher.foreign.0')):
            self.records.put('runtime.'+children[index].trajectory+suffix,{'orphan':True})
        name=closure_name(children[3].cohort).removesuffix('.closure.json')+'.registration.json'
        self.chain.retain(name,b'{}')
        facts=self.audit()
        self.assertEqual([slot.outcome for slot in facts.slots],['evidence_unknown']*4+['unattempted']*2)
        self.assertFalse(facts.freeze_eligible)


class CumulativeTerminalCompositionV1Tests(Fixture,unittest.TestCase):
    """Real six-child SQL joins; process/Git facts remain explicit unit stubs."""
    def setUp(self):
        super().setUp()
        from dataclasses import replace
        from gossip_harness.peer_financial_authority_v3 import profile_manifest
        base=synthetic_plan()
        self.plan=replace(base,cohort=replace(base.cohort,model_profiles_sha256=digest({'mini':profile_manifest(self.worker)})))
        self.roster=self.plan.roster
        self.records=Records(self.chain)
        self.records.put('contract',self.plan.record())
        self.records.put('ledger',ledger_identity(self.path))
        self.process_stubs={}

    def permit(self,opening):
        value=super().permit(opening)
        value['sources']=self.plan.source_pins
        value['max_workers']=value['execution_design']['max_workers']=self.plan.executor_slots
        value['execution_design_sha256']=digest(value['execution_design'])
        return value

    def evidence(self,status='completed'):
        from gossip_harness.peer_financial_terminal_v1 import ChildTerminalSealRequest, EvidenceReference
        stops=[]
        for actor in self.child.actors:
            value={'protocol':'financial-role-stop-v1','roster_sha256':self.roster.sha256,'cohort':self.child.cohort,
                'trajectory':self.child.trajectory,'actor':actor,'stopped':True}
            raw=canonical_payload(value)
            name='unit-stop-'+sha(actor.encode())+'.json'
            self.chain.retain(name,raw)
            stops.append(EvidenceReference(name,sha(raw)))
        value={'protocol':'financial-child-terminal-v1','roster_sha256':self.roster.sha256,'cohort':self.child.cohort,
            'trajectory':self.child.trajectory,'status':status,'final_source_sha256':'f'*64}
        raw=canonical_payload(value)
        name='unit-terminal-'+sha(self.child.cohort.encode())+'.json'
        self.chain.retain(name,raw)
        return ChildTerminalSealRequest(tuple(stops),EvidenceReference(name,sha(raw)))

    def complete_six(self,barrier_mode="valid"):
        from dataclasses import replace
        from gossip_harness.peer_project_contract_v2 import to_dict
        from gossip_harness.peer_financial_terminal_v1 import verified_study_barrier
        receipts=[]
        for index,child in enumerate(self.roster.children):
            self.select(index)
            self.context=replace(self.context,execution_contract_sha256=self.plan.sha256)
            self.contract['execution_contract_sha256']=self.plan.sha256
            for spec in self.contract['task_specs']:
                spec['context']=to_dict(self.context)
            ckey,rkey='child.'+child.trajectory,'runtime.'+child.trajectory
            self.records.put(ckey+'.begin',{'trajectory':plain(asdict(self.plan.cohort.trajectories[index])),
                'started_at':0,'deadline':30,'contract_sha256':self.plan.sha256})
            self.open(max_workers=self.plan.executor_slots)
            self.records.put(rkey+'.permit',self.last_permit)
            self.records.put(rkey+'.financial-config',self.authority.config)
            status='stopped_failure' if index==0 else 'completed'
            request=self.evidence(status)
            name='unit-process-confirmation-'+str(index)+'.json'
            self.chain.retain(name,b'{"unit_process_stub":true}')
            confirmation=self.chain.position(name)
            seal=self.authority.seal_terminal(self.authority.prepare_terminal_snapshot(request))
            config=dict(self.authority.config)
            self.authority.close()
            self.authority=None
            runtime_root=self.root/('child-'+str(index))
            for directory in ('provider-journals','financial-payloads','seed-mesh','finance-mesh','roles','processes','private-git','protected.git'):
                (runtime_root/directory).mkdir(parents=True)
            file=runtime_root/'protected.git'/'unit-source'
            file.write_bytes(b'unit source only')
            inventory=[{'path':str(file),'sha256':sha(file.read_bytes()),'size':file.stat().st_size}]
            source={'protocol':PROTOCOL,'kind':'final-source','registration':{'unit_stub':True},
                'repository':str(runtime_root/'protected.git'),'commit_oid':'c'*40,'tree_oid':'f'*40,
                'source_identity_protocol':SOURCE_PROTOCOL,'source_sha256':'f'*64,'files':[]}
            counts=dict(planned=len(child.actors),launched=len(child.actors),exited=len(child.actors),
                        never_launched=0,known_launch_failed=0,missing_final=0)
            self.process_stubs[child.cohort]=(source,counts,{'request':request.record()},confirmation)
            self.records.put(rkey+'.originals',{'writer_lifetimes_closed':True,'ledger_identity':ledger_identity(self.path),
                'cohort':child.cohort,'config':config,'config_sha256':digest(config),'complete_child_files':inventory,
                'original_paths':{'runtime_root':str(runtime_root),'protected_git':str(runtime_root/'protected.git')}})
            self.records.put(ckey+'.terminal',{'trajectory_id':child.trajectory,'status':status,'acceptance_authority':False,
                'final_source':{'unit_stub':True},'financial_seal':{'raw_utf8':seal.raw.decode(),
                    'publication_name':seal.publication_name,'commitment':asdict(seal.commitment)}})
            receipts.append(seal)
        barrier=verified_study_barrier(self.roster,self.chain,self.chain.commitment,tuple(receipts),
            existing_ledger_path=self.path,expected_ledger_identity=ledger_identity(self.path))
        if barrier_mode=='invalid':
            barrier={**barrier,'role_count':95}
        if barrier_mode!='missing':
            self.records.put('complete-cohort-barrier',barrier)

    def observe(self):
        with patch('gossip_harness.cumulative_terminal_originals_v1._process',
                   side_effect=lambda *args,**kwargs:self.process_stubs[kwargs['cohort']]):
            return audit_terminal_cohort(self.chain,self.chain.commitment,plan=self.plan,
                ledger_identity=ledger_identity(self.path),repository=Path(__file__).resolve().parents[1])

    def test_six_real_sql_seals_compose_with_explicit_stubbed_process_boundary(self):
        self.complete_six()
        facts=self.observe()
        self.assertTrue(facts.freeze_eligible,[slot.evidence_errors for slot in facts.slots])
        self.assertEqual([slot.outcome for slot in facts.slots],['stopped_failure']+['completed']*5)
        self.assertEqual(facts.barrier_sha256,sha(self.chain.read(facts.barrier_name)))
        self.assertTrue(all(slot.financial_origin['mode']=='fixture' for slot in facts.slots))
        self.assertFalse(facts.acceptance_authority)

    def test_changed_closed_originals_keep_all_six_slots_and_revoke_global_freeze(self):
        self.complete_six()
        (self.root/'child-2'/'protected.git'/'unit-source').write_bytes(b'changed after closed inventory')
        facts=self.observe()
        self.assertFalse(facts.freeze_eligible)
        self.assertEqual(len(facts.slots),6)
        self.assertEqual([slot.outcome for slot in facts.slots[:2]],['stopped_failure','completed'])
        self.assertEqual(facts.slots[2].outcome,'evidence_unknown')
        self.assertIsNone(facts.barrier)

    def test_missing_global_barrier_preserves_six_authentic_terminal_slots(self):
        self.complete_six(barrier_mode='missing')
        facts=self.observe()
        self.assertEqual([slot.outcome for slot in facts.slots],['stopped_failure']+['completed']*5)
        self.assertFalse(facts.freeze_eligible)
        self.assertTrue(any(error.startswith('global_barrier_unavailable:') for error in facts.evidence_errors))
        self.assertIsNone(facts.barrier)

    def test_incorrect_global_barrier_preserves_six_authentic_terminal_slots(self):
        self.complete_six(barrier_mode='invalid')
        facts=self.observe()
        self.assertEqual([slot.outcome for slot in facts.slots],['stopped_failure']+['completed']*5)
        self.assertFalse(facts.freeze_eligible)
        self.assertIsNone(facts.barrier_name)

    def test_global_pending_git_intent_blocks_freeze_without_erasing_sealed_subjects(self):
        self.complete_six()
        import sqlite3
        with sqlite3.connect(self.path) as db:
            db.execute('INSERT INTO intents VALUES (?,?,?,?,?,?,?)',('later-intent','/fixture','a'*40,'b'*40,'[]','pending',None))
        facts=self.observe()
        self.assertFalse(facts.freeze_eligible)
        self.assertEqual([slot.outcome for slot in facts.slots],['stopped_failure']+['completed']*5)
        self.assertIn('cumulative_wallet_pending_git_intent',facts.evidence_errors)
        self.assertIsNone(facts.barrier)

    def test_changed_original_historical_spend_blocks_freeze_preserving_six_slots(self):
        self.complete_six()
        import sqlite3
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE reservations SET spent=1001 WHERE id='historical-payment'")
        facts=self.observe()
        self.assertFalse(facts.freeze_eligible)
        self.assertEqual([slot.outcome for slot in facts.slots],['stopped_failure']+['completed']*5)
        self.assertTrue(any('Current wallet usage differs' in error for error in facts.evidence_errors))

    def test_late_unrelated_unsettled_reservation_blocks_freeze_preserving_six_slots(self):
        self.complete_six()
        import sqlite3
        with sqlite3.connect(self.path) as db:
            db.execute("INSERT INTO reservations VALUES ('late','historical',1,1,NULL,'reserved')")
        facts=self.observe()
        self.assertFalse(facts.freeze_eligible)
        self.assertEqual([slot.outcome for slot in facts.slots],['stopped_failure']+['completed']*5)
        self.assertTrue(any('invalid or unsettled reservation' in error for error in facts.evidence_errors))

    def test_compensating_historical_change_and_settled_suffix_cannot_preserve_wallet_authority(self):
        self.complete_six()
        import sqlite3
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE reservations SET spent=999 WHERE id='historical-payment'")
            db.execute("INSERT INTO reservations VALUES ('late','historical',1,1,1,'settled')")
        facts=self.observe()
        self.assertFalse(facts.freeze_eligible)
        self.assertEqual([slot.outcome for slot in facts.slots],['stopped_failure']+['completed']*5)
        self.assertTrue(any('Historical wallet rows changed' in error for error in facts.evidence_errors))

    def test_zero_cost_unowned_reservation_suffix_cannot_pass_equal_totals(self):
        self.complete_six()
        import sqlite3
        with sqlite3.connect(self.path) as db:
            db.execute("INSERT INTO reservations VALUES ('late','historical',1,0,0,'settled')")
        facts=self.observe()
        self.assertFalse(facts.freeze_eligible)
        self.assertTrue(any('unowned reservation suffix' in error for error in facts.evidence_errors))

    def test_same_inode_reservation_schema_without_unique_ids_cannot_collapse_suffix(self):
        self.complete_six()
        import sqlite3
        original_identity=ledger_identity(self.path)
        with sqlite3.connect(self.path) as db:
            db.execute("ALTER TABLE reservations RENAME TO original_reservations")
            db.execute("CREATE TABLE reservations (id TEXT,task_id TEXT,epoch INTEGER,amount INTEGER,spent INTEGER,state TEXT)")
            db.execute("INSERT INTO reservations SELECT * FROM original_reservations")
            db.execute("INSERT INTO reservations VALUES ('historical-payment','historical',1,0,0,'settled')")
            db.execute("DROP TABLE original_reservations")
        self.assertEqual(ledger_identity(self.path),original_identity)
        facts=self.observe()
        self.assertFalse(facts.freeze_eligible)
        self.assertEqual([slot.outcome for slot in facts.slots],['stopped_failure']+['completed']*5)
        self.assertTrue(any('reservation identity is duplicated' in error for error in facts.evidence_errors))
