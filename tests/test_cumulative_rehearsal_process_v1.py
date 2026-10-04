"""Reader boundary controls; synthetic linked records are never a rehearsal."""
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch, Mock

from gossip_harness.candidate_checkpoint_chain_v1 import CheckpointChain
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from gossip_harness.candidate_observation_admission_v1 import PROTOCOL as SOURCE_PROTOCOL, source_manifest, source_sha256
from gossip_harness.cumulative_process_evidence_v1 import PROTOCOL, ROLE_PROTOCOL, closure_name, confirmation_name
from gossip_harness.cumulative_rehearsal_process_v1 import audit_process_closure
from gossip_harness.peer_financial_authority_v2 import canonical_payload, FinancialError
from gossip_harness.cumulative_study_controller_v1 import Records
from gossip_harness.peer_financial_terminal_v1 import ChildRegistration, TerminalRoster, sha, digest


class CumulativeRehearsalProcessV1Tests(unittest.TestCase):
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
        self.source = {'source_sha256':source_sha256(self.files), 'commit_oid':'c'*40}
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

    def records(self, mutation=None, *, confirm=True):
        reg = self.retain({'protocol':PROTOCOL,'kind':'registration','roster':self.roster.record(),
            'roster_sha256':self.roster.sha256,'child':self.child.record(),'sources':self.sources,
            'max_generations':2,'parent_pid':1,'parent_thread':1,'protected_repository':self.protected})
        rows=[]
        for ordinal,actor in enumerate(self.child.actors):
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
            launch_value={'protocol':PROTOCOL,'kind':'launch','identity':identity,'intent':intent,
                'pid':100+ordinal,'process_group':100+ordinal,'parent_pid':1}
            if mutation:
                mutation('launch',actor,launch_value)
            launch=self.retain(launch_value)
            events={}
            for event in ('ready','final'):
                value={'protocol':ROLE_PROTOCOL,'event':event,'identity':identity,'pid':100+ordinal}
                if event=='final':
                    value.update(outcome='stopped',completed_action_ids=['action'],remaining_action_ids=[])
                if mutation:
                    mutation(event,actor,value)
                (directory/(event+'.json')).write_bytes(canonical_payload(value))
                events[event]=self.retain(value)
            wait_value={'protocol':PROTOCOL,'kind':'wait','identity':identity,'launch':launch,**events,
                'pid':100+ordinal,'returncode':0,'reaped_with':'Popen.wait','process_group_empty':True,
                'event_errors':[],'parent_pid':1}
            if mutation:
                mutation('wait',actor,wait_value)
            wait=self.retain(wait_value)
            rows.append({'actor':actor,'incarnations':[{'generation':0,'intent':intent,'launch':launch,
                **events,'wait':wait,'launch_failed':None}], 'never_launched':False})
        source_ref=self.retain({'protocol':PROTOCOL,'kind':'final-source','registration':reg,
            'repository':str(self.root),'tree_oid':'f'*40,'source_identity_protocol':SOURCE_PROTOCOL,
            **self.source,'files':source_manifest(self.files)})
        stops=[]
        for row in rows:
            stop=self.retain({'protocol':'financial-role-stop-v1','roster_sha256':self.roster.sha256,
                'cohort':self.child.cohort,'trajectory':self.child.trajectory,'actor':row['actor'],'stopped':True})
            row['stop']=stop
            stops.append(stop)
        terminal=self.retain({'protocol':'financial-child-terminal-v1','roster_sha256':self.roster.sha256,
            'cohort':self.child.cohort,'trajectory':self.child.trajectory,'status':'completed',
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

    def audit(self):
        # Only the read-only Git boundary is mocked: this is an integrity unit
        # fixture, not actual process/Git execution or financial qualification.
        with patch('gossip_harness.cumulative_rehearsal_process_v1.GitStore',return_value=Mock(head=lambda:'c'*40)), \
             patch('gossip_harness.cumulative_rehearsal_process_v1.capture_git_source',return_value=('f'*40,self.files)):
            return audit_process_closure(self.chain,self.chain.commitment,roster=self.roster,
                cohort=self.child.cohort,expected_sources=self.sources,expected_source=self.source,runtime_root=self.root,
                execution_repository=self.root,python_executable='/fixture/python')

    def test_complete_linked_record_fixture_is_scoped_not_live_qualification(self):
        self.records()
        proof=self.audit()
        self.assertEqual((len(proof.actors),proof.incarnation_count),(8,8))
        self.assertFalse(proof.live_qualification)

    def test_child_final_without_owned_wait_is_rejected(self):
        self.records(lambda kind,actor,value: value.update(reaped_with='child-said-so') if kind=='wait' else None)
        with self.assertRaises(FinancialError):
            self.audit()

    def test_wrong_launch_pid_does_not_match_original_child_event(self):
        self.records(lambda kind,actor,value: value.update(pid=999) if kind=='ready' else None)
        with self.assertRaises(FinancialError):
            self.audit()

    def test_incomplete_final_actions_cannot_complete_rehearsal(self):
        self.records(lambda kind,actor,value: value.update(remaining_action_ids=['pending']) if kind=='final' else None)
        with self.assertRaises(FinancialError):
            self.audit()

    def test_missing_role_or_reordered_roster_is_rejected(self):
        self.records(lambda kind,actor,value: value['roles'].reverse() if kind=='closure' else None)
        with self.assertRaises(FinancialError):
            self.audit()

    def test_changed_original_event_file_is_rejected(self):
        self.records()
        (self.root/'processes'/self.child.actors[0]/'0'/'final.json').write_bytes(b'{}')
        with self.assertRaises(FinancialError):
            self.audit()

    def test_stale_independent_head_cannot_authenticate_new_suffix(self):
        expected=self.chain.commitment
        self.records()
        with self.assertRaises(ValueError):
            audit_process_closure(self.chain,expected,roster=self.roster,cohort=self.child.cohort,
                expected_sources=self.sources,expected_source=self.source,runtime_root=self.root,
                execution_repository=self.root,python_executable='/fixture/python')

    def test_changed_final_git_bytes_fail_source_binding(self):
        self.records()
        self.files={'src/a.py':b'changed\n'}
        with self.assertRaises(FinancialError):
            self.audit()

    def test_never_launched_role_does_not_count_as_complete(self):
        self.records(lambda kind,actor,value: value['roles'][0].update(never_launched=True) if kind=='closure' else None)
        with self.assertRaises(FinancialError):
            self.audit()

    def test_prepared_closure_without_producer_confirmation_is_rejected(self):
        self.records(confirm=False)
        with self.assertRaises(ValueError):
            self.audit()
