"""Controller-history integrity fixtures; no process, Git or rehearsal credit."""
from dataclasses import asdict, replace
from pathlib import Path
import hashlib
import json
import tempfile
import unittest
from unittest.mock import Mock, patch

from gossip_harness.candidate_checkpoint_chain_v1 import CheckpointChain
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from gossip_harness.candidate_observation_admission_v1 import source_sha256
from gossip_harness.cumulative_rehearsal_validator_v1 import (
    _Originals, _histories, _faults, _fault_publication_barrier, audit_cohort_originals, REQUIRED_READER_SOURCES,
)
from gossip_harness.cumulative_rehearsal_process_v1 import ProcessAudit
from gossip_harness.cumulative_study_controller_v1 import StudyController, Records, PUBLIC_PURPOSE, PACKAGES, REVIEWERS, package_for, plain
from gossip_harness.peer_financial_authority_v2 import canonical_payload
from gossip_harness.peer_financial_terminal_v1 import FinancialError, digest, sha
from gossip_harness.peer_project_contract_v2 import EvidenceRef, to_dict
from gossip_harness.peer_role_loop_v2 import directive_id
from tests.financial_rehearsal_fixture_v1 import synthetic_plan


class CumulativeRehearsalValidatorV1Tests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.root=Path(self.temp.name).resolve()
        self.head=ExternalHead.create(self.root/'anchor',journal_roots=(self.root/'raw',self.root/'delta'))
        self.chain=CheckpointChain.create(self.root/'raw',self.root/'delta',context={'fixture':'history-reader'},authority=self.head)
        self.records=Records(self.chain)
        self.plan=synthetic_plan()
        self.child=self.plan.roster.children[0]
        self.trajectory=self.plan.cohort.trajectories[0]
        self.files={'base.py':b'pass\n'}
        self.source={'commit_oid':'c'*40,'source_sha256':source_sha256(self.files),'files':{'base.py':'pass\n'}}
        self.addCleanup(self.cleanup)

    def cleanup(self):
        self.chain.close()
        self.head.close()
        self.temp.cleanup()

    def retain_history(self, *, wrong_order=False, unscoped=False, output_wrong=False, broken_source=False,
                       wrong_instruction=False, private_offer_wrong=False, cas_wrong=False,
                       release_wrong=False, initial_wrong=False, final_wrong=False, selection_wrong=False, repaired_rejection=False):
        # Actual retained producer schemas with a deterministic synthetic Git
        # read model. No Git operation, provider, process or acceptance runs.
        self.git_files={}
        self.git_changed={}
        self.serial=0
        def commit(files):
            self.serial+=1
            oid=format(self.serial,'040x')
            self.git_files[oid]=dict(files)
            return oid
        def source(oid):
            files=self.git_files[oid]
            return {'commit_oid':oid,'source_sha256':source_sha256(files),'files':{n:v.decode() for n,v in files.items()}}
        def publication(key,kind,payload):
            request={'kind':kind,'payload_sha256':digest(payload),'command':key}
            ref=EvidenceRef(digest({'publication':key}),'seed',kind,digest(payload))
            self.records.put('publish.'+key+'.intent',request)
            self.records.put('publish.'+key,{'ref':to_dict(ref),'payload':payload,**request})
            return ref
        def candidate(old,offered,new,paths):
            self.git_changed[(old,offered)]=tuple(sorted(paths))
            return {'status':'prepared','old_head':old,'offered_sha':offered,'candidate_sha':new,
                    'detail':'Synthetic read model','changed_paths':sorted(paths)}
        current=commit(self.files)
        key='child.'+self.child.trajectory
        runtime='runtime.'+self.child.trajectory
        self.records.put(key+'.begin',{'trajectory':plain(asdict(self.trajectory)),'started_at':0,'deadline':30,
            'contract_sha256':self.plan.sha256})
        self.records.put(runtime+'.source-initialization.intent',{'path':str(self.root/'protected.git'),
            'initial_files_sha256':digest(self.plan.initial_files)})
        initial=source(current)
        if initial_wrong:
            initial={**initial,'files':{'foreign.py':'pass'}}
        self.records.put(runtime+'.source-initialization.result',initial)
        producer=object.__new__(StudyController)
        producer.plan=self.plan
        for number,release in enumerate(self.plan.releases,1):
            mkey=key+'.'+release.milestone
            rkey=runtime+'.release.'+release.milestone
            self.records.put(rkey+'.proposal-intent',{'old':current,'changes_sha256':digest(release.files)})
            files={**self.git_files[current],**{n:v.encode() for n,v in release.files.items()}}
            offered=commit(files)
            self.records.put(rkey+'.git-intent',{'candidate':candidate(current,offered,offered,release.files),
                'release_sha256':release.sha256})
            current=offered
            result={**{k:v for k,v in source(current).items() if k!='files'},'release_sha256':release.sha256}
            self.records.put(rkey,result)
            self.records.put(mkey+'.release',result)
            if release_wrong and number==1:
                self.git_files[offered]={**files,'hidden.py':b'unreleased'}
            for generation in range(2 if repaired_rejection and number==1 else 1):
                gkey=mkey+'.g'+str(generation)
                reject=repaired_rejection and number==1 and generation==0
                base,current_files=current,dict(files)
                before=source_sha256(files)
                source_ref=publication(gkey+'.source','project-source',{'files':{n:v.decode() for n,v in files.items()},'base_sha':base})
                evidence=()
                if generation:
                    feedback=self.records.read(mkey+'.g'+str(generation-1)+'.terminal')
                    evidence=(publication(gkey+'.feedback','cumulative-feedback',feedback),)
                groups={}
                for suffix in ('builds','reviews'):
                    actors=tuple(a for a in self.child.actors if (a.rsplit('.',1)[-1] in REVIEWERS)==(suffix=='reviews'))
                    if not wrong_order:
                        actors=tuple(sorted(actors,key=lambda actor:digest({'seed':self.trajectory.block_seed_sha256,'actor':actor})))
                    directives=dict(producer._directives(0,number,generation,source_ref,evidence if suffix=='builds' else (),reviews=suffix=='reviews'))
                    if suffix=='reviews':
                        for actor,directive in directives.items():
                            package=package_for(actor)
                            scoped=[{'actor':item['actor'],'directive_id':item['directive_id'],'original_ref':item['original_ref'],
                                     'snapshot':item['snapshot'],'proposal':item['result_payload']}
                                    for item in groups['builds'] if package_for(item['actor'])==package]
                            ref=publication(gkey+'.frontier.'+package,'cumulative-frontier',{'stage_id':gkey,'package':package,
                                'builds':scoped,'shared_source_ref':to_dict(source_ref)})
                            directives[actor]=replace(directive,evidence_refs=(ref,))
                    declarations=[]
                    for actor in actors:
                        directive=directives[actor]
                        if wrong_instruction and suffix=='builds':
                            directive=replace(directive,instructions='Fixture instruction')
                        identity=directive_id(directive)
                        self.records.put(runtime+'.directive.'+identity,{'actor':actor,'directive':directive.to_dict()})
                        scope=digest({'source':to_dict(source_ref),'evidence':[to_dict(ref) for ref in directive.evidence_refs]})
                        stage=gkey+('.review' if suffix=='reviews' else '.build')
                        if suffix=='reviews' and not unscoped:
                            stage+='.'+scope[:12]
                        package=package_for(actor)
                        if suffix=='reviews':
                            selected=next(row for row in groups['builds'] if package_for(row['actor'])==package)
                            decision={'stage_id':gkey,'package':package,'selected_actor':selected['actor'],'reasons':'Scoped fixture choice',
                                      'tests':['Original authored '+release.milestone+' case']}
                            changes={'decision.json':'{' if reject else canonical_payload(decision).decode()}
                        else:
                            changes={self.plan.package_paths[package][0]:'# '+gkey+' '+package+'\n'}
                        declarations.append({'actor':actor,'stage_id':stage,'directive_id':identity,
                            'snapshot':{'reply':{'state':'completed'}},'original_ref':to_dict(EvidenceRef(digest({'actor':actor,'stage':stage}),
                                actor,'cumulative-result',digest({'fixture':identity}))),
                            'result_payload':{'payload':{'changes':changes}},
                            'worker_request':{'base_sha':base,'files':{n:v.decode() for n,v in files.items()}}})
                    refs=[]
                    for index,row in enumerate(declarations):
                        slot=gkey+'.'+suffix+'.original.'+str(index)
                        self.records.put(slot,row)
                        refs.append({'slot':slot,'name':Records.name(slot),'sha256':digest(row)})
                    self.records.put(gkey+'.'+suffix,{'count':len(refs),'originals':refs})
                    groups[suffix]=declarations
                if reject:
                    integrated={'status':'selection_rejected','package':PACKAGES[0],'reason':'Invalid JSON',
                                'selections':[],'acceptance_authority':False}
                else:
                    selections,merges=[],[]
                    for package in PACKAGES:
                        selected=next(row for row in groups['builds'] if package_for(row['actor'])==package)
                        review=next(row for row in groups['reviews'] if package_for(row['actor'])==package)
                        decision=json.loads(review['result_payload']['payload']['changes']['decision.json'])
                        selections.append({'package':package,'reviewer':review['actor'],'selected_actor':selected['actor'],
                            'proposal_ref':selected['original_ref'],'review_ref':review['original_ref'],'decision':decision})
                        changes={n:v.encode() for n,v in selected['result_payload']['payload']['changes'].items()}
                        offered=commit({**files,**changes})
                        new=commit({**current_files,**changes})
                        mergekey=gkey+'.merge.'+package
                        pin=digest(selected)
                        private=str(self.root/'private-git'/hashlib.sha256((gkey+selected['actor']).encode()).hexdigest())
                        intent={'proposal_sha256':pin,'base_sha':base,'git_path':private}
                        self.records.put(mergekey+'.proposal-intent',intent)
                        proposal={**intent,'offered_sha':offered}
                        self.records.put(mergekey+'.proposal',proposal)
                        self.records.put(mergekey+'.git-intent',{'proposal_sha256':pin,'candidate':candidate(current,offered,new,changes)})
                        merge={'proposal_sha256':pin,'private_git':proposal,**{k:v for k,v in source(new).items() if k!='files'}}
                        self.records.put(mergekey,merge)
                        merges.append(merge)
                        current,current_files=new,dict(self.git_files[new])
                        if number==1 and package==PACKAGES[0]:
                            if private_offer_wrong:
                                self.git_files[offered]={**files,'unselected.py':b'foreign'}
                            if cas_wrong:
                                self.git_files[new]={**current_files,'unselected.py':b'foreign'}
                    integrated={'status':'integrated',**{k:v for k,v in source(current).items() if k!='files'},
                                'selections':selections,'private_git_merges':merges,'acceptance_authority':False}
                    if selection_wrong:
                        integrated['selections'][0]['proposal_ref']=integrated['selections'][1]['proposal_ref']
                self.records.put(gkey+'.integration',integrated)
                self.source=source(current)
                self.records.put(gkey+'.public-evaluation.intent',{'source_sha256':self.source['source_sha256'],
                    'commit_oid':current,'release_sha256':release.sha256,'purpose':PUBLIC_PURPOSE})
                outcomes=[[x,'passed'] for x in release.ordered_check_ids]
                output={'purpose':PUBLIC_PURPOSE,'outcomes':[] if output_wrong else outcomes}
                evaluation={'status':'completed','commit_oid':current,'source_sha256':self.source['source_sha256'],
                    'release_sha256':release.sha256,'ordered_check_ids':list(release.ordered_check_ids),'outcomes':outcomes,
                    'raw_receipt':{'sandbox':{'output':canonical_payload(output).decode(),'cleanup_verified':True,
                                             'output_truncated':False,'status':'passed','schema_version':1,
                                             'image_id':self.plan.runtime['image'],'command':list(release.command),
                                             'checks_sha256':hashlib.sha256(json.dumps(release.checks,sort_keys=True,ensure_ascii=True,separators=(',',':')).encode()).hexdigest(),
                                             'timeout_seconds':self.plan.runtime['public_timeout_seconds'],'timed_out':False,
                                             'exit_code':0,'container_name':'gossip-check-'+format(number,'032x'),
                                             'staging':{'files':len(current_files),'bytes':sum(map(len,current_files.values())),'excluded_paths':['.git']}}},'purpose':PUBLIC_PURPOSE}
                self.records.put(gkey+'.public-evaluation.result',evaluation)
                self.records.put(gkey+'.terminal',{'milestone':release.milestone,'generation':generation,
                    'source_before':'f'*64 if broken_source else before,'source_after':self.source['source_sha256'],'commit_oid':current,
                    'public_result':evaluation,'integration':integrated,'public_passed':not reject,'acceptance_authority':False})
            self.records.put(mkey+'.terminal',{'milestone':release.milestone,'status':'public_completed',
                'source':self.source,'acceptance_authority':False})
        final={**self.source,'files':{'unverified.py':'foreign'}} if final_wrong else self.source
        self.records.put(key+'.terminal',{'trajectory_id':self.child.trajectory,'status':'completed',
            'milestone_records':[key+'.'+r.milestone+'.terminal' for r in self.plan.releases],
            'final_source':final,'acceptance_authority':False})

    def audit_history(self):
        def store(path):
            value=Mock(path=path,head=lambda:self.source['commit_oid'],is_ancestor=lambda *args:True,
                       _introduced_paths=lambda old,offered:self.git_changed[(old,offered)],
                       _candidate_ref=lambda old,new:'refs/harness/candidates/'+old+'/'+new,
                       _git=lambda *args:args[-1].rsplit('/',1)[-1])
            return value
        with patch('gossip_harness.cumulative_rehearsal_validator_v1.GitStore',side_effect=store), \
                patch('gossip_harness.cumulative_rehearsal_validator_v1.capture_git_source',side_effect=lambda store,oid:('d'*40,self.git_files[oid])):
            return _histories(_Originals(self.chain),self.plan,0,self.root/'protected.git')

    def test_seeded_scope_split_original_history_roundtrip(self):
        self.retain_history()
        rows,terminal=self.audit_history()
        self.assertEqual(set(rows),set(self.child.actors))
        self.assertTrue(all(len(values)==4 for values in rows.values()))
        self.assertEqual(terminal['status'],'completed')

    def test_roster_order_cannot_replace_actual_seeded_result_order(self):
        self.retain_history(wrong_order=True)
        with self.assertRaises(FinancialError):
            self.audit_history()

    def test_unscoped_review_stage_cannot_replace_scope_bound_originals(self):
        self.retain_history(unscoped=True)
        with self.assertRaises(FinancialError):
            self.audit_history()

    def test_passed_flag_without_matching_original_case_output_is_rejected(self):
        self.retain_history(output_wrong=True)
        with self.assertRaises(FinancialError):
            self.audit_history()

    def test_broken_source_inheritance_is_rejected(self):
        self.retain_history(broken_source=True)
        with self.assertRaises(FinancialError):
            self.audit_history()

    def test_missing_original_result_slot_cannot_be_replaced_by_group_count(self):
        key='group'
        self.records.put(key,{'count':1,'originals':[{'slot':'group.original.0','name':Records.name('group.original.0'),'sha256':'a'*64}]})
        with self.assertRaises(FinancialError):
            _Originals(self.chain).results(key,(self.child.actors[0],))

    def test_financial_only_gate_cannot_skip_new_reader_classes(self):
        from gossip_harness.peer_financial_authority_v4 import REQUIRED_TEST_CLASSES
        plan=replace(self.plan,source_pins={name:'a'*64 for name in REQUIRED_READER_SOURCES})
        with patch.object(type(plan),'verify_sources'), \
                patch('gossip_harness.cumulative_rehearsal_validator_v1._gate',side_effect=AssertionError('incomplete gate entered')):
            with self.assertRaisesRegex(FinancialError,'qualification suite'):
                audit_cohort_originals(self.chain,self.chain.commitment,plan=plan,ledger_identity={},repository=self.root,
                                       gate={},ordered_test_classes=REQUIRED_TEST_CLASSES,expected_design_envelope={})

    def test_healthy_history_cannot_relabel_an_unregistered_restart(self):
        pids=tuple((actor,(100+i,)) for i,actor in enumerate(self.child.actors))
        proof=ProcessAudit(self.child.cohort,self.child.trajectory,self.child.actors,8,pids,(),self.source['source_sha256'],'c'*40,1,2)
        _faults(_Originals(self.chain),self.plan,0,proof,{},self.root)
        changed=replace(proof,process_ids=((self.child.actors[0],(100,101)),*pids[1:]))
        with self.assertRaises(FinancialError):
            _faults(_Originals(self.chain),self.plan,0,changed,{},self.root)

    def fault_frontier(self,index,*,early=False,wrong=False,omit=False):
        child=self.plan.roster.children[index]
        trajectory=self.plan.cohort.trajectories[index]
        key='runtime.'+child.trajectory
        stage='child.'+child.trajectory+'.M1.g0.build'
        actors=sorted((actor for actor in child.actors if actor.rsplit('.',1)[-1] not in REVIEWERS),
                      key=lambda actor:digest({'seed':trajectory.block_seed_sha256,'actor':actor}))
        names=[stage+'.work']
        if trajectory.decision_placement=='durable_central_scheduler':
            for actor in actors:
                names.extend((stage+'.decision.'+actor,stage+'.assignment.'+actor))
        if omit:
            names.pop()
        def ref(slot):
            name=Records.name(slot)
            return {'name':name,'sha256':sha(self.chain.read(name)),'position':self.chain.position(name)}
        if early:
            self.records.put('publish.'+names[0]+'.intent',{'fixture':'early'})
        self.records.put(key+'.partition-start.result',{'fixture':'original acknowledged block','observed_at':1.0})
        start=ref(key+'.partition-start.result')
        if wrong:
            start['position']+=1
        self.records.put(key+'.partition-frontier',{'stage':stage,'partition_start':start,'publication_keys':names})
        for name in names:
            if not early or name!=names[0]:
                self.records.put('publish.'+name+'.intent',{'fixture':'intent'})
            self.records.put('publish.'+name,{'fixture':'original publication','ref':{'fixture':name}})
            if name==names[0]:
                self.records.put(key+'.partition-first-publication',{'boundary':'first-work-publication-durably-acknowledged',
                    'publication_key':name,'ref':{'fixture':name},'observed_at':1.01})
        self.records.put(key+'.partition-frontier-complete',{'stage':stage,'publications':[ref('publish.'+name) for name in names]})
        self.records.put(key+'.partition-end.intent',{'fixture':'healing'})

    def test_both_placement_frontiers_follow_original_partition_ack(self):
        for index in (1,5):
            self.fault_frontier(index)
            _fault_publication_barrier(_Originals(self.chain),self.plan,index)

    def test_fault_frontier_cannot_rehash_a_false_record_position(self):
        self.fault_frontier(1,wrong=True)
        with self.assertRaises(FinancialError):
            _fault_publication_barrier(_Originals(self.chain),self.plan,1)

    def test_work_publication_before_fault_ack_is_not_matching_recovery(self):
        self.fault_frontier(1,early=True)
        with self.assertRaises(FinancialError):
            _fault_publication_barrier(_Originals(self.chain),self.plan,1)

    def test_central_frontier_cannot_omit_an_original_assignment(self):
        self.fault_frontier(5,omit=True)
        with self.assertRaises(FinancialError):
            _fault_publication_barrier(_Originals(self.chain),self.plan,5)

    def test_consistently_rehashed_foreign_instructions_are_rejected(self):
        self.retain_history(wrong_instruction=True)
        with self.assertRaisesRegex(FinancialError,'Exact prospective directive'):
            self.audit_history()

    def test_private_offer_tree_must_match_selected_original_proposal(self):
        self.retain_history(private_offer_wrong=True)
        with self.assertRaisesRegex(FinancialError,'Private Git offer'):
            self.audit_history()

    def test_protected_candidate_tree_cannot_include_unselected_changes(self):
        self.retain_history(cas_wrong=True)
        with self.assertRaisesRegex(FinancialError,'Protected candidate tree'):
            self.audit_history()

    def test_release_tree_cannot_include_undeclared_source(self):
        self.retain_history(release_wrong=True)
        with self.assertRaisesRegex(FinancialError,'Original release proposal'):
            self.audit_history()

    def test_initial_source_cannot_be_adopted_from_foreign_bytes(self):
        self.retain_history(initial_wrong=True)
        with self.assertRaisesRegex(FinancialError,'Original initial source'):
            self.audit_history()

    def test_final_terminal_files_are_authenticated_not_merely_returned(self):
        self.retain_history(final_wrong=True)
        with self.assertRaisesRegex(FinancialError,'Terminal full source'):
            self.audit_history()

    def test_integration_selection_must_match_original_reviewer_and_proposal(self):
        self.retain_history(selection_wrong=True)
        with self.assertRaisesRegex(FinancialError,'Integration summary'):
            self.audit_history()

    def test_rejected_original_selection_can_repair_with_exact_feedback_and_new_directives(self):
        self.retain_history(repaired_rejection=True)
        rows,terminal=self.audit_history()
        self.assertTrue(all(len(values)==5 for values in rows.values()))
        self.assertEqual(terminal['status'],'completed')
        original=self.records.read('child.'+self.child.trajectory+'.M1.g0.integration')
        self.assertEqual(original['reason'],'Invalid JSON')
