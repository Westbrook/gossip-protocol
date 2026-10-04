"""Issued-proof mechanics and actual live-mode V5 entry, with zero network.

The complete semantic cold auditor is explicitly stubbed in component positive
fixtures; these are not rehearsal qualification or complete scope evidence.
Issuance, anchored bytes, original-head checks, V5 permit/claim/SQL/journal/lease,
and default worker transport path are real. Only urllib's opener performs local
controlled responses; no provider request or secret discovery is possible.
"""
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
import unittest
from unittest.mock import patch
from gossip_harness import cumulative_issued_qualification_v1 as issued
from gossip_harness import cumulative_rehearsal_capsule_v3 as capsule
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import peer_financial_authority_v5 as financial
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from gossip_harness.peer_financial_authority_v3 import LIVE_TRANSPORT
from gossip_harness.peer_financial_authority_v2 import ledger_identity
from gossip_harness.peer_financial_terminal_v1 import FinancialError, digest
from gossip_harness.worker import OpenAIWorker
from gossip_harness.peer_project_contract_v2 import WorkKey, to_dict
from tests.financial_v5_fixture import Fixture
from tests.test_cumulative_rehearsal_inputs_v1 import plans
from tests.test_peer_financial_authority_v2 import Clock


class LocalResponse:
    def __init__(self, response):
        self.status=response.status; self.headers=response.headers; self.body=response.body
    def read(self, limit): return self.body[:limit]
    def __enter__(self): return self
    def __exit__(self,*args): return None


class LocalOpener:
    def __init__(self, transport): self.transport=transport
    def open(self, request, timeout): return LocalResponse(self.transport(request,timeout,2_000_000))


class CumulativeIssuedQualificationV1Tests(Fixture,unittest.TestCase):
    # Disposable simulated wallet only: four 300288 reservations need 1201152,
    # plus the original 1000 opening usage. No study or live allowance changes.
    CHILD_CAP = 1_300_000
    GLOBAL_CAP = 1_500_000

    def setUp(self):
        super().setUp()
        self.ledger.increase_budget('four-overlap-component-fixture',self.GLOBAL_CAP,
            expected_old=1_000_000,reason='Fund four overlapping simulated component calls',now=2)
        self.clock=Clock()
        self.worker=OpenAIWorker('not-a-real-key-for-local-only-control',max_output_tokens=64,timeout=2)
        self.contract['transport_identity']=LIVE_TRANSPORT
        self.sources=issued.implementation_sources()
        self.sources.update(financial.source_fingerprints())
        self.repo=Path(issued.__file__).resolve().parents[1]
        _,_,plan,_=plans()
        runtime={**plan.runtime,'live_qualification_protocol':issued.PROTOCOL,
            'qualified_study_protocol':'cumulative-qualified-study-v1'}
        resources={name:getattr(plan,name) for name in ('horizon_seconds','source_generations','partition_seconds','executor_slots')}
        self.plan=replace(plan,runtime=runtime,source_pins=self.sources,
            cohort=replace(plan.cohort,resource_contract_sha256=digest({**resources,'runtime':runtime})))
        self.roster=self.plan.roster
        self.select(0)
        self.context=replace(self.context,execution_contract_sha256=self.plan.sha256)
        self.contract['execution_contract_sha256']=self.plan.sha256
        self.contract['transport_identity']=LIVE_TRANSPORT
        self.works=[WorkKey('catalog','m1','slot'+str(index),0) for index in range(4)]
        template=self.contract['task_specs'][0]
        self.contract['task_specs']=[{**template,'context':to_dict(self.context),'work':to_dict(work)} for work in self.works]
        self.base_permit=super().permit(1000)
        self.base_permit['incremental_cap_micro_usd']=self.CHILD_CAP
        self.base_permit['expected_global_cap']=self.GLOBAL_CAP
        self.base_permit['max_workers']=4
        self.base_permit['execution_design']['max_workers']=4
        self.base_permit['mode']='live'; self.base_permit['sources']=self.sources
        self.base_permit['execution_design']['runtime']={'live_qualification_protocol':issued.PROTOCOL}
        self.base_permit['execution_design_sha256']=digest(self.base_permit['execution_design'])
        self.reference={'path':str(self.root/'authored-capsule.json'),'sha256':'e'*64}
        original_root=self.root/'original-proof'; original_root.mkdir()
        self.original=original_root/'original-head-control.json'; self.original.write_bytes(b'authored independent original')
        self.original_witness=issued.FileWitness.capture(self.original)
        self.facts={'protocol':capsule.PROTOCOL,'capsule':self.reference,'live_study_sha256':self.plan.sha256,
            'fixture_study_sha256':'b'*64,'sources':self.sources,
            'children':[{'cohort':child.cohort,'trajectory':child.trajectory} for child in self.roster.children],
            'execution_designs':[deepcopy(self.base_permit['execution_design']) for _ in range(6)],
            'wallet_authorization':{'protocol':capsule.WALLET_POLICY,'ledger_identity':ledger_identity(self.path),
                'initial_opening_usage':1000,'expected_global_cap':self.GLOBAL_CAP,'children':[
                    {'cohort':child.cohort,'trajectory':child.trajectory,'incremental_cap_micro_usd':self.CHILD_CAP,'max_workers':4}
                    for child in self.roster.children]},'final_originals':{'component_stub':True},
            'candidate_acceptance_transferred':False}
        # This sole semantic-boundary substitution is why no control below is a
        # complete matching rehearsal. Loaded-source checking is disabled only
        # so this explicit replacement does not masquerade as original code.
        self.loaded=patch.object(admission,'verify_loaded_sources').start()
        self.addCleanup(patch.stopall)
        self.cold=patch.object(capsule,'audit_closed_capsule',return_value=self.facts).start()
        self.witnesses=patch.object(issued,'_inputs_and_heads',return_value=(self.original_witness,)).start()
        patch('urllib.request.build_opener',return_value=LocalOpener(self.transport)).start()
        self.grants=[]
        self.session=self.new_session()
        self.addCleanup(self.revoke_all)

    def revoke_all(self):
        for grant in self.grants:
            if not grant._revoked:
                try: grant.revoke('component fixture cleanup')
                except (ValueError,OSError): pass

    def new_session(self):
        number=len(self.grants)
        raw,delta,head=(self.root/(name+str(number)) for name in ('grant-raw','grant-delta','grant-head'))
        authority=ExternalHead.create(head,journal_roots=(raw,delta)); self.addCleanup(authority.close)
        result=issued.IssuedQualification.issue(self.reference,root=raw,delta_root=delta,authority=authority,
            live_plan=self.plan,repository=self.repo)
        self.grants.append(result)
        return result

    def permit(self, opening):
        permit=deepcopy(self.base_permit); permit['expected_opening_usage']=opening
        permit['cohort_contract_sha256']=digest(self.contract)
        permit['qualification']=self.session.reference_for({'cohort':self.child.cohort,'trajectory':self.child.trajectory})
        return permit

    def open_live(self, **kwargs):
        return self.open(mode='live',qualification_session=self.session,clock=self.clock,max_workers=4,
            incremental_cap_micro_usd=self.CHILD_CAP,expected_global_cap=self.GLOBAL_CAP,**kwargs)

    def check(self, **kwargs):
        permit=self.permit(1000)
        values=dict(reference=permit['qualification'],execution_design=permit['execution_design'],sources=self.sources,
            live_limits={key:permit[key] for key in ('expected_opening_usage','expected_global_cap','incremental_cap_micro_usd','max_workers')},
            ledger_identity=self.contract['ledger_identity'])
        values.update(kwargs)
        return self.session.check(**values)

    def test_anchor_retains_exact_issuance_and_admits_real_live_mode_path(self):
        self.open_live(); action,_=self.start(); result=self.terminal(action)
        self.assertEqual(result.state,'completed'); self.assertEqual(len(self.transport.calls),1)
        self.assertEqual(self.cold.call_count,1)
        self.assertFalse(self.check()['raw_regraded_at_invocation'])
        self.assertFalse(self.check()['candidate_acceptance_authority'])
        self.assertEqual(self.authority.config['mode'],'live')
        self.assertIsNotNone(self.authority._qualification_owner_witness)

    def test_four_actual_live_workers_overlap_without_cold_lock_or_global_halt(self):
        self.open_live(); self.transport.release.clear()
        actions=[self.start(index)[0] for index in range(4)]
        # Both actual provider-entry paths must reach the local opener while the
        # other remains blocked there; a grant lock across I/O would fail this.
        import time
        until=time.monotonic()+10
        while len(self.transport.calls)<4 and time.monotonic()<until: time.sleep(.005)
        self.transport.release.set()
        self.assertEqual(len(self.transport.calls),4)
        self.assertEqual([self.terminal(action).state for action in actions],['completed']*4)
        self.assertEqual(self.cold.call_count,1); self.assertFalse(self.authority.failed_closed)

    def test_changed_request_after_fresh_grant_never_enters_transport(self):
        self.open_live()
        recheck=financial.CumulativeAuthorityV5._recheck_qualified_entry
        observed=[]; denials=[]
        def changed(owner,worker,request):
            # Real issued-proof guard has returned; substitute only the in-memory
            # request. Python equality treats True == 1, but the exact wire
            # comparison must reject this change before original SQL/lease use.
            substituted=replace(request,attempt=True)
            observed.append((request==substituted,type(request.attempt),type(substituted.attempt)))
            try: return recheck(owner,worker,substituted)
            except FinancialError as error:
                denials.append(str(error)); raise
        with patch.object(financial.CumulativeAuthorityV5,'_recheck_qualified_entry',autospec=True,side_effect=changed):
            action,_=self.start(); result=self.terminal(action)
        self.assertEqual(observed,[(True,int,bool)])
        self.assertEqual(denials,['Original request, profile or pending state changed after qualification'])
        self.assertEqual(result.state,'unknown'); self.assertTrue(self.authority.failed_closed)
        self.assertEqual(self.transport.calls,[])
        _,_,_,original=self.authority._action(self.actor,action.request_id)
        self.assertIs(type(original.attempt),int); self.assertEqual(original.attempt,1)

    def test_revocation_before_claim_creates_no_reservation(self):
        self.open_live(); before=self.rows('reservations')
        self.session.revoke('independent revocation')
        with self.assertRaises(FinancialError): self.start()
        self.assertEqual(self.rows('reservations'),before); self.assertEqual(self.transport.calls,[])

    def test_changed_original_head_blocks_new_claim(self):
        self.open_live(); self.original.write_bytes(b'changed current original head')
        with self.assertRaises(FinancialError): self.start()
        self.assertEqual(self.transport.calls,[])

    def test_changed_grant_raw_blocks_new_claim(self):
        self.open_live(); Path(self.session.proof_reference['path']).write_bytes(b'{}')
        with self.assertRaises(FinancialError): self.start()
        self.assertEqual(self.transport.calls,[])

    def test_equal_bytes_replacement_of_original_is_rejected(self):
        self.open_live(); replacement=self.root/'replacement'; replacement.write_bytes(self.original.read_bytes())
        replacement.replace(self.original)
        with self.assertRaises(FinancialError): self.start()

    def test_fabricated_reference_or_foreign_child_never_authorizes(self):
        for change in ({'accepted':True},{**self.permit(1000)['qualification'],'approved_child':{'cohort':'foreign','trajectory':'other'}}):
            with self.subTest(change=change),self.assertRaises(FinancialError): self.check(reference=change)

    def test_design_purpose_limits_and_source_changes_rejected(self):
        for name in ('purpose','limits','source'):
            with self.subTest(name=name),self.assertRaises(FinancialError):
                design=deepcopy(self.base_permit['execution_design']); sources=dict(self.sources)
                if name=='source': sources[next(iter(sources))]='f'*64
                else: design[name]='changed'
                self.check(execution_design=design,sources=sources)

    def test_wallet_rule_accepts_monotonic_original_opening_only(self):
        limits={key:self.permit(1000)[key] for key in ('expected_opening_usage','expected_global_cap','incremental_cap_micro_usd','max_workers')}
        self.check(live_limits={**limits,'expected_opening_usage':2000})
        for key,value in (('expected_opening_usage',999),('expected_global_cap',2_000_000),
                          ('incremental_cap_micro_usd',799_999),('max_workers',3),('expected_opening_usage',900_000)):
            with self.subTest(key=key,value=value),self.assertRaises(FinancialError): self.check(live_limits={**limits,key:value})

    def test_same_issued_proof_cannot_reopen_financial_owner(self):
        self.open_live(); self.authority.close(); self.authority=None
        with self.assertRaisesRegex(FinancialError,'recovery requires'): self.open_live(recovery=True)
        self.assertEqual(self.cold.call_count,1)

    def test_recovery_with_new_full_issuance_preserves_original_permit(self):
        self.open_live(); original=self.last_permit; self.authority.close(); self.authority=None
        self.session=self.new_session()
        self.open_live(recovery=True)
        self.assertEqual(self.last_permit,original); self.assertEqual(self.cold.call_count,2)
        action,_=self.start(); self.assertEqual(self.terminal(action).state,'completed')

    def test_forked_or_mutated_process_capability_rejected(self):
        with patch.object(issued.os,'getpid',return_value=self.session._pid+1),self.assertRaises(FinancialError): self.check()
        self.assertTrue(self.session._revoked)
        self.session=self.new_session()
        self.session._record=b'{}'
        with self.assertRaisesRegex(FinancialError, 'Issued capability state changed'): self.check()
        self.assertTrue(self.session._revoked)

    def test_missing_complete_cold_originals_never_produce_grant(self):
        self.cold.side_effect=FinancialError('full scope/originals unavailable')
        with self.assertRaisesRegex(FinancialError,'full scope'): self.new_session()
        self.assertFalse((self.root/'grant-raw1'/issued.GRANT_NAME).exists())

    def test_issuance_lost_ack_cannot_return_capability(self):
        original=issued.checkpoint.CheckpointChain.retain
        def lost(owner,name,raw,**kwargs):
            result=original(owner,name,raw,**kwargs)
            if name==issued.GRANT_NAME: raise OSError('simulated lost issuance acknowledgement')
            return result
        with patch.object(issued.checkpoint.CheckpointChain,'retain',lost),self.assertRaises(OSError): self.new_session()
        self.assertTrue((self.root/'grant-raw1'/issued.GRANT_NAME).exists())
        self.assertEqual(len(self.grants),1)

    def test_no_grant_cannot_enter_live_even_with_claimed_qualification_dict(self):
        with self.assertRaisesRegex(FinancialError,'issued before'):
            self.open(mode='live',qualification_session=None,clock=self.clock)
        self.assertEqual(self.transport.calls,[])

    def test_expired_actual_lease_after_fresh_guard_never_enters_transport(self):
        self.open_live()
        original=issued.IssuedQualification.check
        calls=0
        def expires(*args,**kwargs):
            nonlocal calls
            result=original(*args,**kwargs); calls+=1
            # start() checks claim + submit + actual before-invoke in that order.
            if calls==3: self.clock.now=1000
            return result
        with patch.object(issued.IssuedQualification,'check',autospec=True,side_effect=expires):
            action,_=self.start(); result=self.terminal(action)
        self.assertEqual(result.state,'unknown'); self.assertEqual(self.transport.calls,[])


    def test_uncertain_grant_head_suffix_blocks_entry_and_does_not_heal(self):
        self.open_live()
        pending=Path(self.session._journal['head'])/'head-pending-unacknowledged'
        pending.write_bytes(b'uncertain')
        with self.assertRaises(FinancialError): self.start()
        pending.unlink()
        with self.assertRaises(FinancialError): self.start()
        self.assertEqual(self.transport.calls,[])


    def rpc(self):
        from gossip_harness.peer_financial_rpc_v5 import FinancialRPCV5
        self.keys={actor:format(index+1,'064x') for index,actor in enumerate(self.child.actors)}
        return FinancialRPCV5(self.authority,self.keys,expected_financial_config_sha256=self.authority.config_sha256,
            request_guard=lambda action:self.payloads.read_owned(action.actor,action.worker_payload_ref.payload_sha256),
            request_guard_sha256='a'*64)

    def envelope(self,operation,request_id,payload):
        from gossip_harness.peer_financial_rpc_v2 import _signed,PROTOCOL,SCHEMA_VERSION
        return _signed({'protocol':PROTOCOL,'schema_version':SCHEMA_VERSION,
            'contract_sha256':self.plan.sha256,'actor':self.actor,'request_id':request_id,'nonce':'a'*32,
            'operation':operation,'payload':payload},self.keys[self.actor])

    def test_actual_signed_rpc_four_worker_path_uses_one_issued_audit(self):
        from tests.financial_rpc_diagnostics import FinancialRPCDiagnostics
        from gossip_harness.peer_financial_rpc_v5 import FinancialRPCV5
        diagnostics = FinancialRPCDiagnostics(self)
        with diagnostics.instrument(financial.CumulativeAuthorityV5, FinancialRPCV5, LocalOpener):
            self.open_live(); rpc=self.rpc(); self.transport.release.clear()
            actions=[]
            for index in range(4):
                action=self.action(index)
                claimed=rpc.handle(self.envelope('claim','rpc-claim-'+str(index),{
                    'context':to_dict(self.context),'work':to_dict(self.works[index]),'ttl':180}))['body']['receipt']
                self.assertEqual(claimed['status'],'ok')
                submitted=rpc.handle(self.envelope('submit',action.request_id,{
                    'action':to_dict(action),'lease':claimed['body']}))['body']['receipt']
                self.assertEqual(submitted['status'],'ok'); actions.append(action)
            import time
            until=time.monotonic()+10
            while len(self.transport.calls)<4 and time.monotonic()<until: time.sleep(.005)
            self.transport.release.set()
            self.assertEqual(len(self.transport.calls),4)
            self.assertEqual([self.terminal(action).state for action in actions],['completed']*4)
            self.assertEqual(self.cold.call_count,1); self.assertFalse(self.authority.failed_closed)

    def test_signed_rpc_revocation_blocks_new_claim_and_renew_but_preserves_exact_replay(self):
        self.open_live(); rpc=self.rpc()
        request=self.envelope('claim','original-claim',{'context':to_dict(self.context),
            'work':to_dict(self.works[0]),'ttl':180})
        original=rpc.handle(request)['body']['receipt']
        self.assertEqual(original['status'],'ok')
        before=self.rows('tasks'); intents=self.rows('financial_rpc_requests_v2')
        self.session.revoke('stop new claims and renewals')
        self.assertEqual(rpc.handle(request)['body']['receipt'],original)
        claim=rpc.handle(self.envelope('claim','new-claim',{'context':to_dict(self.context),
            'work':to_dict(self.works[1]),'ttl':180}))['body']['receipt']
        renewal=rpc.handle(self.envelope('renew','new-renew',{'lease':original['body'],'ttl':180}))['body']['receipt']
        self.assertEqual(claim,{'status':'denied','reason':'qualification_unavailable'})
        self.assertEqual(renewal,claim)
        self.assertEqual(self.rows('tasks'),before)
        self.assertEqual(self.rows('financial_rpc_requests_v2'),intents)
        self.assertEqual(self.transport.calls,[])

    def test_actual_ledger_replacement_after_grant_guard_blocks_provider_without_writing_copy(self):
        import sqlite3
        self.open_live()
        original_check=issued.IssuedQualification.check
        count=0; moved=self.root/'retained-original-wallet.sqlite'; replacement=self.root/'replacement.sqlite'
        def replacing(*args,**kwargs):
            nonlocal count
            result=original_check(*args,**kwargs); count+=1
            if count==3:
                # SQLite backup is a coherent equivalent database with a new
                # inode. No connection remains active at this guard boundary.
                with sqlite3.connect(self.path) as source,sqlite3.connect(replacement) as target: source.backup(target)
                self.path.rename(moved); replacement.rename(self.path)
                self.copy_before=self.path.read_bytes()
            return result
        with patch.object(issued.IssuedQualification,'check',autospec=True,side_effect=replacing):
            action,_=self.start()
            import time
            until=time.monotonic()+10
            while (self.actor,action.request_id) not in self.authority.persistence_failed and time.monotonic()<until: time.sleep(.005)
        self.assertIn((self.actor,action.request_id),self.authority.persistence_failed)
        self.assertEqual(self.transport.calls,[])
        self.assertTrue(self.authority.failed_closed)
        self.assertEqual(self.path.read_bytes(),self.copy_before)
        with sqlite3.connect(moved) as original:
            self.assertEqual(original.execute("SELECT spent,state FROM reservations WHERE id != 'historical-payment'").fetchall(),[(None,'reserved')])
        # Restore the actual original only for retained fixture teardown; this
        # owner must keep its identity-loss fence and cannot heal itself.
        self.path.rename(replacement); moved.rename(self.path)
        with self.assertRaises(FinancialError): self.authority.claim(self.actor,self.context,self.works[1],ttl=180)

    def test_signed_rpc_original_wallet_replacement_never_creates_a_new_lease(self):
        import sqlite3
        self.open_live(); rpc=self.rpc()
        copied=self.root/'copied-wallet.sqlite'; moved=self.root/'original-wallet.sqlite'
        with sqlite3.connect(self.path) as source,sqlite3.connect(copied) as target: source.backup(target)
        self.path.rename(moved); copied.rename(self.path)
        before=self.path.read_bytes()
        with self.assertRaises(FinancialError):
            rpc.handle(self.envelope('claim','wrong-inode',{'context':to_dict(self.context),
                'work':to_dict(self.works[0]),'ttl':180}))
        self.assertEqual(self.path.read_bytes(),before); self.assertEqual(self.transport.calls,[])
        self.path.rename(copied); moved.rename(self.path)
