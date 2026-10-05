"""Real inert Git/checkpoint fixtures; synthetic mesh and financial-proof seams.

Actual runtime result/materialization verification runs. Mock finance returns
synthetic typed originals, so this is not full financial or physical qualification.
"""
from contextlib import ExitStack
from dataclasses import asdict
import hashlib
from pathlib import Path
import tempfile
from unittest.mock import Mock

from gossip_harness import candidate_checkpoint_chain_v1 as chain
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from gossip_harness import cumulative_generated_probe_ranked_originals_v1 as ranked
from gossip_harness import cumulative_study_controller_v2 as study
from gossip_harness import cumulative_study_runtime_v2 as runtime
from gossip_harness.gitstore import GitStore
from gossip_harness.ledger import Lease
from gossip_harness.peer_financial_authority_v2 import ledger_identity
from gossip_harness.peer_project_contract_v2 import (ActionRequest, Context, DispatchBinding, DispatchReply,
    EvidenceRef, WorkKey, identity, to_dict, worker_request_digest)
from gossip_harness.peer_role_loop_v2 import WorkDirective, directive_id, materialize
from tests.test_cumulative_study_repaired_runtime_v2 import CumulativeStudyRepairedRuntimeV2Tests


class Mesh:
    def __init__(self): self.values = {}
    def publish(self, kind, value, producer='seed'):
        raw = runtime.canonical_payload(value)
        ref = EvidenceRef(study.digest([kind, producer, raw.hex()]), producer, kind, hashlib.sha256(raw).hexdigest())
        self.values[ref] = raw
        return ref
    def arrived(self): return tuple(self.values)
    def resolve(self, ref): return self.values.get(ref)
    def want(self, ref): raise AssertionError('No subscription or missing local view allowed')


class OriginalFixture:
    def __init__(self, *, index=0, review_change=None, frontier_change=None, result_change=None,
                 stopped_builder=None, stopped_reviewer=None, legacy_review=False, reverse_roster=False,
                 late_contract=False, directive_change=None, at_build_boundary=None, builds_only=False):
        self.stack = ExitStack(); self.directive_change = directive_change
        helper = CumulativeStudyRepairedRuntimeV2Tests('test_known_failure_is_authenticated_and_retained_without_success_proof')
        helper.setUp(); self.stack.callback(helper.doCleanups)
        self.plan = helper.plan(); self.limits = ranked.ReviewLimits(16384, 4, 1024, 16)
        self.root = Path(self.stack.enter_context(tempfile.TemporaryDirectory(prefix='ranked-originals-'))).resolve()
        owner = object.__new__(runtime.GossipChildRuntime); self.owner = owner
        owner.plan, owner.index, owner.repository = self.plan, index, helper.repository
        owner.child, owner.trajectory = self.plan.roster.children[index], self.plan.cohort.trajectories[index]
        owner.key = 'runtime.' + owner.trajectory.id
        owner.ledger = self.root/'ledger.sqlite'; owner.ledger.write_bytes(b'inert identity fixture, not a database')
        owner.expected_ledger_identity = ledger_identity(owner.ledger)
        owner.protected = GitStore.create(self.root/'source.git', self.plan.initial_files)
        raw, delta = self.root/'raw', self.root/'delta'
        self.head = ExternalHead.create(self.root/'head', journal_roots=(raw,delta)); self.stack.callback(self.head.close)
        self.chain = chain.CheckpointChain.create(raw,delta,context={'synthetic_ranked_originals':True},authority=self.head)
        self.stack.callback(self.chain.close); owner.records = study.Records(self.chain)
        owner.records.put('ledger',owner.expected_ledger_identity)
        declared=ranked.contract(self.plan,self.limits)
        if not late_contract: owner.records.put(ranked.CONTRACT_SLOT,declared)
        owner.seed=Mesh(); owner.finance=Mock(); self.proofs={}
        owner.finance.verified_terminal.side_effect=lambda b:self.proofs[identity(b)]
        owner.finance.verified_known_failure.side_effect=lambda b:self.proofs[identity(b)]
        self.release=self.plan.releases[1]; self.stage='child.'+owner.trajectory.id+'.M2.g0'
        self.source_ref=owner.seed.publish('project-source',{'base_sha':owner.protected.head(),'files':self.plan.initial_files})
        self.context=Context(self.plan.sha256,owner.child.cohort,owner.child.trajectory,2,self.release.sha256)
        builders=sorted(owner.child.actors[:-4],key=lambda a:study.digest({'seed':owner.trajectory.block_seed_sha256,'actor':a}))
        self.builds=[]
        controller=object.__new__(study.StudyController);controller.plan=self.plan
        actual_directives=dict(controller._directives(index,2,0,self.source_ref,(),reviews=False))
        for actor in builders:
            role=actor.rsplit('.',1)[-1];package=study.package_for(role)
            directive=WorkDirective(self.context,WorkKey(package,'M2',role,0),'mini','build',self.source_ref,(),
                self.plan.package_paths[package],actual_directives[actor].instructions)
            changes={self.plan.package_paths[package][0]:'# inert '+role+'\n'}
            self.builds.append(self.result(actor,directive,changes,stopped=role==stopped_builder,mutate=result_change))
        self.retain('builds',list(reversed(self.builds)) if reverse_roster else self.builds)
        if at_build_boundary: at_build_boundary(self)
        if builds_only:
            self.expected=self.chain.commitment
            return
        self.reviews=[]
        reviewers=sorted(owner.child.actors[-4:],key=lambda a:study.digest({'seed':owner.trajectory.block_seed_sha256,'actor':a}))
        for actor in reviewers:
            role=actor.rsplit('.',1)[-1];package=study.package_for(role)
            scoped=[{'actor':x['actor'],'directive_id':x['directive_id'],'original_ref':x['original_ref'],
                     'snapshot':x['snapshot'],'proposal':x['result_payload']} for x in self.builds if study.package_for(x['actor'])==package]
            frontier={'stage_id':self.stage,'package':package,'builds':scoped,'shared_source_ref':to_dict(self.source_ref)}
            if frontier_change: frontier_change(frontier)
            ref=owner.seed.publish('cumulative-frontier',frontier)
            directive=WorkDirective(self.context,WorkKey(package,'M2',role,0),'strong','review',self.source_ref,(ref,),
                ('decision.json',),'legacy one-choice request' if legacy_review else ranked.instructions(self.release,self.stage,package,self.limits))
            ranks=[x['actor'] for x in self.builds if study.package_for(x['actor'])==package and x['snapshot']['reply'] is not None]
            decision={'protocol':ranked.DECISION_PROTOCOL,'stage_id':self.stage,'package':package,
                      'ranked_actors':ranks,'reasons':'Synthetic fixed ranking','probes':[]}
            if review_change: review_change(decision)
            self.reviews.append(self.result(actor,directive,{'decision.json':runtime.canonical_payload(decision).decode()},
                                            stopped=role==stopped_reviewer))
        self.retain('reviews',self.reviews)
        if late_contract: owner.records.put(ranked.CONTRACT_SLOT,declared)
        self.expected=self.chain.commitment

    def result(self,actor,directive,changes,*,stopped=False,mutate=None):
        if self.directive_change: directive=self.directive_change(directive)
        owner=self.owner;key=directive_id(directive)
        owner.records.put(owner.key+'.directive.'+key,{'actor':actor,'directive':directive.to_dict()})
        request,view=materialize(directive,owner.seed,self.plan.cohort.shared_policy_sha256)
        action=ActionRequest(self.context,'action-'+actor,'request-'+actor,actor,directive.kind,directive.work,directive.profile_id,
            EvidenceRef(study.digest(['request',actor]),actor,'worker-request','d'*64),identity(view))
        binding=DispatchBinding(action,Lease(request.task_id,actor,1,4_000_000_000.),worker_request_digest(request),
            'e'*64,'f'*64,'call-'+actor,'reservation-'+actor,100)
        payload={'kind':'proposal','payload':{'changes':changes,'summary':'synthetic proposal'}}
        reply=DispatchReply(action.request_id,identity(action),'completed','durable_outcome',binding,
            hashlib.sha256(runtime.canonical_payload(payload)).hexdigest(),7)
        self.proofs[identity(binding)]={'reply':reply,'worker_request':request,'result_payload':payload}
        scope=study.digest({'source':to_dict(directive.source_ref),'evidence':[to_dict(x) for x in directive.evidence_refs]})
        stage=self.stage+('.review.'+scope[:12] if directive.kind=='review' else '.build')
        value={'actor':actor,'stage_id':stage,
               'directive_id':key,'snapshot':{'state':'stopped' if stopped else 'published',
                   'reply':None if stopped else to_dict(reply),'action':to_dict(action),'view':to_dict(view)},
               'worker_request':study.plain(asdict(request)),'result_payload':payload}
        if mutate: mutate(value)
        value['original_ref']=to_dict(owner.seed.publish('cumulative-result',value,actor))
        return value

    def retain(self,kind,values):
        controller=object.__new__(study.StudyController);controller.records=self.owner.records
        study.StudyController._retain_results(controller,self.stage+'.'+kind,tuple(values))

    def read(self,**changes):
        args={'expected':self.expected,'milestone':'M2','generation':0,'limits':self.limits};args.update(changes)
        return ranked.reconstruct(self.owner,**args)

    def close(self): self.stack.close()
