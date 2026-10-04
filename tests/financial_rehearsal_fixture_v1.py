"""Synthetic prospective plan only; never a physical qualification fixture."""
from dataclasses import asdict
from pathlib import Path

from gossip_harness.cumulative_study_controller_v1 import (
    MILESTONES, PACKAGES, SHARED_POLICY, TRANSPORT_CONTRACT, Release, StudyPlan, actors_for, digest,
)
from gossip_harness.project_acceptance_compiler_v1 import ARMS, BLOCKS, FAULTS, CohortDesign, Trajectory


def synthetic_plan():
    releases=tuple(Release(m,'Authored '+m,{}, {'check.py':'# unit fixture only'},
        ('python','/checks/check.py'),(m+'-case',),(m+'-obligation',)) for m in MILESTONES)
    runtime={'max_reserved_units':1000000,'image':'sha256:'+'a'*64,'public_timeout_seconds':180}
    limits={'horizon_seconds':30,'source_generations':2,'partition_seconds':.1,'executor_slots':4,'runtime':runtime}
    from gossip_harness import cumulative_study_controller_v1 as controller
    fault=getattr(controller,'fault_schedule',lambda block,seconds: {'block':block,'seconds':seconds})
    trajectories=tuple(Trajectory(a.lower().replace('-','')+'.'+b.replace('_',''),a,b,actors_for(a),
        'durable_central_scheduler' if a=='O16-G' else 'peer_local',digest({'seed':b}),
        digest(fault(b,.1)),FAULTS if b=='compound_recovery' else ()) for a in ARMS for b in BLOCKS)
    cohort=CohortDesign('reader-unit',trajectories,MILESTONES,digest(TRANSPORT_CONTRACT),digest(SHARED_POLICY),
        digest([asdict(x) for x in releases]),digest(limits),digest({'mini':{'fixture':True},'strong':{'fixture':True}}),'b'*64,'c'*64,'d'*64)
    source=Path(__file__).resolve().parents[1]
    from gossip_harness.peer_financial_terminal_v1 import sha
    pins={name:sha((source/name).read_bytes()) for name in getattr(controller,'SOURCE_CLOSURE',
          ('gossip_harness/cumulative_study_controller_v1.py',))}
    return StudyPlan(cohort,releases,{'base.py':'pass\n'},
        {p:('library/'+p+'/impl.py',) for p in PACKAGES},pins,**limits)


def synthetic_design_envelope(plan):
    profiles={'mini':{'fixture':True},'strong':{'fixture':True}}
    return {'protocol':'cumulative-execution-design-envelope-v1','execution_contract_sha256':plan.sha256,
        'terminal_roster':plan.roster.record(),'sources':plan.source_pins,'runtime':plan.runtime,'profiles':profiles,
        'children':[{'cohort':child.cohort,'trajectory':child.trajectory,'execution_design':{
            'terminal_roster':plan.roster.record(),'profiles':profiles,'max_workers':4,
            'action_limits':{'total':512,'by_kind':{'build':512,'repair':512,'review':512},
                'by_kind_generation':{kind:{'0':512,'1':512} for kind in ('build','repair','review')},
                'by_actor':dict.fromkeys(child.actors,12)}}} for child in plan.roster.children]}
