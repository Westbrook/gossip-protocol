"""Complete comparison capacity declarations and an anchored reservation ledger.

These are prospective accounting records, not measured footprints, qualified
execution profiles, actual executor leases, or observation capabilities. The
successor controller must authenticate corpus/setup/source inputs and bind the
real shared executors before these records can support physical admission.
No existing model/evaluator pool is silently claimed by this module.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from pathlib import Path
import time
from typing import Any

from . import candidate_checkpoint_chain_v1 as chain
from .candidate_checkpoint_head_v1 import ExternalHead
from . import cumulative_generated_probe_matrix_v1 as matrices
from . import cumulative_generated_probe_values_v2 as values
from . import cumulative_study_controller_v2 as study
from . import project_acceptance_registry_v1 as registry

PROTOCOL = 'cumulative-generated-probe-capacity-v1'
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
CONFIG_SLOT = 'generated-probe.capacity-contract'
PHASES = ('before_review', 'contextual', 'merged')
require = study.require
MAX_INT = 2**63-1
MAX_REQUEST_BYTES = 8 * 1024 * 1024


def positive(value: Any) -> bool:
    return type(value) is int and 0 < value <= MAX_INT


@dataclass(frozen=True, slots=True)
class Budget:
    retained_raw_bytes: int
    journal_delta_bytes: int
    workspace_bytes: int
    execution_cells: int
    max_requests: int
    executor_slots: int = 4

    def __post_init__(self) -> None:
        require(all(positive(x) for x in asdict(self).values()), 'positive_exact_budget_integers_required')
        require(self.executor_slots == 4 and self.max_requests <= 108,
                'existing_four_slots_and_six_by_three_by_three_two_phase_ceiling_required')


@dataclass(frozen=True, slots=True)
class Charge:
    cell_sha256: str
    retained_raw_bytes: int
    journal_delta_bytes: int
    workspace_bytes: int
    active_ns: int
    cleanup_ns: int

    def __post_init__(self) -> None:
        registry.sha256(self.cell_sha256)
        require(all(positive(v) for k, v in asdict(self).items() if k != 'cell_sha256'),
                'complete_positive_cell_envelope_required')
        require(self.active_ns + self.cleanup_ns <= MAX_INT, 'cell_window_overflow')


@dataclass(frozen=True, slots=True)
class Window:
    clock_domain: str
    started_ns: int
    deadline_ns: int
    review_ns: int
    selection_ns: int

    def __post_init__(self) -> None:
        registry.identifier(self.clock_domain)
        require(positive(self.started_ns) and positive(self.deadline_ns)
                and self.started_ns < self.deadline_ns, 'absolute_capacity_window_required')
        require(type(self.review_ns) is int and type(self.selection_ns) is int
                and 0 <= self.review_ns <= MAX_INT and 0 <= self.selection_ns <= MAX_INT,
                'explicit_nonnegative_phase_overheads_required')


def declaration(matrix: matrices.Matrix, charges: tuple[Charge, ...], window: Window, *, phase: str) -> dict[str, Any]:
    """Freeze one complete phase; both phases are required for the generation."""
    require(type(matrix) is matrices.Matrix and type(window) is Window
            and type(charges) is tuple and all(type(c) is Charge for c in charges),
            'exact_matrix_charges_and_window_required')
    require(phase in ('before-review', 'after-review'), 'explicit_capacity_phase_required')
    cells = matrix.base_cells if phase == 'before-review' else (*matrix.contextual_cells, *matrix.merged_cells)
    ids = tuple(c.sha256 for c in cells)
    require(len(ids) == len(set(ids)) and tuple(c.cell_sha256 for c in charges) == ids,
            'complete_ordered_unique_charge_roster_required')
    record = matrix.record()
    require(set(record['cells']) == set(PHASES), 'complete_three_phase_census_required')
    return {'protocol': PROTOCOL, 'matrix': record, 'matrix_sha256': study.digest(record),
        'generation_id': json.loads(matrix.build_phase_raw)['build_phase_sha256'], 'phase': phase,
        'charges': [asdict(c) for c in charges], 'window': asdict(window),
        'qualification_status': 'caller_declared_envelopes_not_qualified_measurements',
        'dispatch_authority': False, 'acceptance_authority': False}


def _request(raw: bytes) -> dict[str, Any]:
    require(type(raw) is bytes and len(raw) <= MAX_REQUEST_BYTES, 'bounded_immutable_request_snapshot_required')
    body = json.loads(raw, object_pairs_hook=values._pairs, parse_constant=values._constant)
    values._bounded(body, max_nodes=1_000_000, max_depth=64)
    require(values.canonical(body) == raw and type(body) is dict and set(body) == {
        'protocol','matrix','matrix_sha256','generation_id','phase','charges','window','qualification_status',
        'dispatch_authority','acceptance_authority'}, 'closed_canonical_capacity_request_required')
    require(body['protocol'] == PROTOCOL and body['dispatch_authority'] is False
            and body['acceptance_authority'] is False
            and body['qualification_status'] == 'caller_declared_envelopes_not_qualified_measurements',
            'capacity_declaration_is_not_execution_authority')
    registry.sha256(body['generation_id'])
    matrix = body['matrix']
    require(type(matrix) is dict and study.digest(matrix) == body['matrix_sha256'], 'capacity_matrix_changed')
    require(set(matrix['cells']) == set(PHASES), 'complete_three_phase_census_required')
    require(body['phase'] in ('before-review','after-review'), 'explicit_capacity_phase_required')
    phases = PHASES[:1] if body['phase'] == 'before-review' else PHASES[1:]
    cells = [c for phase in phases for c in matrix['cells'][phase]]
    require(type(body['charges']) is list, 'ordered_charge_list_required')
    charges = [Charge(**c) for c in body['charges']]
    ids = [c['cell_sha256'] for c in cells]
    require(len(ids) == len(set(ids)) and ids == [c.cell_sha256 for c in charges]
            and all(study.digest({k:v for k,v in c.items() if k != 'cell_sha256'}) == c['cell_sha256'] for c in cells),
            'complete_ordered_unique_charge_roster_required')
    require(body['generation_id'] == matrix['build_phase']['build_phase_sha256'], 'capacity_generation_changed')
    window=Window(**body['window'])
    require((body['phase']=='before-review' and window.selection_ns==0)
            or (body['phase']=='after-review' and window.review_ns==0), 'phase_overhead_scope_differs')
    return body


def assess(raw: bytes, budget: Budget, *, used: dict[str, int], busy_until: tuple[int, ...],
           now_ns: int) -> dict[str, Any]:
    """Deterministic bounded list schedule; ceilings are not observed runtimes.

    Cleanup keeps a slot occupied. Review and selection are barriers, so future
    phases cannot borrow idle time before their dependencies exist. Reservations
    never refund retained artifacts or conditional merged checks.
    """
    require(type(budget) is Budget and type(busy_until) is tuple and len(busy_until) == budget.executor_slots
            and all(type(x) is int and 0 <= x <= MAX_INT for x in busy_until) and positive(now_ns),
            'complete_shared_slot_state_required')
    names = ('retained_raw_bytes','journal_delta_bytes','workspace_bytes','execution_cells')
    require(type(used) is dict and set(used) == set(names)
            and all(type(v) is int and 0 <= v <= getattr(budget,k) for k,v in used.items()),
            'complete_nonnegative_cumulative_usage_required')
    body = _request(raw);window=Window(**body['window']);charges=[Charge(**c) for c in body['charges']]
    totals = {k:sum(getattr(c,k) for c in charges) for k in names[:-1]}
    totals['execution_cells']=len(charges)
    reasons = [k+'_capacity' for k in names if used[k]+totals[k] > getattr(budget,k)]
    if now_ns >= window.deadline_ns: reasons.append('absolute_deadline_expired')
    if body['phase']=='after-review' and body['matrix']['post_review']['missing_anchor_packages']: reasons.append('peer_anchor_unavailable')
    slots = list(busy_until);ready=max(now_ns,window.started_ns);schedule=[];cursor=0
    phases=PHASES[:1] if body['phase']=='before-review' else PHASES[1:]
    for phase in phases:
        if phase == 'merged': ready += window.selection_ns
        phase_ends=[]
        for cell in body['matrix']['cells'][phase]:
            charge=charges[cursor];cursor+=1
            slot=min(range(budget.executor_slots),key=lambda i:(max(slots[i],ready),i))
            start=max(slots[slot],ready);active_end=start+charge.active_ns;finish=active_end+charge.cleanup_ns
            slots[slot]=finish;phase_ends.append(finish)
            schedule.append({'phase':phase,'cell_sha256':cell['cell_sha256'],'slot':slot,
                'started_ns':start,'active_deadline_ns':active_end,'released_ns':finish})
        if phase_ends: ready=max(phase_ends)
    require(cursor==len(charges),'unscheduled_charge')
    review_barrier=None
    if body['phase']=='before-review' and window.review_ns:
        start=max(ready,*slots);ready=start+window.review_ns;slots=[ready]*budget.executor_slots
        review_barrier={'started_ns':start,'released_ns':ready,'all_slots':True}
    if ready > window.deadline_ns or any(row['released_ns'] > window.deadline_ns for row in schedule): reasons.append('complete_schedule_exceeds_deadline')
    if ready > MAX_INT: reasons.append('schedule_overflow')
    return {'protocol':PROTOCOL,'generation_id':body['generation_id'],'matrix_sha256':body['matrix_sha256'],
        'status':'declined' if reasons else 'reserved_declaration','reasons':reasons,'charges':totals,
        'schedule':schedule,'review_barrier':review_barrier,'proposed_busy_until':slots,'assessed_at_ns':now_ns,
        'raw_bounds_are_measured':False,'executor_leases_issued':False,'dispatch_authority':False,
        'acceptance_authority':False}


class ReservationLedger:
    """One prospective accounting namespace in the controller's anchored journal.

    Cold replay recomputes every allocation, including declines. This is not an
    executor semaphore or a registration authenticator. Real model/evaluator
    pools, source/setup authorities and physical envelope qualification still
    have to adopt this book before an observation capability can be issued.
    """
    def __init__(self, records: study.Records, budget: Budget, *, clock_domain: str,
                 expected: chain.PrefixCommitment, create: bool = False) -> None:
        require(type(records) is study.Records and type(records.chain) is chain.CheckpointChain
                and type(records.chain.authority) is ExternalHead and type(budget) is Budget
                and type(expected) is chain.PrefixCommitment and type(create) is bool,
                'actual_controller_journal_and_budget_required')
        registry.identifier(clock_domain)
        self.records=records;self.budget=budget;self.clock_domain=clock_domain
        config={'protocol':PROTOCOL,'budget':asdict(budget),'clock_domain':clock_domain,
            'source_sha256':LOADED_SOURCE_SHA256,'execution_authority':False}
        self._config_raw=values.canonical(config)
        records.chain.validate_boundary(expected=expected)
        original=records.read(CONFIG_SLOT)
        if create:
            require(original is None and records.read('generated-probe.ranking-contract') is None,
                    'capacity_contract_must_precede_ranked_work')
            records.put(CONFIG_SLOT,config)
        else:
            require(values.canonical(original)==self._config_raw,'original_capacity_contract_differs')
        self.snapshot()

    @staticmethod
    def _slot(index: int) -> str:
        return 'generated-probe.capacity-request.'+str(index)

    def snapshot(self) -> dict[str, Any]:
        require(hashlib.sha256(Path(__file__).read_bytes()).hexdigest()==LOADED_SOURCE_SHA256,
                'loaded_capacity_owner_changed')
        self.records.chain.validate_boundary()
        require(values.canonical(self.records.read(CONFIG_SLOT))==self._config_raw,'capacity_contract_changed')
        config=json.loads(self._config_raw)
        require(type(self.budget) is Budget and asdict(self.budget)==config['budget']
                and self.clock_domain==config['clock_domain'],'capacity_owner_configuration_changed')
        used={k:0 for k in ('retained_raw_bytes','journal_delta_bytes','workspace_bytes','execution_cells')}
        busy=(0,)*self.budget.executor_slots;entries: list[dict[str, Any]]=[];seen=set();gap=False
        position=self.records.chain.position(self.records.name(CONFIG_SLOT))
        if self.records.read('generated-probe.ranking-contract') is not None:
            require(position < self.records.chain.position(self.records.name('generated-probe.ranking-contract')),
                    'capacity_contract_must_precede_ranked_work')
        for index in range(self.budget.max_requests):
            row=self.records.read(self._slot(index))
            if row is None:gap=True;continue
            require(not gap and set(row)=={'request','decision'},'capacity_history_gap_or_schema_change')
            current=self.records.chain.position(self.records.name(self._slot(index)))
            require(current>position,'capacity_history_order_changed');position=current
            raw=values.canonical(row['request']);request=_request(raw)
            require(request['window']['clock_domain']==self.clock_domain,'capacity_clock_domain_changed')
            identity=request['generation_id']+'.'+request['phase']
            require(identity not in seen,'generation_phase_reserved_twice');seen.add(identity)
            self._prior_before(request,entries)
            decision=assess(raw,self.budget,used=used,busy_until=busy,now_ns=row['decision']['assessed_at_ns'])
            require(values.exact(decision,row['decision']),'original_capacity_decision_differs')
            if decision['status']=='reserved_declaration':
                used={k:used[k]+decision['charges'][k] for k in used};busy=tuple(decision['proposed_busy_until'])
            entries.append(row)
        require(self.records.read(self._slot(self.budget.max_requests)) is None,'capacity_history_exceeds_request_cap')
        self.records.chain.validate_boundary()
        return {'used':used,'busy_until':busy,'entries':entries,'phase_ids':sorted(seen),
            'executor_leases_issued':False,'dispatch_authority':False}

    @staticmethod
    def _prior_before(request: dict[str, Any], entries: list[dict[str, Any]]) -> None:
        if request['phase']=='before-review': return
        prior=[row for row in entries if row['request']['generation_id']==request['generation_id']
               and row['request']['phase']=='before-review']
        require(len(prior)==1 and prior[0]['decision']['status']=='reserved_declaration',
                'complete_prior_pre_review_reservation_required')
        before=prior[0]['request']
        require(values.exact(before['matrix']['cells']['before_review'],request['matrix']['cells']['before_review'])
                and values.exact(before['matrix']['build_phase'],request['matrix']['build_phase']),
                'pre_review_census_changed_after_rankings')
        require(before['window']['started_ns']==request['window']['started_ns']
                and before['window']['deadline_ns']==request['window']['deadline_ns'],
                'absolute_generation_window_changed')

    def reserve(self, raw: bytes, *, expected: chain.PrefixCommitment) -> dict[str, Any]:
        self.records.chain.validate_boundary(expected=expected)
        current=self.snapshot();request=_request(raw)
        require(request['window']['clock_domain']==self.clock_domain,'capacity_clock_domain_changed')
        identity=request['generation_id']+'.'+request['phase']
        require(identity not in current['phase_ids'],'generation_phase_already_attempted')
        self._prior_before(request,current['entries'])
        require(len(current['entries'])<self.budget.max_requests,'capacity_request_limit')
        decision=assess(raw,self.budget,used=current['used'],busy_until=current['busy_until'],now_ns=time.monotonic_ns())
        self.records.chain.validate_boundary(expected=expected)
        # Retain declines as well; no smaller opportunistic retry is permitted.
        slot=self._slot(len(current['entries']))
        self.records.put(slot,{'request':request,'decision':decision})
        self.records.chain.validate_boundary()
        return {'slot':slot,**decision}
