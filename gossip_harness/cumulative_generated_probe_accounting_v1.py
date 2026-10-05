"""Bind declared comparison costs to original enrolled study matrices.

The generic capacity ledger deliberately accepts declarations. This adapter
reconstructs every candidate and applicable probe before allowing its one ledger
write. Cold inspection repeats the original joins and chronology checks. The
selected clock policy also binds the original child horizon. Neither path issues
executor leases or proves physical costs, execution or independent acceptance.
"""
from __future__ import annotations

import hashlib
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any

from . import candidate_checkpoint_chain_v1 as chain
from .candidate_checkpoint_head_v1 import ExternalHead
from . import candidate_observation_admission_v1 as admission
from . import cumulative_generated_probe_capacity_v1 as capacity
from . import cumulative_generated_probe_context_v1 as contexts
from . import cumulative_generated_probe_plan_v1 as plans
from . import cumulative_generated_probe_state_v1 as states
from . import cumulative_generated_probe_corpus_v1 as corpus
from . import cumulative_generated_probe_matrix_v1 as matrices
from . import cumulative_generated_probe_ranked_originals_v1 as ranked
from . import cumulative_generated_probe_values_v2 as values
from . import cumulative_study_controller_v2 as study
from . import cumulative_study_runtime_v2 as runtime
from . import cumulative_child_deadline_v1 as child_deadlines
from .gitstore import GitStore

PROTOCOL = 'cumulative-generated-probe-original-accounting-v1-cell-enrollment-v4'
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
require = study.require


def sources() -> dict[str, str]:
    pins = {**ranked.sources(), **states.evaluator_sources(),
        'gossip_harness/cumulative_generated_probe_accounting_v1.py': LOADED_SOURCE_SHA256,
        'gossip_harness/cumulative_generated_probe_capacity_v1.py': capacity.LOADED_SOURCE_SHA256,
        'gossip_harness/cumulative_generated_probe_corpus_v1.py': corpus.LOADED_SOURCE_SHA256}
    root = Path(__file__).resolve().parents[1]
    require(all(hashlib.sha256((root / name).read_bytes()).hexdigest() == pin
                for name, pin in pins.items()), 'original_accounting_source_changed')
    admission.verify_loaded_sources(pins)
    return pins


def _matrix(owner: runtime.GossipChildRuntime, book: capacity.ReservationLedger, *,
            expected: chain.PrefixCommitment, milestone: str, generation: int,
            phase: str, quotas: corpus.Quotas, limits: ranked.ReviewLimits,
            historical: bool) -> tuple[matrices.Matrix, dict[str, Any]]:
    require(type(owner) is runtime.GossipChildRuntime and type(book) is capacity.ReservationLedger
            and book.records is owner.records, 'same_original_controller_accounting_journal_required')
    owner.records.chain.validate_boundary(expected=expected)
    require(all(owner.plan.source_pins.get(name) == pin for name, pin in sources().items()),
            'original_accounting_not_prospectively_pinned')
    owner.plan.verify_sources(owner.repository)
    require(phase in ('before-review', 'after-review'), 'explicit_original_accounting_phase_required')
    require(type(quotas) is corpus.Quotas and type(limits) is ranked.ReviewLimits,
            'exact_original_accounting_policies_required')
    stage = 'child.' + owner.trajectory.id + '.' + milestone + '.g' + str(generation)
    if phase == 'before-review':
        compiled, proof = ranked.reconstruct_frozen_builds(owner, expected=expected,
            milestone=milestone, generation=generation, limits=limits, historical=historical)
        number = study.MILESTONES.index(milestone)
        matrix = matrices.compile_matrix(compiled, current=owner.plan.releases[number],
            inherited=owner.plan.releases[number-1], active_probes=())
        original = proof['before_review_freeze']
    else:
        _, matrix, _ = corpus.read(owner, expected=expected, milestone=milestone,
            generation=generation, quotas=quotas, limits=limits)
        slot = stage + '.corpus'
        original = {'slot': slot, 'record_sha256': study.digest(owner.records.read(slot)),
            'position': owner.records.chain.position(owner.records.name(slot))}
        if not historical:
            freeze = owner.records.read(stage + '.before-review-freeze')
            assert freeze is not None
            require(owner.protected.head() == freeze['build_phase']['base']['commit_oid'],
                    'accepted_source_moved_before_original_accounting')
    owner.records.chain.validate_boundary(expected=expected)
    return matrix, {'stage': stage, **original}


def _compare(raw: bytes, matrix: matrices.Matrix, phase: str) -> dict[str, Any]:
    request = capacity._request(raw)
    require(request['phase'] == phase and values.exact(request['matrix'], matrix.record()),
            'capacity_matrix_differs_from_enrolled_originals')
    return request


def _child_window(owner: runtime.GossipChildRuntime, request: dict[str, Any], *, stage: str, active: bool) -> dict[str, Any] | None:
    if not child_deadlines.selected(owner.plan):
        return None
    original = child_deadlines.read(owner.records, owner.plan, owner.index, active=active)
    require(owner.records.chain.position(owner.records.name(original.origin_slot))
            < owner.records.chain.position(owner.records.name(stage + '.before-review-freeze')),
            'child_clock_must_precede_probe_build_freeze')
    window = request['window']
    require((window['clock_domain'], window['started_ns'], window['deadline_ns']) ==
            (original.clock_domain, original.started_ns, original.deadline_ns),
            'capacity_window_differs_from_original_child_start')
    if active:
        derived = owner.probe_capacity_window(review_ns=window['review_ns'], selection_ns=window['selection_ns'])
        require(values.exact(window, asdict(derived)), 'active_child_capacity_window_differs')
    return {'slot': original.origin_slot, 'sha256': original.origin_sha256,
            'clock_domain': original.clock_domain, 'started_ns': original.started_ns,
            'deadline_ns': original.deadline_ns}


def _require_before(owner: runtime.GossipChildRuntime, book: capacity.ReservationLedger,
                    request: dict[str, Any], *, expected: chain.PrefixCommitment,
                    milestone: str, generation: int, quotas: corpus.Quotas,
                    limits: ranked.ReviewLimits) -> None:
    entries = book.snapshot()['entries']
    prior = [(book._slot(i), row) for i, row in enumerate(entries)
             if row['request']['generation_id'] == request['generation_id']
             and row['request']['phase'] == 'before-review']
    require(len(prior) == 1 and prior[0][1]['decision']['status'] == 'reserved_declaration',
            'complete_prior_pre_review_reservation_required')
    # Generic ledger rows cannot prove the phase chronology themselves. This
    # also runs during cold post-review inspection and never issues a new write.
    inspect(owner, book, prior[0][0], expected=expected, milestone=milestone,
        generation=generation, phase='before-review', quotas=quotas, limits=limits)


def reserve(owner: runtime.GossipChildRuntime, book: capacity.ReservationLedger, raw: bytes, *,
            expected: chain.PrefixCommitment, milestone: str, generation: int, phase: str,
            quotas: corpus.Quotas, limits: ranked.ReviewLimits) -> dict[str, Any]:
    """Charge exactly the enrolled phase, immediately after its original freeze.

    The ledger retains a decline as well as a successful declaration. There is
    no new callback, dispatch, retry, or second journal write to recover.
    """
    matrix, original = _matrix(owner, book, expected=expected, milestone=milestone,
        generation=generation, phase=phase, quotas=quotas, limits=limits, historical=False)
    request = _compare(raw, matrix, phase)
    original_clock = _child_window(owner, request, stage=original['stage'], active=True)
    if phase == 'after-review':
        _require_before(owner, book, request, expected=expected, milestone=milestone,
            generation=generation, quotas=quotas, limits=limits)
    require(original['position'] == expected.sequence,
            'original_accounting_must_immediately_follow_enrollment')
    owner.records.chain.validate_boundary(expected=expected)
    decision = book.reserve(raw, expected=expected)
    return {'protocol': PROTOCOL, 'original': original, 'decision': decision,
        'original_matrix_bound': True, 'executor_leases_issued': False,
        'whole_child_deadline_bound': original_clock is not None, 'original_child_clock': original_clock,
        'dispatch_authority': False, 'acceptance_authority': False}


def inspect(owner: runtime.GossipChildRuntime, book: capacity.ReservationLedger, slot: str, *,
            expected: chain.PrefixCommitment, milestone: str, generation: int, phase: str,
            quotas: corpus.Quotas, limits: ranked.ReviewLimits) -> dict[str, Any]:
    """Reconstruct a retained allocation/decline; never consume its label as proof."""
    matrix, original = _matrix(owner, book, expected=expected, milestone=milestone,
        generation=generation, phase=phase, quotas=quotas, limits=limits, historical=True)
    snapshot = book.snapshot()
    rows = {book._slot(i): row for i, row in enumerate(snapshot['entries'])}
    require(type(slot) is str and slot in rows, 'original_capacity_slot_missing')
    row = rows[slot]
    request = _compare(values.canonical(row['request']), matrix, phase)
    original_clock = _child_window(owner, request, stage=original['stage'], active=False)
    if phase == 'after-review':
        _require_before(owner, book, request, expected=expected, milestone=milestone,
            generation=generation, quotas=quotas, limits=limits)
    position = owner.records.chain.position(owner.records.name(slot))
    require(position == original['position'] + 1,
            'original_capacity_record_not_at_enrollment_boundary')
    owner.records.chain.validate_boundary(expected=expected)
    return {'protocol': PROTOCOL, 'original': original, 'slot': slot,
        'record_sha256': study.digest(row), 'decision': row['decision'],
        'original_matrix_bound': True, 'executor_leases_issued': False,
        'whole_child_deadline_bound': original_clock is not None, 'original_child_clock': original_clock,
        'dispatch_authority': False, 'acceptance_authority': False}


def inspect_probe_cell(owner: runtime.GossipChildRuntime, book: capacity.ReservationLedger,
                       slot: str, cell_sha256: str, plan: plans.ProbePlan, *,
                       candidate_store: GitStore, expected: chain.PrefixCommitment, milestone: str, generation: int,
                       quotas: corpus.Quotas, limits: ranked.ReviewLimits,
                       selected: tuple[str, ...] | None = None) -> dict[str, Any]:
    """Join one planned probe to its complete original reservation and Git bytes.

    This read-only join does not consume a cell, qualify an envelope or authorize
    dispatch. A merged vector proves source construction only, not selection.
    The future controller must retain a unique intent and authenticate qualified
    resource limits before using this evidence for admission.
    """
    require(type(plan) is plans.ProbePlan and type(candidate_store) is GitStore,
            'exact_reserved_probe_plan_and_store_required')
    require(candidate_store.path != owner.protected.path, 'separate_probe_candidate_view_required')
    original = inspect(owner, book, slot, expected=expected, milestone=milestone,
        generation=generation, phase='after-review', quotas=quotas, limits=limits)
    require(original['decision']['status'] == 'reserved_declaration', 'successful_probe_reservation_required')
    require(original['whole_child_deadline_bound'], 'original_child_bound_probe_reservation_required')
    plan_raw = values.canonical(plan.record())
    row = owner.records.read(slot)
    assert row is not None
    request = row['request']
    cells = [c for phase in ('contextual', 'merged') for c in request['matrix']['cells'][phase]
             if c['cell_sha256'] == cell_sha256]
    require(type(cell_sha256) is str and len(cells) == 1, 'exact_reserved_probe_cell_required')
    cell = cells[0]
    require(cell['kind'] == 'generated-history', 'generated_probe_cell_required')
    target = plan.target
    subject = target.subject
    require((subject.cohort_id, subject.trajectory_id, subject.execution_contract_sha256,
             subject.milestone, target.generation) ==
            (owner.child.cohort, owner.trajectory.id, owner.plan.sha256, milestone, generation),
            'reserved_probe_subject_or_generation_differs')
    release = owner.plan.releases[study.MILESTONES.index(milestone)]
    require(plan.release_raw == study.canonical_payload(study.plain(asdict(release))),
            'reserved_probe_public_release_differs')
    record = plan.record()
    require(values.digest(record['probe']) == cell['definition_sha256']
            and cell['ordered_check_ids'] == ['probe-' + record['probe']['probe_id'] + '-value',
                                               'probe-' + record['probe']['probe_id'] + '-mechanics'],
            'reserved_probe_definition_or_cases_differ')
    compiled, _ = ranked.reconstruct_frozen(owner, expected=expected,
        milestone=milestone, generation=generation, limits=limits, historical=True)
    if cell['phase'] == 'contextual':
        require(selected is None and target.stage == 'candidate-context'
                and target.candidate_id == cell['actor'], 'reserved_probe_candidate_context_differs')
        context = contexts.compose(compiled, 'contextual', actor=cell['actor'])
        require(values.exact(cell['context'], context.record()), 'reserved_probe_context_original_differs')
    else:
        require(target.stage == 'merged' and target.candidate_id == 'merged',
                'reserved_probe_merged_stage_differs')
        context = contexts.compose(compiled, 'merged', selected=selected)
    require(target.context_id == context.sha256, 'reserved_probe_context_identity_differs')
    materialized = contexts.verify_materialized(owner.protected, compiled, context, target.commit_oid)
    require((target.tree_oid, subject.source_sha256) ==
            (materialized['tree_oid'], materialized['source_sha256']), 'reserved_probe_source_identity_differs')
    plans.verify_current_source(candidate_store, plan)
    charge = next(c for c in request['charges'] if c['cell_sha256'] == cell_sha256)
    schedule = next(c for c in row['decision']['schedule'] if c['cell_sha256'] == cell_sha256)
    require(values.canonical(plan.record()) == plan_raw, 'reserved_probe_plan_changed_during_join')
    owner.records.chain.validate_boundary(expected=expected)
    return {'protocol': PROTOCOL, 'reservation': original, 'cell': cell, 'declared_charge': charge,
        'declared_schedule': schedule, 'probe_plan_sha256': values.digest(record),
        'context': context.record(), 'materialized_source': materialized,
        'candidate_store': str(candidate_store.path),
        'original_matrix_bound': True, 'original_source_bound': True,
        'unique_execution_intent_retained': False, 'qualified_resource_envelope': False,
        'selection_authority': False, 'dispatch_authority': False, 'acceptance_authority': False}


def _enrollment_slot(reservation_slot: str, cell_sha256: str) -> str:
    return 'generated-probe.cell-enrollment.' + values.digest([reservation_slot, cell_sha256])


def _canonical_root(root: Path) -> Path:
    require(isinstance(root, Path) and root.is_absolute() and root.resolve() == root,
            'canonical_probe_execution_root_required')
    return root


def _overlap(left: Path, right: Path) -> bool:
    return left.is_relative_to(right) or right.is_relative_to(left)


def _enrollments(owner: runtime.GossipChildRuntime, book: capacity.ReservationLedger) -> list[dict[str, Any]]:
    """Enumerate exactly the bounded original cell roster, not an untrusted index."""
    found = []
    for index, allocation in enumerate(book.snapshot()['entries']):
        slot = book._slot(index)
        for phase in ('contextual', 'merged'):
            for cell in allocation['request']['matrix']['cells'][phase]:
                if allocation['request']['phase'] != 'after-review' or cell['kind'] != 'generated-history':
                    continue
                key = _enrollment_slot(slot, cell['cell_sha256'])
                row = owner.records.read(key)
                if row is None:
                    continue
                require(row.get('protocol') == PROTOCOL and row.get('reservation_slot') == slot
                        and row.get('cell_sha256') == cell['cell_sha256']
                        and allocation['decision']['status'] == 'reserved_declaration',
                        'original_probe_enrollment_identity_differs')
                require(owner.records.chain.position(owner.records.name(key)) >
                        owner.records.chain.position(owner.records.name(slot)),
                        'probe_enrollment_precedes_reservation')
                _canonical_root(Path(row['execution_root']))
                _canonical_root(Path(row['candidate_store']))
                found.append(row)
    return found


def _enrollment_value(owner: runtime.GossipChildRuntime, book: capacity.ReservationLedger,
                      slot: str, cell_sha256: str, plan: plans.ProbePlan, *,
                      candidate_store: GitStore, binding: states.ProbeBinding,
                      registration: admission.ObservationRegistration, execution_root: Path,
                      expected: chain.PrefixCommitment, milestone: str, generation: int,
                      quotas: corpus.Quotas, limits: ranked.ReviewLimits,
                      selected: tuple[str, ...] | None) -> dict[str, Any]:
    proof = inspect_probe_cell(owner, book, slot, cell_sha256, plan, candidate_store=candidate_store,
        expected=expected, milestone=milestone, generation=generation, quotas=quotas, limits=limits,
        selected=selected)
    require(type(binding) is states.ProbeBinding and type(registration) is admission.ObservationRegistration,
            'exact_probe_enrollment_binding_and_registration_required')
    expected_registration = states.observation_registration(plan, binding,
        gate_id=registration.gate.gate_id, repetition_id=registration.repetition_id,
        cohort_trajectory_ids=tuple(t.id for t in owner.plan.cohort.trajectories))
    require(registration == expected_registration, 'probe_enrollment_registration_differs')
    original = proof['reservation']['original_child_clock']
    require(original['started_ns'] <= binding.window.started_ns < binding.window.deadline_ns <= original['deadline_ns'],
            'probe_enrollment_window_outside_original_child')
    require(binding.window.deadline_ns - binding.window.started_ns <= plan.policy.history_seconds * 1_000_000_000,
            'probe_enrollment_window_exceeds_history_limit')
    root = _canonical_root(execution_root)
    head = owner.records.chain.authority
    require(type(head) is ExternalHead, 'original_probe_enrollment_head_required')
    assert isinstance(head, ExternalHead)
    protected = (owner.protected.path, candidate_store.path, owner.records.chain.raw_root,
                 owner.records.chain.delta_root, head.root)
    require(all(not _overlap(root, p) for p in protected), 'probe_execution_root_overlaps_protected_state')
    return {'protocol': PROTOCOL, 'reservation_slot': slot, 'cell_sha256': cell_sha256,
        'source_join_sha256': values.digest(proof), 'probe_plan_sha256': values.digest(plan.record()),
        'binding': asdict(binding), 'registration': study.plain(asdict(registration)),
        'candidate_store': str(candidate_store.path), 'execution_root': str(root),
        'roots': {name: str(root / name) for name in ('raw', 'delta', 'head', 'cleanup')},
        'status': 'enrolled_pending_qualification', 'qualified_resource_envelope': False,
        'dispatch_authority': False, 'selection_authority': False, 'acceptance_authority': False}


def enroll_probe_cell(owner: runtime.GossipChildRuntime, book: capacity.ReservationLedger,
                      slot: str, cell_sha256: str, plan: plans.ProbePlan, *,
                      candidate_store: GitStore, binding: states.ProbeBinding,
                      registration: admission.ObservationRegistration, execution_root: Path,
                      expected: chain.PrefixCommitment, milestone: str, generation: int,
                      quotas: corpus.Quotas, limits: ranked.ReviewLimits,
                      selected: tuple[str, ...] | None = None) -> dict[str, Any]:
    """One irreversible root assignment per reserved cell; no dispatch callback.

    Unknown appends follow the existing anchored journal's fail-closed rules.
    Even an identical second call is refused. Inspection never issues a new root.
    """
    value = _enrollment_value(owner, book, slot, cell_sha256, plan, candidate_store=candidate_store,
        binding=binding, registration=registration, execution_root=execution_root, expected=expected,
        milestone=milestone, generation=generation, quotas=quotas, limits=limits, selected=selected)
    key = _enrollment_slot(slot, cell_sha256)
    require(owner.records.read(key) is None, 'probe_cell_already_enrolled')
    require(not execution_root.exists(), 'probe_execution_root_must_be_fresh')
    for row in _enrollments(owner, book):
        old = Path(row['execution_root'])
        require(not _overlap(execution_root, old)
                and not _overlap(execution_root, Path(row['candidate_store']))
                and not _overlap(candidate_store.path, old), 'probe_execution_root_already_assigned_or_overlapping')
    original = child_deadlines.read(owner.records, owner.plan, owner.index, active=True)
    issued = time.monotonic_ns()
    require(binding.window.started_ns <= issued < binding.window.deadline_ns
            and not original.expired(time.time()), 'probe_enrollment_window_expired_or_not_started')
    owner.records.chain.validate_boundary(expected=expected)
    owner.records.put(key, {**value, 'issued_at_ns': issued, 'input_checkpoint': asdict(expected)})
    owner.records.chain.validate_boundary()
    return {'slot': key, 'record_sha256': study.digest(owner.records.read(key)),
            'cell_root_assigned': True, 'dispatch_authority': False, 'acceptance_authority': False}


def inspect_probe_enrollment(owner: runtime.GossipChildRuntime, book: capacity.ReservationLedger,
                             slot: str, cell_sha256: str, plan: plans.ProbePlan, *,
                             candidate_store: GitStore, binding: states.ProbeBinding,
                             registration: admission.ObservationRegistration, execution_root: Path,
                             expected: chain.PrefixCommitment, milestone: str, generation: int,
                             quotas: corpus.Quotas, limits: ranked.ReviewLimits,
                             selected: tuple[str, ...] | None = None) -> dict[str, Any]:
    """Reconstruct the exact retained assignment, including after its time window."""
    value = _enrollment_value(owner, book, slot, cell_sha256, plan, candidate_store=candidate_store,
        binding=binding, registration=registration, execution_root=execution_root, expected=expected,
        milestone=milestone, generation=generation, quotas=quotas, limits=limits, selected=selected)
    key = _enrollment_slot(slot, cell_sha256); row = owner.records.read(key)
    require(type(row) is dict and set(row) == set(value) | {'issued_at_ns', 'input_checkpoint'},
            'complete_original_probe_enrollment_required')
    assert row is not None
    require(values.exact({k: row[k] for k in value}, value), 'original_probe_enrollment_changed')
    prior = chain.PrefixCommitment(**row['input_checkpoint'])
    require(prior == ranked._original_prefix(owner, prior.sequence, expected)
            and owner.records.chain.position(owner.records.name(key)) == prior.sequence + 1
            and type(row['issued_at_ns']) is int
            and binding.window.started_ns <= row['issued_at_ns'] < binding.window.deadline_ns,
            'original_probe_enrollment_chronology_differs')
    rows = _enrollments(owner, book)
    for other in rows:
        if other['reservation_slot'] == slot and other['cell_sha256'] == cell_sha256:
            continue
        require(not _overlap(execution_root, Path(other['execution_root']))
                and not _overlap(execution_root, Path(other['candidate_store']))
                and not _overlap(candidate_store.path, Path(other['execution_root'])),
                'original_probe_root_assignment_conflict')
    owner.records.chain.validate_boundary(expected=expected)
    return {'slot': key, 'record_sha256': study.digest(row), 'cell_root_assigned': True,
            'dispatch_authority': False, 'acceptance_authority': False}
