"""Read the complete generated-probe phase from enrolled original observations.

This is the generated part of the ranked comparison, not a winner selector.
Authored public suites, qualified resource envelopes and controller adoption
remain required. Missing cells stay in the census; no input verdict is trusted.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
from pathlib import Path
from typing import Any

from . import candidate_checkpoint_chain_v1 as chain
from . import cumulative_generated_probe_accounting_v1 as accounting
from . import cumulative_generated_probe_context_v1 as contexts
from . import cumulative_generated_probe_reader_v1 as reader
from . import cumulative_generated_probe_values_v2 as values

PROTOCOL = 'cumulative-generated-probe-matrix-reader-v1'
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
require = reader.require


@dataclass(frozen=True, slots=True)
class OriginalObservation:
    state: reader.state.ProbeExecutionState
    expected: chain.PrefixCommitment
    enrollment: accounting.ProbeCellEnrollment

    def __post_init__(self) -> None:
        require(type(self.state) is reader.state.ProbeExecutionState
                and type(self.expected) is chain.PrefixCommitment
                and type(self.enrollment) is accounting.ProbeCellEnrollment,
                'exact_original_observation_capabilities_required')


def sources() -> dict[str, str]:
    require(hashlib.sha256(Path(__file__).read_bytes()).hexdigest() == LOADED_SOURCE_SHA256,
            'loaded_matrix_reader_changed')
    return {**reader.sources(),
            'gossip_harness/cumulative_generated_probe_matrix_reader_v1.py': LOADED_SOURCE_SHA256}


def reconstruct(controller: accounting.runtime.GossipChildRuntime,
                book: accounting.capacity.ReservationLedger, reservation_slot: str, *,
                expected: chain.PrefixCommitment, milestone: str, generation: int,
                phase: str, quotas: accounting.corpus.Quotas, limits: accounting.ranked.ReviewLimits,
                observations: tuple[OriginalObservation, ...],
                selected: tuple[str, ...] | None = None) -> dict[str, Any]:
    """Preserve every required cell, failed and unavailable observations included.

    A subset of original observations is allowed for interrupted runs, but the
    matrix itself is always rebuilt from the complete original corpus. Missing
    observations are unavailable, not passes or omissions. Extra, duplicated or
    foreign capabilities invalidate the join. Contextual and merged phases are
    separate even when their source bytes happen to be identical.
    """
    pins = sources()
    require(phase in ('contextual', 'merged'), 'explicit_probe_observation_phase_required')
    require(type(observations) is tuple and all(type(o) is OriginalObservation for o in observations),
            'original_observation_roster_required')
    original = accounting.inspect(controller, book, reservation_slot, expected=expected,
        milestone=milestone, generation=generation, phase='after-review', quotas=quotas, limits=limits)
    require(original['decision']['status'] == 'reserved_declaration'
            and original['whole_child_deadline_bound'], 'successful_original_child_reservation_required')
    _, matrix, _ = accounting.corpus.read(controller, expected=expected, milestone=milestone,
        generation=generation, quotas=quotas, limits=limits)
    compiled, _ = accounting.ranked.reconstruct_frozen(controller, expected=expected,
        milestone=milestone, generation=generation, limits=limits, historical=True)
    if phase == 'merged':
        # Endorsement authenticates construction only. Selection is still owed.
        context = contexts.compose(compiled, 'merged', selected=selected).record()
    else:
        require(selected is None, 'contextual_phase_cannot_borrow_selected_vector')
        context = None
    cells = matrix.contextual_cells if phase == 'contextual' else matrix.merged_cells
    required = {c.sha256: c for c in cells if c.kind == 'generated-history'}
    supplied: dict[str, OriginalObservation] = {}
    roots: set[Path] = set()
    for observation in observations:
        e = observation.enrollment
        require(e.owner is controller and e.book is book
                and (e.reservation_slot, e.milestone, e.generation, e.quotas, e.limits, e.selected) ==
                    (reservation_slot, milestone, generation, quotas, limits, selected),
                'observation_belongs_to_another_original_comparison')
        require(e.cell_sha256 in required and e.cell_sha256 not in supplied,
                'duplicate_or_foreign_probe_cell')
        require(e.execution_root not in roots, 'probe_execution_reused_across_cells')
        supplied[e.cell_sha256] = observation
        roots.add(e.execution_root)
    rows: list[dict[str, Any]] = []
    for identity, cell in required.items():
        available = supplied.get(identity)
        result = None
        if available is not None:
            result = reader.reconstruct_enrolled(available.state, expected=available.expected,
                enrollment=available.enrollment, controller_expected=expected)
        rows.append({'cell_sha256': identity, 'actor': cell.actor,
            'definition_sha256': cell.definition_sha256, 'ordered_check_ids': list(cell.ordered_check_ids),
            'disposition': result['observation']['disposition'] if result else 'unavailable',
            'mechanics_complete': result['observation']['mechanics_complete'] if result else False,
            'missing_original': result is None, 'original': result})
    # An earlier execution may advance while a later one is read. Check every
    # independent prefix again; a per-cell check alone cannot prove one census.
    for observation in observations:
        observation.state._validate_current(check_window=False)
        journal = observation.state.journal
        require(journal is not None and journal.checkpoint() == observation.expected,
                'probe_changed_during_matrix_read')
    controller.records.chain.validate_boundary(expected=expected)
    require(sources() == pins, 'matrix_reader_sources_changed')
    counts = {kind: sum(row['disposition'] == kind for row in rows)
              for kind in ('pass', 'fail', 'unavailable')}
    # Empty means no generated tests were available, never empirical support.
    disposition = ('fail' if counts['fail'] else 'unavailable' if counts['unavailable']
                   else 'pass' if rows else 'not_applicable')
    return {'protocol': PROTOCOL, 'reader_sources': pins, 'phase': phase,
        'generation_sha256': compiled.sha256, 'matrix_sha256': values.digest(matrix.record()),
        'controller_checkpoint': asdict(expected), 'reservation': original,
        'selected_context': context, 'cells': rows, 'counts': counts,
        'required_cells': len(required), 'supplied_cells': len(supplied),
        'disposition': disposition, 'mechanics_complete': all(row['mechanics_complete'] for row in rows),
        'authored_public_cells_outstanding': [c.record() for c in cells if c.kind == 'authored-public-suite'],
        'complete_comparison': False, 'qualified_resource_envelope': False,
        'selection_authority': False, 'acceptance_authority': False,
        'physically_executed_by_reader': False, 'independent_acceptance': False}
