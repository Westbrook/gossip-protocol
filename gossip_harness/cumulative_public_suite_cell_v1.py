"""Exact source and authored-suite binding for ranked comparison cells.

The accepted-tree evaluator cannot be reused as evidence for a different
candidate context. This reader reconstructs the original matrix, release and
Git materialization. It grants no execution, reuse, selection or acceptance.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from pathlib import Path
from typing import Any

from . import candidate_checkpoint_chain_v1 as chain
from . import cumulative_generated_probe_accounting_v1 as accounting
from . import cumulative_generated_probe_context_v1 as contexts
from . import cumulative_generated_probe_plan_v1 as plans
from . import cumulative_generated_probe_values_v2 as values
from .gitstore import GitStore

PROTOCOL = 'cumulative-public-suite-cell-v1'
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
require = accounting.require


def sources() -> dict[str, str]:
    require(hashlib.sha256(Path(__file__).read_bytes()).hexdigest() == LOADED_SOURCE_SHA256,
            'loaded_public_cell_reader_changed')
    return {**accounting.sources(),
        'gossip_harness/cumulative_public_suite_cell_v1.py': LOADED_SOURCE_SHA256}


@dataclass(frozen=True, slots=True)
class PublicSuiteCell:
    controller: accounting.runtime.GossipChildRuntime
    book: accounting.capacity.ReservationLedger
    reservation_slot: str
    cell_sha256: str
    candidate_store: GitStore
    commit_oid: str
    milestone: str
    generation: int
    quotas: accounting.corpus.Quotas
    limits: accounting.ranked.ReviewLimits
    selected: tuple[str, ...] | None = None

    def inspect(self, *, expected: chain.PrefixCommitment) -> dict[str, Any]:
        """Read a specific cell at an independent original-controller prefix.

        Before-review cells use the preceding milestone's full authored suite,
        against one base overlay. Contextual cells use the current suite against
        the frozen peer anchor. Merged cells require their complete endorsed
        construction vector. Neither byte equality nor endorsement proves reuse
        eligibility or successful selection.
        """
        require(type(self.controller) is accounting.runtime.GossipChildRuntime
                and type(self.book) is accounting.capacity.ReservationLedger
                and self.book.records is self.controller.records
                and type(self.candidate_store) is GitStore,
                'exact_public_cell_controller_book_and_source_required')
        pins = sources()
        owner = self.controller
        owner.records.chain.validate_boundary(expected=expected)
        row = owner.records.read(self.reservation_slot)
        require(type(row) is dict and type(row.get('request')) is dict,
                'original_public_cell_reservation_missing')
        assert row is not None
        phase = row['request'].get('phase')
        require(phase in ('before-review', 'after-review'), 'original_public_reservation_phase_required')
        original = accounting.inspect(owner, self.book, self.reservation_slot, expected=expected,
            milestone=self.milestone, generation=self.generation, phase=phase,
            quotas=self.quotas, limits=self.limits)
        require(original['decision']['status'] == 'reserved_declaration'
                and original['whole_child_deadline_bound'], 'successful_original_child_public_reservation_required')
        phases = ('before_review',) if phase == 'before-review' else ('contextual', 'merged')
        found = [c for p in phases for c in row['request']['matrix']['cells'][p]
                 if c['cell_sha256'] == self.cell_sha256]
        require(len(found) == 1 and found[0]['kind'] == 'authored-public-suite',
                'exact_reserved_authored_public_cell_required')
        cell = found[0]
        number = accounting.study.MILESTONES.index(self.milestone)
        if phase == 'before-review':
            compiled, _ = accounting.ranked.reconstruct_frozen_builds(owner, expected=expected,
                milestone=self.milestone, generation=self.generation, limits=self.limits, historical=True)
            require(self.selected is None, 'base_overlay_has_no_selected_vector')
            context = contexts.compose(compiled, 'base-overlay', actor=cell['actor'])
            release = owner.plan.releases[number - 1]
        else:
            compiled, _ = accounting.ranked.reconstruct_frozen(owner, expected=expected,
                milestone=self.milestone, generation=self.generation, limits=self.limits, historical=True)
            if cell['phase'] == 'contextual':
                require(self.selected is None, 'contextual_public_cell_has_no_selected_vector')
                context = contexts.compose(compiled, 'contextual', actor=cell['actor'])
            else:
                require(cell['phase'] == 'merged', 'unknown_public_cell_phase')
                context = contexts.compose(compiled, 'merged', selected=self.selected)
            release = owner.plan.releases[number]
        # Copy the mutable dictionaries inside the frozen release. Its exact
        # suite, order and requirement release must match the original cell.
        release_raw = values.canonical(asdict(release))
        public = plans._read_release(release_raw)
        require(cell['definition_sha256'] == public.sha256
                and cell['ordered_check_ids'] == list(public.ordered_check_ids),
                'original_public_cell_release_or_suite_differs')
        if cell['phase'] != 'merged':
            require(values.exact(cell['context'], context.record()), 'original_public_cell_context_differs')
        require(self.candidate_store.path != owner.protected.path,
                'separate_public_candidate_view_required')
        contexts.oid(self.commit_oid)
        require(self.candidate_store.head() == self.commit_oid, 'public_candidate_head_differs')
        materialized = contexts.verify_materialized(self.candidate_store, compiled, context, self.commit_oid)
        require(self.candidate_store.head() == self.commit_oid, 'public_candidate_head_changed_during_read')
        # Re-authenticate financial/role originals and release bytes after the
        # potentially long source capture, not just the journal prefix.
        repeated = accounting.inspect(owner, self.book, self.reservation_slot, expected=expected,
            milestone=self.milestone, generation=self.generation, phase=phase,
            quotas=self.quotas, limits=self.limits)
        require(values.exact(repeated, original) and values.canonical(asdict(release)) == release_raw,
                'public_cell_originals_changed_during_read')
        require(self.candidate_store.head() == self.commit_oid, 'public_candidate_head_changed_during_read')
        owner.records.chain.validate_boundary(expected=expected)
        require(sources() == pins, 'public_cell_reader_sources_changed')
        return {'protocol': PROTOCOL, 'reader_sources': pins, 'cell': cell,
            'reservation': original, 'controller_checkpoint': asdict(expected),
            'context': context.record(), 'materialized_source': materialized,
            'candidate_store': str(self.candidate_store.path), 'release': json.loads(release_raw),
            'release_sha256': public.sha256, 'authored_checks_available': bool(public.checks),
            'declared_charge': next(c for c in row['request']['charges'] if c['cell_sha256'] == self.cell_sha256),
            'declared_schedule': next(c for c in original['decision']['schedule'] if c['cell_sha256'] == self.cell_sha256),
            'original_cell_bound': True, 'original_source_bound': True,
            'qualified_resource_envelope': False, 'fresh_execution_required': True,
            'dispatch_authority': False, 'execution_reuse_authority': False,
            'selection_authority': False, 'acceptance_authority': False}
