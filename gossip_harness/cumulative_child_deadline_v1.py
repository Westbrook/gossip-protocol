"""Original child-start clock, explicitly selected by the prospective plan.

Monotonic time is never reconstructed from a later wall-clock sample. Active
use is limited to the creating interpreter incarnation; cold reads remain
possible elsewhere. A process restart cannot mint a new active horizon.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import math
import os
from pathlib import Path
import time
from typing import Any, Callable, TYPE_CHECKING
import uuid

if TYPE_CHECKING:
    from .cumulative_study_controller_v2 import Records, StudyPlan

PROTOCOL = 'cumulative-original-child-deadline-v1'
POLICY_KEY = 'original_child_deadline'
POLICY = {'protocol': PROTOCOL, 'origin': 'child.begin.before-runtime-construction',
          'clock': 'monotonic_ns', 'active_reopen': 'same-interpreter-incarnation-only',
          'wall_clock': 'additional-earlier-stop-only', 'renewal': 'forbidden'}
SOURCE = 'gossip_harness/cumulative_child_deadline_v1.py'
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
_INCARNATION = uuid.uuid4().hex


def _require(condition: bool, reason: str) -> None:
    from .cumulative_study_controller_v2 import require
    require(condition, reason)


def clock_domain() -> str:
    return 'child-clock-' + str(os.getpid()) + '-' + _INCARNATION


def selected(plan: StudyPlan) -> bool:
    if POLICY_KEY not in plan.runtime:
        return False
    from .cumulative_study_controller_v2 import digest
    _require(digest(plan.runtime[POLICY_KEY]) == digest(POLICY), 'exact_original_child_deadline_policy_required')
    _require(plan.source_pins.get(SOURCE) == LOADED_SOURCE_SHA256
             == hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), 'original_child_deadline_source_not_pinned')
    return True


def _wall(value: Any) -> bool:
    return type(value) in (int, float) and math.isfinite(value) and 0 <= value <= 1e12


def begin(plan: StudyPlan, index: int, wall_clock: Callable[[], float]) -> dict[str, Any]:
    """Sample once before setup; the controller durably records this exact value."""
    _require(selected(plan), 'original_child_deadline_not_selected')
    started_ns = time.monotonic_ns()
    started_at = wall_clock()
    _require(_wall(started_at) and _wall(started_at + plan.horizon_seconds), 'invalid_child_start_wall_clock')
    deadline_ns = started_ns + plan.horizon_seconds * 1_000_000_000
    _require(0 < started_ns < deadline_ns <= 2**63-1, 'invalid_original_child_clock')
    from .cumulative_study_controller_v2 import plain
    return {'trajectory': plain(asdict(plan.cohort.trajectories[index])),
            'started_at': started_at, 'deadline': started_at + plan.horizon_seconds,
            'contract_sha256': plan.sha256,
            'original_clock': {'protocol': PROTOCOL, 'clock_domain': clock_domain(),
                               'started_ns': started_ns, 'deadline_ns': deadline_ns}}


@dataclass(frozen=True, slots=True)
class ChildClock:
    origin_slot: str
    origin_sha256: str
    plan_sha256: str
    trajectory: str
    clock_domain: str
    started_ns: int
    deadline_ns: int
    deadline_unix: float

    def expired(self, wall_now: float) -> bool:
        # This immutable projection can be consumed by the financial RPC thread
        # without crossing the journal's owner-thread boundary. Only read()
        # authenticates originals; direct construction grants no admission.
        _require(type(self) is ChildClock and self.clock_domain == clock_domain(),
                 'foreign_child_clock_incarnation')
        _require(_wall(wall_now), 'invalid_current_child_wall_clock')
        now = time.monotonic_ns()
        _require(now >= self.started_ns, 'child_monotonic_clock_moved_backwards')
        return now >= self.deadline_ns or wall_now >= self.deadline_unix


def read(records: Records, plan: StudyPlan, index: int, *, active: bool = False) -> ChildClock:
    """Rejoin the anchored child.begin; never append, renew or dispatch."""
    from .cumulative_study_controller_v2 import Records, digest, plain
    from .candidate_checkpoint_chain_v1 import CheckpointChain
    from .candidate_checkpoint_head_v1 import ExternalHead
    _require(type(records) is Records and type(records.chain) is CheckpointChain
             and type(records.chain.authority) is ExternalHead, 'original_child_clock_journal_required')
    _require(type(index) is int and 0 <= index < len(plan.cohort.trajectories)
             and selected(plan), 'selected_original_child_clock_required')
    records.chain.validate_boundary()
    _require(digest(records.read('contract')) == digest(plan.record()), 'original_child_contract_differs')
    trajectory = plan.cohort.trajectories[index]
    slot = 'child.' + trajectory.id + '.begin'
    row = records.read(slot)
    _require(type(row) is dict and set(row) == {'trajectory','started_at','deadline','contract_sha256','original_clock'},
             'complete_original_child_start_required')
    assert row is not None
    _require(row['contract_sha256'] == plan.sha256
             and digest(row['trajectory']) == digest(plain(asdict(trajectory)))
             and _wall(row['started_at']) and _wall(row['deadline'])
             and row['deadline'] == row['started_at'] + plan.horizon_seconds,
             'original_child_start_identity_or_horizon_differs')
    c = row['original_clock']
    _require(type(c) is dict and set(c) == {'protocol','clock_domain','started_ns','deadline_ns'}
             and c['protocol'] == PROTOCOL and type(c['clock_domain']) is str
             and c['clock_domain'].startswith('child-clock-')
             and type(c['started_ns']) is int and type(c['deadline_ns']) is int
             and 0 < c['started_ns'] < c['deadline_ns'] <= 2**63-1
             and c['deadline_ns'] - c['started_ns'] == plan.horizon_seconds * 1_000_000_000,
             'original_child_monotonic_horizon_differs')
    position = records.chain.position(records.name(slot))
    _require(records.chain.position(records.name('contract')) < position,
             'child_clock_must_follow_original_contract')
    # Reject a genuine but late start inserted after setup or source work.
    for suffix in ('.source-initialization.intent', '.financial-contract', '.financial-config'):
        key = 'runtime.' + trajectory.id + suffix
        if records.read(key) is not None:
            _require(position < records.chain.position(records.name(key)), 'child_clock_follows_runtime_work')
    for milestone in plan.releases:
        key = 'child.' + trajectory.id + '.' + milestone.milestone + '.release'
        if records.read(key) is not None:
            _require(position < records.chain.position(records.name(key)), 'child_clock_follows_public_release')
    value = ChildClock(slot, digest(row), plan.sha256, trajectory.id, c['clock_domain'],
                       c['started_ns'], c['deadline_ns'], float(row['deadline']))
    if active:
        _require(value.clock_domain == clock_domain(), 'foreign_child_clock_incarnation')
        _require(time.monotonic_ns() >= value.started_ns, 'child_monotonic_clock_moved_backwards')
    records.chain.validate_boundary()
    return value
