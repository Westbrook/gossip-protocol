"""Fresh registered-source capture bounded by the caller's monotonic deadline.

Reuse the frozen parsers and owned-process cleanup, never their fresh relative
clock. Both accepted-head reads and the two capture children share one deadline.
Owned reap retains a separate five-second allowance; OS calls are not hard
real-time preemptible. This module creates no controller or acceptance authority.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import math
from pathlib import Path
import time

from . import candidate_git_source_batch_v1 as legacy
from . import candidate_git_source_two_process_v1 as capture
from . import candidate_source_capture_policy_v2 as frozen
from .gitstore import ACCEPTED, GitStore

PROTOCOL = 'candidate-source-capture-enclosing-deadline-v1'
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
require = legacy._require


def evaluator_sources() -> dict[str, str]:
    require(hashlib.sha256(Path(__file__).read_bytes()).hexdigest() == LOADED_SOURCE_SHA256,
            'loaded_deadline_capture_changed')
    return {**frozen.evaluator_sources(),
            'gossip_harness/candidate_source_capture_deadline_v1.py': LOADED_SOURCE_SHA256}


@dataclass(frozen=True, slots=True)
class DeadlineCapturePolicy:
    timeout_seconds: int = 60
    cleanup_reap_seconds: int = 5

    def record(self) -> dict[str, object]:
        require(type(self) is DeadlineCapturePolicy, 'exact_deadline_capture_policy_required')
        limits = frozen.TwoProcessCapturePolicy(self.timeout_seconds, self.cleanup_reap_seconds).record()
        return {**limits, 'protocol': PROTOCOL, 'sources': evaluator_sources(),
                'deadline_contract': 'min(caller-absolute-monotonic-ns,entry+60s);no-renewal;owned-reap=5s',
                'children_per_capture': 4,
                'phase_order': ['accepted-head', 'ls-tree', 'info-all', 'contents-all',
                                'EOF-exit-cleanup', 'accepted-head'],
                'enclosing_deadline_required': True}


def capture_registered_source(store: GitStore, commit_oid: str, *,
                              policy: DeadlineCapturePolicy,
                              deadline_ns: int) -> tuple[str, dict[str, bytes]]:
    started = time.monotonic_ns()
    require(type(deadline_ns) is int and 0 < deadline_ns <= 2**63 - 1,
            'exact_enclosing_capture_deadline_required')
    require(type(store) is GitStore and type(policy) is DeadlineCapturePolicy,
            'exact_store_and_deadline_capture_policy_required')
    require(type(commit_oid) is str and capture._OID.fullmatch(commit_oid) is not None,
            'exact_capture_commit_required')
    require(started < deadline_ns, 'Capture deadline exhausted')
    policy.record()
    # Round down when handing nanoseconds to the retained float-clock transport.
    deadline = math.nextafter(min(deadline_ns, started + 60_000_000_000) / 1_000_000_000, -math.inf)

    def current_head() -> None:
        legacy._deadline(deadline)
        raw = legacy._command(store.path, ('rev-parse', '--verify', ACCEPTED), b'', 41, deadline)
        require(raw == commit_oid.encode('ascii') + b'\n', 'registered_head_changed')
        legacy._deadline(deadline)

    current_head()
    listing = legacy._command(store.path, ('ls-tree', '-r', '-z', commit_oid),
                              b'', capture.MAX_TREE_BYTES, deadline)
    legacy._deadline(deadline)
    entries = capture._entries(listing, deadline)
    legacy._deadline(deadline)
    result = capture._cat_capture(store.path, commit_oid, entries, deadline)
    legacy._deadline(deadline)
    current_head()
    return result
