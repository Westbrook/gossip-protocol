"""Bridge durable task authority and Git CAS without claiming a joint transaction.

An integration intent reserves task attempts before publishing a tested commit.
On an interrupted promotion, recovery inspects accepted Git ancestry and either
finishes task completion or releases the intent for a fresh attempt. Only this
coordinator should finalize journaled promotions. Git refs must not be rewritten
outside the harness. These are local crash/restart semantics, not multi-host HA.
"""

from __future__ import annotations

from dataclasses import dataclass
from contextlib import contextmanager
import fcntl
from pathlib import Path
from typing import Sequence

from .ledger import Ledger, Lease


class SimulatedCrash(RuntimeError):
    """Fault injection after a durable boundary; no cleanup is performed."""


@dataclass(frozen=True)
class PromotionOutcome:
    status: str
    intent_id: str
    head: str
    detail: str


class PromotionCoordinator:
    def __init__(self, ledger: Ledger):
        self.ledger = ledger

    @contextmanager
    def _exclusive(self):
        # Recovery must not cancel a live intent between its journal write and
        # Git CAS. The OS releases this local process lock after a crash.
        with open(str(self.ledger.path) + ".promotion.lock", "a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    def promote(self, store, candidate, leases: Sequence[Lease], *, now: float,
                crash_at: str | None = None) -> PromotionOutcome:
        with self._exclusive():
            return self._promote(store, candidate, leases, now=now, crash_at=crash_at)

    def _promote(self, store, candidate, leases: Sequence[Lease], *, now: float,
                 crash_at: str | None = None) -> PromotionOutcome:
        if candidate.status != "prepared" or not candidate.candidate_sha:
            raise ValueError("Only an exactly tested, prepared candidate can be promoted")
        if crash_at not in {None, "after_intent", "after_git"}:
            raise ValueError("Unknown crash point")
        intent = self.ledger.begin_intent(leases, store.path, candidate.old_head,
                                          candidate.candidate_sha, now=now)
        if intent["state"] != "pending":
            return PromotionOutcome(intent["state"], intent["id"], store.head(), "Replayed finalized intent")
        if crash_at == "after_intent":
            raise SimulatedCrash("Intent persisted; Git has not been updated")
        result = store.accept(candidate)
        if crash_at == "after_git":
            raise SimulatedCrash("Git CAS returned; ledger finalization was interrupted")
        # A repeated caller can see stale after another caller already performed
        # the same CAS. Accepted ancestry, not the stale label alone, decides.
        head = store.head()
        accepted = head == candidate.candidate_sha or store.is_ancestor(candidate.candidate_sha, head)
        self.ledger.finish_intent(intent["id"], accepted, result.detail)
        return PromotionOutcome("accepted" if accepted else result.status, intent["id"], head, result.detail)

    def recover(self) -> list[PromotionOutcome]:
        with self._exclusive():
            return self._recover()

    def _recover(self) -> list[PromotionOutcome]:
        from .gitstore import GitStore
        outcomes = []
        for intent in self.ledger.pending_intents():
            store = GitStore(Path(intent["repository"]))
            head = store.head()
            accepted = head == intent["new_head"] or store.is_ancestor(intent["new_head"], head)
            detail = "Recovered accepted Git ancestry" if accepted else "Unpublished/stale candidate; attempt released"
            self.ledger.finish_intent(intent["id"], accepted, detail)
            outcomes.append(PromotionOutcome("accepted" if accepted else "rejected", intent["id"], head, detail))
        return outcomes
