"""Durable one-shot probe intent and exact prospective admission; no process IO.

This component authenticates registered declarations, current source and review.
It does not verify a Docker runtime, execute a candidate, produce an observation,
or approve a merge. The physical owner must add those original-only boundaries.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
import os
from pathlib import Path
import threading
import time
from typing import Any

from . import candidate_checkpoint_chain_v1 as chain
from .candidate_checkpoint_head_v1 import ExternalHead
from . import candidate_execution_journal_v1 as journals
from . import candidate_observation_admission_v1 as admission
from . import cumulative_generated_probe_plan_v1 as plans
from . import cumulative_generated_probe_review_v1 as reviews
from . import cumulative_generated_probe_values_v2 as values
from . import project_acceptance_registry_v1 as registry
from .gitstore import GitStore

PROTOCOL = 'cumulative-generated-probe-state-v1-enclosing-deadline-v2'
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
require = plans.require


def evaluator_sources() -> dict[str, str]:
    root = Path(__file__).resolve().parent
    result = {**plans.evaluator_sources(), **{'gossip_harness/' + name: hashlib.sha256((root / name).read_bytes()).hexdigest()
        for name in ('cumulative_generated_probe_state_v1.py', 'candidate_execution_journal_v1.py')}}
    require(result['gossip_harness/cumulative_generated_probe_state_v1.py'] == LOADED_SOURCE_SHA256,
            'loaded_probe_state_source_changed')
    admission.verify_loaded_sources(result)
    return dict(sorted(result.items()))


@dataclass(frozen=True, slots=True)
class ProbeWindow:
    started_ns: int
    deadline_ns: int

    def __post_init__(self) -> None:
        require(type(self.started_ns) is int and type(self.deadline_ns) is int
                and 0 < self.started_ns < self.deadline_ns <= 2**63-1, 'exact_monotonic_window_required')


@dataclass(frozen=True, slots=True)
class ProbeBinding:
    source_sha256: str
    requirements_sha256: str
    milestone: str
    plan_sha256: str
    review_sha256: str
    review_origin_sha256: str
    evaluator_sha256: str
    runtime_sha256: str
    environment_sha256: str
    limits_sha256: str
    seed_sha256: str
    window: ProbeWindow
    purpose: str = 'public_release'
    protocol: str = PROTOCOL

    def __post_init__(self) -> None:
        require(self.protocol == PROTOCOL and self.purpose == 'public_release', 'public_probe_binding_required')
        require(self.requirements_sha256 == values.PRODUCT_SHA256 and self.milestone in ('M2', 'M3', 'M4'),
                'released_product_binding_required')
        require(type(self.window) is ProbeWindow, 'exact_window_required')
        ProbeWindow(**asdict(self.window))
        for name, value in asdict(self).items():
            if name.endswith('_sha256'):
                registry.sha256(value)


def binding_for(plan: plans.ProbePlan, review: reviews.ProbeReviewAuthority, *,
                runtime: dict[str, Any], environment: dict[str, Any], window: ProbeWindow) -> ProbeBinding:
    require(type(plan) is plans.ProbePlan and type(review) is reviews.ProbeReviewAuthority,
            'exact_plan_review_required')
    require(type(runtime) is dict and bool(runtime) and type(environment) is dict and bool(environment),
            'explicit_declared_runtime_environment_required')
    require(type(window) is ProbeWindow, 'exact_window_required')
    require(window.deadline_ns-window.started_ns <= plan.policy.history_seconds*1_000_000_000,
            'window_exceeds_registered_history_limit')
    plan_record = plan.record()
    report, provenance = review.authenticate_with_provenance(plan)
    require(values.canonical(plan.record()) == values.canonical(plan_record), 'plan_changed_during_review')
    subject = plan.target.subject
    return ProbeBinding(subject.source_sha256, subject.requirements_sha256, subject.milestone,
        values.digest(plan_record), report, values.digest(provenance), values.digest(evaluator_sources()),
        values.digest(runtime), values.digest(environment),
        values.digest({'policy': asdict(plan.policy), 'window': asdict(window),
                       'journal': asdict(journals.Limits()), 'clock': 'controller-registered-absolute-monotonic-v1'}),
        values.digest({'seed': plan.policy.seed}), window)


def observation_registration(plan: plans.ProbePlan, binding: ProbeBinding, *, gate_id: str,
                             repetition_id: str, cohort_trajectory_ids: tuple[str, ...]) -> admission.ObservationRegistration:
    require(type(plan) is plans.ProbePlan and type(binding) is ProbeBinding, 'exact_plan_binding_required')
    record = plan.record()
    require(values.digest(record) == binding.plan_sha256 and plan.target.subject.source_sha256 == binding.source_sha256
            and plan.target.subject.milestone == binding.milestone, 'plan_binding_differs')
    probe = record['probe']
    cases = ('probe-' + probe['probe_id'] + '-value', 'probe-' + probe['probe_id'] + '-mechanics')
    gate = registry.Gate(gate_id, (probe['requirement_id'],), cases,
        registry.Binding(plan.target.subject, values.digest({'plan': record, 'ordered_cases': cases}),
            binding.evaluator_sha256, binding.runtime_sha256, binding.environment_sha256,
            binding.limits_sha256, binding.seed_sha256, binding.protocol, binding.purpose))
    return admission.ObservationRegistration(gate, plan.target.commit_oid, plan.target.tree_oid,
        repetition_id, cohort_trajectory_ids, binding.plan_sha256, binding.plan_sha256,
        plans.PURPOSE, admission.binding_sha256(binding, gate=gate))


class ProbeExecutionState:
    """Thread/process-affine state primitive with independently anchored originals.

    No execution callback is accepted. begin() only records intent. A physical
    owner must still check actual runtime, retain its dispatch original and
    enforce these same boundaries before any external effect or continuation.
    Reopening an intent permits inspection; it never permits redispatch.
    """
    def __init__(self, root: Path, delta_root: Path, *, head: ExternalHead, store: GitStore,
                 plan: plans.ProbePlan, review: reviews.ProbeReviewAuthority,
                 binding: ProbeBinding, registration: admission.ObservationRegistration,
                 admission_authority: admission.ObservationAdmission,
                 runtime: dict[str, Any], environment: dict[str, Any],
                 expected_checkpoint: chain.PrefixCommitment | None = None) -> None:
        require(type(head) is ExternalHead and type(store) is GitStore and type(plan) is plans.ProbePlan
                and type(review) is reviews.ProbeReviewAuthority and type(binding) is ProbeBinding
                and type(registration) is admission.ObservationRegistration
                and type(admission_authority) is admission.ObservationAdmission, 'exact_probe_state_capabilities_required')
        self.root, self.delta_root = Path(root), Path(delta_root)
        own = (self.root, self.delta_root, head.root)
        review_head = review.journal.authority
        require(type(review_head) is ExternalHead, 'exact_review_head_required')
        assert isinstance(review_head, ExternalHead)
        protected = (review.journal.raw_root, review.journal.delta_root, review_head.root, store.path)
        require(all(path.is_absolute() and path.resolve() == path for path in own + protected),
                'canonical_separate_state_roots_required')
        require(head.journal_roots == (self.root, self.delta_root), 'state_head_roots_differ')
        require(all(not left.is_relative_to(right) and not right.is_relative_to(left)
                    for i, left in enumerate(own) for right in own[i+1:] + protected), 'state_proof_source_roots_overlap')
        self._pid, self._thread, self.closed = os.getpid(), threading.get_ident(), False
        self.store, self.plan, self.review = store, plan, review
        self.binding, self.registration, self.admission = binding, registration, admission_authority
        self.runtime_raw, self.environment_raw = values.canonical(runtime), values.canonical(environment)
        self._plan_raw = values.canonical(plan.record())
        self._binding_raw = values.canonical(asdict(binding))
        self.journal: journals.OwnerJournal | None = None
        expected = observation_registration(plan, binding, gate_id=registration.gate.gate_id,
            repetition_id=registration.repetition_id, cohort_trajectory_ids=registration.cohort_trajectory_ids)
        require(expected == registration == admission_authority.registration, 'prospective_probe_registration_differs')
        self.sources = evaluator_sources()
        config = {'protocol': PROTOCOL, 'scope': 'durable execution state only; no physical observation authority',
            'repository': str(store.path), 'plan': json.loads(self._plan_raw), 'release_raw_sha256': hashlib.sha256(plan.release_raw).hexdigest(),
            'binding': asdict(binding), 'registration': asdict(registration),
            'runtime_declaration': json.loads(self.runtime_raw), 'environment_declaration': json.loads(self.environment_raw),
            'evaluator_sources': self.sources}
        self.config_raw = values.canonical(config)
        existed = self.root.exists()
        require(existed == (expected_checkpoint is not None), 'reopen_requires_independent_expected_prefix')
        self._validate_current(check_window=not existed)
        try:
            self.journal = journals.OwnerJournal(self.root, self.delta_root,
                context={'protocol': PROTOCOL, 'config_sha256': hashlib.sha256(self.config_raw).hexdigest()},
                authority=head, expected=expected_checkpoint)
            if existed:
                require(self.journal.read('config.json') == self.config_raw, 'original_probe_config_differs')
            else:
                self.journal.retain('config.json', self.config_raw)
            self.journal.checkpoint()
            if not existed:
                self._window()
        except BaseException:
            self.close()
            raise

    def _owner(self) -> None:
        require(not self.closed and self._pid == os.getpid() and self._thread == threading.get_ident(),
                'probe_state_closed_or_wrong_owner')

    def _window(self) -> None:
        now = time.monotonic_ns()
        require(self.binding.window.started_ns <= now < self.binding.window.deadline_ns,
                'probe_absolute_window_unavailable')

    def _validate_current(self, *, check_window: bool, deadline_ns: int | None = None) -> None:
        self._owner()
        if check_window:
            self._window()
            if deadline_ns is not None:
                require(type(deadline_ns) is int and 0 < deadline_ns <= 2**63-1,
                        'exact_probe_control_deadline_required')
            deadline_ns = min(self.binding.window.deadline_ns,
                              deadline_ns if deadline_ns is not None else self.binding.window.deadline_ns)
            require(time.monotonic_ns() < deadline_ns, 'probe_control_deadline')
        require(values.canonical(self.plan.record()) == self._plan_raw
                and values.canonical(asdict(self.binding)) == self._binding_raw, 'probe_plan_or_binding_replaced')
        require(evaluator_sources() == self.sources, 'probe_state_evaluator_changed')
        self.admission.check_current(self.registration, None)
        plans.verify_current_source(self.store, self.plan, deadline_ns=deadline_ns)
        expected = binding_for(self.plan, self.review, runtime=json.loads(self.runtime_raw),
                               environment=json.loads(self.environment_raw), window=self.binding.window)
        require(expected == self.binding, 'probe_runtime_review_or_binding_changed')
        self.admission.check_current(self.registration, None)
        if check_window:
            self._window()
            assert deadline_ns is not None
            require(time.monotonic_ns() < deadline_ns, 'probe_control_deadline')

    def current(self, *, deadline_ns: int | None = None) -> chain.PrefixCommitment:
        self._owner()
        require(self.journal is not None, 'probe_journal_unavailable')
        assert self.journal is not None
        self.journal.checkpoint()
        self._validate_current(check_window=True, deadline_ns=deadline_ns)
        checkpoint = self.journal.checkpoint()
        self._window()
        require(deadline_ns is None or time.monotonic_ns() < deadline_ns, 'probe_control_deadline')
        return checkpoint

    def begin(self) -> dict[str, Any]:
        """Durable one-shot intent only; never invoke or authorize a process here."""
        self._owner()
        assert self.journal is not None
        self.journal.checkpoint()
        require(not self.journal.has('intent.json'), 'existing_probe_intent_forbids_redispatch')
        self.current()
        require(self.admission.before_intent(self.registration) is None, 'public_probe_cannot_adopt_acceptance_freeze')
        intent = {'protocol': PROTOCOL, 'binding_sha256': values.digest(asdict(self.binding)),
            'registration': asdict(self.registration), 'plan_sha256': self.binding.plan_sha256,
            'window': asdict(self.binding.window), 'status': 'intent_recorded',
            'candidate_dispatched': False, 'execution_authority': False, 'acceptance_authority': False}
        self.journal.retain('intent.json', values.canonical(intent))
        self.journal.checkpoint()
        self.current()
        return intent

    def close(self) -> None:
        if not self.closed:
            require(self._pid == os.getpid() and self._thread == threading.get_ident(), 'wrong_probe_close_owner')
            if self.journal is not None:
                self.journal.close()
            self.closed = True

    def __enter__(self) -> ProbeExecutionState:
        self._owner()
        return self

    def __exit__(self, *_args: Any) -> None:
        self.close()
