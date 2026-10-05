"""Fresh Git-bound declarations for public generated probes; no dispatch authority.

Plans produce exact independent review requests. A plan, hash or successful Git
check is not a review, observation, candidate selection or product acceptance.
The future physical owner must recheck this binding at every effect boundary.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
import time
from pathlib import Path, PurePosixPath
from typing import Any

from . import candidate_observation_admission_v1 as admission
from . import candidate_source_capture_deadline_v1 as capture
from . import cumulative_generated_probe_driver_v1 as driver
from . import cumulative_generated_probe_values_v2 as values
from . import cumulative_generated_probe_wire_v1 as wire
from . import cumulative_study_controller_v2 as study
from . import project_acceptance_registry_v1 as registry
from .gitstore import GitStore

PROTOCOL = 'cumulative-generated-probe-plan-v1-enclosing-deadline-v2'
PURPOSE = 'public-generated-probe-development-v1'
REVIEW_PURPOSE = 'independent-generated-probe-source-layout-invocation-v1'
SOURCE_POLICY = capture.DeadlineCapturePolicy()
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
DUTIES = ('complete_source_and_public_invocation', 'capture_representation_or_not_applicable',
          'bounds_release_and_remaining_scope')


def require(condition: bool, detail: str) -> None:
    if not condition:
        raise ValueError(detail)


def _oid(value: str) -> None:
    require(type(value) is str and len(value) == 40 and all(c in '0123456789abcdef' for c in value),
            'complete_git_identity_required')


def evaluator_sources() -> dict[str, str]:
    root = Path(__file__).resolve().parent
    names = ('cumulative_generated_probe_review_v1.py', 'candidate_checkpoint_chain_v1.py',
        'candidate_checkpoint_head_v1.py', 'candidate_http_journal_v3.py',
        'cumulative_generated_probe_plan_v1.py', 'cumulative_generated_probe_driver_v1.py',
        'cumulative_generated_probe_values_v2.py', 'cumulative_generated_probe_wire_v1.py',
        'candidate_observation_admission_v1.py', 'project_acceptance_registry_v1.py', 'gitstore.py')
    result = {**capture.evaluator_sources(),
              **{name: hashlib.sha256((root.parent / name).read_bytes()).hexdigest() for name in study.SOURCE_CLOSURE},
              **{'gossip_harness/' + name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in names}}
    require(result['gossip_harness/cumulative_generated_probe_plan_v1.py'] == LOADED_SOURCE_SHA256,
            'loaded_plan_source_changed')
    admission.verify_loaded_sources(result)
    return dict(sorted(result.items()))


@dataclass(frozen=True, slots=True)
class ProbeTarget:
    subject: registry.Subject
    generation: int
    candidate_id: str
    context_id: str
    stage: str
    commit_oid: str
    tree_oid: str

    def __post_init__(self) -> None:
        require(type(self.subject) is registry.Subject, 'exact_subject_required')
        registry.Subject(**asdict(self.subject))
        require(self.subject.requirements_sha256 == values.PRODUCT_SHA256
                and self.subject.milestone in ('M2', 'M3', 'M4'), 'released_product_subject_required')
        require(type(self.generation) is int and self.generation >= 0, 'exact_nonnegative_generation_required')
        for identifier in (self.candidate_id, self.context_id):
            registry.identifier(identifier)
        require(self.stage in ('candidate-context', 'merged'), 'explicit_candidate_or_merged_stage_required')
        _oid(self.commit_oid)
        _oid(self.tree_oid)


@dataclass(frozen=True, slots=True)
class ProbePolicy:
    """Prospectively chosen limits within existing workflow observation bounds.

    These bounds allocate nothing. The controller still owes aggregate quotas,
    image/runtime/environment binding and the enclosing absolute deadline.
    """
    wire_limits: wire.WireLimits
    stderr_bytes: int
    history_seconds: int
    control_seconds: int
    seed: str

    def __post_init__(self) -> None:
        require(type(self.wire_limits) is wire.WireLimits, 'exact_wire_limits_required')
        wire.WireLimits(**asdict(self.wire_limits))
        require(self.wire_limits.frame_bytes <= 131072 and self.wire_limits.stream_bytes <= 524288
                and self.wire_limits.json_nodes <= 65536 and self.wire_limits.json_depth <= 64,
                'wire_limits_exceed_existing_envelope')
        for number, maximum in ((self.stderr_bytes, 65536), (self.history_seconds, 300), (self.control_seconds, 30)):
            require(type(number) is int and 1 <= number <= maximum, 'limit_exceeds_existing_envelope')
        require(self.control_seconds <= self.history_seconds, 'control_exceeds_history_window')
        registry.identifier(self.seed)


@dataclass(frozen=True, slots=True)
class CaptureLayout:
    """Declared mapper input; only an independent exact-source review can approve it."""
    storage_paths: tuple[str, ...]
    schema_sha256: str

    def __post_init__(self) -> None:
        require(type(self.storage_paths) is tuple and 1 <= len(self.storage_paths) <= 128
                and all(type(path) is str for path in self.storage_paths)
                and tuple(sorted(set(self.storage_paths))) == self.storage_paths
                and 'm2/library.sqlite' in self.storage_paths, 'complete_sorted_capture_path_declaration_required')
        for path in self.storage_paths:
            require(len(path.encode('utf-8')) <= 1024 and not path.startswith('/')
                    and '\\' not in path and '\x00' not in path
                    and all(part not in ('', '.', '..', '.git') for part in path.split('/')),
                    'safe_relative_capture_path_required')
            require(str(PurePosixPath(path)) == path, 'canonical_capture_path_required')
        registry.sha256(self.schema_sha256)


def _release_snapshot(release: study.Release) -> tuple[bytes, tuple[str, ...]]:
    require(type(release) is study.Release, 'exact_public_release_required')
    # Release is frozen but contains mutable dictionaries. Reconstruct and copy.
    fresh = study.Release(**asdict(release))
    return study.canonical_payload(study.plain(asdict(fresh))), fresh.requirement_ids


def _read_release(raw: bytes) -> study.Release:
    body = json.loads(raw, object_pairs_hook=values._pairs, parse_constant=values._constant)
    require(type(body) is dict and study.canonical_payload(body) == raw, 'canonical_release_snapshot_required')
    for name in ('command', 'ordered_check_ids', 'requirement_ids'):
        require(type(body.get(name)) is list, 'release_sequence_required')
        body[name] = tuple(body[name])
    return study.Release(**body)


@dataclass(frozen=True, slots=True)
class ProbePlan:
    target: ProbeTarget
    release_raw: bytes
    admitted_raw: bytes
    policy: ProbePolicy
    layout: CaptureLayout | None
    source_pins: tuple[tuple[str, str], ...]

    def record(self) -> dict[str, Any]:
        require(type(self.target) is ProbeTarget and type(self.policy) is ProbePolicy,
                'exact_target_and_policy_required')
        ProbeTarget(**asdict_target(self.target))
        ProbePolicy(**{**asdict(self.policy), 'wire_limits': self.policy.wire_limits})
        release = _read_release(self.release_raw)
        require(release.milestone == self.target.subject.milestone, 'release_milestone_differs')
        probe = json.loads(self.admitted_raw, object_pairs_hook=values._pairs, parse_constant=values._constant)
        require(values.canonical(probe) == self.admitted_raw, 'canonical_probe_snapshot_required')
        probe = wire.checked_probe(probe, released_requirements=release.requirement_ids)
        needs_capture = probe['template_id'] == 'manifest-content-hash-v1'
        require((needs_capture and type(self.layout) is CaptureLayout) or (not needs_capture and self.layout is None),
                'exact_capture_applicability_required')
        if needs_capture:
            require(release.milestone in ('M3', 'M4'), 'capture_template_not_released')
        if self.layout is not None:
            CaptureLayout(**asdict(self.layout))
        require(type(self.source_pins) is tuple and all(type(row) is tuple and len(row) == 2 for row in self.source_pins),
                'immutable_source_pins_required')
        require(self.source_pins == tuple(evaluator_sources().items()), 'evaluator_source_binding_changed')
        helpers = driver.adapter_files(probe, released_requirements=release.requirement_ids)
        return {'protocol': PROTOCOL, 'purpose': PURPOSE, 'target': asdict(self.target),
            'release_sha256': hashlib.sha256(self.release_raw).hexdigest(),
            'released_requirements': list(release.requirement_ids), 'probe': probe,
            'policy': asdict(self.policy), 'layout': None if self.layout is None else asdict(self.layout),
            'source_capture_policy': SOURCE_POLICY.record(), 'evaluator_sources': dict(self.source_pins),
            'helper_manifest': admission.source_manifest(helpers), 'command': ['python', '-I', '-B', '/checks/child_driver.py'],
            'deadline': 'one controller-supplied absolute window includes source/setup/retention; no resets',
            'acceptance_authority': False, 'dispatch_authority': False}

    def review_request(self) -> dict[str, Any]:
        return {'protocol': PROTOCOL, 'purpose': REVIEW_PURPOSE, 'plan': self.record(),
                'duties': list(DUTIES), 'scope': 'named public generated probe only; not whole-project acceptance'}


def asdict_target(target: ProbeTarget) -> dict[str, Any]:
    return {**asdict(target), 'subject': target.subject}


def prepare_plan(store: GitStore, target: ProbeTarget, admitted: dict[str, Any],
                 release: study.Release, policy: ProbePolicy, layout: CaptureLayout | None = None,
                 *, deadline_ns: int | None = None) -> ProbePlan:
    require(type(store) is GitStore and type(target) is ProbeTarget and type(policy) is ProbePolicy,
            'exact_source_target_policy_required')
    release_raw, requirements = _release_snapshot(release)
    probe_raw = values.canonical(wire.checked_probe(admitted, released_requirements=requirements))
    pins = tuple(evaluator_sources().items())
    plan = ProbePlan(target, release_raw, probe_raw, policy, layout, pins)
    plan.record()
    verify_current_source(store, plan, deadline_ns=deadline_ns)
    require(_release_snapshot(release)[0] == release_raw
            and values.canonical(wire.checked_probe(admitted, released_requirements=requirements)) == probe_raw,
            'caller_inputs_changed_during_capture')
    return plan


def verify_current_source(store: GitStore, plan: ProbePlan, *, deadline_ns: int | None = None) -> dict[str, bytes]:
    """Fresh capture; live callers supply their enclosing bound.

    Construction and cold audit have an independent 60-second capture budget;
    they cannot dispatch. Active state/owner paths always supply their deadline.
    """
    if deadline_ns is None:
        deadline_ns = time.monotonic_ns() + 60_000_000_000
    require(type(store) is GitStore and type(plan) is ProbePlan, 'exact_store_and_plan_required')
    before = values.canonical(plan.record())
    tree, files = capture.capture_registered_source(store, plan.target.commit_oid,
        policy=SOURCE_POLICY, deadline_ns=deadline_ns)
    require(tree == plan.target.tree_oid and admission.source_sha256(files) == plan.target.subject.source_sha256,
            'complete_registered_source_differs')
    require(values.canonical(plan.record()) == before, 'plan_changed_during_capture')
    return files
