"""Source-bound workflow execution and a separate closed harness qualifier.

Actual solution.solve calls share one inspected Python exec process. Enrolled
read-only boundaries allow host-owned captures before temporary state disappears.
Expected product values stay on the host. Trace records alone never prove
delegation, hook placement, persistence or process identity.
"""
from __future__ import annotations

from contextlib import contextmanager, nullcontext
from dataclasses import asdict, dataclass
import hashlib
import os
from pathlib import Path
import platform
import queue
import re
import select
import json
import time
import subprocess
import tempfile
import threading
from typing import Any, Iterator
import uuid

from . import candidate_storage_product_execution_v1 as transport
from . import candidate_storage_driver_v1 as b01
from . import candidate_intake_store_driver_v3 as b02
from . import candidate_workflow_profile_v1 as profile
from . import candidate_workflow_review_v1 as review
from . import candidate_observation_admission_v1 as admission
from . import candidate_execution_journal_v1 as journals
from . import candidate_checkpoint_chain_v1 as chain
from .candidate_checkpoint_head_v1 import ExternalHead
from . import candidate_emergency_cleanup_v1 as cleanup
from . import candidate_client_process_v4 as process
from . import candidate_source_capture_policy_v1 as source_capture
from . import cumulative_workflow_exposure_v1 as exposure
from .gitstore import GitStore
from .sandbox import DockerValidator
from . import candidate_storage_prestart_v1 as prestart
from . import project_acceptance_registry_v1 as registry

PROTOCOL = 'candidate-workflow-execution-v1'
ADAPTER_PROTOCOL = 'candidate-workflow-wire-v1'
TARGET_CONTRACT = transport.TARGET_CONTRACT
FAMILY = 'workflow-final-m4-v1'
SOURCE_CAPTURE_POLICY = source_capture.BatchCapturePolicy()
LIMITS = chain.Limits()
CHUNK_BYTES = transport.CHUNK_BYTES
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
encoded, digest = profile.encoded, profile.digest
ExecutionError, ExecutionUnknown = transport.ExecutionError, transport.ExecutionUnknown
require, sha = transport.require, transport.sha
_verify_regular_tree = transport._verify_regular_tree


DEADLINE_POLICY_ID = 'workflow-absolute-deadline-enforcement-v2'
TIMING_PROTOCOL = 'workflow-host-segments-diagnostic-v1'
TIMING_MAX_RECORDS = 4096
TIMING_MAX_BYTES = 2097152
TIMING_STAGES = ('effect', 'checkpoint', 'source', 'current', 'runtime', 'control',
    'queue', 'write', 'capture', 'teardown')


class WorkflowDeadlineExceeded(transport.ExecutionError):
    """Expired owner observation window, distinct from source/admission failure."""


def deadline_policy() -> dict[str, Any]:
    return {'id': DEADLINE_POLICY_ID, 'call_seconds': 30, 'history_seconds': 300,
        'clock': 'one absolute monotonic window; no reset or host-work discount',
        'eligibility': 'required durable source-bound decision; optional timings never authority',
        'cleanup': 'separate existing bounded owned teardown; no application writes',
        'diagnostics': {'protocol': TIMING_PROTOCOL, 'max_records': TIMING_MAX_RECORDS,
            'max_bytes': TIMING_MAX_BYTES, 'durations': 'inclusive nested spans; do not sum',
            'retention': 'one exclusive sibling sidecar after owned teardown; never scored'}}


class _Timings:
    """Fixed-size diagnostic memory only: no journal writes, inputs or verdicts."""
    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []
        self.stack: list[int] = []
        self.omitted = False

    @contextmanager
    def span(self, stage: str) -> Iterator[None]:
        if stage not in TIMING_STAGES or len(self.rows) >= TIMING_MAX_RECORDS or len(self.stack) >= 16:
            self.omitted = True
            yield
            return
        try:
            started = time.monotonic_ns()
        except BaseException:
            self.omitted = True
            yield
            return
        seq = len(self.rows)
        row: dict[str, Any] = {'seq': seq, 'parent': self.stack[-1] if self.stack else None,
            'stage': stage, 'start_ns': started, 'end_ns': None, 'status': 'open'}
        self.rows.append(row)
        self.stack.append(seq)
        try:
            yield
        except BaseException:
            row['status'] = 'error'
            raise
        else:
            row['status'] = 'complete'
        finally:
            try:
                row['end_ns'] = time.monotonic_ns()
            except BaseException:
                self.omitted = True
            self.stack.pop()


def _runtime_identity(endpoint: Any, image_id: str, *, deadline: float,
                      retain: Any, label: str) -> dict[str, Any]:
    """Exact frozen-v4 identity projection over its original absolute-deadline IO.

    Only the caller-owned deadline differs. Frozen v4 and its retention pins are
    untouched; no new parsing, runtime allowance or synthetic identity is added.
    """
    process.ProcessPolicy(image_id, transport_timeout_seconds=15)
    process._require(bool(process._LABEL.fullmatch(label)), 'Unsafe runtime label')
    version = process._json_control(endpoint, '/version', deadline=deadline, retain=retain, label=label + '-version')
    info = process._json_control(endpoint, '/info', deadline=deadline, retain=retain, label=label + '-info')
    image = process._json_control(endpoint, '/images/' + image_id + '/json', deadline=deadline,
        retain=retain, label=label + '-image')
    def api_tuple(value: Any) -> tuple[int, int]:
        process._require(type(value) is str and bool(re.fullmatch(r'[0-9]+\.[0-9]+', value)),
            'Invalid Engine API version')
        first, second = value.split('.')
        return int(first), int(second)
    process._require(api_tuple(version.get('MinAPIVersion')) <= api_tuple(process.API_VERSION)
        <= api_tuple(version.get('ApiVersion')), 'Pinned API unsupported')
    process._require(version.get('Os') == 'linux' and info.get('OSType') == 'linux'
        and type(info.get('ID')) is str and bool(info['ID'])
        and image.get('Id') == image_id and image.get('Os') == 'linux', 'Runtime or image identity differs')
    process._require(type(version.get('GitCommit')) is str
        and bool(re.fullmatch(r'[0-9a-f]{7,40}', version['GitCommit']))
        and type(info.get('OomKillDisable')) is bool
        and type(info.get('CgroupVersion')) is str and info['CgroupVersion'] in ('1', '2')
        and type(info.get('CgroupDriver')) is str and bool(info['CgroupDriver']),
        'Runtime compatibility capabilities unavailable')
    result = {'protocol': process.PROTOCOL, 'endpoint': asdict(endpoint), 'api_version': process.API_VERSION,
        'os': version['Os'], 'engine_git_commit': version['GitCommit'],
        'cgroup_version': info['CgroupVersion'], 'cgroup_driver': info['CgroupDriver'],
        'oom_kill_disable_supported': info['OomKillDisable'], 'daemon_id': info['ID'],
        'engine_version': version.get('Version'), 'architecture': version.get('Arch'),
        'kernel_version': version.get('KernelVersion'), 'image_id': image_id,
        'image_inspect_sha256': process._sha(process._encoded(image))}
    retain(label + '.json', process._encoded(result))
    return result


def evaluator_sources() -> dict[str, str]:
    result = transport.evaluator_sources()
    result.update(source_capture.evaluator_sources())
    result.update(exposure.definition_sources())
    result.update(profile.definition_sources())
    names = ('candidate_workflow_execution_v1.py', 'candidate_workflow_profile_v1.py',
        'candidate_workflow_observation_v1.py', 'candidate_workflow_review_v1.py',
        'candidate_intake_store_observer_v1.py', 'candidate_storage_prestart_v1.py', 'sandbox.py')
    root = Path(__file__).resolve().parent
    result.update({'gossip_harness/' + name: sha((root / name).read_bytes()) for name in names})
    admission.verify_loaded_sources(result)
    return dict(sorted(result.items()))


def capture_git_source(store: GitStore, commit_oid: str) -> tuple[str, dict[str, bytes]]:
    """The new workflow owner always selects exact reviewed batch60/cleanup5 capture.

    No callback, legacy selection or cached capture crosses an effect boundary.
    SourceCaptureUnavailable remains an infrastructure error for the reader.
    """
    return source_capture.capture_registered_source(store, commit_oid, policy=SOURCE_CAPTURE_POLICY)


@dataclass(frozen=True, slots=True)
class WorkflowPolicy:
    timeout_seconds: int = 30
    image_id: str = b01.RUNTIME_IMAGE
    seed: str = 'fixed-workflow-histories-v1'

    def __post_init__(self) -> None:
        require(type(self.timeout_seconds) is int and self.timeout_seconds == 30, 'Exact workflow call/control deadline required')
        require(self.image_id == b01.RUNTIME_IMAGE, 'Exact retained image required')
        registry.identifier(self.seed)


@dataclass(frozen=True, slots=True)
class WorkflowBinding:
    source_sha256: str
    requirements_sha256: str
    milestone: str
    purpose: str
    family: str
    case_id: str
    native_source_sha256: str
    definition_sha256: str
    profile_sha256: str
    fixture_sha256: str
    review_sha256: str
    review_origin_sha256: str
    evaluator_sha256: str
    runtime_sha256: str
    environment_sha256: str
    limits_sha256: str
    seed_sha256: str
    protocol: str = PROTOCOL

    def __post_init__(self) -> None:
        require(self.protocol == PROTOCOL and self.milestone == 'M4'
            and self.requirements_sha256 == TARGET_CONTRACT, 'Explicit final-M4 contract required')
        require(self.family == FAMILY and self.purpose in registry.PURPOSES, 'Closed workflow family/purpose required')
        for name, value in asdict(self).items():
            if name.endswith('_sha256'):
                registry.sha256(value)
        registry.identifier(self.case_id)


def profile_for_binding(binding: WorkflowBinding) -> profile.WorkflowProfile:
    require(type(binding) is WorkflowBinding, 'Exact workflow binding required')
    value = profile.profile_for(binding.case_id, binding.purpose)
    require(value.sha256 == binding.profile_sha256 and digest(value.record()) == binding.definition_sha256,
            'Workflow profile/definition differs')
    return value


def fixture_identity(case_id: str, plan: review.WorkflowSourcePlan) -> str:
    return digest({'adapter_files': admission.source_manifest(adapter_files(case_id, plan)),
        'input_files': admission.source_manifest(profile.input_files(case_id)), 'recipe': profile.recipe_for(case_id)})


def binding_for(files: dict[str, bytes], value: profile.WorkflowProfile, policy: WorkflowPolicy,
                runtime: dict[str, Any], plan: review.WorkflowSourcePlan, *,
                review_authority: review.WorkflowReviewAuthority) -> WorkflowBinding:
    require(type(value) is profile.WorkflowProfile and profile.accepted_profile(value)
        and type(policy) is WorkflowPolicy and type(plan) is review.WorkflowSourcePlan
        and type(review_authority) is review.WorkflowReviewAuthority,
        'Exact workflow profile/policy/source-plan authority required')
    require(value == profile.reconstruct(value), 'Reconstructed workflow profile differs')
    provenance = review_authority.provenance(plan)
    native = profile.source_sha256(files)
    require(plan.source_sha256 == admission.source_sha256(files) and plan.native_source_sha256 == native
        and plan.case_id == value.case_id and plan.profile_sha256 == value.sha256
        and plan.purpose == value.purpose and plan.profile_kind == 'workflow',
        'Exact source, workflow profile or independent mechanism plan differs')
    # Both immutable supplements are checked against actual complete candidate files.
    exposure.validate_source(files)
    return WorkflowBinding(admission.source_sha256(files), TARGET_CONTRACT, 'M4', value.purpose,
        FAMILY, value.case_id, native, digest(value.record()), value.sha256, fixture_identity(value.case_id, plan),
        review_authority.enrollment.report_sha256, digest(provenance), digest(evaluator_sources()), digest(runtime),
        digest({'environment': DockerValidator._environment(),
            'host_python': [platform.python_implementation(), platform.python_version()],
            'snapshot_protocol': b01.SNAPSHOT_PROTOCOL, 'volume_options': b01.VOLUME_OPTIONS}),
        digest({'policy': asdict(policy), 'deadline_policy': deadline_policy(), 'source_capture_policy': SOURCE_CAPTURE_POLICY.record(),
            'journal': asdict(LIMITS), 'chunk_bytes': CHUNK_BYTES, 'prestart_policy': prestart.definition(),
            'capture_bytes': b02.MAX_CAPTURE_BYTES, 'wire': wire_definition(),
            'call_count': len(value.phases), 'cleanup': asdict(cleanup.CleanupLimits())}), digest({'seed': policy.seed}))


def mechanics_case_id(value: profile.WorkflowProfile) -> str:
    return FAMILY + ':' + value.case_id + ':mechanics'


def gate_for(subject: registry.Subject, binding: WorkflowBinding, *, gate_id: str) -> registry.Gate:
    require(type(binding) is WorkflowBinding, 'Exact workflow binding required')
    value = profile_for_binding(binding)
    require(binding.profile_sha256 == value.sha256 and subject.source_sha256 == binding.source_sha256
        and subject.milestone == 'M4' and subject.requirements_sha256 == TARGET_CONTRACT,
        'Prospective subject/profile differs')
    roster = value.ordered_case_ids + (mechanics_case_id(value),)
    return registry.Gate(gate_id, value.requirement_ids, roster,
        registry.Binding(subject, digest({'profile': value.record(), 'ordered_cases': roster}),
            binding.evaluator_sha256, binding.runtime_sha256, binding.environment_sha256,
            binding.limits_sha256, binding.seed_sha256, binding.protocol, binding.purpose))


@dataclass(frozen=True, slots=True)
class WorkflowRegistration:
    binding: WorkflowBinding
    commit_oid: str
    tree_oid: str
    repetition_id: str
    gate: registry.Gate
    cohort_trajectory_ids: tuple[str, ...]


def observation_registration(registration: WorkflowRegistration) -> admission.ObservationRegistration:
    require(type(registration) is WorkflowRegistration, 'Exact workflow registration required')
    binding = registration.binding
    require(registration.gate == gate_for(registration.gate.binding.subject, binding,
            gate_id=registration.gate.gate_id), 'Registered workflow gate differs')
    return admission.ObservationRegistration(registration.gate, registration.commit_oid,
        registration.tree_oid, registration.repetition_id, registration.cohort_trajectory_ids,
        binding.definition_sha256, binding.profile_sha256, profile_for_binding(binding).original_definition_purpose,
        admission.binding_sha256(binding, gate=registration.gate))


FRAME_BYTES = 131072
STDOUT_BYTES = 524288
STDERR_BYTES = 65536
NORMALIZED_BYTES = 61824
HISTORY_TIMEOUT_SECONDS = 300
MAX_EVENTS = 256


def wire_definition() -> dict[str, Any]:
    return {'protocol': ADAPTER_PROTOCOL, 'frame_payload_bytes': FRAME_BYTES,
        'stdout_bytes': STDOUT_BYTES, 'stderr_bytes': STDERR_BYTES, 'normalized_expected_support': NORMALIZED_BYTES,
        'call_timeout_seconds': 30, 'history_timeout_seconds': HISTORY_TIMEOUT_SECONDS, 'max_calls': 3,
        'max_boundary_events_per_call': MAX_EVENTS, 'delimiter': 'single LF outside payload bound',
        'deadline_policy': deadline_policy(),
        'capture': 'fresh source-qualified paused complete /tmp archive; trace values alone are not authority'}


# Inputs contain no expected answers. Source-qualified trace callbacks observe
# locals without invoking properties, changing returns, repairing state or
# catching/replacing candidate exceptions. A host acknowledgement gates progress.
ADAPTER = r'''import contextlib
import json
import os
from pathlib import Path
import sys

wire = sys.stdout
commands = sys.stdin
sys.stdout = sys.stderr
sys.path.insert(0, '/workspace')
from solution import solve
with open('/inputs/workflow-input.json', 'rb') as stream:
    recipe = json.load(stream)
with open('/checks/workflow-plan.json', 'rb') as stream:
    plan = json.load(stream)

def emit(value):
    wire.write(json.dumps(value, ensure_ascii=True, separators=(',', ':'), allow_nan=False) + '\n')
    wire.flush()

def acknowledge(text):
    if commands.readline() != text + '\n':
        raise RuntimeError('Closed workflow handshake differs')

def location(value):
    if type(value) is not str and type(value) not in (Path, type(Path('/tmp'))):
        return None
    text = str(value)
    if not text.startswith('/tmp/') or '\\' in text or '\x00' in text or any(x in ('', '.', '..') for x in text.split('/')[1:]):
        return None
    return text

emit({'kind': 'ready', 'protocol': 'candidate-workflow-wire-v1'})
acknowledge('ready')
for index, payload in enumerate(recipe['calls']):
    phase = 'call-%03d' % index
    acknowledge(phase)
    occurrences = {}
    count = [0]
    carried = {'root': None, 'database': None}
    origins = {'root': None, 'database': None}
    def trace(frame, event, arg):
        for boundary in plan['boundaries']:
            if (event == boundary['event'] and frame.f_code.co_filename == '/workspace/' + boundary['source_path']
                and frame.f_code.co_name == boundary['function'] and frame.f_lineno == boundary['line']):
                occurrence = occurrences.get(boundary['id'], 0)
                if occurrence >= boundary['occurrences']:
                    continue
                occurrences[boundary['id']] = occurrence + 1
                ordinal = count[0]
                count[0] += 1
                if count[0] > 256:
                    raise RuntimeError('Workflow boundary observation limit')
                for kind in ('root', 'database'):
                    if boundary[kind + '_local'] is not None:
                        carried[kind] = location(frame.f_locals.get(boundary[kind + '_local']))
                        origins[kind] = ordinal if carried[kind] is not None else None
                paths = dict(carried)
                emit({'kind': 'boundary', 'phase': phase, 'ordinal': ordinal,
                    'boundary': boundary['id'], 'occurrence': occurrence, 'paths': paths, 'path_origins': dict(origins)})
                acknowledge('resume:' + phase + ':' + str(ordinal))
        return trace
    sys.settrace(trace)
    try:
        answer = solve(payload)
    finally:
        sys.settrace(None)
    emit({'kind': 'result', 'phase': phase, 'value': answer})
    acknowledge('next:' + phase)
acknowledge('finish')
'''

PATH_PROBE = r'''import json
import os
import stat
import sys

def inspect(path):
    if path is None:
        return None
    if type(path) is not str or not path.startswith('/tmp/') or '\\' in path or '\x00' in path:
        raise ValueError('Unconfined source-qualified path')
    parts = path.split('/')[1:]
    if any(x in ('', '.', '..') for x in parts):
        raise ValueError('Noncanonical source-qualified path')
    fd = os.open('/', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for part in parts[:-1]:
            nxt = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = nxt
        try:
            value = os.stat(parts[-1], dir_fd=fd, follow_symlinks=False)
        except FileNotFoundError:
            return {'path': path, 'exists': False}
        if not (stat.S_ISREG(value.st_mode) or stat.S_ISDIR(value.st_mode)):
            raise ValueError('Unsupported source-qualified path type')
        return {'path': path, 'exists': True, 'device': value.st_dev, 'inode': value.st_ino,
            'mode': value.st_mode, 'bytes': value.st_size, 'mtime_ns': value.st_mtime_ns}
    finally:
        os.close(fd)
paths = json.loads(sys.argv[1])
if type(paths) is not dict or set(paths) != {'root', 'database'}:
    raise ValueError('Closed path request required')
print(json.dumps({key: inspect(value) for key, value in paths.items()}, sort_keys=True, separators=(',', ':')))
'''


def adapter_files(case_id: str, plan: review.WorkflowSourcePlan) -> dict[str, bytes]:
    require(type(plan) is review.WorkflowSourcePlan and plan.case_id == case_id,
        'Exact case-specific source plan required')
    return {'workflow_adapter.py': ADAPTER.encode(), 'workflow_paths.py': PATH_PROBE.encode(),
        'workflow-plan.json': encoded(asdict(plan))}


def stage_inputs(root: Path, files: dict[str, bytes]) -> None:
    require(type(files) is dict and set(files) == {'workflow-input.json'}
        and all(type(raw) is bytes for raw in files.values()), 'Closed workflow input files required')
    for name, raw in files.items():
        (root / name).write_bytes(raw)
        (root / name).chmod(0o444)
    verify_inputs(root, files)


def verify_inputs(root: Path, files: dict[str, bytes]) -> list[dict[str, Any]]:
    _verify_regular_tree(root, files)
    return admission.source_manifest(files)


def exec_identity(value: Any, *, container_id: str, exec_id: str, pid: int | None,
                  completed: bool = False) -> int | None:
    require(type(value) is dict and value.get('ID') == exec_id and value.get('ContainerID') == container_id,
            'Exact Engine exec/container identity unavailable')
    cfg = value.get('ProcessConfig')
    require(type(cfg) is dict and cfg.get('entrypoint') == 'python'
        and cfg.get('arguments') == ['-I', '-B', '/checks/workflow_adapter.py']
        and cfg.get('user') == '65534:65534' and cfg.get('privileged') is False
        and cfg.get('tty') is False and value.get('OpenStdin') is True,
        'Exact unprivileged workflow exec command unavailable')
    if completed:
        require(value.get('Running') is False and type(value.get('ExitCode')) is int,
            'Natural Engine exec completion unavailable')
        return pid
    actual = value.get('Pid')
    require(value.get('Running') is True and type(actual) is int and actual > 0
        and (pid is None or actual == pid), 'Same live solve process unavailable')
    return actual


class _Commands(transport._Commands):
    owner: CandidateWorkflowExecution
    _control_deadline: float | None = None

    def _before_spawn(self) -> None:
        self.owner._check_deadline()
        require(self._control_deadline is not None and time.monotonic() < self._control_deadline,
            'Declared workflow control deadline reached')

    def _wait_timeout(self) -> float:
        require(self._control_deadline is not None, 'Missing control deadline')
        assert self._control_deadline is not None
        remaining = min(self._control_deadline, self.owner._operation_deadline(self.owner.policy.timeout_seconds)) - time.monotonic()
        if remaining <= 0:
            # Enter the inherited timeout/kill/retention path, not an early escape
            # that drops already-started child streams or their cleanup record.
            raise subprocess.TimeoutExpired('workflow owned control', 0)
        return remaining

    def run(self, label: str, arguments: list[str], limit: int = b01.MAX_STREAM_BYTES) -> dict[str, Any]:
        require(self.owner.mode == 'physical', 'Fixture owners never dispatch Docker commands')
        require(self._control_deadline is None, 'Control processes cannot nest')
        self._control_deadline = self.owner._operation_deadline(self.owner.policy.timeout_seconds)
        try:
            with self.owner._timings.span('control'):
                record = super().run(label, arguments, limit)
                self.owner._check_deadline()
                assert self._control_deadline is not None
                require(time.monotonic() < self._control_deadline, 'Declared workflow control deadline reached')
                return record
        finally:
            self._control_deadline = None


class _Session:
    """Owned finite call protocol, bounded independent drains, no B02 grammar."""
    def __init__(self, owner: CandidateWorkflowExecution, commands: _Commands, argv: list[str]):
        require(owner.mode == 'physical', 'Fixture owners never start candidate sessions')
        self.owner, self.commands, self.finished = owner, commands, False
        self.requests: list[str] = []
        self.lines: queue.Queue[bytes | None] = queue.Queue(maxsize=MAX_EVENTS + 8)
        self.streams = {'stdout': bytearray(), 'stderr': bytearray()}
        self.counts = {'stdout': 0, 'stderr': 0}
        self.errors: set[str] = set()
        self.threads: list[threading.Thread] = []
        self.arguments = owner.docker + argv[1:]
        owner._effect_boundary()
        owner._retain('session-dispatch.json', encoded({'argv': self.arguments}))
        owner.checkpoint()
        owner._effect_boundary()
        owner._check_deadline()
        self.process = subprocess.Popen(self.arguments, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, env=DockerValidator._environment())
        try:
            assert self.process.stdin is not None
            os.set_blocking(self.process.stdin.fileno(), False)
            for kind in self.streams:
                thread = threading.Thread(target=self._drain, args=(kind,), daemon=True)
                self.threads.append(thread)
                thread.start()
        except BaseException:
            try:
                self.stop_local()
            except BaseException:
                pass
            raise

    def _drain(self, kind: str) -> None:
        pipe = getattr(self.process, kind)
        limit = STDOUT_BYTES if kind == 'stdout' else STDERR_BYTES
        pending = bytearray()
        oversized = False
        try:
            while chunk := pipe.read1(65536):
                self.counts[kind] += len(chunk)
                self.streams[kind].extend(chunk[:max(0, limit - len(self.streams[kind]))])
                if self.counts[kind] > limit:
                    self.errors.add(kind + '-limit')
                if kind != 'stdout':
                    continue
                for piece in chunk.splitlines(keepends=True):
                    # Only LF is protocol framing; splitlines also recognizes CR,
                    # so pieces without LF are joined unchanged.
                    if not oversized:
                        pending.extend(piece)
                        if len(pending) > FRAME_BYTES + (1 if pending.endswith(b'\n') else 0):
                            oversized = True
                            self.errors.add('frame-limit')
                    if piece.endswith(b'\n'):
                        if not oversized:
                            try:
                                self.lines.put_nowait(bytes(pending))
                            except queue.Full:
                                self.errors.add('frame-queue-limit')
                        pending.clear()
                        oversized = False
        except (OSError, ValueError):
            self.errors.add(kind + '-read')
        finally:
            if kind == 'stdout':
                if pending or oversized:
                    self.errors.add('truncated-frame')
                try:
                    self.lines.put_nowait(None)
                except queue.Full:
                    self.errors.add('frame-queue-limit')
            pipe.close()

    def _remaining(self, deadline: float) -> float:
        final = min(deadline, self.owner._operation_deadline(self.owner.policy.timeout_seconds))
        remaining = final - time.monotonic()
        if remaining <= 0:
            raise WorkflowDeadlineExceeded('Declared workflow observation deadline reached')
        return remaining

    def _line(self, deadline: float) -> bytes:
        with self.owner._timings.span('queue'):
            try:
                raw = self.lines.get(timeout=self._remaining(deadline))
            except queue.Empty as error:
                raise ExecutionUnknown('Workflow response timeout') from error
            # Drain threads retain even late bytes; dequeue is not qualification.
            self._remaining(deadline)
            require(raw is not None and raw.endswith(b'\n') and len(raw) - 1 <= FRAME_BYTES,
                'Complete bounded workflow frame unavailable')
            assert raw is not None
            return raw

    def _write(self, value: str, label: str) -> None:
        require(not self.owner._cleanup_phase, 'Cleanup cannot send workflow application requests')
        with self.owner._timings.span('write'):
            self.owner._effect_boundary()
            self.owner._retain(label, encoded({'request': value + '\n'}))
            self.owner.checkpoint()
            self.owner._effect_boundary()
            assert self.process.stdin is not None
            raw = (value + '\n').encode('ascii')
            fd = self.process.stdin.fileno()
            deadline = self.owner._operation_deadline(self.owner.policy.timeout_seconds)
            offset = 0
            while offset < len(raw):
                remaining = self._remaining(deadline)
                _, writable, _ = select.select([], [fd], [], remaining)
                self._remaining(deadline)
                require(bool(writable), 'Workflow request pipe deadline reached')
                self.owner._check_deadline()
                try:
                    count = os.write(fd, raw[offset:])
                except BlockingIOError:
                    continue
                require(count > 0, 'Workflow request pipe closed')
                offset += count
            self._remaining(deadline)

    def ready(self, capture: Any) -> None:
        with self.owner._call_window('session-ready') as deadline:
            raw = self._line(deadline)
            self.owner._retain('session-ready.bin', raw)
            self.owner.checkpoint()
            require(profile.decode(raw) == {'kind': 'ready', 'protocol': ADAPTER_PROTOCOL}, 'Closed workflow ready differs')
            capture({'kind': 'ready'})
            self._write('ready', 'session-ready-ack.json')

    def phase(self, phase: str, capture: Any) -> None:
        require(len(self.requests) < len(self.owner.profile.phases)
            and phase == self.owner.profile.phases[len(self.requests)], 'Workflow call replay/reordering refused')
        with self.owner._call_window(phase) as deadline:
            self._write(phase, phase + '-request.json')
            self.requests.append(phase)
            seen: dict[str, int] = {}
            for index in range(MAX_EVENTS + 1):
                raw = self._line(deadline)
                self.owner._retain(phase + '-frame-%03d.bin' % index, raw)
                self.owner.checkpoint()
                self.owner._effect_boundary()
                value = profile.decode(raw)
                require(type(value) is dict and value.get('phase') == phase, 'Unattributable workflow frame')
                if value.get('kind') == 'result':
                    require(set(value) == {'kind', 'phase', 'value'}, 'Closed result frame differs')
                    self.owner._retain(phase + '-response.bin', raw)
                    self.owner.checkpoint()
                    capture(phase + '-result', value)
                    self.owner._eligible(phase + '-completion', (phase + '-result-response-eligible.json',
                        phase + '-result-capture-eligible.json', phase + '-response.bin'))
                    self._write('next:' + phase, phase + '-next.json')
                    return
                require(value.get('kind') == 'boundary' and set(value) == {'kind', 'phase', 'ordinal', 'boundary', 'occurrence', 'paths', 'path_origins'}
                    and type(value['ordinal']) is int and value['ordinal'] == index, 'Closed boundary frame differs')
                boundaries = [item for item in self.owner.plan.boundaries if item.id == value['boundary']]
                require(len(boundaries) == 1, 'Undeclared workflow boundary')
                boundary = boundaries[0]
                occurrence = seen.get(boundary.id, 0)
                require(type(value['occurrence']) is int and value['occurrence'] == occurrence
                    and occurrence < boundary.occurrences, 'Boundary occurrence differs')
                require(type(value['paths']) is dict and set(value['paths']) == {'root', 'database'}, 'Boundary path inventory differs')
                for path in value['paths'].values():
                    require(path is None or (type(path) is str and path.startswith('/tmp/') and '\\' not in path
                        and '\x00' not in path and all(part not in ('', '.', '..') for part in path.split('/')[1:])),
                        'Unconfined boundary path')
                seen[boundary.id] = occurrence + 1
                capture(phase + '-boundary-%03d' % index, value)
                self._write('resume:' + phase + ':' + str(index), phase + '-resume-%03d.json' % index)
            raise ExecutionUnknown('Boundary event limit')

    def finish(self, success: bool, *, send_finish: bool = True) -> bool:
        require(not self.finished, 'Session finish is one-shot')
        self.finished = True
        timed_out = False
        natural = False
        primary: BaseException | None = None
        try:
            if success:
                window = (self.owner._call_window('session-finish') if self.owner._active_call_deadline is None
                    else nullcontext(self.owner._active_call_deadline))
                with window as deadline:
                    if send_finish:
                        self._write('finish', 'session-finish-request.json')
                    assert self.process.stdin is not None
                    self.process.stdin.close()
                    try:
                        self.process.wait(timeout=self._remaining(deadline))
                        natural = True
                        self._remaining(deadline)
                    except subprocess.TimeoutExpired:
                        timed_out = True
            else:
                timed_out = True
        except BaseException as error:
            primary = error
            if isinstance(error, WorkflowDeadlineExceeded):
                timed_out = True
        finally:
            try:
                self.stop_local()
            except BaseException as error:
                self.errors.add('teardown:' + type(error).__name__)
                if primary is None:
                    primary = error
        record: dict[str, Any] = {'argv': self.arguments, 'exit_code': self.process.returncode,
            'natural_exit': natural, 'timed_out': timed_out,
            'capture_complete': all(not t.is_alive() for t in self.threads) and not self.errors,
            'errors': sorted(self.errors), 'requests': list(self.requests)}
        for kind, data in self.streams.items():
            raw = bytes(data)
            self.owner._retain_blob('session-' + kind + '.bin', raw)
            record[kind] = {'path': 'session-' + kind + '.bin', 'bytes': len(raw),
                'observed_bytes': self.counts[kind], 'sha256': sha(raw), 'truncated': self.counts[kind] > len(raw)}
        self.owner._retain('session.json', encoded(record))
        self.owner.checkpoint()
        if primary is not None:
            raise primary
        return bool(natural and self.process.returncode == 0 and record['capture_complete'])

    def stop_local(self) -> None:
        with self.owner._timings.span('teardown'):
            transport._stop_local_process(self.process, self.threads)


class CandidateWorkflowExecution(transport.CandidateStorageExecution):
    """Separate exact workflow owner, sharing only neutral bounded journal mechanics.

    The inherited owner/thread checks, raw retention, chunk validation, external
    checkpoints, effect boundary, runtime revalidation and close methods do not
    select cases or authenticate a storage-family owner. All source, review,
    binding, profile, admission, configuration and dispatch routes are workflow-owned.
    """
    # The reused base defines storage-family attributes. These slots are
    # deliberately rebound by this separate exact-type constructor; inherited
    # neutral mechanics do not interpret those family-specific values.
    policy: Any
    registration: Any
    plan: Any
    review_authority: Any
    binding: Any

    def __init__(self, root: Path, store: GitStore, registration: WorkflowRegistration, policy: WorkflowPolicy, *,
                 value: profile.WorkflowProfile, plan: review.WorkflowSourcePlan, review_authority: review.WorkflowReviewAuthority,
                 admission_authority: admission.ObservationAdmission, checkpoint_authority: chain.HeadAuthority,
                 delta_root: Path, cleanup_root: Path, endpoint: Any = None, mode: str = 'physical',
                 expected_checkpoint: chain.PrefixCommitment | None = None):
        require(type(registration) is WorkflowRegistration and profile.accepted_profile(value), 'Exact workflow registration/profile required')
        require(mode in ('physical', 'fixture') and type(policy) is WorkflowPolicy, 'Exact owner mode/policy required')
        require(type(review_authority) is review.WorkflowReviewAuthority
            and type(admission_authority) is admission.ObservationAdmission, 'Actual review and prospective admission required')
        self.root, self.delta_root, self.cleanup_root = (Path(p).absolute() for p in (root, delta_root, cleanup_root))
        require(all(p.resolve() == p for p in (self.root, self.delta_root, self.cleanup_root)), 'Canonical roots required')
        require(all(not a.is_relative_to(b) and not b.is_relative_to(a) for a, b in
            ((self.root, self.cleanup_root), (self.delta_root, self.cleanup_root))), 'Separate cleanup roots required')
        require(mode != 'physical' or type(checkpoint_authority) is ExternalHead, 'Physical owner needs external head')
        if type(checkpoint_authority) is ExternalHead:
            require(checkpoint_authority.journal_roots == (self.root, self.delta_root), 'Different external journal roots')
            require(not self.cleanup_root.is_relative_to(checkpoint_authority.root)
                and not checkpoint_authority.root.is_relative_to(self.cleanup_root), 'Head/cleanup overlap')
        self.mode, self.policy, self.store = mode, policy, store
        self.registration, self.profile, self.plan = registration, value, plan
        self.review_authority, self.admission, self.checkpoint_authority = review_authority, admission_authority, checkpoint_authority
        self._pid, self._thread, self.closed = os.getpid(), threading.get_ident(), False
        self.journal: journals.OwnerJournal | None = None
        self._cleanup: cleanup.CleanupChannel | None = None
        self._cleanup_phase = False
        self.cleanup_result: Any = None
        self._session: _Session | None = None
        self._exec_id: str | None = None
        self._exec_pid: int | None = None
        self._deadline: float | None = None
        self._active_call_deadline: float | None = None
        self._active_call: str | None = None
        self._timings = _Timings()
        self.diagnostic_path: str | None = None
        self.diagnostic_error: str | None = None
        self.close_errors: list[str] = []
        self._freeze: registry.CohortFreeze | None = None
        self.endpoint: Any = (endpoint or process.EngineEndpoint.from_environment()) if mode == 'physical' else None
        self.docker = ['docker'] if self.endpoint is None else ['docker', '--host', 'unix://' + self.endpoint.socket_path]
        try:
            if mode == 'physical':
                assert self.endpoint is not None
                self.runtime = process.runtime_identity(self.endpoint, policy.image_id)
            else:
                self.runtime = {'kind': 'fixture-no-Docker'}
            self.sources = evaluator_sources()
            self.tree, self.files = capture_git_source(store, registration.commit_oid)
            require(self.tree == registration.tree_oid and plan.commit_oid == registration.commit_oid
                and plan.tree_oid == self.tree, 'Complete final Git source differs')
            self.review_sha256 = review_authority.authenticate(plan)
            self.binding = binding_for(self.files, value, policy, self.runtime, plan, review_authority=review_authority)
            require(self.binding == registration.binding, 'Complete registered workflow binding differs')
            self.observation_registration = observation_registration(registration)
            require(self.admission.registration == self.observation_registration, 'Admission belongs to another workflow execution')
            self.config = {'protocol': self.binding.protocol, 'mode': mode, 'root': str(self.root), 'delta_root': str(self.delta_root),
                'cleanup_root': str(self.cleanup_root), 'repository': str(store.path.resolve()), 'registration': asdict(registration),
                'profile': value.record(), 'plan': asdict(plan), 'review_sha256': self.review_sha256,
                'review_provenance': review_authority.provenance(plan),
                'source_manifest': admission.source_manifest(self.files), 'sources': self.sources,
                'runtime': self.runtime, 'policy': asdict(policy), 'source_capture_policy': SOURCE_CAPTURE_POLICY.record(),
                'deadline_policy': deadline_policy(),
                'journal_limits': asdict(LIMITS), 'prestart_policy': prestart.definition(),
                'endpoint': None if self.endpoint is None else asdict(self.endpoint)}
            context = {'protocol': self.binding.protocol, 'config_sha256': digest(self.config),
                'source_sha256': self.binding.source_sha256, 'purpose': self.binding.purpose,
                'original_definition_purpose': self.observation_registration.original_definition_purpose,
                'original_binding_sha256': digest(asdict(self.binding))}
            existed = self.root.exists()
            require(existed == (expected_checkpoint is not None), 'Reopen requires independently supplied exact prefix')
            self.journal = journals.OwnerJournal(self.root, self.delta_root, context=context,
                authority=checkpoint_authority, expected=expected_checkpoint, limits=LIMITS)
            if existed:
                require(self.read_authenticated('config.json') == encoded(self.config), 'Authenticated config differs')
            else:
                self._retain('config.json', encoded(self.config))
            self.checkpoint()
        except BaseException:
            try:
                self.close()
            except BaseException:
                pass
            raise

    def current(self, freeze: registry.CohortFreeze | None = None) -> None:
        with self._timings.span('current'):
            self._current(freeze)

    def _current(self, freeze: registry.CohortFreeze | None = None) -> None:
        self._owner()
        require(self.profile == profile_for_binding(self.binding)
            and type(self.plan) is review.WorkflowSourcePlan, 'Original workflow mapping profile differs')
        require(evaluator_sources() == self.sources, 'workflow evaluator changed')
        with self._timings.span('source'):
            tree, files = capture_git_source(self.store, self.registration.commit_oid)
        require(tree == self.tree and files == self.files, 'Final Git source changed')
        require(self.review_authority.authenticate(self.plan) == self.review_sha256
            and digest(self.review_authority.provenance(self.plan)) == self.binding.review_origin_sha256, 'Layout authority changed')
        self.admission.check_current(self.observation_registration, freeze)

    def execute_once(self) -> dict[str, Any]:
        self._owner()
        self.checkpoint()
        require(not self.has_retained('intent.json'), 'Existing intent forbids redispatch, including incomplete execution')
        require(self.mode == 'physical', 'Fixture owners never dispatch candidate code')
        self._freeze = self.admission.before_intent(self.observation_registration)
        self.current(self._freeze)
        execution_id = 'workflow-' + uuid.uuid4().hex
        intent = {'protocol': self.binding.protocol, 'execution_id': execution_id,
            'source_sha256': self.binding.source_sha256,
            'original_definition_purpose': self.observation_registration.original_definition_purpose,
            'original_binding': asdict(self.binding),
            'registration': asdict(self.observation_registration),
            'cohort_freeze': None if self._freeze is None else asdict(self._freeze),
            'container': 'gossip-' + execution_id, 'volume': 'gossip-volume-' + execution_id,
            'snapshot_protocol': b01.SNAPSHOT_PROTOCOL, 'ordered_phases': list(self.profile.phases)}
        self._retain('intent.json', encoded(intent))
        self.checkpoint()
        self.current(self._freeze)
        self._deadline = time.monotonic() + HISTORY_TIMEOUT_SECONDS
        try:
            self._dispatch(intent)
            self.current(self._freeze)
            self.checkpoint()
            return profile.decode(self.read_authenticated('terminal.json'))
        finally:
            self._flush_timings(intent)

    def _dispatch(self, intent: dict[str, Any]) -> None:
        require(self.mode == 'physical', 'Fixture owners never dispatch candidate code')
        commands = _Commands(self)
        case = self.binding.case_id
        recipe = self._recipe()
        fixtures = self._inputs()
        self._retain('recipe.json', encoded(recipe))
        name, volume = intent['container'], intent['volume']
        errors: list[str] = []
        session: _Session | None = None
        container_id: str | None = None
        claims: dict[str, str] = {}
        removed = {'container': False, 'volume': False}
        attempted = {'container': False, 'volume': False}
        labels = {'gossip.execution': intent['execution_id'], 'gossip.source': self.binding.source_sha256,
                  'gossip.fixture': self.binding.fixture_sha256}
        sandbox = DockerValidator(self.policy.image_id, {'workflow_adapter.py': ADAPTER}, command=('python', '-I', '-c', 'import time;time.sleep(1800)'))

        def checked(label: str, argv: list[str], limit: int = b01.MAX_STREAM_BYTES) -> dict[str, Any]:
            record = commands.run(label, argv, limit)
            require(b01._clean(record), label + ': control process incomplete or failed')
            return record

        def parsed(record: dict[str, Any]) -> Any:
            return process.strict_json_loads(commands.raw(record))

        def ordinary_cleanup() -> None:
            self._cleanup_phase = True
            assert self._cleanup is not None
            if container_id is not None:
                self._cleanup.note_normal_removal(claims['container'])
                checked('container-remove', ['docker', 'rm', '--force', container_id])
                absent = checked('container-after', ['docker', 'container', 'ls', '--all', '--quiet', '--filter', 'name=^/' + name + '$'])
                removed['container'] = not commands.raw(absent).strip()
                require(removed['container'], 'Owned container absence unproven')
                self._cleanup.confirm_normal_removal(claims['container'], remove_record='container-remove.json', absence_record='container-after.json')
            elif not attempted['container']:
                removed['container'] = True
            # A missing creation response never authorizes guessed removal.
            require(removed['container'], 'Container creation outcome unknown; use prior-only cleanup')
            if attempted['volume'] and 'volume' in claims:
                inspected = checked('volume-cleanup-inspect', ['docker', 'volume', 'inspect', '--format', '{{json .}}', volume])
                require(b01._volume_valid(parsed(inspected), volume, intent['execution_id']), 'Owned volume identity differs')
                self._cleanup.note_normal_removal(claims['volume'])
                checked('volume-remove', ['docker', 'volume', 'rm', volume])
                absent = checked('volume-after', ['docker', 'volume', 'ls', '--quiet', '--filter', 'name=^' + volume + '$'])
                removed['volume'] = not commands.raw(absent).strip()
                require(removed['volume'], 'Owned volume absence unproven')
                self._cleanup.confirm_normal_removal(claims['volume'], remove_record='volume-remove.json', absence_record='volume-after.json')
            elif not attempted['volume']:
                removed['volume'] = True

        primary: BaseException | None = None
        with tempfile.TemporaryDirectory(prefix='gossip-m2-product-') as temp:
            staging = Path(temp).resolve()
            workspace, checks, inputs = (staging / name for name in ('source', 'checks', 'inputs'))
            require(type(self.checkpoint_authority) is ExternalHead, 'Physical execution external head unavailable')
            assert isinstance(self.checkpoint_authority, ExternalHead)
            authority_roots = (self.root, self.delta_root, self.cleanup_root, self.checkpoint_authority.root)
            require(all(not mounted.is_relative_to(trusted) and not trusted.is_relative_to(mounted)
                for mounted in (workspace, checks, inputs) for trusted in authority_roots), 'Candidate/authority roots overlap')
            for directory in (workspace, checks, inputs):
                directory.mkdir(mode=0o755)
            for path, raw in self.files.items():
                dest = workspace / path
                dest.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
                dest.write_bytes(raw)
                dest.chmod(0o444)
            adapter = 'workflow_adapter.py'
            expected_checks = self._helpers()
            require(digest({'adapter_files': admission.source_manifest(expected_checks),
                'input_files': admission.source_manifest(fixtures), 'recipe': recipe}) == self.binding.fixture_sha256,
                'Original registered workflow helper and fixture bytes differ before dispatch')
            for helper_name, helper_raw in expected_checks.items():
                (checks / helper_name).write_bytes(helper_raw)
            stage_inputs(inputs, fixtures)
            self._retain('fixture-manifest.json', encoded(verify_inputs(inputs, fixtures)))
            for check_path in checks.iterdir():
                check_path.chmod(0o444)
            def verify_staging() -> dict[str, Any]:
                _verify_regular_tree(workspace, self.files)
                _verify_regular_tree(checks, expected_checks)
                verify_inputs(inputs, fixtures)
                return {'source_manifest': admission.source_manifest(self.files),
                    'helper_manifest': admission.source_manifest(expected_checks),
                    'fixtures_sha256': digest(admission.source_manifest(fixtures))}
            staging_proof = verify_staging()
            plan = {'workspace': str(workspace), 'checks': str(checks), 'inputs': str(inputs),
                    'source_manifest': admission.source_manifest(self.files), 'adapter_sha256': sha(ADAPTER.encode()),
                    'proof': staging_proof}
            self._retain('staging.json', encoded(plan))
            assert self.journal is not None
            self._cleanup = cleanup.CleanupChannel.create(self.cleanup_root, journal=self.journal, endpoint=self.endpoint,
                runtime=self.runtime, source_sha256=self.binding.source_sha256, fixture_sha256=labels['gossip.fixture'],
                execution_id=intent['execution_id'], image_id=self.policy.image_id,
                candidate_mount_roots=(workspace, checks, inputs),
                journal_roots=(self.root, self.delta_root, self.checkpoint_authority.root))
            try:
                self._effect_boundary()
                observed_runtime = self._runtime('runtime')
                require(observed_runtime == self.runtime, 'Runtime changed before dispatch')
                for kind, args in (('volume', ['docker', 'volume', 'ls', '--quiet', '--filter', 'name=^' + volume + '$']),
                    ('container', ['docker', 'container', 'ls', '--all', '--quiet', '--filter', 'name=^/' + name + '$'])):
                    require(not commands.raw(checked(kind + '-before', args)).strip(), 'Owned name already exists: ' + kind)
                claims['volume'] = self._cleanup.claim_volume(name=volume,
                    labels={'gossip.execution': intent['execution_id'], 'gossip.snapshot': b01.SNAPSHOT_PROTOCOL},
                    options=b01.VOLUME_OPTIONS, preabsence_record='volume-before.json')
                attempted['volume'] = True
                argv = ['docker', 'volume', 'create', '--driver', 'local', '--label', 'gossip.execution=' + intent['execution_id'],
                    '--label', 'gossip.snapshot=' + b01.SNAPSHOT_PROTOCOL]
                for key, val in b01.VOLUME_OPTIONS.items():
                    argv.extend(('--opt', key + '=' + val))
                argv.append(volume)
                require(commands.raw(checked('volume-create', argv)).strip() == volume.encode(), 'Created volume differs')
                record = checked('volume-created', ['docker', 'volume', 'inspect', '--format', '{{json .}}', volume])
                require(b01._volume_valid(parsed(record), volume, intent['execution_id']), 'Volume ownership differs')
                self._cleanup.confirm_volume(claims['volume'], inspection_record='volume-created.json')
                argv = b02._start_arguments(sandbox, name, workspace, checks, inputs, volume)
                argv[1] = 'create'
                argv.remove('--detach')
                index = argv.index('--entrypoint')
                for key, val in labels.items():
                    argv[index:index] = ['--label', key + '=' + val]
                    index += 2
                claims['container'] = self._cleanup.claim_container(name=name, labels=labels,
                    argv=sandbox.command, preabsence_record='container-before.json')
                attempted['container'] = True
                container_id = commands.raw(checked('container-create', argv)).strip().decode('ascii')
                registry.sha256(container_id)
                self._cleanup.confirm_container(claims['container'], create_record='container-create.json')
                record = checked('container-prestart',
                    ['docker', 'inspect', '--format', '{{json .}}', container_id], process.CONTROL_LIMIT)
                binds = {'/workspace': str(workspace), '/checks': str(checks), '/inputs': str(inputs)}
                proof = prestart.proof_for(commands.raw(record), container_id=container_id, name=name,
                    image_id=self.policy.image_id, volume=volume, labels=labels, mounts=binds,
                    runtime=self.runtime, runtime_originals={name: self.read_authenticated(name)
                        for name in prestart.RUNTIME_ORIGINAL_NAMES if self.has_retained(name)})
                self._retain(prestart.PROOF_FILE, encoded(proof))
                self.checkpoint()
                checked('container-start', ['docker', 'start', container_id])
                session_argv = ['docker', 'exec', '--interactive', '--user', '65534:65534', container_id,
                    'python', '-I', '-B', '/checks/' + adapter]
                session = _Session(self, commands, session_argv)
                self._session = session
                def capture(label: str, event: dict[str, Any]) -> None:
                    self._phase_runtime(label + '-runtime-before')
                    self._retain(label + '-staging-before.json', encoded(verify_staging()))
                    try:
                        self._exec_identity(commands, container_id, label)
                        if event.get('kind') == 'boundary':
                            self._retain(label + '-event.json', encoded(event))
                            probe = checked(label + '-paths', ['docker', 'exec', '--user', '65534:65534', container_id,
                                'python', '-I', '-B', '/checks/workflow_paths.py', encoded(event['paths']).decode('ascii')])
                            self._retain(label + '-path-facts.json', commands.raw(probe))
                        try:
                            checked(label + '-pause', ['docker', 'pause', container_id])
                            record = checked(label + '-state', ['docker', 'inspect', '--format', '{{json .}}', container_id])
                            state = parsed(record)
                            require(b01._paused(state, volume, self.policy.image_id) and state.get('Id') == container_id
                                and state.get('Config', {}).get('Labels') == labels, 'Exact snapshot ownership differs')
                            checked(label + '-capture', ['docker', 'cp', container_id + ':/tmp', '-'], b02.MAX_CAPTURE_BYTES)
                        finally:
                            checked(label + '-unpause', ['docker', 'unpause', container_id])
                    finally:
                        self._phase_runtime(label + '-runtime-after')
                        self._retain(label + '-staging-after.json', encoded(verify_staging()))
                        if event.get('kind') == 'result':
                            self._eligible(label + '-response', (label + '-exec-verified.json',
                                label + '-runtime-after-verified.json', label + '-staging-after.json',
                                event['phase'] + '-response.bin'))
                    self._eligible(label + '-capture', (label + '-capture.json', label + '-unpause.json',
                        label + '-runtime-after-verified.json', label + '-staging-after.json'))
                def measured_capture(label: str, event: dict[str, Any]) -> None:
                    with self._timings.span('capture'):
                        capture(label, event)
                self._session_calls(session, commands, container_id, measured_capture, verify_staging)
                session = None
                self._session = None
            except BaseException as error:
                primary = error
                errors.append(type(error).__name__ + ':' + str(error)[:500])
            finally:
                try:
                    with self._timings.span('teardown'):
                        ordinary_cleanup()
                except BaseException as error:
                    errors.append('cleanup:' + type(error).__name__ + ':' + str(error)[:300])
                    # Prior-only owner claims, independent diagnostics; never heals
                    # the observation journal or upgrades cleanup success.
                    try:
                        self.cleanup_result = self._cleanup.run(reason=repr(primary or error))
                    except BaseException as cleanup_error:
                        errors.append('emergency-cleanup:' + type(cleanup_error).__name__)
                if session is not None and not session.finished:
                    try:
                        session.finish(False)
                    except BaseException as error:
                        errors.append('session-close:' + type(error).__name__)
                if session is not None:
                    try:
                        session.stop_local()
                    except BaseException as error:
                        errors.append('local-session-teardown:' + type(error).__name__)
                self._session = None
                self._cleanup_phase = False
            try:
                self._retain('staging-final.json', encoded(verify_staging()))
            except BaseException as error:
                errors.append('final-staging:' + type(error).__name__)
        assert self.journal is not None
        if self.journal.uncertain:
            raise ExecutionUnknown('Original workflow journal became uncertain; redispatch forbidden') from primary
        terminal = {'protocol': self.binding.protocol, 'intent_sha256': sha(self.read_authenticated('intent.json')),
            'execution_id': intent['execution_id'], 'case_id': case, 'family': self.binding.family,
            'source_sha256': self.binding.source_sha256, 'native_source_sha256': self.binding.native_source_sha256,
            'commands': list(commands.records), 'infrastructure': errors,
            'original_definition_purpose': self.observation_registration.original_definition_purpose,
            'container_cleanup': removed['container'], 'volume_cleanup': removed['volume'],
            'cleanup_verified': all(removed.values()), 'status': 'completed' if not errors else 'infrastructure_error',
            'missing_step_ids': [phase for phase in self.profile.phases if not self.has_retained(phase + '-response.bin')]}
        self._retain('terminal.json', encoded(terminal))
        if primary is not None and not isinstance(primary, Exception):
            raise primary


    def __enter__(self) -> CandidateWorkflowExecution:
        return self

    def _exec_identity(self, commands: _Commands, container_id: str, label: str, *, completed: bool = False) -> None:
        self._effect_boundary()
        deadline = self._operation_deadline(self.policy.timeout_seconds)
        if self._exec_id is None:
            value = process._json_control(self.endpoint, '/containers/' + container_id + '/json',
                deadline=deadline, retain=self._retain, label=label + '-exec-container')
            self.checkpoint()
            ids = value.get('ExecIDs')
            require(value.get('Id') == container_id and type(ids) is list and len(ids) == 1,
                'Unambiguous original solve ExecID unavailable')
            assert isinstance(ids, list)
            registry.sha256(ids[0])
            self._exec_id = ids[0]
        value = process._json_control(self.endpoint, '/exec/' + self._exec_id + '/json',
            deadline=deadline, retain=self._retain, label=label + '-exec')
        self.checkpoint()
        self._effect_boundary()
        self._exec_pid = exec_identity(value, container_id=container_id, exec_id=self._exec_id,
            pid=self._exec_pid, completed=completed)
        self._retain(label + '-exec-verified.json', encoded({'exec_id': self._exec_id,
            'pid': self._exec_pid, 'completed': completed, 'value_sha256': digest(value)}))
        self.checkpoint()

    def checkpoint(self) -> chain.PrefixCommitment:
        with self._timings.span('checkpoint'):
            return super().checkpoint()

    @contextmanager
    def _call_window(self, phase: str) -> Iterator[float]:
        require(self._active_call_deadline is None and self._active_call is None,
            'Workflow call windows cannot nest or reset')
        require(phase in ('session-ready', 'session-finish') or phase in self.profile.phases, 'Undeclared call window')
        self._check_deadline()
        self._active_call = phase
        self._active_call_deadline = time.monotonic() + self.policy.timeout_seconds
        try:
            yield self._active_call_deadline
        finally:
            self._active_call_deadline = None
            self._active_call = None

    def _operation_deadline(self, seconds: float) -> float:
        deadline = time.monotonic() + seconds
        if not self._cleanup_phase:
            for boundary in (self._deadline, self._active_call_deadline):
                if boundary is not None:
                    deadline = min(deadline, boundary)
        return deadline

    def _runtime(self, label: str) -> dict[str, Any]:
        with self._timings.span('runtime'):
            self._check_deadline()
            value = _runtime_identity(self.endpoint, self.policy.image_id,
                deadline=self._operation_deadline(15), retain=self._retain, label=label)
            self._check_deadline()
            return value

    def _phase_runtime(self, label: str) -> None:
        self._effect_boundary()
        value = self._runtime(label)
        require(value == self.runtime, 'Original runtime changed at phase boundary')
        self._retain(label + '-verified.json', encoded({'runtime': value, 'runtime_sha256': digest(value)}))
        self._check_deadline()

    def _eligible(self, label: str, members: tuple[str, ...]) -> None:
        # Correctness provenance, separate from optional segment diagnostics.
        # First authenticate all required originals; check after their durability.
        self.checkpoint()
        require(all(self.has_retained(name) for name in members), 'Deadline eligibility originals unavailable')
        originals = {name: sha(self.read_authenticated(name)) for name in members}
        self._check_deadline()
        require(self._active_call is not None and self._active_call_deadline is not None,
            'Eligibility requires the original active call window')
        record = {'policy_id': DEADLINE_POLICY_ID, 'policy_sha256': digest(deadline_policy()),
            'binding_sha256': digest(asdict(self.binding)), 'phase': self._active_call,
            'label': label, 'originals': originals, 'decision': 'eligible'}
        # Passive recording can finish after the decision; every subsequent
        # effect checks its own budget. No timestamp summary grants authority.
        self._retain(label + '-eligible.json', encoded(record))
        self.checkpoint()
        self._check_deadline()

    def _effect_boundary(self) -> None:
        with self._timings.span('effect'):
            self._owner()
            self._check_deadline()
            self.checkpoint()
            if self.mode == 'physical':
                self.endpoint.validate()
            if not self._cleanup_phase:
                self.current(self._freeze)
                self._check_deadline()

    def _check_deadline(self) -> None:
        if self.mode == 'physical' and not self._cleanup_phase:
            now = time.monotonic()
            if self._deadline is not None and now >= self._deadline:
                raise WorkflowDeadlineExceeded('Declared workflow history deadline reached')
            if self._active_call_deadline is not None and now >= self._active_call_deadline:
                raise WorkflowDeadlineExceeded('Declared workflow observation deadline reached')

    def _flush_timings(self, intent: dict[str, Any]) -> None:
        # Diagnostic sidecar is deliberately outside the observation journal.
        # Never attempt to append to an uncertain chain or turn timing into proof.
        fd: int | None = None
        try:
            record = {'protocol': TIMING_PROTOCOL, 'policy': deadline_policy(),
                'execution_id': intent['execution_id'], 'binding_sha256': digest(asdict(self.binding)),
                'source_sha256': self.binding.source_sha256, 'evaluator_sha256': self.binding.evaluator_sha256,
                'semantics': 'inclusive nested host spans; diagnostic only; never sum nested totals',
                'truncated': self._timings.omitted, 'spans': self._timings.rows}
            raw = encoded(record)
            require(len(raw) <= TIMING_MAX_BYTES, 'Diagnostic byte cap exceeded')
            path = self.root.with_name(self.root.name + '-' + intent['execution_id'] + '-timings.json')
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            offset = 0
            while offset < len(raw):
                count = os.write(fd, raw[offset:])
                require(count > 0, 'Incomplete diagnostic write')
                offset += count
            os.fsync(fd)
            self.diagnostic_path = str(path)
        except BaseException as error:
            self.diagnostic_error = type(error).__name__
        finally:
            if fd is not None:
                try:
                    os.close(fd)
                except BaseException:
                    self.diagnostic_error = 'DiagnosticCloseError'

    def close(self) -> None:
        if getattr(self, 'closed', True):
            return
        self._owner()
        errors: list[BaseException] = []
        # Every owned release is attempted even after another release failed.
        for item in (getattr(self, '_session', None), getattr(self, '_cleanup', None), getattr(self, 'journal', None)):
            if item is None:
                continue
            try:
                if isinstance(item, _Session):
                    item.stop_local()
                else:
                    item.close()
            except BaseException as error:
                errors.append(error)
        self.closed = True
        self.close_errors = [type(error).__name__ + ':' + str(error)[:500] for error in errors]
        if errors:
            raise errors[0]

    def __exit__(self, error_type: Any, error: Any, traceback: Any) -> None:
        try:
            self.close()
        except BaseException:
            if error is None:
                raise


    def _recipe(self) -> dict[str, Any]:
        return profile.recipe_for(self.binding.case_id)

    def _inputs(self) -> dict[str, bytes]:
        return profile.input_files(self.binding.case_id)

    def _helpers(self) -> dict[str, bytes]:
        return adapter_files(self.binding.case_id, self.plan)

    def _session_calls(self, session: _Session, commands: _Commands, container_id: str,
                       capture: Any, verify_staging: Any) -> None:
        session.ready(lambda event: capture('session-ready', event))
        for phase in self.profile.phases:
            self._phase_runtime(phase + '-runtime-before')
            self._retain(phase + '-staging-before.json', encoded(verify_staging()))
            try:
                session.phase(phase, lambda label, event: capture(label, event))
            finally:
                self._phase_runtime(phase + '-runtime-after')
                self._retain(phase + '-staging-after.json', encoded(verify_staging()))
        require(session.finish(True), 'Workflow session termination incomplete')
        self._exec_identity(commands, container_id, 'session-final', completed=True)
        self._session = None


observation_registration_for = observation_registration

# This type is deliberately separate from WorkflowProfile. It is excluded from
# product reconstruction, recipe/scope factories and Registry Observation routes.
QUALIFICATION_PROTOCOL = 'candidate-workflow-wire-qualification-v1'
QUALIFICATION_FAMILY = 'workflow-wire-harness-qualification-v1'
QUALIFICATION_IDS = ('WQ-NORMALIZED-61824', 'WQ-FRAME-EXACT', 'WQ-FRAME-OVER',
    'WQ-STDOUT-EXACT', 'WQ-STDOUT-OVER', 'WQ-STDERR-EXACT', 'WQ-STDERR-OVER')
_READY = b'{"kind":"ready","protocol":"candidate-workflow-wire-v1"}\n'


def qualification_bytes(control_id: str) -> tuple[bytes, bytes]:
    require(control_id in QUALIFICATION_IDS, 'Closed qualification control required')
    value: Any = 'x' * 61822 if control_id == 'WQ-NORMALIZED-61824' else {}
    frame = json.dumps({'kind': 'result', 'phase': 'call-000', 'value': value},
        ensure_ascii=True, separators=(',', ':'), allow_nan=False).encode()
    if control_id.startswith('WQ-FRAME-'):
        frame += b' ' * ((FRAME_BYTES + int(control_id.endswith('OVER'))) - len(frame))
    if control_id.startswith('WQ-STDOUT-'):
        return _READY + b'q' * (STDOUT_BYTES + int(control_id.endswith('OVER')) - len(_READY)), b''
    stderr = b'e' * (STDERR_BYTES + int(control_id.endswith('OVER'))) if control_id.startswith('WQ-STDERR-') else b''
    return _READY + frame + b'\n', stderr


def qualification_source_files(control_id: str) -> dict[str, bytes]:
    """Seven fixed producer sources; no caller-supplied output or expectations."""
    stdout, stderr = qualification_bytes(control_id)
    # Producer bytes include no candidate/oracle imports or alternative paths.
    program = ('import os\ndef run():\n'
        + '    out = ' + repr(stdout[len(_READY):]) + '\n'
        + '    err = ' + repr(stderr) + '\n'
        + '    for fd, raw in ((1,out),(2,err)):\n'
        + '        while raw:\n'
        + '            count = os.write(fd, raw)\n'
        + '            raw = raw[count:]\n')
    return {'solution.py': program.encode('ascii')}


QUALIFICATION_ADAPTER = '''import sys
sys.path.insert(0, '/workspace')
from solution import run
sys.stdout.write('{"kind":"ready","protocol":"candidate-workflow-wire-v1"}\\n')
sys.stdout.flush()
if sys.stdin.readline() != 'ready\\n' or sys.stdin.readline() != 'call-000\\n':
    raise RuntimeError('Closed qualification handshake differs')
run()
'''


@dataclass(frozen=True, slots=True)
class WorkflowQualificationProfile:
    control_id: str
    purpose: str = 'public_release'

    def __post_init__(self) -> None:
        require(self.control_id in QUALIFICATION_IDS and self.purpose == 'public_release', 'Closed harness qualification profile required')

    @property
    def case_id(self) -> str:
        return self.control_id

    @property
    def family(self) -> str:
        return QUALIFICATION_FAMILY

    @property
    def phases(self) -> tuple[str, ...]:
        return ('call-000',)

    @property
    def original_definition_purpose(self) -> str:
        return 'harness_qualification'

    @property
    def ordered_case_ids(self) -> tuple[str, ...]:
        return (QUALIFICATION_FAMILY + ':' + self.control_id,)

    def record(self) -> dict[str, Any]:
        stdout, stderr = qualification_bytes(self.control_id)
        return {'protocol': QUALIFICATION_PROTOCOL, 'control_id': self.control_id,
            'original_definition_purpose': 'harness_qualification', 'low_level_execution_label': self.purpose,
            'source_manifest': admission.source_manifest(qualification_source_files(self.control_id)),
            'stdout': {'bytes': len(stdout), 'sha256': sha(stdout)},
            'stderr': {'bytes': len(stderr), 'sha256': sha(stderr)}, 'wire': wire_definition(),
            'product_history': False, 'product_acceptance_authority': False}

    @property
    def sha256(self) -> str:
        return digest(self.record())


@dataclass(frozen=True, slots=True)
class WorkflowQualificationBinding(WorkflowBinding):
    protocol: str = QUALIFICATION_PROTOCOL
    original_definition_purpose: str = 'harness_qualification'

    def __post_init__(self) -> None:
        require(self.protocol == QUALIFICATION_PROTOCOL and self.family == QUALIFICATION_FAMILY
            and self.original_definition_purpose == 'harness_qualification' and self.purpose == 'public_release' and self.milestone == 'M4'
            and self.requirements_sha256 == TARGET_CONTRACT, 'Separate closed qualifier binding required')
        require(self.case_id in QUALIFICATION_IDS, 'Closed qualifier source required')
        for name, value in asdict(self).items():
            if name.endswith('_sha256'):
                registry.sha256(value)


@dataclass(frozen=True, slots=True)
class WorkflowQualificationRegistration:
    binding: WorkflowQualificationBinding
    commit_oid: str
    tree_oid: str
    repetition_id: str
    gate: registry.Gate
    cohort_trajectory_ids: tuple[str, ...]


def qualification_adapter_files(control_id: str) -> dict[str, bytes]:
    require(control_id in QUALIFICATION_IDS, 'Closed qualification source required')
    return {'workflow_adapter.py': QUALIFICATION_ADAPTER.encode(), 'workflow_paths.py': PATH_PROBE.encode(),
        'workflow-plan.json': encoded({'original_definition_purpose': 'harness_qualification', 'boundaries': []})}


def qualification_recipe(control_id: str) -> dict[str, Any]:
    require(control_id in QUALIFICATION_IDS, 'Closed qualification source required')
    return {'protocol': QUALIFICATION_PROTOCOL, 'control_id': control_id,
        'original_definition_purpose': 'harness_qualification', 'calls': [{}]}


def qualification_inputs(control_id: str) -> dict[str, bytes]:
    return {'workflow-input.json': encoded(qualification_recipe(control_id))}


def qualification_binding_for(files: dict[str, bytes], value: WorkflowQualificationProfile,
                              policy: WorkflowPolicy, runtime: dict[str, Any]) -> WorkflowQualificationBinding:
    require(type(value) is WorkflowQualificationProfile and type(policy) is WorkflowPolicy,
        'Exact separate qualification profile/policy required')
    require(files == qualification_source_files(value.control_id), 'Only exact fixed qualifier source bytes allowed')
    marker = digest({'original_definition_purpose': 'harness_qualification', 'independent_review': 'not-product-source-inspection',
        'control_definition': value.record()})
    fixture = digest({'adapter_files': admission.source_manifest(qualification_adapter_files(value.control_id)),
        'input_files': admission.source_manifest(qualification_inputs(value.control_id)), 'recipe': qualification_recipe(value.control_id)})
    return WorkflowQualificationBinding(admission.source_sha256(files), TARGET_CONTRACT, 'M4', 'public_release',
        QUALIFICATION_FAMILY, value.control_id, review.source_sha256(files), digest(value.record()), value.sha256,
        fixture, marker, marker, digest(evaluator_sources()), digest(runtime),
        digest({'environment': DockerValidator._environment(), 'host_python': [platform.python_implementation(), platform.python_version()]}),
        digest({'policy': asdict(policy), 'deadline_policy': deadline_policy(), 'source_capture_policy': SOURCE_CAPTURE_POLICY.record(), 'wire': wire_definition(),
            'prestart_policy': prestart.definition(), 'journal': asdict(LIMITS), 'cleanup': asdict(cleanup.CleanupLimits())}),
        digest({'seed': policy.seed}))


def qualification_gate_for(subject: registry.Subject, binding: WorkflowQualificationBinding, *, gate_id: str) -> registry.Gate:
    require(type(binding) is WorkflowQualificationBinding and subject.source_sha256 == binding.source_sha256
        and subject.milestone == 'M4' and subject.requirements_sha256 == TARGET_CONTRACT, 'Exact qualifier subject required')
    value = WorkflowQualificationProfile(binding.case_id)
    require(binding.profile_sha256 == value.sha256 and binding.definition_sha256 == digest(value.record()), 'Qualifier profile differs')
    return registry.Gate(gate_id, ('WORKFLOW-HARNESS-MECHANISM',), value.ordered_case_ids,
        registry.Binding(subject, digest({'profile': value.record(), 'ordered_cases': value.ordered_case_ids}),
            binding.evaluator_sha256, binding.runtime_sha256, binding.environment_sha256, binding.limits_sha256,
            binding.seed_sha256, binding.protocol, binding.purpose))


def qualification_observation_registration(registration: WorkflowQualificationRegistration) -> admission.ObservationRegistration:
    require(type(registration) is WorkflowQualificationRegistration and type(registration.binding) is WorkflowQualificationBinding,
        'Exact separate qualifier registration required')
    binding = registration.binding
    require(registration.gate == qualification_gate_for(registration.gate.binding.subject, binding,
        gate_id=registration.gate.gate_id), 'Exact qualifier gate required')
    return admission.ObservationRegistration(registration.gate, registration.commit_oid, registration.tree_oid,
        registration.repetition_id, registration.cohort_trajectory_ids, binding.definition_sha256,
        binding.profile_sha256, 'harness_qualification', admission.binding_sha256(binding, gate=registration.gate))


class CandidateWorkflowQualificationExecution(CandidateWorkflowExecution):
    """Exact seven-producer physical harness; never a product observation owner."""
    policy: Any
    registration: Any
    profile: Any
    binding: Any

    def __init__(self, root: Path, store: GitStore, registration: WorkflowQualificationRegistration, policy: WorkflowPolicy, *,
                 value: WorkflowQualificationProfile,
                 admission_authority: admission.ObservationAdmission, checkpoint_authority: chain.HeadAuthority,
                 delta_root: Path, cleanup_root: Path, endpoint: Any = None, mode: str = 'physical',
                 expected_checkpoint: chain.PrefixCommitment | None = None):
        require(type(self) is CandidateWorkflowQualificationExecution and type(registration) is WorkflowQualificationRegistration
            and type(value) is WorkflowQualificationProfile, 'Exact qualifier registration/profile required')
        require(mode in ('physical', 'fixture') and type(policy) is WorkflowPolicy, 'Exact owner mode/policy required')
        require(type(admission_authority) is admission.ObservationAdmission, 'Actual prospective qualification admission required')
        self.root, self.delta_root, self.cleanup_root = (Path(p).absolute() for p in (root, delta_root, cleanup_root))
        require(all(p.resolve() == p for p in (self.root, self.delta_root, self.cleanup_root)), 'Canonical roots required')
        require(all(not a.is_relative_to(b) and not b.is_relative_to(a) for a, b in
            ((self.root, self.cleanup_root), (self.delta_root, self.cleanup_root))), 'Separate cleanup roots required')
        require(mode != 'physical' or type(checkpoint_authority) is ExternalHead, 'Physical owner needs external head')
        if type(checkpoint_authority) is ExternalHead:
            require(checkpoint_authority.journal_roots == (self.root, self.delta_root), 'Different external journal roots')
            require(not self.cleanup_root.is_relative_to(checkpoint_authority.root)
                and not checkpoint_authority.root.is_relative_to(self.cleanup_root), 'Head/cleanup overlap')
        self.mode, self.policy, self.store = mode, policy, store
        self.registration, self.profile = registration, value
        self.plan = None
        self.review_authority = None
        self.admission, self.checkpoint_authority = admission_authority, checkpoint_authority
        self._pid, self._thread, self.closed = os.getpid(), threading.get_ident(), False
        self.journal: journals.OwnerJournal | None = None
        self._cleanup: cleanup.CleanupChannel | None = None
        self._cleanup_phase = False
        self.cleanup_result: Any = None
        self._session: _Session | None = None
        self._exec_id: str | None = None
        self._exec_pid: int | None = None
        self._deadline: float | None = None
        self._active_call_deadline: float | None = None
        self._active_call: str | None = None
        self._timings = _Timings()
        self.diagnostic_path: str | None = None
        self.diagnostic_error: str | None = None
        self.close_errors: list[str] = []
        self._freeze: registry.CohortFreeze | None = None
        self.endpoint: Any = (endpoint or process.EngineEndpoint.from_environment()) if mode == 'physical' else None
        self.docker = ['docker'] if self.endpoint is None else ['docker', '--host', 'unix://' + self.endpoint.socket_path]
        try:
            if mode == 'physical':
                assert self.endpoint is not None
                self.runtime = process.runtime_identity(self.endpoint, policy.image_id)
            else:
                self.runtime = {'kind': 'fixture-no-Docker'}
            self.sources = evaluator_sources()
            self.tree, self.files = capture_git_source(store, registration.commit_oid)
            require(self.tree == registration.tree_oid and self.files == qualification_source_files(value.control_id),
                'Exact fixed qualifier Git source differs')
            self.binding = qualification_binding_for(self.files, value, policy, self.runtime)
            require(self.binding == registration.binding, 'Complete registered workflow binding differs')
            self.observation_registration = qualification_observation_registration(registration)
            require(self.admission.registration == self.observation_registration, 'Admission belongs to another workflow execution')
            self.config = {'protocol': self.binding.protocol, 'mode': mode, 'root': str(self.root), 'delta_root': str(self.delta_root),
                'cleanup_root': str(self.cleanup_root), 'repository': str(store.path.resolve()), 'registration': asdict(registration),
                'profile': value.record(), 'original_definition_purpose': 'harness_qualification',
                'product_acceptance_authority': False,
                'source_manifest': admission.source_manifest(self.files), 'sources': self.sources,
                'runtime': self.runtime, 'policy': asdict(policy), 'source_capture_policy': SOURCE_CAPTURE_POLICY.record(),
                'deadline_policy': deadline_policy(),
                'journal_limits': asdict(LIMITS), 'prestart_policy': prestart.definition(),
                'endpoint': None if self.endpoint is None else asdict(self.endpoint)}
            context = {'protocol': self.binding.protocol, 'config_sha256': digest(self.config),
                'source_sha256': self.binding.source_sha256, 'purpose': self.binding.purpose,
                'original_definition_purpose': self.observation_registration.original_definition_purpose,
                'original_binding_sha256': digest(asdict(self.binding))}
            existed = self.root.exists()
            require(existed == (expected_checkpoint is not None), 'Reopen requires independently supplied exact prefix')
            self.journal = journals.OwnerJournal(self.root, self.delta_root, context=context,
                authority=checkpoint_authority, expected=expected_checkpoint, limits=LIMITS)
            if existed:
                require(self.read_authenticated('config.json') == encoded(self.config), 'Authenticated config differs')
            else:
                self._retain('config.json', encoded(self.config))
            self.checkpoint()
        except BaseException:
            try:
                self.close()
            except BaseException:
                pass
            raise

    def current(self, freeze: registry.CohortFreeze | None = None) -> None:
        with self._timings.span('current'):
            self._current(freeze)

    def _current(self, freeze: registry.CohortFreeze | None = None) -> None:
        self._owner()
        require(type(self) is CandidateWorkflowQualificationExecution and type(self.profile) is WorkflowQualificationProfile,
            'Exact qualifier owner/profile required')
        require(evaluator_sources() == self.sources, 'Qualifier evaluator changed')
        with self._timings.span('source'):
            tree, files = capture_git_source(self.store, self.registration.commit_oid)
        require(tree == self.tree and files == self.files and files == qualification_source_files(self.profile.control_id),
            'Fixed qualifier source changed')
        require(qualification_binding_for(files, self.profile, self.policy, self.runtime) == self.binding,
            'Original qualifier binding changed')
        self.admission.check_current(self.observation_registration, freeze)

    def execute_qualification_once(self) -> dict[str, Any]:
        return super().execute_once()

    def execute_once(self) -> dict[str, Any]:
        raise ExecutionError('Qualification requires explicit non-product dispatch')

    def _recipe(self) -> dict[str, Any]:
        return qualification_recipe(self.profile.control_id)

    def _inputs(self) -> dict[str, bytes]:
        return qualification_inputs(self.profile.control_id)

    def _helpers(self) -> dict[str, bytes]:
        return qualification_adapter_files(self.profile.control_id)

    def _session_calls(self, session: _Session, commands: _Commands, container_id: str,
                       capture: Any, verify_staging: Any) -> None:
        session.ready(lambda event: capture('session-ready', event))
        self._phase_runtime('call-000-runtime-before')
        self._retain('call-000-staging-before.json', encoded(verify_staging()))
        with self._call_window('call-000'):
            session._write('call-000', 'call-000-request.json')
            session.requests.append('call-000')
            # Overflow is an expected mechanism observation for three closed controls;
            # capture completeness and natural process exit are retained separately.
            session.finish(True, send_finish=False)
            self._exec_identity(commands, container_id, 'session-final', completed=True)
            self._phase_runtime('call-000-runtime-after')
            self._retain('call-000-staging-after.json', encoded(verify_staging()))
            self._eligible('call-000-qualification', ('session.json', 'session-final-exec-verified.json',
                'call-000-runtime-after-verified.json', 'call-000-staging-after.json'))
