"""Prospective final-M4 B01/B02 owner; original qualification drivers stay frozen.

The fixed adapters and bounded capture parsers are reused, not their historical
purpose or source-review grants. Every new physical history has a fresh durable
intent, explicit full-cohort admission and independently enrolled layout review.
A response-followed pause is not an atomic method-return boundary. Store reopen
is not process death. This module does not supply full semantic ScopePlan review.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
import os
from pathlib import Path
import platform
import queue
import stat
import subprocess
import tempfile
import threading
from typing import Any, cast
import uuid

from . import candidate_storage_driver_v1 as b01
from . import candidate_intake_store_driver_v3 as b02
from . import candidate_storage_product_profile_v1 as profile
from . import candidate_storage_review_authority_v1 as review
from . import candidate_observation_admission_v1 as admission
from . import candidate_execution_journal_v1 as journals
from . import candidate_checkpoint_chain_v1 as chain
from .candidate_checkpoint_head_v1 import ExternalHead
from . import candidate_emergency_cleanup_v1 as cleanup
from . import candidate_client_process_v4 as process
from .candidate_release_execution_v2 import capture_git_source
from . import candidate_source_capture_policy_v1 as source_capture
from .gitstore import GitStore
from .sandbox import DockerValidator
from . import candidate_storage_prestart_v1 as prestart
from . import project_acceptance_registry_v1 as registry

PROTOCOL = "candidate-storage-product-execution-v1-ascii-json-v1-prestart-v2-desktop-inputs-v1"
BATCH_PROTOCOL = PROTOCOL + "-git-source-batch-v1"
MAPPED_PROTOCOL = PROTOCOL + "-finite-b02-mapping-v1"
MAPPED_BATCH_PROTOCOL = MAPPED_PROTOCOL + "-git-source-batch-v1"
EXECUTION_PROTOCOLS = (PROTOCOL, BATCH_PROTOCOL, MAPPED_PROTOCOL, MAPPED_BATCH_PROTOCOL)
TARGET_CONTRACT = "2d88ce0775888f148b0ec3caf90b3d5c82d8fed71f53bec5f7e75f492ae998dc"
LIMITS = chain.Limits()
CHUNK_BYTES = 16 * 1024 * 1024
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
encoded, digest = profile.encoded, profile.digest


class ExecutionError(ValueError):
    pass


class ExecutionUnknown(ExecutionError):
    pass


def require(ok: bool, message: str) -> None:
    if not ok:
        raise ExecutionError(message)


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def evaluator_sources() -> dict[str, str]:
    names = set(b01.driver_sources()) | set(b02.driver_sources()) | {
        'candidate_storage_product_execution_v1.py', 'candidate_storage_product_profile_v1.py',
        'candidate_storage_product_observation_v1.py', 'candidate_storage_review_authority_v1.py',
        'candidate_storage_prestart_v1.py',
        'candidate_observation_admission_v1.py', 'candidate_execution_journal_v1.py',
        'candidate_checkpoint_chain_v1.py', 'candidate_checkpoint_head_v1.py',
        'candidate_http_journal_v3.py', 'candidate_emergency_cleanup_v1.py',
        'candidate_client_process_v4.py', 'candidate_release_execution_v2.py', 'gitstore.py',
        'project_acceptance_registry_v1.py'}
    root = Path(__file__).resolve().parent
    result = {'gossip_harness/' + name: sha((root / name).read_bytes()) for name in sorted(names)}
    result.update(source_capture.evaluator_sources())
    result.update(profile.finite.sources())
    admission.verify_loaded_sources(result)
    return result


def capture_policy_for(protocol: str) -> source_capture.BatchCapturePolicy | None:
    require(type(protocol) is str and protocol in EXECUTION_PROTOCOLS, 'Unknown storage source-capture protocol')
    return None if protocol in (PROTOCOL, MAPPED_PROTOCOL) else source_capture.BatchCapturePolicy()


def mapping_profile_for(protocol: str) -> str | None:
    require(type(protocol) is str and protocol in EXECUTION_PROTOCOLS, 'Unknown storage mapping protocol')
    return profile.finite.STORAGE_MAPPING if protocol in (MAPPED_PROTOCOL, MAPPED_BATCH_PROTOCOL) else None


def protocol_for(mapping_profile: str | None, capture_policy: source_capture.BatchCapturePolicy | None) -> str:
    require(mapping_profile is None or type(mapping_profile) is str and mapping_profile == profile.finite.STORAGE_MAPPING,
            'Unknown storage mapping profile')
    require(capture_policy is None or type(capture_policy) is source_capture.BatchCapturePolicy,
            'Exact closed source-capture policy required')
    if mapping_profile is None:
        return PROTOCOL if capture_policy is None else BATCH_PROTOCOL
    return MAPPED_PROTOCOL if capture_policy is None else MAPPED_BATCH_PROTOCOL


def capture_source(store: GitStore, commit_oid: str, *,
                   policy: source_capture.BatchCapturePolicy | None = None) -> tuple[str, dict[str, bytes]]:
    """Registration and owner boundaries use the same closed fresh capture route."""
    return source_capture.capture_registered_source(store, commit_oid, policy=policy)


@dataclass(frozen=True, slots=True)
class StoragePolicy:
    timeout_seconds: int = 30
    image_id: str = b01.RUNTIME_IMAGE
    seed: str = 'fixed-public-storage-histories-v1'

    def __post_init__(self) -> None:
        require(type(self.timeout_seconds) is int and 1 <= self.timeout_seconds <= 120, 'Invalid timeout')
        require(self.image_id == b01.RUNTIME_IMAGE, 'Exact retained image required')
        registry.identifier(self.seed)


@dataclass(frozen=True, slots=True)
class StorageBinding:
    source_sha256: str
    requirements_sha256: str
    milestone: str
    purpose: str
    family: str
    case_id: str
    native_source_sha256: str
    definition_sha256: str
    profile_sha256: str
    review_sha256: str
    review_origin_sha256: str
    evaluator_sha256: str
    runtime_sha256: str
    environment_sha256: str
    limits_sha256: str
    seed_sha256: str
    protocol: str = PROTOCOL

    def __post_init__(self) -> None:
        require(self.protocol in EXECUTION_PROTOCOLS and self.milestone == 'M4'
            and self.requirements_sha256 == TARGET_CONTRACT, 'Explicit current final-M4 contract required')
        require(self.family in ('b01', 'b02') and self.purpose in registry.PURPOSES, 'Product family/purpose required')
        require(mapping_profile_for(self.protocol) is None or self.family == 'b02' and self.purpose == 'independent_acceptance',
                'Mapped storage requires B02 independent acceptance')
        for name, value in asdict(self).items():
            if name.endswith('_sha256'):
                registry.sha256(value)
        registry.identifier(self.case_id)


def profile_for_binding(binding: StorageBinding) -> profile.StorageProductProfile:
    require(type(binding) is StorageBinding, 'Exact storage binding required')
    value = profile.profile_for(binding.family, binding.case_id, binding.purpose,
        mapping_profile=mapping_profile_for(binding.protocol))
    require(binding.profile_sha256 == value.sha256 and binding.definition_sha256 == digest(value.record()),
            'Stored binding differs from exact closed profile')
    return value


def binding_for(files: dict[str, bytes], value: Any, policy: StoragePolicy, runtime: dict[str, Any],
                plan: review.LayoutPlan, *, review_authority: review.StorageReviewAuthority,
                capture_policy: source_capture.BatchCapturePolicy | None = None) -> StorageBinding:
    require(type(review_authority) is review.StorageReviewAuthority, 'Exact original layout authority required')
    provenance = review_authority.provenance(plan)
    review_sha256 = review_authority.enrollment.report_sha256
    actual = profile.reconstruct(value)
    require(value == actual, 'Exact source-derived storage profile required')
    native = (b01 if value.family == 'b01' else b02).source_sha256(files)
    require(type(policy) is StoragePolicy and review.accepted_layout_plan(plan)
        and plan.source_sha256 == admission.source_sha256(files) and plan.native_source_sha256 == native
        and plan.family == value.family and plan.case_id == value.case_id and plan.profile_sha256 == value.sha256
        and plan.purpose == value.purpose
        and getattr(plan, 'mapping_profile', None) == profile.mapping_profile_for(actual),
        'Source, original driver identity, profile and reviewed layout differ')
    require(capture_policy is None or type(capture_policy) is source_capture.BatchCapturePolicy,
            'Exact closed source-capture policy required')
    capture_record = None if capture_policy is None else capture_policy.record()
    limits = {'policy': asdict(policy), 'journal': asdict(LIMITS), 'chunk_bytes': CHUNK_BYTES,
        'capture_bytes': b02.MAX_CAPTURE_BYTES, 'original_stream_bytes': b01.MAX_STREAM_BYTES if value.family == 'b01' else b02.MAX_STREAM_BYTES,
        'cleanup': asdict(cleanup.CleanupLimits()), 'prestart_policy': prestart.definition()}
    if capture_record is not None:
        limits['source_capture'] = capture_record
    forced = value.family == 'b02' and value.case_id in b02.FORCED_CASE_IDS
    require(plan.schedule == ('forced_schedule_unavailable' if forced else 'ordinary_public_operations'),
            'No source-specific forced schedule qualification is supplied')
    return StorageBinding(admission.source_sha256(files), TARGET_CONTRACT, 'M4', value.purpose,
        value.family, value.case_id, native, digest(value.record()), value.sha256, review_sha256, digest(provenance),
        digest(evaluator_sources()), digest(runtime), digest({'environment': DockerValidator._environment(),
            'host_python': [platform.python_implementation(), platform.python_version()],
            'snapshot_protocol': b01.SNAPSHOT_PROTOCOL, 'volume_options': b01.VOLUME_OPTIONS}),
        digest(limits), digest({'seed': policy.seed}),
        protocol=protocol_for(profile.mapping_profile_for(actual), capture_policy))


def mechanics_case_id(value: Any) -> str:
    return value.family + ':' + value.case_id + ':mechanics'


def gate_for(subject: registry.Subject, binding: StorageBinding, *, gate_id: str) -> registry.Gate:
    value = profile_for_binding(binding)
    require(type(binding) is StorageBinding and binding.profile_sha256 == value.sha256
        and subject.source_sha256 == binding.source_sha256 and subject.milestone == 'M4'
        and subject.requirements_sha256 == TARGET_CONTRACT, 'Prospective subject/profile differs')
    roster = value.ordered_case_ids + (mechanics_case_id(value),)
    return registry.Gate(gate_id, value.requirement_ids, roster,
        registry.Binding(subject, digest({'profile': value.record(), 'ordered_cases': roster}),
            binding.evaluator_sha256, binding.runtime_sha256, binding.environment_sha256,
            binding.limits_sha256, binding.seed_sha256, binding.protocol, binding.purpose))


@dataclass(frozen=True, slots=True)
class StorageRegistration:
    binding: StorageBinding
    commit_oid: str
    tree_oid: str
    repetition_id: str
    gate: registry.Gate
    cohort_trajectory_ids: tuple[str, ...]


def observation_registration(registration: StorageRegistration) -> admission.ObservationRegistration:
    require(type(registration) is StorageRegistration, 'Exact storage registration required')
    binding = registration.binding
    require(registration.gate == gate_for(registration.gate.binding.subject, binding,
            gate_id=registration.gate.gate_id), 'Registered storage gate differs')
    return admission.ObservationRegistration(registration.gate, registration.commit_oid,
        registration.tree_oid, registration.repetition_id, registration.cohort_trajectory_ids,
        binding.definition_sha256, binding.profile_sha256, profile.ORIGINAL_DEFINITION_PURPOSE,
        admission.binding_sha256(binding, gate=registration.gate))


def adapter_files(family: str, case_id: str, application: dict[str, Any]) -> dict[str, bytes]:
    if family == 'b01':
        return {'storage_adapter.py': b01.CHILD_ADAPTER.encode()}
    recipe = b02.validate_recipe(b02.cases.execution_recipe(case_id))
    return {'intake_store_adapter.py': b02.CHILD_ADAPTER.encode(),
            'recipe.json': encoded({'phases': recipe['phases']}), 'applicability.json': encoded(application)}


def _verify_regular_tree(root: Path, expected: dict[str, bytes]) -> None:
    require(root.is_dir() and not root.is_symlink() and root.resolve() == root, 'Canonical staging root required')
    directories = {str(parent) for name in expected for parent in Path(name).parents if str(parent) != '.'}
    observed_dirs: set[str] = set()
    observed: dict[str, bytes] = {}
    for parent, dirs, files in os.walk(root, followlinks=False):
        for name in dirs + files:
            path = Path(parent) / name
            relative = path.relative_to(root).as_posix()
            mode = path.lstat().st_mode
            require(not stat.S_ISLNK(mode), 'Staging symlink refused')
            if stat.S_ISDIR(mode):
                observed_dirs.add(relative)
            else:
                require(stat.S_ISREG(mode), 'Staging special file refused')
                observed[relative] = path.read_bytes()
    require(observed == expected and observed_dirs == directories, 'Complete staging bytes/directories differ')




class _Commands(b01._Commands):
    def __init__(self, owner: CandidateStorageExecution):
        super().__init__(owner.root, owner.policy.timeout_seconds)
        self.owner = owner

    def retain(self, name: str, raw: bytes) -> None:
        # Frozen transport records used `arguments`; canonical endpoint-bound
        # argv is added in this new protocol for typed cleanup proof consumers.
        if name.endswith('.json'):
            try:
                record = json.loads(raw)
                if type(record) is dict and 'arguments' in record and 'exit_code' in record:
                    record['argv'] = record['arguments']
                    raw = encoded(record)
            except (ValueError, UnicodeError):
                pass
        self.owner._retain_blob(name, raw)

    def run(self, label: str, arguments: list[str], limit: int = b01.MAX_STREAM_BYTES) -> dict[str, Any]:
        self.owner._effect_boundary()
        require(arguments[0] == 'docker', 'Closed Docker controller operations only')
        argv = self.owner.docker + arguments[1:]
        self.owner._retain(label + '-dispatch.json', encoded({'argv': argv, 'limit': limit}))
        self.owner.checkpoint()
        self.owner._effect_boundary()
        require(label not in self.records, 'Duplicate control process')
        streams = {'stdout': bytearray(), 'stderr': bytearray()}
        counts = {'stdout': 0, 'stderr': 0}
        errors: list[str] = []
        child = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            stdin=subprocess.DEVNULL, env=DockerValidator._environment())
        threads: list[threading.Thread] = []
        timed_out = False
        primary: BaseException | None = None
        def drain(kind: str) -> None:
            pipe = getattr(child, kind)
            try:
                while chunk := pipe.read(65536):
                    counts[kind] += len(chunk)
                    streams[kind].extend(chunk[:max(0, limit - len(streams[kind]))])
            except OSError:
                errors.append(kind)
            finally:
                pipe.close()
        try:
            threads = [threading.Thread(target=drain, args=(kind,), daemon=True) for kind in streams]
            for thread in threads:
                thread.start()
            try:
                child.wait(timeout=self.timeout)
            except subprocess.TimeoutExpired:
                timed_out = True
                child.kill()
                child.wait(timeout=5)
        except BaseException as error:
            primary = error
            raise
        finally:
            try:
                _stop_local_process(child, threads)
            except BaseException:
                if primary is None:
                    raise
        complete = not errors and all(not thread.is_alive() for thread in threads)
        record = {'arguments': argv, 'argv': argv, 'exit_code': child.returncode,
            'timed_out': timed_out, 'capture_complete': complete}
        for kind, data in streams.items():
            raw = bytes(data)
            name = label + '-' + kind + '.bin'
            self.retain(name, raw)
            record[kind] = {'path': name, 'bytes': len(raw), 'observed_bytes': counts[kind],
                'sha256': sha(raw), 'truncated': counts[kind] > len(raw)}
        self.retain(label + '.json', encoded(record))
        self.records[label] = record
        self.owner.checkpoint()
        return record

    def raw(self, record: dict[str, Any], kind: str = 'stdout') -> bytes:
        row = record[kind]
        raw = self.owner.read_blob(row['path'])
        require(sha(raw) == row['sha256'] and len(raw) == row['bytes'], 'Authenticated command bytes differ')
        return raw


def _stop_local_process(child: Any, threads: list[threading.Thread]) -> None:
    """Bound teardown of the local CLI, including partial reader construction."""
    if child.poll() is None:
        child.kill()
        child.wait(timeout=5)
    if child.stdin is not None and not child.stdin.closed:
        child.stdin.close()
    for thread in threads:
        if thread.ident is not None:
            thread.join(timeout=5)
    require(all(not thread.is_alive() for thread in threads), 'Local process reader teardown incomplete')
    for kind in ('stdout', 'stderr'):
        pipe = getattr(child, kind)
        if pipe is not None and not pipe.closed:
            pipe.close()


class _Session:
    def __init__(self, owner: CandidateStorageExecution, commands: _Commands, argv: list[str]):
        self.owner, self.finished = owner, False
        owner._effect_boundary()
        argv = owner.docker + argv[1:]
        owner._retain('session-dispatch.json', encoded({'argv': argv}))
        owner.checkpoint()
        owner._effect_boundary()
        cls = b01._Session if owner.binding.family == 'b01' else b02._Session
        # Construct the frozen phase/drain object with ownership established
        # before starting any reader. Frozen constructors are not exception-safe.
        self.original = cast(Any, object.__new__(cls))
        self.original.commands, self.original.arguments = commands, argv
        self.original.lines = queue.Queue(maxsize=8)
        self.original.streams = {'stdout': bytearray(), 'stderr': bytearray()}
        self.original.counts = {'stdout': 0, 'stderr': 0}
        self.original.errors = set()
        self.original.requests = []
        self.original.threads = []
        self.original.process = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, env=DockerValidator._environment())
        try:
            self.original.threads = [threading.Thread(target=self.original._drain, args=(kind,), daemon=True)
                                     for kind in self.original.streams]
            for thread in self.original.threads:
                thread.start()
        except BaseException:
            try:
                self.stop_local()
            except BaseException:
                pass  # Preserve the original interruption; acceptance is impossible.
            raise

    def phase(self, phase: str) -> Any:
        self.owner._effect_boundary()
        self.owner._retain(phase + '-request.json', encoded({'phase': phase, 'request': phase + '\n'}))
        self.owner.checkpoint()
        self.owner._effect_boundary()
        value = self.original.phase(phase)
        self.owner.checkpoint()
        return value

    def finish(self, success: bool) -> bool:
        require(not self.finished, 'Session finish is one-shot')
        if success:
            self.owner._effect_boundary()
            self.owner._retain('session-finish-request.json', encoded({'request': 'finish\n'}))
            self.owner.checkpoint()
            self.owner._effect_boundary()
        self.finished = True
        return bool(self.original.finish(success))

    def stop_local(self) -> None:
        # Bounded teardown of our Docker CLI only. Resource disposal remains the
        # separate acknowledged ownership path, and this grants no success.
        _stop_local_process(self.original.process, self.original.threads)


class CandidateStorageExecution:
    """Exact trusted owner, no uploaded-directory or duck-typed receipt route."""
    def __init__(self, root: Path, store: GitStore, registration: StorageRegistration, policy: StoragePolicy, *,
                 value: Any, plan: review.LayoutPlan, review_authority: review.StorageReviewAuthority,
                 admission_authority: admission.ObservationAdmission, checkpoint_authority: chain.HeadAuthority,
                 delta_root: Path, cleanup_root: Path, endpoint: Any = None, mode: str = 'physical',
                 expected_checkpoint: chain.PrefixCommitment | None = None):
        require(mode in ('physical', 'fixture') and type(policy) is StoragePolicy, 'Exact owner mode/policy required')
        require(type(review_authority) is review.StorageReviewAuthority
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
        self.capture_policy = capture_policy_for(registration.binding.protocol)
        self.registration, self.profile, self.plan = registration, value, plan
        self.review_authority, self.admission, self.checkpoint_authority = review_authority, admission_authority, checkpoint_authority
        self._pid, self._thread, self.closed = os.getpid(), threading.get_ident(), False
        self.journal: journals.OwnerJournal | None = None
        self._cleanup: cleanup.CleanupChannel | None = None
        self._cleanup_phase = False
        self.cleanup_result: Any = None
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
            self.tree, self.files = capture_source(store, registration.commit_oid, policy=self.capture_policy)
            require(self.tree == registration.tree_oid and plan.commit_oid == registration.commit_oid
                and plan.tree_oid == self.tree, 'Complete final Git source differs')
            self.review_sha256 = review_authority.authenticate(plan)
            self.binding = binding_for(self.files, value, policy, self.runtime, plan, review_authority=review_authority,
                capture_policy=self.capture_policy)
            require(self.binding == registration.binding, 'Complete registered storage binding differs')
            self.observation_registration = observation_registration(registration)
            require(self.admission.registration == self.observation_registration, 'Admission belongs to another storage execution')
            self.config = {'protocol': self.binding.protocol, 'mode': mode, 'root': str(self.root), 'delta_root': str(self.delta_root),
                'cleanup_root': str(self.cleanup_root), 'repository': str(store.path.resolve()), 'registration': asdict(registration),
                'profile': value.record(), 'plan': asdict(plan), 'review_sha256': self.review_sha256,
                'review_provenance': review_authority.provenance(plan),
                'source_manifest': admission.source_manifest(self.files), 'sources': self.sources,
                'runtime': self.runtime, 'policy': asdict(policy), 'journal_limits': asdict(LIMITS),
                'prestart_policy': prestart.definition(),
                'endpoint': None if self.endpoint is None else asdict(self.endpoint)}
            if self.capture_policy is not None:
                self.config['source_capture'] = self.capture_policy.record()
            context = {'protocol': self.binding.protocol, 'config_sha256': digest(self.config),
                'source_sha256': self.binding.source_sha256, 'purpose': self.binding.purpose,
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
            self.close()
            raise

    def _owner(self) -> None:
        require(not self.closed and self._pid == os.getpid() and self._thread == threading.get_ident(),
                'Owner is closed or belongs to another process/thread')

    def _retain(self, name: str, raw: bytes) -> None:
        self._owner()
        require(self.journal is not None, 'Journal unavailable')
        assert self.journal is not None
        self.journal.retain(name, raw, cleanup=self._cleanup_phase)

    def _retain_blob(self, name: str, raw: bytes) -> None:
        if len(raw) <= CHUNK_BYTES:
            self._retain(name, raw)
            return
        require(len(raw) <= b02.MAX_CAPTURE_BYTES, 'Original capture bound exceeded')
        parts = []
        for index in range(0, len(raw), CHUNK_BYTES):
            part = raw[index:index + CHUNK_BYTES]
            part_name = name + '-part-' + str(index // CHUNK_BYTES)
            self._retain(part_name, part)
            parts.append({'name': part_name, 'bytes': len(part), 'sha256': sha(part)})
        self._retain(name + '-chunks.json', encoded({'bytes': len(raw), 'sha256': sha(raw), 'parts': parts}))

    def read_authenticated(self, name: str) -> bytes:
        self._owner()
        assert self.journal is not None
        return self.journal.read(name)

    def authenticated_position(self, name: str) -> int:
        """Authenticate original raw bytes and their acknowledged chain order."""
        self._owner()
        require(self.journal is not None, 'Journal unavailable')
        assert self.journal is not None
        return self.journal._chain.position(name)

    def has_retained(self, name: str) -> bool:
        self._owner()
        assert self.journal is not None
        return self.journal.has(name)

    def read_blob(self, name: str) -> bytes:
        if self.has_retained(name):
            require(not self.has_retained(name + '-chunks.json'), 'Ambiguous original blob')
            return self.read_authenticated(name)
        value = json.loads(self.read_authenticated(name + '-chunks.json'))
        require(type(value) is dict and set(value) == {'bytes', 'sha256', 'parts'}
            and type(value['parts']) is list and 2 <= len(value['parts']) <= 3, 'Original chunk roster differs')
        raws = []
        for i, row in enumerate(value['parts']):
            require(type(row) is dict and set(row) == {'name', 'bytes', 'sha256'}
                and row['name'] == name + '-part-' + str(i), 'Original chunk identity differs')
            raw = self.read_authenticated(row['name'])
            require(len(raw) == row['bytes'] and sha(raw) == row['sha256'] and len(raw) <= CHUNK_BYTES,
                    'Original chunk bytes differ')
            raws.append(raw)
        result = b''.join(raws)
        require(len(result) == value['bytes'] and sha(result) == value['sha256']
            and CHUNK_BYTES < len(result) <= b02.MAX_CAPTURE_BYTES, 'Original blob digest/bound differs')
        return result

    def checkpoint(self) -> chain.PrefixCommitment:
        self._owner()
        assert self.journal is not None
        return self.journal.checkpoint()

    def current(self, freeze: registry.CohortFreeze | None) -> None:
        self._owner()
        require(evaluator_sources() == self.sources, 'Storage evaluator changed')
        require(self.capture_policy == capture_policy_for(self.binding.protocol), 'Source-capture policy changed')
        if mapping_profile_for(self.binding.protocol) is not None:
            require(self.profile == profile_for_binding(self.binding)
                and review.accepted_layout_plan(self.plan)
                and getattr(self.plan, 'mapping_profile', None) == mapping_profile_for(self.binding.protocol),
                'Current mapped owner profile/layout differs from registered protocol')
        tree, files = capture_source(self.store, self.registration.commit_oid, policy=self.capture_policy)
        require(tree == self.tree and files == self.files, 'Final Git source changed')
        require(self.review_authority.authenticate(self.plan) == self.review_sha256
            and digest(self.review_authority.provenance(self.plan)) == self.binding.review_origin_sha256, 'Layout authority changed')
        self.admission.check_current(self.observation_registration, freeze)

    def _effect_boundary(self) -> None:
        self._owner()
        self.checkpoint()
        if self.mode == 'physical':
            self.endpoint.validate()
        if not self._cleanup_phase:
            self.current(self._freeze)

    def retained_freeze(self) -> registry.CohortFreeze | None:
        return admission.freeze_from_record(json.loads(self.read_authenticated('intent.json'))['cohort_freeze'])

    def execute_once(self) -> dict[str, Any]:
        self._owner()
        self.checkpoint()
        require(not self.has_retained('intent.json'), 'Existing intent forbids redispatch, including incomplete execution')
        require(self.mode == 'physical', 'Fixture owners never dispatch candidate code')
        self._freeze = self.admission.before_intent(self.observation_registration)
        self.current(self._freeze)
        execution_id = 'storage-product-' + uuid.uuid4().hex
        intent = {'protocol': self.binding.protocol, 'execution_id': execution_id,
            'source_sha256': self.binding.source_sha256, 'original_binding': asdict(self.binding),
            'registration': asdict(self.observation_registration),
            'cohort_freeze': None if self._freeze is None else asdict(self._freeze),
            'container': 'gossip-' + execution_id, 'volume': 'gossip-volume-' + execution_id,
            'snapshot_protocol': b01.SNAPSHOT_PROTOCOL, 'ordered_phases': list(b01.PHASES)}
        self._retain('intent.json', encoded(intent))
        self.checkpoint()
        self.current(self._freeze)
        self._dispatch(intent)
        self.current(self._freeze)
        self.checkpoint()
        return json.loads(self.read_authenticated('terminal.json'))

    def _phase_runtime(self, label: str) -> None:
        self._effect_boundary()
        value = process.runtime_identity(self.endpoint, self.policy.image_id, retain=self._retain, label=label)
        require(value == self.runtime, 'Original runtime changed at phase boundary')
        self._retain(label + '-verified.json', encoded({'runtime': value, 'runtime_sha256': digest(value)}))

    def _dispatch(self, intent: dict[str, Any]) -> None:
        commands = _Commands(self)
        original = b01 if self.binding.family == 'b01' else b02
        case = self.binding.case_id
        recipe = None if self.binding.family == 'b01' else b02.validate_recipe(b02.cases.execution_recipe(case))
        application = {'protocol': self.binding.protocol, 'decision': 'unavailable' if self.plan.schedule == 'forced_schedule_unavailable' else 'not-requested',
            'review_sha256': self.review_sha256, 'production_forced_schedule_qualified': False}
        self._retain('applicability.json', encoded(application))
        if recipe is not None:
            self._retain('recipe.json', encoded(recipe))
        name, volume = intent['container'], intent['volume']
        errors: list[str] = []
        session: _Session | None = None
        container_id: str | None = None
        claims: dict[str, str] = {}
        removed = {'container': False, 'volume': False}
        attempted = {'container': False, 'volume': False}
        labels = {'gossip.execution': intent['execution_id'], 'gossip.source': self.binding.source_sha256,
                  'gossip.fixture': digest({'recipe': recipe, 'adapter': sha(original.CHILD_ADAPTER.encode())})}
        expected_checks = adapter_files(self.binding.family, case, application)
        sandbox = DockerValidator(self.policy.image_id,
            {name: raw.decode('utf-8') for name, raw in expected_checks.items()},
            command=('python', '-I', '-c', 'import time;time.sleep(1800)'))

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
        with tempfile.TemporaryDirectory(prefix='gossip-storage-product-') as temp:
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
            adapter = 'storage_adapter.py' if recipe is None else 'intake_store_adapter.py'
            for helper_name, helper_raw in expected_checks.items():
                (checks / helper_name).write_bytes(helper_raw)
            if recipe is not None:
                b02._stage_inputs(inputs, recipe['fixtures'])
                self._retain('fixture-manifest.json', encoded(b02._verify_inputs(inputs, recipe['fixtures'])))
            for check_path in checks.iterdir():
                check_path.chmod(0o444)
            def verify_staging() -> dict[str, Any]:
                _verify_regular_tree(workspace, self.files)
                _verify_regular_tree(checks, expected_checks)
                if recipe is not None:
                    b02._verify_inputs(inputs, recipe['fixtures'])
                else:
                    _verify_regular_tree(inputs, {})
                return {'source_manifest': admission.source_manifest(self.files),
                    'helper_manifest': admission.source_manifest(expected_checks),
                    'fixtures_sha256': digest([] if recipe is None else recipe['fixtures'])}
            staging_proof = verify_staging()
            plan = {'workspace': str(workspace), 'checks': str(checks), 'inputs': str(inputs),
                    'source_manifest': admission.source_manifest(self.files), 'adapter_sha256': sha(original.CHILD_ADAPTER.encode()),
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
                observed_runtime = process.runtime_identity(self.endpoint, self.policy.image_id, retain=self._retain)
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
                argv = b01._start_arguments(sandbox, name, workspace, checks, volume) if recipe is None else b02._start_arguments(sandbox, name, workspace, checks, inputs, volume)
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
                binds = {'/workspace': str(workspace), '/checks': str(checks)}
                if recipe is not None:
                    binds['/inputs'] = str(inputs)
                proof = prestart.proof_for(commands.raw(record), container_id=container_id, name=name,
                    image_id=self.policy.image_id, volume=volume, labels=labels, mounts=binds,
                    runtime=self.runtime, runtime_originals={name: self.read_authenticated(name)
                        for name in prestart.RUNTIME_ORIGINAL_NAMES if self.has_retained(name)})
                self._retain(prestart.PROOF_FILE, encoded(proof))
                self.checkpoint()
                checked('container-start', ['docker', 'start', container_id])
                session_argv = ['docker', 'exec', '--interactive', '--user', '65534:65534', container_id,
                    'python', '-I', '-B', '/checks/' + adapter] + ([case] if recipe is None else [])
                session = _Session(self, commands, session_argv)
                for phase in b01.PHASES:
                    self._phase_runtime(phase + '-runtime-before')
                    self._retain(phase + '-staging-before.json', encoded(verify_staging()))
                    try:
                        session.phase(phase)
                        checked(phase + '-pause', ['docker', 'pause', container_id])
                        record = checked(phase + '-state', ['docker', 'inspect', '--format', '{{json .}}', container_id])
                        state = parsed(record)
                        require(b01._paused(state, volume, self.policy.image_id) and state.get('Id') == container_id
                            and state.get('Config', {}).get('Labels') == labels, 'Exact container snapshot ownership differs')
                        checked(phase + '-capture', ['docker', 'cp', container_id + ':/tmp', '-'], original.MAX_CAPTURE_BYTES)
                        checked(phase + '-unpause', ['docker', 'unpause', container_id])
                    finally:
                        self._phase_runtime(phase + '-runtime-after')
                        self._retain(phase + '-staging-after.json', encoded(verify_staging()))
                require(session.finish(True), 'Persistent adapter session termination incomplete')
                session = None
            except BaseException as error:
                primary = error
                errors.append(type(error).__name__ + ':' + str(error)[:500])
            finally:
                try:
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
                self._cleanup_phase = False
            try:
                self._retain('staging-final.json', encoded(verify_staging()))
            except BaseException as error:
                errors.append('final-staging:' + type(error).__name__)
        assert self.journal is not None
        if self.journal.uncertain:
            raise ExecutionUnknown('Original storage journal became uncertain; redispatch forbidden') from primary
        terminal = {'protocol': self.binding.protocol, 'intent_sha256': sha(self.read_authenticated('intent.json')),
            'execution_id': intent['execution_id'], 'case_id': case, 'family': self.binding.family,
            'source_sha256': self.binding.source_sha256, 'native_source_sha256': self.binding.native_source_sha256,
            'commands': list(commands.records), 'infrastructure': errors,
            'container_cleanup': removed['container'], 'volume_cleanup': removed['volume']}
        self._retain('terminal.json', encoded(terminal))
        if primary is not None and not isinstance(primary, Exception):
            raise primary

    def close(self) -> None:
        if getattr(self, 'closed', True):
            return
        self._owner()
        if self._cleanup is not None:
            self._cleanup.close()
        if self.journal is not None:
            self.journal.close()
        self.closed = True

    def __enter__(self) -> CandidateStorageExecution:
        return self

    def __exit__(self, *_args: Any) -> None:
        self.close()
